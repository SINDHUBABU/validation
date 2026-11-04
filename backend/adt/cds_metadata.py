"""
SAP ABAP CDS metadata discovery through ADT.

This module parses CDS DDL for metadata/lineage discovery only. It does not
execute CDS expressions and does not replace SAP's returned column metadata
as the authority for data extraction.

Lineage parsing is intentionally conservative: expressions and conditions are
preserved as source text. Unsupported syntax should be treated as unresolved,
not guessed.
"""
from __future__ import annotations

import re
from typing import Any, Optional

import requests
from requests.auth import HTTPBasicAuth


class CDSMetadataClient:
    IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
    PARAMETERS_PATTERN = re.compile(
        r"\bwith\s+parameters\b(.*?)\bas\s+select\s+from\b",
        re.IGNORECASE | re.DOTALL,
    )
    PARAMETER_LINE_PATTERN = re.compile(
    r"^\s*(?P<name>[A-Za-z][A-Za-z0-9_]*)\s*:\s*"
    r"(?P<type>[A-Za-z][A-Za-z0-9_]*(?:\.[A-Za-z][A-Za-z0-9_]*)*"
    r"(?:\s*\([^)]*\))?)\s*,?\s*$",
    re.IGNORECASE,
)
    ANNOTATION_LINE_PATTERN = re.compile(
        r"^\s*@(?P<key>[A-Za-z][A-Za-z0-9_.]*)\s*:\s*(?P<value>.*?)\s*$"
    )
    LABEL_PATTERN = re.compile(
        r"@EndUserText\.label\s*:\s*'(?P<label>(?:''|[^'])*)'",
        re.IGNORECASE,
    )
    CAST_TYPE_PATTERN = re.compile(
        r"\bas\s+(?:abap\.)?(?P<type>[A-Za-z][A-Za-z0-9_]*)(?:\s*\((?P<args>[^)]*)\))?",
        re.IGNORECASE,
    )

    def __init__(self, base_url, username, password, client="100", timeout=30):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(username, password)
        self.session.headers.update({"sap-client": str(client)})

    def get_source(self, cds_entity):
        if not isinstance(cds_entity, str) or not self.IDENTIFIER_PATTERN.fullmatch(
            cds_entity.strip()
        ):
            raise ValueError("Invalid CDS entity name.")
        cds_entity = cds_entity.strip()
        endpoint = (
            f"{self.base_url}/sap/bc/adt/ddic/ddl/sources/"
            f"{cds_entity.lower()}/source/main"
        )
        try:
            response = self.session.get(
                endpoint,
                headers={"Accept": "text/plain", "X-CSRF-Token": "Fetch"},
                timeout=self.timeout,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            raise RuntimeError(
                f"Failed to retrieve CDS source for {cds_entity}: {exc}"
            ) from exc
        return response.text

    @staticmethod
    def _remove_comments(source):
        """Remove // and /* */ comments while preserving quoted strings."""
        result, i, n = [], 0, len(source)
        while i < n:
            if source.startswith("//", i):
                end = source.find("\n", i)
                if end == -1:
                    break
                result.append("\n")
                i = end + 1
                continue
            if source.startswith("/*", i):
                end = source.find("*/", i + 2)
                if end == -1:
                    # Preserve malformed remainder for diagnostics.
                    result.append(source[i:])
                    break
                comment = source[i:end + 2]
                result.append("".join("\n" for ch in comment if ch == "\n"))
                i = end + 2
                continue
            if source[i] in ("'", '"'):
                quote, start = source[i], i
                i += 1
                while i < n:
                    if source[i] == quote:
                        if i + 1 < n and source[i + 1] == quote:
                            i += 2
                            continue
                        i += 1
                        break
                    i += 1
                result.append(source[start:i])
                continue
            result.append(source[i])
            i += 1
        return "".join(result)

    @staticmethod
    def _matching_delimiter(text, start):
        pairs = {"{": "}", "[": "]", "(": ")"}
        if start < 0 or start >= len(text) or text[start] not in pairs:
            return start
        stack, quote, i = [pairs[text[start]]], None, start + 1
        while i < len(text):
            ch = text[i]
            if quote:
                if ch == quote:
                    if i + 1 < len(text) and text[i + 1] == quote:
                        i += 2
                        continue
                    quote = None
                i += 1
                continue
            if ch in ("'", '"'):
                quote = ch
            elif ch in pairs:
                stack.append(pairs[ch])
            elif ch in ")]}":
                if stack and ch == stack[-1]:
                    stack.pop()
                    if not stack:
                        return i + 1
            i += 1
        return len(text)

    @classmethod
    def _find_projection_body(cls, source):
        cleaned = cls._remove_comments(source)
        match = re.search(r"\bas\s+select\s+from\b", cleaned, re.IGNORECASE)
        if not match:
            return None
        i = match.end()
        quote = None
        while i < len(cleaned):
            ch = cleaned[i]
            if quote:
                if ch == quote:
                    if i + 1 < len(cleaned) and cleaned[i + 1] == quote:
                        i += 2
                        continue
                    quote = None
            elif ch in ("'", '"'):
                quote = ch
            elif ch == "{":
                end = cls._matching_delimiter(cleaned, i)
                if end > i + 1 and cleaned[end - 1] == "}":
                    return cleaned[i + 1:end - 1]
                return None
            i += 1
        return None

    @staticmethod
    def _split_top_level(text, delimiter=","):
        """Split on delimiter only outside strings and nested brackets."""
        items, current = [], []
        depths = {"(": 0, "{": 0, "[": 0}
        close_to_open = {")": "(", "}": "{", "]": "["}
        quote, i = None, 0
        while i < len(text):
            ch = text[i]
            if quote:
                current.append(ch)
                if ch == quote:
                    if i + 1 < len(text) and text[i + 1] == quote:
                        current.append(text[i + 1])
                        i += 2
                        continue
                    quote = None
            elif ch in ("'", '"'):
                quote = ch
                current.append(ch)
            elif ch in depths:
                depths[ch] += 1
                current.append(ch)
            elif ch in close_to_open:
                depths[close_to_open[ch]] = max(
                    0, depths[close_to_open[ch]] - 1
                )
                current.append(ch)
            elif ch == delimiter and not any(depths.values()):
                item = "".join(current).strip()
                if item:
                    items.append(item)
                current = []
            else:
                current.append(ch)
            i += 1
        final = "".join(current).strip()
        if final:
            items.append(final)
        return items

    @classmethod
    def _split_projection_items(cls, body):
        return cls._split_top_level(body)

    @classmethod
    def _remove_leading_annotations(cls, expression):
        _, declaration = cls._extract_leading_field_annotations(expression)
        return declaration

    @classmethod
    def _parse_annotation_value(cls, value):
        value = value.strip().rstrip(",").strip()
        if not value:
            return ""
        if len(value) >= 2 and value[0] == "'" and value[-1] == "'":
            return value[1:-1].replace("''", "'")
        if value.lower() == "true":
            return True
        if value.lower() == "false":
            return False
        if value.lower() == "null":
            return None
        if re.fullmatch(r"-?\d+", value):
            try:
                return int(value)
            except ValueError:
                pass
        return value

    @classmethod
    def _extract_annotations(cls, source):
        cleaned = cls._remove_comments(source)
        declaration = re.search(
            r"^\s*define\s+(?:view|table|custom\s+entity|abstract\s+entity|root\s+view)\b",
            cleaned, re.IGNORECASE | re.MULTILINE,
        )
        if declaration:
            cleaned = cleaned[:declaration.start()]
        annotations, lines, i = {}, cleaned.splitlines(), 0
        while i < len(lines):
            match = cls.ANNOTATION_LINE_PATTERN.match(lines[i].strip())
            if not match:
                i += 1
                continue
            key = "@" + match.group("key")
            block = [match.group("value").strip()]
            i += 1
            combined = "\n".join(block)
            while i < len(lines):
                if cls._bracket_balance(combined) <= 0 and (
                    lines[i].lstrip().startswith("@")
                    or re.match(r"^\s*(define\s+|as\s+select\s+from\b)",
                                lines[i], re.IGNORECASE)
                ):
                    break
                block.append(lines[i].strip())
                combined = "\n".join(block)
                i += 1
                if cls._bracket_balance(combined) <= 0:
                    break
            raw = "\n".join(block).strip().rstrip(",").strip()
            annotations[key] = cls._parse_annotation_value(raw)

        analytics = annotations.get("@Analytics")
        if isinstance(analytics, str):
            m = re.search(
                r"dataExtraction\s*:\s*\{.*?\benabled\s*:\s*(true|false)",
                analytics, re.IGNORECASE | re.DOTALL,
            )
            if m:
                annotations["@Analytics.dataExtraction.enabled"] = (
                    m.group(1).lower() == "true"
                )
            m = re.search(r"dataCategory\s*:\s*(#[A-Za-z0-9_]+)",
                          analytics, re.IGNORECASE)
            if m:
                annotations["@Analytics.dataCategory"] = m.group(1)
        return annotations

    @classmethod
    def _extract_structured_value(cls, source, key_pattern):
        cleaned = cls._remove_comments(source)
        match = re.search(key_pattern + r"\s*:\s*\{", cleaned, re.IGNORECASE)
        if not match:
            return None
        start = cleaned.find("{", match.start(), match.end())
        end = cls._matching_delimiter(cleaned, start)
        return cleaned[start:end] if end > start and cleaned[end - 1] == "}" else None

    @staticmethod
    def _parse_cds_string_list(value):
        if not value:
            return []
        return [item.replace("''", "'") for item in
                re.findall(r"'((?:''|[^'])*)'", value)]

    @classmethod
    def parse_extraction_metadata(cls, source):
        annotations = cls._extract_annotations(source)
        extraction = {"enabled": annotations.get("@Analytics.dataExtraction.enabled")}
        cdc = cls._extract_structured_value(
            source, r"@Analytics\.dataExtraction\.delta\.changeDataCapture"
        )
        if cdc is None:
            analytics = cls._extract_structured_value(source, r"@Analytics")
            if analytics:
                nested = re.search(r"changeDataCapture\s*:\s*\{", analytics,
                                   re.IGNORECASE)
                if nested:
                    start = analytics.find("{", nested.start(), nested.end())
                    end = cls._matching_delimiter(analytics, start)
                    if end > start and analytics[end - 1] == "}":
                        cdc = analytics[start:end]
        if cdc:
            mappings = []
            mapping_match = re.search(r"\bmapping\s*:\s*\[", cdc, re.IGNORECASE)
            if mapping_match:
                start = cdc.find("[", mapping_match.start(), mapping_match.end())
                end = cls._matching_delimiter(cdc, start)
                array = cdc[start + 1:end - 1] if end > start else ""
                i = 0
                while i < len(array):
                    if array[i] == "{":
                        obj_end = cls._matching_delimiter(array, i)
                        if obj_end <= i or array[obj_end - 1] != "}":
                            break
                        obj = array[i + 1:obj_end - 1]
                        table = re.search(r"\btable\s*:\s*'((?:''|[^'])*)'", obj, re.I)
                        role = re.search(r"\brole\s*:\s*#?([A-Za-z0-9_]+)", obj, re.I)
                        view = re.search(r"\bviewElement\s*:\s*\[([^\]]*)\]", obj, re.I | re.S)
                        table_elements = re.search(r"\btableElement\s*:\s*\[([^\]]*)\]", obj, re.I | re.S)
                        mappings.append({
                            "table": table.group(1).replace("''", "'") if table else None,
                            "role": role.group(1).upper() if role else None,
                            "view_elements": cls._parse_cds_string_list(view.group(1)) if view else [],
                            "table_elements": cls._parse_cds_string_list(table_elements.group(1)) if table_elements else [],
                        })
                        i = obj_end
                    else:
                        i += 1
            extraction["change_data_capture"] = {"mapping": mappings}
        return extraction

    @staticmethod
    def _bracket_balance(text):
        balance, quote, i = 0, None, 0
        while i < len(text):
            ch = text[i]
            if quote:
                if ch == quote:
                    if i + 1 < len(text) and text[i + 1] == quote:
                        i += 2
                        continue
                    quote = None
            elif ch in ("'", '"'):
                quote = ch
            elif ch in "([{":
                balance += 1
            elif ch in ")]}":
                balance -= 1
            i += 1
        return balance

    @classmethod
    def _extract_leading_field_annotations(cls, item):
        """Extract leading field annotations, including bare marker annotations.

        Standard ``@Name: value`` annotations are parsed as before. A bare
        marker such as ``@Semantics.systemTime.createdAt`` is also consumed as
        a boolean marker so it cannot be mistaken for a projection expression.
        """
        text, annotations = item.lstrip(), {}
        while text.startswith("@"):
            lines = text.splitlines(keepends=True)
            if not lines:
                break
            first_line = lines[0].strip()
            match = cls.ANNOTATION_LINE_PATTERN.match(first_line)
            if match:
                key, value = "@" + match.group("key"), match.group("value").strip()
                consumed = len(lines[0])
                balance = cls._bracket_balance(value)
                idx = 1
                while balance > 0 and idx < len(lines):
                    value += "\n" + lines[idx].strip()
                    consumed += len(lines[idx])
                    balance = cls._bracket_balance(value)
                    idx += 1
                annotations[key] = cls._parse_annotation_value(value)
            else:
                marker = re.fullmatch(r"@(?P<key>[A-Za-z][A-Za-z0-9_.]*)", first_line)
                if not marker:
                    break
                key = "@" + marker.group("key")
                annotations[key] = True
                consumed = len(lines[0])
            text = text[consumed:].lstrip("\r\n \t")
        return annotations, text

    @classmethod
    def parse_parameters(cls, source):
        return [{"name": p["name"], "type": p["type"]}
                for p in cls.parse_parameter_details(source)]

    @classmethod
    def parse_parameter_details(cls, source):
        cleaned = cls._remove_comments(source)
        match = cls.PARAMETERS_PATTERN.search(cleaned)
        if not match:
            return []
        parameters, pending = [], {}
        for raw_line in match.group(1).splitlines():
            line = raw_line.strip()
            if not line:
                continue
            annotation = cls.ANNOTATION_LINE_PATTERN.match(line)
            if annotation:
                pending["@" + annotation.group("key")] = cls._parse_annotation_value(
                    annotation.group("value")
                )
                continue
            parameter = cls.PARAMETER_LINE_PATTERN.match(line)
            if not parameter:
                continue
            parameters.append({
                "name": parameter.group("name"),
                "type": re.sub(r"\s+", "", parameter.group("type")),
                "annotations": dict(pending),
                "description": pending.get("@EndUserText.label"),
            })
            pending = {}
        return parameters

    @classmethod
    def parse_fields(cls, source):
        body = cls._find_projection_body(source)
        if body is None:
            return []
        fields, seen = [], set()
        for item in cls._split_projection_items(body):
            annotations, expression = cls._extract_leading_field_annotations(item)
            expression = expression.strip()
            if not expression:
                continue
            key_match = re.match(r"^\s*key\s+", expression, re.IGNORECASE)
            is_key = bool(key_match)
            if key_match:
                expression = expression[key_match.end():].strip()
            if not expression or re.fullmatch(r"_[A-Za-z0-9_]+", expression):
                continue
            alias_match = re.search(
                r"\s+as\s+([A-Za-z_][A-Za-z0-9_]*)\s*$",
                expression, re.IGNORECASE,
            )
            if alias_match:
                name = alias_match.group(1)
                source_expression = expression[:alias_match.start()].strip()
            else:
                source_expression = expression
                direct = re.fullmatch(
                    r"(?:[A-Za-z_][A-Za-z0-9_]*\s*\.\s*)?([A-Za-z_][A-Za-z0-9_]*)",
                    source_expression,
                )
                if not direct:
                    # Keep calculated/unknown expressions rather than silently
                    # dropping them. Name can be derived only when explicitly aliased.
                    continue
                name = direct.group(1)
            if name.startswith("_") or name.lower() in seen:
                continue
            seen.add(name.lower())
            cast_type = cast_args = None
            cast_match = cls.CAST_TYPE_PATTERN.search(source_expression)
            if cast_match and re.search(r"\bcast\s*\(", source_expression, re.I):
                cast_type, cast_args = cast_match.group("type"), cast_match.group("args")
            semantic_type = currency_reference = unit_reference = None
            for key, value in annotations.items():
                lower = key.lower()
                if lower.endswith(".amount.currencycode"):
                    semantic_type, currency_reference = "amount", value
                elif lower.endswith(".quantity.unitofmeasure"):
                    semantic_type, unit_reference = "quantity", value
            label = annotations.get("@EndUserText.label")
            if not label:
                lm = cls.LABEL_PATTERN.search(item)
                label = lm.group("label").replace("''", "'") if lm else name
            fields.append({
                "name": name,
                "is_key": is_key,
                "is_key_figure": False,
                "description": label,
                "expression": source_expression,
                "cast_type": cast_type,
                "cast_args": cast_args,
                "data_type": cast_type,
                "semantic_type": semantic_type,
                "currency_reference": currency_reference,
                "unit_reference": unit_reference,
                "annotations": annotations,
            })
        return fields

    # ---------- CDS source / lineage metadata ----------

    @classmethod
    def _find_select_from_clause(cls, source):
        """Return cleaned DDL and location of SELECT FROM after the projection."""
        cleaned = cls._remove_comments(source)
        match = re.search(r"\bas\s+select\s+from\b", cleaned, re.IGNORECASE)
        if not match:
            return cleaned, None
        body = cls._find_projection_body(cleaned)
        if body is None:
            return cleaned, None
        projection_open = cleaned.find("{", match.end())
        projection_end = cls._matching_delimiter(cleaned, projection_open)
        return cleaned, (projection_end, len(cleaned)) if projection_end < len(cleaned) else None

    @staticmethod
    def _extract_clause(text, start_pattern, stop_patterns):
        """Extract a clause body, stopping at top-level clause keywords."""
        match = re.search(start_pattern, text, re.IGNORECASE)
        if not match:
            return None
        start = match.end()
        quote, depths, i = None, {"(": 0, "{": 0, "[": 0}, start
        while i < len(text):
            ch = text[i]
            if quote:
                if ch == quote:
                    if i + 1 < len(text) and text[i + 1] == quote:
                        i += 2
                        continue
                    quote = None
            elif ch in ("'", '"'):
                quote = ch
            elif ch in depths:
                depths[ch] += 1
            elif ch == ")" and depths["("]:
                depths["("] -= 1
            elif ch == "}" and depths["{"]:
                depths["{"] -= 1
            elif ch == "]" and depths["["]:
                depths["["] -= 1
            elif not any(depths.values()):
                for stop in stop_patterns:
                    stop_match = re.match(stop, text[i:], re.IGNORECASE)
                    if stop_match:
                        return text[start:i].strip(), i
            i += 1
        return text[start:].strip(), len(text)

    @classmethod
    def parse_lineage(cls, source):
        """Parse conservative CDS source lineage without claiming completeness.

        ``supported`` means the main projection structure was recognized.
        ``complete`` is tri-state: False when known gaps were detected, and
        None when no known gap was detected but static parsing cannot prove
        that every SAP CDS construct was captured. It is never set to True.
        Expressions and conditions are returned as text and are not evaluated.
        """
        cleaned = cls._remove_comments(source)
        empty_result = {
            "supported": False, "complete": False,
            "completeness_status": "unsupported", "unresolved": [],
            "sources": [], "joins": [], "associations": [],
            "projection": [], "where": None, "group_by": None,
            "having": None, "limitations": [],
        }
        projection_match = re.search(r"\bas\s+select\s+from\b", cleaned, re.I)
        if not projection_match:
            empty_result["unresolved"].append("No AS SELECT FROM projection found.")
            empty_result["limitations"].append("Only CDS entities using AS SELECT FROM are handled.")
            return empty_result

        body_start = cleaned.find("{", projection_match.end())
        body_end = cls._matching_delimiter(cleaned, body_start)
        if body_start < 0 or body_end <= body_start:
            empty_result["unresolved"].append("Could not identify balanced projection body.")
            empty_result["limitations"].append("Projection body could not be parsed safely.")
            return empty_result

        projection_body = cleaned[body_start + 1:body_end - 1]
        source_clause = cleaned[projection_match.end():body_start].strip()
        tail = re.sub(r";\s*$", "", cleaned[body_end:]).strip()
        unresolved = []

        where = cls._extract_simple_clause(tail, r"\bwhere\b", [
            r"\bgroup\s+by\b", r"\bhaving\b", r"\border\s+by\b",
            r"\bunion\b", r"\bintersect\b", r"\bexcept\b",
        ])
        group_by = cls._extract_simple_clause(tail, r"\bgroup\s+by\b", [
            r"\bhaving\b", r"\border\s+by\b", r"\bunion\b",
            r"\bintersect\b", r"\bexcept\b",
        ])
        having = cls._extract_simple_clause(tail, r"\bhaving\b", [
            r"\border\s+by\b", r"\bunion\b", r"\bintersect\b", r"\bexcept\b",
        ])

        # A source must be identifiable immediately after SELECT FROM.
        from_match = re.search(
            r"\bselect\s+from\s+([A-Za-z_][A-Za-z0-9_./]*)"
            r"(?:\s+as\s+([A-Za-z_][A-Za-z0-9_]*))?",
            cleaned[:body_start], re.I,
        )
        sources, joins = [], []
        if from_match:
            sources.append({
                "entity": from_match.group(1),
                "alias": from_match.group(2) or from_match.group(1),
                "kind": "base",
            })
        else:
            unresolved.append("Base source could not be identified after SELECT FROM.")

        join_pattern = re.compile(
            r"\b(?:(left\s+outer|right\s+outer|left|right|inner|cross)\s+)?join\s+"
            r"([A-Za-z_][A-Za-z0-9_./]*)"
            r"(?:\s+as\s+([A-Za-z_][A-Za-z0-9_]*))?",
            re.I,
        )
        for jm in join_pattern.finditer(source_clause):
            after = source_clause[jm.end():]
            on_match = re.match(r"\s*on\b", after, re.I)
            condition = None
            if on_match:
                cond_text = after[on_match.end():]
                condition, _ = cls._extract_clause(cond_text, r"^\s*", [
                    r"\bleft\s+(?:outer\s+)?join\b", r"\bright\s+(?:outer\s+)?join\b",
                    r"\binner\s+join\b", r"\bcross\s+join\b", r"\bjoin\b",
                    r"\bwhere\b", r"\bgroup\s+by\b", r"\bhaving\b",
                    r"\border\s+by\b", r"\bunion\b", r"\bintersect\b", r"\bexcept\b",
                ])
            else:
                unresolved.append(f"Join source {jm.group(2)} has no recognizable ON condition.")
            alias = jm.group(3) or jm.group(2)
            join_type = re.sub(r"\s+", " ", (jm.group(1) or "inner").lower())
            joins.append({"entity": jm.group(2), "alias": alias,
                          "join_type": join_type, "condition": condition})
            sources.append({"entity": jm.group(2), "alias": alias, "kind": "join"})

        # Association declarations are metadata, not automatically traversed joins.
        # ABAP CDS normally places cardinality before TO, e.g.
        # ``association [0..1] to I_Country as _Country on ...``.
        associations = []
        association_pattern = re.compile(
            r"\b(?P<kind>association|composition)\s+"
            r"(?P<cardinality>\[[^\]]*\])?\s*to\s+"
            r"(?P<target>[A-Za-z_][A-Za-z0-9_./]*)"
            r"(?:\s+as\s+(?P<alias>_[A-Za-z0-9_]+))?\s+on\s+"
            r"(?P<condition>.*?)(?=,\s*(?=(?:association|composition)\b)|"
            r"\n\s*(?=(?:association|composition)\s+(?:\[[^\]]*\]\s*)?to\b|"
            r"where\b|group\s+by\b|having\b|order\s+by\b|"
            r"union\b|intersect\b|except\b)|$)",
            re.I | re.S,
        )
        for am in association_pattern.finditer(source_clause):
            associations.append({
                "kind": am.group("kind").lower(), "target": am.group("target"),
                "alias": am.group("alias"),
                "cardinality": am.group("cardinality").strip() if am.group("cardinality") else None,
                "condition": am.group("condition").strip(),
            })

        projection = []
        for item in cls._split_projection_items(projection_body):
            annotations, expr = cls._extract_leading_field_annotations(item)
            expr = expr.strip()
            key = bool(re.match(r"^key\s+", expr, re.I))
            if key:
                expr = re.sub(r"^key\s+", "", expr, flags=re.I).strip()
            alias_match = re.search(r"\s+as\s+([A-Za-z_][A-Za-z0-9_]*)\s*$", expr, re.I)
            if alias_match:
                field_name = alias_match.group(1)
                expression = expr[:alias_match.start()].strip()
            else:
                expression = expr
                direct_name = re.fullmatch(
                    r"(?:[A-Za-z_][A-Za-z0-9_]*\s*\.\s*)?([A-Za-z_][A-Za-z0-9_]*)", expr
                )
                field_name = direct_name.group(1) if direct_name else None
            if not expr or re.fullmatch(r"_[A-Za-z0-9_]+", expr):
                continue
            direct = re.fullmatch(
                r"(?:(?P<qualifier>[A-Za-z_][A-Za-z0-9_]*)\s*\.\s*)?"
                r"(?P<field>[A-Za-z_][A-Za-z0-9_]*)", expression
            )
            path_like = bool(re.search(r"\b_[A-Za-z0-9_]+\.", expression))
            if field_name is None:
                unresolved.append(f"Projection expression could not be mapped to an output field: {expression[:100]}")
            if path_like:
                unresolved.append(f"Association path in projection is not resolved: {expression[:100]}")
            projection.append({
                "field": field_name, "expression": expression, "is_key": key,
                "source_alias": direct.group("qualifier") if direct else None,
                "source_field": direct.group("field") if direct else None,
                "expression_kind": "direct" if direct else (
                    "cast" if re.search(r"\bcast\s*\(", expression, re.I)
                    else "calculated_or_unresolved"),
                "annotations": annotations,
            })

        # These constructs alter lineage semantics and are not fully modeled here.
        for pattern, label in [
            (r"\bunion\b", "UNION lineage is not modeled."),
            (r"\bintersect\b", "INTERSECT lineage is not modeled."),
            (r"\bexcept\b", "EXCEPT lineage is not modeled."),
            (r"\bextend\s+view\b", "View extensions are not modeled."),
            (r"\bselect\s+from\b.*\(\s*select\b", "Nested subquery lineage may be incomplete."),
        ]:
            if re.search(pattern, cleaned, re.I):
                unresolved.append(label)

        limitations = [
            "Static DDL parsing is not SAP compiler validation.",
            "Calculated expressions and conditions are preserved as text, not semantically evaluated.",
            "Association declarations are reported but association-path expansion is not resolved recursively.",
            "Unions, view extensions, nested subqueries, and some advanced CDS syntax may require additional parsing.",
            "A detected source or CDC mapping does not prove extraction completeness or unique row grain.",
        ]
        # Static parsing cannot prove completeness, even when no known gap was found.
        return {
            "supported": True,
            "complete": False if unresolved else None,
            "completeness_status": "known_gaps" if unresolved else "unverified",
            "unresolved": list(dict.fromkeys(unresolved)),
            "sources": sources, "joins": joins, "associations": associations,
            "projection": projection, "where": where,
            "group_by": group_by, "having": having,
            "limitations": limitations,
        }

    @classmethod
    def _extract_simple_clause(cls, text, start_pattern, stop_patterns):
        result = cls._extract_clause(text, start_pattern, stop_patterns)
        return result[0] if result else None

    def discover_metadata(self, cds_entity):
        source = self.get_source(cds_entity)
        parameters = self.parse_parameter_details(source)
        fields = self.parse_fields(source)
        annotations = self._extract_annotations(source)
        extraction = self.parse_extraction_metadata(source)
        lineage = self.parse_lineage(source)
        return {
            "entity": cds_entity,
            "sql_view_name": annotations.get("@AbapCatalog.sqlViewName"),
            "parameters": parameters,
            "parameterized": bool(parameters),
            "annotations": annotations,
            "extraction": extraction,
            "fields": fields,
            "key_fields": [field["name"] for field in fields if field["is_key"]],
            "lineage": lineage,
            "source_ddl": source,
        }

    def discover_parameters(self, cds_entity):
        metadata = self.discover_metadata(cds_entity)
        return {
            "entity": metadata["entity"],
            "parameters": [
                {"name": p["name"], "type": p["type"]}
                for p in metadata["parameters"]
            ],
            "parameterized": metadata["parameterized"],
        }

    def close(self):
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
