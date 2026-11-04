from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any
from concurrent.futures import ThreadPoolExecutor


logger = logging.getLogger(__name__)


class DatasphereDependencyResolver:

    READABLE_TYPES = {
        "view",
        "local_table",
        "remote_table",
        "analytic_model",
        "fact_model",
    }

    BOUNDARY_TYPES = {
        "system",
        "external",
        "shared_remote_table",
        "unknown",
    }

    # ----------------------------------------------------------------
    # Discovery shape knowledge
    #
    # The Datasphere CLI wraps list results in different envelopes
    # depending on endpoint and version. We unwrap the common ones.
    # ----------------------------------------------------------------

    _LIST_WRAPPER_KEYS = (
        "value", "objects", "items", "data",
        "results", "rows", "content", "entries",
    )

    _NAME_KEYS = (
        "technicalName", "technical_name",
        "objectName", "object_name",
        "entityName", "entity_name",
        "name",
        "key",
        "id",
    )

    _NESTED_NAME_WRAPPERS = (
        "metadata", "attributes", "details", "header", "properties",
    )

    # ----------------------------------------------------------------
    # Detail-detection constants
    # ----------------------------------------------------------------

    _ASSOCIATION_TYPES = {
        "cds.Association",
        "cds.association",
        "Association",
        "association",
    }

    _JOIN_NODE_TYPES = {
        "join",
        "Join",
        "cds.Join",
        "cds.join",
        "JoinNode",
    }

    _JOIN_SIDE_KEYS = {
        "left", "right",
        "leftSource", "rightSource",
        "on", "condition",
        "joinType", "join_type", "join",
        "cardinality",
        "args",
        "as", "alias",
    }

    _JOIN_TYPE_ALIASES = {
        "inner": "INNER JOIN",
        "inner join": "INNER JOIN",
        "left": "LEFT OUTER JOIN",
        "left join": "LEFT OUTER JOIN",
        "left outer": "LEFT OUTER JOIN",
        "left outer join": "LEFT OUTER JOIN",
        "right": "RIGHT OUTER JOIN",
        "right join": "RIGHT OUTER JOIN",
        "right outer": "RIGHT OUTER JOIN",
        "right outer join": "RIGHT OUTER JOIN",
        "full": "FULL OUTER JOIN",
        "full outer": "FULL OUTER JOIN",
        "full outer join": "FULL OUTER JOIN",
        "cross": "CROSS JOIN",
        "cross join": "CROSS JOIN",
    }

    _SKIP_KEYS = {
        "ref",
        "columns",
        "elements",
        "measures",
        "dimensions",
        "keys",
    }

    _FIELD_ANNOTATION_MAP = {
        "@EndUserText.label": "label",
        "@EndUserText.quickInfo": "quick_info",
        "@AnalyticsDetails.measureType": "measure_type",
        "@AnalyticsDetails.dimension": "dimension",
        "@AnalyticsDetails.query.axis": "axis",
        "@AnalyticsDetails.query.measure": "measure_kind",
        "@Aggregation.default": "aggregation",
        "@ObjectModel.foreignKey.association": "foreign_key",
        "@ObjectModel.key": "object_model_key",
        "@Semantics.amount.currencyCode": "currency_code",
        "@Semantics.quantity.unitOfMeasure": "unit_of_measure",
        "@Semantics.text": "text_association",
    }

    _PATH_STRUCTURAL_SEGMENTS = frozenset({
        "definitions", "businessLayerDefinitions",
        "elements", "columns", "measures", "dimensions", "keys",
        "attributes",
        "calculatedMeasures", "restrictedMeasures",
        "calculatedDimensions", "calculatedAttributes",
        "parameters",
        "query", "SELECT", "SET", "from", "FROM",
        "args", "expression", "xpr", "on", "where", "filter",
        "ref", "val", "func", "target",
        "left", "right", "joinType", "join_type",
        "as", "alias", "sourceModel",
    })

    _DETAIL_SIGNATURES = {
        "joins": lambda j: (
            j.get("join_type"),
            repr(j.get("left")),
            repr(j.get("right")),
            j.get("on"),
        ),
        "filters": lambda f: (
            f.get("location"),
            f.get("condition"),
        ),
        "associations": lambda a: (
            a.get("name"),
            a.get("target"),
            a.get("cardinality"),
        ),
        "calculated_columns": lambda c: (
            c.get("name"),
            c.get("expression"),
        ),
        "formulas": lambda f: (
            f.get("name"),
            f.get("formula"),
        ),
        "field_catalog": lambda f: (
            f.get("name"),
            f.get("element_type"),
        ),
        "parameters": lambda p: (
            p.get("name"),
        ),
    }

    def __init__(
        self,
        client,
        space: str,
        max_depth: int = 30,
        silent: bool = False,
        log_callback=None,
        replication_space: str | None = None,
    ):
        self.client = client
        self.space = space
        self.max_depth = max_depth
        self.silent = silent
        self._log_callback = log_callback
        self.replication_space = (
            replication_space
            or getattr(client, "replication_space", None)
            or os.getenv("DATASPHERE_REPLICATION_SPACE", "").strip()
            or None
        )

        self.object_index: dict[Any, str] = {}
        self.cache: dict[tuple, dict] = {}
        self.unresolved_types: set[Any] = set()

        self._discover_objects()

    # ================================================================
    # PUBLIC API
    # ================================================================

    def resolve(
        self,
        object_name: str,
        object_type: str | None = None,
        space: str | None = None,
    ) -> dict:

        self.cache.clear()

        target_space = space or self.space

        if object_type is None:
            object_type = self._classify_object(object_name, space=target_space)

        tree = self._resolve(
            object_name=object_name,
            object_type=object_type,
            depth=0,
            path=set(),
            space=target_space,
        )
        self.propagate_calculated_columns(tree)
        return tree

    def resolve_many(
        self,
        names: list[str],
        object_type: str | None = None,
        on_progress=None,
    ) -> dict[str, dict]:

        results: dict[str, dict] = {}

        for i, name in enumerate(names, start=1):
            if on_progress is not None:
                try:
                    on_progress(i, len(names), name)
                except Exception:
                    pass

            if not self.silent:
                print(f"\n===== RESOLVING [{i}/{len(names)}] {name} =====")

            results[name] = self.resolve(name, object_type=object_type)

        return results

    # ================================================================
    # LOGGING
    # ================================================================

    def _log(self, message: str):
        if self._log_callback is not None:
            try:
                self._log_callback(message)
            except Exception:
                pass
        if not self.silent:
            try:
                print(message)
            except UnicodeEncodeError:
                print(message.encode("ascii", "replace").decode("ascii"))

    # ================================================================
    # OBJECT DISCOVERY
    # ================================================================

    def _discover_objects(self):

        self._log("[DISCOVERY] Building Datasphere object index...")

        loaders = [
            ("view", self.client.list_views),
            ("local_table", self.client.list_local_tables),
            ("analytic_model", self.client.list_analytic_models),
            ("fact_model", self.client.list_fact_models),
            ("remote_table", self.client.list_remote_tables),
        ]

        def load_one(spec):
            object_type, loader = spec
            try:
                raw = loader(self.space, top=500)
            except Exception as exc:
                return object_type, None, exc

            objects = self._unwrap_list_result(raw)
            if objects is None:
                return object_type, None, ValueError(
                    f"unexpected result shape from "
                    f"{type(loader).__name__}: "
                    f"{type(raw).__name__}"
                )
            return object_type, objects, None

        try:
            with ThreadPoolExecutor(max_workers=len(loaders)) as pool:
                results = list(pool.map(load_one, loaders))
        except Exception as exc:
            self._log(
                f"[DISCOVERY] Parallel discovery failed, "
                f"falling back to sequential: {exc}"
            )
            results = [load_one(spec) for spec in loaders]

        for object_type, objects, exc in results:
            if exc is not None:
                msg = (
                    f"[DISCOVERY] {object_type}: no results ({exc})"
                )
                self._log(msg)
                logger.warning(msg)
                continue

            if not isinstance(objects, list):
                continue

            count = 0
            first_unrecognised: Any = None

            for obj in objects:
                if not isinstance(obj, dict):
                    continue

                name = self._extract_object_name(obj)
                if not name:
                    if first_unrecognised is None:
                        first_unrecognised = obj
                    continue

                self.object_index[(self.space, name)] = object_type
                self.object_index[name] = object_type
                count += 1

            self._log(f"[DISCOVERY] {object_type}: {count}")

            if count == 0 and objects:
                logger.warning(
                    "[DISCOVERY] %s returned %d item(s) but none had "
                    "a recognised name key. First item shape: %r",
                    object_type,
                    len(objects),
                    first_unrecognised,
                )

        total = len(self.object_index)
        self._log(f"[DISCOVERY] Total indexed objects: {total}")

        if total == 0:
            logger.error(
                "[DISCOVERY] Object index is empty. Every dependency "
                "will require a 4-way type probe. Check the shape of "
                "the list_* client responses against _NAME_KEYS."
            )
        else:
            sample = list(self.object_index.items())[:5]
            self._log(f"[DISCOVERY] Sample: {sample}")

    @classmethod
    def _unwrap_list_result(cls, raw: Any) -> list | None:
        """
        Pull a list of objects out of a possibly-wrapped CLI
        response. Accepts:
          * a bare list
          * a dict with a known wrapper key (value/objects/items/…)
          * a dict with exactly one list-valued field
        Returns None if no list can be identified.
        """
        if raw is None:
            return None
        if isinstance(raw, list):
            return raw
        if not isinstance(raw, dict):
            return None

        for key in cls._LIST_WRAPPER_KEYS:
            value = raw.get(key)
            if isinstance(value, list):
                return value

        list_values = [v for v in raw.values() if isinstance(v, list)]
        if len(list_values) == 1:
            return list_values[0]

        return None

    @classmethod
    def _extract_object_name(cls, obj: Any) -> str | None:
        """
        Return the technical name from a list-result item, checking
        top-level keys first and then common nested wrappers.
        """
        if not isinstance(obj, dict):
            return None

        for key in cls._NAME_KEYS:
            value = obj.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

        for wrapper in cls._NESTED_NAME_WRAPPERS:
            inner = obj.get(wrapper)
            if isinstance(inner, dict):
                for key in cls._NAME_KEYS:
                    value = inner.get(key)
                    if isinstance(value, str) and value.strip():
                        return value.strip()

        return None

    # ================================================================
    # RECURSIVE RESOLUTION
    # ================================================================

    def _resolve(
        self,
        object_name: str,
        object_type: str,
        depth: int,
        path: set[tuple[str, str, str]],
        space: str | None = None,
    ) -> dict:

        target_space = space or self.space
        indent = "  " * depth
        self._log(f"{indent}→ {object_type} [{target_space}]: {object_name}")

        node = {
            "name": object_name,
            "object_type": object_type,
            "space": target_space,
            "is_shared": (target_space.casefold() != self.space.casefold()),
            "is_boundary": False,
            "status": None,
            "dependencies": [],
            "candidate_cds_view": None,
            "cds_verification_status": "unverified",
            "cds_metadata": None,
        }

        if depth >= self.max_depth:
            node["status"] = "max_depth_reached"
            return node

        key = (target_space, object_type, object_name)

        if key in path:
            node["status"] = "cycle"
            return node

        if key in self.cache:
            return dict(self.cache[key])

        # ---- Lazy type resolution for unknowns -------------------
        preloaded_definition: dict | None = None

        if object_type == "unknown":
            resolved_type, resolved_space, preloaded_definition = (
                self._resolve_unknown_type(object_name, space=target_space)
            )

            if resolved_type:
                object_type = resolved_type
                node["object_type"] = resolved_type
                if resolved_space:
                    target_space = resolved_space
                    node["space"] = resolved_space
                    node["is_shared"] = (resolved_space.casefold() != self.space.casefold())
                self.object_index[(target_space, object_name)] = resolved_type
                self.object_index[object_name] = resolved_type
                key = (target_space, object_type, object_name)

                self._log(
                    f"   [LAZY TYPE RESOLVED] "
                    f"{target_space}.{object_name} -> {resolved_type}"
                )
            else:
                node["status"] = "boundary"
                node["is_boundary"] = True
                return node

        if object_type in self.BOUNDARY_TYPES:
            node["status"] = "boundary"
            node["is_boundary"] = True
            return node

        if preloaded_definition is not None:
            definition = preloaded_definition
        else:
            definition = self._read_definition(object_name, object_type, space=target_space)

        if definition is None:
            node["status"] = "unreadable"
            node["is_boundary"] = True
            self.cache[key] = node
            return dict(node)

        node["status"] = "resolved"
        node["definition"] = definition

        # Check sharing in definition
        sharing = definition.get("sharing")
        if isinstance(sharing, dict) and sharing:
            node["is_shared"] = True
            node["sharing"] = sharing

        metadata = self._extract_metadata(definition)
        if metadata:
            node["metadata"] = metadata

        details = self._extract_details(
            definition, object_type, object_name
        )
        for detail_key, detail_value in details.items():
            if detail_value:
                node[detail_key] = detail_value

        dependencies = self._extract_dependencies(
            definition=definition,
            object_type=object_type,
            current_object=object_name,
        )

        if object_type in ("local_table", "remote_table", "shared_remote_table") or not dependencies:
            node["is_boundary"] = True

        next_path = path | {key}

        for dependency in dependencies:
            child = self._resolve(
                object_name=dependency["name"],
                object_type=dependency["object_type"],
                depth=depth + 1,
                path=next_path,
                space=dependency.get("space"),
            )

            child["relationship"] = dependency["relationship"]
            node["dependencies"].append(child)

        self.cache[key] = node
        return dict(node)

    # ================================================================
    # READ DATASPHERE DEFINITION
    # ================================================================

    def _read_definition(
        self,
        object_name: str,
        object_type: str,
        space: str | None = None,
    ) -> dict | None:

        readers = {
            "view": ("read_view", "view"),
            "local_table": ("read_local_table", "local table"),
            "remote_table": ("read_remote_table", "remote table"),
            "analytic_model": ("read_analytic_model", "analytical model"),
            "fact_model": ("read_fact_model", "fact model"),
        }

        spec = readers.get(object_type)
        if not spec:
            return None

        method_name, label = spec
        reader = getattr(self.client, method_name, None)

        if reader is None:
            return None

        target_space = space or self.space

        try:
            self._log(f"   [READ] {label} in {target_space}: {object_name}")
            raw = reader(target_space, object_name)

            if not isinstance(raw, dict):
                return raw

            # The CLI returns queryBuilder JSON for views and
            # analytic models. Normalize to CSN before the
            # downstream extractors walk it.
            return self._normalize_definition(raw, object_name)

        except Exception as exc:
            self._log(f"   [READ FAILED] {object_type}: {object_name}")
            self._log(f"   {exc}")

        return None

    # ================================================================
    # QUERYBUILDER → CSN NORMALIZATION
    # ================================================================
    #
    # The Datasphere CLI returns queryBuilder JSON, not CSN. This is
    # a flat map of UUID → entry where entries are typed by
    # `classDefinition`:
    #
    #   sap.cdw.querybuilder.Element       → a field
    #   sap.cdw.commonmodel.Parameter      → a parameter
    #   sap.cdw.querybuilder.ui.*          → UI symbols (ignored)
    #   sap.cdw.commonmodel.ElementMapping → mappings (ignored)
    #
    # The resolver was written against CSN, which nests fields under
    # definitions[name].elements. This normalizer bridges the two
    # shapes so nothing downstream has to change.
    # ----------------------------------------------------------------

    @classmethod
    def _normalize_definition(
        cls,
        definition: dict,
        object_name: str,
    ) -> dict:
        """
        Convert queryBuilder JSON into CSN shape. If the definition
        is already CSN, return it unchanged.
        """
        if not isinstance(definition, dict):
            return definition

        # Already CSN: has definitions[name].elements or .query
        definitions = definition.get("definitions")
        if isinstance(definitions, dict):
            for body in definitions.values():
                if isinstance(body, dict) and (
                    "elements" in body or "query" in body
                ):
                    return definition

        elements: dict = {}
        parameters: dict = {}

        cls._collect_querybuilder_entries(
            definition, elements, parameters, depth=0,
        )

        if not elements and not parameters:
            # Nothing to normalize — return unchanged so the resolver
            # can still attempt its default handling.
            return definition

        normalized = {
            "definitions": {
                object_name: {
                    "kind": "entity",
                    "elements": elements,
                },
            },
            "parameters": parameters,
        }
        source_names = cls._source_name_candidates(definition)
        if source_names:
            normalized["source_names"] = source_names
        return normalized

    @staticmethod
    def _source_name_candidates(value: Any) -> list[str]:
        source_keys = {
            "sourceentity",
            "sourceobject",
            "remoteobject",
            "remotesourceentity",
            "sourcecdsview",
            "cdsviewname",
            "sourcename",
            "remoteobjectname",
            "dataentity",
            "datasource",
        }
        name_keys = {"name", "technicalname", "entityname", "objectname", "key"}
        candidates: list[str] = []

        def add(candidate: Any) -> None:
            if isinstance(candidate, str) and candidate.strip():
                candidates.append(candidate.strip())

        def visit(node: Any) -> None:
            if isinstance(node, dict):
                for key, child in node.items():
                    normalized_key = re.sub(r"[^a-z]", "", str(key).lower())
                    if normalized_key in source_keys:
                        if isinstance(child, str):
                            add(child)
                        elif isinstance(child, dict):
                            for nested_key, nested_value in child.items():
                                if re.sub(r"[^a-z]", "", str(nested_key).lower()) in name_keys:
                                    add(nested_value)
                        elif isinstance(child, list):
                            for item in child:
                                visit(item)
                    visit(child)
            elif isinstance(node, list):
                for child in node:
                    visit(child)

        visit(value)
        return list(dict.fromkeys(candidates))

    @classmethod
    def _collect_querybuilder_entries(
        cls,
        node: Any,
        elements: dict,
        parameters: dict,
        depth: int,
    ) -> None:
        """
        Recursively scan the JSON. Whenever a node has a
        `classDefinition`, convert it into a CSN entry and add it.
        """
        if depth > 25:
            return

        if isinstance(node, dict):
            class_def = node.get("classDefinition")

            if isinstance(class_def, str):
                # --- Field ---
                if "querybuilder.Element" in class_def:
                    name = node.get("newName") or node.get("name")
                    if isinstance(name, str) and name:
                        element = cls._build_csn_element(node, name)
                        existing = elements.get(name)
                        if existing is None or len(element) > len(existing):
                            elements[name] = element
                    return

                # --- Parameter ---
                if "commonmodel.Parameter" in class_def:
                    name = node.get("name")
                    if isinstance(name, str) and name:
                        parameters[name] = {
                            "name": name,
                            "type": node.get("dataType") or "cds.String",
                            "@EndUserText.label": (
                                node.get("label") or name
                            ),
                        }
                    return

                # --- Skip UI symbols, mappings, hierarchies ---
                if (
                    "ui." in class_def
                    or "ElementMapping" in class_def
                    or "ReferenceEntity" in class_def
                    or "LevelBasedHierarchy" in class_def
                    or "LevelElement" in class_def
                ):
                    return

            # Recurse into dict values
            for value in node.values():
                cls._collect_querybuilder_entries(
                    value, elements, parameters, depth + 1,
                )

        elif isinstance(node, list):
            for item in node:
                cls._collect_querybuilder_entries(
                    item, elements, parameters, depth + 1,
                )

    @staticmethod
    def _build_csn_element(entry: dict, name: str) -> dict:
        """
        Build a CSN-shaped element dict from a queryBuilder entry.
        Only emits keys that the downstream extractors actually read.
        """
        element: dict = {
            "type": entry.get("dataType") or "cds.String",
        }

        label = entry.get("label")
        if isinstance(label, str) and label and label != name:
            element["@EndUserText.label"] = label

        if entry.get("isKey"):
            element["key"] = True
            element["notNull"] = True

        if entry.get("isMeasure"):
            element["@AnalyticsDetails.measureType"] = {"#": "BASE"}

        agg = entry.get("defaultAggregation")
        if isinstance(agg, str) and agg:
            element["@Aggregation.default"] = {"#": agg}

        # Detect a real calculated column: the expression differs
        # from the field's own name. Passthrough expressions are
        # ignored so they do not show up as calculated columns.
        expression = entry.get("expression")
        if isinstance(expression, str) and expression:
            original = entry.get("name") or name
            if expression.strip() not in (name, original):
                element["expression"] = expression

        for key in ("length", "precision", "scale"):
            value = entry.get(key)
            if value:
                element[key] = value

        return element

    # ================================================================
    # DETAIL EXTRACTION
    # ================================================================

    def _extract_details(
        self,
        definition: dict,
        object_type: str,
        current_object: str,
    ) -> dict:

        details: dict[str, list] = {
            "joins": [],
            "filters": [],
            "associations": [],
            "calculated_columns": [],
            "formulas": [],
            "field_catalog": [],
            "parameters": [],
        }

        if not isinstance(definition, dict):
            return {}

        targets = self._definition_targets(definition, current_object)

        if object_type == "analytic_model":
            business_layer = definition.get("businessLayerDefinitions")
            if isinstance(business_layer, dict):
                for model in business_layer.values():
                    if isinstance(model, dict):
                        targets.append(model)

        for target in targets:
            self._extract_field_catalog(target, details)

        self._extract_parameters(definition, details)

        for target in targets:
            self._walk_details(target, details, ())

        self._sync_calculated_columns_to_field_catalog(current_object, details)

        for key, items in details.items():
            sig_fn = self._DETAIL_SIGNATURES.get(key)
            if sig_fn is None or not items:
                continue

            seen = set()
            unique = []

            for item in items:
                try:
                    sig = sig_fn(item)
                except Exception:
                    sig = repr(item)

                if sig in seen:
                    continue
                seen.add(sig)
                unique.append(item)

            details[key] = unique

        return {k: v for k, v in details.items() if v}

    @staticmethod
    def _extract_referenced_columns(expr: Any) -> list[str]:
        refs: list[str] = []

        def walk(n):
            if isinstance(n, list):
                for x in n:
                    walk(x)
            elif isinstance(n, dict):
                ref = n.get("ref")
                if isinstance(ref, list) and ref:
                    col = str(ref[-1])
                    if col and not col.startswith(("$", "_", ":")):
                        refs.append(col)
                for k, v in n.items():
                    if k != "ref":
                        walk(v)

        walk(expr)
        return list(dict.fromkeys(refs))

    def _sync_calculated_columns_to_field_catalog(
        self,
        current_object: str,
        details: dict,
    ):
        calcs = details.get("calculated_columns", [])
        field_catalog = details.setdefault("field_catalog", [])
        catalog_by_name = {
            str(f.get("name", "")).casefold(): f
            for f in field_catalog
            if isinstance(f, dict) and f.get("name")
        }

        for calc in calcs:
            if not isinstance(calc, dict):
                continue
            name = str(calc.get("name") or "")
            if not name:
                continue
            name_key = name.casefold()
            calc["owning_object"] = calc.get("owning_object") or current_object
            if name_key in catalog_by_name:
                field = catalog_by_name[name_key]
                field["is_calculated"] = True
                field["calculation"] = calc.get("expression")
                field["referenced_columns"] = calc.get("referenced_columns", [])
                field["source_object"] = calc["owning_object"]
                field["transformation_category"] = calc.get("transformation_category", "calculated_column")
            else:
                new_field = {
                    "name": name,
                    "element_type": calc.get("data_type") or "unknown",
                    "category": "calculated",
                    "is_key": False,
                    "is_calculated": True,
                    "calculation": calc.get("expression"),
                    "referenced_columns": calc.get("referenced_columns", []),
                    "source_object": calc["owning_object"],
                    "transformation_category": calc.get("transformation_category", "calculated_column"),
                }
                field_catalog.append(new_field)
                catalog_by_name[name_key] = new_field

    @classmethod
    def propagate_calculated_columns(
        cls,
        node: dict,
        visited: set[str] | None = None,
    ) -> dict[str, dict]:
        """
        Propagates calculated columns bottom-up through the dependency graph.
        Upstream view calculations are propagated to downstream views and analytical models,
        including renamed projections and alias mappings.
        Returns a dictionary mapping column_name (casefold) -> calculated column metadata.
        """
        if not isinstance(node, dict):
            return {}
        if visited is None:
            visited = set()
        node_name = node.get("name")
        if node_name and node_name in visited:
            return {}
        if node_name:
            visited.add(node_name)

        # 1. Recurse into dependencies bottom-up
        upstream_calcs: dict[str, dict] = {}
        for dep in node.get("dependencies", []):
            if isinstance(dep, dict):
                dep_calcs = cls.propagate_calculated_columns(dep, visited)
                for k, v in dep_calcs.items():
                    if k not in upstream_calcs:
                        upstream_calcs[k] = v

        # 2. Collect this node's own calculations
        node_calcs: dict[str, dict] = {}
        for c in node.get("calculated_columns", []):
            if isinstance(c, dict) and c.get("name"):
                node_calcs[str(c["name"]).casefold()] = c

        # 3. Collect aliases/renames from node definition
        alias_map: dict[str, str] = {}
        def find_aliases(d):
            if isinstance(d, dict):
                cols = d.get("columns") or (d.get("SELECT", {}).get("columns") if isinstance(d.get("SELECT"), dict) else None)
                if isinstance(cols, list):
                    for col in cols:
                        if isinstance(col, dict) and col.get("as") and col.get("ref"):
                            ref_list = col["ref"]
                            if isinstance(ref_list, list) and ref_list:
                                source_col = str(ref_list[-1])
                                alias_map[str(col["as"]).casefold()] = source_col.casefold()
                for v in d.values():
                    find_aliases(v)
            elif isinstance(d, list):
                for item in d:
                    find_aliases(item)

        find_aliases(node.get("definition", {}))

        # 4. For any field in this node's field_catalog:
        field_catalog = node.setdefault("field_catalog", [])
        if isinstance(field_catalog, list):
            for field in field_catalog:
                if not isinstance(field, dict):
                    continue
                f_name = str(field.get("name") or "")
                if not f_name:
                    continue
                f_key = f_name.casefold()

                if f_key in node_calcs:
                    local_c = node_calcs[f_key]
                    field["is_calculated"] = True
                    field["calculation"] = local_c.get("expression")
                    field["referenced_columns"] = local_c.get("referenced_columns", [])
                    field["source_object"] = local_c.get("owning_object", node_name)
                    field["transformation_category"] = local_c.get("transformation_category", "calculated_column")
                elif f_key in upstream_calcs:
                    up_c = upstream_calcs[f_key]
                    field["is_calculated"] = True
                    field["calculation"] = up_c.get("expression")
                    field["referenced_columns"] = up_c.get("referenced_columns", [])
                    field["source_object"] = up_c.get("owning_object")
                    field["transformation_category"] = up_c.get("transformation_category", "calculated_column")

                    if not any(str(c.get("name", "")).casefold() == f_key for c in node.get("calculated_columns", [])):
                        inherited_entry = dict(up_c)
                        inherited_entry["propagated_to"] = node_name
                        node.setdefault("calculated_columns", []).append(inherited_entry)
                        node_calcs[f_key] = inherited_entry
                elif f_key in alias_map and alias_map[f_key] in upstream_calcs:
                    source_key = alias_map[f_key]
                    up_c = upstream_calcs[source_key]
                    field["is_calculated"] = True
                    field["calculation"] = up_c.get("expression")
                    field["referenced_columns"] = up_c.get("referenced_columns", [])
                    field["source_object"] = up_c.get("owning_object")
                    field["transformation_category"] = "renamed_calculated_column"

                    if not any(str(c.get("name", "")).casefold() == f_key for c in node.get("calculated_columns", [])):
                        inherited_entry = dict(up_c)
                        inherited_entry["name"] = f_name
                        inherited_entry["original_name"] = up_c.get("name")
                        inherited_entry["transformation_category"] = "renamed_calculated_column"
                        inherited_entry["propagated_to"] = node_name
                        node.setdefault("calculated_columns", []).append(inherited_entry)
                        node_calcs[f_key] = inherited_entry

        # Also register renamed calculations from alias_map even if not pre-populated in field_catalog
        for alias_key, source_key in alias_map.items():
            if alias_key not in node_calcs and source_key in upstream_calcs:
                up_c = upstream_calcs[source_key]
                inherited_entry = dict(up_c)
                inherited_entry["name"] = alias_key
                inherited_entry["original_name"] = up_c.get("name")
                inherited_entry["transformation_category"] = "renamed_calculated_column"
                inherited_entry["propagated_to"] = node_name
                node.setdefault("calculated_columns", []).append(inherited_entry)
                node_calcs[alias_key] = inherited_entry

        combined = dict(upstream_calcs)
        combined.update(node_calcs)
        return combined

    @staticmethod
    def _definition_targets(
        definition: dict,
        current_object: str,
    ) -> list[dict]:

        definitions = definition.get("definitions")

        if not isinstance(definitions, dict):
            return [definition]

        current = definitions.get(current_object)

        if isinstance(current, dict):
            return [current]

        if len(definitions) == 1:
            only = next(iter(definitions.values()))
            return [only] if isinstance(only, dict) else [definition]

        return [d for d in definitions.values() if isinstance(d, dict)]

    # ================================================================
    # FIELD CATALOG
    # ================================================================

    def _extract_field_catalog(
        self,
        target: dict,
        details: dict,
    ):

        if not isinstance(target, dict):
            return

        containers = (
            "elements", "columns", "measures", "dimensions",
            "calculatedMeasures", "calculatedDimensions",
            "calculatedAttributes", "restrictedMeasures", "attributes",
        )
        for container_name in containers:
            container = target.get(container_name)
            if isinstance(container, dict):
                entries = container.items()
            elif isinstance(container, list):
                entries = (
                    (
                        str(item.get("name") or item.get("technicalName") or item.get("key") or ""),
                        item,
                    )
                    for item in container
                    if isinstance(item, dict)
                )
            else:
                continue

            for name, element in entries:
                if not isinstance(element, dict) or not name:
                    continue
                if element.get("type") in self._ASSOCIATION_TYPES:
                    continue
                if element.get("type") in ("cds.Parameter", "Parameter"):
                    continue

                entry: dict = {
                    "name": str(element.get("name") or name),
                    "element_type": element.get("type") or "unknown",
                    "category": container_name,
                    "is_key": self._detect_key(element, str(name)),
                }

                for ann_key, short in self._FIELD_ANNOTATION_MAP.items():
                    if ann_key in element:
                        value = element[ann_key]
                        if isinstance(value, dict) and "#" in value:
                            value = value["#"]
                        entry[short] = value

                omk = entry.pop("object_model_key", None)
                if omk is True or omk == "true" or omk == "TRUE":
                    entry["is_key"] = True

                for num_key in ("precision", "scale", "length"):
                    if num_key in element:
                        entry[num_key] = element[num_key]

                expr_raw = next(
                    (
                        element.get(key)
                        for key in (
                            "expression", "xpr", "formula", "calculation",
                            "calculatedExpression", "calculationDefinition",
                        )
                        if element.get(key) is not None
                    ),
                    None,
                )
                if expr_raw is not None:
                    entry["is_calculated"] = True
                    entry["calculation"] = self._render_safe(expr_raw)

                details["field_catalog"].append(entry)

    @staticmethod
    def _detect_key(
        element: dict,
        name: str | None = None,
    ) -> bool:
        """
        Return True if the Datasphere element is marked as a key.
        Reads every known spelling; does not guess from the name.
        """

        for flag in ("key", "isKey", "is_key"):
            value = element.get(flag)
            if value is True:
                return True
            if isinstance(value, str) and value.lower() in {"true", "yes"}:
                return True

        ann = element.get("@ObjectModel.key")
        if isinstance(ann, dict):
            inner = ann.get("#")
            if inner is True:
                return True
            if isinstance(inner, str) and inner.lower() in {"true", "yes"}:
                return True
        elif ann is True:
            return True

        keys = element.get("keys")
        if isinstance(keys, list) and name and name in keys:
            return True

        if isinstance(element.get("key"), dict):
            return True

        return False

    # ================================================================
    # PARAMETERS
    # ================================================================

    def _extract_parameters(
        self,
        definition: dict,
        details: dict,
    ):

        if not isinstance(definition, dict):
            return

        declared: dict[str, dict] = {}
        used_at: dict[str, str] = {}

        def walk(node, path):
            if isinstance(node, list):
                for i, item in enumerate(node):
                    walk(item, path + (i,))
                return
            if not isinstance(node, dict):
                return

            ref = node.get("ref")
            if isinstance(ref, list) and node.get("param") is True:
                name = ref[0] if ref and isinstance(ref[0], str) else None
                if name:
                    used_at.setdefault(name, self._path_str(path))

            if node.get("type") in ("cds.Parameter", "Parameter"):
                name = node.get("name") or node.get("key")
                if isinstance(name, str):
                    declared[name] = self._parameter_entry(name, node)

            for k, v in node.items():
                walk(v, path + (k,))

        walk(definition, ())

        params_section = definition.get("parameters")
        if isinstance(params_section, dict):
            for name, p in params_section.items():
                if isinstance(p, dict):
                    declared[name] = self._parameter_entry(name, p)
                elif isinstance(p, str):
                    declared[name] = {
                        "name": name,
                        "data_type": p,
                    }

        all_names = set(declared) | set(used_at)

        for name in sorted(all_names):
            entry = declared.get(name, {"name": name})
            if name in used_at:
                entry["first_used_at"] = used_at[name]
            details["parameters"].append(entry)

    @staticmethod
    def _parameter_entry(name: str, node: dict) -> dict:
        entry: dict = {
            "name": name,
            "data_type": node.get("type"),
        }

        label = node.get("@EndUserText.label")
        if label:
            entry["label"] = label

        if "default" in node:
            entry["default"] = node["default"]

        if "mandatory" in node:
            entry["mandatory"] = node["mandatory"]

        return entry

    # ================================================================
    # DETAIL WALKER
    # ================================================================

    def _walk_details(
        self,
        node: Any,
        details: dict[str, list],
        path: tuple,
    ):

        if isinstance(node, list):
            for i, item in enumerate(node):
                self._walk_details(item, details, path + (i,))
            return

        if not isinstance(node, dict):
            return

        raw_type = node.get("type") or node.get("kind") or ""
        node_type = raw_type if isinstance(raw_type, str) else ""

        # ---- ASSOCIATION ----
        target = node.get("target")
        looks_like_assoc = (
            node_type in self._ASSOCIATION_TYPES
            or (
                isinstance(target, str)
                and "on" in node
                and "cardinality" in node
            )
        )

        if looks_like_assoc and isinstance(target, str) and target:
            on_raw = node.get("on")

            details["associations"].append({
                "path": self._path_str(path),
                "name": (
                    node.get("name")
                    or node.get("as")
                    or self._leaf_name(path)
                ),
                "association_type": node_type or "cds.Association",
                "target": self._normalize_name(target),
                "cardinality": self._normalize_cardinality(
                    node.get("cardinality")
                    or node.get("$cardinality")
                ),
                "on": self._render_safe(on_raw),
                "on_raw": on_raw,
            })
            return

        # ---- JOIN ----
        join = self._extract_join(node, path)

        if join is not None:
            details["joins"].append(join)

            where = node.get("where")
            if isinstance(where, list) and where:
                details["filters"].append({
                    "location": f"{self._path_str(path)}.where",
                    "condition": self._render_safe(where),
                    "condition_raw": where,
                })

            for key, value in node.items():
                if key in self._JOIN_SIDE_KEYS:
                    continue
                if key == "where":
                    continue
                if isinstance(value, (dict, list)):
                    self._walk_details(value, details, path + (key,))
            return

        # ---- CALCULATED COLUMN / EXPRESSION ----
        # Distinguish standard measure aggregations (e.g. SUM(col)) from calculated column formulas
        is_simple_agg = False
        if "func" in node and isinstance(node.get("args"), list) and len(node["args"]) == 1:
            if str(node.get("func", "")).upper() in ("SUM", "COUNT", "AVG", "MIN", "MAX"):
                first_arg = node["args"][0]
                if isinstance(first_arg, dict) and "ref" in first_arg:
                    is_simple_agg = True
                    agg_name = node.get("name") or node.get("as") or self._leaf_name(path)
                    for f in details.get("field_catalog", []):
                        if isinstance(f, dict) and str(f.get("name", "")).casefold() == str(agg_name).casefold():
                            f["aggregation"] = str(node.get("func", "")).upper()

        has_func = ("func" in node and "args" in node) and not is_simple_agg
        has_constant = (
            "val" in node
            and ("as" in node or "name" in node)
            and "ref" not in node
            and not isinstance(node.get("val"), (dict, list))
        )
        expression_raw = (
            node.get("expression")
            or node.get("xpr")
            or (node if has_func else None)
            or (node.get("val") if has_constant else None)
        )

        is_calc_column = (
            expression_raw is not None
            and (
                "expression" in node
                or "xpr" in node
                or has_func
                or has_constant
                or (
                    isinstance(node_type, str)
                    and node_type.startswith("cds.")
                )
            )
        )

        if is_calc_column:
            expr_str = self._render_safe(expression_raw)

            if expr_str:
                calc_name = (
                    node.get("name")
                    or node.get("as")
                    or self._leaf_name(path)
                )
                category = "calculated_column"
                if "xpr" in node and isinstance(node["xpr"], list) and any(str(x).lower() == "case" for x in node["xpr"]):
                    category = "case_expression"
                elif has_func:
                    category = "function"
                elif has_constant:
                    category = "constant"

                input_cols = self._extract_referenced_columns(expression_raw)
                details["calculated_columns"].append({
                    "path": self._path_str(path),
                    "name": calc_name,
                    "expression": expr_str,
                    "data_type": node_type or "unknown",
                    "label": node.get("@EndUserText.label"),
                    "transformation_category": category,
                    "referenced_columns": input_cols,
                })

        # ---- FORMULA ----
        formula_raw = (
            node.get("formula")
            or node.get("calculation")
            or node.get("@AnalyticsDetails.query.formula")
        )

        if formula_raw:
            formula_str = self._render_safe(formula_raw)

            if formula_str:
                restriction_raw = node.get("filter")

                details["formulas"].append({
                    "path": self._path_str(path),
                    "name": (
                        node.get("name")
                        or node.get("as")
                        or self._leaf_name(path)
                    ),
                    "formula": formula_str,
                    "kind": node_type or None,
                    "label": node.get("@EndUserText.label"),
                    "restriction": self._render_safe(restriction_raw),
                    "restriction_raw": restriction_raw,
                })

        # ---- FILTER ----
        where = node.get("where")

        if isinstance(where, list) and where:
            details["filters"].append({
                "location": f"{self._path_str(path)}.where",
                "condition": self._render_safe(where),
                "condition_raw": where,
            })

        # ---- RECURSE ----
        for key, value in node.items():
            if key == "where":
                continue
            if isinstance(value, (dict, list)):
                self._walk_details(value, details, path + (key,))

    # ----------------------------------------------------------------

    def _render_safe(self, raw: Any) -> str | None:
        if raw is None:
            return None
        try:
            return self._render_expression(raw)
        except Exception:
            return str(raw)

    @staticmethod
    def _render_expression(raw: Any) -> str | None:

        if raw is None:
            return None

        if isinstance(raw, str):
            return raw.strip() or None

        if isinstance(raw, list):
            tokens: list[str] = []
            for token in raw:
                if isinstance(token, dict):
                    if "ref" in token and isinstance(token["ref"], list):
                        tokens.append(".".join(str(x) for x in token["ref"]))
                    elif "val" in token:
                        val = token["val"]
                        tokens.append(
                            f"'{val}'" if isinstance(val, str) else str(val)
                        )
                    elif "func" in token:
                        args = token.get("args") or []
                        inner = ", ".join(
                            DatasphereDependencyResolver._render_expression([a])
                            or ""
                            for a in args
                        )
                        tokens.append(f"{token['func']}({inner})")
                    elif "xpr" in token and isinstance(token["xpr"], list):
                        inner = DatasphereDependencyResolver._render_expression(
                            token["xpr"]
                        )
                        tokens.append(f"({inner})" if inner else "")
                    elif len(token) == 1 and next(iter(token)) in {
                        "+", "-", "*", "/", "=", "<>", "<", ">", "<=", ">="
                    }:
                        op = next(iter(token))
                        payload = token[op]
                        inner = DatasphereDependencyResolver._render_expression(
                            payload if isinstance(payload, list) else [payload]
                        )
                        tokens.append(f"({inner})" if inner else "")
                    else:
                        tokens.append(str(token))
                else:
                    tokens.append(str(token))
            rendered = " ".join(t for t in tokens if t)
            return rendered or None

        if isinstance(raw, dict):
            return DatasphereDependencyResolver._render_expression([raw])

        return str(raw)

    # ================================================================
    # JOIN EXTRACTION
    # ================================================================

    def _extract_join(
        self,
        node: dict,
        path: tuple,
    ) -> dict | None:

        join_type_raw = (
            node.get("joinType")
            or node.get("join_type")
            or node.get("join")
        )

        if not join_type_raw and path:
            last_segment = str(path[-1])
            if "JOIN" in last_segment.upper():
                join_type_raw = last_segment

        node_type = node.get("type") or node.get("kind") or ""
        node_type = node_type if isinstance(node_type, str) else ""

        left_raw = node.get("left") or node.get("leftSource")
        right_raw = node.get("right") or node.get("rightSource")

        args = node.get("args")

        if (
            left_raw is None
            and right_raw is None
            and isinstance(args, list)
            and len(args) >= 2
        ):
            args_join_signal = (
                join_type_raw is not None
                or node_type in self._JOIN_NODE_TYPES
                or "on" in node
                or "condition" in node
            )
            if args_join_signal:
                left_raw, right_raw = args[0], args[1]

        is_join = (
            join_type_raw is not None
            or node_type in self._JOIN_NODE_TYPES
            or (left_raw is not None and right_raw is not None)
            or ("on" in node and (left_raw is not None or right_raw is not None))
        )

        if not is_join:
            return None

        if left_raw is None and right_raw is None:
            return None

        on_raw = node.get("on") or node.get("condition")

        return {
            "path": self._path_str(path),
            "join_type": self._normalize_join_type(
                join_type_raw or "JOIN"
            ),
            "left": self._extract_join_side(
                left_raw, path + ("left",)
            ),
            "right": self._extract_join_side(
                right_raw, path + ("right",)
            ),
            "on": self._render_safe(on_raw),
            "on_raw": on_raw,
            "cardinality": self._normalize_cardinality(
                node.get("cardinality")
            ),
        }

    def _extract_join_side(
        self,
        node: Any,
        path: tuple,
    ) -> dict | None:

        if node is None or not isinstance(node, dict):
            return None

        alias = node.get("as") or node.get("alias")

        nested = self._extract_join(node, path)
        if nested is not None:
            out: dict = {"nested_join": nested}
            if isinstance(alias, str) and alias:
                out["as"] = alias
            return out

        ref = node.get("ref")
        if isinstance(ref, list) and ref:
            name = self._object_name_from_ref(ref)
            if name:
                out = {"source": name}
                if isinstance(alias, str) and alias:
                    out["as"] = alias
                return out

        args = node.get("args")
        if isinstance(args, list):
            for arg in args:
                side = self._extract_join_side(arg, path + ("args",))
                if side:
                    if (
                        isinstance(alias, str)
                        and alias
                        and "as" not in side
                    ):
                        side["as"] = alias
                    return side

        if node.get("SET") or node.get("type") == "setOperation":
            out = {"set_operation": True}
            if isinstance(alias, str) and alias:
                out["as"] = alias
            return out

        return None

    @staticmethod
    def _path_str(path: tuple) -> str:
        if not path:
            return "root"
        return ".".join(str(p) for p in path)

    @classmethod
    def _leaf_name(cls, path: tuple) -> str | None:
        for p in reversed(path):
            if not isinstance(p, str):
                continue
            if p in cls._PATH_STRUCTURAL_SEGMENTS:
                continue
            return p
        return None

    @classmethod
    def _normalize_join_type(cls, raw: Any) -> str:
        if isinstance(raw, dict):
            raw = raw.get("type") or raw.get("kind") or str(raw)
        if not isinstance(raw, str):
            return str(raw)

        key = raw.strip().lower()
        return cls._JOIN_TYPE_ALIASES.get(key, raw.upper())

    @staticmethod
    def _normalize_cardinality(raw: Any) -> Any:
        if raw is None:
            return None

        if isinstance(raw, str):
            return raw

        if isinstance(raw, dict):
            max_v = raw.get("max")
            min_v = raw.get("min")

            if max_v in ("1", 1) and min_v in (None, "1", 1):
                return "to-one"
            if max_v in ("*", "n", "N"):
                return "to-many"

            return raw

        return raw

    # ================================================================
    # CDS ASSOCIATIONS (dependency form)
    # ================================================================

    def _extract_cds_associations(
        self,
        current_definition: dict,
    ) -> list[dict]:

        dependencies: list[dict] = []

        if not isinstance(current_definition, dict):
            return dependencies

        item_sources = [current_definition.items()]

        elements = current_definition.get("elements")
        if isinstance(elements, dict):
            item_sources.append(elements.items())

        for items in item_sources:
            for _key, value in items:
                if not isinstance(value, dict):
                    continue
                val_type = value.get("type")
                if not isinstance(val_type, str) or val_type not in self._ASSOCIATION_TYPES:
                    continue

                target = value.get("target")
                if isinstance(target, str) and target:
                    dependencies.append(
                        self._make_dependency(target, "association")
                    )

        return dependencies

    # ================================================================
    # DEPENDENCY EXTRACTION
    # ================================================================

    def _extract_dependencies(
        self,
        definition: dict,
        object_type: str,
        current_object: str,
    ) -> list[dict]:

        dependencies: list[dict] = []

        if object_type in ("local_table", "remote_table", "shared_remote_table"):
            return dependencies

        if object_type == "analytic_model":
            dependencies.extend(
                self._extract_analytic_model_dependencies(definition)
            )
        else:
            dependencies.extend(
                self._extract_query_dependencies(
                    definition, current_object
                )
            )

        return self._deduplicate_dependencies(dependencies)

    def _extract_analytic_model_dependencies(
        self,
        definition: dict,
    ) -> list[dict]:

        dependencies: list[dict] = []

        definitions = definition.get("definitions", {})
        if not isinstance(definitions, dict):
            return dependencies

        business_layer = definition.get("businessLayerDefinitions", {})
        if not isinstance(business_layer, dict):
            business_layer = {}

        for model_name, model in definitions.items():
            if not isinstance(model, dict):
                continue

            query = model.get("query")
            if isinstance(query, dict):
                for source in self._extract_from_sources(query):
                    dependencies.append(
                        self._make_dependency(
                            source, "analytic_model_source"
                        )
                    )

            business_model = business_layer.get(model_name, {})
            source_model = (
                business_model.get("sourceModel", {})
                if isinstance(business_model, dict)
                else {}
            )
            if not isinstance(source_model, dict):
                source_model = {}

            self._extract_data_entity_sources(
                source_model.get("factSources"),
                "fact_source",
                dependencies,
            )
            self._extract_data_entity_sources(
                source_model.get("dimensionSources"),
                "dimension_source",
                dependencies,
            )
            self._extract_data_entity_sources(
                model.get("factSources"),
                "fact_source",
                dependencies,
            )
            self._extract_data_entity_sources(
                model.get("dimensionSources"),
                "dimension_source",
                dependencies,
            )

            dependencies.extend(
                self._extract_cds_associations(model)
            )

            self._walk_for_explicit_entities(model, dependencies)

        return dependencies

    def _extract_data_entity_sources(
        self,
        sources: Any,
        relationship: str,
        dependencies: list[dict],
    ):

        if isinstance(sources, dict):
            iterable = sources.values()
        elif isinstance(sources, list):
            iterable = sources
        else:
            return

        for source in iterable:
            if not isinstance(source, dict):
                continue

            entity = source.get("dataEntity")
            name = None

            if isinstance(entity, dict):
                name = (
                    entity.get("key")
                    or entity.get("name")
                    or entity.get("technicalName")
                )
            elif isinstance(entity, str):
                name = entity

            if name:
                dependencies.append(
                    self._make_dependency(name, relationship)
                )

    def _extract_query_dependencies(
        self,
        definition: dict,
        current_object: str,
    ) -> list[dict]:

        dependencies: list[dict] = []

        definitions = definition.get("definitions", {})
        if not isinstance(definitions, dict):
            return dependencies

        current_definition = definitions.get(current_object)

        if (
            not isinstance(current_definition, dict)
            and len(definitions) == 1
        ):
            current_definition = next(iter(definitions.values()))

        if not isinstance(current_definition, dict):
            return dependencies

        query = current_definition.get("query")
        if isinstance(query, dict):
            for source in self._extract_from_sources(query):
                dependencies.append(
                    self._make_dependency(source, "query_source")
                )

        dependencies.extend(
            self._extract_cds_associations(current_definition)
        )

        self._walk_for_explicit_entities(
            current_definition, dependencies
        )

        return dependencies

    def _extract_from_sources(self, query: dict) -> list[str]:

        sources: list[str] = []

        select = query.get("SELECT")
        if isinstance(select, dict):
            self._walk_from_node(select.get("from"), sources)

        set_node = query.get("SET")
        if isinstance(set_node, dict):
            self._walk_from_node(set_node, sources)

        for key in ("from", "FROM", "source", "sources"):
            value = query.get(key)
            if isinstance(value, (dict, list)):
                self._walk_from_node(value, sources)

        return self._deduplicate_names(sources)

    def _walk_from_node(self, node: Any, sources: list[str]):

        if isinstance(node, list):
            for item in node:
                self._walk_from_node(item, sources)
            return

        if not isinstance(node, dict):
            return

        ref = node.get("ref")
        if isinstance(ref, list) and ref:
            name, space = self._parse_reference(ref)
            if name:
                full_name = f"{space}.{name}" if space else name
                sources.append(full_name)

        for key in ("args", "from", "SELECT", "SET", "left", "right"):
            child = node.get(key)
            if isinstance(child, (dict, list)):
                self._walk_from_node(child, sources)

    def _walk_for_explicit_entities(
        self,
        value: Any,
        dependencies: list[dict],
    ):

        if isinstance(value, dict):

            entity = value.get("dataEntity")

            if isinstance(entity, dict):
                name = (
                    entity.get("key")
                    or entity.get("name")
                    or entity.get("technicalName")
                )
                if name:
                    dependencies.append(
                        self._make_dependency(name, "data_entity")
                    )
            elif isinstance(entity, str):
                dependencies.append(
                    self._make_dependency(entity, "data_entity")
                )

            for key in (
                "source", "sourceEntity",
                "sourceObject", "dataSource",
            ):
                candidate = value.get(key)
                name = self._extract_name(candidate)
                if name:
                    dependencies.append(
                        self._make_dependency(name, key)
                    )

            for child_key, child in value.items():
                if child_key in self._SKIP_KEYS:
                    continue
                self._walk_for_explicit_entities(
                    child, dependencies
                )

        elif isinstance(value, list):
            for item in value:
                self._walk_for_explicit_entities(
                    item, dependencies
                )

    @staticmethod
    def _extract_name(value: Any) -> str | None:

        if isinstance(value, str):
            return value

        if isinstance(value, dict):
            for key in ("key", "name", "technicalName"):
                candidate = value.get(key)
                if isinstance(candidate, str):
                    return candidate

        return None

    @staticmethod
    def _object_name_from_ref(ref: list) -> str | None:

        if not ref:
            return None

        first = ref[0]

        if isinstance(first, dict):
            first = (
                first.get("id")
                or first.get("name")
                or first.get("technicalName")
            )

        if not isinstance(first, str):
            return None

        if first.startswith(("$", "_", ":")):
            return None

        if first.lower() in {"projection", "parameters"}:
            return None

        if first.upper().startswith("SAP."):
            return first

        # Two-part ref: ["SPACE", "OBJECT"]
        if len(ref) >= 2 and isinstance(ref[1], str) and not ref[1].startswith(("$", "_", ":")):
            second = ref[1]
            if "." not in first:
                return second, first

        # Single string with dot: "SPACE.OBJECT"
        if "." in first:
            parts = first.split(".", 1)
            return parts[1], parts[0]

        return first, None

    @classmethod
    def _parse_reference(cls, ref: list) -> tuple[str | None, str | None]:
        """
        Extract (object_name, space) from a ref list.
        Supports:
          - ["RAD_S3X.ZI_CUSTOMERRECEIVABLESKPI"] -> ("ZI_CUSTOMERRECEIVABLESKPI", "RAD_S3X")
          - ["RAD_S3X", "ZI_CUSTOMERRECEIVABLESKPI"] -> ("ZI_CUSTOMERRECEIVABLESKPI", "RAD_S3X")
          - ["ZI_CUSTOMERRECEIVABLESKPI"] -> ("ZI_CUSTOMERRECEIVABLESKPI", None)
          - ["SAP.TIME.YEAR"] -> ("SAP.TIME.YEAR", None)
        """
        if not ref:
            return None, None

        first = ref[0]

        if isinstance(first, dict):
            first = (
                first.get("id")
                or first.get("name")
                or first.get("technicalName")
            )

        if not isinstance(first, str):
            return None, None

        if first.startswith(("$", "_", ":")):
            return None, None

        if first.lower() in {"projection", "parameters"}:
            return None, None

        if first.upper().startswith("SAP."):
            return first, None

        # Two-part ref: ["SPACE", "OBJECT"]
        if len(ref) >= 2 and isinstance(ref[1], str) and not ref[1].startswith(("$", "_", ":")):
            second = ref[1]
            if "." not in first:
                return second, first

        # Single string with dot: "SPACE.OBJECT"
        if "." in first:
            parts = first.split(".", 1)
            return parts[1], parts[0]

        return first, None

    @classmethod
    def _object_name_from_ref(cls, ref: list) -> str | None:
        name, _ = cls._parse_reference(ref)
        return name

    def _classify_object(self, object_name: str, space: str | None = None) -> str:

        if object_name.upper().startswith("SAP."):
            return "system"

        target_space = space or self.space
        # Check space-qualified index first: (space, name)
        discovered = self.object_index.get((target_space, object_name))
        if discovered:
            return discovered

        discovered = self.object_index.get(object_name)
        if discovered:
            return discovered

        return "unknown"

    def _resolve_unknown_type(
        self,
        object_name: str,
        space: str | None = None,
    ) -> tuple[str | None, str | None, dict | None]:
        """
        Probe readable object types for `object_name` across candidate spaces.
        Candidate spaces: [space, self.space, self.replication_space] + client spaces.
        Returns (object_type, resolved_space, normalized_definition) on success,
        or (None, None, None) if not found.
        """
        cache_key = (space or self.space, object_name)
        if cache_key in self.unresolved_types or object_name in self.unresolved_types:
            self._log(f"   [TYPE CACHED-UNRESOLVED] {cache_key}")
            return None, None, None

        self._log(f"   [TYPE RESOLUTION] {object_name} (preferred space: {space or self.space})")

        candidate_spaces: list[str] = []
        if space and space not in candidate_spaces:
            candidate_spaces.append(space)
        if self.space and self.space not in candidate_spaces:
            candidate_spaces.append(self.space)
        if self.replication_space and self.replication_space not in candidate_spaces:
            candidate_spaces.append(self.replication_space)

        if hasattr(self.client, "list_spaces"):
            try:
                for sp in self.client.list_spaces():
                    sp_name = (
                        sp.get("space") or sp.get("spaceId") or sp.get("name")
                        if isinstance(sp, dict) else str(sp)
                    )
                    if sp_name and sp_name not in candidate_spaces:
                        candidate_spaces.append(sp_name)
            except Exception:
                pass

        candidates = [
            ("local_table", self.client.read_local_table),
            ("view", self.client.read_view),
            ("remote_table", getattr(self.client, "read_remote_table", None)),
            ("analytic_model", self.client.read_analytic_model),
            ("fact_model", self.client.read_fact_model),
        ]

        def try_reader(spec, target_sp):
            object_type, reader = spec
            if reader is None:
                return object_type, target_sp, None
            try:
                self._log(f"   [TYPE CHECK] {object_type} in {target_sp}: {object_name}")
                definition = reader(target_sp, object_name)
                if isinstance(definition, dict):
                    normalized = self._normalize_definition(
                        definition, object_name
                    )
                    return object_type, target_sp, normalized
                return object_type, target_sp, None
            except Exception:
                self._log(
                    f"   [TYPE CHECK FAILED] "
                    f"{object_type} in {target_sp}: {object_name}"
                )
                return object_type, target_sp, None

        for sp in candidate_spaces:
            try:
                with ThreadPoolExecutor(max_workers=len(candidates)) as pool:
                    futures = [
                        pool.submit(try_reader, spec, sp)
                        for spec in candidates
                    ]
                    for future in futures:
                        object_type, found_sp, definition = future.result()
                        if definition is not None:
                            self._log(
                                f"   [TYPE RESOLVED] "
                                f"{found_sp}.{object_name} -> {object_type}"
                            )
                            self.object_index[(found_sp, object_name)] = object_type
                            self.object_index[object_name] = object_type
                            return object_type, found_sp, definition
            except Exception:
                for spec in candidates:
                    object_type, found_sp, definition = try_reader(spec, sp)
                    if definition is not None:
                        self._log(
                            f"   [TYPE RESOLVED] "
                            f"{found_sp}.{object_name} -> {object_type}"
                        )
                        self.object_index[(found_sp, object_name)] = object_type
                        self.object_index[object_name] = object_type
                        return object_type, found_sp, definition

        self._log(f"   [TYPE UNRESOLVED] {object_name}")
        self.unresolved_types.add(cache_key)
        self.unresolved_types.add(object_name)
        return None, None, None

    def _make_dependency(
        self,
        object_name: str,
        relationship: str,
        space: str | None = None,
    ) -> dict:

        if "." in object_name and not object_name.upper().startswith("SAP."):
            extracted_space, clean_name = object_name.split(".", 1)
            space = space or extracted_space
            object_name = clean_name
        else:
            object_name = self._normalize_name(object_name)

        target_space = space or self.space

        return {
            "name": object_name,
            "object_type": self._classify_object(object_name, space=target_space),
            "relationship": relationship,
            "space": target_space,
        }

    @staticmethod
    def _normalize_name(name: str) -> str:

        if name.upper().startswith("SAP."):
            return name

        if "." in name:
            return name.split(".")[-1]

        return name

    def _extract_metadata(self, definition: dict) -> dict:

        metadata: dict = {}

        definitions = definition.get("definitions")
        if not isinstance(definitions, dict):
            return metadata

        for name, value in definitions.items():
            if not isinstance(value, dict):
                continue

            entry: dict = {}

            for key in (
                "kind",
                "@EndUserText.label",
                "@ObjectModel.modelingPattern",
                "@DataWarehouse.editorType",
            ):
                if key in value:
                    entry[key] = value[key]

            if entry:
                metadata[name] = entry

        return metadata

    @staticmethod
    def _deduplicate_names(names: list[str]) -> list[str]:

        seen: set[str] = set()
        result: list[str] = []

        for name in names:
            if name in seen:
                continue
            seen.add(name)
            result.append(name)

        return result

    @staticmethod
    def _deduplicate_dependencies(
        dependencies: list[dict],
    ) -> list[dict]:

        seen: set[tuple] = set()
        result: list[dict] = []

        for dep in dependencies:
            key = (dep["name"], dep["object_type"], dep.get("space"))
            if key in seen:
                continue
            seen.add(key)
            result.append(dep)

        return result


propagate_calculated_columns = DatasphereDependencyResolver.propagate_calculated_columns


# ====================================================================
# CLIENT FACTORY
# ====================================================================


def build_client(host: str, space: str):
    """
    Return a client instance. Wired up with DatasphereResolverClient.
    """
    configured_space = (space or os.getenv("DATASPHERE_MODELING_SPACE", "")).strip()
    if not configured_space:
        raise RuntimeError(
            "Datasphere space is not configured. Set DATASPHERE_MODELING_SPACE "
            "in backend/.env or pass a space explicitly."
        )
    try:
        from reconciliation.datasphere_resolver_client import DatasphereResolverClient
    except ImportError:
        from datasphere_resolver_client import DatasphereResolverClient
    return DatasphereResolverClient(space=configured_space)


# ====================================================================
# MAIN
# ====================================================================


def main(argv: list[str] | None = None) -> int:

    parser = argparse.ArgumentParser(
        prog="dependency_resolver",
        description=(
            "Resolve the dependency tree of one or more Datasphere "
            "objects (views, tables, analytical models, fact models)."
        ),
    )

    parser.add_argument(
        "names",
        nargs="+",
        help="One or more object names.",
    )

    parser.add_argument(
        "--type",
        default=None,
        choices=["view", "local_table", "analytic_model", "fact_model"],
        help="Force the object type for every name.",
    )

    parser.add_argument(
        "--space",
        default=os.environ.get("DWR_SPACE", ""),
        help="Datasphere space. Defaults to env DWR_SPACE.",
    )

    parser.add_argument(
        "--host",
        default=os.environ.get("DWR_HOST", ""),
        help="Datasphere host URL. Defaults to env DWR_HOST.",
    )

    parser.add_argument(
        "--max-depth",
        type=int,
        default=30,
        help="Maximum recursion depth (default: 30).",
    )

    parser.add_argument(
        "--out",
        default="dependency_trees.json",
        help="Where to write the JSON output (default: %(default)s).",
    )

    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress progress logging.",
    )

    args = parser.parse_args(argv)

    if not args.space:
        print(
            "ERROR: space is required (pass --space or set DWR_SPACE).",
            file=sys.stderr,
        )
        return 2

    try:
        client = build_client(host=args.host, space=args.space)
    except NotImplementedError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 3

    resolver = DatasphereDependencyResolver(
        client=client,
        space=args.space,
        max_depth=args.max_depth,
        silent=args.quiet,
    )

    def _progress(i: int, total: int, name: str):
        print(f"[{i}/{total}] {name}")

    results = resolver.resolve_many(
        names=args.names,
        object_type=args.type,
        on_progress=None if args.quiet else _progress,
    )

    for name, tree in results.items():
        if not args.quiet:
            print(f"\n===== DEPENDENCY TREE: {name} =====")
        print(json.dumps(tree, indent=2, default=str))

    out_path = Path(args.out)
    out_path.write_text(
        json.dumps(results, indent=2, default=str),
        encoding="utf-8",
    )
    print(f"\nSaved {len(results)} tree(s) -> {out_path.resolve()}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())