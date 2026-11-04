"""Strict Datasphere execution-target resolution and SQL compilation.

The analytical model is a metadata entry point, not an implicit HANA table.
This module only accepts a runtime SQL object when its identity and columns are
explicitly present in the resolved Datasphere metadata.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


class ExecutionTargetError(ValueError):
    """Raised when Datasphere metadata is insufficient for safe execution."""


class ReconciliationGrainError(ValueError):
    """Raised when no verified comparison grain can be established."""


def _quote_catalog_identifier(identifier: str) -> str:
    if not identifier or any(ord(char) < 32 for char in identifier):
        raise ExecutionTargetError("Verified HANA catalog identifier is empty or contains control characters.")
    return '"' + identifier.replace('"', '""') + '"'


def bind_readonly_sql_to_target(sql: str, logical_model: str, target: "ExecutionTarget") -> str:
    """Bind a single-source LLM SELECT to the verified fact-model runtime object."""
    statement = sql.strip().rstrip(";").strip()
    if not re.match(r"(?is)^SELECT\b", statement) or ";" in statement:
        raise ExecutionTargetError("LLM SQL must be one read-only SELECT statement.")
    if re.search(r"(?i)\b(INSERT|UPDATE|DELETE|MERGE|UPSERT|CREATE|ALTER|DROP|CALL|DO)\b", statement):
        raise ExecutionTargetError("LLM SQL contains a non-read-only operation.")

    source_pattern = re.compile(
        r'(?i)\b(FROM|JOIN)\s+'
        r'((?:"[^"]+"|[A-Za-z_][A-Za-z0-9_$]*)'
        r'(?:\s*\.\s*(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_$]*))?)'
        r'(\s*\([^)]*\))?'
    )
    matches = list(source_pattern.finditer(statement))
    if not matches:
        raise ExecutionTargetError("LLM SQL does not reference the verified fact model.")

    allowed_sources = {
        logical_model.casefold(),
        target.fact_model.casefold(),
        target.object_name.casefold(),
        f"{target.schema}.{target.object_name}".casefold(),
    }
    replacements = []
    verified_source = (
        f"{_quote_catalog_identifier(target.schema)}."
        f"{_quote_catalog_identifier(target.object_name)}"
    )
    for match in matches:
        source = match.group(2).replace('"', '').replace(" ", "").casefold()
        if source not in allowed_sources:
            raise ExecutionTargetError(
                f"LLM SQL references unverified source {match.group(2)!r}; only the resolved fact model is allowed."
            )
        replacements.append((match.start(2), match.end(2), verified_source))
    for start, end, replacement in reversed(replacements):
        statement = statement[:start] + replacement + statement[end:]
    return statement


@dataclass(frozen=True)
class ExecutionTarget:
    analytical_model: str
    analytical_model_space: str
    fact_model: str
    fact_model_space: str
    relationship: str
    object_type: str
    object_name: str
    schema: str
    columns: tuple[dict[str, Any], ...]
    lineage: tuple[str, ...]

    @property
    def column_names(self) -> set[str]:
        return {str(item["name"]).casefold() for item in self.columns}

    def as_dict(self) -> dict[str, Any]:
        return {
            "analytical_model": self.analytical_model,
            "analytical_model_space": self.analytical_model_space,
            "fact_model": self.fact_model,
            "fact_model_space": self.fact_model_space,
            "relationship": self.relationship,
            "object_type": self.object_type,
            "object_name": self.object_name,
            "schema": self.schema,
            "columns": [dict(item) for item in self.columns],
            "lineage": list(self.lineage),
        }


_RUNTIME_KEYS = (
    "runtime_object", "physical_object", "sql_object", "hana_object",
    "execution_object", "runtimeObject", "physicalObject", "sqlObject",
    "hanaObject", "executionObject",
)
_SCHEMA_KEYS = ("schema", "sql_schema", "hana_schema", "runtime_schema")


def _explicit_runtime_reference(node: dict[str, Any]) -> tuple[str | None, str | None]:
    """Read only explicit runtime identity; never derive it from display names."""
    values = [node]
    definition = node.get("definition")
    if isinstance(definition, dict):
        values.append(definition)
        definitions = definition.get("definitions")
        if isinstance(definitions, dict):
            values.extend(value for value in definitions.values() if isinstance(value, dict))

    object_name = None
    schema = None
    for value in values:
        if not isinstance(value, dict):
            continue
        for key in _RUNTIME_KEYS:
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                object_name = candidate.strip()
                break
            if isinstance(candidate, dict):
                object_name = str(candidate.get("name") or candidate.get("object") or "").strip() or object_name
                schema = str(candidate.get("schema") or "").strip() or schema
        for key in _SCHEMA_KEYS:
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                schema = candidate.strip()
    return object_name, schema


def _node_columns(node: dict[str, Any]) -> list[dict[str, Any]]:
    def projection_mappings(value: Any, result: dict[str, str] | None = None) -> dict[str, str]:
        """Collect only explicit output-to-input mappings from the object definition."""
        if result is None:
            result = {}
        if isinstance(value, list):
            for item in value:
                projection_mappings(item, result)
            return result
        if not isinstance(value, dict):
            return result

        output_name = value.get("as") or value.get("output") or value.get("target_field")
        ref = value.get("ref")
        if isinstance(output_name, str) and output_name.strip() and isinstance(ref, list) and ref:
            source_name = ref[-1]
            if isinstance(source_name, str) and source_name.strip():
                result[output_name.strip().casefold()] = source_name.strip()
        for output_key in ("source_field", "sourceField", "source_column", "sourceColumn"):
            source_name = value.get(output_key)
            if isinstance(output_name, str) and output_name.strip() and isinstance(source_name, str) and source_name.strip():
                result[output_name.strip().casefold()] = source_name.strip()

        for child in value.values():
            projection_mappings(child, result)
        return result

    definition = node.get("definition") if isinstance(node.get("definition"), dict) else {}
    mappings = projection_mappings(definition)

    def enrich(fields: list[dict[str, Any]]) -> list[dict[str, Any]]:
        enriched = []
        for field in fields:
            item = dict(field)
            name = str(item.get("name") or "")
            if name and not any(
                item.get(key) for key in ("source_field", "sourceField", "source_column", "sourceColumn", "mapped_field", "mappedField")
            ):
                source_name = mappings.get(name.casefold())
                if source_name and source_name.casefold() != name.casefold():
                    item["source_field"] = source_name
                    item["mapping_evidence"] = "explicit Datasphere projection ref"
            enriched.append(item)
        return enriched

    fields = node.get("field_catalog")
    if isinstance(fields, list):
        return enrich([dict(field) for field in fields if isinstance(field, dict) and field.get("name")])
    if isinstance(definition, dict):
        definitions = definition.get("definitions")
        if isinstance(definitions, dict):
            for body in definitions.values():
                elements = body.get("elements") if isinstance(body, dict) else None
                if isinstance(elements, dict):
                    return enrich([
                        {"name": name, **(element if isinstance(element, dict) else {})}
                        for name, element in elements.items()
                    ])
    return []


def resolve_execution_target(
    dependency_tree: dict[str, Any],
    *,
    selected_fields: list[str],
    required_keys: list[str],
    runtime_discovery: Any | None = None,
) -> ExecutionTarget:
    """Resolve the verified second-node fact-model/runtime object.

    The immediate dependency of the analytical-model root is authoritative
    when it is an explicitly identified source view. Deeper dependencies are
    retained as lineage evidence and are never promoted to the execution
    target automatically.
    """
    if not isinstance(dependency_tree, dict):
        raise ExecutionTargetError("Datasphere dependency metadata is missing.")

    nodes: list[dict[str, Any]] = []
    stack = [dependency_tree]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            nodes.append(node)
            stack.extend(reversed(node.get("dependencies") or []))

    root = dependency_tree
    immediate = [item for item in (root.get("dependencies") or []) if isinstance(item, dict)]
    if not immediate:
        raise ExecutionTargetError("The analytical model has no immediate source/fact-model dependency.")

    explicitly_role_bound = [
        item for item in immediate
        if str(item.get("relationship") or "").casefold() == "analytic_model_source"
    ]
    if explicitly_role_bound:
        if len(explicitly_role_bound) != 1:
            raise ExecutionTargetError("Multiple immediate analytic_model_source dependencies were found.")
        fact = explicitly_role_bound[0]
    else:
        view_dependencies = [item for item in immediate if item.get("object_type") == "view"]
        if len(view_dependencies) != 1:
            # Backward-compatible support for normalized trees that explicitly
            # classify the second node as fact_model rather than view.
            fact_dependencies = [item for item in immediate if item.get("object_type") == "fact_model"]
            if len(fact_dependencies) != 1:
                raise ExecutionTargetError(
                    "The analytical model's immediate fact-model/source view is missing or ambiguous."
                )
            fact = fact_dependencies[0]
        else:
            fact = view_dependencies[0]

    if fact.get("object_type") not in {"view", "fact_model"}:
        raise ExecutionTargetError(
            f"Immediate source {fact.get('name')!r} is not a verified fact-model view."
        )
    object_name, schema = _explicit_runtime_reference(fact)
    discovered = None
    if (not object_name or not schema or not _node_columns(fact)) and runtime_discovery:
        discovered = runtime_discovery(
            logical_name=str(fact.get("name") or ""),
            space=str(fact.get("space") or dependency_tree.get("space") or ""),
        )
        if isinstance(discovered, dict):
            object_name = object_name or str(discovered.get("object") or discovered.get("execution_object") or "").strip() or None
            schema = schema or str(discovered.get("schema") or discovered.get("execution_schema") or "").strip() or None
    if not object_name or not schema:
        raise ExecutionTargetError(
            f"Fact model {fact.get('name')!r} has no verified physical/runtime SQL object and schema."
        )
    columns = _node_columns(fact)
    if not columns and isinstance(discovered, dict):
        discovered_columns = discovered.get("columns")
        if isinstance(discovered_columns, list):
            columns = [dict(item) for item in discovered_columns if isinstance(item, dict) and item.get("name")]
    if not columns:
        raise ExecutionTargetError(f"Verified runtime object {schema}.{object_name} has no column metadata.")
    available = {str(item["name"]).casefold() for item in columns}
    missing = [field for field in list(dict.fromkeys(selected_fields + required_keys)) if field.casefold() not in available]
    if missing:
        raise ExecutionTargetError(
            f"Verified runtime object {schema}.{object_name} does not expose required field(s): {', '.join(missing)}."
        )
    lineage = tuple(str(item.get("name")) for item in nodes if item.get("name"))
    return ExecutionTarget(
        analytical_model=str(dependency_tree.get("name") or ""),
        analytical_model_space=str(dependency_tree.get("space") or ""),
        fact_model=str(fact.get("name") or ""),
        fact_model_space=str(fact.get("space") or dependency_tree.get("space") or ""),
        relationship=str(fact.get("relationship") or "analytic_model_source"),
        object_type=str(fact.get("object_type") or "unknown"),
        object_name=object_name,
        schema=schema,
        columns=tuple(columns),
        lineage=lineage,
    )


def _quote(identifier: str) -> str:
    if not isinstance(identifier, str) or not identifier or any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_" for ch in identifier):
        raise ExecutionTargetError(f"Unsafe SQL identifier: {identifier!r}")
    return f'"{identifier}"'


def compile_reconciliation_sql(
    target: ExecutionTarget,
    *,
    selected_fields: list[str],
    required_keys: list[str],
    where_clause: str | None = None,
    having_clause: str | None = None,
) -> str:
    """Compile deterministic, metadata-only SQL for the verified target."""
    by_name = {str(item["name"]).casefold(): item for item in target.columns}
    fields = list(dict.fromkeys(required_keys + selected_fields))
    projections: list[str] = []
    dimensions: list[str] = []
    for field in fields:
        metadata = by_name[field.casefold()]
        name = str(metadata["name"])
        expression = _quote(name)
        aggregation = str(metadata.get("aggregation") or "").lstrip("#").upper()
        if aggregation in {"SUM", "COUNT", "MIN", "MAX", "AVG"} and field.casefold() not in {key.casefold() for key in required_keys}:
            expression = f'{aggregation}({_quote(name)}) AS {_quote(name)}'
        else:
            dimensions.append(_quote(name))
        projections.append(expression)
    source = f"{_quote(target.schema)}.{_quote(target.object_name)}"
    sql = f"SELECT {', '.join(projections)} FROM {source}"
    if where_clause:
        sql += f" WHERE {where_clause}"
    if dimensions:
        sql += f" GROUP BY {', '.join(dimensions)}"
    if having_clause:
        sql += f" HAVING {having_clause}"
    return sql


def resolve_reconciliation_grain(
    target: ExecutionTarget,
    *,
    requested_keys: list[str] | None = None,
    analytical_fields: list[dict[str, Any]] | None = None,
    source_fields: list[dict[str, Any]] | None = None,
    source_key_fields: list[str] | None = None,
) -> dict[str, Any]:
    """Choose a verified row or aggregate grain for both systems.

    Requested keys are treated as candidates, not universal requirements. The
    target's declared keys and dimensions take precedence when they are
    exposed by the selected fact-model view.
    """
    available = {str(item["name"]).casefold(): item for item in target.columns}
    source_names = {
        str(item.get("name")).casefold()
        for item in (source_fields or [])
        if isinstance(item, dict) and item.get("name")
    }
    source_keys = {str(item).casefold() for item in (source_key_fields or [])}

    def explicit_source_name(item: dict[str, Any]) -> str | None:
        for key in (
            "source_field", "sourceField", "source_column", "sourceColumn",
            "mapped_field", "mappedField", "source_name", "sourceName",
        ):
            value = item.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        for key in ("mapping", "source_mapping", "lineage_mapping"):
            value = item.get(key)
            if isinstance(value, dict):
                for nested_key in ("source_field", "sourceField", "field", "name", "column"):
                    nested = value.get(nested_key)
                    if isinstance(nested, str) and nested.strip():
                        return nested.strip()
        return None

    explicit_mappings = {
        str(item["name"]).casefold(): explicit_source_name(item)
        for item in target.columns
        if item.get("name") and explicit_source_name(item)
    }

    def valid(fields: list[str], require_source: bool = False) -> bool:
        if not fields or any(field.casefold() not in available for field in fields):
            return False
        if not require_source:
            return True
        return all(
            field.casefold() in source_names
            or explicit_mappings.get(field.casefold(), "").casefold() in source_names
            for field in fields
        )

    def mappings_for(fields: list[str]) -> list[dict[str, str]]:
        return [
            {
                "target_field": field,
                "source_field": explicit_mappings.get(field.casefold(), field),
            }
            for field in fields
        ]

    requested = list(dict.fromkeys(str(field).strip() for field in (requested_keys or []) if str(field).strip()))
    declared = [
        str(item["name"])
        for item in target.columns
        if item.get("is_key") or item.get("key") or item.get("primary_key")
    ]
    model_declared = [
        str(item["name"])
        for item in (analytical_fields or [])
        if isinstance(item, dict) and item.get("name") and (item.get("is_key") or item.get("key"))
    ]

    candidates = [requested, declared, model_declared]
    for candidate in candidates:
        if valid(candidate, bool(source_fields)):
            return {
                "strategy": "row_level",
                "keys": candidate,
                "source_keys": [item["source_field"] for item in mappings_for(candidate)],
                "field_mappings": mappings_for(candidate),
                "reason": "verified compatible key metadata",
            }

    # A declared source key can be used when the fact model exposes the same
    # explicitly named key. This is still metadata validation, not name-based
    # guessing across unrelated source columns.
    source_key_overlap = [
        name for name in available
        if name in source_keys
    ]
    if source_key_overlap and valid(source_key_overlap, bool(source_fields)):
        return {
            "strategy": "row_level",
            "keys": [str(available[name]["name"]) for name in source_key_overlap],
            "source_keys": [str(available[name]["name"]) for name in source_key_overlap],
            "field_mappings": mappings_for([str(available[name]["name"]) for name in source_key_overlap]),
            "reason": "verified source and fact-model key metadata",
        }

    dimensions = [
        str(item["name"])
        for item in target.columns
        if not item.get("aggregation") and not item.get("is_measure") and not item.get("measure_type")
    ]
    if dimensions:
        if source_fields:
            common = [
                field for field in dimensions
                if field.casefold() in source_names
                or explicit_mappings.get(field.casefold(), "").casefold() in source_names
            ]
            if not common:
                raise ReconciliationGrainError(
                    "The fact model exposes dimensions, but none are verified in the S/4HANA source metadata."
                )
            dimensions = common
        return {
            "strategy": "aggregated",
            "keys": dimensions,
            "source_keys": [item["source_field"] for item in mappings_for(dimensions)],
            "field_mappings": mappings_for(dimensions),
            "reason": "shared verified dimension grain; source row keys are not exposed",
        }

    raise ReconciliationGrainError(
        "No compatible row-level key or shared aggregation grain is exposed by both systems."
    )
