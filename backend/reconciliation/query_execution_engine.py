"""Validated query execution facade for source and Datasphere adapters.

The ADT adapter wraps the existing DataRetriever.fetch_query method.
Datasphere query execution is intentionally injected: the project must
provide a supported Datasphere SQL/consumption API adapter before it can
be used. This module does not pretend the ADT endpoint queries Datasphere.
"""
from __future__ import annotations

import re
from typing import Any, Callable


class ReadOnlyQueryError(ValueError):
    pass


_FORBIDDEN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|UPSERT|DROP|ALTER|CREATE|TRUNCATE|CALL|EXECUTE)\b",
    re.IGNORECASE,
)
_ALLOWED_START = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)


def validate_read_only_query(query: str) -> str:
    if not isinstance(query, str) or not query.strip():
        raise ReadOnlyQueryError("Query must be a non-empty string.")
    candidate = query.strip()
    if not _ALLOWED_START.match(candidate):
        raise ReadOnlyQueryError("Only SELECT/WITH read-only queries are permitted.")
    # Reject comments to reduce ambiguity in this first version.
    if "--" in candidate or "/*" in candidate or "*/" in candidate:
        raise ReadOnlyQueryError("SQL comments are not allowed in generated queries.")
    if ";" in candidate.rstrip(";"):
        raise ReadOnlyQueryError("Multiple SQL statements are not permitted.")
    if _FORBIDDEN.search(candidate):
        raise ReadOnlyQueryError("Potentially mutating SQL keyword detected.")
    return candidate.rstrip(";").strip()


class QueryExecutionEngine:
    def __init__(
        self,
        *,
        adt_retriever: Any | None = None,
        datasphere_executor: Callable[..., Any] | None = None,
        max_rows: int = 10000,
    ):
        if max_rows < 1:
            raise ValueError("max_rows must be positive")
        self.adt_retriever = adt_retriever
        self.datasphere_executor = datasphere_executor
        self.max_rows = max_rows

    def execute_adt(
        self,
        *,
        query: str,
        row_limit: int | None = None,
        source_name: str | None = None,
        parameters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if self.adt_retriever is None:
            raise RuntimeError("No ADT DataRetriever was configured.")
        safe_query = validate_read_only_query(query)
        limit = min(row_limit or self.max_rows, self.max_rows)
        # Parameterized CDS entities should normally be built with the
        # existing DataRetriever parameter-aware API. Do not silently ignore
        # parameters here; require the caller to pass a correctly bound query
        # or use execute_adt_view() below.
        if parameters:
            raise NotImplementedError(
                "Use execute_adt_view() for parameterized CDS retrieval so "
                "the existing parameter validator/binder is applied."
            )
        result = self.adt_retriever.fetch_query(
            query=safe_query,
            row_limit=limit,
            csrf_entity=source_name,
        )
        return {
            "system": "S4HANA_ADT",
            "source": source_name,
            "query": safe_query,
            "row_limit": limit,
            "result": result,
        }

    def execute_adt_view(
        self,
        *,
        view_name: str,
        parameters: dict[str, Any] | None = None,
        fields: list[str] | None = None,
        row_limit: int | None = None,
    ) -> dict[str, Any]:
        if self.adt_retriever is None:
            raise RuntimeError("No ADT DataRetriever was configured.")
        limit = min(row_limit or self.max_rows, self.max_rows)
        # The existing DataRetriever owns identifier validation, parameter
        # discovery, parameter typing, and ABAP Open SQL formatting.
        result = self.adt_retriever.fetch_view(
            view_name=view_name,
            parameters=parameters,
            fields=fields,
            row_limit=limit,
        )
        return {
            "system": "S4HANA_ADT",
            "source": view_name,
            "parameters": parameters or {},
            "row_limit": limit,
            "result": result,
        }

    def execute_datasphere(
        self,
        *,
        query: str,
        object_name: str | None = None,
        parameters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if self.datasphere_executor is None:
            raise RuntimeError(
                "Datasphere executor is not configured. Implement this adapter "
                "using the SQL/consumption interface and permissions available "
                "in your Datasphere tenant."
            )
        safe_query = validate_read_only_query(query)
        result = self.datasphere_executor(
            query=safe_query,
            object_name=object_name,
            parameters=parameters or {},
        )
        return {
            "system": "DATASPHERE",
            "source": object_name,
            "query": safe_query,
            "result": result,
        }
