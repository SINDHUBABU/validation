"""Persistent local storage for explicitly selected S/4HANA CDS extracts."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import time
import uuid
import csv
import io
import re
from pathlib import Path
from typing import Any, Callable, Iterable


class CdsStagingStore:
    """Atomically publish complete CDS extracts as local SQLite tables."""

    CACHE_TTL_SECONDS = 24 * 60 * 60

    def __init__(self, db_path: str | Path | None = None) -> None:
        self.db_path = Path(db_path) if db_path else Path(__file__).resolve().parents[1] / "cds_staging.db"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        db = self._connect()
        try:
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS staged_cds_metadata (
                    space_id TEXT NOT NULL,
                    source_name TEXT NOT NULL COLLATE NOCASE,
                    table_name TEXT NOT NULL,
                    fields_json TEXT NOT NULL,
                    column_types_json TEXT NOT NULL,
                    key_fields_json TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    total_rows INTEGER NOT NULL,
                    batch_count INTEGER NOT NULL,
                    staged_at REAL NOT NULL,
                    metadata_fingerprint TEXT NOT NULL DEFAULT '',
                    snapshot_id TEXT NOT NULL DEFAULT '',
                    validation_run_id TEXT NOT NULL DEFAULT '',
                    extraction_started_at REAL NOT NULL DEFAULT 0,
                    last_refresh_at REAL NOT NULL DEFAULT 0,
                    expires_at REAL NOT NULL DEFAULT 0,
                    consistent_snapshot INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (space_id, source_name)
                )
                """
            )
            existing = {row[1] for row in db.execute("PRAGMA table_info(staged_cds_metadata)")}
            upgrades = {
                "metadata_fingerprint": "TEXT NOT NULL DEFAULT ''",
                "snapshot_id": "TEXT NOT NULL DEFAULT ''",
                "validation_run_id": "TEXT NOT NULL DEFAULT ''",
                "extraction_started_at": "REAL NOT NULL DEFAULT 0",
                "last_refresh_at": "REAL NOT NULL DEFAULT 0",
                "expires_at": "REAL NOT NULL DEFAULT 0",
                "consistent_snapshot": "INTEGER NOT NULL DEFAULT 0",
            }
            for name, declaration in upgrades.items():
                if name not in existing:
                    db.execute(f"ALTER TABLE staged_cds_metadata ADD COLUMN {name} {declaration}")
            db.execute("""
                CREATE TABLE IF NOT EXISTS active_staged_snapshots (
                    snapshot_id TEXT NOT NULL,
                    lease_id TEXT NOT NULL,
                    lease_expires REAL NOT NULL,
                    PRIMARY KEY (snapshot_id, lease_id)
                )
            """)
            db.commit()
        finally:
            db.close()

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(str(self.db_path), timeout=30)
        db.row_factory = sqlite3.Row
        return db

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        if not identifier or any(ord(char) < 32 for char in identifier):
            raise ValueError("A staged SQLite identifier is empty or contains control characters.")
        return '"' + identifier.replace('"', '""') + '"'

    @staticmethod
    def _stable_table_name(space_id: str, source_name: str) -> str:
        digest = hashlib.sha256(
            f"{space_id.casefold()}\0{source_name.casefold()}".encode("utf-8")
        ).hexdigest()[:20]
        return f"stg_cds_{digest}"

    @staticmethod
    def _normalize_fields(fields: Iterable[Any]) -> list[str]:
        normalized = [
            field.strip() if isinstance(field, str) else ""
            for field in fields
        ]
        if not normalized or any(not field for field in normalized):
            raise ValueError("SAP returned missing CDS column names.")
        if len({field.casefold() for field in normalized}) != len(normalized):
            raise ValueError("SAP returned duplicate CDS column names; staging is unsafe.")
        return normalized

    def stage_source(
        self,
        *,
        space_id: str,
        source_name: str,
        extract: Callable[[Callable[[list[dict[str, Any]], dict[str, Any]], None]], dict[str, Any]],
        metadata_fingerprint: str = "",
        validation_run_id: str = "",
        force_refresh: bool = False,
        cache_ttl_seconds: int = CACHE_TTL_SECONDS,
    ) -> dict[str, Any]:
        """Stream an extraction into a temporary table and publish it only if complete."""
        if cache_ttl_seconds < self.CACHE_TTL_SECONDS:
            raise ValueError("CDS snapshots must be retained for at least 24 hours.")
        now = time.time()
        if metadata_fingerprint and not force_refresh:
            cached = self.get_compatible_snapshot(
                space_id=space_id, source_name=source_name,
                metadata_fingerprint=metadata_fingerprint, now=now,
            )
            if cached:
                cached["cache_status"] = "REUSED"
                cached["cache_age_seconds"] = max(0, int(now - cached["staged_at"]))
                return cached
        db = self._connect()
        temporary_table = f"__cds_stage_{uuid.uuid4().hex}"
        final_table = self._stable_table_name(space_id, source_name)
        fields: list[str] | None = None
        inserted = 0

        def create_table(column_names: Iterable[Any]) -> list[str]:
            nonlocal fields
            fields = self._normalize_fields(column_names)
            definitions = ", ".join(
                f"{self._quote_identifier(name)} TEXT" for name in fields
            )
            db.execute(
                f"CREATE TABLE {self._quote_identifier(temporary_table)} ({definitions})"
            )
            return fields

        def write_batch(rows: list[dict[str, Any]], _info: dict[str, Any]) -> None:
            nonlocal inserted
            if not rows:
                return
            if fields is None:
                create_table(rows[0].keys())

            if fields is None:
                raise ValueError("SAP returned rows before the staging schema was initialized.")
            field_lookup = {field.casefold(): field for field in fields}
            insert_fields = []
            for row in rows:
                row_lookup = {str(key).casefold(): key for key in row}
                if set(row_lookup) != set(field_lookup):
                    raise ValueError("SAP returned inconsistent columns between extraction rows.")
                values = tuple(
                    row[row_lookup[field.casefold()]]
                    if row[row_lookup[field.casefold()]] is None
                    or isinstance(row[row_lookup[field.casefold()]], (str, int, float, bytes))
                    else str(row[row_lookup[field.casefold()]])
                    for field in fields
                )
                insert_fields.append(values)
            placeholders = ", ".join("?" for _ in fields)
            column_sql = ", ".join(self._quote_identifier(field) for field in fields)
            db.executemany(
                f"INSERT INTO {self._quote_identifier(temporary_table)} "
                f"({column_sql}) VALUES ({placeholders})",
                insert_fields,
            )
            inserted += len(rows)

        try:
            db.execute("BEGIN IMMEDIATE")
            summary = extract(write_batch)
            if not isinstance(summary, dict) or summary.get("complete") is not True:
                raise ValueError("SAP CDS extraction did not verify as complete; staged data was not published.")

            if fields is None:
                fields = self._normalize_fields(summary.get("fields") or [])
                create_table(fields)
            else:
                summary_fields = self._normalize_fields(summary.get("fields") or fields)
                if [field.casefold() for field in summary_fields] != [field.casefold() for field in fields]:
                    raise ValueError("SAP extraction metadata does not match the returned CDS columns.")

            row_count = summary.get("row_count")
            total_rows = summary.get("total_rows_reported")
            if (
                isinstance(row_count, bool)
                or not isinstance(row_count, int)
                or row_count != inserted
                or isinstance(total_rows, bool)
                or not isinstance(total_rows, int)
                or total_rows != inserted
            ):
                raise ValueError("SAP extraction row counts do not match the staged rows.")

            db.execute(f"DROP TABLE IF EXISTS {self._quote_identifier(final_table)}")
            db.execute(
                f"ALTER TABLE {self._quote_identifier(temporary_table)} "
                f"RENAME TO {self._quote_identifier(final_table)}"
            )
            staged_at = time.time()
            column_types = summary.get("column_types") or {}
            type_lookup = {str(name).casefold(): value for name, value in column_types.items()}
            key_fields = summary.get("key_fields") or []
            snapshot_id = uuid.uuid4().hex
            extraction_started_at = float(summary.get("extraction_started_at") or staged_at)
            expires_at = staged_at + cache_ttl_seconds
            db.execute(
                """
                INSERT INTO staged_cds_metadata (
                    space_id, source_name, table_name, fields_json,
                    column_types_json, key_fields_json, row_count,
                    total_rows, batch_count, staged_at, metadata_fingerprint,
                    snapshot_id, validation_run_id, extraction_started_at,
                    last_refresh_at, expires_at, consistent_snapshot
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(space_id, source_name) DO UPDATE SET
                    table_name = excluded.table_name,
                    fields_json = excluded.fields_json,
                    column_types_json = excluded.column_types_json,
                    key_fields_json = excluded.key_fields_json,
                    row_count = excluded.row_count,
                    total_rows = excluded.total_rows,
                    batch_count = excluded.batch_count,
                    staged_at = excluded.staged_at,
                    metadata_fingerprint = excluded.metadata_fingerprint,
                    snapshot_id = excluded.snapshot_id,
                    validation_run_id = excluded.validation_run_id,
                    extraction_started_at = excluded.extraction_started_at,
                    last_refresh_at = excluded.last_refresh_at,
                    expires_at = excluded.expires_at,
                    consistent_snapshot = excluded.consistent_snapshot
                """,
                (
                    space_id,
                    source_name,
                    final_table,
                    json.dumps(fields),
                    json.dumps(column_types),
                    json.dumps(key_fields),
                    inserted,
                    total_rows,
                    int(summary.get("batches") or 0),
                    staged_at,
                    metadata_fingerprint,
                    snapshot_id,
                    validation_run_id,
                    extraction_started_at,
                    staged_at,
                    expires_at,
                    int(bool(summary.get("consistent_snapshot"))),
                ),
            )
            preview = db.execute(
                f"SELECT * FROM {self._quote_identifier(final_table)} LIMIT 1000"
            ).fetchall()
            db.commit()
            return {
                "source": source_name,
                "status": "STAGED_COMPLETE",
                "table_name": final_table,
                "columns": [
                    {"name": name, "type": type_lookup.get(name.casefold())}
                    for name in fields
                ],
                "key_fields": key_fields,
                "row_count": inserted,
                "total_rows": total_rows,
                "batch_count": int(summary.get("batches") or 0),
                "staged_at": staged_at,
                "extraction_timestamp": extraction_started_at,
                "last_refresh_at": staged_at,
                "expires_at": expires_at,
                "cache_age_seconds": 0,
                "cache_status": "REFRESHED",
                "metadata_fingerprint": metadata_fingerprint,
                "snapshot_id": snapshot_id,
                "validation_run_id": validation_run_id,
                "consistent_snapshot": bool(summary.get("consistent_snapshot")),
                "preview": [dict(row) for row in preview],
                "issues": summary.get("issues") or [],
            }
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    @staticmethod
    def _formula_sql(expression: str, columns: list[str]) -> str:
        """Normalize Datasphere calculated-field expressions for SQLite."""
        if not isinstance(expression, str) or not expression.strip():
            raise ValueError("Calculated-field formulas must be non-empty strings.")
        if any(token in expression for token in (";", "--", "/*", "*/")):
            raise ValueError("Formula contains unsupported SQL statement syntax.")
        bare_value = expression.strip()
        if (
            bare_value.casefold() not in {column.casefold() for column in columns}
            and " " in bare_value
            and re.fullmatch(r"[A-Za-z][A-Za-z0-9 _-]*", bare_value)
        ):
            # Datasphere formulas can encode constant dimension labels without
            # quotes (for example, "Procurement Lead Time"). Treat those as
            # text values instead of SQLite identifiers.
            return "'" + bare_value.replace("'", "''") + "'"
        column_lookup = {str(column).casefold() for column in columns}

        def remove_verified_abap_qualifier(match: re.Match[str]) -> str:
            # ABAP Open SQL uses Entity~Field qualification. The replica query
            # runs against a single staged SQLite table, so retain the field
            # only when ADT confirmed that it exists in this source snapshot.
            field = match.group("field")
            return field if field.casefold() in column_lookup else match.group(0)

        normalized = re.sub(
            r"(?<![A-Za-z0-9_])[A-Za-z_][A-Za-z0-9_]*~(?P<field>[A-Za-z_][A-Za-z0-9_]*)(?![A-Za-z0-9_])",
            remove_verified_abap_qualifier,
            expression.strip(),
        )
        normalized = re.sub(r"\b(?:[A-Za-z_][A-Za-z0-9_]*\.)+", "", normalized)
        if "~" in normalized:
            raise ValueError("Formula contains an unresolved ABAP field qualifier.")
        for name in sorted(columns, key=len, reverse=True):
            normalized = re.sub(
                rf"(?<![A-Za-z0-9_\"]){re.escape(name)}(?![A-Za-z0-9_\"])",
                lambda _match, column=name: f'"{column.replace(chr(34), chr(34) * 2)}"',
                normalized,
                flags=re.IGNORECASE,
            )
        return normalized

    def write_source_csv(
        self, *, space_id: str, source_name: str, output_path: str | Path,
        calculated_fields: dict[str, str] | None = None,
        selected_fields: list[str] | None = None,
        where_clause: str | None = None,
    ) -> dict[str, Any]:
        """Write a complete staged source or calculated replica to Excel-ready CSV."""
        db = self._connect()
        try:
            snapshot = db.execute(
                "SELECT * FROM staged_cds_metadata WHERE space_id=? AND source_name=? COLLATE NOCASE",
                (space_id, source_name),
            ).fetchone()
            if not snapshot or snapshot["row_count"] != snapshot["total_rows"]:
                raise ValueError(f"No complete staged snapshot exists for {source_name}.")
            if snapshot["expires_at"] and snapshot["expires_at"] <= time.time():
                raise ValueError(f"The staged snapshot for {source_name} has expired. Fetch it again from ADT.")
            source_columns = json.loads(snapshot["fields_json"])
            source_lookup = {name.casefold(): name for name in source_columns}
            formulas = calculated_fields or {}
            # Match the final Datasphere projection. Formula inputs can still
            # be referenced by calculated expressions without being exported.
            replaced_columns = {key.casefold() for key in formulas}
            requested = list(dict.fromkeys(str(name).strip() for name in (selected_fields or source_columns) if str(name).strip()))
            available_output_names = {name.casefold() for name in source_columns} | {str(name).casefold() for name in formulas}
            unknown = [name for name in requested if name.casefold() not in available_output_names]
            if unknown:
                raise ValueError(f"Projected Datasphere fields are not available from the staged CDS source or calculated fields: {', '.join(unknown)}")
            requested_set = {name.casefold() for name in requested}
            selected_source_columns = [name for name in source_columns
                                       if name.casefold() in requested_set and name.casefold() not in replaced_columns]
            expressions: list[str] = [self._quote_identifier(name) for name in selected_source_columns]
            result_columns = list(selected_source_columns)
            calculated_logic: list[dict[str, str]] = []
            for name, expression in formulas.items():
                clean_name = str(name).strip()
                if not clean_name or clean_name.casefold() in source_lookup:
                    # Calculated fields may intentionally replace an existing
                    # source column; the projected base column above is omitted.
                    result_columns = [item for item in result_columns if item.casefold() != clean_name.casefold()]
                    expressions = [item for item in expressions if item.strip('"').casefold() != clean_name.casefold()]
                if not clean_name:
                    raise ValueError("Calculated field names must not be empty.")
                safe_expression = self._formula_sql(str(expression), source_columns)
                if clean_name.casefold() in requested_set:
                    expressions.append(f"({safe_expression}) AS {self._quote_identifier(clean_name)}")
                    result_columns.append(clean_name)
                calculated_logic.append({
                    "name": clean_name,
                    "formula": str(expression),
                    "sql_expression": safe_expression,
                })
            if not expressions:
                raise ValueError("The CDS replica has no output columns.")

            def _left(value: Any, length: Any) -> str | None:
                if value is None:
                    return None
                try:
                    count = max(0, int(length))
                except (TypeError, ValueError):
                    return None
                return str(value)[:count]

            def _right(value: Any, length: Any) -> str | None:
                if value is None:
                    return None
                try:
                    count = max(0, int(length))
                except (TypeError, ValueError):
                    return None
                return str(value)[-count:] if count else ""

            def _month(value: Any) -> int | None:
                digits = re.sub(r"\D", "", str(value or ""))
                return int(digits[4:6]) if len(digits) >= 6 and digits[4:6].isdigit() else None

            def _year(value: Any) -> int | None:
                digits = re.sub(r"\D", "", str(value or ""))
                return int(digits[:4]) if len(digits) >= 4 and digits[:4].isdigit() else None

            db.create_function("MONTH", 1, _month, deterministic=True)
            db.create_function("YEAR", 1, _year, deterministic=True)
            db.create_function("LEFT", 2, _left, deterministic=True)
            db.create_function("RIGHT", 2, _right, deterministic=True)
            db.create_function("SUBSTRING", 3, lambda value, start, length: None if value is None else str(value)[max(0, int(start) - 1):max(0, int(start) - 1) + max(0, int(length))], deterministic=True)
            db.set_authorizer(
                lambda action, arg1, _arg2, _database, _trigger:
                sqlite3.SQLITE_OK if action in (sqlite3.SQLITE_SELECT, sqlite3.SQLITE_FUNCTION, sqlite3.SQLITE_RECURSIVE)
                or (action == sqlite3.SQLITE_READ and str(arg1).casefold() == snapshot["table_name"].casefold())
                else sqlite3.SQLITE_DENY
            )
            query = f"SELECT {', '.join(expressions)} FROM {self._quote_identifier(snapshot['table_name'])}"
            translated_where = None
            if where_clause and where_clause.strip():
                if any(token in where_clause for token in (";", "--", "/*", "*/")):
                    raise ValueError("CDS metadata filter contains unsupported SQL statement syntax.")
                translated_where = self._formula_sql(where_clause, source_columns)
                query += f" WHERE ({translated_where})"
            cursor = db.execute(query)
            actual_columns = [item[0] for item in cursor.description or []]
            output = Path(output_path)
            output.parent.mkdir(parents=True, exist_ok=True)
            preview: list[dict[str, Any]] = []
            row_count = 0
            with output.open("w", newline="", encoding="utf-8-sig") as target:
                writer = csv.writer(target, lineterminator="\r\n")
                writer.writerow(actual_columns)
                while True:
                    batch = cursor.fetchmany(1000)
                    if not batch:
                        break
                    writer.writerows(batch)
                    if len(preview) < 1000:
                        preview.extend(dict(zip(actual_columns, row)) for row in batch[:1000 - len(preview)])
                    row_count += len(batch)
            return {
                "columns": actual_columns,
                "rows": preview,
                "row_count": row_count,
                "path": str(output),
                "execution_engine": "SQLite on the backend, reading the complete staged S/4HANA ADT CDS snapshot",
                "sql": query,
                "calculated_fields": calculated_logic,
                "where_clause": translated_where,
            }
        finally:
            db.close()

    def list_staged_sources(self, space_id: str | None = None) -> list[dict[str, Any]]:
        query = """
            SELECT space_id, source_name, table_name, fields_json,
                   column_types_json, key_fields_json, row_count,
                   total_rows, batch_count, staged_at, metadata_fingerprint,
                   snapshot_id, validation_run_id, extraction_started_at,
                   last_refresh_at, expires_at, consistent_snapshot
            FROM staged_cds_metadata
        """
        params: tuple[Any, ...] = ()
        if space_id:
            query += " WHERE space_id = ?"
            params = (space_id,)
        query += " ORDER BY staged_at DESC"
        db = self._connect()
        try:
            rows = db.execute(query, params).fetchall()
        finally:
            db.close()
        return [
            {
                "space_id": row["space_id"],
                "source": row["source_name"],
                "table_name": row["table_name"],
                "columns": json.loads(row["fields_json"]),
                "column_types": json.loads(row["column_types_json"]),
                "column_types": json.loads(row["column_types_json"]),
                "key_fields": json.loads(row["key_fields_json"]),
                "row_count": row["row_count"],
                "total_rows": row["total_rows"],
                "batch_count": row["batch_count"],
                "staged_at": row["staged_at"],
                "complete": row["row_count"] == row["total_rows"],
                "metadata_fingerprint": row["metadata_fingerprint"],
                "snapshot_id": row["snapshot_id"],
                "validation_run_id": row["validation_run_id"],
                "extraction_timestamp": row["extraction_started_at"],
                "last_refresh_at": row["last_refresh_at"],
                "expires_at": row["expires_at"],
                "cache_age_seconds": max(0, int(time.time() - row["staged_at"])),
                "fresh": bool(row["expires_at"] and row["expires_at"] > time.time()),
                "consistent_snapshot": bool(row["consistent_snapshot"]),
            }
            for row in rows
        ]

    def get_compatible_snapshot(
        self, *, space_id: str, source_name: str,
        metadata_fingerprint: str, now: float | None = None,
    ) -> dict[str, Any] | None:
        """Return a complete, unexpired snapshot matching source metadata."""
        now = time.time() if now is None else now
        db = self._connect()
        try:
            row = db.execute(
                "SELECT * FROM staged_cds_metadata WHERE space_id=? AND source_name=? COLLATE NOCASE",
                (space_id, source_name),
            ).fetchone()
        finally:
            db.close()
        if not row or row["row_count"] != row["total_rows"]:
            return None
        expiry = row["expires_at"] or (row["staged_at"] + self.CACHE_TTL_SECONDS)
        if expiry <= now or row["metadata_fingerprint"] != metadata_fingerprint:
            return None
        column_types = json.loads(row["column_types_json"])
        fields = json.loads(row["fields_json"])
        preview_db = self._connect()
        try:
            preview_rows = preview_db.execute(
                f"SELECT * FROM {self._quote_identifier(row['table_name'])} LIMIT 1000"
            ).fetchall()
        finally:
            preview_db.close()
        return {
            "source": row["source_name"], "status": "STAGED_COMPLETE",
            "table_name": row["table_name"],
            "columns": [{"name": name, "type": next((value for key, value in column_types.items()
                      if key.casefold() == name.casefold()), None)} for name in fields],
            "key_fields": json.loads(row["key_fields_json"]),
            "row_count": row["row_count"], "total_rows": row["total_rows"],
            "batch_count": row["batch_count"], "staged_at": row["staged_at"],
            "extraction_timestamp": row["extraction_started_at"],
            "last_refresh_at": row["last_refresh_at"], "expires_at": expiry,
            "metadata_fingerprint": row["metadata_fingerprint"],
            "snapshot_id": row["snapshot_id"], "validation_run_id": row["validation_run_id"],
            "consistent_snapshot": bool(row["consistent_snapshot"]),
            "preview": [dict(item) for item in preview_rows], "issues": [],
        }

    def approval_hash(
        self, sql: str, space_id: str | None = None,
        approval_context: dict[str, Any] | None = None,
    ) -> str:
        """Bind approval to SQL and exact staged source snapshots."""
        sources = self.list_staged_sources(space_id)
        context = [{key: item.get(key) for key in (
            "space_id", "source", "table_name", "snapshot_id", "metadata_fingerprint"
        )} for item in sources]
        return hashlib.sha256(json.dumps(
            {"sql": sql.strip(), "staged_sources": context,
             "approval_context": approval_context or {}}, sort_keys=True,
        ).encode("utf-8")).hexdigest()

    def execute_read_only_sql(
        self, *, sql: str, approved_hash: str, space_id: str | None = None,
        max_rows: int = 10000, approval_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute one approved SELECT against staged tables only."""
        from .query_execution_engine import validate_read_only_query
        query = validate_read_only_query(sql)
        if not approved_hash or approved_hash != self.approval_hash(query, space_id, approval_context):
            raise ValueError("SQL approval is missing or stale; regenerate and approve the exact plan.")
        staged = self.list_staged_sources(space_id)
        allowed_names = (approval_context or {}).get("allowed_table_names")
        allowed = {item["table_name"].casefold() for item in staged
                   if item["complete"] and item["fresh"]
                   and (allowed_names is None or item["table_name"] in allowed_names)}
        active = [item for item in staged if item["complete"] and item["fresh"]]
        lease_id = uuid.uuid4().hex
        lease_db = self._connect()
        try:
            lease_db.execute("BEGIN IMMEDIATE")
            lease_db.executemany(
                "INSERT INTO active_staged_snapshots (snapshot_id, lease_id, lease_expires) VALUES (?, ?, ?)",
                [(item["snapshot_id"], lease_id, time.time() + 3600) for item in active],
            )
            lease_db.commit()
        except Exception:
            lease_db.rollback()
            raise
        finally:
            lease_db.close()
        started = time.perf_counter()
        db = self._connect()
        def authorize(action, arg1, _arg2, _database, _trigger):
            if action in (sqlite3.SQLITE_SELECT, sqlite3.SQLITE_RECURSIVE, sqlite3.SQLITE_FUNCTION):
                return sqlite3.SQLITE_OK
            if action == sqlite3.SQLITE_READ and str(arg1).casefold() in allowed:
                return sqlite3.SQLITE_OK
            return sqlite3.SQLITE_DENY
        db.set_authorizer(authorize)
        try:
            cursor = db.execute(query)
            columns = [item[0] for item in cursor.description or []]
            rows = cursor.fetchmany(max_rows + 1)
            truncated = len(rows) > max_rows
            rows = rows[:max_rows]
            return {
                "status": "SUCCEEDED_TRUNCATED" if truncated else ("SUCCEEDED_ZERO_ROWS" if not rows else "SUCCEEDED"),
                "sql": query, "columns": columns,
                "rows": [dict(zip(columns, row)) for row in rows],
                "row_count": len(rows), "truncated": truncated,
                "duration_seconds": time.perf_counter() - started,
                "snapshot_ids": [item["snapshot_id"] for item in staged],
            }
        finally:
            db.close()
            lease_db = self._connect()
            try:
                lease_db.execute("DELETE FROM active_staged_snapshots WHERE lease_id=?", (lease_id,))
                lease_db.commit()
            finally:
                lease_db.close()

    def validate_read_only_sql(
        self, sql: str, space_id: str | None = None,
        approval_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Compile SQL against the staged schema without executing its result query."""
        from .query_execution_engine import validate_read_only_query
        query = validate_read_only_query(sql)
        staged = self.list_staged_sources(space_id)
        allowed_names = (approval_context or {}).get("allowed_table_names")
        allowed = {item["table_name"].casefold() for item in staged
                   if item["complete"] and item["fresh"]
                   and (allowed_names is None or item["table_name"] in allowed_names)}
        db = self._connect()
        def authorize(action, arg1, _arg2, _database, _trigger):
            if action in (sqlite3.SQLITE_SELECT, sqlite3.SQLITE_RECURSIVE, sqlite3.SQLITE_FUNCTION):
                return sqlite3.SQLITE_OK
            if action == sqlite3.SQLITE_READ and str(arg1).casefold() in allowed:
                return sqlite3.SQLITE_OK
            return sqlite3.SQLITE_DENY
        db.set_authorizer(authorize)
        try:
            cursor = db.execute("EXPLAIN QUERY PLAN " + query)
            return {"valid": True, "query": query,
                    "approval_hash": self.approval_hash(query, space_id, approval_context),
                    "query_plan": [dict(row) for row in cursor.fetchall()]}
        finally:
            db.close()

    def cleanup_expired(self, now: float | None = None) -> dict[str, int]:
        """Delete expired snapshots and retain snapshots still inside their cache window."""
        now = time.time() if now is None else now
        db = self._connect()
        removed = 0
        try:
            db.execute("BEGIN IMMEDIATE")
            db.execute("DELETE FROM active_staged_snapshots WHERE lease_expires <= ?", (now,))
            rows = db.execute(
                "SELECT space_id, source_name, table_name FROM staged_cds_metadata "
                "WHERE ((expires_at > 0 AND expires_at <= ?) OR (expires_at = 0 AND staged_at <= ?)) "
                "AND snapshot_id NOT IN (SELECT snapshot_id FROM active_staged_snapshots WHERE lease_expires > ?)",
                (now, now - self.CACHE_TTL_SECONDS, now),
            ).fetchall()
            for row in rows:
                db.execute(f"DROP TABLE IF EXISTS {self._quote_identifier(row['table_name'])}")
                db.execute("DELETE FROM staged_cds_metadata WHERE space_id=? AND source_name=?",
                           (row["space_id"], row["source_name"]))
                removed += 1
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()
        return {"removed_snapshots": removed}
