"""SQLite-backed registry for source metadata and dependency edges.

This registry stores metadata only. It does not cache raw business rows.
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalise_name(name: str) -> str:
    return str(name or "").strip().upper()

class SourceRegistry:
    def __init__(self, database_path: str | Path | None = None):
        self.database_path = "file:source_registry?mode=memory&cache=shared"
        self._shared_conn = sqlite3.connect(self.database_path, uri=True, check_same_thread=False)
        self._shared_conn.row_factory = sqlite3.Row
        self._initialise()

    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(self.database_path, uri=True, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _initialise(self) -> None:
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sources (
                    source_id TEXT PRIMARY KEY,
                    system_name TEXT NOT NULL,
                    object_name TEXT NOT NULL,
                    object_type TEXT NOT NULL,
                    source_kind TEXT NOT NULL,
                    query_interface TEXT,
                    parameterized INTEGER NOT NULL DEFAULT 0,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    metadata_status TEXT NOT NULL DEFAULT 'DISCOVERED',
                    discovered_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_sources_object_name
                ON sources(object_name)
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS dependencies (
                    model_id TEXT NOT NULL,
                    dependency_id TEXT NOT NULL,
                    relationship_json TEXT NOT NULL DEFAULT '{}',
                    discovered_at TEXT NOT NULL,
                    PRIMARY KEY (model_id, dependency_id)
                )
            """)

    def upsert_source(
        self,
        *,
        source_id: str,
        system_name: str,
        object_name: str,
        object_type: str,
        source_kind: str,
        query_interface: str | None = None,
        parameterized: bool = False,
        metadata: dict[str, Any] | None = None,
        metadata_status: str = "DISCOVERED",
    ) -> dict[str, Any]:
        required = {
            "source_id": source_id,
            "system_name": system_name,
            "object_name": object_name,
            "object_type": object_type,
            "source_kind": source_kind,
        }
        missing = [k for k, v in required.items() if not str(v or "").strip()]
        if missing:
            raise ValueError(f"Missing required source fields: {', '.join(missing)}")

        now = _utc_now()
        payload = json.dumps(metadata or {}, ensure_ascii=False, default=str)
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT discovered_at FROM sources WHERE source_id = ?",
                (source_id,),
            ).fetchone()
            discovered_at = existing["discovered_at"] if existing else now
            conn.execute("""
                INSERT INTO sources (
                    source_id, system_name, object_name, object_type,
                    source_kind, query_interface, parameterized,
                    metadata_json, metadata_status, discovered_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_id) DO UPDATE SET
                    system_name=excluded.system_name,
                    object_name=excluded.object_name,
                    object_type=excluded.object_type,
                    source_kind=excluded.source_kind,
                    query_interface=excluded.query_interface,
                    parameterized=excluded.parameterized,
                    metadata_json=excluded.metadata_json,
                    metadata_status=excluded.metadata_status,
                    updated_at=excluded.updated_at
            """, (
                source_id, system_name, object_name, object_type, source_kind,
                query_interface, int(parameterized), payload, metadata_status,
                discovered_at, now,
            ))
        return self.get_source(source_id)

    def get_source(self, source_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM sources WHERE source_id = ?", (source_id,)
            ).fetchone()
        return self._source_row(row) if row else None

    def find_by_name(self, object_name: str) -> list[dict[str, Any]]:
        name = _normalise_name(object_name)
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM sources WHERE UPPER(object_name) = ? "
                "ORDER BY system_name, source_id",
                (name,),
            ).fetchall()
        return [self._source_row(row) for row in rows]

    def list_sources(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM sources ORDER BY system_name, object_name"
            ).fetchall()
        return [self._source_row(row) for row in rows]

    def replace_dependencies(
        self,
        model_id: str,
        dependencies: list[dict[str, Any]],
    ) -> None:
        if not model_id:
            raise ValueError("model_id is required")
        now = _utc_now()
        with self._connect() as conn:
            conn.execute("DELETE FROM dependencies WHERE model_id = ?", (model_id,))
            for dep in dependencies:
                dependency_id = str(dep.get("source_id") or dep.get("name") or "").strip()
                if not dependency_id:
                    continue
                conn.execute("""
                    INSERT OR REPLACE INTO dependencies
                    (model_id, dependency_id, relationship_json, discovered_at)
                    VALUES (?, ?, ?, ?)
                """, (
                    model_id,
                    dependency_id,
                    json.dumps(dep, ensure_ascii=False, default=str),
                    now,
                ))

    def get_dependencies(self, model_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("""
                SELECT dependency_id, relationship_json, discovered_at
                FROM dependencies WHERE model_id = ? ORDER BY dependency_id
            """, (model_id,)).fetchall()
        result = []
        for row in rows:
            try:
                details = json.loads(row["relationship_json"])
            except (TypeError, json.JSONDecodeError):
                details = {}
            details.setdefault("source_id", row["dependency_id"])
            details["discovered_at"] = row["discovered_at"]
            result.append(details)
        return result

    @staticmethod
    def _source_row(row: sqlite3.Row) -> dict[str, Any]:
        item = dict(row)
        try:
            item["metadata"] = json.loads(item.pop("metadata_json"))
        except (TypeError, json.JSONDecodeError):
            item["metadata"] = {}
        item["parameterized"] = bool(item["parameterized"])
        return item
