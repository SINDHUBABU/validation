"""
Datasphere Space Cache Service
Manages persistent caching of Datasphere space views, local tables,
and remote tables replicated from Eclipse ADT (S/4HANA CDS views).
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger("DatasphereSpaceCache")

DEFAULT_REPLICATED_ADT_TABLES: List[Dict[str, Any]] = []
DEFAULT_ANALYTICAL_MODELS: List[Dict[str, Any]] = []


REPLICATION_SPACE_ID = os.getenv("DATASPHERE_REPLICATION_SPACE", "").strip()


def metadata_cache_ttl_seconds() -> int:
    try:
        configured = int(os.getenv("DATASPHERE_METADATA_CACHE_TTL_SECONDS", "3600"))
    except (TypeError, ValueError):
        configured = 3600
    return max(60, configured)


class DatasphereSpaceCache:
    """
    SQLite persistent cache for SAP Datasphere space objects.
    Maintains inventory of views, local tables, and remote tables replicated from Eclipse ADT.
    Caches local tables from the configured Datasphere replication space.
    Configure DATASPHERE_REPLICATION_SPACE; the code does not assume a tenant-specific ID.
    """

    def __init__(self, db_path: str | Path | None = None):
        if db_path is None:
            self.db_path = str(Path(__file__).resolve().parents[1] / "datasphere_cache.db")
            self._use_uri = False
        elif str(db_path) == ":memory:":
            self.db_path = f"file:datasphere_cache_{uuid.uuid4().hex}?mode=memory&cache=shared"
            self._use_uri = True
        else:
            self.db_path = str(db_path)
            self._use_uri = self.db_path.startswith("file:")
        if not self._use_uri:
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._shared_conn = sqlite3.connect(
            self.db_path,
            uri=self._use_uri,
            timeout=20.0,
            check_same_thread=False,
        )
        self._shared_conn.row_factory = sqlite3.Row
        self._init_db()

    @contextmanager
    def _db(self):
        conn = sqlite3.connect(
            self.db_path,
            uri=self._use_uri,
            timeout=20.0,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    def _init_db(self):
        with self._db() as db:
            db.execute("PRAGMA journal_mode = WAL")
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS space_table_cache (
                    space_id TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    table_key TEXT NOT NULL,
                    cds_view_name TEXT NOT NULL,
                    table_type TEXT NOT NULL,           -- 'local_table', 'remote_table', 'view'
                    replicated_from_adt INTEGER DEFAULT 1,
                    description TEXT,
                    key_fields_json TEXT DEFAULT '[]',
                    measures_json TEXT DEFAULT '[]',
                    dimensions_json TEXT DEFAULT '[]',
                    payload_json TEXT DEFAULT '{}',
                    last_synced REAL NOT NULL,
                    PRIMARY KEY (space_id, table_key)
                );

                CREATE INDEX IF NOT EXISTS idx_stc_lookup 
                ON space_table_cache(space_id, table_key);

                CREATE INDEX IF NOT EXISTS idx_stc_cds_name 
                ON space_table_cache(space_id, cds_view_name);

                CREATE TABLE IF NOT EXISTS space_analytical_model_cache (
                    space_id TEXT NOT NULL,
                    model_name TEXT NOT NULL,
                    model_key TEXT NOT NULL,
                    source_view_name TEXT,
                    reconciliation_grain TEXT,
                    description TEXT,
                    measures_json TEXT DEFAULT '[]',
                    dimensions_json TEXT DEFAULT '[]',
                    csn_definition_json TEXT DEFAULT '{}',
                    last_synced REAL NOT NULL,
                    PRIMARY KEY (space_id, model_key)
                );

                CREATE INDEX IF NOT EXISTS idx_samc_lookup
                ON space_analytical_model_cache(space_id, model_key);

                CREATE TABLE IF NOT EXISTS datasphere_model_metadata_cache (
                    space_id TEXT NOT NULL,
                    model_name TEXT NOT NULL,
                    model_key TEXT NOT NULL,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    last_synced REAL NOT NULL,
                    PRIMARY KEY (space_id, model_key)
                );

                CREATE INDEX IF NOT EXISTS idx_dmmc_lookup
                ON datasphere_model_metadata_cache(space_id, model_key);
                """
            )
        # Tables and models are managed dynamically via cache and Datasphere APIs

    def close(self) -> None:
        """Close the cache's anchor connection."""
        self._shared_conn.close()

    @staticmethod
    def _norm(name: str) -> str:
        return str(name or "").strip().lower()

    @staticmethod
    def normalize_cds_name(name: str) -> str:
        """Normalize known technical wrappers only; never infer a business object."""
        raw = str(name or "").strip()
        upper = raw.upper()
        for prefix in ("RT_", "LT_", "FV_", "GV_", "AM_", "V_"):
            if upper.startswith(prefix):
                return raw[len(prefix):]
        return raw

    def seed_default_inventory(self, space_id: str, force_table_type: Optional[str] = None):
        """Pre-populates known replicated CDS views into cache for instant availability."""
        now = time.time()
        with self._db() as db:
            count = db.execute("SELECT COUNT(*) as c FROM space_table_cache WHERE space_id = ?", (space_id,)).fetchone()["c"]
            if count == 0:
                rows = []
                for t in DEFAULT_REPLICATED_ADT_TABLES:
                    name = t["table_name"]
                    key = self._norm(name)
                    actual_type = force_table_type or t["table_type"]
                    rows.append((
                        space_id,
                        name,
                        key,
                        t["cds_view_name"],
                        actual_type,
                        t["replicated_from_adt"],
                        t["description"],
                        json.dumps(t["key_fields"]),
                        json.dumps(t["measures"]),
                        json.dumps(t["dimensions"]),
                        json.dumps(t),
                        now
                    ))
                db.executemany(
                    """
                    INSERT OR REPLACE INTO space_table_cache
                        (space_id, table_name, table_key, cds_view_name, table_type,
                         replicated_from_adt, description, key_fields_json, measures_json,
                         dimensions_json, payload_json, last_synced)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows
                )
                logger.info(f"Seeded {len(rows)} replicated ADT tables into cache for space [{space_id}] (type: {force_table_type or 'mixed'}).")

    def upsert_table(
        self,
        space_id: str,
        table_name: str,
        table_type: str = "remote_table",
        cds_view_name: Optional[str] = None,
        replicated_from_adt: bool = True,
        description: str = "",
        key_fields: Optional[List[str]] = None,
        measures: Optional[List[str]] = None,
        dimensions: Optional[List[str]] = None,
        payload: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Caches or updates a single space view / remote table."""
        now = time.time()
        clean_name = table_name.strip()
        table_key = self._norm(clean_name)
        cds_name = cds_view_name.strip() if cds_view_name else clean_name

        with self._db() as db:
            db.execute(
                """
                INSERT INTO space_table_cache
                    (space_id, table_name, table_key, cds_view_name, table_type,
                     replicated_from_adt, description, key_fields_json, measures_json,
                     dimensions_json, payload_json, last_synced)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(space_id, table_key) DO UPDATE SET
                    table_name = excluded.table_name,
                    cds_view_name = excluded.cds_view_name,
                    table_type = excluded.table_type,
                    replicated_from_adt = excluded.replicated_from_adt,
                    description = excluded.description,
                    key_fields_json = excluded.key_fields_json,
                    measures_json = excluded.measures_json,
                    dimensions_json = excluded.dimensions_json,
                    payload_json = excluded.payload_json,
                    last_synced = excluded.last_synced
                """,
                (
                    space_id,
                    clean_name,
                    table_key,
                    cds_name,
                    table_type,
                    1 if replicated_from_adt else 0,
                    description,
                    json.dumps(key_fields or []),
                    json.dumps(measures or []),
                    json.dumps(dimensions or []),
                    json.dumps(payload or {}),
                    now
                )
            )
        return {
            "space_id": space_id,
            "table_name": clean_name,
            "cds_view_name": cds_name,
            "table_type": table_type,
            "replicated_from_adt": replicated_from_adt
        }

    def sync_space_inventory(
        self,
        space_id: str,
        tables: List[Dict[str, Any] | str],
        complete_snapshot: bool = False
    ) -> int:
        """
        Syncs a list of tables/views from Datasphere discovery into the SQLite cache.
        """
        now = time.time()
        rows = []
        for item in tables:
            if isinstance(item, str):
                name = item.strip()
                t_type = "remote_table" if name.upper().startswith("RT_") else "view"
                desc = f"Space {t_type} {name}"
                cds_name = name
                payload = {"name": name}
                key_fields, measures, dimensions = [], [], []
                replicated_from_adt = False
            elif isinstance(item, dict):
                name = str(item.get("table_name") or item.get("name") or item.get("technicalName") or "").strip()
                if not name:
                    continue
                t_type = item.get("table_type", "remote_table" if name.upper().startswith("RT_") else "view")
                desc = item.get("description", "")
                cds_name = item.get("cds_view_name") or name
                key_fields = item.get("key_fields", [])
                measures = item.get("measures", [])
                dimensions = item.get("dimensions", [])
                payload = item
                replicated_from_adt = bool(item.get("replicated_from_adt", False))
            else:
                continue

            rows.append((
                space_id,
                name,
                self._norm(name),
                cds_name,
                t_type,
                1 if replicated_from_adt else 0,
                desc,
                json.dumps(key_fields),
                json.dumps(measures),
                json.dumps(dimensions),
                json.dumps(payload),
                now
            ))

        with self._db() as db:
            if complete_snapshot:
                db.execute("DELETE FROM space_table_cache WHERE space_id = ?", (space_id,))
            db.executemany(
                """
                INSERT OR REPLACE INTO space_table_cache
                    (space_id, table_name, table_key, cds_view_name, table_type,
                     replicated_from_adt, description, key_fields_json, measures_json,
                     dimensions_json, payload_json, last_synced)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows
            )
        return len(rows)

    def list_cached_tables(
        self,
        space_id: str,
        filter_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Lists all cached space tables and remote tables replicated from ADT."""
        query = """
            SELECT space_id, table_name, cds_view_name, table_type, replicated_from_adt,
                   description, key_fields_json, measures_json, dimensions_json, last_synced
            FROM space_table_cache
            WHERE space_id = ?
        """
        params: List[Any] = [space_id]
        if filter_type:
            query += " AND table_type = ?"
            params.append(filter_type)
        query += " ORDER BY table_name COLLATE NOCASE"

        with self._db() as db:
            rows = db.execute(query, params).fetchall()

        result = []
        for r in rows:
            result.append({
                "space_id": r["space_id"],
                "table_name": r["table_name"],
                "cds_view_name": r["cds_view_name"],
                "table_type": r["table_type"],
                "replicated_from_adt": bool(r["replicated_from_adt"]),
                "description": r["description"],
                "key_fields": json.loads(r["key_fields_json"] or "[]"),
                "measures": json.loads(r["measures_json"] or "[]"),
                "dimensions": json.loads(r["dimensions_json"] or "[]"),
                "last_synced": r["last_synced"]
            })
        return result

    def match_replicated_source(
        self,
        space_id: str,
        candidate_name: str,
        replication_space: str = REPLICATION_SPACE_ID
    ) -> Optional[Dict[str, Any]]:
        """
        Matches an underlying table/view name from an Analytical Model against the cache.
        Checks:
        1. Configured replication-space local tables (when separately indexed)
        2. The selected modeling space
        Checks:
        - Exact match on table_name / local table name
        - Normalized match (stripping RT_, LT_, V_)
        - Match on cds_view_name
        """
        if not candidate_name:
            return None

        clean_name = candidate_name.strip()
        norm_key = self._norm(clean_name)

        # Check the configured replication space first when a separate inventory exists
        with self._db() as db:
            row = db.execute(
                """
                SELECT space_id, table_name, cds_view_name, table_type, replicated_from_adt,
                       description, key_fields_json, measures_json, dimensions_json
                FROM space_table_cache
                WHERE (space_id = ? OR space_id = ?) AND (
                    table_key = ?
                    OR LOWER(cds_view_name) = ?
                )
                ORDER BY CASE WHEN space_id = ? THEN 0 ELSE 1 END
                LIMIT 1
                """,
                (replication_space, space_id, norm_key, norm_key, replication_space)
            ).fetchone()

        if row:
            is_repl_space = (row["space_id"] == replication_space)
            return {
                "space_id": row["space_id"],
                "table_name": row["table_name"],
                "cds_view_name": row["cds_view_name"],
                "table_type": row["table_type"],
                "replicated_from_adt": bool(row["replicated_from_adt"]),
                "description": row["description"],
                "key_fields": json.loads(row["key_fields_json"] or "[]"),
                "measures": json.loads(row["measures_json"] or "[]"),
                "dimensions": json.loads(row["dimensions_json"] or "[]"),
                "matched_by": candidate_name,
                "verified_in_replication_space": is_repl_space,
                "replication_space": replication_space
            }
        return None

    def list_replication_local_tables(
        self,
        replication_space: str = REPLICATION_SPACE_ID
    ) -> List[Dict[str, Any]]:
        """Lists local tables cached from the configured replication space."""
        return self.list_cached_tables(replication_space, filter_type="local_table")

    def match_replication_local_tables(
        self,
        candidate_names: List[str],
        replication_space: str = REPLICATION_SPACE_ID,
    ) -> List[Dict[str, Any]]:
        """Return exact, case-insensitive matches against configured replication-space local-table names."""
        candidates = {
            str(name or "").strip().rsplit(".", 1)[-1].casefold()
            for name in candidate_names
            if str(name or "").strip()
        }
        if not candidates:
            return []

        return [
            {
                **table,
                "matched_by": table["table_name"],
                "verified_in_replication_space": True,
                "replication_space": replication_space,
            }
            for table in self.list_replication_local_tables(replication_space)
            if table["table_name"].strip().casefold() in candidates
        ]

    # --------------------------------------------------------------------------
    # Analytical Models Cache Management
    # --------------------------------------------------------------------------
    def seed_default_models(self, space_id: str, overwrite: bool = False):
        """Deprecated no-op: analytical models must be discovered from live metadata.

        Inferring models from table names produces false lineage, so this method
        intentionally does not create or copy synthetic analytical models.
        """
        logger.debug("Synthetic analytical-model seeding is disabled for space %s", space_id)
        return None

    def list_cached_models(self, space_id: str) -> List[Dict[str, Any]]:
        """Returns all cached Analytical Models in the given space."""
        with self._db() as db:
            rows = db.execute(
                """
                SELECT space_id, model_name, source_view_name, reconciliation_grain,
                       description, measures_json, dimensions_json, last_synced
                FROM space_analytical_model_cache
                WHERE space_id = ?
                ORDER BY model_name ASC
                """,
                (space_id,)
            ).fetchall()

        if not rows:
            # Auto-seed if empty
            self.seed_default_models(space_id)
            with self._db() as db:
                rows = db.execute(
                    """
                    SELECT space_id, model_name, source_view_name, reconciliation_grain,
                           description, measures_json, dimensions_json, last_synced
                    FROM space_analytical_model_cache
                    WHERE space_id = ?
                    ORDER BY model_name ASC
                    """,
                    (space_id,)
                ).fetchall()

        result = []
        for r in rows:
            result.append({
                "model_name": r["model_name"],
                "space_id": r["space_id"],
                "source_view_name": r["source_view_name"],
                "reconciliation_grain": r["reconciliation_grain"],
                "description": r["description"],
                "measures": json.loads(r["measures_json"] or "[]"),
                "dimensions": json.loads(r["dimensions_json"] or "[]"),
                "last_synced": r["last_synced"]
            })
        return result

    def get_cached_model(self, space_id: str, model_name: str) -> Optional[Dict[str, Any]]:
        """Gets a single analytical model by name."""
        key = self._norm(model_name)
        with self._db() as db:
            r = db.execute(
                """
                SELECT space_id, model_name, source_view_name, reconciliation_grain,
                       description, measures_json, dimensions_json, csn_definition_json, last_synced
                FROM space_analytical_model_cache
                WHERE space_id = ? AND model_key = ?
                LIMIT 1
                """,
                (space_id, key)
            ).fetchone()

        if r:
            return {
                "model_name": r["model_name"],
                "space_id": r["space_id"],
                "source_view_name": r["source_view_name"],
                "reconciliation_grain": r["reconciliation_grain"],
                "description": r["description"],
                "measures": json.loads(r["measures_json"] or "[]"),
                "dimensions": json.loads(r["dimensions_json"] or "[]"),
                "csn_definition": json.loads(r["csn_definition_json"] or "{}"),
                "last_synced": r["last_synced"]
            }
        return None

    def add_or_update_model(
        self,
        space_id: str,
        model_name: str,
        source_view_name: str = "",
        measures: Optional[List[str]] = None,
        dimensions: Optional[List[str]] = None,
        description: str = "",
        reconciliation_grain: str = "",
        csn_definition: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Dynamically registers or updates an Analytical Model in the space cache."""
        clean_name = str(model_name).strip().upper()
        key = self._norm(clean_name)
        measures = measures or []
        dimensions = dimensions or []
        csn = csn_definition or {}
        now = time.time()

        with self._db() as db:
            db.execute(
                """
                INSERT OR REPLACE INTO space_analytical_model_cache
                    (space_id, model_name, model_key, source_view_name, reconciliation_grain,
                     description, measures_json, dimensions_json, csn_definition_json, last_synced)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    space_id,
                    clean_name,
                    key,
                    source_view_name or clean_name,
                    reconciliation_grain,
                    description or f"Analytical Model {clean_name} in Datasphere Space {space_id}",
                    json.dumps(measures),
                    json.dumps(dimensions),
                    json.dumps(csn),
                    now
                )
            )

        logger.info(f"Registered Analytical Model [{clean_name}] in space [{space_id}] cache.")
        return {
            "model_name": clean_name,
            "space_id": space_id,
            "source_view_name": source_view_name,
            "reconciliation_grain": reconciliation_grain,
            "description": description,
            "measures": measures,
            "dimensions": dimensions,
            "last_synced": now
        }

    def delete_model(self, space_id: str, model_name: str) -> bool:
        """Removes a model from the cache."""
        key = self._norm(model_name)
        with self._db() as db:
            res = db.execute(
                "DELETE FROM space_analytical_model_cache WHERE space_id = ? AND model_key = ?",
                (space_id, key)
            )
            return res.rowcount > 0

    def get_model_metadata(self, space_id: str, model_name: str) -> Optional[Dict[str, Any]]:
        """Return the persisted full metadata/lineage payload for one model."""
        with self._db() as db:
            row = db.execute(
                """
                SELECT space_id, model_name, metadata_json, last_synced
                FROM datasphere_model_metadata_cache
                WHERE space_id = ? AND model_key = ?
                LIMIT 1
                """,
                (space_id, self._norm(model_name)),
            ).fetchone()
        if not row:
            return None
        try:
            payload = json.loads(row["metadata_json"] or "{}")
        except (TypeError, json.JSONDecodeError):
            logger.warning("Ignoring invalid cached metadata for %s/%s", space_id, model_name)
            return None
        return {
            "space_id": row["space_id"],
            "model_name": row["model_name"],
            "metadata": payload,
            "last_synced": row["last_synced"],
        }

    def upsert_model_metadata(
        self,
        space_id: str,
        model_name: str,
        metadata: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Persist complete design-time metadata and lineage for a model."""
        clean_name = str(model_name or "").strip()
        if not clean_name:
            raise ValueError("model_name is required to cache Datasphere metadata")
        if not isinstance(metadata, dict):
            raise TypeError("metadata must be a JSON-compatible dictionary")
        synced_at = time.time()
        serialized = json.dumps(metadata, ensure_ascii=False, default=str)
        with self._db() as db:
            db.execute(
                """
                INSERT INTO datasphere_model_metadata_cache
                    (space_id, model_name, model_key, metadata_json, last_synced)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(space_id, model_key) DO UPDATE SET
                    model_name = excluded.model_name,
                    metadata_json = excluded.metadata_json,
                    last_synced = excluded.last_synced
                """,
                (space_id, clean_name, self._norm(clean_name), serialized, synced_at),
            )
        return {
            "space_id": space_id,
            "model_name": clean_name,
            "last_synced": synced_at,
        }

    def list_cached_model_metadata(
        self,
        space_id: str,
        *,
        since: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """List cached models, optionally restricted to entries newer than a timestamp."""
        query = (
            "SELECT space_id, model_name, metadata_json, last_synced "
            "FROM datasphere_model_metadata_cache WHERE space_id = ?"
        )
        params: List[Any] = [space_id]
        if since is not None:
            query += " AND last_synced >= ?"
            params.append(since)
        query += " ORDER BY model_name COLLATE NOCASE"
        with self._db() as db:
            rows = db.execute(query, params).fetchall()
        result = []
        for row in rows:
            try:
                metadata = json.loads(row["metadata_json"] or "{}")
            except (TypeError, json.JSONDecodeError):
                logger.warning("Ignoring invalid cached metadata for %s/%s", space_id, row["model_name"])
                continue
            result.append({
                "space_id": row["space_id"],
                "model_name": row["model_name"],
                "metadata": metadata,
                "last_synced": row["last_synced"],
            })
        return result
