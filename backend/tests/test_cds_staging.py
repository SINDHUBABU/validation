import sqlite3

import pytest

from reconciliation.cds_staging_store import CdsStagingStore
from retrieval.data_retriever import DataRetriever, ExtractionError


def test_staging_store_publishes_all_rows_and_metadata(tmp_path):
    store = CdsStagingStore(tmp_path / "staging.db")

    def extract(on_batch):
        on_batch(
            [{"ID": "001", "AMOUNT": "12.50"}, {"ID": "002", "AMOUNT": "0"}],
            {"batch": 1},
        )
        on_batch([{"ID": "003", "AMOUNT": None}], {"batch": 2})
        return {
            "complete": True,
            "fields": ["ID", "AMOUNT"],
            "column_types": {"ID": "NVARCHAR", "AMOUNT": "DECIMAL"},
            "key_fields": ["ID"],
            "row_count": 3,
            "total_rows_reported": 3,
            "batches": 2,
            "issues": [],
        }

    result = store.stage_source(
        space_id="SPACE_A",
        source_name="ZI_EXAMPLE",
        extract=extract,
    )

    assert result["status"] == "STAGED_COMPLETE"
    assert result["row_count"] == result["total_rows"] == 3
    assert result["key_fields"] == ["ID"]
    assert result["preview"][0] == {"ID": "001", "AMOUNT": "12.50"}
    assert store.list_staged_sources("SPACE_A")[0]["complete"] is True

    with sqlite3.connect(store.db_path) as db:
        rows = db.execute(
            f'SELECT "ID", "AMOUNT" FROM "{result["table_name"]}" ORDER BY rowid'
        ).fetchall()
    assert rows == [("001", "12.50"), ("002", "0"), ("003", None)]


def test_failed_replacement_preserves_previous_complete_snapshot(tmp_path):
    store = CdsStagingStore(tmp_path / "staging.db")

    def initial_extract(on_batch):
        on_batch([{"ID": "old"}], {"batch": 1})
        return {
            "complete": True,
            "fields": ["ID"],
            "column_types": {"ID": "NVARCHAR"},
            "key_fields": ["ID"],
            "row_count": 1,
            "total_rows_reported": 1,
            "batches": 1,
        }

    original = store.stage_source(
        space_id="SPACE_A",
        source_name="ZI_EXAMPLE",
        extract=initial_extract,
    )

    def failing_extract(on_batch):
        on_batch([{"ID": "partial"}], {"batch": 1})
        raise RuntimeError("ADT connection interrupted")

    with pytest.raises(RuntimeError, match="connection interrupted"):
        store.stage_source(
            space_id="SPACE_A",
            source_name="ZI_EXAMPLE",
            extract=failing_extract,
        )

    staged = store.list_staged_sources("SPACE_A")
    assert len(staged) == 1
    assert staged[0]["table_name"] == original["table_name"]
    with sqlite3.connect(store.db_path) as db:
        rows = db.execute(
            f'SELECT "ID" FROM "{original["table_name"]}"'
        ).fetchall()
    assert rows == [("old",)]


def test_incomplete_extract_is_not_published(tmp_path):
    store = CdsStagingStore(tmp_path / "staging.db")

    def incomplete_extract(on_batch):
        on_batch([{"ID": "1"}], {"batch": 1})
        return {
            "complete": False,
            "fields": ["ID"],
            "row_count": 1,
            "total_rows_reported": 2,
            "batches": 1,
        }

    with pytest.raises(ValueError, match="did not verify as complete"):
        store.stage_source(
            space_id="SPACE_A",
            source_name="ZI_EXAMPLE",
            extract=incomplete_extract,
        )
    assert store.list_staged_sources("SPACE_A") == []


def test_empty_complete_extract_creates_an_empty_local_table(tmp_path):
    store = CdsStagingStore(tmp_path / "staging.db")

    def empty_extract(_on_batch):
        return {
            "complete": True,
            "fields": ["ID", "VALUE"],
            "column_types": {"ID": "NVARCHAR", "VALUE": "DECIMAL"},
            "key_fields": ["ID"],
            "row_count": 0,
            "total_rows_reported": 0,
            "batches": 1,
            "issues": [],
        }

    result = store.stage_source(
        space_id="SPACE_A",
        source_name="ZI_EMPTY",
        extract=empty_extract,
    )
    assert result["row_count"] == 0
    assert result["preview"] == []

    with sqlite3.connect(store.db_path) as db:
        rows = db.execute(
            f'PRAGMA table_info("{result["table_name"]}")'
        ).fetchall()
    assert [row[1] for row in rows] == ["ID", "VALUE"]


def test_fetch_all_streams_complete_keyset_pages():
    retriever = object.__new__(DataRetriever)
    retriever.default_row_limit = 100
    retriever.max_row_limit = 1000
    retriever._discover_metadata = lambda _name: {
        "parameterized": False,
        "key_fields": ["ID"],
    }
    retriever.discover_columns = lambda _name: [
        {"name": "ID", "type": "NVARCHAR"},
        {"name": "VALUE", "type": "DECIMAL"},
    ]
    retriever.fetch_query = lambda **_kwargs: {
        "rows": [{"ID": "1", "VALUE": "10"}, {"ID": "2", "VALUE": "20"}],
        "total_rows": 3,
    }
    pages = {
        None: ([{"ID": "1", "VALUE": "10"}, {"ID": "2", "VALUE": "20"}], 3),
        ("2",): ([{"ID": "3", "VALUE": "30"}], 1),
        ("3",): ([], 0),
    }
    cursors = []

    def fetch_batch(_view, _fields, _keys, cursor, _size):
        cursors.append(cursor)
        rows, total = pages[cursor]
        return {"rows": rows, "total_rows": total}

    retriever._fetch_batch = fetch_batch
    streamed_rows = []
    summary = retriever.fetch_all(
        "ZI_EXAMPLE",
        key_fields=["ID"],
        batch_size=2,
        on_batch=lambda rows, _info: streamed_rows.extend(rows),
    )

    assert summary["complete"] is True
    assert summary["row_count"] == summary["total_rows_reported"] == 3
    assert summary["batches"] == 3
    assert summary["streamed"] is True
    assert summary["rows"] == []
    assert [row["ID"] for row in streamed_rows] == ["1", "2", "3"]
    assert cursors == [None, ("2",), ("3",)]


def test_fetch_all_rejects_large_extract_without_stable_key():
    retriever = object.__new__(DataRetriever)
    retriever.default_row_limit = 100
    retriever.max_row_limit = 1000
    retriever._discover_metadata = lambda _name: {
        "parameterized": False,
        "key_fields": [],
    }
    retriever.discover_columns = lambda _name: [{"name": "VALUE", "type": "DECIMAL"}]
    retriever.fetch_query = lambda **_kwargs: {
        "rows": [{"VALUE": "10"}],
        "total_rows": 2,
    }

    with pytest.raises(ExtractionError, match="Cannot safely paginate"):
        retriever.fetch_all("ZI_EXAMPLE", batch_size=1)


def test_compatible_snapshot_reused_and_metadata_change_refreshes(tmp_path):
    store = CdsStagingStore(tmp_path / "cache.db")
    calls = []

    def extract(on_batch, value):
        calls.append(value)
        on_batch([{"ID": value}], {"batch": 1})
        return {"complete": True, "fields": ["ID"], "column_types": {"ID": "NVARCHAR"},
                "key_fields": ["ID"], "row_count": 1, "total_rows_reported": 1, "batches": 1}

    first = store.stage_source(space_id="S", source_name="SRC", metadata_fingerprint="schema-v1",
                               extract=lambda writer: extract(writer, "first"))
    reused = store.stage_source(space_id="S", source_name="SRC", metadata_fingerprint="schema-v1",
                                extract=lambda writer: extract(writer, "should-not-run"))
    refreshed = store.stage_source(space_id="S", source_name="SRC", metadata_fingerprint="schema-v2",
                                   extract=lambda writer: extract(writer, "second"))
    assert first["snapshot_id"] == reused["snapshot_id"]
    assert reused["cache_status"] == "REUSED"
    assert refreshed["snapshot_id"] != first["snapshot_id"]
    assert calls == ["first", "second"]


def test_expired_snapshot_is_not_reused_and_cleanup_removes_it(tmp_path):
    store = CdsStagingStore(tmp_path / "cache.db")

    def extract(on_batch):
        on_batch([{"ID": "1"}], {"batch": 1})
        return {"complete": True, "fields": ["ID"], "column_types": {"ID": "NVARCHAR"},
                "key_fields": ["ID"], "row_count": 1, "total_rows_reported": 1, "batches": 1}

    snapshot = store.stage_source(space_id="S", source_name="SRC", metadata_fingerprint="v1", extract=extract)
    assert store.get_compatible_snapshot(space_id="S", source_name="SRC", metadata_fingerprint="v1",
                                         now=snapshot["expires_at"] + 1) is None
    assert store.cleanup_expired(now=snapshot["expires_at"] + 1)["removed_snapshots"] == 1
    assert store.list_staged_sources("S") == []


def test_cleanup_preserves_snapshots_with_active_execution_lease(tmp_path):
    store = CdsStagingStore(tmp_path / "cache.db")
    def extract(on_batch):
        on_batch([{"ID": "1"}], {"batch": 1})
        return {"complete": True, "fields": ["ID"], "column_types": {"ID": "NVARCHAR"},
                "key_fields": ["ID"], "row_count": 1, "total_rows_reported": 1, "batches": 1}
    snapshot = store.stage_source(space_id="S", source_name="SRC", metadata_fingerprint="v1", extract=extract)
    with sqlite3.connect(store.db_path) as db:
        db.execute("INSERT INTO active_staged_snapshots VALUES (?, ?, ?)",
                   (snapshot["snapshot_id"], "active-run", snapshot["expires_at"] + 1000))
    assert store.cleanup_expired(now=snapshot["expires_at"] + 1)["removed_snapshots"] == 0
    assert store.list_staged_sources("S")


def test_local_sql_requires_snapshot_bound_approval_and_is_read_only(tmp_path):
    store = CdsStagingStore(tmp_path / "cache.db")

    def extract(on_batch):
        on_batch([{"ID": "1", "AMOUNT": "12.50"}], {"batch": 1})
        return {"complete": True, "fields": ["ID", "AMOUNT"],
                "column_types": {"ID": "NVARCHAR", "AMOUNT": "DECIMAL"},
                "key_fields": ["ID"], "row_count": 1, "total_rows_reported": 1, "batches": 1}

    staged = store.stage_source(space_id="S", source_name="SRC", metadata_fingerprint="v1", extract=extract)
    sql = f'SELECT COUNT(*) AS "N" FROM "{staged["table_name"]}"'
    preview = store.validate_read_only_sql(sql, "S")
    result = store.execute_read_only_sql(sql=sql, approved_hash=preview["approval_hash"], space_id="S")
    assert result["rows"] == [{"N": 1}]
    with pytest.raises(ValueError, match="approval is missing or stale"):
        store.execute_read_only_sql(sql=sql + " WHERE 1 = 0", approved_hash=preview["approval_hash"], space_id="S")
    with pytest.raises(Exception):
        store.execute_read_only_sql(sql=f'DELETE FROM "{staged["table_name"]}"',
                                    approved_hash=preview["approval_hash"], space_id="S")


def test_multiple_sources_are_staged_independently_for_one_run(tmp_path):
    store = CdsStagingStore(tmp_path / "cache.db")
    source_names = ["SOURCE_A", "SOURCE_B", "SOURCE_C", "SOURCE_D"]
    run_id = "validation-snapshot"
    staged = []
    for index, source_name in enumerate(source_names):
        def extract(on_batch, row_id=index):
            on_batch([{"ID": str(row_id)}], {"batch": 1})
            return {"complete": True, "fields": ["ID"], "column_types": {"ID": "NVARCHAR"},
                    "key_fields": ["ID"], "row_count": 1, "total_rows_reported": 1, "batches": 1}
        staged.append(store.stage_source(
            space_id="S", source_name=source_name, validation_run_id=run_id,
            metadata_fingerprint=f"schema-{index}", extract=extract,
        ))
    assert len({item["table_name"] for item in staged}) == len(source_names)
    assert all(item["validation_run_id"] == run_id for item in staged)
