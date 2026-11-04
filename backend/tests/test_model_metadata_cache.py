from reconciliation.datasphere_space_cache import DatasphereSpaceCache


def test_full_model_metadata_and_lineage_persist_across_cache_instances(tmp_path):
    database = tmp_path / "metadata-cache.sqlite3"
    dependency_tree = {
        "name": "AM_Orders",
        "object_type": "analytic_model",
        "field_catalog": [{"name": "NetAmount", "aggregation": "SUM"}],
        "dependencies": [{
            "name": "FV_Orders",
            "object_type": "view",
            "dependencies": [{"name": "ZI_Orders", "object_type": "local_table"}],
        }],
    }
    first = DatasphereSpaceCache(database)
    first.upsert_model_metadata("SPACE_A", "AM_Orders", {
        "dependency_tree": dependency_tree,
        "field_catalog": dependency_tree["field_catalog"],
        "lineage_path": ["AM_Orders", "FV_Orders", "ZI_Orders"],
        "response": {"view_name": "AM_Orders", "fields": dependency_tree["field_catalog"]},
    })
    first.close()

    reopened = DatasphereSpaceCache(database)
    cached = reopened.get_model_metadata("SPACE_A", "am_orders")
    assert cached is not None
    assert cached["metadata"]["dependency_tree"] == dependency_tree
    assert cached["metadata"]["lineage_path"] == ["AM_Orders", "FV_Orders", "ZI_Orders"]
    assert cached["metadata"]["response"]["view_name"] == "AM_Orders"
    assert len(reopened.list_cached_model_metadata("SPACE_A")) == 1
    assert reopened.list_cached_model_metadata("SPACE_B") == []
    reopened.close()


def test_model_metadata_upsert_replaces_case_insensitive_model_key(tmp_path):
    cache = DatasphereSpaceCache(tmp_path / "metadata-cache.sqlite3")
    cache.upsert_model_metadata("SPACE_A", "AM_Orders", {"revision": 1})
    cache.upsert_model_metadata("SPACE_A", "am_orders", {"revision": 2})

    entries = cache.list_cached_model_metadata("SPACE_A")
    assert len(entries) == 1
    assert entries[0]["metadata"] == {"revision": 2}
    cache.close()
