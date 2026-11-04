import pytest

from reconciliation.execution_target import (
    ExecutionTargetError,
    ReconciliationGrainError,
    bind_readonly_sql_to_target,
    compile_reconciliation_sql,
    resolve_reconciliation_grain,
    resolve_execution_target,
)


def tree(runtime=None, columns=None, fact_count=1):
    facts = []
    for index in range(fact_count):
        fact = {
            "name": f"FV_{index}",
            "object_type": "fact_model",
            "status": "resolved",
            "field_catalog": columns or [
                {"name": "FiscalYear", "is_key": True},
                {"name": "DSO_Critical", "aggregation": "SUM"},
            ],
        }
        if runtime is not None:
            fact["runtime_object"] = runtime
            fact["schema"] = "RADH_S3X"
        facts.append(fact)
    return {
        "name": "AM_Customer_Receivables_KPI",
        "object_type": "analytic_model",
        "status": "resolved",
        "dependencies": facts,
    }


def test_analytical_model_resolves_to_explicit_fact_runtime_object():
    target = resolve_execution_target(
        tree("FV_CUSTOMER_RECEIVABLES"),
        selected_fields=["DSO_Critical"],
        required_keys=["FiscalYear"],
    )
    sql = compile_reconciliation_sql(
        target, selected_fields=["DSO_Critical"], required_keys=["FiscalYear"]
    )
    assert 'FROM "RADH_S3X"."FV_CUSTOMER_RECEIVABLES"' in sql
    assert '"AM_Customer_Receivables_KPI"' not in sql


def test_display_name_alone_cannot_infer_physical_object():
    with pytest.raises(ExecutionTargetError, match="no verified physical/runtime"):
        resolve_execution_target(tree(), selected_fields=["DSO_Critical"], required_keys=["FiscalYear"])


def test_missing_required_column_blocks_execution():
    with pytest.raises(ExecutionTargetError, match="does not expose required field"):
        resolve_execution_target(
            tree("FV_CUSTOMER_RECEIVABLES"),
            selected_fields=["CompanyCode"],
            required_keys=["FiscalYear"],
        )


def test_ambiguous_fact_models_block_execution():
    with pytest.raises(ExecutionTargetError, match="ambiguous"):
        resolve_execution_target(
            tree("FV_CUSTOMER_RECEIVABLES", fact_count=2),
            selected_fields=["DSO_Critical"],
            required_keys=["FiscalYear"],
        )


def test_immediate_second_node_view_is_target_not_terminal_table():
    source = {
        "name": "FV_AR_MODEL",
        "object_type": "view",
        "relationship": "analytic_model_source",
        "status": "resolved",
        "runtime_object": "RUNTIME_FV_AR",
        "schema": "SPACE_A",
        "field_catalog": [
            {"name": "FiscalYear", "is_key": True},
            {"name": "DSO_Critical", "aggregation": "SUM"},
        ],
        "dependencies": [{
            "name": "ZI_AR_TABLE",
            "object_type": "local_table",
            "relationship": "query_source",
            "runtime_object": "RUNTIME_ZI_AR",
            "schema": "SPACE_A",
            "field_catalog": [{"name": "CompanyCode"}],
            "dependencies": [],
        }],
    }
    target = resolve_execution_target(
        {"name": "AM_AR", "object_type": "analytic_model", "dependencies": [source]},
        selected_fields=["DSO_Critical"],
        required_keys=["FiscalYear"],
    )
    assert target.object_name == "RUNTIME_FV_AR"
    assert target.object_type == "view"
    assert "RUNTIME_ZI_AR" not in compile_reconciliation_sql(
        target, selected_fields=["DSO_Critical"], required_keys=["FiscalYear"]
    )


def test_multiple_immediate_views_without_role_are_ambiguous():
    root = {
        "name": "AM_AR",
        "object_type": "analytic_model",
        "dependencies": [
            {"name": "FV_ONE", "object_type": "view", "runtime_object": "ONE", "schema": "S", "field_catalog": [{"name": "ID"}]},
            {"name": "FV_TWO", "object_type": "view", "runtime_object": "TWO", "schema": "S", "field_catalog": [{"name": "ID"}]},
        ],
    }
    with pytest.raises(ExecutionTargetError, match="missing or ambiguous"):
        resolve_execution_target(root, selected_fields=["ID"], required_keys=[])


def test_missing_runtime_metadata_uses_verified_catalog_discovery():
    root = {
        "name": "AM_DYNAMIC",
        "space": "MODEL_SPACE",
        "object_type": "analytic_model",
        "dependencies": [{
            "name": "FV_DYNAMIC",
            "space": "MODEL_SPACE",
            "object_type": "view",
            "relationship": "analytic_model_source",
            "field_catalog": [{"name": "ID", "is_key": True}],
            "dependencies": [],
        }],
    }
    calls = []

    def discover(*, logical_name, space):
        calls.append((logical_name, space))
        return {
            "schema": "CATALOG_SCHEMA",
            "object": "PHYSICAL_DYNAMIC_VIEW",
            "columns": [{"name": "ID", "data_type": "INTEGER"}],
        }

    target = resolve_execution_target(
        root, selected_fields=["ID"], required_keys=[], runtime_discovery=discover
    )
    assert calls == [("FV_DYNAMIC", "MODEL_SPACE")]
    assert target.schema == "CATALOG_SCHEMA"
    assert target.object_name == "PHYSICAL_DYNAMIC_VIEW"


def test_four_column_composite_grain_is_supported_when_declared():
    columns = [
        {"name": name, "is_key": True}
        for name in ("Doc", "Item", "Year", "Company")
    ] + [{"name": "Amount", "aggregation": "SUM"}]
    target = resolve_execution_target(
        tree("RUNTIME_FOUR", columns=columns),
        selected_fields=["Amount"],
        required_keys=[],
    )
    grain = resolve_reconciliation_grain(
        target,
        source_fields=[{"name": name} for name in ("Doc", "Item", "Year", "Company", "Amount")],
        source_key_fields=["Doc", "Item", "Year", "Company"],
    )
    assert grain["strategy"] == "row_level"
    assert grain["keys"] == ["Doc", "Item", "Year", "Company"]


def test_single_key_and_different_composite_key_are_model_specific():
    single = resolve_execution_target(
        tree("RUNTIME_SINGLE", columns=[{"name": "Customer", "is_key": True}, {"name": "Balance", "aggregation": "SUM"}]),
        selected_fields=["Balance"], required_keys=[]
    )
    assert resolve_reconciliation_grain(single)["keys"] == ["Customer"]

    composite = resolve_execution_target(
        tree("RUNTIME_OTHER", columns=[{"name": "Region", "is_key": True}, {"name": "Product", "is_key": True}, {"name": "Value", "aggregation": "SUM"}]),
        selected_fields=["Value"], required_keys=[]
    )
    assert resolve_reconciliation_grain(composite)["keys"] == ["Region", "Product"]


def test_no_key_or_shared_dimension_blocks_instead_of_fabricating_matches():
    target = resolve_execution_target(
        tree("RUNTIME_NO_KEY", columns=[{"name": "Value", "aggregation": "SUM"}]),
        selected_fields=["Value"], required_keys=[]
    )
    with pytest.raises(ReconciliationGrainError):
        resolve_reconciliation_grain(target, source_fields=[{"name": "Other"}])


def test_aggregate_grain_requires_a_verified_shared_dimension():
    target = resolve_execution_target(
        tree("RUNTIME_AGG", columns=[{"name": "Region"}, {"name": "Value", "aggregation": "SUM"}]),
        selected_fields=["Value"], required_keys=[]
    )
    grain = resolve_reconciliation_grain(target, source_fields=[{"name": "Region"}, {"name": "Value"}])
    assert grain["strategy"] == "aggregated"
    assert grain["keys"] == ["Region"]


def test_explicit_fact_to_source_mapping_supports_renamed_grain_fields():
    target = resolve_execution_target(
        tree("RUNTIME_MAPPED", columns=[
            {"name": "BusinessUnit", "is_key": True, "source_field": "Company"},
            {"name": "Amount", "aggregation": "SUM"},
        ]),
        selected_fields=["Amount"], required_keys=[]
    )
    grain = resolve_reconciliation_grain(
        target,
        source_fields=[{"name": "Company"}, {"name": "Amount"}],
        source_key_fields=["Company"],
    )
    assert grain["keys"] == ["BusinessUnit"]
    assert grain["source_keys"] == ["Company"]
    assert grain["field_mappings"] == [{"target_field": "BusinessUnit", "source_field": "Company"}]


def test_fact_projection_ref_is_preserved_as_verified_source_mapping():
    tree = {
        "name": "AM_RENAMED",
        "space": "SPACE",
        "object_type": "analytic_model",
        "field_catalog": [{"name": "BusinessUnit", "is_key": True}, {"name": "Amount", "aggregation": "SUM"}],
        "dependencies": [{
            "name": "FV_RENAMED",
            "space": "SPACE",
            "object_type": "view",
            "relationship": "analytic_model_source",
            "runtime_object": "FV_RENAMED_RUNTIME",
            "schema": "SPACE",
            "field_catalog": [{"name": "BusinessUnit", "is_key": True}, {"name": "Amount", "aggregation": "SUM"}],
            "definition": {"columns": [
                {"ref": ["Company"], "as": "BusinessUnit"},
                {"ref": ["Amount"], "as": "Amount"},
            ]},
        }],
    }
    target = resolve_execution_target(tree, selected_fields=["Amount"], required_keys=[])
    grain = resolve_reconciliation_grain(
        target,
        source_fields=[{"name": "Company"}, {"name": "Amount"}],
    )
    assert grain["keys"] == ["BusinessUnit"]
    assert grain["field_mappings"] == [{"target_field": "BusinessUnit", "source_field": "Company"}]


def test_llm_sql_is_bound_to_verified_fact_runtime_object_and_rejects_other_sources():
    target = resolve_execution_target(
        tree("FV_RUNTIME", [
            {"name": "Region", "is_key": True},
            {"name": "Amount", "aggregation": "SUM"},
        ]),
        selected_fields=["Amount"],
        required_keys=[],
    )
    sql = 'SELECT "Region", SUM("Amount") FROM "FV_0" GROUP BY "Region"'
    bound = bind_readonly_sql_to_target(sql, "AM_Customer_Receivables_KPI", target)
    assert 'FROM "RADH_S3X"."FV_RUNTIME"' in bound

    try:
        bind_readonly_sql_to_target('SELECT "Region" FROM "LOCAL_SOURCE"', "AM_X", target)
    except ExecutionTargetError as exc:
        assert "unverified source" in str(exc)
    else:
        raise AssertionError("unverified upstream dependency was accepted")
