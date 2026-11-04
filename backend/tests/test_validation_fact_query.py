import asyncio

import validation_service as service
from reconciliation.execution_target import ExecutionTarget
from schemas import ValidationRequest


def _target():
    return ExecutionTarget(
        analytical_model="AM_Sales",
        analytical_model_space="SPACE_A",
        fact_model="FV_Sales",
        fact_model_space="SPACE_A",
        relationship="analytic_model_source",
        object_type="view",
        object_name="FV_Sales_RT",
        schema="RUNTIME_SCHEMA",
        columns=(
            {"name": "Region", "data_type": "NVARCHAR"},
            {"name": "NetAmount", "data_type": "DECIMAL", "aggregation": "SUM"},
        ),
        lineage=("AM_Sales", "FV_Sales", "GV_Sales"),
    )


def test_aggregate_comparison_rolls_up_only_metadata_supported_measures():
    comparisons = service._aggregate_measure_comparisons(
        selected_fields=["Region", "Revenue", "Orders", "Minimum", "Maximum"],
        target_columns=[
            {"name": "Region"},
            {"name": "Revenue", "aggregation": "SUM", "is_measure": True},
            {"name": "Orders", "aggregation": "COUNT", "is_measure": True},
            {"name": "Minimum", "aggregation": "MIN", "is_measure": True},
            {"name": "Maximum", "aggregation": "MAX", "is_measure": True},
        ],
        transformation_ir={"aggregations": []},
        datasphere_rows=[
            {"Region": "N", "Revenue": "10", "Orders": "2", "Minimum": "1", "Maximum": "7"},
            {"Region": "S", "Revenue": "20", "Orders": "3", "Minimum": "4", "Maximum": "9"},
        ],
        s4hana_rows=[
            {"Region": "N", "Revenue": "10", "Orders": "2", "Minimum": "1", "Maximum": "7"},
            {"Region": "S", "Revenue": "21", "Orders": "3", "Minimum": "4", "Maximum": "9"},
        ],
        datasphere_status="SUCCEEDED_WITH_ROWS",
        s4hana_status="SUCCEEDED_WITH_ROWS",
        semantic_status="VERIFIED",
    )

    by_name = {item["metric_name"]: item for item in comparisons}
    assert by_name["Revenue"]["datasphere_total"] == 30
    assert by_name["Revenue"]["s4hana_total"] == 31
    assert by_name["Revenue"]["delta"] == 1
    assert by_name["Revenue"]["status"] == "MISMATCH"
    assert by_name["Orders"]["datasphere_total"] == 5
    assert by_name["Minimum"]["datasphere_total"] == 1
    assert by_name["Maximum"]["s4hana_total"] == 9


def test_aggregate_comparison_does_not_invent_values_for_missing_failed_empty_or_truncated_data():
    fields = [{"name": "Amount", "aggregation": "SUM", "is_measure": True}]

    def compare(ds_status, s4_status, ds_rows, s4_rows, semantic_status="VERIFIED"):
        return service._aggregate_measure_comparisons(
            selected_fields=["Amount"],
            target_columns=fields,
            transformation_ir={"aggregations": []},
            datasphere_rows=ds_rows,
            s4hana_rows=s4_rows,
            datasphere_status=ds_status,
            s4hana_status=s4_status,
            semantic_status=semantic_status,
        )[0]

    failed = compare("SUCCEEDED_WITH_ROWS", "EXECUTION_FAILED", [{"Amount": 0}], [])
    assert failed["status"] == "NOT_COMPUTED"
    assert failed["delta"] is None
    assert failed["datasphere_total"] is None

    empty = compare("SUCCEEDED_ZERO_ROWS", "SUCCEEDED_ZERO_ROWS", [], [])
    assert empty["status"] == "INDETERMINATE"
    assert empty["delta"] is None

    truncated = compare(
        "SUCCEEDED_WITH_ROWS",
        "SUCCEEDED_TRUNCATED",
        [{"Amount": 5}],
        [{"Amount": 5}],
    )
    assert truncated["status"] == "INDETERMINATE"
    assert truncated["completeness"] == "TRUNCATED"
    assert truncated["delta"] is None

    unverified = compare(
        "SUCCEEDED_WITH_ROWS",
        "SUCCEEDED_WITH_ROWS",
        [{"Amount": 5}],
        [{"Amount": 5}],
        semantic_status="UNVERIFIED",
    )
    assert unverified["status"] == "INDETERMINATE"
    assert unverified["delta"] is None


def test_aggregate_comparison_keeps_grouped_averages_indeterminate_without_weights():
    result = service._aggregate_measure_comparisons(
        selected_fields=["Region", "Average"],
        target_columns=[
            {"name": "Region"},
            {"name": "Average", "aggregation": "AVG", "is_measure": True},
        ],
        transformation_ir={"aggregations": []},
        datasphere_rows=[
            {"Region": "N", "Average": "5"},
            {"Region": "S", "Average": "9"},
        ],
        s4hana_rows=[
            {"Region": "N", "Average": "5"},
            {"Region": "S", "Average": "9"},
        ],
        datasphere_status="SUCCEEDED_WITH_ROWS",
        s4hana_status="SUCCEEDED_WITH_ROWS",
        semantic_status="VERIFIED",
    )[0]

    assert result["status"] == "INDETERMINATE"
    assert result["datasphere_total"] is None
    assert "without verified counts" in result["reason"]


def test_grouped_measure_totals_do_not_claim_group_level_match():
    result = service._aggregate_measure_comparisons(
        selected_fields=["Region", "Revenue"],
        target_columns=[
            {"name": "Region"},
            {"name": "Revenue", "aggregation": "SUM", "is_measure": True},
        ],
        transformation_ir={"aggregations": []},
        datasphere_rows=[
            {"Region": "N", "Revenue": "10"},
            {"Region": "S", "Revenue": "20"},
        ],
        s4hana_rows=[
            {"Region": "N", "Revenue": "20"},
            {"Region": "S", "Revenue": "10"},
        ],
        datasphere_status="SUCCEEDED_WITH_ROWS",
        s4hana_status="SUCCEEDED_WITH_ROWS",
        semantic_status="VERIFIED",
    )[0]

    assert result["status"] == "MATCH"
    assert result["delta"] == 0
    assert result["group_status"] == "MISMATCH"


def test_aggregate_comparison_uses_ir_aggregation_and_reports_missing_semantics():
    from_ir = service._aggregate_measure_comparisons(
        selected_fields=["VerifiedCount"],
        target_columns=[{"name": "VerifiedCount", "is_measure": True}],
        transformation_ir={"aggregations": [{
            "name": "VerifiedCount",
            "aggregation": "COUNT",
        }]},
        datasphere_rows=[{"VerifiedCount": "4"}],
        s4hana_rows=[{"VerifiedCount": "4"}],
        datasphere_status="SUCCEEDED_WITH_ROWS",
        s4hana_status="SUCCEEDED_WITH_ROWS",
        semantic_status="VERIFIED",
    )[0]
    missing = service._aggregate_measure_comparisons(
        selected_fields=["UnknownMeasure"],
        target_columns=[{"name": "UnknownMeasure", "is_measure": True}],
        transformation_ir={"aggregations": []},
        datasphere_rows=[{"UnknownMeasure": "4"}],
        s4hana_rows=[{"UnknownMeasure": "4"}],
        datasphere_status="SUCCEEDED_WITH_ROWS",
        s4hana_status="SUCCEEDED_WITH_ROWS",
        semantic_status="VERIFIED",
    )[0]

    assert from_ir["aggregation"] == "COUNT"
    assert from_ir["datasphere_total"] == 4
    assert missing["status"] == "INDETERMINATE"
    assert missing["aggregation"] is None
    assert missing["delta"] is None


def test_approved_fact_query_returns_independent_rows_and_dynamic_measure_comparison(monkeypatch):
    rows = [
        {"Region": "N", "NetAmount": "10"},
        {"Region": "S", "NetAmount": "20"},
    ]
    target = ExecutionTarget(
        analytical_model="AM_IntegrationMeasure",
        analytical_model_space="SPACE_A",
        fact_model="FV_IntegrationMeasure",
        fact_model_space="SPACE_A",
        relationship="analytic_model_source",
        object_type="view",
        object_name="FV_IntegrationMeasure_RT",
        schema="RUNTIME_SCHEMA",
        columns=(
            {"name": "Region", "source_field": "Region", "data_type": "NVARCHAR"},
            {
                "name": "NetAmount", "source_field": "Amount",
                "data_type": "DECIMAL", "aggregation": "SUM", "is_measure": True,
            },
        ),
        lineage=("AM_IntegrationMeasure", "FV_IntegrationMeasure"),
    )
    transformation_ir = {
        "projections": [
            {"output_field": "Region", "source_field": "Region"},
            {"output_field": "NetAmount", "source_field": "Amount"},
        ],
        "filters": [],
        "joins": [],
        "calculations": [],
        "formulas": [],
        "aggregations": [{
            "owner": "FV_IntegrationMeasure",
            "name": "NetAmount",
            "aggregation": "SUM",
        }],
        "parameters": [],
        "constants": [],
        "groupings": [{"expression": "Region"}],
        "null_handling": [],
        "distincts": [],
        "unsupported_constructs": [],
    }

    async def generate_fact_query(*args, **kwargs):
        return {
            "sql_query": 'SELECT "Region", SUM("NetAmount") AS "NetAmount" FROM "FV_IntegrationMeasure" GROUP BY "Region"',
            "explanation": "Aggregate the verified model measure by region.",
        }

    async def generate_s4_query(**kwargs):
        return {
            "s4hana_sql": (
                "SELECT ZC_IntegrationMeasure~Region AS Region, "
                "SUM(ZC_IntegrationMeasure~Amount) AS NetAmount "
                "FROM ZC_IntegrationMeasure GROUP BY ZC_IntegrationMeasure~Region"
            ),
            "datasphere_sql": "",
            "output_fields": ["Region", "NetAmount"],
            "key_fields": [],
            "assumptions": [],
        }

    def execute_datasphere(sql, space, prompt):
        return rows, 0.01, sql

    def execute_adt(queries, row_limit):
        return {
            "latency_seconds": 0.02,
            "sources": [{
                "source": "ZC_IntegrationMeasure",
                "status": "SUCCEEDED_WITH_ROWS",
                "sql": queries[0]["sql"],
                "parameters": {},
                "columns": [
                    {"name": "Region", "type": "NVARCHAR"},
                    {"name": "NetAmount", "type": "DECIMAL"},
                ],
                "rows": rows,
                "row_count": len(rows),
                "total_rows": len(rows),
                "execution_time": 0.01,
                "latency_seconds": 0.02,
                "error": None,
            }],
        }

    monkeypatch.setattr(service, "generate_odata_query_blueprint", generate_fact_query)
    monkeypatch.setattr(service, "generate_reconciliation_query_blueprint", generate_s4_query)
    monkeypatch.setattr(service, "execute_direct_hana_query", execute_datasphere)
    monkeypatch.setattr(service, "_execute_adt_source_queries", execute_adt)
    monkeypatch.setattr(service, "USE_SAP_ADT", True)
    monkeypatch.setattr(service, "DataRetriever", object())
    source_metadata = [{
        "entity": "ZC_IntegrationMeasure",
        "fields": [
            {"name": "Region", "type": "CHAR"},
            {"name": "Amount", "type": "CURR"},
        ],
        "key_fields": [],
        "parameters": [],
    }]
    request = ValidationRequest(
        view_name="AM_IntegrationMeasure",
        user_prompt="compare net amount by region",
    )
    plan = asyncio.run(service._fact_query_only_response(
        request=request,
        view_name=request.view_name,
        space_id="SPACE_A",
        source_cds_view="ZC_IntegrationMeasure",
        user_prompt=request.user_prompt,
        target_fields=["Region", "NetAmount"],
        key_fields=[],
        architecture_metadata={"transformation_ir": transformation_ir},
        reconstruction={},
        target=target,
        reason="Row-level grain is unresolved.",
        source_metadata=source_metadata,
        discovered_sources=["ZC_IntegrationMeasure"],
    ))
    assert plan.s4hana["query_status"] == "PENDING_APPROVAL"
    assert plan.s4hana["record_count"] is None
    approved = ValidationRequest(
        view_name=request.view_name,
        user_prompt=request.user_prompt,
        human_approved=True,
        approved_query_hash=plan.execution_plan["query_hash"],
    )
    result = asyncio.run(service._fact_query_only_response(
        request=approved,
        view_name=request.view_name,
        space_id="SPACE_A",
        source_cds_view="ZC_IntegrationMeasure",
        user_prompt=request.user_prompt,
        target_fields=["Region", "NetAmount"],
        key_fields=[],
        architecture_metadata={"transformation_ir": transformation_ir},
        reconstruction={},
        target=target,
        reason="Row-level grain is unresolved.",
        source_metadata=source_metadata,
        discovered_sources=["ZC_IntegrationMeasure"],
    ))

    assert result.status == "INDETERMINATE"
    assert result.datasphere["data"] == rows
    assert result.s4hana["data"] == rows
    assert result.datasphere["columns"][1]["type"] == "DECIMAL"
    assert result.s4hana["columns"][1]["type"] == "DECIMAL"
    comparison = result.metrics_comparison[0]
    assert comparison["metric_name"] == "NetAmount"
    assert comparison["aggregation"] == "SUM"
    assert comparison["datasphere_total"] == 30
    assert comparison["s4hana_total"] == 30
    assert comparison["delta"] == 0
    assert comparison["status"] == "MATCH"
    assert "No reconciliation match was attempted" in result.comparison["summary_text"]


def test_hashlib_is_not_shadowed_in_metadata_validation_scope():
    code = service.run_metadata_driven_validation.__code__

    assert "hashlib" not in code.co_varnames
    assert "_hashlib" not in code.co_varnames
    assert service._hashlib.sha256(b"approval").hexdigest()


def test_unresolved_grain_returns_approved_fact_query_and_never_claims_match(monkeypatch):
    calls = []

    async def generate(*args, **kwargs):
        return {
            "sql_query": 'SELECT "Region", SUM("NetAmount") AS "NetAmount" FROM "FV_Sales" GROUP BY "Region"',
            "explanation": "Sum net amount by region.",
        }

    def execute(sql, space, prompt):
        calls.append((sql, space, prompt))
        return [], 0.02, sql

    monkeypatch.setattr(service, "generate_odata_query_blueprint", generate)
    monkeypatch.setattr(service, "execute_direct_hana_query", execute)

    request = ValidationRequest(view_name="AM_Sales", user_prompt="sum net amount by region")
    initial = asyncio.run(service._fact_query_only_response(
        request=request,
        view_name="AM_Sales",
        space_id="SPACE_A",
        source_cds_view="ZC_SALES",
        user_prompt=request.user_prompt,
        target_fields=["Region", "NetAmount"],
        key_fields=[],
        architecture_metadata={},
        reconstruction={},
        target=_target(),
        reason="No shared verified grain.",
    ))

    assert initial.status == "NOT_RUN"
    assert initial.approval_required is True
    assert initial.execution_plan["mode"] == "FACT_QUERY_ONLY"
    assert 'FROM "RUNTIME_SCHEMA"."FV_Sales_RT"' in initial.datasphere["sql"]
    assert initial.s4hana["sql"] == ""
    assert initial.comparison["delta"] is None
    assert initial.metrics_comparison[0]["status"] == "NOT_COMPUTED"

    approved_request = ValidationRequest(
        view_name="AM_Sales",
        user_prompt="sum net amount by region",
        human_approved=True,
        approved_query_hash=initial.execution_plan["query_hash"],
    )
    result = asyncio.run(service._fact_query_only_response(
        request=approved_request,
        view_name="AM_Sales",
        space_id="SPACE_A",
        source_cds_view="ZC_SALES",
        user_prompt=approved_request.user_prompt,
        target_fields=["Region", "NetAmount"],
        key_fields=[],
        architecture_metadata={},
        reconstruction={},
        target=_target(),
        reason="No shared verified grain.",
    ))

    assert calls and calls[0][0] == initial.datasphere["sql"]
    assert result.status == "INDETERMINATE"
    assert result.comparison["is_match"] is False
    assert result.datasphere["record_count"] == 0
    assert result.datasphere["execution_engine"] == "SAP HANA Cloud Direct SQL"
    assert result.datasphere["error"] is None
    assert "no reconciliation match was attempted" in result.comparison["summary_text"].lower()
    assert result.metrics_comparison[0]["status"] == "NOT_COMPUTED"


def test_unresolved_grain_does_not_execute_raw_source_without_verified_reconstruction(monkeypatch):
    async def generate(*args, **kwargs):
        return {
            "sql_query": 'SELECT "Region", SUM("NetAmount") AS "NetAmount" FROM "FV_Sales" GROUP BY "Region"',
            "explanation": "Sum net amount by region.",
        }

    def execute_ds(sql, space, prompt):
        return [{"Region": "NORTH", "NetAmount": 4}], 0.01, sql

    def execute_s4(queries, row_limit):
        assert queries[0]["source"] == "ZC_Sales"
        assert "FROM ZC_Sales" in queries[0]["sql"]
        return {
            "latency_seconds": 0.03,
            "sources": [{
                "source": "ZC_Sales",
                "status": "SUCCEEDED_WITH_ROWS",
                "sql": queries[0]["sql"],
                "parameters": {},
                "columns": [{"name": "CompanyCode", "type": "CHAR"}],
                "rows": [{"CompanyCode": "1000"}],
                "row_count": 1,
                "total_rows": 1,
                "execution_time": 0.02,
                "latency_seconds": 0.03,
                "error": None,
            }],
        }

    monkeypatch.setattr(service, "generate_odata_query_blueprint", generate)
    monkeypatch.setattr(service, "execute_direct_hana_query", execute_ds)
    monkeypatch.setattr(service, "_execute_adt_source_queries", execute_s4)

    source_metadata = [{
        "entity": "ZC_Sales",
        "fields": [{"name": "CompanyCode", "type": "CHAR", "is_key": True}],
        "key_fields": ["CompanyCode"],
        "parameters": [],
    }]
    request = ValidationRequest(
        view_name="AM_Sales",
        user_prompt="sum net amount by region",
        cds_parameters={},
    )
    plan = asyncio.run(service._fact_query_only_response(
        request=request,
        view_name="AM_Sales",
        space_id="SPACE_A",
        source_cds_view="ZC_Sales",
        user_prompt=request.user_prompt,
        target_fields=["Region", "NetAmount"],
        key_fields=[],
        architecture_metadata={"reconciliation_grain": {"status": "unresolved"}},
        reconstruction={},
        target=_target(),
        reason="No shared verified grain.",
        source_metadata=source_metadata,
        discovered_sources=["ZC_Sales"],
    ))
    assert plan.execution_plan["mode"] == "FACT_QUERY_ONLY"
    assert plan.s4hana["query_status"] == "BLOCKED"
    assert plan.execution_plan["s4hana_queries"] == []
    assert plan.reconstruction["semantic_equivalence_status"] == "SEMANTIC_EQUIVALENCE_UNVERIFIED"
    assert plan.reconstruction["transformation_coverage"]["overall_status"] == "SEMANTIC_EQUIVALENCE_UNVERIFIED"

    approved = ValidationRequest(
        view_name="AM_Sales",
        user_prompt=request.user_prompt,
        human_approved=True,
        approved_query_hash=plan.execution_plan["query_hash"],
    )
    result = asyncio.run(service._fact_query_only_response(
        request=approved,
        view_name="AM_Sales",
        space_id="SPACE_A",
        source_cds_view="ZC_Sales",
        user_prompt=approved.user_prompt,
        target_fields=["Region", "NetAmount"],
        key_fields=[],
        architecture_metadata={"reconciliation_grain": {"status": "unresolved"}},
        reconstruction={},
        target=_target(),
        reason="No shared verified grain.",
        source_metadata=source_metadata,
        discovered_sources=["ZC_Sales"],
    ))

    assert result.status == "INDETERMINATE"
    assert result.datasphere["query_status"] == "SUCCEEDED_WITH_ROWS"
    assert result.s4hana["query_status"] == "BLOCKED"
    assert result.s4hana["record_count"] is None
    assert result.s4hana["queries"] == []
    assert result.comparison["is_match"] is False
    assert "No reconciliation match was attempted" in result.comparison["summary_text"]


def test_unverified_reconstruction_sql_is_visible_as_non_executable_draft(monkeypatch):
    async def generate_fact_query(*args, **kwargs):
        return {
            "sql_query": 'SELECT "Region", SUM("NetAmount") AS "NetAmount" FROM "FV_Sales" GROUP BY "Region"',
            "explanation": "Aggregate sales by region.",
        }

    async def generate_s4_query(**kwargs):
        return {
            "s4hana_sql": candidate_sql,
            "datasphere_sql": "",
            "output_fields": ["Region", "NetAmount"],
            "key_fields": [],
            "assumptions": [],
        }

    candidate_sql = (
        "SELECT ZC_Sales~CompanyCode AS Region, SUM(ZC_Sales~Amount) AS NetAmount "
        "FROM ZC_Sales GROUP BY ZC_Sales~CompanyCode"
    )
    transformation_ir = {
        "projections": [
            {"output_field": "Region", "source_field": "CompanyCode"},
            {"output_field": "NetAmount", "source_field": "Amount"},
        ],
        "filters": [{
            "owner": "FV_Sales",
            "path": "FV_Sales.query.SELECT.where",
            "condition": "CompanyCode = '1000'",
            "metadata_status": "RETRIEVED",
            "metadata_source": "Datasphere CLI definition",
        }],
        "joins": [],
        "calculations": [],
        "formulas": [],
        "aggregations": [],
        "parameters": [],
        "constants": [],
        "groupings": [],
        "null_handling": [],
        "distincts": [],
        "unsupported_constructs": [],
    }
    monkeypatch.setattr(service, "generate_odata_query_blueprint", generate_fact_query)
    monkeypatch.setattr(
        service,
        "generate_reconciliation_query_blueprint",
        generate_s4_query,
    )
    source_metadata = [{
        "entity": "ZC_Sales",
        "fields": [
            {"name": "CompanyCode", "type": "CHAR", "is_key": True},
            {"name": "Amount", "type": "CURR"},
        ],
        "key_fields": ["CompanyCode"],
        "parameters": [],
    }]
    response = asyncio.run(service._fact_query_only_response(
        request=ValidationRequest(
            view_name="AM_Sales",
            user_prompt="sum net amount by region",
        ),
        view_name="AM_Sales",
        space_id="SPACE_A",
        source_cds_view="ZC_Sales",
        user_prompt="sum net amount by region",
        target_fields=["Region", "NetAmount"],
        key_fields=[],
        architecture_metadata={
            "transformation_ir": transformation_ir,
            "reconciliation_grain": {"status": "unresolved"},
        },
        reconstruction={},
        target=_target(),
        reason="No shared verified grain.",
        source_metadata=source_metadata,
        discovered_sources=["ZC_Sales"],
    ))

    assert response.status == "NOT_RUN"
    assert response.s4hana["query_status"] == "BLOCKED"
    formatted_candidate_sql = service._format_open_sql_lines(candidate_sql)
    assert response.s4hana["sql"] == formatted_candidate_sql
    assert response.reconstruction["equivalence_candidate_s4hana_sql"] == formatted_candidate_sql
    assert response.execution_plan["s4hana_sql"] == formatted_candidate_sql
    assert response.execution_plan["s4hana_queries"] == []
    assert response.s4hana["record_count"] is None
    coverage = response.reconstruction["transformation_coverage"]
    assert coverage["overall_status"] == "SEMANTIC_EQUIVALENCE_UNVERIFIED"
    filter_coverage = next(
        item for item in coverage["items"] if item["transformation"] == "filter"
    )
    assert filter_coverage["state"] == "SUPPORTED_NOT_COMPILED"
    assert "FV_Sales.query.SELECT.where" in coverage["diagnostic"]
    assert "WHERE or HAVING" in coverage["diagnostic"]


def test_rejected_candidate_sql_remains_visible_but_non_executable(monkeypatch):
    candidate_sql = "SELECT ZC_Sales~Invented AS Region FROM ZC_Sales"

    async def generate_fact_query(*args, **kwargs):
        return {
            "sql_query": 'SELECT "Region" FROM "FV_Sales"',
            "explanation": "Read the selected model.",
        }

    async def generate_s4_query(**kwargs):
        return {
            "s4hana_sql": candidate_sql,
            "datasphere_sql": "",
            "output_fields": ["Region"],
            "key_fields": [],
            "assumptions": [],
        }

    monkeypatch.setattr(service, "generate_odata_query_blueprint", generate_fact_query)
    monkeypatch.setattr(service, "generate_reconciliation_query_blueprint", generate_s4_query)
    response = asyncio.run(service._fact_query_only_response(
        request=ValidationRequest(view_name="AM_Sales", user_prompt="show region"),
        view_name="AM_Sales",
        space_id="SPACE_A",
        source_cds_view="ZC_Sales",
        user_prompt="show region",
        target_fields=["Region"],
        key_fields=[],
        architecture_metadata={},
        reconstruction={},
        target=_target(),
        reason="Comparison grain is unresolved.",
        source_metadata=[{
            "entity": "ZC_Sales",
            "fields": [{"name": "CompanyCode"}, {"name": "Amount"}],
            "key_fields": [],
            "parameters": [],
        }],
        discovered_sources=["ZC_Sales"],
    ))

    assert response.status == "NOT_RUN"
    assert response.s4hana["query_status"] == "BLOCKED"
    assert "ZC_Sales~Invented AS Region" in response.s4hana["sql"]
    assert response.execution_plan["s4hana_queries"] == []
    assert "not present on verified CDS entity" in response.s4hana["error"]


def test_datasphere_failure_does_not_trigger_unverified_raw_adt_query(monkeypatch):
    async def generate(*args, **kwargs):
        return {"sql_query": 'SELECT "Region" FROM "FV_Sales"'}

    def fail_ds(*args, **kwargs):
        raise RuntimeError("mock HANA failure")



    monkeypatch.setattr(service, "generate_odata_query_blueprint", generate)
    monkeypatch.setattr(service, "execute_direct_hana_query", fail_ds)
    adt_calls = []
    monkeypatch.setattr(
        service,
        "_execute_adt_source_queries",
        lambda *args, **kwargs: adt_calls.append((args, kwargs)),
    )
    source = {"entity": "ZC_Sales", "fields": [{"name": "CompanyCode", "type": "CHAR"}], "key_fields": [], "parameters": []}
    request = ValidationRequest(view_name="AM_Sales", user_prompt="show sales")
    plan = asyncio.run(service._fact_query_only_response(
        request=request, view_name="AM_Sales", space_id="SPACE_A",
        source_cds_view="ZC_Sales", user_prompt=request.user_prompt,
        target_fields=["Region"], key_fields=[], architecture_metadata={},
        reconstruction={}, target=_target(), reason="Unresolved grain.",
        source_metadata=[source], discovered_sources=["ZC_Sales"],
    ))
    approved = ValidationRequest(
        view_name="AM_Sales", user_prompt=request.user_prompt,
        human_approved=True, approved_query_hash=plan.execution_plan["query_hash"],
    )
    result = asyncio.run(service._fact_query_only_response(
        request=approved, view_name="AM_Sales", space_id="SPACE_A",
        source_cds_view="ZC_Sales", user_prompt=approved.user_prompt,
        target_fields=["Region"], key_fields=[], architecture_metadata={},
        reconstruction={}, target=_target(), reason="Unresolved grain.",
        source_metadata=[source], discovered_sources=["ZC_Sales"],
    ))

    assert result.status == "INDETERMINATE"
    assert result.datasphere["query_status"] == "EXECUTION_FAILED"
    assert result.s4hana["query_status"] == "BLOCKED"
    assert result.s4hana["record_count"] is None
    assert adt_calls == []
    assert result.comparison["is_match"] is False
