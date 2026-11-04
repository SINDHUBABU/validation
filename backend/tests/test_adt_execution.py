import pytest

from adt.query_executor import QueryExecutor
from adt.response_parser import parse_data_preview
from reconciliation.auto_query import ParameterResolutionError, resolve_parameter_value
from retrieval.data_retriever import DataRetriever
import validation_service as validation
from validation_service import (
    _build_transformation_ir,
    _bind_declared_cds_parameters,
    _compile_cds_source_query,
    _format_abap_open_sql,
    _execute_adt_source_queries,
    _format_open_sql_lines,
    _project_open_sql_grouping_expressions,
    _resolve_declared_cds_parameters,
    _verify_transformation_coverage,
)


class _Response:
    def __init__(self, *, text="", status_code=200, headers=None):
        self.text = text
        self.status_code = status_code
        self.headers = headers or {}
        self.ok = 200 <= status_code < 400


class _Session:
    def __init__(self):
        self.headers = {}
        self.auth = None
        self.get_calls = []
        self.post_calls = []

    def get(self, url, **kwargs):
        self.get_calls.append((url, kwargs))
        return _Response(headers={"X-CSRF-Token": "mock-token"})

    def post(self, url, **kwargs):
        self.post_calls.append((url, kwargs))
        return _Response(text="<response />")

    def close(self):
        pass


def test_query_executor_fetches_csrf_from_the_verified_cds_entity(monkeypatch):
    session = _Session()
    monkeypatch.setattr("adt.query_executor.requests.Session", lambda: session)
    monkeypatch.setattr("adt.query_executor.get_config", lambda: {
        "base_url": "https://sap.example",
        "username": "test-user",
        "password": "test-password",
        "row_limit": 10,
    })

    executor = QueryExecutor()
    body = executor.execute(
        "SELECT CompanyCode FROM ZC_Orders",
        row_limit=5,
        csrf_entity="ZC_Orders",
    )

    assert body == "<response />"
    assert session.auth.username == "test-user"
    assert session.auth.password == "test-password"
    assert session.get_calls[0][0].endswith(
        "/sap/bc/adt/ddic/ddl/sources/zc_orders/source/main"
    )
    assert session.get_calls[0][1]["headers"]["X-CSRF-Token"] == "Fetch"
    assert session.post_calls[0][1]["headers"]["X-CSRF-Token"] == "mock-token"
    assert session.post_calls[0][1]["params"] == {"rowNumber": 5}


def test_query_executor_infers_csrf_entity_for_existing_generic_query_callers(monkeypatch):
    session = _Session()
    monkeypatch.setattr("adt.query_executor.requests.Session", lambda: session)
    monkeypatch.setattr("adt.query_executor.get_config", lambda: {
        "base_url": "https://sap.example", "username": "u", "password": "p", "row_limit": 10,
    })

    QueryExecutor().execute("SELECT Amount FROM ZI_Sales")

    assert session.get_calls[0][0].endswith("/sources/zi_sales/source/main")


def test_query_executor_reports_adt_authorization_failure_without_echoing_sql(monkeypatch):
    session = _Session()
    session.post = lambda url, **kwargs: _Response(status_code=403, text="authorization denied")
    monkeypatch.setattr("adt.query_executor.requests.Session", lambda: session)
    monkeypatch.setattr("adt.query_executor.get_config", lambda: {
        "base_url": "https://sap.example", "username": "u", "password": "p", "row_limit": 10,
    })

    with pytest.raises(RuntimeError, match="HTTP 403") as error:
        QueryExecutor().execute(
            "SELECT SensitiveField FROM ZC_Sales",
            csrf_entity="ZC_Sales",
        )
    assert "SensitiveField" not in str(error.value)


def test_query_executor_preserves_bounded_adt_error_body_for_sql_diagnosis(monkeypatch):
    session = _Session()
    session.post = lambda url, **kwargs: _Response(
        status_code=400,
        text="ABAP parser rejected expression near substring",
    )
    monkeypatch.setattr("adt.query_executor.requests.Session", lambda: session)
    monkeypatch.setattr("adt.query_executor.get_config", lambda: {
        "base_url": "https://sap.example", "username": "u", "password": "p", "row_limit": 10,
    })

    with pytest.raises(RuntimeError, match="HTTP 400") as error:
        QueryExecutor().execute(
            "SELECT Amount FROM ZC_Sales",
            csrf_entity="ZC_Sales",
        )
    assert "ABAP parser rejected expression near substring" in str(error.value)


def test_query_executor_redacts_secrets_from_adt_error_body_and_logs(monkeypatch, caplog):
    caplog.set_level("INFO")
    auth_value = "abc" + "123"
    csrf_value = "csrf" + "-secret"
    password_value = "secret" + "-pass"
    session = _Session()
    session.post = lambda url, **kwargs: _Response(
        status_code=400,
        text="Open SQL failed\n"
        + "Authorization: Bearer "
        + auth_value
        + "\nX-CSRF-Token: "
        + csrf_value
        + "\npassword="
        + password_value,
    )
    monkeypatch.setattr("adt.query_executor.requests.Session", lambda: session)
    monkeypatch.setattr("adt.query_executor.get_config", lambda: {
        "base_url": "https://sap.example", "username": "u", "password": "p", "row_limit": 10,
    })

    with pytest.raises(RuntimeError, match="HTTP 400") as error:
        QueryExecutor().execute("SELECT Amount FROM ZC_Sales", csrf_entity="ZC_Sales")

    assert "Open SQL failed" in str(error.value)
    assert auth_value not in str(error.value)
    assert csrf_value not in str(error.value)
    assert password_value not in str(error.value)
    assert "adt_stage=data_preview_request_started" in caplog.text
    assert "adt_stage=data_preview_response_failed" in caplog.text
    assert auth_value not in caplog.text
    assert csrf_value not in caplog.text
    assert password_value not in caplog.text


def test_reconciliation_sql_reports_unwrapped_source_line_limit():
    with pytest.raises(ValueError, match="255-character source-line limit"):
        validation._validate_reconciliation_sql(
            "SELECT " + ("src~Amount AS Amount, " * 15) + "FROM ZC_Sales AS src",
            expected_sources=["ZC_Sales"],
            allowed_source_fields=["Amount"],
            enforce_abap_line_limit=True,
        )


def test_group_by_expressions_are_also_projected_for_adt_open_sql():
    sql = """SELECT
    SUM( CASE WHEN ZI_SALES~Status = 'A' THEN ZI_SALES~Amount ELSE 0 END ) AS ConditionalAmount,
    CASE WHEN ZI_SALES~Status = 'A' THEN 'Y' ELSE 'N' END AS StatusGroup
FROM ZI_SALES
WHERE ZI_SALES~Status <> ''
GROUP BY
    CASE WHEN ZI_SALES~Status = 'A' THEN 'Y' ELSE 'N' END,
    substring( val = ZI_SALES~PostingDate off = 0 len = 4 ),
    ZI_SALES~PostingDate,
    'Sales KPI'"""

    projected = _project_open_sql_grouping_expressions(sql)

    assert (
        "SUM( CASE WHEN ZI_SALES~Status = 'A' "
        "THEN ZI_SALES~Amount ELSE 0 END ) AS ConditionalAmount"
    ) in projected
    assert projected.count(
        "CASE WHEN ZI_SALES~Status = 'A' THEN 'Y' ELSE 'N' END"
    ) == 2
    assert (
        "substring( val = ZI_SALES~PostingDate off = 0 len = 4 ) "
        "AS DS_GROUP_2"
    ) in projected
    assert "ZI_SALES~PostingDate AS DS_GROUP_3" in projected
    assert "'Sales KPI' AS DS_GROUP_4" in projected
    assert "GROUP BY\n    CASE WHEN ZI_SALES~Status" in projected


def test_generated_open_sql_is_wrapped_before_the_adt_line_limit():
    executor = object.__new__(QueryExecutor)
    query = (
        "SELECT "
        + ", ".join(f"src~LongFieldName{index} AS LongFieldName{index}" for index in range(20))
        + " FROM ZC_Sales AS src WHERE src~Company = 'A, B''s'"
    )
    with pytest.raises(ValueError, match="line 1"):
        executor._check_line_lengths(query)

    formatted = _format_abap_open_sql(query)
    executor._check_line_lengths(formatted)
    assert all(len(line) <= QueryExecutor.MAX_LINE_LENGTH for line in formatted.splitlines())
    assert "'A, B''s'" in formatted
    assert formatted.startswith("SELECT")
    assert "\n" in formatted
    assert "FROM ZC_Sales AS src" in formatted


def test_data_preview_parser_returns_rows_columns_and_rejects_invalid_counts():
    xml = '''<dataPreview xmlns="http://www.sap.com/adt/dataPreview">
      <totalRows>1</totalRows><queryExecutionTime>0.2</queryExecutionTime>
      <columns><metadata xmlns:dp="http://www.sap.com/adt/dataPreview" dp:name="CompanyCode" dp:type="CHAR" dp:keyAttribute="true"/><dataSet><data>1000</data></dataSet></columns>
    </dataPreview>'''
    parsed = parse_data_preview(xml)
    assert parsed["rows"] == [{"CompanyCode": "1000"}]
    assert parsed["columns"][0]["name"] == "CompanyCode"
    assert parsed["columns"][0]["is_key"] is True

    with pytest.raises(ValueError, match="invalid totalRows"):
        parse_data_preview(xml.replace("<totalRows>1</totalRows>", "<totalRows>many</totalRows>"))


def test_data_preview_parser_surfaces_endpoint_error_when_success_xml_is_missing_rows():
    response = '''<dataPreview xmlns="http://www.sap.com/adt/dataPreview">
      <messages><message severity="error">Open SQL expression is unsupported</message></messages>
    </dataPreview>'''

    with pytest.raises(ValueError, match="Open SQL expression is unsupported"):
        parse_data_preview(response)


def test_data_preview_parser_redacts_sensitive_error_text_and_omits_raw_xml():
    auth_value = "abc" + "123"
    sensitive_value = "customer" + "-value"
    response = (
        '<dataPreview xmlns="http://www.sap.com/adt/dataPreview">'
        '<messages><message severity="error">Authorization: Bearer '
        + auth_value
        + '</message></messages><columns><metadata dp:name="SensitiveValue" '
        'xmlns:dp="http://www.sap.com/adt/dataPreview"/><dataSet><data>'
        + sensitive_value
        + '</data></dataSet></columns></dataPreview>'
    )

    with pytest.raises(ValueError) as error:
        parse_data_preview(response)

    assert "did not include totalRows" in str(error.value)
    assert auth_value not in str(error.value)
    assert sensitive_value not in str(error.value)


def test_data_retriever_logs_response_parse_stage_and_row_counts(monkeypatch, caplog):
    caplog.set_level("INFO")
    xml = '''<dataPreview xmlns="http://www.sap.com/adt/dataPreview">
      <totalRows>2</totalRows><columns>
        <metadata xmlns:dp="http://www.sap.com/adt/dataPreview" dp:name="CompanyCode"/>
        <dataSet><data>1000</data></dataSet>
      </columns>
    </dataPreview>'''

    class _Executor:
        def execute(self, query, row_limit, csrf_entity):
            return xml

        def close(self):
            pass

    monkeypatch.setattr("retrieval.data_retriever.QueryExecutor", _Executor)
    retriever = DataRetriever(default_row_limit=1, max_row_limit=10)

    result = retriever.fetch_query(
        "SELECT CompanyCode FROM ZC_Orders",
        row_limit=1,
        csrf_entity="ZC_Orders",
    )

    assert result["total_rows"] == 2
    assert len(result["rows"]) == 1
    assert "adt_stage=data_preview_parsed" in caplog.text
    assert "total_rows=2" in caplog.text
    assert "returned_rows=1" in caplog.text
    assert "truncated=True" in caplog.text
    retriever.close()


def test_parameterized_query_uses_declared_defaults_or_explicit_values_only():
    metadata = {
        "entity": "ZC_Orders",
        "fields": [
            {"name": "CompanyCode", "type": "CHAR", "is_key": True},
            {"name": "Amount", "type": "CURR", "is_key": False},
        ],
        "key_fields": ["CompanyCode"],
        "parameters": [{
            "name": "P_FiscalYear",
            "type": "abap.numc(4)",
            "annotations": {"@Consumption.defaultValue": "2025"},
        }],
    }
    compiled = _compile_cds_source_query(metadata)
    assert "FROM ZC_Orders(" in compiled["sql"]
    assert "P_FiscalYear = '2025'" in compiled["sql"]
    assert "ZC_Orders~CompanyCode" in compiled["sql"]
    assert compiled["keys"] == ["CompanyCode"]

    explicit = _compile_cds_source_query(
        {**metadata, "parameters": [{"name": "P_Year", "type": "abap.numc(4)", "annotations": {}}]},
        parameter_overrides={"P_Year": 2024},
    )
    assert "P_Year = 2024" in explicit["sql"]

    with pytest.raises(ValueError, match="no explicit value"):
        _resolve_declared_cds_parameters(
            {"entity": "ZC_Orders", "parameters": [{"name": "P_Year", "type": "abap.numc(4)", "annotations": {}}]},
            {},
        )


def test_unbound_parameter_named_p_year_is_not_guessed():
    with pytest.raises(ValueError, match="no explicit value"):
        _compile_cds_source_query({
            "entity": "ZC_Orders",
            "fields": [{"name": "Amount", "type": "CURR"}],
            "parameters": [{"name": "P_YEAR", "type": "abap.numc(4)", "annotations": {}}],
        })
    with pytest.raises(ParameterResolutionError, match="explicit override"):
        resolve_parameter_value({"name": "P_YEAR", "type": "abap.numc(4)", "annotations": {}}, {})


def test_generated_open_sql_wraps_projection_and_grouping_lists_for_adt():
    sql = _format_open_sql_lines(
        "SELECT src~CompanyCode AS CompanyCode, "
        "SUM(src~NetAmount) AS NetAmount FROM ZC_Sales AS src "
        "GROUP BY src~CompanyCode"
    )
    assert sql == (
        "SELECT\n"
        "    src~CompanyCode AS CompanyCode,\n"
        "    SUM(src~NetAmount) AS NetAmount\n"
        "FROM ZC_Sales AS src GROUP BY\n"
        "    src~CompanyCode"
    )


def test_existing_open_sql_plan_is_bound_from_cds_parameter_metadata():
    sources = [{
        "entity": "ZC_Orders",
        "parameters": [{
            "name": "P_Year", "type": "abap.numc(4)",
            "annotations": {"@Consumption.defaultValue": "2025"},
        }],
    }]
    sql, resolved = _bind_declared_cds_parameters(
        "SELECT src~Amount FROM ZC_Orders AS src", sources, {}
    )
    assert "FROM ZC_Orders(" in sql
    assert "P_Year = '2025'" in sql
    assert resolved == {"ZC_Orders": {"P_Year": "2025"}}


def test_parameterized_open_sql_is_blocked_when_value_is_unbound():
    source = [{"entity": "ZC_Orders", "parameters": [{
        "name": "P_Year", "type": "abap.numc(4)", "annotations": {},
    }]}]
    with pytest.raises(ValueError, match="no explicit value"):
        _bind_declared_cds_parameters(
            "SELECT src~Amount FROM ZC_Orders AS src", source, {}
        )


def test_adt_source_queries_report_rows_zero_rows_and_failures_separately(monkeypatch):
    class FakeRetriever:
        def __init__(self, **kwargs):
            self.calls = []

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def fetch_query(self, query, row_limit, csrf_entity):
            self.calls.append((query, row_limit, csrf_entity))
            if csrf_entity == "ZC_Fail":
                raise RuntimeError("mock ADT failure")
            if csrf_entity == "ZC_Empty":
                return {"rows": [], "total_rows": 0, "columns": []}
            if csrf_entity == "ZC_Truncated":
                return {
                    "rows": [{"Amount": "1"}], "total_rows": 5,
                    "columns": [{"name": "Amount", "type": "CURR"}],
                }
            return {
                "rows": [{"Amount": "1"}], "total_rows": 1,
                "columns": [{"name": "Amount", "type": "CURR"}],
            }

    monkeypatch.setattr(validation, "DataRetriever", FakeRetriever)
    result = _execute_adt_source_queries([
        {"source": "ZC_Orders", "sql": "SELECT Amount FROM ZC_Orders", "parameters": {}},
        {"source": "ZC_Empty", "sql": "SELECT Amount FROM ZC_Empty", "parameters": {}},
        {"source": "ZC_Truncated", "sql": "SELECT Amount FROM ZC_Truncated", "parameters": {}},
        {"source": "ZC_Fail", "sql": "SELECT Amount FROM ZC_Fail", "parameters": {}},
    ], 20)
    by_source = {item["source"]: item for item in result["sources"]}
    assert by_source["ZC_Orders"]["status"] == "SUCCEEDED_WITH_ROWS"
    assert by_source["ZC_Empty"]["status"] == "SUCCEEDED_ZERO_ROWS"
    assert by_source["ZC_Truncated"]["status"] == "SUCCEEDED_TRUNCATED"
    assert by_source["ZC_Truncated"]["truncated"] is True
    assert by_source["ZC_Truncated"]["row_count"] == 1
    assert by_source["ZC_Fail"]["status"] == "EXECUTION_FAILED"
    assert by_source["ZC_Fail"]["row_count"] is None
    assert by_source["ZC_Fail"]["error"] == "mock ADT failure"
    assert by_source["ZC_Fail"]["row_count"] is None


def test_transformation_coverage_requires_filters_joins_calculations_and_aggregations():
    target_columns = [
        {"name": "Region", "source_field": "SalesRegion"},
        {"name": "TotalValue", "source_field": "SalesValue", "aggregation": "SUM"},
    ]
    sources = [
        {"name": "SalesRegion", "type": "CHAR"},
        {"name": "SalesValue", "type": "CURR"},
        {"name": "IsRelevant", "type": "CHAR"},
    ]
    sql = """SELECT src~SalesRegion AS Region,
       SUM(src~SalesValue) AS TotalValue
FROM ZC_Sales AS src
WHERE src~IsRelevant = 'X'
GROUP BY src~SalesRegion"""
    ir = {
        "projections": [],
        "filters": [{"owner": "GV_Sales", "path": "GV_Sales.where", "condition": "IsRelevant = 'X'"}],
        "joins": [],
        "calculations": [],
        "formulas": [],
        "aggregations": [{"owner": "FV_Sales", "name": "TotalValue", "aggregation": "SUM"}],
        "unsupported_constructs": [],
    }
    verified = _verify_transformation_coverage(
        ir, s4hana_sql=sql, selected_fields=["Region", "TotalValue"],
        target_columns=target_columns, source_fields=sources,
    )
    assert verified["overall_status"] == "VERIFIED"
    assert verified["filters"]["status"] == "VERIFIED"
    assert verified["aggregations"]["status"] == "VERIFIED"

    missing_filter = _verify_transformation_coverage(
        {**ir, "filters": [{
            "owner": "GV_Sales",
            "path": "GV_Sales.where",
            "condition": "IsRelevant = 'X'",
        }]},
        s4hana_sql=sql.replace("WHERE src~IsRelevant = 'X'", ""),
        selected_fields=["Region", "TotalValue"], target_columns=target_columns,
        source_fields=sources,
    )
    assert missing_filter["overall_status"] == "SEMANTIC_EQUIVALENCE_UNVERIFIED"
    assert missing_filter["filters"]["status"] == "UNVERIFIED"
    missing_filter_item = next(
        item for item in missing_filter["items"]
        if item["transformation"] == "filter"
    )
    assert missing_filter_item["state"] == "SUPPORTED_NOT_COMPILED"
    assert missing_filter_item["datasphere_path"] == "GV_Sales.where"
    assert missing_filter_item["metadata_status"] == "RETRIEVED"
    assert "IsRelevant = 'X'" in missing_filter["diagnostic"]
    assert "WHERE or HAVING" in missing_filter_item["diagnostic"]


def test_transformation_coverage_accepts_parenthesized_filters_and_quoted_measure_aliases():
    report = _verify_transformation_coverage(
        {
            "projections": [{"output_field": "TotalAmount", "source_field": "Amount"}],
            "filters": [{
                "owner": "FV_Sales",
                "path": "FV_Sales.query.SELECT.where",
                "condition": "Region = 'NORTH'",
            }],
            "joins": [],
            "calculations": [],
            "formulas": [],
            "aggregations": [{
                "owner": "FV_Sales",
                "name": "TotalAmount",
                "aggregation": "SUM",
            }],
            "parameters": [],
            "constants": [],
            "groupings": [],
            "null_handling": [],
            "distincts": [],
            "unsupported_constructs": [],
        },
        s4hana_sql=(
            'SELECT SUM(src~Amount) AS "TotalAmount" FROM ZC_Sales AS src '
            "WHERE ( src~Region = 'NORTH' )"
        ),
        selected_fields=["TotalAmount"],
        target_columns=[{"name": "TotalAmount", "source_field": "Amount", "aggregation": "SUM"}],
        source_fields=[{"name": "Amount"}, {"name": "Region"}],
    )

    assert report["overall_status"] == "VERIFIED"
    assert report["filters"]["status"] == "VERIFIED"
    assert report["aggregations"]["status"] == "VERIFIED"


def test_transformation_coverage_blocks_unsupported_constructs():
    report = _verify_transformation_coverage(
        {"projections": [], "filters": [], "joins": [], "calculations": [], "formulas": [], "aggregations": [], "unsupported_constructs": [{"construct": "union"}]},
        s4hana_sql="SELECT src~Code AS Code FROM ZC_One AS src",
        selected_fields=["Code"],
        target_columns=[{"name": "Code", "source_field": "Code"}],
        source_fields=[{"name": "Code"}],
    )
    assert report["overall_status"] == "SEMANTIC_EQUIVALENCE_UNVERIFIED"
    unsupported_item = next(
        item for item in report["items"]
        if item["transformation"] == "unsupported_construct"
    )
    assert unsupported_item["state"] == "TRANSLATION_UNSUPPORTED"
    assert unsupported_item["datasphere_expression"] == "union"


def test_transformation_coverage_distinguishes_metadata_gaps_and_unassessed_translation():
    base = {
        "projections": [],
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
    report = _verify_transformation_coverage(
        {
            **base,
            "filters": [
                {
                    "owner": "FV_Orders",
                    "path": "FV_Orders.query.SELECT.where",
                    "condition": None,
                    "metadata_status": "UNAVAILABLE",
                },
                {
                    "owner": "FV_Orders",
                    "path": "FV_Orders.query.SELECT.where.function",
                    "condition": "UNKNOWN_FUNCTION(Amount) > 0",
                    "metadata_status": "RETRIEVED",
                },
            ],
        },
        s4hana_sql="SELECT src~Amount AS Amount FROM ZC_Orders AS src",
        selected_fields=["Amount"],
        target_columns=[{"name": "Amount", "source_field": "Amount"}],
        source_fields=[{"name": "Amount"}],
    )

    filter_items = [item for item in report["items"] if item["transformation"] == "filter"]
    assert [item["state"] for item in filter_items] == [
        "METADATA_UNAVAILABLE",
        "UNASSESSED",
    ]


def test_transformation_coverage_expands_calculated_filter_and_checks_only_selected_calculations():
    filter_expression = "Customer_Receivable_G_View.KPI_Breach = 'X'"
    breach_expression = (
        "CASE WHEN ZI_CUSTOMERRECEIVABLESKPI.DaysOverdue > 40 "
        "THEN 'X' ELSE ' ' END"
    )
    report = _verify_transformation_coverage(
        {
            "projections": [],
            "filters": [{
                "owner": "FV_Customer_Receivables",
                "path": "FV_Customer_Receivables.query.SELECT.where",
                "condition": filter_expression,
                "metadata_status": "RETRIEVED",
                "metadata_source": "Datasphere CLI definition",
            }],
            "joins": [],
            "calculations": [
                {
                    "owner": "Customer_Receivable_G_View",
                    "name": "KPI_Breach",
                    "expression": breach_expression,
                    "transformation_category": "case_expression",
                },
                {
                    "owner": "FV_Customer_Receivables",
                    "name": "Posting_Year",
                    "expression": "LEFT(PostingDate, 4)",
                    "transformation_category": "function",
                },
            ],
            "formulas": [],
            "aggregations": [],
            "parameters": [],
            "constants": [],
            "groupings": [],
            "null_handling": [],
            "distincts": [],
            "unsupported_constructs": [],
        },
        s4hana_sql=(
            "SELECT CASE WHEN src~DaysOverdue > 40 THEN 'X' ELSE ' ' END AS KPI_Breach "
            "FROM ZI_CUSTOMERRECEIVABLESKPI AS src "
            "WHERE src~DaysOverdue > 40"
        ),
        selected_fields=["KPI_Breach"],
        target_columns=[{"name": "KPI_Breach", "calculation": breach_expression}],
        source_fields=[{"name": "DaysOverdue"}, {"name": "PostingDate"}],
    )

    assert report["overall_status"] == "VERIFIED"
    filter_coverage = next(
        item for item in report["items"] if item["transformation"] == "filter"
    )
    assert filter_coverage["datasphere_expression"] == filter_expression
    assert filter_coverage["s4hana_expression"] == "src~DaysOverdue > 40"
    assert report["calculations"]["status"] == "VERIFIED"
    assert all(item.get("name") != "Posting_Year" for item in report["calculations"]["items"])


def test_transformation_coverage_translates_supported_substring_and_string_constants():
    report = _verify_transformation_coverage(
        {
            "projections": [],
            "filters": [],
            "joins": [],
            "calculations": [
                {
                    "owner": "FV_Receivables",
                    "name": "Posting_Year",
                    "expression": "LEFT(Customer_Receivable_G_View.PostingDate, 4)",
                    "transformation_category": "function",
                },
                {
                    "owner": "FV_Receivables",
                    "name": "KPI_Name",
                    "expression": "Customer Receivables KPI",
                    "transformation_category": "constant",
                },
            ],
            "formulas": [],
            "aggregations": [],
            "parameters": [],
            "constants": [{
                "owner": "FV_Receivables",
                "path": "FV_Receivables.KPI_Name",
                "output_field": "KPI_Name",
                "value": "Customer Receivables KPI",
            }],
            "groupings": [],
            "null_handling": [],
            "distincts": [],
            "unsupported_constructs": [],
        },
        s4hana_sql=(
            "SELECT substring( val = src~PostingDate off = 0 len = 4 ) AS Posting_Year, "
            "'Customer Receivables KPI' AS KPI_Name "
            "FROM ZI_CUSTOMERRECEIVABLESKPI AS src"
        ),
        selected_fields=["Posting_Year", "KPI_Name"],
        target_columns=[
            {"name": "Posting_Year", "calculation": "LEFT(PostingDate, 4)"},
            {"name": "KPI_Name", "calculation": "Customer Receivables KPI"},
        ],
        source_fields=[{"name": "PostingDate"}],
    )

    assert report["overall_status"] == "VERIFIED"
    assert report["calculations"]["status"] == "VERIFIED"
    assert report["constants"]["items"][0]["s4hana_expression"] == "'Customer Receivables KPI'"


def test_transformation_coverage_expands_datasphere_group_by_references():
    report = _verify_transformation_coverage(
        {
            "projections": [],
            "filters": [],
            "joins": [],
            "calculations": [
                {
                    "name": "Posting_Year",
                    "expression": "LEFT(PostingDate, 4)",
                    "transformation_category": "function",
                    "referenced_columns": ["PostingDate"],
                },
                {
                    "name": "Posting_Month",
                    "expression": "SUBSTRING(PostingDate, 5, 2)",
                    "transformation_category": "function",
                    "referenced_columns": ["PostingDate"],
                },
                {
                    "name": "KPI_Breach",
                    "expression": "CASE WHEN DaysOverdue > 40 THEN 'X' ELSE ' ' END",
                    "transformation_category": "case_expression",
                    "referenced_columns": ["DaysOverdue"],
                },
                {
                    "name": "KPI__Name",
                    "expression": "Customer Receivables KPI",
                    "transformation_category": "constant",
                },
            ],
            "formulas": [],
            "aggregations": [],
            "parameters": [],
            "constants": [],
            "groupings": [{
                "owner": "FV_Receivables",
                "path": "FV_Receivables.query.SELECT.groupBy",
                "expression": [
                    {"ref": ["Posting_Year"]},
                    {"ref": ["Posting_Month"]},
                    {"ref": ["PostingDate"]},
                    {"ref": ["KPI_Breach"]},
                    {"ref": ["KPI__Name"]},
                ],
                "metadata_status": "RETRIEVED",
                "metadata_source": "Datasphere CLI definition",
            }],
            "null_handling": [],
            "distincts": [],
            "unsupported_constructs": [],
        },
        s4hana_sql=(
            "SELECT CASE WHEN src~DaysOverdue > 40 THEN 'X' ELSE ' ' END AS KPI_Breach "
            "FROM ZI_RECEIVABLES AS src GROUP BY "
            "substring( val = src~PostingDate off = 0 len = 4 ), "
            "substring( val = src~PostingDate off = 4 len = 2 ), "
            "src~PostingDate, CASE WHEN src~DaysOverdue > 40 THEN 'X' ELSE ' ' END, "
            "'Customer Receivables KPI'"
        ),
        selected_fields=["KPI_Breach"],
        target_columns=[{
            "name": "KPI_Breach",
            "calculation": "CASE WHEN DaysOverdue > 40 THEN 'X' ELSE ' ' END",
        }],
        source_fields=[{"name": "PostingDate"}, {"name": "DaysOverdue"}],
    )

    assert report["groupings"]["status"] == "VERIFIED"
    assert len(report["groupings"]["items"]) == 5
    grouping_coverage = [
        item for item in report["items"] if item["transformation"] == "grouping"
    ]
    assert all(item["state"] == "VERIFIED" for item in grouping_coverage)
    posting_year = next(
        item for item in grouping_coverage
        if item["datasphere_expression"] == "Posting_Year"
    )
    assert posting_year["source_fields"] == ["PostingDate"]


def test_transformation_coverage_reports_grouping_constants_and_null_handling():
    sql = """SELECT src~Region AS Region,
       COALESCE(SUM(src~Amount), 0) AS TotalAmount,
       'ACTIVE' AS Status
FROM ZC_Sales AS src
GROUP BY src~Region"""
    report = _verify_transformation_coverage(
        {
            "projections": [],
            "filters": [],
            "joins": [],
            "calculations": [{
                "owner": "FV_Sales",
                "path": "FV_Sales.Status",
                "name": "Status",
                "expression": "'ACTIVE'",
                "transformation_category": "constant",
            }],
            "formulas": [],
            "aggregations": [{
                "owner": "FV_Sales",
                "path": "FV_Sales.TotalAmount",
                "name": "TotalAmount",
                "aggregation": "SUM",
            }],
            "parameters": [],
            "constants": [{
                "owner": "FV_Sales",
                "path": "FV_Sales.Status",
                "output_field": "Status",
                "value": "'ACTIVE'",
            }],
            "groupings": [],
            "null_handling": [{
                "owner": "FV_Sales",
                "path": "FV_Sales.TotalAmount",
                "expression": "COALESCE(SUM(Amount), 0)",
            }],
            "unsupported_constructs": [],
        },
        s4hana_sql=sql,
        selected_fields=["Region", "TotalAmount", "Status"],
        target_columns=[
            {"name": "Region", "source_field": "Region"},
            {"name": "TotalAmount", "source_field": "Amount", "aggregation": "SUM"},
            {"name": "Status", "calculation": "'ACTIVE'"},
        ],
        source_fields=[{"name": "Region"}, {"name": "Amount"}],
    )

    assert report["overall_status"] == "VERIFIED"
    assert report["groupings"]["items"][0]["s4hana_expression"] == "Region"
    assert report["constants"]["items"][0]["status"] == "VERIFIED"
    assert report["null_handling"]["items"][0]["status"] == "VERIFIED"
    assert all(item.get("s4hana_expression") for item in report["items"])


def test_transformation_coverage_fails_closed_when_grouping_or_null_semantics_are_missing():
    report = _verify_transformation_coverage(
        {
            "projections": [],
            "filters": [],
            "joins": [],
            "calculations": [],
            "formulas": [],
            "aggregations": [{
                "owner": "FV_Sales", "name": "TotalAmount", "aggregation": "SUM",
            }],
            "parameters": [],
            "constants": [],
            "groupings": [],
            "null_handling": [{
                "owner": "FV_Sales", "expression": "COALESCE(Amount, 0)",
            }],
            "unsupported_constructs": [],
        },
        s4hana_sql=(
            "SELECT src~Region AS Region, SUM(src~Amount) AS TotalAmount "
            "FROM ZC_Sales AS src"
        ),
        selected_fields=["Region", "TotalAmount"],
        target_columns=[
            {"name": "Region", "source_field": "Region"},
            {"name": "TotalAmount", "source_field": "Amount", "aggregation": "SUM"},
        ],
        source_fields=[{"name": "Region"}, {"name": "Amount"}],
    )

    assert report["overall_status"] == "SEMANTIC_EQUIVALENCE_UNVERIFIED"
    assert report["groupings"]["status"] == "UNVERIFIED"
    assert report["null_handling"]["status"] == "UNVERIFIED"


def test_transformation_coverage_accepts_left_join_and_equivalent_coalesce():
    sql = """SELECT a~ID AS ID, COALESCE(a~Amount, 0) AS Amount
FROM ZC_Header AS a
LEFT OUTER JOIN ZC_Item AS b ON a~ID = b~HeaderID"""
    report = _verify_transformation_coverage(
        {
            "projections": [],
            "filters": [],
            "joins": [{
                "owner": "FV_Sales",
                "path": "FV_Sales.join",
                "join_type": "left outer",
                "condition": "Header.ID = Item.HeaderID",
            }],
            "calculations": [],
            "formulas": [],
            "aggregations": [],
            "parameters": [],
            "constants": [],
            "groupings": [],
            "null_handling": [{
                "owner": "FV_Sales",
                "path": "FV_Sales.Amount",
                "expression": "IFNULL(Amount, 0)",
            }],
            "unsupported_constructs": [],
        },
        s4hana_sql=sql,
        selected_fields=["ID"],
        target_columns=[{"name": "ID", "source_field": "ID"}],
        source_fields=[{"name": "ID"}, {"name": "Amount"}],
    )

    assert report["overall_status"] == "VERIFIED"
    assert report["joins"]["items"][0]["status"] == "VERIFIED"
    assert report["null_handling"]["items"][0]["s4hana_expression"] == "COALESCE(Amount, 0)"


def test_transformation_coverage_rejects_join_types_without_verified_open_sql_support():
    report = _verify_transformation_coverage(
        {
            "projections": [],
            "filters": [],
            "joins": [{
                "owner": "FV_Sales",
                "path": "FV_Sales.join",
                "join_type": "full outer",
                "condition": "A.ID = B.ID",
            }],
            "calculations": [],
            "formulas": [],
            "aggregations": [],
            "parameters": [],
            "constants": [],
            "groupings": [],
            "null_handling": [],
            "unsupported_constructs": [],
        },
        s4hana_sql=(
            "SELECT a~ID AS ID FROM ZC_A AS a "
            "FULL OUTER JOIN ZC_B AS b ON a~ID = b~ID"
        ),
        selected_fields=["ID"],
        target_columns=[{"name": "ID", "source_field": "ID"}],
        source_fields=[{"name": "ID"}],
    )

    assert report["overall_status"] == "SEMANTIC_EQUIVALENCE_UNVERIFIED"
    assert report["joins"]["items"][0]["status"] == "UNVERIFIED"
    assert "not supported" in report["items"][1]["diagnostic"]


def test_transformation_ir_preserves_constants_grouping_distinct_and_null_filters():
    ir = _build_transformation_ir({
        "name": "FV_Sales",
        "object_type": "view",
        "definition": {
            "query": {
                "SELECT": {
                    "groupBy": [{"ref": ["Region"]}],
                    "distinct": True,
                }
            }
        },
        "field_catalog": [{"name": "Region"}],
        "calculated_columns": [{
            "name": "Status",
            "expression": "'ACTIVE'",
            "transformation_category": "constant",
        }],
        "filters": [{"path": "query.SELECT.where", "condition": "Amount IS NULL"}],
        "dependencies": [],
    })

    assert ir["constants"][0]["name"] == "Status"
    assert ir["groupings"][0]["expression"] == [{"ref": ["Region"]}]
    assert ir["distincts"][0]["value"] is True
    assert ir["null_handling"][0]["expression"] == "Amount IS NULL"
    assert ir["filters"][0]["metadata_status"] == "RETRIEVED"
    assert ir["filters"][0]["metadata_source"] == "Datasphere CLI definition"
