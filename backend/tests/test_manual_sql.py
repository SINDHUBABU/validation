import asyncio

import pytest
from fastapi import HTTPException

import validation_service as service
from reconciliation.manual_sql import validate_manual_select
from schemas import ValidationRequest


def test_manual_select_accepts_verified_fields_and_rejects_unknown_sources_or_fields():
    sql = "SELECT Amount FROM FV_Orders WHERE Region = 'West'"
    validate_manual_select(
        sql,
        allowed_sources=["FV_Orders"],
        allowed_fields=["Amount", "Region"],
    )

    with pytest.raises(ValueError, match="not verified"):
        validate_manual_select(
            "SELECT Amount FROM OtherView",
            allowed_sources=["FV_Orders"],
            allowed_fields=["Amount"],
        )
    with pytest.raises(ValueError, match="not present"):
        validate_manual_select(
            "SELECT UnknownField FROM FV_Orders",
            allowed_sources=["FV_Orders"],
            allowed_fields=["Amount"],
        )


@pytest.mark.parametrize("sql", [
    "",
    "UPDATE FV_Orders SET Amount = 0",
    "SELECT Amount FROM FV_Orders; DROP TABLE FV_Orders",
    "SELECT Amount FROM FV_Orders -- comment",
    "SELECT Amount FROM FV_Orders FOR UPDATE",
    "SELECT Amount FROM FV_Orders INTO @DATA(result)",
    "SELECT Amount FROM FV_Orders WHERE Amount = ?",
])
def test_manual_select_rejects_empty_write_and_multi_statement_sql(sql):
    with pytest.raises(ValueError):
        validate_manual_select(
            sql,
            allowed_sources=["FV_Orders"],
            allowed_fields=["Amount"],
        )


class _FakeCache:
    def get_model_metadata(self, space_id, view_name):
        return None


class _FakeResolverClient:
    def __init__(self, space):
        self.cache = _FakeCache()


class _FakeDependencyResolver:
    def __init__(self, **kwargs):
        pass

    def resolve(self, view_name):
        return {
            "name": view_name,
            "object_type": "analytic_model",
            "status": "resolved",
            "dependencies": [{
                "name": "FV_Orders",
                "object_type": "fact_model",
                "status": "resolved",
                "runtime_object": "FV_Orders_RT",
                "schema": "RUNTIME",
                "field_catalog": [{"name": "Amount"}, {"name": "Region"}],
            }],
        }


def _request(sql, *, approved=False, query_hash=None):
    return ValidationRequest(
        view_name="AM_Orders",
        space_id="SPACE",
        sql_mode="manual",
        manual_sql=sql,
        manual_sql_target="datasphere",
        human_approved=approved,
        approved_query_hash=query_hash,
    )


def test_manual_datasphere_sql_requires_exact_approval_and_never_calls_llm(monkeypatch):
    monkeypatch.setattr(service, "DatasphereResolverClient", _FakeResolverClient)
    monkeypatch.setattr(service, "DatasphereDependencyResolver", _FakeDependencyResolver)
    monkeypatch.setattr(
        service,
        "generate_reconciliation_query_blueprint",
        lambda *args, **kwargs: pytest.fail("manual SQL must not call the LLM"),
    )
    monkeypatch.setattr(
        service,
        "discover_hana_execution_object_sync",
        lambda **kwargs: None,
    )
    executed = []

    def execute(sql, space, prompt, **kwargs):
        executed.append((sql, kwargs))
        return [], 0.02, sql.strip(), [{"name": "Amount", "type": "DECIMAL"}]

    monkeypatch.setattr(service, "execute_direct_hana_query", execute)
    query = "SELECT Amount FROM \"RUNTIME\".\"FV_Orders_RT\""
    preview = asyncio.run(service.run_manual_sql_validation(_request(query)))
    assert preview.status == "NOT_RUN"
    assert preview.execution_plan["sql"] == query
    assert preview.datasphere["query_status"] == "PENDING_APPROVAL"
    assert executed == []

    completed = asyncio.run(service.run_manual_sql_validation(
        _request(query, approved=True, query_hash=preview.execution_plan["query_hash"])
    ))
    assert completed.datasphere["query_status"] == "SUCCEEDED_ZERO_ROWS"
    assert completed.datasphere["record_count"] == 0
    assert completed.datasphere["columns"] == [{"name": "Amount", "type": "DECIMAL"}]
    assert completed.comparison["delta"] is None
    assert executed[0][0] == query
    assert executed[0][1]["allow_parameter_fallback"] is False

    changed = asyncio.run(service.run_manual_sql_validation(
        _request(query + " ", approved=True, query_hash=preview.execution_plan["query_hash"])
    ))
    assert changed.status == "NOT_RUN"
    assert len(executed) == 1


def test_manual_sql_metadata_failure_is_explicit(monkeypatch):
    monkeypatch.setattr(service, "DatasphereResolverClient", _FakeResolverClient)

    class FailedResolver:
        def __init__(self, **kwargs):
            pass

        def resolve(self, view_name):
            raise RuntimeError("metadata unavailable")

    monkeypatch.setattr(service, "DatasphereDependencyResolver", FailedResolver)
    with pytest.raises(HTTPException, match="Could not resolve Datasphere metadata"):
        asyncio.run(service.run_manual_sql_validation(_request("SELECT Amount FROM FV_Orders")))


def test_manual_s4_sql_uses_adt_and_returns_actual_nonempty_rows(monkeypatch):
    class S4Resolver(_FakeDependencyResolver):
        def resolve(self, view_name):
            return {
                "name": view_name,
                "object_type": "analytic_model",
                "status": "resolved",
                "dependencies": [{
                    "name": "ORDERS_REMOTE",
                    "object_type": "remote_table",
                    "is_boundary": True,
                    "definition": {"sourceLineage": {"cdsEntity": "ZI_Orders"}},
                    "field_catalog": [{"name": "Amount"}],
                }],
            }

    class FakeMetadataClient:
        def __init__(self, *args):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def discover_metadata(self, entity):
            return {
                "entity": entity,
                "fields": [{"name": "Amount", "type": "CURR"}],
            }

    class FakeRetriever:
        calls = []
        should_fail = False

        def __init__(self, **kwargs):
            pass

        def fetch_query(self, query, row_limit, csrf_entity):
            self.calls.append((query, csrf_entity))
            if self.should_fail:
                raise RuntimeError("mock ADT HTTP 400")
            return {
                "rows": [{"Amount": "42.00"}],
                "total_rows": 1,
                "execution_time": 0.1,
                "columns": [{"name": "Amount", "type": "CURR"}],
            }

        def close(self):
            pass

    monkeypatch.setattr(service, "DatasphereResolverClient", _FakeResolverClient)
    monkeypatch.setattr(service, "DatasphereDependencyResolver", S4Resolver)
    monkeypatch.setattr(service, "USE_SAP_ADT", True)
    monkeypatch.setattr(service, "DataRetriever", FakeRetriever)
    monkeypatch.setattr(service, "CDSMetadataClient", FakeMetadataClient)
    monkeypatch.setattr(
        service,
        "get_config",
        lambda: {"base_url": "https://sap.invalid", "username": "user", "password": "secret"},
    )
    monkeypatch.setattr(
        service,
        "execute_direct_hana_query",
        lambda *args, **kwargs: pytest.fail("S/4HANA SQL must not route to HANA"),
    )

    query = "SELECT ZI_Orders~Amount FROM ZI_Orders"
    request = ValidationRequest(
        view_name="AM_Orders",
        space_id="SPACE",
        source_cds_view="ZI_Orders",
        sql_mode="manual",
        manual_sql_target="s4hana",
        manual_sql=query,
    )
    preview = asyncio.run(service.run_manual_sql_validation(request))
    assert preview.execution_plan["execution_target"] == "s4hana"

    request.human_approved = True
    request.approved_query_hash = preview.execution_plan["query_hash"]
    result = asyncio.run(service.run_manual_sql_validation(request))
    assert result.status == "INDETERMINATE"
    assert result.s4hana["query_status"] == "SUCCEEDED_WITH_ROWS"
    assert result.s4hana["data"] == [{"Amount": "42.00"}]
    assert FakeRetriever.calls == [(query, "ZI_Orders")]

    FakeRetriever.should_fail = True
    failed = asyncio.run(service.run_manual_sql_validation(request))
    assert failed.status == "ERROR"
    assert failed.s4hana["query_status"] == "EXECUTION_FAILED"
    assert failed.s4hana["record_count"] is None
