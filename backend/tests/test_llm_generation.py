import asyncio

import httpx

import llm_client


def _no_examples():
    async def empty(*args, **kwargs):
        return []

    return empty


def _planner_arguments(architecture=None):
    return {
        "user_prompt": "Show amounts by region",
        "model_name": "AM_Dynamic",
        "space_id": "SPACE_X",
        "architecture_metadata": architecture or {
            "dependency_tree": {
                "field_catalog": [
                    {"name": "Region", "source_field": "Region"},
                    {"name": "Amount", "is_measure": True, "aggregation": "SUM"},
                ],
            },
            "transformation_ir": {
                "projections": [],
                "filters": [],
                "joins": [],
                "calculations": [],
                "formulas": [],
                "aggregations": [],
                "parameters": [],
                "constants": [],
                "groupings": [],
                "null_handling": [],
                "unsupported_constructs": [],
            },
        },
        "source_metadata": [{
            "entity": "ZI_Dynamic",
            "fields": [{"name": "Region"}, {"name": "Amount"}],
        }],
        "selected_fields": ["Region", "Amount"],
        "key_fields": [],
    }


def test_reconciliation_prefers_configured_provider_and_accepts_entity_qualified_query(monkeypatch):
    monkeypatch.setattr(llm_client, "ANTHROPIC_API_KEY", "anthropic-test-key")
    monkeypatch.setattr(llm_client, "GEMINI_API_KEY", "gemini-test-key")
    monkeypatch.setattr(llm_client, "OPENAI_API_KEY", "")
    monkeypatch.setattr(llm_client.rag_service, "retrieve_golden_sql_examples", _no_examples())
    monkeypatch.setattr(llm_client.rag_service, "retrieve_glossary_terms", _no_examples())
    calls = []

    async def anthropic(prompt, key):
        calls.append(("anthropic", key))
        return {
            "datasphere_sql": 'SELECT "Region", SUM("Amount") AS "Amount" FROM "SPACE_X"."AM_Dynamic" GROUP BY "Region"',
            "s4hana_sql": (
                "SELECT ZI_Dynamic~Region AS Region, "
                "SUM(ZI_Dynamic~Amount) AS Amount "
                "FROM ZI_Dynamic GROUP BY ZI_Dynamic~Region"
            ),
            "output_fields": ["Region", "Amount"],
            "key_fields": [],
        }

    async def gemini(prompt, key):
        calls.append(("gemini", key))
        return None

    monkeypatch.setattr(llm_client, "call_claude_api", anthropic)
    monkeypatch.setattr(llm_client, "call_gemini_api", gemini)
    result = asyncio.run(llm_client.generate_reconciliation_query_blueprint(**_planner_arguments()))

    assert result["s4hana_sql"].startswith("SELECT ZI_Dynamic~Region")
    assert "FROM ZI_Dynamic" in result["s4hana_sql"]
    assert "AS src" not in result["s4hana_sql"]
    assert calls == [("anthropic", "anthropic-test-key")]


def test_invalid_llm_plan_falls_back_to_metadata_compiler(monkeypatch):
    monkeypatch.setattr(llm_client, "ANTHROPIC_API_KEY", "anthropic-test-key")
    monkeypatch.setattr(llm_client, "GEMINI_API_KEY", "")
    monkeypatch.setattr(llm_client, "OPENAI_API_KEY", "")
    monkeypatch.setattr(llm_client.rag_service, "retrieve_golden_sql_examples", _no_examples())
    monkeypatch.setattr(llm_client.rag_service, "retrieve_glossary_terms", _no_examples())

    async def invalid_plan(prompt, key):
        return {
            "datasphere_sql": "SELECT * FROM AM_Dynamic",
            "s4hana_sql": "SELECT src~Amount FROM ZI_Dynamic AS src",
            "output_fields": ["Region", "Amount"],
        }

    monkeypatch.setattr(llm_client, "call_claude_api", invalid_plan)
    result = asyncio.run(llm_client.generate_reconciliation_query_blueprint(**_planner_arguments()))

    assert result["s4hana_sql"].startswith("SELECT\n")
    assert "ZI_Dynamic~Region" in result["s4hana_sql"]
    assert "SUM( ZI_Dynamic~Amount )" in result["s4hana_sql"]
    assert result["generation_diagnostics"][0]["provider"] == "Anthropic"
    assert "AS src" not in result["s4hana_sql"]


def test_provider_failure_uses_deterministic_fallback_and_surfaces_safe_diagnostics(monkeypatch):
    monkeypatch.setattr(llm_client, "ANTHROPIC_API_KEY", "anthropic-test-key")
    monkeypatch.setattr(llm_client, "GEMINI_API_KEY", "gemini-test-key")
    monkeypatch.setattr(llm_client, "OPENAI_API_KEY", "")
    monkeypatch.setattr(llm_client.rag_service, "retrieve_golden_sql_examples", _no_examples())
    monkeypatch.setattr(llm_client.rag_service, "retrieve_glossary_terms", _no_examples())

    async def anthro_error(prompt, key):
        return {
            "_provider_failure": {
                "provider": "Anthropic",
                "model": "configured-model",
                "status_code": 401,
                "message": "Authentication rejected.",
            },
        }

    async def gemini_error(prompt, key):
        return {
            "_provider_failure": {
                "provider": "Google Gemini",
                "model": "configured-model",
                "status_code": 404,
                "message": "Model not found.",
            },
        }

    monkeypatch.setattr(llm_client, "call_claude_api", anthro_error)
    monkeypatch.setattr(llm_client, "call_gemini_api", gemini_error)
    result = asyncio.run(llm_client.generate_reconciliation_query_blueprint(**_planner_arguments()))

    assert len(result["generation_diagnostics"]) == 2
    assert [item["status_code"] for item in result["generation_diagnostics"]] == [401, 404]
    assert "ZI_Dynamic~Region" in result["s4hana_sql"]


def test_anthropic_invalid_credentials_are_not_retried(monkeypatch):
    calls = []

    class MockClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, url, *, headers, json):
            calls.append((url, json["model"]))
            return httpx.Response(401, json={"error": {"message": "Invalid API key"}})

    monkeypatch.setattr(llm_client.httpx, "AsyncClient", lambda **kwargs: MockClient())
    response = asyncio.run(llm_client.call_claude_api("{}", "never-log-this"))
    cached_response = asyncio.run(llm_client.call_claude_api("{}", "never-log-this"))

    assert len(calls) == 1
    assert calls[0][1] == llm_client.ANTHROPIC_MODEL
    assert response["_provider_failure"]["status_code"] == 401
    assert "never-log-this" not in response["_provider_failure"]["message"]
    assert cached_response["_provider_failure"]["status_code"] == 401


def test_gemini_request_uses_configured_model_and_redacts_api_key_from_url(monkeypatch):
    calls = []

    class MockClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, url, *, headers, json):
            calls.append((url, headers, json))
            return httpx.Response(404, json={"error": {"message": "Model not found"}})

    monkeypatch.setattr(llm_client.httpx, "AsyncClient", lambda **kwargs: MockClient())
    response = asyncio.run(llm_client.call_gemini_api("{}", "never-log-this"))

    assert len(calls) == 1
    url, headers, _ = calls[0]
    assert llm_client.GEMINI_MODEL in url
    assert "never-log-this" not in url
    assert headers["x-goog-api-key"] == "never-log-this"
    assert response["_provider_failure"]["status_code"] == 404
    assert "never-log-this" not in str(response["_provider_failure"])


def test_configured_provider_is_attempted_first(monkeypatch):
    monkeypatch.setattr(llm_client, "LLM_PROVIDER", "openai")
    monkeypatch.setattr(llm_client, "ANTHROPIC_API_KEY", "anthropic-test-key")
    monkeypatch.setattr(llm_client, "GEMINI_API_KEY", "gemini-test-key")
    monkeypatch.setattr(llm_client, "OPENAI_API_KEY", "openai-test-key")
    monkeypatch.setattr(llm_client.rag_service, "retrieve_golden_sql_examples", _no_examples())
    monkeypatch.setattr(llm_client.rag_service, "retrieve_glossary_terms", _no_examples())
    calls = []

    async def openai(prompt, key):
        calls.append(("openai", key))
        return {
            "datasphere_sql": 'SELECT "Region", SUM("Amount") AS "Amount" FROM "SPACE_X"."AM_Dynamic" GROUP BY "Region"',
            "s4hana_sql": (
                "SELECT ZI_Dynamic~Region AS Region, "
                "SUM(ZI_Dynamic~Amount) AS Amount "
                "FROM ZI_Dynamic GROUP BY ZI_Dynamic~Region"
            ),
            "output_fields": ["Region", "Amount"],
            "key_fields": [],
        }

    async def unexpected_provider(prompt, key):
        calls.append(("unexpected", key))
        return None

    monkeypatch.setattr(llm_client, "call_openai_api", openai)
    monkeypatch.setattr(llm_client, "call_claude_api", unexpected_provider)
    monkeypatch.setattr(llm_client, "call_gemini_api", unexpected_provider)

    result = asyncio.run(llm_client.generate_reconciliation_query_blueprint(**_planner_arguments()))

    assert result["llm_provider"] == "OpenAI"
    assert calls == [("openai", "openai-test-key")]


def test_openai_authentication_failure_is_cached_and_names_required_variable(monkeypatch):
    calls = []
    key = "unique-openai-auth-test-key"

    class MockClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, url, *, headers, json):
            calls.append((url, headers, json))
            return httpx.Response(401, json={"error": {"message": "Invalid API key"}})

    monkeypatch.setattr(llm_client.httpx, "AsyncClient", lambda **kwargs: MockClient())
    response = asyncio.run(llm_client.call_openai_api("{}", key))
    cached_response = asyncio.run(llm_client.call_openai_api("{}", key))

    assert len(calls) == 1
    assert calls[0][1]["Authorization"] == f"Bearer {key}"
    assert calls[0][2]["model"] == llm_client.OPENAI_MODEL
    assert response["_provider_failure"]["status_code"] == 401
    assert "OPENAI_API_KEY" in response["_provider_failure"]["message"]
    assert key not in str(response["_provider_failure"])
    assert cached_response["_provider_failure"]["status_code"] == 401
