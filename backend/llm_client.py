import re
import json
import hashlib
from typing import Optional, Dict, Any, List, Tuple
import httpx
from config import (
    logger, ANTHROPIC_API_KEY, ANTHROPIC_MODEL, GEMINI_API_KEY, GEMINI_MODEL,
    OPENAI_API_KEY, OPENAI_MODEL, LLM_PROVIDER, cached_gemini_working
)
import rag_service
from sql_compiler import parse_conversational_sql_or_metadata

_AUTH_FAILED_CREDENTIALS: set[tuple[str, str]] = set()

def extract_json_from_text(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    clean = text.strip()
    if clean.startswith("```"):
        clean = re.sub(r"^```[a-zA-Z]*\s*", "", clean)
        clean = re.sub(r"\s*```$", "", clean)
    try:
        return json.loads(clean)
    except json.JSONDecodeError:
        match = re.search(r"(\{.*\})", clean, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                pass
    return None


def _provider_failure(provider: str, model: str, status: Optional[int], message: str) -> Dict[str, Any]:
    detail = str(message or "The provider returned no usable response.")
    for secret in (ANTHROPIC_API_KEY, GEMINI_API_KEY, OPENAI_API_KEY):
        if secret:
            detail = detail.replace(secret, "[redacted]")
    detail = re.sub(r"(?i)(key=)[^&\s]+", r"\1[redacted]", detail)
    if status in (401, 403):
        credential_variable = {
            "Anthropic": "ANTHROPIC_API_KEY",
            "Google Gemini": "GEMINI_API_KEY",
            "OpenAI": "OPENAI_API_KEY",
        }.get(provider)
        if credential_variable:
            detail += f" Update {credential_variable}; also verify {provider}'s model access."
    elif status == 404:
        model_variable = {
            "Anthropic": "ANTHROPIC_MODEL",
            "Google Gemini": "GEMINI_MODEL",
            "OpenAI": "OPENAI_MODEL",
        }.get(provider)
        if model_variable:
            detail += f" Verify {model_variable} is available to this account."
    diagnostic = {
        "provider": provider,
        "model": model,
        "status_code": status,
        "message": detail[:300],
    }
    logger.warning(
        "LLM provider request failed: provider=%s model=%s status=%s detail=%s",
        provider, model, status if status is not None else "request_error",
        diagnostic["message"],
    )
    return {"_provider_failure": diagnostic}


def _credential_was_rejected(provider: str, api_key: str) -> bool:
    return bool(api_key) and _credential_fingerprint(provider, api_key) in _AUTH_FAILED_CREDENTIALS


def _provider_attempt_order():
    providers = [
        ("Anthropic", ANTHROPIC_API_KEY, call_claude_api, ANTHROPIC_MODEL),
        ("Google Gemini", GEMINI_API_KEY, call_gemini_api, GEMINI_MODEL),
        ("OpenAI", OPENAI_API_KEY, call_openai_api, OPENAI_MODEL),
    ]
    preferred = {
        "anthropic": "Anthropic",
        "gemini": "Google Gemini",
        "google": "Google Gemini",
        "openai": "OpenAI",
    }.get(LLM_PROVIDER)
    if preferred:
        providers.sort(key=lambda provider: provider[0] != preferred)
    return providers


def _response_error_message(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return "Provider returned a non-JSON error response."
    if not isinstance(payload, dict):
        return "Provider returned an invalid error response."
    error = payload.get("error")
    if isinstance(error, dict):
        return str(error.get("message") or error.get("type") or "Provider request rejected.")
    return str(error or payload.get("message") or "Provider request rejected.")


def _credential_fingerprint(provider: str, api_key: str) -> tuple[str, str]:
    return provider, hashlib.sha256(api_key.encode("utf-8")).hexdigest()


def _is_valid_reconciliation_plan(
    plan: Any,
    source_metadata: List[Dict[str, Any]],
    model_name: str,
    selected_fields: List[str],
    key_fields: List[str],
) -> bool:
    if not isinstance(plan, dict):
        return False
    datasphere_sql = str(plan.get("datasphere_sql") or "").strip()
    s4hana_sql = str(plan.get("s4hana_sql") or "").strip()
    for statement in (datasphere_sql, s4hana_sql):
        if (
            not re.match(r"(?is)^SELECT\b", statement)
            or ";" in statement
            or "--" in statement
            or "/*" in statement
            or re.search(r"\bSELECT\s+\*", statement, re.IGNORECASE)
        ):
            return False
    datasphere_sources = re.findall(
        r"\b(?:FROM|JOIN)\s+(?:\"[^\"]+\"\.)?\"?([A-Za-z][A-Za-z0-9_]*)\"?",
        datasphere_sql,
        re.IGNORECASE,
    )
    if (
        len(datasphere_sources) != 1
        or datasphere_sources[0].casefold() != model_name.casefold()
    ):
        return False

    entities = {
        str(source.get("entity") or "").casefold(): {
            str(field.get("name") or "").casefold()
            for field in source.get("fields", [])
            if isinstance(field, dict) and field.get("name")
        }
        for source in source_metadata
        if isinstance(source, dict) and source.get("entity")
    }
    refs = [
        match.casefold()
        for match in re.findall(
            r"\b(?:FROM|JOIN)\s+([A-Za-z][A-Za-z0-9_]*)",
            s4hana_sql,
            re.IGNORECASE,
        )
    ]
    if not refs or any(ref not in entities for ref in refs):
        return False
    if re.search(
        r"\b(?:FROM|JOIN)\s+[A-Za-z][A-Za-z0-9_]*(?:\s*\([^)]*\))?\s+AS\b",
        s4hana_sql,
        re.IGNORECASE,
    ):
        return False
    unquoted_s4_sql = re.sub(r"'(?:''|[^'])*'", "''", s4hana_sql)
    unquoted_s4_sql = re.sub(
        r"\bAS\s+[A-Za-z][A-Za-z0-9_]*\b",
        "",
        unquoted_s4_sql,
        flags=re.IGNORECASE,
    )
    for fields in entities.values():
        for field in fields:
            for match in re.finditer(
                rf"(?<![A-Za-z0-9_]){re.escape(field)}(?![A-Za-z0-9_])",
                unquoted_s4_sql,
                re.IGNORECASE,
            ):
                if not re.search(
                    r"\b[A-Za-z][A-Za-z0-9_]*~$",
                    unquoted_s4_sql[:match.start()],
                ):
                    return False
    for entity, field in re.findall(
        r"\b([A-Za-z][A-Za-z0-9_]*)~([A-Za-z][A-Za-z0-9_]*)\b",
        s4hana_sql,
    ):
        if entity.casefold() not in entities or field.casefold() not in entities[entity.casefold()]:
            return False
    required_fields = {str(item).casefold() for item in selected_fields + key_fields}
    returned_fields = {
        str(item).casefold()
        for item in plan.get("output_fields", [])
        if isinstance(item, str)
    }
    return required_fields == returned_fields


async def call_claude_api(system_prompt: str, claude_key: str) -> Optional[Dict[str, Any]]:
    """Primary LLM: Calls Anthropic Claude for query blueprint planning."""
    if not claude_key:
        return None

    model = ANTHROPIC_MODEL
    credential_fingerprint = _credential_fingerprint("Anthropic", claude_key)
    if credential_fingerprint in _AUTH_FAILED_CREDENTIALS:
        return _provider_failure(
            "Anthropic", model, 401,
            "Credentials were previously rejected; update configuration and restart the backend.",
        )
    headers = {
        "x-api-key": claude_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json"
    }

    payload = {
        "model": model,
        "max_tokens": 4096,
        "temperature": 0.0,
        "system": "You are an expert SAP Datasphere-to-S/4HANA query planner. Return only a JSON object matching the requested schema.",
        "messages": [{"role": "user", "content": system_prompt}],
    }
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers=headers,
                json=payload,
            )
    except httpx.RequestError as exc:
        return _provider_failure("Anthropic", model, None, type(exc).__name__)

    if response.status_code != 200:
        if response.status_code in (401, 403):
            _AUTH_FAILED_CREDENTIALS.add(credential_fingerprint)
        return _provider_failure(
            "Anthropic", model, response.status_code,
            _response_error_message(response),
        )
    try:
        data = response.json()
        content_blocks = data.get("content", [])
        raw_text = "".join(
            block.get("text", "")
            for block in content_blocks
            if isinstance(block, dict) and block.get("type") == "text"
        )
    except (ValueError, AttributeError, TypeError):
        return _provider_failure(
            "Anthropic", model, response.status_code,
            "Provider returned an invalid message payload.",
        )
    parsed = extract_json_from_text(raw_text)
    if not parsed:
        return _provider_failure(
            "Anthropic", model, response.status_code,
            "Provider response did not contain valid JSON.",
        )
    logger.info("LLM provider generated a plan: provider=Anthropic model=%s", model)
    return parsed

async def call_gemini_api(system_prompt: str, gem_key: str) -> Optional[Dict[str, Any]]:
    """Fallback LLM: Calls Google Gemini for query blueprint planning."""
    global cached_gemini_working
    if not gem_key:
        return None

    model = GEMINI_MODEL
    credential_fingerprint = _credential_fingerprint("Google Gemini", gem_key)
    if credential_fingerprint in _AUTH_FAILED_CREDENTIALS:
        return _provider_failure(
            "Google Gemini", model, 401,
            "Credentials were previously rejected; update configuration and restart the backend.",
        )
    payload_json = {
        "contents": [{"parts": [{"text": system_prompt}]}],
        "generationConfig": {"response_mime_type": "application/json"}
    }
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                url,
                headers={"Content-Type": "application/json", "x-goog-api-key": gem_key},
                json=payload_json,
            )
    except httpx.RequestError as exc:
        return _provider_failure("Google Gemini", model, None, type(exc).__name__)

    if response.status_code != 200:
        if response.status_code in (401, 403):
            _AUTH_FAILED_CREDENTIALS.add(credential_fingerprint)
        return _provider_failure(
            "Google Gemini", model, response.status_code,
            _response_error_message(response),
        )
    try:
        data = response.json()
        raw_text = data["candidates"][0]["content"]["parts"][0]["text"]
    except (ValueError, KeyError, IndexError, TypeError):
        return _provider_failure(
            "Google Gemini", model, response.status_code,
            "Provider returned no text candidate.",
        )
    parsed = extract_json_from_text(raw_text)
    if not parsed:
        return _provider_failure(
            "Google Gemini", model, response.status_code,
            "Provider response did not contain valid JSON.",
        )
    cached_gemini_working = True
    logger.info("LLM provider generated a plan: provider=Google Gemini model=%s", model)
    return parsed


async def call_openai_api(system_prompt: str, openai_key: str) -> Optional[Dict[str, Any]]:
    """Calls OpenAI using the configured model and caches rejected credentials."""
    if not openai_key:
        return None

    model = OPENAI_MODEL
    credential_fingerprint = _credential_fingerprint("OpenAI", openai_key)
    if credential_fingerprint in _AUTH_FAILED_CREDENTIALS:
        return _provider_failure(
            "OpenAI", model, 401,
            "Credentials were previously rejected; update configuration and restart the backend.",
        )
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {openai_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model,
                    "messages": [
                        {"role": "system", "content": "Return only JSON matching the requested query plan schema."},
                        {"role": "user", "content": system_prompt},
                    ],
                    "response_format": {"type": "json_object"},
                },
            )
    except httpx.RequestError as exc:
        return _provider_failure("OpenAI", model, None, type(exc).__name__)

    if response.status_code != 200:
        if response.status_code in (401, 403):
            _AUTH_FAILED_CREDENTIALS.add(credential_fingerprint)
        return _provider_failure(
            "OpenAI", model, response.status_code,
            _response_error_message(response),
        )
    try:
        raw_text = response.json()["choices"][0]["message"]["content"]
    except (ValueError, KeyError, IndexError, TypeError):
        return _provider_failure(
            "OpenAI", model, response.status_code,
            "Provider returned no usable completion.",
        )
    parsed = extract_json_from_text(raw_text)
    if not parsed:
        return _provider_failure(
            "OpenAI", model, response.status_code,
            "Provider response did not contain valid JSON.",
        )
    logger.info("LLM provider generated a plan: provider=OpenAI model=%s", model)
    return parsed


async def generate_odata_query_blueprint(
    user_prompt: str, 
    view_schemas: Dict[str, List[str]], 
    space_id: str, 
    foreign_keys: List[Dict[str, Any]],
    selected_fields: Optional[List[str]] = None,
    view_params: Optional[Dict[str, List[Dict[str, Any]]]] = None
) -> Dict[str, Any]:
    view_names = list(view_schemas.keys())
    schema_desc = []
    for v, cols in view_schemas.items():
        v_p = (view_params or {}).get(v, [])
        p_desc = f" (Mandatory Parameters: {', '.join([p['name'] + ' [' + p.get('type', 'VARCHAR') + ']' for p in v_p])})" if v_p else ""
        schema_desc.append(f"- View '{v}': [{', '.join(cols)}]{p_desc}")
    
    fk_desc = []
    for r in foreign_keys:
        for k in r["join_keys"]:
            fk_desc.append(f"- Relate '{r['view_a']}' and '{r['view_b']}' on '{k['col1']}' = '{k['col2']}'")

    sel_str = f"Selected Fields to Retrieve: [{', '.join(selected_fields)}]" if selected_fields else "All fields selected"

    claude_key = ANTHROPIC_API_KEY
    gem_key = GEMINI_API_KEY
    oa_key = OPENAI_API_KEY

    golden_examples = await rag_service.retrieve_golden_sql_examples(user_prompt, space_id=space_id, top_k=2, gemini_key=gem_key, openai_key=oa_key)
    glossary_terms = await rag_service.retrieve_glossary_terms(user_prompt, top_k=2, gemini_key=gem_key, openai_key=oa_key)

    few_shot_section = ""
    if golden_examples:
        few_shot_section = "\n\nFEW-SHOT VERIFIED QUERY EXAMPLES (RAG KNOWLEDGE):\n" + "\n".join([
            f"- User Request: \"{ex['natural_query']}\"\n  Target SQL: {ex['sql_query']}\n  Explanation: {ex.get('explanation', '')}"
            for ex in golden_examples
        ])

    glossary_section = ""
    if glossary_terms:
        glossary_section = "\n\nBUSINESS GLOSSARY & FORMULA DEFINITIONS (RAG KNOWLEDGE):\n" + "\n".join([
            f"- Term: {gt['term']}\n  Formula/Rule: {gt.get('formula') or gt.get('definition')}"
            for gt in glossary_terms
        ])

    # This phrasing has precise row-count semantics. Do not let a model's
    # default measure aggregation or a fuzzy fallback turn it into SUM/GROUP BY.
    if (
        len(view_schemas) == 1
        and re.search(
            r"\b(?:total\s+)?count\s+of\s+['\"]?[^'\"]+?['\"]?\s+under\s+['\"]?[A-Za-z_][A-Za-z0-9_]*",
            user_prompt,
            re.IGNORECASE,
        )
    ):
        return parse_conversational_sql_or_metadata(
            user_prompt, view_schemas, foreign_keys, selected_fields=None, view_params=view_params
        )

    system_prompt = f"""You are an expert SAP Datasphere Analytical & Relational SQL Query Planner.
Translate the user's natural language request into an exact ANSI SQL query joining the views on foreign key / relation columns.

TARGET VIEWS METADATA (Zero Transactional Data Leakage):
Space ID: {space_id}
Views:
{chr(10).join(schema_desc)}

{sel_str}

DETECTED FOREIGN KEY / JOIN KEYS:
{chr(10).join(fk_desc) if fk_desc else "No explicit FK detected, match common column names"}
{few_shot_section}
{glossary_section}

USER PROMPT: "{user_prompt}"

INSTRUCTIONS:
1. Generate valid ANSI SQL with JOINs, WHERE, GROUP BY, or ORDER BY as requested.
   - Interpret "count/total of VALUE under FIELD" as `COUNT(*)` filtered by `FIELD = VALUE`, unless the user explicitly asks to count non-null field values.
   - Keep quoted field/value references distinct from filters: the field named after "under" is the predicate column, and the explicit "where/for FIELD = VALUE" clause is an additional predicate.
2. DYNAMIC JOIN ALIGNMENT: 
   - If the user prompt groups by, filters on, or analyzes a specific dimension, you MUST join the views on that specific common dimension.
   - Do NOT join on an unmentioned or unrelated key.
3. CRITICAL FIELD PROJECTION RULE: If 'Selected Fields to Retrieve' is specified, project ALL of those selected fields.
   - When no fields are selected, project only the requested aggregate/result fields; do not add model measures or unrelated dimensions.
4. SAP HANA PARAMETERIZED VIEWS:
   - If a view has mandatory parameters (e.g. FV_os_cs with START_DATE, CLOSE_DATE), include parameter arguments in the FROM / JOIN clause:
     `FROM "FV_os_cs"('2024-01-01', '2024-12-31') t1`
   - If dates were not specified, default start dates to '1900-01-01' and close/end dates to '2099-12-31'.
5. Output valid JSON:
{{
  "sql_query": "<executable ANSI SQL query across the views>",
  "$filter": "<primary view OData filter or null>",
  "$orderby": "<primary view OData orderby or null>",
  "$top": <integer limit or null>,
  "group_by": "<dimension column or null>",
  "target_metric": "<metric column or null>",
  "explanation": "<brief summary of join and operations>"
}}"""

    # 1. Primary LLM: Anthropic Claude
    if claude_key:
        parsed = await call_claude_api(system_prompt, claude_key)
        if parsed and parsed.get("sql_query"):
            return parsed

    # 2. Fallback LLM: Google Gemini
    if gem_key:
        parsed = await call_gemini_api(system_prompt, gem_key)
        if parsed and parsed.get("sql_query"):
            return parsed

    # 3. Fallback LLM: OpenAI GPT-4o
    if oa_key:
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                payload = {
                    "model": OPENAI_MODEL,
                    "messages": [
                        {"role": "system", "content": "Return only a JSON object matching the requested query schema."},
                        {"role": "user", "content": system_prompt},
                    ],
                    "response_format": {"type": "json_object"}
                }
                res = await client.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={"Authorization": f"Bearer {oa_key}", "Content-Type": "application/json"},
                    json=payload
                )
                if res.status_code == 200:
                    data = res.json()
                    parsed = extract_json_from_text(data["choices"][0]["message"]["content"])
                    if parsed and parsed.get("sql_query"):
                        return parsed
                else:
                    logger.warning(
                        "OpenAI query planning failed: model=%s status=%s detail=%s",
                        OPENAI_MODEL, res.status_code, _response_error_message(res),
                    )
        except httpx.RequestError as e:
            logger.warning(
                "OpenAI query planning failed: model=%s request_error=%s",
                OPENAI_MODEL, type(e).__name__,
            )

    # 4. Final Fallback: Local Relational SQL Compiler
    return parse_conversational_sql_or_metadata(user_prompt, view_schemas, foreign_keys, selected_fields, view_params=view_params)


async def generate_reconciliation_query_blueprint(
    *,
    user_prompt: str,
    model_name: str,
    space_id: str,
    architecture_metadata: Dict[str, Any],
    source_metadata: List[Dict[str, Any]],
    selected_fields: List[str],
    key_fields: List[str],
    parameter_overrides: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Generate paired read-only queries from complete model and CDS metadata."""

    def compact_fields(items: Any) -> List[Dict[str, Any]]:
        if not isinstance(items, list):
            return []
        allowed = {
            "name", "data_type", "type", "element_type", "is_key", "key",
            "aggregation", "is_measure", "measure_type", "source_field",
            "sourceField", "source_column", "expression", "formula",
            "calculation", "referenced_columns", "transformation_category",
        }
        return [
            {key: value for key, value in item.items() if key in allowed and value not in (None, "", [], {})}
            for item in items if isinstance(item, dict)
        ]

    dependency_tree = architecture_metadata.get("dependency_tree", {})
    dependency_tree = dependency_tree if isinstance(dependency_tree, dict) else {}
    planning_architecture = {
        "model_name": architecture_metadata.get("model_name") or model_name,
        "space_id": architecture_metadata.get("space_id") or space_id,
        "transformation_ir": architecture_metadata.get("transformation_ir", {}),
        "reconciliation_grain": architecture_metadata.get("reconciliation_grain", {}),
        # Keep the catalogs the deterministic compiler needs; omit full CLI
        # definitions, raw source payloads, and duplicated nested artifacts.
        "dependency_tree": {
            "name": dependency_tree.get("name"),
            "object_type": dependency_tree.get("object_type"),
            "space": dependency_tree.get("space"),
            "field_catalog": compact_fields(dependency_tree.get("field_catalog")),
            "calculated_columns": compact_fields(dependency_tree.get("calculated_columns")),
        },
    }
    planning_sources = []
    for source in source_metadata:
        if not isinstance(source, dict):
            continue
        planning_sources.append({
            key: source[key]
            for key in ("entity", "sql_view_name", "key_fields", "parameterized")
            if source.get(key) not in (None, "", [], {})
        } | {
            "fields": compact_fields(source.get("fields")),
            "parameters": [
                {key: value for key, value in item.items()
                 if key in {"name", "type", "data_type", "default", "required", "system_field"}
                 and value not in (None, "", [], {})}
                for item in source.get("parameters", []) if isinstance(item, dict)
            ],
        })

    metadata_payload = json.dumps({
        "datasphere_architecture": planning_architecture,
        "s4hana_cds_sources": planning_sources,
        "explicit_cds_parameter_values": parameter_overrides or {},
    }, ensure_ascii=True, default=str)
    if len(metadata_payload) > 180_000:
        raise ValueError(
            "Resolved architecture metadata exceeds the safe planning limit "
            "(180000 characters); split the model or reduce its dependency graph."
        )

    system_prompt = f"""You are a SAP Datasphere-to-S/4HANA reconciliation query planner.
Reconstruct the Datasphere analytical model's complete transformations on the listed S/4HANA CDS sources, including every applicable join type and condition, filter, projection mapping, calculated field/formula, constants, parameter bindings, aggregation, grouping, null-handling semantics, and user-requested filter. Use only definitions present in the supplied metadata; never invent mappings or business logic.

Return a JSON object with exactly these properties:
{{
  "datasphere_sql": "read-only SAP HANA SQL selecting the requested output fields and grouping at the requested key grain from the verified underlying fact runtime object",
  "s4hana_sql": "read-only ABAP Open SQL SELECT reproducing the same model logic from the allowed S/4HANA CDS entities",
  "output_fields": ["field names returned by both queries, using identical aliases"],
  "key_fields": ["composite row identity fields returned by both queries"],
  "field_mappings": [{{"datasphere_field": "...", "cds_field": "...", "evidence": "metadata path"}}],
  "filters": [{{"expression": "...", "scope": "...", "source_path": "..."}}],
  "joins": [{{"left_source": "...", "right_source": "...", "join_type": "...", "condition": "...", "source_path": "..."}}],
  "calculated_expressions": [{{"output_field": "...", "expression": "...", "source_path": "..."}}],
  "aggregation_semantics": [{{"field": "...", "aggregation": "...", "source_path": "..."}}],
  "parameter_bindings": [{{"entity": "...", "name": "...", "value": "...", "evidence": "explicit override or CDS annotation"}}],
  "constants": [{{"output_field": "...", "value": "...", "source_path": "..."}}],
  "groupings": [{{"expression": "...", "source_path": "..."}}],
  "null_handling": [{{"datasphere_expression": "...", "s4hana_expression": "...", "source_path": "..."}}],
  "unsupported_transformations": ["..."],
  "logic_summary": "concise description of the replicated lineage and transformations",
  "assumptions": ["explicit metadata gaps or assumptions; empty only when none"]
}}

Rules:
- Analytical model metadata entry point: {architecture_metadata.get('model_name') or model_name}; Datasphere space: {space_id}.
- Query source: the verified underlying fact runtime object {model_name!r}. Use this object in the FROM clause, never the analytical model name.
- User request/filter: {user_prompt or "(none)"}.
- Requested fields: {json.dumps(selected_fields)}.
- Required composite keys: {json.dumps(key_fields)}.
- Return exactly the requested final Datasphere projection fields and required reconciliation keys, with identical technical output names on both sides. Use explicit column lists; never SELECT * or return intermediate/grouping-only columns.
- Use only technical Datasphere field names from the resolved field catalog in Datasphere SQL; never use business labels or display names.
- Apply every user-requested filter to both Datasphere SQL and S/4HANA SQL with equivalent conditions. Translate Datasphere technical fields to the metadata-mapped CDS entity fields, and preserve the same values, operators, and AND/OR grouping.
- Datasphere SQL must read only the verified underlying fact runtime object {model_name!r}, quote its schema/object identifiers, and must not query the analytical model or raw Datasphere sources.
- S/4HANA SQL must use only the supplied CDS entities, ABAP Open SQL syntax, and metadata-confirmed field names. Qualify every physical reference as ENTITY~FIELD, using the exact CDS entity after FROM/JOIN. Do not assign source aliases. Keep output aliases equal to Datasphere field names.
- Preserve left/right/full/inner join semantics and every model-level filter. Do not replace a missing source or transformation with a guess.
- Preserve constants, group-by grain, and null behavior; use only ABAP Open SQL expressions supported by the ADT Data Preview query mechanism. If a translation is unsupported or uncertain, list it under unsupported_transformations.
- For a verified Datasphere LEFT(value, length) expression, use ABAP Open SQL substring( val = value off = 0 len = length ). For SUBSTRING(value, start, length), translate only a numeric 1-based start to substring( val = value off = start - 1 len = length ), evaluating the offset before emitting SQL; otherwise report it as unsupported.
- A filter on a calculated CASE field may be reduced to the CASE WHEN predicate only when the filter equals that CASE expression's THEN value and the definition is present. Preserve null behavior; do not simplify ELSE-branch comparisons.
- The S/4HANA statement must be one read-only SELECT, with no comments, semicolon, DDL, DML, procedure calls, or dynamic SQL.
- Treat explicit parameter overrides and CDS-declared defaults/system-field annotations as the only valid parameter-value sources. Never infer values from parameter names.
- Include provenance paths for every represented filter, join, projection, calculation, and aggregation. Mark unsupported or unavailable transformations explicitly; do not claim equivalence for them.
- Return JSON only.

METADATA (definitions and schemas only; no transaction rows):
{metadata_payload}
"""

    claude_key = ANTHROPIC_API_KEY
    gem_key = GEMINI_API_KEY
    oa_key = OPENAI_API_KEY

    provider_failures: List[Dict[str, Any]] = []
    provider_models = {
        "Anthropic": ANTHROPIC_MODEL,
        "Google Gemini": GEMINI_MODEL,
        "OpenAI": OPENAI_MODEL,
    }
    for provider_name, api_key, call, model in _provider_attempt_order():
        if not api_key:
            continue
        if _credential_was_rejected(provider_name, api_key):
            provider_failures.append({
                "provider": provider_name,
                "model": model,
                "status_code": 401,
                "message": "Previously rejected credentials were skipped; update the configured API key.",
            })
            continue
        parsed = await call(system_prompt, api_key)
        if parsed and parsed.get("_provider_failure"):
            provider_failures.append(parsed["_provider_failure"])
            continue
        if _is_valid_reconciliation_plan(
            parsed, source_metadata, model_name, selected_fields, key_fields
        ):
            parsed["llm_provider"] = provider_name
            parsed["llm_model"] = provider_models[provider_name]
            return parsed
        provider_failures.append({
            "provider": provider_name,
            "message": "Provider output did not contain a valid metadata-grounded read-only reconstruction plan.",
        })
    if oa_key and not any(
        failure.get("provider") == "OpenAI" for failure in provider_failures
    ):
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    'https://api.openai.com/v1/chat/completions',
                    headers={
                        'Authorization': f'Bearer {oa_key}',
                        'Content-Type': 'application/json',
                    },
                    json={
                        'model': OPENAI_MODEL,
                        'messages': [
                            {'role': 'system', 'content': 'Return only JSON matching the requested query plan schema.'},
                            {'role': 'user', 'content': system_prompt},
                        ],
                        'response_format': {'type': 'json_object'},
                    },
                )
                if response.status_code == 200:
                    parsed = extract_json_from_text(
                        response.json()['choices'][0]['message']['content']
                    )
                    if _is_valid_reconciliation_plan(
                        parsed, source_metadata, model_name, selected_fields, key_fields
                    ):
                        parsed["llm_provider"] = "OpenAI"
                        parsed["llm_model"] = OPENAI_MODEL
                        return parsed
                else:
                    provider_failures.append(_provider_failure(
                        "OpenAI", OPENAI_MODEL, response.status_code,
                        _response_error_message(response),
                    )["_provider_failure"])
        except httpx.RequestError as exc:
            provider_failures.append(_provider_failure(
                "OpenAI", OPENAI_MODEL, None, type(exc).__name__,
            )["_provider_failure"])
    logger.info("Using deterministic metadata-driven query compiler after configured LLM attempts.")
    try:
        fallback = compile_reconciliation_blueprint_deterministic(
            model_name=model_name,
            space_id=space_id,
            architecture_metadata=architecture_metadata,
            source_metadata=source_metadata,
            selected_fields=selected_fields,
            key_fields=key_fields,
        )
    except ValueError as exc:
        provider_details = "; ".join(
            f"{item.get('provider')} ({item.get('status_code') or 'request error'}): {item.get('message')}"
            for item in provider_failures
        )
        suffix = f" LLM provider failures: {provider_details}" if provider_details else ""
        raise ValueError(f"{exc}{suffix}") from exc
    fallback["generation_diagnostics"] = provider_failures
    fallback["llm_provider"] = None
    return fallback


def _substitute_cds_fields(
    expr: str,
    cds_fields: Dict[str, str],
    entity_name: Optional[str] = None,
) -> str:
    cleaned = re.sub(r'["\w]+\.["\']?([A-Za-z0-9_]+)["\']?', r'\1', expr)
    cleaned = cleaned.replace('"', '')

    parts = re.split(r"('(?:''|[^'])*')", cleaned)
    for i in range(0, len(parts), 2):
        chunk = parts[i]
        for _, c_orig in sorted(cds_fields.items(), key=lambda x: len(x[1]), reverse=True):
            reference = f"{entity_name}~{c_orig}" if entity_name else c_orig
            chunk = re.sub(
                rf'(?<![A-Za-z0-9_~]){re.escape(c_orig)}(?![A-Za-z0-9_])',
                reference,
                chunk,
                flags=re.IGNORECASE,
            )
        parts[i] = chunk
    return "".join(parts)


def _translate_metadata_expression(
    expression: str,
    cds_fields: Dict[str, str],
    entity_name: str,
) -> str:
    translated = _substitute_cds_fields(expression, cds_fields, entity_name)
    if re.search(r"\b(?:NULLIF|IFNULL|NVL)\s*\(", translated, re.IGNORECASE):
        if re.search(r"\bNULLIF\s*\(", translated, re.IGNORECASE):
            raise ValueError("The deterministic fallback cannot verify NULLIF null semantics for ADT Open SQL.")
        translated = re.sub(r"\b(?:IFNULL|NVL)\s*\(", "COALESCE(", translated, flags=re.IGNORECASE)

    def translate_substring(match: re.Match[str]) -> str:
        function = match.group(1).casefold()
        args = [argument.strip() for argument in match.group(2).split(",")]
        if function == "left" and len(args) == 2 and args[1].isdigit():
            return f"substring( val = {args[0]} off = 0 len = {args[1]} )"
        if (
            function == "substring"
            and len(args) == 3
            and args[1].isdigit()
            and int(args[1]) >= 1
            and args[2].isdigit()
        ):
            return f"substring( val = {args[0]} off = {int(args[1]) - 1} len = {args[2]} )"
        raise ValueError(f"Unsupported or unresolved {match.group(1)} expression in deterministic fallback.")

    return re.sub(
        r"\b(LEFT|SUBSTRING)\s*\(([^()]*)\)",
        translate_substring,
        translated,
        flags=re.IGNORECASE,
    )


def _expand_calculated_field_dependencies(
    expression: str,
    calculations: Dict[str, List[str]],
    projections: Optional[Dict[str, str]] = None,
) -> str:
    """Inline only uniquely defined calculated fields and verified projection aliases."""
    identifier = re.compile(
        r"(?<![A-Za-z0-9_])"
        r"(?:(?:[A-Za-z_][A-Za-z0-9_]*[.~]))?"
        r"[\"`]?(?P<name>[A-Za-z_][A-Za-z0-9_]*)[\"`]?"
        r"(?![A-Za-z0-9_])"
    )
    projection_map = {key.casefold(): value for key, value in (projections or {}).items()}
    normalized_calculations = {
        str(key).casefold(): list(values)
        for key, values in calculations.items()
    }

    def expand(name: str, stack: Tuple[str, ...]) -> str:
        key = name.casefold()
        definitions = normalized_calculations.get(key, [])
        distinct = {_normalize_expression_preserving_literals(value) for value in definitions}
        if len(distinct) > 1:
            raise ValueError(
                f"Calculated field {name!r} has conflicting definitions across transformation layers."
            )
        if not definitions:
            return projection_map.get(key, name)
        if key in stack:
            cycle = " -> ".join(stack + (key,))
            raise ValueError(f"Calculated-field dependency cycle detected: {cycle}.")
        raw = definitions[0]
        return replace_references(raw, stack + (key,))

    def replace_references(source: str, stack: Tuple[str, ...]) -> str:
        parts = re.split(r"('(?:''|[^'])*')", source)
        for part_index in range(0, len(parts), 2):
            part = parts[part_index]

            def replace(match: re.Match[str]) -> str:
                name = match.group("name")
                if part[match.end():].lstrip().startswith("("):
                    return match.group(0)
                key = name.casefold()
                if key in normalized_calculations:
                    return f"({expand(name, stack)})"
                return projection_map.get(key, match.group(0))

            parts[part_index] = identifier.sub(replace, part)
        return "".join(parts)

    return replace_references(str(expression), ())


def _normalize_expression_preserving_literals(expression: Any) -> str:
    parts = re.split(r"('(?:''|[^'])*')", str(expression or "").strip())
    return "".join(
        part if index % 2 else re.sub(r"\s+", "", part).casefold()
        for index, part in enumerate(parts)
    )


def _render_csn_expression(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        rendered = [_render_csn_expression(item) for item in value]
        return " ".join(item for item in rendered if item)
    if not isinstance(value, dict):
        return str(value) if value is not None else ""
    reference = value.get("ref")
    if isinstance(reference, list) and reference:
        return ".".join(str(part) for part in reference)
    if "val" in value:
        literal = value["val"]
        if isinstance(literal, str):
            return "'" + literal.replace("'", "''") + "'"
        if literal is None:
            return "NULL"
        return str(literal)
    if "xpr" in value:
        return _render_csn_expression(value["xpr"])
    function = value.get("func")
    if function:
        args = value.get("args") or []
        return f"{function}({', '.join(_render_csn_expression(arg) for arg in args)})"
    return ""


def compile_reconciliation_blueprint_deterministic(
    *,
    model_name: str,
    space_id: str,
    architecture_metadata: Dict[str, Any],
    source_metadata: List[Dict[str, Any]],
    selected_fields: List[str],
    key_fields: List[str],
) -> Dict[str, Any]:
    all_output_fields = list(dict.fromkeys(key_fields + selected_fields))
    dep_tree = architecture_metadata.get("dependency_tree", {}) if isinstance(architecture_metadata, dict) else {}
    execution_target = architecture_metadata.get("execution_target", {}) if isinstance(architecture_metadata, dict) else {}
    runtime_schema = str(execution_target.get("schema") or space_id)
    runtime_object = str(execution_target.get("object_name") or model_name)
    field_catalog = dep_tree.get("field_catalog", []) if isinstance(dep_tree, dict) else []
    field_meta = {
        str(f.get("name", "")).casefold(): f
        for f in field_catalog
        if isinstance(f, dict) and f.get("name")
    }
    transformation_ir = architecture_metadata.get("transformation_ir", {})
    if not isinstance(transformation_ir, dict):
        transformation_ir = {}
    join_specs = transformation_ir.get("joins") or []
    if transformation_ir.get("unsupported_constructs"):
        constructs = ", ".join(
            str(item.get("construct") or item.get("expression") or item)
            for item in transformation_ir["unsupported_constructs"]
        )
        raise ValueError(f"Required Datasphere constructs are unsupported by the deterministic fallback: {constructs}.")

    grain_mappings = {
        str(item.get("target_field", "")).casefold(): str(item.get("source_field", ""))
        for item in (architecture_metadata.get("reconciliation_grain", {}).get("field_mappings", []) or [])
        if isinstance(item, dict) and item.get("target_field") and item.get("source_field")
    }
    calc_map = {}
    calc_definitions: Dict[str, List[str]] = {}
    for calculation in (
        list(dep_tree.get("calculated_columns", []))
        + list(transformation_ir.get("calculations", []))
        + list(transformation_ir.get("formulas", []))
    ):
        if isinstance(calculation, dict) and calculation.get("name"):
            expression = calculation.get("expression") or calculation.get("formula")
            if expression:
                calc_name = str(calculation["name"]).casefold()
                calc_map[calc_name] = str(expression)
                calc_definitions.setdefault(calc_name, []).append(str(expression))
                field_meta.setdefault(str(calculation["name"]).casefold(), calculation)
    for constant in transformation_ir.get("constants", []):
        if isinstance(constant, dict):
            output_name = constant.get("output_field") or constant.get("name")
            value = constant.get("value")
            if output_name and value is not None:
                calc_name = str(output_name).casefold()
                calc_map[calc_name] = str(value)
                calc_definitions.setdefault(calc_name, []).append(str(value))
                field_meta.setdefault(calc_name, {"name": output_name})
                field_meta[calc_name]["transformation_category"] = "constant"
    for aggregation in transformation_ir.get("aggregations", []):
        if isinstance(aggregation, dict) and aggregation.get("name"):
            field_meta.setdefault(str(aggregation["name"]).casefold(), {
                "name": aggregation["name"],
            })
            if aggregation.get("aggregation"):
                field_meta[str(aggregation["name"]).casefold()]["aggregation"] = aggregation["aggregation"]

    if not source_metadata or any(not isinstance(item, dict) for item in source_metadata):
        raise ValueError("Verified ADT source metadata is missing or malformed.")
    sources_by_entity: Dict[str, Dict[str, Any]] = {}
    source_fields: Dict[str, Dict[str, str]] = {}
    source_names: Dict[str, str] = {}
    ambiguous_source_names: set[str] = set()
    for source in source_metadata:
        entity = str(source.get("entity") or "")
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", entity):
            raise ValueError("Verified ADT metadata did not provide a valid CDS entity name.")
        if entity.casefold() in sources_by_entity:
            raise ValueError(f"ADT returned duplicate metadata for CDS entity {entity}.")
        fields = {
            str(field.get("name") or "").casefold(): str(field.get("name"))
            for field in source.get("fields", [])
            if isinstance(field, dict) and field.get("name")
        }
        if not fields:
            raise ValueError(f"Verified ADT field metadata is unavailable for CDS entity {entity}.")
        sources_by_entity[entity.casefold()] = source
        source_fields[entity.casefold()] = fields
        for label in (entity, source.get("sql_view_name")):
            if label:
                label_key = str(label).casefold()
                existing = source_names.get(label_key)
                if existing and existing.casefold() != entity.casefold():
                    ambiguous_source_names.add(label_key)
                else:
                    source_names[label_key] = entity
    cds_name = str(source_metadata[0].get("entity") or "")
    all_field_owners: Dict[str, List[str]] = {}
    for entity_key, fields in source_fields.items():
        for field_key in fields:
            all_field_owners.setdefault(field_key, []).append(entity_key)
    cds_fields = {
        field_key: source_fields[owners[0]][field_key]
        for field_key, owners in all_field_owners.items()
        if len(owners) == 1
    }

    join_clauses: List[str] = []
    if len(source_metadata) > 1 and not join_specs:
        raise ValueError("Multiple verified CDS entities were discovered, but no Datasphere join condition was supplied.")
    if join_specs and not isinstance(join_specs, list):
        raise ValueError("Datasphere join metadata has an invalid shape.")

    def join_source(side: Any, join: Dict[str, Any]) -> str:
        if isinstance(side, dict):
            source_label = side.get("source") or side.get("name")
            alias = side.get("as") or side.get("alias")
        else:
            source_label = side
            alias = None
        if not isinstance(source_label, str) or not source_label.strip():
            raise ValueError(
                f"Datasphere join at {join.get('path') or 'unknown path'} has an unresolved source."
            )
        label_key = source_label.casefold()
        if label_key in ambiguous_source_names:
            raise ValueError(f"Datasphere join source {source_label!r} is ambiguous in the ADT metadata.")
        entity = source_names.get(label_key)
        if not entity:
            raise ValueError(
                f"Datasphere join source {source_label!r} at {join.get('path') or 'unknown path'} "
                "does not map exactly to verified ADT CDS metadata."
            )
        if alias:
            alias_key = str(alias).casefold()
            existing = source_names.get(alias_key)
            if existing and existing.casefold() != entity.casefold():
                ambiguous_source_names.add(alias_key)
            else:
                source_names[alias_key] = entity
        return entity

    join_entities: List[Tuple[str, str, str, str]] = []
    for join in join_specs:
        if not isinstance(join, dict):
            raise ValueError("Datasphere join metadata has an invalid shape.")
        left_entity = join_source(join.get("left"), join)
        right_entity = join_source(join.get("right"), join)
        join_type = str(join.get("join_type") or join.get("type") or "INNER").casefold()
        if join_type in {"inner", "join", "inner join"}:
            sql_join_type = "INNER JOIN"
        elif join_type in {"left", "left outer", "left outer join"}:
            sql_join_type = "LEFT OUTER JOIN"
        else:
            raise ValueError(
                f"Datasphere join type {join_type!r} at {join.get('path') or 'unknown path'} "
                "has no verified ABAP Open SQL translation."
            )
        condition = join.get("condition") or join.get("on")
        if not isinstance(condition, str) or not condition.strip():
            raise ValueError(
                f"Datasphere join at {join.get('path') or 'unknown path'} has no explicit join condition."
            )
        join_entities.append((left_entity, right_entity, sql_join_type, condition))

    def translate_join_expression(expression: str) -> str:
        if len(source_metadata) == 1:
            return _translate_metadata_expression(expression, cds_fields, cds_name)
        reserved = {
            "and", "or", "not", "is", "null", "like", "in", "between",
            "case", "when", "then", "else", "end", "as", "true", "false",
            "distinct", "cast", "char", "integer", "decimal", "date",
        }
        reference = re.compile(
            r"(?<![A-Za-z0-9_])"
            r"(?:(?P<qualifier>[A-Za-z_][A-Za-z0-9_]*)[.~])?"
            r"[\"`]?(?P<field>[A-Za-z_][A-Za-z0-9_]*)[\"`]?"
            r"(?![A-Za-z0-9_])"
        )
        parts = re.split(r"('(?:''|[^'])*')", expression)
        for part_index in range(0, len(parts), 2):
            part = parts[part_index]

            def qualify(match: re.Match[str]) -> str:
                field = match.group("field")
                if part[match.end():].lstrip().startswith("(") or field.casefold() in reserved:
                    return match.group(0)
                qualifier = match.group("qualifier")
                if qualifier:
                    qualifier_key = qualifier.casefold()
                    if qualifier_key in ambiguous_source_names:
                        raise ValueError(
                            f"Expression qualifier {qualifier!r} is ambiguous in the verified join metadata."
                        )
                    entity = source_names.get(qualifier_key)
                    if entity is None:
                        raise ValueError(
                            f"Expression reference {qualifier}.{field} does not match a verified CDS entity or join alias."
                        )
                    source_key = entity.casefold()
                    canonical_field = source_fields[source_key].get(field.casefold())
                    if canonical_field is None:
                        raise ValueError(
                            f"Field {field!r} is not present in verified CDS entity {entity}."
                        )
                    return f"{entity}~{canonical_field}"
                owners = all_field_owners.get(field.casefold(), [])
                if len(owners) != 1:
                    raise ValueError(
                        f"Unqualified field {field!r} is missing or ambiguous across verified CDS sources."
                    )
                entity = owners[0]
                return f"{sources_by_entity[entity]['entity']}~{source_fields[entity][field.casefold()]}"

            parts[part_index] = reference.sub(qualify, part)
        return "".join(parts)

    if join_entities:
        introduced = {join_entities[0][0].casefold()}
        if join_entities[0][1] == join_entities[0][0]:
            raise ValueError("A Datasphere join cannot join a CDS entity to itself without an explicit alias.")
        first_left, first_right, first_type, first_condition = join_entities[0]
        cds_name = str(sources_by_entity[first_left.casefold()]["entity"])
        join_clauses.append(
            f"{first_type} {first_right} ON {translate_join_expression(first_condition)}"
        )
        introduced.add(first_right.casefold())
        for left_entity, right_entity, sql_join_type, condition in join_entities[1:]:
            if left_entity.casefold() not in introduced or right_entity.casefold() in introduced:
                raise ValueError(
                    "Datasphere join order does not form an unambiguous source-connected join chain."
                )
            join_clauses.append(
                f"{sql_join_type} {right_entity} ON {translate_join_expression(condition)}"
            )
            introduced.add(right_entity.casefold())
        if introduced != set(sources_by_entity):
            missing_entities = sorted(
                str(source["entity"])
                for key, source in sources_by_entity.items()
                if key not in introduced
            )
            raise ValueError(
                "Datasphere join metadata does not connect every verified CDS entity: "
                + ", ".join(missing_entities)
            )

    projection_sources = {
        str(item.get("output_field") or "").casefold(): str(item.get("source_field") or "")
        for item in transformation_ir.get("projections", [])
        if isinstance(item, dict) and item.get("output_field") and item.get("source_field")
    }
    projection_sources.update(grain_mappings)

    def selected_source_field(field_name: str) -> Optional[str]:
        mapped = projection_sources.get(
            field_name.casefold(),
            str(field_meta.get(field_name.casefold(), {}).get("source_field") or field_name),
        )
        return cds_fields.get(mapped.casefold())

    def output_expression(field_name: str) -> str:
        lowered = field_name.casefold()
        if lowered in calc_map:
            raw = _expand_calculated_field_dependencies(
                calc_map[lowered], calc_definitions, projection_sources
            ).strip()
            if raw.startswith("'") and raw.endswith("'"):
                return "'" + raw[1:-1].replace("''", "'").replace("'", "''") + "'"
            if raw.startswith('"') and raw.endswith('"'):
                return "'" + raw[1:-1].replace("'", "''") + "'"
            if field_meta.get(lowered, {}).get("transformation_category") == "constant":
                if raw.replace(".", "", 1).isdigit():
                    return raw
                return "'" + raw.replace("'", "''") + "'"
            return translate_join_expression(raw)
        source_field = selected_source_field(field_name)
        if source_field:
                return translate_join_expression(source_field)
        raise ValueError(
            f"No verified source field or calculation maps selected output {field_name!r}."
        )

    def is_measure(field_name: str) -> bool:
        metadata = field_meta.get(field_name.casefold(), {})
        return bool(metadata.get("is_measure") or metadata.get("measure_type") or metadata.get("aggregation"))

    def aggregation_for(field_name: str) -> str:
        aggregation = str(field_meta.get(field_name.casefold(), {}).get("aggregation") or "").lstrip("#").upper()
        if aggregation not in {"SUM", "COUNT", "MIN", "MAX", "AVG"}:
            raise ValueError(f"Verified aggregation semantics are missing or unsupported for measure {field_name!r}.")
        return aggregation

    # 1. Datasphere SQL
    select_ds = []
    has_measures = False
    for f_name in all_output_fields:
        fm = field_meta.get(f_name.casefold(), {})
        if is_measure(f_name):
            has_measures = True
            agg = aggregation_for(f_name)
            select_ds.append(f'{agg}("{f_name}") AS "{f_name}"')
        else:
            select_ds.append(f'"{f_name}"')

    if has_measures:
        non_measures_ds = [
            f'"{f_name}"'
            for f_name in all_output_fields
            if not is_measure(f_name)
        ]
        if non_measures_ds:
            datasphere_sql = (
                f'SELECT {", ".join(select_ds)} '
                f'FROM "{runtime_schema}"."{runtime_object}" '
                f'GROUP BY {", ".join(non_measures_ds)}'
            )
        else:
            datasphere_sql = (
                f'SELECT {", ".join(select_ds)} '
                f'FROM "{runtime_schema}"."{runtime_object}"'
            )
    else:
        datasphere_sql = (
            f'SELECT {", ".join(select_ds)} '
            f'FROM "{runtime_schema}"."{runtime_object}"'
        )

    # 2. S/4HANA SQL
    select_s4 = []
    group_by_s4 = []
    for f_name in all_output_fields:
        expr_abap = output_expression(f_name)
        if is_measure(f_name):
            select_s4.append(f"{aggregation_for(f_name)}( {expr_abap} ) AS {f_name}")
        else:
            select_s4.append(f"{expr_abap} AS {f_name}")
            if not (expr_abap.startswith("'") and expr_abap.endswith("'")) and not expr_abap.replace(".", "", 1).isdigit():
                group_by_s4.append(expr_abap)

    where_filters = []
    having_filters = []
    for filter_item in transformation_ir.get("filters", []):
        if not isinstance(filter_item, dict):
            raise ValueError("A Datasphere filter has an invalid metadata shape.")
        condition = filter_item.get("condition") or filter_item.get("expression")
        if not isinstance(condition, str) or not condition.strip():
            raise ValueError(
                f"A Datasphere filter at {filter_item.get('path') or filter_item.get('location') or 'unknown path'} "
                "has no retrievable expression."
            )
        case_comparison = re.fullmatch(
            r"\s*(?:[A-Za-z_][A-Za-z0-9_]*[.~])*"
            r"[\"`]?(?P<name>[A-Za-z_][A-Za-z0-9_]*)[\"`]?\s*=\s*(?P<value>'(?:''|[^'])*'|[-+]?\d+(?:\.\d+)?)\s*",
            condition,
        )
        if case_comparison and case_comparison.group("name").casefold() in calc_map:
            case_name = case_comparison.group("name")
            case_expr = _expand_calculated_field_dependencies(
                calc_map[case_name.casefold()], calc_definitions, projection_sources
            )
            case_match = re.fullmatch(
                r"\s*CASE\s+WHEN\s+(.+?)\s+THEN\s+(.+?)\s+ELSE\s+(.+?)\s+END\s*",
                case_expr,
                re.IGNORECASE,
            )
            requested_value = case_comparison.group("value")
            literal_pattern = r"(?:'(?:''|[^'])*'|[-+]?\d+(?:\.\d+)?)"
            if (
                case_match
                and re.fullmatch(literal_pattern, case_match.group(2).strip())
                and re.fullmatch(literal_pattern, case_match.group(3).strip())
                and _normalize_expression_preserving_literals(case_match.group(2))
                == _normalize_expression_preserving_literals(requested_value)
                and _normalize_expression_preserving_literals(case_match.group(3))
                != _normalize_expression_preserving_literals(requested_value)
            ):
                condition = case_match.group(1)
        expanded_condition = _expand_calculated_field_dependencies(
            condition, calc_definitions, projection_sources
        )
        translated_filter = translate_join_expression(expanded_condition)
        filter_location = str(
            filter_item.get("location") or filter_item.get("path") or ""
        ).casefold()
        if re.search(r"\bhaving\b", filter_location):
            having_filters.append(translated_filter)
        elif re.search(r"\bwhere\b", filter_location):
            where_filters.append(translated_filter)
        else:
            raise ValueError(
                f"Cannot determine whether Datasphere filter at "
                f"{filter_item.get('path') or filter_item.get('location') or 'unknown path'} "
                "belongs in WHERE or HAVING."
            )

    grouping_metadata = transformation_ir.get("groupings", [])
    for grouping in grouping_metadata:
        expression = grouping.get("expression") if isinstance(grouping, dict) else grouping
        if isinstance(expression, list):
            if all(
                isinstance(item, dict) and isinstance(item.get("ref"), list)
                for item in expression
            ):
                grouping_expressions = [
                    _render_csn_expression(item) for item in expression
                ]
            else:
                grouping_expressions = [_render_csn_expression(expression)]
        elif isinstance(expression, dict):
            grouping_expressions = [_render_csn_expression(expression)]
        else:
            grouping_expressions = [str(expression or "").strip()]
        if not any(grouping_expressions):
            raise ValueError("A Datasphere grouping definition is missing its expression.")
        for grouping_expression in grouping_expressions:
            if not grouping_expression:
                continue
            translated_group = output_expression(grouping_expression.rsplit(".", 1)[-1])
            if translated_group not in group_by_s4:
                group_by_s4.append(translated_group)

    group_by_s4 = list(dict.fromkeys(group_by_s4))
    if has_measures:
        selected_expressions = {
            _normalize_expression_preserving_literals(
                re.sub(
                    r"\s+AS\s+[A-Za-z_][A-Za-z0-9_]*\s*$",
                    "",
                    select_expression,
                    flags=re.IGNORECASE,
                )
            )
            for select_expression in select_s4
        }
        selected_aliases = set()
        for select_expression in select_s4:
            alias_match = re.search(
                r"\s+AS\s+([A-Za-z_][A-Za-z0-9_]*)\s*$",
                select_expression,
                re.IGNORECASE,
            )
            if alias_match:
                selected_aliases.add(alias_match.group(1).casefold())
        for index, grouping_expression in enumerate(group_by_s4, start=1):
            normalized_grouping = _normalize_expression_preserving_literals(
                grouping_expression
            )
            if normalized_grouping in selected_expressions:
                continue
            alias_index = index
            hidden_alias = f"DS_GROUP_{alias_index}"
            while hidden_alias.casefold() in selected_aliases:
                alias_index += 1
                hidden_alias = f"DS_GROUP_{alias_index}"
            select_s4.append(f"{grouping_expression} AS {hidden_alias}")
            selected_expressions.add(normalized_grouping)
            selected_aliases.add(hidden_alias.casefold())

    s4hana_sql = "SELECT\n    " + ",\n    ".join(select_s4) + f"\nFROM {cds_name}"
    if join_clauses:
        s4hana_sql += "\n" + "\n".join(join_clauses)
    if where_filters:
        s4hana_sql += "\nWHERE " + "\n  AND ".join(where_filters)
    if has_measures and group_by_s4:
        s4hana_sql += "\nGROUP BY\n    " + ",\n    ".join(group_by_s4)
    if having_filters:
        if not has_measures:
            raise ValueError("Datasphere HAVING filter exists without a verified aggregate measure.")
        s4hana_sql += "\nHAVING " + "\n  AND ".join(having_filters)

    return {
        "datasphere_sql": datasphere_sql,
        "s4hana_sql": s4hana_sql,
        "output_fields": all_output_fields,
        "key_fields": key_fields,
        "logic_summary": f"Metadata fallback selected {len(all_output_fields)} outputs from verified CDS entity {cds_name} and retained discovered filters, calculations, and aggregation semantics.",
        "assumptions": [],
    }
