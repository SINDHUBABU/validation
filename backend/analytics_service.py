import re
import time
import uuid
import asyncio
from typing import Dict, Any, List, Optional
from fastapi import HTTPException

from config import (
    logger, DATASPHERE_SPACE_ID, DATASPHERE_API_URL, HANA_HOST, HANA_PORT,
    USE_DIRECT_HANA, GEMINI_API_KEY, OPENAI_API_KEY, API_KEY,
    space_views_cache, conversation_sessions
)
from schemas import QueryRequest, QueryResponse, KPICard, KPISummary
from datasphere_client import (
    get_datasphere_token, get_tenant_base_host, discover_space_views,
    probe_view_columns, find_views_matching_pattern, extract_view_names_from_prompt,
    route_prompt_to_views, deduplicate_view_names
)
from hana_client import (
    probe_hana_view_parameters_sync, inject_hana_view_placeholders, execute_direct_hana_query,
    discover_hana_execution_object_sync,
)
from sql_compiler import detect_foreign_key_relations, resolve_conversational_follow_up
from llm_client import generate_odata_query_blueprint
import rag_service

async def run_analytics_query(request: QueryRequest) -> QueryResponse:
    """Core analytics execution service: auto-discovers views, compiles SQL, runs direct on HANA, and builds KPI summary."""
    raw_prompt = request.user_prompt.strip()
    if (raw_prompt.startswith('"') and raw_prompt.endswith('"')) or (raw_prompt.startswith("'") and raw_prompt.endswith("'")):
        inner = raw_prompt[1:-1].strip()
        if inner:
            raw_prompt = inner

    space_id = request.space_id.strip() if request.space_id else DATASPHERE_SPACE_ID
    session_id = request.session_id or str(uuid.uuid4())
    selected_fields = [f.strip() for f in request.selected_fields if f.strip()] if request.selected_fields else []
    base_host = get_tenant_base_host(DATASPHERE_API_URL)

    try:
        # 1. Resolve Conversational Multi-Turn Follow-Ups
        user_prompt, conv_context = resolve_conversational_follow_up(raw_prompt, session_id)

        # 2. Acquire Bearer token
        bearer_token = await get_datasphere_token()

        # 3. Parse or Auto-Discover Views by Space ID
        requested_views = []
        if request.view_names:
            for v in request.view_names:
                if v and v.strip():
                    for sub_v in v.replace(";", ",").split(","):
                        if sub_v.strip() and sub_v.strip() not in requested_views:
                            requested_views.append(sub_v.strip())
        elif request.view_name:
            for sub_v in request.view_name.replace(";", ",").split(","):
                if sub_v.strip() and sub_v.strip() not in requested_views:
                    requested_views.append(sub_v.strip())

        if not requested_views:
            discovered_catalog = await discover_space_views(base_host, space_id, bearer_token, search_hint=raw_prompt)

            # 1. Pattern matching across views
            pattern_matches = find_views_matching_pattern(raw_prompt, discovered_catalog) if discovered_catalog else []
            if pattern_matches:
                requested_views = pattern_matches
                logger.info(f"Pattern matched View(s) in Space [{space_id}]: {requested_views}")

            # 2. Explicit exact view name in prompt
            if not requested_views:
                prompt_extracted_views = extract_view_names_from_prompt(raw_prompt)
                if prompt_extracted_views:
                    for ev in prompt_extracted_views:
                        matched_ev = next((v for v in discovered_catalog if v.lower() == ev.lower()), ev)
                        if matched_ev not in requested_views:
                            requested_views.append(matched_ev)
                            logger.info(f"Extracted View '{matched_ev}' directly from prompt text.")

            # 3. Conversational context inherit previous views
            if not requested_views and conv_context and conv_context.get("previous_views"):
                requested_views = conv_context["previous_views"]

            # 4. RAG-based Semantic View Vector Retrieval
            if not requested_views:
                gem_key = GEMINI_API_KEY or (API_KEY if API_KEY and not API_KEY.startswith("sk-") else "")
                oa_key = OPENAI_API_KEY or (API_KEY if API_KEY and API_KEY.startswith("sk-") else "")
                rag_results = await rag_service.retrieve_relevant_views_rag(raw_prompt, space_id=space_id, top_k=3, gemini_key=gem_key, openai_key=oa_key)
                if rag_results:
                    rag_views = [r[0] for r in rag_results if r[1] > 0.15]
                    if rag_views:
                        requested_views = rag_views
                        logger.info(f"RAG semantically routed View(s) in Space [{space_id}]: {requested_views}")

            # 5. Column-level routing fallback
            if not requested_views and discovered_catalog:
                requested_views = await route_prompt_to_views(raw_prompt, space_id, discovered_catalog, base_host, bearer_token)
                logger.info(f"Semantically routed View(s) in Space [{space_id}]: {requested_views}")

        if not requested_views:
            return QueryResponse(
                session_id=session_id,
                query_blueprint={"explanation": f"No view found matching '{raw_prompt}'."},
                retrieved_view_metadata={"space_id": space_id, "columns": []},
                execution_info={
                    "datasphere_endpoint": base_host,
                    "space_id": space_id,
                    "view_name": "None",
                    "live_mode": False,
                    "total_records": 0,
                    "odata_parameters": {}
                },
                kpi_summary=KPISummary(
                    direct_answer=f"Could not find any live view in Space '{space_id}' matching '{raw_prompt}'. Please check the view name or enter it in the left sidebar.",
                    target_metric=None,
                    cards=[
                        KPICard(label="Status", value=0, formatted_value="View Not Found", metric_type="count"),
                        KPICard(label="Active Space", value=space_id, formatted_value=space_id, metric_type="text")
                    ]
                ),
                data=[]
            )

        requested_views = deduplicate_view_names(requested_views)

        if space_id not in space_views_cache:
            space_views_cache[space_id] = []
        for rv in requested_views:
            if not any(x.lower() == rv.lower() for x in space_views_cache[space_id]):
                space_views_cache[space_id].append(rv)

        logger.info(f"Executing Query on View(s) [{', '.join(requested_views)}] in Space [{space_id}] for: '{user_prompt}'")

        # 4. Discover live columns for selected views
        probe_tasks = [probe_view_columns(base_host, space_id, v, bearer_token) for v in requested_views]
        probe_results = await asyncio.gather(*probe_tasks)
        
        view_schemas: Dict[str, List[str]] = {}
        for idx, v in enumerate(requested_views):
            view_schemas[v] = probe_results[idx] if probe_results[idx] else []

        if len(requested_views) > 1:
            def score_view_relevance(v_name: str, cols: List[str]) -> int:
                score = 0
                v_clean = v_name.lower().replace("_", "")
                p_clean = user_prompt.lower().replace("_", "")
                if v_name.lower() in user_prompt.lower() or v_clean in p_clean:
                    score += 100
                for term in re.findall(r'[a-zA-Z0-9]{3,}', user_prompt.lower()):
                    if term in v_clean:
                        score += 30
                for col in cols:
                    c_clean = col.lower().replace("_", "")
                    if col.lower() in user_prompt.lower() or (len(c_clean) >= 3 and c_clean in p_clean):
                        score += 25
                return score

            requested_views.sort(key=lambda v: score_view_relevance(v, view_schemas.get(v, [])), reverse=True)
            view_schemas = {v: view_schemas.get(v, []) for v in requested_views}

        # 5. Detect Foreign Keys
        foreign_keys = detect_foreign_key_relations(view_schemas)

        # 5.1 Probe input parameters for views
        view_params: Dict[str, List[Dict[str, Any]]] = {}
        for v in requested_views:
            if USE_DIRECT_HANA:
                v_p = probe_hana_view_parameters_sync(space_id, v)
                if v_p:
                    view_params[v] = v_p

        # 6. Generate a query, or directly preview the explicitly selected live view.
        if request.raw_view_preview:
            preview_view = requested_views[0]
            execution_object = discover_hana_execution_object_sync(preview_view, preferred_schema=space_id)
            object_schema = str((execution_object or {}).get("schema") or space_id)
            object_name = str((execution_object or {}).get("object") or preview_view)
            quote_identifier = lambda value: '"' + value.replace('"', '""') + '"'
            available_columns = {
                str(column.get("name") or "").casefold(): str(column.get("name") or "")
                for column in (execution_object or {}).get("columns", [])
                if isinstance(column, dict) and column.get("name")
            }
            projected_fields = [
                available_columns[field.casefold()]
                for field in selected_fields
                if field.casefold() in available_columns
            ]
            missing_projected_fields = [
                field for field in selected_fields if field.casefold() not in available_columns
            ]
            # The selected analytical model can expose calculated fields that
            # do not physically exist on its underlying fact view. In that
            # case retrieve the fact view's technical columns as inputs; the
            # model-level query later returns the calculated outputs.
            # This is the selected model's underlying fact-view preview. Keep
            # all of its technical columns (including model inputs and
            # calculated outputs) so the CDS replica can mirror the complete
            # fact projection; the final model query still uses its own field
            # projection during validation.
            projections = "*"
            sql_to_run = f"SELECT {projections} FROM {quote_identifier(object_schema)}.{quote_identifier(object_name)}"
            blueprint = {
                "sql_query": sql_to_run,
                "explanation": f"Direct read-only preview of {preview_view} using all fields returned by the Datasphere fact view.",
                "requested_model_fields": selected_fields,
                "unavailable_on_fact_view": missing_projected_fields,
                "retrieved_fact_fields": "returned by HANA result metadata",
            }
        else:
            blueprint = await generate_odata_query_blueprint(user_prompt, view_schemas, space_id, foreign_keys, selected_fields, view_params=view_params)
            sql_to_run = blueprint.get("sql_query", f'SELECT * FROM "{requested_views[0]}" t1')

        # 6.1 Inject parameter placeholders
        for v in requested_views:
            if v in view_params and view_params[v]:
                sql_to_run = inject_hana_view_placeholders(sql_to_run, space_id, v, view_params[v], user_prompt)
        blueprint["sql_query"] = sql_to_run

        # 7. Pure Direct SAP HANA Cloud Execution (Zero-Buffer Mode)
        if not USE_DIRECT_HANA:
            raise HTTPException(
                status_code=500,
                detail="SAP Datasphere HANA direct SQL connection is not configured in backend/.env. Please set HANA_HOST, HANA_USER, and HANA_PASSWORD."
            )

        try:
            final_data, exec_duration, actual_sql = execute_direct_hana_query(sql_to_run, space_id, user_prompt=user_prompt)
            blueprint["sql_query"] = actual_sql
            sql_to_run = actual_sql
            if request.raw_view_preview and final_data:
                actual_by_casefold = {str(name).casefold(): str(name) for name in final_data[0]}
                blueprint["retrieved_fact_fields"] = list(actual_by_casefold.values())
                blueprint["unavailable_on_fact_view"] = [
                    name for name in selected_fields
                    if name.casefold() not in actual_by_casefold
                ]
        except Exception as hana_err:
            logger.error(f"[SAP HANA Execution Error] {hana_err}")
            return QueryResponse(
                session_id=session_id,
                query_blueprint=blueprint,
                retrieved_view_metadata={
                    "views": requested_views,
                    "space_id": space_id,
                    "foreign_keys": foreign_keys,
                    "columns": []
                },
                execution_info={
                    "datasphere_endpoint": f"{HANA_HOST}:{HANA_PORT}",
                    "space_id": space_id,
                    "view_name": ", ".join(requested_views),
                    "live_mode": True,
                    "total_records": 0,
                    "execution_engine": "SAP HANA Cloud (Direct SQL - Error)",
                    "error": str(hana_err)
                },
                kpi_summary=KPISummary(
                    direct_answer=f"SAP HANA Cloud Query Error: {str(hana_err)}",
                    target_metric=None,
                    cards=[
                        KPICard(label="Execution Status", value="SQL Error", formatted_value="Error", metric_type="text"),
                        KPICard(label="Space ID", value=space_id, formatted_value=space_id, metric_type="text")
                    ]
                ),
                data=[],
                views_data={v: [] for v in requested_views}
            )

        # 8. Compute KPI Summary
        direct_ans = f"Returned {len(final_data):,} records from [{', '.join(requested_views)}] via Direct SAP HANA SQL ({exec_duration:.2f}s)."
        if conv_context and conv_context.get("referenced_entity"):
            direct_ans = f"Details for {conv_context['referenced_entity']}: {len(final_data):,} records analyzed."

        cards = [
            KPICard(label="Combined Records", value=len(final_data), formatted_value=f"{len(final_data):,}", metric_type="count"),
            KPICard(label="Execution Time", value=f"{exec_duration:.2f}s", formatted_value=f"{exec_duration:.2f}s", metric_type="text"),
            KPICard(label="Execution Engine", value="SAP HANA Cloud", formatted_value="Zero-Buffer SQL", metric_type="text")
        ]

        if final_data and len(final_data) > 0:
            sample = final_data[0]
            num_keys = [k for k, v in sample.items() if isinstance(v, (int, float))]
            if num_keys:
                top_metric = num_keys[0]
                m_sum = sum(float(r.get(top_metric, 0) or 0) for r in final_data)
                m_avg = m_sum / len(final_data) if len(final_data) > 0 else 0
                cards.append(KPICard(label=f"Total {top_metric}", value=m_sum, formatted_value=f"{m_sum:,.2f}" if abs(m_sum - round(m_sum)) > 0.01 else f"{int(m_sum):,}", metric_type="sum"))
                cards.append(KPICard(label=f"Avg {top_metric}", value=m_avg, formatted_value=f"{m_avg:,.2f}", metric_type="avg"))

        if len(requested_views) > 1 and foreign_keys:
            cards.append(KPICard(label="Relational Joins", value=len(foreign_keys), formatted_value=str(len(foreign_keys)), metric_type="count"))

        kpi_summary = KPISummary(
            direct_answer=direct_ans,
            target_metric=blueprint.get("target_metric"),
            cards=cards
        )

        conversation_sessions[session_id] = {
            "last_prompt": raw_prompt,
            "last_views": requested_views,
            "last_data": final_data[:20],
            "last_sql": sql_to_run,
            "updated_at": time.time()
        }

        return QueryResponse(
            session_id=session_id,
            query_blueprint=blueprint,
            retrieved_view_metadata={
                "views": requested_views,
                "space_id": space_id,
                "foreign_keys": foreign_keys,
                "selected_fields": selected_fields,
                "description": f"Direct SAP HANA Relational dataset across {len(requested_views)} views",
                "columns": list(final_data[0].keys()) if final_data else []
            },
            execution_info={
                "datasphere_endpoint": f"{HANA_HOST}:{HANA_PORT}",
                "space_id": space_id,
                "view_name": ", ".join(requested_views),
                "views_joined": requested_views,
                "views_auto_discovered": len(request.view_names) == 0,
                "conversational_follow_up": bool(conv_context),
                "foreign_key_relations": foreign_keys,
                "odata_parameters": {},
                "live_mode": True,
                "total_records": len(final_data),
                "execution_engine": "SAP HANA Cloud (Direct Database Explorer SQL)"
            },
            kpi_summary=kpi_summary,
            data=final_data,
            views_data={v: [] for v in requested_views}
        )
    except HTTPException as he:
        logger.warning(f"HTTPException in execute_analytics: {he.detail}")
        return QueryResponse(
            session_id=session_id,
            query_blueprint={"explanation": str(he.detail)},
            retrieved_view_metadata={"space_id": space_id, "columns": []},
            execution_info={
                "datasphere_endpoint": base_host,
                "space_id": space_id,
                "view_name": "None",
                "live_mode": False,
                "total_records": 0,
                "odata_parameters": {}
            },
            kpi_summary=KPISummary(
                direct_answer=f"Could not complete query: {he.detail}",
                target_metric=None,
                cards=[
                    KPICard(label="Error Notice", value=0, formatted_value="Check Connection", metric_type="count"),
                    KPICard(label="Space ID", value=space_id, formatted_value=space_id, metric_type="text")
                ]
            ),
            data=[]
        )
    except Exception as e:
        logger.exception(f"Unhandled error in execute_analytics: {e}")
        return QueryResponse(
            session_id=session_id,
            query_blueprint={"explanation": f"Notice: {str(e)}"},
            retrieved_view_metadata={"space_id": space_id, "columns": []},
            execution_info={
                "datasphere_endpoint": base_host,
                "space_id": space_id,
                "view_name": "None",
                "live_mode": False,
                "total_records": 0,
                "odata_parameters": {}
            },
            kpi_summary=KPISummary(
                direct_answer=f"Could not complete query: {str(e)}",
                target_metric=None,
                cards=[
                    KPICard(label="Error Notice", value=0, formatted_value="Check Connection", metric_type="count"),
                    KPICard(label="Space ID", value=space_id, formatted_value=space_id, metric_type="text")
                ]
            ),
            data=[]
        )

