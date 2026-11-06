import asyncio
import hashlib
import csv
import json
import os
import re
import sqlite3
import time
import uuid
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Optional, Dict, Any, List
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# Configuration & Logging
from config import (
    logger, DATASPHERE_SPACE_ID, DATASPHERE_MODELING_SPACE, DATASPHERE_REPLICATION_SPACE,
    DATASPHERE_API_URL, DATASPHERE_TOKEN_URL,
    GEMINI_API_KEY, OPENAI_API_KEY, API_KEY,
    conversation_sessions, space_assets_cache, USE_SAP_ADT, get_config,
    get_system_diagnostics
)

# Pydantic Schemas
from schemas import (
    SchemaRequest, QueryRequest, QueryResponse,
    SaveSessionRequest, RenameSessionRequest, GoldenSQLRequest,
    GlossaryRequest, DocumentRequest, RAGSearchRequest,
    ValidationRequest, ValidationResponse, ViewDetailsResponse,
    CDSStageRequest, StagedSQLExecutionRequest, StagedSQLPreviewRequest,
    StagedCsvPromptRequest,
)

# Local Modular Services
from session_store import (
    init_session_db, save_chat_session_to_db, get_all_chat_sessions_from_db,
    rename_chat_session_in_db, delete_chat_session_from_db, clear_all_chat_sessions_from_db
)
from datasphere_client import (
    get_datasphere_token, get_tenant_base_host, discover_space_views, probe_view_columns,
    fetch_view_edmx_schema, fetch_catalog_asset_details
)
from sql_compiler import detect_foreign_key_relations
from analytics_service import run_analytics_query
from validation_service import _walk_dependency_nodes, _build_transformation_ir, run_metadata_driven_validation
from retrieval.data_retriever import DataRetriever
import rag_service

try:
    from reconciliation.reconciliation_engine import ReconciliationEngine
except ImportError:
    ReconciliationEngine = None

from reconciliation.datasphere_resolver_client import DatasphereResolverClient
from reconciliation.dependency_resolver import DatasphereDependencyResolver
from reconciliation.datasphere_space_cache import metadata_cache_ttl_seconds
from reconciliation.cds_staging_store import CdsStagingStore
from adt.cds_metadata import CDSMetadataClient

_MODEL_METADATA_CACHE_TTL = metadata_cache_ttl_seconds()
_MODEL_CACHE_WARM_TASKS: Dict[str, asyncio.Task] = {}


def _warm_model_metadata_cache(space_id: str, model_names: List[str], force_refresh: bool = False) -> Dict[str, int]:
    """Persist dependency metadata for all discovered analytical models."""
    client = DatasphereResolverClient(space=space_id)
    cache = client.cache
    if cache is None:
        return {"cached": 0, "skipped": len(model_names), "failed": len(model_names)}
    resolver = DatasphereDependencyResolver(client=client, space=space_id, silent=True)
    fresh_after = time.time() - _MODEL_METADATA_CACHE_TTL
    cached_count = skipped_count = failed_count = 0
    for model_name in model_names:
        try:
            existing = cache.get_model_metadata(space_id, model_name)
            payload = existing.get("metadata", {}) if existing else {}
            if (
                not force_refresh
                and existing
                and float(existing.get("last_synced", 0)) >= fresh_after
                and isinstance(payload.get("dependency_tree"), dict)
            ):
                skipped_count += 1
                continue
            tree = resolver.resolve(model_name, object_type="analytic_model", space=space_id)
            cache.upsert_model_metadata(space_id, model_name, {
                **payload,
                "dependency_tree": tree,
                "field_catalog": tree.get("field_catalog", []),
                "calculated_columns": tree.get("calculated_columns", []),
                "lineage_path": [
                    str(node.get("name"))
                    for node in _walk_dependency_nodes(tree)
                    if node.get("name")
                ],
                "cache_source": "datasphere_cli_design_time",
            })
            cached_count += 1
        except Exception as exc:
            failed_count += 1
            logger.warning("Metadata cache warm failed for %s/%s: %s", space_id, model_name, exc)
    return {"cached": cached_count, "skipped": skipped_count, "failed": failed_count}


def _schedule_model_metadata_warm(space_id: str, model_names: List[str], force_refresh: bool = False) -> None:
    task_key = space_id.casefold()
    current = _MODEL_CACHE_WARM_TASKS.get(task_key)
    if current is not None and not current.done():
        if not force_refresh:
            return

    async def run_warm() -> None:
        try:
            if current is not None and not current.done():
                await current
            result = await asyncio.to_thread(
                _warm_model_metadata_cache, space_id, model_names, force_refresh
            )
            logger.info("Datasphere metadata cache warm for %s: %s", space_id, result)
        except Exception:
            logger.exception("Datasphere metadata cache warm failed for %s", space_id)
        finally:
            if _MODEL_CACHE_WARM_TASKS.get(task_key) is asyncio.current_task():
                _MODEL_CACHE_WARM_TASKS.pop(task_key, None)

    _MODEL_CACHE_WARM_TASKS[task_key] = asyncio.create_task(run_warm())


def _resolved_catalog_object_type(asset: Dict[str, Any], dependency_tree: Dict[str, Any]) -> str:
    """Prefer the catalog type, then fall back to the resolved Datasphere object type."""
    catalog_type = asset.get("type") or asset.get("assetType")
    resolved_type = str(catalog_type or "").strip()
    if not resolved_type or resolved_type.casefold() == "unknown":
        resolved_type = str(dependency_tree.get("object_type") or "").strip()

    canonical_types = {
        "analytic model": "Analytical Model",
        "analytic_model": "Analytical Model",
        "analytical model": "Analytical Model",
        "analytical_model": "Analytical Model",
        "fact model": "Fact Model",
        "fact_model": "Fact Model",
        "view": "View",
        "local table": "Local Table",
        "local_table": "Local Table",
        "remote table": "Remote Table",
        "remote_table": "Remote Table",
        "shared remote table": "Shared Remote Table",
        "shared_remote_table": "Shared Remote Table",
    }
    return canonical_types.get(resolved_type.casefold(), resolved_type or "unknown")


# Lifespan Management
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting SAP Datasphere Conversational AI Agent (Space Auto-Discovery & Multi-Turn Chat)...")
    init_session_db()
    rag_service.init_rag_db()
    yield
    logger.info("Stopping Agent.")

# FastAPI Application
app = FastAPI(
    title="Data Sphere Validation Agent",
    description="Zero-Leakage Multi-Measure Data Validation Engine for SAP Datasphere & S/4HANA CDS",
    version="5.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Info & Health API Endpoints
# ---------------------------------------------------------------------------
@app.get("/api/info")
@app.get("/health")
async def get_info():
    diag = get_system_diagnostics()
    return {
        "status": "online",
        "space_id": DATASPHERE_SPACE_ID,
        "token_url": DATASPHERE_TOKEN_URL,
        "base_url": DATASPHERE_API_URL,
        "modeling_space": DATASPHERE_MODELING_SPACE,
        "replication_space": DATASPHERE_REPLICATION_SPACE,
        "diagnostics": diag,
    }

# ---------------------------------------------------------------------------
# Space Views & Schema Discovery API Endpoints
# ---------------------------------------------------------------------------
@app.get("/api/views")
@app.get("/api/views/catalog")
async def get_space_catalog(space_id: Optional[str] = None, refresh: bool = False):
    target_space = space_id.strip() if space_id else DATASPHERE_SPACE_ID
    if not target_space:
        raise HTTPException(
            status_code=400,
            detail="Datasphere space is not configured. Set DATASPHERE_SPACE_ID in backend/.env or specify space_id parameter."
        )

    discovered_views: List[str] = []
    source = "datasphere_consumption_catalog"

    # 1. Primary: Extract live Analytical Models and Views from Datasphere CLI
    try:
        resolver_client = DatasphereResolverClient(space=target_space)
        if resolver_client.cli_available:
            models_list = await asyncio.to_thread(resolver_client.list_analytic_models, target_space, 500)
            views_list = await asyncio.to_thread(resolver_client.list_views, target_space, 500)
            
            # Prioritize Analytical Models for the analyzer dashboard
            for m in models_list:
                name = m.get("technicalName") or m.get("name")
                if name and name not in discovered_views:
                    discovered_views.append(name)
            for v in views_list:
                name = v.get("technicalName") or v.get("name")
                if name and name not in discovered_views:
                    discovered_views.append(name)

            if discovered_views:
                source = "datasphere_cli"
                model_names = [
                    str(item.get("technicalName") or item.get("name") or "").strip()
                    for item in models_list
                    if str(item.get("technicalName") or item.get("name") or "").strip()
                ]
                _schedule_model_metadata_warm(target_space, model_names, force_refresh=refresh)
                return {
                    "space_id": target_space,
                    "total_views": len(discovered_views),
                    "views": discovered_views,
                    "source": source
                }
    except Exception as e:
        logger.warning(f"Datasphere CLI catalog extraction notice for {target_space}: {e}")

    # 2. Secondary: Extract consumption-exposed views from official Datasphere Catalog
    try:
        bearer_token = await get_datasphere_token()
        base_host = get_tenant_base_host(DATASPHERE_API_URL)
        views = await discover_space_views(base_host, target_space, bearer_token)
        if views:
            for v in views:
                if v not in discovered_views:
                    discovered_views.append(v)
            return {
                "space_id": target_space,
                "total_views": len(discovered_views),
                "views": discovered_views,
                "source": "datasphere_consumption_catalog"
            }
    except Exception as e:
        logger.warning(f"Datasphere Catalog extraction notice for {target_space}: {e}")

    return {
        "space_id": target_space,
        "total_views": len(discovered_views),
        "views": discovered_views,
        "source": source
    }

@app.post("/api/views/schema")
async def get_views_schema(req: SchemaRequest):
    space_id = req.space_id.strip() if req.space_id else DATASPHERE_SPACE_ID
    bearer_token = await get_datasphere_token()
    base_host = get_tenant_base_host(DATASPHERE_API_URL)

    clean_views = [v.strip() for v in req.view_names if v.strip()]
    if not clean_views:
        clean_views = await discover_space_views(base_host, space_id, bearer_token)

    tasks = [probe_view_columns(base_host, space_id, v, bearer_token) for v in clean_views]
    results = await asyncio.gather(*tasks)

    schemas: Dict[str, List[str]] = {}
    for idx, v in enumerate(clean_views):
        schemas[v] = results[idx] if results[idx] else []

    foreign_keys = detect_foreign_key_relations(schemas)

    return {
        "space_id": space_id,
        "views": schemas,
        "foreign_keys": foreign_keys
    }

@app.get("/api/views/{view_name}/details", response_model=ViewDetailsResponse)
async def get_view_details_endpoint(
    view_name: str,
    space_id: Optional[str] = None,
    force_refresh: bool = False,
):
    """
    Returns live consumption metadata and any lineage/impact properties present
    in the tenant's catalog asset descriptor. Missing lineage remains explicit.
    """
    target_space = space_id.strip() if space_id else DATASPHERE_SPACE_ID

    resolver_client = DatasphereResolverClient(space=target_space)
    cache = resolver_client.cache
    if cache is not None and not force_refresh:
        cached = cache.get_model_metadata(target_space, view_name)
        if cached:
            cached_payload = cached.get("metadata", {})
            cached_response = cached_payload.get("response") if isinstance(cached_payload, dict) else None
            response_synced_at = float(
                cached_payload.get("response_synced_at", cached.get("last_synced", 0))
            ) if isinstance(cached_payload, dict) else 0
            is_fresh = time.time() - response_synced_at <= _MODEL_METADATA_CACHE_TTL
            if is_fresh and isinstance(cached_response, dict):
                cached_response = dict(cached_response)
                cached_tree = (
                    cached_payload.get("dependency_tree", {})
                    if isinstance(cached_payload, dict) else {}
                )
                resolved_type = _resolved_catalog_object_type(
                    {"type": cached_response.get("type")},
                    cached_tree if isinstance(cached_tree, dict) else {},
                )
                cached_response["type"] = resolved_type
                cached_metadata_json = cached_response.get("metadata_json")
                if isinstance(cached_metadata_json, dict):
                    cached_response["metadata_json"] = {
                        **cached_metadata_json,
                        "type": resolved_type,
                    }
                return cached_response
    
    # 1. Fetch live EDMX schema directly from SAP Datasphere (Zero Hardcoding)
    token = await get_datasphere_token()
    base_host = get_tenant_base_host(DATASPHERE_API_URL)
    
    edmx_res = await fetch_view_edmx_schema(base_host, target_space, view_name, token)
    measures = edmx_res.get("measures", [])
    dimensions = edmx_res.get("dimensions", [])
    all_fields = edmx_res.get("fields", [])

    # If EDMX returned no fields (e.g. relational view or direct table), probe columns
    if not all_fields:
        probed_cols = await probe_view_columns(base_host, target_space, view_name, token)
        for c in probed_cols:
            all_fields.append({"name": c, "label": c, "type": "", "category": "unknown", "aggregation": None, "is_key": False})

    # 4. Catalog asset descriptor
    asset_key = f"{target_space}/{view_name}".lower()
    asset = space_assets_cache.get(asset_key, {})
    asset = await fetch_catalog_asset_details(base_host, target_space, asset, token)

    # The consumption catalog response is the only configured live source here.
    def find_values(node, names):
        found = []
        if isinstance(node, dict):
            for key, value in node.items():
                if str(key).replace("_", "").lower() in names:
                    found.append(value)
                found.extend(find_values(value, names))
        elif isinstance(node, list):
            for value in node:
                found.extend(find_values(value, names))
        return found

    def to_nodes(node, parent_path=""):
        output = []
        if isinstance(node, list):
            for child in node:
                output.extend(to_nodes(child, parent_path))
        elif isinstance(node, dict):
            name = node.get("name") or node.get("technicalName") or node.get("objectName") or node.get("assetName") or node.get("id")
            path = f"{parent_path} -> {name}" if parent_path and name else (str(name) if name else parent_path)
            if name:
                output.append({"name": str(name), "type": node.get("type") or node.get("objectType") or node.get("assetType") or "unknown", "space": node.get("space") or node.get("spaceName"), "lineage_path": path, "metadata": node})
            for key, child in node.items():
                if str(key).replace("_", "").lower() in {"dependencies", "children", "lineage", "sources", "upstream", "inputobjects", "objects", "nodes", "items", "value", "downstream", "dependents", "impacts"}:
                    output.extend(to_nodes(child, path))
        return output

    lineage_values = find_values(asset, {"lineage", "lineagechain", "dependencies", "upstream", "sources", "inputobjects"})
    impact_values = find_values(asset, {"impact", "impacts", "downstream", "dependents", "affectedobjects"})
    lineage_raw = lineage_values[0] if lineage_values else []
    impact_raw = impact_values[0] if impact_values else []
    lineage_nodes = to_nodes(lineage_raw)
    impact_nodes = to_nodes(impact_raw)
    lineage = [{"step": i + 1, "name": node["name"], "type": node["type"], "space": node.get("space") or target_space, "role": "Reported by Datasphere catalog"} for i, node in enumerate(lineage_nodes)]
    raw_sources = [node for node in lineage_nodes if not any(node["name"] in other.get("lineage_path", "").split(" -> ")[:-1] for other in lineage_nodes)]
    sources = [{**node, "matched_cds_view": None, "replicated_from_adt": None, "replication_space": node.get("space"), "description": None, "key_fields": []} for node in raw_sources]
    lineage_analysis = {"status": "available" if lineage_nodes else "unavailable", "items": lineage_nodes, "raw_sources": raw_sources, "message": None if lineage_nodes else "The current tenant catalog response does not expose lineage for this asset."}
    impact_analysis = {"status": "available" if impact_nodes else "unavailable", "items": impact_nodes, "message": None if impact_nodes else "The current tenant catalog response does not expose impact data for this asset."}

    dependency_tree: Dict[str, Any] = {}
    try:
        inventory_refresh_error = None
        try:
            replicated_tables = await asyncio.to_thread(
                resolver_client.refresh_replication_local_table_index
            )
        except Exception as exc:
            inventory_refresh_error = exc
            logger.warning(
                "Configured replication-space refresh failed for %s; trying the saved inventory: %s",
                view_name,
                exc,
            )
            replicated_tables = (
                resolver_client.cache.list_replication_local_tables()
                if resolver_client.cache is not None
                else []
            )

        resolver = DatasphereDependencyResolver(
            client=resolver_client,
            space=target_space,
            silent=True,
        )
        cached_tree_entry = (
            cache.get_model_metadata(target_space, view_name)
            if cache is not None and not force_refresh else None
        )
        cached_tree_payload = cached_tree_entry.get("metadata", {}) if cached_tree_entry else {}
        cached_tree = cached_tree_payload.get("dependency_tree") if isinstance(cached_tree_payload, dict) else None
        tree_is_fresh = bool(
            cached_tree_entry
            and time.time() - float(cached_tree_entry.get("last_synced", 0)) <= _MODEL_METADATA_CACHE_TTL
            and isinstance(cached_tree, dict)
        )
        if tree_is_fresh:
            dependency_tree = cached_tree
            resolver_client.cli_reads.add((
                str(dependency_tree.get("object_type") or "analytic_model"),
                view_name.casefold(),
            ))
        else:
            dependency_tree = await asyncio.to_thread(resolver.resolve, view_name)
            if cache is not None:
                cache.upsert_model_metadata(target_space, view_name, {
                    **(cached_tree_payload if isinstance(cached_tree_payload, dict) else {}),
                    "dependency_tree": dependency_tree,
                    "field_catalog": dependency_tree.get("field_catalog", []),
                    "calculated_columns": dependency_tree.get("calculated_columns", []),
                    "lineage_path": [
                        str(node.get("name")) for node in _walk_dependency_nodes(dependency_tree)
                        if node.get("name")
                    ],
                    "cache_source": "datasphere_cli_design_time",
                })
        
        # If consumption EDMX returned no fields (common for Analytical Models), extract from dependency_tree field_catalog
        if (not all_fields or not measures) and dependency_tree.get("field_catalog"):
            for f in dependency_tree.get("field_catalog", []):
                if not isinstance(f, dict):
                    continue
                f_name = f.get("name", "")
                if not f_name or any(existing.get("name") == f_name for existing in all_fields):
                    continue
                is_m = bool(f.get("measure_type") or f.get("aggregation") or str(f.get("category", "")).lower() == "measures")
                agg = f.get("aggregation") or ("SUM" if is_m else None)
                field_item = {
                    "name": f_name,
                    "label": f.get("label") or f_name,
                    "type": f.get("element_type") or ("cds.Integer" if is_m else "cds.String"),
                    "category": "measure" if is_m else "dimension",
                    "aggregation": agg,
                    "is_key": bool(f.get("is_key", False))
                }
                all_fields.append(field_item)
                if is_m:
                    measures.append({
                        "name": f_name,
                        "label": field_item["label"],
                        "aggregation": agg or "SUM",
                        "type": field_item["type"]
                    })
                else:
                    dimensions.append({
                        "name": f_name,
                        "label": field_item["label"],
                        "is_key": field_item["is_key"],
                        "type": field_item["type"]
                    })

        source_matches = resolver_client.match_dependency_sources(
            dependency_tree,
            replicated_tables,
        )
        dependency_nodes = list(_walk_dependency_nodes(dependency_tree))
        known_node_names = {
            str(node.get("name") or "").casefold()
            for node in dependency_nodes
        }

        # If replication-space matching produced no sources, identify verified leaf local tables from the dependency tree
        if not source_matches and dependency_nodes:
            for node in dependency_nodes:
                n_type = str(node.get("object_type") or "")
                n_name = str(node.get("name") or "")
                if (n_type in ("local_table", "remote_table") or not node.get("dependencies")) and n_name and n_name != view_name:
                    if not any(s.get("name") == n_name for s in source_matches):
                        source_matches.append({
                            "name": n_name,
                            "type": n_type or "local_table",
                            "lineage_path": node.get("lineage_path", n_name),
                            "matched_cds_view": n_name,
                            "replicated_from_adt": True if n_type == "local_table" else False,
                            "replication_space": target_space,
                            "description": f"Verified Data Builder leaf source: {n_name}",
                            "key_fields": [
                                f["name"] for f in node.get("field_catalog", [])
                                if isinstance(f, dict) and f.get("is_key")
                            ],
                            "table_name": n_name,
                            "cds_view_name": n_name,
                            "matched_by": "data_builder_lineage",
                            "source_origin_status": "verified"
                        })

        for source in source_matches:
            source_name = str(source.get("matched_cds_view") or "")
            if source_name and source_name.casefold() not in known_node_names:
                dependency_nodes.append({
                    "name": source_name,
                    "object_type": "local_table",
                    "status": "matched_in_source_metadata",
                    "relationship": "Exact match to configured replication-space source",
                    "dependencies": [],
                    "field_catalog": [],
                    "matched_cds_view": source_name,
                })
                known_node_names.add(source_name.casefold())
        lineage = [
            {
                "step": index,
                "name": node.get("name", ""),
                "type": node.get("object_type", "unknown"),
                "space": target_space,
                "role": node.get("relationship", "Resolved Data Builder dependency"),
                "matched_cds_view": next(
                    (
                        match["matched_cds_view"]
                        for match in source_matches
                        if match["name"].casefold()
                        == str(node.get("name", "")).casefold()
                    ),
                    None,
                ),
            }
            for index, node in enumerate(dependency_nodes, start=1)
        ]
        sources = source_matches
        lineage_analysis = {
            "status": "available" if dependency_tree.get("status") == "resolved" else "unavailable",
            "items": dependency_nodes,
            "raw_sources": source_matches,
            "message": (
                f"The live configured replication-space inventory could not be refreshed; using "
                f"{len(replicated_tables)} saved local-table records. "
                if inventory_refresh_error and replicated_tables
                else (
                    "The configured replication-space inventory could not be refreshed and no saved "
                    "inventory is available. "
                    if inventory_refresh_error
                    else ""
                )
            ) + (
                "" if source_matches else (
                    "The dependency metadata was read, but no source reference exactly "
                    "matched a configured replication-space local table."
                )
            ),
            "replication_space": os.getenv("DATASPHERE_REPLICATION_SPACE", "").strip() or None,
            "replicated_local_table_count": len(replicated_tables),
            "inventory_status": (
                "stale" if inventory_refresh_error and replicated_tables
                else "unavailable" if inventory_refresh_error
                else "refreshed"
            ),
        }
    except Exception as exc:
        logger.warning("Data Builder dependency lineage unavailable for %s: %s", view_name, exc)
        sources = []
        if not lineage_nodes:
            lineage_analysis = {
                "status": "unavailable",
                "items": [],
                "raw_sources": [],
                "message": (
                    "Could not resolve Data Builder dependencies or refresh the "
                    f"configured replication-space local-table inventory: {exc}"
                ),
            }
        else:
            lineage_analysis = {
                "status": "unavailable",
                "items": lineage_nodes,
                "raw_sources": [],
                "message": (
                    "Catalog lineage was reported, but it could not be verified against "
                    f"the configured replication-space local-table inventory: {exc}"
                ),
            }

    # Enrich all_fields, measures, dimensions with propagated calculations from dependency_tree
    tree_calcs = {
        str(c.get("name", "")).casefold(): c
        for c in dependency_tree.get("calculated_columns", [])
        if isinstance(c, dict) and c.get("name")
    }
    for f in dependency_tree.get("field_catalog", []):
        if isinstance(f, dict) and f.get("name") and f.get("is_calculated") and f.get("calculation"):
            tree_calcs[str(f["name"]).casefold()] = {
                "name": f["name"],
                "expression": f["calculation"],
                "owning_object": f.get("source_object", view_name),
                "referenced_columns": f.get("referenced_columns", []),
                "transformation_category": f.get("transformation_category", "calculated_column"),
            }

    for f in all_fields:
        c = tree_calcs.get(str(f["name"]).casefold())
        if c:
            f["is_calculated"] = True
            f["calculation"] = c.get("expression")
            f["source_object"] = c.get("owning_object")
            f["referenced_columns"] = c.get("referenced_columns", [])
            f["transformation_category"] = c.get("transformation_category")

    for m in measures:
        c = tree_calcs.get(str(m["name"]).casefold())
        if c:
            m["is_calculated"] = True
            m["calculation"] = c.get("expression")
            m["source_object"] = c.get("owning_object")
            m["referenced_columns"] = c.get("referenced_columns", [])

    for d in dimensions:
        c = tree_calcs.get(str(d["name"]).casefold())
        if c:
            d["is_calculated"] = True
            d["calculation"] = c.get("expression")
            d["source_object"] = c.get("owning_object")
            d["referenced_columns"] = c.get("referenced_columns", [])

    # 7. Metadata JSON
    object_type = _resolved_catalog_object_type(asset, dependency_tree)
    metadata_json = {
        "view_name": view_name,
        "space_id": target_space,
        "label": asset.get("label") or view_name,
        "type": object_type,
        "supports_analytical_queries": asset.get("supportsAnalyticalQueries", False),
        "has_parameters": asset.get("hasParameters", False),
        "consumption_endpoints": {
            "relational_data_url": asset.get("assetRelationalDataUrl"),
            "analytical_data_url": asset.get("assetAnalyticalDataUrl"),
            "relational_metadata_url": asset.get("assetRelationalMetadataUrl"),
            "analytical_metadata_url": asset.get("assetAnalyticalMetadataUrl")
        },
        "fields": all_fields,
        "measures": measures,
        "dimensions": dimensions,
        "sources": sources,
        "calculated_columns": dependency_tree.get("calculated_columns", []),
        "lineage_analysis": lineage_analysis,
        "impact_analysis": impact_analysis,
        "lineage_path": [node["name"] for node in lineage]
    }

    response_payload = {
        "view_name": view_name,
        "space_id": target_space,
        "label": asset.get("label") or view_name,
        "type": object_type,
        "supports_analytical_queries": asset.get("supportsAnalyticalQueries", False),
        "has_parameters": asset.get("hasParameters", False),
        "fields": all_fields,
        "measures": measures,
        "dimensions": dimensions,
        "sources": sources,
        "lineage_chain": lineage,
        "lineage_analysis": lineage_analysis,
        "impact_analysis": impact_analysis,
        "metadata_json": metadata_json
    }
    if cache is not None:
        cached_payload = cache.get_model_metadata(target_space, view_name)
        cache.upsert_model_metadata(target_space, view_name, {
            **(cached_payload.get("metadata", {}) if cached_payload else {}),
            "response": response_payload,
            "response_synced_at": time.time(),
            "dependency_tree": dependency_tree,
            "field_catalog": dependency_tree.get("field_catalog", []) if isinstance(dependency_tree, dict) else [],
            "calculated_columns": dependency_tree.get("calculated_columns", []) if isinstance(dependency_tree, dict) else [],
            "lineage_path": [
                str(node.get("name")) for node in _walk_dependency_nodes(dependency_tree)
                if node.get("name")
            ] if isinstance(dependency_tree, dict) else [],
            "cache_source": "datasphere_cli_and_consumption_metadata",
        })
    return response_payload


@app.get("/api/cache/models")
async def get_cached_model_metadata(space_id: Optional[str] = None):
    """Inspect cache coverage without returning stored definitions or source payloads."""
    target_space = space_id.strip() if space_id else DATASPHERE_SPACE_ID
    client = DatasphereResolverClient(space=target_space)
    if client.cache is None:
        return {"space_id": target_space, "cached_models": 0, "models": []}
    entries = client.cache.list_cached_model_metadata(target_space)
    now = time.time()
    models = []
    for entry in entries:
        metadata = entry.get("metadata", {})
        tree = metadata.get("dependency_tree", {}) if isinstance(metadata, dict) else {}
        response = metadata.get("response") if isinstance(metadata, dict) else None
        models.append({
            "model_name": entry.get("model_name"),
            "last_synced": entry.get("last_synced"),
            "age_seconds": max(0, int(now - float(entry.get("last_synced", now)))),
            "has_dependency_tree": isinstance(tree, dict) and bool(tree),
            "lineage_nodes": sum(1 for _ in _walk_dependency_nodes(tree)) if isinstance(tree, dict) and tree else 0,
            "field_count": len(metadata.get("field_catalog", [])) if isinstance(metadata, dict) else 0,
            "has_full_details": isinstance(response, dict),
        })
    return {"space_id": target_space, "cached_models": len(models), "models": models}


@app.post("/api/cache/models/refresh")
async def refresh_model_metadata_cache(space_id: Optional[str] = None):
    """Refresh metadata and lineage for every analytical model in a space."""
    target_space = space_id.strip() if space_id else DATASPHERE_SPACE_ID
    if not target_space:
        raise HTTPException(status_code=400, detail="Datasphere space is not configured.")
    client = DatasphereResolverClient(space=target_space)
    if not client.cli_available:
        raise HTTPException(status_code=503, detail="Datasphere CLI is required to refresh model lineage metadata.")
    try:
        model_items = await asyncio.to_thread(client.list_analytic_models, target_space, 500)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Could not list Datasphere analytical models: {exc}") from exc
    model_names = [
        str(item.get("technicalName") or item.get("name") or "").strip()
        for item in model_items
        if str(item.get("technicalName") or item.get("name") or "").strip()
    ]
    _schedule_model_metadata_warm(target_space, model_names, force_refresh=True)
    return {
        "status": "refresh_scheduled",
        "space_id": target_space,
        "model_count": len(model_names),
        "cache_status_url": f"/api/cache/models?space_id={target_space}",
    }

# ---------------------------------------------------------------------------
# Local SQLite Chat Session Management API Endpoints
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# CDS Source Fields & New Fields Comparison Endpoint
# ---------------------------------------------------------------------------
@app.get("/api/views/{view_name}/cds-fields-comparison")
async def get_cds_fields_comparison(
    view_name: str,
    space_id: Optional[str] = None,
    source_cds_view: Optional[str] = None
):
    """
    Compares the fields of the selected Datasphere view with the raw S/4HANA CDS view:
    1. Discovers calculations across upstream dependencies and populates verified formulas.
    2. Identifies matching source fields and new calculated fields with lineage evidence.
    """
    target_space = space_id or DATASPHERE_SPACE_ID
    base_host = get_tenant_base_host(DATASPHERE_API_URL)
    bearer_token = await get_datasphere_token()

    # 1. Fetch live dependency tree with bottom-up calculation propagation
    resolver_client = DatasphereResolverClient(space=target_space)
    resolver = DatasphereDependencyResolver(client=resolver_client, space=target_space, silent=True)
    dependency_tree = {}
    try:
        dependency_tree = await asyncio.to_thread(resolver.resolve, view_name)
    except Exception as exc:
        logger.warning("Dependency tree resolution in fields comparison for %s: %s", view_name, exc)

    propagated_calcs = {
        str(c.get("name", "")).casefold(): c
        for c in dependency_tree.get("calculated_columns", [])
        if isinstance(c, dict) and c.get("name")
    }
    for f in dependency_tree.get("field_catalog", []):
        if isinstance(f, dict) and f.get("name") and f.get("is_calculated") and f.get("calculation"):
            propagated_calcs[str(f["name"]).casefold()] = {
                "name": f["name"],
                "expression": f["calculation"],
                "owning_object": f.get("source_object", view_name),
                "referenced_columns": f.get("referenced_columns", []),
                "transformation_category": f.get("transformation_category", "calculated_column"),
            }

    # 2. Fetch Datasphere metadata
    edmx = await fetch_view_edmx_schema(base_host, target_space, view_name, bearer_token)
    ds_measures = edmx.get("measures", [])
    ds_dimensions = edmx.get("dimensions", [])

    all_ds_fields = []
    for m in ds_measures:
        all_ds_fields.append({"name": m["name"], "label": m.get("label") or m["name"], "category": "measure", "type": m.get("type", "cds.Decimal")})
    for d in ds_dimensions:
        all_ds_fields.append({"name": d["name"], "label": d.get("label") or d["name"], "category": "dimension", "type": d.get("type", "cds.String")})

    if not all_ds_fields:
        cols = await probe_view_columns(base_host, target_space, view_name, bearer_token)
        for c in cols:
            all_ds_fields.append({"name": c, "label": c, "category": "unknown", "type": ""})

    if not all_ds_fields and dependency_tree.get("field_catalog"):
        for f in dependency_tree.get("field_catalog", []):
            if isinstance(f, dict) and f.get("name"):
                is_m = bool(f.get("measure_type") or f.get("aggregation"))
                all_ds_fields.append({
                    "name": f["name"],
                    "label": f.get("label") or f["name"],
                    "category": "measure" if is_m else "dimension",
                    "type": f.get("element_type") or "cds.String",
                })

    # 3. Determine target CDS view (cleaning user inputs)
    cds_view = source_cds_view.strip() if source_cds_view else None
    if cds_view and " " in cds_view:
        cds_view = re.sub(r"\s+", "", cds_view)

    if not cds_view and dependency_tree:
        for node in _walk_dependency_nodes(dependency_tree):
            n_type = str(node.get("object_type") or "")
            n_name = str(node.get("name") or "")
            if (n_type in ("local_table", "remote_table") or not node.get("dependencies")) and n_name and n_name != view_name:
                cds_view = n_name
                break

    if not cds_view:
        new_fields = []
        for field in all_ds_fields:
            c = propagated_calcs.get(str(field["name"]).casefold())
            new_fields.append({
                **field,
                "suggested_formula": c.get("expression", "") if c else "",
                "source_object": c.get("owning_object") if c else None,
                "referenced_columns": c.get("referenced_columns", []) if c else [],
                "transformation_category": c.get("transformation_category") if c else None,
                "is_calculated": bool(c),
            })
        return {
            "view_name": view_name,
            "source_cds_view": None,
            "matched_source_fields": [],
            "new_fields": new_fields,
            "raw_cds_columns_count": 0,
            "raw_cds_columns": [],
            "lineage_status": "unavailable"
        }

    # 4. Discover columns from S/4HANA ADT
    raw_cols = []
    if DataRetriever and USE_SAP_ADT:
        try:
            r = DataRetriever(max_row_limit=500)
            raw_cols_res = r.discover_columns(cds_view)
            raw_cols = [c.get("name", "") if isinstance(c, dict) else str(c) for c in raw_cols_res]
            r.close()
        except Exception as e:
            logger.warning(f"ADT column discovery notice for {cds_view}: {e}")

    s4_col_set = {c.upper(): c for c in raw_cols}

    matched_source_fields = []
    new_fields = []

    for f in all_ds_fields:
        f_name = f["name"]
        f_upper = f_name.upper()
        if f_upper in s4_col_set:
            matched_source_fields.append({
                "name": f_name,
                "source_col": s4_col_set[f_upper],
                "category": f["category"],
                "type": f["type"]
            })
        else:
            calc = propagated_calcs.get(f_name.casefold())
            new_fields.append({
                "name": f_name,
                "category": f["category"],
                "type": f["type"],
                "suggested_formula": calc.get("expression", "") if calc else "",
                "source_object": calc.get("owning_object") if calc else None,
                "referenced_columns": calc.get("referenced_columns", []) if calc else [],
                "transformation_category": calc.get("transformation_category") if calc else None,
                "is_calculated": bool(calc),
            })

    return {
        "view_name": view_name,
        "source_cds_view": cds_view,
        "matched_source_fields": matched_source_fields,
        "new_fields": new_fields,
        "raw_cds_columns_count": len(raw_cols),
        "raw_cds_columns": raw_cols[:30]
    }


@app.get("/api/sessions")
async def get_sessions():
    sessions = get_all_chat_sessions_from_db()
    return {"sessions": sessions}

@app.get("/api/sessions/{session_id}")
async def get_session(session_id: str):
    sessions = get_all_chat_sessions_from_db()
    for s in sessions:
        if s["id"] == session_id:
            return {"session": s}
    raise HTTPException(status_code=404, detail="Session not found")

@app.post("/api/sessions")
async def save_session(req: SaveSessionRequest):
    save_chat_session_to_db(req.session_id, req.title, req.space_id or DATASPHERE_SPACE_ID, req.views or [], req.messages)
    return {"status": "success", "session_id": req.session_id}

@app.put("/api/sessions/{session_id}/rename")
async def rename_session(session_id: str, req: RenameSessionRequest):
    try:
        rename_chat_session_in_db(session_id, req.title.strip())
        return {"status": "success", "session_id": session_id, "title": req.title.strip()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/sessions/{session_id}")
async def delete_session(session_id: str):
    delete_chat_session_from_db(session_id)
    if session_id in conversation_sessions:
        del conversation_sessions[session_id]
    return {"status": "deleted", "session_id": session_id}

@app.delete("/api/sessions")
async def clear_all_sessions():
    clear_all_chat_sessions_from_db()
    conversation_sessions.clear()
    return {"status": "cleared"}

# ---------------------------------------------------------------------------
# RAG Knowledge Store API Endpoints
# ---------------------------------------------------------------------------
@app.get("/api/rag/stats")
async def get_rag_statistics():
    return rag_service.get_rag_stats()

@app.post("/api/rag/search")
async def rag_unified_search(req: RAGSearchRequest):
    gem_key = GEMINI_API_KEY or (API_KEY if API_KEY and not API_KEY.startswith("sk-") else "")
    oa_key = OPENAI_API_KEY or (API_KEY if API_KEY and API_KEY.startswith("sk-") else "")
    views = await rag_service.retrieve_relevant_views_rag(req.query, space_id=req.space_id, top_k=req.top_k or 3, gemini_key=gem_key, openai_key=oa_key)
    golden = await rag_service.retrieve_golden_sql_examples(req.query, space_id=req.space_id, top_k=req.top_k or 2, gemini_key=gem_key, openai_key=oa_key)
    glossary = await rag_service.retrieve_glossary_terms(req.query, top_k=req.top_k or 2, gemini_key=gem_key, openai_key=oa_key)
    docs = await rag_service.retrieve_context_documents(req.query, top_k=req.top_k or 2, gemini_key=gem_key, openai_key=oa_key)
    return {
        "query": req.query,
        "views": [{"view_name": v[0], "score": round(v[1], 4), "columns": v[2]} for v in views],
        "golden_sql": golden,
        "glossary": glossary,
        "documents": docs
    }

@app.post("/api/rag/golden-sql")
async def add_golden_query(req: GoldenSQLRequest):
    gem_key = GEMINI_API_KEY or (API_KEY if API_KEY and not API_KEY.startswith("sk-") else "")
    oa_key = OPENAI_API_KEY or (API_KEY if API_KEY and API_KEY.startswith("sk-") else "")
    q_id = await rag_service.add_golden_sql_query(
        natural_query=req.natural_query,
        sql_query=req.sql_query,
        space_id=req.space_id or DATASPHERE_SPACE_ID,
        target_views=req.target_views,
        odata_filter=req.odata_filter,
        explanation=req.explanation or "",
        gemini_key=gem_key,
        openai_key=oa_key
    )
    return {"status": "saved", "id": q_id}

@app.post("/api/rag/glossary")
async def add_glossary_definition(req: GlossaryRequest):
    gem_key = GEMINI_API_KEY or (API_KEY if API_KEY and not API_KEY.startswith("sk-") else "")
    oa_key = OPENAI_API_KEY or (API_KEY if API_KEY and API_KEY.startswith("sk-") else "")
    t_id = await rag_service.add_glossary_term(
        term=req.term,
        definition=req.definition,
        formula=req.formula,
        synonyms=req.synonyms,
        gemini_key=gem_key,
        openai_key=oa_key
    )
    return {"status": "saved", "id": t_id}

@app.post("/api/rag/documents")
async def add_context_doc(req: DocumentRequest):
    gem_key = GEMINI_API_KEY or (API_KEY if API_KEY and not API_KEY.startswith("sk-") else "")
    oa_key = OPENAI_API_KEY or (API_KEY if API_KEY and API_KEY.startswith("sk-") else "")
    d_id = await rag_service.add_context_document(
        title=req.title,
        content=req.content,
        category=req.category or "general",
        gemini_key=gem_key,
        openai_key=oa_key
    )
    return {"status": "saved", "id": d_id}

@app.post("/api/rag/index-views")
async def index_all_space_views(space_id: Optional[str] = None):
    target_space = space_id or DATASPHERE_SPACE_ID
    base_host = get_tenant_base_host(DATASPHERE_API_URL)
    bearer_token = await get_datasphere_token()
    discovered = await discover_space_views(base_host, target_space, bearer_token)
    
    gem_key = GEMINI_API_KEY or (API_KEY if API_KEY and not API_KEY.startswith("sk-") else "")
    oa_key = OPENAI_API_KEY or (API_KEY if API_KEY and API_KEY.startswith("sk-") else "")
    
    indexed_count = 0
    for v in discovered:
        cols = await probe_view_columns(base_host, target_space, v, bearer_token)
        if cols:
            ok = await rag_service.index_view_metadata(target_space, v, cols, gemini_key=gem_key, openai_key=oa_key)
            if ok:
                indexed_count += 1
                
    return {
        "status": "completed",
        "space_id": target_space,
        "total_discovered": len(discovered),
        "indexed_count": indexed_count
    }

# ---------------------------------------------------------------------------
# Core Analytics Query API Endpoint
# ---------------------------------------------------------------------------
@app.post("/api/analytics", response_model=QueryResponse)
async def execute_analytics(request: QueryRequest):
    return await run_analytics_query(request)

# ---------------------------------------------------------------------------
# Explicit S/4HANA CDS Staging API
# ---------------------------------------------------------------------------
@app.get("/api/cds-source-data/{source_name}")
async def fetch_cds_source_data(source_name: str):
    """Run a plain SELECT * against one CDS entity through the configured ADT connection."""
    if not USE_SAP_ADT:
        raise HTTPException(status_code=503, detail="S/4HANA ADT is not configured.")
    try:
        result = await asyncio.to_thread(_fetch_cds_source_data, source_name)
        return {
            "source": source_name,
            "columns": result.get("columns", []),
            "rows": result.get("rows", []),
            "row_count": len(result.get("rows", [])),
            "total_rows": int(result.get("total_rows", len(result.get("rows", [])))),
            "execution_time": result.get("execution_time"),
            "row_limit": result.get("total_rows", len(result.get("rows", []))),
        }
    except Exception as exc:
        logger.exception("Direct ADT CDS read failed for %s", source_name)
        raise HTTPException(status_code=502, detail=str(exc)) from exc


def _fetch_cds_source_data(source_name: str) -> Dict[str, Any]:
    # Discover ADT's total with one ordinary SELECT * request, then ask the
    # same ADT endpoint for that many rows. This sends the CDS result directly
    # without key inference, key validation, or keyset paging.
    with DataRetriever(default_row_limit=1, max_row_limit=1_000_000) as retriever:
        first_page = retriever.fetch_view(view_name=source_name, row_limit=1)
        total_rows = int(first_page.get("total_rows", len(first_page.get("rows", []))))
        if total_rows <= len(first_page.get("rows", [])):
            return first_page
        if total_rows > 1_000_000:
            raise ValueError(f"ADT reports {total_rows} rows, above the 1,000,000-row direct response limit.")
        full_result = retriever.fetch_view(view_name=source_name, row_limit=max(total_rows, 1))
        rows = full_result.get("rows", [])
        if len(rows) < total_rows:
            raise RuntimeError(f"ADT returned {len(rows)} of {total_rows} rows for the direct CDS read.")
        return full_result


@app.post("/api/cds-staging")
async def stage_selected_cds_sources(request: CDSStageRequest):
    """Fetch complete selected lineage sources through ADT and store them locally."""
    target_space = (request.space_id or DATASPHERE_SPACE_ID).strip()
    model_name = request.view_name.strip()
    if not target_space:
        raise HTTPException(status_code=400, detail="A Datasphere space is required.")
    if not model_name:
        raise HTTPException(status_code=400, detail="A Datasphere model name is required.")
    if not request.selected_sources:
        raise HTTPException(status_code=400, detail="Select at least one CDS source to stage.")
    if not USE_SAP_ADT:
        raise HTTPException(status_code=503, detail="S/4HANA ADT is not configured.")

    client = DatasphereResolverClient(space=target_space)
    if client.cache is None:
        raise HTTPException(status_code=503, detail="Datasphere lineage cache is unavailable for source verification.")

    try:
        # Source discovery already refreshed this inventory when the user
        # selected the model. Reuse the cached snapshot here instead of making
        # a second network round trip before the ADT read.
        replicated_tables = client.cache.list_replication_local_tables()
        resolver = DatasphereDependencyResolver(client=client, space=target_space, silent=True)
        cached_entry = client.cache.get_model_metadata(target_space, model_name)
        cached_metadata = cached_entry.get("metadata", {}) if cached_entry else {}
        cached_tree = cached_metadata.get("dependency_tree") if isinstance(cached_metadata, dict) else None
        dependency_tree = cached_tree if isinstance(cached_tree, dict) else await asyncio.to_thread(resolver.resolve, model_name)
        verified_sources = client.match_dependency_sources(dependency_tree, replicated_tables)

        # Match the existing lineage view's verified-leaf fallback when no
        # replication-inventory match is available.
        if not verified_sources:
            for node in _walk_dependency_nodes(dependency_tree):
                node_type = str(node.get("object_type") or "")
                node_name = str(node.get("name") or "")
                if (
                    node_name
                    and node_name.casefold() != model_name.casefold()
                    and (node_type in {"local_table", "remote_table"} or not node.get("dependencies"))
                ):
                    verified_sources.append({
                        "matched_cds_view": node_name,
                        "key_fields": [
                            str(field.get("name"))
                            for field in node.get("field_catalog", [])
                            if isinstance(field, dict) and field.get("name") and field.get("is_key")
                        ],
                    })
    except Exception as exc:
        logger.exception("Could not verify Datasphere lineage before CDS staging")
        raise HTTPException(
            status_code=502,
            detail=f"Could not verify the selected CDS sources against Datasphere lineage: {exc}",
        ) from exc

    source_index = {
        str(item.get("matched_cds_view") or "").casefold(): item
        for item in verified_sources
        if item.get("matched_cds_view")
    }
    unique_requests = {}
    for item in request.selected_sources:
        clean_name = item.cds_view.strip()
        if clean_name:
            unique_requests.setdefault(clean_name.casefold(), item)
    selected_requests = list(unique_requests.values())
    selected_names = [item.cds_view.strip() for item in selected_requests]
    if any(not name for name in selected_names):
        raise HTTPException(status_code=400, detail="CDS source names must not be empty.")
    unverified = [name for name in selected_names if name.casefold() not in source_index]
    if unverified:
        raise HTTPException(
            status_code=400,
            detail="Selected CDS source(s) are not present in the resolved model lineage: "
            + ", ".join(unverified),
        )

    store = CdsStagingStore()
    run_id = uuid.uuid4().hex
    dependency_nodes = _walk_dependency_nodes(dependency_tree)
    staged_sources = []
    failed_sources = []
    for selected in selected_requests:
        source_name = selected.cds_view.strip()
        lineage_metadata = source_index[source_name.casefold()]
        # The raw CDS transfer is a direct ADT SELECT * response. Lineage keys
        # may describe the model grain, but they are not used to extract rows.
        key_fields: List[str] = []
        source_metadata = []
        for node in dependency_nodes:
            definition = node.get("definition") if isinstance(node.get("definition"), dict) else {}
            candidates = [str(node.get("name") or "")]
            candidates.extend(str(item) for item in definition.get("source_names", []) if isinstance(item, str))
            candidates.extend(client._source_name_candidates(definition))
            if any(candidate.rsplit(".", 1)[-1].casefold() == source_name.casefold() for candidate in candidates):
                source_metadata.append({
                    "name": node.get("name"), "type": node.get("object_type"),
                    "field_catalog": node.get("field_catalog", []),
                    "definition": definition,
                })
        fingerprint_payload = {
            "source": source_name.casefold(),
            "source_metadata": source_metadata,
            "definition_digest": lineage_metadata.get("definition_digest"),
        }
        metadata_fingerprint = hashlib.sha256(
            json.dumps(fingerprint_payload, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()

        def extract(on_batch, *, cds_name=source_name):
            started = time.perf_counter()
            result = _fetch_cds_source_data(cds_name)
            rows = result.get("rows", [])
            total_rows = int(result.get("total_rows", len(rows)))
            if rows:
                on_batch(rows, {
                    "batch": 1, "rows": len(rows),
                    "extracted_so_far": len(rows), "total_rows": total_rows,
                })
            complete = len(rows) == total_rows
            return {
                "entity": cds_name, "rows": [], "streamed": True,
                "row_count": len(rows), "total_rows_reported": total_rows,
                "batches": 1 if rows else 0, "batch_size": len(rows),
                "key_fields": [],
                "fields": [item.get("name") for item in result.get("columns", []) if item.get("name")],
                "column_types": {item.get("name"): item.get("type") for item in result.get("columns", []) if item.get("name")},
                "complete": complete,
                "issues": [] if complete else [f"ADT returned {len(rows)} of {total_rows} rows."],
                "elapsed_seconds": time.perf_counter() - started,
                "consistent_snapshot": True,
            }

        try:
            stage_started = time.perf_counter()
            result = await asyncio.to_thread(
                store.stage_source,
                space_id=target_space,
                source_name=source_name,
                extract=extract,
                metadata_fingerprint=metadata_fingerprint,
                validation_run_id=run_id,
                force_refresh=True,
            )
            result["extraction_duration_seconds"] = round(time.perf_counter() - stage_started, 3)
            staged_sources.append(result)
        except Exception as exc:
            logger.exception("Failed to stage selected CDS source %s", source_name)
            failed_sources.append({
                "source": source_name,
                "status": "STAGING_FAILED",
                "error": str(exc),
                "integrity": getattr(exc, "summary", None),
            })

    if staged_sources and failed_sources:
        status = "PARTIAL_FAILURE"
    elif failed_sources:
        status = "FAILED"
    else:
        status = "STAGED_COMPLETE"
    return {
        "status": status,
        "space_id": target_space,
        "view_name": model_name,
        "method": "ADT direct SELECT * with total-row verification; no keyset extraction",
        "staged_sources": staged_sources,
        "failed_sources": failed_sources,
    }

@app.get("/api/cds-staging")
async def list_staged_cds_sources(space_id: Optional[str] = None):
    """List complete CDS snapshots available in the local SQLite staging database."""
    return {"staged_sources": CdsStagingStore().list_staged_sources(space_id)}

@app.post("/api/cds-staging/cleanup")
async def cleanup_expired_cds_snapshots():
    """Remove expired snapshots; an active SQLite reader holds its own read lock."""
    return CdsStagingStore().cleanup_expired()


def _cds_csv_export_path(export_id: str) -> Path:
    if not re.fullmatch(r"[a-f0-9]{32}", export_id):
        raise HTTPException(status_code=404, detail="CSV export was not found.")
    return Path(tempfile.gettempdir()) / "datasphere-validation-csv" / f"{export_id}.csv"


@app.get("/api/cds-staging/source-csv")
async def download_staged_source_csv(space_id: str, source_name: str):
    """Download the complete staged ADT source as an Excel-compatible CSV."""
    export_id = uuid.uuid4().hex
    path = _cds_csv_export_path(export_id)
    try:
        await asyncio.to_thread(
            CdsStagingStore().write_source_csv,
            space_id=space_id, source_name=source_name, output_path=path,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    filename = f"{re.sub(r'[^A-Za-z0-9_-]+', '_', source_name)}_source.csv"
    return FileResponse(path, media_type="text/csv", filename=filename)


def _csn_to_sql_expression(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return " ".join(part for part in (_csn_to_sql_expression(item) for item in value) if part)
    if not isinstance(value, dict):
        return "" if value is None else str(value)
    if isinstance(value.get("ref"), list):
        return ".".join(str(part) for part in value["ref"])
    if "val" in value:
        literal = value["val"]
        return "'" + str(literal).replace("'", "''") + "'" if isinstance(literal, str) else ("NULL" if literal is None else str(literal))
    if "xpr" in value:
        return _csn_to_sql_expression(value["xpr"])
    return ""


def _datasphere_csn_joins(tree: dict[str, Any]) -> list[dict[str, Any]]:
    """Read explicit CSN join nodes from Datasphere view definitions."""
    found: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            if str(value.get("join") or "").casefold() in {"inner", "left", "right", "full", "left outer", "inner join", "left outer join"}:
                args = value.get("args") or []
                on = value.get("on")
                if len(args) == 2 and on:
                    sides = []
                    for arg in args:
                        ref = arg.get("ref") if isinstance(arg, dict) else None
                        if not isinstance(ref, list) or not ref:
                            sides = []
                            break
                        sides.append({"source": str(ref[-1]), "as": str(arg.get("as") or ref[-1])})
                    condition = _csn_to_sql_expression(on)
                    if sides and condition:
                        signature = (sides[0]["as"].casefold(), sides[1]["as"].casefold(), condition.casefold())
                        if signature not in seen:
                            seen.add(signature)
                            found.append({"left": sides[0], "right": sides[1], "join_type": value.get("join"), "condition": condition})
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(tree)
    return found


@app.post("/api/cds-staging/joined-csv")
async def create_joined_cds_csv(request: dict[str, Any]):
    """Join all freshly staged model sources using the Datasphere CSN join condition."""
    space_id = str(request.get("space_id") or DATASPHERE_SPACE_ID).strip()
    model_name = str(request.get("view_name") or "").strip()
    source_names = list(dict.fromkeys(str(name).strip() for name in (request.get("source_names") or []) if str(name).strip()))
    if not space_id or not model_name or not source_names:
        raise HTTPException(status_code=400, detail="A model and its staged CDS source names are required.")

    client = DatasphereResolverClient(space=space_id)
    cached_entry = client.cache.get_model_metadata(space_id, model_name) if client.cache else None
    cached_metadata = cached_entry.get("metadata", {}) if cached_entry else {}
    tree = cached_metadata.get("dependency_tree") if isinstance(cached_metadata, dict) else None
    if not isinstance(tree, dict) or not tree:
        resolver = DatasphereDependencyResolver(client=client, space=space_id, silent=True)
        tree = await asyncio.to_thread(resolver.resolve, model_name, object_type="analytic_model")
    if not isinstance(tree, dict) or tree.get("status") != "resolved":
        raise HTTPException(status_code=422, detail=f"Could not resolve Datasphere source joins for {model_name}.")

    transformation_ir = _build_transformation_ir(tree)
    joins = transformation_ir.get("joins", [])
    if len(source_names) == 1:
        # No join is needed for a model with one resolved CDS input. Ignore
        # any nested join-shaped metadata that may belong to upstream objects.
        joins = []
    elif not joins:
        joins = _datasphere_csn_joins(tree)
    if len(source_names) > 1 and not joins:
        raise HTTPException(status_code=422, detail="The model metadata has multiple sources but no explicit Datasphere join condition.")

    store = CdsStagingStore()
    staged_by_name = {str(item.get("source", "")).casefold(): item for item in store.list_staged_sources(space_id)}
    snapshots = []
    for name in source_names:
        item = staged_by_name.get(name.casefold())
        if not item or not item.get("complete") or not item.get("fresh"):
            raise HTTPException(status_code=409, detail=f"CDS source {name} is not completely staged yet.")
        snapshots.append(item)

    source_lookup = {str(item["source"]).casefold(): str(item["source"]) for item in snapshots}
    normalized_joins = []
    for join in joins:
        if not isinstance(join, dict):
            continue
        normalized = dict(join)
        for side_name in ("left", "right"):
            side = normalized.get(side_name)
            if not isinstance(side, dict):
                continue
            label = str(side.get("source") or side.get("name") or "").strip()
            matched = [entity for key, entity in source_lookup.items() if label.casefold() == key or label.casefold().endswith(key)]
            if len(matched) != 1:
                raise HTTPException(status_code=422, detail=f"Datasphere join input {label!r} does not resolve to one staged CDS source.")
            side["source"] = matched[0]
            normalized[side_name] = side
        normalized_joins.append(normalized)
    if len(snapshots) > 1 and not normalized_joins:
        raise HTTPException(status_code=422, detail="No verified join connects the staged CDS sources.")

    table_by_entity = {str(item["source"]).casefold(): item for item in snapshots}
    alias_to_entity: dict[str, str] = {}
    sql_alias_by_entity: dict[str, str] = {}
    join_clauses: list[str] = []
    first = normalized_joins[0] if len(snapshots) > 1 and normalized_joins else None
    if first:
        first_side = first["left"]
        root_entity = str(first_side["source"])
        root_alias = str(first_side.get("as") or root_entity)
        alias_to_entity[root_alias.casefold()] = root_entity
        sql_alias_by_entity[root_entity.casefold()] = root_alias
        root_table = store._quote_identifier(table_by_entity[root_entity.casefold()]["table_name"])
        from_sql = f"{root_table} AS {store._quote_identifier(root_alias)}"
        for join in normalized_joins:
            right = join["right"]
            entity = str(right["source"])
            alias = str(right.get("as") or entity)
            alias_to_entity[alias.casefold()] = entity
            sql_alias_by_entity[entity.casefold()] = alias
            join_type = str(join.get("join_type") or join.get("type") or "inner").casefold()
            sql_join = "LEFT JOIN" if "left" in join_type else "INNER JOIN"
            condition = str(join.get("condition") or join.get("on") or "")
            if isinstance(join.get("on"), (list, dict)):
                condition = _csn_to_sql_expression(join["on"])

            def qualify_ref(match: re.Match[str]) -> str:
                qualifier = match.group("alias")
                owner = alias_to_entity.get(qualifier.casefold())
                if owner is None:
                    raise ValueError(f"Join qualifier {qualifier!r} is not present in the Datasphere join inputs.")
                column_map = {str(column).casefold(): str(column) for column in table_by_entity[owner.casefold()].get("columns", [])}
                column = column_map.get(match.group("field").casefold())
                if column is None:
                    raise ValueError(f"Join field {match.group('field')!r} is missing from staged source {owner}.")
                sql_alias = sql_alias_by_entity[owner.casefold()]
                return f"{store._quote_identifier(sql_alias)}.{store._quote_identifier(column)}"

            reference = re.compile(r"(?<![A-Za-z0-9_])(?P<alias>[A-Za-z_][A-Za-z0-9_]*)[.~][\"`]?(?P<field>[A-Za-z_][A-Za-z0-9_]*)[\"`]?(?![A-Za-z0-9_])")
            translated_condition = reference.sub(qualify_ref, condition)
            if translated_condition == condition:
                raise HTTPException(status_code=422, detail=f"Could not translate Datasphere join condition: {condition}")
            table = store._quote_identifier(table_by_entity[entity.casefold()]["table_name"])
            from_sql += f" {sql_join} {table} AS {store._quote_identifier(alias)} ON {translated_condition}"
            join_clauses.append(f"{sql_join} {entity} ON {condition}")
    elif len(snapshots) == 1:
        # A single staged source is already the complete input dataset. Do not
        # inspect or apply model join metadata in this case; project directly
        # from that source and leave join_conditions empty.
        entity = str(snapshots[0]["source"])
        alias = entity
        alias_to_entity[alias.casefold()] = entity
        sql_alias_by_entity[entity.casefold()] = alias
        table = store._quote_identifier(table_by_entity[entity.casefold()]["table_name"])
        from_sql = f"{table} AS {store._quote_identifier(alias)}"

    # Keep only the selected Datasphere view's final projection. Resolve each
    # output through its projection ref first, then fall back to a unique
    # source field. Calculated outputs remain explicit NULL placeholders for
    # the following calculation step.
    requested_projection = request.get("projection_fields")
    if not isinstance(requested_projection, list) or not requested_projection:
        requested_projection = [str(item.get("name")) for item in tree.get("field_catalog", []) if isinstance(item, dict) and item.get("name")]
    projection_metadata = {
        str(item.get("name") or "").casefold(): item
        for item in requested_projection if isinstance(item, dict) and item.get("name")
    }
    requested_projection = list(dict.fromkeys(
        str(item.get("name") if isinstance(item, dict) else item).strip()
        for item in requested_projection
        if str(item.get("name") if isinstance(item, dict) else item).strip()
    ))
    projection_by_output = {
        str(item.get("output_field") or "").casefold(): item
        for item in transformation_ir.get("projections", [])
        if isinstance(item, dict) and item.get("output_field")
    }
    calculations = {
        str(item.get("name") or "").casefold()
        for item in (tree.get("calculated_columns", []) or []) + (transformation_ir.get("calculations", []) or []) + (transformation_ir.get("formulas", []) or [])
        if isinstance(item, dict) and item.get("name")
    }
    calculations.update(
        name for name, item in projection_metadata.items()
        if item.get("calculation") or item.get("expression") or item.get("is_calculated")
    )
    select_parts: list[str] = []
    pending_calculated_fields: list[str] = []
    missing_calculation_metadata: list[str] = []
    field_mappings: list[dict[str, str]] = []
    for output_field in requested_projection:
        projection = projection_by_output.get(output_field.casefold(), {})
        source_field = str(projection.get("source_field") or "")
        source_ref = projection.get("source_ref") if isinstance(projection.get("source_ref"), list) else []
        owners: list[str] = []
        for reference_part in source_ref[:-1]:
            owner = alias_to_entity.get(str(reference_part).casefold())
            if owner is None:
                matching_sources = [str(item["source"]) for item in snapshots if str(reference_part).casefold().endswith(str(item["source"]).casefold())]
                owner = matching_sources[0] if len(matching_sources) == 1 else None
            if owner:
                owners.append(owner)
        if not source_field and source_ref:
            source_field = str(source_ref[-1])
        if source_field:
            owners = list(dict.fromkeys(owner for owner in owners if source_field.casefold() in {str(column).casefold() for column in table_by_entity[owner.casefold()].get("columns", [])}))
        # Projection metadata can name the output differently from its source
        # field, and wrapper aliases are not always present in the lineage
        # alias map. Search every staged source by the metadata source field
        # before falling back to an exact output-name match.
        if not owners and source_field:
            owners = [
                str(item["source"]) for item in snapshots
                if source_field.casefold() in {str(column).casefold() for column in item.get("columns", [])}
            ]
        if not owners:
            owners = [str(item["source"]) for item in snapshots if output_field.casefold() in {str(column).casefold() for column in item.get("columns", [])}]
            if owners:
                source_field = output_field
        if len(owners) > 1:
            root_owner = str(normalized_joins[0]["left"]["source"]) if normalized_joins else ""
            owners = [root_owner] if root_owner in owners else owners[:1]
        if owners:
            owner = owners[0]
            column_map = {str(column).casefold(): str(column) for column in table_by_entity[owner.casefold()].get("columns", [])}
            column = column_map.get(source_field.casefold()) or column_map.get(output_field.casefold())
            if column:
                sql_alias = sql_alias_by_entity[owner.casefold()]
                select_parts.append(f"{store._quote_identifier(sql_alias)}.{store._quote_identifier(column)} AS {store._quote_identifier(output_field)}")
                field_mappings.append({"output_field": output_field, "source": owner, "source_field": column, "kind": "source"})
                continue
        field_metadata = projection_metadata.get(output_field.casefold(), {})
        if output_field.casefold() in calculations or str(field_metadata.get("category") or "").casefold() == "measure":
            pending_calculated_fields.append(output_field)
            if not (field_metadata.get("calculation") or field_metadata.get("expression")) and output_field.casefold() not in calculations:
                missing_calculation_metadata.append(output_field)
            select_parts.append(f"NULL AS {store._quote_identifier(output_field)}")
            field_mappings.append({"output_field": output_field, "source": "Datasphere calculation", "source_field": output_field, "kind": "calculated"})
        else:
            raise HTTPException(status_code=422, detail=f"Final Datasphere field {output_field!r} cannot be mapped to a staged source or calculated column.")
    if not select_parts:
        raise HTTPException(status_code=422, detail="The selected Datasphere view has no final fields to project.")

    export_id = uuid.uuid4().hex
    path = _cds_csv_export_path(export_id)
    db = store._connect()
    allowed_tables = {str(item["table_name"]).casefold() for item in snapshots}
    db.set_authorizer(lambda action, arg1, _arg2, _database, _trigger:
        sqlite3.SQLITE_OK if action in (sqlite3.SQLITE_SELECT, sqlite3.SQLITE_RECURSIVE)
        or (action == sqlite3.SQLITE_READ and str(arg1).casefold() in allowed_tables) else sqlite3.SQLITE_DENY)
    try:
        cursor = db.execute(f"SELECT {', '.join(select_parts)} FROM {from_sql}")
        columns = [str(item[0]) for item in cursor.description or []]
        row_count = 0
        preview = []
        with path.open("w", encoding="utf-8-sig", newline="") as output_file:
            writer = csv.DictWriter(output_file, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            while rows := cursor.fetchmany(1000):
                for values in rows:
                    row = dict(zip(columns, values))
                    writer.writerow(row)
                    if len(preview) < 50:
                        preview.append(row)
                    row_count += 1
        safe_name = re.sub(r"[^A-Za-z0-9_-]+", "_", model_name)
        return {"status": "JOINED_CSV_CREATED", "view_name": model_name, "source_names": source_names,
                "row_count": row_count, "columns": columns, "preview": preview,
                "projected_fields": requested_projection, "pending_calculated_fields": pending_calculated_fields,
                "missing_calculation_metadata": missing_calculation_metadata,
                "field_mappings": field_mappings,
                "join_conditions": join_clauses, "execution_engine": "Read-only SQLite over all staged CDS sources",
                "sql": f"SELECT {', '.join(select_parts)} FROM {from_sql}",
                "export_id": export_id, "download_url": f"/api/cds-staging/exports/{export_id}",
                "filename": f"{safe_name}_joined_sources.csv"}
    except Exception as exc:
        path.unlink(missing_ok=True)
        raise HTTPException(status_code=422, detail=f"Could not join the staged CDS sources: {exc}") from exc
    finally:
        db.close()


@app.post("/api/cds-staging/replica-csv")
async def create_cds_replica_csv(request: dict[str, Any]):
    """Rebuild the Datasphere projection from all staged sources and its metadata joins."""
    space_id = str(request.get("space_id") or DATASPHERE_SPACE_ID).strip()
    source_name = str(request.get("source_name") or "").strip()
    source_names = request.get("source_names")
    if not isinstance(source_names, list) or not source_names:
        source_names = [source_name] if source_name else []
    source_names = list(dict.fromkeys(str(name).strip() for name in source_names if str(name).strip()))
    if not space_id or not source_names:
        raise HTTPException(status_code=400, detail="A Datasphere space and staged CDS sources are required.")
    raw_calculated_fields = request.get("calculated_fields") or {}
    if not isinstance(raw_calculated_fields, dict):
        raise HTTPException(status_code=400, detail="Calculated fields must be a name-to-formula mapping.")
    calculated_fields = {
        str(name).strip(): str(formula).strip()
        for name, formula in raw_calculated_fields.items()
        if str(name).strip() and str(formula).strip()
    }
    selected_fields = request.get("selected_fields")
    if not isinstance(selected_fields, list):
        selected_fields = None
    if selected_fields is not None:
        selected_set = {str(name).casefold() for name in selected_fields}
        calculated_fields = {name: formula for name, formula in calculated_fields.items() if name.casefold() in selected_set}

    export_id = uuid.uuid4().hex
    path = _cds_csv_export_path(export_id)
    try:
        model_name = str(request.get("view_name") or "").strip()
        if not model_name:
            raise ValueError("The selected Datasphere model name is required to rebuild its source joins.")
        client = DatasphereResolverClient(space=space_id)
        cached_entry = client.cache.get_model_metadata(space_id, model_name) if client.cache else None
        cached_metadata = cached_entry.get("metadata", {}) if cached_entry else {}
        dependency_tree = cached_metadata.get("dependency_tree") if isinstance(cached_metadata, dict) else None
        if not isinstance(dependency_tree, dict) or not dependency_tree:
            resolver = DatasphereDependencyResolver(client=client, space=space_id, silent=True)
            dependency_tree = await asyncio.to_thread(resolver.resolve, model_name, object_type="analytic_model")
        if not isinstance(dependency_tree, dict) or dependency_tree.get("status") != "resolved":
            raise ValueError(f"Could not resolve Datasphere join metadata for model {model_name}.")

        store = CdsStagingStore()
        staged_by_name = {str(item.get("source", "")).casefold(): item for item in store.list_staged_sources(space_id)}
        snapshots = []
        for name in source_names:
            snapshot = staged_by_name.get(name.casefold())
            if not snapshot or not snapshot.get("complete") or not snapshot.get("fresh"):
                raise ValueError(f"Source {name} has no complete, fresh staged snapshot. Retrieve all model sources again.")
            snapshots.append(snapshot)

        transformation_ir = _build_transformation_ir(dependency_tree)
        joins = transformation_ir.get("joins", []) if len(snapshots) > 1 else []
        if len(snapshots) > 1 and not joins:
            raise ValueError("The model resolves to multiple CDS sources, but its Datasphere metadata contains no join condition. No cross join was created.")

        # Join operands can be Datasphere wrapper views (for example
        # WRAP_Z_TESTVIEW02) around replicated CDS entities. Resolve a wrapper
        # only when its name unambiguously ends in one of the staged CDS names.
        source_metadata = [{
            "entity": item["source"],
            "fields": [{"name": column, "type": item.get("column_types", {}).get(column)} for column in item.get("columns", [])],
            "key_fields": item.get("key_fields", []),
        } for item in snapshots]
        source_lookup = {str(item["entity"]).casefold(): str(item["entity"]) for item in source_metadata}
        normalized_joins = []
        for join in joins:
            if not isinstance(join, dict):
                continue
            join = dict(join)
            for side_name in ("left", "right"):
                side = join.get(side_name)
                if not isinstance(side, dict):
                    continue
                side = dict(side)
                label = str(side.get("source") or side.get("name") or "").strip()
                candidate_matches = [entity for key, entity in source_lookup.items() if label.casefold() == key or label.casefold().endswith(key)]
                if len(candidate_matches) == 1:
                    side.setdefault("as", label)
                    side["source"] = candidate_matches[0]
                join[side_name] = side
            normalized_joins.append(join)
        transformation_ir["joins"] = normalized_joins
        if calculated_fields:
            known = {str(item.get("name", "")).casefold() for item in dependency_tree.get("calculated_columns", []) if isinstance(item, dict)}
            dependency_tree.setdefault("calculated_columns", [])
            for name, formula in calculated_fields.items():
                if name.casefold() not in known:
                    dependency_tree["calculated_columns"].append({"name": name, "expression": formula})
            transformation_ir["calculations"] = list(transformation_ir.get("calculations", [])) + [
                {"name": name, "expression": formula} for name, formula in calculated_fields.items()
            ]

        from llm_client import compile_reconciliation_blueprint_deterministic
        output_fields = [str(name) for name in selected_fields] if selected_fields is not None else [
            str(field.get("name")) for field in dependency_tree.get("field_catalog", []) if isinstance(field, dict) and field.get("name")
        ]
        key_fields = [str(field.get("name")) for field in dependency_tree.get("field_catalog", []) if isinstance(field, dict) and field.get("name") and field.get("is_key")]
        plan = compile_reconciliation_blueprint_deterministic(
            model_name=model_name, space_id=space_id,
            architecture_metadata={
                "dependency_tree": dependency_tree,
                "transformation_ir": transformation_ir,
                # The local replica is row-level CDS data. Retain all rows
                # selected by Datasphere filters and calculate statistics
                # afterward instead of aggregating away duplicate grains.
                "preserve_source_rows": True,
            },
            source_metadata=source_metadata, selected_fields=output_fields, key_fields=key_fields,
        )
        open_sql = str(plan.get("s4hana_sql") or "").strip().rstrip(";")
        if not re.match(r"(?is)^SELECT\b", open_sql):
            raise ValueError("Datasphere metadata did not produce a safe SELECT for the final model projection.")
        table_by_entity = {str(item["source"]).casefold(): str(item["table_name"]) for item in snapshots}
        def replace_source(match: re.Match[str]) -> str:
            keyword, entity = match.group(1), match.group(2)
            table = table_by_entity.get(entity.casefold())
            if not table:
                raise ValueError(f"Generated model query references unstaged CDS source {entity}.")
            return f'{keyword} {store._quote_identifier(table)} AS {store._quote_identifier(entity)}'
        local_sql = re.sub(r"(?i)\b(FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_]*)", replace_source, open_sql).replace("~", ".")
        # The deterministic Datasphere translator emits ABAP Open SQL syntax
        # for substring. Replica evaluation runs in SQLite, so convert that
        # function form to SQLite substr() while preserving the 0-based ABAP
        # offset semantics.
        local_sql = re.sub(
            r"(?is)\bsubstring\s*\(\s*val\s*=\s*(.*?)\s+off\s*=\s*(-?\d+)\s+len\s*=\s*(\d+)\s*\)",
            lambda match: f"substr({match.group(1).strip()}, {int(match.group(2)) + 1}, {match.group(3)})",
            local_sql,
        )
        numeric_type_codes = {"N", "I", "P", "F", "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "DECIMAL", "NUMERIC", "NUMBER", "FLOAT", "DOUBLE", "REAL", "DECFLOAT"}
        numeric_source_columns = {
            str(column).casefold()
            for snapshot in snapshots
            for column, raw_type in (snapshot.get("column_types") or {}).items()
            if str(raw_type or "").strip().upper() in numeric_type_codes
            or bool(re.search(r"(?i)(?:^|[._])(?:EDM\.)?(?:INT\d*|INTEGER|BIGINT|SMALLINT|TINYINT|DECIMAL|NUMERIC|NUMBER|FLOAT|DOUBLE|REAL|DECFLOAT)(?:$|[._])", str(raw_type or "")))
        }
        if numeric_source_columns:
            numeric_comparison = re.compile(
                r"(?P<lhs>(?:(?:\"?[A-Za-z_][A-Za-z0-9_]*\"?)\s*\.\s*)?\"?[A-Za-z_][A-Za-z0-9_]*\"?)"
                r"\s*(?P<operator><=|>=|<>|!=|=|<|>)\s*"
                r"(?P<rhs>'[+-]?\d+(?:\.\d+)?'|[+-]?\d+(?:\.\d+)?)"
            )

            def cast_numeric_comparison(match: re.Match[str]) -> str:
                lhs = match.group("lhs")
                field_name = lhs.rsplit(".", 1)[-1].strip().strip("\"").casefold()
                if field_name not in numeric_source_columns:
                    return match.group(0)
                rhs = match.group("rhs")
                numeric_rhs = rhs[1:-1] if rhs.startswith("'") else rhs
                return f"CAST({lhs} AS NUMERIC) {match.group('operator')} CAST({numeric_rhs} AS NUMERIC)"

            local_sql = numeric_comparison.sub(cast_numeric_comparison, local_sql)
        allowed_tables = {str(item["table_name"]).casefold() for item in snapshots}
        db = store._connect()
        db.create_function("MONTH", 1, lambda value: int(str(value or "")[4:6]) if len(str(value or "")) >= 6 and str(value or "")[4:6].isdigit() else None, deterministic=True)
        db.create_function("YEAR", 1, lambda value: int(str(value or "")[:4]) if len(str(value or "")) >= 4 and str(value or "")[:4].isdigit() else None, deterministic=True)

        def parse_formula_date(value: Any, fmt: Optional[str] = None) -> Optional[date]:
            if value is None or str(value).strip() == "":
                return None
            raw = str(value).strip()
            try:
                if fmt and fmt.upper() == "YYYYMMDD":
                    return datetime.strptime(raw[:8], "%Y%m%d").date()
                if len(raw) == 8 and raw.isdigit():
                    return datetime.strptime(raw, "%Y%m%d").date()
                return date.fromisoformat(raw[:10])
            except (TypeError, ValueError):
                return None

        db.create_function(
            "TO_DATE", -1,
            lambda value, fmt=None: (parse_formula_date(value, fmt).isoformat()
                                     if parse_formula_date(value, fmt) else None),
            deterministic=True,
        )
        db.create_function(
            "DAYS_BETWEEN", 2,
            lambda start, end: ((parse_formula_date(end) - parse_formula_date(start)).days
                                if parse_formula_date(start) and parse_formula_date(end) else None),
            deterministic=True,
        )
        db.set_authorizer(lambda action, arg1, _arg2, _database, _trigger:
            sqlite3.SQLITE_OK if action in (sqlite3.SQLITE_SELECT, sqlite3.SQLITE_FUNCTION, sqlite3.SQLITE_RECURSIVE)
            or (action == sqlite3.SQLITE_READ and str(arg1).casefold() in allowed_tables) else sqlite3.SQLITE_DENY)
        try:
            cursor = db.execute(local_sql)
            output_columns = [str(item[0]) for item in cursor.description or []]
            if not output_columns:
                raise ValueError("The joined Datasphere projection returned no columns.")
            with path.open("w", encoding="utf-8-sig", newline="") as output_file:
                writer = csv.DictWriter(output_file, fieldnames=output_columns, extrasaction="ignore")
                writer.writeheader()
                row_count = 0
                preview = []
                while rows := cursor.fetchmany(1000):
                    for raw_row in rows:
                        row = dict(zip(output_columns, raw_row))
                        writer.writerow(row)
                        if len(preview) < 20:
                            preview.append(row)
                        row_count += 1
        finally:
            db.close()
        result = {"row_count": row_count, "columns": output_columns, "rows": preview, "execution_engine": "Read-only SQLite over all staged CDS sources", "sql": local_sql}
    except Exception as exc:
        path.unlink(missing_ok=True)
        raise HTTPException(status_code=422, detail=f"Could not create the CDS replica CSV: {exc}") from exc
    safe_name = re.sub(r"[^A-Za-z0-9_-]+", "_", model_name)
    return {
        "status": "CSV_CREATED", "source_name": ", ".join(source_names), "source_names": source_names,
        "export_id": export_id,
        "row_count": result["row_count"], "columns": result["columns"],
        "preview": result["rows"], "calculated_fields": list(calculated_fields),
        "omitted_output_fields": plan.get("omitted_output_fields", []),
        "execution_engine": result["execution_engine"],
        "replica_sql": result["sql"],
        "applied_metadata_filter": "; ".join(str(item.get("condition") or item.get("expression")) for item in transformation_ir.get("filters", []) if isinstance(item, dict)),
        "calculated_field_logic": [{"name": name, "formula": formula, "sql_expression": formula} for name, formula in calculated_fields.items()],
        "download_url": f"/api/cds-staging/exports/{export_id}",
        "filename": f"{safe_name}_cds_replica.csv",
    }


@app.get("/api/cds-staging/exports/{export_id}")
async def download_cds_replica_csv(export_id: str):
    path = _cds_csv_export_path(export_id)
    if not path.is_file():
        raise HTTPException(status_code=404, detail="CSV export has expired or was not found.")
    return FileResponse(path, media_type="text/csv", filename=f"cds_replica_{export_id[:8]}.csv")


@app.post("/api/cds-staging/csv-prompt-query")
async def query_final_cds_csv(request: StagedCsvPromptRequest):
    """Generate and run the user's read-only query against the final replica CSV."""
    if not re.fullmatch(r"[a-f0-9]{32}", request.export_id):
        raise HTTPException(status_code=400, detail="Invalid final CSV export ID.")
    path = _cds_csv_export_path(request.export_id)
    if not path.is_file():
        raise HTTPException(status_code=404, detail="The final CDS replica CSV is missing or expired.")
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as csv_file:
            reader = csv.DictReader(csv_file)
            columns = [str(name) for name in (reader.fieldnames or []) if name]
            rows = list(reader)
        if not columns:
            raise ValueError("The final CDS CSV has no columns.")

        from llm_client import generate_odata_query_blueprint
        table_name = "FINAL_CDS_CSV"
        blueprint = await generate_odata_query_blueprint(
            request.user_prompt,
            {table_name: columns},
            DATASPHERE_SPACE_ID,
            [],
            selected_fields=None,
        )
        sql = str(blueprint.get("sql_query") or "").strip().rstrip(";").strip()
        if not re.match(r"(?is)^SELECT\b", sql) or ";" in sql or re.search(
            r"(?i)\b(INSERT|UPDATE|DELETE|MERGE|UPSERT|CREATE|ALTER|DROP|CALL|DO)\b", sql
        ):
            raise ValueError("Generated CDS query is not a single read-only SELECT.")
        source_matches = re.findall(
            r'(?i)\b(?:FROM|JOIN)\s+(?:"([^"]+)"|([A-Za-z_][A-Za-z0-9_]*))', sql
        )
        sources = [quoted or unquoted for quoted, unquoted in source_matches]
        if len(sources) != 1 or sources[0].casefold() != table_name.casefold() or re.search(
            r"(?i)\bJOIN\b", sql
        ):
            raise ValueError("Generated SQL must read only from the final CDS replica CSV.")

        connection = sqlite3.connect(":memory:")
        try:
            quote = lambda value: '"' + value.replace('"', '""') + '"'
            connection.execute(
                f"CREATE TABLE {quote(table_name)} ({', '.join(quote(column) + ' TEXT' for column in columns)})"
            )
            if rows:
                placeholders = ",".join("?" for _ in columns)
                connection.executemany(
                    f"INSERT INTO {quote(table_name)} VALUES ({placeholders})",
                    [[row.get(column) for column in columns] for row in rows],
                )

            def read_only_authorizer(action, _arg1, _arg2, _database, _trigger):
                allowed = {
                    sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ,
                    sqlite3.SQLITE_FUNCTION, sqlite3.SQLITE_RECURSIVE,
                }
                return sqlite3.SQLITE_OK if action in allowed else sqlite3.SQLITE_DENY

            connection.set_authorizer(read_only_authorizer)
            # CSV columns are loaded as TEXT so SAP identifiers retain their
            # leading zeroes (for example ACCOUNTINGDOCUMENTITEM='002'). The
            # generated SQL may express the same identifier as numeric 2 or
            # text '2'. Compare numeric predicates numerically in that case.
            numeric_comparison = re.compile(
                r"(?P<lhs>(?:[A-Za-z_][A-Za-z0-9_]*\s*\.\s*)?(?:\"(?:[^\"]|\"\")+\"|[A-Za-z_][A-Za-z0-9_]*))"
                r"\s*(?P<operator><=|>=|<>|!=|=|<|>)\s*"
                r"(?P<rhs>'[+-]?\d+(?:\.\d+)?'|[+-]?\d+(?:\.\d+)?)"
            )
            known_columns = {column.casefold() for column in columns}

            def normalize_numeric_comparison(match: re.Match[str]) -> str:
                lhs = match.group("lhs")
                identifier = lhs.rsplit(".", 1)[-1].strip().strip('"').replace('""', '"')
                if identifier.casefold() not in known_columns:
                    return match.group(0)
                rhs = match.group("rhs")
                numeric_rhs = rhs[1:-1] if rhs.startswith("'") else rhs
                return f"CAST({lhs} AS NUMERIC) {match.group('operator')} {numeric_rhs}"

            sql = numeric_comparison.sub(normalize_numeric_comparison, sql)
            cursor = connection.execute(sql)
            result_columns = [item[0] for item in (cursor.description or [])]
            result_rows = [dict(zip(result_columns, row)) for row in cursor.fetchall()]
        finally:
            connection.close()
        total_count = next((
            int(row["TOTAL_COUNT"]) for row in result_rows
            if row.get("TOTAL_COUNT") is not None
        ), len(result_rows))
        return {
            "status": "EXECUTED", "sql": sql, "data": result_rows,
            "columns": result_columns, "record_count": len(result_rows),
            "total_count": total_count, "source": "Final CDS replica CSV",
            "execution_engine": "Read-only SQLite over the final CDS replica CSV",
            "query_blueprint": blueprint,
        }
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Could not query final CDS replica CSV: {exc}") from exc

@app.post("/api/cds-staging/sql-preview")
async def preview_staged_reconstruction(request: StagedSQLPreviewRequest):
    """Resolve lineage, generate local SQL, and validate it before approval."""
    target_space = (request.space_id or DATASPHERE_SPACE_ID).strip()
    if not target_space or not request.view_name.strip():
        raise HTTPException(status_code=400, detail="A Datasphere space and analytical model are required.")
    try:
        client = DatasphereResolverClient(space=target_space)
        resolver = DatasphereDependencyResolver(client=client, space=target_space, silent=True)
        tree = await asyncio.to_thread(resolver.resolve, request.view_name.strip(), object_type="analytic_model")
        if tree.get("status") != "resolved" or not isinstance(tree.get("definition"), dict):
            raise HTTPException(status_code=409, detail="Analytical model or its dependency graph is unresolved.")
        all_nodes = _walk_dependency_nodes(tree)
        leaf_nodes: dict[str, dict[str, Any]] = {}
        for node in all_nodes:
            name = str(node.get("name") or "").strip()
            if name and name.casefold() != request.view_name.casefold() and not node.get("dependencies"):
                leaf_nodes.setdefault(name.casefold(), node)
        try:
            inventory = client.cache.list_replication_local_tables() if client.cache else []
            verified = client.match_dependency_sources(tree, inventory)
        except Exception:
            verified = []
        source_nodes: dict[str, dict[str, Any]] = {}
        for item in verified:
            source_name = str(item.get("matched_cds_view") or "").strip()
            if source_name:
                source_nodes.setdefault(source_name.casefold(), {**item, "name": source_name})
        if not source_nodes:
            source_nodes = leaf_nodes
        store = CdsStagingStore()
        staged = store.list_staged_sources(target_space)
        staged_by_name = {str(item.get("source", "")).casefold(): item
                          for item in staged if item.get("complete") and item.get("fresh")}
        missing = sorted(name for key, node in source_nodes.items()
                         if (name := str(node.get("name") or "")) and key not in staged_by_name)
        if missing:
            raise HTTPException(status_code=409, detail={
                "message": "Every terminal CDS source must have a complete staged snapshot.",
                "missing_sources": missing,
            })
        from llm_client import generate_staged_reconstruction_blueprint
        source_snapshots = [staged_by_name[key] for key in source_nodes]
        plan = await generate_staged_reconstruction_blueprint(
            model_name=request.view_name.strip(), space_id=target_space,
            architecture_metadata={"dependency_tree": tree}, staged_sources=source_snapshots,
            selected_fields=request.selected_fields, key_fields=request.key_fields,
        )
        if not plan.get("local_sql") or plan.get("unsupported_transformations"):
            return {"status": "BLOCKED", "plan": plan, "approval_required": True,
                    "message": "Reconstruction SQL is incomplete; execution is blocked."}
        graph_text = re.sub(r"\s+", "", json.dumps(tree, ensure_ascii=True, default=str)).casefold()
        join_errors = []
        if re.search(r"\bjoin\b", plan["local_sql"], re.IGNORECASE):
            joins = plan.get("joins") or []
            if not joins:
                join_errors.append("SQL contains a JOIN but the structured plan has no verified join definitions.")
            for index, join in enumerate(joins):
                if not isinstance(join, dict):
                    join_errors.append(f"Join {index + 1} is not a structured metadata mapping.")
                    continue
                required = ("left_source", "right_source", "join_type", "condition", "source_path")
                missing_join_fields = [field for field in required if not str(join.get(field) or "").strip()]
                if missing_join_fields:
                    join_errors.append(f"Join {index + 1} lacks: {', '.join(missing_join_fields)}.")
                    continue
                normalized_condition = re.sub(r"\s+", "", str(join["condition"])).casefold()
                normalized_path = re.sub(r"\s+", "", str(join["source_path"])).casefold()
                if normalized_condition not in graph_text or normalized_path not in graph_text:
                    join_errors.append(f"Join {index + 1} condition or provenance is not present in Datasphere metadata.")
        if join_errors:
            plan["unsupported_transformations"] = join_errors
            return {"status": "BLOCKED", "plan": plan, "approval_required": True,
                    "message": "Join conditions could not be verified against the dependency metadata; execution is blocked."}
        approval_context = {
            **plan,
            "allowed_table_names": [item["table_name"] for item in source_snapshots],
            "source_snapshot_ids": [item["snapshot_id"] for item in source_snapshots],
        }
        try:
            validation = store.validate_read_only_sql(
                plan["local_sql"], target_space, approval_context=approval_context,
            )
        except Exception as exc:
            return {"status": "SQL_INVALID", "plan": plan, "approval_required": True,
                    "message": f"Generated local SQL failed schema validation: {exc}"}
        return {
            "status": "READY_FOR_APPROVAL", "plan": plan,
            "sql": validation["query"], "query_plan": validation["query_plan"],
            "approved_query_hash": validation["approval_hash"],
            "approval_context": approval_context,
            "staged_sources": [{key: item.get(key) for key in
                ("source", "table_name", "row_count", "staged_at", "expires_at", "snapshot_id", "fresh")}
                for item in source_snapshots],
            "approval_required": True,
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Could not generate staged reconstruction plan")
        raise HTTPException(status_code=502, detail=f"Staged reconstruction planning failed: {exc}") from exc

@app.post("/api/cds-staging/sql-execute")
async def execute_staged_reconstruction(request: StagedSQLExecutionRequest):
    """Execute only the approved SQL against the exact currently staged snapshots."""
    try:
        return CdsStagingStore().execute_read_only_sql(
            sql=request.sql, approved_hash=request.approved_query_hash,
            space_id=request.space_id, approval_context=request.approval_context,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Staged local SQL execution failed")
        raise HTTPException(status_code=500, detail=f"Staged SQL execution failed: {exc}") from exc

# ---------------------------------------------------------------------------
# Data Validation & Reconciliation API Endpoint
# ---------------------------------------------------------------------------
@app.post("/api/validate", response_model=ValidationResponse)
async def execute_validation(request: ValidationRequest):
    """
    Validates Datasphere Analytical Model against underlying S/4HANA CDS views.
    Target Datasphere view is supplied directly by the user.
    Reconciliation execution is separate from the explicit CDS staging action.
    """
    return await run_metadata_driven_validation(request)

# ---------------------------------------------------------------------------
# Application Server Launcher
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("Starting SAP Datasphere Conversational AI backend on http://0.0.0.0:9050")
    uvicorn.run("main:app", host="0.0.0.0", port=9050, reload=True)
