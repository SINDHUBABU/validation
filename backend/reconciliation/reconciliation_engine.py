"""
Reconciliation & Step Replication Engine
Coordinates:
1. Analytical Model Dependency Resolution (via DatasphereDependencyResolver)
2. Cache Matching (against replicated ADT/Eclipse tables in DatasphereSpaceCache)
3. Step Replication into S/4HANA CDS Open SQL
4. Human Approval Gateway
5. Zero-Data-Leakage Execution & Comparison
6. Data Quality Report Generation
"""
from __future__ import annotations

import logging
import os
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path

# Local imports
from .datasphere_space_cache import DatasphereSpaceCache
import sys
from dotenv import load_dotenv

# Ensure parent directory and reconciliation directory are importable
current_dir = str(Path(__file__).resolve().parent)
parent_dir = str(Path(__file__).resolve().parents[1])
for p_item in (current_dir, parent_dir):
    if p_item not in sys.path:
        sys.path.insert(0, p_item)

# Load .env from active paths
for p in (Path(parent_dir) / ".env", Path(parent_dir).parent / "demo1-main" / "backend" / ".env", Path(".env")):
    if p.exists():
        load_dotenv(str(p), override=True)
        break

try:
    from reconciliation.dependency_resolver import DatasphereDependencyResolver, build_client
except ImportError:
    from dependency_resolver import DatasphereDependencyResolver, build_client

try:
    from retrieval.data_retriever import DataRetriever
except ImportError:
    from data_retriever import DataRetriever

logger = logging.getLogger("ReconciliationEngine")


class ReconciliationEngine:
    def __init__(
        self,
        space_id: Optional[str] = None,
        cache_db_path: str = "cache/datasphere_metadata.sqlite3",
        default_row_limit: int = 100
    ):
        self.space_id = (space_id or os.getenv("DATASPHERE_MODELING_SPACE", "")).strip()
        if not self.space_id:
            raise RuntimeError(
                "Datasphere modeling space is not configured. Set "
                "DATASPHERE_MODELING_SPACE in backend/.env."
            )
        self.cache = DatasphereSpaceCache(cache_db_path)
        self.default_row_limit = default_row_limit
        self.reports_history: List[Dict[str, Any]] = []

    # ==========================================================================
    # PHASE 1: DEPENDENCY RESOLUTION & CACHE MATCHING
    # ==========================================================================
    def resolve_model_and_match_sources(
        self,
        model_name: str,
        space_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Resolve live metadata without inventing sources, transformations, or lineage.

        Shared local tables are discovered in the selected modeling space first.
        The separate replication-space cache is supplementary evidence only; a
        failed cache lookup is never converted into a guessed CDS source.
        """
        target_space = (space_id or self.space_id).strip()
        if not target_space:
            raise ValueError("A Datasphere modeling space is required.")

        client = build_client(host="", space=target_space)
        resolver = DatasphereDependencyResolver(client=client, space=target_space, silent=True)
        tree = resolver.resolve(model_name, object_type="analytic_model")
        if tree.get("status") != "resolved" or not isinstance(tree.get("definition"), dict):
            raise RuntimeError(
                f"Analytical model {model_name!r} could not be resolved from live metadata. "
                f"Resolver status: {tree.get('status')!r}. No synthetic model will be created."
            )

        field_catalog = tree.get("field_catalog", [])
        measures: list[dict] = []
        dimensions: list[dict] = []
        for field in field_catalog:
            name = field.get("name")
            if not isinstance(name, str) or not name.strip():
                continue
            aggregation = field.get("aggregation")
            element_type = field.get("element_type")
            common = {
                "name": name,
                "label": field.get("label") or name,
                "type": element_type,
            }
            if aggregation:
                measures.append({
                    **common,
                    "aggregation": str(aggregation).lstrip("#").upper(),
                })
            elif field.get("measure_type") or field.get("measure_kind"):
                # A measure without an explicit aggregation is incomplete metadata.
                measures.append({**common, "aggregation": None, "metadata_incomplete": True})
            else:
                dimensions.append({**common, "is_key": bool(field.get("is_key"))})

        all_dependencies: list[dict] = []
        leaf_nodes: list[dict] = []
        lineage_chain: list[dict] = []

        def walk(node: dict, path: str = "", depth: int = 0):
            name = node.get("name")
            object_type = node.get("object_type", "unknown")
            current_path = f"{path} -> {name}" if path and name else (name or path)
            lineage_chain.append({
                "name": name,
                "type": object_type,
                "space": target_space,
                "status": node.get("status"),
                "relationship": node.get("relationship"),
                "depth": depth,
            })
            if name and name != model_name:
                all_dependencies.append({
                    "name": name,
                    "type": object_type,
                    "relationship": node.get("relationship", "dependency"),
                    "path": current_path,
                    "status": node.get("status"),
                })
            children = node.get("dependencies") or []
            if not children and name and name != model_name:
                leaf_nodes.append({
                    "name": name,
                    "type": object_type,
                    "path": current_path,
                    "status": node.get("status"),
                })
            for child in children:
                if isinstance(child, dict):
                    walk(child, current_path, depth + 1)

        walk(tree)

        # Discover local tables in the modeling space (the same space in which
        # shared objects are visible). A table-name match is not, by itself,
        # proof that it is an S/4 CDS view.
        local_table_items = client.list_local_tables(target_space, top=5000)
        local_names = {
            str(item.get("technicalName") or item.get("technical_name") or item.get("name") or "").strip().casefold(): item
            for item in local_table_items
            if isinstance(item, dict)
            and str(item.get("technicalName") or item.get("technical_name") or item.get("name") or "").strip()
        }
        matched_sources: list[dict] = []
        for candidate in leaf_nodes:
            key = candidate["name"].strip().casefold()
            if candidate.get("status") != "resolved" or key not in local_names:
                continue
            table_item = local_names[key]
            try:
                table_definition = client.read_local_table(target_space, candidate["name"])
            except Exception as exc:
                logger.warning("Could not read shared local-table definition %s: %s", candidate["name"], exc)
                continue
            matched_sources.append({
                "datasphere_node_name": candidate["name"],
                "datasphere_node_type": "local_table",
                "lineage_path": candidate["path"],
                "matched_datasphere_local_table": candidate["name"],
                "matched_cds_view": None,
                "replicated_from_adt": False,
                "replication_space": os.getenv("DATASPHERE_REPLICATION_SPACE", "").strip() or None,
                "verified_in_replication_space": False,
                "description": table_item.get("description"),
                "key_fields": [],
                "definition": table_definition,
                "matched_by": "exact_name_in_modeling_space",
                "source_origin_status": "unverified",
            })

        # If a configured replication-space snapshot independently confirms the
        # same exact table name, attach that evidence. Do not infer prefixes or
        # invent key fields.
        replication_space = os.getenv("DATASPHERE_REPLICATION_SPACE", "").strip()
        if replication_space:
            for match in matched_sources:
                try:
                    cached = self.cache.match_replicated_source(
                        target_space,
                        match["matched_datasphere_local_table"],
                        replication_space=replication_space,
                    )
                except Exception as exc:
                    logger.debug("Replication cache match unavailable: %s", exc)
                    cached = None
                if cached:
                    match.update({
                        "matched_cds_view": cached.get("cds_view_name"),
                        "replicated_from_adt": bool(cached.get("replicated_from_adt")),
                        "verified_in_replication_space": bool(cached.get("verified_in_replication_space")),
                        "description": cached.get("description") or match.get("description"),
                        "key_fields": cached.get("key_fields") or [],
                        "source_origin_status": "replication_cache_match",
                    })

        primary_source = None
        adt_metadata = None
        adt_fields: list[dict] = []
        adt_joins: list[dict] = []
        adt_sources: list[dict] = []
        source_candidate = None
        if matched_sources:
            source_candidate = matched_sources[0].get("matched_cds_view") or matched_sources[0].get("matched_datasphere_local_table")
            try:
                from adt.cds_metadata import CDSMetadataClient
                from config import get_config
                cfg = get_config()
                with CDSMetadataClient(cfg["base_url"], cfg["username"], cfg["password"]) as adt_client:
                    adt_metadata = adt_client.discover_metadata(source_candidate)
                if adt_metadata:
                    primary_source = source_candidate
                    adt_fields = adt_metadata.get("fields", []) or []
                    lineage = adt_metadata.get("lineage", {}) or {}
                    adt_joins = lineage.get("joins", []) or []
                    adt_sources = lineage.get("sources", []) or []
            except Exception as exc:
                logger.warning("Live ADT verification for candidate [%s] failed: %s", source_candidate, exc)

        if primary_source:
            matched_sources[0]["matched_cds_view"] = primary_source
            matched_sources[0]["source_origin_status"] = "verified_by_adt_metadata"
            matched_sources[0]["replicated_from_adt"] = True
        else:
            logger.warning(
                "No S/4HANA CDS source was verified from the selected model's live dependencies. "
                "A reconciliation plan must not be generated from a guessed source."
            )

        unresolved = [
            item for item in all_dependencies
            if item.get("status") not in {"resolved", "boundary"}
        ]
        return {
            "model_name": model_name,
            "space_id": target_space,
            "status": tree.get("status", "unknown"),
            "dependency_tree": tree,
            "lineage_chain": lineage_chain,
            "all_dependencies": all_dependencies,
            "measures": measures,
            "dimensions": dimensions,
            "adt_fields": adt_fields,
            "adt_joins": adt_joins,
            "adt_sources": adt_sources,
            "matched_sources": matched_sources,
            "primary_source_cds": primary_source,
            "source_candidate": source_candidate,
            "source_metadata_verified": bool(primary_source and adt_metadata),
            "unresolved_dependencies": unresolved,
            "replication_verification": {
                "verified": bool(primary_source and matched_sources and matched_sources[0].get("verified_in_replication_space")),
                "replication_space": replication_space or None,
                "replicated_table_name": matched_sources[0].get("matched_datasphere_local_table") if matched_sources else None,
                "matched_cds_view": primary_source,
                "reason": None if primary_source else "No exact source was verified through live ADT metadata.",
            },
        }

    # ==========================================================================
    # PHASE 2: STEP REPLICATION & SQL GENERATION (WITH HUMAN APPROVAL)
    # ==========================================================================
    def build_reconciliation_plan(
        self,
        model_name: str,
        space_id: Optional[str] = None,
        custom_filters: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Replicates the exact analytical model transformations (group by, sum, count, filters)
        into an equivalent Open SQL query for the matched S/4HANA CDS view.
        Produces query plans, metadata explanation, and human approval gateway payload.
        """
        resolution = self.resolve_model_and_match_sources(model_name, space_id)
        primary_source = resolution.get("primary_source_cds")
        if not primary_source or not resolution.get("source_metadata_verified"):
            raise RuntimeError(
                "Cannot generate reconciliation SQL: the selected model's leaf source "
                "has not been verified against live S/4HANA ADT CDS metadata. "
                f"Candidate={resolution.get('source_candidate')!r}; "
                f"reason={resolution.get('replication_verification', {}).get('reason')!r}."
            )
        if resolution.get("unresolved_dependencies"):
            raise RuntimeError(
                "Cannot generate reconciliation SQL because one or more model dependencies "
                "could not be resolved from live Datasphere metadata."
            )
        logic_nodes = []
        def inspect_logic(node):
            if not isinstance(node, dict):
                return
            if node.get("object_type") in {"view", "analytic_model", "fact_model"}:
                for logic_key in ("joins", "filters", "calculated_columns", "formulas"):
                    entries = node.get(logic_key)
                    if isinstance(entries, list) and entries:
                        logic_nodes.append({"object": node.get("name"), "kind": logic_key, "count": len(entries)})
            for child in node.get("dependencies", []) or []:
                inspect_logic(child)
        inspect_logic(resolution.get("dependency_tree", {}))
        if logic_nodes:
            raise RuntimeError(
                "The selected model contains Datasphere transformations that this SQL planner "
                "has not yet translated into equivalent ABAP Open SQL. Refusing to generate a "
                f"potentially non-equivalent query. Detected metadata: {logic_nodes!r}"
            )
        measures = resolution.get("measures", [])
        dimensions = resolution.get("dimensions", [])
        adt_joins = resolution.get("adt_joins", [])
        adt_sources = resolution.get("adt_sources", [])
        adt_fields = resolution.get("adt_fields", [])

        # Choose primary dimension for reconciliation grain (e.g. SalesOrganization or CompanyCode)
        key_dim = next((d["name"] for d in dimensions if d.get("is_key")), None)
        if not key_dim and dimensions:
            key_dim = dimensions[0]["name"]
        if not key_dim:
            raise RuntimeError(
                "Cannot generate reconciliation SQL: the model metadata does not identify "
                "a reconciliation dimension/key. Select a verified grain from model metadata."
            )
        if not adt_fields:
            raise RuntimeError(
                "Cannot generate reconciliation SQL: ADT returned no source field metadata."
            )
        field_names = {str(f.get("name")) for f in adt_fields if f.get("name")}
        missing_measures = [m.get("name") for m in measures if m.get("name") not in field_names]
        if missing_measures:
            raise RuntimeError(
                "Cannot safely translate these model measures to direct source fields: "
                + ", ".join(str(x) for x in missing_measures)
                + ". Their calculated/restricted-measure logic must be compiled from verified "
                "Datasphere metadata before a reconciliation query can be generated."
            )
        incomplete_measures = [m.get("name") for m in measures if not m.get("aggregation")]
        if incomplete_measures:
            raise RuntimeError(
                "Cannot generate SQL because aggregation metadata is missing for: "
                + ", ".join(str(x) for x in incomplete_measures)
            )

        # Build Select Columns from verified metadata only.
        s4_select_parts = [key_dim]
        s4_select_parts.append("COUNT( * ) AS RecordCount")

        for m in measures:
            m_name = m["name"]
            m_agg = m.get("aggregation", "SUM")

            # Only use exact source-field matches. Do not infer semantic mappings.
            cds_field_name = m_name
            if m_agg == "COUNT":
                s4_select_parts.append(f"COUNT( {key_dim} ) AS Total{m_name}")
            elif m_agg in {"SUM", "MIN", "MAX", "AVG"}:
                s4_select_parts.append(f"{m_agg}( {cds_field_name} ) AS Total{m_name}")
            else:
                raise RuntimeError(f"Unsupported or unverified aggregation {m_agg!r} for measure {m_name!r}.")

        s4_select_clause = ", ".join(s4_select_parts)
        s4_where_clause = f" WHERE {custom_filters}" if custom_filters else ""
        
        # 1. Primary Replicated CDS View Query (runs on ZI_SalesKPIBase etc.)
        s4_sql = f"SELECT {s4_select_clause} FROM {primary_source}{s4_where_clause} GROUP BY {key_dim} ORDER BY {key_dim}"

        # 2. Decomposed Source Lineage Joined Query (derived from ADT joins)
        s4_decomposed_sql = s4_sql
        if adt_sources and adt_joins:
            base_src = adt_sources[0]
            base_alias = base_src.get("alias", "base")
            join_clauses = []
            for j in adt_joins:
                j_type = str(j.get("join_type", "inner")).upper()
                if "OUTER" not in j_type and "JOIN" not in j_type:
                    j_type = f"{j_type} JOIN"
                join_clauses.append(f"  {j_type} {j.get('entity')} AS {j.get('alias')}\n    ON {j.get('condition')}")
            
            joins_block = "\n".join(join_clauses)
            s4_decomposed_sql = (
                f"SELECT\n"
                f"  {base_alias}.{key_dim},\n"
                f"  COUNT( * ) AS RecordCount,\n"
                f"  " + ",\n  ".join([f"{m.get('aggregation', 'SUM')}( {base_alias}.{m['name']} ) AS Total{m['name']}" for m in measures[:3]]) + "\n"
                f"FROM {base_src.get('entity')} AS {base_alias}\n"
                f"{joins_block}\n"
                f"{s4_where_clause}\n"
                f"GROUP BY {base_alias}.{key_dim}\n"
                f"ORDER BY {base_alias}.{key_dim}"
            )

        # 3. Equivalent Datasphere Analytical Model Query
        ds_sql = f'SELECT {s4_select_clause} FROM "{resolution["space_id"]}"."{model_name}"{s4_where_clause} GROUP BY {key_dim} ORDER BY {key_dim}'

        # Human-readable metadata explanation
        explanation = (
            f"Analytical Model '{model_name}' in Datasphere Space '{resolution['space_id']}' "
            f"uses the verified S/4HANA CDS source '{primary_source}'.\n\n"
            f"Lineage & Verification Summary:\n"
            f"1. Modeling Space: [{resolution['space_id']}].\n"
            f"2. Matched Eclipse CDS View: [{primary_source}] (verified by live ADT metadata).\n"
            f"3. Underlying Base Joins Discovered: {len(adt_joins)} relational join conditions.\n"
            f"4. Grain / Grouping: Aggregated across dimension '{key_dim}'.\n"
            f"5. Security: Zero-Data-Leakage Mode active. Queries execute locally in private memory; no business rows are sent to LLMs."
        )

        # ----------------------------------------------------------------------
        # Determine Model Joins Metadata
        # ----------------------------------------------------------------------
        joins_metadata = []
        if adt_joins:
            for j in adt_joins:
                joins_metadata.append({
                    "left_table": primary_source,
                    "right_table": j.get("entity"),
                    "alias": j.get("alias"),
                    "join_type": f"{str(j.get('join_type', 'INNER')).upper()} JOIN",
                    "condition": j.get("condition"),
                    "cardinality": "N:1",
                    "description": f"Underlying CDS join between {primary_source} and {j.get('entity')} ({j.get('alias')})."
                })
        # An empty list means no join definition was found in the metadata;
        # it must not be replaced with a fabricated join/relationship.

        # ----------------------------------------------------------------------
        # Determine Model Filters Metadata
        # ----------------------------------------------------------------------
        filters_metadata = []
        if custom_filters:
            filters_metadata.append({
                "field": "Custom User Filter",
                "operator": "WHERE",
                "value": custom_filters,
                "type": "Runtime Filter",
                "description": "User-supplied filter applied across both S/4HANA CDS and Datasphere queries."
            })
        # No filter is reported unless it is present in model metadata or was
        # explicitly supplied by the user.

        approval_token = str(uuid.uuid4())

        return {
            "approval_token": approval_token,
            "model_name": model_name,
            "space_id": resolution["space_id"],
            "matched_source_cds": primary_source,
            "reconciliation_dimension": key_dim,
            "measures_evaluated": measures,
            "dimensions_evaluated": dimensions,
            "joins_metadata": joins_metadata,
            "filters_metadata": filters_metadata,
            "lineage_chain": resolution.get("lineage_chain", []),
            "replication_verification": resolution.get("replication_verification", {}),
            "generated_s4_sql": s4_sql,
            "generated_s4_decomposed_sql": s4_decomposed_sql,
            "generated_datasphere_sql": ds_sql,
            "sql_validation": {
                "validation_status": "REVIEW_REQUIRED",
                "validation_message": "Generated from metadata; syntax and transformation equivalence still require review and execution against the target systems.",
                "select_clause": s4_select_clause,
                "from_clause": primary_source,
                "where_clause": custom_filters or "ALL (No active filter)",
                "group_by_clause": key_dim,
                "order_by_clause": key_dim
            },
            "metadata_explanation": explanation,
            "security": {
                "zero_data_leakage_enforced": True,
                "raw_business_data_sent_to_llm": False,
                "human_approval_required": True
            },
            "status": "AWAITING_HUMAN_APPROVAL"
        }

    # ==========================================================================
    # PHASE 3: DUAL EXECUTION & ZERO-DATA-LEAKAGE RECONCILIATION
    # ==========================================================================
    def execute_and_reconcile(
        self,
        plan: Dict[str, Any],
        human_approved: bool = False
    ) -> Dict[str, Any]:
        """
        Executes both queries:
        1. On S/4HANA CDS view via ADT DataRetriever
        2. On Datasphere Analytical Model via HANA / Simulated Model Engine
        Compares results in local memory and outputs the Data Quality Report.
        """
        if not human_approved:
            raise PermissionError("Human approval is required before executing reconciliation queries on SAP systems.")

        t_start = time.time()
        model_name = plan["model_name"]
        space_id = plan["space_id"]
        source_cds = plan["matched_source_cds"]
        key_dim = plan["reconciliation_dimension"]
        s4_sql = plan["generated_s4_sql"]
        measures = plan["measures_evaluated"]

        logger.info(f"Executing reconciliation for Model '{model_name}' against S/4HANA CDS '{source_cds}'...")

        # ----------------------------------------------------------------------
        # 1. Execute against S/4HANA via ADT
        # ----------------------------------------------------------------------
        s4_rows = []
        s4_total_rows = 0
        s4_latency = 0.0
        s4_error = None

        try:
            retriever = DataRetriever(max_row_limit=self.default_row_limit)
            s4_result = retriever.fetch_query(query=s4_sql, row_limit=self.default_row_limit)
            s4_rows = s4_result.get("rows", [])
            s4_total_rows = s4_result.get("total_rows", len(s4_rows))
            s4_latency = float(s4_result.get("execution_time", 0.0) or 0.0)
            retriever.close()
            logger.info(f"S/4HANA ADT execution succeeded: {len(s4_rows)} grouped rows returned.")
        except Exception as e:
            s4_error = str(e)
            logger.error(f"S/4HANA ADT execution error: {e}")
            s4_rows = []
            s4_total_rows = 0

        # ----------------------------------------------------------------------
        # 2. Execute against Datasphere Analytical Model
        # ----------------------------------------------------------------------
        ds_rows = []
        ds_total_rows = 0
        ds_latency = 0.0
        ds_error = None

        try:
            from hana_client import execute_direct_hana_query
            ds_result, ds_latency, _ = execute_direct_hana_query(plan["generated_datasphere_sql"], target_space)
            ds_rows = ds_result
            ds_total_rows = len(ds_rows)
            logger.info(f"Datasphere HANA execution succeeded: {len(ds_rows)} rows returned.")
        except Exception as e:
            ds_error = str(e)
            logger.error("Datasphere query execution failed; refusing to simulate or copy S/4HANA rows: %s", e)
            ds_rows = []
            ds_total_rows = 0

        # Never compare incomplete or simulated results. A reconciliation report
        # can only be PASS/FAIL after both independent executions succeed.
        if s4_error or ds_error:
            report = {
                "report_id": f"DQR-{int(time.time())}-{uuid.uuid4().hex[:6].upper()}",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
                "execution_duration_sec": round(time.time() - t_start, 3),
                "model_name": model_name,
                "space_id": space_id,
                "source_cds_view": source_cds,
                "reconciliation_grain": key_dim,
                "overall_status": "INDETERMINATE",
                "quality_score_pct": None,
                "summary": {"reason": "Both source executions must succeed before comparison.", "s4hana_rows": len(s4_rows), "datasphere_rows": len(ds_rows)},
                "execution_errors": {"s4hana": s4_error, "datasphere": ds_error},
                "security_governance": {
                    "zero_business_data_sent_to_llm": True,
                    "executed_privately": True,
                    "human_approval_timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
                },
                "executed_queries": {
                    "s4hana_open_sql": s4_sql,
                    "datasphere_sql": plan["generated_datasphere_sql"],
                    "s4_latency_sec": round(s4_latency, 3),
                    "datasphere_latency_sec": round(ds_latency, 3),
                },
            }
            self.reports_history.append(report)
            return report

        # ----------------------------------------------------------------------
        # 3. Macro & Key-Level Reconciliation Analysis (Zero Data Leakage)
        # ----------------------------------------------------------------------
        elapsed = time.time() - t_start

        def get_val(row: dict, k: str) -> Any:
            if not isinstance(row, dict):
                return None
            for candidate in (k, k.upper(), k.lower(), k.capitalize()):
                if candidate in row and row[candidate] is not None:
                    return row[candidate]
            return None

        # Normalize keys for matching
        s4_map = {str(get_val(r, key_dim) or "").strip(): r for r in s4_rows if get_val(r, key_dim) is not None}
        ds_map = {str(get_val(r, key_dim) or "").strip(): r for r in ds_rows if get_val(r, key_dim) is not None}
        s4_map.pop("", None)
        ds_map.pop("", None)

        all_keys = sorted(list(set(s4_map.keys()) | set(ds_map.keys())))
        matched_keys = [k for k in all_keys if k in s4_map and k in ds_map]
        only_in_s4 = [k for k in all_keys if k in s4_map and k not in ds_map]
        only_in_ds = [k for k in all_keys if k not in s4_map and k in ds_map]

        # Key-level comparisons
        key_comparisons = []
        macro_deltas = {}

        # Initialize measure totals
        s4_kpi_totals = {f"Total{m['name']}": 0.0 for m in measures}
        ds_kpi_totals = {f"Total{m['name']}": 0.0 for m in measures}
        s4_record_total = 0
        ds_record_total = 0

        for k in all_keys:
            s4_r = s4_map.get(k, {})
            ds_r = ds_map.get(k, {})

            s4_cnt = int(self._parse_float(get_val(s4_r, "RecordCount") or 0))
            ds_cnt = int(self._parse_float(get_val(ds_r, "RecordCount") or 0))
            s4_record_total += s4_cnt
            ds_record_total += ds_cnt

            row_metrics = {}
            for m in measures:
                field = f"Total{m['name']}"
                v_s4 = self._parse_float(get_val(s4_r, field))
                v_ds = self._parse_float(get_val(ds_r, field))
                s4_kpi_totals[field] += v_s4
                ds_kpi_totals[field] += v_ds

                delta = round(v_s4 - v_ds, 2)
                variance_pct = round((abs(delta) / v_s4 * 100), 2) if v_s4 > 0 else (0.0 if delta == 0 else 100.0)

                row_metrics[m["name"]] = {
                    "s4_value": v_s4,
                    "datasphere_value": v_ds,
                    "delta": delta,
                    "variance_pct": variance_pct,
                    "status": "MATCH" if delta == 0 else ("MINOR_DIFF" if variance_pct < 2.0 else "DISCREPANCY")
                }

            key_comparisons.append({
                "key": k,
                "s4_count": s4_cnt,
                "datasphere_count": ds_cnt,
                "count_delta": s4_cnt - ds_cnt,
                "metrics": row_metrics,
                "status": "PERFECT_MATCH" if s4_cnt == ds_cnt and all(rm["delta"] == 0 for rm in row_metrics.values()) else "VARIANCE_DETECTED"
            })

        # Calculate Macro Deltas & KPI Score
        kpi_summaries = []
        total_variances = []

        for m in measures:
            field = f"Total{m['name']}"
            s4_tot = round(s4_kpi_totals[field], 2)
            ds_tot = round(ds_kpi_totals[field], 2)
            kpi_delta = round(s4_tot - ds_tot, 2)
            var_pct = round((abs(kpi_delta) / s4_tot * 100), 2) if s4_tot > 0 else (0.0 if kpi_delta == 0 else 100.0)
            total_variances.append(var_pct)

            kpi_summaries.append({
                "measure_name": m["name"],
                "aggregation": m.get("aggregation", "SUM"),
                "s4_total": s4_tot,
                "datasphere_total": ds_tot,
                "delta": kpi_delta,
                "variance_pct": var_pct,
                "status": "MATCH" if kpi_delta == 0 else ("WARNING" if var_pct < 3.0 else "FAIL")
            })

        # Overall Data Quality Score & Strict Zero Validation
        count_match_rate = round((len(matched_keys) / len(all_keys) * 100), 1) if all_keys else 100.0
        avg_variance = sum(total_variances) / len(total_variances) if total_variances else 0.0
        quality_score = max(0.0, min(100.0, round(100.0 - avg_variance * 2.0, 1)))

        # Never grant PASS on matching zeroes or empty results
        zero_kpi_detected = False
        for m in measures:
            field = f"Total{m['name']}"
            if s4_kpi_totals.get(field, 0.0) == 0.0 and ds_kpi_totals.get(field, 0.0) == 0.0:
                zero_kpi_detected = True
                break

        if len(all_keys) == 0 or s4_total_rows == 0:
            overall_status = "FAIL"
            quality_score = 0.0
        elif zero_kpi_detected:
            overall_status = "INDETERMINATE"
            quality_score = 50.0
        else:
            overall_status = "PASS" if quality_score >= 95.0 else ("WARNING" if quality_score >= 80.0 else "FAIL")

        # ----------------------------------------------------------------------
        # 4. Generate Data Quality Report
        # ----------------------------------------------------------------------
        report_id = f"DQR-{int(time.time())}-{uuid.uuid4().hex[:6].upper()}"

        report = {
            "report_id": report_id,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
            "execution_duration_sec": round(elapsed, 3),
            "model_name": model_name,
            "space_id": space_id,
            "source_cds_view": source_cds,
            "reconciliation_grain": key_dim,
            "overall_status": overall_status,
            "quality_score_pct": quality_score,
            "summary": {
                "total_distinct_keys": len(all_keys),
                "matched_keys_count": len(matched_keys),
                "unmatched_s4_only": len(only_in_s4),
                "unmatched_datasphere_only": len(only_in_ds),
                "key_match_rate_pct": count_match_rate,
                "s4_total_records": s4_record_total,
                "datasphere_total_records": ds_record_total,
                "record_count_delta": s4_record_total - ds_record_total
            },
            "kpi_metrics_reconciliation": kpi_summaries,
            "key_level_breakdown": key_comparisons[:50],  # Return up to 50 key groups for display
            "security_governance": {
                "zero_business_data_sent_to_llm": True,
                "executed_privately": True,
                "human_approval_timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
                "compliance": "SAP Zero Data Leakage Standard"
            },
            "executed_queries": {
                "s4hana_open_sql": s4_sql,
                "datasphere_sql": plan["generated_datasphere_sql"],
                "s4_latency_sec": round(s4_latency, 3),
                "datasphere_latency_sec": round(ds_latency, 3)
            }
        }

        self.reports_history.append(report)
        return report

    # --------------------------------------------------------------------------
    # Helper simulation & parsing routines
    # --------------------------------------------------------------------------
    @staticmethod
    def _parse_float(val: Any) -> float:
        if val is None:
            return 0.0
        try:
            return float(str(val).strip().replace(",", ""))
        except (ValueError, TypeError):
            return 0.0

