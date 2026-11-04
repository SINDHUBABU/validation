"""
Validation Service: Integrates SAP Datasphere and SAP S/4HANA CDS
Validates that Datasphere analytical models accurately reflect underlying S/4HANA source CDS views.

Constraints Enforced:
1. No data leakage to LLMs (metadata only).
2. No hardcoding (CSN lineage dynamic resolution).
3. Validation uses direct source execution; explicit CDS staging is a separate user action.
4. The target Datasphere view is supplied directly by the user.
"""

import os
import sys
from pathlib import Path

# Ensure backend and subdirectories are always in sys.path
_cur_dir = str(Path(__file__).resolve().parent)
_rec_dir = str(Path(__file__).resolve().parent / "reconciliation")
_root_dir = str(Path(__file__).resolve().parent.parent)
for _p in (_cur_dir, _rec_dir, _root_dir):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import time
import logging
import json
import hashlib as _hashlib
import re
import ast
import asyncio
from decimal import Decimal, InvalidOperation
from typing import Dict, Any, List, Optional, Tuple
from fastapi import HTTPException

from config import (
    logger, DATASPHERE_SPACE_ID, DATASPHERE_API_URL, HANA_HOST, HANA_PORT,
    USE_DIRECT_HANA, USE_SAP_ADT, SAP_BASE_URL, SAP_USERNAME, SAP_PASSWORD,
    GEMINI_API_KEY, OPENAI_API_KEY, API_KEY, ADT_ROW_LIMIT,
)
from schemas import ValidationRequest, ValidationResponse
from hana_client import (
    discover_hana_execution_object_sync,
    execute_direct_hana_query,
    probe_hana_columns_sync,
)
from llm_client import (
    _expand_calculated_field_dependencies,
    _normalize_expression_preserving_literals,
    generate_odata_query_blueprint,
)
from config import get_config
from reconciliation.dependency_resolver import DatasphereDependencyResolver
from reconciliation.datasphere_resolver_client import DatasphereResolverClient
from reconciliation.datasphere_space_cache import metadata_cache_ttl_seconds
from reconciliation.execution_target import (
    ExecutionTargetError,
    ReconciliationGrainError,
    bind_readonly_sql_to_target,
    compile_reconciliation_sql,
    resolve_reconciliation_grain,
    resolve_execution_target,
)
from reconciliation.manual_sql import validate_manual_select
from llm_client import generate_reconciliation_query_blueprint
from adt.cds_metadata import CDSMetadataClient

_FACT_QUERY_PLAN_CACHE: Dict[str, Dict[str, Any]] = {}

# Import reconciliation engine components
try:
    from reconciliation.reconciliation_engine import ReconciliationEngine
    from reconciliation.datasphere_space_cache import DatasphereSpaceCache
    from retrieval.data_retriever import DataRetriever
except ImportError:
    try:
        from backend.reconciliation.reconciliation_engine import ReconciliationEngine
        from backend.reconciliation.datasphere_space_cache import DatasphereSpaceCache
        from backend.retrieval.data_retriever import DataRetriever
    except ImportError:
        try:
            from reconciliation_engine import ReconciliationEngine
            # pyrefly: ignore[missing-import]
            from datasphere_space_cache import DatasphereSpaceCache
            # pyrefly: ignore[missing-import]
            from data_retriever import DataRetriever
        except ImportError as e:
            logger.warning(f"Reconciliation engine import warning: {e}")
            ReconciliationEngine = None
            DatasphereSpaceCache = None
            DataRetriever = None


def extract_filter_from_prompt(user_prompt: str, available_columns: List[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Extracts simple SQL filters and target metric from user prompt without needing an external LLM,
    or falls back to LLM blueprint if complex.
    Zero data leakage: operates purely on column names.
    """
    p_lower = user_prompt.lower()
    filters = []

    # Year detection (e.g. "this year", "2024", "2023", "2025")
    year_match = re.search(r'\b(20[2-3][0-9])\b', user_prompt)
    if year_match:
        year_val = year_match.group(1)
        year_col = next((c for c in available_columns if any(k in c.lower() for k in ("fiscalyear", "year", "gjahr"))), None)
        if year_col:
            filters.append(f"{year_col} = '{year_val}'")
    elif "this year" in p_lower:
        year_col = next((c for c in available_columns if any(k in c.lower() for k in ("fiscalyear", "year", "gjahr"))), None)
        if year_col:
            filters.append(f"{year_col} = '2024'")

    # Customer detection (e.g. "customer 10001" or "customer US-101")
    cust_match = re.search(r'\bcustomer\s+([a-zA-Z0-9_\-]+)\b', user_prompt, re.IGNORECASE)
    if cust_match:
        cust_val = cust_match.group(1)
        cust_col = next((c for c in available_columns if any(k in c.lower() for k in ("customer", "kunnr", "customernumber"))), None)
        if cust_col:
            filters.append(f"{cust_col} = '{cust_val}'")

    # Company code detection (e.g. "company 1010" or "company code 1010")
    cc_match = re.search(r'\bcompany(?:\s+code)?\s+([a-zA-Z0-9_\-]+)\b', user_prompt, re.IGNORECASE)
    if cc_match:
        cc_val = cc_match.group(1)
        cc_col = next((c for c in available_columns if any(k in c.lower() for k in ("companycode", "bukrs"))), None)
        if cc_col:
            filters.append(f"{cc_col} = '{cc_val}'")

    # Target metric detection
    target_metric = None
    if any(k in p_lower for k in ("revenue", "sales", "gross", "income", "amount", "price", "net", "quantity")):
        target_metric = next(
            (c for c in available_columns 
             if any(k in c.lower() for k in ("revenue", "netamount", "grossamount", "totalamount", "totalnetamount", "amount", "price", "val", "quantity", "orderquantity"))
             and not any(dim in c.lower() for dim in ("org", "code", "group", "type", "id", "district", "customer", "material", "office", "item", "order"))),
            None
        )
    elif "quantity" in p_lower or "count" in p_lower:
        target_metric = next(
            (c for c in available_columns 
             if any(k in c.lower() for k in ("quantity", "orderquantity", "itemcount", "count"))
             and not any(dim in c.lower() for dim in ("org", "code", "group", "type", "id"))),
            None
        )

    combined_filter = " AND ".join(filters) if filters else None
    return combined_filter, target_metric


def build_cds_measure_expression(
    view_name: str,
    source_cds: str,
    metric: str,
    calculated_formulas: Optional[Dict[str, str]] = None,
) -> str:
    """
    Dynamically generates the Open SQL calculation expression for the measure on the underlying S/4HANA CDS view.
    Zero hardcoding of specific business domains: Uses explicit formulas or column aggregation.
    """
    m_clean = metric.strip()
    if calculated_formulas:
        for k, v in calculated_formulas.items():
            if k.strip().lower() == m_clean.lower():
                formula = v.strip()
                if formula.upper().startswith("SUM(") or formula.upper().startswith("COUNT("):
                    return formula
                if any(formula.upper().startswith(kw) for kw in ("CASE", "CAST")):
                    return f"SUM( CAST( {formula} AS FLTP ) )"
                return f"SUM( CAST( {formula} AS FLTP ) )"

    # Default SQL aggregation on column
    return f"SUM( CAST( {m_clean} AS FLTP ) )"




def eval_row_formula(expr: str, row: Dict[str, Any]) -> Any:
    """
    Evaluate a row-level CDS formula supplied by the user without calling an LLM.
    Supports field references, arithmetic, comparisons, BETWEEN, AND/OR, and
    simple CASE WHEN expressions.
    """
    if not expr or not expr.strip():
        return None
    clean = expr.strip()
    for key in (clean, clean.upper(), clean.lower()):
        if key in row:
            return row[key]

    case_match = re.fullmatch(r"CASE\s+WHEN\s+(.+?)\s+THEN\s+(.+?)\s+ELSE\s+(.+?)\s+END", clean, re.IGNORECASE)
    if case_match:
        condition, when_true, when_false = case_match.groups()
        clean = f"({when_true} if ({condition}) else {when_false})"

    clean = re.sub(
        r"\b([A-Za-z_][A-Za-z0-9_]*)\s+BETWEEN\s+(-?\d+(?:\.\d+)?)\s+AND\s+(-?\d+(?:\.\d+)?)",
        r"(\1 >= \2 and \1 <= \3)", clean, flags=re.IGNORECASE
    )
    clean = re.sub(r"<>|!=", "!=", clean)
    clean = re.sub(r"(?<![<>=!])=(?!=)", "==", clean)
    clean = re.sub(r"\bAND\b", "and", clean, flags=re.IGNORECASE)
    clean = re.sub(r"\bOR\b", "or", clean, flags=re.IGNORECASE)

    def lookup(name):
        for key in (name, name.upper(), name.lower()):
            if key in row:
                return row[key]
        return None

    env = {}
    tokens = set(re.findall(r"\b[A-Za-z_][A-Za-z0-9_]*\b", clean))
    reserved = {"and", "or", "not", "if", "else", "True", "False", "None"}
    for token in tokens - reserved:
        value = lookup(token)
        if value is None:
            return None
        env[token] = value
    try:
        tree = ast.parse(clean, mode="eval")
        allowed = (ast.Expression, ast.Constant, ast.Name, ast.Load, ast.BinOp, ast.UnaryOp, ast.BoolOp, ast.Compare, ast.IfExp,
                   ast.Add, ast.Sub, ast.Mult, ast.Div, ast.FloorDiv, ast.Mod, ast.Pow, ast.USub, ast.UAdd, ast.Not,
                   ast.And, ast.Or, ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE)
        if any(not isinstance(node, allowed) for node in ast.walk(tree)):
            return None
        result = eval(compile(tree, "<user-formula>", "eval"), {"__builtins__": {}}, env)
        if isinstance(result, bool):
            return int(result)
        return round(float(result), 6) if isinstance(result, (int, float)) else result
    except Exception:
        return None


def parse_calculated_field_formulas(
    formula_prompt: Optional[str],
    target_metrics: List[str],
    view_name: str,
    source_cds: str,
    explicit_formulas: Optional[Dict[str, str]] = None
) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Parses user-specified prompt or formula for calculated fields at the CDS side.
    Returns:
      expressions: {metric_name: open_sql_expression}
      explanations: {metric_name: human_readable_formula}
    """
    expressions: Dict[str, str] = {}
    explanations: Dict[str, str] = {}

    user_rules: Dict[str, str] = {}
    if explicit_formulas:
        for k, v in explicit_formulas.items():
            if v and v.strip():
                user_rules[k.strip().lower()] = v.strip()

    if formula_prompt and formula_prompt.strip():
        parts = re.split(r'[;\n]+', formula_prompt.strip())
        for part in parts:
            part = part.strip()
            if not part:
                continue
            if ":" in part:
                k, v = part.split(":", 1)
                user_rules[k.strip().lower()] = v.strip()
            elif "=" in part and not any(part.strip().upper().startswith(kw) for kw in ("CASE", "WHEN", "SUM")):
                k, v = part.split("=", 1)
                user_rules[k.strip().lower()] = v.strip()
            else:
                if target_metrics:
                    user_rules[target_metrics[0].lower()] = part

    for metric in target_metrics:
        m_lower = metric.lower()
        if m_lower in user_rules:
            rule = user_rules[m_lower]
            rule_clean = rule.strip()
            if rule_clean.upper().startswith("SUM(") or rule_clean.upper().startswith("COUNT("):
                expr = rule_clean
            elif "CASE" in rule_clean.upper() and "THEN" in rule_clean.upper():
                expr = f"SUM( CAST( {rule_clean} AS FLTP ) )"
            elif any(kw in rule_clean.upper() for kw in ("BETWEEN", ">", "<", "=", "LIKE", "AND", "OR")):
                expr = f"SUM( CAST( CASE WHEN {rule_clean} THEN 1 ELSE 0 END AS FLTP ) )"
            else:
                expr = f"SUM( CAST( {rule_clean} AS FLTP ) )"
            expressions[metric] = expr
            explanations[metric] = f"User Formula: {rule_clean}"
        else:
            expr = build_cds_measure_expression(view_name, source_cds, metric)
            expressions[metric] = expr
            explanations[metric] = "Standard CDS Projection / Business Rule"

    return expressions, explanations


async def run_data_validation(request: ValidationRequest) -> ValidationResponse:
    """
    Dynamic Zero-Hardcoding End-to-End Data Validation:
    1. Retrieves all fields of the Analytical Model (or user-selected fields).
    2. Maps Analytical Model -> S/4HANA CDS View.
    3. Fetches all available Datasphere model pages (Direct HANA or OData REST).
    4. Fetches live S/4HANA CDS data and applies custom calculated field formulas.
    5. MATCHES records strictly by KEY FIELD (e.g. Product / ID / primary key), never by random row index.
    6. Directly validates every selected field side-by-side:
          [ field name ]
       datasphere | s/4 hana cds
         value    |    value
    """
    view_name = request.view_name.strip()
    space_id = request.space_id.strip() if request.space_id else DATASPHERE_SPACE_ID
    user_prompt = request.user_prompt.strip()

    logger.info(f"Starting Key-Matched Data Validation for View [{view_name}] in Space [{space_id}] | Prompt: '{user_prompt}'")

    # 1. Discover live measures & dimensions from Datasphere EDMX
    from datasphere_client import get_datasphere_token, get_tenant_base_host, fetch_view_edmx_schema, fetch_single_view_data
    bearer_token = await get_datasphere_token()
    base_host = get_tenant_base_host(DATASPHERE_API_URL)

    edmx_res = await fetch_view_edmx_schema(base_host, space_id, view_name, bearer_token)
    discovered_measures = [m["name"] for m in edmx_res.get("measures", [])]
    dimension_items = edmx_res.get("dimensions", [])
    discovered_dimensions = [d["name"] for d in dimension_items]
    all_discovered_fields = discovered_measures + discovered_dimensions
    if not all_discovered_fields:
        from datasphere_client import probe_view_columns
        all_discovered_fields = await probe_view_columns(base_host, space_id, view_name, bearer_token)

    # The raw source must be selected from actual lineage and confirmed by the user.
    source_cds_view = (request.source_cds_view or "").strip()
    if not source_cds_view:
        raise HTTPException(status_code=400, detail="Provide the raw S/4HANA CDS source view from the view's lineage before validation.")

    # 3. Determine selected fields (Unified all-fields selection requested by user)
    selected_fields: List[str] = []
    if request.selected_fields and len(request.selected_fields) > 0:
        selected_fields = [f.strip() for f in request.selected_fields if f and f.strip()]
    elif request.selected_metrics and len(request.selected_metrics) > 0:
        selected_fields = [m.strip() for m in request.selected_metrics if m and m.strip()]
    
    if not selected_fields and all_discovered_fields:
        selected_fields = all_discovered_fields
    elif not selected_fields:
        raise HTTPException(status_code=400, detail="Datasphere did not return its final projected fields. Select the fields to validate.")

    # Deduplicate while preserving order
    seen_f = set()
    target_fields = []
    for f in selected_fields:
        if f.lower() not in seen_f:
            seen_f.add(f.lower())
            target_fields.append(f)

    key_fields = [str(value).strip() for value in (request.key_fields or []) if str(value).strip()]
    if not key_fields and request.reconciliation_dimension and request.reconciliation_dimension.strip():
        key_fields = [request.reconciliation_dimension.strip()]
    if not key_fields:
        key_fields = [str(d["name"]) for d in dimension_items if d.get("is_key") and d.get("name")]
    key_fields = list(dict.fromkeys(key_fields))
    if not key_fields:
        raise HTTPException(status_code=400, detail="No key fields were supplied or annotated by Datasphere. Select every field that uniquely identifies each final row.")
    available_lower = {field.lower() for field in all_discovered_fields}
    missing_keys = [field for field in key_fields if field.lower() not in available_lower]
    if missing_keys:
        raise HTTPException(status_code=400, detail=f"Key fields are not projected by the selected Datasphere view: {', '.join(missing_keys)}")
    logger.info(f"Using key fields for row reconciliation: {key_fields}")

    lineage_chain = []

    # Combine formulas for calculated fields
    combined_formulas: Dict[str, str] = {}
    if request.calculated_field_formulas:
        for k, v in request.calculated_field_formulas.items():
            if v and v.strip():
                combined_formulas[k.strip().lower()] = v.strip()
    if request.formula_prompt and request.formula_prompt.strip():
        parts = re.split(r'[;\n]+', request.formula_prompt.strip())
        for part in parts:
            part = part.strip()
            if ":" in part:
                k, v = part.split(":", 1)
                combined_formulas[k.strip().lower()] = v.strip()
            elif "=" in part:
                k, v = part.split("=", 1)
                combined_formulas[k.strip().lower()] = v.strip()

    # 4. Fetch every Datasphere row. Keys are included for matching but are
    # compared only when they are among the final fields selected above.
    ds_raw_rows = []
    ds_latency = 0.0
    ds_error = None
    ds_execution_engine = "SAP HANA Cloud Direct SQL (Database Explorer)"

    query_fields = list(dict.fromkeys(target_fields + key_fields))
    ds_sql = f'SELECT {", ".join([f"\"{f}\"" for f in query_fields])} FROM "{space_id}"."{view_name}"'
    s4_sql = f"SELECT * FROM {source_cds_view}"

    if USE_DIRECT_HANA:
        try:
            ds_raw_rows, ds_latency, _ = execute_direct_hana_query(ds_sql, space_id, user_prompt=user_prompt)
            logger.info(f"HANA Cloud Direct Query succeeded: {len(ds_raw_rows)} rows in {ds_latency:.2f}s")
        except Exception as he:
            ds_error = str(he)
            logger.warning(f"HANA Cloud direct query unavailable ({he}). Executing automatic fallback to Datasphere Consumption OData REST API...")

    if not ds_raw_rows:
        try:
            t0 = time.time()
            # Continue through every OData page; $top controls page size only.
            odata_params: Dict[str, str] = {"$top": "1000", "$count": "true"}
            odata_res = await fetch_single_view_data(view_name, space_id, bearer_token, base_host, odata_params)
            if odata_res.get("success") and odata_res.get("data"):
                ds_raw_rows = odata_res["data"]
                ds_latency = round(time.time() - t0, 3)
                ds_execution_engine = "SAP Datasphere (Consumption OData REST API • Auto-Fallback)"
                ds_error = None
                logger.info(f"Datasphere Consumption OData fallback succeeded: {len(ds_raw_rows)} live rows in {ds_latency:.2f}s")
        except Exception as oe:
            logger.warning(f"Datasphere Consumption OData fallback notice: {oe}")

    if not ds_raw_rows and not ds_error:
        ds_error = "Datasphere endpoint unreachable or returned 0 rows."

    # 5. Fetch all raw CDS rows using the selected reconciliation keys.
    s4_raw_rows = []
    s4_latency = 0.0
    s4_error = None

    if DataRetriever and USE_SAP_ADT:
        try:
            retriever = DataRetriever(default_row_limit=1000, max_row_limit=1000)
            s4_res = retriever.fetch_all(
                view_name=source_cds_view,
                fields=None,
                key_fields=key_fields,
                batch_size=1000,
                strict=True,
                verify_unique=True,
            )
            s4_raw_rows = s4_res.get("rows", [])
            s4_latency = float(s4_res.get("elapsed_seconds", 0.0) or 0.0)
            retriever.close()
            logger.info(f"S/4HANA ADT execution succeeded: {len(s4_raw_rows)} rows in {s4_latency:.2f}s")
        except Exception as ae:
            s4_error = str(ae)
            logger.warning(f"S/4HANA ADT execution notice: {ae}")
    else:
        s4_error = "SAP ADT connection credentials not configured or host unreachable."

    # 6. Apply Calculated Field formulas on S/4HANA rows
    s4_processed_rows = []
    for r in s4_raw_rows:
        row_copy = dict(r)
        for f in target_fields:
            f_low = f.lower()
            if f_low in combined_formulas:
                calc_val = eval_row_formula(combined_formulas[f_low], row_copy)
                row_copy[f] = calc_val
                row_copy[f.upper()] = calc_val
        s4_processed_rows.append(row_copy)

    # 7. Helper to extract case-insensitive value
    def get_field_val(row: dict, field_name: str) -> Any:
        if not row:
            return None
        for k in (field_name, field_name.upper(), field_name.lower(), field_name.capitalize()):
            if k in row:
                return row[k]
        return None

    def parse_numeric(v: Any) -> float:
        if v is None:
            return 0.0
        try:
            return float(str(v).strip())
        except (ValueError, TypeError):
            return 0.0

    # 8. Index CDS records using the full composite key. Never choose an
    # arbitrary first row when the selected key is not unique.
    def row_key(row: dict) -> Optional[Tuple[str, ...]]:
        values = [get_field_val(row, field) for field in key_fields]
        if any(value is None or str(value).strip() == "" for value in values):
            return None
        return tuple(str(value).strip() for value in values)

    s4_key_map: Dict[Tuple[str, ...], List[dict]] = {}
    for s4_row in s4_processed_rows:
        key = row_key(s4_row)
        if key is not None:
            s4_key_map.setdefault(key, []).append(s4_row)

    logger.info(f"Indexed {len(s4_key_map)} S/4HANA CDS key groups using {key_fields}")

    # 9. MATCH ROWS BY KEY (NEVER BY RANDOM ORDER)
    grain_breakdown = []
    field_totals = {f: {"ds": 0.0, "s4": 0.0, "is_numeric": True} for f in target_fields}
    s4_validated_rows = []

    datasphere_key_counts: Dict[Tuple[str, ...], int] = {}
    for ds_row in ds_raw_rows:
        ds_key = row_key(ds_row)
        if ds_key is not None:
            datasphere_key_counts[ds_key] = datasphere_key_counts.get(ds_key, 0) + 1

    for idx, ds_r in enumerate(ds_raw_rows):
        ds_key = row_key(ds_r)
        ds_k_val = " | ".join(ds_key) if ds_key else f"Missing key on Datasphere row {idx + 1}"
        s4_matches = s4_key_map.get(ds_key, []) if ds_key else []
        s4_r = s4_matches[0] if len(s4_matches) == 1 else {}
        if s4_r and datasphere_key_counts.get(ds_key, 0) == 1:
            s4_validated_rows.append(s4_r)
        grain_key = ds_k_val
        row_metrics = {}
        row_all_match = bool(s4_r) and datasphere_key_counts.get(ds_key, 0) == 1

        for f in target_fields:
            dv = get_field_val(ds_r, f)
            sv = get_field_val(s4_r, f) if s4_r else None

            # If field had a formula for CDS side, ensure it's evaluated
            if sv is None and f.lower() in combined_formulas and s4_r:
                sv = eval_row_formula(combined_formulas[f.lower()], s4_r)

            dv_num = parse_numeric(dv)
            sv_num = parse_numeric(sv)

            is_both_numeric = False
            try:
                if dv is not None and sv is not None and str(dv).strip() != "" and str(sv).strip() != "":
                    str_dv = str(dv).strip()
                    str_sv = str(sv).strip()
                    if f.lower() in {key.lower() for key in key_fields}:
                        is_both_numeric = False
                    else:
                        float(str_dv)
                        float(str_sv)
                        is_both_numeric = True
            except ValueError:
                is_both_numeric = False

            if not is_both_numeric:
                field_totals[f]["is_numeric"] = False
                str_d = str(dv or "").strip()
                str_s = str(sv or "").strip()
                f_match = str_d.lower() == str_s.lower()
                f_delta = 0.0 if f_match else 1.0
                f_var = 0.0 if f_match else 100.0
            else:
                f_delta = round(abs(dv_num - sv_num), 2)
                f_var = round((f_delta / sv_num * 100), 2) if sv_num > 0 else (0.0 if f_delta == 0 else 100.0)
                f_match = (f_delta == 0.0)
                field_totals[f]["ds"] += dv_num
                if s4_r:
                    field_totals[f]["s4"] += sv_num

            if not f_match:
                row_all_match = False

            row_metrics[f] = {
                "datasphere_kpi": dv if not is_both_numeric else dv_num,
                "s4hana_kpi": sv if not is_both_numeric else sv_num,
                "delta": f_delta,
                "variance_pct": f_var,
                "is_match": f_match
            }

        primary_f = target_fields[0]
        p_info = row_metrics.get(primary_f, {})

        grain_breakdown.append({
            "grain_key": grain_key,
            "datasphere_kpi": p_info.get("datasphere_kpi", 0.0) if isinstance(p_info.get("datasphere_kpi"), (int, float)) else 0.0,
            "s4hana_kpi": p_info.get("s4hana_kpi", 0.0) if isinstance(p_info.get("s4hana_kpi"), (int, float)) else 0.0,
            "delta": p_info.get("delta", 0.0),
            "variance_pct": p_info.get("variance_pct", 0.0),
            "datasphere_count": 1,
            "s4hana_count": 1 if s4_r else 0,
            "metrics": row_metrics,
            "fields": row_metrics,
            "status": "MATCH" if row_all_match else ("AMBIGUOUS_KEY" if ds_key and (datasphere_key_counts.get(ds_key, 0) > 1 or len(s4_matches) > 1) else "DISCREPANCY")
        })

    # 10. Metrics Comparison (Multi-field summary over evaluated records)
    metrics_comparison = []
    overall_is_match = all(r["status"] == "MATCH" for r in grain_breakdown) and len(grain_breakdown) > 0

    for f in target_fields:
        t_info = field_totals[f]
        if t_info["is_numeric"]:
            ds_tot = round(t_info["ds"], 2)
            s4_tot = round(t_info["s4"], 2)
            tot_delta = round(abs(ds_tot - s4_tot), 2)
            tot_var = round((tot_delta / s4_tot * 100), 2) if s4_tot > 0 else (0.0 if tot_delta == 0 else 100.0)
            f_is_match = (tot_delta == 0.0)
            fmt_ds = f"{ds_tot:,}"
            fmt_s4 = f"{s4_tot:,}"
        else:
            ds_tot = len([r for r in grain_breakdown if r["metrics"].get(f, {}).get("datasphere_kpi") not in (None, "", "-")])
            s4_tot = len([r for r in grain_breakdown if r["metrics"].get(f, {}).get("s4hana_kpi") not in (None, "", "-")])
            tot_delta = abs(ds_tot - s4_tot)
            tot_var = 0.0 if tot_delta == 0 else 100.0
            f_is_match = all(r["metrics"].get(f, {}).get("is_match", False) for r in grain_breakdown)
            fmt_ds = f"{ds_tot} values"
            fmt_s4 = f"{s4_tot} values"

        metrics_comparison.append({
            "metric_name": f,
            "aggregation": "SUM" if t_info["is_numeric"] else "COUNT",
            "datasphere_total": ds_tot,
            "formatted_datasphere": fmt_ds,
            "s4hana_total": s4_tot,
            "formatted_s4hana": fmt_s4,
            "delta": tot_delta,
            "variance_pct": tot_var,
            "is_match": f_is_match
        })

    primary_metric = target_fields[0]
    primary_comp = metrics_comparison[0]
    status = "VALIDATED" if (overall_is_match and not (ds_error or s4_error)) else ("DISCREPANCY_DETECTED" if not (ds_error or s4_error) else "ERROR")

    # Discover raw S/4HANA columns for metadata tab
    raw_cds_columns: List[str] = []
    if DataRetriever and USE_SAP_ADT:
        try:
            r = DataRetriever(max_row_limit=500)
            raw_cols = r.discover_columns(source_cds_view)
            raw_cds_columns = [c.get("name", "") if isinstance(c, dict) else str(c) for c in raw_cols]
            r.close()
        except Exception:
            pass

    field_projections = {
        "datasphere_view": view_name,
        "datasphere_fields": target_fields,
        "raw_cds_view": source_cds_view,
        "raw_cds_fields_count": len(raw_cds_columns),
        "raw_cds_fields": raw_cds_columns,
        "projected_fields": target_fields,
        "calculated_fields": combined_formulas,
        "filtered_out_count": max(0, len(raw_cds_columns) - len(target_fields)),
        "filtered_out_fields": [c for c in raw_cds_columns if c.lower() not in [f.lower() for f in target_fields]][:30]
    }

    match_count = sum(1 for r in grain_breakdown if r["status"] == "MATCH")
    summary_text = (
        f"Validation Successful: All {match_count} of {len(grain_breakdown)} evaluated records in Datasphere Model '{view_name}' "
        f"match S/4HANA CDS View '{source_cds_view}' across selected fields ({', '.join(target_fields)})."
        if overall_is_match and not (ds_error or s4_error) else
        f"Key-Matched Validation: {match_count} records match, {len(grain_breakdown) - match_count} discrepancies detected between Datasphere Model '{view_name}' and S/4HANA CDS '{source_cds_view}'."
    )

    return ValidationResponse(
        status=status,
        view_name=view_name,
        space_id=space_id,
        source_cds_view=source_cds_view,
        target_metric=primary_metric,
        target_metrics=target_fields,
        reconciliation_dimension=f"Keys: {', '.join(key_fields)}",
        user_prompt=user_prompt,
        datasphere={
            "view": view_name,
            "sql": ds_sql,
            "total_kpi": primary_comp["datasphere_total"],
            "formatted_kpi": primary_comp["formatted_datasphere"],
            "record_count": len(ds_raw_rows),
            "latency_seconds": round(ds_latency, 3),
            "execution_engine": ds_execution_engine,
            "data": ds_raw_rows,
            "error": ds_error
        },
        s4hana={
            "cds_view": source_cds_view,
            "sql": s4_sql,
            "total_kpi": primary_comp["s4hana_total"],
            "formatted_kpi": primary_comp["formatted_s4hana"],
            "record_count": len(s4_validated_rows),
            "latency_seconds": round(s4_latency, 3),
            "execution_engine": "SAP S/4HANA CDS Open SQL (Eclipse ADT)",
            "data": s4_validated_rows,
            "error": s4_error
        },
        comparison={
            "target_metric": primary_metric,
            "delta": primary_comp["delta"],
            "variance_pct": primary_comp["variance_pct"],
            "is_match": overall_is_match,
            "summary_text": summary_text,
            "security": "Zero-Data-Leakage Mode (Zero Rows Sent to LLM, In-Memory Validation)"
        },
        metrics_comparison=metrics_comparison,
        lineage_chain=lineage_chain,
        grain_breakdown=grain_breakdown,
        field_projections=field_projections,
        triage={
            "root_cause": "Key Field Value Differences or Replication Lag",
            "orphan_keys_count": len(grain_breakdown) - match_count,
            "orphan_sample": [r["grain_key"] for r in grain_breakdown if r["status"] != "MATCH"][:5],
            "cardinality_status": "Key-based parity evaluated across records",
            "action_recommended": "Verify S/4HANA replication status or check formula logic for calculated fields."
        } if not overall_is_match else None
    )


def _walk_dependency_nodes(node: Dict[str, Any]):
    yield node
    for child in node.get("dependencies", []):
        if isinstance(child, dict):
            yield from _walk_dependency_nodes(child)


def _validate_reconciliation_sql(
    sql: str,
    *,
    expected_sources: List[str],
    datasphere_model: Optional[str] = None,
    allowed_source_fields: Optional[List[str]] = None,
    source_field_map: Optional[Dict[str, List[str]]] = None,
    enforce_abap_line_limit: bool = False,
    enforce_entity_qualified_fields: bool = False,
) -> None:
    clean = sql.strip()
    if enforce_abap_line_limit:
        long_lines = [
            index for index, line in enumerate(clean.splitlines(), start=1)
            if len(line) > 255
        ]
        if long_lines:
            raise ValueError(
                "Generated ABAP Open SQL still exceeds the 255-character source-line "
                f"limit on line(s): {', '.join(map(str, long_lines))}."
            )
    without_literals = re.sub(r"'(?:''|[^'])*'", "''", clean)
    keyword_input = re.sub(r'"[^"]+"', '""', without_literals)
    if (
        not re.match(r"^SELECT\b", clean, re.IGNORECASE)
        or ";" in clean
        or "--" in clean
        or "/*" in clean
        or "*/" in clean
        or re.search(r'(?m)^\s*"(?:\s|$)|^\s*\*', clean)
        or re.search(
            r"\b(INSERT|UPDATE|DELETE|MODIFY|UPSERT|MERGE|DROP|ALTER|"
            r"CREATE|TRUNCATE|EXECUTE|CALL|DO|GRANT|REVOKE)\b",
            keyword_input,
            re.IGNORECASE,
        )
        or re.search(r"\bSELECT\s+\*", clean, re.IGNORECASE)
    ):
        raise ValueError("The generated statement is not an allowed single SELECT.")

    table_refs = re.findall(
        r"\b(?:FROM|JOIN)\s+(?:\"[^\"]+\"\.)?\"?([A-Za-z][A-Za-z0-9_.]*)\"?",
        clean,
        re.IGNORECASE,
    )
    normalized_refs = [ref.split(".")[-1].casefold() for ref in table_refs]
    if not normalized_refs:
        raise ValueError("The generated SELECT does not identify a source object.")

    if datasphere_model:
        if len(normalized_refs) != 1 or normalized_refs[0] != datasphere_model.casefold():
            raise ValueError("Datasphere SQL must read only the selected analytical model.")
    else:
        allowed = {name.casefold() for name in expected_sources}
        if not allowed or any(name not in allowed for name in normalized_refs):
            raise ValueError("S/4HANA SQL references a source not confirmed by Datasphere lineage.")
        if enforce_entity_qualified_fields:
            if re.search(
                r"\b(?:FROM|JOIN)\s+[A-Za-z][A-Za-z0-9_]*(?:\s*\([^)]*\))?\s+(?:AS\s+)?"
                r"(?!WHERE\b|ON\b|INNER\b|LEFT\b|RIGHT\b|FULL\b|CROSS\b|JOIN\b|GROUP\b|ORDER\b|HAVING\b)"
                r"[A-Za-z][A-Za-z0-9_]*\b",
                clean,
                re.IGNORECASE,
            ):
                raise ValueError(
                    "ABAP Open SQL source aliases are not allowed; qualify each field with its CDS entity name."
                )
            source_names = {str(name).casefold() for name in expected_sources}
            fields_by_source = {
                str(entity).casefold(): {str(field).casefold() for field in fields}
                for entity, fields in (source_field_map or {}).items()
            }
            for entity, field in re.findall(
                r"\b([A-Za-z][A-Za-z0-9_]*)~([A-Za-z][A-Za-z0-9_]*)\b",
                clean,
            ):
                if entity.casefold() not in source_names:
                    raise ValueError(
                        f"ABAP Open SQL field qualifier {entity!r} is not a verified CDS entity."
                    )
                if fields_by_source and field.casefold() not in fields_by_source.get(entity.casefold(), set()):
                    raise ValueError(
                        f"Field {field!r} is not present on verified CDS entity {entity!r}."
                    )
        if allowed_source_fields:
            without_literals = re.sub(r"'(?:''|[^'])*'", "''", clean)
            referenced_fields = re.findall(
                r"\b[A-Za-z][A-Za-z0-9_]*[.~]([A-Za-z][A-Za-z0-9_]*)",
                without_literals,
            )
            allowed_fields = {name.casefold() for name in allowed_source_fields}
            unknown_fields = [
                name for name in referenced_fields
                if name.casefold() not in allowed_fields
            ]
            if unknown_fields:
                raise ValueError(
                    "S/4HANA SQL references fields absent from the discovered CDS metadata: "
                    + ", ".join(dict.fromkeys(unknown_fields))
                )
            if enforce_entity_qualified_fields:
                field_alias_mask = re.sub(
                    r"\bAS\s+[A-Za-z][A-Za-z0-9_]*\b",
                    "",
                    without_literals,
                    flags=re.IGNORECASE,
                )
                for field_name in allowed_fields:
                    for match in re.finditer(
                        rf"(?<![A-Za-z0-9_]){re.escape(field_name)}(?![A-Za-z0-9_])",
                        field_alias_mask,
                        re.IGNORECASE,
                    ):
                        prefix = field_alias_mask[:match.start()]
                        if not re.search(
                            r"\b[A-Za-z][A-Za-z0-9_]*~$",
                            prefix,
                            re.IGNORECASE,
                        ):
                            raise ValueError(
                                f"ABAP Open SQL field {match.group(0)!r} must be qualified as ENTITY~FIELD."
                            )


def _format_open_sql_lines(sql: str) -> str:
    """Split long SELECT and GROUP BY lists without changing their expressions."""
    clean = sql.strip()
    select_match = re.match(r"(?is)^\s*SELECT\s+(.*?)\s+FROM\s+(.*)$", clean)
    if not select_match:
        return clean

    projections = _split_sql_expressions(select_match.group(1))
    tail = select_match.group(2).strip()
    group_match = re.search(
        r"(?is)\s+GROUP\s+BY\s+(.*?)(?=\s+(?:HAVING|ORDER\s+BY)\b|$)",
        tail,
    )
    if group_match:
        group_expressions = _split_sql_expressions(group_match.group(1))
        tail = (
            tail[:group_match.start()]
            + " GROUP BY\n    "
            + ",\n    ".join(group_expressions)
            + tail[group_match.end():]
        )
    return "SELECT\n    " + ",\n    ".join(projections) + "\nFROM " + tail


def _project_open_sql_grouping_expressions(sql: str) -> str:
    """Project every GROUP BY expression because ABAP SQL requires it in SELECT."""
    select_match = re.match(r"(?is)^\s*SELECT\s+(.*?)\s+FROM\s+(.*)$", sql.strip())
    if not select_match:
        return sql

    projections = _split_sql_expressions(select_match.group(1))
    tail = select_match.group(2).strip()
    group_match = re.search(
        r"(?is)\bGROUP\s+BY\s+(.*?)(?=\s+\b(?:HAVING|ORDER\s+BY)\b|$)",
        tail,
    )
    if not group_match:
        return sql

    group_expressions = _split_sql_expressions(group_match.group(1))
    selected_expressions = set()
    selected_aliases = set()
    for projection in projections:
        alias_match = re.search(
            r"(?is)\s+AS\s+([A-Za-z_][A-Za-z0-9_]*)\s*$",
            projection,
        )
        selected_aliases.add(alias_match.group(1).casefold() if alias_match else "")
        expression = (
            projection[:alias_match.start()].strip()
            if alias_match else projection
        )
        selected_expressions.add(re.sub(r"\s+", "", expression).casefold())

    for index, expression in enumerate(group_expressions, start=1):
        normalized = re.sub(r"\s+", "", expression).casefold()
        if normalized in selected_expressions:
            continue
        alias_index = index
        alias = f"DS_GROUP_{alias_index}"
        while alias.casefold() in selected_aliases:
            alias_index += 1
            alias = f"DS_GROUP_{alias_index}"
        projections.append(f"{expression} AS {alias}")
        selected_expressions.add(normalized)
        selected_aliases.add(alias.casefold())

    return "SELECT\n    " + ",\n    ".join(projections) + "\nFROM " + tail


def _source_parameter_overrides(
    all_overrides: Optional[Dict[str, Any]],
    entity: str,
    source_count: int,
) -> Dict[str, Any]:
    supplied = all_overrides or {}
    if not isinstance(supplied, dict):
        raise ValueError("CDS parameter values must be a JSON object.")
    nested = any(isinstance(value, dict) for value in supplied.values())
    if nested:
        if any(not isinstance(value, dict) for value in supplied.values()):
            raise ValueError("Use either a flat CDS parameter object or an entity-keyed object, not both.")
        match = next((value for name, value in supplied.items() if str(name).casefold() == entity.casefold()), None)
        return dict(match or {})
    if source_count > 1 and supplied:
        raise ValueError("With multiple CDS sources, provide parameters grouped by CDS entity name.")
    return dict(supplied)


def _resolve_declared_cds_parameters(
    source_metadata: Dict[str, Any],
    overrides: Dict[str, Any],
) -> Dict[str, Any]:
    """Resolve only explicit values and verified CDS annotations; never guess by parameter name."""
    from reconciliation.auto_query import SYSTEM_FIELD_RESOLVERS

    declarations = source_metadata.get("parameters") or []
    if not isinstance(declarations, list):
        raise ValueError("ADT returned malformed CDS parameter metadata.")
    if not declarations:
        if overrides:
            raise ValueError(
                f"CDS entity {source_metadata.get('entity')} declares no parameters, but parameter values were supplied."
            )
        return {}

    override_by_name = {str(name).casefold(): value for name, value in overrides.items()}
    declared_names = {str(parameter.get("name") or "").casefold() for parameter in declarations}
    unexpected = sorted(name for name in override_by_name if name not in declared_names)
    if unexpected:
        raise ValueError("Undeclared CDS parameter(s): " + ", ".join(unexpected))

    resolved: Dict[str, Any] = {}
    for parameter in declarations:
        name = str(parameter.get("name") or "").strip()
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", name):
            raise ValueError("ADT returned an invalid CDS parameter identifier.")
        annotations = parameter.get("annotations") or {}
        if name.casefold() in override_by_name:
            value = override_by_name[name.casefold()]
        else:
            system_field = str(annotations.get("@Environment.systemField") or "").strip()
            resolver = SYSTEM_FIELD_RESOLVERS.get(system_field)
            if resolver is not None:
                value = resolver()
            elif "@Consumption.defaultValue" in annotations:
                value = annotations["@Consumption.defaultValue"]
            else:
                raise ValueError(
                    f"Required CDS parameter {name} has no explicit value or verified CDS default/system-field annotation."
                )
        if DataRetriever is None:
            raise RuntimeError("ADT query execution is unavailable because DataRetriever could not be imported.")
        DataRetriever._format_parameter_value(value)
        resolved[name] = value
    return resolved


def _compile_cds_source_query(
    source_metadata: Dict[str, Any],
    parameter_overrides: Optional[Dict[str, Any]] = None,
    source_count: int = 1,
) -> Dict[str, Any]:
    """Compile an explicit-field Open SQL preview query from ADT-verified metadata."""
    entity = str(source_metadata.get("entity") or "").strip()
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", entity):
        raise ValueError("Verified ADT metadata did not include a valid CDS entity name.")
    fields = [
        str(field.get("name") or "").strip()
        for field in (source_metadata.get("fields") or [])
        if isinstance(field, dict) and field.get("name")
    ]
    fields = list(dict.fromkeys(fields))
    invalid = [name for name in fields if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", name)]
    if invalid:
        raise ValueError(f"ADT returned invalid field identifiers for CDS entity {entity}.")
    if not fields:
        raise ValueError(f"ADT metadata for CDS entity {entity} contains no selectable fields.")

    overrides = _source_parameter_overrides(parameter_overrides, entity, source_count)
    parameters = _resolve_declared_cds_parameters(source_metadata, overrides)
    selected = [f"{entity}~{name}" for name in fields] if parameters else fields
    projection = ",\n    ".join(selected)
    if parameters:
        assignments = ",\n    ".join(
            f"{name} = {DataRetriever._format_parameter_value(value)}"
            for name, value in parameters.items()
        )
        source = f"{entity}(\n    {assignments}\n)"
    else:
        source = entity
    query = f"SELECT\n    {projection}\nFROM {source}"
    if any(len(line) > 255 for line in query.splitlines()):
        raise ValueError(f"Generated ADT query for {entity} exceeds the ABAP line-length limit.")
    return {
        "source": entity,
        "sql": query,
        "parameters": parameters,
        "fields": fields,
        "keys": list(source_metadata.get("key_fields") or []),
        "column_metadata": fields and [dict(field) for field in source_metadata.get("fields", []) if isinstance(field, dict)],
    }


def _bind_declared_cds_parameters(
    sql: str,
    source_metadata: List[Dict[str, Any]],
    parameter_overrides: Optional[Dict[str, Any]],
) -> Tuple[str, Dict[str, Dict[str, Any]]]:
    """Bind every used CDS entity from verified declarations and approved values."""
    bound_sql = sql
    bound_parameters: Dict[str, Dict[str, Any]] = {}
    for source in source_metadata:
        entity = str(source.get("entity") or "").strip()
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", entity):
            continue
        pattern = re.compile(
            rf'(?i)\b(FROM|JOIN)\s+("?{re.escape(entity)}"?)'
            r'(?:\s*\((?P<args>[^()]*)\))?'
        )
        matches = list(pattern.finditer(bound_sql))
        if not matches:
            continue
        declarations = source.get("parameters") or []
        existing_call = any(match.group("args") is not None for match in matches)
        if not declarations:
            if existing_call:
                raise ValueError(f"Generated SQL parameterizes {entity}, but ADT metadata declares no CDS parameters.")
            continue
        overrides = _source_parameter_overrides(parameter_overrides, entity, len(source_metadata))
        resolved = _resolve_declared_cds_parameters(source, overrides)
        bound_parameters[entity] = resolved
        formatted = ",\n    ".join(
            f"{name} = {DataRetriever._format_parameter_value(value)}"
            for name, value in resolved.items()
        )
        replacement = f"{entity}(\n    {formatted}\n)"
        for match in reversed(matches):
            bound_sql = bound_sql[:match.start(2)] + replacement + bound_sql[match.end(2):]
    return bound_sql, bound_parameters


def _execute_adt_source_queries(
    source_queries: List[Dict[str, Any]],
    row_limit: int,
) -> Dict[str, Any]:
    """Execute each verified source query through the existing DataRetriever/ADT client."""
    if DataRetriever is None:
        raise RuntimeError("ADT query execution is unavailable because DataRetriever could not be imported.")
    results = []
    started = time.perf_counter()
    with DataRetriever(default_row_limit=row_limit, max_row_limit=1000) as retriever:
        for item in source_queries:
            source_started = time.perf_counter()
            source = str(item["source"])
            try:
                parsed = retriever.fetch_query(
                    query=str(item["sql"]),
                    row_limit=row_limit,
                    csrf_entity=source,
                )
                rows = parsed.get("rows") or []
                total_rows = int(parsed["total_rows"])
                if total_rows > len(rows):
                    status = "SUCCEEDED_TRUNCATED"
                    error = f"ADT returned {len(rows)} of {total_rows} rows at row limit {row_limit}."
                elif total_rows == 0:
                    status = "SUCCEEDED_ZERO_ROWS"
                    error = None
                else:
                    status = "SUCCEEDED_WITH_ROWS"
                    error = None
                results.append({
                    "source": source,
                    "status": status,
                    "truncated": status == "SUCCEEDED_TRUNCATED",
                    "sql": str(item["sql"]),
                    "parameters": item.get("parameters", {}),
                    "columns": parsed.get("columns", []),
                    "rows": rows,
                    "row_count": len(rows),
                    "total_rows": total_rows,
                    "execution_time": parsed.get("execution_time"),
                    "latency_seconds": round(time.perf_counter() - source_started, 3),
                    "error": error,
                })
            except Exception as exc:
                results.append({
                    "source": source,
                    "status": "EXECUTION_FAILED",
                    "sql": str(item["sql"]),
                    "parameters": item.get("parameters", {}),
                    "columns": [],
                    "rows": [],
                    "row_count": None,
                    "total_rows": None,
                    "execution_time": None,
                    "latency_seconds": round(time.perf_counter() - source_started, 3),
                    "error": str(exc),
                })
    return {"sources": results, "latency_seconds": round(time.perf_counter() - started, 3)}


def _build_transformation_ir(dependency_tree: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize available Datasphere lineage and operations without discarding raw provenance."""
    nodes: List[Dict[str, Any]] = []
    edges: List[Dict[str, str]] = []
    filters: List[Dict[str, Any]] = []
    joins: List[Dict[str, Any]] = []
    calculations: List[Dict[str, Any]] = []
    formulas: List[Dict[str, Any]] = []
    projections: List[Dict[str, Any]] = []
    aggregations: List[Dict[str, Any]] = []
    parameters: List[Dict[str, Any]] = []
    constants: List[Dict[str, Any]] = []
    groupings: List[Dict[str, Any]] = []
    distincts: List[Dict[str, Any]] = []
    null_handling: List[Dict[str, Any]] = []
    unsupported: List[Dict[str, str]] = []
    unsupported_keys = {
        "union", "intersect", "except", "set", "window", "over", "rank",
        "analyticfunction",
    }
    null_expression = re.compile(
        r"\b(?:coalesce|ifnull|nullif|nvl|isnull|isnotnull)\s*\(|\bis\s+(?:not\s+)?null\b",
        re.IGNORECASE,
    )

    def visit(node: Dict[str, Any], parent_path: str = "") -> None:
        name = str(node.get("name") or "")
        object_type = str(node.get("object_type") or "unknown")
        space = str(node.get("space") or dependency_tree.get("space") or "")
        path = f"{parent_path} -> {name}" if parent_path and name else name or parent_path
        definition = node.get("definition") if isinstance(node.get("definition"), dict) else {}
        operation_metadata = {
            "metadata_status": "RETRIEVED" if definition else "UNAVAILABLE",
            "metadata_source": "Datasphere CLI definition",
        }
        definition_digest = _hashlib.sha256(
            json.dumps(definition, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest() if definition else None
        fields = [
            {
                "name": str(field.get("name")),
                "data_type": field.get("element_type") or field.get("type"),
                "is_key": bool(field.get("is_key") or field.get("key")),
                "aggregation": field.get("aggregation"),
                "is_measure": bool(field.get("is_measure") or field.get("measure_type")),
                "source_field": field.get("source_field") or field.get("sourceField") or field.get("source_column"),
                "calculation": field.get("calculation") or field.get("expression"),
                "referenced_columns": field.get("referenced_columns", []),
            }
            for field in (node.get("field_catalog") or [])
            if isinstance(field, dict) and field.get("name")
        ]
        nodes.append({
            "name": name,
            "object_type": object_type,
            "space": space,
            "path": path,
            "status": node.get("status"),
            "definition_digest": definition_digest,
            "field_catalog": fields,
            "definition_available": bool(definition),
        })
        if parent_path and name:
            edges.append({"from": parent_path.split(" -> ")[-1], "to": name, "path": path})

        for field in fields:
            if field.get("aggregation"):
                aggregations.append({"owner": name, "path": path, **field})
        for item in node.get("filters", []) or []:
            if isinstance(item, dict):
                filter_item = {"owner": name, "path": path, **item, **operation_metadata}
                filters.append(filter_item)
                expression = item.get("condition") or item.get("expression")
                if expression and null_expression.search(str(expression)):
                    null_handling.append({
                        "owner": name,
                        "path": item.get("path") or item.get("location") or path,
                        "expression": expression,
                        **operation_metadata,
                    })
        for item in node.get("joins", []) or []:
            if isinstance(item, dict):
                join_item = {"owner": name, "path": path, **item, **operation_metadata}
                joins.append(join_item)
                expression = item.get("condition") or item.get("on")
                if expression and null_expression.search(str(expression)):
                    null_handling.append({
                        "owner": name,
                        "path": item.get("path") or path,
                        "expression": expression,
                        **operation_metadata,
                    })
        for item in node.get("calculated_columns", []) or []:
            if isinstance(item, dict):
                calculation = {"owner": name, "path": path, **item, **operation_metadata}
                calculations.append(calculation)
                if str(item.get("transformation_category") or "").casefold() == "constant":
                    constants.append(calculation)
                expression = item.get("expression")
                if expression and null_expression.search(str(expression)):
                    null_handling.append({
                        "owner": name,
                        "path": item.get("path") or path,
                        "name": item.get("name"),
                        "expression": expression,
                        **operation_metadata,
                    })
        for item in node.get("formulas", []) or []:
            if isinstance(item, dict):
                formula = {"owner": name, "path": path, **item, **operation_metadata}
                formulas.append(formula)
                expression = item.get("formula") or item.get("expression")
                if expression and null_expression.search(str(expression)):
                    null_handling.append({
                        "owner": name,
                        "path": item.get("path") or path,
                        "name": item.get("name"),
                        "expression": expression,
                        **operation_metadata,
                    })
                restriction = item.get("restriction")
                if restriction and null_expression.search(str(restriction)):
                    null_handling.append({
                        "owner": name,
                        "path": item.get("path") or path,
                        "name": item.get("name"),
                        "expression": restriction,
                        **operation_metadata,
                    })
        for item in node.get("parameters", []) or []:
            if isinstance(item, dict):
                parameters.append({"owner": name, "path": path, **item, **operation_metadata})

        def scan(value: Any, value_path: str = "definition") -> None:
            if isinstance(value, dict):
                normalized_keys = {str(key).replace("_", "").casefold(): key for key in value}
                for normalized, original in normalized_keys.items():
                    if normalized in unsupported_keys:
                        unsupported.append({
                            "owner": name,
                            "path": f"{path}.{value_path}.{original}",
                            "construct": str(original),
                            **operation_metadata,
                        })
                    if normalized in {"groupby", "grouping", "groupings"}:
                        groupings.append({
                            "owner": name,
                            "path": f"{path}.{value_path}.{original}",
                            "expression": value[original],
                            **operation_metadata,
                        })
                    if normalized == "distinct" and bool(value[original]):
                        distincts.append({
                            "owner": name,
                            "path": f"{path}.{value_path}.{original}",
                            "value": value[original],
                            **operation_metadata,
                        })
                output_name = value.get("as") or value.get("output") or value.get("target_field")
                ref = value.get("ref")
                if output_name and isinstance(ref, list) and ref:
                    projections.append({
                        "owner": name,
                        "path": f"{path}.{value_path}",
                        "output_field": str(output_name),
                        "source_field": str(ref[-1]),
                        "source_ref": list(ref),
                        **operation_metadata,
                    })
                if output_name and "val" in value and "ref" not in value:
                    constants.append({
                        "owner": name,
                        "path": f"{path}.{value_path}",
                        "output_field": str(output_name),
                        "value": value.get("val"),
                        **operation_metadata,
                    })
                for key, child in value.items():
                    scan(child, f"{value_path}.{key}")
            elif isinstance(value, list):
                for index, child in enumerate(value):
                    scan(child, f"{value_path}[{index}]")

        if definition:
            scan(definition)
        for child in node.get("dependencies", []) or []:
            if isinstance(child, dict):
                visit(child, path)

    visit(dependency_tree)
    return {
        "version": 1,
        "root_model": dependency_tree.get("name"),
        "root_space": dependency_tree.get("space"),
        "nodes": nodes,
        "edges": edges,
        "projections": projections,
        "filters": filters,
        "joins": joins,
        "calculations": calculations,
        "formulas": formulas,
        "aggregations": aggregations,
        "parameters": parameters,
        "constants": constants,
        "groupings": groupings,
        "distincts": distincts,
        "null_handling": null_handling,
        "unsupported_constructs": unsupported,
        "provenance": "Raw Datasphere definitions remain in dependency_tree; each normalized operation records its owner and source path.",
    }


def _normalize_expression(expression: Any) -> str:
    text = str(expression or "").strip().casefold()
    text = re.sub(r"[\"`\[\]]", "", text)
    text = re.sub(r"\b[a-z_][a-z0-9_]*[.~]", "", text)
    text = re.sub(r"\s+", "", text)
    while text.startswith("(") and text.endswith(")"):
        depth = 0
        quote: Optional[str] = None
        encloses_expression = True
        index = 0
        while index < len(text):
            char = text[index]
            if quote:
                if char == quote:
                    if index + 1 < len(text) and text[index + 1] == quote:
                        index += 2
                        continue
                    quote = None
            elif char in {"'", '"'}:
                quote = char
            elif char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0 and index != len(text) - 1:
                    encloses_expression = False
                    break
            index += 1
        if not encloses_expression:
            break
        text = text[1:-1]
    return text


def _format_abap_open_sql(query: str, max_line_length: int = 255) -> str:
    """Wrap generated Open SQL without changing tokens or quoted literals."""
    if not isinstance(query, str) or not query.strip():
        raise ValueError("Generated ABAP Open SQL must be a non-empty string.")
    if max_line_length < 1:
        raise ValueError("ABAP Open SQL line length must be positive.")

    lines: List[str] = []
    current: List[str] = []
    quote: Optional[str] = None

    def finish_line() -> None:
        line = "".join(current).strip()
        if line:
            lines.append(line)
        current.clear()

    index = 0
    while index < len(query):
        char = query[index]
        if quote:
            current.append(char)
            if char == quote:
                if index + 1 < len(query) and query[index + 1] == quote:
                    current.append(query[index + 1])
                    index += 2
                    continue
                quote = None
            if len(current) > max_line_length:
                raise ValueError(
                    "Generated ABAP Open SQL contains a quoted token longer "
                    f"than {max_line_length} characters."
                )
            index += 1
            continue

        if char in {"'", '"'}:
            quote = char
            current.append(char)
        elif char in "\r\n":
            finish_line()
        elif char == ",":
            current.append(char)
            finish_line()
        elif char.isspace():
            if current:
                if len(current) >= max_line_length:
                    finish_line()
                else:
                    current.append(" ")
        else:
            if len(current) >= max_line_length:
                raise ValueError(
                    "Generated ABAP Open SQL contains an unbreakable token "
                    f"longer than {max_line_length} characters."
                )
            current.append(char)
        index += 1

    if quote:
        raise ValueError("Generated ABAP Open SQL contains an unterminated quoted token.")
    finish_line()
    return "\n".join(lines)


def _has_select_alias(select_clause: str, field_name: str) -> bool:
    return bool(re.search(
        rf"(?i)\bas\s+\"?{re.escape(field_name)}\"?(?=\s|,|$)",
        select_clause,
    ))


def _compare_aggregate_measures(
    target_columns: List[Dict[str, Any]],
    selected_fields: List[str],
    datasphere_rows: List[Dict[str, Any]],
    s4hana_rows: List[Dict[str, Any]],
    *,
    datasphere_status: str,
    s4hana_status: str,
    transformation_ir: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Compare only selected measures whose aggregation semantics are verified."""
    ir_aggregations = {
        str(item.get("name") or "").casefold(): item.get("aggregation")
        for item in (transformation_ir or {}).get("aggregations", [])
        if isinstance(item, dict) and item.get("name") and item.get("aggregation")
    }
    columns_by_name = {
        str(column.get("name") or "").casefold(): column
        for column in target_columns
        if isinstance(column, dict) and column.get("name")
    }

    def value_for(row: Dict[str, Any], name: str) -> Any:
        return next(
            (value for key, value in row.items() if str(key).casefold() == name.casefold()),
            None,
        )

    def numeric_values(rows: List[Dict[str, Any]], name: str) -> Optional[List[Decimal]]:
        values = []
        for row in rows:
            value = value_for(row, name)
            if value is None or str(value).strip() == "":
                continue
            try:
                number = Decimal(str(value).replace(",", "").strip())
            except (InvalidOperation, ValueError, TypeError):
                return None
            if not number.is_finite():
                return None
            values.append(number)
        return values

    def aggregate(
        rows: List[Dict[str, Any]],
        name: str,
        semantics: str,
    ) -> Tuple[Optional[Decimal], Optional[str]]:
        if not rows:
            return None, "EMPTY_RESULT"
        values = numeric_values(rows, name)
        if values is None:
            return None, "NON_NUMERIC_RESULT"
        if not values:
            return None, "MEASURE_VALUE_MISSING"
        if semantics in {"SUM", "COUNT"}:
            return sum(values, Decimal(0)), None
        if semantics == "MIN":
            return min(values), None
        if semantics == "MAX":
            return max(values), None
        if semantics in {"AVG", "AVERAGE", "COUNT_DISTINCT", "DISTINCT_COUNT"}:
            if len(values) == 1:
                return values[0], None
            return None, "AGGREGATE_NOT_RECONSTRUCTIBLE_AT_RETURNED_GRAIN"
        return None, "UNSUPPORTED_AGGREGATION"

    completeness = {
        "datasphere": (
            "COMPLETE" if datasphere_status.startswith("SUCCEEDED")
            and datasphere_status != "SUCCEEDED_TRUNCATED"
            else "INCOMPLETE" if datasphere_status == "SUCCEEDED_TRUNCATED"
            else "NOT_EXECUTED"
        ),
        "s4hana": (
            "COMPLETE" if s4hana_status.startswith("SUCCEEDED")
            and s4hana_status != "SUCCEEDED_TRUNCATED"
            else "INCOMPLETE" if s4hana_status == "SUCCEEDED_TRUNCATED"
            else "NOT_EXECUTED"
        ),
    }
    results = []
    for field in selected_fields:
        column = columns_by_name.get(field.casefold(), {})
        raw_semantics = (
            column.get("aggregation")
            or column.get("aggregation_semantics")
            or column.get("measure_type")
            or ir_aggregations.get(field.casefold())
        )
        is_measure = bool(
            raw_semantics
            or column.get("is_measure")
            or str(column.get("category") or "").casefold() == "measure"
        )
        if not is_measure:
            continue

        semantics = str(raw_semantics or "UNSPECIFIED").strip().upper().lstrip("#")
        if "DISTINCT" in semantics and "COUNT" in semantics:
            normalized_semantics = "COUNT_DISTINCT"
        else:
            normalized_semantics = {
                "AVERAGE": "AVG",
                "MEAN": "AVG",
            }.get(semantics, semantics)

        ds_value = None
        s4_value = None
        if not semantics or semantics == "UNSPECIFIED":
            metric_status = (
                "NOT_EXECUTED" if "NOT_EXECUTED" in completeness.values()
                else "INCOMPLETE" if "INCOMPLETE" in completeness.values()
                else "UNSPECIFIED_AGGREGATION"
            )
        else:
            ds_issue = None
            s4_issue = None
            if completeness["datasphere"] == "COMPLETE":
                ds_value, ds_issue = aggregate(
                    datasphere_rows, field, normalized_semantics
                )
            if completeness["s4hana"] == "COMPLETE":
                s4_value, s4_issue = aggregate(
                    s4hana_rows, field, normalized_semantics
                )
            if all(value == "COMPLETE" for value in completeness.values()):
                metric_status = ds_issue or s4_issue or "READY"
            elif "INCOMPLETE" in completeness.values():
                metric_status = "INCOMPLETE"
            else:
                metric_status = "NOT_EXECUTED"

        difference = (
            abs(ds_value - s4_value)
            if metric_status == "READY" and ds_value is not None and s4_value is not None
            else None
        )
        relative_difference = (
            difference / abs(ds_value) * 100
            if difference is not None and ds_value
            else None
        )
        results.append({
            "metric_name": field,
            "aggregation": semantics,
            "aggregation_semantics": semantics,
            "datasphere_total": float(ds_value) if ds_value is not None else None,
            "formatted_datasphere": format(ds_value, "f") if ds_value is not None else "Not available",
            "s4hana_total": float(s4_value) if s4_value is not None else None,
            "formatted_s4hana": format(s4_value, "f") if s4_value is not None else "Not available",
            "delta": float(difference) if difference is not None else None,
            "absolute_difference": float(difference) if difference is not None else None,
            "variance_pct": (
                float(relative_difference) if relative_difference is not None else None
            ),
            "relative_difference_pct": (
                float(relative_difference) if relative_difference is not None else None
            ),
            "comparison_status": metric_status,
            "completeness_status": (
                "COMPLETE" if "NOT_EXECUTED" not in completeness.values()
                and "INCOMPLETE" not in completeness.values() else
                "INCOMPLETE" if "INCOMPLETE" in completeness.values()
                else "NOT_EXECUTED"
            ),
            "is_match": difference == 0 if difference is not None else False,
        })
    return results


def _select_clause(sql: str) -> str:
    match = re.search(r"(?is)\bselect\b(.*?)\bfrom\b", sql)
    return match.group(1) if match else ""


def _group_by_clause(sql: str) -> str:
    match = re.search(
        r"(?is)\bgroup\s+by\b(.*?)(?:\bhaving\b|\border\s+by\b|$)",
        sql,
    )
    return match.group(1) if match else ""


def _where_clause(sql: str) -> str:
    match = re.search(
        r"(?is)\bwhere\b(.*?)(?:\bgroup\s+by\b|\bhaving\b|\border\s+by\b|$)",
        sql,
    )
    return match.group(1) if match else ""


def _having_clause(sql: str) -> str:
    match = re.search(r"(?is)\bhaving\b(.*?)(?:\border\s+by\b|$)", sql)
    return match.group(1) if match else ""


def _join_on_clauses(sql: str) -> List[str]:
    matches = re.finditer(
        r"(?is)\bon\b(.*?)(?=\b(?:inner|left|right|full|cross)\s+(?:outer\s+)?join\b|"
        r"\bwhere\b|\bgroup\s+by\b|\bhaving\b|\border\s+by\b|$)",
        sql,
    )
    return [match.group(1) for match in matches]


def _split_sql_expressions(clause: str) -> List[str]:
    expressions: List[str] = []
    start = 0
    depth = 0
    quote: Optional[str] = None
    index = 0
    while index < len(clause):
        char = clause[index]
        if quote:
            if char == quote:
                if index + 1 < len(clause) and clause[index + 1] == quote:
                    index += 1
                else:
                    quote = None
        elif char in {"'", '"'}:
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth = max(0, depth - 1)
        elif char == "," and depth == 0:
            expressions.append(clause[start:index].strip())
            start = index + 1
        index += 1
    tail = clause[start:].strip()
    if tail:
        expressions.append(tail)
    return expressions


def _coverage_expression(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        return " ".join(
            expression for expression in (_coverage_expression(item) for item in value)
            if expression
        )
    if isinstance(value, dict):
        if isinstance(value.get("ref"), list):
            return ".".join(str(part) for part in value["ref"])
        if "val" in value:
            literal = value["val"]
            return f"'{literal}'" if isinstance(literal, str) else str(literal)
        if "func" in value:
            args = ", ".join(
                _coverage_expression(arg) for arg in (value.get("args") or [])
            )
            return f"{value['func']}({args})"
        if "xpr" in value:
            return _coverage_expression(value["xpr"])
    return ""


def _match_null_expression(expression: Any, normalized_sql: str) -> Tuple[bool, Optional[str], Optional[str]]:
    source_expression = str(expression or "").strip()
    if not source_expression:
        return False, None, "Datasphere null-handling expression is missing."
    if re.search(r"\bnullif\s*\(", source_expression, re.IGNORECASE):
        return False, None, "NULLIF has no verified ABAP Open SQL translation in this planner."
    translations = [
        re.sub(r"\b(?:ifnull|nvl)\s*\(", "COALESCE(", source_expression, flags=re.IGNORECASE),
    ]
    is_null_match = re.fullmatch(r"\s*isnull\s*\(\s*([^()]*)\s*\)\s*", source_expression, re.IGNORECASE)
    if is_null_match:
        translations.append(f"{is_null_match.group(1).strip()} IS NULL")
    is_not_null_match = re.fullmatch(r"\s*isnotnull\s*\(\s*([^()]*)\s*\)\s*", source_expression, re.IGNORECASE)
    if is_not_null_match:
        translations.append(f"{is_not_null_match.group(1).strip()} IS NOT NULL")
    if not re.search(r"\b(?:ifnull|nvl|isnull|isnotnull|nullif)\s*\(", source_expression, re.IGNORECASE):
        translations.append(source_expression)
    for candidate in translations:
        normalized_candidate = _normalize_expression(candidate)
        if normalized_candidate and normalized_candidate in normalized_sql:
            return True, candidate, None
    return False, None, "No equivalent null-handling expression was found in the generated ABAP Open SQL."


def _expand_case_filter_reference(
    expression: Any,
    calculations_by_name: Dict[str, List[str]],
) -> Optional[str]:
    """Translate equality against a CASE output into its verified true-branch predicate."""
    comparison = re.fullmatch(
        r"\s*(?:[A-Za-z_][A-Za-z0-9_]*[.~])*"
        r"[\"`]?([A-Za-z_][A-Za-z0-9_]*)[\"`]?\s*(=|==)\s*(.+?)\s*",
        str(expression or ""),
        re.IGNORECASE,
    )
    if not comparison:
        return None

    field_name, operator, requested_value = comparison.groups()
    if operator != "=" and operator != "==":
        return None
    requested = _normalize_expression_preserving_literals(requested_value)
    definitions = calculations_by_name.get(field_name.casefold(), [])
    predicates = []
    literal_pattern = r"(?:'(?:''|[^'])*'|[-+]?\d+(?:\.\d+)?)"
    for definition in definitions:
        case_match = re.fullmatch(
            r"\s*CASE\s+WHEN\s+(.+?)\s+THEN\s+(.+?)\s+ELSE\s+(.+?)\s+END\s*",
            str(definition or ""),
            re.IGNORECASE,
        )
        if not case_match:
            return None
        predicate, true_value, _false_value = case_match.groups()
        if not re.fullmatch(literal_pattern, true_value.strip()) or not re.fullmatch(
            literal_pattern, _false_value.strip()
        ):
            return None
        if (
            _normalize_expression_preserving_literals(true_value) != requested
            or _normalize_expression_preserving_literals(_false_value) == requested
        ):
            return None
        predicates.append(predicate.strip())

    normalized_predicates = {_normalize_expression(value) for value in predicates}
    if not predicates or len(normalized_predicates) != 1:
        return None
    return predicates[0]


def _open_sql_expression_candidates(expression: Any, *, constant: bool = False) -> List[str]:
    """Translate only literal-argument LEFT/SUBSTRING calls in verified metadata expressions."""
    if not isinstance(expression, str) or not expression.strip():
        return []
    source = expression.strip()
    candidates = [source]
    if constant and not (source.startswith("'") and source.endswith("'")):
        candidates = []

    def translate_function(match: re.Match[str]) -> str:
        function = match.group(1).casefold()
        arguments = _split_sql_expressions(match.group(2))
        if function == "left" and len(arguments) == 2 and arguments[1].strip().isdigit():
            return f"substring( val = {arguments[0].strip()} off = 0 len = {arguments[1].strip()} )"
        if (
            function == "substring"
            and len(arguments) == 3
            and arguments[1].strip().isdigit()
            and int(arguments[1].strip()) >= 1
            and arguments[2].strip().isdigit()
        ):
            offset = int(arguments[1].strip()) - 1
            return f"substring( val = {arguments[0].strip()} off = {offset} len = {arguments[2].strip()} )"
        return match.group(0)

    translated = re.sub(
        r"\b(LEFT|SUBSTRING)\s*\(([^()]*)\)",
        translate_function,
        source,
        flags=re.IGNORECASE,
    )
    if translated != source:
        candidates.append(translated)
    if constant and not (source.startswith("'") and source.endswith("'")):
        candidates.append("'" + source.replace("'", "''") + "'")
    return list(dict.fromkeys(candidates))


def _verify_transformation_coverage(
    transformation_ir: Dict[str, Any],
    *,
    s4hana_sql: str,
    selected_fields: List[str],
    target_columns: List[Dict[str, Any]],
    source_fields: List[Dict[str, Any]],
    cds_parameters: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Map discovered model transformations to query expressions; fail closed on gaps."""
    raw_select_clause = _select_clause(s4hana_sql)
    normalized_sql = _normalize_expression(s4hana_sql)
    select_expressions = [
        _normalize_expression(re.split(r"(?i)\s+as\s+", expression, maxsplit=1)[0])
        for expression in _split_sql_expressions(raw_select_clause)
    ]
    group_expressions = {
        _normalize_expression(expression)
        for expression in _split_sql_expressions(_group_by_clause(s4hana_sql))
    }
    normalized_filter_clauses = _normalize_expression(
        _where_clause(s4hana_sql) + " " + _having_clause(s4hana_sql)
    )
    normalized_join_clauses = [
        _normalize_expression(clause)
        for clause in _join_on_clauses(s4hana_sql)
    ]
    source_names = {str(item.get("name") or "").casefold() for item in source_fields if item.get("name")}
    target_by_name = {
        str(item.get("name") or "").casefold(): item
        for item in target_columns if item.get("name")
    }
    projection_sources: Dict[str, set[str]] = {}
    for item in (transformation_ir.get("projections") or []):
        if item.get("output_field") and item.get("source_field"):
            projection_sources.setdefault(str(item["output_field"]).casefold(), set()).add(
                str(item["source_field"])
            )
    explicit_projection_map = {
        output_name: next(iter(source_names_for_output))
        for output_name, source_names_for_output in projection_sources.items()
        if len(source_names_for_output) == 1
    }
    calc_by_name = {
        str(item.get("name") or "").casefold(): item
        for item in (transformation_ir.get("calculations") or [])
        if item.get("name")
    }
    calc_by_name.update({
        str(item.get("name") or "").casefold(): item
        for item in (transformation_ir.get("formulas") or [])
        if item.get("name")
    })
    calculation_expressions_by_name: Dict[str, List[str]] = {}
    for item in (
        list(transformation_ir.get("calculations") or [])
        + list(transformation_ir.get("formulas") or [])
    ):
        name = str(item.get("name") or "").casefold()
        expression = item.get("expression") or item.get("formula")
        if name and expression:
            calculation_expressions_by_name.setdefault(name, []).append(str(expression))

    calculation_names = set(calculation_expressions_by_name)
    required_calculation_names = {
        name.casefold() for name in selected_fields
        if name.casefold() in calculation_names
    }
    while True:
        discovered_references = set()
        for name in required_calculation_names:
            for definition in calculation_expressions_by_name.get(name, []):
                for token in re.findall(r"\b[A-Za-z_][A-Za-z0-9_]*\b", definition):
                    if token.casefold() in calculation_names:
                        discovered_references.add(token.casefold())
        new_references = discovered_references - required_calculation_names
        if not new_references:
            break
        required_calculation_names.update(new_references)

    coverage_items: List[Dict[str, Any]] = []

    def has_known_supported_translation(kind: str, item: Dict[str, Any], expression: Any) -> bool:
        if kind == "field_mapping" and isinstance(expression, dict):
            source_field = expression.get("source_field")
            return bool(source_field and str(source_field).casefold() in source_names)
        if kind == "aggregation":
            return str(item.get("aggregation") or "").lstrip("#").casefold() in {
                "sum", "count", "min", "max", "avg",
            }
        if kind == "join":
            join_type = str(item.get("join_type") or item.get("type") or "inner").casefold()
            return any(term in join_type for term in ("inner", "left")) and bool(
                item.get("condition") or item.get("on")
            )
        if kind == "filter":
            if _expand_case_filter_reference(
                expression, calculation_expressions_by_name
            ) is not None:
                return True
            predicate = re.sub(r"'(?:''|[^'])*'", " ", str(expression or ""))
            predicate = re.sub(r"\b[A-Za-z_][A-Za-z0-9_]*[.~]", "", predicate)
            tokens = {
                token.casefold()
                for token in re.findall(r"\b[A-Za-z_][A-Za-z0-9_]*\b", predicate)
            }
            allowed = source_names | calculation_names | {
                "and", "or", "not", "is", "null", "like", "in", "between",
                "true", "false",
            }
            has_predicate_operator = bool(re.search(
                r"(?:=|<>|<=|>=|<|>|\b(?:IS|LIKE|IN|BETWEEN)\b)",
                predicate,
                re.IGNORECASE,
            ))
            return bool(tokens) and tokens <= allowed and has_predicate_operator
        if kind == "calculation":
            if str(item.get("transformation_category") or "").casefold() == "constant":
                return isinstance(expression, (str, int, float))
            expression_text = str(expression or "").strip()
            if re.fullmatch(
                r"(?is)CASE\s+WHEN\s+.+?\s+THEN\s+.+?\s+ELSE\s+.+?\s+END",
                expression_text,
            ) and not re.search(
                r"\b[A-Za-z_][A-Za-z0-9_]*\s*\(",
                expression_text,
            ):
                return True
            candidates = _open_sql_expression_candidates(expression)
            return len(candidates) > 1 and not re.search(
                r"\b(?!LEFT\b|SUBSTRING\b)[A-Za-z_][A-Za-z0-9_]*\s*\(",
                expression_text,
                re.IGNORECASE,
            )
        return False

    def report(
        kind: str,
        item: Dict[str, Any],
        expression: Any,
        matched_expression: Optional[str],
        verified: bool,
        detail: Optional[str] = None,
    ) -> None:
        expression_text = (
            expression if isinstance(expression, str)
            else json.dumps(expression, sort_keys=True, default=str) if expression is not None
            else ""
        )
        metadata_status = item.get("metadata_status") or (
            "RETRIEVED" if expression is not None or item.get("path") else "UNAVAILABLE"
        )
        if verified:
            state = "VERIFIED"
        elif kind == "unsupported_construct":
            state = "TRANSLATION_UNSUPPORTED"
        elif not expression_text or metadata_status != "RETRIEVED":
            state = "METADATA_UNAVAILABLE"
        elif has_known_supported_translation(kind, item, expression):
            state = "SUPPORTED_NOT_COMPILED"
        else:
            state = "UNASSESSED"
        relevant_fields = [
            str(name)
            for name in item.get("referenced_columns", [])
            if name
        ] if isinstance(item.get("referenced_columns"), list) else []
        if not relevant_fields and expression_text:
            relevant_fields = [
                str(field["name"])
                for field in source_fields
                if field.get("name") and re.search(
                    rf"(?<![A-Za-z0-9_]){re.escape(str(field['name']))}(?![A-Za-z0-9_])",
                    expression_text,
                    re.IGNORECASE,
                )
            ]
        transformation_id = str(
            item.get("id")
            or item.get("path")
            or f"{item.get('owner') or 'unknown'}:{kind}:"
            f"{item.get('name') or item.get('output_field') or item.get('construct') or 'unnamed'}"
        )
        coverage_items.append({
            "transformation_id": transformation_id,
            "transformation": kind,
            "owner": item.get("owner"),
            "datasphere_path": item.get("path") or item.get("location"),
            "datasphere_expression": expression,
            "s4hana_expression": matched_expression if verified else None,
            "status": "VERIFIED" if verified else "UNVERIFIED",
            "state": state,
            "source_object": item.get("source_object") or item.get("owning_object") or item.get("owner"),
            "source_fields": list(dict.fromkeys(relevant_fields)),
            "metadata_status": metadata_status,
            "metadata_source": item.get("metadata_source") or (
                "Transformation IR" if metadata_status == "RETRIEVED" else None
            ),
            "diagnostic": (
                detail
                or (
                    "Recognized supported transformation, but its equivalent expression "
                    "was not found in the candidate ABAP Open SQL."
                    if state == "SUPPORTED_NOT_COMPILED"
                    else "Translation support could not be established from the available metadata; explicit review is required."
                )
            ) if not verified else None,
        })

    projection_checks = []
    for output in selected_fields:
        field = target_by_name.get(output.casefold(), {})
        explicit_source = (
            field.get("source_field") or field.get("sourceField")
            or field.get("source_column") or field.get("sourceColumn")
            or field.get("mapped_field") or field.get("mappedField")
        )
        mapped_projection = explicit_projection_map.get(output.casefold())
        source_field = str(explicit_source or mapped_projection or "")
        conflicting_projection_sources = projection_sources.get(output.casefold(), set())
        alias_seen = _has_select_alias(_select_clause(s4hana_sql), output)
        source_present = bool(source_field) and source_field.casefold() in source_names
        calc = calc_by_name.get(output.casefold())
        calc_expression = calc.get("expression") or calc.get("formula") if calc else None
        calc_candidates = _open_sql_expression_candidates(
            calc_expression,
            constant=bool(calc and str(calc.get("transformation_category") or "").casefold() == "constant"),
        )
        if calc_expression:
            try:
                expanded_calculation = _expand_calculated_field_dependencies(
                    str(calc_expression),
                    calculation_expressions_by_name,
                    projection_sources,
                )
            except ValueError:
                expanded_calculation = ""
            if expanded_calculation:
                calc_candidates.extend(
                    _open_sql_expression_candidates(expanded_calculation)
                )
        matched_calculation = next(
            (
                candidate for candidate in calc_candidates
                if _normalize_expression(candidate) in normalized_sql
            ),
            None,
        )
        expression_seen = matched_calculation is not None
        normalized_source = _normalize_expression(source_field)
        direct_projection_seen = bool(normalized_source) and any(
            re.search(
                rf"(?<![a-z0-9_]){re.escape(normalized_source)}(?![a-z0-9_])",
                expression,
            )
            for expression in select_expressions
        )
        verified = len(conflicting_projection_sources) <= 1 and alias_seen and (
            (source_present and direct_projection_seen)
            or (bool(calc_expression) and expression_seen)
        )
        check = {
            "output_field": output,
            "source_field": source_field if source_present else None,
            "s4hana_expression": (
                matched_calculation if expression_seen
                else source_field if source_present and direct_projection_seen
                else None
            ),
            "evidence": "explicit Datasphere mapping plus exact SELECT alias/expression" if verified else "explicit source mapping and equivalent SELECT expression were not both proven",
            "status": "VERIFIED" if verified else "UNVERIFIED",
        }
        projection_checks.append(check)
        report(
            "field_mapping",
            {"name": output, "path": field.get("path")},
            {"output_field": output, "source_field": source_field or None},
            check["s4hana_expression"],
            verified,
            check["evidence"] if verified else (
                f"Output {output!r} has no verified source mapping and equivalent SELECT alias/expression."
            ),
        )

    filter_checks = []
    for item in transformation_ir.get("filters", []) or []:
        expression = item.get("condition") or item.get("expression")
        translated_expression = _expand_case_filter_reference(
            expression,
            calculation_expressions_by_name,
        )
        candidates = [expression]
        if translated_expression:
            candidates.append(translated_expression)
        try:
            dependency_expanded = _expand_calculated_field_dependencies(
                str(expression or ""),
                calculation_expressions_by_name,
                projection_sources,
            )
        except ValueError:
            dependency_expanded = None
        if dependency_expanded and dependency_expanded != expression:
            candidates.append(dependency_expanded)
        matched_candidate = next(
            (
                candidate for candidate in candidates
                if _normalize_expression(candidate)
                and _normalize_expression(candidate) in normalized_filter_clauses
            ),
            None,
        )
        verified = matched_candidate is not None
        matched_expression = (
            _where_clause(s4hana_sql).strip()
            if verified and translated_expression and matched_candidate == translated_expression
            else matched_candidate
        )
        check = {
            "owner": item.get("owner"), "path": item.get("path") or item.get("location"),
            "expression": expression,
            "status": "VERIFIED" if verified else "UNVERIFIED",
            "s4hana_expression": matched_expression if verified else None,
        }
        filter_checks.append(check)
        report(
            "filter", item, expression, matched_expression, verified,
            None if verified else "Filter expression was not found in the generated SQL WHERE or HAVING clause.",
        )

    join_checks = []
    for item in transformation_ir.get("joins", []) or []:
        condition = item.get("condition") or item.get("on")
        join_type = str(item.get("join_type") or item.get("type") or "inner").casefold()
        type_terms = {
            "full": "full", "left": "left", "right": "right",
            "cross": "cross", "inner": "inner",
        }
        expected_type = next((kind for key, kind in type_terms.items() if key in join_type), "inner")
        condition_verified = (
            expected_type == "cross"
            or bool(condition) and any(
                _normalize_expression(condition) in clause
                for clause in normalized_join_clauses
            )
        )
        type_verified = bool(re.search(
            rf"(?i)\b{expected_type}\s+(?:outer\s+)?join\b", s4hana_sql
        ))
        supported_join_type = expected_type in {"inner", "left"}
        verified = supported_join_type and condition_verified and type_verified
        check = {
            "owner": item.get("owner"), "path": item.get("path"),
            "join_type": join_type, "condition": condition,
            "status": "VERIFIED" if verified else "UNVERIFIED",
            "s4hana_expression": condition if verified else None,
        }
        join_checks.append(check)
        report(
            "join",
            item,
            {"join_type": join_type, "condition": condition},
            (
                f"{expected_type.upper()} JOIN"
                + (f" ON {condition}" if condition else "")
            ) if verified else None,
            verified,
            None if supported_join_type else (
                f"{expected_type.upper()} JOIN is not supported by the verified "
                "ABAP Open SQL query subset."
            ) if not verified else None,
        )

    aggregation_checks = []
    for item in transformation_ir.get("aggregations", []) or []:
        output = str(item.get("name") or "")
        if output and output.casefold() not in required_calculation_names | {
            name.casefold() for name in selected_fields
        }:
            continue
        aggregation = str(item.get("aggregation") or "").lstrip("#").casefold()
        output_expression = next(
            (
                expression for expression in _split_sql_expressions(raw_select_clause)
                if output and _has_select_alias(expression, output)
            ),
            None,
        )
        output_expression_body = (
            re.split(r"(?i)\s+as\s+", output_expression, maxsplit=1)[0]
            if output_expression else ""
        )
        verified = bool(
            aggregation
            and output_expression_body
            and re.search(
                rf"(?is)\b{re.escape(aggregation)}\s*\(",
                output_expression_body,
            )
        )
        check = {
            "owner": item.get("owner"), "field": output, "aggregation": aggregation,
            "status": "VERIFIED" if verified else "UNVERIFIED",
            "s4hana_expression": (
                f"{aggregation.upper()}(...) AS {output}" if verified else None
            ),
        }
        aggregation_checks.append(check)
        report(
            "aggregation",
            item,
            {"field": output, "aggregation": aggregation},
            check["s4hana_expression"],
            verified,
            None if verified else (
                f"{aggregation or 'Unspecified aggregation'} was not found on output {output!r} "
                "in the generated SELECT list."
            ),
        )

    parameter_checks = []
    bound_parameters = {
        str(name).casefold(): (entity, value)
        for entity, values in (cds_parameters or {}).items()
        for name, value in (values or {}).items()
    }
    for item in transformation_ir.get("parameters", []) or []:
        name = str(item.get("name") or "")
        bound = bound_parameters.get(name.casefold())
        verified = False
        bound_expression = None
        if bound is not None:
            entity, value = bound
            expected = _normalize_expression(
                f"{name}={DataRetriever._format_parameter_value(value)}"
            )
            verified = expected in normalized_sql
            bound_expression = f"{name}={DataRetriever._format_parameter_value(value)}"
        else:
            entity = None
        check = {
            "owner": item.get("owner"), "path": item.get("path"),
            "entity": entity, "parameter": name,
            "s4hana_expression": bound_expression if verified else None,
            "status": "VERIFIED" if verified else "UNVERIFIED",
        }
        parameter_checks.append(check)
        report(
            "parameter_binding",
            item,
            item,
            bound_expression,
            verified,
            f"No verified S/4HANA binding was found for Datasphere parameter {name!r}."
            if not verified else None,
        )
    transformed_parameter_names = {
        str(item.get("name") or "").casefold()
        for item in (transformation_ir.get("parameters") or [])
    }
    for entity, values in (cds_parameters or {}).items():
        for name, value in (values or {}).items():
            if str(name).casefold() in transformed_parameter_names:
                continue
            bound_expression = f"{name}={DataRetriever._format_parameter_value(value)}"
            verified = _normalize_expression(bound_expression) in normalized_sql
            check = {
                "owner": entity, "entity": entity, "parameter": name,
                "s4hana_expression": bound_expression if verified else None,
                "status": "VERIFIED" if verified else "UNVERIFIED",
            }
            parameter_checks.append(check)
            report(
                "cds_parameter_binding",
                check,
                {"parameter": name, "value": value},
                bound_expression,
                verified,
                None if verified else (
                    f"CDS parameter {name!r} was not found in the generated ABAP Open SQL."
                ),
            )

    constant_checks = []
    for item in transformation_ir.get("constants", []) or []:
        output = str(item.get("output_field") or item.get("name") or "")
        if output and output.casefold() not in {name.casefold() for name in selected_fields}:
            continue
        value = item.get("value", item.get("expression"))
        expression = _coverage_expression(value)
        candidates = _open_sql_expression_candidates(
            expression,
            constant=isinstance(value, str),
        )
        matched_constant = next(
            (
                candidate for candidate in candidates
                if _normalize_expression(candidate) in normalized_sql
            ),
            None,
        )
        alias_seen = bool(output) and _has_select_alias(raw_select_clause, output)
        verified = matched_constant is not None and alias_seen
        check = {
            "owner": item.get("owner"), "path": item.get("path"),
            "output_field": output, "expression": expression,
            "s4hana_expression": matched_constant if verified else None,
            "status": "VERIFIED" if verified else "UNVERIFIED",
        }
        constant_checks.append(check)
        report("constant", item, value, matched_constant, verified)

    calculation_checks = []
    for kind, entries, value_key in (
        ("calculation", transformation_ir.get("calculations") or [], "expression"),
        ("formula", transformation_ir.get("formulas") or [], "formula"),
    ):
        for item in entries:
            name = str(item.get("name") or "").casefold()
            if name not in required_calculation_names:
                continue
            expression = item.get(value_key) or item.get("expression")
            if str(item.get("transformation_category") or "").casefold() == "constant":
                continue
            candidates = _open_sql_expression_candidates(expression)
            if expression:
                try:
                    expanded_expression = _expand_calculated_field_dependencies(
                        str(expression),
                        calculation_expressions_by_name,
                        projection_sources,
                    )
                except ValueError:
                    expanded_expression = ""
                if expanded_expression:
                    candidates.extend(
                        _open_sql_expression_candidates(expanded_expression)
                    )
            matched_expression = next(
                (
                    candidate for candidate in candidates
                    if _normalize_expression(candidate) in normalized_sql
                ),
                None,
            )
            verified = matched_expression is not None
            check = {
                "owner": item.get("owner"), "name": item.get("name"),
                "path": item.get("path"), "expression": expression,
                "s4hana_expression": matched_expression,
                "status": "VERIFIED" if verified else "UNVERIFIED",
            }
            calculation_checks.append(check)
            report(
                kind, item, expression, matched_expression, verified,
                None if verified else (
                    f"{kind.capitalize()} expression for {item.get('name') or 'unnamed output'} "
                    "was not found in the generated ABAP Open SQL."
                ),
            )

    grouping_checks = []
    has_aggregation = bool(aggregation_checks) or any(
        target_by_name.get(name.casefold(), {}).get("aggregation")
        for name in selected_fields
    )
    grouping_items = list(transformation_ir.get("groupings") or [])
    if has_aggregation:
        selected_names = {name.casefold() for name in selected_fields}
        grouping_items.extend(
            {
                "owner": "selected_fact_model_output",
                "path": f"selected_fields.{name}",
                "expression": (
                    target_by_name.get(name.casefold(), {}).get("source_field")
                    or target_by_name.get(name.casefold(), {}).get("sourceColumn")
                    or explicit_projection_map.get(name.casefold())
                    or (calc_by_name.get(name.casefold()) or {}).get("expression")
                    or name
                ),
            }
            for name in selected_fields
            if name.casefold() in selected_names
            and not (
                target_by_name.get(name.casefold(), {}).get("aggregation")
                or (calc_by_name.get(name.casefold()) or {}).get("aggregation")
                or str(
                    (calc_by_name.get(name.casefold()) or {}).get("transformation_category")
                    or ""
                ).casefold() == "constant"
            )
        )
    seen_grouping_expressions = set()
    expanded_grouping_items = []
    for item in grouping_items:
        raw_expressions = item.get("expression")
        if isinstance(raw_expressions, list):
            expanded_grouping_items.extend(
                {
                    **item,
                    "path": f"{item.get('path') or 'groupBy'}[{index}]",
                    "expression": expression,
                }
                for index, expression in enumerate(raw_expressions)
            )
        else:
            expanded_grouping_items.append(item)
    for item in expanded_grouping_items:
        expression = _coverage_expression(item.get("expression"))
        normalized = _normalize_expression(expression)
        if normalized in seen_grouping_expressions:
            continue
        seen_grouping_expressions.add(normalized)
        candidates = [expression] if expression else []
        grouping_name = expression.rsplit(".", 1)[-1].casefold()
        calculation = calc_by_name.get(grouping_name)
        if calculation is None:
            calculation = next(
                (
                    candidate for candidate in calc_by_name.values()
                    if _normalize_expression(
                        candidate.get("expression") or candidate.get("formula")
                    ) == normalized
                ),
                None,
            )
        if calculation:
            item = {
                **item,
                "referenced_columns": calculation.get("referenced_columns", []),
            }
            calculation_expression = (
                calculation.get("expression") or calculation.get("formula")
            )
            candidates.extend(_open_sql_expression_candidates(
                calculation_expression,
                constant=str(
                    calculation.get("transformation_category") or ""
                ).casefold() == "constant",
            ))
            if calculation_expression:
                try:
                    expanded_grouping = _expand_calculated_field_dependencies(
                        str(calculation_expression),
                        calculation_expressions_by_name,
                        projection_sources,
                    )
                except ValueError:
                    expanded_grouping = ""
                if expanded_grouping:
                    candidates.extend(
                        _open_sql_expression_candidates(expanded_grouping)
                    )
        target_field = target_by_name.get(grouping_name)
        if target_field:
            mapped_source = (
                target_field.get("source_field")
                or target_field.get("sourceField")
                or target_field.get("source_column")
                or target_field.get("sourceColumn")
            )
            if mapped_source:
                candidates.append(str(mapped_source))
        matched_expression = next(
            (
                candidate for candidate in candidates
                if _normalize_expression(candidate) in group_expressions
            ),
            None,
        )
        verified = matched_expression is not None
        check = {
            "owner": item.get("owner"), "path": item.get("path"),
            "expression": expression,
            "s4hana_expression": matched_expression if verified else None,
            "status": "VERIFIED" if verified else "UNVERIFIED",
        }
        grouping_checks.append(check)
        report(
            "grouping", item, expression, matched_expression, verified,
            None if verified else (
                f"Grouping expression {expression!r} and its verified source/calculation "
                "equivalent were not found in GROUP BY."
            ),
        )

    null_checks = []
    for item in transformation_ir.get("null_handling", []) or []:
        item_name = str(item.get("name") or "").casefold()
        if item_name and item_name not in required_calculation_names:
            continue
        expression = item.get("expression")
        verified, matched_expression, diagnostic = _match_null_expression(
            expression,
            normalized_sql,
        )
        check = {
            "owner": item.get("owner"), "path": item.get("path"),
            "expression": expression,
            "s4hana_expression": matched_expression,
            "status": "VERIFIED" if verified else "UNVERIFIED",
        }
        null_checks.append(check)
        report(
            "null_handling", item, expression, matched_expression,
            verified, diagnostic,
        )

    distinct_checks = []
    for item in transformation_ir.get("distincts", []) or []:
        verified = bool(re.search(r"(?is)\bselect\s+distinct\b", s4hana_sql))
        check = {
            "owner": item.get("owner"), "path": item.get("path"),
            "s4hana_expression": "SELECT DISTINCT" if verified else None,
            "status": "VERIFIED" if verified else "UNVERIFIED",
        }
        distinct_checks.append(check)
        report(
            "distinct", item, item.get("value"), check["s4hana_expression"], verified,
            None if verified else "Datasphere DISTINCT semantics are not present in the generated SELECT.",
        )

    unsupported = transformation_ir.get("unsupported_constructs") or []
    for item in unsupported:
        report(
            "unsupported_construct",
            item,
            item.get("construct"),
            None,
            False,
            f"Datasphere construct {item.get('construct')!r} has no verified S/4HANA Open SQL translation.",
        )
    checks = (
        projection_checks + filter_checks + join_checks + aggregation_checks
        + parameter_checks + calculation_checks + constant_checks
        + grouping_checks + null_checks + distinct_checks
    )
    overall = (
        "VERIFIED"
        if selected_fields
        and checks
        and all(item.get("status") == "VERIFIED" for item in checks)
        and not unsupported
        else "SEMANTIC_EQUIVALENCE_UNVERIFIED"
    )
    failed_items = [item for item in coverage_items if item["status"] != "VERIFIED"]
    diagnostic = None
    if overall != "VERIFIED":
        diagnostic = (
            "Unresolved Datasphere transformations: " + "; ".join(
                f"{item['transformation_id']} [{item['state']}]"
                + (f" at {item['datasphere_path']}" if item.get("datasphere_path") else "")
                + (
                    f" ({item['datasphere_expression']})"
                    if item.get("datasphere_expression") is not None else ""
                )
                + f" — {item['diagnostic']}"
                for item in failed_items
            )
            if failed_items
            else "No complete transformation coverage could be established for the selected output fields."
        )
    return {
        "overall_status": overall,
        "projection": {"status": "VERIFIED" if all(item["status"] == "VERIFIED" for item in projection_checks) else "UNVERIFIED", "items": projection_checks},
        "filters": {"status": "VERIFIED" if all(item["status"] == "VERIFIED" for item in filter_checks) else "UNVERIFIED", "items": filter_checks},
        "joins": {"status": "VERIFIED" if all(item["status"] == "VERIFIED" for item in join_checks) else "UNVERIFIED", "items": join_checks},
        "aggregations": {"status": "VERIFIED" if all(item["status"] == "VERIFIED" for item in aggregation_checks) else "UNVERIFIED", "items": aggregation_checks},
        "parameters": {"status": "VERIFIED" if all(item["status"] == "VERIFIED" for item in parameter_checks) else "UNVERIFIED", "items": parameter_checks},
        "calculations": {"status": "VERIFIED" if all(item["status"] == "VERIFIED" for item in calculation_checks) else "UNVERIFIED", "items": calculation_checks},
        "groupings": {"status": "VERIFIED" if all(item["status"] == "VERIFIED" for item in grouping_checks) else "UNVERIFIED", "items": grouping_checks},
        "constants": {"status": "VERIFIED" if all(item["status"] == "VERIFIED" for item in constant_checks) else "UNVERIFIED", "items": constant_checks},
        "distinct": {"status": "VERIFIED" if all(item["status"] == "VERIFIED" for item in distinct_checks) else "UNVERIFIED", "items": distinct_checks},
        "null_handling": {"status": "VERIFIED" if all(item["status"] == "VERIFIED" for item in null_checks) else "UNVERIFIED", "items": null_checks},
        "items": coverage_items,
        "unsupported_constructs": unsupported,
        "diagnostic": diagnostic,
    }


def _reconciliation_incomplete_response(
    *,
    view_name: str,
    space_id: str,
    source_cds_view: str,
    user_prompt: str,
    target_fields: List[str],
    key_fields: List[str],
    architecture_metadata: Dict[str, Any],
    reconstruction: Dict[str, Any],
    message: str,
    status: str = "INDETERMINATE",
    approval_required: bool = False,
    execution_plan: Optional[Dict[str, Any]] = None,
) -> ValidationResponse:
    return ValidationResponse(
        status=status,
        view_name=view_name,
        space_id=space_id,
        source_cds_view=source_cds_view,
        target_metric=target_fields[0] if target_fields else "",
        target_metrics=target_fields,
        reconciliation_dimension=f"Keys: {', '.join(key_fields)}" if key_fields else "",
        user_prompt=user_prompt,
        datasphere={
            "view": view_name, "sql": reconstruction.get("datasphere_sql", ""),
            "total_kpi": None, "formatted_kpi": "Not executed", "record_count": None,
            "latency_seconds": 0, "execution_engine": "Not executed",
            "query_status": reconstruction.get("datasphere_query_status", "NOT_ATTEMPTED"),
            "total_rows": None,
            "truncated": reconstruction.get("datasphere_query_status") == "SUCCEEDED_TRUNCATED",
            "columns": [],
            "data": [], "error": message,
        },
        s4hana={
            "cds_view": source_cds_view, "sql": reconstruction.get("s4hana_sql", ""),
            "total_kpi": None, "formatted_kpi": "Not executed", "record_count": None,
            "latency_seconds": 0, "execution_engine": "Not executed",
            "query_status": reconstruction.get("s4hana_query_status", "NOT_ATTEMPTED"),
            "metadata_status": architecture_metadata.get("s4hana_metadata_status", "NOT_ATTEMPTED"),
            "total_rows": None,
            "truncated": reconstruction.get("s4hana_query_status") == "SUCCEEDED_TRUNCATED",
            "columns": [],
            "data": [], "error": message,
        },
        comparison={
            "target_metric": target_fields[0] if target_fields else "",
            "delta": None, "variance_pct": None, "is_match": False,
            "summary_text": f"Validation indeterminate: {message}",
            "security": "Metadata-only planning; no transaction rows sent to the LLM.",
        },
        metrics_comparison=[],
        lineage_chain=[],
        grain_breakdown=[],
        architecture_metadata=architecture_metadata,
        reconstruction={**reconstruction, "status": status, "message": message},
        approval_required=approval_required,
        execution_plan=execution_plan,
    )


def _aggregate_measure_comparisons(
    *,
    selected_fields: List[str],
    target_columns: List[Dict[str, Any]],
    transformation_ir: Dict[str, Any],
    datasphere_rows: List[Dict[str, Any]],
    s4hana_rows: List[Dict[str, Any]],
    datasphere_status: str,
    s4hana_status: str,
    semantic_status: str,
) -> List[Dict[str, Any]]:
    """Compare only metadata-declared measures whose result semantics are comparable."""
    transformation_ir = (
        transformation_ir if isinstance(transformation_ir, dict) else {}
    )
    target_by_name = {
        str(item.get("name") or "").casefold(): item
        for item in target_columns
        if isinstance(item, dict) and item.get("name")
    }
    aggregations_by_name = {
        str(item.get("name") or "").casefold(): str(item.get("aggregation") or "")
        for item in transformation_ir.get("aggregations", [])
        if isinstance(item, dict) and item.get("name")
    }
    success_statuses = {
        "SUCCEEDED_WITH_ROWS",
        "SUCCEEDED_ZERO_ROWS",
    }
    complete = datasphere_status in success_statuses and s4hana_status in success_statuses
    truncated = (
        datasphere_status == "SUCCEEDED_TRUNCATED"
        or s4hana_status == "SUCCEEDED_TRUNCATED"
    )
    completeness = "TRUNCATED" if truncated else ("COMPLETE" if complete else "UNKNOWN")
    measures = []
    for field in selected_fields:
        metadata = target_by_name.get(field.casefold(), {})
        aggregation = str(
            metadata.get("aggregation")
            or aggregations_by_name.get(field.casefold())
            or ""
        ).lstrip("#").upper()
        is_measure = bool(
            aggregation
            or metadata.get("is_measure")
            or metadata.get("measure_type")
        )
        if not is_measure:
            continue

        result = {
            "metric_name": field,
            "aggregation": aggregation or None,
            "datasphere_total": None,
            "formatted_datasphere": "Not computed",
            "s4hana_total": None,
            "formatted_s4hana": "Not computed",
            "delta": None,
            "variance_pct": None,
            "relative_difference_pct": None,
            "is_match": False,
            "status": "NOT_COMPUTED",
            "completeness": completeness,
            "reason": None,
        }
        if truncated:
            result.update({
                "status": "INDETERMINATE",
                "reason": "At least one result set is truncated; complete aggregates cannot be established.",
            })
            measures.append(result)
            continue
        if not complete:
            result["reason"] = "Both Datasphere and S/4HANA queries must execute successfully."
            measures.append(result)
            continue
        if semantic_status != "VERIFIED":
            result.update({
                "status": "INDETERMINATE",
                "reason": "Transformation semantics are not verified for both query plans.",
            })
            measures.append(result)
            continue
        if aggregation not in {"SUM", "COUNT", "MIN", "MAX", "AVG", "COUNT_DISTINCT"}:
            result.update({
                "status": "INDETERMINATE",
                "reason": "Verified aggregation semantics are missing or unsupported.",
            })
            measures.append(result)
            continue

        dimensions = [
            name for name in selected_fields
            if name.casefold() != field.casefold()
            and not (
                target_by_name.get(name.casefold(), {}).get("aggregation")
                or target_by_name.get(name.casefold(), {}).get("is_measure")
                or target_by_name.get(name.casefold(), {}).get("measure_type")
                or aggregations_by_name.get(name.casefold())
            )
        ]

        def row_value(row: Dict[str, Any], name: str) -> Any:
            return next(
                (value for key, value in row.items() if str(key).casefold() == name.casefold()),
                None,
            )

        def grouped_values(rows: List[Dict[str, Any]]) -> Optional[Dict[Tuple[Any, ...], List[Any]]]:
            grouped: Dict[Tuple[Any, ...], List[Any]] = {}
            for row in rows:
                if not isinstance(row, dict):
                    return None
                if dimensions:
                    keys = tuple(row_value(row, name) for name in dimensions)
                    available_fields = {str(key).casefold() for key in row}
                    if any(name.casefold() not in available_fields for name in dimensions):
                        return None
                else:
                    keys = ()
                grouped.setdefault(keys, []).append(row_value(row, field))
            return grouped

        datasphere_groups = grouped_values(datasphere_rows)
        s4hana_groups = grouped_values(s4hana_rows)
        if (
            datasphere_groups is None
            or s4hana_groups is None
            or not datasphere_groups
            or not s4hana_groups
        ):
            result.update({
                "status": "INDETERMINATE",
                "reason": "A complete result did not contain comparable aggregate rows and grouping fields.",
            })
            measures.append(result)
            continue
        if any(len(values) != 1 for values in datasphere_groups.values()) or any(
            len(values) != 1 for values in s4hana_groups.values()
        ):
            result.update({
                "status": "INDETERMINATE",
                "reason": "Results contain duplicate rows at the declared aggregate grouping grain.",
            })
            measures.append(result)
            continue

        def decimal_value(value: Any) -> Optional[Decimal]:
            if value is None or str(value).strip() == "":
                return None
            try:
                number = Decimal(str(value).replace(",", "").strip())
            except (InvalidOperation, ValueError, TypeError):
                return None
            return number if number.is_finite() else None

        def roll_up(groups: Dict[Tuple[Any, ...], List[Any]]) -> Optional[Decimal]:
            values = [decimal_value(value) for group in groups.values() for value in group]
            if not values or any(value is None for value in values):
                return None
            numeric_values = [value for value in values if value is not None]
            if aggregation in {"SUM", "COUNT"}:
                return sum(numeric_values, Decimal(0))
            if aggregation == "MIN":
                return min(numeric_values)
            if aggregation == "MAX":
                return max(numeric_values)
            if aggregation == "AVG" and len(numeric_values) == 1:
                return numeric_values[0]
            if aggregation == "COUNT_DISTINCT" and len(numeric_values) == 1:
                return numeric_values[0]
            return None

        datasphere_total = roll_up(datasphere_groups)
        s4hana_total = roll_up(s4hana_groups)
        if datasphere_total is None or s4hana_total is None:
            reason = (
                "Grouped AVG values cannot be rolled up without verified counts or sum components."
                if aggregation == "AVG" and (len(datasphere_groups) > 1 or len(s4hana_groups) > 1)
                else "This aggregation cannot be safely rolled up from the returned grouped values."
            )
            result.update({"status": "INDETERMINATE", "reason": reason})
            measures.append(result)
            continue

        delta = abs(datasphere_total - s4hana_total)
        group_values_match = (
            set(datasphere_groups) == set(s4hana_groups)
            and all(
                decimal_value(datasphere_groups[key][0])
                == decimal_value(s4hana_groups[key][0])
                for key in datasphere_groups
            )
        )
        relative = (
            delta / abs(datasphere_total) * 100
            if datasphere_total != 0 else None
        )
        result.update({
            "datasphere_total": float(datasphere_total),
            "formatted_datasphere": f"{datasphere_total:,}",
            "s4hana_total": float(s4hana_total),
            "formatted_s4hana": f"{s4hana_total:,}",
            "delta": float(delta),
            "variance_pct": float(relative) if relative is not None else None,
            "relative_difference_pct": float(relative) if relative is not None else None,
            "status": "MATCH" if delta == 0 else "MISMATCH",
            "group_status": (
                "NOT_APPLICABLE"
                if not dimensions
                else ("MATCH" if group_values_match else "MISMATCH")
            ),
            "is_match": delta == 0,
            "reason": None if relative is not None else "Relative difference is undefined when the Datasphere value is zero.",
        })
        measures.append(result)
    return measures


async def _fact_query_only_response(
    *,
    request: ValidationRequest,
    view_name: str,
    space_id: str,
    source_cds_view: str,
    user_prompt: str,
    target_fields: List[str],
    key_fields: List[str],
    architecture_metadata: Dict[str, Any],
    reconstruction: Dict[str, Any],
    target: Any,
    reason: str,
    source_metadata: Optional[List[Dict[str, Any]]] = None,
    discovered_sources: Optional[List[str]] = None,
) -> ValidationResponse:
    """Run a fact query and only a transformation-verified S/4HANA reconstruction."""
    columns = [str(field["name"]) for field in target.columns if field.get("name")]
    source_metadata = [item for item in (source_metadata or []) if isinstance(item, dict)]
    discovered_sources = list(discovered_sources or [])
    signature_payload = {
        "model": view_name,
        "space": space_id,
        "target": target.as_dict(),
        "prompt": user_prompt,
        "fields": target_fields,
        "filters": request.custom_filters or "",
        "source_metadata": source_metadata,
        "discovered_sources": discovered_sources,
        "cds_parameters": request.cds_parameters or {},
        "comparison_grain": architecture_metadata.get("reconciliation_grain", {}),
        "transformation_ir_digest": architecture_metadata.get("transformation_ir_digest"),
    }
    signature = _hashlib.sha256(
        json.dumps(signature_payload, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    cached = _FACT_QUERY_PLAN_CACHE.get(str(request.approved_query_hash or ""))
    if cached and cached.get("signature") == signature:
        sql = cached["sql"]
        blueprint = cached["blueprint"]
        s4_queries = cached.get("s4_queries", [])
        s4_plan_errors = cached.get("s4_plan_errors", [])
        transformation_coverage = cached.get("transformation_coverage")
        candidate_s4_sql = str(cached.get("candidate_s4_sql") or "")
    else:
        candidate_s4_sql = str(
            reconstruction.get("equivalence_candidate_s4hana_sql") or ""
        ).strip()
        prompt = user_prompt
        if request.custom_filters:
            prompt = f"{prompt}\nUser-specified filter: {request.custom_filters}".strip()
        blueprint = await generate_odata_query_blueprint(
            prompt,
            {target.fact_model: columns},
            target.fact_model_space or space_id,
            [],
            target_fields,
        )
        raw_sql = str(blueprint.get("sql_query") or "").strip()
        try:
            sql = bind_readonly_sql_to_target(raw_sql, view_name, target)
        except ExecutionTargetError as exc:
            reconstruction.update({"status": "BLOCKED", "mode": "FACT_QUERY_ONLY", "message": str(exc)})
            return _reconciliation_incomplete_response(
                view_name=view_name, space_id=space_id, source_cds_view=source_cds_view,
                user_prompt=user_prompt, target_fields=target_fields, key_fields=key_fields,
                architecture_metadata=architecture_metadata, reconstruction=reconstruction,
                message=f"Fact-model SQL was rejected: {exc}", status="INDETERMINATE",
            )

        s4_queries = []
        s4_plan_errors = []
        transformation_ir = architecture_metadata.get("transformation_ir")
        transformation_coverage = _verify_transformation_coverage(
            transformation_ir if isinstance(transformation_ir, dict) else {},
            s4hana_sql="",
            selected_fields=target_fields,
            target_columns=list(target.columns),
            source_fields=[
                field
                for source in source_metadata
                for field in source.get("fields", [])
                if isinstance(field, dict) and field.get("name")
            ],
            cds_parameters={},
        )
        if not source_metadata:
            s4_plan_errors.append({
                "source": source_cds_view or None,
                "status": "SEMANTIC_EQUIVALENCE_UNVERIFIED",
                "error": (
                    "Verified S/4HANA source metadata is unavailable; a transformed "
                    "ABAP Open SQL query cannot be constructed."
                ),
            })
        else:
            try:
                s4_plan: Dict[str, Any] = {}
                if not candidate_s4_sql:
                    s4_plan = await generate_reconciliation_query_blueprint(
                        user_prompt=user_prompt,
                        model_name=view_name,
                        space_id=space_id,
                        architecture_metadata=architecture_metadata,
                        source_metadata=source_metadata,
                        selected_fields=target_fields,
                        key_fields=[],
                        parameter_overrides=request.cds_parameters,
                    )
                    candidate_s4_sql = str(s4_plan.get("s4hana_sql") or "").strip()
                    reconstruction["generation_diagnostics"] = s4_plan.get(
                        "generation_diagnostics", []
                    )
                    reconstruction["llm_provider"] = s4_plan.get("llm_provider")
                    reconstruction["llm_model"] = s4_plan.get("llm_model")
                candidate_s4_sql = _format_open_sql_lines(candidate_s4_sql)
                candidate_s4_sql, resolved_cds_parameters = _bind_declared_cds_parameters(
                    candidate_s4_sql,
                    source_metadata,
                    request.cds_parameters,
                )
                expected_sources = discovered_sources or [
                    str(item.get("entity"))
                    for item in source_metadata
                    if item.get("entity")
                ]
                _validate_reconciliation_sql(
                    candidate_s4_sql,
                    expected_sources=expected_sources,
                    allowed_source_fields=[
                        str(field.get("name"))
                        for source in source_metadata
                        for field in source.get("fields", [])
                        if isinstance(field, dict) and field.get("name")
                    ],
                    source_field_map={
                        str(source.get("entity")): [
                            str(field.get("name"))
                            for field in source.get("fields", [])
                            if isinstance(field, dict) and field.get("name")
                        ]
                        for source in source_metadata
                        if source.get("entity")
                    },
                    enforce_abap_line_limit=True,
                    enforce_entity_qualified_fields=True,
                )
                transformation_coverage = _verify_transformation_coverage(
                    transformation_ir if isinstance(transformation_ir, dict) else {},
                    s4hana_sql=candidate_s4_sql,
                    selected_fields=target_fields,
                    target_columns=list(target.columns),
                    source_fields=[
                        field
                        for source in source_metadata
                        for field in source.get("fields", [])
                        if isinstance(field, dict) and field.get("name")
                    ],
                    cds_parameters=resolved_cds_parameters,
                )
                reconstruction["equivalence_candidate_s4hana_sql"] = candidate_s4_sql
                unsupported_plan_items = s4_plan.get("unsupported_transformations", [])
                if unsupported_plan_items:
                    s4_plan_errors.append({
                        "source": ", ".join(expected_sources),
                        "status": "TRANSLATION_UNSUPPORTED",
                        "error": "; ".join(str(item) for item in unsupported_plan_items),
                    })
                elif transformation_coverage["overall_status"] == "VERIFIED":
                    source_name = next((
                        str(item.get("entity"))
                        for item in source_metadata
                        if item.get("entity")
                    ), "")
                    if not source_name:
                        raise ValueError("The reconstructed query has no verified CDS entity for ADT execution.")
                    s4_queries.append({
                        "source": source_name,
                        "sql": candidate_s4_sql,
                        "parameters": resolved_cds_parameters,
                        "output_fields": target_fields,
                    })
                else:
                    s4_plan_errors.append({
                        "source": ", ".join(expected_sources),
                        "status": "SEMANTIC_EQUIVALENCE_UNVERIFIED",
                        "error": transformation_coverage["diagnostic"],
                    })
            except Exception as exc:
                s4_plan_errors.append({
                    "source": source_cds_view or None,
                    "status": "RECONSTRUCTION_BLOCKED",
                    "error": str(exc),
                })
        reconstruction["transformation_coverage"] = transformation_coverage
        architecture_metadata["transformation_coverage"] = transformation_coverage

    if transformation_coverage is not None:
        reconstruction["semantic_equivalence_status"] = (
            "VERIFIED"
            if (
                transformation_coverage.get("overall_status") == "VERIFIED"
                and not any(
                    item.get("status") == "TRANSLATION_UNSUPPORTED"
                    for item in s4_plan_errors
                )
            )
            else "SEMANTIC_EQUIVALENCE_UNVERIFIED"
        )
        reconstruction["transformation_coverage"] = transformation_coverage
        architecture_metadata["transformation_coverage"] = transformation_coverage
    mode = "DUAL_QUERY_ONLY" if s4_queries else "FACT_QUERY_ONLY"
    hash_payload = {
        "signature": signature,
        "datasphere_sql": sql,
        "s4hana_queries": s4_queries,
        "s4hana_candidate_sql": candidate_s4_sql,
        "s4hana_plan_errors": s4_plan_errors,
        "transformation_coverage": reconstruction.get("transformation_coverage"),
        "parameters": request.cds_parameters or {},
    }
    query_hash = _hashlib.sha256(
        json.dumps(hash_payload, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    _FACT_QUERY_PLAN_CACHE[query_hash] = {
        "signature": signature,
        "sql": sql,
        "blueprint": blueprint,
        "s4_queries": s4_queries,
        "candidate_s4_sql": candidate_s4_sql,
        "s4_plan_errors": s4_plan_errors,
        "transformation_coverage": reconstruction.get("transformation_coverage"),
    }
    displayed_s4_sql = (
        s4_queries[0]["sql"] if len(s4_queries) == 1 else candidate_s4_sql
    )
    reconstruction.update({
        "status": "PLANNED",
        "mode": mode,
        "datasphere_sql": sql,
        "s4hana_sql": displayed_s4_sql,
        "equivalence_candidate_s4hana_sql": candidate_s4_sql or None,
        "s4hana_queries": s4_queries,
        "s4hana_plan_errors": s4_plan_errors,
        "transformation_coverage": reconstruction.get("transformation_coverage"),
        "s4hana_query_status": "PENDING_APPROVAL" if s4_queries else ("BLOCKED" if s4_plan_errors else "NOT_ATTEMPTED"),
        "output_fields": target_fields,
        "key_fields": [],
        "query_hash": query_hash,
        "logic_summary": str(blueprint.get("explanation") or "LLM query against the verified fact-model runtime object."),
    })
    execution_plan = {
        "model_name": view_name,
        "space_id": space_id,
        "mode": mode,
        "query_hash": query_hash,
        "approval_required": True,
        "datasphere_sql": sql,
        "s4hana_sql": displayed_s4_sql,
        "s4hana_queries": s4_queries,
        "s4hana_plan_errors": s4_plan_errors,
        "transformation_coverage": reconstruction.get("transformation_coverage"),
        "cds_parameters": request.cds_parameters or {},
        "output_fields": target_fields,
        "key_fields": [],
        "execution_readiness": (
            "READY_FOR_APPROVAL" if s4_queries else "FACT_QUERY_ONLY_S4HANA_BLOCKED"
        ),
        "diagnostic": (
            "; ".join(
                str(item.get("error") or item.get("status"))
                for item in s4_plan_errors
            )
            + f"; Comparison grain remains unresolved: {reason}"
            if s4_plan_errors
            else f"Comparison grain remains unresolved: {reason}"
        ),
    }

    if not request.human_approved or request.approved_query_hash != query_hash:
        return ValidationResponse(
            status="NOT_RUN", view_name=view_name, space_id=space_id,
            source_cds_view=source_cds_view,
            target_metric=target_fields[0] if target_fields else "",
            target_metrics=target_fields, reconciliation_dimension="",
            user_prompt=user_prompt,
            datasphere={
                "view": target.fact_model, "sql": sql, "total_kpi": None,
                "formatted_kpi": "Pending Approval", "record_count": None,
                "latency_seconds": 0, "execution_engine": "Pending Approval",
                "query_status": "PENDING_APPROVAL", "data": [], "error": None,
            },
            s4hana={
                "cds_view": source_cds_view, "sql": displayed_s4_sql,
                "queries": s4_queries, "source_results": [],
                "columns": [], "total_kpi": None, "formatted_kpi": "Pending Approval",
                "record_count": None, "latency_seconds": 0,
                "execution_engine": "Pending Approval",
                "query_status": "PENDING_APPROVAL" if s4_queries else "BLOCKED",
                "metadata_status": architecture_metadata.get("s4hana_metadata_status", "NOT_ATTEMPTED"),
                "data": [],
                "diagnostics": s4_plan_errors,
                "error": None if s4_queries else "; ".join(
                    str(item.get("error") or item.get("status")) for item in s4_plan_errors
                ) or reason,
            },
            comparison={
                "target_metric": target_fields[0] if target_fields else "",
                "delta": None, "variance_pct": None, "is_match": False,
                "summary_text": (
                    "Approved-plan preview is ready for both the verified Datasphere fact model and "
                    "verified S/4HANA CDS source(s). Reconciliation remains blocked: " + reason
                    if s4_queries else
                    "Fact-model query is ready for approval. S/4HANA query is blocked by metadata/parameter requirements; "
                    f"reconciliation remains blocked: {reason}"
                ),
                "security": "Read-only queries; approval binds both SQL plans, source metadata, parameter values, mappings, and grain. No match is claimed.",
            },
            metrics_comparison=_aggregate_measure_comparisons(
                selected_fields=target_fields,
                target_columns=list(target.columns),
                transformation_ir=architecture_metadata.get("transformation_ir", {}),
                datasphere_rows=[],
                s4hana_rows=[],
                datasphere_status="PENDING_APPROVAL",
                s4hana_status="PENDING_APPROVAL" if s4_queries else "BLOCKED",
                semantic_status=reconstruction.get("semantic_equivalence_status", "UNVERIFIED"),
            ), lineage_chain=[], grain_breakdown=[],
            architecture_metadata=architecture_metadata,
            reconstruction={**reconstruction, "message": reason},
            approval_required=True, execution_plan=execution_plan,
        )

    datasphere_result: Dict[str, Any]
    try:
        ds_rows, ds_latency, executed_sql = await asyncio.to_thread(
            execute_direct_hana_query, sql, target.fact_model_space or space_id, user_prompt
        )
        datasphere_result = {
            "query_status": "SUCCEEDED_ZERO_ROWS" if not ds_rows else "SUCCEEDED_WITH_ROWS",
            "rows": ds_rows,
            "row_count": len(ds_rows),
            "total_rows": len(ds_rows),
            "truncated": False,
            "latency_seconds": round(ds_latency, 3),
            "sql": executed_sql,
            "error": None,
        }
    except Exception as exc:
        datasphere_result = {
            "query_status": "EXECUTION_FAILED", "rows": [], "row_count": None,
            "total_rows": None, "truncated": None,
            "latency_seconds": 0, "sql": sql, "error": str(exc),
        }

    if s4_queries and USE_SAP_ADT and DataRetriever is not None:
        adt_result = await asyncio.to_thread(
            _execute_adt_source_queries,
            s4_queries,
            max(1, min(int(ADT_ROW_LIMIT or 100), 1000)),
        )
        source_results = adt_result["sources"]
    else:
        source_results = []
        adt_result = {"latency_seconds": 0}
        if s4_queries:
            s4_plan_errors.append({
                "source": source_cds_view,
                "status": "EXECUTION_BLOCKED",
                "error": "SAP ADT credentials or the DataRetriever client are not configured.",
            })

    adt_rows = []
    adt_columns = []
    adt_totals = [item.get("total_rows") for item in source_results]
    adt_total_rows = (
        sum(int(value) for value in adt_totals)
        if source_results and all(isinstance(value, int) and not isinstance(value, bool) for value in adt_totals)
        else None
    )
    for source_result in source_results:
        for column in source_result.get("columns", []):
            adt_columns.append({"source": source_result["source"], **column})
        if len(source_results) == 1:
            adt_rows.extend(source_result.get("rows", []))
        else:
            for row in source_result.get("rows", []):
                adt_rows.append({"__source_entity": source_result["source"], **row})
    adt_statuses = [item.get("status") for item in source_results]
    if any(status == "EXECUTION_FAILED" for status in adt_statuses):
        adt_query_status = "PARTIAL_FAILURE" if any(status.startswith("SUCCEEDED") for status in adt_statuses) else "EXECUTION_FAILED"
    elif any(status == "SUCCEEDED_TRUNCATED" for status in adt_statuses):
        adt_query_status = "SUCCEEDED_TRUNCATED"
    elif source_results and all(status == "SUCCEEDED_ZERO_ROWS" for status in adt_statuses):
        adt_query_status = "SUCCEEDED_ZERO_ROWS"
    elif source_results:
        adt_query_status = "SUCCEEDED_WITH_ROWS"
    elif s4_plan_errors:
        adt_query_status = "BLOCKED"
    else:
        adt_query_status = "NOT_ATTEMPTED"

    any_query_succeeded = datasphere_result["query_status"].startswith("SUCCEEDED") or any(
        status.startswith("SUCCEEDED") for status in adt_statuses
    )
    reconstruction.update({
        "status": "QUERY_ONLY_EXECUTED" if any_query_succeeded else "EXECUTION_FAILED",
        "mode": mode,
        "datasphere_query_status": datasphere_result["query_status"],
        "s4hana_query_status": adt_query_status,
        "s4hana_metadata_status": architecture_metadata.get("s4hana_metadata_status", "NOT_ATTEMPTED"),
        "datasphere_sql": datasphere_result["sql"],
        "s4hana_sql": displayed_s4_sql,
        "s4hana_queries": s4_queries,
        "s4hana_plan_errors": s4_plan_errors,
        "query_hash": query_hash,
    })
    response = _reconciliation_incomplete_response(
        view_name=view_name, space_id=space_id, source_cds_view=source_cds_view,
        user_prompt=user_prompt, target_fields=target_fields, key_fields=[],
        architecture_metadata=architecture_metadata, reconstruction=reconstruction,
        message=f"Independent query execution completed; reconciliation remains blocked because the comparison grain is unresolved: {reason}",
        status="INDETERMINATE",
    )
    response.datasphere.update({
        "view": target.fact_model,
        "sql": datasphere_result["sql"],
        "total_kpi": datasphere_result["row_count"],
        "formatted_kpi": f"{datasphere_result['row_count']} rows" if datasphere_result["query_status"].startswith("SUCCEEDED") else "Execution failed",
        "record_count": datasphere_result["row_count"],
        "total_rows": datasphere_result["total_rows"],
        "truncated": datasphere_result["truncated"],
        "latency_seconds": datasphere_result["latency_seconds"],
        "execution_engine": "SAP HANA Cloud Direct SQL" if datasphere_result["query_status"].startswith("SUCCEEDED") else "SAP HANA Cloud Direct SQL (Error)",
        "query_status": datasphere_result["query_status"],
        "columns": [
            {
                "name": str(column.get("name")),
                "type": column.get("data_type") or column.get("type"),
                "description": column.get("description"),
            }
            for column in target.columns
            if isinstance(column, dict)
            and column.get("name")
            and str(column.get("name")).casefold() in {
                name.casefold() for name in target_fields
            }
        ] if datasphere_result["query_status"].startswith("SUCCEEDED") else [],
        "data": datasphere_result["rows"],
        "error": datasphere_result["error"],
    })
    response.s4hana.update({
        "cds_view": source_cds_view,
        "sql": displayed_s4_sql,
        "queries": s4_queries,
        "source_results": source_results,
        "columns": adt_columns,
        "total_kpi": (
            len(adt_rows) if adt_query_status.startswith("SUCCEEDED")
            or adt_query_status == "PARTIAL_FAILURE" else None
        ),
        "total_rows": adt_total_rows,
        "truncated": adt_query_status == "SUCCEEDED_TRUNCATED",
        "formatted_kpi": (
            f"{len(adt_rows)} rows"
            if adt_query_status.startswith("SUCCEEDED") or adt_query_status == "PARTIAL_FAILURE"
            else "Not executed"
        ),
        "record_count": (
            len(adt_rows) if adt_query_status.startswith("SUCCEEDED")
            or adt_query_status == "PARTIAL_FAILURE" else None
        ),
        "latency_seconds": adt_result["latency_seconds"],
        "execution_engine": "SAP S/4HANA CDS Open SQL (ADT)" if any(status.startswith("SUCCEEDED") for status in adt_statuses) else ("SAP S/4HANA CDS Open SQL (ADT Error)" if adt_statuses else "Not executed"),
        "query_status": adt_query_status,
        "metadata_status": architecture_metadata.get("s4hana_metadata_status", "NOT_ATTEMPTED"),
        "data": adt_rows,
        "diagnostics": s4_plan_errors + [
            {"source": item.get("source"), "status": item.get("status"), "error": item.get("error")}
            for item in source_results if item.get("error")
        ],
        "error": "; ".join(
            str(item.get("error") or item.get("status"))
            for item in s4_plan_errors + [
                {"source": item.get("source"), "status": item.get("status"), "error": item.get("error")}
                for item in source_results if item.get("error")
            ]
        ) or None,
    })
    response.metrics_comparison = _aggregate_measure_comparisons(
        selected_fields=target_fields,
        target_columns=list(target.columns),
        transformation_ir=architecture_metadata.get("transformation_ir", {}),
        datasphere_rows=datasphere_result["rows"],
        s4hana_rows=adt_rows,
        datasphere_status=datasphere_result["query_status"],
        s4hana_status=adt_query_status,
        semantic_status=reconstruction.get("semantic_equivalence_status", "UNVERIFIED"),
    )
    ds_status = datasphere_result["query_status"]
    response.comparison["summary_text"] = (
        f"Datasphere query: {ds_status} ({datasphere_result['row_count'] if datasphere_result['row_count'] is not None else 'unknown'} rows); "
        f"S/4HANA ADT query: {adt_query_status} ({len(adt_rows) if adt_total_rows is not None else 'unknown'} rows). "
        f"No reconciliation match was attempted because the comparison grain is unresolved: {reason}"
    )
    # Prompt-driven aggregate queries (for example, a count filtered to one
    # value) produce a single result row and do not need record-level keys.
    # Compare that result directly when both independently executed queries
    # succeeded and the CDS translation passed transformation coverage.
    is_prompt_aggregate = bool(re.search(
        r"\b(count|sum|average|avg|minimum|min|maximum|max)\b",
        user_prompt,
        re.IGNORECASE,
    )) and bool(re.search(
        r"\b(COUNT|SUM|AVG|MIN|MAX)\s*\(",
        f"{datasphere_result['sql']} {displayed_s4_sql}",
        re.IGNORECASE,
    ))
    if (
        is_prompt_aggregate
        and not key_fields
        and datasphere_result["query_status"].startswith("SUCCEEDED")
        and adt_query_status.startswith("SUCCEEDED")
        and reconstruction.get("semantic_equivalence_status") == "VERIFIED"
        and len(datasphere_result["rows"]) == 1
        and len(adt_rows) == 1
    ):
        ds_row, s4_row = datasphere_result["rows"][0], adt_rows[0]
        ds_values = {str(name).casefold(): value for name, value in ds_row.items()}
        s4_values = {str(name).casefold(): value for name, value in s4_row.items()}
        comparable_fields = sorted(set(ds_values) & set(s4_values))

        def normalized_result(value: Any) -> Any:
            if value is None:
                return None
            try:
                number = Decimal(str(value).replace(",", "").strip())
                return number.normalize() if number.is_finite() else str(value).strip().casefold()
            except (InvalidOperation, ValueError, TypeError):
                return str(value).strip().casefold()

        scalar_match = bool(comparable_fields) and all(
            normalized_result(ds_values[name]) == normalized_result(s4_values[name])
            for name in comparable_fields
        )
        response.status = "VALIDATED" if scalar_match else "MISMATCH"
        response.reconciliation_dimension = "Prompt result (single aggregate row)"
        response.comparison.update({
            "target_metric": comparable_fields[0] if comparable_fields else "",
            "delta": None,
            "variance_pct": None,
            "is_match": scalar_match,
            "summary_text": (
                "The prompt result matches between the Datasphere model and final CDS query."
                if scalar_match else
                "The prompt result differs between the Datasphere model and final CDS query."
                if comparable_fields else
                "Both prompt queries succeeded, but returned no common result columns to compare."
            ),
        })
        response.reconstruction["status"] = "PROMPT_RESULT_VALIDATED"
    return response


def _grain_metadata_diagnostics(
    *,
    view_name: str,
    space_id: str,
    execution_target: Any,
    source_metadata: Optional[List[Dict[str, Any]]] = None,
    requested_keys: Optional[List[str]] = None,
    reason: Optional[str] = None,
) -> Dict[str, Any]:
    """Return safe metadata-only diagnostics for grain resolution."""
    target = execution_target.as_dict() if execution_target is not None else {}
    target_columns = target.get("columns", []) if isinstance(target, dict) else []
    source_metadata = source_metadata or []
    source_fields = [
        str(field.get("name"))
        for source in source_metadata
        if isinstance(source, dict)
        for field in (source.get("fields") or [])
        if isinstance(field, dict) and field.get("name")
    ]
    source_names = {field.casefold() for field in source_fields}
    candidates = []
    for field in target_columns:
        if not isinstance(field, dict) or not field.get("name"):
            continue
        target_name = str(field["name"])
        mapped = (
            field.get("source_field") or field.get("sourceField")
            or field.get("source_column") or field.get("sourceColumn")
            or field.get("mapped_field") or field.get("mappedField")
        )
        evidence = "explicit mapping" if mapped else "same technical name"
        source_name = str(mapped or target_name)
        accepted = source_name.casefold() in source_names
        candidates.append({
            "target_field": target_name,
            "source_field": source_name,
            "evidence": evidence,
            "accepted": accepted,
            "reason": "source field returned by ADT" if accepted else "not present in ADT field metadata",
        })
    diagnostic = {
        "selected_model": view_name,
        "space_id": space_id,
        "execution_target": {
            "logical_fact_model": target.get("fact_model"),
            "physical_object": target.get("object_name"),
            "schema": target.get("schema"),
            "column_count": len(target_columns),
            "columns": [str(field.get("name")) for field in target_columns if isinstance(field, dict) and field.get("name")],
        },
        "resolved_sources": [
            {
                "entity": source.get("entity"),
                "fields": [str(field.get("name")) for field in (source.get("fields") or []) if isinstance(field, dict) and field.get("name")],
                "declared_keys": [str(key) for key in (source.get("key_fields") or []) if str(key).strip()],
            }
            for source in source_metadata
            if isinstance(source, dict)
        ],
        "requested_keys": list(requested_keys or []),
        "candidate_mappings": candidates,
        "final_grain": None,
        "failure_reason": reason,
    }
    logger.info("reconciliation_metadata_diagnostics=%s", json.dumps(diagnostic, sort_keys=True, default=str))
    return diagnostic


def _derive_model_keys(
    dependency_tree: Dict[str, Any],
    replicated_sources: Optional[List[Dict[str, Any]]] = None,
) -> List[str]:
    """Derive output grain and key fields dynamically without hardcoding."""
    field_catalog = dependency_tree.get("field_catalog", []) if isinstance(dependency_tree, dict) else []

    # 1. Primary keys declared directly on model output
    model_keys = [
        str(field["name"])
        for field in field_catalog
        if isinstance(field, dict) and field.get("name") and field.get("is_key")
    ]
    if model_keys:
        return model_keys

    # 2. For aggregated models: derive output grain from grouping dimensions/attributes
    has_measures = any(
        isinstance(f, dict) and (f.get("is_measure") or f.get("measure_type") or f.get("aggregation"))
        for f in field_catalog
    )
    dimensions = [
        str(field["name"])
        for field in field_catalog
        if isinstance(field, dict) and field.get("name")
        and not field.get("is_measure")
        and not field.get("measure_type")
        and not field.get("aggregation")
    ]
    if has_measures and dimensions:
        return dimensions

    # 3. For leaf / unaggregated models: propagate verified source keys that exist on model output
    replicated_sources = replicated_sources or []
    model_field_names = {
        str(f.get("name", "")).casefold()
        for f in field_catalog
        if isinstance(f, dict) and f.get("name")
    }
    leaf_keys = []
    for s in replicated_sources:
        for k in s.get("key_fields", []):
            if k and k.casefold() in model_field_names and k not in leaf_keys:
                leaf_keys.append(k)
    return leaf_keys


async def run_manual_sql_validation(request: ValidationRequest) -> ValidationResponse:
    """Validate, approve, and execute one user-owned query without calling an LLM."""
    view_name = request.view_name.strip()
    space_id = request.space_id.strip() if request.space_id else DATASPHERE_SPACE_ID
    sql = request.manual_sql or ""
    target_name = (request.manual_sql_target or "").strip().casefold()
    if not view_name or not space_id:
        raise HTTPException(status_code=400, detail="An analytical model and Datasphere space are required.")
    if target_name not in {"datasphere", "s4hana"}:
        raise HTTPException(status_code=400, detail="Manual SQL target must be datasphere or s4hana.")

    client = DatasphereResolverClient(space=space_id)
    cache_entry = client.cache.get_model_metadata(space_id, view_name) if client.cache else None
    cached_payload = cache_entry.get("metadata", {}) if isinstance(cache_entry, dict) else {}
    cached_tree = cached_payload.get("dependency_tree") if isinstance(cached_payload, dict) else None
    cache_fresh = bool(
        isinstance(cached_tree, dict)
        and isinstance(cache_entry, dict)
        and time.time() - float(cache_entry.get("last_synced", 0)) <= metadata_cache_ttl_seconds()
    )
    if cache_fresh:
        dependency_tree = cached_tree
    else:
        resolver = DatasphereDependencyResolver(client=client, space=space_id, silent=True)
        try:
            dependency_tree = await asyncio.to_thread(resolver.resolve, view_name)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Could not resolve Datasphere metadata: {exc}") from exc

    if target_name == "datasphere":
        try:
            execution_target = resolve_execution_target(
                dependency_tree,
                selected_fields=[],
                required_keys=[],
                runtime_discovery=lambda *, logical_name, space: discover_hana_execution_object_sync(
                    logical_name=logical_name,
                ),
            )
        except (ExecutionTargetError, ReconciliationGrainError) as exc:
            raise HTTPException(status_code=400, detail=f"Datasphere execution target is unverified: {exc}") from exc
        allowed_sources = [execution_target.object_name]
        allowed_fields = [str(column["name"]) for column in execution_target.columns]
        expected_schema = execution_target.schema
        metadata_identity: Any = execution_target.as_dict()
        source_cds_view = ""
        s4_metadata: list[dict[str, Any]] = []
    else:
        if not USE_SAP_ADT or DataRetriever is None:
            raise HTTPException(status_code=400, detail="S/4HANA ADT execution is not configured.")
        nodes = list(_walk_dependency_nodes(dependency_tree))
        terminal_nodes = [
            node for node in nodes
            if (node.get("is_boundary")
                or node.get("object_type") in ("local_table", "remote_table", "shared_remote_table")
                or not node.get("dependencies"))
            and node.get("name") != view_name
        ]
        lineage_sources: list[str] = []
        for node in terminal_nodes:
            definition = node.get("definition") if isinstance(node.get("definition"), dict) else {}
            source = (
                definition.get("sourceLineage", {}).get("cdsEntity")
                or definition.get("remoteObject", {}).get("name")
                or node.get("source_entity")
            )
            if source and str(source) not in lineage_sources:
                lineage_sources.append(str(source))
        requested_source = (request.source_cds_view or "").strip()
        if not lineage_sources and requested_source and len(terminal_nodes) == 1:
            lineage_sources = [requested_source]
        if requested_source:
            if requested_source.casefold() not in {source.casefold() for source in lineage_sources}:
                raise HTTPException(status_code=400, detail="Selected CDS source is not verified by Datasphere lineage.")
            lineage_sources = [source for source in lineage_sources if source.casefold() == requested_source.casefold()]
        if not lineage_sources:
            raise HTTPException(status_code=400, detail="No S/4HANA CDS source could be verified from Datasphere lineage.")
        cfg = get_config()
        try:
            with CDSMetadataClient(cfg["base_url"], cfg["username"], cfg["password"]) as adt_client:
                s4_metadata = [
                    adt_client.discover_metadata(source)
                    for source in lineage_sources
                ]
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Could not verify S/4HANA CDS metadata: {exc}") from exc
        if any(not item.get("fields") for item in s4_metadata):
            raise HTTPException(status_code=400, detail="One or more verified CDS sources returned no field metadata.")
        allowed_sources = lineage_sources
        allowed_fields = list(dict.fromkeys(
            str(field["name"])
            for item in s4_metadata
            for field in item.get("fields", [])
            if isinstance(field, dict) and field.get("name")
        ))
        expected_schema = None
        metadata_identity = [
            {
                "entity": item.get("entity"),
                "fields": [
                    {"name": field.get("name"), "type": field.get("type")}
                    for field in item.get("fields", [])
                    if isinstance(field, dict)
                ],
            }
            for item in s4_metadata
        ]
        source_cds_view = ", ".join(lineage_sources)

    try:
        validate_manual_select(
            sql,
            allowed_sources=allowed_sources,
            allowed_fields=allowed_fields,
            expected_schema=expected_schema,
            allow_source_aliases=target_name == "datasphere",
        )
        if target_name == "s4hana":
            _validate_reconciliation_sql(
                sql,
                expected_sources=allowed_sources,
                source_field_map={
                    str(item["entity"]): [
                        str(field["name"]) for field in item.get("fields", [])
                        if isinstance(field, dict) and field.get("name")
                    ]
                    for item in s4_metadata
                },
                allowed_source_fields=allowed_fields,
                enforce_entity_qualified_fields=True,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    plan_payload = {
        "mode": "manual",
        "model": view_name,
        "space": space_id,
        "target": target_name,
        "sql": sql,
        "target_metadata": metadata_identity,
        "parameters": request.cds_parameters or {},
    }
    query_hash = _hashlib.sha256(
        json.dumps(plan_payload, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    reconstruction = {
        "sql_mode": "manual",
        "manual_sql": sql,
        "manual_sql_target": target_name,
        "query_hash": query_hash,
        "semantic_equivalence_status": "NOT_ASSESSED",
    }
    pending = not request.human_approved or request.approved_query_hash != query_hash
    result_template = {
        "view": view_name,
        "sql": sql if target_name == "datasphere" else "",
        "total_kpi": None,
        "formatted_kpi": "Pending Approval" if pending else "Not Executed",
        "record_count": None,
        "total_rows": None,
        "truncated": False,
        "latency_seconds": 0,
        "execution_engine": "SAP HANA Cloud" if target_name == "datasphere" else "Not Selected",
        "query_status": "PENDING_APPROVAL" if pending and target_name == "datasphere" else "NOT_ATTEMPTED",
        "columns": [],
        "data": [],
        "error": None,
    }
    s4_template = {
        "cds_view": source_cds_view,
        "sql": sql if target_name == "s4hana" else "",
        "total_kpi": None,
        "formatted_kpi": "Pending Approval" if pending else "Not Executed",
        "record_count": None,
        "total_rows": None,
        "truncated": False,
        "latency_seconds": 0,
        "execution_engine": "Eclipse ADT Data Preview" if target_name == "s4hana" else "Not Selected",
        "query_status": "PENDING_APPROVAL" if pending and target_name == "s4hana" else "NOT_ATTEMPTED",
        "columns": [],
        "data": [],
        "error": None,
    }
    if pending:
        return ValidationResponse(
            status="NOT_RUN",
            view_name=view_name,
            space_id=space_id,
            source_cds_view=source_cds_view,
            target_metric="",
            target_metrics=[],
            reconciliation_dimension="Not compared",
            user_prompt=request.user_prompt,
            datasphere=result_template,
            s4hana=s4_template,
            comparison={
                "target_metric": "",
                "delta": None,
                "variance_pct": None,
                "is_match": None,
                "summary_text": "Manual SQL validated. Explicit approval is required before execution; cross-system comparison is indeterminate.",
                "security": "Read-only query, metadata checks, and exact-query approval required.",
            },
            architecture_metadata={
                "manual_sql_target": target_name,
                "verified_sources": allowed_sources,
                "verified_field_count": len(allowed_fields),
            },
            reconstruction=reconstruction,
            approval_required=True,
            execution_plan={
                "mode": "MANUAL_SQL",
                "execution_target": target_name,
                "query_hash": query_hash,
                "sql": sql,
                "approval_required": True,
            },
        )

    rows: list[dict[str, Any]] = []
    columns: list[dict[str, Any]] = []
    total_rows: int | None = None
    latency = 0.0
    error = None
    try:
        if target_name == "datasphere":
            rows, latency, executed_sql, columns = await asyncio.to_thread(
                execute_direct_hana_query,
                sql,
                space_id,
                "",
                allow_parameter_fallback=False,
                include_column_metadata=True,
            )
            if executed_sql != sql.strip():
                raise RuntimeError("The Datasphere executor did not preserve the approved SQL.")
            total_rows = len(rows)
        else:
            retriever = DataRetriever(default_row_limit=1000, max_row_limit=1000)
            try:
                s4_result = await asyncio.to_thread(
                    retriever.fetch_query,
                    sql,
                    row_limit=max(1, min(int(ADT_ROW_LIMIT or 100), 1000)),
                    csrf_entity=allowed_sources[0],
                )
            finally:
                retriever.close()
            rows = s4_result.get("rows", [])
            columns = s4_result.get("columns", [])
            total_rows = int(s4_result.get("total_rows", len(rows)))
            latency = float(s4_result.get("execution_time") or 0)
    except Exception as exc:
        error = str(exc)
        logger.exception("manual_sql_execution_failed target=%s query_sha256=%s", target_name, query_hash)

    truncated = total_rows is not None and total_rows > len(rows)
    selected_result = result_template if target_name == "datasphere" else s4_template
    selected_result.update({
        "formatted_kpi": "Execution Failed" if error else ("0 rows" if not rows else f"{len(rows)} rows"),
        "record_count": len(rows) if not error else None,
        "total_rows": total_rows,
        "truncated": truncated,
        "latency_seconds": latency,
        "query_status": "EXECUTION_FAILED" if error else (
            "SUCCEEDED_TRUNCATED" if truncated else "SUCCEEDED_ZERO_ROWS" if not rows else "SUCCEEDED_WITH_ROWS"
        ),
        "columns": columns,
        "data": rows,
        "error": error,
    })
    return ValidationResponse(
        status="ERROR" if error else "INDETERMINATE",
        view_name=view_name,
        space_id=space_id,
        source_cds_view=source_cds_view,
        target_metric="",
        target_metrics=[],
        reconciliation_dimension="Not compared",
        user_prompt=request.user_prompt,
        datasphere=result_template,
        s4hana=s4_template,
        comparison={
            "target_metric": "",
            "delta": None,
            "variance_pct": None,
            "is_match": None,
            "summary_text": (
                "Manual SQL execution failed; no comparison was performed."
                if error else
                "One target was queried using manual SQL. Cross-system comparability has not been established."
            ),
            "security": "Read-only query, verified metadata, and exact-query approval enforced.",
        },
        architecture_metadata={
            "manual_sql_target": target_name,
            "verified_sources": allowed_sources,
            "verified_field_count": len(allowed_fields),
        },
        reconstruction=reconstruction,
        approval_required=False,
        execution_plan=None,
    )


def _extract_sql_clause(sql: str, clause: str) -> str | None:
    stop = r"\b(?:GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT)\b" if clause.casefold() == "where" else r"\b(?:ORDER\s+BY|LIMIT)\b"
    match = re.search(
        rf"\b{re.escape(clause)}\b\s+(.*?)(?={stop}|$)",
        sql,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return None
    expression = re.sub(r'"?T\d+"?\s*\.\s*', "", match.group(1), flags=re.IGNORECASE).strip()
    return expression or None


def _select_output_names(sql: str) -> list[str]:
    match = re.match(r"(?is)^\s*SELECT\s+(.*?)\s+FROM\s+", sql)
    if not match:
        return []
    output_names: list[str] = []
    for expression in _split_sql_expressions(match.group(1)):
        alias = re.search(r"(?is)\s+AS\s+[`\"]?([A-Za-z][A-Za-z0-9_]*)[`\"]?\s*$", expression)
        if alias:
            output_names.append(alias.group(1))
            continue
        field = re.search(r"(?:[A-Za-z][A-Za-z0-9_]*~)?[`\"]?([A-Za-z][A-Za-z0-9_]*)[`\"]?\s*$", expression)
        if field:
            output_names.append(field.group(1))
    return output_names


async def run_metadata_driven_validation(request: ValidationRequest) -> ValidationResponse:
    """Resolve the live Datasphere architecture, reconstruct it, and compare both executions."""
    if (request.sql_mode or "llm").casefold() == "manual":
        return await run_manual_sql_validation(request)
    if (request.sql_mode or "llm").casefold() != "llm":
        raise HTTPException(status_code=400, detail="SQL mode must be llm or manual.")
    view_name = request.view_name.strip()
    space_id = request.space_id.strip() if request.space_id else DATASPHERE_SPACE_ID
    user_prompt = request.user_prompt.strip()
    key_fields = list(dict.fromkeys(
        field.strip()
        for field in (request.key_fields or [])
        if field and field.strip()
    ))
    if not key_fields and request.reconciliation_dimension:
        key_fields = [
            field.strip()
            for field in request.reconciliation_dimension.split(",")
            if field.strip()
        ]
    target_fields = list(dict.fromkeys(
        field.strip()
        for field in (request.selected_fields or request.selected_metrics or [])
        if field and field.strip()
    ))
    if not view_name or not space_id:
        raise HTTPException(status_code=400, detail="An analytical model and Datasphere space are required.")
    invalid_fields = [
        field for field in target_fields
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", field)
    ]
    if invalid_fields:
        raise HTTPException(
            status_code=400,
            detail="Invalid output field or key identifier: " + ", ".join(invalid_fields),
        )

    client = DatasphereResolverClient(space=space_id)
    requested_key_fields = list(key_fields)
    architecture: Dict[str, Any] = {}
    reconstruction: Dict[str, Any] = {}
    source_cds_view = (request.source_cds_view or "").strip()

    metadata_cache_entry = (
        client.cache.get_model_metadata(space_id, view_name)
        if client.cache is not None else None
    )
    if not isinstance(metadata_cache_entry, dict):
        metadata_cache_entry = None
    cached_metadata = metadata_cache_entry.get("metadata", {}) if metadata_cache_entry else {}
    cached_tree = cached_metadata.get("dependency_tree") if isinstance(cached_metadata, dict) else None
    cached_synced_at = metadata_cache_entry.get("last_synced", 0) if metadata_cache_entry else 0
    cache_is_fresh = bool(
        metadata_cache_entry
        and isinstance(cached_synced_at, (int, float))
        and time.time() - float(cached_synced_at) <= metadata_cache_ttl_seconds()
        and isinstance(cached_tree, dict)
    )

    inventory_refresh_error = None
    inventory_cache_hit = cache_is_fresh
    try:
        if cache_is_fresh and client.cache is not None:
            replicated_tables = client.cache.list_replication_local_tables()
        elif not client.cli_available:
            raise RuntimeError("SAP Datasphere CLI is not available.")
        else:
            replicated_tables = await asyncio.to_thread(client.refresh_replication_local_table_index)
    except Exception as exc:
        inventory_refresh_error = exc
        logger.warning("Configured replication-space refresh failed; trying the saved inventory: %s", exc)
        replicated_tables = (
            client.cache.list_replication_local_tables()
            if client.cache is not None
            else []
        )

    configured_rep_space = os.getenv("DATASPHERE_REPLICATION_SPACE", "").strip() or None
    try:
        if cache_is_fresh:
            dependency_tree = cached_tree
            client.cli_reads.add((
                str(dependency_tree.get("object_type") or "analytic_model"),
                view_name.casefold(),
            ))
        else:
            resolver = DatasphereDependencyResolver(
                client=client,
                space=space_id,
                silent=True,
                replication_space=configured_rep_space,
            )
            dependency_tree = await asyncio.to_thread(resolver.resolve, view_name)
            if client.cache is not None:
                client.cache.upsert_model_metadata(space_id, view_name, {
                    **(cached_metadata if isinstance(cached_metadata, dict) else {}),
                    "dependency_tree": dependency_tree,
                    "field_catalog": dependency_tree.get("field_catalog", []),
                    "calculated_columns": dependency_tree.get("calculated_columns", []),
                    "lineage_path": [
                        str(node.get("name")) for node in _walk_dependency_nodes(dependency_tree)
                        if node.get("name")
                    ],
                    "cache_source": "datasphere_cli_design_time",
                })
    except Exception as exc:
        logger.exception("Datasphere dependency resolution failed for %s", view_name)
        architecture = {
            "model_name": view_name, "space_id": space_id,
            "metadata_source": "SAP Datasphere CLI", "dependencies": [],
            "completeness": "incomplete",
        }
        return _reconciliation_incomplete_response(
            view_name=view_name, space_id=space_id, source_cds_view=source_cds_view,
            user_prompt=user_prompt, target_fields=target_fields, key_fields=key_fields,
            architecture_metadata=architecture, reconstruction=reconstruction,
            message=f"Could not resolve the live Datasphere dependency graph: {exc}",
        )

    nodes = list(_walk_dependency_nodes(dependency_tree))
    transformation_ir = _build_transformation_ir(dependency_tree)
    transformation_ir_digest = _hashlib.sha256(
        json.dumps(transformation_ir, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    gaps = [
        f"{node.get('object_type')} {node.get('name')}: {node.get('status') or 'missing status'}"
        for node in nodes
        if node.get("status") not in ("resolved", "boundary") or not node.get("definition")
    ]
    root_type = str(dependency_tree.get("object_type") or "unknown")
    if root_type not in {"analytic_model", "view"}:
        gaps.append(
            f"The selected artifact type ({root_type}) is not a readable analytical model or view."
        )
    elif (root_type, view_name.casefold()) not in client.cli_reads:
        gaps.append(
            f"The selected {root_type.replace('_', ' ')} definition was not read from the Datasphere CLI."
        )

    # Generic Terminal Source Boundary Resolution & ADT Verification
    terminal_nodes = [
        node for node in nodes
        if (node.get("is_boundary")
            or node.get("object_type") in ("local_table", "remote_table", "shared_remote_table")
            or not node.get("dependencies"))
        and node.get("name") != view_name
    ]

    replicated_source_matches: List[Dict[str, Any]] = []
    adt_discovered_metadata: Dict[str, Dict[str, Any]] = {}
    cfg = get_config() if USE_SAP_ADT else {}
    adt_available = bool(USE_SAP_ADT and cfg.get("base_url") and cfg.get("username") and cfg.get("password"))
    verified_inventory_sources: Dict[str, Dict[str, Any]] = {}
    try:
        inventory_matches = client.match_dependency_sources(dependency_tree, replicated_tables)
        if isinstance(inventory_matches, list):
            verified_inventory_sources = {
                str(item.get("name") or "").casefold(): item
                for item in inventory_matches
                if isinstance(item, dict)
                and item.get("replicated_from_adt") is True
                and item.get("matched_cds_view")
            }
    except Exception as exc:
        logger.info("No verified replicated-source inventory mapping for %s: %s", view_name, type(exc).__name__)

    for node in terminal_nodes:
        n_name = str(node.get("name") or "")
        n_type = str(node.get("object_type") or "local_table")
        n_space = str(node.get("space") or space_id)
        is_shared = bool(node.get("is_shared") or (n_space.casefold() != space_id.casefold()))
        is_replicated = bool(
            configured_rep_space and n_space.casefold() == configured_rep_space.casefold()
        )

        candidate_name = None
        node_def = node.get("definition") if isinstance(node.get("definition"), dict) else {}
        lineage_cds = (
            node_def.get("sourceLineage", {}).get("cdsEntity")
            or node_def.get("remoteObject", {}).get("name")
            or node.get("source_entity")
        )
        identity_evidence = ""
        if lineage_cds:
            candidate_name = str(lineage_cds)
            identity_evidence = "explicit Datasphere sourceLineage/remoteObject metadata"
        elif source_cds_view and len(terminal_nodes) == 1:
            candidate_name = source_cds_view
            identity_evidence = "explicit user-selected CDS source override"
        elif n_name.casefold() in verified_inventory_sources:
            inventory_match = verified_inventory_sources[n_name.casefold()]
            candidate_name = str(inventory_match["matched_cds_view"])
            identity_evidence = "exact verified ADT replication inventory mapping"
        else:
            candidate_name = None
            identity_evidence = (
                "No explicit sourceLineage/remoteObject mapping, user override, or exact verified ADT replication mapping was available."
            )

        cds_status = "unavailable"
        cds_meta_entry = None
        evidence = ""

        if adt_available and candidate_name:
            try:
                with CDSMetadataClient(cfg["base_url"], cfg["username"], cfg["password"]) as adt_client:
                    adt_meta = adt_client.discover_metadata(candidate_name)
                    if adt_meta and adt_meta.get("fields"):
                        ds_fields = {
                            f["name"].casefold()
                            for f in node.get("field_catalog", [])
                            if isinstance(f, dict) and f.get("name")
                        }
                        adt_fields = {
                            f["name"].casefold()
                            for f in adt_meta.get("fields", [])
                            if isinstance(f, dict) and f.get("name")
                        }
                        overlap = ds_fields & adt_fields
                        if overlap or not ds_fields:
                            cds_status = "verified"
                            cds_meta_entry = adt_meta
                            adt_discovered_metadata[candidate_name] = adt_meta
                            evidence = (
                                f"Verified via SAP ADT DDL discovery. {len(overlap)} matching fields "
                                f"confirmed between Datasphere {n_type} and S/4HANA CDS entity {candidate_name}."
                            )
                        else:
                            cds_status = "ambiguous"
                            evidence = (
                                f"Candidate CDS entity '{candidate_name}' found in ADT, but zero fields "
                                f"matched Datasphere {n_type} catalog ({len(ds_fields)} vs {len(adt_fields)} fields)."
                            )
                    else:
                        cds_status = "unavailable"
                        evidence = f"Candidate CDS entity '{candidate_name}' returned no fields from ADT."
            except Exception as exc:
                cds_status = "unavailable"
                evidence = f"SAP ADT metadata query for candidate '{candidate_name}' failed: {exc}"
        elif not candidate_name:
            cds_status = "unavailable"
            evidence = identity_evidence
        else:
            cds_status = "unavailable"
            evidence = "SAP ADT is not configured; source verification is unavailable."

        node["candidate_cds_view"] = candidate_name
        node["cds_identity_evidence"] = identity_evidence
        node["cds_verification_status"] = cds_status
        node["cds_metadata"] = cds_meta_entry
        node["cds_mapping_evidence"] = evidence

        if cds_status == "verified":
            source_entry = {
                "name": n_name,
                "type": n_type,
                "space": n_space,
                "lineage_path": node.get("lineage_path", n_name),
                "matched_cds_view": candidate_name,
                "cds_view_name": candidate_name,
                "replicated_from_adt": is_replicated or (n_type == "local_table"),
                "replication_space": n_space,
                "is_shared": is_shared,
                "is_boundary": True,
                "source_origin_status": "verified",
                "cds_verification_status": "verified",
                "description": f"Verified S/4HANA source for {n_type} {n_name} in space {n_space}",
                "key_fields": cds_meta_entry.get("key_fields", []) if cds_meta_entry else [],
                "field_catalog": node.get("field_catalog", []),
                "evidence": evidence,
                "identity_evidence": identity_evidence,
            }
            replicated_source_matches.append(source_entry)
        else:
            gaps.append(
                f"Terminal source boundary '{n_name}' ({n_type} in space '{n_space}') could not be verified: {evidence}"
            )

    discovered_sources = list(dict.fromkeys(
        str(source["cds_view_name"])
        for source in replicated_source_matches
        if source.get("cds_view_name")
    ))

    # Derive output grain and key fields (Requirement 5)
    if not key_fields:
        key_fields = _derive_model_keys(dependency_tree, replicated_source_matches)
        if not key_fields:
            gaps.append(
                "No output grain or unique row keys could be derived from the selected model's dimensions or keys. "
                "Select every field that uniquely identifies one output row."
            )

    if source_cds_view:
        if source_cds_view.casefold() not in {name.casefold() for name in discovered_sources}:
            gaps.append(
                f"Selected CDS source '{source_cds_view}' could not be verified against any "
                "terminal source boundary in this model's dependencies."
            )
    else:
        source_cds_view = ", ".join(discovered_sources)

    if not discovered_sources:
        gaps.append(
            "No terminal source boundaries were verified against S/4HANA CDS via ADT."
        )

    architecture = {
        "model_name": view_name,
        "space_id": space_id,
        "metadata_source": "SAP Datasphere CLI",
        "artifact_types": ["analytic-models", "views", "remote-tables", "local-tables"],
        "transformation_ir": transformation_ir,
        "transformation_ir_digest": transformation_ir_digest,
        "dependency_tree": dependency_tree,
        "resolved_artifacts": [
            {
                "name": node.get("name"),
                "type": node.get("object_type"),
                "space": node.get("space"),
                "is_shared": node.get("is_shared"),
                "is_boundary": node.get("is_boundary"),
                "status": node.get("status"),
                "candidate_cds_view": node.get("candidate_cds_view"),
                "cds_verification_status": node.get("cds_verification_status"),
            }
            for node in nodes
        ],
        "replication_space": configured_rep_space,
        "replicated_local_table_count": len(replicated_tables),
        "inventory_status": (
            "cached" if inventory_cache_hit
            else "stale" if inventory_refresh_error and replicated_tables
            else "unavailable" if inventory_refresh_error
            else "refreshed"
        ),
        "replicated_source_matches": replicated_source_matches,
        "s4hana_sources": discovered_sources,
        "completeness": "complete" if not gaps else "incomplete",
        "gaps": gaps,
    }
    if not target_fields:
        root_catalog = dependency_tree.get("field_catalog", [])
        if root_catalog:
            target_fields = [
                str(field["name"])
                for field in root_catalog
                if isinstance(field, dict) and field.get("name")
            ]
        else:
            target_fields = list(dict.fromkeys(
                str(field.get("name"))
                for node in nodes
                for field in node.get("field_catalog", [])
                if isinstance(field, dict) and field.get("name")
            ))
    execution_target = None
    execution_target_error = None
    try:
        execution_target = resolve_execution_target(
            dependency_tree,
            selected_fields=target_fields,
            # Grain selection is performed from verified target metadata below;
            # request keys are candidates, not universal fact-model columns.
            required_keys=[],
            # Datasphere space IDs are not assumed to be HANA schemas. The
            # catalog resolver returns the schema that HANA actually reports.
            runtime_discovery=lambda *, logical_name, space: discover_hana_execution_object_sync(
                logical_name=logical_name,
            ),
        )
        architecture["execution_target"] = execution_target.as_dict()
        logger.info(
            "reconciliation_execution_target_resolved model=%s object=%s schema=%s",
            view_name,
            execution_target.object_name,
            execution_target.schema,
        )
        architecture["reconciliation_diagnostics"] = _grain_metadata_diagnostics(
            view_name=view_name,
            space_id=space_id,
            execution_target=execution_target,
            requested_keys=requested_key_fields,
        )
        grain = resolve_reconciliation_grain(
            execution_target,
            requested_keys=requested_key_fields,
            analytical_fields=dependency_tree.get("field_catalog", []),
        )
        key_fields = list(grain["keys"])
        architecture["reconciliation_grain"] = grain
    except ExecutionTargetError as exc:
        execution_target_error = str(exc)
        architecture["execution_target"] = {"status": "unresolved", "error": str(exc)}
        logger.warning(
            "reconciliation_execution_target_unresolved model=%s error_type=%s",
            view_name,
            type(exc).__name__,
        )
        gaps.append(f"Datasphere execution target: {exc}")
    except ReconciliationGrainError as exc:
        architecture["reconciliation_grain"] = {"status": "unresolved", "error": str(exc)}
        gaps.append(f"Reconciliation grain: {exc}")
    if execution_target_error:
        return _reconciliation_incomplete_response(
            view_name=view_name, space_id=space_id, source_cds_view=source_cds_view,
            user_prompt=user_prompt, target_fields=target_fields, key_fields=key_fields,
            architecture_metadata=architecture, reconstruction=reconstruction,
            message=f"Execution blocked: {execution_target_error}",
            status="INDETERMINATE", approval_required=False, execution_plan=None,
        )
    if request.prompt_only or request.sql_mode.strip().casefold() == "llm":
        # Natural-language prompts are compiled against the verified fact view.
        # In LLM mode, do not let the metadata-driven reconciliation plan replace
        # the user's requested filters and aggregation with selected-field defaults.
        return await _fact_query_only_response(
            request=request, view_name=view_name, space_id=space_id,
            source_cds_view=source_cds_view, user_prompt=user_prompt,
            target_fields=[], key_fields=[],
            architecture_metadata=architecture, reconstruction=reconstruction,
            target=execution_target,
            reason="Prompt-only Datasphere query; S/4HANA reconciliation is not part of this run.",
            source_metadata=[], discovered_sources=[],
        )
    if not target_fields:
        return _reconciliation_incomplete_response(
            view_name=view_name, space_id=space_id, source_cds_view=source_cds_view,
            user_prompt=user_prompt, target_fields=target_fields, key_fields=key_fields,
            architecture_metadata=architecture, reconstruction=reconstruction,
            message="No verified output fields were selected for the Datasphere fact query.",
        )

    if not source_cds_view or gaps:
        reason = "Architecture metadata is incomplete for reconciliation: " + "; ".join(gaps or ["no verified S/4HANA source was resolved"])
        return await _fact_query_only_response(
            request=request, view_name=view_name, space_id=space_id,
            source_cds_view=source_cds_view, user_prompt=user_prompt,
            target_fields=target_fields, key_fields=key_fields,
            architecture_metadata=architecture, reconstruction=reconstruction,
            target=execution_target, reason=reason,
            source_metadata=[], discovered_sources=discovered_sources,
        )

    if not USE_SAP_ADT or DataRetriever is None:
        architecture["s4hana_metadata_status"] = "NOT_CONFIGURED"
        return await _fact_query_only_response(
            request=request, view_name=view_name, space_id=space_id,
            source_cds_view=source_cds_view, user_prompt=user_prompt,
            target_fields=target_fields, key_fields=key_fields,
            architecture_metadata=architecture, reconstruction=reconstruction,
            target=execution_target,
            reason="SAP ADT credentials/client are not configured; only the Datasphere fact query can run.",
            source_metadata=[], discovered_sources=discovered_sources,
        )

    architecture["s4hana_metadata_status"] = "DISCOVERY_IN_PROGRESS"
    try:
        source_metadata = [
            adt_discovered_metadata.get(source_name)
            for source_name in discovered_sources or [source_cds_view]
            if adt_discovered_metadata.get(source_name)
        ]
        if len(source_metadata) < len(discovered_sources or [source_cds_view]):
            cfg = get_config()
            with CDSMetadataClient(cfg["base_url"], cfg["username"], cfg["password"]) as adt_client:
                source_metadata = [
                    adt_discovered_metadata.get(source_name) or adt_client.discover_metadata(source_name)
                    for source_name in discovered_sources or [source_cds_view]
                ]
    except Exception as exc:
        architecture["s4hana_metadata_status"] = "DISCOVERY_FAILED"
        logger.exception("S/4HANA CDS metadata discovery failed")
        return await _fact_query_only_response(
            request=request, view_name=view_name, space_id=space_id,
            source_cds_view=source_cds_view, user_prompt=user_prompt,
            target_fields=target_fields, key_fields=key_fields,
            architecture_metadata=architecture, reconstruction=reconstruction,
            target=execution_target,
            reason=f"Could not read the S/4HANA CDS source metadata: {exc}",
            source_metadata=[], discovered_sources=discovered_sources,
        )
    if any(not source.get("fields") for source in source_metadata):
        architecture["s4hana_metadata_status"] = "DISCOVERY_INCOMPLETE"
        architecture["s4hana_metadata_status"] = "partial_or_empty"
        architecture["s4hana_metadata_diagnostics"] = [
            {"entity": source.get("entity"), "field_count": len(source.get("fields") or [])}
            for source in source_metadata
        ]
        return await _fact_query_only_response(
            request=request, view_name=view_name, space_id=space_id,
            source_cds_view=source_cds_view, user_prompt=user_prompt,
            target_fields=target_fields, key_fields=key_fields,
            architecture_metadata=architecture, reconstruction=reconstruction,
            target=execution_target,
            reason="One or more S/4HANA sources returned no field metadata.",
            source_metadata=source_metadata, discovered_sources=discovered_sources,
        )

    architecture["s4hana_metadata_status"] = "VERIFIED"

    architecture["verified_adt_sources"] = [
        {
            "entity": source.get("entity"),
            "sql_view_name": source.get("sql_view_name"),
            "fields": source.get("fields", []),
            "key_fields": source.get("key_fields", []),
            "parameters": source.get("parameters", []),
            "parameterized": bool(source.get("parameterized")),
            "annotations": source.get("annotations", {}),
            "lineage": source.get("lineage", {}),
            "source_ddl": source.get("source_ddl", ""),
        }
        for source in source_metadata
    ]
    if client.cache is not None:
        try:
            current_cache_entry = client.cache.get_model_metadata(space_id, view_name)
            current_cache_metadata = (
                current_cache_entry.get("metadata", {})
                if isinstance(current_cache_entry, dict)
                else {}
            )
            client.cache.upsert_model_metadata(space_id, view_name, {
                **(current_cache_metadata if isinstance(current_cache_metadata, dict) else {}),
                "verified_adt_sources": architecture["verified_adt_sources"],
                "adt_metadata_synced_at": time.time(),
            })
        except Exception as exc:
            logger.warning("Could not persist verified ADT metadata for %s/%s: %s", space_id, view_name, exc)

    # Reconcile the provisional fact-model grain against the verified CDS
    # metadata before generating or approving either SQL statement.
    source_fields_for_grain = [
        field
        for source in source_metadata
        for field in source.get("fields", [])
        if isinstance(field, dict) and field.get("name")
    ]
    source_key_fields_for_grain = [
        key
        for source in source_metadata
        for key in (source.get("key_fields") or [])
        if str(key).strip()
    ]
    architecture["reconciliation_diagnostics"] = _grain_metadata_diagnostics(
        view_name=view_name,
        space_id=space_id,
        execution_target=execution_target,
        source_metadata=source_metadata,
        requested_keys=requested_key_fields,
    )
    try:
        grain = resolve_reconciliation_grain(
            execution_target,
            requested_keys=requested_key_fields,
            analytical_fields=dependency_tree.get("field_catalog", []),
            source_fields=source_fields_for_grain,
            source_key_fields=source_key_fields_for_grain,
        )
        key_fields = list(grain["keys"])
        architecture["reconciliation_grain"] = grain
        architecture["reconciliation_diagnostics"]["final_grain"] = {
            "strategy": grain.get("strategy"),
            "keys": grain.get("keys", []),
            "source_keys": grain.get("source_keys", []),
            "field_mappings": grain.get("field_mappings", []),
        }
        logger.info(
            "reconciliation_grain_resolved model=%s strategy=%s keys=%s mappings=%s",
            view_name, grain.get("strategy"), grain.get("keys"), grain.get("field_mappings"),
        )
    except ReconciliationGrainError as exc:
        architecture["reconciliation_grain"] = {"status": "unresolved", "error": str(exc)}
        architecture["reconciliation_diagnostics"]["failure_reason"] = str(exc)
        logger.warning(
            "reconciliation_grain_unresolved model=%s reason=%s",
            view_name, str(exc),
        )
        return await _fact_query_only_response(
            request=request,
            view_name=view_name, space_id=space_id, source_cds_view=source_cds_view,
            user_prompt=user_prompt, target_fields=target_fields, key_fields=key_fields,
            architecture_metadata=architecture, reconstruction=reconstruction,
            target=execution_target, reason=str(exc),
            source_metadata=source_metadata,
            discovered_sources=discovered_sources,
        )

    effective_prompt = user_prompt
    if request.custom_filters:
        effective_prompt = f"{effective_prompt}\nUser-specified reconciliation filter: {request.custom_filters}".strip()
    if request.formula_prompt:
        effective_prompt = f"{effective_prompt}\nUser-specified calculated-field guidance: {request.formula_prompt}".strip()
    if request.calculated_field_formulas:
        effective_prompt = (
            f"{effective_prompt}\nUser-specified calculated-field formulas: "
            f"{request.calculated_field_formulas}"
        ).strip()
    try:
        plan = await generate_reconciliation_query_blueprint(
            user_prompt=effective_prompt,
            # Prompt SQL must target the verified underlying fact runtime object,
            # not the analytical model that only supplies its metadata.
            model_name=execution_target.object_name,
            space_id=space_id,
            architecture_metadata=architecture,
            source_metadata=source_metadata,
            selected_fields=target_fields,
            key_fields=key_fields,
            parameter_overrides=request.cds_parameters,
        )
    except Exception as exc:
        logger.exception("Reconciliation query planning failed")
        return _reconciliation_incomplete_response(
            view_name=view_name, space_id=space_id, source_cds_view=source_cds_view,
            user_prompt=user_prompt, target_fields=target_fields, key_fields=key_fields,
            architecture_metadata=architecture, reconstruction=reconstruction,
            message=f"Could not generate a metadata-grounded reconstruction plan: {exc}",
        )

    reconstruction = {
        "status": "PLANNED",
        "datasphere_sql": str(plan.get("datasphere_sql", "")).strip(),
        "s4hana_sql": str(plan.get("s4hana_sql", "")).strip(),
        "output_fields": plan.get("output_fields", []),
        "key_fields": plan.get("key_fields", []),
        "logic_summary": plan.get("logic_summary", ""),
        "filters": plan.get("filters", []),
        "assumptions": plan.get("assumptions", []),
        "generation_diagnostics": plan.get("generation_diagnostics", []),
        "llm_provider": plan.get("llm_provider"),
        "llm_model": plan.get("llm_model"),
        "unsupported_transformations": plan.get("unsupported_transformations", []),
    }
    provider_failures = [
        {
            "provider": failure.get("provider"),
            "status_code": failure.get("status_code"),
        }
        for failure in reconstruction["generation_diagnostics"]
        if isinstance(failure, dict)
    ]
    logger.info(
        "reconstruction_plan_generated model=%s generator=%s provider_failures=%s",
        view_name,
        reconstruction["llm_provider"] or "deterministic_fallback",
        provider_failures,
    )
    try:
        if reconstruction["unsupported_transformations"]:
            raise ValueError(
                "The LLM identified transformations it cannot translate: "
                + "; ".join(map(str, reconstruction["unsupported_transformations"]))
            )
        if execution_target is None:
            raise ValueError("A verified Datasphere execution target is required before SQL generation.")
        planned_datasphere_sql = str(plan.get("datasphere_sql") or "").strip()
        _validate_reconciliation_sql(
            planned_datasphere_sql,
            expected_sources=[],
            datasphere_model=execution_target.object_name,
        )
        user_where_clause = _extract_sql_clause(planned_datasphere_sql, "WHERE")
        user_having_clause = _extract_sql_clause(planned_datasphere_sql, "HAVING")
        datasphere_field_names = {
            str(column.get("name") or "").casefold()
            for column in execution_target.columns
            if isinstance(column, dict) and column.get("name")
        }
        quoted_filter_fields = re.findall(
            r'"([^\"]+)"',
            " ".join(item for item in (user_where_clause, user_having_clause) if item),
        )
        unknown_filter_fields = [
            field for field in quoted_filter_fields
            if field.casefold() not in datasphere_field_names
        ]
        if unknown_filter_fields:
            raise ValueError(
                "Datasphere filters must use technical field names from the final model projection: "
                + ", ".join(dict.fromkeys(unknown_filter_fields))
            )
        if (user_where_clause or user_having_clause) and not re.search(
            r"\b(?:WHERE|HAVING)\b", str(plan.get("s4hana_sql") or ""), re.IGNORECASE
        ):
            raise ValueError("The generated CDS query omitted the filter used by the Datasphere query.")
        # The LLM blueprint remains descriptive for the plan, but never supplies
        # the Datasphere object identity or source columns.
        reconstruction["datasphere_sql"] = compile_reconciliation_sql(
            execution_target,
            selected_fields=target_fields,
            required_keys=key_fields,
            where_clause=user_where_clause,
            having_clause=user_having_clause,
        )
        reconstruction["s4hana_sql"], resolved_cds_parameters = _bind_declared_cds_parameters(
            _format_open_sql_lines(reconstruction["s4hana_sql"]),
            source_metadata,
            request.cds_parameters,
        )
        reconstruction["s4hana_sql"] = _format_open_sql_lines(reconstruction["s4hana_sql"])
        reconstruction["cds_parameters"] = resolved_cds_parameters
        s4_sql_hash = _hashlib.sha256(
            reconstruction["s4hana_sql"].encode("utf-8")
        ).hexdigest()
        logger.info(
            "reconstruction_query_pre_validation model=%s generator=%s source_entities=%s sql_sha256=%s sql_chars=%s",
            view_name,
            reconstruction["llm_provider"] or "deterministic_fallback",
            discovered_sources or [source_cds_view],
            s4_sql_hash,
            len(reconstruction["s4hana_sql"]),
        )
        _validate_reconciliation_sql(
            reconstruction["datasphere_sql"],
            expected_sources=[],
            datasphere_model=execution_target.object_name,
        )
        _validate_reconciliation_sql(
            reconstruction["s4hana_sql"],
            expected_sources=discovered_sources or [source_cds_view],
            allowed_source_fields=[
                str(field.get("name"))
                for source in source_metadata
                for field in source.get("fields", [])
                if isinstance(field, dict) and field.get("name")
            ],
            source_field_map={
                str(source.get("entity")): [
                    str(field.get("name"))
                    for field in source.get("fields", [])
                    if isinstance(field, dict) and field.get("name")
                ]
                for source in source_metadata
                if source.get("entity")
            },
            enforce_abap_line_limit=True,
            enforce_entity_qualified_fields=True,
        )
        expected_output_names = {name.casefold() for name in target_fields + key_fields}
        planned_output_names = {
            str(name).casefold() for name in reconstruction["output_fields"]
        }
        planned_keys = {
            str(name).casefold() for name in reconstruction["key_fields"]
        }
        if planned_output_names != expected_output_names:
            raise ValueError("The generated query plan must project exactly the final Datasphere fields and reconciliation keys.")
        if not {name.casefold() for name in key_fields}.issubset(planned_keys):
            raise ValueError("The generated plan omitted one or more required composite keys.")
        cds_projected_names = {name.casefold() for name in _select_output_names(reconstruction["s4hana_sql"])}
        if cds_projected_names != expected_output_names:
            raise ValueError("The CDS query must return exactly the final Datasphere projected fields and reconciliation keys.")
        if reconstruction["assumptions"]:
            raise ValueError("The LLM identified unresolved assumptions; review metadata before execution.")
    except Exception as exc:
        logger.warning(
            "reconstruction_query_validation_failed model=%s error_type=%s",
            view_name,
            type(exc).__name__,
        )
        return _reconciliation_incomplete_response(
            view_name=view_name, space_id=space_id, source_cds_view=source_cds_view,
            user_prompt=user_prompt, target_fields=target_fields, key_fields=key_fields,
            architecture_metadata=architecture, reconstruction=reconstruction,
            message=f"Generated query plan failed validation and was not executed: {exc}",
        )

    transformation_coverage = _verify_transformation_coverage(
        transformation_ir,
        s4hana_sql=reconstruction["s4hana_sql"],
        selected_fields=target_fields,
        target_columns=list(execution_target.columns),
        source_fields=source_fields_for_grain,
        cds_parameters=reconstruction.get("cds_parameters", {}),
    )
    architecture["transformation_coverage"] = transformation_coverage
    reconstruction["transformation_coverage"] = transformation_coverage
    reconstruction["transformation_ir_digest"] = transformation_ir_digest
    coverage_blockers = [
        {
            "transformation_id": item.get("transformation_id"),
            "transformation": item.get("transformation"),
            "state": item.get("state"),
        }
        for item in transformation_coverage.get("items", [])
        if item.get("status") != "VERIFIED"
    ]
    logger.info(
        "reconstruction_transformation_coverage model=%s status=%s blockers=%s",
        view_name,
        transformation_coverage.get("overall_status"),
        coverage_blockers,
    )
    if transformation_coverage["overall_status"] != "VERIFIED":
        reconstruction["semantic_equivalence_status"] = "SEMANTIC_EQUIVALENCE_UNVERIFIED"
        reconstruction["equivalence_candidate_s4hana_sql"] = reconstruction["s4hana_sql"]
        return await _fact_query_only_response(
            request=request,
            view_name=view_name,
            space_id=space_id,
            source_cds_view=source_cds_view,
            user_prompt=user_prompt,
            target_fields=target_fields,
            key_fields=key_fields,
            architecture_metadata=architecture,
            reconstruction=reconstruction,
            target=execution_target,
            reason=(
                "SEMANTIC_EQUIVALENCE_UNVERIFIED: the generated ADT SQL does not prove coverage "
                "of every discovered Datasphere transformation."
            ),
            source_metadata=source_metadata,
            discovered_sources=discovered_sources,
        )
    reconstruction["semantic_equivalence_status"] = "VERIFIED"

    # Human approval gate: queries are previewed and require approval before execution
   
    plan_payload = {
        "model": view_name,
        "space": space_id,
        "execution_target": architecture.get("execution_target"),
        "transformation_ir_digest": transformation_ir_digest,
        "transformation_coverage": transformation_coverage,
        "datasphere_sql": reconstruction["datasphere_sql"],
        "s4hana_sql": reconstruction["s4hana_sql"],
        "source_metadata": source_metadata,
        "cds_parameters": reconstruction.get("cds_parameters", {}),
        "field_mappings": architecture.get("reconciliation_grain", {}).get("field_mappings", []),
        "grain": architecture.get("reconciliation_grain", {}),
        "keys": key_fields,
    }
    query_hash = _hashlib.sha256(
        json.dumps(plan_payload, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    reconstruction["query_hash"] = query_hash

    if not request.human_approved or request.approved_query_hash != query_hash:
        logger.info(
            "reconciliation_approval_required model=%s query_hash=%s",
            view_name,
            query_hash,
        )
        execution_plan = {
            "model_name": view_name,
            "space_id": space_id,
            "query_hash": query_hash,
            "approval_required": True,
            "datasphere_sql": reconstruction["datasphere_sql"],
            "s4hana_sql": reconstruction["s4hana_sql"],
            "cds_parameters": reconstruction.get("cds_parameters", {}),
            "field_mappings": architecture.get("reconciliation_grain", {}).get("field_mappings", []),
            "reconciliation_grain": architecture.get("reconciliation_grain", {}),
            "execution_target": architecture.get("execution_target", {}),
            "output_fields": reconstruction["output_fields"],
            "key_fields": key_fields,
            "logic_summary": reconstruction["logic_summary"],
            "source_cds_views": discovered_sources or [source_cds_view],
            "transformation_coverage": transformation_coverage,
            "execution_readiness": "READY_FOR_APPROVAL",
        }
        return ValidationResponse(
            status="NOT_RUN",
            view_name=view_name,
            space_id=space_id,
            source_cds_view=", ".join(discovered_sources or [source_cds_view]),
            target_metric=target_fields[0] if target_fields else "",
            target_metrics=target_fields,
            reconciliation_dimension=f"Keys: {', '.join(key_fields)}",
            user_prompt=user_prompt,
            datasphere={"view": view_name, "sql": reconstruction["datasphere_sql"], "total_kpi": None, "formatted_kpi": "Pending Approval", "record_count": None, "latency_seconds": 0, "execution_engine": "Pending Approval", "query_status": "PENDING_APPROVAL", "data": [], "error": None},
            s4hana={"cds_view": ", ".join(discovered_sources or [source_cds_view]), "sql": reconstruction["s4hana_sql"], "total_kpi": None, "formatted_kpi": "Pending Approval", "record_count": None, "latency_seconds": 0, "execution_engine": "Pending Approval", "query_status": "PENDING_APPROVAL", "columns": [], "data": [], "error": None},
            comparison={"target_metric": target_fields[0] if target_fields else "", "delta": None, "variance_pct": None, "is_match": False, "summary_text": "Reconciliation query plan generated. Explicit human approval required before execution.", "security": "Zero row data sent to LLM; approval required."},
            metrics_comparison=[],
            lineage_chain=[],
            grain_breakdown=[],
            architecture_metadata=architecture,
            reconstruction=reconstruction,
            approval_required=True,
            execution_plan=execution_plan,
        )

    logger.info(
        "reconciliation_approval_verified model=%s query_hash=%s",
        view_name,
        query_hash,
    )
    if not USE_DIRECT_HANA:
        return _reconciliation_incomplete_response(
            view_name=view_name, space_id=space_id, source_cds_view=source_cds_view,
            user_prompt=user_prompt, target_fields=target_fields, key_fields=key_fields,
            architecture_metadata=architecture, reconstruction=reconstruction,
            message="Direct HANA access is required to execute the same generated filters and grain on Datasphere; OData fallback is not considered equivalent.",
        )

    datasphere_rows: List[Dict[str, Any]] = []
    datasphere_latency = 0.0
    datasphere_error = None
    datasphere_status = "NOT_ATTEMPTED"
    datasphere_columns: List[Dict[str, Any]] = []
    s4_rows: List[Dict[str, Any]] = []
    s4_latency = 0.0
    s4_error = None
    s4_status = "NOT_ATTEMPTED"
    s4_columns: List[Dict[str, Any]] = []
    s4_total_rows: Optional[int] = None
    adt_row_limit = max(1, min(int(ADT_ROW_LIMIT or 100), 1000))

    try:
        datasphere_rows, datasphere_latency, executed_ds_sql = await asyncio.to_thread(
            execute_direct_hana_query,
            reconstruction["datasphere_sql"],
            space_id,
            effective_prompt,
        )
        reconstruction["datasphere_sql"] = executed_ds_sql
        target_column_metadata = {
            str(column.get("name")).casefold(): column
            for column in execution_target.columns
            if column.get("name")
        }
        datasphere_columns = []
        if datasphere_rows:
            for name in datasphere_rows[0]:
                metadata = target_column_metadata.get(str(name).casefold(), {})
                datasphere_columns.append({
                    "name": name,
                    "type": metadata.get("data_type") or metadata.get("type")
                    or type(datasphere_rows[0].get(name)).__name__,
                })
        if not datasphere_columns:
            requested_names = {name.casefold() for name in target_fields + key_fields}
            datasphere_columns = [
                {"name": str(column.get("name")), "type": column.get("data_type")}
                for column in execution_target.columns
                if column.get("name")
                and str(column.get("name")).casefold() in requested_names
            ]
        datasphere_status = "SUCCEEDED_ZERO_ROWS" if not datasphere_rows else "SUCCEEDED_WITH_ROWS"
    except Exception as exc:
        datasphere_error = str(exc)
        datasphere_status = "EXECUTION_FAILED"
        logger.exception("Datasphere reconciliation query failed")

    try:
        if DataRetriever is None:
            raise RuntimeError("ADT query execution is unavailable because DataRetriever could not be imported.")
        retriever = DataRetriever(default_row_limit=1000, max_row_limit=1000)
        try:
            s4_result = await asyncio.to_thread(
                retriever.fetch_query,
                reconstruction["s4hana_sql"],
                row_limit=adt_row_limit,
                csrf_entity=(discovered_sources or [source_cds_view])[0],
            )
        finally:
            retriever.close()
        s4_rows = s4_result.get("rows", [])
        s4_latency = float(s4_result.get("execution_time") or 0)
        s4_columns = s4_result.get("columns", [])
        s4_total_rows = int(s4_result.get("total_rows", len(s4_rows)))
        if s4_total_rows > len(s4_rows):
            s4_status = "SUCCEEDED_TRUNCATED"
            s4_error = f"ADT returned {len(s4_rows)} of {s4_total_rows} rows at row limit {adt_row_limit}; comparison is blocked as partial."
        elif not s4_rows:
            s4_status = "SUCCEEDED_ZERO_ROWS"
        else:
            s4_status = "SUCCEEDED_WITH_ROWS"
    except Exception as exc:
        s4_error = str(exc)
        s4_status = "EXECUTION_FAILED"
        logger.warning(
            "reconciliation_execution_result system=s4hana model=%s query_sha256=%s status=%s error_type=%s",
            view_name,
            _hashlib.sha256(reconstruction["s4hana_sql"].encode("utf-8")).hexdigest(),
            s4_status,
            type(exc).__name__,
        )
        logger.exception("S/4HANA ADT reconciliation query failed")

    reconstruction["datasphere_query_status"] = datasphere_status
    reconstruction["s4hana_query_status"] = s4_status
    if (
        datasphere_error
        or s4_error
        or datasphere_status == "SUCCEEDED_ZERO_ROWS"
        or s4_status in {"SUCCEEDED_ZERO_ROWS", "SUCCEEDED_TRUNCATED"}
    ):
        reason_text = (
            f"Datasphere={datasphere_status}; S/4HANA ADT={s4_status}. "
            "No reconciliation result is reported because one or both datasets failed, were empty, or were incomplete."
        )
        response = _reconciliation_incomplete_response(
            view_name=view_name, space_id=space_id, source_cds_view=source_cds_view,
            user_prompt=user_prompt, target_fields=target_fields, key_fields=key_fields,
            architecture_metadata=architecture, reconstruction=reconstruction,
            message=reason_text,
        )
        response.datasphere.update({
            "sql": reconstruction["datasphere_sql"],
            "query_status": datasphere_status,
            "record_count": len(datasphere_rows) if datasphere_status.startswith("SUCCEEDED") else None,
            "total_rows": len(datasphere_rows) if datasphere_status.startswith("SUCCEEDED") else None,
            "truncated": False,
            "latency_seconds": round(datasphere_latency, 3),
            "execution_engine": "SAP HANA Cloud Direct SQL" if not datasphere_error else "SAP HANA Cloud Direct SQL (Error)",
            "columns": datasphere_columns,
            "total_rows": len(datasphere_rows) if datasphere_status.startswith("SUCCEEDED") else None,
            "truncated": False,
            "data": datasphere_rows,
            "error": datasphere_error,
        })
        response.s4hana.update({
            "sql": reconstruction["s4hana_sql"],
            "query_status": s4_status,
            "record_count": len(s4_rows) if s4_status.startswith("SUCCEEDED") else None,
            "total_rows": s4_total_rows,
            "truncated": s4_status == "SUCCEEDED_TRUNCATED",
            "latency_seconds": round(s4_latency, 3),
            "execution_engine": "SAP S/4HANA CDS Open SQL (ADT)" if not s4_error else "SAP S/4HANA CDS Open SQL (ADT Error)",
            "columns": s4_columns,
            "data": s4_rows,
            "error": s4_error,
        })
        response.comparison["summary_text"] = reason_text
        return response

    required_result_fields = {name.casefold() for name in target_fields + key_fields}
    for label, rows in (("Datasphere", datasphere_rows), ("S/4HANA", s4_rows)):
        actual_result_fields = {
            str(name).casefold()
            for row in rows
            for name in row
        }
        missing_result_fields = sorted(required_result_fields - actual_result_fields)
        if missing_result_fields:
            message = f"{label} query did not return required fields: {', '.join(missing_result_fields)}"
            response = _reconciliation_incomplete_response(
                view_name=view_name, space_id=space_id, source_cds_view=source_cds_view,
                user_prompt=user_prompt, target_fields=target_fields, key_fields=key_fields,
                architecture_metadata=architecture, reconstruction=reconstruction,
                message=message,
            )
            response.datasphere.update({
                "sql": reconstruction["datasphere_sql"], "query_status": datasphere_status,
                "record_count": len(datasphere_rows) if datasphere_status.startswith("SUCCEEDED") else None,
                "total_rows": len(datasphere_rows) if datasphere_status.startswith("SUCCEEDED") else None,
                "truncated": False, "latency_seconds": round(datasphere_latency, 3),
                "execution_engine": "SAP HANA Cloud Direct SQL", "columns": datasphere_columns,
                "data": datasphere_rows, "error": None,
            })
            response.s4hana.update({
                "sql": reconstruction["s4hana_sql"], "query_status": s4_status,
                "record_count": len(s4_rows) if s4_status.startswith("SUCCEEDED") else None,
                "total_rows": s4_total_rows, "truncated": s4_status == "SUCCEEDED_TRUNCATED",
                "latency_seconds": round(s4_latency, 3),
                "execution_engine": "SAP S/4HANA CDS Open SQL (ADT)",
                "columns": s4_columns, "data": s4_rows, "error": None,
            })
            response.comparison["summary_text"] = message + "; both queries ran, but no reconciliation was attempted."
            return response

    def row_value(row: Dict[str, Any], name: str) -> Any:
        lower_name = name.casefold()
        return next((value for key, value in row.items() if str(key).casefold() == lower_name), None)

    def record_key(row: Dict[str, Any]) -> Optional[Tuple[str, ...]]:
        values = [row_value(row, key) for key in key_fields]
        if any(value is None or not str(value).strip() for value in values):
            return None
        return tuple(str(value).strip() for value in values)

    def numeric_value(value: Any) -> Optional[Decimal]:
        if value is None or str(value).strip() == "":
            return None
        try:
            number = Decimal(str(value).replace(",", "").strip())
            return number if number.is_finite() else None
        except (InvalidOperation, ValueError, TypeError):
            return None

    ds_by_key: Dict[Tuple[str, ...], List[Dict[str, Any]]] = {}
    s4_by_key: Dict[Tuple[str, ...], List[Dict[str, Any]]] = {}
    for row in datasphere_rows:
        key = record_key(row)
        if key is not None:
            ds_by_key.setdefault(key, []).append(row)
    for row in s4_rows:
        key = record_key(row)
        if key is not None:
            s4_by_key.setdefault(key, []).append(row)

    all_keys = sorted(set(ds_by_key) | set(s4_by_key))
    breakdown = []
    ds_missing_key_rows = [row for row in datasphere_rows if record_key(row) is None]
    s4_missing_key_rows = [row for row in s4_rows if record_key(row) is None]
    all_match = not ds_missing_key_rows and not s4_missing_key_rows
    for key in all_keys:
        ds_matches = ds_by_key.get(key, [])
        s4_matches = s4_by_key.get(key, [])
        matched_rows = len(ds_matches) == 1 and len(s4_matches) == 1
        ds_row = ds_matches[0] if len(ds_matches) == 1 else {}
        s4_row = s4_matches[0] if len(s4_matches) == 1 else {}
        metrics: Dict[str, Any] = {}
        row_match = matched_rows
        for field in target_fields:
            ds_val = row_value(ds_row, field) if ds_row else None
            s4_val = row_value(s4_row, field) if s4_row else None
            ds_number = numeric_value(ds_val)
            s4_number = numeric_value(s4_val)
            if matched_rows and ds_number is not None and s4_number is not None:
                delta = abs(ds_number - s4_number)
                match = delta == 0
                variance = (delta / abs(s4_number) * 100) if s4_number else (Decimal(0) if delta == 0 else Decimal(100))
                shown_ds, shown_s4 = float(ds_number), float(s4_number)
            else:
                match = matched_rows and str(ds_val or "").strip().casefold() == str(s4_val or "").strip().casefold()
                delta = 0 if match else 1
                variance = 0 if match else 100
                shown_ds, shown_s4 = ds_val, s4_val
            row_match = row_match and match
            metrics[field] = {
                "datasphere_kpi": shown_ds, "s4hana_kpi": shown_s4,
                "delta": round(float(delta), 6), "variance_pct": round(float(variance), 6),
                "is_match": match,
            }
        all_match = all_match and row_match
        primary = metrics.get(target_fields[0], {}) if target_fields else {}
        row_status = "MATCH" if row_match else (
            "AMBIGUOUS_KEY" if len(ds_matches) > 1 or len(s4_matches) > 1 else "DISCREPANCY"
        )
        breakdown.append({
            "grain_key": " | ".join(key),
            "datasphere_kpi": primary.get("datasphere_kpi") or 0,
            "s4hana_kpi": primary.get("s4hana_kpi") or 0,
            "delta": primary.get("delta", 0),
            "variance_pct": primary.get("variance_pct", 0),
            "datasphere_count": len(ds_matches),
            "s4hana_count": len(s4_matches),
            "metrics": metrics,
            "fields": metrics,
            "status": row_status,
        })
    for source_name, missing_rows in (
        ("Datasphere", ds_missing_key_rows),
        ("S/4HANA", s4_missing_key_rows),
    ):
        for index, _ in enumerate(missing_rows, start=1):
            breakdown.append({
                "grain_key": f"Missing key on {source_name} row {index}",
                "datasphere_kpi": 0,
                "s4hana_kpi": 0,
                "delta": 1,
                "variance_pct": 100,
                "datasphere_count": 0 if source_name == "S/4HANA" else 1,
                "s4hana_count": 0 if source_name == "Datasphere" else 1,
                "metrics": {
                    field: {
                        "datasphere_kpi": None, "s4hana_kpi": None,
                        "delta": 1, "variance_pct": 100, "is_match": False,
                    }
                    for field in target_fields
                },
                "fields": {
                    field: {
                        "datasphere_kpi": None, "s4hana_kpi": None,
                        "delta": 1, "variance_pct": 100, "is_match": False,
                    }
                    for field in target_fields
                },
                "status": "MISSING_KEY",
            })

    metric_comparisons = _aggregate_measure_comparisons(
        selected_fields=list(dict.fromkeys(target_fields + key_fields)),
        target_columns=list(execution_target.columns),
        transformation_ir=architecture.get("transformation_ir", {}),
        datasphere_rows=datasphere_rows,
        s4hana_rows=s4_rows,
        datasphere_status=datasphere_status,
        s4hana_status=s4_status,
        semantic_status=reconstruction.get(
            "semantic_equivalence_status",
            "SEMANTIC_EQUIVALENCE_UNVERIFIED",
        ),
    )

    status = "MATCH" if all_match and bool(breakdown) else "MISMATCH"
    summary = (
        f"All {len(breakdown)} grouped records match after reproducing the resolved Datasphere architecture."
        if status == "MATCH" else
        f"{sum(item['status'] == 'MATCH' for item in breakdown)} of {len(breakdown)} grouped records match; review discrepancies and duplicate/missing keys."
    )
    reconstruction["status"] = "EXECUTED"
    reconstruction["logic_summary"] = str(plan.get("logic_summary", ""))
    return ValidationResponse(
        status=status,
        view_name=view_name,
        space_id=space_id,
        source_cds_view=", ".join(discovered_sources or [source_cds_view]),
        target_metric=target_fields[0] if target_fields else "",
        target_metrics=target_fields,
        reconciliation_dimension=f"Keys: {', '.join(key_fields)}",
        user_prompt=user_prompt,
        datasphere={
            "view": execution_target.fact_model, "runtime_object": execution_target.object_name,
            "sql": reconstruction["datasphere_sql"],
            "total_kpi": (
                metric_comparisons[0]["datasphere_total"]
                if metric_comparisons else None
            ),
            "formatted_kpi": (
                metric_comparisons[0]["formatted_datasphere"]
                if metric_comparisons else "Not computed"
            ),
            "record_count": len(datasphere_rows), "latency_seconds": round(datasphere_latency, 3),
            "execution_engine": "SAP HANA Cloud Direct SQL", "data": datasphere_rows,
            "query_status": datasphere_status,
            "columns": datasphere_columns,
            "error": None,
        },
        s4hana={
            "cds_view": ", ".join(discovered_sources or [source_cds_view]),
            "sql": reconstruction["s4hana_sql"],
            "total_kpi": (
                metric_comparisons[0]["s4hana_total"]
                if metric_comparisons else None
            ),
            "formatted_kpi": (
                metric_comparisons[0]["formatted_s4hana"]
                if metric_comparisons else "Not computed"
            ),
            "record_count": len(s4_rows), "total_rows": s4_total_rows,
            "truncated": s4_status == "SUCCEEDED_TRUNCATED",
            "latency_seconds": round(s4_latency, 3),
            "execution_engine": "SAP S/4HANA CDS Open SQL (ADT)", "data": s4_rows,
            "query_status": s4_status,
            "columns": s4_columns,
            "error": None,
        },
        comparison={
            "target_metric": target_fields[0] if target_fields else "",
            "delta": metric_comparisons[0]["delta"] if metric_comparisons else None,
            "variance_pct": metric_comparisons[0]["variance_pct"] if metric_comparisons else None,
            "is_match": status == "VALIDATED",
            "summary_text": summary,
            "security": "Metadata-only LLM planning; transaction rows remain in SAP/back-end comparison memory.",
        },
        metrics_comparison=metric_comparisons,
        lineage_chain=[
            {"name": node.get("name", ""), "type": node.get("object_type", ""),
             "space": space_id, "role": node.get("relationship", "resolved Datasphere dependency")}
            for node in nodes
        ],
        grain_breakdown=breakdown,
        architecture_metadata=architecture,
        reconstruction=reconstruction,
    )
