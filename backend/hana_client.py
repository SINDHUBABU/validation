import re
import time
from typing import List, Dict, Any, Tuple
from fastapi import HTTPException
from config import (
    logger, hana_dbapi, HANA_HOST, HANA_PORT, HANA_USER, HANA_PASSWORD,
    HANA_ENCRYPT, HANA_SSL_VALIDATE_CERT, USE_DIRECT_HANA, view_parameters_cache
)

def get_hana_connection():
    if not hana_dbapi:
        raise HTTPException(
            status_code=500, 
            detail="The 'hdbcli' package is not installed. Please run: pip install hdbcli"
        )
    return hana_dbapi.connect(
        address=HANA_HOST,
        port=HANA_PORT,
        user=HANA_USER,
        password=HANA_PASSWORD,
        encrypt=HANA_ENCRYPT,
        sslValidateCertificate=HANA_SSL_VALIDATE_CERT
    )

def probe_hana_view_parameters_sync(space_id: str, view_name: str) -> List[Dict[str, Any]]:
    """
    Queries SAP HANA data dictionary (SYS.VIEW_PARAMETERS / SYS.TABLE_PARAMETERS / _SYS_BI.BIMC_VARIABLE)
    to discover mandatory and optional input parameters for parameterized calculation/SQL views.
    """
    cache_key = f"{space_id}/{view_name}"
    if cache_key in view_parameters_cache and view_parameters_cache[cache_key]:
        return view_parameters_cache[cache_key]

    params: List[Dict[str, Any]] = []

    if USE_DIRECT_HANA and hana_dbapi:
        try:
            conn = get_hana_connection()
            cursor = conn.cursor()
            
            # 1. Query SYS.VIEW_PARAMETERS
            try:
                cursor.execute("""
                    SELECT PARAMETER_NAME, DATA_TYPE_NAME, POSITION 
                    FROM SYS.VIEW_PARAMETERS 
                    WHERE (SCHEMA_NAME = ? OR UPPER(SCHEMA_NAME) = UPPER(?)) 
                      AND (VIEW_NAME = ? OR UPPER(VIEW_NAME) = UPPER(?))
                    ORDER BY POSITION
                """, (space_id, space_id, view_name, view_name))
                rows = cursor.fetchall()
                if not rows:
                    cursor.execute("""
                        SELECT PARAMETER_NAME, DATA_TYPE_NAME, POSITION 
                        FROM SYS.VIEW_PARAMETERS 
                        WHERE (VIEW_NAME = ? OR UPPER(VIEW_NAME) = UPPER(?))
                        ORDER BY POSITION
                    """, (view_name, view_name))
                    rows = cursor.fetchall()
                for r in rows:
                    p_name = str(r[0]).strip()
                    p_type = str(r[1]).strip() if len(r) > 1 and r[1] else "VARCHAR"
                    if p_name and not any(p["name"].upper() == p_name.upper() for p in params):
                        params.append({"name": p_name, "type": p_type})
            except Exception as ve:
                logger.debug(f"SYS.VIEW_PARAMETERS check notice for {space_id}/{view_name}: {ve}")

            # 2. Query SYS.TABLE_PARAMETERS if nothing found yet
            if not params:
                try:
                    cursor.execute("""
                        SELECT PARAMETER_NAME, DATA_TYPE_NAME, POSITION 
                        FROM SYS.TABLE_PARAMETERS 
                        WHERE (SCHEMA_NAME = ? OR UPPER(SCHEMA_NAME) = UPPER(?)) 
                          AND (TABLE_NAME = ? OR UPPER(TABLE_NAME) = UPPER(?))
                        ORDER BY POSITION
                    """, (space_id, space_id, view_name, view_name))
                    rows = cursor.fetchall()
                    if not rows:
                        cursor.execute("""
                            SELECT PARAMETER_NAME, DATA_TYPE_NAME, POSITION 
                            FROM SYS.TABLE_PARAMETERS 
                            WHERE (TABLE_NAME = ? OR UPPER(TABLE_NAME) = UPPER(?))
                            ORDER BY POSITION
                        """, (view_name, view_name))
                        rows = cursor.fetchall()
                    for r in rows:
                        p_name = str(r[0]).strip()
                        p_type = str(r[1]).strip() if len(r) > 1 and r[1] else "VARCHAR"
                        if p_name and not any(p["name"].upper() == p_name.upper() for p in params):
                            params.append({"name": p_name, "type": p_type})
                except Exception as te:
                    logger.debug(f"SYS.TABLE_PARAMETERS check notice for {space_id}/{view_name}: {te}")

            # 3. Query _SYS_BI.BIMC_VARIABLE if still not found
            if not params:
                try:
                    cursor.execute("""
                        SELECT VARIABLE_NAME, DATA_TYPE 
                        FROM _SYS_BI.BIMC_VARIABLE 
                        WHERE (CUBE_NAME = ? OR UPPER(CUBE_NAME) = UPPER(?))
                    """, (view_name, view_name))
                    rows = cursor.fetchall()
                    for r in rows:
                        p_name = str(r[0]).strip()
                        p_type = str(r[1]).strip() if len(r) > 1 and r[1] else "VARCHAR"
                        if p_name and not any(p["name"].upper() == p_name.upper() for p in params):
                            params.append({"name": p_name, "type": p_type})
                except Exception as be:
                    logger.debug(f"_SYS_BI.BIMC_VARIABLE check notice for {view_name}: {be}")

            conn.close()
        except Exception as e:
            logger.warning(f"Error checking parameters for {space_id}/{view_name}: {e}")

    # Fallback for known parameterized views
    if not params and ("os_cs" in view_name.lower() or "fv_os" in view_name.lower()):
        params = [{"name": "START_DATE", "type": "DATE"}, {"name": "CLOSE_DATE", "type": "DATE"}]

    if params:
        logger.info(f"Discovered {len(params)} input parameters for [{space_id}.{view_name}]: {[p['name'] for p in params]}")
        view_parameters_cache[cache_key] = params
    return params

def extract_dates_from_prompt(prompt: str) -> List[str]:
    """Extracts dates in ISO format (YYYY-MM-DD) from natural language."""
    dates = []
    p_lower = (prompt or "").lower()
    
    months_map = {
        "jan": "01", "january": "01", "feb": "02", "february": "02", "mar": "03", "march": "03",
        "apr": "04", "april": "04", "may": "05", "jun": "06", "june": "06", "jul": "07", "july": "07",
        "aug": "08", "august": "08", "sep": "09", "september": "09", "oct": "10", "october": "10",
        "nov": "11", "november": "11", "dec": "12", "december": "12"
    }
    month_names_regex = "|".join(months_map.keys())
    
    # Month Day Year
    m_d_y = re.findall(rf'\b({month_names_regex})\s+(\d{{1,2}})(?:st|nd|rd|th)?,?\s+(20\d{{2}})\b', p_lower)
    for m_name, day, yr in m_d_y:
        m_num = months_map[m_name]
        d_num = f"{int(day):02d}"
        d_str = f"{yr}-{m_num}-{d_num}"
        if d_str not in dates:
            dates.append(d_str)
        
    # Day Month Year
    d_m_y = re.findall(rf'\b(\d{{1,2}})(?:st|nd|rd|th)?\s+({month_names_regex}),?\s+(20\d{{2}})\b', p_lower)
    for day, m_name, yr in d_m_y:
        m_num = months_map[m_name]
        d_num = f"{int(day):02d}"
        d_str = f"{yr}-{m_num}-{d_num}"
        if d_str not in dates:
            dates.append(d_str)
            
    # Standard ISO dates: YYYY-MM-DD or YYYY/MM/DD
    iso_matches = re.findall(r'\b(20\d{2})[-/.](0?[1-9]|1[0-2])[-/.](0?[1-9]|[12]\d|3[01])\b', p_lower)
    for yr, mo, dy in iso_matches:
        d_str = f"{yr}-{int(mo):02d}-{int(dy):02d}"
        if d_str not in dates:
            dates.append(d_str)
            
    # Standalone 4-digit years
    if not dates:
        year_matches = re.findall(r'\b(20\d{2})\b', p_lower)
        for yr in year_matches:
            dates.append(f"{yr}-01-01")
            dates.append(f"{yr}-12-31")
            
    return dates

def resolve_parameter_values(user_prompt: str, params: List[Dict[str, Any]]) -> List[Tuple[str, str]]:
    resolved = []
    date_matches = extract_dates_from_prompt(user_prompt or "")
    current_year = time.strftime("%Y")
    today_str = time.strftime("%Y-%m-%d")
    
    for p in params:
        p_name = p["name"]
        p_type = p.get("type", "VARCHAR").upper()
        p_upper = p_name.upper()
        val = None
        
        explicit_match = re.search(rf'{re.escape(p_name)}\s*[:=]\s*[\'"]?([^\s,\'\"]+)[\'"]?', user_prompt or "", re.IGNORECASE)
        if explicit_match:
            val = explicit_match.group(1).strip()
            
        if not val:
            if "START" in p_upper or "FROM" in p_upper or "BEGIN" in p_upper or "BEGDA" in p_upper or "MIN" in p_upper:
                val = date_matches[0] if date_matches else "1900-01-01"
            elif "CLOSE" in p_upper or "END" in p_upper or "TO" in p_upper or "ENDDA" in p_upper or "MAX" in p_upper:
                if len(date_matches) > 1:
                    val = date_matches[1]
                elif len(date_matches) == 1:
                    val = date_matches[0]
                else:
                    val = "2099-12-31"
            elif "DATE" in p_upper or p_type in ("DATE", "SECONDDATE", "TIMESTAMP"):
                val = date_matches[0] if date_matches else today_str
            elif "YEAR" in p_upper:
                val = current_year
            elif "CURRENCY" in p_upper or "WAERS" in p_upper:
                val = "USD"
            elif p_type in ("INTEGER", "INT", "BIGINT", "DECIMAL", "NUMERIC"):
                val = "0"
            else:
                val = "*"
                
        resolved.append((p_name, str(val)))
        
    return resolved

def format_hana_view_param_clause(view_name: str, resolved_params: List[Tuple[str, str]], style: str = "positional") -> str:
    if not resolved_params:
        return f'"{view_name}"'
        
    if style == "positional":
        args = [f"'{p_val}'" for _, p_val in resolved_params]
        return f'"{view_name}"({", ".join(args)})'
    elif style == "named":
        args = [f"{p_name} => '{p_val}'" for p_name, p_val in resolved_params]
        return f'"{view_name}"({", ".join(args)})'
    elif style == "named_quoted":
        args = [f'"{p_name}" => \'{p_val}\'' for p_name, p_val in resolved_params]
        return f'"{view_name}"({", ".join(args)})'
    elif style == "date_typed":
        args = [f"DATE '{p_val}'" if re.match(r'^\d{4}-\d{2}-\d{2}$', p_val) else f"'{p_val}'" for _, p_val in resolved_params]
        return f'"{view_name}"({", ".join(args)})'
    elif style == "placeholder":
        placeholders = [f"'PLACEHOLDER' = ('$${p_name}$$', '{p_val}')" for p_name, p_val in resolved_params]
        return f'"{view_name}"({", ".join(placeholders)})'
    else:
        args = [f"'{p_val}'" for _, p_val in resolved_params]
        return f'"{view_name}"({", ".join(args)})'

def format_hana_view_placeholder(view_name: str, resolved_params: List[Tuple[str, str]]) -> str:
    return format_hana_view_param_clause(view_name, resolved_params, style="positional")

def inject_hana_view_placeholders(sql_str: str, space_id: str, view_name: str, params: List[Dict[str, Any]], user_prompt: str = "", style: str = "positional") -> str:
    if not params:
        return sql_str
        
    resolved = resolve_parameter_values(user_prompt, params)
    formatted_ref = format_hana_view_param_clause(view_name, resolved, style=style)
    
    escaped_v = re.escape(view_name)
    escaped_s = re.escape(space_id) if space_id else ""
    
    cleaned_sql = re.sub(rf'("?{escaped_v}"?)\s*\([^\)]*\)', r'\1', sql_str, flags=re.IGNORECASE)
    
    if escaped_s:
        pattern_schema = re.compile(rf'("?{escaped_s}"?\.)("?{escaped_v}"?)', re.IGNORECASE)
        if pattern_schema.search(cleaned_sql):
            return pattern_schema.sub(rf'\1{formatted_ref}', cleaned_sql)
            
    pattern_table = re.compile(rf'(\bFROM|\bJOIN)\s+("?{escaped_v}"?)', re.IGNORECASE)
    if pattern_table.search(cleaned_sql):
        return pattern_table.sub(rf'\1 {formatted_ref}', cleaned_sql)
        
    pattern_generic = re.compile(rf'("?{escaped_v}"?)', re.IGNORECASE)
    return pattern_generic.sub(rf'{formatted_ref}', cleaned_sql, count=1)

def execute_direct_hana_query(
    sql_query: str,
    space_id: str,
    user_prompt: str = "",
    *,
    allow_parameter_fallback: bool = True,
    include_column_metadata: bool = False,
) -> Tuple[List[Dict[str, Any]], float, str] | Tuple[List[Dict[str, Any]], float, str, List[Dict[str, Any]]]:
    """Executes compiled SQL query directly on SAP Datasphere HANA Cloud engine."""
    clean_sql = sql_query.strip().rstrip(";")
    t_start = time.time()
    conn = get_hana_connection()
    try:
        cursor = conn.cursor()
        if space_id:
            try:
                cursor.execute(f'SET SCHEMA "{space_id}"')
            except Exception as se:
                logger.debug(f"HANA SET SCHEMA notice: {se}")
        
        logger.info(f"[HANA Cloud Direct SQL] Executing query in [{space_id}]:\n{clean_sql}")
        try:
            cursor.execute(clean_sql)
        except Exception as first_err:
            err_msg = str(first_err)
            if allow_parameter_fallback and (
                "383" in err_msg or "parameterized sql view" in err_msg.lower()
                or "input arguments" in err_msg.lower() or "placeholder" in clean_sql.lower()
            ):
                logger.warning(f"[HANA Error Detected] {err_msg}. Testing multi-syntax parameterized view cascade...")
                
                views_in_query = []
                for token in re.findall(r'["\']?([a-zA-Z0-9_]+)["\']?', clean_sql):
                    if token.upper() not in ("SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "CROSS", "ON", "AND", "OR", "GROUP", "BY", "ORDER", "AS", "T1", "T2", "T3", "LIMIT", "TOP", "SUM", "AVG", "MAX", "MIN", "COUNT", "CAST", "REAL", "NULL", "COALESCE", "PLACEHOLDER", "DATE"):
                        if token not in views_in_query:
                            views_in_query.append(token)

                styles_to_try = ["positional", "named", "named_quoted", "date_typed", "placeholder"]
                succeeded = False
                last_try_err = first_err

                for style in styles_to_try:
                    candidate_sql = clean_sql
                    for v in views_in_query:
                        v_params = probe_hana_view_parameters_sync(space_id, v)
                        if not v_params and ("os_cs" in v.lower() or "fv_os" in v.lower()):
                            v_params = [{"name": "START_DATE", "type": "DATE"}, {"name": "CLOSE_DATE", "type": "DATE"}]
                        if v_params:
                            candidate_sql = inject_hana_view_placeholders(candidate_sql, space_id, v, v_params, user_prompt, style=style)

                    if candidate_sql == clean_sql and views_in_query:
                        fallback_v = views_in_query[0]
                        default_params = [{"name": "START_DATE", "type": "DATE"}, {"name": "CLOSE_DATE", "type": "DATE"}]
                        candidate_sql = inject_hana_view_placeholders(candidate_sql, space_id, fallback_v, default_params, user_prompt, style=style)

                    try:
                        logger.info(f"[HANA Cloud Direct SQL] Testing [{style}] parameterized syntax:\n{candidate_sql}")
                        cursor.execute(candidate_sql)
                        succeeded = True
                        clean_sql = candidate_sql
                        logger.info(f"[HANA Cloud Direct SQL] Succeeded with [{style}] syntax!")
                        break
                    except Exception as try_err:
                        last_try_err = try_err
                        logger.debug(f"Syntax [{style}] failed: {try_err}")

                if not succeeded:
                    raise last_try_err
            else:
                raise first_err

        elapsed = time.time() - t_start
        
        if cursor.description:
            cols = [desc[0] for desc in cursor.description]
            column_metadata = [
                {"name": desc[0], "type": str(desc[1]) if len(desc) > 1 and desc[1] is not None else None}
                for desc in cursor.description
            ]
            raw_rows = cursor.fetchall()
            results = [dict(zip(cols, row)) for row in raw_rows]
            logger.info(f"[HANA Cloud Direct SQL] Succeeded in {elapsed:.3f}s -> {len(results)} rows returned.")
            if include_column_metadata:
                return results, elapsed, clean_sql, column_metadata
            return results, elapsed, clean_sql
        if include_column_metadata:
            return [], elapsed, clean_sql, []
        return [], elapsed, clean_sql
    finally:
        conn.close()

def probe_hana_columns_sync(space_id: str, view_name: str) -> List[str]:
    """Queries SAP HANA data dictionary (SYS.TABLE_COLUMNS) for view schema."""
    if not USE_DIRECT_HANA or not hana_dbapi:
        return []


def discover_hana_execution_object_sync(
    logical_name: str,
    preferred_schema: str = "",
) -> Dict[str, Any] | None:
    """Resolve an exact queryable HANA catalog object and its columns.

    This is verification, not name inference: only an exact catalog object
    name match is accepted, and the schema is returned by HANA metadata.
    """
    if not USE_DIRECT_HANA or not hana_dbapi or not logical_name:
        return None
    conn = None
    try:
        conn = get_hana_connection()
        cursor = conn.cursor()
        schema_filter = " AND UPPER(SCHEMA_NAME) = UPPER(?)" if preferred_schema else ""
        args: tuple[Any, ...] = (logical_name,)
        if preferred_schema:
            args = (logical_name, preferred_schema)
        cursor.execute(
            "SELECT SCHEMA_NAME, TABLE_NAME FROM SYS.TABLES "
            "WHERE UPPER(TABLE_NAME) = UPPER(?)" + schema_filter + " "
            "UNION ALL "
            "SELECT SCHEMA_NAME, VIEW_NAME FROM SYS.VIEWS "
            "WHERE UPPER(VIEW_NAME) = UPPER(?)" + schema_filter,
            args + args,
        )
        matches = cursor.fetchall()
        if len(matches) != 1:
            return None
        schema, object_name = str(matches[0][0]), str(matches[0][1])
        cursor.execute(
            "SELECT COLUMN_NAME, DATA_TYPE_NAME FROM SYS.TABLE_COLUMNS "
            "WHERE SCHEMA_NAME = ? AND TABLE_NAME = ? ORDER BY POSITION",
            (schema, object_name),
        )
        columns = [{"name": str(row[0]), "data_type": str(row[1])} for row in cursor.fetchall()]
        return {"schema": schema, "object": object_name, "columns": columns}
    except Exception as exc:
        logger.warning("HANA execution-object catalog discovery failed for %s: %s", logical_name, type(exc).__name__)
        return None
    finally:
        if conn is not None:
            conn.close()
    try:
        conn = get_hana_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COLUMN_NAME 
            FROM SYS.TABLE_COLUMNS 
            WHERE (SCHEMA_NAME = ? OR UPPER(SCHEMA_NAME) = UPPER(?)) 
              AND (TABLE_NAME = ? OR UPPER(TABLE_NAME) = UPPER(?))
            ORDER BY POSITION
        """, (space_id, space_id, view_name, view_name))
        rows = cursor.fetchall()
        cols = [r[0] for r in rows] if rows else []
        conn.close()
        return cols
    except Exception as e:
        logger.debug(f"Direct HANA column probe notice for {space_id}/{view_name}: {e}")
        return []

def probe_hana_space_views_sync(space_id: str) -> List[str]:
    """Queries SAP HANA dictionary (SYS.VIEWS & SYS.TABLES) for all views in the space."""
    if not USE_DIRECT_HANA or not hana_dbapi:
        return []
    try:
        conn = get_hana_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT VIEW_NAME 
            FROM SYS.VIEWS 
            WHERE (SCHEMA_NAME = ? OR UPPER(SCHEMA_NAME) = UPPER(?))
            ORDER BY VIEW_NAME
        """, (space_id, space_id))
        rows = cursor.fetchall()
        v_names = [r[0] for r in rows if r and r[0] and not r[0].startswith("SYS_") and not r[0].startswith("_SYS_")]
        
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM SYS.TABLES 
            WHERE (SCHEMA_NAME = ? OR UPPER(SCHEMA_NAME) = UPPER(?))
              AND TABLE_TYPE != 'SYSTEM TABLE'
            ORDER BY TABLE_NAME
        """, (space_id, space_id))
        t_rows = cursor.fetchall()
        for tr in t_rows:
            if tr and tr[0] and not tr[0].startswith("SYS_") and not tr[0].startswith("_SYS_"):
                if tr[0] not in v_names:
                    v_names.append(tr[0])
                    
        conn.close()
        logger.info(f"Direct HANA Discovery found {len(v_names)} views/tables in Space [{space_id}]")
        return v_names
    except Exception as e:
        logger.warning(f"Error querying HANA views for space {space_id}: {e}")
        return []
