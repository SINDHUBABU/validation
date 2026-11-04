import re
import difflib
import time
import sqlite3
from typing import Optional, Dict, Any, List, Tuple
from config import logger, conversation_sessions, USE_DIRECT_HANA
from hana_client import resolve_parameter_values, format_hana_view_placeholder
from datasphere_client import deduplicate_view_names

def resolve_best_matching_column(target_col: str, available_columns: List[str]) -> Optional[str]:
    if not target_col or not available_columns:
        return None
    cleaned_input = target_col.strip().strip("'\"`")
    raw_clean = re.sub(r'^(?:the|a|an|all|its|their|that|where|by|each|per|for)\s+', '', cleaned_input, flags=re.IGNORECASE).strip().strip("'\"`")
    target_clean = raw_clean.lower().replace("_", "").replace(" ", "")
    if not target_clean or len(target_clean) < 2:
        return None

    for c in available_columns:
        if c.lower() == raw_clean.lower() or c.lower() == cleaned_input.lower():
            return c
    for c in available_columns:
        if c.lower().replace("_", "").replace(" ", "") == target_clean:
            return c
    if len(target_clean) >= 3:
        for c in available_columns:
            c_clean = c.lower().replace("_", "").replace(" ", "")
            if target_clean in c_clean or (len(c_clean) >= 4 and c_clean in target_clean):
                return c
    t_tokens = [t for t in re.split(r'[^a-zA-Z0-9]+', raw_clean.lower()) if len(t) >= 3]
    if t_tokens:
        best_c = None
        best_score = 0
        for c in available_columns:
            c_tokens = [t for t in re.split(r'[^a-zA-Z0-9]+', c.lower()) if len(t) >= 3]
            overlap = sum(1 for t in t_tokens if any(t in ct or ct in t for ct in c_tokens))
            if overlap > best_score:
                best_score = overlap
                best_c = c
        if best_score > 0:
            return best_c
    if len(target_clean) >= 3:
        matches = difflib.get_close_matches(target_clean, [c.lower().replace("_", "").replace(" ", "") for c in available_columns], n=1, cutoff=0.60)
        if matches:
            matched_str = matches[0]
            for c in available_columns:
                if c.lower().replace("_", "").replace(" ", "") == matched_str:
                    return c
    return None

def detect_foreign_key_relations(view_schemas: Dict[str, List[str]]) -> List[Dict[str, Any]]:
    relations = []
    view_names = list(view_schemas.keys())
    
    material_keys = ["materialnumber", "matnr", "productnumber", "material", "product", "article", "itemnumber"]
    customer_keys = ["customernumber", "kunnr", "customer", "customerid", "accountnumber", "supplieraccount", "supplier"]
    order_doc_keys = ["salesorder", "salesdocument", "purchaseorder", "documentnumber", "belnr", "vbeln", "ebeln", "accountingdocument"]
    org_keys = ["companycode", "bukrs", "plant", "werks", "salesorg", "vkorg", "fiscalyear", "gjahr", "postingyear"]
    low_priority_terms = ["description", "desc", "name", "text", "comment", "breach", "flag", "status", "indicator", "warning", "critical", "type", "mark", "valid"]

    for i in range(len(view_names)):
        for j in range(i + 1, len(view_names)):
            v1 = view_names[i]
            v2 = view_names[j]
            cols1 = view_schemas[v1]
            cols2 = view_schemas[v2]
            
            scored_keys = []
            for c1 in cols1:
                c1_clean = c1.lower().replace("_", "").replace(" ", "")
                for c2 in cols2:
                    c2_clean = c2.lower().replace("_", "").replace(" ", "")
                    is_match = (c1.lower() == c2.lower() or c1_clean == c2_clean)
                    is_fuzzy = not is_match and (c1_clean in c2_clean or c2_clean in c1_clean) and any(term in c1_clean for term in material_keys + customer_keys + order_doc_keys + org_keys)
                    
                    if is_match or is_fuzzy:
                        score = 150 if is_match else 50
                        if any(t in c1_clean for t in org_keys):
                            score += 280
                        elif any(t in c1_clean for t in material_keys):
                            score += 270
                        elif any(t in c1_clean for t in customer_keys):
                            score += 260
                        elif any(t in c1_clean for t in order_doc_keys):
                            score += 200
                        if any(t in c1_clean for t in low_priority_terms):
                            score -= 200
                        
                        scored_keys.append((score, {
                            "col1": c1,
                            "col2": c2,
                            "key_name": c1 if is_match else f"{c1} ~ {c2}"
                        }))
            
            if scored_keys:
                scored_keys.sort(key=lambda x: x[0], reverse=True)
                sorted_keys = [k[1] for k in scored_keys]
                relations.append({
                    "view_a": v1,
                    "view_b": v2,
                    "join_keys": sorted_keys
                })
    return relations

def resolve_conversational_follow_up(
    user_prompt: str,
    session_id: Optional[str]
) -> Tuple[str, Optional[Dict[str, Any]]]:
    if not session_id or session_id not in conversation_sessions:
        return user_prompt, None

    session = conversation_sessions[session_id]
    prev_data = session.get("last_data", [])
    prev_views = session.get("last_views", [])
    prev_prompt = session.get("last_prompt", "")
    prompt_lower = user_prompt.lower().strip()
    
    same_patterns = [
        r'\b(?:same\s+like\s+this\s+for|same\s+for|do\s+(?:the\s+)?same\s+for|now\s+get\s+me\s+same\s+(?:like\s+this\s+)?for|now\s+for|what\s+about\s+(?:for\s+)?|also\s+for|similar\s+for|repeat\s+for)\s+(?:the\s+)?(.+)',
        r'\b(?:same\s+like\s+this|same\s+thing|do\s+the\s+same)\b'
    ]
    for s_pat in same_patterns:
        s_match = re.search(s_pat, prompt_lower)
        if s_match:
            target_entity = s_match.group(1).strip() if s_match.groups() else ""
            augmented_prompt = f"get me only the data for {target_entity}" if target_entity else user_prompt
            context_info = {
                "follow_up_type": "same_operation",
                "previous_views": prev_views,
                "previous_prompt": prev_prompt,
                "target_entity": target_entity
            }
            logger.info(f"Conversational Follow-Up Resolved: '{user_prompt}' -> Augmented: '{augmented_prompt}' with View(s) {prev_views}")
            return augmented_prompt, context_info

    if not prev_data:
        if prev_views:
            return user_prompt, {"previous_views": prev_views}
        return user_prompt, None
    
    ordinal_map = {
        "1st": 0, "first": 0, "top 1": 0, "top one": 0, "number 1": 0, "#1": 0,
        "2nd": 1, "second": 1, "top 2": 1, "number 2": 1, "#2": 1,
        "3rd": 2, "third": 2, "number 3": 2, "#3": 2,
        "4th": 3, "fourth": 3, "number 4": 3,
        "5th": 4, "fifth": 4, "number 5": 4,
        "last": -1, "last one": -1
    }

    target_index = None
    for pattern, idx in ordinal_map.items():
        if re.search(rf'\b{re.escape(pattern)}\b', prompt_lower):
            target_index = idx
            break

    if target_index is None and any(p in prompt_lower for p in ["their orders", "explain customer", "that customer", "this customer", "their details", "break down"]):
        target_index = 0

    if target_index is not None and abs(target_index) < len(prev_data):
        target_row = prev_data[target_index]
        key_col = None
        key_val = None
        
        for col, val in target_row.items():
            c_clean = col.lower().replace("_", "")
            if val is not None and str(val).strip() != "" and any(term in c_clean for term in ["name", "code", "id", "num", "key"]):
                key_col = col
                key_val = val
                break

        if not key_col:
            for col, val in target_row.items():
                if isinstance(val, str) and val.strip() and not col.lower().startswith("sum") and not col.lower().startswith("avg"):
                    key_col = col
                    key_val = val
                    break

        if key_col and key_val:
            augmented_prompt = f"{user_prompt} WHERE {key_col} IS '{key_val}'"
            context_info = {
                "referenced_entity": f"{key_col} = '{key_val}'",
                "entity_key": key_col,
                "entity_value": key_val,
                "target_row": target_row,
                "previous_views": prev_views
            }
            logger.info(f"Conversational Context Resolved: '{user_prompt}' -> Filter: [{key_col} = '{key_val}']")
            return augmented_prompt, context_info

    if prev_views:
        return user_prompt, {"previous_views": prev_views}
    return user_prompt, None

def infer_column_sqlite_type(values: List[Any]) -> str:
    non_nulls = [v for v in values if v is not None and str(v).strip() != ""]
    if not non_nulls:
        return "TEXT"
    is_int = True
    is_float = True
    for v in non_nulls[:100]:
        v_str = str(v).replace(",", "").replace("$", "").replace("€", "").replace("₹", "").strip()
        try:
            val_f = float(v_str)
            if not (val_f.is_integer() or abs(val_f - round(val_f)) < 0.0001):
                is_int = False
        except ValueError:
            is_int = False
            is_float = False
            break
            
    if is_int:
        return "INTEGER"
    if is_float:
        return "REAL"
    return "TEXT"

def evaluate_condition_value(row_val: Any, op: str, target_val: Any, is_num: bool) -> bool:
    if row_val is None:
        return False
    if is_num:
        try:
            num_clean = float(str(row_val).replace(",", "").replace("$", "").replace("€", "").replace("₹", "").strip())
            target_num = float(target_val)
            if op == ">": return num_clean > target_num
            if op == ">=": return num_clean >= target_num
            if op == "<": return num_clean < target_num
            if op == "<=": return num_clean <= target_num
            if op == "=": return abs(num_clean - target_num) < 0.0001
            if op == "!=": return abs(num_clean - target_num) >= 0.0001
        except Exception:
            return False
    else:
        str_val = str(row_val).strip().lower()
        target_str = str(target_val).strip().lower()
        if op == "=": return str_val == target_str
        if op == "!=": return str_val != target_str
        if op == "LIKE": return target_str in str_val
    return True

def execute_multi_table_sql(
    multi_table_data: Dict[str, List[Dict[str, Any]]], 
    sql_query: str,
    selected_fields: Optional[List[str]] = None,
    structured_conditions: Optional[List[Dict[str, Any]]] = None,
    view_schemas: Optional[Dict[str, List[str]]] = None
) -> List[Dict[str, Any]]:
    if not multi_table_data and not view_schemas:
        return []

    results = []
    try:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        all_target_views = list(view_schemas.keys()) if view_schemas else list(multi_table_data.keys())
        table_row_counts = {}

        for idx, view_name in enumerate(all_target_views, start=1):
            raw_data = multi_table_data.get(view_name, [])
            primary_name = f't{idx}'
            table_row_counts[primary_name] = len(raw_data)
            unique_cols = []
            seen_c = set()
            col_types = {}

            if raw_data:
                for c in raw_data[0].keys():
                    c_clean = str(c).strip()
                    if c_clean.lower() not in seen_c:
                        seen_c.add(c_clean.lower())
                        unique_cols.append(c_clean)
                for c in unique_cols:
                    col_vals = [r.get(c) for r in raw_data[:200]]
                    col_types[c] = infer_column_sqlite_type(col_vals)
            elif view_schemas and view_name in view_schemas and view_schemas[view_name]:
                for c in view_schemas[view_name]:
                    c_clean = str(c).strip()
                    if c_clean.lower() not in seen_c:
                        seen_c.add(c_clean.lower())
                        unique_cols.append(c_clean)
                        col_types[c_clean] = "TEXT"
            else:
                continue

            col_defs = [f'"{c}" {col_types.get(c, "TEXT")}' for c in unique_cols]
            cursor.execute(f'CREATE TABLE {primary_name} ({", ".join(col_defs)})')

            if raw_data:
                placeholders = ", ".join(["?"] * len(unique_cols))
                insert_sql = f'INSERT INTO {primary_name} ({", ".join([f"{c}" for c in unique_cols])}) VALUES ({placeholders})'
                rows_to_insert = []
                for r in raw_data:
                    row_vals = []
                    for c in unique_cols:
                        v = r.get(c)
                        if v is not None and col_types.get(c) in ("INTEGER", "REAL"):
                            try:
                                v_clean = str(v).replace(",", "").replace("$", "").replace("€", "").replace("₹", "").strip()
                                row_vals.append(int(float(v_clean)) if col_types[c] == "INTEGER" else float(v_clean))
                            except (ValueError, TypeError):
                                row_vals.append(v)
                        else:
                            row_vals.append(v)
                    rows_to_insert.append(row_vals)

                cursor.executemany(insert_sql, rows_to_insert)

        conn.commit()
        cursor.execute(sql_query)
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
    except Exception as sql_err:
        logger.warning(f"SQL execution error on '{sql_query}': {sql_err}.")
        results = []
    return results

def generate_table_info_and_descriptions(raw_data: List[Dict[str, Any]], view_name: str, space_id: str) -> Dict[str, Any]:
    if not raw_data:
        return {
            "view_name": view_name,
            "space_id": space_id,
            "total_records": 0,
            "total_columns": 0,
            "columns": [],
            "summary": "Dataset is empty."
        }

    total_rows = len(raw_data)
    cols = list(raw_data[0].keys())
    col_profiles = []

    for c in cols:
        vals = [r.get(c) for r in raw_data if r.get(c) is not None and str(r.get(c)).strip() != ""]
        distinct_vals = list(dict.fromkeys(vals))
        inferred_type = infer_column_sqlite_type(vals)

        c_lower = c.lower()
        if "year" in c_lower:
            biz_type = "Fiscal/Posting Year"
        elif "month" in c_lower:
            biz_type = "Posting Month"
        elif "date" in c_lower or "time" in c_lower:
            biz_type = "Date / Timestamp"
        elif "breach" in c_lower or "status" in c_lower or "flag" in c_lower:
            biz_type = "SAP Indicator / Flag"
        elif inferred_type in ("INTEGER", "REAL"):
            biz_type = "Numerical Metric / KPI"
        else:
            biz_type = "Text Dimension"

        col_profiles.append({
            "column_name": c,
            "friendly_name": c.replace("_", " ").title(),
            "data_type": biz_type,
            "storage_type": inferred_type,
            "null_count": total_rows - len(vals),
            "distinct_count": len(distinct_vals),
            "sample_values": distinct_vals[:5],
            "description": f"Column '{c}' contains {len(distinct_vals)} distinct {biz_type.lower()} values."
        })

    return {
        "view_name": view_name,
        "space_id": space_id,
        "total_records": total_rows,
        "total_columns": len(cols),
        "columns": col_profiles,
        "summary": f"Table '{view_name}' in Space '{space_id}' contains {total_rows:,} records across {len(cols)} columns."
    }

def extract_where_conditions_from_prompt(
    user_prompt: str, 
    all_cols: List[str],
    col_where_map: Optional[Dict[str, str]] = None
) -> Tuple[List[str], List[str], List[Dict[str, Any]]]:
    where_clauses: List[str] = []
    odata_filters: List[str] = []
    structured_conditions: List[Dict[str, Any]] = []
    prompt_lower = user_prompt.lower().strip()

    noise_col_words = {
        "where", "filter", "select", "and", "or", "get", "show", "data", "records", 
        "view", "views", "table", "tables", "space", "by", "each", "per", "for", 
        "group", "grouped", "breakdown", "summarize", "top", "bottom", "first", 
        "last", "highest", "lowest", "limit", "me", "the"
    }

    # Equality match
    nl_matches = re.finditer(
        r'(?:(?:where|filter(?:ed)?\s+by|for|of|with|having|on)\s+)?'
        r'(?:the\s+)?'
        r'([a-zA-Z0-9_\s]{2,30}?)\s*'
        r'(?:\b(?:is\s+equal\s+to|equals\s+to|equal\s+to|equals|is|as|must\s+be|should\s+be|named|like|contains)\b|==|=|:=)\s*'
        r'[\'"`]?([a-zA-Z0-9_\-\s]{2,60}?)[\'"`]?'
        r'(?:\s*(?:and\s+|or\s+|order\s+by|group\s+by|limit\s+|sorted\s+|only\b|$))',
        user_prompt,
        re.IGNORECASE
    )
    for nl_m in nl_matches:
        cand_col = nl_m.group(1).strip()
        cand_val = nl_m.group(2).strip()
        if not cand_col or not cand_val:
            continue
        for stop_w in (" and", " or", " where", " with", " by", " each", " per", " only", " data", " please"):
            if cand_val.lower().endswith(stop_w):
                cand_val = cand_val[:-len(stop_w)].strip()

        matched_c = resolve_best_matching_column(cand_col, all_cols)
        if matched_c and matched_c.lower() not in ("view", "table", "space") and cand_val.lower() not in noise_col_words:
            if not any(matched_c.lower() in wc.lower() for wc in where_clauses):
                col_expr = col_where_map.get(matched_c, f'"{matched_c}"') if col_where_map else f'"{matched_c}"'
                clean_str = cand_val.replace("'", "''")
                where_clauses.append(
                    f'(UPPER({col_expr}) = \'{clean_str.upper()}\' '
                    f'OR LOWER({col_expr}) = \'{clean_str.lower()}\' '
                    f'OR {col_expr} LIKE \'%{clean_str}%\' '
                    f'OR {col_expr} = \'{clean_str}\')'
                )
                odata_filters.append(f"{matched_c} eq '{clean_str}'")
                structured_conditions.append({"col": matched_c, "op": "=", "target": clean_str, "is_num": False})

    # Operators
    op_specs = [
        (r'(?:>=|\b(?:greater\s*than\s*or\s*equal\s*(?:to|s)?|greaterthanorequal(?:to|s)?|at\s*least|min\s*(?:of)?|minimum\s*(?:of)?)\b)', '>='),
        (r'(?:<=|\b(?:less\s*than\s*or\s*equal\s*(?:to|s)?|lessthanorequal(?:to|s)?|at\s*most|max\s*(?:of)?|maximum\s*(?:of)?)\b)', '<='),
        (r'(?:!=|<>|\b(?:not\s*equal\s*(?:to|s)?|notequal(?:to|s)?|is\s*not|are\s*not|does\s*not\s*equal|doesn\'t\s*equal)\b)', '!='),
        (r'(?:>|\b(?:greater\s*than|greaterthan|more\s*than|morethan|higher\s*than|higherthan|larger\s*than|largerthan|strictly\s*greater|strictly\s*more|exceeds|exceeding|above|over)\b)', '>'),
        (r'(?:<|\b(?:less\s*than|lessthan|lower\s*than|lowerthan|smaller\s*than|smallerthan|strictly\s*less|below|under)\b)', '<'),
        (r'\b(?:contains|like|includes|starting\s*with|starts\s*with|ending\s*with|ends\s*with)\b', 'LIKE'),
        (r'(?:==|=|:=|\b(?:equals\s*(?:to)?|equal\s*(?:to)?|equalto|is\s*equal\s*to|are\s*equal\s*to|equals|must\s*be|should\s*be|is|are)\b)', '='),
    ]

    comp_regex = re.compile(
        r'(?:(?:where|and|or|,)\s+)?'
        r'(?:(?:the|a|an|that|all|these|those)\s+)*'
        r'[\'"`]?(?P<col>[a-zA-Z0-9_]{2,})[\'"`]?\s*'
        r'(?P<op>'
        r'(?:(?:is|are|was|were)\s+)?'
        r'(?:'
        r'greater\s*than\s*or\s*equal\s*(?:to|s)?|greaterthanorequal(?:to|s)?|at\s*least|min\s*(?:of)?|minimum\s*(?:of)?'
        r'|less\s*than\s*or\s*equal\s*(?:to|s)?|lessthanorequal(?:to|s)?|at\s*most|max\s*(?:of)?|maximum\s*(?:of)?'
        r'|greater\s*than|greaterthan|more\s*than|morethan|higher\s*than|higherthan|larger\s*than|largerthan|strictly\s*greater|strictly\s*more|exceeds|exceeding|above|over'
        r'|less\s*than|lessthan|lower\s*than|lowerthan|smaller\s*than|smallerthan|strictly\s*less|below|under'
        r'|not\s*equal\s*(?:to|s)?|notequal(?:to|s)?|is\s*not|are\s*not|does\s*not\s*equal|doesn\'t\s*equal'
        r'|contains|like|includes'
        r'|equals\s*(?:to)?|equal\s*(?:to)?|equalto|is\s*equal\s*to|are\s*equal\s*to|equals|must\s*be|should\s*be|is|are'
        r'|>=|<=|!=|<>|>|<|==|='
        r')'
        r')\s*'
        r'(?P<val>'
        r'\'[^\']+\'|"[^"]+"|`[^`]+`|[$€₹]?\s*[\d,]+(?:\.\d+)?(?:\s*(?:million|billion|thousand|k|m|b|cr|lakh|crore|usd|eur|inr|days|hours|hrs|mins|percent|%))?|[a-zA-Z0-9_\-]+'
        r')'
        r'(?:\s+only)?',
        re.IGNORECASE
    )

    for m in comp_regex.finditer(user_prompt):
        raw_col = m.group("col").strip().strip("'\"`")
        op_text = m.group("op").strip()
        raw_val = m.group("val").strip().strip("'\"`")
        
        if raw_col.lower() in noise_col_words:
            continue
            
        matched_c = resolve_best_matching_column(raw_col, all_cols)
        if not matched_c or matched_c.lower() in ("view", "table", "space"):
            continue
        if any(matched_c.lower() in wc.lower() for wc in where_clauses):
            continue

        col_expr = col_where_map.get(matched_c, f'"{matched_c}"') if col_where_map else f'"{matched_c}"'
        op = "="
        for pat, op_symbol in op_specs:
            if re.search(pat, op_text, re.IGNORECASE):
                op = op_symbol
                break

        num_clean = re.sub(r'[^\d.\-]', '', raw_val)
        is_num = False
        num_val = None
        if num_clean and not any(c.isalpha() for c in raw_val.replace('$', '').replace('€', '').replace('₹', '').replace('%', '').strip()):
            try:
                num_val = float(num_clean)
                is_num = True
            except ValueError:
                is_num = False

        if is_num and num_val is not None:
            formatted_num = int(num_val) if num_val.is_integer() else num_val
            if op == 'LIKE':
                where_clauses.append(f'LOWER({col_expr}) LIKE \'%{formatted_num}%\'')
            elif op == '=':
                where_clauses.append(f'({col_expr} = {formatted_num} OR {col_expr} = \'{formatted_num}\')')
            else:
                where_clauses.append(f'({col_expr} {op} {formatted_num})')
            
            odata_op = "eq"
            if op == ">=": odata_op = "ge"
            elif op == "<=": odata_op = "le"
            elif op == ">": odata_op = "gt"
            elif op == "<": odata_op = "lt"
            elif op == "!=": odata_op = "ne"
            odata_filters.append(f"{matched_c} {odata_op} {formatted_num}")
            structured_conditions.append({"col": matched_c, "op": op, "target": formatted_num, "is_num": True})
        else:
            clean_str = raw_val.replace("'", "''")
            if op == 'LIKE' or 'contains' in op_text.lower() or 'includes' in op_text.lower():
                where_clauses.append(f'LOWER({col_expr}) LIKE \'%{clean_str.lower()}%\'')
                odata_filters.append(f"contains({matched_c}, '{clean_str}')")
                structured_conditions.append({"col": matched_c, "op": "LIKE", "target": clean_str, "is_num": False})
            elif op == '!=':
                where_clauses.append(f'({col_expr} != \'{clean_str}\' AND {col_expr} IS NOT NULL)')
                odata_filters.append(f"{matched_c} ne '{clean_str}'")
                structured_conditions.append({"col": matched_c, "op": "!=", "target": clean_str, "is_num": False})
            else:
                where_clauses.append(f'(UPPER({col_expr}) = \'{clean_str.upper()}\' OR LOWER({col_expr}) = \'{clean_str.lower()}\' OR {col_expr} = \'{clean_str}\')')
                odata_filters.append(f"{matched_c} eq '{clean_str}'")
                structured_conditions.append({"col": matched_c, "op": "=", "target": clean_str, "is_num": False})

    # Year
    year_match = re.search(r'\b(?:year|fiscalyear|posting_year|fiscal_year)\s*(?:must\s+be|is|=|==|>=|<=|>|<)?\s*(\d{4})\b', prompt_lower) or re.search(r'\b(\d{4})\s+(?:only|data)\b', prompt_lower)
    if year_match and not any("year" in c.lower() for c in where_clauses):
        target_year = year_match.group(1)
        year_col = resolve_best_matching_column("year", all_cols) or resolve_best_matching_column("fiscalyear", all_cols) or resolve_best_matching_column("posting_year", all_cols)
        if year_col:
            col_expr = col_where_map.get(year_col, f'"{year_col}"') if col_where_map else f'"{year_col}"'
            where_clauses.append(f'({col_expr} = \'{target_year}\' OR {col_expr} = {target_year})')
            odata_filters.append(f"{year_col} eq '{target_year}'")
            structured_conditions.append({"col": year_col, "op": "=", "target": target_year, "is_num": False})

    return where_clauses, odata_filters, structured_conditions

def parse_conversational_sql_or_metadata(
    user_prompt: str, 
    view_schemas: Dict[str, List[str]], 
    foreign_keys: List[Dict[str, Any]],
    selected_fields: Optional[List[str]] = None,
    view_params: Optional[Dict[str, List[Dict[str, Any]]]] = None
) -> Dict[str, Any]:
    prompt_lower = user_prompt.lower().strip()
    view_names = list(view_schemas.keys())
    primary_view = view_names[0]
    
    col_select_map: Dict[str, str] = {}
    col_where_map: Dict[str, str] = {}
    all_unique_cols: List[str] = []
    seen_unique_lower = set()
    
    for v_idx, (v_name, cols) in enumerate(view_schemas.items()):
        for c in cols:
            c_clean = str(c).strip()
            if c_clean.lower() not in seen_unique_lower:
                seen_unique_lower.add(c_clean.lower())
                all_unique_cols.append(c_clean)

    for c in all_unique_cols:
        c_lower = c.lower().strip()
        matching_aliases = []
        for idx, (v_name, cols) in enumerate(view_schemas.items()):
            alias = f"t{idx + 1}"
            exact_col = next((col for col in cols if col.lower().strip() == c_lower), None)
            if exact_col:
                matching_aliases.append(f'{alias}."{exact_col}"')
        
        if len(matching_aliases) == 1:
            col_select_map[c] = f'{matching_aliases[0]} AS "{c}"'
            col_where_map[c] = matching_aliases[0]
        elif len(matching_aliases) > 1:
            coalesce_expr = f'COALESCE({", ".join(matching_aliases)})'
            col_select_map[c] = f'{coalesce_expr} AS "{c}"'
            col_where_map[c] = coalesce_expr
        else:
            col_select_map[c] = f'"{c}"'
            col_where_map[c] = f'"{c}"'

    # A value "count under field" is a row predicate, not a SUM of the
    # model's measure and not a grouping request. Resolve every predicate
    # against the selected fact-view catalog before producing SQL.
    count_value_match = re.search(
        r"\b(?:total\s+)?count\s+of\s+['\"]?([^'\"]+?)['\"]?\s+under\s+['\"]?([A-Za-z_][A-Za-z0-9_]*)['\"]?",
        user_prompt,
        re.IGNORECASE,
    )
    if count_value_match and len(view_names) == 1:
        count_value, count_field_text = (part.strip() for part in count_value_match.groups())
        count_field = resolve_best_matching_column(count_field_text, all_unique_cols)
        filters: List[Tuple[str, str, str]] = []
        if count_field:
            filters.append((count_field, "=", count_value))
            condition_pattern = re.compile(
                r"\b(?:for|where)\s+(?:the\s+)?['\"]?([A-Za-z_][A-Za-z0-9_]*)['\"]?\s*(=|is|equals?)\s*(?:['\"]([^'\"]*)['\"]|([A-Za-z0-9_.-]+))",
                re.IGNORECASE,
            )
            for match in condition_pattern.finditer(user_prompt):
                field = resolve_best_matching_column(match.group(1), all_unique_cols)
                value = match.group(3) if match.group(3) is not None else match.group(4)
                if field and field.casefold() != count_field.casefold():
                    filters.append((field, "=", value))
        if count_field and filters:
            conditions = []
            for field, operator, value in filters:
                escaped_value = value.replace("'", "''")
                conditions.append(f't1."{field}" {operator} \'{escaped_value}\'')
            source = f'"{primary_view}" t1' if USE_DIRECT_HANA else "t1"
            sql_query = (
                f'SELECT COUNT(*) OVER () AS "TOTAL_COUNT", t1.* FROM {source} '
                f'WHERE {" AND ".join(conditions)}'
            )
            return {
                "is_table_info": False,
                "sql_query": sql_query,
                "$filter": None,
                "$top": None,
                "group_by": None,
                "target_metric": count_field,
                "structured_conditions": [
                    {"field": field, "operator": operator, "value": value}
                    for field, operator, value in filters
                ],
                "explanation": "Counts fact-view rows matching the requested field value and filters.",
            }

    info_keywords = [
        "describe", "description", "table info", "what columns", "columns description",
        "column description", "schema", "table information", "about the table",
        "tell me about", "what data is this", "summary of table", "fields", "list columns"
    ]
    if any(k in prompt_lower for k in info_keywords) and not any(k in prompt_lower for k in ["where", "filter", "top ", "sum ", "avg ", "join", "greater", "less", "more"]):
        return {
            "is_table_info": True,
            "sql_query": f"SELECT * FROM t1",
            "direct_answer": f"Displaying schema metadata and column descriptions for {', '.join(view_names)}.",
            "explanation": "Generated dataset profile and column descriptions"
        }

    group_by_col = None
    target_metric = None
    limit_n = None
    
    top_dim_match = re.search(
        r'\b(?:top|bottom|first|last|highest|lowest)\s+(?:(\d+)\s+)?([a-zA-Z0-9_\s]+?)\s+(?:based\s+on|by|with|for|ordered\s+by|having|in\s+terms\s+of)\s+([a-zA-Z0-9_\s]+)\b',
        prompt_lower
    )
    if top_dim_match:
        lim_cand = top_dim_match.group(1)
        dim_cand = top_dim_match.group(2).strip()
        metric_cand = top_dim_match.group(3).strip()
        if lim_cand:
            limit_n = int(lim_cand)
        matched_d = resolve_best_matching_column(dim_cand, all_unique_cols)
        if matched_d:
            group_by_col = matched_d
        matched_m = resolve_best_matching_column(metric_cand, all_unique_cols)
        if matched_m:
            target_metric = matched_m

    if not group_by_col:
        top_simple_match = re.search(
            r'\b(?:top|bottom|first|last|highest|lowest)\s+(\d+)\s+([a-zA-Z0-9_]+)\b',
            prompt_lower
        )
        if top_simple_match:
            limit_n = int(top_simple_match.group(1))
            cand_dim = top_simple_match.group(2).strip()
            if cand_dim.lower() not in ("records", "rows", "items", "data", "results", "entries"):
                matched_d = resolve_best_matching_column(cand_dim, all_unique_cols)
                if matched_d:
                    group_by_col = matched_d

    if not group_by_col:
        group_pat = re.search(
            r'\b(?:group(?:\s+the\s+data)?\s+by|grouped\s+by|breakdown\s+by|summarize\s+by|by\s+each|for\s+each(?:\s+distinct)?|for\s+every(?:\s+distinct)?|for\s+all(?:\s+distinct)?|per(?:\s+distinct)?|across\s+each|by|for\s+every|each(?:\s+distinct)?)\s+[\'"`]?([a-zA-Z0-9_]+)[\'"`]?', 
            prompt_lower
        )
        if group_pat:
            cand = group_pat.group(1).strip().strip("'\"`")
            if cand not in ("distinct", "each", "all", "every", "data", "records", "rows", "table", "view", "the"):
                matched_g = resolve_best_matching_column(cand, all_unique_cols)
                if matched_g:
                    group_by_col = matched_g
        elif "customer" in prompt_lower and any(k in prompt_lower for k in ["top ", "purchases", "most", "spend", "orders"]):
            group_by_col = resolve_best_matching_column("CustomerName", all_unique_cols) or resolve_best_matching_column("CustomerNumber", all_unique_cols)
        elif "material" in prompt_lower and any(k in prompt_lower for k in ["distinct", "each", "every", "per", "by material"]):
            group_by_col = resolve_best_matching_column("MaterialNumber", all_unique_cols) or resolve_best_matching_column("MaterialDescription", all_unique_cols)
        elif "company" in prompt_lower or "companycode" in prompt_lower:
            group_by_col = resolve_best_matching_column("CompanyCode", all_unique_cols)

    join_clause = ""
    primary_p = (view_params or {}).get(primary_view, [])
    if primary_p and USE_DIRECT_HANA:
        p_res = resolve_parameter_values(user_prompt, primary_p)
        from_clause = f'FROM {format_hana_view_placeholder(primary_view, p_res)} t1'
    else:
        from_clause = f'FROM "{primary_view}" t1' if USE_DIRECT_HANA else f'FROM t1'

    if len(view_names) > 1:
        join_parts = []
        for idx, sec_view in enumerate(view_names[1:], start=2):
            t_alias = f"t{idx}"
            sec_p = (view_params or {}).get(sec_view, [])
            if sec_p and USE_DIRECT_HANA:
                s_res = resolve_parameter_values(user_prompt, sec_p)
                sec_table_ref = f'{format_hana_view_placeholder(sec_view, s_res)} {t_alias}'
            else:
                sec_table_ref = f'"{sec_view}" {t_alias}' if USE_DIRECT_HANA else f'{t_alias}'
            
            join_cond = None
            if group_by_col:
                c1_match = next((c for c in view_schemas[primary_view] if c.lower() == group_by_col.lower()), None)
                c2_match = next((c for c in view_schemas[sec_view] if c.lower() == group_by_col.lower()), None)
                if c1_match and c2_match:
                    join_cond = f't1."{c1_match}" = {t_alias}."{c2_match}"'

            if not join_cond:
                matched_rel = next((r for r in foreign_keys if (r["view_a"] == primary_view and r["view_b"] == sec_view) or (r["view_b"] == primary_view and r["view_a"] == sec_view)), None)
                if matched_rel and matched_rel["join_keys"]:
                    first_k = matched_rel["join_keys"][0]
                    k1 = first_k["col1"] if matched_rel["view_a"] == primary_view else first_k["col2"]
                    k2 = first_k["col2"] if matched_rel["view_a"] == primary_view else first_k["col1"]
                    join_cond = f't1."{k1}" = {t_alias}."{k2}"'

            if not join_cond:
                common_cols = [c for c in view_schemas[primary_view] if any(c.lower() == sc.lower() for sc in view_schemas[sec_view])]
                if common_cols:
                    join_cond = f't1."{common_cols[0]}" = {t_alias}."{common_cols[0]}"'

            if join_cond:
                join_parts.append(f'LEFT JOIN {sec_table_ref} ON {join_cond}')
            else:
                join_parts.append(f'CROSS JOIN {sec_table_ref}')
        join_clause = " " + " ".join(join_parts)

    where_clauses, odata_filters, structured_conditions = extract_where_conditions_from_prompt(
        user_prompt, 
        all_unique_cols, 
        col_where_map
    )
    if group_by_col:
        where_clauses = [wc for wc in where_clauses if f'"{group_by_col}"' not in wc and group_by_col.lower() not in wc.lower()]

    agg_type = "SUM" if any(k in prompt_lower for k in ["most", "top", "purchases", "spend", "sum", "total", "highest", "revenue", "amount", "each", "per", "by"]) else None
    if not target_metric:
        for c in all_unique_cols:
            c_clean = c.lower().replace("_", "")
            if (c.lower() in prompt_lower or (len(c_clean) >= 4 and c_clean in prompt_lower.replace("_", "").replace(" ", ""))) and c != group_by_col:
                target_metric = c
                break

    if not target_metric:
        for c in all_unique_cols:
            if any(term in c.lower() for term in ["grossrevenue", "netrevenue", "amount", "revenue", "spend", "quantity", "price", "val", "ordercount"]):
                if c != group_by_col:
                    target_metric = c
                    break

    if limit_n is None:
        top_match = re.search(r'\b(?:top|bottom|first|last|highest|lowest|limit)\s+(\d+)\b', prompt_lower)
        if top_match:
            limit_n = int(top_match.group(1))

    is_bottom = bool(re.search(r'\b(?:bottom|lowest|least|last)\b', prompt_lower))
    order_by_clause = ""
    
    if group_by_col:
        sort_metric = target_metric or group_by_col
        sort_expr = f'SUM(CAST({col_where_map.get(sort_metric, f"{sort_metric}")} AS REAL))' if sort_metric != group_by_col else col_where_map.get(sort_metric, f'"{sort_metric}"')
        order_by_clause = f'ORDER BY {sort_expr} {"ASC" if is_bottom else "DESC"}'
    elif limit_n or "most" in prompt_lower or is_bottom:
        sort_metric = target_metric or all_unique_cols[0]
        sort_expr = col_where_map.get(sort_metric, f'"{sort_metric}"')
        order_by_clause = f'ORDER BY {sort_expr} {"ASC" if is_bottom else "DESC"}'

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    limit_sql = f"LIMIT {limit_n}" if limit_n else ""

    if selected_fields and len(selected_fields) > 0 and not group_by_col:
        seen_proj_aliases = set()
        valid_fields = []
        for f in selected_fields:
            matched_f = next((c for c in all_unique_cols if c.lower() == f.lower().strip()), None)
            if matched_f and matched_f.lower() not in seen_proj_aliases:
                seen_proj_aliases.add(matched_f.lower())
                valid_fields.append(col_select_map.get(matched_f, f'"{matched_f}"'))
        select_projection = ", ".join(valid_fields) if valid_fields else "*"
    else:
        select_projection = ", ".join([col_select_map[c] for c in all_unique_cols]) if len(view_names) > 1 else "*"

    if group_by_col:
        gb_expr = col_where_map.get(group_by_col, f'"{group_by_col}"')
        gb_select = col_select_map.get(group_by_col, f'{gb_expr} AS "{group_by_col}"')
        agg_selects = [gb_select]
        seen_agg_cols = {group_by_col.lower()}
        
        if selected_fields and len(selected_fields) > 0:
            for sf in selected_fields:
                matched_f = next((c for c in all_unique_cols if c.lower() == sf.lower().strip()), None)
                if matched_f and matched_f.lower() not in seen_agg_cols:
                    seen_agg_cols.add(matched_f.lower())
                    f_expr = col_where_map.get(matched_f, f'"{matched_f}"')
                    if any(term in matched_f.lower() for term in ["revenue", "amount", "price", "spend", "cost", "margin", "qty", "quantity", "val", "dso", "kpi", "days", "count", "rate", "balance"]):
                        agg_selects.append(f'SUM(CAST({f_expr} AS REAL)) AS "{matched_f}"')
                    else:
                        agg_selects.append(f'MAX({f_expr}) AS "{matched_f}"')
        else:
            if target_metric and target_metric.lower() not in seen_agg_cols:
                tm_expr = col_where_map.get(target_metric, f'"{target_metric}"')
                agg_selects.append(f'SUM(CAST({tm_expr} AS REAL)) AS "{target_metric}"')
                seen_agg_cols.add(target_metric.lower())
            
            if not target_metric:
                num_cols = [c for c in all_unique_cols if c.lower() not in seen_agg_cols and any(p in c.lower() for p in ["spend", "amount", "revenue", "qty", "val", "dso", "kpi", "days"])]
                for nc in num_cols[:2]:
                    nc_expr = col_where_map.get(nc, f'"{nc}"')
                    agg_selects.append(f'SUM(CAST({nc_expr} AS REAL)) AS "{nc}"')
                    seen_agg_cols.add(nc.lower())
        
        agg_selects.append('COUNT(*) AS "RECORD_COUNT"')
        sql_query = f'SELECT {", ".join(agg_selects)} {from_clause}{join_clause} {where_sql} GROUP BY {gb_expr} {order_by_clause} {limit_sql}'.strip()
    elif agg_type and target_metric and not where_clauses and not limit_n and not (selected_fields and len(selected_fields) > 0):
        tm_expr = col_where_map.get(target_metric, f'"{target_metric}"')
        sql_query = f'SELECT {agg_type}(CAST({tm_expr} AS REAL)) AS "{agg_type}_{target_metric}", COUNT(*) AS "RECORD_COUNT" {from_clause}{join_clause} {where_sql}'.strip()
    else:
        sql_query = f'SELECT {select_projection} {from_clause}{join_clause} {where_sql} {order_by_clause} {limit_sql}'.strip()

    odata_filter_str = " and ".join(odata_filters) if odata_filters else None

    return {
        "is_table_info": False,
        "sql_query": sql_query,
        "$filter": odata_filter_str,
        "$top": limit_n,
        "group_by": group_by_col,
        "target_metric": target_metric,
        "structured_conditions": structured_conditions,
        "explanation": f"Compiled Relational SQL: {sql_query}"
    }

