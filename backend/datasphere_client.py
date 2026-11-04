import re
import time
import json
import asyncio
import xml.etree.ElementTree as ET
from typing import Optional, Dict, Any, List, Tuple
from urllib.parse import quote
from fastapi import HTTPException
import httpx
from config import (
    logger, DATASPHERE_API_URL, DATASPHERE_TOKEN_URL, OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET,
    USE_DIRECT_HANA, GEMINI_API_KEY, OPENAI_API_KEY,
    view_columns_cache, view_working_endpoint_cache, space_views_cache, space_assets_cache, cached_oauth_token
)
import rag_service
from hana_client import probe_hana_columns_sync

def get_tenant_base_host(url: str) -> str:
    match = re.match(r"(https?://[^/]+)", url.strip())
    if match:
        return match.group(1)
    return ""

def resolve_next_link(current_url: str, next_link_str: str) -> str:
    if next_link_str.startswith("http://") or next_link_str.startswith("https://"):
        return next_link_str
    if next_link_str.startswith("/"):
        base_h = get_tenant_base_host(current_url)
        return f"{base_h}{next_link_str}"
    if next_link_str.startswith("?"):
        clean_curr = current_url.split("?")[0]
        return f"{clean_curr}{next_link_str}"
    parent_url = current_url.split("?")[0].rsplit("/", 1)[0]
    return f"{parent_url}/{next_link_str}"

def deduplicate_view_names(views: List[str]) -> List[str]:
    seen = set()
    deduped = []
    for v in views:
        if not v:
            continue
        v_clean = v.strip()
        v_lower = v_clean.lower()
        if v_lower not in seen:
            seen.add(v_lower)
            deduped.append(v_clean)
    return deduped


async def fetch_catalog_asset_details(base_host: str, space_id: str, asset: Dict[str, Any], bearer_token: str) -> Dict[str, Any]:
    """Fetch the tenant's detailed consumption-catalog descriptor for an asset."""
    asset_id = asset.get("id") or asset.get("technicalName") or asset.get("name")
    if not asset_id:
        return asset
    space_key = quote(str(space_id).replace("'", "''"), safe="")
    object_key = quote(str(asset_id).replace("'", "''"), safe="")
    url = f"{base_host}/api/v1/datasphere/consumption/catalog/spaces(%27{space_key}%27)/assets(%27{object_key}%27)"
    headers = {"Authorization": f"Bearer {bearer_token}", "Accept": "application/json", "User-Agent": "Datasphere-Agent/5.0"}
    try:
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
            response = await client.get(url, headers=headers)
            if response.status_code == 200:
                payload = response.json()
                if isinstance(payload, dict):
                    return {**asset, **payload}
    except Exception as exc:
        logger.debug(f"Catalog asset details unavailable for {asset_id}: {exc}")
    return asset

async def get_datasphere_token() -> str:
    """Acquires OAuth 2.0 Bearer Token from SAP BTP."""
    global cached_oauth_token
    now = time.time()
    
    if cached_oauth_token and cached_oauth_token.get("expires_at", 0) > now + 60:
        return cached_oauth_token["access_token"]
    
    if not DATASPHERE_TOKEN_URL or not OAUTH_CLIENT_ID or not OAUTH_CLIENT_SECRET:
        raise HTTPException(
            status_code=500,
            detail="OAuth credentials missing in backend/.env"
        )

    async with httpx.AsyncClient(timeout=15.0) as client:
        # Try 1: HTTP Basic Auth
        try:
            res = await client.post(
                DATASPHERE_TOKEN_URL,
                data={"grant_type": "client_credentials"},
                auth=(OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET),
                headers={"Accept": "application/json"}
            )
            if res.status_code == 200:
                payload = res.json()
                token = payload.get("access_token")
                expires_in = payload.get("expires_in", 3600)
                cached_oauth_token = {"access_token": token, "expires_at": now + expires_in}
                logger.info("Acquired SAP OAuth Bearer token via Basic Auth.")
                return token
        except Exception as e:
            logger.warning(f"Basic auth token attempt: {e}")

        # Try 2: Body Form Credentials
        try:
            res = await client.post(
                DATASPHERE_TOKEN_URL,
                data={
                    "grant_type": "client_credentials",
                    "client_id": OAUTH_CLIENT_ID,
                    "client_secret": OAUTH_CLIENT_SECRET
                },
                headers={"Accept": "application/json"}
            )
            if res.status_code == 200:
                payload = res.json()
                token = payload.get("access_token")
                expires_in = payload.get("expires_in", 3600)
                cached_oauth_token = {"access_token": token, "expires_at": now + expires_in}
                logger.info("Acquired SAP OAuth Bearer token via Body Credentials.")
                return token
            else:
                raise HTTPException(status_code=502, detail=f"SAP OAuth Error (HTTP {res.status_code}): {res.text}")
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Cannot reach SAP Token URL: {str(e)}")

def extract_views_from_xml(xml_text: str) -> List[str]:
    names = set()
    for m in re.finditer(r'<[a-zA-Z0-9_:]*EntitySet\b[^>]*\bName=["\']([a-zA-Z0-9_]+)["\']', xml_text, re.IGNORECASE):
        v = m.group(1).strip()
        if not v.startswith("Edm.") and not v.startswith("SAP__"):
            names.add(v)
    for m in re.finditer(r'<[a-zA-Z0-9_:]*EntityType\b[^>]*\bName=["\']([a-zA-Z0-9_]+)["\']', xml_text, re.IGNORECASE):
        v = m.group(1).strip()
        if v.endswith("Type") and len(v) > 4:
            names.add(v[:-4])
        elif not v.startswith("Edm.") and not v.startswith("SAP__"):
            names.add(v)
    for m in re.finditer(r'<[a-zA-Z0-9_:]*(?:Singleton|FunctionImport|ActionImport|EntitySetMapping)\b[\s\S]*?\b(?:Name|EntitySet)=["\']([a-zA-Z0-9_]+)["\']', xml_text, re.IGNORECASE):
        v = m.group(1).strip()
        if not v.startswith("Edm.") and not v.startswith("SAP__"):
            names.add(v)
    try:
        clean_xml = re.sub(r'\sxmlns(:\w+)?="[^"]+"', '', xml_text, count=0)
        root = ET.fromstring(clean_xml)
        for elem in root.iter():
            tag_name = elem.tag.split("}")[-1].lower()
            if tag_name in ("entityset", "collection", "singleton"):
                n = elem.get("Name") or elem.get("name") or elem.get("href")
                if n and not n.startswith("Edm.") and not n.startswith("SAP__"):
                    names.add(n.strip())
            elif tag_name == "entitytype":
                n = elem.get("Name") or elem.get("name")
                if n and not n.startswith("Edm.") and not n.startswith("SAP__"):
                    if n.endswith("Type") and len(n) > 4:
                        names.add(n[:-4].strip())
                    else:
                        names.add(n.strip())
    except Exception:
        pass
    return list(names)

def parse_edmx_schema(xml_text: str, view_name: str) -> Dict[str, Any]:
    """
    Parses live Datasphere OData EDMX metadata XML to extract actual measures,
    dimensions, and annotated aggregation types with zero hardcoding.
    """
    measures: List[Dict[str, Any]] = []
    dimensions: List[Dict[str, Any]] = []
    all_fields: List[Dict[str, Any]] = []

    try:
        root = ET.fromstring(xml_text)
    except Exception as e:
        logger.warning(f"Failed to parse EDMX XML for {view_name}: {e}")
        return {"measures": [], "dimensions": [], "fields": []}

    # Extract properties annotated as Measures / Aggregations
    measure_names = set()
    key_names = set()
    entity_types = list(root.iter('{http://docs.oasis-open.org/odata/ns/edm}EntityType'))
    matching_types = [
        entity for entity in entity_types
        if entity.attrib.get('Name', '').split('.')[-1].lower() == view_name.split('.')[-1].lower()
    ]
    key_entities = matching_types or (entity_types if len(entity_types) == 1 else [])
    for entity in key_entities:
        for key_ref in entity.findall('{http://docs.oasis-open.org/odata/ns/edm}Key/{http://docs.oasis-open.org/odata/ns/edm}PropertyRef'):
            if key_ref.attrib.get('Name'):
                key_names.add(key_ref.attrib['Name'])
    for annot in root.iter('{http://docs.oasis-open.org/odata/ns/edm}Annotations'):
        target = annot.attrib.get('Target', '')
        for a in annot.findall('{http://docs.oasis-open.org/odata/ns/edm}Annotation'):
            term = a.attrib.get('Term', '')
            if any(k in term for k in ('Measure', 'defaultAggregation', 'Aggregation')):
                measure_names.add(target.split('/')[-1])

    for prop in root.iter('{http://docs.oasis-open.org/odata/ns/edm}Property'):
        p_name = prop.attrib.get('Name')
        p_type = prop.attrib.get('Type', 'Edm.String')
        if not p_name or p_name.startswith('SAP__') or p_name.startswith('@'):
            continue
        
        is_num = any(t in p_type.lower() for t in ('int', 'decimal', 'double', 'single', 'float'))
        is_measure = (p_name in measure_names) or (is_num and any(k in p_name.lower() for k in ('warning', 'critical', 'val', 'amount', 'qty', 'count', 'kpi', 'dso', 'price', 'variance')))
        
        field_item = {
            'name': p_name,
            'label': p_name,
            'type': p_type,
            'category': 'measure' if is_measure else 'dimension',
            'aggregation': 'SUM' if is_measure else None,
            'is_key': p_name in key_names
        }
        all_fields.append(field_item)
        if is_measure:
            measures.append({
                'name': p_name,
                'label': p_name,
                'aggregation': 'SUM',
                'type': p_type
            })
        else:
            dimensions.append({
                'name': p_name,
                'label': p_name,
                'is_key': p_name in key_names,
                'type': p_type
            })

    return {
        'measures': measures,
        'dimensions': dimensions,
        'fields': all_fields
    }

async def fetch_view_edmx_schema(
    base_host: str,
    space_id: str,
    view_name: str,
    bearer_token: str
) -> Dict[str, Any]:
    """
    Dynamically retrieves and parses live EDMX metadata for any view or analytical model in SAP Datasphere.
    """
    asset_key = f"{space_id}/{view_name}".lower()
    asset = space_assets_cache.get(asset_key, {})
    meta_url = (
        asset.get("assetAnalyticalMetadataUrl") or
        asset.get("assetRelationalMetadataUrl") or
        f"{base_host}/api/v1/datasphere/consumption/analytical/{space_id}/{view_name}/$metadata"
    )

    headers = {"Authorization": f"Bearer {bearer_token}", "Accept": "application/xml, application/json"}
    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        try:
            res = await client.get(meta_url, headers=headers)
            if res.status_code == 200 and ("<edmx" in res.text or "<Schema" in res.text):
                return parse_edmx_schema(res.text, view_name)
        except Exception as e:
            logger.warning(f"Live EDMX probe failed for {view_name} at {meta_url}: {e}")

        # Fallback to relational metadata if analytical failed
        if "analytical" in meta_url:
            alt_url = f"{base_host}/api/v1/datasphere/consumption/relational/{space_id}/{view_name}/$metadata"
            try:
                res = await client.get(alt_url, headers=headers)
                if res.status_code == 200 and ("<edmx" in res.text or "<Schema" in res.text):
                    return parse_edmx_schema(res.text, view_name)
            except Exception:
                pass

    return {"measures": [], "dimensions": [], "fields": []}


def extract_views_from_json(json_obj: Any) -> List[str]:
    names = []
    if isinstance(json_obj, dict):
        if "value" in json_obj and isinstance(json_obj["value"], list):
            for item in json_obj["value"]:
                if isinstance(item, dict):
                    n = (item.get("name") or item.get("url") or item.get("title") or 
                         item.get("id") or item.get("technicalName") or item.get("businessName") or
                         item.get("artifactName") or item.get("viewName") or item.get("tableName") or 
                         item.get("entityName") or item.get("assetName") or item.get("displayName") or 
                         item.get("externalName") or item.get("objectName") or item.get("resourceName"))
                    if n and isinstance(n, str) and not n.startswith("Edm.") and not n.startswith("$") and not n.startswith("http"):
                        names.append(n.strip())
                elif isinstance(item, str) and not item.startswith("Edm.") and not item.startswith("$"):
                    names.append(item.strip())
        d_obj = json_obj.get("d")
        if isinstance(d_obj, dict):
            items = d_obj.get("EntitySets", d_obj.get("results", []))
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict):
                        n = (item.get("name") or item.get("url") or item.get("title") or 
                             item.get("technicalName") or item.get("businessName") or item.get("artifactName") or
                             item.get("assetName") or item.get("displayName"))
                        if n and isinstance(n, str) and not n.startswith("Edm."):
                            names.append(n.strip())
                    elif isinstance(item, str) and not item.startswith("Edm."):
                        names.append(item.strip())
        for k, v in json_obj.items():
            k_lower = k.lower()
            if k_lower in ("name", "url", "viewname", "tablename", "entityname", "technicalname", "businessname", "artifactname", "entitysetname", "assetname", "displayname", "externalname", "objectname", "resourcename") and isinstance(v, str):
                if v and not v.startswith("Edm.") and not v.startswith("$") and not v.startswith("http") and len(v) < 100:
                    names.append(v.strip())
            elif isinstance(v, (dict, list)):
                names.extend(extract_views_from_json(v))
    elif isinstance(json_obj, list):
        for item in json_obj:
            names.extend(extract_views_from_json(item))
    return names

def extract_columns_from_metadata_xml(xml_text: str, target_view: str) -> List[str]:
    if not xml_text:
        return []
    cols: List[str] = []
    clean_target = target_view.lower().replace("_", "").replace(" ", "")
    target_tokens = [t for t in re.split(r'[^a-zA-Z0-9]+', target_view.lower()) if len(t) >= 2]

    linked_entity_type = None
    entity_set_match = re.search(
        rf'<[a-zA-Z0-9_:]*EntitySet\b[^>]*\bName=["\']({re.escape(target_view)}|{re.escape(clean_target)})["\'][^>]*\bEntityType=["\']([a-zA-Z0-9_.]+)["\']',
        xml_text,
        re.IGNORECASE
    )
    if entity_set_match:
        linked_entity_type = entity_set_match.group(2).split(".")[-1].lower()

    entity_blocks = re.findall(r'<[a-zA-Z0-9_:]*EntityType\b[\s\S]*?</[a-zA-Z0-9_:]*EntityType>', xml_text, re.IGNORECASE)
    if len(entity_blocks) == 1:
        for pm in re.finditer(r'<[a-zA-Z0-9_:]*Property\b[\s\S]*?\bName=["\']([a-zA-Z0-9_]+)["\']', entity_blocks[0], re.IGNORECASE):
            c = pm.group(1).strip()
            if c and not c.startswith("SAP__") and not c.startswith("@") and not c.startswith("__") and c not in cols:
                cols.append(c)
        if cols:
            return cols

    for block in entity_blocks:
        name_m = re.search(r'\bName=["\']([a-zA-Z0-9_.]+)["\']', block, re.IGNORECASE)
        if name_m:
            raw_e_name = name_m.group(1).lower()
            e_name_clean = raw_e_name.replace("_", "").replace(" ", "")
            is_match = (
                clean_target == e_name_clean or
                clean_target in e_name_clean or 
                e_name_clean in clean_target or
                (linked_entity_type and (linked_entity_type in raw_e_name or raw_e_name in linked_entity_type)) or
                any(t in e_name_clean for t in target_tokens if len(t) >= 3)
            )
            if is_match:
                for pm in re.finditer(r'<[a-zA-Z0-9_:]*Property\b[\s\S]*?\bName=["\']([a-zA-Z0-9_]+)["\']', block, re.IGNORECASE):
                    c = pm.group(1).strip()
                    if c and not c.startswith("SAP__") and not c.startswith("@") and not c.startswith("__") and c not in cols:
                        cols.append(c)
                if cols:
                    return cols
    return cols

def extract_columns_from_json_schema(payload: Any) -> List[str]:
    cols: List[str] = []
    if isinstance(payload, dict):
        for key in ("columns", "fields", "properties", "attributes", "elements", "schema", "items"):
            if key in payload and isinstance(payload[key], list):
                for item in payload[key]:
                    if isinstance(item, dict):
                        c_name = item.get("name") or item.get("columnName") or item.get("fieldName") or item.get("id") or item.get("technicalName") or item.get("businessName")
                        if c_name and isinstance(c_name, str) and not c_name.startswith("@") and not c_name.startswith("__") and c_name not in cols:
                            cols.append(c_name.strip())
                    elif isinstance(item, str) and not item.startswith("@") and item not in cols:
                        cols.append(item.strip())
    return cols

async def probe_view_columns(base_host: str, space_id: str, view_name: str, bearer_token: str) -> List[str]:
    cache_key = f"{space_id}/{view_name}"
    if cache_key in view_columns_cache and view_columns_cache[cache_key]:
        return view_columns_cache[cache_key]

    rag_cols = rag_service.get_indexed_view_columns(space_id, view_name)
    if rag_cols:
        view_columns_cache[cache_key] = rag_cols
        return rag_cols

    # Retrieve cached asset details from consumption catalog
    asset = space_assets_cache.get(cache_key.lower())
    
    headers_json = {"Authorization": f"Bearer {bearer_token}", "Accept": "application/json", "User-Agent": "Datasphere-Agent/5.0"}
    headers_xml = {"Authorization": f"Bearer {bearer_token}", "Accept": "application/xml, text/xml, */*", "User-Agent": "Datasphere-Agent/5.0"}

    async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
        # Case A: If asset is an analytical model, fetch its $metadata XML schema
        an_meta_url = asset.get("assetAnalyticalMetadataUrl") if asset else None
        if not an_meta_url:
            an_meta_url = f"{base_host}/api/v1/datasphere/consumption/analytical/{space_id}/{view_name}/$metadata"
        
        try:
            res_meta = await client.get(an_meta_url, headers=headers_xml)
            if res_meta.status_code == 200 and "<" in res_meta.text:
                cols = extract_columns_from_metadata_xml(res_meta.text, view_name)
                if cols:
                    view_columns_cache[cache_key] = cols
                    if asset and asset.get("assetAnalyticalDataUrl"):
                        view_working_endpoint_cache[cache_key] = f"{asset['assetAnalyticalDataUrl'].rstrip('/')}/{view_name}"
                    asyncio.create_task(rag_service.index_view_metadata(space_id, view_name, cols, gemini_key=GEMINI_API_KEY, openai_key=OPENAI_API_KEY))
                    return cols
        except Exception as e:
            logger.debug(f"Analytical metadata probe notice for {view_name}: {e}")

        # Case B: If asset is a relational view, probe its data endpoint with $top=1
        rel_data_url = (asset.get("assetRelationalDataUrl") if asset else f"{base_host}/api/v1/datasphere/consumption/relational/{space_id}/{view_name}/") or ""
        rel_data_url = rel_data_url.rstrip("/")
        candidate_rel_endpoints = [
            f"{rel_data_url}/{view_name}?$top=1",
            f"{rel_data_url}/?$top=1",
            f"{base_host}/api/v1/datasphere/consumption/relational/{space_id}/{view_name}/{view_name}?$top=1",
            f"{base_host}/api/v1/datasphere/consumption/relational/{space_id}/{view_name}?$top=1"
        ]
        for u in candidate_rel_endpoints:
            try:
                res = await client.get(u, headers=headers_json)
                if res.status_code == 200:
                    payload = res.json()
                    val = payload.get("value", payload.get("d", {}).get("results", []))
                    if isinstance(val, list) and len(val) > 0 and isinstance(val[0], dict):
                        cols = [k for k in val[0].keys() if not k.startswith("@") and not k.startswith("__")]
                        if cols:
                            view_columns_cache[cache_key] = cols
                            view_working_endpoint_cache[cache_key] = u.split("?")[0]
                            asyncio.create_task(rag_service.index_view_metadata(space_id, view_name, cols, gemini_key=GEMINI_API_KEY, openai_key=OPENAI_API_KEY))
                            return cols
            except Exception:
                pass

        # Case C: Relational $metadata XML probe
        rel_meta_url = asset.get("assetRelationalMetadataUrl") if asset else None
        if not rel_meta_url:
            rel_meta_url = f"{base_host}/api/v1/datasphere/consumption/relational/{space_id}/{view_name}/$metadata"
        try:
            res_rm = await client.get(rel_meta_url, headers=headers_xml)
            if res_rm.status_code == 200 and "<" in res_rm.text:
                cols = extract_columns_from_metadata_xml(res_rm.text, view_name)
                if cols:
                    view_columns_cache[cache_key] = cols
                    asyncio.create_task(rag_service.index_view_metadata(space_id, view_name, cols, gemini_key=GEMINI_API_KEY, openai_key=OPENAI_API_KEY))
                    return cols
        except Exception:
            pass

    return []

async def discover_space_views(base_host: str, space_id: str, bearer_token: str, search_hint: Optional[str] = None) -> List[str]:
    """
    Extracts views and analytical models from the SAP Datasphere Consumption Catalog
    that are exposed for consumption ONLY, using credentials from .env.
    """
    common_hdrs = {
        "Authorization": f"Bearer {bearer_token}",
        "Accept": "application/json",
        "User-Agent": "Datasphere-Agent/5.0"
    }

    discovered: List[str] = []
    
    catalog_url_filtered = f"{base_host}/api/v1/datasphere/consumption/catalog/assets?$filter=spaceName eq '{space_id}'"
    catalog_url_all = f"{base_host}/api/v1/datasphere/consumption/catalog/assets"

    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        # Step 1: Filtered catalog query
        try:
            res = await client.get(catalog_url_filtered, headers=common_hdrs)
            if res.status_code == 200:
                payload = res.json()
                items = payload.get("value", [])
                for it in items:
                    # Filter for assets exposed for consumption only
                    is_exposed = bool(
                        it.get("assetRelationalDataUrl") or 
                        it.get("assetAnalyticalDataUrl") or 
                        it.get("supportsAnalyticalQueries") is True or
                        it.get("assetRelationalMetadataUrl") or 
                        it.get("assetAnalyticalMetadataUrl")
                    )
                    if is_exposed:
                        name = (it.get("name") or it.get("label") or "").strip()
                        if name:
                            discovered.append(name)
                            key = f"{space_id}/{name}".lower()
                            space_assets_cache[key] = it
                            if it.get("assetRelationalDataUrl"):
                                view_working_endpoint_cache[f"{space_id}/{name}"] = f"{it['assetRelationalDataUrl'].rstrip('/')}/{name}"
                            elif it.get("assetAnalyticalDataUrl"):
                                view_working_endpoint_cache[f"{space_id}/{name}"] = f"{it['assetAnalyticalDataUrl'].rstrip('/')}/{name}"
        except Exception as e:
            logger.warning(f"Filtered catalog query notice for {space_id}: {e}")

        # Step 2: Fallback to full catalog and filter by spaceName
        if not discovered:
            try:
                res = await client.get(catalog_url_all, headers=common_hdrs)
                if res.status_code == 200:
                    payload = res.json()
                    items = payload.get("value", [])
                    target_space_clean = space_id.strip().upper()
                    for it in items:
                        item_space = str(it.get("spaceName") or "").strip().upper()
                        if item_space == target_space_clean:
                            is_exposed = bool(
                                it.get("assetRelationalDataUrl") or 
                                it.get("assetAnalyticalDataUrl") or 
                                it.get("supportsAnalyticalQueries") is True or
                                it.get("assetRelationalMetadataUrl") or 
                                it.get("assetAnalyticalMetadataUrl")
                            )
                            if is_exposed:
                                name = (it.get("name") or it.get("label") or "").strip()
                                if name:
                                    discovered.append(name)
                                    key = f"{space_id}/{name}".lower()
                                    space_assets_cache[key] = it
                                    if it.get("assetRelationalDataUrl"):
                                        view_working_endpoint_cache[f"{space_id}/{name}"] = f"{it['assetRelationalDataUrl'].rstrip('/')}/{name}"
                                    elif it.get("assetAnalyticalDataUrl"):
                                        view_working_endpoint_cache[f"{space_id}/{name}"] = f"{it['assetAnalyticalDataUrl'].rstrip('/')}/{name}"
            except Exception as e:
                logger.warning(f"Full catalog fallback notice for {space_id}: {e}")

    merged = deduplicate_view_names(discovered)
    if merged:
        space_views_cache[space_id] = merged
        logger.info(f"Successfully extracted {len(merged)} consumption-exposed views from Datasphere space [{space_id}]")
    return merged

def find_views_matching_pattern(user_prompt: str, candidate_views: List[str]) -> List[str]:
    if not candidate_views:
        return []
    prompt_lower = user_prompt.lower().strip()
    matched_views: List[str] = []

    view_pattern_regex = [
        r'view\s+name\s+(?:has|contains|like|is|matching|with|having)\s+["\']?([^"\',.?;]+)["\']?',
        r'views\s+(?:have|contain|like|matching|with|having)\s+["\']?([^"\',.?;]+)["\']?',
        r'related\s+to\s+["\']?([^"\',.?;]+)["\']?',
        r'regarding\s+["\']?([^"\',.?;]+)["\']?',
        r'about\s+["\']?([^"\',.?;]+)["\']?',
        r'views?\s+named\s+["\']?([^"\',.?;]+)["\']?'
    ]

    target_phrases: List[str] = []
    for pat in view_pattern_regex:
        for m in re.finditer(pat, prompt_lower):
            raw_phrase = m.group(1).strip()
            cleaned_phrase = re.sub(r'\b(view|views|table|tables|data|records|dataset|space|please|get|me|the)\b', '', raw_phrase).strip()
            if cleaned_phrase and len(cleaned_phrase) >= 2:
                target_phrases.append(cleaned_phrase)

    for phrase in target_phrases:
        phrase_clean = phrase.replace("_", "").replace(" ", "").lower()
        phrase_tokens = [t for t in re.findall(r'[a-zA-Z0-9]{2,}', phrase) if t not in ("the", "and", "or", "for", "with", "get", "data", "where")]
        for v in candidate_views:
            v_clean = v.lower().replace("_", "").replace(" ", "")
            if phrase_clean and (phrase_clean in v_clean or v_clean in phrase_clean):
                matched_views.append(v)
            elif phrase_tokens and all(tok in v_clean for tok in phrase_tokens):
                matched_views.append(v)

    matched_views = deduplicate_view_names(matched_views)
    if matched_views:
        return matched_views

    prompt_tokens = set(re.findall(r'\b[a-zA-Z0-9_]{3,}\b', prompt_lower))
    stop_words = {"get", "the", "data", "where", "view", "views", "name", "has", "contains", "like", "with", "from", "show", "all", "select", "records"}
    salient_tokens = [t for t in prompt_tokens if t not in stop_words]

    for v in candidate_views:
        v_clean = v.lower().replace("_", "")
        if any(tok in v_clean or v_clean in tok for tok in salient_tokens):
            matched_views.append(v)

    return deduplicate_view_names(matched_views)

def extract_view_names_from_prompt(user_prompt: str) -> List[str]:
    prompt_lower = user_prompt.lower().strip()
    extracted: List[str] = []
    view_pattern_regex = [
        r'view\s+name\s+(?:is|=|==)\s+["\']?([a-zA-Z0-9_]+)["\']?',
        r'from\s+(?:the\s+)?view\s+["\']?([a-zA-Z0-9_]+)["\']?',
        r'of\s+(?:the\s+)?view\s+["\']?([a-zA-Z0-9_]+)["\']?',
        r'in\s+(?:the\s+)?view\s+["\']?([a-zA-Z0-9_]+)["\']?',
        r'table\s+["\']?([a-zA-Z0-9_]+)["\']?'
    ]
    for pat in view_pattern_regex:
        for m in re.finditer(pat, prompt_lower):
            v_cand = m.group(1).strip()
            if v_cand and v_cand not in ("has", "contains", "like", "with", "where", "data", "records", "the", "all", "is", "for", "from", "show", "get"):
                orig_m = re.search(re.escape(v_cand), user_prompt, re.IGNORECASE)
                val_to_add = orig_m.group(0) if orig_m else v_cand
                if val_to_add not in extracted:
                    extracted.append(val_to_add)
    return extracted

def has_explicit_view_name_intent(prompt: str) -> bool:
    prompt_lower = prompt.lower().strip()
    return bool(re.search(
        r'\b(?:view\s+name|views\s+named|views\s+have|view\s+has|view\s+contains|view\s+like|from\s+view|view\s+[a-zA-Z0-9_]{2,})\b',
        prompt_lower
    ))

async def route_prompt_to_views(
    user_prompt: str, 
    space_id: str, 
    candidate_views: List[str], 
    base_host: str, 
    bearer_token: str
) -> List[str]:
    if not candidate_views:
        return []

    name_matched_views = find_views_matching_pattern(user_prompt, candidate_views)
    if name_matched_views:
        return name_matched_views

    prompt_lower = user_prompt.lower()
    probe_tasks = [probe_view_columns(base_host, space_id, v, bearer_token) for v in candidate_views]
    probe_results = await asyncio.gather(*probe_tasks)
    view_scores: List[Tuple[str, int, List[str]]] = []

    stop_words = {"get", "the", "data", "where", "view", "views", "name", "has", "contains", "like", "with", "from", "show", "all", "select"}
    raw_tokens = set(re.findall(r'\b[a-zA-Z0-9_]{3,}\b', prompt_lower)) - stop_words
    prompt_tokens = set(raw_tokens)

    for idx, v in enumerate(candidate_views):
        cols = probe_results[idx] or []
        score = 0
        matched_cols = []
        v_clean = v.lower().replace("_", "")
        for t in prompt_tokens:
            t_clean = t.replace("_", "")
            if t_clean in v_clean or v_clean in t_clean:
                score += 8
        for c in cols:
            c_clean = c.lower().replace("_", "")
            for t in prompt_tokens:
                t_clean = t.replace("_", "")
                if t_clean in c_clean or c_clean in t_clean:
                    score += 4
                    matched_cols.append(c)
        view_scores.append((v, score, matched_cols))

    view_scores.sort(key=lambda x: x[1], reverse=True)
    if view_scores and view_scores[0][1] >= 2:
        top_score = view_scores[0][1]
        top_views = [x[0] for x in view_scores if x[1] >= top_score - 2 and x[1] >= 2]
        return top_views if top_views else [view_scores[0][0]]

    return candidate_views[:1] if candidate_views else []

async def fetch_single_view_data(
    view_name: str,
    space_id: str,
    bearer_token: str,
    base_host: str,
    params: Dict[str, str],
    user_email: Optional[str] = None,
    max_pages: Optional[int] = None
) -> Dict[str, Any]:
    raw_endpoints = [
        f"{base_host}/api/v1/datasphere/consumption/relational/{space_id}/{view_name}/{view_name}",
        f"{base_host}/api/v1/datasphere/consumption/relational/{space_id}/{view_name}",
        f"{base_host}/api/v1/datasphere/consumption/analytical/{space_id}/{view_name}/{view_name}",
        f"{base_host}/api/v1/datasphere/consumption/analytical/{space_id}/{view_name}"
    ]

    cached_url = view_working_endpoint_cache.get(f"{space_id}/{view_name}")
    candidate_endpoints = [cached_url] if cached_url else []
    for ep in raw_endpoints:
        if ep not in candidate_endpoints:
            candidate_endpoints.append(ep)

    headers = {"Authorization": f"Bearer {bearer_token}", "Accept": "application/json", "User-Agent": "Datasphere-Agent/5.0"}
    raw_data = []
    executed_url = ""
    error_logs = []
    success = False

    async with httpx.AsyncClient(timeout=45.0, follow_redirects=True) as client:
        for ep in candidate_endpoints:
            try:
                page_url = ep
                page_params = params
                page_count = 0
                candidate_data = []
                expected_count = None
                while page_url and (max_pages is None or page_count < max_pages):
                    res = await client.get(page_url, headers=headers, params=page_params)
                    if res.status_code == 400 and page_params and "$count" in page_params:
                        # Analytical consumption endpoints can reject $count even
                        # when the same request without it is valid. Pagination
                        # still follows nextLink when the service provides one.
                        page_params = {key: value for key, value in page_params.items() if key != "$count"}
                        res = await client.get(page_url, headers=headers, params=page_params)
                    if res.status_code != 200:
                        break
                    payload = res.json()
                    data_obj = payload.get("d", {}) if isinstance(payload, dict) else {}
                    count_value = payload.get("@odata.count") or data_obj.get("__count") if isinstance(payload, dict) else None
                    if count_value is not None:
                        try:
                            expected_count = int(count_value)
                        except (TypeError, ValueError):
                            pass
                    val = payload.get("value", data_obj.get("results", [])) if isinstance(payload, dict) else []
                    if not isinstance(val, list):
                        break
                    candidate_data.extend(val)
                    page_count += 1
                    next_link = (payload.get("@odata.nextLink") or payload.get("odata.nextLink") or data_obj.get("__next")) if isinstance(payload, dict) else None
                    if not next_link:
                        if expected_count is not None and len(candidate_data) < expected_count:
                            previous_count = len(candidate_data)
                            page_url = ep
                            page_params = dict(params)
                            page_params["$skip"] = str(previous_count)
                            if previous_count == 0 or not val:
                                error_logs.append(f"Pagination ended at {previous_count} of {expected_count} rows for {ep}")
                                page_url = None
                        else:
                            page_url = None
                    else:
                        page_url = resolve_next_link(page_url, str(next_link))
                        page_params = None
                else:
                    # Explicit max_pages values are retained for legacy callers;
                    # validation passes None so it always follows every page.
                    if page_url:
                        error_logs.append(f"Pagination stopped after {page_count} pages for {ep}")
                complete = expected_count is None or len(candidate_data) >= expected_count
                if page_count and not page_url and complete:
                    raw_data = candidate_data
                    success = True
                    executed_url = ep
                    view_working_endpoint_cache[f"{space_id}/{view_name}"] = executed_url
                    break
                if page_count and not complete:
                    error_logs.append(f"Incomplete result: received {len(candidate_data)} of {expected_count} rows for {ep}")
            except Exception as e:
                error_logs.append(str(e))

    return {
        "view_name": view_name,
        "success": success,
        "data": raw_data,
        "executed_url": executed_url,
        "expected_count": expected_count if 'expected_count' in locals() else None,
        "error_logs": error_logs
    }

