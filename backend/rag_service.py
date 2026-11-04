"""
RAG (Retrieval-Augmented Generation) Service for SAP Datasphere Analytics.

Provides:
1. Schema & Metadata RAG: Semantic view routing & column discovery across hundreds of views.
2. Golden SQL RAG: Few-shot context retrieval of verified SQL queries and OData patterns.
3. Business Glossary & Metric RAG: Semantic lookup of domain formulas (e.g. ARR, Gross Margin, Churn).
4. Unstructured Context Documents RAG: Domain documentation, policy notes, and catalog definitions.
5. Hybrid Embedding Engine: Gemini Embeddings -> OpenAI Embeddings -> Local Fallback.
"""

import os
import math
import json
import time
import uuid
import sqlite3
import logging
from typing import List, Dict, Any, Optional, Tuple
import httpx

logger = logging.getLogger("datasphere-rag")

RAG_DB_PATH = "file:rag_memdb?mode=memory&cache=shared"
_RAG_BASE_CONN = None

def get_rag_connection() -> sqlite3.Connection:
    global _RAG_BASE_CONN
    if _RAG_BASE_CONN is None:
        _RAG_BASE_CONN = sqlite3.connect(RAG_DB_PATH, uri=True, check_same_thread=False)
    return sqlite3.connect(RAG_DB_PATH, uri=True, check_same_thread=False)

# ---------------------------------------------------------------------------
# Vector Math & Hybrid Embedding Engine
# ---------------------------------------------------------------------------

def cosine_similarity(vec_a: List[float], vec_b: List[float]) -> float:
    """Compute cosine similarity between two vectors."""
    if not vec_a or not vec_b or len(vec_a) != len(vec_b):
        return 0.0
    dot = sum(a * b for a, b in zip(vec_a, vec_b))
    norm_a = math.sqrt(sum(a * a for a in vec_a))
    norm_b = math.sqrt(sum(b * b for b in vec_b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def local_text_embedding(text: str, dim: int = 128) -> List[float]:
    """
    Lightweight zero-dependency deterministic subword-hash embedding.
    Guarantees fast, offline semantic similarity fallback.
    """
    cleaned = text.lower().strip()
    words = cleaned.split()
    vec = [0.0] * dim
    if not words:
        return vec
    
    # 1. Word and char-trigram hash distributions
    for w in words:
        h = hash(w) % dim
        vec[h] += 1.0
        if len(w) >= 3:
            for i in range(len(w) - 2):
                tri = w[i:i+3]
                h_tri = hash(tri) % dim
                vec[h_tri] += 0.5
    
    # Normalize vector to unit length
    norm = math.sqrt(sum(x * x for x in vec))
    if norm > 0:
        vec = [x / norm for x in vec]
    return vec


async def get_text_embedding(text: str, gemini_key: str = "", openai_key: str = "") -> List[float]:
    """
    Generate dense vector embedding using Gemini, OpenAI, or local fallback.
    """
    text = (text or "").strip()
    if not text:
        return [0.0] * 128

    # 1. Try Google Gemini Text Embedding (text-embedding-004)
    if gemini_key:
        try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/text-embedding-004:embedContent?key={gemini_key}"
            payload = {
                "model": "models/text-embedding-004",
                "content": {"parts": [{"text": text[:2000]}]}
            }
            async with httpx.AsyncClient(timeout=4.0) as client:
                res = await client.post(url, json=payload, headers={"Content-Type": "application/json"})
                if res.status_code == 200:
                    data = res.json()
                    values = data.get("embedding", {}).get("values")
                    if values:
                        return values
        except Exception as e:
            logger.debug(f"Gemini embedding fallback: {e}")

    # 2. Try OpenAI Text Embedding (text-embedding-3-small)
    if openai_key:
        try:
            url = "https://api.openai.com/v1/embeddings"
            payload = {
                "model": "text-embedding-3-small",
                "input": text[:2000]
            }
            async with httpx.AsyncClient(timeout=4.0) as client:
                res = await client.post(
                    url,
                    json=payload,
                    headers={"Authorization": f"Bearer {openai_key}", "Content-Type": "application/json"}
                )
                if res.status_code == 200:
                    data = res.json()
                    values = data.get("data", [{}])[0].get("embedding")
                    if values:
                        return values
        except Exception as e:
            logger.debug(f"OpenAI embedding fallback: {e}")

    # 3. Fallback: Local deterministic vector
    return local_text_embedding(text)


# ---------------------------------------------------------------------------
# Database Initialization & Schema
# ---------------------------------------------------------------------------

def init_rag_db():
    """Initialize persistent SQLite vector and knowledge store."""
    try:
        conn = get_rag_connection()
        cursor = conn.cursor()
        
        # 1. Schema / View Metadata Index
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rag_views (
                id TEXT PRIMARY KEY,
                space_id TEXT NOT NULL,
                view_name TEXT NOT NULL,
                description TEXT,
                columns_json TEXT,
                semantic_text TEXT,
                embedding_json TEXT,
                updated_at REAL,
                UNIQUE(space_id, view_name)
            )
        """)
        
        # 2. Golden SQL / Few-Shot Query Index
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rag_golden_sql (
                id TEXT PRIMARY KEY,
                space_id TEXT,
                natural_query TEXT NOT NULL,
                sql_query TEXT NOT NULL,
                target_views_json TEXT,
                odata_filter TEXT,
                explanation TEXT,
                embedding_json TEXT,
                created_at REAL
            )
        """)
        
        # 3. Business Glossary & Formulas Index
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rag_glossary (
                id TEXT PRIMARY KEY,
                term TEXT NOT NULL,
                definition TEXT NOT NULL,
                formula TEXT,
                synonyms TEXT,
                embedding_json TEXT,
                updated_at REAL
            )
        """)
        
        # 4. Contextual Documents Index
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rag_documents (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                category TEXT,
                embedding_json TEXT,
                created_at REAL
            )
        """)
        
        conn.commit()
        conn.close()
        logger.info(f"Initialized RAG Knowledge Store at {RAG_DB_PATH}")
        seed_default_rag_knowledge()
    except Exception as e:
        logger.error(f"Failed to initialize RAG database: {e}")


def seed_default_rag_knowledge():
    """Seed starter golden SQL and business glossary definitions if empty."""
    try:
        conn = get_rag_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM rag_golden_sql")
        count_sql = cursor.fetchone()[0]
        
        if count_sql == 0:
            default_golden = [
                (
                    str(uuid.uuid4()),
                    os.getenv("DATASPHERE_SPACE_ID", os.getenv("DATASPHERE_MODELING_SPACE", "")),
                    "Show total revenue by country sorted descending",
                    'SELECT "COUNTRY", SUM("REVENUE") AS "TOTAL_REVENUE" FROM t1 GROUP BY "COUNTRY" ORDER BY "TOTAL_REVENUE" DESC',
                    json.dumps(["SALES_VIEW"]),
                    None,
                    "Aggregates revenue dimension by country",
                    json.dumps(local_text_embedding("Show total revenue by country sorted descending")),
                    time.time()
                ),
                (
                    str(uuid.uuid4()),
                    os.getenv("DATASPHERE_SPACE_ID", os.getenv("DATASPHERE_MODELING_SPACE", "")),
                    "Find top 10 customers by order value with pending delivery",
                    'SELECT "CUSTOMER_NAME", SUM("NET_AMOUNT") AS "TOTAL_VALUE" FROM t1 WHERE "DELIVERY_STATUS" = \'PENDING\' GROUP BY "CUSTOMER_NAME" ORDER BY "TOTAL_VALUE" DESC LIMIT 10',
                    json.dumps(["ORDERS_VIEW"]),
                    "DELIVERY_STATUS eq 'PENDING'",
                    "Filters pending orders and groups by customer",
                    json.dumps(local_text_embedding("Find top 10 customers by order value with pending delivery")),
                    time.time()
                ),
                (
                    str(uuid.uuid4()),
                    os.getenv("DATASPHERE_SPACE_ID", os.getenv("DATASPHERE_MODELING_SPACE", "")),
                    "Join customer details with order history to calculate average deal size per customer",
                    'SELECT t1."CUSTOMER_ID", t1."CUSTOMER_NAME", COUNT(t2."ORDER_ID") AS "ORDER_COUNT", AVG(t2."AMOUNT") AS "AVG_DEAL_SIZE" FROM t1 INNER JOIN t2 ON t1."CUSTOMER_ID" = t2."CUSTOMER_ID" GROUP BY t1."CUSTOMER_ID", t1."CUSTOMER_NAME" ORDER BY "AVG_DEAL_SIZE" DESC',
                    json.dumps(["CUSTOMERS", "ORDERS"]),
                    None,
                    "Relational inner join between customer master and orders table",
                    json.dumps(local_text_embedding("Join customer details with order history to calculate average deal size")),
                    time.time()
                )
            ]
            cursor.executemany("""
                INSERT INTO rag_golden_sql (id, space_id, natural_query, sql_query, target_views_json, odata_filter, explanation, embedding_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, default_golden)

        cursor.execute("SELECT COUNT(*) FROM rag_glossary")
        count_glossary = cursor.fetchone()[0]
        if count_glossary == 0:
            default_glossary = [
                (
                    str(uuid.uuid4()),
                    "Gross Margin",
                    "Percentage of total sales revenue that company retains after incurring direct costs of producing goods/services.",
                    "(Revenue - COGS) / Revenue * 100",
                    "margin, gross profit margin, gpm",
                    json.dumps(local_text_embedding("Gross Margin margin gross profit margin gpm")),
                    time.time()
                ),
                (
                    str(uuid.uuid4()),
                    "ARR (Annual Recurring Revenue)",
                    "Metric used by subscription businesses representing the predictable and recurring revenue generated per year.",
                    "SUM(Monthly Recurring Revenue) * 12",
                    "annual recurring revenue, arr, annualized run rate",
                    json.dumps(local_text_embedding("ARR Annual Recurring Revenue annual run rate")),
                    time.time()
                ),
                (
                    str(uuid.uuid4()),
                    "Active Account",
                    "Customer account that has generated at least one transaction or logged activity within the last 90 days.",
                    "LAST_ACTIVITY_DATE >= CURRENT_DATE - 90 DAYS",
                    "active customer, engaged user, current client",
                    json.dumps(local_text_embedding("Active Account active customer engaged user")),
                    time.time()
                )
            ]
            cursor.executemany("""
                INSERT INTO rag_glossary (id, term, definition, formula, synonyms, embedding_json, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, default_glossary)

        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"Error seeding RAG knowledge: {e}")


# ---------------------------------------------------------------------------
# View Metadata Indexing & Retrieval (Metadata RAG)
# ---------------------------------------------------------------------------

def get_indexed_view_columns(space_id: str, view_name: str) -> Optional[List[str]]:
    """Retrieve cached schema columns for a space and view from RAG knowledge store."""
    try:
        conn = get_rag_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT columns_json FROM rag_views WHERE space_id = ? AND LOWER(view_name) = ?", (space_id, view_name.lower().strip()))
        row = cursor.fetchone()
        conn.close()
        if row and row[0]:
            cols = json.loads(row[0])
            if cols and isinstance(cols, list):
                return cols
    except Exception:
        pass
    return None

async def index_view_metadata(
    space_id: str,
    view_name: str,
    columns: List[str],
    description: str = "",
    gemini_key: str = "",
    openai_key: str = ""
) -> bool:
    """Index a Datasphere View schema & columns into the RAG vector store."""
    try:
        clean_name = view_name.strip()
        space_id = space_id.strip()
        cols_text = ", ".join(columns) if columns else "No columns defined"
        semantic_text = f"Space: {space_id} | View: {clean_name} | Description: {description} | Columns: {cols_text}"
        
        embedding = await get_text_embedding(semantic_text, gemini_key, openai_key)
        doc_id = f"{space_id}:{clean_name}"
        
        conn = get_rag_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO rag_views (id, space_id, view_name, description, columns_json, semantic_text, embedding_json, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(space_id, view_name) DO UPDATE SET
                description=excluded.description,
                columns_json=excluded.columns_json,
                semantic_text=excluded.semantic_text,
                embedding_json=excluded.embedding_json,
                updated_at=excluded.updated_at
        """, (
            doc_id,
            space_id,
            clean_name,
            description,
            json.dumps(columns),
            semantic_text,
            json.dumps(embedding),
            time.time()
        ))
        conn.commit()
        conn.close()
        logger.info(f"RAG: Indexed View [{clean_name}] in Space [{space_id}] ({len(columns)} cols).")
        return True
    except Exception as e:
        logger.error(f"Failed to index view metadata in RAG: {e}")
        return False


async def retrieve_relevant_views_rag(
    query: str,
    space_id: Optional[str] = None,
    top_k: int = 3,
    gemini_key: str = "",
    openai_key: str = ""
) -> List[Tuple[str, float, List[str]]]:
    """
    Semantically search view schemas and return Top-K matching view names,
    similarity scores, and their column lists.
    """
    query_vec = await get_text_embedding(query, gemini_key, openai_key)
    results: List[Tuple[str, float, List[str]]] = []
    
    try:
        conn = get_rag_connection()
        cursor = conn.cursor()
        if space_id:
            cursor.execute("SELECT view_name, columns_json, embedding_json FROM rag_views WHERE space_id = ?", (space_id,))
        else:
            cursor.execute("SELECT view_name, columns_json, embedding_json FROM rag_views")
        
        rows = cursor.fetchall()
        conn.close()
        
        scored: List[Tuple[str, float, List[str]]] = []
        for v_name, cols_json, emb_json in rows:
            if not emb_json:
                continue
            emb = json.loads(emb_json)
            sim = cosine_similarity(query_vec, emb)
            cols = json.loads(cols_json) if cols_json else []
            scored.append((v_name, sim, cols))
            
        scored.sort(key=lambda x: x[1], reverse=True)
        results = scored[:top_k]
    except Exception as e:
        logger.error(f"RAG view retrieval error: {e}")
        
    return results


# ---------------------------------------------------------------------------
# Golden SQL / Few-Shot Query Retrieval
# ---------------------------------------------------------------------------

async def add_golden_sql_query(
    natural_query: str,
    sql_query: str,
    space_id: str = os.getenv("DATASPHERE_SPACE_ID", os.getenv("DATASPHERE_MODELING_SPACE", "")),
    target_views: Optional[List[str]] = None,
    odata_filter: Optional[str] = None,
    explanation: str = "",
    gemini_key: str = "",
    openai_key: str = ""
) -> str:
    """Save a verified Natural Language -> SQL blueprint into RAG store."""
    query_id = str(uuid.uuid4())
    emb = await get_text_embedding(natural_query, gemini_key, openai_key)
    
    conn = get_rag_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO rag_golden_sql (id, space_id, natural_query, sql_query, target_views_json, odata_filter, explanation, embedding_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        query_id,
        space_id,
        natural_query.strip(),
        sql_query.strip(),
        json.dumps(target_views or []),
        odata_filter,
        explanation,
        json.dumps(emb),
        time.time()
    ))
    conn.commit()
    conn.close()
    return query_id


async def retrieve_golden_sql_examples(
    query: str,
    space_id: Optional[str] = None,
    top_k: int = 2,
    gemini_key: str = "",
    openai_key: str = ""
) -> List[Dict[str, Any]]:
    """Retrieve top few-shot Golden SQL examples matching the user request."""
    query_vec = await get_text_embedding(query, gemini_key, openai_key)
    examples: List[Dict[str, Any]] = []
    
    try:
        conn = get_rag_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id, natural_query, sql_query, target_views_json, odata_filter, explanation, embedding_json FROM rag_golden_sql")
        rows = cursor.fetchall()
        conn.close()
        
        scored = []
        for q_id, nat_q, sql_q, tv_json, filt, exp, emb_json in rows:
            if not emb_json:
                continue
            emb = json.loads(emb_json)
            sim = cosine_similarity(query_vec, emb)
            scored.append({
                "id": q_id,
                "natural_query": nat_q,
                "sql_query": sql_q,
                "target_views": json.loads(tv_json) if tv_json else [],
                "odata_filter": filt,
                "explanation": exp,
                "score": sim
            })
            
        scored.sort(key=lambda x: x["score"], reverse=True)
        # Filter reasonably relevant ones
        examples = [item for item in scored[:top_k] if item["score"] > 0.15]
    except Exception as e:
        logger.error(f"RAG golden SQL retrieval error: {e}")
        
    return examples


# ---------------------------------------------------------------------------
# Business Glossary & Metric Retrieval
# ---------------------------------------------------------------------------

async def add_glossary_term(
    term: str,
    definition: str,
    formula: Optional[str] = None,
    synonyms: Optional[str] = None,
    gemini_key: str = "",
    openai_key: str = ""
) -> str:
    """Add or update a business glossary term."""
    term_id = str(uuid.uuid4())
    searchable_text = f"{term} {definition} {formula or ''} {synonyms or ''}"
    emb = await get_text_embedding(searchable_text, gemini_key, openai_key)
    
    conn = get_rag_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO rag_glossary (id, term, definition, formula, synonyms, embedding_json, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        term_id,
        term.strip(),
        definition.strip(),
        formula.strip() if formula else None,
        synonyms.strip() if synonyms else None,
        json.dumps(emb),
        time.time()
    ))
    conn.commit()
    conn.close()
    return term_id


async def retrieve_glossary_terms(
    query: str,
    top_k: int = 2,
    gemini_key: str = "",
    openai_key: str = ""
) -> List[Dict[str, Any]]:
    """Retrieve relevant business glossary definitions for prompt injection."""
    query_vec = await get_text_embedding(query, gemini_key, openai_key)
    terms: List[Dict[str, Any]] = []
    
    try:
        conn = get_rag_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id, term, definition, formula, synonyms, embedding_json FROM rag_glossary")
        rows = cursor.fetchall()
        conn.close()
        
        scored = []
        for t_id, term, definition, formula, syns, emb_json in rows:
            if not emb_json:
                continue
            emb = json.loads(emb_json)
            sim = cosine_similarity(query_vec, emb)
            scored.append({
                "id": t_id,
                "term": term,
                "definition": definition,
                "formula": formula,
                "synonyms": syns,
                "score": sim
            })
            
        scored.sort(key=lambda x: x["score"], reverse=True)
        terms = [item for item in scored[:top_k] if item["score"] > 0.25]
    except Exception as e:
        logger.error(f"RAG glossary retrieval error: {e}")
        
    return terms


# ---------------------------------------------------------------------------
# Unstructured Documents / Footnotes Retrieval
# ---------------------------------------------------------------------------

async def add_context_document(
    title: str,
    content: str,
    category: str = "general",
    gemini_key: str = "",
    openai_key: str = ""
) -> str:
    """Index an unstructured enterprise context document."""
    doc_id = str(uuid.uuid4())
    searchable = f"{title}\n{content}"
    emb = await get_text_embedding(searchable, gemini_key, openai_key)
    
    conn = get_rag_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO rag_documents (id, title, content, category, embedding_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        doc_id,
        title.strip(),
        content.strip(),
        category.strip(),
        json.dumps(emb),
        time.time()
    ))
    conn.commit()
    conn.close()
    return doc_id


async def retrieve_context_documents(
    query: str,
    top_k: int = 2,
    gemini_key: str = "",
    openai_key: str = ""
) -> List[Dict[str, Any]]:
    """Retrieve unstructured business context documents."""
    query_vec = await get_text_embedding(query, gemini_key, openai_key)
    docs: List[Dict[str, Any]] = []
    
    try:
        conn = get_rag_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id, title, content, category, embedding_json FROM rag_documents")
        rows = cursor.fetchall()
        conn.close()
        
        scored = []
        for d_id, title, content, cat, emb_json in rows:
            if not emb_json:
                continue
            emb = json.loads(emb_json)
            sim = cosine_similarity(query_vec, emb)
            scored.append({
                "id": d_id,
                "title": title,
                "content": content,
                "category": cat,
                "score": sim
            })
            
        scored.sort(key=lambda x: x["score"], reverse=True)
        docs = [d for d in scored[:top_k] if d["score"] > 0.25]
    except Exception as e:
        logger.error(f"RAG document retrieval error: {e}")
        
    return docs


# ---------------------------------------------------------------------------
# RAG Statistics & Health
# ---------------------------------------------------------------------------

def get_rag_stats() -> Dict[str, Any]:
    """Return counts and status of RAG index collections."""
    stats = {
        "status": "ready",
        "database_path": RAG_DB_PATH,
        "indexed_views_count": 0,
        "golden_sql_count": 0,
        "glossary_terms_count": 0,
        "documents_count": 0
    }
    try:
        conn = get_rag_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM rag_views")
        stats["indexed_views_count"] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM rag_golden_sql")
        stats["golden_sql_count"] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM rag_glossary")
        stats["glossary_terms_count"] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM rag_documents")
        stats["documents_count"] = cursor.fetchone()[0]
        
        conn.close()
    except Exception as e:
        stats["status"] = f"error: {e}"
        
    return stats
