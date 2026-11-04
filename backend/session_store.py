"""
In-Memory Volatile Session Store (Zero Database Persistence).
Guarantees zero data leakage and zero persistent database storage.
All sessions live purely in runtime RAM and are never persisted to disk.
"""
import time
from typing import List, Dict, Any
from config import logger

# Pure in-memory storage dictionary
_IN_MEMORY_SESSIONS: Dict[str, Dict[str, Any]] = {}

def init_session_db():
    """Initializes in-memory volatile session store (no disk database)."""
    global _IN_MEMORY_SESSIONS
    _IN_MEMORY_SESSIONS = {}
    logger.info("Initialized In-Memory Volatile Session Store (Zero Database Storage).")

def save_chat_session_to_db(session_id: str, title: str, space_id: str, views: List[str], messages: List[Dict[str, Any]]):
    """Stores session temporarily in volatile RAM only."""
    now = time.time()
    existing = _IN_MEMORY_SESSIONS.get(session_id, {})
    created_at = existing.get("created_at", now)
    _IN_MEMORY_SESSIONS[session_id] = {
        "id": session_id,
        "title": title,
        "space_id": space_id,
        "views": list(views) if views else [],
        "created_at": created_at,
        "updated_at": now,
        "timestamp": time.strftime("%b %d, %Y", time.localtime(now)),
        "messages": list(messages) if messages else []
    }

def get_all_chat_sessions_from_db() -> List[Dict[str, Any]]:
    """Retrieves volatile sessions from memory."""
    sessions = list(_IN_MEMORY_SESSIONS.values())
    sessions.sort(key=lambda s: s.get("updated_at", 0), reverse=True)
    return sessions

def rename_chat_session_in_db(session_id: str, new_title: str):
    """Renames session in memory."""
    if session_id in _IN_MEMORY_SESSIONS:
        _IN_MEMORY_SESSIONS[session_id]["title"] = new_title
        _IN_MEMORY_SESSIONS[session_id]["updated_at"] = time.time()

def delete_chat_session_from_db(session_id: str):
    """Deletes session from memory."""
    _IN_MEMORY_SESSIONS.pop(session_id, None)

def clear_all_chat_sessions_from_db():
    """Clears all sessions from memory."""
    global _IN_MEMORY_SESSIONS
    _IN_MEMORY_SESSIONS = {}
