"""
Persistent chat conversation storage.
PostgreSQL is the production source of truth when DATABASE_URL is set.
"""

import os
import re
import uuid
from datetime import datetime
from pathlib import Path

HISTORY_DIR = Path("data/chat_history_store")
_VALID_ID_PATTERN = re.compile(r'^[a-f0-9]{8}-[a-f0-9]{4}$')

# Backend selection
_use_postgres = False
_use_object_storage = False
_use_replit_db = False
_pg_required = bool(os.environ.get("DATABASE_URL"))

# PostgreSQL functions (primary)
try:
    from data.pg_storage import (
        is_available as _pg_available,
        init_tables as _pg_init,
        chat_create_conversation as _pg_chat_create,
        chat_append_message as _pg_chat_append,
        chat_get_conversation as _pg_chat_read,
        chat_replace_messages as _pg_chat_replace,
        chat_delete as _pg_chat_delete,
        chat_list as _pg_chat_list,
    )

    if _pg_available():
        _pg_init()
        _use_postgres = True
        print("[CHAT_HISTORY] Using PostgreSQL conversations/messages tables")
except Exception as e:
    print(f"[CHAT_HISTORY] PostgreSQL unavailable: {e}")

if _pg_required and not _use_postgres:
    print("[CHAT_HISTORY] WARNING: DATABASE_URL is set but PostgreSQL init failed; legacy fallback disabled for production")


def _ensure_postgres_backend() -> bool:
    """Retry PostgreSQL activation if DATABASE_URL is configured."""
    global _use_postgres
    if _use_postgres:
        return True
    if not _pg_required:
        return False
    try:
        if _pg_available():
            _pg_init()
            _use_postgres = True
            print("[CHAT_HISTORY] PostgreSQL backend activated")
            return True
    except Exception as e:
        print(f"[CHAT_HISTORY] PostgreSQL activation failed: {e}")
    return False


def _validate_id(conv_id: str) -> bool:
    return bool(conv_id and _VALID_ID_PATTERN.match(conv_id))


def _fallback_create(first_query: str) -> dict:
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    conv_id = str(uuid.uuid4())[:13]
    now = datetime.now().isoformat()
    title = first_query.strip()[:60]
    if len(first_query.strip()) > 60:
        title += "..."
    conv = {"id": conv_id, "title": title, "created_at": now, "updated_at": now, "messages": []}
    return conv


def create_conversation(first_query: str, session_id: str | None = None) -> dict:
    conv_id = str(uuid.uuid4())[:13]
    now = datetime.now().isoformat()
    title = first_query.strip()[:60]
    if len(first_query.strip()) > 60:
        title += "..."

    conversation = {
        "id": conv_id,
        "title": title,
        "created_at": now,
        "updated_at": now,
        "messages": [],
    }

    if _ensure_postgres_backend():
        _pg_chat_create(conv_id, title=title, session_id=session_id)
        return _pg_chat_read(conv_id) or conversation

    if _pg_required:
        raise RuntimeError("PostgreSQL required for chat persistence but is unavailable")

    return _fallback_create(first_query)


def append_message(
    conv_id: str,
    role: str,
    content: str,
    *,
    message_type: str = "chat",
    structured_payload: dict | None = None,
    preset_key: str | None = None,
    model_used: str | None = None,
) -> bool:
    if not _validate_id(conv_id):
        return False
    if not _ensure_postgres_backend():
        return False
    return _pg_chat_append(
        conv_id=conv_id,
        role=role,
        content=content or "",
        message_type=message_type,
        structured_payload=structured_payload,
        preset_key=preset_key,
        model_used=model_used,
    )


def save_messages(conv_id: str, messages: list):
    if not _validate_id(conv_id):
        return False

    if _ensure_postgres_backend():
        title = None
        if messages and isinstance(messages[0], dict) and messages[0].get("role") == "user":
            base = (messages[0].get("content") or "").strip()
            title = (base[:60] + "...") if len(base) > 60 else base
        return _pg_chat_replace(conv_id, messages=messages or [], title=title)

    return False


def get_conversation(conv_id: str) -> dict | None:
    if not _validate_id(conv_id):
        return None
    if _ensure_postgres_backend():
        return _pg_chat_read(conv_id)
    return None


def list_conversations() -> list:
    if _ensure_postgres_backend():
        return _pg_chat_list()
    return []


def delete_conversation(conv_id: str) -> bool:
    if not _validate_id(conv_id):
        return False
    if _ensure_postgres_backend():
        return _pg_chat_delete(conv_id)
    return False


def migrate_file_history_to_db():
    """Legacy migration no-op for PostgreSQL-only production mode."""
    return
