"""
Persistent chat conversation storage.
Primary: Replit DB (persists across deploys).
Fallback: JSON files (for local dev outside Replit).
"""

import json
import os
import re
import uuid
from datetime import datetime
from pathlib import Path

HISTORY_DIR = Path("data/chat_history_store")
_VALID_ID_PATTERN = re.compile(r'^[a-f0-9]{8}-[a-f0-9]{3}$')

# ── Storage backend detection ────────────────────────────────
_use_replit_db = False
_replit_db = None

try:
    if os.environ.get("REPLIT_DB_URL"):
        from replit import db as _replit_db
        _use_replit_db = True
        print("[CHAT_HISTORY] Using Replit DB for chat history (persistent)")
except Exception as e:
    print(f"[CHAT_HISTORY] Replit DB unavailable ({e}), falling back to JSON files")


def _validate_id(conv_id: str) -> bool:
    if not conv_id or not _VALID_ID_PATTERN.match(conv_id):
        return False
    return True


# ── Replit DB helpers ────────────────────────────────────────

def _db_conv_key(conv_id: str) -> str:
    return f"chat:{conv_id}"

DB_INDEX_KEY = "chat:__index__"


def _db_read_conv(conv_id: str) -> dict | None:
    try:
        raw = _replit_db.get(_db_conv_key(conv_id))
        if raw is None:
            return None
        if isinstance(raw, str):
            return json.loads(raw)
        return json.loads(json.dumps(raw, default=str))
    except Exception as e:
        print(f"[CHAT_HISTORY] DB read error for {conv_id}: {e}")
        return None


def _db_write_conv(conv_id: str, data: dict):
    try:
        _replit_db[_db_conv_key(conv_id)] = json.loads(json.dumps(data, default=str))
    except Exception as e:
        print(f"[CHAT_HISTORY] DB write error for {conv_id}: {e}")


def _db_delete_conv(conv_id: str):
    try:
        key = _db_conv_key(conv_id)
        if key in _replit_db:
            del _replit_db[key]
    except Exception as e:
        print(f"[CHAT_HISTORY] DB delete error for {conv_id}: {e}")


def _db_read_index() -> list:
    """Read the conversation index (list of {id, title, created_at, updated_at, message_count})."""
    try:
        raw = _replit_db.get(DB_INDEX_KEY)
        if raw is None:
            return []
        if isinstance(raw, str):
            return json.loads(raw)
        return json.loads(json.dumps(raw, default=str))
    except Exception as e:
        print(f"[CHAT_HISTORY] DB index read error: {e}")
        return []


def _db_write_index(index: list):
    try:
        _replit_db[DB_INDEX_KEY] = json.loads(json.dumps(index, default=str))
    except Exception as e:
        print(f"[CHAT_HISTORY] DB index write error: {e}")


def _db_update_index(conv: dict):
    """Upsert a conversation's metadata into the index."""
    index = _db_read_index()
    entry = {
        "id": conv["id"],
        "title": conv.get("title", ""),
        "created_at": conv.get("created_at", ""),
        "updated_at": conv.get("updated_at", ""),
        "message_count": len(conv.get("messages", [])),
    }
    # Replace existing or append
    index = [e for e in index if e.get("id") != conv["id"]]
    index.insert(0, entry)
    _db_write_index(index)


def _db_remove_from_index(conv_id: str):
    index = _db_read_index()
    index = [e for e in index if e.get("id") != conv_id]
    _db_write_index(index)


# ── JSON file helpers (fallback) ─────────────────────────────

def _ensure_dir():
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)


# ── Public API ───────────────────────────────────────────────

def create_conversation(first_query: str) -> dict:
    conv_id = str(uuid.uuid4())[:12]
    now = datetime.now()

    title = first_query.strip()[:60]
    if len(first_query.strip()) > 60:
        title += "..."

    conversation = {
        "id": conv_id,
        "title": title,
        "created_at": now.isoformat(),
        "updated_at": now.isoformat(),
        "messages": [],
    }

    if _use_replit_db:
        _db_write_conv(conv_id, conversation)
        _db_update_index(conversation)
    else:
        _ensure_dir()
        filepath = HISTORY_DIR / f"{conv_id}.json"
        with open(filepath, "w") as f:
            json.dump(conversation, f)

    return conversation


def save_messages(conv_id: str, messages: list):
    if not _validate_id(conv_id):
        return False

    if _use_replit_db:
        conversation = _db_read_conv(conv_id)
        if conversation is None:
            return False

        conversation["messages"] = messages
        conversation["updated_at"] = datetime.now().isoformat()

        if messages and messages[0].get("role") == "user":
            title = messages[0]["content"].strip()[:60]
            if len(messages[0]["content"].strip()) > 60:
                title += "..."
            conversation["title"] = title

        _db_write_conv(conv_id, conversation)
        _db_update_index(conversation)
        return True
    else:
        _ensure_dir()
        filepath = HISTORY_DIR / f"{conv_id}.json"
        if not filepath.exists():
            return False
        try:
            with open(filepath, "r") as f:
                conversation = json.load(f)
            conversation["messages"] = messages
            conversation["updated_at"] = datetime.now().isoformat()
            if messages and messages[0].get("role") == "user":
                title = messages[0]["content"].strip()[:60]
                if len(messages[0]["content"].strip()) > 60:
                    title += "..."
                conversation["title"] = title
            with open(filepath, "w") as f:
                json.dump(conversation, f)
            return True
        except Exception as e:
            print(f"[CHAT_HISTORY] Error saving: {e}")
            return False


def get_conversation(conv_id: str) -> dict:
    if not _validate_id(conv_id):
        return None

    if _use_replit_db:
        return _db_read_conv(conv_id)
    else:
        _ensure_dir()
        filepath = HISTORY_DIR / f"{conv_id}.json"
        if not filepath.exists():
            return None
        try:
            with open(filepath, "r") as f:
                return json.load(f)
        except Exception:
            return None


def list_conversations() -> list:
    if _use_replit_db:
        index = _db_read_index()
        index.sort(key=lambda x: x.get("updated_at", ""), reverse=True)
        return index
    else:
        _ensure_dir()
        conversations = []
        for f in HISTORY_DIR.glob("*.json"):
            try:
                with open(f, "r") as fh:
                    conv = json.load(fh)
                conversations.append({
                    "id": conv["id"],
                    "title": conv["title"],
                    "created_at": conv["created_at"],
                    "updated_at": conv["updated_at"],
                    "message_count": len(conv.get("messages", [])),
                })
            except Exception:
                pass
        conversations.sort(key=lambda x: x["updated_at"], reverse=True)
        return conversations


def delete_conversation(conv_id: str) -> bool:
    if not _validate_id(conv_id):
        return False

    if _use_replit_db:
        existing = _db_read_conv(conv_id)
        if existing is None:
            return False
        _db_delete_conv(conv_id)
        _db_remove_from_index(conv_id)
        return True
    else:
        filepath = HISTORY_DIR / f"{conv_id}.json"
        if filepath.exists():
            filepath.unlink()
            return True
        return False


def migrate_file_history_to_db():
    """
    One-time migration: move any existing JSON file conversations into Replit DB.
    Safe to call multiple times (skips already-migrated conversations).
    """
    if not _use_replit_db:
        return

    if not HISTORY_DIR.exists():
        return

    migrated = 0
    for f in HISTORY_DIR.glob("*.json"):
        try:
            with open(f, "r") as fh:
                conv = json.load(fh)
            conv_id = conv.get("id")
            if not conv_id:
                continue
            # Skip if already in DB
            if _db_read_conv(conv_id) is not None:
                continue
            _db_write_conv(conv_id, conv)
            _db_update_index(conv)
            migrated += 1
        except Exception as e:
            print(f"[CHAT_HISTORY] Migration error for {f}: {e}")

    if migrated:
        print(f"[CHAT_HISTORY] Migrated {migrated} conversations from files -> Replit DB")
