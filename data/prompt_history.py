"""
Persistent storage for prompt response history.
Stores responses grouped by category and prompt type (intent).

Primary: Replit DB (persists across deploys).
Fallback: JSON files (for local dev outside Replit).
"""

import json
import os
import time
from pathlib import Path
from threading import Lock

MAX_PER_INTENT = 100
_locks: dict = {}

# ── Storage backend detection ────────────────────────────────
_use_replit_db = False
_replit_db = None

try:
    if os.environ.get("REPLIT_DB_URL"):
        from replit import db as _replit_db
        _use_replit_db = True
        print("[HISTORY] Using Replit DB for prompt history (persistent)")
except Exception as e:
    print(f"[HISTORY] Replit DB unavailable ({e}), falling back to JSON files")


def _get_lock(user_id: str) -> Lock:
    if user_id not in _locks:
        _locks[user_id] = Lock()
    return _locks[user_id]


# ── Replit DB helpers ────────────────────────────────────────

def _db_key(user_id: str) -> str:
    return f"ph:{user_id}"


def _db_read(user_id: str) -> dict:
    try:
        raw = _replit_db.get(_db_key(user_id))
        if raw is None:
            return {}
        if isinstance(raw, str):
            return json.loads(raw)
        # replit db may return ObservedDict — convert to plain dict
        return json.loads(json.dumps(raw, default=str))
    except Exception as e:
        print(f"[HISTORY] Replit DB read error for {user_id}: {e}")
        return {}


def _db_write(user_id: str, data: dict):
    try:
        _replit_db[_db_key(user_id)] = json.loads(json.dumps(data, default=str))
    except Exception as e:
        print(f"[HISTORY] Replit DB write error for {user_id}: {e}")


# ── JSON file helpers (fallback) ─────────────────────────────

def _history_file(user_id: str) -> Path:
    return Path(f"data/prompt_history_{user_id}.json")


def _ensure_file(user_id: str):
    path = _history_file(user_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with open(path, "w") as f:
            json.dump({}, f)


def _file_read(user_id: str) -> dict:
    _ensure_file(user_id)
    try:
        with open(_history_file(user_id), "r") as f:
            return json.load(f)
    except Exception:
        return {}


def _file_write(user_id: str, data: dict):
    _ensure_file(user_id)
    with open(_history_file(user_id), "w") as f:
        json.dump(data, f, separators=(",", ":"))


# ── Unified read/write (picks backend automatically) ─────────

def _read(user_id: str) -> dict:
    if _use_replit_db:
        return _db_read(user_id)
    return _file_read(user_id)


def _write(user_id: str, data: dict):
    if _use_replit_db:
        _db_write(user_id, data)
    else:
        _file_write(user_id, data)


# ── Public API (unchanged signatures) ───────────────────────

def save_response(category: str, intent: str, content: str, display_type: str | None = None, user_id: str = "default", model_used: str | None = None, query: str | None = None) -> dict:
    """Save a prompt response. Returns the created entry."""
    entry = {
        "id": str(int(time.time() * 1000)),
        "timestamp": time.time(),
        "content": content,
        "display_type": display_type,
    }
    if model_used:
        entry["model_used"] = model_used
    if query:
        entry["query"] = query[:200]
    with _get_lock(user_id):
        data = _read(user_id)
        key = f"{category}::{intent}"
        if key not in data:
            data[key] = {"category": category, "intent": intent, "entries": []}
        entries = data[key]["entries"]
        entries.insert(0, entry)
        if len(entries) > MAX_PER_INTENT:
            data[key]["entries"] = entries[:MAX_PER_INTENT]
        _write(user_id, data)
    return entry


def get_all(user_id: str = "default") -> dict:
    """Return all history grouped by category::intent."""
    return _read(user_id)


def get_by_intent(category: str, intent: str, user_id: str = "default") -> list:
    """Return entries for a specific intent."""
    data = _read(user_id)
    key = f"{category}::{intent}"
    bucket = data.get(key, {})
    return bucket.get("entries", [])


def delete_entry(category: str, intent: str, entry_id: str, user_id: str = "default") -> bool:
    """Delete a single history entry."""
    with _get_lock(user_id):
        data = _read(user_id)
        key = f"{category}::{intent}"
        if key not in data:
            return False
        before = len(data[key]["entries"])
        data[key]["entries"] = [e for e in data[key]["entries"] if e["id"] != entry_id]
        if len(data[key]["entries"]) == before:
            return False
        _write(user_id, data)
    return True


def clear_intent(category: str, intent: str, user_id: str = "default") -> bool:
    """Clear all entries for an intent."""
    with _get_lock(user_id):
        data = _read(user_id)
        key = f"{category}::{intent}"
        if key not in data:
            return False
        data[key]["entries"] = []
        _write(user_id, data)
    return True


def migrate_legacy_history(user_id: str):
    """
    Migrate legacy data into Replit DB (or user-scoped JSON file).
    Checks both the old prompt_history.json and user-scoped JSON files.
    Safe to call multiple times (idempotent).
    """
    # If on Replit DB, migrate any existing JSON files into it
    if _use_replit_db:
        # Check if data already exists in Replit DB
        existing = _db_read(user_id)
        if existing:
            return  # already migrated

        # Try user-scoped JSON file first
        user_file = Path(f"data/prompt_history_{user_id}.json")
        if user_file.exists():
            try:
                with open(user_file, "r") as f:
                    data = json.load(f)
                if data:
                    _db_write(user_id, data)
                    print(f"[HISTORY] Migrated {user_file} -> Replit DB")
                return
            except Exception as e:
                print(f"[HISTORY] Failed to migrate {user_file}: {e}")

        # Try legacy file
        legacy_file = Path("data/prompt_history.json")
        if legacy_file.exists():
            try:
                with open(legacy_file, "r") as f:
                    data = json.load(f)
                if data:
                    _db_write(user_id, data)
                    print(f"[HISTORY] Migrated legacy prompt_history.json -> Replit DB")
            except Exception as e:
                print(f"[HISTORY] Failed to migrate legacy history: {e}")
        return

    # Fallback: original JSON-to-JSON migration
    legacy_file = Path("data/prompt_history.json")
    target_file = _history_file(user_id)
    if legacy_file.exists() and not target_file.exists():
        try:
            import shutil
            shutil.copy2(legacy_file, target_file)
            print(f"[AUTH] Migrated prompt_history.json -> {target_file}")
        except Exception as e:
            print(f"[AUTH] Failed to migrate prompt history: {e}")
