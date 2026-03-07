"""
Persistent storage for prompt response history.
Stores responses grouped by category and prompt type (intent).
JSON file-based, matching the chat_history.py pattern.
"""

import json
import time
from pathlib import Path
from threading import Lock

HISTORY_FILE = Path("data/prompt_history.json")
MAX_PER_INTENT = 100
_lock = Lock()


def _ensure_file():
    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    if not HISTORY_FILE.exists():
        with open(HISTORY_FILE, "w") as f:
            json.dump({}, f)


def _read() -> dict:
    _ensure_file()
    try:
        with open(HISTORY_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def _write(data: dict):
    _ensure_file()
    with open(HISTORY_FILE, "w") as f:
        json.dump(data, f, separators=(",", ":"))


def save_response(category: str, intent: str, content: str, display_type: str | None = None) -> dict:
    """Save a prompt response. Returns the created entry."""
    entry = {
        "id": str(int(time.time() * 1000)),
        "timestamp": time.time(),
        "content": content,
        "display_type": display_type,
    }
    with _lock:
        data = _read()
        key = f"{category}::{intent}"
        if key not in data:
            data[key] = {"category": category, "intent": intent, "entries": []}
        entries = data[key]["entries"]
        entries.insert(0, entry)
        if len(entries) > MAX_PER_INTENT:
            data[key]["entries"] = entries[:MAX_PER_INTENT]
        _write(data)
    return entry


def get_all() -> dict:
    """Return all history grouped by category::intent."""
    return _read()


def get_by_intent(category: str, intent: str) -> list:
    """Return entries for a specific intent."""
    data = _read()
    key = f"{category}::{intent}"
    bucket = data.get(key, {})
    return bucket.get("entries", [])


def delete_entry(category: str, intent: str, entry_id: str) -> bool:
    """Delete a single history entry."""
    with _lock:
        data = _read()
        key = f"{category}::{intent}"
        if key not in data:
            return False
        before = len(data[key]["entries"])
        data[key]["entries"] = [e for e in data[key]["entries"] if e["id"] != entry_id]
        if len(data[key]["entries"]) == before:
            return False
        _write(data)
    return True


def clear_intent(category: str, intent: str) -> bool:
    """Clear all entries for an intent."""
    with _lock:
        data = _read()
        key = f"{category}::{intent}"
        if key not in data:
            return False
        data[key]["entries"] = []
        _write(data)
    return True
