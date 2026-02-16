import json
import re
import uuid
from datetime import datetime
from pathlib import Path

HISTORY_DIR = Path("data/chat_history_store")
_VALID_ID_PATTERN = re.compile(r'^[a-f0-9]{8}-[a-f0-9]{3}$')


def _validate_id(conv_id: str) -> bool:
    if not conv_id or not _VALID_ID_PATTERN.match(conv_id):
        return False
    resolved = (HISTORY_DIR / f"{conv_id}.json").resolve()
    return str(resolved).startswith(str(HISTORY_DIR.resolve()))


def _ensure_dir():
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)


def create_conversation(first_query: str) -> dict:
    _ensure_dir()

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

    filepath = HISTORY_DIR / f"{conv_id}.json"
    with open(filepath, "w") as f:
        json.dump(conversation, f)

    return conversation


def save_messages(conv_id: str, messages: list):
    if not _validate_id(conv_id):
        return False
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
    filepath = HISTORY_DIR / f"{conv_id}.json"
    if filepath.exists():
        filepath.unlink()
        return True
    return False
