"""
Persistent storage for prompt response history.
Stores responses grouped by category and prompt type (intent).

Primary: Replit Object Storage (persists across deploys AND autoscale).
Secondary: Replit DB (dev environment only).
Fallback: JSON files (for local dev outside Replit).
"""

import json
import os
import re
import time
from pathlib import Path
from threading import Lock

try:
    from langsmith import traceable
except ImportError:
    def traceable(*args, **kwargs):
        def _noop(fn):
            return fn
        if args and callable(args[0]):
            return args[0]
        return _noop


MAX_PER_INTENT = 100
_locks: dict = {}

# ── Storage backend detection ────────────────────────────────
# Priority: PostgreSQL > Object Storage > Replit DB > JSON files
_use_postgres = False
_use_object_storage = False
_obj_client = None
_use_replit_db = False
_replit_db = None
_pg_write_bucket = None
_pg_read_bucket = None

# 1. Try PostgreSQL first (most reliable — real database)
try:
    from data.pg_storage import is_available as _pg_available, init_tables as _pg_init
    from data.pg_storage import ph_read as _pg_read, ph_write as _pg_write, ph_write_bucket as _pg_write_bucket, ph_read_bucket as _pg_read_bucket
    if _pg_available():
        _pg_init()
        _use_postgres = True
        print("[HISTORY] Using PostgreSQL for prompt history (persistent across deploys)")
except Exception as e:
    print(f"[HISTORY] PostgreSQL unavailable ({e}), trying Object Storage...")
    _pg_write_bucket = None
    _pg_read_bucket = None

# 2. Try Object Storage (also used as migration source when PostgreSQL is primary)
_obj_client_for_migration = None
if not _use_postgres:
    try:
        from replit.object_storage import Client as _ObjClient
        _obj_client = _ObjClient()
        _use_object_storage = True
        print("[HISTORY] Using Replit Object Storage for prompt history (persistent across deploys)")
    except Exception as e:
        print(f"[HISTORY] Object Storage unavailable ({e}), trying Replit DB...")
        try:
            if os.environ.get("REPLIT_DB_URL"):
                from replit import db as _replit_db
                _use_replit_db = True
                print("[HISTORY] Using Replit DB for prompt history (dev only)")
        except Exception as e2:
            print(f"[HISTORY] Replit DB unavailable ({e2}), falling back to JSON files")
else:
    # PostgreSQL is primary — check if Object Storage has data to migrate
    try:
        from replit.object_storage import Client as _ObjClient
        _obj_client_for_migration = _ObjClient()
    except Exception:
        _obj_client_for_migration = None

# ── One-time migration: Object Storage / JSON → PostgreSQL ──
if _use_postgres and _obj_client_for_migration is not None:
    try:
        # Check if PostgreSQL is empty for the default user
        _existing = _pg_read("default")
        if not _existing:
            # Try migrating from Object Storage
            try:
                _obj_raw = _obj_client_for_migration.download_as_text("ph/default.json")
                if _obj_raw:
                    _obj_data = json.loads(_obj_raw)
                    if isinstance(_obj_data, dict) and _obj_data:
                        _pg_write("default", _obj_data)
                        print(f"[HISTORY] Migrated {len(_obj_data)} buckets from Object Storage → PostgreSQL")
                    else:
                        print("[HISTORY] Object Storage has no prompt history to migrate")
                else:
                    print("[HISTORY] Object Storage has no prompt history to migrate")
            except Exception as _mig_err:
                print(f"[HISTORY] Object Storage migration skipped: {_mig_err}")

            # Also try migrating from JSON files
            if not _pg_read("default"):
                _json_path = Path("data/prompt_history_default.json")
                if _json_path.exists():
                    try:
                        with open(_json_path, "r") as _jf:
                            _json_data = json.load(_jf)
                        if isinstance(_json_data, dict) and _json_data:
                            _pg_write("default", _json_data)
                            print(f"[HISTORY] Migrated {len(_json_data)} buckets from JSON → PostgreSQL")
                    except Exception as _jmig_err:
                        print(f"[HISTORY] JSON migration skipped: {_jmig_err}")
        else:
            print(f"[HISTORY] PostgreSQL already has {len(_existing)} buckets, no migration needed")
    except Exception as _mig_outer:
        print(f"[HISTORY] Migration check failed: {_mig_outer}")


def _get_lock(user_id: str) -> Lock:
    if user_id not in _locks:
        _locks[user_id] = Lock()
    return _locks[user_id]


# ── Object Storage helpers ───────────────────────────────────

def _obj_key(user_id: str) -> str:
    return f"ph/{user_id}.json"


def _obj_read(user_id: str) -> dict:
    try:
        raw = _obj_client.download_as_text(_obj_key(user_id))
        if not raw:
            return {}
        return json.loads(raw)
    except Exception:
        return {}


def _obj_write(user_id: str, data: dict):
    try:
        _obj_client.upload_from_text(_obj_key(user_id), json.dumps(data, default=str))
    except Exception as e:
        print(f"[HISTORY] Object Storage write error for {user_id}: {e}")


# ── Replit DB helpers (fallback for dev) ─────────────────────

def _db_key(user_id: str) -> str:
    return f"ph:{user_id}"


def _db_read(user_id: str) -> dict:
    try:
        raw = _replit_db.get(_db_key(user_id))
        if raw is None:
            return {}
        if isinstance(raw, str):
            return json.loads(raw)
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
    if _use_postgres:
        return _pg_read(user_id)
    if _use_object_storage:
        return _obj_read(user_id)
    if _use_replit_db:
        return _db_read(user_id)
    return _file_read(user_id)


def _write(user_id: str, data: dict):
    if _use_postgres:
        _pg_write(user_id, data)
    elif _use_object_storage:
        _obj_write(user_id, data)
    elif _use_replit_db:
        _db_write(user_id, data)
    else:
        _file_write(user_id, data)


# ── Ticker extraction from structured responses ─────────────

@traceable(name="prompt_history.parse_price")
def _parse_price(val) -> float | None:
    """Parse price from string like '$123.45' or float."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val) if val > 0 else None
    if isinstance(val, str):
        cleaned = re.sub(r'[^\d.]', '', val)
        try:
            p = float(cleaned)
            return p if p > 0 else None
        except (ValueError, TypeError):
            return None
    return None


@traceable(name="prompt_history.extract_tickers_from_structured")
def extract_tickers_from_structured(structured: dict) -> list[dict]:
    """
    Extract {ticker, rec_price} from a structured agent response.
    Handles all display_type formats: trades, investments, fundamentals,
    technicals, analysis, briefing, crypto, trending, screener, portfolio, etc.
    """
    if not isinstance(structured, dict):
        return []

    found = []
    seen = set()

    @traceable(name="prompt_history.add")
    def _add(ticker: str, price_val):
        if not ticker or ticker in seen:
            return
        t = ticker.upper().strip()
        if len(t) < 1 or len(t) > 10:
            return
        price = _parse_price(price_val)
        seen.add(t)
        found.append({"ticker": t, "rec_price": price})

    # picks[] — trades, investments, fundamentals, technicals
    for pick in structured.get("picks", []):
        if isinstance(pick, dict):
            _add(pick.get("ticker", ""), pick.get("price"))

    # top_trades[] — best trades format
    for trade in structured.get("top_trades", []):
        if isinstance(trade, dict):
            _add(trade.get("ticker", ""), trade.get("entry") or trade.get("price"))

    # bearish_setups[]
    for setup in structured.get("bearish_setups", []):
        if isinstance(setup, dict):
            _add(setup.get("ticker", ""), setup.get("entry") or setup.get("price"))

    # top_moves[] — briefing format
    for move in structured.get("top_moves", []):
        if isinstance(move, dict):
            _add(move.get("ticker", ""), move.get("entry") or move.get("price"))

    # signal_highlights — briefing format
    sh = structured.get("signal_highlights", {})
    if isinstance(sh, dict):
        for key, val in sh.items():
            if isinstance(val, dict) and val.get("ticker"):
                _add(val["ticker"], None)

    # top_momentum[] — crypto format
    for coin in structured.get("top_momentum", []):
        if isinstance(coin, dict):
            _add(coin.get("symbol", "") or coin.get("coin", ""), coin.get("price"))

    # trending_tickers[]
    for tt in structured.get("trending_tickers", []):
        if isinstance(tt, dict):
            _add(tt.get("ticker", ""), tt.get("price"))

    # results[] — screener format
    for r in structured.get("results", []):
        if isinstance(r, dict):
            _add(r.get("ticker", ""), r.get("price"))

    # top_picks[]
    for tp in structured.get("top_picks", []):
        if isinstance(tp, dict):
            _add(tp.get("ticker", ""), tp.get("price"))

    # positions[] — portfolio format
    for pos in structured.get("positions", []):
        if isinstance(pos, dict):
            _add(pos.get("ticker", ""), pos.get("price"))

    # analysis — single ticker deep dive
    if structured.get("display_type") == "analysis" and structured.get("ticker"):
        _add(structured["ticker"], structured.get("price"))

    # cross_market format — equities nested
    equities = structured.get("equities", {})
    if isinstance(equities, dict):
        for bucket in ["large_caps", "mid_caps", "small_micro_caps"]:
            for item in equities.get(bucket, []):
                if isinstance(item, dict):
                    _add(item.get("symbol", "") or item.get("ticker", ""), item.get("price"))

    # cross_market crypto/commodities
    for section in ["crypto", "commodities"]:
        for item in structured.get(section, []):
            if isinstance(item, dict):
                _add(item.get("symbol", "") or item.get("ticker", ""), item.get("price"))

    # csv_watchlist — strong_buy, buy, hold, sell buckets
    for bucket in ["strong_buy", "buy", "hold", "sell"]:
        for item in structured.get(bucket, []):
            if isinstance(item, dict):
                _add(item.get("ticker", ""), item.get("price"))

    return found


# ── Public API ───────────────────────────────────────────────

@traceable(name="prompt_history.save_response")
def save_response(category: str, intent: str, content: str, display_type: str | None = None, user_id: str = "default", model_used: str | None = None, query: str | None = None, tickers: list | None = None, conversation: list | None = None, structured_response: dict | None = None) -> dict:
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
    if tickers:
        entry["tickers"] = tickers
    if conversation:
        entry["conversation"] = conversation
    if structured_response:
        entry["structured_response"] = structured_response

    key = f"{category}::{intent}"

    # PostgreSQL fast path: single-bucket read + UPSERT — no global read/write, no delete cascade
    if _use_postgres and _pg_write_bucket and _pg_read_bucket:
        with _get_lock(user_id):
            bucket = _pg_read_bucket(user_id, key) or {"category": category, "intent": intent, "entries": []}
            entries = bucket.get("entries", [])
            entries.insert(0, entry)
            if len(entries) > MAX_PER_INTENT:
                entries = entries[:MAX_PER_INTENT]
            bucket["entries"] = entries
            bucket["category"] = category
            bucket["intent"] = intent
            _pg_write_bucket(user_id, key, bucket)
        return entry

    # Non-PostgreSQL path: full read/write (object storage / replit db / file)
    with _get_lock(user_id):
        data = _read(user_id)
        if key not in data:
            data[key] = {"category": category, "intent": intent, "entries": []}
        entries = data[key]["entries"]
        entries.insert(0, entry)
        if len(entries) > MAX_PER_INTENT:
            data[key]["entries"] = entries[:MAX_PER_INTENT]
        _write(user_id, data)
    return entry


@traceable(name="prompt_history.get_all")
def get_all(user_id: str = "default") -> dict:
    """Return all history grouped by category::intent."""
    return _read(user_id)


@traceable(name="prompt_history.get_by_intent")
def get_by_intent(category: str, intent: str, user_id: str = "default") -> list:
    """Return entries for a specific intent."""
    data = _read(user_id)
    key = f"{category}::{intent}"
    bucket = data.get(key, {})
    return bucket.get("entries", [])


@traceable(name="prompt_history.delete_entry")
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


@traceable(name="prompt_history.clear_intent")
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


@traceable(name="prompt_history.migrate_legacy_history")
def migrate_legacy_history(user_id: str):
    """
    Migrate legacy data into the active storage backend.
    Checks Object Storage, Replit DB, user-scoped JSON files, and legacy JSON files.
    Safe to call multiple times (idempotent).
    """
    # Check if data already exists in the current backend
    existing = _read(user_id)
    if existing:
        return  # already have data

    # Source 1: Object Storage (may have data from before PostgreSQL migration)
    if _use_postgres:
        try:
            from replit.object_storage import Client as _tmp_obj
            _tmp_client = _tmp_obj()
            raw = _tmp_client.download_as_text(f"ph/{user_id}.json")
            if raw:
                data = json.loads(raw)
                if data:
                    _write(user_id, data)
                    print(f"[HISTORY] Migrated Object Storage -> PostgreSQL for {user_id}")
                    return
        except Exception as e:
            print(f"[HISTORY] Object Storage migration check failed: {e}")

    # Source 2: Replit DB (may have data from before Object Storage migration)
    if (_use_object_storage or _use_postgres) and os.environ.get("REPLIT_DB_URL"):
        try:
            from replit import db as _tmp_db
            raw = _tmp_db.get(_db_key(user_id))
            if raw:
                data = json.loads(raw) if isinstance(raw, str) else json.loads(json.dumps(raw, default=str))
                if data:
                    _write(user_id, data)
                    print(f"[HISTORY] Migrated Replit DB -> active backend for {user_id}")
                    return
        except Exception as e:
            print(f"[HISTORY] Replit DB migration check failed: {e}")

    # Source 3: User-scoped JSON file
    user_file = Path(f"data/prompt_history_{user_id}.json")
    if user_file.exists():
        try:
            with open(user_file, "r") as f:
                data = json.load(f)
            if data:
                _write(user_id, data)
                print(f"[HISTORY] Migrated {user_file} -> active backend")
                return
        except Exception as e:
            print(f"[HISTORY] Failed to migrate {user_file}: {e}")

    # Source 4: Legacy global file
    legacy_file = Path("data/prompt_history.json")
    if legacy_file.exists():
        try:
            with open(legacy_file, "r") as f:
                data = json.load(f)
            if data:
                _write(user_id, data)
                print(f"[HISTORY] Migrated legacy prompt_history.json -> active backend")
        except Exception as e:
            print(f"[HISTORY] Failed to migrate legacy history: {e}")
