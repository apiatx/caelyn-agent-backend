"""
Ticker name override layer — user-approved ticker→company name assignments that
win over Tradier description, FMP quote name, and CSV column lookups.

Priority chain (highest → lowest):
  1. Manual name override  (this module)
  2. FMP profile companyName (if in screener cache)
  3. Tradier quote description  (exchange-listed name)
  4. FMP /stable/quote name  (FMP's name field)
  5. CSV "Company Name" column
  6. Ticker symbol itself

Overrides are persisted in Neon (ticker_name_overrides table) and cached
in-process for 5 minutes to avoid DB round-trips on every request.

Needed because exchanges are slow to update company names after reverse
mergers and corporate name changes, so Tradier/FMP frequently return stale
or wrong names for OTC/small-cap stocks.
"""

import time
import threading
from typing import Optional

# ── In-memory cache ────────────────────────────────────────────────────────────
# keyed by user_id → (ticker→name dict, expires_at monotonic seconds)

_CACHE: dict[str, tuple[dict[str, str], float]] = {}
_CACHE_TTL = 300.0   # 5 minutes
_CACHE_LOCK = threading.Lock()


def _cache_get(user_id: str) -> dict[str, str] | None:
    with _CACHE_LOCK:
        entry = _CACHE.get(user_id)
        if entry and time.monotonic() < entry[1]:
            return entry[0]
    return None


def _cache_set(user_id: str, overrides: dict[str, str]) -> None:
    with _CACHE_LOCK:
        _CACHE[user_id] = (overrides, time.monotonic() + _CACHE_TTL)


def _cache_invalidate(user_id: str) -> None:
    with _CACHE_LOCK:
        _CACHE.pop(user_id, None)


# ── Public read API ────────────────────────────────────────────────────────────

def get_name_overrides(user_id: str = "default") -> dict[str, str]:
    """
    Return ticker→name map for the given user.
    Served from in-memory cache; repopulated from DB on miss/expiry.
    Never raises — returns {} on any error.
    """
    cached = _cache_get(user_id)
    if cached is not None:
        return cached
    try:
        from data.pg_storage import get_name_overrides as _db_get
        overrides = _db_get(user_id)
        _cache_set(user_id, overrides)
        return overrides
    except Exception as e:
        print(f"[NAME_OVERRIDES] get_name_overrides failed (non-fatal): {e}")
        return {}


# ── Public write API ───────────────────────────────────────────────────────────

def upsert_override(
    user_id: str,
    ticker: str,
    name: str,
    source: str = "manual",
    reason: Optional[str] = None,
) -> bool:
    """Persist a single ticker→name override. Invalidates cache on success."""
    try:
        from data.pg_storage import upsert_name_override as _db_upsert
        ok = _db_upsert(
            user_id,
            ticker.strip().upper(),
            name.strip(),
            source,
            reason,
        )
        if ok:
            _cache_invalidate(user_id)
        return ok
    except Exception as e:
        print(f"[NAME_OVERRIDES] upsert_override failed: {e}")
        return False


def bulk_upsert(
    user_id: str,
    updates: list[dict],
) -> int:
    """
    Persist multiple overrides in one transaction.
    Each entry: {ticker, name, source='manual', reason=None}
    Returns count of rows inserted/updated. Invalidates cache.
    """
    if not updates:
        return 0
    try:
        from data.pg_storage import bulk_upsert_name_overrides as _db_bulk
        count = _db_bulk(user_id, updates)
        if count > 0:
            _cache_invalidate(user_id)
        return count
    except Exception as e:
        print(f"[NAME_OVERRIDES] bulk_upsert failed: {e}")
        return 0


def apply_to_ticker_rows(
    rows: list[dict],
    user_id: str = "default",
) -> list[dict]:
    """
    Apply name overrides to a list of ticker dicts (each must have 'symbol' key).
    Returns a new list; input is NOT mutated.
    """
    overrides = get_name_overrides(user_id)
    if not overrides:
        return rows
    result = []
    for row in rows:
        sym = str(row.get("symbol") or row.get("ticker") or "").strip().upper()
        override_name = overrides.get(sym)
        if override_name:
            result.append({**row, "name": override_name})
        else:
            result.append(row)
    return result


# ── Startup seeding ────────────────────────────────────────────────────────────
# User-approved company name corrections applied once on server start.
# Needed because exchanges are slow to reflect corporate name changes for
# OTC/small-cap tickers, so Tradier and FMP return stale or wrong names.

_SEED_NAMES: list[tuple[str, str]] = [
    # Incorrect Tradier/FMP exchange name → correct current company name
    ("TE",   "T1 Energy"),
    ("KEEL", "Keel Infrastructure Group"),
    ("INFQ", "Infleqtion, Inc."),
    ("FRMI", "Fermi, Inc."),
    ("NUAI", "Nu Era Energy & Digital Inc."),
    ("BZAI", "Blaize Holdings, Inc."),
    ("IMSR", "Terrestrial Energy"),
    ("XNDU", "Xanadu Quantum Technologies Limited"),
    ("P",    "Pure Storage"),
    ("VELO", "Velo3D, Inc."),
    ("KRMN", "Karman Holdings"),
    ("FJET", "Starfighters Space Inc."),
    ("VIVO", "Vivo Power PLC"),
    ("Q",    "Qnity Electronics, Inc."),
    ("ALMU", "Aeluma, Inc."),
    ("DGXX", "Digi Power X Inc"),
]


def seed_initial_name_overrides(user_id: str = "default") -> int:
    """
    Apply the predefined name corrections if they are not already in the DB.
    Called once at server startup. Returns count of rows upserted.
    Idempotent — safe to call on every restart.
    """
    updates = [
        {"ticker": t, "name": n, "source": "manual", "reason": "initial_seed"}
        for t, n in _SEED_NAMES
    ]
    count = bulk_upsert(user_id, updates)
    print(
        f"[NAME_OVERRIDES] seed_initial_name_overrides: {count} row(s) upserted "
        f"(user_id={user_id!r})"
    )
    return count
