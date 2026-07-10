"""
Category override layer — user-approved ticker→category assignments that win
over AI-generated, industry-fallback, and static theme-mapper classifications.

Priority chain (highest → lowest):
  1. Manual override  (this module)
  2. analysis.sections from saved watchlist / Chart Radar section_map
  3. industry fallback  (map_industry_to_theme)
  4. static theme_ticker_mapper
  5. Unknown Theme / Other / Uncategorized

Overrides are persisted in Neon (watchlist_category_overrides table) and
cached in-process for 5 minutes to avoid DB round-trips on every request.

Both Watchlist and Chart Radar call into this module; a single write
automatically propagates to both pages on the next request.
"""

import time
import threading
from typing import Optional

# ── In-memory cache ────────────────────────────────────────────────────────────
# keyed by user_id → (ticker→category dict, expires_at monotonic seconds)

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

def get_overrides(user_id: str = "default") -> dict[str, str]:
    """
    Return ticker→category map for the given user.
    Served from in-memory cache; repopulated from DB on miss/expiry.
    Never raises — returns {} on any error.
    """
    cached = _cache_get(user_id)
    if cached is not None:
        return cached
    try:
        from data.pg_storage import get_category_overrides as _db_get
        overrides = _db_get(user_id)
        _cache_set(user_id, overrides)
        return overrides
    except Exception as e:
        print(f"[CATEGORY_OVERRIDES] get_overrides failed (non-fatal): {e}")
        return {}


# ── Public write API ───────────────────────────────────────────────────────────

def upsert_override(
    user_id: str,
    ticker: str,
    category: str,
    source: str = "manual",
    reason: Optional[str] = None,
) -> bool:
    """Persist a single ticker→category override. Invalidates cache on success."""
    try:
        from data.pg_storage import upsert_category_override as _db_upsert
        ok = _db_upsert(
            user_id,
            ticker.strip().upper(),
            category.strip(),
            source,
            reason,
        )
        if ok:
            _cache_invalidate(user_id)
        return ok
    except Exception as e:
        print(f"[CATEGORY_OVERRIDES] upsert_override failed: {e}")
        return False


def delete_override(
    user_id: str,
    ticker: str,
    only_if_category: Optional[str] = None,
) -> bool:
    """
    Delete a single ticker→category override. If only_if_category is given,
    only deletes when the current stored category matches (safe cleanup on
    reassignment). Invalidates cache on success.
    """
    try:
        from data.pg_storage import delete_category_override as _db_delete
        deleted = _db_delete(user_id, ticker.strip().upper(), only_if_category)
        if deleted:
            _cache_invalidate(user_id)
        return deleted
    except Exception as e:
        print(f"[CATEGORY_OVERRIDES] delete_override failed: {e}")
        return False


def bulk_upsert(
    user_id: str,
    updates: list[dict],
) -> int:
    """
    Persist multiple overrides in one transaction.
    Each entry: {ticker, category, source='manual', reason=None}
    Returns count of rows inserted/updated. Invalidates cache.
    """
    if not updates:
        return 0
    try:
        from data.pg_storage import bulk_upsert_category_overrides as _db_bulk
        count = _db_bulk(user_id, updates)
        if count > 0:
            _cache_invalidate(user_id)
        return count
    except Exception as e:
        print(f"[CATEGORY_OVERRIDES] bulk_upsert failed: {e}")
        return 0


# ── Integration helpers ────────────────────────────────────────────────────────

def apply_to_section_map(
    section_map: dict[str, str],
    user_id: str = "default",
) -> dict[str, str]:
    """
    Return a new section_map with manual overrides applied on top.
    Overrides always win — Chart Radar integration point.

    section_map: ticker (upper) → section title
    Returns:     same shape, with override entries replacing any prior assignment.
    """
    overrides = get_overrides(user_id)
    if not overrides:
        return section_map
    merged = dict(section_map)
    merged.update(overrides)   # manual wins
    return merged


def apply_to_sections(
    sections: list[dict],
    user_id: str = "default",
) -> list[dict]:
    """
    Re-route tickers that have a manual override into the correct section.
    Returns a new sections list; input is NOT mutated.
    Watchlist page integration point.

    Each section dict: {title, tickers: [{symbol, ...}, ...], ...}
    """
    overrides = get_overrides(user_id)
    if not overrides:
        return sections

    # Build mutable copy: title → section dict with mutable tickers list
    sec_by_title: dict[str, dict] = {}
    result: list[dict] = []
    for sec in sections:
        title = (sec.get("title") or sec.get("name") or "").strip()
        new_sec = {**sec, "tickers": list(sec.get("tickers", []))}
        sec_by_title[title] = new_sec
        result.append(new_sec)

    moved = 0
    for sec in result:
        current_title = (sec.get("title") or "").strip()
        kept: list[dict] = []
        for row in sec["tickers"]:
            sym = str(row.get("symbol") or row.get("ticker") or "").strip().upper()
            new_cat = overrides.get(sym)
            if new_cat and new_cat != current_title:
                # Move ticker into override category
                moved_row = {
                    **row,
                    "canonical_theme_name": new_cat,
                    "theme_source":         "manual_override",
                }
                if new_cat in sec_by_title:
                    sec_by_title[new_cat]["tickers"].append(moved_row)
                else:
                    new_id = (
                        new_cat.lower()
                        .replace(" ", "_")
                        .replace("/", "_")
                        .replace("&", "and")
                    )
                    new_s = {
                        "id":           new_id,
                        "title":        new_cat,
                        "subtitle":     "Manual category assignment",
                        "tickers":      [moved_row],
                        "theme_source": "manual_override",
                    }
                    sec_by_title[new_cat] = new_s
                    result.append(new_s)
                moved += 1
            else:
                kept.append(row)
        sec["tickers"] = kept

    if moved:
        print(f"[CATEGORY_OVERRIDES] applied {moved} manual override(s) to sections")

    # Drop sections that became empty after moves
    return [s for s in result if s.get("tickers")]


# ── Startup seeding ────────────────────────────────────────────────────────────
# These are the user-approved corrections applied once on server start.
# They are idempotent (upsert) — safe to run on every restart.

_SEED_OVERRIDES: list[tuple[str, str]] = [
    # Cloud Software → Data Center Infrastructure
    ("BTDR", "Data Center Infrastructure"),
    ("ADEA", "Data Center Infrastructure"),
    ("BZAI", "Data Center Infrastructure"),
    # Space Economy
    ("BKSY", "Space Economy"),
    # Semiconductors
    ("PDFS", "Semiconductors"),
    ("SVCO", "Semiconductors"),
    # Crypto Equities / Blockchain → Data Center Infrastructure
    ("CIFR", "Data Center Infrastructure"),
    ("CLSK", "Data Center Infrastructure"),
    ("GLXY", "Data Center Infrastructure"),
    ("HUT",  "Data Center Infrastructure"),
    ("MARA", "Data Center Infrastructure"),
    # Solar → Clean Energy
    ("ARRY", "Clean Energy"),
    # Clean Energy
    ("HYLN", "Clean Energy"),
    # Robotics & Automation
    ("AUR",  "Robotics & Automation"),
    ("AMBA", "Robotics & Automation"),
    ("OUST", "Robotics & Automation"),
    ("AEVA", "Robotics & Automation"),
    # Nuclear / Grid → Uranium & Nuclear Energy
    ("ASPI", "Uranium & Nuclear Energy"),
    ("IMSR", "Uranium & Nuclear Energy"),
    # Previously Unknown Theme → Data Center Infrastructure
    ("IREN", "Data Center Infrastructure"),
    ("SLNH", "Data Center Infrastructure"),
    ("WULF", "Data Center Infrastructure"),
    # Fintech
    ("CRCL", "Fintech"),
    ("INV",  "Fintech"),
    # Quantum Computing
    ("XNDU", "Quantum Computing"),
    # Clean Energy
    ("TE",   "Clean Energy"),
]


def seed_initial_overrides(user_id: str = "default") -> int:
    """
    Apply the predefined category corrections if they are not already in the DB.
    Called once at server startup. Returns count of rows upserted.
    Idempotent — safe to call on every restart.
    """
    updates = [
        {"ticker": t, "category": c, "source": "manual", "reason": "initial_seed"}
        for t, c in _SEED_OVERRIDES
    ]
    count = bulk_upsert(user_id, updates)
    print(
        f"[CATEGORY_OVERRIDES] seed_initial_overrides: {count} row(s) upserted "
        f"(user_id={user_id!r})"
    )
    return count
