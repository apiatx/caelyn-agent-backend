"""
Options Flow display-name resolution service.

Provides fast memory-only lookups for ticker display_name in the sector/theme
tree builder, backed by a disk-persistent LKG.

Storage: backend/data/options_display_name_lkg.json
  {
    "NVDA":  "NVIDIA Corporation",
    "SMH":   "VanEck Semiconductor ETF",
    "_saved_at": 1720000000.0,
    "_failed":   { "XYZ": 1720000000.0 },
    "_attempts": { "XYZ": 3 }
  }

Sources (priority order, highest first):
  1. options_display_name_lkg.json — persisted LKG, loaded at import
  2. warm_up_display_names_from_db — screener_fundamentals_cache (DB, sync, at startup)
  3. enrich_display_names_background — only for NEW symbols absent from all existing
     caches; checks fmp_cache_service first, falls back to FMP /stable/profile.

FMP enrichment is NOT called on a fixed timer.  It is called only when the
background instrument-type loop detects symbols with display_name_missing > 0.
After _MAX_FMP_ATTEMPTS consecutive empty-name FMP responses, a symbol is marked
permanently failed (sentinel timestamp) and never retried.

Keys must NOT start with "_" (those are metadata fields in the JSON file).
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

_LKG_PATH = Path(__file__).parent / "options_display_name_lkg.json"

_DNAME: dict[str, str] = {}           # symbol → resolved display name
_DNAME_FAILED: dict[str, float] = {}  # symbol → timestamp (or _PERM_FAILED_SENTINEL)
_DNAME_ATTEMPT: dict[str, int] = {}   # symbol → consecutive FMP no-name count
_LOADED = False
_SAVE_NEEDED = False
_LAST_SAVE_AT: float | None = None

# After this many consecutive empty-name FMP responses the symbol is permanently failed.
_MAX_FMP_ATTEMPTS = 3
# Sentinel stored in _DNAME_FAILED for permanent (never-retry) failures.
# Chosen to be ~year 2286 so it never expires by the TTL check.
_PERM_FAILED_SENTINEL = 9_999_999_999.0
# Temporary failures are retried after 24 h (FMP data occasionally improves).
_FAILED_TTL_S = 86_400


# ── Disk persistence ──────────────────────────────────────────────────────────

def _load_disk() -> None:
    global _LOADED, _LAST_SAVE_AT
    if _LOADED:
        return
    _LOADED = True
    if not _LKG_PATH.exists():
        return
    try:
        d = json.loads(_LKG_PATH.read_text())
        _LAST_SAVE_AT  = d.pop("_saved_at", None)
        raw_failed     = d.pop("_failed",   {}) or {}
        raw_attempts   = d.pop("_attempts", {}) or {}
        for k, v in d.items():
            if not k.startswith("_") and isinstance(v, str) and v:
                _DNAME[k.upper()] = v
        for k, ts in raw_failed.items():
            _DNAME_FAILED[k.upper()] = float(ts)
        for k, n in raw_attempts.items():
            _DNAME_ATTEMPT[k.upper()] = int(n)
    except Exception as e:
        print(f"[DISPLAY_NAME] _load_disk error (non-fatal): {e}")


def _save_disk() -> None:
    global _SAVE_NEEDED, _LAST_SAVE_AT
    try:
        payload: dict = {k: v for k, v in _DNAME.items()}
        payload["_saved_at"]  = time.time()
        payload["_failed"]    = dict(_DNAME_FAILED)
        payload["_attempts"]  = dict(_DNAME_ATTEMPT)
        _LKG_PATH.write_text(json.dumps(payload))
        _LAST_SAVE_AT = payload["_saved_at"]
        _SAVE_NEEDED  = False
    except Exception as e:
        print(f"[DISPLAY_NAME] _save_disk error (non-fatal): {e}")


# ── Internal set helper ───────────────────────────────────────────────────────

def _is_perm_failed(sym: str) -> bool:
    """True when the symbol has been permanently marked as name_resolution_failed."""
    return _DNAME_FAILED.get(sym, 0.0) >= _PERM_FAILED_SENTINEL


def _set_name(symbol: str, name: str) -> None:
    global _SAVE_NEEDED
    sym = symbol.upper()
    if _DNAME.get(sym) != name:
        _DNAME[sym] = name
        _DNAME_FAILED.pop(sym, None)
        _DNAME_ATTEMPT.pop(sym, None)   # clear retry counter on successful resolution
        _SAVE_NEEDED = True


# ── Public read API (memory-only, always fast) ────────────────────────────────

def get_display_name(symbol: str) -> str | None:
    if not _LOADED:
        _load_disk()
    return _DNAME.get(symbol.upper())


def get_display_name_bulk(symbols) -> dict[str, str]:
    """
    Return {symbol → display_name} for each symbol that has a resolved name.
    Symbols with no name are omitted from the result dict.
    Never blocks — memory read only.
    """
    if not _LOADED:
        _load_disk()
    result: dict[str, str] = {}
    for sym in symbols:
        name = _DNAME.get(sym.upper())
        if name:
            result[sym.upper()] = name
    return result


def get_display_name_stats(required_symbols) -> dict:
    """
    Return coverage metrics for the given required universe.
    """
    if not _LOADED:
        _load_disk()
    syms = [s.upper() for s in required_symbols]
    total     = len(syms)
    resolved  = sum(1 for s in syms if _DNAME.get(s))
    failed    = sum(1 for s in syms if s in _DNAME_FAILED)
    missing   = total - resolved - failed
    return {
        "display_name_total":    total,
        "display_name_resolved": resolved,
        "display_name_failed":   failed,
        "display_name_missing":  missing,
        "display_name_coverage_pct": round(resolved / max(total, 1) * 100, 1),
        "display_name_updated_at": _LAST_SAVE_AT,
    }


# ── DB warm-up (sync, safe to call at startup) ────────────────────────────────

def warm_up_display_names_from_db(symbols: list[str] | None = None) -> int:
    """
    Populate _DNAME from screener_fundamentals_cache.profile_json.companyName.
    Called at startup.  Returns count of newly stored names.
    """
    if not _LOADED:
        _load_disk()
    try:
        from data.pg_storage import _get_conn, _put_conn
        conn = _get_conn()
        cur  = conn.cursor()
        if symbols:
            cur.execute(
                "SELECT symbol, profile_json FROM screener_fundamentals_cache "
                "WHERE symbol = ANY(%s)",
                (list(symbols),),
            )
        else:
            cur.execute(
                "SELECT symbol, profile_json FROM screener_fundamentals_cache"
            )
        rows = cur.fetchall()
        cur.close()
        _put_conn(conn)

        count = 0
        for sym, pj in rows:
            sym = sym.upper()
            if _DNAME.get(sym):
                continue  # already have a name — skip
            d    = (json.loads(pj) if isinstance(pj, str) else pj) or {}
            name = (d.get("companyName") or "").strip()
            if name:
                _set_name(sym, name)
                count += 1

        if count > 0:
            _save_disk()
        return count
    except Exception as e:
        print(f"[DISPLAY_NAME] warm_up_display_names_from_db: {e}")
        return 0


# ── FMP background enrichment (new symbols only) ──────────────────────────────

async def enrich_display_names_background(
    symbols: list[str],
    fmp_provider=None,
    *,
    rate_limit_sleep: float = 0.35,
    max_per_pass: int = 60,
) -> int:
    """
    Enrich display names for symbols that are genuinely missing from all sources.

    MUST only be called from background tasks, never from API request handlers.
    This function is NOT invoked on a fixed timer; the caller (itype_classify_loop)
    gates it behind display_name_missing > 0 so it only runs when new symbols have
    entered the required universe without a cached name.

    Pass order for each missing symbol:
      1. fmp_cache_service.get_company_profiles_bulk_cached — no network call.
      2. FMP /stable/profile — one network call per symbol, paced at rate_limit_sleep.

    Permanently-failed symbols (_PERM_FAILED_SENTINEL) are never retried.
    Temporary failures (empty FMP response) are retried up to _MAX_FMP_ATTEMPTS
    times; on the final attempt the symbol is permanently failed.

    Returns count of newly stored names.
    """
    if not _LOADED:
        _load_disk()

    if fmp_provider is None:
        return 0

    global _SAVE_NEEDED

    now = time.time()

    # Candidates: no name, not permanently failed, and either never failed or
    # their temporary failure has expired beyond _FAILED_TTL_S.
    missing = [
        s.upper() for s in symbols
        if not _DNAME.get(s.upper())
        and not _is_perm_failed(s.upper())
        and (
            s.upper() not in _DNAME_FAILED
            or now - _DNAME_FAILED[s.upper()] > _FAILED_TTL_S
        )
    ][:max_per_pass]

    if not missing:
        return 0

    count = 0
    fmp_failed_count = 0

    # ── Pass 1: existing fmp_cache_service profiles (no network call) ──────────
    try:
        from services.fmp_cache_service import get_company_profiles_bulk_cached as _bulk
        cached = _bulk(missing)
        still_missing: list[str] = []
        for sym in missing:
            if _DNAME.get(sym):
                continue  # filled by a concurrent operation
            cached_name = (cached.get(sym, {}).get("name") or "").strip()
            if cached_name:
                _set_name(sym, cached_name)
                count += 1
            else:
                still_missing.append(sym)
        missing = still_missing
    except Exception:
        pass  # fall through to FMP calls

    if not missing:
        if count:
            _save_disk()
            print(f"[DISPLAY_NAME] enriched {count} names from cache (no FMP calls); "
                  f"total_resolved={len(_DNAME)}")
        return count

    # ── Pass 2: FMP /stable/profile for symbols still missing after cache ──────
    for sym in missing:
        try:
            data = await fmp_provider._get_stable("profile", {"symbol": sym})
            if data and isinstance(data, list) and len(data) > 0:
                name = (data[0].get("companyName") or "").strip()
                if name:
                    _set_name(sym, name)
                    count += 1
                else:
                    _record_fmp_failure(sym, now)
                    fmp_failed_count += 1
            else:
                _record_fmp_failure(sym, now)
                fmp_failed_count += 1
            await asyncio.sleep(rate_limit_sleep)
        except Exception as e:
            print(f"[DISPLAY_NAME] enrich {sym}: {e}")
            await asyncio.sleep(rate_limit_sleep)

    if count > 0 or fmp_failed_count > 0:
        _save_disk()
        perm_count = sum(1 for s in missing if _is_perm_failed(s))
        print(
            f"[DISPLAY_NAME] enriched {count} names via FMP "
            f"(+{fmp_failed_count} no-name, {perm_count} now permanently failed); "
            f"total_resolved={len(_DNAME)}"
        )
    return count


def _record_fmp_failure(sym: str, now: float) -> None:
    """
    Increment the FMP attempt counter for *sym*.  On reaching _MAX_FMP_ATTEMPTS,
    set the permanent-failure sentinel so the symbol is never retried.
    """
    global _SAVE_NEEDED
    attempts = _DNAME_ATTEMPT.get(sym, 0) + 1
    _DNAME_ATTEMPT[sym] = attempts
    if attempts >= _MAX_FMP_ATTEMPTS:
        _DNAME_FAILED[sym] = _PERM_FAILED_SENTINEL
    else:
        _DNAME_FAILED[sym] = now  # temporary — retry after _FAILED_TTL_S
    _SAVE_NEEDED = True


# ── load on import ────────────────────────────────────────────────────────────
_load_disk()
