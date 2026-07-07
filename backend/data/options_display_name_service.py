"""
Options Flow display-name resolution service.

Provides fast memory-only lookups for ticker display_name in the sector/theme
tree builder, backed by a disk-persistent LKG and an async background enrichment
loop that fills gaps via FMP /stable/profile.

Storage: backend/data/options_display_name_lkg.json
  {
    "NVDA":  "NVIDIA Corporation",
    "SMH":   "VanEck Semiconductor ETF",
    "_saved_at": 1720000000.0,
    "_failed": { "XYZ": 1720000000.0 }
  }

Sources (priority order, highest first):
  1. warm_up_display_names_from_db — screener_fundamentals_cache (DB, sync, at startup)
  2. enrich_display_names_background — FMP /stable/profile background pass (async, paced)

Keys must NOT start with "_" (those are metadata fields in the JSON file).
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

_LKG_PATH = Path(__file__).parent / "options_display_name_lkg.json"

_DNAME: dict[str, str] = {}              # symbol → resolved display name
_DNAME_FAILED: dict[str, float] = {}    # symbol → timestamp (FMP returned no name)
_LOADED = False
_SAVE_NEEDED = False
_LAST_SAVE_AT: float | None = None

# Re-attempt failed symbols after 24 h in case FMP data improved.
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
        _LAST_SAVE_AT = d.pop("_saved_at", None)
        raw_failed    = d.pop("_failed", {}) or {}
        for k, v in d.items():
            if not k.startswith("_") and isinstance(v, str) and v:
                _DNAME[k.upper()] = v
        for k, ts in raw_failed.items():
            _DNAME_FAILED[k.upper()] = float(ts)
    except Exception as e:
        print(f"[DISPLAY_NAME] _load_disk error (non-fatal): {e}")


def _save_disk() -> None:
    global _SAVE_NEEDED, _LAST_SAVE_AT
    try:
        payload: dict = {k: v for k, v in _DNAME.items()}
        payload["_saved_at"] = time.time()
        payload["_failed"]   = dict(_DNAME_FAILED)
        _LKG_PATH.write_text(json.dumps(payload))
        _LAST_SAVE_AT = payload["_saved_at"]
        _SAVE_NEEDED  = False
    except Exception as e:
        print(f"[DISPLAY_NAME] _save_disk error (non-fatal): {e}")


# ── Internal set helper ───────────────────────────────────────────────────────

def _set_name(symbol: str, name: str) -> None:
    global _SAVE_NEEDED
    sym = symbol.upper()
    if _DNAME.get(sym) != name:
        _DNAME[sym] = name
        _DNAME_FAILED.pop(sym, None)
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


# ── FMP background enrichment ─────────────────────────────────────────────────

async def enrich_display_names_background(
    symbols: list[str],
    fmp_provider=None,
    *,
    rate_limit_sleep: float = 0.35,
    max_per_pass: int = 60,
) -> int:
    """
    Enrich missing display names via FMP /stable/profile.

    MUST only be called from background tasks, never from API request handlers.

    Skips symbols that already have a name in _DNAME.
    Skips symbols marked _DNAME_FAILED within the last _FAILED_TTL_S seconds.

    Returns count of newly stored names.
    """
    if not _LOADED:
        _load_disk()

    if fmp_provider is None:
        return 0

    now = time.time()
    missing = [
        s.upper() for s in symbols
        if not _DNAME.get(s.upper())
        and (
            s.upper() not in _DNAME_FAILED
            or now - _DNAME_FAILED[s.upper()] > _FAILED_TTL_S
        )
    ][:max_per_pass]

    if not missing:
        return 0

    count = 0
    failed_count = 0
    for sym in missing:
        try:
            data = await fmp_provider._get_stable("profile", {"symbol": sym})
            if data and isinstance(data, list) and len(data) > 0:
                name = (data[0].get("companyName") or "").strip()
                if name:
                    _set_name(sym, name)
                    count += 1
                else:
                    _DNAME_FAILED[sym] = now
                    global _SAVE_NEEDED
                    _SAVE_NEEDED = True
                    failed_count += 1
            else:
                _DNAME_FAILED[sym] = now
                _SAVE_NEEDED = True
                failed_count += 1
            await asyncio.sleep(rate_limit_sleep)
        except Exception as e:
            print(f"[DISPLAY_NAME] enrich {sym}: {e}")
            await asyncio.sleep(rate_limit_sleep)

    if count > 0 or failed_count > 0:
        _save_disk()
        print(
            f"[DISPLAY_NAME] enriched {count} names "
            f"(+{failed_count} no-name); total_resolved={len(_DNAME)}"
        )
    return count


# ── load on import ────────────────────────────────────────────────────────────
_load_disk()
