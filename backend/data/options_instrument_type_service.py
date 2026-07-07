"""
Persistent ETF vs stock classification for the options flow universe.

Classification hierarchy (in priority order):
  1. In-memory LKG (loaded from disk at import time)
  2. screener_fundamentals_cache.profile_json.isEtf  (FMP data in Neon DB)
  3. FMP /v3/profile/{symbol}  — background-only, never during request handling

Returns:
  "etf"     — confirmed ETF / fund / closed-end fund
  "stock"   — confirmed equity (common stock or similar)
  "unknown" — classification not yet available

NEVER called during request handling for per-symbol FMP lookups.
  get_instrument_type() and get_instrument_type_bulk() are always fast
  (memory-only reads) and safe to call anywhere.

  classify_symbols_background() is the async writer — call it only from
  background tasks (startup warm-up, post-scan passes, supplement loop).
"""
from __future__ import annotations

import asyncio
import json
import math
import os
import time
from pathlib import Path

_LKG_PATH = Path(__file__).parent / "options_instrument_type_lkg.json"
_MEM: dict[str, str] = {}   # symbol → "stock" | "etf" | "unknown"
_LOADED = False
_SAVE_NEEDED = False

# Stamp so we know when the LKG was last populated
_LAST_SAVE_AT: float = 0.0


# ── Disk persistence ──────────────────────────────────────────────────────────

def _load_disk() -> None:
    global _MEM, _LOADED, _LAST_SAVE_AT
    try:
        if _LKG_PATH.exists():
            with open(_LKG_PATH) as f:
                d = json.load(f)
            if isinstance(d, dict):
                for k, v in d.items():
                    if v in ("stock", "etf", "unknown"):
                        _MEM[k.upper()] = v
                _LAST_SAVE_AT = d.get("_saved_at", 0.0)
    except Exception as e:
        print(f"[INSTRUMENT_TYPE] disk load failed: {e}")
    _LOADED = True


def _save_disk() -> None:
    global _SAVE_NEEDED, _LAST_SAVE_AT
    if not _SAVE_NEEDED:
        return
    try:
        payload = dict(_MEM)
        payload["_saved_at"] = time.time()
        tmp = str(_LKG_PATH) + ".tmp"
        with open(tmp, "w") as f:
            json.dump(payload, f)
        os.replace(tmp, _LKG_PATH)
        _LAST_SAVE_AT = payload["_saved_at"]
        _SAVE_NEEDED = False
    except Exception as e:
        print(f"[INSTRUMENT_TYPE] disk save failed: {e}")


def _set(symbol: str, itype: str) -> None:
    global _SAVE_NEEDED
    sym = symbol.upper()
    if sym == "_SAVED_AT":
        return
    if _MEM.get(sym) != itype:
        _MEM[sym] = itype
        _SAVE_NEEDED = True


# ── Public read API (memory-only, always fast) ────────────────────────────────

def get_instrument_type(symbol: str) -> str:
    """Return 'stock', 'etf', or 'unknown'. Never blocks."""
    if not _LOADED:
        _load_disk()
    return _MEM.get(symbol.upper(), "unknown")


def get_instrument_type_bulk(symbols) -> dict[str, str]:
    """
    Return {symbol → type} for each symbol in the iterable.
    Never blocks — memory read only.
    """
    if not _LOADED:
        _load_disk()
    return {sym.upper(): _MEM.get(sym.upper(), "unknown") for sym in symbols}


def get_stats() -> dict:
    """Diagnostic stats for admin endpoints and tree builders."""
    stocks  = sum(1 for v in _MEM.values() if v == "stock")
    etfs    = sum(1 for v in _MEM.values() if v == "etf")
    unknown = sum(1 for v in _MEM.values() if v == "unknown")
    unknown_syms = [k for k, v in _MEM.items() if v == "unknown"][:20]
    return {
        "total":          len(_MEM),
        "stocks":         stocks,
        "etfs":           etfs,
        "unknown":        unknown,
        "unknown_sample": unknown_syms,
        "saved_at":       _LAST_SAVE_AT or None,
        "updated_at":     _LAST_SAVE_AT or None,
    }


def get_unresolved_symbols(symbols) -> list[str]:
    """
    Return the subset of *symbols* whose instrument_type is still 'unknown'.
    Never blocks — memory read only.
    """
    if not _LOADED:
        _load_disk()
    return [s.upper() for s in symbols if _MEM.get(s.upper(), "unknown") == "unknown"]


# ── DB warm-up (sync, safe to call at startup) ───────────────────────────────

def warm_up_from_db(symbols: list[str] | None = None) -> int:
    """
    Populate _MEM from screener_fundamentals_cache.profile_json.isEtf.
    Called at startup.  Returns count of newly classified symbols.
    """
    if not _LOADED:
        _load_disk()
    try:
        from data.pg_storage import _get_conn, _put_conn
        conn = _get_conn()
        cur = conn.cursor()
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
            if _MEM.get(sym) in ("stock", "etf"):
                continue  # already classified — don't overwrite known
            d = (json.loads(pj) if isinstance(pj, str) else pj) or {}
            is_etf = d.get("isEtf")
            if is_etf is True:
                _set(sym, "etf")
                count += 1
            elif is_etf is False:
                _set(sym, "stock")
                count += 1
            elif is_etf is None:
                # FMP returned null for isEtf — infer from other profile fields.
                # ETFs never carry a fundamental sector/industry classification;
                # any non-empty sector value is a strong stock indicator.
                sector = (d.get("sector") or "").strip()
                if sector:
                    _set(sym, "stock")
                    count += 1

        if count > 0:
            _save_disk()
        return count
    except Exception as e:
        print(f"[INSTRUMENT_TYPE] warm_up_from_db: {e}")
        return 0


# ── FMP background classification ─────────────────────────────────────────────

async def classify_symbols_background(
    symbols: list[str],
    fmp_provider=None,
    *,
    rate_limit_sleep: float = 0.35,
    max_per_pass: int = 20,
) -> int:
    """
    Classify symbols not yet in _MEM (or classified as "unknown") using
    FMP /v3/profile.

    MUST only be called from background tasks, never from API request handlers.

    Parameters
    ----------
    symbols          : list of candidate symbols to classify
    fmp_provider     : FMPProvider instance  (skips if None)
    rate_limit_sleep : seconds between FMP calls
    max_per_pass     : max symbols to classify in one call to avoid runaway

    Returns count of newly classified symbols.
    """
    if not _LOADED:
        _load_disk()

    missing = [
        s.upper() for s in symbols
        if _MEM.get(s.upper()) not in ("stock", "etf")
    ][:max_per_pass]

    if not missing or fmp_provider is None:
        return 0

    count = 0
    for sym in missing:
        try:
            itype = await fmp_provider.get_etf_flag(sym)
            if itype in ("stock", "etf"):
                _set(sym, itype)
                count += 1
            await asyncio.sleep(rate_limit_sleep)
        except Exception as e:
            print(f"[INSTRUMENT_TYPE] classify {sym}: {e}")
            continue

    if count > 0:
        _save_disk()
        print(f"[INSTRUMENT_TYPE] classified {count} new symbols; total={len(_MEM)}")
    return count


# ── load on import ────────────────────────────────────────────────────────────
_load_disk()
