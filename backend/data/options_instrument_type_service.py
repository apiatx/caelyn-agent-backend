"""
Persistent ETF vs stock classification for the options flow universe.

Classification hierarchy (in priority order):
  1. In-memory LKG (loaded from disk at import time)
  2. screener_fundamentals_cache.profile_json.isEtf  (FMP data in Neon DB)
     - explicit True  → etf   (source: fmp_is_etf)
     - explicit False → stock (source: fmp_is_etf)
     - None + non-empty sector → stock (source: sector_inference) [TEMPORARY]
  3. FMP /stable/profile  — background-only, never during request handling
     (source: fmp_profile)
  4. unresolved — classification not yet available

Returns:
  "etf"     — confirmed ETF / fund / closed-end fund
  "stock"   — confirmed equity (common stock or similar)
  "unknown" — classification not yet available

instrument_type_source values (per symbol):
  "fmp_is_etf"       — isEtf True/False in screener_fundamentals_cache
  "sector_inference" — isEtf=None, non-empty FMP sector → inferred stock [TEMPORARY]
  "fmp_profile"      — resolved via FMP /stable/profile background call
  "lkg"              — loaded from persisted LKG (source not re-derived on load)
  "unresolved"       — not yet classified

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
_MEM: dict[str, str] = {}        # symbol → "stock" | "etf" | "unknown"
_MEM_SOURCE: dict[str, str] = {} # symbol → source string
_LOADED = False
_SAVE_NEEDED = False

# Stamp so we know when the LKG was last populated
_LAST_SAVE_AT: float = 0.0


# ── Disk persistence ──────────────────────────────────────────────────────────

def _load_disk() -> None:
    global _MEM, _MEM_SOURCE, _LOADED, _LAST_SAVE_AT
    try:
        if _LKG_PATH.exists():
            with open(_LKG_PATH) as f:
                d = json.load(f)
            if isinstance(d, dict):
                sources = d.get("_sources", {})
                for k, v in d.items():
                    if k.startswith("_"):
                        continue
                    if v in ("stock", "etf", "unknown"):
                        _MEM[k.upper()] = v
                        _MEM_SOURCE[k.upper()] = sources.get(k.upper(), "lkg")
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
        payload["_sources"]  = dict(_MEM_SOURCE)
        tmp = str(_LKG_PATH) + ".tmp"
        with open(tmp, "w") as f:
            json.dump(payload, f)
        os.replace(tmp, _LKG_PATH)
        _LAST_SAVE_AT = payload["_saved_at"]
        _SAVE_NEEDED = False
    except Exception as e:
        print(f"[INSTRUMENT_TYPE] disk save failed: {e}")


def _set(symbol: str, itype: str, source: str = "lkg") -> None:
    global _SAVE_NEEDED
    sym = symbol.upper()
    if sym.startswith("_"):
        return
    if _MEM.get(sym) != itype or _MEM_SOURCE.get(sym) != source:
        _MEM[sym] = itype
        _MEM_SOURCE[sym] = source
        _SAVE_NEEDED = True


# ── Public read API (memory-only, always fast) ────────────────────────────────

def get_instrument_type(symbol: str) -> str:
    """Return 'stock', 'etf', or 'unknown'. Never blocks."""
    if not _LOADED:
        _load_disk()
    return _MEM.get(symbol.upper(), "unknown")


def get_instrument_type_source(symbol: str) -> str:
    """
    Return the classification source for a symbol.

    Values: fmp_is_etf | sector_inference | fmp_profile | lkg | unresolved
    Never blocks — memory read only.
    """
    if not _LOADED:
        _load_disk()
    sym = symbol.upper()
    if sym not in _MEM:
        return "unresolved"
    return _MEM_SOURCE.get(sym, "lkg")


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
    inferred = sum(1 for s, src in _MEM_SOURCE.items()
                   if src == "sector_inference" and _MEM.get(s) == "stock")
    return {
        "total":             len(_MEM),
        "stocks":            stocks,
        "etfs":              etfs,
        "unknown":           unknown,
        "sector_inferred":   inferred,
        "unknown_sample":    unknown_syms,
        "saved_at":          _LAST_SAVE_AT or None,
        "updated_at":        _LAST_SAVE_AT or None,
    }


def get_required_universe_classification_stats(required_symbols) -> dict:
    """
    Compute classification statistics against the ACTUAL required universe.

    A symbol is "unresolved" if it is absent from _MEM OR if its value is
    "unknown". This is the correct required-universe metric, not the count
    of symbols explicitly stored as "unknown" in the LKG (which excludes
    symbols absent from the cache altogether).

    Parameters
    ----------
    required_symbols : iterable of symbol strings (the canonical required set)

    Returns
    -------
    dict with required_total, required_stock_tickers, required_etf_tickers,
    unresolved_instrument_type_tickers, unresolved_instrument_type_sample,
    sector_inferred_tickers, classification_coverage_pct,
    classification_cache_updated_at.
    """
    if not _LOADED:
        _load_disk()
    syms = [s.upper() for s in required_symbols]
    required_total  = len(syms)
    stock_count     = 0
    etf_count       = 0
    inferred_count  = 0
    unresolved_syms: list[str] = []

    for sym in syms:
        itype = _MEM.get(sym, "unknown")
        src   = _MEM_SOURCE.get(sym, "unresolved")
        if itype == "stock":
            stock_count += 1
            if src == "sector_inference":
                inferred_count += 1
        elif itype == "etf":
            etf_count += 1
        else:
            unresolved_syms.append(sym)

    resolved = stock_count + etf_count
    coverage_pct = round(resolved / max(required_total, 1) * 100, 1)

    return {
        "required_total":                   required_total,
        "required_stock_tickers":           stock_count,
        "required_etf_tickers":             etf_count,
        "unresolved_instrument_type_tickers": len(unresolved_syms),
        "unresolved_instrument_type_sample":  sorted(unresolved_syms)[:20],
        "sector_inferred_tickers":          inferred_count,
        "classification_coverage_pct":      coverage_pct,
        "classification_cache_updated_at":  _LAST_SAVE_AT or None,
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

    Source tagging:
      isEtf True/False (explicit)  → source = "fmp_is_etf"
      isEtf=None, non-empty sector → source = "sector_inference"  [TEMPORARY]
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
                # Already classified with authority — skip unless sector_inference
                # (an explicit isEtf value is more authoritative than sector inference)
                existing_src = _MEM_SOURCE.get(sym, "lkg")
                if existing_src != "sector_inference":
                    continue
            d = (json.loads(pj) if isinstance(pj, str) else pj) or {}
            is_etf = d.get("isEtf")
            if is_etf is True:
                _set(sym, "etf",   source="fmp_is_etf")
                count += 1
            elif is_etf is False:
                _set(sym, "stock", source="fmp_is_etf")
                count += 1
            elif is_etf is None:
                # FMP returned null for isEtf — infer from other profile fields.
                # ETFs never carry a fundamental sector/industry classification;
                # any non-empty sector value is a strong stock indicator.
                # This is a TEMPORARY inference — superseded by explicit fmp_profile.
                sector = (d.get("sector") or "").strip()
                if sector:
                    _set(sym, "stock", source="sector_inference")
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
    Classify symbols not yet in _MEM (or classified as "unknown" or
    "sector_inference") using FMP /stable/profile.

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

    # Prioritise symbols that are genuinely missing OR only sector_inferred
    # (sector_inference is temporary and should be upgraded to fmp_profile when possible).
    missing = [
        s.upper() for s in symbols
        if _MEM.get(s.upper()) not in ("stock", "etf")
        or _MEM_SOURCE.get(s.upper()) == "sector_inference"
    ][:max_per_pass]

    if not missing or fmp_provider is None:
        return 0

    count = 0
    for sym in missing:
        try:
            itype = await fmp_provider.get_etf_flag(sym)
            if itype in ("stock", "etf"):
                _set(sym, itype, source="fmp_profile")
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
