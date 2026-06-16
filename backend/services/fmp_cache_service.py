"""
Canonical FMP cache-first interface for Caelyn AI.

Read-only: all public functions read from existing caches in priority order.
No FMP network calls are ever made from this module.

Cache priority for profile/fundamentals data:
  1. screener_fundamentals_cache (Neon DB, 7-day TTL)
     Written by: Screener Hub warm job (warm_fundamentals),
                 Social Screener enrich_fund write-through
  2. FMPProvider in-memory cache  [profile only, best-effort]
     Written by: FMPProvider._get_stable() calls in market_data_service

Rules:
  - All functions return None / [] / {} on total miss.
  - No exceptions propagate to callers — every function is try/except guarded.
  - No FMP network call is ever initiated here.
  - Import this module from page-load paths without fear of triggering FMP.
"""

from __future__ import annotations

import os
from typing import Optional

# ── Store imports (guarded so startup never fails) ───────────────────────────

def _get_fundamentals(symbols: list[str]) -> dict[str, dict]:
    try:
        from data.screener_hub_store import get_fundamentals
        return get_fundamentals(symbols)
    except Exception:
        return {}


def _get_returns(symbols: list[str]) -> dict[str, dict]:
    try:
        from data.screener_hub_store import get_returns
        return get_returns(symbols)
    except Exception:
        return {}


def _get_latest_universe(universe_type: str, theme_key: Optional[str] = None) -> Optional[dict]:
    try:
        from data.screener_hub_store import get_latest_universe
        return get_latest_universe(universe_type, theme_key)
    except Exception:
        return None


# ── In-memory cache accessor (FMPProvider uses data.cache) ───────────────────

def _inmem_cache_get(key: str):
    try:
        from data.cache import cache
        return cache.get(key)
    except Exception:
        return None


def _fmp_api_key() -> str:
    return os.getenv("FMP_API_KEY", "")


def _try_fmp_provider_profile(symbol: str) -> Optional[dict]:
    """
    Best-effort read from FMPProvider in-memory cache.
    Key format: fmp:stable:profile:{str(params)[:80]}
    Only attempted when API key is available to reconstruct the key.
    """
    key_val = _fmp_api_key()
    if not key_val:
        return None
    sym = symbol.upper()
    params_repr = str({"symbol": sym, "apikey": key_val})[:80]
    raw = _inmem_cache_get(f"fmp:stable:profile:{params_repr}")
    if raw is None:
        return None
    if isinstance(raw, list) and raw:
        item = raw[0] if isinstance(raw[0], dict) else {}
    elif isinstance(raw, dict):
        item = raw
    else:
        return None
    if not item:
        return None
    return {
        "name":       item.get("companyName") or item.get("name") or "",
        "sector":     item.get("sector") or "",
        "industry":   item.get("industry") or "",
        "market_cap": item.get("marketCap") or item.get("market_cap"),
        "avg_volume": item.get("volAvg") or item.get("avg_volume"),
        "exchange":   item.get("exchangeShortName") or item.get("exchange") or "",
        "country":    item.get("country") or "",
        "beta":       item.get("beta"),
        "source":     "fmp_provider_inmem",
    }


# ── Canonical read-through functions ─────────────────────────────────────────

def get_fundamentals_cached(symbol: str) -> Optional[dict]:
    """
    Full {profile, metrics, ratios, market_cap, sector, industry, country, exchange,
    fetched_at, expires_at} from screener_fundamentals_cache.

    Returns None on total miss (symbol not in DB cache).
    Never raises.
    """
    try:
        sym = symbol.upper()
        rows = _get_fundamentals([sym])
        return rows.get(sym)
    except Exception:
        return None


def get_company_profile_cached(symbol: str) -> Optional[dict]:
    """
    Normalized company profile dict.  Cache-only — no network calls.

    Priority:
      1. screener_fundamentals_cache (DB, 7-day TTL)  ← shared across all tabs
      2. FMPProvider in-memory cache (fmp:stable:profile:*)  ← best-effort

    Returns dict with keys: name, sector, industry, market_cap, exchange,
    country, beta, source.
    Returns None on total miss.
    Never raises.
    """
    try:
        sym = symbol.upper()

        # ── 1. DB fundamentals cache ─────────────────────────────────────────
        rows = _get_fundamentals([sym])
        if sym in rows:
            f = rows[sym]
            p = f.get("profile") or {}
            return {
                "name": (
                    p.get("companyName")
                    or p.get("name")
                    or p.get("company_name")
                    or ""
                ),
                "sector":     f.get("sector")   or p.get("sector")   or "",
                "industry":   f.get("industry") or p.get("industry") or "",
                "market_cap": f.get("market_cap") or p.get("marketCap") or p.get("market_cap"),
                "avg_volume": p.get("volAvg") or f.get("avg_volume"),
                "exchange":   f.get("exchange") or p.get("exchangeShortName") or p.get("exchange") or "",
                "country":    f.get("country")  or p.get("country")  or "",
                "beta":       p.get("beta"),
                "source":     "screener_fundamentals_cache",
            }

        # ── 2. FMPProvider in-memory cache (best-effort) ─────────────────────
        inmem = _try_fmp_provider_profile(sym)
        if inmem:
            return inmem

        return None
    except Exception:
        return None


def get_company_profiles_bulk_cached(symbols: list[str]) -> dict[str, dict]:
    """
    Bulk read of normalized company profiles from screener_fundamentals_cache.

    Returns {SYMBOL: profile_dict} for symbols found in the DB cache.
    Symbols not in cache are absent from the result.
    Never raises.
    """
    try:
        if not symbols:
            return {}
        rows = _get_fundamentals(symbols)
        result: dict[str, dict] = {}
        for sym, f in rows.items():
            p = f.get("profile") or {}
            result[sym] = {
                "name": (
                    p.get("companyName")
                    or p.get("name")
                    or p.get("company_name")
                    or ""
                ),
                "sector":     f.get("sector")   or p.get("sector")   or "",
                "industry":   f.get("industry") or p.get("industry") or "",
                "market_cap": f.get("market_cap") or p.get("marketCap") or p.get("market_cap"),
                "avg_volume": p.get("volAvg") or f.get("avg_volume"),
                "exchange":   f.get("exchange") or p.get("exchangeShortName") or p.get("exchange") or "",
                "country":    f.get("country")  or p.get("country")  or "",
                "beta":       p.get("beta"),
                "source":     "screener_fundamentals_cache",
            }
        return result
    except Exception:
        return {}


def get_screener_metadata_cached(symbol: str) -> Optional[dict]:
    """
    Full fundamentals row for a symbol — sector, industry, market_cap, beta,
    profile_json, metrics_json, ratios_json, fetched_at, expires_at.

    This is a superset of get_company_profile_cached().
    Returns None on total miss.
    Never raises.
    """
    return get_fundamentals_cached(symbol)


def get_historical_returns_cached(symbol: str) -> Optional[dict]:
    """
    Trailing return row {return_2w, return_4w, return_10w, rs_accel, ...}
    from screener_returns_cache.

    Returns None on miss.
    Never raises.
    """
    try:
        sym = symbol.upper()
        rows = _get_returns([sym])
        return rows.get(sym)
    except Exception:
        return None


def get_peers_cached(symbol: str) -> list[str]:
    """
    Peer symbols for a ticker from FMPProvider in-memory cache (stock-peers).

    Returns [] on miss.  Never raises.  No network calls.
    """
    try:
        key_val = _fmp_api_key()
        if not key_val:
            return []
        sym = symbol.upper()
        params_repr = str({"symbol": sym, "apikey": key_val})[:80]
        raw = _inmem_cache_get(f"fmp:stable:stock-peers:{params_repr}")
        if not isinstance(raw, list):
            return []
        peers: list[str] = []
        for item in raw:
            if isinstance(item, str):
                peers.append(item.upper())
            elif isinstance(item, dict):
                s = item.get("symbol") or ""
                if s:
                    peers.append(str(s).upper())
                pl = item.get("peersList") or []
                peers.extend(str(p).upper() for p in pl if p)
        return peers
    except Exception:
        return []


def get_stock_screener_snapshot_cached(theme_or_filter_key: str) -> Optional[dict]:
    """
    Latest thematic universe snapshot for a theme key from screener_universe_snapshots.

    Returns None on miss.  Never raises.
    """
    try:
        snap = _get_latest_universe("thematic", theme_or_filter_key)
        return snap or None
    except Exception:
        return None


# ── Admin / audit helper ──────────────────────────────────────────────────────

def get_profiles_by_industries(industries: list[str]) -> list[dict]:
    """
    Query screener_fundamentals_cache for all non-expired cached profiles whose
    FMP industry classification is in the given list.

    Returns list[dict] — each entry has:
        {symbol, company_name, sector, industry, market_cap, description}

    Zero FMP network calls — Neon DB only.  Never raises.
    """
    if not industries:
        return []
    try:
        import json as _json
        from data.screener_hub_store import get_fundamentals as _sh_get_fund
        # Use direct pg query via the store's _get_conn/_put_conn for industry filter
        try:
            from data.pg_storage import _get_conn, _put_conn
        except Exception:
            return []
        conn = _get_conn()
        if conn is None:
            return []
        rows_out: list[dict] = []
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT symbol, sector, industry, market_cap, profile_json
                FROM public.screener_fundamentals_cache
                WHERE industry = ANY(%s)
                  AND expires_at > NOW() - INTERVAL '30 days'
                LIMIT 500
                """,
                (list(industries),),
            )
            for row in cur.fetchall():
                sym, sector, industry, mcap, profile_json = row
                p = (
                    profile_json if isinstance(profile_json, dict)
                    else (_json.loads(profile_json) if profile_json else {})
                )
                company_name = (
                    p.get("companyName") or p.get("name")
                    or p.get("company_name") or ""
                )
                rows_out.append({
                    "symbol":       sym,
                    "company_name": company_name,
                    "sector":       sector  or "",
                    "industry":     industry or "",
                    "market_cap":   float(mcap) if mcap is not None else None,
                    "description":  p.get("description") or "",
                })
            cur.close()
        finally:
            _put_conn(conn)
        return rows_out
    except Exception as e:
        print(f"[FMP_CACHE] get_profiles_by_industries error: {e}")
        return []


def cache_coverage(symbols: list[str]) -> dict:
    """
    Coverage report for a list of symbols against screener_fundamentals_cache.

    Returns:
        total, db_cached, db_missing, uncached,
        db_cached_symbols (first 20), uncached_symbols (first 20)
    Never raises.
    """
    try:
        if not symbols:
            return {"total": 0, "db_cached": 0, "db_missing": 0, "uncached": 0}
        rows = _get_fundamentals(symbols)
        db_cached = set(rows.keys())
        syms_upper = [s.upper() for s in symbols if s]
        db_missing = [s for s in syms_upper if s not in db_cached]
        return {
            "total":              len(syms_upper),
            "db_cached":          len(db_cached),
            "db_missing":         len(db_missing),
            "uncached":           len(db_missing),
            "db_cached_symbols":  sorted(db_cached)[:20],
            "uncached_symbols":   db_missing[:20],
        }
    except Exception:
        return {"total": 0, "db_cached": 0, "db_missing": 0, "uncached": 0, "error": "cache_coverage failed"}
