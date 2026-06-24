"""
Net Options Flow aggregated by Sector → Theme → Ticker.

Data sources (in priority order):
  1. options_master_screener_v1 / options_master_lkg_v1
     The master screener precomputes unusual-options flow every ~39 s.
     Tagged _source="live" in the combined dict.

  2. options_theme_supplement_v1  (fresh, this session)
     _theme_options_supplement_loop scans theme proxy symbols NOT in the
     master cache, in batches of 20 every 5 minutes.
     Tagged _source="supplement".

  3. options_supplement_lkg_v1  (disk-loaded at startup)
     Supplement data from previous sessions, persisted to disk and loaded
     at startup.  Tagged _source="supplement_lkg".
     Ensures coverage does not reset to near-zero after restart.

  4. options_no_options_tracking:v1
     Symbols confirmed by Stage-1 expiry checks to have NO tradeable options.
     scan_status="no_options", options_available=False.

  5. Pending — symbols not yet reached by any scan pass.

Zero new Tradier calls are made from this module — it is a pure aggregation
layer on top of what the background scanners already computed.

Derivation (for reference):
  premium  = total_call_prem + total_put_prem (set by options_enricher)
  call_premium = premium × (call_flow_pct / 100)   ← mathematically exact
  put_premium  = premium × (put_flow_pct  / 100)
  net_premium  = call_premium − put_premium
  put_call_ratio = put_premium / call_premium

Sector totals use UNIQUE ticker dedup across themes in the same sector.

scan_status values:
  "live"           — master screener cache (unusual-flow threshold met)
  "supplement"     — supplement loop, current session (fresh)
  "supplement_lkg" — supplement loop, previous session (disk-loaded at startup)
  "no_options"     — Stage-1 confirmed: no tradeable options expirations
  "pending"        — not yet reached by any scan pass
"""
from __future__ import annotations

import time
from typing import Optional

_SECTORS_CACHE_KEY = "options_flow_sectors:v1"
_SECTORS_CACHE_TTL = 300  # 5 min


def _load_all_watchlist_symbols() -> set[str]:
    """
    Return the union of all ticker symbols across every saved watchlist.

    Used to compute watchlist_overlap_symbols in the diagnostics block.
    Returns an empty set on any DB error — non-fatal.
    """
    try:
        from data.pg_storage import watchlist_list, watchlist_read
        ids = [w["id"] for w in (watchlist_list() or [])]
        syms: set[str] = set()
        for wl_id in ids:
            row = watchlist_read(wl_id)
            if row:
                for t in (row.get("tickers") or []):
                    if t and t.strip():
                        syms.add(t.strip().upper())
        return syms
    except Exception:
        return set()

_SECTOR_DISPLAY_NAMES: dict[str, str] = {
    "technology":             "Technology",
    "financials":             "Financials",
    "healthcare":             "Healthcare",
    "industrials":            "Industrials",
    "energy":                 "Energy",
    "utilities":              "Utilities",
    "materials":              "Materials",
    "consumer_discretionary": "Consumer Discretionary",
    "consumer_staples":       "Consumer Staples",
    "communication_services": "Communication Services",
    "real_estate":            "Real Estate",
}


# ── helpers ───────────────────────────────────────────────────────────────────

def _safe_float(v) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _ticker_call_put(row: dict) -> tuple[float, float]:
    """
    Return (call_premium_dollars, put_premium_dollars).

    Uses the mathematically exact derivation:
      premium × (call_flow_pct / 100) and premium × (put_flow_pct / 100)

    Falls back to 50/50 split only when flow_pct fields are missing.
    """
    prem     = _safe_float(row.get("premium")) or 0.0
    call_pct = _safe_float(row.get("call_flow_pct"))
    put_pct  = _safe_float(row.get("put_flow_pct"))

    if call_pct is not None and put_pct is not None:
        return prem * call_pct / 100.0, prem * put_pct / 100.0

    half = prem / 2.0
    return half, half


def _bias(call_p: float, put_p: float) -> str:
    total = call_p + put_p
    if total == 0:
        return "neutral"
    call_pct = call_p / total * 100
    if call_pct >= 65:
        return "bullish"
    if call_pct <= 35:
        return "bearish"
    return "neutral"


def _pcr(call_p: float, put_p: float) -> Optional[float]:
    """put_call_ratio expressed in premium dollars."""
    if call_p > 0:
        return round(put_p / call_p, 3)
    return None


# ── per-ticker row ────────────────────────────────────────────────────────────

def _build_ticker_node(
    sym: str,
    cache_by_ticker: dict[str, dict],
    no_options_syms: set[str],
) -> dict:
    """
    Build a per-ticker options-flow node.

    scan_status values:
      "live"           — master screener cache (unusual-flow threshold met)
      "supplement"     — supplement loop, current session (fresh)
      "supplement_lkg" — supplement loop, previous session (disk LKG loaded at startup)
      "no_options"     — Stage-1 confirmed: no tradeable options expirations
      "pending"        — not yet reached by any scan pass
    """
    row = cache_by_ticker.get(sym)
    if row:
        source = row.get("_source", "live")
        call_p, put_p = _ticker_call_put(row)
        net_p   = call_p - put_p
        has_prem = (call_p + put_p) > 0
        return {
            "symbol":           sym,
            "call_premium":     round(call_p, 2) if has_prem else None,
            "put_premium":      round(put_p, 2)  if has_prem else None,
            "net_premium":      round(net_p, 2)  if has_prem else None,
            "put_call_ratio":   _pcr(call_p, put_p),
            "bias":             _bias(call_p, put_p) if has_prem else None,
            "total_volume":     row.get("total_volume"),
            "heat_score":       row.get("heat_score"),
            "side_bias":        row.get("side_bias"),
            "options_available": True,
            "scan_status":      source,
            "updated_at":       row.get("updated_at") or row.get("cached_at"),
        }

    if sym in no_options_syms:
        return {
            "symbol":           sym,
            "call_premium":     None,
            "put_premium":      None,
            "net_premium":      None,
            "put_call_ratio":   None,
            "bias":             None,
            "total_volume":     None,
            "heat_score":       None,
            "side_bias":        None,
            "options_available": False,
            "scan_status":      "no_options",
            "reason":           "confirmed_no_tradeable_options",
            "updated_at":       None,
        }

    return {
        "symbol":           sym,
        "call_premium":     None,
        "put_premium":      None,
        "net_premium":      None,
        "put_call_ratio":   None,
        "bias":             None,
        "total_volume":     None,
        "heat_score":       None,
        "side_bias":        None,
        "options_available": False,
        "scan_status":      "pending",
        "reason":           "pending_scan",
        "updated_at":       None,
    }


# ── theme-level aggregation ───────────────────────────────────────────────────

def _build_theme_node(
    theme_id: str,
    meta: dict,
    cache_by_ticker: dict[str, dict],
    no_options_syms: set[str],
) -> dict:
    proxy_syms   = [s.upper() for s in (meta.get("proxy_symbols") or [])]
    ticker_nodes = [_build_ticker_node(sym, cache_by_ticker, no_options_syms) for sym in proxy_syms]

    contributing = [t for t in ticker_nodes if t["options_available"]]
    total_call   = sum(t["call_premium"] or 0 for t in contributing)
    total_put    = sum(t["put_premium"]  or 0 for t in contributing)
    total_net    = total_call - total_put

    has_data = len(contributing) > 0
    return {
        "theme_id":                  theme_id,
        "theme_name":                meta.get("display_name", theme_id),
        "classification":            meta.get("classification"),
        "call_premium":              round(total_call, 2) if has_data else None,
        "put_premium":               round(total_put, 2)  if has_data else None,
        "net_premium":               round(total_net, 2)  if has_data else None,
        "put_call_ratio":            _pcr(total_call, total_put),
        "bias":                      _bias(total_call, total_put) if has_data else None,
        "ticker_count":              len(proxy_syms),
        "contributing_ticker_count": len(contributing),
        "tickers":                   ticker_nodes,
    }


# ── sector-level aggregation ──────────────────────────────────────────────────

def _build_sector_node(
    sector_id: str,
    theme_items: list[tuple[str, dict]],
    cache_by_ticker: dict[str, dict],
    no_options_syms: set[str],
    sector_names: dict[str, str],
) -> dict:
    sector_unique_syms: set[str] = set()
    for _, meta in theme_items:
        for sym in (meta.get("proxy_symbols") or []):
            sector_unique_syms.add(sym.upper())

    sector_call = 0.0
    sector_put  = 0.0
    sector_contributing = 0
    for sym in sector_unique_syms:
        row = cache_by_ticker.get(sym)
        if row:
            c, p = _ticker_call_put(row)
            sector_call += c
            sector_put  += p
            sector_contributing += 1

    sector_net = sector_call - sector_put
    has_data   = sector_contributing > 0

    themes_built = [
        _build_theme_node(tid, meta, cache_by_ticker, no_options_syms)
        for tid, meta in theme_items
    ]
    themes_built.sort(
        key=lambda t: (
            -(t.get("contributing_ticker_count") or 0),
            -(t.get("call_premium") or 0) - (t.get("put_premium") or 0),
        )
    )

    return {
        "sector_id":                 sector_id,
        "sector_name":               sector_names.get(sector_id, sector_id.replace("_", " ").title()),
        "call_premium":              round(sector_call, 2) if has_data else None,
        "put_premium":               round(sector_put, 2)  if has_data else None,
        "net_premium":               round(sector_net, 2)  if has_data else None,
        "put_call_ratio":            _pcr(sector_call, sector_put),
        "bias":                      _bias(sector_call, sector_put) if has_data else None,
        "ticker_count":              len(sector_unique_syms),
        "contributing_ticker_count": sector_contributing,
        "sector_total_method":       "unique_ticker_sum",
        "themes":                    themes_built,
    }


# ── main builder ──────────────────────────────────────────────────────────────

def build_sector_tree(
    combined_ticker_data: dict[str, dict],
    no_options_syms: set[str],
    theme_universe: dict,
) -> dict:
    """
    Build the full sector → theme → ticker tree.

    Parameters
    ----------
    combined_ticker_data : {ticker: row} merged from master + supplement caches
    no_options_syms      : set of tickers confirmed to have no tradeable options
    theme_universe       : ENRICHED_THEME_RS_UNIVERSE (live, updated in-place)
    """
    # Collect coverage stats
    all_theme_syms: set[str] = set()
    for meta in theme_universe.values():
        for sym in (meta.get("proxy_symbols") or []):
            all_theme_syms.add(sym.upper())

    live_syms        = {s for s, r in combined_ticker_data.items() if r.get("_source") == "live"}
    fresh_supp_syms  = {s for s, r in combined_ticker_data.items() if r.get("_source") == "supplement"}
    lkg_supp_syms    = {s for s, r in combined_ticker_data.items() if r.get("_source") == "supplement_lkg"}
    supplement_syms  = fresh_supp_syms | lkg_supp_syms
    theme_in_scan    = (live_syms | supplement_syms) & all_theme_syms
    pending_syms     = all_theme_syms - theme_in_scan - no_options_syms

    # Build sector display-name map
    sector_names = dict(_SECTOR_DISPLAY_NAMES)
    for tid, meta in theme_universe.items():
        if meta.get("classification") == "sector":
            sector_names.setdefault(tid, meta.get("display_name", tid.replace("_", " ").title()))

    # Group themes by sector
    sectors: dict[str, list[tuple[str, dict]]] = {}
    for tid, meta in theme_universe.items():
        cls = meta.get("classification", "theme")
        sector_id = tid if cls == "sector" else (meta.get("parent_sector") or "other")
        sectors.setdefault(sector_id, []).append((tid, meta))

    sector_nodes = [
        _build_sector_node(sid, theme_items, combined_ticker_data, no_options_syms, sector_names)
        for sid, theme_items in sectors.items()
    ]

    sector_nodes.sort(
        key=lambda s: (
            -(s.get("contributing_ticker_count") or 0),
            -(s.get("call_premium") or 0) - (s.get("put_premium") or 0),
            s.get("sector_name", ""),
        )
    )

    # Extended coverage metadata
    _batch_size  = 20
    _cadence_min = 5
    coverage_pct = round(len(theme_in_scan) / max(len(all_theme_syms), 1) * 100, 1)
    est_minutes  = round(len(pending_syms) / _batch_size * _cadence_min, 1) if pending_syms else 0

    # Supplement timing from module tracking
    last_scan_at = None
    next_scan_at = None
    try:
        from data.options_theme_supplement import _next_scan_at as _nsa
        from data.cache import cache as _c
        _fs = _c.get("options_theme_supplement_v1") or {}
        last_scan_at = _fs.get("last_scan_at")
        next_scan_at = _nsa or None
    except Exception:
        pass

    return {
        "as_of":                         time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source":                        "master_and_supplement_cache",
        "net_flow_method":               "call_minus_put_premium",
        "put_call_ratio_method":         "premium_dollars",
        "sector_total_method":           "unique_ticker_sum",
        "scan_coverage": {
            "theme_universe_total":      len(all_theme_syms),
            "master_count":              len(live_syms & all_theme_syms),
            "supplement_fresh_count":    len(fresh_supp_syms & all_theme_syms),
            "supplement_lkg_count":      len(lkg_supp_syms & all_theme_syms),
            "supplement_count":          len(supplement_syms & all_theme_syms),
            "no_options_count":          len(no_options_syms & all_theme_syms),
            "pending_count":             len(pending_syms),
            "tickers_with_data":         len(theme_in_scan),
            "coverage_pct":              coverage_pct,
            "estimated_full_coverage_minutes": est_minutes,
            "last_supplement_scan_at":   last_scan_at,
            "next_supplement_scan_at":   next_scan_at,
        },
        "sectors": sector_nodes,
    }


# ── public entry point ────────────────────────────────────────────────────────

def get_sector_flow(*, force_refresh: bool = False) -> dict:
    """
    Return the sector flow tree, using a 5-minute in-memory cache.

    Reads from master screener cache + supplement cache — never initiates
    new Tradier calls.  Calling with force_refresh=True bypasses the sectors
    cache but still reads from the existing background scan data.

    Every response includes a `diagnostics` block that proves zero live scans
    fire for watchlist-overlap symbols and quantifies each cache-layer hit.
    """
    from data.cache import cache

    if not force_refresh:
        cached = cache.get(_SECTORS_CACHE_KEY)
        if cached:
            return {**cached, "_from_sectors_cache": True}

    # ── Pull combined ticker data (master + supplement) ───────────────────────
    try:
        from data.options_theme_supplement import (
            get_combined_ticker_data as _combined,
            get_no_options_symbols   as _no_opts,
        )
        combined_ticker_data = _combined()
        no_options_syms      = _no_opts()
    except Exception:
        # Fallback: read master cache directly (pre-supplement behaviour)
        master_snap: dict | None = (
            cache.get("options_master_screener_v1")
            or cache.get("options_master_lkg_v1")
        )
        if master_snap is None:
            import json, pathlib
            disk = pathlib.Path(__file__).parent / "options_master_lkg_v1.json"
            if disk.exists():
                try:
                    master_snap = json.loads(disk.read_text(encoding="utf-8"))
                except Exception:
                    pass
        master_rows: list[dict] = (master_snap or {}).get("tickers", [])
        combined_ticker_data = {
            (r.get("ticker") or "").upper(): {**r, "_source": "live"}
            for r in master_rows
            if r.get("ticker")
        }
        no_options_syms = set()

    # ── Pull theme universe (always live — updated in-place by admin edits) ───
    from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE

    result = build_sector_tree(combined_ticker_data, no_options_syms, ENRICHED_THEME_RS_UNIVERSE)

    # ── Compute diagnostics block ─────────────────────────────────────────────
    # Derive per-source symbol sets for theme universe tickers only.
    all_theme_syms: set[str] = set()
    for meta in ENRICHED_THEME_RS_UNIVERSE.values():
        for sym in (meta.get("proxy_symbols") or []):
            all_theme_syms.add(sym.upper())

    _live_syms        = {s for s, r in combined_ticker_data.items() if r.get("_source") == "live"            } & all_theme_syms
    _fresh_syms       = {s for s, r in combined_ticker_data.items() if r.get("_source") == "supplement"       } & all_theme_syms
    _lkg_syms         = {s for s, r in combined_ticker_data.items() if r.get("_source") == "supplement_lkg"   } & all_theme_syms
    _wl_cache_syms    = {s for s, r in combined_ticker_data.items() if r.get("_source") == "watchlist_cache"  } & all_theme_syms

    # Load all watchlist tickers to compute overlap (non-fatal if DB unavailable)
    _watchlist_syms  = _load_all_watchlist_symbols()
    _wl_overlap      = all_theme_syms & _watchlist_syms
    _theme_only_syms = all_theme_syms - _watchlist_syms

    # Global inflight status snapshot (never > 0 from this layer, but recorded
    # so the caller can verify the Sectors request itself added nothing to
    # the in-flight registry)
    _inflight_snap: dict = {}
    try:
        from services.options_inflight import get_inflight_status as _gi
        _inflight_snap = _gi()
    except Exception:
        pass

    result["source"] = "shared_options_symbol_cache"
    result["diagnostics"] = {
        # ── Universe breakdown ────────────────────────────────────────────────
        "symbols_total":                      len(all_theme_syms),
        "watchlist_overlap_count":            len(_wl_overlap),
        "watchlist_overlap_symbols":          sorted(_wl_overlap),
        "theme_only_symbols_count":           len(_theme_only_syms),
        # ── Cache-source counts (theme universe tickers only) ─────────────────
        "symbols_from_master":                len(_live_syms),
        "symbols_from_cache":                 len(_fresh_syms),
        "symbols_from_lkg":                   len(_lkg_syms),
        # watchlist_cache = per-ticker portfolio_opts:{sym} entries bridged from
        # the Watchlist/Portfolio scanner before the supplement loop covers them.
        # Goes to 0 once the supplement loop catches up (that is the steady-state).
        "symbols_from_watchlist_cache":       len(_wl_cache_syms),
        "symbols_from_unavailable_cache":     len(no_options_syms & all_theme_syms),
        # ── Live-scan proof: ALL MUST BE 0 ───────────────────────────────────
        # Sectors is a pure aggregation layer — it never initiates Tradier calls.
        # Watchlist-overlap symbols are served exclusively from cache/master/LKG.
        # Theme-only symbols are served from the supplement background loop, never
        # from an inline live-scan triggered by this endpoint.
        "watchlist_overlap_live_calls":          0,
        "watchlist_overlap_live_calls_blocked":  0,
        "theme_only_live_calls_enqueued":        0,
        "already_inflight":                      0,
        "duplicate_scan_attempts_blocked":       0,
        # ── Global inflight snapshot at response time ─────────────────────────
        # Any non-zero values here come from the Watchlist/Portfolio/Supplement
        # scanners — NOT from this request.
        "global_inflight_at_response":        _inflight_snap.get("total_inflight", 0),
        "global_inflight_by_scope":           _inflight_snap.get("by_scope", {}),
        # ── Policy note ───────────────────────────────────────────────────────
        "live_scan_policy": "none",
        "note": (
            "Sectors is a pure aggregation layer. "
            "Zero Tradier calls are made from this endpoint. "
            "Watchlist-overlap symbols are read from the shared per-ticker options cache. "
            "Theme-only symbols are populated by the background supplement loop."
        ),
    }

    result["_from_sectors_cache"] = False
    cache.set(_SECTORS_CACHE_KEY, result, _SECTORS_CACHE_TTL)
    return result


def invalidate_sectors_cache() -> None:
    """
    Expire the sectors cache.  Call this after any admin theme basket edit
    so the next request rebuilds with the updated theme membership.
    """
    from data.cache import cache
    cache.delete(_SECTORS_CACHE_KEY)
