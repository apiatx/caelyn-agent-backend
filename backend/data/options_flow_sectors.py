"""
Net Options Flow aggregated by Sector → Theme → Ticker.

Reads exclusively from the existing options master screener cache
(options_master_screener_v1 / options_master_lkg_v1).  Zero new Tradier
calls are made — this is a pure aggregation layer on top of what the
background scanner already computed.

Derivation:
  - premium (row field) = total_call_prem + total_put_prem (summed over
    all top_contracts, split by contract side).  This is set by
    options_enricher._build_enriched_fields.
  - call_flow_pct = total_call_prem / premium * 100  (exact, not rounded)
  - Therefore:
      call_premium = premium × (call_flow_pct / 100)   ← mathematically exact
      put_premium  = premium × (put_flow_pct  / 100)   ← mathematically exact
  - net_premium  = call_premium − put_premium
  - put_call_ratio = put_premium / call_premium

Sector totals use UNIQUE ticker dedup across themes in the same sector so
a ticker that appears in two themes (e.g. NVDA in ai_networking AND datacenter_infra)
is only counted once in the sector total.

Theme totals include the ticker in every theme it belongs to (mirrors the
theme basket display in the frontend).
"""
from __future__ import annotations

import time
from typing import Optional

_SECTORS_CACHE_KEY = "options_flow_sectors:v1"
_SECTORS_CACHE_TTL = 300  # 5 min — matches master screener hot-cache TTL

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


# ── helpers ──────────────────────────────────────────────────────────────────

def _safe_float(v) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _ticker_call_put(row: dict) -> tuple[float, float]:
    """
    Return (call_premium_dollars, put_premium_dollars) for a master cache row.

    Uses the mathematically exact derivation:
      premium × (call_flow_pct / 100) and premium × (put_flow_pct / 100)

    where `premium` = total_call_prem + total_put_prem (from options_enricher).
    Falls back to 50/50 split only when flow_pct fields are missing.
    """
    prem     = _safe_float(row.get("premium")) or 0.0
    call_pct = _safe_float(row.get("call_flow_pct"))
    put_pct  = _safe_float(row.get("put_flow_pct"))

    if call_pct is not None and put_pct is not None:
        return prem * call_pct / 100.0, prem * put_pct / 100.0

    # Fallback: 50/50 when enricher fields are absent
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

def _build_ticker_node(sym: str, cache_by_ticker: dict[str, dict]) -> dict:
    row = cache_by_ticker.get(sym)
    if not row:
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
            "reason":           "not_in_options_scan_universe",
            "updated_at":       None,
        }
    call_p, put_p = _ticker_call_put(row)
    net_p = call_p - put_p
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
        "updated_at":       row.get("updated_at") or row.get("cached_at"),
    }


# ── theme-level aggregation ───────────────────────────────────────────────────

def _build_theme_node(theme_id: str, meta: dict, cache_by_ticker: dict[str, dict]) -> dict:
    proxy_syms = [s.upper() for s in (meta.get("proxy_symbols") or [])]
    ticker_nodes = [_build_ticker_node(sym, cache_by_ticker) for sym in proxy_syms]

    contributing = [t for t in ticker_nodes if t["options_available"]]
    total_call = sum(t["call_premium"] or 0 for t in contributing)
    total_put  = sum(t["put_premium"]  or 0 for t in contributing)
    total_net  = total_call - total_put

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
    sector_names: dict[str, str],
) -> dict:
    # Unique ticker set for this sector (dedup across all themes within it)
    sector_unique_syms: set[str] = set()
    for _, meta in theme_items:
        for sym in (meta.get("proxy_symbols") or []):
            sector_unique_syms.add(sym.upper())

    # Sector premium: sum over unique tickers only
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
    has_data = sector_contributing > 0

    themes_built = [_build_theme_node(tid, meta, cache_by_ticker) for tid, meta in theme_items]
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
    master_rows: list[dict],
    theme_universe: dict,
) -> dict:
    """
    Build the full sector → theme → ticker tree.

    Parameters
    ----------
    master_rows    : tickers from options master screener cache
    theme_universe : ENRICHED_THEME_RS_UNIVERSE (live, updated in-place by admin edits)

    Returns a ready-to-serialize payload dict.
    """
    # Index master cache by ticker (upper-cased)
    cache_by_ticker: dict[str, dict] = {
        (r.get("ticker") or "").upper(): r
        for r in master_rows
        if r.get("ticker")
    }
    options_scan_symbols = set(cache_by_ticker.keys())

    # Collect all theme tickers for coverage stats
    all_theme_syms: set[str] = set()
    for meta in theme_universe.values():
        for sym in (meta.get("proxy_symbols") or []):
            all_theme_syms.add(sym.upper())

    reused_symbols    = options_scan_symbols & all_theme_syms
    theme_only_symbols = all_theme_syms - options_scan_symbols

    # Build sector display-name map (override static map with live universe names)
    sector_names = dict(_SECTOR_DISPLAY_NAMES)
    for tid, meta in theme_universe.items():
        if meta.get("classification") == "sector":
            sector_names.setdefault(tid, meta.get("display_name", tid.replace("_", " ").title()))

    # Group themes by their sector:
    #   - sub_themes and plain themes → parent_sector (or "other")
    #   - sector-level themes        → their own theme_id is the sector
    sectors: dict[str, list[tuple[str, dict]]] = {}
    for tid, meta in theme_universe.items():
        cls = meta.get("classification", "theme")
        if cls == "sector":
            sector_id = tid
        else:
            sector_id = meta.get("parent_sector") or "other"
        sectors.setdefault(sector_id, []).append((tid, meta))

    # Build all sectors
    sector_nodes = [
        _build_sector_node(sid, theme_items, cache_by_ticker, sector_names)
        for sid, theme_items in sectors.items()
    ]

    # Sort: sectors with live data first (contributing_ticker_count desc),
    # then by total premium desc, then alpha
    sector_nodes.sort(
        key=lambda s: (
            -(s.get("contributing_ticker_count") or 0),
            -(s.get("call_premium") or 0) - (s.get("put_premium") or 0),
            s.get("sector_name", ""),
        )
    )

    return {
        "as_of":                                  time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source":                                 "master_screener_cache",
        "net_flow_method":                        "call_minus_put_premium",
        "put_call_ratio_method":                  "premium_dollars",
        "sector_total_method":                    "unique_ticker_sum",
        "master_scan_tickers":                    len(master_rows),
        "theme_universe_tickers":                 len(all_theme_syms),
        "tickers_in_both_scan_and_themes":        len(reused_symbols),
        "theme_only_tickers_no_options_data":     len(theme_only_symbols),
        "sectors":                                sector_nodes,
    }


# ── public entry point ────────────────────────────────────────────────────────

def get_sector_flow(*, force_refresh: bool = False) -> dict:
    """
    Return the sector flow tree, using a 5-minute in-memory cache.

    Reads from the existing master screener cache — never initiates
    new Tradier calls.  Calling with force_refresh=True bypasses the
    sectors cache but still reads from the existing master screener data.
    """
    from data.cache import cache

    if not force_refresh:
        cached = cache.get(_SECTORS_CACHE_KEY)
        if cached:
            return {**cached, "_from_sectors_cache": True}

    # ── Pull master screener data ─────────────────────────────────────────────
    master_snap: dict | None = (
        cache.get("options_master_screener_v1")
        or cache.get("options_master_lkg_v1")
    )

    if master_snap is None:
        # Last resort: try disk LKG
        import json
        import pathlib
        disk = pathlib.Path(__file__).parent / "options_master_lkg_v1.json"
        if disk.exists():
            try:
                master_snap = json.loads(disk.read_text(encoding="utf-8"))
            except Exception:
                pass

    master_rows: list[dict] = (master_snap or {}).get("tickers", [])

    # ── Pull theme universe (always live — updated in-place by admin edits) ───
    from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE

    result = build_sector_tree(master_rows, ENRICHED_THEME_RS_UNIVERSE)
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
