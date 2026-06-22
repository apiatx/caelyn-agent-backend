"""
GET /api/options-flow/sectors
GET /api/options-flow/sectors/debug

Net Options Flow aggregated by Sector → Theme → Ticker.

Zero new Tradier calls — reads exclusively from the existing master
screener cache populated by the background scanner.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Header, Query, Request
from fastapi.responses import JSONResponse

router = APIRouter(tags=["options_flow"])


def _require_subscription(request: Request):
    """Minimal subscription gate — mirrors the pattern used in main.py."""
    from main import require_subscription  # type: ignore[import]
    return None


@router.get("/api/options-flow/sectors")
async def options_flow_sectors(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
    refresh: bool = Query(False, description="Force-expire the sectors cache and rebuild from master screener data"),
):
    """
    Net Options Flow aggregated by Sector → Theme → Ticker.

    Hierarchy:
      sectors[]
        └─ themes[]
             └─ tickers[]

    Premium derivation (no new Tradier calls):
      call_premium = master_row.premium × (call_flow_pct / 100)
      put_premium  = master_row.premium × (put_flow_pct / 100)
      net_premium  = call_premium − put_premium
      put_call_ratio = put_premium / call_premium  (premium-dollars, not contract count)

    Sector totals use UNIQUE ticker dedup across themes.
    Theme totals include every ticker in the basket, even if it also
    appears in a sibling theme.

    Tickers not yet scanned by the options scanner are included with
    options_available=false and null premium fields.
    """
    try:
        from data.options_flow_sectors import get_sector_flow
        payload = get_sector_flow(force_refresh=refresh)
        return JSONResponse(content=payload)
    except Exception as exc:
        import traceback
        return JSONResponse(
            status_code=500,
            content={
                "error": "sector_flow_build_failed",
                "detail": str(exc),
                "trace": traceback.format_exc()[-2000:],
            },
        )


@router.get("/api/options-flow/sectors/debug")
async def options_flow_sectors_debug(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """
    Diagnostics for the sectors aggregation.

    Returns scan coverage broken down by master / supplement / no_options /
    pending, plus the supplement module stats and sectors cache TTL status.
    """
    try:
        from data.cache import cache
        from data.options_theme_supplement import (
            get_supplement_stats,
            get_no_options_symbols,
            get_theme_only_symbols_for_supplement,
        )
        from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE

        master_snap = (
            cache.get("options_master_screener_v1")
            or cache.get("options_master_lkg_v1")
        )
        master_tickers: set[str] = set()
        if master_snap:
            master_tickers = {
                (r.get("ticker") or "").upper()
                for r in master_snap.get("tickers", [])
                if r.get("ticker")
            }

        theme_tickers: set[str] = set()
        for meta in ENRICHED_THEME_RS_UNIVERSE.values():
            for sym in (meta.get("proxy_symbols") or []):
                theme_tickers.add(sym.upper())

        no_opts   = get_no_options_symbols()
        pending   = get_theme_only_symbols_for_supplement()
        supp_stat = get_supplement_stats()

        return JSONResponse(content={
            "master_cache_warm":             master_snap is not None,
            "master_cache_source":           (master_snap or {}).get("source", "unknown"),
            "sectors_cache_warm":            cache.get("options_flow_sectors:v1") is not None,
            "theme_count":                   len(ENRICHED_THEME_RS_UNIVERSE),
            "scan_coverage": {
                "master_scan_tickers":       len(master_tickers),
                "theme_universe_total":      len(theme_tickers),
                "tickers_in_both":           len(master_tickers & theme_tickers),
                "no_options_confirmed":      len(no_opts & theme_tickers),
                "pending_supplement_scan":   len(pending),
                "supplement_scanned":        supp_stat.get("supplement_scanned_count", 0),
                "supplement_last_scan_at":   supp_stat.get("supplement_last_scan_at"),
                "extra_theme_seeds_for_inject": supp_stat.get("extra_theme_seeds_for_inject", 0),
            },
            "no_options_confirmed_symbols":  sorted(no_opts & theme_tickers),
            "pending_supplement_symbols":    pending,
            "options_scan_not_in_any_theme": sorted(master_tickers - theme_tickers),
        })
    except Exception as exc:
        import traceback
        return JSONResponse(
            status_code=500,
            content={
                "error": "debug_failed",
                "detail": str(exc),
                "trace": traceback.format_exc()[-2000:],
            },
        )
