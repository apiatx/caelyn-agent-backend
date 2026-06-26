"""
GET /api/options-flow/sectors
GET /api/options-flow/sectors/debug

Net Options Flow aggregated by Sector → Theme → Ticker.

Zero new Tradier calls — reads exclusively from the existing master
screener cache and supplement caches populated by background scanners.
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

    scan_status values per ticker:
      "fresh"       — real data from the current session (live master screener
                      or sectors chain summarizer run this session)
      "cached_data" — real data from a PRIOR session, loaded from disk LKG
                      on startup.  Shown as ticker_state=stale_lkg until the
                      backfill loop refreshes this ticker.
      "pending"     — budget-deferred or transient failure; will be retried
                      automatically in the next backfill cycle.
      "no_options"  — Tradier confirmed no tradeable options for this ticker.
      "missing_data"— no scan has been attempted yet (ticker in queue).

    Sector/theme premium totals include ONLY tickers with real scanned data
    (fresh or cached_data).  Deferred, missing, and no-options tickers are
    excluded from totals but appear in diagnostics.scan_coverage.
    """
    try:
        # Signal to the backfill loop that Sectors is actively being viewed.
        # This switches the loop to priority mode: larger batches, shorter
        # sleep, and the "sectors" budget lane (60 RPM vs 20 RPM maintenance).
        try:
            from data.options_theme_supplement import register_sectors_active
            register_sectors_active()
        except Exception:
            pass

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


@router.get("/api/options-flow/sectors/validate")
async def options_flow_sectors_validate(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """
    Coverage validation for the Options Flow → Sectors universe.

    Proves:
      1. Every required ticker is classified in exactly one category
         (fresh | cached_real_lkg | confirmed_no_options | deferred | missing)
      2. Zero supplement rows with scan_result=deferred_retry exist
         (these would block re-scanning by removing them from the pending queue)
      3. Sector totals are computed from real scanned rows only

    Returns valid=True if all checks pass.
    """
    try:
        from data.options_flow_sectors import validate_sectors_coverage
        result = validate_sectors_coverage()
        status_code = 200 if result.get("valid") else 422
        return JSONResponse(status_code=status_code, content=result)
    except Exception as exc:
        import traceback
        return JSONResponse(
            status_code=500,
            content={
                "error": "validate_failed",
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

    Returns:
      - scan coverage (master / supplement_fresh / supplement_lkg / no_options / pending)
      - supplement persistence status (disk path, exists, ticker count)
      - next 20 pending symbols
      - last 20 scanned symbols
      - batch size / cadence / estimated full-coverage ETA
      - sectors cache status
      - theme count and pre_ipo/VCX confirmation
    """
    try:
        from data.cache import cache
        from data.options_theme_supplement import (
            get_supplement_stats,
            get_supplement_debug_info,
            get_no_options_symbols,
            get_theme_only_symbols_for_supplement,
            _SUPPLEMENT_LKG_DISK_PATH,
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
        supp_stat = get_supplement_stats()
        debug_inf = get_supplement_debug_info()

        # Supplement cache layers
        fresh_snap = cache.get("options_theme_supplement_v1") or {}
        lkg_snap   = cache.get("options_supplement_lkg_v1") or {}
        fresh_count = len(fresh_snap.get("ticker_data", {}))
        lkg_count   = len(lkg_snap.get("ticker_data", {}))

        # Overall coverage
        all_supp_syms = (
            set(fresh_snap.get("ticker_data", {}).keys())
            | set(lkg_snap.get("ticker_data", {}).keys())
        )
        covered = (master_tickers | all_supp_syms) & theme_tickers
        pending_count = len(theme_tickers - covered - (no_opts & theme_tickers))

        return JSONResponse(content={
            "master_cache_warm":         master_snap is not None,
            "master_cache_source":       (master_snap or {}).get("source", "unknown"),
            "sectors_cache_warm":        cache.get("options_flow_sectors:v1") is not None,
            "theme_count":               len(ENRICHED_THEME_RS_UNIVERSE),
            "pre_ipo_present":           any("pre_ipo" in tid for tid in ENRICHED_THEME_RS_UNIVERSE),
            "scan_coverage": {
                "theme_universe_total":       len(theme_tickers),
                "master_count":               len(master_tickers & theme_tickers),
                "supplement_fresh_count":     fresh_count,
                "supplement_lkg_count":       lkg_count,
                "supplement_total_count":     len(all_supp_syms & theme_tickers),
                "no_options_confirmed":       len(no_opts & theme_tickers),
                "pending_count":              pending_count,
                "tickers_with_data":          len(covered),
                "coverage_pct":              round(len(covered) / max(len(theme_tickers), 1) * 100, 1),
                "estimated_full_coverage_minutes": debug_inf.get("estimated_full_coverage_minutes"),
                "supplement_last_scan_at":   fresh_snap.get("last_scan_at"),
                "next_scan_at":              debug_inf.get("next_scan_at"),
                "batch_size":                debug_inf.get("batch_size", 20),
                "cadence_seconds":           debug_inf.get("cadence_seconds", 300),
            },
            "supplement_persistence": {
                "disk_lkg_exists":           debug_inf.get("disk_lkg_exists"),
                "disk_lkg_path":             debug_inf.get("disk_lkg_path"),
                "fresh_in_memory":           fresh_count > 0,
                "lkg_in_memory":             lkg_count > 0,
                "lkg_loaded_at":             debug_inf.get("lkg_loaded_at"),
            },
            "last_20_scanned_symbols":   debug_inf.get("last_scanned_symbols", []),
            "next_20_pending_symbols":   debug_inf.get("next_pending_symbols", []),
            "no_options_confirmed_symbols": sorted(no_opts & theme_tickers),
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
