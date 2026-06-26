"""
GET /api/options-flow/sectors          — Sector → Theme → Ticker tree
GET /api/options-flow/sectors?view=themes — flat Theme → Ticker tree (same cache)
GET /api/options-flow/sectors/validate
GET /api/options-flow/sectors/validate?view=themes
GET /api/options-flow/sectors/debug

Net Options Flow aggregated views.  Both views read from the same canonical
per-ticker options cache populated by the master screener and supplement loop.
Zero new Tradier calls from either view.
"""
from __future__ import annotations

from fastapi import APIRouter, Header, Query, Request
from fastapi.responses import JSONResponse

router = APIRouter(tags=["options_flow"])


@router.get("/api/options-flow/sectors")
async def options_flow_sectors(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
    refresh: bool = Query(
        False,
        description="Force-expire the cache and rebuild from the current master screener data",
    ),
    view: str = Query(
        "sectors",
        description=(
            "sectors (default) — Sector → Theme → Ticker hierarchy with unique-ticker dedup at sector level. "
            "themes — flat Theme → Ticker list; every theme is a top-level item with no sector grouping. "
            "Both views read the same canonical per-ticker options cache."
        ),
    ),
):
    """
    Net Options Flow aggregated by view.

    view=sectors (default)
      Hierarchy: sectors[] → themes[] → tickers[]
      Sector totals use UNIQUE ticker dedup across themes in the same sector.
      Theme totals include every ticker in the basket.

    view=themes
      Hierarchy: themes[] → tickers[]
      Every theme in the canonical universe is a top-level item.
      Theme totals = proxy_symbols_sum (no cross-theme dedup at the theme level).
      Themes sorted by contributing_ticker_count desc, then total flow desc.

    Both views share:
      - Same canonical ticker cache (master screener + supplement LKG)
      - Same _build_ticker_node and _rollup_ticker_nodes aggregation engine
      - cached_data (stale_lkg) rows contribute identically to fresh rows
      - Zero new Tradier calls

    scan_status per ticker:
      fresh        — current session (live master screener or backfill run)
      cached_data  — prior session LKG loaded from disk; valid premium data
      pending      — budget-deferred or transient; retried next backfill cycle
      no_options   — Tradier confirmed no tradeable options
      missing_data — not yet reached by any scan pass
    """
    # Signal the backfill loop to stay in priority mode (sectors lane, 60 RPM).
    # Both views benefit from the same priority scan — the supplement loop
    # populates tickers consumed by both Sectors and Themes.
    try:
        from data.options_theme_supplement import register_sectors_active
        register_sectors_active()
    except Exception:
        pass

    try:
        if view == "themes":
            from data.options_flow_sectors import get_theme_flow
            payload = get_theme_flow(force_refresh=refresh)
        else:
            from data.options_flow_sectors import get_sector_flow
            payload = get_sector_flow(force_refresh=refresh)
        return JSONResponse(content=payload)
    except Exception as exc:
        import traceback
        return JSONResponse(
            status_code=500,
            content={
                "error": f"{view}_flow_build_failed",
                "detail": str(exc),
                "trace": traceback.format_exc()[-2000:],
            },
        )


@router.get("/api/options-flow/sectors/validate")
async def options_flow_sectors_validate(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
    view: str = Query(
        "sectors",
        description="sectors (default) — sector coverage checks. themes — adds theme_totals_match_ticker_sums check.",
    ),
):
    """
    Coverage validation for the Net Options Flow universe.

    view=sectors (default)
      1. complete_accounting
      2. no_silent_placeholders
      3. no_missing_data
      4. no_blank_cached_data_premiums
      5. sector_totals_from_real_rows

    view=themes
      1–4 same as above
      5. theme_totals_match_ticker_sums — recomputes each theme's call/put from
         raw cache independently and compares to the theme node total, proving
         that cached_data rows contribute correctly to theme aggregates.

    Returns valid=True if all checks pass.
    """
    try:
        if view == "themes":
            from data.options_flow_sectors import validate_themes_coverage
            result = validate_themes_coverage()
        else:
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


@router.post("/api/options-flow/sectors/page-active")
async def options_flow_sectors_page_active(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """
    Signal that the Sectors page is actively being viewed.

    Calling this switches the backfill loop to priority mode:
      - Larger batches (25 tickers vs 8)
      - Shorter sleep (25 s vs 60 s)
      - "sectors" budget lane (60 RPM vs 20 RPM maintenance)

    The priority window lasts 5 minutes from the last call.
    The main /api/options-flow/sectors endpoint also calls this automatically.
    """
    try:
        from data.options_theme_supplement import (
            register_sectors_active,
            is_sectors_active,
            get_sectors_pending_symbols,
        )
        register_sectors_active()
        pending = get_sectors_pending_symbols()
        return JSONResponse(content={
            "ok": True,
            "priority_active": is_sectors_active(),
            "queue_remaining": len(pending),
            "next_pending": pending[:10],
        })
    except Exception as exc:
        import traceback
        return JSONResponse(
            status_code=500,
            content={"error": str(exc), "trace": traceback.format_exc()[-1000:]},
        )


@router.get("/api/options-flow/sectors/rate-status")
async def options_flow_sectors_rate_status(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """
    Real-time status of the sectors backfill loop and coverage.

    Returns:
      - priority_mode: whether the loop is in page-active priority mode
      - queue_remaining: how many tickers still need scanning this session
      - missing_symbols: tickers with no scan data at all (generic_pending)
      - pass_count / last_pass_at / next_at: loop timing
      - scan_coverage snapshot (fresh / cached_data / missing / no_options counts)
    """
    try:
        import time as _time
        from data.options_theme_supplement import (
            is_sectors_active,
            get_sectors_pending_symbols,
            get_sectors_backfill_diag,
            get_combined_ticker_data,
            get_no_options_symbols,
        )
        from data.options_flow_sectors import _ticker_state
        from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE

        pending  = get_sectors_pending_symbols()
        bf_diag  = get_sectors_backfill_diag()
        combined = get_combined_ticker_data()
        no_opts  = get_no_options_symbols()

        # Compute coverage directly from raw data (not from the sector tree cache,
        # which may be stale/absent on fresh restart).
        all_theme_syms: set[str] = {
            s.upper()
            for m in ENRICHED_THEME_RS_UNIVERSE.values()
            for s in (m.get("proxy_symbols") or [])
        }
        _state_counts: dict[str, int] = {}
        _missing_syms: list[str] = []
        _fresh_syms: list[str] = []
        _lkg_syms: list[str] = []
        _deferred_syms: list[str] = []
        _no_opts_syms: list[str] = []
        for _s in sorted(all_theme_syms):
            _row   = combined.get(_s)
            _state = _ticker_state(_row, _s, no_opts)
            _state_counts[_state] = _state_counts.get(_state, 0) + 1
            if _state == "generic_pending":
                _missing_syms.append(_s)
            elif _state == "stale_lkg":
                _lkg_syms.append(_s)
            elif _state == "deferred_retry":
                _deferred_syms.append(_s)
            elif _state == "confirmed_no_options":
                _no_opts_syms.append(_s)
            else:
                _fresh_syms.append(_s)

        _total    = len(all_theme_syms)
        _covered  = _total - len(_missing_syms) - len(_deferred_syms)
        _full_pct = round(_covered / max(_total, 1) * 100, 1)

        return JSONResponse(content={
            "as_of":           _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime()),
            "priority_mode":   is_sectors_active(),
            "queue_remaining": len(pending),
            "next_pending_10": pending[:10],
            "sectors_backfill": {
                "pass_count":       bf_diag.get("pass_count"),
                "last_pass_at":     bf_diag.get("last_pass_at"),
                "next_at":          bf_diag.get("next_at"),
                "last_batch_syms":  bf_diag.get("last_batch_syms", []),
                **{k: v for k, v in bf_diag.items()
                   if k.startswith("last_full_pass")},
            },
            "coverage": {
                "total":           _total,
                "fresh":           len(_fresh_syms),
                "cached_data":     len(_lkg_syms),
                "missing":         len(_missing_syms),
                "no_options":      len(_no_opts_syms),
                "deferred":        len(_deferred_syms),
                "full_pct":        _full_pct,
                "missing_symbols": _missing_syms,
            },
        })
    except Exception as exc:
        import traceback
        return JSONResponse(
            status_code=500,
            content={"error": str(exc), "trace": traceback.format_exc()[-1500:]},
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
