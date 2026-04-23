"""
FastAPI router for the Sector Rotation dashboard.

Endpoints:
  GET  /api/sector-rotation/dashboard          — full dashboard (market data + cached AI analysis)
  GET  /api/sector-rotation/analysis           — just the AI analysis
  GET  /api/sector-rotation/history            — compact price series for chart widgets
  POST /api/sector-rotation/refresh-analysis   — force-regenerate AI analysis (admin/internal)
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request, Depends
from fastapi.responses import JSONResponse

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from subscription import require_subscription

from services.sector_rotation.providers import fetch_etf_quotes, fetch_all_histories
from services.sector_rotation.schemas import (
    SectorRotationDashboard,
    AIAnalysis,
    ETFSeries,
    SECTOR_ETF_MAP,
)
from services.sector_rotation.service import get_dashboard, get_analysis_only
from services.sector_rotation.analytics import _compact_series

router = APIRouter(prefix="/api/sector-rotation", tags=["sector-rotation"])


@router.get("/dashboard", response_model=SectorRotationDashboard)
async def dashboard_endpoint(
    include_analysis: bool = Query(True, description="Include AI analysis in response"),
):
    """
    Full sector rotation dashboard.
    Market data refreshes every 5 minutes.
    AI analysis is cached for 7 days then auto-regenerated.
    """
    try:
        data = await get_dashboard(include_analysis=include_analysis)
        return data
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Dashboard error: {e}")


@router.get("/analysis", response_model=Optional[AIAnalysis])
async def analysis_endpoint():
    """Return the cached weekly AI sector rotation analysis."""
    from services.sector_rotation.gemini_analysis import load_cached_analysis, _load_disk_cache
    cached = _load_disk_cache()
    if cached:
        try:
            return AIAnalysis(**{k: v for k, v in cached.items() if not k.startswith("_")})
        except Exception:
            pass
    return JSONResponse(content=None)


@router.get("/history")
async def history_endpoint(
    range: str = Query("30d", description="Range: 1d | 7d | 30d | ytd | 1y"),
    tickers: Optional[str] = Query(None, description="Comma-separated tickers; defaults to all sectors"),
):
    """
    Return compact price series for the requested range.
    Suitable for frontend sparkline / chart widgets.
    """
    _range_bars: dict[str, int] = {
        "1d": 1, "7d": 5, "30d": 22, "ytd": 65, "1y": 252,
    }
    n_bars = _range_bars.get(range, 22)

    target = (
        [t.strip().upper() for t in tickers.split(",") if t.strip()]
        if tickers
        else list(SECTOR_ETF_MAP.keys())
    )
    unknown = [t for t in target if t not in SECTOR_ETF_MAP and t not in ("SPY", "QQQ")]
    if unknown:
        raise HTTPException(status_code=400, detail=f"Unknown tickers: {unknown}")

    histories = await fetch_all_histories()
    result: dict[str, ETFSeries] = {}
    for t in target:
        h = histories.get(t, [])
        result[t] = _compact_series(h, n_bars)

    return {
        "range": range,
        "tickers": target,
        "series": {t: s.model_dump() for t, s in result.items()},
    }


@router.post("/refresh-analysis")
async def refresh_analysis_endpoint(request: Request, _sub: None = Depends(require_subscription)):
    """
    Force-regenerate the AI analysis regardless of cache age.
    Intended for admin / scheduled use — Gemini call may take 15–30 seconds.
    """
    try:
        analysis = await get_analysis_only(force=True)
        if analysis is None:
            return JSONResponse(
                status_code=503,
                content={"status": "error", "detail": "AI generation failed — check logs"},
            )
        return {"status": "ok", "generated_at": analysis.generated_at}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── /api/sectors/* — Sectors page endpoints ──────────────────────────────────

sectors_router = APIRouter(prefix="/api/sectors", tags=["sectors"])


@sectors_router.get("/page-data")
async def sectors_page_data_endpoint(
    include_stocks: bool = Query(True, description="Include per-sector stock scan"),
    top_n: int = Query(2, description="Number of winning sectors to return stocks for"),
):
    """
    Unified Sectors page payload.

    Returns:
    - All 11 sector snapshots (sorted by rotation_score)
    - Winning-sector detection (multi-timeframe composite)
    - Stock scan for top N sectors (momentum, bottleneck, anchor roles)
    - Persisted AI analysis — survives page refresh until user manually regenerates
    """
    try:
        from services.sector_rotation.service import get_sectors_page_data
        data = await get_sectors_page_data(
            include_stocks=include_stocks,
            top_sectors_for_stocks=max(1, min(top_n, 3)),
        )
        return data
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Sectors page error: {e}")


@sectors_router.post("/generate-analysis")
async def sectors_generate_analysis_endpoint(
    request: Request,
    _sub: None = Depends(require_subscription),
):
    """
    User-triggered analysis generation for the Sectors page.
    Subscription required. Triggers a new Gemini analysis call that:
    - Includes stock-level context for winning sectors
    - Outputs a "top 10 stocks to watch" ranking
    - Persists to disk (survives page refresh indefinitely)
    Returns the new analysis on success.
    """
    try:
        from services.sector_rotation.service import get_analysis_only
        from services.sector_rotation.analytics import get_winning_sectors
        from services.sector_rotation.gemini_analysis import get_or_generate_analysis
        from services.sector_rotation.service import (
            get_dashboard, _fetch_macro_overlay, _enrich_macro_with_treasuries,
        )
        from services.sector_rotation.analytics import build_sector_snapshots
        from services.sector_rotation.providers import fetch_etf_quotes, fetch_all_histories
        from services.sector_rotation.analytics import _pct_change

        import asyncio

        # Gather fresh market data for the analysis prompt context
        quotes, histories = await asyncio.gather(
            fetch_etf_quotes(),
            fetch_all_histories(),
        )
        macro = _fetch_macro_overlay()
        macro = await _enrich_macro_with_treasuries(macro)
        spy_hist = histories.get("SPY", [])
        spy_30d = _pct_change(spy_hist, 22)
        if spy_30d is not None:
            macro["spy_change_30d"] = round(spy_30d, 2)

        snapshots = build_sector_snapshots(quotes, histories)

        from services.sector_rotation.analytics import derive_regime
        regime = derive_regime(snapshots, macro)

        # Detect winning sectors to focus the analysis
        winners = get_winning_sectors(snapshots, top_n=3)
        winning_etfs = [w.etf for w in winners]

        analysis = await get_or_generate_analysis(
            snapshots=snapshots,
            regime=regime,
            macro=macro,
            force=True,
            winning_etfs=winning_etfs,
        )

        if analysis is None:
            return JSONResponse(
                status_code=503,
                content={"status": "error", "detail": "AI analysis generation failed — check server logs"},
            )

        # Always inject structured stock objects into top_stocks_to_watch so the
        # frontend panel has renderable rows immediately after generation.
        from services.sector_rotation.sector_stocks import get_sector_stocks, build_top_stocks_list
        from services.sector_rotation.gemini_analysis import _save_disk_cache
        etfs_for_stocks = winning_etfs[:2]  # top 1-2 winning sectors
        stock_groups = await get_sector_stocks(etfs_for_stocks)
        structured_stocks = build_top_stocks_list(stock_groups, limit=10)
        analysis = analysis.model_copy(update={"top_stocks_to_watch": structured_stocks})
        # Re-persist with structured stocks so future page loads also have them
        _save_disk_cache(analysis.model_dump())

        return {
            "status": "ok",
            "generated_at": analysis.generated_at,
            "winning_sector_etfs": winning_etfs,
            "top_stocks_to_watch": structured_stocks,
            "analysis": analysis.model_dump(),
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
