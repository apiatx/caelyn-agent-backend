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

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

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
async def refresh_analysis_endpoint():
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
