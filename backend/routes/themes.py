"""
Themes by Relative Strength — HTTP endpoints.

GET  /api/themes/relative-strength?timeframe=1D|7D|30D|YTD|1Y
GET  /api/themes/relative-strength/refresh          (force-refresh cache)
GET  /api/themes/list                               (static theme registry)
"""
from __future__ import annotations

from fastapi import APIRouter, Query, HTTPException

from services.theme_rs_universe import THEME_RS_UNIVERSE

router = APIRouter(prefix="/api/themes", tags=["themes"])

_VALID_TIMEFRAMES = {"1D", "7D", "30D", "YTD", "1Y"}


@router.get("/relative-strength")
async def themes_relative_strength(
    timeframe: str = Query(
        "30D",
        description="Return timeframe: 1D | 7D | 30D | YTD | 1Y",
    ),
):
    """
    Sorted list of themes by relative strength score.
    Each item includes:
      theme_id, display_name, proxy_type, proxy_symbols,
      performance (1D/7D/30D/YTD/1Y), rs_score, rs_vs_spy, rs_vs_qqq,
      state (active/emerging/neutral/weakening/dead_zone),
      leaders, laggards, breadth_pct, momentum_rank,
      sector_tags, keywords, macro_sensitivities,
      source_health, last_updated.
    No LLM calls. Cached 15 min (market hours) / 60 min (off-hours).
    LKG persisted to disk — page loads work after restart.
    """
    tf = timeframe.upper()
    if tf not in _VALID_TIMEFRAMES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid timeframe '{timeframe}'. Must be one of: {sorted(_VALID_TIMEFRAMES)}",
        )
    try:
        from services.theme_rs_service import get_theme_rs_data
        payload = await get_theme_rs_data(timeframe=tf, force=False)
        return payload
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Theme RS error: {e}")


@router.get("/relative-strength/refresh")
async def themes_relative_strength_refresh(
    timeframe: str = Query("30D", description="Timeframe to recompute"),
):
    """Force-refresh the theme RS cache (bypasses TTL). Same response shape."""
    tf = timeframe.upper()
    if tf not in _VALID_TIMEFRAMES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid timeframe '{timeframe}'. Must be one of: {sorted(_VALID_TIMEFRAMES)}",
        )
    try:
        from services.theme_rs_service import get_theme_rs_data
        payload = await get_theme_rs_data(timeframe=tf, force=True)
        return payload
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Theme RS refresh error: {e}")


@router.get("/list")
async def themes_list():
    """Return the full static theme registry (no price data)."""
    themes = []
    for theme_id, meta in THEME_RS_UNIVERSE.items():
        themes.append({
            "theme_id":           theme_id,
            "display_name":       meta["display_name"],
            "proxy_type":         meta["proxy_type"],
            "proxy_symbols":      meta["proxy_symbols"],
            "candidate_symbols":  meta.get("candidate_symbols", []),
            "sector_tags":        meta.get("sector_tags", []),
            "keywords":           meta.get("keywords", []),
            "macro_sensitivities": meta.get("macro_sensitivities", []),
        })
    return {
        "themes":      themes,
        "theme_count": len(themes),
    }
