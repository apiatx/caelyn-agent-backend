"""
Themes by Relative Strength — HTTP endpoints.

GET  /api/themes/relative-strength?timeframe=1D|7D|30D|YTD|1Y|5Y[&classification=all|sector|theme|sub_theme]
GET  /api/themes/relative-strength/refresh  (force-refresh cache, same params)
GET  /api/themes/list                        (static theme registry)
"""
from __future__ import annotations

from fastapi import APIRouter, Query, HTTPException

from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE as THEME_RS_UNIVERSE

router = APIRouter(prefix="/api/themes", tags=["themes"])

_VALID_TIMEFRAMES      = {"1D", "7D", "30D", "YTD", "1Y", "5Y"}
_VALID_CLASSIFICATIONS = {"all", "sector", "theme", "sub_theme"}


@router.get("/relative-strength")
async def themes_relative_strength(
    timeframe: str = Query(
        "30D",
        description="Return timeframe: 1D | 7D | 30D | YTD | 1Y | 5Y",
    ),
    classification: str = Query(
        "all",
        description="Filter by classification: all | sector | theme | sub_theme",
    ),
):
    """
    Sorted list of themes/sectors by relative strength score.

    Each item includes:
      theme_id, display_name, classification, parent_sector, sector_tags,
      proxy_type, proxy_symbols, proxy_symbols_used,
      return_pct, performance (1D/7D/30D/YTD/1Y),
      rs_score, rs_vs_spy, rs_vs_qqq,
      state (active/emerging/neutral/weakening/dead_zone), state_reason,
      leaders, laggards, breadth_pct, momentum_rank, last_updated.

    classification filter:
      all       → all 60 rows (11 SPDR sectors + 49 themes/sub-themes) [default]
      sector    → exactly 11 SPDR broad sector rows
      theme     → broad cross-sector/factor theme rows
      sub_theme → narrow industry sub-theme rows

    No LLM calls. 1D cached 60s market hours / 3600s off-hours.
    7D/30D/YTD/1Y cached 900s market hours / 3600s off-hours.
    LKG persisted to disk — page loads work after restart.
    """
    tf  = timeframe.upper()
    clf = classification.lower()

    if tf not in _VALID_TIMEFRAMES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid timeframe '{timeframe}'. Must be one of: {sorted(_VALID_TIMEFRAMES)}",
        )
    if clf not in _VALID_CLASSIFICATIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid classification '{classification}'. Must be one of: {sorted(_VALID_CLASSIFICATIONS)}",
        )
    try:
        from services.theme_rs_service import get_theme_rs_data
        return await get_theme_rs_data(timeframe=tf, force=False, classification=clf)
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Theme RS error: {e}")


@router.get("/relative-strength/refresh")
async def themes_relative_strength_refresh(
    timeframe: str = Query("30D", description="Timeframe to recompute: 1D | 7D | 30D | YTD | 1Y | 5Y"),
    classification: str = Query(
        "all",
        description="Filter by classification: all | sector | theme | sub_theme",
    ),
):
    """Force-refresh the theme RS cache (bypasses TTL). Same response shape."""
    tf  = timeframe.upper()
    clf = classification.lower()

    if tf not in _VALID_TIMEFRAMES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid timeframe '{timeframe}'. Must be one of: {sorted(_VALID_TIMEFRAMES)}",
        )
    if clf not in _VALID_CLASSIFICATIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid classification '{classification}'. Must be one of: {sorted(_VALID_CLASSIFICATIONS)}",
        )
    try:
        from services.theme_rs_service import get_theme_rs_data
        return await get_theme_rs_data(timeframe=tf, force=True, classification=clf)
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Theme RS refresh error: {e}")


@router.get("/merge-debug")
async def themes_merge_debug():
    """
    Diagnostic: shows which themes were enriched with watchlist tickers and what was added.
    No price data. No user PII. Dev/admin use only.
    """
    from services.theme_merge_layer import get_merge_debug_info
    return get_merge_debug_info()


@router.get("/list")
async def themes_list(
    classification: str = Query(
        "all",
        description="Filter by classification: all | sector | theme | sub_theme",
    ),
):
    """Return the full static theme registry (no price data)."""
    clf = classification.lower()
    themes = []
    for theme_id, meta in THEME_RS_UNIVERSE.items():
        if clf != "all" and meta.get("classification") != clf:
            continue
        themes.append({
            "theme_id":            theme_id,
            "display_name":        meta["display_name"],
            "classification":      meta.get("classification", "theme"),
            "parent_sector":       meta.get("parent_sector"),
            "proxy_type":          meta["proxy_type"],
            "proxy_symbols":       meta["proxy_symbols"],
            "candidate_symbols":   meta.get("candidate_symbols", []),
            "sector_tags":         meta.get("sector_tags", []),
            "keywords":            meta.get("keywords", []),
            "macro_sensitivities": meta.get("macro_sensitivities", []),
        })
    return {
        "themes":                themes,
        "theme_count":           len(themes),
        "classification_filter": clf,
    }
