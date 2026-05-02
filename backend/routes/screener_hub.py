"""
Screener Hub HTTP endpoints.

GET  /api/screener-hub/themes
GET  /api/screener-hub
POST /api/admin/screener-hub/rebuild   (X-API-Key: AGENT_API_KEY)
GET  /api/admin/screener-hub/status    (X-API-Key: AGENT_API_KEY)
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Body, Header, Query, Request
from fastapi.responses import JSONResponse

from services.screener_hub_service import (
    _theme_metadata,
    get_admin_status,
    get_screener_hub,
    rebuild_universe,
    warm_tab_fundamentals,
)

router = APIRouter(tags=["screener_hub"])

_AUTH_HEADER = "X-API-Key"
_VALID_TABS = {"thematic", "social", "bottlenecks", "watchlist_portfolio"}
_VALID_REBUILD_TABS = _VALID_TABS | {"all"}


def _check_admin_key(api_key: Optional[str]) -> Optional[JSONResponse]:
    """Mirror existing AGENT_API_KEY pattern (catalyst_calendar.py)."""
    try:
        from config import AGENT_API_KEY
    except Exception:
        AGENT_API_KEY = None
    if AGENT_API_KEY and api_key != AGENT_API_KEY:
        return JSONResponse(
            status_code=401,
            content={"error": "Invalid or missing API key"},
        )
    return None


# ── GET /api/screener-hub/themes ──────────────────────────────────────────────

@router.get("/api/screener-hub/themes")
async def screener_hub_themes(request: Request):
    """Return the catalogue of themes available for the thematic tab."""
    try:
        themes = _theme_metadata()
        return JSONResponse(content={
            "status": "ok",
            "count": len(themes),
            "themes": themes,
        })
    except Exception as e:
        print(f"[SCREENER_HUB] /themes error: {e}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "error": str(e), "themes": []},
        )


# ── GET /api/screener-hub ─────────────────────────────────────────────────────

@router.get("/api/screener-hub")
async def screener_hub(
    request: Request,
    tab: str = Query("thematic", description="thematic|social|bottlenecks|watchlist_portfolio"),
    theme: Optional[str] = Query(None, description="theme key (thematic tab only)"),
    category: Optional[str] = Query(None, description="filter by category: Leading|Improving|Weakening|Lagging"),
    scoreMode: Optional[bool] = Query(None, description="enable score column"),
    cocFilter: Optional[bool] = Query(None, description="enable change-on-change filter"),
):
    tab_norm = (tab or "").lower()
    if tab_norm not in _VALID_TABS:
        return JSONResponse(
            status_code=400,
            content={"error": f"invalid tab '{tab}'. Valid: {sorted(_VALID_TABS)}"},
        )

    try:
        data = await get_screener_hub(
            tab=tab_norm,
            theme=theme,
            category=category,
            score_mode=bool(scoreMode),
            coc_filter=bool(cocFilter),
        )
        return JSONResponse(content=data)
    except Exception as e:
        print(f"[SCREENER_HUB] /api/screener-hub error: {e}")
        return JSONResponse(
            status_code=500,
            content={
                "status": "error", "tab": tab_norm, "theme": theme,
                "rows": [], "error": str(e),
            },
        )


# ── POST /api/admin/screener-hub/rebuild ──────────────────────────────────────

@router.post("/api/admin/screener-hub/rebuild")
async def screener_hub_rebuild(
    request: Request,
    api_key: Optional[str] = Header(None, alias=_AUTH_HEADER),
    body: dict = Body(default_factory=dict),
):
    err = _check_admin_key(api_key)
    if err:
        return err

    tab   = (body or {}).get("tab", "all")
    theme = (body or {}).get("theme")
    force = bool((body or {}).get("force", False))
    if tab not in _VALID_REBUILD_TABS:
        return JSONResponse(
            status_code=400,
            content={"error": f"invalid tab '{tab}'. Valid: {sorted(_VALID_REBUILD_TABS)}"},
        )

    try:
        universe_summary = await rebuild_universe(tab, theme=theme, force=force)
        warm_summary     = await warm_tab_fundamentals(
            tab, theme=theme, force=force, max_calls=250,
        )
        return JSONResponse(content={
            "status": "ok",
            "tab": tab, "theme": theme, "force": force,
            "universe": universe_summary,
            "warm_fundamentals": warm_summary,
        })
    except Exception as e:
        print(f"[SCREENER_HUB] rebuild error: {e}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "error": str(e)},
        )


# ── GET /api/admin/screener-hub/status ────────────────────────────────────────

@router.get("/api/admin/screener-hub/status")
async def screener_hub_status(
    request: Request,
    api_key: Optional[str] = Header(None, alias=_AUTH_HEADER),
):
    err = _check_admin_key(api_key)
    if err:
        return err
    try:
        return JSONResponse(content={"status": "ok", **get_admin_status()})
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"status": "error", "error": str(e)},
        )
