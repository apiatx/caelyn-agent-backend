"""
Watchlist Router — FastAPI endpoints for multi-watchlist CRUD, news, refresh, and stock detail.
"""

from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict
from typing import Any, Dict, List, Optional

from services.watchlist_service import (
    save_watchlist,
    load_watchlist,
    list_watchlists,
    clear_watchlist,
    extract_tickers,
    fetch_news_for_tickers,
    refresh_watchlist_analysis,
    get_stock_detail,
    _WATCHLIST_FILE,
)

router = APIRouter(prefix="/api/watchlist", tags=["watchlist"])


# ── Request / Response Models ────────────────────────────────────────────────

class WatchlistSaveRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    csv_data: List[Dict[str, Any]]
    analysis: Dict[str, Any]
    watchlist_id: Optional[str] = None
    name: Optional[str] = None


# ── Helper ──────────────────────────────────────────────────────────────────

def _get_agent():
    import main
    if main.agent is None:
        raise HTTPException(status_code=503, detail="Server is still starting up.")
    return main.agent


# ── Endpoints — STATIC paths first, then parameterized ──────────────────────

@router.get("/list")
async def list_endpoint():
    """List all saved watchlists (metadata only)."""
    return list_watchlists()


@router.post("/save")
async def save_endpoint(body: WatchlistSaveRequest):
    """Save CSV data + AI analysis to the watchlist store."""
    result = save_watchlist(body.csv_data, body.analysis, body.watchlist_id, body.name)
    return result


@router.get("/debug")
async def debug_endpoint():
    """Debug endpoint — returns file path, existence, Postgres availability."""
    info: Dict[str, Any] = {
        "json_file_path": str(_WATCHLIST_FILE),
        "json_file_exists": _WATCHLIST_FILE.exists(),
    }
    try:
        from data.pg_storage import is_available, watchlist_list as pg_wl_list
        info["postgres_available"] = is_available()
        if is_available():
            entries = pg_wl_list()
            info["postgres_watchlist_count"] = len(entries)
            info["postgres_watchlists"] = entries
    except Exception as e:
        info["postgres_error"] = str(e)
    if _WATCHLIST_FILE.exists():
        try:
            content = _WATCHLIST_FILE.read_text()
            info["json_file_size_bytes"] = len(content)
            info["json_preview"] = content[:500]
        except Exception as e:
            info["json_read_error"] = str(e)
    return info


@router.get("/news")
async def news_endpoint():
    """Fetch fresh news for all tickers in the most recent watchlist."""
    store = load_watchlist()
    if store is None:
        return {}
    tickers = store.get("tickers", [])
    if not tickers:
        return {}
    return await fetch_news_for_tickers(tickers)


@router.post("/refresh")
async def refresh_endpoint():
    """Re-run AI analysis with latest news (most recent watchlist)."""
    agent = _get_agent()
    result = await refresh_watchlist_analysis(agent)
    if result.get("error"):
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@router.get("")
async def get_endpoint():
    """Return the most recent saved watchlist, or {empty: true}."""
    store = load_watchlist()
    if store is None:
        return {"empty": True}
    return store


@router.delete("")
async def delete_endpoint():
    """Clear the most recent watchlist."""
    return clear_watchlist()


# ── Parameterized endpoints (MUST be after static paths) ────────────────────

@router.get("/{watchlist_id}")
async def get_by_id_endpoint(watchlist_id: str):
    """Return a specific watchlist by ID."""
    store = load_watchlist(watchlist_id)
    if store is None:
        return {"empty": True}
    return store


@router.post("/{watchlist_id}/refresh")
async def refresh_by_id_endpoint(watchlist_id: str):
    """Re-run AI analysis for a specific watchlist."""
    agent = _get_agent()
    result = await refresh_watchlist_analysis(agent, watchlist_id=watchlist_id)
    if result.get("error"):
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@router.get("/{watchlist_id}/news")
async def news_by_id_endpoint(watchlist_id: str):
    """Fetch news for a specific watchlist's tickers."""
    store = load_watchlist(watchlist_id)
    if store is None:
        return {}
    tickers = store.get("tickers", [])
    if not tickers:
        return {}
    return await fetch_news_for_tickers(tickers)


@router.get("/{watchlist_id}/stock/{ticker}")
async def stock_detail_by_id_endpoint(watchlist_id: str, ticker: str):
    """Return enriched data for a single ticker within a specific watchlist."""
    agent = _get_agent()
    result = await get_stock_detail(ticker, agent, watchlist_id=watchlist_id)
    if result.get("error"):
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.delete("/{watchlist_id}")
async def delete_by_id_endpoint(watchlist_id: str):
    """Delete a specific watchlist."""
    return clear_watchlist(watchlist_id)
