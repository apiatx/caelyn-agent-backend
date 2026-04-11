"""
Watchlist Router — FastAPI endpoints for watchlist CRUD, news, refresh, and stock detail.
Supports multiple named watchlists.
"""

from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict
from typing import Any, Dict, List, Optional

from services.watchlist_service import (
    save_watchlist,
    load_watchlist,
    list_watchlists,
    clear_watchlist,
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


class WatchlistSaveResponse(BaseModel):
    success: bool
    watchlist_id: str = ""
    name: str = ""
    saved_at: str = ""
    ticker_count: int = 0


# ── Helper to get the global agent ──────────────────────────────────────────

def _get_agent():
    """Import the global agent from main — it's initialized on startup."""
    import main
    if main.agent is None:
        raise HTTPException(status_code=503, detail="Server is still starting up.")
    return main.agent


# ── Fixed-path endpoints (MUST be registered before /{watchlist_id} routes) ──

@router.get("/list")
async def list_endpoint():
    """List all saved watchlists (id, name, ticker_count, saved_at, updated_at)."""
    return list_watchlists()


@router.post("/save")
async def save_endpoint(body: WatchlistSaveRequest):
    """Save CSV data + AI analysis. Optionally specify watchlist_id and name."""
    result = save_watchlist(body.csv_data, body.analysis, watchlist_id=body.watchlist_id, name=body.name)
    return result


@router.get("/debug")
async def debug_endpoint():
    """Debug endpoint — returns file path, existence, size, and preview."""
    info: Dict[str, Any] = {
        "resolved_path": str(_WATCHLIST_FILE),
        "exists": _WATCHLIST_FILE.exists(),
    }
    if _WATCHLIST_FILE.exists():
        try:
            content = _WATCHLIST_FILE.read_text()
            info["file_size_bytes"] = len(content)
            info["preview"] = content[:500]
        except Exception as e:
            info["read_error"] = str(e)
    else:
        info["parent_exists"] = _WATCHLIST_FILE.parent.exists()
        info["parent_path"] = str(_WATCHLIST_FILE.parent)
    return info


@router.get("/news")
async def news_endpoint():
    """Fetch fresh news for all tickers in the most recent watchlist.
    Returns a flat { TICKER: [articles] } map.
    """
    store = load_watchlist()
    if store is None:
        return {}
    tickers = store.get("tickers", [])
    if not tickers:
        return {}
    news_map = await fetch_news_for_tickers(tickers)
    return news_map


@router.post("/refresh")
async def refresh_default_endpoint():
    """Re-run AI analysis with latest news for the most recent watchlist."""
    agent = _get_agent()
    result = await refresh_watchlist_analysis(agent)
    if result.get("error"):
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@router.get("/stock/{ticker}")
async def stock_detail_endpoint(ticker: str):
    """Return enriched data for a single ticker: CSV row + news + AI deep dive."""
    agent = _get_agent()
    result = await get_stock_detail(ticker, agent)
    if result.get("error"):
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.get("")
async def get_endpoint():
    """Return the most recently updated watchlist (backward compat)."""
    store = load_watchlist()
    if store is None:
        return {"empty": True}
    return store


@router.delete("")
async def delete_endpoint():
    """Clear the most recent watchlist (backward compat)."""
    return clear_watchlist()


# ── Parameterized endpoints (/{watchlist_id} — MUST come after fixed routes) ──

@router.get("/{watchlist_id}")
async def get_by_id_endpoint(watchlist_id: str):
    """Return a specific watchlist by id."""
    store = load_watchlist(watchlist_id)
    if store is None:
        raise HTTPException(status_code=404, detail=f"Watchlist '{watchlist_id}' not found.")
    return store


@router.post("/{watchlist_id}/refresh")
async def refresh_endpoint(watchlist_id: str):
    """Re-run AI analysis with latest news for a specific watchlist."""
    agent = _get_agent()
    result = await refresh_watchlist_analysis(agent, watchlist_id=watchlist_id)
    if result.get("error"):
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@router.get("/{watchlist_id}/news")
async def news_by_id_endpoint(watchlist_id: str):
    """Fetch fresh news for all tickers in a specific watchlist."""
    store = load_watchlist(watchlist_id)
    if store is None:
        return {}
    tickers = store.get("tickers", [])
    if not tickers:
        return {}
    news_map = await fetch_news_for_tickers(tickers)
    return news_map


@router.delete("/{watchlist_id}")
async def delete_by_id_endpoint(watchlist_id: str):
    """Delete a specific watchlist."""
    return clear_watchlist(watchlist_id)
