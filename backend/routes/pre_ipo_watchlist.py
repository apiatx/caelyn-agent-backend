"""
Pre-IPO Watchlist router — GET /api/calendar/pre-ipo-watchlist

Additive endpoint that aggregates pre-IPO intel for a fixed set of
high-profile private companies using Perplexity, Polymarket, and
Finnhub.  Independent of the existing FMP IPO calendar surface.
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Header, Query, Request
from fastapi.responses import JSONResponse

try:
    from langsmith import traceable
except ImportError:
    def traceable(*args, **kwargs):
        def _noop(fn):
            return fn
        if args and callable(args[0]):
            return args[0]
        return _noop

from services.pre_ipo_watchlist_service import get_pre_ipo_watchlist

router = APIRouter(tags=["pre_ipo_watchlist"])

_AUTH_HEADER = "X-API-Key"


def _check_key(api_key: Optional[str]) -> Optional[JSONResponse]:
    """Return a 401 response if the API key is invalid, else None."""
    from config import AGENT_API_KEY
    if AGENT_API_KEY and api_key != AGENT_API_KEY:
        return JSONResponse(status_code=401, content={"error": "Invalid or missing API key"})
    return None


@router.get("/api/calendar/pre-ipo-watchlist")
@traceable(name="pre_ipo_watchlist.endpoint")
async def pre_ipo_watchlist(
    request: Request,
    api_key: str = Header(None, alias=_AUTH_HEADER),
    refresh: bool = Query(
        default=False,
        description="Force-bypass the cache and refetch from upstream sources.",
    ),
):
    """
    Return pre-IPO watchlist data for a fixed set of high-profile private
    companies (SpaceX, OpenAI, Anthropic, Databricks, Anduril, Stripe).

    Data is cached for ~8 hours.  Stale data is returned if a refresh
    fails so callers always receive a usable response when at least one
    successful fetch has happened previously.
    """
    err = _check_key(api_key)
    if err:
        return err

    try:
        data = await get_pre_ipo_watchlist(refresh=refresh)
        return JSONResponse(content=data)
    except Exception as e:
        print(f"[pre_ipo_watchlist] unhandled: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "status": "error"},
        )
