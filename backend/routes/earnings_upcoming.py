"""
earnings_upcoming.py — Clean FMP-only Upcoming Earnings routes.

Endpoints
─────────
GET /api/catalysts/earnings/upcoming-clean
GET /api/catalysts/earnings/day-clean

These endpoints are completely isolated from the legacy Catalyst Calendar
tabs logic.  No Finnhub, no Polymarket, no beat odds, no AI curation.
source="fmp" on every event.
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

router = APIRouter(tags=["earnings_clean"])

_AUTH_HEADER = "X-API-Key"


def _check_key(api_key: Optional[str]) -> Optional[JSONResponse]:
    from config import AGENT_API_KEY
    if AGENT_API_KEY and api_key != AGENT_API_KEY:
        return JSONResponse(
            status_code=401,
            content={"error": "Invalid or missing API key"},
        )
    return None


def _get_fmp_key() -> Optional[str]:
    try:
        from config import FMP_API_KEY
        return FMP_API_KEY or None
    except ImportError:
        return None


# ── GET /api/catalysts/earnings/upcoming-clean ────────────────────────────────

@router.get("/api/catalysts/earnings/upcoming-clean")
@traceable(name="earnings_clean.upcoming")
async def upcoming_clean(
    request: Request,
    api_key:  str = Header(None, alias=_AUTH_HEADER),
    from_date: Optional[str] = Query(None, alias="from"),
    to_date:   Optional[str] = Query(None, alias="to"),
    selected_date: Optional[str] = Query(None, alias="selectedDate"),
    search:    Optional[str] = Query(None),
    scope:     Optional[str] = Query(None),
    limit:     int           = Query(10000, ge=1, le=20000),
):
    """
    Clean FMP earnings calendar for a date range.

    Returns:
      events        — flat sorted list (date ASC, symbol ASC)
      eventsByDate  — {YYYY-MM-DD: [events]}
      countsByDate  — {YYYY-MM-DD: n}  ← use this for calendar dot counts
      asOf, source, from, to, status, errors

    No enrichment on this endpoint (fast, returns null for logo/price/marketCap).
    Use /day-clean to get fully enriched events for a specific selected date.

    Params:
      from         YYYY-MM-DD, default today
      to           YYYY-MM-DD, default today+30
      selectedDate YYYY-MM-DD — if provided, also returns day-level events inline
      search       ticker or company name substring
      scope        all | watchlist | portfolio
      limit        max events in flat list (default 10000)
    """
    err = _check_key(api_key)
    if err:
        return err

    fmp_key = _get_fmp_key()
    if not fmp_key:
        return JSONResponse(
            status_code=503,
            content={"error": "FMP API key not configured", "status": "error"},
        )

    from services.earnings_clean_service import get_upcoming_clean

    result = await get_upcoming_clean(
        api_key=fmp_key,
        from_date=from_date,
        to_date=to_date,
        search=search,
        scope=scope,
        limit=limit,
    )

    return JSONResponse(content=result)


# ── GET /api/catalysts/earnings/day-clean ─────────────────────────────────────

@router.get("/api/catalysts/earnings/day-clean")
@traceable(name="earnings_clean.day")
async def day_clean(
    request: Request,
    api_key:  str = Header(None, alias=_AUTH_HEADER),
    date:     Optional[str] = Query(None),
    search:   Optional[str] = Query(None),
    scope:    Optional[str] = Query(None),
    limit:    int           = Query(2000, ge=1, le=5000),
    max_live: int           = Query(80, ge=0, le=500),
):
    """
    Fully enriched FMP earnings events for a single selected date.

    Enrichment includes: companyName, logo/image, price, marketCap,
    marketCapBucket, EPS estimates/actuals, revenue estimates/actuals.

    Sort order (deterministic, no AI):
      1. US major exchange (NASDAQ/NYSE/AMEX) first
      2. Largest market cap first
      3. Revenue estimate present first
      4. EPS estimate present first
      5. Ticker alphabetical

    Cache:
      Profile data: fmp:co_profile:v2:{symbol} (24 h)
      Cached profiles are always resolved; only uncached symbols make HTTP calls.
      max_live controls the number of live HTTP profile fetches per request.

    Params:
      date     YYYY-MM-DD (required)
      search   ticker or company name substring
      scope    all | watchlist | portfolio
      limit    max events returned (default 2000)
      max_live max cold-cache HTTP profile fetches (default 80)
    """
    err = _check_key(api_key)
    if err:
        return err

    fmp_key = _get_fmp_key()
    if not fmp_key:
        return JSONResponse(
            status_code=503,
            content={"error": "FMP API key not configured", "status": "error"},
        )

    if not date:
        from services.earnings_clean_service import _today
        date = _today()

    from services.earnings_clean_service import get_day_clean

    result = await get_day_clean(
        api_key=fmp_key,
        date=date,
        search=search,
        scope=scope,
        limit=limit,
        max_live=max_live,
    )

    return JSONResponse(content=result)
