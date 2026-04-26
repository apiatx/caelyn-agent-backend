"""
earnings_upcoming.py — Clean FMP-only Upcoming Earnings routes (v2).

Endpoints
─────────
GET /api/catalysts/earnings/upcoming-clean
GET /api/catalysts/earnings/day-clean

Safety constraints (enforced here and in earnings_clean_service):
  • upcoming-clean: 7-day default window, 14-day hard max, NO enrichment.
  • day-clean: single day, enrich=false by default, max_live capped at 20.
  • Router is DISABLED in main.py until explicitly re-enabled after review.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Header, HTTPException, Query, Request
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

_AUTH_HEADER    = "X-API-Key"
_MAX_RANGE_DAYS = 14      # matches earnings_clean_service._MAX_RANGE_DAYS


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


def _validate_date(val: str, param: str) -> None:
    try:
        datetime.strptime(val, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid {param}: expected YYYY-MM-DD, got '{val}'",
        )


# ── GET /api/catalysts/earnings/upcoming-clean ────────────────────────────────

@router.get("/api/catalysts/earnings/upcoming-clean")
@traceable(name="earnings_clean.upcoming")
async def upcoming_clean(
    request:    Request,
    api_key:    str          = Header(None, alias=_AUTH_HEADER),
    from_date:  Optional[str] = Query(None, alias="from",
                                      description="YYYY-MM-DD, default today"),
    to_date:    Optional[str] = Query(None, alias="to",
                                      description="YYYY-MM-DD, default today+7, max today+14"),
    search:     Optional[str] = Query(None, description="Ticker or company name substring"),
    scope:      Optional[str] = Query(None, description="all | watchlist | portfolio"),
    limit:      int           = Query(500, ge=1, le=2000,
                                      description="Max events in flat list"),
):
    """
    Clean FMP earnings calendar for a date range.

    Constraints:
      • Default window: today → today+7 days.
      • Max range: 14 days. If caller sends a wider range, it is CLAMPED (not rejected).
      • No profile/quote/marketCap enrichment — basic event fields + counts only.
      • Max 2 sequential FMP calls per request.
      • 429 circuit breaker: if FMP rate-limits, returns partial data with
        status=partial and rateLimited=true. Does not affect other site routes.

    Returns: { status, source, fmpCallsUsed, rateLimited, errors,
               from, to, events, eventsByDate, countsByDate }
    """
    err = _check_key(api_key)
    if err:
        return err

    fmp_key = _get_fmp_key()
    if not fmp_key:
        return JSONResponse(
            status_code=503,
            content={"error": "FMP API key not configured", "status": "error",
                     "fmpCallsUsed": 0, "rateLimited": False, "errors": []},
        )

    if from_date:
        _validate_date(from_date, "from")
    if to_date:
        _validate_date(to_date, "to")

    # Range enforcement: warn in response if clamped (clamping done inside service)
    clamped = False
    if from_date and to_date:
        try:
            start = datetime.strptime(from_date, "%Y-%m-%d")
            end   = datetime.strptime(to_date,   "%Y-%m-%d")
            if (end - start).days >= _MAX_RANGE_DAYS:
                clamped = True
        except ValueError:
            pass

    from services.earnings_clean_service import get_upcoming_clean

    result = await get_upcoming_clean(
        api_key=fmp_key,
        from_date=from_date,
        to_date=to_date,
        search=search,
        scope=scope,
        limit=limit,
    )

    if clamped:
        result.setdefault("errors", [])
        result["errors"].append(
            f"Requested range exceeded {_MAX_RANGE_DAYS} days — clamped to {result.get('to')}"
        )

    return JSONResponse(content=result)


# ── GET /api/catalysts/earnings/day-clean ─────────────────────────────────────

@router.get("/api/catalysts/earnings/day-clean")
@traceable(name="earnings_clean.day")
async def day_clean(
    request:  Request,
    api_key:  str          = Header(None, alias=_AUTH_HEADER),
    date:     Optional[str] = Query(None,
                                    description="YYYY-MM-DD (required)"),
    search:   Optional[str] = Query(None),
    scope:    Optional[str] = Query(None, description="all | watchlist | portfolio"),
    limit:    int           = Query(200, ge=1, le=1000),
    enrich:   bool          = Query(False,
                                    description="Fetch FMP profiles (default false)"),
    max_live: int           = Query(10, ge=0, le=20,
                                    description="Max live profile fetches (0-20, default 10)"),
):
    """
    FMP earnings events for a single selected date.

    enrich=false (default):
      • 1 FMP calendar call only. No profile/quote/marketCap calls.
      • Sort: revenueEstimated desc → symbol alpha.

    enrich=true:
      • Enriches up to max_live (default 10, max 20) symbols with FMP profiles.
      • Concurrency: 2 simultaneous HTTP calls (semaphore-limited).
      • Timeout: 3 s per profile call.
      • Cached profiles (24 h TTL) are always resolved — don't count against max_live.
      • On 429: circuit breaker fires, enrichment stops, returns partial with
        rateLimited=true. Does not affect other site routes.
      • Sort: US exchange first → marketCap desc → revenueEst → epsEst → symbol.

    Returns: { status, source, fmpCallsUsed, rateLimited, errors, date, count, events }
    """
    err = _check_key(api_key)
    if err:
        return err

    fmp_key = _get_fmp_key()
    if not fmp_key:
        return JSONResponse(
            status_code=503,
            content={"error": "FMP API key not configured", "status": "error",
                     "fmpCallsUsed": 0, "rateLimited": False, "errors": []},
        )

    from services.earnings_clean_service import get_day_clean, _today
    if not date:
        date = _today()
    else:
        _validate_date(date, "date")

    result = await get_day_clean(
        api_key=fmp_key,
        date=date,
        search=search,
        scope=scope,
        limit=limit,
        enrich=enrich,
        max_live=max_live,
    )

    return JSONResponse(content=result)
