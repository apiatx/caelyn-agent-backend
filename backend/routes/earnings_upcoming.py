"""
earnings_upcoming.py — Clean FMP-only Upcoming Earnings routes (v2).

Endpoints
─────────
GET /api/catalysts/earnings/upcoming-clean
GET /api/catalysts/earnings/day-clean
GET /api/catalysts/earnings/week-clean
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
_MAX_RANGE_DAYS = 30      # matches earnings_clean_service._MAX_RANGE_DAYS


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
                                      description="YYYY-MM-DD, default today+14, max today+30"),
    search:     Optional[str] = Query(None, description="Ticker or company name substring"),
    scope:      Optional[str] = Query(None, description="all | watchlist | portfolio"),
    limit:      int           = Query(5000, ge=1, le=10000,
                                      description="Max events in flat list"),
):
    """
    FMP earnings calendar for a date range (up to 30 days).

    Returns logos via FMP image CDN (no extra API call), counts per day, and
    the full event list with EPS/revenue estimates.

    Constraints:
      • Default window: today → today+14 days.
      • Max range: 30 days. If caller sends a wider range, it is CLAMPED (not rejected).
      • Logos populated for every event via predictable FMP CDN URL.
      • No profile enrichment on this endpoint — use day-clean for full company data.
      • Max 5 sequential FMP calls per request (5 × 7-day chunks).
      • 429 circuit breaker: returns partial data with status=partial and rateLimited=true.

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
    limit:    int           = Query(500, ge=1, le=2000),
    enrich:   bool          = Query(True,
                                    description="Fetch FMP profiles for company name, price, mktcap (default true)"),
    max_live: int           = Query(30, ge=0, le=50,
                                    description="Max live profile fetches (0-50, default 30)"),
):
    """
    Full FMP earnings data for a single selected date.

    With enrich=true (default):
      • Returns companyName, logo, price, changesPercentage, marketCap, sector, industry.
      • Profiles are cached 24 h — subsequent calls for the same day are fast.
      • Concurrency: 5 simultaneous HTTP calls, capped at max_live (default 30).
      • On 429: circuit breaker fires, returns partial with rateLimited=true.
      • Sort: US exchange first → marketCap desc → revenueEst → epsEst → symbol.

    With enrich=false:
      • 1 FMP calendar call only. Logos still populated via CDN URL.
      • Sort: revenueEstimated desc → symbol alpha.

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


# ── GET /api/catalysts/earnings/week-clean ────────────────────────────────────

@router.get("/api/catalysts/earnings/week-clean")
@traceable(name="earnings_clean.week")
async def week_clean(
    request:           Request,
    api_key:           str          = Header(None, alias=_AUTH_HEADER),
    week_start:        Optional[str] = Query(None, description="YYYY-MM-DD (Monday); default current week"),
    week_end:          Optional[str] = Query(None, description="YYYY-MM-DD (Friday); default current week Friday"),
    scope:             Optional[str] = Query(None, description="all | watchlist | portfolio"),
    search:            Optional[str] = Query(None, description="Ticker or company name filter"),
    limit_per_session: int           = Query(8, ge=1, le=15, description="Max events per session slot per day (default 8)"),
    max_total:         int           = Query(60, ge=1, le=100, description="Max events in topEvents (default 60)"),
):
    """
    Curated weekly earnings board for the Catalyst Calendar 'This Week' view.

    Returns Mon–Fri earnings grouped by day and session (preMarket / afterHours /
    duringMarket / unknown), scored by market cap + theme relevance + estimate
    quality. FMP only — no AI, no Finnhub, no Polymarket.

    Accepts camelCase aliases: weekStart, weekEnd (resolved from raw query params).

    Constraints:
      • One week only (7 days max).
      • Max 2 FMP calendar calls per request (1 week = 1 chunk).
      • Enrichment: top 40 candidates, concurrency=2, cache 24 h.
      • 429 circuit breaker → partial data, status=partial.

    Returns: { asOf, source, weekStart, weekEnd, days[], topEvents[], status, errors }
    """
    err = _check_key(api_key)
    if err:
        return err

    fmp_key = _get_fmp_key()
    if not fmp_key:
        return JSONResponse(
            status_code=503,
            content={"error": "FMP API key not configured", "status": "error", "errors": []},
        )

    # Accept camelCase aliases from raw query string
    qp         = request.query_params
    week_start = week_start or qp.get("weekStart") or None
    week_end   = week_end   or qp.get("weekEnd")   or None

    if week_start:
        _validate_date(week_start, "week_start")
    if week_end:
        _validate_date(week_end, "week_end")

    from services.earnings_clean_service import get_week_clean

    result = await get_week_clean(
        api_key=fmp_key,
        week_start=week_start,
        week_end=week_end,
        scope=scope,
        search=search,
        limit_per_session=limit_per_session,
        max_total=max_total,
    )

    return JSONResponse(content=result)
