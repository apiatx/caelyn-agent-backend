"""
earnings_upcoming.py — Clean FMP-only Upcoming Earnings routes (v2).

Endpoints
─────────
GET /api/catalysts/earnings/upcoming-clean   — All: date range calendar
GET /api/catalysts/earnings/day-clean        — All: single day full list
GET /api/catalysts/earnings/week-clean       — Curated: scored weekly board
GET /api/catalysts/earnings/day-curated      — Curated: scored single day
GET /api/catalysts/earnings/week-all         — All: full week list
GET /api/catalysts/earnings/month-curated    — Curated: monthly calendar overview
"""
from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import date, datetime, timedelta
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
    limit_per_session: int           = Query(13, ge=1, le=26, description="Max events per session slot per day (default 13)"),
    max_total:         int           = Query(100, ge=1, le=165, description="Max events in topEvents (default 100)"),
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
      • Enrichment: top 180 candidates, concurrency=2, cache 24 h.
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


# ── GET /api/catalysts/earnings/day-curated ───────────────────────────────────

@router.get("/api/catalysts/earnings/day-curated")
@traceable(name="earnings_clean.day_curated")
async def day_curated(
    request: Request,
    api_key: str           = Header(None, alias=_AUTH_HEADER),
    date:    Optional[str] = Query(None,
                                   description="YYYY-MM-DD (required)"),
    search:  Optional[str] = Query(None),
    scope:   Optional[str] = Query(None, description="all | watchlist | portfolio"),
    limit:   int           = Query(25, ge=1, le=50,
                                   description="Max curated events returned (1-50, default 25)"),
):
    """
    Curated FMP earnings for a single date — same scoring / eligibility engine
    as week-clean.  Only high-signal events are returned; never pads with junk.

    Constraints:
      • 1 FMP calendar call; enrich up to 80 candidates, max 20 live fetches.
      • Concurrency=2. 429 circuit breaker → partial data.
      • No AI, no Finnhub, no Polymarket.

    Returns: { asOf, source, date, events[], count, status, errors }
    Event shape matches week-clean (session, themeTags, importanceScore, …).
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

    from services.earnings_clean_service import get_day_curated as _get_day_curated, _today
    if not date:
        date = _today()
    else:
        _validate_date(date, "date")

    result = await _get_day_curated(
        api_key=fmp_key,
        date=date,
        scope=scope,
        search=search,
        limit=limit,
    )
    return JSONResponse(content=result)


# ── GET /api/catalysts/earnings/week-all ─────────────────────────────────────

@router.get("/api/catalysts/earnings/week-all")
@traceable(name="earnings_clean.week_all")
async def week_all(
    request:    Request,
    api_key:    str           = Header(None, alias=_AUTH_HEADER),
    week_start: Optional[str] = Query(None, description="YYYY-MM-DD (Monday); default current week"),
    week_end:   Optional[str] = Query(None, description="YYYY-MM-DD (Friday); default current week Friday"),
    scope:      Optional[str] = Query(None, description="all | watchlist | portfolio"),
    search:     Optional[str] = Query(None, description="Ticker or company name filter"),
    limit:      int           = Query(5000, ge=1, le=10000,
                                      description="Max events in flat list (default 5000)"),
):
    """
    Full FMP earnings list for a business week — no scoring, no filtering.

    Mirrors upcoming-clean but scoped to one week.  No profile enrichment;
    logos populated via CDN URL.  Accepts camelCase weekStart / weekEnd aliases.

    Constraints:
      • Max 2 FMP calendar calls (one 7-day chunk, heavily cached).
      • 429 circuit breaker → partial data.

    Returns: { asOf, source, weekStart, weekEnd, events[], eventsByDate,
               countsByDate, fmpCallsUsed, rateLimited, status, errors }
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

    qp         = request.query_params
    week_start = week_start or qp.get("weekStart") or None
    week_end   = week_end   or qp.get("weekEnd")   or None

    if week_start:
        _validate_date(week_start, "week_start")
    if week_end:
        _validate_date(week_end, "week_end")

    from services.earnings_clean_service import get_week_all as _get_week_all

    result = await _get_week_all(
        api_key=fmp_key,
        week_start=week_start,
        week_end=week_end,
        scope=scope,
        search=search,
        limit=limit,
    )
    return JSONResponse(content=result)


# ── GET /api/catalysts/earnings/month-all ─────────────────────────────────────

@router.get("/api/catalysts/earnings/month-all")
@traceable(name="earnings_clean.month_all")
async def month_all(
    request: Request,
    api_key: str           = Header(None, alias=_AUTH_HEADER),
    year:    Optional[int] = Query(None, description="4-digit year (default current year)"),
    month:   Optional[int] = Query(None, ge=1, le=12,
                                   description="Month 1-12 (default current month)"),
    scope:   Optional[str] = Query(None, description="all | watchlist | portfolio"),
    search:  Optional[str] = Query(None),
    top_n:   int           = Query(5, ge=1, le=20,
                                   description="Max symbols shown per day (1-20, default 5)"),
):
    """
    Lightweight FMP earnings calendar for a full month.

    ZERO profile/quote enrichment — only raw FMP calendar data.
    Returns count-per-day and top N symbols sorted by revenue estimate.

    Budget:
      • Max 5 sequential FMP calendar calls (5 × 7-day chunks covers any month).
      • Zero live profile calls — no marketCap, no sector, no price fetches.
      • 429 circuit breaker (earnings-service only — Home/Sectors/Macro unaffected).
      • Cache: earnings:all:month:{year}:{month}, TTL 6 h.

    Returns: { asOf, source, year, month, monthLabel, monthStart, monthEnd,
               days[{date, dayOfMonth, isCurrentMonth, count, topSymbols[]}],
               fmpCallsUsed, rateLimited, status, errors }
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

    now = datetime.utcnow()
    y   = year  if year  else now.year
    m   = month if month else now.month

    from services.earnings_clean_service import get_month_all as _get_month_all

    result = await _get_month_all(
        api_key=fmp_key,
        year=y,
        month=m,
        scope=scope,
        search=search,
        top_n=top_n,
    )
    return JSONResponse(content=result)


# ── GET /api/catalysts/earnings/month-curated ─────────────────────────────────

@router.get("/api/catalysts/earnings/month-curated")
@traceable(name="earnings_clean.month_curated")
async def month_curated(
    request:     Request,
    api_key:     str           = Header(None, alias=_AUTH_HEADER),
    year:        Optional[int] = Query(None, description="4-digit year (default current year)"),
    month:       Optional[int] = Query(None, ge=1, le=12,
                                       description="Month 1-12 (default current month)"),
    scope:       Optional[str] = Query(None, description="all | watchlist | portfolio"),
    search:      Optional[str] = Query(None),
    max_per_day: int           = Query(9, ge=1, le=16,
                                       description="Max curated events per day (1-16, default 9)"),
):
    """
    Curated monthly earnings calendar overview.

    For each calendar day:
      count     = total FMP earnings that day (raw, before any filtering)
      topEvents = curated scored events (up to max_per_day)

    Curated pipeline is identical to week-clean scaled to a full month:
      • Raw month fetch in sequential 7-day chunks (≤ 5 FMP calls).
      • Enrich top 200 candidates, max 40 live profiles, concurrency=2.
      • 429 circuit breaker → partial result.
      • No AI, no Finnhub, no Polymarket.

    Returns: { asOf, source, year, month, monthLabel, monthStart, monthEnd,
               days[{date, dayOfMonth, isCurrentMonth, count, topEvents[]}],
               status, errors }
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

    now = datetime.utcnow()
    y   = year  if year  else now.year
    m   = month if month else now.month

    from services.earnings_clean_service import get_month_curated as _get_month_curated

    result = await _get_month_curated(
        api_key=fmp_key,
        year=y,
        month=m,
        scope=scope,
        search=search,
        max_per_day=max_per_day,
    )
    return JSONResponse(content=result)


# ── POST /api/catalysts/earnings/admin/rebuild-week ────────────────────────────
# Admin-only: rebuild the curated weekly snapshot for a specific week.
# Normal page loads NEVER call this — it triggers the full enrichment pipeline.

@router.post("/api/catalysts/earnings/admin/rebuild-week")
@traceable(name="earnings_clean.admin_rebuild_week")
async def admin_rebuild_week(
    request:    Request,
    api_key:    str           = Header(None, alias=_AUTH_HEADER),
    week_start: Optional[str] = Query(None,
                                       description="Monday YYYY-MM-DD (default: current week Mon)"),
    force:      bool          = Query(True,
                                       description="Force rebuild even if snapshot is fresh (default true)"),
    weeks:      int           = Query(1, ge=1, le=8,
                                       description="Number of consecutive weeks to rebuild (default 1)"),
):
    """
    Admin endpoint: rebuild curated weekly snapshot(s) via full enrichment pipeline.

    Use for:
      • Forcing a rebuild after scoring changes
      • Recovering from a bad snapshot
      • Pre-seeding snapshots for upcoming weeks

    Requires valid X-API-Key. Runs synchronously — may take 30-60s per week.
    Normal page load endpoints are NOT affected during rebuild (LKG still served).

    Returns per-week build results.
    """
    err = _check_key(api_key)
    if err:
        return err

    fmp_key = _get_fmp_key()
    if not fmp_key:
        return JSONResponse(
            status_code=503,
            content={"error": "FMP API key not configured"},
        )

    from services.earnings_clean_service import build_curated_week_snapshot
    import asyncio as _asyncio

    now = datetime.utcnow()
    if week_start:
        _validate_date(week_start, "week_start")
        try:
            ws = datetime.strptime(week_start, "%Y-%m-%d").date()
        except ValueError:
            ws = now.date()
    else:
        from datetime import date as _date
        today = _date.today()
        ws = today - timedelta(days=today.weekday())  # Monday

    results = []
    for i in range(weeks):
        mon = (ws + timedelta(weeks=i)).strftime("%Y-%m-%d")
        fri = (ws + timedelta(weeks=i) + timedelta(days=4)).strftime("%Y-%m-%d")
        try:
            result = await build_curated_week_snapshot(fmp_key, mon, fri, force=force)
            results.append({
                "week":          f"{mon}→{fri}",
                "status":        "ok",
                "events":        len(result.get("allEvents", [])),
                "focus":         sum(1 for e in result.get("allEvents", []) if e.get("isFocus")),
                "rawEvents":     result.get("rawEventsCount", 0),
                "calHttpCalls":  result.get("calHttpCalls", 0),
                "enrichHttpCalls": result.get("enrichHttpCalls", 0),
                "rateLimited":   result.get("rateLimited", False),
            })
            if i < weeks - 1:
                await _asyncio.sleep(3)   # stagger between weeks
        except Exception as e:
            results.append({
                "week":   f"{mon}→{fri}",
                "status": "error",
                "error":  str(e),
            })

    return JSONResponse(content={"rebuilt": results, "totalWeeks": weeks})


# ── GET /api/catalysts/earnings/admin/snapshot-status ─────────────────────────

@router.get("/api/catalysts/earnings/admin/snapshot-status")
@traceable(name="earnings_clean.admin_snapshot_status")
async def admin_snapshot_status(
    request:     Request,
    api_key:     str = Header(None, alias=_AUTH_HEADER),
    weeks_ahead: int = Query(6, ge=1, le=12,
                             description="Number of upcoming weeks to check (default 6)"),
):
    """
    Admin diagnostic: report snapshot status for upcoming weeks.

    Returns per-week:
      week_start, week_end, status (fresh/lkg/missing/building/failed),
      rows, focus, last_built, last_error, last_attempt, next_retry_at,
      memFresh, memLKG, diskFresh, diskLKG, memAgeS, diskAgeS.
    """
    err = _check_key(api_key)
    if err:
        return err

    from services.earnings_clean_service import (
        _snap_ck, _snap_lkg_ck, _snap_disk_path, _snap_lkg_disk_path,
        _read_earn_snap_from_disk, get_week_state,
    )
    from data.cache import cache
    from datetime import date as _date
    import time as _time

    _now = datetime.utcnow()
    today = _date.today()
    mon0  = today - timedelta(days=today.weekday())
    wall_now = _time.time()

    report = []
    for i in range(weeks_ahead):
        mon = (mon0 + timedelta(weeks=i)).strftime("%Y-%m-%d")
        fri = (mon0 + timedelta(weeks=i) + timedelta(days=4)).strftime("%Y-%m-%d")

        mem_snap  = cache.get(_snap_ck(mon, fri))
        mem_lkg   = cache.get(_snap_lkg_ck(mon, fri))
        disk_snap = _read_earn_snap_from_disk(_snap_disk_path(mon, fri))
        disk_lkg  = _read_earn_snap_from_disk(_snap_lkg_disk_path(mon, fri))

        best = mem_snap or disk_snap or mem_lkg or disk_lkg or {}

        mem_age  = int(wall_now - mem_snap["cached_at"])  if mem_snap  and mem_snap.get("cached_at")  else None
        disk_age = int(wall_now - disk_snap["cached_at"]) if disk_snap and disk_snap.get("cached_at") else None

        rows  = len(best.get("allEvents", []))
        focus = sum(1 for e in best.get("allEvents", []) if e.get("isFocus"))

        loop_state = get_week_state(mon)

        # Derive display status (loop state overrides if building/failed)
        if loop_state["status"] in ("building", "failed"):
            display_status = loop_state["status"]
        elif mem_snap is not None or disk_snap is not None:
            display_status = "fresh"
        elif mem_lkg is not None or disk_lkg is not None:
            display_status = "lkg"
        else:
            display_status = "missing"

        # Convert monotonic next_retry_at → wall-clock ISO string
        nr_mono = loop_state.get("next_retry_at")
        if nr_mono is not None:
            delta_s = nr_mono - _time.monotonic()
            nr_iso  = datetime.utcfromtimestamp(wall_now + delta_s).strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            nr_iso  = None

        last_built = loop_state.get("last_built")
        last_attempt = loop_state.get("last_attempt")

        report.append({
            "week_start":    mon,
            "week_end":      fri,
            "week":          f"{mon}→{fri}",
            "status":        display_status,
            "rows":          rows,
            "focus":         focus,
            "last_built":    datetime.utcfromtimestamp(last_built).strftime("%Y-%m-%dT%H:%M:%SZ")
                             if last_built else None,
            "last_error":    loop_state.get("last_error"),
            "last_attempt":  datetime.utcfromtimestamp(last_attempt).strftime("%Y-%m-%dT%H:%M:%SZ")
                             if last_attempt else None,
            "next_retry_at": nr_iso,
            "retry_count":   loop_state.get("retry_count", 0),
            "memFresh":      mem_snap  is not None,
            "memLKG":        mem_lkg   is not None,
            "diskFresh":     disk_snap is not None,
            "diskLKG":       disk_lkg  is not None,
            "memAgeS":       mem_age,
            "diskAgeS":      disk_age,
            "needsBuild":    display_status in ("missing", "failed"),
        })

    return JSONResponse(content={"asOf": _now.isoformat(), "weeks": report})


# ── POST /api/catalysts/earnings/admin/rebuild-missing ─────────────────────────
# Admin-only: sequential staggered build for any weeks that are currently missing.
# Skips weeks that already have valid fresh snapshots (unless force=true).

@router.post("/api/catalysts/earnings/admin/rebuild-missing")
@traceable(name="earnings_clean.admin_rebuild_missing")
async def admin_rebuild_missing(
    request:     Request,
    api_key:     str  = Header(None, alias=_AUTH_HEADER),
    weeks_ahead: int  = Query(5, ge=1, le=8,
                              description="How many weeks ahead to check/build (default 5)"),
    force:       bool = Query(False,
                              description="Rebuild even if fresh snapshot exists (default false)"),
    stagger_s:   int  = Query(30, ge=5, le=120,
                              description="Seconds to sleep between week builds (default 30)"),
):
    """
    Sequential staggered rebuild for weeks that are missing (or all if force=true).

    Processes current week + next N in priority order.
    Skips weeks with a valid fresh snapshot unless force=true.
    Returns per-week: built / skipped / failed / rate_limited.

    NOTE: This endpoint runs synchronously and may take several minutes
    (stagger_s × missing_weeks). Use the admin UI or call from a background script.
    """
    err = _check_key(api_key)
    if err:
        return err

    fmp_key = _get_fmp_key()
    if not fmp_key:
        return JSONResponse(status_code=503, content={"error": "FMP API key not configured"})

    from services.earnings_clean_service import (
        build_curated_week_snapshot,
        _snap_ck, _snap_disk_path, _read_earn_snap_from_disk,
    )
    from data.cache import cache
    import asyncio as _asyncio
    from datetime import date as _date

    today = _date.today()
    mon0  = today - timedelta(days=today.weekday())

    results = []
    for i in range(weeks_ahead):
        mon = (mon0 + timedelta(weeks=i)).strftime("%Y-%m-%d")
        fri = (mon0 + timedelta(weeks=i) + timedelta(days=4)).strftime("%Y-%m-%d")

        # Skip if fresh snapshot exists (unless force=true)
        if not force:
            if cache.get(_snap_ck(mon, fri)) is not None:
                results.append({"week": f"{mon}→{fri}", "status": "skipped", "reason": "fresh_snap_exists"})
                continue
            if _read_earn_snap_from_disk(_snap_disk_path(mon, fri)) is not None:
                results.append({"week": f"{mon}→{fri}", "status": "skipped", "reason": "fresh_disk_exists"})
                continue

        try:
            result = await build_curated_week_snapshot(fmp_key, mon, fri, force=force)
            if result.get("rateLimited"):
                results.append({
                    "week":        f"{mon}→{fri}",
                    "status":      "rate_limited",
                    "events":      len(result.get("allEvents", [])),
                    "focus":       sum(1 for e in result.get("allEvents", []) if e.get("isFocus")),
                    "calHttp":     result.get("calHttpCalls", 0),
                    "enrichHttp":  result.get("enrichHttpCalls", 0),
                })
            else:
                results.append({
                    "week":        f"{mon}→{fri}",
                    "status":      "built",
                    "events":      len(result.get("allEvents", [])),
                    "focus":       sum(1 for e in result.get("allEvents", []) if e.get("isFocus")),
                    "calHttp":     result.get("calHttpCalls", 0),
                    "enrichHttp":  result.get("enrichHttpCalls", 0),
                })
        except Exception as exc:
            results.append({"week": f"{mon}→{fri}", "status": "failed", "error": str(exc)})

        # Stagger between builds — avoid FMP rate limits
        if i < weeks_ahead - 1:
            await _asyncio.sleep(stagger_s)

    built_count    = sum(1 for r in results if r["status"] == "built")
    skipped_count  = sum(1 for r in results if r["status"] == "skipped")
    rl_count       = sum(1 for r in results if r["status"] == "rate_limited")
    failed_count   = sum(1 for r in results if r["status"] == "failed")

    return JSONResponse(content={
        "summary": {
            "built":        built_count,
            "skipped":      skipped_count,
            "rate_limited": rl_count,
            "failed":       failed_count,
            "total":        len(results),
        },
        "weeks": results,
    })


# ── GET /api/catalysts/earnings/portfolio-full-year ───────────────────────────

def _safe_f(v) -> float | None:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


@router.get("/api/catalysts/earnings/portfolio-full-year")
@traceable(name="earnings_clean.portfolio_full_year")
async def portfolio_full_year(
    request: Request,
    api_key: str = Header(None, alias=_AUTH_HEADER),
):
    """
    Full-year upcoming earnings for every portfolio holding (today → +365 days).

    Replaces the Express 52-week fan-out: loads portfolio tickers from Neon,
    builds 52 Monday-aligned weekly FMP calls in batches of 13, deduplicates to
    one row per ticker (earliest upcoming date), and returns a flat array sorted
    by date ascending.

    Response: { "earnings": [ { symbol, date, eps_estimate, revenue_estimate,
                                company_name }, ... ] }
    Cache: 10 min per portfolio symbol-set hash.
    """
    err = _check_key(api_key)
    if err:
        return err

    fmp_key = _get_fmp_key()
    if not fmp_key:
        return JSONResponse(
            status_code=503,
            content={"error": "FMP API key not configured", "earnings": []},
        )

    # ── Load portfolio symbols from Neon ──────────────────────────────────────
    try:
        from data.portfolio_store import load_active_holdings  # type: ignore
        holdings  = load_active_holdings()
        port_syms: set[str] = {
            (h.get("ticker") or h.get("symbol") or "").upper().strip()
            for h in holdings
            if (h.get("ticker") or h.get("symbol") or "").strip()
        }
    except Exception as _pe:
        print(f"[pfull_year] portfolio load error: {_pe}")
        port_syms = set()

    if not port_syms:
        return {"earnings": [], "portfolioCount": 0, "source": "neon"}

    # ── Endpoint-level cache (90 days, fixed key — patched incrementally) ───
    from data.cache import cache  # type: ignore
    from services.pfull_year_service import PFULL_CACHE_KEY, PFULL_TTL  # type: ignore
    cache_key = PFULL_CACHE_KEY
    hit = cache.get(cache_key)
    if hit is not None:
        return {"earnings": hit, "portfolioCount": len(port_syms),
                "source": "cache", "cacheKey": cache_key}

    # ── Build 52 weekly date ranges (today → today+364) ───────────────────────
    today = date.today()
    weeks: list[tuple[str, str]] = []
    cur = today
    for _ in range(52):
        weeks.append((cur.isoformat(), (cur + timedelta(days=6)).isoformat()))
        cur += timedelta(days=7)

    # ── Fan-out in 4 sequential batches of 13 (avoids FMP rate-limit bursts) ─
    from services.catalyst_calendar_service import CatalystFMP  # type: ignore
    fmp      = CatalystFMP(fmp_key)
    all_rows: list[dict] = []
    today_s  = today.isoformat()

    for batch_start in range(0, 52, 13):
        batch   = weeks[batch_start:batch_start + 13]
        results = await asyncio.gather(
            *[fmp.earnings_calendar(f, t) for f, t in batch],
            return_exceptions=True,
        )
        for res in results:
            if isinstance(res, list):
                all_rows.extend(res)

    # ── Filter to portfolio symbols with a future/today date ─────────────────
    filtered = [
        row for row in all_rows
        if (row.get("symbol") or "").upper() in port_syms
        and (row.get("date") or "") >= today_s
    ]

    # ── Deduplicate: one row per symbol, keep earliest upcoming date ──────────
    best: dict[str, dict] = {}
    for row in filtered:
        sym = (row.get("symbol") or "").upper()
        if sym not in best or (row.get("date") or "") < (best[sym].get("date") or ""):
            best[sym] = row

    # ── Build response — sort by date asc ────────────────────────────────────
    earnings = [
        {
            "symbol":           sym,
            "date":             row.get("date"),
            "eps_estimate":     _safe_f(row.get("epsEstimated")),
            "revenue_estimate": _safe_f(row.get("revenueEstimated")),
            "company_name":     row.get("name") or row.get("companyName") or None,
        }
        for sym, row in sorted(best.items(), key=lambda x: x[1].get("date") or "")
    ]

    cache.set(cache_key, earnings, PFULL_TTL)   # 90 days

    print(
        f"[pfull_year] portfolio={len(port_syms)} fmp_rows={len(all_rows)} "
        f"filtered={len(filtered)} deduped={len(earnings)}"
    )
    return {
        "earnings":       earnings,
        "portfolioCount": len(port_syms),
        "source":         "fmp",
        "cacheKey":       cache_key,
    }
