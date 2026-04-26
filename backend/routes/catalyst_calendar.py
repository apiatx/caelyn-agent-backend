"""
Catalyst Calendar router — /api/catalysts/*

All endpoints use FMP Starter plan data via catalyst_calendar_service.
No existing /api/earnings/* routes are touched.

Endpoints
─────────
GET /api/catalysts/overview
GET /api/catalysts/events
GET /api/catalysts/filters
GET /api/catalysts/ask-context
GET /api/catalysts/by-symbol/{symbol}
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Header, Query, Request
from fastapi.responses import JSONResponse

try:
    from slowapi import Limiter
    from slowapi.util import get_remote_address
    limiter = Limiter(key_func=get_remote_address)
    _has_limiter = True
except ImportError:
    _has_limiter = False

from config import FMP_API_KEY
from services.catalyst_calendar_service import (
    ALL_TABS,
    get_ask_context,
    get_by_symbol,
    get_events,
    get_filters,
    get_overview,
)

try:
    from langsmith import traceable
except ImportError:
    def traceable(*args, **kwargs):
        def _noop(fn):
            return fn
        if args and callable(args[0]):
            return args[0]
        return _noop

router = APIRouter(tags=["catalyst_calendar"])

_AUTH_HEADER = "X-API-Key"


def _check_key(api_key: Optional[str]) -> Optional[JSONResponse]:
    """Return a 401 response if the API key is invalid, else None."""
    from config import AGENT_API_KEY
    if AGENT_API_KEY and api_key != AGENT_API_KEY:
        return JSONResponse(status_code=401, content={"error": "Invalid or missing API key"})
    return None


# ── GET /api/catalysts/overview ───────────────────────────────────────────────

@router.get("/api/catalysts/overview")
@traceable(name="catalyst_calendar.overview")
async def catalyst_overview(
    request: Request,
    api_key: str = Header(None, alias=_AUTH_HEADER),
):
    """
    Return all 10 tabs in parallel, capped at 20 events each.
    Includes a summary of high-importance, watchlist, and portfolio catalysts.
    """
    err = _check_key(api_key)
    if err:
        return err

    if not FMP_API_KEY:
        return JSONResponse(
            status_code=503,
            content={"error": "FMP API key not configured", "status": "error"},
        )

    try:
        data = await get_overview(FMP_API_KEY)
        return JSONResponse(content=data)
    except Exception as e:
        print(f"[catalyst] overview unhandled: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "status": "error"},
        )


# ── GET /api/catalysts/events ─────────────────────────────────────────────────

@router.get("/api/catalysts/events")
@traceable(name="catalyst_calendar.events")
async def catalyst_events(
    request: Request,
    api_key: str = Header(None, alias=_AUTH_HEADER),
    tab: str = Query(
        default="all",
        description=f"Tab name or 'all'. Options: all, {', '.join(ALL_TABS)}",
    ),
    mode: str = Query(
        default="upcoming",
        description=(
            "'upcoming' (default) = future/calendar events. "
            "'recent' = historical events for list view. "
            "Default windows: earnings 30d, dividends 60d, "
            "ipos/splits 90d, economic/treasury 30d."
        ),
    ),
    from_date: Optional[str] = Query(
        default=None,
        description="Start date YYYY-MM-DD. Overrides mode default window.",
        alias="from",
    ),
    to_date: Optional[str] = Query(
        default=None,
        description="End date YYYY-MM-DD. Overrides mode default window.",
        alias="to",
    ),
    symbols: Optional[str] = Query(
        default=None,
        description="Comma-separated symbol list for explicit filtering.",
    ),
    scope: str = Query(
        default="all",
        description="'all', 'watchlist', or 'portfolio'.",
    ),
    sector: Optional[str] = Query(
        default=None,
        description="Filter by sector name (case-insensitive).",
    ),
    mc_bucket: Optional[str] = Query(
        default=None,
        description="Filter by marketCap bucket: mega, large, mid, small, micro.",
        alias="marketCap",
    ),
    event_type: Optional[str] = Query(
        default=None,
        description="Filter by eventType. Same values as tab.",
        alias="eventType",
    ),
    limit: int = Query(
        default=100,
        ge=1,
        le=500,
        description="Max events to return.",
    ),
):
    """
    Flexible event feed with filtering.

    Use `tab=all` for a unified sorted stream, or specify a single tab.
    Use `mode=upcoming` (default) for forward-looking calendar events.
    Use `mode=recent` for historical events in list view.
    """
    err = _check_key(api_key)
    if err:
        return err

    if not FMP_API_KEY:
        return JSONResponse(
            status_code=503,
            content={"error": "FMP API key not configured", "status": "error"},
        )

    if tab != "all" and tab not in ALL_TABS:
        return JSONResponse(
            status_code=400,
            content={"error": f"Unknown tab {tab!r}. Valid: all, {', '.join(ALL_TABS)}"},
        )

    if mode not in ("upcoming", "recent"):
        return JSONResponse(
            status_code=400,
            content={"error": f"Invalid mode {mode!r}. Valid: upcoming, recent"},
        )

    # Parse comma-separated symbols
    sym_set: Optional[set] = None
    if symbols:
        sym_set = {s.strip().upper() for s in symbols.split(",") if s.strip()}

    try:
        data = await get_events(
            fmp_key=FMP_API_KEY,
            tab=tab,
            from_date=from_date,
            to_date=to_date,
            symbols_filter=sym_set,
            scope=scope,
            sector=sector,
            mc_bucket=mc_bucket,
            event_type_filter=event_type,
            limit=limit,
            mode=mode,
        )
        return JSONResponse(content=data)
    except Exception as e:
        print(f"[catalyst] events unhandled: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "status": "error"},
        )


# ── GET /api/catalysts/filters ────────────────────────────────────────────────

@router.get("/api/catalysts/filters")
@traceable(name="catalyst_calendar.filters")
async def catalyst_filters(
    request: Request,
    api_key: str = Header(None, alias=_AUTH_HEADER),
):
    """
    Return available filter values: sectors, marketCapBuckets, eventTypes,
    watchlist symbols, and portfolio symbols.
    """
    err = _check_key(api_key)
    if err:
        return err

    try:
        data = await get_filters(FMP_API_KEY or "")
        return JSONResponse(content=data)
    except Exception as e:
        print(f"[catalyst] filters unhandled: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "status": "error"},
        )


# ── GET /api/catalysts/ask-context ────────────────────────────────────────────

@router.get("/api/catalysts/ask-context")
@traceable(name="catalyst_calendar.ask_context")
async def catalyst_ask_context(
    request: Request,
    api_key: str = Header(None, alias=_AUTH_HEADER),
    scope: str = Query(
        default="all",
        description="'all', 'watchlist', or 'portfolio'.",
    ),
    symbols: Optional[str] = Query(
        default=None,
        description="Comma-separated symbols to restrict context to.",
    ),
    from_date: Optional[str] = Query(
        default=None,
        description="Start date YYYY-MM-DD for the context window.",
        alias="from",
    ),
    to_date: Optional[str] = Query(
        default=None,
        description="End date YYYY-MM-DD for the context window.",
        alias="to",
    ),
    include_recent: bool = Query(
        default=True,
        description="Include recent (historical) events.",
        alias="includeRecent",
    ),
    include_upcoming: bool = Query(
        default=True,
        description="Include upcoming (future) events.",
        alias="includeUpcoming",
    ),
    refresh: bool = Query(
        default=False,
        description="Force-bypass the 4-hour cache and re-fetch all data.",
    ),
):
    """
    Full Catalyst Calendar context package across all six tabs for Ask Caelyn.

    Returns earnings, dividends, IPOs, splits, economic releases, and treasury
    data in a single response.  Reuses existing per-tab caches — no duplicated
    FMP calls.  Response is itself cached for 4 hours (bypass with refresh=true).

    Tabs: earnings_dates, dividends, ipos, splits, economic_releases, treasury_macro
    """
    err = _check_key(api_key)
    if err:
        return err

    if not FMP_API_KEY:
        return JSONResponse(
            status_code=503,
            content={"error": "FMP API key not configured", "status": "error"},
        )

    if scope not in ("all", "watchlist", "portfolio"):
        return JSONResponse(
            status_code=400,
            content={"error": f"Invalid scope {scope!r}. Valid: all, watchlist, portfolio"},
        )

    if not include_recent and not include_upcoming:
        return JSONResponse(
            status_code=400,
            content={"error": "At least one of includeRecent or includeUpcoming must be true"},
        )

    try:
        data = await get_ask_context(
            fmp_key=FMP_API_KEY,
            scope=scope,
            symbols=symbols,
            from_date=from_date,
            to_date=to_date,
            include_recent=include_recent,
            include_upcoming=include_upcoming,
            refresh=refresh,
        )
        return JSONResponse(content=data)
    except Exception as e:
        print(f"[catalyst] ask-context unhandled: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "status": "error"},
        )


# ── GET /api/catalysts/by-symbol/{symbol} ─────────────────────────────────────

@router.get("/api/catalysts/by-symbol/{symbol}")
@traceable(name="catalyst_calendar.by_symbol")
async def catalyst_by_symbol(
    request: Request,
    symbol: str,
    api_key: str = Header(None, alias=_AUTH_HEADER),
):
    """
    Return all upcoming and recent catalysts for a single ticker.
    Powers the symbol-level detail panel / popup.
    Includes company profile, earnings, dividends, splits, SEC filings,
    analyst ratings, and insider transactions.
    """
    err = _check_key(api_key)
    if err:
        return err

    if not FMP_API_KEY:
        return JSONResponse(
            status_code=503,
            content={"error": "FMP API key not configured", "status": "error"},
        )

    symbol = symbol.upper().strip()
    if not symbol or len(symbol) > 6:
        return JSONResponse(status_code=400, content={"error": "Invalid symbol"})

    try:
        data = await get_by_symbol(FMP_API_KEY, symbol)
        return JSONResponse(content=data)
    except Exception as e:
        print(f"[catalyst] by-symbol {symbol} unhandled: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "status": "error"},
        )
