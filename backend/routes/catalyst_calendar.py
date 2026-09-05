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
from services.calendar_snapshot_service import (
    HORIZON_TABS as _HORIZON_TABS,
    TARGET_TABS as _SNAPSHOT_TABS,
    get_read_source as _get_read_source,
    get_snapshot_async as _get_snapshot,
    get_snapshot_window_async as _get_snapshot_window,
)
from services.calendar_curation import (
    CURATED_TABS as _CURATED_TABS,
    DEFAULT_CAP_PER_SLICE as _CURATION_CAP,
    curate_envelope as _curate_envelope,
    get_canonical_macro_window as _get_canonical_macro_window,
)
from services.top_catalysts_service import (
    DEFAULT_CAP as _TOP_DEFAULT_CAP,
    MAX_CAP as _TOP_MAX_CAP,
    MIN_CAP as _TOP_MIN_CAP,
    get_top_catalysts as _get_top_catalysts,
)


router = APIRouter(tags=["catalyst_calendar"])

_AUTH_HEADER = "X-API-Key"

# Month view keeps a larger curation cap so all matching horizon events are
# returned (per-view; week/day/recent keep the normal display cap).
_MONTH_VIEW_CAP = 500


def _check_key(api_key: Optional[str]) -> Optional[JSONResponse]:
    """Return a 401 response if the API key is invalid, else None."""
    from config import AGENT_API_KEY
    if AGENT_API_KEY and api_key != AGENT_API_KEY:
        return JSONResponse(status_code=401, content={"error": "Invalid or missing API key"})
    return None


# ── GET /api/catalysts/overview ───────────────────────────────────────────────

@router.get("/api/catalysts/overview")
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
    view: Optional[str] = Query(
        default=None,
        description=(
            "Requested window for horizon tabs (economic_releases): "
            "'recent' | 'day' | 'week' | 'month'. Anchored at `date` (ET). "
            "Ignored for non-horizon snapshot tabs."
        ),
    ),
    date: Optional[str] = Query(
        default=None,
        description="Anchor date YYYY-MM-DD for the requested view window (ET).",
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
        le=5000,
        description=(
            "Max events to return.  For earnings_dates upcoming calendar view the "
            "effective minimum is 1000 so every weekday in the requested window "
            "is represented.  Pass limit=5000 to get the full FMP dataset."
        ),
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
        # Silently map legacy / unknown modes (e.g. "curated", "week") → upcoming
        # so frontend clients that pass non-standard mode strings still get data.
        print(f"[catalyst] mode={mode!r} not in (upcoming|recent) — remapping to upcoming")
        mode = "upcoming"

    # ── Snapshot short-circuit for weekly-cached tabs ──────────────────────
    # Dividends, IPOs, Splits, Economic Releases, Treasury/Macro are served
    # exclusively from the persistent weekly snapshot. They MUST NOT trigger
    # a live FMP fetch on request, regardless of mode. Earnings is excluded.
    if tab in _SNAPSHOT_TABS:
        # For horizon tabs (economic_releases) a requested window (view/date/
        # from/to) selects the matching day/week/month slice from the persisted
        # rolling `events` horizon, with truthful coverage metadata. Without a
        # requested window the existing full envelope is served unchanged.
        window_requested = bool(view or date or from_date or to_date)
        if window_requested and tab in _HORIZON_TABS:
            snap = await _get_snapshot_window(
                tab,
                view=view,
                date=date,
                from_date=from_date,
                to_date=to_date,
            )
        else:
            snap = await _get_snapshot(tab)
        # Display-layer curation. Raw Neon storage is unchanged; this only
        # trims/dedupes/re-ranks the response payload. Uses already-cached
        # event fields only — no FMP, no profile enrichment, no DB lookups
        # beyond watchlist/portfolio symbol sets.
        if tab in _CURATED_TABS:
            try:
                from services.catalyst_calendar_service import (
                    _load_watchlist_symbols,
                    _load_portfolio_symbols,
                )
                wl = _load_watchlist_symbols()
                pf = _load_portfolio_symbols()
            except Exception as e:
                print(f"[catalyst] curation watchlist load failed: {e}")
                wl, pf = set(), set()
            snap = _curate_envelope(
                tab, snap, cap=_CURATION_CAP, watchlist=wl, portfolio=pf,
            )
            if window_requested and tab in _HORIZON_TABS:
                # Curate the selected window through the shared canonical macro
                # window so Economic Releases, Calendar Top, and Home Top all
                # see the same logical events.
                macro_window = _get_canonical_macro_window(
                    snap.get("window_start") or snap.get("window", {}).get("requested_from"),
                    snap.get("window_end") or snap.get("window", {}).get("requested_to"),
                    include_treasury_context=False,
                    watchlist=wl,
                    portfolio=pf,
                    economic_envelope=snap,
                )
                snap["events"] = macro_window.get("macro_logical_events") or []
                snap["event_count"] = len(snap["events"])
                # Preserve truthful coverage metadata from the snapshot.
                snap["coverage_complete"] = macro_window.get("coverage_complete")
                snap["horizon_start"] = macro_window.get("horizon_start")
                snap["horizon_end"] = macro_window.get("horizon_end")

        # Additive window metadata (horizon tabs, requested-window path only).
        _window_fields = (
            {
                "view":              snap.get("view"),
                "requested_date":    snap.get("requested_date"),
                "window_start":      snap.get("window_start"),
                "window_end":        snap.get("window_end"),
                "event_count":       snap.get("event_count"),
                "coverage_complete": snap.get("coverage_complete"),
                "horizon_start":     snap.get("horizon_start"),
                "horizon_end":       snap.get("horizon_end"),
                "empty_reason":      snap.get("empty_reason"),
            }
            if window_requested and tab in _HORIZON_TABS else {}
        )

        return JSONResponse(content={
            "tab":           tab,
            "mode":          mode,
            **_window_fields,
            "current_week":  snap["current_week"],
            "previous_week": snap["previous_week"],
            "last_updated":  snap["last_updated"],
            "status":        snap["status"],
            "is_stale":      snap.get("is_stale", False),
            "bootstrapping": snap.get("bootstrapping", False),
            "window":        snap.get("window"),
            "diagnostics":   snap.get("diagnostics"),
            "events":        snap.get("events"),
            "horizon":       snap.get("horizon"),
            "coverage":      snap.get("coverage"),
        })

    # Parse comma-separated symbols
    sym_set: Optional[set] = None
    if symbols:
        sym_set = {s.strip().upper() for s in symbols.split(",") if s.strip()}

    # For earnings_dates calendar upcoming: override the 100-event default so
    # every weekday in the requested window gets events.  Callers that want a
    # smaller slice can pass an explicit limit.
    effective_limit = limit
    if tab == "earnings_dates" and mode == "upcoming" and limit == 100:
        effective_limit = 1000

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
            limit=effective_limit,
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


# ── GET /api/debug/calendar-snapshots ─────────────────────────────────────────
#
# Lightweight authenticated diagnostics for verifying that the deployed API is
# running the latest code AND can see the same Neon snapshots that the manual
# CLI backfill writes. Does NOT trigger FMP. Does NOT expose secrets.
#
# Auth: requires header X-Diagnostics-Token matching env var
# CALENDAR_DIAGNOSTICS_TOKEN (preferred) or DIAGNOSTICS_TOKEN. If neither env
# var is set, the endpoint returns 404 so it is invisible by default.

_DIAG_HEADER = "X-Diagnostics-Token"


def _resolve_diagnostics_token() -> Optional[str]:
    import os
    return (
        os.getenv("CALENDAR_DIAGNOSTICS_TOKEN")
        or os.getenv("DIAGNOSTICS_TOKEN")
        or None
    )


def _resolve_git_commit() -> str:
    """Best-effort commit/version resolver. Never raises."""
    import os
    for var in (
        "GIT_SHA",
        "GIT_COMMIT",
        "COMMIT_SHA",
        "SOURCE_VERSION",
        "RENDER_GIT_COMMIT",
        "VERCEL_GIT_COMMIT_SHA",
        "REPL_SLUG",
    ):
        val = os.getenv(var)
        if val:
            return f"{var}={val}"
    try:
        import subprocess
        from pathlib import Path
        repo_root = Path(__file__).resolve().parent.parent.parent
        out = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            capture_output=True, text=True, timeout=2,
        )
        if out.returncode == 0 and out.stdout.strip():
            return out.stdout.strip()
    except Exception:
        pass
    return "unknown"


def _neon_url_fingerprint() -> dict:
    """
    Return a SAFE summary of the Neon URL: presence boolean and (if present)
    a short host hash so two environments can be compared without leaking
    credentials. Never returns the raw URL or any password.
    """
    import os
    raw = os.environ.get("NEON_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not raw:
        return {"neon_database_url_present": False}
    info: dict = {"neon_database_url_present": True}
    try:
        from urllib.parse import urlparse
        parsed = urlparse(raw)
        host = parsed.hostname or ""
        if host:
            import hashlib
            info["neon_host_sha256_prefix"] = hashlib.sha256(
                host.encode("utf-8")
            ).hexdigest()[:12]
    except Exception:
        pass
    return info


@router.get("/api/debug/calendar-snapshots")
async def debug_calendar_snapshots(
    request: Request,
    token: str = Header(None, alias=_DIAG_HEADER),
):
    """
    Authenticated diagnostics for the calendar snapshot pipeline.

    Reports the loaded git commit, Neon URL presence (no secret leakage),
    per-tab snapshot row counts, and which backing store the read path
    actually resolves to (neon vs disk_fallback vs empty).
    """
    expected = _resolve_diagnostics_token()
    if not expected:
        return JSONResponse(
            status_code=404,
            content={"error": "Not Found"},
        )
    if not token or token != expected:
        return JSONResponse(
            status_code=403,
            content={"error": "Forbidden"},
        )

    payload: dict = {
        "git_commit": _resolve_git_commit(),
        **_neon_url_fingerprint(),
        "target_tabs": list(_SNAPSHOT_TABS),
    }

    # Neon connectivity probe (safe, no secrets returned).
    try:
        from data.pg_storage import is_available, get_last_conn_error
        payload["neon_connected"] = bool(is_available())
        if not payload["neon_connected"]:
            payload["neon_last_conn_error"] = get_last_conn_error()
    except Exception as e:
        payload["neon_connected"] = False
        payload["neon_probe_error"] = str(e)

    # Per-tab read-source + counts. Each tab call is best-effort; on error we
    # record the error and continue so the endpoint never 500s.
    tabs: dict = {}
    for tab in _SNAPSHOT_TABS:
        try:
            tabs[tab] = _get_read_source(tab)
        except Exception as e:
            tabs[tab] = {"source": "error", "error": str(e)}
    payload["tabs"] = tabs

    # Convenience: confirm the read path /api/catalysts/events would use for
    # snapshot tabs. It is always the snapshot service for these tabs.
    payload["events_read_path"] = "calendar_snapshot_service.get_snapshot"

    return JSONResponse(content=payload)


# ── GET /api/catalysts/top ────────────────────────────────────────────────────

@router.get("/api/catalysts/top")
async def catalyst_top(
    request: Request,
    api_key: str = Header(None, alias=_AUTH_HEADER),
    cap: int = Query(
        default=_TOP_DEFAULT_CAP,
        ge=_TOP_MIN_CAP,
        le=_TOP_MAX_CAP,
        description=(
            f"Max events returned in current_week. "
            f"Clamped to [{_TOP_MIN_CAP}, {_TOP_MAX_CAP}]."
        ),
    ),
):
    """
    Top Catalysts This Week — high-signal weekly intelligence layer.

    Grouped by day:
      days[].earnings — top earnings ranked on options flow + watchlist + sector
      days[].macro    — whitelisted macro events (CPI/PPI/NFP/FOMC/GDP/Treasury)
      days[].other    — rare IPO/dividend/split (cap 2-3/week, large-cap or hot theme)

    `current_week` is also returned as a flat ranked list for backward compat
    with previous clients.

    Pure read across already-cached services. No request-time FMP / Finnhub /
    profile-enrichment / external calls.
    """
    err = _check_key(api_key)
    if err:
        return err

    try:
        data = _get_top_catalysts(cap=cap)
        return JSONResponse(content=data)
    except Exception as e:
        print(f"[catalyst] top unhandled: {e}")
        return JSONResponse(
            status_code=500,
            content={
                "tab":           "top_catalysts",
                "mode":          "weekly",
                "week":          "",
                "days":          [],
                "current_week":  [],
                "previous_week": [],
                "last_updated":  None,
                "status":        "empty",
                "error":         str(e),
            },
        )
