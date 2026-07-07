from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Header, HTTPException, Body, Depends, Query
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse, FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from subscription import require_subscription
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from pydantic import BaseModel, ConfigDict
from typing import List, Optional

import asyncio
import json as _json
import os
import time
import uuid as _uuid
from datetime import datetime as _dt, timezone as _tz
from agent.mode_normalizer import (
    normalize_reasoning_model, normalize_collab_preset,
    mode_concept, mode_display_label, collab_preset_display_label,
    DEFAULT_PRESET_COLLABORATORS, DEFAULT_PRESET_PRIMARY,
    COLLAB_PRESET_DEFAULT, COLLAB_PRESET_AUTO, COLLAB_PRESET_FULL, COLLAB_PRESET_CUSTOM,
)

import config as _cfg  # noqa: F401  — triggers os.environ.setdefault calls


from pathlib import Path
from urllib.parse import urlparse, unquote
from agent.model_policy import (  # Phase 3: centralized model registry
    MODEL_CLAUDE_FAST,
    MODEL_CLAUDE_BALANCED,
    MODEL_CLAUDE_PREMIUM,
)

# ── Hyperliquid Screener service ──────────────────────────────────────────────
from services.hyperliquid.state import HyperliquidState as _HLState
from services.hyperliquid.router import router as _hl_router, set_state as _hl_set_state
from services.hyperliquid.websocket_manager import boot_and_run as _hl_boot_and_run
from services.sector_rotation.router import router as _sr_router, sectors_router as _sectors_router
from services.insider_activity_service import (
    router as _insider_router,
    insider_activity_background_loop as _insider_bg_loop,
)
from services.congressional_trading_service import (
    router as _cong_router,
    congressional_trading_background_loop as _cong_bg_loop,
)
from services.whale_watch_service import (
    router as _whale_router,
    whale_watch_background_loop as _whale_bg_loop,
    _create_tables as _whale_create_tables,
    seed_whales as _seed_whales,
)
from services.predict.router import router as _predict_router
from services.predict.investor.router import router as _investor_router
from services.bittensor.router import router as _bittensor_router
from services.watchlist_router import router as _watchlist_router

_hl_state = _HLState()
_hl_set_state(_hl_state)
# ─────────────────────────────────────────────────────────────────────────────

AGENT_API_KEY = os.getenv("AGENT_API_KEY")
_pg_startup_checked = False
_pg_startup_attempts = 0
_pg_last_init_error = None


def _init_postgres_chat_storage_on_startup(reason: str = "startup"):
    """Ensure PostgreSQL chat tables exist in the live runtime entrypoint."""
    global _pg_startup_checked, _pg_startup_attempts, _pg_last_init_error
    _pg_startup_attempts += 1

    db_url = os.getenv("NEON_DATABASE_URL") or os.getenv("DATABASE_URL")
    print(f"[STARTUP][PG] reason={reason} attempt={_pg_startup_attempts} db_configured={'YES' if bool(db_url) else 'NO'} source={'NEON' if os.getenv('NEON_DATABASE_URL') else 'REPLIT_INTERNAL' if os.getenv('DATABASE_URL') else 'NONE'}")
    if not db_url:
        print("[STARTUP][PG] Skipping PostgreSQL init — no database URL configured")
        _pg_last_init_error = "No database URL configured"
        return

    if _pg_startup_checked:
        _pg_last_init_error = None
        print("[STARTUP][PG] PostgreSQL init already completed in this process")
        return

    try:
        from data.pg_storage import startup_probe as _pg_probe, init_tables as _pg_init

        before = _pg_probe()
        print(
            f"[STARTUP][PG] connection={'OK' if before.get('connected') else 'FAILED'} "
            f"database={before.get('database')} schema={before.get('schema')} tables_before={before.get('tables', [])}"
        )

        print("[STARTUP][PG] table initialization start (schema=public)")
        ok = _pg_init()
        print(f"[STARTUP][PG] table initialization {'SUCCESS' if ok else 'FAIL'}")
        if not ok:
            _pg_last_init_error = "init_tables returned False"
            return

        # Seed user-approved category corrections (idempotent upsert)
        try:
            from services.category_overrides import seed_initial_overrides as _seed_overrides
            _seed_overrides(user_id="default")
        except Exception as _seed_err:
            print(f"[STARTUP] category override seeding failed (non-fatal): {_seed_err}")

        # Seed user-approved company name corrections (idempotent upsert)
        try:
            from services.name_overrides import seed_initial_name_overrides as _seed_names
            _seed_names(user_id="default")
        except Exception as _seed_name_err:
            print(f"[STARTUP] name override seeding failed (non-fatal): {_seed_name_err}")

        after = _pg_probe()
        print(
            f"[STARTUP][PG] tables_after={after.get('tables', [])} "
            f"has_conversations={'conversations' in after.get('tables', [])} "
            f"has_messages={'messages' in after.get('tables', [])}"
        )
        _pg_startup_checked = True
        _pg_last_init_error = None
    except Exception as e:
        _pg_last_init_error = str(e)
        print(f"[STARTUP][PG] FATAL PostgreSQL startup init error: {e}")



# Eager bootstrap in the actual module entrypoint path (uvicorn imports main:app).
_init_postgres_chat_storage_on_startup("module_import")

def _jwt_or_key(request: Request, api_key) -> bool:
    """Auth disabled — always allow. Re-enable when login page is ready."""
    return True


async def _alert_bus_fire(
    source: str, user_id: str, ticker: str, metrics: dict, raw: dict | None = None
):
    """
    Thin fire-and-forget wrapper around alert_signal_bus.record_signal_snapshot.
    Called via asyncio.create_task() so it never blocks the caller.
    Never raises — all errors are silently swallowed.
    Zero provider calls guaranteed by the bus itself.
    """
    try:
        from services.alert_signal_bus import record_signal_snapshot as _rs
        await _rs(source, user_id, ticker, metrics, raw)
    except Exception:
        pass

# ── Auth middleware (pure ASGI — no BaseHTTPMiddleware) ──────────
# BaseHTTPMiddleware is known to break StreamingResponse by buffering the
# body through an internal pipe. This pure ASGI implementation passes the
# response through without touching it, which is critical for the keepalive
# streaming used by /api/query.
#
# Public paths that do NOT require a valid JWT token
_AUTH_PUBLIC_PATHS = {
    "/api/auth/login",
    "/api/auth/verify",
    "/api/auth/logout",
    "/api/presets",
    "/",
    "/ping",
    "/health",
    "/docs",
    "/openapi.json",
    "/redoc",
    "/should-i-be-trading",
    "/api/trading-dashboard",
    "/api/trading-dashboard/refresh",
}


class JWTAuthMiddleware:
    """DISABLED — pass-through. Auth is handled by _jwt_or_key() in endpoints.
    JWT login can be re-enabled later by restoring the middleware logic."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        await self.app(scope, receive, send)


async def _x_consensus_loop():
    """Background loop: refresh X Select Trader Consensus once daily at 10:00 AM Chicago.

    Sleeps until the next 10:00 AM America/Chicago, fires the Grok/XAI refresh,
    then sleeps until the following 10:00 AM.  Uses the same lock as the
    on-demand path so manual and scheduled refreshes never race.

    The screener-hub scheduler's social_scan slot is intentionally offset to
    10:10 AM CT (11:10 AM ET) so it always rebuilds the social universe from
    the freshly written cache, not yesterday's snapshot.
    Saturday is skipped on both this loop and the scheduler.
    """
    import asyncio as _asyncio
    from datetime import datetime, timedelta, timezone as _tz
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo

    loop = _asyncio.get_event_loop()
    await loop.run_in_executor(None, _init_event.wait, 30)

    if data_service is None or not getattr(data_service, "xai", None):
        print("[X_CONSENSUS_LOOP] xAI provider not available — background loop disabled")
        return

    from services.x_consensus_cache import _run_refresh, _CACHE_PATH, _CACHE_TTL_SECONDS
    import time as _xc_time

    _CT = ZoneInfo("America/Chicago")
    _TARGET_HOUR = 10  # 10:00 AM Chicago (= 11:00 AM ET)

    print("[X_CONSENSUS_LOOP] Started — scheduled once daily at 10:00 AM America/Chicago")

    # ── Startup catch-up ────────────────────────────────────────────────────
    # Problem: if the server restarts AFTER 10 AM CT (deployments, checkpoints,
    # manual restarts), the loop below always computes "next target = tomorrow
    # 10 AM" and sleeps.  Repeated restarts after 10 AM mean the cache can go
    # days without refreshing — users see "Last Updated N days ago".
    # Fix: on startup, if the disk cache is stale AND we're within business
    # hours (08:00–20:00 CT, not Saturday), fire immediately before entering
    # the daily sleep/fire loop.
    _now_ct_su  = datetime.now(_CT)
    _in_window  = 8 <= _now_ct_su.hour < 20
    _not_sat    = _now_ct_su.weekday() != 5  # Saturday = 5

    if _CACHE_PATH.exists():
        _cache_age  = _xc_time.time() - _CACHE_PATH.stat().st_mtime
    else:
        _cache_age  = float("inf")  # file missing → treat as infinitely stale

    _is_stale   = _cache_age > _CACHE_TTL_SECONDS

    if _is_stale and _in_window and _not_sat:
        print(
            f"[X_CONSENSUS_LOOP] Startup catch-up: cache is "
            f"{_cache_age / 3600:.1f}h old (TTL={_CACHE_TTL_SECONDS / 3600:.0f}h), "
            f"within business hours — firing refresh now instead of waiting until tomorrow"
        )
        try:
            await _run_refresh(data_service)
        except Exception as _su_exc:
            print(f"[X_CONSENSUS_LOOP] Startup catch-up error: {_su_exc}")
        await _asyncio.sleep(90)  # buffer before computing next target
    else:
        print(
            f"[X_CONSENSUS_LOOP] Startup catch-up skipped: "
            f"cache_age={_cache_age / 3600:.1f}h "
            f"ttl={_CACHE_TTL_SECONDS / 3600:.0f}h "
            f"stale={_is_stale} in_window={_in_window} not_saturday={_not_sat}"
        )

    while True:
        now_ct = datetime.now(_CT)

        # Next 10:00 AM: today if before 10:00, otherwise tomorrow
        if now_ct.hour < _TARGET_HOUR:
            target_date = now_ct.date()
        else:
            target_date = now_ct.date() + timedelta(days=1)

        next_target_ct = datetime(
            target_date.year, target_date.month, target_date.day,
            _TARGET_HOUR, 0, 0,
            tzinfo=_CT,
        )
        sleep_secs = max(
            (next_target_ct.astimezone(_tz.utc) - datetime.now(_tz.utc)).total_seconds(),
            0,
        )
        print(
            f"[X_CONSENSUS_LOOP] Next refresh: {next_target_ct.isoformat()} "
            f"(sleeping {sleep_secs / 3600:.1f}h)"
        )
        await _asyncio.sleep(sleep_secs)

        # Saturday (weekday 5) — skip entirely, no Grok call
        if datetime.now(_CT).weekday() == 5:
            print("[X_CONSENSUS_LOOP] Saturday — skipping refresh (no Grok call)")
        else:
            print("[X_CONSENSUS_LOOP] 10:00 AM CT reached — running daily Grok/XAI refresh")
            try:
                await _run_refresh(data_service)
            except Exception as exc:
                print(f"[X_CONSENSUS_LOOP] Refresh error: {exc}")

        # Small buffer before recalculating next target (avoids same-minute re-trigger)
        await _asyncio.sleep(90)


async def _terminal_prewarm():
    """Build the Caelyn Terminal cache in the background at startup.

    Waits for the data_service (Tradier / yfinance) to be ready, then kicks
    off a full terminal build so the first user request hits the cache instead
    of blocking a worker for 60-120 s.  Failures are logged and silently
    swallowed — they never affect the startup sequence.
    """
    import asyncio as _asyncio_pw
    await _asyncio_pw.sleep(15)          # let _do_init thread finish first
    try:
        await _wait_for_init()           # data_service ready
        from data.portfolio_store import canonical_file as _cf, load_active_holdings as _lh
        holdings = _lh()
        if not holdings:
            print("[TERMINAL_PREWARM] no holdings — skipping pre-warm")
            return
        from data.caelyn_terminal import CaelynTerminalProvider
        provider = CaelynTerminalProvider(
            tradier=data_service.tradier   if data_service else None,
            finnhub=data_service.finnhub   if data_service else None,
            fmp=data_service.fmp           if data_service else None,
            yahoo=data_service.yahoo       if data_service else None,
            coingecko=data_service.coingecko if data_service else None,
        )
        await provider.get(_cf())
        print(f"[TERMINAL_PREWARM] cache built for {len(holdings)} holdings")
    except Exception as _pw_err:
        print(f"[TERMINAL_PREWARM] error (non-fatal): {_pw_err}")


async def _home_planning_warmup_loop():
    """
    Hourly background loop. On Saturday ET, proactively warms next-week macro
    events for the Home Top Catalysts planning feed into the process-local cache.
    On other days it is a no-op (cheap early-exit check once per hour).
    The in-process cache has a 23h TTL so the first Saturday request is served
    instantly instead of triggering an inline FMP fetch.
    """
    await asyncio.sleep(60)           # brief startup delay — non-blocking
    while True:
        try:
            try:
                from zoneinfo import ZoneInfo as _ZI
            except ImportError:
                from backports.zoneinfo import ZoneInfo as _ZI  # type: ignore
            from datetime import datetime as _dt
            now_et = _dt.now(_ZI("America/New_York"))
            if now_et.weekday() == 5:  # Saturday only
                from services.home_top_catalysts import warm_planning_window
                print("[HOME_PLANNING_WARMUP] Saturday detected — warming next-week catalysts")
                await warm_planning_window()
        except Exception as _hwl_err:
            print(f"[HOME_PLANNING_WARMUP] error (non-fatal): {_hwl_err}")
        await asyncio.sleep(3600)     # re-check every hour


async def _odds_scanner_loop():
    """
    Tracked Odds Registry — scan and persist loop.

    Fetches live Polymarket markets every 30 minutes, matches them to all
    registry families, writes snapshots to Neon/Postgres (7-day retention),
    computes 1h/24h/7d deltas from stored history, and caches the live payload.

    Runs 90 s after startup so the Polymarket cache is warm but before the
    investor intelligence loop (120 s) fires — ensuring get_intelligence()
    sees a populated odds_scanner on its first call.

    Kill switch: set ODDS_SCANNER_ENABLED=false to disable entirely.
    """
    import os as _os_env
    if _os_env.getenv("ODDS_SCANNER_ENABLED", "true").strip().lower() == "false":
        print("[ODDS_SCANNER] disabled via ODDS_SCANNER_ENABLED=false — loop not started")
        return

    await asyncio.sleep(90)    # let Polymarket cache warm; beat intelligence loop
    while True:
        try:
            from services.predict.odds_scanner import odds_scanner as _os
            await _os.scan_and_persist()
            print("[ODDS_SCANNER] Tracked odds scan complete — snapshot persisted")
        except Exception as _osl_err:
            print(f"[ODDS_SCANNER] scan error (non-fatal): {_osl_err}")
        await asyncio.sleep(1800)   # 30 minutes


async def _investor_intelligence_loop():
    """
    Pre-warm the Predict page investor intelligence cache every 30 minutes.

    Builds: tracked macro odds families, event-family-centric equity signals,
    watchlist-first ticker resolution, and diagnostics.

    Uses a 2-min startup delay so the Polymarket scored-markets cache is already
    warm before the first intelligence build fires.
    """
    await asyncio.sleep(120)   # let Polymarket scored-markets cache warm first
    while True:
        try:
            from services.predict.investor.investor_intel import investor_intel as _ii
            await _ii.get_intelligence()
            print("[INVESTOR_INTEL] Intelligence cache refreshed (tracked_odds + equity_signals)")
        except Exception as _iil_err:
            print(f"[INVESTOR_INTEL] refresh error (non-fatal): {_iil_err}")
        await asyncio.sleep(1800)   # 30 minutes


@asynccontextmanager
async def lifespan(app):
    _init_postgres_chat_storage_on_startup("lifespan")

    # Diagnostic: confirm storage backends
    try:
        from data.prompt_history import _use_postgres as _ph_pg, _use_object_storage as _ph_obj, _use_replit_db as _ph_db
        from data.chat_history import _use_postgres as _ch_pg, _use_object_storage as _ch_obj, _use_replit_db as _ch_db
        _ph_backend = "PostgreSQL (persistent)" if _ph_pg else ("Object Storage (persistent)" if _ph_obj else ("Replit DB (dev)" if _ph_db else "JSON files (EPHEMERAL!)"))
        _ch_backend = "PostgreSQL (persistent)" if _ch_pg else ("Object Storage (persistent)" if _ch_obj else ("Replit DB (dev)" if _ch_db else "JSON files (EPHEMERAL!)"))
        print(f"[STARTUP] prompt_history backend: {_ph_backend}")
        print(f"[STARTUP] chat_history backend: {_ch_backend}")
    except Exception as _e:
        print(f"[STARTUP] Storage diagnostic error: {_e}")

    # Portfolio holdings source-of-truth audit (runs migration if first boot)
    try:
        from data.portfolio_store import startup_audit as _portfolio_startup_audit
        _portfolio_startup_audit()
    except Exception as _psa_err:
        print(f"[portfolio-source-audit] startup_audit error: {_psa_err}")

    # Manual anchor bottlenecks overlay table
    try:
        from data.manual_anchor_bottlenecks_store import ensure_manual_anchor_table
        ensure_manual_anchor_table()
    except Exception as _mab_err:
        print(f"[MANUAL_ANCHOR] startup ensure error: {_mab_err}")

    import threading
    threading.Thread(target=_do_init, daemon=True).start()
    asyncio.create_task(_briefing_precompute_loop())
    asyncio.create_task(_edgar_cache_loop())
    _load_lkg_from_disk()          # Warm per-tab LKG cache from disk (backward compat)
    _load_prefilter_from_disk()    # Warm per-tab prefilter cache (backward compat)
    _load_master_lkg_from_disk()       # Warm master screener LKG — serves on first request
    _load_master_prefilter_from_disk() # Warm master prefilter — skips cold build on restart
    # Warm supplement LKG — theme-only options data persisted from previous sessions
    try:
        from data.options_theme_supplement import _load_supplement_lkg_from_disk as _load_supp_lkg
        _load_supp_lkg()
    except Exception as _supp_lkg_err:
        print(f"[STARTUP] Supplement LKG load failed (non-fatal): {_supp_lkg_err}")
    # Warm Sectors universe LKG — full theme universe snapshot for immediate post-restart coverage
    try:
        from data.options_theme_supplement import load_sectors_universe_lkg_from_disk as _load_sectors_lkg
        _load_sectors_lkg()
    except Exception as _sectors_lkg_err:
        print(f"[STARTUP] Sectors universe LKG load failed (non-fatal): {_sectors_lkg_err}")
    # Warm options instrument type LKG (ETF vs stock) from screener_fundamentals_cache DB
    try:
        from data.options_instrument_type_service import warm_up_from_db as _warm_itype
        _itype_count = _warm_itype()
        print(f"[STARTUP] instrument_type warm-up: classified {_itype_count} symbols from screener cache")
    except Exception as _itype_err:
        print(f"[STARTUP] instrument_type warm-up failed (non-fatal): {_itype_err}")
    # Launch background FMP classification pass for any symbols still "unknown"
    # after the DB warm-up (symbols absent from screener_fundamentals_cache).
    async def _itype_classify_startup():
        await asyncio.sleep(30)   # let the master screener initialize first
        try:
            from data.options_instrument_type_service import (
                get_stats as _ityp_stats,
                classify_symbols_background as _ityp_classify,
            )
            from data.options_theme_supplement import get_theme_proxy_symbols_for_supplement as _ityp_theme_syms
            from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE as _ityp_univ
            _ityp_all = set()
            for _m in _ityp_univ.values():
                for _s in (_m.get("proxy_symbols") or []):
                    _ityp_all.add(_s.upper())
            _ityp_all.update(s.upper() for s in _ityp_theme_syms())
            _ityp_s0 = _ityp_stats()
            if _ityp_s0.get("unknown", 0) > 0 and data_service and data_service.fmp:
                _classified = await _ityp_classify(
                    list(_ityp_all), fmp_provider=data_service.fmp, max_per_pass=40
                )
                _ityp_s1 = _ityp_stats()
                print(
                    f"[STARTUP] instrument_type FMP pass: classified {_classified} new symbols; "
                    f"unknown remaining={_ityp_s1.get('unknown', 0)}"
                )
        except Exception as _ityp_e:
            print(f"[STARTUP] instrument_type FMP pass error (non-fatal): {_ityp_e}")
    asyncio.create_task(_itype_classify_startup())
    asyncio.create_task(_master_screener_loop())
    asyncio.create_task(_sectors_fast_backfill_loop())
    asyncio.create_task(_theme_options_supplement_loop())
    # Tradier precompute loop removed — Options Flow now uses TradierFlowEngine directly
    asyncio.create_task(_polygon_options_ingestion_loop())
    asyncio.create_task(_macro_precompute_loop())
    asyncio.create_task(_strategy_history_precompute_loop())
    # _sector_rotation_precompute_loop DISABLED (2025-05):
    # /api/themes/relative-strength?classification=sector now covers all 11 SPDR sector RS data
    # via theme_rs_service (warmup_theme_rs loop). Running both loops duplicated Tradier + yfinance
    # calls for the same 11 SPDR ETFs + QQQ every 15 minutes into separate cache namespaces.
    # The function is preserved below for reference.  Old /api/sector-rotation/dashboard and
    # /api/sectors/page-data endpoints still work lazily (populated on first request via sr:dashboard:v1).
    # asyncio.create_task(_sector_rotation_precompute_loop())
    asyncio.create_task(_insider_bg_loop())
    asyncio.create_task(_cong_bg_loop())
    asyncio.create_task(_hl_boot_and_run(_hl_state))
    try:
        _whale_create_tables()
        asyncio.create_task(_seed_whales())
    except Exception as _e:
        print(f"[STARTUP] Whale Watch DB init error: {_e}")
    asyncio.create_task(_whale_bg_loop())
    try:
        from services.bittensor.router import _dashboard_refresh_loop as _bittensor_refresh_loop
        asyncio.create_task(_bittensor_refresh_loop())
    except Exception as _e:
        print(f"[STARTUP] Bittensor refresh task error: {_e}")
    asyncio.create_task(_x_consensus_loop())
    # Alert Signal Bus: periodic retention cleanup (every 12 h)
    async def _alert_bus_retention_loop():
        import asyncio as _aio
        while True:
            await _aio.sleep(12 * 3600)
            try:
                from services.alert_signal_bus import run_retention_cleanup as _arc
                result = await _arc()
                print(f"[ALERT_BUS] Retention cleanup: {result}")
            except Exception as _arc_e:
                print(f"[ALERT_BUS] Retention cleanup error (non-fatal): {_arc_e}")
    asyncio.create_task(_alert_bus_retention_loop())

    # Watchlist Fundamentals weekly FMP refresh — checks every hour, refreshes
    # symbols whose next_refresh_at <= NOW (set to upload_time + 7 days on CSV save).
    # Never runs on page render. Never storms FMP. Uses existing rate limiter cadence.
    try:
        from data.watchlist_fundamentals_store import ensure_table as _fund_ensure_table
        _fund_ensure_table()
    except Exception as _fund_init_e:
        print(f"[STARTUP] watchlist_fundamentals_cache table init error (non-fatal): {_fund_init_e}")

    # Job-level lock — prevents overlap if a prior run is still in progress.
    _fund_weekly_lock = asyncio.Lock()

    # ── Maintenance window helpers ────────────────────────────────────────────
    # Sunday 02:00–05:00 America/New_York (handles EST/EDT automatically).
    def _fund_in_window(now_utc) -> bool:
        from zoneinfo import ZoneInfo
        from datetime import datetime, timezone
        _ET = ZoneInfo("America/New_York")
        now_et = now_utc.astimezone(_ET)
        # weekday(): Monday=0 … Sunday=6
        return now_et.weekday() == 6 and 2 <= now_et.hour < 5

    def _fund_next_window_start(now_utc):
        """Return UTC datetime of next Sunday 02:00 ET."""
        from zoneinfo import ZoneInfo
        from datetime import datetime, timezone, timedelta
        _ET = ZoneInfo("America/New_York")
        now_et = now_utc.astimezone(_ET)
        days_until_sunday = (6 - now_et.weekday()) % 7
        if days_until_sunday == 0 and now_et.hour >= 5:
            # Already past today's window — next Sunday
            days_until_sunday = 7
        elif days_until_sunday == 0 and now_et.hour < 2:
            days_until_sunday = 0   # window is later today
        candidate = now_et.replace(hour=2, minute=0, second=0, microsecond=0)
        candidate = candidate + timedelta(days=days_until_sunday)
        return candidate.astimezone(timezone.utc)

    async def _watchlist_fundamentals_weekly_loop():
        import asyncio as _aio
        import os as _os
        from datetime import datetime, timezone
        from zoneinfo import ZoneInfo
        _ET = ZoneInfo("America/New_York")

        await _aio.sleep(300)   # 5-min startup delay — let other loops warm first

        while True:
            now_utc = datetime.now(timezone.utc)
            now_et  = now_utc.astimezone(_ET)

            if not _fund_in_window(now_utc):
                # Outside maintenance window — sleep until 10 min before next window,
                # then poll every 10 min until inside.
                _nxt = _fund_next_window_start(now_utc)
                _wait = max(60.0, (_nxt - now_utc).total_seconds() - 600)
                print(
                    f"[FUND_WEEKLY] outside window "
                    f"current_et={now_et.strftime('%a %Y-%m-%d %H:%M %Z')} "
                    f"next_window_start_et={_nxt.astimezone(_ET).strftime('%a %Y-%m-%d %H:%M %Z')} "
                    f"sleeping={_wait/3600:.2f}h"
                )
                await _aio.sleep(min(_wait, 600))   # wake at most every 10 min to check
                continue

            # ── Inside Sunday 02:00–05:00 ET maintenance window ──────────────
            if _fund_weekly_lock.locked():
                print("[FUND_WEEKLY] prior batch still running — waiting 60s")
                await _aio.sleep(60)
                continue

            async with _fund_weekly_lock:
                try:
                    from data.pg_storage import watchlist_list as _wl_list, is_available as _pg_ok
                    from data.watchlist_fundamentals_store import list_due_symbols as _due_syms
                    from services.watchlist_fundamentals_refresh import FmpFundamentalsRefresher

                    _fmp_key = _os.getenv("FMP_API_KEY", "")
                    if not _fmp_key or not _pg_ok():
                        await _aio.sleep(300)
                        continue

                    _max_per_run = int(_os.getenv("WATCHLIST_FUNDAMENTALS_MAX_PER_RUN", "50"))
                    _refresher   = FmpFundamentalsRefresher(_fmp_key)
                    _cycle_started = datetime.now(timezone.utc)
                    _et_now_str    = _cycle_started.astimezone(_ET).strftime("%H:%M %Z")

                    # Collect due symbols across all known watchlists (oldest-due first).
                    _all_due: dict[str, list[str]] = {}
                    for _wl in (_wl_list() or []):
                        _wl_id = _wl.get("id") or ""
                        if _wl_id:
                            _due = _due_syms(_wl_id)
                            if _due:
                                _all_due[_wl_id] = _due

                    _total_due = sum(len(v) for v in _all_due.values())

                    if _total_due == 0:
                        print(f"[FUND_WEEKLY] window open at {_et_now_str} — no due symbols, done")
                        # Sleep to end of window to avoid log spam
                        await _aio.sleep(3600)
                        continue

                    # Process one capped batch; remaining stay due for next iteration
                    # within this window, or next week's window.
                    _budget = _max_per_run
                    _run_refreshed = _run_failed = _run_skipped = 0

                    for _wl_id, _syms in _all_due.items():
                        if _budget <= 0:
                            break
                        _batch = _syms[:_budget]
                        _remaining_after = len(_syms) - len(_batch)
                        print(
                            f"[FUND_WEEKLY] window={_et_now_str} wl={_wl_id[:8]} "
                            f"due_total={len(_syms)} batch={len(_batch)} "
                            f"remaining_after={_remaining_after} cap={_max_per_run}"
                        )
                        _res = await _refresher.refresh_symbols(_batch, _wl_id, dev_force=False)
                        _run_refreshed += _res.get("refreshed_symbols", 0)
                        _run_failed    += _res.get("failed_symbols", 0)
                        _run_skipped   += _res.get("skipped_fresh_symbols", 0)
                        _budget -= len(_batch)

                    _cycle_finished = datetime.now(timezone.utc)
                    _duration = (_cycle_finished - _cycle_started).total_seconds()
                    _processed = _run_refreshed + _run_failed + _run_skipped
                    _remaining = max(0, _total_due - _processed)

                    print(
                        f"[FUND_WEEKLY] batch done — "
                        f"window_et={_et_now_str} "
                        f"due_symbols_total={_total_due} "
                        f"processed_this_run={_processed} "
                        f"remaining_due_after_run={_remaining} "
                        f"refreshed={_run_refreshed} "
                        f"failed={_run_failed} "
                        f"skipped_fresh={_run_skipped} "
                        f"started_at={_cycle_started.isoformat()[:19]}Z "
                        f"finished_at={_cycle_finished.isoformat()[:19]}Z "
                        f"duration_seconds={_duration:.1f}"
                    )

                except Exception as _fund_loop_e:
                    print(f"[FUND_WEEKLY] batch error (non-fatal): {_fund_loop_e}")

            # Still inside window after batch? Sleep 60s then check again
            # (will pick up remaining due symbols or exit when window closes).
            await _aio.sleep(60)

    asyncio.create_task(_watchlist_fundamentals_weekly_loop())

    # Watchlist rank snapshot cadence — advances RV + vol/MC rank baselines every
    # ~5 minutes using warm quote-cache data.  GET path is read-only (never writes
    # new rank snapshots); this loop is the sole writer of current/previous snapshots.
    async def _watchlist_rank_snapshot_loop():
        import asyncio as _aio
        import math   as _math
        import time   as _time

        await _aio.sleep(120)   # let quote cache warm before first snapshot

        while True:
            try:
                from services.watchlist_router import (
                    _rv_registry,    _rv_mem,    _rv_neon_save,
                    _volmc_registry, _volmc_mem, _volmc_neon_save,
                )
                from services.watchlist_quote_cache import _quote_cache as _qc

                _t0 = _time.monotonic()
                _rv_count = _vm_count = 0

                # ── RV rank snapshots ─────────────────────────────────────────
                for _wl_id, _tickers in list(_rv_registry.items()):
                    try:
                        _elig: list[tuple[str, float]] = []
                        for _sym in _tickers:
                            _rv_val = _qc.get(_sym, {}).get("relative_volume")
                            if (
                                isinstance(_rv_val, (int, float))
                                and _math.isfinite(_rv_val)
                                and _rv_val > 0
                            ):
                                _elig.append((_sym, float(_rv_val)))
                        _us_elig = sum(1 for _s in _tickers if ":" not in _s)
                        if not _us_elig:
                            continue
                        _cov = len(_elig) / _us_elig
                        if _cov < 0.5:
                            print(f"[RV_SNAP_BG] wl={_wl_id} coverage={_cov:.0%} <50% — skip")
                            continue
                        _elig.sort(key=lambda x: (-x[1], x[0]))
                        _rv_snap = {
                            _s: {"rank": _i + 1, "rel_vol": round(_rv, 6)}
                            for _i, (_s, _rv) in enumerate(_elig)
                        }
                        _rv_mem[_wl_id] = {
                            "previous": (_rv_mem.get(_wl_id) or {}).get("current"),
                            "current":  _rv_snap,
                        }
                        await _aio.get_event_loop().run_in_executor(
                            None,
                            lambda wid=_wl_id, snap=_rv_snap: _rv_neon_save(wid, snap),
                        )
                        _rv_count += 1
                        print(
                            f"[RV_SNAP_BG] wl={_wl_id} ranked={len(_rv_snap)} "
                            f"coverage={_cov:.0%}"
                        )
                    except Exception as _rv_e:
                        print(f"[RV_SNAP_BG] error wl={_wl_id}: {_rv_e}")

                # ── vol/MC rank snapshots ─────────────────────────────────────
                for _wl_id, _vm_data in list(_volmc_registry.items()):
                    try:
                        _tickers_vm  = _vm_data.get("tickers", [])
                        _pcts        = _vm_data.get("pcts", {})
                        _elig_vm     = [
                            (_s, _p) for _s, _p in _pcts.items()
                            if isinstance(_p, (int, float)) and _p > 0
                        ]
                        _us_elig_vm  = sum(1 for _s in _tickers_vm if ":" not in _s)
                        if not _us_elig_vm or len(_elig_vm) / _us_elig_vm < 0.5:
                            continue
                        _elig_vm.sort(key=lambda x: (-x[1], x[0]))
                        _vm_snap = {
                            _s: {"rank": _i + 1, "vol_mc_pct": round(_p, 6)}
                            for _i, (_s, _p) in enumerate(_elig_vm)
                        }
                        _volmc_mem[_wl_id] = {
                            "previous": (_volmc_mem.get(_wl_id) or {}).get("current"),
                            "current":  _vm_snap,
                        }
                        await _aio.get_event_loop().run_in_executor(
                            None,
                            lambda wid=_wl_id, snap=_vm_snap: _volmc_neon_save(wid, snap),
                        )
                        _vm_count += 1
                    except Exception as _vm_e:
                        print(f"[VOLMC_SNAP_BG] error wl={_wl_id}: {_vm_e}")

                _elapsed_ms = round((_time.monotonic() - _t0) * 1000)
                print(
                    f"[RANK_SNAP_BG] cycle complete: "
                    f"rv_watchlists={_rv_count} vm_watchlists={_vm_count} "
                    f"elapsed={_elapsed_ms}ms"
                )

            except Exception as _outer_e:
                print(f"[RANK_SNAP_BG] outer loop error: {_outer_e}")

            await _aio.sleep(300)   # ~5-minute cadence

    asyncio.create_task(_watchlist_rank_snapshot_loop())

    # Thematic context warmup: load LKG from disk immediately, then rebuild from caches.
    # Runs after a 5s delay so sector rotation loop has a head start.
    # No LLM calls, no API calls — pure cache/disk reads + static registry.
    try:
        from services.thematic_context_provider import warmup_thematic_context as _thematic_warmup
        asyncio.create_task(_thematic_warmup())
    except Exception as _e:
        print(f"[STARTUP] Thematic context warmup task error: {_e}")
    # Defiance 2X catalog: load disk LKG on startup, refresh daily off-hours.
    try:
        from services.defiance_leveraged_etfs_service import (
            load_catalog_lkg  as _d2x_load_lkg,
            refresh_catalog   as _d2x_refresh_catalog,
        )
        _d2x_load_lkg()   # synchronous — zero I/O if LKG exists

        async def _defiance_2x_daily_loop():
            await asyncio.sleep(120)   # 2-min startup delay
            while True:
                try:
                    await _d2x_refresh_catalog()
                except Exception as _e:
                    print(f"[DEFIANCE_2X] Daily refresh error: {_e}")
                await asyncio.sleep(20 * 3600)   # 20-hour cadence

        asyncio.create_task(_defiance_2x_daily_loop())
    except Exception as _e:
        print(f"[STARTUP] Defiance 2X catalog init error: {_e}")
    # Dynamic thematic universe: build and refresh every 15 min.
    # Provides ETF-holdings + FMP-peers + X-consensus tickers to TA Screener and Options Flow.
    asyncio.create_task(_dynamic_thematic_universe_loop())
    # Themes by Relative Strength warmup: load LKG → seed caches → start background loop.
    # Non-blocking. 1D refreshes every ~60s market hours, historical every ~15min.
    try:
        from services.theme_rs_service import warmup_theme_rs as _theme_rs_warmup
        asyncio.create_task(_theme_rs_warmup())
    except Exception as _e:
        print(f"[STARTUP] Theme RS warmup error: {_e}")
    # Watchlist Stage 2 — load disk LKG into memory then kick a gentle
    # background warmup that fetches bars for any stale/missing symbols.
    try:
        from services.watchlist_stage2_service import (
            load_lkg as _wl_stage2_load,
            warmup_stage2_all_watchlists as _wl_stage2_warmup,
        )
        _wl_stage2_load()   # synchronous — zero I/O if disk file exists
        asyncio.create_task(_wl_stage2_warmup(startup_delay_s=60.0))
    except Exception as _e:
        print(f"[STARTUP] Watchlist Stage2 warmup error: {_e}")
    try:
        from data.options_screener_snapshot import load_state as _load_opt_snapshot
        _load_opt_snapshot()
    except Exception as _e:
        print(f"[STARTUP] Options snapshot state load error: {_e}")
    asyncio.create_task(_earnings_calendar_warmup())
    # Curated earnings snapshot precompute loop.
    # Loads disk snapshots into memory first (so page loads are instant after restart),
    # then starts the background loop that builds/refreshes weekly curated snapshots.
    try:
        from services.earnings_clean_service import (
            _load_all_earn_snaps_from_disk as _load_earn_snaps,
            _earnings_curated_precompute_loop as _earn_precompute_loop,
        )
        _load_earn_snaps()   # synchronous — warm in-memory cache from disk before any request
        asyncio.create_task(_earn_precompute_loop())
    except Exception as _e:
        print(f"[STARTUP] Earnings curated precompute init error: {_e}")
    # Weekly calendar snapshots (Dividends, IPOs, Splits, Economic, Treasury).
    # Reads only from disk on request; refreshes Sunday in ET per per-tab hour.
    # Also runs a startup staleness check (45s delay) so restarts mid-week
    # immediately refresh stale snapshots without waiting for Sunday.
    try:
        from services.calendar_snapshot_service import (
            weekly_scheduler_loop as _calendar_snap_loop,
            check_and_refresh_stale as _cal_stale_check,
        )
        from config import FMP_API_KEY as _fmp_key_for_snap
        asyncio.create_task(_calendar_snap_loop(lambda: _fmp_key_for_snap))
        asyncio.create_task(_cal_stale_check(_fmp_key_for_snap or ""))
    except Exception as _e:
        print(f"[STARTUP] calendar snapshot scheduler init error: {_e}")
    # ── Screener Hub scheduler ──────────────────────────────────────────────
    # Eastern-time recurring jobs (>= 30 min spacing):
    #   Sun 00:30 ET  thematic universe rebuild
    #   Sun 01:15 ET  thematic fundamentals warm
    #   Sun 03:15 ET  bottlenecks fundamentals warm
    #   Sun-Fri 11:10 ET  social universe rebuild (10 min after Grok fires at 10:00 AM CT)
    #   Sun-Fri 11:45 ET  social fundamentals warm
    #   Fri 02:00 ET  watchlist+portfolio fundamentals warm
    try:
        from services.screener_hub_scheduler import scheduler_loop as _screener_hub_loop
        asyncio.create_task(_screener_hub_loop())
        print("[SCREENER_HUB] Scheduler task registered")
    except Exception as _e:
        print(f"[STARTUP] Screener Hub scheduler init error: {_e}")
    # ── Home Top Catalysts Saturday planning warmup ─────────────────────────
    # Proactively fetches next-week macro events on Saturday ET so the first
    # /api/home/top-catalysts request is served from the in-process cache
    # rather than triggering an on-demand FMP fetch inline.
    asyncio.create_task(_home_planning_warmup_loop())
    # Tracked Odds Registry: fetch → match → persist → delta → cache, every 30 min.
    # Runs at 90 s so it beats the intelligence loop (120 s) on first cycle.
    asyncio.create_task(_odds_scanner_loop())
    # Predict page investor intelligence: pre-warm event-family payload every 30 min.
    # Covers tracked macro odds, equity signals, watchlist-first ticker resolution.
    asyncio.create_task(_investor_intelligence_loop())

    # Pre-warm the Caelyn Terminal cache in the background so the portfolio
    # dashboard is ready before the first user request arrives.
    asyncio.create_task(_terminal_prewarm())

    # Watchlist RSS sweeper — continuous ~2-min full-universe RSS archive sweep.
    # Creates the watchlist_rss_article_archive Neon table, then registers one
    # background loop that fetches Yahoo + Google RSS in parallel for every
    # active Watchlist ticker and upserts 72-hour rolling article associations.
    # FMP is never called by this sweeper. Registered exactly once.
    try:
        from data.rss_article_archive import ensure_table as _rss_ensure_table
        _rss_ensure_table()
    except Exception as _rss_tbl_err:
        print(f"[STARTUP] RSS archive table init error (non-fatal): {_rss_tbl_err}")
    try:
        from services.watchlist_rss_sweeper import rss_sweeper_loop as _rss_sweeper_loop
        asyncio.create_task(_rss_sweeper_loop())
        print("[STARTUP] Watchlist RSS sweeper loop registered")
    except Exception as _rss_err:
        print(f"[STARTUP] RSS sweeper loop registration error: {_rss_err}")

    yield

app = FastAPI(title="Trading Agent API", lifespan=lifespan)

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# ── Hyperliquid Screener router ───────────────────────────────────────────────
app.include_router(_hl_router)
# ─────────────────────────────────────────────────────────────────────────────

# ── Sector Rotation router ────────────────────────────────────────────────────
app.include_router(_sr_router)
app.include_router(_sectors_router)
# ─────────────────────────────────────────────────────────────────────────────

# ── Insider Activity router ───────────────────────────────────────────────────
app.include_router(_insider_router, prefix="/api")
# ─────────────────────────────────────────────────────────────────────────────

# ── Congressional Trading router ──────────────────────────────────────────────
app.include_router(_cong_router, prefix="/api")
# ─────────────────────────────────────────────────────────────────────────────

# ── Whale Watch router ────────────────────────────────────────────────────────
app.include_router(_whale_router, prefix="/api")
# ─────────────────────────────────────────────────────────────────────────────

# ── Predict / Polymarket Intelligence router ──────────────────────────────────
app.include_router(_predict_router)
# ── Investor Mode router (additive — Gambler endpoints unchanged) ─────────────
app.include_router(_investor_router)
# ─────────────────────────────────────────────────────────────────────────────

# ── Bittensor Dashboard router ───────────────────────────────────────────────
app.include_router(_bittensor_router)
# ─────────────────────────────────────────────────────────────────────────────

# ── Watchlist router ─────────────────────────────────────────────────────────
app.include_router(_watchlist_router)
# ─────────────────────────────────────────────────────────────────────────────

# ── Chart Radar router (/api/chart-radar/*) ───────────────────────────────────
try:
    from services.chart_radar_router import router as _chart_radar_router
    app.include_router(_chart_radar_router)
    print("[CHART_RADAR] Router registered at /api/chart-radar/*")
except Exception as _cr_err:
    print(f"[CHART_RADAR] Router unavailable (non-fatal): {_cr_err}")
# ─────────────────────────────────────────────────────────────────────────────

# ── Playbook engine router (isolated, additive — does NOT modify /api/query) ──
try:
    from services.playbook.router import router as _playbook_router
    app.include_router(_playbook_router)
    print("[PLAYBOOK] Router registered at /api/playbooks")
except Exception as _pb_err:
    print(f"[PLAYBOOK] Router unavailable (non-fatal): {_pb_err}")

# ── Strategy Screener router (isolated, no /api/query coupling) ────────────
try:
    from services.playbook.strategy_screener.screener_router import router as _screener_router
    app.include_router(_screener_router)
    print("[SCREENER] Router registered at /api/strategy-screener")
except Exception as _sc_err:
    print(f"[SCREENER] Router unavailable (non-fatal): {_sc_err}")
# ─────────────────────────────────────────────────────────────────────────────

# ── Stock Compare router (/api/fundamentals/compare) ─────────────────────────
try:
    from routes.stock_compare import router as _stock_compare_router
    app.include_router(_stock_compare_router)
    print("[STOCK_COMPARE] Router registered at /api/fundamentals/compare")
except Exception as _cmp_err:
    print(f"[STOCK_COMPARE] Router unavailable (non-fatal): {_cmp_err}")
# ─────────────────────────────────────────────────────────────────────────────

# ── Catalyst Calendar router (/api/catalysts/*) ───────────────────────────────
try:
    from routes.catalyst_calendar import router as _catalyst_router
    app.include_router(_catalyst_router)
    print("[CATALYST_CALENDAR] Router registered at /api/catalysts/*")
except Exception as _cat_err:
    print(f"[CATALYST_CALENDAR] Router unavailable (non-fatal): {_cat_err}")

# ── Calendar-snapshot diagnostics fallback (/api/debug/calendar-snapshots) ────
# Mounted directly on `app` so the diagnostics endpoint is reachable even if
# the catalyst_calendar router fails to import on a given deploy. The route
# remains 404 unless CALENDAR_DIAGNOSTICS_TOKEN/DIAGNOSTICS_TOKEN is set.
try:
    from routes.catalyst_calendar import (
        debug_calendar_snapshots as _diag_calendar_snapshots,
    )
    if not any(
        getattr(r, "path", None) == "/api/debug/calendar-snapshots"
        for r in app.routes
    ):
        app.add_api_route(
            "/api/debug/calendar-snapshots",
            _diag_calendar_snapshots,
            methods=["GET"],
        )
        print("[CALENDAR_DIAG] Fallback route registered at /api/debug/calendar-snapshots")
except Exception as _diag_err:
    print(f"[CALENDAR_DIAG] Fallback handler unavailable, registering inline: {_diag_err}")

    @app.get("/api/debug/calendar-snapshots")
    async def _calendar_snapshots_inline_diag(
        request: Request,
        x_diagnostics_token: Optional[str] = Header(None, alias="X-Diagnostics-Token"),
    ):
        expected = (
            os.getenv("CALENDAR_DIAGNOSTICS_TOKEN")
            or os.getenv("DIAGNOSTICS_TOKEN")
            or None
        )
        if not expected:
            return JSONResponse(status_code=404, content={"error": "Not Found"})
        if not x_diagnostics_token or x_diagnostics_token != expected:
            return JSONResponse(status_code=403, content={"error": "Forbidden"})
        return JSONResponse(content={
            "status": "degraded",
            "reason": "catalyst_calendar router failed to import",
            "import_error": str(_diag_err),
        })

# ── Clean Earnings Upcoming router (/api/catalysts/earnings/*) ───────────────
# v2 safe architecture: sequential chunks, max 2 FMP calls, circuit breaker.
try:
    from routes.earnings_upcoming import router as _earnings_clean_router
    app.include_router(_earnings_clean_router)
    print("[EARNINGS_CLEAN] Router registered at /api/catalysts/earnings/*")
except Exception as _earn_err:
    print(f"[EARNINGS_CLEAN] Router unavailable (non-fatal): {_earn_err}")

# ── Pre-IPO Watchlist router (/api/calendar/pre-ipo-watchlist) ───────────────
# Additive: Perplexity + Polymarket + Finnhub aggregation for high-profile
# private companies.  Does NOT touch existing FMP IPO calendar behavior.
try:
    from routes.pre_ipo_watchlist import router as _pre_ipo_router
    app.include_router(_pre_ipo_router)
    print("[PRE_IPO_WATCHLIST] Router registered at /api/calendar/pre-ipo-watchlist")
except Exception as _pre_ipo_err:
    print(f"[PRE_IPO_WATCHLIST] Router unavailable (non-fatal): {_pre_ipo_err}")
# ─────────────────────────────────────────────────────────────────────────────

# ── Realtime Quotes router (/api/market/realtime-quotes) ─────────────────────
# Centralized real-time equity quotes with vendor priority:
# Tradier -> Public.com -> FMP -> Twelve Data -> LKG.  Additive only —
# does not change FMP usage for fundamentals/historical/enrichment/news.
try:
    from routes.realtime_quotes import router as _realtime_quotes_router
    app.include_router(_realtime_quotes_router)
    print("[REALTIME_QUOTES] Router registered at /api/market/realtime-quotes")
except Exception as _rtq_err:
    print(f"[REALTIME_QUOTES] Router unavailable (non-fatal): {_rtq_err}")
# ─────────────────────────────────────────────────────────────────────────────

# ── Themes by Relative Strength router (/api/themes) ─────────────────────────
# 39-theme canonical RS service: Tradier quotes + yfinance history.
# No LLM calls. Cached 15 min (market hours) / 60 min (off-hours).
# LKG persisted to disk (backend/data/themes_rs_lkg.json).
try:
    from routes.themes import router as _themes_router
    app.include_router(_themes_router)
    print("[THEMES_RS] Router registered at /api/themes")
except Exception as _themes_err:
    print(f"[THEMES_RS] Router unavailable (non-fatal): {_themes_err}")

# ── Options Flow Sectors router (/api/options-flow/sectors) ──────────────────
# Net options flow aggregated by Sector → Theme → Ticker.
# Zero new Tradier calls — reads from existing master screener cache.
try:
    from routes.options_flow_sectors import router as _opts_sectors_router
    app.include_router(_opts_sectors_router)
    print("[OPTIONS_SECTORS] Router registered at /api/options-flow/sectors")
except Exception as _opts_sectors_err:
    print(f"[OPTIONS_SECTORS] Router unavailable (non-fatal): {_opts_sectors_err}")

# ── Portfolio categorize-themes router (/api/portfolio/categorize-themes) ─────
# LLM-powered classifier: assigns unclassified portfolio tickers to named
# investment themes, persists to data/llm_theme_overrides.json, and invalidates
# the terminal cache so the next load picks up the new theme assignments.
try:
    from routes.portfolio_categorize import router as _portfolio_cat_router
    app.include_router(_portfolio_cat_router)
    print("[PORTFOLIO_CAT] Router registered at /api/portfolio/categorize-themes")
except Exception as _pcat_err:
    print(f"[PORTFOLIO_CAT] Router unavailable (non-fatal): {_pcat_err}")
# ─────────────────────────────────────────────────────────────────────────────

# ── Screener Hub router (/api/screener-hub, /api/admin/screener-hub/*) ───────
# Page-aware screener that aggregates Thematic / Social / Bottlenecks /
# Watchlist+Portfolio universes. FMP fundamentals warmed weekly; Tradier
# quotes refreshed only for the active page's symbols.
try:
    from routes.screener_hub import router as _screener_hub_router
    app.include_router(_screener_hub_router)
    print("[SCREENER_HUB] Router registered at /api/screener-hub & /api/admin/screener-hub/*")
except Exception as _sh_err:
    print(f"[SCREENER_HUB] Router unavailable (non-fatal): {_sh_err}")
# ─────────────────────────────────────────────────────────────────────────────

# ── Static file serving ───────────────────────────────────────────────────────
_STATIC_DIR = Path(__file__).parent / "static"
if _STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    body = None
    try:
        body = (await request.body()).decode("utf-8", errors="replace")[:2000]
    except Exception:
        body = "<unreadable>"
    print(f"[VALIDATION_ERROR] path={request.url.path} method={request.method}")
    print(f"[VALIDATION_ERROR] errors={exc.errors()}")
    print(f"[VALIDATION_ERROR] body={body}")
    return JSONResponse(
        status_code=422,
        content={
            "detail": exc.errors(),
            "message": "Request validation failed — check field names and types.",
            "request_id": str(_uuid.uuid4()),
            "as_of": _dt.now(_tz.utc).isoformat(),
        },
    )


@app.exception_handler(_json.JSONDecodeError)
async def json_decode_exception_handler(request: Request, exc: _json.JSONDecodeError):
    body = None
    try:
        body = (await request.body()).decode("utf-8", errors="replace")[:2000]
    except Exception:
        body = "<unreadable>"
    print(f"[JSON_DECODE_ERROR] path={request.url.path} method={request.method}")
    print(f"[JSON_DECODE_ERROR] error={exc}")
    print(f"[JSON_DECODE_ERROR] raw_body={body}")
    return JSONResponse(
        status_code=400,
        content={
            "detail": f"Malformed JSON: {str(exc)}",
            "message": "Could not parse request body as JSON.",
            "request_id": str(_uuid.uuid4()),
            "as_of": _dt.now(_tz.utc).isoformat(),
        },
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    """Catch-all for any unhandled exception.

    Logs the full traceback so it appears in production logs even when buried
    in LangSmith noise, and returns the error detail as JSON so the frontend
    can surface the real message instead of 'Internal Server Error'.
    """
    import traceback as _tb
    tb_str = _tb.format_exc()
    req_id = str(_uuid.uuid4())
    print(
        f"[UNHANDLED_500] req_id={req_id}  path={request.url.path}  "
        f"method={request.method}  exc={exc!r}\n{tb_str}",
        flush=True,
    )
    return JSONResponse(
        status_code=500,
        content={
            "detail":     str(exc),
            "traceback":  tb_str,
            "request_id": req_id,
            "path":       request.url.path,
        },
    )


# CORSMiddleware must be outermost — handles OPTIONS preflights and adds
# CORS headers to ALL responses (including 401s from JWT middleware).
# JWTAuthMiddleware is pure ASGI now, so add_middleware ordering works correctly.
app.add_middleware(JWTAuthMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Auth Endpoints ───────────────────────────────────────────────

class LoginRequest(BaseModel):
    username: str
    password: str
    remember_me: bool = False


@app.post("/api/auth/login")
async def auth_login(body: LoginRequest):
    """Authenticate user and return JWT token."""
    from auth import validate_credentials, create_token, AUTH_USERNAME
    if not validate_credentials(body.username, body.password):
        raise HTTPException(status_code=401, detail="Invalid username or password.")
    user_id = body.username
    token = create_token(user_id, remember_me=body.remember_me)

    # Migrate legacy data on first login
    try:
        legacy_portfolio = Path("data/portfolio_holdings.json")
        user_portfolio = Path(f"data/portfolio_holdings_{user_id}.json")
        if legacy_portfolio.exists() and not user_portfolio.exists():
            import shutil
            shutil.copy2(legacy_portfolio, user_portfolio)
            print(f"[AUTH] Migrated portfolio_holdings.json -> {user_portfolio}")
    except Exception as e:
        print(f"[AUTH] Portfolio migration error: {e}")

    try:
        from data.prompt_history import migrate_legacy_history
        migrate_legacy_history(user_id)
    except Exception as e:
        print(f"[AUTH] Prompt history migration error: {e}")

    try:
        from data.chat_history import migrate_file_history_to_db
        migrate_file_history_to_db()
    except Exception as e:
        print(f"[AUTH] Chat history migration error: {e}")

    return {"token": token, "user_id": user_id}


@app.get("/api/auth/verify")
async def auth_verify(request: Request):
    """Verify the current JWT token and return user info.
    This endpoint is public so we must extract the token manually."""
    from auth import verify_token
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated.")
    token = auth_header[7:]
    try:
        payload = verify_token(token)
        user_id = payload.get("sub", "default")
    except Exception:
        raise HTTPException(status_code=401, detail="Token expired or invalid.")
    return {"valid": True, "user_id": user_id}


@app.post("/api/auth/logout")
async def auth_logout(request: Request):
    """Logout — client should delete the token. Server-side is stateless."""
    return {"success": True, "message": "Logged out. Delete the token client-side."}


@app.post("/api/auth/request-reset")
async def auth_request_reset(request: Request):
    """Generate a password-reset token and email it to the owner address."""
    from password_reset import generate_reset_token, send_reset_email, get_app_base_url
    token = generate_reset_token()
    base  = get_app_base_url(request)
    reset_url = f"{base}/reset-password?token={token}"
    sent = send_reset_email(reset_url)
    if not sent:
        return JSONResponse(
            status_code=500,
            content={"error": "Failed to send reset email. Check RESEND_API_KEY and RESET_EMAIL_TO secrets."},
        )
    return {"success": True, "message": "Reset link sent to your registered email address."}


@app.get("/api/auth/verify-reset-token")
async def auth_verify_reset_token(token: str = ""):
    """Check if a reset token is still valid (without consuming it)."""
    from password_reset import _reset_tokens
    import time as _time2
    if not token:
        return {"valid": False}
    exp = _reset_tokens.get(token)
    valid = exp is not None and _time2.time() <= exp
    return {"valid": valid}


@app.post("/api/auth/reset-password")
async def auth_reset_password(request: Request):
    """Validate reset token and update AUTH_PASSWORD_HASH."""
    from password_reset import validate_and_consume_token, hash_password
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass
    token    = (body.get("token") or "").strip()
    new_pass = (body.get("new_password") or "").strip()

    if not token or not new_pass:
        raise HTTPException(status_code=400, detail="token and new_password are required.")
    if len(new_pass) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters.")
    if not validate_and_consume_token(token):
        raise HTTPException(status_code=400, detail="Reset token is invalid or has expired.")

    new_hash = hash_password(new_pass)

    # Update in-process so the new password works immediately (until next cold restart)
    os.environ["AUTH_PASSWORD_HASH"] = new_hash
    try:
        import auth as _auth_mod
        _auth_mod.AUTH_PASSWORD_HASH = new_hash
    except Exception:
        pass

    print(f"[RESET] Password updated. Paste this hash into AUTH_PASSWORD_HASH secret to persist:")
    print(f"[RESET] {new_hash}")

    return {
        "success": True,
        "message": "Password updated. You can now log in with your new password.",
        "new_hash": new_hash,
        "_note": "Paste this hash into your AUTH_PASSWORD_HASH secret to persist across server restarts.",
    }


@app.get("/reset-password")
async def reset_password_page():
    """Serve the password-reset form."""
    html_path = Path(__file__).parent / "static" / "reset-password" / "index.html"
    if html_path.exists():
        return FileResponse(str(html_path), media_type="text/html")
    return HTMLResponse("<h1>Reset page not found</h1>", status_code=404)


data_service = None
agent = None
_init_done = False
_init_error = None  # stores init failure message for fast 503 responses
import threading as _threading
_init_event = _threading.Event()

def _do_init():
    global data_service, agent, _init_done, _init_error
    try:
        from config import ANTHROPIC_API_KEY, POLYGON_API_KEY, FMP_API_KEY, COINGECKO_API_KEY, CMC_API_KEY, ALTFINS_API_KEY, XAI_API_KEY, TWELVEDATA_API_KEY
        from data.market_data_service import MarketDataService
        from agent.claude_agent import TradingAgent
        data_service = MarketDataService(polygon_key=POLYGON_API_KEY, fmp_key=FMP_API_KEY, coingecko_key=COINGECKO_API_KEY, cmc_key=CMC_API_KEY, altfins_key=ALTFINS_API_KEY, xai_key=XAI_API_KEY, twelvedata_key=TWELVEDATA_API_KEY)
        agent = TradingAgent(api_key=ANTHROPIC_API_KEY, data_service=data_service)
        _init_done = True
        _init_event.set()
        print("[INIT] All services initialized successfully")
    except Exception as e:
        _init_error = str(e)
        print(f"[INIT] FATAL ERROR during initialization: {e}")
        import traceback
        traceback.print_exc()
        # Set event so _wait_for_init returns immediately with 503
        # instead of blocking every request for 60 seconds
        _init_event.set()
async def _briefing_precompute_loop():
    """
    Background precomputation for Daily Briefing.
    Runs every 30 minutes using free/unlimited APIs + one Perplexity web search
    for market news context. Caches Phase 1 data (screeners, macro, trending,
    news) so briefing requests are near-instant.
    """
    # Wait for init to complete
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _init_event.wait, 120)

    if data_service is None:
        print("[BRIEFING_PRECOMPUTE] data_service not available, aborting background loop")
        return

    from data.cache import cache, BRIEFING_PRECOMPUTE_TTL

    while True:
        try:
            print("[BRIEFING_PRECOMPUTE] Starting background scan...")

            # Phase 1: All free API screener + macro calls (same as get_morning_briefing Phase 1)
            from data.scoring_engine import score_for_trades, score_for_investments

            briefing_tasks = [
                data_service.fear_greed.get_fear_greed_index(),
                asyncio.to_thread(data_service.fred.get_quick_macro),
                data_service.finviz.get_stage2_breakouts(),
                data_service.finviz.get_volume_breakouts(),
                data_service.finviz.get_macd_crossovers(),
                data_service.finviz.get_unusual_volume(),
                data_service.finviz.get_new_highs(),
                data_service.finviz.get_high_short_float(),
                data_service.finviz.get_insider_buying(),
                data_service.finviz.get_revenue_growth_leaders(),
                data_service.finviz.get_rsi_recovery(),
                data_service.finviz.get_accumulation_stocks(),
                data_service.stocktwits.get_trending(),
                asyncio.to_thread(data_service.finnhub.get_upcoming_earnings),
            ]
            # News: FMP is primary source. Perplexity only if PERPLEXITY_BACKGROUND_ENABLED=true.
            from data.perplexity_guards import pplx_background_allowed as _pplx_bg, pplx_blocked as _pplx_blk, pplx_allowed as _pplx_ok
            if _pplx_bg() and data_service.web_search:
                briefing_tasks.append(
                    asyncio.wait_for(
                        data_service.web_search.get_market_news(topic="stock market financial news today"),
                        timeout=10.0))
                _pplx_ok("background", "_briefing_precompute_loop:market_news")
                print("[BRIEFING_PRECOMPUTE] news source=perplexity_enabled")
            elif data_service.fmp:
                briefing_tasks.append(
                    asyncio.wait_for(data_service.fmp.get_market_news(limit=15), timeout=8.0))
                print("[BRIEFING_PRECOMPUTE] news source=fmp")
            else:
                briefing_tasks.append(asyncio.sleep(0))
                print("[BRIEFING_PRECOMPUTE] news source=none")

            results = await asyncio.gather(*briefing_tasks, return_exceptions=True)

            def safe(val, default=None):
                if default is None:
                    default = []
                return val if not isinstance(val, Exception) else default

            (fear_greed, fred_macro, stage2_breakouts, volume_breakouts,
             macd_crossovers, unusual_volume, new_highs, high_short,
             insider_buying, revenue_leaders, rsi_recovery, accumulation,
             trending, upcoming_earnings, market_news_raw) = results

            market_news_val = safe(market_news_raw)
            # Normalize: web_search returns dict with 'articles', FMP returns list
            if isinstance(market_news_val, dict):
                market_news = market_news_val.get("articles", [])
            else:
                market_news = market_news_val if isinstance(market_news_val, list) else []
            fear_greed = safe(fear_greed, {})
            fred_macro = safe(fred_macro, {})
            stage2_breakouts = safe(stage2_breakouts)
            volume_breakouts = safe(volume_breakouts)
            macd_crossovers = safe(macd_crossovers)
            unusual_volume = safe(unusual_volume)
            new_highs = safe(new_highs)
            high_short = safe(high_short)
            insider_buying = safe(insider_buying)
            revenue_leaders = safe(revenue_leaders)
            rsi_recovery = safe(rsi_recovery)
            accumulation = safe(accumulation)
            trending = safe(trending)
            upcoming_earnings = safe(upcoming_earnings)

            # FMP macro data + Yahoo Finance DXY
            fmp_data = {}
            try:
                _fmp_tasks = []
                _fmp_names = []
                if data_service.fmp:
                    _fmp_tasks += [
                        data_service.fmp.get_key_commodities(),
                        data_service.fmp.get_treasury_rates(),
                        data_service.fmp.get_sector_performance(),
                        data_service.fmp.get_market_indices(),
                    ]
                    _fmp_names += ["commodities", "treasury_yields", "sector_performance", "indices"]
                if hasattr(data_service, "yahoo") and data_service.yahoo:
                    _fmp_tasks.append(data_service.yahoo.get_dxy())
                    _fmp_names.append("dxy")

                if _fmp_tasks:
                    _fmp_results = await asyncio.gather(*_fmp_tasks, return_exceptions=True)
                    _fmp_map = {
                        n: r if not isinstance(r, Exception) else {}
                        for n, r in zip(_fmp_names, _fmp_results)
                    }
                    fmp_data = {
                        "dxy": _fmp_map.get("dxy", {}),
                        "commodities": _fmp_map.get("commodities", {}),
                        "treasury_yields": _fmp_map.get("treasury_yields", {}),
                        "sector_performance": _fmp_map.get("sector_performance", []),
                        "indices": _fmp_map.get("indices", {}),
                    }
            except Exception:
                pass

            # Macro snapshot
            try:
                macro_snapshot = await asyncio.wait_for(
                    data_service._build_macro_snapshot(), timeout=10.0)
            except Exception:
                macro_snapshot = {}

            # Compute priority tickers + screener signals
            all_tickers = set()
            screener_sources = {}
            raw_screener_data = {}

            source_map = {
                "stage2_breakout": stage2_breakouts,
                "volume_breakout": volume_breakouts,
                "macd_crossover": macd_crossovers,
                "unusual_volume": unusual_volume,
                "new_high": new_highs,
                "high_short_float": high_short,
                "insider_buying": insider_buying,
                "revenue_growth": revenue_leaders,
                "rsi_recovery": rsi_recovery,
                "accumulation": accumulation,
            }

            for source_name, source_list in source_map.items():
                if isinstance(source_list, list):
                    for item in source_list:
                        if isinstance(item, dict) and item.get("ticker"):
                            t = item["ticker"].upper().strip()
                            if len(t) <= 5 and t.isalpha():
                                all_tickers.add(t)
                                if t not in screener_sources:
                                    screener_sources[t] = []
                                screener_sources[t].append(source_name)
                                if t not in raw_screener_data:
                                    raw_screener_data[t] = item
                                else:
                                    for k, v in item.items():
                                        if k != "ticker" and v and not raw_screener_data[t].get(k):
                                            raw_screener_data[t][k] = v

            for t in (trending or []):
                if isinstance(t, dict) and t.get("ticker"):
                    ticker = t["ticker"].upper().strip()
                    all_tickers.add(ticker)
                    if ticker not in screener_sources:
                        screener_sources[ticker] = []
                    screener_sources[ticker].append("social_trending")

            # ── Thematic prefilter source (TA Screener priority universe) ──────
            # Active/emerging theme tickers are registered as a "thematic_priority"
            # source so they count toward multi-signal even if Finviz misses them.
            # Does NOT call any API — uses LKG / static registry.
            try:
                from services.thematic_context_provider import get_thematic_prefilter_universe as _tc_pf
                _tc_uni = _tc_pf(max_tickers=60)
                _tc_active   = _tc_uni.get("active_theme_tickers", [])[:20]
                _tc_emerging = _tc_uni.get("emerging_theme_tickers", [])[:15]
                for _sym in dict.fromkeys([*_tc_active, *_tc_emerging]):
                    if not _sym or len(_sym) > 5 or not _sym.replace("-", "").isalpha():
                        continue
                    all_tickers.add(_sym)
                    screener_sources.setdefault(_sym, []).append("thematic_priority")
                    if _sym not in raw_screener_data:
                        raw_screener_data[_sym] = {
                            "ticker": _sym,
                            "source": "thematic_priority",
                            "thematic_state": "active" if _sym in _tc_active else "emerging",
                        }
            except Exception as _tce:
                print(f"[BRIEFING_PRECOMPUTE] Thematic source skipped: {_tce}")

            multi_signal = {t: sources for t, sources in screener_sources.items() if len(sources) >= 2}
            priority_tickers = list(multi_signal.keys())[:15]
            remaining_slots = 20 - len(priority_tickers)
            if remaining_slots > 0:
                single_signal = {t: sources for t, sources in screener_sources.items() if len(sources) == 1}
                filler = [t for t in single_signal.keys() if t not in priority_tickers][:remaining_slots]
                priority_tickers.extend(filler)

            # web_news: reuse Phase-1 market_news already fetched above (no second API call).
            # If PERPLEXITY_BACKGROUND_ENABLED=true the Phase-1 call already used Perplexity;
            # in all other cases market_news came from FMP so we reuse it here too.
            web_news_source = "fmp_reuse"
            if _pplx_bg() and data_service.web_search:
                from api_budget import daily_budget
                if daily_budget.can_spend("web_search", 1):
                    try:
                        _wn = await asyncio.wait_for(
                            data_service.web_search.get_market_news(
                                topic="stock market today breaking news"
                            ),
                            timeout=12.0,
                        )
                        daily_budget.spend("web_search", 1)
                        web_news = _wn
                        web_news_source = "perplexity_enabled"
                        _pplx_ok("background", "_briefing_precompute_loop:web_news")
                        print(f"[BRIEFING_PRECOMPUTE] web_news source=perplexity_enabled articles={web_news.get('article_count', 0)}")
                    except Exception as e:
                        print(f"[BRIEFING_PRECOMPUTE] Perplexity web_news failed: {e}")
                        web_news = {"articles": market_news, "article_count": len(market_news), "source": "fmp_reuse"}
                        web_news_source = "fmp_reuse"
                else:
                    _pplx_blk("background", "_briefing_precompute_loop:web_news_budget_stop")
                    web_news = {"articles": market_news, "article_count": len(market_news), "source": "fmp_reuse"}
            else:
                _pplx_blk("background", "_briefing_precompute_loop:web_news_disabled")
                web_news = {"articles": market_news, "article_count": len(market_news), "source": "fmp_reuse"}
                print(f"[BRIEFING_PRECOMPUTE] web_news source={web_news_source} (reusing Phase-1 FMP news)")

            precomputed = {
                "macro_snapshot": macro_snapshot,
                "news_context": {"market_news": market_news, "web_news": web_news},
                "total_tickers_detected": len(all_tickers),
                "multi_signal_tickers": {t: sources for t, sources in list(multi_signal.items())[:10]},
                "priority_tickers": priority_tickers,
                "screener_sources": screener_sources,
                "raw_screener_data": raw_screener_data,
                "fear_greed": fear_greed,
                "fred_macro": fred_macro,
                "fmp_market_data": fmp_data,
                "highlights": {
                    "stage2_breakouts": stage2_breakouts[:3] if isinstance(stage2_breakouts, list) else [],
                    "volume_breakouts": volume_breakouts[:3] if isinstance(volume_breakouts, list) else [],
                    "macd_crossovers": macd_crossovers[:3] if isinstance(macd_crossovers, list) else [],
                    "high_short_float": high_short[:3] if isinstance(high_short, list) else [],
                    "insider_buying": insider_buying[:3] if isinstance(insider_buying, list) else [],
                    "revenue_growth": revenue_leaders[:3] if isinstance(revenue_leaders, list) else [],
                    "rsi_recovery": rsi_recovery[:3] if isinstance(rsi_recovery, list) else [],
                    "social_trending": [t.get("ticker") for t in trending[:5]] if isinstance(trending, list) else [],
                },
                "upcoming_earnings": upcoming_earnings[:5] if isinstance(upcoming_earnings, list) else [],
                "precomputed_at": _dt.now(_tz.utc).isoformat(),
            }

            cache.set("briefing_precomputed_v1", precomputed, BRIEFING_PRECOMPUTE_TTL)
            print(f"[BRIEFING_PRECOMPUTE] Cached {len(all_tickers)} tickers, {len(priority_tickers)} priority. Next run in 30m.")

        except Exception as e:
            print(f"[BRIEFING_PRECOMPUTE] Error: {e}")
            import traceback
            traceback.print_exc()

        await asyncio.sleep(1800)  # 30 minutes


# Smart earnings scanner fires ONLY on explicit user request ("Ask Caelyn").
# No background loop — zero automatic Grok/Perplexity calls at startup or on schedule.
_smart_scan_running = False



# ============================================================
# EDGAR Background Cache Loop
# ============================================================
async def _edgar_cache_loop():
    """
    Background EDGAR data caching with two schedules:
      - Full refresh: nightly at midnight CST (financials + filings + catalysts + insider)
      - Filings refresh: every 2 hours during market hours (filings + catalysts only)

    Insider data (Form 4) and earnings-day tickers stay live with short TTL (5 min)
    for real-time trade signals.
    """
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _init_event.wait, 120)

    if data_service is None:
        print("[EDGAR_CACHE] data_service not available, aborting background loop")
        return

    from data.edgar_cache import refresh_universe, is_midnight_cst, is_market_hours

    # Initial full refresh on startup (populate cache if empty)
    await asyncio.sleep(30)  # Let other init tasks finish first
    try:
        print("[EDGAR_CACHE] Running initial full refresh on startup...")
        await refresh_universe(data_service.sec_edgar, mode="full")
    except Exception as e:
        print(f"[EDGAR_CACHE] Initial refresh error: {e}")

    last_full_refresh = time.time()
    last_filings_refresh = time.time()

    while True:
        try:
            now = time.time()

            # Nightly full refresh at midnight CST
            if is_midnight_cst() and (now - last_full_refresh > 3600):
                print("[EDGAR_CACHE] Midnight CST — running full refresh")
                await refresh_universe(data_service.sec_edgar, mode="full")
                last_full_refresh = now

            # Intraday filings refresh every 2 hours during market hours
            elif is_market_hours() and (now - last_filings_refresh > 7200):
                print("[EDGAR_CACHE] Market hours — refreshing filings + catalysts")
                await refresh_universe(data_service.sec_edgar, mode="filings")
                last_filings_refresh = now

            # Check every 5 minutes
            await asyncio.sleep(300)

        except Exception as e:
            print(f"[EDGAR_CACHE] Loop error: {e}")
            import traceback
            traceback.print_exc()
            await asyncio.sleep(600)


# ============================================================
# API Routes
# ============================================================


async def _wait_for_init():
    import asyncio
    _init_postgres_chat_storage_on_startup("wait_for_init")
    if _init_done:
        return
    if _init_error:
        raise HTTPException(status_code=503, detail=f"Server init failed: {_init_error}")
    # Use run_in_executor to properly wait on the threading.Event
    # This avoids thread-visibility issues with plain boolean polling
    loop = asyncio.get_event_loop()
    ready = await loop.run_in_executor(None, _init_event.wait, 60)
    if not _init_done:
        err = _init_error or "Init timed out after 60s"
        print("[INIT] _wait_for_init failed — _init_done=%s, error=%s, agent=%s, data_service=%s" % (_init_done, _init_error, agent is not None, data_service is not None))
        raise HTTPException(status_code=503, detail=f"Server init failed: {err}")

@app.get("/")
async def root():
    """Health check — visit this URL to confirm the backend is running."""
    return {"status": "running", "message": "Trading Agent API is live"}


@app.get("/ping")
async def ping():
    import os as _os
    return {
        "status": "ok",
        "code_version": "2026-03-08-v3-pure-asgi",
        "server": "fastapi",
        "port": 5000,
        "public_url": f"https://{_os.getenv('REPLIT_DEV_DOMAIN', 'unknown')}",
    }


@app.get("/health")
async def health():
    return {
        "status": "ok" if _init_done else ("init_failed" if _init_error else "starting"),
        "code_version": "2026-03-08-v4-no-auth",
        "init_complete": _init_done,
        "init_error": _init_error,
        "agent_loaded": agent is not None,
        "data_service_loaded": data_service is not None,
    }


@app.post("/api/debug/echo")
async def debug_echo(request: Request):
    """Debug endpoint: echoes the request body back to verify the full pipeline works."""
    try:
        body = await request.body()
        body_str = body.decode("utf-8", errors="replace")[:500]
        return JSONResponse(content={
            "echo": body_str,
            "code_version": "2026-03-08-v3-pure-asgi",
            "has_user_id": bool(getattr(request.state, "user_id", None)),
            "user_id": getattr(request.state, "user_id", None),
        })
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


def _safe_database_url_parts(db_url: str | None) -> tuple[str | None, str | None]:
    if not db_url:
        return None, None
    try:
        parsed = urlparse(db_url)
        host = parsed.hostname
        db_name = unquote((parsed.path or "").lstrip("/")) or None
        return host, db_name
    except Exception:
        return None, None


@app.get("/api/debug/db")
async def debug_db(request: Request):
    """Temporary debugging endpoint for PostgreSQL runtime state."""
    _init_postgres_chat_storage_on_startup("debug_endpoint")
    db_url = os.getenv("NEON_DATABASE_URL") or os.getenv("DATABASE_URL")
    database_host, database_name = _safe_database_url_parts(db_url)

    current_database = None
    current_schema = None
    public_tables = []
    pg_probe_error = None
    try:
        from data.pg_storage import startup_probe as _pg_probe
        probe = _pg_probe()
        current_database = probe.get("database")
        current_schema = probe.get("schema")
        public_tables = probe.get("tables") or []
        pg_probe_error = probe.get("error")
    except Exception as e:
        pg_probe_error = str(e)

    pg_backend_active = False
    try:
        import data.chat_history as _chat_hist
        _chat_hist._ensure_postgres_backend()
        pg_backend_active = bool(getattr(_chat_hist, "_use_postgres", False))
    except Exception:
        pg_backend_active = False

    dev_domain = os.getenv("REPLIT_DEV_DOMAIN")
    suggested_debug_url = f"https://{dev_domain}/api/debug/db" if dev_domain else "/api/debug/db"

    return {
        "database_host": database_host,
        "database_name": database_name,
        "current_database": current_database,
        "current_schema": current_schema,
        "public_tables": public_tables,
        "init_tables_executed_in_process": bool(_pg_startup_checked),
        "postgres_backend_active_in_process": pg_backend_active,
        "last_initialization_error": _pg_last_init_error,
        "pg_probe_error": pg_probe_error,
        "init_attempts": _pg_startup_attempts,
        "suggested_debug_url": suggested_debug_url,
    }


@app.get("/api/debug/provider-call-audit")
async def debug_provider_call_audit(
    request: Request,
    reset: bool = False,
    force_429: Optional[bool] = None,
):
    """
    Provider-call telemetry report.

    Query params:
      - reset=true        Clear all counters and ring buffers (dev/audit use only)
      - force_429=true    Enable FMP_FORCE_429 simulation (all FMP calls return 429)
      - force_429=false   Disable FMP_FORCE_429 simulation
    """
    from services.api_audit import get_report, reset_stats, set_force_429

    if force_429 is not None:
        set_force_429(force_429)

    if reset:
        reset_stats()
        return {"status": "reset", "force_429": force_429}

    report = get_report()
    return report


@app.get("/api/debug/fmp-cost-status")
async def fmp_cost_status(request: Request):
    """
    FMP API cost control status.

    Reports:
      - full-historical call guard state (env vars)
      - theme_rs unique proxy count, last refresh, next eligible refresh per TF
      - stock_compare historical endpoint mode
      - screener_returns endpoint mode + governor state
    No FMP network calls are made.
    """
    from services.fmp_full_guard import is_full_historical_blocked, is_diagnostic_dry_run

    theme_rs_status: dict = {}
    try:
        from services.theme_rs_service import get_theme_rs_status
        theme_rs_status = get_theme_rs_status()
    except Exception as e:
        theme_rs_status = {"error": str(e)}

    return {
        "fmp_call_audit": {
            "full_historical_calls_blocked": is_full_historical_blocked(),
            "diagnostic_dry_run_active":     is_diagnostic_dry_run(),
            "env_vars": {
                "FMP_BLOCK_FULL_HISTORICAL": os.getenv("FMP_BLOCK_FULL_HISTORICAL", "not-set"),
                "FMP_DIAGNOSTIC_DRY_RUN":    os.getenv("FMP_DIAGNOSTIC_DRY_RUN",    "not-set"),
            },
        },
        "theme_rs": theme_rs_status,
        "stock_compare": {
            "hist_price_endpoint":    "historical-price-eod (date-ranged, from/to)",
            "hist_price_range_days":  1826,
            "hist_price_cache_ttl_s": 1800,
            "profile_cache_ttl_s":    86400,
            "quote_cache_ttl_s":      900,
            "statement_cache_ttl_s":  86400,
            "full_history_endpoint":  False,
        },
        "screener_returns": {
            "hist_endpoint":    "historical-price-eod (date-ranged, from/to)",
            "hist_range_days":  180,
            "governor_enabled": True,
            "full_history_endpoint": False,
            "endpoint_mode":    "ranged",
        },
    }


@app.get("/api/debug/quote-consistency/{symbol}")
async def debug_quote_consistency(symbol: str):
    """
    Inspect every quote cache layer for a single symbol.

    Returns the live state of:
      - tradier:quote:sym:{SYM}          — shared 60s per-symbol canonical cache
      - home:wl_tradier_lkg:{SYM}        — Home/Watchlist 72h LKG
      - quote:lkg:{SYM}                  — shared canonical LKG (written by all paths)
      - portfolio:tradier_lkg:{SYM}      — Portfolio 72h LKG
      - watchlist_quote_cache._quote_cache[SYM] — Watchlist 10-min module cache

    Use this endpoint to diagnose price discrepancies across pages.
    """
    from data.cache import cache as _c
    import time as _t
    import services.watchlist_quote_cache as _wqc

    sym = symbol.upper()

    per_sym   = _c.get(f"tradier:quote:sym:{sym}")
    home_lkg  = _c.get(f"home:wl_tradier_lkg:{sym}")
    shared_lkg = _c.get(f"quote:lkg:{sym}")
    port_lkg  = _c.get(f"portfolio:tradier_lkg:{sym}")

    wq_entry  = _wqc._quote_cache.get(sym)
    wq_age_s  = round(_t.monotonic() - _wqc._cache_ts, 1) if _wqc._cache_ts > 0 else None

    def _price(d, *keys):
        for k in keys:
            v = (d or {}).get(k)
            if v is not None:
                try:
                    return float(v)
                except Exception:
                    pass
        return None

    def _str(d, *keys):
        for k in keys:
            v = (d or {}).get(k)
            if v is not None:
                return v
        return None

    return {
        "symbol": sym,
        # ── Canonical 60s per-symbol cache (all Tradier callers share this) ──
        "per_symbol_cache": {
            "key":         f"tradier:quote:sym:{sym}",
            "price":       _price(per_sym, "last"),
            "change":      _price(per_sym, "change"),
            "change_pct":  _price(per_sym, "change_percentage"),
            "volume":      _price(per_sym, "volume"),
            "avg_volume":  _price(per_sym, "average_volume"),
            "prev_close":  _price(per_sym, "prevclose"),
            "high":        _price(per_sym, "high"),
            "low":         _price(per_sym, "low"),
            "bid":         _price(per_sym, "bid"),
            "ask":         _price(per_sym, "ask"),
            "source":      "tradier",          # implied by key namespace
            "has_data":    per_sym is not None,
            "note":        "60s TTL; refreshed by any Tradier caller (Home, Portfolio, screener)",
        },
        # ── Last-known-good caches (fallback only, written on live Tradier hit) ─
        "home_lkg": {
            "key":        f"home:wl_tradier_lkg:{sym}",
            "price":      _price(home_lkg, "last"),
            "change":     _price(home_lkg, "change"),
            "change_pct": _price(home_lkg, "change_percentage"),
            "prev_close": _price(home_lkg, "prevclose"),
            "source":     _str(home_lkg, "quote_source"),
            "stale_flag": True,   # all LKG entries are served as stale=True
            "has_data":   home_lkg is not None,
            "ttl":        "72h",
        },
        "shared_lkg": {
            "key":        f"quote:lkg:{sym}",
            "price":      _price(shared_lkg, "last", "price"),
            "change":     _price(shared_lkg, "change"),
            "change_pct": _price(shared_lkg, "change_percentage", "change_pct"),
            "prev_close": _price(shared_lkg, "prevclose"),
            "source":     _str(shared_lkg, "quote_source"),
            "stale_flag": True,
            "has_data":   shared_lkg is not None,
            "ttl":        "72h",
            "note":       "written by ALL live Tradier paths (Home, Portfolio, screener)",
        },
        "portfolio_lkg": {
            "key":        f"portfolio:tradier_lkg:{sym}",
            "price":      _price(port_lkg, "price", "last"),
            "change":     _price(port_lkg, "change"),
            "change_pct": _price(port_lkg, "change_pct", "change_percentage"),
            "prev_close": _price(port_lkg, "prevclose"),
            "source":     _str(port_lkg, "quote_source"),
            "stale_flag": True,
            "has_data":   port_lkg is not None,
            "ttl":        "72h",
        },
        # ── Watchlist 10-min module cache (overlaid with per-symbol on every read) ─
        "watchlist_module_cache": {
            "price":          _price(wq_entry, "price"),
            "change_pct_1d":  _price(wq_entry, "change_pct_1d"),
            "volume":         _price(wq_entry, "volume"),
            "avg_volume":     _price(wq_entry, "average_volume"),
            "relative_volume": _price(wq_entry, "relative_volume"),
            "quote_source":   (wq_entry or {}).get("quote_source"),
            "quote_updated_at": (wq_entry or {}).get("quote_updated_at"),
            "has_data":       wq_entry is not None,
            "module_cache_age_s": wq_age_s,
            "note":           "Overlay runs on every read: per_symbol_cache wins if fresh",
        },
        # ── Cross-page consistency summary ────────────────────────────────────
        "summary": {
            "prices": {
                "per_symbol_cache": _price(per_sym, "last"),
                "watchlist_module":  _price(wq_entry, "price"),
                "home_lkg":          _price(home_lkg, "last"),
                "shared_lkg":        _price(shared_lkg, "last", "price"),
                "portfolio_lkg":     _price(port_lkg, "price", "last"),
            },
            "change_pcts": {
                "per_symbol_cache": _price(per_sym, "change_percentage"),
                "watchlist_module":  _price(wq_entry, "change_pct_1d"),
                "shared_lkg":        _price(shared_lkg, "change_percentage", "change_pct"),
                "portfolio_lkg":     _price(port_lkg, "change_pct", "change_percentage"),
            },
            "consistent": len({
                v for v in [
                    _price(per_sym, "last"),
                    _price(wq_entry, "price"),
                    _price(shared_lkg, "last", "price"),
                    _price(port_lkg, "price", "last"),
                ]
                if v is not None
            }) <= 1,
            "prev_close_available": per_sym is not None and per_sym.get("prevclose") is not None,
            "notes": [
                "per_symbol_cache wins over all LKG if present (60s TTL)",
                "shared_lkg is written by every live Tradier hit (Home, Portfolio, screener)",
                "LKG entries are served as stale=True / quote_is_stale=True",
                "Watchlist overlay applies per_symbol_cache on every module-cache read",
            ],
        },
    }


@app.get("/api/presets")
async def list_presets(request: Request):
    """List all available preset_intent values the backend supports.

    Useful for verifying frontend-backend sync. Returns the complete
    list of PRESET_ALIASES and INTENT_PROFILES so the frontend can
    check that every button's intent value resolves correctly.
    """
    if agent is None:
        return JSONResponse(status_code=503, content={"error": "Agent not initialized"})
    profiles = list(agent.INTENT_PROFILES.keys())
    aliases = dict(agent.PRESET_ALIASES)
    return {
        "profiles": profiles,
        "aliases": aliases,
        "total_profiles": len(profiles),
        "total_aliases": len(aliases),
    }


# ============================================================
# Agent Collaboration Options (for dropdown menu)
# ============================================================

@app.get("/api/collab-options")
async def get_collab_options(request: Request):
    """Return available solo reasoning models, collaborating agents, and collab presets.

    IMPORTANT — Solo vs Collab semantics:
    • When a user selects a solo model (e.g. "claude", "gpt-4o"), the frontend
      must send  reasoning_model=<model_id>  with NO collab_agents.
      That single model handles the ENTIRE flow: orchestrate → fetch data → reason → respond.
    • The "Custom Collab" button is ONLY for multi-agent collaboration.
      It should ALWAYS display "Custom Collab" regardless of which solo model is selected.
    • Selecting a solo model should NOT change the Custom Collab button label.
    """
    return {
        # Solo models — each one runs the complete flow independently.
        # Frontend sends: { reasoning_model: "<id>" }  (no collab_agents)
        "reasoning_models": [
            {"id": "claude", "name": "Claude", "description": "Anthropic Claude — deep reasoning & synthesis", "mode": "solo", "default": True},
            {"id": "gpt-4o", "name": "ChatGPT", "description": "OpenAI GPT-4o — web search & reasoning", "mode": "solo"},
            {"id": "gemini", "name": "Gemini", "description": "Google Gemini — Google Search grounding & reasoning", "mode": "solo"},
            {"id": "grok", "name": "Grok", "description": "xAI Grok — X/Twitter native search & reasoning", "mode": "solo"},
            {"id": "perplexity", "name": "Perplexity", "description": "Perplexity Sonar — citation-heavy web research", "mode": "solo"},
            {"id": "deepseek", "name": "DeepSeek", "description": "DeepSeek — cost-efficient reasoning & analysis", "mode": "solo"},
        ],
        # Collab agents — full list available for multi-agent collaboration.
        # Frontend sends based on preset — ALWAYS include collab_preset to disambiguate:
        #
        #   Solo (top-row):     { reasoning_model: "<id>" }
        #                       No collab_preset, no collab_agents. Backend preserves family.
        #
        #   Auto preset:        { reasoning_model: "agent_collab", collab_preset: "auto" }
        #                       collab_agents may be omitted — backend chooses dynamically.
        #                       ONLY preset where backend may override provider family.
        #
        #   Default preset:     { reasoning_model: "agent_collab", collab_preset: "default",
        #                         collab_agents: ["grok","gemini"], primary_model: "claude" }
        #                       Fixed family. Backend enforces Claude+Grok+Gemini exactly.
        #                       Backend MUST NOT auto-route to different families.
        #
        #   Full Collaboration: { reasoning_model: "all_agents", collab_preset: "full",
        #                         collab_agents: [all], primary_model: "<user-chosen>" }
        #                       All agents fixed. Backend MUST NOT prune/alter agents.
        #
        #   Custom Collab:      { reasoning_model: "agent_collab", collab_preset: "custom",
        #                         collab_agents: [...user-picked], primary_model: "<user-chosen>" }
        #                       User's exact selections preserved. Backend MUST NOT alter.
        #
        # LEGACY (no collab_preset): inferred from reasoning_model + collab_agents as before.
        "collab_agents": [
            {"id": "claude", "name": "Claude (Anthropic)", "description": "Deep reasoning, analysis & synthesis", "icon": "anthropic"},
            {"id": "grok", "name": "Grok (X/Twitter)", "description": "Real-time X social scanning & sentiment", "icon": "xai"},
            {"id": "gpt-4o", "name": "ChatGPT/OpenAI", "description": "Web search, orchestration & reasoning", "icon": "openai"},
            {"id": "gemini", "name": "Gemini", "description": "Google Search grounding & reasoning", "icon": "gemini"},
            {"id": "perplexity", "name": "Perplexity", "description": "Deep web research with citations", "icon": "perplexity"},
            {"id": "deepseek", "name": "DeepSeek", "description": "Cost-efficient reasoning & analysis", "icon": "🔷"},
        ],
        # Collab presets — pre-configured multi-agent collaboration setups.
        # lock_agents: if true, the collaborator checkboxes are locked (user cannot change them)
        # lock_reasoning: if true, the reasoning model radio is locked (user cannot change it)
        # collab_preset: the discriminator field frontend MUST send to distinguish presets.
        #
        # FOUR DISTINCT PRESETS (Default, Auto, Full Collab, Custom Collab):
        #
        # 1. DEFAULT (id="default", collab_preset="default") — FIXED collaboration preset.
        #    Claude primary + Grok + Gemini collaborators. Provider families are LOCKED.
        #    Backend enforces these families exactly — no auto-routing, no family override.
        #    User may change synthesis tier within Claude family (primary_model tier hint).
        #
        # 2. AUTO (id="auto", collab_preset="auto") — DYNAMIC collaboration preset.
        #    Backend router chooses collaborators freely based on prompt and category.
        #    ONLY preset where family auto-routing is allowed. No fixed agent set.
        #
        # 3. FULL COLLAB (id="full_collab", collab_preset="full") — FIXED, all agents.
        #    Every agent runs simultaneously, user picks synthesis model.
        #    Provider families LOCKED. Backend MUST NOT prune/alter agents.
        #
        # 4. CUSTOM COLLAB (id="custom_collab", collab_preset="custom") — USER-DEFINED.
        #    User picks WHICH collaborating agents AND which synthesis model.
        #    Backend MUST preserve user's exact selections. No family override.
        "presets": [
            {
                "id": "default",
                "collab_preset": "default",
                "ui_concept": "caelyn",
                "ui_label": "Default",
                "name": "Default",
                "description": "Fixed collaboration preset — Claude reasons over Grok + Gemini intelligence. Provider families locked; no auto-routing.",
                "agents": ["grok", "gemini"],
                "reasoning_model": "agent_collab",
                "primary": "claude",
                "mode": "collab",
                "default": True,
                "lock_agents": True,
                "lock_reasoning": False,
                "auto_routing_allowed": False,
            },
            {
                "id": "auto",
                "collab_preset": "auto",
                "ui_concept": "caelyn",
                "ui_label": "Auto",
                "name": "Auto",
                "description": "Dynamic collaboration preset — backend router chooses the best agent mix for each prompt. Only preset with provider-family auto-routing.",
                "agents": [],
                "reasoning_model": "agent_collab",
                "primary": "claude",
                "mode": "collab",
                "lock_agents": True,
                "lock_reasoning": False,
                "auto_routing_allowed": True,
            },
            {
                "id": "full_collab",
                "collab_preset": "full",
                "ui_concept": "customize",
                "ui_label": "Full Collaboration",
                "name": "Full Collaboration",
                "description": "All agents collaborate simultaneously — choose which model reasons. Provider families locked.",
                "agents": ["claude", "grok", "gpt-4o", "gemini", "perplexity"],
                "reasoning_model": "all_agents",
                "primary": "claude",
                "mode": "collab",
                "lock_agents": True,
                "lock_reasoning": False,
                "auto_routing_allowed": False,
            },
            {
                "id": "custom_collab",
                "collab_preset": "custom",
                "ui_concept": "customize",
                "ui_label": "Customize",
                "name": "Customize",
                "description": "Pick your collaborating agent(s) and reasoning model — fully customizable. Backend preserves your exact selections.",
                "agents": [],
                "reasoning_model": "agent_collab",
                "primary": "claude",
                "mode": "collab",
                "lock_agents": False,
                "lock_reasoning": False,
                "auto_routing_allowed": False,
            },
        ],
    }


# ============================================================
# Polymarket Gamma API Proxy
# ============================================================

@app.get("/api/polymarket/events")
@limiter.limit("30/minute")
async def polymarket_events_proxy(request: Request):
    """
    Proxy for Polymarket Gamma API — avoids CORS issues on the frontend.
    Post-processes data to strip expired markets/events so the dashboard
    always shows live, actionable markets only.
    """
    import httpx
    from datetime import datetime, timezone

    params = dict(request.query_params)
    params.setdefault("limit", "75")
    params.setdefault("active", "true")
    params.setdefault("closed", "false")
    params.setdefault("order", "volume24hr")
    params.setdefault("ascending", "false")
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; TradingAgent/1.0)",
        "Accept": "application/json",
    }
    url = "https://gamma-api.polymarket.com/events"
    now = datetime.now(timezone.utc)

    def _market_is_active(market: dict) -> bool:
        """
        Return True only if the market is still open for trading.
        Gamma marks closed markets inconsistently — some have closed=True
        with endDate=None, others have endDate in the past with closed=False
        (resolution lag). We reject on either signal.
        """
        if market.get("closed") is True:
            return False
        if not market.get("acceptingOrders", True):
            return False
        end_date = market.get("endDate") or market.get("endDateIso")
        if end_date:
            try:
                exp = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                if exp <= now:
                    return False
            except Exception:
                pass
        return True

    def _event_is_active(event: dict) -> bool:
        """Return True only if the event itself has not closed."""
        if event.get("closed") is True:
            return False
        end_date = event.get("endDate") or event.get("endDateIso")
        if end_date:
            try:
                exp = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                if exp <= now:
                    return False
            except Exception:
                pass
        return True

    print(f"[POLYMARKET_PROXY] Fetching {url} params={params}")
    try:
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
            resp = await client.get(url, params=params, headers=headers)
            print(f"[POLYMARKET_PROXY] Response status={resp.status_code} len={len(resp.content)}")
            resp.raise_for_status()
            data = resp.json()

            if isinstance(data, list):
                filtered = []
                for event in data:
                    if not _event_is_active(event):
                        continue
                    # Strip expired markets within each event
                    markets = event.get("markets", [])
                    if markets:
                        event = dict(event)
                        event["markets"] = [m for m in markets if _market_is_active(m)]
                        if not event["markets"]:
                            continue
                    filtered.append(event)

                requested_limit = int(params.get("limit", 75))
                filtered = filtered[:requested_limit]
                print(f"[POLYMARKET_PROXY] Filtered {len(data)} → {len(filtered)} active events")
                data = filtered

            response = JSONResponse(content=data)
            response.headers["Cache-Control"] = "no-store"
            return response
    except httpx.HTTPStatusError as e:
        print(f"[POLYMARKET_PROXY] HTTP error: {e.response.status_code} {e.response.text[:300]}")
        return JSONResponse(status_code=502, content={"error": f"Polymarket returned {e.response.status_code}", "detail": e.response.text[:200]})
    except Exception as e:
        print(f"[POLYMARKET_PROXY] Error: {type(e).__name__}: {e}")
        return JSONResponse(status_code=502, content={"error": f"Polymarket API unavailable: {type(e).__name__}: {str(e)[:200]}"})


# ============================================================
# News Feed Endpoint — Categorized news for NotifAI page
# ============================================================

@app.get("/api/news/feed")
@limiter.limit("15/minute")
async def news_feed(request: Request, category: str = "finance"):
    """
    DEPRECATED: News feed has moved to a frontend RSS proxy
    (/api/proxy/news/feed) using free RSS feeds. This endpoint no longer calls
    Perplexity/Brave/Tavily/FMP APIs to avoid wasting API credits.
    """
    return JSONResponse(content={
        "articles": [],
        "category": category,
        "count": 0,
        "notice": "News feed moved to frontend proxy (/api/proxy/news/feed). This endpoint is deprecated.",
    })


# ============================================================
# NotifAI — Weekly AI Summary  (refreshes Saturdays at 7 am)
# ============================================================

def _notifai_weekly_cache_ttl() -> int:
    """Seconds until next Saturday 07:00 local time (minimum 60 s)."""
    from datetime import datetime, timedelta
    now = datetime.now()
    days_until_sat = (5 - now.weekday()) % 7  # Saturday = weekday 5
    if days_until_sat == 0 and (now.hour > 7 or (now.hour == 7 and now.minute > 0)):
        days_until_sat = 7  # already past 7 am Saturday → next Saturday
    next_sat = now.replace(hour=7, minute=0, second=0, microsecond=0) + timedelta(days=days_until_sat)
    ttl = int((next_sat - now).total_seconds())
    return max(ttl, 60)


@app.get("/api/notifai/weekly-summary")
@limiter.limit("20/minute")
async def notifai_weekly_summary(request: Request, force: bool = False):
    """
    Returns a Claude-written weekly market recap + outlook.
    Cached until next Saturday at 07:00 (stays up all weekend).
    Pass ?force=true to regenerate immediately (owner-only in prod).
    Logic lives in services/notifai_service.py.
    """
    from services import notifai_service as _notifai_svc
    await _wait_for_init()
    result = await _notifai_svc.generate_weekly_summary(data_service, agent, force=force)
    return JSONResponse(content=result)


@app.get("/api/notifai/the-brief")
@limiter.limit("30/minute")
async def notifai_the_brief(request: Request):
    """
    Returns structured earnings + economic calendar for the current week.
    Lighter endpoint used by the 'THE BRIEF' section on the NotifAI page.
    Cached 2 hours. Logic lives in services/notifai_service.py.
    """
    from services import notifai_service as _notifai_svc
    await _wait_for_init()
    result = await _notifai_svc.fetch_the_brief(data_service, agent)
    return JSONResponse(content=result)


# ============================================================
# Earnings Calendar — startup cache warmer
# ============================================================

async def _earnings_calendar_warmup():
    """
    Background task that runs once on startup.
    Pre-warms the earnings calendar cache for the current week and next two
    weeks so the very first user click is instant (cache hit) rather than
    triggering a cold FMP call that can take 2-5 s.

    Each weekly fetch also pre-populates the individual per-day cache entries
    (via the pre-warm logic inside earnings_calendar), so clicking any day in
    those weeks is also instant.
    """
    from datetime import datetime, timedelta
    import asyncio as _asyncio
    from data.cache import cache as _cache
    from services.catalyst_calendar_service import CatalystFMP as _CFMP, _enrich_profiles as _enrich
    from config import FMP_API_KEY as _fmp_key

    await _asyncio.sleep(5)   # Brief pause so the server finishes starting up

    today = datetime.now()
    _fmp_logo_base = "https://financialmodelingprep.com/image-stock"

    # Find the Sunday of the current week (week start for the frontend's weekly view)
    days_since_sunday = today.weekday() + 1 if today.weekday() != 6 else 0
    week_start = (today - timedelta(days=days_since_sunday)).date()

    # Warm current week + next 2 weeks
    weeks_to_warm = [week_start + timedelta(weeks=i) for i in range(3)]

    for ws in weeks_to_warm:
        from_d = ws.strftime("%Y-%m-%d")
        to_d   = (ws + timedelta(days=6)).strftime("%Y-%m-%d")
        ck     = f"fmp:earnings_calendar:v5:{from_d}:{to_d}"

        if _cache.get(ck) is not None:
            print(f"[EARN_WARMUP] {from_d}→{to_d}: already cached, skip")
            continue

        try:
            print(f"[EARN_WARMUP] Warming {from_d}→{to_d} …")
            fmp = _CFMP(_fmp_key)
            raw_rows = await fmp.earnings_calendar(from_d, to_d)
            if not isinstance(raw_rows, list):
                raw_rows = []

            seen_syms: dict[str, float] = {}
            for r in raw_rows:
                sym = (r.get("symbol") or "").upper()
                if sym:
                    rev = r.get("revenueEstimated") or 0
                    if sym not in seen_syms or rev > seen_syms[sym]:
                        seen_syms[sym] = rev

            sorted_syms = sorted(seen_syms, key=lambda s: seen_syms[s], reverse=True)
            enriched    = await _enrich(sorted_syms[:200], fmp, max_live_fetches=50)

            results: list = []
            counts_by_date: dict[str, int] = {}
            events_by_date: dict[str, list] = {}

            for row in raw_rows:
                sym = (row.get("symbol") or "").upper()
                if not sym:
                    continue
                date_str = row.get("date") or ""
                profile  = enriched.get(sym, {})
                co_name  = (profile.get("companyName") or row.get("name")
                            or row.get("companyName") or sym)
                logo_url = profile.get("logo") or f"{_fmp_logo_base}/{sym}.png"
                item = {
                    "ticker": sym, "date": date_str, "symbol": sym,
                    "companyName": co_name, "logo": logo_url, "image": logo_url,
                    "price": profile.get("price"),
                    "changesPercentage": (profile.get("changePercentage")
                                         or profile.get("changesPercentage")),
                    "marketCap": profile.get("marketCap"),
                    "sector": profile.get("sector"),
                    "industry": profile.get("industry"),
                    "epsEstimated":    row.get("epsEstimated"),
                    "epsActual":       row.get("eps") or row.get("epsActual"),
                    "revenueEstimated":row.get("revenueEstimated"),
                    "revenueActual":   row.get("revenue") or row.get("revenueActual"),
                    "eps_estimate":    row.get("epsEstimated"),
                    "eps_actual":      row.get("eps") or row.get("epsActual"),
                    "revenue_estimate":row.get("revenueEstimated"),
                    "revenue_actual":  row.get("revenue") or row.get("revenueActual"),
                    "hour": row.get("time", ""), "quarter": row.get("fiscalDateEnding"),
                    "year": None, "source": "fmp",
                    "title": f"{co_name} Earnings",
                }
                results.append(item)
                if date_str:
                    counts_by_date[date_str] = counts_by_date.get(date_str, 0) + 1
                    events_by_date.setdefault(date_str, []).append(item)

            response = {
                "earnings": results, "countsByDate": counts_by_date,
                "eventsByDate": events_by_date, "from": from_d, "to": to_d,
                "count": len(results), "source": "fmp",
            }
            _cache.set(ck, response, 6 * 3600)

            # Also pre-warm each individual day
            for day_date, day_events in events_by_date.items():
                day_key = f"fmp:earnings_calendar:v5:{day_date}:{day_date}"
                if _cache.get(day_key) is None:
                    _cache.set(day_key, {
                        "earnings": day_events,
                        "countsByDate": {day_date: len(day_events)},
                        "eventsByDate": {day_date: day_events},
                        "from": day_date, "to": day_date,
                        "count": len(day_events), "source": "fmp",
                    }, 6 * 3600)

            print(f"[EARN_WARMUP] {from_d}→{to_d}: {len(results)} events, "
                  f"{len(events_by_date)} days pre-warmed")
            await _asyncio.sleep(2)   # Throttle between weeks to avoid FMP rate spikes

        except Exception as _ex:
            print(f"[EARN_WARMUP] {from_d}→{to_d} error: {_ex}")


# ============================================================
# Earnings Calendar Endpoint — FMP calendar by date range (enriched)
# ============================================================

@app.get("/api/earnings/calendar")
@limiter.limit("10/minute")
async def earnings_calendar(
    request: Request,
    from_date: str = "",
    to_date: str = "",
    days: int = 30,
):
    """
    Returns FMP earnings calendar for a date range with company enrichment.

    Accepts ?from=YYYY-MM-DD&to=YYYY-MM-DD (preferred, used by frontend) OR
    ?from_date=...&to_date=... (legacy).  Defaults to today → today+30 days.

    Enriches up to 50 companies by profile
    (name, logo, price, changesPercentage, marketCap, sector, industry).
    Logos fall back to the FMP image-stock CDN pattern for un-enriched companies.

    Returns: { earnings, countsByDate, eventsByDate, from, to, count, source }
    Cache key: fmp:earnings_calendar:v5:{from}:{to}  TTL: 6 h
    """
    from datetime import datetime, timedelta
    from services.catalyst_calendar_service import CatalystFMP, _enrich_profiles

    await _wait_for_init()

    # Accept both ?from=... and ?from_date=... (frontend sends the shorter form)
    qp = request.query_params
    from_date = qp.get("from") or from_date or ""
    to_date   = qp.get("to")   or to_date   or ""

    today = datetime.now()
    if not from_date:
        from_date = today.strftime("%Y-%m-%d")
    if not to_date:
        to_date = (today + timedelta(days=days)).strftime("%Y-%m-%d")

    from data.cache import cache
    from config import FMP_API_KEY as _fmp_key
    cache_key = f"fmp:earnings_calendar:v5:{from_date}:{to_date}"
    cached = cache.get(cache_key)
    if cached is not None:
        return JSONResponse(content=cached)

    _fmp_logo_base = "https://financialmodelingprep.com/image-stock"

    try:
        fmp = CatalystFMP(_fmp_key)
        raw_rows = await fmp.earnings_calendar(from_date, to_date)
        if not isinstance(raw_rows, list):
            raw_rows = []

        # Deduplicate symbols — enrich top 50 by revenue estimate (most important)
        seen_syms: dict[str, float] = {}
        for r in raw_rows:
            sym = (r.get("symbol") or "").upper()
            if sym:
                rev = r.get("revenueEstimated") or 0
                if sym not in seen_syms or rev > seen_syms[sym]:
                    seen_syms[sym] = rev

        # Sort by revenue estimate desc — prioritises the biggest companies for enrichment
        sorted_syms = sorted(seen_syms, key=lambda s: seen_syms[s], reverse=True)
        unique_syms = sorted_syms[:200]   # fetch profiles for up to 200; hard cap inside _enrich_profiles

        # Enrich profiles — capped at 50 live HTTP calls; cached 24 h
        enriched_profiles = await _enrich_profiles(unique_syms, fmp, max_live_fetches=50)

        results = []
        counts_by_date: dict[str, int] = {}
        events_by_date: dict[str, list] = {}

        for row in raw_rows:
            sym = (row.get("symbol") or "").upper()
            if not sym:
                continue
            date_str = row.get("date") or ""
            profile = enriched_profiles.get(sym, {})

            company_name = (
                profile.get("companyName")
                or row.get("name")
                or row.get("companyName")
                or sym
            )
            # Profile logo wins; fall back to predictable FMP CDN URL (no API call)
            logo_url = profile.get("logo") or f"{_fmp_logo_base}/{sym}.png"
            price = profile.get("price")
            changes_pct = profile.get("changePercentage") or profile.get("changesPercentage") or profile.get("changes")
            market_cap = profile.get("marketCap")

            item = {
                # backward-compatible fields
                "ticker":             sym,
                "date":               date_str,
                "eps_estimate":       row.get("epsEstimated"),
                "eps_actual":         row.get("eps") or row.get("epsActual"),
                "revenue_estimate":   row.get("revenueEstimated"),
                "revenue_actual":     row.get("revenue") or row.get("revenueActual"),
                "hour":               row.get("time", ""),
                "quarter":            row.get("fiscalDateEnding") or row.get("period"),
                "year":               None,
                # enriched fields
                "symbol":             sym,
                "companyName":        company_name,
                "logo":               logo_url,
                "image":              logo_url,
                "price":              price,
                "changesPercentage":  changes_pct,
                "sector":             profile.get("sector"),
                "industry":           profile.get("industry"),
                "marketCap":          market_cap,
                "epsEstimated":       row.get("epsEstimated"),
                "epsActual":          row.get("eps") or row.get("epsActual"),
                "revenueEstimated":   row.get("revenueEstimated"),
                "revenueActual":      row.get("revenue") or row.get("revenueActual"),
                "title":              f"{company_name} Earnings",
                "source":             "fmp",
            }
            results.append(item)

            if date_str:
                counts_by_date[date_str] = counts_by_date.get(date_str, 0) + 1
                events_by_date.setdefault(date_str, []).append(item)

        response = {
            "earnings":      results,
            "countsByDate":  counts_by_date,
            "eventsByDate":  events_by_date,
            "from":          from_date,
            "to":            to_date,
            "count":         len(results),
            "source":        "fmp",
        }
        cache.set(cache_key, response, 6 * 3600)

        # ── Pre-warm per-day cache entries ──────────────────────────────────
        # When the frontend loads a weekly view then clicks a single day, it
        # calls ?from=DATE&to=DATE.  That is a different cache key from the
        # weekly response, so without pre-warming it would trigger a live FMP
        # call (slow, ~2-5 s) while the frontend shows "No earnings calls".
        # We build and store each day's response now so single-day clicks are
        # instant cache hits.
        for day_date, day_events in events_by_date.items():
            day_key = f"fmp:earnings_calendar:v5:{day_date}:{day_date}"
            if cache.get(day_key) is None:
                day_response = {
                    "earnings":      day_events,
                    "countsByDate":  {day_date: len(day_events)},
                    "eventsByDate":  {day_date: day_events},
                    "from":          day_date,
                    "to":            day_date,
                    "count":         len(day_events),
                    "source":        "fmp",
                }
                cache.set(day_key, day_response, 6 * 3600)

        print(f"[EARNINGS_CALENDAR] FMP {len(results)} events ({from_date} → {to_date}), "
              f"enriched={len(enriched_profiles)}, pre-warmed {len(events_by_date)} day caches")
        return JSONResponse(content=response)

    except Exception as e:
        print(f"[EARNINGS_CALENDAR] Error: {e}")
        return JSONResponse(
            status_code=502,
            content={"error": f"FMP earnings calendar unavailable: {str(e)[:200]}"},
        )


# ============================================================
# Smart Earnings Endpoints — AI-curated ticker filtering
# ============================================================

@app.get("/api/earnings/smart/{date}")
@limiter.limit("20/minute")
async def smart_earnings_for_date(request: Request, date: str):
    """
    Return Tier 2 (social + news ranked) earnings tickers for a specific date.
    Reads from file-backed cache only — NO automatic Grok/Perplexity calls.
    Use POST /api/earnings/refresh-smart-cache to trigger a fresh scan on demand
    (the "Ask Caelyn" button on the Earnings Calendar page).
    """
    await _wait_for_init()

    from data.smart_earnings_scanner import get_cached_smart_day, get_cache_status

    cached = get_cached_smart_day(date)
    if cached:
        cached["cache_status"] = get_cache_status()
        cached["scanning"] = _smart_scan_running
        return JSONResponse(content=cached)

    # Cache miss — return empty, do NOT auto-trigger a scan
    return JSONResponse(content={
        "tickers": [],
        "count": 0,
        "cached_at": 0,
        "cache_status": get_cache_status(),
        "scanning": _smart_scan_running,
        "message": "No scan yet for this week. Click 'Ask Caelyn' to run one.",
    })


@app.get("/api/earnings/smart-status")
@limiter.limit("30/minute")
async def smart_earnings_status(request: Request):
    """Return cache freshness status for UI display."""
    from data.smart_earnings_scanner import get_cache_status
    status = get_cache_status()
    status["scanning"] = _smart_scan_running
    return JSONResponse(content=status)


@app.post("/api/earnings/refresh-smart-cache")
@limiter.limit("2/minute")
async def refresh_smart_cache(request: Request, x_api_key: str = Header(None), date: str = None):
    """Manual trigger for smart earnings scan. Runs in background.
    Optional 'date' query param to scan a specific week."""
    if not _jwt_or_key(request, x_api_key):
        raise HTTPException(status_code=403, detail="Invalid API key")

    global _smart_scan_running
    if _smart_scan_running:
        return JSONResponse(content={"status": "already_running"})

    await _wait_for_init()
    from config import XAI_API_KEY, PERPLEXITY_API_KEY
    from data.smart_earnings_scanner import run_smart_scan

    async def _run():
        global _smart_scan_running
        _smart_scan_running = True
        try:
            await run_smart_scan(data_service.finnhub.client, XAI_API_KEY, PERPLEXITY_API_KEY, reference_date=date)
        except Exception as e:
            print(f"[SMART_EARNINGS] Manual refresh failed: {e}")
        finally:
            _smart_scan_running = False

    asyncio.create_task(_run())
    return JSONResponse(content={"status": "started"})


# ============================================================
# User Settings Endpoints — Standing Instructions + Profile
# ============================================================

@app.get("/api/settings")
@limiter.limit("20/minute")
async def get_settings_endpoint(request: Request):
    from data.user_settings import get_settings
    from agent.prompts import DEFAULT_PERSONAL_PROFILE, CORE_QUANT_DNA
    settings = get_settings()
    settings["default_personal_profile"] = DEFAULT_PERSONAL_PROFILE
    settings["core_quant_dna"] = CORE_QUANT_DNA
    return JSONResponse(content=settings)


@app.put("/api/settings")
@limiter.limit("20/minute")
async def update_settings_endpoint(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    if not _jwt_or_key(request, api_key):
        return JSONResponse(status_code=403, content={"error": "Invalid API key"})
    body = await request.json()
    from data.user_settings import save_settings
    settings = save_settings(
        standing_instructions=body.get("standing_instructions"),
        personal_profile=body.get("personal_profile"),
        instruction_presets=body.get("instruction_presets"),
        profile_presets=body.get("profile_presets"),
        active_instruction_template=body.get("active_instruction_template"),
        active_profile_template=body.get("active_profile_template"),
    )
    return JSONResponse(content=settings)


@app.post("/api/settings/templates")
@limiter.limit("20/minute")
async def save_template_endpoint(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    if not _jwt_or_key(request, api_key):
        return JSONResponse(status_code=403, content={"error": "Invalid API key"})
    body = await request.json()
    template_type = body.get("type")  # "instruction" or "profile"
    name = body.get("name", "")
    content = body.get("content", "")
    if template_type not in ("instruction", "profile"):
        return JSONResponse(status_code=400, content={"error": "type must be 'instruction' or 'profile'"})
    try:
        from data.user_settings import save_template
        settings = save_template(template_type, name, content)
        return JSONResponse(content=settings)
    except ValueError as e:
        return JSONResponse(status_code=400, content={"error": str(e)})


@app.delete("/api/settings/templates")
@limiter.limit("20/minute")
async def delete_template_endpoint(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
    template_type: str = "",
    name: str = "",
):
    if not _jwt_or_key(request, api_key):
        return JSONResponse(status_code=403, content={"error": "Invalid API key"})
    if template_type not in ("instruction", "profile"):
        return JSONResponse(status_code=400, content={"error": "type must be 'instruction' or 'profile'"})
    from data.user_settings import delete_template
    settings = delete_template(template_type, name)
    return JSONResponse(content=settings)


# ============================================================
# Earnings Detail Endpoint — Web Search + Finnhub enrichment
# ============================================================

@app.get("/api/earnings/detail")
@limiter.limit("30/minute")
async def earnings_detail(request: Request, ticker: str = ""):
    """
    Enriched earnings detail for a single ticker.
    Called on-demand when user clicks an earnings entry (NOT on page load).

    Data sources (all free tier / included in API key):
      - Finnhub: company profile, earnings history, analyst trends, quote, news
      - SEC EDGAR XBRL: revenue, financials (free, no key needed)

    IMPORTANT: No Perplexity, no LLM, no web_search calls here.
    News sentiment is a simple keyword heuristic (see lines below).
    news_summary field is always empty string — no AI summarization.
    """
    ticker = ticker.upper().strip()
    if not ticker or len(ticker) > 6:
        return JSONResponse(status_code=400, content={"error": "Invalid ticker"})

    await _wait_for_init()

    from data.cache import cache
    cache_key = f"earnings_detail_v3:{ticker}"
    cached = cache.get(cache_key)
    if cached is not None:
        return JSONResponse(content=cached)

    result = {"ticker": ticker}

    # Phase 1: Get company profile first (usually cached, fast)
    # We need the company name to make news searches relevant
    company_name = ""
    try:
        profile = await asyncio.wait_for(
            asyncio.to_thread(agent.data.finnhub.get_company_profile, ticker),
            timeout=4.0,
        )
        if isinstance(profile, dict):
            result["company_profile"] = profile
            company_name = profile.get("name", "")
    except Exception as e:
        print(f"[EARNINGS_DETAIL] {ticker}/company_profile failed: {e}")

    # Phase 2: Fetch remaining data in parallel (including news with company name)
    tasks = {}

    # Finnhub: earnings surprises (past 4 quarters)
    try:
        tasks["earnings_history"] = asyncio.wait_for(
            asyncio.to_thread(agent.data.finnhub.get_earnings_surprises, ticker),
            timeout=6.0,
        )
    except Exception:
        pass

    # Finnhub: upcoming earnings for this ticker
    try:
        tasks["earnings_upcoming"] = asyncio.wait_for(
            asyncio.to_thread(agent.data.finnhub.get_earnings_calendar, ticker),
            timeout=6.0,
        )
    except Exception:
        pass

    # Finnhub: analyst recommendations
    try:
        tasks["analyst_recommendations"] = asyncio.wait_for(
            asyncio.to_thread(agent.data.finnhub.get_recommendation_trends, ticker),
            timeout=5.0,
        )
    except Exception:
        pass

    # Finnhub: quote for current price
    try:
        tasks["quote"] = asyncio.wait_for(
            asyncio.to_thread(agent.data.finnhub.get_quote, ticker),
            timeout=4.0,
        )
    except Exception:
        pass

    # Finnhub: company-specific news (guaranteed relevant to this ticker)
    try:
        tasks["company_news"] = asyncio.wait_for(
            asyncio.to_thread(agent.data.finnhub.get_company_news, ticker),
            timeout=8.0,
        )
    except Exception:
        pass

    # FMP: earnings enrichment — additive, non-breaking
    # Adds revenue context (actual + estimate per quarter) that Finnhub surprises lacks.
    # stable/earnings works on Starter; stable/income-statement also works.
    if agent.data.fmp:
        try:
            tasks["fmp_earnings_enrichment"] = asyncio.wait_for(
                agent.data.fmp.get_earnings_enrichment(ticker),
                timeout=6.0,
            )
        except Exception:
            pass

    if tasks:
        task_keys = list(tasks.keys())
        task_coros = list(tasks.values())
        results = await asyncio.gather(*task_coros, return_exceptions=True)

        for key, res in zip(task_keys, results):
            if isinstance(res, Exception):
                print(f"[EARNINGS_DETAIL] {ticker}/{key} failed: {type(res).__name__}: {res}")
                continue
            if not res:
                continue
            result[key] = res

    # Compute earnings track record from history
    history = result.get("earnings_history", [])
    if isinstance(history, list) and history:
        beats = sum(1 for h in history if isinstance(h, dict) and h.get("beat") is True)
        total = sum(1 for h in history if isinstance(h, dict) and h.get("beat") is not None)
        if total > 0:
            result["beat_rate"] = f"{beats}/{total}"
            result["beat_pct"] = round((beats / total) * 100)
            avg_surprise = sum(
                h.get("surprise_percent", 0) or 0
                for h in history if isinstance(h, dict)
            ) / len(history)
            result["avg_surprise_pct"] = round(avg_surprise, 2)

    # Extract key fields from profile
    profile = result.get("company_profile", {})
    if isinstance(profile, dict):
        result["company_name"] = profile.get("name", ticker)
        result["sector"] = profile.get("sector", "")
        result["industry"] = profile.get("industry", "")
        result["market_cap"] = profile.get("market_cap")
        result["logo"] = profile.get("logo", "")

    # Current price from quote
    quote = result.get("quote", {})
    if isinstance(quote, dict) and quote.get("price"):
        result["current_price"] = quote["price"]
        result["price_change_pct"] = quote.get("change_pct")

    # Extract analyst consensus from recommendations
    recs = result.get("analyst_recommendations", [])
    if isinstance(recs, list) and recs:
        latest = recs[0] if isinstance(recs[0], dict) else {}
        buy = (latest.get("buy", 0) or 0) + (latest.get("strongBuy", 0) or 0)
        sell = (latest.get("sell", 0) or 0) + (latest.get("strongSell", 0) or 0)
        hold = latest.get("hold", 0) or 0
        total_analysts = buy + sell + hold
        if total_analysts > 0:
            result["analyst_consensus"] = {
                "buy": buy,
                "hold": hold,
                "sell": sell,
                "total": total_analysts,
                "rating": "Buy" if buy > hold + sell else "Hold" if hold >= sell else "Sell",
            }

    # Company news from Finnhub (guaranteed relevant — tagged to this ticker)
    company_articles = result.get("company_news", [])
    if isinstance(company_articles, list) and company_articles:
        result["news_articles"] = company_articles[:6]

        # Simple sentiment heuristic from article titles
        all_titles = " ".join(a.get("title", "") for a in company_articles).lower()
        bullish_words = ["beat", "surge", "rally", "upgrade", "outperform", "strong", "record", "growth"]
        bearish_words = ["miss", "decline", "downgrade", "underperform", "cut", "warning", "loss", "weak"]
        bull = sum(1 for w in bullish_words if w in all_titles)
        bear = sum(1 for w in bearish_words if w in all_titles)
        result["news_sentiment"] = "Bullish" if bull > bear else "Bearish" if bear > bull else "Neutral"
        result["news_summary"] = ""
    else:
        result["news_articles"] = []
        result["news_sentiment"] = "Neutral"
        result["news_summary"] = ""

    # Phase 3: EDGAR XBRL — revenue trend (free, no key needed)
    # Adds last 4 quarters of revenue to show growth/decline context for earnings
    try:
        cik = await agent.data.sec_edgar.resolve_cik(ticker)
        if cik:
            from data.sec_edgar_provider import EdgarBudget
            edgar_budget = EdgarBudget(max_requests=2)
            edgar_financials = await asyncio.wait_for(
                agent.data.sec_edgar.get_company_financials(cik, budget=edgar_budget),
                timeout=6.0,
            )
            if edgar_financials:
                result["edgar_financials"] = edgar_financials
                print(f"[EARNINGS_DETAIL] EDGAR enriched {ticker}: {list(edgar_financials.keys())}")
    except Exception as e:
        print(f"[EARNINGS_DETAIL] EDGAR enrichment failed for {ticker}: {e}")

    # Phase 4: FMP earnings enrichment — flatten from gathered task result
    fmp_enrich = result.pop("fmp_earnings_enrichment", None)
    if isinstance(fmp_enrich, dict):
        fmp_earnings_history = fmp_enrich.get("earnings_history", [])
        fmp_income_stmts = fmp_enrich.get("income_statements", [])
        if fmp_earnings_history:
            result["fmp_earnings_history"] = fmp_earnings_history
        if fmp_income_stmts:
            result["fmp_income_statements"] = fmp_income_stmts
        # Promote upcoming EPS+revenue estimate from FMP if not already covered
        upcoming_fmp = [e for e in fmp_earnings_history if not e.get("report_available")]
        if upcoming_fmp:
            next_event = upcoming_fmp[0]
            result.setdefault("fmp_next_earnings_date", next_event.get("date"))
            result.setdefault("fmp_next_eps_estimate", next_event.get("eps_estimate"))
            result.setdefault("fmp_next_revenue_estimate", next_event.get("revenue_estimate"))

    # Phase 5: Polymarket enrichment — popup-only, never shown in main calendar
    # Search for active prediction markets referencing this ticker or company.
    # Results are keyed as "polymarket_markets" so the frontend can render them
    # as a distinct "Prediction Markets / Beat-Miss Odds" section inside the popup.
    try:
        # Common words that appear in company names but are too generic to search on
        _POLY_STOPWORDS = {
            "inc", "corp", "ltd", "llc", "co", "company", "group", "holdings",
            "the", "and", "of", "for", "a", "an", "plc", "sa", "ag", "se",
            "technologies", "technology", "services", "solutions", "international",
        }

        def _poly_keywords(text: str) -> list[str]:
            """Extract meaningful keywords from a ticker or company name."""
            words = text.lower().replace(",", " ").replace(".", " ").split()
            return [w for w in words if len(w) > 2 and w not in _POLY_STOPWORDS]

        def _poly_event_matches(ev: dict, keywords: list[str]) -> bool:
            """True if any keyword appears as a whole-word-ish match in the event text."""
            combined = (
                f"{ev.get('title', '')} {ev.get('description', '')} "
                + " ".join(ev.get("tags", []))
            ).lower()
            return any(kw in combined for kw in keywords)

        # Fetch top events once (cached by polymarket provider)
        all_poly = await asyncio.wait_for(
            agent.data.polymarket.get_top_events(limit=100),
            timeout=5.0,
        )

        poly_markets: list[dict] = []
        if isinstance(all_poly, list):
            ticker_kw = _poly_keywords(ticker)
            cname_kw = _poly_keywords(result.get("company_name", ""))
            # Use all unique meaningful keywords; require at least one to be >3 chars
            all_kw = list({*ticker_kw, *cname_kw})
            all_kw = [kw for kw in all_kw if len(kw) > 3]

            if all_kw:
                for ev in all_poly:
                    if _poly_event_matches(ev, all_kw):
                        poly_markets.append(ev)

        if poly_markets:
            result["polymarket_markets"] = poly_markets[:8]
            print(f"[EARNINGS_DETAIL] Polymarket enriched {ticker}: {len(poly_markets)} markets found")
        else:
            result["polymarket_markets"] = []
    except Exception as e:
        print(f"[EARNINGS_DETAIL] Polymarket enrichment failed for {ticker}: {e}")
        result["polymarket_markets"] = []

    # Cache for 10 minutes
    cache.set(cache_key, result, 600)
    return JSONResponse(content=result)
async def verify_api_key(request: Request, x_api_key: Optional[str] = Header(None)):
    """Verify the API key sent in the X-API-Key header, or pass if JWT-authenticated."""
    if not _jwt_or_key(request, x_api_key):
        raise HTTPException(
            status_code=403,
            detail="Invalid or missing API key.",
        )
    return x_api_key


class QueryRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    query: Optional[str] = None
    prompt: Optional[str] = None
    conversation_id: Optional[str] = None
    preset_intent: Optional[str] = None
    csv_data: Optional[str] = None
    chatbox_mode: Optional[bool] = False
    reasoning_model: Optional[str] = "agent_collab"
    collab_agents: Optional[List[str]] = None       # e.g. ["grok","gpt-4o","gemini","perplexity"]
    primary_model: Optional[str] = None              # final synthesis model (default: claude)
    history: Optional[List[dict]] = None             # client-side conversation history (for model-switch continuity)
    # Phase 5: optional tier hint for AI Terminal solo-model selection.
    # "fast" | "balanced" | "premium" — only respected when reasoning_model is a solo model
    # (claude/gpt-4o/grok/gemini/perplexity/deepseek). Ignored for agent_collab/all_agents.
    # Default None = tier determined automatically by prompt complexity + category floor.
    reasoning_mode: Optional[str] = None
    # collab_preset: explicit discriminator for Customize-panel presets.
    # Tells backend EXACTLY which preset the user is in, eliminating ambiguity between
    # Default (fixed) and Custom (user-defined) which otherwise look identical by
    # reasoning_model + collab_agents alone.
    # Values: "default" | "auto" | "full" | "custom" | None (legacy/solo inference)
    collab_preset: Optional[str] = None
    # collaboration_mode: alternate field name the current frontend sends.
    # Frontend sends collaboration_mode (e.g. "default", "auto", "full_collab", "custom_collab")
    # alongside reasoning_model=<family> (e.g. "claude", "grok").
    # Phase 0 bridges this to collab_preset so all downstream logic stays unchanged.
    # If both are provided, collab_preset wins.
    collaboration_mode: Optional[str] = None
    # screen_context: page-level snapshot injected by the frontend on every /api/query call.
    # Contains the current page slug, active tab, visible symbols list, and top visible rows
    # so the agent can tailor its reasoning to what the user is actually looking at.
    # All fields are optional — absence of any key must be handled gracefully.
    screen_context: Optional[dict] = None
def _build_meta(req_id: str, preset_intent=None, conv_id=None, routing=None, timing_ms=None, reasoning_model=None):
    rm = normalize_reasoning_model(reasoning_model)
    return {
        "request_id": req_id,
        "preset_intent": preset_intent,
        "conversation_id": conv_id,
        "routing": routing or {"source": "unknown", "confidence": "low", "category": "unknown"},
        "timing_ms": timing_ms or {"total": 0, "grok": 0, "data": 0, "claude": 0},
        "mode_concept": mode_concept(rm),
        "mode_label": mode_display_label(rm),
        "reasoning_model": rm,
    }
def _render_cross_market_analysis(s: dict) -> str:
    parts = []
    regime = s.get("macro_regime", {})
    if regime:
        verdict = regime.get("verdict", "N/A")
        summary = regime.get("summary", "")
        fg = regime.get("fear_greed", "")
        vix = regime.get("vix", "")
        parts.append(f"MACRO REGIME: {verdict}")
        if summary:
            parts.append(summary)
        indicators = []
        if fg:
            indicators.append(f"Fear & Greed: {fg}")
        if vix:
            indicators.append(f"VIX: {vix}")
        if indicators:
            parts.append(" | ".join(indicators))
        parts.append("")

    assessments = s.get("asset_class_assessment", [])
    if assessments:
        parts.append("ASSET CLASS OUTLOOK:")
        for a in assessments:
            ac = a.get("asset_class", "")
            reg = a.get("regime", "")
            rat = a.get("rationale", "")
            parts.append(f"  {ac}: {reg} — {rat}")
        parts.append("")

    def _render_item(p):
        sym = p.get("symbol", p.get("ticker", "?"))
        company = p.get("company", "")
        classification = p.get("classification", "")
        rating = p.get("rating", "")
        confidence = p.get("confidence", "")
        change = p.get("change", "")
        mcap = p.get("market_cap", "")
        header = f"{sym}"
        if company:
            header += f" ({company})"
        if classification:
            header += f" [{classification}]"
        detail_parts = []
        if rating:
            detail_parts.append(f"Rating: {rating}")
        if confidence:
            detail_parts.append(f"Confidence: {confidence}")
        if change:
            detail_parts.append(f"Change: {change}")
        if mcap:
            detail_parts.append(f"MCap: {mcap}")
        vel = p.get("social_velocity_label", "")
        if vel:
            detail_parts.append(f"Velocity: {vel}")
        if detail_parts:
            header += " | " + " | ".join(detail_parts)
        parts.append(header)
        bullets = p.get("thesis_bullets", [])
        thesis_str = p.get("thesis", "")
        if bullets:
            for b in bullets:
                parts.append(f"  • {b}")
        elif thesis_str:
            parts.append(f"  {thesis_str}")
        catalyst = p.get("catalyst", "")
        if catalyst:
            parts.append(f"  Catalyst: {catalyst}")
        confs = p.get("confirmations", {})
        if confs and isinstance(confs, dict):
            conf_strs = []
            for k in ("ta", "volume", "catalyst", "fa"):
                v = confs.get(k)
                if v is True:
                    conf_strs.append(f"{k.upper()}:Y")
                elif v is False:
                    conf_strs.append(f"{k.upper()}:N")
            if conf_strs:
                parts.append(f"  Confirmations: {' | '.join(conf_strs)}")
        fail = p.get("why_could_fail", "")
        if fail:
            parts.append(f"  Risk: {fail}")
        ps = p.get("position_size", "")
        if ps:
            parts.append(f"  Position: {ps}")
        parts.append("")

    def _render_group(label, items):
        if not items:
            return
        parts.append(f"--- {label} ---")
        for p in items:
            _render_item(p)

    equities = s.get("equities", {})
    if isinstance(equities, dict):
        _render_group("EQUITIES — LARGE CAPS", equities.get("large_caps", []))
        _render_group("EQUITIES — MID CAPS", equities.get("mid_caps", []))
        _render_group("EQUITIES — SMALL/MICRO CAPS", equities.get("small_micro_caps", []))
    elif isinstance(equities, list):
        _render_group("EQUITIES", equities)

    picks = s.get("top_picks", [])
    if picks and not equities:
        eq = [p for p in picks if p.get("asset_class") in ("stock", "equities", "equity")]
        cr = [p for p in picks if p.get("asset_class") in ("crypto", "cryptocurrency")]
        co = [p for p in picks if p.get("asset_class") in ("commodity", "commodities")]
        ot = [p for p in picks if p not in eq and p not in cr and p not in co]
        _render_group("EQUITIES", eq)
        _render_group("CRYPTO", cr)
        _render_group("COMMODITIES", co)
        _render_group("OTHER", ot)

    crypto_list = s.get("crypto", [])
    if isinstance(crypto_list, list) and crypto_list:
        _render_group("CRYPTO", crypto_list)

    commodities_list = s.get("commodities", [])
    if isinstance(commodities_list, list) and commodities_list:
        _render_group("COMMODITIES", commodities_list)
    elif isinstance(commodities_list, str) and commodities_list:
        parts.append(f"--- COMMODITIES ---")
        parts.append(commodities_list)
        parts.append("")

    sts = s.get("social_trading_signal", {})
    if sts and isinstance(sts, dict) and sts.get("symbol"):
        sts_parts = []
        sym = sts.get("symbol", "?")
        classification = sts.get("classification", "WATCHLIST")
        rating = sts.get("rating", "")
        conf = sts.get("confidence", "")
        signal_header = f"SOCIAL TRADING SIGNAL — {sym} [{classification}]"
        if rating:
            signal_header += f" | {rating}"
        if conf:
            signal_header += f" | Confidence: {conf}"
        vel = sts.get("social_velocity_label", "")
        vel_score = sts.get("mention_velocity_score", 0)
        if vel:
            signal_header += f" | Velocity: {vel}"
        if vel_score:
            signal_header += f" ({vel_score})"
        sts_parts.append(signal_header)
        confs = sts.get("confirmations", {})
        if confs and isinstance(confs, dict):
            conf_strs = []
            for k in ("ta", "volume", "catalyst", "fa"):
                v = confs.get(k)
                if v is True:
                    conf_strs.append(f"{k.upper()}:Y")
                elif v is False:
                    conf_strs.append(f"{k.upper()}:N")
            if conf_strs:
                sts_parts.append(f"  Confirmations: {' | '.join(conf_strs)}")
        else:
            grid = sts.get("confirmation_grid", {})
            if grid:
                grid_parts = []
                for k, v in grid.items():
                    grid_parts.append(f"{k.upper()}: {v}")
                sts_parts.append("  " + " | ".join(grid_parts))
        bullets = sts.get("thesis_bullets", sts.get("thesis", []))
        if isinstance(bullets, list):
            for b in bullets:
                sts_parts.append(f"  • {b}")
        elif isinstance(bullets, str) and bullets:
            sts_parts.append(f"  {bullets}")
        risks = sts.get("risks", [])
        for r in risks:
            sts_parts.append(f"  ⚠ {r}")
        receipts = sts.get("receipts", [])
        for r in receipts[:2]:
            if isinstance(r, dict):
                sts_parts.append(f"  [{r.get('stance', '?')}] \"{r.get('text', '')}\"")
            elif isinstance(r, str):
                sts_parts.append(f"  \"{r}\"")
        ps = sts.get("position_size", "")
        if ps:
            sts_parts.append(f"  Position: {ps}")
        sts_parts.append("")
        parts = sts_parts + parts

    positioning = s.get("portfolio_positioning", "")
    if positioning:
        parts.append(f"POSITIONING: {positioning}")

    bias = s.get("portfolio_bias", {})
    if bias and isinstance(bias, dict):
        regime_b = bias.get("risk_regime", "")
        cash = bias.get("cash_guidance", "")
        if regime_b or cash:
            bias_parts = []
            if regime_b:
                bias_parts.append(f"Regime: {regime_b}")
            if cash:
                bias_parts.append(f"Cash: {cash}")
            parts.append("PORTFOLIO BIAS: " + " | ".join(bias_parts))

    disclaimer = s.get("disclaimer", "")
    if disclaimer:
        parts.append("")
        parts.append(disclaimer)

    return "\n".join(parts).strip()
def _render_trades_analysis(s: dict) -> str:
    parts = []
    pulse = s.get("market_pulse", {})
    if pulse:
        verdict = pulse.get("verdict", "N/A")
        summary = pulse.get("summary", "")
        parts.append(f"MARKET PULSE: {verdict}")
        if summary:
            parts.append(summary)
        parts.append("")

    def _render_trade(t):
        ticker = t.get("ticker", "?")
        name = t.get("name", "")
        direction = t.get("direction", "long").upper()
        action = t.get("action", "")
        conf = t.get("confidence_score", "")
        tech = t.get("technical_score", "")
        pattern = t.get("pattern", "")
        header = f"{ticker}"
        if name:
            header += f" ({name})"
        header += f" [{direction}]"
        if action:
            header += f" — {action}"
        detail_parts = []
        if conf:
            detail_parts.append(f"Confidence: {conf}")
        if tech:
            detail_parts.append(f"TA Score: {tech}")
        if pattern:
            detail_parts.append(f"Pattern: {pattern}")
        if detail_parts:
            header += " | " + " | ".join(detail_parts)
        parts.append(header)
        signals = t.get("signals_stacking", [])
        if signals:
            parts.append(f"  Signals: {', '.join(signals)}")
        entry = t.get("entry", "")
        stop = t.get("stop", "")
        targets = t.get("targets", [])
        rr = t.get("risk_reward", "")
        tf = t.get("timeframe", "")
        plan_parts = []
        if entry:
            plan_parts.append(f"Entry: {entry}")
        if stop:
            plan_parts.append(f"Stop: {stop}")
        if targets:
            plan_parts.append(f"Targets: {', '.join(targets)}")
        if rr:
            plan_parts.append(f"R:R {rr}")
        if tf:
            plan_parts.append(f"Timeframe: {tf}")
        if plan_parts:
            parts.append(f"  {' | '.join(plan_parts)}")
        confs = t.get("confirmations", {})
        if confs and isinstance(confs, dict):
            conf_strs = []
            for k in ("ta", "volume", "catalyst", "fa"):
                v = confs.get(k)
                if v is True:
                    conf_strs.append(f"{k.upper()}:Y")
                elif v is False:
                    conf_strs.append(f"{k.upper()}:N")
            if conf_strs:
                parts.append(f"  Confirmations: {' | '.join(conf_strs)}")
        thesis = t.get("thesis", "")
        if thesis:
            parts.append(f"  {thesis}")
        fail = t.get("why_could_fail", "")
        if fail:
            parts.append(f"  Risk: {fail}")
        tv = t.get("tv_url", "")
        if tv:
            parts.append(f"  Chart: {tv}")
        gaps = t.get("data_gaps", [])
        if gaps:
            parts.append(f"  Data gaps: {', '.join(gaps)}")
        parts.append("")

    top = s.get("top_trades", [])
    if top:
        parts.append("--- TOP TRADES ---")
        for t in top:
            _render_trade(t)

    bearish = s.get("bearish_setups", [])
    if bearish:
        parts.append("--- BEARISH SETUPS ---")
        for t in bearish:
            _render_trade(t)

    notes = s.get("notes", [])
    if notes:
        parts.append("NOTES:")
        for n in notes:
            parts.append(f"  • {n}")

    disclaimer = s.get("disclaimer", "")
    if disclaimer:
        parts.append("")
        parts.append(disclaimer)

    return "\n".join(parts).strip()


_NARRATIVE_KEYS = ("summary", "narrative", "analysis", "report", "text", "message", "observations")
def _render_screener_analysis(s: dict) -> str:
    parts = []
    screen_name = s.get("screen_name", "Screener")
    parts.append(f"**{screen_name}**")
    explain = s.get("explain", [])
    if explain:
        parts.append("**Screen Criteria:**")
        for e in explain:
            parts.append(f"- {e}")
        parts.append("")
    top_picks = s.get("top_picks", [])
    if top_picks:
        parts.append("**Top Picks:**")
        for p in top_picks:
            ticker = p.get("ticker", "?")
            conf = p.get("confidence", 0)
            reason = p.get("reason", "")
            parts.append(f"- **{ticker}** (score: {conf}) -- {reason}")
        parts.append("")
    rows = s.get("rows", [])
    if rows:
        parts.append(f"**{len(rows)} stocks qualified** from screening pipeline.")
        for r in rows[:10]:
            ticker = r.get("ticker", "?")
            price = r.get("price", "N/A")
            change = r.get("change", r.get("chg_pct", ""))
            score = r.get("composite_score", "")
            signals = ", ".join(r.get("signals", [])[:3]) if r.get("signals") else ""
            line = f"- **{ticker}** {price}"
            if change:
                line += f" ({change})"
            if score:
                line += f" | Score: {score}"
            if signals:
                line += f" | {signals}"
            parts.append(line)
    scan_stats = s.get("scan_stats", {})
    if scan_stats:
        parts.append(f"Scanned {scan_stats.get('candidates_total', '?')} candidates")
    return "\n".join(parts).strip()


_RENDERERS = {
    "cross_market": _render_cross_market_analysis,
    "trades": _render_trades_analysis,
    "screener": _render_screener_analysis,
}
def _ensure_analysis(result: dict, meta: dict = None) -> dict:
    analysis = result.get("analysis", "")
    structured = result.get("structured", {})
    if not isinstance(structured, dict):
        return result

    display_type = structured.get("display_type", "")
    req_id = (meta or {}).get("request_id", "")
    has_narrative = False

    if not analysis:
        for key in _NARRATIVE_KEYS:
            val = structured.get(key, "")
            if val and isinstance(val, str) and len(val) > 10:
                analysis = val
                has_narrative = True
                break

    if not analysis and display_type in _RENDERERS:
        analysis = _RENDERERS[display_type](structured)

    if analysis:
        result["analysis"] = analysis

    s_keys = [k for k in structured.keys() if k != "display_type"][:8]
    print(f"[RENDER] id={req_id} display_type={display_type} analysis_len={len(analysis)} has_structured_message={has_narrative} structured_keys={s_keys}")

    return result
def _ok_envelope(result: dict, meta: dict) -> dict:
    if not isinstance(result, dict):
        result = {"analysis": str(result) if result else "", "structured": {}}
    result.setdefault("analysis", "")
    result.setdefault("structured", {})
    result = _ensure_analysis(result, meta)
    result["type"] = "ok"
    # Surface display_type at top level — frontend checks data.display_type
    # for history categorization (e.g. saveToPromptHistory)
    structured = result.get("structured", {})
    if isinstance(structured, dict) and structured.get("display_type"):
        result["display_type"] = structured["display_type"]
    result["meta"] = meta
    result["error"] = None
    result["conversation_id"] = meta.get("conversation_id")
    result["request_id"] = meta.get("request_id")
    result["as_of"] = _dt.now(_tz.utc).isoformat()
    return result
def _error_envelope(code: str, message: str, meta: dict, details=None, partial=None) -> dict:
    env = {
        "type": "error",
        "analysis": "",
        "structured": partial or {},
        "meta": meta,
        "error": {
            "code": code,
            "message": message,
            "details": details or {},
        },
        "conversation_id": meta.get("conversation_id"),
        "request_id": meta.get("request_id"),
        "as_of": _dt.now(_tz.utc).isoformat(),
    }
    return env
def _resp_log(req_id: str, status: int, resp_type: str, resp: dict):
    resp_bytes = len(_json.dumps(resp, default=str).encode("utf-8"))
    print(f"[RESP] id={req_id} status={status} type={resp_type} bytes={resp_bytes}")


@app.post("/api/social/query")
@limiter.limit("10/minute")
async def social_grok_query(
    request: Request,
    body: dict = Body(...),
    _sub: None = Depends(require_subscription),
):
    """Direct Grok/X query for the Social page — real-time X search via xAI."""
    query = body.get("query", "")
    preset_intent = body.get("preset_intent", "")

    await _wait_for_init()
    if not data_service or not data_service.xai:
        return JSONResponse(status_code=503, content={"error": "xAI sentiment provider not initialized"})

    # ── Select Trader Consensus preset ──────────────────────────────────
    # Detect by preset_intent OR by query text matching the button label.
    _SELECT_TRIGGERS = {"x_select_trader_consensus", "select_traders",
                        "select_trader_consensus", "curated_traders", "x_select_consensus"}
    _SELECT_QUERY_HINTS = ["select x traders", "select traders", "concensus tickers among select",
                           "consensus tickers among select"]
    is_select_consensus = (
        preset_intent in _SELECT_TRIGGERS
        or any(hint in (query or "").lower() for hint in _SELECT_QUERY_HINTS)
    )

    if is_select_consensus:
        from agent.prompts import X_SELECT_TRADER_CONSENSUS_CONTRACT
        from services.x_consensus_cache import (
            X_SELECT_HANDLES as _X_SELECT_HANDLES,
            _load_disk_cache as _xc_load_cache,
            _is_fresh as _xc_is_fresh,
        )
        import time as _time_mod

        # ── Serve from cache whenever possible ──────────────────────────
        # The scheduled daily refresh (noon Chicago) already runs the full
        # 3-call pipeline and persists to disk.  Re-running it on every
        # button press was the single biggest cost driver (~$3/day alone).
        # Only fall through to live calls if the cache is genuinely stale
        # OR if the caller explicitly passes force_refresh=true.
        _force_refresh = bool(body.get("force_refresh", False))
        _cached_snap = _xc_load_cache()
        if not _force_refresh and _xc_is_fresh(_cached_snap) and _cached_snap.get("raw"):
            _age_min = round((_time_mod.time() - (_cached_snap.get("_saved_at") or 0)) / 60, 1)
            print(f"[SOCIAL_GROK] Returning cached consensus (age={_age_min}min) — no Grok calls fired")
            return JSONResponse(content={
                "response":      _cached_snap["raw"],
                "query":         query or "Consensus tickers among select X traders",
                "structured":    True,
                "preset":        "x_select_trader_consensus",
                "from_cache":    True,
                "cache_age_min": _age_min,
            })

        print(f"[SOCIAL_GROK] Cache stale or force_refresh — firing live Grok calls")

        # ── Phase 1: Parallel batched x_search (max ~8 handles per call) ──
        # Grok's allowed_x_handles supports ~10 per call; we batch into groups
        # using the fast non-reasoning model for data collection.
        BATCH_SIZE = 8
        batches = [_X_SELECT_HANDLES[i:i + BATCH_SIZE]
                    for i in range(0, len(_X_SELECT_HANDLES), BATCH_SIZE)]
        print(f"[SOCIAL_GROK] Select trader consensus — {len(_X_SELECT_HANDLES)} handles in {len(batches)} batches")

        async def _fetch_batch(handles: list[str], batch_num: int) -> str:
            """Fetch raw post data for a batch of handles."""
            import datetime as _dt
            _since = (_dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(days=7)).strftime("%Y-%m-%d")
            batch_prompt = (
                f"Search X/Twitter posts from the last 7 days from EACH of these accounts: "
                + ", ".join(f"@{h}" for h in handles)
                + ". For each account, list the tickers/assets they mention with bullish/bearish context, "
                "their thesis, conviction level, and any catalysts they cite. "
                "Include the account handle with each finding. Be thorough and specific — "
                "quote or closely paraphrase their actual posts."
            )
            result = await data_service.xai._call_grok_with_x_search(
                prompt=batch_prompt,
                raw_mode=True,
                use_deep_model=False,
                timeout=60.0,
                x_search_config={"allowed_x_handles": handles, "from_date": _since},
            )
            text = ""
            if isinstance(result, dict):
                text = result.get("_raw_analysis", "") or result.get("error", "")
            print(f"[SOCIAL_GROK] Batch {batch_num + 1}/{len(batches)}: {len(handles)} handles -> {len(text)} chars")
            return text

        try:
            # Run all batches in parallel
            batch_results = await asyncio.gather(
                *[_fetch_batch(batch, i) for i, batch in enumerate(batches)],
                return_exceptions=True,
            )
            # Combine results, skip failures
            combined_data = []
            for i, res in enumerate(batch_results):
                if isinstance(res, Exception):
                    print(f"[SOCIAL_GROK] Batch {i + 1} failed: {res}")
                    continue
                if res and not res.startswith("xAI"):
                    combined_data.append(f"=== Batch {i + 1} ({', '.join('@' + h for h in batches[i])}) ===\n{res}")

            if not combined_data:
                return JSONResponse(status_code=502, content={
                    "error": "All x_search batches failed — xAI may be experiencing issues",
                    "query": query,
                })

            # ── Phase 2: Synthesize with reasoning model ──────────────────
            combined_text = "\n\n".join(combined_data)
            print(f"[SOCIAL_GROK] Synthesis phase: {len(combined_text):,} chars from {len(combined_data)} batches")

            synthesis_prompt = (
                f"Below is raw data from X/Twitter posts (last 7 days) by {len(_X_SELECT_HANDLES)} select trader accounts. "
                "Analyze ALL of this data and produce the consensus JSON output per your schema.\n\n"
                "RAW X DATA:\n" + combined_text + "\n\n"
                "Now synthesize this into the exact JSON schema from your system instructions. "
                "Return ONLY valid JSON — no markdown, no backticks, no extra text."
            )

            result = await data_service.xai._call_grok_with_x_search(
                prompt=synthesis_prompt,
                raw_mode=False,
                use_deep_model=True,
                timeout=90.0,
                system_text=X_SELECT_TRADER_CONSENSUS_CONTRACT,
                max_output_tokens=4000,
            )
            if isinstance(result, dict) and not result.get("error"):
                return JSONResponse(content={
                    "response": result,
                    "query": query or "Consensus tickers among select X traders",
                    "structured": True,
                    "preset": "x_select_trader_consensus",
                })
            else:
                err = result.get("error", "unknown") if isinstance(result, dict) else str(result)
                print(f"[SOCIAL_GROK] Synthesis error: {err}")
                return JSONResponse(status_code=502, content={"error": err, "query": query})
        except Exception as e:
            print(f"[SOCIAL_GROK] Select consensus exception: {e}")
            return JSONResponse(status_code=500, content={"error": str(e), "query": query})

    # ── Generic social query (free-form) ────────────────────────────────
    if not query.strip():
        return JSONResponse(status_code=400, content={"error": "No query provided"})

    system_prompt = (
        "You are a financial social media analyst with real-time access to X/Twitter. "
        "Search X thoroughly for the user's query. Always include: specific @usernames "
        "and their posts, engagement metrics when available, overall sentiment scoring "
        "(Bullish/Bearish/Neutral with a 1-10 confidence score), and specific ticker "
        "mentions with context. Format your response clearly with sections. Be specific "
        "— cite actual posts and accounts, don't give vague summaries."
    )

    try:
        # Build combined prompt with system instructions
        full_prompt = f"{system_prompt}\n\nUser query: {query}"
        result = await data_service.xai._call_grok_with_x_search(
            prompt=full_prompt,
            raw_mode=True,
            timeout=45.0,
        )

        if isinstance(result, dict) and result.get("_raw_analysis"):
            return JSONResponse(content={"response": result["_raw_analysis"], "query": query})
        elif isinstance(result, dict) and result.get("error"):
            return JSONResponse(status_code=502, content={"error": result["error"], "query": query})
        else:
            return JSONResponse(content={"response": str(result), "query": query})

    except Exception as e:
        print(f"[SOCIAL_GROK] Error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e), "query": query})


@app.get("/api/social/x-dashboard")
@limiter.limit("60/minute")
async def social_x_dashboard(
    request: Request,
    _sub: None = Depends(require_subscription),
):
    """
    Social page X-dashboard — 4 sections derived from the shared X consensus snapshot.

    Zero additional Grok/XAI calls on every page load.  All sections are derived
    from the same cached snapshot that powers Home 'Trending on X'.

    Sections returned:
      x_consensus          — from raw.consensus_picks
      freshest_alpha       — from raw.fresh_trades + raw.spotlight.freshest_alpha
      theme_leadership     — from raw.hype_radar + raw.market_pulse
      sentiment_acceleration — deterministic delta between current and prior snapshots

    Also returns: market_pulse, portfolio_bias, spotlight, metadata.

    Additive (non-breaking) keys appended on top of the above, derived from the
    SAME existing Social/X/Grok snapshot — no second Grok/XAI call is made:
      social_screener      — per-ticker rows w/ consensus / freshness /
                             social-acceleration scores, enriched from FMP
                             (company name, market cap, volume, price changes).
      fundamental_screener — per-ticker fundamentals from FMP (TTM ratios +
                             latest annual income/balance/cashflow).  Capped to
                             top 50 tickers by social-acceleration/consensus
                             score.  Missing per-symbol data returns null +
                             data_quality="partial|missing" rather than 500.
    """
    import time as _time
    try:
        from services.social_x_service import build_x_dashboard
        import services.social_screener_service as _sss
        from services.social_screener_service import (
            _is_us_market_open, _run_screeners_bg,
        )
        from services.x_consensus_cache import _load_disk_cache
        from data.cache import cache as _ss_cache
        from config import FMP_API_KEY
        from datetime import datetime as _dt, timezone as _tz

        _t0 = _time.monotonic()

        # ── 1. XAI snapshot (mtime hot-cached — no JSON re-parse if unchanged) ──
        snapshot = _load_disk_cache()

        # ── 2. Dashboard sections (sections cache — skip classifier on same snap) ──
        payload = build_x_dashboard()

        # ── 3. Screener: cache-first, NEVER await in request thread ───────────────
        # Fast path:  both compiled caches warm → serve from cache, zero network calls.
        # Cold path:  fire asyncio.create_task(build_screeners(...)) so Tradier/FMP
        #             batch runs in the background; return "warming" status immediately.
        # build_screeners() never raises — all exceptions produce empty-screener dicts.
        _cached_ss = _ss_cache.get("social_screener:social_payload")
        _cached_fs = _ss_cache.get("social_screener:fs_payload")
        _ss_warm   = bool(_cached_ss and isinstance(_cached_ss, dict) and _cached_ss.get("rows"))
        _fs_warm   = bool(_cached_fs and isinstance(_cached_fs, dict) and _cached_fs.get("rows"))
        _bg_started = False
        _mkt = _is_us_market_open()
        _now_str = _dt.now(_tz.utc).isoformat()

        def _empty_ss_payload(status: str) -> dict:
            return {
                "generated_at": _now_str,
                "source":       "cache",
                "rows":         [],
                "meta": {
                    "xai_call_added":    False,
                    "ticker_count":      0,
                    "enrichment_status": status,
                    "market_hours_open": _mkt,
                },
            }

        def _empty_fs_payload(status: str) -> dict:
            return {
                "generated_at": _now_str,
                "source":       "fmp_enrichment",
                "rows":         [],
                "meta": {
                    "ticker_count":      0,
                    "cache_status":      status,
                    "market_hours_open": _mkt,
                },
            }

        if _ss_warm and _fs_warm:
            # Both caches warm — serve immediately with zero network calls
            payload["social_screener"]      = _cached_ss
            payload["fundamental_screener"] = _cached_fs
            print(
                f"[SOCIAL_X_DASHBOARD] screener CACHE HIT — "
                f"social={len((_cached_ss or {}).get('rows', []))} "
                f"fund={len((_cached_fs or {}).get('rows', []))} rows"
            )
        else:
            # At least one cache cold — return warming status, fire background build.
            # Single-flight: set the flag synchronously before create_task so that
            # any concurrent requests arriving before the first await all see True
            # and skip creating a duplicate task.  asyncio is single-threaded so
            # this test-and-set is race-free.
            payload["social_screener"]      = _cached_ss if _ss_warm else _empty_ss_payload("warming")
            payload["fundamental_screener"] = _cached_fs if _fs_warm else _empty_fs_payload("warming")
            if _sss._screener_warmup_running:
                print(
                    f"[SOCIAL_X_DASHBOARD] background_refresh_already_running=true "
                    f"(ss_warm={_ss_warm} fs_warm={_fs_warm}) — skipping duplicate task"
                )
            else:
                _sss._screener_warmup_running = True   # set before create_task
                asyncio.create_task(_run_screeners_bg(
                    snapshot=snapshot,
                    x_consensus_rows=payload.get("x_consensus") or [],
                    sentiment_accel_rows=payload.get("sentiment_acceleration") or [],
                    freshest_alpha=payload.get("freshest_alpha") or {},
                    theme_leadership=payload.get("theme_leadership") or {},
                    fmp_api_key=FMP_API_KEY,
                ))
                _bg_started = True
                print(
                    f"[SOCIAL_X_DASHBOARD] screener cache cold "
                    f"(ss_warm={_ss_warm} fs_warm={_fs_warm}) — background warmup fired"
                )

        # ── 4. Response metadata ──────────────────────────────────────────────────
        _total_ms = int((_time.monotonic() - _t0) * 1000)
        payload["cache_hit"]                  = _ss_warm and _fs_warm
        payload["cache_source"]               = (
            "screener_cache" if (_ss_warm and _fs_warm) else
            "social_cache"   if _ss_warm                else
            "cold"
        )
        payload["cache_age_seconds"]          = (payload.get("metadata") or {}).get("age_seconds")
        payload["xai_called"]                 = False
        payload["screener_awaited"]           = False
        payload["background_refresh_started"] = _bg_started
        payload["total_ms"]                   = _total_ms

        return JSONResponse(content=payload)
    except Exception as e:
        print(f"[SOCIAL_X_DASHBOARD] Error: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "endpoint": "/api/social/x-dashboard"},
        )


@app.get("/api/social/fundamental-screener")
@limiter.limit("30/minute")
async def social_fundamental_screener(
    request: Request,
    _sub: None = Depends(require_subscription),
):
    """
    Lazy fundamental screener — reuses the most recent cached Social ticker
    universe and enriches via FMP.  No additional Grok/XAI call is made.

    Missing per-symbol data returns null + data_quality="partial|missing"
    rather than 500ing the endpoint.
    """
    try:
        from services.social_x_service import build_x_dashboard
        from services.social_screener_service import (
            build_screeners, _is_us_market_open,
        )
        from services.x_consensus_cache import _load_disk_cache
        from data.cache import cache
        from config import FMP_API_KEY

        # ── Fast path: serve from 7-day fundamental cache ─────────────────────
        # This endpoint is called frequently; avoid running live social enrichment
        # when the fundamental payload is still within its 7-day TTL.
        _cached_fs = cache.get("social_screener:fs_payload")
        if _cached_fs and isinstance(_cached_fs, dict) and _cached_fs.get("rows"):
            mkt_open = _is_us_market_open()
            print(
                f"[SOCIAL_FUND_SCREENER] served from 7-day cache "
                f"rows={len(_cached_fs.get('rows', []))}"
            )
            return JSONResponse(content={
                **_cached_fs,
                "meta": {**(_cached_fs.get("meta") or {}), "market_hours_open": mkt_open},
            })

        # ── Cache expired or cold: run full inline fund enrichment ─────────────
        payload   = build_x_dashboard()
        snapshot  = _load_disk_cache()
        _social, fundamental_screener = await build_screeners(
            snapshot=snapshot,
            x_consensus_rows=payload.get("x_consensus") or [],
            sentiment_accel_rows=payload.get("sentiment_acceleration") or [],
            freshest_alpha=payload.get("freshest_alpha") or {},
            theme_leadership=payload.get("theme_leadership") or {},
            fmp_api_key=FMP_API_KEY,
        )
        return JSONResponse(content=fundamental_screener)
    except Exception as e:
        print(f"[SOCIAL_FUND_SCREENER] Error: {e}")
        from datetime import datetime as _dt, timezone as _tz
        return JSONResponse(
            content={
                "generated_at": _dt.now(_tz.utc).isoformat(),
                "source":       "fmp_enrichment",
                "rows":         [],
                "meta": {
                    "ticker_count": 0,
                    "cache_status": "unavailable",
                },
                "error": str(e),
            },
        )


@app.get("/api/social/diagnostics")
@limiter.limit("30/minute")
async def social_diagnostics(
    request: Request,
    _sub: None = Depends(require_subscription),
):
    """
    Social page scan diagnostics — last N scan entries showing per-section
    validation results, LKG-merge activity, ticker counts, and any errors.

    Returns:
      diagnostics   — list of scan entries, newest last
      summary       — quick health snapshot derived from the latest entry
      cache_info    — current disk-cache state (age, staleness, section counts)
    """
    try:
        from services.x_consensus_cache import (
            load_scan_diagnostics,
            _load_disk_cache,
            _is_fresh,
        )
        import time as _time

        entries = load_scan_diagnostics()
        latest  = entries[-1] if entries else {}

        # Summary from the most recent scan
        summary: dict = {}
        if latest:
            summary = {
                "last_scan_at":       latest.get("scan_ts"),
                "sections_ok":        latest.get("sections_ok", []),
                "sections_missing":   latest.get("sections_missing", []),
                "lkg_sections_used":  latest.get("lkg_sections_used", []),
                "ticker_count":       latest.get("ticker_count", 0),
                "mention_records":    latest.get("mention_records", 0),
                "cache_write_status": latest.get("cache_write_status"),
                "health": (
                    "degraded" if latest.get("sections_missing")
                    else "ok"
                ),
            }

        # Current disk-cache state
        raw = _load_disk_cache()
        cache_info: dict = {"available": bool(raw)}
        if raw:
            age_s = int(_time.time() - float(raw.get("_saved_at") or 0))
            cache_info.update({
                "generated_at":    raw.get("generated_at"),
                "age_seconds":     age_s,
                "is_fresh":        _is_fresh(raw),
                "top_tickers":     len(raw.get("top_tickers") or []),
                "backend_ranked":  len(raw.get("_backend_ranked") or []),
                "mention_records": len(raw.get("_mention_data") or []),
                "consensus_picks": len((raw.get("raw") or {}).get("consensus_picks") or []),
                "lkg_sections":    raw.get("_lkg_sections_used") or [],
            })

        # Screener compiled-cache state
        from data.cache import cache as _diag_cache
        _ss = _diag_cache.get("social_screener:social_payload")
        _fs = _diag_cache.get("social_screener:fs_payload")
        _ss_rows = _ss.get("rows", []) if isinstance(_ss, dict) else []
        _fs_rows = _fs.get("rows", []) if isinstance(_fs, dict) else []
        screener_info: dict = {
            "social_payload_warm":      bool(_ss),
            "social_payload_rows":      len(_ss_rows),
            "social_payload_updated_at": _ss.get("generated_at") if isinstance(_ss, dict) else None,
            "first_5_tickers":          [
                r.get("symbol") or r.get("ticker")
                for r in _ss_rows[:5] if isinstance(r, dict)
            ],
            "empty_reason": (
                "social_payload cache missing — rebuild needed"
                if not _ss else
                "social_payload has 0 rows — check enrichment logs"
                if not _ss_rows else
                None
            ),
            "fs_payload_warm":          bool(_fs),
            "fs_payload_rows":          len(_fs_rows),
            "fs_payload_updated_at":    _fs.get("generated_at") if isinstance(_fs, dict) else None,
        }

        # Ask Livermore signal status
        _al = (raw or {}).get("ask_livermore_signal") if raw else None
        ask_livermore_info: dict = {
            "exists":          isinstance(_al, dict),
            "stance":          _al.get("stance")        if isinstance(_al, dict) else None,
            "confidence":      _al.get("confidence")    if isinstance(_al, dict) else None,
            "signal_label":    _al.get("signal_label")  if isinstance(_al, dict) else None,
            "updated_at":      _al.get("updated_at")    if isinstance(_al, dict) else None,
            "stale":           _al.get("stale", True)   if isinstance(_al, dict) else True,
            "fallback_reason": _al.get("fallback_reason") if isinstance(_al, dict) else "no_snapshot",
            "summary_snippet": (
                (_al.get("summary") or "")[:160]
                if isinstance(_al, dict) else None
            ),
        }

        return JSONResponse(content={
            "diagnostics":         entries,
            "summary":             summary,
            "cache_info":          cache_info,
            "screener_info":       screener_info,
            "ask_livermore_info":  ask_livermore_info,
        })
    except Exception as exc:
        print(f"[SOCIAL_DIAGNOSTICS] Error: {exc}")
        return JSONResponse(
            status_code=500,
            content={"error": str(exc), "endpoint": "/api/social/diagnostics"},
        )


@app.post("/api/social/x-dashboard/refresh")
@limiter.limit("4/minute")
async def social_x_dashboard_refresh(
    request: Request,
    _sub: None = Depends(require_subscription),
):
    """
    Manual user-initiated refresh of the X consensus snapshot.

    Bypasses the automatic 08:00–20:00 America/Chicago quiet-hours gate so a
    user can trigger a one-off refresh from the Social page at any time of day.

    Guardrails (applied even outside the window):
      1. Single-flight: if a refresh is already running the request is rejected
         immediately with reason="refresh_already_running".
      2. 30-minute cooldown: the per-process _last_manual_refresh_at timestamp
         prevents back-to-back overnight Grok calls.  Rejected with
         reason="cooldown" and next_manual_refresh_allowed_at set.
      3. Rate limiter: 4 requests per minute at the HTTP layer (belt-and-
         suspenders against any client loop bugs).

    The refresh updates the SAME shared x_consensus snapshot used by both
    the Social dashboard (GET /api/social/x-dashboard) and the Home page
    "Trending on X" widget — no new snapshot family is created.

    Response fields:
      accepted                    bool
      refresh_in_progress         bool
      last_updated_at             str | null  (ISO-8601 UTC — snapshot timestamp)
      next_manual_refresh_allowed_at  str | null  (ISO-8601 UTC)
      manual_refresh_available    bool
      reason                      str | null  (present when not accepted)
    """
    try:
        from services.x_consensus_cache import trigger_manual_refresh
        result = await trigger_manual_refresh(data_service)
        status = 202 if result.get("accepted") else 429
        return JSONResponse(status_code=status, content=result)
    except Exception as e:
        print(f"[SOCIAL_X_DASHBOARD_REFRESH] Error: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "endpoint": "/api/social/x-dashboard/refresh"},
        )


@app.post("/api/query")
@limiter.limit("10/minute")
async def query_agent(
    request: Request,
    body: QueryRequest,
    api_key: str = Header(None, alias="X-API-Key"),
    _sub: None = Depends(require_subscription),
):
    import asyncio
    import time as _time
    t0 = _time.time()
    req_id = str(_uuid.uuid4())
    user_query = body.query or body.prompt or ""
    print(f"[REQ] id={req_id} query_len={len(user_query)} preset={body.preset_intent} conversation_id={body.conversation_id} csv_data={'YES (' + str(len(body.csv_data)) + ' chars)' if body.csv_data else 'NO'}")

    meta = _build_meta(req_id, preset_intent=body.preset_intent, conv_id=body.conversation_id, reasoning_model=body.reasoning_model)

    if not _jwt_or_key(request, api_key):
        resp = _error_envelope("AUTH_FAILED", "Invalid or missing API key.", meta)
        _resp_log(req_id, 403, "error", resp)
        return JSONResponse(status_code=403, content=resp)

    try:
        await _wait_for_init()
    except HTTPException:
        resp = _error_envelope("SERVER_STARTING", "Server is still starting up. Please try again in a moment.", meta)
        _resp_log(req_id, 503, "error", resp)
        return JSONResponse(status_code=503, content=resp)

    if not user_query.strip() and not body.preset_intent and not body.csv_data:
        resp = _error_envelope("NO_QUERY", "No query provided. Send query or use preset_intent.", meta)
        _resp_log(req_id, 400, "error", resp)
        return JSONResponse(status_code=400, content=resp)

    # If CSV data present but no query, provide a default analysis prompt
    if body.csv_data and not user_query.strip():
        user_query = "Analyze every ticker in this uploaded CSV watchlist. For each stock, provide deep analysis including investment thesis, valuation vs peers, upcoming catalysts, competitive moat, and sentiment. Categorize into: top buys now, most undervalued, best catalysts, hidden gems, most revolutionary, and right sector right time. Include an avoid list for overvalued or deteriorating names."

    # ── Phase 0: normalize inbound mode/reasoning_model + collab_preset ────
    # Accepts "caelyn", "customize", legacy aliases, and existing identifiers.
    # All downstream code continues to use internal identifiers unchanged.
    body.reasoning_model = normalize_reasoning_model(body.reasoning_model)
    body.collab_preset   = normalize_collab_preset(body.collab_preset)

    # ── Bridge: collaboration_mode → collab_preset ───────────────────────
    # Current frontend sends collaboration_mode (e.g. "default", "auto", "full_collab",
    # "custom_collab") alongside reasoning_model=<family_alias> (e.g. "claude", "grok").
    # If collab_preset was not explicitly set, derive it from collaboration_mode.
    # collab_preset wins when both are present.
    # Special case: collaboration_mode="auto" + no collab_agents + solo family = Solo mode.
    #   Top-row solo selections send collaboration_mode="auto" as a side-effect of the
    #   frontend state machine; we must NOT promote them to Auto (dynamic routing) preset.
    if body.collab_preset is None and body.collaboration_mode:
        _cm_mapped = normalize_collab_preset(body.collaboration_mode)
        _is_solo_family = body.reasoning_model not in ("agent_collab", "all_agents")
        _no_collabs     = not body.collab_agents
        if _cm_mapped == COLLAB_PRESET_AUTO and _is_solo_family and _no_collabs:
            # Solo selection with collaboration_mode="auto" — keep as solo, do not promote.
            pass
        else:
            body.collab_preset = _cm_mapped

    # Structured log so every request shows its exact preset semantics — Default vs Auto must be unmistakable.
    _cp_label = collab_preset_display_label(body.collab_preset) if body.collab_preset else "none[legacy_solo_inference]"
    _auto_routing = (
        body.collab_preset == COLLAB_PRESET_AUTO or
        (not body.collab_preset and body.reasoning_model == "agent_collab" and not body.collab_agents)
    )
    print(
        f"[PRESET_SEMANTICS] id={req_id} "
        f"collab_preset={body.collab_preset!r} ({_cp_label}) "
        f"collaboration_mode={body.collaboration_mode!r} "
        f"reasoning_model={body.reasoning_model!r} "
        f"collab_agents={body.collab_agents or 'none'} "
        f"primary_model={body.primary_model or 'none'} "
        f"auto_routing_allowed={'YES' if _auto_routing else 'NO'}"
    )

    from data.chat_history import create_conversation, get_conversation, append_message as _append_msg

    conv_id = body.conversation_id
    history = []

    if conv_id:
        conv = get_conversation(conv_id)
        if conv and conv.get("messages"):
            history = conv["messages"]
        elif conv is None:
            print(f"[API] Conversation {conv_id} not found, creating new one")
            conv_id = None

    # If client explicitly sent history WITH messages, prefer it — handles model switches
    # and message deletions.  An empty list means the client has no history loaded,
    # so fall back to the DB history to preserve follow-up context.
    if body.history is not None and len(body.history) > 0:
        print(f"[API] Using client-provided history ({len(body.history)} msgs) over DB history ({len(history)} msgs)")
        history = body.history

    if not conv_id:
        try:
            conv = create_conversation(user_query)
            conv_id = conv["id"]
        except Exception as e:
            print(f"[API] Failed to create conversation: {e}")
            conv_id = None

    meta["conversation_id"] = conv_id

    print(f"[API] request_id={req_id} query={user_query[:100]}, history_turns={len(history)}, conv_id={conv_id}")

    if conv_id and user_query.strip():
        try:
            _append_msg(
                conv_id,
                "user",
                user_query,
                message_type="preset" if body.preset_intent else "chat",
                preset_key=body.preset_intent,
                model_used=body.reasoning_model or "agent_collab",
            )
            conv_now = get_conversation(conv_id)
            history = conv_now.get("messages", []) if conv_now else history
        except Exception as e:
            print(f"[API] Failed to persist user message: {e}")

    _user_id = getattr(request.state, "user_id", "default")

    async def _stream_query():
        """
        Runs the query and streams keepalive spaces every 8s.
        Prevents Replit proxy from killing connections on slow queries (Grok, investments).
        Frontend strips leading whitespace before JSON.parse() — no frontend logic change needed.
        Final payload is always a single valid JSON object.
        """
        import json as _j
        import traceback as _tb

        print(f"[STREAM] Starting _stream_query for req_id={req_id}")
        if agent is None:
            raise RuntimeError("Agent not initialized — server startup may have failed. Check [INIT] logs.")
        _sc = body.screen_context or {}
        _sc_page = _sc.get("page") or _sc.get("route") or ""
        _sc_symbols = _sc.get("visible_symbols") or []
        _sc_rows = _sc.get("visible_rows_count") or len(_sc.get("visible_rows") or [])
        if _sc:
            print(
                f"[SCREEN_CTX] page={_sc_page!r} | tab={_sc.get('tab', '')!r} | "
                f"symbols={len(_sc_symbols)} | visible_rows={_sc_rows}"
            )
        task = asyncio.create_task(
            agent.handle_query(
                user_query,
                history=history,
                preset_intent=body.preset_intent,
                request_id=req_id,
                csv_data=body.csv_data,
                chatbox_mode=body.chatbox_mode or False,
                reasoning_model=body.reasoning_model or "agent_collab",
                collab_agents=body.collab_agents,
                primary_model=body.primary_model,
                user_id=_user_id,
                reasoning_mode=body.reasoning_mode,
                collab_preset=body.collab_preset,
                screen_context=body.screen_context,
            )
        )

        result = None
        timed_out = False
        _task_error = None

        for _ in range(22):  # max 22 * 8s = 176s
            try:
                result = await asyncio.wait_for(asyncio.shield(task), timeout=8.0)
                break
            except asyncio.TimeoutError:
                yield b" "  # keepalive — proxy sees bytes, stays alive
            except Exception as _exc:
                # handle_query raised a non-timeout exception — capture it
                # so we can return a proper JSON error instead of an empty body
                _task_error = _exc
                break
        else:
            task.cancel()
            timed_out = True

        meta["timing_ms"]["total"] = int((_time.time() - t0) * 1000)

        if timed_out:
            resp = _error_envelope("REQUEST_TIMEOUT", "Request timed out after 176s — please try again.", meta)
            _resp_log(req_id, 200, "timeout", resp)
            yield _j.dumps(resp).encode()
            return

        if _task_error:
            import traceback
            print(f"[API] request_id={req_id} status=agent_error error={_task_error}")
            traceback.print_exc()
            resp = _error_envelope(
                "AGENT_ERROR",
                f"Something went wrong during analysis: {str(_task_error)}",
                meta,
            )
            _resp_log(req_id, 500, "error", resp)
            yield _j.dumps(resp).encode()
            return

        try:
            timing_meta = None
            if isinstance(result, dict) and result.get("_timing"):
                timing_meta = result.pop("_timing")
            if isinstance(result, dict) and result.get("_routing"):
                meta["routing"] = result.pop("_routing")
            if isinstance(result, dict) and result.get("_cross_asset_debug"):
                meta["cross_asset_debug"] = result.pop("_cross_asset_debug")
            if timing_meta:
                meta["timing_ms"] = timing_meta

            def _is_truly_empty(r):
                if not r:
                    return True
                if not isinstance(r, dict):
                    return True
                if r.get("type") == "error":
                    return False
                structured = r.get("structured", {})
                if not isinstance(structured, dict) or not structured:
                    analysis = r.get("analysis", "")
                    return not analysis or len(str(analysis).strip()) == 0
                meaningful_keys = {"message", "summary", "picks", "conviction_picks",
                                   "recommendations", "tickers", "sectors", "results",
                                   "analysis_text", "briefing", "holdings", "top_picks",
                                   "opportunities", "ranked_candidates", "watchlist",
                                   "equities", "crypto", "commodities", "social_trading_signal",
                                   "rows", "screen_name",
                                   "top_trades", "bearish_setups"}
                has_content = any(structured.get(k) for k in meaningful_keys)
                if has_content:
                    return False
                non_meta = {k: v for k, v in structured.items()
                            if k not in {"display_type", "type", "scan_type"} and v}
                return len(non_meta) == 0

            if isinstance(result, dict) and result.get("_parse_error"):
                parse_err = result.pop("_parse_error")
                meta["timing_ms"]["total"] = int((_time.time() - t0) * 1000)
                resp = _error_envelope(
                    "CLAUDE_JSON_PARSE_FAIL",
                    "Claude returned a response that could not be parsed as structured JSON.",
                    meta,
                    details={"preview": parse_err.get("preview", "")[:800]},
                )
                _resp_log(req_id, 200, "error", resp)
                if conv_id:
                    try:
                        _asst_content = resp.get("analysis", "") or _json.dumps(resp, default=str)[:8000]
                        _append_msg(conv_id, "assistant", _asst_content, message_type="error", structured_payload=resp, preset_key=body.preset_intent, model_used=body.reasoning_model or "agent_collab")
                    except Exception:
                        pass
                yield _j.dumps(resp).encode()
                return

            if _is_truly_empty(result):
                print(f"[API] WARNING: Empty/blank result returned for query: {user_query[:80]}")
                meta["timing_ms"]["total"] = int((_time.time() - t0) * 1000)
                resp = _error_envelope(
                    "EMPTY_RESPONSE",
                    "The analysis returned empty. This usually means data sources were rate-limited. Please wait a minute and try again.",
                    meta,
                )
                _resp_log(req_id, 200, "error", resp)
                if conv_id:
                    try:
                        _asst_content2 = resp.get("analysis", "") or _json.dumps(resp, default=str)[:8000]
                        _append_msg(conv_id, "assistant", _asst_content2, message_type="error", structured_payload=resp, preset_key=body.preset_intent, model_used=body.reasoning_model or "agent_collab")
                    except Exception:
                        pass
                yield _j.dumps(resp).encode()
                return

            if conv_id:
                try:
                    _asst_content3 = result.get("analysis", "") if isinstance(result, dict) else ""
                    if not _asst_content3:
                        _asst_content3 = _json.dumps(result, default=str)[:8000]
                    _append_msg(
                        conv_id,
                        "assistant",
                        _asst_content3,
                        message_type="preset" if body.preset_intent else "chat",
                        structured_payload=result if isinstance(result, dict) else None,
                        preset_key=body.preset_intent,
                        model_used=body.reasoning_model or "agent_collab",
                    )
                except Exception as e:
                    print(f"[API] Failed to save conversation: {e}")

            meta["timing_ms"]["total"] = int((_time.time() - t0) * 1000)
            resp = _ok_envelope(result, meta)
            _resp_log(req_id, 200, "ok", resp)

            # Auto-save to prompt history for the History page
            try:
                from data.prompt_history import save_response as _save_prompt_history, extract_tickers_from_structured
                _hist_user_id = getattr(request.state, "user_id", "default")
                _hist_category = ""
                _hist_intent = ""
                _hist_display_type = ""
                _hist_model = body.reasoning_model or "agent_collab"

                # Determine category and intent from the response or preset
                _structured_data = {}
                if isinstance(result, dict):
                    _s = result.get("structured", {})
                    if isinstance(_s, dict):
                        _structured_data = _s
                        _hist_display_type = _s.get("display_type", "")
                        # Normalize legacy/new chatbox naming so frontend history
                        # grouping stays consistent (free-form chat should be "chat").
                        if _hist_display_type == "chatbox":
                            _hist_display_type = "chat"
                        _hist_category = _s.get("scan_type", "") or _hist_display_type

                # Map preset_intent to history category
                _PRESET_TO_HISTORY = {
                    "daily_briefing": ("daily_briefing", "briefing"),
                    "morning_briefing": ("daily_briefing", "briefing"),
                    "briefing": ("daily_briefing", "briefing"),
                    "macro": ("macro", "overview"),
                    "macro_outlook": ("macro", "overview"),
                    "news_intelligence": ("headlines", "news"),
                    "headlines": ("headlines", "news"),
                    "earnings_catalyst": ("upcoming_catalysts", "catalysts"),
                    "cross_asset_trending": ("trending_now", "trending"),
                    "social_momentum": ("social_momentum", "social"),
                    "social_momentum_scan": ("social_momentum", "social"),
                    "sector_rotation": ("sector_rotation", "rotation"),
                    "best_trades": ("best_trades", "trades"),
                    "investments": ("investments", "ideas"),
                    "prediction_markets": ("prediction_markets", "predictions"),
                    "ticker_analysis": ("ticker_analysis", "analysis"),
                    "portfolio_review": ("portfolio_review", "review"),
                    "crypto": ("crypto", "scan"),
                    "x_select_trader_consensus": ("x_trader_consensus", "briefing"),
                    "select_trader_consensus": ("x_trader_consensus", "briefing"),
                }
                _preset = body.preset_intent or ""
                if _preset and _preset in _PRESET_TO_HISTORY:
                    _hist_category, _hist_intent = _PRESET_TO_HISTORY[_preset]
                elif _hist_display_type:
                    # Fallback: use display_type as category for free-form queries
                    _hist_category = _hist_display_type
                    _hist_intent = "freeform"
                else:
                    _hist_category = "general"
                    _hist_intent = "query"

                # Build content snippet for history entry — human-readable, not raw JSON
                from data.history_renderer import render_structured_to_text
                _hist_content = ""
                if isinstance(result, dict):
                    _hist_content = render_structured_to_text(result)

                # Do not skip history rows when renderer returns blank (this caused
                # prompt/response pairs to silently disappear in History UI).
                if not _hist_content:
                    if isinstance(result, dict):
                        _hist_content = (
                            (result.get("analysis") or "").strip()
                            or ((_structured_data.get("message") or "") if isinstance(_structured_data, dict) else "")
                            or ((_structured_data.get("summary") or "") if isinstance(_structured_data, dict) else "")
                        )
                    if not _hist_content:
                        _hist_content = (user_query or "").strip() or "(empty response)"

                # Extract tickers + recommended prices from structured response
                _hist_tickers = extract_tickers_from_structured(_structured_data) if _structured_data else None

                # Build conversation snapshot (user query + full response)
                _hist_conversation = None
                try:
                    _conv_messages = []
                    if user_query:
                        _conv_messages.append({"role": "user", "content": user_query})
                    _asst_resp = {}
                    if isinstance(result, dict):
                        if result.get("analysis"):
                            _asst_resp["analysis"] = result["analysis"]
                        if result.get("structured"):
                            _asst_resp["structured"] = result["structured"]
                    if _asst_resp:
                        _conv_messages.append({"role": "assistant", "content": _json.dumps(_asst_resp, default=str)[:16000]})
                    if _conv_messages:
                        _hist_conversation = _conv_messages
                except Exception:
                    pass

                # Build the full structured response object for the frontend
                _hist_structured_response = None
                if isinstance(result, dict):
                    _hist_structured_response = {}
                    if result.get("analysis"):
                        _hist_structured_response["analysis"] = result["analysis"]
                    if result.get("structured"):
                        _hist_structured_response["structured"] = result["structured"]

                _save_prompt_history(
                    category=_hist_category,
                    intent=_hist_intent,
                    content=_hist_content[:8000],
                    display_type=_hist_display_type or "chat",
                    user_id=_hist_user_id,
                    model_used=_hist_model,
                    query=user_query,
                    tickers=_hist_tickers,
                    conversation=_hist_conversation,
                    structured_response=_hist_structured_response,
                )
                _ticker_count = len(_hist_tickers) if _hist_tickers else 0
                print(f"[HISTORY] Saved to prompt_history: category={_hist_category}, intent={_hist_intent}, model={_hist_model}, tickers={_ticker_count}, len={len(_hist_content)}")
            except Exception as _hist_err:
                print(f"[HISTORY] Failed to auto-save prompt history: {_hist_err}")

            yield _j.dumps(resp).encode()

        except asyncio.TimeoutError:
            meta["timing_ms"]["total"] = int((_time.time() - t0) * 1000)
            resp = _error_envelope("REQUEST_TIMEOUT", "Request timed out — please try again.", meta)
            _resp_log(req_id, 200, "timeout", resp)
            yield _j.dumps(resp).encode()

        except Exception as e:
            import traceback
            print(f"[API] request_id={req_id} status=error error={e}")
            traceback.print_exc()
            meta["timing_ms"]["total"] = int((_time.time() - t0) * 1000)
            resp = _error_envelope("UNHANDLED_EXCEPTION", f"Something went wrong: {str(e)}", meta)
            _resp_log(req_id, 500, "error", resp)
            yield _j.dumps(resp).encode()

    async def _safe_stream_query():
        """Wraps _stream_query to catch any crash and always yield JSON."""
        import json as _sj
        import traceback as _stb
        try:
            async for chunk in _stream_query():
                yield chunk
        except Exception as exc:
            print(f"[STREAM] FATAL CRASH in _stream_query: {exc}")
            _stb.print_exc()
            try:
                err_resp = _error_envelope(
                    "STREAM_CRASH",
                    f"Internal streaming error: {str(exc)}",
                    meta,
                )
                yield _sj.dumps(err_resp).encode()
            except Exception:
                yield _sj.dumps({"type": "error", "code": "STREAM_CRASH", "analysis": str(exc)}).encode()

    return StreamingResponse(
        _safe_stream_query(),
        media_type="application/json",
        headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"},
    )


class TestCsvRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    csv_data: Optional[str] = None


@app.post("/api/test-csv")
@limiter.limit("10/minute")
async def test_csv(request: Request, body: TestCsvRequest):
    """Debug endpoint: accepts csv_data, parses it, returns tickers + first 3 rows."""
    import csv as _csv
    import io as _io

    if not body.csv_data:
        return JSONResponse(status_code=400, content={"error": "No csv_data provided"})

    raw = body.csv_data
    print(f"[TEST-CSV] Received {len(raw)} chars, first 200: {raw[:200]}")

    try:
        clean = raw.replace(chr(65279), "").replace("\r\n", "\n").replace("\r", "\n")
        reader = _csv.DictReader(_io.StringIO(clean))
        rows = []
        ticker_col = None
        for row in reader:
            if not ticker_col:
                for key in row.keys():
                    kl = key.lower().strip()
                    if kl in ("ticker", "symbol", "stock", "name", "company"):
                        ticker_col = key
                        break
                if not ticker_col:
                    ticker_col = list(row.keys())[0]
            rows.append(row)

        tickers = []
        for row in rows:
            val = (row.get(ticker_col, "") or "").strip().upper()
            if ":" in val:
                val = val.split(":")[-1]
            if val and 1 <= len(val) <= 10:
                tickers.append(val)

        print(f"[TEST-CSV] Parsed {len(tickers)} tickers from col '{ticker_col}', columns={list(rows[0].keys()) if rows else []}")

        return {
            "status": "ok",
            "chars_received": len(raw),
            "ticker_column": ticker_col,
            "columns": list(rows[0].keys()) if rows else [],
            "total_rows": len(rows),
            "tickers": tickers,
            "first_3_rows": rows[:3],
        }
    except Exception as e:
        print(f"[TEST-CSV] Parse error: {e}")
        return JSONResponse(status_code=400, content={"error": f"CSV parse failed: {str(e)}"})


@app.post("/api/cache/clear")
@limiter.limit("5/minute")
async def clear_cache(request: Request, api_key: str = Header(None, alias="X-API-Key")):
    if not _jwt_or_key(request, api_key):
        raise HTTPException(status_code=403, detail="Invalid API key")
    from data.cache import cache
    cache.clear()
    return {"status": "Cache cleared"}


class WatchlistRequest(BaseModel):
    model_config = {"extra": "ignore"}
    tickers: List[str]
    name: Optional[str] = None
    csv_data: Optional[str] = None


@app.post("/api/watchlist")
@limiter.limit("20/minute")
async def create_watchlist_endpoint(
    request: Request,
    body: WatchlistRequest,
):
    """Create a new watchlist record (tickers + optional CSV). Analysis starts null; call POST /api/watchlist/:id/refresh to populate it."""
    import csv as _csv
    import io as _io
    import uuid as _uuid
    from datetime import datetime, timezone

    if not body.tickers:
        raise HTTPException(status_code=400, detail="No tickers provided.")

    tickers = list(dict.fromkeys(t.strip().upper() for t in body.tickers if t.strip()))
    watchlist_id = str(_uuid.uuid4())
    saved_at = datetime.now(timezone.utc).isoformat()

    if body.name:
        name = body.name.strip() or None
    if not body.name or not body.name.strip():
        name = ", ".join(tickers[:3]) + (f" +{len(tickers) - 3}" if len(tickers) > 3 else "")

    # Parse raw CSV string into list-of-dicts if provided
    csv_rows: list = []
    if body.csv_data:
        try:
            reader = _csv.DictReader(_io.StringIO(body.csv_data.strip()))
            csv_rows = [dict(row) for row in reader]
        except Exception:
            csv_rows = [{"Symbol": t} for t in tickers]
    if not csv_rows:
        csv_rows = [{"Symbol": t} for t in tickers]

    # Enrich with fundamentals from FMP + SEC EDGAR when CSV data is missing them
    # (manual ticker-entry uploads have only a Symbol column — no market data)
    try:
        from services.fundamentals_enricher import enrich_if_needed
        from config import FMP_API_KEY as _fmp_key
        csv_rows = await enrich_if_needed(tickers, csv_rows, fmp_api_key=_fmp_key)
    except Exception as _enrich_err:
        print(f"[API] Fundamentals enrichment skipped: {_enrich_err}")

    print(f"[API] Creating watchlist '{name}' id={watchlist_id} tickers={tickers}")

    try:
        from data.pg_storage import watchlist_write, is_available as pg_available
        if pg_available():
            ok = watchlist_write(watchlist_id, name, csv_rows, {}, tickers)
            if ok:
                print(f"[API] Watchlist '{name}' saved to PostgreSQL (id={watchlist_id})")
                # Trigger symbol-driven earnings sync for the watchlist universe
                try:
                    from services.user_earnings_service import (
                        invalidate_user_earnings,
                        sync_universe_background,
                    )
                    from config import FMP_API_KEY as _fmp_key_earn
                    from services.catalyst_calendar_service import _load_watchlist_symbols
                    # Load all watchlist symbols (including this new watchlist)
                    all_wl_syms = _load_watchlist_symbols()
                    invalidate_user_earnings("watchlist")
                    import asyncio as _aio
                    _aio.create_task(
                        sync_universe_background("watchlist", all_wl_syms, _fmp_key_earn or "")
                    )
                    print(f"[API] Watchlist earnings sync triggered ({len(all_wl_syms)} symbols)")
                except Exception as _sync_err:
                    print(f"[API] Watchlist earnings sync trigger failed: {_sync_err}")
                return {
                    "id": watchlist_id,
                    "name": name,
                    "ticker_count": len(tickers),
                    "saved_at": saved_at,
                    "analysis": None,
                }
    except Exception as e:
        print(f"[API] PostgreSQL watchlist create failed ({e}), falling back to JSON")

    # JSON file fallback
    try:
        import json as _json_mod
        from pathlib import Path
        data_dir = Path(__file__).resolve().parent / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        store_path = data_dir / "watchlist_store.json"
        store = {
            "id": watchlist_id, "name": name, "tickers": tickers,
            "csv_data": csv_rows, "analysis": None, "saved_at": saved_at,
        }
        store_path.write_text(_json_mod.dumps(store, default=str, indent=2))
        print(f"[API] Watchlist '{name}' saved to JSON fallback (id={watchlist_id})")
    except Exception as e:
        print(f"[API] JSON watchlist fallback failed: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to save watchlist: {e}")

    return {
        "id": watchlist_id,
        "name": name,
        "ticker_count": len(tickers),
        "saved_at": saved_at,
        "analysis": None,
    }


class CreateConversationRequest(BaseModel):
    first_query: str = "New conversation"

class UpdateConversationRequest(BaseModel):
    messages: List[dict] = []
def _shape_prompt_history(all_history: dict, recent_limit: int = 10, current_prices: dict | None = None) -> dict:
    """Return a frontend-friendly history payload while preserving bucket grouping."""
    if not isinstance(all_history, dict):
        all_history = {}

    # Enrich ticker entries with current prices if provided
    if current_prices:
        for _key, bucket in all_history.items():
            if not isinstance(bucket, dict):
                continue
            for entry in bucket.get("entries", []):
                if not isinstance(entry, dict):
                    continue
                for t in entry.get("tickers", []):
                    rec = t.get("rec_price")
                    cur = current_prices.get(t.get("ticker"))
                    if rec and cur:
                        t["current_price"] = round(cur, 2)
                        t["pct_change"] = round(((cur - rec) / rec) * 100, 2)

    # Re-render content from structured_response so old entries get the latest renderer
    from data.history_renderer import render_structured_to_text as _re_render

    categories: dict = {}
    items: list[dict] = []

    for bucket_key, bucket in all_history.items():
        if not isinstance(bucket, dict):
            continue

        category = bucket.get("category") or "general"
        intent = bucket.get("intent") or "query"
        entries = bucket.get("entries", [])
        if not isinstance(entries, list):
            entries = []

        categories.setdefault(category, {})
        categories[category][intent] = entries

        for entry in entries:
            if not isinstance(entry, dict):
                continue

            # Re-render content from structured_response if available
            sr = entry.get("structured_response")
            if isinstance(sr, dict) and sr:
                try:
                    fresh = _re_render(sr)
                    if fresh and len(fresh) > 20:
                        entry["content"] = fresh[:8000]
                except Exception:
                    pass

            items.append(
                {
                    "category": category,
                    "intent": intent,
                    "bucket_key": bucket_key,
                    **entry,
                }
            )

    def _sort_ts(x):
        """Extract a sortable timestamp, falling back to id (ms epoch)."""
        ts = x.get("timestamp")
        if isinstance(ts, (int, float)) and ts > 0:
            return float(ts)
        # id is str(int(time.time() * 1000))
        try:
            return int(x.get("id", 0)) / 1000.0
        except (ValueError, TypeError):
            return 0.0

    items.sort(key=_sort_ts, reverse=True)
    recent = items[: max(1, min(recent_limit, 100))]

    return {
        "buckets": all_history,
        "categories": categories,
        "items": items,
        "recent": recent,
        "recent_count": len(recent),
        "total_count": len(items),
    }

@app.get("/api/conversations")
@limiter.limit("30/minute")
async def get_conversations(request: Request):
    from data.chat_history import list_conversations
    return {"conversations": list_conversations(), "_meta": _history_storage_meta()}

@app.get("/api/conversations/{conv_id}")
@limiter.limit("30/minute")
async def get_conversation_detail(request: Request, conv_id: str):
    from data.chat_history import get_conversation
    conv = get_conversation(conv_id)
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return conv

@app.post("/api/conversations")
@limiter.limit("30/minute")
async def create_new_conversation(request: Request, body: CreateConversationRequest):
    from data.chat_history import create_conversation
    conv = create_conversation(body.first_query)
    return conv

@app.put("/api/conversations/{conv_id}")
@limiter.limit("30/minute")
async def update_conversation(request: Request, conv_id: str, body: UpdateConversationRequest):
    from data.chat_history import save_messages
    success = save_messages(conv_id, body.messages)
    if not success:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return {"success": success}

@app.delete("/api/conversations/{conv_id}")
@limiter.limit("30/minute")
async def delete_conv(request: Request, conv_id: str):
    from data.chat_history import delete_conversation
    success = delete_conversation(conv_id)
    return {"success": success}

# ── Prompt History ──────────────────────────────────────────────

def _history_storage_meta() -> dict:
    """Build a _meta block describing the active storage backend + any errors."""
    try:
        from data.prompt_history import _use_postgres as _ph_pg
        from data.pg_storage import is_available as _pg_ok, get_last_conn_error as _pg_err
        backend = "postgresql" if _ph_pg else "fallback"
        return {
            "storage_backend": backend,
            "db_connected": _pg_ok() if _ph_pg else False,
            "db_error": _pg_err() if _ph_pg else None,
        }
    except Exception as e:
        return {"storage_backend": "unknown", "db_connected": False, "db_error": str(e)}


@app.get("/api/history")
@limiter.limit("30/minute")
async def get_history(request: Request):
    import asyncio as _aio
    from data.prompt_history import get_all
    user_id = getattr(request.state, "user_id", "default")
    all_history = get_all(user_id=user_id)

    # Enrich entries with current prices for tickers that have rec_price
    ticker_set = set()
    for key, bucket in all_history.items():
        for entry in bucket.get("entries", []):
            for t in entry.get("tickers", []):
                if t.get("rec_price") and t.get("ticker"):
                    ticker_set.add(t["ticker"])

    current_prices = {}
    if ticker_set and data_service:
        async def _fetch(ticker):
            try:
                quote = await _aio.to_thread(data_service.finnhub.get_quote, ticker)
                return ticker, quote.get("price")
            except Exception:
                return ticker, None
        results = await _aio.gather(*[_fetch(t) for t in ticker_set])
        current_prices = {t: p for t, p in results if p and p > 0}

    # Inject current_price and pct_change into each ticker entry
    if current_prices:
        for key, bucket in all_history.items():
            for entry in bucket.get("entries", []):
                for t in entry.get("tickers", []):
                    rec = t.get("rec_price")
                    cur = current_prices.get(t.get("ticker"))
                    if rec and cur:
                        t["current_price"] = round(cur, 2)
                        t["pct_change"] = round(((cur - rec) / rec) * 100, 2)

    # Keep backward compatibility for older clients that expect raw {"category::intent": bucket}.
    fmt = (request.query_params.get("format") or "").lower().strip()
    if fmt in {"legacy", "raw"}:
        return all_history

    limit_param = request.query_params.get("recent_limit", "10")
    try:
        recent_limit = int(limit_param)
    except Exception:
        recent_limit = 10
    result = _shape_prompt_history(all_history, recent_limit=recent_limit)
    result["_meta"] = _history_storage_meta()
    return result


@app.get("/api/history/recent")
@limiter.limit("30/minute")
async def get_history_recent(request: Request, limit: int = 10):
    from data.prompt_history import get_all
    user_id = getattr(request.state, "user_id", "default")
    all_history = get_all(user_id=user_id)
    shaped = _shape_prompt_history(all_history, recent_limit=limit)
    return {
        "recent": shaped.get("recent", []),
        "recent_count": shaped.get("recent_count", 0),
        "total_count": shaped.get("total_count", 0),
        "_meta": _history_storage_meta(),
    }

@app.get("/api/history/sidebar")
@limiter.limit("30/minute")
async def get_history_sidebar(request: Request, limit: int = 10):
    """Sidebar-friendly recent history — same shape as /api/history/recent."""
    from data.prompt_history import get_all
    user_id = getattr(request.state, "user_id", "default")
    all_history = get_all(user_id=user_id)
    shaped = _shape_prompt_history(all_history, recent_limit=limit)
    return {
        "recent": shaped.get("recent", []),
        "recent_count": shaped.get("recent_count", 0),
        "total_count": shaped.get("total_count", 0),
        "_meta": _history_storage_meta(),
    }

@app.get("/api/history/storage-info")
@limiter.limit("10/minute")
async def history_storage_info(request: Request):
    """Diagnostic: which storage backend is active and how much data is stored."""
    from data.prompt_history import _use_postgres as _ph_pg, _use_object_storage as _ph_obj, _use_replit_db as _ph_db
    from data.chat_history import _use_postgres as _ch_pg, _use_object_storage as _ch_obj, _use_replit_db as _ch_db
    ph_backend = "PostgreSQL" if _ph_pg else ("Object Storage" if _ph_obj else ("Replit DB" if _ph_db else "JSON files (EPHEMERAL)"))
    ch_backend = "PostgreSQL" if _ch_pg else ("Object Storage" if _ch_obj else ("Replit DB" if _ch_db else "JSON files (EPHEMERAL)"))
    info = {
        "prompt_history_backend": ph_backend,
        "chat_history_backend": ch_backend,
    }
    try:
        from data.pg_storage import storage_info as _pg_info, get_last_conn_error as _pg_err
        info["postgresql"] = _pg_info()
        info["last_connection_error"] = _pg_err()
    except Exception:
        info["postgresql"] = {"available": False}
    return info


@app.get("/api/history/backtest-summary")
@limiter.limit("10/minute")
async def history_backtest_summary(request: Request):
    """
    For each history entry that has tickers with rec_price,
    fetch current prices and return cumulative % change per entry_id.
    No LLM — pure math + Finnhub price lookup.
    """
    import asyncio as _aio
    await _wait_for_init()
    if not data_service:
        raise HTTPException(status_code=503, detail="Service not ready")

    from data.prompt_history import get_all
    user_id = getattr(request.state, "user_id", "default")
    all_history = get_all(user_id=user_id)

    # Collect all unique tickers that need price lookups
    ticker_set = set()
    entries_with_tickers = []  # (entry_id, tickers_list)
    for key, bucket in all_history.items():
        for entry in bucket.get("entries", []):
            tickers = entry.get("tickers", [])
            priced = [t for t in tickers if t.get("rec_price")]
            if priced:
                entries_with_tickers.append((entry["id"], priced))
                for t in priced:
                    ticker_set.add(t["ticker"])

    if not ticker_set:
        return {"backtest": {}, "as_of": _dt.now(_tz.utc).isoformat()}

    # Batch-fetch current prices (in thread to not block)
    async def _fetch_price(ticker: str) -> tuple:
        try:
            quote = await _aio.to_thread(data_service.finnhub.get_quote, ticker)
            return ticker, quote.get("price")
        except Exception:
            return ticker, None

    price_tasks = [_fetch_price(t) for t in ticker_set]
    price_results = await _aio.gather(*price_tasks)
    current_prices = {t: p for t, p in price_results if p and p > 0}

    # Compute cumulative % per entry
    backtest = {}
    for entry_id, tickers in entries_with_tickers:
        total_pct = 0.0
        valid_count = 0
        ticker_details = []
        for t in tickers:
            cur = current_prices.get(t["ticker"])
            if cur and t["rec_price"]:
                pct = ((cur - t["rec_price"]) / t["rec_price"]) * 100
                total_pct += pct
                valid_count += 1
                ticker_details.append({
                    "ticker": t["ticker"],
                    "rec_price": round(t["rec_price"], 2),
                    "current_price": round(cur, 2),
                    "pct_change": round(pct, 2),
                })
        if valid_count > 0:
            avg_pct = round(total_pct / valid_count, 2)
            backtest[entry_id] = {
                "cumulative_pct": avg_pct,
                "ticker_count": valid_count,
                "details": ticker_details,
            }

    return {"backtest": backtest, "as_of": _dt.now(_tz.utc).isoformat()}

@app.get("/api/history/{category}/{intent}")
@limiter.limit("30/minute")
async def get_history_by_intent(request: Request, category: str, intent: str):
    from data.prompt_history import get_by_intent
    user_id = getattr(request.state, "user_id", "default")
    return {"entries": get_by_intent(category, intent, user_id=user_id)}

@app.post("/api/history")
@limiter.limit("30/minute")
async def save_history(request: Request, x_api_key: str = Header(None)):
    body = await request.json()
    category = body.get("category", "")
    intent = body.get("intent", "")
    content = body.get("content", "")
    display_type = body.get("display_type")
    if not category or not intent or not content:
        raise HTTPException(status_code=400, detail="category, intent, and content are required")
    user_id = getattr(request.state, "user_id", "default")
    from data.prompt_history import save_response
    entry = save_response(
        category,
        intent,
        content,
        display_type,
        user_id=user_id,
        model_used=body.get("model_used"),
        query=body.get("query"),
        tickers=body.get("tickers"),
        conversation=body.get("conversation"),
        structured_response=body.get("structured_response"),
    )
    return {"success": True, "entry": entry}

@app.delete("/api/history/{category}/{intent}/{entry_id}")
@limiter.limit("30/minute")
async def delete_history_entry(request: Request, category: str, intent: str, entry_id: str):
    user_id = getattr(request.state, "user_id", "default")
    from data.prompt_history import delete_entry
    success = delete_entry(category, intent, entry_id, user_id=user_id)
    return {"success": success}

@app.delete("/api/history/{category}/{intent}")
@limiter.limit("30/minute")
async def clear_history_intent(request: Request, category: str, intent: str):
    user_id = getattr(request.state, "user_id", "default")
    from data.prompt_history import clear_intent
    success = clear_intent(category, intent, user_id=user_id)
    return {"success": success}

# ── Backtest ──────────────────────────────────────────────────

class BacktestItem(BaseModel):
    ticker: str
    recommended_price: float
    recommended_date: str  # ISO date or human-readable

class BacktestRequest(BaseModel):
    items: List[BacktestItem]
    model_used: Optional[str] = None  # which model made the recommendation

@app.post("/api/backtest")
@limiter.limit("20/minute")
async def backtest_recommendations(request: Request, body: BacktestRequest, _sub: None = Depends(require_subscription)):
    """
    Backtest historical recommendations: fetch current price via Finnhub,
    calculate % gain/loss, and return a Haiku-generated summary row per ticker.
    """
    await _wait_for_init()
    if not data_service:
        raise HTTPException(status_code=503, detail="Service not ready")

    from config import ANTHROPIC_API_KEY
    import anthropic

    results = []
    for item in body.items:
        ticker = item.ticker.upper()
        quote = data_service.finnhub.get_quote(ticker)
        current_price = quote.get("price")
        if current_price and current_price > 0:
            pct_change = round(((current_price - item.recommended_price) / item.recommended_price) * 100, 2)
            results.append({
                "ticker": ticker,
                "recommended_price": item.recommended_price,
                "recommended_date": item.recommended_date,
                "current_price": current_price,
                "pct_change": pct_change,
                "direction": "gain" if pct_change >= 0 else "loss",
            })
        else:
            results.append({
                "ticker": ticker,
                "recommended_price": item.recommended_price,
                "recommended_date": item.recommended_date,
                "current_price": None,
                "pct_change": None,
                "direction": "unknown",
                "error": "Could not fetch current price",
            })

    # Build a quick Haiku summary
    rows_text = "\n".join(
        f"- {r['ticker']}: recommended ${r['recommended_price']:.2f} on {r['recommended_date']}, "
        f"now ${r['current_price']:.2f}, {'+' if r['pct_change'] >= 0 else ''}{r['pct_change']}% {'gain' if r['pct_change'] >= 0 else 'loss'}"
        if r["current_price"] else f"- {r['ticker']}: price unavailable"
        for r in results
    )

    model_label = body.model_used or "the model"
    haiku_prompt = (
        f"You are a concise trading performance tracker. Given these backtest results from {model_label}, "
        f"produce a brief, clean one-row-per-ticker summary table (use | separators) showing: "
        f"Ticker | Rec Price | Current Price | % Change | Verdict. "
        f"After the table, add ONE sentence overall verdict on how {model_label} performed.\n\n"
        f"Results:\n{rows_text}"
    )

    summary = ""
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        resp = await asyncio.to_thread(
            client.messages.create,
            model=MODEL_CLAUDE_FAST,
            max_tokens=400,
            messages=[{"role": "user", "content": haiku_prompt}],
        )
        summary = resp.content[0].text if resp.content else ""
    except Exception as e:
        print(f"[BACKTEST] Haiku summary error: {e}")
        summary = rows_text  # fallback to raw data

    return {
        "results": results,
        "summary": summary,
        "model_used": body.model_used,
        "as_of": _dt.now(_tz.utc).isoformat(),
    }

@app.get("/api/health")
@limiter.limit("30/minute")
async def health_check(request: Request):
    """Full diagnostic — tests Claude, Finviz, and StockAnalysis."""
    import asyncio
    await _wait_for_init()
    errors = []

    claude_ok = False
    try:
        response = await asyncio.wait_for(
            asyncio.to_thread(
                agent.client.messages.create,
                model=MODEL_CLAUDE_BALANCED,
                max_tokens=20,
                messages=[{"role": "user", "content": "Say ok"}],
            ),
            timeout=15.0,
        )
        claude_ok = True
    except Exception as e:
        errors.append(f"Claude Reasoning: {str(e)}")

    finviz_ok = False
    try:
        result = await asyncio.wait_for(
            agent.data.finviz.get_screener_results("ta_topgainers"),
            timeout=10.0,
        )
        finviz_ok = isinstance(result, list) and len(result) > 0
        if not finviz_ok:
            errors.append(f"Finviz returned {len(result) if isinstance(result, list) else 'non-list'} results")
    except Exception as e:
        errors.append(f"Finviz: {str(e)}")

    sa_ok = False
    try:
        result = await asyncio.wait_for(
            agent.data.stockanalysis.get_overview("AAPL"),
            timeout=10.0,
        )
        sa_ok = result is not None and len(result) > 0
        if not sa_ok:
            errors.append("StockAnalysis returned empty for AAPL")
    except Exception as e:
        errors.append(f"StockAnalysis: {str(e)}")

    edgar_health = {}
    try:
        edgar_health = agent.data.sec_edgar.get_health()
    except Exception as e:
        edgar_health = {"enabled": True, "last_error": str(e), "circuit": "unknown"}

    return {
        "claude_reasoning": claude_ok,
        "finviz": finviz_ok,
        "stockanalysis": sa_ok,
        "edgar": edgar_health,
        "errors": errors,
        "status": "ok" if (claude_ok and finviz_ok and sa_ok) else "degraded",
    }


# ============================================================
# API Rate Monitor
# ============================================================

@app.get("/api/rate-status")
async def rate_status(request: Request, api_key: str = Header(None, alias="X-API-Key")):
    """
    Real-time view of Tradier API usage vs limits.
    Shows calls in the last 60s, headroom, lifetime throttle count,
    and a breakdown of which services bypass the provider.
    """
    if not _jwt_or_key(request, api_key):
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")

    from data.tradier_provider import TRADIER_LIMITER, TradierProvider
    tradier_status = TRADIER_LIMITER.status()

    # ── Remaining unmanaged (bypass) Tradier paths ───────────────────────────
    # Phase 1 cleaned: social, catalyst, whale, discovery, sector-rotation,
    # watchlist-topup. The following are intentional isolations or not yet migrated.
    bypass_services = [
        "congressional_trading_service.py",
        "insider_activity_service.py",
    ]

    unmanaged_tradier_paths = [
        {
            "module": "services/theme_rs_service.py",
            "function": "_tradier_quotes_batch",
            "endpoint": "/markets/quotes",
            "mechanism": "raw httpx + own Semaphore(20)",
            "note": "Intentional isolation for 1D intraday warmup; does NOT count against TRADIER_LIMITER",
        },
        {
            "module": "services/theme_rs_service.py",
            "function": "_fetch_intraday_bars",
            "endpoint": "/markets/timesales",
            "mechanism": "raw httpx + own Semaphore(20)",
            "note": "Intentional isolation for 1D theme RS curves",
        },
        {
            "module": "services/theme_rs_service.py",
            "function": "_fetch_tradier_daily_history",
            "endpoint": "/markets/history",
            "mechanism": "raw httpx",
            "note": "Daily bar fallback; bypasses TRADIER_LIMITER",
        },
        {
            "module": "services/watchlist_quote_cache.py",
            "function": "_fetch_batch_direct",
            "endpoint": "/markets/quotes",
            "mechanism": "raw httpx",
            "note": "Startup-only fallback fires only when data_service not yet initialised; suppressed once warm",
        },
    ]

    # ── Options in-flight status ──────────────────────────────────────────────
    try:
        from services.options_inflight import get_inflight_status as _inflight_status
        inflight_data = _inflight_status()
    except Exception:
        inflight_data = {"error": "options_inflight module unavailable"}

    # ── Coalescing counters ───────────────────────────────────────────────────
    try:
        coalescing_data = TradierProvider.get_coalescing_status()
    except Exception:
        coalescing_data = {"error": "coalescing status unavailable"}

    # ── Supplement loop anti-duplication diagnostics ──────────────────────────
    try:
        from data.options_theme_supplement import get_supplement_diag as _get_supp_diag
        supplement_loop_diag = _get_supp_diag()
    except Exception:
        supplement_loop_diag = {"error": "supplement diag unavailable"}

    # ── Market session classification ─────────────────────────────────────────
    try:
        from data.tradier_market_session import get_session_info as _get_sess_info
        session_info = _get_sess_info()
    except Exception:
        session_info = {"tradier_market_session": "unknown", "is_tradier_active_session": None}

    # ── Loop diagnostics (master screener + supplement) ───────────────────────
    try:
        import data.loop_diagnostics as _loop_diag_mod
        loop_diag = _loop_diag_mod.get_loop_diag()
    except Exception:
        loop_diag = {}

    # ── last_429_at from TradierProvider module ───────────────────────────────
    try:
        import data.tradier_provider as _td_mod
        _last_429_at_ts = getattr(_td_mod, "_last_429_at", None)
    except Exception:
        _last_429_at_ts = None

    # ── Phase 3 lane budget diagnostics ──────────────────────────────────────
    try:
        import data.tradier_budget as _bgt_mod
        budget_diag = _bgt_mod.diagnostics()
    except Exception:
        budget_diag = {"tradier_budget_enabled": False, "error": "budget module unavailable"}

    # ── Phase 4A: active quote demand diagnostics ─────────────────────────────
    try:
        import data.quote_demand_registry as _qdr_mod
        quote_demand_diag = _qdr_mod.diagnostics()
    except Exception:
        quote_demand_diag = {"error": "quote_demand_registry unavailable"}

    return {
        # ── Session & loop health (Phase 2) ──────────────────────────────────
        **session_info,
        **loop_diag,
        "calls_used_last_60s": tradier_status.get("calls_last_60s"),
        "last_429_at": _last_429_at_ts,
        # ── Phase 3 lane budget (Phase 3) ─────────────────────────────────────
        **budget_diag,
        # ── Tradier rate-limiter ──────────────────────────────────────────────
        "tradier": {
            **tradier_status,
            "limit_note": "110/min cap; theme_rs_service.py uses its own Semaphore(20) pool separately",
            "bypass_services": bypass_services,
            "bypass_note": (
                f"{len(bypass_services)} services still call Tradier directly "
                f"(congressional, insider — not yet migrated)"
            ),
        },
        "options_inflight": inflight_data,
        "coalescing": coalescing_data,
        "supplement_loop": supplement_loop_diag,
        # ── Phase 4A: active quote demand ─────────────────────────────────────
        "quote_demand": quote_demand_diag,
        "unmanaged_tradier_paths": {
            "count": len(unmanaged_tradier_paths),
            "paths": unmanaged_tradier_paths,
        },
        "fmp": {
            "note": "No rate limiter — FMP calls use per-endpoint caching; add limiter if 429s appear",
        },
    }


# ============================================================
# Candle Stats Debug Endpoint
# ============================================================

@app.get("/api/candle_stats")
async def candle_stats(request: Request):
    from data.market_data_service import get_last_candle_stats, _is_finnhub_candles_disabled, _is_twelvedata_disabled
    stats = get_last_candle_stats()
    stats["finnhub_circuit_open"] = _is_finnhub_candles_disabled()
    stats["twelvedata_circuit_open"] = _is_twelvedata_disabled()
    return stats


@app.get("/api/health/budget")
async def health_budget(request: Request):
    from api_budget import daily_budget
    return daily_budget.status()


@app.get("/api/health/perplexity-guards")
async def perplexity_guards_status(request: Request):
    """
    Diagnostic: Perplexity safety guard state and blocked-call counters.
    Shows which flags are active and how many automatic Perplexity calls
    have been blocked since boot.
    """
    from data.perplexity_guards import guard_diagnostics
    from api_budget import daily_budget
    diag = guard_diagnostics()
    budget = daily_budget.status().get("providers", {}).get("web_search", {})
    diag["web_search_budget"] = budget
    return diag


# ============================================================
# Portfolio Holdings CRUD
# ============================================================
def _portfolio_file(user_id: str = "default") -> Path:
    """Return the canonical portfolio file path.
    Auth is disabled — all requests share one canonical source.
    When auth is re-enabled, extend portfolio_store to support per-user files.
    """
    from data.portfolio_store import canonical_file as _canonical
    return _canonical()


@app.get("/api/portfolio/holdings")
async def get_holdings(request: Request, api_key: str = Header(None, alias="X-API-Key")):
    """Return saved portfolio holdings and derived aggregate category lists.

    Response shape
    --------------
    {
      "holdings": [...],                    # flat active holdings (backward compat)
      "open_positions": [...],              # all active holdings (open + partial)
      "partially_closed_positions": [...],  # active holdings trimmed from current lot
      "fully_closed_positions": [...],      # closed symbols (from closed-trade records)
    }

    Category derivation rules (universal — no ticker-specific logic):
      open_positions           : all holdings where classification in ("open",
                                 "partially_closed_open") and shares > tolerance
      partially_closed_positions: subset of open_positions where classification
                                  == "partially_closed_open"
      fully_closed_positions   : symbols that appear in closed-trade records
                                 with final_symbol_status=="fully_closed" AND
                                 are NOT present in active holdings
    """
    import time as _time_h
    _t0_h = _time_h.perf_counter()

    from data.portfolio_store import load_active_holdings as _load
    from data.closed_trades_store import load_closed_trades as _load_ct
    from collections import defaultdict as _dd

    _SHARE_TOL = 1e-6

    user_id  = getattr(request.state, "user_id", "default")
    holdings = _load()
    syms     = [h.get("ticker", "?") for h in holdings]
    # ── Phase 4A: register portfolio demand for quote priority ────────────────
    _pf_syms = [s for s in syms if s and s != "?"]
    if _pf_syms:
        try:
            import data.quote_demand_registry as _qdr
            _qdr.register(_pf_syms, "portfolio", ttl=90)
        except Exception:
            pass
    print(
        f"[portfolio-source-audit] endpoint=dashboard-get  user_id={user_id}  "
        f"source_file=data/portfolio/active_holdings.json  "
        f"count={len(holdings)}  symbols={syms[:20]}"
    )

    # ── Load closed trades FIRST so we can re-derive classification ───────────
    # Classification is written at import time via the full ledger pipeline.
    # Holdings saved through the simple POST /api/portfolio/holdings endpoint
    # (frontend sync) do NOT carry a classification field — for those records
    # we derive it here from the closed-trade ledger:
    #
    #   partially_closed_open  — the holding has ≥1 sell event that reduced
    #                            the CURRENT open lot (exit_date ≥ entry_date)
    #                            AND shares remained after that sell
    #                            (remaining_shares_after > SHARE_TOL,
    #                             is_full_close == False)
    #
    #   open                   — no such qualifying sell event exists
    #
    # Buy-close-reopen correctness: a prior full-close sell has exit_date
    # BEFORE entry_date of the new lot and is therefore ignored.
    # Dust/rounding correctness: sell events must have remaining_shares_after
    # above SHARE_TOL and is_full_close=False to count as a partial trim.
    _trades       = _load_ct()
    _trades_by_sym: dict[str, list[dict]] = _dd(list)
    for _t in _trades:
        _s = (_t.get("ticker") or "").upper()
        if _s:
            _trades_by_sym[_s].append(_t)

    # ── Holdings loop: bucket into open_positions + partially_closed ─────────
    open_positions:     list[dict] = []
    partially_closed:   list[dict] = []
    active_syms:        set[str]   = set()

    for _h in holdings:
        _sym = (_h.get("ticker") or "").upper()
        if not _sym:
            continue
        _shares = float(_h.get("shares") or 0)
        if _shares <= _SHARE_TOL:
            continue                    # dust holding — skip
        active_syms.add(_sym)

        # ── Classification: use persisted field when present; otherwise
        # re-derive from the closed-trade ledger so that holdings saved via
        # the simple POST /api/portfolio/holdings endpoint (which carries no
        # classification field) are still correctly bucketed.
        #
        # Re-derivation rule (universal — no ticker-specific logic):
        #   partially_closed_open  ←  ≥1 closed-trade for this symbol satisfies:
        #     • exit_date ≥ entry_date  (sell reduced the CURRENT open lot, not
        #                                a prior lot that was fully closed first)
        #     • remaining_shares_after > SHARE_TOL  (shares still remained)
        #     • is_full_close is not True  (it was a partial trim, not a full exit)
        #   open                   ←  no such qualifying sell event
        #
        # Buy-close-reopen: a prior full close has exit_date < entry_date → ignored.
        # Dust/fees: remaining_shares_after ≤ SHARE_TOL → ignored.
        _stored_cls = _h.get("classification")
        if _stored_cls:
            _cls = _stored_cls
        else:
            _entry_dt = str(_h.get("entry_date") or "")
            _sym_trades = _trades_by_sym.get(_sym, [])
            _has_partial_sell = any(
                (str(_t.get("exit_date") or "") >= _entry_dt or not _entry_dt)
                and float(_t.get("remaining_shares_after") or 0) > _SHARE_TOL
                and not _t.get("is_full_close", False)
                # Guard against stale/corrupt closed-trade artifacts:
                # a real partial trim ALWAYS changes the position — the qty sold
                # must differ from the qty remaining (beyond FP noise).
                # If sold == remaining it means the sell "doubled" the position
                # (e.g. 152 → sell 76 → 76 remaining) — for a single event this
                # is indistinguishable from a corrupt orphan record, so we require
                # the delta to be non-trivial before calling it a partial trim.
                and abs(float(_t.get("shares") or 0) - float(_t.get("remaining_shares_after") or 0)) > _SHARE_TOL
                for _t in _sym_trades
            )
            _cls = "partially_closed_open" if _has_partial_sell else "open"

        _avg = float(_h.get("avg_cost") or 0)
        _rec = {
            "symbol":              _sym,
            "shares":              _shares,
            "avg_cost":            _avg,
            "cost_basis":          round(_shares * _avg, 2),
            "entry_date":          _h.get("entry_date"),
            "last_buy_date":       _h.get("entry_date"),
            "classification":      _cls,
            "final_symbol_status": _cls,
            "status":              _cls,
            "basis_source":        _h.get("basis_source", "csv_lot"),
            "import_batch_id":     _h.get("import_batch_id"),
            "source_file":         _h.get("source_file"),
        }
        open_positions.append(_rec)
        if _cls == "partially_closed_open":
            partially_closed.append(_rec)

    # ── Enrich partially_closed cards with closed-trade aggregates ─────────────
    # Add realized P&L, shares_sold, last_exit_price, sell_events_count.
    for _pc in partially_closed:
        _sym    = _pc["symbol"]
        _st     = _trades_by_sym.get(_sym, [])
        _st_asc = sorted(_st, key=lambda x: x.get("exit_date") or "")
        _shares_sold = round(sum(float(_t.get("shares") or 0) for _t in _st_asc), 8)
        _total_pnl   = round(sum(float(_t.get("realized_pnl") or 0) for _t in _st_asc), 4)
        _sb = _pc["shares"] + _shares_sold          # approx total bought
        _pc.update({
            "shares_bought":        _sb,
            "shares_sold":          _shares_sold,
            "shares_remaining":     _pc["shares"],
            "percent_closed":       round(_shares_sold / _sb * 100, 2) if _sb else None,
            "percent_remaining":    round(_pc["shares"] / _sb * 100, 2) if _sb else None,
            "realized_pnl":         _total_pnl,
            "last_exit_price":      _st_asc[-1].get("exit_price") if _st_asc else None,
            "last_exit_date":       _st_asc[-1].get("exit_date")  if _st_asc else None,
            "sell_events_count":    len(_st_asc),
        })

    # ── Derive fully_closed_positions from closed-trade records ───────────────
    # A symbol is fully closed if:
    #   1. It appears in closed trades with final_symbol_status == "fully_closed"
    #   2. It is NOT currently in active holdings (active_syms)
    # This correctly handles buy-close-reopen: if a new lot is open the symbol
    # is already in active_syms and won't appear here.
    _fc_by_sym: dict[str, list[dict]] = _dd(list)
    for _t in _trades:
        _s = (_t.get("ticker") or "").upper()
        if not _s or _s in active_syms:
            continue
        # Use stored final_symbol_status; fall back to inferring from remaining_shares_after
        _fss = _t.get("final_symbol_status") or ""
        if not _fss:
            _rsaf = _t.get("remaining_shares_after")
            _fss  = "fully_closed" if (_rsaf is not None and float(_rsaf) <= _SHARE_TOL) else ""
        if _fss == "fully_closed":
            _fc_by_sym[_s].append(_t)

    fully_closed: list[dict] = []
    for _sym, _st in _fc_by_sym.items():
        _st_asc    = sorted(_st, key=lambda x: x.get("exit_date") or "")
        _tot_sh    = round(sum(float(_t.get("shares") or 0) for _t in _st_asc), 8)
        _tot_cost  = sum(float(_t.get("shares") or 0) * float(_t.get("entry_price") or 0)
                         for _t in _st_asc)
        _tot_pnl   = round(sum(float(_t.get("realized_pnl") or 0) for _t in _st_asc), 4)
        _exits     = [_t.get("exit_date")  for _t in _st_asc if _t.get("exit_date")]
        _entries   = [_t.get("entry_date") for _t in _st_asc if _t.get("entry_date")]
        _final_ex  = max(_exits)   if _exits   else None
        _first_en  = min(_entries) if _entries else None
        _avg_entry = round(_tot_cost / _tot_sh, 6) if _tot_sh else 0.0
        _pnl_pct   = round(_tot_pnl / _tot_cost * 100, 4) if _tot_cost else None
        _hpd: int | None = None
        if _first_en and _final_ex:
            try:
                from datetime import date as _dc
                _hpd = (_dc.fromisoformat(_final_ex) - _dc.fromisoformat(_first_en)).days
            except Exception:
                pass
        fully_closed.append({
            "symbol":              _sym,
            "total_shares_bought": _tot_sh,   # approx from sell records
            "total_shares_sold":   _tot_sh,
            "avg_entry_price":     _avg_entry,
            "last_exit_price":     _st_asc[-1].get("exit_price"),
            "realized_pnl":        _tot_pnl,
            "realized_pnl_pct":    _pnl_pct,
            "first_entry_date":    _first_en,
            "final_exit_date":     _final_ex,
            "holding_period_days": _hpd,
            "final_symbol_status": "fully_closed",
            "classification":      "fully_closed",
            "status":              "fully_closed",
            "basis_source":        _st_asc[-1].get("basis_source", "csv_lot"),
        })

    # ── Load option positions and closed trades from DB ───────────────────────
    from data.option_trades_store import (
        load_option_positions    as _load_opt_pos,
        load_option_closed_trades as _load_opt_ct,
    )
    _raw_opt_pos = _load_opt_pos()
    _raw_opt_ct  = _load_opt_ct()

    # Split option positions into open / partially-closed / fully-closed
    _opt_open_pos:    list[dict] = []
    _opt_partial_pos: list[dict] = []
    _opt_fc_pos:      list[dict] = []
    for _op in _raw_opt_pos:
        _st = _op.get("final_status", "open")
        if _st in ("open", "partially_closed_open", "short_option_tracked_basic"):
            _opt_open_pos.append(_op)
            if _st == "partially_closed_open":
                _opt_partial_pos.append(_op)
        elif _st in ("fully_closed", "expired", "orphan_expired"):
            _opt_fc_pos.append(_op)

    # ── Enrich open option positions with Tradier mark quotes ─────────────────
    def _occ_key_to_tradier_sym(occ_key: str) -> str | None:
        """Convert internal occ_key → Tradier OCC compact symbol for quote lookups."""
        try:
            parts = occ_key.split("_")
            if len(parts) < 4:
                return None
            _und  = parts[0]
            _exp  = parts[1]   # YYYY-MM-DD
            _strk = float(parts[2])
            _otyp = parts[3].upper()  # CALL or PUT
            from datetime import datetime as _dtm2
            _yymmdd = _dtm2.strptime(_exp, "%Y-%m-%d").strftime("%y%m%d")
            _cp     = "C" if _otyp == "CALL" else "P"
            _si     = round(_strk * 1000)
            return f"{_und}{_yymmdd}{_cp}{_si:08d}"
        except Exception:
            return None

    # Build occ_key → Tradier compact symbol map for all open positions
    _opt_sym_map: dict[str, str] = {}
    for _op in _opt_open_pos:
        _ok   = _op.get("occ_key", "")
        _tsym = _occ_key_to_tradier_sym(_ok)
        if _ok and _tsym:
            _opt_sym_map[_ok] = _tsym

    # Batch-fetch Tradier option quotes (single call per unique symbol, cached 60 s)
    _opt_quote_map: dict[str, dict] = {}  # upper(tradier_sym) → quote dict
    if _opt_sym_map:
        try:
            from data.tradier_provider import TradierProvider as _TPOV
            _tpov = _TPOV(api_key=os.getenv("TRADIER_API_KEY", ""))
            _tradier_syms = list(_opt_sym_map.values())
            from data.tradier_budget import lane as _pf_opt_lane
            with _pf_opt_lane("saved_options"):
                _opt_quotes_raw = await _tpov.get_quotes(_tradier_syms)
            for _q in _opt_quotes_raw:
                _qsym = (_q.get("symbol") or "").upper()
                if _qsym:
                    _opt_quote_map[_qsym] = _q
        except Exception as _eq:
            print(f"[portfolio-holdings] option quote fetch error: {_eq}")

    # Enrich each open option position with mark/market_value/unrealized_pnl
    _opt_total_cost   = 0.0
    _opt_total_mktval = 0.0
    _opt_val_hit      = 0
    _opt_val_miss     = 0
    _OPT_MULT         = 100

    for _op in _opt_open_pos:
        _ok        = _op.get("occ_key", "")
        _tsym_u    = (_opt_sym_map.get(_ok) or "").upper()
        _q         = _opt_quote_map.get(_tsym_u) if _tsym_u else None
        _contracts = float(_op.get("contracts_open") or 0)
        _cb        = float(_op.get("cost_basis") or 0)
        _opt_total_cost += _cb

        _op["tradier_symbol"] = _tsym_u or None

        if _q:
            _bid  = _q.get("bid")
            _ask  = _q.get("ask")
            _last = _q.get("last")
            # Mark = midpoint when both bid & ask available and sum > 0; else last; else None
            if _bid is not None and _ask is not None and (_bid + _ask) > 0:
                _mark = round((_bid + _ask) / 2, 4)
            elif _last is not None and _last > 0:
                _mark = round(float(_last), 4)
            else:
                _mark = None

            if _mark is not None:
                _mktval   = round(_contracts * _mark * _OPT_MULT, 2)
                _upnl     = round(_mktval - _cb, 2)
                _upnl_pct = round(_upnl / _cb * 100, 4) if _cb else None
                _op["mark_price"]         = _mark
                _op["mark_bid"]           = _bid
                _op["mark_ask"]           = _ask
                _op["mark_last"]          = _last
                _op["mark_source"]        = "tradier"
                _op["market_value"]       = _mktval
                _op["unrealized_pnl"]     = _upnl
                _op["unrealized_pnl_pct"] = _upnl_pct
                _opt_total_mktval        += _mktval
                _opt_val_hit             += 1
            else:
                _op["mark_price"]         = None
                _op["mark_bid"]           = _bid
                _op["mark_ask"]           = _ask
                _op["mark_last"]          = _last
                _op["mark_source"]        = "tradier_no_mark"
                _op["market_value"]       = None
                _op["unrealized_pnl"]     = None
                _op["unrealized_pnl_pct"] = None
                _opt_val_miss            += 1
        else:
            _op["mark_price"]         = None
            _op["mark_bid"]           = None
            _op["mark_ask"]           = None
            _op["mark_last"]          = None
            _op["mark_source"]        = "unavailable"
            _op["market_value"]       = None
            _op["unrealized_pnl"]     = None
            _op["unrealized_pnl_pct"] = None
            _opt_val_miss            += 1

    # Stock cost basis from active open_positions (avg_cost × shares already computed)
    _stock_cost = round(sum(float(_rec.get("cost_basis") or 0) for _rec in open_positions), 2)

    # Build portfolio_summary block
    _opt_upnl_total = round(_opt_total_mktval - _opt_total_cost, 2) if _opt_val_hit else None
    _opt_upnl_pct_total = (
        round(_opt_upnl_total / _opt_total_cost * 100, 4)
        if (_opt_val_hit and _opt_total_cost and _opt_upnl_total is not None)
        else None
    )
    _portfolio_summary = {
        "stock_cost_basis":                  _stock_cost,
        "option_cost_basis":                 round(_opt_total_cost, 2),
        "option_market_value":               round(_opt_total_mktval, 2) if _opt_val_hit else None,
        "option_unrealized_pnl":             _opt_upnl_total,
        "option_unrealized_pnl_pct":         _opt_upnl_pct_total,
        "option_positions_count":            len(_opt_open_pos),
        "options_valuation_available_count": _opt_val_hit,
        "options_valuation_missing_count":   _opt_val_miss,
        "options_value_method":              "tradier_mark" if _opt_val_hit else "cost_basis_fallback",
    }

    print(
        f"[portfolio-options-active-debug] "
        f"open={len(_opt_open_pos)} val_hit={_opt_val_hit} val_miss={_opt_val_miss} "
        f"opt_market_value={round(_opt_total_mktval, 2)} "
        f"opt_cost_basis={round(_opt_total_cost, 2)}"
    )

    _resp_ms = round((_time_h.perf_counter() - _t0_h) * 1000, 1)
    _fast_debug = {
        "response_ms":        _resp_ms,
        "active_count":       len(holdings),
        "open_count":         len(open_positions),
        "partial_count":      len(partially_closed),
        "fully_closed_count": len(fully_closed),
        "option_open_count":  len(_opt_open_pos),
        "option_closed_count":len(_opt_fc_pos),
        "closed_trades_count":len(_trades),
        "provider_calls_made":0,
        "recomputed_ledger":  False,
        "source":             "neon_db_with_mem_cache",
    }
    print(f"[portfolio-holdings-fast-path] {_fast_debug}")

    return {
        "holdings":                          holdings,
        "open_positions":                    open_positions,
        "partially_closed_positions":        partially_closed,
        "fully_closed_positions":            fully_closed,
        "option_open_positions":             _opt_open_pos,
        "option_partially_closed_positions": _opt_partial_pos,
        "option_fully_closed_positions":     _opt_fc_pos,
        "option_closed_trades":              _raw_opt_ct,
        "portfolio_summary":                 _portfolio_summary,
    }


@app.delete("/api/portfolio/holdings/clear-all")
async def clear_all_holdings(request: Request, api_key: str = Header(None, alias="X-API-Key")):
    """Wipe all active holdings — use before a clean CSV re-import.

    Clears the canonical holdings store and invalidates the terminal cache.
    Does NOT touch closed trade records.
    Returns: { cleared: true, previous_count: N }
    """
    from data.portfolio_store import (
        load_active_holdings  as _load,
        save_active_holdings  as _save,
        canonical_file        as _cf,
    )
    previous = _load()
    prev_count = len(previous)
    _save([])
    print(f"[PORTFOLIO_CLEAR_ALL] wiped {prev_count} holdings")

    # Invalidate terminal cache so the next visit starts fresh
    try:
        from data.caelyn_terminal import CaelynTerminalProvider
        from data.cache import cache as _app_cache
        _app_cache.delete(CaelynTerminalProvider.cache_key_for(_cf()))
    except Exception:
        pass

    return {"cleared": True, "previous_count": prev_count}


@app.delete("/api/portfolio/reset-all")
async def reset_all_portfolio(request: Request, api_key: str = Header(None, alias="X-API-Key")):
    """Nuclear reset: wipe ALL holdings AND ALL closed trade records.

    Use this to start completely fresh before re-importing your CSVs.
    This is irreversible.

    Returns: { cleared: true, holdings_removed: N, closed_trades_removed: N }
    """
    if not _jwt_or_key(request, api_key):
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")

    from data.portfolio_store import (
        load_active_holdings  as _load_h,
        save_active_holdings  as _save_h,
        canonical_file        as _cf,
    )
    from data.closed_trades_store import (
        load_closed_trades as _load_ct,
        delete_closed_trade as _del_ct,
    )

    # 1. Clear holdings
    prev_holdings = _load_h()
    holdings_count = len(prev_holdings)
    _save_h([])
    print(f"[PORTFOLIO_RESET_ALL] wiped {holdings_count} holdings")

    # 2. Clear closed trades (delete each record)
    all_trades = _load_ct()
    ct_count = len(all_trades)
    ct_deleted = 0
    for t in all_trades:
        if _del_ct(t["id"]):
            ct_deleted += 1
    print(f"[PORTFOLIO_RESET_ALL] deleted {ct_deleted}/{ct_count} closed trades")

    # 3. Invalidate terminal cache
    try:
        from data.caelyn_terminal import CaelynTerminalProvider
        from data.cache import cache as _app_cache
        _app_cache.delete(CaelynTerminalProvider.cache_key_for(_cf()))
    except Exception:
        pass

    return {
        "cleared":               True,
        "holdings_removed":      holdings_count,
        "closed_trades_removed": ct_deleted,
    }


@app.post("/api/portfolio/holdings")
async def save_holdings(request: Request, api_key: str = Header(None, alias="X-API-Key")):
    """Save portfolio holdings to the canonical source.
    Accepts {holdings: [...]} OR a raw array [...].
    Invalidates Terminal cache and triggers earnings sync.
    """
    from data.portfolio_store import save_active_holdings as _save, canonical_file as _cf
    user_id = getattr(request.state, "user_id", "default")

    raw_bytes = await request.body()
    print(f"[portfolio-save] raw_body_preview={raw_bytes[:200]!r}  len={len(raw_bytes)}")

    try:
        body = __import__("json").loads(raw_bytes)
    except Exception as _je:
        print(f"[portfolio-save] JSON parse error: {_je}")
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {_je}")

    # Accept both {holdings:[...]} and a bare array [...]
    if isinstance(body, list):
        holdings = body
    elif isinstance(body, dict):
        holdings = body.get("holdings") or body.get("positions") or []
        if not isinstance(holdings, list):
            raise HTTPException(status_code=400, detail="holdings must be a list")
    else:
        raise HTTPException(status_code=400, detail="Body must be {holdings:[...]} or [...]")

    # Normalize: accept both ticker and symbol keys.
    # IMPORTANT: preserve existing lots so that a flat frontend sync never
    # strips lot arrays that were written by the CSV import.
    from data.portfolio_store import load_active_holdings as _load_existing
    existing_lots_map: dict = {}
    try:
        for _eh in _load_existing():
            _t = (_eh.get("ticker") or "").upper()
            _lots = _eh.get("lots")
            if _t and isinstance(_lots, list) and _lots:
                existing_lots_map[_t] = _lots
    except Exception:
        pass

    normalized = []
    for h in holdings:
        if not isinstance(h, dict):
            continue
        ticker = (h.get("ticker") or h.get("symbol") or "").strip().upper()
        if not ticker:
            continue
        _ed = h.get("entry_date") or h.get("date_added") or None
        _entry: dict = {
            "ticker":     ticker,
            "shares":     float(h.get("shares") or h.get("quantity") or 0),
            "avg_cost":   float(h.get("avg_cost") or h.get("avgCost") or h.get("average_cost") or h.get("cost_basis") or 0),
            "asset_type": (h.get("asset_type") or h.get("assetType") or h.get("type") or "stock").lower(),
        }
        if _ed:
            _entry["entry_date"] = _ed
        # Preserve lots from incoming data if present, otherwise carry over
        # existing lots so a flat frontend sync never wipes import data.
        incoming_lots = h.get("lots")
        if isinstance(incoming_lots, list) and incoming_lots:
            _entry["lots"] = incoming_lots
        elif ticker in existing_lots_map:
            _entry["lots"] = existing_lots_map[ticker]
        normalized.append(_entry)
    holdings = normalized
    new_syms = [h["ticker"] for h in holdings if h.get("ticker")]

    # ── Write to canonical source ─────────────────────────────────────────────
    _save(holdings)
    print(
        f"[portfolio-save] endpoint=POST-holdings  user_id={user_id}  "
        f"count={len(new_syms)}  symbols={new_syms[:25]}"
    )

    # ── Invalidate Terminal cache ──────────────────────────────────────────────
    try:
        from data.caelyn_terminal import CaelynTerminalProvider
        from data.cache import cache as _app_cache
        term_ck = CaelynTerminalProvider.cache_key_for(_cf())
        _app_cache.delete(term_ck)
        print(
            f"[portfolio-source-audit] terminal_cache_invalidated  "
            f"key={term_ck}  holdings_count={len(new_syms)}"
        )
    except Exception as _inv_err:
        print(f"[portfolio-source-audit] terminal_cache_invalidation_failed: {_inv_err}")

    # ── Trigger earnings sync for the new universe ────────────────────────────
    try:
        pf_syms: set[str] = set(new_syms)
        if pf_syms:
            from services.user_earnings_service import (
                invalidate_user_earnings,
                sync_universe_background,
            )
            from config import FMP_API_KEY as _fmp_key_pf
            import asyncio as _aio
            invalidate_user_earnings("portfolio")
            _aio.create_task(
                sync_universe_background("portfolio", pf_syms, _fmp_key_pf or "")
            )
            print(f"[portfolio-source-audit] earnings_sync_triggered  count={len(pf_syms)}")
    except Exception as _pf_sync_err:
        print(f"[portfolio-source-audit] earnings_sync_trigger_failed: {_pf_sync_err}")

    # ── Patch full-year earnings cache (add new tickers, drop removed ones) ──
    try:
        from services.pfull_year_service import patch_pfull_cache as _patch_pfull
        from config import FMP_API_KEY as _fmp_key_pfull
        import asyncio as _aio2
        _aio2.create_task(_patch_pfull(set(new_syms), _fmp_key_pfull or ""))
        print(f"[portfolio-save] pfull_cache_patch_scheduled  symbols={new_syms[:5]}...")
    except Exception as _pfull_err:
        print(f"[portfolio-save] pfull_cache_patch_schedule_failed (non-fatal): {_pfull_err}")

    return {"success": True, "holdings_count": len(new_syms), "symbols": new_syms}


# ── Portfolio CSV Upload (/api/portfolio/upload-csv) ──────────────────────────

def _parse_portfolio_csv(csv_text: str) -> dict:
    """Parse a brokerage CSV export into structured buy/sell lots.

    Normalises column names across brokerages (Schwab, Fidelity, TD, IBKR, etc.).
    Returns:
      {
        "buy_lots":  {TICKER: [{"date", "shares", "price", "notes"}, ...]},
        "sell_lots": {TICKER: [{"date", "shares", "price", "notes"}, ...]},
        "skipped":   [{"row", "reason"}, ...],
        "columns":   [...],
        "rows_total": int,
      }
    """
    import csv as _csv
    import io as _io
    import re as _re

    # Normalise BOM, line endings
    clean = csv_text.replace(chr(65279), "").replace("\r\n", "\n").replace("\r", "\n").strip()

    # Some Schwab CSVs have header junk rows before the actual header;
    # find the first line that contains "Symbol" or "Ticker"
    lines = clean.splitlines()
    header_idx = 0
    for i, line in enumerate(lines):
        low = line.lower()
        if "symbol" in low or "ticker" in low or "stock" in low:
            header_idx = i
            break
    clean = "\n".join(lines[header_idx:])

    # ── Auto-detect delimiter (comma vs tab vs pipe vs semicolon) ────────────
    # Schwab and some other brokerages export TSV (tab-separated), not CSV.
    # We sniff a sample of the cleaned text; fall back to comma on failure.
    _sample = clean[:4096]
    _delimiter = ","
    try:
        _dialect = _csv.Sniffer().sniff(_sample, delimiters=",\t|;")
        _delimiter = _dialect.delimiter
    except Exception:
        # Manual fallback: pick whichever of comma/tab appears more on line 1
        _first_line = clean.split("\n")[0]
        if _first_line.count("\t") > _first_line.count(","):
            _delimiter = "\t"

    reader = _csv.DictReader(_io.StringIO(clean), delimiter=_delimiter)
    raw_cols = reader.fieldnames or []
    cols_lower = {c.lower().strip(): c for c in raw_cols}

    # ── Column name resolver ──────────────────────────────────────────────────
    def _find_col(*candidates) -> str | None:
        for c in candidates:
            if c.lower() in cols_lower:
                return cols_lower[c.lower()]
        return None

    col_symbol = _find_col("symbol", "ticker", "stock", "security", "instrument")
    col_date   = _find_col("date", "trade date", "transaction date", "activity date",
                            "settlement date", "run date")
    col_type   = _find_col("transaction type", "type", "action", "transaction",
                            "activity", "description type", "trans type")
    col_desc   = _find_col("description", "security description", "name")
    col_qty    = _find_col("quantity", "shares", "qty", "units", "amount (quantity)")
    col_price  = _find_col("price", "price/share", "unit price", "cost/share",
                            "exec price", "execution price", "trade price")
    col_amount = _find_col("amount", "value", "total", "net amount", "principal",
                            "market value", "cost basis total", "net proceeds")

    BUY_KEYWORDS  = {"buy", "bought", "purchase", "purchased", "reinvestment",
                     "reinvest", "shares purchased", "exchange in", "transfer in",
                     "journaled shares", "you bought",
                     # Brokerage transfer / receive terminology:
                     # Covers ACAT transfers, DTC deliveries, and positions
                     # moved between brokerages (common for OTC/foreign stocks).
                     "received", "receive", "securities received",
                     "shares received", "stock received", "received shares",
                     "securities transfer", "acat",
                     "deliver in", "delivered in",
                     # Platform-specific buy keywords (Robinhood, Public, etc.)
                     "market buy", "limit buy", "stop buy", "buy open",
                     }
    SELL_KEYWORDS = {"sell", "sold", "sale", "shares sold", "exchange out",
                     "transfer out", "you sold",
                     "sell short", "short sale", "sold short",
                     "market sell", "limit sell"}

    # Options action keywords — must be checked BEFORE buy/sell because
    # "buy to open" contains "buy" and would otherwise be mis-classified.
    # Phrase-level match: check full raw_type string contains the phrase.
    OPTIONS_ACTION_PHRASES = (
        "buy to open", "sell to close", "buy to close", "sell to open",
        "to open", "to close",              # catch abbreviated brokerage variants
        "option expired", "expired",
        "assigned", "assignment",
        "exercised", "exercise",
        "opening purchase", "closing sale",
        "opening sale", "closing purchase",
    )

    # Instrument-type column (some brokerages have a dedicated column)
    col_instr_type = _find_col(
        "instrument type", "security type", "asset type",
        "product type", "type of security",
    )

    # ── Money-market / cash symbols to skip ──────────────────────────────────
    SKIP_SYMBOLS = {"", "cash", "cashbalance", "--", "n/a", "spaxx", "fdrxx",
                    "swvxx", "vmfxx", "sprxx", "fdlxx", "fzfxx", "mmda1"}

    buy_lots:       dict[str, list] = {}
    sell_lots:      dict[str, list] = {}
    skipped:        list[dict]      = []
    options_rows:   list[dict]      = []   # collected separately for summary
    action_values:  set             = set()  # every raw action string seen

    for row in reader:
        def _v(col): return (row.get(col, "") or "").strip() if col else ""

        raw_sym = _v(col_symbol)
        # Strip exchange prefix (e.g. "NASDAQ:AAPL" → "AAPL")
        if ":" in raw_sym:
            raw_sym = raw_sym.split(":")[-1]
        ticker = raw_sym.upper().strip()

        # ── Options detection: symbol shape ───────────────────────────────────
        # Detect before the normal symbol-validity gate so we can give a
        # specific "options skipped" reason instead of a generic invalid-symbol.
        def _is_option_symbol(raw: str, sym: str) -> bool:
            if raw.startswith("-"):                              # Fidelity dash prefix
                return True
            # Space followed by a digit = OCC-style date embedded in symbol
            # (e.g. "AAPL 240119C00180000").  A bare suffix like "SIVEF E" or
            # "SIVEF GREY" (exchange/market annotation) must NOT be treated as
            # an option — only flag when the space precedes a numeric date.
            if _re.search(r'\s+\d', raw):                       # space + digit → date
                return True
            if _re.search(r'\d{6}[CP]\d+', sym):               # OCC: AAPL240119C00180000
                return True
            if _re.search(r'_\d{6}[CP]', sym):                 # TD/ToS: AAPL_240119C180
                return True
            if _re.search(r'\d{2}/\d{2}/\d{2,4}', raw):       # date in symbol field
                return True
            # Explicit instrument-type column
            if col_instr_type:
                instr = _v(col_instr_type).lower()
                if "option" in instr or "opt" == instr:
                    return True
            return False

        if _is_option_symbol(raw_sym, ticker):
            options_rows.append({
                "ticker": ticker or raw_sym,
                "action": _v(col_type) or _v(col_desc),
                "reason": "options contract — skipped (equity positions only)",
            })
            continue

        if ticker.lower() in SKIP_SYMBOLS or len(ticker) > 12:
            skipped.append({"row": dict(row), "reason": f"skipped symbol '{ticker}'"})
            continue

        if not ticker or not _re.match(r'^[A-Z0-9.\-]{1,12}$', ticker):
            skipped.append({"row": dict(row), "reason": f"invalid symbol '{ticker}'"})
            continue

        # Transaction type
        raw_type = _v(col_type).lower()
        # Only fall back to description when a type column EXISTS but this
        # particular row is empty.  When there is no type column at all,
        # description typically holds a company name ("Sivers Semiconductors"),
        # NOT a transaction verb — using it as a type would drop every row that
        # doesn't happen to mention "buy" or "sell" in the company name.
        if col_type and not raw_type and col_desc:
            raw_type = _v(col_desc).lower()

        # Track every unique action value for diagnostics
        if raw_type:
            action_values.add(_v(col_type) or raw_type)   # preserve original casing

        # ── Options detection: action phrase ──────────────────────────────────
        # Must happen BEFORE buy/sell keyword check — "buy to open" contains
        # "buy" and would otherwise be added as a stock position.
        if any(phrase in raw_type for phrase in OPTIONS_ACTION_PHRASES):
            options_rows.append({
                "ticker": ticker,
                "action": _v(col_type) or _v(col_desc),
                "reason": "options action — skipped (equity positions only)",
            })
            continue

        # Explicit instrument-type column check (catch stragglers)
        if col_instr_type and "option" in _v(col_instr_type).lower():
            options_rows.append({
                "ticker": ticker,
                "action": _v(col_type) or _v(col_desc),
                "reason": "options instrument type — skipped (equity positions only)",
            })
            continue

        is_buy  = any(kw in raw_type for kw in BUY_KEYWORDS)
        is_sell = any(kw in raw_type for kw in SELL_KEYWORDS)

        # If no dedicated type/action column exists, treat all rows as buys.
        # Previously this only fired when BOTH type and description were absent,
        # but description columns contain company names, not transaction verbs —
        # so the absence of a type column alone is enough to default to buy.
        if not col_type:
            is_buy = True

        if not is_buy and not is_sell:
            skipped.append({"row": dict(row), "reason": f"non-trade type '{_v(col_type)}'"})
            continue

        # Parse quantity (abs value; sign carried by is_buy/is_sell)
        try:
            qty_raw = _v(col_qty).replace(",", "").replace("(", "-").replace(")", "")
            qty = abs(float(qty_raw))
        except Exception:
            # Try to back-calculate from amount ÷ price
            try:
                amt   = abs(float(_v(col_amount).replace(",", "").replace("(", "-").replace(")", "").replace("$", "")))
                price = abs(float(_v(col_price).replace(",", "").replace("$", "")))
                qty   = amt / price if price else 0
            except Exception:
                skipped.append({"row": dict(row), "reason": "could not parse quantity"})
                continue

        if qty <= 0:
            skipped.append({"row": dict(row), "reason": "zero quantity"})
            continue

        # Parse price
        try:
            price_raw = _v(col_price).replace(",", "").replace("$", "").replace("(", "-").replace(")", "")
            price = abs(float(price_raw)) if price_raw else 0.0
        except Exception:
            price = 0.0

        # Back-calculate price from amount if missing
        if price == 0.0 and col_amount:
            try:
                amt_raw = _v(col_amount).replace(",", "").replace("$", "").replace("(", "-").replace(")", "")
                amt = abs(float(amt_raw))
                price = round(amt / qty, 6) if qty else 0.0
            except Exception:
                pass

        # Parse date
        raw_date = _v(col_date)
        lot_date: str | None = None
        for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y", "%d/%m/%Y",
                    "%B %d, %Y", "%b %d, %Y", "%Y%m%d"):
            try:
                from datetime import datetime as _dt
                lot_date = _dt.strptime(raw_date.split(" ")[0], fmt).date().isoformat()
                break
            except Exception:
                pass

        notes_val = _v(col_desc) or None
        lot = {"date": lot_date, "shares": qty, "price": price, "notes": notes_val}

        if is_buy:
            buy_lots.setdefault(ticker, []).append(lot)
        else:
            sell_lots.setdefault(ticker, []).append(lot)

    # Orphan sells (tickers with ONLY sells in this CSV, no buy) are kept in
    # sell_lots.  The endpoint will try to apply them against existing DB
    # holdings — if there are no DB holdings they are skipped with a reason.

    return {
        "buy_lots":          buy_lots,
        "sell_lots":         sell_lots,
        "options_rows":      options_rows,
        "skipped":           skipped,
        "columns":           list(raw_cols),
        "action_distribution": sorted(action_values),   # every unique action seen in CSV
        "rows_total":        sum(len(v) for v in buy_lots.values()) +
                             sum(len(v) for v in sell_lots.values()) +
                             len(options_rows) + len(skipped),
    }


@app.post("/api/portfolio/upload-csv")
async def portfolio_upload_csv(request: Request, api_key: str = Header(None, alias="X-API-Key")):
    """Import holdings from a brokerage CSV export.

    Accepts standard brokerage transaction columns (works with Schwab, Fidelity,
    TD Ameritrade, Interactive Brokers, Robinhood, Webull, E*Trade, etc.):
      Date, Transaction Type, Symbol, Description, Quantity, Price, Amount

    Body (JSON):
      {
        "csv_data":        str,      # raw CSV text (required)
        "mode":            str,      # "preview" | "import"  (default "import")
        "merge_strategy":  str,      # "add_lots" | "replace"  (default "add_lots")
                                     #   add_lots → appends new lots, deduplicates by
                                     #              date+shares+price so re-importing
                                     #              the same CSV is safe; use this
                                     #              when uploading multiple CSVs
                                     #   replace  → overwrites ALL lots for matching
                                     #              tickers with the new CSV lots
        "include_sells":   bool      # if true, sell rows are logged as closed trades
                                     # (default false — sells are reported but skipped)
      }

    Response:
      {
        "success":          bool,
        "mode":             str,
        "rows_total":       int,
        "rows_skipped":     int,
        "symbols_imported": [...],
        "symbols_skipped":  [...],
        "holdings_created": int,
        "holdings_updated": int,
        "lots_added":       int,
        "sells_found":      int,
        "preview":          [{ticker, lots, total_shares, avg_cost, action}, ...],
        "skipped_detail":   [{row, reason}, ...]   # first 20 only
      }
    """
    if not _jwt_or_key(request, api_key):
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")
    try:
        body = await request.json()
    except Exception as _je:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {_je}")

    csv_data = (body.get("csv_data") or "").strip()
    if not csv_data:
        raise HTTPException(status_code=400, detail="csv_data is required")

    mode           = (body.get("mode") or "import").lower().strip()
    # Default is "add_lots" so multiple CSV files (e.g. Roth IRA + Individual
    # Brokerage) can be uploaded sequentially and their holdings accumulate.
    # Duplicate protection is handled by _dedup_lots (exact date+shares+price
    # match), so re-importing the same CSV file is also safe.
    # Pass merge_strategy="replace" explicitly to overwrite lots for a ticker.
    merge_strategy = (body.get("merge_strategy") or "add_lots").lower().strip()
    include_sells  = bool(body.get("include_sells", False))
    # account_id: optional string that scopes this CSV to a specific brokerage
    # account (e.g. "roth_ira", "individual", "401k").  When set:
    #   • Every new lot is tagged with {"account_id": account_id}
    #   • Sells in this CSV only net against buys in THIS CSV — they never
    #     close positions that belong to a different account already in the DB
    #   • A ticker "fully closed" in this account still shows in holdings if
    #     another account holds open lots for the same ticker
    account_id = (body.get("account_id") or "").strip() or None

    # full_replace=True wipes ALL holdings + closed-trade records before
    # importing.  Use this for a complete-history CSV so stale data from prior
    # test imports or partial imports never contaminates the result.
    full_replace = bool(body.get("full_replace", False))

    if mode not in ("preview", "import"):
        raise HTTPException(status_code=400, detail="mode must be 'preview' or 'import'")
    if merge_strategy not in ("add_lots", "replace"):
        raise HTTPException(status_code=400, detail="merge_strategy must be 'add_lots' or 'replace'")

    print(
        f"[upload-csv] mode={mode}  merge_strategy={merge_strategy}  "
        f"full_replace={full_replace}  account_id={account_id!r}  "
        f"include_sells={include_sells}  csv_bytes={len(csv_data)}"
    )

    # ── Parse CSV ─────────────────────────────────────────────────────────────
    try:
        parsed = _parse_portfolio_csv(csv_data)
    except Exception as _parse_err:
        import traceback as _tb
        _detail = f"CSV parse error: {_parse_err}\n{_tb.format_exc()}"
        print(f"[upload-csv][ERROR] {_detail}", flush=True)
        raise HTTPException(status_code=500, detail=_detail)

    buy_lots  = parsed["buy_lots"]   # {TICKER: [lot, ...]}
    sell_lots = parsed["sell_lots"]
    skipped   = parsed["skipped"]

    if not buy_lots and not sell_lots:
        return {
            "success":          False,
            "mode":             mode,
            "error":            "No tradeable rows found. Check that the CSV contains Buy/Sell transactions with Symbol, Quantity, and Price columns.",
            "columns_detected": parsed["columns"],
            "rows_total":       parsed["rows_total"],
            "rows_skipped":     len(skipped),
            "skipped_detail":   skipped[:20],
        }

    # ── Helpers ───────────────────────────────────────────────────────────────
    from data.portfolio_store import (
        load_active_holdings  as _load_h,
        save_active_holdings  as _save_h,
        get_holdings_signature as _sig,
        compute_lot_totals    as _totals,
    )
    from data.closed_trades_store import (
        save_closed_trade          as _save_ct,
        save_closed_trades_batch   as _save_ct_batch,
        load_closed_trades         as _load_ct,
        delete_closed_trade        as _del_ct_one,
        delete_all_closed_trades   as _del_ct_all,
    )
    from datetime import date as _date_cls
    import uuid as _uuid_mod

    def _fifo_reduce(b_lots: list, shares_to_remove: float) -> list:
        """Remove shares from oldest lots first (FIFO). Returns remaining lots."""
        sorted_lots = sorted(b_lots, key=lambda l: l.get("date") or "")
        remaining: list = []
        left = round(float(shares_to_remove), 8)
        for lot in sorted_lots:
            lot_shares = round(float(lot.get("shares") or 0), 8)
            if left <= 0:
                remaining.append(dict(lot))
            elif lot_shares <= left + 1e-9:
                left = round(left - lot_shares, 8)   # lot fully consumed
            else:
                partial = dict(lot)
                partial["shares"] = round(lot_shares - left, 8)
                left = 0
                remaining.append(partial)
        return remaining

    def _dedup_lots(existing: list, new_lots: list) -> list:
        """Append new_lots, skipping exact duplicates (date + shares + price)."""
        seen = set()
        result: list = []
        for lot in existing:
            key = (
                str(lot.get("date") or "")[:10],
                round(float(lot.get("shares") or 0), 4),
                round(float(lot.get("price") or 0), 4),
            )
            seen.add(key)
            result.append(lot)
        for lot in new_lots:
            key = (
                str(lot.get("date") or "")[:10],
                round(float(lot.get("shares") or 0), 4),
                round(float(lot.get("price") or 0), 4),
            )
            if key not in seen:
                result.append(lot)
                seen.add(key)
        return result

    existing_holdings = _load_h()

    # ── full_replace: wipe all existing data so this is a clean-slate import ────
    # Triggered by  {"full_replace": true}  in the request body.
    # Recommended for full-history CSVs to avoid stale data from prior imports.
    _fr_prev_holdings  = 0
    _fr_prev_ct        = 0
    if full_replace and mode == "import":
        _fr_prev_holdings = len(existing_holdings)
        # Single DELETE query — replaces N individual round-trips that caused
        # Gunicorn worker timeouts on large portfolios.
        _fr_prev_ct = _del_ct_all()
        _save_h([])
        existing_holdings = []
        print(f"[PORTFOLIO_CSV] full_replace=True: wiped {_fr_prev_holdings} holdings "
              f"+ {_fr_prev_ct} closed trades before import (single-query wipe)")

    existing_map      = {h["ticker"].upper(): h for h in existing_holdings}

    # ── Pre-load closed trades for duplicate detection ─────────────────────────
    # Build a set of (ticker, entry_date, exit_date, shares_rounded) keys so we
    # can skip writing a closed-trade record that already exists in the DB.
    # This prevents duplicate records when the same CSV is imported multiple times.
    _existing_ct_rows = _load_ct() if mode == "import" else []
    _existing_ct_keys: set[tuple] = set()
    for _ct in _existing_ct_rows:
        _existing_ct_keys.add((
            (_ct.get("ticker") or "").upper(),
            str(_ct.get("entry_date") or "")[:10],
            str(_ct.get("exit_date")  or "")[:10],
            round(float(_ct.get("shares") or 0), 2),
        ))

    preview:          list[dict] = []
    _ct_batch:        list[dict] = []   # collected; flushed in one batch INSERT
    holdings_created  = 0
    holdings_updated  = 0
    lots_added        = 0
    sells_logged      = 0          # closed trade records written to DB
    closed_full_count = 0          # tickers fully closed in this CSV
    closed_part_count = 0          # tickers partially closed in this CSV

    # Build updated holdings map — start from all existing holdings so
    # tickers not in this CSV remain untouched.
    updated_map = {k: dict(v) for k, v in existing_map.items()}

    # ── Process each ticker in buy_lots ───────────────────────────────────────
    for ticker, b_lots in buy_lots.items():
        b_lots_valid = [l for l in b_lots if float(l.get("shares") or 0) > 0]
        if not b_lots_valid:
            continue

        s_lots_valid = [l for l in sell_lots.get(ticker, [])
                        if float(l.get("shares") or 0) > 0]

        total_bought = round(sum(float(l["shares"]) for l in b_lots_valid), 8)
        total_sold   = round(sum(float(l["shares"]) for l in s_lots_valid),  8)
        net_shares   = round(total_bought - total_sold, 8)

        # Weighted avg buy price across all buy lots in this CSV
        total_buy_cost  = sum(float(l["shares"]) * float(l.get("price") or 0)
                              for l in b_lots_valid)
        avg_entry_price = round(total_buy_cost / total_bought, 6) if total_bought else 0.0

        buy_dates  = sorted(l["date"] for l in b_lots_valid if l.get("date"))
        entry_date = buy_dates[0] if buy_dates else _date_cls.today().isoformat()

        # ── If any sells exist for this ticker, create a closed trade record ──
        if total_sold > 0 and mode == "import":
            total_sell_value = sum(float(l["shares"]) * float(l.get("price") or 0)
                                   for l in s_lots_valid)
            avg_exit_price   = round(total_sell_value / total_sold, 6)

            sell_dates = sorted(l["date"] for l in s_lots_valid if l.get("date"))
            exit_date  = sell_dates[-1] if sell_dates else _date_cls.today().isoformat()

            # Shares covered by the sell (capped at total bought in this CSV)
            shares_for_ct = min(total_sold, total_bought)
            is_full_close = net_shares <= 0

            # ── Dedup: skip if this exact closed trade record already exists ──
            _ct_key = (
                ticker,
                str(entry_date)[:10],
                str(exit_date)[:10],
                round(float(shares_for_ct), 2),
            )
            if _ct_key in _existing_ct_keys:
                # Same trade already in DB — don't create a duplicate
                sells_logged += 0   # noqa: keep counter at same value
                if is_full_close:
                    closed_full_count += 0
            else:
                _existing_ct_keys.add(_ct_key)   # prevent intra-import doubles

                cost_basis    = avg_entry_price * shares_for_ct
                realized_pnl  = round((avg_exit_price - avg_entry_price) * shares_for_ct, 4)
                realized_pct  = round(realized_pnl / cost_basis * 100, 4) if cost_basis else None

                group_id = str(_uuid_mod.uuid4())
                _ct_batch.append({
                    "ticker":                 ticker,
                    "shares":                 shares_for_ct,
                    "entry_price":            avg_entry_price,
                    "exit_price":             avg_exit_price,
                    "entry_date":             entry_date,
                    "exit_date":              exit_date,
                    "realized_pnl":           realized_pnl,
                    "realized_pnl_pct":       realized_pct,
                    "notes":                  "Imported from CSV",
                    "sell_type":              "full" if is_full_close else "partial",
                    "is_full_close":          is_full_close,
                    "remaining_shares_after": max(net_shares, 0.0),
                    "trade_group_id":         group_id,
                    "cost_method":            "average_cost",
                })
                sells_logged += 1
                if is_full_close:
                    closed_full_count += 1
                else:
                    closed_part_count += 1

        # ── Determine what stays in holdings ──────────────────────────────────
        if net_shares <= 0:
            # This account fully closed its position in this ticker.
            # ── Account isolation ────────────────────────────────────────────
            # If an account_id is set, only remove THIS account's lots from
            # updated_map.  Lots belonging to a DIFFERENT account (e.g. Roth
            # lots when importing Individual Brokerage) must NOT be removed.
            existing_for_close = updated_map.get(ticker)
            if account_id and existing_for_close:
                other_acct_lots = [
                    l for l in (existing_for_close.get("lots") or [])
                    if l.get("account_id") and l["account_id"] != account_id
                ]
                if other_acct_lots:
                    # Other accounts still hold this ticker — update the holding
                    # to show only their lots, not the now-closed account's lots.
                    other_totals = _totals(other_acct_lots)
                    updated_map[ticker] = {
                        **existing_for_close,
                        "shares":     other_totals["shares"],
                        "avg_cost":   other_totals["avg_cost"],
                        "lots":       other_acct_lots,
                    }
                else:
                    # No other-account lots — remove the holding entirely.
                    updated_map.pop(ticker, None)
            else:
                # No account isolation — remove outright (original behaviour).
                updated_map.pop(ticker, None)

            preview.append({
                "ticker":       ticker,
                "action":       "closed",
                "lots_added":   len(b_lots_valid),
                "total_shares": 0,
                "avg_cost":     avg_entry_price,
                "entry_date":   entry_date,
                "closed":       True,
            })
            continue

        # Position still (partially or fully) open — apply FIFO reduction
        if total_sold > 0:
            open_lots = _fifo_reduce(b_lots_valid, total_sold)
        else:
            open_lots = b_lots_valid

        # Tag each new lot with the account_id so future imports can isolate
        # per-account sells correctly.
        if account_id:
            for lot in open_lots:
                lot["account_id"] = account_id

        # Merge with any lots already in the holding (dedup to prevent doubles)
        existing = updated_map.get(ticker)
        if existing and merge_strategy == "add_lots":
            base_lots: list = list(existing.get("lots") or [])
            if base_lots:
                if account_id:
                    # ── Account-scoped dedup ─────────────────────────────────
                    # Only dedup THIS account's existing lots against the new
                    # CSV lots.  Other accounts' lots are kept verbatim.
                    same_acct = [l for l in base_lots
                                 if not l.get("account_id")
                                 or l["account_id"] == account_id]
                    other_acct = [l for l in base_lots
                                  if l.get("account_id")
                                  and l["account_id"] != account_id]
                    merged_lots = other_acct + _dedup_lots(same_acct, open_lots)
                else:
                    merged_lots = _dedup_lots(base_lots, open_lots)
            else:
                # No existing lots — replace with CSV lots (prevents
                # accumulation on flat holdings from the frontend sync).
                merged_lots = open_lots
        else:
            merged_lots = open_lots

        totals   = _totals(merged_lots)
        action   = "updated" if existing else "created"
        updated_h = {
            "ticker":     ticker,
            "shares":     totals["shares"],
            "avg_cost":   totals["avg_cost"],
            "entry_date": totals.get("entry_date") or (existing or {}).get("entry_date")
                          or _date_cls.today().isoformat(),
            "asset_type": (existing or {}).get("asset_type", "stock"),
            "lots":       merged_lots,
        }
        if (existing or {}).get("notes"):
            updated_h["notes"] = existing["notes"]

        updated_map[ticker] = updated_h
        lots_added += len(open_lots)

        if action == "created":
            holdings_created += 1
        else:
            holdings_updated += 1

        preview.append({
            "ticker":       ticker,
            "action":       action,
            "lots_added":   len(open_lots),
            "total_shares": totals["shares"],
            "avg_cost":     totals["avg_cost"],
            "entry_date":   updated_h["entry_date"],
            "sold_shares":  round(total_sold, 8) if total_sold > 0 else None,
            "closed":       False,
        })

    # ── Process orphan sells: sell-only tickers matched against existing holdings ─
    # These are positions opened before the CSV date range that were sold during
    # it.  We create closed-trade records using the existing holding's avg_cost as
    # the entry price so the Trading Journal captures their P&L.
    _orphan_sell_tickers = [t for t in sell_lots if t not in buy_lots]
    for _ot in _orphan_sell_tickers:
        _s_lots_v = [l for l in sell_lots[_ot] if float(l.get("shares") or 0) > 0]
        if not _s_lots_v:
            continue
        _existing_oh = updated_map.get(_ot)
        if not _existing_oh:
            skipped.append({
                "ticker": _ot,
                "reason": "sell-only (no buy in CSV, no existing holding — "
                          "position was opened before the CSV date range; "
                          "entry price unknown so P&L cannot be computed)",
                "lots": _s_lots_v,
            })
            continue
        _oh_lots = list(_existing_oh.get("lots") or [])
        if account_id:
            _oh_same  = [l for l in _oh_lots if not l.get("account_id") or l["account_id"] == account_id]
            _oh_other = [l for l in _oh_lots if l.get("account_id") and l["account_id"] != account_id]
        else:
            _oh_same  = _oh_lots
            _oh_other = []
        if not _oh_same:
            continue
        _oh_total_sell = round(sum(float(l.get("shares") or 0) for l in _s_lots_v), 8)
        _oh_remaining  = _fifo_reduce(_oh_same, _oh_total_sell)
        if mode == "import":
            _oh_base_cost = float(_existing_oh.get("avg_cost") or 0)
            for _sl in sorted(_s_lots_v, key=lambda l: l.get("date") or ""):
                _sl_sh   = float(_sl.get("shares") or 0)
                _sl_ep   = float(_sl.get("price")  or 0)
                _sl_date = _sl.get("date") or _date_cls.today().isoformat()
                _oh_key  = (_ot, str(_existing_oh.get("entry_date") or "")[:10],
                            str(_sl_date)[:10], round(_sl_sh, 2))
                if _oh_key in _existing_ct_keys:
                    continue
                _existing_ct_keys.add(_oh_key)
                _oh_cb   = _oh_base_cost * _sl_sh
                _oh_pnl  = round((_sl_ep - _oh_base_cost) * _sl_sh, 4)
                _oh_pct  = round(_oh_pnl / _oh_cb * 100, 4) if _oh_cb else None
                _oh_full = (len(_oh_remaining) == 0)
                _ct_batch.append({
                    "ticker":                 _ot,
                    "shares":                 _sl_sh,
                    "entry_price":            _oh_base_cost,
                    "exit_price":             _sl_ep,
                    "entry_date":             _existing_oh.get("entry_date"),
                    "exit_date":              _sl_date,
                    "realized_pnl":           _oh_pnl,
                    "realized_pnl_pct":       _oh_pct,
                    "notes":                  "Imported from CSV (sell matched to existing holding)",
                    "sell_type":              "full" if _oh_full else "partial",
                    "is_full_close":          _oh_full,
                    "remaining_shares_after": round(sum(float(l.get("shares", 0)) for l in _oh_remaining), 8),
                    "trade_group_id":         str(_uuid_mod.uuid4()),
                    "cost_method":            "average_cost",
                })
                sells_logged += 1
                if _oh_full:
                    closed_full_count += 1
                else:
                    closed_part_count += 1
        _oh_all_remaining = _oh_other + _oh_remaining
        if _oh_all_remaining:
            _oh_totals = _totals(_oh_all_remaining)
            updated_map[_ot] = {**_existing_oh, "shares": _oh_totals["shares"],
                                "avg_cost": _oh_totals["avg_cost"], "lots": _oh_all_remaining}
            preview.append({"ticker": _ot, "action": "updated", "lots_added": 0,
                            "total_shares": _oh_totals["shares"], "avg_cost": _oh_totals["avg_cost"],
                            "entry_date": _existing_oh.get("entry_date"),
                            "sold_shares": round(_oh_total_sell, 8), "closed": False})
            holdings_updated += 1
        else:
            updated_map.pop(_ot, None)
            preview.append({"ticker": _ot, "action": "closed", "lots_added": 0,
                            "total_shares": 0, "avg_cost": float(_existing_oh.get("avg_cost") or 0),
                            "entry_date": _existing_oh.get("entry_date"), "closed": True})

    # ── Write to DB (import mode only) ────────────────────────────────────────
    any_change = holdings_created or holdings_updated or closed_full_count or closed_part_count
    if mode == "import" and any_change:
        final_holdings = list(updated_map.values())
        _save_h(final_holdings)
        # Flush all collected closed trades in ONE batch INSERT instead of N
        # individual round-trips. This is critical for Gunicorn deployments:
        # 71 individual DB calls took ~43s and caused a worker timeout/SIGKILL
        # which sent an empty response body to the browser.
        if _ct_batch:
            _save_ct_batch(_ct_batch)
        sig = _sig(final_holdings)
        print(f"[PORTFOLIO_CSV] imported {holdings_created} new + {holdings_updated} updated + "
              f"{closed_full_count} fully-closed + {closed_part_count} partial-close, "
              f"{lots_added} lots, {sells_logged} closed-trade records (batch), sig={sig}")

        # Invalidate terminal cache
        try:
            from data.caelyn_terminal import CaelynTerminalProvider
            from data.cache import cache as _app_cache
            from data.portfolio_store import canonical_file as _cf
            _app_cache.delete(CaelynTerminalProvider.cache_key_for(_cf()))
        except Exception:
            pass

        # Trigger earnings sync for still-open tickers only
        try:
            from services.user_earnings_service import (
                invalidate_user_earnings,
                sync_universe_background,
            )
            from config import FMP_API_KEY as _fmp_key_csv
            import asyncio as _aio_csv
            open_syms = {t for t in buy_lots if updated_map.get(t)}
            invalidate_user_earnings("portfolio")
            if open_syms:
                _aio_csv.create_task(sync_universe_background("portfolio", open_syms, _fmp_key_csv or ""))
        except Exception:
            pass

    # ── Build lightweight netting summary (ticker-level only, no lot data) ──────
    # Kept small intentionally — the full lot arrays are NOT included so
    # the summary stays <1 KB regardless of how many positions are imported.
    netting_summary = []
    for ticker, b_lots in buy_lots.items():
        b_valid = [l for l in b_lots if float(l.get("shares") or 0) > 0]
        s_valid = [l for l in sell_lots.get(ticker, []) if float(l.get("shares") or 0) > 0]
        tb = round(sum(float(l["shares"]) for l in b_valid), 4)
        ts = round(sum(float(l["shares"]) for l in s_valid), 4)
        ns = round(tb - ts, 4)
        action_label = "closed" if ns <= 0 else ("partial_sell" if ts > 0 else "open")
        netting_summary.append({
            "ticker":       ticker,
            "buy_lots":     len(b_valid),
            "sell_lots":    len(s_valid),
            "total_bought": tb,
            "total_sold":   ts,
            "net_shares":   max(ns, 0.0),
            "action":       action_label,
        })
    netting_summary.sort(key=lambda x: x["ticker"])

    # Summarise closed positions for the preview card
    closed_preview = [p for p in preview if p.get("closed")]
    open_preview   = [p for p in preview if not p.get("closed")]

    # ── Response size control ─────────────────────────────────────────────────
    # In import mode the response must stay lean (<10 KB) so it passes cleanly
    # through any proxy layer regardless of portfolio size.
    #
    # Large arrays that can grow with position count are OMITTED from import
    # mode responses:
    #   • updated_holdings  — full lot-detail array; frontend uses GET
    #                         /api/portfolio/holdings to refresh instead
    #   • preview           — per-ticker action rows; not needed post-import
    #
    # Both arrays ARE included in preview mode (response is never written to
    # DB and the caller explicitly needs to see the detail).
    if mode == "preview":
        response_updated_holdings = []        # preview never writes to DB
        response_preview          = preview   # caller needs per-ticker detail
    else:
        response_updated_holdings = []        # frontend fetches via GET
        response_preview          = []        # not needed after a real import

    return {
        "success":               True,
        "mode":                  mode,
        "columns_detected":      parsed["columns"],
        "rows_total":            parsed["rows_total"],
        "rows_skipped":          len(skipped),
        # Open positions
        "symbols_imported":      [p["ticker"] for p in open_preview],
        "symbols_closed":        [p["ticker"] for p in closed_preview],
        "holdings_created":      holdings_created,
        "holdings_updated":      holdings_updated,
        "lots_added":            lots_added,
        # Closed trade records
        "sells_found":           sum(len(v) for v in sell_lots.values()),
        "closed_trades_created": sells_logged,
        "closed_full_count":     closed_full_count,
        "closed_partial_count":  closed_part_count,
        "sells_logged":          sells_logged,   # legacy alias
        # full_replace metadata
        "full_replace":          full_replace,
        "full_replace_cleared":  ({"holdings": _fr_prev_holdings, "closed_trades": _fr_prev_ct}
                                  if full_replace and mode == "import" else None),
        # Options
        "options_skipped":       len(parsed.get("options_rows", [])),
        "options_detail":        parsed.get("options_rows", [])[:10],
        # Diagnostics (small — ticker names + numbers only, no lot arrays)
        "action_distribution":   parsed.get("action_distribution", []),
        "netting_summary":       netting_summary,
        # Large arrays — see size-control comment above
        "updated_holdings":      response_updated_holdings,
        "preview":               response_preview,
        "skipped_detail":        skipped[:10],
    }


@app.post("/api/portfolio/transactions/import-csv")
async def portfolio_transactions_import_csv(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """Ledger-based portfolio CSV import — correct accounting engine.

    Replaces the legacy /api/portfolio/upload-csv for full-history imports.
    Uses average-cost accounting, one closed-trade record per sell event, and
    classifies positions only from final ledger state (not per-row or per-month).

    Body (JSON)::

        {
          "csv_data":     str,       # raw CSV text (required)
          "mode":         str,       # "preview" | "import"  (default "import")
          "full_replace": bool,      # wipe all existing holdings+trades first
                                     # (default true for clean-slate import)
          "source_file":  str,       # optional label for diagnostics
          "validate":     bool,      # if true, also run built-in test suite
        }

    Response::

        {
          "success":                   bool,
          "mode":                      str,
          "import_diagnostics":        {...},
          "open_positions":            [...],
          "partially_closed_positions":[...],
          "fully_closed_positions":    [...],
          "closed_trade_records":      [...],
          "monthly_closed_positions":  {"2026-05": [...]},
          "symbol_audit":              {...},
          "test_results":              {...},    # only when validate=true
        }
    """
    if not _jwt_or_key(request, api_key):
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")
    try:
        body = await request.json()
    except Exception as _je:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {_je}")

    csv_data    = (body.get("csv_data") or "").strip()
    if not csv_data:
        raise HTTPException(status_code=400, detail="csv_data is required")

    mode         = (body.get("mode") or "import").lower().strip()
    # Default full_replace=True so a fresh import never inherits stale state
    # from prior bad imports (e.g. wrong is_full_close flags on SIVEF/OUST).
    # Pass full_replace=false explicitly for incremental/append imports.
    full_replace     = bool(body.get("full_replace", True))
    source_file      = (body.get("source_file") or "uploaded_csv").strip()
    run_validate     = bool(body.get("validate", False))
    import uuid as _uuid_mod_early
    _import_batch_id = str(_uuid_mod_early.uuid4())

    if mode not in ("preview", "import"):
        raise HTTPException(status_code=400, detail="mode must be 'preview' or 'import'")

    print(
        f"[portfolio-ledger-import] mode={mode}  full_replace={full_replace}  "
        f"source={source_file!r}  csv_bytes={len(csv_data)}"
    )

    # ── Imports ───────────────────────────────────────────────────────────────
    from services.portfolio_ledger import (
        normalize_csv_rows,
        deduplicate_transactions,
        build_symbol_ledgers,
        classify_positions,
        build_symbol_diagnostics,
        run_ledger_tests,
        SHARE_EPSILON,
        VALUE_EPSILON,
        PCT_EPSILON,
    )
    from data.portfolio_store import (
        load_active_holdings  as _load_h,
        save_active_holdings  as _save_h,
        get_holdings_signature as _sig,
        compute_lot_totals    as _totals,
    )
    from data.closed_trades_store import (
        save_closed_trades_batch        as _save_ct_batch,
        delete_all_closed_trades        as _del_ct_all,
        delete_csv_import_closed_trades as _del_csv_ct,
        load_closed_trades              as _load_ct,
    )
    from datetime import date as _date_cls
    import uuid as _uuid_mod
    from services.option_ledger import (
        normalize_option_rows             as _norm_opt,
        deduplicate_option_transactions   as _dedup_opt,
        build_option_ledgers              as _build_opt,
        classify_option_positions         as _classify_opt,
    )
    from data.option_trades_store import (
        save_option_positions_batch            as _save_opt_pos,
        delete_csv_import_option_positions     as _del_opt_pos,
        save_option_closed_trades_batch        as _save_opt_ct,
        delete_csv_import_option_closed_trades as _del_opt_ct,
    )

    # ── Step 1: Normalize ─────────────────────────────────────────────────────
    try:
        normalized = normalize_csv_rows(csv_data, source_file=source_file)
    except Exception as _e:
        import traceback as _tb
        raise HTTPException(status_code=500,
                            detail=f"CSV parse error: {_e}\n{_tb.format_exc()}")

    all_txns      = normalized["transactions"]
    ignored_rows  = normalized["ignored"]
    unknown_rows  = normalized["unknown_type"]
    rows_total    = normalized["rows_total"]

    buy_sell_txns = [t for t in all_txns if t["side"] in ("BUY", "SELL")]

    # ── Step 1b: Option rows (parsed independently — never mixed with stocks) ──
    try:
        _opt_norm = _norm_opt(csv_data, source_file=source_file)
    except Exception as _oe:
        _opt_norm = {
            "transactions": [], "ignored": [], "errors": [{"reason": str(_oe)}],
            "rows_total": 0, "rows_parsed": 0, "option_rows_detected": 0,
        }
    _opt_unique, _opt_dupes = _dedup_opt(_opt_norm["transactions"])

    # ── Run built-in test suite early so validate=true always returns results ─────
    test_results_early: dict = {}
    if run_validate:
        from services.portfolio_ledger import run_ledger_tests as _rlt
        try:
            test_results_early = _rlt()
        except Exception as _te:
            test_results_early = {"error": str(_te)}

    if not buy_sell_txns:
        return {
            "success":           False,
            "mode":              mode,
            "error":             "No BUY or SELL transactions found. Check that the "
                                 "CSV contains transaction rows with Symbol, Quantity, "
                                 "and a recognisable Action/Type column.",
            "columns_detected":  normalized["columns"],
            "rows_total":        rows_total,
            "ignored_count":     len(ignored_rows),
            "ignored_detail":    ignored_rows[:20],
            "unknown_type_rows": unknown_rows[:10],
            **({"test_results": test_results_early} if run_validate else {}),
        }

    # ── Step 2: Deduplicate ───────────────────────────────────────────────────
    # When full_replace=True we wipe everything first so dedup against DB is
    # moot; still run it to catch intra-CSV duplicates.
    unique_txns, dupe_txns = deduplicate_transactions(buy_sell_txns)

    # ── Orphan-sell resolution ────────────────────────────────────────────────
    # "Orphan sell" = ticker that has SELL rows in the CSV but no BUY rows.
    # This happens when the original buy predates the CSV export window (e.g.
    # you bought SIVEF in 2023, but the exported CSV only covers 2025-2026).
    #
    # Without resolution the ledger engine sees 0 open shares → flags an
    # "oversell" accounting error and caps the sell to zero, losing all P&L.
    #
    # Fix: load existing DB holdings BEFORE any full_replace wipe, then inject
    # a synthetic BUY for each orphan-sell ticker that has a live DB holding.
    # The synthetic lot is tagged so the persistence block can derive the
    # remaining open lots from the DB holding's own lot history rather than
    # from the synthetic transaction itself.
    _pre_import_holdings: list[dict] = _load_h()
    _pre_import_map: dict[str, dict] = {
        h["ticker"].upper(): h for h in _pre_import_holdings
    }

    csv_buy_syms  = {t["symbol"] for t in unique_txns if t["side"] == "BUY"}
    csv_sell_syms = {t["symbol"] for t in unique_txns if t["side"] == "SELL"}

    # Pure orphan sells: ticker has SELL rows but zero BUY rows in the CSV
    orphan_syms   = csv_sell_syms - csv_buy_syms

    # Oversell tickers: ticker has CSV buys but CSV sells exceed CSV buys
    # (e.g. IREN: bought 23, sold 186 — original lot predates the CSV window)
    _csv_qty_bought: dict[str, float] = {}
    _csv_qty_sold:   dict[str, float] = {}
    for _t in unique_txns:
        _s = _t["symbol"]
        if _t["side"] == "BUY":
            _csv_qty_bought[_s] = round(_csv_qty_bought.get(_s, 0) + float(_t.get("quantity") or 0), 8)
        elif _t["side"] == "SELL":
            _csv_qty_sold[_s]   = round(_csv_qty_sold.get(_s, 0)   + float(_t.get("quantity") or 0), 8)
    oversell_syms = {
        _s for _s in (csv_buy_syms & csv_sell_syms)
        if _csv_qty_sold.get(_s, 0) > _csv_qty_bought.get(_s, 0) + 1e-6
    }

    _synthetic_buys: list[dict]  = []
    synthetic_buy_syms: set[str] = set()

    def _make_synthetic_buy(_sym: str, _synth_shares: float, _synth_price: float,
                            _synth_date: str, _reason: str) -> dict:
        return {
            "transaction_id":  str(_uuid_mod.uuid4()),
            "fingerprint":     f"synth_db_{_sym}_{_synth_shares:.4f}_{_synth_price:.4f}",
            "source_file":     "db_holding_synthetic",
            "raw_row_index":   -1,
            "account":         _pre_import_map.get(_sym, {}).get("account", ""),
            "symbol":          _sym,
            "trade_date":      _synth_date,
            "settlement_date": None,
            "action":          "BUY",
            "side":            "BUY",
            "quantity":        _synth_shares,
            "price":           _synth_price,
            "gross_amount":    _synth_shares * _synth_price,
            "fees":            0.0,
            "net_amount":      _synth_shares * _synth_price,
            "currency":        "USD",
            "raw_description": f"Synthetic lot from prior DB holding ({_reason})",
            "normalized_type": "BUY",
            "synthetic":       True,
        }

    # ── Case 1: pure orphan sells — inject full DB holding ─────────────────
    for _sym in orphan_syms:
        _h = _pre_import_map.get(_sym)
        if _h and float(_h.get("shares") or 0) > 0:
            _synth_shares = float(_h["shares"])
            _synth_price  = float(_h.get("avg_cost") or 0)
            _synth_date   = str(_h.get("entry_date") or "2000-01-01")[:10]
            _synthetic_buys.append(
                _make_synthetic_buy(_sym, _synth_shares, _synth_price, _synth_date, "orphan_sell")
            )
            synthetic_buy_syms.add(_sym)
            print(
                f"[portfolio-ledger-import] orphan-sell resolved: {_sym} → "
                f"injected synthetic BUY {_synth_shares} @ {_synth_price} "
                f"(DB entry={_synth_date})"
            )

    # ── Case 2: oversell tickers — inject gap shares from DB ───────────────
    # The CSV has some buys for the ticker but sells exceed them. The gap
    # shares represent a pre-CSV lot. We inject only the gap amount (not the
    # full DB holding) so we don't double-count the CSV buys.
    for _sym in oversell_syms:
        _h = _pre_import_map.get(_sym)
        if _h and float(_h.get("shares") or 0) > 0:
            _gap = round(_csv_qty_sold.get(_sym, 0) - _csv_qty_bought.get(_sym, 0), 8)
            if _gap > 1e-6:
                _synth_price = float(_h.get("avg_cost") or 0)
                _synth_date  = str(_h.get("entry_date") or "2000-01-01")[:10]
                _synthetic_buys.append(
                    _make_synthetic_buy(_sym, _gap, _synth_price, _synth_date, "oversell_gap")
                )
                synthetic_buy_syms.add(_sym)
                print(
                    f"[portfolio-ledger-import] oversell resolved: {_sym} → "
                    f"injected synthetic BUY {_gap} @ {_synth_price} "
                    f"(gap={_gap}, DB entry={_synth_date})"
                )

    # Pass CSV transactions + synthetic opening lots to the ledger engine
    txns_for_ledger = unique_txns + _synthetic_buys

    # ── Step 3 & 4: Ledger + classify ─────────────────────────────────────────
    ledgers = build_symbol_ledgers(txns_for_ledger)
    result  = classify_positions(ledgers)

    open_positions    = result["open_positions"]
    partially_closed  = result["partially_closed_positions"]
    fully_closed      = result["fully_closed_positions"]
    closed_events     = result["closed_trade_records"]
    monthly           = result["monthly_closed_positions"]
    symbol_audit      = result["symbol_audit"]

    # ── Step 3b: Option ledger + classify ─────────────────────────────────────
    _opt_ledgers    = _build_opt(_opt_unique)
    _opt_classified = _classify_opt(_opt_ledgers)
    _opt_open       = _opt_classified["open_positions"]
    _opt_partial    = _opt_classified["partially_closed_positions"]
    _opt_fc         = _opt_classified["fully_closed_positions"]
    _opt_ct         = _opt_classified["closed_trade_records"]
    print(
        f"[portfolio-csv-options] detected={_opt_norm['option_rows_detected']} "
        f"parsed={_opt_norm['rows_parsed']} "
        f"open={len(_opt_open)} partial={len(_opt_partial)} "
        f"fully_closed={len(_opt_fc)} closed_trades={len(_opt_ct)}"
    )

    # ── Final symbol status map — used for closed trade tagging ───────────────
    # Determines the ticker's definitive state AFTER the full import is processed.
    # This is written onto every closed trade record for that ticker so the
    # frontend knows whether a sell event belongs to an open, partially-closed,
    # or fully-closed ticker — without needing to re-run the ledger.
    _open_syms    = {p["symbol"] for p in open_positions}
    _partial_syms = {p["symbol"] for p in partially_closed}
    _closed_syms  = {p["symbol"] for p in fully_closed}
    def _final_status(sym: str) -> str:
        if sym in _partial_syms:
            return "partially_closed_open"
        if sym in _open_syms:
            return "open"
        if sym in _closed_syms:
            return "fully_closed"
        return "fully_closed"   # sell-only ticker with no surviving open lot

    # ── Diagnostics log ───────────────────────────────────────────────────────
    acct_errors = [
        e for ledger in ledgers.values()
        for e in ledger.get("accounting_errors", [])
    ]

    # ── Basis-source classification ────────────────────────────────────────
    # For every symbol that appears in the CSV (buy or sell), determine where
    # the cost basis came from so callers know which P&L figures are reliable.
    #
    #   csv_lot            — all buys in this CSV; P&L is fully computable
    #   existing_db_holding — orphan/oversell resolved via a pre-import holding
    #   manual_required    — sell has no matching buy in CSV or DB; P&L unknown

    # Tickers where synthetic injection succeeded (orphan OR oversell)
    _resolved_syms: set[str] = synthetic_buy_syms

    # Tickers that are UNRESOLVED:
    #   pure orphans with no DB match
    _unresolved_orphan_syms: set[str] = orphan_syms - _resolved_syms
    #   oversell tickers where DB had no holding to cover the gap
    _unresolved_oversell_syms: set[str] = oversell_syms - _resolved_syms
    _needs_basis_review: set[str] = _unresolved_orphan_syms | _unresolved_oversell_syms

    # Build per-symbol basis source map
    _all_csv_syms: set[str] = csv_buy_syms | csv_sell_syms
    _basis_source: dict[str, str] = {}
    for _s in _all_csv_syms:
        if _s in _resolved_syms:
            _basis_source[_s] = "existing_db_holding"
        elif _s in _needs_basis_review:
            _basis_source[_s] = "manual_required"
        else:
            _basis_source[_s] = "csv_lot"

    # ── Enrich category lists with basis_source (computed above) ──────────────
    # This is done here rather than inside classify_positions() because
    # basis_source requires knowledge of orphan/oversell resolution context
    # that is only available at the import-endpoint level.
    for _pos in open_positions:
        _pos["basis_source"] = _basis_source.get(_pos["symbol"], "csv_lot")
    for _pos in partially_closed:
        _pos["basis_source"] = _basis_source.get(_pos["symbol"], "csv_lot")
    for _pos in fully_closed:
        _pos["basis_source"] = _basis_source.get(_pos["symbol"], "csv_lot")

    # Overall accuracy status
    if not _needs_basis_review:
        _accuracy_status = "complete"
    elif len(_needs_basis_review) < len(_all_csv_syms):
        _accuracy_status = "partial_missing_basis"
    else:
        _accuracy_status = "failed"

    # User-facing diagnostic message (only when there are unresolved orphans)
    _basis_warning: str | None = None
    if _needs_basis_review:
        _basis_warning = (
            f"Some sell transactions ({', '.join(sorted(_needs_basis_review))}) "
            "have no matching buy in the uploaded CSV or existing portfolio. "
            "Upload an earlier transaction CSV or enter starting cost basis "
            "to calculate realized P&L."
        )

    diag = {
        "source_file":                    source_file,
        "rows_total":                     rows_total,
        "normalized_transactions":        len(all_txns),
        "buy_sell_transactions":          len(buy_sell_txns),
        "duplicate_transactions":         len(dupe_txns),
        "ignored_rows":                   len(ignored_rows),
        "unknown_type_rows":              len(unknown_rows),
        "accounting_errors":              len(acct_errors),
        # Orphan-sell resolution (sell in CSV, NO buy in CSV)
        "orphan_sells_detected":          len(orphan_syms),
        "orphan_sells_resolved":          len(orphan_syms & _resolved_syms),
        "orphan_sells_unresolved":        len(_unresolved_orphan_syms),
        "orphan_sells_unresolved_symbols": sorted(_unresolved_orphan_syms),
        # Oversell resolution (buy in CSV but sell > buy)
        "oversell_detected":              len(oversell_syms),
        "oversell_resolved":              len(oversell_syms & _resolved_syms),
        "oversell_unresolved":            len(_unresolved_oversell_syms),
        "oversell_unresolved_symbols":    sorted(_unresolved_oversell_syms),
        # Combined needs-review set
        "needs_basis_review_symbols":     sorted(_needs_basis_review),
        # Accuracy status
        "import_accuracy_status":         _accuracy_status,
        # Position counts
        "open_count":                     len(open_positions),
        "partially_closed_count":         len(partially_closed),
        "fully_closed_count":             len(fully_closed),
        "closed_trades_count":            len(closed_events),
    }
    print(f"[portfolio-csv-import] {diag}")

    # ── Per-symbol detailed diagnostics (OPTX/NBIS always logged; all others logged) ──
    _DEBUG_SYMBOLS = {"OPTX", "NBIS", "SIVEF", "OUST", "ALMU"}
    _symbol_diagnostics: dict[str, dict] = {}
    for sym, audit in symbol_audit.items():
        ledger = ledgers.get(sym, {})
        _diag = build_symbol_diagnostics(
            symbol=sym,
            ledger=ledger,
            audit=audit,
            final_symbol_status=_final_status(sym),
        )
        _symbol_diagnostics[sym] = _diag
        # Always log to stdout; verbose for watch symbols
        _log_body = (
            f"\"symbol\":\"{sym}\", "
            f"\"buys\":{audit['buys']}, "
            f"\"sells\":{audit['sells']}, "
            f"\"total_bought_shares\":{_diag['total_bought_shares']}, "
            f"\"total_sold_shares\":{_diag['total_sold_shares']}, "
            f"\"shares_remaining_raw\":{_diag['shares_remaining_raw']}, "
            f"\"shares_remaining_after_tolerance\":{_diag['shares_remaining_after_tolerance']}, "
            f"\"cost_basis_remaining_raw\":{_diag['cost_basis_remaining_raw']}, "
            f"\"cost_basis_remaining_after_tolerance\":{_diag['cost_basis_remaining_after_tolerance']}, "
            f"\"fees_total\":{_diag['fees_total']}, "
            f"\"residual_pct\":{_diag['residual_pct']}, "
            f"\"has_real_sell\":{_diag['has_real_sell']}, "
            f"\"is_dust\":{_diag['is_dust']}, "
            f"\"classification_before_tolerance\":\"{_diag['classification_before_tolerance']}\", "
            f"\"classification_after_tolerance\":\"{_diag['classification_after_tolerance']}\", "
            f"\"final_symbol_status\":\"{_diag['final_symbol_status']}\", "
            f"\"reason\":\"{_diag['reason']}\""
        )
        print(f"[portfolio-csv-symbol-audit] {{{_log_body}}}")

    # ── Step 5: Persist (import mode only) ────────────────────────────────────
    if mode == "import":
        # Re-use the pre-import snapshot loaded earlier for orphan-sell
        # resolution — avoids a redundant DB round-trip and guarantees we
        # wipe *after* we have the data we need.
        existing_holdings = _pre_import_holdings

        if full_replace:
            # Wipe only CSV-imported closed trades (source='csv_import' or legacy NULL).
            # Manually-entered trades (source='manual') are preserved.
            prev_ct = _del_csv_ct()
            _save_h([])
            existing_holdings = []
            print(f"[portfolio-ledger-import] full_replace: wiped holdings + "
                  f"{prev_ct} csv-import closed trades (manual records preserved)")

        existing_map = {h["ticker"].upper(): h for h in existing_holdings}

        # ── Build holdings from open positions ────────────────────────────
        # Derive lots from the buy transactions for each open symbol,
        # then FIFO-reduce by shares_sold so the lot list matches net shares.
        def _fifo_reduce_lots(b_txns: list[dict], shares_sold: float) -> list[dict]:
            """Remove sold shares from oldest buy lots first. Returns open lots."""
            sorted_b = sorted(b_txns, key=lambda t: t.get("trade_date") or "")
            remaining: list[dict] = []
            left = round(float(shares_sold), 8)
            for txn in sorted_b:
                lot_shares = round(float(txn.get("quantity") or 0), 8)
                if left <= 0:
                    remaining.append({
                        "date":   txn.get("trade_date") or _date_cls.today().isoformat(),
                        "shares": lot_shares,
                        "price":  float(txn.get("price") or 0),
                        "notes":  f"CSV import ({source_file})",
                    })
                elif lot_shares <= left + 1e-9:
                    left = round(left - lot_shares, 8)   # lot fully consumed
                else:
                    remaining.append({
                        "date":   txn.get("trade_date") or _date_cls.today().isoformat(),
                        "shares": round(lot_shares - left, 8),
                        "price":  float(txn.get("price") or 0),
                        "notes":  f"CSV import ({source_file})",
                    })
                    left = 0
            return remaining

        def _fifo_reduce_db_lots(db_lots: list[dict], shares_sold: float) -> list[dict]:
            """FIFO-reduce DB holding's own lot records (not CSV txn dicts)."""
            sorted_lots = sorted(db_lots, key=lambda l: str(l.get("date") or ""))
            remaining: list[dict] = []
            left = round(float(shares_sold), 8)
            for lot in sorted_lots:
                lot_shares = round(float(lot.get("shares") or 0), 8)
                if left <= 0:
                    remaining.append(lot)
                elif lot_shares <= left + 1e-9:
                    left = round(left - lot_shares, 8)
                else:
                    remaining.append({**lot, "shares": round(lot_shares - left, 8)})
                    left = 0
            return remaining

        # Build symbol → buy transactions lookup (exclude synthetic lots —
        # their lot derivation is handled via the DB holding's own records).
        buy_by_sym: dict[str, list[dict]] = {}
        for txn in txns_for_ledger:
            if txn["side"] == "BUY" and not txn.get("synthetic"):
                buy_by_sym.setdefault(txn["symbol"], []).append(txn)

        updated_map = {k: dict(v) for k, v in existing_map.items()}
        holdings_created = 0
        holdings_updated = 0

        for pos in open_positions:
            sym = pos["symbol"]
            ledger = ledgers.get(sym, {})
            shares_remaining = pos["shares"]
            shares_sold      = ledger.get("shares_sold", 0.0)

            # ── Lot derivation ─────────────────────────────────────────────
            if sym in synthetic_buy_syms:
                # Orphan-sell ticker: opening position came from the DB.
                # FIFO-reduce the DB holding's existing lots by CSV sells so
                # the surviving lot list exactly matches shares_remaining.
                db_h    = _pre_import_map.get(sym, {})
                db_lots = list(db_h.get("lots") or [])
                if db_lots:
                    open_lots = _fifo_reduce_db_lots(db_lots, shares_sold)
                else:
                    # DB has no granular lots — synthesise one lot at avg_cost
                    open_lots = [{
                        "date":   db_h.get("entry_date") or _date_cls.today().isoformat(),
                        "shares": shares_remaining,
                        "price":  float(db_h.get("avg_cost") or 0),
                        "notes":  "Orphan-sell: DB holding basis",
                    }]
            else:
                b_txns = buy_by_sym.get(sym, [])
                open_lots = _fifo_reduce_lots(b_txns, shares_sold) if b_txns else [{
                "date":   pos.get("entry_date") or _date_cls.today().isoformat(),
                "shares": shares_remaining,
                "price":  pos.get("avg_cost") or 0.0,
                "notes":  f"CSV import ({source_file})",
            }]

            existing = updated_map.get(sym)
            if existing and not full_replace:
                # Merge: keep other-source lots, add new CSV lots (deduped)
                base_lots = list(existing.get("lots") or [])
                seen_keys: set[tuple] = set()
                merged: list[dict] = []
                for lot in base_lots:
                    k = (
                        str(lot.get("date") or "")[:10],
                        round(float(lot.get("shares") or 0), 4),
                        round(float(lot.get("price") or 0), 4),
                    )
                    seen_keys.add(k)
                    merged.append(lot)
                for lot in open_lots:
                    k = (
                        str(lot.get("date") or "")[:10],
                        round(float(lot.get("shares") or 0), 4),
                        round(float(lot.get("price") or 0), 4),
                    )
                    if k not in seen_keys:
                        merged.append(lot)
                        seen_keys.add(k)
                final_lots = merged
            else:
                final_lots = open_lots

            totals = _totals(final_lots)
            updated_map[sym] = {
                "ticker":         sym,
                "shares":         totals["shares"],
                "avg_cost":       totals["avg_cost"],
                "entry_date":     (
                    totals.get("entry_date")
                    or (existing or {}).get("entry_date")
                    or pos.get("entry_date")
                    or _date_cls.today().isoformat()
                ),
                "asset_type":     (existing or {}).get("asset_type", "stock"),
                "lots":           final_lots,
                # Classification written at import time so GET /api/portfolio/holdings
                # can serve the correct dashboard category without re-running the ledger.
                # Values: "partially_closed_open" | "open"
                "classification":      _final_status(sym),
                "final_symbol_status": _final_status(sym),
                "basis_source":        _basis_source.get(sym, "csv_lot"),
                "import_batch_id": _import_batch_id,
                "source_file":    source_file,
            }
            if (existing or {}).get("notes"):
                updated_map[sym]["notes"] = existing["notes"]

            if existing:
                holdings_updated += 1
            else:
                holdings_created += 1

        # Remove fully-closed positions from holdings (if full_replace=False,
        # only remove those that appeared in this CSV).
        for pos in fully_closed:
            sym = pos["symbol"]
            if sym in updated_map and full_replace:
                updated_map.pop(sym, None)
            elif sym in updated_map and sym in {l["symbol"] for l in ledgers.values()}:
                updated_map.pop(sym, None)

        final_holdings = list(updated_map.values())
        _save_h(final_holdings)

        # ── Save closed trade records (one per sell event) ────────────────
        ct_batch: list[dict] = []
        for ev in closed_events:
            _sym = (ev.get("symbol") or ev.get("ticker") or "").upper()
            trade_group_id = str(_uuid_mod.uuid4())
            ct_batch.append({
                "ticker":                 _sym,
                "shares":                 ev.get("shares_sold") or ev.get("shares"),
                "entry_price":            ev.get("avg_cost_at_sale") or ev.get("entry_price"),
                "exit_price":             ev.get("exit_price"),
                "entry_date":             ev.get("entry_date"),
                "exit_date":              ev.get("exit_date"),
                "realized_pnl":           ev.get("realized_pnl"),
                "realized_pnl_pct":       ev.get("realized_pnl_pct"),
                "notes":                  ev.get("notes", f"Imported from CSV ({source_file})"),
                "sell_type":              ev.get("sell_type") or ev.get("close_type"),
                "is_full_close":          bool(ev.get("is_full_close")),
                "remaining_shares_after": ev.get("remaining_shares_after"),
                "trade_group_id":         trade_group_id,
                "cost_method":            "average_cost",
                # ── Source-tracking fields ─────────────────────────────────
                "source":                 "csv_import",
                "import_batch_id":        _import_batch_id,
                "source_file":            source_file,
                "final_symbol_status":    _final_status(_sym),
                "basis_source":           _basis_source.get(_sym, "csv_lot"),
            })
        if ct_batch:
            saved_ct = _save_ct_batch(ct_batch)
        else:
            saved_ct = 0

        sig = _sig(final_holdings)
        print(
            f"[portfolio-ledger-import] persisted: "
            f"holdings_created={holdings_created} "
            f"holdings_updated={holdings_updated} "
            f"closed_trades_saved={saved_ct}  sig={sig}"
        )

        # ── Persist option positions and closed trades ─────────────────────
        if full_replace:
            _del_opt_pos()
            _del_opt_ct()

        # Annotate option positions with batch metadata before saving
        def _annotate_opt(rec: dict) -> dict:
            return {
                **rec,
                "source":          "csv_import",
                "import_batch_id": _import_batch_id,
                "source_file":     source_file,
            }

        _opt_pos_to_save = [_annotate_opt(p) for p in _opt_open]
        _opt_ct_to_save  = [_annotate_opt(t) for t in _opt_ct]
        _saved_opt_pos   = _save_opt_pos(_opt_pos_to_save)
        _saved_opt_ct    = _save_opt_ct(_opt_ct_to_save)
        print(
            f"[portfolio-csv-options] persisted: "
            f"positions={_saved_opt_pos} closed_trades={_saved_opt_ct}"
        )

        # Invalidate downstream caches
        try:
            from data.caelyn_terminal import CaelynTerminalProvider
            from data.cache import cache as _app_cache
            from data.portfolio_store import canonical_file as _cf
            _app_cache.delete(CaelynTerminalProvider.cache_key_for(_cf()))
        except Exception:
            pass

        # Trigger earnings sync for open tickers
        try:
            from services.user_earnings_service import (
                invalidate_user_earnings,
                sync_universe_background,
            )
            from config import FMP_API_KEY as _fmp_key
            import asyncio as _aio
            open_syms = {p["symbol"] for p in open_positions}
            invalidate_user_earnings("portfolio")
            if open_syms:
                _aio.create_task(sync_universe_background(
                    "portfolio", open_syms, _fmp_key or ""))
        except Exception:
            pass

        diag.update({
            "holdings_created":    holdings_created,
            "holdings_updated":    holdings_updated,
            "closed_trades_saved": saved_ct,
            "option_positions_saved":     _saved_opt_pos,
            "option_closed_trades_saved": _saved_opt_ct,
        })

    # ── Optional: run built-in test suite ─────────────────────────────────────
    test_results: dict = {}
    if run_validate:
        try:
            test_results = run_ledger_tests()
        except Exception as _te:
            test_results = {"error": str(_te)}

    # ── Universal symbol classification report (all symbols in CSV) ───────────
    # No ticker-specific hardcoding: covers every symbol seen in this import.
    # Derived entirely from ledger math + computed fields.
    named_report = {}
    for _sym, _audit in symbol_audit.items():
        named_report[_sym] = {
            "found_in_csv":              True,
            "buys":                      _audit["buys"],
            "sells":                     _audit["sells"],
            "shares_bought":             _audit["shares_bought"],
            "shares_sold":               _audit["shares_sold"],
            "shares_remaining":          _audit["shares_remaining"],
            "cost_basis_remaining":      _audit["cost_basis_remaining"],
            "sell_events_count":         len(ledgers.get(_sym, {}).get("closed_events", [])),
            "final_symbol_status":       _final_status(_sym),
            "classification":            _audit["classification"],
            "basis_source":              _basis_source.get(_sym, "csv_lot"),
            "appears_in_open_positions":          _audit["appears_in_open"],
            "appears_in_partially_closed_positions": _audit["appears_in_partial"],
            "appears_in_fully_closed_positions":   _audit["appears_in_fully_closed"],
            "appears_in_closed_trades":  _audit["has_real_sell"],
            "is_dust":                   _audit["is_dust"],
            "reason": (
                ledgers.get(_sym, {}).get("closed_events", [{}])[-1].get("notes", "")
                if ledgers.get(_sym, {}).get("closed_events") else
                _audit["classification"]
            ),
        }

    _opt_diag = {
        "option_rows_detected":      _opt_norm["option_rows_detected"],
        "option_transactions_normalized": _opt_norm["rows_parsed"],
        "option_duplicate_rows":     len(_opt_dupes),
        "option_parse_errors":       len(_opt_norm["errors"]),
        "option_open_count":         len(_opt_open),
        "option_partially_closed_count": len(_opt_partial),
        "option_fully_closed_count": len(_opt_fc),
        "option_closed_trades_count": len(_opt_ct),
        "option_errors":             _opt_norm["errors"][:10],
    }

    return {
        "success":                    True,
        "mode":                       mode,
        "import_batch_id":            _import_batch_id,
        "import_diagnostics":         diag,
        "basis_source_by_symbol":     _basis_source,
        "import_accuracy_status":     _accuracy_status,
        **({"basis_warning": _basis_warning} if _basis_warning else {}),
        "named_symbol_report":        named_report,
        "open_positions":             open_positions,
        "partially_closed_positions": partially_closed,
        "fully_closed_positions":     fully_closed,
        "closed_trade_records":       closed_events,
        "monthly_closed_positions":   monthly,
        "symbol_audit":               symbol_audit,
        "symbol_diagnostics":         _symbol_diagnostics,
        "accounting_errors":          acct_errors,
        "tolerance_config":           {
            "SHARE_EPSILON": SHARE_EPSILON,
            "VALUE_EPSILON": VALUE_EPSILON,
            "PCT_EPSILON":   PCT_EPSILON,
        },
        "unknown_type_rows":          unknown_rows[:20],
        "ignored_detail":             ignored_rows[:10],
        # ── Option outputs ─────────────────────────────────────────────────
        "option_open_positions":             _opt_open,
        "option_partially_closed_positions": _opt_partial,
        "option_fully_closed_positions":     _opt_fc,
        "option_closed_trades":              _opt_ct,
        "option_import_diagnostics":         _opt_diag,
        **({"test_results": test_results} if run_validate else {}),
    }


@app.get("/api/portfolio/options-positions")
async def get_option_positions(request: Request, api_key: str = Header(None, alias="X-API-Key")):
    """Return all option positions from the DB, split by status.

    Response::

        {
          "open_positions":             [...],  # open + partially-closed
          "partially_closed_positions": [...],  # subset with some sells
          "fully_closed_positions":     [...],  # fully closed / expired
          "all_positions":              [...],  # every row unfiltered
        }
    """
    if not _jwt_or_key(request, api_key):
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")
    from data.option_trades_store import load_option_positions as _lop
    rows = _lop()
    open_pos:    list[dict] = []
    partial_pos: list[dict] = []
    fc_pos:      list[dict] = []
    for r in rows:
        st = r.get("final_status", "open")
        if st in ("open", "partially_closed_open", "short_option_tracked_basic"):
            open_pos.append(r)
            if st == "partially_closed_open":
                partial_pos.append(r)
        elif st in ("fully_closed", "expired", "orphan_expired"):
            fc_pos.append(r)
    return {
        "open_positions":             open_pos,
        "partially_closed_positions": partial_pos,
        "fully_closed_positions":     fc_pos,
        "all_positions":              rows,
    }


@app.get("/api/portfolio/options-trades")
async def get_option_trades(request: Request, api_key: str = Header(None, alias="X-API-Key")):
    """Return all option closed-trade events from the DB.

    Response::

        {
          "option_closed_trades": [...],
          "count": int,
        }
    """
    if not _jwt_or_key(request, api_key):
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")
    from data.option_trades_store import load_option_closed_trades as _loct
    trades = _loct()
    return {
        "option_closed_trades": trades,
        "count":                len(trades),
    }


@app.post("/api/portfolio/sync")
async def portfolio_sync(request: Request):
    """Maximally-permissive portfolio sync endpoint.

    Accepts any of:
      - {holdings: [...]}
      - {positions: [...]}
      - bare array [...]

    Accepts any field names: ticker|symbol, shares|quantity,
    avg_cost|avgCost|average_cost|cost_basis, asset_type|assetType|type.
    No auth required. No size guard. Always writes if incoming non-empty.
    Designed so the frontend can verify FastAPI connectivity and push holdings.
    """
    import json as _json
    from data.portfolio_store import save_active_holdings as _save, canonical_file as _cf, load_active_holdings as _load

    raw = await request.body()
    print(f"[portfolio-sync] received  len={len(raw)}  preview={raw[:300]!r}")

    try:
        body = _json.loads(raw)
    except Exception as e:
        print(f"[portfolio-sync] JSON parse error: {e}")
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {e}"})

    # Extract list regardless of shape
    if isinstance(body, list):
        raw_list = body
    elif isinstance(body, dict):
        raw_list = body.get("holdings") or body.get("positions") or body.get("data") or []
    else:
        return JSONResponse(status_code=400, content={"error": "Body must be object or array"})

    if not isinstance(raw_list, list):
        return JSONResponse(status_code=400, content={"error": "Holdings must be an array"})

    # Normalize fields
    normalized = []
    for h in raw_list:
        if not isinstance(h, dict):
            continue
        ticker = (h.get("ticker") or h.get("symbol") or "").strip().upper()
        if not ticker:
            continue
        _ed2 = h.get("entry_date") or h.get("date_added") or None
        _e2: dict = {
            "ticker":     ticker,
            "shares":     float(h.get("shares") or h.get("quantity") or 0),
            "avg_cost":   float(h.get("avg_cost") or h.get("avgCost") or h.get("average_cost") or h.get("cost_basis") or 0),
            "asset_type": (h.get("asset_type") or h.get("assetType") or h.get("type") or "stock").lower(),
        }
        if _ed2:
            _e2["entry_date"] = _ed2
        normalized.append(_e2)

    prev_count = len(_load())
    symbols = [h["ticker"] for h in normalized]

    if not normalized:
        return JSONResponse(content={
            "synced": False,
            "reason": "incoming_empty",
            "canonical_count": prev_count,
        })

    _save(normalized)

    # Invalidate Terminal cache
    try:
        from data.caelyn_terminal import CaelynTerminalProvider
        from data.cache import cache as _app_cache
        _app_cache.delete(CaelynTerminalProvider.cache_key_for(_cf()))
    except Exception:
        pass

    # Patch full-year earnings cache (add new tickers, drop removed ones)
    try:
        from services.pfull_year_service import patch_pfull_cache as _patch_pfull_sync
        from config import FMP_API_KEY as _fmp_key_sync
        import asyncio as _aio_sync
        _aio_sync.create_task(_patch_pfull_sync(set(symbols), _fmp_key_sync or ""))
        print(f"[portfolio-sync] pfull_cache_patch_scheduled  count={len(symbols)}")
    except Exception as _pfull_sync_err:
        print(f"[portfolio-sync] pfull_cache_patch_schedule_failed (non-fatal): {_pfull_sync_err}")

    print(f"[portfolio-sync] saved  count={len(normalized)}  symbols={symbols[:25]}")

    return JSONResponse(content={
        "synced": True,
        "canonical_count": len(normalized),
        "canonical_symbols": symbols,
        "prev_count": prev_count,
        "source_file": str(_cf()),
    })


@app.post("/api/portfolio/holdings/migrate-from-client")
async def migrate_holdings_from_client(request: Request, api_key: str = Header(None, alias="X-API-Key")):
    """One-time migration endpoint: the frontend posts its localStorage holdings
    here to persist them to the canonical backend source.

    Body: {holdings: [{ticker, shares, avg_cost, asset_type?, ...}]}

    Returns current canonical count and symbols so the frontend can verify.
    Only writes if the incoming list is non-empty and LARGER than what the
    backend already has (prevents accidental overwrite with an empty array).
    Use force=true in the body to override the size guard.
    """
    from data.portfolio_store import (
        load_active_holdings as _load,
        save_active_holdings as _save,
        canonical_file as _cf,
    )
    user_id = getattr(request.state, "user_id", "default")
    body = await request.json()
    if not isinstance(body, dict) or "holdings" not in body:
        raise HTTPException(status_code=400, detail="Body must be {holdings: [...]}")

    incoming = [h for h in body["holdings"] if isinstance(h, dict) and (h.get("ticker") or h.get("symbol"))]
    force    = bool(body.get("force", False))
    existing = _load()

    if not incoming:
        return {
            "migrated": False,
            "reason":   "incoming_empty",
            "canonical_count":   len(existing),
            "canonical_symbols": [h.get("ticker") for h in existing],
        }

    if not force and len(incoming) <= len(existing):
        return {
            "migrated": False,
            "reason":   f"incoming ({len(incoming)}) not larger than existing ({len(existing)}) — pass force=true to override",
            "canonical_count":   len(existing),
            "canonical_symbols": [h.get("ticker") for h in existing],
        }

    _save(incoming)
    new_syms = [(h.get("ticker") or h.get("symbol") or "").upper().strip() for h in incoming]

    # Invalidate Terminal cache
    try:
        from data.caelyn_terminal import CaelynTerminalProvider
        from data.cache import cache as _app_cache
        _app_cache.delete(CaelynTerminalProvider.cache_key_for(_cf()))
    except Exception:
        pass

    print(
        f"[portfolio-source-audit] migrate-from-client  user_id={user_id}  "
        f"count={len(incoming)}  symbols={new_syms[:20]}  force={force}"
    )
    return {
        "migrated":          True,
        "canonical_count":   len(incoming),
        "canonical_symbols": new_syms,
        "source_file":       str(_cf()),
    }


@app.get("/api/portfolio/source-audit")
async def portfolio_source_audit(request: Request, api_key: str = Header(None, alias="X-API-Key")):
    """Return a full audit of all portfolio-related files: canonical + legacy.
    Shows count, symbols, modified_at, and whether each file is active or archived.
    """
    from data.portfolio_store import startup_audit as _audit
    return _audit()


# ============================================================
# Portfolio Fundamentals (/api/portfolio/fundamentals)
# ============================================================

def _pf_row_from_cache(sym: str, neon_row: dict) -> dict:
    """Reconstruct a fundamental row from a screener_fundamentals_cache Neon row.

    Stored profile uses FMP-native camelCase keys; translate to snake_case so
    _build_fundamental_row can consume it.  Income/balance/cashflow are not stored
    in the cache so those statement fields (revenue, net_income, etc.) will be null —
    data_quality will reflect partial coverage for cached rows.
    """
    from services.social_screener_service import _build_fundamental_row as _bfr
    raw_profile = neon_row.get("profile") or {}
    ratios      = neon_row.get("ratios")  or {}
    metrics     = neon_row.get("metrics") or {}

    profile_snake = {
        "company_name": (
            raw_profile.get("companyName")
            or raw_profile.get("company_name")
            or ""
        ),
        "market_cap": (
            neon_row.get("market_cap")
            or raw_profile.get("marketCap")
            or raw_profile.get("market_cap")
        ),
    }

    row = _bfr(sym, profile_snake, ratios, metrics, {}, {}, {})
    row["sector"]        = neon_row.get("sector")   or raw_profile.get("sector")   or ""
    row["industry"]      = neon_row.get("industry") or raw_profile.get("industry") or ""
    row["_cache_source"] = "neon"
    row["fetched_at"]    = neon_row.get("fetched_at")
    return row


@app.get("/api/portfolio/fundamentals")
async def portfolio_fundamentals(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
    force_refresh: bool = Query(False),
):
    """FMP fundamentals for every current open stock holding + option underlyings.

    Cache strategy (shared with Social Screener — no new table):
      screener_fundamentals_cache (Neon), 7-day TTL.
      Cache hit  → serve from Neon, zero FMP calls.
      Cache miss → fetch all 6 FMP endpoints via fetch_enrichment_for_symbols,
                   write-through to Neon, return fresh rows.

    force_refresh=true  → treat ALL canonical portfolio symbols as stale,
                          re-fetch from FMP, update Neon cache.  Use for the
                          manual Refresh button on the Portfolio Fundamentals tab.

    Symbol universe: open equity holdings (shares > 0) + open option underlyings.
    OCC contract IDs are never included — only underlying tickers.
    Response shape mirrors Social Screener Fundamental toggle for frontend reuse.
    """
    if not _jwt_or_key(request, api_key):
        raise HTTPException(status_code=403, detail="Forbidden")

    import time as _pf_time
    _t0 = _pf_time.monotonic()

    from data.portfolio_store import load_active_holdings as _load_holdings
    from data.screener_hub_store import (
        get_fundamentals                  as _get_fund,
        fundamentals_cached_symbols       as _cached_syms,
    )
    from services.social_screener_service import (
        fetch_enrichment_for_symbols as _fetch_enrich,
    )
    from config import FMP_API_KEY as _fmp_key

    _TTL_PORTFOLIO_FUND = 7 * 24 * 3600   # 7 days — matches Social Screener TTL

    # ── 1. Current open stock/equity holdings ──────────────────────────────
    holdings = _load_holdings()
    equity_symbols: list[str] = sorted({
        (h.get("ticker") or h.get("symbol") or "").upper().strip()
        for h in holdings
        if isinstance(h, dict)
        and (h.get("ticker") or h.get("symbol"))
        and float(h.get("shares", 0) or 0) > 0
    })
    equity_symbols = [s for s in equity_symbols if s]

    # ── 1b. Option underlying symbols (from open option positions) ──────────
    # Fetch open option positions for counts and underlying symbols
    from data.option_trades_store import (
        load_option_positions      as _lop_fund,
        load_open_option_underlyings as _opt_unds_fund,
    )
    _OPEN_STATUS_FUND = {"open", "partially_closed_open", "short_option_tracked_basic"}
    _all_opt_positions = _lop_fund()
    _open_opt_positions = [p for p in _all_opt_positions if p.get("final_status") in _OPEN_STATUS_FUND]
    _opt_underlyings: set[str] = {
        (p.get("underlying") or "").upper().strip()
        for p in _open_opt_positions
        if (p.get("underlying") or "").strip()
    }

    # Counts: each option row is 1 position (not contracts_open)
    _equity_position_count = len(equity_symbols)
    _option_position_count = len(_open_opt_positions)
    _total_position_count  = _equity_position_count + _option_position_count

    # Merged symbol list for fundamentals — option underlyings that are already
    # equity holdings are deduped (no double-fetch from FMP)
    _opt_extra = sorted(_opt_underlyings - set(equity_symbols))
    open_symbols: list[str] = sorted(set(equity_symbols) | _opt_underlyings)

    _now_iso = _dt.now(_tz.utc).isoformat()
    _empty_cache_meta = {
        "source": "neon",
        "hit_count": 0,
        "miss_count": 0,
        "stale_count": 0,
        "ttl_seconds": _TTL_PORTFOLIO_FUND,
        "updated_at": _now_iso,
    }
    if not open_symbols:
        return JSONResponse({
            "holdings_count":         0,
            "equity_position_count":  _equity_position_count,
            "option_position_count":  _option_position_count,
            "total_position_count":   _total_position_count,
            "unique_symbol_count":    0,
            "symbols":                [],
            "rows":                   [],
            "cache":                  _empty_cache_meta,
            "unavailable":            [],
            "unavailable_symbols":    [],
        })

    # ── 2. Cache-exists check (no TTL for portfolio fundamentals) ────────────
    # Portfolio policy: treat any usable cached row as fresh indefinitely.
    # FMP is only called for symbols with NO cached row, or on force_refresh.
    # Social Screener keeps its own 7-day TTL path via fundamentals_fresh_symbols().
    if force_refresh:
        cached_set: set[str] = set()          # bypass cache for all symbols
    else:
        cached_set: set[str] = _cached_syms(open_symbols)
    stale_set: set[str] = set(open_symbols) - cached_set
    fresh_set = cached_set                    # alias kept for downstream references

    # ── 3. Live FMP fetch for cache misses / stale symbols ─────────────────
    live_enrichment: dict[str, dict] = {}
    unavailable: list[dict] = []

    if stale_set:
        if _fmp_key:
            _, fund_enr, _, _, _ = await _fetch_enrich(
                [],          # skip social enrichment — not needed here
                _fmp_key,
                fundamental_symbols=sorted(stale_set),
                allow_live_fmp=True,
            )
            for sym, row in fund_enr.items():
                if row.get("data_quality") == "missing":
                    unavailable.append({
                        "symbol": sym,
                        "reason": "No FMP fundamentals available",
                    })
                else:
                    live_enrichment[sym] = row
            # Symbols that returned no row at all from FMP
            for sym in sorted(stale_set):
                if sym not in fund_enr:
                    unavailable.append({
                        "symbol": sym,
                        "reason": "No FMP fundamentals available",
                    })
        else:
            for sym in sorted(stale_set):
                unavailable.append({
                    "symbol": sym,
                    "reason": "FMP API key not configured",
                })

    # ── 4. Load fresh cached rows from Neon ────────────────────────────────
    cached_rows: dict[str, dict] = {}
    if fresh_set:
        neon_data = _get_fund(list(fresh_set))
        for sym, neon_row in neon_data.items():
            cached_rows[sym] = _pf_row_from_cache(sym, neon_row)
        # Guard: fresh_set said the symbol is fresh but the row wasn't in DB
        # (edge case: cache was cleared between freshness check and load)
        for sym in sorted(fresh_set):
            if sym not in neon_data and sym not in live_enrichment:
                unavailable.append({
                    "symbol": sym,
                    "reason": "Cache row missing (please retry)",
                })

    # ── 5. Assemble ordered rows (open_symbols order, then sort by mktcap) ─
    unavail_syms = {u["symbol"] for u in unavailable}
    rows: list[dict] = []
    for sym in open_symbols:
        if sym in live_enrichment:
            rows.append(live_enrichment[sym])
        elif sym in cached_rows:
            rows.append(cached_rows[sym])
        elif sym not in unavail_syms:
            unavailable.append({
                "symbol": sym,
                "reason": "No fundamentals data available",
            })

    rows.sort(key=lambda r: -(r.get("market_cap") or 0))

    # ── 6. Response metadata ───────────────────────────────────────────────
    _ms        = int((_pf_time.monotonic() - _t0) * 1000)
    hit_count  = len(fresh_set)
    miss_count = len(stale_set)
    source     = (
        "neon"        if not stale_set else
        "fresh_fetch" if not fresh_set else
        "mixed"
    )

    print(
        f"[portfolio-fundamentals-debug] "
        f"open_symbols_count={len(open_symbols)} "
        f"open_symbols={open_symbols} "
        f"cache_hits={hit_count} "
        f"cache_misses={miss_count} "
        f"stale_symbols={sorted(stale_set)} "
        f"fetched_symbols={sorted(live_enrichment)} "
        f"unavailable_symbols={[u['symbol'] for u in unavailable]} "
        f"provider_calls={len(stale_set)} "
        f"response_ms={_ms} "
        f"source={source}"
    )

    return {
        "holdings_count":        len(open_symbols),   # backward-compat: unique symbol count
        "equity_position_count": _equity_position_count,
        "option_position_count": _option_position_count,
        "total_position_count":  _total_position_count,
        "unique_symbol_count":   len(open_symbols),
        "option_underlying_symbols": _opt_extra,      # option underlyings not already in equity
        "symbols": open_symbols,
        "rows": rows,
        "cache": {
            "source":      source,
            "hit_count":   hit_count,
            "miss_count":  miss_count,
            "stale_count": 0,
            "ttl_seconds": _TTL_PORTFOLIO_FUND,
            "updated_at":  _now_iso,
        },
        "unavailable":         unavailable,
        "unavailable_symbols": [u["symbol"] for u in unavailable],  # flat strings — fixes [object Object]
    }


# ============================================================
# Portfolio Quotes (batch price lookup)
# ============================================================

COMMODITY_SYMBOLS = {
    "SILVER": "SIUSD", "GOLD": "GCUSD", "OIL": "CLUSD", "CRUDE": "CLUSD",
    "NATGAS": "NGUSD", "COPPER": "HGUSD", "PLATINUM": "PLUSD",
    "PALLADIUM": "PAUSD", "WHEAT": "ZSUSD", "CORN": "ZCUSD",
}

INDEX_MAP = {
    "VIX": {"yahoo": "^VIX", "proxy": "VIXY", "tv": "TVC:VIX", "name": "CBOE Volatility Index"},
    "SPX": {"yahoo": "^GSPC", "proxy": "SPY", "tv": "SP:SPX", "name": "S&P 500"},
    "DJI": {"yahoo": "^DJI", "proxy": "DIA", "tv": "TVC:DJI", "name": "Dow Jones Industrial Average"},
    "DJIA": {"yahoo": "^DJI", "proxy": "DIA", "tv": "TVC:DJI", "name": "Dow Jones Industrial Average"},
    "IXIC": {"yahoo": "^IXIC", "proxy": "QQQ", "tv": "NASDAQ:IXIC", "name": "NASDAQ Composite"},
    "NDX": {"yahoo": "^NDX", "proxy": "QQQ", "tv": "NASDAQ:NDX", "name": "NASDAQ 100"},
    "RUT": {"yahoo": "^RUT", "proxy": "IWM", "tv": "TVC:RUT", "name": "Russell 2000"},
    "DXY": {"yahoo": "DX-Y.NYB", "proxy": "UUP", "tv": "TVC:DXY", "name": "US Dollar Index"},
    "TNX": {"yahoo": "^TNX", "proxy": "TLT", "tv": "TVC:TNX", "name": "10-Year Treasury Yield"},
    "GSPC": {"yahoo": "^GSPC", "proxy": "SPY", "tv": "SP:SPX", "name": "S&P 500"},
}

INDEX_YAHOO_SYMBOLS = {k: v["yahoo"] for k, v in INDEX_MAP.items()}
INDEX_YAHOO_SYMBOLS["SPY"] = "SPY"
INDEX_YAHOO_SYMBOLS["QQQ"] = "QQQ"

def _is_known_index(ticker: str) -> bool:
    return ticker.upper().strip() in INDEX_MAP

COINGECKO_COIN_LIST_TTL = 86400
async def get_coingecko_symbol_map() -> dict:
    """Fetch CoinGecko's full coin list and build symbol->id mapping. Cached 24h."""
    import httpx
    from data.cache import cache as _c
    cached = _c.get("cg:coin_list")
    if cached is not None:
        return cached

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get("https://api.coingecko.com/api/v3/coins/list")

        if resp.status_code != 200:
            print(f"[COINGECKO] Coin list fetch failed: {resp.status_code}")
            return {}

        coins = resp.json()
        symbol_map = {}
        for coin in coins:
            symbol = coin.get("symbol", "").upper()
            coin_id = coin.get("id", "")
            if symbol not in symbol_map:
                symbol_map[symbol] = coin_id

        print(f"[COINGECKO] Loaded {len(symbol_map)} coin symbols")
        _c.set("cg:coin_list", symbol_map, COINGECKO_COIN_LIST_TTL)
        return symbol_map

    except Exception as e:
        print(f"[COINGECKO] Error fetching coin list: {e}")
        return {}


@app.post("/api/portfolio/quotes")
async def get_portfolio_quotes(request: Request, api_key: str = Header(None, alias="X-API-Key")):
    """Get current quotes — stocks via FMP, crypto via dynamic CoinGecko lookup, commodities via FMP."""
    import httpx
    import asyncio

    if not _jwt_or_key(request, api_key):
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")

    from data.cache import cache as _cache

    body = await request.json()
    tickers = [t.upper() for t in body.get("tickers", []) if t][:25]
    asset_types = body.get("asset_types", {})
    asset_types = {k.upper(): v for k, v in asset_types.items()} if asset_types else {}
    print(f"[PORTFOLIO] Quotes requested for: {tickers}")
    print(f"[PORTFOLIO] Asset types: {asset_types}")

    if not tickers:
        return {"quotes": {}}

    cache_key = f"portfolio:quotes:{','.join(sorted(tickers))}"
    cached_quotes = _cache.get(cache_key)
    if cached_quotes is not None:
        print(f"[PORTFOLIO] Returning cached quotes for {len(tickers)} tickers")
        return {"quotes": cached_quotes}

    for t in tickers:
        if _is_known_index(t) and asset_types.get(t) != "crypto":
            asset_types[t] = "index"

    index_tickers = [t for t in tickers if asset_types.get(t) == "index"]
    stock_tickers = [t for t in tickers if asset_types.get(t, "stock") in ("stock", "etf") and t not in index_tickers]
    crypto_tickers = [t for t in tickers if asset_types.get(t) == "crypto"]
    commodity_tickers = [t for t in tickers if asset_types.get(t) == "commodity"]

    print(f"[PORTFOLIO] Routing: stocks={stock_tickers}, crypto={crypto_tickers}, commodities={commodity_tickers}, indices={index_tickers}")

    quotes = {}

    PRIORITY_OVERRIDES = {
        "BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana",
        "DOGE": "dogecoin", "ADA": "cardano", "XRP": "ripple",
        "DOT": "polkadot", "LINK": "chainlink", "AVAX": "avalanche-2",
        "MATIC": "matic-network", "UNI": "uniswap", "AAVE": "aave",
        "ATOM": "cosmos", "LTC": "litecoin", "BCH": "bitcoin-cash",
        "SHIB": "shiba-inu", "NEAR": "near", "SUI": "sui",
        "APT": "aptos", "ARB": "arbitrum", "OP": "optimism",
        "INJ": "injective-protocol", "TIA": "celestia", "SEI": "sei-network",
        "PEPE": "pepe", "WIF": "dogwifcoin", "RENDER": "render-token",
        "FET": "fetch-ai", "TAO": "bittensor", "FIL": "filecoin",
        "HYPE": "hyperliquid",
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        # ---- STOCKS: Tradier primary → LKG Tradier → Finnhub → Yahoo → FMP ----
        if stock_tickers:
            import time as _time_mod
            _quote_ts = _time_mod.time()

            # ── Profile helper (sector / company metadata — cached 24 h) ──────
            async def _finnhub_profile(sym):
                sector_cache_key = f"sector:{sym}"
                cached = _cache.get(sector_cache_key)
                if cached is not None:
                    return sym, cached
                try:
                    r = await client.get(
                        "https://finnhub.io/api/v1/stock/profile2",
                        params={"symbol": sym, "token": os.getenv("FINNHUB_API_KEY", "")},
                    )
                    if r.status_code == 200:
                        d = r.json()
                        if d.get("name"):
                            profile = {
                                "sector": d.get("finnhubIndustry", ""),
                                "industry": d.get("finnhubIndustry", ""),
                                "company_name": d.get("name", ""),
                                "market_cap": (d.get("marketCapitalization") or 0) * 1_000_000,
                                "logo": d.get("logo", ""),
                            }
                            _cache.set(sector_cache_key, profile, 86400)
                            return sym, profile
                except Exception:
                    pass
                return sym, None

            # Pre-fetch profiles for all tickers in parallel (mostly cache hits)
            _prof_results = await asyncio.gather(*[_finnhub_profile(s) for s in stock_tickers])
            stock_profiles: dict = {}
            for sym, pd in _prof_results:
                if pd:
                    stock_profiles[sym] = pd

            # LKG Tradier helpers — write to both path-specific and shared canonical LKG
            def _tradier_lkg_key(sym): return f"portfolio:tradier_lkg:{sym}"
            _SHARED_LKG_PFX = "quote:lkg:"
            def _tradier_lkg_save(sym, row):
                _cache.set(_tradier_lkg_key(sym), row, 3 * 24 * 3600)
                _cache.set(f"{_SHARED_LKG_PFX}{sym}", row, 3 * 24 * 3600)

            # ── Step 1: Tradier batch (real-time — includes volume + bid/ask) ─
            tradier_covered: set = set()
            if data_service and getattr(data_service, "tradier", None):
                try:
                    from data.tradier_budget import lane as _pf_q_lane
                    with _pf_q_lane("quotes"):
                        _tr_results = await asyncio.wait_for(
                            data_service.tradier.get_quotes(stock_tickers), timeout=8.0
                        )
                    for q in (_tr_results or []):
                        sym = (q.get("symbol") or "").upper()
                        last = q.get("last")
                        if not sym or not last or last <= 0:
                            continue
                        p = stock_profiles.get(sym, {})
                        row = {
                            "price": last,
                            "change": q.get("change"),
                            "change_pct": q.get("change_percentage"),
                            "day_high": q.get("high"),
                            "day_low": q.get("low"),
                            "volume": q.get("volume"),
                            "avg_volume": q.get("average_volume"),
                            "bid": q.get("bid"),
                            "ask": q.get("ask"),
                            "week_52_high": q.get("week_52_high"),
                            "week_52_low": q.get("week_52_low"),
                            "sector": p.get("sector", ""),
                            "industry": p.get("industry", ""),
                            "company_name": p.get("company_name", ""),
                            "market_cap": p.get("market_cap"),
                            "source": "tradier",
                            "quote_source": "tradier",
                            "quote_cached_at": _quote_ts,
                            "quote_is_stale": False,
                            "quote_fallback_reason": None,
                        }
                        quotes[sym] = row
                        tradier_covered.add(sym)
                        _tradier_lkg_save(sym, row)
                    print(f"[PORTFOLIO] Tradier: {len(tradier_covered)} live quotes")
                except Exception as _te:
                    print(f"[PORTFOLIO] Tradier batch failed (non-fatal): {_te}")

            # ── Step 2: LKG Tradier for tickers Tradier missed ────────────────
            tradier_missed = [t for t in stock_tickers if t not in tradier_covered]
            for sym in tradier_missed:
                # Try portfolio-specific LKG first, then shared canonical LKG written
                # by Home/Watchlist — this eliminates separate-LKG divergence.
                lkg = _cache.get(_tradier_lkg_key(sym)) or _cache.get(f"{_SHARED_LKG_PFX}{sym}")
                if lkg and (lkg.get("price") or lkg.get("last")):
                    price = lkg.get("price") or lkg.get("last")
                    # Normalise field names: shared quote:lkg entries (written by
                    # home_service) use Tradier raw names (last, change_percentage,
                    # average_volume); portfolio shape uses price, change_pct, avg_volume.
                    chg_pct = (
                        lkg.get("change_pct")
                        or lkg.get("change_percentage")
                    )
                    avg_vol = (
                        lkg.get("avg_volume")
                        or lkg.get("average_volume")
                    )
                    quotes[sym] = {
                        **lkg,
                        "price":               price,
                        "change_pct":          chg_pct,
                        "avg_volume":          avg_vol,
                        "quote_is_stale":      True,
                        "quote_fallback_reason": "tradier_lkg",
                        "quote_cached_at":     _quote_ts,
                    }

            # ── Step 3: Finnhub quote for tickers still not covered ───────────
            still_missing = [t for t in stock_tickers if t not in quotes]
            if still_missing:
                async def _finnhub_quote(sym):
                    try:
                        r = await client.get(
                            "https://finnhub.io/api/v1/quote",
                            params={"symbol": sym, "token": os.getenv("FINNHUB_API_KEY", "")},
                        )
                        if r.status_code == 200:
                            d = r.json()
                            if d.get("c") and d["c"] > 0:
                                return sym, d
                    except Exception:
                        pass
                    return sym, None

                fh_results = await asyncio.gather(*[_finnhub_quote(s) for s in still_missing])
                for sym, qd in fh_results:
                    if qd:
                        p = stock_profiles.get(sym, {})
                        quotes[sym] = {
                            "price": qd.get("c"),
                            "change": qd.get("d"),
                            "change_pct": qd.get("dp"),
                            "day_high": qd.get("h"),
                            "day_low": qd.get("l"),
                            "volume": None,
                            "sector": p.get("sector", ""),
                            "industry": p.get("industry", ""),
                            "company_name": p.get("company_name", ""),
                            "market_cap": p.get("market_cap"),
                            "source": "finnhub",
                            "quote_source": "finnhub",
                            "quote_cached_at": _quote_ts,
                            "quote_is_stale": False,
                            "quote_fallback_reason": "tradier_miss",
                        }
                print(f"[PORTFOLIO] Finnhub covered: {[t for t in still_missing if t in quotes]}, missing: {[t for t in still_missing if t not in quotes]}")

            # ── Step 4: Yahoo Finance for tickers still missing ───────────────
            yahoo_need = [t for t in stock_tickers if t not in quotes]
            if yahoo_need:
                print(f"[PORTFOLIO] Trying Yahoo for: {yahoo_need}")
                for sym in yahoo_need:
                    try:
                        resp = await client.get(
                            "https://query1.finance.yahoo.com/v8/finance/chart/" + sym,
                            params={"interval": "1d", "range": "2d"},
                            headers={"User-Agent": "Mozilla/5.0"},
                        )
                        if resp.status_code == 200:
                            chart_data = resp.json()
                            result = chart_data.get("chart", {}).get("result", [])
                            if result:
                                meta = result[0].get("meta", {})
                                price = meta.get("regularMarketPrice", 0)
                                if price and price > 0:
                                    prev_close = meta.get("chartPreviousClose", meta.get("previousClose", 0))
                                    change = round(price - prev_close, 2) if prev_close else 0
                                    change_pct = round((change / prev_close) * 100, 2) if prev_close else 0
                                    p = stock_profiles.get(sym, {})
                                    quotes[sym] = {
                                        "price": price,
                                        "change": change,
                                        "change_pct": change_pct,
                                        "day_high": meta.get("regularMarketDayHigh"),
                                        "day_low": meta.get("regularMarketDayLow"),
                                        "volume": meta.get("regularMarketVolume"),
                                        "sector": p.get("sector", ""),
                                        "industry": p.get("industry", ""),
                                        "company_name": p.get("company_name", ""),
                                        "market_cap": p.get("market_cap"),
                                        "source": "yahoo",
                                        "quote_source": "yahoo",
                                        "quote_cached_at": _quote_ts,
                                        "quote_is_stale": False,
                                        "quote_fallback_reason": "tradier_finnhub_miss",
                                    }
                                    print(f"[PORTFOLIO] Yahoo: {sym} = ${price}")
                    except Exception as e:
                        print(f"[PORTFOLIO] Yahoo {sym} error: {e}")

            # ── Step 5: FMP last resort ───────────────────────────────────────
            fmp_need = [t for t in stock_tickers if t not in quotes]
            if fmp_need:
                print(f"[PORTFOLIO] FMP last resort for: {fmp_need}")
                ticker_str = ",".join(fmp_need)
                try:
                    full_resp = await client.get(
                        "https://financialmodelingprep.com/stable/quote",
                        params={"symbol": ticker_str, "apikey": FMP_API_KEY},
                    )
                    if full_resp.status_code == 200:
                        for item in full_resp.json():
                            symbol = item.get("symbol", "")
                            if not symbol:
                                continue
                            quotes[symbol] = {
                                "price": item.get("price"),
                                "change": item.get("change"),
                                "change_pct": item.get("changesPercentage"),
                                "day_high": item.get("dayHigh"),
                                "day_low": item.get("dayLow"),
                                "year_high": item.get("yearHigh"),
                                "year_low": item.get("yearLow"),
                                "market_cap": item.get("marketCap"),
                                "volume": item.get("volume"),
                                "avg_volume": item.get("avgVolume"),
                                "pe": item.get("pe"),
                                "eps": item.get("eps"),
                                "sector": item.get("sector", ""),
                                "source": "fmp",
                                "quote_source": "fmp",
                                "quote_cached_at": _quote_ts,
                                "quote_is_stale": False,
                                "quote_fallback_reason": "tradier_finnhub_yahoo_miss",
                            }
                        print(f"[PORTFOLIO] FMP fallback returned {len([t for t in fmp_need if t in quotes])} quotes")
                except Exception as e:
                    print(f"[PORTFOLIO] FMP fallback error: {e}")

        # ---- INDICES: VIX via FRED (actual value), others via Yahoo → unavailable with note ----
        if index_tickers:
            async def _fetch_index_quote(ticker):
                idx_info = INDEX_MAP.get(ticker, {})
                tv_symbol = idx_info.get("tv", f"TVC:{ticker}")
                idx_name = idx_info.get("name", ticker)
                proxy_etf = idx_info.get("proxy")
                yahoo_symbol = idx_info.get("yahoo") or INDEX_YAHOO_SYMBOLS.get(ticker, ticker)

                if ticker == "VIX":
                    try:
                        from data.fred_provider import FredProvider
                        fred = FredProvider(api_key=os.getenv("FRED_API_KEY", ""))
                        vix_data = await asyncio.to_thread(fred.get_vix)
                        if isinstance(vix_data, dict) and vix_data.get("current_vix"):
                            vix_price = vix_data["current_vix"]
                            trend = vix_data.get("trend", [])
                            prev_vix = trend[-2]["vix"] if len(trend) >= 2 else vix_price
                            change = round(vix_price - prev_vix, 2)
                            change_pct = round((change / prev_vix) * 100, 2) if prev_vix else 0
                            quotes[ticker] = {
                                "price": vix_price,
                                "change": change,
                                "change_pct": change_pct,
                                "source": "fred",
                                "asset_type": "index",
                                "sector": "Index",
                                "company_name": "CBOE Volatility Index",
                                "tradingview_symbol": "TVC:VIX",
                                "signal": vix_data.get("signal", ""),
                            }
                            print(f"[PORTFOLIO] VIX from FRED: {vix_price} ({vix_data.get('signal', '')})", flush=True)
                            return
                    except Exception as e:
                        print(f"[PORTFOLIO] FRED VIX failed: {e}", flush=True)

                try:
                    resp = await client.get(
                        "https://query1.finance.yahoo.com/v8/finance/chart/" + yahoo_symbol,
                        params={"interval": "1d", "range": "2d"},
                        headers={"User-Agent": "Mozilla/5.0"},
                        timeout=6.0,
                    )
                    if resp.status_code == 200:
                        chart_data = resp.json()
                        result = chart_data.get("chart", {}).get("result", [])
                        if result:
                            meta = result[0].get("meta", {})
                            price = meta.get("regularMarketPrice", 0)
                            if price and price > 0:
                                prev_close = meta.get("chartPreviousClose", meta.get("previousClose", 0))
                                change = round(price - prev_close, 2) if prev_close else 0
                                change_pct = round((change / prev_close) * 100, 2) if prev_close else 0
                                quotes[ticker] = {
                                    "price": price,
                                    "change": change,
                                    "change_pct": change_pct,
                                    "day_high": meta.get("regularMarketDayHigh"),
                                    "day_low": meta.get("regularMarketDayLow"),
                                    "volume": meta.get("regularMarketVolume"),
                                    "source": "yahoo",
                                    "asset_type": "index",
                                    "sector": "Index",
                                    "company_name": idx_name,
                                    "tradingview_symbol": tv_symbol,
                                }
                                print(f"[PORTFOLIO] Index: {ticker} via Yahoo = ${price}", flush=True)
                                return
                    print(f"[PORTFOLIO] Yahoo index {ticker} returned {resp.status_code}", flush=True)
                except Exception as e:
                    print(f"[PORTFOLIO] Yahoo index {ticker} error: {e}", flush=True)

                etf_note = f"Consider tracking {proxy_etf} instead for real-time data." if proxy_etf else ""
                quotes[ticker] = {
                    "price": None,
                    "change": None,
                    "change_pct": None,
                    "source": "unavailable",
                    "asset_type": "index",
                    "sector": "Index",
                    "company_name": idx_name,
                    "tradingview_symbol": tv_symbol,
                    "note": f"Live {ticker} index data unavailable on free API tier. {etf_note}".strip(),
                }
                print(f"[PORTFOLIO] Index {ticker}: no actual index quote available", flush=True)

            await asyncio.gather(*[_fetch_index_quote(t) for t in index_tickers])

        # ---- CRYPTO: CoinGecko primary → CoinMarketCap fallback on 429 ----
        if crypto_tickers:
            cg_rate_limited = False
            symbol_map = await get_coingecko_symbol_map()

            crypto_ids_to_fetch = {}
            for ticker in crypto_tickers:
                cg_id = PRIORITY_OVERRIDES.get(ticker) or symbol_map.get(ticker)
                if not cg_id and ticker.endswith("USD"):
                    cg_id = PRIORITY_OVERRIDES.get(ticker[:-3]) or symbol_map.get(ticker[:-3])
                if not cg_id and ticker.endswith("USDT"):
                    cg_id = PRIORITY_OVERRIDES.get(ticker[:-4]) or symbol_map.get(ticker[:-4])
                if cg_id:
                    crypto_ids_to_fetch[cg_id] = ticker
                else:
                    print(f"[PORTFOLIO] No CoinGecko ID found for crypto ticker: {ticker}")

            if crypto_ids_to_fetch:
                ids_list = list(crypto_ids_to_fetch.keys())
                print(f"[PORTFOLIO] CoinGecko direct lookup for {len(ids_list)} crypto tickers")

                for i in range(0, len(ids_list), 50):
                    batch = ids_list[i:i+50]
                    ids_str = ",".join(batch)
                    try:
                        resp = await client.get(
                            "https://api.coingecko.com/api/v3/simple/price",
                            params={
                                "ids": ids_str,
                                "vs_currencies": "usd",
                                "include_24hr_change": "true",
                                "include_24hr_vol": "true",
                                "include_market_cap": "true",
                            },
                        )
                        if resp.status_code == 200:
                            cg_data = resp.json()
                            for cg_id, price_data in cg_data.items():
                                original_ticker = crypto_ids_to_fetch.get(cg_id, cg_id.upper())
                                price = price_data.get("usd", 0)
                                change_pct = price_data.get("usd_24h_change", 0)
                                quotes[original_ticker] = {
                                    "price": price,
                                    "change": round(price * (change_pct / 100), 4) if change_pct else 0,
                                    "change_pct": round(change_pct, 2) if change_pct else 0,
                                    "market_cap": price_data.get("usd_market_cap", 0),
                                    "volume": price_data.get("usd_24h_vol", 0),
                                    "source": "coingecko",
                                    "asset_type": "crypto",
                                    "sector": "Crypto",
                                }
                                print(f"[PORTFOLIO] CoinGecko: {original_ticker} = ${price}")
                        elif resp.status_code == 429:
                            cg_rate_limited = True
                            print(f"[PORTFOLIO] CoinGecko rate limited (429), will try CoinMarketCap")
                        else:
                            print(f"[PORTFOLIO] CoinGecko error: {resp.status_code}")
                    except Exception as e:
                        print(f"[PORTFOLIO] CoinGecko error: {e}")
                    if i + 50 < len(ids_list):
                        await asyncio.sleep(1.0)

            crypto_still_missing = [t for t in crypto_tickers if t not in quotes]
            if crypto_still_missing and CMC_API_KEY and cg_rate_limited:
                print(f"[PORTFOLIO] CoinMarketCap fallback (CoinGecko 429) for: {crypto_still_missing}")
                try:
                    cmc_lookup = {}
                    for t in crypto_still_missing:
                        sym = t
                        if sym.endswith("USD"):
                            sym = sym[:-3]
                        elif sym.endswith("USDT"):
                            sym = sym[:-4]
                        cmc_lookup[sym] = t
                    cmc_symbols = ",".join(cmc_lookup.keys())
                    resp = await client.get(
                        "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest",
                        params={"symbol": cmc_symbols, "convert": "USD"},
                        headers={"X-CMC_PRO_API_KEY": CMC_API_KEY},
                    )
                    if resp.status_code == 200:
                        cmc_data = resp.json().get("data", {})
                        for sym_key, token_data in cmc_data.items():
                            original_ticker = cmc_lookup.get(sym_key.upper(), sym_key.upper())
                            if isinstance(token_data, list):
                                token_data = token_data[0]
                            usd_quote = token_data.get("quote", {}).get("USD", {})
                            price = usd_quote.get("price", 0)
                            if price:
                                change_pct = usd_quote.get("percent_change_24h", 0)
                                quotes[original_ticker] = {
                                    "price": round(price, 6) if price < 1 else round(price, 2),
                                    "change": round(price * (change_pct / 100), 4) if change_pct else 0,
                                    "change_pct": round(change_pct, 2) if change_pct else 0,
                                    "market_cap": usd_quote.get("market_cap", 0),
                                    "volume": usd_quote.get("volume_24h", 0),
                                    "source": "coinmarketcap",
                                    "asset_type": "crypto",
                                    "sector": "Crypto",
                                }
                                print(f"[PORTFOLIO] CMC: {original_ticker} = ${price}")
                    else:
                        print(f"[PORTFOLIO] CoinMarketCap error: {resp.status_code}")
                except Exception as e:
                    print(f"[PORTFOLIO] CoinMarketCap error: {e}")
            elif crypto_still_missing and not CMC_API_KEY:
                print(f"[PORTFOLIO] CMC_API_KEY not set, cannot fallback for: {crypto_still_missing}")

        # ---- COMMODITIES: FMP commodity symbols (parallel) ----
        if commodity_tickers:
            _commodity_pairs = [(t, COMMODITY_SYMBOLS.get(t)) for t in commodity_tickers]
            _valid_commodities = [(t, sym) for t, sym in _commodity_pairs if sym]
            _invalid_commodities = [t for t, sym in _commodity_pairs if not sym]
            for t in _invalid_commodities:
                print(f"[PORTFOLIO] No commodity symbol mapping for: {t}")

            async def _fetch_commodity(t, fmp_symbol):
                try:
                    resp = await client.get(
                        "https://financialmodelingprep.com/stable/quote-short",
                        params={"symbol": fmp_symbol, "apikey": FMP_API_KEY},
                    )
                    if resp.status_code == 200:
                        items = resp.json()
                        if items:
                            item = items[0]
                            print(f"[PORTFOLIO] Commodity: {t} = ${item.get('price')}")
                            return t, {
                                "price": item.get("price"),
                                "change": item.get("change"),
                                "change_pct": item.get("changesPercentage"),
                                "volume": item.get("volume"),
                                "source": "fmp_commodity",
                                "asset_type": "commodity",
                                "sector": "Commodities",
                            }
                except Exception as e:
                    print(f"[PORTFOLIO] Commodity {t} error: {e}")
                return t, None

            if _valid_commodities:
                _comm_results = await asyncio.gather(
                    *[_fetch_commodity(t, sym) for t, sym in _valid_commodities]
                )
                for t, data in _comm_results:
                    if data:
                        quotes[t] = data

        missing_tickers = [t for t in tickers if t not in quotes]
        if missing_tickers:
            print(f"[PORTFOLIO] Fallback for unresolved tickers: {missing_tickers}")

            # Parallel commodity fallback for missing tickers
            _missing_commodity = [(t, COMMODITY_SYMBOLS.get(t)) for t in missing_tickers if COMMODITY_SYMBOLS.get(t)]

            async def _fallback_commodity(t, fmp_symbol):
                try:
                    resp = await client.get(
                        "https://financialmodelingprep.com/stable/quote-short",
                        params={"symbol": fmp_symbol, "apikey": FMP_API_KEY},
                    )
                    if resp.status_code == 200:
                        items = resp.json()
                        if items:
                            item = items[0]
                            return t, {
                                "price": item.get("price"),
                                "change": item.get("change"),
                                "change_pct": item.get("changesPercentage"),
                                "volume": item.get("volume"),
                                "source": "fmp_commodity",
                                "asset_type": "commodity",
                                "sector": "Commodities",
                            }
                except Exception:
                    pass
                return t, None

            if _missing_commodity:
                _mc_results = await asyncio.gather(
                    *[_fallback_commodity(t, sym) for t, sym in _missing_commodity]
                )
                for t, data in _mc_results:
                    if data:
                        quotes[t] = data

            still_missing = [t for t in tickers if t not in quotes]
            if still_missing:
                symbol_map = await get_coingecko_symbol_map()
                crypto_ids_to_fetch = {}
                for ticker in still_missing:
                    cg_id = PRIORITY_OVERRIDES.get(ticker) or symbol_map.get(ticker)
                    if cg_id:
                        crypto_ids_to_fetch[cg_id] = ticker
                    elif ticker.endswith("USD") and (PRIORITY_OVERRIDES.get(ticker[:-3]) or symbol_map.get(ticker[:-3])):
                        crypto_ids_to_fetch[PRIORITY_OVERRIDES.get(ticker[:-3]) or symbol_map[ticker[:-3]]] = ticker
                    elif ticker.endswith("USDT") and (PRIORITY_OVERRIDES.get(ticker[:-4]) or symbol_map.get(ticker[:-4])):
                        crypto_ids_to_fetch[PRIORITY_OVERRIDES.get(ticker[:-4]) or symbol_map[ticker[:-4]]] = ticker

                if crypto_ids_to_fetch:
                    ids_list = list(crypto_ids_to_fetch.keys())
                    print(f"[PORTFOLIO] CoinGecko fallback resolving {len(ids_list)} tickers")
                    for i in range(0, len(ids_list), 50):
                        batch = ids_list[i:i+50]
                        ids_str = ",".join(batch)
                        try:
                            resp = await client.get(
                                "https://api.coingecko.com/api/v3/simple/price",
                                params={
                                    "ids": ids_str,
                                    "vs_currencies": "usd",
                                    "include_24hr_change": "true",
                                    "include_24hr_vol": "true",
                                    "include_market_cap": "true",
                                },
                            )
                            if resp.status_code == 200:
                                cg_data = resp.json()
                                for cg_id, price_data in cg_data.items():
                                    original_ticker = crypto_ids_to_fetch.get(cg_id, cg_id.upper())
                                    price = price_data.get("usd", 0)
                                    change_pct = price_data.get("usd_24h_change", 0)
                                    quotes[original_ticker] = {
                                        "price": price,
                                        "change": round(price * (change_pct / 100), 4) if change_pct else 0,
                                        "change_pct": round(change_pct, 2) if change_pct else 0,
                                        "market_cap": price_data.get("usd_market_cap", 0),
                                        "volume": price_data.get("usd_24h_vol", 0),
                                        "source": "coingecko",
                                        "asset_type": "crypto",
                                        "sector": "Crypto",
                                    }
                                    print(f"[PORTFOLIO] CoinGecko fallback: {original_ticker} = ${price}")
                            else:
                                print(f"[PORTFOLIO] CoinGecko fallback error: {resp.status_code}")
                        except Exception as e:
                            print(f"[PORTFOLIO] CoinGecko fallback error: {e}")
                        if i + 50 < len(ids_list):
                            await asyncio.sleep(1.0)

        final_missing = [t for t in tickers if t not in quotes]
        if final_missing:
            print(f"[PORTFOLIO] No price data found for: {final_missing}")

    for ticker, quote in quotes.items():
        if not quote.get("sector"):
            if quote.get("asset_type") == "crypto" or quote.get("source") == "coingecko":
                quote["sector"] = "Crypto"
            elif quote.get("asset_type") == "commodity" or quote.get("source") == "fmp_commodity":
                quote["sector"] = "Commodities"
            elif not quote.get("sector"):
                quote["sector"] = "Other"

    _cache.set(cache_key, quotes, 60)
    print(f"[PORTFOLIO] Final sectors: {[(t, q.get('sector')) for t, q in quotes.items()]}")
    print(f"[PORTFOLIO] Returning {len(quotes)} quotes for: {list(quotes.keys())}")
    return {"quotes": quotes}


# ============================================================
# Portfolio Events (earnings + dividends for holdings)
# ============================================================

@app.get("/api/caelyn-terminal")
async def caelyn_terminal(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
    _sub: None = Depends(require_subscription),
):
    """Full portfolio analytics payload for the Caelyn Terminal dashboard."""
    if not _jwt_or_key(request, api_key):
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")

    from data.caelyn_terminal import CaelynTerminalProvider

    from data.portfolio_store import canonical_file as _cf, load_active_holdings as _load_h
    user_id = getattr(request.state, "user_id", "default")
    portfolio_file = _cf()

    # Diagnostic: log canonical state before building
    try:
        _pf_h = _load_h()
        _pf_syms = [h.get("ticker", "?") for h in _pf_h]
        print(
            f"[portfolio-source-audit] endpoint=terminal  user_id={user_id}  "
            f"source_file={portfolio_file}  "
            f"count={len(_pf_syms)}  symbols={_pf_syms[:20]}"
        )
    except Exception as _log_err:
        print(f"[portfolio-source-audit] endpoint=terminal  log_error={_log_err}")

    provider = CaelynTerminalProvider(
        tradier=data_service.tradier if data_service else None,
        finnhub=data_service.finnhub if data_service else None,
        fmp=data_service.fmp if data_service else None,
        yahoo=data_service.yahoo if data_service else None,
        coingecko=data_service.coingecko if data_service else None,
    )
    return await provider.get(portfolio_file)


@app.get("/api/portfolio/relative-volume")
@limiter.limit("60/minute")
async def portfolio_relative_volume(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
    tickers: str = "",
):
    """
    Batch relative-volume (VolX) for portfolio holdings.

    One Tradier batch call for all requested tickers — returns for each:
      volume      — today's shares traded so far
      avg_volume  — 3-month average daily volume (Tradier)
      vol_x       — volume / avg_volume, null when volume=0 (pre-market / closed)
                    or when Tradier has no data for the ticker (OTC/pink-sheet)

    Frontend should call this instead of N per-ticker Tradier quote calls.
    Cached 60 s matching the Tradier quote TTL.
    """
    import time as _time_mod
    from data.cache import cache as _cache

    if not _jwt_or_key(request, api_key):
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")

    await _wait_for_init()

    syms = [t.strip().upper() for t in tickers.split(",") if t.strip()][:25]
    if not syms:
        from data.portfolio_store import load_active_holdings as _load_h
        syms = [h["ticker"].upper() for h in _load_h() if h.get("ticker")][:25]
    if not syms:
        return {"tickers": {}}

    _sym_key  = ",".join(sorted(syms))
    _ck       = f"portfolio:relvol:{_sym_key}"
    _lkg_ck   = f"portfolio:relvol:lkg:{_sym_key}"
    _lkg_ttl  = 4 * 3600  # 4 h — survives restarts / Tradier outages

    # ── Hot cache ─────────────────────────────────────────────────────────
    _t0 = _time_mod.time()
    _hot = _cache.get(_ck)
    if _hot is not None:
        return {"tickers": _hot, "from_cache": True, "data_status": "live",
                "cache_age_seconds": round(_time_mod.time() - _t0)}

    # ── Saturation / provider check → serve LKG immediately ──────────────
    _saturated = False
    if not data_service or not data_service.tradier:
        _saturated = True
    else:
        try:
            from data.tradier_provider import TRADIER_LIMITER as _TL
            _saturated = _TL.is_saturated()
        except Exception:
            pass

    if _saturated:
        _lkg = _cache.get(_lkg_ck)
        _ms  = round((_time_mod.time() - _t0) * 1000)
        if _lkg is not None:
            print(f"[PORTFOLIO_RELVOL] status=lkg_saturated tickers={len(syms)} elapsed_ms={_ms}")
            return {"tickers": _lkg["data"], "from_cache": True,
                    "data_status": "lkg", "lkg_age_seconds": round(_time_mod.time() - _lkg["ts"])}
        print(f"[PORTFOLIO_RELVOL] status=saturated_no_lkg tickers={len(syms)} elapsed_ms={_ms}")
        return {"tickers": {t: {"volume": None, "avg_volume": None, "vol_x": None} for t in syms},
                "data_status": "unavailable", "error": "tradier_saturated"}

    # ── Live Tradier call ─────────────────────────────────────────────────
    try:
        import asyncio as _aio
        from data.tradier_budget import lane as _rv_lane
        with _rv_lane("quotes"):
            raw = await _aio.wait_for(
                data_service.tradier.get_quotes(syms), timeout=5.0
            )
    except Exception as _e:
        _ms = round((_time_mod.time() - _t0) * 1000)
        print(f"[PORTFOLIO_RELVOL] status=tradier_error elapsed_ms={_ms} exc={type(_e).__name__}")
        # Tradier failed — serve LKG if available
        _lkg = _cache.get(_lkg_ck)
        if _lkg is not None:
            return {"tickers": _lkg["data"], "from_cache": True,
                    "data_status": "lkg", "lkg_age_seconds": round(_time_mod.time() - _lkg["ts"]),
                    "error": str(_e)}
        return {"tickers": {t: {"volume": None, "avg_volume": None, "vol_x": None} for t in syms},
                "data_status": "unavailable", "error": str(_e)}

    result: dict = {}
    for q in (raw or []):
        sym = (q.get("symbol") or "").upper()
        if not sym:
            continue
        vol     = q.get("volume")
        avg_vol = q.get("average_volume")
        vol_x   = round(vol / avg_vol, 2) if vol and avg_vol and avg_vol > 0 else None
        result[sym] = {"volume": vol, "avg_volume": avg_vol, "vol_x": vol_x}

    for sym in syms:
        if sym not in result:
            result[sym] = {"volume": None, "avg_volume": None, "vol_x": None}

    _ms = round((_time_mod.time() - _t0) * 1000)
    _found = sum(1 for v in result.values() if v["avg_volume"])
    print(
        f"[PORTFOLIO_RELVOL] status=live elapsed_ms={_ms} tickers={len(syms)} "
        f"found={_found} vol_x_available={sum(1 for v in result.values() if v['vol_x'] is not None)}"
    )

    _now_ts = _time_mod.time()
    _cache.set(_ck, result, 60)
    _cache.set(_lkg_ck, {"data": result, "ts": _now_ts}, _lkg_ttl)

    # ── Alert bus hook: portfolio relative-volume ─────────────────────────────
    for _rv_sym, _rv_data in result.items():
        _vx = _rv_data.get("vol_x")
        if _vx is None:
            continue  # only record when vol_x is actually available
        asyncio.create_task(_alert_bus_fire(
            "portfolio_relvol", "default", _rv_sym,
            {
                "volx":   _vx,
                "volume": _rv_data.get("volume"),
            }
        ))

    return {"tickers": result, "from_cache": False, "data_status": "live"}


@app.get("/api/portfolio/events")
async def get_portfolio_events(request: Request, api_key: str = Header(None, alias="X-API-Key")):
    """Get upcoming earnings and dividend dates for portfolio holdings."""
    import httpx
    from datetime import datetime, timedelta

    user_id = getattr(request.state, "user_id", "default")
    portfolio_file = _portfolio_file(user_id)
    if not portfolio_file.exists():
        return {"events": []}
    try:
        with open(portfolio_file) as f:
            data = _json.load(f)
    except Exception:
        return {"events": []}

    tickers = [t["ticker"] for t in data.get("holdings", []) if "ticker" in t]
    if not tickers:
        return {"events": []}

    today = datetime.now().strftime("%Y-%m-%d")
    future = (datetime.now() + timedelta(days=60)).strftime("%Y-%m-%d")

    events = []
    errors = []

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                "https://financialmodelingprep.com/stable/earnings-calendar",
                params={"from": today, "to": future, "apikey": FMP_API_KEY},
            )
        if resp.status_code == 200:
            for item in resp.json():
                if item.get("symbol") in tickers:
                    events.append({
                        "ticker": item["symbol"],
                        "type": "earnings",
                        "date": item.get("date"),
                        "eps_estimated": item.get("epsEstimated"),
                        "revenue_estimate": item.get("revenueEstimated"),
                    })
        else:
            errors.append(f"earnings_calendar: FMP {resp.status_code}")
    except Exception as e:
        errors.append(f"earnings_calendar: {str(e)}")

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                "https://financialmodelingprep.com/stable/dividends-calendar",
                params={"from": today, "to": future, "apikey": FMP_API_KEY},
            )
        if resp.status_code == 200:
            for item in resp.json():
                if item.get("symbol") in tickers:
                    events.append({
                        "ticker": item["symbol"],
                        "type": "dividend",
                        "date": item.get("date"),
                        "yield": item.get("yield"),
                    })
        else:
            errors.append(f"dividend_calendar: FMP {resp.status_code}")
    except Exception as e:
        errors.append(f"dividend_calendar: {str(e)}")

    events.sort(key=lambda x: x.get("date", ""))
    result = {"events": events}
    if errors:
        result["errors"] = errors
    return result


# ============================================================
# Portfolio Options — options signals for portfolio tickers
# ============================================================

@app.get("/api/portfolio/options")
@limiter.limit("30/minute")
async def portfolio_options(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
    _sub: None = Depends(require_subscription),
    tickers: str = "",
):
    """
    Options signals for specific portfolio tickers.
    Primary path: filter the master screener cache by requested tickers (instant).
    Fallback path: targeted lightweight Tradier scan for tickers not in cache.
    Returns the same composite_score / pc_ratio / iv / primary_signal fields
    as /api/options/screener, normalised into a flat list.
    """
    import asyncio
    import time as _time
    from data.cache import cache

    await _wait_for_init()

    if not data_service or not data_service.tradier:
        return {"tickers": [], "error": "options_unavailable"}

    requested = [t.strip().upper() for t in tickers.split(",") if t.strip()][:20]
    if not requested:
        return {"tickers": []}

    from data.portfolio_options_service import scan_portfolio_options

    master_snap = cache.get(_OPTIONS_MASTER_CACHE_KEY) or cache.get(_OPTIONS_MASTER_LKG_KEY)

    scan = await scan_portfolio_options(
        symbols     = requested,
        tradier     = data_service.tradier,
        cache       = cache,
        master_snap = master_snap,
        holdings_sig = None,   # no per-portfolio cache for direct HTTP calls
    )

    available_rows = scan.get("rows", [])
    all_rows       = list(scan.get("by_symbol", {}).values())

    print(
        f"[PORTFOLIO_OPTIONS] requested={requested} "
        f"available={scan.get('available_count',0)} "
        f"unavailable={scan.get('unavailable_count',0)} "
        f"cache_status={scan.get('options_cache_status','?')} "
        f"provider_calls={scan.get('provider_calls',0)}"
    )
    return {
        "tickers":              available_rows,
        "all_statuses":         all_rows,
        "available_count":      scan.get("available_count", 0),
        "unavailable_count":    scan.get("unavailable_count", 0),
        "unavailable_reasons":  scan.get("unavailable_reasons_by_symbol", {}),
        "options_cache_status": scan.get("options_cache_status", "unknown"),
        "source":               scan.get("source", "portfolio_scoped_options_screener"),
        "from_master_cache": sum(
            1 for r in all_rows
            if r.get("data_available") and not r.get("live_scanned")
        ),
        "live_scanned": [
            r.get("ticker") for r in available_rows if r.get("source") == "portfolio_scoped_options_screener"
        ],
    }


# ============================================================
# Portfolio Options Position Detail — per-underlying popup
# ============================================================

@app.get("/api/portfolio/options-position-detail/{underlying}")
@limiter.limit("60/minute")
async def portfolio_option_position_detail(
    request: Request,
    underlying: str,
    api_key: str = Header(None, alias="X-API-Key"),
    _sub: None = Depends(require_subscription),
):
    """
    Per-underlying option position detail for the Dashboard popup panel.

    Loads the current user's open option positions for `underlying`, enriches
    them with Tradier mark quotes (using existing per-symbol quote cache), and
    attaches the portfolio-scoped options signal + pullback-risk data produced
    by scan_portfolio_options.

    Returns 404 if no open option positions exist for the underlying.
    Returns nulls in signal/risk blocks if options data is unavailable —
    the popup can still render contract-level position data.
    """
    import time as _t_opd
    _t0_opd = _t_opd.perf_counter()

    sym = underlying.strip().upper()
    if not sym:
        return JSONResponse(status_code=400, content={"error": "underlying required"})

    # ── 1. Load open option positions; filter to this underlying ─────────────
    from data.option_trades_store import load_option_positions as _load_opt_pos
    _OPEN_ST = frozenset({"open", "partially_closed_open", "short_option_tracked_basic"})
    _all_pos = _load_opt_pos()
    _matching = [
        p for p in _all_pos
        if (p.get("underlying") or "").upper() == sym
        and (p.get("final_status") or "open") in _OPEN_ST
    ]
    if not _matching:
        return JSONResponse(
            status_code=404,
            content={"error": f"No open option positions for {sym}", "underlying": sym},
        )

    # ── 2. OCC key → Tradier compact symbol ──────────────────────────────────
    def _ok2ts(occ_key: str) -> str | None:
        try:
            parts = occ_key.split("_")
            if len(parts) < 4:
                return None
            _u, _e, _s, _t = parts[0], parts[1], float(parts[2]), parts[3].upper()
            from datetime import datetime as _dtz
            _yymmdd = _dtz.strptime(_e, "%Y-%m-%d").strftime("%y%m%d")
            _cp     = "C" if _t == "CALL" else "P"
            return f"{_u}{_yymmdd}{_cp}{round(_s * 1000):08d}"
        except Exception:
            return None

    _sym_map: dict[str, str] = {}   # occ_key → tradier compact sym
    for _p in _matching:
        _ok   = _p.get("occ_key", "")
        _tsym = _ok2ts(_ok)
        if _ok and _tsym:
            _sym_map[_ok] = _tsym

    # ── 3. Tradier option mark quotes (reuses existing 60-s quote cache) ─────
    from data.cache import cache as _dcache
    _tradier = data_service.tradier if data_service else None

    _quote_map: dict[str, dict] = {}   # upper(tradier_sym) → quote dict
    _opt_qcache_hit = True
    if _sym_map and _tradier:
        try:
            _tradier_syms = list(_sym_map.values())
            _raw_qs = await _tradier.get_quotes(_tradier_syms)
            for _q in (_raw_qs or []):
                _qs = (_q.get("symbol") or "").upper()
                if _qs:
                    _quote_map[_qs] = _q
            _opt_qcache_hit = len(_quote_map) >= len(_tradier_syms)
        except Exception as _qe:
            print(f"[portfolio-option-popup-detail] option quote error: {_qe}")

    # ── 4. Underlying equity quote → company_name, underlying_price ──────────
    _company_name     = ""
    _underlying_price: float | None = None
    _eq_q = _dcache.get(f"tradier:quote:sym:{sym}")
    if _eq_q:
        _company_name     = _eq_q.get("description") or ""
        _underlying_price = _eq_q.get("last")
    elif _tradier:
        try:
            _eq_raw = await _tradier.get_quotes([sym])
            _eq_q   = next(
                (q for q in (_eq_raw or []) if (q.get("symbol") or "").upper() == sym), {}
            )
            _company_name     = _eq_q.get("description") or ""
            _underlying_price = _eq_q.get("last")
        except Exception as _eqe:
            print(f"[portfolio-option-popup-detail] equity quote error: {_eqe}")

    # ── 5. Build enriched contract list ──────────────────────────────────────
    _MULT       = 100
    _enriched:  list[dict] = []
    _total_cost  = 0.0
    _total_mv    = 0.0
    _val_hit     = 0
    _val_miss    = 0

    for _p in _matching:
        _ok      = _p.get("occ_key", "")
        _tsym_u  = (_sym_map.get(_ok) or "").upper()
        _q2      = _quote_map.get(_tsym_u) if _tsym_u else None
        _contr   = float(_p.get("contracts_open") or 0)
        _cb      = float(_p.get("cost_basis")     or 0)
        _avgp    = float(_p.get("avg_premium")    or 0)
        _total_cost += _cb

        # Human-readable display symbol: "BWEN 07/17/2026 $2.50 CALL"
        _exp  = _p.get("expiration_date", "")
        _strk = float(_p.get("strike") or 0)
        _otyp = (_p.get("option_type") or "CALL").upper()
        try:
            from datetime import datetime as _dtd
            _exp_disp = _dtd.strptime(_exp, "%Y-%m-%d").strftime("%m/%d/%Y")
        except Exception:
            _exp_disp = _exp
        _disp_sym = f"{sym} {_exp_disp} ${_strk:g} {_otyp}"

        _mark = _bid = _ask = _last = None
        _mark_src  = "unavailable"
        _mktval = _upnl = _upnl_pct = None
        _q_unavail = None

        if _q2:
            _bid  = _q2.get("bid")
            _ask  = _q2.get("ask")
            _last = _q2.get("last")
            if _bid is not None and _ask is not None and (_bid + _ask) > 0:
                _mark = round((_bid + _ask) / 2, 4)
            elif _last is not None and _last > 0:
                _mark = round(float(_last), 4)

            if _mark is not None:
                _mark_src  = "tradier"
                _mktval    = round(_contr * _mark * _MULT, 2)
                _upnl      = round(_mktval - _cb, 2)
                _upnl_pct  = round(_upnl / _cb * 100, 4) if _cb else None
                _total_mv += _mktval
                _val_hit  += 1
            else:
                _mark_src = "tradier_no_mark"
                _q_unavail = "no_active_market"
                _val_miss += 1
        else:
            _q_unavail = "tradier_lookup_failed"
            _val_miss += 1

        _enriched.append({
            "underlying":               sym,
            "occ_key":                  _ok,
            "option_symbol":            _p.get("display_symbol", ""),
            "display_symbol":           _disp_sym,
            "tradier_symbol":           _tsym_u or None,
            "expiration_date":          _exp,
            "strike":                   _strk,
            "option_type":              _otyp,
            "contracts_open":           _contr,
            "avg_premium":              _avgp,
            "cost_basis":               _cb,
            "first_entry_date":         _p.get("first_entry_date"),
            "last_entry_date":          _p.get("last_entry_date"),
            "final_status":             _p.get("final_status", "open"),
            "mark_price":               _mark,
            "mark_bid":                 _bid,
            "mark_ask":                 _ask,
            "mark_last":                _last,
            "mark_source":              _mark_src,
            "market_value":             _mktval,
            "unrealized_pnl":           _upnl,
            "unrealized_pnl_pct":       _upnl_pct,
            "quote_unavailable_reason": _q_unavail,
        })

    # ── 6. Portfolio-scoped options signal + risk (per-ticker cache) ──────────
    # scan_portfolio_options checks portfolio_opts:{sym} cache (300s) first,
    # then master screener cache, then live Tradier scan — in that order.
    # _compute_pullback_risk is called inside scan_portfolio_options, so the
    # returned row already contains risk_score / risk_level / risk_reasons.
    _master_snap = _dcache.get(_OPTIONS_MASTER_CACHE_KEY) or _dcache.get(_OPTIONS_MASTER_LKG_KEY)
    from data.portfolio_options_service import scan_portfolio_options as _scan_opt

    _opt_scan = await _scan_opt(
        symbols                = [sym],
        tradier                = _tradier,
        cache                  = _dcache,
        master_snap            = _master_snap,
        holdings_sig           = None,
        open_option_positions  = {sym: _matching},
    )
    _opt_row       = _opt_scan.get("by_symbol", {}).get(sym, {})
    _sig_cache_hit = _opt_scan.get("cache_hit", False) or \
                     _opt_scan.get("options_cache_status") == "all_cached"

    if _opt_row.get("data_available"):
        _signal_block = {
            "ticker":                   sym,
            "has_open_option_position": _opt_row.get("has_open_option_position", True),
            "has_options":              _opt_row.get("has_options", True),
            "score":                    _opt_row.get("score"),
            "p_c":                      _opt_row.get("p_c"),
            "put_call":                 _opt_row.get("put_call"),
            "iv":                       _opt_row.get("iv"),
            "em":                       _opt_row.get("em"),
            "expected_move":            _opt_row.get("expected_move"),
            "vol":                      _opt_row.get("vol"),
            "volume":                   _opt_row.get("volume"),
            "call_volume":              _opt_row.get("call_volume"),
            "put_volume":               _opt_row.get("put_volume"),
            "open_interest":            _opt_row.get("open_interest"),
            "call_open_interest":       _opt_row.get("call_open_interest"),
            "put_open_interest":        _opt_row.get("put_open_interest"),
            "signal":                   _opt_row.get("signal"),
            "put_call_direction":       _opt_row.get("put_call_direction"),
            "confidence":               _opt_row.get("confidence"),
            "source":                   _opt_row.get("source", "portfolio_scoped_options_screener"),
        }
        _risk_block = {
            "risk_score":          _opt_row.get("risk_score"),
            "risk_level":          _opt_row.get("risk_level", "UNKNOWN"),
            "risk_signal":         _opt_row.get("risk_signal"),
            "risk_reasons":        _opt_row.get("risk_reasons", []),
            "risk_confidence":     _opt_row.get("risk_confidence", "LOW"),
            "risk_source":         _opt_row.get("risk_source", "portfolio_options_risk_v1"),
        }
    else:
        _unavail_reason = _opt_row.get("unavailable_reason", "options_data_unavailable")
        _signal_block = {
            "ticker":                   sym,
            "has_open_option_position": _opt_row.get("has_open_option_position", True),
            "has_options":              _opt_row.get("has_options", True),
            "score":                    None,
            "p_c":                      None,
            "put_call":                 None,
            "iv":                       None,
            "em":                       None,
            "expected_move":            None,
            "vol":                      None,
            "volume":                   None,
            "call_volume":              None,
            "put_volume":               None,
            "open_interest":            None,
            "call_open_interest":       None,
            "put_open_interest":        None,
            "signal":                   _opt_row.get("signal"),
            "put_call_direction":       _opt_row.get("put_call_direction"),
            "confidence":               None,
            "unavailable_reason":       _unavail_reason,
            "source":                   _opt_row.get("source", "portfolio_scoped_options_screener"),
            # Position-level fallback fields (populated when signal is CONTRACT DATA ONLY)
            "position_contracts":       _opt_row.get("position_contracts"),
            "position_expiration":      _opt_row.get("position_expiration"),
            "position_strike":          _opt_row.get("position_strike"),
            "position_option_type":     _opt_row.get("position_option_type"),
            "position_avg_premium":     _opt_row.get("position_avg_premium"),
        }
        _risk_block = {
            "risk_score":          None,
            "risk_level":          "UNKNOWN",
            "risk_signal":         None,
            "risk_reasons":        [_unavail_reason],
            "risk_confidence":     "LOW",
            "risk_source":         "portfolio_options_risk_v1",
        }

    # ── 7. Summary block ──────────────────────────────────────────────────────
    _upnl_total = round(_total_mv - _total_cost, 2) if _val_hit else None
    _upnl_pct_total = (
        round(_upnl_total / _total_cost * 100, 4)
        if (_upnl_total is not None and _total_cost)
        else None
    )
    _summary = {
        "contracts_total":      sum(float(p.get("contracts_open") or 0) for p in _matching),
        "cost_basis_total":     round(_total_cost, 2),
        "market_value_total":   round(_total_mv, 2) if _val_hit else None,
        "unrealized_pnl_total": _upnl_total,
        "unrealized_pnl_pct":   _upnl_pct_total,
        "valuation_available":  _val_hit > 0,
    }

    _resp_ms = round((_t_opd.perf_counter() - _t0_opd) * 1000, 1)

    print(
        f"[portfolio-option-popup-detail] "
        f"underlying={sym} "
        f"open_option_contracts={len(_matching)} "
        f"signal_found={_opt_row.get('data_available', False)} "
        f"risk_found={_opt_row.get('risk_score') is not None} "
        f"quote_found={_val_hit} "
        f"market_value_total={round(_total_mv, 2)} "
        f"source=portfolio_scoped_options_screener "
        f"provider_calls={_opt_scan.get('provider_calls', 0)} "
        f"cache_hit={_sig_cache_hit} "
        f"resp_ms={_resp_ms}"
    )

    return {
        "underlying":               sym,
        "tradingview_symbol":       sym,
        "chart_symbol":             sym,
        "company_name":             _company_name,
        "underlying_price":         _underlying_price,
        "asset_type":               "stock",
        "open_option_positions":    _enriched,
        "portfolio_options_signal": _signal_block,
        "portfolio_options_risk":   _risk_block,
        "summary":                  _summary,
        "cache": {
            "option_quote_cache_hit": _opt_qcache_hit,
            "signal_cache_hit":       _sig_cache_hit,
            "risk_cache_hit":         _sig_cache_hit,
        },
        "_debug": {
            "response_ms":          _resp_ms,
            "provider_calls":       _opt_scan.get("provider_calls", 0),
            "options_cache_status": _opt_scan.get("options_cache_status", "unknown"),
        },
    }


# ============================================================
# Options Position — Edit (PATCH) and Sell/Close (POST)
# ============================================================

@app.patch("/api/portfolio/options-positions/{occ_key}")
@limiter.limit("60/minute")
async def patch_option_position(
    request: Request,
    occ_key: str,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """Edit editable fields on an open option position.

    Updatable fields: contracts_open, avg_premium, cost_basis, entry_date,
    expiration_date, strike, option_type, notes.

    If contracts_open or avg_premium is changed but cost_basis is not
    explicitly supplied, cost_basis is recomputed as
    contracts_open × avg_premium × 100 (average-cost accounting).

    Returns 404 if the occ_key is not found.
    """
    if not _jwt_or_key(request, api_key):
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")
    try:
        body = await request.json()
    except Exception as _je:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {_je}")

    from data.option_trades_store import (
        load_option_position_by_occ_key as _load_one,
        update_option_position          as _update_pos,
    )
    from datetime import date as _dc

    pos = _load_one(occ_key)
    if pos is None:
        raise HTTPException(status_code=404, detail=f"Option position {occ_key!r} not found")

    updates: dict = {}

    if "contracts_open" in body:
        try:
            _c = float(body["contracts_open"])
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="contracts_open must be a number")
        if _c < 0:
            raise HTTPException(status_code=400, detail="contracts_open must be >= 0")
        updates["contracts_open"] = _c

    if "avg_premium" in body:
        try:
            _ap = float(body["avg_premium"])
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="avg_premium must be a number")
        if _ap < 0:
            raise HTTPException(status_code=400, detail="avg_premium must be >= 0")
        updates["avg_premium"] = _ap

    # cost_basis: explicit override wins; otherwise recompute when contracts or premium changes
    if "cost_basis" in body:
        try:
            updates["cost_basis"] = float(body["cost_basis"])
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="cost_basis must be a number")
    elif "contracts_open" in updates or "avg_premium" in updates:
        _contr_eff = updates.get("contracts_open", float(pos.get("contracts_open") or 0))
        _prem_eff  = updates.get("avg_premium",    float(pos.get("avg_premium")    or 0))
        updates["cost_basis"] = round(_contr_eff * _prem_eff * 100, 2)

    # entry_date maps to both first_entry_date and last_entry_date
    if "entry_date" in body:
        _ed = str(body["entry_date"] or "").split("T")[0]
        try:
            _dc.fromisoformat(_ed)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"entry_date must be YYYY-MM-DD, got {_ed!r}")
        updates["first_entry_date"] = _ed
        updates["last_entry_date"]  = _ed

    if "expiration_date" in body:
        _xp = str(body["expiration_date"] or "").split("T")[0]
        try:
            _dc.fromisoformat(_xp)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"expiration_date must be YYYY-MM-DD, got {_xp!r}")
        updates["expiration_date"] = _xp

    if "strike" in body:
        try:
            updates["strike"] = float(body["strike"])
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="strike must be a number")

    if "option_type" in body:
        _ot = str(body["option_type"] or "").upper()
        if _ot not in ("CALL", "PUT"):
            raise HTTPException(status_code=400, detail="option_type must be CALL or PUT")
        updates["option_type"] = _ot

    if "notes" in body:
        updates["notes"] = body["notes"]

    if not updates:
        return {"success": True, "occ_key": occ_key, "message": "no_changes", "position": pos}

    updated = _update_pos(occ_key, updates)
    if updated is None:
        raise HTTPException(status_code=500, detail="Failed to update option position — row not found after write")

    # Invalidate per-ticker options signal cache so next popup fetch is fresh
    try:
        from data.cache import cache as _dc2
        _dc2.delete(f"portfolio_opts:{(pos.get('underlying') or '').upper()}")
    except Exception:
        pass

    print(
        f"[PATCH_OPTION_POS] occ_key={occ_key}  fields={list(updates.keys())}  "
        f"contracts_open={updated.get('contracts_open')}  "
        f"avg_premium={updated.get('avg_premium')}  cost_basis={updated.get('cost_basis')}"
    )
    return {
        "success":       True,
        "occ_key":       occ_key,
        "updated_fields": list(updates.keys()),
        "position":      updated,
    }


@app.post("/api/portfolio/options-positions/{occ_key}/sell")
@limiter.limit("30/minute")
async def sell_option_position(
    request: Request,
    occ_key: str,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """Partial or full sell of an open option position (average-cost accounting).

    sell_type: "contracts" | "dollars" | "percent" | "full"
      contracts -> contracts_closed = body["contracts_closed"]
      dollars   -> contracts_closed = dollar_amount / (exit_premium × 100)
      percent   -> contracts_closed = contracts_open × percent_closed / 100
      full      -> contracts_closed = contracts_open

    Required: exit_premium >= 0, sell_type
    Optional: contracts_closed / dollar_amount / percent_closed (per sell_type),
              exit_date (default today), fees (default 0), close_reason

    Contract multiplier: 100 (standard US equity options).
    Accounting: average-cost — avg_premium unchanged on the remaining position.

    Returns 404 if occ_key not found.
    Returns 400 for invalid amounts or over-close attempts.
    """
    if not _jwt_or_key(request, api_key):
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")
    try:
        body = await request.json()
    except Exception as _je:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {_je}")

    from data.option_trades_store import (
        load_option_position_by_occ_key as _load_one,
        update_option_position          as _update_pos,
        delete_option_position          as _del_pos,
        save_option_closed_trade        as _save_ct,
        load_option_positions           as _load_all_pos,
    )
    from datetime import date as _dc_cls

    _MULT    = 100
    _EPSILON = 0.001   # CONTRACTS_EPSILON from option_ledger

    # ── Load and validate position ────────────────────────────────────────────
    pos = _load_one(occ_key)
    if pos is None:
        raise HTTPException(status_code=404, detail=f"Option position {occ_key!r} not found")

    _OPEN_ST = frozenset({"open", "partially_closed_open", "short_option_tracked_basic"})
    if (pos.get("final_status") or "open") not in _OPEN_ST:
        raise HTTPException(
            status_code=400,
            detail=f"Position {occ_key!r} is already fully closed (status={pos.get('final_status')})",
        )

    contracts_before = float(pos.get("contracts_open") or 0)
    avg_premium      = float(pos.get("avg_premium")    or 0)
    if contracts_before <= 0:
        raise HTTPException(status_code=400, detail=f"Position {occ_key!r} has 0 contracts_open")

    # ── sell_type ─────────────────────────────────────────────────────────────
    sell_type = (body.get("sell_type") or "").lower().strip()
    if sell_type not in ("contracts", "dollars", "percent", "full"):
        raise HTTPException(
            status_code=400,
            detail="sell_type must be one of: contracts, dollars, percent, full",
        )

    # ── exit_premium ──────────────────────────────────────────────────────────
    ep_raw = body.get("exit_premium")
    if ep_raw is None:
        raise HTTPException(status_code=400, detail="exit_premium is required")
    try:
        exit_premium = float(ep_raw)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="exit_premium must be a number")
    if exit_premium < 0:
        raise HTTPException(status_code=400, detail="exit_premium must be >= 0")

    # ── exit_date ─────────────────────────────────────────────────────────────
    exit_date = str(body.get("exit_date") or _dc_cls.today().isoformat()).split("T")[0]
    try:
        _dc_cls.fromisoformat(exit_date)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"exit_date must be YYYY-MM-DD, got {exit_date!r}")

    fees         = max(0.0, float(body.get("fees") or 0))
    close_reason = body.get("close_reason") or body.get("notes") or None

    # ── Resolve contracts_closed ──────────────────────────────────────────────
    if sell_type == "full":
        contracts_closed = contracts_before

    elif sell_type == "contracts":
        cc_raw = body.get("contracts_closed")
        if cc_raw is None:
            raise HTTPException(status_code=400, detail="contracts_closed is required for sell_type='contracts'")
        try:
            contracts_closed = float(cc_raw)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="contracts_closed must be a number")
        if contracts_closed <= 0:
            raise HTTPException(status_code=400, detail="contracts_closed must be > 0")
        if contracts_closed > contracts_before + _EPSILON:
            raise HTTPException(
                status_code=400,
                detail=f"Cannot close {contracts_closed} contracts — only {contracts_before} open",
            )
        contracts_closed = min(contracts_closed, contracts_before)

    elif sell_type == "dollars":
        da_raw = body.get("dollar_amount")
        if da_raw is None:
            raise HTTPException(status_code=400, detail="dollar_amount is required for sell_type='dollars'")
        try:
            dollar_amount = float(da_raw)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="dollar_amount must be a number")
        if dollar_amount <= 0:
            raise HTTPException(status_code=400, detail="dollar_amount must be > 0")
        if exit_premium <= 0:
            raise HTTPException(status_code=400, detail="exit_premium must be > 0 for sell_type='dollars'")
        contracts_closed = min(round(dollar_amount / (exit_premium * _MULT), 8), contracts_before)

    elif sell_type == "percent":
        pc_raw = body.get("percent_closed")
        if pc_raw is None:
            raise HTTPException(status_code=400, detail="percent_closed is required for sell_type='percent'")
        try:
            percent_closed = float(pc_raw)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="percent_closed must be a number")
        if percent_closed <= 0 or percent_closed > 100:
            raise HTTPException(status_code=400, detail="percent_closed must be between 0 and 100")
        contracts_closed = min(round(contracts_before * percent_closed / 100.0, 8), contracts_before)

    if contracts_closed <= 0:
        raise HTTPException(status_code=400, detail="Resolved contracts_closed must be > 0")

    # ── Average-cost P&L math ─────────────────────────────────────────────────
    proceeds          = round(contracts_closed * exit_premium * _MULT - fees, 2)
    cost_basis_closed = round(avg_premium * contracts_closed * _MULT, 2)
    realized_pnl      = round(proceeds - cost_basis_closed, 2)
    realized_pnl_pct  = (
        round(realized_pnl / cost_basis_closed * 100, 4) if cost_basis_closed else None
    )

    contracts_remaining = round(contracts_before - contracts_closed, 8)
    is_full_close       = contracts_remaining <= _EPSILON
    if is_full_close:
        contracts_remaining = 0.0

    final_opt_status = "fully_closed" if is_full_close else "partially_closed_open"

    # ── Closed trade record ───────────────────────────────────────────────────
    _entry_date  = pos.get("first_entry_date") or pos.get("last_entry_date") or None
    _display_sym = pos.get("display_symbol") or occ_key

    trade_payload = {
        "occ_key":                   occ_key,
        "underlying":                pos.get("underlying", ""),
        "display_symbol":            _display_sym,
        "expiration_date":           pos.get("expiration_date"),
        "strike":                    pos.get("strike"),
        "option_type":               pos.get("option_type", ""),
        "contracts_closed":          round(contracts_closed, 8),
        "entry_date":                _entry_date,
        "exit_date":                 exit_date,
        "avg_entry_premium":         avg_premium,
        "exit_premium":              exit_premium,
        "cost_basis_sold":           cost_basis_closed,
        "proceeds":                  proceeds,
        "fees":                      fees,
        "realized_pnl":              realized_pnl,
        "realized_pnl_pct":          realized_pnl_pct,
        "contracts_remaining_after": contracts_remaining,
        "is_full_close":             is_full_close,
        "close_type":                sell_type,
        "final_option_status":       final_opt_status,
        "source":                    "dashboard_manual",
        "notes":                     close_reason,
        "sell_type":                 sell_type,
        "cost_method":               "average_cost",
    }

    closed_trade = _save_ct(trade_payload)
    if closed_trade.get("_error"):
        raise HTTPException(
            status_code=500,
            detail=f"Failed to save closed trade: {closed_trade['_error']}",
        )

    # ── Update the open position (mark fully_closed or update partial) ───────
    # We always UPDATE (never delete) so fully-closed positions remain visible
    # in option_fully_closed_positions inside GET /api/portfolio/holdings.
    updated_pos: dict | None = None
    if is_full_close:
        _full_close_updates = {
            "contracts_open":  0.0,
            "cost_basis":      0.0,
            "contracts_sold":  round(float(pos.get("contracts_sold") or 0) + contracts_closed, 8),
            "realized_pnl":    round(float(pos.get("realized_pnl") or 0) + realized_pnl, 2),
            "last_exit_date":  exit_date,
            "final_status":    "fully_closed",
        }
        _update_pos(occ_key, _full_close_updates)
        # updated_pos stays None — position is closed
    else:
        _remaining_cb   = round(avg_premium * contracts_remaining * _MULT, 2)
        _pos_updates    = {
            "contracts_open":  contracts_remaining,
            "cost_basis":      _remaining_cb,
            "contracts_sold":  round(float(pos.get("contracts_sold") or 0) + contracts_closed, 8),
            "realized_pnl":    round(float(pos.get("realized_pnl") or 0) + realized_pnl, 2),
            "last_exit_date":  exit_date,
            "final_status":    "partially_closed_open",
        }
        updated_pos = _update_pos(occ_key, _pos_updates)

    # ── Invalidate per-ticker options signal cache ────────────────────────────
    try:
        from data.cache import cache as _dcache2
        _dcache2.delete(f"portfolio_opts:{(pos.get('underlying') or '').upper()}")
    except Exception:
        pass

    # ── Live portfolio summary from refreshed positions ───────────────────────
    _OPEN_ST_S    = frozenset({"open", "partially_closed_open", "short_option_tracked_basic"})
    _remaining    = _load_all_pos()
    _still_open   = [p for p in _remaining if (p.get("final_status") or "open") in _OPEN_ST_S]
    _psum_cost    = round(sum(float(p.get("cost_basis") or 0)      for p in _still_open), 2)
    _psum_contr   = round(sum(float(p.get("contracts_open") or 0)  for p in _still_open), 4)

    print(
        f"[SELL_OPTION_POS] occ_key={occ_key}  sell_type={sell_type}  "
        f"contracts_closed={contracts_closed:.4f}  remaining={contracts_remaining:.4f}  "
        f"exit_premium={exit_premium}  proceeds={proceeds}  realized_pnl={realized_pnl}  "
        f"is_full_close={is_full_close}  trade_id={closed_trade.get('id')}"
    )

    return {
        "success":              True,
        "occ_key":              occ_key,
        "underlying":           pos.get("underlying", ""),
        "sell_type":            sell_type,
        "contracts_before":     contracts_before,
        "contracts_closed":     round(contracts_closed, 8),
        "contracts_remaining":  contracts_remaining,
        "exit_premium":         exit_premium,
        "proceeds":             proceeds,
        "cost_basis_closed":    cost_basis_closed,
        "realized_pnl":         realized_pnl,
        "realized_pnl_pct":     realized_pnl_pct,
        "fees":                 fees,
        "is_full_close":        is_full_close,
        "final_option_status":  final_opt_status,
        "closed_trade":         closed_trade,
        "open_option_position": updated_pos,
        "portfolio_summary": {
            "open_option_positions_count": len(_still_open),
            "total_contracts_open":        _psum_contr,
            "total_cost_basis_open":       _psum_cost,
        },
    }


# ============================================================
# Portfolio Review (AI-powered Buy/Hold/Sell analysis)
# ============================================================

@app.post("/api/portfolio/review")
@limiter.limit("20/minute")
async def review_portfolio(request: Request, api_key: str = Header(None, alias="X-API-Key"), _sub: None = Depends(require_subscription)):
    """Portfolio Review Agent — streaming keepalive response, claude-sonnet-4-5, deterministic fallback."""
    import asyncio
    import time as _time
    import json as _j
    from datetime import datetime, timezone as _tz

    await _wait_for_init()
    body = await request.json()

    holdings = body.get("holdings", [])
    print(f"[PORTFOLIO_REVIEW] === ENDPOINT HIT === tickers={[h.get('ticker') for h in holdings]}", flush=True)

    if not holdings:
        return JSONResponse(status_code=200, content={
            "ok": False,
            "agent_status": "no_holdings",
            "type": "portfolio_review",
            "message": "No holdings to review. Add some positions to your portfolio first.",
            "structured": {"display_type": "chat", "message": "No holdings to review. Add some positions to your portfolio first."},
        })

    async def _do_review():
        """All work in one coroutine — launched as a task so the streaming loop can send keepalives."""
        import hashlib as _hashlib
        from data.cache import cache as _cache
        from data.portfolio_options_service import scan_portfolio_options as _scan_portfolio_opts
        from services.portfolio_review_service import (
            get_portfolio_social_ranking as _get_social_ranking,
            build_portfolio_context as _build_context,
            build_deterministic_review as _build_deterministic,
            build_review_prompt as _build_prompt,
            parse_claude_review as _parse_review,
            flatten_review_to_text as _flatten_text,
            raw_inputs_used as _raw_inputs,
            _SOCIAL_CACHE, _SOCIAL_TTL, _holdings_sig as _hsig,
        )
        from services.watchlist_service import list_watchlists as _list_wl, load_watchlist as _load_wl
        from services.x_consensus_cache import _load_disk_cache as _xc_load
        from agent.prompts import SYSTEM_PROMPT as _SYSTEM_PROMPT

        start = _time.time()
        tickers = [h.get("ticker", "").upper().strip() for h in holdings if h.get("ticker")]
        _sig = _hashlib.md5(",".join(sorted(tickers)).encode()).hexdigest()[:12]

        _master_snap = _cache.get(_OPTIONS_MASTER_CACHE_KEY) or _cache.get(_OPTIONS_MASTER_LKG_KEY)

        async def _fetch_options():
            if not data_service or not data_service.tradier:
                return {}
            try:
                return await asyncio.wait_for(
                    _scan_portfolio_opts(symbols=tickers, tradier=data_service.tradier,
                                        cache=_cache, master_snap=_master_snap, holdings_sig=_sig),
                    timeout=12.0,
                )
            except Exception as _e:
                print(f"[PORTFOLIO_REVIEW] options fetch failed: {_e}", flush=True)
                return {}

        async def _fetch_macro():
            try:
                return await asyncio.wait_for(agent.data._build_macro_snapshot(), timeout=6.0)
            except Exception as _e:
                print(f"[PORTFOLIO_REVIEW] macro failed: {_e}", flush=True)
                return {}

        async def _fetch_watchlist():
            try:
                wls = _list_wl()
                if not wls:
                    return []
                wld = _load_wl(wls[0].get("id"))
                return (wld.get("tickers") or [])[:20] if wld else []
            except Exception as _e:
                print(f"[PORTFOLIO_REVIEW] watchlist failed: {_e}", flush=True)
                return []

        async def _fetch_xc():
            try:
                return await asyncio.to_thread(_xc_load) or {}
            except Exception:
                return {}

        # Social: cache-only unless include_social=true
        _social_sig = _hsig(tickers)
        _cached_social = _SOCIAL_CACHE.get(_social_sig)
        _include_social = bool(body.get("include_social", False))
        if _cached_social and (_time.time() - _cached_social[0]) < _SOCIAL_TTL:
            _social_res = _cached_social[1]
            print("[PORTFOLIO_REVIEW] XAI social: cache hit", flush=True)
        elif _include_social:
            try:
                xai_p = getattr(agent.data, "xai", None)
                _social_res = await asyncio.wait_for(_get_social_ranking(xai_p, tickers), timeout=18.0)
                print("[PORTFOLIO_REVIEW] XAI social: fetched on request", flush=True)
            except Exception as _se:
                _social_res = {"status": "unavailable", "ranked": []}
                print(f"[PORTFOLIO_REVIEW] XAI social failed: {_se}", flush=True)
        else:
            _social_res = {"status": "unavailable", "ranked": []}

        _opts_res, _macro_res, _wl_tickers, _xc_data = await asyncio.gather(
            _fetch_options(), _fetch_macro(), _fetch_watchlist(), _fetch_xc(),
            return_exceptions=True,
        )
        if isinstance(_opts_res, Exception):   _opts_res   = {}
        if isinstance(_macro_res, Exception):  _macro_res  = {}
        if isinstance(_wl_tickers, Exception): _wl_tickers = []
        if isinstance(_xc_data, Exception):    _xc_data    = {}

        print(
            f"[PORTFOLIO_REVIEW] data gathered {_time.time()-start:.1f}s | "
            f"opts={bool(_opts_res)} macro={bool(_macro_res)} "
            f"social={(_social_res or {}).get('status','?')} "
            f"wl={len(_wl_tickers or [])} xc={bool(_xc_data)}",
            flush=True,
        )

        _opts_by_sym = (_opts_res or {}).get("by_symbol", {}) if isinstance(_opts_res, dict) else {}
        _context = _build_context(
            holdings=holdings,
            options_data=_opts_by_sym,
            macro_snapshot=_macro_res if isinstance(_macro_res, dict) else {},
            social_ranking=_social_res if isinstance(_social_res, dict) else {},
            watchlist_tickers=_wl_tickers if isinstance(_wl_tickers, list) else [],
            x_consensus=_xc_data if isinstance(_xc_data, dict) else {},
        )

        # Slim context — strips verbose nested objects so Claude writes terse views
        def _slim_ctx(ctx: dict) -> str:
            port = ctx.get("portfolio", {})
            slim = {
                "portfolio": {
                    "count": port.get("count"),
                    "hhi": port.get("hhi"),
                    "max_weight_pct": port.get("max_weight_pct"),
                    "total_return_pct": port.get("total_return_pct"),
                },
                "macro": {k: v for k, v in ctx.get("macro", {}).items() if v is not None},
                "holdings": [
                    {k: v for k, v in {
                        "ticker":   r.get("ticker"),
                        "weight":   r.get("weight_pct"),
                        "cost":     r.get("avg_cost"),
                        "pnl_pct":  r.get("pnl_pct"),
                        "price":    r.get("current_price"),
                        "opts":     (r.get("options") or {}).get("signal"),
                        "soc":      (r.get("social") or {}).get("sentiment"),
                        "xc":       (r.get("x_consensus") or {}).get("sentiment"),
                    }.items() if v is not None}
                    for r in ctx.get("holdings", [])
                ],
            }
            wl = ctx.get("watchlist_candidates", [])
            if wl:
                slim["watchlist_top"] = [(t if isinstance(t, str) else t.get("ticker", "")) for t in wl[:5] if t]
            return _j.dumps(slim, separators=(",", ":"), default=str)

        _ctx_str = _slim_ctx(_context)
        print(f"[PORTFOLIO_REVIEW] context {len(_ctx_str):,} chars | model={MODEL_CLAUDE_PREMIUM}", flush=True)

        _claude_result = None
        _agent_error = None
        _prompt = _build_prompt(_ctx_str, bool(_wl_tickers), n_holdings=len(holdings))
        try:
            _resp = await asyncio.to_thread(
                agent.client.messages.create,
                model=MODEL_CLAUDE_PREMIUM,
                max_tokens=4096,
                system=_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": _prompt}],
            )
            _raw = _resp.content[0].text.strip()
            print(f"[PORTFOLIO_REVIEW] Claude {len(_raw)} chars ({_time.time()-start:.1f}s total)", flush=True)
            _claude_result = _parse_review(_raw)
            if not _claude_result:
                _agent_error = "Claude response did not match expected JSON structure"
                print(f"[PORTFOLIO_REVIEW] parse fail | first300: {_raw[:300]!r}", flush=True)
        except Exception as _ce:
            import traceback as _tb; _tb.print_exc()
            _agent_error = f"LLM error: {type(_ce).__name__}: {str(_ce)[:120]}"
            print(f"[PORTFOLIO_REVIEW] Claude error: {_ce}", flush=True)

        _as_of = datetime.now(_tz.utc).isoformat()
        _raw_in = _raw_inputs(_context, _social_res, _opts_by_sym, bool(_macro_res), bool(_wl_tickers))

        if _claude_result and isinstance(_claude_result, dict):
            _txt = _flatten_text(_claude_result)
            return {
                "ok": True, "agent_status": "ok", "as_of": _as_of,
                "portfolio_summary":     _claude_result.get("portfolio_summary", {}),
                "weighting_suggestions": _claude_result.get("weighting_suggestions", []),
                "asset_reviews":         _claude_result.get("asset_reviews", []),
                "watchlist_swaps":       _claude_result.get("watchlist_swaps", []),
                "risk_flags":            _claude_result.get("risk_flags", []),
                "raw_inputs_used": _raw_in, "unavailable_inputs": [],
                "agent_error_summary": None, "type": "portfolio_review",
                "analysis": _txt,
                "message": (_claude_result.get("portfolio_summary") or {}).get("headline", "Portfolio review complete."),
                "structured": {"display_type": "portfolio_review", **_claude_result},
            }
        else:
            _fallback = _build_deterministic(_context)
            _fallback["agent_error_summary"] = _agent_error or "LLM synthesis unavailable"
            _fallback["raw_inputs_used"] = _raw_in
            _txt = _flatten_text(_fallback)
            return {
                **_fallback, "as_of": _as_of, "type": "portfolio_review",
                "analysis": _txt,
                "message": (_fallback.get("portfolio_summary") or {}).get("headline", "Portfolio review (fallback)."),
                "structured": {"display_type": "portfolio_review", **_fallback},
            }

    async def _stream():
        """Streaming wrapper: yields keepalive spaces while _do_review() runs, then the JSON result."""
        task = asyncio.ensure_future(_do_review())
        try:
            while True:
                try:
                    result = await asyncio.wait_for(asyncio.shield(task), timeout=8.0)
                    yield _j.dumps(result, default=str).encode()
                    return
                except asyncio.TimeoutError:
                    yield b" "  # keepalive — proxy sees bytes flowing, stays alive
        except Exception as _exc:
            import traceback as _tb; _tb.print_exc()
            print(f"[PORTFOLIO_REVIEW] FATAL: {_exc}", flush=True)
            task.cancel()
            yield _j.dumps({
                "ok": False, "agent_status": "error", "type": "portfolio_review",
                "message": "Portfolio review encountered an error. Please try again.",
                "structured": {"display_type": "chat", "message": "Portfolio review encountered an error. Please try again."},
                "agent_error_summary": str(_exc)[:200],
            }, default=str).encode()

    return StreamingResponse(_stream(), media_type="application/json")




# ============================================================
# Portfolio Compare-to-Watchlist  (3 endpoints)
# ============================================================

@app.get("/api/portfolio/compare-watchlist/options")
@limiter.limit("30/minute")
async def compare_watchlist_options(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """Return all saved watchlists the user can compare against their portfolio."""
    if not _jwt_or_key(request, api_key):
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")

    from services.watchlist_service import list_watchlists

    try:
        watchlists_raw = list_watchlists()
    except Exception as e:
        print(f"[COMPARE] list_watchlists error: {e}")
        watchlists_raw = []

    watchlists = [
        {
            "id":           w.get("id", "default"),
            "name":         w.get("name", "Watchlist"),
            "ticker_count": w.get("ticker_count", 0),
            "updated_at":   w.get("saved_at") or w.get("updated_at"),
        }
        for w in (watchlists_raw or [])
    ]

    return {
        "ok": True,
        "watchlists": watchlists,
        "default_watchlist_id": watchlists[0]["id"] if watchlists else None,
    }


# ============================================================
# Watchlist Options Signals
# ============================================================

@app.get("/api/watchlist/{watchlist_id}/options-signals")
@limiter.limit("30/minute")
async def watchlist_options_signals(
    request: Request,
    watchlist_id: str,
    force_refresh: bool = Query(False),
    api_key: str = Header(None, alias="X-API-Key"),
):
    """
    Options signal snapshot for all tickers in a watchlist.

    Returns immediately from cache — never blocks on Tradier calls.

    Cache layers (shared with Portfolio Terminal via portfolio_opts:{sym} keys):
      1. Per-ticker memory cache (300s TTL)
      2. Master options screener snapshot
      3. Disk LKG (survives restarts / outside market hours)

    Uncached tickers are returned immediately as stale placeholders
    (options_stale=True, options_unavailable_reason="scan_pending") while
    background Tradier scans are enqueued in batches through the existing
    rate-limiter/spacer — no second limiter, no FMP calls.

    Response:
      {
        "signals":      { "AAPL": { options_score, options_signal, options_iv, ... } },
        "options_meta": { scope, symbols_requested, cache_hits, cache_misses,
                          live_calls_enqueued, live_calls_completed,
                          rate_limited_or_deferred, generated_at, ttl_seconds }
      }
    """
    if not _jwt_or_key(request, api_key):
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")

    from services.watchlist_service import load_watchlist as _load_wl
    from data.portfolio_options_service import scan_watchlist_options as _scan_wl_opts
    from data.cache import cache as _cache

    store = _load_wl(watchlist_id)
    if store is None:
        import datetime as _dt
        return {
            "signals": {},
            "options_meta": {
                "scope": "watchlist",
                "symbols_requested": 0,
                "cache_hits": 0,
                "cache_misses": 0,
                "live_calls_enqueued": 0,
                "live_calls_completed": 0,
                "rate_limited_or_deferred": 0,
                "generated_at": _dt.datetime.utcnow().isoformat() + "Z",
                "ttl_seconds": 300,
                "error": "watchlist_not_found",
            },
        }

    tickers = [t.strip().upper() for t in (store.get("tickers") or []) if t.strip()]
    if not tickers:
        import datetime as _dt
        return {
            "signals": {},
            "options_meta": {
                "scope": "watchlist",
                "symbols_requested": 0,
                "cache_hits": 0,
                "cache_misses": 0,
                "live_calls_enqueued": 0,
                "live_calls_completed": 0,
                "rate_limited_or_deferred": 0,
                "generated_at": _dt.datetime.utcnow().isoformat() + "Z",
                "ttl_seconds": 300,
            },
        }

    master_snap = _cache.get(_OPTIONS_MASTER_CACHE_KEY) or _cache.get(_OPTIONS_MASTER_LKG_KEY)
    _tradier    = data_service.tradier if data_service else None

    result = await _scan_wl_opts(
        symbols       = tickers,
        tradier       = _tradier,
        cache         = _cache,
        master_snap   = master_snap,
        force_refresh = force_refresh,
        watchlist_id  = watchlist_id,
    )

    print(
        f"[WATCHLIST_OPTIONS_SIGNALS] id={watchlist_id!r} "
        f"tickers={len(tickers)} "
        f"cache_hits={result['options_meta'].get('cache_hits',0)} "
        f"cache_misses={result['options_meta'].get('cache_misses',0)} "
        f"enqueued={result['options_meta'].get('live_calls_enqueued',0)}"
    )
    return result


@app.get("/api/portfolio/compare-watchlist/latest")
@limiter.limit("30/minute")
async def compare_watchlist_latest(
    request: Request,
    watchlist_id: str = None,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """
    Return the most recent saved comparison report for user+watchlist.
    Does NOT regenerate anything — read-only.
    """
    if not _jwt_or_key(request, api_key):
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")

    user_id = getattr(request.state, "user_id", "default")

    if not watchlist_id:
        return {"ok": False, "exists": False, "message": "watchlist_id query param is required."}

    from services.portfolio_compare_service import load_report, check_staleness
    from services.watchlist_service import load_watchlist

    # Resolve current portfolio and watchlist tickers for staleness check
    portfolio_tickers: list[str] = []
    try:
        pf = _portfolio_file(user_id)
        if pf.exists():
            with open(pf) as f:
                pd_ = _json.load(f)
            portfolio_tickers = [
                h.get("ticker", "").upper().strip()
                for h in (pd_.get("holdings") or []) if h.get("ticker")
            ]
    except Exception:
        pass

    watchlist_tickers: list[str] = []
    try:
        wd = load_watchlist(watchlist_id)
        if wd:
            watchlist_tickers = wd.get("tickers") or []
    except Exception:
        pass

    report = load_report(user_id, watchlist_id)
    if not report:
        return {
            "ok": True,
            "exists": False,
            "message": "No saved comparison report exists for this portfolio/watchlist combination yet.",
        }

    stale, stale_reasons = check_staleness(report, portfolio_tickers, watchlist_tickers)
    report["stale"]        = stale
    report["stale_reasons"] = stale_reasons
    report["cache_status"] = "stale_cached" if stale else "cached"
    report["ok"]           = True
    report["exists"]       = True
    return report


@app.post("/api/portfolio/compare-watchlist/run")
@limiter.limit("3/minute")
async def compare_watchlist_run(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
    _sub: None = Depends(require_subscription),
):
    """
    Manually generate (or refresh) the expensive portfolio vs watchlist report.
    Body: { "watchlist_id": "...", "force_refresh": false }

    Rate-limited to 3/minute to prevent abuse.  Each run may take 30-90s.
    Returns the full structured report + markdown.
    """
    if not _jwt_or_key(request, api_key):
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")

    await _wait_for_init()

    user_id = getattr(request.state, "user_id", "default")
    body = await request.json()
    watchlist_id   = (body.get("watchlist_id") or "").strip()
    force_refresh  = bool(body.get("force_refresh", False))

    if not watchlist_id:
        raise HTTPException(status_code=400, detail="watchlist_id is required.")

    from services.watchlist_service import load_watchlist
    from services.portfolio_compare_service import run_comparison

    # ── Load portfolio from canonical source ──────────────────────────────────
    from data.portfolio_store import load_active_holdings as _load_pf
    portfolio_holdings = _load_pf()
    if not portfolio_holdings:
        raise HTTPException(
            status_code=400,
            detail="No portfolio found. Please add or upload your holdings first.",
        )

    # ── Load watchlist ────────────────────────────────────────────────────────
    try:
        watchlist_data = load_watchlist(watchlist_id)
    except Exception as e:
        watchlist_data = None
        print(f"[COMPARE] load_watchlist error: {e}")

    if not watchlist_data:
        raise HTTPException(
            status_code=404,
            detail=f"Watchlist '{watchlist_id}' not found. Please save a watchlist first.",
        )
    if not watchlist_data.get("tickers"):
        raise HTTPException(
            status_code=400,
            detail=f"Watchlist '{watchlist_data.get('name', watchlist_id)}' has no tickers.",
        )

    # ── Run comparison ────────────────────────────────────────────────────────
    print(f"[COMPARE] Starting run: user={user_id} watchlist={watchlist_id} force={force_refresh}")

    try:
        result = await run_comparison(
            user_id=user_id,
            watchlist_id=watchlist_id,
            portfolio_holdings=portfolio_holdings,
            watchlist_data=watchlist_data,
            data_service=agent.data,
            claude_client=agent.client,
            force_refresh=force_refresh,
        )
    except Exception as e:
        import traceback as _tb
        _tb.print_exc()
        print(f"[COMPARE] run_comparison FATAL: {e}")
        raise HTTPException(status_code=500, detail=f"Comparison failed: {str(e)[:300]}")

    result["exists"] = True
    return result


# ── Closed Trades (/api/portfolio/closed-trades) ──────────────────────────────

@app.get("/api/portfolio/closed-trades")
async def get_closed_trades(request: Request, api_key: str = Header(None, alias="X-API-Key")):
    """Return closed trades grouped by trade lifecycle, enriched with current_price.

    Response shape
    --------------
    {
      "trade_groups": [                  # ← primary payload for the frontend
        {
          "trade_group_id": str,
          "ticker": str,
          "entry_date": "YYYY-MM-DD",
          "final_exit_date": "YYYY-MM-DD" | null,
          "total_shares_sold": float,
          "avg_entry_price": float,
          "avg_exit_price": float,
          "total_cost_basis": float,
          "total_exit_value": float,
          "total_realized_pnl": float,
          "total_realized_pnl_pct": float,
          "holding_period_days": int | null,
          "is_fully_closed": bool,       # true when final sell has is_full_close=True
          "current_price": float | null,
          "sell_events": [               # individual sell rows, asc by exit_date
            { id, ticker, shares, entry_date, exit_date, entry_price, exit_price,
              realized_pnl, realized_pnl_pct, sell_type, remaining_shares_after,
              is_full_close, trade_group_id, ... }
          ]
        },
        ...
      ],
      "closed_trades": [...],            # ← flat list (backward compat)
      "count": int
    }

    Partial sells of the same holding (same trade lifecycle) share a
    trade_group_id and appear as one group with multiple sell_events.
    Legacy records without a trade_group_id each form their own single-event group.

    current_price is sourced from the shared Tradier per-ticker cache.
    """
    if not _jwt_or_key(request, api_key):
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")
    from data.closed_trades_store import (
        load_closed_trades as _load_ct,
        load_closed_trades_grouped as _load_grouped,
    )
    trades = _load_ct()
    trade_groups = _load_grouped(trades)

    # ── Enrich with current_price via shared Tradier per-ticker cache ──────────
    _CRYPTO_SYMBOLS = {
        "BTC","ETH","SOL","DOGE","ADA","XRP","DOT","LINK","AVAX","MATIC","UNI",
        "AAVE","ATOM","LTC","BCH","SHIB","NEAR","SUI","APT","ARB","OP","INJ",
        "TIA","SEI","PEPE","WIF","RENDER","FET","TAO","FIL","HYPE",
    }
    unique_tickers = list({
        (t.get("ticker") or "").upper()
        for t in trades
        if (t.get("ticker") or "").upper() and
           (t.get("ticker") or "").upper() not in _CRYPTO_SYMBOLS
    })

    price_map: dict[str, float | None] = {}
    if unique_tickers:
        try:
            from data.tradier_provider import TradierProvider as _TP
            _tradier = _TP(api_key=os.getenv("TRADIER_API_KEY", ""))
            from data.tradier_budget import lane as _ct_lane
            with _ct_lane("quotes"):
                quotes = await _tradier.get_quotes(unique_tickers)
            for q in quotes:
                sym = (q.get("symbol") or "").upper()
                price = q.get("last") or q.get("price") or q.get("close")
                if sym and price is not None:
                    price_map[sym] = float(price)
        except Exception as _pe:
            print(f"[CLOSED_TRADES] Tradier enrichment error: {_pe}")

        missing_price = [s for s in unique_tickers if price_map.get(s) is None]
        if missing_price:
            try:
                import yfinance as _yf
                import asyncio as _aio
                def _yf_fetch():
                    batch = _yf.Tickers(" ".join(missing_price))
                    out = {}
                    for sym, tkr in batch.tickers.items():
                        try:
                            p = tkr.fast_info.get("lastPrice") or tkr.fast_info.get("regularMarketPrice")
                            if p:
                                out[sym.upper()] = float(p)
                        except Exception:
                            pass
                    return out
                yf_prices = await _aio.to_thread(_yf_fetch)
                price_map.update(yf_prices)
                print(f"[CLOSED_TRADES] yfinance filled {len(yf_prices)} OTC prices: {list(yf_prices.keys())}")
            except Exception as _yfe:
                print(f"[CLOSED_TRADES] yfinance enrichment error: {_yfe}")

    # Enrich flat list (backward compat)
    for t in trades:
        sym = (t.get("ticker") or "").upper()
        t["current_price"] = price_map.get(sym)

    # Enrich grouped list (current_price on the group + each sell_event)
    for g in trade_groups:
        sym = (g.get("ticker") or "").upper()
        g["current_price"] = price_map.get(sym)
        for ev in g.get("sell_events", []):
            ev["current_price"] = price_map.get((ev.get("ticker") or "").upper())

    # ── Pre-compute portfolio performance summary ─────────────────────────────
    # Only fully-closed groups count toward win rate / avg return
    fully_closed = [g for g in trade_groups if g.get("is_fully_closed")]
    partial_open  = [g for g in trade_groups if not g.get("is_fully_closed")]

    wins   = [g for g in fully_closed if (g.get("total_realized_pnl") or 0) > 0]
    losses = [g for g in fully_closed if (g.get("total_realized_pnl") or 0) <= 0]

    total_closed_count = len(fully_closed)
    win_count          = len(wins)
    loss_count         = len(losses)
    win_rate           = round(win_count / total_closed_count * 100, 1) if total_closed_count else 0.0

    all_pnl_pcts  = [g["total_realized_pnl_pct"] for g in fully_closed
                     if g.get("total_realized_pnl_pct") is not None]
    avg_return_pct = round(sum(all_pnl_pcts) / len(all_pnl_pcts), 2) if all_pnl_pcts else 0.0

    total_realized_pnl = round(sum(g.get("total_realized_pnl") or 0 for g in fully_closed), 2)

    best_trade  = max(fully_closed, key=lambda g: g.get("total_realized_pnl_pct") or 0, default=None)
    worst_trade = min(fully_closed, key=lambda g: g.get("total_realized_pnl_pct") or 0, default=None)

    all_hold_days = [g["holding_period_days"] for g in fully_closed
                     if g.get("holding_period_days") is not None]
    avg_hold_days = round(sum(all_hold_days) / len(all_hold_days), 0) if all_hold_days else None

    portfolio_performance = {
        # Win rate & counts (per trade lifecycle, not per lot)
        "total_closed_trades":   total_closed_count,
        "open_partial_trades":   len(partial_open),
        "win_count":             win_count,
        "loss_count":            loss_count,
        "win_rate":              win_rate,              # 0-100 pct
        # Return stats
        "avg_return_pct":        avg_return_pct,        # avg realized pnl_pct across closed trades
        "total_realized_pnl":    total_realized_pnl,    # sum $
        # Best / worst single lifecycle
        "best_trade": {
            "ticker":        best_trade["ticker"],
            "pnl_pct":       best_trade["total_realized_pnl_pct"],
            "pnl":           best_trade["total_realized_pnl"],
            "exit_date":     best_trade["final_exit_date"],
        } if best_trade else None,
        "worst_trade": {
            "ticker":        worst_trade["ticker"],
            "pnl_pct":       worst_trade["total_realized_pnl_pct"],
            "pnl":           worst_trade["total_realized_pnl"],
            "exit_date":     worst_trade["final_exit_date"],
        } if worst_trade else None,
        # Holding period
        "avg_hold_days":        avg_hold_days,
        # Streak helpers (sorted newest-first for the UI)
        "recent_trades": [
            {
                "ticker":    g["ticker"],
                "pnl_pct":   g["total_realized_pnl_pct"],
                "pnl":       g["total_realized_pnl"],
                "exit_date": g["final_exit_date"],
                "won":       (g.get("total_realized_pnl") or 0) > 0,
            }
            for g in sorted(
                fully_closed,
                key=lambda g: g.get("final_exit_date") or "",
                reverse=True,
            )[:10]
        ],
    }

    return {
        "trade_groups":          trade_groups,
        "closed_trades":         trades,           # flat list kept for backward compat
        "count":                 len(trades),
        "portfolio_performance": portfolio_performance,
    }


@app.post("/api/portfolio/closed-trades")
async def add_closed_trade(request: Request, api_key: str = Header(None, alias="X-API-Key")):
    """Add a closed trade.

    Body: {
      ticker, shares, entry_date, exit_date,
      entry_price, exit_price,
      realized_pnl?,        # auto-computed if omitted
      realized_pnl_pct?,    # auto-computed if omitted
      holding_period_days?, # auto-computed if omitted
      notes?
    }
    """
    if not _jwt_or_key(request, api_key):
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")
    try:
        body = await request.json()
    except Exception as _je:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {_je}")

    ticker = (body.get("ticker") or "").strip().upper()
    if not ticker:
        raise HTTPException(status_code=400, detail="ticker is required")
    if not body.get("exit_date"):
        raise HTTPException(status_code=400, detail="exit_date is required")

    import uuid as _uuid_mod
    from data.closed_trades_store import save_closed_trade as _save_ct

    # Determine is_full_close: explicit field, or infer from remaining_shares_after==0
    _remaining = body.get("remaining_shares_after")
    _is_full_close = body.get("is_full_close")
    if _is_full_close is None:
        _is_full_close = (_remaining is not None and float(_remaining) == 0)

    trade = {
        "ticker":                 ticker,
        "shares":                 float(body.get("shares") or 0),
        "entry_date":             body.get("entry_date") or None,
        "exit_date":              body.get("exit_date"),
        "entry_price":            float(body.get("entry_price") or body.get("avg_cost") or 0),
        "exit_price":             float(body.get("exit_price") or 0),
        "realized_pnl":           body.get("realized_pnl"),
        "realized_pnl_pct":       body.get("realized_pnl_pct"),
        "holding_period_days":    body.get("holding_period_days"),
        "notes":                  body.get("notes"),
        "sell_type":              body.get("sell_type") or ("full" if _is_full_close else "partial"),
        "remaining_shares_after": float(_remaining) if _remaining is not None else None,
        "cost_method":            body.get("cost_method") or "average_cost",
        "trade_group_id":         body.get("trade_group_id") or str(_uuid_mod.uuid4()),
        "is_full_close":          bool(_is_full_close),
    }
    result = _save_ct(trade)
    if result.get("_error"):
        raise HTTPException(status_code=500, detail=result["_error"])

    # If this is a full close, remove the ticker from active holdings
    if bool(_is_full_close):
        try:
            from data.portfolio_store import (
                load_active_holdings as _load_h,
                save_active_holdings as _save_h,
            )
            _holdings = _load_h()
            _remaining_h = [h for h in _holdings if h.get("ticker", "").upper() != ticker]
            if len(_remaining_h) < len(_holdings):
                _save_h(_remaining_h)
                print(f"[CLOSED_TRADES] Removed {ticker} from active holdings via POST (is_full_close=True)")
        except Exception as _he:
            print(f"[CLOSED_TRADES] Warning: could not remove {ticker} from active holdings: {_he}")

    # Invalidate terminal cache so next chart load includes this trade
    try:
        from data.caelyn_terminal import CaelynTerminalProvider
        from data.cache import cache as _app_cache
        from data.portfolio_store import canonical_file as _cf
        _app_cache.delete(CaelynTerminalProvider.cache_key_for(_cf()))
    except Exception:
        pass

    return {"success": True, "trade": result}


@app.patch("/api/portfolio/closed-trades/{trade_id}")
async def update_closed_trade(
    trade_id: str,
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """Patch an existing closed trade by id."""
    if not _jwt_or_key(request, api_key):
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")
    try:
        updates = await request.json()
    except Exception as _je:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {_je}")

    from data.closed_trades_store import update_closed_trade as _upd_ct
    result = _upd_ct(trade_id, updates)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Trade {trade_id!r} not found")

    # Invalidate terminal cache
    try:
        from data.caelyn_terminal import CaelynTerminalProvider
        from data.cache import cache as _app_cache
        from data.portfolio_store import canonical_file as _cf
        _app_cache.delete(CaelynTerminalProvider.cache_key_for(_cf()))
    except Exception:
        pass

    return {"success": True, "trade": result}


@app.delete("/api/portfolio/closed-trades/{trade_id}")
async def delete_closed_trade(
    trade_id: str,
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """Remove a closed trade by id."""
    if not _jwt_or_key(request, api_key):
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")

    from data.closed_trades_store import delete_closed_trade as _del_ct
    deleted = _del_ct(trade_id)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Trade {trade_id!r} not found")

    # Invalidate terminal cache
    try:
        from data.caelyn_terminal import CaelynTerminalProvider
        from data.cache import cache as _app_cache
        from data.portfolio_store import canonical_file as _cf
        _app_cache.delete(CaelynTerminalProvider.cache_key_for(_cf()))
    except Exception:
        pass

    return {"success": True, "deleted_id": trade_id}


# ── Patch a single active holding (entry_date, shares, avg_cost) ──────────────

@app.patch("/api/portfolio/holdings/{ticker}")
async def patch_holding(
    ticker: str,
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """Update fields on a single active holding (entry_date, shares, avg_cost, notes).

    Only the supplied fields are changed; all others are preserved.
    Returns 404 if the ticker is not in the active portfolio.
    """
    if not _jwt_or_key(request, api_key):
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")
    try:
        updates = await request.json()
    except Exception as _je:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {_je}")

    ticker = ticker.upper().strip()
    from data.portfolio_store import load_active_holdings as _load, save_active_holdings as _save_h
    holdings = _load()
    idx = next((i for i, h in enumerate(holdings) if h.get("ticker", "").upper() == ticker), None)
    if idx is None:
        raise HTTPException(status_code=404, detail=f"Ticker {ticker!r} not in active portfolio")

    h = dict(holdings[idx])
    allowed = ("entry_date", "date_added", "shares", "avg_cost", "asset_type", "notes")
    for k in allowed:
        if k in updates:
            h[k] = updates[k]

    holdings[idx] = h
    _save_h(holdings)

    # Invalidate terminal cache
    try:
        from data.caelyn_terminal import CaelynTerminalProvider
        from data.cache import cache as _app_cache
        from data.portfolio_store import canonical_file as _cf
        _app_cache.delete(CaelynTerminalProvider.cache_key_for(_cf()))
    except Exception:
        pass

    return {"success": True, "ticker": ticker, "holding": h}


@app.post("/api/portfolio/holdings/{ticker}/buy")
async def add_buy_lot(
    ticker: str,
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """Add a buy lot to an existing (or new) holding.

    Each buy lot records the exact shares, price, and date of one purchase.
    The holding's top-level shares / avg_cost / entry_date are always
    recomputed as the weighted average across all lots.

    Body:
      {
        "shares":  float,         # required — shares purchased, > 0
        "price":   float,         # required — cost per share, > 0
        "date":    "YYYY-MM-DD",  # optional — defaults to today
        "notes":   str            # optional
      }

    Behaviour:
      - Ticker already in active holdings → lot is appended, totals recomputed.
      - Ticker NOT in active holdings     → new holding is created with this
                                            as its first lot.

    Returns:
      {
        "success":  true,
        "ticker":   str,
        "holding":  { ...updated holding with lots array... },
        "lot_added":{ ...the new lot... },
        "active_count": int,
        "holdings_signature": str
      }
    """
    if not _jwt_or_key(request, api_key):
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")
    try:
        body = await request.json()
    except Exception as _je:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {_je}")

    ticker = ticker.upper().strip()

    # ── Validate required fields ──────────────────────────────────────────────
    try:
        new_shares = float(body.get("shares", 0))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="shares must be a number")
    if new_shares <= 0:
        raise HTTPException(status_code=400, detail="shares must be > 0")

    try:
        new_price = float(body.get("price", 0))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="price must be a number")
    if new_price <= 0:
        raise HTTPException(status_code=400, detail="price must be > 0")

    from datetime import date as _date_cls
    raw_date = body.get("date") or body.get("entry_date") or _date_cls.today().isoformat()
    # strip time if full ISO datetime was sent
    buy_date = str(raw_date).split("T")[0]

    new_lot: dict = {"shares": new_shares, "price": new_price, "date": buy_date}
    if body.get("notes"):
        new_lot["notes"] = body["notes"]

    from data.portfolio_store import (
        load_active_holdings as _load,
        save_active_holdings as _save_h,
        get_holdings_signature as _sig,
        compute_lot_totals as _totals,
    )

    holdings = _load()
    idx = next(
        (i for i, h in enumerate(holdings) if h.get("ticker", "").upper() == ticker),
        None,
    )

    if idx is not None:
        h = dict(holdings[idx])
        # Build existing lots — if none recorded yet, synthesise one from the
        # current flat fields so history is preserved.
        existing_lots: list[dict] = list(h.get("lots") or [])
        if not existing_lots:
            synth: dict = {
                "shares": float(h.get("shares", 0)),
                "price":  float(h.get("avg_cost", 0)),
            }
            ed = h.get("entry_date") or h.get("date_added")
            if ed:
                synth["date"] = str(ed).split("T")[0]
            if synth["shares"] > 0:
                existing_lots = [synth]

        existing_lots.append(new_lot)
        totals = _totals(existing_lots)
        h["lots"]       = existing_lots
        h["shares"]     = totals["shares"]
        h["avg_cost"]   = totals["avg_cost"]
        if totals["entry_date"]:
            h["entry_date"] = totals["entry_date"]
        holdings[idx] = h
    else:
        # Brand-new holding — create it with this as the first lot
        h = {
            "ticker":     ticker,
            "shares":     new_shares,
            "avg_cost":   new_price,
            "entry_date": buy_date,
            "asset_type": (body.get("asset_type") or "stock").lower(),
            "lots":       [new_lot],
        }
        if body.get("notes"):
            h["notes"] = body["notes"]
        holdings.append(h)

    _save_h(holdings)
    sig = _sig(holdings)

    # Invalidate terminal cache
    try:
        from data.caelyn_terminal import CaelynTerminalProvider
        from data.cache import cache as _app_cache
        from data.portfolio_store import canonical_file as _cf
        _app_cache.delete(CaelynTerminalProvider.cache_key_for(_cf()))
    except Exception:
        pass

    lot_count = len(h.get("lots", [new_lot]))
    print(
        f"[ADD_BUY_LOT] ticker={ticker}  date={buy_date}  shares={new_shares}"
        f"  price={new_price}  total_lots={lot_count}"
        f"  new_total_shares={h['shares']}  new_avg_cost={h['avg_cost']}"
    )
    return {
        "success":            True,
        "ticker":             ticker,
        "holding":            h,
        "lot_added":          new_lot,
        "active_count":       len(holdings),
        "holdings_signature": sig,
    }


@app.post("/api/portfolio/holdings/{ticker}/close")
async def close_holding(
    ticker: str,
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """Atomically close an active holding.

    Finds the active holding, creates a closed trade record, removes the holding
    from the active portfolio, saves to Neon, and invalidates the Terminal cache —
    all in one call so the frontend does not need to make two separate requests.

    Body (all optional):
      {
        "exit_date":    "YYYY-MM-DD",  # defaults to today
        "exit_price":   float,         # defaults to avg_cost if unavailable
        "close_reason": str            # stored in notes
      }

    Returns:
      {
        "closed_trade":       { ...trade record... },
        "active_count":       int,
        "active_symbols":     [...],
        "holdings_signature": str
      }
    """
    if not _jwt_or_key(request, api_key):
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")
    try:
        body = await request.json()
    except Exception:
        body = {}

    ticker = ticker.upper().strip()

    from data.portfolio_store import (
        load_active_holdings as _load,
        save_active_holdings as _save_h,
        get_holdings_signature as _sig,
    )
    from data.closed_trades_store import save_closed_trade as _save_ct
    from datetime import date as _date_cls

    holdings = _load()
    idx = next((i for i, h in enumerate(holdings) if h.get("ticker", "").upper() == ticker), None)
    if idx is None:
        raise HTTPException(status_code=404, detail=f"Ticker {ticker!r} not in active portfolio")

    h = holdings[idx]

    exit_date  = (body.get("exit_date") or _date_cls.today().isoformat())
    exit_price = float(body.get("exit_price") or h.get("avg_cost") or 0)
    notes      = body.get("close_reason") or body.get("notes") or None

    # Build the closed trade record using the holding's own data
    # Strip time/timezone from entry_date if the frontend sent a full ISO datetime
    _raw_ed = h.get("entry_date") or h.get("date_added") or None
    _clean_ed = str(_raw_ed).split("T")[0] if _raw_ed else None

    # Trade-group ID: reuse from holding (if it was partially sold before) or new
    import uuid as _uuid_mod
    trade_group_id = h.get("trade_group_id") or str(_uuid_mod.uuid4())

    trade_payload = {
        "ticker":        ticker,
        "shares":        float(h.get("shares") or 0),
        "entry_date":    _clean_ed,
        "exit_date":     exit_date,
        "entry_price":   float(h.get("avg_cost") or 0),
        "exit_price":    exit_price,
        "notes":         notes,
        "sell_type":     "full",
        "remaining_shares_after": 0,
        "cost_method":   "average_cost",
        "trade_group_id": trade_group_id,
        "is_full_close": True,
    }
    closed_trade = _save_ct(trade_payload)
    if closed_trade.get("_error"):
        raise HTTPException(status_code=500, detail=f"Failed to save closed trade: {closed_trade['_error']}")

    # Remove from active holdings and persist
    remaining = [hh for i, hh in enumerate(holdings) if i != idx]
    _save_h(remaining)
    sig = _sig(remaining)

    # Invalidate terminal cache
    try:
        from data.caelyn_terminal import CaelynTerminalProvider
        from data.cache import cache as _app_cache
        from data.portfolio_store import canonical_file as _cf
        _app_cache.delete(CaelynTerminalProvider.cache_key_for(_cf()))
    except Exception:
        pass

    active_syms = [hh.get("ticker") for hh in remaining]
    print(
        f"[CLOSE_HOLDING] ticker={ticker}  exit_date={exit_date}  exit_price={exit_price}"
        f"  active_remaining={len(remaining)}  trade_id={closed_trade.get('id')}"
    )
    return {
        "closed_trade":       closed_trade,
        "active_count":       len(remaining),
        "active_symbols":     active_syms,
        "holdings_signature": sig,
    }


@app.post("/api/portfolio/holdings/{ticker}/sell")
async def sell_holding_partial(
    ticker: str,
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """Partial or full sell of an active holding using average-cost accounting.

    sell_type: "shares" | "dollars" | "percent" | "full"
      shares   -> shares_sold = body["shares_sold"]
      dollars  -> shares_sold = dollar_amount / exit_price
      percent  -> shares_sold = current_shares * percent_sold / 100
      full     -> shares_sold = current_shares (same as close endpoint)

    Required: exit_price > 0
    Optional: exit_date (default today), close_reason

    Returns:
      {
        "success", "ticker", "sell_type", "cost_method",
        "shares_before", "shares_sold", "shares_remaining",
        "exit_price", "exit_value", "cost_basis_sold",
        "realized_pnl", "realized_pnl_pct",
        "closed_trade", "active_holding" (null if full close),
        "active_count", "active_symbols", "holdings_signature"
      }
    """
    if not _jwt_or_key(request, api_key):
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")
    try:
        body = await request.json()
    except Exception:
        body = {}

    ticker = ticker.upper().strip()

    from data.portfolio_store import (
        load_active_holdings as _load,
        save_active_holdings as _save_h,
        get_holdings_signature as _sig,
    )
    from data.closed_trades_store import save_closed_trade as _save_ct
    from datetime import date as _date_cls

    # ── Validate sell_type ────────────────────────────────────────────────────
    sell_type = (body.get("sell_type") or "shares").lower().strip()
    if sell_type not in ("shares", "dollars", "percent", "full"):
        raise HTTPException(
            status_code=400,
            detail="sell_type must be one of: shares, dollars, percent, full",
        )

    # ── Validate exit_price ───────────────────────────────────────────────────
    exit_price_raw = body.get("exit_price")
    if exit_price_raw is None:
        raise HTTPException(status_code=400, detail="exit_price is required")
    try:
        exit_price = float(exit_price_raw)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="exit_price must be a number")
    if exit_price <= 0:
        raise HTTPException(status_code=400, detail="exit_price must be greater than 0")

    exit_date   = body.get("exit_date") or _date_cls.today().isoformat()
    close_reason = body.get("close_reason") or body.get("notes") or None

    # ── Load holding ──────────────────────────────────────────────────────────
    holdings = _load()
    idx = next(
        (i for i, h in enumerate(holdings) if h.get("ticker", "").upper() == ticker),
        None,
    )
    if idx is None:
        raise HTTPException(status_code=404, detail=f"Ticker {ticker!r} not in active portfolio")

    h              = holdings[idx]
    current_shares = float(h.get("shares") or 0)
    avg_cost       = float(h.get("avg_cost") or 0)

    if current_shares <= 0:
        raise HTTPException(status_code=400, detail=f"Holding {ticker!r} has 0 shares")

    # ── Resolve shares_sold ───────────────────────────────────────────────────
    _TINY = 0.0001

    if sell_type == "full":
        shares_sold = current_shares
    elif sell_type == "shares":
        raw = body.get("shares_sold")
        if raw is None:
            raise HTTPException(status_code=400, detail="shares_sold is required for sell_type='shares'")
        try:
            shares_sold = float(raw)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="shares_sold must be a number")
        if shares_sold <= 0:
            raise HTTPException(status_code=400, detail="shares_sold must be greater than 0")
    elif sell_type == "dollars":
        raw = body.get("dollar_amount")
        if raw is None:
            raise HTTPException(status_code=400, detail="dollar_amount is required for sell_type='dollars'")
        try:
            dollar_amount = float(raw)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="dollar_amount must be a number")
        if dollar_amount <= 0:
            raise HTTPException(status_code=400, detail="dollar_amount must be greater than 0")
        shares_sold = dollar_amount / exit_price
        # Cap at current_shares for floating-point safety (e.g. exact dollar full sell)
        shares_sold = min(round(shares_sold, 8), current_shares)
    elif sell_type == "percent":
        raw = body.get("percent_sold")
        if raw is None:
            raise HTTPException(status_code=400, detail="percent_sold is required for sell_type='percent'")
        try:
            percent_sold = float(raw)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="percent_sold must be a number")
        if percent_sold <= 0 or percent_sold > 100:
            raise HTTPException(status_code=400, detail="percent_sold must be between 0 and 100")
        shares_sold = current_shares * percent_sold / 100.0
        # Cap at current_shares for floating-point safety (100% may produce marginal overshoot)
        shares_sold = min(round(shares_sold, 8), current_shares)

    # For sell_type="shares": no silent cap — explicitly reject if over-selling
    if shares_sold <= 0:
        raise HTTPException(status_code=400, detail="Resolved shares_sold must be greater than 0")
    if shares_sold > current_shares + _TINY:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot sell {shares_sold:.4f} shares — holding only has {current_shares:.4f}",
        )

    # ── P&L (average-cost accounting) ─────────────────────────────────────────
    cost_basis_sold  = round(shares_sold * avg_cost, 4)
    exit_value       = round(shares_sold * exit_price, 4)
    realized_pnl     = round(exit_value - cost_basis_sold, 4)
    realized_pnl_pct = round(realized_pnl / cost_basis_sold * 100, 4) if cost_basis_sold else None

    entry_date          = h.get("entry_date") or h.get("date_added") or None
    # Strip time/timezone component if the frontend stored a full ISO datetime
    if entry_date:
        entry_date = str(entry_date).split("T")[0]
    holding_period_days = None
    if entry_date:
        try:
            _d1 = _date_cls.fromisoformat(entry_date)
            _d2 = _date_cls.fromisoformat(str(exit_date).split("T")[0])
            holding_period_days = (_d2 - _d1).days
        except Exception:
            pass

    # ── Remaining shares ──────────────────────────────────────────────────────
    remaining_shares = round(current_shares - shares_sold, 8)
    is_full_close    = remaining_shares <= _TINY
    if is_full_close:
        remaining_shares = 0.0

    # ── Trade-group ID: links all partial sells of the same trade lifecycle ─────
    # Reuse the group ID stored on the holding (set on first sell); generate a
    # new one if the holding has never been partially sold before.
    import uuid as _uuid_mod
    trade_group_id = h.get("trade_group_id") or str(_uuid_mod.uuid4())

    # ── Create closed trade for the sold portion ──────────────────────────────
    trade_payload = {
        "ticker":                 ticker,
        "shares":                 round(shares_sold, 8),
        "entry_date":             entry_date,
        "exit_date":              exit_date,
        "entry_price":            avg_cost,
        "exit_price":             exit_price,
        "realized_pnl":           realized_pnl,
        "realized_pnl_pct":       realized_pnl_pct,
        "holding_period_days":    holding_period_days,
        "notes":                  close_reason,
        "sell_type":              sell_type,
        "remaining_shares_after": remaining_shares,
        "cost_method":            "average_cost",
        "trade_group_id":         trade_group_id,
        "is_full_close":          is_full_close,
    }
    closed_trade = _save_ct(trade_payload)
    if closed_trade.get("_error"):
        raise HTTPException(
            status_code=500,
            detail=f"Failed to save closed trade: {closed_trade['_error']}",
        )

    # ── Update active holdings ────────────────────────────────────────────────
    if is_full_close:
        updated_holdings = [hh for i, hh in enumerate(holdings) if i != idx]
        active_holding   = None
    else:
        # Persist the trade_group_id on the holding so subsequent partial sells
        # can reuse it to stay in the same group.
        updated_h        = {**h, "shares": remaining_shares, "trade_group_id": trade_group_id}
        updated_holdings = [updated_h if i == idx else hh for i, hh in enumerate(holdings)]
        # Return the normalised shape that will be persisted
        active_holding = {
            "ticker":         ticker,
            "shares":         remaining_shares,
            "avg_cost":       avg_cost,
            "asset_type":     h.get("asset_type", "stock"),
            "trade_group_id": trade_group_id,
        }
        for _k in ("entry_date", "date_added", "notes", "id"):
            if h.get(_k) is not None:
                active_holding[_k] = h[_k]

    _save_h(updated_holdings)
    sig = _sig(updated_holdings)

    # ── Invalidate terminal cache ─────────────────────────────────────────────
    try:
        from data.caelyn_terminal import CaelynTerminalProvider
        from data.cache import cache as _app_cache
        from data.portfolio_store import canonical_file as _cf
        _app_cache.delete(CaelynTerminalProvider.cache_key_for(_cf()))
    except Exception:
        pass

    active_syms = [hh.get("ticker") for hh in updated_holdings]
    print(
        f"[SELL_HOLDING] ticker={ticker}  sell_type={sell_type}  "
        f"shares_sold={shares_sold:.4f}  remaining={remaining_shares:.4f}  "
        f"exit_price={exit_price}  pnl={realized_pnl}  trade_id={closed_trade.get('id')}"
    )
    return {
        "success":            True,
        "ticker":             ticker,
        "sell_type":          sell_type,
        "cost_method":        "average_cost",
        "shares_before":      current_shares,
        "shares_sold":        round(shares_sold, 8),
        "shares_remaining":   remaining_shares,
        "exit_price":         exit_price,
        "exit_value":         exit_value,
        "cost_basis_sold":    cost_basis_sold,
        "realized_pnl":       realized_pnl,
        "realized_pnl_pct":   realized_pnl_pct,
        "closed_trade":       closed_trade,
        "active_holding":     active_holding,
        "active_count":       len(updated_holdings),
        "active_symbols":     active_syms,
        "holdings_signature": sig,
    }


@app.get("/api/test-altfins")
async def test_altfins(symbol: str = "BTC", api_key: str = Header(None, alias="X-API-Key")):
    await verify_api_key(api_key)
    if not data_service.altfins:
        return {"error": "altFINS API key not configured"}
    try:
        import asyncio
        result = await asyncio.wait_for(
            data_service.altfins.get_coin_analytics(symbol.upper(), "1d"),
            timeout=15.0,
        )
        return {
            "status": "ok",
            "symbol": symbol.upper(),
            "data_keys": list(result.keys()) if isinstance(result, dict) else f"type={type(result).__name__}, len={len(result) if isinstance(result, list) else 'N/A'}",
            "sample": result,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


# ═══════════════════════════════════════════════════════════════════
# OPTIONS DASHBOARD — Live options flow + Claude signal extraction
# ═══════════════════════════════════════════════════════════════════

# ── Options Screener: ticker universe ───────────────────────────────────────
# Core high-volume tickers for the 90-second real-time Public.com precompute loop
# ── Seed tickers per tier ──────────────────────────────────────────────
# Seeds guarantee coverage of known high-flow names. The prefilter also
# pulls in Finviz screen results, so these are NOT the only tickers scanned.

_OPTIONS_ETF_SEEDS = [
    # Highest-volume options ETFs — seeds ensure coverage even if Finviz misses them
    "SPY", "QQQ", "IWM", "DIA",             # Index
    "GLD", "SLV", "TLT", "HYG", "LQD",     # Commodities / fixed income
    "XLF", "XLK", "XLE", "XLV", "XLC",     # Sector
    "SMH", "SOXX",                           # Semis
    "EEM", "EFA", "FXI",                     # International
    "ARKK", "IBIT",                          # Thematic
    "VXX", "UVXY",                           # Volatility
    "TQQQ", "SQQQ", "SOXL",                 # Leveraged
    "KRE", "XBI", "OIH",                    # Industry
]

_OPTIONS_MEGACAP_SEEDS = [
    # $1 T+ mega-caps (stocks only — ETFs are in their own tab)
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA",
]

_OPTIONS_LARGE_CAP_SEEDS = [
    # $100 B – $999 B — blue-chip names with heavy options flow
    "BRK-B", "JPM", "V", "UNH", "MA", "HD", "LLY", "AVGO", "XOM",
    "COST", "WMT", "CRM", "ORCL", "AMD", "NFLX", "ADBE", "PEP",
    "CSCO", "ABT", "TMO", "MRK", "ABBV", "ACN", "QCOM", "TXN",
]

_OPTIONS_SMALL_CAP_SEEDS = [
    # $500 M – $99 B — signal-rich mid/small-cap names
    # Semis / hardware
    "MRVL", "ON", "SMCI", "CRDO", "ALAB", "MCHP", "PSTG", "COHR",
    # Growth software / infra
    "SHOP", "NET", "DDOG", "SNOW", "CRWD", "ZS", "BILL", "CFLT",
    # Energy / clean
    "CEG", "FSLR", "BE", "EQT",
    # Space / defense
    "RKLB", "LUNR", "ASTS", "PL", "AVAV",
    # Crypto-adjacent / digital infra
    "MARA", "CLSK", "HUT", "IONQ",
    # Biotech / health with options activity
    "HIMS", "RXRX", "KRYS", "VERA",
]

_OPTIONS_VALID_TABS = {"megacap", "large_cap", "small_cap", "etf"}

# Backwards-compat alias so old "high_growth" requests still work
_TAB_ALIASES = {"high_growth": "small_cap"}

_OPTIONS_TAB_SEEDS = {
    "etf": _OPTIONS_ETF_SEEDS,
    "megacap": _OPTIONS_MEGACAP_SEEDS,
    "large_cap": _OPTIONS_LARGE_CAP_SEEDS,
    "small_cap": _OPTIONS_SMALL_CAP_SEEDS,
}

# Extended watchlist for Polygon historic data ingestion (runs at 5 calls/min)
# Imported from the ingestion module — used by the background pipeline
from data.options_ingestion import OPTIONS_WATCHLIST as _OPTIONS_FULL_WATCHLIST
from data.tradier_flow_engine import TradierFlowEngine

_OPTIONS_PRECOMPUTE_INTERVAL = 60     # 60s sleep between cycles; ~4 tabs concurrent → ~120s cycle
_OPTIONS_CACHE_TTL = 300              # 5 min hot cache — safely exceeds one full scan cycle
_OPTIONS_PREFILTER_CACHE_TTL = 3600   # 1 hour — stock-side prefilter (Finnhub/FMP called only once/hr)
_OPTIONS_PRECOMPUTE_CACHE_TTL = 300   # 5 min — matches hot cache TTL
_OPTIONS_LKG_CACHE_TTL = 14400        # 4 hours — last-known-good fallback

# Shared Tradier semaphore — all 4 concurrent tab scans share this so total
# Tradier request rate stays within the 120 req/min vendor limit.
# 3 concurrent slots × ~3 calls/ticker × ~5s per ticker ≈ 108 calls/min.
# Created lazily inside _options_precompute_loop (needs running event loop).
_TRADIER_GLOBAL_SEM: asyncio.Semaphore | None = None

# Disk LKG directory — snapshots survive server restarts so users never see blank on revisit.
import pathlib as _pathlib
_LKG_DISK_DIR = _pathlib.Path(__file__).resolve().parent / "data"


def _options_cache_key(tab: str) -> str:
    return f"options_screener_v9:{tab}"


def _options_prefilter_cache_key(tab: str) -> str:
    return f"options_screener_prefilter_v8:{tab}"


def _options_lkg_cache_key(tab: str) -> str:
    """Last-known-good fallback — survives cache expiry, used when live scan is cold."""
    return f"options_screener_lkg_v1:{tab}"


def _lkg_disk_path(tab: str) -> "_pathlib.Path":
    return _LKG_DISK_DIR / f"options_lkg_v1_{tab}.json"


# ── Master screener cache keys (unified architecture) ─────────────────────────
_OPTIONS_MASTER_CACHE_KEY    = "options_master_screener_v1"
_OPTIONS_MASTER_LKG_KEY      = "options_master_lkg_v1"
_OPTIONS_MASTER_PREFILTER_KEY = "options_master_prefilter_v1"

# Tab → market_cap_bucket mapping for dashboard backward-compat filtering
_TAB_TO_BUCKET: dict[str, str] = {
    "megacap":   "megacap",
    "large_cap": "large",
    "small_cap": "small",
    "etf":       "etf",
}
# Reverse: bucket → tab (used when re-grouping for all-tabs response)
_BUCKET_TO_TAB: dict[str, str] = {v: k for k, v in _TAB_TO_BUCKET.items()}


def _master_lkg_disk_path() -> "_pathlib.Path":
    return _LKG_DISK_DIR / "options_master_lkg_v1.json"


def _master_prefilter_disk_path() -> "_pathlib.Path":
    return _LKG_DISK_DIR / "options_master_prefilter_v1.json"


_LKG_DISK_MAX_AGE_S = 86400   # reject snapshots older than 24 h (stale beyond usefulness)
_LKG_DISK_MIN_TICKERS = 1     # reject empty snapshots (scan found true_zero_results)


def _save_lkg_to_disk(tab: str, payload: dict) -> None:
    """
    Atomically persist LKG snapshot to disk.

    Write to a .tmp file first, then rename — guarantees that a crash during
    write never leaves a half-written file that would corrupt the next load.
    Only persists non-empty scans so disk never holds a zero-result snapshot.
    """
    import json as _json
    tickers = payload.get("tickers")
    if not isinstance(tickers, list) or len(tickers) < _LKG_DISK_MIN_TICKERS:
        return   # Don't persist empty scans
    try:
        path = _lkg_disk_path(tab)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        serialized = _json.dumps(payload, default=str)
        tmp.write_text(serialized, encoding="utf-8")
        tmp.replace(path)   # Atomic on POSIX — rename is atomic within same filesystem
        print(f"[OPTIONS_LKG_DISK] [{tab}] Persisted {len(tickers)} tickers to disk ({len(serialized)//1024}KB)")
    except Exception as _e:
        print(f"[OPTIONS_LKG_DISK] [{tab}] Write failed (non-fatal): {_e}")


def _load_lkg_from_disk() -> None:
    """
    Load each tab's last-known-good snapshot from disk into the in-memory
    LKG cache at startup.  Called synchronously before the precompute loop
    starts so users immediately get stale-but-usable data on revisit after
    any server restart / deployment.

    Validation guardrails (guardrail #1):
      - payload must be a dict with a non-empty 'tickers' list
      - snapshot must be newer than _LKG_DISK_MAX_AGE_S (24 h)
      - JSON must parse cleanly; corrupt files are skipped without crashing
      - .tmp partial files are ignored (atomic write guarantee)
    """
    import json as _json
    import time as _t
    from data.cache import cache as _cache
    now = _t.time()
    loaded = 0
    for tab in _OPTIONS_TAB_SEEDS:
        path = _lkg_disk_path(tab)
        if not path.exists() or path.suffix == ".tmp":
            continue
        try:
            raw = path.read_text(encoding="utf-8")
            payload = _json.loads(raw)

            # ── Content validation ───────────────────────────────────────
            if not isinstance(payload, dict):
                print(f"[OPTIONS_LKG_DISK] [{tab}] Skipping: not a dict")
                continue

            tickers = payload.get("tickers")
            if not isinstance(tickers, list) or len(tickers) < _LKG_DISK_MIN_TICKERS:
                print(f"[OPTIONS_LKG_DISK] [{tab}] Skipping: empty tickers list")
                continue

            cached_at = payload.get("cached_at")
            if not isinstance(cached_at, (int, float)) or cached_at <= 0:
                print(f"[OPTIONS_LKG_DISK] [{tab}] Skipping: missing or invalid cached_at")
                continue

            age_s = int(now - cached_at)
            if age_s > _LKG_DISK_MAX_AGE_S:
                print(f"[OPTIONS_LKG_DISK] [{tab}] Skipping: too old ({age_s}s > {_LKG_DISK_MAX_AGE_S}s limit)")
                continue

            # Mark snapshot as disk-loaded so metadata reflects its true origin
            payload = {**payload, "source": "disk_lkg", "disk_loaded": True}
            _cache.set(_options_lkg_cache_key(tab), payload, _OPTIONS_LKG_CACHE_TTL)
            print(f"[OPTIONS_LKG_DISK] [{tab}] Loaded: {len(tickers)} tickers, age={age_s}s")
            loaded += 1

        except _json.JSONDecodeError as _je:
            print(f"[OPTIONS_LKG_DISK] [{tab}] JSON parse error (skipping): {_je}")
        except Exception as _e:
            print(f"[OPTIONS_LKG_DISK] [{tab}] Load failed (non-fatal): {_e}")

    if loaded:
        print(f"[OPTIONS_LKG_DISK] Pre-warmed {loaded}/{len(_OPTIONS_TAB_SEEDS)} tabs from disk — Options Flow page will serve immediately on revisit")
    else:
        print("[OPTIONS_LKG_DISK] No valid disk snapshots found — first scan required before revisit works")


# ── Master screener disk persistence ─────────────────────────────────────────
# Single master LKG file replaces 4 separate tab LKG files in the new arch.

def _save_master_lkg_to_disk(payload: dict) -> None:
    """Atomically persist master screener LKG snapshot to disk."""
    import json as _json
    tickers = payload.get("tickers")
    if not isinstance(tickers, list) or len(tickers) < _LKG_DISK_MIN_TICKERS:
        return
    try:
        path = _master_lkg_disk_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        serialized = _json.dumps(payload, default=str)
        tmp.write_text(serialized, encoding="utf-8")
        tmp.replace(path)
        print(f"[MASTER_LKG_DISK] Persisted {len(tickers)} tickers to disk ({len(serialized)//1024}KB)")
    except Exception as _e:
        print(f"[MASTER_LKG_DISK] Write failed (non-fatal): {_e}")


def _load_master_lkg_from_disk() -> None:
    """Load master screener LKG from disk into the in-memory cache at startup."""
    import json as _json
    import time as _t
    from data.cache import cache as _cache
    now = _t.time()
    path = _master_lkg_disk_path()
    if not path.exists() or path.suffix == ".tmp":
        print("[MASTER_LKG_DISK] No valid disk snapshot found — first scan required")
        return
    try:
        payload = _json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            print("[MASTER_LKG_DISK] Skipping: not a dict")
            return
        tickers = payload.get("tickers")
        if not isinstance(tickers, list) or len(tickers) < _LKG_DISK_MIN_TICKERS:
            print("[MASTER_LKG_DISK] Skipping: empty tickers list")
            return
        cached_at = payload.get("cached_at")
        if not isinstance(cached_at, (int, float)) or cached_at <= 0:
            print("[MASTER_LKG_DISK] Skipping: missing cached_at")
            return
        age_s = int(now - cached_at)
        if age_s > _LKG_DISK_MAX_AGE_S:
            print(f"[MASTER_LKG_DISK] Skipping: too old ({age_s}s > {_LKG_DISK_MAX_AGE_S}s limit)")
            return
        payload = {**payload, "source": "disk_lkg", "disk_loaded": True}
        _cache.set(_OPTIONS_MASTER_LKG_KEY, payload, _OPTIONS_LKG_CACHE_TTL)
        print(f"[MASTER_LKG_DISK] Loaded: {len(tickers)} tickers, age={age_s}s — master screener ready on first request")
    except _json.JSONDecodeError as _je:
        print(f"[MASTER_LKG_DISK] JSON parse error (skipping): {_je}")
    except Exception as _e:
        print(f"[MASTER_LKG_DISK] Load failed (non-fatal): {_e}")


def _save_master_prefilter_to_disk(payload: dict) -> None:
    """Atomically persist master prefilter snapshot to disk."""
    import json as _json
    candidates = payload.get("candidates")
    if not isinstance(candidates, list) or len(candidates) < _PREFILTER_DISK_MIN_CANDIDATES:
        return
    try:
        path = _master_prefilter_disk_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        stamped = {**payload, "disk_saved_at": __import__("time").time()}
        serialized = _json.dumps(stamped, default=str)
        tmp.write_text(serialized, encoding="utf-8")
        tmp.replace(path)
        print(f"[MASTER_PREFILTER_DISK] Persisted {len(candidates)} candidates to disk ({len(serialized)//1024}KB)")
    except Exception as _e:
        print(f"[MASTER_PREFILTER_DISK] Write failed (non-fatal): {_e}")


def _load_master_prefilter_from_disk() -> None:
    """Load master prefilter from disk into the in-memory cache at startup."""
    import json as _json
    import time as _t
    from data.cache import cache as _cache
    now = _t.time()
    path = _master_prefilter_disk_path()
    if not path.exists() or path.suffix == ".tmp":
        print("[MASTER_PREFILTER_DISK] No valid disk snapshot — first scan will build from Finviz/Finnhub")
        return
    try:
        payload = _json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return
        candidates = payload.get("candidates")
        if not isinstance(candidates, list) or len(candidates) < _PREFILTER_DISK_MIN_CANDIDATES:
            return
        saved_at = payload.get("disk_saved_at") or payload.get("cached_at")
        if not isinstance(saved_at, (int, float)) or saved_at <= 0:
            return
        age_s = int(now - saved_at)
        if age_s > _PREFILTER_DISK_MAX_AGE_S:
            print(f"[MASTER_PREFILTER_DISK] Skipping: too old ({age_s}s > {_PREFILTER_DISK_MAX_AGE_S}s)")
            return
        _cache.set(_OPTIONS_MASTER_PREFILTER_KEY, payload, _OPTIONS_PREFILTER_CACHE_TTL)
        print(f"[MASTER_PREFILTER_DISK] Loaded: {len(candidates)} candidates, age={age_s}s — skipping cold prefilter build")
    except _json.JSONDecodeError as _je:
        print(f"[MASTER_PREFILTER_DISK] JSON parse error (skipping): {_je}")
    except Exception as _e:
        print(f"[MASTER_PREFILTER_DISK] Load failed (non-fatal): {_e}")


# ── Prefilter disk persistence ────────────────────────────────────────────────
# Prefilters (Finnhub/FMP-enriched candidate lists) are persisted so restarts
# skip the 48–112s cold-build phase and go straight to Tradier scanning.
# Age limit is 4 h (vs 24 h for LKG) because candidate quality decays faster
# (stock liquidity / market-cap tier membership shifts within the trading day).

_PREFILTER_DISK_MAX_AGE_S = 4 * 3600   # 4 hours
_PREFILTER_DISK_MIN_CANDIDATES = 1


def _prefilter_disk_path(tab: str) -> "_pathlib.Path":
    return _LKG_DISK_DIR / f"options_prefilter_v1_{tab}.json"


def _save_prefilter_to_disk(tab: str, payload: dict) -> None:
    """
    Atomically persist prefilter snapshot to disk.
    Same atomic .tmp→rename pattern as _save_lkg_to_disk.
    Only persists non-empty candidate lists.
    """
    import json as _json
    candidates = payload.get("candidates")
    if not isinstance(candidates, list) or len(candidates) < _PREFILTER_DISK_MIN_CANDIDATES:
        return
    try:
        path = _prefilter_disk_path(tab)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        # Stamp the write time so load side can check age
        stamped = {**payload, "disk_saved_at": __import__("time").time()}
        serialized = _json.dumps(stamped, default=str)
        tmp.write_text(serialized, encoding="utf-8")
        tmp.replace(path)
        print(f"[OPTIONS_PREFILTER_DISK] [{tab}] Persisted {len(candidates)} candidates to disk ({len(serialized)//1024}KB)")
    except Exception as _e:
        print(f"[OPTIONS_PREFILTER_DISK] [{tab}] Write failed (non-fatal): {_e}")


def _load_prefilter_from_disk() -> None:
    """
    Load each tab's prefilter snapshot from disk into the in-memory cache at startup.
    Eliminates the 48–112s cold-build wait on every restart by pre-warming the
    prefilter cache before the first Tradier scan cycle begins.

    Validation guardrails:
      - payload must be a dict with a non-empty 'candidates' list
      - snapshot must be newer than _PREFILTER_DISK_MAX_AGE_S (4 h)
      - JSON must parse cleanly; corrupt files are skipped without crashing
    """
    import json as _json
    import time as _t
    from data.cache import cache as _cache
    now = _t.time()
    loaded = 0
    for tab in _OPTIONS_TAB_SEEDS:
        path = _prefilter_disk_path(tab)
        if not path.exists() or path.suffix == ".tmp":
            continue
        try:
            raw = path.read_text(encoding="utf-8")
            payload = _json.loads(raw)

            if not isinstance(payload, dict):
                print(f"[OPTIONS_PREFILTER_DISK] [{tab}] Skipping: not a dict")
                continue

            candidates = payload.get("candidates")
            if not isinstance(candidates, list) or len(candidates) < _PREFILTER_DISK_MIN_CANDIDATES:
                print(f"[OPTIONS_PREFILTER_DISK] [{tab}] Skipping: empty candidates list")
                continue

            saved_at = payload.get("disk_saved_at") or payload.get("cached_at")
            if not isinstance(saved_at, (int, float)) or saved_at <= 0:
                print(f"[OPTIONS_PREFILTER_DISK] [{tab}] Skipping: missing timestamp")
                continue

            age_s = int(now - saved_at)
            if age_s > _PREFILTER_DISK_MAX_AGE_S:
                print(f"[OPTIONS_PREFILTER_DISK] [{tab}] Skipping: too old ({age_s}s > {_PREFILTER_DISK_MAX_AGE_S}s limit)")
                continue

            _cache.set(_options_prefilter_cache_key(tab), payload, _OPTIONS_PREFILTER_CACHE_TTL)
            print(f"[OPTIONS_PREFILTER_DISK] [{tab}] Loaded: {len(candidates)} candidates, age={age_s}s")
            loaded += 1

        except _json.JSONDecodeError as _je:
            print(f"[OPTIONS_PREFILTER_DISK] [{tab}] JSON parse error (skipping): {_je}")
        except Exception as _e:
            print(f"[OPTIONS_PREFILTER_DISK] [{tab}] Load failed (non-fatal): {_e}")

    if loaded:
        print(f"[OPTIONS_PREFILTER_DISK] Pre-warmed {loaded}/{len(_OPTIONS_TAB_SEEDS)} prefilters from disk — first scan skips cold build")
    else:
        print("[OPTIONS_PREFILTER_DISK] No valid prefilter snapshots on disk — first scan will build from Finviz/Finnhub")


_HOME_OPTIONS_FAST_SEEDS: list[str] = [
    # Highest options volume / most-unusual-flow candidates for the Home panel.
    # Intentionally compact — scan completes in ~15-20s with Semaphore(3).
    "NVDA", "TSLA", "META", "AAPL", "MSFT", "GOOGL", "AMZN",
    "SPY", "QQQ", "IWM", "SMH", "TLT", "IBIT", "UVXY",
    "AMD", "AVGO", "NFLX", "CRM",
    "MRVL", "CRWD", "RKLB", "ASTS", "HIMS", "IONQ",
]
_HOME_OPTIONS_FAST_CACHE_KEY = "home:unusual_options:v1"
_HOME_OPTIONS_FAST_CACHE_TTL = 300      # 5-min soft TTL; loop refreshes every ~90s
_HOME_OPTIONS_FAST_TOP_N = 10           # only scan top movers, not all seeds
_HOME_OPTIONS_FAST_LOOP_INTERVAL = 90   # seconds between scan cycles
_HOME_OPTIONS_FAST_LOCK = asyncio.Lock()  # prevent concurrent home scans


async def _home_options_fast_loop():
    """
    Persistent background loop that keeps the Home 'Unusual Options Flows'
    section always populated with a fresh Tradier snapshot.

    Architecture:
      - Starts immediately on app startup (no initial delay).
      - Batch-quotes all seeds → picks top-N movers by volume/movement.
      - Scans top-N tickers with Semaphore(3) + 1.5s sleep → ~15-20s per cycle.
      - Stores result to home:unusual_options:v1 with metadata:
          updated_at, refresh_in_progress, data_state, stale, result_count
      - Sets refresh_in_progress=True BEFORE scan; clears it after.
      - On error: preserves previous snapshot, clears refresh_in_progress.
      - No Finviz / FMP / Finnhub — Tradier-only minimal prefilter.
      - No AI API calls.
    """
    import time as _t
    import traceback as _tb
    from datetime import datetime as _dtloop, timezone as _tzloop
    from data.tradier_flow_engine import TradierFlowEngine
    from data.cache import cache

    # Wait for data_service to be initialized (mirrors _options_precompute_loop pattern)
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _init_event.wait, 30)

    if data_service is None or not getattr(data_service, "tradier", None):
        print("[HOME_OPTIONS_LOOP] Tradier not available, skipping loop")
        return

    def _to_float(v) -> float | None:
        try:
            return float(v) if v is not None else None
        except Exception:
            return None

    print("[HOME_OPTIONS_LOOP] Starting persistent Home options fast loop")

    while True:
        async with _HOME_OPTIONS_FAST_LOCK:
            try:
                t0 = _t.time()

                # Mark refresh in progress so any concurrent reader knows
                existing = cache.get(_HOME_OPTIONS_FAST_CACHE_KEY) or {}
                cache.set(
                    _HOME_OPTIONS_FAST_CACHE_KEY,
                    {**existing, "refresh_in_progress": True},
                    _HOME_OPTIONS_FAST_CACHE_TTL,
                )

                # Batch-quote all seeds to rank by current activity
                from data.tradier_budget import lane as _hof_lane
                with _hof_lane("saved_options"):
                    raw_quotes = await data_service.tradier.get_quotes(_HOME_OPTIONS_FAST_SEEDS)
                quote_map: dict = {
                    (q.get("symbol") or "").upper(): q
                    for q in (raw_quotes or [])
                }

                def _activity_score(sym: str) -> float:
                    q = quote_map.get(sym, {})
                    chg = abs(q.get("change_percentage") or 0)
                    vol = q.get("volume") or 0
                    avg = q.get("average_volume") or 1
                    return chg * 0.6 + min(vol / avg / 5.0, 1.0) * 0.4

                top_seeds = sorted(
                    _HOME_OPTIONS_FAST_SEEDS, key=_activity_score, reverse=True
                )[:_HOME_OPTIONS_FAST_TOP_N]

                minimal_prefilter = {
                    "candidates": [
                        {
                            "ticker": sym,
                            "price": _to_float((quote_map.get(sym) or {}).get("last")),
                            "source_score": 5.0,
                            "prefilter_score": 5.0,
                        }
                        for sym in top_seeds
                    ],
                    "degraded_sources": [],
                    "macro": {},
                }

                # Semaphore(3) + 1.5s sleep — ~3× faster than the old Sem(1)+3s.
                # tab="small_cap" uses the most relaxed contract filters so diverse
                # seeds (RKLB, ASTS, IONQ, etc.) aren't filtered out by megacap thresholds.
                # The prefilter is bypassed (minimal_prefilter) so the tab only affects
                # _contract_filter and _inspect_one_ticker — the right level.
                engine = TradierFlowEngine(
                    data_service,
                    overrides={
                        "inspect_concurrency": 3,
                        "inspect_inter_ticker_sleep": 1.5,
                    },
                )
                scan = await engine.run_live_scan(
                    seed_tickers=top_seeds,
                    prefilter_snapshot=minimal_prefilter,
                    tab="small_cap",
                )

                tickers = scan.get("tickers") or []
                elapsed = _t.time() - t0

                data_state = "live_ok" if tickers else "true_zero_results"
                now_iso = _dtloop.now(_tzloop.utc).isoformat()

                payload = {
                    "tickers": tickers,
                    "data_source": "tradier",
                    "cached_at": _t.time(),
                    "updated_at": now_iso,
                    "refresh_in_progress": False,
                    "data_state": data_state,
                    "result_count": len(tickers),
                    "stale": False,
                    "scan_scope": f"top_{_HOME_OPTIONS_FAST_TOP_N}_movers",
                    "scan_elapsed_s": round(elapsed, 1),
                }
                cache.set(_HOME_OPTIONS_FAST_CACHE_KEY, payload, _HOME_OPTIONS_FAST_CACHE_TTL)
                print(
                    f"[HOME_OPTIONS_LOOP] Cycle done: {len(tickers)} tickers in {elapsed:.1f}s "
                    f"(top {_HOME_OPTIONS_FAST_TOP_N} of {len(_HOME_OPTIONS_FAST_SEEDS)} seeds). "
                    f"Next in {_HOME_OPTIONS_FAST_LOOP_INTERVAL}s."
                )
                # ── Alert bus hook: home unusual options ──────────────────────
                for _i, _ht in enumerate(tickers):
                    _hsym = (_ht.get("ticker") or "").upper()
                    if not _hsym:
                        continue
                    asyncio.create_task(_alert_bus_fire(
                        "home_unusual_options", "default", _hsym,
                        {
                            "options_score":   _ht.get("composite_score"),
                            "options_rank":    _i + 1,
                            "call_put_bias":   _ht.get("side_bias"),
                            "call_put_ratio":  _ht.get("call_put_premium_ratio"),
                            "price":           _ht.get("underlying_price"),
                            "iv":              _ht.get("implied_volatility"),
                            "call_volume":     _ht.get("call_flow_pct"),
                            "put_volume":      _ht.get("put_flow_pct"),
                            "expected_move":   _ht.get("expected_move_pct"),
                        }
                    ))

            except Exception as exc:
                _tb.print_exc()
                print(f"[HOME_OPTIONS_LOOP] Error (non-fatal): {exc}")
                # Clear refresh_in_progress flag so callers don't wait forever
                existing = cache.get(_HOME_OPTIONS_FAST_CACHE_KEY) or {}
                if existing.get("refresh_in_progress"):
                    cache.set(
                        _HOME_OPTIONS_FAST_CACHE_KEY,
                        {**existing, "refresh_in_progress": False},
                        _HOME_OPTIONS_FAST_CACHE_TTL,
                    )

        await asyncio.sleep(_HOME_OPTIONS_FAST_LOOP_INTERVAL)


async def _dynamic_thematic_universe_loop():
    """
    Background refresh loop for the dynamic thematic universe.

    Builds the universe from ETF holdings + FMP peers + X consensus and caches
    it for 15 minutes.  Runs every 15 minutes so the cache never goes cold during
    normal operation.  The initial build runs after a 30-second startup delay
    (giving sector-rotation and X-consensus loops time to warm first).

    Never raises — all errors are caught and logged.
    """
    await asyncio.sleep(30)   # Let thematic context + X-consensus warm up first
    while True:
        try:
            from services.dynamic_thematic_universe import get_dynamic_thematic_universe as _build_dtu
            result = await _build_dtu(force_refresh=True)
            n = result.get("ticker_count", 0)
            status = result.get("snapshot_status", "?")
            health = result.get("source_health", {})
            print(
                f"[DTU_LOOP] Refresh complete: {n} tickers | status={status} | "
                f"etf={health.get('etf_holdings','?')} "
                f"peers={health.get('fmp_peers','?')} "
                f"xc={health.get('x_consensus','?')}"
            )
        except Exception as _exc:
            print(f"[DTU_LOOP] Refresh error (non-fatal): {_exc}")
        await asyncio.sleep(15 * 60)   # 15 minutes


async def _master_screener_loop():
    """
    Unified master unusual-options screener background loop (Phase 5 + speed).

    Architecture:
      - Single scan covers ALL universes: ETFs, megacap, large_cap, small_cap.
      - UnifiedOptionsEngine merges ALL Finviz screens in one batch and tags
        each candidate with asset_type + market_cap_bucket instead of
        filtering by per-tab market-cap ranges.
      - Stage 1 sweeps top-60 candidates.  Results are cached for 12 minutes
        (expirations change weekly, not per-cycle) → ~0s on hot cycles.
      - Stage 1.5 trims to top-30 by prefilter_score (bounding chain calls).
      - Stage 2 chain-fetches top-30 (~1.6 exp each → ~47 calls) at 115 req/min.
      - Polygon DB enrichment runs OUTSIDE the Stage-2 semaphore: all survivors
        enrich concurrently instead of being serialised 6-at-a-time.
      - Inter-cycle sleep: 5s (down from 60s) — rate limiter controls throughput.

    Expected cycle times:
      Cold (first cycle / cache miss every 12 min):
        Stage 1: ~33s  Stage 2 chains: ~25s  DB: ~4s  misc: ~5s  → ~67s
      Hot (cache warm):
        Stage 1:  ~0s  Stage 2 chains: ~25s  DB: ~4s  misc: ~5s  → ~34s
      Period (hot): 34s cycle + 5s sleep ≈ 39s between starts (vs 149s before)
    """
    global _TRADIER_GLOBAL_SEM

    from data.unified_options_engine import UnifiedOptionsEngine
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _init_event.wait, 30)

    if data_service is None or not data_service.tradier:
        print("[MASTER_SCREENER] Tradier provider not available, skipping loop")
        return

    _TRADIER_GLOBAL_SEM = asyncio.Semaphore(6)

    import time as _time
    import traceback as _tb
    from data.cache import cache

    # Unified seed universe: all tabs merged, deduplicated
    _master_seeds: list[str] = list(dict.fromkeys([
        *_OPTIONS_ETF_SEEDS,
        *_OPTIONS_MEGACAP_SEEDS,
        *_OPTIONS_LARGE_CAP_SEEDS,
        *_OPTIONS_SMALL_CAP_SEEDS,
    ]))

    print(f"[MASTER_SCREENER] Unified seed universe: {len(_master_seeds)} tickers")

    # Stage-1 expiry cache: persists across cycles for the life of this loop.
    # Dict[ticker → (valid_expirations: list[str], stored_at: float)]
    # TTL enforced inside _inspect_shortlist (12 minutes).
    _master_expiry_cache: dict = {}

    _MASTER_CYCLE_SLEEP = 5   # seconds between cycles; rate limiter controls Tradier throughput

    from data.tradier_market_session import get_session as _get_ms_session
    import data.loop_diagnostics as _ld_ms

    _MASTER_OFFHOURS_SLEEP = 1200  # 20 min off-hours/weekends; chains don't change overnight

    while True:
        _ms_session = _get_ms_session()
        _ms_is_active = _ms_session not in ("off_hours", "weekend")

        if not _ms_is_active:
            _ms_next = _time.time() + _MASTER_OFFHOURS_SLEEP
            _ld_ms.update_master_loop("maintenance", next_run_at=_ms_next)
            _ld_ms.increment_suppressed(1)
            print(
                f"[MASTER_SCREENER] Off-hours ({_ms_session}) — skipping scan, "
                f"sleeping {_MASTER_OFFHOURS_SLEEP // 60} min. LKG cache serves stale data."
            )
            await asyncio.sleep(_MASTER_OFFHOURS_SLEEP)
            continue

        t_cycle = _time.time()
        _ld_ms.update_master_loop("active")
        try:
            engine = UnifiedOptionsEngine(data_service)
            engine._shared_sem = _TRADIER_GLOBAL_SEM
            # Share the expiry cache with the engine so Stage 1 can use cached results.
            engine._expiry_cache = _master_expiry_cache

            # Prefilter: load from cache/disk or rebuild
            prefilter_data = cache.get(_OPTIONS_MASTER_PREFILTER_KEY)
            if prefilter_data:
                n_pf = len(prefilter_data.get("candidates", []))
                print(f"[MASTER_SCREENER] Prefilter cache hit ({n_pf} candidates) — running scan...")
            else:
                print("[MASTER_SCREENER] Prefilter cold — building from Finviz/Finnhub/FMP (all universes)...")
                t_pf = _time.time()

                # ── Dynamic thematic seed injection ───────────────────────────
                # First-priority tickers come from the dynamic thematic universe:
                # ETF holdings + FMP peers + X consensus (15-min cached, sync read).
                # Static _master_seeds follow as the liquid options fallback.
                # Deduplication preserves dynamic-first ordering.
                _cycle_seeds = _master_seeds
                try:
                    from services.dynamic_thematic_universe import get_cached_thematic_universe as _get_dtu_c
                    _dtu_snap = _get_dtu_c()
                    _thematic_priority = _dtu_snap.get("tickers", [])[:80]
                    if _thematic_priority:
                        _cycle_seeds = list(dict.fromkeys([*_thematic_priority, *_master_seeds]))
                        print(
                            f"[MASTER_SCREENER] Dynamic thematic seeds: "
                            f"+{len(_thematic_priority)} priority tickers "
                            f"(status={_dtu_snap.get('snapshot_status', '?')}) "
                            f"→ {len(_cycle_seeds)} total seeds"
                        )
                    else:
                        print("[MASTER_SCREENER] Dynamic thematic universe cold — using static seeds only")
                except Exception as _te:
                    print(f"[MASTER_SCREENER] Dynamic thematic injection skipped: {_te}")

                # ── Curated theme universe seed injection ─────────────────
                # Add theme proxy symbols not already in the static seeds.
                # ETF proxies are prioritised (better options liquidity);
                # hard-capped at 60 so the Tradier batch-quote pre-fetch
                # stays fast.  High-activity theme tickers will reach Stage 2
                # naturally; low-activity ones are handled by the supplement loop.
                try:
                    from data.options_theme_supplement import get_theme_proxy_symbols_for_supplement as _get_theme_seeds
                    _theme_extra = _get_theme_seeds(max_symbols=60)
                    if _theme_extra:
                        _prev_len = len(_cycle_seeds)
                        _cycle_seeds = list(dict.fromkeys([*_cycle_seeds, *_theme_extra]))
                        print(
                            f"[MASTER_SCREENER] Curated theme seeds added: "
                            f"+{len(_cycle_seeds) - _prev_len} → {len(_cycle_seeds)} total seeds"
                        )
                except Exception as _thseed_err:
                    print(f"[MASTER_SCREENER] Curated theme seed injection skipped: {_thseed_err}")

                from data.tradier_budget import lane as _ms_lane
                with _ms_lane("options_flow"):
                    prefilter_data = await engine.build_prefilter_snapshot(
                        _cycle_seeds, tab="master",
                    )
                cache.set(_OPTIONS_MASTER_PREFILTER_KEY, prefilter_data, _OPTIONS_PREFILTER_CACHE_TTL)
                _save_master_prefilter_to_disk(prefilter_data)
                n_pf = len(prefilter_data.get("candidates", []))
                print(f"[MASTER_SCREENER] Prefilter built: {n_pf} candidates in {_time.time()-t_pf:.1f}s.")

            # Tradier-only live scan.
            # Pass seed_tickers=None — seeds were already guaranteed by
            # TradierFlowEngine.build_prefilter_snapshot.  Passing them again
            # causes the base engine to re-inflate the inspectable list to
            # all 94 seeds, ballooning Stage-1 calls from ~60 to 147.
            from data.tradier_budget import lane as _ms_lane2
            with _ms_lane2("options_flow"):
                screener_data = await engine.run_live_scan(
                    None, prefilter_snapshot=prefilter_data, tab="master",
                )
            # ── Enrich rows with premium analytics, OTM metrics, heat_score ──
            try:
                from data.options_enricher import enrich_ticker_rows
                enrich_ticker_rows(screener_data.get("tickers", []))
            except Exception as _enrich_exc:
                print(f"[MASTER_SCREENER] Enrichment error (non-fatal): {_enrich_exc}")

            # ── Bake thematic annotation into cached tickers (additive) ──────
            # Annotates tickers with theme_name, theme_state, regime_alignment_score,
            # regime_alignment_label, thematic_badges, dead_zone_warning,
            # base_composite_score, final_composite_score, discovery_sources.
            # Also propagates theme fields to all_contracts by underlying ticker.
            # No LLM calls. Never raises. Cache miss = silent skip.
            try:
                from services.thematic_context_provider import get_shared_thematic_context as _get_stc_ms
                from services.theme_ticker_mapper import get_ticker_theme_alignment as _get_tta_ms
                from services.dynamic_thematic_universe import get_cached_thematic_universe as _get_dtu_ms
                _tc_ms    = _get_stc_ms()
                _dtu_ms   = _get_dtu_ms()
                _active_ms   = _tc_ms.get("active_themes", [])
                _emerging_ms = _tc_ms.get("emerging_themes", [])
                _dead_ms     = _tc_ms.get("dead_zones", [])
                _dtu_src_ms  = _dtu_ms.get("sources_by_ticker", {})
                _theme_by_sym: dict = {}
                for _tk in screener_data.get("tickers", []):
                    _sym_ms = (_tk.get("ticker") or "").upper()
                    _base_ms = float(_tk.get("composite_score") or 0)
                    _align_ms = _get_tta_ms(_sym_ms, _active_ms, _emerging_ms, _dead_ms)
                    _boost_ms = _align_ms["regime_alignment_score"]
                    _tk["theme_name"]             = _align_ms["theme_name"]
                    _tk["theme_state"]            = _align_ms["theme_state"]
                    _tk["regime_alignment_score"] = _boost_ms
                    _tk["regime_alignment_label"] = _align_ms["regime_alignment_label"]
                    _tk["thematic_badges"]        = _align_ms["thematic_badges"]
                    _tk["dead_zone_warning"]      = _align_ms["dead_zone_warning"]
                    _tk["base_composite_score"]   = _base_ms
                    _tk["final_composite_score"]  = round(_base_ms + _boost_ms, 2)
                    _tk["discovery_sources"]      = _dtu_src_ms.get(_sym_ms, [])
                    _theme_by_sym[_sym_ms] = {
                        "theme_name":             _align_ms["theme_name"],
                        "theme_state":            _align_ms["theme_state"],
                        "regime_alignment_score": _boost_ms,
                        "regime_alignment_label": _align_ms["regime_alignment_label"],
                        "thematic_badges":        _align_ms["thematic_badges"],
                    }
                for _ct in screener_data.get("all_contracts", []):
                    _ct_sym = (_ct.get("ticker") or "").upper()
                    if _ct_sym in _theme_by_sym:
                        _ct.update(_theme_by_sym[_ct_sym])
                print(f"[MASTER_SCREENER] Thematic overlay applied to {len(_theme_by_sym)} tickers")
            except Exception as _theme_exc_ms:
                print(f"[MASTER_SCREENER] Thematic overlay error (non-fatal): {_theme_exc_ms}")
            # ── end thematic overlay ──────────────────────────────────────────

            from datetime import datetime as _dt_ms, timezone as _tz_ms
            now_ts = _time.time()
            now_iso = _dt_ms.now(_tz_ms.utc).isoformat()
            full_result = {
                "display_type": "options_screener",
                "scan_type":    "options_flow",
                "tab":          "master",
                "cached_at":    now_ts,
                "updated_at":   now_iso,
                "source":       "tradier",
                "tickers_scanned": _master_seeds,
                **screener_data,
            }
            cache.set(_OPTIONS_MASTER_CACHE_KEY, full_result, _OPTIONS_PRECOMPUTE_CACHE_TTL)
            cache.set(_OPTIONS_MASTER_LKG_KEY,   full_result, _OPTIONS_LKG_CACHE_TTL)
            _save_master_lkg_to_disk(full_result)

            # ── Alert bus hook: master options screener ───────────────────────
            _ms_tickers = screener_data.get("tickers", [])
            for _mi, _mt in enumerate(_ms_tickers):
                _msym = (_mt.get("ticker") or "").upper()
                if not _msym:
                    continue
                asyncio.create_task(_alert_bus_fire(
                    "options_flow", "default", _msym,
                    {
                        "options_score":   _mt.get("composite_score"),
                        "options_rank":    _mi + 1,
                        "call_put_bias":   _mt.get("side_bias"),
                        "call_put_ratio":  _mt.get("call_put_premium_ratio"),
                        "price":           _mt.get("underlying_price"),
                        "iv":              _mt.get("implied_volatility"),
                        "call_volume":     _mt.get("call_flow_pct"),
                        "put_volume":      _mt.get("put_flow_pct"),
                        "expected_move":   _mt.get("expected_move_pct"),
                        "volume":          _mt.get("total_volume"),
                    }
                ))

            n_tickers   = len(screener_data.get("tickers", []))
            n_contracts = len(screener_data.get("all_contracts", []))
            elapsed = _time.time() - t_cycle
            _now_ts = _time.time()
            s1_warm = sum(1 for v in _master_expiry_cache.values() if _now_ts - v[1] < 12 * 60)
            print(
                f"[MASTER_SCREENER] Cycle done: {n_tickers} tickers, "
                f"{n_contracts} contracts in {elapsed:.1f}s "
                f"(expiry-cache: {s1_warm}/{len(_master_expiry_cache)} warm). "
                f"Next in {_MASTER_CYCLE_SLEEP}s."
            )

        except Exception as _exc:
            _tb.print_exc()
            print(f"[MASTER_SCREENER] Cycle error: {_exc}")

        # ── Persist no-options info from Stage-1 expiry cache ──────────────
        # Zero extra Tradier calls — uses data already gathered in Stage 1.
        try:
            from data.options_theme_supplement import update_no_options_from_expiry_cache as _upd_no_opts
            _upd_no_opts(_master_expiry_cache)
        except Exception:
            pass

        # ── Write neutral rows for theme symbols confirmed in expiry cache ──
        # The master screener already fetched expirations for 60+ symbols per
        # cycle.  Symbols that passed Stage-1 (non-empty expirations) but were
        # not selected for unusual flow are optionable but neutral.  Writing
        # them here gives sectors near-instant coverage without extra Tradier
        # calls, replacing the supplement loop's slow 6-symbol-per-batch path.
        try:
            from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE as _ms_theme_univ
            from data.options_theme_supplement import update_supplement_cache as _ms_supp_upd
            _ms_result_syms = {
                (t.get("ticker") or "").upper()
                for t in screener_data.get("tickers", [])
            }
            _ms_all_theme: set = set()
            for _ms_meta in _ms_theme_univ.values():
                for _ms_s in (_ms_meta.get("proxy_symbols") or []):
                    _ms_all_theme.add(_ms_s.upper())
            _ms_now = _time.time()
            # Stage-2 neutral/pending sets from the engine (correctly classified)
            _ms_s2_neutral  = {s.upper() for s in (screener_data.get("stage2_neutral_tickers") or [])}
            _ms_s2_pending  = {s.upper() for s in (screener_data.get("stage2_pending_chain_tickers") or [])}

            # true neutral_no_unusual_flow rows (Stage-2 chain scanned, no unusual flow)
            _ms_neutral: list = [
                {
                    "ticker":        _ms_sym,
                    "_source":       "supplement",
                    "premium":       0.0,
                    "call_flow_pct": 50.0,
                    "put_flow_pct":  50.0,
                    "total_volume":  0,
                    "heat_score":    0.0,
                    "side_bias":     "neutral",
                    "scan_result":   "neutral_no_unusual_flow",
                    "cached_at":     _ms_now,
                    "updated_at":    _ms_now,
                }
                for _ms_sym in _ms_s2_neutral
                if _ms_sym not in _ms_result_syms and _ms_sym in _ms_all_theme
            ]

            # optionable_pending_chain rows: Stage-1.5 trimmed by engine + any
            # Stage-1 confirmed symbols not reached by Stage-2 this master cycle.
            # Do NOT use _master_expiry_cache directly — Stage-1 confirmation
            # alone is insufficient to declare neutral_no_unusual_flow.
            _ms_pending_extra: set[str] = set()
            for _ms_sym, _ms_entry in _master_expiry_cache.items():
                if _ms_sym in _ms_result_syms or _ms_sym not in _ms_all_theme:
                    continue
                if not isinstance(_ms_entry, (list, tuple)) or not _ms_entry[0]:
                    continue
                if _ms_now - _ms_entry[1] > 3600:
                    continue
                if _ms_sym in _ms_s2_neutral:
                    continue  # already classified as neutral_no_unusual_flow above
                _ms_pending_extra.add(_ms_sym)
            _ms_pending_syms = (_ms_s2_pending | _ms_pending_extra) - _ms_result_syms
            _ms_pending: list = [
                {
                    "ticker":        _ms_sym,
                    "_source":       "supplement",
                    "premium":       0.0,
                    "call_flow_pct": 50.0,
                    "put_flow_pct":  50.0,
                    "total_volume":  0,
                    "heat_score":    0.0,
                    "side_bias":     "neutral",
                    "scan_result":   "optionable_pending_chain",
                    "cached_at":     _ms_now,
                    "updated_at":    _ms_now,
                }
                for _ms_sym in _ms_pending_syms
                if _ms_sym not in _ms_s2_neutral  # double-guard
            ]

            _ms_all_coverage = _ms_neutral + _ms_pending
            if _ms_all_coverage:
                _ms_supp_upd(_ms_all_coverage)
                print(
                    f"[MASTER_SCREENER] Coverage: {len(_ms_neutral)} neutral_no_unusual_flow "
                    f"(Stage-2 chain scanned), {len(_ms_pending)} optionable_pending_chain"
                )
        except Exception as _ms_ne:
            print(f"[MASTER_SCREENER] Neutral coverage error (non-fatal): {_ms_ne}")

        _ms_done = _time.time()
        _ld_ms.update_master_loop(
            "active",
            last_run_at=_ms_done,
            next_run_at=_ms_done + _MASTER_CYCLE_SLEEP,
        )
        await asyncio.sleep(_MASTER_CYCLE_SLEEP)




async def _sectors_fast_backfill_loop():
    """
    Dedicated Sectors coverage backfill — uses a direct chain summarizer
    instead of run_live_scan(), so ALL tickers get real call/put premium
    data regardless of unusual flow threshold.

    Two modes:

    Priority (Sectors page visited within 5 min):
      batch=25, sleep=25 s, sectors lane (60 RPM)
      First-pass ETA: ~4 min (expiry cached) / ~8 min (cold start)

    Background (Sectors not actively being viewed):
      batch=8, sleep=60 s, maintenance lane (20 RPM)
      First-pass ETA: ~30 min (same as before)

    After each complete pass, saves the Sectors LKG so restarts begin with
    100% stale_lkg coverage immediately.

    Does NOT change Options Flow Screener behaviour.
    """
    global _TRADIER_GLOBAL_SEM

    import traceback as _tb_sbf
    import time as _ts_sbf

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _init_event.wait, 60)

    if data_service is None or not data_service.tradier:
        print("[SECTORS_BF] Tradier unavailable — Sectors fast backfill disabled")
        return

    # Wait for master screener to initialise the shared semaphore
    _sbf_ws = _ts_sbf.time()
    while _TRADIER_GLOBAL_SEM is None:
        if _ts_sbf.time() - _sbf_ws > 120:
            print("[SECTORS_BF] Semaphore not ready after 120 s — backfill disabled")
            return
        await asyncio.sleep(3)

    from data.options_theme_supplement import (
        get_sectors_pending_symbols          as _sbf_get_pending,
        update_supplement_cache              as _sbf_update_supp,
        update_no_options_from_expiry_cache  as _sbf_upd_no_opts,
        save_sectors_universe_lkg_to_disk    as _sbf_save_lkg,
        update_sectors_backfill_tracking     as _sbf_tracking,
        is_sectors_active                    as _sbf_is_page_active,
        update_sectors_refresh_diag          as _sbf_upd_diag,
    )
    from data.sectors_chain_summarizer import scan_batch_for_sectors as _sbf_scan_batch
    from data.tradier_budget import lane as _sbf_lane, diagnostics as _sbf_budget_diag
    from data.tradier_market_session import get_session as _sbf_get_session

    # Batch / sleep / lane settings
    _SBF_PRIORITY_BATCH   = 25   # tickers per batch when Sectors page is active
    _SBF_PRIORITY_SLEEP   = 25   # seconds between batches (priority mode)
    _SBF_BACKGROUND_BATCH = 8    # tickers per batch when Sectors not active
    _SBF_BACKGROUND_SLEEP = 60   # seconds between batches (background mode)
    _SBF_OFFHOURS_SLEEP   = 600  # 10 min between batches off-hours

    _sbf_cursor            = 0
    _sbf_pass_count        = 0
    _sbf_session_completed = 0
    _sbf_local_expiry: dict = {}  # {sym: ([expirations], checked_at_float)}
    _sbf_sleep_s           = _SBF_BACKGROUND_SLEEP  # safe default for except block

    # Brief startup delay — let master screener warm up first
    await asyncio.sleep(30)
    print("[SECTORS_BF] Sectors fast backfill loop started (chain summarizer mode)")

    while True:
        _sbf_sleep_s = _SBF_BACKGROUND_SLEEP   # reset each cycle
        try:
            _sbf_sess    = _sbf_get_session()
            _sbf_off     = _sbf_sess in ("off_hours", "weekend")

            if _sbf_off:
                _sbf_upd_diag({
                    "sectors_active": _sbf_is_page_active(),
                    "sectors_refresh_queue_depth": 0,
                    "sectors_refresh_eta_seconds": None,
                })
                await asyncio.sleep(_SBF_OFFHOURS_SLEEP)
                continue

            pending = _sbf_get_pending()

            if not pending:
                # ── Full pass complete ─────────────────────────────────────────
                _sbf_pass_count += 1
                _n_saved = _sbf_save_lkg()

                # Assertion: missing_data must be 0 after a full pass.
                # pending=[] means get_sectors_pending_symbols() returned empty,
                # which guarantees no generic_pending (missing_data) or stale_lkg
                # symbols remain.  Run a quick count as a sanity check.
                try:
                    from data.options_theme_supplement import (
                        get_combined_ticker_data as _sbf_gtd,
                        get_no_options_symbols   as _sbf_gno,
                    )
                    from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE as _sbf_tu
                    _sbf_all = {
                        s.upper()
                        for m in _sbf_tu.values()
                        for s in (m.get("proxy_symbols") or [])
                    }
                    _sbf_comb = _sbf_gtd()
                    _sbf_noop = _sbf_gno()
                    _sbf_missing = [
                        s for s in _sbf_all
                        if s not in _sbf_noop and _sbf_comb.get(s) is None
                    ]
                    if _sbf_missing:
                        print(
                            f"[SECTORS_BF] WARNING: Pass {_sbf_pass_count} complete but "
                            f"{len(_sbf_missing)} missing_data tickers still present "
                            f"(budget-deferred?): {_sbf_missing}"
                        )
                    else:
                        print(
                            f"[SECTORS_BF] ASSERTION PASS: missing_data=0 after pass {_sbf_pass_count}"
                        )
                    _sbf_upd_diag({
                        "last_full_pass_missing_count": len(_sbf_missing),
                        "last_full_pass_missing_symbols": _sbf_missing,
                        "last_full_pass_coverage_pct":  round(
                            (len(_sbf_all) - len(_sbf_missing)) / max(len(_sbf_all), 1) * 100, 1
                        ),
                    })
                except Exception as _sbf_ae:
                    print(f"[SECTORS_BF] Full-pass assertion error (non-fatal): {_sbf_ae}")

                _sbf_tracking(
                    pass_count   = _sbf_pass_count,
                    last_pass_at = _ts_sbf.time(),
                    next_at      = _ts_sbf.time() + 300,
                )
                print(
                    f"[SECTORS_BF] Pass {_sbf_pass_count} complete — "
                    f"0 pending, {_n_saved} in Sectors LKG. Re-checking in 5 min."
                )
                await asyncio.sleep(300)
                continue

            # ── Choose batch size / sleep / lane based on page-active status ──
            _priority = _sbf_is_page_active()
            if _priority:
                _batch_size = _SBF_PRIORITY_BATCH
                _sbf_sleep_s = _SBF_PRIORITY_SLEEP
                _lane_name   = "sectors"
            else:
                _batch_size = _SBF_BACKGROUND_BATCH
                _sbf_sleep_s = _SBF_BACKGROUND_SLEEP
                _lane_name   = "maintenance"

            # Advance cursor (wraps on next pass)
            _sbf_cursor = _sbf_cursor % len(pending)
            batch = pending[_sbf_cursor: _sbf_cursor + _batch_size]
            _sbf_cursor += _batch_size

            if not batch:
                await asyncio.sleep(_sbf_sleep_s)
                continue

            # ETA
            remaining   = max(0, len(pending) - _sbf_cursor)
            eta_batches = (remaining + _batch_size - 1) // _batch_size if _batch_size else 0
            eta_s       = eta_batches * _sbf_sleep_s

            _sbf_tracking(
                batch_syms = batch,
                next_at    = _ts_sbf.time() + _sbf_sleep_s,
            )
            _sbf_upd_diag({
                "sectors_active":              _priority,
                "sectors_refresh_queue_depth": len(pending),
                "sectors_refresh_eta_seconds": eta_s,
                "sectors_pending_no_lkg":      len([s for s in pending if s not in _sbf_local_expiry]),
            })

            print(
                f"[SECTORS_BF] {'[PRIORITY] ' if _priority else ''}"
                f"Batch [{_sbf_cursor}/{len(pending)}]: {batch} "
                f"(lane={_lane_name}, sleep={_sbf_sleep_s}s)"
            )

            # ── Run chain summarizer for this batch ────────────────────────────
            _now_sbf = _ts_sbf.time()
            results: list[dict] = []
            try:
                with _sbf_lane(_lane_name):
                    results = await _sbf_scan_batch(
                        batch,
                        data_service.tradier,
                        _sbf_local_expiry,
                        concurrency=6,
                    )
            except Exception as _be:
                print(f"[SECTORS_BF] Batch scan error: {_be}")
                _tb_sbf.print_exc()

            # ── Process results ────────────────────────────────────────────────
            supplement_rows: list[dict] = []
            no_options_expiry: dict = {}

            for r in results:
                if not isinstance(r, dict):
                    continue
                sym = (r.get("ticker") or "").upper()
                if not sym:
                    continue
                scan_result = r.get("scan_result", "")

                if scan_result == "confirmed_no_options":
                    # Populate local expiry so the no-options update picks it up.
                    # The session gate inside update_no_options_from_expiry_cache
                    # prevents false no-options conclusions off-hours.
                    _sbf_local_expiry[sym] = ([], _now_sbf)
                    continue

                if scan_result == "deferred_retry":
                    # Do NOT write a coverage row for budget-deferred tickers.
                    # Writing a supplement row tags them _source="supplement",
                    # which causes get_sectors_pending_symbols() to exclude them
                    # from the next batch's queue — they would NEVER be retried.
                    # Leaving them absent means they stay generic_pending (or
                    # stale_lkg) and are included in the next cycle automatically.
                    continue

                # Real chain data (sectors_chain_summarized or any result
                # with call/put premium fields populated)
                _scan_ts = r.get("updated_at", _now_sbf)
                row = {
                    "ticker":          sym,
                    "_source":         "supplement",
                    "call_premium":    r.get("call_premium", 0.0),
                    "put_premium":     r.get("put_premium",  0.0),
                    "net_premium":     r.get("net_premium",  0.0),
                    "call_volume":     r.get("call_volume",  0),
                    "put_volume":      r.get("put_volume",   0),
                    "total_volume":    r.get("total_volume", 0),
                    "put_call_ratio":  r.get("put_call_ratio"),
                    # premium = total dollar flow (backward-compat field)
                    "premium":         (r.get("call_premium") or 0.0) + (r.get("put_premium") or 0.0),
                    "scan_result":     scan_result,
                    "expiration_used": r.get("expiration_used"),
                    "scanned_at":      _scan_ts,   # explicit audit timestamp
                    "updated_at":      _scan_ts,
                    # ── Interval trade-side classification pass-through ────────
                    # Direct copy from summarize_ticker_chain() — no recomputation.
                    # _build_ticker_node() strips these to None for stale LKG rows
                    # (source in ("supplement_lkg", "watchlist_cache")).
                    "interval_ask_premium":                  r.get("interval_ask_premium"),
                    "interval_bid_premium":                  r.get("interval_bid_premium"),
                    "interval_midpoint_unknown_premium":     r.get("interval_midpoint_unknown_premium"),
                    "interval_total_premium":                r.get("interval_total_premium"),
                    "interval_new_contract_volume":          r.get("interval_new_contract_volume"),
                    "interval_ask_premium_pct":              r.get("interval_ask_premium_pct"),
                    "interval_bid_premium_pct":              r.get("interval_bid_premium_pct"),
                    "interval_midpoint_unknown_premium_pct": r.get("interval_midpoint_unknown_premium_pct"),
                    "interval_classified_trade_side_pct":    r.get("interval_classified_trade_side_pct"),
                    "interval_started_at":                   r.get("interval_started_at"),
                    "interval_ended_at":                     r.get("interval_ended_at"),
                    "interval_seconds":                      r.get("interval_seconds"),
                }
                supplement_rows.append(row)
                _sbf_session_completed += 1

            if supplement_rows:
                _sbf_update_supp(supplement_rows)

            # Update no-options set from expiry cache (gated to regular session)
            _sbf_upd_no_opts(_sbf_local_expiry)

            # ── Instrument-type reconciliation for this batch ──────────────────
            # Re-run the DB warm-up for batch symbols that are still "unknown".
            # This catches tickers newly added to themes whose screener profile
            # may now be in screener_fundamentals_cache but weren't present at
            # startup.  DB-only — never a Tradier or FMP call in this loop.
            try:
                from data.options_instrument_type_service import (
                    get_unresolved_symbols as _sbf_unresolv,
                    warm_up_from_db        as _sbf_warm_itype,
                )
                _sbf_unknown_in_batch = _sbf_unresolv(batch)
                if _sbf_unknown_in_batch:
                    _sbf_warm_itype(_sbf_unknown_in_batch)
            except Exception:
                pass
            # ─────────────────────────────────────────────────────────────────

            # Update diagnostics
            _rows_with_prem = sum(
                1 for r in supplement_rows
                if (r.get("call_premium") or 0) + (r.get("put_premium") or 0) > 0
            )
            try:
                _bdiag = _sbf_budget_diag()
                _sbf_upd_diag({
                    "sectors_refresh_completed_this_session": _sbf_session_completed,
                    "sectors_rows_with_premium":             _rows_with_prem,
                    "sectors_refresh_calls_last_60s": (
                        _bdiag.get("calls_last_60s_by_lane", {}).get(_lane_name, 0)
                    ),
                    "sectors_refresh_deferred_count": (
                        _bdiag.get("deferred_by_lane", {}).get(_lane_name, 0)
                    ),
                })
            except Exception:
                pass

            _no_opts_n = len([s for s in batch if _sbf_local_expiry.get(s, (["x"],))[0] == []])
            print(
                f"[SECTORS_BF] Done: {len(supplement_rows)} rows updated "
                f"({_rows_with_prem} with premium, {_no_opts_n} no-options) | "
                f"session_total={_sbf_session_completed}, cursor={_sbf_cursor}/{len(pending)}"
            )

        except Exception as _sbf_exc:
            print(f"[SECTORS_BF] Cycle error: {_sbf_exc}")
            _tb_sbf.print_exc()

        await asyncio.sleep(_sbf_sleep_s)


async def _theme_options_supplement_loop():
    """
    Slow supplemental options scan for curated theme proxy symbols that are
    not already covered by the master screener results.

    Cadence  : up to 6 symbols every 10 minutes.
    Rate use : ~6 × 4 Tradier calls / 10 min ≈ 2.4 calls/min  (<2.2% budget).

    Uses the same _TRADIER_GLOBAL_SEM and data_service.tradier as the master
    screener — no second Tradier client.  Results go to options_theme_supplement_v1
    (separate from the master cache).  The sectors endpoint merges both caches.
    """
    global _TRADIER_GLOBAL_SEM

    import traceback as _tb_s
    import time as _ts

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _init_event.wait, 60)

    if data_service is None or not data_service.tradier:
        print("[THEME_SUPP] Tradier unavailable — supplement scan disabled")
        return

    # Wait for master screener to initialise the shared semaphore
    _ws = _ts.time()
    while _TRADIER_GLOBAL_SEM is None:
        if _ts.time() - _ws > 120:
            print("[THEME_SUPP] Semaphore not ready after 120 s — supplement scan disabled")
            return
        await asyncio.sleep(3)

    from data.options_theme_supplement import (
        get_theme_only_symbols_for_supplement  as _get_pending,
        update_supplement_cache                as _update_supp,
        update_no_options_from_expiry_cache    as _upd_no_opts,
    )
    from data.options_enricher import enrich_ticker_rows
    from data.tradier_flow_engine import TradierFlowEngine
    from data.options_flow_engine import ETF_SET as _ETF_SET

    _BATCH_SIZE          = 20
    _ACTIVE_CYCLE_SLEEP  = 120    # 2 min during pre/regular/post-market (backup to Sectors backfill)
    _OFFHOURS_SUPP_SLEEP = 2400   # 40 min during off-hours/weekends
    _scan_cursor = 0
    _local_expiry: dict = {}   # separate from master screener expiry cache

    from data.tradier_market_session import get_session as _get_supp_session
    import data.loop_diagnostics as _ld_supp

    def _sf(v):
        try: return float(v) if v is not None else None
        except: return None
    def _si(v):
        try: return int(v) if v is not None else 0
        except: return 0

    # Initial delay: give master screener time to warm up first
    await asyncio.sleep(60)
    print("[THEME_SUPP] Theme options supplement loop started")

    while True:
        _supp_sess = _get_supp_session()
        _supp_is_active = _supp_sess not in ("off_hours", "weekend")

        if not _supp_is_active:
            _supp_next = _ts.time() + _OFFHOURS_SUPP_SLEEP
            _ld_supp.update_supplement_loop("maintenance", next_run_at=_supp_next)
            _ld_supp.increment_suppressed(1)
            print(
                f"[THEME_SUPP] Off-hours ({_supp_sess}) — skipping scan, "
                f"sleeping {_OFFHOURS_SUPP_SLEEP // 60} min."
            )
            await asyncio.sleep(_OFFHOURS_SUPP_SLEEP)
            continue

        _ld_supp.update_supplement_loop("active")
        try:
            pending = _get_pending()
            if not pending:
                await asyncio.sleep(_ACTIVE_CYCLE_SLEEP)
                continue

            _scan_cursor = _scan_cursor % len(pending)
            batch = pending[_scan_cursor: _scan_cursor + _BATCH_SIZE]
            _scan_cursor += _BATCH_SIZE

            if not batch:
                await asyncio.sleep(_ACTIVE_CYCLE_SLEEP)
                continue

            # ── Global in-flight guard ────────────────────────────────────────
            # Skip symbols already being scanned by Watchlist / Portfolio so no
            # duplicate Tradier calls fire for the same symbol concurrently.
            # Safe defaults — ensure finally can always reference these names.
            _batch_claimed: list[str] = []
            _release_opts = None
            try:
                from services.options_inflight import (
                    claim_many   as _claim_opts,
                    release_many as _release_opts,
                )
                _batch_claimed, _batch_blocked = _claim_opts(batch, "supplement")
                if _batch_blocked:
                    print(
                        f"[THEME_SUPP] {len(_batch_blocked)} symbols already in-flight "
                        f"(skipped): {_batch_blocked}"
                    )
            except Exception:
                _batch_claimed = list(batch)
                _release_opts  = None

            batch = _batch_claimed
            if not batch:
                await asyncio.sleep(_ACTIVE_CYCLE_SLEEP)
                continue
            # ─────────────────────────────────────────────────────────────────

            # ── Cache-first filter for watchlist-overlap symbols ──────────────
            # Policy: supplement SHOULD NOT live-scan overlap symbols that the
            # Watchlist scanner already refreshed (fresh portfolio_opts:{sym}
            # cache entry with data_available=True).  It MAY gap-fill them when
            # the Watchlist scanner hasn't gotten there yet.
            #
            # This prevents sequential duplicate Tradier calls (the inflight
            # guard only blocks concurrent ones).
            _wl_syms: set[str] = set()
            try:
                from data.options_flow_sectors import _load_all_watchlist_symbols as _load_wl
                _wl_syms = _load_wl()
            except Exception:
                pass

            _overlap_in_batch    = [s for s in batch if s.upper() in _wl_syms]
            _theme_only_in_batch = [s for s in batch if s.upper() not in _wl_syms]

            _overlap_cache_hits: list[str] = []
            _overlap_needs_scan: list[str] = []

            if _overlap_in_batch:
                try:
                    from data.portfolio_options_service import (
                        _per_ticker_cache_key as _ptck,
                        _load_portfolio_lkg as _load_wl_lkg,
                    )
                    from data.cache import cache as _opts_cache
                    _wl_disk_lkg = _load_wl_lkg()
                    for _s in _overlap_in_batch:
                        _su = _s.upper()
                        _row = _opts_cache.get(_ptck(_su))
                        if _row and isinstance(_row, dict) and _row.get("data_available"):
                            # Watchlist memory cache has good data — skip live scan
                            _overlap_cache_hits.append(_s)
                        elif _wl_disk_lkg.get(_su, {}).get("data_available"):
                            # Watchlist disk LKG has prior good data — skip live scan.
                            # The supplement loop may only gap-fill when NO last-good
                            # watchlist-owned row exists AND no refresh is in-flight.
                            _overlap_cache_hits.append(_s)
                        else:
                            _overlap_needs_scan.append(_s)
                except Exception:
                    _overlap_needs_scan.extend(_overlap_in_batch)
            # no overlap: nothing to filter
            _supp_diag_payload = {
                "supplement_symbols_total":              len(pending),
                "supplement_watchlist_overlap_symbols":  sorted(s.upper() for s in _overlap_in_batch),
                "supplement_only_symbols":               sorted(s.upper() for s in _theme_only_in_batch),
                "supplement_overlap_cache_hits":         len(_overlap_cache_hits),
                "supplement_overlap_live_scans":         len(_overlap_needs_scan),
                "supplement_overlap_live_scans_blocked": len(
                    [s for s in (_batch_blocked if isinstance(_batch_blocked, list) else [])
                     if s.upper() in _wl_syms]
                ),
                "supplement_only_live_scans":            len(_theme_only_in_batch),
                "supplement_duplicate_scans_blocked":    len(
                    _batch_blocked if isinstance(_batch_blocked, list) else []
                ),
            }
            try:
                from data.options_theme_supplement import update_supplement_diag as _upd_supp_diag
                _upd_supp_diag(_supp_diag_payload)
            except Exception:
                pass

            if _overlap_cache_hits:
                print(
                    f"[THEME_SUPP] {len(_overlap_cache_hits)} overlap syms skipped "
                    f"(fresh watchlist cache): {_overlap_cache_hits}"
                )

            # Rebuild batch: theme-only + overlap symbols that need gap-fill
            batch = _theme_only_in_batch + _overlap_needs_scan
            if not batch:
                print("[THEME_SUPP] All batch symbols already covered by watchlist cache — skipping scan")
                await asyncio.sleep(_ACTIVE_CYCLE_SLEEP)
                continue
            # ─────────────────────────────────────────────────────────────────

            print(f"[THEME_SUPP] Scanning batch [{_scan_cursor}/{len(pending)}]: {batch}")

            # Pre-fetch Tradier quotes — Stage 1 needs a price to proceed
            candidates = []
            from data.tradier_budget import lane as _supp_lane
            try:
                with _supp_lane("maintenance"):
                    raw_q = await data_service.tradier.get_quotes(batch)
                qmap = {(q.get("symbol") or "").upper(): q for q in (raw_q or []) if isinstance(q, dict)}
                for sym in batch:
                    sym_u = sym.upper()
                    q = qmap.get(sym_u) or {}
                    price = _sf(q.get("last"))
                    if price:
                        candidates.append({
                            "ticker":               sym_u,
                            "price":                price,
                            "change_pct":           _sf(q.get("change_percentage")),
                            "volume":               _si(q.get("volume")),
                            "category":             "etf" if sym_u in _ETF_SET else "stock",
                            "source_score":         5.0,
                            "source_hits":          ["theme_supplement"],
                            "reasons":              ["theme_basket_inclusion"],
                            "prefilter_score":      5.0,
                            "profile":              {},
                            "technicals":           {},
                            "stock_relative_volume": None,
                            "liquidity_dollars":    None,
                            "liquidity_supported":  False,
                        })
            except Exception as _qe:
                print(f"[THEME_SUPP] Quote prefetch error: {_qe}")

            if not candidates:
                await asyncio.sleep(_ACTIVE_CYCLE_SLEEP)
                continue

            # TradierFlowEngine: master_stage2_limit defaults to 0
            # (set to 30 only by UnifiedOptionsEngine), so all Stage-1
            # survivors are chain-fetched here.
            engine = TradierFlowEngine(data_service)
            engine._shared_sem  = _TRADIER_GLOBAL_SEM
            engine._expiry_cache = _local_expiry

            from data.tradier_budget import lane as _supp_lane2
            with _supp_lane2("maintenance"):   # supplement background scan
                screener_data = await engine.run_live_scan(
                    None,
                    prefilter_snapshot={
                        "candidates":       candidates,
                        "macro":            {},
                        "degraded_sources": [],
                    },
                    tab="master",
                )

            results = screener_data.get("tickers", [])
            if results:
                enrich_ticker_rows(results)
                _update_supp(results)
                print(f"[THEME_SUPP] Batch done: {len(results)}/{len(batch)} tickers with unusual flow")
            else:
                print(f"[THEME_SUPP] Batch done: 0/{len(batch)} unusual flow (checking for neutral coverage)")

            # Write coverage rows with the correct 9-state model:
            #   neutral_no_unusual_flow  — Stage-2 chain scanned AND no unusual flow
            #                             (total_normalized > 0, no contracts passing filter)
            #   optionable_pending_chain — Stage-1 confirmed (expirations exist) but Stage-2
            #                             was budget-deferred, trimmed by Stage-1.5, or the
            #                             chain returned 0 normalised contracts (ambiguous).
            # Do NOT write neutral_no_unusual_flow for Stage-1-only confirmation.
            try:
                _now_s2     = _ts.time()
                _result_syms = {(r.get("ticker") or "").upper() for r in results}
                _s2_neutral  = {s.upper() for s in (screener_data.get("stage2_neutral_tickers") or [])}
                _s2_pending  = {s.upper() for s in (screener_data.get("stage2_pending_chain_tickers") or [])}

                # True neutral rows (Stage-2 chain scanned, had contracts, none passed filter)
                _neutral_rows = [
                    {
                        "ticker":        _sym,
                        "_source":       "supplement",
                        "premium":       0.0,
                        "call_flow_pct": 50.0,
                        "put_flow_pct":  50.0,
                        "total_volume":  0,
                        "heat_score":    0.0,
                        "side_bias":     "neutral",
                        "scan_result":   "neutral_no_unusual_flow",
                        "cached_at":     _now_s2,
                        "updated_at":    _now_s2,
                    }
                    for _sym in _s2_neutral if _sym and _sym not in _result_syms
                ]

                # Optionable-pending rows: Stage-1.5 trimmed by engine + Stage-1 alive
                # that reached Stage-2 but chain was deferred/empty (total_normalized == 0).
                # Also capture any Stage-1 alive symbols not accounted for by either set
                # (defensive fallback — should be empty for normal 6-symbol supplement batches).
                _s1_alive: set[str] = set()
                for _c in candidates:
                    _csym = (_c.get("ticker") or "").upper()
                    if not _csym:
                        continue
                    _expiry_entry = _local_expiry.get(_csym)
                    if _expiry_entry and isinstance(_expiry_entry, (list, tuple)) and _expiry_entry[0]:
                        _s1_alive.add(_csym)
                # pending = engine Stage-1.5 trimmed + Stage-1 alive not accounted for
                _pending_syms = (_s2_pending | (_s1_alive - _s2_neutral - _result_syms)) - _result_syms
                _pending_rows = [
                    {
                        "ticker":        _sym,
                        "_source":       "supplement",
                        "premium":       0.0,
                        "call_flow_pct": 50.0,
                        "put_flow_pct":  50.0,
                        "total_volume":  0,
                        "heat_score":    0.0,
                        "side_bias":     "neutral",
                        "scan_result":   "optionable_pending_chain",
                        "cached_at":     _now_s2,
                        "updated_at":    _now_s2,
                    }
                    for _sym in _pending_syms if _sym
                ]

                _all_coverage = _neutral_rows + _pending_rows
                if _all_coverage:
                    _update_supp(_all_coverage)
                    print(
                        f"[THEME_SUPP] Coverage: {len(_neutral_rows)} neutral_no_unusual_flow "
                        f"(Stage-2 chain scanned), {len(_pending_rows)} optionable_pending_chain"
                    )
            except Exception as _ne:
                print(f"[THEME_SUPP] Coverage row error (non-fatal): {_ne}")

            # Persist no-options info discovered in this batch's Stage-1 sweep
            _upd_no_opts(_local_expiry)

            # Update tracking state for debug endpoint
            try:
                from data.options_theme_supplement import update_scan_tracking as _upd_tracking
                _upd_tracking(batch, _ts.time() + _ACTIVE_CYCLE_SLEEP)
            except Exception:
                pass

        except Exception as _exc:
            print(f"[THEME_SUPP] Cycle error: {_exc}")
            _tb_s.print_exc()
        finally:
            # Always release global in-flight claims for this batch, even on error.
            try:
                if _batch_claimed and _release_opts is not None:
                    _release_opts(_batch_claimed, "supplement")
            except Exception:
                pass

        _supp_done = _ts.time()
        _ld_supp.update_supplement_loop(
            "active",
            last_run_at=_supp_done,
            next_run_at=_supp_done + _ACTIVE_CYCLE_SLEEP,
        )
        await asyncio.sleep(_ACTIVE_CYCLE_SLEEP)


_OPTIONS_DEFAULT_TICKERS = _OPTIONS_MEGACAP_SEEDS

# In-memory user overrides for scan defaults (reset on restart).
# Market-cap ranges are NOT editable — only options-level params.
_SCAN_USER_OVERRIDES: dict[str, dict] = {}   # keyed by tab name

# Keys the user is allowed to override (options-level screening only).
# Market-cap ranges are hardcoded in TIER_MCAP_RANGES and NOT here.
_EDITABLE_SCAN_KEYS = {
    "prefilter_target", "options_inspection_limit", "min_stock_price",
    "min_stock_liquidity", "relative_volume_threshold",
    "min_dte", "max_dte", "max_expirations_per_ticker",
}


# ── Master screener helpers ───────────────────────────────────────────────────

def _filter_master_snapshot(snapshot: dict, tab: str | None) -> dict:
    """
    Given the master screener snapshot, return a view filtered to a specific
    tab (by market_cap_bucket).  If tab is None or 'master', return all rows.

    Used by /api/options/dashboard for backward-compat tab-based views and
    by /api/options/all-tabs to build the per-tab breakdown.
    """
    if not tab or tab == "master":
        return snapshot
    bucket = _TAB_TO_BUCKET.get(tab)
    if not bucket:
        return snapshot  # unknown tab → return everything
    all_tickers = snapshot.get("tickers", [])
    all_contracts = snapshot.get("all_contracts", [])
    filtered_symbols: set[str] = set()
    filtered_tickers = [
        t for t in all_tickers
        if t.get("market_cap_bucket") == bucket or
           (bucket == "etf" and t.get("asset_type") == "etf")
    ]
    filtered_symbols = {t.get("ticker") for t in filtered_tickers if t.get("ticker")}
    filtered_contracts = [
        c for c in all_contracts
        if c.get("ticker") in filtered_symbols
    ]
    return {
        **snapshot,
        "tab":          tab,
        "tickers":      filtered_tickers,
        "all_contracts": filtered_contracts,
    }


# ── /api/thematic-context — shared thematic/regime snapshot endpoints ─────────

@app.get("/api/thematic-context/snapshot")
@limiter.limit("60/minute")
async def thematic_context_snapshot(request: Request):
    """
    Return the normalized thematic/regime/sector snapshot.

    Sources reused (no LLM calls during this endpoint):
      regime:current_v1, sr:dashboard:v1, sr:theme_data:v2,
      x_consensus_weekly.json, sector_rotation_analysis.json

    Schema:
      macro_regime, active_themes, emerging_themes, dead_zones,
      sector_leaders, sector_laggards, risk_notes, source_health
    """
    from services.thematic_context_provider import get_shared_thematic_context
    snap = get_shared_thematic_context(force_refresh=False)
    return {"ok": True, "snapshot": snap}


@app.post("/api/thematic-context/refresh")
@limiter.limit("10/minute")
async def thematic_context_refresh(request: Request):
    """
    Force-rebuild the thematic snapshot from existing caches.
    Does NOT trigger Claude/Gemini/Grok or any multi-API scan.
    Safe to call manually to flush the 10-minute cache.
    """
    from services.thematic_context_provider import get_shared_thematic_context
    snap = get_shared_thematic_context(force_refresh=True)
    return {"ok": True, "refreshed": True, "snapshot": snap}


# ── /api/options/screener — master unified leaderboard endpoint ──────────────

@app.get("/api/options/screener")
@limiter.limit("60/minute")
async def options_screener(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
    _sub: None = Depends(require_subscription),
    asset_type: str | None = None,
    market_cap_bucket: str | None = None,
    limit: int = 50,
):
    """
    Unified master unusual-options screener — single globally ranked leaderboard
    covering ETFs, megacap, large-cap, and small/mid-cap stocks together.

    Optional filters:
      ?asset_type=etf|stock
      ?market_cap_bucket=megacap|large|small|etf|unknown
      ?limit=N  (default 50, max 200)

    Every result row carries:
      asset_type        : "etf" | "stock"
      market_cap_bucket : "megacap" | "large" | "small" | "etf" | "unknown"
      composite_score   : global rank key (higher = more unusual flow)
    """
    await _wait_for_init()

    if not data_service or not data_service.tradier:
        return JSONResponse(
            status_code=503,
            content={"error": "Tradier options provider not configured."},
        )

    import time as _time
    from data.cache import cache
    from datetime import datetime as _dt_s, timezone as _tz_s

    limit = max(1, min(limit, 200))

    snap = cache.get(_OPTIONS_MASTER_CACHE_KEY) or cache.get(_OPTIONS_MASTER_LKG_KEY)

    def _snap_meta_master(snap: dict, stale: bool) -> dict:
        n = len(snap.get("tickers", []))
        age = int(_time.time() - snap.get("cached_at", _time.time()))
        updated = snap.get("updated_at") or snap.get("cached_at")
        if isinstance(updated, (int, float)):
            updated = _dt_s.fromtimestamp(updated, tz=_tz_s.utc).isoformat()
        if stale and n:
            ds = "stale_but_available"
        elif n:
            ds = "live_ok"
        else:
            ds = "true_zero_results" if not stale else "refresh_in_progress"
        return {
            "data_state":          ds,
            "result_count":        n,
            "stale":               stale,
            "cache_age_seconds":   age,
            "refresh_in_progress": stale,
            "updated_at":          updated,
            "source":              snap.get("source", "tradier"),
        }

    if not snap:
        return {
            "response": {
                "display_type":  "options_screener",
                "scan_type":     "options_flow",
                "tab":           "master",
                "tickers":       [],
                "all_contracts": [],
            },
            "structured":     True,
            "preset":         "options_screener",
            "tab":            "master",
            "from_cache":     False,
            "stale":          False,
            "data_state":     "no_data_yet",
            "result_count":   0,
            "refresh_in_progress": True,
            "updated_at":     None,
            "source":         "none",
        }

    stale = cache.get(_OPTIONS_MASTER_CACHE_KEY) is None
    tickers = snap.get("tickers", [])

    # Apply optional filters
    if asset_type:
        tickers = [t for t in tickers if t.get("asset_type") == asset_type]
    if market_cap_bucket:
        tickers = [t for t in tickers if t.get("market_cap_bucket") == market_cap_bucket]

    # Global rank: score DESC → heat_score DESC → premium DESC → volume DESC
    tickers = sorted(
        tickers,
        key=lambda t: (
            -(t.get("composite_score") or 0),
            -(t.get("heat_score") or 0),
            -(t.get("premium") or 0),
            -(t.get("total_volume") or 0),
        ),
    )
    tickers = tickers[:limit]
    filtered_syms = {t.get("ticker") for t in tickers if t.get("ticker")}
    all_contracts = [
        c for c in snap.get("all_contracts", [])
        if c.get("ticker") in filtered_syms
    ]

    # ── Additive thematic overlay (no LLM calls, no score changes) ───────────
    # Reads pre-populated caches only: regime:current_v1, sr:dashboard:v1,
    # sr:theme_data:v2, x_consensus_weekly.json.  Never raises, never 500.
    try:
        from services.thematic_context_provider import get_shared_thematic_context
        from services.theme_ticker_mapper import get_ticker_theme_alignment
        _tc = get_shared_thematic_context()
        _active    = _tc.get("active_themes", [])
        _emerging  = _tc.get("emerging_themes", [])
        _dead      = _tc.get("dead_zones", [])
        _macro_reg = _tc.get("macro_regime")
        _sec_lead  = {e["ticker"] for e in _tc.get("sector_leaders", []) if e.get("ticker")}

        for row in tickers:
            sym  = (row.get("ticker") or "").upper()
            base = float(row.get("composite_score") or 0)
            align = get_ticker_theme_alignment(sym, _active, _emerging, _dead)
            boost = align["regime_alignment_score"]
            final = round(base + boost, 2)
            row["theme_name"]             = align["theme_name"]
            row["theme_state"]            = align["theme_state"]
            row["sector_alignment"]       = sym in _sec_lead
            row["macro_fit"]              = _macro_reg
            row["regime_alignment_score"] = boost
            row["thematic_badges"]        = align["thematic_badges"]
            row["dead_zone_warning"]      = align["dead_zone_warning"]
            row["base_composite_score"]   = base
            row["final_composite_score"]  = final
    except Exception as _tc_err:
        print(f"[OPTIONS_SCREENER] thematic overlay error: {_tc_err}")
    # ── end thematic overlay ─────────────────────────────────────────────────

    meta = _snap_meta_master(snap, stale)
    return {
        "response": {
            **snap,
            "tickers":       tickers,
            "all_contracts": all_contracts,
        },
        "structured":          True,
        "preset":              "options_screener",
        "tab":                 "master",
        "from_cache":          True,
        "stale":               stale,
        "cache_age_seconds":   meta["cache_age_seconds"],
        "next_refresh_in_seconds": max(0, _OPTIONS_CACHE_TTL - meta["cache_age_seconds"]),
        "data_state":          meta["data_state"],
        "result_count":        len(tickers),
        "refresh_in_progress": stale,
        "updated_at":          meta["updated_at"],
        "source":              meta["source"],
        "available_tabs":      sorted(_OPTIONS_VALID_TABS),
    }


@app.get("/api/options-flow/master/latest")
@limiter.limit("60/minute")
async def options_flow_master_latest(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
    _sub: None = Depends(require_subscription),
    limit: int = 10,
):
    """
    Verification / debug endpoint — returns the top N enriched rows from the
    master screener, confirming that all enriched fields (premium, heat_score,
    oi_change_pct, etc.) are present.

    Example:
      curl /api/options-flow/master/latest?limit=5
    """
    from data.cache import cache
    limit = max(1, min(limit, 50))
    snap = cache.get(_OPTIONS_MASTER_CACHE_KEY) or cache.get(_OPTIONS_MASTER_LKG_KEY)
    if not snap:
        return {"error": "no_data_yet", "tickers": []}

    rows = snap.get("tickers", [])
    rows = sorted(
        rows,
        key=lambda t: (
            -(t.get("composite_score") or 0),
            -(t.get("heat_score") or 0),
            -(t.get("premium") or 0),
            -(t.get("total_volume") or 0),
        ),
    )[:limit]

    # ── Phase 4A: register options-flow demand for quote priority ─────────────
    try:
        import data.quote_demand_registry as _qdr
        _flow_syms = [t.get("ticker") for t in rows if isinstance(t, dict) and t.get("ticker")]
        if _flow_syms:
            _qdr.register(_flow_syms, "options_flow", ttl=90)
    except Exception:
        pass

    # Return a slimmed-down verification payload for each row
    _ENRICHED_KEYS = [
        "ticker", "composite_score", "heat_score", "primary_signal",
        "premium", "premium_display", "premium_change_pct",
        "oi_change_pct", "call_flow_pct", "put_flow_pct",
        "call_put_premium_ratio", "call_put_volume_ratio",
        "otm_pct", "is_otm", "is_unusual_otm",
        "days_to_expiry", "expiry", "strike", "option_type",
        "side_bias", "sweep_like", "unusual_volume_ratio",
        "liquidity_score", "asset_type", "market_cap_bucket",
        "underlying_price", "total_volume",
    ]
    return {
        "tab": "master",
        "result_count": len(rows),
        "updated_at": snap.get("updated_at"),
        "tickers": [{k: r.get(k) for k in _ENRICHED_KEYS} for r in rows],
    }


@app.get("/api/options-flow/symbols")
@limiter.limit("120/minute")
async def options_flow_symbols(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
    _sub: None = Depends(require_subscription),
    q: str | None = None,
):
    """
    Lightweight autocomplete source for the Options Flow ticker search box.

    Returns symbols currently in the master screener cache, enriched with
    company name and asset/cap metadata. Falls back to the static seed lists
    if the master cache is not yet warm.

    Optional ?q=<prefix> filter (case-insensitive prefix match, max 20 results).

    Zero new Tradier or FMP calls — reads exclusively from in-memory cache.
    """
    from data.cache import cache

    _SYMS_CACHE_KEY = "options_flow:symbols_autocomplete:v1"
    cached = cache.get(_SYMS_CACHE_KEY)
    if not cached:
        snap = cache.get(_OPTIONS_MASTER_CACHE_KEY) or cache.get(_OPTIONS_MASTER_LKG_KEY)
        if snap:
            entries = [
                {
                    "symbol":           t.get("ticker", ""),
                    "asset_type":       t.get("asset_type", "stock"),
                    "market_cap_bucket": t.get("market_cap_bucket", "unknown"),
                    "composite_score":  t.get("composite_score"),
                }
                for t in snap.get("tickers", [])
                if t.get("ticker")
            ]
        else:
            # Fall back to static seed universe — zero API calls
            _all_seeds = (
                _OPTIONS_ETF_SEEDS
                + _OPTIONS_MEGACAP_SEEDS
                + _OPTIONS_LARGE_CAP_SEEDS
                + _OPTIONS_SMALL_CAP_SEEDS
            )
            _cap_map = {
                **{s: ("etf",      "etf")      for s in _OPTIONS_ETF_SEEDS},
                **{s: ("stock",    "megacap")  for s in _OPTIONS_MEGACAP_SEEDS},
                **{s: ("stock",    "large")    for s in _OPTIONS_LARGE_CAP_SEEDS},
                **{s: ("stock",    "small")    for s in _OPTIONS_SMALL_CAP_SEEDS},
            }
            entries = [
                {
                    "symbol":            sym,
                    "asset_type":        _cap_map.get(sym, ("stock", "unknown"))[0],
                    "market_cap_bucket": _cap_map.get(sym, ("stock", "unknown"))[1],
                    "composite_score":   None,
                }
                for sym in dict.fromkeys(_all_seeds)
            ]
        # Sort by composite_score desc (None sorts last)
        entries.sort(key=lambda e: -(e.get("composite_score") or 0))
        cached = {"symbols": entries, "source": "master_cache" if snap else "seed_fallback"}
        cache.set(_SYMS_CACHE_KEY, cached, 120)  # 2-min TTL — refreshes with master cache

    symbols = cached["symbols"]
    if q:
        prefix = q.upper().strip()
        symbols = [e for e in symbols if e["symbol"].startswith(prefix)][:20]

    return {
        "symbols":  symbols,
        "count":    len(symbols),
        "source":   cached["source"],
        "filtered": q is not None,
    }


@app.get("/api/options/screener/{symbol}")
@limiter.limit("60/minute")
async def options_screener_ticker_detail(
    request: Request,
    symbol: str,
    api_key: str = Header(None, alias="X-API-Key"),
    _sub: None = Depends(require_subscription),
):
    """
    Per-ticker enriched detail view for the popup / drill-down panel.

    Pulls the ticker's row from the master screener cache and adds:
      premium_breakdown   — per-expiry premium totals
      call_put_breakdown  — call vs put premium/volume split
      otm_breakdown       — ITM / ATM / OTM contract counts and premium
      oi_delta_history    — (reserved for future multi-cycle storage)
      recent_snapshot_history — most recent contract snapshots for the ticker

    Existing keys (top_contracts, thesis, score, signal, etc.) are
    preserved exactly.
    """
    import re as _re
    from data.cache import cache
    sym = symbol.upper().strip()
    if not _re.match(r'^[A-Z]{1,5}(-[A-Z]{1,2})?$', sym):
        return JSONResponse(
            status_code=400,
            content={"error": f"Invalid ticker format: {sym!r}. Use 1–5 uppercase letters (e.g. NVDA, BRK-B)."},
        )
    snap = cache.get(_OPTIONS_MASTER_CACHE_KEY) or cache.get(_OPTIONS_MASTER_LKG_KEY)
    if not snap:
        return JSONResponse(status_code=503, content={"error": "no_data_yet"})

    row = next((t for t in snap.get("tickers", []) if t.get("ticker") == sym), None)
    if not row:
        # ── On-demand scan: run live Tradier scoring for tickers not in master cache ──
        import asyncio as _aio

        # Check if portfolio/options already scored this ticker recently
        _od_key = f"portfolio_opts:{sym}"
        _od_cached = cache.get(_od_key)
        if _od_cached:
            return {
                **_od_cached,
                "on_demand": True,
                "premium_breakdown": [],
                "call_put_breakdown": {},
                "otm_breakdown": {},
                "recent_snapshot_history": [],
            }

        if not data_service or not data_service.tradier:
            return JSONResponse(status_code=503, content={"error": "tradier_unavailable"})

        try:
            _raw_q = await _aio.wait_for(
                data_service.tradier.get_quotes([sym]), timeout=5.0
            )
            _q = next(
                (qq for qq in (_raw_q or []) if (qq.get("symbol") or "").upper() == sym),
                {},
            )
            _price = float(_q.get("last") or 0)
            if _price <= 0:
                return JSONResponse(
                    status_code=404,
                    content={"error": f"{sym} not in current screener results"},
                )

            _exps = await _aio.wait_for(
                data_service.tradier.get_option_expirations(sym), timeout=4.0
            )
            if not _exps:
                return JSONResponse(
                    status_code=404,
                    content={"error": f"{sym} has no options chain"},
                )

            _chain_tasks = [
                _aio.wait_for(
                    data_service.tradier.get_option_chain(sym, exp), timeout=5.0
                )
                for exp in _exps[:2]
            ]
            _chains = await _aio.gather(*_chain_tasks, return_exceptions=True)

            _calls_all, _puts_all = [], []
            for _ch in _chains:
                if isinstance(_ch, Exception) or not isinstance(_ch, dict):
                    continue
                _calls_all.extend(_ch.get("calls", []))
                _puts_all.extend(_ch.get("puts", []))

            if not _calls_all and not _puts_all:
                return JSONResponse(
                    status_code=404,
                    content={"error": f"{sym} has no options chain"},
                )

            _call_vol  = sum(int(c.get("volume") or 0) for c in _calls_all)
            _put_vol   = sum(int(c.get("volume") or 0) for c in _puts_all)
            _total_vol = _call_vol + _put_vol
            _pc_ratio  = round(_put_vol / _call_vol, 3) if _call_vol else None

            _iv_vals = [
                float(c.get("smv_vol") or c.get("iv"))
                for c in (_calls_all + _puts_all)[:20]
                if (c.get("smv_vol") or c.get("iv"))
            ]
            _iv_current = round(sum(_iv_vals) / len(_iv_vals), 4) if _iv_vals else None

            _expected_move = None
            _fc = _chains[0] if _chains and not isinstance(_chains[0], Exception) else {}
            _fc_calls = _fc.get("calls", []) if isinstance(_fc, dict) else []
            _fc_puts  = _fc.get("puts",  []) if isinstance(_fc, dict) else []
            _atm_c = min(_fc_calls, key=lambda c: abs((c.get("strike") or 0) - _price), default=None) if _fc_calls else None
            _atm_p = min(_fc_puts,  key=lambda c: abs((c.get("strike") or 0) - _price), default=None) if _fc_puts  else None
            if _atm_c and _atm_p and _price > 0:
                _c_mid = (((_atm_c.get("bid") or 0) + (_atm_c.get("ask") or 0)) / 2)
                _p_mid = (((_atm_p.get("bid") or 0) + (_atm_p.get("ask") or 0)) / 2)
                if _c_mid + _p_mid > 0:
                    _expected_move = round((_c_mid + _p_mid) / _price, 4)

            if _total_vol > 5000:
                if _pc_ratio is not None and _pc_ratio < 0.5:
                    _primary_signal = "unusual_call_flow"
                elif _pc_ratio is not None and _pc_ratio > 2.0:
                    _primary_signal = "unusual_put_flow"
                elif _iv_current and _iv_current > 0.65:
                    _primary_signal = "high_iv"
                else:
                    _primary_signal = "options_activity"
                _confidence = "medium"
            else:
                _primary_signal = "low_activity"
                _confidence = "low"

            _vol_score = min(35, (_total_vol / 10000) * 20) if _total_vol > 0 else 0
            _iv_score  = min(20, (_iv_current or 0) * 40)
            _dir_score = min(20, abs((_pc_ratio or 1.0) - 1.0) * 15) if _pc_ratio else 0
            _composite = round(_vol_score + _iv_score + _dir_score, 1)

            _od_result = {
                "ticker":           sym,
                "underlying_price": round(_price, 4),
                "price_change_pct": float(_q.get("change_percentage") or 0),
                "pc_ratio":         _pc_ratio,
                "iv_current":       _iv_current,
                "expected_move":    _expected_move,
                "primary_signal":   _primary_signal,
                "confidence":       _confidence,
                "composite_score":  _composite,
                "total_volume":     _total_vol,
                "on_demand":        True,
            }
            cache.set(_od_key, _od_result, 600)
            print(f"[OPTIONS_SCREENER_ONDEMAND] scored {sym}: score={_composite} signal={_primary_signal}")
            return {
                **_od_result,
                "premium_breakdown": [],
                "call_put_breakdown": {},
                "otm_breakdown": {},
                "recent_snapshot_history": [],
            }

        except Exception as _od_err:
            print(f"[OPTIONS_SCREENER_ONDEMAND] {sym} error: {_od_err}")
            return JSONResponse(
                status_code=404,
                content={"error": f"{sym} not in current screener results"},
            )

    top_contracts = row.get("top_contracts") or []
    underlying = row.get("underlying_price")

    # ── premium_breakdown (by expiry) ────────────────────────────────────────
    from collections import defaultdict as _dd
    by_expiry: dict = _dd(lambda: {"call_premium": 0.0, "put_premium": 0.0,
                                    "call_volume": 0, "put_volume": 0, "contracts": 0})
    for c in top_contracts:
        expiry = c.get("expiration") or "unknown"
        side = (c.get("type") or c.get("side") or "").lower()
        mid = c.get("mid") or c.get("midpoint") or 0
        vol = c.get("volume") or 0
        prem = (mid * vol * 100) if mid and vol else (c.get("premium_traded_estimate") or 0)
        by_expiry[expiry]["contracts"] += 1
        if side == "call":
            by_expiry[expiry]["call_premium"] += prem
            by_expiry[expiry]["call_volume"] += vol
        elif side == "put":
            by_expiry[expiry]["put_premium"] += prem
            by_expiry[expiry]["put_volume"] += vol

    premium_breakdown = [
        {"expiry": k, **v, "total_premium": round(v["call_premium"] + v["put_premium"], 2)}
        for k, v in sorted(by_expiry.items())
    ]

    # ── call_put_breakdown ───────────────────────────────────────────────────
    total_c_prem = sum(b["call_premium"] for b in by_expiry.values())
    total_p_prem = sum(b["put_premium"] for b in by_expiry.values())
    total_c_vol = sum(b["call_volume"] for b in by_expiry.values())
    total_p_vol = sum(b["put_volume"] for b in by_expiry.values())
    total_prem = total_c_prem + total_p_prem
    call_put_breakdown = {
        "call_premium": round(total_c_prem, 2),
        "put_premium": round(total_p_prem, 2),
        "call_volume": total_c_vol,
        "put_volume": total_p_vol,
        "call_premium_pct": round(total_c_prem / total_prem * 100, 1) if total_prem else None,
        "put_premium_pct": round(total_p_prem / total_prem * 100, 1) if total_prem else None,
    }

    # ── otm_breakdown ────────────────────────────────────────────────────────
    itm_contracts, atm_contracts, otm_contracts = [], [], []
    for c in top_contracts:
        strike = c.get("strike")
        side = (c.get("type") or c.get("side") or "").lower()
        if strike is None or underlying is None:
            otm_contracts.append(c)
            continue
        if side == "call":
            dist = abs(strike - underlying) / underlying
            bucket = "itm" if strike < underlying else ("atm" if dist <= 0.01 else "otm")
        elif side == "put":
            dist = abs(underlying - strike) / underlying
            bucket = "itm" if strike > underlying else ("atm" if dist <= 0.01 else "otm")
        else:
            bucket = "otm"
        if bucket == "itm":
            itm_contracts.append(c)
        elif bucket == "atm":
            atm_contracts.append(c)
        else:
            otm_contracts.append(c)

    def _prem_sum(cs):
        total = 0.0
        for c in cs:
            mid = c.get("mid") or c.get("midpoint") or 0
            vol = c.get("volume") or 0
            total += (mid * vol * 100) if mid and vol else (c.get("premium_traded_estimate") or 0)
        return round(total, 2)

    otm_breakdown = {
        "itm": {"count": len(itm_contracts), "premium": _prem_sum(itm_contracts)},
        "atm": {"count": len(atm_contracts), "premium": _prem_sum(atm_contracts)},
        "otm": {"count": len(otm_contracts), "premium": _prem_sum(otm_contracts)},
    }

    # ── recent_snapshot_history — most recent snapshot entries for this ticker
    from data.options_screener_snapshot import _state as _snap_state
    snap_history = [
        {"key": k, **v}
        for k, v in _snap_state.items()
        if k.startswith(f"{sym}:")
    ]

    return {
        **row,
        "premium_breakdown":      premium_breakdown,
        "call_put_breakdown":     call_put_breakdown,
        "otm_breakdown":          otm_breakdown,
        "oi_delta_history":       [],
        "recent_snapshot_history": snap_history,
    }


@app.api_route("/api/options/dashboard", methods=["GET", "POST"])
@limiter.limit("60/minute")
async def options_dashboard(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
    _sub: None = Depends(require_subscription),
):
    """
    Options flow screener — tab-filtered view of the unified master screener.

    Accepts tab via query param (?tab=large_cap) or JSON body {"tab": "..."}.
    Data is always derived by filtering the master screener snapshot by
    market_cap_bucket — no separate per-tab scan pipelines.

    Backward compatible: same response shape, same ?tab param, same LKG fallback.
    """
    await _wait_for_init()

    if not data_service or not data_service.tradier:
        return JSONResponse(
            status_code=503,
            content={"error": "Tradier options provider not configured. Set TRADIER_API_KEY in secrets."},
        )

    # Parse tab from query param OR request body (default: megacap)
    tab = "megacap"
    query_tab = request.query_params.get("tab")
    raw_tab = query_tab
    if not raw_tab:
        try:
            body = await request.json()
            if isinstance(body, dict):
                raw_tab = body.get("tab")
        except Exception:
            pass
    if raw_tab:
        raw_tab = _TAB_ALIASES.get(raw_tab, raw_tab)
        if raw_tab in _OPTIONS_VALID_TABS:
            tab = raw_tab

    import time as _time
    from data.cache import cache
    from datetime import datetime as _dt_d, timezone as _tz_d

    def _snap_meta(snap: dict, stale: bool) -> dict:
        filtered = _filter_master_snapshot(snap, tab)
        n = len(filtered.get("tickers", []))
        age = int(_time.time() - snap.get("cached_at", _time.time()))
        updated = snap.get("updated_at") or snap.get("cached_at")
        if isinstance(updated, (int, float)):
            updated = _dt_d.fromtimestamp(updated, tz=_tz_d.utc).isoformat()
        if stale and n:
            ds = "stale_but_available"
        elif stale:
            ds = "refresh_in_progress"
        elif n:
            ds = "live_ok"
        else:
            ds = "true_zero_results"
        return filtered, {
            "data_state":          ds,
            "result_count":        n,
            "stale":               stale,
            "refresh_in_progress": stale,
            "cache_age_seconds":   age,
            "updated_at":          updated,
            "source":              snap.get("source", "tradier"),
        }

    # ── Primary: master screener hot cache ──────────────────────────────────
    hot = cache.get(_OPTIONS_MASTER_CACHE_KEY)
    if hot:
        filtered, meta = _snap_meta(hot, stale=False)
        print(f"[OPTIONS_DASH] [{tab}] Master cache hit — {meta['result_count']} tickers")
        return {
            "response":               filtered,
            "structured":             True,
            "preset":                 "options_screener",
            "tab":                    tab,
            "available_tabs":         sorted(_OPTIONS_VALID_TABS),
            "from_cache":             True,
            "next_refresh_in_seconds": max(0, _OPTIONS_CACHE_TTL - meta["cache_age_seconds"]),
            **meta,
        }

    # ── Stale-while-revalidate: master screener LKG ──────────────────────────
    lkg = cache.get(_OPTIONS_MASTER_LKG_KEY)
    if lkg:
        filtered, meta = _snap_meta(lkg, stale=True)
        print(
            f"[OPTIONS_DASH] [{tab}] Master LKG (age={meta['cache_age_seconds']}s, "
            f"{meta['result_count']} tickers)"
        )
        return {
            "response":               filtered,
            "structured":             True,
            "preset":                 "options_screener",
            "tab":                    tab,
            "available_tabs":         sorted(_OPTIONS_VALID_TABS),
            "from_cache":             True,
            "next_refresh_in_seconds": 120,
            **meta,
        }

    # ── Legacy fallback: per-tab LKG (populated by old architecture on disk) ─
    # Allows a seamless transition — disk LKG from before the master screener
    # was deployed will continue to serve until the first master cycle completes.
    legacy_lkg = cache.get(_options_lkg_cache_key(tab))
    if legacy_lkg:
        filtered, meta = _snap_meta(legacy_lkg, stale=True)
        print(f"[OPTIONS_DASH] [{tab}] Legacy per-tab LKG fallback — {meta['result_count']} tickers")
        return {
            "response":               filtered,
            "structured":             True,
            "preset":                 "options_screener",
            "tab":                    tab,
            "available_tabs":         sorted(_OPTIONS_VALID_TABS),
            "from_cache":             True,
            "next_refresh_in_seconds": 120,
            **meta,
        }

    # ── True cold start — no data available anywhere ─────────────────────────
    print(f"[OPTIONS_DASH] [{tab}] No data yet — master screener loop will warm within ~90s")
    from data.options_flow_engine import OPTIONS_FLOW_DEFAULTS, OPTIONS_FLOW_WEIGHTS
    empty_result = {
        "display_type":  "options_screener",
        "scan_type":     "options_flow",
        "tab":           tab,
        "cached_at":     _time.time(),
        "updated_at":    _dt_d.now(_tz_d.utc).isoformat(),
        "source":        "none",
        "tickers":       [],
        "all_contracts": [],
        "filter_defaults":  dict(OPTIONS_FLOW_DEFAULTS),
        "score_weights":    dict(OPTIONS_FLOW_WEIGHTS),
        "pipeline_stats":   {
            "prefilter_candidate_count": 0,
            "options_inspection_count":  0,
            "ranked_result_count":       0,
            "degraded_sources":          ["cold_start:master_screener_warming"],
        },
        "market_summary": {"message": "Master screener warming — check back in ~90 seconds."},
    }
    return {
        "response":            empty_result,
        "structured":          True,
        "preset":              "options_screener",
        "tab":                 tab,
        "available_tabs":      sorted(_OPTIONS_VALID_TABS),
        "from_cache":          False,
        "stale":               False,
        "data_state":          "no_data_yet",
        "result_count":        0,
        "refresh_in_progress": True,
        "updated_at":          None,
        "source":              "none",
        "cache_age_seconds":   None,
        "next_refresh_in_seconds": 90,
    }


# ── OPTIONS FLOW — Unified 4-panel all-tabs endpoint ────────────────────

@app.get("/api/options/all-tabs")
@limiter.limit("60/minute")
async def options_all_tabs(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
    _sub: None = Depends(require_subscription),
):
    """
    Returns all four Options Flow category datasets in a single response,
    derived by filtering the unified master screener snapshot by
    market_cap_bucket.  Same response shape as before for full backward compat.

    Response shape:
      {
        "tabs": {
          "etf":       { "data_state": "live_ok", "result_count": N, "tickers": [...], ... },
          "megacap":   { ... },
          "large_cap": { ... },
          "small_cap": { ... }
        },
        "any_live": true,
        "all_live": false,
        "generated_at": "2026-..."
      }
    """
    await _wait_for_init()
    import time as _time
    from data.cache import cache
    import datetime as _dt_mod

    # Load master snapshot (hot preferred, LKG fallback)
    master = cache.get(_OPTIONS_MASTER_CACHE_KEY) or cache.get(_OPTIONS_MASTER_LKG_KEY)
    stale_master = cache.get(_OPTIONS_MASTER_CACHE_KEY) is None

    tabs_out: dict = {}
    any_live = False
    all_live = True

    def _tab_entry_from_master(master_snap: dict, tab: str, stale: bool) -> dict:
        """Build one tab's entry by filtering the master snapshot."""
        filtered = _filter_master_snapshot(master_snap, tab)
        n = len(filtered.get("tickers", []))
        age = int(_time.time() - master_snap.get("cached_at", _time.time()))
        updated = master_snap.get("updated_at") or master_snap.get("cached_at")
        if isinstance(updated, (int, float)):
            updated = _dt_mod.datetime.fromtimestamp(updated, tz=_dt_mod.timezone.utc).isoformat()
        if stale and n == 0:
            ds = "refresh_in_progress"
        elif stale and n:
            ds = "stale_but_available"
        elif n:
            ds = "live_ok"
        else:
            ds = "true_zero_results"
        return {
            "data_state":          ds,
            "result_count":        n,
            "stale":               stale,
            "cache_age_seconds":   age,
            "refresh_in_progress": stale,
            "updated_at":          updated,
            "source":              master_snap.get("source", "tradier"),
            "tickers":             filtered.get("tickers", []),
            "all_contracts":       filtered.get("all_contracts", [])[:200],
            "market_summary":      master_snap.get("market_summary", {}),
            "pipeline_stats":      master_snap.get("pipeline_stats", {}),
            "filter_defaults":     master_snap.get("filter_defaults", {}),
            "score_weights":       master_snap.get("score_weights", {}),
        }

    for tab in sorted(_OPTIONS_VALID_TABS):
        if master:
            entry = _tab_entry_from_master(master, tab, stale=stale_master)
            if entry["result_count"]:
                any_live = True
            if stale_master:
                all_live = False
        else:
            # No master data yet — try per-tab legacy LKG
            legacy = cache.get(_options_lkg_cache_key(tab))
            if legacy:
                filtered_legacy = _filter_master_snapshot(legacy, None)  # legacy has no bucket tags, return as-is
                n = len(legacy.get("tickers", []))
                entry = {
                    "data_state":          "stale_but_available" if n else "refresh_in_progress",
                    "result_count":        n,
                    "stale":               True,
                    "cache_age_seconds":   int(_time.time() - legacy.get("cached_at", _time.time())),
                    "refresh_in_progress": True,
                    "updated_at":          legacy.get("updated_at"),
                    "source":              legacy.get("source", "tradier"),
                    "tickers":             legacy.get("tickers", []),
                    "all_contracts":       legacy.get("all_contracts", [])[:200],
                    "market_summary":      legacy.get("market_summary", {}),
                    "pipeline_stats":      legacy.get("pipeline_stats", {}),
                    "filter_defaults":     legacy.get("filter_defaults", {}),
                    "score_weights":       legacy.get("score_weights", {}),
                }
                if n:
                    any_live = True
                all_live = False
            else:
                entry = {
                    "data_state":          "no_data_yet",
                    "result_count":        0,
                    "stale":               False,
                    "cache_age_seconds":   None,
                    "refresh_in_progress": True,
                    "updated_at":          None,
                    "source":              "none",
                    "tickers":             [],
                    "all_contracts":       [],
                    "market_summary":      {},
                    "pipeline_stats":      {},
                    "filter_defaults":     {},
                    "score_weights":       {},
                }
                all_live = False

        tabs_out[tab] = entry

    return {
        "tabs":           tabs_out,
        "any_live":       any_live,
        "all_live":       all_live,
        "available_tabs": sorted(_OPTIONS_VALID_TABS),
        "generated_at":   _dt_mod.datetime.utcnow().isoformat() + "Z",
    }


# ── OPTIONS FLOW — Agent chat query ─────────────────────────────────────

@app.post("/api/options/query")
@limiter.limit("10/minute")
async def options_flow_query(
    request: Request,
    body: dict = Body(...),
    api_key: str = Header(None, alias="X-API-Key"),
    _sub: None = Depends(require_subscription),
):
    """
    Agent-powered chat for the Options Flow page.
    Sends the user's question + full cached dashboard data to Claude.
    """
    query = (body.get("query") or body.get("prompt") or "").strip()
    tab = body.get("tab", "megacap")
    conversation_id = body.get("conversation_id")
    history = body.get("history") or []

    if not query:
        return JSONResponse(status_code=400, content={"error": "No query provided."})

    await _wait_for_init()

    if not _jwt_or_key(request, api_key):
        return JSONResponse(status_code=403, content={"error": "Invalid or missing API key."})

    # ── Gather all cached options data the user can see on screen ────────
    from data.cache import cache
    import json as _oj

    context_parts = []

    for _tab in sorted(_OPTIONS_VALID_TABS):
        cached = cache.get(_options_cache_key(_tab))
        if cached:
            tickers = cached.get("tickers", [])
            all_contracts = cached.get("all_contracts", [])
            context_parts.append(f"=== OPTIONS FLOW DATA — {_tab.upper()} TAB ({len(tickers)} tickers, {len(all_contracts)} contracts) ===")

            for t in tickers:
                sym = t.get("ticker", "?")
                price = t.get("price", "")
                change_pct = t.get("change_pct", "")
                composite = t.get("composite_score", "")
                flow_score = t.get("flow_score", "")
                gamma_score = t.get("gamma_score", "")
                asymmetry_score = t.get("asymmetry_score", "")
                volatility_score = t.get("volatility_score", "")
                sentiment_score = t.get("sentiment_score", "")
                stock_context_score = t.get("stock_context_score", "")
                signal_type = t.get("signal_type", "")
                confidence = t.get("confidence", "")
                focus_dates = t.get("focus_dates", [])
                tags = t.get("tags", [])
                reasons = t.get("reasons", [])
                total_vol = t.get("total_volume", "")
                pc_ratio = t.get("pc_ratio", "")
                calls = t.get("calls", "")
                puts = t.get("puts", "")
                iv_avg = t.get("iv_avg", "")
                exp_move = t.get("expected_move", "")
                rel_vol = t.get("relative_volume", "")
                vol_ratio = t.get("volume_ratio", "")
                oi_ratio = t.get("oi_ratio", "")
                technicals = t.get("technicals", {})
                hist_vol = t.get("historic_volume", {})

                lines = [
                    f"\n## {sym}  ${price}  {change_pct}%",
                    f"   Composite: {composite} | Flow: {flow_score} | Gamma: {gamma_score} | Asymmetry: {asymmetry_score}",
                    f"   Volatility: {volatility_score} | Sentiment: {sentiment_score} | StockContext: {stock_context_score}",
                    f"   Signal: {signal_type} | Confidence: {confidence}",
                    f"   Tags: {', '.join(tags) if tags else 'none'}",
                    f"   Reasons: {'; '.join(reasons) if reasons else 'none'}",
                    f"   Focus dates: {', '.join(focus_dates) if focus_dates else 'none'}",
                    f"   Total Vol: {total_vol} | P/C Ratio: {pc_ratio} | Calls: {calls} | Puts: {puts}",
                    f"   IV Avg: {iv_avg} | Exp Move: {exp_move} | Rel Vol: {rel_vol} | Vol Ratio: {vol_ratio} | OI Ratio: {oi_ratio}",
                ]
                if technicals:
                    lines.append(f"   Technicals: {_oj.dumps(technicals, default=str)[:500]}")
                if hist_vol:
                    lines.append(f"   Historic Volume: {_oj.dumps(hist_vol, default=str)[:500]}")
                context_parts.append("\n".join(lines))

            # Include top contracts
            if all_contracts:
                context_parts.append(f"\n### Top Contracts ({_tab})")
                for c in all_contracts[:30]:
                    context_parts.append(
                        f"  {c.get('underlying','?')} {c.get('contract_symbol','')} "
                        f"Strike:{c.get('strike','')} Exp:{c.get('expiration','')} "
                        f"Side:{c.get('side','')} Vol:{c.get('volume','')} OI:{c.get('open_interest','')} "
                        f"IV:{c.get('iv','')} Delta:{c.get('delta','')} Gamma:{c.get('gamma','')} "
                        f"Bid:{c.get('bid','')} Ask:{c.get('ask','')} Last:{c.get('last','')}"
                    )

    if not context_parts:
        context_parts.append("(No cached options data available. Answering based on general options knowledge.)")

    options_context = "\n".join(context_parts)

    # ── Build system prompt ─────────────────────────────────────────────
    system_prompt = f"""You are Caelyn, an elite options flow analyst at a quantitative hedge fund.
You have access to LIVE options flow data from the user's dashboard. This data is real-time
and comes from Public.com's brokerage API with proprietary composite scoring.

DASHBOARD DATA (this is exactly what the user sees on their screen):
{options_context}

SCORING SYSTEM:
- Composite Score (0-100): Weighted blend of all sub-scores. Higher = stronger signal.
- Flow Score: Measures unusual volume vs open interest — high means new large positions.
- Gamma Score: Measures gamma exposure concentration — high means market makers are heavily hedged.
- Asymmetry Score: Risk/reward asymmetry of the options positioning.
- Volatility Score: IV percentile and term structure signals.
- Sentiment Score: Put/call ratio and directional bias.
- Stock Context Score: Underlying stock technicals, price action quality.

SIGNAL TYPES:
- UNUSUAL_FLOW: Volume significantly exceeds open interest — new large positions being opened.
- GAMMA_APPROX: Concentrated gamma near current price — potential for sharp moves.
- BREAKOUT_CONFIRM: Options flow confirms a technical breakout pattern.

CONFIDENCE LEVELS: HIGH_CONFIDENCE > MEDIUM > LOW

RULES:
1. Reference SPECIFIC data from the dashboard — cite actual numbers, scores, tickers.
2. When asked for top picks, rank by composite score and explain the signal confluence.
3. When asked about a specific ticker, give the full breakdown of all its scores and what they mean.
4. For entry suggestions, consider: IV level (is premium expensive?), expiration timing, strike selection relative to expected move.
5. Be direct and actionable. No generic advice — everything should reference the live data.
6. If the user asks about a ticker not in the data, say so clearly.
7. Put/call ratio below 0.7 = bullish, above 1.0 = bearish.
8. Always mention risk factors alongside opportunities.
9. Keep responses focused and scannable — use bullet points and bold for key numbers."""

    # ── Build messages (with conversation history) ──────────────────────
    messages = []
    for msg in history[-10:]:
        role = msg.get("role", "user")
        content = msg.get("content", "")
        if role in ("user", "assistant") and content:
            messages.append({"role": role, "content": content})
    messages.append({"role": "user", "content": query})

    # ── Call Claude ──────────────────────────────────────────────────────
    # NOTE: Use asyncio.to_thread() so the sync Anthropic client does NOT
    # block the uvicorn event loop while waiting for the API response.
    from config import ANTHROPIC_API_KEY
    import anthropic

    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        resp = await asyncio.to_thread(
            client.messages.create,
            model=MODEL_CLAUDE_BALANCED,
            max_tokens=2000,
            system=system_prompt,
            messages=messages,
        )
        answer = resp.content[0].text if resp.content else ""
    except Exception as e:
        print(f"[OPTIONS_QUERY] Claude error: {e}")
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=502, content={"error": f"AI analysis error: {str(e)[:200]}"})

    # ── Persist to conversation history ─────────────────────────────────
    if conversation_id:
        try:
            from data.chat_history import append_message as _append_msg
            _append_msg(conversation_id, "user", query, message_type="options_chat")
            _append_msg(conversation_id, "assistant", answer, message_type="options_chat")
        except Exception as e:
            print(f"[OPTIONS_QUERY] History save error (non-fatal): {e}")

    print(f"[OPTIONS_QUERY] Answered query ({len(query)} chars) -> {len(answer)} chars response")
    return {
        "response": answer,
        "query": query,
        "tab": tab,
        "tickers_in_context": len([p for p in context_parts if p.startswith("\n##")]),
    }


@app.get("/api/options/chain/{symbol}")
@limiter.limit("30/minute")
async def get_options_chain(
    request: Request,
    symbol: str,
    expiration: str = None,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """Get option chain for a single ticker. Used when clicking into a specific ticker."""
    await _wait_for_init()

    if not data_service or not data_service.public_com:
        return JSONResponse(status_code=503, content={"error": "Public.com not configured"})

    symbol = symbol.upper()

    try:
        expirations = await data_service.public_com.get_option_expirations(symbol)
        if not expirations:
            return {"symbol": symbol, "expirations": [], "chain": {}, "error": "No expirations found"}

        target_exp = expiration if expiration and expiration in expirations else expirations[0]
        chain = await data_service.public_com.get_full_chain_with_greeks(symbol, target_exp)

        return {
            "symbol": symbol,
            "expirations": expirations,
            "selected_expiration": target_exp,
            "chain": chain,
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"Chain error for {symbol}: {str(e)[:200]}"})


@app.get("/api/options/expirations/{symbol}")
@limiter.limit("30/minute")
async def get_options_expirations(
    request: Request,
    symbol: str,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """Get available option expiration dates for a ticker."""
    await _wait_for_init()

    if not data_service or not data_service.public_com:
        return JSONResponse(status_code=503, content={"error": "Public.com not configured"})

    try:
        expirations = await data_service.public_com.get_option_expirations(symbol.upper())
        return {"symbol": symbol.upper(), "expirations": expirations}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)[:200]})


# ═══════════════════════════════════════════════════════════════════
# POLYGON HISTORIC OPTIONS INGESTION — Background data pipeline
# ═══════════════════════════════════════════════════════════════════

async def _polygon_options_ingestion_loop():
    """
    Background loop that fetches historic options data + technical indicators
    from Polygon (Massive free tier, 5 calls/min) for the full watchlist.
    Stores everything in Neon PostgreSQL for the agent's TA reference.
    Initial load: ~4-5 hours for 95 tickers. Then refreshes every 6 hours.
    """
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _init_event.wait, 180)

    if data_service is None or not getattr(data_service, "polygon_options", None):
        print("[POLYGON_INGEST] Polygon options provider not available, skipping ingestion loop")
        return

    try:
        from data.options_ingestion import run_ingestion_loop
        print("[POLYGON_INGEST] Starting historic options data ingestion loop")
        await run_ingestion_loop(data_service.polygon_options, init_event=_init_event)
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"[POLYGON_INGEST] Fatal error in ingestion loop: {e}")


# ── Historic Options Data API Endpoints ─────────────────────────────

@app.get("/api/options/history/{symbol}")
@limiter.limit("30/minute")
async def get_options_history_endpoint(
    request: Request,
    symbol: str,
    option_type: str = None,
    from_date: str = None,
    to_date: str = None,
    limit: int = 500,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """Get stored historic options data for a ticker from Neon DB."""
    from data.options_history_store import get_options_history
    try:
        history = get_options_history(
            symbol.upper(),
            option_type=option_type,
            from_date=from_date,
            to_date=to_date,
            limit=min(limit, 2000),
        )
        return {"symbol": symbol.upper(), "count": len(history), "data": history}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)[:200]})


@app.get("/api/options/volume-summary/{symbol}")
@limiter.limit("30/minute")
async def get_options_volume_summary_endpoint(
    request: Request,
    symbol: str,
    days: int = 30,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """Get aggregated options volume summary from stored historic data."""
    from data.options_history_store import get_options_volume_summary
    try:
        summary = get_options_volume_summary(symbol.upper(), days=min(days, 365))
        return {"symbol": symbol.upper(), "summary": summary}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)[:200]})


@app.get("/api/options/technicals/{symbol}")
@limiter.limit("30/minute")
async def get_options_technicals_endpoint(
    request: Request,
    symbol: str,
    indicator: str = None,
    from_date: str = None,
    limit: int = 250,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """Get stored technical indicators for a ticker from Neon DB."""
    from data.options_history_store import get_technicals, get_latest_technicals
    try:
        if not indicator and not from_date and limit <= 10:
            # Return latest snapshot
            latest = get_latest_technicals(symbol.upper())
            return {"symbol": symbol.upper(), "latest": latest}

        data = get_technicals(
            symbol.upper(),
            indicator=indicator,
            from_date=from_date,
            limit=min(limit, 2000),
        )
        return {"symbol": symbol.upper(), "count": len(data), "data": data}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)[:200]})


@app.get("/api/options/data-coverage")
@limiter.limit("10/minute")
async def get_options_data_coverage(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """Get summary of historic options data coverage in the database."""
    from data.options_history_store import get_data_coverage
    try:
        coverage = get_data_coverage()
        return {"coverage": coverage}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)[:200]})


@app.get("/api/options/fetch-progress")
@limiter.limit("10/minute")
async def get_options_fetch_progress_endpoint(
    request: Request,
    ticker: str = None,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """Get ingestion progress for watchlist tickers."""
    from data.options_history_store import get_fetch_progress
    try:
        if ticker:
            progress = get_fetch_progress(ticker.upper())
            return {"ticker": ticker.upper(), "progress": progress}
        else:
            progress = get_fetch_progress()
            return {"count": len(progress), "progress": progress}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)[:200]})


# ── Ingestion Summary (for frontend "Ingestion Status" dropdown) ────────

@app.get("/api/options/ingestion-summary")
@limiter.limit("20/minute")
async def get_options_ingestion_summary(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """
    Aggregate ingestion stats for the frontend Ingestion Status dropdown.
    """
    from data.options_history_store import get_data_coverage, get_fetch_progress
    try:
        coverage = get_data_coverage()
        progress = get_fetch_progress() or []

        completed = sum(1 for p in progress if p.get("status") == "complete")
        in_progress = sum(1 for p in progress if p.get("status") == "in_progress")
        errored = sum(1 for p in progress if p.get("status") == "error")
        pending = sum(1 for p in progress if p.get("status") == "pending")

        last_updated = None
        for p in progress:
            ts = p.get("updated_at")
            if ts and (last_updated is None or ts > last_updated):
                last_updated = ts

        oh = coverage.get("options_history", {})

        return {
            "tickers_ingested": completed,
            "tickers_total": len(_OPTIONS_FULL_WATCHLIST),
            "tickers_in_progress": in_progress,
            "tickers_errored": errored,
            "tickers_pending": pending,
            "total_bars": oh.get("total_bars", 0),
            "total_contracts": oh.get("contracts", 0),
            "earliest_date": oh.get("earliest_date"),
            "latest_date": oh.get("latest_date"),
            "last_updated": last_updated,
            "fetch_progress_by_status": coverage.get("fetch_progress", {}),
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)[:200]})


# ── Scan Defaults (options-level params, all tabs) ────────────────────
# Market-cap ranges are hardcoded per tier (TIER_MCAP_RANGES) and NOT editable.
# Only options-level screening params (OI, volume, spread, DTE, etc.) are editable.

@app.get("/api/options/scan-defaults")
@limiter.limit("30/minute")
async def get_scan_defaults(
    request: Request,
    tab: str = "megacap",
    api_key: str = Header(None, alias="X-API-Key"),
):
    """
    Get current scan defaults for any tab.
    Market-cap ranges are hardcoded per tier and shown for info only.
    """
    from data.options_flow_engine import OPTIONS_FLOW_DEFAULTS, TIER_MCAP_RANGES
    tab = _TAB_ALIASES.get(tab, tab)
    if tab not in _OPTIONS_VALID_TABS:
        tab = "megacap"

    base = dict(OPTIONS_FLOW_DEFAULTS)
    user_ov = _SCAN_USER_OVERRIDES.get(tab, {})
    merged = {**base, **{k: v for k, v in user_ov.items() if k in _EDITABLE_SCAN_KEYS}}

    tier_min, tier_max = TIER_MCAP_RANGES.get(tab, (0, None))
    return {
        "tab": tab,
        "editable": True,
        "editable_keys": sorted(_EDITABLE_SCAN_KEYS),
        "defaults": merged,
        "user_overrides": dict(user_ov),
        "tier_mcap_range": {
            "min": tier_min,
            "max": tier_max,
            "note": "Market-cap range is hardcoded per tier and not editable via scan-defaults.",
        },
        "available_tabs": sorted(_OPTIONS_VALID_TABS),
    }


@app.put("/api/options/scan-defaults")
@limiter.limit("10/minute")
async def update_scan_defaults(
    request: Request,
    body: dict = Body(...),
    api_key: str = Header(None, alias="X-API-Key"),
):
    """
    Update scan defaults for any tab (options-level params only).
    Market-cap ranges are NOT editable.
    Send {"reset": true} to clear all overrides back to defaults.
    """
    from data.options_flow_engine import OPTIONS_FLOW_DEFAULTS

    tab = body.get("tab", "megacap")
    tab = _TAB_ALIASES.get(tab, tab)
    if tab not in _OPTIONS_VALID_TABS:
        return JSONResponse(
            status_code=400,
            content={"error": f"Invalid tab. Valid tabs: {sorted(_OPTIONS_VALID_TABS)}"},
        )

    if body.get("reset"):
        _SCAN_USER_OVERRIDES.pop(tab, None)
        from data.cache import cache
        cache.delete(_options_cache_key(tab))
        return {"message": f"{tab} defaults reset to system defaults", "defaults": dict(OPTIONS_FLOW_DEFAULTS)}

    overrides = body.get("overrides", {})
    if not overrides:
        return JSONResponse(status_code=400, content={"error": "No overrides provided. Send {\"overrides\": {\"key\": value}}"})

    accepted = {}
    rejected = []
    for k, v in overrides.items():
        if k in _EDITABLE_SCAN_KEYS:
            try:
                base_type = type(OPTIONS_FLOW_DEFAULTS[k])
                _SCAN_USER_OVERRIDES.setdefault(tab, {})[k] = base_type(v)
                accepted[k] = base_type(v)
            except (ValueError, TypeError) as e:
                rejected.append({"key": k, "error": str(e)})
        else:
            rejected.append({"key": k, "error": f"Not an editable key. Editable: {sorted(_EDITABLE_SCAN_KEYS)}"})

    from data.cache import cache
    cache.delete(_options_cache_key(tab))

    merged = {**OPTIONS_FLOW_DEFAULTS, **_SCAN_USER_OVERRIDES.get(tab, {})}
    return {
        "message": f"Updated {len(accepted)} scan defaults for {tab} tab",
        "accepted": accepted,
        "rejected": rejected if rejected else None,
        "current_defaults": merged,
    }


# ── Tradier dashboard removed — Options Flow now uses TradierFlowEngine directly ──
# The /api/tradier/dashboard endpoint has been consolidated into /api/options/dashboard.
# Tradier-specific utility endpoints (chain, history, timesales, etc.) are kept below.


# ── Tradier-powered options utility endpoints ────────────────────────
# These endpoints expose Tradier's richer data (chain with inline greeks,
# history, timesales, contract detail) under /api/options/.
# Legacy /api/tradier/ aliases are kept for backward compatibility.

@app.get("/api/options/chain/{symbol}")
@app.get("/api/tradier/chain/{symbol}")
@limiter.limit("30/minute")
async def tradier_chain(
    request: Request,
    symbol: str,
    expiration: str = None,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """Get Tradier option chain with inline greeks/IV."""
    await _wait_for_init()
    if not data_service or not data_service.tradier:
        return JSONResponse(status_code=503, content={"error": "Tradier not configured"})

    if not expiration:
        expirations = await data_service.tradier.get_option_expirations(symbol)
        if not expirations:
            return {"chain": {"calls": [], "puts": []}, "expirations": []}
        expiration = expirations[0]

    chain = await data_service.tradier.get_option_chain(symbol, expiration)
    expirations = await data_service.tradier.get_option_expirations(symbol)
    return {
        "chain": chain,
        "expirations": expirations,
        "data_source": "tradier",
    }


@app.get("/api/options/expirations/{symbol}")
@app.get("/api/tradier/expirations/{symbol}")
@limiter.limit("30/minute")
async def tradier_expirations(
    request: Request,
    symbol: str,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """Get available option expirations from Tradier."""
    await _wait_for_init()
    if not data_service or not data_service.tradier:
        return JSONResponse(status_code=503, content={"error": "Tradier not configured"})

    expirations = await data_service.tradier.get_option_expirations(symbol)
    strikes_tasks = [data_service.tradier.get_option_strikes(symbol, exp) for exp in expirations[:5]]
    strikes_results = await asyncio.gather(*strikes_tasks, return_exceptions=True)
    strikes_map = {}
    for exp, strikes in zip(expirations[:5], strikes_results):
        if not isinstance(strikes, Exception):
            strikes_map[exp] = strikes

    return {
        "expirations": expirations,
        "strikes_by_expiration": strikes_map,
        "data_source": "tradier",
    }


@app.get("/api/options/strikes/{symbol}")
@app.get("/api/tradier/strikes/{symbol}")
@limiter.limit("30/minute")
async def tradier_strikes(
    request: Request,
    symbol: str,
    expiration: str = None,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """Get strike prices for a symbol/expiration from Tradier."""
    await _wait_for_init()
    if not data_service or not data_service.tradier:
        return JSONResponse(status_code=503, content={"error": "Tradier not configured"})

    if not expiration:
        expirations = await data_service.tradier.get_option_expirations(symbol)
        expiration = expirations[0] if expirations else None
    if not expiration:
        return {"strikes": [], "expiration": None}

    strikes = await data_service.tradier.get_option_strikes(symbol, expiration)
    return {"strikes": strikes, "expiration": expiration, "data_source": "tradier"}


@app.get("/api/options/history-live/{symbol}")
@app.get("/api/tradier/history/{symbol}")
@limiter.limit("30/minute")
async def tradier_history(
    request: Request,
    symbol: str,
    interval: str = "daily",
    start: str = None,
    end: str = None,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """
    Historical OHLCV from Tradier. Works for both equities AND option OCC symbols.
    This is a Tradier-specific upgrade — Public.com doesn't offer this.
    """
    await _wait_for_init()
    if not data_service or not data_service.tradier:
        return JSONResponse(status_code=503, content={"error": "Tradier not configured"})

    bars = await data_service.tradier.get_history(symbol, interval=interval, start=start, end=end)
    return {"history": bars, "symbol": symbol, "interval": interval, "data_source": "tradier"}


@app.get("/api/options/timesales/{symbol}")
@app.get("/api/tradier/timesales/{symbol}")
@limiter.limit("30/minute")
async def tradier_timesales(
    request: Request,
    symbol: str,
    interval: str = "5min",
    start: str = None,
    end: str = None,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """
    Intraday time-and-sales from Tradier. Works for equities and option OCC symbols.
    Tradier-specific upgrade — not available on the Options Flow page.
    """
    await _wait_for_init()
    if not data_service or not data_service.tradier:
        return JSONResponse(status_code=503, content={"error": "Tradier not configured"})

    ticks = await data_service.tradier.get_timesales(symbol, interval=interval, start=start, end=end)
    return {"timesales": ticks, "symbol": symbol, "interval": interval, "data_source": "tradier"}


@app.get("/api/options/quote/{symbol}")
@app.get("/api/tradier/quote/{symbol}")
@limiter.limit("60/minute")
async def tradier_quote(
    request: Request,
    symbol: str,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """Get a real-time quote from Tradier (equity or option OCC symbol)."""
    await _wait_for_init()
    if not data_service or not data_service.tradier:
        return JSONResponse(status_code=503, content={"error": "Tradier not configured"})

    quote = await data_service.tradier.get_quote(symbol)
    return {"quote": quote, "data_source": "tradier"}


@app.get("/api/options/contract-detail/{symbol}")
@app.get("/api/tradier/contract-detail/{symbol}")
@limiter.limit("30/minute")
async def tradier_contract_detail(
    request: Request,
    symbol: str,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """
    Rich contract detail view — Tradier-specific.
    Fetches quote + historical bars + intraday time-and-sales for a single OCC option symbol.
    Not available on the Options Flow page.
    """
    await _wait_for_init()
    if not data_service or not data_service.tradier:
        return JSONResponse(status_code=503, content={"error": "Tradier not configured"})

    from datetime import timedelta, date as _date
    quote_task = data_service.tradier.get_quote(symbol)
    history_task = data_service.tradier.get_history(
        symbol, interval="daily",
        start=(_date.today() - timedelta(days=90)).isoformat(),
    )
    timesales_task = data_service.tradier.get_timesales(symbol, interval="5min")

    quote, history, timesales = await asyncio.gather(
        quote_task, history_task, timesales_task, return_exceptions=True,
    )
    quote = quote if not isinstance(quote, Exception) else None
    history = history if not isinstance(history, Exception) else []
    timesales = timesales if not isinstance(timesales, Exception) else []

    return {
        "contract_symbol": symbol,
        "quote": quote,
        "history": history,
        "timesales": timesales,
        "data_source": "tradier",
    }


# ── Tradier: delegate shared endpoints to Options Flow handlers ─────
# These endpoints read from local DB/shared state — not provider-specific.

@app.get("/api/tradier/volume-summary/{symbol}")
@limiter.limit("30/minute")
async def tradier_volume_summary(request: Request, symbol: str, api_key: str = Header(None, alias="X-API-Key")):
    return await get_options_volume_summary_endpoint(request, symbol, api_key)

@app.get("/api/tradier/technicals/{symbol}")
@limiter.limit("30/minute")
async def tradier_technicals(request: Request, symbol: str, api_key: str = Header(None, alias="X-API-Key")):
    return await get_options_technicals_endpoint(request, symbol, api_key)

@app.get("/api/tradier/scan-defaults")
@limiter.limit("30/minute")
async def tradier_scan_defaults(request: Request, tab: str = "megacap", api_key: str = Header(None, alias="X-API-Key")):
    return await get_scan_defaults(request, tab, api_key)

@app.put("/api/tradier/scan-defaults")
@limiter.limit("10/minute")
async def tradier_update_scan_defaults(request: Request, body: dict = Body(...), api_key: str = Header(None, alias="X-API-Key")):
    return await update_scan_defaults(request, body, api_key)


# ═══════════════════════════════════════════════════════════════════════
# ── Macro Terminal API Endpoints ──────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════

_macro_provider = None  # Initialized lazily after data_service is ready


def _get_macro_provider():
    global _macro_provider
    if _macro_provider is None:
        if data_service and data_service.fred:
            from data.macro_provider import MacroProvider
            _macro_provider = MacroProvider(
                fred_provider=data_service.fred,
                fmp_provider=data_service.fmp if hasattr(data_service, "fmp") else None,
                tradier_provider=data_service.tradier if hasattr(data_service, "tradier") else None,
                fear_greed_provider=data_service.fear_greed if hasattr(data_service, "fear_greed") else None,
                yahoo_provider=data_service.yahoo if hasattr(data_service, "yahoo") else None,
            )
    return _macro_provider


# ── Background refresh loop ──────────────────────────────────────────

_MACRO_PRECOMPUTE_INTERVAL = 720  # 12 minutes (cache TTL is 15 min)


async def _macro_precompute_loop():
    """Background loop to keep macro data warm in cache."""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _init_event.wait, 120)

    import time as _time

    while True:
        mp = _get_macro_provider()
        if not mp:
            print("[MACRO_PRECOMPUTE] Macro provider not available, retrying in 60s")
            await asyncio.sleep(60)
            continue

        try:
            t0 = _time.time()
            # Hybrid async: FMP real-time + FRED economic releases
            dashboard = await mp.get_dashboard()

            # Pre-warm indicators
            await asyncio.to_thread(mp.get_indicators)

            # Pre-warm calendar
            await mp.get_calendar(days_ahead=14)

            # Pre-warm tab endpoints (rates, inflation, growth, labor, risk)
            await asyncio.gather(
                mp.get_rates(),
                mp.get_inflation(),
                mp.get_growth(),
                mp.get_labor(),
                mp.get_risk(),
                return_exceptions=True,
            )

            elapsed = _time.time() - t0
            print(f"[MACRO_PRECOMPUTE] Refreshed dashboard + indicators + calendar + tabs in {elapsed:.1f}s")

        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"[MACRO_PRECOMPUTE] Error: {e}")

        await asyncio.sleep(_MACRO_PRECOMPUTE_INTERVAL)


# ── Strategy History precompute loop ─────────────────────────────────

async def _strategy_history_precompute_loop():
    """
    Background loop to pre-warm strategy historical series caches
    (FRED VIXCLS, FRED DGS10, yfinance ^GSPC — all 5-year windows).

    Fires 3 minutes after init (so _macro_precompute_loop's FRED calls
    have already settled), then repeats every 3 hours.  The 6-h TTLs on
    the strategy:hist:* and strategy:spx_hist:* cache keys never expire
    between runs.  Strategy endpoints read from cache on every page load
    — no provider calls at request time.
    """
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _init_event.wait, 180)

    import time as _time

    while True:
        mp = _get_macro_provider()
        if not mp:
            print("[STRATEGY_HIST] Macro provider not ready, retrying in 60s")
            await asyncio.sleep(60)
            continue

        try:
            from services.strategy_macro_service import precompute_strategy_history
            await precompute_strategy_history(mp)
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"[STRATEGY_HIST] Precompute error: {e}")

        await asyncio.sleep(10800)  # 3 hours


# ── Sector Rotation precompute loop ──────────────────────────────────

_SR_PRECOMPUTE_INTERVAL = 900  # 15 minutes


async def _sector_rotation_precompute_loop():
    """
    Pre-warm the sector rotation dashboard cache on startup and every 15 minutes.
    Runs immediately — no wait for _init_event because it has no dependency on data_service.
    """
    import time as _time

    first_run = True
    while True:
        try:
            t0 = _time.time()
            from services.sector_rotation.service import get_dashboard as _sr_get_dashboard
            await _sr_get_dashboard(include_analysis=False)
            elapsed = _time.time() - t0
            print(f"[SR_PRECOMPUTE] {'Startup warm' if first_run else 'Refreshed'} sector rotation dashboard in {elapsed:.1f}s")
            first_run = False
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"[SR_PRECOMPUTE] Error: {e}")

        await asyncio.sleep(_SR_PRECOMPUTE_INTERVAL)


# ── GET /api/home/dashboard ──────────────────────────────────────────
# Aggregator for the new Home page. Composes ALREADY-CACHED services; does
# not add net-new third-party API calls. Wrapped tasks individually with
# return_exceptions so one upstream failure never breaks the payload.

@app.get("/api/home/dashboard")
@limiter.limit("60/minute")
async def home_dashboard(
    request: Request,
    force: bool = False,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """Normalized payload for the Artemis-inspired Home landing page."""
    await _wait_for_init()
    try:
        from services.home_service import build_home_dashboard
        payload = await build_home_dashboard(
            data_service=data_service,
            macro_provider=_get_macro_provider(),
            force=force,
        )
        return JSONResponse(content=payload)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(
            status_code=500,
            content={"error": f"Home dashboard error: {str(e)}"},
        )


# ── GET /api/home/movers ─────────────────────────────────────────────
# Category-aware Top Gainers / Top Losers for the Home dashboard toggle.
# category: stocks | etfs | commodities | crypto | all
# Each category is cached independently for 5 minutes.

@app.get("/api/home/movers")
@limiter.limit("120/minute")
async def home_movers(
    request: Request,
    category: str = "stocks",
    api_key: str = Header(None, alias="X-API-Key"),
):
    """
    Top Gainers and Losers for the Home category toggle.

    Sources by category:
      stocks      — FMP biggest-gainers/losers (same as existing Home movers)
      etfs        — FMP quotes on curated 30-ETF universe
      commodities — Hyperliquid perps filtered to commodity preset
      crypto      — CMC top-250 by market cap, ranked by 24h % change
      all         — parallel merge of all four, ranked globally by % move

    Normalized row shape (same across all categories):
      symbol, name, asset_type, price, change_percent, change_label,
      source, volume_24h, market_cap
    """
    await _wait_for_init()
    try:
        from services.home_service import get_movers_by_category
        payload = await get_movers_by_category(
            category=category,
            data_service=data_service,
        )
        return JSONResponse(content=payload)
    except Exception as exc:
        import traceback
        traceback.print_exc()
        return JSONResponse(
            status_code=500,
            content={"error": f"Home movers error: {str(exc)}"},
        )


# ── GET /api/home/daily-alpha-board ──────────────────────────────────
# Cache-only cross-market signal ranking engine.
# ZERO external provider/API calls — reads only from existing caches,
# disk snapshots, and Neon snapshot tables.

@app.get("/api/home/daily-alpha-board/diagnostics")
@limiter.limit("30/minute")
async def daily_alpha_board_diagnostics(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """
    Full diagnostics for the Daily Alpha Board:
    source availability, cache ages, candidate counts, top-20 pre-ranked ideas.
    external_api_calls is always 0.
    """
    try:
        from services.daily_alpha_board_service import build_diagnostics
        payload = build_diagnostics()
        return JSONResponse(content=payload)
    except Exception as exc:
        import traceback
        traceback.print_exc()
        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "error": f"Diagnostics error: {str(exc)}",
                "external_api_calls": 0,
            },
        )


@app.get("/api/home/daily-alpha-board")
@limiter.limit("60/minute")
async def daily_alpha_board(
    request: Request,
    limit: int = 10,
    asset_type: str = "all",
    scope: str = "all",
    refresh: bool = False,
    diagnostics: bool = False,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """
    Daily Alpha Board — top trade ideas ranked across stocks, ETFs, crypto,
    watchlist, portfolio, social, themes, catalysts, options, and Hyperliquid.

    ZERO external provider/API calls.  Reads only from existing in-memory
    caches, disk JSON snapshots, and Neon snapshot tables.

    Query params:
      limit      — number of ideas to return (default 10)
      asset_type — all | stocks | crypto
      scope      — all | watchlist | portfolio
      refresh    — force re-rank from cached sources (bypasses aggregator cache)
      diagnostics — include full diagnostics block in response
    """
    # Clamp limit to sane range
    limit = max(1, min(int(limit), 50))
    asset_type = asset_type.lower().strip()
    scope      = scope.lower().strip()
    if asset_type not in ("all", "stocks", "crypto"):
        asset_type = "all"
    if scope not in ("all", "watchlist", "portfolio"):
        scope = "all"

    try:
        from services.daily_alpha_board_service import build_daily_alpha_board_safe
        payload = build_daily_alpha_board_safe(
            limit=limit,
            asset_type=asset_type,
            scope=scope,
            refresh=refresh,
            include_diagnostics=diagnostics,
        )
        return JSONResponse(content=payload)
    except Exception as exc:
        import traceback
        traceback.print_exc()
        return JSONResponse(
            status_code=500,
            content={
                "ok":                 False,
                "error":              f"Daily alpha board error: {str(exc)}",
                "external_api_calls": 0,
                "mode":               "cache_only",
            },
        )


# ── GET /api/home/risk-intelligence ──────────────────────────────────
# Composer/aggregator: reuses existing cached services only.
# - market_snapshot  → macro:dashboard:v3  (same source as Home page)
# - trade_decision   → strategy:vix_regime:v1  (same engine as Macro "Should I Be Trading?")
# - upcoming_events  → calendar_snapshots/economic_releases Neon  (same as Calendar page)
# - risk_cluster     → deterministic rules on already-cached values
# - BTC              → Hyperliquid in-memory state (zero API call)
# ZERO new FMP/upstream calls.

@app.get("/api/home/risk-intelligence")
@limiter.limit("60/minute")
async def home_risk_intelligence(
    request: Request,
    force: bool = False,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """
    Home Risk Intelligence payload — single endpoint for the risk banner,
    trade decision bubble, market snapshot, and upcoming economic events.

    Validation checklist (all must hold):
      ✓ trade_decision matches /api/strategy/vix-risk-regime
      ✓ upcoming_economic_events match /api/catalysts/events?tab=economic_releases
      ✓ market_snapshot values match /api/home/dashboard cards
      ✓ Loading this endpoint does NOT increase FMP call count
      ✓ risk_cluster activates automatically on multi-signal risk days
    """
    await _wait_for_init()
    mp = _get_macro_provider()
    if force:
        from data.cache import cache as _cache
        _cache.delete("home:risk_intel:v1")
    try:
        from services.home_risk_intelligence import build_home_risk_intelligence_safe
        payload = await build_home_risk_intelligence_safe(mp)
        return JSONResponse(content=payload)
    except Exception as exc:
        import traceback
        traceback.print_exc()
        return JSONResponse(
            status_code=500,
            content={"error": f"Risk Intelligence error: {str(exc)}"},
        )


# ── GET /api/home/top-catalysts ──────────────────────────────────────
# Compact Top Catalysts feed for the Home page.
# Reuses Calendar Top Catalysts source data; applies macro grouping + limits.
# Zero new external API calls.

@app.get("/api/home/top-catalysts")
@limiter.limit("60/minute")
async def home_top_catalysts(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """
    Home compact Top Catalysts feed.

    Reuses the same underlying data as Calendar → Top Catalysts This Week
    (top_catalysts_service + calendar snapshot cache).

    Returns 6–8 cards max:
      • Macro events grouped by category (inflation / fed-rates / labor /
        growth / treasury / consumer / housing) — max 3 macro groups
      • Top earnings by options-flow + watchlist score — max 3 cards
      • High-signal IPOs / splits — max 2 cards

    Macro duplicates (CPI YoY, CPI MoM, Core CPI …) are consolidated into
    a single "Inflation Data" card with a subtitle listing the releases.

    Pure read across already-cached services. No FMP / Tradier / LLM calls.
    """
    await _wait_for_init()
    try:
        from services.home_top_catalysts import build_home_top_catalysts
        result = await build_home_top_catalysts()
        return JSONResponse(content=result)
    except Exception as exc:
        import traceback
        traceback.print_exc()
        return JSONResponse(
            status_code=500,
            content={
                "view":     "home_compact",
                "source":   "calendar_top_catalysts",
                "error":    str(exc),
                "catalysts": [],
            },
        )


# ── GET /api/macro/dashboard ─────────────────────────────────────────

@app.get("/api/macro/dashboard")
@limiter.limit("60/minute")
async def macro_dashboard(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """Full macro overview for the Macro Terminal frontend."""
    await _wait_for_init()

    mp = _get_macro_provider()
    if not mp:
        return JSONResponse(
            status_code=503,
            content={"error": "Macro data provider not configured. Ensure FRED_API_KEY is set."},
        )

    try:
        import time as _time
        t0 = _time.time()

        # Hybrid async: FMP real-time market data + FRED economic releases
        dashboard = await mp.get_dashboard()

        elapsed = _time.time() - t0

        # Transform to frontend-expected flat shape
        from data.macro_transforms import transform_dashboard
        transformed = transform_dashboard(dashboard)

        # Return both flat keys AND wrapped in "response" for compat
        return {
            **transformed,
            "response": transformed,
            "structured": True,
            "preset": "macro_dashboard",
            "from_cache": elapsed < 0.5,
            "timing": {"total_seconds": round(elapsed, 1)},
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": f"Macro dashboard error: {str(e)}"})


# ── GET /api/macro/indicators ────────────────────────────────────────

@app.get("/api/macro/indicators")
@limiter.limit("60/minute")
async def macro_indicators(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """Individual indicator cards with signal, source, and trend."""
    await _wait_for_init()

    mp = _get_macro_provider()
    if not mp:
        return JSONResponse(
            status_code=503,
            content={"error": "Macro data provider not configured."},
        )

    try:
        result = await asyncio.to_thread(mp.get_indicators)
        return {"response": result, "structured": True, "preset": "macro_indicators"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── GET /api/macro/calendar ──────────────────────────────────────────

@app.get("/api/macro/calendar")
@limiter.limit("60/minute")
async def macro_calendar(
    request: Request,
    days: int = 14,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """Upcoming economic events/releases."""
    await _wait_for_init()

    mp = _get_macro_provider()
    if not mp:
        return JSONResponse(
            status_code=503,
            content={"error": "Macro data provider not configured."},
        )

    try:
        result = await mp.get_calendar(days_ahead=min(days, 30))
        return {"response": result, "structured": True, "preset": "macro_calendar"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── GET /api/macro/history/{indicator} ───────────────────────────────

@app.get("/api/macro/history/{indicator}")
@limiter.limit("60/minute")
async def macro_history(
    request: Request,
    indicator: str,
    months: int = 12,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """Time-series data for charting a specific indicator."""
    await _wait_for_init()

    mp = _get_macro_provider()
    if not mp:
        return JSONResponse(
            status_code=503,
            content={"error": "Macro data provider not configured."},
        )

    try:
        result = await asyncio.to_thread(mp.get_history, indicator, min(months, 60))
        if "error" in result and "Unknown indicator" in result.get("error", ""):
            return JSONResponse(status_code=400, content=result)
        return {"response": result, "structured": True, "preset": "macro_history"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── GET /api/macro/rates ─────────────────────────────────────────

@app.get("/api/macro/rates")
@limiter.limit("60/minute")
async def macro_rates(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """RATES tab: yield curve, Fed policy, spreads, mortgage, credit."""
    await _wait_for_init()

    mp = _get_macro_provider()
    if not mp:
        return JSONResponse(
            status_code=503,
            content={"error": "Macro data provider not configured."},
        )

    try:
        result = await mp.get_rates()
        from data.macro_transforms import transform_rates
        transformed = transform_rates(result)
        return {**transformed, "response": transformed, "structured": True, "preset": "macro_rates"}
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── GET /api/macro/inflation ─────────────────────────────────────

@app.get("/api/macro/inflation")
@limiter.limit("60/minute")
async def macro_inflation(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """INFLATION tab: CPI, PCE, PPI, breakevens, alternative measures."""
    await _wait_for_init()

    mp = _get_macro_provider()
    if not mp:
        return JSONResponse(
            status_code=503,
            content={"error": "Macro data provider not configured."},
        )

    try:
        result = await mp.get_inflation()
        from data.macro_transforms import transform_inflation
        transformed = transform_inflation(result)
        return {**transformed, "response": transformed, "structured": True, "preset": "macro_inflation"}
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── GET /api/macro/growth ────────────────────────────────────────

@app.get("/api/macro/growth")
@limiter.limit("60/minute")
async def macro_growth(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """GROWTH tab: GDP, ISM, retail sales, industrial production, sentiment."""
    await _wait_for_init()

    mp = _get_macro_provider()
    if not mp:
        return JSONResponse(
            status_code=503,
            content={"error": "Macro data provider not configured."},
        )

    try:
        result = await mp.get_growth()
        from data.macro_transforms import transform_growth
        transformed = transform_growth(result)
        return {**transformed, "response": transformed, "structured": True, "preset": "macro_growth"}
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── GET /api/macro/labor ─────────────────────────────────────────

@app.get("/api/macro/labor")
@limiter.limit("60/minute")
async def macro_labor(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """LABOR tab: NFP, unemployment, claims, wages, JOLTS, participation."""
    await _wait_for_init()

    mp = _get_macro_provider()
    if not mp:
        return JSONResponse(
            status_code=503,
            content={"error": "Macro data provider not configured."},
        )

    try:
        result = await mp.get_labor()
        from data.macro_transforms import transform_labor
        transformed = transform_labor(result)
        return {**transformed, "response": transformed, "structured": True, "preset": "macro_labor"}
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── GET /api/macro/risk ──────────────────────────────────────────

@app.get("/api/macro/risk")
@limiter.limit("60/minute")
async def macro_risk(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """RISK tab: VIX, credit spreads, Fear & Greed, DXY, yield curve risk."""
    await _wait_for_init()

    mp = _get_macro_provider()
    if not mp:
        return JSONResponse(
            status_code=503,
            content={"error": "Macro data provider not configured."},
        )

    try:
        result = await mp.get_risk()
        from data.macro_transforms import transform_risk
        transformed = transform_risk(result)
        return {**transformed, "response": transformed, "structured": True, "preset": "macro_risk"}
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": str(e)})


# ═══════════════════════════════════════════════════════════════════════════════
# ── SHOULD I BE TRADING? DASHBOARD ─────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

_trading_dashboard_cache: dict = {}
_TRADING_DASHBOARD_TTL = 720  # 12 min — aligned with macro precompute loop interval

# Sector ETF mapping (FMP sector name → ticker + display name)
_SECTOR_ETF_MAP = {
    "Technology": ("XLK", "Technology"),
    "Healthcare": ("XLV", "Health Care"),
    "Financial Services": ("XLF", "Financials"),
    "Energy": ("XLE", "Energy"),
    "Industrials": ("XLI", "Industrials"),
    "Consumer Defensive": ("XLP", "Cons Staples"),
    "Consumer Cyclical": ("XLY", "Cons Disc"),
    "Basic Materials": ("XLB", "Materials"),
    "Utilities": ("XLU", "Utilities"),
    "Real Estate": ("XLRE", "Real Estate"),
    "Communication Services": ("XLC", "Comm Svcs"),
}


@app.get("/should-i-be-trading")
async def should_i_be_trading_page():
    """Serve the Bloomberg Terminal-style trading dashboard HTML."""
    html_path = Path(__file__).parent / "static" / "should-i-be-trading" / "index.html"
    if html_path.exists():
        return FileResponse(str(html_path), media_type="text/html")
    return HTMLResponse("<h1>Dashboard not found</h1>", status_code=404)


@app.get("/whale-watch")
async def whale_watch_page():
    """Serve the Whale Watch institutional 13F tracker page."""
    html_path = Path(__file__).parent / "static" / "whale-watch" / "index.html"
    if html_path.exists():
        return FileResponse(str(html_path), media_type="text/html")
    return HTMLResponse("<h1>Whale Watch not found</h1>", status_code=404)


@app.get("/options-flow")
async def options_flow_page():
    """Serve the Options Flow dashboard page."""
    html_path = Path(__file__).parent / "static" / "options-flow" / "index.html"
    if html_path.exists():
        return FileResponse(str(html_path), media_type="text/html")
    return HTMLResponse("<h1>Options Flow not found</h1>", status_code=404)


@app.get("/subscribe")
async def subscribe_page():
    """Serve the subscription / paywall page."""
    html_path = Path(__file__).parent / "static" / "subscribe" / "index.html"
    if html_path.exists():
        return FileResponse(str(html_path), media_type="text/html")
    return HTMLResponse("<h1>Coming Soon</h1>", status_code=404)


def _vix_score(vix: float | None) -> float:
    if vix is None:
        return 50.0
    if vix <= 13:
        return 95.0
    if vix <= 15:
        return 85.0
    if vix <= 18:
        return 72.0
    if vix <= 20:
        return 58.0
    if vix <= 25:
        return 40.0
    if vix <= 30:
        return 22.0
    return 8.0


def _fg_score(fg: float | None) -> float:
    """Fear & Greed (0-100) -> pillar score. Contrarian: greed = risky."""
    if fg is None:
        return 50.0
    if fg >= 75:
        return 35.0
    if fg >= 60:
        return 65.0
    if fg >= 45:
        return 75.0
    if fg >= 30:
        return 65.0
    if fg >= 20:
        return 40.0
    return 20.0


def _spread_score(spread: float | None) -> float:
    """2s10s yield spread -> score. Negative = inverted = bad."""
    if spread is None:
        return 50.0
    if spread > 0.5:
        return 85.0
    if spread > 0:
        return 70.0
    if spread > -0.5:
        return 55.0
    if spread > -1.0:
        return 38.0
    return 20.0


def _hy_oas_score(hy_oas: float | None) -> float:
    """HY credit spread (OAS in %). Lower = healthier."""
    if hy_oas is None:
        return 60.0
    if hy_oas < 2.5:
        return 90.0
    if hy_oas < 3.5:
        return 75.0
    if hy_oas < 4.5:
        return 55.0
    if hy_oas < 6.0:
        return 35.0
    return 15.0


def _pct_from_high_score(pct: float | None) -> float:
    """% from 52-week high (negative = below high)."""
    if pct is None:
        return 60.0
    if pct >= -2:
        return 90.0
    if pct >= -5:
        return 78.0
    if pct >= -10:
        return 60.0
    if pct >= -15:
        return 42.0
    if pct >= -20:
        return 25.0
    return 10.0


def _change_pct_score(chg: float | None) -> float:
    """Daily change % -> momentum component."""
    if chg is None:
        return 50.0
    if chg > 1.5:
        return 85.0
    if chg > 0.5:
        return 75.0
    if chg > 0:
        return 62.0
    if chg > -0.5:
        return 48.0
    if chg > -1.5:
        return 30.0
    return 12.0


def _compute_dashboard(
    mode: str,
    risk_data: dict,
    macro_data: dict,
    calendar_data: dict,
    sector_perf_raw: list | None = None,
    spy_qqq_extended: dict | None = None,
    vix_history: dict | None = None,
) -> dict:
    from datetime import datetime as _dt2, timezone as _tz2, timedelta as _td

    def _s(d, *keys, default=None):
        for k in keys:
            if not isinstance(d, dict):
                return default
            d = d.get(k, None)
            if d is None:
                return default
        return d

    # ── Raw value helpers ─────────────────────────────────────────────────────
    def _fg_comp(key):
        comps = _s(risk_data, "fear_greed", "components") or {}
        v = comps.get(key)
        if isinstance(v, dict):
            return v.get("score", 50)
        return 50

    def _fmt(v, suffix="", precision=2, signed=False):
        if v is None:
            return "N/A"
        fmt = f"{v:+.{precision}f}" if signed else f"{v:.{precision}f}"
        return f"{fmt}{suffix}"

    def _ma_signal(price, sma, threshold_pct=1.0):
        if price is None or sma is None or sma == 0:
            return None, "N/A"
        diff = ((price - sma) / sma) * 100
        if abs(diff) <= threshold_pct:
            return 0, f"At MA ({diff:+.1f}%)"
        elif diff > 0:
            return 1, f"Above ({diff:+.1f}%)"
        else:
            return -1, f"Below ({diff:+.1f}%)"

    def _next_event_label(ev_types: list[str], events: list) -> str:
        today = _dt2.utcnow().date()
        for ev in events:
            ev_name = (ev.get("event", "") or ev.get("name", "") or "").lower()
            if any(k in ev_name for k in ev_types):
                ev_date_str = ev.get("date", "")
                if not ev_date_str:
                    continue
                try:
                    ev_date = _dt2.strptime(ev_date_str[:10], "%Y-%m-%d").date()
                except Exception:
                    continue
                if ev_date < today:
                    continue
                delta = (ev_date - today).days
                if delta == 0:
                    return "TODAY"
                elif delta == 1:
                    return "Tomorrow"
                else:
                    return ev_date.strftime("%b %-d")
        return "N/A"

    # ── Core risk data ────────────────────────────────────────────────────────
    vix = _s(risk_data, "volatility", "vix")
    vix_change = _s(risk_data, "volatility", "vix_change")
    vix_signal = _s(risk_data, "volatility", "signal", default="normal")
    fg_score_raw = _s(risk_data, "fear_greed", "score")
    fg_rating = _s(risk_data, "fear_greed", "rating", default="Neutral")
    hy_oas = _s(risk_data, "credit_spreads", "hy_oas")
    bbb_oas = _s(risk_data, "credit_spreads", "bbb_oas")
    hy_signal = _s(risk_data, "credit_spreads", "hy_signal", default="normal")
    spread_2s10s = _s(risk_data, "yield_curve_risk", "spread_2s10s")
    curve_inverted = _s(risk_data, "yield_curve_risk", "inverted", default=False)
    dxy = _s(risk_data, "dollar", "dxy")
    dxy_chg = _s(risk_data, "dollar", "dxy_change_pct")

    benchmark_etfs = macro_data.get("benchmark_etfs", []) if isinstance(macro_data, dict) else []
    etf_map = {e.get("ticker"): e for e in benchmark_etfs if isinstance(e, dict)}
    spy_bench = etf_map.get("SPY", {})
    qqq_bench = etf_map.get("QQQ", {})
    spy_chg = spy_bench.get("change_pct") or spy_bench.get("change") or 0.0
    qqq_chg = qqq_bench.get("change_pct") or qqq_bench.get("change") or 0.0
    spy_from_high = spy_bench.get("pct_from_52w_high")
    qqq_from_high = qqq_bench.get("pct_from_52w_high")
    us10y = _s(macro_data, "rates_and_yields", "us_10y")

    # ── VIX enrichment ────────────────────────────────────────────────────────
    if vix_change is not None:
        vix_trend = "Rising" if vix_change > 0.3 else ("Falling" if vix_change < -0.3 else "Stable")
    else:
        vix_trend = "Stable"

    vix_pctile_str = "N/A"
    if vix_history and vix:
        try:
            hist_vals = [d.get("value") for d in (vix_history.get("data") or []) if d.get("value") is not None]
            if hist_vals:
                rank = sum(1 for v in hist_vals if v < vix) / len(hist_vals) * 100
                ordinal = "st" if int(rank) % 10 == 1 and int(rank) != 11 else \
                          "nd" if int(rank) % 10 == 2 and int(rank) != 12 else \
                          "rd" if int(rank) % 10 == 3 and int(rank) != 13 else "th"
                vix_pctile_str = f"{round(rank)}{ordinal} %ile"
        except Exception:
            pass

    # ── Put/Call ratio (approximated from Fear & Greed component) ─────────────
    pc_comp = _fg_comp("put_call_options")
    pc_ratio = round(max(0.5, min(2.0, 1.5 - (pc_comp / 100))), 2)
    pc_status = "Elevated" if pc_ratio > 1.15 else ("Low" if pc_ratio < 0.8 else "Neutral")

    # ── SPY/QQQ vs Moving Averages ────────────────────────────────────────────
    ext = spy_qqq_extended or {}
    spy_ext = ext.get("SPY", {})
    qqq_ext = ext.get("QQQ", {})

    spy_price = spy_bench.get("price") or spy_ext.get("price")
    spy_sma50 = spy_ext.get("priceAvg50")
    spy_sma200 = spy_ext.get("priceAvg200")
    qqq_price = qqq_bench.get("price") or qqq_ext.get("price")
    qqq_sma50 = qqq_ext.get("priceAvg50")
    qqq_sma200 = qqq_ext.get("priceAvg200")

    spy_vs50_dir, spy_vs50_str = _ma_signal(spy_price, spy_sma50)
    spy_vs200_dir, spy_vs200_str = _ma_signal(spy_price, spy_sma200)
    qqq_vs50_dir, qqq_vs50_str = _ma_signal(qqq_price, qqq_sma50)
    qqq_vs200_dir, qqq_vs200_str = _ma_signal(qqq_price, qqq_sma200)

    spx_regime = "Uptrend" if (spy_vs200_dir or 0) > 0 and (spy_vs50_dir or 0) > 0 else \
                 "Downtrend" if (spy_vs200_dir or 0) < 0 else "Mixed"

    # ── Sector performance ────────────────────────────────────────────────────
    sector_list = []
    for item in (sector_perf_raw or []):
        sector_name = item.get("sector", "")
        if sector_name in _SECTOR_ETF_MAP:
            ticker, display_name = _SECTOR_ETF_MAP[sector_name]
            chg = item.get("changesPercentage", 0)
            if isinstance(chg, str):
                try:
                    chg = float(chg.strip("%").strip())
                except Exception:
                    chg = 0.0
            sector_list.append({
                "ticker": ticker,
                "name": display_name,
                "change_pct": round(float(chg), 2),
            })
    sector_list.sort(key=lambda x: x["change_pct"], reverse=True)
    sectors_positive = sum(1 for s in sector_list if s["change_pct"] > 0)
    sectors_total = len(sector_list)
    sector_leader = sector_list[0] if sector_list else None
    sector_laggard = sector_list[-1] if sector_list else None
    participation_pct = round(sectors_positive / sectors_total * 100) if sectors_total else 0

    # ── Calendar: FOMC + CPI next dates ──────────────────────────────────────
    upcoming_events = calendar_data.get("events", []) if isinstance(calendar_data, dict) else []
    fomc_next = _next_event_label(["fomc", "federal funds", "interest rate decision"], upcoming_events)
    cpi_next = _next_event_label(["cpi", "consumer price"], upcoming_events)

    # ── Pillar score computation ───────────────────────────────────────────────
    vix_s = _vix_score(vix)
    hy_s = _hy_oas_score(hy_oas)
    fg_vol_s = _fg_score(fg_score_raw)
    p1_score = round(vix_s * 0.5 + hy_s * 0.3 + fg_vol_s * 0.2, 1)
    p1_dir = "up" if vix_s >= 70 else ("down" if vix_s < 40 else "sideways")

    spy_high_s = _pct_from_high_score(spy_from_high)
    qqq_high_s = _pct_from_high_score(qqq_from_high)
    spy_chg_s = _change_pct_score(spy_chg)
    ma_bonus = 10.0 if (spy_vs50_dir or 0) > 0 and (spy_vs200_dir or 0) > 0 else \
               -10.0 if (spy_vs200_dir or 0) < 0 else 0.0
    p2_score = round(min(100.0, max(0.0, spy_high_s * 0.35 + qqq_high_s * 0.30 + spy_chg_s * 0.20 + 50.0 * 0.15 + ma_bonus)), 1)
    p2_dir = "up" if spy_chg > 0.2 else ("down" if spy_chg < -0.2 else "sideways")

    breadth_fg = _fg_comp("stock_price_breadth")
    strength_fg = _fg_comp("stock_price_strength")
    safe_haven = _fg_comp("safe_haven_demand")
    participation_bonus = ((sectors_positive - sectors_total / 2) / max(sectors_total, 1)) * 20 if sectors_total else 0
    p3_score = round(min(100.0, max(0.0, breadth_fg * 0.35 + strength_fg * 0.35 + (100 - safe_haven) * 0.20 + 50 * 0.10 + participation_bonus)), 1)
    p3_dir = "up" if p3_score >= 60 else ("down" if p3_score < 40 else "sideways")

    spread_s = _spread_score(spread_2s10s)
    dxy_s = 70.0 if (dxy_chg or 0) < 0 else (55.0 if abs(dxy_chg or 0) < 0.3 else 40.0)
    bbb_s = _hy_oas_score((bbb_oas or 0) * 2 if bbb_oas else None)
    p4_score = round(spread_s * 0.4 + bbb_s * 0.3 + dxy_s * 0.3, 1)
    p4_dir = "up" if p4_score >= 60 else ("down" if p4_score < 40 else "sideways")

    momentum_fg = _fg_comp("market_momentum_sp500")
    junk_bond = _fg_comp("junk_bond_demand")
    put_call_fg = _fg_comp("put_call_options")
    p5_score = round(momentum_fg * 0.4 + junk_bond * 0.3 + put_call_fg * 0.3, 1)
    p5_dir = "up" if p5_score >= 60 else ("down" if p5_score < 40 else "sideways")

    weights = [0.30, 0.25, 0.20, 0.15, 0.10] if mode == "swing" else [0.25, 0.20, 0.20, 0.15, 0.20]
    pillar_scores = [p1_score, p2_score, p3_score, p4_score, p5_score]
    mqs = round(sum(s * w for s, w in zip(pillar_scores, weights)), 1)

    # ── EWS + calendar penalties ──────────────────────────────────────────────
    ews_penalty = 0
    alert_events = []
    for ev in upcoming_events[:20]:
        days_out = ev.get("days_out", 99)
        ev_type = (ev.get("event", "") or ev.get("name", "") or "").lower()
        if days_out is not None and days_out <= 1:
            if any(k in ev_type for k in ["fomc", "fed", "interest rate"]):
                ews_penalty += 15
                alert_events.append(f"FOMC/Fed Decision in {days_out}d — elevated volatility expected")
            elif "cpi" in ev_type or "inflation" in ev_type:
                ews_penalty += 10
                alert_events.append(f"CPI Release in {days_out}d — price action may spike")
            elif any(k in ev_type for k in ["nfp", "payroll", "jobs"]):
                ews_penalty += 10
                alert_events.append(f"Jobs Report in {days_out}d — directional risk elevated")

    _mqs_adj = max(0.0, round(mqs - ews_penalty, 1))
    decision = "YES" if _mqs_adj >= 70 else ("CAUTION" if _mqs_adj >= 40 else "NO")

    # ── Pillar metric blocks ──────────────────────────────────────────────────
    vix_str = _fmt(vix, precision=1)
    fg_str = f"{fg_score_raw:.0f} ({fg_rating})" if fg_score_raw else "N/A"
    hy_str = _fmt(hy_oas, suffix="%")
    spread_str = _fmt(spread_2s10s, suffix="%", signed=True)
    spy_str = _fmt(spy_chg, suffix="%", signed=True)
    qqq_str = _fmt(qqq_chg, suffix="%", signed=True)
    dxy_str = _fmt(dxy, precision=2)
    dxy_chg_str = _fmt(dxy_chg, suffix="%", signed=True)

    pillars = [
        {
            "title": "VOLATILITY / RISK",
            "score": p1_score,
            "weight": int(weights[0] * 100),
            "direction": p1_dir,
            "metrics": [
                {"label": "VIX Level", "value": vix_str, "status": vix_signal.replace("_", " ").title(),
                 "ok": (vix or 99) < 20},
                {"label": "VIX Trend", "value": vix_trend,
                 "status": "Improving" if vix_trend == "Falling" else ("Worsening" if vix_trend == "Rising" else "Neutral"),
                 "ok": vix_trend == "Falling"},
                {"label": "VIX 1Y %ile", "value": vix_pctile_str,
                 "status": "Elevated" if "7" in vix_pctile_str or "8" in vix_pctile_str or "9" in vix_pctile_str else "Normal",
                 "ok": vix_pctile_str not in ("N/A",) and not vix_pctile_str.startswith(("7", "8", "9"))},
                {"label": "Put/Call Ratio", "value": f"{pc_ratio:.2f}", "status": pc_status,
                 "ok": pc_ratio <= 1.0},
                {"label": "HY OAS", "value": hy_str,
                 "status": hy_signal.upper(),
                 "ok": hy_s >= 60},
                {"label": "Fear & Greed", "value": fg_str,
                 "status": fg_rating,
                 "ok": 30 <= (fg_score_raw or 50) <= 70},
            ],
        },
        {
            "title": "TREND & STRUCTURE",
            "score": p2_score,
            "weight": int(weights[1] * 100),
            "direction": p2_dir,
            "metrics": [
                {"label": "SPX vs 50d MA", "value": spy_vs50_str,
                 "status": "N/A" if spy_vs50_dir is None else ("Above" if spy_vs50_dir > 0 else ("At" if spy_vs50_dir == 0 else "Below")),
                 "ok": spy_vs50_dir is None or spy_vs50_dir >= 0},
                {"label": "SPX vs 200d MA", "value": spy_vs200_str,
                 "status": "N/A" if spy_vs200_dir is None else ("Above" if spy_vs200_dir > 0 else ("At" if spy_vs200_dir == 0 else "Below")),
                 "ok": spy_vs200_dir is None or spy_vs200_dir >= 0},
                {"label": "Market Regime", "value": spx_regime,
                 "status": spx_regime,
                 "ok": spx_regime == "Uptrend"},
                {"label": "QQQ vs 50d MA", "value": qqq_vs50_str,
                 "status": "N/A" if qqq_vs50_dir is None else ("Above" if qqq_vs50_dir > 0 else ("At" if qqq_vs50_dir == 0 else "Below")),
                 "ok": qqq_vs50_dir is None or qqq_vs50_dir >= 0},
                {"label": "QQQ vs 200d MA", "value": qqq_vs200_str,
                 "status": "N/A" if qqq_vs200_dir is None else ("Above" if qqq_vs200_dir > 0 else ("At" if qqq_vs200_dir == 0 else "Below")),
                 "ok": qqq_vs200_dir is None or qqq_vs200_dir >= 0},
                {"label": "SPY vs 52w High", "value": _fmt(spy_from_high, suffix="%", signed=True),
                 "status": "Healthy" if (spy_from_high or -100) >= -5 else "Extended",
                 "ok": (spy_from_high or -100) >= -5},
            ],
        },
        {
            "title": "MARKET BREADTH",
            "score": p3_score,
            "weight": int(weights[2] * 100),
            "direction": p3_dir,
            "metrics": [
                {"label": "Price Breadth", "value": f"{breadth_fg:.0f}/100",
                 "status": "Positive" if breadth_fg >= 60 else ("Neutral" if breadth_fg >= 40 else "Weak"),
                 "ok": breadth_fg >= 50},
                {"label": "Price Strength", "value": f"{strength_fg:.0f}/100",
                 "status": "Strong" if strength_fg >= 60 else ("Neutral" if strength_fg >= 40 else "Weak"),
                 "ok": strength_fg >= 50},
                {"label": "Sectors Positive", "value": f"{sectors_positive}/{sectors_total}",
                 "status": f"{participation_pct}% Participation",
                 "ok": participation_pct >= 55},
                {"label": "Participation", "value": f"{participation_pct}%",
                 "status": "Broad" if participation_pct >= 70 else ("Mixed" if participation_pct >= 40 else "Narrow"),
                 "ok": participation_pct >= 55},
                {"label": "Safe Haven Dem", "value": f"{safe_haven:.0f}/100",
                 "status": "Elevated" if safe_haven >= 60 else ("Low" if safe_haven < 40 else "Normal"),
                 "ok": safe_haven < 50},
                {"label": "HYG Signal", "value": hy_signal.upper(),
                 "status": hy_signal.title(),
                 "ok": hy_signal == "normal"},
            ],
        },
        {
            "title": "MACRO / LIQUIDITY",
            "score": p4_score,
            "weight": int(weights[3] * 100),
            "direction": p4_dir,
            "metrics": [
                {"label": "10Y Yield", "value": _fmt(us10y, suffix="%") if us10y else "N/A",
                 "status": "Elevated" if (us10y or 0) >= 4.8 else ("Rising" if (us10y or 0) >= 4.5 else "Moderate"),
                 "ok": bool(us10y and us10y < 4.8)},
                {"label": "DXY", "value": f"{dxy_str} ({dxy_chg_str})",
                 "status": "Falling" if (dxy_chg or 0) < -0.1 else ("Rising" if (dxy_chg or 0) > 0.1 else "Stable"),
                 "ok": (dxy_chg or 0) < 0.3},
                {"label": "2s10s Spread", "value": spread_str,
                 "status": "INVERTED" if curve_inverted else "NORMAL",
                 "ok": (spread_2s10s or -1) > -0.5},
                {"label": "Next FOMC", "value": fomc_next,
                 "status": "Today" if fomc_next == "TODAY" else ("Soon" if fomc_next not in ("N/A",) else "Distant"),
                 "ok": fomc_next not in ("TODAY", "Tomorrow")},
                {"label": "Fed Stance", "value": "Easing" if (us10y or 5) < 4.5 else "Restrictive",
                 "status": "Easing" if (us10y or 5) < 4.5 else "Restrictive",
                 "ok": bool(us10y and us10y < 4.8)},
                {"label": "Next CPI", "value": cpi_next,
                 "status": "Today" if cpi_next == "TODAY" else "Upcoming",
                 "ok": cpi_next not in ("TODAY", "Tomorrow")},
            ],
        },
        {
            "title": "MOMENTUM / SENTIMENT",
            "score": p5_score,
            "weight": int(weights[4] * 100),
            "direction": p5_dir,
            "metrics": [
                {"label": "Mkt Momentum", "value": f"{momentum_fg:.0f}/100",
                 "status": "Positive" if momentum_fg >= 55 else ("Neutral" if momentum_fg >= 45 else "Negative"),
                 "ok": momentum_fg >= 50},
                {"label": "Put/Call Ratio", "value": f"{pc_ratio:.2f}",
                 "status": pc_status,
                 "ok": pc_ratio <= 1.05},
                {"label": "Junk Bond Dem", "value": f"{junk_bond:.0f}/100",
                 "status": "High" if junk_bond >= 60 else ("Low" if junk_bond < 40 else "Normal"),
                 "ok": junk_bond >= 50},
                {"label": "Sector Leader", "value": sector_leader["ticker"] if sector_leader else "N/A",
                 "status": f"{sector_leader['change_pct']:+.2f}%" if sector_leader else "N/A",
                 "ok": bool(sector_leader and sector_leader["change_pct"] > 0)},
                {"label": "Sector Laggard", "value": sector_laggard["ticker"] if sector_laggard else "N/A",
                 "status": f"{sector_laggard['change_pct']:+.2f}%" if sector_laggard else "N/A",
                 "ok": bool(sector_laggard and sector_laggard["change_pct"] > -1.5)},
                {"label": "DXY Trend", "value": "FALLING" if (dxy_chg or 0) < 0 else "RISING",
                 "status": "Bullish" if (dxy_chg or 0) < 0 else "Headwind",
                 "ok": (dxy_chg or 0) < 0},
            ],
        },
    ]

    # ── Execution condition 1: Breakouts working? (price breadth proxy) ──────
    _breakout_ok = breadth_fg > 55
    _breakout_val = "Yes" if breadth_fg > 55 else ("Mixed" if breadth_fg >= 35 else "No")
    _breakout_status = "Working" if breadth_fg > 55 else ("Inconsistent" if breadth_fg >= 35 else "Failing")

    # ── Execution condition 2: Leaders holding? (XLK/XLY/XLC vs SPY today) ──
    _leader_tickers = {"XLK", "XLY", "XLC"}
    _leader_chgs = [s["change_pct"] for s in sector_list if s["ticker"] in _leader_tickers]
    _spy_chg_today = spy_bench.get("change_pct") or 0.0
    if _leader_chgs:
        _leaders_vs_spy = (sum(_leader_chgs) / len(_leader_chgs)) - _spy_chg_today
    else:
        _leaders_vs_spy = None
    if _leaders_vs_spy is None:
        _leaders_ok = False
        _leaders_val = "N/A"
        _leaders_status = "No data"
    elif _leaders_vs_spy > 0.5:
        _leaders_ok = True
        _leaders_val = "Yes"
        _leaders_status = "Holding"
    elif _leaders_vs_spy >= -1.5:
        _leaders_ok = False
        _leaders_val = "Mixed"
        _leaders_status = "Fading"
    else:
        _leaders_ok = False
        _leaders_val = "No"
        _leaders_status = "Breaking down"

    # ── Execution condition 3: Pullbacks bought? (SPY intraday recovery) ─────
    _spy_bars = (spy_ext.get("recent_bars") or [])[-5:]
    _recovered_days = 0
    for _b in _spy_bars:
        _h, _l, _c = _b.get("high"), _b.get("low"), _b.get("close")
        if _h and _l and _c and (_h - _l) > 0:
            _rec = (_c - _l) / (_h - _l)
            if _rec > 0.50:
                _recovered_days += 1
    if _spy_bars:
        _pullback_ok = _recovered_days >= 3
        _pullback_val = "Yes" if _recovered_days >= 3 else ("Mixed" if _recovered_days == 2 else "No")
        _pullback_status = "Support" if _recovered_days >= 3 else ("Inconsistent" if _recovered_days == 2 else "Selling into rallies")
    else:
        _pullback_ok = False
        _pullback_val = "N/A"
        _pullback_status = "No data"

    # ── Execution condition 4: Follow-through? (green day confirmation) ───────
    _spy_bars10 = (spy_ext.get("recent_bars") or [])[-10:]
    _ft_ok = False
    _ft_val = "N/A"
    _ft_status = "No data"
    if len(_spy_bars10) >= 4:
        _closes10 = [_b["close"] for _b in _spy_bars10 if _b.get("close")]
        _vols10 = [_b.get("volume") or 0 for _b in _spy_bars10]
        _green_days, _ft_days = [], []
        _up_vols, _dn_vols = [], []
        for _i in range(1, len(_closes10)):
            _is_green = _closes10[_i] > _closes10[_i - 1]
            if _is_green:
                _green_days.append(_i)
                _up_vols.append(_vols10[_i])
                if _i + 1 < len(_closes10) and _closes10[_i + 1] > _closes10[_i]:
                    _ft_days.append(_i)
            else:
                _dn_vols.append(_vols10[_i])
        _ft_rate = len(_ft_days) / len(_green_days) if _green_days else 0
        _avg_up_vol = sum(_up_vols) / len(_up_vols) if _up_vols else 0
        _avg_dn_vol = sum(_dn_vols) / len(_dn_vols) if _dn_vols else 1
        _vol_ratio = _avg_up_vol / _avg_dn_vol if _avg_dn_vol else 1
        if _ft_rate > 0.5 and _vol_ratio > 1.2:
            _ft_ok = True
            _ft_val = "Strong"
            _ft_status = "Confirming"
        elif _ft_rate > 0.5:
            _ft_ok = False
            _ft_val = "Weak"
            _ft_status = "Low conviction"
        else:
            _ft_ok = False
            _ft_val = "No"
            _ft_status = "Reversing"

    exec_conditions = [
        {"label": "Breakouts working?", "value": _breakout_val, "status": _breakout_status, "ok": _breakout_ok},
        {"label": "Leaders holding?", "value": _leaders_val, "status": _leaders_status, "ok": _leaders_ok},
        {"label": "Pullbacks bought?", "value": _pullback_val, "status": _pullback_status, "ok": _pullback_ok},
        {"label": "Follow-through?", "value": _ft_val, "status": _ft_status, "ok": _ft_ok},
    ]

    ews = float(sum(25 for c in exec_conditions if c["ok"]))

    decision_text = {
        "YES": "Market conditions are favorable. Volatility is controlled, trend is intact, and breadth supports participation. Trade your plan with normal position sizing.",
        "CAUTION": "Mixed signals across pillars. Consider reducing position size by 30-50%, tightening stops, and avoiding aggressive new entries until conditions clarify.",
        "NO": "Risk environment is elevated. High VIX, poor breadth, or a major macro event is pending. Stay flat or reduce/hedge existing positions.",
    }[decision]

    terminal = [
        {"type": "dim", "text": f"$ caelyn --mode={mode} --analyze --pillars=5"},
        {"type": "dim", "text": ""},
        {"type": "green" if decision == "YES" else ("yellow" if decision == "CAUTION" else "red"),
         "text": f"DECISION: {decision}"},
        {"type": "dim", "text": decision_text},
        {"type": "dim", "text": ""},
        {"type": "blue", "text": f"[VOLATILITY/RISK]    {p1_score:.0f}/100 | VIX: {vix_str} ({vix_trend}) | 1Y%ile: {vix_pctile_str} | P/C: {pc_ratio:.2f} | HY OAS: {hy_str}"},
        {"type": "blue", "text": f"[TREND/STRUCTURE]    {p2_score:.0f}/100 | SPX vs 50d: {spy_vs50_str} | vs 200d: {spy_vs200_str} | Regime: {spx_regime}"},
        {"type": "blue", "text": f"[MARKET BREADTH]     {p3_score:.0f}/100 | {sectors_positive}/{sectors_total} sectors positive | Breadth: {breadth_fg:.0f} | Strength: {strength_fg:.0f}"},
        {"type": "blue", "text": f"[MACRO/LIQUIDITY]    {p4_score:.0f}/100 | 10Y: {_fmt(us10y, suffix='%') if us10y else 'N/A'} | DXY: {dxy_str} | 2s10s: {spread_str} | FOMC: {fomc_next}"},
        {"type": "blue", "text": f"[MOMENTUM/SENT]      {p5_score:.0f}/100 | Momentum: {momentum_fg:.0f} | Leader: {sector_leader['ticker']} ({sector_leader['change_pct']:+.2f}%)" if sector_leader else f"[MOMENTUM/SENT]      {p5_score:.0f}/100 | Momentum: {momentum_fg:.0f}"},
        {"type": "dim", "text": ""},
        {"type": "green" if mqs >= 70 else ("yellow" if mqs >= 40 else "red"),
         "text": f"Market Quality Score (MQS): {mqs:.0f}/100"},
        {"type": "green" if ews >= 70 else ("yellow" if ews >= 40 else "red"),
         "text": f"Execution Window Score (EWS): {ews:.0f}/100" + (f"  [event penalty: -{ews_penalty}]" if ews_penalty else "")},
    ]
    if alert_events:
        terminal.append({"type": "yellow", "text": f"ALERT: {alert_events[0]}"})

    return {
        "decision": decision,
        "market_quality_score": mqs,
        "execution_window_score": ews,
        "mode": mode,
        "pillars": pillars,
        "summary": decision_text,
        "execution_conditions": exec_conditions,
        "terminal_analysis": terminal,
        "alert": {
            "show": bool(alert_events),
            "text": alert_events[0] if alert_events else "",
        },
        "sector_performance": sector_list,
        "as_of": _dt2.now(_tz2.utc).isoformat(),
        "from_cache": False,
    }


@app.get("/api/trading-dashboard")
@limiter.limit("30/minute")
async def trading_dashboard(
    request: Request,
    mode: str = "swing",
    force: bool = False,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """Should I Be Trading? — 5-pillar market scoring dashboard."""
    await _wait_for_init()

    import time as _t

    mode = mode.lower() if mode.lower() in ("swing", "day") else "swing"
    cache_key = f"trading_dashboard_{mode}"

    if not force:
        cached = _trading_dashboard_cache.get(cache_key)
        if cached and (_t.time() - cached.get("_ts", 0)) < _TRADING_DASHBOARD_TTL:
            result = {**cached, "from_cache": True}
            result.pop("_ts", None)
            return JSONResponse(content=result)

    mp = _get_macro_provider()
    if not mp:
        return JSONResponse(
            status_code=503,
            content={"error": "Macro provider not initialized. Ensure FRED_API_KEY is set."},
        )

    async def _fetch_spy_qqq_extended():
        if not mp.tradier:
            return {}
        try:
            from datetime import date, timedelta
            start = (date.today() - timedelta(days=320)).isoformat()
            end = date.today().isoformat()
            spy_hist, qqq_hist = await asyncio.gather(
                mp.tradier.get_history("SPY", interval="daily", start=start, end=end),
                mp.tradier.get_history("QQQ", interval="daily", start=start, end=end),
                return_exceptions=True,
            )
            result_ext = {}
            for sym, hist in [("SPY", spy_hist), ("QQQ", qqq_hist)]:
                if isinstance(hist, Exception) or not hist:
                    continue
                bars = [d for d in hist if d.get("close")]
                closes = [d["close"] for d in bars]
                if len(closes) >= 50:
                    sma50 = round(sum(closes[-50:]) / 50, 2)
                    sma200 = round(sum(closes[-200:]) / 200, 2) if len(closes) >= 200 else None
                    result_ext[sym] = {
                        "price": closes[-1] if closes else None,
                        "priceAvg50": sma50,
                        "priceAvg200": sma200,
                        "recent_bars": bars[-10:],
                    }
            return result_ext
        except Exception:
            return {}

    async def _fetch_sector_perf():
        _SECTOR_ETFS = ["XLK", "XLV", "XLF", "XLE", "XLI", "XLP", "XLY", "XLB", "XLU", "XLRE", "XLC"]
        if mp.tradier:
            try:
                from data.tradier_budget import lane as _sec_lane
                with _sec_lane("quotes"):
                    quotes = await mp.tradier.get_quotes(_SECTOR_ETFS)
                result = []
                ETF_TO_SECTOR = {
                    "XLK": "Technology", "XLV": "Healthcare", "XLF": "Financial Services",
                    "XLE": "Energy", "XLI": "Industrials", "XLP": "Consumer Defensive",
                    "XLY": "Consumer Cyclical", "XLB": "Basic Materials", "XLU": "Utilities",
                    "XLRE": "Real Estate", "XLC": "Communication Services",
                }
                for q in quotes:
                    sym = q.get("symbol", "")
                    if sym in ETF_TO_SECTOR:
                        result.append({
                            "sector": ETF_TO_SECTOR[sym],
                            "changesPercentage": q.get("change_percentage", 0) or 0,
                        })
                return result
            except Exception:
                pass
        if mp.fmp:
            try:
                return await mp.fmp.get_sector_performance() or []
            except Exception:
                pass
        return []

    def _fetch_vix_history():
        try:
            return mp.get_history("vix", 12)
        except Exception:
            return {}

    try:
        risk_data, macro_data, calendar_data, sector_perf_raw, spy_qqq_extended, vix_history = await asyncio.gather(
            mp.get_risk(),
            mp.get_dashboard(),
            mp.get_calendar(days_ahead=14),
            _fetch_sector_perf(),
            _fetch_spy_qqq_extended(),
            asyncio.to_thread(_fetch_vix_history),
            return_exceptions=True,
        )
        if isinstance(risk_data, Exception):
            risk_data = {}
        if isinstance(macro_data, Exception):
            macro_data = {}
        if isinstance(calendar_data, Exception):
            calendar_data = {}
        if isinstance(sector_perf_raw, Exception):
            sector_perf_raw = []
        if isinstance(spy_qqq_extended, Exception):
            spy_qqq_extended = {}
        if isinstance(vix_history, Exception):
            vix_history = {}

        result = _compute_dashboard(
            mode, risk_data, macro_data, calendar_data,
            sector_perf_raw=sector_perf_raw,
            spy_qqq_extended=spy_qqq_extended,
            vix_history=vix_history,
        )
        _trading_dashboard_cache[cache_key] = {**result, "_ts": _t.time()}
        return JSONResponse(content=result)

    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(
            status_code=500,
            content={"error": f"Trading dashboard error: {str(e)}"},
        )


@app.post("/api/trading-dashboard/refresh")
@limiter.limit("10/minute")
async def trading_dashboard_refresh(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """Force-clear the trading dashboard cache."""
    cleared = [k for k in list(_trading_dashboard_cache.keys()) if k.startswith("trading_dashboard_")]
    for k in cleared:
        del _trading_dashboard_cache[k]
    return JSONResponse(content={"status": "ok", "cleared": cleared, "message": "Cache cleared — next request fetches fresh data"})


# ═══════════════════════════════════════════════════════════════════════════════
#  STRATEGY PAGE — Tab #2: Smart Options
#  Completely isolated from Bottleneck / Chain-Reaction / Serenity logic.
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/strategy/smart-options")
@limiter.limit("30/minute")
async def strategy_smart_options(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """
    Smart Options strategy tab.

    Compares Hyperliquid 24/7 perpetual prices against real market prices
    (Tradier) for all equity perps tracked on HL.  The gap between the two
    prices indicates where the real stock is likely to open — useful for
    placing options before market open on Monday mornings or after major
    after-hours moves.

    Response shape
    --------------
    {
      "market": {
        "status": "weekend|pre_market|open|after_hours|overnight",
        "context": "<human-readable explanation>",
        "gap_meaningful": true|false,
        "et_time": "2026-05-18 19:42 ET"
      },
      "data_source": "...",
      "total_hl_equities": 44,
      "with_gap": 7,
      "rows": [
        {
          "ticker": "MU",
          "signal": "call|put|neutral|no_data",
          "signal_strength": "strong|moderate|weak|neutral|null",
          "hl": {
            "price": 734.0,
            "oracle_px": 733.5,
            "chg_24h_pct": 2.95,
            "funding_rate_hourly": 0.003,
            "funding_rate_ann": 26.28,
            "oi_usd": 12500000,
            "oi_contracts": 17040.5,
            "volume_24h_usd": 5200000
          },
          "actual": {
            "price": 713.0,
            "close": 713.0,
            "prevclose": 710.5,
            "bid": null,
            "ask": null,
            "volume": 18500000,
            "change_pct": -0.42,
            "options_oi": 125000
          },
          "gap": {
            "abs": 21.0,
            "pct": 2.946,
            "direction": "hl_premium|hl_discount|aligned"
          }
        }
      ]
    }

    Rows are sorted by absolute gap % descending (biggest opportunities first).
    Rows without an actual price (markets closed and no cached close) appear last.
    """
    from services.smart_options_service import build_smart_options_data

    if not _hl_state:
        return JSONResponse(
            status_code=503,
            content={"error": "Hyperliquid state not initialised — try again in ~30 seconds."},
        )

    tradier = getattr(data_service, "tradier", None) if data_service else None

    # Pull actual options OI from the master screener LKG (in-memory or disk)
    from data.cache import cache as _cache
    options_lkg = _cache.get(_OPTIONS_MASTER_LKG_KEY) or _cache.get(_OPTIONS_MASTER_CACHE_KEY)
    if not options_lkg:
        try:
            import json as _json
            _disk = _master_lkg_disk_path()
            if _disk.exists():
                options_lkg = _json.loads(_disk.read_text())
        except Exception:
            pass

    result = await build_smart_options_data(_hl_state, tradier, options_lkg)
    return JSONResponse(content=result)


# ── GET /api/strategy/vix-risk-regime ───────────────────────────────────────

@app.get("/api/strategy/vix-risk-regime")
@limiter.limit("30/minute")
async def strategy_vix_risk_regime(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """
    VIX Risk Regime tab payload.

    Reuses the existing macro:dashboard:v3 cache (no new provider calls) for
    current VIX, SPX proxy (SPY), 10Y yield, DXY.  Historical VIX comes from
    FRED VIXCLS (4-h cache); historical SPX from yfinance ^GSPC (4-h cache).

    Response includes:
      current_market_snapshot  — live values from Home Market Snapshot cache
      vix_regime_signal        — zone/warning/signal_title/summary
      vix_spx_correlation      — rolling 7d/30d/63d Pearson correlation
      historical_windows       — 7d / quarter / 1y / 5y summaries
    """
    from services.strategy_macro_service import build_vix_regime_payload
    mp = _get_macro_provider()
    if not mp:
        return JSONResponse(
            status_code=503,
            content={"error": "Macro provider not ready — retry in ~30 s"},
        )
    try:
        result = await build_vix_regime_payload(mp)
        return JSONResponse(content=result)
    except Exception as exc:
        import traceback; traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": str(exc)})


# ── GET /api/strategy/weekly-price-movements ────────────────────────────────

@app.get("/api/strategy/weekly-price-movements")
@limiter.limit("30/minute")
async def strategy_weekly_price_movements(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """
    Weekly Price Movements scorecard tab payload.

    Deterministic Python — no AI.  Uses yfinance ^GSPC daily history (4-h
    cache, ~5 years).  Computes four scenarios across 5y / 1y / quarter / 7d
    windows:

      red_friday_to_monday   — Friday closes red → Monday outcome
      green_friday_to_monday — Friday closes green → Monday outcome
      red_monday_to_friday   — Monday closes red → rest-of-week outcome
      green_monday_to_friday — Monday closes green → rest-of-week outcome

    Each scenario includes: sample_count, green/red probabilities,
    avg/median/best/worst return %, confidence_label, insufficient_sample flag.
    """
    from services.strategy_macro_service import build_weekly_price_movements_payload
    try:
        result = await build_weekly_price_movements_payload()
        return JSONResponse(content=result)
    except Exception as exc:
        import traceback; traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": str(exc)})


# ── GET /api/strategy/ten-year-spx ──────────────────────────────────────────

@app.get("/api/strategy/ten-year-spx")
@limiter.limit("30/minute")
async def strategy_ten_year_spx(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """
    10Y Yield vs S&P 500 tab payload.

    Reuses the existing macro:dashboard:v3 cache for current values.
    10Y yield history from FRED DGS10 (4-h cache); SPX history from
    yfinance ^GSPC (4-h cache).

    Correlation basis: US 10Y daily bps change vs S&P 500 daily % return.

    Response includes:
      current_market_snapshot  — live values from Home Market Snapshot cache
      ten_year_spx_tracker     — side-by-side current + 7d changes
      rolling_correlation      — 7d / 30d / 63d Pearson correlation
      regime_labels            — yields_rising_spx_rising / _falling / mixed_flat
      historical_windows       — 7d / quarter / 1y / 5y summaries
    """
    from services.strategy_macro_service import build_ten_year_spx_payload
    mp = _get_macro_provider()
    if not mp:
        return JSONResponse(
            status_code=503,
            content={"error": "Macro provider not ready — retry in ~30 s"},
        )
    try:
        result = await build_ten_year_spx_payload(mp)
        return JSONResponse(content=result)
    except Exception as exc:
        import traceback; traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": str(exc)})


# ── GET /api/strategy/defiance ───────────────────────────────────────────────

@app.get("/api/strategy/defiance")
@limiter.limit("30/minute")
async def strategy_defiance(
    request: Request,
    force_refresh: bool = False,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """
    Defiance Daily Target 2X Long single-stock ETF tab — Strategy page.

    Contract
    --------
    - `symbol` is always the underlying stock ticker (e.g. AMAT, AMKR).
    - `defiance_etf.symbol` is the Defiance ETF ticker (e.g. AMA, AMKL) — metadata only.
    - `chart_symbol` and `quote_source_symbol` always equal `symbol` (the underlying).
    - The Defiance ETF ticker is NEVER used for price, market cap, VolX, or chart data.
    - `is_already_tracked` / `quote_reused` = underlying was already in canonical
      quote cache; no new Tradier call was needed.
    - Any ETF whose underlying cannot be parsed (or underlying == ETF ticker) is
      quarantined and excluded from this response.

    Source: https://www.defianceetfs.com/wp-json/defiance/v1/etfs-explore
    Refreshed daily off-hours; served from cache on normal user requests.
    """
    import os as _os
    from services.defiance_leveraged_etfs_service import (
        get_catalog         as _d2x_catalog,
        refresh_catalog     as _d2x_refresh,
        get_last_refresh_ts as _d2x_ts,
        get_quarantined     as _d2x_quarantined,
    )
    from services.watchlist_quote_cache import (
        get_watchlist_quotes as _get_quotes,
        _quote_cache         as _qc,
    )
    from services.theme_ticker_mapper import map_ticker_to_primary_theme
    from services.watchlist_router import (
        _build_defiance_rows,
        _vol_mc_fields,
        _get_stage2_breakout,
    )

    # ── Admin force-refresh ───────────────────────────────────────────────────
    if force_refresh:
        _ak = _os.getenv("AGENT_API_KEY", "")
        _auth = request.headers.get("Authorization", "")
        if _ak and _auth != f"Bearer {_ak}":
            return JSONResponse(status_code=403, content={"error": "Admin only"})
        result = await _d2x_refresh(force=True)
        print(f"[DEFIANCE_2X] Admin force-refresh via /api/strategy/defiance: {result}")

    catalog = _d2x_catalog()
    quarantined = _d2x_quarantined()

    if not catalog:
        import asyncio as _aio
        _aio.create_task(_d2x_refresh())
        return JSONResponse(content={
            "tab":              "defiance",
            "title":            "Defiance 2× Long ETFs",
            "rows":             [],
            "count":            0,
            "updated_at":       None,
            "status":           "warming_up",
            "unmapped_count":   len(quarantined),
            "unmapped_etfs":    [q["defiance_etf_ticker"] for q in quarantined],
        })

    # Detailed pre-enrichment logging
    unmapped_count = len(quarantined)
    if unmapped_count:
        print(
            f"[DEFIANCE_2X] quarantined_count={unmapped_count}  "
            f"unmapped_etfs={[q['defiance_etf_ticker'] for q in quarantined]}"
        )

    response = await _build_defiance_rows(
        catalog      = catalog,
        get_quotes   = _get_quotes,
        quote_cache  = _qc,
        get_theme    = map_ticker_to_primary_theme,
        get_ts       = _d2x_ts,
        tab_key      = "defiance",
    )

    # Append quarantine metadata
    response["unmapped_count"] = unmapped_count
    response["unmapped_etfs"]  = [q["defiance_etf_ticker"] for q in quarantined]

    return JSONResponse(content=response)


# ═══════════════════════════════════════════════════════════════════════════════
# Alert Signal Bus Endpoints
# Order matters: literal-path routes must precede /{alert_id} param routes.
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/alerts/stream")
async def alerts_stream(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """
    SSE stream — emits newly-created alert events after the client connects.

    Event types:
      event: alert      — new alert payload (only fired after connection time)
      event: keepalive  — sent every 20 s; data contains server timestamp only

    Does NOT replay existing DB rows on reconnect.  If the frontend needs
    previously-created alerts, it should call GET /api/alerts/recent with a
    `since` timestamp.
    """
    import json as _j
    from datetime import datetime, timezone as _tz
    from services.alert_signal_bus import subscribe_alerts as _sub, unsubscribe_alerts as _unsub

    user_id = "default"
    q = await _sub(user_id)

    async def _event_gen():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(q.get(), timeout=20.0)
                    payload = _j.dumps(event, default=str)
                    yield f"event: alert\ndata: {payload}\n\n"
                except asyncio.TimeoutError:
                    ts = datetime.now(tz=_tz.utc).isoformat(timespec="seconds")
                    yield f"event: keepalive\ndata: {_j.dumps({'ts': ts})}\n\n"
        finally:
            await _unsub(user_id, q)

    from fastapi.responses import StreamingResponse as _SR
    return _SR(
        _event_gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/api/alerts/diagnostics")
async def alerts_diagnostics(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """
    Diagnostic view for the Alert Signal Bus.

    Returns in-memory bus state plus DB-sourced 7-day history counts:
      - snapshots_by_source, alerts_by_lane, suppressed candidates
      - active cooldowns with remaining time and last severity
      - last SSE emit (alert_id + timestamp)
      - provider_calls (always 0)
      - history_count_7d, history_count_24h, dismissed_count_7d, acknowledged_count_7d
    """
    from services.alert_signal_bus import get_diagnostics as _gd, get_history_counts as _ghc
    diag, counts = await asyncio.gather(_ghc("default"), asyncio.to_thread(_gd))
    return {**counts, **diag}


@app.get("/api/alerts/recent")
@limiter.limit("60/minute")
async def alerts_recent(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
    limit: int = 25,
    since: str = None,
    include_acknowledged: bool = True,
    include_dismissed: bool = False,
    popup_only: bool = False,
):
    """
    Return recent alert events for the current user.

    Query params:
      limit                  Max results (1-100, default 25).
      since                  ISO 8601 timestamp — only return alerts created after this time.
                             Use the `created_at` of the last known alert to poll for new ones.
      include_acknowledged   Include alerts that have been acked (default true).
      include_dismissed      Include dismissed alerts (default false).
      popup_only             Shortcut: excludes both dismissed AND acknowledged alerts.
                             Ideal for toast/popup candidates.

    Alert payload always includes:
      id, ticker, alert_type, alert_lane, short_label, title, severity,
      coverage_label, score, summary, reasons, source_tags,
      created_at, acknowledged_at, dismissed_at, is_acknowledged, is_dismissed
    """
    from services.alert_signal_bus import get_recent_alerts as _gra
    user_id = "default"
    limit   = max(1, min(limit, 100))
    # popup_only is a convenience shortcut
    if popup_only:
        include_dismissed    = False
        include_acknowledged = False
    alerts = await _gra(
        user_id, limit,
        since=since,
        include_acknowledged=include_acknowledged,
        include_dismissed=include_dismissed,
    )
    return {"alerts": alerts, "count": len(alerts), "user_id": user_id}


@app.get("/api/alerts/history")
@limiter.limit("60/minute")
async def alerts_history(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
    days: int = 7,
    limit: int = 100,
    offset: int = 0,
    ticker: str = None,
    alert_lane: str = None,
    severity: str = None,
    include_acknowledged: bool = True,
    include_dismissed: bool = True,
):
    """
    7-day (default) alert history for the current user, read from ticker_alert_events.

    Query params:
      days                 Look-back window in days (1-30, default 7).
      limit                Max results per page (1-500, default 100).
      offset               Pagination offset (default 0).
      ticker               Filter to a single ticker (case-insensitive).
      alert_lane           Filter by alert_lane value.
      severity             Filter by severity (medium / high / critical).
      include_acknowledged Include acked alerts (default true).
      include_dismissed    Include dismissed alerts (default true).

    Both acknowledged and dismissed alerts are included by default (unlike
    /api/alerts/recent which excludes dismissed by default).

    Response:
      items     — list of alert rows (no chart/news data — use /{id}/detail for that)
      limit     — effective limit used
      offset    — effective offset used
      days      — effective look-back days used
      has_more  — true when there may be additional pages

    Does not trigger any provider calls.  Does not affect SSE, cooldowns, or deduplication.
    """
    from services.alert_signal_bus import get_alert_history as _gah
    user_id = "default"
    days    = max(1, min(days, 30))
    limit   = max(1, min(limit, 500))
    offset  = max(0, offset)

    items = await _gah(
        user_id,
        days=days,
        limit=limit,
        offset=offset,
        ticker=ticker,
        alert_lane=alert_lane,
        severity=severity,
        include_acknowledged=include_acknowledged,
        include_dismissed=include_dismissed,
    )
    return {
        "items":    items,
        "limit":    limit,
        "offset":   offset,
        "days":     days,
        "has_more": len(items) == limit,
    }


@app.get("/api/alerts/{alert_id}/detail")
@limiter.limit("60/minute")
async def alert_detail(
    request: Request,
    alert_id: int,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """
    Return full alert detail with reasons, source tags, coverage label, and
    pointers to cached chart/news paths.

    No new provider calls are made here — chart and news data is served from
    existing cached endpoints.  An explanatory note is returned for options-only
    tickers that lack full VolX / Vol-MC tracking.
    """
    from services.alert_signal_bus import get_alert_by_id as _gabi
    record = await _gabi(alert_id)
    if record is None:
        return JSONResponse(status_code=404, content={"error": "alert_not_found"})

    ticker = record.get("ticker", "")
    coverage = record.get("coverage_label", "")
    chart_note = None
    if "Options-only" in coverage:
        chart_note = (
            "This alert is based on options activity from the Options Flow / Home scan. "
            "Full VolX and Vol/MC tracking is only available for Watchlist and Portfolio tickers."
        )

    return {
        **record,
        "chart_note":       chart_note,
        "chart_cache_path": f"/api/chart/{ticker}" if ticker else None,
        "news_cache_path":  f"/api/watchlist/news?tickers={ticker}" if ticker else None,
    }


@app.get("/api/alerts/{alert_id}")
@limiter.limit("60/minute")
async def alert_get(
    request: Request,
    alert_id: int,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """Return alert metadata by ID."""
    from services.alert_signal_bus import get_alert_by_id as _gabi
    record = await _gabi(alert_id)
    if record is None:
        return JSONResponse(status_code=404, content={"error": "alert_not_found"})
    return record


@app.post("/api/alerts/{alert_id}/ack")
@limiter.limit("60/minute")
async def alert_ack(
    request: Request,
    alert_id: int,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """Mark an alert as acknowledged (read)."""
    from services.alert_signal_bus import ack_alert as _aa
    user_id = "default"
    ok = await _aa(alert_id, user_id)
    return {"ok": ok, "alert_id": alert_id}


@app.post("/api/alerts/{alert_id}/dismiss")
@limiter.limit("60/minute")
async def alert_dismiss(
    request: Request,
    alert_id: int,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """Dismiss an alert so it no longer appears in /api/alerts/recent."""
    from services.alert_signal_bus import dismiss_alert as _da
    user_id = "default"
    ok = await _da(alert_id, user_id)
    return {"ok": ok, "alert_id": alert_id}


# ── Admin: Watchlist Stage2 force recovery ───────────────────────────────────

@app.post("/api/admin/stage2/force-warmup")
async def admin_stage2_force_warmup(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """
    Force-recompute Weinstein stage for all watchlist tickers that currently
    have null/failed stage results, bypassing the normal freshness gate.

    Entries with valid stage labels that are still within their TTL are skipped.
    Entries with label=None or score=None are recomputed unconditionally.

    Fires as a background task and returns immediately.
    Poll GET /api/admin/stage2/status to watch progress.

    Uses the slow historical bar path (Tradier /markets/history, 4 concurrent).
    Does NOT touch the realtime/options pacer or options-chain allocation.
    """
    import asyncio as _asyncio
    from services.watchlist_stage2_service import force_warmup_stage2_nulls as _force

    null_count = sum(
        1 for v in __import__("services.watchlist_stage2_service", fromlist=["_STAGE2_LKG"])._STAGE2_LKG.values()
        if v.get("label") is None
    )
    print(f"[ADMIN] stage2 force-warmup triggered: {null_count} null entries will be recomputed")
    _asyncio.create_task(_force())
    return {
        "status": "started",
        "message": f"Force warmup running in background for {null_count} null entries.",
        "poll": "GET /api/admin/stage2/status",
    }


@app.get("/api/admin/stage2/status")
async def admin_stage2_status(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """
    Return current stage LKG summary: total symbols, valid/null counts,
    status breakdown, and freshness info.
    """
    from services import watchlist_stage2_service as _s2
    from datetime import datetime, timezone as _tz

    lkg = _s2._STAGE2_LKG
    total = len(lkg)
    valid = sum(1 for v in lkg.values() if v.get("label") is not None and v.get("score") is not None)
    null  = total - valid

    status_counts: dict[str, int] = {}
    fresh_counts:  dict[str, int] = {"fresh": 0, "stale": 0}
    for sym, entry in lkg.items():
        st = entry.get("status") or "legacy"
        status_counts[st] = status_counts.get(st, 0) + 1
        if _s2._is_fresh(sym):
            fresh_counts["fresh"] += 1
        else:
            fresh_counts["stale"] += 1

    samples: list[dict] = []
    for sym, entry in list(lkg.items())[:6]:
        samples.append({
            "symbol":      sym,
            "label":       entry.get("label"),
            "score":       entry.get("score"),
            "status":      entry.get("status") or "legacy",
            "computed_at": entry.get("computed_at"),
            "is_fresh":    _s2._is_fresh(sym),
        })

    return {
        "total_symbols":   total,
        "valid_labels":    valid,
        "null_labels":     null,
        "coverage_pct":    round(valid / total * 100, 1) if total else 0,
        "status_counts":   status_counts,
        "freshness":       fresh_counts,
        "lkg_loaded_at":   _s2._lkg_loaded_at,
        "lkg_path":        str(_s2._LKG_PATH),
        "sample_entries":  samples,
    }
