from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Header, HTTPException, Body
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
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
from agent.mode_normalizer import normalize_reasoning_model, mode_concept, mode_display_label

# Ensure LangSmith env vars are set before any langsmith import
import config as _cfg  # noqa: F401  — triggers os.environ.setdefault calls

try:
    from langsmith import traceable as _ls_traceable
except ImportError:
    def _ls_traceable(*args, **kwargs):
        def _noop(fn):
            return fn
        if args and callable(args[0]):
            return args[0]
        return _noop

traceable = _ls_traceable

from pathlib import Path
from urllib.parse import urlparse, unquote

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
}


class JWTAuthMiddleware:
    """DISABLED — pass-through. Auth is handled by _jwt_or_key() in endpoints.
    JWT login can be re-enabled later by restoring the middleware logic."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        await self.app(scope, receive, send)


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

    import threading
    threading.Thread(target=_do_init, daemon=True).start()
    asyncio.create_task(_briefing_precompute_loop())
    asyncio.create_task(_smart_earnings_loop())
    asyncio.create_task(_edgar_cache_loop())
    asyncio.create_task(_options_precompute_loop())
    asyncio.create_task(_polygon_options_ingestion_loop())
    yield

app = FastAPI(title="Trading Agent API", lifespan=lifespan)

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


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
@traceable(name="main.auth_login")
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
@traceable(name="main.auth_verify")
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
@traceable(name="main.auth_logout")
async def auth_logout(request: Request):
    """Logout — client should delete the token. Server-side is stateless."""
    return {"success": True, "message": "Logged out. Delete the token client-side."}

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

@traceable(name="main.briefing_precompute_loop")
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
            # News: prefer web_search (Perplexity→Brave→Tavily), FMP free tier is slow/unreliable
            if data_service.web_search:
                briefing_tasks.append(
                    asyncio.wait_for(
                        data_service.web_search.get_market_news(topic="stock market financial news today"),
                        timeout=10.0))
            elif data_service.fmp:
                briefing_tasks.append(
                    asyncio.wait_for(data_service.fmp.get_market_news(limit=15), timeout=8.0))
            else:
                briefing_tasks.append(asyncio.sleep(0))

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

            # FMP macro data
            fmp_data = {}
            if data_service.fmp:
                try:
                    dxy, commodities, treasuries, sector_perf, indices = await asyncio.gather(
                        data_service.fmp.get_dxy(),
                        data_service.fmp.get_key_commodities(),
                        data_service.fmp.get_treasury_rates(),
                        data_service.fmp.get_sector_performance(),
                        data_service.fmp.get_market_indices(),
                        return_exceptions=True,
                    )
                    fmp_data = {
                        "dxy": dxy if not isinstance(dxy, Exception) else {},
                        "commodities": commodities if not isinstance(commodities, Exception) else {},
                        "treasury_yields": treasuries if not isinstance(treasuries, Exception) else {},
                        "sector_performance": sector_perf if not isinstance(sector_perf, Exception) else [],
                        "indices": indices if not isinstance(indices, Exception) else {},
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

            multi_signal = {t: sources for t, sources in screener_sources.items() if len(sources) >= 2}
            priority_tickers = list(multi_signal.keys())[:15]
            remaining_slots = 20 - len(priority_tickers)
            if remaining_slots > 0:
                single_signal = {t: sources for t, sources in screener_sources.items() if len(sources) == 1}
                filler = [t for t in single_signal.keys() if t not in priority_tickers][:remaining_slots]
                priority_tickers.extend(filler)

            # Pre-cache market news via Perplexity (1 API call per 30-min cycle)
            web_news = {}
            if data_service.web_search:
                from api_budget import daily_budget
                if daily_budget.can_spend("web_search", 1):
                    try:
                        web_news = await asyncio.wait_for(
                            data_service.web_search.get_market_news(
                                topic="stock market today breaking news"
                            ),
                            timeout=12.0,
                        )
                        daily_budget.spend("web_search", 1)
                        print(f"[BRIEFING_PRECOMPUTE] Web search: {web_news.get('article_count', 0)} articles cached")
                    except Exception as e:
                        print(f"[BRIEFING_PRECOMPUTE] Web search failed: {e}")
                        web_news = {}

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


# ── Smart Earnings Scanner Background Loop ──────────────────────
# Runs twice daily at 8:00am and 12:00pm EST on weekdays.
# Makes ONE Grok call + ONE Perplexity call per scan across ALL tickers.
# Results cached to disk for 6 hours.
_smart_scan_running = False

@traceable(name="main.smart_earnings_loop")
async def _smart_earnings_loop():
    """Background loop: runs smart earnings scan at 8am + 12pm EST on weekdays."""
    global _smart_scan_running
    # Wait for init
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _init_event.wait, 120)

    if data_service is None:
        print("[SMART_EARNINGS] data_service not available, aborting loop")
        return

    from data.smart_earnings_scanner import run_smart_scan, get_cache_status
    from config import XAI_API_KEY, PERPLEXITY_API_KEY

    # Run once on startup if cache is empty/stale
    status = get_cache_status()
    if status["status"] != "fresh":
        print("[SMART_EARNINGS] Cache stale on startup, running initial scan")
        _smart_scan_running = True
        try:
            await run_smart_scan(data_service.finnhub.client, XAI_API_KEY, PERPLEXITY_API_KEY)
        except Exception as e:
            print(f"[SMART_EARNINGS] Initial scan failed: {e}")
        finally:
            _smart_scan_running = False

    while True:
        try:
            # Sleep until next 8am or 12pm EST
            from datetime import timezone, timedelta
            est = timezone(timedelta(hours=-5))
            now = _dt.now(est)
            target_hours = [8, 12]
            next_run = None
            for h in target_hours:
                candidate = now.replace(hour=h, minute=0, second=0, microsecond=0)
                if candidate > now:
                    next_run = candidate
                    break
            if next_run is None:
                # Next day 8am
                tomorrow = now + timedelta(days=1)
                next_run = tomorrow.replace(hour=8, minute=0, second=0, microsecond=0)

            # Skip weekends
            while next_run.weekday() >= 5:
                next_run += timedelta(days=1)
                next_run = next_run.replace(hour=8, minute=0, second=0, microsecond=0)

            wait_seconds = (next_run - now).total_seconds()
            print(f"[SMART_EARNINGS] Next scan at {next_run.strftime('%Y-%m-%d %H:%M')} EST ({wait_seconds/3600:.1f}h)")
            await asyncio.sleep(max(wait_seconds, 60))

            # Run scan
            _smart_scan_running = True
            try:
                await run_smart_scan(data_service.finnhub.client, XAI_API_KEY, PERPLEXITY_API_KEY)
            finally:
                _smart_scan_running = False

        except Exception as e:
            print(f"[SMART_EARNINGS] Loop error: {e}")
            import traceback
            traceback.print_exc()
            _smart_scan_running = False
            await asyncio.sleep(3600)  # Retry in 1 hour on error



# ============================================================
# EDGAR Background Cache Loop
# ============================================================

@traceable(name="main.edgar_cache_loop")
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
@traceable(name="main.root")
async def root():
    """Health check — visit this URL to confirm the backend is running."""
    return {"status": "running", "message": "Trading Agent API is live"}


@app.get("/ping")
@traceable(name="main.ping")
async def ping():
    return {"status": "ok", "code_version": "2026-03-08-v3-pure-asgi"}


@app.get("/health")
@traceable(name="main.health")
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
@traceable(name="main.debug_echo")
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
@traceable(name="main.debug_db")
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


@app.get("/api/debug/langsmith")
async def debug_langsmith(
    request: Request,
    hours: int = 24,
    limit: int = 20,
    errors_only: bool = False,
    run_id: Optional[str] = None,
):
    """Pull recent LangSmith traces, errors, and diagnostics.

    Query params:
      - hours: lookback window (default 24)
      - limit: max runs to return (default 20)
      - errors_only: only return error runs
      - run_id: get full detail for a specific run
    """
    try:
        from data.langsmith_diagnostics import diagnose, get_run_detail, get_recent_runs
    except ImportError as e:
        return JSONResponse(status_code=500, content={"error": f"Import failed: {e}"})

    try:
        if run_id:
            return get_run_detail(run_id)
        if errors_only:
            runs = get_recent_runs(hours=hours, error_only=True, limit=limit)
            return {"errors": runs}
        return diagnose(hours=hours, limit=limit)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/presets")
@traceable(name="main.list_presets")
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
@traceable(name="main.get_collab_options")
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
        ],
        # Collab agents — full list available for multi-agent collaboration.
        # Frontend sends based on preset:
        #   Default:            { reasoning_model: "agent_collab", collab_agents: ["grok","perplexity"] }
        #   Full Collaboration: { reasoning_model: "all_agents", collab_agents: [all], primary_model: "<user-chosen>" }
        #   Custom Collab:      { reasoning_model: "agent_collab", collab_agents: [...user-picked], primary_model: "<user-chosen>" }
        #                       OR { reasoning_model: "all_agents", collab_agents: [...user-picked], primary_model: "<user-chosen>" }
        #                       (frontend decides based on user's agent selection — agent_collab for data-source mode, all_agents for full fan-out)
        "collab_agents": [
            {"id": "claude", "name": "Claude (Anthropic)", "description": "Deep reasoning, analysis & synthesis", "icon": "anthropic"},
            {"id": "grok", "name": "Grok (X/Twitter)", "description": "Real-time X social scanning & sentiment", "icon": "xai"},
            {"id": "gpt-4o", "name": "ChatGPT/OpenAI", "description": "Web search, orchestration & reasoning", "icon": "openai"},
            {"id": "gemini", "name": "Gemini", "description": "Google Search grounding & reasoning", "icon": "gemini"},
            {"id": "perplexity", "name": "Perplexity", "description": "Deep web research with citations", "icon": "perplexity"},
        ],
        # Collab presets — pre-configured multi-agent collaboration setups.
        # lock_agents: if true, the collaborator checkboxes are locked (user cannot change them)
        # lock_reasoning: if true, the reasoning model radio is locked (user cannot change it)
        #
        # THREE DISTINCT MODES (currently implemented):
        #
        # 1. DEFAULT COLLAB — reasoning_model: "agent_collab", agents LOCKED (Grok + Perplexity).
        #    Backend pipeline: Grok X scan + Perplexity web search + proprietary data → single
        #    reasoning model synthesizes. User can change the reasoning model (primary_model)
        #    but cannot change which data sources run. The reasoner does NOT do its own web
        #    search — all live data comes through the pipeline.
        #
        # 2. FULL COLLAB — reasoning_model: "all_agents", ALL agents LOCKED.
        #    Every agent (Claude, Grok, GPT-4o, Gemini, Perplexity) runs simultaneously,
        #    each does its own web search / analysis, then the chosen primary_model synthesizes
        #    all agent theses into a unified response. User picks only the synthesis model.
        #
        # 3. CUSTOM COLLAB — reasoning_model: "agent_collab", agents UNLOCKED, reasoning UNLOCKED.
        #    Sits between Default and Full. User picks WHICH collaborating agent(s) to include
        #    AND which reasoning/primary model synthesizes. Highly customizable — NOT a
        #    one-size-fits-all preset. Defaults to agent_collab (not all_agents) so the user
        #    builds up their configuration from a clean slate via the frontend dropdowns.
        #    When the user selects specific collab_agents, the frontend sends the appropriate
        #    reasoning_model ("agent_collab" for data-source mode, "all_agents" for full fan-out).
        "presets": [
            {
                "id": "default",
                # ── Phase 0: ui_concept / ui_label tell the frontend which UX mode to show ──
                "ui_concept": "caelyn",
                "ui_label": "Caelyn",
                "name": "Caelyn",
                "description": "Automatic smart mode — Grok X scan + Perplexity web search + proprietary data → chosen reasoning model synthesizes",
                "agents": ["grok", "perplexity"],
                "reasoning_model": "agent_collab",
                "primary": "claude",
                "mode": "collab",
                "default": True,
                "lock_agents": True,
                "lock_reasoning": False,
            },
            {
                "id": "full_collab",
                "ui_concept": "customize",
                "ui_label": "Full Collaboration",
                "name": "Full Collaboration",
                "description": "All agents collaborate simultaneously — choose which model reasons",
                "agents": ["claude", "grok", "gpt-4o", "gemini", "perplexity"],
                "reasoning_model": "all_agents",
                "primary": "claude",
                "mode": "collab",
                "lock_agents": True,
                "lock_reasoning": False,
            },
            {
                "id": "custom_collab",
                "ui_concept": "customize",
                "ui_label": "Customize",
                "name": "Customize",
                "description": "Pick your collaborating agent(s) and reasoning model — fully customizable",
                "agents": [],
                "reasoning_model": "agent_collab",
                "primary": "claude",
                "mode": "collab",
                "lock_agents": False,
                "lock_reasoning": False,
            },
        ],
    }


# ============================================================
# Polymarket Gamma API Proxy
# ============================================================

@app.get("/api/polymarket/events")
@limiter.limit("30/minute")
@traceable(name="main.polymarket_events_proxy")
async def polymarket_events_proxy(request: Request):
    """Proxy for Polymarket Gamma API — avoids CORS issues on the frontend."""
    import httpx
    params = dict(request.query_params)
    params.setdefault("limit", "50")
    params.setdefault("active", "true")
    params.setdefault("closed", "false")
    params.setdefault("order", "volume24hr")
    params.setdefault("ascending", "false")
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; TradingAgent/1.0)",
        "Accept": "application/json",
    }
    url = "https://gamma-api.polymarket.com/events"
    print(f"[POLYMARKET_PROXY] Fetching {url} params={params}")
    try:
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
            resp = await client.get(url, params=params, headers=headers)
            print(f"[POLYMARKET_PROXY] Response status={resp.status_code} len={len(resp.content)}")
            resp.raise_for_status()
            data = resp.json()
            # Polymarket returns a JSON array — pass it through directly
            return JSONResponse(content=data)
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
@traceable(name="main.news_feed")
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
# Earnings Calendar Endpoint — Full Finnhub calendar by date range
# ============================================================

@app.get("/api/earnings/calendar")
@limiter.limit("10/minute")
@traceable(name="main.earnings_calendar")
async def earnings_calendar(request: Request, from_date: str = "", to_date: str = ""):
    """
    Returns all earnings for a date range from Finnhub.
    Defaults to current week (Mon-Fri) if no dates provided.
    """
    from datetime import datetime, timedelta

    await _wait_for_init()

    # Default to current week
    today = datetime.now()
    if not from_date:
        # Go to Monday of current week
        monday = today - timedelta(days=today.weekday())
        from_date = monday.strftime("%Y-%m-%d")
    if not to_date:
        monday = today - timedelta(days=today.weekday())
        friday = monday + timedelta(days=4)
        to_date = friday.strftime("%Y-%m-%d")

    from data.cache import cache
    cache_key = f"earnings_calendar:{from_date}:{to_date}"
    cached = cache.get(cache_key)
    if cached is not None:
        return JSONResponse(content=cached)

    try:
        data = await asyncio.wait_for(
            asyncio.to_thread(
                agent.data.finnhub.client.earnings_calendar,
                _from=from_date,
                to=to_date,
                symbol=None,
            ),
            timeout=10.0,
        )
        earnings = data.get("earningsCalendar", [])

        results = []
        for e in earnings:
            symbol = e.get("symbol")
            if not symbol:
                continue
            results.append({
                "ticker": symbol,
                "date": e.get("date"),
                "eps_estimate": e.get("epsEstimate"),
                "eps_actual": e.get("epsActual"),
                "revenue_estimate": e.get("revenueEstimate"),
                "revenue_actual": e.get("revenueActual"),
                "hour": e.get("hour", ""),  # "bmo", "amc", or ""
                "quarter": e.get("quarter"),
                "year": e.get("year"),
            })

        response = {"earnings": results, "from": from_date, "to": to_date, "count": len(results)}
        cache.set(cache_key, response, 300)  # Cache 5 minutes
        return JSONResponse(content=response)

    except Exception as e:
        print(f"[EARNINGS_CALENDAR] Error: {e}")
        return JSONResponse(status_code=502, content={"error": f"Finnhub calendar unavailable: {str(e)[:200]}"})


# ============================================================
# Smart Earnings Endpoints — AI-curated ticker filtering
# ============================================================

@app.get("/api/earnings/smart/{date}")
@limiter.limit("20/minute")
@traceable(name="main.smart_earnings_for_date")
async def smart_earnings_for_date(request: Request, date: str):
    """
    Return Tier 2 (social + news ranked) earnings tickers for a specific date.
    Reads from file-backed cache (populated by background scheduler).
    If cache miss, returns empty tier2 list and triggers a background scan
    for that week.
    """
    await _wait_for_init()

    from data.smart_earnings_scanner import get_cached_smart_day, get_cache_status

    cached = get_cached_smart_day(date)
    if cached:
        cached["cache_status"] = get_cache_status()
        cached["scanning"] = _smart_scan_running
        return JSONResponse(content=cached)

    # Cache miss — return empty tier2, trigger background scan for this week
    result = {
        "tickers": [],
        "count": 0,
        "cached_at": 0,
        "cache_status": get_cache_status(),
        "scanning": True,  # we're about to start one
    }

    # Trigger background scan for the requested week if not already running
    if not _smart_scan_running:
        from config import XAI_API_KEY, PERPLEXITY_API_KEY
        from data.smart_earnings_scanner import run_smart_scan
        async def _bg_refresh():
            global _smart_scan_running
            _smart_scan_running = True
            try:
                await run_smart_scan(data_service.finnhub.client, XAI_API_KEY, PERPLEXITY_API_KEY, reference_date=date)
            except Exception as ex:
                print(f"[SMART_EARNINGS] Background refresh failed: {ex}")
            finally:
                _smart_scan_running = False
        asyncio.create_task(_bg_refresh())

    return JSONResponse(content=result)


@app.get("/api/earnings/smart-status")
@limiter.limit("30/minute")
@traceable(name="main.smart_earnings_status")
async def smart_earnings_status(request: Request):
    """Return cache freshness status for UI display."""
    from data.smart_earnings_scanner import get_cache_status
    status = get_cache_status()
    status["scanning"] = _smart_scan_running
    return JSONResponse(content=status)


@app.post("/api/earnings/refresh-smart-cache")
@limiter.limit("2/minute")
@traceable(name="main.refresh_smart_cache")
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
@traceable(name="main.get_settings_endpoint")
async def get_settings_endpoint(request: Request):
    from data.user_settings import get_settings
    from agent.prompts import DEFAULT_PERSONAL_PROFILE, CORE_QUANT_DNA
    settings = get_settings()
    settings["default_personal_profile"] = DEFAULT_PERSONAL_PROFILE
    settings["core_quant_dna"] = CORE_QUANT_DNA
    return JSONResponse(content=settings)


@app.put("/api/settings")
@limiter.limit("20/minute")
@traceable(name="main.update_settings_endpoint")
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
@traceable(name="main.save_template_endpoint")
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
@traceable(name="main.delete_template_endpoint")
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
@traceable(name="main.earnings_detail")
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

    # Cache for 10 minutes
    cache.set(cache_key, result, 600)
    return JSONResponse(content=result)


@traceable(name="main.verify_api_key")
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

@traceable(name="main.build_meta")
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
    }


@traceable(name="main.render_cross_market_analysis")
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


@traceable(name="main.render_trades_analysis")
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
@traceable(name="main.render_screener_analysis")
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
        parts.append(f"Scanned {scan_stats.get(chr(39)+'candidates_total'+chr(39), chr(39)+'?'+chr(39))} candidates")
    return chr(10).join(parts).strip()


_RENDERERS = {
    "cross_market": _render_cross_market_analysis,
    "trades": _render_trades_analysis,
    "screener": _render_screener_analysis,
}


@traceable(name="main.ensure_analysis")
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


@traceable(name="main.ok_envelope")
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


@traceable(name="main.error_envelope")
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


@traceable(name="main.resp_log")
def _resp_log(req_id: str, status: int, resp_type: str, resp: dict):
    resp_bytes = len(_json.dumps(resp, default=str).encode("utf-8"))
    print(f"[RESP] id={req_id} status={status} type={resp_type} bytes={resp_bytes}")


@app.post("/api/social/query")
@limiter.limit("10/minute")
@traceable(name="main.social_grok_query")
async def social_grok_query(
    request: Request,
    body: dict = Body(...),
    api_key: str = Header(None, alias="X-API-Key"),
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
        _X_SELECT_HANDLES = [
            "aleabitoreddit", "KobeissiLetter", "HyperTechInvest", "crux_capital_",
            "SJCapitalInvest", "BlackPantherCap", "Kaizen_Investor", "Venu_7_",
            "DrJebaim", "CKCapitalxx", "TheTape_TNM", "equitydd",
            "Speculator_io", "StonkValue", "stamatoudism", "yianisz",
            "sunxliao", "futurist_lens", "Thomas_james_1", "DeepValueBagger",
            "ConnorJBates_", "BussinBiotech", "BambroughKevin", "AlexfromBabylon",
            "UncleAlpha007",
        ]

        # ── Phase 1: Parallel batched x_search (max ~8 handles per call) ──
        # Grok's allowed_x_handles supports ~10 per call; we batch into groups
        # using the fast non-reasoning model for data collection.
        BATCH_SIZE = 8
        batches = [_X_SELECT_HANDLES[i:i + BATCH_SIZE]
                    for i in range(0, len(_X_SELECT_HANDLES), BATCH_SIZE)]
        print(f"[SOCIAL_GROK] Select trader consensus — {len(_X_SELECT_HANDLES)} handles in {len(batches)} batches")

        async def _fetch_batch(handles: list[str], batch_num: int) -> str:
            """Fetch raw post data for a batch of handles."""
            batch_prompt = (
                f"Search the last 20 posts from EACH of these accounts: "
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
                x_search_config={"allowed_x_handles": handles},
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
                "Below is raw data from X/Twitter posts by 25 select trader accounts. "
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


@app.post("/api/query")
@limiter.limit("10/minute")
@traceable(name="main.query_agent")
async def query_agent(
    request: Request,
    body: QueryRequest,
    api_key: str = Header(None, alias="X-API-Key"),
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
        user_query = "Analyze every ticker in this uploaded CSV. Give a BUY, HOLD, or SELL rating for each, plus identify the top 2-3 best investments."

    # ── Phase 0: normalize inbound mode/reasoning_model string ──────────────
    # Accepts "caelyn", "customize", legacy aliases, and existing identifiers.
    # All downstream code continues to use internal identifiers unchanged.
    body.reasoning_model = normalize_reasoning_model(body.reasoning_model)

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
@traceable(name="main.test_csv")
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
@traceable(name="main.clear_cache")
async def clear_cache(request: Request, api_key: str = Header(None, alias="X-API-Key")):
    if not _jwt_or_key(request, api_key):
        raise HTTPException(status_code=403, detail="Invalid API key")
    from data.cache import cache
    cache.clear()
    return {"status": "Cache cleared"}


class WatchlistRequest(BaseModel):
    tickers: List[str]
    conversation_id: Optional[str] = None
    reasoning_model: Optional[str] = "agent_collab"

@app.post("/api/watchlist")
@limiter.limit("10/minute")
@traceable(name="main.review_watchlist")
async def review_watchlist(
    request: Request,
    body: WatchlistRequest,
    api_key: str = Header(None, alias="X-API-Key"),
):
    import asyncio
    if not _jwt_or_key(request, api_key):
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")
    if not body.tickers:
        raise HTTPException(status_code=400, detail="No tickers provided.")
    await _wait_for_init()

    tickers = [t.strip().upper() for t in body.tickers if t.strip()][:25]
    body.reasoning_model = normalize_reasoning_model(body.reasoning_model)
    print(f"[API] Watchlist review request: {tickers}")

    try:
        result = await asyncio.wait_for(
            agent.review_watchlist(tickers, reasoning_model=body.reasoning_model or "agent_collab"),
            timeout=90.0,
        )

        if body.conversation_id:
            try:
                from data.chat_history import append_message as _append2
                _append2(body.conversation_id, "user", f"Review my watchlist: {', '.join(tickers)}", message_type="watchlist", model_used=body.reasoning_model or "agent_collab")
                _append2(body.conversation_id, "assistant", result.get("analysis", "") if isinstance(result, dict) else _json.dumps(result, default=str)[:8000], message_type="watchlist", structured_payload=result if isinstance(result, dict) else None, model_used=body.reasoning_model or "agent_collab")
            except Exception as e:
                print(f"[API] Failed to save watchlist conversation: {e}")

        return result
    except asyncio.TimeoutError:
        print("[API] Watchlist review timed out after 90s")
        return {
            "type": "chat",
            "analysis": "",
            "structured": {
                "display_type": "chat",
                "message": "Watchlist review timed out. Try fewer tickers.",
            },
        }
    except Exception as e:
        import traceback
        print(f"[API] Error in /api/watchlist: {e}")
        traceback.print_exc()
        return {
            "type": "chat",
            "analysis": "",
            "structured": {
                "display_type": "chat",
                "message": f"Error reviewing watchlist: {str(e)}",
            },
        }


class CreateConversationRequest(BaseModel):
    first_query: str = "New conversation"

class UpdateConversationRequest(BaseModel):
    messages: List[dict] = []


@traceable(name="main.shape_prompt_history")
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
@traceable(name="main.get_conversations")
async def get_conversations(request: Request):
    from data.chat_history import list_conversations
    return {"conversations": list_conversations(), "_meta": _history_storage_meta()}

@app.get("/api/conversations/{conv_id}")
@limiter.limit("30/minute")
@traceable(name="main.get_conversation_detail")
async def get_conversation_detail(request: Request, conv_id: str):
    from data.chat_history import get_conversation
    conv = get_conversation(conv_id)
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return conv

@app.post("/api/conversations")
@limiter.limit("30/minute")
@traceable(name="main.create_new_conversation")
async def create_new_conversation(request: Request, body: CreateConversationRequest):
    from data.chat_history import create_conversation
    conv = create_conversation(body.first_query)
    return conv

@app.put("/api/conversations/{conv_id}")
@limiter.limit("30/minute")
@traceable(name="main.update_conversation")
async def update_conversation(request: Request, conv_id: str, body: UpdateConversationRequest):
    from data.chat_history import save_messages
    success = save_messages(conv_id, body.messages)
    if not success:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return {"success": success}

@app.delete("/api/conversations/{conv_id}")
@limiter.limit("30/minute")
@traceable(name="main.delete_conv")
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
@traceable(name="main.get_history")
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
        @traceable(name="fetch")
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
@traceable(name="main.get_history_recent")
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
@traceable(name="main.get_history_sidebar")
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
@traceable(name="main.history_storage_info")
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
@traceable(name="main.history_backtest_summary")
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
    @traceable(name="fetch_price")
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
@traceable(name="main.get_history_by_intent")
async def get_history_by_intent(request: Request, category: str, intent: str):
    from data.prompt_history import get_by_intent
    user_id = getattr(request.state, "user_id", "default")
    return {"entries": get_by_intent(category, intent, user_id=user_id)}

@app.post("/api/history")
@limiter.limit("30/minute")
@traceable(name="main.save_history")
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
@traceable(name="main.delete_history_entry")
async def delete_history_entry(request: Request, category: str, intent: str, entry_id: str):
    user_id = getattr(request.state, "user_id", "default")
    from data.prompt_history import delete_entry
    success = delete_entry(category, intent, entry_id, user_id=user_id)
    return {"success": success}

@app.delete("/api/history/{category}/{intent}")
@limiter.limit("30/minute")
@traceable(name="main.clear_history_intent")
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
@traceable(name="main.backtest_recommendations")
async def backtest_recommendations(request: Request, body: BacktestRequest):
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
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
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
@traceable(name="main.health_check")
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
                model="claude-sonnet-4-20250514",
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
# Candle Stats Debug Endpoint
# ============================================================

@app.get("/api/candle_stats")
@traceable(name="main.candle_stats")
async def candle_stats(request: Request):
    from data.market_data_service import get_last_candle_stats, _is_finnhub_candles_disabled, _is_twelvedata_disabled
    stats = get_last_candle_stats()
    stats["finnhub_circuit_open"] = _is_finnhub_candles_disabled()
    stats["twelvedata_circuit_open"] = _is_twelvedata_disabled()
    return stats


@app.get("/api/health/budget")
@traceable(name="main.health_budget")
async def health_budget(request: Request):
    from api_budget import daily_budget
    return daily_budget.status()


# ============================================================
# Portfolio Holdings CRUD
# ============================================================

@traceable(name="main.portfolio_file")
def _portfolio_file(user_id: str) -> Path:
    return Path(f"data/portfolio_holdings_{user_id}.json")


@app.get("/api/portfolio/holdings")
@traceable(name="main.get_holdings")
async def get_holdings(request: Request, api_key: str = Header(None, alias="X-API-Key")):
    """Return saved portfolio holdings (JSON file, per-user)."""
    user_id = getattr(request.state, "user_id", "default")
    portfolio_file = _portfolio_file(user_id)
    if not portfolio_file.exists():
        return {"holdings": []}
    try:
        with open(portfolio_file) as f:
            data = _json.load(f)
        if isinstance(data, dict) and "holdings" in data:
            return data
        return {"holdings": []}
    except Exception:
        return {"holdings": []}


@app.post("/api/portfolio/holdings")
@traceable(name="main.save_holdings")
async def save_holdings(request: Request, api_key: str = Header(None, alias="X-API-Key")):
    """Save portfolio holdings. Expects {holdings: [{ticker, shares, avg_cost, ...}]}."""
    user_id = getattr(request.state, "user_id", "default")
    body = await request.json()
    if not isinstance(body, dict) or "holdings" not in body:
        raise HTTPException(status_code=400, detail="Body must be {holdings: [...]}")
    if not isinstance(body["holdings"], list):
        raise HTTPException(status_code=400, detail="holdings must be a list")
    portfolio_file = _portfolio_file(user_id)
    portfolio_file.parent.mkdir(parents=True, exist_ok=True)
    with open(portfolio_file, "w") as f:
        _json.dump(body, f)
    return {"success": True}


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


@traceable(name="main.get_coingecko_symbol_map")
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
@traceable(name="main.get_portfolio_quotes")
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
        # ---- STOCKS: Finnhub primary → Yahoo fallback → FMP last resort ----
        if stock_tickers:
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
                                "market_cap": d.get("marketCapitalization", 0),
                                "logo": d.get("logo", ""),
                            }
                            if profile.get("market_cap"):
                                profile["market_cap"] = profile["market_cap"] * 1_000_000
                            _cache.set(sector_cache_key, profile, 86400)
                            return sym, profile
                except Exception:
                    pass
                return sym, None

            tasks = []
            for sym in stock_tickers:
                tasks.append(_finnhub_quote(sym))
                tasks.append(_finnhub_profile(sym))
            results = await asyncio.gather(*tasks)

            finnhub_quotes = {}
            finnhub_profiles = {}
            for i in range(0, len(results), 2):
                sym, quote_data = results[i]
                _, profile_data = results[i + 1]
                if quote_data:
                    finnhub_quotes[sym] = quote_data
                if profile_data:
                    finnhub_profiles[sym] = profile_data

            for sym in stock_tickers:
                q = finnhub_quotes.get(sym)
                p = finnhub_profiles.get(sym, {})
                if q:
                    quotes[sym] = {
                        "price": q.get("c"),
                        "change": q.get("d"),
                        "change_pct": q.get("dp"),
                        "day_high": q.get("h"),
                        "day_low": q.get("l"),
                        "market_cap": p.get("market_cap"),
                        "volume": None,
                        "sector": p.get("sector", ""),
                        "industry": p.get("industry", ""),
                        "company_name": p.get("company_name", ""),
                        "source": "finnhub",
                    }

            finnhub_found = [t for t in stock_tickers if t in quotes]
            finnhub_missing = [t for t in stock_tickers if t not in quotes]
            print(f"[PORTFOLIO] Finnhub returned {len(finnhub_found)} quotes, missing: {finnhub_missing}")

            if finnhub_missing:
                print(f"[PORTFOLIO] Trying Yahoo for: {finnhub_missing}")
                for sym in finnhub_missing:
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
                                    p = finnhub_profiles.get(sym, {})
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
                                        "source": "yahoo",
                                    }
                                    print(f"[PORTFOLIO] Yahoo: {sym} = ${price}")
                    except Exception as e:
                        print(f"[PORTFOLIO] Yahoo {sym} error: {e}")

            yahoo_missing = [t for t in stock_tickers if t not in quotes]
            if yahoo_missing:
                print(f"[PORTFOLIO] FMP last resort for: {yahoo_missing}")
                ticker_str = ",".join(yahoo_missing)
                try:
                    full_resp = await client.get(
                        "https://financialmodelingprep.com/stable/quote",
                        params={"symbol": ticker_str, "apikey": FMP_API_KEY},
                    )
                    if full_resp.status_code == 200:
                        for item in full_resp.json():
                            symbol = item.get("symbol", "")
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
                            }
                        print(f"[PORTFOLIO] FMP fallback returned {len([t for t in yahoo_missing if t in quotes])} quotes")
                except Exception as e:
                    print(f"[PORTFOLIO] FMP fallback error: {e}")

            stocks_needing_sector = [t for t in stock_tickers if t in quotes and not quotes[t].get("sector")]
            if stocks_needing_sector:
                print(f"[PORTFOLIO] Fetching sector via FMP /stable/profile for: {stocks_needing_sector}")
                # Resolve cached sectors first
                _uncached_sector_tickers = []
                for ticker in stocks_needing_sector:
                    sector_cache_key = f"sector:{ticker}"
                    cached_sector = _cache.get(sector_cache_key)
                    if cached_sector is not None:
                        quotes[ticker]["sector"] = cached_sector.get("sector", "Other")
                        quotes[ticker]["industry"] = cached_sector.get("industry", "")
                        quotes[ticker]["company_name"] = cached_sector.get("company_name", "")
                    else:
                        _uncached_sector_tickers.append(ticker)

                # Fetch uncached sectors in parallel
                async def _fetch_sector(t):
                    try:
                        profile_resp = await client.get(
                            "https://financialmodelingprep.com/stable/profile",
                            params={"symbol": t, "apikey": FMP_API_KEY},
                        )
                        if profile_resp.status_code == 200:
                            profile_data = profile_resp.json()
                            if isinstance(profile_data, list) and len(profile_data) > 0:
                                sector = profile_data[0].get("sector", "")
                                industry = profile_data[0].get("industry", "")
                                company_name = profile_data[0].get("companyName", "")
                                return t, sector, industry, company_name
                        return t, "", "", ""
                    except Exception as e:
                        print(f"[PORTFOLIO] FMP profile {t} error: {e}")
                        return t, "", "", ""

                if _uncached_sector_tickers:
                    _sector_results = await asyncio.gather(
                        *[_fetch_sector(t) for t in _uncached_sector_tickers]
                    )
                    for t, sector, industry, company_name in _sector_results:
                        if sector:
                            quotes[t]["sector"] = sector
                            quotes[t]["industry"] = industry
                            quotes[t]["company_name"] = company_name
                            _cache.set(f"sector:{t}", {"sector": sector, "industry": industry, "company_name": company_name}, 86400)
                        else:
                            quotes[t]["sector"] = "Other"

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

@app.get("/api/portfolio/events")
@traceable(name="main.get_portfolio_events")
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
# Portfolio Review (AI-powered Buy/Hold/Sell analysis)
# ============================================================

@app.post("/api/portfolio/review")
@limiter.limit("5/minute")
@traceable(name="main.review_portfolio")
async def review_portfolio(request: Request, api_key: str = Header(None, alias="X-API-Key")):
    """AI Portfolio Review — comprehensive analysis with Buy/Hold/Sell verdicts."""
    import asyncio
    import sys
    import time as _time
    from fastapi.responses import JSONResponse

    @traceable(name="log")
    def _log(msg):
        print(msg, flush=True)

    await _wait_for_init()
    body = await request.json()
    start = _time.time()
    DEADLINE = 55.0

    _log(f"[PORTFOLIO_REVIEW] === ENDPOINT HIT ===")
    holdings = body.get("holdings", [])
    _log(f"[PORTFOLIO_REVIEW] Holdings: {[h.get('ticker') for h in holdings]}")

    if not holdings:
        return {
            "type": "chat",
            "analysis": "",
            "structured": {
                "display_type": "chat",
                "message": "No holdings to review. Add some positions to your portfolio first.",
            },
        }

    @traceable(name="err_response")
    def _err_response(msg):
        return JSONResponse(status_code=200, content={
            "type": "chat", "analysis": "",
            "structured": {"display_type": "chat", "message": msg},
        })

    try:
        tickers = [h.get("ticker", "").upper().strip() for h in holdings if h.get("ticker")]

        # Enrich equity holdings with free EDGAR XBRL financials
        stock_tickers = [
            h.get("ticker", "").upper().strip() for h in holdings
            if h.get("ticker") and h.get("type", h.get("asset_type", "stock")).lower()
            not in ("crypto", "index", "etf")
        ][:8]
        edgar_enrichment = {}
        if stock_tickers:
            try:
                edgar_enrichment = await asyncio.wait_for(
                    agent.data.enrich_with_edgar(stock_tickers, mode="standard"),
                    timeout=10.0,
                )
                _log(f"[PORTFOLIO_REVIEW] EDGAR enriched: {list(edgar_enrichment.keys())}")
            except Exception as e:
                _log(f"[PORTFOLIO_REVIEW] EDGAR enrichment error: {e}")

        holdings_context = []
        for h in holdings:
            ticker = h.get("ticker", "").upper().strip()
            shares = float(h.get("shares", 0) or 0)
            avg_cost = float(h.get("avg_cost", 0) or h.get("avgCost", 0) or 0)
            asset_type = h.get("type", h.get("asset_type", "stock")).lower()
            holdings_context.append({
                "ticker": ticker,
                "shares": shares,
                "avg_cost": avg_cost,
                "cost_basis": round(shares * avg_cost, 2),
                "asset_type": asset_type,
            })

        async def fetch_ticker_data(ticker, asset_type="stock"):
            if _is_known_index(ticker):
                asset_type = "index"
            result = {"ticker": ticker, "asset_type": asset_type}
            tasks = {}

            if asset_type == "index":
                idx_info = INDEX_MAP.get(ticker, {})
                proxy_etf = idx_info.get("proxy")
                result["company_name"] = idx_info.get("name", ticker)
                result["tradingview_symbol"] = idx_info.get("tv", f"TVC:{ticker}")

                if ticker == "VIX":
                    try:
                        from data.fred_provider import FredProvider
                        fred = FredProvider(api_key=os.getenv("FRED_API_KEY", ""))
                        tasks["vix_fred"] = asyncio.wait_for(
                            asyncio.to_thread(fred.get_vix), timeout=5.0)
                    except Exception:
                        pass
                elif proxy_etf:
                    try:
                        tasks["quote"] = asyncio.wait_for(
                            asyncio.to_thread(agent.data.finnhub.get_quote, proxy_etf), timeout=4.0)
                    except Exception:
                        pass
                    try:
                        tasks["candles"] = asyncio.wait_for(
                            agent.data.get_candles(proxy_etf, days=120), timeout=6.0)
                    except Exception:
                        pass
            else:
                try:
                    tasks["quote"] = asyncio.wait_for(
                        asyncio.to_thread(agent.data.finnhub.get_quote, ticker), timeout=4.0)
                except Exception:
                    pass

                try:
                    tasks["candles"] = asyncio.wait_for(
                        agent.data.get_candles(ticker, days=120), timeout=6.0)
                except Exception:
                    pass

                try:
                    if agent.data.fmp:
                        tasks["news"] = asyncio.wait_for(
                            agent.data.fmp.get_stock_news(ticker, limit=3), timeout=4.0)
                except Exception:
                    pass

            if not tasks:
                return result

            task_keys = list(tasks.keys())
            task_coros = list(tasks.values())
            results = await asyncio.gather(*task_coros, return_exceptions=True)

            for key, res in zip(task_keys, results):
                if isinstance(res, Exception):
                    _log(f"[PORTFOLIO_REVIEW] {ticker}/{key} failed: {type(res).__name__}: {res}")
                    continue
                if not res:
                    continue
                if key == "vix_fred":
                    if isinstance(res, dict) and res.get("current_vix"):
                        result["current_price"] = res["current_vix"]
                        trend = res.get("trend", [])
                        if len(trend) >= 2:
                            prev = trend[-2]["vix"]
                            result["change_pct"] = round(((res["current_vix"] - prev) / prev) * 100, 2) if prev else 0
                        result["vix_signal"] = res.get("signal", "")
                elif key == "quote":
                    if isinstance(res, dict) and res.get("price"):
                        result["current_price"] = res["price"]
                        result["change_pct"] = res.get("change_pct")
                elif key == "candles":
                    if isinstance(res, list) and len(res) >= 20:
                        try:
                            from data.ta_utils import compute_technicals_from_bars
                            result["technicals"] = compute_technicals_from_bars(res)
                        except Exception as ta_err:
                            _log(f"[PORTFOLIO_REVIEW] {ticker}/ta compute failed: {ta_err}")
                        result["current_price"] = result.get("current_price") or res[-1].get("c")
                elif key == "news":
                    result["recent_news"] = res[:3] if isinstance(res, list) else res

            # Inject web search enrichment (pre-fetched in batch)
            if _tavily_prefetch_data and ticker.upper() in _tavily_prefetch_data:
                t_data = _tavily_prefetch_data[ticker.upper()]
                result["tavily_enrichment"] = t_data
                result["overview"] = t_data  # replaces StockAnalysis overview
                result["analyst_ratings"] = t_data  # replaces StockAnalysis analyst

            return result

        # Web search batch pre-fetch: 1-2 calls replaces 45 StockAnalysis+StockTwits calls
        _tavily_prefetch_data = {}
        stock_tickers = [h["ticker"] for h in holdings_context
                         if h.get("asset_type", "stock") == "stock" and not _is_known_index(h["ticker"])]
        if agent.data.web_search and stock_tickers:
            from api_budget import daily_budget
            if daily_budget.can_spend("web_search", 2):
                try:
                    _tavily_prefetch_data = await asyncio.wait_for(
                        agent.data.web_search.enrich_tickers_batched(stock_tickers[:12]),
                        timeout=12.0,
                    )
                    daily_budget.spend("web_search", min(2, (len(stock_tickers[:12]) + 5) // 6))
                    _log(f"[PORTFOLIO_REVIEW] Web search pre-fetched {len([k for k in _tavily_prefetch_data if not k.startswith('_')])} tickers")
                except Exception as e:
                    _log(f"[PORTFOLIO_REVIEW] Web search pre-fetch failed: {e}")

        _log(f"[PORTFOLIO_REVIEW] Starting parallel fetch for {len(tickers)} tickers + macro...")

        @traceable(name="fetch_all_tickers")
        async def _fetch_all_tickers():
            ticker_asset_map = {h["ticker"]: h.get("asset_type", "stock") for h in holdings_context}
            ticker_tasks = [fetch_ticker_data(t, ticker_asset_map.get(t, "stock")) for t in tickers[:15]]
            return await asyncio.gather(*ticker_tasks, return_exceptions=True)

        @traceable(name="fetch_macro")
        async def _fetch_macro():
            try:
                return await asyncio.wait_for(agent.data._build_macro_snapshot(), timeout=6.0)
            except Exception as e:
                _log(f"[PORTFOLIO_REVIEW] Macro snapshot failed: {e}")
                return {}

        async def _fetch_grok():
            if not agent.data.xai or not tickers:
                return {}
            try:
                return await asyncio.wait_for(
                    agent.data.xai.get_batch_sentiment(tickers[:3]), timeout=10.0)
            except Exception as e:
                _log(f"[PORTFOLIO_REVIEW] Grok sentiment failed: {e}")
                return {}

        all_results = await asyncio.gather(
            _fetch_all_tickers(), _fetch_macro(), _fetch_grok(),
            return_exceptions=True,
        )

        ticker_results = all_results[0] if not isinstance(all_results[0], Exception) else []
        macro_snapshot = all_results[1] if not isinstance(all_results[1], Exception) else {}
        grok_sentiment = all_results[2] if not isinstance(all_results[2], Exception) else {}

        if isinstance(ticker_results, Exception):
            _log(f"[PORTFOLIO_REVIEW] Ticker fetch FATAL: {ticker_results}")
            ticker_results = []

        ticker_data = {}
        for res in ticker_results:
            if isinstance(res, Exception):
                _log(f"[PORTFOLIO_REVIEW] Ticker fetch exception: {res}")
                continue
            if isinstance(res, dict) and res.get("ticker"):
                ticker_data[res["ticker"]] = res

        elapsed = _time.time() - start
        _log(f"[PORTFOLIO_REVIEW] Data gathered for {len(ticker_data)}/{len(tickers)} tickers ({elapsed:.1f}s)")

        if isinstance(grok_sentiment, dict):
            for ticker, grok_data in grok_sentiment.items():
                if ticker in ticker_data and isinstance(grok_data, dict) and "error" not in grok_data:
                    ticker_data[ticker]["x_sentiment"] = grok_data
            if grok_sentiment:
                _log(f"[PORTFOLIO_REVIEW] Grok sentiment merged: {len(grok_sentiment)} tickers")

        total_cost_basis = sum(h["cost_basis"] for h in holdings_context)

        portfolio_summary = {
            "total_holdings": len(holdings_context),
            "total_cost_basis": total_cost_basis,
            "holdings": [],
        }

        for h in holdings_context:
            ticker = h["ticker"]
            td = ticker_data.get(ticker, {})
            current_price = td.get("current_price")

            position = {
                "ticker": ticker,
                "shares": h["shares"],
                "avg_cost": h["avg_cost"],
                "cost_basis": h["cost_basis"],
                "weight_pct": round(h["cost_basis"] / total_cost_basis * 100, 1) if total_cost_basis else 0,
            }

            if current_price:
                position["current_price"] = current_price
                position["market_value"] = round(h["shares"] * current_price, 2)
                position["unrealized_pnl"] = round((current_price - h["avg_cost"]) * h["shares"], 2)
                position["unrealized_pnl_pct"] = round((current_price - h["avg_cost"]) / h["avg_cost"] * 100, 1) if h["avg_cost"] else 0
                position["change_today_pct"] = td.get("change_pct")

            overview = td.get("overview", {})
            if isinstance(overview, dict):
                for key in ["pe_ratio", "ps_ratio", "market_cap", "revenue_growth", "eps_growth",
                            "profit_margin", "sector", "industry", "beta", "52_week_high", "52_week_low",
                            "dividend_yield", "analyst_rating", "price_target"]:
                    val = overview.get(key)
                    if val is not None:
                        position[key] = val

            technicals = td.get("technicals", {})
            if technicals:
                position["ta"] = {
                    "rsi": technicals.get("rsi") or technicals.get("rsi_14"),
                    "sma_20": technicals.get("sma_20"),
                    "sma_50": technicals.get("sma_50"),
                    "sma_200": technicals.get("sma_200"),
                    "macd_signal": technicals.get("macd_signal"),
                }

            social = td.get("social_sentiment", {})
            if social and isinstance(social, dict):
                position["sentiment"] = social.get("sentiment", "unknown")
                position["sentiment_score"] = social.get("bullish_pct")

            x_sent = td.get("x_sentiment", {})
            if x_sent and isinstance(x_sent, dict):
                position["x_sentiment"] = x_sent.get("overall_sentiment")
                position["x_summary"] = x_sent.get("summary")

            analyst = td.get("analyst_ratings", {})
            if analyst and isinstance(analyst, dict):
                position["analyst_consensus"] = analyst.get("consensus") or analyst.get("rating")
                position["analyst_target"] = analyst.get("price_target") or analyst.get("target_price")

            news = td.get("recent_news", [])
            if news and isinstance(news, list):
                position["news_headlines"] = [
                    n.get("title", n.get("headline", "")) for n in news[:3] if isinstance(n, dict)
                ]

            portfolio_summary["holdings"].append(position)

        total_market_value = sum(
            p.get("market_value", p.get("cost_basis", 0))
            for p in portfolio_summary["holdings"]
        )
        portfolio_summary["total_market_value"] = total_market_value
        portfolio_summary["total_unrealized_pnl"] = round(total_market_value - total_cost_basis, 2)
        portfolio_summary["total_return_pct"] = round(
            (total_market_value - total_cost_basis) / total_cost_basis * 100, 1
        ) if total_cost_basis else 0

        weights = [p.get("weight_pct", 0) for p in portfolio_summary["holdings"]]
        portfolio_summary["max_weight"] = max(weights) if weights else 0
        portfolio_summary["hhi"] = round(sum(w**2 for w in weights), 1)

        if isinstance(macro_snapshot, dict) and macro_snapshot:
            portfolio_summary["macro_context"] = {
                "fear_greed": macro_snapshot.get("fear_greed"),
                "vix": macro_snapshot.get("vix"),
                "treasury_10y": macro_snapshot.get("treasury_rates", {}).get("10y") if isinstance(macro_snapshot.get("treasury_rates"), dict) else None,
                "regime": macro_snapshot.get("regime"),
            }

        if edgar_enrichment:
            portfolio_summary["edgar_fundamentals"] = edgar_enrichment

        elapsed = _time.time() - start
        _log(f"[PORTFOLIO_REVIEW] Portfolio context built ({elapsed:.1f}s)")

        from agent.data_compressor import compress_data
        try:
            compressed = compress_data(portfolio_summary)
        except Exception:
            compressed = portfolio_summary
        data_str = _json.dumps(compressed, default=str)

        _log(f"[PORTFOLIO_REVIEW] Sending {len(data_str):,} chars to Claude")

        time_remaining = DEADLINE - (_time.time() - start)
        claude_timeout = max(15.0, min(45.0, time_remaining - 2.0))
        _log(f"[PORTFOLIO_REVIEW] Claude timeout: {claude_timeout:.0f}s (elapsed: {_time.time()-start:.1f}s)")

        if claude_timeout < 10:
            _log(f"[PORTFOLIO_REVIEW] Not enough time for Claude, returning data summary")
            return _err_response("Portfolio review ran out of time gathering data. Please try again — cached data should make it faster on retry.")

        from agent.prompts import SYSTEM_PROMPT
        messages = [{
            "role": "user",
            "content": f"""[PORTFOLIO REVIEW]

{data_str}

Review my portfolio. For EACH position give:
- **VERDICT**: BUY MORE / HOLD / TRIM / SELL
- **THESIS** (2 sentences): Why? Reference price vs cost, P&L, fundamentals, TA (RSI, SMAs), sentiment
- **KEY RISK**: Single biggest risk
- **CATALYST**: Next catalyst
- **POSITION SIZE**: Current weight appropriate?

OVERALL:
- Portfolio grade (A-F)
- Concentration/correlation risk
- Macro alignment (Fear & Greed, VIX, regime)
- Top 1-2 action items this week
- One new ticker to add with brief thesis

Be direct. No disclaimers except one line at bottom. Keep response concise.

IMPORTANT: Return plain text analysis (not JSON). Be formatted with markdown headers and bullets.""",
        }]

        try:
            response = await asyncio.wait_for(
                asyncio.to_thread(
                    agent.client.messages.create,
                    model="claude-sonnet-4-20250514",
                    max_tokens=2000,
                    system=SYSTEM_PROMPT,
                    messages=messages,
                ),
                timeout=claude_timeout,
            )

            response_text = response.content[0].text.strip()
            _log(f"[PORTFOLIO_REVIEW] Claude responded: {len(response_text)} chars ({_time.time()-start:.1f}s total)")

            try:
                clean = response_text
                if clean.startswith("```"):
                    clean = clean.split("\n", 1)[1].rsplit("```", 1)[0].strip()
                parsed = _json.loads(clean)
                if "structured" in parsed:
                    return parsed
                return {
                    "type": "chat",
                    "analysis": parsed.get("message", response_text),
                    "structured": parsed,
                }
            except Exception:
                return {
                    "type": "chat",
                    "analysis": response_text,
                    "structured": {
                        "display_type": "chat",
                        "message": response_text,
                    },
                }

        except asyncio.TimeoutError:
            _log(f"[PORTFOLIO_REVIEW] Claude timed out after {claude_timeout:.0f}s")
            return _err_response("Portfolio review timed out waiting for AI analysis. Please try again — cached data should make it faster on retry.")
        except Exception as claude_err:
            _log(f"[PORTFOLIO_REVIEW] Claude error: {claude_err}")
            import traceback; traceback.print_exc()
            return _err_response(f"AI analysis failed: {str(claude_err)[:200]}")

    except Exception as e:
        import traceback
        traceback.print_exc()
        _log(f"[PORTFOLIO_REVIEW] FATAL ERROR: {e}")
        return _err_response(f"Portfolio review encountered an error: {str(e)[:200]}. Please try again.")


@app.get("/api/test-altfins")
@traceable(name="main.test_altfins")
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
_OPTIONS_SCREENER_TICKERS = [
    # ETFs — macro + sector flow
    "SPY", "QQQ", "IWM", "GLD", "TLT", "XLF", "XLK",
    # Stocks — highest-volume options universe
    "AAPL", "NVDA", "TSLA", "AMZN", "META", "MSFT", "AMD", "GOOGL", "NFLX", "COIN",
]

# High Growth tab: signal-rich seed tickers in $500M–$100B range
# These are NOT the only tickers scanned — the prefilter casts a wide net via
# Finviz mid-cap screens. These seeds just ensure coverage of known active names.
_OPTIONS_HIGH_GROWTH_SEEDS = [
    # Semis / hardware with active options flow
    "MRVL", "ON", "SMCI", "CRDO", "ALAB", "MCHP", "PSTG", "COHR",
    # Growth software / infra
    "SHOP", "NET", "DDOG", "SNOW", "CRWD", "ZS", "BILL", "CFLT",
    # Energy / clean
    "CEG", "FSLR", "BE", "EQT",
    # Space / defense
    "RKLB", "LUNR", "ASTS", "PL", "AVAV",
    # Crypto-adjacent / digital infra
    "COIN", "MARA", "CLSK", "HUT", "IONQ",
    # Biotech / health with options activity
    "HIMS", "RXRX", "KRYS", "VERA",
]

_OPTIONS_VALID_TABS = {"megacap", "high_growth"}

# Extended watchlist for Polygon historic data ingestion (runs at 5 calls/min)
# Imported from the ingestion module — used by the background pipeline
from data.options_ingestion import OPTIONS_WATCHLIST as _OPTIONS_FULL_WATCHLIST
from data.options_flow_engine import OptionsFlowEngine

_OPTIONS_PRECOMPUTE_INTERVAL = 1800  # 30 minutes — stock-side prefilter only
_OPTIONS_CACHE_TTL = 45              # short-lived page response cache
_OPTIONS_PREFILTER_CACHE_TTL = 3600  # 1 hour — proprietary stock-side data only


def _options_cache_key(tab: str) -> str:
    return f"options_screener_v3:{tab}"


def _options_prefilter_cache_key(tab: str) -> str:
    return f"options_screener_prefilter_v2:{tab}"


async def _options_precompute_loop():
    """
    Background screener loop for the Options Flow dashboard.
    Runs every 30 minutes. Precomputes ONLY stock-side/catalyst prefilter data
    from the supporting proprietary/free sources so the page can stay responsive.
    Precomputes BOTH megacap and high_growth tabs.
    """
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _init_event.wait, 120)

    if data_service is None:
        print("[OPTIONS_PRECOMPUTE] Data service not available, skipping loop")
        return

    import time as _time
    from data.cache import cache

    while True:
        for tab, seeds in [("megacap", _OPTIONS_SCREENER_TICKERS), ("high_growth", _OPTIONS_HIGH_GROWTH_SEEDS)]:
            try:
                print(f"[OPTIONS_PRECOMPUTE] [{tab}] Refreshing stock-side prefilter for {len(seeds)} seed tickers...")
                t0 = _time.time()

                prefilter_data = await OptionsFlowEngine(data_service).build_prefilter_snapshot(
                    seeds, tab=tab
                )

                elapsed = _time.time() - t0
                cache.set(_options_prefilter_cache_key(tab), prefilter_data, _OPTIONS_PREFILTER_CACHE_TTL)
                print(f"[OPTIONS_PRECOMPUTE] [{tab}] Cached {len(prefilter_data.get('candidates', []))} prefilter candidates in {elapsed:.1f}s.")

            except Exception as e:
                import traceback
                traceback.print_exc()
                print(f"[OPTIONS_PRECOMPUTE] [{tab}] Error: {e}")

        print(f"[OPTIONS_PRECOMPUTE] Both tabs refreshed. Next in {_OPTIONS_PRECOMPUTE_INTERVAL}s.")
        await asyncio.sleep(_OPTIONS_PRECOMPUTE_INTERVAL)


_OPTIONS_DEFAULT_TICKERS = _OPTIONS_SCREENER_TICKERS

# In-memory user overrides for high_growth scan defaults (reset on restart).
# Megacap tab defaults are NOT editable.
_HIGH_GROWTH_USER_OVERRIDES: dict = {}

# Keys the user is allowed to override for the high_growth tab
_EDITABLE_SCAN_KEYS = {
    "prefilter_target", "options_inspection_limit", "min_stock_price",
    "min_stock_liquidity", "high_growth_min_mcap", "high_growth_max_mcap",
    "relative_volume_threshold", "min_dte", "max_dte",
    "max_expirations_per_ticker",
}


@app.post("/api/options/dashboard")
@limiter.limit("60/minute")
@traceable(name="main.options_dashboard")
async def options_dashboard(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """
    Options flow screener — pure data endpoint, no Claude involved.
    Accepts optional JSON body {"tab": "megacap" | "high_growth"}.
    Returns a live Public.com options scan over a stock-side shortlist that is
    pre-fetched periodically in the background. Claude remains available via
    the chat bar instead of being part of this route.
    """
    await _wait_for_init()

    if not data_service or not data_service.public_com:
        return JSONResponse(
            status_code=503,
            content={"error": "Public.com options provider not configured. Set PUBLIC_COM_API_KEY in secrets."},
        )

    # Parse tab from request body (default: megacap)
    tab = "megacap"
    try:
        body = await request.json()
        if isinstance(body, dict) and body.get("tab") in _OPTIONS_VALID_TABS:
            tab = body["tab"]
    except Exception:
        pass  # No body or invalid JSON → default tab

    seed_tickers = _OPTIONS_HIGH_GROWTH_SEEDS if tab == "high_growth" else _OPTIONS_SCREENER_TICKERS

    import time as _time
    from data.cache import cache

    # ── Primary path: serve short-lived live-response cache ─────────────────
    cache_key = _options_cache_key(tab)
    cached = cache.get(cache_key)
    if cached:
        age = int(_time.time() - cached.get("cached_at", _time.time()))
        print(f"[OPTIONS_DASH] [{tab}] Cache hit (age={age}s, {len(cached.get('tickers', []))} tickers, {len(cached.get('all_contracts', []))} contracts)")
        return {
            "response": cached,
            "structured": True,
            "preset": "options_screener",
            "tab": tab,
            "available_tabs": sorted(_OPTIONS_VALID_TABS),
            "from_cache": True,
            "cache_age_seconds": age,
        }

    # ── Live page visit path: use cached prefilter, then call Public.com now ─
    prefilter_key = _options_prefilter_cache_key(tab)
    prefilter_snapshot = cache.get(prefilter_key)
    if prefilter_snapshot:
        pre_age = int(_time.time() - _dt.fromisoformat(prefilter_snapshot.get("generated_at")).timestamp()) if prefilter_snapshot.get("generated_at") else None
        print(f"[OPTIONS_DASH] [{tab}] Using prefilter cache (age={pre_age}s)")
    else:
        print(f"[OPTIONS_DASH] [{tab}] Prefilter cache cold — building stock-side shortlist now...")
    t0 = _time.time()

    async def _full_scan():
        overrides = _HIGH_GROWTH_USER_OVERRIDES if tab == "high_growth" and _HIGH_GROWTH_USER_OVERRIDES else None
        engine = OptionsFlowEngine(data_service, overrides=overrides)
        nonlocal prefilter_snapshot
        if not prefilter_snapshot:
            prefilter_snapshot = await engine.build_prefilter_snapshot(seed_tickers, tab=tab)
            cache.set(prefilter_key, prefilter_snapshot, _OPTIONS_PREFILTER_CACHE_TTL)

        screener_data = await engine.run_live_scan(
            seed_tickers,
            prefilter_snapshot=prefilter_snapshot,
            tab=tab,
        )

        # Keep legacy enrichment fields the page may already rely on.
        try:
            from data.options_history_store import get_latest_technicals, get_options_volume_summary
            for ticker_row in screener_data.get("tickers", []):
                sym = ticker_row.get("ticker", "")
                if not sym:
                    continue
                techs = get_latest_technicals(sym)
                if techs and len(techs) > 1:
                    ticker_row["technicals"] = techs
                vol_summary = get_options_volume_summary(sym, days=30)
                if vol_summary and vol_summary.get("call_total_volume"):
                    ticker_row["historic_volume"] = vol_summary
        except Exception as _enrich_err:
            print(f"[OPTIONS_DASH] [{tab}] Enrichment warning (non-fatal): {_enrich_err}")

        return screener_data

    try:
        screener_data = await asyncio.wait_for(_full_scan(), timeout=50)
        elapsed = _time.time() - t0

        result = {
            "display_type": "options_screener",
            "scan_type": "options_flow",
            "tab": tab,
            "cached_at": _time.time(),
            "tickers_scanned": seed_tickers,
            **screener_data,
        }
        cache.set(cache_key, result, _OPTIONS_CACHE_TTL)
        print(f"[OPTIONS_DASH] [{tab}] Live scan completed in {elapsed:.1f}s — {len(screener_data.get('tickers', []))} tickers")

        return {
            "response": result,
            "structured": True,
            "preset": "options_screener",
            "tab": tab,
            "available_tabs": sorted(_OPTIONS_VALID_TABS),
            "from_cache": False,
            "timing": {"total_seconds": round(elapsed, 1)},
        }

    except asyncio.TimeoutError:
        elapsed = _time.time() - t0
        print(f"[OPTIONS_DASH] [{tab}] Full scan timed out after {elapsed:.1f}s — returning empty shell")
        from data.options_flow_engine import OPTIONS_FLOW_DEFAULTS, OPTIONS_FLOW_WEIGHTS
        result = {
            "display_type": "options_screener",
            "scan_type": "options_flow",
            "tab": tab,
            "cached_at": _time.time(),
            "tickers_scanned": seed_tickers,
            "tickers": [],
            "all_contracts": [],
            "filter_defaults": dict(OPTIONS_FLOW_DEFAULTS),
            "score_weights": dict(OPTIONS_FLOW_WEIGHTS),
            "pipeline_stats": {
                "prefilter_candidate_count": len(prefilter_snapshot.get("candidates", [])) if prefilter_snapshot else 0,
                "options_inspection_count": 0,
                "ranked_result_count": 0,
                "degraded_sources": [f"timeout:scan_exceeded_{int(elapsed)}s"],
            },
            "market_summary": {"error": "Scan timed out. Data will appear after the background precompute cycle completes (~30s after server start)."},
        }
        return {
            "response": result,
            "structured": True,
            "preset": "options_screener",
            "tab": tab,
            "available_tabs": sorted(_OPTIONS_VALID_TABS),
            "from_cache": False,
            "timing": {"total_seconds": round(elapsed, 1), "timed_out": True},
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"[OPTIONS_DASH] [{tab}] Error: {e}")
        return JSONResponse(status_code=500, content={"error": f"Options screener error: {str(e)[:300]}"})


# ── OPTIONS FLOW — Agent chat query ─────────────────────────────────────

@app.post("/api/options/query")
@limiter.limit("10/minute")
@traceable(name="main.options_flow_query")
async def options_flow_query(
    request: Request,
    body: dict = Body(...),
    api_key: str = Header(None, alias="X-API-Key"),
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
    from config import ANTHROPIC_API_KEY
    import anthropic

    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        resp = client.messages.create(
            model="claude-sonnet-4-20250514",
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
@traceable(name="main.options_chain")
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
@traceable(name="main.options_expirations")
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
@traceable(name="main.options_history")
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
@traceable(name="main.options_volume_summary")
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
@traceable(name="main.options_technicals")
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
@traceable(name="main.options_data_coverage")
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
@traceable(name="main.options_fetch_progress")
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


# ── Scan Defaults (editable for high_growth tab only) ───────────────────

@app.get("/api/options/scan-defaults")
@limiter.limit("30/minute")
async def get_scan_defaults(
    request: Request,
    tab: str = "high_growth",
    api_key: str = Header(None, alias="X-API-Key"),
):
    """
    Get current scan defaults. For high_growth, returns user overrides merged
    with base defaults. For megacap, returns fixed defaults (not editable).
    """
    from data.options_flow_engine import OPTIONS_FLOW_DEFAULTS
    base = dict(OPTIONS_FLOW_DEFAULTS)

    if tab == "high_growth":
        merged = {**base, **{k: v for k, v in _HIGH_GROWTH_USER_OVERRIDES.items() if k in _EDITABLE_SCAN_KEYS}}
        return {
            "tab": "high_growth",
            "editable": True,
            "editable_keys": sorted(_EDITABLE_SCAN_KEYS),
            "defaults": merged,
            "user_overrides": dict(_HIGH_GROWTH_USER_OVERRIDES),
        }
    else:
        return {
            "tab": "megacap",
            "editable": False,
            "editable_keys": [],
            "defaults": base,
            "user_overrides": {},
        }


@app.put("/api/options/scan-defaults")
@limiter.limit("10/minute")
async def update_scan_defaults(
    request: Request,
    body: dict = Body(...),
    api_key: str = Header(None, alias="X-API-Key"),
):
    """
    Update scan defaults for the high_growth tab only.
    Send {"reset": true} to clear all overrides back to defaults.
    """
    from data.options_flow_engine import OPTIONS_FLOW_DEFAULTS

    tab = body.get("tab", "high_growth")
    if tab != "high_growth":
        return JSONResponse(
            status_code=400,
            content={"error": "Only high_growth tab defaults are editable. Megacap defaults are fixed."},
        )

    if body.get("reset"):
        _HIGH_GROWTH_USER_OVERRIDES.clear()
        from data.cache import cache
        cache.delete(_options_cache_key("high_growth"))
        return {"message": "High growth defaults reset to system defaults", "defaults": dict(OPTIONS_FLOW_DEFAULTS)}

    overrides = body.get("overrides", {})
    if not overrides:
        return JSONResponse(status_code=400, content={"error": "No overrides provided. Send {\"overrides\": {\"key\": value}}"})

    accepted = {}
    rejected = []
    for k, v in overrides.items():
        if k in _EDITABLE_SCAN_KEYS:
            try:
                base_type = type(OPTIONS_FLOW_DEFAULTS[k])
                _HIGH_GROWTH_USER_OVERRIDES[k] = base_type(v)
                accepted[k] = base_type(v)
            except (ValueError, TypeError) as e:
                rejected.append({"key": k, "error": str(e)})
        else:
            rejected.append({"key": k, "error": "Not an editable key"})

    from data.cache import cache
    cache.delete(_options_cache_key("high_growth"))

    merged = {**OPTIONS_FLOW_DEFAULTS, **_HIGH_GROWTH_USER_OVERRIDES}
    return {
        "message": f"Updated {len(accepted)} scan defaults for high_growth tab",
        "accepted": accepted,
        "rejected": rejected if rejected else None,
        "current_defaults": merged,
    }
