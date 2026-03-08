from fastapi import FastAPI, Request, Header, HTTPException, Body
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from pydantic import BaseModel, ConfigDict
from typing import List, Optional

import asyncio
import json as _json
import os
import uuid as _uuid
from datetime import datetime as _dt, timezone as _tz

from pathlib import Path

AGENT_API_KEY = os.getenv("AGENT_API_KEY")


def _jwt_or_key(request: Request, api_key) -> bool:
    """Return True if the request is authenticated via JWT middleware OR a valid API key."""
    if getattr(request.state, "user_id", None):
        return True
    return bool(api_key and api_key == AGENT_API_KEY)

# ── Auth middleware ──────────────────────────────────────────────
# Public paths that do NOT require a valid JWT token
_AUTH_PUBLIC_PATHS = {
    "/api/auth/login",
    "/api/auth/verify",
    "/api/auth/logout",
    "/",
    "/ping",
    "/health",
    "/docs",
    "/openapi.json",
    "/redoc",
}

class JWTAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        # Pass CORS preflight requests through so CORSMiddleware can handle them
        if request.method == "OPTIONS":
            return await call_next(request)
        # Allow public paths without auth
        if path in _AUTH_PUBLIC_PATHS or not path.startswith("/api/"):
            return await call_next(request)
        # Extract Bearer token
        auth_header = request.headers.get("Authorization", "")
        token = None
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
        if not token:
            return JSONResponse(status_code=401, content={"error": "Unauthorized", "detail": "Missing or invalid Authorization header."})
        try:
            from auth import verify_token
            payload = verify_token(token)
            request.state.user_id = payload.get("sub", "default")
        except Exception:
            return JSONResponse(status_code=401, content={"error": "Unauthorized", "detail": "Token expired or invalid."})
        return await call_next(request)

app = FastAPI(title="Trading Agent API")

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

# CORSMiddleware must be added LAST so it is outermost — it handles OPTIONS
# preflights before JWTAuthMiddleware can block them, and adds CORS headers
# to ALL responses (including 401s from JWT middleware).
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
        print(f"[AUTH] History migration error: {e}")

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

data_service = None
agent = None
_init_done = False

def _do_init():
    global data_service, agent, _init_done
    try:
        from config import ANTHROPIC_API_KEY, POLYGON_API_KEY, FMP_API_KEY, COINGECKO_API_KEY, CMC_API_KEY, ALTFINS_API_KEY, XAI_API_KEY, OPENAI_API_KEY, TWELVEDATA_API_KEY
        from data.market_data_service import MarketDataService
        from agent.claude_agent import TradingAgent
        data_service = MarketDataService(polygon_key=POLYGON_API_KEY, fmp_key=FMP_API_KEY, coingecko_key=COINGECKO_API_KEY, cmc_key=CMC_API_KEY, altfins_key=ALTFINS_API_KEY, xai_key=XAI_API_KEY, twelvedata_key=TWELVEDATA_API_KEY)
        agent = TradingAgent(api_key=ANTHROPIC_API_KEY, data_service=data_service, openai_api_key=OPENAI_API_KEY)
        _init_done = True
        print("[INIT] All services initialized successfully")
    except Exception as e:
        print(f"[INIT] ERROR during initialization: {e}")
        import traceback
        traceback.print_exc()
        _init_done = True

async def _briefing_precompute_loop():
    """
    Background precomputation for Daily Briefing.
    Runs every 30 minutes using free/unlimited APIs + one Perplexity web search
    for market news context. Caches Phase 1 data (screeners, macro, trending,
    news) so briefing requests are near-instant.
    """
    # Wait for init to complete
    for _ in range(120):
        if _init_done and data_service is not None:
            break
        await asyncio.sleep(1)

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

async def _smart_earnings_loop():
    """Background loop: runs smart earnings scan at 8am + 12pm EST on weekdays."""
    global _smart_scan_running
    # Wait for init
    for _ in range(120):
        if _init_done and data_service is not None:
            break
        await asyncio.sleep(1)

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


@app.on_event("startup")
async def startup_event():
    import threading
    threading.Thread(target=_do_init, daemon=True).start()
    # Launch background briefing precomputation (free APIs only)
    asyncio.create_task(_briefing_precompute_loop())
    # Launch smart earnings scanner (runs at 8am + 12pm EST on weekdays)
    asyncio.create_task(_smart_earnings_loop())

# ============================================================
# API Routes
# ============================================================


async def _wait_for_init():
    import asyncio
    for _ in range(60):
        if _init_done:
            return
        await asyncio.sleep(0.5)
    raise HTTPException(status_code=503, detail="Server is still starting up. Please try again in a moment.")

@app.get("/")
async def root():
    """Health check — visit this URL to confirm the backend is running."""
    return {"status": "running", "message": "Trading Agent API is live"}


@app.get("/ping")
async def ping():
    return {"status": "ok"}


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "init_complete": _init_done,
        "agent_loaded": agent is not None,
        "data_service_loaded": data_service is not None,
    }


# ============================================================
# Polymarket Gamma API Proxy
# ============================================================

@app.get("/api/polymarket/events")
@limiter.limit("30/minute")
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

def _build_meta(req_id: str, preset_intent=None, conv_id=None, routing=None, timing_ms=None):
    return {
        "request_id": req_id,
        "preset_intent": preset_intent,
        "conversation_id": conv_id,
        "routing": routing or {"source": "unknown", "confidence": "low", "category": "unknown"},
        "timing_ms": timing_ms or {"total": 0, "grok": 0, "data": 0, "claude": 0},
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
        parts.append(f"Scanned {scan_stats.get(chr(39)+'candidates_total'+chr(39), chr(39)+'?'+chr(39))} candidates")
    return chr(10).join(parts).strip()


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
    api_key: str = Header(None, alias="X-API-Key"),
):
    """Direct Grok/X query for the Social page — real-time X search via xAI."""
    query = body.get("query", "")
    if not query.strip():
        return JSONResponse(status_code=400, content={"error": "No query provided"})

    await _wait_for_init()
    if not data_service or not data_service.xai:
        return JSONResponse(status_code=503, content={"error": "xAI sentiment provider not initialized"})

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

    meta = _build_meta(req_id, preset_intent=body.preset_intent, conv_id=body.conversation_id)

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

    from data.chat_history import create_conversation, get_conversation, save_messages as _save_msgs

    conv_id = body.conversation_id
    history = []

    if conv_id:
        conv = get_conversation(conv_id)
        if conv and conv.get("messages"):
            history = conv["messages"]
        elif conv is None:
            print(f"[API] Conversation {conv_id} not found, creating new one")
            conv_id = None

    if not conv_id:
        try:
            conv = create_conversation(user_query)
            conv_id = conv["id"]
        except Exception as e:
            print(f"[API] Failed to create conversation: {e}")
            conv_id = None

    meta["conversation_id"] = conv_id

    print(f"[API] request_id={req_id} query={user_query[:100]}, history_turns={len(history)}, conv_id={conv_id}")

    async def _stream_query():
        """
        Runs the query and streams keepalive spaces every 8s.
        Prevents Replit proxy from killing connections on slow queries (Grok, investments).
        Frontend strips leading whitespace before JSON.parse() — no frontend logic change needed.
        Final payload is always a single valid JSON object.
        """
        import json as _j

        task = asyncio.create_task(
            agent.handle_query(
                user_query,
                history=history,
                preset_intent=body.preset_intent,
                request_id=req_id,
                csv_data=body.csv_data,
                chatbox_mode=body.chatbox_mode or False,
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
                        updated_messages = list(history)
                        updated_messages.append({"role": "user", "content": user_query})
                        _asst_content = resp.get("analysis", "") or _json.dumps(resp, default=str)[:8000]
                        updated_messages.append({"role": "assistant", "content": _asst_content})
                        _save_msgs(conv_id, updated_messages)
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
                        updated_messages = list(history)
                        updated_messages.append({"role": "user", "content": user_query})
                        _asst_content2 = resp.get("analysis", "") or _json.dumps(resp, default=str)[:8000]
                        updated_messages.append({"role": "assistant", "content": _asst_content2})
                        _save_msgs(conv_id, updated_messages)
                    except Exception:
                        pass
                yield _j.dumps(resp).encode()
                return

            if conv_id:
                try:
                    updated_messages = list(history)
                    updated_messages.append({"role": "user", "content": user_query})
                    _asst_content3 = result.get("analysis", "") if isinstance(result, dict) else ""
                    if not _asst_content3:
                        _asst_content3 = _json.dumps(result, default=str)[:8000]
                    updated_messages.append({"role": "assistant", "content": _asst_content3})
                    _save_msgs(conv_id, updated_messages)
                except Exception as e:
                    print(f"[API] Failed to save conversation: {e}")

            meta["timing_ms"]["total"] = int((_time.time() - t0) * 1000)
            resp = _ok_envelope(result, meta)
            _resp_log(req_id, 200, "ok", resp)
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

    return StreamingResponse(
        _stream_query(),
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
    tickers: List[str]
    conversation_id: Optional[str] = None

@app.post("/api/watchlist")
@limiter.limit("10/minute")
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
    print(f"[API] Watchlist review request: {tickers}")

    try:
        result = await asyncio.wait_for(
            agent.review_watchlist(tickers),
            timeout=90.0,
        )

        if body.conversation_id:
            try:
                from data.chat_history import save_messages as _save2
                _save2(body.conversation_id, [
                    {"role": "user", "content": f"Review my watchlist: {', '.join(tickers)}"},
                    {"role": "assistant", "content": result.get("analysis", "") if isinstance(result, dict) else _json.dumps(result, default=str)[:8000]},
                ])
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

@app.get("/api/conversations")
@limiter.limit("30/minute")
async def get_conversations(request: Request):
    from data.chat_history import list_conversations
    return {"conversations": list_conversations()}

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

@app.get("/api/history")
@limiter.limit("30/minute")
async def get_history(request: Request):
    from data.prompt_history import get_all
    user_id = getattr(request.state, "user_id", "default")
    return get_all(user_id=user_id)

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
    entry = save_response(category, intent, content, display_type, user_id=user_id)
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

@app.get("/api/health")
@limiter.limit("30/minute")
async def health_check(request: Request):
    """Full diagnostic — tests Claude, Finviz, and StockAnalysis."""
    import asyncio
    await _wait_for_init()
    errors = []

    openai_ok = False
    if agent.openai_client:
        try:
            response = await asyncio.wait_for(
                asyncio.to_thread(
                    agent.openai_client.chat.completions.create,
                    model="gpt-4o",
                    max_tokens=20,
                    messages=[{"role": "user", "content": "Say ok"}],
                ),
                timeout=15.0,
            )
            openai_ok = True
        except Exception as e:
            errors.append(f"OpenAI Orchestrator: {str(e)}")
    else:
        errors.append("OpenAI Orchestrator: No API key configured")

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
        "openai_orchestrator": openai_ok,
        "claude_reasoning": claude_ok,
        "finviz": finviz_ok,
        "stockanalysis": sa_ok,
        "edgar": edgar_health,
        "errors": errors,
        "status": "ok" if (openai_ok and claude_ok and finviz_ok and sa_ok) else "degraded",
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


# ============================================================
# Portfolio Holdings CRUD
# ============================================================

def _portfolio_file(user_id: str) -> Path:
    return Path(f"data/portfolio_holdings_{user_id}.json")


@app.get("/api/portfolio/holdings")
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
                for ticker in stocks_needing_sector:
                    sector_cache_key = f"sector:{ticker}"
                    cached_sector = _cache.get(sector_cache_key)
                    if cached_sector is not None:
                        quotes[ticker]["sector"] = cached_sector.get("sector", "Other")
                        quotes[ticker]["industry"] = cached_sector.get("industry", "")
                        quotes[ticker]["company_name"] = cached_sector.get("company_name", "")
                        continue
                    try:
                        profile_resp = await client.get(
                            "https://financialmodelingprep.com/stable/profile",
                            params={"symbol": ticker, "apikey": FMP_API_KEY},
                        )
                        if profile_resp.status_code == 200:
                            profile_data = profile_resp.json()
                            if isinstance(profile_data, list) and len(profile_data) > 0:
                                sector = profile_data[0].get("sector", "")
                                industry = profile_data[0].get("industry", "")
                                company_name = profile_data[0].get("companyName", "")
                                if sector:
                                    quotes[ticker]["sector"] = sector
                                    quotes[ticker]["industry"] = industry
                                    quotes[ticker]["company_name"] = company_name
                                    _cache.set(sector_cache_key, {"sector": sector, "industry": industry, "company_name": company_name}, 86400)
                                else:
                                    quotes[ticker]["sector"] = "Other"
                            else:
                                quotes[ticker]["sector"] = "Other"
                    except Exception as e:
                        print(f"[PORTFOLIO] FMP profile {ticker} error: {e}")
                        quotes[ticker]["sector"] = "Other"

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

        # ---- COMMODITIES: FMP commodity symbols ----
        if commodity_tickers:
            for ticker in commodity_tickers:
                fmp_symbol = COMMODITY_SYMBOLS.get(ticker)
                if fmp_symbol:
                    try:
                        resp = await client.get(
                            "https://financialmodelingprep.com/stable/quote-short",
                            params={"symbol": fmp_symbol, "apikey": FMP_API_KEY},
                        )
                        if resp.status_code == 200:
                            items = resp.json()
                            if items:
                                item = items[0]
                                quotes[ticker] = {
                                    "price": item.get("price"),
                                    "change": item.get("change"),
                                    "change_pct": item.get("changesPercentage"),
                                    "volume": item.get("volume"),
                                    "source": "fmp_commodity",
                                    "asset_type": "commodity",
                                    "sector": "Commodities",
                                }
                                print(f"[PORTFOLIO] Commodity: {ticker} = ${item.get('price')}")
                    except Exception as e:
                        print(f"[PORTFOLIO] Commodity {ticker} error: {e}")
                else:
                    print(f"[PORTFOLIO] No commodity symbol mapping for: {ticker}")

        missing_tickers = [t for t in tickers if t not in quotes]
        if missing_tickers:
            print(f"[PORTFOLIO] Fallback for unresolved tickers: {missing_tickers}")

            for ticker in list(missing_tickers):
                if ticker in quotes:
                    continue
                fmp_symbol = COMMODITY_SYMBOLS.get(ticker)
                if fmp_symbol:
                    try:
                        resp = await client.get(
                            "https://financialmodelingprep.com/stable/quote-short",
                            params={"symbol": fmp_symbol, "apikey": FMP_API_KEY},
                        )
                        if resp.status_code == 200:
                            items = resp.json()
                            if items:
                                item = items[0]
                                quotes[ticker] = {
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
async def review_portfolio(request: Request, api_key: str = Header(None, alias="X-API-Key")):
    """AI Portfolio Review — comprehensive analysis with Buy/Hold/Sell verdicts."""
    import asyncio
    import sys
    import time as _time
    from fastapi.responses import JSONResponse

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

        async def _fetch_all_tickers():
            ticker_asset_map = {h["ticker"]: h.get("asset_type", "stock") for h in holdings_context}
            ticker_tasks = [fetch_ticker_data(t, ticker_asset_map.get(t, "stock")) for t in tickers[:15]]
            return await asyncio.gather(*ticker_tasks, return_exceptions=True)

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