from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from pydantic import BaseModel
from typing import List, Optional

import json as _json

from pathlib import Path

from config import ANTHROPIC_API_KEY, POLYGON_API_KEY, AGENT_API_KEY, FMP_API_KEY, COINGECKO_API_KEY, CMC_API_KEY
from data.market_data_service import MarketDataService
from agent.claude_agent import TradingAgent
from data.chat_history import (
    create_conversation, save_messages, get_conversation,
    list_conversations, delete_conversation,
)

# ============================================================
# Initialize the app
# ============================================================
app = FastAPI(title="Trading Agent API")

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# CORS: Allow your website to call this backend.
# IMPORTANT: Replace these URLs with YOUR actual website URLs.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# Wire up the services
# ============================================================
data_service = MarketDataService(polygon_key=POLYGON_API_KEY, fmp_key=FMP_API_KEY, coingecko_key=COINGECKO_API_KEY, cmc_key=CMC_API_KEY)
agent = TradingAgent(api_key=ANTHROPIC_API_KEY, data_service=data_service)

# ============================================================
# API Routes
# ============================================================


@app.get("/")
async def root():
    """Health check — visit this URL to confirm the backend is running."""
    return {"status": "running", "message": "Trading Agent API is live"}


async def verify_api_key(x_api_key: Optional[str] = Header(None)):
    """Verify the API key sent in the X-API-Key header."""
    if not x_api_key:
        raise HTTPException(
            status_code=401,
            detail="Missing API key. Include X-API-Key header.",
        )
    if x_api_key != AGENT_API_KEY:
        raise HTTPException(
            status_code=403,
            detail="Invalid API key.",
        )
    return x_api_key


class Message(BaseModel):
    role: str
    content: str

class QueryRequest(BaseModel):
    query: Optional[str] = None
    prompt: Optional[str] = None
    conversation_history: Optional[List[Message]] = []
    history: Optional[List[Message]] = None
    conversation_id: Optional[str] = None

@app.post("/api/query")
@limiter.limit("10/minute")
async def query_agent(
    request: Request,
    body: QueryRequest,
    api_key: str = Header(None, alias="X-API-Key"),
):
    import asyncio
    if not api_key or api_key != AGENT_API_KEY:
        raise HTTPException(
            status_code=403,
            detail="Invalid or missing API key.",
        )
    user_query = body.query or body.prompt or ""
    if not user_query.strip():
        raise HTTPException(status_code=400, detail="No query provided. Send 'query' or 'prompt' field.")
    hist_source = body.conversation_history if body.conversation_history else (body.history if body.history else [])
    history = [h.dict() for h in hist_source] if hist_source else []
    print(f"[API] Received query: query={user_query[:100]}, history_turns={len(history)}")
    try:
        result = await asyncio.wait_for(
            agent.handle_query(
                user_query,
                history=history,
            ),
            timeout=90.0,
        )

        if body.conversation_id:
            try:
                updated_messages = list(history)
                updated_messages.append({"role": "user", "content": user_query})
                updated_messages.append({"role": "assistant", "content": _json.dumps(result, default=str)})
                save_messages(body.conversation_id, updated_messages)
            except Exception as e:
                print(f"[API] Failed to save conversation: {e}")

        return result
    except asyncio.TimeoutError:
        print("[API] Request timed out after 90s")
        return {
            "type": "chat",
            "analysis": "",
            "structured": {
                "display_type": "chat",
                "message": "Request timed out after 90 seconds. The data sources may be slow — please try again.",
            },
        }
    except Exception as e:
        import traceback
        print(f"[API] Error in /api/query: {e}")
        traceback.print_exc()
        return {
            "type": "chat",
            "analysis": "",
            "structured": {
                "display_type": "chat",
                "message": f"Something went wrong: {str(e)}",
            },
        }


@app.post("/api/cache/clear")
@limiter.limit("5/minute")
async def clear_cache(request: Request, api_key: str = Header(None, alias="X-API-Key")):
    if not api_key or api_key != AGENT_API_KEY:
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
    if not api_key or api_key != AGENT_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")
    if not body.tickers:
        raise HTTPException(status_code=400, detail="No tickers provided.")

    tickers = [t.strip().upper() for t in body.tickers if t.strip()][:25]
    print(f"[API] Watchlist review request: {tickers}")

    try:
        result = await asyncio.wait_for(
            agent.review_watchlist(tickers),
            timeout=90.0,
        )

        if body.conversation_id:
            try:
                save_messages(body.conversation_id, [
                    {"role": "user", "content": f"Review my watchlist: {', '.join(tickers)}"},
                    {"role": "assistant", "content": _json.dumps(result, default=str)},
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
    return {"conversations": list_conversations()}

@app.get("/api/conversations/{conv_id}")
@limiter.limit("30/minute")
async def get_conversation_detail(request: Request, conv_id: str):
    conv = get_conversation(conv_id)
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return conv

@app.post("/api/conversations")
@limiter.limit("30/minute")
async def create_new_conversation(request: Request, body: CreateConversationRequest):
    conv = create_conversation(body.first_query)
    return conv

@app.put("/api/conversations/{conv_id}")
@limiter.limit("30/minute")
async def update_conversation(request: Request, conv_id: str, body: UpdateConversationRequest):
    success = save_messages(conv_id, body.messages)
    if not success:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return {"success": success}

@app.delete("/api/conversations/{conv_id}")
@limiter.limit("30/minute")
async def delete_conv(request: Request, conv_id: str):
    success = delete_conversation(conv_id)
    return {"success": success}

@app.get("/api/health")
@limiter.limit("30/minute")
async def health_check(request: Request):
    """Full diagnostic — tests Claude, Finviz, and StockAnalysis."""
    import asyncio
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
        errors.append(f"Claude API: {str(e)}")

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

    return {
        "claude_api": claude_ok,
        "finviz": finviz_ok,
        "stockanalysis": sa_ok,
        "errors": errors,
        "status": "ok" if (claude_ok and finviz_ok and sa_ok) else "degraded",
    }


# ============================================================
# Portfolio Holdings CRUD
# ============================================================

PORTFOLIO_FILE = Path("data/portfolio_holdings.json")


@app.get("/api/portfolio/holdings")
async def get_holdings(api_key: str = Header(None, alias="X-API-Key")):
    """Return saved portfolio holdings (JSON file, same approach as chat history)."""
    if not api_key or api_key != AGENT_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")
    if not PORTFOLIO_FILE.exists():
        return {"holdings": []}
    try:
        with open(PORTFOLIO_FILE) as f:
            data = _json.load(f)
        if isinstance(data, dict) and "holdings" in data:
            return data
        return {"holdings": []}
    except Exception:
        return {"holdings": []}


@app.post("/api/portfolio/holdings")
async def save_holdings(request: Request, api_key: str = Header(None, alias="X-API-Key")):
    """Save portfolio holdings. Expects {holdings: [{ticker, shares, avg_cost, ...}]}."""
    if not api_key or api_key != AGENT_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")
    body = await request.json()
    if not isinstance(body, dict) or "holdings" not in body:
        raise HTTPException(status_code=400, detail="Body must be {holdings: [...]}")
    if not isinstance(body["holdings"], list):
        raise HTTPException(status_code=400, detail="holdings must be a list")
    PORTFOLIO_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(PORTFOLIO_FILE, "w") as f:
        _json.dump(body, f)
    return {"success": True}


# ============================================================
# Portfolio Quotes (batch price lookup)
# ============================================================

@app.post("/api/portfolio/quotes")
async def get_portfolio_quotes(request: Request, api_key: str = Header(None, alias="X-API-Key")):
    """Get current quotes for a list of tickers — 1 API call for all tickers."""
    import httpx

    if not api_key or api_key != AGENT_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")

    body = await request.json()
    tickers = body.get("tickers", [])
    print(f"[PORTFOLIO] Quotes requested for: {tickers}")

    if not tickers:
        print("[PORTFOLIO] No tickers provided")
        return {"quotes": {}}

    import asyncio

    ticker_str = ",".join(tickers[:25])

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            full_resp = await client.get(
                "https://financialmodelingprep.com/stable/quote",
                params={"symbol": ticker_str, "apikey": FMP_API_KEY},
            )

            if full_resp.status_code == 200:
                data = full_resp.json()
                print(f"[PORTFOLIO] FMP stable/quote returned {len(data)} quotes")
            else:
                print(f"[PORTFOLIO] stable/quote returned {full_resp.status_code}, fetching individual quote-short")
                async def fetch_one(c, sym):
                    try:
                        r = await c.get(
                            "https://financialmodelingprep.com/stable/quote-short",
                            params={"symbol": sym, "apikey": FMP_API_KEY},
                        )
                        if r.status_code == 200:
                            items = r.json()
                            return items[0] if items else None
                    except Exception:
                        pass
                    return None

                results = await asyncio.gather(*[fetch_one(client, t) for t in tickers[:25]])
                data = [r for r in results if r]
                print(f"[PORTFOLIO] FMP quote-short returned {len(data)} quotes (individual calls)")

        quotes = {}
        for item in data:
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
                "earnings_date": item.get("earningsAnnouncement"),
                "sector": item.get("sector", ""),
            }

        print(f"[PORTFOLIO] Returning quotes for: {list(quotes.keys())}")
        return {"quotes": quotes}

    except Exception as e:
        print(f"[PORTFOLIO] Error: {e}")
        import traceback
        traceback.print_exc()
        return {"quotes": {}, "error": str(e)}


# ============================================================
# Portfolio Events (earnings + dividends for holdings)
# ============================================================

@app.get("/api/portfolio/events")
async def get_portfolio_events(api_key: str = Header(None, alias="X-API-Key")):
    """Get upcoming earnings and dividend dates for portfolio holdings."""
    import httpx
    from datetime import datetime, timedelta

    if not api_key or api_key != AGENT_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")

    if not PORTFOLIO_FILE.exists():
        return {"events": []}
    try:
        with open(PORTFOLIO_FILE) as f:
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
    """AI Portfolio Review — takes holdings with cost basis, returns Buy/Hold/Sell for each."""
    import asyncio
    import httpx

    if not api_key or api_key != AGENT_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")

    body = await request.json()
    holdings = body.get("holdings", [])

    if not holdings:
        return {
            "type": "chat",
            "analysis": "",
            "structured": {
                "display_type": "chat",
                "message": "No holdings to review. Add some positions to your portfolio first.",
            },
        }

    print(f"[PORTFOLIO_REVIEW] Reviewing {len(holdings)} holdings")

    holdings_text = ""
    for h in holdings:
        ticker = h.get("ticker", "")
        shares = h.get("shares", 0)
        avg_cost = h.get("avg_cost", 0) or h.get("avgCost", 0)
        holdings_text += f"- {ticker}: {shares} shares @ ${avg_cost} avg cost\n"

    tickers = [h.get("ticker", "").upper() for h in holdings if h.get("ticker")]

    ticker_data = {}
    for ticker in tickers[:25]:
        data_item = {"ticker": ticker}

        try:
            overview = await asyncio.wait_for(
                agent.data.stockanalysis.get_overview(ticker),
                timeout=6.0,
            )
            if overview:
                data_item.update(overview)
        except Exception as e:
            print(f"[PORTFOLIO_REVIEW] {ticker} overview failed: {e}")

        try:
            sentiment = await asyncio.wait_for(
                agent.data.stocktwits.get_sentiment(ticker),
                timeout=5.0,
            )
            if sentiment:
                data_item["social_sentiment"] = sentiment
        except Exception:
            pass

        try:
            if agent.data.fmp:
                news = await asyncio.wait_for(
                    agent.data.fmp.get_stock_news(ticker, limit=3),
                    timeout=5.0,
                )
                if news:
                    data_item["recent_news"] = news
        except Exception:
            pass

        ticker_data[ticker] = data_item
        await asyncio.sleep(0.3)

    print(f"[PORTFOLIO_REVIEW] Data gathered for {len(ticker_data)} tickers")

    from agent.data_compressor import compress_data
    compressed = compress_data({"portfolio_data": ticker_data})
    data_str = _json.dumps(compressed, default=str)

    from agent.prompts import SYSTEM_PROMPT
    messages = [{
        "role": "user",
        "content": f"""[PORTFOLIO HOLDINGS]
{holdings_text}

[MARKET DATA FOR HOLDINGS]
{data_str}

[REQUEST]
Review my portfolio and give me a clear VERDICT for each position. For EACH holding provide:

1. **VERDICT**: BUY MORE / HOLD / TRIM / SELL — be decisive, pick one
2. **REASONING** (2-3 sentences max): Why this verdict? Reference specific data — recent news, sentiment shift, fundamental trend, technical setup, or valuation concern
3. **KEY RISK**: The single biggest risk to this position right now
4. **CATALYST**: The next potential catalyst (earnings date, product launch, sector trend, macro event)

Then provide an OVERALL PORTFOLIO ASSESSMENT:
- Portfolio grade (A through F)
- Biggest strength
- Biggest weakness
- Concentration risk (are positions too correlated?)
- Top 1-2 action items I should take this week
- If you had to add ONE new position to improve this portfolio, what would it be and why?

Be direct. Be opinionated. No hedge-everything disclaimers in the body — just one disclaimer at the very bottom.

IMPORTANT: Respond with display_type "chat" and put your full analysis in the "message" field as formatted text.""",
    }]

    try:
        response = await asyncio.wait_for(
            asyncio.to_thread(
                agent.client.messages.create,
                model="claude-sonnet-4-20250514",
                max_tokens=4096,
                system=SYSTEM_PROMPT,
                messages=messages,
            ),
            timeout=60.0,
        )

        response_text = response.content[0].text.strip()
        print(f"[PORTFOLIO_REVIEW] Claude responded: {len(response_text)} chars")

        try:
            if response_text.startswith("```"):
                response_text = response_text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
            parsed = _json.loads(response_text)
            if "structured" in parsed:
                return parsed
            return {
                "type": "chat",
                "analysis": parsed.get("message", response_text),
                "structured": parsed,
            }
        except _json.JSONDecodeError:
            return {
                "type": "chat",
                "analysis": response_text,
                "structured": {
                    "display_type": "chat",
                    "message": response_text,
                },
            }

    except asyncio.TimeoutError:
        return {
            "type": "chat",
            "analysis": "",
            "structured": {
                "display_type": "chat",
                "message": "Portfolio review timed out. Try with fewer holdings.",
            },
        }
    except Exception as e:
        print(f"[PORTFOLIO_REVIEW] Error: {e}")
        import traceback
        traceback.print_exc()
        return {
            "type": "chat",
            "analysis": "",
            "structured": {
                "display_type": "chat",
                "message": f"Error reviewing portfolio: {str(e)}",
            },
        }