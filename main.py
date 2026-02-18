from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from pydantic import BaseModel, ConfigDict
from typing import List, Optional

import json as _json
import os
import uuid as _uuid
from datetime import datetime as _dt, timezone as _tz

from pathlib import Path

AGENT_API_KEY = os.getenv("AGENT_API_KEY")

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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

data_service = None
agent = None
_init_done = False

def _do_init():
    global data_service, agent, _init_done
    try:
        from config import ANTHROPIC_API_KEY, POLYGON_API_KEY, FMP_API_KEY, COINGECKO_API_KEY, CMC_API_KEY, ALTFINS_API_KEY, XAI_API_KEY, OPENAI_API_KEY
        from data.market_data_service import MarketDataService
        from agent.claude_agent import TradingAgent
        data_service = MarketDataService(polygon_key=POLYGON_API_KEY, fmp_key=FMP_API_KEY, coingecko_key=COINGECKO_API_KEY, cmc_key=CMC_API_KEY, altfins_key=ALTFINS_API_KEY, xai_key=XAI_API_KEY)
        agent = TradingAgent(api_key=ANTHROPIC_API_KEY, data_service=data_service, openai_api_key=OPENAI_API_KEY)
        _init_done = True
        print("[INIT] All services initialized successfully")
    except Exception as e:
        print(f"[INIT] ERROR during initialization: {e}")
        import traceback
        traceback.print_exc()
        _init_done = True

@app.on_event("startup")
async def startup_event():
    import threading
    threading.Thread(target=_do_init, daemon=True).start()

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


class QueryRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    query: Optional[str] = None
    prompt: Optional[str] = None
    conversation_id: Optional[str] = None
    preset_intent: Optional[str] = None

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

    picks = s.get("top_picks", [])
    if picks:
        equities = [p for p in picks if p.get("asset_class") in ("stock", "equities", "equity")]
        crypto = [p for p in picks if p.get("asset_class") in ("crypto", "cryptocurrency")]
        commodities = [p for p in picks if p.get("asset_class") in ("commodity", "commodities")]
        other = [p for p in picks if p not in equities and p not in crypto and p not in commodities]

        def _render_group(label, items):
            if not items:
                return
            parts.append(f"--- {label} ---")
            for p in items:
                ticker = p.get("ticker", "?")
                company = p.get("company", "")
                conv = p.get("conviction", "")
                score = p.get("conviction_score", "")
                change = p.get("change", "")
                mcap = p.get("market_cap", "")
                header = f"{ticker}"
                if company:
                    header += f" ({company})"
                detail_parts = []
                if conv:
                    detail_parts.append(f"Conviction: {conv}")
                if score:
                    detail_parts.append(f"Score: {score}")
                if change:
                    detail_parts.append(f"Change: {change}")
                if mcap:
                    detail_parts.append(f"MCap: {mcap}")
                if detail_parts:
                    header += " | " + " | ".join(detail_parts)
                parts.append(header)
                thesis = p.get("thesis", "")
                if thesis:
                    parts.append(f"  {thesis}")
                catalyst = p.get("catalyst", "")
                if catalyst:
                    parts.append(f"  Catalyst: {catalyst}")
                fail = p.get("why_could_fail", "")
                if fail:
                    parts.append(f"  Risk: {fail}")
                parts.append("")

        _render_group("EQUITIES", equities)
        _render_group("CRYPTO", crypto)
        _render_group("COMMODITIES", commodities)
        _render_group("OTHER", other)

    excluded = s.get("excluded_with_reason", [])
    if excluded:
        parts.append("EXCLUDED:")
        for ex in excluded:
            parts.append(f"  {ex.get('ticker', '?')} — {ex.get('reason', '')}")
        parts.append("")

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


_NARRATIVE_KEYS = ("summary", "narrative", "analysis", "report", "text", "message")
_RENDERERS = {
    "cross_market": _render_cross_market_analysis,
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
    print(f"[REQ] id={req_id} query_len={len(user_query)} preset={body.preset_intent} conversation_id={body.conversation_id}")

    meta = _build_meta(req_id, preset_intent=body.preset_intent, conv_id=body.conversation_id)

    if not api_key or api_key != AGENT_API_KEY:
        resp = _error_envelope("AUTH_FAILED", "Invalid or missing API key.", meta)
        _resp_log(req_id, 403, "error", resp)
        return JSONResponse(status_code=403, content=resp)

    try:
        await _wait_for_init()
    except HTTPException:
        resp = _error_envelope("SERVER_STARTING", "Server is still starting up. Please try again in a moment.", meta)
        _resp_log(req_id, 503, "error", resp)
        return JSONResponse(status_code=503, content=resp)

    if not user_query.strip() and not body.preset_intent:
        resp = _error_envelope("NO_QUERY", "No query provided. Send query or use preset_intent.", meta)
        _resp_log(req_id, 400, "error", resp)
        return JSONResponse(status_code=400, content=resp)

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

    try:
        result = await asyncio.wait_for(
            agent.handle_query(
                user_query,
                history=history,
                preset_intent=body.preset_intent,
                request_id=req_id,
            ),
            timeout=150.0,
        )

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
                               "opportunities", "ranked_candidates", "watchlist"}
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
                    updated_messages.append({"role": "assistant", "content": _json.dumps(resp, default=str)})
                    _save_msgs(conv_id, updated_messages)
                except Exception:
                    pass
            return JSONResponse(content=resp)

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
                    updated_messages.append({"role": "assistant", "content": _json.dumps(resp, default=str)})
                    _save_msgs(conv_id, updated_messages)
                except Exception:
                    pass
            return JSONResponse(content=resp)

        if conv_id:
            try:
                updated_messages = list(history)
                updated_messages.append({"role": "user", "content": user_query})
                updated_messages.append({"role": "assistant", "content": _json.dumps(result, default=str)})
                _save_msgs(conv_id, updated_messages)
            except Exception as e:
                print(f"[API] Failed to save conversation: {e}")

        meta["timing_ms"]["total"] = int((_time.time() - t0) * 1000)
        resp = _ok_envelope(result, meta)
        _resp_log(req_id, 200, "ok", resp)
        return JSONResponse(content=resp)

    except asyncio.TimeoutError:
        print(f"[API] request_id={req_id} status=timeout after 150s")
        meta["timing_ms"]["total"] = int((_time.time() - t0) * 1000)
        resp = _error_envelope(
            "REQUEST_TIMEOUT",
            "Request timed out. The data sources may be slow or rate-limited — please wait a minute and try again.",
            meta,
        )
        _resp_log(req_id, 200, "error", resp)
        return JSONResponse(content=resp)

    except Exception as e:
        import traceback
        print(f"[API] request_id={req_id} status=error error={e}")
        traceback.print_exc()
        meta["timing_ms"]["total"] = int((_time.time() - t0) * 1000)
        resp = _error_envelope(
            "UNHANDLED_EXCEPTION",
            f"Something went wrong: {str(e)}",
            meta,
        )
        _resp_log(req_id, 500, "error", resp)
        return JSONResponse(content=resp)


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

    return {
        "openai_orchestrator": openai_ok,
        "claude_reasoning": claude_ok,
        "finviz": finviz_ok,
        "stockanalysis": sa_ok,
        "errors": errors,
        "status": "ok" if (openai_ok and claude_ok and finviz_ok and sa_ok) else "degraded",
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

COMMODITY_SYMBOLS = {
    "SILVER": "SIUSD", "GOLD": "GCUSD", "OIL": "CLUSD", "CRUDE": "CLUSD",
    "NATGAS": "NGUSD", "COPPER": "HGUSD", "PLATINUM": "PLUSD",
    "PALLADIUM": "PAUSD", "WHEAT": "ZSUSD", "CORN": "ZCUSD",
}

INDEX_YAHOO_SYMBOLS = {
    "SPX": "^GSPC",
    "SPY": "SPY",
    "DJI": "^DJI",
    "IXIC": "^IXIC",
    "NDX": "^NDX",
    "QQQ": "QQQ",
    "RUT": "^RUT",
    "VIX": "^VIX",
    "DXY": "DX-Y.NYB",
}

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

    if not api_key or api_key != AGENT_API_KEY:
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

        # ---- INDICES: Yahoo index symbols ----
        if index_tickers:
            for ticker in index_tickers:
                yahoo_symbol = INDEX_YAHOO_SYMBOLS.get(ticker, ticker)
                try:
                    resp = await client.get(
                        "https://query1.finance.yahoo.com/v8/finance/chart/" + yahoo_symbol,
                        params={"interval": "1d", "range": "2d"},
                        headers={"User-Agent": "Mozilla/5.0"},
                    )
                    if resp.status_code == 200:
                        chart_data = resp.json()
                        result = chart_data.get("chart", {}).get("result", [])
                        if result:
                            meta = result[0].get("meta", {})
                            price = meta.get("regularMarketPrice", 0)
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
                            }
                            print(f"[PORTFOLIO] Index: {ticker} ({yahoo_symbol}) = ${price}")
                    else:
                        print(f"[PORTFOLIO] Yahoo index {ticker} ({yahoo_symbol}) returned {resp.status_code}")
                except Exception as e:
                    print(f"[PORTFOLIO] Index {ticker} ({yahoo_symbol}) error: {e}")

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

    await _wait_for_init()
    body = await request.json()
    print(f"[PORTFOLIO_REVIEW] === ENDPOINT HIT ===")
    print(f"[PORTFOLIO_REVIEW] Request keys: {list(body.keys())}")
    holdings = body.get("holdings", [])
    print(f"[PORTFOLIO_REVIEW] Holdings count: {len(holdings)}")

    if not holdings:
        print("[PORTFOLIO_REVIEW] No holdings found in request!")
        return {
            "type": "chat",
            "analysis": "",
            "structured": {
                "display_type": "chat",
                "message": "No holdings to review. Add some positions to your portfolio first.",
            },
        }

    print(f"[PORTFOLIO_REVIEW] Reviewing {len(holdings)} holdings: {[h.get('ticker') for h in holdings]}")

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