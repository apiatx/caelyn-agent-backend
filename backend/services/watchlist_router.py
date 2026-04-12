"""
Watchlist Router — FastAPI endpoints for multi-watchlist CRUD, news, refresh, and stock detail.
"""

from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import asyncio
import json as _json
import re as _re

import httpx
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict
from typing import Any, Dict, List, Optional

from services.watchlist_service import (
    save_watchlist,
    load_watchlist,
    list_watchlists,
    clear_watchlist,
    extract_tickers,
    fetch_news_for_tickers,
    get_stock_detail,
    _WATCHLIST_FILE,
)
from services.watchlist_analysis import run_analysis_pipeline

router = APIRouter(prefix="/api/watchlist", tags=["watchlist"])


# ── Request / Response Models ────────────────────────────────────────────────

class WatchlistSaveRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    csv_data: List[Dict[str, Any]]
    analysis: Dict[str, Any]
    watchlist_id: Optional[str] = None
    name: Optional[str] = None


class WatchlistAnalyzeRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    tickers: Optional[List[str]] = None
    csv_data: Optional[List[Dict[str, Any]]] = None
    watchlist_id: Optional[str] = None


# ── Helper ──────────────────────────────────────────────────────────────────

def _get_agent():
    import main
    if main.agent is None:
        raise HTTPException(status_code=503, detail="Server is still starting up.")
    return main.agent


def _get_data_service():
    import main
    if main.data_service is None:
        raise HTTPException(status_code=503, detail="Server is still starting up.")
    return main.data_service


# ── Endpoints — STATIC paths first, then parameterized ──────────────────────

@router.get("/list")
async def list_endpoint():
    """List all saved watchlists (metadata only)."""
    return list_watchlists()


@router.post("/save")
async def save_endpoint(body: WatchlistSaveRequest):
    """Save CSV data + AI analysis to the watchlist store."""
    result = save_watchlist(body.csv_data, body.analysis, body.watchlist_id, body.name)
    return result


@router.get("/debug")
async def debug_endpoint():
    """Debug endpoint — returns file path, existence, Postgres availability."""
    info: Dict[str, Any] = {
        "json_file_path": str(_WATCHLIST_FILE),
        "json_file_exists": _WATCHLIST_FILE.exists(),
    }
    try:
        from data.pg_storage import is_available, watchlist_list as pg_wl_list
        info["postgres_available"] = is_available()
        if is_available():
            entries = pg_wl_list()
            info["postgres_watchlist_count"] = len(entries)
            info["postgres_watchlists"] = entries
    except Exception as e:
        info["postgres_error"] = str(e)
    if _WATCHLIST_FILE.exists():
        try:
            content = _WATCHLIST_FILE.read_text()
            info["json_file_size_bytes"] = len(content)
            info["json_preview"] = content[:500]
        except Exception as e:
            info["json_read_error"] = str(e)
    return info


@router.get("/news")
async def news_endpoint():
    """Fetch fresh news for all tickers in the most recent watchlist."""
    store = load_watchlist()
    if store is None:
        return {}
    tickers = store.get("tickers", [])
    if not tickers:
        return {}
    return await fetch_news_for_tickers(tickers)


@router.post("/refresh")
async def refresh_endpoint():
    """Re-run multi-source parallel analysis on the most recent watchlist."""
    agent = _get_agent()
    data_service = _get_data_service()

    store = load_watchlist()
    if store is None:
        raise HTTPException(status_code=404, detail="No watchlist saved. Upload a CSV first.")

    tickers = store.get("tickers", [])
    csv_data = store.get("csv_data", [])
    if not tickers:
        raise HTTPException(status_code=400, detail="Watchlist has no tickers")

    result = await run_analysis_pipeline(tickers, csv_data, agent, data_service)

    if isinstance(result, dict) and result.get("error") and "sections" not in result:
        raise HTTPException(status_code=500, detail=result["error"])

    # Save updated analysis back to the watchlist
    store_id = store.get("id", "default")
    store_name = store.get("name", "Watchlist")
    save_watchlist(csv_data, result, watchlist_id=store_id, name=store_name)

    return result


@router.post("/analyze")
async def analyze_endpoint(body: WatchlistAnalyzeRequest):
    """
    Run multi-source parallel analysis pipeline on a watchlist.

    Accepts either:
    - tickers + csv_data directly in the request body
    - watchlist_id to load from Postgres

    Fires all 5 data sources in parallel (Grok, Gemini, Claude, SEC Edgar, TA),
    then synthesizes through Claude with intelligence rules.
    Returns 6 sections × 4 tickers structured JSON.
    """
    agent = _get_agent()
    data_service = _get_data_service()

    tickers = body.tickers or []
    csv_data = body.csv_data or []

    # If watchlist_id provided, load tickers and CSV from stored watchlist
    if body.watchlist_id:
        store = load_watchlist(body.watchlist_id)
        if store is None:
            raise HTTPException(status_code=404, detail="Watchlist not found")
        if not tickers:
            tickers = store.get("tickers", [])
        if not csv_data:
            csv_data = store.get("csv_data", [])
    elif not tickers and csv_data:
        # Extract tickers from CSV data
        tickers = extract_tickers(csv_data)

    if not tickers:
        raise HTTPException(
            status_code=400,
            detail="No tickers provided. Send tickers, csv_data, or watchlist_id.",
        )

    result = await run_analysis_pipeline(tickers, csv_data, agent, data_service)

    if isinstance(result, dict) and result.get("error") and "sections" not in result:
        raise HTTPException(status_code=500, detail=result["error"])

    return result


@router.get("")
async def get_endpoint():
    """Return the most recent saved watchlist, or {empty: true}."""
    store = load_watchlist()
    if store is None:
        return {"empty": True}
    return store


@router.delete("")
async def delete_endpoint():
    """Clear the most recent watchlist."""
    return clear_watchlist()


# ── Stock Deep-Dive ─────────────────────────────────────────────────────────

class StockDeepDiveRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    models: List[str] = ["grok", "gemini", "claude_gpt"]
    report_model: str = "claude"


@router.post("/stock/{ticker}/deep-dive")
async def stock_deep_dive_endpoint(ticker: str, body: StockDeepDiveRequest):
    """
    Multi-model parallel deep-dive for a single stock ticker.

    Phase 1 (parallel): Grok X/Twitter sentiment + Gemini Google/web intelligence
    Phase 2 (sequential): Claude or GPT synthesis using Phase 1 outputs as context
    """
    ticker = ticker.strip().upper()
    models = [m.strip().lower() for m in (body.models or ["grok", "gemini", "claude_gpt"])]
    report_model = (body.report_model or "claude").strip().lower()

    # ── Grok: X/Twitter real-time sentiment ──────────────────────────────────
    async def call_grok() -> str:
        xai_key = os.getenv("XAI_API_KEY") or os.getenv("GROK_API_KEY", "")
        if not xai_key:
            return "[No XAI_API_KEY configured]"
        prompt = (
            f"Search X (formerly Twitter) for real-time sentiment, trending mentions, and breaking "
            f"news about ${ticker} stock. Look for posts mentioning ${ticker}, #{ticker}, "
            f"and '{ticker} stock'. Summarize what the X/Twitter crowd is saying in 2-4 paragraphs. "
            f"Include notable bull and bear signals, key influencers or analysts mentioned, "
            f"and any breaking news or catalysts driving discussion."
        )
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                resp = await client.post(
                    "https://api.x.ai/v1/responses",
                    headers={
                        "Authorization": f"Bearer {xai_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": "grok-4-1-fast-non-reasoning",
                        "tools": [{"type": "x_search", "x_search": {}}],
                        "input": [{"role": "user", "content": prompt}],
                    },
                )
            if resp.status_code != 200:
                return f"[Grok API error {resp.status_code}: {resp.text[:200]}]"
            data = resp.json()
            for block in data.get("output", []):
                if block.get("type") == "message":
                    for c in block.get("content", []):
                        if c.get("type") == "output_text":
                            return c.get("text", "") or "[Grok: empty output_text]"
            return "[Grok: no text found in response]"
        except asyncio.TimeoutError:
            return "[Grok: timed out after 60s]"
        except Exception as e:
            return f"[Grok error: {type(e).__name__}: {e}]"

    # ── Gemini: Google/web analyst intelligence ───────────────────────────────
    async def call_gemini() -> str:
        gemini_key = os.getenv("GEMINI_API_KEY", "")
        if not gemini_key:
            return "[No GEMINI_API_KEY configured]"
        prompt = (
            f"Use Google Search to find the latest news, analyst upgrades/downgrades, price targets, "
            f"and web intelligence about {ticker} stock. Search for '{ticker} stock analysis', "
            f"'{ticker} analyst upgrade', '{ticker} earnings', and '{ticker} Google News'. "
            f"Return 2-4 paragraphs summarizing what analysts and the web are saying about {ticker}. "
            f"Include recent price targets, analyst ratings, news headlines, and any notable catalysts."
        )
        try:
            url = (
                "https://generativelanguage.googleapis.com/v1beta/models/"
                f"gemini-3-flash-preview:generateContent?key={gemini_key}"
            )
            async with httpx.AsyncClient(timeout=60.0) as client:
                resp = await client.post(
                    url,
                    headers={"Content-Type": "application/json"},
                    json={
                        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
                        "tools": [{"google_search": {}}],
                        "generationConfig": {"maxOutputTokens": 2048, "temperature": 0.3},
                    },
                )
            resp.raise_for_status()
            data = resp.json()
            candidates = data.get("candidates", [])
            if not candidates:
                return "[Gemini: no candidates in response]"
            parts = candidates[0].get("content", {}).get("parts", [])
            text = "".join(p.get("text", "") for p in parts if "text" in p)
            return text or "[Gemini: empty response]"
        except asyncio.TimeoutError:
            return "[Gemini: timed out after 60s]"
        except Exception as e:
            return f"[Gemini error: {type(e).__name__}: {e}]"

    # ── Synthesis prompt builder ──────────────────────────────────────────────
    def _build_synthesis_prompt(grok_text: str, gemini_text: str) -> str:
        context = ""
        if grok_text and not grok_text.startswith("["):
            context += f"\n\n=== X/Twitter Sentiment (Grok) ===\n{grok_text}"
        if gemini_text and not gemini_text.startswith("["):
            context += f"\n\n=== Web & Analyst Intelligence (Gemini) ===\n{gemini_text}"
        return (
            f"You are a senior equity analyst. Produce a structured deep-dive on {ticker} stock."
            f"{context}\n\n"
            f"Using the intelligence above (and your own knowledge), respond with ONLY a valid JSON "
            f"object containing exactly these keys:\n"
            f'  "summary": "2-3 sentence combined summary",\n'
            f'  "bull_case": "paragraph on the bull case",\n'
            f'  "bear_case": "paragraph on the bear case",\n'
            f'  "risk_factors": ["bullet 1", "bullet 2", "bullet 3"],\n'
            f'  "technical_outlook": "paragraph on the technical outlook",\n'
            f'  "analyst_sentiment": "paragraph summarizing analyst sentiment"\n\n'
            f"Return ONLY the JSON object. No markdown fences, no preamble, no commentary."
        )

    def _parse_synthesis(text: str) -> dict:
        m = _re.search(r'\{[\s\S]*\}', text)
        if m:
            try:
                return _json.loads(m.group())
            except Exception:
                pass
        return {"raw": text}

    # ── Claude synthesis ──────────────────────────────────────────────────────
    async def call_claude(grok_text: str, gemini_text: str) -> dict:
        anthropic_key = os.getenv("ANTHROPIC_API_KEY", "")
        if not anthropic_key:
            return {"error": "[No ANTHROPIC_API_KEY configured]"}
        prompt = _build_synthesis_prompt(grok_text, gemini_text)
        try:
            import anthropic as _anthropic
            client = _anthropic.AsyncAnthropic(api_key=anthropic_key, timeout=60.0)
            response = await client.messages.create(
                model="claude-opus-4-5",
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}],
            )
            text = "".join(b.text for b in response.content if hasattr(b, "text"))
            return _parse_synthesis(text)
        except asyncio.TimeoutError:
            return {"error": "[Claude: timed out after 60s]"}
        except Exception as e:
            return {"error": f"[Claude error: {type(e).__name__}: {e}]"}

    # ── GPT synthesis ─────────────────────────────────────────────────────────
    async def call_gpt(grok_text: str, gemini_text: str) -> dict:
        openai_key = os.getenv("OPENAI_API_KEY", "")
        if not openai_key:
            return {"error": "[No OPENAI_API_KEY configured]"}
        prompt = _build_synthesis_prompt(grok_text, gemini_text)
        try:
            from openai import AsyncOpenAI
            client = AsyncOpenAI(api_key=openai_key, timeout=60.0)
            resp = await client.chat.completions.create(
                model="gpt-4o",
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
            )
            text = resp.choices[0].message.content or "{}"
            return _json.loads(text)
        except asyncio.TimeoutError:
            return {"error": "[GPT: timed out after 60s]"}
        except Exception as e:
            return {"error": f"[GPT error: {type(e).__name__}: {e}]"}

    # ── Phase 1: run data-gathering models in parallel ────────────────────────
    phase1_coros: list = []
    phase1_keys: list = []

    if "grok" in models:
        phase1_coros.append(call_grok())
        phase1_keys.append("grok")
    if "gemini" in models:
        phase1_coros.append(call_gemini())
        phase1_keys.append("gemini")

    phase1_results = await asyncio.gather(*phase1_coros, return_exceptions=True) if phase1_coros else []

    result: Dict[str, Any] = {}
    grok_text = ""
    gemini_text = ""

    for key, val in zip(phase1_keys, phase1_results):
        if isinstance(val, Exception):
            val = f"[{key} exception: {val}]"
        result[key] = val
        if key == "grok":
            grok_text = val
        elif key == "gemini":
            gemini_text = val

    if "grok" not in result:
        result["grok"] = None
    if "gemini" not in result:
        result["gemini"] = None

    # ── Phase 2: synthesis (Claude or GPT) ───────────────────────────────────
    run_synthesis = "claude_gpt" in models or "claude" in models or "gpt" in models
    synthesis: dict = {}

    if run_synthesis and report_model == "gpt":
        synthesis = await call_gpt(grok_text, gemini_text)
        result["gpt"] = synthesis.pop("raw", None)
        result["claude"] = None
    elif run_synthesis:
        synthesis = await call_claude(grok_text, gemini_text)
        result["claude"] = synthesis.pop("raw", None)
        result["gpt"] = None
    else:
        result["claude"] = None
        result["gpt"] = None

    # Flatten synthesis fields into top-level keys
    result["summary"] = synthesis.get("summary", "")
    result["bull_case"] = synthesis.get("bull_case", "")
    result["bear_case"] = synthesis.get("bear_case", "")
    result["risk_factors"] = synthesis.get("risk_factors", [])
    result["technical_outlook"] = synthesis.get("technical_outlook", "")
    result["analyst_sentiment"] = synthesis.get("analyst_sentiment", "")

    if synthesis.get("error"):
        result["synthesis_error"] = synthesis["error"]

    print(f"[DEEP-DIVE] {ticker}: grok={'ok' if grok_text and not grok_text.startswith('[') else 'err'}, gemini={'ok' if gemini_text and not gemini_text.startswith('[') else 'err'}, report_model={report_model}")
    return result


# ── Parameterized endpoints (MUST be after static paths) ────────────────────

@router.patch("/{watchlist_id}/rename")
async def rename_endpoint(watchlist_id: str, body: dict):
    """Rename a specific watchlist."""
    new_name = body.get("name", "").strip()
    if not new_name:
        raise HTTPException(status_code=400, detail="Name is required")
    try:
        from data.pg_storage import watchlist_rename, is_available
        if is_available():
            ok = watchlist_rename(watchlist_id, new_name)
            if ok:
                return {"success": True, "name": new_name}
        return {"error": "Postgres unavailable"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{watchlist_id}")
async def get_by_id_endpoint(watchlist_id: str):
    """Return a specific watchlist by ID."""
    store = load_watchlist(watchlist_id)
    if store is None:
        return {"empty": True}
    return store


@router.post("/{watchlist_id}/analyze")
async def analyze_by_id_endpoint(watchlist_id: str):
    """Run multi-source parallel analysis pipeline for a specific watchlist."""
    agent = _get_agent()
    data_service = _get_data_service()

    store = load_watchlist(watchlist_id)
    if store is None:
        raise HTTPException(status_code=404, detail="Watchlist not found")

    tickers = store.get("tickers", [])
    csv_data = store.get("csv_data", [])

    if not tickers:
        raise HTTPException(status_code=400, detail="Watchlist has no tickers")

    result = await run_analysis_pipeline(tickers, csv_data, agent, data_service)

    if isinstance(result, dict) and result.get("error") and "sections" not in result:
        raise HTTPException(status_code=500, detail=result["error"])

    return result


@router.post("/{watchlist_id}/refresh")
async def refresh_by_id_endpoint(watchlist_id: str):
    """Re-run multi-source parallel analysis for a specific watchlist and return the updated watchlist record."""
    agent = _get_agent()
    data_service = _get_data_service()

    store = load_watchlist(watchlist_id)
    if store is None:
        raise HTTPException(status_code=404, detail="Watchlist not found")

    tickers = store.get("tickers", [])
    csv_data = store.get("csv_data", [])
    if not tickers:
        raise HTTPException(status_code=400, detail="Watchlist has no tickers")

    result = await run_analysis_pipeline(tickers, csv_data, agent, data_service)

    if isinstance(result, dict) and result.get("error") and "sections" not in result:
        raise HTTPException(status_code=500, detail=result["error"])

    # Save updated analysis back to the watchlist
    store_name = store.get("name", "Watchlist")
    save_watchlist(csv_data, result, watchlist_id=watchlist_id, name=store_name)

    # Return the full updated watchlist record so frontend can access id/name/analysis together
    updated = load_watchlist(watchlist_id)
    if updated:
        return updated
    # Fallback: compose inline if re-load fails
    from datetime import datetime, timezone
    return {
        "id": watchlist_id,
        "name": store_name,
        "tickers": tickers,
        "ticker_count": len(tickers),
        "csv_data": csv_data,
        "analysis": result,
        "saved_at": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/{watchlist_id}/news")
async def news_by_id_endpoint(watchlist_id: str):
    """Fetch news for a specific watchlist's tickers."""
    store = load_watchlist(watchlist_id)
    if store is None:
        return {}
    tickers = store.get("tickers", [])
    if not tickers:
        return {}
    return await fetch_news_for_tickers(tickers)


@router.get("/{watchlist_id}/stock/{ticker}")
async def stock_detail_by_id_endpoint(watchlist_id: str, ticker: str):
    """Return enriched data for a single ticker within a specific watchlist."""
    agent = _get_agent()
    result = await get_stock_detail(ticker, agent, watchlist_id=watchlist_id)
    if result.get("error"):
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.delete("/{watchlist_id}")
async def delete_by_id_endpoint(watchlist_id: str):
    """Delete a specific watchlist."""
    return clear_watchlist(watchlist_id)
