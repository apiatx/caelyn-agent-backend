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
    models: List[str] = ["grok", "gemini", "claude"]
    report_model: str = "claude"


@router.post("/stock/{ticker}/deep-dive")
async def stock_deep_dive_endpoint(ticker: str, body: StockDeepDiveRequest):
    """
    Three-model concurrent deep-dive for a single stock ticker.

    Phase 1 (all parallel): Grok X/Twitter sentiment + Gemini Google News +
                            Claude/GPT deep fundamental analysis
    Phase 2: short final synthesis call → structured JSON output
    """
    import traceback as _tb

    try:
        ticker = ticker.strip().upper()
        models = [m.strip().lower() for m in (body.models or ["grok", "gemini", "claude"])]
        report_model = (body.report_model or "claude").strip().lower()

        # ── Look up any stored CSV fundamentals for this ticker ───────────────
        def _get_fundamentals() -> str:
            try:
                store = load_watchlist()
                if store:
                    for row in store.get("csv_data", []):
                        sym = (row.get("Symbol") or row.get("symbol") or "").strip().upper()
                        if sym == ticker:
                            parts = []
                            for label, key in [
                                ("Price", "Stock Price"), ("MCap", "Market Cap"),
                                ("PE", "PE Ratio"), ("FwdPE", "Forward PE"),
                                ("RSI", "Relative Strength Index (RSI)"),
                                ("RevGrowth", "Revenue Growth (YoY)"),
                                ("EPSEst", "EPS Growth Est."), ("FCF", "FCF Margin"),
                                ("GrossMargin", "Gross Margin"), ("DE", "Debt / Equity"),
                                ("ShortFloat", "Short % Float"), ("EarningsDate", "Earnings Date"),
                            ]:
                                val = row.get(key, "")
                                if val:
                                    parts.append(f"{label}={val}")
                            return ", ".join(parts)
            except Exception:
                pass
            return ""

        fundamentals = _get_fundamentals()
        fundamentals_str = f" Fundamentals: {fundamentals}." if fundamentals else ""

        # ── Grok: X/Twitter real-time social sentiment ────────────────────────
        async def call_grok() -> str:
            xai_key = os.getenv("XAI_API_KEY") or os.getenv("GROK_API_KEY", "")
            if not xai_key:
                return "[No XAI_API_KEY configured]"
            prompt = (
                f"Search X/Twitter for recent posts about {ticker}. "
                f"Summarize the current retail and institutional sentiment, any viral catalysts "
                f"or concerns, and notable accounts discussing it. Be specific — mention price "
                f"targets, meme activity, earnings reactions if relevant. "
                f"2-4 paragraphs, conversational tone."
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
                    return f"[Grok API error {resp.status_code}: {resp.text[:300]}]"
                data = resp.json()
                for block in data.get("output", []):
                    if block.get("type") == "message":
                        for c in block.get("content", []):
                            if c.get("type") == "output_text":
                                return c.get("text", "") or "[Grok: empty output_text]"
                return "[Grok: no text in response]"
            except asyncio.TimeoutError:
                return "[Grok: timed out after 60s]"
            except Exception as exc:
                return f"[Grok error: {type(exc).__name__}: {exc}]"

        # ── Gemini: Google News headlines ─────────────────────────────────────
        async def call_gemini() -> str:
            gemini_key = os.getenv("GEMINI_API_KEY", "")
            if not gemini_key:
                return "[No GEMINI_API_KEY configured]"
            prompt = (
                f"Search Google News for {ticker} stock. Summarize the 3-5 most important "
                f"headlines from the last 30 days: analyst upgrades/downgrades, earnings surprises, "
                f"product launches, regulatory news, or macro headwinds. "
                f"Include the source and approximate date for each. "
                f"2-4 paragraphs."
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
                            "generationConfig": {"maxOutputTokens": 2048, "temperature": 0.2},
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
            except Exception as exc:
                return f"[Gemini error: {type(exc).__name__}: {exc}]"

        # ── Claude: deep fundamental + technical analysis ─────────────────────
        async def call_claude_analysis() -> str:
            anthropic_key = os.getenv("ANTHROPIC_API_KEY", "")
            if not anthropic_key:
                return "[No ANTHROPIC_API_KEY configured]"
            prompt = (
                f"You are a senior equity analyst.{fundamentals_str} "
                f"Given the fundamentals for {ticker}, provide a structured analysis covering: "
                f"(1) a 2-3 sentence executive summary, (2) the bull case, (3) the bear case, "
                f"(4) top 3 risk factors, (5) key technical levels/pattern to watch, "
                f"(6) what sell-side consensus looks like. "
                f"Write in clear, specific prose — cite actual metrics where possible."
            )
            try:
                import anthropic as _anthropic
                client = _anthropic.AsyncAnthropic(
                    api_key=anthropic_key, timeout=60.0
                )
                response = await client.messages.create(
                    model="claude-opus-4-5",
                    max_tokens=1500,
                    messages=[{"role": "user", "content": prompt}],
                )
                return "".join(b.text for b in response.content if hasattr(b, "text"))
            except asyncio.TimeoutError:
                return "[Claude: timed out after 60s]"
            except Exception as exc:
                return f"[Claude error: {type(exc).__name__}: {exc}]"

        # ── GPT: deep fundamental + technical analysis ────────────────────────
        async def call_gpt_analysis() -> str:
            openai_key = os.getenv("OPENAI_API_KEY", "")
            if not openai_key:
                return "[No OPENAI_API_KEY configured]"
            prompt = (
                f"You are a senior equity analyst.{fundamentals_str} "
                f"Given the fundamentals for {ticker}, provide a structured analysis covering: "
                f"(1) a 2-3 sentence executive summary, (2) the bull case, (3) the bear case, "
                f"(4) top 3 risk factors, (5) key technical levels/pattern to watch, "
                f"(6) what sell-side consensus looks like. "
                f"Write in clear, specific prose — cite actual metrics where possible."
            )
            try:
                from openai import AsyncOpenAI
                client = AsyncOpenAI(api_key=openai_key, timeout=60.0)
                resp = await client.chat.completions.create(
                    model="gpt-4o",
                    max_tokens=1500,
                    messages=[{"role": "user", "content": prompt}],
                )
                return resp.choices[0].message.content or "[GPT: empty response]"
            except asyncio.TimeoutError:
                return "[GPT: timed out after 60s]"
            except Exception as exc:
                return f"[GPT error: {type(exc).__name__}: {exc}]"

        # ── Phase 1: run ALL requested models concurrently ────────────────────
        coro_map: Dict[str, Any] = {}
        if "grok" in models:
            coro_map["grok"] = call_grok()
        if "gemini" in models:
            coro_map["gemini"] = call_gemini()
        # "claude" or "claude_gpt" in models → run the deep analysis persona
        run_claude = "claude" in models or "claude_gpt" in models
        run_gpt_model = "gpt" in models
        if run_claude and report_model != "gpt":
            coro_map["claude"] = call_claude_analysis()
        elif run_gpt_model or (run_claude and report_model == "gpt"):
            coro_map["gpt"] = call_gpt_analysis()

        keys = list(coro_map.keys())
        coros = list(coro_map.values())
        raw_results = await asyncio.gather(*coros, return_exceptions=True)

        model_outputs: Dict[str, str] = {}
        for k, v in zip(keys, raw_results):
            if isinstance(v, Exception):
                model_outputs[k] = f"[{k} exception: {v}]"
            else:
                model_outputs[k] = str(v)

        grok_text = model_outputs.get("grok", "")
        gemini_text = model_outputs.get("gemini", "")
        claude_text = model_outputs.get("claude", "")
        gpt_text = model_outputs.get("gpt", "")

        # ── Phase 2: final synthesis → structured JSON ────────────────────────
        synthesis_input_parts = []
        if grok_text and not grok_text.startswith("["):
            synthesis_input_parts.append(f"=== GROK (X/Twitter Sentiment) ===\n{grok_text}")
        if gemini_text and not gemini_text.startswith("["):
            synthesis_input_parts.append(f"=== GEMINI (Google News) ===\n{gemini_text}")
        analysis_text = claude_text or gpt_text
        if analysis_text and not analysis_text.startswith("["):
            synthesis_input_parts.append(f"=== ANALYST DEEP-DIVE ===\n{analysis_text}")

        synthesis_context = "\n\n".join(synthesis_input_parts)

        synthesis_prompt = (
            f"You are synthesizing a multi-source research report on {ticker}.{fundamentals_str}\n\n"
            f"{synthesis_context}\n\n"
            f"Based on all of the above, respond with ONLY a valid JSON object "
            f"(no markdown fences, no preamble) containing exactly these keys:\n"
            f'  "summary": "2-3 sentence executive summary",\n'
            f'  "bull_case": "bull case paragraph",\n'
            f'  "bear_case": "bear case paragraph",\n'
            f'  "risk_factors": ["risk 1", "risk 2", "risk 3"],\n'
            f'  "technical_outlook": "key technical levels and pattern to watch",\n'
            f'  "analyst_sentiment": "what sell-side analysts are saying"\n'
            f"Return ONLY the JSON object."
        )

        synthesis: dict = {}
        synth_key = os.getenv("ANTHROPIC_API_KEY", "")
        if report_model == "gpt" or not synth_key:
            openai_key = os.getenv("OPENAI_API_KEY", "")
            if openai_key:
                try:
                    from openai import AsyncOpenAI
                    _oa = AsyncOpenAI(api_key=openai_key, timeout=60.0)
                    _resp = await _oa.chat.completions.create(
                        model="gpt-4o",
                        max_tokens=1500,
                        messages=[{"role": "user", "content": synthesis_prompt}],
                        response_format={"type": "json_object"},
                    )
                    synthesis = _json.loads(_resp.choices[0].message.content or "{}")
                except Exception as exc:
                    print(f"[DEEP-DIVE] GPT synthesis failed: {exc}")
        if not synthesis and synth_key:
            try:
                import anthropic as _anthropic
                _ac = _anthropic.AsyncAnthropic(api_key=synth_key, timeout=60.0)
                _cr = await _ac.messages.create(
                    model="claude-opus-4-5",
                    max_tokens=1500,
                    messages=[{"role": "user", "content": synthesis_prompt}],
                )
                synth_text = "".join(b.text for b in _cr.content if hasattr(b, "text"))
                synth_text = _re.sub(r"```json\s*", "", synth_text)
                synth_text = _re.sub(r"```\s*", "", synth_text).strip()
                m = _re.search(r'\{[\s\S]*\}', synth_text)
                if m:
                    synthesis = _json.loads(m.group())
            except Exception as exc:
                print(f"[DEEP-DIVE] Claude synthesis failed: {exc}")

        print(
            f"[DEEP-DIVE] {ticker}: "
            f"grok={'ok' if grok_text and not grok_text.startswith('[') else 'skip/err'}, "
            f"gemini={'ok' if gemini_text and not gemini_text.startswith('[') else 'skip/err'}, "
            f"claude={'ok' if claude_text and not claude_text.startswith('[') else 'skip/err'}, "
            f"gpt={'ok' if gpt_text and not gpt_text.startswith('[') else 'skip/err'}"
        )

        return {
            "grok":              grok_text or None,
            "gemini":            gemini_text or None,
            "claude":            claude_text or None,
            "gpt":               gpt_text or None,
            "summary":           synthesis.get("summary", ""),
            "bull_case":         synthesis.get("bull_case", ""),
            "bear_case":         synthesis.get("bear_case", ""),
            "risk_factors":      synthesis.get("risk_factors", []),
            "technical_outlook": synthesis.get("technical_outlook", ""),
            "analyst_sentiment": synthesis.get("analyst_sentiment", ""),
        }

    except HTTPException:
        raise
    except Exception as e:
        _tb.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


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
    """
    Batched LLM analysis for any size watchlist.

    Splits tickers into batches of 22, runs up to 3 batches in parallel,
    collects sections, saves analysis, and returns the full updated watchlist.
    Works directly via the Anthropic API — no agent/data_service dependency.
    """
    from datetime import datetime, timezone

    try:
        store = load_watchlist(watchlist_id)
        if store is None:
            raise HTTPException(status_code=404, detail="Watchlist not found")

        tickers: list = store.get("tickers", [])
        csv_data: list = store.get("csv_data", [])
        store_name: str = store.get("name", "Watchlist")

        if not tickers:
            raise HTTPException(status_code=400, detail="Watchlist has no tickers")

        # Build Symbol → row lookup from csv_data
        csv_map: dict = {}
        for row in csv_data:
            sym = row.get("Symbol") or row.get("symbol") or row.get("Ticker") or ""
            if sym:
                csv_map[sym.strip().upper()] = row

        # ── Batch analysis via Claude ─────────────────────────────────────────
        BATCH_SIZE = 22
        batches = [tickers[i:i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]

        anthropic_key = os.getenv("ANTHROPIC_API_KEY", "")

        async def analyze_batch(batch_tickers: list, batch_index: int) -> dict | None:
            ticker_summaries = []
            for sym in batch_tickers:
                row = csv_map.get(sym.upper(), {})
                parts = [f"{sym}:"]
                for label, key in [
                    ("Price", "Stock Price"),
                    ("MCap", "Market Cap"),
                    ("PE", "PE Ratio"),
                    ("FwdPE", "Forward PE"),
                    ("RSI", "Relative Strength Index (RSI)"),
                    ("RevGrowth", "Revenue Growth (YoY)"),
                    ("EPSEst", "EPS Growth Est."),
                    ("FCF", "FCF Margin"),
                    ("GrossMargin", "Gross Margin"),
                    ("DE", "Debt / Equity"),
                ]:
                    val = row.get(key, "")
                    if val:
                        parts.append(f"{label}={val}")
                ticker_summaries.append(" ".join(parts))

            prompt = (
                f"Analyze these {len(batch_tickers)} stocks and return ONLY a valid JSON object "
                f"(no markdown fences, no extra text).\n\n"
                f"Stocks:\n" + "\n".join(ticker_summaries) + "\n\n"
                f'Return exactly this structure:\n'
                f'{{\n'
                f'  "id": "batch_{batch_index}",\n'
                f'  "title": "<thematic group name, e.g. AI Infrastructure Leaders>",\n'
                f'  "subtitle": "<one-line market context for this group>",\n'
                f'  "tickers": [\n'
                f'    {{\n'
                f'      "symbol": "<ticker>",\n'
                f'      "name": "<company name>",\n'
                f'      "price": <float or null>,\n'
                f'      "change_pct": <float or null>,\n'
                f'      "catalyst": "<key near-term catalyst>",\n'
                f'      "sentiment": "<bullish|neutral|bearish>",\n'
                f'      "action_note": "<specific actionable note>",\n'
                f'      "risk_level": "<low|medium|high>",\n'
                f'      "key_insight": "<single most important insight>",\n'
                f'      "technical_setup": "<technical pattern or level to watch>"\n'
                f'    }}\n'
                f'  ]\n'
                f'}}'
            )

            try:
                import anthropic as _anthropic
                client = _anthropic.AsyncAnthropic(api_key=anthropic_key, timeout=90.0)
                response = await client.messages.create(
                    model="claude-opus-4-5",
                    max_tokens=4000,
                    messages=[{"role": "user", "content": prompt}],
                )
                text = "".join(b.text for b in response.content if hasattr(b, "text"))
                # Strip markdown fences if present
                text = _re.sub(r"```json\s*", "", text)
                text = _re.sub(r"```\s*", "", text).strip()
                # Extract JSON object
                m = _re.search(r'\{[\s\S]*\}', text)
                if m:
                    parsed = _json.loads(m.group())
                    print(f"[REFRESH] Batch {batch_index}: {len(batch_tickers)} tickers → {len(parsed.get('tickers', []))} analyzed")
                    return parsed
                print(f"[REFRESH] Batch {batch_index}: no JSON found in response")
                return None
            except Exception as e:
                print(f"[REFRESH] Batch {batch_index} failed: {type(e).__name__}: {e}")
                return None

        # Run batches with max 3 in parallel to respect rate limits
        semaphore = asyncio.Semaphore(3)

        async def guarded_batch(batch_tickers: list, batch_index: int) -> dict | None:
            async with semaphore:
                return await analyze_batch(batch_tickers, batch_index)

        print(f"[REFRESH] {watchlist_id}: {len(tickers)} tickers → {len(batches)} batches")
        batch_tasks = [guarded_batch(b, i) for i, b in enumerate(batches)]
        batch_results = await asyncio.gather(*batch_tasks, return_exceptions=False)

        sections = [r for r in batch_results if r is not None]

        # ── Extract market themes from section titles ─────────────────────────
        market_themes: list = []
        seen: set = set()
        for section in sections:
            title = section.get("title", "")
            if title and title not in seen:
                market_themes.append(title)
                seen.add(title)
        market_themes = market_themes[:8]

        analysis = {
            "sections": sections,
            "market_themes": market_themes,
            "last_updated": datetime.now(timezone.utc).isoformat() + "Z",
        }

        # ── Persist analysis back to the watchlist ────────────────────────────
        try:
            save_watchlist(csv_data, analysis, watchlist_id=watchlist_id, name=store_name)
        except Exception as save_err:
            print(f"[REFRESH] Save failed (returning anyway): {save_err}")

        # ── Return the full updated watchlist record ──────────────────────────
        updated = load_watchlist(watchlist_id)
        if updated:
            return updated

        return {
            "id": watchlist_id,
            "name": store_name,
            "tickers": tickers,
            "ticker_count": len(tickers),
            "csv_data": csv_data,
            "analysis": analysis,
            "saved_at": datetime.now(timezone.utc).isoformat(),
        }

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Refresh failed: {e}")


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
