"""
Multi-Source Parallel Watchlist Analysis Pipeline

Fires ALL data sources simultaneously (Promise.all equivalent):
  a) Grok/xAI — social sentiment & investing themes on X
  b) Gemini — web search for real-time news per ticker
  c) Claude — CSV fundamental analysis
  d) SEC Edgar — recent filings & insider activity
  e) Technical Analysis — reuses existing TA signal engine

Then synthesizes everything through Claude with intelligence rules
to produce exactly 6 sections of 4 tickers each.
"""

from __future__ import annotations

import asyncio
import csv
import io
import json

from agent.model_policy import MODEL_GROK
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx

try:
    from langsmith import traceable
except ImportError:
    def traceable(*args, **kwargs):
        def _noop(fn):
            return fn
        if args and callable(args[0]):
            return args[0]
        return _noop


# ── Helper: parse AI JSON ──────────────────────────────────────────────────

def _parse_ai_json(raw_text: str) -> Dict[str, Any]:
    """Parse AI response as JSON, handling markdown code fences."""
    try:
        return json.loads(raw_text)
    except json.JSONDecodeError:
        cleaned = re.sub(r"```json\s*", "", raw_text)
        cleaned = re.sub(r"```\s*", "", cleaned).strip()
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            brace_start = cleaned.find("{")
            if brace_start != -1:
                depth = 0
                for i in range(brace_start, len(cleaned)):
                    if cleaned[i] == "{":
                        depth += 1
                    elif cleaned[i] == "}":
                        depth -= 1
                        if depth == 0:
                            try:
                                return json.loads(cleaned[brace_start:i + 1])
                            except json.JSONDecodeError:
                                break
                            break
            return {"error": "Failed to parse AI response", "raw": raw_text[:2000]}


# ═══════════════════════════════════════════════════════════════════════════
# SOURCE A: Grok/xAI — Social Sentiment & Investing Themes on X
# ═══════════════════════════════════════════════════════════════════════════

@traceable(name="watchlist_analysis.grok_social_sentiment")
async def _collect_grok_sentiment(tickers: List[str]) -> Dict[str, Any]:
    """
    Call Grok for major investing themes on X and per-ticker social sentiment.
    Batches tickers in groups of 10 if >10 tickers.
    Skips gracefully if XAI_API_KEY / GROK_API_KEY not available.
    """
    api_key = os.environ.get("XAI_API_KEY") or os.environ.get("GROK_API_KEY", "")
    if not api_key:
        print("[WATCHLIST-ANALYSIS] No XAI_API_KEY or GROK_API_KEY — skipping Grok sentiment")
        return {"skipped": True, "reason": "No API key configured for Grok/xAI"}

    batch_size = 10
    batches = [tickers[i:i + batch_size] for i in range(0, len(tickers), batch_size)]
    print(f"[WATCHLIST-ANALYSIS] Grok: scanning {len(tickers)} tickers in {len(batches)} batches")

    all_results = []
    themes_data = None

    async def _scan_batch(batch: List[str], batch_idx: int) -> Dict[str, Any]:
        tickers_str = ", ".join([f"${t}" for t in batch])
        prompt = f"""Search X/Twitter for recent posts (last 48 hours) about these stocks: {tickers_str}

For EACH ticker, analyze:
1. Social sentiment on X (bullish/bearish/neutral/mixed)
2. Volume of discussion (high/medium/low) and whether it's increasing
3. Key narratives being discussed (catalysts, sector themes, technical setups)
4. Quality of discussion (smart money analysis vs retail hype vs bot noise)
5. Notable signals: insider buying discussion, institutional accumulation, EBITDA inflection, bottleneck positioning

Also identify the MAJOR INVESTING THEMES currently trending on X with momentum.

Return ONLY valid JSON:
{{
    "investing_themes": [
        {{"theme": "theme name", "momentum": "surging/rising/stable/fading", "key_tickers": ["SYM1", "SYM2"], "narrative": "1-2 sentence description"}}
    ],
    "ticker_sentiment": [
        {{
            "ticker": "SYM",
            "sentiment": "bullish/bearish/neutral/mixed",
            "sentiment_score": -1.0 to 1.0,
            "buzz_level": "high/medium/low",
            "buzz_trend": "surging/rising/stable/declining",
            "key_narratives": ["narrative1", "narrative2"],
            "catalysts_discussed": ["catalyst1"],
            "smart_money_signals": "description or null",
            "risk_flags": ["flag1"],
            "summary": "1-2 sentence summary of what X is saying"
        }}
    ]
}}"""

        try:
            async with httpx.AsyncClient(timeout=90.0) as client:
                response = await client.post(
                    "https://api.x.ai/v1/responses",
                    headers={
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {api_key}",
                    },
                    json={
                        "model": MODEL_GROK,
                        "tools": [{"type": "x_search", "x_search": {}}],
                        "input": [{"role": "user", "content": prompt}],
                    },
                )
            if response.status_code != 200:
                print(f"[WATCHLIST-ANALYSIS] Grok batch {batch_idx} error: {response.status_code}")
                return {"error": f"Grok API returned {response.status_code}"}

            data = response.json()
            # Extract text from responses API format
            text = ""
            for item in data.get("output", []):
                if item.get("type") == "message":
                    for block in item.get("content", []):
                        if block.get("type") in ("output_text", "text"):
                            text += block.get("text", "")

            if not text:
                return {"error": "No text in Grok response"}

            return _parse_ai_json(text)
        except Exception as e:
            print(f"[WATCHLIST-ANALYSIS] Grok batch {batch_idx} exception: {e}")
            return {"error": str(e)}

    tasks = [_scan_batch(batch, i) for i, batch in enumerate(batches)]
    batch_results = await asyncio.gather(*tasks, return_exceptions=True)

    # Aggregate results
    all_ticker_sentiment = []
    all_themes = []
    for i, result in enumerate(batch_results):
        if isinstance(result, Exception):
            print(f"[WATCHLIST-ANALYSIS] Grok batch {i} failed: {result}")
            continue
        if isinstance(result, dict) and "error" not in result:
            all_ticker_sentiment.extend(result.get("ticker_sentiment", []))
            all_themes.extend(result.get("investing_themes", []))

    print(f"[WATCHLIST-ANALYSIS] Grok: got sentiment for {len(all_ticker_sentiment)} tickers, {len(all_themes)} themes")
    return {
        "investing_themes": all_themes,
        "ticker_sentiment": {
            item.get("ticker", ""): item for item in all_ticker_sentiment if isinstance(item, dict)
        },
    }


# ═══════════════════════════════════════════════════════════════════════════
# SOURCE B: Gemini — Web Search for Real-Time News
# ═══════════════════════════════════════════════════════════════════════════

@traceable(name="watchlist_analysis.gemini_news")
async def _collect_gemini_news(tickers: List[str]) -> Dict[str, Any]:
    """
    Call Gemini with grounding/web search for real-time news per ticker.
    Batches if needed. Skips gracefully if GEMINI_API_KEY not available.
    """
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        print("[WATCHLIST-ANALYSIS] No GEMINI_API_KEY — skipping Gemini news")
        return {"skipped": True, "reason": "No API key configured for Gemini"}

    batch_size = 10
    batches = [tickers[i:i + batch_size] for i in range(0, len(tickers), batch_size)]
    print(f"[WATCHLIST-ANALYSIS] Gemini: searching news for {len(tickers)} tickers in {len(batches)} batches")

    async def _search_batch(batch: List[str], batch_idx: int) -> Dict[str, Any]:
        tickers_str = ", ".join(batch)
        prompt = f"""For each of these stock tickers, search the web for the most recent and important news, catalysts, and developments from the past 2 weeks: {tickers_str}

For EACH ticker, provide:
1. Recent news headlines and their significance
2. Upcoming catalysts (earnings dates, FDA decisions, contract announcements, product launches)
3. Recent partnerships, acquisitions, or major business developments
4. Analyst upgrades/downgrades
5. Any sector-level news affecting these stocks

Return ONLY valid JSON:
{{
    "ticker_news": [
        {{
            "ticker": "SYM",
            "headlines": [
                {{"title": "headline", "date": "YYYY-MM-DD", "significance": "high/medium/low", "summary": "1 sentence"}}
            ],
            "upcoming_catalysts": [
                {{"event": "description", "expected_date": "approximate date", "potential_impact": "high/medium/low"}}
            ],
            "recent_developments": "2-3 sentence summary of key recent developments",
            "analyst_sentiment": "bullish/bearish/neutral/mixed",
            "sector_context": "1 sentence on sector-level trends affecting this stock"
        }}
    ]
}}"""

        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(
                    f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={api_key}",
                    headers={"Content-Type": "application/json"},
                    json={
                        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
                        "generationConfig": {"maxOutputTokens": 8192},
                        "tools": [{"google_search": {}}],
                    },
                )
            if response.status_code != 200:
                print(f"[WATCHLIST-ANALYSIS] Gemini batch {batch_idx} error: {response.status_code}: {response.text[:200]}")
                return {"error": f"Gemini API returned {response.status_code}"}

            data = response.json()
            candidates = data.get("candidates", [])
            if not candidates:
                return {"error": "No candidates in Gemini response"}

            parts = candidates[0].get("content", {}).get("parts", [])
            text = "".join(p.get("text", "") for p in parts if "text" in p)
            if not text:
                return {"error": "No text in Gemini response"}

            return _parse_ai_json(text)
        except Exception as e:
            print(f"[WATCHLIST-ANALYSIS] Gemini batch {batch_idx} exception: {e}")
            return {"error": str(e)}

    tasks = [_search_batch(batch, i) for i, batch in enumerate(batches)]
    batch_results = await asyncio.gather(*tasks, return_exceptions=True)

    # Aggregate
    all_ticker_news = {}
    for i, result in enumerate(batch_results):
        if isinstance(result, Exception):
            print(f"[WATCHLIST-ANALYSIS] Gemini batch {i} failed: {result}")
            continue
        if isinstance(result, dict) and "error" not in result:
            for item in result.get("ticker_news", []):
                if isinstance(item, dict) and item.get("ticker"):
                    all_ticker_news[item["ticker"]] = item

    print(f"[WATCHLIST-ANALYSIS] Gemini: got news for {len(all_ticker_news)} tickers")
    return {"ticker_news": all_ticker_news}


# ═══════════════════════════════════════════════════════════════════════════
# SOURCE C: Claude — CSV Fundamental Analysis
# ═══════════════════════════════════════════════════════════════════════════

@traceable(name="watchlist_analysis.claude_csv_analysis")
async def _collect_claude_csv_analysis(csv_data: List[Dict[str, str]], agent: Any) -> Dict[str, Any]:
    """
    Use existing Claude integration to parse CSV fundamental data.
    CRITICAL: Instructs Claude that declining revenue alone is NOT bearish
    if there are massive contracts/pivots in pipeline.
    """
    if not csv_data:
        return {"skipped": True, "reason": "No CSV data provided"}

    csv_table = "\n".join(
        ", ".join(f"{k}: {v}" for k, v in row.items() if v)
        for row in csv_data[:200]
    )
    columns = list(csv_data[0].keys()) if csv_data else []

    prompt = f"""You are analyzing a stock watchlist CSV for an active trader. Extract and analyze fundamental data.

SPREADSHEET ({len(csv_data)} stocks):
Columns: {', '.join(columns)}

{csv_table}

CRITICAL ANALYSIS RULES:
1. Declining revenue alone does NOT mean "avoid" — if a company has massive contracts in the pipeline (hyperscaler orders, government contracts, enterprise deals), declining current revenue with a growing pipeline is BULLISH.
2. Companies pivoting business models (e.g., Bitcoin mining → AI infrastructure) should be evaluated on their NEW trajectory, not their old metrics.
3. Cash burn in a high-growth company with a clear path to profitability or EBITDA inflection is NOT automatically bearish.
4. Look for: revenue trends, gross/operating margins, cash position, debt levels, growth rates, and any signs of business model transformation.

For each stock in the CSV, extract:
- Current financial metrics from the CSV data
- Revenue trajectory (growing/stable/declining)
- Margin profile (expanding/stable/compressing)
- Cash vs debt position (strong/adequate/weak)
- Growth rate classification (hypergrowth >50%, high 25-50%, moderate 10-25%, slow <10%, declining)
- Any signs of business model pivot or transformation
- Overall fundamental assessment (strong/moderate/weak/transforming)

Return ONLY valid JSON:
{{
    "fundamental_analysis": [
        {{
            "ticker": "SYM",
            "revenue_trend": "growing/stable/declining",
            "revenue_notes": "context about revenue trajectory",
            "margin_profile": "expanding/stable/compressing",
            "cash_position": "strong/adequate/weak",
            "debt_level": "low/moderate/high",
            "growth_rate": "hypergrowth/high/moderate/slow/declining",
            "pivot_detected": true/false,
            "pivot_description": "description if pivot detected, null otherwise",
            "fundamental_score": 1-10,
            "assessment": "strong/moderate/weak/transforming",
            "key_insight": "1-2 sentence key fundamental insight for a trader"
        }}
    ]
}}"""

    try:
        raw_text = await asyncio.wait_for(
            asyncio.to_thread(agent._call_simple_model, "claude", prompt, 16384),
            timeout=90.0,
        )
        result = _parse_ai_json(raw_text)
        # Index by ticker
        analysis_map = {}
        for item in result.get("fundamental_analysis", []):
            if isinstance(item, dict) and item.get("ticker"):
                analysis_map[item["ticker"]] = item
        print(f"[WATCHLIST-ANALYSIS] Claude CSV: analyzed {len(analysis_map)} tickers")
        return {"fundamental_analysis": analysis_map}
    except Exception as e:
        print(f"[WATCHLIST-ANALYSIS] Claude CSV analysis failed: {e}")
        return {"error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════
# SOURCE D: SEC Edgar — Recent Filings & Insider Activity
# ═══════════════════════════════════════════════════════════════════════════

@traceable(name="watchlist_analysis.sec_edgar_filings")
async def _collect_sec_edgar(tickers: List[str]) -> Dict[str, Any]:
    """
    Hit SEC EDGAR for recent filings and insider transactions.
    No API key needed — just proper User-Agent header.
    """
    from data.edgar_provider import EdgarProvider
    edgar = EdgarProvider()

    results = {}
    sem = asyncio.Semaphore(3)  # Rate limit SEC requests

    async def _fetch_ticker(ticker: str) -> None:
        async with sem:
            try:
                # Get recent filings (10-K, 10-Q, 8-K)
                filings = await edgar.get_recent_filings(ticker)

                # Get insider transactions (Form 4)
                insider_filings = await edgar.get_insider_filings(ticker)

                # Separate filing types
                ten_k = [f for f in filings if f.get("form_type") == "10-K"][:2]
                ten_q = [f for f in filings if f.get("form_type") == "10-Q"][:2]
                eight_k = [f for f in filings if f.get("form_type") == "8-K"][:3]
                form_4 = [f for f in insider_filings if not f.get("error")][:5]

                results[ticker] = {
                    "recent_10k": ten_k,
                    "recent_10q": ten_q,
                    "recent_8k": eight_k,
                    "insider_transactions": form_4,
                    "has_recent_insider_buying": len(form_4) > 0,
                    "filing_count": len(filings),
                }
            except Exception as e:
                print(f"[WATCHLIST-ANALYSIS] Edgar failed for {ticker}: {e}")
                results[ticker] = {"error": str(e)}

            # Small delay to respect SEC rate limits
            await asyncio.sleep(0.2)

    tasks = [_fetch_ticker(t) for t in tickers]
    await asyncio.gather(*tasks, return_exceptions=True)

    print(f"[WATCHLIST-ANALYSIS] SEC Edgar: got filings for {len(results)} tickers")
    return {"edgar_data": results}


# ═══════════════════════════════════════════════════════════════════════════
# SOURCE E: Technical Analysis — Reuse Existing TA Signal Engine
# ═══════════════════════════════════════════════════════════════════════════

@traceable(name="watchlist_analysis.technical_analysis")
async def _collect_technical_analysis(tickers: List[str], data_service: Any) -> Dict[str, Any]:
    """
    Use the existing TA signal engine and MarketDataService to get
    technical analysis for each ticker: price, MAs, RSI, MACD, volume,
    trend identification, and dip-in-uptrend detection.
    """
    from core.ta_signal_engine import analyze_bars

    results = {}
    sem = asyncio.Semaphore(5)

    async def _analyze_ticker(ticker: str) -> None:
        async with sem:
            try:
                # Get candle data (120 days for proper MA calculation)
                bars = await data_service.get_candles(ticker, days=200)
                if not bars or len(bars) < 20:
                    results[ticker] = {"error": "Insufficient candle data"}
                    return

                # Run the full TA signal engine
                ta_result = analyze_bars(bars, ticker=ticker)
                if not ta_result:
                    results[ticker] = {"error": "TA analysis returned no results"}
                    return

                # Add dip-in-uptrend detection
                price = float(ta_result.get("price", "0").replace("$", ""))
                rsi = ta_result.get("rsi")
                sma_50 = None
                sma_200 = None

                # Extract from indicator signals
                for sig in ta_result.get("ta_signals", []):
                    if sig["name"] == "price_above_sma50":
                        sma_50_match = re.search(r'SMA50 \$([0-9.]+)', sig.get("evidence", ""))
                        if sma_50_match:
                            sma_50 = float(sma_50_match.group(1))
                    if sig["name"] == "sma50_above_sma200":
                        sma_200_match = re.search(r'SMA200 \$([0-9.]+)', sig.get("evidence", ""))
                        if sma_200_match:
                            sma_200 = float(sma_200_match.group(1))

                # Detect dip in uptrend
                is_dip_in_uptrend = False
                dip_details = None
                bullish_signals = [s for s in ta_result.get("ta_signals", []) if s["direction"] == "bullish"]
                bearish_signals = [s for s in ta_result.get("ta_signals", []) if s["direction"] == "bearish"]

                # Dip in uptrend = overall trend bullish (SMA50 > SMA200 or Stage 2)
                # but RSI pulling back or price near support
                has_uptrend = any(
                    s["name"] in ("sma50_above_sma200", "stage2_uptrend", "price_above_sma50")
                    for s in bullish_signals
                )
                has_pullback = (rsi is not None and rsi < 45) or any(
                    s["name"] in ("price_below_sma50",) for s in bearish_signals
                )
                vol_declining = ta_result.get("volume_ratio", 1.0) < 1.0

                if has_uptrend and (has_pullback or vol_declining):
                    is_dip_in_uptrend = True
                    dip_details = (
                        f"RSI={rsi:.1f}" if rsi else "RSI N/A"
                    ) + f", vol_ratio={ta_result.get('volume_ratio', 'N/A')}"

                results[ticker] = {
                    "price": ta_result.get("price"),
                    "change": ta_result.get("change"),
                    "rsi": rsi,
                    "ta_score": ta_result.get("technical_score"),
                    "setup_type": ta_result.get("setup_type"),
                    "pattern": ta_result.get("pattern"),
                    "action": ta_result.get("action"),
                    "confidence_score": ta_result.get("confidence_score"),
                    "entry": ta_result.get("entry"),
                    "stop": ta_result.get("stop"),
                    "targets": ta_result.get("targets"),
                    "risk_reward": ta_result.get("risk_reward"),
                    "volume_ratio": ta_result.get("volume_ratio"),
                    "indicator_signals": ta_result.get("indicator_signals"),
                    "signals_stacking": ta_result.get("signals_stacking"),
                    "is_dip_in_uptrend": is_dip_in_uptrend,
                    "dip_details": dip_details,
                    "trend": "uptrend" if has_uptrend and not has_pullback else (
                        "dip_in_uptrend" if is_dip_in_uptrend else (
                            "downtrend" if len(bearish_signals) > len(bullish_signals) else "consolidation"
                        )
                    ),
                }
            except Exception as e:
                print(f"[WATCHLIST-ANALYSIS] TA failed for {ticker}: {e}")
                results[ticker] = {"error": str(e)}

    tasks = [_analyze_ticker(t) for t in tickers]
    await asyncio.gather(*tasks, return_exceptions=True)

    print(f"[WATCHLIST-ANALYSIS] TA: analyzed {len(results)} tickers")
    return {"technical_analysis": results}


# ═══════════════════════════════════════════════════════════════════════════
# SYNTHESIS: Intelligent Categorization via Claude
# ═══════════════════════════════════════════════════════════════════════════

def _build_synthesis_prompt(
    tickers: List[str],
    grok_data: Dict[str, Any],
    gemini_data: Dict[str, Any],
    claude_csv_data: Dict[str, Any],
    edgar_data: Dict[str, Any],
    ta_data: Dict[str, Any],
) -> str:
    """Build the master synthesis prompt for Claude."""

    # Format Grok social data
    grok_section = "=== SOURCE: X/TWITTER SOCIAL SENTIMENT (via Grok) ===\n"
    if grok_data.get("skipped"):
        grok_section += "UNAVAILABLE — No Grok API key configured.\n"
    elif grok_data.get("error"):
        grok_section += f"ERROR: {grok_data['error']}\n"
    else:
        themes = grok_data.get("investing_themes", [])
        if themes:
            grok_section += "MAJOR INVESTING THEMES ON X:\n"
            for t in themes[:10]:
                if isinstance(t, dict):
                    grok_section += f"  - {t.get('theme', 'N/A')} (momentum: {t.get('momentum', 'N/A')}): {t.get('narrative', '')}\n"

        ticker_sent = grok_data.get("ticker_sentiment", {})
        if ticker_sent:
            grok_section += "\nPER-TICKER X SENTIMENT:\n"
            for sym, data in ticker_sent.items():
                if isinstance(data, dict):
                    grok_section += (
                        f"  {sym}: sentiment={data.get('sentiment', 'N/A')} "
                        f"(score={data.get('sentiment_score', 'N/A')}), "
                        f"buzz={data.get('buzz_level', 'N/A')} ({data.get('buzz_trend', '')}), "
                        f"narratives={data.get('key_narratives', [])}, "
                        f"smart_money={data.get('smart_money_signals', 'none')}, "
                        f"summary: {data.get('summary', '')}\n"
                    )

    # Format Gemini news data
    gemini_section = "\n=== SOURCE: REAL-TIME NEWS (via Gemini web search) ===\n"
    if gemini_data.get("skipped"):
        gemini_section += "UNAVAILABLE — No Gemini API key configured.\n"
    elif gemini_data.get("error"):
        gemini_section += f"ERROR: {gemini_data['error']}\n"
    else:
        ticker_news = gemini_data.get("ticker_news", {})
        for sym, data in ticker_news.items():
            if isinstance(data, dict):
                gemini_section += f"\n  {sym}:\n"
                for h in data.get("headlines", [])[:3]:
                    if isinstance(h, dict):
                        gemini_section += f"    - [{h.get('significance', 'N/A')}] {h.get('title', '')} ({h.get('date', '')})\n"
                for c in data.get("upcoming_catalysts", [])[:2]:
                    if isinstance(c, dict):
                        gemini_section += f"    CATALYST: {c.get('event', '')} (expected: {c.get('expected_date', 'TBD')}, impact: {c.get('potential_impact', 'N/A')})\n"
                dev = data.get("recent_developments", "")
                if dev:
                    gemini_section += f"    Developments: {dev}\n"

    # Format Claude CSV analysis
    csv_section = "\n=== SOURCE: CSV FUNDAMENTAL ANALYSIS (Claude) ===\n"
    if claude_csv_data.get("skipped"):
        csv_section += "UNAVAILABLE — No CSV data provided.\n"
    elif claude_csv_data.get("error"):
        csv_section += f"ERROR: {claude_csv_data['error']}\n"
    else:
        fund_analysis = claude_csv_data.get("fundamental_analysis", {})
        for sym, data in fund_analysis.items():
            if isinstance(data, dict):
                csv_section += (
                    f"  {sym}: revenue={data.get('revenue_trend', 'N/A')}, "
                    f"margins={data.get('margin_profile', 'N/A')}, "
                    f"cash={data.get('cash_position', 'N/A')}, "
                    f"debt={data.get('debt_level', 'N/A')}, "
                    f"growth={data.get('growth_rate', 'N/A')}, "
                    f"pivot={'YES: ' + data.get('pivot_description', '') if data.get('pivot_detected') else 'No'}, "
                    f"score={data.get('fundamental_score', 'N/A')}/10, "
                    f"insight: {data.get('key_insight', '')}\n"
                )

    # Format SEC Edgar data
    edgar_section = "\n=== SOURCE: SEC EDGAR (Filings & Insider Activity) ===\n"
    edgar_filings = edgar_data.get("edgar_data", {})
    for sym, data in edgar_filings.items():
        if isinstance(data, dict) and not data.get("error"):
            insider_info = ""
            if data.get("has_recent_insider_buying"):
                insider_info = f" *** INSIDER TRANSACTIONS DETECTED ({len(data.get('insider_transactions', []))} Form 4 filings) ***"

            eight_k_info = ""
            for filing in data.get("recent_8k", []):
                if isinstance(filing, dict):
                    eight_k_info += f" 8-K: {filing.get('description', '')} ({filing.get('filing_date', '')})"

            edgar_section += f"  {sym}: {data.get('filing_count', 0)} total filings{insider_info}{eight_k_info}\n"

    # Format Technical Analysis
    ta_section = "\n=== SOURCE: TECHNICAL ANALYSIS ===\n"
    ta_analysis = ta_data.get("technical_analysis", {})
    for sym, data in ta_analysis.items():
        if isinstance(data, dict) and not data.get("error"):
            dip_flag = " *** DIP IN UPTREND ***" if data.get("is_dip_in_uptrend") else ""
            ta_section += (
                f"  {sym}: price={data.get('price', 'N/A')}, "
                f"RSI={data.get('rsi', 'N/A')}, "
                f"TA_score={data.get('ta_score', 'N/A')}/100, "
                f"setup={data.get('setup_type', 'N/A')}, "
                f"pattern={data.get('pattern', 'N/A')}, "
                f"action={data.get('action', 'N/A')}, "
                f"trend={data.get('trend', 'N/A')}, "
                f"vol_ratio={data.get('volume_ratio', 'N/A')}, "
                f"R:R={data.get('risk_reward', 'N/A')}, "
                f"signals={data.get('signals_stacking', [])}"
                f"{dip_flag}\n"
            )

    # Build the master synthesis prompt
    synthesis_prompt = f"""You are an elite trading intelligence system synthesizing data from 5 independent sources to produce the most intelligent watchlist analysis possible.

{grok_section}
{gemini_section}
{csv_section}
{edgar_section}
{ta_section}

═══ INTELLIGENCE RULES (CRITICAL — follow these exactly) ═══

1. CONTRARIAN SIGNALS: If fundamentals show declining revenue BUT there are massive contracts in the pipeline (hyperscaler orders, government contracts, enterprise deals), this is BULLISH not bearish. The market is looking backward at declining revenue while the pipeline tells the forward story.

2. PIVOT RECOGNITION: If a company is classified as "just a Bitcoin miner" or any single-business-model label but has pivoted to AI infrastructure, energy, or another high-growth sector, that pivot IS the catalyst. Evaluate on the new trajectory, not the old label.

3. PIPELINE > CURRENT REVENUE: A company with $50M current revenue but $500M in signed contracts/backlog is MORE bullish than a company with $500M revenue and flat pipeline. Revenue is backward-looking; pipeline is forward-looking.

4. CHART PATTERN ANALOGIES: If a ticker's chart setup resembles another ticker before a major move (e.g., consolidation pattern similar to AAOI before 500% move), that's a significant technical signal worth highlighting.

5. INSIDER BUYING IN DOWNTREND: When insiders are buying while the stock price is depressed, this is one of the strongest contrarian signals. Insiders know their business better than any analyst.

6. SOCIAL SENTIMENT + TECHNICALS ALIGNMENT: When X sentiment is turning bullish AND technicals confirm (RSI recovering, volume expanding, approaching breakout), the probability of a move increases significantly.

7. MACRO THEME ALIGNMENT: Stocks riding multiple macro themes simultaneously (e.g., AI + energy + government spending) have compounding tailwinds.

═══ OUTPUT REQUIREMENTS ═══

Produce EXACTLY 6 sections with EXACTLY 4 tickers each (24 total ticker placements from the {len(tickers)} tickers: {', '.join(tickers)}).
A ticker CAN appear in multiple sections if it qualifies.
Every section MUST have exactly 4 entries.

SECTIONS:
1. "best_entries" — "Best Entries on Strong Assets": Stocks currently in a dip within an uptrend. Strong technical setup with RSI pulling back or price at support, volume declining on pullback, upcoming catalysts or tailwinds. The "buy the dip" opportunities.

2. "momentum_plays" — "Momentum Plays": Highest social sentiment momentum on X. Strong volume, breaking out or about to break out. Active narrative/theme driving the move.

3. "catalyst_watch" — "Catalyst Watch": Tickers with the most significant upcoming catalysts. Earnings, FDA decisions, contract announcements, product launches, partnerships. Weighted by magnitude of potential impact + timeline proximity.

4. "sector_rotation" — "Sector Rotation Signals": Tickers benefiting from macro/sector rotation themes. Money flowing into their sector (AI infrastructure, space, energy, etc.). Theme-level momentum, not just individual ticker momentum.

5. "high_conviction" — "High Conviction Setups": Best overall risk/reward based on ALL data sources combined. Technical + fundamental + sentiment + catalyst alignment. The "if you could only pick 4" picks.

6. "contrarian_value" — "Contrarian / Deep Value": Tickers where sentiment is negative but fundamentals/catalysts tell a different story. Insider buying while price is depressed. Market misunderstanding the business.

For EACH ticker entry in each section, provide ALL these fields:
- "symbol": ticker symbol
- "name": company name
- "price": current price (number)
- "change_pct": recent % change (number)
- "technical_setup": 1-2 sentence description of current technical picture
- "catalyst": most significant upcoming catalyst
- "sentiment": 1 sentence on social/market sentiment
- "key_insight": THE critical insight — why this is in this section, what the market is missing (1-2 sentences, be specific and opinionated)
- "risk_level": "low", "moderate", or "high"
- "action_note": 1-2 sentence actionable note with specific levels (support, resistance, entry zone, catalyst timing)

Also provide:
- "market_themes": array of 3-5 major market themes you identified across all data sources

Return ONLY valid JSON matching this exact structure:
{{
    "sections": [
        {{
            "id": "best_entries",
            "title": "Best Entries on Strong Assets",
            "subtitle": "Dips in uptrends with strong tailwinds",
            "tickers": [ {{ ticker objects }} ]
        }},
        {{
            "id": "momentum_plays",
            "title": "Momentum Plays",
            "subtitle": "Social momentum + volume breakouts",
            "tickers": [ {{ ticker objects }} ]
        }},
        {{
            "id": "catalyst_watch",
            "title": "Catalyst Watch",
            "subtitle": "Upcoming events that could move these stocks",
            "tickers": [ {{ ticker objects }} ]
        }},
        {{
            "id": "sector_rotation",
            "title": "Sector Rotation Signals",
            "subtitle": "Riding macro theme momentum",
            "tickers": [ {{ ticker objects }} ]
        }},
        {{
            "id": "high_conviction",
            "title": "High Conviction Setups",
            "subtitle": "Best risk/reward across all signals",
            "tickers": [ {{ ticker objects }} ]
        }},
        {{
            "id": "contrarian_value",
            "title": "Contrarian / Deep Value",
            "subtitle": "Market is wrong on these — here's why",
            "tickers": [ {{ ticker objects }} ]
        }}
    ],
    "market_themes": ["theme1", "theme2", "theme3"],
    "last_updated": "ISO timestamp"
}}

Be bold, opinionated, and specific. Generic analysis is worthless. Every key_insight should tell the trader something they couldn't figure out from a basic stock screener."""

    return synthesis_prompt


@traceable(name="watchlist_analysis.synthesize")
async def _synthesize_all_sources(
    tickers: List[str],
    grok_data: Dict[str, Any],
    gemini_data: Dict[str, Any],
    claude_csv_data: Dict[str, Any],
    edgar_data: Dict[str, Any],
    ta_data: Dict[str, Any],
    agent: Any,
) -> Dict[str, Any]:
    """Pass all collected data to Claude for intelligent synthesis."""
    prompt = _build_synthesis_prompt(
        tickers, grok_data, gemini_data, claude_csv_data, edgar_data, ta_data,
    )

    print(f"[WATCHLIST-ANALYSIS] Synthesis prompt: {len(prompt)} chars")

    try:
        raw_text = await asyncio.wait_for(
            asyncio.to_thread(agent._call_simple_model, "claude", prompt, 16384),
            timeout=120.0,
        )
        result = _parse_ai_json(raw_text)
        if result.get("error"):
            print(f"[WATCHLIST-ANALYSIS] Synthesis parse error: {result['error']}")
        else:
            sections = result.get("sections", [])
            print(f"[WATCHLIST-ANALYSIS] Synthesis complete: {len(sections)} sections")
        return result
    except Exception as e:
        print(f"[WATCHLIST-ANALYSIS] Synthesis failed: {e}")
        return {"error": f"Synthesis failed: {str(e)}"}


# ═══════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE — Orchestrates all sources in parallel
# ═══════════════════════════════════════════════════════════════════════════

@traceable(name="watchlist_analysis.run_pipeline")
async def run_analysis_pipeline(
    tickers: List[str],
    csv_data: List[Dict[str, str]],
    agent: Any,
    data_service: Any,
) -> Dict[str, Any]:
    """
    Run the full multi-source parallel analysis pipeline.

    1. Fire all 5 data sources simultaneously (asyncio.gather)
    2. Synthesize all results through Claude
    3. Return structured JSON with 6 sections × 4 tickers

    Gracefully degrades if any source is unavailable.
    """
    start = time.time()
    print(f"[WATCHLIST-ANALYSIS] Starting pipeline for {len(tickers)} tickers: {', '.join(tickers[:10])}...")

    # ── Phase 1: Parallel data collection ──────────────────────────────────
    grok_task = _collect_grok_sentiment(tickers)
    gemini_task = _collect_gemini_news(tickers)
    claude_csv_task = _collect_claude_csv_analysis(csv_data, agent)
    edgar_task = _collect_sec_edgar(tickers)
    ta_task = _collect_technical_analysis(tickers, data_service)

    results = await asyncio.gather(
        grok_task,
        gemini_task,
        claude_csv_task,
        edgar_task,
        ta_task,
        return_exceptions=True,
    )

    # Unpack results (handle exceptions gracefully)
    def _safe_result(result, source_name: str) -> Dict[str, Any]:
        if isinstance(result, Exception):
            print(f"[WATCHLIST-ANALYSIS] {source_name} exception: {result}")
            return {"error": str(result)}
        return result

    grok_data = _safe_result(results[0], "Grok")
    gemini_data = _safe_result(results[1], "Gemini")
    claude_csv_data = _safe_result(results[2], "Claude CSV")
    edgar_data = _safe_result(results[3], "SEC Edgar")
    ta_data = _safe_result(results[4], "Technical Analysis")

    collection_time = time.time() - start
    print(f"[WATCHLIST-ANALYSIS] Data collection completed in {collection_time:.1f}s")

    # Report source availability
    sources_used = []
    sources_skipped = []
    for name, data in [
        ("grok", grok_data), ("gemini", gemini_data),
        ("claude_csv", claude_csv_data), ("sec_edgar", edgar_data),
        ("technical_analysis", ta_data),
    ]:
        if data.get("skipped") or data.get("error"):
            sources_skipped.append(name)
        else:
            sources_used.append(name)

    print(f"[WATCHLIST-ANALYSIS] Sources used: {sources_used}, skipped: {sources_skipped}")

    # ── Phase 2: Intelligent synthesis ─────────────────────────────────────
    synthesis_result = await _synthesize_all_sources(
        tickers, grok_data, gemini_data, claude_csv_data, edgar_data, ta_data, agent,
    )

    total_time = time.time() - start
    print(f"[WATCHLIST-ANALYSIS] Full pipeline completed in {total_time:.1f}s")

    # Add metadata
    if isinstance(synthesis_result, dict) and "sections" in synthesis_result:
        synthesis_result["_meta"] = {
            "pipeline_time_seconds": round(total_time, 1),
            "collection_time_seconds": round(collection_time, 1),
            "tickers_analyzed": len(tickers),
            "sources_used": sources_used,
            "sources_skipped": sources_skipped,
        }
        # Ensure last_updated is set
        if "last_updated" not in synthesis_result:
            synthesis_result["last_updated"] = datetime.now(timezone.utc).isoformat()

    return synthesis_result
