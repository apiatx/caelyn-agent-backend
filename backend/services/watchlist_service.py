"""
Watchlist Service — Persistence, News Aggregation, and AI Enrichment
Stores the user's CSV watchlist + AI analysis, fetches targeted news via RSS,
and re-runs AI analysis with news context on refresh.
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from xml.etree import ElementTree as ET

import httpx

# ── Persistence (JSON file store) ────────────────────────────────────────────

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
_WATCHLIST_FILE = _DATA_DIR / "watchlist_store.json"


def _read_store() -> Dict[str, Any]:
    """Read the watchlist JSON store from disk."""
    if _WATCHLIST_FILE.exists():
        try:
            return json.loads(_WATCHLIST_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def _write_store(data: Dict[str, Any]) -> None:
    """Write data to the watchlist JSON store."""
    _WATCHLIST_FILE.write_text(json.dumps(data, default=str, indent=2))


def extract_tickers(csv_data: List[Dict[str, str]], analysis: Optional[Dict] = None) -> List[str]:
    """Extract unique ticker symbols from CSV data or analysis."""
    tickers: set[str] = set()

    # From CSV rows — check common column names
    ticker_keys = {"ticker", "Ticker", "TICKER", "symbol", "Symbol", "SYMBOL"}
    for row in csv_data:
        for key in ticker_keys:
            val = row.get(key, "").strip().upper()
            if val:
                tickers.add(val)
                break

    # Also pull from analysis categories if available
    if analysis and isinstance(analysis, dict):
        categories = analysis.get("categories", {})
        for cat_stocks in categories.values():
            if isinstance(cat_stocks, list):
                for stock in cat_stocks:
                    t = stock.get("ticker", "").strip().upper()
                    if t:
                        tickers.add(t)
        for item in analysis.get("avoid_list", []):
            t = item.get("ticker", "").strip().upper()
            if t:
                tickers.add(t)

    return sorted(tickers)


def save_watchlist(csv_data: List[Dict[str, str]], analysis: Dict[str, Any]) -> Dict[str, Any]:
    """Save CSV data + AI analysis to the watchlist store."""
    tickers = extract_tickers(csv_data, analysis)
    saved_at = datetime.now(timezone.utc).isoformat()
    store = {
        "tickers": tickers,
        "csv_data": csv_data,
        "analysis": analysis,
        "saved_at": saved_at,
    }
    _write_store(store)
    print(f"[WATCHLIST] Saved {len(tickers)} tickers at {saved_at}")
    return {"success": True, "saved_at": saved_at, "ticker_count": len(tickers)}


def load_watchlist() -> Optional[Dict[str, Any]]:
    """Load the saved watchlist. Returns None if nothing saved."""
    store = _read_store()
    if not store or not store.get("tickers"):
        return None
    return store


def clear_watchlist() -> Dict[str, Any]:
    """Clear the saved watchlist."""
    if _WATCHLIST_FILE.exists():
        _WATCHLIST_FILE.unlink()
    return {"success": True}


# ── News Aggregation via RSS ─────────────────────────────────────────────────

_news_cache: Dict[str, Dict[str, Any]] = {}  # ticker -> {"fetched_at": float, "articles": [...]}
_NEWS_CACHE_TTL = 900  # 15 minutes


def _parse_rss_xml(xml_text: str, source_label: str) -> List[Dict[str, Any]]:
    """Parse RSS XML and extract articles."""
    articles = []
    try:
        root = ET.fromstring(xml_text)
        # Handle both RSS 2.0 and Atom feeds
        for item in root.iter("item"):
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            description = (item.findtext("description") or "").strip()
            pub_date = (item.findtext("pubDate") or "").strip()
            source_el = item.find("source")
            source = source_el.text.strip() if source_el is not None and source_el.text else source_label

            if title:
                articles.append({
                    "title": title,
                    "summary": description[:500] if description else "",
                    "url": link,
                    "published_at": pub_date,
                    "source": source,
                })
    except ET.ParseError:
        pass
    return articles


async def fetch_news_for_ticker(ticker: str, client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    """Fetch news for a single ticker via Yahoo Finance RSS, with Google News fallback."""
    # Check cache
    cached = _news_cache.get(ticker)
    if cached and (time.time() - cached["fetched_at"]) < _NEWS_CACHE_TTL:
        return cached["articles"]

    articles = []

    # Try Yahoo Finance RSS
    yahoo_url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
    try:
        resp = await client.get(yahoo_url, timeout=10.0)
        if resp.status_code == 200:
            articles = _parse_rss_xml(resp.text, "Yahoo Finance")
    except Exception as e:
        print(f"[WATCHLIST-NEWS] Yahoo RSS failed for {ticker}: {e}")

    # Fallback to Google News if Yahoo returned nothing
    if not articles:
        google_url = f"https://news.google.com/rss/search?q={ticker}+stock&hl=en-US&gl=US&ceid=US:en"
        try:
            resp = await client.get(google_url, timeout=10.0)
            if resp.status_code == 200:
                articles = _parse_rss_xml(resp.text, "Google News")
        except Exception as e:
            print(f"[WATCHLIST-NEWS] Google News RSS failed for {ticker}: {e}")

    # Cache results
    _news_cache[ticker] = {"fetched_at": time.time(), "articles": articles[:15]}
    return articles[:15]


async def fetch_news_for_tickers(tickers: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch news for all tickers concurrently."""
    async with httpx.AsyncClient(
        follow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0 (compatible; CaelynAI/1.0)"},
    ) as client:
        tasks = [fetch_news_for_ticker(t, client) for t in tickers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    news_map: Dict[str, List[Dict[str, Any]]] = {}
    for ticker, result in zip(tickers, results):
        if isinstance(result, Exception):
            print(f"[WATCHLIST-NEWS] Error fetching news for {ticker}: {result}")
            news_map[ticker] = []
        else:
            news_map[ticker] = result
    return news_map


# ── AI Analysis (uses the existing TradingAgent) ────────────────────────────

def _build_csv_table(csv_data: List[Dict[str, str]]) -> str:
    """Format CSV data as a readable table string."""
    if not csv_data:
        return ""
    return "\n".join(
        ", ".join(f"{k}: {v}" for k, v in row.items() if v)
        for row in csv_data[:200]
    )


def _build_refresh_prompt(
    csv_data: List[Dict[str, str]],
    tickers: List[str],
    news_map: Dict[str, List[Dict[str, Any]]],
) -> str:
    """Build the enhanced AI prompt with CSV data + latest news context."""
    csv_table = _build_csv_table(csv_data)
    columns = list(csv_data[0].keys()) if csv_data else []
    ticker_list = ", ".join(tickers)

    # Build news context section
    news_sections = []
    for ticker in tickers:
        articles = news_map.get(ticker, [])
        if articles:
            headlines = "; ".join(a["title"] for a in articles[:5])
            news_sections.append(f"LATEST NEWS FOR {ticker}: {headlines}")
        else:
            news_sections.append(f"LATEST NEWS FOR {ticker}: No recent articles found")
    news_context = "\n".join(news_sections)

    prompt = (
        "You are a world-class equity analyst and portfolio strategist. "
        "A user has uploaded their stock watchlist. Your job is to produce actionable intelligence on every stock in this list.\n\n"
        "CRITICAL: Go BEYOND the data in the CSV. Use your training knowledge of each company's recent news, "
        "competitive position, upcoming catalysts, valuation vs peers, and sector dynamics. "
        "Do NOT just summarize the spreadsheet — that is useless. The user already has the spreadsheet. "
        "Add value by providing insights they cannot get from the raw data alone.\n\n"
        "Do NOT just rank by market cap or revenue size. Focus on asymmetric opportunity, undervaluation, and timing.\n\n"
        f"=== LATEST NEWS CONTEXT ===\n{news_context}\n\n"
        "Factor in the latest news headlines provided above when assessing sentiment, catalysts, and timing for each stock.\n\n"
        f"SPREADSHEET ({len(csv_data)} stocks):\n"
        f"Columns: {', '.join(columns)}\n"
        f"Tickers: {ticker_list}\n\n"
        f"{csv_table}\n\n"
        "INSTRUCTIONS:\n"
        "Analyze every stock and place each into one or more of these categories (a stock CAN appear in multiple categories):\n\n"
        "1. top_buys_now — Best risk/reward RIGHT NOW based on news momentum + social sentiment + fundamentals alignment\n"
        "2. most_undervalued — Trading at significant discount to growth rate or peer group. Use P/S, P/E, P/FCF, EV/Revenue, PEG ratios\n"
        "3. best_catalysts — Stocks with specific upcoming events: earnings, FDA approvals, product launches, contract announcements, index inclusion, etc.\n"
        "4. hidden_gems — Companies closing massive contracts or signing enterprise deals that haven't yet shown up in reported revenue/earnings\n"
        "5. most_revolutionary — Category-defining companies with true competitive moats, no direct substitutes, genuine bottleneck technology\n"
        "6. right_sector_right_time — Stocks in sectors with strong macro/political tailwinds right now (AI infrastructure, defense, energy, etc.)\n\n"
        "FOR EACH STOCK you place in a category, provide ALL of these fields:\n"
        '- "ticker": the stock symbol\n'
        '- "name": full company name\n'
        '- "signal": one of "STRONG BUY", "BUY", "HOLD", or "AVOID"\n'
        '- "score": numeric 1-10 (10 = highest conviction)\n'
        '- "thesis": 2-3 sentence investment thesis — be specific and opinionated\n'
        '- "sentiment": one of "bullish", "neutral", "bearish"\n'
        '- "sentiment_reason": why sentiment is what it is right now (specific news, social chatter, institutional flows)\n'
        '- "catalysts": array of 2-3 specific upcoming catalysts with approximate timing\n'
        '- "valuation": object with "ps_ratio", "pe_ratio" (null if negative earnings), "pfcf", "ev_revenue", "peg" — use your best knowledge of current valuations. Include "vs_peers": a comparison string like "30% discount to semiconductor peer group"\n'
        '- "moat": description of competitive advantage (or lack thereof)\n'
        '- "why_now": 1-2 sentences on why timing is right (or wrong) for this stock\n\n'
        "ALSO produce an avoid_list for stocks that look overvalued, have deteriorating fundamentals, or face significant near-term headwinds. "
        "Each entry needs: ticker, name, reason (be specific about why to avoid).\n\n"
        "Not every stock needs to be in a category — only include stocks where you have genuine conviction or insight. "
        "But EVERY stock must appear in at least one category OR in the avoid_list. Do not silently skip any ticker.\n\n"
        "Respond ONLY with valid JSON. No markdown, no explanation, no code blocks. Just the raw JSON object.\n\n"
        "Required JSON structure:\n"
        "{\n"
        '  "display_type": "csv_watchlist_analysis",\n'
        '  "summary": "One paragraph summary of what this watchlist represents and current market context",\n'
        '  "analysis_date": "ISO date string",\n'
        '  "market_context": "2-3 sentences on the macro environment relevant to these stocks",\n'
        '  "categories": {\n'
        '    "top_buys_now": [ { stock objects as described above } ],\n'
        '    "most_undervalued": [ { stock objects } ],\n'
        '    "best_catalysts": [ { stock objects } ],\n'
        '    "hidden_gems": [ { stock objects } ],\n'
        '    "most_revolutionary": [ { stock objects } ],\n'
        '    "right_sector_right_time": [ { stock objects } ]\n'
        "  },\n"
        '  "avoid_list": [\n'
        '    { "ticker": "XYZ", "name": "Company Name", "reason": "Specific reason to avoid" }\n'
        "  ]\n"
        "}\n\n"
        "User request: Refresh analysis for my watchlist incorporating the latest news headlines."
    )
    return prompt


def _build_stock_deep_dive_prompt(
    ticker: str,
    csv_row: Dict[str, str],
    news: List[Dict[str, Any]],
) -> str:
    """Build prompt for deep-dive analysis of a single stock."""
    row_data = ", ".join(f"{k}: {v}" for k, v in csv_row.items() if v)
    headlines = "\n".join(f"- {a['title']} ({a.get('source', 'N/A')})" for a in news[:10])

    return (
        f"You are a world-class equity analyst. Provide a deep dive on {ticker} "
        "based on the following data and latest news.\n\n"
        f"CSV DATA: {row_data}\n\n"
        f"LATEST NEWS:\n{headlines}\n\n"
        "Return ONLY valid JSON (no markdown, no code blocks) with these fields:\n"
        "{\n"
        '  "extended_thesis": "3-5 paragraph detailed investment thesis",\n'
        '  "risk_factors": ["risk 1", "risk 2", ...],\n'
        '  "bull_case": "2-3 paragraph bull case scenario",\n'
        '  "bear_case": "2-3 paragraph bear case scenario",\n'
        '  "technical_outlook": "Current technical setup and key pattern observations",\n'
        '  "analyst_sentiment_summary": "Summary of buy-side and sell-side analyst positioning",\n'
        '  "sector_peers_comparison": [\n'
        '    { "ticker": "PEER1", "name": "...", "market_cap": "...", "pe_ratio": ..., "ps_ratio": ..., "comparison_note": "..." }\n'
        "  ],\n"
        '  "key_levels": {\n'
        '    "support": [price1, price2],\n'
        '    "resistance": [price1, price2],\n'
        '    "note": "Brief context on these levels"\n'
        "  }\n"
        "}"
    )


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
            # Try to extract JSON object by finding matched braces
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


async def run_ai_analysis(prompt: str, agent: Any) -> Dict[str, Any]:
    """Run AI analysis using the trading agent's model. Returns parsed JSON."""
    try:
        raw_text = await asyncio.wait_for(
            asyncio.to_thread(
                agent._call_simple_model, "claude", prompt, 16384
            ),
            timeout=120.0,
        )
        return _parse_ai_json(raw_text)
    except asyncio.TimeoutError:
        return {"error": "AI analysis timed out after 120s"}
    except Exception as e:
        return {"error": f"AI analysis failed: {str(e)}"}


async def refresh_watchlist_analysis(agent: Any) -> Dict[str, Any]:
    """Reload saved CSV, fetch news, re-run AI with news context, save results."""
    store = load_watchlist()
    if not store:
        return {"error": "No watchlist saved. Upload a CSV first."}

    csv_data = store["csv_data"]
    tickers = store["tickers"]

    # Fetch news concurrently for all tickers
    print(f"[WATCHLIST-REFRESH] Fetching news for {len(tickers)} tickers...")
    news_map = await fetch_news_for_tickers(tickers)

    # Build enhanced prompt with news
    prompt = _build_refresh_prompt(csv_data, tickers, news_map)
    print(f"[WATCHLIST-REFRESH] Running AI analysis ({len(prompt)} chars)...")

    # Run AI
    analysis = await run_ai_analysis(prompt, agent)
    if analysis.get("error"):
        return analysis

    # Save updated analysis
    save_watchlist(csv_data, analysis)
    return analysis


async def get_stock_detail(ticker: str, agent: Any) -> Dict[str, Any]:
    """Get enriched detail for a single stock: CSV data + news + AI deep dive."""
    store = load_watchlist()
    if not store:
        return {"error": "No watchlist saved."}

    # Find the CSV row for this ticker
    csv_row = None
    ticker_upper = ticker.upper()
    ticker_keys = {"ticker", "Ticker", "TICKER", "symbol", "Symbol", "SYMBOL"}
    for row in store.get("csv_data", []):
        for key in ticker_keys:
            if row.get(key, "").strip().upper() == ticker_upper:
                csv_row = row
                break
        if csv_row:
            break

    if not csv_row:
        return {"error": f"Ticker {ticker} not found in saved watchlist."}

    # Fetch news for this ticker
    async with httpx.AsyncClient(
        follow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0 (compatible; CaelynAI/1.0)"},
    ) as client:
        news = await fetch_news_for_ticker(ticker_upper, client)

    # Run AI deep dive
    prompt = _build_stock_deep_dive_prompt(ticker_upper, csv_row, news)
    ai_enrichment = await run_ai_analysis(prompt, agent)

    return {
        "ticker": ticker_upper,
        "csv_data": csv_row,
        "news": news[:10],
        "ai_enrichment": ai_enrichment,
    }
