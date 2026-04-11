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
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[Watchlist] Writing to {_WATCHLIST_FILE}")
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
        # Check nested "categories" key (legacy format)
        categories = analysis.get("categories", {})
        for cat_stocks in categories.values():
            if isinstance(cat_stocks, list):
                for stock in cat_stocks:
                    t = stock.get("ticker", "").strip().upper()
                    if t:
                        tickers.add(t)
        # Check top-level category keys (current flat format from AI)
        _CATEGORY_KEYS = ("top_buys", "most_undervalued", "best_catalysts",
                          "hidden_gems", "most_revolutionary", "right_sector")
        for key in _CATEGORY_KEYS:
            cat_list = analysis.get(key, [])
            if isinstance(cat_list, list):
                for stock in cat_list:
                    if isinstance(stock, dict):
                        t = stock.get("ticker", "").strip().upper()
                        if t:
                            tickers.add(t)
        for item in analysis.get("avoid_list", []):
            if isinstance(item, dict):
                t = item.get("ticker", "").strip().upper()
                if t:
                    tickers.add(t)

    return sorted(tickers)


def save_watchlist(csv_data: List[Dict[str, str]], analysis: Dict[str, Any], watchlist_id: str = None, name: str = None) -> Dict[str, Any]:
    """Save CSV data + AI analysis. PostgreSQL first (survives deploys), JSON file fallback."""
    import uuid
    if not watchlist_id:
        watchlist_id = str(uuid.uuid4())[:8]
    tickers = extract_tickers(csv_data, analysis)
    if not name:
        # Auto-name from tickers: "AAPL, NVDA, CRDO +5"
        if len(tickers) <= 3:
            name = ", ".join(tickers[:3]) if tickers else "Watchlist"
        else:
            name = ", ".join(tickers[:3]) + f" +{len(tickers)-3}"
    saved_at = datetime.now(timezone.utc).isoformat()

    # Try PostgreSQL first (survives deploys)
    try:
        from data.pg_storage import watchlist_write, is_available as pg_available
        if pg_available():
            ok = watchlist_write(watchlist_id, name, csv_data, analysis, tickers)
            if ok:
                print(f"[WATCHLIST] Saved to PostgreSQL id={watchlist_id} name={name} ({len(tickers)} tickers)")
                return {"success": True, "watchlist_id": watchlist_id, "name": name, "saved_at": saved_at, "ticker_count": len(tickers)}
    except Exception as e:
        print(f"[WATCHLIST] PostgreSQL save failed ({e}), falling back to JSON file")

    # Fallback: JSON file
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    store = {"id": watchlist_id, "name": name, "csv_data": csv_data, "analysis": analysis, "tickers": tickers, "saved_at": saved_at}
    _WATCHLIST_FILE.write_text(json.dumps(store, default=str))
    print(f"[WATCHLIST] Saved to JSON file id={watchlist_id} ({len(tickers)} tickers)")
    return {"success": True, "watchlist_id": watchlist_id, "name": name, "saved_at": saved_at, "ticker_count": len(tickers)}


def load_watchlist(watchlist_id: str = None) -> Optional[Dict[str, Any]]:
    """Load a saved watchlist by id. If no id given, returns the most recently updated one.
    PostgreSQL first, JSON file fallback."""
    # Try PostgreSQL first
    try:
        from data.pg_storage import watchlist_read, watchlist_list as pg_watchlist_list, is_available as pg_available
        if pg_available():
            if watchlist_id:
                data = watchlist_read(watchlist_id)
            else:
                # No id specified — return the most recently updated watchlist
                all_wl = pg_watchlist_list()
                if all_wl:
                    data = watchlist_read(all_wl[0]["id"])
                else:
                    data = None
            if data is not None:
                print(f"[WATCHLIST] Loaded from PostgreSQL id={data.get('id')} ({len(data.get('tickers', []))} tickers)")
                return data
    except Exception as e:
        print(f"[WATCHLIST] PostgreSQL load failed ({e}), falling back to JSON file")

    # Fallback: JSON file
    store = _read_store()
    if not store or not store.get("tickers"):
        return None
    return store


def list_watchlists() -> List[Dict[str, Any]]:
    """List all saved watchlists (metadata only)."""
    # Try PostgreSQL
    try:
        from data.pg_storage import watchlist_list as pg_watchlist_list, is_available as pg_available
        if pg_available():
            return pg_watchlist_list()
    except Exception as e:
        print(f"[WATCHLIST] PostgreSQL list failed: {e}")

    # Fallback: scan JSON file
    results = []
    if _WATCHLIST_FILE.exists():
        try:
            store = json.loads(_WATCHLIST_FILE.read_text())
            results.append({
                "id": store.get("id", "default"),
                "name": store.get("name", "Watchlist"),
                "ticker_count": len(store.get("tickers", [])),
                "saved_at": store.get("saved_at", ""),
                "updated_at": store.get("saved_at", ""),
            })
        except Exception:
            pass
    return results


def clear_watchlist(watchlist_id: str = 'default') -> Dict[str, Any]:
    """Delete a specific watchlist by id."""
    cleared = False
    # Try PostgreSQL
    try:
        from data.pg_storage import watchlist_delete, is_available as pg_available
        if pg_available():
            watchlist_delete(watchlist_id)
            cleared = True
    except Exception as e:
        print(f"[WATCHLIST] PostgreSQL clear failed: {e}")

    # Also clear JSON file if it matches
    if watchlist_id == 'default' and _WATCHLIST_FILE.exists():
        _WATCHLIST_FILE.unlink()
        cleared = True

    return {"success": cleared}


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


async def _fetch_news_perplexity(ticker: str) -> List[Dict[str, Any]]:
    """Use Perplexity Sonar API (fast web search) to get latest news for a ticker."""
    api_key = os.environ.get("PERPLEXITY_API_KEY", "")
    if not api_key:
        return []
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.post(
                "https://api.perplexity.ai/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "sonar",
                    "messages": [
                        {
                            "role": "user",
                            "content": (
                                f"Give me the 8 most recent and important news headlines about {ticker} stock. "
                                "For each, provide: title, a 1-2 sentence summary, the source name, and the publication date. "
                                "Focus on: earnings, contracts, partnerships, product launches, analyst upgrades/downgrades, "
                                "regulatory news, and significant price movements. "
                                "Respond ONLY with a JSON array: "
                                '[{"title": "...", "summary": "...", "source": "...", "published_at": "YYYY-MM-DD", "url": ""}]'
                            ),
                        }
                    ],
                    "search_recency_filter": "week",
                    "return_citations": True,
                },
            )
        if resp.status_code != 200:
            print(f"[WATCHLIST-NEWS] Perplexity API error {resp.status_code} for {ticker}")
            return []
        data = resp.json()
        content = data["choices"][0]["message"]["content"].strip()
        # Strip markdown code fences if present
        if content.startswith("```"):
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]
        citations = data.get("citations", [])
        articles = json.loads(content)
        # Inject real URLs from citations where possible
        for i, art in enumerate(articles):
            if not art.get("url") and i < len(citations):
                art["url"] = citations[i]
        print(f"[WATCHLIST-NEWS] Perplexity returned {len(articles)} articles for {ticker}")
        return articles[:10]
    except Exception as e:
        print(f"[WATCHLIST-NEWS] Perplexity news fetch failed for {ticker}: {e}")
        return []


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

    # Final fallback: Perplexity Sonar (fast web search)
    if not articles:
        print(f"[WATCHLIST-NEWS] RSS feeds empty for {ticker}, falling back to Perplexity")
        articles = await _fetch_news_perplexity(ticker)

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


# ── Perplexity Sonar (real-time web search context) ────────────────────────


async def _fetch_sonar_context_for_tickers(tickers: List[str]) -> str:
    """
    Use Perplexity Sonar to get real-time web search context for all tickers at once.
    Returns a formatted string ready to inject into the Claude analysis prompt.
    """
    api_key = os.environ.get("PERPLEXITY_API_KEY", "")
    if not api_key:
        print("[WATCHLIST-REFRESH] No PERPLEXITY_API_KEY — skipping live news context")
        return ""

    ticker_list = ", ".join(tickers)
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                "https://api.perplexity.ai/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "sonar",
                    "messages": [
                        {
                            "role": "user",
                            "content": (
                                f"For each of the following stocks: {ticker_list}\n\n"
                                "Search the web and provide the most important CURRENT developments from the past 2 weeks for each ticker. "
                                "Focus on: earnings surprises, contract wins, product launches, analyst upgrades/downgrades, "
                                "regulatory decisions, CEO changes, partnerships, short squeeze activity, institutional buying/selling, "
                                "and any other market-moving events. "
                                "Format your response EXACTLY like this for each ticker:\n"
                                "TICKER: [headline 1] | [headline 2] | [headline 3]\n"
                                "One line per ticker. Be specific and factual. Only include things that actually happened recently."
                            ),
                        }
                    ],
                    "search_recency_filter": "month",
                    "return_citations": False,
                },
            )
        if resp.status_code != 200:
            print(f"[WATCHLIST-REFRESH] Perplexity sonar error: {resp.status_code}")
            return ""
        content = resp.json()["choices"][0]["message"]["content"].strip()
        print(f"[WATCHLIST-REFRESH] Perplexity sonar returned {len(content)} chars of news context")
        return content
    except Exception as e:
        print(f"[WATCHLIST-REFRESH] Perplexity sonar failed: {e}")
        return ""


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
        "1. top_buys — Best risk/reward RIGHT NOW based on news momentum + social sentiment + fundamentals alignment\n"
        "2. most_undervalued — Trading at significant discount to growth rate or peer group. Use P/S, P/E, P/FCF, EV/Revenue, PEG ratios\n"
        "3. best_catalysts — Stocks with specific upcoming events: earnings, FDA approvals, product launches, contract announcements, index inclusion, etc.\n"
        "4. hidden_gems — Companies closing massive contracts or signing enterprise deals that haven't yet shown up in reported revenue/earnings\n"
        "5. most_revolutionary — Category-defining companies with true competitive moats, no direct substitutes, genuine bottleneck technology\n"
        "6. right_sector — Stocks in sectors with strong macro/political tailwinds right now (AI infrastructure, defense, energy, etc.)\n\n"
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
        '  "top_buys": [ { stock objects as described above } ],\n'
        '  "most_undervalued": [ { stock objects } ],\n'
        '  "best_catalysts": [ { stock objects } ],\n'
        '  "hidden_gems": [ { stock objects } ],\n'
        '  "most_revolutionary": [ { stock objects } ],\n'
        '  "right_sector": [ { stock objects } ],\n'
        '  "avoid_list": [\n'
        '    { "ticker": "XYZ", "name": "Company Name", "reason": "Specific reason to avoid" }\n'
        "  ]\n"
        "}\n\n"
        "User request: Refresh analysis for my watchlist incorporating the latest news headlines."
    )
    return prompt


def _build_refresh_prompt_with_context(
    csv_data: List[Dict[str, str]],
    tickers: List[str],
    news_context: str,
) -> str:
    """Build refresh prompt using raw Perplexity sonar context instead of RSS news map."""
    csv_table = _build_csv_table(csv_data)

    news_section = (
        f"=== CURRENT MARKET INTELLIGENCE (live web search) ===\n{news_context}\n"
        if news_context
        else "=== CURRENT MARKET INTELLIGENCE ===\nNo live news context available — rely on fundamental analysis.\n"
    )

    return (
        "You are a world-class equity analyst with access to real-time market intelligence. "
        "CRITICAL: Go BEYOND the data in the CSV. Use both the CSV fundamentals AND the live news context below "
        "to produce a high-signal analysis. The news context is from a live web search — treat it as current fact.\n\n"
        f"=== WATCHLIST DATA ===\n{csv_table}\n\n"
        f"{news_section}\n"
        "Factor in ALL of the above when assessing sentiment, catalysts, timing, and conviction for each stock.\n\n"
        "Produce a JSON response with this EXACT flat structure (no nested 'categories' object):\n"
        "{\n"
        '  "display_type": "csv_watchlist_analysis",\n'
        '  "summary": "One paragraph summary of what this watchlist represents and current market context",\n'
        '  "analysis_date": "<today ISO date>",\n'
        '  "market_context": "2-3 sentences on macro environment relevant to these stocks",\n'
        '  "top_buys": [ <stock objects> ],\n'
        '  "most_undervalued": [ <stock objects> ],\n'
        '  "best_catalysts": [ <stock objects> ],\n'
        '  "hidden_gems": [ <stock objects> ],\n'
        '  "most_revolutionary": [ <stock objects> ],\n'
        '  "right_sector": [ <stock objects> ],\n'
        '  "avoid_list": [ {"ticker": "...", "name": "...", "reason": "..."} ]\n'
        "}\n\n"
        "Each stock object must have: ticker, name, signal (STRONG BUY/BUY/HOLD/AVOID), score (1-10), "
        "thesis (2-3 sentences), sentiment, sentiment_reason, catalysts (array), "
        "valuation ({ps_ratio, pe_ratio, pfcf, ev_revenue, vs_peers}), moat, why_now.\n\n"
        "CRITICAL RULES:\n"
        "- Do NOT rank by market cap or revenue size\n"
        "- Focus on asymmetric opportunity, undervaluation relative to growth, and timing\n"
        "- Use the live news context to identify stocks with recent catalysts not visible in the CSV numbers\n"
        "- A stock can appear in multiple categories if it qualifies\n"
        "- Respond ONLY with valid JSON. No markdown, no explanation, no code blocks.\n\n"
        f"User request: Refresh analysis for my watchlist: {', '.join(tickers)}"
    )


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


async def refresh_watchlist_analysis(agent: Any, watchlist_id: str = None) -> Dict[str, Any]:
    """Reload saved CSV, fetch news, re-run AI with news context, save results."""
    store = load_watchlist(watchlist_id)
    if not store:
        return {"error": "No watchlist saved. Upload a CSV first."}

    wl_id = store.get("id", watchlist_id or "default")
    wl_name = store.get("name", "Watchlist")
    csv_data = store["csv_data"]
    tickers = store["tickers"]

    # Fetch live news context via Perplexity sonar
    print(f"[WATCHLIST-REFRESH] Fetching sonar context for {len(tickers)} tickers...")
    news_context = await _fetch_sonar_context_for_tickers(tickers)

    # Build enhanced prompt with live news context
    prompt = _build_refresh_prompt_with_context(csv_data, tickers, news_context)
    print(f"[WATCHLIST-REFRESH] Running AI analysis ({len(prompt)} chars)...")

    # Run AI
    analysis = await run_ai_analysis(prompt, agent)
    if analysis.get("error"):
        return analysis

    # Save updated analysis back to the same watchlist
    save_watchlist(csv_data, analysis, watchlist_id=wl_id, name=wl_name)
    return analysis


async def get_stock_detail(ticker: str, agent: Any, watchlist_id: str = None) -> Dict[str, Any]:
    """Get enriched detail for a single stock: CSV data + news + AI deep dive."""
    store = load_watchlist(watchlist_id)
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
