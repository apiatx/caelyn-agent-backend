"""
Perplexity Search API provider for market intelligence.
Drop-in replacement for BraveProvider/TavilyProvider -- same 4-method interface.

Endpoint: POST https://api.perplexity.ai/search
Pricing:  $5 per 1,000 requests (flat, no token cost).

Features:
  - Domain allowlists/denylists (up to 20 domains)
  - Multi-query (up to 5 queries per request)
  - Recency filters (search_recency_filter: day, week, month, year)
  - Content extraction control (max_tokens_per_page)

Response format:
  {"results": [{"title", "url", "snippet", "date", "last_updated"}], "id": "..."}
"""
import asyncio
import httpx
from data.cache import cache

PERPLEXITY_TTL = 300       # 5 minutes
PERPLEXITY_NEWS_TTL = 600  # 10 minutes for broad market news

# Trusted financial news domains for allowlist mode
FINANCIAL_DOMAIN_ALLOWLIST = [
    "reuters.com",
    "bloomberg.com",
    "wsj.com",
    "ft.com",
    "cnbc.com",
    "seekingalpha.com",
    "marketwatch.com",
    "barrons.com",
    "benzinga.com",
    "investors.com",
    "thestreet.com",
    "finance.yahoo.com",
    "sec.gov",
    "investing.com",
    "zerohedge.com",
]


class PerplexityProvider:
    """Web search via Perplexity Search API -- same interface as BraveProvider/TavilyProvider."""

    SEARCH_URL = "https://api.perplexity.ai/search"
    CHAT_URL = "https://api.perplexity.ai/chat/completions"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self._headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

    async def _search(self, query, max_results: int = 10,
                      search_depth: str = "basic", topic: str = "news",
                      domain_filter: list = None,
                      recency: str = None) -> dict:
        """
        Execute a Perplexity Search API call.
        Returns a Brave/Tavily-compatible response format:
          {"answer": "", "results": [{"title", "content", "url", "age"}]}

        Perplexity Search API response:
          {"results": [{"title", "url", "snippet", "date", "last_updated"}]}
        """
        body = {
            "query": query if isinstance(query, str) else query,
            "max_results": min(max_results, 20),
        }

        if recency:
            body["search_recency_filter"] = recency
        elif topic == "news":
            body["search_recency_filter"] = "day"

        if domain_filter:
            body["search_domain_filter"] = domain_filter[:20]

        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    self.SEARCH_URL,
                    headers=self._headers,
                    json=body,
                    timeout=12.0,
                )
            if resp.status_code == 429:
                print(f"[Perplexity] Rate limited for query: {str(query)[:60]}")
                return {"error": "rate_limited", "results": []}
            if resp.status_code in (401, 403, 451):
                print(f"[Perplexity] HTTP {resp.status_code} -- API key may be invalid")
                return {"error": "invalid_key", "results": []}
            if resp.status_code in (400, 404, 405, 410):
                print(f"[Perplexity] Search endpoint unavailable (HTTP {resp.status_code}), falling back to chat endpoint")
                return await self._search_via_chat(
                    query=query,
                    max_results=max_results,
                    topic=topic,
                    domain_filter=domain_filter,
                    recency=recency,
                )
            if resp.status_code != 200:
                print(f"[Perplexity] HTTP {resp.status_code} for query: {str(query)[:60]}: {resp.text[:200]}")
                return {"error": f"HTTP {resp.status_code}", "results": []}

            raw = resp.json()
            return self._normalize_response(raw)

        except Exception as e:
            print(f"[Perplexity] Error: {e}")
            return {"error": str(e), "results": []}

    async def _search_via_chat(self, query, max_results: int = 10,
                               topic: str = "news", domain_filter: list = None,
                               recency: str = None) -> dict:
        """Fallback for accounts/models that no longer support /search endpoint."""
        recency_hint = recency or ("day" if topic == "news" else "week")
        domains = ", ".join((domain_filter or [])[:20])
        domain_instruction = f"Prefer sources from: {domains}." if domains else ""

        prompt = (
            "Search the web and return up to "
            f"{min(max_results, 20)} relevant results for this request: {query}. "
            "For each result include title, url, and a short snippet. "
            f"Prioritize recency: {recency_hint}. {domain_instruction}"
        )

        try:
            body = {
                "model": "sonar-reasoning-pro",
                "messages": [
                    {"role": "system", "content": "You are a web search assistant. Use web results and provide concise factual output."},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.1,
                "max_tokens": 1200,
            }
            if domain_filter:
                body["search_domain_filter"] = domain_filter[:20]

            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    self.CHAT_URL,
                    headers=self._headers,
                    json=body,
                    timeout=15.0,
                )

            if resp.status_code in (401, 403, 451):
                return {"error": "invalid_key", "results": []}
            if resp.status_code == 429:
                return {"error": "rate_limited", "results": []}
            if resp.status_code != 200:
                return {"error": f"HTTP {resp.status_code}", "results": []}

            raw = resp.json()
            return self._normalize_chat_response(raw, max_results=max_results)
        except Exception as e:
            return {"error": str(e), "results": []}

    def _normalize_response(self, raw: dict) -> dict:
        """
        Convert Perplexity Search API response to Brave/Tavily-compatible format.

        Perplexity Search response:
          {"results": [{"title", "url", "snippet", "date", "last_updated"}]}

        Normalized format:
          {"answer": "", "results": [{"title", "content", "url", "age"}]}
        """
        results = []
        for item in raw.get("results", []):
            results.append({
                "title": item.get("title", ""),
                "content": item.get("snippet", ""),
                "url": item.get("url", ""),
                "age": item.get("date", item.get("last_updated", "")),
            })

        return {
            "answer": "",
            "results": results,
        }

    def _normalize_chat_response(self, raw: dict, max_results: int = 10) -> dict:
        """Normalize chat-completions response to the same shape as _normalize_response."""
        import re as _re
        answer = ""
        choices = raw.get("choices", [])
        if choices:
            answer = choices[0].get("message", {}).get("content", "") or ""
        # Strip <think>...</think> reasoning tokens from reasoning models
        answer = _re.sub(r"<think>[\s\S]*?</think>", "", answer).strip()

        citations = raw.get("citations", []) or []
        results = []
        for url in citations[:min(max_results, 20)]:
            results.append({
                "title": url,
                "content": answer[:500],
                "url": url,
                "age": "",
            })

        return {
            "answer": answer,
            "results": results,
        }

    # -- Public methods matching BraveProvider/TavilyProvider interface --

    async def search_ticker_batch(self, tickers: list,
                                  focus: str = "analyst_ratings_news") -> dict:
        """
        Search for multiple tickers in a single call.
        Returns parsed results keyed by ticker.
        """
        if not tickers:
            return {}

        ticker_str = ", ".join(tickers[:6])

        if focus == "analyst_ratings_news":
            query = f"Latest analyst ratings, price targets, and breaking news for {ticker_str} stocks today"
        elif focus == "sentiment":
            query = f"Market sentiment, social media buzz, and investor opinion on {ticker_str} stocks"
        elif focus == "fundamentals":
            query = f"Key fundamentals: P/E ratio, market cap, revenue growth, earnings for {ticker_str}"
        else:
            query = f"Latest analyst ratings and news for {ticker_str}"

        cache_key = f"pplx:batch:{focus}:{':'.join(sorted(t.upper() for t in tickers[:6]))}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        data = await self._search(
            query, max_results=10, topic="news",
            domain_filter=FINANCIAL_DOMAIN_ALLOWLIST,
            recency="day",
        )

        result = self._parse_batch_results(tickers, data)
        cache.set(cache_key, result, PERPLEXITY_TTL)
        return result

    def _parse_batch_results(self, tickers: list, raw: dict) -> dict:
        """Parse results and attribute them to tickers."""
        parsed = {t.upper(): {"ticker": t.upper(), "headlines": [], "snippets": []}
                  for t in tickers}

        answer = raw.get("answer", "")
        if answer:
            parsed["_summary"] = answer

        for item in raw.get("results", []):
            title = item.get("title", "")
            content = item.get("content", "")
            url = item.get("url", "")
            combined = (title + " " + content).upper()

            matched = False
            for ticker in tickers:
                t_upper = ticker.upper()
                if t_upper in combined or t_upper in title.upper():
                    parsed[t_upper]["headlines"].append(title[:200])
                    parsed[t_upper]["snippets"].append(content[:300])
                    if url:
                        parsed[t_upper].setdefault("sources", [])
                        parsed[t_upper]["sources"].append(url)
                    matched = True

            if not matched:
                parsed.setdefault("_general", [])
                parsed["_general"].append({
                    "title": title[:200],
                    "snippet": content[:300],
                })

        return parsed

    async def enrich_tickers_batched(self, tickers: list) -> dict:
        """
        Full enrichment for up to 12 tickers.
        Runs batches of 6, ~2 API calls total.
        """
        if not tickers:
            return {}

        cache_key = f"pplx:enriched:{':'.join(sorted(t.upper() for t in tickers[:12]))}"
        cached = cache.get(cache_key)
        if cached is not None:
            print(f"[Perplexity] enrichment cache hit for {len(tickers)} tickers")
            return cached

        batches = [tickers[i:i + 6] for i in range(0, len(tickers), 6)]

        tasks = []
        for batch in batches[:2]:
            tasks.append(self.search_ticker_batch(batch, focus="analyst_ratings_news"))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        combined = {}
        for result in results:
            if isinstance(result, Exception):
                continue
            if isinstance(result, dict):
                for ticker, data in result.items():
                    if ticker.startswith("_"):
                        combined[ticker] = data
                        continue
                    if ticker in combined:
                        existing = combined[ticker]
                        existing["headlines"].extend(data.get("headlines", []))
                        existing["snippets"].extend(data.get("snippets", []))
                    else:
                        combined[ticker] = data

        print(f"[Perplexity] enriched {len([k for k in combined if not k.startswith('_')])} tickers with {len(batches)} API calls")
        cache.set(cache_key, combined, PERPLEXITY_TTL)
        return combined

    async def get_market_news(self, topic: str = "stock market today") -> dict:
        """Get broad market news -- same interface as BraveProvider/TavilyProvider."""
        cache_key = f"pplx:market_news:{topic.replace(' ', '_')[:30]}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        data = await self._search(
            query=f"{topic} latest developments breaking news",
            max_results=10,
            topic="news",
            domain_filter=FINANCIAL_DOMAIN_ALLOWLIST,
            recency="day",
        )

        articles = []
        for item in data.get("results", []):
            articles.append({
                "title": item.get("title", ""),
                "source": item.get("url", "").split("/")[2] if item.get("url") else "",
                "content": item.get("content", "")[:400],
                "url": item.get("url", ""),
            })

        result = {
            "topic": topic,
            "article_count": len(articles),
            "summary": data.get("answer", ""),
            "articles": articles,
        }

        cache.set(cache_key, result, PERPLEXITY_NEWS_TTL)
        return result

    async def get_ticker_news_sentiment(self, ticker: str, company_name: str = "") -> dict:
        """Get news + sentiment for a single ticker."""
        ticker = ticker.upper()
        cache_key = f"pplx:ticker_news_v2:{ticker}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        short_name = ""
        if company_name:
            short_name = company_name.split(",")[0].split(" Inc")[0].split(" Corp")[0].split(" Ltd")[0].split(" Group")[0].strip()
        if short_name:
            query = f'"{short_name}" {ticker} earnings'
        else:
            query = f"{ticker} stock earnings report"

        data = await self._search(
            query=query,
            max_results=8,
            topic="news",
            domain_filter=FINANCIAL_DOMAIN_ALLOWLIST,
            recency="week",
        )

        # Strict filter: article must be primarily ABOUT this company
        ticker_lower = ticker.lower()
        short_name_lower = short_name.lower() if short_name else ""

        articles = []
        for item in data.get("results", []):
            title = item.get("title", "")
            content = item.get("content", "")
            title_lower = title.lower()
            content_lower = content.lower()

            in_title = (
                ticker_lower in title_lower
                or (short_name_lower and short_name_lower in title_lower)
            )

            body_mentions = 0
            if ticker_lower in content_lower:
                body_mentions += content_lower.count(ticker_lower)
            if short_name_lower and short_name_lower in content_lower:
                body_mentions += content_lower.count(short_name_lower)

            if not in_title and body_mentions < 2:
                continue

            articles.append({
                "title": title,
                "source": item.get("url", "").split("/")[2] if item.get("url") else "",
                "content": content[:400],
                "url": item.get("url", ""),
            })

        # Simple sentiment heuristic from article text
        all_text = " ".join(a["title"] + " " + a["content"] for a in articles).lower()
        sentiment_label = "Neutral"
        bullish_words = ["bullish", "upgrade", "beat", "surge", "rally", "outperform", "buy", "strong buy"]
        bearish_words = ["bearish", "downgrade", "miss", "decline", "underperform", "sell", "cut", "warning"]
        bull_count = sum(1 for w in bullish_words if w in all_text)
        bear_count = sum(1 for w in bearish_words if w in all_text)
        if bull_count > bear_count:
            sentiment_label = "Bullish"
        elif bear_count > bull_count:
            sentiment_label = "Bearish"

        result = {
            "ticker": ticker,
            "article_count": len(articles),
            "summary": "",
            "sentiment_label": sentiment_label,
            "articles": articles,
        }

        cache.set(cache_key, result, PERPLEXITY_TTL)
        return result

    # ── Sonar API for structured commodity data ──────────────────

    SONAR_URL = "https://api.perplexity.ai/chat/completions"

    # TradingView-compatible symbols — futures (primary) + ETF proxies (secondary)
    COMMODITY_TV_MAP = {
        "gold": {"proxy": "GLD", "tv_symbol": "TVC:GOLD", "tv_futures": "COMEX:GC1!", "tv_etf": "AMEX:GLD", "type": "metals"},
        "silver": {"proxy": "SLV", "tv_symbol": "TVC:SILVER", "tv_futures": "COMEX:SI1!", "tv_etf": "AMEX:SLV", "type": "metals"},
        "oil": {"proxy": "USO", "tv_symbol": "TVC:USOIL", "tv_futures": "NYMEX:CL1!", "tv_etf": "AMEX:USO", "type": "energy"},
        "crude oil": {"proxy": "USO", "tv_symbol": "TVC:USOIL", "tv_futures": "NYMEX:CL1!", "tv_etf": "AMEX:USO", "type": "energy"},
        "natural gas": {"proxy": "UNG", "tv_symbol": "TVC:NATGAS", "tv_futures": "NYMEX:NG1!", "tv_etf": "AMEX:UNG", "type": "energy"},
        "copper": {"proxy": "COPX", "tv_symbol": "TVC:COPPER", "tv_futures": "COMEX:HG1!", "tv_etf": "AMEX:COPX", "type": "metals"},
        "uranium": {"proxy": "URA", "tv_symbol": "AMEX:URA", "tv_futures": "", "tv_etf": "AMEX:URA", "type": "energy"},
        "platinum": {"proxy": "PPLT", "tv_symbol": "TVC:PLATINUM", "tv_futures": "NYMEX:PL1!", "tv_etf": "AMEX:PPLT", "type": "metals"},
        "palladium": {"proxy": "PALL", "tv_symbol": "TVC:PALLADIUM", "tv_futures": "NYMEX:PA1!", "tv_etf": "AMEX:PALL", "type": "metals"},
        "lithium": {"proxy": "LIT", "tv_symbol": "AMEX:LIT", "tv_futures": "", "tv_etf": "AMEX:LIT", "type": "metals"},
        "wheat": {"proxy": "WEAT", "tv_symbol": "TVC:WHEAT", "tv_futures": "CBOT:ZW1!", "tv_etf": "AMEX:WEAT", "type": "agriculture"},
        "corn": {"proxy": "CORN", "tv_symbol": "TVC:CORN", "tv_futures": "CBOT:ZC1!", "tv_etf": "AMEX:CORN", "type": "agriculture"},
        "agriculture": {"proxy": "DBA", "tv_symbol": "AMEX:DBA", "tv_futures": "", "tv_etf": "AMEX:DBA", "type": "agriculture"},
    }

    async def get_trending_commodities(self) -> list:
        """
        Use Perplexity Sonar to get current trending commodities with prices/moves,
        then map to TradingView-compatible ETF proxy symbols.
        Returns list of commodity dicts compatible with cross_asset_trending pipeline.
        """
        cache_key = "pplx:sonar_commodities"
        cached = cache.get(cache_key)
        if cached is not None:
            print(f"[Perplexity] Sonar commodities cache hit: {len(cached)} items")
            return cached

        prompt = (
            "List the top 5 most trending/moving commodities and futures right now. "
            "For each one, provide: name, current price, daily % change, and a brief "
            "1-sentence reason why it's moving — connect the move to the underlying "
            "driver (geopolitical events, supply disruptions, macro data, weather, etc.). "
            "Focus on: crude oil, gold, silver, "
            "natural gas, copper, uranium, platinum, palladium, lithium, wheat, corn. "
            "Format each as: NAME | PRICE | CHANGE% | REASON"
        )

        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    self.SONAR_URL,
                    headers=self._headers,
                    json={
                        "model": "sonar-reasoning-pro",
                        "messages": [
                            {"role": "system", "content": "You are a commodities market analyst. Provide current, accurate market data. Always include the daily percentage change with a + or - sign. Connect price moves to their underlying drivers (geopolitical events, supply disruptions, macro data, weather)."},
                            {"role": "user", "content": prompt},
                        ],
                        "temperature": 0.1,
                        "max_tokens": 1200,
                        "search_domain_filter": [
                            "reuters.com",
                            "bloomberg.com",
                            "investing.com",
                            "marketwatch.com",
                            "cnbc.com",
                        ],
                    },
                    timeout=25.0,
                )

            if resp.status_code != 200:
                print(f"[Perplexity] Sonar commodities HTTP {resp.status_code}: {resp.text[:200]}")
                return []

            raw = resp.json()
            content = ""
            choices = raw.get("choices", [])
            if choices:
                content = choices[0].get("message", {}).get("content", "")
            citations = raw.get("citations", [])

            # Strip <think>...</think> reasoning tokens from reasoning models
            import re as _re
            content = _re.sub(r"<think>[\s\S]*?</think>", "", content).strip()

            if not content:
                print("[Perplexity] Sonar commodities: empty response")
                return []

            commodities = self._parse_commodity_response(content, citations)
            if commodities:
                cache.set(cache_key, commodities, PERPLEXITY_TTL)
                print(f"[Perplexity] Sonar commodities: {len(commodities)} items parsed")
            return commodities

        except Exception as e:
            print(f"[Perplexity] Sonar commodities error: {e}")
            return []

    def _parse_commodity_response(self, content: str, citations: list = None) -> list:
        """Parse Sonar response into structured commodity data with TradingView symbols."""
        import re
        results = []

        lines = content.strip().split("\n")
        for line in lines:
            line = line.strip().lstrip("0123456789.-)*] ")
            if not line or len(line) < 5:
                continue

            # Try to parse "NAME | PRICE | CHANGE% | REASON" format
            parts = [p.strip() for p in line.split("|")]
            if len(parts) >= 3:
                name = parts[0].strip().lstrip("*#- ").rstrip("*")
                price_str = parts[1].strip()
                change_str = parts[2].strip()
                reason = parts[3].strip() if len(parts) >= 4 else ""
            else:
                # Try free-form: look for price ($xxx) and change (+/-x.x%)
                name_match = re.match(r'^[*#\-\s]*([A-Za-z\s]+)', line)
                if not name_match:
                    continue
                name = name_match.group(1).strip()
                price_match = re.search(r'\$?([\d,]+\.?\d*)', line)
                change_match = re.search(r'([+-]?\d+\.?\d*)\s*%', line)
                price_str = price_match.group(1) if price_match else ""
                change_str = change_match.group(0) if change_match else ""
                reason_match = re.search(r'(?:due to|because|amid|as|driven by|on)\s+(.+)', line, re.IGNORECASE)
                reason = reason_match.group(1).strip() if reason_match else ""

            # Parse price
            try:
                price = float(price_str.replace("$", "").replace(",", ""))
            except (ValueError, AttributeError):
                price = None

            # Parse change %
            try:
                change_pct = float(re.search(r'([+-]?\d+\.?\d*)', change_str).group(1))
            except (ValueError, AttributeError):
                change_pct = 0.0

            # Match to TradingView symbol
            name_lower = name.lower().strip()
            tv_info = None
            for key, info in self.COMMODITY_TV_MAP.items():
                if key in name_lower or name_lower in key:
                    tv_info = info
                    break

            if not tv_info:
                # Default fallback
                tv_info = {"proxy": name[:4].upper(), "tv_symbol": "", "tv_futures": "", "tv_etf": "", "type": "commodity"}

            results.append({
                "symbol": tv_info["proxy"],
                "name": name,
                "theme": name_lower.replace(" ", "_"),
                "type": tv_info["type"],
                "price": price,
                "change": round(price * change_pct / 100, 2) if price and change_pct else None,
                "change_pct": change_pct,
                "abs_change_pct": abs(change_pct),
                "volume": None,
                "avg_volume": None,
                "year_high": None,
                "year_low": None,
                "grok_theme_match": False,
                "tradingview_symbol": tv_info["tv_symbol"],
                "tradingview_futures": tv_info.get("tv_futures", ""),
                "tradingview_etf": tv_info.get("tv_etf", ""),
                "source": "perplexity_sonar",
                "reason": reason[:200] if reason else "",
                "citations": citations[:3] if citations else [],
            })

        return results[:5]
