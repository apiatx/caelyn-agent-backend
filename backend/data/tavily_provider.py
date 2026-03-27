"""
Tavily Search API provider for market intelligence.
Replaces per-ticker StockTwits/StockAnalysis/AlphaVantage calls
with batched search queries that return pre-parsed, AI-ready results.

Free tier: 1,000 calls/month.
Budget strategy: ~5 calls per user prompt, ~5 prompts/day = 750/month.
"""
import asyncio
import httpx
from data.cache import cache

try:
    from langsmith import traceable
except ImportError:
    def traceable(*args, **kwargs):
        def _noop(fn):
            return fn
        if args and callable(args[0]):
            return args[0]
        return _noop


TAVILY_TTL = 300  # 5 minutes — same as FMP/Finviz
TAVILY_NEWS_TTL = 600  # 10 minutes for broad market news


class TavilyProvider:
    """Batched web search via Tavily for analyst ratings, news, and sentiment."""

    BASE_URL = "https://api.tavily.com/search"

    def __init__(self, api_key: str):
        self.api_key = api_key

    @traceable(name="search")
    async def _search(self, query: str, max_results: int = 8,
                      search_depth: str = "basic", topic: str = "news") -> dict:
        """Execute a single Tavily search."""
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    self.BASE_URL,
                    json={
                        "query": query,
                        "api_key": self.api_key,
                        "search_depth": search_depth,
                        "max_results": max_results,
                        "topic": topic,
                        "include_answer": True,
                    },
                    timeout=12.0,
                )
            if resp.status_code != 200:
                print(f"[Tavily] HTTP {resp.status_code} for query: {query[:60]}")
                return {"error": f"HTTP {resp.status_code}", "results": []}
            return resp.json()
        except Exception as e:
            print(f"[Tavily] Error: {e}")
            return {"error": str(e), "results": []}

    @traceable(name="search_ticker_batch")
    async def search_ticker_batch(self, tickers: list,
                                  focus: str = "analyst_ratings_news") -> dict:
        """
        Search for multiple tickers in a single Tavily call.
        Returns parsed results keyed by ticker.

        Args:
            tickers: List of ticker symbols (e.g. ["AAPL", "NVDA", "TSLA"])
            focus: What to search for. Options:
                - "analyst_ratings_news" (default): analyst ratings, price targets, recent news
                - "sentiment": social sentiment and market buzz
                - "fundamentals": P/E, market cap, revenue, earnings
        """
        if not tickers:
            return {}

        ticker_str = ", ".join(tickers[:6])  # Max 6 per batch for quality

        if focus == "analyst_ratings_news":
            query = f"Latest analyst ratings, price targets, and breaking news for {ticker_str} stocks today"
        elif focus == "sentiment":
            query = f"Market sentiment, social media buzz, and investor opinion on {ticker_str} stocks"
        elif focus == "fundamentals":
            query = f"Key fundamentals: P/E ratio, market cap, revenue growth, earnings for {ticker_str}"
        else:
            query = f"Latest analyst ratings and news for {ticker_str}"

        cache_key = f"tavily:batch:{focus}:{':'.join(sorted(t.upper() for t in tickers[:6]))}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        data = await self._search(query, max_results=10, topic="news")

        result = self._parse_batch_results(tickers, data)
        cache.set(cache_key, result, TAVILY_TTL)
        return result

    @traceable(name="parse_batch_results")
    def _parse_batch_results(self, tickers: list, raw: dict) -> dict:
        """Parse Tavily results and attribute them to tickers."""
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

    @traceable(name="enrich_tickers_batched")
    async def enrich_tickers_batched(self, tickers: list) -> dict:
        """
        Full enrichment replacing StockTwits + StockAnalysis + AlphaVantage.
        Runs 2 batched searches: one for ratings/news, one for sentiment.
        Uses ~2 Tavily calls for up to 12 tickers.

        Returns dict keyed by ticker with combined data.
        """
        if not tickers:
            return {}

        cache_key = f"tavily:enriched:{':'.join(sorted(t.upper() for t in tickers[:12]))}"
        cached = cache.get(cache_key)
        if cached is not None:
            print(f"[Tavily] enrichment cache hit for {len(tickers)} tickers")
            return cached

        # Split into batches of 6 for quality
        batches = [tickers[i:i + 6] for i in range(0, len(tickers), 6)]

        tasks = []
        for batch in batches[:2]:  # Max 2 batches = 12 tickers
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

        print(f"[Tavily] enriched {len([k for k in combined if not k.startswith('_')])} tickers with {len(batches)} API calls")
        cache.set(cache_key, combined, TAVILY_TTL)
        return combined

    @traceable(name="get_market_news")
    async def get_market_news(self, topic: str = "stock market today") -> dict:
        """
        Get broad market news — replaces AlphaVantage get_market_news_sentiment().
        Single Tavily call for market-wide news context.
        """
        cache_key = f"tavily:market_news:{topic.replace(' ', '_')[:30]}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        data = await self._search(
            query=f"{topic} latest developments breaking news",
            max_results=10,
            topic="news",
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

        cache.set(cache_key, result, TAVILY_NEWS_TTL)
        return result

    @traceable(name="get_ticker_news_sentiment")
    async def get_ticker_news_sentiment(self, ticker: str, company_name: str = "") -> dict:
        """
        Get news + sentiment for a single ticker.
        Replaces AlphaVantage get_news_sentiment(ticker).
        """
        ticker = ticker.upper()
        cache_key = f"tavily:ticker_news_v2:{ticker}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        # Build a highly specific query — company name in quotes to force exact match
        short_name = ""
        if company_name:
            # Extract the core company name (e.g. "Asana" from "Asana, Inc.")
            short_name = company_name.split(",")[0].split(" Inc")[0].split(" Corp")[0].split(" Ltd")[0].split(" Group")[0].strip()
        if short_name:
            query = f'"{short_name}" {ticker} earnings'
        else:
            query = f"{ticker} stock earnings report"

        data = await self._search(
            query=query,
            max_results=8,
            topic="news",
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

            # Require ticker or company name in the TITLE for strong relevance
            in_title = (
                ticker_lower in title_lower
                or (short_name_lower and short_name_lower in title_lower)
            )

            # Or require multiple mentions in body (article is primarily about this company)
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

        # Simple sentiment heuristic from Tavily's AI answer
        answer = data.get("answer", "").lower()
        sentiment_label = "Neutral"
        if any(w in answer for w in ["bullish", "upgrade", "beat", "surge", "rally", "outperform"]):
            sentiment_label = "Bullish"
        elif any(w in answer for w in ["bearish", "downgrade", "miss", "decline", "underperform", "sell"]):
            sentiment_label = "Bearish"

        result = {
            "ticker": ticker,
            "article_count": len(articles),
            "summary": data.get("answer", ""),
            "sentiment_label": sentiment_label,
            "articles": articles,
        }

        cache.set(cache_key, result, TAVILY_TTL)
        return result
