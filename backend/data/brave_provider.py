"""
Brave Search API provider for market intelligence.
Drop-in replacement for TavilyProvider using Brave's Web Search and News Search APIs.

Endpoints:
  - Web Search:  GET https://api.search.brave.com/res/v1/web/search
  - News Search: GET https://api.search.brave.com/res/v1/news/search

Free tier: $5/month credit = ~1,000 requests at $5/1K.
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


BRAVE_TTL = 300       # 5 minutes — matches Tavily TTL
BRAVE_NEWS_TTL = 600  # 10 minutes for broad market news


class BraveProvider:
    """Web search via Brave Search API — same interface as TavilyProvider."""

    WEB_URL = "https://api.search.brave.com/res/v1/web/search"
    NEWS_URL = "https://api.search.brave.com/res/v1/news/search"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self._headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "X-Subscription-Token": api_key,
        }

    @traceable(name="search")
    async def _search(self, query: str, max_results: int = 8,
                      search_depth: str = "basic", topic: str = "news") -> dict:
        """
        Execute a Brave search. Uses News endpoint for topic="news",
        Web endpoint otherwise. Returns Tavily-compatible response format.
        """
        is_news = topic == "news"
        url = self.NEWS_URL if is_news else self.WEB_URL

        params = {
            "q": query,
            "count": min(max_results, 20),
            "extra_snippets": "true",
            "text_decorations": "false",
            "freshness": "pd" if is_news else "pw",  # past day for news, past week for web
        }

        # Web endpoint can include news in results too
        if not is_news:
            params["result_filter"] = "web,news"

        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    url,
                    headers=self._headers,
                    params=params,
                    timeout=12.0,
                )
            if resp.status_code == 429:
                print(f"[Brave] Rate limited for query: {query[:60]}")
                return {"error": "rate_limited", "results": []}
            if resp.status_code != 200:
                print(f"[Brave] HTTP {resp.status_code} for query: {query[:60]}")
                return {"error": f"HTTP {resp.status_code}", "results": []}

            raw = resp.json()
            return self._normalize_response(raw, is_news)

        except Exception as e:
            print(f"[Brave] Error: {e}")
            return {"error": str(e), "results": []}

    @traceable(name="normalize_response")
    def _normalize_response(self, raw: dict, is_news: bool) -> dict:
        """
        Convert Brave response to Tavily-compatible format so the rest
        of the codebase doesn't need to change.

        Tavily format:
          {"answer": "...", "results": [{"title": ..., "content": ..., "url": ...}]}
        """
        results = []

        if is_news:
            # News endpoint: raw["results"] is a list of news articles
            for item in raw.get("results", []):
                content = item.get("description", "")
                extra = item.get("extra_snippets", [])
                if extra:
                    content = content + " " + " ".join(extra)
                results.append({
                    "title": item.get("title", ""),
                    "content": content,
                    "url": item.get("url", ""),
                    "age": item.get("age", ""),
                })
        else:
            # Web endpoint: results are in raw["web"]["results"]
            for item in raw.get("web", {}).get("results", []):
                content = item.get("description", "")
                extra = item.get("extra_snippets", [])
                if extra:
                    content = content + " " + " ".join(extra)
                results.append({
                    "title": item.get("title", ""),
                    "content": content,
                    "url": item.get("url", ""),
                    "age": item.get("age", ""),
                })

            # Also pull in any inline news results from web endpoint
            for item in raw.get("news", {}).get("results", []):
                results.append({
                    "title": item.get("title", ""),
                    "content": item.get("description", ""),
                    "url": item.get("url", ""),
                    "age": item.get("age", ""),
                })

        return {
            "answer": "",  # Brave doesn't generate AI summaries (that's Answers API)
            "results": results,
        }

    # ── Public methods matching TavilyProvider interface ──────────────

    @traceable(name="search_ticker_batch")
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

        cache_key = f"brave:batch:{focus}:{':'.join(sorted(t.upper() for t in tickers[:6]))}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        data = await self._search(query, max_results=10, topic="news")

        result = self._parse_batch_results(tickers, data)
        cache.set(cache_key, result, BRAVE_TTL)
        return result

    @traceable(name="parse_batch_results")
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

    @traceable(name="enrich_tickers_batched")
    async def enrich_tickers_batched(self, tickers: list) -> dict:
        """
        Full enrichment for up to 12 tickers.
        Runs batches of 6, ~2 API calls total.
        """
        if not tickers:
            return {}

        cache_key = f"brave:enriched:{':'.join(sorted(t.upper() for t in tickers[:12]))}"
        cached = cache.get(cache_key)
        if cached is not None:
            print(f"[Brave] enrichment cache hit for {len(tickers)} tickers")
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

        print(f"[Brave] enriched {len([k for k in combined if not k.startswith('_')])} tickers with {len(batches)} API calls")
        cache.set(cache_key, combined, BRAVE_TTL)
        return combined

    @traceable(name="get_market_news")
    async def get_market_news(self, topic: str = "stock market today") -> dict:
        """Get broad market news — same interface as TavilyProvider."""
        cache_key = f"brave:market_news:{topic.replace(' ', '_')[:30]}"
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

        cache.set(cache_key, result, BRAVE_NEWS_TTL)
        return result

    @traceable(name="get_ticker_news_sentiment")
    async def get_ticker_news_sentiment(self, ticker: str, company_name: str = "") -> dict:
        """Get news + sentiment for a single ticker."""
        ticker = ticker.upper()
        cache_key = f"brave:ticker_news_v2:{ticker}"
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

        # Simple sentiment heuristic from article text (same as TavilyProvider)
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
            "summary": "",  # No AI summary from Search API
            "sentiment_label": sentiment_label,
            "articles": articles,
        }

        cache.set(cache_key, result, BRAVE_TTL)
        return result
