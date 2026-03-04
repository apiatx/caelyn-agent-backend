"""
Perplexity Sonar API provider for market intelligence.
Drop-in replacement for BraveProvider/TavilyProvider -- same 4-method interface.

Endpoint: POST https://api.perplexity.ai/chat/completions
Model: sonar (web-search grounded)
Pricing: $5 per 1,000 requests (flat, no token cost).

Features:
  - Web search grounded responses with citations
  - Domain allowlists (search_domain_filter)
  - Recency filters (day, week, month, year)
  - Returns citations as URLs
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
    "cnbc.com",
    "wsj.com",
    "finance.yahoo.com",
    "marketwatch.com",
    "seekingalpha.com",
    "barrons.com",
    "sec.gov",
    "fool.com",
    "investing.com",
    "benzinga.com",
    "thestreet.com",
    "ft.com",
]


class PerplexityProvider:
    """Web search via Perplexity Sonar API -- same interface as BraveProvider/TavilyProvider."""

    SONAR_URL = "https://api.perplexity.ai/chat/completions"

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
        Execute a Perplexity Sonar chat/completions call with web search grounding.
        Returns a Brave/Tavily-compatible response format:
          {"answer": "...", "results": [{"title", "content", "url", "age"}]}
        """
        body = {
            "model": "sonar",
            "messages": [
                {"role": "system", "content": "You are a financial research assistant. Provide detailed, sourced answers about markets, stocks, and economic news."},
                {"role": "user", "content": str(query) if isinstance(query, str) else " | ".join(query)},
            ],
            "temperature": 0.1,
            "max_tokens": 1500,
            "return_citations": True,
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
                    self.SONAR_URL,
                    headers=self._headers,
                    json=body,
                    timeout=15.0,
                )
            if resp.status_code == 429:
                print(f"[Perplexity] Rate limited for query: {str(query)[:60]}")
                return {"error": "rate_limited", "results": []}
            if resp.status_code == 451:
                print(f"[Perplexity] HTTP 451 -- API key may need regeneration")
                return {"error": "invalid_key", "results": []}
            if resp.status_code != 200:
                print(f"[Perplexity] HTTP {resp.status_code} for query: {str(query)[:60]}: {resp.text[:200]}")
                return {"error": f"HTTP {resp.status_code}", "results": []}

            raw = resp.json()
            return self._normalize_response(raw)

        except Exception as e:
            print(f"[Perplexity] Error: {e}")
            return {"error": str(e), "results": []}

    def _normalize_response(self, raw: dict) -> dict:
        """
        Convert Perplexity Sonar response to the Brave/Tavily-compatible format.

        Sonar format:
          {"choices": [{"message": {"content": "..."}}], "citations": ["url1", ...]}

        Normalized format:
          {"answer": "...", "results": [{"title", "content", "url", "age"}]}
        """
        content = raw.get("choices", [{}])[0].get("message", {}).get("content", "")
        citations = raw.get("citations", [])

        results = []
        for url in citations:
            if isinstance(url, str) and url.startswith("http"):
                domain = url.split("/")[2].replace("www.", "") if len(url.split("/")) > 2 else ""
                results.append({
                    "title": domain,
                    "content": "",
                    "url": url,
                    "age": "",
                })

        return {
            "answer": content,
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
