import asyncio
import httpx
from data.cache import cache

REDDIT_CACHE_TTL = 300


class RedditSentimentProvider:
    BASE_URL = "https://apewisdom.io/api/v1.0"

    async def _get(self, endpoint: str) -> dict:
        cache_key = f"reddit:{endpoint}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(
                    f"{self.BASE_URL}/{endpoint}",
                    headers={"User-Agent": "TradingAgent/1.0"},
                )
            if resp.status_code != 200:
                return {}
            data = resp.json()
            cache.set(cache_key, data, REDDIT_CACHE_TTL)
            return data
        except Exception as e:
            print(f"[REDDIT] Request failed ({endpoint}): {e}")
            return {}

    async def get_wsb_trending(self) -> list:
        data = await self._get("filter/wallstreetbets")
        results = data.get("results", [])

        for r in results:
            prev = int(r.get("mentions_24h_ago") or 0)
            curr = int(r.get("mentions") or 0)
            if prev > 0:
                r["mention_change_pct"] = round((curr - prev) / prev * 100, 1)
            else:
                r["mention_change_pct"] = None

        return results[:30]

    async def get_all_stocks_trending(self) -> list:
        data = await self._get("filter/all-stocks")
        results = data.get("results", [])

        for r in results:
            prev = int(r.get("mentions_24h_ago") or 0)
            curr = int(r.get("mentions") or 0)
            if prev > 0:
                r["mention_change_pct"] = round((curr - prev) / prev * 100, 1)
            else:
                r["mention_change_pct"] = None

        return results[:30]

    async def get_crypto_trending(self) -> list:
        data = await self._get("filter/all-crypto")
        return data.get("results", [])[:20]

    async def get_ticker_rank(self, ticker: str) -> dict:
        all_stocks = await self.get_all_stocks_trending()
        for stock in all_stocks:
            if stock.get("ticker", "").upper() == ticker.upper():
                return {
                    "rank": stock.get("rank"),
                    "mentions": stock.get("mentions"),
                    "upvotes": stock.get("upvotes"),
                    "mention_change_pct": stock.get("mention_change_pct"),
                    "on_reddit": True,
                }
        return {"on_reddit": False}

    async def get_full_reddit_dashboard(self) -> dict:
        wsb, all_stocks, crypto = await asyncio.gather(
            self.get_wsb_trending(),
            self.get_all_stocks_trending(),
            self.get_crypto_trending(),
            return_exceptions=True,
        )

        return {
            "wsb_trending": wsb if not isinstance(wsb, Exception) else [],
            "all_stocks_trending": all_stocks if not isinstance(all_stocks, Exception) else [],
            "crypto_trending": crypto if not isinstance(crypto, Exception) else [],
        }
