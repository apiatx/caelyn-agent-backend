import httpx
from data.cache import cache, STOCKTWITS_TTL


class StockTwitsProvider:
    """Fetches sentiment and trending data from StockTwits."""

    BASE_URL = "https://api.stocktwits.com/api/2"

    async def get_sentiment(self, ticker: str) -> dict:
        """Get sentiment and recent messages for a specific ticker."""
        ticker = ticker.upper()
        cache_key = f"stocktwits:sentiment:{ticker}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"{self.BASE_URL}/streams/symbol/{ticker}.json",
                    timeout=15,
                )
            if resp.status_code != 200:
                return {"ticker": ticker, "error": f"HTTP {resp.status_code}"}

            data = resp.json()
            if not data or not isinstance(data, dict):
                return {"ticker": ticker, "error": "Empty response"}

            symbol_data = data.get("symbol") or {}
            messages = data.get("messages") or []

            bullish = 0
            bearish = 0
            for msg in messages:
                if not msg or not isinstance(msg, dict):
                    continue
                entities = msg.get("entities") or {}
                sentiment = entities.get("sentiment") or {}
                if isinstance(sentiment, dict):
                    basic = sentiment.get("basic")
                    if basic == "Bullish":
                        bullish += 1
                    elif basic == "Bearish":
                        bearish += 1

            total_rated = bullish + bearish
            bull_pct = round((bullish / total_rated) * 100) if total_rated > 0 else None

            recent_messages = []
            for msg in messages[:5]:
                if not msg or not isinstance(msg, dict):
                    continue
                entities = msg.get("entities") or {}
                sentiment = entities.get("sentiment") or {}
                recent_messages.append({
                    "body": (msg.get("body") or "")[:200],
                    "sentiment": sentiment.get("basic") if isinstance(sentiment, dict) else None,
                    "created_at": msg.get("created_at"),
                })

            result = {
                "ticker": ticker,
                "watchlist_count": symbol_data.get("watchlist_count"),
                "bullish_count": bullish,
                "bearish_count": bearish,
                "bullish_percent": bull_pct,
                "total_messages_sampled": len(messages),
                "recent_messages": recent_messages,
            }
            cache.set(cache_key, result, STOCKTWITS_TTL)
            return result
        except Exception as e:
            print(f"StockTwits error for {ticker}: {e}")
            return {"ticker": ticker, "error": str(e)}

    async def get_trending(self) -> list:
        """Get currently trending tickers on StockTwits."""
        cache_key = "stocktwits:trending"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"{self.BASE_URL}/trending/symbols.json",
                    timeout=15,
                )
            if resp.status_code != 200:
                return []

            data = resp.json()
            if not data or not isinstance(data, dict):
                return []

            symbols = data.get("symbols") or []
            result = [
                {
                    "ticker": s.get("symbol"),
                    "title": s.get("title"),
                    "watchlist_count": s.get("watchlist_count"),
                }
                for s in symbols[:15]
                if s and isinstance(s, dict)
            ]
            cache.set(cache_key, result, STOCKTWITS_TTL)
            return result
        except Exception as e:
            print(f"StockTwits trending error: {e}")
            return []