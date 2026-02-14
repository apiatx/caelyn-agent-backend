import httpx


class StockTwitsProvider:
    """Fetches sentiment and trending data from StockTwits."""

    BASE_URL = "https://api.stocktwits.com/api/2"

    async def get_sentiment(self, ticker: str) -> dict:
        """Get sentiment and recent messages for a specific ticker."""
        ticker = ticker.upper()
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"{self.BASE_URL}/streams/symbol/{ticker}.json",
                    timeout=15,
                )
            data = resp.json()

            if resp.status_code != 200:
                return {"ticker": ticker, "error": "Could not fetch data"}

            symbol_data = data.get("symbol", {})
            messages = data.get("messages", [])

            # Count bullish vs bearish sentiment from recent messages
            bullish = 0
            bearish = 0
            for msg in messages:
                sentiment = msg.get("entities", {}).get("sentiment", {})
                if sentiment:
                    if sentiment.get("basic") == "Bullish":
                        bullish += 1
                    elif sentiment.get("basic") == "Bearish":
                        bearish += 1

            total_rated = bullish + bearish
            bull_pct = round((bullish / total_rated) * 100) if total_rated > 0 else None

            # Get the most recent messages as context
            recent_messages = []
            for msg in messages[:5]:
                recent_messages.append({
                    "body": msg.get("body", "")[:200],
                    "sentiment": msg.get("entities", {}).get("sentiment", {}).get("basic"),
                    "created_at": msg.get("created_at"),
                })

            return {
                "ticker": ticker,
                "watchlist_count": symbol_data.get("watchlist_count"),
                "bullish_count": bullish,
                "bearish_count": bearish,
                "bullish_percent": bull_pct,
                "total_messages_sampled": len(messages),
                "recent_messages": recent_messages,
            }
        except Exception as e:
            print(f"StockTwits error for {ticker}: {e}")
            return {"ticker": ticker, "error": str(e)}

    async def get_trending(self) -> list:
        """Get currently trending tickers on StockTwits."""
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"{self.BASE_URL}/trending/symbols.json",
                    timeout=15,
                )
            data = resp.json()

            if resp.status_code != 200:
                return []

            symbols = data.get("symbols", [])
            return [
                {
                    "ticker": s.get("symbol"),
                    "title": s.get("title"),
                    "watchlist_count": s.get("watchlist_count"),
                }
                for s in symbols[:15]
            ]
        except Exception as e:
            print(f"StockTwits trending error: {e}")
            return []