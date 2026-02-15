import time
from polygon import RESTClient
from data.cache import cache, POLYGON_SNAPSHOT_TTL, POLYGON_TECHNICALS_TTL, POLYGON_DETAILS_TTL, POLYGON_NEWS_TTL


class PolygonProvider:
    def __init__(self, api_key: str):
        self.client = RESTClient(api_key=api_key)

    def _retry_on_rate_limit(self, func, *args, max_retries=2, delay=0.5, **kwargs):
        for attempt in range(max_retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                error_str = str(e)
                if "429" in error_str or "too many" in error_str.lower() or "rate" in error_str.lower():
                    if attempt < max_retries:
                        time.sleep(delay * (attempt + 1))
                        continue
                raise

    def get_snapshot(self, ticker: str) -> dict:
        """Get current price, volume, and daily change for a ticker."""
        cache_key = f"polygon:snapshot:{ticker.upper()}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            snap = self._retry_on_rate_limit(self.client.get_snapshot_ticker, "stocks", ticker.upper())
            result = {
                "ticker": ticker.upper(),
                "price": snap.day.close if snap.day else None,
                "open": snap.day.open if snap.day else None,
                "high": snap.day.high if snap.day else None,
                "low": snap.day.low if snap.day else None,
                "volume": snap.day.volume if snap.day else None,
                "change_pct": snap.todays_change_percent,
                "prev_close": snap.prev_day.close if snap.prev_day else None,
            }
            cache.set(cache_key, result, POLYGON_SNAPSHOT_TTL)
            return result
        except Exception as e:
            print(f"Error getting snapshot for {ticker}: {e}")
            return {"ticker": ticker.upper(), "error": str(e)}

    def get_market_movers(self) -> dict:
        """Get top gainers and losers for the day."""
        cache_key = "polygon:movers"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            gainers = list(self._retry_on_rate_limit(self.client.get_snapshot_direction, "stocks", "gainers"))
            losers = list(self._retry_on_rate_limit(self.client.get_snapshot_direction, "stocks", "losers"))
            result = {
                "gainers": [
                    {
                        "ticker": t.ticker,
                        "price": t.day.close if t.day else None,
                        "change_pct": t.todays_change_percent,
                        "volume": t.day.volume if t.day else None,
                    }
                    for t in gainers[:15]
                ],
                "losers": [
                    {
                        "ticker": t.ticker,
                        "price": t.day.close if t.day else None,
                        "change_pct": t.todays_change_percent,
                        "volume": t.day.volume if t.day else None,
                    }
                    for t in losers[:15]
                ],
            }
            cache.set(cache_key, result, POLYGON_SNAPSHOT_TTL)
            return result
        except Exception as e:
            print(f"Error getting market movers: {e}")
            return {"gainers": [], "losers": [], "error": str(e)}

    def get_news(self, ticker: str = None, limit: int = 15) -> list:
        """Get recent news articles, optionally filtered by ticker."""
        cache_key = f"polygon:news:{ticker}:{limit}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            params = {"limit": limit}
            if ticker:
                params["ticker"] = ticker.upper()
            news_items = list(self._retry_on_rate_limit(self.client.list_ticker_news, **params))
            result = [
                {
                    "title": n.title,
                    "summary": getattr(n, "description", ""),
                    "source": n.publisher.name if n.publisher else "Unknown",
                    "published": str(n.published_utc),
                    "url": getattr(n, "article_url", ""),
                }
                for n in news_items
            ]
            cache.set(cache_key, result, POLYGON_NEWS_TTL)
            return result
        except Exception as e:
            print(f"Error getting news: {e}")
            return []

    def get_technicals(self, ticker: str) -> dict:
        """Get RSI, SMA, and MACD indicators for a ticker."""
        cache_key = f"polygon:technicals:{ticker.upper()}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        result = {}
        ticker = ticker.upper()
        try:
            rsi = list(self._retry_on_rate_limit(self.client.get_rsi, ticker, timespan="day", limit=1))
            result["rsi"] = rsi[0].value if rsi else None
        except Exception:
            result["rsi"] = None

        try:
            sma_20 = list(
                self._retry_on_rate_limit(self.client.get_sma, ticker, timespan="day", window=20, limit=1)
            )
            result["sma_20"] = sma_20[0].value if sma_20 else None
        except Exception:
            result["sma_20"] = None

        try:
            sma_50 = list(
                self._retry_on_rate_limit(self.client.get_sma, ticker, timespan="day", window=50, limit=1)
            )
            result["sma_50"] = sma_50[0].value if sma_50 else None
        except Exception:
            result["sma_50"] = None

        try:
            macd = list(self._retry_on_rate_limit(self.client.get_macd, ticker, timespan="day", limit=1))
            if macd:
                result["macd"] = macd[0].value
                result["macd_signal"] = macd[0].signal
                result["macd_histogram"] = macd[0].histogram
            else:
                result["macd"] = None
                result["macd_signal"] = None
                result["macd_histogram"] = None
        except Exception:
            result["macd"] = None
            result["macd_signal"] = None
            result["macd_histogram"] = None

        cache.set(cache_key, result, POLYGON_TECHNICALS_TTL)
        return result

    def get_ticker_details(self, ticker: str) -> dict:
        """Get company info: name, sector, market cap, etc."""
        cache_key = f"polygon:details:{ticker.upper()}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            details = self._retry_on_rate_limit(self.client.get_ticker_details, ticker.upper())
            result = {
                "name": details.name,
                "sector": getattr(details, "sic_description", "Unknown"),
                "market_cap": getattr(details, "market_cap", None),
                "description": getattr(details, "description", ""),
            }
            cache.set(cache_key, result, POLYGON_DETAILS_TTL)
            return result
        except Exception as e:
            print(f"Error getting details for {ticker}: {e}")
            return {"name": ticker.upper(), "error": str(e)}

    def get_ticker_events(self, ticker: str) -> dict:
        """Get upcoming earnings, dividends, and recent news catalysts."""
        ticker = ticker.upper()
        result = {"earnings": None, "news": []}

        try:
            news = list(self._retry_on_rate_limit(self.client.list_ticker_news, ticker=ticker, limit=10))
            result["news"] = [
                {
                    "title": n.title,
                    "summary": getattr(n, "description", ""),
                    "source": n.publisher.name if n.publisher else "Unknown",
                    "published": str(n.published_utc),
                }
                for n in news
            ]
        except Exception as e:
            print(f"Error getting events for {ticker}: {e}")

        return result