import time
import threading
import requests
from datetime import datetime, timedelta
from data.cache import cache, POLYGON_SNAPSHOT_TTL, POLYGON_TECHNICALS_TTL, POLYGON_DETAILS_TTL, POLYGON_NEWS_TTL


class PolygonProvider:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.polygon.io"
        self._rate_lock = threading.Lock()
        self._call_times = []
        self._max_per_minute = 4

    def _request(self, path: str, params: dict = None, timeout: int = 8) -> dict:
        if params is None:
            params = {}
        params["apiKey"] = self.api_key

        with self._rate_lock:
            now = time.time()
            self._call_times = [t for t in self._call_times if now - t < 60]

            if len(self._call_times) >= self._max_per_minute:
                print(f"[Polygon] Rate limit reached ({self._max_per_minute}/min), skipping")
                return {}

            self._call_times.append(now)

        try:
            resp = requests.get(f"{self.base_url}{path}", params=params, timeout=timeout)
            if resp.status_code == 429:
                print("[Polygon] 429 rate limited, skipping (no retry)")
                return {"error": "rate_limited", "status": 429}
            if resp.status_code == 403:
                return {"error": "not_authorized", "status": 403}
            if resp.status_code != 200:
                return {"error": f"HTTP {resp.status_code}", "status": resp.status_code}
            return resp.json()
        except requests.exceptions.Timeout:
            print(f"[Polygon] Request timed out: {path}")
            return {"error": "timeout"}
        except Exception as e:
            print(f"[Polygon] Request error: {e}")
            return {"error": str(e)}

    def get_daily_bars(self, ticker: str, days: int = 120) -> list:
        """Fetch daily OHLCV bars. Cached separately since multiple methods use it."""
        ticker = ticker.upper()
        cache_key = f"polygon:bars:{ticker}:{days}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        end = datetime.now().strftime("%Y-%m-%d")
        start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

        data = self._request(
            f"/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}",
            params={"adjusted": "true", "sort": "asc", "limit": days},
        )
        if "error" in data:
            print(f"Polygon bars failed for {ticker}: {data['error']}")
            return []

        bars = data.get("results") or []
        cache.set(cache_key, bars, POLYGON_TECHNICALS_TTL)
        return bars

    def get_snapshot(self, ticker: str) -> dict:
        """Get latest price data from daily bars (works on free tier)."""
        ticker = ticker.upper()
        cache_key = f"polygon:snapshot:{ticker}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            bars = self.get_daily_bars(ticker)
            if not bars:
                return {"ticker": ticker, "error": "no_data"}

            bar = bars[-1]
            prev_bar = bars[-2] if len(bars) >= 2 else None

            open_price = bar.get("o")
            close_price = bar.get("c")
            change_pct = None
            if prev_bar and prev_bar.get("c") and prev_bar["c"] > 0 and close_price:
                change_pct = round(((close_price - prev_bar["c"]) / prev_bar["c"]) * 100, 2)
            elif open_price and open_price > 0 and close_price:
                change_pct = round(((close_price - open_price) / open_price) * 100, 2)

            result = {
                "ticker": ticker,
                "price": close_price,
                "open": open_price,
                "high": bar.get("h"),
                "low": bar.get("l"),
                "volume": bar.get("v"),
                "vwap": bar.get("vw"),
                "change_pct": change_pct,
            }
            cache.set(cache_key, result, POLYGON_SNAPSHOT_TTL)
            return result
        except Exception as e:
            print(f"Polygon snapshot error for {ticker}: {e}")
            return {"ticker": ticker, "error": str(e)}

    def get_market_movers(self) -> dict:
        """
        Get top gainers and losers.
        Tries Polygon snapshot endpoint first (paid tier).
        Falls back to FMP stock_market/gainers and stock_market/losers (free tier).
        """
        cache_key = "polygon:movers"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            data = self._request("/v2/snapshot/locale/us/markets/stocks/gainers")
            if "error" not in data and data.get("tickers"):
                gainers = []
                for t in (data.get("tickers") or [])[:15]:
                    day = t.get("day") or {}
                    gainers.append({
                        "ticker": t.get("ticker"),
                        "price": day.get("c"),
                        "change_pct": t.get("todaysChangePerc"),
                        "volume": day.get("v"),
                    })

                data2 = self._request("/v2/snapshot/locale/us/markets/stocks/losers")
                losers = []
                if "error" not in data2:
                    for t in (data2.get("tickers") or [])[:15]:
                        day = t.get("day") or {}
                        losers.append({
                            "ticker": t.get("ticker"),
                            "price": day.get("c"),
                            "change_pct": t.get("todaysChangePerc"),
                            "volume": day.get("v"),
                        })

                result = {"gainers": gainers, "losers": losers}
                cache.set(cache_key, result, POLYGON_SNAPSHOT_TTL)
                return result
            else:
                print("[Polygon movers] Snapshot endpoint unavailable (paid tier). Falling back to FMP.")
        except Exception as e:
            print(f"[Polygon movers] Snapshot failed: {e}. Falling back to FMP.")

        try:
            import httpx
            from config import FMP_API_KEY
            if not FMP_API_KEY:
                return {"gainers": [], "losers": []}

            base = "https://financialmodelingprep.com/api/v3"
            gainers = []
            losers = []

            resp_g = httpx.get(f"{base}/stock_market/gainers", params={"apikey": FMP_API_KEY}, timeout=10)
            if resp_g.status_code == 200:
                for item in (resp_g.json() or [])[:15]:
                    if isinstance(item, dict):
                        gainers.append({
                            "ticker": item.get("symbol", ""),
                            "price": item.get("price"),
                            "change_pct": item.get("changesPercentage"),
                            "volume": item.get("volume"),
                        })

            resp_l = httpx.get(f"{base}/stock_market/losers", params={"apikey": FMP_API_KEY}, timeout=10)
            if resp_l.status_code == 200:
                for item in (resp_l.json() or [])[:15]:
                    if isinstance(item, dict):
                        losers.append({
                            "ticker": item.get("symbol", ""),
                            "price": item.get("price"),
                            "change_pct": item.get("changesPercentage"),
                            "volume": item.get("volume"),
                        })

            result = {"gainers": gainers, "losers": losers}
            if gainers or losers:
                print(f"[Polygon movers] FMP fallback: {len(gainers)} gainers, {len(losers)} losers")
                cache.set(cache_key, result, POLYGON_SNAPSHOT_TTL)
            return result
        except Exception as e2:
            print(f"[Polygon movers] FMP fallback also failed: {e2}")
            return {"gainers": [], "losers": []}

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
            data = self._request("/v2/reference/news", params=params)
            if "error" in data:
                print(f"Error getting news: {data['error']}")
                return []

            result = [
                {
                    "title": n.get("title", ""),
                    "summary": n.get("description", ""),
                    "source": (n.get("publisher") or {}).get("name", "Unknown"),
                    "published": n.get("published_utc", ""),
                    "url": n.get("article_url", ""),
                }
                for n in (data.get("results") or [])
            ]
            cache.set(cache_key, result, POLYGON_NEWS_TTL)
            return result
        except Exception as e:
            print(f"Error getting news: {e}")
            return []

    def get_technicals(self, ticker: str) -> dict:
        """Calculate technicals from daily bars using shared ta_utils (works on free Polygon tier)."""
        from data.ta_utils import compute_technicals_from_bars
        ticker = ticker.upper()
        cache_key = f"polygon:technicals:{ticker}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        try:
            bars = self.get_daily_bars(ticker)
            if len(bars) < 20:
                return {}

            result = compute_technicals_from_bars(bars)
            if not result:
                return {}

            result = {
                "rsi": result.get("rsi"),
                "sma_20": result.get("sma_20"),
                "sma_50": result.get("sma_50"),
                "macd": result.get("macd"),
                "macd_signal": result.get("macd_signal"),
                "macd_histogram": result.get("macd_histogram"),
                "avg_volume": result.get("avg_volume"),
            }
            cache.set(cache_key, result, POLYGON_TECHNICALS_TTL)
            return result
        except Exception as e:
            print(f"Polygon technicals error for {ticker}: {e}")
            return {}

    def get_ticker_details(self, ticker: str) -> dict:
        """Get company info: name, sector, market cap, etc."""
        ticker = ticker.upper()
        cache_key = f"polygon:details:{ticker}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            data = self._request(f"/v3/reference/tickers/{ticker}")
            if "error" in data:
                print(f"Error getting details for {ticker}: {data['error']}")
                return {"name": ticker, "error": data["error"]}

            details = data.get("results") or {}
            result = {
                "name": details.get("name", ticker),
                "sector": details.get("sic_description", "Unknown"),
                "market_cap": details.get("market_cap"),
                "description": details.get("description", ""),
            }
            cache.set(cache_key, result, POLYGON_DETAILS_TTL)
            return result
        except Exception as e:
            print(f"Error getting details for {ticker}: {e}")
            return {"name": ticker, "error": str(e)}

    def get_ticker_events(self, ticker: str) -> dict:
        """Get upcoming earnings, dividends, and recent news catalysts."""
        ticker = ticker.upper()
        result = {"earnings": None, "news": []}

        try:
            news_data = self._request("/v2/reference/news", params={"ticker": ticker, "limit": 10})
            if "error" not in news_data:
                result["news"] = [
                    {
                        "title": n.get("title", ""),
                        "summary": n.get("description", ""),
                        "source": (n.get("publisher") or {}).get("name", "Unknown"),
                        "published": n.get("published_utc", ""),
                    }
                    for n in (news_data.get("results") or [])
                ]
        except Exception as e:
            print(f"Error getting events for {ticker}: {e}")

        return result
