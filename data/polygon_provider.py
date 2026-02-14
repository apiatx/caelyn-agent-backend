from polygon import RESTClient


class PolygonProvider:
    def __init__(self, api_key: str):
        self.client = RESTClient(api_key=api_key)

    def get_snapshot(self, ticker: str) -> dict:
        """Get current price, volume, and daily change for a ticker."""
        try:
            snap = self.client.get_snapshot_ticker("stocks", ticker.upper())
            return {
                "ticker": ticker.upper(),
                "price": snap.day.close if snap.day else None,
                "open": snap.day.open if snap.day else None,
                "high": snap.day.high if snap.day else None,
                "low": snap.day.low if snap.day else None,
                "volume": snap.day.volume if snap.day else None,
                "change_pct": snap.todays_change_percent,
                "prev_close": snap.prev_day.close if snap.prev_day else None,
            }
        except Exception as e:
            print(f"Error getting snapshot for {ticker}: {e}")
            return {"ticker": ticker.upper(), "error": str(e)}

    def get_market_movers(self) -> dict:
        """Get top gainers and losers for the day."""
        try:
            gainers = list(self.client.get_snapshot_direction("stocks", "gainers"))
            losers = list(self.client.get_snapshot_direction("stocks", "losers"))
            return {
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
        except Exception as e:
            print(f"Error getting market movers: {e}")
            return {"gainers": [], "losers": [], "error": str(e)}

    def get_news(self, ticker: str = None, limit: int = 15) -> list:
        """Get recent news articles, optionally filtered by ticker."""
        try:
            params = {"limit": limit}
            if ticker:
                params["ticker"] = ticker.upper()
            news_items = list(self.client.list_ticker_news(**params))
            return [
                {
                    "title": n.title,
                    "summary": getattr(n, "description", ""),
                    "source": n.publisher.name if n.publisher else "Unknown",
                    "published": str(n.published_utc),
                    "url": getattr(n, "article_url", ""),
                }
                for n in news_items
            ]
        except Exception as e:
            print(f"Error getting news: {e}")
            return []

    def get_technicals(self, ticker: str) -> dict:
        """Get RSI, SMA, and MACD indicators for a ticker."""
        result = {}
        ticker = ticker.upper()
        try:
            rsi = list(self.client.get_rsi(ticker, timespan="day", limit=1))
            result["rsi"] = rsi[0].value if rsi else None
        except Exception:
            result["rsi"] = None

        try:
            sma_20 = list(
                self.client.get_sma(ticker, timespan="day", window=20, limit=1)
            )
            result["sma_20"] = sma_20[0].value if sma_20 else None
        except Exception:
            result["sma_20"] = None

        try:
            sma_50 = list(
                self.client.get_sma(ticker, timespan="day", window=50, limit=1)
            )
            result["sma_50"] = sma_50[0].value if sma_50 else None
        except Exception:
            result["sma_50"] = None

        try:
            macd = list(self.client.get_macd(ticker, timespan="day", limit=1))
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

        return result

    def get_ticker_details(self, ticker: str) -> dict:
        """Get company info: name, sector, market cap, etc."""
        try:
            details = self.client.get_ticker_details(ticker.upper())
            return {
                "name": details.name,
                "sector": getattr(details, "sic_description", "Unknown"),
                "market_cap": getattr(details, "market_cap", None),
                "description": getattr(details, "description", ""),
            }
        except Exception as e:
            print(f"Error getting details for {ticker}: {e}")
            return {"name": ticker.upper(), "error": str(e)}