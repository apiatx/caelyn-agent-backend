from data.polygon_provider import PolygonProvider
from data.finviz_scraper import FinvizScraper


class MarketDataService:
    """
    Unified interface for all market data.
    Your agent talks to THIS — never directly to Polygon or scrapers.
    """

    def __init__(self, polygon_key: str):
        self.polygon = PolygonProvider(polygon_key)
        self.finviz = FinvizScraper()

    async def research_ticker(self, ticker: str) -> dict:
        """
        Get everything about a single stock.
        Used when someone asks "analyze NVDA" or "what's happening with AAPL".
        """
        ticker = ticker.upper()
        return {
            "snapshot": self.polygon.get_snapshot(ticker),
            "technicals": self.polygon.get_technicals(ticker),
            "details": self.polygon.get_ticker_details(ticker),
            "news": self.polygon.get_news(ticker, limit=10),
        }

    async def scan_market(self) -> dict:
        """
        Broad market overview.
        Used for "best trades today", "what's moving", etc.
        """
        return {
            "movers": self.polygon.get_market_movers(),
            "news": self.polygon.get_news(limit=15),
            "screener_gainers": await self.finviz.get_screener_results(
                "ta_topgainers"
            ),
        }

    async def get_unusual_volume(self) -> dict:
        """
        Stocks with unusual volume spikes.
        Used for "unusual volume" or "what stocks are seeing big volume".
        """
        return {
            "unusual_volume": await self.finviz.get_screener_results(
                "ta_unusualvolume"
            ),
        }

    async def get_oversold(self) -> dict:
        """Stocks that are oversold based on RSI."""
        return {
            "oversold": await self.finviz.get_screener_results("ta_oversold"),
        }

    async def get_overbought(self) -> dict:
        """Stocks that are overbought based on RSI."""
        return {
            "overbought": await self.finviz.get_screener_results(
                "ta_overbought"
            ),
        }