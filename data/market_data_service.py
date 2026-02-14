from data.polygon_provider import PolygonProvider
from data.finviz_scraper import FinvizScraper
from data.stocktwits_provider import StockTwitsProvider
from data.stockanalysis_scraper import StockAnalysisScraper
from data.options_scraper import OptionsScraper
from data.finnhub_provider import FinnhubProvider
from config import FINNHUB_API_KEY, ALPHA_VANTAGE_API_KEY
from data.alphavantage_provider import AlphaVantageProvider

class MarketDataService:
    """
    Unified interface for all market data.
    Your agent talks to THIS — never directly to Polygon or scrapers.
    """

    def __init__(self, polygon_key: str):
        self.polygon = PolygonProvider(polygon_key)
        self.finviz = FinvizScraper()
        self.stocktwits = StockTwitsProvider()
        self.stockanalysis = StockAnalysisScraper()
        self.options = OptionsScraper()
        self.finnhub = FinnhubProvider(FINNHUB_API_KEY)
        self.alphavantage = AlphaVantageProvider(ALPHA_VANTAGE_API_KEY)

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
            "sentiment": await self.stocktwits.get_sentiment(ticker),
            "fundamentals": await self.stockanalysis.get_overview(ticker),
            "financials": await self.stockanalysis.get_financials(ticker),
            "analyst_ratings": await self.stockanalysis.get_analyst_ratings(ticker),
            "options_put_call": await self.options.get_put_call_ratio(ticker),
            "insider_sentiment": self.finnhub.get_insider_sentiment(ticker),
            "insider_transactions": self.finnhub.get_insider_transactions(ticker),
            "earnings_history": self.finnhub.get_earnings_surprises(ticker),
            "earnings_upcoming": self.finnhub.get_earnings_calendar(ticker),
            "recommendation_trends": self.finnhub.get_recommendation_trends(ticker),
            "social_sentiment": self.finnhub.get_social_sentiment(ticker),
            "peer_companies": self.finnhub.get_company_peers(ticker),
            "news_sentiment_ai": await self.alphavantage.get_news_sentiment(ticker),
        }

    async def scan_market(self) -> dict:
        """Broad market overview with catalyst and sentiment data."""
        movers = self.polygon.get_market_movers()

        top_gainer_tickers = [
            g["ticker"] for g in movers.get("gainers", [])[:5]
        ]

        catalyst_data = {}
        for ticker in top_gainer_tickers:
            catalyst_data[ticker] = {
                "details": self.polygon.get_ticker_details(ticker),
                "technicals": self.polygon.get_technicals(ticker),
                "news": self.polygon.get_ticker_events(ticker)["news"],
                "sentiment": await self.stocktwits.get_sentiment(ticker),
                "fundamentals": await self.stockanalysis.get_overview(ticker),
                "analyst_ratings": await self.stockanalysis.get_analyst_ratings(ticker),
                "insider_sentiment": self.finnhub.get_insider_sentiment(ticker),
                "earnings_upcoming": self.finnhub.get_earnings_calendar(ticker),
            }

        trending = await self.stocktwits.get_trending()

        unusual_options = await self.options.get_unusual_options_activity()
        options_signals = self.options.interpret_flow(unusual_options)
        options_volume_leaders = await self.options.get_options_volume_leaders()

        upcoming_earnings = self.finnhub.get_upcoming_earnings()

        macro = await self.alphavantage.get_macro_overview()

        return {
            "movers": movers,
            "market_news": self.polygon.get_news(limit=15),
            "screener_gainers": await self.finviz.get_screener_results(
                "ta_topgainers"
            ),
            "catalyst_data": catalyst_data,
            "stocktwits_trending": trending,
            "unusual_options_activity": unusual_options,
            "options_flow_signals": options_signals,
            "options_volume_leaders": options_volume_leaders,
            "upcoming_earnings_this_week": upcoming_earnings,
            "macro_economic_data": macro,
        }

    async def get_options_flow(self) -> dict:
        """
        Dedicated options flow scan.
        Used for "show me unusual options activity" type queries.
        """
        unusual = await self.options.get_unusual_options_activity()
        signals = self.options.interpret_flow(unusual)
        volume_leaders = await self.options.get_options_volume_leaders()

        return {
            "unusual_activity": unusual,
            "flow_signals": signals,
            "volume_leaders": volume_leaders,
        }

    async def get_earnings_scan(self) -> dict:
        """
        Dedicated earnings scan.
        Used for "what earnings are coming up" type queries.
        """
        return {
            "upcoming_earnings": self.finnhub.get_upcoming_earnings(),
        }

    async def get_macro_overview(self) -> dict:
        """
        Dedicated macro economics scan.
        Used for "what's happening with the economy" type queries.
        """
        return await self.alphavantage.get_macro_overview()

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