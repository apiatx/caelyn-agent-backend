import asyncio

from data.polygon_provider import PolygonProvider
from data.finviz_scraper import FinvizScraper
from data.stocktwits_provider import StockTwitsProvider
from data.stockanalysis_scraper import StockAnalysisScraper
from data.options_scraper import OptionsScraper
from data.finnhub_provider import FinnhubProvider
from config import FINNHUB_API_KEY, ALPHA_VANTAGE_API_KEY, FRED_API_KEY
from data.alphavantage_provider import AlphaVantageProvider
from data.fred_provider import FredProvider
from data.edgar_provider import EdgarProvider
from data.fear_greed_provider import FearGreedProvider

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
        self.fred = FredProvider(FRED_API_KEY)
        self.edgar = EdgarProvider()
        self.fear_greed = FearGreedProvider()

    async def research_ticker(self, ticker: str) -> dict:
        """
        Get everything about a single stock — all sources in parallel.
        """
        ticker = ticker.upper()

        sync_data = {
            "snapshot": self.polygon.get_snapshot(ticker),
            "technicals": self.polygon.get_technicals(ticker),
            "details": self.polygon.get_ticker_details(ticker),
            "news": self.polygon.get_news(ticker, limit=10),
            "insider_sentiment": self.finnhub.get_insider_sentiment(ticker),
            "insider_transactions": self.finnhub.get_insider_transactions(ticker),
            "earnings_history": self.finnhub.get_earnings_surprises(ticker),
            "earnings_upcoming": self.finnhub.get_earnings_calendar(ticker),
            "recommendation_trends": self.finnhub.get_recommendation_trends(ticker),
            "social_sentiment": self.finnhub.get_social_sentiment(ticker),
            "peer_companies": self.finnhub.get_company_peers(ticker),
        }

        async_results = await asyncio.gather(
            self.stocktwits.get_sentiment(ticker),
            self.stockanalysis.get_overview(ticker),
            self.stockanalysis.get_financials(ticker),
            self.stockanalysis.get_analyst_ratings(ticker),
            self.options.get_put_call_ratio(ticker),
            self.alphavantage.get_news_sentiment(ticker),
            self.edgar.get_company_summary(ticker),
            return_exceptions=True,
        )

        async_keys = [
            "sentiment", "fundamentals", "financials", "analyst_ratings",
            "options_put_call", "news_sentiment_ai", "sec_filings",
        ]
        for key, result in zip(async_keys, async_results):
            if isinstance(result, Exception):
                sync_data[key] = {"error": str(result)}
            else:
                sync_data[key] = result

        return sync_data

    async def scan_market(self) -> dict:
        """Broad market overview — parallelized for speed."""
        movers = self.polygon.get_market_movers()

        top_gainer_tickers = [
            g["ticker"] for g in movers.get("gainers", [])[:5]
        ]

        async def get_catalyst(ticker):
            async_results = await asyncio.gather(
                self.stocktwits.get_sentiment(ticker),
                self.stockanalysis.get_overview(ticker),
                self.stockanalysis.get_analyst_ratings(ticker),
                self.edgar.get_8k_filings(ticker),
                return_exceptions=True,
            )
            return {
                "details": self.polygon.get_ticker_details(ticker),
                "technicals": self.polygon.get_technicals(ticker),
                "news": self.polygon.get_ticker_events(ticker)["news"],
                "sentiment": async_results[0] if not isinstance(async_results[0], Exception) else {},
                "fundamentals": async_results[1] if not isinstance(async_results[1], Exception) else {},
                "analyst_ratings": async_results[2] if not isinstance(async_results[2], Exception) else {},
                "insider_sentiment": self.finnhub.get_insider_sentiment(ticker),
                "earnings_upcoming": self.finnhub.get_earnings_calendar(ticker),
                "recent_sec_filings": async_results[3] if not isinstance(async_results[3], Exception) else [],
            }

        catalyst_results = await asyncio.gather(
            *[get_catalyst(t) for t in top_gainer_tickers],
            return_exceptions=True,
        )
        catalyst_data = {}
        for ticker, result in zip(top_gainer_tickers, catalyst_results):
            if not isinstance(result, Exception):
                catalyst_data[ticker] = result

        trending, unusual_options, options_volume_leaders, upcoming_earnings, fear_greed = (
            await asyncio.gather(
                self.stocktwits.get_trending(),
                self.options.get_unusual_options_activity(),
                self.options.get_options_volume_leaders(),
                asyncio.to_thread(self.finnhub.get_upcoming_earnings),
                self.fear_greed.get_fear_greed_index(),
                return_exceptions=True,
            )
        )

        if isinstance(trending, Exception): trending = []
        if isinstance(unusual_options, Exception): unusual_options = []
        if isinstance(options_volume_leaders, Exception): options_volume_leaders = []
        if isinstance(upcoming_earnings, Exception): upcoming_earnings = []
        if isinstance(fear_greed, Exception): fear_greed = {}

        options_signals = self.options.interpret_flow(unusual_options) if unusual_options else {}
        macro = self.fred.get_quick_macro()

        screener_gainers = await self.finviz.get_screener_results("ta_topgainers")

        return {
            "movers": movers,
            "market_news": self.polygon.get_news(limit=15),
            "screener_gainers": screener_gainers,
            "catalyst_data": catalyst_data,
            "stocktwits_trending": trending,
            "unusual_options_activity": unusual_options,
            "options_flow_signals": options_signals,
            "options_volume_leaders": options_volume_leaders,
            "upcoming_earnings_this_week": upcoming_earnings,
            "macro_economic_data": macro,
            "fear_greed_index": fear_greed,
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
        Dedicated macro economics dashboard.
        Used for "what's happening with the economy" type queries.
        Returns the full macro dashboard from FRED, Alpha Vantage
        market news sentiment, and Fear & Greed Index.
        """
        fred_data = self.fred.get_full_macro_dashboard()
        market_sentiment = await self.alphavantage.get_market_news_sentiment(
            "economy_macro"
        )
        fear_greed = await self.fear_greed.get_fear_greed_index()
        return {
            "macro_indicators": fred_data,
            "macro_news_sentiment": market_sentiment,
            "fear_greed_index": fear_greed,
        }

    async def get_sec_filings(self, ticker: str) -> dict:
        """
        Dedicated SEC filings lookup.
        Used for "show me SEC filings for AAPL" type queries.
        """
        return {
            "company_summary": await self.edgar.get_company_summary(ticker),
            "all_recent_filings": await self.edgar.get_recent_filings(ticker),
            "insider_filings": await self.edgar.get_insider_filings(ticker),
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

    async def get_top_ta_setups(self) -> dict:
        """
        Scan for the best technical analysis setups across stocks.
        Looks for Stage 2 breakouts, volume surges, and momentum.
        """
        movers = self.polygon.get_market_movers()
        screener_gainers = await self.finviz.get_screener_results("ta_topgainers")

        unusual_options, options_volume_leaders = await asyncio.gather(
            self.options.get_unusual_options_activity(),
            self.options.get_options_volume_leaders(),
            return_exceptions=True,
        )
        if isinstance(unusual_options, Exception): unusual_options = []
        if isinstance(options_volume_leaders, Exception): options_volume_leaders = []

        options_signals = self.options.interpret_flow(unusual_options) if unusual_options else {}

        top_tickers = [g["ticker"] for g in movers.get("gainers", [])[:10]]

        async def get_ta_for_ticker(ticker):
            return {
                "snapshot": self.polygon.get_snapshot(ticker),
                "technicals": self.polygon.get_technicals(ticker),
                "details": self.polygon.get_ticker_details(ticker),
            }

        ta_results = await asyncio.gather(
            *[get_ta_for_ticker(t) for t in top_tickers],
            return_exceptions=True,
        )
        ta_data = {}
        for ticker, result in zip(top_tickers, ta_results):
            if not isinstance(result, Exception):
                ta_data[ticker] = result

        return {
            "movers": movers,
            "screener_gainers": screener_gainers,
            "technical_data": ta_data,
            "options_flow": unusual_options,
            "options_signals": options_signals,
        }

    async def get_top_fundamental_catalysts(self) -> dict:
        """
        Scan for the best fundamental catalysts — earnings beats,
        revenue growth, insider buying, analyst upgrades.
        """
        upcoming_earnings = self.finnhub.get_upcoming_earnings()
        market_news = self.polygon.get_news(limit=15)
        movers = self.polygon.get_market_movers()

        earnings_tickers = [
            e["ticker"] for e in upcoming_earnings[:8] if e.get("ticker")
        ]
        mover_tickers = [g["ticker"] for g in movers.get("gainers", [])[:5]]
        all_tickers = list(dict.fromkeys(earnings_tickers[:5] + mover_tickers))

        async def get_fundamentals(ticker):
            async_results = await asyncio.gather(
                self.stockanalysis.get_overview(ticker),
                self.stockanalysis.get_analyst_ratings(ticker),
                self.edgar.get_8k_filings(ticker),
                return_exceptions=True,
            )
            return {
                "overview": async_results[0] if not isinstance(async_results[0], Exception) else {},
                "analyst_ratings": async_results[1] if not isinstance(async_results[1], Exception) else {},
                "sec_filings": async_results[2] if not isinstance(async_results[2], Exception) else [],
                "earnings_history": self.finnhub.get_earnings_surprises(ticker),
                "insider_sentiment": self.finnhub.get_insider_sentiment(ticker),
                "recommendations": self.finnhub.get_recommendation_trends(ticker),
            }

        fund_results = await asyncio.gather(
            *[get_fundamentals(t) for t in all_tickers],
            return_exceptions=True,
        )
        fundamental_data = {}
        for ticker, result in zip(all_tickers, fund_results):
            if not isinstance(result, Exception):
                fundamental_data[ticker] = result

        return {
            "upcoming_earnings": upcoming_earnings,
            "market_news": market_news,
            "fundamental_data": fundamental_data,
        }

    async def get_social_buzz(self) -> dict:
        """
        Scan for the most hyped stocks on social media.
        StockTwits trending + Reddit/Twitter sentiment + volume surge correlation.
        """
        trending = await self.stocktwits.get_trending()
        trending_tickers = [
            t["ticker"] for t in trending[:10] if t.get("ticker")
        ]

        async def get_buzz(ticker):
            async_results = await asyncio.gather(
                self.stocktwits.get_sentiment(ticker),
                self.alphavantage.get_news_sentiment(ticker),
                return_exceptions=True,
            )
            return {
                "stocktwits": async_results[0] if not isinstance(async_results[0], Exception) else {},
                "social_sentiment": self.finnhub.get_social_sentiment(ticker),
                "snapshot": self.polygon.get_snapshot(ticker),
                "details": self.polygon.get_ticker_details(ticker),
                "news_sentiment": async_results[1] if not isinstance(async_results[1], Exception) else {},
            }

        buzz_results = await asyncio.gather(
            *[get_buzz(t) for t in trending_tickers[:8]],
            return_exceptions=True,
        )
        buzz_data = {}
        for ticker, result in zip(trending_tickers[:8], buzz_results):
            if not isinstance(result, Exception):
                buzz_data[ticker] = result

        return {
            "stocktwits_trending": trending,
            "buzz_details": buzz_data,
        }

    async def get_dashboard(self) -> dict:
        """
        Full dashboard: TA setups, fundamental catalysts, social buzz,
        plus macro context. All three columns fetched in parallel.
        """
        ta, fundamentals, social, fear_greed = await asyncio.gather(
            self.get_top_ta_setups(),
            self.get_top_fundamental_catalysts(),
            self.get_social_buzz(),
            self.fear_greed.get_fear_greed_index(),
            return_exceptions=True,
        )

        if isinstance(ta, Exception): ta = {}
        if isinstance(fundamentals, Exception): fundamentals = {}
        if isinstance(social, Exception): social = {}
        if isinstance(fear_greed, Exception): fear_greed = {}

        macro = self.fred.get_quick_macro()

        return {
            "ta_setups": ta,
            "fundamental_catalysts": fundamentals,
            "social_buzz": social,
            "macro_data": macro,
            "fear_greed_index": fear_greed,
        }