import asyncio

from data.polygon_provider import PolygonProvider
from data.finviz_scraper import FinvizScraper
from data.stocktwits_provider import StockTwitsProvider
from data.stockanalysis_scraper import StockAnalysisScraper
from data.options_scraper import OptionsScraper
from data.finnhub_provider import FinnhubProvider
from config import FINNHUB_API_KEY, ALPHA_VANTAGE_API_KEY, FRED_API_KEY, FMP_API_KEY
from data.alphavantage_provider import AlphaVantageProvider
from data.fred_provider import FredProvider
from data.edgar_provider import EdgarProvider
from data.fear_greed_provider import FearGreedProvider
from data.fmp_provider import FMPProvider

class MarketDataService:
    """
    Unified interface for all market data.
    Your agent talks to THIS — never directly to Polygon or scrapers.
    """

    def __init__(self, polygon_key: str, fmp_key: str = None):
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
        self.fmp = FMPProvider(fmp_key) if fmp_key else None

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
        Comprehensive macro dashboard combining FRED + FMP + Fear & Greed.
        FRED: Fed rate, CPI, Core PCE, GDP, unemployment, yield curve, VIX, jobless claims
        FMP: DXY, oil, gold, treasuries, sector performance, economic calendar, indices
        Fear & Greed: CNN sentiment index
        """
        fred_macro = self.fred.get_full_macro_dashboard()

        fmp_data = {}
        fear_greed = {}

        if self.fmp:
            fmp_result, fg_result = await asyncio.gather(
                self.fmp.get_macro_market_data(),
                self.fear_greed.get_fear_greed_index(),
                return_exceptions=True,
            )
            fmp_data = fmp_result if not isinstance(fmp_result, Exception) else {}
            fear_greed = fg_result if not isinstance(fg_result, Exception) else {}
        else:
            fear_greed_result = await self.fear_greed.get_fear_greed_index()
            fear_greed = fear_greed_result if not isinstance(fear_greed_result, Exception) else {}

        return {
            "fred_economic_data": fred_macro,
            "market_data": fmp_data,
            "fear_greed_index": fear_greed,
        }

    async def get_commodities_dashboard(self) -> dict:
        """
        Full commodities market dashboard:
        FMP commodity prices + related ETFs + DXY (inverse correlation) +
        FRED inflation data + macro context + sector performance.
        """
        fmp_commodities = {}
        fmp_dxy = {}
        fmp_econ = {}
        fmp_treasuries = {}
        if self.fmp:
            comm_result, dxy_result, econ_result, treasury_result = await asyncio.gather(
                self.fmp.get_full_commodity_dashboard(),
                self.fmp.get_dxy(),
                self.fmp.get_upcoming_economic_events(),
                self.fmp.get_treasury_rates(),
                return_exceptions=True,
            )
            fmp_commodities = comm_result if not isinstance(comm_result, Exception) else {}
            fmp_dxy = dxy_result if not isinstance(dxy_result, Exception) else {}
            fmp_econ = econ_result if not isinstance(econ_result, Exception) else {}
            fmp_treasuries = treasury_result if not isinstance(treasury_result, Exception) else {}

        fred_macro = self.fred.get_quick_macro()

        fear_greed = await self.fear_greed.get_fear_greed_index()

        commodity_news = self.polygon.get_news(limit=15)

        commodity_etfs = ["USO", "GLD", "SLV", "URA", "UNG", "COPX", "GDX", "XLE"]
        async def get_etf_ta(ticker):
            return {
                "technicals": self.polygon.get_technicals(ticker),
                "snapshot": self.polygon.get_snapshot(ticker),
            }

        etf_ta_results = await asyncio.gather(
            *[asyncio.to_thread(lambda t=t: get_etf_ta(t)) for t in commodity_etfs],
            return_exceptions=True,
        )
        etf_technicals = {}
        for ticker, result in zip(commodity_etfs, etf_ta_results):
            if not isinstance(result, Exception):
                etf_technicals[ticker] = result

        commodity_sentiment = {}
        commodity_tickers_social = ["USO", "GLD", "URA", "XLE"]
        social_results = await asyncio.gather(
            *[self.stocktwits.get_sentiment(t) for t in commodity_tickers_social],
            return_exceptions=True,
        )
        for ticker, result in zip(commodity_tickers_social, social_results):
            if not isinstance(result, Exception):
                commodity_sentiment[ticker] = result

        return {
            "commodity_prices": fmp_commodities,
            "dxy": fmp_dxy,
            "economic_calendar": fmp_econ,
            "treasury_yields": fmp_treasuries,
            "fred_macro": fred_macro,
            "fear_greed": fear_greed if not isinstance(fear_greed, Exception) else {},
            "commodity_news": commodity_news,
            "etf_technicals": etf_technicals,
            "commodity_sentiment": commodity_sentiment,
        }

    async def wide_scan_and_rank(self, category: str, filters: dict = None) -> dict:
        """
        WIDE FUNNEL approach:
        1. Cast wide net — pull 50-100+ candidates from multiple screeners
        2. Do lightweight enrichment on all of them
        3. Score them quantitatively
        4. Return top 12-15 fully enriched to Claude

        This ensures we never miss a good setup just because it wasn't
        in the top 5 of one screener.
        """
        from data.scoring_engine import rank_candidates

        # ── Stage 1: Cast Wide Net Based on Category ──
        # Different categories need different screeners to find SETUPS, not just movers

        if category in ["market_scan", "trades"]:
            screener_results = await asyncio.gather(
                self.finviz.get_stage2_breakouts(),
                self.finviz.get_macd_crossovers(),
                self.finviz.get_volume_breakouts(),
                self.finviz.get_sma_crossover_stocks(),
                self.finviz.get_consolidation_breakouts(),
                self.finviz.get_accumulation_stocks(),
                self.finviz.get_small_cap_momentum(),
                self.finviz.get_gap_up_volume(),
                self.finviz.get_unusual_volume(),
                self.finviz.get_new_highs(),
                self.finviz.get_insider_buying(),
                return_exceptions=True,
            )

        elif category in ["squeeze"]:
            screener_results = await asyncio.gather(
                self.finviz.get_high_short_float(),
                self.finviz.get_small_cap_squeeze_setups(),
                self.finviz.get_volume_breakouts(),
                self.finviz.get_unusual_volume(),
                self.finviz.get_small_cap_gainers(),
                self.finviz.get_screener_results("ta_topgainers"),
                return_exceptions=True,
            )

        elif category in ["investments"]:
            screener_results = await asyncio.gather(
                self.finviz.get_revenue_growth_leaders(),
                self.finviz.get_earnings_growth_leaders(),
                self.finviz.get_profitable_growth(),
                self.finviz.get_low_ps_high_growth(),
                self.finviz.get_ebitda_positive_turn(),
                self.finviz.get_low_debt_growth(),
                self.finviz.get_insider_buying(),
                self.finviz.get_institutional_accumulation(),
                self.finviz.get_analyst_upgrades(),
                self.finviz.get_stage2_breakouts(),
                return_exceptions=True,
            )

        elif category in ["fundamentals_scan"]:
            screener_results = await asyncio.gather(
                self.finviz.get_revenue_growth_leaders(),
                self.finviz.get_earnings_growth_leaders(),
                self.finviz.get_profitable_growth(),
                self.finviz.get_ebitda_positive_turn(),
                self.finviz.get_low_ps_high_growth(),
                self.finviz.get_low_debt_growth(),
                self.finviz.get_insider_buying(),
                self.finviz.get_analyst_upgrades(),
                self.finviz.get_earnings_this_week(),
                return_exceptions=True,
            )

        elif category in ["asymmetric"]:
            screener_results = await asyncio.gather(
                self.finviz.get_low_ps_high_growth(),
                self.finviz.get_rsi_recovery(),
                self.finviz.get_ebitda_positive_turn(),
                self.finviz.get_insider_buying(),
                self.finviz.get_volume_breakouts(),
                self.finviz.get_stage2_breakouts(),
                self.finviz.get_low_debt_growth(),
                self.finviz.get_accumulation_stocks(),
                return_exceptions=True,
            )

        elif category in ["bearish"]:
            screener_results = await asyncio.gather(
                self.finviz.get_top_losers(),
                self.finviz.get_overbought_stocks(),
                self.finviz.get_breaking_below_200sma(),
                self.finviz.get_declining_earnings(),
                self.finviz.get_high_short_declining(),
                self.finviz.get_most_volatile(),
                return_exceptions=True,
            )

        elif category in ["small_cap_spec"]:
            screener_results = await asyncio.gather(
                self.finviz.get_small_cap_momentum(),
                self.finviz.get_small_cap_gainers(),
                self.finviz.get_small_cap_squeeze_setups(),
                self.finviz.get_volume_breakouts(),
                self.finviz.get_penny_stock_gainers(),
                return_exceptions=True,
            )

        elif category in ["volume_spikes"]:
            screener_results = await asyncio.gather(
                self.finviz.get_volume_breakouts(),
                self.finviz.get_unusual_volume(),
                self.finviz.get_most_active(),
                self.finviz.get_gap_up_volume(),
                return_exceptions=True,
            )

        elif category in ["social_momentum"]:
            screener_results = await asyncio.gather(
                self.finviz.get_unusual_volume(),
                self.finviz.get_screener_results("ta_topgainers"),
                self.finviz.get_small_cap_gainers(),
                self.finviz.get_volume_breakouts(),
                return_exceptions=True,
            )

        else:
            screener_results = await asyncio.gather(
                self.finviz.get_screener_results("ta_topgainers"),
                self.finviz.get_unusual_volume(),
                self.finviz.get_new_highs(),
                self.finviz.get_most_active(),
                return_exceptions=True,
            )

        trending = []
        try:
            trending = await self.stocktwits.get_trending()
        except:
            pass

        movers = {}
        try:
            movers = self.polygon.get_market_movers()
        except:
            pass

        all_tickers = set()

        for result in screener_results:
            if isinstance(result, list):
                for item in result:
                    if isinstance(item, dict) and item.get("ticker"):
                        ticker = item["ticker"].upper().strip()
                        if len(ticker) <= 5 and ticker.isalpha():
                            all_tickers.add(ticker)

        for t in (trending or []):
            if isinstance(t, dict) and t.get("ticker"):
                all_tickers.add(t["ticker"].upper().strip())

        for g in (movers.get("gainers") or []):
            if g.get("ticker"):
                all_tickers.add(g["ticker"].upper().strip())
        if category == "bearish":
            for l in (movers.get("losers") or []):
                if l.get("ticker"):
                    all_tickers.add(l["ticker"].upper().strip())

        print(f"[Wide Scan] {category}: {len(all_tickers)} unique candidates found")

        needs_fundamentals = category in [
            "investments", "fundamentals_scan", "asymmetric",
            "squeeze",
        ]
        needs_social = True

        async def light_enrich(ticker):
            try:
                snapshot = self.polygon.get_snapshot(ticker)
                technicals = self.polygon.get_technicals(ticker)
                details = self.polygon.get_ticker_details(ticker)

                result = {
                    "snapshot": snapshot,
                    "technicals": technicals,
                    "details": details,
                }

                if needs_fundamentals:
                    try:
                        overview = await self.stockanalysis.get_overview(ticker)
                        result["overview"] = overview if not isinstance(overview, Exception) else {}
                    except:
                        result["overview"] = {}

                if needs_social:
                    try:
                        st = await self.stocktwits.get_sentiment(ticker)
                        result["sentiment"] = st if not isinstance(st, Exception) else {}
                    except:
                        result["sentiment"] = {}

                return result
            except Exception as e:
                return {"error": str(e)}

        max_candidates = 40 if needs_fundamentals else 60
        ticker_list = list(all_tickers)[:max_candidates]

        enrichment_results = await asyncio.gather(
            *[light_enrich(t) for t in ticker_list],
            return_exceptions=True,
        )

        candidates = {}
        for ticker, result in zip(ticker_list, enrichment_results):
            if not isinstance(result, Exception) and isinstance(result, dict) and "error" not in result:
                candidates[ticker] = result

        print(f"[Wide Scan] {len(candidates)} candidates enriched successfully (fundamentals={needs_fundamentals}, social={needs_social})")

        top_ranked = rank_candidates(candidates, category, top_n=15)

        print(f"[Wide Scan] Top 15 scores: {[(t, s) for t, s, _ in top_ranked[:15]]}")

        top_tickers = [(ticker, score) for ticker, score, _ in top_ranked[:12]]

        async def deep_enrich(ticker):
            """Full enrichment with all data sources."""
            try:
                st_result, overview, analyst, insider, earnings, recommendations = (
                    await asyncio.gather(
                        self.stocktwits.get_sentiment(ticker),
                        self.stockanalysis.get_overview(ticker),
                        self.stockanalysis.get_analyst_ratings(ticker),
                        asyncio.to_thread(lambda: self.finnhub.get_insider_sentiment(ticker)),
                        asyncio.to_thread(lambda: self.finnhub.get_earnings_surprises(ticker)),
                        asyncio.to_thread(lambda: self.finnhub.get_recommendation_trends(ticker)),
                        return_exceptions=True,
                    )
                )
                return {
                    "sentiment": st_result if not isinstance(st_result, Exception) else {},
                    "overview": overview if not isinstance(overview, Exception) else {},
                    "analyst_ratings": analyst if not isinstance(analyst, Exception) else {},
                    "insider_sentiment": insider if not isinstance(insider, Exception) else {},
                    "earnings_history": earnings if not isinstance(earnings, Exception) else [],
                    "recommendations": recommendations if not isinstance(recommendations, Exception) else [],
                }
            except Exception as e:
                return {"error": str(e)}

        deep_results = await asyncio.gather(
            *[deep_enrich(t) for t, _ in top_tickers],
            return_exceptions=True,
        )

        enriched_candidates = {}
        for (ticker, quant_score), deep_data in zip(top_tickers, deep_results):
            base_data = candidates.get(ticker, {})
            if not isinstance(deep_data, Exception) and isinstance(deep_data, dict):
                base_data.update(deep_data)
            base_data["quant_score"] = quant_score
            enriched_candidates[ticker] = base_data

        return {
            "total_candidates_scanned": len(all_tickers),
            "candidates_scored": len(candidates),
            "top_ranked": [
                {"ticker": t, "score": s} for t, s, _ in top_ranked[:15]
            ],
            "enriched_data": enriched_candidates,
            "movers": movers,
            "trending": trending[:10] if trending else [],
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
        """Scan for unusual volume stocks with enriched data."""
        unusual_vol = await self.finviz.get_unusual_volume()
        return {
            "unusual_volume_stocks": unusual_vol,
            "market_news": self.polygon.get_news(limit=10),
        }

    async def get_oversold(self) -> dict:
        """Scan for oversold bounce candidates."""
        oversold = await self.finviz.get_oversold_stocks()
        return {
            "oversold_stocks": oversold,
            "market_news": self.polygon.get_news(limit=10),
        }

    async def get_overbought(self) -> dict:
        """Scan for overbought stocks."""
        overbought = await self.finviz.get_overbought_stocks()
        return {
            "overbought_stocks": overbought,
        }

    async def get_squeeze_candidates(self) -> dict:
        """Scan for short squeeze setups."""
        high_short, unusual_vol, new_highs = await asyncio.gather(
            self.finviz.get_high_short_float(),
            self.finviz.get_unusual_volume(),
            self.finviz.get_new_highs(),
            return_exceptions=True,
        )
        if isinstance(high_short, Exception): high_short = []
        if isinstance(unusual_vol, Exception): unusual_vol = []
        if isinstance(new_highs, Exception): new_highs = []

        unusual_options = await self.options.get_unusual_options_activity()
        options_signals = self.options.interpret_flow(unusual_options) if unusual_options else {}

        trending = await self.stocktwits.get_trending()

        return {
            "high_short_float_stocks": high_short,
            "unusual_volume_stocks": unusual_vol,
            "new_highs": new_highs,
            "unusual_options": unusual_options,
            "options_signals": options_signals,
            "stocktwits_trending": trending,
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

        unusual_vol, new_highs, most_active = await asyncio.gather(
            self.finviz.get_unusual_volume(),
            self.finviz.get_new_highs(),
            self.finviz.get_most_active(),
            return_exceptions=True,
        )
        if isinstance(unusual_vol, Exception): unusual_vol = []
        if isinstance(new_highs, Exception): new_highs = []
        if isinstance(most_active, Exception): most_active = []

        return {
            "movers": movers,
            "screener_gainers": screener_gainers,
            "unusual_volume": unusual_vol,
            "new_52_week_highs": new_highs,
            "most_active": most_active,
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

        small_cap_gainers, insider_buying = await asyncio.gather(
            self.finviz.get_small_cap_gainers(),
            self.finviz.get_insider_buying(),
            return_exceptions=True,
        )
        if isinstance(small_cap_gainers, Exception): small_cap_gainers = []
        if isinstance(insider_buying, Exception): insider_buying = []

        return {
            "stocktwits_trending": trending,
            "buzz_details": buzz_data,
            "small_cap_gainers": small_cap_gainers,
            "insider_buying": insider_buying,
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

    async def get_social_momentum(self) -> dict:
        """
        Scan for stocks with accelerating social media mentions
        and positive sentiment in the last 24-48 hours.
        """
        trending = await self.stocktwits.get_trending()
        trending_tickers = [t["ticker"] for t in trending[:12] if t.get("ticker")]

        async def get_social_detail(ticker):
            st_result, finn_result, av_result = await asyncio.gather(
                self.stocktwits.get_sentiment(ticker),
                asyncio.to_thread(self.finnhub.get_social_sentiment, ticker),
                self.alphavantage.get_news_sentiment(ticker),
                return_exceptions=True,
            )
            return {
                "stocktwits": st_result if not isinstance(st_result, Exception) else {},
                "reddit_twitter": finn_result if not isinstance(finn_result, Exception) else {},
                "news_sentiment": av_result if not isinstance(av_result, Exception) else {},
                "snapshot": self.polygon.get_snapshot(ticker),
                "details": self.polygon.get_ticker_details(ticker),
                "technicals": self.polygon.get_technicals(ticker),
            }

        results = await asyncio.gather(
            *[get_social_detail(t) for t in trending_tickers],
            return_exceptions=True,
        )
        social_data = {}
        for ticker, result in zip(trending_tickers, results):
            if not isinstance(result, Exception):
                social_data[ticker] = result

        return {
            "stocktwits_trending": trending,
            "social_details": social_data,
        }

    async def get_volume_spikes(self) -> dict:
        """Scan for stocks with unusual volume vs 30-day average."""
        unusual_vol, most_active = await asyncio.gather(
            self.finviz.get_unusual_volume(),
            self.finviz.get_most_active(),
            return_exceptions=True,
        )
        if isinstance(unusual_vol, Exception): unusual_vol = []
        if isinstance(most_active, Exception): most_active = []

        vol_tickers = [s["ticker"] for s in (unusual_vol or [])[:8]]

        async def enrich_volume(ticker):
            return {
                "snapshot": self.polygon.get_snapshot(ticker),
                "technicals": self.polygon.get_technicals(ticker),
                "details": self.polygon.get_ticker_details(ticker),
            }

        enriched = await asyncio.gather(
            *[enrich_volume(t) for t in vol_tickers],
            return_exceptions=True,
        )
        enriched_data = {}
        for ticker, result in zip(vol_tickers, enriched):
            if not isinstance(result, Exception):
                enriched_data[ticker] = result

        return {
            "unusual_volume_stocks": unusual_vol,
            "most_active": most_active,
            "enriched_data": enriched_data,
        }

    async def get_earnings_catalyst_watch(self) -> dict:
        """
        Enhanced earnings watch: pulls all upcoming earnings,
        enriches with full data, and scores by volatility potential.
        """
        import asyncio

        upcoming_earnings = self.finnhub.get_upcoming_earnings()
        market_news = self.polygon.get_news(limit=15)

        earnings_tickers = [
            e["ticker"] for e in upcoming_earnings[:30]
            if e.get("ticker") and len(e["ticker"]) <= 5
        ]

        async def light_enrich_earnings(ticker):
            try:
                snapshot = self.polygon.get_snapshot(ticker)
                technicals = self.polygon.get_technicals(ticker)
                details = self.polygon.get_ticker_details(ticker)
                earnings_hist = self.finnhub.get_earnings_surprises(ticker)
                recommendations = self.finnhub.get_recommendation_trends(ticker)
                return {
                    "snapshot": snapshot,
                    "technicals": technicals,
                    "details": details,
                    "earnings_history": earnings_hist,
                    "recommendations": recommendations,
                }
            except Exception as e:
                return {"error": str(e)}

        results = await asyncio.gather(
            *[asyncio.to_thread(lambda t=t: light_enrich_earnings(t)) for t in earnings_tickers],
            return_exceptions=True,
        )

        scored = []
        for ticker, result in zip(earnings_tickers, results):
            if isinstance(result, Exception) or not isinstance(result, dict) or "error" in result:
                continue
            score = 0.0

            snapshot = result.get("snapshot", {})
            details = result.get("details", {})
            volume = snapshot.get("volume")
            avg_vol = details.get("avg_volume") if isinstance(details, dict) else None
            if volume and avg_vol:
                try:
                    ratio = float(volume) / float(avg_vol)
                    if ratio >= 2.0: score += 20
                    elif ratio >= 1.5: score += 12
                    elif ratio >= 1.0: score += 5
                except:
                    pass

            earnings_hist = result.get("earnings_history", [])
            if isinstance(earnings_hist, list):
                beats = sum(1 for e in earnings_hist[:4] if isinstance(e, dict) and e.get("surprise_pct") and e["surprise_pct"] > 0)
                score += beats * 10

            mc = details.get("market_cap") if isinstance(details, dict) else None
            if mc:
                try:
                    mc = float(mc)
                    if mc < 2e9: score += 20
                    elif mc < 10e9: score += 12
                    elif mc < 50e9: score += 5
                except:
                    pass

            technicals = result.get("technicals", {})
            rsi = technicals.get("rsi")
            if rsi:
                try:
                    if 45 <= float(rsi) <= 65: score += 10
                except:
                    pass

            result["quant_score"] = round(score, 1)
            scored.append((ticker, score, result))

        scored.sort(key=lambda x: x[1], reverse=True)

        top_tickers = [(t, s) for t, s, _ in scored[:12]]

        async def deep_enrich_earnings(ticker):
            try:
                st, overview, filings = await asyncio.gather(
                    self.stocktwits.get_sentiment(ticker),
                    self.stockanalysis.get_overview(ticker),
                    self.edgar.get_8k_filings(ticker),
                    return_exceptions=True,
                )
                return {
                    "sentiment": st if not isinstance(st, Exception) else {},
                    "overview": overview if not isinstance(overview, Exception) else {},
                    "recent_8k_filings": filings if not isinstance(filings, Exception) else [],
                }
            except:
                return {}

        deep_results = await asyncio.gather(
            *[deep_enrich_earnings(t) for t, _ in top_tickers],
            return_exceptions=True,
        )

        enriched = {}
        for (ticker, quant_score), deep, (_, _, base) in zip(top_tickers, deep_results, scored[:12]):
            if not isinstance(deep, Exception) and isinstance(deep, dict):
                base.update(deep)
            base["quant_score"] = quant_score
            enriched[ticker] = base

        earnings_dates = {}
        for e in upcoming_earnings:
            t = e.get("ticker")
            if t in enriched:
                earnings_dates[t] = e

        return {
            "total_earnings_scanned": len(earnings_tickers),
            "ranked_by_volatility": [{"ticker": t, "score": s} for t, s, _ in scored[:15]],
            "enriched_data": enriched,
            "earnings_dates": earnings_dates,
            "market_news": market_news,
        }

    async def get_sector_rotation(self) -> dict:
        """
        Enhanced sector rotation using FMP sector ETF data + FRED macro.
        """
        fmp_sectors = {}
        if self.fmp:
            fmp_sectors = await self.fmp.get_sector_etf_snapshot()

        key_etfs = ["XLK", "XLV", "XLF", "XLE", "XLI", "XLP", "XLY", "XLB", "XLU", "SPY", "QQQ", "IWM", "SMH", "URA"]

        async def get_etf_technicals(ticker):
            return {
                "technicals": self.polygon.get_technicals(ticker),
                "snapshot": self.polygon.get_snapshot(ticker),
            }

        tech_results = await asyncio.gather(
            *[asyncio.to_thread(lambda t=t: get_etf_technicals(t)) for t in key_etfs],
            return_exceptions=True,
        )
        etf_technicals = {}
        for ticker, result in zip(key_etfs, tech_results):
            if not isinstance(result, Exception):
                etf_technicals[ticker] = result

        macro = self.fred.get_quick_macro()
        fear_greed = await self.fear_greed.get_fear_greed_index()

        dxy = {}
        commodities = {}
        if self.fmp:
            dxy_result, comm_result = await asyncio.gather(
                self.fmp.get_dxy(),
                self.fmp.get_key_commodities(),
                return_exceptions=True,
            )
            dxy = dxy_result if not isinstance(dxy_result, Exception) else {}
            commodities = comm_result if not isinstance(comm_result, Exception) else {}

        return {
            "fmp_sector_data": fmp_sectors,
            "etf_technicals": etf_technicals,
            "macro_data": macro,
            "fear_greed": fear_greed if not isinstance(fear_greed, Exception) else {},
            "dxy": dxy,
            "commodities": commodities,
        }

    async def get_asymmetric_setups(self) -> dict:
        """
        Scan for asymmetric setups: compressed valuation + catalyst + volume.
        Focus on stocks where downside is capped and upside is uncapped.
        """
        gainers, unusual_vol, new_highs, insider_buys = await asyncio.gather(
            self.finviz.get_screener_results("ta_topgainers"),
            self.finviz.get_unusual_volume(),
            self.finviz.get_new_highs(),
            self.finviz.get_insider_buying(),
            return_exceptions=True,
        )
        if isinstance(gainers, Exception): gainers = []
        if isinstance(unusual_vol, Exception): unusual_vol = []
        if isinstance(new_highs, Exception): new_highs = []
        if isinstance(insider_buys, Exception): insider_buys = []

        all_tickers = list(dict.fromkeys(
            [s["ticker"] for s in (gainers or [])[:5]] +
            [s["ticker"] for s in (unusual_vol or [])[:5]] +
            [s["ticker"] for s in (new_highs or [])[:5]] +
            [s["ticker"] for s in (insider_buys or [])[:5]]
        ))[:12]

        async def get_asymmetric_detail(ticker):
            overview, analyst = await asyncio.gather(
                self.stockanalysis.get_overview(ticker),
                self.stockanalysis.get_analyst_ratings(ticker),
                return_exceptions=True,
            )
            return {
                "overview": overview if not isinstance(overview, Exception) else {},
                "analyst_ratings": analyst if not isinstance(analyst, Exception) else {},
                "snapshot": self.polygon.get_snapshot(ticker),
                "technicals": self.polygon.get_technicals(ticker),
                "details": self.polygon.get_ticker_details(ticker),
                "insider_sentiment": self.finnhub.get_insider_sentiment(ticker),
                "earnings_history": self.finnhub.get_earnings_surprises(ticker),
            }

        detail_results = await asyncio.gather(
            *[get_asymmetric_detail(t) for t in all_tickers],
            return_exceptions=True,
        )
        detail_data = {}
        for ticker, result in zip(all_tickers, detail_results):
            if not isinstance(result, Exception):
                detail_data[ticker] = result

        return {
            "screener_gainers": gainers,
            "unusual_volume": unusual_vol,
            "new_highs": new_highs,
            "insider_buying": insider_buys,
            "detail_data": detail_data,
        }

    async def get_bearish_setups(self) -> dict:
        """Scan for breakdown / bearish plays — weakest stocks and sectors."""
        losers, overbought = await asyncio.gather(
            self.finviz.get_top_losers(),
            self.finviz.get_overbought_stocks(),
            return_exceptions=True,
        )
        if isinstance(losers, Exception): losers = []
        if isinstance(overbought, Exception): overbought = []

        movers = self.polygon.get_market_movers()

        loser_tickers = [s["ticker"] for s in (losers or [])[:6]]

        async def enrich_loser(ticker):
            return {
                "snapshot": self.polygon.get_snapshot(ticker),
                "technicals": self.polygon.get_technicals(ticker),
                "details": self.polygon.get_ticker_details(ticker),
            }

        enriched = await asyncio.gather(
            *[enrich_loser(t) for t in loser_tickers],
            return_exceptions=True,
        )
        enriched_data = {}
        for ticker, result in zip(loser_tickers, enriched):
            if not isinstance(result, Exception):
                enriched_data[ticker] = result

        return {
            "top_losers": losers,
            "overbought_stocks": overbought,
            "market_losers": movers.get("losers", []),
            "enriched_data": enriched_data,
        }

    async def get_thematic_scan(self, theme: str = "ai_compute") -> dict:
        """
        Enhanced thematic scanner with full data per ticker:
        Polygon snapshot + technicals + StockTwits sentiment + StockAnalysis overview.
        Ranks by relative strength within the theme.
        """
        import asyncio

        THEMES = {
            "ai_compute": {
                "name": "AI & Compute Infrastructure",
                "tickers": ["NVDA", "AMD", "AVGO", "MRVL", "CRDO", "SMCI", "VRT", "ANET", "DELL", "ORCL", "MSFT", "GOOGL", "AMZN", "META", "TSM"],
            },
            "energy": {
                "name": "Energy & Oil/Gas",
                "tickers": ["XOM", "CVX", "COP", "EOG", "DVN", "FANG", "OXY", "SLB", "HAL", "AR", "EQT", "RRC"],
            },
            "uranium": {
                "name": "Uranium & Nuclear",
                "tickers": ["CCJ", "UEC", "UUUU", "DNN", "NXE", "LEU", "SMR", "OKLO", "VST", "CEG", "TLN"],
            },
            "metals": {
                "name": "Metals & Mining",
                "tickers": ["FCX", "NEM", "GOLD", "AEM", "WPM", "RGLD", "SCCO", "VALE", "RIO", "BHP", "TECK", "MP"],
            },
            "defense": {
                "name": "Defense & Aerospace",
                "tickers": ["LMT", "RTX", "NOC", "GD", "BA", "LHX", "LDOS", "KTOS", "PLTR", "RKLB"],
            },
        }

        theme_data = THEMES.get(theme, THEMES["ai_compute"])
        tickers = theme_data["tickers"]

        async def full_enrich(ticker):
            try:
                snapshot = self.polygon.get_snapshot(ticker)
                technicals = self.polygon.get_technicals(ticker)
                details = self.polygon.get_ticker_details(ticker)

                st_result, overview = await asyncio.gather(
                    self.stocktwits.get_sentiment(ticker),
                    self.stockanalysis.get_overview(ticker),
                    return_exceptions=True,
                )

                return {
                    "snapshot": snapshot,
                    "technicals": technicals,
                    "details": details,
                    "sentiment": st_result if not isinstance(st_result, Exception) else {},
                    "overview": overview if not isinstance(overview, Exception) else {},
                }
            except Exception as e:
                return {"error": str(e)}

        results = await asyncio.gather(
            *[full_enrich(t) for t in tickers],
            return_exceptions=True,
        )

        from data.scoring_engine import score_for_trades
        theme_results = []
        for ticker, result in zip(tickers, results):
            if isinstance(result, Exception) or not isinstance(result, dict) or "error" in result:
                continue
            quant_score = score_for_trades(result)
            result["quant_score"] = quant_score
            theme_results.append((ticker, quant_score, result))

        theme_results.sort(key=lambda x: x[1], reverse=True)

        enriched = {}
        for ticker, score, data in theme_results:
            enriched[ticker] = data

        sector_etf_map = {
            "ai_compute": "SMH",
            "energy": "XLE",
            "uranium": "URA",
            "metals": "GDX",
            "defense": "ITA",
        }
        sector_etf = sector_etf_map.get(theme)
        etf_data = {}
        if sector_etf:
            try:
                etf_data = {
                    "snapshot": self.polygon.get_snapshot(sector_etf),
                    "technicals": self.polygon.get_technicals(sector_etf),
                }
            except:
                pass

        spy_data = {}
        try:
            spy_data = {"snapshot": self.polygon.get_snapshot("SPY")}
        except:
            pass

        return {
            "theme_name": theme_data["name"],
            "ranked_tickers": [
                {"ticker": t, "score": s} for t, s, _ in theme_results
            ],
            "enriched_data": enriched,
            "sector_etf": {sector_etf: etf_data} if sector_etf else {},
            "spy_benchmark": spy_data,
            "market_news": self.polygon.get_news(limit=10),
        }

    async def get_small_cap_spec(self) -> dict:
        """
        Speculative small cap scanner: high volatility, increasing volume,
        positive sentiment, market cap < $2B.
        """
        small_gainers, unusual_vol, high_short = await asyncio.gather(
            self.finviz.get_small_cap_gainers(),
            self.finviz.get_unusual_volume(),
            self.finviz.get_high_short_float(),
            return_exceptions=True,
        )
        if isinstance(small_gainers, Exception): small_gainers = []
        if isinstance(unusual_vol, Exception): unusual_vol = []
        if isinstance(high_short, Exception): high_short = []

        trending = await self.stocktwits.get_trending()

        all_tickers = list(dict.fromkeys(
            [s["ticker"] for s in (small_gainers or [])[:6]] +
            [s["ticker"] for s in (high_short or [])[:4]]
        ))[:10]

        async def enrich_small(ticker):
            st, overview = await asyncio.gather(
                self.stocktwits.get_sentiment(ticker),
                self.stockanalysis.get_overview(ticker),
                return_exceptions=True,
            )
            return {
                "sentiment": st if not isinstance(st, Exception) else {},
                "overview": overview if not isinstance(overview, Exception) else {},
                "snapshot": self.polygon.get_snapshot(ticker),
                "technicals": self.polygon.get_technicals(ticker),
                "details": self.polygon.get_ticker_details(ticker),
            }

        enriched = await asyncio.gather(
            *[enrich_small(t) for t in all_tickers],
            return_exceptions=True,
        )
        enriched_data = {}
        for ticker, result in zip(all_tickers, enriched):
            if not isinstance(result, Exception):
                enriched_data[ticker] = result

        return {
            "small_cap_gainers": small_gainers,
            "unusual_volume": unusual_vol,
            "high_short_float": high_short,
            "trending": trending,
            "enriched_data": enriched_data,
        }