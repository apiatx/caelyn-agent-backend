import asyncio

from data.polygon_provider import PolygonProvider
from data.finviz_scraper import FinvizScraper, scrape_yahoo_trending, scrape_stockanalysis_trending
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
from data.coingecko_provider import CoinGeckoProvider
from data.reddit_provider import RedditSentimentProvider
from data.altfins_provider import AltFINSProvider
from data.xai_sentiment_provider import XAISentimentProvider


def _parse_num(val):
    if val is None:
        return None
    try:
        return float(str(val).replace(",", "").replace("%", "").strip())
    except (ValueError, TypeError):
        return None

def _parse_pct(val):
    if val is None:
        return None
    try:
        s = str(val).replace("%", "").replace(",", "").strip()
        return float(s)
    except (ValueError, TypeError):
        return None

def _parse_vol(val):
    if val is None:
        return None
    try:
        s = str(val).replace(",", "").strip()
        if s.endswith("M"):
            return float(s[:-1]) * 1_000_000
        elif s.endswith("K"):
            return float(s[:-1]) * 1_000
        elif s.endswith("B"):
            return float(s[:-1]) * 1_000_000_000
        return float(s)
    except (ValueError, TypeError):
        return None


class MarketDataService:
    """
    Unified interface for all market data.
    Your agent talks to THIS — never directly to Polygon or scrapers.
    """

    def __init__(self, polygon_key: str, fmp_key: str = None, coingecko_key: str = None, cmc_key: str = None, altfins_key: str = None, xai_key: str = None):
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
        self.coingecko = CoinGeckoProvider(coingecko_key) if coingecko_key else None
        from data.cmc_provider import CMCProvider
        self.cmc = CMCProvider(cmc_key) if cmc_key else None
        self.reddit = RedditSentimentProvider()
        from data.hyperliquid_provider import HyperliquidProvider
        self.hyperliquid = HyperliquidProvider()
        self.altfins = AltFINSProvider(altfins_key) if altfins_key else None
        self.xai = XAISentimentProvider(xai_key) if xai_key else None
        if self.xai:
            print("[INIT] xAI Grok X sentiment provider initialized")
        else:
            print("[INIT] xAI Grok X sentiment provider SKIPPED (no XAI_API_KEY)")

    NEGATIVE_NEWS_KEYWORDS = [
        "fraud", "lawsuit", "sued", "investigation", "sec probe",
        "recall", "scandal", "exposed", "fake", "misleading",
        "class action", "downgrade", "bankruptcy", "default",
        "fda reject", "failed trial", "data breach", "ceo resign",
        "accounting", "restatement", "delisted", "indictment",
        "ponzi", "embezzlement", "whistleblower", "sec charges",
        "criminal", "subpoena", "material weakness",
    ]

    async def get_market_news_context(self, tickers: list = None) -> dict:
        """
        Pull recent news, sentiment, Reddit, and economic calendar data
        BEFORE individual ticker analysis.
        Called at the start of every scan pipeline.
        """
        news_data = {}

        tasks = []
        task_keys = []

        tasks.append(asyncio.wait_for(
            self.alphavantage.get_news_sentiment(topics="financial_markets"),
            timeout=8.0
        ))
        task_keys.append("market_news")

        if self.fmp:
            tasks.append(asyncio.wait_for(self.fmp.get_market_news(limit=10), timeout=8.0))
            task_keys.append("fmp_news")
            tasks.append(asyncio.wait_for(self.fmp.get_economic_calendar(days_ahead=3), timeout=6.0))
            task_keys.append("economic_calendar")

        tasks.append(asyncio.wait_for(self.stocktwits.get_trending(), timeout=6.0))
        task_keys.append("stocktwits_trending")

        tasks.append(asyncio.wait_for(self.fear_greed.get_fear_greed_index(), timeout=5.0))
        task_keys.append("fear_greed")

        tasks.append(asyncio.wait_for(self.reddit.get_full_reddit_dashboard(), timeout=8.0))
        task_keys.append("reddit")

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for key, result in zip(task_keys, results):
            if isinstance(result, Exception):
                news_data[key] = {} if key == "reddit" else []
            else:
                news_data[key] = result

        av_news = news_data.get("market_news", {})
        if isinstance(av_news, dict) and av_news.get("articles"):
            news_data["market_news"] = av_news["articles"]
        elif not isinstance(av_news, list):
            fmp_news = news_data.pop("fmp_news", [])
            news_data["market_news"] = fmp_news if isinstance(fmp_news, list) else []

        reddit_data = news_data.get("reddit", {})
        if isinstance(reddit_data, dict):
            wsb = reddit_data.get("wsb_trending", [])
            all_reddit = reddit_data.get("all_stocks_trending", [])
            news_data["reddit_wsb_trending"] = wsb[:15] if wsb else []
            news_data["reddit_all_trending"] = all_reddit[:15] if all_reddit else []

        return news_data

    async def enrich_with_sentiment_filter(self, ticker_list: list, screener_results: list = None) -> list:
        """
        Enrich tickers with sentiment data and FLAG/WARN on negative catalysts.
        Uses StockTwits sentiment in PARALLEL for speed.
        Returns list of dicts with sentiment_flag and warning fields.
        """
        async def check_one(ticker):
            ticker_data = {"ticker": ticker}
            if screener_results:
                for item in screener_results:
                    if isinstance(item, dict) and item.get("ticker", "").upper() == ticker.upper():
                        ticker_data.update(item)
                        break
            try:
                sentiment = await asyncio.wait_for(
                    self.stocktwits.get_sentiment(ticker), timeout=5.0
                )
                if isinstance(sentiment, Exception):
                    sentiment = {}
            except Exception:
                sentiment = {}

            if sentiment:
                ticker_data["social_sentiment"] = sentiment
                bearish_pct = sentiment.get("bearish_pct", 0) or 0
                if bearish_pct > 70:
                    ticker_data["sentiment_flag"] = "EXTREME_BEARISH"
                    ticker_data["sentiment_warning"] = f"{bearish_pct}% bearish on StockTwits"
                elif bearish_pct > 50:
                    ticker_data["sentiment_flag"] = "BEARISH"
                    ticker_data["sentiment_warning"] = f"{bearish_pct}% bearish — sentiment headwind"
                else:
                    ticker_data["sentiment_flag"] = "OK"
            else:
                ticker_data["sentiment_flag"] = "NO_DATA"

            ticker_data["news_flag"] = "NO_DATA"
            return ticker_data

        results = await asyncio.gather(
            *[check_one(t) for t in ticker_list],
            return_exceptions=True,
        )

        enriched = []
        for r in results:
            if isinstance(r, Exception):
                enriched.append({"ticker": "?", "sentiment_flag": "NO_DATA", "news_flag": "NO_DATA"})
            else:
                enriched.append(r)

        return enriched

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

        async_tasks = [
            self.stocktwits.get_sentiment(ticker),
            self.stockanalysis.get_overview(ticker),
            self.stockanalysis.get_financials(ticker),
            self.stockanalysis.get_analyst_ratings(ticker),
            self.options.get_put_call_ratio(ticker),
            self.alphavantage.get_news_sentiment(ticker),
            self.edgar.get_company_summary(ticker),
        ]
        async_keys = [
            "sentiment", "fundamentals", "financials", "analyst_ratings",
            "options_put_call", "news_sentiment_ai", "sec_filings",
        ]

        async_results = await asyncio.gather(*async_tasks, return_exceptions=True)

        for key, result in zip(async_keys, async_results):
            if isinstance(result, Exception):
                sync_data[key] = {"error": str(result)}
            else:
                sync_data[key] = result

        av_news = sync_data.get("news_sentiment_ai", {})
        news_articles = []
        if isinstance(av_news, dict) and av_news.get("articles"):
            news_articles = av_news["articles"]
        elif isinstance(av_news, list):
            news_articles = av_news

        if news_articles:
            news_text = " ".join(
                (n.get("title", "") + " " + n.get("summary", "") + " " + n.get("text", "")).lower()
                for n in news_articles
            )
            found_negatives = [kw for kw in self.NEGATIVE_NEWS_KEYWORDS if kw in news_text]
            if found_negatives:
                sync_data["news_flag"] = "NEGATIVE_CATALYST"
                sync_data["news_warning"] = f"Negative news detected: {', '.join(found_negatives)}"
            else:
                sync_data["news_flag"] = "OK"

        sentiment = sync_data.get("sentiment", {})
        if isinstance(sentiment, dict):
            bearish_pct = sentiment.get("bearish_pct", 0) or 0
            if bearish_pct > 70:
                sync_data["sentiment_flag"] = "EXTREME_BEARISH"
                sync_data["sentiment_warning"] = f"{bearish_pct}% bearish on StockTwits"
            elif bearish_pct > 50:
                sync_data["sentiment_flag"] = "BEARISH"
            else:
                sync_data["sentiment_flag"] = "OK"

        if self.xai:
            try:
                x_sentiment = await asyncio.wait_for(
                    self.xai.get_ticker_sentiment(ticker),
                    timeout=20.0,
                )
                if x_sentiment and "error" not in x_sentiment:
                    sync_data["x_sentiment"] = x_sentiment
            except Exception as e:
                print(f"[RESEARCH] {ticker} xAI sentiment failed: {e}")

        return sync_data

    async def scan_market(self) -> dict:
        """Broad market overview — parallelized for speed."""
        movers = {}
        if self.fmp:
            try:
                movers = await self.fmp.get_gainers_losers()
            except:
                pass

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
            "market_news": [],
            "screener_gainers": screener_gainers,
            "catalyst_data": {},
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
            "commodity_news": [],
            "etf_technicals": {},
            "commodity_sentiment": commodity_sentiment,
        }

    CATEGORY_FILTERS = {
        "market_scan": {
            "filters": "sh_avgvol_o300,sh_price_o5,ta_sma200_pa",
            "limit": 50,
            "enrich_top": 15,
        },
        "trades": {
            "filters": "sh_avgvol_o300,ta_sma200_pa,ta_rsi_nos,sh_price_o5",
            "limit": 40,
            "enrich_top": 12,
        },
        "investments": {
            "filters": "sh_avgvol_o300,fa_salesqoq_o10,fa_epsqoq_o10,ta_sma200_pa,sh_price_o10",
            "limit": 40,
            "enrich_top": 12,
        },
        "fundamentals_scan": {
            "filters": "sh_avgvol_o300,fa_salesqoq_o20,fa_opermargin_pos,sh_price_o5",
            "limit": 40,
            "enrich_top": 12,
        },
        "squeeze": {
            "filters": "sh_avgvol_o200,sh_short_o15,ta_sma20_pa,sh_price_o2",
            "limit": 30,
            "enrich_top": 10,
        },
        "asymmetric": {
            "filters": "sh_avgvol_o200,ta_rsi_ob30,ta_sma200_pa,sh_price_o5",
            "limit": 30,
            "enrich_top": 10,
        },
        "social_momentum": {
            "filters": "sh_avgvol_o500,sh_relvol_o1.5,sh_price_o3",
            "limit": 30,
            "enrich_top": 10,
        },
        "volume_spikes": {
            "filters": "sh_avgvol_o200,sh_relvol_o2,sh_price_o2",
            "limit": 30,
            "enrich_top": 10,
        },
        "bearish": {
            "filters": "sh_avgvol_o300,ta_sma200_pb,ta_sma50_pb,sh_price_o5",
            "limit": 30,
            "enrich_top": 10,
        },
        "small_cap_spec": {
            "filters": "sh_avgvol_o200,cap_microover,cap_smallunder,ta_sma50_pa,sh_price_o1",
            "limit": 40,
            "enrich_top": 10,
        },
    }

    async def wide_scan_and_rank(self, category: str, filters: dict = None) -> dict:
        """
        News-first scan pipeline:
        1. Finviz screen + market news context IN PARALLEL
        2. Light enrichment for scoring
        3. Score quantitatively → top candidates
        4. Sentiment + news filter on top candidates (flags/warns)
        5. Deep enrich survivors with fundamentals
        """
        import time
        scan_start = time.time()
        from data.scoring_engine import rank_candidates

        cat_config = self.CATEGORY_FILTERS.get(category, self.CATEGORY_FILTERS["market_scan"])
        finviz_filters = cat_config["filters"]
        limit = cat_config["limit"]
        enrich_top = cat_config["enrich_top"]

        print(f"[Wide Scan] {category}: filters={finviz_filters}, limit={limit}, enrich_top={enrich_top}")

        screener_task = self.finviz._custom_screen(
            f"v=111&f={finviz_filters}&ft=4&o=-change"
        )
        news_task = self.get_market_news_context()

        screener_results, news_context = await asyncio.gather(
            screener_task, news_task, return_exceptions=True
        )

        if isinstance(screener_results, Exception):
            print(f"[Wide Scan] Screener failed: {screener_results}")
            screener_results = []
        if isinstance(news_context, Exception):
            news_context = {}

        print(f"[Wide Scan] News context: {len(news_context.get('market_news', []))} headlines, trending: {len(news_context.get('stocktwits_trending', []))} ({time.time()-scan_start:.1f}s)")

        all_tickers = set()

        if isinstance(screener_results, list):
            for item in screener_results:
                if isinstance(item, dict) and item.get("ticker"):
                    ticker = item["ticker"].upper().strip()
                    if ".X" not in ticker and ".U" not in ticker and len(ticker) <= 5 and ticker.isalpha():
                        all_tickers.add(ticker)

        trending = news_context.get("stocktwits_trending", [])
        for t in (trending or []):
            if isinstance(t, dict) and t.get("ticker"):
                all_tickers.add(t["ticker"].upper().strip())

        print(f"[Wide Scan] {category}: {len(all_tickers)} unique candidates found ({time.time()-scan_start:.1f}s)")

        needs_fundamentals = category in [
            "investments", "fundamentals_scan", "asymmetric", "squeeze",
        ]

        async def light_enrich(ticker):
            try:
                finviz_data = {}
                for item in (screener_results if isinstance(screener_results, list) else []):
                    if isinstance(item, dict) and item.get("ticker", "").upper() == ticker.upper():
                        finviz_data = item
                        break

                result = {
                    "snapshot": {
                        "price": _parse_num(finviz_data.get("price")),
                        "change_pct": _parse_pct(finviz_data.get("change")),
                        "volume": _parse_vol(finviz_data.get("volume")),
                    },
                    "technicals": {
                        "rsi": _parse_num(finviz_data.get("rsi")),
                        "sma_20": _parse_num(finviz_data.get("sma20")),
                        "sma_50": _parse_num(finviz_data.get("sma50")),
                        "sma_200": _parse_num(finviz_data.get("sma200")),
                    },
                    "details": {
                        "market_cap": finviz_data.get("market_cap"),
                    },
                }
                async_tasks = []
                if needs_fundamentals:
                    async_tasks.append(("overview", self.stockanalysis.get_overview(ticker)))

                if async_tasks:
                    async_results = await asyncio.gather(
                        *[t[1] for t in async_tasks],
                        return_exceptions=True,
                    )
                    for (key, _), res in zip(async_tasks, async_results):
                        result[key] = res if not isinstance(res, Exception) else {}

                return result
            except Exception as e:
                return {"error": str(e)}

        ticker_list = list(all_tickers)[:limit]

        enrichment_results = []
        for i, ticker in enumerate(ticker_list):
            try:
                result = await asyncio.wait_for(light_enrich(ticker), timeout=6.0)
            except asyncio.TimeoutError:
                print(f"[Wide Scan] {ticker} light enrich timed out, skipping")
                result = {"error": "timeout"}
            enrichment_results.append(result)
            if (i + 1) % 10 == 0 or i == len(ticker_list) - 1:
                print(f"[Wide Scan] Light enriched {i + 1}/{len(ticker_list)} tickers ({time.time()-scan_start:.1f}s)")

        candidates = {}
        for ticker, result in zip(ticker_list, enrichment_results):
            if isinstance(result, dict) and "error" not in result:
                candidates[ticker] = result

        print(f"[Wide Scan] {len(candidates)} candidates enriched successfully ({time.time()-scan_start:.1f}s)")

        top_ranked = rank_candidates(candidates, category, top_n=enrich_top + 3)

        print(f"[Wide Scan] Top scores: {[(t, s) for t, s, _ in top_ranked[:enrich_top]]}")

        top_ticker_names = [ticker for ticker, score, _ in top_ranked[:min(enrich_top, 10)]]
        top_scores = {ticker: score for ticker, score, _ in top_ranked[:min(enrich_top, 10)]}

        print(f"[Wide Scan] Running sentiment + news filter on {len(top_ticker_names)} candidates ({time.time()-scan_start:.1f}s)")
        sentiment_filtered = await self.enrich_with_sentiment_filter(
            top_ticker_names, screener_results
        )
        print(f"[Wide Scan] Sentiment filter complete ({time.time()-scan_start:.1f}s)")

        flagged = []
        clean = []
        for td in sentiment_filtered:
            t = td.get("ticker", "")
            if td.get("sentiment_flag") == "EXTREME_BEARISH" or td.get("news_flag") == "NEGATIVE_CATALYST":
                flagged.append(td)
                print(f"[Wide Scan] FLAGGED {t}: sentiment={td.get('sentiment_flag')} news={td.get('news_flag')} warn={td.get('sentiment_warning', td.get('news_warning', ''))}")
            else:
                clean.append(td)

        deep_tickers = [td["ticker"] for td in clean[:8]]

        async def deep_enrich(ticker):
            try:
                overview, analyst, insider, earnings, recommendations = (
                    await asyncio.gather(
                        self.stockanalysis.get_overview(ticker),
                        self.stockanalysis.get_analyst_ratings(ticker),
                        asyncio.to_thread(lambda: self.finnhub.get_insider_sentiment(ticker)),
                        asyncio.to_thread(lambda: self.finnhub.get_earnings_surprises(ticker)),
                        asyncio.to_thread(lambda: self.finnhub.get_recommendation_trends(ticker)),
                        return_exceptions=True,
                    )
                )
                return {
                    "overview": overview if not isinstance(overview, Exception) else {},
                    "analyst_ratings": analyst if not isinstance(analyst, Exception) else {},
                    "insider_sentiment": insider if not isinstance(insider, Exception) else {},
                    "earnings_history": earnings if not isinstance(earnings, Exception) else [],
                    "recommendations": recommendations if not isinstance(recommendations, Exception) else [],
                }
            except Exception as e:
                return {"error": str(e)}

        deep_results = []
        for i, ticker in enumerate(deep_tickers):
            try:
                result = await asyncio.wait_for(deep_enrich(ticker), timeout=8.0)
            except Exception as e:
                result = {"error": str(e)}
            deep_results.append(result)
            if i < len(deep_tickers) - 1:
                await asyncio.sleep(0.8)

        if self.xai and deep_tickers:
            try:
                x_batch = await asyncio.wait_for(
                    self.xai.get_batch_sentiment(deep_tickers[:10]),
                    timeout=30.0,
                )
                print(f"[SCAN] xAI enriched {len(x_batch)} tickers with X sentiment")
            except Exception as e:
                print(f"[SCAN] xAI batch sentiment failed: {e}")
                x_batch = {}
        else:
            x_batch = {}

        enriched_candidates = {}
        sentiment_map = {td["ticker"]: td for td in sentiment_filtered}

        for ticker, deep_data in zip(deep_tickers, deep_results):
            base_data = candidates.get(ticker, {})
            sent_data = sentiment_map.get(ticker, {})
            if sent_data.get("social_sentiment"):
                base_data["sentiment"] = sent_data["social_sentiment"]
            if sent_data.get("recent_news"):
                base_data["recent_news"] = sent_data["recent_news"]
            base_data["sentiment_flag"] = sent_data.get("sentiment_flag", "NO_DATA")
            base_data["news_flag"] = sent_data.get("news_flag", "NO_DATA")
            if sent_data.get("sentiment_warning"):
                base_data["sentiment_warning"] = sent_data["sentiment_warning"]
            if sent_data.get("news_warning"):
                base_data["news_warning"] = sent_data["news_warning"]
            if not isinstance(deep_data, Exception) and isinstance(deep_data, dict) and "error" not in deep_data:
                base_data.update(deep_data)
            if ticker in x_batch and "error" not in x_batch.get(ticker, {}):
                base_data["x_sentiment"] = x_batch[ticker]
            base_data["quant_score"] = top_scores.get(ticker, 0)
            enriched_candidates[ticker] = base_data

        for td in flagged:
            ticker = td["ticker"]
            base_data = candidates.get(ticker, {})
            base_data["sentiment_flag"] = td.get("sentiment_flag", "UNKNOWN")
            base_data["news_flag"] = td.get("news_flag", "UNKNOWN")
            if td.get("sentiment_warning"):
                base_data["sentiment_warning"] = td["sentiment_warning"]
            if td.get("news_warning"):
                base_data["news_warning"] = td["news_warning"]
            if td.get("social_sentiment"):
                base_data["sentiment"] = td["social_sentiment"]
            if td.get("recent_news"):
                base_data["recent_news"] = td["recent_news"]
            base_data["quant_score"] = top_scores.get(ticker, 0)
            enriched_candidates[f"FLAGGED_{ticker}"] = base_data

        print(f"[Wide Scan] Complete: {len(enriched_candidates)} candidates ({len(flagged)} flagged) ({time.time()-scan_start:.1f}s)")

        return {
            "news_context": news_context,
            "total_candidates_scanned": len(all_tickers),
            "candidates_scored": len(candidates),
            "flagged_tickers": [
                {
                    "ticker": td["ticker"],
                    "sentiment_flag": td.get("sentiment_flag"),
                    "news_flag": td.get("news_flag"),
                    "warning": td.get("sentiment_warning", td.get("news_warning", "")),
                }
                for td in flagged
            ],
            "top_ranked": [
                {"ticker": t, "score": s} for t, s, _ in top_ranked[:enrich_top]
            ],
            "enriched_data": enriched_candidates,
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
            "market_news": [],
        }

    async def get_oversold(self) -> dict:
        """Scan for oversold bounce candidates."""
        oversold = await self.finviz.get_oversold_stocks()
        return {
            "oversold_stocks": oversold,
            "market_news": [],
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
        screener_gainers = await self.finviz.get_screener_results("ta_topgainers")

        unusual_options, options_volume_leaders = await asyncio.gather(
            self.options.get_unusual_options_activity(),
            self.options.get_options_volume_leaders(),
            return_exceptions=True,
        )
        if isinstance(unusual_options, Exception): unusual_options = []
        if isinstance(options_volume_leaders, Exception): options_volume_leaders = []

        options_signals = self.options.interpret_flow(unusual_options) if unusual_options else {}

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
            "screener_gainers": screener_gainers,
            "unusual_volume": unusual_vol,
            "new_52_week_highs": new_highs,
            "most_active": most_active,
            "options_flow": unusual_options,
            "options_signals": options_signals,
        }

    async def get_top_fundamental_catalysts(self) -> dict:
        """
        Scan for the best fundamental catalysts — earnings beats,
        revenue growth, insider buying, analyst upgrades.
        """
        upcoming_earnings = self.finnhub.get_upcoming_earnings()

        earnings_tickers = [
            e["ticker"] for e in upcoming_earnings[:8] if e.get("ticker")
        ]
        all_tickers = list(dict.fromkeys(earnings_tickers[:8]))

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
            "market_news": [],
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

        return {
            "unusual_volume_stocks": unusual_vol,
            "most_active": most_active,
        }

    async def get_earnings_catalyst_watch(self) -> dict:
        """
        Enhanced earnings watch: pulls all upcoming earnings,
        enriches with full data, and scores by volatility potential.
        """
        import asyncio

        upcoming_earnings = self.finnhub.get_upcoming_earnings()

        earnings_tickers = [
            e["ticker"] for e in upcoming_earnings[:30]
            if e.get("ticker") and len(e["ticker"]) <= 5
        ]

        async def light_enrich_earnings(ticker):
            try:
                earnings_hist = self.finnhub.get_earnings_surprises(ticker)
                recommendations = self.finnhub.get_recommendation_trends(ticker)
                return {
                    "snapshot": {},
                    "technicals": {},
                    "details": {},
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
            "market_news": [],
        }

    async def get_sector_rotation(self) -> dict:
        """
        Enhanced sector rotation using FMP sector ETF data + FRED macro.
        """
        fmp_sectors = {}
        if self.fmp:
            fmp_sectors = await self.fmp.get_sector_etf_snapshot()

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
            "macro_data": macro,
            "fear_greed": fear_greed if not isinstance(fear_greed, Exception) else {},
            "dxy": dxy,
            "commodities": commodities,
        }

    async def get_sector_rotation_with_stages(self) -> dict:
        """
        Simplified Weinstein sector rotation.
        Instead of 33 individual Finviz calls (3 per sector x 11), do 3 broad screens
        and count by sector. Much faster and avoids Finviz rate limiting.
        """
        import time
        start = time.time()
        print("[SECTOR] Starting sector rotation scan...")

        sector_etfs = {
            "Technology": "XLK",
            "Healthcare": "XLV",
            "Financial": "XLF",
            "Consumer Cyclical": "XLY",
            "Consumer Defensive": "XLP",
            "Industrials": "XLI",
            "Energy": "XLE",
            "Basic Materials": "XLB",
            "Real Estate": "XLRE",
            "Utilities": "XLU",
            "Communication Services": "XLC",
        }

        try:
            stage2_task = self.finviz._custom_screen(
                "v=111&f=ta_sma200_pa,ta_sma50_pa,sh_avgvol_o300&ft=4"
            )
            stage4_task = self.finviz._custom_screen(
                "v=111&f=ta_sma200_pb,ta_sma50_pb,sh_avgvol_o300&ft=4"
            )
            total_task = self.finviz._custom_screen(
                "v=111&f=sh_avgvol_o300&ft=4"
            )

            stage2_stocks, stage4_stocks, total_stocks = await asyncio.gather(
                stage2_task, stage4_task, total_task,
                return_exceptions=True,
            )

            if isinstance(stage2_stocks, Exception):
                print(f"[SECTOR] Stage 2 screen failed: {stage2_stocks}")
                stage2_stocks = []
            if isinstance(stage4_stocks, Exception):
                print(f"[SECTOR] Stage 4 screen failed: {stage4_stocks}")
                stage4_stocks = []
            if isinstance(total_stocks, Exception):
                print(f"[SECTOR] Total screen failed: {total_stocks}")
                total_stocks = []

            print(f"[SECTOR] Raw counts: Stage2={len(stage2_stocks)}, Stage4={len(stage4_stocks)}, Total={len(total_stocks)} ({time.time()-start:.1f}s)")

            if not stage2_stocks and not stage4_stocks and not total_stocks:
                print("[SECTOR] WARNING: All Finviz screens returned empty. Finviz may be blocking requests.")
                return {
                    "error": "Finviz screener returned no data. The service may be temporarily unavailable.",
                    "sectors": [],
                    "breakout_candidates": [],
                }

            sectors_map = {}

            for stock in total_stocks:
                sector = stock.get("sector") or stock.get("Sector") or "Unknown"
                if sector in ("Unknown", ""):
                    continue
                if sector not in sectors_map:
                    sectors_map[sector] = {"total": 0, "stage2": 0, "stage4": 0}
                sectors_map[sector]["total"] += 1

            for stock in stage2_stocks:
                sector = stock.get("sector") or stock.get("Sector") or "Unknown"
                if sector in sectors_map:
                    sectors_map[sector]["stage2"] += 1

            for stock in stage4_stocks:
                sector = stock.get("sector") or stock.get("Sector") or "Unknown"
                if sector in sectors_map:
                    sectors_map[sector]["stage4"] += 1

            sectors = []
            for sector_name, counts in sectors_map.items():
                total = counts["total"]
                if total == 0:
                    continue

                s2_pct = round(counts["stage2"] / total * 100, 1)
                s4_pct = round(counts["stage4"] / total * 100, 1)

                if s2_pct >= 60:
                    stage = "Stage 2 - Advancing"
                    signal = "STRONG — Fish here for breakouts"
                elif s2_pct >= 40:
                    stage = "Early Stage 2 / Late Stage 1"
                    signal = "EMERGING — Watch for breakout confirmation"
                elif s4_pct >= 50:
                    stage = "Stage 4 - Declining"
                    signal = "AVOID — Don't buy stocks in this sector"
                elif s4_pct >= 30:
                    stage = "Stage 3 - Topping"
                    signal = "CAUTION — Reduce exposure"
                else:
                    stage = "Stage 1 - Basing"
                    signal = "WATCH — Not ready yet"

                sectors.append({
                    "sector": sector_name,
                    "etf": sector_etfs.get(sector_name, ""),
                    "stage2_pct": s2_pct,
                    "stage4_pct": s4_pct,
                    "stage2_count": counts["stage2"],
                    "stage4_count": counts["stage4"],
                    "total_count": total,
                    "sector_stage": stage,
                    "signal": signal,
                })

            sectors.sort(key=lambda x: x["stage2_pct"], reverse=True)

            print(f"[SECTOR] Processed {len(sectors)} sectors ({time.time()-start:.1f}s)")
            for s in sectors:
                print(f"[SECTOR]   {s['sector']}: {s['stage2_pct']}% Stage 2, {s['stage4_pct']}% Stage 4 — {s['sector_stage']}")

            top_sectors = [s["sector"] for s in sectors[:3] if s["stage2_pct"] >= 40]
            breakout_candidates = []

            if top_sectors:
                for stock in stage2_stocks:
                    sector = stock.get("sector") or stock.get("Sector")
                    if sector in top_sectors:
                        ticker = stock.get("ticker") or stock.get("Ticker")
                        if ticker:
                            breakout_candidates.append({
                                "ticker": ticker,
                                "company": stock.get("company") or stock.get("Company", ""),
                                "sector": sector,
                                "price": stock.get("price") or stock.get("Price"),
                                "change": stock.get("change") or stock.get("Change"),
                                "volume": stock.get("volume") or stock.get("Volume"),
                            })

                breakout_candidates = breakout_candidates[:15]
                print(f"[SECTOR] Found {len(breakout_candidates)} breakout candidates from top sectors: {top_sectors} ({time.time()-start:.1f}s)")

                enriched = []
                for candidate in breakout_candidates[:8]:
                    ticker = candidate["ticker"]
                    try:
                        overview = await asyncio.wait_for(
                            self.stockanalysis.get_overview(ticker),
                            timeout=8.0,
                        )
                        candidate.update(overview or {})
                    except Exception as e:
                        print(f"[SECTOR] Failed to enrich {ticker}: {e}")
                    enriched.append(candidate)

                breakout_candidates = enriched
                print(f"[SECTOR] Enriched {len(breakout_candidates)} candidates ({time.time()-start:.1f}s)")

            fear_greed = {}
            try:
                fear_greed = await asyncio.wait_for(
                    self.fear_greed.get_fear_greed_index(),
                    timeout=8.0,
                )
            except Exception:
                pass

            fmp_sectors = {}
            if self.fmp:
                try:
                    fmp_sectors = await asyncio.wait_for(
                        self.fmp.get_sector_performance(),
                        timeout=8.0,
                    )
                except Exception:
                    pass

            result = {
                "sector_stages": sectors,
                "breakout_candidates": breakout_candidates,
                "top_sectors": top_sectors,
                "fear_greed": fear_greed if not isinstance(fear_greed, Exception) else {},
                "fmp_sector_performance": fmp_sectors if not isinstance(fmp_sectors, Exception) else {},
                "scan_summary": {
                    "total_stocks_scanned": len(total_stocks),
                    "stage2_total": len(stage2_stocks),
                    "stage4_total": len(stage4_stocks),
                    "sectors_analyzed": len(sectors),
                },
            }

            print(f"[SECTOR] Final result: {len(str(result)):,} chars ({time.time()-start:.1f}s)")
            return result

        except Exception as e:
            print(f"[SECTOR] Fatal error: {e}")
            import traceback
            traceback.print_exc()
            return {
                "error": f"Sector rotation scan failed: {str(e)}",
                "sectors": [],
                "breakout_candidates": [],
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

        return {
            "top_losers": losers,
            "overbought_stocks": overbought,
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
                st_result, overview = await asyncio.gather(
                    self.stocktwits.get_sentiment(ticker),
                    self.stockanalysis.get_overview(ticker),
                    return_exceptions=True,
                )

                return {
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

        return {
            "theme_name": theme_data["name"],
            "ranked_tickers": [
                {"ticker": t, "score": s} for t, s, _ in theme_results
            ],
            "enriched_data": enriched,
            "market_news": [],
        }

    async def get_morning_briefing(self) -> dict:
        """
        Combined intelligence briefing pulling the top signal from every data source.
        Designed to give a full market snapshot + top actionable moves in one call.

        Runs ALL major scans in parallel, takes the #1 result from each,
        and packages everything for Claude to synthesize into a briefing.
        """
        import asyncio
        from data.scoring_engine import score_for_trades, score_for_investments, score_for_squeeze

        briefing_tasks = [
            self.fear_greed.get_fear_greed_index(),
            asyncio.to_thread(self.fred.get_quick_macro),
            self.finviz.get_stage2_breakouts(),
            self.finviz.get_volume_breakouts(),
            self.finviz.get_macd_crossovers(),
            self.finviz.get_unusual_volume(),
            self.finviz.get_new_highs(),
            self.finviz.get_high_short_float(),
            self.finviz.get_insider_buying(),
            self.finviz.get_revenue_growth_leaders(),
            self.finviz.get_rsi_recovery(),
            self.finviz.get_accumulation_stocks(),
            self.stocktwits.get_trending(),
            asyncio.to_thread(self.finnhub.get_upcoming_earnings),
        ]
        if self.fmp:
            briefing_tasks.append(self.fmp.get_market_news(limit=15))
        else:
            briefing_tasks.append(asyncio.sleep(0))

        (
            fear_greed,
            fred_macro,
            stage2_breakouts,
            volume_breakouts,
            macd_crossovers,
            unusual_volume,
            new_highs,
            high_short,
            insider_buying,
            revenue_leaders,
            rsi_recovery,
            accumulation,
            trending,
            upcoming_earnings,
            market_news_raw,
        ) = await asyncio.gather(*briefing_tasks, return_exceptions=True)

        def safe(val, default=None):
            if default is None:
                default = []
            return val if not isinstance(val, Exception) else default

        market_news = safe(market_news_raw)
        fear_greed = safe(fear_greed, {})
        fred_macro = safe(fred_macro, {})
        stage2_breakouts = safe(stage2_breakouts)
        volume_breakouts = safe(volume_breakouts)
        macd_crossovers = safe(macd_crossovers)
        unusual_volume = safe(unusual_volume)
        new_highs = safe(new_highs)
        high_short = safe(high_short)
        insider_buying = safe(insider_buying)
        revenue_leaders = safe(revenue_leaders)
        rsi_recovery = safe(rsi_recovery)
        accumulation = safe(accumulation)
        trending = safe(trending)
        upcoming_earnings = safe(upcoming_earnings)

        fmp_data = {}
        if self.fmp:
            try:
                dxy, commodities, treasuries, sector_perf, indices = await asyncio.gather(
                    self.fmp.get_dxy(),
                    self.fmp.get_key_commodities(),
                    self.fmp.get_treasury_rates(),
                    self.fmp.get_sector_performance(),
                    self.fmp.get_market_indices(),
                    return_exceptions=True,
                )
                fmp_data = {
                    "dxy": dxy if not isinstance(dxy, Exception) else {},
                    "commodities": commodities if not isinstance(commodities, Exception) else {},
                    "treasury_yields": treasuries if not isinstance(treasuries, Exception) else {},
                    "sector_performance": sector_perf if not isinstance(sector_perf, Exception) else [],
                    "indices": indices if not isinstance(indices, Exception) else {},
                }
            except:
                pass

        all_tickers = set()
        screener_sources = {}

        source_map = {
            "stage2_breakout": stage2_breakouts,
            "volume_breakout": volume_breakouts,
            "macd_crossover": macd_crossovers,
            "unusual_volume": unusual_volume,
            "new_high": new_highs,
            "high_short_float": high_short,
            "insider_buying": insider_buying,
            "revenue_growth": revenue_leaders,
            "rsi_recovery": rsi_recovery,
            "accumulation": accumulation,
        }

        for source_name, source_list in source_map.items():
            if isinstance(source_list, list):
                for item in source_list:
                    if isinstance(item, dict) and item.get("ticker"):
                        t = item["ticker"].upper().strip()
                        if len(t) <= 5 and t.isalpha():
                            all_tickers.add(t)
                            if t not in screener_sources:
                                screener_sources[t] = []
                            screener_sources[t].append(source_name)

        for t in (trending or []):
            if isinstance(t, dict) and t.get("ticker"):
                ticker = t["ticker"].upper().strip()
                all_tickers.add(ticker)
                if ticker not in screener_sources:
                    screener_sources[ticker] = []
                screener_sources[ticker].append("social_trending")

        print(f"[Briefing] {len(all_tickers)} unique tickers across all sources")

        multi_signal = {t: sources for t, sources in screener_sources.items() if len(sources) >= 2}
        single_signal = {t: sources for t, sources in screener_sources.items() if len(sources) == 1}

        print(f"[Briefing] {len(multi_signal)} multi-signal tickers: {list(multi_signal.keys())[:10]}")

        priority_tickers = list(multi_signal.keys())[:15]
        remaining_slots = 20 - len(priority_tickers)
        if remaining_slots > 0:
            filler = [t for t in single_signal.keys() if t not in priority_tickers][:remaining_slots]
            priority_tickers.extend(filler)

        async def enrich_briefing(ticker):
            try:
                st_result, overview = await asyncio.gather(
                    self.stocktwits.get_sentiment(ticker),
                    self.stockanalysis.get_overview(ticker),
                    return_exceptions=True,
                )

                return {
                    "sentiment": st_result if not isinstance(st_result, Exception) else {},
                    "overview": overview if not isinstance(overview, Exception) else {},
                }
            except Exception as e:
                return {"error": str(e)}

        enrichment_results = await asyncio.gather(
            *[enrich_briefing(t) for t in priority_tickers],
            return_exceptions=True,
        )

        enriched = {}
        for ticker, result in zip(priority_tickers, enrichment_results):
            if not isinstance(result, Exception) and isinstance(result, dict) and "error" not in result:
                trade_score = score_for_trades(result)
                invest_score = score_for_investments(result)
                result["trade_score"] = trade_score
                result["invest_score"] = invest_score
                result["signal_count"] = len(screener_sources.get(ticker, []))
                result["signal_sources"] = screener_sources.get(ticker, [])
                enriched[ticker] = result

        ranked = sorted(
            enriched.items(),
            key=lambda x: (x[1].get("signal_count", 0), x[1].get("trade_score", 0)),
            reverse=True,
        )

        return {
            "news_context": {"market_news": market_news},
            "total_tickers_detected": len(all_tickers),
            "multi_signal_tickers": {t: sources for t, sources in list(multi_signal.items())[:10]},
            "ranked_candidates": [
                {
                    "ticker": t,
                    "trade_score": d.get("trade_score", 0),
                    "invest_score": d.get("invest_score", 0),
                    "signal_count": d.get("signal_count", 0),
                    "signal_sources": d.get("signal_sources", []),
                }
                for t, d in ranked[:15]
            ],
            "enriched_data": {t: d for t, d in ranked[:10]},
            "fear_greed": fear_greed,
            "fred_macro": fred_macro,
            "fmp_market_data": fmp_data,
            "highlights": {
                "stage2_breakouts": stage2_breakouts[:3] if isinstance(stage2_breakouts, list) else [],
                "volume_breakouts": volume_breakouts[:3] if isinstance(volume_breakouts, list) else [],
                "macd_crossovers": macd_crossovers[:3] if isinstance(macd_crossovers, list) else [],
                "high_short_float": high_short[:3] if isinstance(high_short, list) else [],
                "insider_buying": insider_buying[:3] if isinstance(insider_buying, list) else [],
                "revenue_growth": revenue_leaders[:3] if isinstance(revenue_leaders, list) else [],
                "rsi_recovery": rsi_recovery[:3] if isinstance(rsi_recovery, list) else [],
                "social_trending": [t.get("ticker") for t in trending[:5]] if isinstance(trending, list) else [],
            },
            "upcoming_earnings": upcoming_earnings[:5] if isinstance(upcoming_earnings, list) else [],
        }

    async def analyze_portfolio(self, tickers: list) -> dict:
        """
        Full analysis pipeline for a user-provided list of tickers (up to 25).
        Fetches all data sources for every ticker with per-ticker timeouts,
        scores each one, and ranks them for portfolio decision-making.
        """
        import time
        start = time.time()
        from data.scoring_engine import score_for_trades, score_for_investments

        tickers = [t.upper().strip() for t in tickers[:25] if t.strip()]
        print(f"[PORTFOLIO] Analyzing {len(tickers)} tickers: {tickers}")

        async def full_enrich(ticker):
            data = {"ticker": ticker}

            try:
                overview = await asyncio.wait_for(
                    self.stockanalysis.get_overview(ticker), timeout=8.0,
                )
                if overview:
                    data["overview"] = overview
            except Exception as e:
                print(f"[PORTFOLIO] {ticker} overview failed: {e}")

            try:
                analyst = await asyncio.wait_for(
                    self.stockanalysis.get_analyst_ratings(ticker), timeout=8.0,
                )
                if analyst:
                    data["analyst_ratings"] = analyst
            except Exception as e:
                print(f"[PORTFOLIO] {ticker} analyst failed: {e}")

            try:
                sentiment = await asyncio.wait_for(
                    self.stocktwits.get_sentiment(ticker), timeout=6.0,
                )
                if sentiment:
                    data["sentiment"] = sentiment
            except Exception as e:
                print(f"[PORTFOLIO] {ticker} sentiment failed: {e}")

            try:
                insider = await asyncio.to_thread(
                    lambda: self.finnhub.get_insider_sentiment(ticker)
                )
                if insider:
                    data["insider_sentiment"] = insider
            except Exception as e:
                print(f"[PORTFOLIO] {ticker} insider failed: {e}")

            try:
                earnings = await asyncio.to_thread(
                    lambda: self.finnhub.get_earnings_surprises(ticker)
                )
                if earnings:
                    data["earnings_history"] = earnings
            except Exception as e:
                print(f"[PORTFOLIO] {ticker} earnings failed: {e}")

            try:
                recs = await asyncio.to_thread(
                    lambda: self.finnhub.get_recommendation_trends(ticker)
                )
                if recs:
                    data["recommendations"] = recs
            except Exception as e:
                print(f"[PORTFOLIO] {ticker} recommendations failed: {e}")

            return data

        all_data = []
        for i in range(0, len(tickers), 5):
            batch = tickers[i:i+5]
            batch_results = await asyncio.gather(
                *[asyncio.wait_for(full_enrich(t), timeout=15.0) for t in batch],
                return_exceptions=True,
            )
            for ticker, result in zip(batch, batch_results):
                if isinstance(result, Exception):
                    print(f"[PORTFOLIO] {ticker} fully failed: {result}")
                    all_data.append({"ticker": ticker, "error": "Failed to fetch data"})
                else:
                    all_data.append(result)

            if i + 5 < len(tickers):
                await asyncio.sleep(0.3)

            print(f"[PORTFOLIO] Enriched {min(i+5, len(tickers))}/{len(tickers)} tickers ({time.time()-start:.1f}s)")

        enriched = {}
        for data in all_data:
            ticker = data.get("ticker", "?")
            if "error" in data:
                enriched[ticker] = data
                continue
            trade_score = score_for_trades(data)
            invest_score = score_for_investments(data)
            combined = round((invest_score * 0.4) + (trade_score * 0.4) + ((invest_score + trade_score) / 2 * 0.2), 1)
            data["trade_score"] = trade_score
            data["invest_score"] = invest_score
            data["combined_score"] = combined
            enriched[ticker] = data

        ranked = sorted(
            [(t, d) for t, d in enriched.items() if "error" not in d],
            key=lambda x: x[1].get("combined_score", 0),
            reverse=True,
        )

        fear_greed = {}
        try:
            fear_greed = await asyncio.wait_for(
                self.fear_greed.get_fear_greed_index(), timeout=8.0,
            )
        except:
            pass

        print(f"[PORTFOLIO] Complete: {len(enriched)} tickers enriched ({time.time()-start:.1f}s)")

        return {
            "tickers_analyzed": len(tickers),
            "ranked_tickers": [
                {"ticker": t, "combined_score": d.get("combined_score", 0),
                 "trade_score": d.get("trade_score", 0),
                 "invest_score": d.get("invest_score", 0)}
                for t, d in ranked
            ],
            "enriched_data": enriched,
            "fear_greed": fear_greed if not isinstance(fear_greed, Exception) else {},
            "macro": self.fred.get_quick_macro(),
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

    async def get_cross_platform_trending(self) -> dict:
        """
        Aggregate trending stocks across ALL available platforms.
        Cross-references: StockTwits, Yahoo Finance, Finviz, Polygon,
        StockAnalysis. Counts how many platforms each ticker appears on.
        Stocks appearing on 3+ platforms = highest conviction trending.
        Then enriches the top tickers with full data.
        """
        from data.scoring_engine import score_for_trades, passes_market_cap_filter
        from collections import Counter

        (
            stocktwits_trending,
            yahoo_trending,
            stockanalysis_trending,
            finviz_most_active,
            finviz_unusual_volume,
            finviz_top_gainers,
            reddit_trending,
        ) = await asyncio.gather(
            self.stocktwits.get_trending(),
            scrape_yahoo_trending(),
            scrape_stockanalysis_trending(),
            self.finviz.get_most_active(),
            self.finviz.get_unusual_volume(),
            self.finviz.get_screener_results("ta_topgainers"),
            self.reddit.get_all_stocks_trending(),
            return_exceptions=True,
        )

        if isinstance(stocktwits_trending, Exception): stocktwits_trending = []
        if isinstance(yahoo_trending, Exception): yahoo_trending = []
        if isinstance(stockanalysis_trending, Exception): stockanalysis_trending = []
        if isinstance(finviz_most_active, Exception): finviz_most_active = []
        if isinstance(finviz_unusual_volume, Exception): finviz_unusual_volume = []
        if isinstance(finviz_top_gainers, Exception): finviz_top_gainers = []
        if isinstance(reddit_trending, Exception): reddit_trending = []

        ticker_sources = {}

        def add_tickers(items, source_name, ticker_key="ticker"):
            for item in (items or []):
                if isinstance(item, dict):
                    t = item.get(ticker_key, "").upper().strip()
                    if t and len(t) <= 6 and t.isalpha():
                        if t not in ticker_sources:
                            ticker_sources[t] = set()
                        ticker_sources[t].add(source_name)

        add_tickers(stocktwits_trending, "StockTwits")
        add_tickers(yahoo_trending, "Yahoo Finance")
        add_tickers(stockanalysis_trending, "StockAnalysis")
        add_tickers(finviz_most_active, "Finviz Active")
        add_tickers(finviz_unusual_volume, "Finviz Volume")
        add_tickers(finviz_top_gainers, "Finviz Gainers")
        add_tickers(reddit_trending, "Reddit")

        xai_trending = {}
        if self.xai:
            try:
                xai_trending = await asyncio.wait_for(
                    self.xai.get_trending_tickers("stock"),
                    timeout=30.0,
                )
                print(f"[TRENDING] xAI returned {len(xai_trending.get('trending_tickers', []))} trending tickers from X")
            except Exception as e:
                print(f"[TRENDING] xAI trending failed: {e}")

        if xai_trending and "trending_tickers" in xai_trending:
            for item in xai_trending["trending_tickers"]:
                ticker_val = item.get("ticker", "").upper().strip()
                if ticker_val and len(ticker_val) <= 6 and ticker_val.isalpha():
                    if ticker_val not in ticker_sources:
                        ticker_sources[ticker_val] = set()
                    ticker_sources[ticker_val].add("X_Twitter")

        ranked = sorted(
            ticker_sources.items(),
            key=lambda x: len(x[1]),
            reverse=True,
        )

        multi_source = [(t, srcs) for t, srcs in ranked if len(srcs) >= 2]

        top_tickers = [t for t, _ in multi_source[:20]]

        print(f"[Trending] {len(ticker_sources)} unique tickers across all platforms")
        print(f"[Trending] {len(multi_source)} appear on 2+ platforms")
        print(f"[Trending] Top multi-platform: {[(t, len(s)) for t, s in multi_source[:10]]}")

        async def full_enrich(ticker):
            try:
                st_result, overview, analyst = await asyncio.gather(
                    self.stocktwits.get_sentiment(ticker),
                    self.stockanalysis.get_overview(ticker),
                    self.stockanalysis.get_analyst_ratings(ticker),
                    return_exceptions=True,
                )

                return {
                    "sentiment": st_result if not isinstance(st_result, Exception) else {},
                    "overview": overview if not isinstance(overview, Exception) else {},
                    "analyst_ratings": analyst if not isinstance(analyst, Exception) else {},
                }
            except Exception as e:
                return {"error": str(e)}

        enrichment_results = await asyncio.gather(
            *[full_enrich(t) for t in top_tickers],
            return_exceptions=True,
        )

        enriched = {}
        for ticker, result in zip(top_tickers, enrichment_results):
            if isinstance(result, Exception) or not isinstance(result, dict) or "error" in result:
                continue

            if not passes_market_cap_filter(result, "market_scan"):
                continue

            quant_score = score_for_trades(result)
            result["quant_score"] = quant_score
            result["trending_sources"] = list(ticker_sources.get(ticker, []))
            result["source_count"] = len(ticker_sources.get(ticker, []))
            enriched[ticker] = result

        sorted_tickers = sorted(
            enriched.items(),
            key=lambda x: (x[1].get("source_count", 0), x[1].get("quant_score", 0)),
            reverse=True,
        )

        sorted_enriched = {}
        for t, d in sorted_tickers:
            sorted_enriched[t] = d

        return {
            "total_unique_tickers": len(ticker_sources),
            "multi_platform_count": len(multi_source),
            "source_summary": {
                "StockTwits": len(stocktwits_trending),
                "Yahoo Finance": len(yahoo_trending),
                "StockAnalysis": len(stockanalysis_trending),
                "Finviz Active": len(finviz_most_active),
                "Finviz Volume": len(finviz_unusual_volume),
                "Finviz Gainers": len(finviz_top_gainers),
                "X_Twitter": len(xai_trending.get("trending_tickers", [])) if xai_trending else 0,
            },
            "x_twitter_data": xai_trending if xai_trending else {},
            "ranked_tickers": [
                {
                    "ticker": t,
                    "source_count": d.get("source_count", 0),
                    "sources": d.get("trending_sources", []),
                    "quant_score": d.get("quant_score", 0),
                }
                for t, d in sorted_tickers[:15]
            ],
            "enriched_data": sorted_enriched,
            "market_news": [],
        }

    async def get_crypto_scanner(self) -> dict:
        """
        Combined crypto scanner pulling from BOTH CoinGecko and CoinMarketCap.

        CoinGecko provides: derivatives/funding rates, social/dev metrics, trending
        CMC provides: most-visited (FOMO signal), new listings, richer metadata, volume change

        Cross-referencing trending from both platforms = strongest momentum signal.
        """
        import asyncio

        tasks = {}

        if self.coingecko:
            tasks["cg_dashboard"] = self.coingecko.get_crypto_dashboard()

        if self.cmc:
            tasks["cmc_dashboard"] = self.cmc.get_full_dashboard()

        tasks["hyperliquid"] = self.hyperliquid.get_crypto_dashboard()
        tasks["fear_greed"] = self.fear_greed.get_fear_greed_index()
        tasks["crypto_news"] = self.alphavantage.get_news_sentiment("CRYPTO:BTC")

        if self.altfins:
            tasks["altfins"] = self.altfins.get_crypto_scanner_data()

        if self.xai:
            tasks["x_twitter_crypto"] = self.xai.get_trending_tickers("crypto")

        task_names = list(tasks.keys())
        results = await asyncio.gather(
            *tasks.values(),
            return_exceptions=True,
        )
        data = {}
        for name, result in zip(task_names, results):
            data[name] = result if not isinstance(result, Exception) else {}

        cg = data.get("cg_dashboard", {})
        cmc = data.get("cmc_dashboard", {})

        cg_trending_symbols = set()
        cg_trending_data = cg.get("trending", {})
        if isinstance(cg_trending_data, dict):
            for coin in cg_trending_data.get("coins", []):
                item = coin.get("item", {})
                sym = item.get("symbol", "").upper()
                if sym:
                    cg_trending_symbols.add(sym)

        cmc_trending_symbols = set()
        for coin in (cmc.get("trending") or []):
            sym = coin.get("symbol", "").upper()
            if sym:
                cmc_trending_symbols.add(sym)

        cmc_most_visited_symbols = set()
        for coin in (cmc.get("most_visited") or []):
            sym = coin.get("symbol", "").upper()
            if sym:
                cmc_most_visited_symbols.add(sym)

        dual_trending = cg_trending_symbols & cmc_trending_symbols
        high_attention = (cg_trending_symbols | cmc_trending_symbols) & cmc_most_visited_symbols

        volume_acceleration = {}
        for coin in (cmc.get("listings") or []):
            sym = coin.get("symbol", "")
            quote = coin.get("quote", {}).get("USD", {})
            vol_change = quote.get("volume_change_24h")
            if vol_change is not None and sym:
                volume_acceleration[sym] = {
                    "volume_24h": quote.get("volume_24h"),
                    "volume_change_24h": vol_change,
                    "market_cap_dominance": quote.get("market_cap_dominance"),
                }

        deep_dive_ids = []
        if isinstance(cg_trending_data, dict):
            for coin in cg_trending_data.get("coins", [])[:4]:
                cid = coin.get("item", {}).get("id")
                if cid:
                    deep_dive_ids.append(cid)

        cg_gl = cg.get("gainers_losers", {})
        if isinstance(cg_gl, dict):
            for g in (cg_gl.get("gainers") or [])[:1]:
                cid = g.get("id")
                if cid and cid not in deep_dive_ids:
                    deep_dive_ids.append(cid)

        deep_dive = {}
        if deep_dive_ids and self.coingecko:
            deep_dive = await self.coingecko.get_coin_deep_dive(deep_dive_ids[:5])

        derivatives = cg.get("derivatives", [])
        funding_analysis = self._analyze_funding_rates(derivatives) if derivatives else {}

        cg_categories = (cg.get("categories") or [])[:10]
        cmc_categories = (cmc.get("categories") or [])[:10]

        new_listings = (cmc.get("new_listings") or [])[:10]

        cmc_gainers_losers = cmc.get("gainers_losers", {})

        trending_symbols = list(dual_trending | high_attention)[:10]
        coin_metadata = {}
        if trending_symbols and self.cmc:
            try:
                coin_metadata = await self.cmc.get_coin_info(trending_symbols)
            except:
                pass

        return {
            "cg_global": cg.get("global_market", {}),
            "cmc_global": cmc.get("global_metrics", {}),

            "cg_top_coins": (cg.get("top_coins") or [])[:20],
            "cmc_listings": (cmc.get("listings") or [])[:15],

            "cg_trending": cg_trending_data,
            "cmc_trending": cmc.get("trending", []),
            "cmc_most_visited": (cmc.get("most_visited") or [])[:10],
            "dual_trending": list(dual_trending),
            "high_attention": list(high_attention),

            "cg_gainers_losers": cg.get("gainers_losers", {}),
            "cmc_gainers_losers": cmc_gainers_losers,

            "derivatives_tickers": (derivatives or [])[:30],
            "funding_analysis": funding_analysis,

            "hyperliquid": data.get("hyperliquid", {}),

            "cg_categories": cg_categories,
            "cmc_categories": cmc_categories,

            "volume_acceleration": dict(sorted(volume_acceleration.items(), key=lambda x: abs(x[1].get("volume_change_24h", 0)), reverse=True)[:15]),

            "new_listings": new_listings,

            "deep_dive": deep_dive,

            "coin_metadata": coin_metadata if not isinstance(coin_metadata, Exception) else {},

            "fear_greed": data.get("fear_greed", {}),
            "crypto_news": data.get("crypto_news", {}),

            "altfins": data.get("altfins", {}),

            "x_twitter_crypto": data.get("x_twitter_crypto", {}),
        }

    async def run_ai_screener(self, filters: dict) -> dict:
        """
        AI-powered custom screener. Takes parsed filter criteria and
        builds Finviz screen URL, runs it, then enriches results with
        StockAnalysis fundamentals.
        """
        import asyncio
        from data.scoring_engine import score_for_trades

        f_parts = []

        mc_min = filters.get("market_cap_min")
        mc_max = filters.get("market_cap_max")
        if mc_min is not None:
            if mc_min >= 200: f_parts.append("cap_megaover")
            elif mc_min >= 10: f_parts.append("cap_largeover")
            elif mc_min >= 2: f_parts.append("cap_midover")
            elif mc_min >= 0.3: f_parts.append("cap_smallover")
            elif mc_min >= 0.05: f_parts.append("cap_microover")
        if mc_max is not None:
            if mc_max <= 0.3: f_parts.append("cap_smallunder")
            elif mc_max <= 2: f_parts.append("cap_midunder")
            elif mc_max <= 10: f_parts.append("cap_largeunder")
            elif mc_max <= 200: f_parts.append("cap_megaunder")

        rg = filters.get("revenue_growth_min")
        if rg is not None:
            if rg >= 30: f_parts.append("fa_salesqoq_o30")
            elif rg >= 25: f_parts.append("fa_salesqoq_o25")
            elif rg >= 20: f_parts.append("fa_salesqoq_o20")
            elif rg >= 15: f_parts.append("fa_salesqoq_o15")
            elif rg >= 10: f_parts.append("fa_salesqoq_o10")
            elif rg >= 5: f_parts.append("fa_salesqoq_o5")

        eg = filters.get("eps_growth_min")
        if eg is not None:
            if eg >= 30: f_parts.append("fa_epsqoq_o30")
            elif eg >= 25: f_parts.append("fa_epsqoq_o25")
            elif eg >= 20: f_parts.append("fa_epsqoq_o20")
            elif eg >= 15: f_parts.append("fa_epsqoq_o15")
            elif eg >= 10: f_parts.append("fa_epsqoq_o10")
            elif eg >= 5: f_parts.append("fa_epsqoq_o5")

        pe_max = filters.get("pe_max")
        if pe_max is not None:
            if pe_max <= 5: f_parts.append("fa_pe_u5")
            elif pe_max <= 10: f_parts.append("fa_pe_u10")
            elif pe_max <= 15: f_parts.append("fa_pe_u15")
            elif pe_max <= 20: f_parts.append("fa_pe_u20")
            elif pe_max <= 30: f_parts.append("fa_pe_u30")
            elif pe_max <= 40: f_parts.append("fa_pe_u40")
            elif pe_max <= 50: f_parts.append("fa_pe_u50")

        ps_max = filters.get("ps_max")
        if ps_max is not None:
            if ps_max <= 1: f_parts.append("fa_ps_u1")
            elif ps_max <= 2: f_parts.append("fa_ps_u2")
            elif ps_max <= 3: f_parts.append("fa_ps_u3")
            elif ps_max <= 5: f_parts.append("fa_ps_u5")

        p_min = filters.get("price_min")
        if p_min is not None:
            if p_min >= 50: f_parts.append("sh_price_o50")
            elif p_min >= 20: f_parts.append("sh_price_o20")
            elif p_min >= 10: f_parts.append("sh_price_o10")
            elif p_min >= 5: f_parts.append("sh_price_o5")

        p_max = filters.get("price_max")
        if p_max is not None:
            if p_max <= 5: f_parts.append("sh_price_u5")
            elif p_max <= 10: f_parts.append("sh_price_u10")
            elif p_max <= 20: f_parts.append("sh_price_u20")
            elif p_max <= 50: f_parts.append("sh_price_u50")

        rsi_max = filters.get("rsi_max")
        if rsi_max is not None:
            if rsi_max <= 30: f_parts.append("ta_rsi_os30")
            elif rsi_max <= 40: f_parts.append("ta_rsi_os40")
            elif rsi_max <= 50: f_parts.append("ta_rsi_os50")
            elif rsi_max <= 60: f_parts.append("ta_rsi_os60")

        rsi_min = filters.get("rsi_min")
        if rsi_min is not None:
            if rsi_min >= 70: f_parts.append("ta_rsi_ob70")
            elif rsi_min >= 60: f_parts.append("ta_rsi_ob60")
            elif rsi_min >= 50: f_parts.append("ta_rsi_ob50")

        if filters.get("above_sma200"): f_parts.append("ta_sma200_pa")
        if filters.get("above_sma50"): f_parts.append("ta_sma50_pa")
        if filters.get("below_sma200"): f_parts.append("ta_sma200_pb")
        if filters.get("below_sma50"): f_parts.append("ta_sma50_pb")

        if filters.get("insider_buying"): f_parts.append("it_latestbuys")

        if filters.get("analyst_upgrades"): f_parts.append("ta_change_u")

        if filters.get("unusual_volume"): f_parts.append("sh_relvol_o1.5")
        rv = filters.get("relative_volume_min")
        if rv is not None:
            if rv >= 3: f_parts.append("sh_relvol_o3")
            elif rv >= 2: f_parts.append("sh_relvol_o2")
            elif rv >= 1.5: f_parts.append("sh_relvol_o1.5")

        av = filters.get("avg_volume_min")
        if av is not None:
            if av >= 1000: f_parts.append("sh_avgvol_o1000")
            elif av >= 500: f_parts.append("sh_avgvol_o500")
            elif av >= 400: f_parts.append("sh_avgvol_o400")
            elif av >= 300: f_parts.append("sh_avgvol_o300")
            elif av >= 200: f_parts.append("sh_avgvol_o200")
            elif av >= 100: f_parts.append("sh_avgvol_o100")
        else:
            f_parts.append("sh_avgvol_o200")

        if filters.get("positive_margin"): f_parts.append("fa_opermargin_pos")

        de_max = filters.get("debt_equity_max")
        if de_max is not None:
            if de_max <= 0.5: f_parts.append("fa_debteq_u0.5")
            elif de_max <= 1: f_parts.append("fa_debteq_u1")

        sf_min = filters.get("short_float_min")
        if sf_min is not None:
            if sf_min >= 20: f_parts.append("sh_short_o20")
            elif sf_min >= 15: f_parts.append("sh_short_o15")
            elif sf_min >= 10: f_parts.append("sh_short_o10")
            elif sf_min >= 5: f_parts.append("sh_short_o5")

        sector = filters.get("sector")
        if sector:
            sector_map = {
                "technology": "sec_technology",
                "healthcare": "sec_healthcare",
                "financial": "sec_financial",
                "energy": "sec_energy",
                "industrials": "sec_industrials",
                "consumer cyclical": "sec_consumercyclical",
                "consumer defensive": "sec_consumerdefensive",
                "basic materials": "sec_basicmaterials",
                "real estate": "sec_realestate",
                "utilities": "sec_utilities",
                "communication services": "sec_communicationservices",
            }
            sec_code = sector_map.get(sector.lower(), "")
            if sec_code:
                f_parts.append(sec_code)

        dy = filters.get("dividend_yield_min")
        if dy is not None:
            if dy >= 5: f_parts.append("fa_div_o5")
            elif dy >= 4: f_parts.append("fa_div_o4")
            elif dy >= 3: f_parts.append("fa_div_o3")
            elif dy >= 2: f_parts.append("fa_div_o2")
            elif dy >= 1: f_parts.append("fa_div_o1")

        perf_week = filters.get("perf_week")
        if perf_week is not None:
            if perf_week >= 20: f_parts.append("ta_perf_w20o")
            elif perf_week >= 10: f_parts.append("ta_perf_w10o")
            elif perf_week >= 5: f_parts.append("ta_perf_w5o")

        perf_month = filters.get("perf_month")
        if perf_month is not None:
            if perf_month >= 30: f_parts.append("ta_perf_4w30o")
            elif perf_month >= 20: f_parts.append("ta_perf_4w20o")
            elif perf_month >= 10: f_parts.append("ta_perf_4w10o")

        perf_week_down = filters.get("perf_week_down")
        if perf_week_down is not None:
            if perf_week_down >= 20: f_parts.append("ta_perf_w20u")
            elif perf_week_down >= 10: f_parts.append("ta_perf_w10u")

        perf_month_down = filters.get("perf_month_down")
        if perf_month_down is not None:
            if perf_month_down >= 20: f_parts.append("ta_perf_4w20u")
            elif perf_month_down >= 10: f_parts.append("ta_perf_4w10u")

        if filters.get("earnings_this_week"): f_parts.append("earningsdate_thisweek")
        if filters.get("earnings_next_week"): f_parts.append("earningsdate_nextweek")
        if filters.get("earnings_today"): f_parts.append("earningsdate_today")

        upside = filters.get("analyst_upside_min")
        if upside is not None:
            if upside >= 50: f_parts.append("targetprice_a50")
            elif upside >= 30: f_parts.append("targetprice_a30")
            elif upside >= 20: f_parts.append("targetprice_a20")
            elif upside >= 10: f_parts.append("targetprice_a10")

        if filters.get("gap_up"): f_parts.append("ta_gap_u")
        if filters.get("gap_down"): f_parts.append("ta_gap_d")

        if filters.get("low_float"): f_parts.append("sh_float_u20")
        float_max = filters.get("float_max_m")
        if float_max is not None:
            if float_max <= 10: f_parts.append("sh_float_u10")
            elif float_max <= 20: f_parts.append("sh_float_u20")
            elif float_max <= 50: f_parts.append("sh_float_u50")
            elif float_max <= 100: f_parts.append("sh_float_u100")

        custom = filters.get("custom_finviz_params")
        if custom:
            f_parts.append(custom)

        filter_str = ",".join(f_parts) if f_parts else "sh_avgvol_o200"

        is_ta_focused = any(k in filters for k in [
            "rsi_max", "rsi_min", "above_sma200", "above_sma50",
            "below_sma200", "below_sma50", "unusual_volume",
            "relative_volume_min", "gap_up", "gap_down",
        ])
        view = "171" if is_ta_focused else "111"

        sort_order = filters.get("sort", "-sh_relvol")
        screen_url = f"v={view}&f={filter_str}&ft=4&o={sort_order}"

        print(f"[AI Screener] Final Finviz URL: v={view}&f={filter_str}&ft=4&o={sort_order}")
        print(f"[AI Screener] Parsed filters: {filters}")
        print(f"[AI Screener] Filter parts: {f_parts}")

        screener_results = await self.finviz._custom_screen(screen_url)
        if isinstance(screener_results, Exception) or not screener_results:
            return {
                "filters_applied": filters,
                "finviz_url": screen_url,
                "total_results": 0,
                "results": [],
                "error": "No stocks matched your criteria. Try loosening some filters.",
            }

        print(f"[AI Screener] Found {len(screener_results)} matches")

        tickers_to_enrich = screener_results[:30]

        async def enrich_ticker(item):
            ticker = item.get("ticker", "")
            if not ticker:
                return item
            try:
                tasks = [
                    self.stockanalysis.get_overview(ticker),
                    self.stockanalysis.get_analyst_ratings(ticker),
                ]
                if self.stocktwits:
                    tasks.append(self.stocktwits.get_sentiment(ticker))

                results = await asyncio.gather(*tasks, return_exceptions=True)

                item["sa_overview"] = results[0] if not isinstance(results[0], Exception) else {}
                item["sa_analyst"] = results[1] if not isinstance(results[1], Exception) else {}
                if len(results) > 2 and not isinstance(results[2], Exception) and results[2]:
                    item["sentiment"] = results[2]
            except:
                item["sa_overview"] = {}
                item["sa_analyst"] = {}
            return item

        enriched = await asyncio.gather(
            *[enrich_ticker(item) for item in tickers_to_enrich],
            return_exceptions=True,
        )

        clean_results = []
        for r in enriched:
            if isinstance(r, Exception):
                continue
            if isinstance(r, dict):
                clean_results.append(r)

        return {
            "filters_applied": filters,
            "finviz_url": screen_url,
            "total_results": len(screener_results),
            "showing": len(clean_results),
            "results": clean_results,
        }

    def _analyze_funding_rates(self, derivatives: list) -> dict:
        if not derivatives or not isinstance(derivatives, list):
            return {}

        perps = [d for d in derivatives if d.get("contract_type") == "perpetual" and d.get("funding_rate") is not None]

        if not perps:
            return {}

        sorted_by_funding = sorted(perps, key=lambda x: x.get("funding_rate", 0), reverse=True)

        highest_funding = [{
            "symbol": p.get("symbol", ""),
            "funding_rate": p.get("funding_rate"),
            "open_interest": p.get("open_interest"),
            "volume_24h": p.get("h24_volume"),
            "price": p.get("last"),
            "change_24h": p.get("h24_percentage_change"),
            "signal": "Crowded longs — correction risk" if p.get("funding_rate", 0) > 0.03 else "Elevated long bias",
        } for p in sorted_by_funding[:10]]

        lowest_funding = [{
            "symbol": p.get("symbol", ""),
            "funding_rate": p.get("funding_rate"),
            "open_interest": p.get("open_interest"),
            "volume_24h": p.get("h24_volume"),
            "price": p.get("last"),
            "change_24h": p.get("h24_percentage_change"),
            "signal": "Crowded shorts — squeeze potential" if p.get("funding_rate", 0) < -0.01 else "Short bias",
        } for p in sorted_by_funding[-10:]]

        avg_funding = sum(p.get("funding_rate", 0) for p in perps) / len(perps) if perps else 0

        sorted_by_oi = sorted(perps, key=lambda x: x.get("open_interest", 0) or 0, reverse=True)
        highest_oi = [{
            "symbol": p.get("symbol", ""),
            "open_interest": p.get("open_interest"),
            "funding_rate": p.get("funding_rate"),
            "volume_24h": p.get("h24_volume"),
        } for p in sorted_by_oi[:10]]

        return {
            "total_perps_tracked": len(perps),
            "avg_funding_rate": round(avg_funding, 6),
            "market_bias": "Bullish (longs paying)" if avg_funding > 0.005 else "Bearish (shorts paying)" if avg_funding < -0.005 else "Neutral",
            "highest_funding": highest_funding,
            "most_negative_funding": lowest_funding,
            "highest_open_interest": highest_oi,
        }