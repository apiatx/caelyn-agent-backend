import asyncio
import time as _time

from data.polygon_provider import PolygonProvider
from data.twelvedata_provider import TwelveDataProvider
from data.finviz_scraper import FinvizScraper, scrape_yahoo_trending, scrape_stockanalysis_trending
from data.stocktwits_provider import StockTwitsProvider
from data.stockanalysis_scraper import StockAnalysisScraper
from data.options_scraper import OptionsScraper
from data.finnhub_provider import FinnhubProvider
from config import FINNHUB_API_KEY, ALPHA_VANTAGE_API_KEY, FRED_API_KEY, FMP_API_KEY, TWELVEDATA_API_KEY
from data.alphavantage_provider import AlphaVantageProvider
from data.fred_provider import FredProvider
from data.edgar_provider import EdgarProvider
from data.fear_greed_provider import FearGreedProvider
from data.fmp_provider import FMPProvider
from data.coingecko_provider import CoinGeckoProvider
from data.reddit_provider import RedditSentimentProvider
from data.altfins_provider import AltFINSProvider
from data.xai_sentiment_provider import XAISentimentProvider
from data.cache import cache, MACRO_TTL, SECTOR_ETF_TTL, CANDLE_TTL, REGIME_CANDLE_TTL


DATA_SOURCES = {
    "equity_price": {"primary": "finnhub", "secondary": "fmp"},
    "fundamentals": {"primary": "fmp", "secondary": "finnhub"},
    "crypto": {"primary": "coingecko", "secondary": "cmc"},
    "macro": {"primary": "fred", "secondary": None},
}

MAX_TICKERS_DEEP_DIVE = 10
MAX_BUDGET_POINTS = 50
MAX_DATA_GATHER_SECONDS = 10

CALL_WEIGHTS = {
    "macro": 1,
    "quote": 1,
    "candle": 2,
    "fundamentals": 3,
    "crypto_market_scan": 4,
    "light_enrich": 1,
    "deep_enrich": 3,
    "sentiment": 1,
    "news": 1,
}

PRESET_BUDGETS = {
    "macro_outlook": {"max_points": 25, "max_seconds": 8, "allow_deep_dive": False},
    "morning_briefing": {"max_points": 30, "max_seconds": 8, "allow_deep_dive": False},
    "social_momentum": {"max_points": 45, "max_seconds": 10, "allow_deep_dive": True},
    "microcap_asymmetry": {"max_points": 60, "max_seconds": 12, "allow_deep_dive": True},
    "asymmetric": {"max_points": 55, "max_seconds": 12, "allow_deep_dive": True},
    "investments": {"max_points": 55, "max_seconds": 12, "allow_deep_dive": True},
    "fundamentals_scan": {"max_points": 55, "max_seconds": 12, "allow_deep_dive": True},
    "small_cap_spec": {"max_points": 55, "max_seconds": 12, "allow_deep_dive": True},
    "squeeze": {"max_points": 50, "max_seconds": 10, "allow_deep_dive": True},
}


class BudgetTracker:
    """Weighted budget tracker for scan operations.
    Uses cost weights per call type instead of flat counting.
    Tracks elapsed time and whether budget was exhausted for graceful degradation."""

    def __init__(self, max_points: int = MAX_BUDGET_POINTS, max_seconds: float = MAX_DATA_GATHER_SECONDS, allow_deep_dive: bool = True):
        self._start = _time.time()
        self._points = 0
        self._max_points = max_points
        self._max_seconds = max_seconds
        self._exhausted = False
        self._exhausted_phase = None
        self.allow_deep_dive = allow_deep_dive

    @classmethod
    def for_preset(cls, preset: str) -> "BudgetTracker":
        config = PRESET_BUDGETS.get(preset, {})
        return cls(
            max_points=config.get("max_points", MAX_BUDGET_POINTS),
            max_seconds=config.get("max_seconds", MAX_DATA_GATHER_SECONDS),
            allow_deep_dive=config.get("allow_deep_dive", True),
        )

    @property
    def elapsed(self) -> float:
        return _time.time() - self._start

    @property
    def points(self) -> int:
        return self._points

    def tick(self, call_type: str = "light_enrich", n: int = 1):
        weight = CALL_WEIGHTS.get(call_type, 1)
        self._points += weight * n

    def can_continue(self) -> bool:
        return self._points < self._max_points and self.elapsed < self._max_seconds

    def mark_exhausted(self, phase: str):
        self._exhausted = True
        self._exhausted_phase = phase

    @property
    def was_exhausted(self) -> bool:
        return self._exhausted

    def degradation_metadata(self) -> dict:
        if not self._exhausted:
            return {"data_completeness": "full"}
        return {
            "data_completeness": "partial",
            "budget_exhausted_at": self._exhausted_phase,
            "budget_status": self.status(),
        }

    def status(self) -> str:
        return f"points={self._points}/{self._max_points} elapsed={self.elapsed:.1f}s/{self._max_seconds}s"


class CandleBudget:
    def __init__(self, max_calls: int = 5):
        self._max = max_calls
        self._used = 0
        self._cache_hits = 0
        self._blocked = 0
        self._twelvedata_used = 0
        self._twelvedata_rate_limited = 0
        self._polygon_used = 0
        self._finnhub_blocked = 0

    def can_spend(self) -> bool:
        return self._used < self._max

    def spend(self, provider: str = "polygon"):
        self._used += 1
        if provider == "twelvedata":
            self._twelvedata_used += 1
        else:
            self._polygon_used += 1

    def record_cache_hit(self):
        self._cache_hits += 1

    def record_blocked(self):
        self._blocked += 1

    def record_finnhub_blocked(self):
        self._finnhub_blocked += 1

    def record_twelvedata_rate_limited(self):
        self._twelvedata_rate_limited += 1

    def summary(self) -> str:
        return f"twelvedata={self._twelvedata_used} td_rate_limited={self._twelvedata_rate_limited} polygon={self._polygon_used} cache_hits={self._cache_hits} blocked={self._blocked} finnhub_disabled={self._finnhub_blocked}"

    def stats_dict(self) -> dict:
        return {
            "twelvedata_used": self._twelvedata_used,
            "twelvedata_rate_limited": self._twelvedata_rate_limited,
            "polygon_used": self._polygon_used,
            "cache_hits": self._cache_hits,
            "blocked": self._blocked,
            "finnhub_disabled": self._finnhub_blocked,
            "total_api_calls": self._used,
            "budget_max": self._max,
        }


_finnhub_candle_disabled_until = 0.0
_twelvedata_disabled_until = 0.0
_last_candle_budget = None


def _is_finnhub_candles_disabled() -> bool:
    return _time.time() < _finnhub_candle_disabled_until


def _disable_finnhub_candles():
    global _finnhub_candle_disabled_until
    _finnhub_candle_disabled_until = _time.time() + 3600
    print("[CIRCUIT_BREAKER] Finnhub candles disabled for 60 minutes (403)")


def _is_twelvedata_disabled() -> bool:
    return _time.time() < _twelvedata_disabled_until


def _disable_twelvedata():
    global _twelvedata_disabled_until
    _twelvedata_disabled_until = _time.time() + 900
    print("[CIRCUIT_BREAKER] TwelveData disabled for 15 minutes (auth error)")


def get_last_candle_stats() -> dict:
    if _last_candle_budget:
        return _last_candle_budget.stats_dict()
    return {}


async def fetch_with_fallback(category: str, fetch_primary, fetch_secondary=None, timeout: float = 3.0):
    """
    Tiered data source fetcher. Tries primary with timeout, falls back to secondary.
    Returns result dict or empty dict on total failure.
    """
    try:
        result = await asyncio.wait_for(fetch_primary(), timeout=timeout)
        if result and (not isinstance(result, dict) or "error" not in result):
            return result
    except Exception as e:
        print(f"[FALLBACK] {category} primary failed: {e}")

    if fetch_secondary:
        try:
            result = await asyncio.wait_for(fetch_secondary(), timeout=timeout)
            if result:
                print(f"[FALLBACK] {category} secondary succeeded")
                return result
        except Exception as e:
            print(f"[FALLBACK] {category} secondary also failed: {e}")

    return {}


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


def _log_provider_stats(phase: str, stats: dict):
    for provider, s in stats.items():
        if s["attempted"] > 0:
            print(f"[BEST_TRADES] [{phase}] {provider}: {s['attempted']} attempted, {s['success']} ok, {s['auth_fail']} auth_fail, {s['rate_limit']} rate_limit, {s['timeout']} timeout, {s['error']} error")


class MarketDataService:
    """
    Unified interface for all market data.
    Your agent talks to THIS — never directly to Polygon or scrapers.
    """

    def __init__(self, polygon_key: str, fmp_key: str = None, coingecko_key: str = None, cmc_key: str = None, altfins_key: str = None, xai_key: str = None, twelvedata_key: str = None):
        self.polygon = PolygonProvider(polygon_key)
        td_key = twelvedata_key or TWELVEDATA_API_KEY
        self.twelvedata = TwelveDataProvider(td_key) if td_key else None
        if self.twelvedata:
            print("[INIT] TwelveData candle provider initialized (8/min)")
        else:
            print("[INIT] TwelveData provider SKIPPED (no TWELVEDATA_API_KEY)")
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

    async def get_candles(self, symbol: str, days: int = 120, budget: CandleBudget = None, ttl: int = None) -> list:
        global _last_candle_budget
        symbol = symbol.upper()
        cache_key = f"candles:{symbol}:1d:{days}"
        use_ttl = ttl or CANDLE_TTL
        if budget:
            _last_candle_budget = budget

        cached = cache.get(cache_key)
        if cached is not None:
            if budget:
                budget.record_cache_hit()
            return cached

        if budget and not budget.can_spend():
            budget.record_blocked()
            return []

        if self.twelvedata and not _is_twelvedata_disabled():
            try:
                td_bars = await asyncio.wait_for(
                    asyncio.to_thread(self.twelvedata.get_daily_bars, symbol, days),
                    timeout=12.0,
                )
                if isinstance(td_bars, dict) and td_bars.get("error"):
                    if td_bars["error"] == "auth":
                        _disable_twelvedata()
                    elif td_bars["error"] == "rate_limited":
                        if budget:
                            budget.record_twelvedata_rate_limited()
                        print(f"[CANDLES] TwelveData {symbol} rate limited, falling through to Finnhub/Polygon")
                elif td_bars and len(td_bars) >= 20:
                    if budget:
                        budget.spend("twelvedata")
                    cache.set(cache_key, td_bars, use_ttl)
                    print(f"[CANDLES] TwelveData {symbol} OK ({len(td_bars)} bars)")
                    return td_bars
            except asyncio.TimeoutError:
                print(f"[CANDLES] TwelveData {symbol} timeout")
            except Exception as e:
                print(f"[CANDLES] TwelveData {symbol} error: {e}")

        if not _is_finnhub_candles_disabled():
            try:
                result = await asyncio.wait_for(
                    asyncio.to_thread(self.finnhub.get_stock_candles, symbol, days),
                    timeout=10.0,
                )
                if result and len(result) >= 20:
                    cache.set(cache_key, result, use_ttl)
                    return result
                elif not result:
                    _disable_finnhub_candles()
            except Exception as e:
                err_str = str(e)
                if "403" in err_str or "401" in err_str:
                    _disable_finnhub_candles()
                else:
                    print(f"[CANDLES] Finnhub {symbol} error: {e}")
        else:
            if budget:
                budget.record_finnhub_blocked()

        try:
            if budget:
                budget.spend("polygon")
            poly_bars = await asyncio.wait_for(
                asyncio.to_thread(self.polygon.get_daily_bars, symbol, days),
                timeout=10.0,
            )
            if poly_bars and len(poly_bars) >= 20:
                bars = [{"o": b.get("o"), "h": b.get("h"), "l": b.get("l"),
                         "c": b.get("c"), "v": b.get("v", 0), "t": b.get("t")}
                        for b in poly_bars]
                cache.set(cache_key, bars, use_ttl)
                return bars
        except asyncio.TimeoutError:
            print(f"[CANDLES] Polygon {symbol} timeout")
        except Exception as e:
            print(f"[CANDLES] Polygon {symbol} error: {e}")

        return []

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
        Uses tiered data sources: Finnhub primary, FMP/Polygon secondary.
        """
        ticker = ticker.upper()

        finnhub_quote = await fetch_with_fallback(
            "equity_price",
            lambda: asyncio.to_thread(self.finnhub.get_quote, ticker),
            timeout=3.0,
        )
        finnhub_profile = await fetch_with_fallback(
            "company_profile",
            lambda: asyncio.to_thread(self.finnhub.get_company_profile, ticker),
            timeout=3.0,
        )

        snapshot_compat = {}
        if finnhub_quote:
            snapshot_compat = {
                "price": finnhub_quote.get("price"),
                "change": finnhub_quote.get("change"),
                "change_pct": finnhub_quote.get("change_pct"),
                "day_high": finnhub_quote.get("high"),
                "day_low": finnhub_quote.get("low"),
                "prev_close": finnhub_quote.get("prev_close"),
            }
        details_compat = {}
        if finnhub_profile:
            details_compat = {
                "name": finnhub_profile.get("name"),
                "sector": finnhub_profile.get("sector"),
                "industry": finnhub_profile.get("industry"),
                "market_cap": finnhub_profile.get("market_cap"),
            }

        technicals = self.finnhub.get_technicals(ticker)
        if not technicals:
            technicals = self.polygon.get_technicals(ticker)

        sync_data = {
            "quote": finnhub_quote,
            "company_profile": finnhub_profile,
            "snapshot": snapshot_compat,
            "details": details_compat,
            "technicals": technicals,
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
        Cached at MACRO_TTL (10 min).
        """
        cached_macro = cache.get("macro_overview_full")
        if cached_macro is not None:
            return cached_macro

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

        macro_snapshot = await self._build_macro_snapshot()

        result = {
            "macro_snapshot": macro_snapshot,
            "fred_economic_data": fred_macro,
            "market_data": fmp_data,
            "fear_greed_index": fear_greed,
        }
        cache.set("macro_overview_full", result, MACRO_TTL)
        return result

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
        5. Deep enrich survivors with fundamentals (capped by budget)
        """
        import time
        scan_start = time.time()
        budget = BudgetTracker.for_preset(category)
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
            if not budget.can_continue():
                print(f"[Wide Scan] Budget exhausted during light enrichment at ticker {i}/{len(ticker_list)} ({budget.status()})")
                budget.mark_exhausted("light_enrichment")
                break
            try:
                result = await asyncio.wait_for(light_enrich(ticker), timeout=6.0)
                budget.tick("light_enrich")
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

        if budget.allow_deep_dive:
            deep_tickers = [td["ticker"] for td in clean[:min(8, MAX_TICKERS_DEEP_DIVE)]]
        else:
            deep_tickers = []
            print(f"[Wide Scan] Deep dive skipped — budget preset disables deep enrichment for {category}")

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
            if not budget.can_continue():
                print(f"[Wide Scan] Budget exhausted during deep enrichment at ticker {i}/{len(deep_tickers)} ({budget.status()})")
                budget.mark_exhausted("deep_enrichment")
                break
            try:
                result = await asyncio.wait_for(deep_enrich(ticker), timeout=8.0)
                budget.tick("deep_enrich")
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

        print(f"[Wide Scan] Complete: {len(enriched_candidates)} candidates ({len(flagged)} flagged) ({time.time()-scan_start:.1f}s) Budget: {budget.status()}")

        scan_result = {
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
        scan_result.update(budget.degradation_metadata())
        return scan_result

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

    async def get_best_trades_scan(self) -> dict:
        """
        TA-first scanner for best trade setups.
        Three-phase pipeline:
          Phase 1: Candidate discovery (cheap Finviz screens) + rank/shortlist
          Phase 2: OHLCV fetch for shortlist with concurrency control
          Phase 3: TA signal engine scores + deterministic trade plans
        """
        import time
        from core.ta_signal_engine import analyze_bars
        scan_start = time.time()

        candle_budget = CandleBudget(max_calls=8)

        new_highs_task = self.finviz.get_new_highs()
        unusual_vol_task = self.finviz.get_unusual_volume()
        gainers_task = self.finviz.get_screener_results("ta_topgainers")
        breakout_task = self.finviz._custom_screen(
            "v=111&f=sh_avgvol_o300,ta_sma200_pa,ta_rsi_nos,sh_price_o5&ft=4&o=-change"
        )
        volume_task = self.finviz._custom_screen(
            "v=111&f=sh_avgvol_o200,sh_relvol_o2,ta_sma50_pa,sh_price_o3&ft=4&o=-volume"
        )

        results = await asyncio.gather(
            new_highs_task, unusual_vol_task, gainers_task, breakout_task, volume_task,
            return_exceptions=True,
        )
        new_highs = results[0] if not isinstance(results[0], Exception) else []
        unusual_vol = results[1] if not isinstance(results[1], Exception) else []
        gainers = results[2] if not isinstance(results[2], Exception) else []
        breakout = results[3] if not isinstance(results[3], Exception) else []
        vol_screen = results[4] if not isinstance(results[4], Exception) else []

        ticker_sources = {}
        for src_name, src_list in [("new_high", new_highs), ("unusual_vol", unusual_vol),
                                     ("gainer", gainers), ("breakout", breakout), ("vol_screen", vol_screen)]:
            if not isinstance(src_list, list):
                continue
            for item in src_list:
                if isinstance(item, dict) and item.get("ticker"):
                    t = item["ticker"].upper().strip()
                    if ".X" not in t and ".U" not in t and len(t) <= 5 and t.isalpha():
                        if t not in ticker_sources:
                            ticker_sources[t] = {"sources": [], "finviz": item}
                        ticker_sources[t]["sources"].append(src_name)

        print(f"[BEST_TRADES] Phase 1: {len(ticker_sources)} unique candidates from {len(new_highs)} highs, {len(unusual_vol)} vol, {len(gainers)} gainers, {len(breakout)} breakout, {len(vol_screen)} vol_screen")

        def _pre_rank_score(ticker: str) -> int:
            info = ticker_sources.get(ticker, {})
            src = info.get("sources", [])
            score = len(src) * 10
            if "new_high" in src:
                score += 15
            if "breakout" in src:
                score += 12
            if "unusual_vol" in src:
                score += 10
            if "gainer" in src:
                score += 8
            return score

        ranked_tickers = sorted(ticker_sources.keys(), key=_pre_rank_score, reverse=True)
        shortlist = ranked_tickers[:25]
        candle_targets = shortlist[:12]
        print(f"[BEST_TRADES] Phase 1: Shortlisted {len(shortlist)}, fetching candles for top {len(candle_targets)}")

        ohlc_results = {}
        no_ta_tickers = []
        ohlc_semaphore = asyncio.Semaphore(3)

        async def _fetch_ohlc_for_ticker(ticker):
            async with ohlc_semaphore:
                bars = await self.get_candles(ticker, days=120, budget=candle_budget)
                if bars and len(bars) >= 20:
                    ohlc_results[ticker] = bars
                else:
                    no_ta_tickers.append(ticker)

        fetch_tasks = [_fetch_ohlc_for_ticker(t) for t in candle_targets]
        await asyncio.gather(*fetch_tasks, return_exceptions=True)

        print(f"[BEST_TRADES] Phase 2: OHLCV {len(ohlc_results)}/{len(candle_targets)} fetched, {len(no_ta_tickers)} missing | {candle_budget.summary()}")

        if len(ohlc_results) < 6 and candle_budget.can_spend():
            already_fetched = set(ohlc_results.keys()) | set(no_ta_tickers)
            retry_targets = [t for t in shortlist if t not in already_fetched][:6]
            if retry_targets:
                print(f"[BEST_TRADES] Phase 2b: Broadening — fetching {len(retry_targets)} additional candles")
                retry_tasks = [_fetch_ohlc_for_ticker(t) for t in retry_targets]
                await asyncio.gather(*retry_tasks, return_exceptions=True)
                print(f"[BEST_TRADES] Phase 2b: After retry OHLCV {len(ohlc_results)} total | {candle_budget.summary()}")

        all_candidates = []
        for ticker, bars in ohlc_results.items():
            try:
                source_info = ticker_sources.get(ticker, {})
                candidate = analyze_bars(
                    bars=bars,
                    ticker=ticker,
                    finviz_data=source_info.get("finviz", {}),
                    source_list=source_info.get("sources", []),
                )
                if candidate:
                    bull_signals = [s for s in candidate.get("ta_signals", []) if s["direction"] == "bullish"]
                    if len(bull_signals) >= 2 or candidate.get("is_bearish"):
                        all_candidates.append(candidate)
                    else:
                        print(f"[BEST_TRADES] {ticker}: filtered (only {len(bull_signals)} bullish signals)")
            except Exception as e:
                print(f"[BEST_TRADES] Analysis error for {ticker}: {e}")

        bullish = sorted([c for c in all_candidates if not c.get("is_bearish")],
                         key=lambda x: (x["technical_score"], x["confidence_score"]), reverse=True)
        bearish_list = sorted([c for c in all_candidates if c.get("is_bearish") and c["technical_score"] >= 70],
                              key=lambda x: x["confidence_score"], reverse=True)[:2]

        top_trades = bullish[:10]

        if top_trades:
            enrich_tasks = [self._enrich_trade_candidate(c) for c in top_trades]
            enriched = await asyncio.gather(*enrich_tasks, return_exceptions=True)
            for i, result in enumerate(enriched):
                if isinstance(result, dict):
                    top_trades[i] = result

        ta_qualified = len(all_candidates)
        top5_debug = []
        for c in (top_trades + bearish_list)[:5]:
            top5_debug.append(f"{c['ticker']}(ta={c['technical_score']},sigs={','.join(c['signals_stacking'][:3])})")

        top_tickers = [c["ticker"] for c in (top_trades + bearish_list)[:10]]
        print(f"[BEST_TRADES] candidates={len(ticker_sources)} shortlist={len(shortlist)} candles_ok={len(ohlc_results)} blocked={candle_budget._blocked} cache_hits={candle_budget._cache_hits} top={top_tickers}")

        for c in top_trades + bearish_list:
            c.pop("is_bearish", None)
            c.pop("ta_signals", None)

        macro = await self._build_macro_snapshot()

        elapsed = time.time() - scan_start
        finnhub_disabled = _is_finnhub_candles_disabled()
        budget_exhausted = not candle_budget.can_spend()

        data_health = {
            "candles_source": "twelvedata" if candle_budget._twelvedata_used > 0 else ("polygon" if candle_budget._polygon_used > 0 else ("cache" if candle_budget._cache_hits > 0 else "none")),
            "finnhub_circuit_breaker": finnhub_disabled,
            "twelvedata_circuit_breaker": _is_twelvedata_disabled(),
            "budget_exhausted": budget_exhausted,
            "candle_stats": candle_budget.summary(),
        }

        if len(top_trades) == 0 and len(bearish_list) == 0:
            reasons = []
            if budget_exhausted:
                reasons.append("Candle budget exhausted")
            if len(no_ta_tickers) > 0:
                reasons.append(f"OHLCV unavailable for {len(no_ta_tickers)} tickers")
            data_health["empty_reason"] = "; ".join(reasons) if reasons else "No qualifying setups found"

        print(f"[BEST_TRADES] Done: candidates={len(all_candidates)} selected={len(top_trades)} bearish={len(bearish_list)} no_ta={len(no_ta_tickers)} ({elapsed:.1f}s)")

        return {
            "scan_type": "best_trades",
            "display_type": "trades",
            "market_pulse": macro,
            "top_trades": top_trades,
            "bearish_setups": bearish_list,
            "scan_stats": {
                "candidates_total": len(ticker_sources),
                "shortlisted": len(shortlist),
                "candle_targets": len(candle_targets),
                "candles_ok": len(ohlc_results),
                "candles_blocked": candle_budget._blocked,
                "cache_hits": candle_budget._cache_hits,
                "ta_qualified": ta_qualified,
                "no_ta": len(no_ta_tickers),
                "elapsed_s": round(elapsed, 1),
            },
            "data_health": data_health,
        }

    async def _enrich_trade_candidate(self, candidate: dict) -> dict:
        ticker = candidate.get("ticker", "")
        try:
            overview = await asyncio.wait_for(
                self.stockanalysis.get_overview(ticker), timeout=5.0
            )
        except Exception:
            overview = None

        if overview and isinstance(overview, dict):
            pe = overview.get("pe_ratio")
            if pe and isinstance(pe, (int, float)) and pe < 0:
                candidate["confirmations"]["fa"] = False
            exchange = overview.get("exchange", "")
            if exchange:
                candidate["exchange"] = exchange
                candidate["tv_url"] = f"https://www.tradingview.com/chart/?symbol={exchange}:{ticker}"
            if not candidate.get("name"):
                candidate["name"] = overview.get("name", "")
            if not candidate.get("market_cap") or candidate["market_cap"] == "":
                candidate["market_cap"] = overview.get("market_cap", "")
        else:
            candidate["data_gaps"].append("fundamentals")
        return candidate

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

    async def _build_macro_snapshot(self) -> dict:
        """
        Lightweight macro snapshot for daily briefing key_numbers.
        Fetches SPY/QQQ/IWM/GLD/USO via Finnhub quotes,
        VIX/10Y via FRED, DXY via FMP. Cached 90s.
        Runs BEFORE heavy scans and is NOT subject to BudgetTracker.
        """
        cached = cache.get("macro_snapshot_v1")
        if cached is not None:
            print("[MACRO_SNAPSHOT] cache hit")
            return cached

        import asyncio

        async def _finnhub_quote(symbol: str) -> tuple:
            try:
                q = await asyncio.to_thread(self.finnhub.get_quote, symbol)
                if q and q.get("price"):
                    chg = q.get("change")
                    chg_pct = q.get("change_pct")
                    if chg is not None and chg > 0:
                        trend = "↑"
                    elif chg is not None and chg < 0:
                        trend = "↓"
                    else:
                        trend = "→"
                    return (symbol, {
                        "price": round(q["price"], 2),
                        "change": f"{chg:+.2f}" if chg is not None else "N/A",
                        "change_pct": f"{chg_pct:+.2f}%" if chg_pct is not None else "",
                        "trend": trend,
                    })
            except Exception as e:
                print(f"[MACRO_SNAPSHOT] Finnhub {symbol} error: {e}")
            return (symbol, None)

        async def _fred_vix() -> dict:
            try:
                vix = await asyncio.to_thread(self.fred.get_vix)
                if isinstance(vix, dict) and "current_vix" in vix:
                    level = vix["current_vix"]
                    trend_data = vix.get("trend", [])
                    delta = None
                    if len(trend_data) >= 2:
                        delta = round(trend_data[-1].get("vix", 0) - trend_data[-2].get("vix", 0), 2)
                    if delta is not None and delta > 0:
                        trend = "↑"
                    elif delta is not None and delta < 0:
                        trend = "↓"
                    else:
                        trend = "→"
                    return {
                        "price": level,
                        "change": f"{delta:+.2f}" if delta is not None else "N/A",
                        "trend": trend,
                        "signal": vix.get("signal", ""),
                    }
            except Exception as e:
                print(f"[MACRO_SNAPSHOT] FRED VIX error: {e}")
            return None

        async def _fred_10y() -> dict:
            try:
                y10 = await asyncio.to_thread(self.fred.get_ten_year_yield)
                if isinstance(y10, dict) and "current_yield" in y10:
                    level = y10["current_yield"]
                    trend_data = y10.get("trend", [])
                    delta = None
                    if len(trend_data) >= 2:
                        delta = round(trend_data[-1].get("yield", 0) - trend_data[-2].get("yield", 0), 2)
                    if delta is not None and delta > 0:
                        trend = "↑"
                    elif delta is not None and delta < 0:
                        trend = "↓"
                    else:
                        trend = "→"
                    return {
                        "yield": f"{level:.2f}%",
                        "change": f"{delta:+.2f}" if delta is not None else "N/A",
                        "trend": trend,
                    }
            except Exception as e:
                print(f"[MACRO_SNAPSHOT] FRED 10Y error: {e}")
            return None

        async def _fmp_dxy() -> dict:
            try:
                if self.fmp:
                    dxy = await asyncio.wait_for(self.fmp.get_dxy(), timeout=8.0)
                    if isinstance(dxy, dict) and dxy.get("price"):
                        chg = dxy.get("change")
                        if chg is not None and chg > 0:
                            trend = "↑"
                        elif chg is not None and chg < 0:
                            trend = "↓"
                        else:
                            trend = "→"
                        return {
                            "price": round(dxy["price"], 2),
                            "change": f"{chg:+.2f}" if chg is not None else "N/A",
                            "trend": trend,
                        }
            except Exception as e:
                print(f"[MACRO_SNAPSHOT] FMP DXY error: {e}")
            try:
                sym, q = await _finnhub_quote("UUP")
                if q:
                    q["note"] = "UUP proxy"
                    return q
            except Exception as e:
                print(f"[MACRO_SNAPSHOT] Finnhub UUP fallback error: {e}")
            return None

        quote_symbols = ["SPY", "QQQ", "IWM", "GLD", "USO"]
        quote_tasks = [_finnhub_quote(s) for s in quote_symbols]
        all_results = await asyncio.gather(
            *quote_tasks, _fred_vix(), _fred_10y(), _fmp_dxy(),
            return_exceptions=True,
        )

        quotes = {}
        for r in all_results[:len(quote_symbols)]:
            if isinstance(r, tuple) and r[1] is not None:
                quotes[r[0]] = r[1]

        vix_result = all_results[len(quote_symbols)]
        ten_y_result = all_results[len(quote_symbols) + 1]
        dxy_result = all_results[len(quote_symbols) + 2]
        if isinstance(vix_result, Exception):
            vix_result = None
        if isinstance(ten_y_result, Exception):
            ten_y_result = None
        if isinstance(dxy_result, Exception):
            dxy_result = None

        def _na():
            return {"price": "N/A", "change": "N/A", "trend": "→"}

        snapshot = {
            "spy": quotes.get("SPY") or _na(),
            "qqq": quotes.get("QQQ") or _na(),
            "iwm": quotes.get("IWM") or _na(),
            "vix": vix_result or _na(),
            "dxy": dxy_result or _na(),
            "ten_year": ten_y_result or {"yield": "N/A", "change": "N/A", "trend": "→"},
            "oil": quotes.get("USO") or _na(),
            "gold": quotes.get("GLD") or _na(),
        }

        filled = [k for k, v in snapshot.items() if v.get("price", v.get("yield", "N/A")) != "N/A"]
        missing = [k for k, v in snapshot.items() if v.get("price", v.get("yield", "N/A")) == "N/A"]
        print(f"[MACRO_SNAPSHOT] filled={','.join(filled)} missing={','.join(missing) if missing else 'none'}")

        cache.set("macro_snapshot_v1", snapshot, 90)
        return snapshot

    def _compute_signal_highlights(
        self, screener_sources, raw_screener_data, enriched,
        stage2_breakouts, macd_crossovers, new_highs, volume_breakouts, unusual_volume,
    ) -> dict:
        def _parse_float(val):
            if val is None:
                return None
            s = str(val).replace(",", "").replace("%", "").strip()
            try:
                return float(s)
            except (ValueError, TypeError):
                return None

        ta_sources = {"stage2_breakout", "macd_crossover", "new_high", "accumulation", "rsi_recovery"}

        candidates_ta = []
        for ticker, sources in screener_sources.items():
            ta_signals = [s for s in sources if s in ta_sources]
            raw = raw_screener_data.get(ticker, {})
            enr = enriched.get(ticker, {})
            trade_score = enr.get("trade_score", 0) or 0

            is_stage2 = "stage2_breakout" in sources
            has_macd = "macd_crossover" in sources
            is_new_high = "new_high" in sources

            ta_score = trade_score
            if not ta_score:
                ta_score = len(ta_signals) * 25

            signal_parts = []
            if is_stage2:
                signal_parts.append("Stage 2 breakout")
            if has_macd:
                signal_parts.append("MACD crossover")
            if is_new_high:
                signal_parts.append("52W high")
            if "accumulation" in sources:
                signal_parts.append("accumulation")
            if "rsi_recovery" in sources:
                signal_parts.append("RSI recovery")

            tier = "C"
            if ta_score >= 70 or is_stage2 or (has_macd and is_new_high):
                tier = "A"
            elif ta_score >= 60:
                tier = "B"

            if ta_signals or ta_score > 0:
                candidates_ta.append({
                    "ticker": ticker,
                    "ta_score": ta_score,
                    "tier": tier,
                    "signal_parts": signal_parts,
                    "is_stage2": is_stage2,
                })

        best_ta = {"ticker": "N/A", "signal": "N/A"}
        best_ta_score = 0
        if candidates_ta:
            tier_a = [c for c in candidates_ta if c["tier"] == "A"]
            tier_b = [c for c in candidates_ta if c["tier"] == "B"]
            tier_c = [c for c in candidates_ta if c["tier"] == "C"]

            pool = tier_a or tier_b or tier_c
            winner = max(pool, key=lambda c: c["ta_score"])
            best_ta_score = winner["ta_score"]

            if winner["signal_parts"]:
                sig = " + ".join(winner["signal_parts"][:3])
            elif winner["ta_score"] >= 60:
                sig = "Strong trend (price > key MAs) despite low breakout count"
            else:
                sig = "Best relative strength in scan (no clean breakouts today)"

            best_ta = {"ticker": winner["ticker"], "signal": sig}

        vol_source_min_rvol = {
            "volume_breakout": 3.0,
            "unusual_volume": 2.0,
            "stage2_breakout": 2.0,
            "accumulation": 1.5,
        }

        candidates_vol = []
        for ticker, sources in screener_sources.items():
            raw = raw_screener_data.get(ticker, {})
            enr = enriched.get(ticker, {})
            overview = enr.get("overview", {}) if enr else {}

            rvol = _parse_float(raw.get("rel_volume"))
            raw_vol_str = raw.get("volume")
            raw_vol = _parse_float(raw_vol_str)
            avg_vol = _parse_float(overview.get("avg_volume")) if overview else None
            if avg_vol is None:
                avg_vol = _parse_float(raw.get("avg_volume"))

            if not rvol:
                for src, min_rv in vol_source_min_rvol.items():
                    if src in sources:
                        rvol = max(rvol or 0, min_rv)

            vol_pct = None
            vol_source = None

            if rvol and rvol > 1:
                vol_pct = (rvol - 1) * 100
                vol_source = "rvol"
            elif raw_vol and avg_vol and avg_vol > 0:
                vol_pct = ((raw_vol / avg_vol) - 1) * 100
                vol_source = "computed"

            if vol_pct is not None and vol_pct > 0:
                candidates_vol.append({
                    "ticker": ticker,
                    "vol_pct": vol_pct,
                    "rvol": rvol,
                    "source": vol_source,
                })
            elif raw_vol and raw_vol > 0:
                candidates_vol.append({
                    "ticker": ticker,
                    "vol_pct": 0,
                    "rvol": None,
                    "source": "raw_only",
                    "raw_vol": raw_vol,
                })

        biggest_vol = {"ticker": "N/A", "signal": "N/A"}
        biggest_vol_pct = "NA"
        if candidates_vol:
            real_vol = [c for c in candidates_vol if c["source"] != "raw_only" and c["vol_pct"] > 0]
            if real_vol:
                winner = max(real_vol, key=lambda c: c["vol_pct"])
            else:
                winner = max(candidates_vol, key=lambda c: c.get("raw_vol", 0))

            pct = winner["vol_pct"]
            biggest_vol_pct = f"{pct:.0f}%"
            rvol_val = winner.get("rvol")

            if pct > 0 and rvol_val:
                sig = f"Volume +{pct:.0f}% vs avg ({rvol_val:.1f}x relative volume)"
            elif pct > 0:
                sig = f"Volume +{pct:.0f}% vs avg"
            elif rvol_val:
                sig = f"Volume +{(rvol_val-1)*100:.0f}% vs avg ({rvol_val:.1f}x RVOL)"
            else:
                raw_v = winner.get("raw_vol", 0)
                if raw_v >= 1_000_000:
                    sig = f"{raw_v/1_000_000:.1f}M shares (volume data limited — avg unavailable)"
                else:
                    sig = f"{raw_v:,.0f} shares (volume data limited — avg unavailable)"

            biggest_vol = {"ticker": winner["ticker"], "signal": sig}

        print(f"[HIGHLIGHTS] best_ta={best_ta['ticker']} biggest_vol={biggest_vol['ticker']} vol_pct={biggest_vol_pct} ta_score={best_ta_score}")

        return {
            "best_ta_setup": best_ta,
            "biggest_volume": biggest_vol,
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

        macro_snapshot = await self._build_macro_snapshot()

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
        raw_screener_data = {}

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
                            if t not in raw_screener_data:
                                raw_screener_data[t] = item
                            else:
                                for k, v in item.items():
                                    if k != "ticker" and v and not raw_screener_data[t].get(k):
                                        raw_screener_data[t][k] = v

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

        pre_computed_highlights = self._compute_signal_highlights(
            screener_sources, raw_screener_data, enriched,
            stage2_breakouts, macd_crossovers, new_highs, volume_breakouts, unusual_volume,
        )

        return {
            "pre_computed_highlights": pre_computed_highlights,
            "macro_snapshot": macro_snapshot,
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
        Hybrid Grok+Claude trending architecture:
        Phase 1: Grok searches X (PRIMARY discovery) in parallel with StockTwits/Reddit/Yahoo/Finviz
        Phase 2: Merge tickers with Grok priority boost, rank by cross-platform presence
        Phase 3: Enrich top 12 with StockAnalysis fundamentals
        Phase 4: Package Grok's full X analysis + social data + FA for Claude to validate
        """
        from data.scoring_engine import score_for_trades, passes_market_cap_filter

        xai_task = None
        if self.xai:
            xai_task = asyncio.create_task(
                asyncio.wait_for(self.xai.get_trending_tickers("stock"), timeout=35.0)
            )

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

        xai_trending = {}
        xai_top_picks = []
        xai_market_mood = "unknown"
        if xai_task:
            try:
                xai_trending = await xai_task
                xai_tickers_raw = xai_trending.get("trending_tickers", [])
                print(f"[TRENDING] xAI Grok returned {len(xai_tickers_raw)} trending tickers from X")
                xai_market_mood = xai_trending.get("market_mood", "unknown")
                for item in xai_tickers_raw:
                    t = item.get("ticker", "").upper().strip()
                    if t and len(t) <= 6:
                        xai_top_picks.append({
                            "ticker": t,
                            "x_sentiment": item.get("sentiment", "unknown"),
                            "x_sentiment_score": item.get("sentiment_score", 0),
                            "x_why_trending": item.get("why_trending", ""),
                            "x_catalyst": item.get("catalyst", ""),
                            "x_mention_intensity": item.get("mention_intensity", "medium"),
                            "x_trade_sentiment": item.get("trade_sentiment", "hold"),
                            "x_risk_flag": item.get("risk_flag"),
                            "x_narratives": item.get("key_narratives", []),
                        })
            except Exception as e:
                print(f"[TRENDING] xAI Grok trending failed: {e}")

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

        for pick in xai_top_picks:
            t = pick["ticker"]
            if t not in ticker_sources:
                ticker_sources[t] = set()
            ticker_sources[t].add("X_Twitter")

        xai_ticker_set = {p["ticker"] for p in xai_top_picks}

        def sort_key(item):
            t, srcs = item
            source_count = len(srcs)
            xai_boost = 2 if t in xai_ticker_set else 0
            return source_count + xai_boost

        ranked = sorted(ticker_sources.items(), key=sort_key, reverse=True)
        top_tickers = [t for t, _ in ranked[:12]]

        print(f"[Trending] {len(ticker_sources)} unique tickers across all platforms")
        xai_in_top = len([t for t in top_tickers if t in xai_ticker_set])
        print(f"[Trending] Top 12 selected ({xai_in_top} from X): {top_tickers}")

        async def full_enrich(ticker):
            try:
                st_result, overview, analyst, profile = await asyncio.gather(
                    self.stocktwits.get_sentiment(ticker),
                    self.stockanalysis.get_overview(ticker),
                    self.stockanalysis.get_analyst_ratings(ticker),
                    asyncio.to_thread(lambda t=ticker: self.finnhub.get_company_profile(t)),
                    return_exceptions=True,
                )
                ov = overview if not isinstance(overview, Exception) and isinstance(overview, dict) else {}
                prof = profile if not isinstance(profile, Exception) and isinstance(profile, dict) else {}
                if prof and not ov.get("sector"):
                    ov["sector"] = prof.get("finnhubIndustry") or prof.get("sector") or ""
                    ov["industry"] = prof.get("finnhubIndustry") or ""
                    if not ov.get("company_name"):
                        ov["company_name"] = prof.get("name") or ""
                return {
                    "stocktwits_sentiment": st_result if not isinstance(st_result, Exception) else {},
                    "overview": ov,
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
                enriched[ticker] = {"overview": {}, "stocktwits_sentiment": {}, "analyst_ratings": {}}
                continue

            quant_score = score_for_trades(result)
            result["quant_score"] = quant_score
            result["trending_sources"] = list(ticker_sources.get(ticker, []))
            result["source_count"] = len(ticker_sources.get(ticker, []))
            xai_pick = next((p for p in xai_top_picks if p["ticker"] == ticker), None)
            if xai_pick:
                result["x_analysis"] = xai_pick
            enriched[ticker] = result

        from data.microcap_scorer import score_trending_tickers
        microcap_results = score_trending_tickers(enriched, xai_top_picks, ticker_sources)

        microcap_scores = {}
        for bucket in ["asymmetric_opportunities", "institutional_plays", "rejected"]:
            for r in microcap_results[bucket]:
                microcap_scores[r["ticker"]] = r

        for ticker, data in enriched.items():
            if ticker in microcap_scores:
                data["microcap_analysis"] = microcap_scores[ticker]

        asymmetric_count = len(microcap_results["asymmetric_opportunities"])
        institutional_count = len(microcap_results["institutional_plays"])
        rejected_count = len(microcap_results["rejected"])
        power_law_tickers = [r["ticker"] for r in microcap_results["power_law_candidates"]]
        print(f"[Trending] Two-tier scoring: {asymmetric_count} asymmetric, {institutional_count} institutional, {rejected_count} rejected, power_law={power_law_tickers}")

        sorted_tickers = sorted(
            enriched.items(),
            key=lambda x: (
                1 if x[0] in xai_ticker_set else 0,
                x[1].get("source_count", 0),
                x[1].get("microcap_analysis", {}).get("microcap_score") or x[1].get("quant_score", 0),
            ),
            reverse=True,
        )

        sorted_enriched = {}
        for t, d in sorted_tickers:
            ov = d.get("overview", {})
            slim = {
                "market_cap": ov.get("market_cap", ""),
                "revenue": ov.get("revenue", ""),
                "revenue_growth": ov.get("revenue_growth", ""),
                "eps": ov.get("eps", ""),
                "pe_ratio": ov.get("pe_ratio", ""),
                "forward_pe": ov.get("forward_pe", ""),
                "analyst_rating": ov.get("analyst_rating", ""),
                "price_target": ov.get("price_target", ""),
                "upside_downside": ov.get("upside_downside", ""),
                "earnings_date": ov.get("earnings_date", ""),
                "sector": ov.get("sector", ""),
                "industry": ov.get("industry", ""),
                "week_52_range": ov.get("week_52_range", ""),
                "beta": ov.get("beta", ""),
                "avg_volume": ov.get("avg_volume", ""),
                "company_name": ov.get("company_name", ""),
            }
            slim = {k: v for k, v in slim.items() if v}

            st = d.get("stocktwits_sentiment", {})
            if st:
                slim["social_sentiment"] = {
                    "sentiment": st.get("sentiment"),
                    "bullish_pct": st.get("bullish_pct"),
                    "volume_change": st.get("volume_change"),
                }

            ar = d.get("analyst_ratings", {})
            if ar and ar.get("total_analysts"):
                slim["analyst_consensus"] = ar.get("consensus", "")
                slim["analyst_count"] = ar.get("total_analysts", 0)

            slim["quant_score"] = d.get("quant_score", 0)
            slim["sources"] = d.get("trending_sources", [])
            slim["source_count"] = d.get("source_count", 0)

            x = d.get("x_analysis")
            if x:
                slim["x_catalyst"] = x.get("x_catalyst", "")
                slim["x_sentiment"] = x.get("x_sentiment", "")
                slim["x_why_trending"] = x.get("x_why_trending", "")

            sorted_enriched[t] = slim

        scoring_summary = []
        for r in microcap_results["asymmetric_opportunities"]:
            b = r.get("breakdown", {})
            compact = {
                "ticker": r["ticker"],
                "tier": r["tier"],
                "mcap": r.get("mcap_formatted", "?"),
                "score": r["microcap_score"],
                "power_law": r.get("power_law_flag", False),
                "catalyst": b.get("catalyst", {}).get("score", 0),
                "catalyst_signals": " | ".join(b.get("catalyst", {}).get("details", {}).get("signals", [])[:3]),
                "sector": b.get("sector_alignment", {}).get("score", 0),
                "sector_detail": b.get("sector_alignment", {}).get("details", {}).get("alignment", ""),
                "technical": b.get("early_technical", {}).get("score", 0),
                "social": b.get("social_momentum", {}).get("score", 0),
                "liquidity": b.get("liquidity", {}).get("score", 0),
            }
            scoring_summary.append(compact)

        return {
            "scan_type": "hybrid_trending",
            "two_tier_analysis": {
                "INSTRUCTION": "PRIORITIZE asymmetric small-caps below. Power-law candidates deserve HIGHEST conviction.",
                "asymmetric_opportunities": asymmetric_count,
                "power_law_candidates": power_law_tickers,
                "scoring_summary": scoring_summary,
                "institutional_plays_count": institutional_count,
                "rejected_count": rejected_count,
            },
            "total_unique_tickers": len(ticker_sources),
            "x_market_mood": xai_market_mood,
            "grok_x_analysis": {
                "summary": xai_trending.get("summary", ""),
                "sector_heat": xai_trending.get("sector_heat", []),
                "notable_themes": xai_trending.get("notable_themes", []),
                "contrarian_signals": xai_trending.get("contrarian_signals", []),
                "top_picks": xai_top_picks,
            },
            "source_summary": {
                "X_Twitter": len(xai_top_picks),
                "StockTwits": len(stocktwits_trending),
                "Yahoo Finance": len(yahoo_trending),
                "Reddit": len(reddit_trending),
                "StockAnalysis": len(stockanalysis_trending),
                "Finviz Active": len(finviz_most_active),
                "Finviz Volume": len(finviz_unusual_volume),
                "Finviz Gainers": len(finviz_top_gainers),
            },
            "ranked_tickers": [
                {
                    "ticker": t,
                    "source_count": d.get("source_count", 0),
                    "sources": d.get("trending_sources", []),
                    "quant_score": d.get("quant_score", 0),
                    "on_x": t in xai_ticker_set,
                    "microcap_score": d.get("microcap_analysis", {}).get("microcap_score"),
                    "tier": d.get("microcap_analysis", {}).get("tier", "unknown"),
                }
                for t, d in sorted_tickers[:15]
            ],
            "enriched_data": sorted_enriched,
        }

    async def get_cross_market_scan(self) -> dict:
        """
        Pull data from ALL asset classes in parallel: stocks, crypto, commodities, macro.
        Used when the user asks about trends/opportunities across multiple markets.
        Returns a unified dataset so Claude can rank across asset classes fairly.
        Each sub-scan has its own timeout to prevent one slow source from blocking everything.
        """
        async def _timed(coro, label, timeout=25.0):
            try:
                result = await asyncio.wait_for(coro, timeout=timeout)
                print(f"[CROSS-MARKET] {label} completed")
                return result
            except asyncio.TimeoutError:
                print(f"[CROSS-MARKET] {label} timed out after {timeout}s")
                return {"error": f"{label} timed out"}
            except Exception as e:
                print(f"[CROSS-MARKET] {label} error: {e}")
                return {"error": str(e)}

        stock_task = _timed(self._get_stock_trending_light(), "stocks", 25.0)
        crypto_task = _timed(self._get_crypto_light(), "crypto", 25.0)
        commodity_task = _timed(self._get_commodities_light(), "commodities", 15.0)
        macro_task = _timed(self.get_macro_overview(), "macro", 15.0)

        stock_data, crypto_data, commodity_data, macro_data = await asyncio.gather(
            stock_task, crypto_task, commodity_task, macro_task,
        )

        stock_data = stock_data if stock_data else {"error": "Stock data unavailable"}
        crypto_data = crypto_data if crypto_data else {"error": "Crypto data unavailable"}
        commodity_data = commodity_data if commodity_data else {"error": "Commodity data unavailable"}
        macro_data = macro_data if macro_data else {"error": "Macro data unavailable"}

        from data.cross_asset_ranker import rank_cross_market
        try:
            ranking_result = rank_cross_market(stock_data, crypto_data, commodity_data, macro_data)
        except Exception as e:
            print(f"[CROSS-MARKET] Ranker failed: {e}")
            ranking_result = {"ranked_candidates": [], "ranking_debug": {"error": str(e)}}

        result = {
            "scan_type": "cross_market",
            "instructions": (
                "CROSS-MARKET SCAN — PRE-RANKED DATA. Candidates have been quantitatively scored, "
                "normalized across asset classes, and filtered. The ranking_debug shows WHY each was selected. "
                "Your job: (1) Use the pre-ranked list as your starting point — do NOT re-rank from scratch. "
                "(2) Add qualitative analysis the math can't capture (narrative, timing, regime context). "
                "(3) You may promote or demote candidates by 1-2 positions based on qualitative factors, but "
                "explain why. (4) MUST include at least one stock and one commodity if they appear in ranked list. "
                "(5) If macro regime is risk_off, do NOT recommend speculative small-cap crypto. "
                "(6) Present your final 3-5 picks with conviction level and specific entry thesis."
            ),
            "ranked_candidates": ranking_result.get("ranked_candidates", []),
            "ranking_debug": ranking_result.get("ranking_debug", {}),
            "stock_trending": stock_data,
            "crypto_scanner": crypto_data,
            "commodities": commodity_data,
            "macro_context": macro_data,
        }

        return result

    async def _get_stock_trending_light(self) -> dict:
        """Lighter stock trending for cross-market scan — skip heavy enrichment."""
        from collections import Counter

        stocktwits_trending, yahoo_trending, finviz_most_active, reddit_trending = await asyncio.gather(
            self.stocktwits.get_trending(),
            scrape_yahoo_trending(),
            self.finviz.get_most_active(),
            self.reddit.get_all_stocks_trending(),
            return_exceptions=True,
        )

        if isinstance(stocktwits_trending, Exception): stocktwits_trending = []
        if isinstance(yahoo_trending, Exception): yahoo_trending = []
        if isinstance(finviz_most_active, Exception): finviz_most_active = []
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
        add_tickers(finviz_most_active, "Finviz Active")
        add_tickers(reddit_trending, "Reddit")

        ranked = sorted(ticker_sources.items(), key=lambda x: len(x[1]), reverse=True)
        multi_source = [(t, srcs) for t, srcs in ranked if len(srcs) >= 2]
        top_tickers = [t for t, _ in multi_source[:10]]

        light_enrichment = await asyncio.gather(
            *[self.stockanalysis.get_overview(t) for t in top_tickers[:8]],
            return_exceptions=True,
        )

        enriched = {}
        for ticker, result in zip(top_tickers[:8], light_enrichment):
            if isinstance(result, Exception) or not isinstance(result, dict):
                continue
            result["trending_sources"] = list(ticker_sources.get(ticker, []))
            result["source_count"] = len(ticker_sources.get(ticker, []))
            enriched[ticker] = result

        return {
            "total_unique_tickers": len(ticker_sources),
            "multi_platform_count": len(multi_source),
            "top_trending": [
                {"ticker": t, "source_count": len(s), "sources": list(s)}
                for t, s in multi_source[:15]
            ],
            "enriched_data": enriched,
        }

    async def _get_crypto_light(self) -> dict:
        """Lighter crypto scan for cross-market — skip xAI and altFINS to save time."""
        tasks = {}

        if self.coingecko:
            tasks["cg_dashboard"] = self.coingecko.get_crypto_dashboard()

        if self.cmc:
            tasks["cmc_dashboard"] = self.cmc.get_full_dashboard()

        tasks["hyperliquid"] = self.hyperliquid.get_crypto_dashboard()
        tasks["fear_greed"] = self.fear_greed.get_fear_greed_index()

        task_names = list(tasks.keys())
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)

        output = {}
        for name, result in zip(task_names, results):
            if isinstance(result, Exception):
                print(f"[CROSS-MARKET] crypto {name} failed: {result}")
                output[name] = {"error": str(result)}
            else:
                output[name] = result

        return output

    COMMODITY_UNIVERSE = {
        "oil":          {"proxy": "USO",  "name": "Crude Oil",        "type": "energy"},
        "nat_gas":      {"proxy": "UNG",  "name": "Natural Gas",      "type": "energy"},
        "gold":         {"proxy": "GLD",  "name": "Gold",             "type": "precious_metals"},
        "silver":       {"proxy": "SLV",  "name": "Silver",           "type": "precious_metals"},
        "platinum":     {"proxy": "PPLT", "name": "Platinum",         "type": "precious_metals"},
        "copper":       {"proxy": "CPER", "name": "Copper",           "type": "base_metals"},
        "base_metals":  {"proxy": "DBB",  "name": "Base Metals",      "type": "base_metals"},
        "steel":        {"proxy": "SLX",  "name": "Steel",            "type": "base_metals",   "equity_proxy": "CLF"},
        "aluminum":     {"proxy": "AA",   "name": "Aluminum (Alcoa)", "type": "base_metals"},
        "uranium":      {"proxy": "URA",  "name": "Uranium",          "type": "energy"},
        "uranium_alt":  {"proxy": "URNM", "name": "Uranium Miners",   "type": "energy"},
        "lithium":      {"proxy": "LIT",  "name": "Lithium",          "type": "battery_metals", "equity_proxy": "ALB"},
        "rare_earth":   {"proxy": "REMX", "name": "Rare Earth",       "type": "battery_metals", "equity_proxy": "MP"},
        "wheat":        {"proxy": "WEAT", "name": "Wheat",            "type": "agriculture"},
        "corn":         {"proxy": "CORN", "name": "Corn",             "type": "agriculture"},
        "soybeans":     {"proxy": "SOYB", "name": "Soybeans",         "type": "agriculture"},
        "agriculture":  {"proxy": "DBA",  "name": "Agriculture Basket","type": "agriculture"},
        "carbon":       {"proxy": "KRBN", "name": "Carbon Credits",   "type": "carbon"},
        "energy_eq":    {"proxy": "XLE",  "name": "Energy Sector",    "type": "energy"},
        "gold_miners":  {"proxy": "GDX",  "name": "Gold Miners",      "type": "precious_metals"},
        "jr_gold":      {"proxy": "GDXJ", "name": "Jr Gold Miners",   "type": "precious_metals"},
        "clean_energy": {"proxy": "ICLN", "name": "Clean Energy",     "type": "energy"},
        "timber":       {"proxy": "WOOD", "name": "Timber",            "type": "agriculture"},
    }

    COMMODITY_THEME_KEYWORDS = {
        "oil": ["oil", "crude", "brent", "wti", "petroleum"],
        "nat_gas": ["natural gas", "nat gas", "natgas", "lng"],
        "gold": ["gold", "bullion", "xauusd"],
        "silver": ["silver", "xagusd"],
        "copper": ["copper", "hg futures"],
        "uranium": ["uranium", "nuclear", "u3o8"],
        "lithium": ["lithium", "battery metals", "ev metals"],
        "rare_earth": ["rare earth", "rare earths"],
        "wheat": ["wheat"],
        "corn": ["corn", "maize"],
        "soybeans": ["soybean", "soybeans", "soy"],
        "steel": ["steel", "iron ore"],
        "platinum": ["platinum", "palladium", "pgm"],
        "carbon": ["carbon", "carbon credits", "emissions"],
        "agriculture": ["agriculture", "agri", "soft commodities"],
        "base_metals": ["base metals", "industrial metals"],
        "energy_eq": ["energy sector", "energy stocks"],
        "clean_energy": ["clean energy", "solar", "wind energy", "renewables"],
    }

    MAX_COMMODITY_QUOTES = 20

    async def _get_commodities_light(self, grok_themes: list[str] | None = None) -> dict:
        """
        Commodity universe quote sampling for cross-market trending.
        Fetches quotes for up to MAX_COMMODITY_QUOTES liquid proxies,
        ranks by absolute % change, and selects top movers.
        Grok themes force-include matching proxies.
        """
        from data.cache import cache

        theme_suffix = "_".join(sorted(grok_themes)) if grok_themes else "base"
        CACHE_KEY = f"commodity_universe_quotes:{theme_suffix}"
        CACHE_TTL = 180

        cached = cache.get(CACHE_KEY)
        if cached is not None:
            print(f"[COMMODITIES] Using cached universe quotes ({len(cached.get('commodity_proxies', []))} items) key={theme_suffix}")
            return cached

        grok_themes = grok_themes or []
        force_include_themes = set()
        for theme_key, keywords in self.COMMODITY_THEME_KEYWORDS.items():
            for gt in grok_themes:
                gt_lower = gt.lower()
                if any(kw in gt_lower for kw in keywords):
                    force_include_themes.add(theme_key)

        all_proxies = []
        seen_symbols = set()
        for theme_key, info in self.COMMODITY_UNIVERSE.items():
            sym = info["proxy"]
            if sym not in seen_symbols:
                all_proxies.append({"symbol": sym, "theme": theme_key, **info})
                seen_symbols.add(sym)
            eq = info.get("equity_proxy")
            if eq and eq not in seen_symbols and len(all_proxies) < self.MAX_COMMODITY_QUOTES + 3:
                all_proxies.append({"symbol": eq, "theme": theme_key, "name": f"{info['name']} (equity)", "type": info["type"]})
                seen_symbols.add(eq)

        quote_symbols = [p["symbol"] for p in all_proxies[:self.MAX_COMMODITY_QUOTES]]

        fmp_quotes = {}
        fmp_treasuries = {}
        if self.fmp:
            batch_size = 20
            quote_tasks = []
            for i in range(0, len(quote_symbols), batch_size):
                batch = quote_symbols[i:i+batch_size]
                quote_tasks.append(self.fmp.get_etf_quotes(batch))
            treasury_task = self.fmp.get_treasury_rates()

            results = await asyncio.gather(*quote_tasks, treasury_task, return_exceptions=True)
            for r in results[:-1]:
                if isinstance(r, dict):
                    fmp_quotes.update(r)
            fmp_treasuries = results[-1] if not isinstance(results[-1], Exception) else {}

        fred_macro = self.fred.get_quick_macro()

        commodity_proxies = []
        missing = []
        for p in all_proxies[:self.MAX_COMMODITY_QUOTES]:
            sym = p["symbol"]
            quote = fmp_quotes.get(sym, {})
            if not quote or not quote.get("price"):
                missing.append(sym)
                continue
            commodity_proxies.append({
                "symbol": sym,
                "name": p.get("name", sym),
                "theme": p.get("theme", ""),
                "type": p.get("type", ""),
                "price": quote.get("price"),
                "change": quote.get("change"),
                "change_pct": quote.get("change_pct"),
                "volume": quote.get("volume"),
                "avg_volume": quote.get("avg_volume"),
                "year_high": quote.get("year_high"),
                "year_low": quote.get("year_low"),
                "abs_change_pct": abs(quote.get("change_pct") or 0),
                "grok_theme_match": p.get("theme", "") in force_include_themes,
            })

        commodity_proxies.sort(key=lambda x: (x["grok_theme_match"], x["abs_change_pct"]), reverse=True)

        selected = []
        selected_themes = set()
        for cp in commodity_proxies:
            if cp["grok_theme_match"] and cp["theme"] not in selected_themes:
                selected.append(cp)
                selected_themes.add(cp["theme"])
                if len(selected) >= 4:
                    break
        for cp in commodity_proxies:
            if cp["symbol"] not in {s["symbol"] for s in selected}:
                selected.append(cp)
                if len(selected) >= 4:
                    break

        grok_theme_names = list(force_include_themes) if force_include_themes else []
        print(f"[COMMODITIES] universe={len(all_proxies)} fetched={len(commodity_proxies)} selected={len(selected)} grok_themes={grok_theme_names} missing={missing[:5]}")

        result = {
            "commodity_proxies": selected,
            "all_commodity_quotes": commodity_proxies,
            "commodity_prices": {
                "all_commodities": commodity_proxies,
            },
            "treasury_rates": fmp_treasuries,
            "fred_macro": fred_macro,
            "coverage": {
                "universe_size": len(all_proxies),
                "quotes_fetched": len(commodity_proxies),
                "selected": len(selected),
                "grok_themes": grok_theme_names,
                "missing_symbols": missing[:5],
            },
        }

        cache.set(CACHE_KEY, result, CACHE_TTL)
        return result

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

    async def run_deterministic_screener(self, preset_name: str) -> dict:
        """
        Deterministic screener pipeline for preset buttons.
        Phase A: Finviz discovery (cheap, 50-200 candidates)
        Phase B: Enrichment (quotes, fundamentals, TA from candles) — max 30 or 12s
        Phase C: Filter by screen rules + rank deterministically
        """
        import time as _t
        from screener_definitions import SCREENER_DEFINITIONS
        from data.ta_utils import compute_technicals_from_bars, compute_sma

        definition = SCREENER_DEFINITIONS.get(preset_name)
        if not definition:
            return {
                "display_type": "screener",
                "screen_name": preset_name,
                "error": f"Unknown screener preset: {preset_name}",
                "top_picks": [],
                "rows": [],
            }

        start_time = _t.time()
        ENRICHMENT_LIMIT = 30
        ENRICHMENT_TIMEOUT = 12.0

        print(f"[SCREENER] Starting preset={preset_name} label={definition['screen_label']}")

        # --- Phase A: Finviz discovery ---
        finviz_filter_str = definition["finviz_filters"]
        finviz_sort = definition.get("finviz_sort", "-change")
        screen_url = f"v=111&f={finviz_filter_str}&ft=4&o={finviz_sort}"
        print(f"[SCREENER] Phase A: Finviz URL: {screen_url}")

        try:
            candidates = await self.finviz._custom_screen(screen_url)
        except Exception as e:
            print(f"[SCREENER] Finviz discovery error: {e}")
            candidates = []

        if not candidates:
            fallback_filter = finviz_filter_str.split(",")[0] if "," in finviz_filter_str else finviz_filter_str
            fallback_url = f"v=111&f={fallback_filter},sh_avgvol_o200&ft=4&o={finviz_sort}"
            print(f"[SCREENER] Phase A fallback: {fallback_url}")
            try:
                candidates = await self.finviz._custom_screen(fallback_url)
            except Exception:
                candidates = []

        candidates_total = len(candidates)
        print(f"[SCREENER] Phase A: {candidates_total} candidates found")

        if not candidates:
            return {
                "display_type": "screener",
                "screen_name": definition["screen_label"],
                "preset": preset_name,
                "explain": definition["explain_template"],
                "top_picks": [],
                "rows": [],
                "scan_stats": {"candidates_total": 0, "enriched": 0, "qualified": 0},
                "meta": {"empty_reason": "No candidates matched Finviz screen criteria"},
            }

        to_enrich = candidates[:ENRICHMENT_LIMIT]

        # --- Phase B: Enrichment ---
        candle_budget = CandleBudget(max_calls=8)
        enriched_rows = []
        enrichment_start = _t.time()

        async def _enrich_one(item):
            ticker = item.get("ticker", "").strip()
            if not ticker or len(ticker) > 6:
                return None

            row = {
                "ticker": ticker,
                "company": item.get("company", "").strip() or None,
                "sector": item.get("sector", "").strip() or None,
                "price": None,
                "chg_pct": None,
                "mkt_cap": item.get("market_cap", "").strip() or None,
                "rev_growth_yoy": None,
                "pe": None,
                "div_yield": None,
                "signals": [],
                "missing_fields": [],
            }

            if row["company"] and len(row["company"]) <= 1:
                row["company"] = None

            try:
                quote = await asyncio.to_thread(self.finnhub.get_quote, ticker)
                if quote and quote.get("price"):
                    row["price"] = quote["price"]
                    row["chg_pct"] = quote.get("change_pct")
            except Exception:
                pass

            try:
                overview = await self.stockanalysis.get_overview(ticker)
                if overview and not overview.get("error"):
                    if not row["company"] or len(row["company"] or "") <= 1:
                        sa_name = overview.get("ticker", "")
                        if overview.get("market_cap"):
                            row["company"] = f"{ticker} Corp"
                    pe_raw = overview.get("pe_ratio") or overview.get("forward_pe")
                    if pe_raw:
                        try:
                            row["pe"] = float(str(pe_raw).replace(",", "").replace("x", ""))
                        except (ValueError, TypeError):
                            pass
                    div_raw = overview.get("dividend_yield")
                    if div_raw and div_raw not in ("N/A", "-", "n/a"):
                        try:
                            row["div_yield"] = float(str(div_raw).replace("%", "").replace(",", ""))
                        except (ValueError, TypeError):
                            pass
                    rg_raw = overview.get("revenue_growth")
                    if rg_raw:
                        try:
                            row["rev_growth_yoy"] = float(str(rg_raw).replace("%", "").replace("+", "").replace(",", ""))
                        except (ValueError, TypeError):
                            pass
                    if not row["mkt_cap"] and overview.get("market_cap"):
                        row["mkt_cap"] = overview["market_cap"]
            except Exception as e:
                print(f"[SCREENER] StockAnalysis enrich error {ticker}: {e}")

            if not row["price"]:
                try:
                    price_str = item.get("price", "")
                    if price_str:
                        row["price"] = float(str(price_str).replace(",", ""))
                except (ValueError, TypeError):
                    pass
            if not row["chg_pct"]:
                try:
                    chg_str = item.get("change", "")
                    if chg_str:
                        row["chg_pct"] = float(str(chg_str).replace("%", "").replace("+", ""))
                except (ValueError, TypeError):
                    pass

            ta_data = {}
            if _t.time() - enrichment_start < ENRICHMENT_TIMEOUT:
                try:
                    bars = await self.get_candles(ticker, days=120, budget=candle_budget)
                    if bars and len(bars) >= 20:
                        ta_data = compute_technicals_from_bars(bars)
                        closes = [b["c"] for b in bars if b.get("c") is not None]
                        volumes = [b.get("v", 0) for b in bars]

                        if ta_data.get("rsi") is not None:
                            row["signals"].append(f"RSI {ta_data['rsi']:.0f}")
                        if ta_data.get("sma_50") and row.get("price"):
                            price = row["price"]
                            if price > ta_data["sma_50"]:
                                row["signals"].append("Above SMA50")
                            else:
                                row["signals"].append("Below SMA50")
                        if ta_data.get("sma_200") and row.get("price"):
                            price = row["price"]
                            if price > ta_data["sma_200"]:
                                row["signals"].append("Above SMA200")
                        if ta_data.get("macd_histogram") is not None:
                            if ta_data["macd_histogram"] > 0:
                                row["signals"].append("MACD positive")
                            else:
                                row["signals"].append("MACD negative")
                        if ta_data.get("macd") is not None and ta_data.get("macd_signal") is not None:
                            if ta_data["macd"] > ta_data["macd_signal"]:
                                row["signals"].append("MACD bull cross")
                        avg_vol = ta_data.get("avg_volume") or 0
                        if avg_vol > 0 and volumes:
                            last_vol = volumes[-1]
                            rel_vol = last_vol / avg_vol
                            row["rel_vol"] = round(rel_vol, 1)
                            if rel_vol >= 1.5:
                                row["signals"].append(f"RelVol {rel_vol:.1f}x")

                        if len(closes) >= 50:
                            sma50_prev = compute_sma(closes[:-5], 50)
                            sma50_now = ta_data.get("sma_50")
                            if sma50_prev and sma50_now and sma50_now > sma50_prev:
                                row["_sma50_trending_up"] = True
                        if len(closes) >= 20:
                            high_20 = max(closes[-20:])
                            if closes[-1] >= high_20 * 0.99:
                                row["_breakout_20d"] = True
                        if ta_data.get("rsi") is not None and len(closes) >= 5:
                            row["_rsi_value"] = ta_data["rsi"]

                        row["_ta"] = ta_data
                except Exception as e:
                    print(f"[SCREENER] TA enrich error {ticker}: {e}")

            for field in ["price", "chg_pct", "mkt_cap"]:
                if row.get(field) is None:
                    row["missing_fields"].append(field)

            return row

        enrich_tasks = [_enrich_one(item) for item in to_enrich]
        try:
            results = await asyncio.wait_for(
                asyncio.gather(*enrich_tasks, return_exceptions=True),
                timeout=ENRICHMENT_TIMEOUT + 2.0,
            )
        except asyncio.TimeoutError:
            results = []
            print(f"[SCREENER] Enrichment timeout after {ENRICHMENT_TIMEOUT}s")

        for r in results:
            if isinstance(r, dict) and r.get("ticker"):
                enriched_rows.append(r)

        print(f"[SCREENER] Phase B: {len(enriched_rows)} enriched (budget: {candle_budget.summary()})")

        # --- Phase C: Filter + Rank ---
        ta_rules = definition.get("ta_rules", {})
        fund_rules = definition.get("fundamental_rules", {})
        weights = definition.get("ranking_weights", {"technical": 0.4, "fundamental": 0.4, "liquidity": 0.2})

        scored_rows = []
        for row in enriched_rows:
            tech_score = 50
            fund_score = 50
            liq_score = 50
            ta = row.get("_ta", {})
            passes = True

            rsi_val = row.get("_rsi_value") or (ta.get("rsi") if ta else None)
            rsi_max = ta_rules.get("rsi_max")
            if rsi_max is not None and rsi_val is not None:
                if rsi_val <= rsi_max:
                    tech_score += 20
                else:
                    tech_score -= 10

            if ta_rules.get("above_sma50") and ta.get("sma_50") and row.get("price"):
                if row["price"] > ta["sma_50"]:
                    tech_score += 15
                else:
                    tech_score -= 15

            if ta_rules.get("above_sma20") and ta.get("sma_20") and row.get("price"):
                if row["price"] > ta["sma_20"]:
                    tech_score += 10

            if ta_rules.get("sma50_trending_up") and row.get("_sma50_trending_up"):
                tech_score += 10

            if ta_rules.get("sma20_above_sma50") and ta.get("sma_20") and ta.get("sma_50"):
                if ta["sma_20"] > ta["sma_50"]:
                    tech_score += 10

            if ta_rules.get("macd_histogram_positive_or_cross"):
                hist = ta.get("macd_histogram")
                if hist is not None and hist > 0:
                    tech_score += 10
                macd_val = ta.get("macd")
                macd_sig = ta.get("macd_signal")
                if macd_val is not None and macd_sig is not None and macd_val > macd_sig:
                    tech_score += 5

            if ta_rules.get("breakout_20d_high") or ta_rules.get("breakout_or_gap_up"):
                if row.get("_breakout_20d"):
                    tech_score += 15
                elif ta_rules.get("breakout_20d_high"):
                    tech_score -= 10

            rel_vol_min = ta_rules.get("rel_vol_min") or ta_rules.get("prefer_rel_vol")
            if rel_vol_min and row.get("rel_vol"):
                if row["rel_vol"] >= rel_vol_min:
                    tech_score += 10
                    liq_score += 10

            if ta_rules.get("above_sma200_or_reclaiming") and ta.get("sma_200") and row.get("price"):
                if row["price"] > ta["sma_200"]:
                    tech_score += 15
                elif ta.get("rsi") and ta["rsi"] > 40:
                    tech_score += 5
                else:
                    tech_score -= 10

            if ta_rules.get("not_severe_downtrend") and ta.get("sma_200") and row.get("price"):
                if row["price"] < ta["sma_200"] * 0.95:
                    if row.get("_sma50_trending_up") is not True:
                        passes = False

            rev_min = fund_rules.get("rev_growth_yoy_min")
            if rev_min is not None:
                if row.get("rev_growth_yoy") is not None:
                    if row["rev_growth_yoy"] >= rev_min:
                        fund_score += 20
                    else:
                        fund_score -= 10
                else:
                    fund_score -= 5

            pe_max = fund_rules.get("pe_max")
            if pe_max is not None:
                if row.get("pe") is not None:
                    if row["pe"] <= pe_max and row["pe"] > 0:
                        fund_score += 15
                    elif row["pe"] > pe_max:
                        fund_score -= 10

            div_min = fund_rules.get("dividend_yield_min")
            if div_min is not None:
                if row.get("div_yield") is not None:
                    if row["div_yield"] >= div_min:
                        fund_score += 15
                    else:
                        fund_score -= 10

            min_dollar_vol = ta_rules.get("min_avg_dollar_vol_m")
            if min_dollar_vol and ta.get("avg_volume") and row.get("price"):
                dollar_vol = ta["avg_volume"] * row["price"] / 1_000_000
                if dollar_vol >= min_dollar_vol:
                    liq_score += 15
                else:
                    liq_score -= 10

            if row.get("price") and row.get("chg_pct") is not None:
                liq_score += 10

            tech_score = max(0, min(100, tech_score))
            fund_score = max(0, min(100, fund_score))
            liq_score = max(0, min(100, liq_score))

            composite = (
                tech_score * weights["technical"]
                + fund_score * weights["fundamental"]
                + liq_score * weights["liquidity"]
            )

            if not passes:
                composite = 0

            clean_row = {k: v for k, v in row.items() if not k.startswith("_")}
            clean_row["composite_score"] = round(composite, 1)
            clean_row["tech_score"] = tech_score
            clean_row["fund_score"] = fund_score
            clean_row["liq_score"] = liq_score
            scored_rows.append(clean_row)

        scored_rows.sort(key=lambda r: r["composite_score"], reverse=True)
        qualified = [r for r in scored_rows if r["composite_score"] > 20]
        final_rows = qualified[:25]

        top_picks = []
        for r in final_rows[:5]:
            top_picks.append({
                "ticker": r["ticker"],
                "confidence": r["composite_score"],
                "reason": ", ".join(r.get("signals", [])[:3]) or "Qualified on screen criteria",
            })

        elapsed = round(_t.time() - start_time, 1)
        print(f"[SCREENER] Phase C: {len(final_rows)} qualified from {len(enriched_rows)} enriched in {elapsed}s")

        return {
            "display_type": "screener",
            "screen_name": definition["screen_label"],
            "preset": preset_name,
            "explain": definition["explain_template"],
            "top_picks": top_picks,
            "rows": final_rows,
            "scan_stats": {
                "candidates_total": candidates_total,
                "enriched": len(enriched_rows),
                "candles_ok": candle_budget._used,
                "candles_blocked": candle_budget._blocked,
                "cache_hits": candle_budget._cache_hits,
                "qualified": len(qualified),
                "returned": len(final_rows),
                "elapsed_s": elapsed,
            },
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