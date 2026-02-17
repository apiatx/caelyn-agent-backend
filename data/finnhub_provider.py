import finnhub
from datetime import datetime, timedelta
from data.cache import cache, FINNHUB_TTL, EARNINGS_TTL


class FinnhubProvider:
    """
    Provides insider trading data, earnings calendar, earnings surprises,
    social sentiment, company peers, and recommendation trends via
    Finnhub's free API.
    """

    def __init__(self, api_key: str):
        self.client = finnhub.Client(api_key=api_key)

    def get_quote(self, ticker: str) -> dict:
        ticker = ticker.upper()
        cache_key = f"finnhub:quote:{ticker}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            data = self.client.quote(ticker)
            if data.get("c") and data["c"] > 0:
                result = {
                    "price": data.get("c"),
                    "change": data.get("d"),
                    "change_pct": data.get("dp"),
                    "high": data.get("h"),
                    "low": data.get("l"),
                    "open": data.get("o"),
                    "prev_close": data.get("pc"),
                }
                cache.set(cache_key, result, 60)
                return result
        except Exception as e:
            print(f"Finnhub quote error for {ticker}: {e}")
        return {}

    def get_company_profile(self, ticker: str) -> dict:
        ticker = ticker.upper()
        cache_key = f"finnhub:profile:{ticker}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            data = self.client.company_profile2(symbol=ticker)
            if data.get("name"):
                result = {
                    "name": data.get("name"),
                    "sector": data.get("finnhubIndustry"),
                    "market_cap": (data.get("marketCapitalization") or 0) * 1_000_000,
                    "industry": data.get("finnhubIndustry"),
                    "logo": data.get("logo"),
                    "exchange": data.get("exchange"),
                    "ipo_date": data.get("ipo"),
                    "country": data.get("country"),
                    "web_url": data.get("weburl"),
                }
                cache.set(cache_key, result, 86400)
                return result
        except Exception as e:
            print(f"Finnhub profile error for {ticker}: {e}")
        return {}

    def get_insider_sentiment(self, ticker: str) -> dict:
        """
        Get insider sentiment (MSPR) for a ticker.
        MSPR ranges from -100 (heavy insider selling) to +100 (heavy insider buying).
        This can signal price changes 30-90 days out.
        """
        ticker = ticker.upper()
        cache_key = f"finnhub:insider:{ticker}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            today = datetime.now()
            one_year_ago = today - timedelta(days=365)
            data = self.client.stock_insider_sentiment(
                ticker,
                one_year_ago.strftime("%Y-%m-%d"),
                today.strftime("%Y-%m-%d"),
            )
            records = data.get("data", [])
            if not records:
                return {"ticker": ticker, "insider_sentiment": None}

            recent = records[-3:] if len(records) >= 3 else records
            latest = records[-1]

            result = {
                "ticker": ticker,
                "latest_mspr": latest.get("mspr"),
                "latest_change": latest.get("change"),
                "latest_month": f"{latest.get('year')}-{latest.get('month')}",
                "trend": [
                    {
                        "month": f"{r.get('year')}-{r.get('month')}",
                        "mspr": r.get("mspr"),
                        "share_change": r.get("change"),
                    }
                    for r in recent
                ],
                "signal": self._interpret_mspr(latest.get("mspr")),
            }
            cache.set(cache_key, result, FINNHUB_TTL)
            return result
        except Exception as e:
            print(f"Finnhub insider sentiment error for {ticker}: {e}")
            return {"ticker": ticker, "insider_sentiment": None, "error": str(e)}

    def get_insider_transactions(self, ticker: str) -> list:
        """Get recent insider buy/sell transactions (SEC Form 4 filings)."""
        ticker = ticker.upper()
        try:
            data = self.client.stock_insider_transactions(ticker)
            transactions = data.get("data", [])[:10]
            return [
                {
                    "name": t.get("name"),
                    "share": t.get("share"),
                    "change": t.get("change"),
                    "transaction_type": t.get("transactionType"),
                    "transaction_date": t.get("transactionDate"),
                    "filing_date": t.get("filingDate"),
                }
                for t in transactions
            ]
        except Exception as e:
            print(f"Finnhub insider transactions error for {ticker}: {e}")
            return []

    def get_earnings_calendar(self, ticker: str = None) -> list:
        """
        Get upcoming earnings dates. If ticker is provided, get earnings
        for that specific stock. Otherwise get the market-wide calendar.
        """
        try:
            today = datetime.now()
            next_month = today + timedelta(days=30)
            data = self.client.earnings_calendar(
                _from=today.strftime("%Y-%m-%d"),
                to=next_month.strftime("%Y-%m-%d"),
                symbol=ticker.upper() if ticker else None,
            )
            earnings = data.get("earningsCalendar", [])

            if ticker:
                earnings = [
                    e for e in earnings
                    if e.get("symbol", "").upper() == ticker.upper()
                ]

            results = []
            for e in earnings[:20]:
                results.append({
                    "ticker": e.get("symbol"),
                    "date": e.get("date"),
                    "eps_estimate": e.get("epsEstimate"),
                    "revenue_estimate": e.get("revenueEstimate"),
                    "hour": e.get("hour"),
                    "quarter": e.get("quarter"),
                    "year": e.get("year"),
                })
            return results
        except Exception as e:
            print(f"Finnhub earnings calendar error: {e}")
            return []

    def get_earnings_surprises(self, ticker: str) -> list:
        """Get past earnings results vs estimates (beat or miss)."""
        ticker = ticker.upper()
        cache_key = f"finnhub:earnings:{ticker}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            data = self.client.company_earnings(ticker, limit=4)
            result = [
                {
                    "period": e.get("period"),
                    "actual_eps": e.get("actual"),
                    "estimate_eps": e.get("estimate"),
                    "surprise": e.get("surprise"),
                    "surprise_percent": e.get("surprisePercent"),
                    "beat": e.get("actual", 0) > e.get("estimate", 0)
                    if e.get("actual") is not None and e.get("estimate") is not None
                    else None,
                }
                for e in data
            ]
            cache.set(cache_key, result, EARNINGS_TTL)
            return result
        except Exception as e:
            print(f"Finnhub earnings surprises error for {ticker}: {e}")
            return []

    def get_recommendation_trends(self, ticker: str) -> list:
        """Get analyst recommendation trends (buy/hold/sell counts over time)."""
        ticker = ticker.upper()
        cache_key = f"finnhub:recommendations:{ticker}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            data = self.client.recommendation_trends(ticker)
            result = [
                {
                    "period": r.get("period"),
                    "strong_buy": r.get("strongBuy"),
                    "buy": r.get("buy"),
                    "hold": r.get("hold"),
                    "sell": r.get("sell"),
                    "strong_sell": r.get("strongSell"),
                }
                for r in data[:4]
            ]
            cache.set(cache_key, result, FINNHUB_TTL)
            return result
        except Exception as e:
            print(f"Finnhub recommendation trends error for {ticker}: {e}")
            return []

    def get_social_sentiment(self, ticker: str) -> dict:
        """Get social media sentiment from Reddit and Twitter."""
        ticker = ticker.upper()
        cache_key = f"finnhub:social:{ticker}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            try:
                data = self.client.stock_social_sentiment(ticker)
            except Exception as api_err:
                err_str = str(api_err)
                if "403" in err_str or "access" in err_str.lower():
                    result = {"ticker": ticker, "reddit": None, "twitter": None, "note": "Not available on current plan"}
                    cache.set(cache_key, result, FINNHUB_TTL)
                    return result
                raise
            reddit_data = data.get("reddit", [])
            twitter_data = data.get("twitter", [])

            reddit_summary = None
            if reddit_data:
                recent_reddit = reddit_data[-5:] if len(reddit_data) >= 5 else reddit_data
                total_mentions = sum(r.get("mention", 0) for r in recent_reddit)
                avg_score = (
                    sum(r.get("score", 0) for r in recent_reddit) / len(recent_reddit)
                    if recent_reddit
                    else 0
                )
                positive = sum(
                    r.get("positiveScore", 0) for r in recent_reddit
                )
                negative = sum(
                    r.get("negativeScore", 0) for r in recent_reddit
                )
                reddit_summary = {
                    "total_mentions": total_mentions,
                    "avg_score": round(avg_score, 2),
                    "positive_score": round(positive, 2),
                    "negative_score": round(negative, 2),
                    "sentiment": "bullish" if positive > negative else "bearish"
                    if negative > positive
                    else "neutral",
                }

            twitter_summary = None
            if twitter_data:
                recent_twitter = twitter_data[-5:] if len(twitter_data) >= 5 else twitter_data
                total_mentions = sum(t.get("mention", 0) for t in recent_twitter)
                avg_score = (
                    sum(t.get("score", 0) for t in recent_twitter)
                    / len(recent_twitter)
                    if recent_twitter
                    else 0
                )
                positive = sum(
                    t.get("positiveScore", 0) for t in recent_twitter
                )
                negative = sum(
                    t.get("negativeScore", 0) for t in recent_twitter
                )
                twitter_summary = {
                    "total_mentions": total_mentions,
                    "avg_score": round(avg_score, 2),
                    "positive_score": round(positive, 2),
                    "negative_score": round(negative, 2),
                    "sentiment": "bullish" if positive > negative else "bearish"
                    if negative > positive
                    else "neutral",
                }

            result = {
                "ticker": ticker,
                "reddit": reddit_summary,
                "twitter": twitter_summary,
            }
            cache.set(cache_key, result, FINNHUB_TTL)
            return result
        except Exception as e:
            print(f"Finnhub social sentiment error for {ticker}: {e}")
            return {"ticker": ticker, "reddit": None, "twitter": None}

    def get_company_peers(self, ticker: str) -> list:
        """Get list of peer/comparable companies."""
        ticker = ticker.upper()
        try:
            return self.client.company_peers(ticker)
        except Exception as e:
            print(f"Finnhub peers error for {ticker}: {e}")
            return []

    def get_upcoming_earnings(self) -> list:
        """Get all earnings coming up in the next 7 days."""
        cache_key = "finnhub:upcoming_earnings"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            today = datetime.now()
            next_week = today + timedelta(days=7)
            data = self.client.earnings_calendar(
                _from=today.strftime("%Y-%m-%d"),
                to=next_week.strftime("%Y-%m-%d"),
                symbol=None,
            )
            earnings = data.get("earningsCalendar", [])
            result = [
                {
                    "ticker": e.get("symbol"),
                    "date": e.get("date"),
                    "eps_estimate": e.get("epsEstimate"),
                    "revenue_estimate": e.get("revenueEstimate"),
                    "hour": e.get("hour"),
                }
                for e in earnings[:30]
            ]
            cache.set(cache_key, result, EARNINGS_TTL)
            return result
        except Exception as e:
            print(f"Finnhub upcoming earnings error: {e}")
            return []

    def _interpret_mspr(self, mspr) -> str:
        """Interpret the insider sentiment MSPR score."""
        if mspr is None:
            return "no data"
        if mspr > 50:
            return "strong insider buying — very bullish signal"
        if mspr > 20:
            return "moderate insider buying — bullish signal"
        if mspr > 0:
            return "slight insider buying — mildly bullish"
        if mspr > -20:
            return "slight insider selling — mildly bearish"
        if mspr > -50:
            return "moderate insider selling — bearish signal"
        return "heavy insider selling — very bearish signal"
