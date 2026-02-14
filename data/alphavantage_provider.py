import httpx


class AlphaVantageProvider:
    """
    Provides news sentiment scores and economic indicator data
    via Alpha Vantage's free API.
    
    NOTE: Free tier is limited to 25 requests per day.
    Use sparingly — only for high-value data that other sources don't provide.
    """

    BASE_URL = "https://www.alphavantage.co/query"

    def __init__(self, api_key: str):
        self.api_key = api_key

    async def get_news_sentiment(self, ticker: str = None, topics: str = None) -> dict:
        """
        Get AI-powered news sentiment analysis.
        Each article gets a sentiment score and relevance score.
        
        Topics can be: earnings, ipo, mergers_and_acquisitions,
        financial_markets, economy_fiscal, economy_monetary,
        economy_macro, energy_transportation, finance, 
        life_sciences, manufacturing, real_estate, 
        retail_wholesale, technology
        """
        try:
            params = {
                "function": "NEWS_SENTIMENT",
                "apikey": self.api_key,
                "limit": 10,
                "sort": "RELEVANCE",
            }
            if ticker:
                params["tickers"] = ticker.upper()
            if topics:
                params["topics"] = topics

            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    self.BASE_URL,
                    params=params,
                    timeout=20,
                )

            data = resp.json()

            if "feed" not in data:
                return {
                    "ticker": ticker,
                    "articles": [],
                    "error": data.get("Note", data.get("Information", "No data")),
                }

            articles = []
            overall_sentiment_sum = 0
            article_count = 0

            for article in data["feed"][:10]:
                ticker_sentiment = None
                if ticker:
                    for ts in article.get("ticker_sentiment", []):
                        if ts.get("ticker", "").upper() == ticker.upper():
                            ticker_sentiment = {
                                "relevance_score": ts.get("relevance_score"),
                                "sentiment_score": ts.get("ticker_sentiment_score"),
                                "sentiment_label": ts.get("ticker_sentiment_label"),
                            }
                            try:
                                overall_sentiment_sum += float(
                                    ts.get("ticker_sentiment_score", 0)
                                )
                                article_count += 1
                            except (ValueError, TypeError):
                                pass
                            break

                articles.append({
                    "title": article.get("title"),
                    "source": article.get("source"),
                    "published": article.get("time_published"),
                    "overall_sentiment": article.get("overall_sentiment_label"),
                    "overall_sentiment_score": article.get("overall_sentiment_score"),
                    "ticker_sentiment": ticker_sentiment,
                })

            avg_sentiment = None
            sentiment_label = None
            if article_count > 0:
                avg_sentiment = round(overall_sentiment_sum / article_count, 4)
                if avg_sentiment > 0.25:
                    sentiment_label = "Bullish"
                elif avg_sentiment > 0.1:
                    sentiment_label = "Somewhat Bullish"
                elif avg_sentiment > -0.1:
                    sentiment_label = "Neutral"
                elif avg_sentiment > -0.25:
                    sentiment_label = "Somewhat Bearish"
                else:
                    sentiment_label = "Bearish"

            return {
                "ticker": ticker,
                "article_count": len(articles),
                "average_sentiment_score": avg_sentiment,
                "average_sentiment_label": sentiment_label,
                "articles": articles,
            }
        except Exception as e:
            print(f"Alpha Vantage news sentiment error: {e}")
            return {"ticker": ticker, "articles": [], "error": str(e)}

    async def get_market_news_sentiment(self, topic: str = "financial_markets") -> dict:
        """
        Get broad market news sentiment for a topic.
        Useful for 'what's the market mood today' type queries.
        """
        return await self.get_news_sentiment(ticker=None, topics=topic)

    async def get_fed_funds_rate(self) -> dict:
        """Get the current federal funds interest rate."""
        try:
            params = {
                "function": "FEDERAL_FUNDS_RATE",
                "interval": "monthly",
                "apikey": self.api_key,
            }
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    self.BASE_URL,
                    params=params,
                    timeout=20,
                )

            data = resp.json()
            records = data.get("data", [])

            if not records:
                return {"error": "No data available"}

            recent = records[:6]
            return {
                "current_rate": recent[0].get("value") if recent else None,
                "current_date": recent[0].get("date") if recent else None,
                "trend": [
                    {"date": r.get("date"), "rate": r.get("value")}
                    for r in recent
                ],
            }
        except Exception as e:
            print(f"Alpha Vantage fed funds rate error: {e}")
            return {"error": str(e)}

    async def get_cpi(self) -> dict:
        """Get Consumer Price Index (inflation) data."""
        try:
            params = {
                "function": "CPI",
                "interval": "monthly",
                "apikey": self.api_key,
            }
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    self.BASE_URL,
                    params=params,
                    timeout=20,
                )

            data = resp.json()
            records = data.get("data", [])

            if not records:
                return {"error": "No data available"}

            recent = records[:6]
            yoy_change = None
            if len(records) >= 13:
                try:
                    current = float(records[0].get("value", 0))
                    year_ago = float(records[12].get("value", 0))
                    if year_ago > 0:
                        yoy_change = round(
                            ((current - year_ago) / year_ago) * 100, 2
                        )
                except (ValueError, TypeError):
                    pass

            return {
                "latest_cpi": recent[0].get("value") if recent else None,
                "latest_date": recent[0].get("date") if recent else None,
                "yoy_inflation_pct": yoy_change,
                "trend": [
                    {"date": r.get("date"), "cpi": r.get("value")}
                    for r in recent
                ],
            }
        except Exception as e:
            print(f"Alpha Vantage CPI error: {e}")
            return {"error": str(e)}

    async def get_unemployment(self) -> dict:
        """Get unemployment rate data."""
        try:
            params = {
                "function": "UNEMPLOYMENT",
                "apikey": self.api_key,
            }
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    self.BASE_URL,
                    params=params,
                    timeout=20,
                )

            data = resp.json()
            records = data.get("data", [])

            if not records:
                return {"error": "No data available"}

            recent = records[:6]
            return {
                "current_rate": recent[0].get("value") if recent else None,
                "current_date": recent[0].get("date") if recent else None,
                "trend": [
                    {"date": r.get("date"), "rate": r.get("value")}
                    for r in recent
                ],
            }
        except Exception as e:
            print(f"Alpha Vantage unemployment error: {e}")
            return {"error": str(e)}

    async def get_macro_overview(self) -> dict:
        """
        Get a combined macro overview: fed funds rate, CPI/inflation,
        and unemployment. This uses 3 of your 25 daily API calls.
        """
        fed = await self.get_fed_funds_rate()
        cpi = await self.get_cpi()
        unemployment = await self.get_unemployment()

        return {
            "federal_funds_rate": fed,
            "inflation_cpi": cpi,
            "unemployment": unemployment,
        }
