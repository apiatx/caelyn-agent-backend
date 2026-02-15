import httpx
from bs4 import BeautifulSoup
from data.cache import cache, STOCKANALYSIS_TTL


class StockAnalysisScraper:
    """Scrapes StockAnalysis.com for fundamental data not available via Polygon."""

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }

    async def get_financials(self, ticker: str) -> dict:
        """Get key financial metrics for a ticker."""
        ticker = ticker.upper()
        cache_key = f"stockanalysis:financials:{ticker}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"https://stockanalysis.com/stocks/{ticker.lower()}/financials/",
                    headers=self.HEADERS,
                    timeout=15,
                )
            print(f"[StockAnalysis] financials {ticker}: status={resp.status_code}, body_len={len(resp.text)}")
            if resp.status_code != 200:
                print(f"[StockAnalysis] financials {ticker}: non-200 response, first 500 chars: {resp.text[:500]}")
                return {"ticker": ticker, "financials": {}, "error": f"HTTP {resp.status_code}"}
            soup = BeautifulSoup(resp.text, "html.parser")

            metrics = {}

            stat_tables = soup.select("table")
            for table in stat_tables:
                rows = table.select("tr")
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) >= 2:
                        label = cells[0].get_text(strip=True).lower()
                        value = cells[1].get_text(strip=True)
                        if "revenue" in label:
                            metrics["revenue"] = value
                        elif "net income" in label:
                            metrics["net_income"] = value
                        elif "eps" in label and "diluted" not in label:
                            metrics["eps"] = value
                        elif "profit margin" in label or "net margin" in label:
                            metrics["profit_margin"] = value
                        elif "operating margin" in label:
                            metrics["operating_margin"] = value
                        elif "free cash flow" in label:
                            metrics["free_cash_flow"] = value

            print(f"[StockAnalysis] financials {ticker}: parsed metrics={list(metrics.keys()) if metrics else 'EMPTY'}")
            result = {"ticker": ticker, "financials": metrics}
            cache.set(cache_key, result, STOCKANALYSIS_TTL)
            return result
        except Exception as e:
            print(f"[StockAnalysis] financials error for {ticker}: {e}")
            return {"ticker": ticker, "financials": {}, "error": str(e)}

    async def get_overview(self, ticker: str) -> dict:
        """Get stock overview stats like P/E, market cap, dividend yield."""
        ticker = ticker.upper()
        cache_key = f"stockanalysis:overview:{ticker}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"https://stockanalysis.com/stocks/{ticker.lower()}/",
                    headers=self.HEADERS,
                    timeout=15,
                )
            print(f"[StockAnalysis] overview {ticker}: status={resp.status_code}, body_len={len(resp.text)}")
            if resp.status_code != 200:
                print(f"[StockAnalysis] overview {ticker}: non-200 response, first 500 chars: {resp.text[:500]}")
                return {"ticker": ticker, "overview": {}, "error": f"HTTP {resp.status_code}"}
            soup = BeautifulSoup(resp.text, "html.parser")

            stats = {}

            stat_items = soup.select("[data-test]")
            for item in stat_items:
                label = item.get("data-test", "").lower()
                value = item.get_text(strip=True)
                if label and value:
                    stats[label] = value

            if not stats:
                tables = soup.select("table")
                for table in tables:
                    rows = table.select("tr")
                    for row in rows:
                        cells = row.find_all("td")
                        if len(cells) >= 2:
                            label = cells[0].get_text(strip=True)
                            value = cells[1].get_text(strip=True)
                            label_lower = label.lower()
                            if "p/e" in label_lower:
                                stats["pe_ratio"] = value
                            elif "forward p/e" in label_lower:
                                stats["forward_pe"] = value
                            elif "market cap" in label_lower:
                                stats["market_cap"] = value
                            elif "dividend" in label_lower and "yield" in label_lower:
                                stats["dividend_yield"] = value
                            elif "52" in label_lower and "high" in label_lower:
                                stats["week_52_high"] = value
                            elif "52" in label_lower and "low" in label_lower:
                                stats["week_52_low"] = value
                            elif "earnings date" in label_lower:
                                stats["earnings_date"] = value
                            elif "beta" in label_lower:
                                stats["beta"] = value
                            elif "short" in label_lower and "float" in label_lower:
                                stats["short_float"] = value
                            elif "analyst" in label_lower and "rating" in label_lower:
                                stats["analyst_rating"] = value
                            elif "price target" in label_lower:
                                stats["price_target"] = value

            print(f"[StockAnalysis] overview {ticker}: parsed stats={list(stats.keys()) if stats else 'EMPTY'}")
            result = {"ticker": ticker, "overview": stats}
            cache.set(cache_key, result, STOCKANALYSIS_TTL)
            return result
        except Exception as e:
            print(f"[StockAnalysis] overview error for {ticker}: {e}")
            return {"ticker": ticker, "overview": {}, "error": str(e)}

    async def get_analyst_ratings(self, ticker: str) -> dict:
        """Get analyst ratings and price targets."""
        ticker = ticker.upper()
        cache_key = f"stockanalysis:analyst:{ticker}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"https://stockanalysis.com/stocks/{ticker.lower()}/forecast/",
                    headers=self.HEADERS,
                    timeout=15,
                )
            print(f"[StockAnalysis] analyst {ticker}: status={resp.status_code}, body_len={len(resp.text)}")
            if resp.status_code != 200:
                print(f"[StockAnalysis] analyst {ticker}: non-200 response, first 500 chars: {resp.text[:500]}")
                return {"ticker": ticker, "analyst_ratings": {}, "error": f"HTTP {resp.status_code}"}
            soup = BeautifulSoup(resp.text, "html.parser")

            ratings = {}

            tables = soup.select("table")
            for table in tables:
                rows = table.select("tr")
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) >= 2:
                        label = cells[0].get_text(strip=True).lower()
                        value = cells[1].get_text(strip=True)
                        if "consensus" in label or "rating" in label:
                            ratings["consensus"] = value
                        elif "price target" in label:
                            ratings["price_target"] = value
                        elif "upside" in label or "downside" in label:
                            ratings["upside_downside"] = value
                        elif "buy" in label:
                            ratings["buy_count"] = value
                        elif "hold" in label:
                            ratings["hold_count"] = value
                        elif "sell" in label:
                            ratings["sell_count"] = value

            print(f"[StockAnalysis] analyst {ticker}: parsed ratings={list(ratings.keys()) if ratings else 'EMPTY'}")
            result = {"ticker": ticker, "analyst_ratings": ratings}
            cache.set(cache_key, result, STOCKANALYSIS_TTL)
            return result
        except Exception as e:
            print(f"[StockAnalysis] analyst error for {ticker}: {e}")
            return {"ticker": ticker, "analyst_ratings": {}, "error": str(e)}
