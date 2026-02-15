import re
import httpx
from bs4 import BeautifulSoup
from data.cache import cache, STOCKANALYSIS_TTL


class StockAnalysisScraper:

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }

    def _parse_tables(self, soup: BeautifulSoup) -> list:
        results = []
        for table in soup.select("table"):
            rows = []
            for row in table.select("tr"):
                cells = row.find_all("td")
                if len(cells) >= 2:
                    label = cells[0].get_text(strip=True)
                    value = cells[1].get_text(strip=True)
                    rows.append((label, value))
            if rows:
                results.append(rows)
        return results

    async def get_overview(self, ticker: str) -> dict:
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
                return {"ticker": ticker, "overview": {}, "error": f"HTTP {resp.status_code}"}

            soup = BeautifulSoup(resp.text, "html.parser")
            tables = self._parse_tables(soup)
            stats = {}

            label_map = {
                "market cap": "market_cap",
                "revenue": "revenue",
                "net income": "net_income",
                "eps": "eps",
                "shares out": "shares_outstanding",
                "pe ratio": "pe_ratio",
                "forward pe": "forward_pe",
                "dividend": "dividend_yield",
                "ex-dividend": "ex_dividend_date",
                "volume": "avg_volume",
                "open": "open",
                "previous close": "prev_close",
                "day's range": "days_range",
                "52-week range": "week_52_range",
                "beta": "beta",
                "analysts": "analyst_rating",
                "price target": "price_target",
                "earnings date": "earnings_date",
            }

            for table_rows in tables:
                for label, value in table_rows:
                    label_lower = label.lower().strip()
                    for pattern, key in label_map.items():
                        if label_lower.startswith(pattern):
                            stats[key] = value
                            break

            if stats.get("revenue"):
                match = re.search(r'([+-]?\d+\.?\d*)%', stats["revenue"])
                if match:
                    stats["revenue_growth"] = match.group(0)
                    stats["revenue"] = stats["revenue"].split(match.group(0))[0].strip()

            if stats.get("net_income"):
                match = re.search(r'([+-]?\d+\.?\d*)%', stats["net_income"])
                if match:
                    stats["net_income_growth"] = match.group(0)
                    stats["net_income"] = stats["net_income"].split(match.group(0))[0].strip()

            if stats.get("eps"):
                match = re.search(r'([+-]?\d+\.?\d*)%', stats["eps"])
                if match:
                    stats["eps_growth"] = match.group(0)
                    stats["eps"] = stats["eps"].split(match.group(0))[0].strip()

            if stats.get("market_cap"):
                match = re.search(r'([+-]?\d+\.?\d*)%', stats["market_cap"])
                if match:
                    stats["market_cap"] = stats["market_cap"].split(match.group(0))[0].strip()

            if stats.get("week_52_range"):
                parts = stats["week_52_range"].split("-")
                if len(parts) == 2:
                    stats["week_52_low"] = parts[0].strip()
                    stats["week_52_high"] = parts[1].strip()

            if stats.get("price_target"):
                pt = stats["price_target"]
                match = re.search(r'\(([+-]?\d+\.?\d*)%\)', pt)
                if match:
                    stats["upside_downside"] = match.group(1) + "%"
                    stats["price_target"] = pt.split("(")[0].strip()

            if stats.get("dividend_yield"):
                div = stats["dividend_yield"]
                match = re.search(r'\((\d+\.?\d*%)\)', div)
                if match:
                    stats["dividend_yield"] = match.group(1)
                elif div.lower() == "n/a" or div == "-":
                    stats["dividend_yield"] = "N/A"

            print(f"[StockAnalysis] overview {ticker}: parsed {len(stats)} fields: {list(stats.keys())}")
            result = {"ticker": ticker, "overview": stats}
            cache.set(cache_key, result, STOCKANALYSIS_TTL)
            return result
        except Exception as e:
            print(f"[StockAnalysis] overview error for {ticker}: {e}")
            return {"ticker": ticker, "overview": {}, "error": str(e)}

    async def get_analyst_ratings(self, ticker: str) -> dict:
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
                return {"ticker": ticker, "analyst_ratings": {}, "error": f"HTTP {resp.status_code}"}

            soup = BeautifulSoup(resp.text, "html.parser")
            tables = self._parse_tables(soup)
            ratings = {}

            for table_rows in tables:
                labels_in_table = [l.lower() for l, v in table_rows]

                if "strong buy" in labels_in_table or "buy" in labels_in_table:
                    for label, value in table_rows:
                        ll = label.lower().strip()
                        if ll == "strong buy":
                            ratings["strong_buy_count"] = value
                        elif ll == "buy":
                            ratings["buy_count"] = value
                        elif ll == "hold":
                            ratings["hold_count"] = value
                        elif ll == "sell":
                            ratings["sell_count"] = value
                        elif ll == "strong sell":
                            ratings["strong_sell_count"] = value
                        elif ll == "total":
                            ratings["total_analysts"] = value

                    try:
                        sb = int(ratings.get("strong_buy_count", 0))
                        b = int(ratings.get("buy_count", 0))
                        h = int(ratings.get("hold_count", 0))
                        s = int(ratings.get("sell_count", 0))
                        ss = int(ratings.get("strong_sell_count", 0))
                        total = sb + b + h + s + ss
                        if total > 0:
                            if (sb + b) / total >= 0.7:
                                ratings["consensus"] = "Strong Buy" if sb > b else "Buy"
                            elif (sb + b) / total >= 0.5:
                                ratings["consensus"] = "Buy"
                            elif (s + ss) / total >= 0.5:
                                ratings["consensus"] = "Sell"
                            else:
                                ratings["consensus"] = "Hold"
                    except (ValueError, TypeError):
                        pass

                if "target" in labels_in_table or "price" in labels_in_table:
                    for label, value in table_rows:
                        ll = label.lower().strip()
                        if ll == "target" or ll == "price target":
                            pass
                        elif "low" in ll:
                            ratings["price_target_low"] = value
                        elif "high" in ll:
                            ratings["price_target_high"] = value
                        elif "change" in ll or "upside" in ll or "downside" in ll:
                            ratings["upside_downside"] = value

                    if "price_target_low" in ratings and "price_target_high" in ratings:
                        try:
                            low = float(ratings["price_target_low"].replace("$", "").replace(",", ""))
                            high = float(ratings["price_target_high"].replace("$", "").replace(",", ""))
                            ratings["price_target"] = f"${(low + high) / 2:.2f}"
                        except (ValueError, TypeError):
                            pass

            print(f"[StockAnalysis] analyst {ticker}: parsed {len(ratings)} fields: {list(ratings.keys())}")
            result = {"ticker": ticker, "analyst_ratings": ratings}
            cache.set(cache_key, result, STOCKANALYSIS_TTL)
            return result
        except Exception as e:
            print(f"[StockAnalysis] analyst error for {ticker}: {e}")
            return {"ticker": ticker, "analyst_ratings": {}, "error": str(e)}

    async def get_financials(self, ticker: str) -> dict:
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
                return {"ticker": ticker, "financials": {}, "error": f"HTTP {resp.status_code}"}

            soup = BeautifulSoup(resp.text, "html.parser")
            tables = self._parse_tables(soup)
            metrics = {}

            label_map = {
                "revenue": "revenue",
                "revenue growth": "revenue_growth",
                "net income": "net_income",
                "net income growth": "net_income_growth",
                "eps": "eps",
                "eps growth": "eps_growth",
                "profit margin": "profit_margin",
                "net margin": "profit_margin",
                "operating margin": "operating_margin",
                "operating income": "operating_income",
                "gross profit": "gross_profit",
                "free cash flow": "free_cash_flow",
                "ebitda": "ebitda",
                "cost of revenue": "cost_of_revenue",
            }

            for table_rows in tables:
                for label, value in table_rows:
                    label_lower = label.lower().strip()
                    for pattern, key in label_map.items():
                        if label_lower.startswith(pattern) or label_lower == pattern:
                            if key not in metrics:
                                metrics[key] = value
                            break

            print(f"[StockAnalysis] financials {ticker}: parsed {len(metrics)} fields: {list(metrics.keys())}")
            result = {"ticker": ticker, "financials": metrics}
            cache.set(cache_key, result, STOCKANALYSIS_TTL)
            return result
        except Exception as e:
            print(f"[StockAnalysis] financials error for {ticker}: {e}")
            return {"ticker": ticker, "financials": {}, "error": str(e)}
