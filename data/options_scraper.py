import httpx
from bs4 import BeautifulSoup


class OptionsScraper:
    """
    Scrapes free publicly available options data from Barchart
    for unusual options activity, put/call ratios, and volume leaders.
    This gives us the most important options flow signals without
    a paid API.
    """

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    async def get_unusual_options_activity(self) -> list:
        """
        Get stocks with unusual options activity from Barchart.
        This shows where volume significantly exceeds open interest,
        indicating large new positions being opened.
        """
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    "https://www.barchart.com/options/unusual-activity/stocks",
                    headers=self.HEADERS,
                    timeout=10,
                )

            if resp.status_code != 200:
                return []

            soup = BeautifulSoup(resp.text, "html.parser")
            rows = soup.select("tr[data-symbol], tbody tr")

            results = []
            for row in rows[:25]:
                cells = row.find_all("td")
                if len(cells) >= 6:
                    ticker = cells[0].get_text(strip=True)
                    if not ticker or len(ticker) > 6:
                        continue

                    results.append({
                        "ticker": ticker,
                        "type": cells[1].get_text(strip=True) if len(cells) > 1 else "",
                        "strike": cells[2].get_text(strip=True) if len(cells) > 2 else "",
                        "expiration": cells[3].get_text(strip=True) if len(cells) > 3 else "",
                        "volume": cells[4].get_text(strip=True) if len(cells) > 4 else "",
                        "open_interest": cells[5].get_text(strip=True) if len(cells) > 5 else "",
                        "vol_oi_ratio": cells[6].get_text(strip=True) if len(cells) > 6 else "",
                    })
            return results
        except Exception as e:
            print(f"Options unusual activity scraper error: {e}")
            return []

    async def get_options_volume_leaders(self) -> list:
        """Get stocks with the highest options volume today."""
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    "https://www.barchart.com/options/volume-leaders/stocks",
                    headers=self.HEADERS,
                    timeout=10,
                )

            if resp.status_code != 200:
                return []

            soup = BeautifulSoup(resp.text, "html.parser")
            rows = soup.select("tr[data-symbol], tbody tr")

            results = []
            for row in rows[:20]:
                cells = row.find_all("td")
                if len(cells) >= 4:
                    ticker = cells[0].get_text(strip=True)
                    if not ticker or len(ticker) > 6:
                        continue

                    results.append({
                        "ticker": ticker,
                        "options_volume": cells[1].get_text(strip=True) if len(cells) > 1 else "",
                        "open_interest": cells[2].get_text(strip=True) if len(cells) > 2 else "",
                        "implied_volatility": cells[3].get_text(strip=True) if len(cells) > 3 else "",
                    })
            return results
        except Exception as e:
            print(f"Options volume leaders scraper error: {e}")
            return []

    async def get_put_call_ratio(self, ticker: str) -> dict:
        """
        Get put/call ratio data for a specific ticker from Barchart.
        A ratio > 1 = bearish sentiment, < 1 = bullish sentiment.
        """
        ticker = ticker.upper()
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"https://www.barchart.com/stocks/quotes/{ticker}/put-call-ratios",
                    headers=self.HEADERS,
                    timeout=10,
                )

            if resp.status_code != 200:
                return {"ticker": ticker, "error": "Could not fetch data"}

            soup = BeautifulSoup(resp.text, "html.parser")

            ratios = {"ticker": ticker}

            tables = soup.select("table")
            for table in tables:
                rows = table.select("tr")
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) >= 2:
                        label = cells[0].get_text(strip=True).lower()
                        value = cells[1].get_text(strip=True)
                        if "put/call vol" in label or "volume ratio" in label:
                            ratios["put_call_volume_ratio"] = value
                        elif "put/call oi" in label or "interest ratio" in label:
                            ratios["put_call_oi_ratio"] = value
                        elif "total call vol" in label:
                            ratios["total_call_volume"] = value
                        elif "total put vol" in label:
                            ratios["total_put_volume"] = value
                        elif "total call oi" in label or "call open int" in label:
                            ratios["total_call_oi"] = value
                        elif "total put oi" in label or "put open int" in label:
                            ratios["total_put_oi"] = value

            stat_items = soup.select("[class*='stat'], [class*='ratio']")
            for item in stat_items:
                text = item.get_text(strip=True)
                if "Put/Call" in text:
                    import re
                    numbers = re.findall(r'[\d.]+', text)
                    if numbers and "put_call_volume_ratio" not in ratios:
                        ratios["put_call_volume_ratio"] = numbers[0]

            return ratios
        except Exception as e:
            print(f"Put/call ratio error for {ticker}: {e}")
            return {"ticker": ticker, "error": str(e)}

    def interpret_flow(self, unusual_activity: list) -> dict:
        """
        Analyze unusual options activity to identify directional signals.
        Groups by ticker and determines if the flow is bullish or bearish.
        """
        ticker_signals = {}

        for trade in unusual_activity:
            ticker = trade.get("ticker", "")
            if not ticker:
                continue

            if ticker not in ticker_signals:
                ticker_signals[ticker] = {
                    "ticker": ticker,
                    "call_count": 0,
                    "put_count": 0,
                    "trades": [],
                }

            trade_type = trade.get("type", "").lower()
            if "call" in trade_type:
                ticker_signals[ticker]["call_count"] += 1
            elif "put" in trade_type:
                ticker_signals[ticker]["put_count"] += 1

            ticker_signals[ticker]["trades"].append(trade)

        for ticker, data in ticker_signals.items():
            calls = data["call_count"]
            puts = data["put_count"]
            total = calls + puts

            if total == 0:
                data["signal"] = "neutral"
                data["confidence"] = "low"
            elif calls > puts * 2:
                data["signal"] = "bullish"
                data["confidence"] = "high" if total >= 3 else "moderate"
            elif puts > calls * 2:
                data["signal"] = "bearish"
                data["confidence"] = "high" if total >= 3 else "moderate"
            elif calls > puts:
                data["signal"] = "slightly bullish"
                data["confidence"] = "moderate"
            elif puts > calls:
                data["signal"] = "slightly bearish"
                data["confidence"] = "moderate"
            else:
                data["signal"] = "mixed"
                data["confidence"] = "low"

        return ticker_signals
