import httpx
from bs4 import BeautifulSoup


class FinvizScraper:
    """Scrapes Finviz for screener data that Polygon doesn't provide."""

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }

    async def get_screener_results(
        self, filters: str = "ta_topgainers"
    ) -> list:
        """
        Scrape Finviz screener. Common filter values:
        - ta_topgainers: Top gainers
        - ta_toplosers: Top losers
        - ta_mostactive: Most active
        - ta_unusualvolume: Unusual volume
        - ta_overbought: Overbought (RSI)
        - ta_oversold: Oversold (RSI)
        """
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"https://finviz.com/screener.ashx?v=111&s={filters}",
                    headers=self.HEADERS,
                    timeout=15,
                )
            soup = BeautifulSoup(resp.text, "html.parser")

            # Finviz table structure — find screener rows
            rows = soup.select("tr.styled-row")
            if not rows:
                rows = soup.select(
                    "table.screener_table tr.screener-body-table-nw"
                )

            results = []
            for row in rows[:20]:
                cols = row.find_all("td")
                if len(cols) >= 10:
                    results.append(
                        {
                            "ticker": cols[1].text.strip(),
                            "company": cols[2].text.strip(),
                            "sector": cols[3].text.strip(),
                            "market_cap": cols[6].text.strip(),
                            "price": cols[8].text.strip(),
                            "change": cols[9].text.strip(),
                        }
                    )
            return results
        except Exception as e:
            print(f"Finviz scraper error: {e}")
            return []

    async def get_oversold_stocks(self) -> list:
        """Get stocks with RSI below 30 — oversold bounce candidates."""
        return await self.get_screener_results("ta_oversold")

    async def get_overbought_stocks(self) -> list:
        """Get stocks with RSI above 70 — potential short or exit candidates."""
        return await self.get_screener_results("ta_overbought")

    async def get_unusual_volume(self) -> list:
        """Get stocks with unusual volume — more than 2x average."""
        return await self.get_screener_results("ta_unusualvolume")

    async def get_new_highs(self) -> list:
        """Get stocks hitting new 52-week highs — momentum leaders."""
        return await self.get_screener_results("ta_newhigh")

    async def get_new_lows(self) -> list:
        """Get stocks hitting new 52-week lows — potential turnaround candidates."""
        return await self.get_screener_results("ta_newlow")

    async def get_most_volatile(self) -> list:
        """Get the most volatile stocks today."""
        return await self.get_screener_results("ta_mostvolatile")

    async def get_most_active(self) -> list:
        """Get the most actively traded stocks today."""
        return await self.get_screener_results("ta_mostactive")

    async def get_top_losers(self) -> list:
        """Get biggest losers today — potential bounce plays."""
        return await self.get_screener_results("ta_toplosers")

    async def get_small_cap_gainers(self) -> list:
        """Get small cap stocks (under $2B) with biggest gains today."""
        return await self._custom_screen({
            "f": "cap_smallunder,ta_change_u",
            "o": "-change",
        })

    async def get_penny_stock_gainers(self) -> list:
        """Get stocks under $5 with biggest gains today."""
        return await self._custom_screen({
            "f": "sh_price_u5,ta_change_u",
            "o": "-change",
        })

    async def get_high_short_float(self) -> list:
        """Get stocks with high short interest — squeeze candidates."""
        return await self._custom_screen({
            "f": "sh_short_o20",
            "o": "-shortinterestshare",
        })

    async def get_earnings_this_week(self) -> list:
        """Get stocks with earnings this week."""
        return await self._custom_screen({
            "f": "earningsdate_thisweek",
            "o": "-marketcap",
        })

    async def get_insider_buying(self) -> list:
        """Get stocks with recent insider buying."""
        return await self._custom_screen({
            "f": "it_latestbuys",
            "o": "-change",
        })

    async def get_analyst_upgrades(self) -> list:
        """Get stocks with recent analyst upgrades."""
        return await self._custom_screen({
            "f": "targetprice_a20",
            "o": "-change",
        })

    async def _custom_screen(self, params: dict) -> list:
        """Run a custom Finviz screener with arbitrary filter parameters."""
        try:
            url = "https://finviz.com/screener.ashx"
            all_params = {
                "v": "111",
                **params,
            }
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    url,
                    params=all_params,
                    headers=self.HEADERS,
                    timeout=15,
                )

            if resp.status_code != 200:
                return []

            soup = BeautifulSoup(resp.text, "html.parser")
            table = soup.find("table", class_="screener_table") or soup.find(
                "table", {"id": "screener-views-table"}
            )

            if not table:
                tables = soup.find_all("table")
                for t in tables:
                    if t.find("td", class_="screener-body-table-nw"):
                        table = t
                        break

            if not table:
                return []

            rows = table.find_all("tr")[1:]
            results = []
            for row in rows[:20]:
                cells = row.find_all("td")
                if len(cells) >= 8:
                    ticker = cells[1].get_text(strip=True)
                    if not ticker or len(ticker) > 6:
                        continue
                    results.append({
                        "ticker": ticker,
                        "company": cells[2].get_text(strip=True) if len(cells) > 2 else "",
                        "sector": cells[3].get_text(strip=True) if len(cells) > 3 else "",
                        "industry": cells[4].get_text(strip=True) if len(cells) > 4 else "",
                        "market_cap": cells[6].get_text(strip=True) if len(cells) > 6 else "",
                        "price": cells[8].get_text(strip=True) if len(cells) > 8 else "",
                        "change": cells[9].get_text(strip=True) if len(cells) > 9 else "",
                        "volume": cells[10].get_text(strip=True) if len(cells) > 10 else "",
                    })
            return results
        except Exception as e:
            print(f"Finviz custom screen error: {e}")
            return []