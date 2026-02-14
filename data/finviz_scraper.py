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