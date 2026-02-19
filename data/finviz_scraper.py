import asyncio
import httpx
from bs4 import BeautifulSoup
from data.cache import cache, FINVIZ_TTL


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
        cache_key = f"finviz:screener:{filters}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"https://finviz.com/screener.ashx?v=111&s={filters}",
                    headers=self.HEADERS,
                    timeout=10,
                )
            soup = BeautifulSoup(resp.text, "html.parser")

            # Finviz table structure — find screener rows
            rows = soup.select("tr.styled-row")
            if not rows:
                rows = soup.select(
                    "table.screener_table tr.screener-body-table-nw"
                )

            results = []
            for row in rows[:60]:
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
            cache.set(cache_key, results, FINVIZ_TTL)
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

    async def _custom_screen(self, params) -> list:
        """Run a custom Finviz screener with arbitrary filter parameters.
        Accepts either a dict of params or a URL query string like 'v=111&f=...'
        """
        if isinstance(params, str):
            cache_key = f"finviz:custom:{params[:100]}"
        else:
            cache_key = f"finviz:custom:{str(sorted(params.items()))[:100]}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            if isinstance(params, str):
                url = f"https://finviz.com/screener.ashx?{params}"
                print(f"[Finviz] Custom screen URL: {url}")
                async with httpx.AsyncClient() as client:
                    resp = await client.get(
                        url,
                        headers=self.HEADERS,
                        timeout=10,
                    )
            else:
                url = "https://finviz.com/screener.ashx"
                all_params = {
                    "v": "111",
                    **params,
                }
                print(f"[Finviz] Custom screen params: {all_params}")
                async with httpx.AsyncClient() as client:
                    resp = await client.get(
                        url,
                        params=all_params,
                        headers=self.HEADERS,
                        timeout=10,
                    )

            if resp.status_code != 200:
                print(f"[Finviz] Custom screen HTTP {resp.status_code}")
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
                for t in soup.find_all("table"):
                    rows = t.find_all("tr")
                    if len(rows) > 1:
                        first_row_cells = rows[0].find_all("td")
                        if any("Ticker" in (c.get_text(strip=True) or "") for c in first_row_cells):
                            table = t
                            break

            if not table:
                title = soup.find("title")
                page_title = title.get_text(strip=True) if title else "unknown"
                print(f"[Finviz] No screener table found. Page title: {page_title}. HTML length: {len(resp.text)}")
                body_text = soup.get_text()[:500] if soup.body else ""
                if "No matches" in body_text or "0 Total" in body_text:
                    print("[Finviz] Page indicates no matches for this filter combination")
                return []

            header_row = table.find("tr")
            headers = []
            if header_row:
                for cell in header_row.find_all("td"):
                    headers.append(cell.get_text(strip=True).lower())

            is_technical = "rsi" in headers or "sma20" in headers

            header_map = {}
            for i, h in enumerate(headers):
                header_map[h] = i

            rows = table.find_all("tr")[1:]
            results = []
            for row in rows[:60]:
                cells = row.find_all("td")
                if len(cells) >= 8:
                    ticker_idx = header_map.get("ticker", 1)
                    ticker = cells[ticker_idx].get_text(strip=True) if len(cells) > ticker_idx else ""
                    if not ticker or len(ticker) > 6:
                        continue

                    item = {
                        "ticker": ticker,
                        "company": cells[header_map.get("company", 2)].get_text(strip=True) if "company" in header_map and len(cells) > header_map["company"] else "",
                        "sector": cells[header_map.get("sector", 3)].get_text(strip=True) if "sector" in header_map and len(cells) > header_map["sector"] else "",
                        "industry": cells[header_map.get("industry", 4)].get_text(strip=True) if "industry" in header_map and len(cells) > header_map["industry"] else "",
                        "market_cap": cells[header_map.get("market cap", 6)].get_text(strip=True) if "market cap" in header_map and len(cells) > header_map["market cap"] else "",
                        "price": cells[header_map.get("price", 8)].get_text(strip=True) if "price" in header_map and len(cells) > header_map["price"] else "",
                        "change": cells[header_map.get("change", 9)].get_text(strip=True) if "change" in header_map and len(cells) > header_map["change"] else "",
                        "volume": cells[header_map.get("volume", 10)].get_text(strip=True) if "volume" in header_map and len(cells) > header_map["volume"] else "",
                    }

                    if is_technical:
                        for tech_field in ["rsi", "sma20", "sma50", "sma200", "rel volume", "avg volume", "perf week", "perf month", "perf quart", "perf half", "perf year", "volatility"]:
                            if tech_field in header_map and len(cells) > header_map[tech_field]:
                                key = tech_field.replace(" ", "_")
                                item[key] = cells[header_map[tech_field]].get_text(strip=True)

                    results.append(item)
            print(f"[Finviz] Custom screen returned {len(results)} results")
            cache.set(cache_key, results, FINVIZ_TTL)
            return results
        except Exception as e:
            import traceback
            print(f"[Finviz] Custom screen error: {e}")
            traceback.print_exc()
            return []

    async def get_stage2_breakouts(self) -> list:
        """
        Stocks breaking out above the 200-day SMA with volume surge.
        This is the Weinstein Stage 2 breakout screen.
        Filters: Price above SMA 200, new high, relative volume > 2x, up today
        """
        return await self._custom_screen(
            "v=111&f=sh_avgvol_o500,sh_relvol_o2,ta_highlow52w_nh,ta_sma200_pa&ft=4&o=-change"
        )

    async def get_macd_crossovers(self) -> list:
        """
        Stocks with recent bullish MACD crossover + positive momentum.
        Signal line just crossed above — early momentum signal.
        """
        return await self._custom_screen(
            "v=111&f=sh_avgvol_o300,ta_change_u,ta_signal_buy&ft=4&o=-change"
        )

    async def get_rsi_recovery(self) -> list:
        """
        Stocks recovering from oversold (RSI was <30, now moving up).
        Bounce play candidates.
        """
        return await self._custom_screen(
            "v=111&f=sh_avgvol_o300,ta_rsi_os40,ta_change_u&ft=4&o=-change"
        )

    async def get_volume_breakouts(self) -> list:
        """
        Stocks with massive volume surge (3x+) AND price increase.
        Volume precedes price — these are early-stage moves.
        """
        return await self._custom_screen(
            "v=111&f=sh_avgvol_o200,sh_relvol_o3,ta_change_u5&ft=4&o=-relvol"
        )

    async def get_sma_crossover_stocks(self) -> list:
        """
        Stocks where price just crossed above the 50-day SMA.
        Medium-term trend change signal.
        """
        return await self._custom_screen(
            "v=111&f=sh_avgvol_o300,ta_sma50_pa,ta_change_u&ft=4&o=-change"
        )

    async def get_small_cap_momentum(self) -> list:
        """
        Small caps (under $2B) with volume surge + price increase + above SMA 20.
        Your bread and butter for trading.
        """
        return await self._custom_screen(
            "v=111&f=cap_smallunder,sh_avgvol_o200,sh_relvol_o2,ta_sma20_pa,ta_change_u&ft=4&o=-change"
        )

    async def get_gap_up_volume(self) -> list:
        """
        Stocks gapping up today on high volume.
        Potential catalyst-driven moves.
        """
        return await self._custom_screen(
            "v=111&f=sh_avgvol_o500,sh_relvol_o2,ta_change_u3&ft=4&o=-change"
        )

    async def get_consolidation_breakouts(self) -> list:
        """
        Stocks breaking out of tight consolidation (low volatility -> expansion).
        Bollinger Band squeeze breakouts.
        """
        return await self._custom_screen(
            "v=111&f=sh_avgvol_o300,ta_change_u3,ta_volatility_wo3&ft=4&o=-change"
        )

    async def get_accumulation_stocks(self) -> list:
        """
        Stocks showing accumulation pattern: up on above-average volume over multiple days.
        Institutional buying signal.
        """
        return await self._custom_screen(
            "v=111&f=sh_avgvol_o500,sh_relvol_o1.5,ta_change_u,ta_sma20_pa,ta_sma50_pa&ft=4&o=-relvol"
        )

    async def get_small_cap_squeeze_setups(self) -> list:
        """
        Small caps with high short interest + volume surge.
        Squeeze candidates specifically.
        """
        return await self._custom_screen(
            "v=111&f=cap_smallunder,sh_avgvol_o200,sh_relvol_o2,sh_short_o15,ta_change_u&ft=4&o=-change"
        )

    async def get_revenue_growth_leaders(self) -> list:
        """Stocks with high revenue growth (>25% YoY) and positive price action."""
        return await self._custom_screen(
            "v=111&f=fa_salesqoq_o25,sh_avgvol_o300,ta_change_u&ft=4&o=-fa_salesqoq"
        )

    async def get_earnings_growth_leaders(self) -> list:
        """Stocks with strong EPS growth and positive momentum."""
        return await self._custom_screen(
            "v=111&f=fa_epsqoq_o25,sh_avgvol_o300,ta_change_u&ft=4&o=-fa_epsqoq"
        )

    async def get_profitable_growth(self) -> list:
        """Growing stocks that are actually profitable (positive margins + growth)."""
        return await self._custom_screen(
            "v=111&f=fa_epsqoq_o15,fa_opermargin_pos,fa_salesqoq_o15,sh_avgvol_o300&ft=4&o=-fa_salesqoq"
        )

    async def get_low_ps_high_growth(self) -> list:
        """Undervalued on P/S but growing fast — asymmetric value + growth."""
        return await self._custom_screen(
            "v=111&f=fa_ps_u5,fa_salesqoq_o20,sh_avgvol_o200&ft=4&o=-fa_salesqoq"
        )

    async def get_ebitda_positive_turn(self) -> list:
        """Stocks with positive operating margins that recently turned profitable."""
        return await self._custom_screen(
            "v=111&f=fa_opermargin_pos,fa_salesqoq_o10,sh_avgvol_o200,ta_change_u&ft=4&o=-fa_salesqoq"
        )

    async def get_low_debt_growth(self) -> list:
        """Growing companies with low debt — financially healthy growers."""
        return await self._custom_screen(
            "v=111&f=fa_debteq_u0.5,fa_salesqoq_o15,sh_avgvol_o200&ft=4&o=-fa_salesqoq"
        )

    async def get_institutional_accumulation(self) -> list:
        """Stocks with increasing institutional ownership + positive momentum."""
        return await self._custom_screen(
            "v=111&f=sh_avgvol_o500,sh_instown_o60,ta_change_u,ta_sma50_pa&ft=4&o=-change"
        )

    async def get_breaking_below_200sma(self) -> list:
        """Stocks breaking below 200 SMA — Stage 3/4 breakdown candidates."""
        return await self._custom_screen(
            "v=111&f=sh_avgvol_o300,ta_sma200_pb,ta_change_d&ft=4&o=change"
        )

    async def get_declining_earnings(self) -> list:
        """Stocks with declining EPS — fundamental deterioration."""
        return await self._custom_screen(
            "v=111&f=fa_epsqoq_d,sh_avgvol_o300&ft=4&o=fa_epsqoq"
        )

    async def get_high_short_declining(self) -> list:
        """High short interest + price declining — shorts are winning."""
        return await self._custom_screen(
            "v=111&f=sh_avgvol_o300,sh_short_o10,ta_change_d,ta_sma50_pb&ft=4&o=-sh_short"
        )


async def scrape_yahoo_trending() -> list:
    """
    Scrape Yahoo Finance trending tickers page.
    Returns list of dicts with ticker, company name, price, change.
    This represents mainstream retail attention — different audience
    from StockTwits (active traders) or Finviz (screener users).
    """
    import httpx
    from bs4 import BeautifulSoup
    from data.cache import cache

    cache_key = "yahoo:trending"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                "https://finance.yahoo.com/trending-tickers/",
                headers=headers,
                timeout=10,
                follow_redirects=True,
            )
        if resp.status_code != 200:
            print(f"Yahoo trending scrape failed: {resp.status_code}")
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        results = []

        rows = soup.find_all("tr")
        for row in rows[:25]:
            cells = row.find_all("td")
            if len(cells) >= 5:
                ticker_link = cells[0].find("a")
                ticker = ticker_link.get_text(strip=True) if ticker_link else ""
                company = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                price = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                change = cells[4].get_text(strip=True) if len(cells) > 4 else ""

                if ticker and len(ticker) <= 6 and ticker.isalpha():
                    results.append({
                        "ticker": ticker.upper(),
                        "company": company,
                        "price": price,
                        "change": change,
                        "source": "yahoo_trending",
                    })

        cache.set(cache_key, results, 300)
        return results

    except Exception as e:
        print(f"Yahoo trending scrape error: {e}")
        return []


async def scrape_stockanalysis_trending() -> list:
    """
    Get most active / trending stocks via FMP API (gainers + actives).
    Previously scraped StockAnalysis HTML but those pages are JS-rendered.
    Now uses FMP stock_market endpoints which return clean JSON.
    Returns list of dicts with ticker, company, source.
    """
    from data.cache import cache
    from config import FMP_API_KEY

    cache_key = "stockanalysis:trending"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    if not FMP_API_KEY:
        print("[StockAnalysis/FMP trending] No FMP_API_KEY, skipping")
        return []

    try:
        import httpx
        results = []
        seen = set()
        base = "https://financialmodelingprep.com/api/v3"

        async with httpx.AsyncClient(timeout=10) as client:
            gainers_resp, actives_resp = await asyncio.gather(
                client.get(f"{base}/stock_market/gainers", params={"apikey": FMP_API_KEY}),
                client.get(f"{base}/stock_market/actives", params={"apikey": FMP_API_KEY}),
                return_exceptions=True,
            )

        for resp in [gainers_resp, actives_resp]:
            if isinstance(resp, Exception) or resp.status_code != 200:
                continue
            for item in (resp.json() or [])[:20]:
                if not isinstance(item, dict):
                    continue
                ticker = (item.get("symbol") or "").upper().strip()
                if ticker and len(ticker) <= 6 and ticker.isalpha() and ticker not in seen:
                    seen.add(ticker)
                    results.append({
                        "ticker": ticker,
                        "company": item.get("name", ""),
                        "price": str(item.get("price", "")),
                        "change": f"{item.get('changesPercentage', 0):+.2f}%",
                        "source": "stockanalysis",
                    })

        print(f"[FMP trending] Got {len(results)} trending tickers from FMP gainers+actives")
        cache.set(cache_key, results, 300)
        return results

    except Exception as e:
        print(f"FMP trending fetch error: {e}")
        return []