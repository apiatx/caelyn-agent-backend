import httpx
from data.cache import cache, FMP_TTL

class FMPProvider:
    """
    Financial Modeling Prep API provider.
    Free tier: 250 calls/day, end-of-day data.
    Covers: DXY, oil, gold, commodities, sector ETFs, economic calendar,
    forex, indices, and financial statements.
    """

    BASE_URL = "https://financialmodelingprep.com/api/v3"

    def __init__(self, api_key: str):
        self.api_key = api_key

    async def _get(self, endpoint: str, params: dict = None) -> dict | list:
        """Make a GET request to FMP API."""
        cache_key = f"fmp:{endpoint}:{str(params)[:80]}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        if params is None:
            params = {}
        params["apikey"] = self.api_key
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"{self.BASE_URL}/{endpoint}",
                    params=params,
                    timeout=15,
                )
            if resp.status_code != 200:
                print(f"FMP error {resp.status_code}: {endpoint}")
                return []
            result = resp.json()
            cache.set(cache_key, result, FMP_TTL)
            return result
        except Exception as e:
            print(f"FMP request failed ({endpoint}): {e}")
            return []

    async def get_forex_quotes(self) -> list:
        """Get real-time forex quotes including DXY."""
        return await self._get("quotes/forex")

    async def get_dxy(self) -> dict:
        """Get US Dollar Index (DXY) quote."""
        data = await self._get("quote/DX-Y.NYB")
        if data and len(data) > 0:
            d = data[0]
            return {
                "symbol": "DXY",
                "price": d.get("price"),
                "change": d.get("change"),
                "change_pct": d.get("changesPercentage"),
                "day_high": d.get("dayHigh"),
                "day_low": d.get("dayLow"),
                "year_high": d.get("yearHigh"),
                "year_low": d.get("yearLow"),
                "prev_close": d.get("previousClose"),
            }
        return {"symbol": "DXY", "error": "No data"}

    async def get_commodity_quotes(self) -> list:
        """Get all commodity quotes (oil, gold, silver, etc.)."""
        return await self._get("quotes/commodity")

    async def get_key_commodities(self) -> dict:
        """Get prices for key commodities: oil, gold, silver, natural gas."""
        symbols = "CLUSD,GCUSD,SIUSD,NGUSD,HGUSD"
        data = await self._get(f"quote/{symbols}")
        result = {}
        name_map = {
            "CLUSD": "Crude Oil (WTI)",
            "GCUSD": "Gold",
            "SIUSD": "Silver",
            "NGUSD": "Natural Gas",
            "HGUSD": "Copper",
        }
        for item in (data or []):
            symbol = item.get("symbol", "")
            result[symbol] = {
                "name": name_map.get(symbol, symbol),
                "price": item.get("price"),
                "change": item.get("change"),
                "change_pct": item.get("changesPercentage"),
                "day_high": item.get("dayHigh"),
                "day_low": item.get("dayLow"),
            }
        return result

    async def get_sector_performance(self) -> list:
        """Get real-time sector performance (S&P 500 sectors)."""
        return await self._get("sectors-performance")

    async def get_sector_performance_historical(self) -> list:
        """Get historical sector performance."""
        return await self._get("historical-sectors-performance")

    async def get_etf_quotes(self, symbols: list) -> dict:
        """Get quotes for a list of ETF symbols."""
        symbols_str = ",".join(symbols)
        data = await self._get(f"quote/{symbols_str}")
        result = {}
        for item in (data or []):
            sym = item.get("symbol", "")
            result[sym] = {
                "price": item.get("price"),
                "change": item.get("change"),
                "change_pct": item.get("changesPercentage"),
                "volume": item.get("volume"),
                "avg_volume": item.get("avgVolume"),
                "day_high": item.get("dayHigh"),
                "day_low": item.get("dayLow"),
                "year_high": item.get("yearHigh"),
                "year_low": item.get("yearLow"),
                "pe": item.get("pe"),
                "market_cap": item.get("marketCap"),
            }
        return result

    async def get_sector_etf_snapshot(self) -> dict:
        """
        Get a complete sector rotation snapshot using sector ETFs.
        Returns performance data for all major sector ETFs.
        """
        sector_etfs = [
            "XLK", "XLV", "XLF", "XLE", "XLI", "XLP", "XLY",
            "XLB", "XLU", "XLRE", "XLC",
            "SPY", "QQQ", "IWM", "DIA",
            "SMH", "URA", "HACK", "XBI", "GDX", "XOP",
        ]
        quotes = await self.get_etf_quotes(sector_etfs)
        sector_perf = await self.get_sector_performance()

        return {
            "etf_quotes": quotes,
            "sector_performance": sector_perf,
        }

    async def get_economic_calendar(self, from_date: str = None, to_date: str = None) -> list:
        """
        Get upcoming economic events (CPI, PPI, FOMC, NFP, etc.).
        Dates in YYYY-MM-DD format.
        """
        params = {}
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date
        return await self._get("economic_calendar", params)

    async def get_upcoming_economic_events(self) -> list:
        """Get economic events for the next 7 days."""
        from datetime import datetime, timedelta
        today = datetime.now().strftime("%Y-%m-%d")
        next_week = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
        events = await self.get_economic_calendar(today, next_week)

        important_keywords = [
            "CPI", "PPI", "FOMC", "Fed", "Interest Rate", "NFP",
            "Non-Farm", "GDP", "Unemployment", "Retail Sales",
            "Consumer Confidence", "PMI", "ISM", "PCE",
            "Jobless Claims", "Housing", "Durable Goods",
        ]
        important_events = []
        other_events = []

        for event in (events or []):
            country = event.get("country", "")
            event_name = event.get("event", "")
            if country == "US":
                is_important = any(
                    kw.lower() in event_name.lower()
                    for kw in important_keywords
                )
                formatted = {
                    "date": event.get("date"),
                    "event": event_name,
                    "country": country,
                    "actual": event.get("actual"),
                    "previous": event.get("previous"),
                    "estimate": event.get("estimate"),
                    "impact": event.get("impact", ""),
                    "is_high_impact": is_important,
                }
                if is_important:
                    important_events.append(formatted)
                else:
                    other_events.append(formatted)

        return {
            "high_impact_events": important_events[:15],
            "other_us_events": other_events[:10],
        }

    async def get_market_indices(self) -> dict:
        """Get major market index quotes."""
        symbols = "^GSPC,^DJI,^IXIC,^RUT,^VIX"
        data = await self._get(f"quote/{symbols}")
        result = {}
        name_map = {
            "^GSPC": "S&P 500",
            "^DJI": "Dow Jones",
            "^IXIC": "Nasdaq",
            "^RUT": "Russell 2000",
            "^VIX": "VIX",
        }
        for item in (data or []):
            sym = item.get("symbol", "")
            result[sym] = {
                "name": name_map.get(sym, sym),
                "price": item.get("price"),
                "change": item.get("change"),
                "change_pct": item.get("changesPercentage"),
            }
        return result

    async def get_treasury_rates(self) -> dict:
        """Get current Treasury yields."""
        data = await self._get("treasury")
        if data and len(data) > 0:
            latest = data[0]
            return {
                "date": latest.get("date"),
                "month_1": latest.get("month1"),
                "month_3": latest.get("month3"),
                "month_6": latest.get("month6"),
                "year_1": latest.get("year1"),
                "year_2": latest.get("year2"),
                "year_5": latest.get("year5"),
                "year_10": latest.get("year10"),
                "year_20": latest.get("year20"),
                "year_30": latest.get("year30"),
            }
        return {}

    async def get_macro_market_data(self) -> dict:
        """
        Full macro market data snapshot:
        DXY, oil, gold, treasuries, indices, sector performance.
        Uses ~8 API calls.
        """
        import asyncio
        dxy, commodities, indices, treasuries, sector_perf, econ_events = (
            await asyncio.gather(
                self.get_dxy(),
                self.get_key_commodities(),
                self.get_market_indices(),
                self.get_treasury_rates(),
                self.get_sector_performance(),
                self.get_upcoming_economic_events(),
                return_exceptions=True,
            )
        )

        return {
            "dxy": dxy if not isinstance(dxy, Exception) else {},
            "commodities": commodities if not isinstance(commodities, Exception) else {},
            "indices": indices if not isinstance(indices, Exception) else {},
            "treasury_yields": treasuries if not isinstance(treasuries, Exception) else {},
            "sector_performance": sector_perf if not isinstance(sector_perf, Exception) else [],
            "economic_calendar": econ_events if not isinstance(econ_events, Exception) else {},
        }

    async def get_full_commodity_dashboard(self) -> dict:
        """
        Comprehensive commodity market snapshot:
        All major commodities with prices, changes, and context.
        Uses ~5 API calls.
        """
        import asyncio

        all_commodities, key_commodities, energy_etfs, metal_etfs, agri_etfs = (
            await asyncio.gather(
                self.get_commodity_quotes(),
                self.get_key_commodities(),
                self.get_etf_quotes(["XLE", "XOP", "OIH", "UNG", "USO", "URA"]),
                self.get_etf_quotes(["GLD", "SLV", "GDX", "GDXJ", "COPX", "PPLT"]),
                self.get_etf_quotes(["DBA", "CORN", "WEAT", "SOYB", "MOO", "COW"]),
                return_exceptions=True,
            )
        )

        return {
            "all_commodities": all_commodities if not isinstance(all_commodities, Exception) else [],
            "key_commodities": key_commodities if not isinstance(key_commodities, Exception) else {},
            "energy_etfs": energy_etfs if not isinstance(energy_etfs, Exception) else {},
            "metals_etfs": metal_etfs if not isinstance(metal_etfs, Exception) else {},
            "agriculture_etfs": agri_etfs if not isinstance(agri_etfs, Exception) else {},
        }

    async def get_commodity_historical(self, symbol: str, days: int = 30) -> list:
        """Get historical daily prices for a commodity."""
        from datetime import datetime, timedelta
        to_date = datetime.now().strftime("%Y-%m-%d")
        from_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        data = await self._get(
            f"historical-price-full/{symbol}",
            {"from": from_date, "to": to_date},
        )
        if isinstance(data, dict) and "historical" in data:
            return data["historical"][:days]
        return []
