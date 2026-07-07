import asyncio
import httpx
from data.cache import cache, FMP_TTL



class FMPProvider:
    """
    Financial Modeling Prep API provider — Starter plan, stable API.
    All endpoints use https://financialmodelingprep.com/stable (v3 is legacy/403).
    Confirmed working: quote, profile, stock-peers, biggest-gainers/losers,
    most-actives, news/stock, news/general-latest, economic-calendar,
    treasury-rates, ETF/index quotes.
    NOT available on Starter: earnings-surprises, per-ticker earnings calendar,
    sector-performance, commodity futures (GC=F), DXY (premium symbol).
    """

    STABLE_URL = "https://financialmodelingprep.com/stable"

    def __init__(self, api_key: str):
        self.api_key = api_key
    async def _get_stable(self, endpoint: str, params: dict = None) -> dict | list:
        """Make a GET request to FMP stable API."""
        cache_key = f"fmp:stable:{endpoint}:{str(params)[:80]}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        if params is None:
            params = {}
        params["apikey"] = self.api_key
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"{self.STABLE_URL}/{endpoint}",
                    params=params,
                    timeout=10,
                )
            if resp.status_code not in (200, 201):
                if resp.status_code not in (403, 402, 404):
                    print(f"[FMP] stable/{endpoint} HTTP {resp.status_code}")
                return []
            result = resp.json()
            if isinstance(result, list) and result:
                cache.set(cache_key, result, FMP_TTL)
            elif isinstance(result, dict) and result:
                cache.set(cache_key, result, FMP_TTL)
            return result
        except Exception as e:
            print(f"[FMP] stable/{endpoint} failed: {e}")
            return []
    async def get_quote(self, symbol: str) -> dict:
        """Single symbol quote. Returns backward-compat keys matching v3 shape."""
        data = await self._get_stable("quote", {"symbol": symbol.upper()})
        if data and isinstance(data, list) and len(data) > 0:
            item = data[0]
            price = item.get("price")
            change = item.get("change")
            prev_close = item.get("previousClose")
            if prev_close is None and price is not None and change is not None:
                try:
                    prev_close = round(price - change, 4)
                except (TypeError, ValueError):
                    prev_close = None
            return {
                "price": price,
                "change": change,
                "changesPercentage": item.get("changePercentage"),
                "previousClose": prev_close,
                "volume": item.get("volume"),
                "dayHigh": item.get("dayHigh"),
                "dayLow": item.get("dayLow"),
                "yearHigh": item.get("yearHigh"),
                "yearLow": item.get("yearLow"),
                "marketCap": item.get("marketCap"),
            }
        return {}
    async def get_gainers_losers(self) -> dict:
        """Get top gaining and losing stocks today (combined)."""
        gainers, losers = await asyncio.gather(
            self.get_stock_market_gainers(),
            self.get_stock_market_losers(),
            return_exceptions=True,
        )
        return {
            "gainers": gainers if not isinstance(gainers, Exception) else [],
            "losers": losers if not isinstance(losers, Exception) else [],
        }
    async def get_stock_market_gainers(self) -> list:
        """Get top gaining stocks today."""
        data = await self._get_stable("biggest-gainers")
        results = []
        for item in (data or [])[:20]:
            if isinstance(item, dict):
                results.append({
                    "ticker": item.get("symbol", ""),
                    "company": item.get("name", ""),
                    "price": str(item.get("price", "")),
                    "change": f"{item.get('changesPercentage', 0):+.2f}%",
                    "source": "fmp_gainers",
                })
        return results
    async def get_stock_market_losers(self) -> list:
        """Get top losing stocks today."""
        data = await self._get_stable("biggest-losers")
        results = []
        for item in (data or [])[:20]:
            if isinstance(item, dict):
                results.append({
                    "ticker": item.get("symbol", ""),
                    "company": item.get("name", ""),
                    "price": str(item.get("price", "")),
                    "change": f"{item.get('changesPercentage', 0):+.2f}%",
                    "source": "fmp_losers",
                })
        return results
    async def get_stock_market_actives(self) -> list:
        """Get most active stocks by volume today."""
        data = await self._get_stable("most-actives")
        results = []
        for item in (data or [])[:20]:
            if isinstance(item, dict):
                results.append({
                    "ticker": item.get("symbol", ""),
                    "company": item.get("name", ""),
                    "price": str(item.get("price", "")),
                    "change": f"{item.get('changesPercentage', 0):+.2f}%",
                    "volume": str(item.get("volume", "")),
                    "source": "fmp_actives",
                })
        return results
    async def get_market_news(self, limit: int = 20) -> list:
        """Get general market news."""
        data = await self._get_stable("news/general-latest", {"limit": limit})
        if not isinstance(data, list):
            return []
        results = []
        for item in data[:limit]:
            if isinstance(item, dict):
                results.append({
                    "title": item.get("title", ""),
                    "text": (item.get("text") or "")[:200],
                    "symbol": item.get("symbol", ""),
                    "source": item.get("site", "") or item.get("publisher", ""),
                    "published": item.get("publishedDate", ""),
                    "url": item.get("url", ""),
                })
        return results
    async def get_stock_news(self, ticker: str, limit: int = 5) -> list:
        """Get news for a specific stock."""
        data = await self._get_stable("news/stock", {"symbols": ticker.upper(), "limit": limit})
        if not isinstance(data, list):
            return []
        results = []
        for item in data[:limit]:
            if isinstance(item, dict):
                results.append({
                    "title": item.get("title", ""),
                    "text": (item.get("text") or "")[:200],
                    "symbol": item.get("symbol", ""),
                    "source": item.get("site", "") or item.get("publisher", ""),
                    "published": item.get("publishedDate", ""),
                    "url": item.get("url", ""),
                })
        return results
    async def get_etf_flag(self, ticker: str) -> str:
        """
        Classify a ticker as 'etf', 'stock', or 'unknown' using FMP /v3/profile.
        Called only from background tasks (options_instrument_type_service).
        Never call from request handlers.
        """
        try:
            data = await self._get_stable("profile", {"symbol": ticker.upper()})
            if data and isinstance(data, list) and len(data) > 0:
                item = data[0]
                if item.get("isEtf"):
                    return "etf"
                if item.get("companyName"):
                    return "stock"
            return "unknown"
        except Exception:
            return "unknown"

    async def get_company_profile(self, ticker: str) -> dict:
        """
        Get company profile from FMP stable API.
        Returns keys matching Finnhub profile format for compatibility.
        """
        data = await self._get_stable("profile", {"symbol": ticker.upper()})
        if data and isinstance(data, list) and len(data) > 0:
            item = data[0]
            return {
                "name": item.get("companyName", ""),
                "sector": item.get("sector", ""),
                "industry": item.get("industry", ""),
                "market_cap": item.get("marketCap"),
                "avg_volume": item.get("volAvg"),
                "logo": item.get("image", ""),
                "exchange": item.get("exchange", ""),
                "ipo_date": item.get("ipoDate", ""),
                "country": item.get("country", ""),
                "web_url": item.get("website", ""),
                "description": item.get("description", ""),
            }
        return {}
    async def get_stock_peers(self, ticker: str) -> list:
        """
        Get peer companies for a stock.
        Returns list of ticker symbol strings (same contract as Finnhub get_company_peers).
        """
        data = await self._get_stable("stock-peers", {"symbol": ticker.upper()})
        if not isinstance(data, list):
            return []
        symbols = []
        for item in data:
            if isinstance(item, dict):
                sym = item.get("symbol", "")
                if sym:
                    symbols.append(sym)
        return symbols
    async def get_earnings_history(self, ticker: str, limit: int = 8) -> list:
        """
        Per-ticker earnings history + upcoming via stable/earnings.
        Returns both historical (epsActual populated) and upcoming (epsActual=null).
        Normalized into a consistent shape for downstream consumption.

        Fields: ticker, date, eps_estimate, eps_actual, revenue_estimate,
                revenue_actual, surprise_pct, report_available, source.
        """
        data = await self._get_stable("earnings", {"symbol": ticker.upper(), "limit": limit})
        if not isinstance(data, list):
            return []
        results = []
        for item in data:
            if not isinstance(item, dict):
                continue
            eps_actual = item.get("epsActual")
            eps_est = item.get("epsEstimated")
            rev_actual = item.get("revenueActual")
            surprise_pct = None
            if eps_actual is not None and eps_est is not None and eps_est != 0:
                try:
                    surprise_pct = round((eps_actual - eps_est) / abs(eps_est) * 100, 2)
                except (TypeError, ZeroDivisionError):
                    surprise_pct = None
            results.append({
                "ticker": item.get("symbol", ticker.upper()),
                "date": item.get("date"),
                "eps_estimate": eps_est,
                "eps_actual": eps_actual,
                "revenue_estimate": item.get("revenueEstimated"),
                "revenue_actual": rev_actual,
                "surprise_pct": surprise_pct,
                "report_available": eps_actual is not None,
                "source": "fmp",
            })
        return results
    async def get_income_statement(self, ticker: str, limit: int = 4, period: str = "quarter") -> list:
        """
        Per-ticker income statement (quarterly or annual) via stable/income-statement.
        Returns key financial fields useful for earnings enrichment.
        period: 'quarter' or 'annual'
        """
        data = await self._get_stable(
            "income-statement",
            {"symbol": ticker.upper(), "limit": limit, "period": period},
        )
        if not isinstance(data, list):
            return []
        results = []
        for item in data:
            if not isinstance(item, dict):
                continue
            results.append({
                "ticker": item.get("symbol", ticker.upper()),
                "date": item.get("date"),
                "fiscal_year": item.get("fiscalYear"),
                "period": item.get("period"),
                "revenue": item.get("revenue"),
                "gross_profit": item.get("grossProfit"),
                "operating_income": item.get("operatingIncome"),
                "ebitda": item.get("ebitda"),
                "net_income": item.get("netIncome"),
                "eps": item.get("eps"),
                "eps_diluted": item.get("epsDiluted"),
                "source": "fmp",
            })
        return results
    async def get_earnings_enrichment(self, ticker: str) -> dict:
        """
        Hybrid earnings enrichment object: combines stable/earnings (event-level EPS+revenue)
        with stable/income-statement (quarterly P&L context).
        Used as an additive enrichment alongside Finnhub calendar/surprise data.
        """
        t = ticker.upper()
        earnings_data, income_data = await asyncio.gather(
            self.get_earnings_history(t, limit=8),
            self.get_income_statement(t, limit=4, period="quarter"),
            return_exceptions=True,
        )
        return {
            "ticker": t,
            "earnings_history": earnings_data if isinstance(earnings_data, list) else [],
            "income_statements": income_data if isinstance(income_data, list) else [],
            "source": "fmp_stable",
        }
    async def get_forex_quotes(self) -> list:
        """Get forex quotes. Returns empty list — DXY requires premium plan."""
        return []
    async def get_dxy(self) -> dict:
        """
        Get US Dollar Index approximation.
        DX-Y.NYB requires FMP premium. Returns empty dict — FRED covers this.
        """
        return {"symbol": "DXY", "error": "Premium symbol — use FRED"}
    async def get_commodity_quotes(self) -> list:
        """
        Get commodity quotes via ETF proxies (futures not available on Starter).
        GLD=Gold, SLV=Silver, USO=Oil, UNG=NatGas, COPX=Copper.
        """
        proxy_symbols = ["GLD", "SLV", "USO", "UNG", "COPX"]
        tasks = [self.get_quote(s) for s in proxy_symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        out = []
        name_map = {
            "GLD": "Gold (ETF)", "SLV": "Silver (ETF)",
            "USO": "Crude Oil (ETF)", "UNG": "Nat Gas (ETF)",
            "COPX": "Copper (ETF)",
        }
        for sym, res in zip(proxy_symbols, results):
            if isinstance(res, dict) and res.get("price"):
                out.append({
                    "symbol": sym,
                    "name": name_map.get(sym, sym),
                    "price": res.get("price"),
                    "change": res.get("change"),
                    "changesPercentage": res.get("changesPercentage"),
                })
        return out
    async def get_key_commodities(self) -> dict:
        """
        Key commodity prices via ETF proxies (futures not on Starter plan).
        """
        symbol_map = {
            "GLD": "Gold",
            "USO": "Crude Oil (WTI)",
            "SLV": "Silver",
            "UNG": "Natural Gas",
            "COPX": "Copper",
        }
        tasks = [self.get_quote(s) for s in symbol_map]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        result = {}
        for sym, res in zip(symbol_map, results):
            if isinstance(res, dict) and res.get("price"):
                result[sym] = {
                    "name": symbol_map[sym],
                    "price": res.get("price"),
                    "change": res.get("change"),
                    "change_pct": res.get("changesPercentage"),
                    "day_high": res.get("dayHigh"),
                    "day_low": res.get("dayLow"),
                }
        return result
    async def get_sector_performance(self) -> list:
        """Sector performance — not available on FMP Starter stable API. Returns []."""
        return []
    async def get_sector_performance_historical(self) -> list:
        """Historical sector performance — not available on Starter. Returns []."""
        return []
    async def get_etf_quotes(self, symbols: list) -> dict:
        """
        Get quotes for a list of ETF/stock symbols.
        Uses individual stable/quote calls (batch not supported on Starter).
        """
        tasks = [self.get_quote(s) for s in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        result = {}
        for sym, res in zip(symbols, results):
            if isinstance(res, dict) and not isinstance(res, Exception) and res.get("price"):
                result[sym] = {
                    "price": res.get("price"),
                    "change": res.get("change"),
                    "change_pct": res.get("changesPercentage"),
                    "volume": res.get("volume"),
                    "day_high": res.get("dayHigh"),
                    "day_low": res.get("dayLow"),
                    "year_high": res.get("yearHigh"),
                    "year_low": res.get("yearLow"),
                    "market_cap": res.get("marketCap"),
                }
        return result
    async def get_sector_etf_snapshot(self) -> dict:
        """
        Sector rotation snapshot using sector ETFs via stable quote.
        Sector performance time series not available on Starter — returns [].
        """
        sector_etfs = [
            "XLK", "XLV", "XLF", "XLE", "XLI", "XLP", "XLY",
            "XLB", "XLU", "XLRE", "XLC",
            "SPY", "QQQ", "IWM", "DIA",
            "SMH", "URA", "HACK", "XBI", "GDX", "XOP",
        ]
        quotes = await self.get_etf_quotes(sector_etfs)
        return {
            "etf_quotes": quotes,
            "sector_performance": [],
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
        return await self._get_stable("economic-calendar", params)
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
        """Get major market index quotes via stable/quote."""
        symbol_map = {
            "^GSPC": "S&P 500",
            "^DJI": "Dow Jones",
            "^IXIC": "Nasdaq",
            "^RUT": "Russell 2000",
            "^VIX": "VIX",
        }
        tasks = [self.get_quote(s) for s in symbol_map]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        result = {}
        for sym, res in zip(symbol_map, results):
            name = symbol_map[sym]
            if isinstance(res, dict) and not isinstance(res, Exception):
                result[sym] = {
                    "name": name,
                    "price": res.get("price"),
                    "change": res.get("change"),
                    "change_pct": res.get("changesPercentage"),
                }
        return result
    async def get_treasury_rates(self) -> dict:
        """Get current Treasury yields from stable/treasury-rates."""
        data = await self._get_stable("treasury-rates")
        if data and isinstance(data, list) and len(data) > 0:
            latest = data[0]
            return {
                "date": latest.get("date"),
                "month_1": latest.get("month1"),
                "month_3": latest.get("month3"),
                "month_6": latest.get("month6"),
                "year_1": latest.get("year1"),
                "year_2": latest.get("year2"),
                "year_3": latest.get("year3"),
                "year_5": latest.get("year5"),
                "year_7": latest.get("year7"),
                "year_10": latest.get("year10"),
                "year_20": latest.get("year20"),
                "year_30": latest.get("year30"),
            }
        return {}
    async def get_macro_market_data(self) -> dict:
        """
        Full macro market data snapshot.
        DXY not available on Starter (uses FRED instead).
        """
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
        Commodity market snapshot using ETF proxies (futures not on Starter).
        """
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
    async def get_economic_calendar_nasdaq(self, days_ahead: int = 7) -> list:
        """
        Get upcoming US economic events for the next N days.
        Uses Nasdaq free calendar API — independent of FMP plan tier.
        """
        from datetime import datetime, timedelta

        cache_key = f"econ_calendar:{datetime.now().strftime('%Y-%m-%d')}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        important_keywords = [
            "fed", "fomc", "interest rate", "cpi", "inflation", "ppi",
            "nonfarm", "payroll", "employment", "unemployment", "jobs",
            "gdp", "retail sales", "consumer confidence", "pce",
            "ism", "manufacturing", "housing", "home sales",
            "trade balance", "treasury", "powell",
        ]
        def clean(v):
            if not v:
                return None
            s = str(v).replace("&nbsp;", "").strip()
            return s if s else None

        high_impact = []
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                for day_offset in range(days_ahead):
                    date_str = (datetime.now() + timedelta(days=day_offset)).strftime("%Y-%m-%d")
                    resp = await client.get(
                        f"https://api.nasdaq.com/api/calendar/economicevents?date={date_str}",
                        headers={"User-Agent": "Mozilla/5.0"},
                    )
                    if resp.status_code != 200:
                        continue
                    data = resp.json()
                    rows = data.get("data", {}).get("rows", []) or []
                    for event in rows:
                        country = (event.get("country") or "").lower()
                        name = (event.get("eventName") or "").lower()
                        if "united states" not in country and country != "us":
                            continue
                        is_important = any(kw in name for kw in important_keywords)
                        if is_important:
                            high_impact.append({
                                "event": event.get("eventName", ""),
                                "date": date_str,
                                "time_gmt": event.get("gmt", ""),
                                "previous": clean(event.get("previous")),
                                "consensus": clean(event.get("consensus")),
                                "actual": clean(event.get("actual")),
                            })
                    if day_offset < days_ahead - 1:
                        await asyncio.sleep(0.3)
        except Exception as e:
            print(f"[ECON] Calendar fetch failed: {e}")

        result = high_impact[:20]
        if result:
            cache.set(cache_key, result, FMP_TTL)
        return result
    async def get_commodity_historical(self, symbol: str, days: int = 30) -> list:
        """
        Historical commodity prices — not available on FMP Starter stable API.
        Returns empty list.
        """
        return []

    # ── Security search ───────────────────────────────────────────────────────
    #
    # FMP stable/search-symbol and stable/search-name are the working endpoints.
    # Both return: {symbol, name, currency, exchangeFullName, exchange}
    # where `exchange` is the FMP exchange code (LSE, PAR, XETRA, TSX, …).
    #
    # Canonical ticker construction is delegated to canonical_security_adapter:
    #   fmp_to_canonical()        — FMP exchange code → Caelyn prefix mapping
    #   resolve_with_registry()   — existing Watchlist member wins (Part G)
    #
    # Three distinct identities (Part E):
    #   provider_symbol    IQE.L, SOI.PA, AIXA.DE
    #   provider_exchange  LSE, PAR, XETRA
    #   canonical_ticker   AIM:IQE, EPA:SOI, ETR:AIXA  (Watchlist membership key)

    async def search_securities(self, query: str, limit: int = 25) -> list:
        """
        Search for securities by ticker or company name.

        Calls FMP stable/search-symbol (ticker prefix) and stable/search-name
        (company name) in parallel, then merges and deduplicates by canonical_ticker.

        Canonical identity is resolved via canonical_security_adapter:
          1. Part G registry check — if the security is already known in any
             saved Watchlist under a canonical prefix (e.g. AIM:IQE), that
             existing identity is returned rather than generating a new one.
          2. Fallback — FMP exchange code → Caelyn prefix mapping.

        Ranking: exact bare-symbol match → symbol prefix → name match.
        5-minute result cache.

        Each result includes:
          canonical_ticker, provider_symbol, provider_exchange,
          company_name, exchange (full name), exchange_short_name,
          country, currency, security_type, is_actively_trading, display_symbol
        """
        import asyncio as _aio
        q = query.strip()
        if not q:
            return []

        cache_key = f"fmp:security_search3:{q.lower()}:{limit}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        params = {"apikey": self.api_key, "query": q, "limit": limit}
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                sym_resp, name_resp = await _aio.gather(
                    client.get(f"{self.STABLE_URL}/search-symbol", params=params),
                    client.get(f"{self.STABLE_URL}/search-name",   params=params),
                    return_exceptions=True,
                )
        except Exception as exc:
            print(f"[FMP] search_securities gather failed: {exc}")
            return []

        raw_items: list = []
        for resp in (sym_resp, name_resp):
            if isinstance(resp, Exception):
                continue
            if getattr(resp, "status_code", None) not in (200, 201):
                continue
            try:
                data = resp.json()
                if isinstance(data, list):
                    raw_items.extend(data)
            except Exception:
                pass

        if not raw_items:
            return []

        from services.canonical_security_adapter import (
            build_canonical_registry,
            resolve_with_registry,
        )
        registry = build_canonical_registry()

        q_upper = q.upper()
        seen_canonical: set = set()
        bucket_exact:  list = []
        bucket_prefix: list = []
        bucket_name:   list = []

        for item in raw_items:
            if not isinstance(item, dict):
                continue
            fmp_sym   = (item.get("symbol") or "").strip()
            name      = (item.get("name") or "").strip()
            exch_code = (item.get("exchange") or "").strip()
            full_exch = (item.get("exchangeFullName") or exch_code).strip()
            currency  = (item.get("currency") or "").strip()

            bare_sym = fmp_sym.split(".")[0].upper()
            if not bare_sym:
                continue

            canonical = resolve_with_registry(bare_sym, exch_code, registry)
            if not canonical or canonical in seen_canonical:
                continue
            seen_canonical.add(canonical)

            result = {
                "canonical_ticker":    canonical,
                "provider_symbol":     bare_sym,
                "provider_exchange":   exch_code,
                "company_name":        name,
                "exchange":            full_exch,
                "exchange_short_name": exch_code,
                "country":             item.get("country") or "",
                "currency":            currency,
                "security_type":       item.get("type") or "stock",
                "is_actively_trading": not item.get("delistingDate"),
                "display_symbol":      canonical,
            }

            if bare_sym == q_upper:
                bucket_exact.append(result)
            elif bare_sym.startswith(q_upper):
                bucket_prefix.append(result)
            else:
                bucket_name.append(result)

        results = bucket_exact + bucket_prefix + bucket_name
        cache.set(cache_key, results, 300)
        return results
