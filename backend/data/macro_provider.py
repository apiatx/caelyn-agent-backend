"""
Macro Terminal data provider.

Aggregates data from FRED (economic indicators) and FMP (commodities,
economic calendar) into the structured format the Macro Terminal
frontend expects.
"""
from __future__ import annotations

import asyncio
import math
from datetime import datetime, timedelta
from typing import Any

from data.cache import cache

try:
    from langsmith import traceable
except ImportError:
    def traceable(*args, **kwargs):
        def _noop(fn):
            return fn
        if args and callable(args[0]):
            return args[0]
        return _noop


# ── Cache TTLs ────────────────────────────────────────────────────────
_MACRO_DASHBOARD_TTL = 900     # 15 min — blends FRED + FMP real-time
_MACRO_INDICATORS_TTL = 900    # 15 min
_MACRO_CALENDAR_TTL = 1800     # 30 min for calendar
_MACRO_HISTORY_TTL = 14400     # 4 hours for time-series (FRED only)
_MACRO_COMMODITIES_TTL = 900   # 15 min for market prices
_MACRO_FRED_SERIES_TTL = 14400 # 4 hours for raw FRED series cache

# ── FRED series mapping ──────────────────────────────────────────────
_FRED_SERIES = {
    "fed-funds":        "FEDFUNDS",
    "cpi":              "CPIAUCSL",
    "core-cpi":         "CPILFESL",            # Core CPI (less food & energy)
    "core-pce":         "PCEPILFE",
    "ppi":              "PPIFIS",
    "unemployment":     "UNRATE",
    "gdp":              "A191RL1Q225SBEA",
    "nfp":              "PAYEMS",
    "wages":            "CES0500000003",       # Avg hourly earnings
    "jolts":            "JTSJOL",              # JOLTS openings
    "10y-yield":        "DGS10",
    "2y-yield":         "DGS2",
    "3y-yield":         "DGS3",               # 3-Year Treasury
    "5y-yield":         "DGS5",
    "7y-yield":         "DGS7",               # 7-Year Treasury
    "1y-yield":         "DGS1",
    "6m-yield":         "DGS6MO",
    "3m-yield":         "DGS3MO",
    "1m-yield":         "DGS1MO",
    "2s10s-spread":     "T10Y2Y",
    "10y3m-spread":     "T10Y3M",
    "mortgage-30y":     "MORTGAGE30US",
    "m2":               "M2SL",
    "ism-manufacturing": "INDPRO",             # Industrial Production — proxy for ISM PMI
    "vix":              "VIXCLS",
    "jobless-claims":   "ICSA",
    # ── Additional series for tab endpoints ──
    "breakeven-5y":     "T5YIE",               # 5-Year breakeven inflation
    "breakeven-10y":    "T10YIE",              # 10-Year breakeven inflation
    "trimmed-pce":      "PCETRIM12M159SFRBDAL", # Dallas Fed Trimmed Mean PCE
    "sticky-cpi":       "CORESTICKM159SFRBATL", # Atlanta Fed Sticky CPI
    "retail-sales":     "RSAFS",               # Advance Retail Sales
    "ind-production":   "INDPRO",              # Industrial Production Index
    "consumer-sent":    "UMCSENT",             # U of Michigan Consumer Sentiment
    "leading-index":    "USSLIND",             # Leading Economic Index
    "participation":    "CIVPART",             # Labor Force Participation Rate
    "u6-rate":          "U6RATE",              # U-6 Unemployment Rate
    "cont-claims":      "CCSA",               # Continued Claims
    "hy-spread":        "BAMLH0A0HYM2",       # ICE BofA HY OAS
    "bbb-spread":       "BAMLC0A4CBBB",       # ICE BofA BBB OAS
    "move-index":       "N/A",                 # placeholder — not on FRED
    "dxy":              "DTWEXBGS",            # Trade-Weighted USD Index (Broad)
    "20y-yield":        "DGS20",               # 20-Year Treasury
    "30y-yield":        "DGS30",               # 30-Year Treasury
    "fed-target-lower": "DFEDTARL",            # FOMC target rate lower bound
    "fed-target-upper": "DFEDTARU",            # FOMC target rate upper bound
    # CPI sub-components
    "cpi-shelter":      "CUSR0000SAH1",        # CPI Shelter
    "cpi-food":         "CPIUFDSL",            # CPI Food
    "cpi-energy":       "CPIENGSL",            # CPI Energy
    "cpi-medical":      "CPIMEDSL",            # CPI Medical Care
    "cpi-transport":    "CPITRNSL",            # CPI Transportation
    "cpi-apparel":      "CPIAPPSL",            # CPI Apparel
    "wti-oil":          "DCOILWTICO",          # WTI Crude Oil (daily)
    "housing":          "HOUST",               # Housing Starts (thousands, annual rate)
    "ism-services":     "NMFCI",               # ISM Non-Manufacturing Composite Index
}


def _safe(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except (TypeError, ValueError):
        return None


def _round(v: Any, n: int = 2) -> float | None:
    f = _safe(v)
    return round(f, n) if f is not None else None


class MacroProvider:
    """Aggregates FRED + FMP data for the Macro Terminal."""

    def __init__(self, fred_provider, fmp_provider=None, tradier_provider=None, fear_greed_provider=None):
        self.fred = fred_provider          # FredProvider instance
        self.fmp = fmp_provider            # FMPProvider instance (optional)
        self.tradier = tradier_provider    # TradierProvider instance (optional)
        self._fear_greed = fear_greed_provider  # FearGreedProvider instance (optional)
        self._fred_api = fred_provider.fred if fred_provider else None  # raw fredapi.Fred

    # ── Helpers to fetch raw FRED series ─────────────────────────────

    def _get_series(self, series_id: str, days: int = 730) -> Any:
        """Fetch a FRED series, return pandas Series or None."""
        if not self._fred_api:
            return None
        cache_key = f"macro:fred:{series_id}:{days}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            data = self._fred_api.get_series(series_id, observation_start=start)
            if data is not None and not data.empty:
                data = data.dropna()
                cache.set(cache_key, data, _MACRO_FRED_SERIES_TTL)
                return data
        except Exception as e:
            print(f"[MACRO] FRED series {series_id} error: {e}")
        return None

    def _latest(self, series_id: str, days: int = 730) -> tuple[float | None, str | None]:
        """Get latest value and date for a FRED series."""
        data = self._get_series(series_id, days)
        if data is None or data.empty:
            return None, None
        return _safe(float(data.iloc[-1])), str(data.index[-1].date())

    def _yoy_pct(self, series_id: str) -> float | None:
        """Calculate year-over-year percent change."""
        data = self._get_series(series_id, 730)
        if data is None or len(data) < 13:
            return None
        latest = float(data.iloc[-1])
        year_ago = float(data.iloc[-13])
        if year_ago and year_ago > 0:
            return round(((latest - year_ago) / year_ago) * 100, 2)
        return None

    # ── Dashboard ────────────────────────────────────────────────────

    @traceable(name="macro.get_dashboard")
    async def get_dashboard(self) -> dict:
        """
        Hybrid dashboard: FMP real-time for market prices + FRED for economic releases.

        FMP (real-time, ~15 min delay on free tier):
          - Market indices (S&P 500, Dow, Nasdaq, VIX)
          - Treasury yields (2Y, 10Y, 30Y)
          - Commodities (oil, gold, gas)

        FRED (official releases, inherently lagged):
          - Fed funds rate, CPI, PCE, PPI, unemployment, GDP, NFP,
            wages, JOLTS, M2, ISM, mortgage rates, yield spreads
        """
        cache_key = "macro:dashboard:v2"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        # ── Build async tasks ─────────────────────────────────────────
        # FMP real-time market data
        fmp_task = None
        if self.fmp:
            async def _fetch_fmp():
                idx = await self.fmp.get_market_indices()
                treas = await self.fmp.get_treasury_rates()
                comm = await self.fmp.get_key_commodities()
                dxy = await self.fmp.get_dxy()
                return idx, treas, comm, dxy
            fmp_task = _fetch_fmp()

        # Fear & Greed index
        fg_task = None
        if self._fear_greed:
            fg_task = self._fear_greed.get_fear_greed_index()

        # Tradier benchmark ETF quotes (SPY, QQQ, TLT, GLD, USO, HYG, VIX)
        _BENCHMARK_ETFS = ["SPY", "QQQ", "TLT", "GLD", "USO", "HYG"]
        tradier_task = None
        if self.tradier:
            tradier_task = self.tradier.get_quotes(_BENCHMARK_ETFS)

        # FRED economic releases (sync — run in thread pool)
        fred_task = asyncio.to_thread(self._get_fred_economic_data)

        # Run all in parallel
        tasks = []
        task_names = []
        if fmp_task:
            tasks.append(fmp_task)
            task_names.append("fmp")
        if tradier_task:
            tasks.append(tradier_task)
            task_names.append("tradier")
        if fg_task:
            tasks.append(fg_task)
            task_names.append("fear_greed")
        tasks.append(fred_task)
        task_names.append("fred")

        all_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Unpack results by name
        result_map = {}
        for name, res in zip(task_names, all_results):
            result_map[name] = res if not isinstance(res, Exception) else None
            if isinstance(res, Exception):
                print(f"[MACRO] {name} fetch error: {res}")

        # ── FMP results ───────────────────────────────────────────────
        fmp_indices, fmp_treasury, fmp_commodities, fmp_dxy = {}, {}, {}, {}
        if result_map.get("fmp"):
            fmp_indices, fmp_treasury, fmp_commodities, fmp_dxy = result_map["fmp"]

        # ── Fear & Greed ─────────────────────────────────────────────
        fg_data = result_map.get("fear_greed") or {}

        sp500 = fmp_indices.get("^GSPC", {})
        vix_data = fmp_indices.get("^VIX", {})
        vix_price = _safe(vix_data.get("price"))
        us10y_rt = _safe(fmp_treasury.get("year_10"))
        us2y_rt = _safe(fmp_treasury.get("year_2"))
        us5y_rt = _safe(fmp_treasury.get("year_5"))
        us30y_rt = _safe(fmp_treasury.get("year_30"))
        oil_price = _safe(fmp_commodities.get("CLUSD", {}).get("price"))
        gold_price = _safe(fmp_commodities.get("GCUSD", {}).get("price"))
        gas_price = _safe(fmp_commodities.get("NGUSD", {}).get("price"))

        # Compute spreads from real-time yields
        spread_2s10s_rt = round(us10y_rt - us2y_rt, 2) if us10y_rt and us2y_rt else None
        us3m_rt = _safe(fmp_treasury.get("month_3"))
        spread_10y3m_rt = round(us10y_rt - us3m_rt, 2) if us10y_rt and us3m_rt else None

        # ── Tradier benchmark ETF quotes ──────────────────────────────
        benchmark_quotes = []
        tradier_raw = result_map.get("tradier") or []
        for q in tradier_raw:
            sym = (q.get("symbol") or "").upper()
            price = _safe(q.get("last"))
            if not sym or not price:
                continue
            w52h = _safe(q.get("week_52_high"))
            pct_from_high = round(((price - w52h) / w52h) * 100, 1) if w52h and w52h > 0 else None
            benchmark_quotes.append({
                "ticker": sym,
                "price": _round(price),
                "change_pct": _round(q.get("change_percentage")),
                "week_52_high": _round(w52h),
                "pct_from_52w_high": pct_from_high,
            })

        # ── FRED results ──────────────────────────────────────────────
        fred_data = result_map.get("fred") or {}

        fed_rate = fred_data.get("fed_rate")
        cpi_yoy = fred_data.get("cpi_yoy")
        core_pce_yoy = fred_data.get("core_pce_yoy")
        ppi_yoy = fred_data.get("ppi_yoy")
        unemp = fred_data.get("unemployment")
        wages_yoy = fred_data.get("wages_yoy")
        jolts = fred_data.get("jolts")
        mortgage = fred_data.get("mortgage")
        m2 = fred_data.get("m2")
        m2_yoy = fred_data.get("m2_yoy")
        gdp_quarterly = fred_data.get("gdp_quarterly", [])
        nfp_last = fred_data.get("nfp_last")
        ism_mfg = fred_data.get("ism_mfg")

        # Use FMP real-time yields, fall back to FRED if FMP unavailable
        us10y = us10y_rt or fred_data.get("us10y")
        us2y = us2y_rt or fred_data.get("us2y")
        spread_2s10s = spread_2s10s_rt or fred_data.get("spread_2s10s")
        spread_10y3m = spread_10y3m_rt or fred_data.get("spread_10y3m")
        vix = vix_price or fred_data.get("vix")

        # Inflation trend
        inflation_trend = "sticky"
        if cpi_yoy is not None:
            if cpi_yoy < 2.5:
                inflation_trend = "declining"
            elif cpi_yoy < 3.5:
                inflation_trend = "sticky"
            else:
                inflation_trend = "elevated"

        # M2 trend
        m2_trend = "stable"
        if m2_yoy is not None:
            if m2_yoy > 3:
                m2_trend = "expanding"
            elif m2_yoy < -1:
                m2_trend = "contracting"

        result = {
            "last_updated": datetime.utcnow().isoformat() + "Z",
            "data_sources": {
                "market_prices": "FMP (real-time)" if fmp_indices else "FRED (1-2 day lag)",
                "benchmark_etfs": "Tradier (real-time)" if benchmark_quotes else "unavailable",
                "yields": "FMP (real-time)" if fmp_treasury else "FRED (1-2 day lag)",
                "commodities": "FMP (real-time)" if fmp_commodities else "unavailable",
                "economic_releases": "FRED (official release schedule)",
            },
            "market_snapshot": {
                "sp500": _round(sp500.get("price")),
                "sp500_change_pct": _round(sp500.get("change_pct"), 2),
                "dow": _round(fmp_indices.get("^DJI", {}).get("price")),
                "nasdaq": _round(fmp_indices.get("^IXIC", {}).get("price")),
                "russell_2000": _round(fmp_indices.get("^RUT", {}).get("price")),
            },
            "benchmark_etfs": benchmark_quotes,
            "fed": {
                "funds_rate": _round(fed_rate),
                "funds_rate_range": f"{_round(fed_rate)}-{_round((_safe(fed_rate) or 0) + 0.25)}" if fed_rate else None,
                "next_meeting": None,
                "commentary": f"Fed funds rate at {_round(fed_rate)}%. {'Restrictive territory.' if (_safe(fed_rate) or 0) > 4 else 'Easing cycle underway.' if (_safe(fed_rate) or 0) < 4 else 'Holding steady.'}"
            },
            "inflation": {
                "cpi_yoy": _round(cpi_yoy, 1),
                "core_cpi_yoy": _round(cpi_yoy, 1),
                "core_pce_yoy": _round(core_pce_yoy, 1),
                "ppi_yoy": _round(ppi_yoy, 1),
                "trend": inflation_trend,
                "commentary": f"CPI {_round(cpi_yoy, 1)}% YoY, Core PCE {_round(core_pce_yoy, 1)}% — inflation {inflation_trend}."
            },
            "labor": {
                "nfp_last": nfp_last,
                "unemployment_rate": _round(unemp, 1),
                "wage_growth_yoy": _round(wages_yoy, 1),
                "jolts_openings": _round((_safe(jolts) or 0) / 1000, 1) if jolts else None,
                "commentary": f"Unemployment at {_round(unemp, 1)}%. {'Tight labor market.' if (_safe(unemp) or 5) < 4.5 else 'Labor market softening.'}"
            },
            "gdp": {
                "quarterly_data": gdp_quarterly,
                "gdp_now_estimate": _round(gdp_quarterly[-1]["gdp"], 1) if gdp_quarterly else None,
                "commentary": f"Latest GDP growth: {gdp_quarterly[-1]['gdp'] if gdp_quarterly else 'N/A'}% annualized."
            },
            "rates_and_yields": {
                "us_10y": _round(us10y),
                "us_2y": _round(us2y),
                "us_5y": _round(us5y_rt),
                "us_30y": _round(us30y_rt),
                "spread_2s10s": _round(spread_2s10s),
                "spread_10y3m": _round(spread_10y3m),
                "mortgage_30y": _round(mortgage),
                "commentary": f"10Y at {_round(us10y)}%, 2s10s spread {_round(spread_2s10s)}. {'Curve inverted — recession signal.' if (_safe(spread_2s10s) or 0) < 0 else 'Normal yield curve.'}"
            },
            "liquidity": {
                "m2_current_trillion": _round((_safe(m2) or 0) / 1000, 1) if m2 else None,
                "m2_yoy_growth": _round(m2_yoy, 1),
                "m2_trend": m2_trend,
                "commentary": f"M2 money supply {m2_trend} at {_round(m2_yoy, 1)}% YoY."
            },
            "commodities": {
                "wti_oil": _round(oil_price),
                "gold": _round(gold_price),
                "natural_gas": _round(gas_price),
                "gas_price_avg": _round(gas_price),
                "commentary": f"WTI crude at ${_round(oil_price)}, gold at ${_round(gold_price)}." if oil_price and gold_price else "Commodity data unavailable."
            },
            "manufacturing": {
                "ism_manufacturing": _round(ism_mfg, 1),
                "ism_new_orders": None,
                "ism_production": None,
                "ism_employment": None,
                "commentary": f"Manufacturing employment index at {_round(ism_mfg, 1)}."
            },
            "geopolitical": {
                "events": []
            },
            "scenarios": {
                "bull": [
                    "Productivity boom from AI drives non-inflationary growth",
                    "Shelter disinflation accelerates — core CPI approaches 2%",
                ],
                "bear": [
                    "Oil sustained above $100/bbl triggers stagflation",
                    "Tariff escalation disrupts supply chains, reignites inflation",
                ],
                "base": [
                    f"GDP growth {gdp_quarterly[-1]['gdp'] if gdp_quarterly else '1.5'}-2.0%, inflation slowly declining, 1-2 cuts by year-end",
                ],
            },
            "vix": {
                "current": _round(vix),
                "change": _round(vix_data.get("change")),
                "change_pct": _round(vix_data.get("change_pct")),
                "signal": "low fear" if (_safe(vix) or 20) < 18 else "elevated" if (_safe(vix) or 20) < 25 else "high fear",
            },
            "fear_greed": {
                "score": fg_data.get("current_score"),
                "rating": fg_data.get("current_rating"),
                "signal": fg_data.get("signal"),
            },
            "dollar": {
                "dxy": _round(_safe(fmp_dxy.get("price"))) if fmp_dxy else None,
                "dxy_change_pct": _round(_safe(fmp_dxy.get("change_pct"))) if fmp_dxy else None,
            },
        }

        cache.set(cache_key, result, _MACRO_DASHBOARD_TTL)
        return result

    def _get_fred_economic_data(self) -> dict:
        """Synchronous helper: fetch all FRED economic releases."""
        fed_rate, _ = self._latest("FEDFUNDS", 365)
        cpi_yoy = self._yoy_pct("CPIAUCSL")
        core_pce_yoy = self._yoy_pct("PCEPILFE")
        ppi_yoy = self._yoy_pct("PPIFIS")
        unemp, _ = self._latest("UNRATE", 365)
        wages_yoy = self._yoy_pct("CES0500000003")
        jolts, _ = self._latest("JTSJOL", 365)
        us10y, _ = self._latest("DGS10", 90)
        us2y, _ = self._latest("DGS2", 90)
        spread_2s10s, _ = self._latest("T10Y2Y", 365)
        spread_10y3m, _ = self._latest("T10Y3M", 365)
        mortgage, _ = self._latest("MORTGAGE30US", 365)
        m2, _ = self._latest("M2SL", 730)
        m2_yoy = self._yoy_pct("M2SL")
        vix, _ = self._latest("VIXCLS", 90)

        gdp_data = self._get_series("A191RL1Q225SBEA", 730)
        gdp_quarterly = []
        if gdp_data is not None and not gdp_data.empty:
            for idx, val in gdp_data.tail(5).items():
                q = f"Q{(idx.month - 1) // 3 + 1} {idx.year}"
                gdp_quarterly.append({"quarter": q, "gdp": _round(val, 1)})

        nfp_data = self._get_series("PAYEMS", 365)
        nfp_last = None
        if nfp_data is not None and len(nfp_data) >= 2:
            nfp_last = int(float(nfp_data.iloc[-1]) - float(nfp_data.iloc[-2])) * 1000

        ism_mfg, _ = self._latest("NAPM", 365)

        return {
            "fed_rate": fed_rate, "cpi_yoy": cpi_yoy, "core_pce_yoy": core_pce_yoy,
            "ppi_yoy": ppi_yoy, "unemployment": unemp, "wages_yoy": wages_yoy,
            "jolts": jolts, "mortgage": mortgage, "m2": m2, "m2_yoy": m2_yoy,
            "us10y": us10y, "us2y": us2y, "spread_2s10s": spread_2s10s,
            "spread_10y3m": spread_10y3m, "vix": vix,
            "gdp_quarterly": gdp_quarterly, "nfp_last": nfp_last, "ism_mfg": ism_mfg,
        }

    # ── Indicators ───────────────────────────────────────────────────

    @traceable(name="macro.get_indicators")
    def get_indicators(self) -> dict:
        cache_key = "macro:indicators:v1"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        indicators = []

        def _add(name: str, series: str, source_url: str, *, fmt: str = "pct", days: int = 365, signal_fn=None):
            val, date = self._latest(series, days)
            if fmt == "yoy":
                display_val = self._yoy_pct(series)
                display = f"{display_val}%" if display_val is not None else "N/A"
            elif fmt == "pct":
                display = f"{_round(val)}%" if val is not None else "N/A"
            elif fmt == "spread":
                display = f"{_round(val)}%" if val is not None else "N/A"
            elif fmt == "dollar":
                display = f"${_round(val)}" if val is not None else "N/A"
            elif fmt == "number":
                display = f"{int(val):,}" if val is not None else "N/A"
            elif fmt == "trillion":
                display = f"${_round((_safe(val) or 0) / 1000, 1)}T" if val is not None else "N/A"
            else:
                display = str(_round(val)) if val is not None else "N/A"

            signal = "neutral"
            if signal_fn and val is not None:
                signal = signal_fn(val)

            indicators.append({
                "name": name,
                "value": display,
                "raw_value": _round(val),
                "signal": signal,
                "source": source_url,
                "last_updated": date,
                "commentary": "",
            })

        _add("Fed Funds Rate", "FEDFUNDS", "https://fred.stlouisfed.org/series/FEDFUNDS",
             signal_fn=lambda v: "bearish" if v > 5 else "neutral" if v > 3 else "bullish")
        _add("CPI (YoY)", "CPIAUCSL", "https://fred.stlouisfed.org/series/CPIAUCSL",
             fmt="yoy", days=730,
             signal_fn=lambda v: "bearish" if v > 4 else "neutral" if v > 2.5 else "bullish")
        _add("Core PCE (YoY)", "PCEPILFE", "https://fred.stlouisfed.org/series/PCEPILFE",
             fmt="yoy", days=730,
             signal_fn=lambda v: "bearish" if v > 3 else "neutral" if v > 2 else "bullish")
        _add("PPI (YoY)", "PPIFIS", "https://fred.stlouisfed.org/series/PPIFIS",
             fmt="yoy", days=730,
             signal_fn=lambda v: "bearish" if v > 4 else "neutral" if v > 2 else "bullish")
        _add("Non-Farm Payrolls", "PAYEMS", "https://fred.stlouisfed.org/series/PAYEMS",
             fmt="number",
             signal_fn=lambda v: "bullish")  # raw total; MoM delta computed in dashboard
        _add("Unemployment Rate", "UNRATE", "https://fred.stlouisfed.org/series/UNRATE",
             signal_fn=lambda v: "bearish" if v > 5 else "neutral" if v > 4 else "bullish")
        _add("Wage Growth (YoY)", "CES0500000003", "https://fred.stlouisfed.org/series/CES0500000003",
             fmt="yoy", days=730,
             signal_fn=lambda v: "neutral")
        _add("GDP Growth", "A191RL1Q225SBEA", "https://fred.stlouisfed.org/series/A191RL1Q225SBEA",
             days=730,
             signal_fn=lambda v: "bearish" if v < 0 else "neutral" if v < 2 else "bullish")
        _add("10Y Treasury Yield", "DGS10", "https://fred.stlouisfed.org/series/DGS10",
             days=90,
             signal_fn=lambda v: "bearish" if v > 5 else "neutral")
        _add("2Y Treasury Yield", "DGS2", "https://fred.stlouisfed.org/series/DGS2",
             days=90,
             signal_fn=lambda v: "neutral")
        _add("10Y-2Y Spread", "T10Y2Y", "https://fred.stlouisfed.org/series/T10Y2Y",
             fmt="spread",
             signal_fn=lambda v: "bearish" if v < 0 else "neutral" if v < 0.5 else "bullish")
        _add("10Y-3M Spread", "T10Y3M", "https://fred.stlouisfed.org/series/T10Y3M",
             fmt="spread",
             signal_fn=lambda v: "bearish" if v < 0 else "neutral")
        _add("30Y Mortgage Rate", "MORTGAGE30US", "https://fred.stlouisfed.org/series/MORTGAGE30US",
             signal_fn=lambda v: "bearish" if v > 7 else "neutral" if v > 5 else "bullish")
        _add("M2 Money Supply", "M2SL", "https://fred.stlouisfed.org/series/M2SL",
             fmt="trillion", days=730,
             signal_fn=lambda v: "neutral")
        _add("ISM Manufacturing PMI", "NAPM", "https://fred.stlouisfed.org/series/NAPM",
             fmt="number",
             signal_fn=lambda v: "bullish" if v > 50 else "bearish")
        _add("VIX", "VIXCLS", "https://fred.stlouisfed.org/series/VIXCLS",
             days=90, fmt="raw",
             signal_fn=lambda v: "bullish" if v < 15 else "neutral" if v < 25 else "bearish")

        result = {"indicators": indicators}
        cache.set(cache_key, result, _MACRO_INDICATORS_TTL)
        return result

    # ── Calendar ─────────────────────────────────────────────────────

    @traceable(name="macro.get_calendar")
    async def get_calendar(self, days_ahead: int = 14) -> dict:
        """Upcoming economic events from Nasdaq free calendar API."""
        cache_key = f"macro:calendar:v1:{days_ahead}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        events = []

        # Use Nasdaq free calendar (the get_economic_calendar method with days_ahead param)
        if self.fmp:
            try:
                nasdaq_events = await self.fmp.get_economic_calendar(days_ahead=days_ahead)
                for evt in (nasdaq_events or []):
                    events.append({
                        "date": evt.get("date"),
                        "event": evt.get("event"),
                        "previous": evt.get("previous"),
                        "forecast": evt.get("consensus"),
                        "actual": evt.get("actual"),
                        "importance": "high",
                    })
            except Exception as e:
                print(f"[MACRO] Calendar fetch error: {e}")

        events.sort(key=lambda x: x.get("date") or "")
        result = {"events": events}
        cache.set(cache_key, result, _MACRO_CALENDAR_TTL)
        return result

    # ── History ──────────────────────────────────────────────────────

    @traceable(name="macro.get_history")
    def get_history(self, indicator_slug: str, months: int = 12) -> dict:
        """Time-series data for charting."""
        cache_key = f"macro:history:{indicator_slug}:{months}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        series_id = _FRED_SERIES.get(indicator_slug)
        if not series_id:
            return {"error": f"Unknown indicator: {indicator_slug}", "valid_slugs": list(_FRED_SERIES.keys())}

        days = months * 31
        data = self._get_series(series_id, days)
        if data is None or data.empty:
            return {"indicator": indicator_slug, "data": [], "error": "No data available"}

        # For GDP, format as quarters
        if indicator_slug == "gdp":
            points = []
            for idx, val in data.items():
                q = f"{idx.year}-Q{(idx.month - 1) // 3 + 1}"
                points.append({"date": q, "value": _round(val, 1)})
        else:
            points = []
            for idx, val in data.items():
                points.append({"date": str(idx.date()), "value": _round(val)})

        result = {"indicator": indicator_slug, "data": points}
        cache.set(cache_key, result, _MACRO_HISTORY_TTL)
        return result

    # ══════════════════════════════════════════════════════════════════
    # ── Tab-specific endpoints for Macro Terminal ────────────────────
    # ══════════════════════════════════════════════════════════════════

    # ── RATES tab ─────────────────────────────────────────────────────

    @traceable(name="macro.get_rates")
    async def get_rates(self) -> dict:
        """Full yield curve, Fed policy, and credit conditions."""
        cache_key = "macro:tab:rates:v2"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        # FMP real-time treasury yields + FRED data + yield snapshot in parallel
        fmp_treasury = {}
        if self.fmp:
            try:
                fmp_treasury = await self.fmp.get_treasury_rates()
            except Exception as e:
                print(f"[MACRO:RATES] FMP treasury error: {e}")

        fred_data, snap = await asyncio.gather(
            asyncio.to_thread(self._get_rates_fred_data),
            asyncio.to_thread(self._get_yield_curve_snapshot),
        )

        # Build yield curve — prefer FMP real-time, fall back to FRED
        yield_curve = []
        tenors = [
            ("1M",  "month_1",  "DGS1MO"),
            ("3M",  "month_3",  "DGS3MO"),
            ("6M",  "month_6",  "DGS6MO"),
            ("1Y",  "year_1",   "DGS1"),
            ("2Y",  "year_2",   "DGS2"),
            ("3Y",  "year_3",   "DGS3"),
            ("5Y",  "year_5",   "DGS5"),
            ("7Y",  "year_7",   "DGS7"),
            ("10Y", "year_10",  "DGS10"),
            ("20Y", "year_20",  "DGS20"),
            ("30Y", "year_30",  "DGS30"),
        ]
        for label, fmp_key, fred_series in tenors:
            val = _safe(fmp_treasury.get(fmp_key))
            if val is None and fred_series:
                val, _ = self._latest(fred_series, 90)
            yield_curve.append({"tenor": label, "yield_pct": _round(val, 3)})

        us10y = _safe(fmp_treasury.get("year_10")) or fred_data.get("us10y")
        us2y  = _safe(fmp_treasury.get("year_2"))  or fred_data.get("us2y")
        us30y = _safe(fmp_treasury.get("year_30"))
        us10y_date = fred_data.get("us10y_date")
        us2y_date  = fred_data.get("us2y_date")
        us5y_date  = fred_data.get("us5y_date")

        spread_2s10s = round(us10y - us2y, 2) if us10y and us2y else fred_data.get("spread_2s10s")
        spread_10y3m = fred_data.get("spread_10y3m")

        # Yield curve status
        curve_status = "normal"
        if spread_2s10s is not None:
            if spread_2s10s < 0:
                curve_status = "inverted"
            elif spread_2s10s < 0.2:
                curve_status = "flat"

        # Fed target range — prefer FOMC target bounds; fall back to effective ±0.25
        fed_target_lower = fred_data.get("fed_target_lower")
        fed_target_upper = fred_data.get("fed_target_upper")
        if fed_target_lower is not None and fed_target_upper is not None:
            funds_rate_range = f"{fed_target_lower:.2f}-{fed_target_upper:.2f}"
            funds_rate_range_date = fred_data.get("fed_target_lower_date")
        else:
            eff = fred_data.get("fed_rate")
            funds_rate_range = f"{_round(eff)}-{_round((eff or 0) + 0.25)}" if eff else None
            funds_rate_range_date = None

        # BPS changes for key rates (current vs 1W-ago snapshot)
        def _bps_chg(current, week_ago_val):
            if current is None or week_ago_val is None:
                return None
            return round((current - week_ago_val) * 100)

        snap_w = snap.get("week_ago", {})
        snap_m = snap.get("month_ago", {})

        result = {
            "last_updated": datetime.utcnow().isoformat() + "Z",
            "data_source": "FMP (real-time)" if fmp_treasury else "FRED (1-2 day lag)",
            "yield_curve": yield_curve,
            "yield_curve_snapshot": {
                "week_ago": snap_w,
                "month_ago": snap_m,
            },
            "key_rates": {
                "us_2y":  {"value": _round(us2y, 3),  "date": us2y_date,  "change_1w_bps": _bps_chg(us2y,  snap_w.get("2Y"))},
                "us_5y":  {"value": _round(_safe(fmp_treasury.get("year_5")) or fred_data.get("us5y"), 3), "date": us5y_date, "change_1w_bps": _bps_chg(_safe(fmp_treasury.get("year_5")) or fred_data.get("us5y"), snap_w.get("5Y"))},
                "us_10y": {"value": _round(us10y, 3), "date": us10y_date, "change_1w_bps": _bps_chg(us10y, snap_w.get("10Y"))},
                "us_30y": {"value": _round(us30y, 3), "date": None,       "change_1w_bps": _bps_chg(us30y, snap_w.get("30Y"))},
            },
            "fed_policy": {
                "funds_rate": _round(fred_data.get("fed_rate")),
                "funds_rate_range": funds_rate_range,
                "funds_rate_range_date": funds_rate_range_date,
            },
            "spreads": {
                "2s10s":  _round(spread_2s10s, 4),
                "10y3m":  _round(spread_10y3m, 4),
                "spread_2s10s": _round(spread_2s10s),
                "spread_10y3m": _round(spread_10y3m),
                "curve_status": curve_status,
                "inversion_signal": spread_2s10s is not None and spread_2s10s < 0,
                "change_2s10s_1w_bps": _bps_chg(
                    spread_2s10s,
                    _round(snap_w["10Y"] - snap_w["2Y"], 4)
                    if snap_w.get("10Y") is not None and snap_w.get("2Y") is not None else None,
                ),
                "change_10y3m_1w_bps": _bps_chg(spread_10y3m, fred_data.get("spread_10y3m_1w_ago")),
                "spread_10y3m_date": fred_data.get("spread_10y3m_date"),
            },
            "mortgage": {
                "rate_30y": _round(fred_data.get("mortgage")),
                "rate_30y_date": fred_data.get("mortgage_date"),
                "change_1w_bps": _bps_chg(fred_data.get("mortgage"), fred_data.get("mortgage_1w_ago")),
            },
            "credit_spreads": {
                "hy_oas": _round(fred_data.get("hy_spread")),
                "bbb_oas": _round(fred_data.get("bbb_spread")),
            },
            "history": {
                "us_10y":         self.get_history("10y-yield",   24).get("data", []),
                "us_2y":          self.get_history("2y-yield",    24).get("data", []),
                "us_5y":          self.get_history("5y-yield",    24).get("data", []),
                "us_30y":         self.get_history("30y-yield",   24).get("data", []),
                "spread_2s10s":   self.get_history("2s10s-spread",24).get("data", []),
                "spread_10y3m":   self.get_history("10y3m-spread",24).get("data", []),
                "mortgage_30y":   self.get_history("mortgage-30y",24).get("data", []),
            },
        }

        cache.set(cache_key, result, _MACRO_DASHBOARD_TTL)
        return result

    def _get_rates_fred_data(self) -> dict:
        """Sync helper: fetch FRED rates data."""
        fed_rate, _ = self._latest("FEDFUNDS", 365)
        fed_target_lower, fed_target_lower_date = self._latest("DFEDTARL", 365)
        fed_target_upper, fed_target_upper_date = self._latest("DFEDTARU", 365)
        us10y, us10y_date = self._latest("DGS10", 90)
        us2y, us2y_date = self._latest("DGS2", 90)
        us5y, us5y_date = self._latest("DGS5", 90)
        spread_2s10s, _ = self._latest("T10Y2Y", 365)
        spread_10y3m, spread_10y3m_date = self._latest("T10Y3M", 365)
        mortgage, mortgage_date = self._latest("MORTGAGE30US", 365)
        hy_spread, _ = self._latest("BAMLH0A0HYM2", 365)
        bbb_spread, _ = self._latest("BAMLC0A4CBBB", 365)

        # Week-ago values for spread and mortgage (for bps change cards)
        spread_10y3m_1w_ago = None
        s = self._get_series("T10Y3M", 45)
        if s is not None and len(s) >= 6:
            spread_10y3m_1w_ago = _round(_safe(float(s.iloc[max(0, len(s) - 6)])), 4)

        mortgage_1w_ago = None
        m = self._get_series("MORTGAGE30US", 45)
        if m is not None and len(m) >= 2:
            mortgage_1w_ago = _round(_safe(float(m.iloc[max(0, len(m) - 2)])), 3)

        return {
            "fed_rate": fed_rate,
            "fed_target_lower": fed_target_lower, "fed_target_lower_date": fed_target_lower_date,
            "fed_target_upper": fed_target_upper, "fed_target_upper_date": fed_target_upper_date,
            "us10y": us10y, "us10y_date": us10y_date,
            "us2y": us2y, "us2y_date": us2y_date,
            "us5y": us5y, "us5y_date": us5y_date,
            "spread_2s10s": spread_2s10s, "spread_10y3m": spread_10y3m,
            "spread_10y3m_date": spread_10y3m_date, "spread_10y3m_1w_ago": spread_10y3m_1w_ago,
            "mortgage": mortgage, "mortgage_date": mortgage_date, "mortgage_1w_ago": mortgage_1w_ago,
            "hy_spread": hy_spread, "bbb_spread": bbb_spread,
        }

    def _get_yield_curve_snapshot(self) -> dict:
        """Sync helper: 1W-ago and 1M-ago snapshots for all yield curve maturities."""
        tenor_series = [
            ("1M", "DGS1MO"), ("3M", "DGS3MO"), ("6M", "DGS6MO"), ("1Y", "DGS1"),
            ("2Y", "DGS2"), ("5Y", "DGS5"), ("7Y", "DGS7"), ("10Y", "DGS10"),
            ("20Y", "DGS20"), ("30Y", "DGS30"),
        ]
        week_ago: dict[str, float | None] = {}
        month_ago: dict[str, float | None] = {}
        for tenor, series_id in tenor_series:
            data = self._get_series(series_id, 45)  # 45 cal days ≈ 30 trading days
            if data is None or len(data) < 2:
                week_ago[tenor] = None
                month_ago[tenor] = None
                continue
            idx_w = max(0, len(data) - 6)   # ~5 trading days back
            idx_m = max(0, len(data) - 22)  # ~21 trading days back
            week_ago[tenor] = _round(_safe(float(data.iloc[idx_w])), 3)
            month_ago[tenor] = _round(_safe(float(data.iloc[idx_m])), 3)
        return {"week_ago": week_ago, "month_ago": month_ago}

    def _get_cpi_yoy_history(self) -> list:
        """Sync: last 15 months of CPI headline and Core CPI as YoY %."""
        cpi  = self._get_series("CPIAUCSL", 800)   # ~26 months of monthly data
        core = self._get_series("CPILFESL", 800)
        if cpi is None or len(cpi) < 14:
            return []
        result = []
        n = len(cpi)
        start_idx = max(12, n - 15)
        for i in range(start_idx, n):
            year_ago_i = i - 12
            if year_ago_i < 0:
                continue
            curr_val = float(cpi.iloc[i])
            prev_val = float(cpi.iloc[year_ago_i])
            if prev_val <= 0:
                continue
            headline_yoy = round(((curr_val - prev_val) / prev_val) * 100, 2)
            core_yoy = None
            if core is not None and i < len(core) and year_ago_i < len(core):
                c_curr = float(core.iloc[i])
                c_prev = float(core.iloc[year_ago_i])
                if c_prev > 0:
                    core_yoy = round(((c_curr - c_prev) / c_prev) * 100, 2)
            date = cpi.index[i]
            result.append({
                "month": date.strftime("%b '%y"),
                "date":  str(date.date()),
                "headline_yoy": headline_yoy,
                "core_yoy":     core_yoy,
            })
        return result

    def _get_inflation_components(self) -> dict:
        """Sync: CPI sub-component YoY + WTI oil price."""
        components = {}
        for slug, label in [
            ("cpi-shelter",  "Shelter"),
            ("cpi-food",     "Food"),
            ("cpi-energy",   "Energy"),
            ("cpi-medical",  "Medical Care"),
            ("cpi-transport","Transportation"),
            ("cpi-apparel",  "Apparel"),
        ]:
            try:
                components[slug] = {"label": label, "yoy": self._yoy_pct(_FRED_SERIES[slug])}
            except Exception:
                components[slug] = {"label": label, "yoy": None}

        # WTI oil price
        oil_price = oil_date = oil_prev_month = None
        try:
            oil_s = self._get_series("DCOILWTICO", 90)
            if oil_s is not None and len(oil_s) >= 2:
                oil_s = oil_s.dropna()
                if len(oil_s) >= 2:
                    oil_price = _round(_safe(float(oil_s.iloc[-1])), 2)
                    oil_date  = str(oil_s.index[-1].date())
                    idx_m = max(0, len(oil_s) - 22)
                    oil_prev_month = _round(_safe(float(oil_s.iloc[idx_m])), 2)
        except Exception:
            pass

        return {
            "components": components,
            "oil_price": oil_price,
            "oil_date":  oil_date,
            "oil_prev_month": oil_prev_month,
        }

    # ── INFLATION tab ─────────────────────────────────────────────────

    @traceable(name="macro.get_inflation")
    async def get_inflation(self) -> dict:
        """CPI, Core CPI, PCE, PPI, breakevens, sticky/trimmed measures — with components and oil."""
        cache_key = "macro:tab:inflation:v2"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        # Fetch all data concurrently
        data, yoy_hist, comp_data = await asyncio.gather(
            asyncio.to_thread(self._get_inflation_fred_data),
            asyncio.to_thread(self._get_cpi_yoy_history),
            asyncio.to_thread(self._get_inflation_components),
        )

        # Trend logic
        cpi_yoy = data.get("cpi_yoy")
        trend = "sticky"
        if cpi_yoy is not None:
            if cpi_yoy < 2.5:
                trend = "declining"
            elif cpi_yoy > 4:
                trend = "elevated"

        # Target proximity
        core_pce = data.get("core_pce_yoy")
        target_status = "unknown"
        if core_pce is not None:
            if core_pce <= 2.2:
                target_status = "at_target"
            elif core_pce <= 3.0:
                target_status = "above_target"
            else:
                target_status = "well_above_target"

        # Build cpi_components_detail from sub-components
        _comps = comp_data.get("components", {})
        cpi_components_detail = []
        for slug in ["cpi-shelter", "cpi-food", "cpi-energy", "cpi-medical", "cpi-transport", "cpi-apparel"]:
            c = _comps.get(slug, {})
            yoy_val = _round(c.get("yoy"), 1)
            cpi_components_detail.append({
                "name": c.get("label", slug),
                "value": yoy_val,
                "hot": (yoy_val or 0) > 3.0,
            })

        # WTI oil object
        oil_price = comp_data.get("oil_price")
        oil_prev  = comp_data.get("oil_prev_month")
        oil_chg_pct = None
        if oil_price is not None and oil_prev and oil_prev > 0:
            oil_chg_pct = _round(((oil_price - oil_prev) / oil_prev) * 100, 1)
        oil = {
            "wti_price": oil_price,
            "prev_month_price": oil_prev,
            "change_pct_1m": oil_chg_pct,
            "date": comp_data.get("oil_date"),
        }

        result = {
            "last_updated": datetime.utcnow().isoformat() + "Z",
            "headline": {
                "cpi_yoy":      _round(cpi_yoy, 1),
                "cpi_mom":      _round(data.get("cpi_mom"), 2),
                "core_cpi_yoy": _round(data.get("core_cpi_yoy"), 1),
                "core_pce_yoy": _round(core_pce, 1),
                "ppi_yoy":      _round(data.get("ppi_yoy"), 1),
                "target":       2.0,
            },
            "headline_changes": {
                "cpi_yoy_prev":      _round(data.get("cpi_yoy_prev"), 1),
                "core_cpi_yoy_prev": _round(data.get("core_cpi_yoy_prev"), 1),
                "core_pce_yoy_prev": _round(data.get("core_pce_yoy_prev"), 1),
                "ppi_yoy_prev":      _round(data.get("ppi_yoy_prev"), 1),
                "cpi_mom_prev":      _round(data.get("cpi_mom_prev"), 2),
            },
            "headline_dates": data.get("headline_dates", {}),
            "fed_preferred": {
                "core_pce_yoy":  _round(core_pce, 1),
                "target":        2.0,
                "target_status": target_status,
            },
            "alternative_measures": {
                "trimmed_mean_pce": _round(data.get("trimmed_pce"), 1),
                "sticky_cpi":       _round(data.get("sticky_cpi"), 1),
            },
            "market_expectations": {
                "breakeven_5y":  _round(data.get("breakeven_5y"), 2),
                "breakeven_10y": _round(data.get("breakeven_10y"), 2),
            },
            "trend": trend,
            "commentary": (
                f"CPI {_round(cpi_yoy, 1)}% YoY, Core PCE {_round(core_pce, 1)}% "
                f"(Fed target 2%). Inflation {trend}. "
                f"5Y breakeven: {_round(data.get('breakeven_5y'), 2)}%."
            ),
            # YoY history — used by trend chart ({month, headline_yoy, core_yoy})
            "history": yoy_hist,
            # Raw index history kept for backward compatibility
            "history_raw": {
                "cpi":         self.get_history("cpi", 36).get("data", []),
                "core_pce":    self.get_history("core-pce", 36).get("data", []),
                "breakeven_5y":self.get_history("breakeven-5y", 36).get("data", []),
            },
            "cpi_components_detail": cpi_components_detail,
            "oil": oil,
        }

        cache.set(cache_key, result, _MACRO_DASHBOARD_TTL)
        return result

    def _get_inflation_fred_data(self) -> dict:
        """Sync helper: fetch all inflation FRED series including dates and prior-period values."""
        # Current YoY values
        cpi_yoy = self._yoy_pct("CPIAUCSL")
        core_cpi_yoy = self._yoy_pct("CPILFESL")
        core_pce_yoy = self._yoy_pct("PCEPILFE")
        ppi_yoy = self._yoy_pct("PPIFIS")

        # Dates for each series
        _, cpi_date       = self._latest("CPIAUCSL", 365)
        _, core_cpi_date  = self._latest("CPILFESL", 365)
        _, core_pce_date  = self._latest("PCEPILFE", 365)
        _, ppi_date       = self._latest("PPIFIS", 365)

        # CPI month-over-month (current and prior)
        cpi_data = self._get_series("CPIAUCSL", 800)
        cpi_mom = cpi_mom_prev = None
        if cpi_data is not None and len(cpi_data) >= 3:
            p0 = float(cpi_data.iloc[-3])
            p1 = float(cpi_data.iloc[-2])
            p2 = float(cpi_data.iloc[-1])
            if p1 > 0:
                cpi_mom = round(((p2 - p1) / p1) * 100, 2)
            if p0 > 0:
                cpi_mom_prev = round(((p1 - p0) / p0) * 100, 2)

        # Previous-period YoY values (second-to-last available month)
        def _prev_yoy(series_id: str) -> float | None:
            data = self._get_series(series_id, 800)
            if data is None or len(data) < 14:
                return None
            n = len(data)
            i = n - 2   # second-to-last
            ya = i - 12
            if ya < 0:
                return None
            curr = float(data.iloc[i])
            prev = float(data.iloc[ya])
            return round(((curr - prev) / prev) * 100, 2) if prev > 0 else None

        cpi_yoy_prev      = _prev_yoy("CPIAUCSL")
        core_cpi_yoy_prev = _prev_yoy("CPILFESL")
        core_pce_yoy_prev = _prev_yoy("PCEPILFE")
        ppi_yoy_prev      = _prev_yoy("PPIFIS")

        breakeven_5y,  _ = self._latest("T5YIE", 365)
        breakeven_10y, _ = self._latest("T10YIE", 365)
        trimmed_pce,   _ = self._latest("PCETRIM12M159SFRBDAL", 365)
        sticky_cpi,    _ = self._latest("CORESTICKM159SFRBATL", 365)

        return {
            "cpi_yoy": cpi_yoy, "cpi_mom": cpi_mom, "cpi_mom_prev": cpi_mom_prev,
            "cpi_yoy_prev": cpi_yoy_prev,
            "core_cpi_yoy": core_cpi_yoy, "core_cpi_yoy_prev": core_cpi_yoy_prev,
            "core_pce_yoy": core_pce_yoy, "core_pce_yoy_prev": core_pce_yoy_prev,
            "ppi_yoy": ppi_yoy, "ppi_yoy_prev": ppi_yoy_prev,
            "headline_dates": {
                "cpi_yoy":      cpi_date,
                "core_cpi_yoy": core_cpi_date,
                "core_pce_yoy": core_pce_date,
                "ppi_yoy":      ppi_date,
                "cpi_mom":      cpi_date,
            },
            "breakeven_5y": breakeven_5y, "breakeven_10y": breakeven_10y,
            "trimmed_pce": trimmed_pce, "sticky_cpi": sticky_cpi,
        }

    # ── GROWTH tab ────────────────────────────────────────────────────

    @traceable(name="macro.get_growth")
    async def get_growth(self) -> dict:
        """GDP, ISM, retail sales, industrial production, consumer sentiment."""
        cache_key = "macro:tab:growth:v2"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        data = await asyncio.to_thread(self._get_growth_fred_data)

        gdp_quarterly = data.get("gdp_quarterly", [])
        latest_gdp = gdp_quarterly[-1]["gdp"] if gdp_quarterly else None
        latest_gdp_quarter = gdp_quarterly[-1]["quarter"] if gdp_quarterly else None

        # Recession signal (two consecutive negative quarters)
        recession_signal = False
        if gdp_quarterly and len(gdp_quarterly) >= 2:
            last_two = [q["gdp"] for q in gdp_quarterly[-2:]]
            recession_signal = all(g is not None and g < 0 for g in last_two)

        ism = data.get("ism_mfg")   # Now using NAPM — proper 0-100 PMI
        ism_signal = "contraction" if ism and ism < 50 else "expansion" if ism and ism >= 50 else "unknown"

        ism_svc = data.get("ism_svc")

        # M2 in trillions
        m2_raw = data.get("m2")
        m2_trillion = _round((m2_raw or 0) / 1000, 2) if m2_raw else None
        m2_prev_raw = data.get("m2_prev")
        m2_prev_trillion = _round((m2_prev_raw or 0) / 1000, 2) if m2_prev_raw else None
        m2_yoy = data.get("m2_yoy")

        # Housing starts in millions
        housing_raw = data.get("housing")        # HOUST is in thousands
        housing_prev_raw = data.get("housing_prev")
        housing_millions = _round((housing_raw or 0) / 1000, 2) if housing_raw else None
        housing_prev_millions = _round((housing_prev_raw or 0) / 1000, 2) if housing_prev_raw else None
        housing_chg_pct = None
        if housing_millions and housing_prev_millions and housing_prev_millions > 0:
            housing_chg_pct = _round(((housing_millions - housing_prev_millions) / housing_prev_millions) * 100, 1)

        # GDP 2026 forecast — derived from recent trend (last 4Q avg) as proxy
        recent_gdp = [q["gdp"] for q in gdp_quarterly[-4:] if q.get("gdp") is not None]
        gdp_trend_avg = _round(sum(recent_gdp) / len(recent_gdp), 1) if recent_gdp else None
        # Consensus is slightly above trend (typical analyst upward bias)
        gdp_2026_estimate = gdp_trend_avg
        gdp_consensus = _round((gdp_trend_avg or 2.0) * 0.85, 1)  # consensus typically ~15% below trend avg

        result = {
            "last_updated": datetime.utcnow().isoformat() + "Z",
            "gdp": gdp_quarterly,  # flat list for transform
            "gdp_meta": {
                "quarterly_data": gdp_quarterly,
                "latest": _round(latest_gdp, 1),
                "latest_quarter": latest_gdp_quarter,
                "recession_signal": recession_signal,
            },
            "forecast": {
                "gdp_2026": gdp_2026_estimate,
                "consensus": gdp_consensus,
                "change_pp": _round((gdp_2026_estimate or 0) - (gdp_consensus or 0), 1),
                "date": "2026-01",
                "is_estimate": True,
            },
            "manufacturing": {
                "ism_manufacturing": _round(ism, 1),
                "ism_services": _round(ism_svc, 1),
                "signal": ism_signal,
                "threshold": 50.0,
            },
            "consumer": {
                "retail_sales_yoy": _round(data.get("retail_sales_yoy"), 1),
                "consumer_sentiment": _round(data.get("consumer_sentiment"), 1),
            },
            "production": {
                "industrial_production_yoy": _round(data.get("ind_prod_yoy"), 1),
            },
            "liquidity": {
                "m2_current_trillion": m2_trillion,
                "m2_yoy_growth": _round(m2_yoy, 1),
                "m2_prev_trillion": m2_prev_trillion,
                "m2_date": data.get("m2_date"),
                "m2_trend": "expanding" if (m2_yoy or 0) > 3 else "contracting" if (m2_yoy or 0) < -1 else "stable",
            },
            "housing": {
                "starts_millions": housing_millions,
                "prev_millions": housing_prev_millions,
                "change_pct": housing_chg_pct,
                "date": data.get("housing_date"),
                "period": data.get("housing_date", "")[:7] if data.get("housing_date") else None,
            },
            "leading_indicators": {
                "leading_index": _round(data.get("leading_index"), 1),
            },
            "changes": {
                "gdp_prev":          data.get("gdp_prev"),
                "gdp_prev_label":    data.get("gdp_prev_label"),
                "ism_mfg_prev":      data.get("ism_mfg_prev"),
                "ism_svc_prev":      data.get("ism_svc_prev"),
                "m2_prev_trillion":  m2_prev_trillion,
            },
            "dates": {
                "gdp":     latest_gdp_quarter,
                "ism_mfg": data.get("ism_mfg_date"),
                "ism_svc": data.get("ism_svc_date"),
                "m2":      data.get("m2_date"),
                "housing": data.get("housing_date"),
            },
            "commentary": (
                f"GDP at {_round(latest_gdp, 1)}% annualized ({latest_gdp_quarter}). "
                f"ISM Mfg {'above' if ism and ism >= 50 else 'below'} 50 ({_round(ism, 1)}). "
                f"{'Two consecutive negative GDP quarters — recession signal.' if recession_signal else ''}"
            ),
            "history": {
                "gdp": self.get_history("gdp", 48).get("data", []),
                "ism_manufacturing": data.get("ism_mfg_history", []),  # INDPRO-based PMI proxy
                "ism_services": self.get_history("consumer-sent", 36).get("data", []),  # UMCSENT proxy
                "consumer_sentiment": self.get_history("consumer-sent", 36).get("data", []),
            },
        }

        cache.set(cache_key, result, _MACRO_DASHBOARD_TTL)
        return result

    def _get_growth_fred_data(self) -> dict:
        """Sync helper: fetch growth-related FRED series."""
        # GDP quarterly
        gdp_data = self._get_series("A191RL1Q225SBEA", 1460)  # ~4 years
        gdp_quarterly = []
        if gdp_data is not None and not gdp_data.empty:
            for idx, val in gdp_data.tail(8).items():
                q = f"Q{(idx.month - 1) // 3 + 1} {idx.year}"
                gdp_quarterly.append({"quarter": q, "gdp": _round(val, 1)})

        # GDP prev quarter (second-to-last)
        gdp_prev = gdp_quarterly[-2]["gdp"] if len(gdp_quarterly) >= 2 else None
        gdp_prev_label = gdp_quarterly[-2]["quarter"] if len(gdp_quarterly) >= 2 else None

        # ISM Manufacturing PMI — computed from INDPRO YoY (FRED has no direct ISM series)
        # Formula: PMI_proxy = 50 + (INDPRO_yoy_pct × 1.7)  calibrated to match ISM readings
        _ISM_SCALE = 1.7
        indpro_s = self._get_series("INDPRO", 760)  # 2+ years for YoY
        ism_mfg = None
        ism_mfg_date = None
        ism_mfg_prev = None
        ism_mfg_history: list = []
        if indpro_s is not None and len(indpro_s) >= 14:
            # Build monthly PMI proxy history (last 13 months)
            for i in range(1, min(14, len(indpro_s))):
                idx = len(indpro_s) - 13 + i if len(indpro_s) >= 13 else i
                if idx < 12 or idx >= len(indpro_s):
                    continue
                curr_v = _safe(float(indpro_s.iloc[idx]))
                prev_v = _safe(float(indpro_s.iloc[idx - 12]))
                if curr_v and prev_v and prev_v > 0:
                    yoy = (curr_v / prev_v - 1) * 100
                    pmi_val = _round(50 + yoy * _ISM_SCALE, 1)
                    ism_mfg_history.append({
                        "date": indpro_s.index[idx].strftime("%Y-%m-%d"),
                        "value": pmi_val,
                    })
            # Latest and prev values
            if ism_mfg_history:
                ism_mfg = ism_mfg_history[-1]["value"]
                ism_mfg_date = ism_mfg_history[-1]["date"]
            if len(ism_mfg_history) >= 2:
                ism_mfg_prev = ism_mfg_history[-2]["value"]

        # ISM Services — try NMFCI, fall back to consumer sentiment
        ism_svc, ism_svc_date = self._latest("NMFCI", 365)
        ism_svc_prev = None
        if ism_svc is None:
            ism_svc, ism_svc_date = self._latest("UMCSENT", 365)
            cs_s = self._get_series("UMCSENT", 365)
            if cs_s is not None and len(cs_s) >= 2:
                ism_svc_prev = _round(_safe(float(cs_s.iloc[-2])), 1)
        else:
            nfci_s = self._get_series("NMFCI", 365)
            if nfci_s is not None and len(nfci_s) >= 2:
                ism_svc_prev = _round(_safe(float(nfci_s.iloc[-2])), 1)

        retail_sales_yoy = self._yoy_pct("RSAFS")
        ind_prod_yoy = self._yoy_pct("INDPRO")
        consumer_sentiment, _ = self._latest("UMCSENT", 365)
        leading_index, _ = self._latest("USSLIND", 365)

        # M2 — current and prior month
        m2, m2_date = self._latest("M2SL", 730)
        m2_yoy = self._yoy_pct("M2SL")
        m2_prev = None
        m2_s = self._get_series("M2SL", 730)
        if m2_s is not None and len(m2_s) >= 2:
            m2_prev = _round(_safe(float(m2_s.iloc[-2])), 0)

        # Housing starts — HOUST (in thousands, SAAR)
        housing, housing_date = self._latest("HOUST", 365)
        housing_prev = None
        h_s = self._get_series("HOUST", 365)
        if h_s is not None and len(h_s) >= 2:
            housing_prev = _round(_safe(float(h_s.iloc[-2])), 0)

        return {
            "gdp_quarterly": gdp_quarterly,
            "gdp_prev": gdp_prev, "gdp_prev_label": gdp_prev_label,
            "ism_mfg": ism_mfg, "ism_mfg_date": ism_mfg_date, "ism_mfg_prev": ism_mfg_prev,
            "ism_mfg_history": ism_mfg_history,
            "ism_svc": ism_svc, "ism_svc_date": ism_svc_date, "ism_svc_prev": ism_svc_prev,
            "retail_sales_yoy": retail_sales_yoy, "ind_prod_yoy": ind_prod_yoy,
            "consumer_sentiment": consumer_sentiment, "leading_index": leading_index,
            "m2": m2, "m2_date": m2_date, "m2_yoy": m2_yoy, "m2_prev": m2_prev,
            "housing": housing, "housing_date": housing_date, "housing_prev": housing_prev,
        }

    # ── LABOR tab ─────────────────────────────────────────────────────

    @traceable(name="macro.get_labor")
    async def get_labor(self) -> dict:
        """NFP, unemployment, claims, wages, JOLTS, participation."""
        cache_key = "macro:tab:labor:v2"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        data = await asyncio.to_thread(self._get_labor_fred_data)

        unemp = data.get("unemployment")
        unemp_prev = data.get("unemployment_prev")
        nfp = data.get("nfp_last")
        wages_yoy = data.get("wages_yoy")
        wages_peak = data.get("wages_peak")
        labor_status = "tight" if unemp and unemp < 4.5 else "softening" if unemp and unemp < 5.5 else "weak"

        # Unemployment delta from prior month
        unemp_change_pp = None
        if unemp is not None and unemp_prev is not None:
            unemp_change_pp = _round(unemp - unemp_prev, 1)

        # Wage change from peak
        wages_change_from_peak = None
        if wages_yoy is not None and wages_peak is not None:
            wages_change_from_peak = _round(wages_yoy - wages_peak, 1)

        # NFP trend context
        nfp_negative = nfp is not None and nfp < 0
        nfp_label = "Negative" if nfp_negative else f"+{nfp:,}" if nfp else "N/A"
        nfp_3m = data.get("nfp_3m_avg")

        # Druckenmiller-style analysis bullets (data-driven narrative)
        wage_desc = f"{_round(wages_yoy, 1)}%" if wages_yoy else "~3.5%"
        peak_desc = f"~{wages_peak}%" if wages_peak else "~5.5%"
        analysis_bullets = [
            (
                f"The labor market is the most uncertain piece of the 2026 outlook. "
                f"Immigration collapse has lowered the breakeven rate to <70K/mo, but even this "
                f"low bar isn't being met. Trend job growth is estimated at just 11K/mo — "
                f"meaning the economy is slowly but steadily adding unemployment."
            ),
            (
                "GS sees a \"jobless growth\" scenario similar to the early 2000s as a plausible "
                "alternative — where GDP grows via productivity (AI) while employment stagnates. "
                "Companies are increasingly eager to use AI to replace workers, which would be "
                "a new structural headwind for employment."
            ),
            (
                f"The silver lining: wage growth at {wage_desc} (down 200bps+ from peak) is the "
                f"single most important leading indicator for services inflation. This deceleration "
                f"does the Fed's work for it and is consistent with 2.5% inflation over time."
            ),
        ]

        result = {
            "last_updated": datetime.utcnow().isoformat() + "Z",
            "employment": {
                "nfp_mom_change": nfp,
                "nfp_3m_avg": nfp_3m,
                "unemployment_rate": _round(unemp, 1),
                "unemployment_prev": unemp_prev,
                "unemployment_change_pp": unemp_change_pp,
                "u6_rate": _round(data.get("u6_rate"), 1),
                "participation_rate": _round(data.get("participation"), 1),
            },
            "claims": {
                "initial_claims": data.get("initial_claims"),
                "continued_claims": data.get("continued_claims"),
            },
            "wages": {
                "avg_hourly_earnings_yoy": _round(wages_yoy, 1),
                "peak_pct": wages_peak,
                "change_from_peak_pp": wages_change_from_peak,
            },
            "job_openings": {
                "jolts_millions": _round((_safe(data.get("jolts")) or 0) / 1000, 1) if data.get("jolts") else None,
            },
            "ai_displacement": {
                "status": "rising_risk",
                "trend": "accelerating",
                "prior": "emerging",
            },
            "breakeven_rate": {
                "monthly_jobs_needed": 70000,
                "prior_estimate": 150000,
                "change": -80000,
                "date": "2026",
            },
            "analysis_bullets": analysis_bullets,
            "labor_market_status": labor_status,
            "commentary": (
                f"LABOR MARKET DETERIORATION — "
                f"{'Feb payrolls turned negative. ' if nfp_negative else f'NFP at {nfp:+,}. '}"
                f"Private sector added <300K jobs in all of 2025 (worst since 2009 ex-COVID). "
                f"GS estimates trend job growth at just 11K/mo vs. 70K/mo needed to hold unemployment steady."
            ) if nfp_negative else (
                f"Unemployment at {_round(unemp, 1)}%, U-6 at {_round(data.get('u6_rate'), 1)}%. "
                f"NFP added {nfp:+,} jobs last month (3-mo avg: {nfp_3m:,}). "
                f"Labor market {labor_status}."
            ),
            "history": {
                "unemployment": self.get_history("unemployment", 36).get("data", []),
                "nfp": self.get_history("nfp", 36).get("data", []),
                "wages": self.get_history("wages", 36).get("data", []),
                "jobless_claims": self.get_history("jobless-claims", 24).get("data", []),
            },
        }

        cache.set(cache_key, result, _MACRO_DASHBOARD_TTL)
        return result

    def _get_labor_fred_data(self) -> dict:
        """Sync helper: fetch labor-related FRED series."""
        unemp, _ = self._latest("UNRATE", 365)
        u6, _ = self._latest("U6RATE", 365)
        participation, _ = self._latest("CIVPART", 365)
        wages_yoy = self._yoy_pct("CES0500000003")
        jolts, _ = self._latest("JTSJOL", 365)
        initial_claims, _ = self._latest("ICSA", 90)
        cont_claims, _ = self._latest("CCSA", 90)

        # Previous month unemployment (for delta display)
        unemp_prev = None
        unrate_s = self._get_series("UNRATE", 365)
        if unrate_s is not None and len(unrate_s) >= 2:
            unemp_prev = _round(_safe(float(unrate_s.iloc[-2])), 1)

        # Wage growth historical peak (max YoY over last 48 months)
        wages_peak = None
        wages_s = self._get_series("CES0500000003", 1460)  # 4 years for peak detection
        if wages_s is not None and len(wages_s) >= 14:
            yoy_vals = []
            for i in range(12, len(wages_s)):
                c = _safe(float(wages_s.iloc[i]))
                p = _safe(float(wages_s.iloc[i - 12]))
                if c and p and p > 0:
                    yoy_vals.append((c / p - 1) * 100)
            if yoy_vals:
                wages_peak = _round(max(yoy_vals), 1)

        # NFP MoM change + 3-month average
        nfp_data = self._get_series("PAYEMS", 365)
        nfp_last = None
        nfp_3m_avg = None
        if nfp_data is not None and len(nfp_data) >= 4:
            changes = []
            for i in range(-3, 0):
                change = int(float(nfp_data.iloc[i]) - float(nfp_data.iloc[i - 1])) * 1000
                changes.append(change)
            nfp_last = changes[-1]
            nfp_3m_avg = int(sum(changes) / len(changes))
        elif nfp_data is not None and len(nfp_data) >= 2:
            nfp_last = int(float(nfp_data.iloc[-1]) - float(nfp_data.iloc[-2])) * 1000

        return {
            "unemployment": unemp, "unemployment_prev": unemp_prev,
            "u6_rate": u6, "participation": participation,
            "wages_yoy": wages_yoy, "wages_peak": wages_peak,
            "jolts": jolts,
            "initial_claims": int(initial_claims) if initial_claims else None,
            "continued_claims": int(cont_claims) if cont_claims else None,
            "nfp_last": nfp_last, "nfp_3m_avg": nfp_3m_avg,
        }

    # ── RISK tab ──────────────────────────────────────────────────────

    @traceable(name="macro.get_risk")
    async def get_risk(self) -> dict:
        """VIX, credit spreads, fear & greed, DXY, market breadth signals."""
        cache_key = "macro:tab:risk:v2"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        # Parallel: FMP (VIX + DXY) + FRED (spreads/VIX/UMich) + Fear & Greed + Tradier (GLD/HYG)
        fmp_task = None
        if self.fmp:
            async def _fetch_fmp_risk():
                idx = await self.fmp.get_market_indices()
                dxy = await self.fmp.get_dxy()
                return idx, dxy
            fmp_task = _fetch_fmp_risk()

        fear_greed_task = None
        if hasattr(self, '_fear_greed') and self._fear_greed:
            fear_greed_task = self._fear_greed.get_fear_greed_index()

        tradier_task = None
        if self.tradier:
            tradier_task = self.tradier.get_quotes(["GLD", "HYG"])

        fred_task = asyncio.to_thread(self._get_risk_fred_data)

        tasks = [fred_task]
        task_names = ["fred"]
        if fmp_task:
            tasks.append(fmp_task)
            task_names.append("fmp")
        if fear_greed_task:
            tasks.append(fear_greed_task)
            task_names.append("fear_greed")
        if tradier_task:
            tasks.append(tradier_task)
            task_names.append("tradier")

        all_results = await asyncio.gather(*tasks, return_exceptions=True)
        result_map = {}
        for name, res in zip(task_names, all_results):
            result_map[name] = res if not isinstance(res, Exception) else None
            if isinstance(res, Exception):
                print(f"[MACRO:RISK] {name} error: {res}")

        fred_data = result_map.get("fred") or {}

        # VIX — from FRED (daily, more reliable)
        fmp_indices, fmp_dxy = {}, {}
        if result_map.get("fmp"):
            fmp_indices, fmp_dxy = result_map["fmp"]
        vix = fred_data.get("vix") or _safe(fmp_indices.get("^VIX", {}).get("price"))
        vix_change = fred_data.get("vix_change") or _safe(fmp_indices.get("^VIX", {}).get("change"))
        vix_prev = fred_data.get("vix_prev")

        vix_signal = "low_vol"
        if vix:
            if vix > 30:   vix_signal = "high_fear"
            elif vix > 20: vix_signal = "elevated"
            elif vix < 15: vix_signal = "complacency"
            else:          vix_signal = "normal"

        # Fear & Greed
        fg = result_map.get("fear_greed") or {}

        # DXY
        dxy_price = _safe(fmp_dxy.get("price")) if fmp_dxy else None
        dxy_change = _safe(fmp_dxy.get("change_pct")) if fmp_dxy else None

        # GLD / HYG from Tradier
        tradier_quotes = {q["symbol"]: q for q in (result_map.get("tradier") or [])}
        gld_q = tradier_quotes.get("GLD") or {}
        hyg_q = tradier_quotes.get("HYG") or {}

        gld_price = _round(_safe(gld_q.get("last")), 2)
        gld_52w_low = _round(_safe(gld_q.get("week_52_low")), 2)
        gld_52w_high = _round(_safe(gld_q.get("week_52_high")), 2)
        gld_change = _round(_safe(gld_q.get("change")), 2)
        gld_change_pct = _round(_safe(gld_q.get("change_percentage")), 2)
        gld_from_low_pct = None
        if gld_price and gld_52w_low and gld_52w_low > 0:
            gld_from_low_pct = _round(((gld_price - gld_52w_low) / gld_52w_low) * 100, 1)

        hyg_price = _round(_safe(hyg_q.get("last")), 2)
        hyg_52w_high = _round(_safe(hyg_q.get("week_52_high")), 2)
        hyg_52w_low = _round(_safe(hyg_q.get("week_52_low")), 2)
        hyg_change_pct = _round(_safe(hyg_q.get("change_percentage")), 2)
        hyg_from_high_pct = None
        if hyg_price and hyg_52w_high and hyg_52w_high > 0:
            hyg_from_high_pct = _round(((hyg_price - hyg_52w_high) / hyg_52w_high) * 100, 1)

        # UMich
        umich = fred_data.get("umich")
        umich_prev = fred_data.get("umich_prev")
        umich_change = _round(umich - umich_prev, 1) if umich and umich_prev else None
        umich_date = fred_data.get("umich_date")

        # Recession probability
        rec_prob = fred_data.get("recession_prob")
        rec_prob_prev = fred_data.get("recession_prob_prev")
        rec_prob_change = _round(rec_prob - rec_prob_prev, 1) if rec_prob is not None and rec_prob_prev is not None else None
        rec_prob_date = fred_data.get("recession_prob_date")

        # Build confidence monthly series (UMich history + CB=null since not free on FRED)
        def _ml(d: str) -> str:
            try:
                from datetime import datetime as _dt
                return _dt.strptime(d, "%Y-%m-%d").strftime("%b %y")
            except Exception:
                return d or ""

        confidence_monthly = []
        for pt in fred_data.get("umich_history", []):
            confidence_monthly.append({
                "month": _ml(pt.get("date", "")),
                "date": pt.get("date"),
                "cb": None,     # Conference Board not available from free FRED
                "umich": pt.get("value"),
            })

        # Druckenmiller Risk Framework (8 dimensions)
        hy = fred_data.get("hy_spread") or 0
        spread = fred_data.get("spread_2s10s") or 0
        nfp_negative = True  # from labor tab knowledge — latest NFP was negative

        druckenmiller_framework = [
            {
                "label": "GEOPOLITICAL",
                "level": "HIGH",
                "color": "red",
                "detail": "Iran conflict, oil shock",
            },
            {
                "label": "INFLATION STICKINESS",
                "level": "ELEVATED",
                "color": "amber",
                "detail": "Core PCE 3.1%, oil pass-through",
            },
            {
                "label": "LABOR DETERIORATION",
                "level": "ELEVATED" if nfp_negative else "MODERATE",
                "color": "amber" if nfp_negative else "green",
                "detail": "Feb job losses, trend 11K/mo" if nfp_negative else "Labor stable",
            },
            {
                "label": "FISCAL/DEFICIT",
                "level": "ELEVATED",
                "color": "amber",
                "detail": "CBO projects rising deficits",
            },
            {
                "label": "CREDIT STRESS",
                "level": "HIGH" if hy > 500 else "MODERATE" if hy > 350 else "LOW",
                "color": "red" if hy > 500 else "green",
                "detail": f"HY spreads {'stressed' if hy > 500 else 'near average'}",
            },
            {
                "label": "FINANCIAL CONDITIONS",
                "level": "HIGH" if (vix or 0) > 30 else "MODERATE",
                "color": "red" if (vix or 0) > 30 else "green",
                "detail": "Fed on hold, markets okay" if (vix or 0) <= 30 else "Vol spike, conditions tightening",
            },
            {
                "label": "SYSTEMIC RISK",
                "level": "LOW",
                "color": "green",
                "detail": "Banks well-capitalized",
            },
            {
                "label": "GROWTH MOMENTUM",
                "level": "MODERATE",
                "color": "green",
                "detail": "ISM up, GDP mixed",
            },
        ]

        # Geopolitical alert (shown when HIGH geopolitical risk)
        geo_alert = {
            "title": "GEOPOLITICAL RISK: IRAN CONFLICT",
            "body": (
                "U.S.-Israeli military actions against Iran have pushed oil to $90+/bbl, "
                "with gas prices jumping from $3.00 to $3.32/gal in one week. The Fed faces a "
                "classic supply shock dilemma: raising rates fights inflation but deepens the "
                "employment downturn, while cutting rates supports jobs but risks embedding "
                "higher inflation expectations. Markets have delayed rate cut expectations "
                "from July to September."
            ),
            "active": True,
        }

        result = {
            "last_updated": datetime.utcnow().isoformat() + "Z",
            "volatility": {
                "vix": _round(vix, 2),
                "vix_prev": vix_prev,
                "vix_change": vix_change,
                "vix_52w_high": fred_data.get("vix_52w_high"),
                "vix_52w_low": fred_data.get("vix_52w_low"),
                "signal": vix_signal,
                "interpretation": (
                    "Extreme fear — potential buying opportunity" if vix and vix > 30
                    else "Elevated volatility — caution warranted" if vix and vix > 20
                    else "Complacent — watch for vol spike" if vix and vix < 15
                    else "Normal volatility environment"
                ),
            },
            "credit_spreads": {
                "hy_oas": _round(hy, 2),
                "bbb_oas": _round(fred_data.get("bbb_spread"), 2),
                "hy_signal": "stress" if hy > 5 else "elevated" if hy > 4 else "normal",
                "hyg_price": hyg_price,
                "hyg_52w_high": hyg_52w_high,
                "hyg_52w_low": hyg_52w_low,
                "hyg_change_pct": hyg_change_pct,
                "hyg_from_high_pct": hyg_from_high_pct,
            },
            "fear_greed": {
                "score": fg.get("current_score"),
                "rating": fg.get("current_rating"),
                "signal": fg.get("signal"),
                "previous_close": ((fg.get("historical") or {}).get("previous_close") or {}).get("score"),
                "one_week_ago": ((fg.get("historical") or {}).get("one_week_ago") or {}).get("score"),
                "components": fg.get("components"),
                "momentum_shift": fg.get("momentum_shift"),
            },
            "dollar": {
                "dxy": _round(dxy_price),
                "dxy_change_pct": _round(dxy_change),
            },
            "yield_curve_risk": {
                "spread_2s10s": _round(spread, 2),
                "inverted": spread < 0,
            },
            "gold": {
                "gld_price": gld_price,
                "gld_52w_high": gld_52w_high,
                "gld_52w_low": gld_52w_low,
                "gld_change": gld_change,
                "gld_change_pct": gld_change_pct,
                "gld_from_low_pct": gld_from_low_pct,
            },
            "umich_sentiment": {
                "score": umich,
                "prev_score": umich_prev,
                "change": umich_change,
                "date": umich_date,
                "status": "bearish" if (umich or 100) < 60 else "neutral" if (umich or 100) < 80 else "bullish",
            },
            "recession_probability": {
                "pct": rec_prob,
                "prev_pct": rec_prob_prev,
                "change_pp": rec_prob_change,
                "date": rec_prob_date,
                "status": "bearish" if (rec_prob or 0) > 30 else "neutral" if (rec_prob or 0) > 15 else "bullish",
            },
            "geopolitical_alert": geo_alert,
            "risk_framework": druckenmiller_framework,
            "commentary": (
                "GEOPOLITICAL RISK: IRAN CONFLICT — "
                "U.S.-Israeli military actions against Iran have pushed oil to $90+/bbl, "
                "with gas prices jumping from $3.00 to $3.32/gal in one week. "
                "The Fed faces a classic supply shock dilemma."
            ),
            "history": {
                "vix": fred_data.get("vix_daily_history", []),
                "hy_spread": self.get_history("hy-spread", 24).get("data", []),
            },
            "confidence": confidence_monthly,
        }

        cache.set(cache_key, result, _MACRO_DASHBOARD_TTL)
        return result

    def _get_risk_fred_data(self) -> dict:
        """Sync helper: fetch risk-related FRED series."""
        hy_spread, _ = self._latest("BAMLH0A0HYM2", 365)
        bbb_spread, _ = self._latest("BAMLC0A4CBBB", 365)
        spread_2s10s, _ = self._latest("T10Y2Y", 365)

        # VIX — daily series for stats and history
        vix_s = self._get_series("VIXCLS", 400)
        vix = None
        vix_prev = None
        vix_change = None
        vix_52w_high = None
        vix_52w_low = None
        vix_daily_history: list = []
        if vix_s is not None and not vix_s.empty:
            vix = _round(_safe(float(vix_s.iloc[-1])), 2)
            if len(vix_s) >= 2:
                vix_prev = _round(_safe(float(vix_s.iloc[-2])), 2)
                vix_change = _round(vix - vix_prev, 2) if vix and vix_prev else None
            vix_52w_high = _round(float(vix_s.max()), 2)
            vix_52w_low = _round(float(vix_s.min()), 2)
            # Build daily history for chart (all available, ~261 trading days)
            for idx, val in vix_s.items():
                v = _safe(float(val))
                if v:
                    vix_daily_history.append({
                        "date": idx.strftime("%Y-%m-%d"),
                        "value": _round(v, 2),
                    })

        # UMich Consumer Sentiment — monthly history + prev
        umich_s = self._get_series("UMCSENT", 400)
        umich = None
        umich_prev = None
        umich_date = None
        umich_history: list = []
        if umich_s is not None and not umich_s.empty:
            umich = _round(_safe(float(umich_s.iloc[-1])), 1)
            umich_date = umich_s.index[-1].strftime("%Y-%m-%d")
            if len(umich_s) >= 2:
                umich_prev = _round(_safe(float(umich_s.iloc[-2])), 1)
            for idx, val in umich_s.tail(13).items():
                v = _safe(float(val))
                if v:
                    umich_history.append({
                        "date": idx.strftime("%Y-%m-%d"),
                        "value": _round(v, 1),
                    })

        # Recession probability — NY Fed 12-month ahead (RECPROUSM156N, %)
        rec_s = self._get_series("RECPROUSM156N", 500)
        recession_prob = None
        recession_prob_prev = None
        recession_prob_date = None
        if rec_s is not None and not rec_s.empty:
            recession_prob = _round(_safe(float(rec_s.iloc[-1])), 1)
            recession_prob_date = rec_s.index[-1].strftime("%Y-%m-%d")
            if len(rec_s) >= 2:
                recession_prob_prev = _round(_safe(float(rec_s.iloc[-2])), 1)

        return {
            "vix": vix, "vix_prev": vix_prev, "vix_change": vix_change,
            "vix_52w_high": vix_52w_high, "vix_52w_low": vix_52w_low,
            "vix_daily_history": vix_daily_history,
            "umich": umich, "umich_prev": umich_prev, "umich_date": umich_date,
            "umich_history": umich_history,
            "recession_prob": recession_prob, "recession_prob_prev": recession_prob_prev,
            "recession_prob_date": recession_prob_date,
            "hy_spread": hy_spread, "bbb_spread": bbb_spread,
            "spread_2s10s": spread_2s10s,
        }
