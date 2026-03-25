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
    "core-pce":         "PCEPILFE",
    "ppi":              "PPIFIS",
    "unemployment":     "UNRATE",
    "gdp":              "A191RL1Q225SBEA",
    "nfp":              "PAYEMS",
    "wages":            "CES0500000003",       # Avg hourly earnings
    "jolts":            "JTSJOL",              # JOLTS openings
    "10y-yield":        "DGS10",
    "2y-yield":         "DGS2",
    "2s10s-spread":     "T10Y2Y",
    "10y3m-spread":     "T10Y3M",
    "mortgage-30y":     "MORTGAGE30US",
    "m2":               "M2SL",
    "ism-manufacturing": "MANEMP",             # Mfg employment as ISM proxy
    "vix":              "VIXCLS",
    "jobless-claims":   "ICSA",
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

    def __init__(self, fred_provider, fmp_provider=None):
        self.fred = fred_provider          # FredProvider instance
        self.fmp = fmp_provider            # FMPProvider instance (optional)
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

        # ── Fetch FMP real-time market data (async) ──────────────────
        fmp_indices = {}
        fmp_treasury = {}
        fmp_commodities = {}
        if self.fmp:
            try:
                idx_task = self.fmp.get_market_indices()
                treas_task = self.fmp.get_treasury_rates()
                comm_task = self.fmp.get_key_commodities()
                idx_raw, treas_raw, comm_raw = await asyncio.gather(
                    idx_task, treas_task, comm_task, return_exceptions=True,
                )
                fmp_indices = idx_raw if not isinstance(idx_raw, Exception) else {}
                fmp_treasury = treas_raw if not isinstance(treas_raw, Exception) else {}
                fmp_commodities = comm_raw if not isinstance(comm_raw, Exception) else {}
            except Exception as e:
                print(f"[MACRO] FMP real-time fetch error: {e}")

        # Extract real-time values from FMP
        sp500 = fmp_indices.get("^GSPC", {})
        vix_data = fmp_indices.get("^VIX", {})
        vix_price = _safe(vix_data.get("price"))
        us10y_rt = _safe(fmp_treasury.get("year_10"))
        us2y_rt = _safe(fmp_treasury.get("year_2"))
        us30y_rt = _safe(fmp_treasury.get("year_30"))
        oil_price = _safe(fmp_commodities.get("CLUSD", {}).get("price"))
        gold_price = _safe(fmp_commodities.get("GCUSD", {}).get("price"))
        gas_price = _safe(fmp_commodities.get("NGUSD", {}).get("price"))

        # Compute spreads from real-time yields
        spread_2s10s_rt = round(us10y_rt - us2y_rt, 2) if us10y_rt and us2y_rt else None
        us3m_rt = _safe(fmp_treasury.get("month_3"))
        spread_10y3m_rt = round(us10y_rt - us3m_rt, 2) if us10y_rt and us3m_rt else None

        # ── Fetch FRED economic releases (sync — run in thread pool) ─
        fred_data = await asyncio.to_thread(self._get_fred_economic_data)

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
                "signal": "low fear" if (_safe(vix) or 20) < 18 else "elevated" if (_safe(vix) or 20) < 25 else "high fear",
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

        ism_mfg, _ = self._latest("MANEMP", 365)

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
        _add("ISM Manufacturing (Emp)", "MANEMP", "https://fred.stlouisfed.org/series/MANEMP",
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
