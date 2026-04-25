"""
Stock Compare service — multi-ticker financial metric comparison.

FMP Stable API is the primary data source.  SEC EDGAR is fallback/enrichment only.
No AI/Grok calls are made.

Architecture:
  StockCompareFMP  — lightweight FMP client with stock-compare-specific cache keys
                     and TTLs.  Adds balance_sheet, cash_flow, ratios, key_metrics,
                     financial_growth, and search on top of the existing FMPProvider.
  search_symbols() — ticker / company-name autocomplete
  compare_metrics()— main comparison entry point: validates, fetches, normalises
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import httpx

from data.cache import cache

# ── Constants ─────────────────────────────────────────────────────────────────

FMP_STABLE = "https://financialmodelingprep.com/stable"

# Cache TTLs for stock-compare data
_TTL_SEARCH    = 86_400   # 24 h  — symbol search
_TTL_PROFILE   = 86_400   # 24 h  — company profile
_TTL_STATEMENT = 86_400   # 24 h  — annual income / balance / cash flow
_TTL_RATIOS    = 86_400   # 24 h  — key metrics / ratios
_TTL_GROWTH    = 86_400   # 24 h  — growth endpoints
_TTL_QUOTE     = 900      # 15 min — live price / market cap
_TTL_NEWS      = 1_800    # 30 min — stock news
_TTL_HIST_MC   = 1_800    # 30 min — historical market-cap daily series

# Colour keys for chart series (frontend maps these to actual hex colours)
_COLOR_KEYS = [
    "blue", "red", "green", "orange", "purple",
    "teal", "pink", "yellow", "indigo", "cyan",
    "lime", "amber", "rose", "violet", "emerald",
]

# Supported ranges → (max_limit, timedelta_or_None)
_RANGE_CONFIG: dict[str, tuple[int, Optional[int]]] = {
    "1M":  (5,   31),
    "3M":  (5,   92),
    "6M":  (5,  183),
    "YTD": (5,  None),   # special-cased: days_since_jan_1
    "1Y":  (5,   365),
    "3Y":  (5,  1095),
    "5Y":  (7,  1825),
    "10Y": (12, 3650),
    "MAX": (40, None),
    "ALL": (40, None),
}

VALID_RANGES = set(_RANGE_CONFIG.keys())

# Canonical metric definitions (key → label, unit, source category)
METRIC_MAP: dict[str, dict] = {
    "market_cap":       {"label": "Market Cap",          "unit": "currency",  "source": "market_cap"},
    "revenue":          {"label": "Revenue",              "unit": "currency",  "source": "income"},
    "revenue_growth":   {"label": "Revenue Growth (YoY)", "unit": "percent",   "source": "growth"},
    "gross_profit":     {"label": "Gross Profit",         "unit": "currency",  "source": "income"},
    "gross_margin":     {"label": "Gross Margin",         "unit": "percent",   "source": "income"},
    "profit_margin":    {"label": "Net Profit Margin",    "unit": "percent",   "source": "income"},
    "eps_diluted":      {"label": "EPS (Diluted)",        "unit": "currency",  "source": "income"},
    "operating_income": {"label": "Operating Income",     "unit": "currency",  "source": "income"},
    "net_income":       {"label": "Net Income",           "unit": "currency",  "source": "income"},
    "ebitda":           {"label": "EBITDA",               "unit": "currency",  "source": "income"},
    "free_cash_flow":   {"label": "Free Cash Flow",       "unit": "currency",  "source": "cashflow"},
    "total_debt":       {"label": "Total Debt",           "unit": "currency",  "source": "balance"},
    "ps_ratio":         {"label": "P/S Ratio",            "unit": "ratio",     "source": "ratios"},
    "pe_ratio":         {"label": "P/E Ratio",            "unit": "ratio",     "source": "ratios"},
    "recent_news":      {"label": "Recent News",          "unit": "news",      "source": "news"},
}

# Metric aliases → canonical key
METRIC_ALIASES: dict[str, str] = {
    "price_to_sales": "ps_ratio",
    "p_s_ratio":      "ps_ratio",
    "p_e_ratio":      "pe_ratio",
    "eps":            "eps_diluted",
    "fcf":            "free_cash_flow",
    "debt":           "total_debt",
}

VALID_METRICS = set(METRIC_MAP.keys()) | set(METRIC_ALIASES.keys())
VALID_PERIODS = {"annual", "quarterly"}


# ── FMP client (compare-specific) ─────────────────────────────────────────────

class StockCompareFMP:
    """Minimal FMP Stable API client with compare-specific cache keys and TTLs."""

    def __init__(self, api_key: str):
        self._key = api_key

    async def _get(
        self,
        endpoint: str,
        params: dict,
        cache_key: str,
        ttl: int,
    ) -> Any:
        hit = cache.get(cache_key)
        if hit is not None:
            return hit
        p = dict(params)
        p["apikey"] = self._key
        try:
            async with httpx.AsyncClient(timeout=12.0) as client:
                resp = await client.get(f"{FMP_STABLE}/{endpoint}", params=p)
            if resp.status_code not in (200, 201):
                if resp.status_code not in (402, 403, 404):
                    print(f"[StockCompare] FMP {endpoint} HTTP {resp.status_code}")
                return []
            result = resp.json()
            if result:
                cache.set(cache_key, result, ttl)
            return result
        except Exception as e:
            print(f"[StockCompare] FMP {endpoint} error: {e}")
            return []

    async def search(self, query: str, limit: int = 10) -> list:
        ck = f"sc:search:{query.lower()}:{limit}"
        return await self._get("search", {"query": query, "limit": limit}, ck, _TTL_SEARCH)

    async def profile(self, symbol: str) -> dict:
        ck = f"sc:profile:{symbol}"
        data = await self._get("profile", {"symbol": symbol}, ck, _TTL_PROFILE)
        return data[0] if isinstance(data, list) and data else (data if isinstance(data, dict) else {})

    async def quote(self, symbol: str) -> dict:
        ck = f"sc:quote:{symbol}"
        data = await self._get("quote", {"symbol": symbol}, ck, _TTL_QUOTE)
        return data[0] if isinstance(data, list) and data else (data if isinstance(data, dict) else {})

    async def income_statement(self, symbol: str, period: str = "annual", limit: int = 40) -> list:
        ck = f"sc:income:{symbol}:{period}:{limit}"
        data = await self._get(
            "income-statement",
            {"symbol": symbol, "period": period, "limit": limit},
            ck, _TTL_STATEMENT,
        )
        return data if isinstance(data, list) else []

    async def balance_sheet(self, symbol: str, period: str = "annual", limit: int = 40) -> list:
        ck = f"sc:balance:{symbol}:{period}:{limit}"
        data = await self._get(
            "balance-sheet-statement",
            {"symbol": symbol, "period": period, "limit": limit},
            ck, _TTL_STATEMENT,
        )
        return data if isinstance(data, list) else []

    async def cash_flow(self, symbol: str, period: str = "annual", limit: int = 40) -> list:
        ck = f"sc:cashflow:{symbol}:{period}:{limit}"
        data = await self._get(
            "cash-flow-statement",
            {"symbol": symbol, "period": period, "limit": limit},
            ck, _TTL_STATEMENT,
        )
        return data if isinstance(data, list) else []

    async def key_metrics(self, symbol: str, period: str = "annual", limit: int = 40) -> list:
        ck = f"sc:keymetrics:{symbol}:{period}:{limit}"
        data = await self._get(
            "key-metrics",
            {"symbol": symbol, "period": period, "limit": limit},
            ck, _TTL_RATIOS,
        )
        return data if isinstance(data, list) else []

    async def ratios(self, symbol: str, period: str = "annual", limit: int = 40) -> list:
        ck = f"sc:ratios:{symbol}:{period}:{limit}"
        data = await self._get(
            "ratios",
            {"symbol": symbol, "period": period, "limit": limit},
            ck, _TTL_RATIOS,
        )
        return data if isinstance(data, list) else []

    async def financial_growth(self, symbol: str, period: str = "annual", limit: int = 40) -> list:
        ck = f"sc:growth:{symbol}:{period}:{limit}"
        data = await self._get(
            "financial-growth",
            {"symbol": symbol, "period": period, "limit": limit},
            ck, _TTL_GROWTH,
        )
        return data if isinstance(data, list) else []

    async def hist_market_cap(self, symbol: str, limit: int = 40) -> list:
        """Historical daily market cap (annual approximation via yearly samples)."""
        ck = f"sc:histmc:{symbol}:{limit}"
        data = await self._get(
            "historical-market-capitalization",
            {"symbol": symbol, "limit": 365 * (limit // 5 + 1)},
            ck, _TTL_HIST_MC,
        )
        return data if isinstance(data, list) else []

    async def news(self, symbol: str, limit: int = 5) -> list:
        ck = f"sc:news:{symbol}:{limit}"
        data = await self._get(
            "news/stock",
            {"symbols": symbol, "limit": limit},
            ck, _TTL_NEWS,
        )
        return data if isinstance(data, list) else []


# ── Formatting helpers ─────────────────────────────────────────────────────────

def _fmt(value: Optional[float], unit: str) -> Optional[str]:
    if value is None:
        return None
    if unit == "currency":
        av = abs(value)
        sign = "-" if value < 0 else ""
        if av >= 1e12:
            return f"{sign}${av / 1e12:.2f}T"
        if av >= 1e9:
            return f"{sign}${av / 1e9:.2f}B"
        if av >= 1e6:
            return f"{sign}${av / 1e6:.2f}M"
        if av >= 1e3:
            return f"{sign}${av / 1e3:.2f}K"
        return f"{sign}${av:.2f}"
    if unit == "percent":
        return f"{value * 100:.1f}%"
    if unit == "ratio":
        return f"{value:.2f}x"
    return str(round(value, 4))


def _safe_div(num: Optional[float], denom: Optional[float]) -> Optional[float]:
    if num is None or denom is None or denom == 0:
        return None
    return num / denom


# ── Range / cutoff helpers ─────────────────────────────────────────────────────

def _cutoff_date(range_val: str) -> Optional[datetime]:
    """Return earliest date to include in the series, or None for all data."""
    rv = range_val.upper()
    if rv in ("MAX", "ALL"):
        return None
    today = datetime.now(timezone.utc)
    if rv == "YTD":
        return today.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    cfg = _RANGE_CONFIG.get(rv)
    if cfg is None:
        return None
    _, days = cfg
    if days is None:
        return None
    return today - timedelta(days=days)


def _range_limit(range_val: str) -> int:
    cfg = _RANGE_CONFIG.get(range_val.upper(), (40, None))
    return cfg[0]


def _filter_by_range(
    rows: list[dict],
    date_key: str,
    range_val: str,
) -> list[dict]:
    """Filter a list of {date_key: 'YYYY-MM-DD', ...} rows to the range window."""
    cutoff = _cutoff_date(range_val)
    if cutoff is None:
        return rows
    out = []
    for row in rows:
        raw = row.get(date_key) or ""
        if not raw:
            continue
        try:
            d = datetime.fromisoformat(raw[:10])
            d = d.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        if d >= cutoff:
            out.append(row)
    return out


# ── Symbol search ──────────────────────────────────────────────────────────────

# Exchange / type preference for deduplication
_PREFER_EXCHANGES = {"NASDAQ", "NYSE", "AMEX", "NYSE ARCA", "NYSE MKT"}
_SKIP_TYPES = {"ETF", "FUND", "MUTUAL FUND", "WARRANT", "UNIT", "RIGHT"}


async def search_symbols(
    query: str,
    limit: int,
    fmp_key: str,
) -> dict:
    """
    Search FMP for tickers / company names matching `query`.
    Deduplicates cross-exchange variants; prefers US common stocks.
    """
    fmp = StockCompareFMP(fmp_key)
    raw = await fmp.search(query, limit=min(limit * 3, 50))  # fetch extra to filter

    if not isinstance(raw, list):
        raw = []

    # Deduplicate by symbol (prefer US exchange)
    seen: dict[str, dict] = {}
    for item in raw:
        if not isinstance(item, dict):
            continue
        sym  = (item.get("symbol") or "").upper().strip()
        name = item.get("name") or ""
        exc  = (item.get("exchangeShortName") or item.get("exchange") or "").upper()
        typ  = (item.get("type") or "").upper()

        if not sym or not name:
            continue

        # Skip clearly non-stock types
        skip = any(t in typ for t in _SKIP_TYPES)

        if sym not in seen:
            seen[sym] = {
                "symbol":    sym,
                "name":      name,
                "exchange":  exc,
                "type":      (item.get("type") or "stock").lower(),
                "currency":  item.get("currency") or "USD",
                "cik":       item.get("cik") or "",
                "sector":    item.get("sector") or "",
                "industry":  item.get("industry") or "",
                "marketCap": item.get("marketCap"),
                "_skip":     skip,
                "_score":    1 if exc in _PREFER_EXCHANGES else 0,
            }
        else:
            # Prefer US exchange variant
            if exc in _PREFER_EXCHANGES and seen[sym]["_score"] == 0:
                seen[sym].update({
                    "exchange": exc,
                    "type":     (item.get("type") or "stock").lower(),
                    "_score":   1,
                    "_skip":    skip,
                })

    results = [
        {k: v for k, v in r.items() if not k.startswith("_")}
        for r in sorted(seen.values(), key=lambda x: (-x["_score"], x["symbol"]))
        if not r["_skip"]
    ]

    # If nothing passes the filter, include everything
    if not results:
        results = [
            {k: v for k, v in r.items() if not k.startswith("_")}
            for r in seen.values()
        ]

    results = results[:limit]

    return {
        "query":   query,
        "results": results,
        "source":  "fmp",
        "cached":  False,
    }


# ── Per-symbol data fetch ──────────────────────────────────────────────────────

async def _fetch_symbol_data(
    symbol: str,
    metric_source: str,
    period: str,
    limit: int,
    fmp: StockCompareFMP,
) -> dict:
    """
    Fetch only the FMP endpoints needed for this metric.
    Always fetches profile + quote (needed for snapshot).
    Additional endpoints depend on metric_source.
    """
    tasks: dict[str, Any] = {
        "profile": fmp.profile(symbol),
        "quote":   fmp.quote(symbol),
    }

    if metric_source in ("income", "growth"):
        tasks["income"] = fmp.income_statement(symbol, period, limit)
    if metric_source == "growth":
        tasks["growth"] = fmp.financial_growth(symbol, period, limit)
    if metric_source == "cashflow":
        tasks["income"]   = fmp.income_statement(symbol, period, 5)   # snapshot only
        tasks["cashflow"] = fmp.cash_flow(symbol, period, limit)
    if metric_source == "balance":
        tasks["income"]   = fmp.income_statement(symbol, period, 5)   # snapshot only
        tasks["balance"]  = fmp.balance_sheet(symbol, period, limit)
    if metric_source == "ratios":
        tasks["income"]   = fmp.income_statement(symbol, period, 5)   # snapshot only
        tasks["metrics"]  = fmp.key_metrics(symbol, period, limit)
    if metric_source == "market_cap":
        tasks["income"]   = fmp.income_statement(symbol, period, 5)   # snapshot only
        tasks["hist_mc"]  = fmp.hist_market_cap(symbol, limit)
    if metric_source == "news":
        tasks["income"]   = fmp.income_statement(symbol, period, 5)   # snapshot only
        tasks["news"]     = fmp.news(symbol, limit=5)

    # Always fetch income for snapshot if not already fetched
    if "income" not in tasks:
        tasks["income"] = fmp.income_statement(symbol, period, 5)
    # Always fetch cash flow + balance + key_metrics for full snapshot
    tasks["snapshot_cf"]  = fmp.cash_flow(symbol, period, 2)
    tasks["snapshot_bs"]  = fmp.balance_sheet(symbol, period, 2)
    tasks["snapshot_km"]  = fmp.key_metrics(symbol, period, 2)

    keys   = list(tasks.keys())
    values = await asyncio.gather(*tasks.values(), return_exceptions=True)

    result: dict[str, Any] = {}
    for k, v in zip(keys, values):
        result[k] = [] if isinstance(v, Exception) else v

    return result


# ── Value extraction per metric ────────────────────────────────────────────────

def _extract_series_points(
    symbol: str,
    metric_key: str,
    data: dict,
    period: str,
    range_val: str,
    unit: str,
    warnings: list[str],
) -> tuple[list[dict], Optional[dict]]:
    """
    Extract (date, value) points for the selected metric from fetched data.
    Returns (points_list, latest_point).
    """
    points: list[dict] = []
    source_label = "fmp"

    metric_source = METRIC_MAP[metric_key]["source"]

    if metric_source == "income":
        rows = _filter_by_range(data.get("income") or [], "date", range_val)
        for row in rows:
            v = _income_value(metric_key, row, symbol, warnings)
            if v is not None:
                pts = {
                    "date":       row.get("date") or "",
                    "fiscalYear": row.get("fiscalYear") or row.get("calendarYear"),
                    "value":      v,
                    "formatted":  _fmt(v, unit),
                    "source":     "fmp_income_statement",
                }
                points.append(pts)

    elif metric_source == "growth":
        rows = _filter_by_range(data.get("growth") or [], "date", range_val)
        for row in rows:
            v = row.get("revenueGrowth")
            if v is None:
                # fallback: calculate from income statement
                inc = data.get("income") or []
                v = _calc_revenue_growth_from_income(inc, row.get("date"))
            if v is not None:
                points.append({
                    "date":       row.get("date") or "",
                    "fiscalYear": row.get("fiscalYear") or row.get("calendarYear"),
                    "value":      v,
                    "formatted":  _fmt(v, unit),
                    "source":     "fmp_financial_growth",
                })

    elif metric_source == "cashflow":
        rows = _filter_by_range(data.get("cashflow") or [], "date", range_val)
        for row in rows:
            v = _cashflow_value(metric_key, row, symbol, warnings)
            if v is not None:
                points.append({
                    "date":       row.get("date") or "",
                    "fiscalYear": row.get("fiscalYear") or row.get("calendarYear"),
                    "value":      v,
                    "formatted":  _fmt(v, unit),
                    "source":     "fmp_cash_flow_statement",
                })

    elif metric_source == "balance":
        rows = _filter_by_range(data.get("balance") or [], "date", range_val)
        for row in rows:
            v = _balance_value(metric_key, row, symbol, warnings)
            if v is not None:
                points.append({
                    "date":       row.get("date") or "",
                    "fiscalYear": row.get("fiscalYear") or row.get("calendarYear"),
                    "value":      v,
                    "formatted":  _fmt(v, unit),
                    "source":     "fmp_balance_sheet",
                })

    elif metric_source == "ratios":
        rows = _filter_by_range(data.get("metrics") or [], "date", range_val)
        for row in rows:
            v = _ratio_value(metric_key, row, symbol, warnings)
            if v is not None:
                points.append({
                    "date":       row.get("date") or "",
                    "fiscalYear": row.get("fiscalYear") or row.get("calendarYear"),
                    "value":      v,
                    "formatted":  _fmt(v, unit),
                    "source":     "fmp_key_metrics",
                })

    elif metric_source == "market_cap":
        hist = data.get("hist_mc") or []
        # hist_mc is daily — sample ~annually (every ~252 rows)
        prof = data.get("profile") or {}
        q    = data.get("quote") or {}
        # Use profile/quote for current MC, then walk back through hist
        annual_rows = _sample_annual_hist_mc(hist, range_val)
        # Also add current MC if not already covered
        cur_mc = q.get("marketCap") or prof.get("mktCap")
        if cur_mc and (not annual_rows or annual_rows[0].get("date") != datetime.now(timezone.utc).strftime("%Y-%m-%d")):
            points.append({
                "date":       datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                "fiscalYear": datetime.now(timezone.utc).year,
                "value":      cur_mc,
                "formatted":  _fmt(cur_mc, unit),
                "source":     "fmp_quote",
            })
        for row in annual_rows:
            mc = row.get("marketCap")
            if mc:
                points.append({
                    "date":       row.get("date") or "",
                    "fiscalYear": int((row.get("date") or "0000")[:4]),
                    "value":      mc,
                    "formatted":  _fmt(mc, unit),
                    "source":     "fmp_hist_market_cap",
                })

    elif metric_source == "news":
        pass  # news handled separately

    # Deduplicate by date, keep latest value if same date appears twice
    seen_dates: dict[str, dict] = {}
    for pt in points:
        d = pt["date"]
        if d not in seen_dates:
            seen_dates[d] = pt
    points = sorted(seen_dates.values(), key=lambda x: x["date"])

    latest = points[-1] if points else None
    return points, latest


def _income_value(
    metric_key: str,
    row: dict,
    symbol: str,
    warnings: list[str],
) -> Optional[float]:
    if metric_key == "revenue":
        return row.get("revenue")
    if metric_key == "gross_profit":
        return row.get("grossProfit")
    if metric_key == "operating_income":
        return row.get("operatingIncome")
    if metric_key == "net_income":
        return row.get("netIncome")
    if metric_key == "eps_diluted":
        return row.get("epsDiluted") or row.get("eps")
    if metric_key == "ebitda":
        v = row.get("ebitda")
        if v is None:
            # fallback: operatingIncome + D&A (not always available)
            v = row.get("operatingIncome")
        return v
    if metric_key == "gross_margin":
        gp  = row.get("grossProfit")
        rev = row.get("revenue")
        v   = _safe_div(gp, rev)
        if v is None:
            warnings.append(f"{symbol}: gross margin missing for {row.get('date')}")
        return v
    if metric_key == "profit_margin":
        ni  = row.get("netIncome")
        rev = row.get("revenue")
        v   = _safe_div(ni, rev)
        if v is None:
            warnings.append(f"{symbol}: profit margin missing for {row.get('date')}")
        return v
    return None


def _cashflow_value(
    metric_key: str,
    row: dict,
    symbol: str,
    warnings: list[str],
) -> Optional[float]:
    if metric_key != "free_cash_flow":
        return None
    fcf = row.get("freeCashFlow")
    if fcf is not None:
        return fcf
    # Fallback: operatingCashFlow - |capex|
    ocf  = row.get("operatingCashFlow")
    capex = row.get("capitalExpenditure")
    if ocf is not None and capex is not None:
        return ocf - abs(capex)
    warnings.append(f"{symbol}: free cash flow missing for {row.get('date')}")
    return None


def _balance_value(
    metric_key: str,
    row: dict,
    symbol: str,
    warnings: list[str],
) -> Optional[float]:
    if metric_key != "total_debt":
        return None
    v = row.get("totalDebt")
    if v is not None:
        return v
    # Fallback
    st = row.get("shortTermDebt") or 0
    lt = row.get("longTermDebt")  or 0
    if st or lt:
        return st + lt
    warnings.append(f"{symbol}: total debt missing for {row.get('date')}")
    return None


def _ratio_value(
    metric_key: str,
    row: dict,
    symbol: str,
    warnings: list[str],
) -> Optional[float]:
    if metric_key == "ps_ratio":
        v = row.get("priceToSalesRatio") or row.get("revenuePerShare")
        if v is None:
            warnings.append(f"{symbol}: P/S ratio missing for {row.get('date')}")
        return v
    if metric_key == "pe_ratio":
        v = row.get("peRatio") or row.get("priceEarningsRatio")
        if v is None:
            warnings.append(f"{symbol}: P/E ratio missing for {row.get('date')}")
            return None
        if v < 0:
            warnings.append(f"{symbol}: P/E negative (negative earnings) for {row.get('date')}")
            return None   # skip negative P/E — meaningless for chart
        return v
    return None


def _calc_revenue_growth_from_income(income_rows: list, target_date: Optional[str]) -> Optional[float]:
    """Fallback: calculate YoY revenue growth from income statement rows."""
    if len(income_rows) < 2 or not target_date:
        return None
    for i, row in enumerate(income_rows):
        if (row.get("date") or "")[:10] == target_date[:10]:
            if i + 1 < len(income_rows):
                cur_rev  = row.get("revenue")
                prev_rev = income_rows[i + 1].get("revenue")
                return _safe_div(
                    (cur_rev - prev_rev) if (cur_rev is not None and prev_rev is not None) else None,
                    prev_rev,
                )
    return None


def _sample_annual_hist_mc(rows: list, range_val: str) -> list:
    """
    Sample ~annual data points from the daily historical market cap rows.
    Takes the last row within each calendar year.
    """
    if not rows:
        return []
    cutoff = _cutoff_date(range_val)
    by_year: dict[int, dict] = {}
    for row in rows:
        raw = row.get("date") or ""
        if not raw:
            continue
        try:
            d = datetime.fromisoformat(raw[:10])
        except ValueError:
            continue
        if cutoff and d.replace(tzinfo=timezone.utc) < cutoff:
            continue
        yr = d.year
        if yr not in by_year or raw > (by_year[yr].get("date") or ""):
            by_year[yr] = row

    return sorted(by_year.values(), key=lambda x: x.get("date") or "", reverse=True)


# ── Snapshot builder ───────────────────────────────────────────────────────────

def _build_snapshot_row(symbol: str, profile: dict, quote: dict, data: dict) -> dict:
    """Build the full fundamental snapshot for one ticker."""
    inc    = (data.get("income") or data.get("snapshot_income") or [])
    cf_rows = (data.get("cashflow") or data.get("snapshot_cf") or [])
    bs_rows = (data.get("balance") or data.get("snapshot_bs") or [])
    km_rows = (data.get("metrics") or data.get("snapshot_km") or [])

    inc0  = inc[0]   if inc    else {}
    cf0   = cf_rows[0]  if cf_rows  else {}
    bs0   = bs_rows[0]  if bs_rows  else {}
    km0   = km_rows[0]  if km_rows  else {}

    # Derived
    rev     = inc0.get("revenue")
    gp      = inc0.get("grossProfit")
    ni      = inc0.get("netIncome")
    mc      = quote.get("marketCap") or profile.get("mktCap")

    gross_margin  = _safe_div(gp, rev)
    profit_margin = _safe_div(ni, rev)

    fcf = cf0.get("freeCashFlow")
    if fcf is None:
        ocf   = cf0.get("operatingCashFlow")
        capex = cf0.get("capitalExpenditure")
        if ocf is not None and capex is not None:
            fcf = ocf - abs(capex)

    total_debt = bs0.get("totalDebt")
    if total_debt is None:
        total_debt = (bs0.get("shortTermDebt") or 0) + (bs0.get("longTermDebt") or 0)
        if total_debt == 0:
            total_debt = None

    ps  = km0.get("priceToSalesRatio")
    pe  = km0.get("peRatio") or km0.get("priceEarningsRatio")
    if pe is not None and pe < 0:
        pe = None   # skip nonsensical negative P/E

    # Fallback P/S and P/E
    if ps is None:
        ps = _safe_div(mc, rev)
    if pe is None:
        pe_denom = ni
        if pe_denom and pe_denom > 0:
            pe = _safe_div(mc, pe_denom)

    # Revenue growth from growth endpoint or calculation
    rev_growth = None
    gr_rows = data.get("growth") or []
    if gr_rows:
        rev_growth = gr_rows[0].get("revenueGrowth")
    if rev_growth is None and len(inc) >= 2:
        prev_rev = inc[1].get("revenue")
        rev_growth = _safe_div((rev - prev_rev) if (rev and prev_rev) else None, prev_rev)

    name = profile.get("companyName") or profile.get("name") or symbol

    return {
        "symbol":        symbol,
        "name":          name,
        "marketCap":     mc,
        "revenue":       rev,
        "revenueGrowth": rev_growth,
        "grossProfit":   gp,
        "grossMargin":   gross_margin,
        "profitMargin":  profit_margin,
        "epsDiluted":    inc0.get("epsDiluted") or inc0.get("eps"),
        "operatingIncome": inc0.get("operatingIncome"),
        "netIncome":     ni,
        "ebitda":        inc0.get("ebitda"),
        "freeCashFlow":  fcf,
        "totalDebt":     total_debt,
        "psRatio":       ps,
        "peRatio":       pe,
        "lastPrice":     quote.get("price"),
        "priceChange1D": (
            (quote.get("changesPercentage") or 0) / 100.0
            if quote.get("changesPercentage") is not None else None
        ),
    }


# ── Main compare entry point ───────────────────────────────────────────────────

async def compare_metrics(
    symbols: list[str],
    metric: str,
    period: str,
    range_val: str,
    fmp_key: str,
) -> dict:
    """
    Build the full comparison response for up to 15 tickers.

    Returns chart-ready series, snapshot table, optional news, and metadata.
    """
    # Resolve aliases
    resolved_metric = METRIC_ALIASES.get(metric.lower(), metric.lower())
    if resolved_metric not in METRIC_MAP:
        resolved_metric = "revenue"   # defensive fallback, caller should validate

    meta_def  = METRIC_MAP[resolved_metric]
    unit      = meta_def["unit"]
    mtr_source = meta_def["source"]
    limit     = _range_limit(range_val)

    fmp      = StockCompareFMP(fmp_key)
    warnings: list[str] = []
    invalid_symbols: list[str] = []
    valid_symbols: list[str]   = []

    # Parallel fetch for all symbols
    fetch_tasks = {
        sym: _fetch_symbol_data(sym, mtr_source, period, limit, fmp)
        for sym in symbols
    }
    fetch_keys   = list(fetch_tasks.keys())
    fetch_values = await asyncio.gather(*fetch_tasks.values(), return_exceptions=True)

    symbol_data: dict[str, dict] = {}
    for sym, result in zip(fetch_keys, fetch_values):
        if isinstance(result, Exception):
            warnings.append(f"{sym}: data fetch failed ({result})")
            symbol_data[sym] = {}
        else:
            symbol_data[sym] = result

    # Build series and snapshot
    series:   list[dict] = []
    snapshot: list[dict] = []
    news_map: dict[str, list] = {}

    for idx, sym in enumerate(symbols):
        data    = symbol_data.get(sym) or {}
        profile = data.get("profile") or {}
        quote   = data.get("quote") or {}

        # Detect invalid symbol (profile empty = unknown ticker)
        if not profile and not quote:
            invalid_symbols.append(sym)
            continue

        valid_symbols.append(sym)
        name = profile.get("companyName") or profile.get("name") or sym

        if resolved_metric != "recent_news":
            pts, latest = _extract_series_points(
                sym, resolved_metric, data, period, range_val, unit, warnings
            )

            if not pts:
                warnings.append(f"{sym}: no data available for {resolved_metric}")

            series.append({
                "symbol":   sym,
                "name":     name,
                "colorKey": _COLOR_KEYS[idx % len(_COLOR_KEYS)],
                "points":   pts,
                "latest":   {
                    "date":      latest["date"] if latest else None,
                    "value":     latest["value"] if latest else None,
                    "formatted": latest["formatted"] if latest else None,
                } if latest else None,
            })
        else:
            series.append({
                "symbol":   sym,
                "name":     name,
                "colorKey": _COLOR_KEYS[idx % len(_COLOR_KEYS)],
                "points":   [],
                "latest":   None,
            })

        # Build snapshot row (always, for all metrics)
        snap_row = _build_snapshot_row(sym, profile, quote, data)
        snapshot.append(snap_row)

        # Build news (always include recent news per ticker)
        raw_news = data.get("news") or []
        if resolved_metric == "recent_news" and not raw_news:
            # fetch directly if not already loaded
            try:
                raw_news = await fmp.news(sym, limit=5)
            except Exception:
                raw_news = []

        news_map[sym] = [
            {
                "title":         item.get("title") or "",
                "url":           item.get("url") or "",
                "site":          item.get("site") or item.get("publisher") or "",
                "publishedDate": item.get("publishedDate") or "",
                "summary":       (item.get("text") or "")[:300],
            }
            for item in raw_news[:5]
            if isinstance(item, dict)
        ]

    return {
        "metric": {
            "key":    resolved_metric,
            "label":  meta_def["label"],
            "unit":   unit,
            "period": period,
        },
        "range":   range_val.upper(),
        "symbols": valid_symbols,
        "series":  series,
        "snapshot": snapshot,
        "news":    news_map,
        "meta": {
            "sourcePriority":  ["fmp", "sec_edgar_fallback", "finnhub_fallback"],
            "cached":          False,
            "generatedAt":     datetime.now(timezone.utc).isoformat(),
            "warnings":        warnings,
            "invalidSymbols":  invalid_symbols,
            "validSymbols":    valid_symbols,
        },
    }
