"""
Stock Compare service — multi-ticker financial metric comparison.

FMP Stable API is the primary data source (Starter plan, 300 req/min, 5Y max).
No AI/Grok calls.

Design decisions
────────────────
• 24 supported numeric chart/screener metrics; recent_news is a non-chart section.
• Max history: 5 years.  Ranges 10Y / MAX / ALL are coerced to 5Y + warning.
• Annual period: always return at least 3 points for chart continuity.
• Points are returned ASCENDING (FMP returns newest-first; we reverse).
• Every per-symbol block is wrapped in try/except — no individual symbol failure
  can produce a 500; it is demoted to a warning + missingSymbols entry.
• Structured logging: [stock_compare] prefix on every FMP call.
"""
from __future__ import annotations

import asyncio
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

import httpx

from data.cache import cache

# ── Constants ─────────────────────────────────────────────────────────────────

FMP_STABLE = "https://financialmodelingprep.com/stable"

# Cache TTLs (seconds)
_TTL_SEARCH    = 86_400   # 24 h
_TTL_PROFILE   = 86_400   # 24 h
_TTL_STATEMENT = 86_400   # 24 h
_TTL_RATIOS    = 86_400   # 24 h
_TTL_GROWTH    = 86_400   # 24 h
_TTL_QUOTE     = 900      # 15 min
_TTL_NEWS      = 1_800    # 30 min
_TTL_HIST_MC   = 1_800    # 30 min
_TTL_KEY_MTR   = 86_400   # 24 h
_TTL_EV        = 86_400   # 24 h
_TTL_HIST_PX   = 1_800    # 30 min

# Colour keys for chart series
_COLOR_KEYS = [
    "blue", "red", "green", "orange", "purple",
    "teal", "pink", "yellow", "indigo", "cyan",
    "lime", "amber", "rose", "violet", "emerald",
]

_SUPPORTED_RANGES = {"1Y", "3Y", "5Y", "YTD"}
_COERCE_TO_5Y    = {"10Y", "MAX", "ALL", "1M", "3M", "6M"}

_ANNUAL_ROWS: dict[str, int] = {
    "1Y":  3,
    "YTD": 3,
    "3Y":  3,
    "5Y":  5,
}
_QUARTERLY_ROWS: dict[str, int] = {
    "1Y":  4,
    "YTD": 4,
    "3Y":  12,
    "5Y":  20,
}

VALID_RANGES = {"1Y", "3Y", "5Y", "YTD", "1M", "3M", "6M", "10Y", "MAX", "ALL"}

# ── Canonical metric definitions (24 numeric chart/screener metrics) ──────────
# unit values:
#   currency        — dollar amounts ($1.23B)
#   percent_decimal — stored as 0..1 decimal, display multiplied ×100  (e.g. 0.084 → "8.4%")
#   percent_already — stored as percent number (-1.82 means -1.82%)
#   ratio           — dimensionless ratio (2.1x)
#   number          — plain number (EPS)
#   price           — dollar price ($9.83)
#
# source values map to the FMP endpoint the series is fetched from:
#   income, growth, cashflow, balance, ratios, key_metrics, market_cap,
#   ev, quote_price
#
# fallback: human-readable description of fallback calculation, or null

METRIC_DEFINITIONS: dict[str, dict] = {
    "price": {
        "label": "Price", "unit": "price",
        "source": "quote_price", "chartable": True, "screener": True,
        "fallback": None,
        "fmpFields": ["price"],
        "sourceEndpoint": "historical-price-eod",
    },
    "price_change_percent": {
        "label": "Price Change %", "unit": "percent_already",
        "source": "quote_price", "chartable": True, "screener": True,
        "fallback": None,
        "fmpFields": ["changesPercentage", "changePercentage", "1D"],
        "sourceEndpoint": "quote",
    },
    "market_cap": {
        "label": "Market Cap", "unit": "currency",
        "source": "market_cap", "chartable": True, "screener": True,
        "fallback": None,
        "fmpFields": ["marketCap", "mktCap"],
        "sourceEndpoint": "historical-market-capitalization",
    },
    "enterprise_value": {
        "label": "Enterprise Value", "unit": "currency",
        "source": "ev", "chartable": True, "screener": True,
        "fallback": None,
        "fmpFields": ["enterpriseValue"],
        "sourceEndpoint": "enterprise-values",
    },
    "revenue": {
        "label": "Revenue", "unit": "currency",
        "source": "income", "chartable": True, "screener": True,
        "fallback": None,
        "fmpFields": ["revenue"],
        "sourceEndpoint": "income-statement",
    },
    "revenue_growth": {
        "label": "Revenue Growth (YoY)", "unit": "percent_decimal",
        "source": "growth", "chartable": True, "screener": True,
        "fallback": "(latestRevenue - priorRevenue) / priorRevenue",
        "fmpFields": ["revenueGrowth"],
        "sourceEndpoint": "financial-growth",
    },
    "gross_profit": {
        "label": "Gross Profit", "unit": "currency",
        "source": "income", "chartable": True, "screener": True,
        "fallback": None,
        "fmpFields": ["grossProfit"],
        "sourceEndpoint": "income-statement",
    },
    "gross_margin": {
        "label": "Gross Margin", "unit": "percent_decimal",
        "source": "income", "chartable": True, "screener": True,
        "fallback": "grossProfit / revenue",
        "fmpFields": ["grossProfitMargin"],
        "sourceEndpoint": "income-statement",
    },
    "operating_income": {
        "label": "Operating Income", "unit": "currency",
        "source": "income", "chartable": True, "screener": True,
        "fallback": None,
        "fmpFields": ["operatingIncome"],
        "sourceEndpoint": "income-statement",
    },
    "operating_margin": {
        "label": "Operating Margin", "unit": "percent_decimal",
        "source": "income", "chartable": True, "screener": True,
        "fallback": "operatingIncome / revenue",
        "fmpFields": ["operatingIncomeRatio"],
        "sourceEndpoint": "income-statement",
    },
    "net_income": {
        "label": "Net Income", "unit": "currency",
        "source": "income", "chartable": True, "screener": True,
        "fallback": None,
        "fmpFields": ["netIncome"],
        "sourceEndpoint": "income-statement",
    },
    "profit_margin": {
        "label": "Net Profit Margin", "unit": "percent_decimal",
        "source": "income", "chartable": True, "screener": True,
        "fallback": "netIncome / revenue",
        "fmpFields": ["netIncomeRatio"],
        "sourceEndpoint": "income-statement",
    },
    "eps_diluted": {
        "label": "EPS (Diluted)", "unit": "number",
        "source": "income", "chartable": True, "screener": True,
        "fallback": None,
        "fmpFields": ["epsDiluted", "eps"],
        "sourceEndpoint": "income-statement",
    },
    "ebitda": {
        "label": "EBITDA", "unit": "currency",
        "source": "income", "chartable": True, "screener": True,
        "fallback": None,
        "fmpFields": ["ebitda"],
        "sourceEndpoint": "income-statement",
    },
    "free_cash_flow": {
        "label": "Free Cash Flow", "unit": "currency",
        "source": "cashflow", "chartable": True, "screener": True,
        "fallback": "operatingCashFlow - abs(capitalExpenditure)",
        "fmpFields": ["freeCashFlow"],
        "sourceEndpoint": "cash-flow-statement",
    },
    "fcf_margin": {
        "label": "FCF Margin", "unit": "percent_decimal",
        "source": "cashflow", "chartable": True, "screener": True,
        "fallback": "freeCashFlow / revenue",
        "fmpFields": ["freeCashFlow", "revenue"],
        "sourceEndpoint": "cash-flow-statement + income-statement",
    },
    "total_debt": {
        "label": "Total Debt", "unit": "currency",
        "source": "balance", "chartable": True, "screener": True,
        "fallback": "shortTermDebt + longTermDebt",
        "fmpFields": ["totalDebt"],
        "sourceEndpoint": "balance-sheet-statement",
    },
    "debt_to_equity": {
        "label": "Debt / Equity", "unit": "ratio",
        "source": "ratios", "chartable": True, "screener": True,
        "fallback": "totalDebt / totalStockholdersEquity",
        "fmpFields": ["debtEquityRatio", "debtToEquity"],
        "sourceEndpoint": "ratios",
    },
    "current_ratio": {
        "label": "Current Ratio", "unit": "ratio",
        "source": "ratios", "chartable": True, "screener": True,
        "fallback": "totalCurrentAssets / totalCurrentLiabilities",
        "fmpFields": ["currentRatio"],
        "sourceEndpoint": "ratios",
    },
    "ps_ratio": {
        "label": "P/S Ratio", "unit": "ratio",
        "source": "ratios", "chartable": True, "screener": True,
        "fallback": "marketCap / revenue",
        "fmpFields": ["priceToSalesRatio"],
        "sourceEndpoint": "ratios",
    },
    "pe_ratio": {
        "label": "P/E Ratio", "unit": "ratio",
        "source": "ratios", "chartable": True, "screener": True,
        "fallback": "marketCap / netIncome (only if netIncome > 0)",
        "fmpFields": ["priceToEarningsRatio", "priceEarningsRatio"],
        "sourceEndpoint": "ratios",
    },
    "ev_to_ebitda": {
        "label": "EV / EBITDA", "unit": "ratio",
        "source": "key_metrics", "chartable": True, "screener": True,
        "fallback": "enterpriseValue / ebitda (only if ebitda > 0)",
        "fmpFields": ["enterpriseValueOverEBITDA"],
        "sourceEndpoint": "key-metrics",
    },
    "roe": {
        "label": "ROE", "unit": "percent_decimal",
        "source": "ratios", "chartable": True, "screener": True,
        "fallback": None,
        "fmpFields": ["returnOnEquity", "roe"],
        "sourceEndpoint": "ratios",
    },
    "roa": {
        "label": "ROA", "unit": "percent_decimal",
        "source": "ratios", "chartable": True, "screener": True,
        "fallback": None,
        "fmpFields": ["returnOnAssets", "roa"],
        "sourceEndpoint": "ratios",
    },
}

# Non-chart sections (kept for backwards-compat route validation)
NON_CHART_SECTIONS: list[dict] = [
    {"key": "recent_news", "label": "Recent News"},
]

# METRIC_MAP — backward compat shim derived from METRIC_DEFINITIONS
# Keeps "unit" using old "percent" name for existing _fmt() consumers;
# new unit names also supported via updated _fmt().
METRIC_MAP: dict[str, dict] = {
    k: {"label": v["label"], "unit": v["unit"], "source": v["source"]}
    for k, v in METRIC_DEFINITIONS.items()
}
# Legacy entry so recent_news passes route validation without being charted
METRIC_MAP["recent_news"] = {"label": "Recent News", "unit": "news", "source": "news"}

METRIC_ALIASES: dict[str, str] = {
    # Legacy aliases (kept)
    "price_to_sales":      "ps_ratio",
    "p_s_ratio":           "ps_ratio",
    "p_e_ratio":           "pe_ratio",
    "eps":                 "eps_diluted",
    "fcf":                 "free_cash_flow",
    "debt":                "total_debt",
    # New aliases per task spec
    "price_change":        "price_change_percent",
    "marketcap":           "market_cap",
    "revenuegrowth":       "revenue_growth",
    "grossprofit":         "gross_profit",
    "grossmargin":         "gross_margin",
    "profitmargin":        "profit_margin",
    "epsdiluted":          "eps_diluted",
    "operatingincome":     "operating_income",
    "netincome":           "net_income",
    "freecashflow":        "free_cash_flow",
    "totaldebt":           "total_debt",
    "price_to_earnings":   "pe_ratio",
    "ev_ebitda":           "ev_to_ebitda",
    # camelCase aliases
    "marketCap":           "market_cap",
    "revenueGrowth":       "revenue_growth",
    "grossProfit":         "gross_profit",
    "grossMargin":         "gross_margin",
    "profitMargin":        "profit_margin",
    "epsDiluted":          "eps_diluted",
    "operatingIncome":     "operating_income",
    "netIncome":           "net_income",
    "freeCashFlow":        "free_cash_flow",
    "totalDebt":           "total_debt",
}

VALID_METRICS = (
    set(METRIC_MAP.keys())
    | set(METRIC_ALIASES.keys())
    | {k.lower() for k in METRIC_ALIASES.keys()}
)
VALID_PERIODS = {"annual", "quarterly"}

print(f"[stock_compare] metrics supported={len(METRIC_DEFINITIONS)} non-chart={len(NON_CHART_SECTIONS)}")


# ── FMP client ────────────────────────────────────────────────────────────────

class StockCompareFMP:
    """Lightweight FMP Stable API client with compare-specific cache keys."""

    def __init__(self, api_key: str):
        self._key = api_key

    async def _get(
        self,
        endpoint: str,
        params: dict,
        cache_key: str,
        ttl: int,
        log_label: str = "",
    ) -> Any:
        hit = cache.get(cache_key)
        if hit is not None:
            return hit

        p = dict(params)
        p["apikey"] = self._key
        t0 = time.monotonic()
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(f"{FMP_STABLE}/{endpoint}", params=p)
            ms = int((time.monotonic() - t0) * 1000)
            status = resp.status_code
            if status not in (200, 201):
                print(f"[stock_compare] FMP {endpoint} {log_label} status={status} ms={ms}")
                return []
            result = resp.json()
            rows = len(result) if isinstance(result, list) else (1 if result else 0)
            cached_label = "cache=miss"
            print(f"[stock_compare] FMP {endpoint} {log_label} status={status} rows={rows} {cached_label} ms={ms}")
            if result:
                cache.set(cache_key, result, ttl)
            return result
        except Exception as e:
            ms = int((time.monotonic() - t0) * 1000)
            print(f"[stock_compare] FMP {endpoint} {log_label} error={e} ms={ms}")
            return []

    async def _get_cached(self, cache_key: str, endpoint: str, params: dict, ttl: int, log_label: str = "") -> Any:
        """Same as _get but logs cache hit."""
        hit = cache.get(cache_key)
        if hit is not None:
            print(f"[stock_compare] FMP {endpoint} {log_label} cache=hit")
            return hit
        return await self._get(endpoint, params, cache_key, ttl, log_label)

    async def search(self, query: str, limit: int = 10) -> list:
        ck_sym  = f"sc:searchsym:{query.lower()}:{limit}"
        ck_name = f"sc:searchname:{query.lower()}:{limit}"
        sym_results, name_results = await asyncio.gather(
            self._get("search-symbol", {"query": query, "limit": limit}, ck_sym, _TTL_SEARCH),
            self._get("search-name",   {"query": query, "limit": limit}, ck_name, _TTL_SEARCH),
        )
        merged: dict[str, dict] = {}
        for item in list(sym_results or []) + list(name_results or []):
            if isinstance(item, dict):
                sym = (item.get("symbol") or "").upper()
                if sym and sym not in merged:
                    merged[sym] = item
        return list(merged.values())[:limit]

    async def profile(self, symbol: str) -> dict:
        ck = f"sc:profile:{symbol}"
        data = await self._get_cached(ck, "profile", {"symbol": symbol}, _TTL_PROFILE, symbol)
        return data[0] if isinstance(data, list) and data else (data if isinstance(data, dict) else {})

    async def quote(self, symbol: str) -> dict:
        ck = f"sc:quote:{symbol}"
        data = await self._get_cached(ck, "quote", {"symbol": symbol}, _TTL_QUOTE, symbol)
        return data[0] if isinstance(data, list) and data else (data if isinstance(data, dict) else {})

    async def income_statement(self, symbol: str, period: str, limit: int) -> list:
        ck = f"sc:income:{symbol}:{period}:{limit}"
        data = await self._get_cached(
            ck, "income-statement",
            {"symbol": symbol, "period": period, "limit": limit},
            _TTL_STATEMENT, symbol,
        )
        return data if isinstance(data, list) else []

    async def balance_sheet(self, symbol: str, period: str, limit: int) -> list:
        ck = f"sc:balance:{symbol}:{period}:{limit}"
        data = await self._get_cached(
            ck, "balance-sheet-statement",
            {"symbol": symbol, "period": period, "limit": limit},
            _TTL_STATEMENT, symbol,
        )
        return data if isinstance(data, list) else []

    async def cash_flow(self, symbol: str, period: str, limit: int) -> list:
        ck = f"sc:cashflow:{symbol}:{period}:{limit}"
        data = await self._get_cached(
            ck, "cash-flow-statement",
            {"symbol": symbol, "period": period, "limit": limit},
            _TTL_STATEMENT, symbol,
        )
        return data if isinstance(data, list) else []

    async def ratios(self, symbol: str, period: str, limit: int) -> list:
        ck = f"sc:ratios:{symbol}:{period}:{limit}"
        data = await self._get_cached(
            ck, "ratios",
            {"symbol": symbol, "period": period, "limit": limit},
            _TTL_RATIOS, symbol,
        )
        return data if isinstance(data, list) else []

    async def key_metrics(self, symbol: str, period: str, limit: int) -> list:
        """Key metrics — includes ev_to_ebitda, ROE, ROA, enterprise value."""
        ck = f"sc:keymtr:{symbol}:{period}:{limit}"
        data = await self._get_cached(
            ck, "key-metrics",
            {"symbol": symbol, "period": period, "limit": limit},
            _TTL_KEY_MTR, symbol,
        )
        return data if isinstance(data, list) else []

    async def enterprise_values(self, symbol: str, period: str, limit: int) -> list:
        """Historical enterprise values."""
        ck = f"sc:ev:{symbol}:{period}:{limit}"
        data = await self._get_cached(
            ck, "enterprise-values",
            {"symbol": symbol, "period": period, "limit": limit},
            _TTL_EV, symbol,
        )
        return data if isinstance(data, list) else []

    async def financial_growth(self, symbol: str, period: str, limit: int) -> list:
        ck = f"sc:growth:{symbol}:{period}:{limit}"
        data = await self._get_cached(
            ck, "financial-growth",
            {"symbol": symbol, "period": period, "limit": limit},
            _TTL_GROWTH, symbol,
        )
        return data if isinstance(data, list) else []

    async def hist_market_cap(self, symbol: str) -> list:
        """Historical market cap — daily series; sampled annually."""
        ck = f"sc:histmc:{symbol}"
        data = await self._get_cached(
            ck, "historical-market-capitalization",
            {"symbol": symbol, "limit": 1826},
            _TTL_HIST_MC, symbol,
        )
        return data if isinstance(data, list) else []

    async def hist_price(self, symbol: str) -> list:
        """Historical end-of-day price — daily; sampled annually for chart.

        Uses the date-ranged endpoint instead of /full (which pulled the entire
        ticker history regardless of how much was needed).  1826 calendar days
        (5Y) matches the previous limit= behaviour while being a scoped request.

        FMP_BLOCK_FULL_HISTORICAL=true blocks this call and returns cached data
        or an empty list — the compare page degrades gracefully without it.
        """
        from services.fmp_full_guard import log_and_check as _fmp_guard

        ck = f"sc:histpx:{symbol}"

        # Guard: block under FMP_BLOCK_FULL_HISTORICAL; log under DRY_RUN
        if _fmp_guard(
            symbol,
            caller_func="hist_price",
            caller_file="stock_compare_service.py",
            job_name="user_compare_request",
        ):
            # Return whatever is warm in cache; otherwise empty list
            hit = cache.get(ck)
            if isinstance(hit, dict):
                return hit.get("historical") or []
            return hit if isinstance(hit, list) else []

        from_date = (date.today() - timedelta(days=1826)).isoformat()
        to_date   = date.today().isoformat()

        data = await self._get_cached(
            ck, "historical-price-eod",
            {"symbol": symbol, "from": from_date, "to": to_date},
            _TTL_HIST_PX, symbol,
        )
        if isinstance(data, dict):
            return data.get("historical") or []
        return data if isinstance(data, list) else []

    async def news(self, symbol: str, limit: int = 5) -> list:
        ck = f"sc:news:{symbol}:{limit}"
        data = await self._get_cached(
            ck, "news/stock",
            {"symbols": symbol, "limit": limit},
            _TTL_NEWS, symbol,
        )
        return data if isinstance(data, list) else []


# ── Helpers ───────────────────────────────────────────────────────────────────

def _fmt(value: Optional[float], unit: str) -> Optional[str]:
    if value is None:
        return None
    if unit in ("currency",):
        av, sign = abs(value), ("-" if value < 0 else "")
        if av >= 1e12: return f"{sign}${av/1e12:.2f}T"
        if av >= 1e9:  return f"{sign}${av/1e9:.2f}B"
        if av >= 1e6:  return f"{sign}${av/1e6:.2f}M"
        if av >= 1e3:  return f"{sign}${av/1e3:.2f}K"
        return f"{sign}${av:.2f}"
    if unit == "price":
        return f"${value:.2f}"
    if unit in ("percent", "percent_decimal"):
        return f"{value*100:.1f}%"
    if unit == "percent_already":
        return f"{value:.2f}%"
    if unit == "ratio":
        return f"{value:.2f}x"
    if unit == "number":
        return str(round(value, 4))
    return str(round(value, 4))


def _safe_div(num: Optional[float], denom: Optional[float]) -> Optional[float]:
    if num is None or denom is None or denom == 0:
        return None
    return num / denom


def _row_limit(period: str, range_val: str) -> int:
    rv = range_val.upper()
    if period == "quarterly":
        return _QUARTERLY_ROWS.get(rv, 20)
    return _ANNUAL_ROWS.get(rv, 5)


def _trim_rows(rows: list, period: str, range_val: str) -> list:
    rv = range_val.upper()
    if period == "quarterly":
        n = _QUARTERLY_ROWS.get(rv, 20)
    else:
        n = _ANNUAL_ROWS.get(rv, 5)
    return rows[:max(n, 1)]


def _coerce_range(range_val: str, warnings: list[str]) -> str:
    rv = range_val.upper()
    if rv in _COERCE_TO_5Y:
        warnings.append(
            f"Range {rv!r} is not supported on the current FMP plan (max 5Y); using 5Y instead."
        )
        return "5Y"
    return rv


def _sort_ascending(points: list[dict]) -> list[dict]:
    try:
        return sorted(points, key=lambda p: p.get("date") or "")
    except Exception:
        return points


def _sample_annual(rows: list, date_field: str = "date", value_field: str = "marketCap") -> list:
    """Sample one row per calendar year (take the latest row of each year)."""
    by_year: dict[int, dict] = {}
    for row in rows:
        raw = row.get(date_field) or ""
        if not raw:
            continue
        try:
            yr = int(str(raw)[:4])
        except ValueError:
            continue
        if yr not in by_year or str(raw) > str(by_year[yr].get(date_field) or ""):
            by_year[yr] = row
    return sorted(by_year.values(), key=lambda x: x.get(date_field) or "", reverse=True)


def _sample_annual_hist_mc(rows: list) -> list:
    return _sample_annual(rows, "date", "marketCap")


def _sample_annual_hist_price(rows: list) -> list:
    """Sample end-of-year price from daily historical price rows."""
    by_year: dict[int, dict] = {}
    for row in rows:
        raw = row.get("date") or ""
        if not raw:
            continue
        try:
            yr = int(str(raw)[:4])
        except ValueError:
            continue
        if yr not in by_year or str(raw) > str(by_year[yr].get("date") or ""):
            by_year[yr] = row
    return sorted(by_year.values(), key=lambda x: x.get("date") or "", reverse=True)


# ── Symbol search ─────────────────────────────────────────────────────────────

_PREFER_EXCHANGES = {"NASDAQ", "NYSE", "AMEX", "NYSE ARCA", "NYSE MKT"}
_SKIP_TYPES       = {"ETF", "FUND", "MUTUAL FUND", "WARRANT", "UNIT", "RIGHT"}


async def search_symbols(query: str, limit: int, fmp_key: str) -> dict:
    fmp = StockCompareFMP(fmp_key)
    raw = await fmp.search(query, limit=min(limit * 3, 50))
    if not isinstance(raw, list):
        raw = []

    q_upper = query.upper().strip()

    def _match_rank(sym: str) -> int:
        if sym == q_upper:            return 3
        if sym.startswith(q_upper):   return 2
        if q_upper in sym:            return 1
        return 0

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
        skip        = bool(typ) and any(t in typ for t in _SKIP_TYPES)
        exch_score  = 1 if exc in _PREFER_EXCHANGES else 0
        match_score = _match_rank(sym)
        if sym not in seen:
            seen[sym] = {
                "symbol": sym, "name": name, "exchange": exc,
                "type": (item.get("type") or "stock").lower(),
                "currency": item.get("currency") or "USD",
                "cik": item.get("cik") or "",
                "sector": item.get("sector") or "",
                "industry": item.get("industry") or "",
                "marketCap": item.get("marketCap"),
                "_skip": skip, "_exch": exch_score, "_match": match_score,
            }
        elif exch_score > seen[sym]["_exch"]:
            seen[sym].update({
                "exchange": exc,
                "type": (item.get("type") or "stock").lower(),
                "_exch": exch_score, "_skip": skip,
            })

    key = lambda x: (-x["_match"], -x["_exch"], x["symbol"])
    results = [
        {k: v for k, v in r.items() if not k.startswith("_")}
        for r in sorted(seen.values(), key=key)
        if not r["_skip"]
    ]
    if not results:
        results = [
            {k: v for k, v in r.items() if not k.startswith("_")}
            for r in sorted(seen.values(), key=key)
        ]
    return {"query": query, "results": results[:limit], "source": "fmp", "cached": False}


# ── Per-symbol data fetch ─────────────────────────────────────────────────────

async def _fetch_symbol_data(
    symbol: str,
    metric_source: str,
    period: str,
    range_val: str,
    fmp: StockCompareFMP,
) -> dict:
    """
    Fetch only the FMP endpoints needed for this metric + minimal snapshot data.

    Always fetches: profile, quote
    Series-specific fetches: determined by metric_source
    Snapshot supplements: income(2), cashflow(1), balance(1), ratios(1), key_metrics(1)
    """
    series_limit = _row_limit(period, range_val)
    t0 = time.monotonic()

    tasks: dict[str, Any] = {
        "profile": fmp.profile(symbol),
        "quote":   fmp.quote(symbol),
    }

    # Series fetch (metric-specific)
    if metric_source == "income":
        tasks["income"] = fmp.income_statement(symbol, period, series_limit)

    elif metric_source == "growth":
        tasks["income"] = fmp.income_statement(symbol, period, series_limit + 1)
        tasks["growth"] = fmp.financial_growth(symbol, period, series_limit)

    elif metric_source == "cashflow":
        tasks["cashflow"] = fmp.cash_flow(symbol, period, series_limit)
        if metric_source == "cashflow":
            tasks["income"] = fmp.income_statement(symbol, period, series_limit)

    elif metric_source == "balance":
        tasks["balance"] = fmp.balance_sheet(symbol, period, series_limit)

    elif metric_source == "ratios":
        tasks["ratios"] = fmp.ratios(symbol, period, series_limit)
        # Also fetch income + balance for roe/roa/d-e/current fallback series
        tasks["income"]  = fmp.income_statement(symbol, period, series_limit)
        tasks["balance"] = fmp.balance_sheet(symbol, period, series_limit)

    elif metric_source == "key_metrics":
        tasks["key_metrics"] = fmp.key_metrics(symbol, period, series_limit)
        # Also fetch EV + income so we can compute ev_to_ebitda as fallback
        tasks["ev"]     = fmp.enterprise_values(symbol, period, series_limit)
        tasks["income"] = fmp.income_statement(symbol, period, series_limit)

    elif metric_source == "ev":
        tasks["ev"] = fmp.enterprise_values(symbol, period, series_limit)

    elif metric_source == "market_cap":
        tasks["hist_mc"] = fmp.hist_market_cap(symbol)

    elif metric_source == "quote_price":
        tasks["hist_px"] = fmp.hist_price(symbol)

    elif metric_source == "news":
        tasks["news"] = fmp.news(symbol, limit=5)

    # Snapshot supplements (only if not already fetched)
    if "income" not in tasks:
        tasks["income"] = fmp.income_statement(symbol, period, 2)

    if "cashflow" not in tasks:
        tasks["snapshot_cf"] = fmp.cash_flow(symbol, period, 1)

    if "balance" not in tasks:
        tasks["snapshot_bs"] = fmp.balance_sheet(symbol, period, 1)

    if "ratios" not in tasks:
        tasks["snapshot_ratios"] = fmp.ratios(symbol, period, 1)

    if "key_metrics" not in tasks:
        tasks["snapshot_km"] = fmp.key_metrics(symbol, period, 1)

    if "ev" not in tasks:
        tasks["snapshot_ev"] = fmp.enterprise_values(symbol, period, 1)

    keys   = list(tasks.keys())
    values = await asyncio.gather(*tasks.values(), return_exceptions=True)

    result: dict[str, Any] = {}
    for k, v in zip(keys, values):
        result[k] = [] if isinstance(v, Exception) else (v if v is not None else [])

    ms = int((time.monotonic() - t0) * 1000)
    cached = all(
        cache.get(f"sc:{k.replace('_', '')}:{symbol}:{period}:*") is not None
        for k in ("income", "ratios") if k in result
    )
    print(f"[stock_compare] bundle {symbol} cache={'hit' if cached else 'miss'} ms={ms}")
    return result


# ── Value extractors ──────────────────────────────────────────────────────────

def _income_value(metric_key: str, row: dict, symbol: str, warnings: list[str]) -> Optional[float]:
    try:
        if metric_key == "revenue":          return row.get("revenue")
        if metric_key == "gross_profit":     return row.get("grossProfit")
        if metric_key == "operating_income": return row.get("operatingIncome")
        if metric_key == "net_income":       return row.get("netIncome")
        if metric_key == "eps_diluted":
            return row.get("epsDiluted") or row.get("eps")
        if metric_key == "ebitda":
            v = row.get("ebitda")
            if v is None:
                warnings.append(f"{symbol}: ebitda missing for {row.get('date')}")
            return v
        if metric_key == "gross_margin":
            v = row.get("grossProfitRatio") or row.get("grossProfitMargin")
            if v is None:
                v = _safe_div(row.get("grossProfit"), row.get("revenue"))
            if v is None:
                warnings.append(f"{symbol}: gross_margin unavailable for {row.get('date')}")
            return v
        if metric_key == "profit_margin":
            v = row.get("netIncomeRatio") or row.get("netProfitMargin")
            if v is None:
                v = _safe_div(row.get("netIncome"), row.get("revenue"))
            if v is None:
                warnings.append(f"{symbol}: profit_margin unavailable for {row.get('date')}")
            return v
        if metric_key == "operating_margin":
            v = row.get("operatingIncomeRatio")
            if v is None:
                v = _safe_div(row.get("operatingIncome"), row.get("revenue"))
            if v is None:
                warnings.append(f"{symbol}: operating_margin unavailable for {row.get('date')}")
            return v
    except Exception as e:
        warnings.append(f"{symbol}: error extracting {metric_key}: {e}")
    return None


def _cashflow_value(
    metric_key: str,
    cf_row: dict,
    symbol: str,
    warnings: list[str],
    income_row: Optional[dict] = None,
) -> Optional[float]:
    try:
        fcf = cf_row.get("freeCashFlow")
        if fcf is None:
            ocf   = cf_row.get("operatingCashFlow")
            capex = cf_row.get("capitalExpenditure")
            if ocf is not None and capex is not None:
                fcf = ocf - abs(capex)

        if metric_key == "free_cash_flow":
            if fcf is None:
                warnings.append(f"{symbol}: free_cash_flow unavailable for {cf_row.get('date')}")
            return fcf

        if metric_key == "fcf_margin":
            if fcf is None:
                warnings.append(f"{symbol}: fcf_margin skipped — freeCashFlow missing for {cf_row.get('date')}")
                return None
            rev = None
            if income_row:
                rev = income_row.get("revenue")
            if rev is None:
                rev = cf_row.get("revenue")
            v = _safe_div(fcf, rev)
            if v is None:
                warnings.append(f"{symbol}: fcf_margin skipped — revenue missing for {cf_row.get('date')}")
            return v

    except Exception as e:
        warnings.append(f"{symbol}: error extracting {metric_key}: {e}")
    return None


def _balance_value(metric_key: str, row: dict, symbol: str, warnings: list[str]) -> Optional[float]:
    try:
        v = row.get("totalDebt")
        if v is not None:
            return v
        st = row.get("shortTermDebt") or 0
        lt = row.get("longTermDebt")  or 0
        if st or lt:
            return st + lt
        warnings.append(f"{symbol}: total_debt unavailable for {row.get('date')}")
    except Exception as e:
        warnings.append(f"{symbol}: error extracting {metric_key}: {e}")
    return None


def _ratio_value(metric_key: str, row: dict, symbol: str, warnings: list[str]) -> Optional[float]:
    try:
        if metric_key == "ps_ratio":
            v = row.get("priceToSalesRatio")
            if v is None:
                warnings.append(f"{symbol}: ps_ratio unavailable for {row.get('date')}")
            return v

        if metric_key == "pe_ratio":
            v = row.get("priceToEarningsRatio") or row.get("priceEarningsRatio")
            if v is None:
                warnings.append(f"{symbol}: pe_ratio unavailable for {row.get('date')}")
                return None
            if v <= 0:
                warnings.append(f"{symbol}: pe_ratio skipped (negative/zero earnings) for {row.get('date')}")
                return None
            return v

        if metric_key == "debt_to_equity":
            v = row.get("debtEquityRatio") or row.get("debtToEquity")
            if v is None:
                warnings.append(f"{symbol}: debt_to_equity unavailable for {row.get('date')}")
            return v

        if metric_key == "current_ratio":
            v = row.get("currentRatio")
            if v is None:
                warnings.append(f"{symbol}: current_ratio unavailable for {row.get('date')}")
            return v

        if metric_key == "roe":
            v = row.get("returnOnEquity") or row.get("roe")
            if v is None:
                warnings.append(f"{symbol}: roe unavailable for {row.get('date')}")
            return v

        if metric_key == "roa":
            v = row.get("returnOnAssets") or row.get("roa")
            if v is None:
                warnings.append(f"{symbol}: roa unavailable for {row.get('date')}")
            return v

    except Exception as e:
        warnings.append(f"{symbol}: error extracting {metric_key}: {e}")
    return None


def _key_metrics_value(metric_key: str, row: dict, symbol: str, warnings: list[str]) -> Optional[float]:
    try:
        if metric_key == "ev_to_ebitda":
            v = row.get("enterpriseValueOverEBITDA")
            if v is None:
                warnings.append(f"{symbol}: ev_to_ebitda unavailable for {row.get('date')}")
                return None
            if v <= 0:
                warnings.append(f"{symbol}: ev_to_ebitda skipped (non-positive) for {row.get('date')}")
                return None
            return v
    except Exception as e:
        warnings.append(f"{symbol}: error extracting {metric_key}: {e}")
    return None


def _ev_value(metric_key: str, row: dict, symbol: str, warnings: list[str]) -> Optional[float]:
    try:
        if metric_key == "enterprise_value":
            v = row.get("enterpriseValue")
            if v is None:
                warnings.append(f"{symbol}: enterprise_value unavailable for {row.get('date')}")
            return v
    except Exception as e:
        warnings.append(f"{symbol}: error extracting {metric_key}: {e}")
    return None


def _calc_revenue_growth_from_income(income_rows: list, target_date: Optional[str]) -> Optional[float]:
    if len(income_rows) < 2 or not target_date:
        return None
    for i, row in enumerate(income_rows):
        if (row.get("date") or "")[:10] == target_date[:10]:
            if i + 1 < len(income_rows):
                cur  = row.get("revenue")
                prev = income_rows[i + 1].get("revenue")
                return _safe_div(
                    (cur - prev) if (cur is not None and prev is not None) else None,
                    prev,
                )
    return None


# ── Series point extraction ───────────────────────────────────────────────────

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
    Extract chart points for the chosen metric.
    Returns (points_ascending, latest_point).
    """
    points: list[dict] = []
    metric_source = METRIC_MAP[metric_key]["source"]

    def _make_pt(row: dict, v: float, source_label: str) -> dict:
        return {
            "date":       row.get("date") or "",
            "fiscalYear": row.get("fiscalYear") or row.get("calendarYear"),
            "value":      v,
            "formatted":  _fmt(v, unit),
            "source":     source_label,
        }

    if metric_source == "income":
        rows = _trim_rows(data.get("income") or [], period, range_val)
        for row in rows:
            v = _income_value(metric_key, row, symbol, warnings)
            if v is not None:
                points.append(_make_pt(row, v, "fmp_income_statement"))

    elif metric_source == "growth":
        growth_rows = _trim_rows(data.get("growth") or [], period, range_val)
        if not growth_rows:
            inc = data.get("income") or []
            for i in range(len(inc) - 1):
                cur, prev = inc[i], inc[i + 1]
                v = _safe_div(
                    (cur.get("revenue") - prev.get("revenue"))
                    if (cur.get("revenue") is not None and prev.get("revenue") is not None)
                    else None,
                    prev.get("revenue"),
                )
                if v is not None:
                    points.append(_make_pt(cur, v, "fmp_income_statement_calc"))
        else:
            for row in growth_rows:
                v = row.get("revenueGrowth")
                if v is None:
                    v = _calc_revenue_growth_from_income(
                        data.get("income") or [], row.get("date")
                    )
                if v is not None:
                    points.append(_make_pt(row, v, "fmp_financial_growth"))

    elif metric_source == "cashflow":
        cf_rows  = _trim_rows(data.get("cashflow") or [], period, range_val)
        inc_rows = data.get("income") or []
        inc_by_date: dict[str, dict] = {
            (r.get("date") or "")[:7]: r for r in inc_rows
        }
        for row in cf_rows:
            date_key  = (row.get("date") or "")[:7]
            inc_row   = inc_by_date.get(date_key)
            v = _cashflow_value(metric_key, row, symbol, warnings, inc_row)
            if v is not None:
                points.append(_make_pt(row, v, "fmp_cash_flow_statement"))

    elif metric_source == "balance":
        rows = _trim_rows(data.get("balance") or [], period, range_val)
        for row in rows:
            v = _balance_value(metric_key, row, symbol, warnings)
            if v is not None:
                points.append(_make_pt(row, v, "fmp_balance_sheet"))

    elif metric_source == "ratios":
        _rat_warn_mark = len(warnings)
        rows = _trim_rows(data.get("ratios") or [], period, range_val)
        for row in rows:
            v = _ratio_value(metric_key, row, symbol, warnings)
            if v is not None:
                points.append(_make_pt(row, v, "fmp_ratios"))

        # Fallback series for roe/roa computed from income + balance statement
        if not points and metric_key in ("roe", "roa"):
            inc_rows = _trim_rows(data.get("income") or [], period, range_val)
            bs_rows  = data.get("balance") or []
            bs_by_ym: dict[str, dict] = {(r.get("date") or "")[:7]: r for r in bs_rows}
            for inc_row in inc_rows:
                date_key = (inc_row.get("date") or "")[:7]
                bs_row   = bs_by_ym.get(date_key)
                ni_val   = inc_row.get("netIncome")
                if bs_row and ni_val is not None:
                    if metric_key == "roe":
                        equity = bs_row.get("totalStockholdersEquity")
                        v = _safe_div(ni_val, equity)
                    else:
                        assets = bs_row.get("totalAssets")
                        v = _safe_div(ni_val, assets)
                    if v is not None:
                        points.append(_make_pt(inc_row, v, "fmp_inc_bs_fallback"))
            if points:
                del warnings[_rat_warn_mark:]

    elif metric_source == "key_metrics":
        # Capture position before per-row warnings so we can clean them up if fallback succeeds
        _km_warn_mark = len(warnings)
        rows = _trim_rows(data.get("key_metrics") or [], period, range_val)
        for row in rows:
            v = _key_metrics_value(metric_key, row, symbol, warnings)
            if v is not None:
                points.append(_make_pt(row, v, "fmp_key_metrics"))

        # Fallback for ev_to_ebitda: compute from enterprise-values + income-statement
        if not points and metric_key == "ev_to_ebitda":
            warnings_before = _km_warn_mark
            ev_rows  = _trim_rows(data.get("ev") or [], period, range_val)
            inc_rows = data.get("income") or []
            inc_by_ym: dict[str, dict] = {
                (r.get("date") or "")[:7]: r for r in inc_rows
            }
            for ev_row in ev_rows:
                date_key = (ev_row.get("date") or "")[:7]
                inc_row  = inc_by_ym.get(date_key)
                ev_val   = ev_row.get("enterpriseValue")
                ebitda   = (inc_row or {}).get("ebitda") if inc_row else None
                v = None
                if ev_val is not None and ebitda is not None and ebitda > 0:
                    v = ev_val / ebitda
                if v is not None:
                    points.append(_make_pt(ev_row, v, "fmp_ev_income_fallback"))
            # Clear per-row warnings if fallback succeeded
            if points:
                del warnings[warnings_before:]

    elif metric_source == "ev":
        rows = _trim_rows(data.get("ev") or [], period, range_val)
        for row in rows:
            v = _ev_value(metric_key, row, symbol, warnings)
            if v is not None:
                points.append(_make_pt(row, v, "fmp_enterprise_values"))

    elif metric_source == "market_cap":
        hist        = data.get("hist_mc") or []
        annual_rows = _trim_rows(_sample_annual_hist_mc(hist), period, range_val)
        q           = data.get("quote") or {}
        prof        = data.get("profile") or {}
        cur_mc      = q.get("marketCap") or prof.get("mktCap")
        today_str   = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if cur_mc:
            points.append({
                "date": today_str, "fiscalYear": datetime.now(timezone.utc).year,
                "value": cur_mc, "formatted": _fmt(cur_mc, unit), "source": "fmp_quote",
            })
        for row in annual_rows:
            mc = row.get("marketCap")
            if mc and row.get("date") != today_str:
                points.append({
                    "date":       row.get("date") or "",
                    "fiscalYear": int((row.get("date") or "0000")[:4]),
                    "value":      mc,
                    "formatted":  _fmt(mc, unit),
                    "source":     "fmp_hist_market_cap",
                })

    elif metric_source == "quote_price":
        q   = data.get("quote") or {}
        if metric_key == "price":
            hist = data.get("hist_px") or []
            annual_rows = _trim_rows(_sample_annual_hist_price(hist), period, range_val)
            today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            cur_px = q.get("price")
            if cur_px is not None:
                points.append({
                    "date": today_str, "fiscalYear": datetime.now(timezone.utc).year,
                    "value": cur_px, "formatted": _fmt(cur_px, unit), "source": "fmp_quote",
                })
            for row in annual_rows:
                px = row.get("close") or row.get("adjClose") or row.get("price")
                d  = row.get("date") or ""
                if px is not None and d != today_str:
                    points.append({
                        "date": d, "fiscalYear": int(d[:4]) if len(d) >= 4 else None,
                        "value": px, "formatted": _fmt(px, unit), "source": "fmp_hist_price_eod",
                    })
        else:
            pct = q.get("changesPercentage") or q.get("changePercentage")
            if pct is not None:
                today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                points.append({
                    "date": today_str, "fiscalYear": datetime.now(timezone.utc).year,
                    "value": pct, "formatted": _fmt(pct, unit), "source": "fmp_quote",
                })

    # Deduplicate by date
    seen_dates: dict[str, dict] = {}
    for pt in points:
        d = pt.get("date") or ""
        if d not in seen_dates:
            seen_dates[d] = pt

    points = _sort_ascending(list(seen_dates.values()))

    if not points and metric_source not in ("news",):
        warnings.append(f"{symbol}: FMP returned no {metric_key} data.")

    latest = points[-1] if points else None
    return points, latest


# ── Screener / snapshot builder ───────────────────────────────────────────────

def _build_screener_row(
    symbol: str,
    profile: dict,
    quote: dict,
    data: dict,
    warnings: list[str],
) -> dict:
    """
    Build the full screener bundle (all 24 metrics + availability info).
    Uses fallback calculations where possible; null for unavailable values.
    Warnings are accumulated in-place.
    """
    inc    = data.get("income") or []
    cf     = data.get("cashflow") or data.get("snapshot_cf") or []
    bs     = data.get("balance") or data.get("snapshot_bs") or []
    rat    = data.get("ratios") or data.get("snapshot_ratios") or []
    km     = data.get("key_metrics") or data.get("snapshot_km") or []
    ev_raw = data.get("ev") or data.get("snapshot_ev") or []

    inc0 = inc[0] if inc else {}
    cf0  = cf[0]  if cf  else {}
    bs0  = bs[0]  if bs  else {}
    rat0 = rat[0] if rat else {}
    km0  = km[0]  if km  else {}
    ev0  = ev_raw[0] if ev_raw else {}

    # ── Income statement fields ───────────────────────────────────────────────
    rev  = inc0.get("revenue")
    gp   = inc0.get("grossProfit")
    oi   = inc0.get("operatingIncome")
    ni   = inc0.get("netIncome")
    ebit = inc0.get("ebitda")
    eps  = inc0.get("epsDiluted") or inc0.get("eps")

    gross_margin   = (
        inc0.get("grossProfitRatio") or inc0.get("grossProfitMargin")
        or _safe_div(gp, rev)
    )
    operating_margin = (
        inc0.get("operatingIncomeRatio")
        or _safe_div(oi, rev)
    )
    profit_margin = (
        inc0.get("netIncomeRatio") or inc0.get("netProfitMargin")
        or _safe_div(ni, rev)
    )

    # ── Cash flow fields ─────────────────────────────────────────────────────
    fcf = cf0.get("freeCashFlow")
    if fcf is None:
        ocf   = cf0.get("operatingCashFlow")
        capex = cf0.get("capitalExpenditure")
        if ocf is not None and capex is not None:
            fcf = ocf - abs(capex)
    fcf_margin = _safe_div(fcf, rev)

    # ── Balance sheet fields ─────────────────────────────────────────────────
    total_debt = bs0.get("totalDebt")
    if total_debt is None:
        st = bs0.get("shortTermDebt") or 0
        lt = bs0.get("longTermDebt") or 0
        total_debt = (st + lt) or None

    # ── Market / quote fields ────────────────────────────────────────────────
    mc         = quote.get("marketCap") or profile.get("mktCap")
    px         = quote.get("price")
    # changesPercentage is already a percent number (e.g. -1.82 means -1.82%)
    pct_change = quote.get("changesPercentage") or quote.get("changePercentage")
    if pct_change is None:
        _chg  = quote.get("change")
        _prev = quote.get("previousClose")
        if _chg is not None and _prev and _prev != 0:
            pct_change = (_chg / _prev) * 100

    # ── Ratios ───────────────────────────────────────────────────────────────
    ps = rat0.get("priceToSalesRatio") or km0.get("priceToSalesRatio")
    pe = rat0.get("priceToEarningsRatio") or rat0.get("priceEarningsRatio") or km0.get("peRatio")
    if pe is not None and pe <= 0:
        pe = None

    d_to_e  = rat0.get("debtEquityRatio") or rat0.get("debtToEquity")
    cur_rat = rat0.get("currentRatio")
    roe_val = rat0.get("returnOnEquity") or rat0.get("roe")
    roa_val = rat0.get("returnOnAssets") or rat0.get("roa")

    # Fallbacks for ratios
    if ps is None and mc is not None and rev:
        ps = _safe_div(mc, rev)
    if pe is None and mc is not None and ni and ni > 0:
        pe = _safe_div(mc, ni)
    if d_to_e is None:
        equity = bs0.get("totalStockholdersEquity")
        d_to_e = _safe_div(total_debt, equity)
    if cur_rat is None:
        ca = bs0.get("totalCurrentAssets")
        cl = bs0.get("totalCurrentLiabilities")
        cur_rat = _safe_div(ca, cl)
    # ROE fallback: netIncome / totalStockholdersEquity
    if roe_val is None and ni is not None:
        equity = bs0.get("totalStockholdersEquity")
        roe_val = _safe_div(ni, equity)
    # ROA fallback: netIncome / totalAssets
    if roa_val is None and ni is not None:
        total_assets = bs0.get("totalAssets")
        roa_val = _safe_div(ni, total_assets)

    # ── Key metrics / EV ─────────────────────────────────────────────────────
    ev_val      = ev0.get("enterpriseValue") or km0.get("enterpriseValue")
    ev_to_ebit  = km0.get("enterpriseValueOverEBITDA")
    if ev_to_ebit is None and ev_val is not None and ebit and ebit > 0:
        ev_to_ebit = _safe_div(ev_val, ebit)

    # ── Revenue growth ───────────────────────────────────────────────────────
    rev_growth = None
    gr = data.get("growth") or []
    if gr:
        rev_growth = gr[0].get("revenueGrowth")
    if rev_growth is None and len(inc) >= 2:
        prev_rev = inc[1].get("revenue")
        rev_growth = _safe_div(
            (rev - prev_rev) if (rev is not None and prev_rev is not None) else None,
            prev_rev,
        )

    # ── Fiscal date ──────────────────────────────────────────────────────────
    latest_fiscal_date = inc0.get("date") or bs0.get("date")
    latest_fiscal_year = None
    if latest_fiscal_date:
        try:
            latest_fiscal_year = int(str(latest_fiscal_date)[:4])
        except ValueError:
            pass

    # ── Availability ─────────────────────────────────────────────────────────
    all_values = {
        "price":              px,
        "price_change_percent": pct_change,
        "market_cap":         mc,
        "enterprise_value":   ev_val,
        "revenue":            rev,
        "revenue_growth":     rev_growth,
        "gross_profit":       gp,
        "gross_margin":       gross_margin,
        "operating_income":   oi,
        "operating_margin":   operating_margin,
        "net_income":         ni,
        "profit_margin":      profit_margin,
        "eps_diluted":        eps,
        "ebitda":             ebit,
        "free_cash_flow":     fcf,
        "fcf_margin":         fcf_margin,
        "total_debt":         total_debt,
        "debt_to_equity":     d_to_e,
        "current_ratio":      cur_rat,
        "ps_ratio":           ps,
        "pe_ratio":           pe,
        "ev_to_ebitda":       ev_to_ebit,
        "roe":                roe_val,
        "roa":                roa_val,
    }
    available = [k for k, v in all_values.items() if v is not None]
    missing   = [k for k, v in all_values.items() if v is None]

    if missing:
        print(f"[stock_compare] metricAvailability {symbol} available={len(available)} missing={len(missing)}")
        for m in missing:
            if m in ("ev_to_ebitda", "enterprise_value", "debt_to_equity", "current_ratio", "roe", "roa"):
                print(f"[stock_compare] warning {symbol} missing {m}")

    n_avail = len(available)
    total   = len(all_values)
    if n_avail == total:
        data_quality = "complete"
    elif n_avail >= total * 0.75:
        data_quality = "mostly_complete"
    elif n_avail >= total * 0.5:
        data_quality = "partial"
    else:
        data_quality = "sparse"

    name = profile.get("companyName") or profile.get("name") or symbol

    return {
        "symbol":            symbol,
        "name":              name,
        # Price / market
        "price":             px,
        "priceChangePercent": pct_change,
        "marketCap":         mc,
        "enterpriseValue":   ev_val,
        # Income
        "revenue":           rev,
        "revenueGrowth":     rev_growth,
        "grossProfit":       gp,
        "grossMargin":       gross_margin,
        "operatingIncome":   oi,
        "operatingMargin":   operating_margin,
        "netIncome":         ni,
        "profitMargin":      profit_margin,
        "epsDiluted":        eps,
        "ebitda":            ebit,
        # Cash flow
        "freeCashFlow":      fcf,
        "fcfMargin":         fcf_margin,
        # Balance
        "totalDebt":         total_debt,
        "debtToEquity":      d_to_e,
        "currentRatio":      cur_rat,
        # Valuation ratios
        "psRatio":           ps,
        "peRatio":           pe,
        "evToEbitda":        ev_to_ebit,
        # Profitability ratios
        "roe":               roe_val,
        "roa":               roa_val,
        # Metadata
        "latestFiscalDate":  latest_fiscal_date,
        "latestFiscalYear":  latest_fiscal_year,
        "dataQuality":       data_quality,
        "availableMetrics":  available,
        "missingMetrics":    missing,
        "warnings":          [],
    }


def _build_snapshot_row(symbol: str, profile: dict, quote: dict, data: dict) -> dict:
    """
    Backward-compat snapshot row (subset of screener row).
    Kept for consumers that depend on the old field names.
    """
    row = _build_screener_row(symbol, profile, quote, data, [])
    mc  = row["marketCap"]
    rev = row["revenue"]
    ni  = row["netIncome"]
    return {
        "symbol":          symbol,
        "name":            row["name"],
        "marketCap":       mc,
        "revenue":         rev,
        "revenueGrowth":   row["revenueGrowth"],
        "grossProfit":     row["grossProfit"],
        "grossMargin":     row["grossMargin"],
        "profitMargin":    row["profitMargin"],
        "epsDiluted":      row["epsDiluted"],
        "operatingIncome": row["operatingIncome"],
        "netIncome":       ni,
        "ebitda":          row["ebitda"],
        "freeCashFlow":    row["freeCashFlow"],
        "totalDebt":       row["totalDebt"],
        "psRatio":         row["psRatio"],
        "peRatio":         row["peRatio"],
        "lastPrice":       row["price"],
        "priceChange1D":   (row["priceChangePercent"] / 100.0) if row["priceChangePercent"] is not None else None,
    }


# ── Main entry point ──────────────────────────────────────────────────────────

async def compare_metrics(
    symbols: list[str],
    metric: str,
    period: str,
    range_val: str,
    fmp_key: str,
) -> dict:
    """
    Build the full comparison response for up to 15 tickers.
    Individual symbol failures are demoted to warnings — never a 500.
    """
    resolved_metric = METRIC_ALIASES.get(metric.lower(), metric.lower())
    if resolved_metric not in METRIC_MAP:
        resolved_metric = METRIC_ALIASES.get(metric, metric)
    if resolved_metric not in METRIC_MAP:
        resolved_metric = "revenue"

    warnings: list[str]        = []
    missing_symbols: list[str] = []
    valid_symbols: list[str]   = []

    range_val = _coerce_range(range_val, warnings)

    meta_def   = METRIC_MAP[resolved_metric]
    unit       = meta_def["unit"]
    mtr_source = meta_def["source"]

    # Handle recent_news gracefully — it is not a chart metric
    is_news_metric = (resolved_metric == "recent_news")
    if is_news_metric:
        warnings.append(
            "recent_news is not a numeric chart metric. Use the news field instead."
        )

    print(
        f"[stock_compare] metric={resolved_metric} range={range_val} "
        f"period={period} symbols={','.join(symbols)}"
    )

    fmp = StockCompareFMP(fmp_key)

    fetch_tasks = {
        sym: _fetch_symbol_data(sym, mtr_source if not is_news_metric else "news", period, range_val, fmp)
        for sym in symbols
    }
    fetch_results = await asyncio.gather(*fetch_tasks.values(), return_exceptions=True)
    symbol_data: dict[str, dict] = {}
    for sym, result in zip(fetch_tasks.keys(), fetch_results):
        if isinstance(result, Exception):
            warnings.append(f"{sym}: data fetch exception — {result}")
            symbol_data[sym] = {}
        else:
            symbol_data[sym] = result or {}

    series:   list[dict] = []
    snapshot: list[dict] = []
    screener: list[dict] = []
    news_map: dict[str, list] = {}
    metric_availability: dict[str, dict] = {}

    for idx, sym in enumerate(symbols):
        try:
            data    = symbol_data.get(sym) or {}
            profile = data.get("profile") or {}
            quote   = data.get("quote") or {}

            has_data = bool(
                profile or quote or
                data.get("income") or data.get("cashflow") or data.get("balance") or
                data.get("ratios") or data.get("key_metrics") or data.get("growth") or
                data.get("hist_mc") or data.get("ev") or data.get("hist_px") or
                data.get("news")
            )
            if not has_data:
                missing_symbols.append(sym)
                warnings.append(
                    f"{sym}: FMP returned no usable data — "
                    "symbol may be unsupported, non-US, or delisted."
                )
                series.append({
                    "symbol": sym, "name": sym,
                    "colorKey": _COLOR_KEYS[idx % len(_COLOR_KEYS)],
                    "points": [], "latest": None,
                    "status": "unsupported_or_no_data",
                    "reason": "No usable FMP fundamentals returned for this symbol.",
                })
                metric_availability[sym] = {"available": [], "missing": list(METRIC_DEFINITIONS.keys())}
                continue

            valid_symbols.append(sym)
            name = profile.get("companyName") or profile.get("name") or sym

            # Series points (skip if recent_news)
            if not is_news_metric:
                pts, latest = _extract_series_points(
                    sym, resolved_metric, data, period, range_val, unit, warnings
                )
            else:
                pts, latest = [], None

            series.append({
                "symbol":   sym,
                "name":     name,
                "colorKey": _COLOR_KEYS[idx % len(_COLOR_KEYS)],
                "points":   pts,
                "latest":   {
                    "date":      latest["date"]      if latest else None,
                    "value":     latest["value"]     if latest else None,
                    "formatted": latest["formatted"] if latest else None,
                } if latest else None,
            })

            # Screener row (full 24-metric bundle)
            sym_warnings: list[str] = []
            try:
                scr = _build_screener_row(sym, profile, quote, data, sym_warnings)
                screener.append(scr)
                metric_availability[sym] = {
                    "available": scr["availableMetrics"],
                    "missing":   scr["missingMetrics"],
                }
            except Exception as e:
                sym_warnings.append(f"screener build error — {e}")

            # Backward-compat snapshot row
            try:
                snap = _build_snapshot_row(sym, profile, quote, data)
                snapshot.append(snap)
            except Exception as e:
                warnings.append(f"{sym}: snapshot build error — {e}")

            warnings.extend(sym_warnings)

            # News
            raw_news = data.get("news") or []
            if is_news_metric and not raw_news:
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

        except Exception as sym_err:
            missing_symbols.append(sym)
            warnings.append(f"{sym}: unexpected error during processing — {sym_err}")
            series.append({
                "symbol": sym, "name": sym,
                "colorKey": _COLOR_KEYS[idx % len(_COLOR_KEYS)],
                "points": [], "latest": None,
                "status": "error",
                "reason": str(sym_err),
            })

    if range_val == "1Y" and period == "annual":
        warnings.append(
            "Annual financial data updates once per fiscal year; "
            "showing the latest available annual points for chart continuity."
        )

    metric_def_out = METRIC_DEFINITIONS.get(resolved_metric)
    return {
        "metric": {
            "key":      resolved_metric,
            "label":    meta_def["label"],
            "unit":     unit,
            "period":   period,
            "chartable": metric_def_out["chartable"] if metric_def_out else False,
        },
        "range":               range_val,
        "symbols":             valid_symbols,
        "series":              series,
        "screener":            screener,
        "snapshot":            snapshot,       # kept for backward compat
        "news":                news_map,
        "metricAvailability":  metric_availability,
        "missingSymbols":      missing_symbols,
        "invalidSymbols":      missing_symbols,  # backward compat alias
        "meta": {
            "sourcePriority": ["fmp"],
            "cached":         False,
            "generatedAt":    datetime.now(timezone.utc).isoformat(),
            "warnings":       warnings,
            "invalidSymbols": missing_symbols,
            "validSymbols":   valid_symbols,
        },
    }


# ── Metric definitions endpoint helper ───────────────────────────────────────

def get_metric_definitions() -> dict:
    """Return full metric registry for the /metrics endpoint."""
    metrics = []
    for key, defn in METRIC_DEFINITIONS.items():
        metrics.append({
            "key":       key,
            "label":     defn["label"],
            "unit":      defn["unit"],
            "chartable": defn["chartable"],
            "screener":  defn["screener"],
            "source":    defn["sourceEndpoint"],
            "fallback":  defn.get("fallback"),
        })
    return {
        "metrics":           metrics,
        "nonChartSections":  NON_CHART_SECTIONS,
        "total":             len(metrics),
    }


# ── Diagnostics ───────────────────────────────────────────────────────────────

async def get_diagnostics(symbols: list[str], period: str, fmp_key: str) -> dict:
    """
    Per-symbol data availability diagnostics.
    Fetches all relevant endpoints for each symbol and reports which metrics
    are available or missing, plus per-endpoint status.
    Never throws — all failures are captured as warnings.
    """
    t0   = time.monotonic()
    fmp  = StockCompareFMP(fmp_key)
    rows = []

    for sym in symbols:
        try:
            # Fetch all endpoints in parallel
            keys = [
                "profile", "quote",
                "income_statement", "balance_sheet", "cash_flow",
                "ratios", "key_metrics", "enterprise_values",
            ]
            tasks = [
                fmp.profile(sym),
                fmp.quote(sym),
                fmp.income_statement(sym, period, 5),
                fmp.balance_sheet(sym, period, 5),
                fmp.cash_flow(sym, period, 5),
                fmp.ratios(sym, period, 5),
                fmp.key_metrics(sym, period, 5),
                fmp.enterprise_values(sym, period, 5),
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            fetched: dict[str, Any] = {}
            source_endpoints: dict[str, dict] = {}
            for k, v in zip(keys, results):
                if isinstance(v, Exception):
                    fetched[k] = [] if k != "profile" and k != "quote" else {}
                    source_endpoints[k] = {"status": "error", "rows": 0, "cached": False, "error": str(v)}
                elif isinstance(v, list):
                    fetched[k] = v
                    cached = cache.get(f"sc:{k.split('_')[0]}:{sym}:{period}:5") is not None
                    source_endpoints[k] = {"status": "ok", "rows": len(v), "cached": cached}
                elif isinstance(v, dict):
                    fetched[k] = v
                    source_endpoints[k] = {"status": "ok", "rows": 1 if v else 0, "cached": False}
                else:
                    fetched[k] = []
                    source_endpoints[k] = {"status": "empty", "rows": 0, "cached": False}

            # Map to _build_screener_row input shape
            data = {
                "income":       fetched.get("income_statement") or [],
                "balance":      fetched.get("balance_sheet") or [],
                "cashflow":     fetched.get("cash_flow") or [],
                "ratios":       fetched.get("ratios") or [],
                "key_metrics":  fetched.get("key_metrics") or [],
                "ev":           fetched.get("enterprise_values") or [],
            }
            profile = fetched.get("profile") or {}
            if isinstance(profile, list):
                profile = profile[0] if profile else {}
            quote   = fetched.get("quote") or {}
            if isinstance(quote, list):
                quote = quote[0] if quote else {}

            sym_warnings: list[str] = []
            scr = _build_screener_row(sym, profile, quote, data, sym_warnings)
            name = scr.get("name") or sym

            rows.append({
                "symbol":           sym,
                "name":             name,
                "availableMetrics": scr["availableMetrics"],
                "missingMetrics":   scr["missingMetrics"],
                "dataQuality":      scr["dataQuality"],
                "sourceEndpoints":  source_endpoints,
                "warnings":         sym_warnings,
            })

        except Exception as e:
            rows.append({
                "symbol":           sym,
                "name":             sym,
                "availableMetrics": [],
                "missingMetrics":   list(METRIC_DEFINITIONS.keys()),
                "dataQuality":      "error",
                "sourceEndpoints":  {},
                "warnings":         [f"diagnostics error: {e}"],
            })

    ms = int((time.monotonic() - t0) * 1000)
    print(f"[stock_compare] diagnostics symbols={','.join(symbols)} ms={ms}")

    return {
        "symbols": symbols,
        "results": rows,
        "meta": {
            "generatedAt": datetime.now(timezone.utc).isoformat(),
            "warnings":    [],
        },
    }
