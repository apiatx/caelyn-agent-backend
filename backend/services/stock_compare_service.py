"""
Stock Compare service — multi-ticker financial metric comparison.

FMP Stable API is the primary data source (Starter plan, 300 req/min, 5Y max).
No AI/Grok calls.

Design decisions
────────────────
• Max history: 5 years.  Ranges 10Y / MAX / ALL are coerced to 5Y + warning.
• Annual period: always return at least 3 points for chart continuity.
• Points are returned ASCENDING (FMP returns newest-first; we reverse).
• Metric-aware fetching: only the FMP endpoints needed for the metric are called.
  Snapshot endpoints (CF / BS / ratios) are not fetched when already covered by
  the metric's own series fetch.
• Every per-symbol block is wrapped in try/except — no individual symbol failure
  can produce a 500; it is demoted to a warning + missingSymbols entry.
• Structured logging: [stock_compare] prefix on every FMP call.
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

# Cache TTLs
_TTL_SEARCH    = 86_400   # 24 h
_TTL_PROFILE   = 86_400   # 24 h
_TTL_STATEMENT = 86_400   # 24 h  (annual income / balance / cash flow)
_TTL_RATIOS    = 86_400   # 24 h
_TTL_GROWTH    = 86_400   # 24 h
_TTL_QUOTE     = 900      # 15 min
_TTL_NEWS      = 1_800    # 30 min
_TTL_HIST_MC   = 1_800    # 30 min

# Colour keys for chart series
_COLOR_KEYS = [
    "blue", "red", "green", "orange", "purple",
    "teal", "pink", "yellow", "indigo", "cyan",
    "lime", "amber", "rose", "violet", "emerald",
]

# Supported ranges on FMP Starter plan (max 5Y).
# 10Y / MAX / ALL are accepted by the router for backwards compat, but
# coerced to 5Y inside the service with a warning.
_SUPPORTED_RANGES = {"1Y", "3Y", "5Y", "YTD"}
_COERCE_TO_5Y    = {"10Y", "MAX", "ALL", "1M", "3M", "6M"}

# How many annual rows to guarantee per range bucket.
# FMP returns newest-first; we take the first N rows then sort ascending.
_ANNUAL_ROWS: dict[str, int] = {
    "1Y":  3,   # chart needs continuity — show 3 most recent fiscal years
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

# ── VALID_RANGES is the full set the *router* accepts (router tolerates legacy) ──
VALID_RANGES = {"1Y", "3Y", "5Y", "YTD", "1M", "3M", "6M", "10Y", "MAX", "ALL"}

# Canonical metric definitions
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
            print(f"[stock_compare] FMP {endpoint} {log_label} status={status} rows={rows} ms={ms}")
            if result:
                cache.set(cache_key, result, ttl)
            return result
        except Exception as e:
            ms = int((time.monotonic() - t0) * 1000)
            print(f"[stock_compare] FMP {endpoint} {log_label} error={e} ms={ms}")
            return []

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
        data = await self._get("profile", {"symbol": symbol}, ck, _TTL_PROFILE, symbol)
        return data[0] if isinstance(data, list) and data else (data if isinstance(data, dict) else {})

    async def quote(self, symbol: str) -> dict:
        ck = f"sc:quote:{symbol}"
        data = await self._get("quote", {"symbol": symbol}, ck, _TTL_QUOTE, symbol)
        return data[0] if isinstance(data, list) and data else (data if isinstance(data, dict) else {})

    async def income_statement(self, symbol: str, period: str, limit: int) -> list:
        ck = f"sc:income:{symbol}:{period}:{limit}"
        data = await self._get(
            "income-statement",
            {"symbol": symbol, "period": period, "limit": limit},
            ck, _TTL_STATEMENT, symbol,
        )
        return data if isinstance(data, list) else []

    async def balance_sheet(self, symbol: str, period: str, limit: int) -> list:
        ck = f"sc:balance:{symbol}:{period}:{limit}"
        data = await self._get(
            "balance-sheet-statement",
            {"symbol": symbol, "period": period, "limit": limit},
            ck, _TTL_STATEMENT, symbol,
        )
        return data if isinstance(data, list) else []

    async def cash_flow(self, symbol: str, period: str, limit: int) -> list:
        ck = f"sc:cashflow:{symbol}:{period}:{limit}"
        data = await self._get(
            "cash-flow-statement",
            {"symbol": symbol, "period": period, "limit": limit},
            ck, _TTL_STATEMENT, symbol,
        )
        return data if isinstance(data, list) else []

    async def ratios(self, symbol: str, period: str, limit: int) -> list:
        ck = f"sc:ratios:{symbol}:{period}:{limit}"
        data = await self._get(
            "ratios",
            {"symbol": symbol, "period": period, "limit": limit},
            ck, _TTL_RATIOS, symbol,
        )
        return data if isinstance(data, list) else []

    async def financial_growth(self, symbol: str, period: str, limit: int) -> list:
        ck = f"sc:growth:{symbol}:{period}:{limit}"
        data = await self._get(
            "financial-growth",
            {"symbol": symbol, "period": period, "limit": limit},
            ck, _TTL_GROWTH, symbol,
        )
        return data if isinstance(data, list) else []

    async def hist_market_cap(self, symbol: str) -> list:
        """Historical market cap — daily series; we sample annually."""
        ck = f"sc:histmc:{symbol}"
        data = await self._get(
            "historical-market-capitalization",
            {"symbol": symbol, "limit": 1826},   # ~5 years daily
            ck, _TTL_HIST_MC, symbol,
        )
        return data if isinstance(data, list) else []

    async def news(self, symbol: str, limit: int = 5) -> list:
        ck = f"sc:news:{symbol}:{limit}"
        data = await self._get(
            "news/stock",
            {"symbols": symbol, "limit": limit},
            ck, _TTL_NEWS, symbol,
        )
        return data if isinstance(data, list) else []


# ── Helpers ───────────────────────────────────────────────────────────────────

def _fmt(value: Optional[float], unit: str) -> Optional[str]:
    if value is None:
        return None
    if unit == "currency":
        av, sign = abs(value), ("-" if value < 0 else "")
        if av >= 1e12: return f"{sign}${av/1e12:.2f}T"
        if av >= 1e9:  return f"{sign}${av/1e9:.2f}B"
        if av >= 1e6:  return f"{sign}${av/1e6:.2f}M"
        if av >= 1e3:  return f"{sign}${av/1e3:.2f}K"
        return f"{sign}${av:.2f}"
    if unit == "percent": return f"{value*100:.1f}%"
    if unit == "ratio":   return f"{value:.2f}x"
    return str(round(value, 4))


def _safe_div(num: Optional[float], denom: Optional[float]) -> Optional[float]:
    if num is None or denom is None or denom == 0:
        return None
    return num / denom


def _row_limit(period: str, range_val: str) -> int:
    """How many rows to request from FMP for a given period and range."""
    rv = range_val.upper()
    if period == "quarterly":
        return _QUARTERLY_ROWS.get(rv, 20)
    return _ANNUAL_ROWS.get(rv, 5)


def _trim_rows(rows: list, period: str, range_val: str) -> list:
    """
    Trim FMP rows (newest-first) to the target count for the range.
    Annual always returns at least 3 rows for chart continuity.
    """
    rv = range_val.upper()
    if period == "quarterly":
        n = _QUARTERLY_ROWS.get(rv, 20)
    else:
        n = _ANNUAL_ROWS.get(rv, 5)
    return rows[:max(n, 1)]


def _coerce_range(range_val: str, warnings: list[str]) -> str:
    """Coerce unsupported ranges to 5Y with a warning."""
    rv = range_val.upper()
    if rv in _COERCE_TO_5Y:
        warnings.append(
            f"Range {rv!r} is not supported on the current FMP plan (max 5Y); using 5Y instead."
        )
        return "5Y"
    return rv


def _sort_ascending(points: list[dict]) -> list[dict]:
    """Sort series points by date ascending (FMP returns newest-first)."""
    try:
        return sorted(points, key=lambda p: p.get("date") or "")
    except Exception:
        return points


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

    Snapshot logic:
    - profile + quote are always fetched (snapshot metadata + price)
    - income_statement(limit=2) always fetched (revenue/grossProfit/margin/EPS)
    - cash_flow / balance_sheet / ratios are only fetched for the snapshot when
      they are NOT already being fetched for the metric series (avoids redundant calls).
    """
    series_limit = _row_limit(period, range_val)

    tasks: dict[str, Any] = {
        "profile": fmp.profile(symbol),
        "quote":   fmp.quote(symbol),
    }

    # ── Series fetch (metric-specific) ───────────────────────────────────────
    if metric_source == "income":
        tasks["income"] = fmp.income_statement(symbol, period, series_limit)

    elif metric_source == "growth":
        tasks["income"] = fmp.income_statement(symbol, period, series_limit + 1)
        tasks["growth"] = fmp.financial_growth(symbol, period, series_limit)

    elif metric_source == "cashflow":
        tasks["cashflow"] = fmp.cash_flow(symbol, period, series_limit)

    elif metric_source == "balance":
        tasks["balance"] = fmp.balance_sheet(symbol, period, series_limit)

    elif metric_source == "ratios":
        tasks["metrics"] = fmp.ratios(symbol, period, series_limit)

    elif metric_source == "market_cap":
        tasks["hist_mc"] = fmp.hist_market_cap(symbol)

    elif metric_source == "news":
        tasks["news"] = fmp.news(symbol, limit=5)

    # ── Snapshot supplement (only if not already fetched above) ──────────────
    # income: always needed for snapshot revenue/grossProfit/EPS row
    if "income" not in tasks:
        tasks["income"] = fmp.income_statement(symbol, period, 2)

    # cashflow: needed for FCF in snapshot — skip if already fetched for series
    if "cashflow" not in tasks:
        tasks["snapshot_cf"] = fmp.cash_flow(symbol, period, 1)

    # balance: needed for totalDebt in snapshot
    if "balance" not in tasks:
        tasks["snapshot_bs"] = fmp.balance_sheet(symbol, period, 1)

    # ratios: needed for P/S and P/E in snapshot
    if "metrics" not in tasks:
        tasks["snapshot_km"] = fmp.ratios(symbol, period, 1)

    keys   = list(tasks.keys())
    values = await asyncio.gather(*tasks.values(), return_exceptions=True)

    result: dict[str, Any] = {}
    for k, v in zip(keys, values):
        result[k] = [] if isinstance(v, Exception) else (v if v is not None else [])

    return result


# ── Value extractors ──────────────────────────────────────────────────────────

def _income_value(metric_key: str, row: dict, symbol: str, warnings: list[str]) -> Optional[float]:
    try:
        if metric_key == "revenue":         return row.get("revenue")
        if metric_key == "gross_profit":    return row.get("grossProfit")
        if metric_key == "operating_income":return row.get("operatingIncome")
        if metric_key == "net_income":      return row.get("netIncome")
        if metric_key == "eps_diluted":
            return row.get("epsDiluted") or row.get("eps")
        if metric_key == "ebitda":
            return row.get("ebitda") or row.get("operatingIncome")
        if metric_key == "gross_margin":
            v = _safe_div(row.get("grossProfit"), row.get("revenue"))
            if v is None:
                warnings.append(f"{symbol}: gross margin unavailable for {row.get('date')}")
            return v
        if metric_key == "profit_margin":
            v = _safe_div(row.get("netIncome"), row.get("revenue"))
            if v is None:
                warnings.append(f"{symbol}: profit margin unavailable for {row.get('date')}")
            return v
    except Exception as e:
        warnings.append(f"{symbol}: error extracting {metric_key}: {e}")
    return None


def _cashflow_value(metric_key: str, row: dict, symbol: str, warnings: list[str]) -> Optional[float]:
    try:
        fcf = row.get("freeCashFlow")
        if fcf is not None:
            return fcf
        ocf   = row.get("operatingCashFlow")
        capex = row.get("capitalExpenditure")
        if ocf is not None and capex is not None:
            return ocf - abs(capex)
        warnings.append(f"{symbol}: free cash flow unavailable for {row.get('date')}")
    except Exception as e:
        warnings.append(f"{symbol}: error extracting free_cash_flow: {e}")
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
        warnings.append(f"{symbol}: total debt unavailable for {row.get('date')}")
    except Exception as e:
        warnings.append(f"{symbol}: error extracting total_debt: {e}")
    return None


def _ratio_value(metric_key: str, row: dict, symbol: str, warnings: list[str]) -> Optional[float]:
    try:
        if metric_key == "ps_ratio":
            v = row.get("priceToSalesRatio")
            if v is None:
                warnings.append(f"{symbol}: P/S ratio unavailable for {row.get('date')}")
            return v
        if metric_key == "pe_ratio":
            v = row.get("priceToEarningsRatio")
            if v is None:
                warnings.append(f"{symbol}: P/E ratio unavailable for {row.get('date')}")
                return None
            if v <= 0:
                warnings.append(f"{symbol}: P/E skipped (negative/zero earnings) for {row.get('date')}")
                return None
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


def _sample_annual_hist_mc(rows: list) -> list:
    """Sample one row per calendar year (take the last row of each year)."""
    by_year: dict[int, dict] = {}
    for row in rows:
        raw = row.get("date") or ""
        if not raw:
            continue
        try:
            yr = int(raw[:4])
        except ValueError:
            continue
        if yr not in by_year or raw > (by_year[yr].get("date") or ""):
            by_year[yr] = row
    return sorted(by_year.values(), key=lambda x: x.get("date") or "", reverse=True)


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
        # Use growth endpoint primary; fall back to income-statement YoY calc
        growth_rows = _trim_rows(data.get("growth") or [], period, range_val)
        if not growth_rows:
            # Fallback: compute from income statement
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
        rows = _trim_rows(data.get("cashflow") or [], period, range_val)
        for row in rows:
            v = _cashflow_value(metric_key, row, symbol, warnings)
            if v is not None:
                points.append(_make_pt(row, v, "fmp_cash_flow_statement"))

    elif metric_source == "balance":
        rows = _trim_rows(data.get("balance") or [], period, range_val)
        for row in rows:
            v = _balance_value(metric_key, row, symbol, warnings)
            if v is not None:
                points.append(_make_pt(row, v, "fmp_balance_sheet"))

    elif metric_source == "ratios":
        rows = _trim_rows(data.get("metrics") or [], period, range_val)
        for row in rows:
            v = _ratio_value(metric_key, row, symbol, warnings)
            if v is not None:
                points.append(_make_pt(row, v, "fmp_ratios"))

    elif metric_source == "market_cap":
        hist       = data.get("hist_mc") or []
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

    # Deduplicate by date
    seen_dates: dict[str, dict] = {}
    for pt in points:
        d = pt.get("date") or ""
        if d not in seen_dates:
            seen_dates[d] = pt

    # Sort ASCENDING (FMP is newest-first; frontend needs oldest-first)
    points = _sort_ascending(list(seen_dates.values()))

    if not points and metric_source not in ("news",):
        warnings.append(f"{symbol}: FMP returned no {metric_key} data.")

    latest = points[-1] if points else None
    return points, latest


# ── Snapshot builder ──────────────────────────────────────────────────────────

def _build_snapshot_row(symbol: str, profile: dict, quote: dict, data: dict) -> dict:
    inc   = (data.get("income") or [])
    cf    = (data.get("cashflow") or data.get("snapshot_cf") or [])
    bs    = (data.get("balance") or data.get("snapshot_bs") or [])
    km    = (data.get("metrics") or data.get("snapshot_km") or [])
    inc0  = inc[0] if inc else {}
    cf0   = cf[0]  if cf  else {}
    bs0   = bs[0]  if bs  else {}
    km0   = km[0]  if km  else {}

    rev = inc0.get("revenue")
    gp  = inc0.get("grossProfit")
    ni  = inc0.get("netIncome")
    mc  = quote.get("marketCap") or profile.get("mktCap")

    gross_margin  = _safe_div(gp, rev)
    profit_margin = _safe_div(ni, rev)

    fcf = cf0.get("freeCashFlow")
    if fcf is None:
        ocf = cf0.get("operatingCashFlow")
        cap = cf0.get("capitalExpenditure")
        if ocf is not None and cap is not None:
            fcf = ocf - abs(cap)

    total_debt = bs0.get("totalDebt")
    if total_debt is None:
        total_debt = (bs0.get("shortTermDebt") or 0) + (bs0.get("longTermDebt") or 0) or None

    ps = km0.get("priceToSalesRatio")
    pe = km0.get("priceToEarningsRatio")
    if pe is not None and pe <= 0:
        pe = None

    if ps is None: ps = _safe_div(mc, rev)
    if pe is None and ni and ni > 0: pe = _safe_div(mc, ni)

    rev_growth = None
    gr = data.get("growth") or []
    if gr:
        rev_growth = gr[0].get("revenueGrowth")
    if rev_growth is None and len(inc) >= 2:
        prev_rev = inc[1].get("revenue")
        rev_growth = _safe_div((rev - prev_rev) if (rev and prev_rev) else None, prev_rev)

    name = profile.get("companyName") or profile.get("name") or symbol
    pct_change = quote.get("changesPercentage")

    return {
        "symbol":         symbol,
        "name":           name,
        "marketCap":      mc,
        "revenue":        rev,
        "revenueGrowth":  rev_growth,
        "grossProfit":    gp,
        "grossMargin":    gross_margin,
        "profitMargin":   profit_margin,
        "epsDiluted":     inc0.get("epsDiluted") or inc0.get("eps"),
        "operatingIncome":inc0.get("operatingIncome"),
        "netIncome":      ni,
        "ebitda":         inc0.get("ebitda"),
        "freeCashFlow":   fcf,
        "totalDebt":      total_debt,
        "psRatio":        ps,
        "peRatio":        pe,
        "lastPrice":      quote.get("price"),
        "priceChange1D":  (pct_change / 100.0) if pct_change is not None else None,
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
    # Resolve aliases
    resolved_metric = METRIC_ALIASES.get(metric.lower(), metric.lower())
    if resolved_metric not in METRIC_MAP:
        resolved_metric = "revenue"

    warnings: list[str]        = []
    missing_symbols: list[str] = []
    valid_symbols: list[str]   = []

    # Coerce unsupported ranges
    range_val = _coerce_range(range_val, warnings)

    meta_def    = METRIC_MAP[resolved_metric]
    unit        = meta_def["unit"]
    mtr_source  = meta_def["source"]

    print(
        f"[stock_compare] metric={resolved_metric} range={range_val} "
        f"period={period} symbols={','.join(symbols)}"
    )

    fmp = StockCompareFMP(fmp_key)

    # ── Parallel fetch across all symbols ────────────────────────────────────
    fetch_tasks = {
        sym: _fetch_symbol_data(sym, mtr_source, period, range_val, fmp)
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

    # ── Build per-symbol series + snapshot ───────────────────────────────────
    series:   list[dict] = []
    snapshot: list[dict] = []
    news_map: dict[str, list] = {}

    for idx, sym in enumerate(symbols):
        try:
            data    = symbol_data.get(sym) or {}
            profile = data.get("profile") or {}
            quote   = data.get("quote") or {}

            # Detect symbol with no FMP data at all
            has_data = bool(
                profile or quote or
                data.get("income") or data.get("cashflow") or data.get("balance") or
                data.get("metrics") or data.get("growth") or data.get("hist_mc") or
                data.get("news")
            )
            if not has_data:
                missing_symbols.append(sym)
                warnings.append(
                    f"{sym}: FMP returned no usable data — "
                    "symbol may be unsupported, non-US, or delisted."
                )
                # Still append an empty series so the frontend knows the symbol was attempted
                series.append({
                    "symbol": sym, "name": sym,
                    "colorKey": _COLOR_KEYS[idx % len(_COLOR_KEYS)],
                    "points": [], "latest": None,
                    "status": "unsupported_or_no_data",
                    "reason": "No usable FMP annual fundamentals returned for this symbol.",
                })
                continue

            valid_symbols.append(sym)
            name = profile.get("companyName") or profile.get("name") or sym

            # Series points
            if resolved_metric != "recent_news":
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

            # Snapshot
            try:
                snap = _build_snapshot_row(sym, profile, quote, data)
                snapshot.append(snap)
            except Exception as e:
                warnings.append(f"{sym}: snapshot build error — {e}")

            # News (always included, lightweight)
            raw_news = data.get("news") or []
            if resolved_metric == "recent_news" and not raw_news:
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
            # Absolute safety net — no individual symbol error becomes a 500
            missing_symbols.append(sym)
            warnings.append(f"{sym}: unexpected error during processing — {sym_err}")
            series.append({
                "symbol": sym, "name": sym,
                "colorKey": _COLOR_KEYS[idx % len(_COLOR_KEYS)],
                "points": [], "latest": None,
                "status": "error",
                "reason": str(sym_err),
            })

    # Add annotation when 1Y returns multiple years
    if range_val == "1Y" and period == "annual":
        warnings.append(
            "Annual financial data updates once per fiscal year; "
            "showing the latest available annual points for chart continuity."
        )

    return {
        "metric": {
            "key":    resolved_metric,
            "label":  meta_def["label"],
            "unit":   unit,
            "period": period,
        },
        "range":          range_val,
        "symbols":        valid_symbols,
        "series":         series,
        "snapshot":       snapshot,
        "news":           news_map,
        "missingSymbols": missing_symbols,
        "meta": {
            "sourcePriority": ["fmp", "sec_edgar_fallback"],
            "cached":         False,
            "generatedAt":    datetime.now(timezone.utc).isoformat(),
            "warnings":       warnings,
            "invalidSymbols": missing_symbols,   # kept for backwards compat
            "validSymbols":   valid_symbols,
        },
    }
