"""
Caelyn Terminal — portfolio analytics provider.

Produces the full JSON payload for GET /api/caelyn-terminal.

Supports mixed asset types in a single portfolio:
  - stocks / ETFs  → Tradier (quotes + history)
  - crypto (BTC, ETH …) → CoinGecko API (quotes) + Yahoo Finance (history)
  - commodity (GOLD, etc.) → Tradier (GOLD is a listed equity/ETF)
                             Yahoo Finance fallback
  - Ticker tape extras (VIX, TLT, DXY) → Yahoo Finance
"""
from __future__ import annotations

import asyncio
import json
import math
import os
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

from data.cache import cache

try:
    from langsmith import traceable
except ImportError:
    def traceable(*a, **kw):
        def _d(fn): return fn
        return _d if not (a and callable(a[0])) else a[0]

# ─── Asset-class taxonomy ─────────────────────────────────────────────────────

_US_EQUITY  = "US Equity"
_INTL_DEV   = "Intl Developed"
_EM         = "Emerging Markets"
_FIXED      = "Fixed Income"
_STOCK      = "Individual Stocks"
_REAL       = "Real Estate"
_COMM       = "Commodities"
_CRYPTO     = "Crypto"
_THEMATIC   = "Thematic ETF"
_OTHER      = "Other"

ASSET_CLASS_MAP: dict[str, str] = {
    # Broad US equity
    "SCHB": _US_EQUITY, "VTI": _US_EQUITY, "ITOT": _US_EQUITY,
    "SPY": _US_EQUITY, "IVV": _US_EQUITY, "VOO": _US_EQUITY,
    "QQQ": _US_EQUITY, "QQQM": _US_EQUITY, "IWM": _US_EQUITY,
    "MDY": _US_EQUITY, "IJH": _US_EQUITY, "SCHA": _US_EQUITY,
    "DIA": _US_EQUITY, "RSP": _US_EQUITY,
    # Sector / dividend / factor / thematic
    "DGRO": _US_EQUITY, "VYM": _US_EQUITY, "SCHD": _US_EQUITY,
    "VIG": _US_EQUITY, "SDY": _US_EQUITY, "HDV": _US_EQUITY,
    "NOBL": _US_EQUITY, "DGRW": _US_EQUITY,
    "XLK": _US_EQUITY, "XLF": _US_EQUITY, "XLV": _US_EQUITY,
    "XLE": _US_EQUITY, "XLI": _US_EQUITY, "XLP": _US_EQUITY,
    "XLY": _US_EQUITY, "XLB": _US_EQUITY, "XLU": _US_EQUITY,
    "XLRE": _REAL,     "XLC": _US_EQUITY,
    "BUZZ": _THEMATIC, "ARKK": _THEMATIC, "ARKG": _THEMATIC,
    "ARKF": _THEMATIC, "ARKW": _THEMATIC, "BOTZ": _THEMATIC,
    # International developed
    "SCHF": _INTL_DEV, "VEA": _INTL_DEV, "EFA": _INTL_DEV,
    "IEFA": _INTL_DEV, "SPDW": _INTL_DEV, "VGK": _INTL_DEV,
    "EWJ": _INTL_DEV, "HEDJ": _INTL_DEV,
    # Emerging markets
    "VWO": _EM, "IEMG": _EM, "EEM": _EM, "SCHE": _EM,
    "SPEM": _EM, "DEM": _EM, "GXC": _EM, "MCHI": _EM,
    # Fixed income
    "AGG": _FIXED, "BND": _FIXED, "BNDX": _FIXED,
    "LQD": _FIXED, "HYG": _FIXED, "JNK": _FIXED,
    "TLT": _FIXED, "IEF": _FIXED, "SHY": _FIXED,
    "VTEB": _FIXED, "VCIT": _FIXED, "MUB": _FIXED,
    "SCHZ": _FIXED, "SCHI": _FIXED, "SCHS": _FIXED,
    # Real estate
    "VNQ": _REAL, "IYR": _REAL,
    # Commodities / hard assets
    "GLD": _COMM, "IAU": _COMM, "SLV": _COMM,
    "USO": _COMM, "DJP": _COMM, "PDBC": _COMM,
    "GOLD": _COMM,   # Barrick Gold — treat as commodity-adjacent
}

ASSET_CLASS_COLORS: dict[str, str] = {
    _US_EQUITY: "#38bdf8",
    _INTL_DEV:  "#6366f1",
    _EM:        "#f59e0b",
    _FIXED:     "#22c55e",
    _STOCK:     "#a78bfa",
    _REAL:      "#f43f5e",
    _COMM:      "#fb923c",
    _CRYPTO:    "#e879f9",
    _THEMATIC:  "#fbbf24",
    _OTHER:     "#94a3b8",
}

# CoinGecko coin-id map for common crypto tickers
COINGECKO_IDS: dict[str, str] = {
    "BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana",
    "BNB": "binancecoin", "XRP": "ripple", "ADA": "cardano",
    "AVAX": "avalanche-2", "DOGE": "dogecoin", "MATIC": "matic-network",
    "DOT": "polkadot", "LINK": "chainlink", "UNI": "uniswap",
    "AAVE": "aave", "LTC": "litecoin", "BCH": "bitcoin-cash",
    "SHIB": "shiba-inu", "ATOM": "cosmos", "SUI": "sui",
    "APT": "aptos", "ARB": "arbitrum", "NEAR": "near",
    "FIL": "filecoin", "TAO": "bittensor", "RENDER": "render-token",
    "HYPE": "hyperliquid",
}

# Yahoo Finance symbol overrides for non-standard tickers
YAHOO_SYMBOL_MAP: dict[str, str] = {
    "BTC":    "BTC-USD",
    "ETH":    "ETH-USD",
    "VIX":    "^VIX",
    "DXY":    "DX-Y.NYB",
    "GLD":    "GLD",
}

# Commodity tickers → Yahoo Finance futures symbol
COMMODITY_YAHOO_MAP: dict[str, str] = {
    "GOLD":     "GC=F",    # COMEX Gold Futures
    "SILVER":   "SI=F",    # COMEX Silver Futures
    "OIL":      "CL=F",    # WTI Crude Oil Futures
    "CRUDE":    "CL=F",
    "NATGAS":   "NG=F",    # Natural Gas Futures
    "COPPER":   "HG=F",    # Copper Futures
    "WHEAT":    "ZW=F",    # Wheat Futures
    "CORN":     "ZC=F",    # Corn Futures
    "PLATINUM": "PL=F",    # Platinum Futures
}

# Fixed expanded ticker tape symbols and their Yahoo symbols
TAPE_SYMBOLS: list[tuple[str, str]] = [
    ("SPY",  "SPY"),
    ("QQQ",  "QQQ"),
    ("IWM",  "IWM"),
    ("GLD",  "GLD"),
    ("TLT",  "TLT"),
    ("BTC",  "BTC-USD"),
    ("ETH",  "ETH-USD"),
    ("VIX",  "^VIX"),
    ("DXY",  "DX-Y.NYB"),
]

# Top S&P500 companies to add earnings calendar context for
SP500_EARNINGS_CONTEXT = ["MSFT", "AAPL", "GOOGL", "META", "AMZN", "NVDA", "JPM", "V"]

# ─── Helpers ─────────────────────────────────────────────────────────────────

def _sf(v: Any) -> float | None:
    try:
        return float(v) if v not in (None, "", "-") else None
    except Exception:
        return None

def _sr(v: float | None, n: int = 2) -> float | None:
    return round(v, n) if v is not None else None

def _returns(closes: list[float]) -> list[float]:
    r = []
    for i in range(1, len(closes)):
        if closes[i - 1] and closes[i - 1] != 0:
            r.append((closes[i] - closes[i - 1]) / closes[i - 1])
    return r

def _annualized_vol(closes: list[float]) -> float | None:
    rets = _returns(closes)
    if len(rets) < 10:
        return None
    n = len(rets)
    mean = sum(rets) / n
    variance = sum((r - mean) ** 2 for r in rets) / (n - 1)
    return round(math.sqrt(variance * 252) * 100, 2)

def _max_drawdown(vals: list[float]) -> float | None:
    if len(vals) < 2:
        return None
    peak = vals[0]
    max_dd = 0.0
    for v in vals:
        if v > peak:
            peak = v
        if peak > 0:
            dd = (peak - v) / peak
            if dd > max_dd:
                max_dd = dd
    return round(max_dd * 100, 2)

def _std(vals: list[float]) -> float:
    if not vals:
        return 0.0
    n = len(vals)
    mean = sum(vals) / n
    return math.sqrt(sum((v - mean) ** 2 for v in vals) / max(n - 1, 1))

def _correlation(a: list[float], b: list[float]) -> float | None:
    n = min(len(a), len(b))
    if n < 10:
        return None
    a, b = a[-n:], b[-n:]
    ma = sum(a) / n
    mb = sum(b) / n
    cov = sum((a[i] - ma) * (b[i] - mb) for i in range(n))
    va  = sum((x - ma) ** 2 for x in a)
    vb  = sum((x - mb) ** 2 for x in b)
    if va <= 0 or vb <= 0:
        return None
    return round(cov / math.sqrt(va * vb), 4)

def _market_status_et() -> str:
    import zoneinfo
    et = datetime.now(zoneinfo.ZoneInfo("America/New_York"))
    wd = et.weekday()
    h, m = et.hour, et.minute
    mins = h * 60 + m
    if wd >= 5:
        return "CLOSED"
    if mins < 240:
        return "CLOSED"
    if 240 <= mins < 570:
        return "PRE-MARKET"
    if 570 <= mins < 960:
        return "OPEN"
    if 960 <= mins < 1200:
        return "AFTER-HOURS"
    return "CLOSED"

def _month_label(dt: date) -> str:
    return dt.strftime("%b '%y")

def _asset_class(ticker: str, asset_type: str = "stock") -> str:
    t = ticker.upper()
    if asset_type == "crypto":
        return _CRYPTO
    if asset_type == "commodity":
        return _COMM
    return ASSET_CLASS_MAP.get(t, _STOCK)

# ─── CoinGecko simple price fetch ────────────────────────────────────────────

async def _cg_prices(coin_ids: list[str]) -> dict[str, dict]:
    """Fetch {coin_id: {usd, usd_24h_change}} from CoinGecko. Returns {}  on error."""
    if not coin_ids:
        return {}
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                "https://api.coingecko.com/api/v3/simple/price",
                params={
                    "ids":                 ",".join(coin_ids),
                    "vs_currencies":       "usd",
                    "include_24hr_change": "true",
                },
            )
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"[CAELYN] CoinGecko error: {e}")
    return {}

# ─── Yahoo Finance generic fetch ─────────────────────────────────────────────

_YF_CHART = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
_YF_HEADERS = {"User-Agent": "Mozilla/5.0"}

def _yf_fetch_sync(symbol: str, range_: str = "5d") -> dict:
    url = f"{_YF_CHART.format(symbol=symbol)}?interval=1d&range={range_}"
    req = urllib.request.Request(url, headers=_YF_HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"[CAELYN/YF] fetch error {symbol}: {e}")
        return {}

def _yf_parse_quote(raw: dict, sym: str) -> dict | None:
    try:
        res  = raw["chart"]["result"][0]
        meta = res["meta"]
        closes = res["indicators"]["quote"][0].get("close", [])
        timestamps = res.get("timestamp", [])
        valid = [(timestamps[i], closes[i]) for i in range(min(len(timestamps), len(closes))) if closes[i]]
        price = _sf(meta.get("regularMarketPrice"))
        prev_close = valid[-2][1] if len(valid) >= 2 else _sf(meta.get("previousClose"))
        chg = round(price - prev_close, 4) if price and prev_close else None
        chgpct = round((price - prev_close) / prev_close * 100, 3) if chg and prev_close else None
        w52h = _sf(meta.get("fiftyTwoWeekHigh"))
        w52l = _sf(meta.get("fiftyTwoWeekLow"))
        return {
            "symbol": sym, "price": price, "change": chg, "change_pct": chgpct,
            "prev_close": prev_close, "week_52_high": w52h, "week_52_low": w52l,
        }
    except Exception:
        return None

def _yf_parse_history(raw: dict) -> list[dict]:
    try:
        res = raw["chart"]["result"][0]
        closes = res["indicators"]["quote"][0].get("close", [])
        ts = res.get("timestamp", [])
        bars = []
        for i in range(min(len(ts), len(closes))):
            c = closes[i]
            if c is not None:
                bars.append({"date": datetime.fromtimestamp(ts[i]).strftime("%Y-%m-%d"), "close": c})
        return bars
    except Exception:
        return []

async def _yf_quote(yahoo_sym: str, display_sym: str) -> dict | None:
    raw = await asyncio.to_thread(_yf_fetch_sync, yahoo_sym, "5d")
    return _yf_parse_quote(raw, display_sym)

async def _yf_history(yahoo_sym: str, range_: str = "1y") -> list[dict]:
    raw = await asyncio.to_thread(_yf_fetch_sync, yahoo_sym, range_)
    return _yf_parse_history(raw)

# ─── Core provider ───────────────────────────────────────────────────────────

class CaelynTerminalProvider:

    def __init__(self, tradier, finnhub, fmp, yahoo, coingecko=None):
        self.tradier    = tradier
        self.finnhub    = finnhub
        self.fmp        = fmp
        self.yahoo      = yahoo
        self.coingecko  = coingecko

    @traceable(name="caelyn_terminal.get")
    async def get(self, portfolio_file: Path) -> dict:
        cache_key = "caelyn:terminal:v7"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        result = await self._build(portfolio_file)
        cache.set(cache_key, result, 90)
        return result

    async def _build(self, portfolio_file: Path) -> dict:
        # 1. Load holdings ────────────────────────────────────────────────
        holdings_raw = self._load_holdings(portfolio_file)
        if not holdings_raw:
            return self._empty()

        tickers   = [h["ticker"].upper() for h in holdings_raw]
        asset_map = {h["ticker"].upper(): (h.get("asset_type") or "stock").lower()
                     for h in holdings_raw}

        # Classify tickers by type
        equity_tickers  = [t for t in tickers if asset_map[t] in ("stock","etf","")]
        crypto_tickers  = [t for t in tickers if asset_map[t] == "crypto"]
        all_commodity   = [t for t in tickers if asset_map[t] == "commodity"]
        # Commodities with a futures yahoo symbol → Yahoo Finance
        # Commodities without one (unknown) → Tradier as equity fallback
        yf_commodity    = [t for t in all_commodity if t in COMMODITY_YAHOO_MAP]
        tradier_commodity = [t for t in all_commodity if t not in COMMODITY_YAHOO_MAP]
        tradier_tickers = equity_tickers + tradier_commodity

        # 2. Fetch live quotes (parallel) ─────────────────────────────────
        hist_start = (date.today() - timedelta(days=420)).isoformat()

        tasks = {
            "tradier_quotes":    self._fetch_tradier_quotes(tradier_tickers),
            "crypto_quotes":     self._fetch_crypto_quotes(crypto_tickers),
            "commodity_quotes":  self._fetch_commodity_quotes(yf_commodity),
            "tradier_history":   self._fetch_tradier_histories(tradier_tickers, hist_start),
            "crypto_history":    self._fetch_crypto_histories(crypto_tickers),
            "commodity_history": self._fetch_commodity_histories(yf_commodity),
            "spy_history":       _yf_history("SPY", "2y"),
            "earnings":          self._fetch_earnings_calendar(tickers),
            "news":              self._fetch_news(equity_tickers[:4] or tickers[:4]),
            "tape":              self._fetch_tape(equity_tickers),
        }

        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        R: dict[str, Any] = {
            k: (v if not isinstance(v, Exception) else None)
            for k, v in zip(tasks.keys(), results)
        }

        tradier_quotes     = {q["symbol"]: q for q in (R["tradier_quotes"] or []) if q.get("symbol")}
        crypto_quotes      = R["crypto_quotes"] or {}
        commodity_quotes   = R["commodity_quotes"] or {}
        tradier_history    = R["tradier_history"] or {}
        crypto_history     = R["crypto_history"] or {}
        commodity_history  = R["commodity_history"] or {}
        spy_bars           = R["spy_history"] or []

        # Merge all quotes and history
        def _q(sym: str) -> dict:
            if sym in tradier_quotes:
                q = tradier_quotes[sym]
                return {
                    "price":      _sf(q.get("last")),
                    "change":     _sf(q.get("change")),
                    "change_pct": _sf(q.get("change_percentage")),
                    "w52_high":   _sf(q.get("week_52_high")),
                    "w52_low":    _sf(q.get("week_52_low")),
                }
            if sym in crypto_quotes:
                return crypto_quotes[sym]
            if sym in commodity_quotes:
                return commodity_quotes[sym]
            return {}

        def _hist(sym: str) -> list[dict]:
            if sym in tradier_history:
                return tradier_history[sym]
            if sym in crypto_history:
                return crypto_history[sym]
            if sym in commodity_history:
                return commodity_history[sym]
            return []

        # 3. Build positions ──────────────────────────────────────────────
        positions: list[dict] = []
        total_value = 0.0
        total_cost  = 0.0

        for h in holdings_raw:
            sym    = h["ticker"].upper()
            shares = float(h.get("shares") or 0)
            cost   = float(h.get("avg_cost") or 0)
            q      = _q(sym)
            price  = q.get("price") or 0.0
            chg    = q.get("change") or 0.0
            chgpct = q.get("change_pct") or 0.0
            mval   = shares * price

            total_value += mval
            total_cost  += shares * cost

            positions.append({
                "_sym":       sym,
                "_shares":    shares,
                "_cost":      cost,
                "_atype":     asset_map.get(sym, "stock"),
                "ticker":     sym,
                "price":      _sr(price),
                "change":     _sr(chg),
                "change_pct": _sr(chgpct, 3),
                "market_val": mval,
                "w52_high":   _sr(q.get("w52_high")),
                "w52_low":    _sr(q.get("w52_low")),
            })

        for p in positions:
            p["allocation_pct"] = round(
                p["market_val"] / total_value * 100, 1
            ) if total_value else 0.0

        positions.sort(key=lambda x: x["allocation_pct"], reverse=True)

        # 4. Change today ─────────────────────────────────────────────────
        change_today = sum(p["_shares"] * (p["change"] or 0) for p in positions)
        prev_total   = total_value - change_today
        change_pct_today = round(change_today / prev_total * 100, 2) if prev_total else 0.0

        # 5. Performance chart (built after merge — uses all_history below)
        _perf_chart_deferred = True   # built after all_history is assembled

        # 6. Asset allocation ─────────────────────────────────────────────
        alloc = self._build_allocation(positions, total_value)

        # Merge all per-ticker histories into one dict
        all_history: dict[str, list[dict]] = {**tradier_history, **crypto_history, **commodity_history}

        # 5 (deferred). Performance charts (multi-period) ────────────────
        perf_charts = self._build_perf_charts(positions, all_history, spy_bars)
        perf_chart  = perf_charts.get("1Y", [])   # backward-compat field

        # 7. Correlation matrix — ALL holdings (inner-join on date) ───────
        corr = self._build_correlation(tickers, all_history)

        # 8. Risk metrics ─────────────────────────────────────────────────
        risk = self._build_risk(positions, all_history, spy_bars)

        # 9. Volatility (all holdings) ────────────────────────────────────
        vol_list = self._build_volatility(positions, all_history)

        # 10. Risk suggestions ────────────────────────────────────────────
        suggestions = self._build_suggestions(positions, alloc, risk)

        # 11. Period performance ──────────────────────────────────────────
        periods = self._build_periods(positions, all_history, change_pct_today)

        # 12. Sentiment ───────────────────────────────────────────────────
        sentiment = self._sentiment(change_pct_today)

        # 13. Top movers ──────────────────────────────────────────────────
        top_movers = self._top_movers(positions)

        # 14. Earnings calendar ───────────────────────────────────────────
        earnings_cal = self._build_earnings(R["earnings"] or [], tickers, positions)

        # 15. Ticker tape ─────────────────────────────────────────────────
        ticker_tape = R["tape"] or []

        # 16. News ticker ─────────────────────────────────────────────────
        news_ticker = self._build_news(R["news"] or [], positions)

        # 17. Total return ────────────────────────────────────────────────
        total_return_val = total_value - total_cost
        total_return_pct = round(total_return_val / total_cost * 100, 1) if total_cost else 0.0

        return {
            "portfolio": {
                "value":            round(total_value, 2),
                "change_today":     round(change_today, 2),
                "change_pct_today": change_pct_today,
                "perf_1d":          periods["perf_1d"],
                "perf_5d":          periods["perf_5d"],
                "perf_1m":          periods["perf_1m"],
                "perf_6m":          periods["perf_6m"],
                "perf_1y":          periods["perf_1y"],
                "total_return_pct": total_return_pct,
                "total_return_value": round(total_return_val, 2),
                "sentiment":        sentiment,
                "market_status":    _market_status_et(),
            },
            "positions_count":    len(positions),
            "holdings":           self._format_holdings(positions),
            "performance_chart":  perf_chart,
            "performance_charts": perf_charts,
            "asset_allocation":   alloc,
            "correlation_matrix": corr,
            "risk_metrics":       risk,
            "volatility":         vol_list,
            "risk_suggestions":   suggestions,
            "top_movers":         top_movers,
            "earnings_calendar":  earnings_cal,
            "ticker_tape":        ticker_tape,
            "news_ticker":        news_ticker,
            "as_of":              datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

    # ── Data fetchers ─────────────────────────────────────────────────────

    async def _fetch_tradier_quotes(self, syms: list[str]) -> list[dict]:
        if not syms or not self.tradier:
            return []
        try:
            return await asyncio.wait_for(self.tradier.get_quotes(syms), timeout=12.0)
        except Exception as e:
            print(f"[CAELYN] Tradier quotes error: {e}")
            return []

    async def _fetch_crypto_quotes(self, tickers: list[str]) -> dict[str, dict]:
        """Returns {TICKER: {price, change, change_pct, w52_high, w52_low}}."""
        if not tickers:
            return {}
        id_map = {COINGECKO_IDS[t]: t for t in tickers if t in COINGECKO_IDS}
        if not id_map:
            return {}
        cg = await _cg_prices(list(id_map.keys()))
        result = {}
        for cg_id, sym in id_map.items():
            d = cg.get(cg_id, {})
            price    = _sf(d.get("usd"))
            chgpct   = _sf(d.get("usd_24h_change"))
            chg      = round(price * chgpct / 100, 4) if price and chgpct else None
            result[sym] = {
                "price":      price,
                "change":     chg,
                "change_pct": _sr(chgpct, 3),
                "w52_high":   None,
                "w52_low":    None,
            }
            # Fetch 52W range via Yahoo for display
        return result

    async def _fetch_tradier_histories(
        self, syms: list[str], start: str
    ) -> dict[str, list[dict]]:
        if not syms or not self.tradier:
            return {}
        tasks = [self.tradier.get_history(sym, "daily", start) for sym in syms]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return {
            sym: (res if not isinstance(res, Exception) else [])
            for sym, res in zip(syms, results)
        }

    async def _fetch_crypto_histories(self, tickers: list[str]) -> dict[str, list[dict]]:
        """Fetch 1Y+ history for crypto via Yahoo Finance (BTC-USD, ETH-USD …)."""
        if not tickers:
            return {}
        tasks = [
            _yf_history(YAHOO_SYMBOL_MAP.get(t, f"{t}-USD"), "2y")
            for t in tickers
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return {
            sym: (res if not isinstance(res, Exception) else [])
            for sym, res in zip(tickers, results)
        }

    async def _fetch_commodity_quotes(self, tickers: list[str]) -> dict[str, dict]:
        """Fetch quotes for commodity tickers via Yahoo Finance futures symbols."""
        if not tickers:
            return {}
        tasks = [_yf_quote(COMMODITY_YAHOO_MAP[t], t) for t in tickers if t in COMMODITY_YAHOO_MAP]
        syms  = [t for t in tickers if t in COMMODITY_YAHOO_MAP]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        out: dict[str, dict] = {}
        for sym, res in zip(syms, results):
            if isinstance(res, dict) and res and res.get("price"):
                out[sym] = {
                    "price":      res.get("price"),
                    "change":     res.get("change"),
                    "change_pct": res.get("change_pct"),
                    "w52_high":   res.get("week_52_high"),
                    "w52_low":    res.get("week_52_low"),
                }
        return out

    async def _fetch_commodity_histories(self, tickers: list[str]) -> dict[str, list[dict]]:
        """Fetch 2Y daily history for commodity tickers via Yahoo Finance futures."""
        if not tickers:
            return {}
        syms  = [t for t in tickers if t in COMMODITY_YAHOO_MAP]
        tasks = [_yf_history(COMMODITY_YAHOO_MAP[t], "2y") for t in syms]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return {
            sym: (res if not isinstance(res, Exception) else [])
            for sym, res in zip(syms, results)
        }

    async def _fetch_earnings_calendar(self, holding_tickers: list[str]) -> list[dict]:
        """
        Fetch per-ticker earnings (surprises + next date) for each equity holding,
        plus a market-wide calendar scan for S&P 500 context.
        """
        if not self.finnhub:
            return []

        # Only fetch for portfolio holdings — context tickers no longer displayed
        all_tickers = list(dict.fromkeys(holding_tickers))

        results: list[dict] = []

        # Fetch per-ticker: last EPS (surprises) + next date (per-ticker calendar)
        async def _fetch_one(ticker: str):
            try:
                surprises = await asyncio.wait_for(
                    asyncio.to_thread(self.finnhub.get_earnings_surprises, ticker),
                    timeout=8.0,
                )
                last_eps = None
                if surprises and isinstance(surprises, list):
                    last_eps = surprises[0].get("actual_eps") if surprises[0] else None

                cal = await asyncio.wait_for(
                    asyncio.to_thread(self.finnhub.get_earnings_calendar, ticker),
                    timeout=8.0,
                )
                next_date = None
                est_eps   = None
                if cal and isinstance(cal, list):
                    for e in cal:
                        if e.get("date"):
                            next_date = e.get("date")
                            est_eps   = e.get("eps_estimate")
                            break

                return {
                    "ticker":   ticker,
                    "last_eps": last_eps,
                    "next_date": next_date,
                    "est_eps":  est_eps,
                }
            except Exception as e:
                print(f"[CAELYN] earnings fetch {ticker}: {e}")
                return {"ticker": ticker, "last_eps": None, "next_date": None, "est_eps": None}

        tasks = [_fetch_one(t) for t in all_tickers]
        fetched = await asyncio.gather(*tasks, return_exceptions=True)

        for item in fetched:
            if isinstance(item, dict):
                results.append(item)

        return results

    async def _fetch_news(self, tickers: list[str]) -> list[dict]:
        if not self.finnhub or not tickers:
            return []
        try:
            tasks = [asyncio.to_thread(self.finnhub.get_company_news, t, 7) for t in tickers[:4]]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            combined = []
            for sym, res in zip(tickers[:4], results):
                if isinstance(res, list):
                    for item in res:
                        item["_sym"] = sym
                        combined.append(item)
            return combined
        except Exception as e:
            print(f"[CAELYN] News fetch error: {e}")
            return []

    async def _fetch_tape(self, holding_tickers: list[str]) -> list[dict]:
        """
        Build the 10-symbol ticker tape: fixed symbols + any holding tickers
        not already in the tape (first 2 extras injected after QQQ).
        """
        # Build base tape from the fixed TAPE_SYMBOLS
        tasks = [_yf_quote(yf_sym, disp) for disp, yf_sym in TAPE_SYMBOLS]
        # Also inject holding tickers that are equities (via Tradier already fetched)
        # — those are added at _build() time if in holdings

        results = await asyncio.gather(*tasks, return_exceptions=True)

        tape = []
        for (disp, _), q in zip(TAPE_SYMBOLS, results):
            if isinstance(q, dict) and q and q.get("price"):
                tape.append({
                    "symbol":     disp,
                    "price":      _sr(q.get("price")),
                    "change_pct": _sr(q.get("change_pct"), 3),
                })

        # Inject equity holdings not already in tape (e.g. NVDA, OSS)
        tape_syms = {t["symbol"] for t in tape}
        eq_extras = [t for t in holding_tickers
                     if t not in tape_syms
                     and t not in COINGECKO_IDS][:2]

        if eq_extras and self.tradier:
            try:
                extra_quotes = await asyncio.wait_for(
                    self.tradier.get_quotes(eq_extras), timeout=8.0
                )
                for q in extra_quotes:
                    sym = q.get("symbol", "")
                    price = _sf(q.get("last"))
                    chgpct = _sf(q.get("change_percentage"))
                    if sym and price:
                        tape.insert(2, {   # inject after QQQ
                            "symbol":     sym,
                            "price":      _sr(price),
                            "change_pct": _sr(chgpct, 3),
                        })
            except Exception as e:
                print(f"[CAELYN] tape extras error: {e}")

        return tape

    # ── Builders ──────────────────────────────────────────────────────────

    def _format_holdings(self, positions: list[dict]) -> list[dict]:
        return [
            {
                "ticker":        p["ticker"],
                "price":         p["price"],
                "change":        p["change"],
                "change_pct":    p["change_pct"],
                "allocation_pct": p["allocation_pct"],
            }
            for p in positions
        ]

    def _get_closes(self, sym: str, all_history: dict) -> list[float]:
        bars = all_history.get(sym, [])
        return [b["close"] for b in bars if b.get("close")]

    def _build_perf_charts(
        self, positions: list[dict], all_history: dict, spy_bars: list
    ) -> dict[str, list[dict]]:
        """
        Build performance charts for five periods: 1D, 5D, 1M, 6M, 1Y.
        Each array is normalized to 0.0% at the first point.
        """
        spy_daily = {b["date"]: b["close"] for b in spy_bars if b.get("close")}
        all_spy_dates = sorted(spy_daily.keys())

        # Per-holding daily price maps
        price_maps: dict[str, dict[str, float]] = {}
        for p in positions:
            sym  = p["_sym"]
            bars = all_history.get(sym, [])
            price_maps[sym] = {b["date"]: b["close"] for b in bars if b.get("close")}

        def _port_at(dt_str: str) -> float:
            """Portfolio value at a given date using last-available close."""
            total = 0.0
            for p in positions:
                pm = price_maps.get(p["_sym"], {})
                px = pm.get(dt_str)
                if px is None:
                    cands = [d for d in pm if d <= dt_str]
                    px = pm[max(cands)] if cands else (p["price"] or 0)
                total += p["_shares"] * (px or 0)
            return total

        def _spy_at(dt_str: str) -> float:
            v = spy_daily.get(dt_str)
            if v:
                return v
            cands = [d for d in all_spy_dates if d <= dt_str]
            return spy_daily[max(cands)] if cands else 0.0

        def _normalize(pairs: list[tuple[str, float, float]]) -> list[dict]:
            """pairs = [(label, port_val, spy_val), ...]"""
            if not pairs or pairs[0][1] == 0:
                return []
            p0, s0 = pairs[0][1], pairs[0][2]
            result = []
            for label, pv, sv in pairs:
                result.append({
                    "date":      label,
                    "portfolio": round((pv - p0) / p0 * 100, 2) if p0 else 0.0,
                    "sp500":     round((sv - s0) / s0 * 100, 2) if s0 else 0.0,
                })
            return result

        today = date.today()
        today_str = today.isoformat()

        # ── 1D: intraday using Yahoo Finance 5-min data ─────────────────
        chart_1d = self._build_1d_chart(positions, price_maps, spy_daily)

        # ── 5D: one point per trading day, past 5 trading days ──────────
        trading_days = [d for d in all_spy_dates if d <= today_str][-6:]
        pairs_5d = []
        for d in trading_days:
            dt_obj = datetime.strptime(d, "%Y-%m-%d").date()
            label = dt_obj.strftime("%a")
            pairs_5d.append((label, _port_at(d), _spy_at(d)))
        chart_5d = _normalize(pairs_5d)

        # ── 1M: daily, sample every 3rd trading day (~10 pts) ───────────
        days_1m = [d for d in all_spy_dates if d >= (today - timedelta(days=35)).isoformat() and d <= today_str]
        if days_1m:
            step = max(1, len(days_1m) // 10)
            idxs = list(range(0, len(days_1m), step)) + ([len(days_1m) - 1] if (len(days_1m) - 1) % step != 0 else [])
            idxs = sorted(set(idxs))
            pairs_1m = []
            for i in idxs:
                d = days_1m[i]
                dt_obj = datetime.strptime(d, "%Y-%m-%d").date()
                label = dt_obj.strftime("%b %-d")
                pairs_1m.append((label, _port_at(d), _spy_at(d)))
            chart_1m = _normalize(pairs_1m)
        else:
            chart_1m = []

        # ── 6M: weekly data points (~26 pts) ───────────────────────────
        days_6m = [d for d in all_spy_dates if d >= (today - timedelta(days=186)).isoformat() and d <= today_str]
        if days_6m:
            # Sample every 5 trading days ≈ weekly
            step = max(1, len(days_6m) // 26)
            idxs = list(range(0, len(days_6m), step)) + ([len(days_6m) - 1] if (len(days_6m) - 1) % step != 0 else [])
            idxs = sorted(set(idxs))
            pairs_6m = []
            for i in idxs:
                d = days_6m[i]
                dt_obj = datetime.strptime(d, "%Y-%m-%d").date()
                label = dt_obj.strftime("%b %-d")
                pairs_6m.append((label, _port_at(d), _spy_at(d)))
            chart_6m = _normalize(pairs_6m)
        else:
            chart_6m = []

        # ── 1Y: monthly sampled (~13 pts) ──────────────────────────────
        days_1y = [d for d in all_spy_dates if d >= (today - timedelta(days=375)).isoformat() and d <= today_str]
        if days_1y:
            step = max(1, len(days_1y) // 13)
            idxs = list(range(0, len(days_1y), step)) + ([len(days_1y) - 1] if (len(days_1y) - 1) % step != 0 else [])
            idxs = sorted(set(idxs))
            pairs_1y = []
            for i in idxs:
                d = days_1y[i]
                dt_obj = datetime.strptime(d, "%Y-%m-%d").date()
                label = _month_label(dt_obj)
                pairs_1y.append((label, _port_at(d), _spy_at(d)))
            chart_1y = _normalize(pairs_1y)
        else:
            chart_1y = []

        return {"1D": chart_1d, "5D": chart_5d, "1M": chart_1m, "6M": chart_6m, "1Y": chart_1y}

    def _build_1d_chart(
        self, positions: list[dict], price_maps: dict, spy_daily: dict
    ) -> list[dict]:
        """
        1D intraday chart using Yahoo Finance 5-min data.
        Falls back to empty list if market is closed / data unavailable.
        """
        import zoneinfo
        et = datetime.now(zoneinfo.ZoneInfo("America/New_York"))
        today_str = et.strftime("%Y-%m-%d")

        # Collect all yahoo symbols for holdings
        yahoo_syms: dict[str, str] = {}  # display_sym → yahoo_sym
        for p in positions:
            sym   = p["_sym"]
            atype = p.get("_atype", "stock")
            if atype == "crypto":
                yahoo_syms[sym] = YAHOO_SYMBOL_MAP.get(sym, f"{sym}-USD")
            elif atype == "commodity":
                yahoo_syms[sym] = COMMODITY_YAHOO_MAP.get(sym, sym)
            else:
                yahoo_syms[sym] = sym
        yahoo_syms["SPY"] = "SPY"

        # Fetch intraday data synchronously (we're inside a to_thread already? No — we're async)
        # We do a quick sync fetch via urllib for each ticker
        intraday_map: dict[str, list[tuple[str, float]]] = {}
        for sym, ysym in yahoo_syms.items():
            try:
                raw = _yf_fetch_sync(ysym, "1d")
                res = raw.get("chart", {}).get("result", [{}])[0]
                meta = res.get("meta", {})
                ts   = res.get("timestamp", [])
                closes = res.get("indicators", {}).get("quote", [{}])[0].get("close", [])
                bars = []
                for i in range(min(len(ts), len(closes))):
                    c = closes[i]
                    if c is not None:
                        t_et = datetime.fromtimestamp(ts[i], tz=zoneinfo.ZoneInfo("America/New_York"))
                        if t_et.strftime("%Y-%m-%d") == today_str:
                            bars.append((t_et.strftime("%H:%M"), c))
                intraday_map[sym] = bars
            except Exception:
                intraday_map[sym] = []

        spy_bars_1d = intraday_map.get("SPY", [])
        if not spy_bars_1d:
            return []

        # Common timestamps: intersection of all holding timestamps and SPY
        spy_times = [t for t, _ in spy_bars_1d]
        holding_time_sets = []
        for p in positions:
            b = intraday_map.get(p["_sym"], [])
            if b:
                holding_time_sets.append(set(t for t, _ in b))

        if not holding_time_sets:
            return []

        common_times = sorted(
            set(spy_times).intersection(*holding_time_sets) if holding_time_sets else set(spy_times)
        )
        if not common_times:
            common_times = spy_times

        spy_time_map = {t: v for t, v in spy_bars_1d}
        holding_time_maps: dict[str, dict[str, float]] = {}
        for p in positions:
            bars_1d = intraday_map.get(p["_sym"], [])
            holding_time_maps[p["_sym"]] = {t: v for t, v in bars_1d}

        result_pairs = []
        for t_label in common_times:
            spy_v = spy_time_map.get(t_label)
            if not spy_v:
                continue
            port_v = 0.0
            for p in positions:
                hm = holding_time_maps.get(p["_sym"], {})
                px = hm.get(t_label, p.get("price") or 0)
                port_v += p["_shares"] * px
            result_pairs.append((t_label, port_v, spy_v))

        if not result_pairs or result_pairs[0][1] == 0:
            return []

        p0, s0 = result_pairs[0][1], result_pairs[0][2]
        return [
            {
                "date":      t,
                "portfolio": round((pv - p0) / p0 * 100, 3) if p0 else 0.0,
                "sp500":     round((sv - s0) / s0 * 100, 3) if s0 else 0.0,
            }
            for t, pv, sv in result_pairs
        ]

    def _build_allocation(self, positions, total_value) -> list[dict]:
        class_totals: dict[str, float] = {}
        for p in positions:
            ac = _asset_class(p["ticker"], p["_atype"])
            class_totals[ac] = class_totals.get(ac, 0) + p["market_val"]

        result = []
        for ac, val in sorted(class_totals.items(), key=lambda x: -x[1]):
            pct = round(val / total_value * 100, 1) if total_value else 0.0
            result.append({
                "label": ac,
                "pct":   pct,
                "color": ASSET_CLASS_COLORS.get(ac, ASSET_CLASS_COLORS[_OTHER]),
            })
        return result

    def _build_correlation(self, all_tickers: list[str], all_history: dict) -> dict:
        """
        Compute NxN Pearson correlation matrix for ALL holdings.
        Uses an inner-join on calendar dates so crypto/commodity/equity
        date mismatches are handled correctly.
        """
        returns_map: dict[str, dict[str, float]] = {}
        for t in all_tickers:
            bars = all_history.get(t, [])
            closes = [(b["date"], b["close"]) for b in bars if b.get("close")]
            if len(closes) < 15:
                continue
            rets: dict[str, float] = {}
            for i in range(1, len(closes)):
                d, c = closes[i]
                prev = closes[i - 1][1]
                if prev and prev != 0:
                    rets[d] = (c - prev) / prev
            if len(rets) >= 15:
                returns_map[t] = rets

        valid = [t for t in all_tickers if t in returns_map]
        n = len(valid)
        if n == 0:
            return {"tickers": [], "values": []}

        # Inner-join: only dates where ALL tickers have data
        date_sets = [set(returns_map[t].keys()) for t in valid]
        common_dates = sorted(set.intersection(*date_sets)) if len(date_sets) > 1 else sorted(date_sets[0])

        if len(common_dates) < 10:
            # Fallback: use the most recent 60 days from each ticker independently
            return {"tickers": valid, "values": [[1.0 if i == j else 0.0
                    for j in range(n)] for i in range(n)]}

        vecs = {t: [returns_map[t][d] for d in common_dates] for t in valid}

        mat = []
        for i, ti in enumerate(valid):
            row = []
            for j, tj in enumerate(valid):
                if i == j:
                    row.append(1.0)
                elif j < i:
                    row.append(mat[j][i])
                else:
                    c = _correlation(vecs[ti], vecs[tj])
                    row.append(c if c is not None else 0.0)
            mat.append(row)

        return {"tickers": valid, "values": mat}

    def _build_risk(self, positions, all_history: dict, spy_bars: list) -> dict:
        spy_closes = [b["close"] for b in spy_bars if b.get("close")]
        spy_rets   = _returns(spy_closes)
        spy_std    = _std(spy_rets)
        spy_date_idx = {b["date"]: i for i, b in enumerate(spy_bars)}

        weighted_vol  = 0.0
        weighted_beta = 0.0
        all_port_rets: dict[str, float] = {}

        for p in positions:
            sym   = p["_sym"]
            w     = p["allocation_pct"] / 100
            closes = self._get_closes(sym, all_history)
            if len(closes) < 20:
                continue

            vol = _annualized_vol(closes) or 0.0
            weighted_vol += w * vol

            bars = all_history.get(sym, [])
            rets_map: dict[str, float] = {}
            for i in range(1, len(bars)):
                if bars[i].get("close") and bars[i-1].get("close") and bars[i-1]["close"]:
                    rets_map[bars[i]["date"]] = (bars[i]["close"] - bars[i-1]["close"]) / bars[i-1]["close"]

            for d, r in rets_map.items():
                all_port_rets[d] = all_port_rets.get(d, 0) + w * r

            # Beta computation (equity/ETF tickers whose dates align with SPY)
            if spy_std and spy_std > 0 and sym in all_history:
                common_r, common_spy = [], []
                for d, r in rets_map.items():
                    si = spy_date_idx.get(d)
                    if si and si > 0:
                        sc = spy_bars[si].get("close")
                        sp = spy_bars[si - 1].get("close")
                        if sc and sp and sp:
                            common_r.append(r)
                            common_spy.append((sc - sp) / sp)
                if len(common_r) >= 20:
                    c = _correlation(common_r, common_spy)
                    sr = _std(common_r)
                    ss = _std(common_spy)
                    if c and ss > 0:
                        weighted_beta += w * (c * sr / ss)

        port_rets_list = [all_port_rets[d] for d in sorted(all_port_rets)]
        port_vol = _std(port_rets_list) * math.sqrt(252) * 100 if port_rets_list else weighted_vol
        ann_ret  = (sum(all_port_rets.values()) / len(all_port_rets) * 252) if all_port_rets else 0.0

        rf = 0.0525  # 5.25% risk-free rate
        sharpe  = round((ann_ret - rf) / (port_vol / 100), 2) if port_vol else None
        neg_ret = [r for r in port_rets_list if r < 0]
        down_std = _std(neg_ret) * math.sqrt(252) * 100 if neg_ret else port_vol
        sortino = round((ann_ret - rf) / (down_std / 100), 2) if down_std else None

        sorted_dates = sorted(all_port_rets.keys())
        port_val_series = [100.0]
        v = 100.0
        for d in sorted_dates:
            v *= (1 + all_port_rets[d])
            port_val_series.append(v)
        max_dd = _max_drawdown(port_val_series)

        top_pos = max(positions, key=lambda x: x["allocation_pct"], default=None)
        top_conc = int(round(top_pos["allocation_pct"])) if top_pos else 0
        top_conc_label = top_pos["ticker"] if top_pos else ""

        return {
            "weighted_volatility":  round(weighted_vol, 1),
            "max_drawdown":         max_dd,
            "top_concentration":    top_conc,
            "top_concentration_label": top_conc_label,
            "portfolio_beta":       round(weighted_beta, 2) if weighted_beta else None,
            "sharpe_ratio":         sharpe,
            "sortino_ratio":        sortino,
        }

    def _build_volatility(self, positions, all_history: dict) -> list[dict]:
        vols = []
        for p in positions:
            closes = self._get_closes(p["_sym"], all_history)
            v = _annualized_vol(closes)
            if v is not None:
                vols.append({"ticker": p["ticker"], "vol": v})
        return sorted(vols, key=lambda x: -x["vol"])

    def _build_suggestions(self, positions, alloc, risk) -> list[dict]:
        """
        Generate 2-4 portfolio-specific risk suggestions based on actual allocation.
        Rules applied in priority order; only triggered rules are returned.
        """
        suggestions: list[dict] = []
        alloc_map = {a["label"]: a["pct"] for a in alloc}
        ticker_alloc = {p["ticker"]: p["allocation_pct"] for p in positions}
        vol_map = {p["ticker"]: None for p in positions}

        # ── Rule 1: Single-position concentration risk (>40%) ───────────
        for p in positions:
            pct = p["allocation_pct"]
            if pct >= 40:
                dd_impact = round(pct * 0.2)
                suggestions.append({
                    "level": "RISK",
                    "title": f"High Concentration in {p['ticker']}",
                    "body": (
                        f"{p['ticker']} represents {pct:.0f}% of total portfolio value. "
                        f"A 20% drawdown in {p['ticker']} alone would reduce your total portfolio "
                        f"by ~{dd_impact}%. Consider trimming to below 30%, or hedging with a "
                        f"covered call on the position."
                    ),
                })
                break   # Only flag the top one to avoid repetition

        # ── Rule 2: Tech/AI sector overexposure ─────────────────────────
        nvda_pct  = ticker_alloc.get("NVDA", 0)
        buzz_pct  = ticker_alloc.get("BUZZ", 0)
        ai_combined = nvda_pct + buzz_pct
        if ai_combined > 60:
            suggestions.append({
                "level": "RISK",
                "title": "AI Sector Overexposure",
                "body": (
                    f"NVDA ({nvda_pct:.0f}%) and BUZZ ({buzz_pct:.0f}%) together represent "
                    f"{ai_combined:.0f}% of the portfolio. BUZZ holds AI-heavy equities that "
                    f"significantly overlap with NVDA's sector, doubling your concentration "
                    f"in semiconductors/AI during a sell-off. Consider diversifying into "
                    f"non-correlated sectors."
                ),
            })

        # ── Rule 3: No defensive allocation (0% fixed income) ───────────
        fi_pct = alloc_map.get(_FIXED, 0)
        if fi_pct == 0 and len(suggestions) < 4:
            suggestions.append({
                "level": "WARN",
                "title": "No Defensive Allocation",
                "body": (
                    "The portfolio is 100% risk-on with no fixed income buffer. "
                    "Given the growth/AI tilt (NVDA, BUZZ) and crypto exposure (BTC), "
                    "a 5-10% allocation to TLT (long-duration Treasuries) can act as a "
                    "flight-to-quality hedge during equity drawdowns — not as a core holding, "
                    "but as a volatility dampener."
                ),
            })

        # ── Rule 4: Crypto tail risk (BTC > 3%) ─────────────────────────
        btc_pct = ticker_alloc.get("BTC", 0)
        btc_vol = risk.get("weighted_volatility")
        if btc_pct > 3 and len(suggestions) < 4:
            wv = btc_vol or 0
            contrib_est = round(btc_pct / 100 * wv, 1)
            suggestions.append({
                "level": "WARN",
                "title": "Crypto Tail Risk",
                "body": (
                    f"BTC (~{btc_pct:.0f}% allocation) has historically drawn down 50%+ in "
                    f"risk-off regimes. At current sizing it contributes an estimated "
                    f"~{contrib_est:.1f}% to portfolio volatility. Consider sizing down if VIX "
                    f"spikes above 25 or DXY strengthens — both signal crypto headwinds."
                ),
            })

        # ── Rule 5: Small-cap liquidity risk (OSS > 2%) ─────────────────
        oss_pct = ticker_alloc.get("OSS", 0)
        if oss_pct > 2 and len(suggestions) < 4:
            suggestions.append({
                "level": "INFO",
                "title": "Small Cap Liquidity Risk",
                "body": (
                    f"OSS is a micro-cap with thin daily volume (~{oss_pct:.0f}% of portfolio). "
                    f"In volatile markets, exits can move the stock against you significantly. "
                    f"Use limit orders and plan position sizing accordingly. Avoid market orders."
                ),
            })

        return suggestions[:5]

    def _build_periods(self, positions, all_history: dict, change_pct_1d) -> dict:
        today = date.today().isoformat()

        def _days_ago(n): return (date.today() - timedelta(days=n)).isoformat()

        def _port_value_at(target: str) -> float:
            total = 0.0
            for p in positions:
                bars = all_history.get(p["_sym"], [])
                eligible = [b for b in bars if b.get("date", "") <= target and b.get("close")]
                px = eligible[-1]["close"] if eligible else (p["price"] or 0)
                total += p["_shares"] * px
            return total

        cur = _port_value_at(today)

        def _perf(days):
            past = _port_value_at(_days_ago(days))
            return round((cur - past) / past * 100, 1) if past else None

        return {
            "perf_1d": round(change_pct_1d, 1),
            "perf_5d": _perf(5),
            "perf_1m": _perf(30),
            "perf_6m": _perf(182),
            "perf_1y": _perf(365),
        }

    def _sentiment(self, change_pct: float) -> str:
        if change_pct > 0.4:
            return "BULLISH"
        if change_pct < -0.4:
            return "BEARISH"
        if abs(change_pct) <= 0.1:
            return "NEUTRAL"
        return "UNCERTAIN"

    def _top_movers(self, positions) -> dict:
        sorted_pos = sorted(
            [p for p in positions if p.get("change_pct") is not None],
            key=lambda x: x["change_pct"],
        )

        def _fmt(p):
            return {
                "ticker":     p["ticker"],
                "change_pct": p["change_pct"],
                "price":      p["price"],
                "w52_low":    p.get("w52_low"),
                "w52_high":   p.get("w52_high"),
            }

        all_gainers = [p for p in sorted_pos if (p.get("change_pct") or 0) > 0]
        all_losers  = [p for p in sorted_pos if (p.get("change_pct") or 0) < 0]
        gainers = [_fmt(p) for p in sorted(all_gainers, key=lambda x: -x["change_pct"])[:2]]
        losers  = [_fmt(p) for p in sorted(all_losers,  key=lambda x: x["change_pct"])[:2]]

        # Fall back to best/worst if no strict gainers/losers
        if not gainers and sorted_pos:
            gainers = [_fmt(sorted_pos[-1])]
        if not losers and sorted_pos:
            losers = [_fmt(sorted_pos[0])]

        return {"gainers": gainers, "losers": losers}

    def _build_earnings(self, raw: list, holding_tickers: list, positions: list) -> list[dict]:
        """
        Build earnings calendar entries.
        - Equity holdings (stock/etf asset_type): show EPS, next date, WTD change
        - ETFs: show WTD change only, last_eps/est_eps = null, next_date = "N/A"
        - Crypto/commodity (BTC, GOLD): skip entirely
        - S&P 500 context companies: include if present in raw
        """
        holding_set = set(t.upper() for t in holding_tickers)
        # Skip non-equity holdings from earnings (crypto and commodity)
        skip_types = {"crypto", "commodity"}
        skip_tickers = set()
        for p in positions:
            if p.get("_atype", "stock") in skip_types:
                skip_tickers.add(p["ticker"].upper())

        # Map ticker → position for WTD calculation
        pos_map = {p["ticker"].upper(): p for p in positions}

        def _fmt_date(dt_str: str) -> str:
            if not dt_str:
                return "N/A"
            try:
                dt = datetime.strptime(dt_str, "%Y-%m-%d")
                return dt.strftime("%b %-d")
            except Exception:
                return dt_str or "N/A"

        def _wtd(ticker: str) -> str | None:
            p = pos_map.get(ticker)
            if not p:
                return None
            chg = p.get("change_pct")
            return f"{chg:+.2f}%" if chg is not None else None

        results = []
        seen: set[str] = set()

        # Holdings first (in portfolio order)
        for ticker in holding_tickers:
            t = ticker.upper()
            if t in skip_tickers or t in seen:
                continue
            seen.add(t)

            pos = pos_map.get(t)
            is_etf = pos and pos.get("_atype") == "etf"

            # Find this ticker in raw data
            raw_entry = next((e for e in raw if (e.get("ticker") or "").upper() == t), {})

            if is_etf:
                results.append({
                    "ticker":       t,
                    "company":      t,
                    "in_portfolio": True,
                    "next_date":    "N/A",
                    "est_eps":      None,
                    "last_eps":     None,
                    "wtd":          _wtd(t),
                })
            else:
                next_dt = raw_entry.get("next_date")
                results.append({
                    "ticker":       t,
                    "company":      t,
                    "in_portfolio": True,
                    "next_date":    _fmt_date(next_dt),
                    "est_eps":      raw_entry.get("est_eps"),
                    "last_eps":     raw_entry.get("last_eps"),
                    "wtd":          _wtd(t),
                })

        return results

    def _build_news(self, raw: list, positions: list) -> list[dict]:
        news = []
        seen: set[str] = set()

        def _ts(item):
            try: return int(item.get("datetime") or 0)
            except Exception: return 0

        for item in sorted(raw, key=_ts, reverse=True):
            sym   = (item.get("_sym") or "").upper()
            title = item.get("title", "")
            ts    = item.get("datetime")
            if not title or title in seen:
                continue
            seen.add(title)

            time_ago = ""
            if ts:
                try:
                    dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
                    delta = datetime.now(timezone.utc) - dt
                    mins = int(delta.total_seconds() / 60)
                    if mins < 60:
                        time_ago = f"{mins}m ago"
                    elif mins < 1440:
                        time_ago = f"{mins // 60}h ago"
                    else:
                        time_ago = f"{mins // 1440}d ago"
                except Exception:
                    pass

            news.append({"symbol": sym, "headline": title, "time_ago": time_ago})
            if len(news) >= 8:
                break
        return news

    # ── Holdings loader ───────────────────────────────────────────────────

    def _load_holdings(self, portfolio_file: Path) -> list[dict]:
        candidates = [portfolio_file, Path("data/portfolio_holdings.json")]
        for path in candidates:
            try:
                if not path.exists():
                    continue
                with open(path) as f:
                    data = json.load(f)
                holdings = data.get("holdings", []) if isinstance(data, dict) else []
                result = [
                    h for h in holdings
                    if isinstance(h, dict)
                    and h.get("ticker")
                    and float(h.get("shares") or 0) > 0
                ]
                if result:
                    return result
            except Exception as e:
                print(f"[CAELYN] Holdings load error ({path}): {e}")
        return []

    def _empty(self) -> dict:
        return {
            "portfolio": {
                "value": 0, "change_today": 0, "change_pct_today": 0,
                "perf_1d": None, "perf_5d": None, "perf_1m": None,
                "perf_6m": None, "perf_1y": None,
                "total_return_pct": 0, "total_return_value": 0,
                "sentiment": "NEUTRAL", "market_status": _market_status_et(),
            },
            "positions_count": 0, "holdings": [],
            "performance_chart": [], "asset_allocation": [],
            "correlation_matrix": {"tickers": [], "values": []},
            "risk_metrics": {
                "weighted_volatility": None, "max_drawdown": None,
                "top_concentration": 0, "top_concentration_label": "",
                "portfolio_beta": None, "sharpe_ratio": None, "sortino_ratio": None,
            },
            "volatility": [], "risk_suggestions": [],
            "top_movers": {"gainers": [], "losers": []},
            "earnings_calendar": [], "ticker_tape": [], "news_ticker": [],
            "as_of": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
