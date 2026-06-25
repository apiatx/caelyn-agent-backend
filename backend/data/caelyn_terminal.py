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

def _yf_fetch_sync(symbol: str, range_: str = "5d", interval: str = "1d") -> dict:
    url = f"{_YF_CHART.format(symbol=symbol)}?interval={interval}&range={range_}"
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
        # Volume fields — YF meta includes both today's volume and 3-month average
        volume     = _sf(meta.get("regularMarketVolume"))
        avg_volume = _sf(meta.get("averageDailyVolume3Month") or meta.get("averageDailyVolume10Day"))
        return {
            "symbol": sym, "price": price, "change": chg, "change_pct": chgpct,
            "prev_close": prev_close, "week_52_high": w52h, "week_52_low": w52l,
            "volume": volume, "avg_volume": avg_volume,
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

    @staticmethod
    def cache_key_for(portfolio_file: Path) -> str:
        """Return the terminal cache key for a given portfolio file.

        Keyed by the resolved file path so each user's Terminal has its own
        cache slot — mutations in one user's Dashboard never serve stale data
        to another user, and `save_holdings` can target exactly the right key.
        """
        return f"caelyn:terminal:v9:{portfolio_file.resolve()}"

    @staticmethod
    def _holdings_sig(holdings: list[dict]) -> str:
        """Stable hash of (ticker, shares, avg_cost) so stale cached payloads
        from a prior holdings state are never returned — even if the file-path
        key was not explicitly invalidated (e.g. manual file edits, restarts)."""
        import hashlib
        parts = sorted(
            (
                str(h.get("ticker", "")).upper(),
                str(h.get("shares", 0)),
                str(h.get("avg_cost", 0)),
            )
            for h in holdings
            if h.get("ticker")
        )
        return hashlib.md5(json.dumps(parts).encode()).hexdigest()[:16]
    async def get(self, portfolio_file: Path) -> dict:
        cache_key = self.cache_key_for(portfolio_file)
        cached = cache.get(cache_key)
        if cached is not None:
            # Validate that the cached payload was built from the CURRENT holdings.
            # This catches file edits / restarts that bypass the save_holdings
            # invalidation path.
            current_holdings = self._load_holdings(portfolio_file)
            current_sig = self._holdings_sig(current_holdings)
            if cached.get("_holdings_sig") == current_sig:
                print(
                    f"[TERMINAL] cache=HIT  file={portfolio_file.name}  "
                    f"sig={current_sig}"
                )
                return cached
            # Signature mismatch — holdings changed since this was cached.
            print(
                f"[TERMINAL] cache=STALE  file={portfolio_file.name}  "
                f"cached_sig={cached.get('_holdings_sig')}  "
                f"current_sig={current_sig}  — rebuilding"
            )
            cache.delete(cache_key)

        # Cache miss — load from disk and build
        holdings_raw_check = self._load_holdings(portfolio_file)
        print(
            f"[TERMINAL] cache=MISS  file={portfolio_file.name}  "
            f"holdings_on_disk={len(holdings_raw_check)}  "
            f"symbols={[h.get('ticker') for h in holdings_raw_check[:20]]}  "
            f"source={'user_file' if portfolio_file.exists() else 'legacy_fallback'}"
        )

        result = await self._build(portfolio_file)
        cache.set(cache_key, result, 300)
        return result

    async def _build(self, portfolio_file: Path) -> dict:
        # 1. Load holdings ────────────────────────────────────────────────
        holdings_raw = self._load_holdings(portfolio_file)
        if not holdings_raw:
            return self._empty()

        tickers   = [h["ticker"].upper() for h in holdings_raw]
        asset_map = {h["ticker"].upper(): (h.get("asset_type") or "stock").lower()
                     for h in holdings_raw}

        # 1c. Load open option positions and extract extra underlying symbols
        # These are included in quote/fundamentals/history fetches so the terminal
        # can display exposure for option underlyings alongside equity holdings.
        # OCC contract IDs are never passed into lookup paths — only underlying tickers.
        _opt_positions_raw: list[dict] = []
        _opt_underlyings_extra: list[str] = []
        _OPEN_STATUS_TERM = {"open", "partially_closed_open", "short_option_tracked_basic"}
        try:
            from data.option_trades_store import load_option_positions as _lop_term
            _all_opt_term = _lop_term()
            _opt_positions_raw = [p for p in _all_opt_term if p.get("final_status") in _OPEN_STATUS_TERM]
            _existing_eq = set(tickers)
            _opt_underlyings_extra = sorted({
                (p.get("underlying") or "").upper().strip()
                for p in _opt_positions_raw
                if (p.get("underlying") or "").strip()
                and (p.get("underlying") or "").upper().strip() not in _existing_eq
            })
            if _opt_underlyings_extra:
                print(f"[CAELYN] option underlying extras added to terminal: {_opt_underlyings_extra}")
        except Exception as _opt_term_err:
            print(f"[CAELYN] option positions load error (non-fatal): {_opt_term_err}")

        # 1b. Load closed trades early so their tickers can be included in
        #     the history fetch — this gives accurate mark-to-market during
        #     the holding window instead of always falling back to exit_price.
        try:
            from data.closed_trades_store import load_closed_trades as _load_ct
            _closed_trades_raw = _load_ct()
        except Exception as _ct_load_err:
            _closed_trades_raw = []
            print(f"[CAELYN] closed_trades early load error (non-fatal): {_ct_load_err}")

        # Unique closed-trade tickers that are NOT already active holdings
        _ct_extra_tickers = sorted({
            (ct.get("ticker") or "").upper()
            for ct in _closed_trades_raw
            if (ct.get("ticker") or "").upper() and
               (ct.get("ticker") or "").upper() not in tickers
        })

        # Classify tickers by type
        equity_tickers  = [t for t in tickers if asset_map[t] in ("stock","etf","")]
        # Append option underlying extras — they are equity lookups, deduped vs existing
        equity_tickers  = list(dict.fromkeys(equity_tickers + _opt_underlyings_extra))
        crypto_tickers  = [t for t in tickers if asset_map[t] == "crypto"]
        all_commodity   = [t for t in tickers if asset_map[t] == "commodity"]
        # Commodities with a futures yahoo symbol → Yahoo Finance
        # Commodities without one (unknown) → Tradier as equity fallback
        yf_commodity    = [t for t in all_commodity if t in COMMODITY_YAHOO_MAP]
        tradier_commodity = [t for t in all_commodity if t not in COMMODITY_YAHOO_MAP]
        # Include extra closed-trade tickers in the tradier history fetch
        # (treated as equity; OTC/YF fallback handles anything Tradier misses)
        tradier_tickers = equity_tickers + tradier_commodity + _ct_extra_tickers
        if _ct_extra_tickers:
            print(f"[CAELYN] closed_trade extra tickers added to history fetch: {_ct_extra_tickers}")

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
            "theme_mapping":     self._fetch_theme_mapping(tickers),
            "fundamentals":      self._fetch_fundamentals(equity_tickers),
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
        theme_mapping_raw  = R["theme_mapping"] or {}   # {ticker: {primary_theme, theme_id, classification, parent_sector, source}}
        fundamentals_raw   = R["fundamentals"] or {}    # {ticker: {name, sector, ...}}

        # ── Yahoo Finance fallback for tickers Tradier didn't cover ───────────
        # Handles OTC/pink-sheet/international tickers that Tradier rejects.
        _yf_fb_q: dict[str, dict] = {}
        _yf_fb_h: dict[str, list[dict]] = {}
        _tdr_miss_q = [t for t in tradier_tickers if t not in tradier_quotes]
        _tdr_miss_h = [t for t in tradier_tickers
                       if not tradier_history.get(t) or len(tradier_history[t]) < 5]
        _yf_miss_all = sorted(set(_tdr_miss_q) | set(_tdr_miss_h))
        if _yf_miss_all:
            print(f"[CAELYN] YF fallback needed  miss_quotes={_tdr_miss_q}  miss_hist={_tdr_miss_h}")
            _yf_tasks: list = []
            _yf_keys:  list = []
            for _t in _yf_miss_all:
                _ys = YAHOO_SYMBOL_MAP.get(_t, _t)
                if _t in _tdr_miss_q:
                    _yf_tasks.append(_yf_quote(_ys, _t))
                    _yf_keys.append(("q", _t))
                if _t in _tdr_miss_h:
                    _yf_tasks.append(_yf_history(_ys, "2y"))
                    _yf_keys.append(("h", _t))
            _yf_res = await asyncio.gather(*_yf_tasks, return_exceptions=True)
            for (_kind, _t), _v in zip(_yf_keys, _yf_res):
                if isinstance(_v, Exception) or _v is None:
                    print(f"[CAELYN] YF fallback {_kind} {_t}: {type(_v).__name__ if isinstance(_v, Exception) else 'None'}")
                    continue
                if _kind == "q" and isinstance(_v, dict) and _v.get("price"):
                    _yf_fb_q[_t] = {
                        "price":      _v.get("price"),
                        "change":     _v.get("change"),
                        "change_pct": _v.get("change_pct"),
                        "w52_high":   _v.get("week_52_high"),
                        "w52_low":    _v.get("week_52_low"),
                        "volume":     _v.get("volume"),
                        "avg_volume": _v.get("avg_volume"),
                    }
                    print(f"[CAELYN] YF fallback quote OK  {_t}  price={_v.get('price')}  vol={_v.get('volume')}  avg_vol={_v.get('avg_volume')}")
                elif _kind == "h" and isinstance(_v, list) and len(_v) >= 5:
                    _yf_fb_h[_t] = _v
                    print(f"[CAELYN] YF fallback history OK  {_t}  bars={len(_v)}")

        # Merge all quotes and history (Tradier → YF fallback → crypto → commodity)
        def _q(sym: str) -> dict:
            if sym in tradier_quotes:
                q = tradier_quotes[sym]
                return {
                    "price":      _sf(q.get("last")),
                    "change":     _sf(q.get("change")),
                    "change_pct": _sf(q.get("change_percentage")),
                    "w52_high":   _sf(q.get("week_52_high")),
                    "w52_low":    _sf(q.get("week_52_low")),
                    "volume":     q.get("volume"),
                    "avg_volume": q.get("average_volume"),
                }
            if sym in _yf_fb_q:
                return _yf_fb_q[sym]
            if sym in crypto_quotes:
                return crypto_quotes[sym]
            if sym in commodity_quotes:
                return commodity_quotes[sym]
            return {}

        def _hist(sym: str) -> list[dict]:
            if sym in tradier_history and len(tradier_history[sym]) >= 5:
                return tradier_history[sym]
            if sym in _yf_fb_h:
                return _yf_fb_h[sym]
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
            sym        = h["ticker"].upper()
            shares     = float(h.get("shares") or 0)
            cost       = float(h.get("avg_cost") or 0)
            entry_date = h.get("entry_date") or h.get("date_added") or None
            q          = _q(sym)
            price      = q.get("price") or 0.0
            chg    = q.get("change") or 0.0
            chgpct = q.get("change_pct") or 0.0
            mval   = shares * price

            total_value += mval
            total_cost  += shares * cost

            funda = fundamentals_raw.get(sym, {})
            _vol     = q.get("volume") or None   # normalize 0 → None
            _avg_vol = q.get("avg_volume") or None  # normalize 0 → None

            # ── avg_volume source tracking + FMP volAvg fallback ─────────
            _vol_src     = ("tradier" if sym in tradier_quotes
                            else "yf_fallback" if sym in _yf_fb_q
                            else "crypto"      if sym in crypto_quotes
                            else "commodity"   if sym in commodity_quotes
                            else "unavailable")
            _avg_vol_src: str | None = None
            if _avg_vol:
                _avg_vol_src = _vol_src   # same provider gave avg_volume
            else:
                # Fallback: FMP profile volAvg (already fetched, zero cost)
                _fmp_avg = _sf(funda.get("avg_volume"))
                if _fmp_avg and _fmp_avg > 0:
                    _avg_vol     = _fmp_avg
                    _avg_vol_src = "fmp_profile"

            _vol_x: float | None = round(_vol / _avg_vol, 2) if (_vol and _avg_vol and _avg_vol > 0) else None

            # ── VolX unavailable reason ───────────────────────────────────
            _vol_x_reason: str | None = None
            if _vol_x is None:
                if not _vol:
                    _vol_x_reason = "volume_unavailable"
                elif not _avg_vol:
                    _vol_x_reason = ("avg_volume_unavailable_otc"
                                     if _vol_src in ("yf_fallback", "unavailable")
                                     else "avg_volume_unavailable")
                else:
                    _vol_x_reason = "calculation_error"

            # ── Vol/MC computation ────────────────────────────────────────
            _mktcap = _sf(funda.get("market_cap"))
            # Prefer dollar-volume ($ traded today) over raw share count
            _dollar_vol = round(price * _vol, 2) if (price and _vol) else None
            _vol_mc_ratio: float | None = None
            _vol_mc_pct:   float | None = None
            _vol_mc_label: str   | None = None
            _vol_mc_unavail: str | None = None
            if _dollar_vol is not None and _mktcap and _mktcap > 0:
                _vol_mc_ratio = round(_dollar_vol / _mktcap, 6)
                _vol_mc_pct   = round(_vol_mc_ratio * 100, 4)
                if _vol_mc_pct >= 10:
                    _vol_mc_label = "high"
                elif _vol_mc_pct >= 5:
                    _vol_mc_label = "elevated"
                elif _vol_mc_pct >= 1:
                    _vol_mc_label = "normal"
                else:
                    _vol_mc_label = "low"
            else:
                if not _vol:
                    _vol_mc_unavail = "volume_unavailable"
                elif not _mktcap:
                    _vol_mc_unavail = "market_cap_unavailable"
                else:
                    _vol_mc_unavail = "price_missing"

            positions.append({
                "_sym":        sym,
                "_shares":     shares,
                "_cost":       cost,
                "_entry_date": entry_date,
                "_atype":      asset_map.get(sym, "stock"),
                "_sector":    funda.get("sector", ""),
                "_name":      funda.get("name", sym),
                "ticker":     sym,
                "price":      _sr(price),
                "change":     _sr(chg),
                "change_pct": _sr(chgpct, 3),
                "market_val": mval,
                "w52_high":   _sr(q.get("w52_high")),
                "w52_low":    _sr(q.get("w52_low")),
                "volume":     _vol,
                "avg_volume": _avg_vol,
                "vol_x":      _vol_x,
                # VolX provenance
                "_vol_src":      _vol_src,
                "_avg_vol_src":  _avg_vol_src,
                "_vol_x_reason": _vol_x_reason,
                # Vol/MC fields (passed through to _format_holdings)
                "_market_cap":       _mktcap,
                "_dollar_volume":    _dollar_vol,
                "_vol_mc_ratio":     _vol_mc_ratio,
                "_vol_mc_pct":       _vol_mc_pct,
                "_vol_mc_label":     _vol_mc_label,
                "_vol_mc_unavail":   _vol_mc_unavail,
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
        asset_class_alloc = self._build_asset_class_allocation(positions, total_value)

        # Merge all per-ticker histories into one dict.
        # IMPORTANT: _yf_fb_h MUST be included here — it holds history for OTC /
        # Tradier-miss tickers fetched via Yahoo Finance fallback.  Without it,
        # analytics sections (correlation, volatility, risk, perf charts) see those
        # tickers as having zero history even though the data was fetched successfully.
        all_history: dict[str, list[dict]] = {
            **tradier_history,
            **_yf_fb_h,          # ← Yahoo Finance fallback histories (OTC / Tradier misses)
            **crypto_history,
            **commodity_history,
        }
        _hist_coverage = {t: len(all_history.get(t, [])) for t in tickers}
        print(
            f"[TERMINAL] all_history built  tickers={len(tickers)}  "
            f"coverage={_hist_coverage}  "
            f"tradier_miss_h={_tdr_miss_h}  yf_fb_h_tickers={list(_yf_fb_h.keys())}"
        )

        # 5 (deferred). Performance charts (multi-period) ────────────────
        # Reuse the closed trades already loaded in step 1b — no second DB round trip.
        perf_charts = self._build_perf_charts(positions, all_history, spy_bars, _closed_trades_raw)
        perf_chart  = perf_charts.get("1Y", [])   # backward-compat field

        _lookback_days = 420  # calendar days of history requested for every ticker

        # 7. Correlation matrix — ALL holdings (inner-join on date) ───────
        corr = self._build_correlation(
            tickers, all_history,
            lookback_days=_lookback_days,
            theme_mapping=theme_mapping_raw,
            fundamentals=fundamentals_raw,
        )

        # 8. Risk metrics ─────────────────────────────────────────────────
        risk = self._build_risk(positions, all_history, spy_bars)

        # 9. Volatility (all holdings) ────────────────────────────────────
        vol_list, vol_unavailable = self._build_volatility(positions, all_history, lookback_days=_lookback_days)

        # 10. Risk suggestions ────────────────────────────────────────────
        suggestions = self._build_suggestions(positions, alloc, risk, theme_mapping_raw)

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

        # 18. Build theme mapping list for response ───────────────────────
        theme_mapping_list = self._build_theme_mapping_list(positions, theme_mapping_raw)

        # 18b. Sector allocation — from FMP sector data when available ────
        _sector_totals: dict[str, float] = {}
        for _p in positions:
            _sec = (_p.get("_sector") or "").strip()
            if _sec:
                _sector_totals[_sec] = _sector_totals.get(_sec, 0) + _p["market_val"]
        sector_allocation: list[dict] = []
        if _sector_totals and total_value:
            for _sec, _val in sorted(_sector_totals.items(), key=lambda x: -x[1]):
                sector_allocation.append({
                    "label": _sec,
                    "pct":   round(_val / total_value * 100, 1),
                })
        # If no FMP sector data yet, fall back to theme-based grouping
        if not sector_allocation:
            _theme_totals: dict[str, float] = {}
            for _p in positions:
                _td = theme_mapping_raw.get(_p["_sym"]) or {}
                _t = _td.get("primary_theme") or _asset_class(_p["_sym"], _p.get("_atype", "stock"))
                _theme_totals[_t] = _theme_totals.get(_t, 0) + _p["market_val"]
            for _t, _val in sorted(_theme_totals.items(), key=lambda x: -x[1]):
                sector_allocation.append({
                    "label": _t,
                    "pct":   round(_val / total_value * 100, 1) if total_value else 0.0,
                })

        # 18c. Theme allocation — thematic universe as primary grouping ────
        theme_allocation = self._build_theme_allocation(positions, total_value, theme_mapping_raw)

        # 19b. Options status — portfolio-scoped scan reusing Options Screener logic
        _holdings_sig_early = self._holdings_sig(holdings_raw)
        # Options cache key must reflect the FULL portfolio universe (equity +
        # option underlyings).  Using only holdings_raw would give a stale cache
        # key after new option positions are added, causing the old equity-only
        # scan result to be served.  A separate sig from equity_tickers forces a
        # fresh scan whenever the full symbol universe changes.
        import hashlib as _hs
        _opts_sig = _hs.md5(json.dumps(sorted(equity_tickers)).encode()).hexdigest()[:16]

        # Build underlying → positions map so scan_portfolio_options can tag
        # open-position underlyings and use contract metadata as a fallback
        # when the chain scan fails. _opt_positions_raw was loaded in step 1c.
        _open_opt_by_underlying: dict[str, list[dict]] = {}
        for _op in _opt_positions_raw:
            _ul = (_op.get("underlying") or "").upper().strip()
            if _ul:
                _open_opt_by_underlying.setdefault(_ul, []).append(_op)

        _opts_scan = await self._fetch_options_status(
            equity_tickers,
            _opts_sig,
            open_option_positions=_open_opt_by_underlying if _open_opt_by_underlying else None,
        )
        _options_by_ticker  = _opts_scan.get("by_symbol", {})
        _opts_cache_hit     = _opts_scan.get("cache_hit", False)
        for _p in positions:
            _p["_options_status"] = _options_by_ticker.get(_p["_sym"])

        # 19. Performance chart metadata (per-period transparency) ─────────
        _chart_build_meta = perf_charts.pop("_meta", {})
        _perf_meta: dict[str, dict] = {}
        for _p in ("1D", "5D", "1M", "6M", "1Y"):
            _pts = perf_charts.get(_p, [])
            _perf_meta[_p] = {
                "points":                      len(_pts),
                "source":                      "tradier_daily_history",
                "estimated":                   _chart_build_meta.get("estimated", True),
                "method":                      _chart_build_meta.get("method", "current_holdings_assumed"),
                "entry_dates_set":             _chart_build_meta.get("entry_dates_set", 0),
                "active_trades_used":          _chart_build_meta.get("active_trades_used", len(positions)),
                "closed_trades_included":      _chart_build_meta.get("closed_trades_included", 0),
                "missing_entry_dates":         _chart_build_meta.get("missing_entry_dates", []),
                "missing_exit_dates":          _chart_build_meta.get("missing_exit_dates", []),
                "missing_price_history_symbols": _chart_build_meta.get("missing_price_history_symbols", []),
                "periods_available":           _chart_build_meta.get("periods_available", []),
                "warnings":                    _chart_build_meta.get("warnings", []),
            }
            if _p == "1D" and len(_pts) < 5:
                _perf_meta[_p]["note"] = "intraday_sparse — market may be closed or pre-market"
            if not _pts:
                _perf_meta[_p]["unavailable_reason"] = "no_history"

        # 20. Required diagnostics debug object ───────────────────────────
        _tickers_with_hist = [t for t in tickers if len(all_history.get(t, [])) >= 15]
        _tickers_no_hist   = [t for t in tickers if len(all_history.get(t, [])) < 15]
        _schema_mismatches: list[str] = []
        for _h in holdings_raw:
            if "symbol" in _h and "ticker" not in _h:
                _schema_mismatches.append(f"{_h.get('symbol')}: field=symbol (expected ticker)")
            if "avgCost" in _h and "avg_cost" not in _h:
                _schema_mismatches.append(f"{_h.get('ticker','?')}: field=avgCost (expected avg_cost)")

        _corr_tickers   = corr.get("tickers", [])
        _corr_excluded  = [e["symbol"] for e in corr.get("excluded_symbols", [])]
        _vol_excl       = list(vol_unavailable.keys())
        _unclassified   = [a["symbols"] for a in theme_allocation if a["name"] == "Unclassified"]
        _unclassified_s = _unclassified[0] if _unclassified else []
        _classified_tms = [a["name"] for a in theme_allocation if a["name"] != "Unclassified"]
        _old_demo_syms  = {"NVDA", "OSS", "BUZZ", "GOLD", "BTC", "AEHR", "MRVL", "PLAB"}
        _existing_path_debug = {
            # Holdings provenance
            "canonical_holdings_count":           len(holdings_raw),
            "canonical_symbols":                  tickers,
            "analytics_holdings_count":           len(positions),
            "analytics_symbols":                  [p["ticker"] for p in positions],
            "holdings_signature":                 _holdings_sig_early,
            "cache_key":                          self.cache_key_for(portfolio_file),
            "cache_hit":                          False,
            # Volatility
            "volatility_valid_count":             len(vol_list),
            "volatility_total_count":             len(positions),
            "volatility_excluded_symbols":        _vol_excl,
            # Correlation
            "correlation_dimensions":             [len(_corr_tickers), len(_corr_tickers)],
            "correlation_included_symbols":       _corr_tickers,
            "correlation_excluded_symbols":       _corr_excluded,
            "ticker_meta_count":                  len(corr.get("ticker_meta", [])),
            # Theme allocation
            "theme_allocation_count":             len(theme_allocation),
            "sector_allocation_count":            len(sector_allocation),
            "unclassified_symbols":               _unclassified_s,
            "theme_groups_in_portfolio":          _classified_tms,
            # Zombie / test portfolio detection
            "old_demo_symbols_present":           list(_old_demo_syms & set(tickers)),
            "test_portfolio_detected":            set(tickers) <= {"AEHR", "MRVL", "PLAB"},
            # History / YF
            "tickers_with_history":               _tickers_with_hist,
            "missing_history":                    _tickers_no_hist,
            "yf_fallback_tickers":                list(_yf_fb_h.keys()),
            "tradier_miss_history":               _tdr_miss_h,
            # Analytics checks
            "perf_chart_points":                  {_p: len(perf_charts.get(_p, [])) for _p in ("1D","5D","1M","6M","1Y")},
            "risk_has_values":                    any(v for v in risk.values() if isinstance(v, (int, float)) and v),
            "suggestions_count":                  len(suggestions),
            "schema_mismatches":                  _schema_mismatches,
        }
        print(f"[portfolio-terminal-existing-path-debug] {json.dumps(_existing_path_debug, default=str)}")

        # ── Vol/Options consolidated debug log ────────────────────────────
        _vx_avail   = [p for p in positions if p.get("vol_x") is not None]
        _vx_missing = [p for p in positions if p.get("vol_x") is None]
        _mc_avail   = [p for p in positions if p.get("_vol_mc_pct") is not None]
        _opts_avail = [p for p in positions if (p.get("_options_status") or {}).get("data_available")]
        _opts_miss  = [p for p in positions if not (p.get("_options_status") or {}).get("data_available")]
        _vol_opts_debug = {
            "holdings_count":    len(positions),
            "symbols":           [p["ticker"] for p in positions],
            "holdings_signature": _holdings_sig_early,
            "screener_engine_used": "portfolio_options_service.scan_portfolio_options",
            "cache_key":         f"portfolio_opts_scan_v2:{_holdings_sig_early}",
            "cache_hit":         _opts_cache_hit,
            "requested_symbols": equity_tickers,
            "volx": {
                "available_count":           len(_vx_avail),
                "missing_count":             len(_vx_missing),
                "examples":                  [{"sym": p["ticker"], "vol_x": p.get("vol_x"), "avg_vol_src": p.get("_avg_vol_src")} for p in _vx_avail[:5]],
                "missing_reasons_by_symbol": {p["ticker"]: p.get("_vol_x_reason") for p in _vx_missing},
            },
            "vol_mc": {
                "available_count": len(_mc_avail),
                "missing_count":   len(positions) - len(_mc_avail),
            },
            "options": {
                "holdings_count":             len(tickers),
                "requested_count":            len(equity_tickers),
                "optionable_symbols":         [p["ticker"] for p in _opts_avail],
                "available_symbols":          [p["ticker"] for p in _opts_avail],
                "available_count":            _opts_scan.get("available_count", len(_opts_avail)),
                "unavailable_count":          _opts_scan.get("unavailable_count", len(_opts_miss)),
                "unavailable_reasons_by_symbol": _opts_scan.get("unavailable_reasons_by_symbol", {}),
                "provider_calls":             _opts_scan.get("provider_calls", 0),
                "rate_limit_guard_used":      True,
                "rows_returned":             len(_opts_scan.get("rows", [])),
                "options_cache_status":       _opts_scan.get("options_cache_status", "unknown"),
                "source":                     _opts_scan.get("source", "portfolio_scoped_options_screener"),
                "cache_hit":                  _opts_cache_hit,
            },
            "provider_calls": {
                "tradier_quote_calls":       len(tradier_tickers),
                "tradier_options_calls":     _opts_scan.get("provider_calls", 0),
                "fmp_profile_calls":         len(equity_tickers),
                "yahoo_fallback_calls":      len(_yf_miss_all),
            },
        }
        print(f"[portfolio-options-screener-debug] {json.dumps(_vol_opts_debug, default=str)}")

        # ── Portfolio-only options pullback-risk debug ─────────────────────
        _risk_rows  = _opts_scan.get("rows", [])
        _risk_all   = _risk_rows + [
            r for r in (_opts_scan.get("by_symbol") or {}).values()
            if not r.get("data_available")
        ]
        _risk_debug = {
            "rows_scored":     len(_risk_all),
            "high_count":      sum(1 for r in _risk_all if r.get("risk_level") == "HIGH"),
            "elevated_count":  sum(1 for r in _risk_all if r.get("risk_level") == "ELEVATED"),
            "watch_count":     sum(1 for r in _risk_all if r.get("risk_level") == "WATCH"),
            "low_count":       sum(1 for r in _risk_all if r.get("risk_level") == "LOW"),
            "unknown_count":   sum(1 for r in _risk_all if r.get("risk_level") == "UNKNOWN"),
            "examples": [
                {
                    "ticker":          r.get("ticker"),
                    "risk_score":      r.get("risk_score"),
                    "risk_level":      r.get("risk_level"),
                    "risk_confidence": r.get("risk_confidence"),
                    "risk_reasons":    r.get("risk_reasons"),
                    "p_c":             r.get("p_c"),
                    "iv":              r.get("iv"),
                    "em":              r.get("em"),
                    "vol":             r.get("vol"),
                    "signal":          r.get("signal"),
                }
                for r in _risk_rows[:5]
            ],
        }
        print(f"[portfolio-options-risk-debug] {json.dumps(_risk_debug, default=str)}")

        # Option position summary fields
        _opt_cost_basis = round(sum(float(p.get("cost_basis") or 0) for p in _opt_positions_raw), 2)
        _equity_pos_count = len(positions)
        _opt_pos_count    = len(_opt_positions_raw)
        _total_pos_count  = _equity_pos_count + _opt_pos_count

        # Compact option position rows for the terminal response — preserve full
        # contract fields but strip DB internals (import_batch_id, source_file, etc.)
        _opt_position_rows: list[dict] = []
        for _op in _opt_positions_raw:
            _opt_position_rows.append({
                "row_type":          "option",
                "underlying_symbol": _op.get("underlying", ""),
                "underlying":        _op.get("underlying", ""),
                "display_symbol":    _op.get("display_symbol", ""),
                "option_type":       _op.get("option_type", ""),
                "call_put":          _op.get("option_type", ""),
                "expiration":        _op.get("expiration_date"),
                "expiration_date":   _op.get("expiration_date"),
                "strike":            _op.get("strike"),
                "contracts":         _op.get("contracts_open"),
                "contracts_open":    _op.get("contracts_open"),
                "avg_premium":       _op.get("avg_premium"),
                "cost_basis":        _op.get("cost_basis"),
                "mark":              None,
                "market_value":      None,
                "unrealized_pnl":    None,
                "unrealized_pnl_pct": None,
                "current_underlying_price": None,
                "realized_pnl":      _op.get("realized_pnl"),
                "final_status":      _op.get("final_status"),
                "first_entry_date":  _op.get("first_entry_date"),
                "last_entry_date":   _op.get("last_entry_date"),
            })

        # ── option_underlying_quotes ─────────────────────────────────────────
        # Underlying equity market snapshot for each option underlying that is
        # NOT an actual equity holding.  Quotes + fundamentals are already
        # fetched above (equity_tickers includes _opt_underlyings_extra), so
        # this block is pure computation — zero extra network calls.
        #
        # Field alignment mirrors the stock-holding market columns so the
        # frontend compact Holdings table can render: Price, Chg%, VolX, Vol/MC.
        _option_underlying_quotes: list[dict] = []
        for _ou in _opt_underlyings_extra:
            _ou_q       = _q(_ou)
            _ou_price   = _ou_q.get("price") or 0.0
            _ou_vol     = _ou_q.get("volume") or None
            _ou_avg_vol = _ou_q.get("avg_volume") or None
            _ou_funda   = fundamentals_raw.get(_ou, {})

            # FMP volAvg fallback (already fetched, zero cost)
            _ou_avg_vol_src = ("tradier" if _ou in tradier_quotes
                               else "yf_fallback" if _ou in _yf_fb_q
                               else "unavailable")
            if not _ou_avg_vol:
                _ou_fmp_avg = _sf(_ou_funda.get("avg_volume"))
                if _ou_fmp_avg and _ou_fmp_avg > 0:
                    _ou_avg_vol     = _ou_fmp_avg
                    _ou_avg_vol_src = "fmp_profile"

            # VolX
            _ou_vol_x = (round(_ou_vol / _ou_avg_vol, 2)
                         if (_ou_vol and _ou_avg_vol and _ou_avg_vol > 0) else None)
            _ou_vol_x_reason: str | None = None
            if _ou_vol_x is None:
                if not _ou_vol:
                    _ou_vol_x_reason = "volume_unavailable"
                elif not _ou_avg_vol:
                    _ou_vol_x_reason = ("avg_volume_unavailable_otc"
                                        if _ou_avg_vol_src in ("yf_fallback", "unavailable")
                                        else "avg_volume_unavailable")
                else:
                    _ou_vol_x_reason = "calculation_error"

            # Vol/MC
            _ou_mktcap    = _sf(_ou_funda.get("market_cap"))
            _ou_dol_vol   = round(_ou_price * _ou_vol, 2) if (_ou_price and _ou_vol) else None
            _ou_vol_mc_pct:   float | None = None
            _ou_vol_mc_label: str   | None = None
            _ou_vol_mc_unavail: str | None = None
            if _ou_dol_vol is not None and _ou_mktcap and _ou_mktcap > 0:
                _ou_vm_ratio = round(_ou_dol_vol / _ou_mktcap, 6)
                _ou_vol_mc_pct = round(_ou_vm_ratio * 100, 4)
                if _ou_vol_mc_pct >= 10:
                    _ou_vol_mc_label = "high"
                elif _ou_vol_mc_pct >= 5:
                    _ou_vol_mc_label = "elevated"
                elif _ou_vol_mc_pct >= 1:
                    _ou_vol_mc_label = "normal"
                else:
                    _ou_vol_mc_label = "low"
            else:
                if not _ou_vol:
                    _ou_vol_mc_unavail = "volume_unavailable"
                elif not _ou_mktcap:
                    _ou_vol_mc_unavail = "market_cap_unavailable"
                else:
                    _ou_vol_mc_unavail = "price_missing"

            _option_underlying_quotes.append({
                "ticker":                     _ou,
                "price":                      _sr(_ou_price) if _ou_price else None,
                "change":                     _sr(_ou_q.get("change")),
                "change_pct":                 _sr(_ou_q.get("change_pct"), 3),
                "volume":                     _ou_vol,
                "avg_volume":                 _ou_avg_vol,
                "vol_x":                      _ou_vol_x,
                "vol_x_unavailable_reason":   _ou_vol_x_reason,
                "vol_mc_pct":                 _ou_vol_mc_pct,
                "vol_mc_label":               _ou_vol_mc_label,
                "vol_mc_unavailable_reason":  _ou_vol_mc_unavail,
                "company_name":               _ou_funda.get("name", _ou),
                "sector":                     _ou_funda.get("sector"),
                "market_cap":                 _ou_mktcap,
            })

        return {
            "portfolio": {
                "value":                 round(total_value, 2),
                "change_today":          round(change_today, 2),
                "change_pct_today":      change_pct_today,
                "perf_1d":               periods["perf_1d"],
                "perf_5d":               periods["perf_5d"],
                "perf_1m":               periods["perf_1m"],
                "perf_6m":               periods["perf_6m"],
                "perf_1y":               periods["perf_1y"],
                "total_return_pct":      total_return_pct,
                "total_return_value":    round(total_return_val, 2),
                "sentiment":             sentiment,
                "market_status":         _market_status_et(),
                "options_cost_basis":    _opt_cost_basis,
            },
            "positions_count":           _total_pos_count,        # all positions (equity + options)
            "equity_position_count":     _equity_pos_count,
            "option_position_count":     _opt_pos_count,
            "total_position_count":      _total_pos_count,
            "option_positions":          _opt_position_rows,
            "option_underlying_symbols": _opt_underlyings_extra,
            "option_underlying_quotes":  _option_underlying_quotes,
            "holdings":                  self._format_holdings(positions),
            "performance_chart":      perf_chart,
            "performance_charts":     perf_charts,
            "performance_chart_meta": _perf_meta,
            "period_returns":          periods["period_returns"],
            "asset_allocation":       alloc,
            "asset_class_allocation": asset_class_alloc,
            "theme_allocation":       theme_allocation,
            "sector_allocation":      sector_allocation,
            "theme_mapping":          theme_mapping_list,
            "correlation_matrix":     corr,
            "risk_metrics":           risk,
            "volatility":             vol_list,
            "volatility_meta": {
                "method":              "annualized_daily_returns",
                "lookback_days":       _lookback_days,
                "valid_count":         len(vol_list),
                "total_holdings_count": len(positions),
                "coverage_pct":        round(len(vol_list) / len(positions) * 100, 1) if positions else 0.0,
                "excluded_symbols":    {k: v for k, v in vol_unavailable.items()},
                "unavailable_reasons": vol_unavailable,
            },
            "risk_suggestions":       suggestions,
            "top_movers":             top_movers,
            "earnings_calendar":      earnings_cal,
            "ticker_tape":            ticker_tape,
            "news_ticker":            news_ticker,
            # ── Portfolio-scoped options (reuses Options Screener logic) ──
            "portfolio_options":                   _opts_scan.get("rows", []),
            # All portfolio symbols including those with no options data — lets the
            # frontend render a row for every holding (available rows sorted first by
            # score, then unavailable rows sorted by symbol).
            "portfolio_options_all":               sorted(
                _opts_scan.get("by_symbol", {}).values(),
                key=lambda _r: (
                    0 if _r.get("data_available") else 1,
                    -(_r.get("score") or 0) if _r.get("data_available") else 0,
                    (_r.get("ticker") or _r.get("symbol") or ""),
                ),
            ),
            "options_available_count":             _opts_scan.get("available_count", 0),
            "options_unavailable_count":           _opts_scan.get("unavailable_count", 0),
            "options_unavailable_reasons_by_symbol": _opts_scan.get("unavailable_reasons_by_symbol", {}),
            "options_cache_status":                _opts_scan.get("options_cache_status", "unknown"),
            "options_source":                      "portfolio_scoped_options_screener",
            "as_of":                  datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "_holdings_sig":          _holdings_sig_early,
            "_debug":                 _existing_path_debug,
        }

    # ── Data fetchers ─────────────────────────────────────────────────────

    async def _fetch_tradier_quotes(self, syms: list[str]) -> list[dict]:
        """Batch Tradier quote fetch with saturation-skip + per-ticker LKG fallback.

        Uses the same LKG prefix as home_service._batch_quotes so that quotes
        written by the home dashboard are reused here and vice-versa.
        """
        import time as _tm
        if not syms or not self.tradier:
            return []

        _LKG_PFX = "home:wl_tradier_lkg:"
        _LKG_TTL = 72 * 3600
        from data.cache import cache as _dc

        # ── Saturation check — skip live call, fall to per-ticker LKG ────
        _saturated = False
        try:
            from data.tradier_provider import TRADIER_LIMITER as _TL
            _saturated = _TL.is_saturated()
        except Exception:
            pass

        if not _saturated:
            _t0 = _tm.time()
            try:
                from data.tradier_budget import lane as _term_lane
                with _term_lane("quotes"):
                    raw = await asyncio.wait_for(self.tradier.get_quotes(syms), timeout=8.0)
                # Write per-ticker LKG for future saturated rebuilds
                _now = _tm.time()
                for q in (raw or []):
                    _s = (q.get("symbol") or "").upper()
                    if _s and q.get("last"):
                        _dc.set(f"{_LKG_PFX}{_s}", {**q,
                            "quote_source": "tradier",
                            "quote_cached_at": _now,
                            "quote_is_stale": False,
                            "quote_fallback_reason": None,
                        }, _LKG_TTL)
                print(f"[CAELYN] tradier_quotes=live elapsed_ms={round((_tm.time()-_t0)*1000)} syms={len(syms)} returned={len(raw or [])}")
                return raw or []
            except Exception as e:
                print(f"[CAELYN] Tradier quotes error (falling to LKG): {type(e).__name__}: {e}")

        # ── LKG fallback — assemble from per-ticker cache ─────────────────
        _now = _tm.time()
        lkg_rows: list[dict] = []
        for sym in syms:
            entry = _dc.get(f"{_LKG_PFX}{sym.upper()}")
            if entry and entry.get("last"):
                lkg_rows.append({
                    **entry,
                    "quote_is_stale": True,
                    "quote_fallback_reason": "tradier_lkg_saturated" if _saturated else "tradier_lkg_error",
                    "quote_cached_at": _now,
                })
        print(f"[CAELYN] tradier_quotes=lkg saturated={_saturated} syms={len(syms)} lkg_hits={len(lkg_rows)}")
        return lkg_rows

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
        from data.tradier_budget import lane as _hist_lane
        with _hist_lane("quotes"):
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
        if not tickers:
            return []

        async def _news_for_ticker(t: str) -> list[dict]:
            if self.fmp:
                try:
                    fmp_news = await asyncio.wait_for(
                        self.fmp.get_stock_news(t, limit=7), timeout=4.0
                    )
                    if fmp_news:
                        for item in fmp_news:
                            item["_sym"] = t
                        return fmp_news
                except Exception:
                    pass
            if self.finnhub:
                try:
                    fh_news = await asyncio.to_thread(self.finnhub.get_company_news, t, 7)
                    if isinstance(fh_news, list):
                        for item in fh_news:
                            item["_sym"] = t
                        return fh_news
                except Exception:
                    pass
            return []

        try:
            results = await asyncio.gather(
                *[_news_for_ticker(t) for t in tickers[:4]],
                return_exceptions=True,
            )
            combined = []
            for res in results:
                if isinstance(res, list):
                    combined.extend(res)
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
                from data.tradier_budget import lane as _term2_lane
                with _term2_lane("quotes"):
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

    async def _fetch_theme_mapping(self, tickers: list[str]) -> dict[str, dict]:
        """Return {TICKER: theme_dict} using the backend thematic universe.

        theme_dict keys:
          primary_theme   — human display name (e.g. "Semiconductor Equipment")
          theme_id        — machine key (e.g. "semicap_equipment")
          classification  — "sector" | "theme" | "sub_theme"
          parent_sector   — parent theme_id or ""
          source          — "theme_ticker_mapper" | None

        Never raises — falls back to empty dicts for unknown tickers.
        """
        def _lookup_all(tickers: list[str]) -> dict[str, dict]:
            from services.theme_ticker_mapper import (
                map_ticker_to_primary_theme,
                map_ticker_to_theme_id,
                map_ticker_to_classification,
                map_ticker_to_parent_sector,
            )
            result: dict[str, dict] = {}
            for t in tickers:
                theme = map_ticker_to_primary_theme(t)
                result[t] = {
                    "primary_theme":  theme,
                    "theme_id":       map_ticker_to_theme_id(t),
                    "classification": map_ticker_to_classification(t),
                    "parent_sector":  map_ticker_to_parent_sector(t) or "",
                    "source":         "theme_ticker_mapper" if theme else None,
                }
            return result

        try:
            return await asyncio.to_thread(_lookup_all, tickers)
        except Exception as e:
            print(f"[CAELYN] theme_mapping error: {e}")
            return {}

    async def _fetch_fundamentals(self, equity_tickers: list[str]) -> dict[str, dict]:
        """Fetch company profiles (name, sector, industry, market_cap) for equity tickers.

        Priority:
          1. fmp_cache_service.get_company_profiles_bulk_cached() — DB + in-memory, zero latency
          2. Live FMP get_company_profile()  for cache misses
          3. Finnhub get_company_profile()   for remaining misses (sync → thread)

        Returns {TICKER: profile_dict}.  Never raises.
        """
        if not equity_tickers:
            return {}

        # 1. Bulk DB cache — fastest, no network
        out: dict[str, dict] = {}
        try:
            from services.fmp_cache_service import get_company_profiles_bulk_cached as _bulk
            out = _bulk(equity_tickers)
            hits = [t for t in equity_tickers if out.get(t, {}).get("name")]
            print(f"[CAELYN] fundamentals bulk_cache  hits={hits}  total={len(equity_tickers)}")
        except Exception as e:
            print(f"[CAELYN] fundamentals bulk_cache error: {e}")

        # 2. Live FMP for cache misses
        misses = [t for t in equity_tickers if not out.get(t, {}).get("name")]
        if misses and self.fmp:
            async def _fmp_one(sym: str) -> tuple[str, dict]:
                try:
                    p = await asyncio.wait_for(self.fmp.get_company_profile(sym), timeout=6.0)
                    return sym, p or {}
                except Exception as e:
                    print(f"[CAELYN] FMP profile {sym}: {e}")
                    return sym, {}
            fmp_res = await asyncio.gather(*[_fmp_one(t) for t in misses], return_exceptions=True)
            for item in fmp_res:
                if isinstance(item, tuple):
                    sym, prof = item
                    if prof.get("name") or prof.get("sector"):
                        out[sym] = prof
                        print(f"[CAELYN] FMP profile OK  {sym}  name={prof.get('name')}  sector={prof.get('sector')}")

        # 3. Finnhub fallback for still-missing tickers (sync call in thread)
        still_miss = [t for t in equity_tickers if not out.get(t, {}).get("name")]
        if still_miss and self.finnhub:
            async def _fh_one(sym: str) -> tuple[str, dict]:
                try:
                    p = await asyncio.wait_for(
                        asyncio.to_thread(self.finnhub.get_company_profile, sym), timeout=5.0
                    )
                    if p and p.get("name"):
                        return sym, {
                            "name":     p.get("name", sym),
                            "sector":   p.get("finnhubIndustry", ""),
                            "industry": p.get("finnhubIndustry", ""),
                            "market_cap": p.get("marketCapitalization"),
                            "exchange": p.get("exchange", ""),
                            "country":  p.get("country", ""),
                        }
                except Exception as e:
                    print(f"[CAELYN] Finnhub profile {sym}: {e}")
                return sym, {}
            fh_res = await asyncio.gather(*[_fh_one(t) for t in still_miss], return_exceptions=True)
            for item in fh_res:
                if isinstance(item, tuple):
                    sym, prof = item
                    if prof.get("name"):
                        out[sym] = prof
                        print(f"[CAELYN] Finnhub profile OK  {sym}  name={prof.get('name')}")

        return out

    # ── Builders ──────────────────────────────────────────────────────────

    def _build_theme_mapping_list(
        self, positions: list[dict], theme_mapping_raw: dict[str, dict]
    ) -> list[dict]:
        """Build the theme_mapping response array: one entry per holding with
        full thematic universe metadata per ticker."""
        result = []
        for p in positions:
            sym    = p["ticker"]
            td     = theme_mapping_raw.get(sym) or {}
            theme  = td.get("primary_theme")
            ac     = _asset_class(sym, p.get("_atype", "stock"))
            sector = p.get("_sector") or ""
            display_theme = theme or (sector if sector else ac)
            result.append({
                "ticker":         sym,
                "theme":          display_theme,
                "theme_raw":      theme,
                "theme_id":       td.get("theme_id"),
                "classification": td.get("classification"),
                "parent_sector":  td.get("parent_sector") or "",
                "theme_source":   td.get("source"),
                "asset_class":    ac,
                "sector":         sector,
                "allocation_pct": p.get("allocation_pct", 0.0),
            })
        return result

    async def _fetch_options_status(
        self,
        equity_tickers: list[str],
        holdings_sig: str | None = None,
        open_option_positions: "dict | None" = None,
    ) -> dict:
        """
        Portfolio-scoped options scan — reuses the exact scoring / signal / IV /
        expected-move logic as the master options screener.

        Three-layer strategy:
          1. Whole-portfolio cache  portfolio_opts_scan_v1:{sig}  (300 s)
          2. Per-ticker cache       portfolio_opts:{sym}          (300 s)
          3. Master screener cache  (already scored by TradierFlowEngine)
          4. Live Tradier scan      for tickers not in any cache

        open_option_positions: underlying → list of open position dicts.
          Passed through to scan_portfolio_options so that symbols with open
          contracts are never labelled "NO OPTIONS" when the chain scan fails.

        Returns the full scan dict from portfolio_options_service.scan_portfolio_options().
        """
        from data.portfolio_options_service import scan_portfolio_options
        import json as _json

        _MASTER_KEY = "options_master_screener_v1"
        _LKG_KEY    = "options_master_lkg_v1"
        _LKG_DISK   = Path(__file__).resolve().parent / "options_master_lkg_v1.json"
        _DATA_DIR   = Path(__file__).resolve().parent

        # Resolve master snap: memory → LKG key → disk LKG
        master_snap = cache.get(_MASTER_KEY) or cache.get(_LKG_KEY)
        if not master_snap and _LKG_DISK.exists():
            try:
                master_snap = _json.loads(_LKG_DISK.read_text())
            except Exception:
                master_snap = None

        # Augment master snap with per-segment LKG disk files so portfolio
        # symbols that appear in the regular Options Flow universe (e.g. QCOM
        # in large_cap) are available immediately after restart without waiting
        # for the background screener loop to complete its first cycle.
        # This is purely additive — existing master snap rows are not replaced.
        _SEG_NAMES = ("large_cap", "small_cap", "etf", "megacap")
        _seg_extra: list[dict] = []
        for _seg in _SEG_NAMES:
            _seg_path = _DATA_DIR / f"options_lkg_v1_{_seg}.json"
            if _seg_path.exists():
                try:
                    _seg_data = _json.loads(_seg_path.read_text())
                    _seg_extra.extend(_seg_data.get("tickers") or [])
                except Exception:
                    pass

        if _seg_extra:
            _existing_syms: set[str] = set()
            if master_snap:
                _existing_syms = {
                    (r.get("ticker") or "").upper()
                    for r in (master_snap.get("tickers") or [])
                }
            _new_rows = [
                r for r in _seg_extra
                if (r.get("ticker") or "").upper() not in _existing_syms
            ]
            if _new_rows:
                _base = master_snap or {"tickers": []}
                master_snap = {
                    **_base,
                    "tickers": list(_base.get("tickers") or []) + _new_rows,
                }

        return await scan_portfolio_options(
            symbols                = equity_tickers,
            tradier                = self.tradier,
            cache                  = cache,
            master_snap            = master_snap,
            holdings_sig           = holdings_sig,
            open_option_positions  = open_option_positions,
        )

    def _format_holdings(self, positions: list[dict]) -> list[dict]:
        result = []
        for p in positions:
            price = p.get("price") or 0.0
            cost  = p.get("_cost") or 0.0
            shares = p.get("_shares") or 0.0
            unreal_val = _sr((price - cost) * shares, 2) if price and cost else None
            unreal_pct = _sr((price - cost) / cost * 100, 2) if price and cost else None
            result.append({
                "ticker":             p["ticker"],
                "name":               p.get("_name", p["ticker"]),
                "sector":             p.get("_sector", ""),
                "price":              p["price"],
                "change":             p["change"],
                "change_pct":         p["change_pct"],
                "allocation_pct":     p["allocation_pct"],
                "avg_cost":           cost or None,
                "shares":             shares or None,
                "market_val":         _sr(p.get("market_val"), 2),
                "w52_high":           p.get("w52_high"),
                "w52_low":            p.get("w52_low"),
                "total_return_value": unreal_val,
                "total_return_pct":   unreal_pct,
                "volume":             p.get("volume"),
                "avg_volume":         p.get("avg_volume"),
                "vol_x":              p.get("vol_x"),
                "vol_x_unavailable_reason": p.get("_vol_x_reason"),
                "volume_source":      p.get("_vol_src"),
                "avg_volume_source":  p.get("_avg_vol_src"),
                # Vol/MC — dollar-volume / market cap
                "market_cap":             p.get("_market_cap"),
                "dollar_volume":          p.get("_dollar_volume"),
                "vol_mc_ratio":           p.get("_vol_mc_ratio"),
                "vol_mc_pct":             p.get("_vol_mc_pct"),
                "vol_mc_label":           p.get("_vol_mc_label"),
                "vol_mc_unavailable_reason": p.get("_vol_mc_unavail"),
                # Options status (from master options screener cache)
                "options_status":     p.get("_options_status"),
            })
        return result

    def _get_closes(self, sym: str, all_history: dict) -> list[float]:
        bars = all_history.get(sym, [])
        return [b["close"] for b in bars if b.get("close")]

    def _build_perf_charts(
        self,
        positions: list[dict],
        all_history: dict,
        spy_bars: list,
        closed_trades: list[dict] | None = None,
    ) -> dict[str, list[dict]]:
        """
        Build performance charts for five periods: 1D, 5D, 1M, 6M, 1Y.
        Each array is normalized to 0.0% at the first point.

        Trade-ledger reconstruction:
          - Active holdings: only counted from their _entry_date onward.
            Holdings with no entry_date are included for the full period
            (backward-compatible with pre-ledger portfolios).
          - Closed trades: counted from entry_date → exit_date using the
            historical price series when available, or exit_price as a fixed
            proxy otherwise.  After exit_date the position is frozen at its
            exit value (representing cash removed from the portfolio).
        """
        closed_trades = closed_trades or []

        spy_daily = {b["date"]: b["close"] for b in spy_bars if b.get("close")}
        all_spy_dates = sorted(spy_daily.keys())

        # Per-holding daily price maps (active positions)
        price_maps: dict[str, dict[str, float]] = {}
        for p in positions:
            sym  = p["_sym"]
            bars = all_history.get(sym, [])
            price_maps[sym] = {b["date"]: b["close"] for b in bars if b.get("close")}

        # Price maps for closed trades (reuse existing if same symbol, else empty)
        ct_price_maps: dict[str, dict[str, float]] = {}
        for ct in closed_trades:
            sym = (ct.get("ticker") or "").upper()
            if not sym:
                continue
            if sym in price_maps:
                ct_price_maps[sym] = price_maps[sym]
            elif sym in all_history:
                ct_price_maps[sym] = {
                    b["date"]: b["close"]
                    for b in all_history[sym]
                    if b.get("close")
                }
            else:
                ct_price_maps[sym] = {}

        # Determine whether any entry_date is set (affects meta reporting)
        _has_any_entry_date = any(p.get("_entry_date") for p in positions) or bool(closed_trades)

        def _price_at(pm: dict, dt_str: str, fallback: float) -> float:
            px = pm.get(dt_str)
            if px is not None:
                return px
            cands = [d for d in pm if d <= dt_str]
            return pm[max(cands)] if cands else fallback

        def _spy_at(dt_str: str) -> float:
            v = spy_daily.get(dt_str)
            if v:
                return v
            cands = [d for d in all_spy_dates if d <= dt_str]
            return spy_daily[max(cands)] if cands else 0.0

        def _port_return_at(dt_str: str) -> tuple[float, float]:
            """
            Cost-basis normalised return at dt_str.

            Returns (portfolio_return_pct, spy_dollar_weighted_return_pct).

            portfolio_return = (market_value − cost_basis) / cost_basis × 100
              → opening a new position does NOT spike the chart because mval ≈
                cost at inception; only actual price appreciation changes the %.

            spy_return = dollar-weighted SPY return:
              for each position, compute SPY's gain from that position's
              entry_date to dt_str, weighted by the position's cost basis.
              This answers "what if every dollar had gone into SPY instead?"
              at the same time it was actually deployed.

            De-dup rule: if a ticker appears in both active holdings and closed
            trades on this date, the ACTIVE holding takes priority (closed trade
            is skipped) to avoid double-counting re-bought positions.
            """
            total_mval = 0.0
            total_cost = 0.0
            spy_weighted_gain = 0.0   # Σ( cost_i × spy_return_i )

            active_syms_on_date: set[str] = set()

            # Active positions
            for p in positions:
                ed = p.get("_entry_date")
                if ed and dt_str < ed:
                    continue
                active_syms_on_date.add(p["_sym"])
                pm            = price_maps.get(p["_sym"], {})
                px            = _price_at(pm, dt_str, p.get("price") or 0)
                shares        = p["_shares"]
                cost_per_sh   = p.get("_cost") or 0
                position_cost = shares * cost_per_sh
                total_mval   += shares * (px or 0)
                total_cost   += position_cost
                # Dollar-weighted SPY return for this position's holding period
                if ed and position_cost > 0:
                    spy_start = _spy_at(ed)
                    spy_now   = _spy_at(dt_str)
                    if spy_start:
                        spy_weighted_gain += position_cost * (spy_now - spy_start) / spy_start

            # Closed trades (de-dup: skip if ticker already active today)
            for ct in closed_trades:
                ed  = ct.get("entry_date") or ""
                xd  = ct.get("exit_date")  or ""
                sym = (ct.get("ticker") or "").upper()
                if not ed or not sym:
                    continue
                if dt_str < ed:
                    continue          # position not yet open
                if sym in active_syms_on_date:
                    continue          # same ticker counted via active holding

                shares      = float(ct.get("shares")      or 0)
                entry_price = float(ct.get("entry_price") or 0)
                exit_price  = float(ct.get("exit_price")  or 0)
                position_cost = shares * entry_price

                if xd and dt_str > xd:
                    # Position is fully closed — lock P&L at exit_price so that
                    # subsequent stock price movements do NOT affect the chart.
                    # SPY contribution is also frozen at the exit date.
                    total_mval += shares * exit_price
                    total_cost += position_cost
                    if position_cost > 0:
                        spy_start = _spy_at(ed)
                        spy_exit  = _spy_at(xd)
                        if spy_start and spy_exit:
                            spy_weighted_gain += position_cost * (spy_exit - spy_start) / spy_start
                    continue

                pm          = ct_price_maps.get(sym, {})
                px          = _price_at(pm, dt_str, exit_price)
                total_mval   += shares * (px or exit_price)
                total_cost   += position_cost
                if position_cost > 0:
                    spy_start = _spy_at(ed)
                    spy_now   = _spy_at(dt_str)
                    if spy_start:
                        spy_weighted_gain += position_cost * (spy_now - spy_start) / spy_start

            if not total_cost:
                return (0.0, 0.0)
            port_ret = round((total_mval - total_cost) / total_cost * 100, 2)
            spy_ret  = round(spy_weighted_gain / total_cost * 100, 2)
            return (port_ret, spy_ret)

        def _make_chart(date_labels: list[tuple[str, str]]) -> list[dict]:
            """Build a period-relative performance chart from [(label, date_str), ...].

            Portfolio series:
              Uses cost-basis normalised returns (market_value − cost_basis) /
              cost_basis × 100, then anchored to 0 % at the first active date
              in the window.  This eliminates capital-addition distortion while
              preserving the conventional "starts at 0 %" period chart UX.

            S&P 500 series:
              Simple SPY price change from the anchor date to each date,
              i.e. (SPY_d − SPY_anchor) / SPY_anchor × 100.  This is the
              conventional benchmark comparison users expect to see — "how much
              would I have made if I just bought SPY at the start of this period?"
            """
            # Build raw portfolio cost-basis returns
            raw: list[tuple[str, str, float]] = []   # (label, date_str, port_return)
            for label, d in date_labels:
                pr, _ = _port_return_at(d)
                raw.append((label, d, pr))

            # Trim leading dates where nothing was deployed yet
            first_active = next(
                (i for i, (_, _, pr) in enumerate(raw) if pr != 0.0),
                None,
            )
            if first_active is None:
                return []

            raw = raw[first_active:]
            base_pr  = raw[0][2]
            anchor_d = raw[0][1]
            spy_anchor_px = _spy_at(anchor_d)

            out = []
            for label, d, pr in raw:
                spy_now = _spy_at(d)
                spy_chg = round((spy_now - spy_anchor_px) / spy_anchor_px * 100, 2) if spy_anchor_px else 0.0
                out.append({
                    "date":      label,
                    "portfolio": round(pr - base_pr, 2),
                    "sp500":     spy_chg,
                })
            return out

        today = date.today()
        today_str = today.isoformat()

        # ── 1D: intraday using Yahoo Finance 5-min data ─────────────────
        chart_1d = self._build_1d_chart(positions, price_maps, spy_daily)

        # ── 5D: one point per trading day, past 5 trading days ──────────
        trading_days = [d for d in all_spy_dates if d <= today_str][-6:]
        chart_5d = _make_chart([
            (datetime.strptime(d, "%Y-%m-%d").date().strftime("%a"), d)
            for d in trading_days
        ])

        # ── 1M: daily, sample every 3rd trading day (~10 pts) ───────────
        days_1m = [d for d in all_spy_dates
                   if d >= (today - timedelta(days=35)).isoformat() and d <= today_str]
        if days_1m:
            step = max(1, len(days_1m) // 10)
            idxs = sorted(set(
                list(range(0, len(days_1m), step)) +
                ([len(days_1m) - 1] if (len(days_1m) - 1) % step != 0 else [])
            ))
            chart_1m = _make_chart([
                (datetime.strptime(days_1m[i], "%Y-%m-%d").date().strftime("%b %-d"), days_1m[i])
                for i in idxs
            ])
        else:
            chart_1m = []

        # ── 6M: weekly data points (~26 pts) ────────────────────────────
        days_6m = [d for d in all_spy_dates
                   if d >= (today - timedelta(days=186)).isoformat() and d <= today_str]
        if days_6m:
            step = max(1, len(days_6m) // 26)
            idxs = sorted(set(
                list(range(0, len(days_6m), step)) +
                ([len(days_6m) - 1] if (len(days_6m) - 1) % step != 0 else [])
            ))
            chart_6m = _make_chart([
                (datetime.strptime(days_6m[i], "%Y-%m-%d").date().strftime("%b %-d"), days_6m[i])
                for i in idxs
            ])
        else:
            chart_6m = []

        # ── 1Y: monthly sampled (~13 pts) ───────────────────────────────
        days_1y = [d for d in all_spy_dates
                   if d >= (today - timedelta(days=375)).isoformat() and d <= today_str]
        if days_1y:
            step = max(1, len(days_1y) // 13)
            idxs = sorted(set(
                list(range(0, len(days_1y), step)) +
                ([len(days_1y) - 1] if (len(days_1y) - 1) % step != 0 else [])
            ))
            chart_1y = _make_chart([
                (_month_label(datetime.strptime(days_1y[i], "%Y-%m-%d").date()), days_1y[i])
                for i in idxs
            ])
        else:
            chart_1y = []

        charts = {"1D": chart_1d, "5D": chart_5d, "1M": chart_1m, "6M": chart_6m, "1Y": chart_1y}

        # ── Reconstruct metadata for the meta builder ────────────────────
        _missing_entry_dates  = [p["_sym"] for p in positions if not p.get("_entry_date")]
        _missing_exit_dates   = [(ct.get("ticker") or "") for ct in closed_trades if not ct.get("exit_date")]
        _missing_price_hist   = [
            (ct.get("ticker") or "").upper()
            for ct in closed_trades
            if not ct_price_maps.get((ct.get("ticker") or "").upper())
        ]
        _periods_available    = [p for p in ("1D", "5D", "1M", "6M", "1Y") if charts.get(p)]
        _active_trades_used   = sum(
            1 for p in positions
            if not p.get("_entry_date") or today_str >= p["_entry_date"]
        )

        _warnings: list[str] = []
        if _missing_entry_dates:
            _warnings.append(
                f"{len(_missing_entry_dates)} active holding(s) have no entry_date "
                f"({', '.join(_missing_entry_dates[:5])}); included for full chart period"
            )
        if _missing_exit_dates:
            _warnings.append(
                f"{len(_missing_exit_dates)} closed trade(s) have no exit_date and were skipped"
            )
        if _missing_price_hist:
            _warnings.append(
                f"{len(_missing_price_hist)} closed trade symbol(s) have no price history; "
                f"exit_price used as mark-to-market proxy ({', '.join(_missing_price_hist[:5])})"
            )

        charts["_meta"] = {
            "method":                      "trade_ledger_reconstruction" if _has_any_entry_date else "current_holdings_assumed",
            "estimated":                   True,
            "entry_dates_set":             sum(1 for p in positions if p.get("_entry_date")),
            "active_trades_used":          _active_trades_used,
            "closed_trades_included":      len(closed_trades),
            "missing_entry_dates":         _missing_entry_dates,
            "missing_exit_dates":          _missing_exit_dates,
            "missing_price_history_symbols": _missing_price_hist,
            "periods_available":           _periods_available,
            "warnings":                    _warnings,
        }

        return charts

    def _build_1d_chart(
        self, positions: list[dict], price_maps: dict, spy_daily: dict
    ) -> list[dict]:
        """
        1D intraday chart using Yahoo Finance 5-min data.

        Uses SPY timestamps as the primary axis and forward-fills each holding's
        last-available intraday price so that timestamp sparsity in individual
        holdings never collapses the chart to a single point.
        Falls back to empty list if market is closed / data unavailable.
        """
        import zoneinfo
        et = datetime.now(zoneinfo.ZoneInfo("America/New_York"))
        today_str = et.strftime("%Y-%m-%d")

        # Collect yahoo symbols for all holdings + SPY
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

        # Fetch intraday 5-min data for every symbol (interval=5m, range=1d)
        intraday_map: dict[str, list[tuple[str, float]]] = {}
        for sym, ysym in yahoo_syms.items():
            try:
                raw = _yf_fetch_sync(ysym, "1d", interval="5m")
                res = raw.get("chart", {}).get("result", [{}])[0]
                ts     = res.get("timestamp", [])
                closes = res.get("indicators", {}).get("quote", [{}])[0].get("close", [])
                bars: list[tuple[str, float]] = []
                for i in range(min(len(ts), len(closes))):
                    c = closes[i]
                    if c is not None:
                        t_et = datetime.fromtimestamp(ts[i], tz=zoneinfo.ZoneInfo("America/New_York"))
                        if t_et.strftime("%Y-%m-%d") == today_str:
                            bars.append((t_et.strftime("%H:%M"), c))
                intraday_map[sym] = bars
            except Exception:
                intraday_map[sym] = []

        spy_bars_1d = sorted(intraday_map.get("SPY", []))
        if not spy_bars_1d:
            return []

        # Pre-sort each holding's intraday bars for linear-scan forward-fill
        holding_sorted: dict[str, list[tuple[str, float]]] = {
            p["_sym"]: sorted(intraday_map.get(p["_sym"], []))
            for p in positions
        }

        # Initialise last-known price from Tradier quote (already fetched this request)
        last_px:     dict[str, float] = {p["_sym"]: (p.get("price") or 0.0) for p in positions}
        holding_ptr: dict[str, int]   = {p["_sym"]: 0 for p in positions}

        # Walk SPY timestamps; for each bar advance every holding's pointer and
        # forward-fill its last-known price.  This guarantees we always have a
        # portfolio value even when a holding's last reported bar was 5 min ago.
        result_pairs: list[tuple[str, float, float]] = []
        for t_label, spy_v in spy_bars_1d:
            if not spy_v:
                continue
            for p in positions:
                sym  = p["_sym"]
                bars = holding_sorted[sym]
                ptr  = holding_ptr[sym]
                # Consume all holding bars whose timestamp ≤ current SPY bar
                while ptr < len(bars) and bars[ptr][0] <= t_label:
                    last_px[sym] = bars[ptr][1]
                    ptr += 1
                holding_ptr[sym] = ptr

            port_v = sum(p["_shares"] * last_px[p["_sym"]] for p in positions)
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

    def _build_theme_allocation(
        self,
        positions: list[dict],
        total_value: float,
        theme_mapping_raw: dict[str, dict],
    ) -> list[dict]:
        """Build theme_allocation using the backend thematic universe as primary grouping.

        Each item:
          name          — theme display name (or "Unclassified" for unknown tickers)
          weight_pct    — % of total portfolio market value
          market_value  — absolute USD value
          symbols       — list of tickers in this theme
          source        — "thematic_universe" | "fallback_asset_class"
          fallback_used — True when theme was not found in thematic universe
        """
        theme_groups: dict[str, dict] = {}
        unclassified: list[str] = []

        for p in positions:
            sym  = p["_sym"]
            mval = p["market_val"]
            td   = theme_mapping_raw.get(sym) or {}
            name = td.get("primary_theme")
            src  = td.get("source") or ""
            fallback_used = not bool(name)

            if not name:
                name = "Unclassified"
                unclassified.append(sym)

            if name not in theme_groups:
                theme_groups[name] = {
                    "name":          name,
                    "market_value":  0.0,
                    "symbols":       [],
                    "source":        "thematic_universe" if not fallback_used else "fallback_asset_class",
                    "fallback_used": fallback_used,
                }

            theme_groups[name]["market_value"] += mval
            theme_groups[name]["symbols"].append(sym)
            # Mark as classified if any ticker in group has a real theme
            if not fallback_used:
                theme_groups[name]["source"]        = "thematic_universe"
                theme_groups[name]["fallback_used"] = False

        result = []
        for name, g in sorted(theme_groups.items(), key=lambda x: -x[1]["market_value"]):
            pct = round(g["market_value"] / total_value * 100, 1) if total_value else 0.0
            result.append({
                "name":          name,
                "weight_pct":    pct,
                "market_value":  round(g["market_value"], 2),
                "symbols":       g["symbols"],
                "source":        g["source"],
                "fallback_used": g["fallback_used"],
            })

        return result

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

    def _build_asset_class_allocation(self, positions, total_value) -> list[dict]:
        """Dashboard-compatible asset class grouping: Stocks / ETFs / Crypto / Commodities / Indices.
        Maps holding asset_type directly to the same five buckets shown on Portfolio Dashboard.
        """
        _DASH: dict[str, tuple[str, str]] = {
            "stock":     ("Stocks",      "#a78bfa"),
            "etf":       ("ETFs",        "#38bdf8"),
            "fund":      ("ETFs",        "#38bdf8"),
            "crypto":    ("Crypto",      "#e879f9"),
            "commodity": ("Commodities", "#fb923c"),
            "index":     ("Indices",     "#22c55e"),
        }
        totals: dict[str, float] = {}
        colors: dict[str, str]   = {}
        for p in positions:
            atype = (p.get("_atype") or "stock").lower().strip()
            label, color = _DASH.get(atype, ("Stocks", "#a78bfa"))
            totals[label] = totals.get(label, 0.0) + p["market_val"]
            colors[label] = color

        result = []
        for label, val in sorted(totals.items(), key=lambda x: -x[1]):
            result.append({
                "label": label,
                "pct":   round(val / total_value * 100, 1) if total_value else 0.0,
                "color": colors[label],
            })
        return result

    def _build_correlation(
        self,
        all_tickers: list[str],
        all_history: dict,
        lookback_days: int = 420,
        theme_mapping: dict | None = None,
        fundamentals: dict | None = None,
    ) -> dict:
        """
        Compute NxN Pearson correlation matrix for ALL holdings.
        Uses an inner-join on calendar dates so crypto/commodity/equity
        date mismatches are handled correctly.
        Returns excluded_symbols with reasons for any tickers with insufficient data.

        Each ticker in the result is enriched with:
          - primary_theme  (from the thematic universe / theme_ticker_mapper)
          - sector         (from FMP fundamentals)
          - industry       (from FMP fundamentals, when available)
        so the frontend can group/colour by theme without losing correlation values.
        """
        theme_mapping = theme_mapping or {}
        fundamentals  = fundamentals  or {}

        excluded_symbols: list[dict] = []
        returns_map: dict[str, dict[str, float]] = {}
        for t in all_tickers:
            bars = all_history.get(t, [])
            closes = [(b["date"], b["close"]) for b in bars if b.get("close")]
            if len(closes) < 15:
                excluded_symbols.append({
                    "symbol": t,
                    "reason": f"insufficient_history ({len(closes)} bars, min 15)",
                })
                continue
            rets: dict[str, float] = {}
            for i in range(1, len(closes)):
                d, c = closes[i]
                prev = closes[i - 1][1]
                if prev and prev != 0:
                    rets[d] = (c - prev) / prev
            if len(rets) >= 15:
                returns_map[t] = rets
            else:
                excluded_symbols.append({
                    "symbol": t,
                    "reason": f"insufficient_returns ({len(rets)} dates, min 15)",
                })

        valid = [t for t in all_tickers if t in returns_map]
        n = len(valid)

        # Build per-ticker metadata for frontend grouping/colouring
        def _ticker_meta(t: str) -> dict:
            f  = fundamentals.get(t, {})
            td = theme_mapping.get(t) or {}
            theme    = td.get("primary_theme")
            sector   = (f.get("sector")   or "").strip()
            industry = (f.get("industry") or "").strip()
            display_theme = theme or sector or _asset_class(t) or t
            return {
                "ticker":         t,
                "primary_theme":  display_theme,
                "theme_raw":      theme,
                "theme_id":       td.get("theme_id"),
                "classification": td.get("classification"),
                "parent_sector":  td.get("parent_sector") or "",
                "theme_source":   td.get("source"),
                "sector":         sector,
                "industry":       industry,
            }

        ticker_meta = [_ticker_meta(t) for t in valid]

        # Build theme_groups and sector_groups for frontend grouping/sorting
        _theme_groups: dict[str, list[str]] = {}
        _sector_groups: dict[str, list[str]] = {}
        for tm in ticker_meta:
            _tg = tm["theme_raw"] or tm["primary_theme"] or "Unclassified"
            _sg = tm["sector"] or "Unknown"
            _theme_groups.setdefault(_tg, []).append(tm["ticker"])
            _sector_groups.setdefault(_sg, []).append(tm["ticker"])

        _base = {
            "method":           "pearson",
            "lookback_days":    lookback_days,
            "excluded_symbols": excluded_symbols,
            "theme_groups":     _theme_groups,
            "sector_groups":    _sector_groups,
        }
        if n == 0:
            return {**_base, "tickers": [], "ticker_meta": [], "values": [], "common_dates_count": 0}

        # Inner-join: only dates where ALL tickers have data
        date_sets = [set(returns_map[t].keys()) for t in valid]
        common_dates = sorted(set.intersection(*date_sets)) if len(date_sets) > 1 else sorted(date_sets[0])

        if len(common_dates) < 10:
            return {
                **_base,
                "tickers":            valid,
                "ticker_meta":        ticker_meta,
                "values":             [[1.0 if i == j else 0.0 for j in range(n)] for i in range(n)],
                "common_dates_count": len(common_dates),
                "note":               "insufficient_common_dates — identity matrix used",
            }

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

        return {
            **_base,
            "tickers":            valid,
            "ticker_meta":        ticker_meta,
            "values":             mat,
            "common_dates_count": len(common_dates),
        }

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

        # Collect reasons for any metrics that couldn't be computed
        unavailable_reasons: dict[str, str] = {}
        if not weighted_beta:
            unavailable_reasons["portfolio_beta"] = (
                "insufficient_common_returns with SPY (min 20 matching trading days)"
            )
        if not port_rets_list:
            unavailable_reasons["sharpe_ratio"]  = "no portfolio return history"
            unavailable_reasons["sortino_ratio"] = "no portfolio return history"
            unavailable_reasons["max_drawdown"]  = "no portfolio return history"

        return {
            "weighted_volatility":      round(weighted_vol, 1),
            "max_drawdown":             max_dd,
            "top_concentration":        top_conc,
            "top_concentration_label":  top_conc_label,
            "portfolio_beta":           round(weighted_beta, 2) if weighted_beta else None,
            "sharpe_ratio":             sharpe,
            "sortino_ratio":            sortino,
            "data_source":              "tradier_daily_history",
            "unavailable_reasons":      unavailable_reasons,
        }

    def _build_volatility(
        self, positions, all_history: dict, lookback_days: int = 420
    ) -> tuple[list[dict], dict[str, str]]:
        """Return (items_list, unavailable_reasons).
        Each item includes lookback_days and data_points for transparency.
        unavailable_reasons maps ticker → human-readable explanation for any
        symbol that could not be computed.
        """
        vols: list[dict] = []
        unavailable: dict[str, str] = {}
        for p in positions:
            closes = self._get_closes(p["_sym"], all_history)
            v = _annualized_vol(closes)
            if v is not None:
                vols.append({
                    "ticker":       p["ticker"],
                    "vol":          v,
                    "lookback_days": lookback_days,
                    "data_points":  len(closes),
                })
            else:
                unavailable[p["ticker"]] = (
                    f"insufficient_history ({len(closes)} closes, min 10)"
                )
        return sorted(vols, key=lambda x: -x["vol"]), unavailable

    def _build_suggestions(
        self, positions, alloc, risk, theme_mapping: dict | None = None
    ) -> list[dict]:
        """
        Generate up to 5 portfolio-specific risk suggestions from the ACTUAL
        holdings.  All rules are fully dynamic — no ticker names are hardcoded.
        Rules are evaluated in priority order; only triggered rules are returned.
        """
        suggestions: list[dict] = []
        alloc_map    = {a["label"]: a["pct"] for a in alloc}
        ticker_alloc = {p["ticker"]: p["allocation_pct"] for p in positions}
        theme_mapping = theme_mapping or {}

        # ── Rule 1: Single-position concentration risk (≥40%) ────────────
        for p in sorted(positions, key=lambda x: -x["allocation_pct"]):
            pct = p["allocation_pct"]
            if pct >= 40:
                dd_impact = round(pct * 0.2)
                suggestions.append({
                    "level": "RISK",
                    "title": f"High Concentration in {p['ticker']}",
                    "body": (
                        f"{p['ticker']} represents {pct:.0f}% of total portfolio value. "
                        f"A 20% drawdown in {p['ticker']} alone would reduce your total "
                        f"portfolio by ~{dd_impact}%. Consider trimming to below 30%, or "
                        f"hedging with a covered call on the position."
                    ),
                })
                break  # Flag only the top one to avoid repetition

        # ── Rule 2: Single-sector overexposure (same FMP sector > 70%) ──
        # Only fires when we have real FMP sector data (non-empty _sector).
        # Skips asset-class fallback labels so "Individual Stocks" never
        # triggers a spurious sector-concentration warning.
        if len(suggestions) < 4:
            sector_alloc: dict[str, float] = {}
            sector_tickers: dict[str, list[str]] = {}
            for p in positions:
                sec = (p.get("_sector") or "").strip()
                if not sec:
                    continue  # Skip positions without real FMP sector data
                sector_alloc[sec]   = sector_alloc.get(sec, 0.0) + p["allocation_pct"]
                sector_tickers.setdefault(sec, []).append(p["ticker"])
            for sec, pct in sorted(sector_alloc.items(), key=lambda x: -x[1]):
                if pct >= 70:
                    tks = sector_tickers[sec]
                    tks_str = ", ".join(tks[:4])
                    suggestions.append({
                        "level": "RISK",
                        "title": f"Sector Concentration — {sec}",
                        "body": (
                            f"{tks_str} together represent {pct:.0f}% of the portfolio, "
                            f"all within the {sec} sector. High intra-sector correlation "
                            f"means a single sector-wide event (earnings miss, regulation, "
                            f"macro shock) could impact all positions simultaneously. "
                            f"Consider adding exposure to an uncorrelated sector."
                        ),
                    })
                    break

        # ── Rule 3: No defensive allocation (0% fixed income) ────────────
        fi_pct = alloc_map.get(_FIXED, 0)
        if fi_pct == 0 and len(suggestions) < 4:
            # Build a short description of the portfolio tilt for context
            crypto_pct  = alloc_map.get(_CRYPTO, 0)
            stock_pct   = alloc_map.get(_STOCK, 0)
            tilt_parts  = []
            if stock_pct >= 50:
                tilt_parts.append("individual stocks")
            if crypto_pct > 0:
                tilt_parts.append(f"crypto ({crypto_pct:.0f}%)")
            tilt_desc = " and ".join(tilt_parts) if tilt_parts else "growth assets"
            suggestions.append({
                "level": "WARN",
                "title": "No Defensive Allocation",
                "body": (
                    f"The portfolio is 100% risk-on with no fixed income buffer. "
                    f"With exposure concentrated in {tilt_desc}, a 5-10% allocation "
                    f"to TLT (long-duration Treasuries) or AGG can act as a "
                    f"flight-to-quality hedge during equity drawdowns — not as a "
                    f"core holding, but as a volatility dampener."
                ),
            })

        # ── Rule 4: Crypto tail risk (any crypto holding > 3%) ────────────
        if len(suggestions) < 4:
            crypto_positions = [
                p for p in positions
                if p.get("_atype") == "crypto" and p["allocation_pct"] > 3
            ]
            for cp in sorted(crypto_positions, key=lambda x: -x["allocation_pct"])[:1]:
                cpct = cp["allocation_pct"]
                wv   = risk.get("weighted_volatility") or 0
                contrib_est = round(cpct / 100 * wv, 1)
                suggestions.append({
                    "level": "WARN",
                    "title": f"Crypto Tail Risk — {cp['ticker']}",
                    "body": (
                        f"{cp['ticker']} (~{cpct:.0f}% allocation) has historically "
                        f"drawn down 50%+ in risk-off regimes. At current sizing it "
                        f"contributes an estimated ~{contrib_est:.1f}% to portfolio "
                        f"volatility. Consider sizing down if VIX spikes above 25 or "
                        f"DXY strengthens — both signal crypto headwinds."
                    ),
                })

        # ── Rule 5: Low diversification (< 4 holdings) ───────────────────
        if len(positions) < 4 and len(suggestions) < 4:
            tks = [p["ticker"] for p in positions]
            suggestions.append({
                "level": "WARN",
                "title": "Low Diversification",
                "body": (
                    f"The portfolio holds only {len(positions)} position"
                    f"{'s' if len(positions) != 1 else ''} ({', '.join(tks)}). "
                    f"Concentrated portfolios can outperform, but also expose you to "
                    f"idiosyncratic risk. Consider adding 2-3 uncorrelated positions "
                    f"from different sectors to reduce single-name risk."
                ),
            })

        # ── Rule 6: High portfolio volatility (weighted vol > 45%) ───────
        wvol = risk.get("weighted_volatility") or 0
        if wvol > 45 and len(suggestions) < 5:
            top_vol_p = max(positions, key=lambda x: x["allocation_pct"], default=None)
            anchor = top_vol_p["ticker"] if top_vol_p else "top holding"
            suggestions.append({
                "level": "INFO",
                "title": "High Portfolio Volatility",
                "body": (
                    f"Weighted annualized volatility is {wvol:.0f}% — significantly "
                    f"above the S&P 500's typical 15-18%. This amplifies both gains "
                    f"and losses. If this exceeds your risk tolerance, consider "
                    f"reducing position size in {anchor} or adding a lower-vol "
                    f"asset such as a broad-market ETF."
                ),
            })

        # ── Rule 7: Thematic concentration (backend thematic universe) ───
        if len(suggestions) < 5 and theme_mapping:
            _ttheme_groups: dict[str, list[str]] = {}
            _ttheme_pcts:   dict[str, float]     = {}
            for p in positions:
                td = theme_mapping.get(p["ticker"]) or {}
                t  = td.get("primary_theme")
                if t:
                    _ttheme_groups.setdefault(t, []).append(p["ticker"])
                    _ttheme_pcts[t] = _ttheme_pcts.get(t, 0.0) + p["allocation_pct"]
            # Fire on largest theme cluster (≥2 holdings OR ≥30% concentration)
            for theme, tks in sorted(_ttheme_groups.items(), key=lambda x: -_ttheme_pcts.get(x[0], 0)):
                pct = _ttheme_pcts.get(theme, 0.0)
                if len(tks) >= 2 or pct >= 30:
                    level = "RISK" if pct >= 50 else "WARN" if pct >= 30 else "INFO"
                    tks_str = ", ".join(tks[:5])
                    suggestions.append({
                        "level": level,
                        "title": f"Theme Concentration — {theme}",
                        "body": (
                            f"{tks_str} all map to the '{theme}' thematic universe "
                            f"and together represent {pct:.0f}% of the portfolio. "
                            f"High thematic overlap increases correlation risk — a single "
                            f"sector-wide event (supply chain shock, regulatory change, "
                            f"earnings cycle) can impact all positions simultaneously."
                        ),
                    })
                    break

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

        def _perf_and_val(days: int):
            """Returns (pct, dollar_value) or (None, None) if insufficient history."""
            past = _port_value_at(_days_ago(days))
            if not past:
                return None, None
            return round((cur - past) / past * 100, 1), round(cur - past, 2)

        def _perf(days):
            pct, _ = _perf_and_val(days)
            return pct

        def _pr(pct, val, days=None):
            reason = None
            if pct is None:
                reason = "insufficient_history" if days and days > 5 else "unavailable"
            return {"pct": pct, "value": val, "reason": reason}

        pct_5d,  val_5d  = _perf_and_val(5)
        pct_1m,  val_1m  = _perf_and_val(30)
        pct_6m,  val_6m  = _perf_and_val(182)
        pct_1y,  val_1y  = _perf_and_val(365)

        change_today_val = round(sum(p["_shares"] * (p.get("change") or 0) for p in positions), 2)
        change_pct_1d_r  = round(change_pct_1d, 2)

        period_returns = {
            "1D": _pr(change_pct_1d_r, change_today_val),
            "5D": _pr(pct_5d,  val_5d,  days=5),
            "1M": _pr(pct_1m,  val_1m,  days=30),
            "6M": _pr(pct_6m,  val_6m,  days=182),
            "1Y": _pr(pct_1y,  val_1y,  days=365),
        }

        return {
            "perf_1d": round(change_pct_1d, 1),
            "perf_5d": pct_5d,
            "perf_1m": pct_1m,
            "perf_6m": pct_6m,
            "perf_1y": pct_1y,
            "period_returns": period_returns,
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

            # Use FMP company name when available, fall back to ticker
            _company_name = (pos.get("_name") or t) if pos else t

            if is_etf:
                results.append({
                    "ticker":       t,
                    "company":      _company_name,
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
                    "company":      _company_name,
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
        """Load holdings from the canonical portfolio store (single source of truth).

        Primary:  portfolio_store.load_active_holdings() — always canonical
        Fallback: direct file read of portfolio_file (backward compat)
        """
        # 1. Canonical store — always the right answer
        try:
            from data.portfolio_store import load_active_holdings as _canon
            holdings = _canon()
            valid = [h for h in holdings if float(h.get("shares") or 0) > 0]
            syms  = [h.get("ticker") for h in valid[:25]]
            print(
                f"[CAELYN] _load_holdings  source=portfolio_store  "
                f"count={len(valid)}  symbols={syms}"
            )
            return valid
        except Exception as e:
            print(f"[CAELYN] portfolio_store load error: {e}")

        # 2. Direct file fallback (backward compat — should rarely fire)
        for path in [portfolio_file]:
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
                print(
                    f"[CAELYN] _load_holdings  source=file_fallback  "
                    f"file={path.name}  raw={len(holdings)}  valid={len(result)}"
                )
                return result
            except Exception as e:
                print(f"[CAELYN] Holdings load error ({path}): {e}")

        print(f"[CAELYN] _load_holdings  result=empty — no canonical data")
        return []

    def _empty(self) -> dict:
        _empty_charts = {"1D": [], "5D": [], "1M": [], "6M": [], "1Y": []}
        return {
            "portfolio": {
                "value": 0, "change_today": 0, "change_pct_today": 0,
                "perf_1d": None, "perf_5d": None, "perf_1m": None,
                "perf_6m": None, "perf_1y": None,
                "total_return_pct": 0, "total_return_value": 0,
                "sentiment": "NEUTRAL", "market_status": _market_status_et(),
            },
            "positions_count": 0, "holdings": [],
            "performance_chart": [],
            "performance_charts": _empty_charts,
            "asset_allocation": [],
            "theme_mapping": [],
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
            "_holdings_sig": "",
        }
