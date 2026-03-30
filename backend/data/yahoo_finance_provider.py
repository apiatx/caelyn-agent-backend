"""
Yahoo Finance provider — no API key required.

Covers:
  - DXY (US Dollar Index, DX-Y.NYB)
  - Generic quote fetch for any Yahoo Finance ticker (BTC-USD, ^VIX, TLT …)
  - Generic daily history for any ticker

Usage:
    from data.yahoo_finance_provider import YahooFinanceProvider
    yf = YahooFinanceProvider()
    quote = await yf.get_dxy()              # {price, change, change_pct, …}
    hist  = await yf.get_dxy_history()      # [{date, value}, …]
    q     = await yf.get_quote("BTC-USD")   # generic quote
    h     = await yf.get_history("BTC-USD") # [{date, close}, …]
"""

import asyncio
import json
import urllib.request
from datetime import datetime
from typing import Optional

try:
    from langsmith import traceable
except ImportError:
    def traceable(*args, **kwargs):
        def _noop(fn): return fn
        if args and callable(args[0]): return args[0]
        return _noop

_YAHOO_CHART = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
_HEADERS = {"User-Agent": "Mozilla/5.0"}


def _fetch_yahoo_chart(symbol: str, interval: str = "1d", range_: str = "5d") -> dict:
    """Synchronous Yahoo Finance chart fetch (runs inside asyncio.to_thread)."""
    url = f"{_YAHOO_CHART.format(symbol=symbol)}?interval={interval}&range={range_}"
    req = urllib.request.Request(url, headers=_HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except Exception as exc:
        print(f"[YahooFinance] fetch error for {symbol}: {exc}")
        return {}


def _parse_quote(raw: dict, display_symbol: str) -> dict:
    """Parse a Yahoo Finance chart response into a uniform quote dict."""
    try:
        result = raw["chart"]["result"][0]
        meta = result["meta"]
        closes = result["indicators"]["quote"][0].get("close", [])
        timestamps = result.get("timestamp", [])

        valid = [
            (timestamps[i], closes[i])
            for i in range(min(len(timestamps), len(closes)))
            if closes[i] is not None
        ]

        price = meta.get("regularMarketPrice")
        year_high = meta.get("fiftyTwoWeekHigh")
        year_low = meta.get("fiftyTwoWeekLow")

        change: Optional[float] = None
        change_pct: Optional[float] = None
        prev_close: Optional[float] = None

        if len(valid) >= 2:
            prev_close = valid[-2][1]
            if price and prev_close:
                change = round(price - prev_close, 4)
                change_pct = round((price - prev_close) / prev_close * 100, 4)
        elif meta.get("previousClose"):
            prev_close = meta.get("previousClose")
            if price and prev_close:
                change = round(price - prev_close, 4)
                change_pct = round((price - prev_close) / prev_close * 100, 4)

        # round sensibly based on price magnitude
        def _r(v):
            if v is None: return None
            if v > 10000: return round(v, 0)
            if v > 100:   return round(v, 2)
            if v > 1:     return round(v, 4)
            return round(v, 6)

        return {
            "symbol":     display_symbol,
            "price":      _r(price),
            "change":     _r(change),
            "change_pct": round(change_pct, 3) if change_pct is not None else None,
            "prev_close": _r(prev_close),
            "year_high":  _r(year_high),
            "year_low":   _r(year_low),
            "error":      None,
        }
    except Exception as exc:
        print(f"[YahooFinance] parse_quote error for {display_symbol}: {exc}")
        return {"symbol": display_symbol, "error": str(exc)}


def _parse_history(raw: dict) -> list[dict]:
    """Parse Yahoo Finance chart response into [{date, close}, …]."""
    try:
        result = raw["chart"]["result"][0]
        closes = result["indicators"]["quote"][0].get("close", [])
        timestamps = result.get("timestamp", [])
        history = []
        for i in range(min(len(timestamps), len(closes))):
            c = closes[i]
            if c is not None:
                dt = datetime.fromtimestamp(timestamps[i]).strftime("%Y-%m-%d")
                history.append({"date": dt, "close": c})
        return history
    except Exception as exc:
        print(f"[YahooFinance] parse_history error: {exc}")
        return []


class YahooFinanceProvider:
    """Async wrapper around Yahoo Finance — DXY + generic ticker support."""

    DXY_SYMBOL = "DX-Y.NYB"

    # ── DXY ──────────────────────────────────────────────────────────────

    @traceable(name="yahoo.get_dxy")
    async def get_dxy(self) -> dict:
        """Fetch the live DXY quote (backward-compatible return shape)."""
        raw = await asyncio.to_thread(
            _fetch_yahoo_chart, self.DXY_SYMBOL, "1d", "5d"
        )
        q = _parse_quote(raw, "DXY")
        # Re-expose under the old key names expected by downstream callers
        q["change_pct"] = q.get("change_pct")   # already set
        return q

    @traceable(name="yahoo.get_dxy_history")
    async def get_dxy_history(self, days: int = 252) -> list:
        """Fetch DXY daily close history. Returns [{date, value}, …]."""
        range_map = {252: "1y", 504: "2y", 756: "3y", 1260: "5y"}
        range_ = "1y"
        for threshold, r in sorted(range_map.items()):
            if days <= threshold:
                range_ = r
                break
        else:
            range_ = "5y"

        raw = await asyncio.to_thread(
            _fetch_yahoo_chart, self.DXY_SYMBOL, "1d", range_
        )
        bars = _parse_history(raw)
        return [{"date": b["date"], "value": round(b["close"], 3)} for b in bars]

    # ── Generic quote ─────────────────────────────────────────────────────

    @traceable(name="yahoo.get_quote")
    async def get_quote(self, yahoo_symbol: str, display_symbol: str | None = None) -> dict:
        """
        Fetch a quote for any Yahoo Finance ticker.

        Examples:
            yf.get_quote("BTC-USD",  "BTC")
            yf.get_quote("^VIX",     "VIX")
            yf.get_quote("ETH-USD",  "ETH")
            yf.get_quote("TLT")
        """
        sym = yahoo_symbol.upper()
        label = display_symbol or sym
        raw = await asyncio.to_thread(_fetch_yahoo_chart, sym, "1d", "5d")
        return _parse_quote(raw, label)

    # ── Generic history ───────────────────────────────────────────────────

    @traceable(name="yahoo.get_history")
    async def get_history(
        self,
        yahoo_symbol: str,
        range_: str = "1y",
    ) -> list[dict]:
        """
        Fetch daily OHLC history for any Yahoo Finance ticker.

        Returns [{date: "YYYY-MM-DD", close: float}, …] sorted ascending.

        range_ options: 1mo, 3mo, 6mo, 1y, 2y, 5y
        """
        raw = await asyncio.to_thread(
            _fetch_yahoo_chart, yahoo_symbol.upper(), "1d", range_
        )
        return _parse_history(raw)
