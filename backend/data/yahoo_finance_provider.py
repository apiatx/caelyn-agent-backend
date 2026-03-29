"""
Yahoo Finance provider — no API key required.

Used exclusively for DXY (US Dollar Index, ticker DX-Y.NYB on ICE).
FMP's DX-Y.NYB endpoint returns "No data" on the free tier; Yahoo Finance
delivers real-time quotes (15-min delayed during market hours) plus up to
5 years of daily history at zero cost.

Usage:
    from data.yahoo_finance_provider import YahooFinanceProvider
    yf = YahooFinanceProvider()
    quote = await yf.get_dxy()          # {price, change, change_pct, ...}
    hist  = await yf.get_dxy_history()  # [{date, value}, ...]
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


class YahooFinanceProvider:
    """Thin async wrapper around Yahoo Finance for DXY data."""

    DXY_SYMBOL = "DX-Y.NYB"

    @traceable(name="yahoo.get_dxy")
    async def get_dxy(self) -> dict:
        """
        Fetch the live DXY quote.

        Returns:
            {
                symbol: "DXY",
                price: float,           # current price (15-min delayed)
                change: float,          # change vs prior close
                change_pct: float,      # change % vs prior close
                prev_close: float,
                year_high: float,
                year_low: float,
                error: str | None,
            }
        """
        raw = await asyncio.to_thread(
            _fetch_yahoo_chart, self.DXY_SYMBOL, "1d", "5d"
        )
        try:
            result = raw["chart"]["result"][0]
            meta = result["meta"]
            closes = result["indicators"]["quote"][0]["close"]
            timestamps = result["timestamp"]

            valid = [
                (timestamps[i], closes[i])
                for i in range(len(closes))
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
                    change = round(price - prev_close, 3)
                    change_pct = round((price - prev_close) / prev_close * 100, 3)

            return {
                "symbol": "DXY",
                "price": round(price, 3) if price else None,
                "change": change,
                "change_pct": change_pct,
                "prev_close": round(prev_close, 3) if prev_close else None,
                "year_high": round(year_high, 3) if year_high else None,
                "year_low": round(year_low, 3) if year_low else None,
                "error": None,
            }
        except Exception as exc:
            print(f"[YahooFinance] DXY parse error: {exc}")
            return {"symbol": "DXY", "error": str(exc)}

    @traceable(name="yahoo.get_dxy_history")
    async def get_dxy_history(self, days: int = 252) -> list:
        """
        Fetch DXY daily close history.

        Args:
            days: approximate trading days (252 ≈ 1 year, 504 ≈ 2 years)

        Returns:
            [ {date: "YYYY-MM-DD", value: float}, ... ]  sorted ascending.
        """
        range_map = {
            252: "1y",
            504: "2y",
            756: "3y",
            1260: "5y",
        }
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
        try:
            result = raw["chart"]["result"][0]
            closes = result["indicators"]["quote"][0]["close"]
            timestamps = result["timestamp"]

            history = []
            for i in range(len(timestamps)):
                c = closes[i]
                if c is not None:
                    dt = datetime.fromtimestamp(timestamps[i]).strftime("%Y-%m-%d")
                    history.append({"date": dt, "value": round(c, 3)})

            return history
        except Exception as exc:
            print(f"[YahooFinance] DXY history parse error: {exc}")
            return []
