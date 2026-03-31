"""
Market data provider for sector ETFs.
Quotes:  Finnhub (real-time)
History: yfinance (daily bars, comprehensive 1Y+)
"""
from __future__ import annotations

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Optional

import httpx

from data.cache import cache, FINNHUB_TTL
from services.sector_rotation.schemas import SECTOR_ETF_MAP

SECTOR_TICKERS = list(SECTOR_ETF_MAP.keys())
BENCH_TICKERS  = ["SPY", "QQQ"]
ALL_TICKERS    = SECTOR_TICKERS + BENCH_TICKERS

_HIST_TTL = 3600   # 1h — history doesn't change intraday
_QUOTE_TTL = 120
_executor  = ThreadPoolExecutor(max_workers=13)  # one worker per ticker for max parallelism


def _finnhub_key() -> str:
    return os.getenv("FINNHUB_API_KEY", "")


async def _finnhub_quote(ticker: str, session: httpx.AsyncClient) -> tuple[str, dict]:
    """Fetch real-time quote from Finnhub for a single ticker."""
    cache_key = f"sr_fh_q:{ticker}"
    hit = cache.get(cache_key)
    if hit is not None:
        return ticker, hit
    key = _finnhub_key()
    if not key:
        return ticker, {}
    try:
        resp = await session.get(
            "https://finnhub.io/api/v1/quote",
            params={"symbol": ticker, "token": key},
            timeout=8,
        )
        if resp.status_code != 200:
            return ticker, {}
        d = resp.json()
        current = d.get("c")
        prev    = d.get("pc")
        change_pct = ((current - prev) / prev * 100) if current and prev and prev != 0 else None
        result = {
            "price":        current,
            "change_1d_pct": change_pct,
            "prev_close":   prev,
            "day_high":     d.get("h"),
            "day_low":      d.get("l"),
        }
        cache.set(cache_key, result, _QUOTE_TTL)
        return ticker, result
    except Exception as e:
        print(f"[SR][Finnhub] {ticker}: {e}")
        return ticker, {}


async def fetch_etf_quotes() -> dict[str, dict]:
    """Fetch real-time quotes for all sector + benchmark tickers via Finnhub."""
    async with httpx.AsyncClient() as session:
        results = await asyncio.gather(
            *[_finnhub_quote(t, session) for t in ALL_TICKERS],
            return_exceptions=True,
        )
    return {
        t: q
        for item in results
        if not isinstance(item, Exception)
        for t, q in [item]
        if q
    }


def _yfinance_history_sync(ticker: str, days: int = 400) -> list[dict]:
    """Synchronous yfinance fetch — run in executor."""
    cache_key = f"sr_yf_hist:{ticker}:{days}"
    hit = cache.get(cache_key)
    if hit is not None:
        return hit
    try:
        import yfinance as yf
        period = "2y" if days > 252 else "1y"
        tk   = yf.Ticker(ticker)
        hist = tk.history(period=period, auto_adjust=True)
        if hist.empty:
            return []
        rows = []
        for ts, row in hist.iterrows():
            date_str = ts.strftime("%Y-%m-%d")
            close    = row.get("Close")
            if close is not None and close > 0:
                rows.append({"date": date_str, "close": float(close)})
        rows.sort(key=lambda r: r["date"])
        if rows:
            cache.set(cache_key, rows, _HIST_TTL)
        return rows
    except Exception as e:
        print(f"[SR][yfinance] {ticker}: {e}")
        return []


async def fetch_etf_history(ticker: str, days: int = 400) -> list[dict]:
    """Async wrapper for yfinance historical fetch."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, _yfinance_history_sync, ticker, days)


async def fetch_all_histories() -> dict[str, list[dict]]:
    """Fetch ~1Y of daily closes for all sector + benchmark tickers."""
    results = await asyncio.gather(
        *[fetch_etf_history(t, days=400) for t in ALL_TICKERS],
        return_exceptions=True,
    )
    out: dict[str, list[dict]] = {}
    for t, r in zip(ALL_TICKERS, results):
        if isinstance(r, list):
            out[t] = r
        else:
            print(f"[SR] History error {t}: {r}")
            out[t] = []
    return out
