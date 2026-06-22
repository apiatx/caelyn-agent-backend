"""
Market data provider for sector ETFs.
Quotes:  Tradier (real-time, batch-efficient) → Finnhub fallback
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
_executor  = ThreadPoolExecutor(max_workers=40)  # large pool for theme RS dynamic stock universe


def _tradier_key() -> str:
    return os.getenv("TRADIER_API_KEY", "")


def _tradier_base() -> str:
    sandbox = os.getenv("TRADIER_SANDBOX", "false").lower() in ("true", "1", "yes")
    return "https://sandbox.tradier.com/v1" if sandbox else "https://api.tradier.com/v1"


def _finnhub_key() -> str:
    return os.getenv("FINNHUB_API_KEY", "")


def _normalize_tradier_quote(sym: str, q: dict) -> dict:
    """
    Map Tradier quote fields to the canonical sector-rotation output contract:
    {price, change_1d_pct, prev_close, day_high, day_low}
    Exact same shape as the previous Finnhub contract — no schema leakage.
    """
    last = q.get("last")
    prevclose = q.get("prevclose")
    change_pct = q.get("change_percentage")
    if change_pct is None and last is not None and prevclose is not None and prevclose != 0:
        try:
            change_pct = (last - prevclose) / prevclose * 100
        except (TypeError, ZeroDivisionError):
            change_pct = None
    return {
        "price":        last,
        "change_1d_pct": change_pct,
        "prev_close":   prevclose,
        "day_high":     q.get("high"),
        "day_low":      q.get("low"),
    }


async def _tradier_quotes_batch(tickers: list[str]) -> dict[str, dict]:
    """
    Fetch real-time quotes for all tickers in one Tradier batch call.
    Returns {ticker: normalized_quote_dict}.
    """
    key = _tradier_key()
    if not key:
        return {}

    cache_key = f"sr_td_batch:{'_'.join(sorted(tickers))}"
    hit = cache.get(cache_key)
    if hit is not None:
        return hit

    symbols_str = ",".join(t.upper() for t in tickers)
    base = _tradier_base()
    try:
        async with httpx.AsyncClient(timeout=10) as session:
            resp = await session.get(
                f"{base}/markets/quotes",
                headers={
                    "Authorization": f"Bearer {key}",
                    "Accept": "application/json",
                },
                params={"symbols": symbols_str, "greeks": "false"},
            )
        if resp.status_code != 200:
            print(f"[SR][Tradier] batch quote HTTP {resp.status_code}")
            return {}
        data = resp.json()
        quotes_raw = data.get("quotes", {})
        quote_list = quotes_raw.get("quote", []) if isinstance(quotes_raw, dict) else []
        if isinstance(quote_list, dict):
            quote_list = [quote_list]

        result: dict[str, dict] = {}
        for q in quote_list:
            sym = (q.get("symbol") or "").upper()
            if sym:
                result[sym] = _normalize_tradier_quote(sym, q)

        cache.set(cache_key, result, _QUOTE_TTL)
        return result
    except Exception as e:
        print(f"[SR][Tradier] batch quote error: {e}")
        return {}


async def _finnhub_quote_single(ticker: str, session: httpx.AsyncClient) -> tuple[str, dict]:
    """Fetch real-time quote from Finnhub for a single ticker (fallback path)."""
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
    """
    Fetch real-time quotes for all sector + benchmark tickers.
    Primary: Tradier batch (single HTTP call for all tickers).
    Fallback: Finnhub individual calls (if Tradier unavailable or partial miss).
    Output contract is identical to the previous Finnhub-only implementation:
    {ticker: {price, change_1d_pct, prev_close, day_high, day_low}}
    """
    result: dict[str, dict] = {}

    tradier_data = await _tradier_quotes_batch(ALL_TICKERS)
    if tradier_data:
        result.update(tradier_data)

    missing = [t for t in ALL_TICKERS if t not in result or not result[t].get("price")]
    if missing:
        async with httpx.AsyncClient() as session:
            fallbacks = await asyncio.gather(
                *[_finnhub_quote_single(t, session) for t in missing],
                return_exceptions=True,
            )
        for item in fallbacks:
            if not isinstance(item, Exception):
                t, q = item
                if q:
                    result[t] = q

    return {t: q for t, q in result.items() if q}


def _yfinance_history_sync(ticker: str, days: int = 400) -> list[dict]:
    """Synchronous yfinance fetch — run in executor."""
    cache_key = f"sr_yf_hist:{ticker}:{days}"
    hit = cache.get(cache_key)
    if hit is not None:
        return hit
    try:
        import yfinance as yf
        period = "5y" if days > 1200 else "2y" if days > 252 else "1y"
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
