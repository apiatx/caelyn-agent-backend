"""
Tradier Market Data API provider for the Tradier page.

Covers: option chains (with greeks/IV), expirations, strikes, quotes,
        historical OHLC (equity + option contracts), time-and-sales.

Auth: Bearer token via TRADIER_API_KEY env var.
Rate limit: ~120 req/min on production, ~60 req/min sandbox.
"""
from __future__ import annotations

import asyncio
import os
from datetime import date, datetime, timedelta
from typing import Any

import httpx

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


# Cache TTLs (seconds)
_CHAIN_TTL = 90           # option chains — time-sensitive
_EXPIRATIONS_TTL = 600    # expirations change rarely intraday
_QUOTE_TTL = 60           # quotes — fast refresh
_HISTORY_TTL = 3600       # daily bars — EOD data
_TIMESALES_TTL = 120      # intraday ticks

_TIMEOUT = 12  # seconds per request


def _safe_float(v: Any) -> float | None:
    try:
        if v in (None, "", "-"):
            return None
        return float(v)
    except Exception:
        return None


def _safe_int(v: Any) -> int:
    try:
        if v in (None, "", "-"):
            return 0
        return int(float(v))
    except Exception:
        return 0


class TradierProvider:
    """
    Tradier Market Data API — sole data source for the Tradier page.
    Provides option chains with inline greeks/IV, historical bars,
    time-and-sales, and equity quotes.
    """

    def __init__(self, api_key: str, sandbox: bool = False):
        self.api_key = api_key
        self.base_url = (
            "https://sandbox.tradier.com/v1"
            if sandbox
            else "https://api.tradier.com/v1"
        )
        self._env = "sandbox" if sandbox else "production"
        print(f"[TRADIER] Provider initialized ({self._env})")

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
        }

    async def _get(self, path: str, params: dict | None = None) -> dict | list | None:
        """Generic GET with error handling."""
        url = f"{self.base_url}{path}"
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                resp = await client.get(url, headers=self._headers(), params=params or {})
            if resp.status_code == 200:
                return resp.json()
            print(f"[TRADIER] {path} error {resp.status_code}: {resp.text[:300]}")
            return None
        except Exception as e:
            print(f"[TRADIER] {path} exception: {e}")
            return None

    # ── Option Expirations ──────────────────────────────────────────────

    @traceable(name="tradier.get_option_expirations")
    async def get_option_expirations(self, symbol: str) -> list[str]:
        """Return list of expiration date strings (YYYY-MM-DD)."""
        symbol = symbol.upper()
        cache_key = f"tradier:expirations:{symbol}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        data = await self._get("/markets/options/expirations", {
            "symbol": symbol,
            "includeAllRoots": "true",
        })
        if not data:
            return []

        expirations = data.get("expirations", {})
        dates = expirations.get("date", []) if isinstance(expirations, dict) else []
        if isinstance(dates, str):
            dates = [dates]  # single expiration returned as string

        cache.set(cache_key, dates, _EXPIRATIONS_TTL)
        return dates

    # ── Option Strikes ──────────────────────────────────────────────────

    @traceable(name="tradier.get_option_strikes")
    async def get_option_strikes(self, symbol: str, expiration: str) -> list[float]:
        """Return sorted list of available strike prices."""
        symbol = symbol.upper()
        cache_key = f"tradier:strikes:{symbol}:{expiration}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        data = await self._get("/markets/options/strikes", {
            "symbol": symbol,
            "expiration": expiration,
        })
        if not data:
            return []

        strikes_data = data.get("strikes", {})
        strikes = strikes_data.get("strike", []) if isinstance(strikes_data, dict) else []
        if isinstance(strikes, (int, float)):
            strikes = [strikes]

        result = sorted([float(s) for s in strikes])
        cache.set(cache_key, result, _EXPIRATIONS_TTL)
        return result

    # ── Option Chain (with greeks) ──────────────────────────────────────

    @traceable(name="tradier.get_option_chain")
    async def get_option_chain(self, symbol: str, expiration: str) -> dict:
        """
        Fetch full chain with greeks/IV for one expiration.
        Returns {"calls": [...], "puts": [...], "baseSymbol": symbol}
        Each contract has: symbol, strike, bid, ask, last, volume, openInterest,
                          delta, gamma, theta, vega, rho, mid_iv, bid_iv, ask_iv, smv_vol
        """
        symbol = symbol.upper()
        cache_key = f"tradier:chain:{symbol}:{expiration}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        data = await self._get("/markets/options/chains", {
            "symbol": symbol,
            "expiration": expiration,
            "greeks": "true",
        })
        if not data:
            return {"calls": [], "puts": [], "baseSymbol": symbol}

        options = data.get("options", {})
        raw_list = options.get("option", []) if isinstance(options, dict) else []
        if isinstance(raw_list, dict):
            raw_list = [raw_list]  # single contract

        calls = []
        puts = []
        for opt in raw_list:
            greeks = opt.get("greeks") or {}
            contract = {
                "symbol": opt.get("symbol"),
                "strike": _safe_float(opt.get("strike")),
                "bid": _safe_float(opt.get("bid")),
                "ask": _safe_float(opt.get("ask")),
                "last": _safe_float(opt.get("last")),
                "volume": _safe_int(opt.get("volume")),
                "openInterest": _safe_int(opt.get("open_interest")),
                "delta": _safe_float(greeks.get("delta")),
                "gamma": _safe_float(greeks.get("gamma")),
                "theta": _safe_float(greeks.get("theta")),
                "vega": _safe_float(greeks.get("vega")),
                "rho": _safe_float(greeks.get("rho")),
                "iv": _safe_float(greeks.get("mid_iv")),
                "bid_iv": _safe_float(greeks.get("bid_iv")),
                "ask_iv": _safe_float(greeks.get("ask_iv")),
                "smv_vol": _safe_float(greeks.get("smv_vol")),
                "greeks_updated_at": greeks.get("updated_at"),
                # Extra Tradier fields
                "option_type": opt.get("option_type"),  # "call" or "put"
                "expiration_date": opt.get("expiration_date"),
                "trade_date": opt.get("trade_date"),
                "change": _safe_float(opt.get("change")),
                "change_percentage": _safe_float(opt.get("change_percentage")),
                "average_volume": _safe_int(opt.get("average_volume")),
                "last_volume": _safe_int(opt.get("last_volume")),
                "open": _safe_float(opt.get("open")),
                "high": _safe_float(opt.get("high")),
                "low": _safe_float(opt.get("low")),
                "close": _safe_float(opt.get("close")),
            }

            if opt.get("option_type") == "call":
                calls.append(contract)
            else:
                puts.append(contract)

        result = {"calls": calls, "puts": puts, "baseSymbol": symbol, "expiration": expiration}
        cache.set(cache_key, result, _CHAIN_TTL)
        return result

    # ── Alias for OptionsFlowEngine compatibility ───────────────────────

    async def get_full_chain_with_greeks(self, symbol: str, expiration: str) -> dict:
        """Drop-in replacement for PublicComProvider.get_full_chain_with_greeks.
        Tradier includes greeks inline in the chain response, so no extra calls needed."""
        return await self.get_option_chain(symbol, expiration)

    # ── Equity / Option Quotes ──────────────────────────────────────────

    @traceable(name="tradier.get_quotes")
    async def get_quotes(self, symbols: list[str]) -> list[dict]:
        """Get real-time quotes for equities or options (pass OCC symbols for options)."""
        if not symbols:
            return []

        symbols_str = ",".join(s.upper() for s in symbols)
        cache_key = f"tradier:quotes:{symbols_str[:80]}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        data = await self._get("/markets/quotes", {"symbols": symbols_str, "greeks": "false"})
        if not data:
            return []

        quotes = data.get("quotes", {})
        quote_list = quotes.get("quote", []) if isinstance(quotes, dict) else []
        if isinstance(quote_list, dict):
            quote_list = [quote_list]

        result = []
        for q in quote_list:
            result.append({
                "symbol": q.get("symbol"),
                "description": q.get("description"),
                "last": _safe_float(q.get("last")),
                "bid": _safe_float(q.get("bid")),
                "ask": _safe_float(q.get("ask")),
                "change": _safe_float(q.get("change")),
                "change_percentage": _safe_float(q.get("change_percentage")),
                "volume": _safe_int(q.get("volume")),
                "average_volume": _safe_int(q.get("average_volume")),
                "open": _safe_float(q.get("open")),
                "high": _safe_float(q.get("high")),
                "low": _safe_float(q.get("low")),
                "close": _safe_float(q.get("close")),
                "prevclose": _safe_float(q.get("prevclose")),
                "week_52_high": _safe_float(q.get("week_52_high")),
                "week_52_low": _safe_float(q.get("week_52_low")),
                "type": q.get("type"),  # "stock", "option", "etf"
            })

        cache.set(cache_key, result, _QUOTE_TTL)
        return result

    async def get_quote(self, symbol: str) -> dict | None:
        """Convenience: get a single quote."""
        quotes = await self.get_quotes([symbol])
        return quotes[0] if quotes else None

    # ── Historical Data (equity + option contracts) ─────────────────────

    @traceable(name="tradier.get_history")
    async def get_history(
        self,
        symbol: str,
        interval: str = "daily",
        start: str | None = None,
        end: str | None = None,
    ) -> list[dict]:
        """
        Historical OHLCV bars. Works for both equities AND option OCC symbols.
        interval: daily, weekly, monthly
        Tradier covers lifetime for equities; option history available for active contracts.
        """
        symbol = symbol.upper()
        if not start:
            start = (date.today() - timedelta(days=365)).isoformat()
        if not end:
            end = date.today().isoformat()

        cache_key = f"tradier:history:{symbol}:{interval}:{start}:{end}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        data = await self._get("/markets/history", {
            "symbol": symbol,
            "interval": interval,
            "start": start,
            "end": end,
        })
        if not data:
            return []

        history = data.get("history", {})
        if not history:
            return []
        days = history.get("day", [])
        if isinstance(days, dict):
            days = [days]

        bars = []
        for d in days:
            bars.append({
                "date": d.get("date"),
                "open": _safe_float(d.get("open")),
                "high": _safe_float(d.get("high")),
                "low": _safe_float(d.get("low")),
                "close": _safe_float(d.get("close")),
                "volume": _safe_int(d.get("volume")),
            })

        cache.set(cache_key, bars, _HISTORY_TTL)
        return bars

    # ── Time and Sales (intraday ticks / intervals) ─────────────────────

    @traceable(name="tradier.get_timesales")
    async def get_timesales(
        self,
        symbol: str,
        interval: str = "5min",
        start: str | None = None,
        end: str | None = None,
    ) -> list[dict]:
        """
        Intraday time-and-sales data. Works for equities and option OCC symbols.
        interval: tick, 1min, 5min, 15min
        Returns list of {timestamp, open, high, low, close, volume, vwap}.
        """
        symbol = symbol.upper()
        params: dict[str, str] = {"symbol": symbol, "interval": interval}
        if start:
            params["start"] = start
        if end:
            params["end"] = end

        cache_key = f"tradier:timesales:{symbol}:{interval}:{start}:{end}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        data = await self._get("/markets/timesales", params)
        if not data:
            return []

        series_data = data.get("series", {})
        if not series_data:
            return []
        raw = series_data.get("data", [])
        if isinstance(raw, dict):
            raw = [raw]

        ticks = []
        for t in raw:
            ticks.append({
                "timestamp": t.get("timestamp"),
                "open": _safe_float(t.get("open")),
                "high": _safe_float(t.get("high")),
                "low": _safe_float(t.get("low")),
                "close": _safe_float(t.get("close")),
                "volume": _safe_int(t.get("volume")),
                "vwap": _safe_float(t.get("vwap")),
            })

        cache.set(cache_key, ticks, _TIMESALES_TTL)
        return ticks

    # ── Option Lookup ───────────────────────────────────────────────────

    @traceable(name="tradier.lookup_options")
    async def lookup_options(self, underlying: str) -> list[str]:
        """Lookup all OCC option symbols for an underlying."""
        data = await self._get("/markets/options/lookup", {"underlying": underlying.upper()})
        if not data:
            return []
        symbols = data.get("symbols", [])
        if isinstance(symbols, dict):
            options = symbols.get("options", [])
            if isinstance(options, dict):
                options = [options]
            return [o.get("symbol") for o in options if o.get("symbol")]
        return []

    # ── Market Clock ────────────────────────────────────────────────────

    async def get_market_clock(self) -> dict:
        """Get market status (open/closed), next open/close times."""
        data = await self._get("/markets/clock")
        if not data:
            return {}
        return data.get("clock", {})
