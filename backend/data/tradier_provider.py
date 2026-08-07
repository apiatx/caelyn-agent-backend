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



# Cache TTLs (seconds)
_CHAIN_TTL = 90           # option chains — time-sensitive
_EXPIRATIONS_TTL = 600    # expirations change rarely intraday
_QUOTE_TTL = 60           # quotes — fast refresh
_HISTORY_TTL = 3600       # daily bars — EOD data
_TIMESALES_TTL = 120      # intraday ticks

_TIMEOUT = 12  # seconds per request

# ── Future-based request coalescing ───────────────────────────────────────────
# When two coroutines simultaneously request the same expiration list or chain,
# only one Tradier call fires.  The second awaits the first's Future.
# Keys are evicted from the dict in the finally block of the owning coroutine.
_EXPIRY_FUTURES:     dict[str, "asyncio.Future[list[str]]"] = {}
_CHAIN_FUTURES:      dict[str, "asyncio.Future[dict]"]      = {}
_TIMESALES_FUTURES:  dict[str, "asyncio.Future[list[dict]]"] = {}
_HISTORY_FUTURES:    dict[str, "asyncio.Future[list[dict]]"] = {}

# Lifetime counters — read by /api/rate-status
_coalesced_expiry_count:     int = 0
_coalesced_chain_count:      int = 0
_coalesced_timesales_count:  int = 0
_coalesced_history_count:    int = 0
_last_429_at: float | None   = None  # epoch seconds

# ── Process-wide singleton provider (lazy) ────────────────────────────────────
# Constructed on first call to get_provider(); avoids importing tradier_provider
# just to instantiate TradierProvider everywhere.
_SINGLETON_PROVIDER: "TradierProvider | None" = None


def get_provider() -> "TradierProvider | None":
    """Return the process-wide singleton TradierProvider, or None if no API key.

    Uses TRADIER_API_KEY and TRADIER_SANDBOX env vars.  The singleton is
    constructed lazily so modules that import this file during test
    bootstrapping don't require a real key.

    TRADIER_SANDBOX is read at first call — consistent with how
    market_data_service.py reads it via config.TRADIER_SANDBOX so both
    singletons target the same environment.
    """
    global _SINGLETON_PROVIDER
    if _SINGLETON_PROVIDER is None:
        key = os.environ.get("TRADIER_API_KEY", "")
        if not key:
            return None
        sandbox = os.environ.get("TRADIER_SANDBOX", "false").lower() in ("true", "1", "yes")
        _SINGLETON_PROVIDER = TradierProvider(api_key=key, sandbox=sandbox)
    return _SINGLETON_PROVIDER


# ── Process-wide Tradier rate limiter ─────────────────────────────────────────
# Tradier production: ~120 req/min for market-data endpoints.
# We cap conservatively below that ceiling.
#
# Configurable via env:  TRADIER_MARKET_DATA_RPM (int, default 110, max 110)
# Raising above 110 risks 429s on the Tradier sandbox plan.
# Do NOT use this constant for /trade or order-management endpoints.
_TRADIER_MARKET_DATA_RPM_DEFAULT = 110
_TRADIER_MARKET_DATA_RPM_MAX     = 110
_TRADIER_MARKET_DATA_RPM: int = min(
    _TRADIER_MARKET_DATA_RPM_MAX,
    max(1, int(os.environ.get("TRADIER_MARKET_DATA_RPM", _TRADIER_MARKET_DATA_RPM_DEFAULT)))
)


class _TradierRateLimiter:
    """Async sliding-window rate limiter. Safe for a single asyncio event loop."""

    def __init__(self, max_calls: int = 100, window_seconds: float = 60.0):
        self._max = max_calls
        self._window = window_seconds
        self._timestamps: list[float] = []
        self._lock = asyncio.Lock()
        self.total_calls: int = 0
        self.total_throttled: int = 0

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = asyncio.get_event_loop().time()
                cutoff = now - self._window
                self._timestamps = [t for t in self._timestamps if t > cutoff]
                if len(self._timestamps) < self._max:
                    self._timestamps.append(now)
                    self.total_calls += 1
                    return
                # Over limit — calculate minimum wait and release lock before sleeping
                wait_for = (self._timestamps[0] + self._window) - now + 0.05
            self.total_throttled += 1
            print(
                f"[TRADIER_LIMITER] {len(self._timestamps)}/{self._max} calls in window "
                f"— throttling {wait_for:.2f}s"
            )
            await asyncio.sleep(wait_for)

    async def try_acquire_background(self, reserve: int = 5) -> bool:
        """Atomically try to reserve one slot for a background caller.

        Returns True and records the slot if the current window has enough headroom
        to accommodate this call while preserving *reserve* slots for interactive
        (non-background) traffic.

        Returns False immediately without sleeping if headroom is insufficient.
        NEVER blocks. NEVER sleeps. Safe to call from non-blocking background paths
        (schedulers, warmup loops, supplement scans).

        Callers that receive False must defer the physical request and use their
        existing cache/LKG/null contract — they must NOT fall through to unmetered HTTP.
        """
        async with self._lock:
            now = asyncio.get_event_loop().time()
            cutoff = now - self._window
            self._timestamps = [t for t in self._timestamps if t > cutoff]
            # Need at least reserve + 1 slots free: 1 for this call + reserve for interactive.
            if len(self._timestamps) + reserve + 1 <= self._max:
                self._timestamps.append(now)
                self.total_calls += 1
                return True
            return False

    def is_saturated(self) -> bool:
        """Non-blocking check: True when the rate window is full.

        Reads without acquiring the lock — intentionally approximate.
        Used by callers that want to skip a live call and fall back to LKG
        rather than block for up to ``window_seconds``.
        """
        try:
            now = asyncio.get_event_loop().time()
        except RuntimeError:
            return False
        recent = [t for t in self._timestamps if t > now - self._window]
        return len(recent) >= self._max

    def remaining_capacity(self) -> int:
        """Approximate remaining global slots in the current 60s window.

        Lock-free read — intentionally approximate.  Returns 0 when the
        event loop is not running.
        """
        try:
            now = asyncio.get_event_loop().time()
        except RuntimeError:
            return 0
        recent = len([t for t in self._timestamps if t > now - self._window])
        return max(0, self._max - recent)

    def can_start_background_batch(self, estimated_calls: int, reserve: int = 5) -> bool:
        """Check whether a background batch of *estimated_calls* may proceed
        while preserving at least *reserve* slots for request-driven traffic.

        Returns False when estimated_calls is zero or negative (unknown demand).
        Uses lock-free approximate reads — safe for pre-cycle gating.
        """
        if estimated_calls <= 0:
            return False
        return self.remaining_capacity() >= estimated_calls + reserve

    def status(self) -> dict:
        try:
            now = asyncio.get_event_loop().time()
        except RuntimeError:
            now = 0.0
        recent = [t for t in self._timestamps if t > now - 60]
        return {
            "calls_last_60s": len(recent),
            "limit_per_60s": self._max,
            "headroom": max(0, self._max - len(recent)),
            "total_calls_lifetime": self.total_calls,
            "total_throttled_lifetime": self.total_throttled,
        }


# Singleton — imported by other modules that need to report into the same counter
TRADIER_LIMITER = _TradierRateLimiter(max_calls=_TRADIER_MARKET_DATA_RPM, window_seconds=60.0)
print(f"[TRADIER_LIMITER] market-data cap configured: {_TRADIER_MARKET_DATA_RPM} req/min "
      f"(env TRADIER_MARKET_DATA_RPM={os.environ.get('TRADIER_MARKET_DATA_RPM', 'unset')}, "
      f"max={_TRADIER_MARKET_DATA_RPM_MAX})")


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

    async def _get_preadmitted(self, path: str, params: dict | None = None) -> dict | list | None:
        """Execute the HTTP portion of a Tradier request WITHOUT acquiring the rate limiter.

        *** PROVIDER-INTERNAL METHOD — DO NOT CALL FROM SERVICE CODE ***

        This method is an implementation detail of get_timesales_background() and
        get_history_background().  Service modules must call those methods, which
        own the full admission sequence (budget → try_acquire_background → record_call
        → _get_preadmitted) in one atomic scope.

        Calling _get_preadmitted() from outside the provider defeats the purpose of
        the boundary: it splits admission accounting from HTTP execution, creates the
        same pre-reservation timing defect the 78a5793c correction fixed, and prevents
        the provider from maintaining correct coalescing state.

        Pre-condition (for internal callers only): the caller has ALREADY called
            TRADIER_LIMITER.try_acquire_background(reserve=N) → True
            data.tradier_budget.record_call(lane)
        immediately before invoking this method (no concurrency queue in between).

        Interactive paths must continue using _get(), which calls
        TRADIER_LIMITER.acquire() (blocking).

        Returns the parsed JSON body on HTTP 200, or None on error.
        Does NOT retry on 429 — background callers should defer to next cycle.
        """
        url = f"{self.base_url}{path}"
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                resp = await client.get(url, headers=self._headers(), params=params or {})
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                import time as _t
                global _last_429_at
                _last_429_at = _t.time()
                print(f"[TRADIER][bg] 429 on {path} — deferring (background, no retry)")
                return None
            print(f"[TRADIER][bg] {path} error {resp.status_code}: {resp.text[:300]}")
            return None
        except Exception as e:
            print(f"[TRADIER][bg] {path} exception: {e}")
            return None

    async def _get(self, path: str, params: dict | None = None) -> dict | list | None:
        """Generic GET with lane-budget pre-check, rate limiting, and 429 auto-retry.

        Lane budget is enforced only during active trading sessions (pre/regular/post-market).
        During off-hours and weekends the global TRADIER_LIMITER remains the sole ceiling —
        budget checks are skipped so warmup and background refresh calls are not needlessly
        deferred when no starvation risk exists.
        """
        import data.tradier_budget as _bgt
        _lane = _bgt.get_current_lane()
        try:
            from data.tradier_market_session import is_active_session as _is_active
            _enforce_budget = _bgt.FORCE_ENFORCE or _is_active()
        except Exception:
            _enforce_budget = True  # safe default: enforce if unsure
        if _enforce_budget and not _bgt.check_budget(_lane):
            _bgt.record_defer(_lane)
            print(f"[TRADIER_BUDGET] {_lane!r} lane over budget — deferring {path}")
            return None
        await TRADIER_LIMITER.acquire()
        _bgt.record_call(_lane)
        url = f"{self.base_url}{path}"
        for attempt in range(3):
            try:
                async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                    resp = await client.get(url, headers=self._headers(), params=params or {})
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code == 429:
                    import time as _t
                    global _last_429_at
                    _last_429_at = _t.time()
                    retry_after = int(resp.headers.get("Retry-After", "5"))
                    print(
                        f"[TRADIER] 429 rate limited on {path} — "
                        f"retrying in {retry_after}s (attempt {attempt + 1}/3)"
                    )
                    await asyncio.sleep(retry_after)
                    await TRADIER_LIMITER.acquire()
                    continue
                print(f"[TRADIER] {path} error {resp.status_code}: {resp.text[:300]}")
                return None
            except Exception as e:
                print(f"[TRADIER] {path} exception (attempt {attempt + 1}/3): {e}")
                if attempt < 2:
                    await asyncio.sleep(1.0)
                    continue
                return None
        return None

    def get_rate_status(self) -> dict:
        """Return current rate limiter status — calls in last 60s, headroom, etc."""
        return TRADIER_LIMITER.status()

    @staticmethod
    def get_coalescing_status() -> dict:
        """Return coalescing counters and active in-flight futures (for /api/rate-status)."""
        import time as _t
        return {
            "coalesced_expiry_requests_lifetime":     _coalesced_expiry_count,
            "coalesced_chain_requests_lifetime":      _coalesced_chain_count,
            "coalesced_timesales_requests_lifetime":  _coalesced_timesales_count,
            "coalesced_history_requests_lifetime":    _coalesced_history_count,
            "active_expiry_futures":    len(_EXPIRY_FUTURES),
            "active_chain_futures":     len(_CHAIN_FUTURES),
            "active_timesales_futures": len(_TIMESALES_FUTURES),
            "active_history_futures":   len(_HISTORY_FUTURES),
            "last_429_at":              _last_429_at,
            "last_429_ago_s":           (
                round(_t.time() - _last_429_at) if _last_429_at else None
            ),
        }

    # ── Option Expirations ──────────────────────────────────────────────
    async def get_option_expirations(self, symbol: str) -> list[str]:
        """
        Return list of expiration date strings (YYYY-MM-DD).

        Coalescing: if another coroutine is already fetching expirations for
        this symbol, awaits the same Future rather than firing a duplicate call.
        The owning coroutine writes the result and clears the Future entry.
        """
        global _coalesced_expiry_count
        symbol = symbol.upper()
        cache_key = f"tradier:expirations:{symbol}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        # In-flight coalescing
        if symbol in _EXPIRY_FUTURES:
            _coalesced_expiry_count += 1
            try:
                return await asyncio.shield(_EXPIRY_FUTURES[symbol])
            except Exception:
                return []

        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        _EXPIRY_FUTURES[symbol] = fut
        try:
            data = await self._get("/markets/options/expirations", {
                "symbol": symbol,
                "includeAllRoots": "true",
            })
            if not data:
                if not fut.done():
                    fut.set_result([])
                return []

            expirations = data.get("expirations", {})
            dates = expirations.get("date", []) if isinstance(expirations, dict) else []
            if isinstance(dates, str):
                dates = [dates]  # single expiration returned as string

            cache.set(cache_key, dates, _EXPIRATIONS_TTL)
            if not fut.done():
                fut.set_result(dates)
            return dates
        except Exception as exc:
            if not fut.done():
                fut.set_exception(exc)
            raise
        finally:
            _EXPIRY_FUTURES.pop(symbol, None)

    # ── Option Strikes ──────────────────────────────────────────────────
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
    async def get_option_chain(self, symbol: str, expiration: str) -> dict:
        """
        Fetch full chain with greeks/IV for one expiration.
        Returns {"calls": [...], "puts": [...], "baseSymbol": symbol}
        Each contract has: symbol, strike, bid, ask, last, volume, openInterest,
                          delta, gamma, theta, vega, rho, mid_iv, bid_iv, ask_iv, smv_vol

        Coalescing: if another coroutine is already fetching the same chain key,
        awaits the same Future rather than firing a duplicate Tradier call.
        """
        global _coalesced_chain_count
        symbol = symbol.upper()
        cache_key = f"tradier:chain:{symbol}:{expiration}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        coalesce_key = f"{symbol}:{expiration}"
        if coalesce_key in _CHAIN_FUTURES:
            _coalesced_chain_count += 1
            try:
                return await asyncio.shield(_CHAIN_FUTURES[coalesce_key])
            except Exception:
                return {"calls": [], "puts": [], "baseSymbol": symbol}

        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        _CHAIN_FUTURES[coalesce_key] = fut

        try:
            data = await self._get("/markets/options/chains", {
                "symbol": symbol,
                "expiration": expiration,
                "greeks": "true",
            })
            if not data:
                empty = {"calls": [], "puts": [], "baseSymbol": symbol}
                if not fut.done():
                    fut.set_result(empty)
                return empty

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
                    "option_type": opt.get("option_type"),
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
            if not fut.done():
                fut.set_result(result)
            return result
        except Exception as exc:
            if not fut.done():
                fut.set_exception(exc)
            raise
        finally:
            _CHAIN_FUTURES.pop(coalesce_key, None)

    # ── Alias for OptionsFlowEngine compatibility ───────────────────────

    async def get_full_chain_with_greeks(self, symbol: str, expiration: str) -> dict:
        """Drop-in replacement for PublicComProvider.get_full_chain_with_greeks.
        Tradier includes greeks inline in the chain response, so no extra calls needed."""
        return await self.get_option_chain(symbol, expiration)

    # ── Equity / Option Quotes ──────────────────────────────────────────
    async def get_quotes(self, symbols: list[str]) -> list[dict]:
        """
        Get real-time quotes for equities or options (pass OCC symbols for options).

        Uses per-ticker caching (tradier:quote:sym:{SYM}) so quotes fetched by ANY
        part of the site — watchlist, screener, portfolio, caelyn-terminal — are
        shared with every other caller without an extra Tradier API call.
        Only tickers not already in cache are batched into a single Tradier request.
        """
        if not symbols:
            return []

        syms_upper = [s.upper() for s in symbols]

        # ── Check per-ticker cache first ────────────────────────────────────
        result: list[dict] = []
        missing: list[str] = []
        for sym in syms_upper:
            hit = cache.get(f"tradier:quote:sym:{sym}")
            if hit is not None:
                result.append(hit)
            else:
                missing.append(sym)

        if not missing:
            return result   # 100% cache hit — zero Tradier calls

        # ── Batch-fetch only the tickers not in cache ───────────────────────
        data = await self._get(
            "/markets/quotes",
            {"symbols": ",".join(missing), "greeks": "false"},
        )
        if not data:
            return result   # Return whatever we already had from cache

        quotes = data.get("quotes", {})
        quote_list = quotes.get("quote", []) if isinstance(quotes, dict) else []
        if isinstance(quote_list, dict):
            quote_list = [quote_list]  # single-ticker response is a dict, not list

        for q in quote_list:
            sym = q.get("symbol")
            if not sym:
                continue
            entry = {
                "symbol":            sym,
                "description":       q.get("description"),
                "last":              _safe_float(q.get("last")),
                "bid":               _safe_float(q.get("bid")),
                "ask":               _safe_float(q.get("ask")),
                "change":            _safe_float(q.get("change")),
                "change_percentage": _safe_float(q.get("change_percentage")),
                "volume":            _safe_int(q.get("volume")),
                "average_volume":    _safe_int(q.get("average_volume")),
                "open":              _safe_float(q.get("open")),
                "high":              _safe_float(q.get("high")),
                "low":               _safe_float(q.get("low")),
                "close":             _safe_float(q.get("close")),
                "prevclose":         _safe_float(q.get("prevclose")),
                "week_52_high":      _safe_float(q.get("week_52_high")),
                "week_52_low":       _safe_float(q.get("week_52_low")),
                "type":              q.get("type"),
            }
            cache.set(f"tradier:quote:sym:{sym}", entry, _QUOTE_TTL)
            result.append(entry)

        if missing:
            print(
                f"[TRADIER_QUOTES] requested={len(syms_upper)} "
                f"cache_hits={len(syms_upper)-len(missing)} "
                f"fetched={len(missing)} tickers={missing}"
            )

        return result

    async def get_quote(self, symbol: str) -> dict | None:
        """Convenience: get a single quote."""
        quotes = await self.get_quotes([symbol])
        return quotes[0] if quotes else None

    # ── Historical Data (equity + option contracts) ─────────────────────
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

        Coalescing: concurrent requests for the same key share one in-flight Future.
        """
        global _coalesced_history_count
        symbol = symbol.upper()
        if not start:
            start = (date.today() - timedelta(days=365)).isoformat()
        if not end:
            end = date.today().isoformat()

        cache_key = f"tradier:history:{symbol}:{interval}:{start}:{end}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        # In-flight coalescing
        coalesce_key = cache_key
        if coalesce_key in _HISTORY_FUTURES:
            _coalesced_history_count += 1
            try:
                return await asyncio.shield(_HISTORY_FUTURES[coalesce_key])
            except Exception:
                return []

        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        _HISTORY_FUTURES[coalesce_key] = fut
        try:
            data = await self._get("/markets/history", {
                "symbol": symbol,
                "interval": interval,
                "start": start,
                "end": end,
            })
            if not data:
                if not fut.done():
                    fut.set_result([])
                return []

            history = data.get("history", {})
            if not history:
                if not fut.done():
                    fut.set_result([])
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
            if not fut.done():
                fut.set_result(bars)
            return bars
        except Exception as exc:
            if not fut.done():
                fut.set_exception(exc)
            raise
        finally:
            _HISTORY_FUTURES.pop(coalesce_key, None)

    async def get_history_background(
        self,
        symbol: str,
        interval: str = "daily",
        start: str | None = None,
        end: str | None = None,
        lane: str = "maintenance",
        reserve: int = 5,
    ) -> list[dict]:
        """Non-blocking background variant of get_history().

        Admission flow (no sleeping):
          1. Cache hit → return immediately (zero provider calls).
          2. In-flight coalescing → await existing Future (no new HTTP).
          3. check_budget(lane) → if full: record_defer, return [].
          4. TRADIER_LIMITER.try_acquire_background(reserve) → if False: return [].
          5. record_call(lane) → account the physical slot.
          6. _get_preadmitted() → HTTP (already admitted, no re-acquire).

        Returns [] on any admission failure. Caller must use existing cache/LKG.
        """
        global _coalesced_history_count
        import data.tradier_budget as _bgt

        symbol = symbol.upper()
        if not start:
            start = (date.today() - timedelta(days=365)).isoformat()
        if not end:
            end = date.today().isoformat()

        cache_key = f"tradier:history:{symbol}:{interval}:{start}:{end}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        # In-flight coalescing
        coalesce_key = cache_key
        if coalesce_key in _HISTORY_FUTURES:
            _coalesced_history_count += 1
            try:
                return await asyncio.shield(_HISTORY_FUTURES[coalesce_key])
            except Exception:
                return []

        # Non-blocking admission
        if not _bgt.check_budget(lane):
            _bgt.record_defer(lane)
            print(f"[TRADIER][bg/history] {lane!r} lane full — deferring {symbol}")
            return []

        admitted = await TRADIER_LIMITER.try_acquire_background(reserve=reserve)
        if not admitted:
            print(f"[TRADIER][bg/history] global headroom < {reserve+1} — deferring {symbol}")
            return []

        _bgt.record_call(lane)

        # Register in-flight future
        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        _HISTORY_FUTURES[coalesce_key] = fut
        try:
            data = await self._get_preadmitted("/markets/history", {
                "symbol": symbol,
                "interval": interval,
                "start": start,
                "end": end,
            })
            if not data:
                if not fut.done():
                    fut.set_result([])
                return []

            history = data.get("history", {})
            if not history:
                if not fut.done():
                    fut.set_result([])
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
            if not fut.done():
                fut.set_result(bars)
            return bars
        except Exception as exc:
            if not fut.done():
                fut.set_exception(exc)
            return []
        finally:
            _HISTORY_FUTURES.pop(coalesce_key, None)

    # ── Time and Sales (intraday ticks / intervals) ─────────────────────
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

        Coalescing: concurrent requests for the same symbol+interval+window share
        one in-flight Future rather than firing duplicate Tradier calls.
        """
        global _coalesced_timesales_count
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

        # In-flight coalescing
        coalesce_key = cache_key
        if coalesce_key in _TIMESALES_FUTURES:
            _coalesced_timesales_count += 1
            try:
                return await asyncio.shield(_TIMESALES_FUTURES[coalesce_key])
            except Exception:
                return []

        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        _TIMESALES_FUTURES[coalesce_key] = fut
        try:
            data = await self._get("/markets/timesales", params)
            if not data:
                if not fut.done():
                    fut.set_result([])
                return []

            series_data = data.get("series", {})
            if not series_data:
                if not fut.done():
                    fut.set_result([])
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
            if not fut.done():
                fut.set_result(ticks)
            return ticks
        except Exception as exc:
            if not fut.done():
                fut.set_exception(exc)
            raise
        finally:
            _TIMESALES_FUTURES.pop(coalesce_key, None)

    async def get_timesales_background(
        self,
        symbol: str,
        interval: str = "5min",
        start: str | None = None,
        end: str | None = None,
        lane: str = "maintenance",
        reserve: int = 5,
        extra_params: dict | None = None,
    ) -> list[dict]:
        """Non-blocking background variant of get_timesales().

        Admission flow (no sleeping):
          1. Cache hit → return immediately (zero provider calls).
          2. In-flight coalescing → await existing Future (no new HTTP).
          3. check_budget(lane) → if full: record_defer, return [].
          4. TRADIER_LIMITER.try_acquire_background(reserve) → if False: return [].
          5. record_call(lane) → account the physical slot.
          6. _get_preadmitted() → HTTP (already admitted, no re-acquire).

        The semaphore (_INTRADAY_SEM) must be acquired by the *caller* around
        this method call — not inside, so the semaphore is never held while
        waiting for admission.

        Returns [] on any admission failure. Caller must use existing cache/LKG.
        """
        global _coalesced_timesales_count
        import data.tradier_budget as _bgt

        symbol = symbol.upper()
        params: dict[str, str] = {"symbol": symbol, "interval": interval}
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        if extra_params:
            params.update(extra_params)

        cache_key = f"tradier:timesales:{symbol}:{interval}:{start}:{end}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        # In-flight coalescing — piggyback on an in-progress request for same key
        coalesce_key = cache_key
        if coalesce_key in _TIMESALES_FUTURES:
            _coalesced_timesales_count += 1
            try:
                return await asyncio.shield(_TIMESALES_FUTURES[coalesce_key])
            except Exception:
                return []

        # Non-blocking admission: lane budget check first (no global acquire yet)
        if not _bgt.check_budget(lane):
            _bgt.record_defer(lane)
            print(f"[TRADIER][bg/timesales] {lane!r} lane full — deferring {symbol}")
            return []

        admitted = await TRADIER_LIMITER.try_acquire_background(reserve=reserve)
        if not admitted:
            print(f"[TRADIER][bg/timesales] global headroom < {reserve+1} — deferring {symbol}")
            return []

        # Record the lane call AFTER successful global admission
        _bgt.record_call(lane)

        # Register in-flight future so concurrent calls coalesce
        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        _TIMESALES_FUTURES[coalesce_key] = fut
        try:
            data = await self._get_preadmitted("/markets/timesales", params)
            if not data:
                if not fut.done():
                    fut.set_result([])
                return []

            series_data = data.get("series", {})
            if not series_data:
                if not fut.done():
                    fut.set_result([])
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
            if not fut.done():
                fut.set_result(ticks)
            return ticks
        except Exception as exc:
            if not fut.done():
                fut.set_exception(exc)
            return []
        finally:
            _TIMESALES_FUTURES.pop(coalesce_key, None)

    # ── Option Lookup ───────────────────────────────────────────────────
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
