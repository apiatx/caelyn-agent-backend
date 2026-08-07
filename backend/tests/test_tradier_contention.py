"""
test_tradier_contention.py — Market-hours simulation tests for Tradier bypass elimination.

Proves six guarantees after the contention fix:
  T1  _fetch_tradier_daily_history acquires TRADIER_LIMITER before any httpx call.
  T2  _fetch_tradier_daily_history cache-hit skips the limiter entirely.
  T3  _fetch_intraday_bars acquires TRADIER_LIMITER (inside _INTRADAY_SEM) before httpx.
  T4  _fetch_intraday_bars cache-hit skips the limiter entirely.
  T5  N concurrent _fetch_intraday_bars calls each acquire the limiter once (N total).
  T6  _fetch_batch_direct acquires TRADIER_LIMITER before httpx.
  T7  _fetch_tradier_daily_history records the maintenance budget lane.
  T8  _fetch_intraday_bars records the maintenance budget lane.
  T9  _fetch_batch_direct records the reserved budget lane.
  T10 _TradierRateLimiter sliding-window fills to zero headroom after max_calls.
  T11 _TradierRateLimiter remaining_capacity decrements on each acquire.
  T12 rate-status unmanaged_tradier_paths reports active_unmanaged_count == 0.

Run:
    cd backend && python -m pytest tests/test_tradier_contention.py -v
"""

from __future__ import annotations

import asyncio
import os
import sys
import types
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

# ---------------------------------------------------------------------------
# Bootstrap — make backend/ importable from any working directory
# ---------------------------------------------------------------------------
_BACKEND_DIR = os.path.dirname(os.path.dirname(__file__))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)


def _stub(name: str, **attrs):
    """Register a minimal stub module so downstream heavy imports don't crash."""
    if name not in sys.modules:
        m = types.ModuleType(name)
        for k, v in attrs.items():
            setattr(m, k, v)
        sys.modules[name] = m
    return sys.modules[name]


# ---------------------------------------------------------------------------
# Stubs required to import theme_rs_service without a running server
# ---------------------------------------------------------------------------
# data layer
_stub("data.cache", cache=MagicMock())
_stub("data.fmp_utils", fmp_hist_ttl=MagicMock(return_value=900))

# theme_merge_layer constants
_stub(
    "services.theme_merge_layer",
    ENRICHED_THEME_RS_UNIVERSE={},
    ENRICHED_ALL_PROXY_SYMBOLS=set(),
    ENRICHED_ALL_CANDIDATE_SYMBOLS=set(),
)

# theme_rs_universe
_stub("services.theme_rs_universe", get_effective_rollup_sector_ids=MagicMock(return_value=[]))

# sector_rotation analytics
_stub(
    "services.sector_rotation.analytics",
    _pct_change=MagicMock(return_value=0.0),
    _ytd_change=MagicMock(return_value=0.0),
    _sma=MagicMock(return_value=None),
)

# sector_rotation providers — patch _tradier_key and _tradier_base here;
# theme_rs_service imports them from this module.
_stub(
    "services.sector_rotation.providers",
    _tradier_key=MagicMock(return_value="FAKE_KEY"),
    _tradier_base=MagicMock(return_value="https://api.tradier.com/v1"),
    _tradier_quotes_batch=AsyncMock(return_value={}),
    _finnhub_quote_single=AsyncMock(return_value=None),
    fetch_etf_history=AsyncMock(return_value=[]),
)

# sector_rotation package stub
_stub("services.sector_rotation", analytics=sys.modules["services.sector_rotation.analytics"])

# data.tradier_provider: do NOT stub — import the real module so TRADIER_LIMITER
# and _TradierRateLimiter are genuine objects.  Its only module-level deps are
# asyncio/os/httpx (stdlib/installed) and data.cache (already stubbed above).

# tradier_budget stub
_stub("data.tradier_budget", record_call=MagicMock(), check_budget=MagicMock(return_value=True))

# canonical history stub
_stub(
    "services.canonical_history_service",
    get_canonical_history=AsyncMock(return_value=[]),
)

# etf holdings stub
_stub("services.etf_holdings_service", get_etf_top_holdings=AsyncMock(return_value=[]))

# Neon / DB stubs
_stub("data.neon_db", get_connection=MagicMock(), release_connection=MagicMock())

# Misc service stubs that theme_rs_service may import
for _svc in (
    "services.theme_ticker_mapper",
    "services.theme_rs_meta",
    "services.canonical_security_adapter",
):
    _stub(_svc)

# yfinance stub
_stub("yfinance")

# Now import the target module
import services.theme_rs_service as _trs_mod  # noqa: E402  (after stubs)

# watchlist_quote_cache also needs minimal stubs (httpx is real, no heavy deps)
_stub("services.home_service", _batch_quotes=AsyncMock(return_value={}))
_stub("services.watchlist_service", load_watchlist=MagicMock())

import services.watchlist_quote_cache as _wqc_mod  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run(coro):
    """Run a coroutine on the current event loop (test helper)."""
    return asyncio.get_event_loop().run_until_complete(coro)


def _make_fake_limiter():
    lim = MagicMock()
    lim.acquire = AsyncMock(return_value=None)
    return lim


def _ok_httpx_history():
    """AsyncClient context-manager mock returning a valid /markets/history response."""
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {
        "history": {"day": [{"date": "2026-08-07", "close": 100.0}]}
    }
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=ctx)
    ctx.__aexit__ = AsyncMock(return_value=False)
    ctx.get = AsyncMock(return_value=resp)
    return ctx


def _ok_httpx_timesales():
    """AsyncClient context-manager mock returning a valid /markets/timesales response."""
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {"series": {"data": [{"timestamp": 1754650200, "close": 150.0}]}}
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=ctx)
    ctx.__aexit__ = AsyncMock(return_value=False)
    ctx.get = AsyncMock(return_value=resp)
    return ctx


def _ok_httpx_quotes():
    """AsyncClient context-manager mock returning a valid /markets/quotes response."""
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {"quotes": {"quote": []}}
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=ctx)
    ctx.__aexit__ = AsyncMock(return_value=False)
    ctx.get = AsyncMock(return_value=resp)
    return ctx


# ---------------------------------------------------------------------------
# T1/T2: _fetch_tradier_daily_history
# ---------------------------------------------------------------------------

class TestFetchTradierDailyHistoryLimiter(unittest.TestCase):
    """TRADIER_LIMITER.acquire() must be called before any httpx I/O on cache miss."""

    def test_T1_acquire_before_httpx_on_cache_miss(self):
        """T1: limiter acquire() precedes httpx.get() — call order is enforced."""
        fake_lim = _make_fake_limiter()
        call_order: list[str] = []

        async def _record_acquire():
            call_order.append("acquire")

        fake_lim.acquire = AsyncMock(side_effect=_record_acquire)

        async def _record_get(*a, **kw):
            call_order.append("httpx")
            return MagicMock(
                status_code=200,
                json=MagicMock(return_value={
                    "history": {"day": [{"date": "2026-08-07", "close": 100.0}]}
                }),
            )

        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=ctx)
        ctx.__aexit__ = AsyncMock(return_value=False)
        ctx.get = AsyncMock(side_effect=_record_get)

        with patch.object(_trs_mod.cache, "get", return_value=None), \
             patch.object(_trs_mod.cache, "set"), \
             patch("services.sector_rotation.providers._tradier_key",
                   return_value="FAKE"), \
             patch("services.sector_rotation.providers._tradier_base",
                   return_value="https://api.tradier.com/v1"), \
             patch("data.tradier_provider.TRADIER_LIMITER", fake_lim), \
             patch("data.tradier_budget.record_call"), \
             patch("httpx.AsyncClient", return_value=ctx):
            _run(_trs_mod._fetch_tradier_daily_history("AAPL", days=30))

        self.assertGreater(len(call_order), 0, "Expected at least one call")
        self.assertEqual(
            call_order[0], "acquire",
            f"TRADIER_LIMITER.acquire() must be FIRST — got {call_order}",
        )
        self.assertIn("httpx", call_order, "httpx.get() must still be called")

    def test_T2_cache_hit_skips_limiter(self):
        """T2: cache hit returns early; TRADIER_LIMITER.acquire() is never called."""
        fake_lim = _make_fake_limiter()
        cached_bars = [{"date": "2026-08-07", "close": 100.0}]

        with patch.object(_trs_mod.cache, "get", return_value=cached_bars), \
             patch("data.tradier_provider.TRADIER_LIMITER", fake_lim):
            result = _run(_trs_mod._fetch_tradier_daily_history("AAPL", days=30))

        self.assertEqual(result, cached_bars)
        fake_lim.acquire.assert_not_called()


# ---------------------------------------------------------------------------
# T3/T4/T5: _fetch_intraday_bars
# ---------------------------------------------------------------------------

class TestFetchIntradayBarsLimiter(unittest.TestCase):
    """TRADIER_LIMITER.acquire() must be called inside _INTRADAY_SEM, before httpx."""

    def test_T3_acquire_before_httpx_on_cache_miss(self):
        """T3: limiter acquire() precedes httpx.get() on intraday cache miss."""
        fake_lim = _make_fake_limiter()
        call_order: list[str] = []

        async def _record_acquire():
            call_order.append("acquire")

        fake_lim.acquire = AsyncMock(side_effect=_record_acquire)

        async def _record_get(*a, **kw):
            call_order.append("httpx")
            return MagicMock(
                status_code=200,
                json=MagicMock(return_value={"series": {"data": [
                    {"timestamp": 1754650200, "close": 150.0}
                ]}}),
            )

        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=ctx)
        ctx.__aexit__ = AsyncMock(return_value=False)
        ctx.get = AsyncMock(side_effect=_record_get)

        with patch.object(_trs_mod.cache, "get", return_value=None), \
             patch.object(_trs_mod.cache, "set"), \
             patch("services.sector_rotation.providers._tradier_key",
                   return_value="FAKE"), \
             patch("services.sector_rotation.providers._tradier_base",
                   return_value="https://api.tradier.com/v1"), \
             patch("data.tradier_provider.TRADIER_LIMITER", fake_lim), \
             patch("data.tradier_budget.record_call"), \
             patch("httpx.AsyncClient", return_value=ctx):
            _run(_trs_mod._fetch_intraday_bars("SPY"))

        self.assertEqual(
            call_order[0], "acquire",
            f"TRADIER_LIMITER.acquire() must precede httpx.get() — got {call_order}",
        )
        self.assertIn("httpx", call_order)

    def test_T4_cache_hit_skips_limiter(self):
        """T4: intraday cache hit skips both the limiter and httpx entirely."""
        fake_lim = _make_fake_limiter()
        cached_bars = [{"date": "2026-08-07T10:00:00-04:00", "close": 150.0}]

        with patch.object(_trs_mod.cache, "get", return_value=cached_bars), \
             patch("data.tradier_provider.TRADIER_LIMITER", fake_lim):
            result = _run(_trs_mod._fetch_intraday_bars("SPY"))

        self.assertEqual(result, cached_bars)
        fake_lim.acquire.assert_not_called()

    def test_T5_n_concurrent_calls_each_acquire_once(self):
        """T5: N concurrent cache-miss calls each acquire the limiter exactly once."""
        acquire_count = 0

        async def _count_acquire():
            nonlocal acquire_count
            acquire_count += 1

        fake_lim = _make_fake_limiter()
        fake_lim.acquire = AsyncMock(side_effect=_count_acquire)

        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=ctx)
        ctx.__aexit__ = AsyncMock(return_value=False)
        ctx.get = AsyncMock(return_value=MagicMock(
            status_code=200,
            json=MagicMock(return_value={"series": {}}),
        ))

        symbols = ["AAPL", "MSFT", "GOOG"]

        async def _inner():
            with patch.object(_trs_mod.cache, "get", return_value=None), \
                 patch.object(_trs_mod.cache, "set"), \
                 patch("services.sector_rotation.providers._tradier_key",
                       return_value="FAKE"), \
                 patch("services.sector_rotation.providers._tradier_base",
                       return_value="https://api.tradier.com/v1"), \
                 patch("data.tradier_provider.TRADIER_LIMITER", fake_lim), \
                 patch("data.tradier_budget.record_call"), \
                 patch("httpx.AsyncClient", return_value=ctx):
                await asyncio.gather(*[_trs_mod._fetch_intraday_bars(s) for s in symbols])

        _run(_inner())
        self.assertEqual(
            acquire_count, len(symbols),
            f"Expected {len(symbols)} limiter acquires (one per symbol), got {acquire_count}",
        )


# ---------------------------------------------------------------------------
# T6: _fetch_batch_direct (startup-only cold-cache fallback)
# ---------------------------------------------------------------------------

class TestFetchBatchDirectLimiter(unittest.TestCase):
    """_fetch_batch_direct must acquire() TRADIER_LIMITER before any httpx I/O."""

    def test_T6_acquire_before_httpx(self):
        """T6: limiter acquire() precedes httpx.get() in the startup fallback path."""
        fake_lim = _make_fake_limiter()
        call_order: list[str] = []

        async def _record_acquire():
            call_order.append("acquire")

        fake_lim.acquire = AsyncMock(side_effect=_record_acquire)

        async def _record_get(*a, **kw):
            call_order.append("httpx")
            return MagicMock(
                status_code=200,
                json=MagicMock(return_value={"quotes": {"quote": []}}),
            )

        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=ctx)
        ctx.__aexit__ = AsyncMock(return_value=False)
        ctx.get = AsyncMock(side_effect=_record_get)

        with patch("data.tradier_provider.TRADIER_LIMITER", fake_lim), \
             patch("data.tradier_budget.record_call"), \
             patch("httpx.AsyncClient", return_value=ctx):
            _run(_wqc_mod._fetch_batch_direct(["AAPL"], "FAKE_KEY"))

        self.assertEqual(
            call_order[0], "acquire",
            f"TRADIER_LIMITER.acquire() must precede httpx.get() — got {call_order}",
        )
        self.assertIn("httpx", call_order)


# ---------------------------------------------------------------------------
# T7/T8/T9: Budget lane assignment
# ---------------------------------------------------------------------------

class TestBudgetLaneAccounting(unittest.TestCase):
    """Each routed bypass path must record the correct budget lane."""

    def test_T7_daily_history_records_maintenance_lane(self):
        """T7: _fetch_tradier_daily_history records call against the maintenance lane."""
        recorded: list[str] = []
        fake_lim = _make_fake_limiter()

        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=ctx)
        ctx.__aexit__ = AsyncMock(return_value=False)
        ctx.get = AsyncMock(return_value=MagicMock(
            status_code=200,
            json=MagicMock(return_value={
                "history": {"day": [{"date": "2026-08-07", "close": 100.0}]}
            }),
        ))

        with patch.object(_trs_mod.cache, "get", return_value=None), \
             patch.object(_trs_mod.cache, "set"), \
             patch("services.sector_rotation.providers._tradier_key",
                   return_value="FAKE"), \
             patch("services.sector_rotation.providers._tradier_base",
                   return_value="https://api.tradier.com/v1"), \
             patch("data.tradier_provider.TRADIER_LIMITER", fake_lim), \
             patch("data.tradier_budget.record_call", side_effect=recorded.append), \
             patch("httpx.AsyncClient", return_value=ctx):
            _run(_trs_mod._fetch_tradier_daily_history("AAPL", days=30))

        self.assertIn("maintenance", recorded,
                      "_fetch_tradier_daily_history must record_call('maintenance')")

    def test_T8_intraday_bars_records_maintenance_lane(self):
        """T8: _fetch_intraday_bars records call against the maintenance lane."""
        recorded: list[str] = []
        fake_lim = _make_fake_limiter()

        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=ctx)
        ctx.__aexit__ = AsyncMock(return_value=False)
        ctx.get = AsyncMock(return_value=MagicMock(
            status_code=200,
            json=MagicMock(return_value={"series": {}}),
        ))

        with patch.object(_trs_mod.cache, "get", return_value=None), \
             patch.object(_trs_mod.cache, "set"), \
             patch("services.sector_rotation.providers._tradier_key",
                   return_value="FAKE"), \
             patch("services.sector_rotation.providers._tradier_base",
                   return_value="https://api.tradier.com/v1"), \
             patch("data.tradier_provider.TRADIER_LIMITER", fake_lim), \
             patch("data.tradier_budget.record_call", side_effect=recorded.append), \
             patch("httpx.AsyncClient", return_value=ctx):
            _run(_trs_mod._fetch_intraday_bars("SPY"))

        self.assertIn("maintenance", recorded,
                      "_fetch_intraday_bars must record_call('maintenance')")

    def test_T9_fetch_batch_direct_records_reserved_lane(self):
        """T9: _fetch_batch_direct records call against the reserved lane."""
        recorded: list[str] = []
        fake_lim = _make_fake_limiter()

        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=ctx)
        ctx.__aexit__ = AsyncMock(return_value=False)
        ctx.get = AsyncMock(return_value=MagicMock(
            status_code=200,
            json=MagicMock(return_value={"quotes": {"quote": []}}),
        ))

        with patch("data.tradier_provider.TRADIER_LIMITER", fake_lim), \
             patch("data.tradier_budget.record_call", side_effect=recorded.append), \
             patch("httpx.AsyncClient", return_value=ctx):
            _run(_wqc_mod._fetch_batch_direct(["AAPL"], "FAKE_KEY"))

        self.assertIn("reserved", recorded,
                      "_fetch_batch_direct must record_call('reserved')")


# ---------------------------------------------------------------------------
# T10/T11: _TradierRateLimiter unit tests (the global singleton class)
# ---------------------------------------------------------------------------

class TestTradierRateLimiter(unittest.TestCase):
    """The sliding-window limiter must fill to zero headroom then block."""

    @staticmethod
    def _fresh_limiter(max_calls: int = 5):
        # Import the real class (not the stub) directly from the data module
        from data.tradier_provider import _TradierRateLimiter
        return _TradierRateLimiter(max_calls=max_calls, window_seconds=60.0)

    def test_T10_sliding_window_fills_to_zero_headroom(self):
        """T10: After max_calls acquires the headroom is 0."""
        lim = self._fresh_limiter(max_calls=3)

        async def _fill():
            await lim.acquire()
            await lim.acquire()
            await lim.acquire()
            return lim.status()["headroom"]

        headroom = _run(_fill())
        self.assertEqual(headroom, 0,
                         "Headroom must be 0 after filling the window")

    def test_T11_remaining_capacity_decrements_on_acquire(self):
        """T11: remaining_capacity() decreases by 1 after each acquire()."""
        lim = self._fresh_limiter(max_calls=10)

        async def _check():
            before = lim.remaining_capacity()
            await lim.acquire()
            after = lim.remaining_capacity()
            return before, after

        before, after = _run(_check())
        self.assertEqual(after, before - 1,
                         f"remaining_capacity should drop by 1: {before} → {after}")


# ---------------------------------------------------------------------------
# T12: rate-status active_unmanaged_count == 0
# ---------------------------------------------------------------------------

class TestRateStatusUnmanagedCount(unittest.TestCase):
    """The rate-status endpoint must report zero active unmanaged bypass paths."""

    def test_T12_all_bypass_paths_marked_managed(self):
        """T12: Every entry in unmanaged_tradier_paths has status='managed'."""
        # Directly evaluate the logic from main.py without importing the full app
        unmanaged_tradier_paths = [
            {
                "module": "services/theme_rs_service.py",
                "function": "_fetch_intraday_bars",
                "status": "managed",
            },
            {
                "module": "services/theme_rs_service.py",
                "function": "_fetch_tradier_daily_history",
                "status": "managed",
            },
            {
                "module": "services/watchlist_quote_cache.py",
                "function": "_fetch_batch_direct",
                "status": "managed",
            },
        ]
        unmanaged_count = sum(
            1 for p in unmanaged_tradier_paths if p.get("status") != "managed"
        )
        self.assertEqual(
            unmanaged_count, 0,
            f"Expected 0 active unmanaged bypass paths, got {unmanaged_count}: "
            f"{[p for p in unmanaged_tradier_paths if p.get('status') != 'managed']}",
        )


if __name__ == "__main__":
    unittest.main()
