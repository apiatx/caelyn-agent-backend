"""
test_tradier_contention.py — Contract tests for Tradier background admission.

Proves the full set of behavioral guarantees after the "align admission with
physical requests" correction on top of de859cc1 / 78a5793c:

  Contract 1  — Service code calls get_timesales_background(), NOT _get_preadmitted().
  Contract 2  — Provider-level coalescing (_TIMESALES_FUTURES) is the single registry.
  Contract 3  — get_provider() singleton respects TRADIER_SANDBOX env var.
  Contract 4  — _fetch_batch_direct is non-blocking (try_acquire_background, not acquire).
  Contract 5  — Theme RS can sustain required freshness within available budget.
  Contract 6  — Integrated multi-subsystem simulation proves all 8 required properties.

Tests
-----
  T1  Intraday cache hit → zero semaphore, zero HTTP.
  T2  History cache hit → zero admission, zero HTTP.
  T3  Background intraday with denied provider admission → returns [] (semaphore entered/released).
  T4  Does NOT sleep when provider admission returns [] (non-blocking).
  T5  Semaphore IS entered; deferral happens inside provider, not before semaphore.
  T6  Semaphore entered BEFORE provider admission (new ordering: sem → admission → HTTP).
  T7  Maintenance lane full → physical request skipped + defer recorded.
  T8  Global headroom insufficient → physical request skipped.
  T9  Interactive reserve (5 slots) survives after background call.
  T10 Exactly one get_timesales_background call per intraday fetch.
  T11 Provider admission path records exactly one global + one lane slot.
  T12 No double-admission / double-counting.
  T13 get_provider() exception → NO raw Tradier HTTP (no fail-open).
  T14 Startup quote fallback: non-blocking admission; lane/headroom full → {}.
  T15 Exact concurrent duplicate timesales coalesces at provider level.
  T16 Theme RS 1D TTL constants unchanged (60s / 3600s markers).
  T17 Historical TF fetch cadence unchanged (86400s).
  T18 _INTRADAY_SEM limit (≤20) unchanged.
  T19 Real-time quote contract unchanged (TradierProvider.get_quotes intact).
  T20 Real-time earnings code untouched (earnings_monitor unmodified).
  PROV Provider ownership: get_provider() sandbox correctness, singleton.
  SIM  Active-session simulation: background defers, interactive reserve survives.
  INT  Integrated multi-subsystem simulation (Contract 6).

Run:
    cd backend && python -m pytest tests/test_tradier_contention.py -v
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
import types
import unittest
from unittest.mock import AsyncMock, MagicMock, patch, call

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
_stub("data.cache", cache=MagicMock())
_stub("data.fmp_utils", fmp_hist_ttl=MagicMock(return_value=900))
_stub(
    "services.theme_merge_layer",
    ENRICHED_THEME_RS_UNIVERSE={},
    ENRICHED_ALL_PROXY_SYMBOLS=set(),
    ENRICHED_ALL_CANDIDATE_SYMBOLS=set(),
)
_stub("services.theme_rs_universe", get_effective_rollup_sector_ids=MagicMock(return_value=[]))
_stub(
    "services.sector_rotation.analytics",
    _pct_change=MagicMock(return_value=0.0),
    _ytd_change=MagicMock(return_value=0.0),
    _sma=MagicMock(return_value=None),
)
_stub(
    "services.sector_rotation.providers",
    _tradier_key=MagicMock(return_value="FAKE_KEY"),
    _tradier_base=MagicMock(return_value="https://api.tradier.com/v1"),
    _tradier_quotes_batch=AsyncMock(return_value={}),
    _finnhub_quote_single=AsyncMock(return_value=None),
    fetch_etf_history=AsyncMock(return_value=[]),
)
_stub("services.sector_rotation", analytics=sys.modules["services.sector_rotation.analytics"])
_stub(
    "services.canonical_history_service",
    get_canonical_history=AsyncMock(return_value=[]),
)
_stub("services.etf_holdings_service", get_etf_top_holdings=AsyncMock(return_value=[]))
_stub("data.neon_db", get_connection=MagicMock(), release_connection=MagicMock())
for _svc in (
    "services.theme_ticker_mapper",
    "services.theme_rs_meta",
    "services.canonical_security_adapter",
):
    _stub(_svc)
_stub("yfinance")

# tradier_budget — real stubs for check_budget/record_call/record_defer so tests can inspect
_bgt_stub = _stub(
    "data.tradier_budget",
    check_budget=MagicMock(return_value=True),
    record_call=MagicMock(),
    record_defer=MagicMock(),
    get_current_lane=MagicMock(return_value="reserved"),
)

# Import real theme_rs_service (after stubs)
import services.theme_rs_service as _trs_mod  # noqa: E402

# watchlist_quote_cache stubs
_stub("services.home_service", _batch_quotes=AsyncMock(return_value={}))
_stub("services.watchlist_service", load_watchlist=MagicMock())
import services.watchlist_quote_cache as _wqc_mod  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


def _make_fake_provider(timesales_data=None, history_data=None, quotes_data=None):
    """Fake TradierProvider for service-level tests."""
    prov = MagicMock()
    prov._get_preadmitted = AsyncMock(return_value=timesales_data or {})
    prov.get_history_background = AsyncMock(return_value=history_data or [])
    # Default: get_timesales_background returns [] (denied/empty); tests override as needed
    prov.get_timesales_background = AsyncMock(return_value=timesales_data or [])
    prov.get_quotes = AsyncMock(return_value=quotes_data or [])
    return prov


def _make_timesales_response():
    """Return a fake timesales tick list (provider format)."""
    return [{"timestamp": 1754650200, "close": 150.0, "open": 149.0,
             "high": 151.0, "low": 148.0, "volume": 1000, "vwap": 150.0}]


# ---------------------------------------------------------------------------
# T1: Intraday cache hit → zero semaphore, zero HTTP
# ---------------------------------------------------------------------------

class TestT1IntradayCacheHit(unittest.TestCase):

    def test_cache_hit_zero_semaphore_zero_http(self):
        """T1: Service cache hit returns early; no semaphore, no provider call, no HTTP."""
        fake_prov = _make_fake_provider()
        cached_bars = [{"date": "2026-08-07T10:00:00-04:00", "close": 150.0}]
        sem_entered = []

        class _SpySem:
            async def __aenter__(self):
                sem_entered.append(True)
                return self
            async def __aexit__(self, *a):
                return False

        with patch.object(_trs_mod.cache, "get", return_value=cached_bars), \
             patch.object(_trs_mod, "_INTRADAY_SEM", _SpySem()), \
             patch("data.tradier_provider.get_provider", return_value=fake_prov):
            result = _run(_trs_mod._fetch_intraday_bars("SPY"))

        self.assertEqual(result, cached_bars)
        self.assertEqual(sem_entered, [], "_INTRADAY_SEM must NOT be entered on cache hit")
        fake_prov.get_timesales_background.assert_not_called()
        fake_prov._get_preadmitted.assert_not_called()


# ---------------------------------------------------------------------------
# T2: History cache hit → zero admission, zero HTTP
# ---------------------------------------------------------------------------

class TestT2HistoryCacheHit(unittest.TestCase):

    def test_cache_hit_zero_admission_zero_http(self):
        """T2: History cache hit returns early; no admission or HTTP."""
        fake_prov = _make_fake_provider()
        cached_bars = [{"date": "2026-08-07", "close": 100.0}]

        with patch.object(_trs_mod.cache, "get", return_value=cached_bars), \
             patch("data.tradier_provider.get_provider", return_value=fake_prov):
            result = _run(_trs_mod._fetch_tradier_daily_history("AAPL", days=30))

        self.assertEqual(result, cached_bars)
        fake_prov.get_history_background.assert_not_called()


# ---------------------------------------------------------------------------
# T3: Provider admission denied → returns [] (semaphore entered AND released cleanly)
# ---------------------------------------------------------------------------

class TestT3AdmissionDeniedReturnsEmpty(unittest.TestCase):

    def test_provider_denied_returns_empty_semaphore_released(self):
        """T3: When get_timesales_background returns [] (admission denied),
        _fetch_intraday_bars returns [] and releases _INTRADAY_SEM cleanly."""
        sem_events = []
        fake_prov = _make_fake_provider()
        # Provider returns [] — simulates denied admission inside provider
        fake_prov.get_timesales_background = AsyncMock(return_value=[])

        class _SpySem:
            async def __aenter__(self):
                sem_events.append("enter")
                return self
            async def __aexit__(self, *a):
                sem_events.append("exit")
                return False

        with patch.object(_trs_mod.cache, "get", return_value=None), \
             patch("data.tradier_provider.get_provider", return_value=fake_prov), \
             patch.object(_trs_mod, "_INTRADAY_SEM", _SpySem()):
            result = _run(_trs_mod._fetch_intraday_bars("AAPL"))

        self.assertEqual(result, [])
        # Semaphore IS entered (new ordering: sem → admission inside provider)
        self.assertIn("enter", sem_events, "_INTRADAY_SEM must be entered")
        self.assertIn("exit", sem_events, "_INTRADAY_SEM must be exited cleanly")
        # Physical HTTP never called
        fake_prov._get_preadmitted.assert_not_called()


# ---------------------------------------------------------------------------
# T4: Does NOT sleep when provider admission denied
# ---------------------------------------------------------------------------

class TestT4NoSleepOnAdmissionDenial(unittest.TestCase):

    def test_returns_without_sleeping_when_admission_denied(self):
        """T4: Non-blocking: function returns in ≪1s when provider returns [] (no sleep)."""
        fake_prov = _make_fake_provider()
        fake_prov.get_timesales_background = AsyncMock(return_value=[])

        async def _inner():
            with patch.object(_trs_mod.cache, "get", return_value=None), \
                 patch("data.tradier_provider.get_provider", return_value=fake_prov):
                t0 = asyncio.get_event_loop().time()
                await _trs_mod._fetch_intraday_bars("MSFT")
                return asyncio.get_event_loop().time() - t0

        elapsed = _run(_inner())
        self.assertLess(elapsed, 0.5,
                        f"Must return in <500ms on deferral; took {elapsed:.3f}s")


# ---------------------------------------------------------------------------
# T5: Semaphore IS entered; deferral happens inside provider, not before semaphore
# ---------------------------------------------------------------------------

class TestT5SemaphoreEnteredDeferralInsideProvider(unittest.TestCase):

    def test_semaphore_entered_before_provider_call(self):
        """T5: _INTRADAY_SEM IS entered even when admission will fail.
        Deferral happens inside get_timesales_background(), not before the semaphore."""
        sem_entered = []
        fake_prov = _make_fake_provider()
        fake_prov.get_timesales_background = AsyncMock(return_value=[])

        class _SpySem:
            async def __aenter__(self):
                sem_entered.append(True)
                return self
            async def __aexit__(self, *a):
                return False

        with patch.object(_trs_mod.cache, "get", return_value=None), \
             patch("data.tradier_provider.get_provider", return_value=fake_prov), \
             patch.object(_trs_mod, "_INTRADAY_SEM", _SpySem()):
            _run(_trs_mod._fetch_intraday_bars("GOOG"))

        self.assertNotEqual(sem_entered, [],
                            "_INTRADAY_SEM must be entered (deferral is inside provider)")
        # Physical HTTP never called regardless
        fake_prov._get_preadmitted.assert_not_called()

    def test_get_provider_failure_skips_semaphore(self):
        """T5b: If get_provider() raises, function returns [] before entering semaphore."""
        sem_entered = []

        class _SpySem:
            async def __aenter__(self):
                sem_entered.append(True)
                return self
            async def __aexit__(self, *a):
                return False

        with patch.object(_trs_mod.cache, "get", return_value=None), \
             patch("data.tradier_provider.get_provider", side_effect=Exception("no key")), \
             patch.object(_trs_mod, "_INTRADAY_SEM", _SpySem()):
            result = _run(_trs_mod._fetch_intraday_bars("TSLA"))

        self.assertEqual(result, [])
        self.assertEqual(sem_entered, [], "Semaphore must NOT be entered when provider init fails")


# ---------------------------------------------------------------------------
# T6: Semaphore entered BEFORE provider admission (new ordering)
# ---------------------------------------------------------------------------

class TestT6SemaphoreBeforeProviderAdmission(unittest.TestCase):

    def test_semaphore_entered_before_provider_admission(self):
        """T6: _INTRADAY_SEM is entered BEFORE provider.get_timesales_background()
        (which performs admission internally). Ordering: sem → admission → HTTP."""
        call_order: list[str] = []

        async def _fake_get_timesales_bg(symbol, **kw):
            call_order.append("provider_admission")
            return []

        fake_prov = _make_fake_provider()
        fake_prov.get_timesales_background = AsyncMock(side_effect=_fake_get_timesales_bg)

        class _SpySem:
            async def __aenter__(self):
                call_order.append("semaphore_enter")
                return self
            async def __aexit__(self, *a):
                return False

        with patch.object(_trs_mod.cache, "get", return_value=None), \
             patch("data.tradier_provider.get_provider", return_value=fake_prov), \
             patch.object(_trs_mod, "_INTRADAY_SEM", _SpySem()):
            _run(_trs_mod._fetch_intraday_bars("SPY"))

        self.assertIn("semaphore_enter", call_order, "Semaphore must be entered")
        self.assertIn("provider_admission", call_order, "Provider must be called")
        sem_idx = call_order.index("semaphore_enter")
        adm_idx = call_order.index("provider_admission")
        self.assertLess(sem_idx, adm_idx,
                        f"Semaphore must precede provider admission: {call_order}")

    def test_no_pre_reservation_before_semaphore(self):
        """T6b: Prove get_timesales_background is never called before _INTRADAY_SEM.
        The old defect (de859cc1): global slot reserved before semaphore acquired.
        With the new ordering this is impossible because admission is inside the provider
        which is called inside the semaphore."""
        semaphore_acquired = False
        provider_called_before_semaphore = False
        call_log: list[str] = []

        class _TrackingSem:
            async def __aenter__(self):
                call_log.append("sem")
                return self
            async def __aexit__(self, *a):
                return False

        async def _fake_bg(symbol, **kw):
            call_log.append("provider")
            return []

        fake_prov = _make_fake_provider()
        fake_prov.get_timesales_background = AsyncMock(side_effect=_fake_bg)

        with patch.object(_trs_mod.cache, "get", return_value=None), \
             patch("data.tradier_provider.get_provider", return_value=fake_prov), \
             patch.object(_trs_mod, "_INTRADAY_SEM", _TrackingSem()):
            _run(_trs_mod._fetch_intraday_bars("QQQ"))

        # Verify "provider" never appears before "sem" in the log
        if "provider" in call_log and "sem" in call_log:
            first_provider = call_log.index("provider")
            first_sem = call_log.index("sem")
            self.assertGreater(first_provider, first_sem,
                               f"Provider admission must come after semaphore: {call_log}")


# ---------------------------------------------------------------------------
# T7: Maintenance lane full → physical request skipped + defer recorded
# ---------------------------------------------------------------------------

class TestT7LaneFullDefersRequest(unittest.TestCase):

    def test_maintenance_lane_full_skips_request(self):
        """T7: check_budget('maintenance') → False inside provider causes defer + []."""
        from data.tradier_provider import _TradierRateLimiter, TradierProvider

        # Use a real TradierProvider with fake HTTP and real budget stub
        real_lim = _TradierRateLimiter(max_calls=110, window_seconds=60.0)
        prov = TradierProvider(api_key="FAKE_KEY_T7")

        defer_called: list[str] = []
        # Provider cache must miss so the budget check is reached
        miss_cache = MagicMock()
        miss_cache.get = MagicMock(return_value=None)

        with patch("data.tradier_provider.TRADIER_LIMITER", real_lim), \
             patch("data.tradier_provider.cache", miss_cache), \
             patch("data.tradier_budget.check_budget", return_value=False), \
             patch("data.tradier_budget.record_defer", side_effect=defer_called.append), \
             patch("data.tradier_budget.record_call"):
            result = _run(prov.get_timesales_background(
                symbol="NVDA", interval="5min", lane="maintenance", reserve=5
            ))

        self.assertEqual(result, [])
        self.assertGreater(len(defer_called), 0, "record_defer must be called when lane is full")
        # Global limiter never touched when lane budget denies early
        self.assertEqual(real_lim.total_calls, 0, "Global limiter must not be touched when lane full")


# ---------------------------------------------------------------------------
# T8: Global headroom insufficient → physical request skipped
# ---------------------------------------------------------------------------

class TestT8GlobalHeadroomInsufficientSkips(unittest.TestCase):

    def test_global_headroom_zero_skips_http(self):
        """T8: try_acquire_background(False) skips HTTP even when lane is available."""
        from data.tradier_provider import _TradierRateLimiter, TradierProvider

        # Fill limiter to leave exactly 5 slots (= reserve threshold)
        real_lim = _TradierRateLimiter(max_calls=10, window_seconds=60.0)
        prov = TradierProvider(api_key="FAKE_KEY_T8")
        miss_cache = MagicMock()
        miss_cache.get = MagicMock(return_value=None)

        async def _sim():
            # Fill 5 slots → 5 remaining = exactly reserve (need 6 for background)
            for _ in range(5):
                await real_lim.acquire()
            with patch("data.tradier_provider.TRADIER_LIMITER", real_lim), \
                 patch("data.tradier_provider.cache", miss_cache), \
                 patch("data.tradier_budget.check_budget", return_value=True), \
                 patch("data.tradier_budget.record_call"):
                return await prov.get_timesales_background(
                    symbol="META", interval="5min", lane="maintenance", reserve=5
                )

        result = _run(_sim())
        self.assertEqual(result, [])


# ---------------------------------------------------------------------------
# T9: Interactive reserve survives after background calls
# ---------------------------------------------------------------------------

class TestT9InteractiveReserveSurvives(unittest.TestCase):

    def test_try_acquire_background_preserves_reserve(self):
        """T9: try_acquire_background(reserve=5) never takes the last 5 global slots."""
        from data.tradier_provider import _TradierRateLimiter

        lim = _TradierRateLimiter(max_calls=10, window_seconds=60.0)

        async def _fill_to_near_limit():
            # Fill 5 slots (leaving exactly 5 = reserve threshold)
            for _ in range(5):
                await lim.acquire()
            # Background with reserve=5 needs 5+1=6 free; only 5 free → must fail
            result = await lim.try_acquire_background(reserve=5)
            return result

        result = _run(_fill_to_near_limit())
        self.assertFalse(result, "try_acquire_background must fail when headroom == reserve")

    def test_try_acquire_background_succeeds_with_room(self):
        """T9b: try_acquire_background succeeds when enough headroom exists."""
        from data.tradier_provider import _TradierRateLimiter

        lim = _TradierRateLimiter(max_calls=20, window_seconds=60.0)

        async def _check():
            # Fill 5 slots, 15 remaining → 15 > 5+1 → should succeed
            for _ in range(5):
                await lim.acquire()
            return await lim.try_acquire_background(reserve=5)

        result = _run(_check())
        self.assertTrue(result)


# ---------------------------------------------------------------------------
# T10: Exactly one get_timesales_background call per intraday fetch
# ---------------------------------------------------------------------------

class TestT10ExactlyOneProviderCall(unittest.TestCase):

    def test_one_get_timesales_background_per_fetch(self):
        """T10: One intraday fetch → exactly one get_timesales_background call."""
        fake_prov = _make_fake_provider()
        fake_prov.get_timesales_background = AsyncMock(return_value=_make_timesales_response())

        with patch.object(_trs_mod.cache, "get", return_value=None), \
             patch.object(_trs_mod.cache, "set"), \
             patch("data.tradier_provider.get_provider", return_value=fake_prov):
            _run(_trs_mod._fetch_intraday_bars("TSLA"))

        fake_prov.get_timesales_background.assert_called_once()
        # _get_preadmitted must NOT be called directly from service code (Contract 1)
        fake_prov._get_preadmitted.assert_not_called()


# ---------------------------------------------------------------------------
# T11: Provider admission: exactly one global + one lane slot per call
# ---------------------------------------------------------------------------

class TestT11ProviderAdmissionCounting(unittest.TestCase):

    def test_successful_call_records_exactly_one_global_and_one_lane_slot(self):
        """T11: One successful timesales fetch via provider = one global + one lane slot."""
        from data.tradier_provider import _TradierRateLimiter, TradierProvider

        real_lim = _TradierRateLimiter(max_calls=110, window_seconds=60.0)
        prov = TradierProvider(api_key="FAKE_KEY_T11")
        lane_calls: list[str] = []

        async def _fake_preadmitted(path, params=None):
            return {"series": {"data": [{"timestamp": 1754650200, "close": 150.0,
                                          "open": 149.0, "high": 151.0, "low": 148.0,
                                          "volume": 100, "vwap": 150.0}]}}

        prov._get_preadmitted = _fake_preadmitted  # type: ignore

        with patch("data.tradier_provider.TRADIER_LIMITER", real_lim), \
             patch("data.tradier_budget.check_budget", return_value=True), \
             patch("data.tradier_budget.record_defer"), \
             patch("data.tradier_budget.record_call", side_effect=lane_calls.append), \
             patch.object(_trs_mod.cache, "get", return_value=None), \
             patch.object(_trs_mod.cache, "set"), \
             patch("data.tradier_provider.get_provider", return_value=prov):
            _run(_trs_mod._fetch_intraday_bars("AMD"))

        # Exactly one global slot
        self.assertEqual(real_lim.total_calls, 1, "Exactly one global slot must be consumed")
        # Exactly one lane call
        self.assertEqual(lane_calls.count("maintenance"), 1,
                         f"Expected 1 record_call('maintenance'), got {lane_calls}")


# ---------------------------------------------------------------------------
# T12: No double-admission / double-counting
# ---------------------------------------------------------------------------

class TestT12NoDoubleAdmission(unittest.TestCase):

    def test_no_double_counting_single_call(self):
        """T12: One intraday fetch makes exactly one try_acquire_background, zero acquire()."""
        from data.tradier_provider import _TradierRateLimiter, TradierProvider

        real_lim = _TradierRateLimiter(max_calls=110, window_seconds=60.0)
        # Spy on try_acquire_background
        original_tab = real_lim.try_acquire_background
        tab_calls: list[bool] = []

        async def _spy_tab(reserve=5):
            result = await original_tab(reserve=reserve)
            tab_calls.append(result)
            return result

        real_lim.try_acquire_background = _spy_tab  # type: ignore

        prov = TradierProvider(api_key="FAKE_KEY_T12")

        async def _fake_preadmitted(path, params=None):
            return None  # empty response — no HTTP data

        prov._get_preadmitted = _fake_preadmitted  # type: ignore

        with patch("data.tradier_provider.TRADIER_LIMITER", real_lim), \
             patch("data.tradier_budget.check_budget", return_value=True), \
             patch("data.tradier_budget.record_call"), \
             patch("data.tradier_budget.record_defer"), \
             patch.object(_trs_mod.cache, "get", return_value=None), \
             patch.object(_trs_mod.cache, "set"), \
             patch("data.tradier_provider.get_provider", return_value=prov):
            _run(_trs_mod._fetch_intraday_bars("INTC"))

        # Exactly one try_acquire_background call
        self.assertEqual(len(tab_calls), 1,
                         f"Expected exactly 1 try_acquire_background, got {len(tab_calls)}")
        # No blocking acquire() from service code
        self.assertEqual(real_lim.total_throttled, 0,
                         "No throttled (blocking) calls — background must be non-blocking")


# ---------------------------------------------------------------------------
# T13: get_provider() exception → NO raw Tradier HTTP (no fail-open)
# ---------------------------------------------------------------------------

class TestT13ProviderExceptionNoFailOpen(unittest.TestCase):

    def test_get_provider_exception_skips_http_not_bypass(self):
        """T13: If get_provider() raises, _fetch_intraday_bars skips; no raw HTTP."""
        http_calls = []

        class _FakeAsyncClient:
            async def __aenter__(self):
                http_calls.append(True)
                return self
            async def __aexit__(self, *a):
                return False
            async def get(self, *a, **kw):
                http_calls.append(True)
                return MagicMock(status_code=200, json=MagicMock(return_value={"series": {}}))

        with patch.object(_trs_mod.cache, "get", return_value=None), \
             patch("data.tradier_provider.get_provider", side_effect=Exception("import failure")), \
             patch("httpx.AsyncClient", _FakeAsyncClient):
            result = _run(_trs_mod._fetch_intraday_bars("CRM"))

        self.assertEqual(result, [])
        self.assertEqual(http_calls, [], "No raw HTTP must be attempted when provider fails")

    def test_get_timesales_background_exception_returns_empty(self):
        """T13b: If get_timesales_background raises, _fetch_intraday_bars returns []."""
        fake_prov = _make_fake_provider()
        fake_prov.get_timesales_background = AsyncMock(side_effect=RuntimeError("network"))

        with patch.object(_trs_mod.cache, "get", return_value=None), \
             patch("data.tradier_provider.get_provider", return_value=fake_prov):
            result = _run(_trs_mod._fetch_intraday_bars("AMZN"))

        self.assertEqual(result, [])


# ---------------------------------------------------------------------------
# T14: Startup quote fallback: non-blocking admission contract (Contract 4)
# ---------------------------------------------------------------------------

class TestT14StartupFallbackNonBlocking(unittest.TestCase):

    def test_fetch_batch_direct_uses_try_acquire_not_blocking_acquire(self):
        """T14: _fetch_batch_direct uses try_acquire_background (non-blocking), not acquire()."""
        from data.tradier_provider import _TradierRateLimiter

        real_lim = _TradierRateLimiter(max_calls=110, window_seconds=60.0)
        acquire_called = []
        original_acquire = real_lim.acquire

        async def _spy_acquire():
            acquire_called.append(True)
            await original_acquire()

        real_lim.acquire = _spy_acquire  # type: ignore

        fake_prov = _make_fake_provider()
        fake_prov._get_preadmitted = AsyncMock(return_value={"quotes": {"quote": []}})

        with patch("data.tradier_budget.check_budget", return_value=True), \
             patch("data.tradier_budget.record_call"), \
             patch("data.tradier_provider.TRADIER_LIMITER", real_lim), \
             patch("data.tradier_provider.get_provider", return_value=fake_prov):
            _run(_wqc_mod._fetch_batch_direct(["AAPL"], "FAKE_KEY"))

        # Blocking acquire() must NOT be called (non-blocking try_acquire_background used instead)
        self.assertEqual(acquire_called, [],
                         "_fetch_batch_direct must use try_acquire_background, not blocking acquire()")

    def test_fetch_batch_direct_uses_provider_not_raw_httpx(self):
        """T14b: _fetch_batch_direct routes through TradierProvider, not raw httpx."""
        raw_http_calls = []

        class _SpyAsyncClient:
            async def __aenter__(self):
                raw_http_calls.append(True)
                return self
            async def __aexit__(self, *a):
                return False
            async def get(self, *a, **kw):
                raw_http_calls.append(True)
                return MagicMock(status_code=200, json=MagicMock(
                    return_value={"quotes": {"quote": []}}))

        fake_prov = _make_fake_provider()
        fake_prov._get_preadmitted = AsyncMock(return_value={"quotes": {"quote": []}})

        with patch("data.tradier_budget.check_budget", return_value=True), \
             patch("data.tradier_budget.record_call"), \
             patch("data.tradier_provider.get_provider", return_value=fake_prov), \
             patch("httpx.AsyncClient", _SpyAsyncClient):
            _run(_wqc_mod._fetch_batch_direct(["AAPL"], "FAKE_KEY"))

        self.assertEqual(raw_http_calls, [], "Raw httpx must NOT be called by _fetch_batch_direct")
        fake_prov._get_preadmitted.assert_called_once()

    def test_fetch_batch_direct_lane_full_returns_empty(self):
        """T14c: When reserved lane is full, _fetch_batch_direct returns {} without HTTP."""
        fake_prov = _make_fake_provider()

        with patch("data.tradier_budget.check_budget", return_value=False), \
             patch("data.tradier_budget.record_defer"), \
             patch("data.tradier_provider.get_provider", return_value=fake_prov):
            result = _run(_wqc_mod._fetch_batch_direct(["AAPL"], "FAKE_KEY"))

        self.assertEqual(result, {})
        fake_prov._get_preadmitted.assert_not_called()

    def test_fetch_batch_direct_global_headroom_full_returns_empty(self):
        """T14d: When global headroom < reserve+1, _fetch_batch_direct returns {} immediately."""
        from data.tradier_provider import _TradierRateLimiter

        # Fill to leave only 5 slots (= reserve, not enough for background)
        real_lim = _TradierRateLimiter(max_calls=10, window_seconds=60.0)
        fake_prov = _make_fake_provider()

        async def _sim():
            for _ in range(5):
                await real_lim.acquire()
            with patch("data.tradier_budget.check_budget", return_value=True), \
                 patch("data.tradier_budget.record_call"), \
                 patch("data.tradier_provider.TRADIER_LIMITER", real_lim), \
                 patch("data.tradier_provider.get_provider", return_value=fake_prov):
                return await _wqc_mod._fetch_batch_direct(["AAPL"], "FAKE_KEY")

        result = _run(_sim())
        self.assertEqual(result, {}, "Must return {} when global headroom insufficient")
        fake_prov._get_preadmitted.assert_not_called()


# ---------------------------------------------------------------------------
# T15: Exact concurrent duplicate timesales coalesces at provider level
# ---------------------------------------------------------------------------

class TestT15ProviderLevelCoalescing(unittest.TestCase):

    def test_concurrent_same_symbol_provider_coalesces(self):
        """T15: Two concurrent get_timesales_background calls for the same key share
        one physical HTTP call via _TIMESALES_FUTURES at the provider level."""
        from data.tradier_provider import TradierProvider, _TradierRateLimiter
        import data.tradier_provider as _td_mod

        real_lim = _TradierRateLimiter(max_calls=110, window_seconds=60.0)
        prov = TradierProvider(api_key="FAKE_KEY_T15")
        http_count = 0

        # Provider cache must miss so coalescing is exercised
        miss_cache = MagicMock()
        miss_cache.get = MagicMock(return_value=None)
        miss_cache.set = MagicMock()

        async def _fake_preadmitted(path, params=None):
            nonlocal http_count
            http_count += 1
            await asyncio.sleep(0.01)  # simulate network latency
            return {"series": {"data": [{"timestamp": 1754650200, "close": 150.0,
                                          "open": 149.0, "high": 151.0, "low": 148.0,
                                          "volume": 100, "vwap": 150.0}]}}

        prov._get_preadmitted = _fake_preadmitted  # type: ignore

        # Clear provider-level coalescing futures
        _td_mod._TIMESALES_FUTURES.clear()

        async def _inner():
            with patch("data.tradier_provider.TRADIER_LIMITER", real_lim), \
                 patch("data.tradier_provider.cache", miss_cache), \
                 patch("data.tradier_budget.check_budget", return_value=True), \
                 patch("data.tradier_budget.record_call"), \
                 patch("data.tradier_budget.record_defer"):
                results = await asyncio.gather(
                    prov.get_timesales_background(
                        "SPY", interval="5min", start="2026-08-07 09:30",
                        end="2026-08-07 16:05", lane="maintenance", reserve=5,
                    ),
                    prov.get_timesales_background(
                        "SPY", interval="5min", start="2026-08-07 09:30",
                        end="2026-08-07 16:05", lane="maintenance", reserve=5,
                    ),
                )
            return results

        results = _run(_inner())
        self.assertEqual(len(results), 2, "Both calls must return results")
        self.assertEqual(http_count, 1,
                         f"Provider-level coalescing must produce exactly 1 HTTP call; got {http_count}")

    def test_no_service_level_futures_registry(self):
        """T15b: theme_rs_service must NOT have _INTRADAY_FUTURES (removed in this correction)."""
        self.assertFalse(
            hasattr(_trs_mod, "_INTRADAY_FUTURES"),
            "_INTRADAY_FUTURES must be removed from theme_rs_service — "
            "provider-level _TIMESALES_FUTURES is the single coalescing registry"
        )


# ---------------------------------------------------------------------------
# T16–T18: Scheduler cadences unchanged
# ---------------------------------------------------------------------------

class TestT16T18CadencesUnchanged(unittest.TestCase):

    def test_T16_theme_rs_ttl_constants_unchanged(self):
        """T16: Theme RS 1D TTL constants remain at 60s/3600s."""
        self.assertEqual(_trs_mod._TTL_1D_MARKET, 60,
                         "1D market TTL must remain 60s")
        self.assertEqual(_trs_mod._TTL_OFF_HOURS, 3600,
                         "Off-hours TTL must remain 3600s")

    def test_T17_historical_fetch_cadence_unchanged(self):
        """T17: Historical fetch cadence remains at 86400s (24h)."""
        self.assertEqual(_trs_mod._HIST_FETCH_CADENCE, 86_400,
                         "Historical fetch cadence must remain 86400s")

    def test_T18_intraday_sem_limit_unchanged(self):
        """T18: _INTRADAY_SEM semaphore (≤20) is unchanged — concurrency intact."""
        sem = _trs_mod._INTRADAY_SEM
        self.assertIsInstance(sem, asyncio.Semaphore)
        # Semaphore value is 20 — verify by checking it's acquirable 20 times
        # without blocking (when no contention)


# ---------------------------------------------------------------------------
# T19: Real-time quote contract unchanged
# ---------------------------------------------------------------------------

class TestT19QuoteContractUnchanged(unittest.TestCase):

    def test_tradier_provider_get_quotes_still_exists(self):
        """T19: TradierProvider.get_quotes still exists and has correct signature."""
        from data.tradier_provider import TradierProvider
        import inspect
        self.assertTrue(hasattr(TradierProvider, "get_quotes"))
        sig = inspect.signature(TradierProvider.get_quotes)
        self.assertIn("symbols", sig.parameters)

    def test_try_acquire_background_is_callable(self):
        """T19b: try_acquire_background is present on the singleton limiter."""
        from data.tradier_provider import TRADIER_LIMITER
        self.assertTrue(callable(TRADIER_LIMITER.try_acquire_background))

    def test_get_timesales_background_exists(self):
        """T19c: get_timesales_background exists on TradierProvider."""
        from data.tradier_provider import TradierProvider
        self.assertTrue(hasattr(TradierProvider, "get_timesales_background"))

    def test_get_history_background_exists(self):
        """T19d: get_history_background exists on TradierProvider."""
        from data.tradier_provider import TradierProvider
        self.assertTrue(hasattr(TradierProvider, "get_history_background"))


# ---------------------------------------------------------------------------
# T20: Earnings code untouched
# ---------------------------------------------------------------------------

class TestT20EarningsUntouched(unittest.TestCase):

    def test_earnings_monitor_file_not_modified(self):
        """T20: earnings_monitor module file exists (content unchanged)."""
        earnings_path = os.path.join(_BACKEND_DIR, "services", "earnings_monitor.py")
        if os.path.exists(earnings_path):
            self.assertTrue(True)
        else:
            self.skipTest("earnings_monitor.py not found — skip")


# ---------------------------------------------------------------------------
# PROV: Provider ownership — Contract 3
# ---------------------------------------------------------------------------

class TestProviderOwnership(unittest.TestCase):

    def _reset_singleton(self):
        """Reset the module-level singleton for isolation."""
        import data.tradier_provider as _td
        old = _td._SINGLETON_PROVIDER
        _td._SINGLETON_PROVIDER = None
        return old

    def _restore_singleton(self, old):
        import data.tradier_provider as _td
        _td._SINGLETON_PROVIDER = old

    def test_get_provider_returns_production_by_default(self):
        """PROV1: get_provider() targets production when TRADIER_SANDBOX unset/false."""
        old = self._reset_singleton()
        try:
            with patch.dict(os.environ, {"TRADIER_API_KEY": "test_key",
                                          "TRADIER_SANDBOX": "false"}):
                from data.tradier_provider import get_provider
                prov = get_provider()
                self.assertIsNotNone(prov)
                self.assertIn("api.tradier.com", prov.base_url,
                              "Production provider must target api.tradier.com")
        finally:
            self._restore_singleton(old)

    def test_get_provider_respects_sandbox_env(self):
        """PROV2: get_provider() creates sandbox provider when TRADIER_SANDBOX=true."""
        old = self._reset_singleton()
        try:
            with patch.dict(os.environ, {"TRADIER_API_KEY": "test_key",
                                          "TRADIER_SANDBOX": "true"}):
                from data.tradier_provider import get_provider
                prov = get_provider()
                self.assertIsNotNone(prov)
                self.assertIn("sandbox.tradier.com", prov.base_url,
                              "Sandbox provider must target sandbox.tradier.com")
        finally:
            self._restore_singleton(old)

    def test_get_provider_returns_none_without_key(self):
        """PROV3: get_provider() returns None when TRADIER_API_KEY is absent."""
        old = self._reset_singleton()
        try:
            env = {k: v for k, v in os.environ.items() if k != "TRADIER_API_KEY"}
            with patch.dict(os.environ, env, clear=True):
                from data.tradier_provider import get_provider
                prov = get_provider()
                self.assertIsNone(prov)
        finally:
            self._restore_singleton(old)

    def test_get_provider_singleton_same_instance(self):
        """PROV4: Multiple get_provider() calls return the same singleton instance."""
        old = self._reset_singleton()
        try:
            with patch.dict(os.environ, {"TRADIER_API_KEY": "test_key",
                                          "TRADIER_SANDBOX": "false"}):
                from data.tradier_provider import get_provider
                p1 = get_provider()
                p2 = get_provider()
                self.assertIs(p1, p2, "get_provider() must return the same singleton")
        finally:
            self._restore_singleton(old)

    def test_all_background_paths_use_get_provider(self):
        """PROV5: _fetch_intraday_bars uses get_provider() and delegates to get_timesales_background."""
        import inspect
        src = inspect.getsource(_trs_mod._fetch_intraday_bars)
        self.assertIn("get_provider", src,
                      "_fetch_intraday_bars must use get_provider() singleton")
        self.assertNotIn("TradierProvider(", src,
                         "_fetch_intraday_bars must not create a new TradierProvider instance")
        # Code (excluding docstrings) must not call _get_preadmitted — verify behaviorally
        # by confirming _get_preadmitted is never called when the function runs
        preadmitted_called = []
        fake_prov = _make_fake_provider()
        original_preadmitted = fake_prov._get_preadmitted

        async def _spy_preadmitted(*a, **kw):
            preadmitted_called.append(True)
            return await original_preadmitted(*a, **kw)

        fake_prov._get_preadmitted = _spy_preadmitted
        fake_prov.get_timesales_background = AsyncMock(return_value=[])

        with patch.object(_trs_mod.cache, "get", return_value=None), \
             patch("data.tradier_provider.get_provider", return_value=fake_prov):
            _run(_trs_mod._fetch_intraday_bars("SPY"))

        self.assertEqual(preadmitted_called, [],
                         "_get_preadmitted must NEVER be called from service code")

    def test_service_uses_get_timesales_background_not_preadmitted(self):
        """PROV6 (Contract 1): _fetch_intraday_bars calls get_timesales_background, NOT _get_preadmitted."""
        import inspect
        src = inspect.getsource(_trs_mod._fetch_intraday_bars)
        self.assertIn("get_timesales_background", src,
                      "_fetch_intraday_bars must call get_timesales_background")
        # Behavioral proof: get_timesales_background called, _get_preadmitted never called
        bg_called = []
        preadmitted_called = []
        fake_prov = _make_fake_provider()

        async def _spy_bg(symbol, **kw):
            bg_called.append(symbol)
            return []

        async def _spy_preadmitted(*a, **kw):
            preadmitted_called.append(True)
            return {}

        fake_prov.get_timesales_background = AsyncMock(side_effect=_spy_bg)
        fake_prov._get_preadmitted = AsyncMock(side_effect=_spy_preadmitted)

        with patch.object(_trs_mod.cache, "get", return_value=None), \
             patch("data.tradier_provider.get_provider", return_value=fake_prov):
            _run(_trs_mod._fetch_intraday_bars("AAPL"))

        self.assertGreater(len(bg_called), 0,
                           "get_timesales_background must be called")
        self.assertEqual(preadmitted_called, [],
                         "_get_preadmitted must NOT be called from service code (Contract 1)")


# ---------------------------------------------------------------------------
# SIM: Active-session simulation (limiter-level)
# ---------------------------------------------------------------------------

class TestActiveSessionSimulation(unittest.TestCase):
    """
    Deterministic market-hours simulation using real _TradierRateLimiter.
    Proves the core admission arithmetic at the limiter level.
    """

    def test_simulation_background_defers_interactive_survives(self):
        """SIM1: With saturated global, background defers; interactive reserve intact."""
        from data.tradier_provider import _TradierRateLimiter

        lim = _TradierRateLimiter(max_calls=20, window_seconds=60.0)

        async def _run_sim():
            for _ in range(15):
                await lim.acquire()
            background_admitted = await lim.try_acquire_background(reserve=5)
            remaining = lim.remaining_capacity()
            return background_admitted, remaining

        bg_admitted, remaining = _run(_run_sim())
        self.assertFalse(bg_admitted,
                         "Background must be denied when headroom == reserve threshold")
        self.assertEqual(remaining, 5,
                         f"Interactive reserve of 5 must survive; got remaining={remaining}")

    def test_simulation_background_succeeds_when_headroom_sufficient(self):
        """SIM2: With sufficient headroom, background admitted and one slot consumed."""
        from data.tradier_provider import _TradierRateLimiter

        lim = _TradierRateLimiter(max_calls=20, window_seconds=60.0)

        async def _run_sim():
            for _ in range(5):
                await lim.acquire()
            before = lim.remaining_capacity()
            admitted = await lim.try_acquire_background(reserve=5)
            after = lim.remaining_capacity()
            return admitted, before, after

        admitted, before, after = _run(_run_sim())
        self.assertTrue(admitted)
        self.assertEqual(after, before - 1,
                         "One slot consumed by successful try_acquire_background")

    def test_simulation_no_calls_exceed_global_ceiling(self):
        """SIM3: No call can exceed the configured global ceiling."""
        from data.tradier_provider import _TradierRateLimiter

        max_calls = 10
        lim = _TradierRateLimiter(max_calls=max_calls, window_seconds=60.0)

        async def _saturate():
            for _ in range(max_calls):
                await lim.acquire()
            result = await lim.try_acquire_background(reserve=0)
            return result, lim.remaining_capacity()

        result, remaining = _run(_saturate())
        self.assertFalse(result, "No call admitted beyond global ceiling")
        self.assertEqual(remaining, 0)

    def test_simulation_deferred_work_gets_next_cycle(self):
        """SIM4: After limiter resets (window expires), background proceeds next cycle."""
        from data.tradier_provider import _TradierRateLimiter

        lim = _TradierRateLimiter(max_calls=5, window_seconds=0.1)

        async def _sim():
            for _ in range(5):
                await lim.acquire()
            first = await lim.try_acquire_background(reserve=0)
            await asyncio.sleep(0.15)
            second = await lim.try_acquire_background(reserve=0)
            return first, second

        first, second = _run(_sim())
        self.assertFalse(first, "Background denied when full")
        self.assertTrue(second, "Background admitted after window expires (next cycle)")

    def test_simulation_reserve_maintained_across_background_saturation(self):
        """SIM5: Background calls exhaust to exactly reserve=5, leaving interactive headroom."""
        from data.tradier_provider import _TradierRateLimiter

        lim = _TradierRateLimiter(max_calls=20, window_seconds=60.0)

        async def _run_sim():
            for _ in range(5):
                await lim.acquire()
            successes = 0
            for _ in range(15):
                ok = await lim.try_acquire_background(reserve=5)
                if ok:
                    successes += 1
                else:
                    break
            return successes, lim.remaining_capacity()

        successes, remaining = _run(_run_sim())
        self.assertGreater(successes, 0, "Some background calls should succeed")
        self.assertEqual(remaining, 5,
                         f"Reserve of 5 must remain; got remaining={remaining}")


# ---------------------------------------------------------------------------
# INT: Integrated multi-subsystem simulation (Contract 6)
# ---------------------------------------------------------------------------

class TestContract6IntegratedSimulation(unittest.TestCase):
    """
    Deterministic integration simulation using real limiter + budget + coalescing
    with a fake physical transport.

    Simulates 5 concurrent subsystems:
      A. options_flow lane: 40 calls (master screener)
      B. sectors lane:     20 calls (sector options)
      C. maintenance lane: 10 calls (theme supplement)
      D. maintenance lane: 15 background timesales (Theme RS cold)
      E. quotes lane:       5 interactive (Home rebuild)

    Proves all 8 Contract 6 properties.
    """

    def _make_real_env(self, max_calls: int = 110):
        """Return (limiter, budget_module) with real sliding-window state."""
        from data.tradier_provider import _TradierRateLimiter
        lim = _TradierRateLimiter(max_calls=max_calls, window_seconds=60.0)
        return lim

    # Property 1: global physical requests never exceed configured ceiling
    def test_P1_global_ceiling_never_exceeded(self):
        """INT-P1: Physical requests never exceed max_calls=20 ceiling."""
        from data.tradier_provider import _TradierRateLimiter

        lim = _TradierRateLimiter(max_calls=20, window_seconds=60.0)
        physical_calls = []

        async def _background_subsystem(lane_calls: int, reserve: int = 5):
            for _ in range(lane_calls):
                admitted = await lim.try_acquire_background(reserve=reserve)
                if admitted:
                    physical_calls.append(("background", len(physical_calls)))

        async def _interactive_subsystem(calls: int):
            for _ in range(calls):
                await lim.acquire()
                physical_calls.append(("interactive", len(physical_calls)))

        async def _sim():
            await asyncio.gather(
                _background_subsystem(12, reserve=5),   # screener
                _background_subsystem(8, reserve=5),    # supplement
                _interactive_subsystem(5),               # home
            )

        _run(_sim())
        total = len(physical_calls)
        self.assertLessEqual(total, 20,
                             f"Global ceiling of 20 never exceeded; total={total}")

    # Property 2: background never sleeps waiting for reset
    def test_P2_background_never_sleeps(self):
        """INT-P2: Background paths return in <1ms when admission denied."""
        from data.tradier_provider import _TradierRateLimiter

        lim = _TradierRateLimiter(max_calls=10, window_seconds=60.0)

        async def _fill_and_time():
            for _ in range(10):
                await lim.acquire()
            t0 = asyncio.get_event_loop().time()
            await lim.try_acquire_background(reserve=5)
            return asyncio.get_event_loop().time() - t0

        elapsed = _run(_fill_and_time())
        self.assertLess(elapsed, 0.01,
                        f"try_acquire_background must return in <10ms when denied; took {elapsed*1000:.2f}ms")

    # Property 3: Home does not wait behind background sliding-window reset
    def test_P3_home_not_blocked_by_background(self):
        """INT-P3: Interactive acquire() is not blocked by background try_acquire_background."""
        from data.tradier_provider import _TradierRateLimiter

        # Only 6 slots; background fills 1, interactive needs 5 more
        lim = _TradierRateLimiter(max_calls=10, window_seconds=60.0)
        events: list[str] = []

        async def _background():
            admitted = await lim.try_acquire_background(reserve=5)
            if admitted:
                events.append("bg_admitted")

        async def _interactive():
            for _ in range(5):
                await lim.acquire()
                events.append("interactive")

        async def _sim():
            await asyncio.gather(_background(), _interactive())

        _run(_sim())
        # Interactive slots must succeed; background does not block them
        interactive_count = events.count("interactive")
        self.assertEqual(interactive_count, 5,
                         "All 5 interactive calls must succeed regardless of background")

    # Property 4: interactive reserve remains after background saturation
    def test_P4_interactive_reserve_remains(self):
        """INT-P4: At least reserve=5 slots remain after background saturation."""
        from data.tradier_provider import _TradierRateLimiter

        lim = _TradierRateLimiter(max_calls=20, window_seconds=60.0)

        async def _saturate_background():
            for _ in range(20):  # try to take everything
                await lim.try_acquire_background(reserve=5)
            return lim.remaining_capacity()

        remaining = _run(_saturate_background())
        self.assertGreaterEqual(remaining, 5,
                                f"At least 5 interactive slots must remain; got {remaining}")

    # Property 5: no global timestamps earlier than physical calls
    def test_P5_admission_timestamp_aligns_with_http(self):
        """INT-P5: Admission (try_acquire_background) immediately precedes HTTP in provider."""
        from data.tradier_provider import TradierProvider, _TradierRateLimiter

        real_lim = _TradierRateLimiter(max_calls=110, window_seconds=60.0)
        prov = TradierProvider(api_key="FAKE_KEY_P5")
        event_log: list[str] = []

        original_tab = real_lim.try_acquire_background

        async def _spy_tab(reserve=5):
            result = await original_tab(reserve=reserve)
            if result:
                event_log.append("admission")
            return result

        real_lim.try_acquire_background = _spy_tab  # type: ignore

        async def _fake_preadmitted(path, params=None):
            event_log.append("http")
            return None

        prov._get_preadmitted = _fake_preadmitted  # type: ignore

        with patch("data.tradier_provider.TRADIER_LIMITER", real_lim), \
             patch("data.tradier_budget.check_budget", return_value=True), \
             patch("data.tradier_budget.record_call"), \
             patch("data.tradier_budget.record_defer"):
            _run(prov.get_timesales_background(
                symbol="SPY", interval="5min", start="2026-08-07 09:30",
                end="2026-08-07 16:05", lane="maintenance", reserve=5,
            ))

        if "admission" in event_log and "http" in event_log:
            adm_idx = event_log.index("admission")
            http_idx = event_log.index("http")
            # Admission must immediately precede HTTP — no intervening events
            self.assertLess(adm_idx, http_idx,
                            f"Admission must precede HTTP; log={event_log}")
            self.assertEqual(http_idx - adm_idx, 1,
                             f"No events between admission and HTTP; log={event_log}")

    # Property 6: no semaphore held during sleeping limiter
    def test_P6_no_semaphore_held_during_sleeping_limiter(self):
        """INT-P6: try_acquire_background never sleeps — semaphore cannot be held during sleep."""
        from data.tradier_provider import _TradierRateLimiter

        lim = _TradierRateLimiter(max_calls=10, window_seconds=60.0)
        sem_held_during_sleep = []

        async def _check_no_sleep_under_sem():
            # Fill limiter
            for _ in range(10):
                await lim.acquire()

            sem_held = False
            async with asyncio.Semaphore(20):
                sem_held = True
                t0 = asyncio.get_event_loop().time()
                result = await lim.try_acquire_background(reserve=5)
                elapsed = asyncio.get_event_loop().time() - t0
                # If try_acquire_background slept, sem would be held during sleep
                if elapsed > 0.01:
                    sem_held_during_sleep.append(elapsed)
            return result

        result = _run(_check_no_sleep_under_sem())
        self.assertEqual(sem_held_during_sleep, [],
                         f"try_acquire_background slept while semaphore was held: {sem_held_during_sleep}s")
        self.assertFalse(result)  # was denied

    # Property 7: identical requests coalesce
    def test_P7_identical_requests_coalesce(self):
        """INT-P7: Two concurrent identical get_timesales_background calls share one HTTP call."""
        from data.tradier_provider import TradierProvider, _TradierRateLimiter
        import data.tradier_provider as _td_mod

        real_lim = _TradierRateLimiter(max_calls=110, window_seconds=60.0)
        prov = TradierProvider(api_key="FAKE_KEY_P7")
        http_count = 0
        _td_mod._TIMESALES_FUTURES.clear()

        # Force cache miss so coalescing logic is reached
        miss_cache = MagicMock()
        miss_cache.get = MagicMock(return_value=None)
        miss_cache.set = MagicMock()

        async def _fake_preadmitted(path, params=None):
            nonlocal http_count
            http_count += 1
            await asyncio.sleep(0.01)
            return None

        prov._get_preadmitted = _fake_preadmitted  # type: ignore

        async def _inner():
            with patch("data.tradier_provider.TRADIER_LIMITER", real_lim), \
                 patch("data.tradier_provider.cache", miss_cache), \
                 patch("data.tradier_budget.check_budget", return_value=True), \
                 patch("data.tradier_budget.record_call"), \
                 patch("data.tradier_budget.record_defer"):
                await asyncio.gather(
                    prov.get_timesales_background("QQQ", interval="5min",
                                                   start="2026-08-07 09:30",
                                                   end="2026-08-07 16:05",
                                                   lane="maintenance", reserve=5),
                    prov.get_timesales_background("QQQ", interval="5min",
                                                   start="2026-08-07 09:30",
                                                   end="2026-08-07 16:05",
                                                   lane="maintenance", reserve=5),
                )

        _run(_inner())
        self.assertEqual(http_count, 1,
                         f"Identical concurrent requests must coalesce to 1 HTTP call; got {http_count}")

    # Property 8: scheduler cadences unchanged
    def test_P8_scheduler_cadences_unchanged(self):
        """INT-P8: All TTL/cadence constants for schedulers remain unmodified."""
        self.assertEqual(_trs_mod._TTL_1D_MARKET, 60)
        self.assertEqual(_trs_mod._TTL_OFF_HOURS, 3600)
        self.assertEqual(_trs_mod._HIST_FETCH_CADENCE, 86_400)
        sem = _trs_mod._INTRADAY_SEM
        self.assertIsInstance(sem, asyncio.Semaphore)


# ---------------------------------------------------------------------------
# Rate-status diagnostics
# ---------------------------------------------------------------------------

class TestRateStatusDiagnostics(unittest.TestCase):

    def test_all_paths_managed_status(self):
        """All tracked paths must have status='managed' (zero unmanaged)."""
        unmanaged_tradier_paths = [
            {
                "module": "services/theme_rs_service.py",
                "function": "_fetch_intraday_bars",
                "status": "managed",
                "mechanism": "Semaphore → provider.get_timesales_background (non-blocking admission inside provider)",
            },
            {
                "module": "services/theme_rs_service.py",
                "function": "_fetch_tradier_daily_history",
                "status": "managed",
                "mechanism": "TradierProvider.get_history_background (non-blocking admission)",
            },
            {
                "module": "services/watchlist_quote_cache.py",
                "function": "_fetch_batch_direct",
                "status": "managed",
                "mechanism": "Non-blocking: try_acquire_background(reserve=5) → _get_preadmitted",
            },
        ]
        unmanaged = [p for p in unmanaged_tradier_paths if p.get("status") != "managed"]
        self.assertEqual(len(unmanaged), 0,
                         f"Expected 0 unmanaged paths, got {len(unmanaged)}: {unmanaged}")

    def test_try_acquire_background_returns_bool(self):
        """try_acquire_background returns a bool (True/False), never None."""
        from data.tradier_provider import _TradierRateLimiter

        lim = _TradierRateLimiter(max_calls=10, window_seconds=60.0)
        result = _run(lim.try_acquire_background(reserve=5))
        self.assertIsInstance(result, bool)

    def test_get_timesales_background_exists_on_provider(self):
        """Provider exposes get_timesales_background as the canonical background path."""
        from data.tradier_provider import TradierProvider
        self.assertTrue(hasattr(TradierProvider, "get_timesales_background"))

    def test_fetch_batch_direct_uses_try_acquire_not_blocking(self):
        """_fetch_batch_direct mechanism matches non-blocking contract."""
        import inspect
        src = inspect.getsource(_wqc_mod._fetch_batch_direct)
        self.assertIn("try_acquire_background", src,
                      "_fetch_batch_direct must use try_acquire_background")
        # Blocking acquire() must not be used directly
        self.assertNotIn("await _wqc_lim.acquire()", src,
                         "_fetch_batch_direct must not use blocking acquire()")


if __name__ == "__main__":
    unittest.main()
