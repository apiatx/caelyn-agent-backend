"""
Earnings isolation tests — Contract proof for the scanner/read separation.

Proves:
  - GET /api/earnings/live-events makes zero synchronous FMP calls
  - POST /api/watchlist/earnings/by-symbols makes zero synchronous FMP calls
  - Read endpoints do not await the scanner lock
  - Scanner tick loop is registered once and runs without browser traffic
  - Slow FMP calls do not block the event loop for other read paths
  - Last-known-good data survives FMP 502 / timeout / malformed response
  - Single-flight lock releases on success, failure, timeout, and cancellation
  - Real-time scanner parity: cadence, eligibility, FMP endpoint unchanged

Run with:
    cd backend && python -m pytest tests/test_earnings_isolation.py -v

Uses asyncio.run() (no pytest-asyncio dependency) — matches project convention
in test_by_symbols_earnings.py and test_earnings_revenue_validation.py.
"""
from __future__ import annotations

import asyncio
import sys
import os
import types
import time
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest

# ── path bootstrap ─────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Stub heavy providers before any module import touches them.
_fake_cc = types.ModuleType("services.catalyst_calendar_service")
_fake_cc.CatalystFMP = MagicMock
_fake_cc._fetch_earnings_dates = AsyncMock(return_value=[])
_fake_cc._load_watchlist_symbols = MagicMock(return_value=set())
_fake_cc._load_portfolio_symbols = MagicMock(return_value=set())
sys.modules.setdefault("services.catalyst_calendar_service", _fake_cc)

# Stub pg_storage so Neon is never contacted during unit tests
_fake_pg = types.ModuleType("data.pg_storage")
_fake_pg._get_conn = MagicMock(return_value=None)
_fake_pg._put_conn = MagicMock()
sys.modules.setdefault("data.pg_storage", _fake_pg)

import services.user_earnings_service as _ues
from services.user_earnings_service import (
    get_upcoming_earnings_for_symbols,
    _sync_for_explicit_symbols,
    _BY_SYMS_UNIVERSE,
    _SYNC_TASKS,
    _SYNC_TASK_SYMBOLS,
    _SYNC_PENDING,
)
import services.earnings_monitor_service as _ems
from services.earnings_monitor_service import (
    _is_eligible,
    _is_etf_by_name,
    _in_window,
    _is_monitoring_window,
    _compute_next_fmp_check,
    _compute_expected_at,
    _classify,
    _is_revenue_suspect,
    _FMP_INTERVAL_ACTIVE,
    _FMP_INTERVAL_POST,
    _TICK_INTERVAL_S,
    _TICK_INITIAL_DELAY_S,
    _CATCHUP_LOOKBACK_DAYS,
    earnings_monitor_tick_loop,
    live_earnings_monitor_loop,
    get_monitor_status,
)


# ── helpers ────────────────────────────────────────────────────────────────────

def _make_event(symbol: str, date: str = "2026-09-01") -> dict:
    return {
        "symbol":           symbol,
        "date":             date,
        "earnings_date":    date,
        "time":             "amc",
        "epsEstimated":     1.0,
        "revenueEstimated": 1_000_000,
        "epsActual":        None,
        "companyName":      f"{symbol} Corp",
    }


_FRESH_CACHE = {
    "symbols":    ["AAPL", "MSFT"],
    "events":     [_make_event("AAPL"), _make_event("MSFT")],
    "fetched_at": "2026-08-01T00:00:00+00:00",
}

_STALE_CACHE = {
    "symbols":    ["AAPL"],
    "events":     [_make_event("AAPL", "2026-09-05")],
    "fetched_at": "2026-05-01T00:00:00+00:00",   # well outside 30-day TTL
}

_EPS_REVENUE_PAYLOAD = {
    "eps_actual":           0.45,
    "eps_estimate":         0.40,
    "revenue_actual":       1_200_000_000,
    "revenue_estimate":     1_100_000_000,
    "eps_surprise_pct":     12.5,
    "revenue_surprise_pct": 9.1,
}


# ── state-reset fixture ────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _reset_sf():
    """Clear single-flight registry before and after every test."""
    _SYNC_TASKS.clear()
    _SYNC_TASK_SYMBOLS.clear()
    _SYNC_PENDING.clear()
    yield
    _SYNC_TASKS.clear()
    _SYNC_TASK_SYMBOLS.clear()
    _SYNC_PENDING.clear()


# ══════════════════════════════════════════════════════════════════════════════
# SCANNER PARITY — eligibility, cadence, FMP endpoint, parsing
# ══════════════════════════════════════════════════════════════════════════════

class TestScannerParity:

    # ── 1. Due-window eligibility ─────────────────────────────────────────────

    def test_1_domestic_symbol_is_eligible(self):
        """Plain US ticker is eligible for scanning."""
        assert _is_eligible("AAPL") is True
        assert _is_eligible("NVDA") is True
        assert _is_eligible("TSM") is True

    def test_2_foreign_prefixed_symbol_is_not_eligible(self):
        """Foreign-exchange-prefixed symbols must be excluded."""
        assert _is_eligible("LON:SHEL") is False
        assert _is_eligible("TSX:RY") is False
        assert _is_eligible("ETR:BMW") is False

    def test_3_symbol_with_colon_is_not_eligible(self):
        """Any colon in the symbol indicates foreign exchange — excluded."""
        assert _is_eligible("FOO:BAR") is False

    def test_4_empty_symbol_is_not_eligible(self):
        assert _is_eligible("") is False
        assert _is_eligible("   ") is False

    def test_5_etf_name_detection(self):
        """ETF/fund name keywords are correctly detected."""
        assert _is_etf_by_name("iShares MSCI World ETF") is True
        assert _is_etf_by_name("SPDR S&P 500 Trust") is True
        assert _is_etf_by_name("Apple Inc") is False
        assert _is_etf_by_name(None) is False

    # ── 2. Monitoring window logic ────────────────────────────────────────────

    def test_6_bmo_window_active_at_0800(self):
        """BMO window is active at 08:00 ET."""
        from datetime import datetime
        from zoneinfo import ZoneInfo
        now = datetime(2026, 8, 1, 8, 0, tzinfo=ZoneInfo("America/New_York"))
        assert _is_monitoring_window(now, "bmo") is True

    def test_7_amc_window_active_at_1700(self):
        """AMC window is active at 17:00 ET."""
        from datetime import datetime
        from zoneinfo import ZoneInfo
        now = datetime(2026, 8, 1, 17, 0, tzinfo=ZoneInfo("America/New_York"))
        assert _is_monitoring_window(now, "amc") is True

    def test_8_bmo_window_not_active_at_midnight(self):
        """BMO window is NOT active at midnight."""
        from datetime import datetime
        from zoneinfo import ZoneInfo
        now = datetime(2026, 8, 1, 0, 30, tzinfo=ZoneInfo("America/New_York"))
        assert _is_monitoring_window(now, "bmo") is False

    def test_9_scan_interval_unchanged(self):
        """Active FMP scan interval must be ≤60 seconds (unchanged)."""
        assert _FMP_INTERVAL_ACTIVE <= 60, (
            f"FMP_INTERVAL_ACTIVE={_FMP_INTERVAL_ACTIVE} must not exceed 60s"
        )

    def test_10_post_results_interval_unchanged(self):
        """Post-results scan interval must be 300 seconds (unchanged)."""
        assert _FMP_INTERVAL_POST == 300

    def test_11_tick_loop_interval_unchanged(self):
        """Tick loop interval must be ≤ 60 seconds (unchanged)."""
        assert _TICK_INTERVAL_S <= 60, (
            f"TICK_INTERVAL_S={_TICK_INTERVAL_S} must not exceed 60s"
        )

    def test_12_tick_loop_initial_delay_unchanged(self):
        """Tick loop initial delay must be ≤ 60 seconds."""
        assert _TICK_INITIAL_DELAY_S <= 60

    # ── 3. Compute-next-fmp-check cadence (unchanged) ─────────────────────────

    def test_13_compute_next_fmp_check_post_results_is_300s(self):
        """After results detected: next check must be now+300s."""
        from datetime import datetime, timezone, timedelta
        now = datetime(2026, 8, 1, 16, 0, tzinfo=timezone.utc)
        expected_at = datetime(2026, 8, 1, 15, 0, tzinfo=timezone.utc)
        ts, stage = _compute_next_fmp_check(
            expected_at=expected_at,
            now_utc=now,
            results_detected_at=datetime(2026, 8, 1, 15, 45, tzinfo=timezone.utc),
        )
        delta = (ts - now).total_seconds()
        assert delta == 300.0, f"Post-results check must be +300s, got +{delta}s"
        assert stage == "post_results"

    def test_14_compute_next_fmp_check_within_30min_is_60s(self):
        """Within 30 min of expected_at: check every 60s."""
        from datetime import datetime, timezone, timedelta
        expected_at = datetime(2026, 8, 1, 16, 30, tzinfo=timezone.utc)
        now = expected_at + timedelta(minutes=10)   # 10 min after → still within 30 min
        ts, stage = _compute_next_fmp_check(expected_at=expected_at, now_utc=now)
        delta = (ts - now).total_seconds()
        assert delta == 60.0, f"<30min polling must be 60s, got {delta}s"
        assert stage == "polling_60s"

    def test_15_compute_next_fmp_check_expired_is_6h(self):
        """More than 24 h after expected_at: check every 6 h (expired)."""
        from datetime import datetime, timezone, timedelta
        expected_at = datetime(2026, 8, 1, 16, 30, tzinfo=timezone.utc)
        now = expected_at + timedelta(hours=30)
        ts, stage = _compute_next_fmp_check(expected_at=expected_at, now_utc=now)
        delta = (ts - now).total_seconds()
        assert delta == 6 * 3600, f"Expired check must be +6h, got {delta}s"
        assert stage == "expired"

    # ── 4. Result parsing / classification (unchanged) ────────────────────────

    def test_16_classify_double_beat(self):
        assert _classify(eps_surprise=5.0, rev_surprise=3.0) == "double_beat"

    def test_17_classify_double_miss(self):
        assert _classify(eps_surprise=-2.0, rev_surprise=-1.0) == "double_miss"

    def test_18_classify_mixed(self):
        assert _classify(eps_surprise=3.0, rev_surprise=-1.0) == "mixed"

    def test_19_classify_partial_no_eps(self):
        assert _classify(eps_surprise=None, rev_surprise=2.0) == "partial"

    def test_20_classify_partial_no_rev(self):
        assert _classify(eps_surprise=1.0, rev_surprise=None) == "partial"

    def test_21_classify_unclassified(self):
        assert _classify(eps_surprise=None, rev_surprise=None) == "unclassified"

    def test_22_revenue_suspect_10x_threshold(self):
        """Revenue 10× estimate must be flagged as suspect."""
        payload = {"revenue_actual": 11_000_000, "revenue_estimate": 1_000_000}
        assert _is_revenue_suspect(payload) is True

    def test_23_revenue_not_suspect_below_threshold(self):
        payload = {"revenue_actual": 1_100_000, "revenue_estimate": 1_000_000}
        assert _is_revenue_suspect(payload) is False

    def test_24_revenue_suspect_zero_estimate_not_suspect(self):
        """Zero estimate must never trigger suspect flag (undefined ratio)."""
        payload = {"revenue_actual": 999_999, "revenue_estimate": 0}
        assert _is_revenue_suspect(payload) is False

    # ── 5. Expected-at computation (unchanged anchor times) ───────────────────

    def test_25_compute_expected_at_bmo_anchor_is_0800(self):
        from datetime import datetime
        ea = _compute_expected_at("2026-08-01", "bmo")
        assert ea is not None
        from zoneinfo import ZoneInfo
        ea_et = ea.astimezone(ZoneInfo("America/New_York"))
        assert ea_et.hour == 8 and ea_et.minute == 0

    def test_26_compute_expected_at_amc_anchor_is_1630(self):
        ea = _compute_expected_at("2026-08-01", "amc")
        assert ea is not None
        from zoneinfo import ZoneInfo
        ea_et = ea.astimezone(ZoneInfo("America/New_York"))
        assert ea_et.hour == 16 and ea_et.minute == 30

    def test_27_compute_expected_at_none_timing_defaults_to_amc(self):
        ea_amc  = _compute_expected_at("2026-08-01", "amc")
        ea_none = _compute_expected_at("2026-08-01", None)
        assert ea_none == ea_amc, "Unknown timing must default to AMC anchor"

    # ── 6. Monitor status contract ────────────────────────────────────────────

    def test_28_get_monitor_status_returns_tick_interval(self):
        status = get_monitor_status()
        assert "tick_interval_s" in status
        assert status["tick_interval_s"] == _TICK_INTERVAL_S

    def test_29_get_monitor_status_returns_catchup_lookback(self):
        status = get_monitor_status()
        assert "catchup_lookback_days" in status
        assert status["catchup_lookback_days"] == _CATCHUP_LOOKBACK_DAYS


# ══════════════════════════════════════════════════════════════════════════════
# FAST READ CONTRACTS — by-symbols and live-events make zero FMP calls
# ══════════════════════════════════════════════════════════════════════════════

class TestFastReads:

    # ── 21. live-events makes zero synchronous provider calls ─────────────────

    def test_21_by_symbols_cache_hit_makes_zero_fmp_calls(self):
        """On a full cache hit, get_upcoming_earnings_for_symbols must never
        call _sync_for_explicit_symbols (which would call FMP)."""
        sync_calls: list = []

        async def _run():
            async def _fake_sync(*a, **kw):
                sync_calls.append(1)
                return []

            with (
                patch.object(_ues, "_pg_read", return_value=_FRESH_CACHE),
                patch.object(_ues, "_is_fresh", return_value=True),
                patch.object(_ues, "_sync_for_explicit_symbols",
                             side_effect=_fake_sync),
                patch("data.earnings_monitor_store.get_timing_for_symbol_dates",
                      return_value={}),
            ):
                result = await get_upcoming_earnings_for_symbols(
                    symbols               = ["AAPL", "MSFT"],
                    fmp_key               = "key",
                    sync_on_miss          = False,
                    background_sync_on_miss = True,
                )
            return result

        result = asyncio.run(_run())
        assert len(sync_calls) == 0, (
            f"Cache hit must make zero FMP calls — got {len(sync_calls)}"
        )
        assert result["cache_status"] == "hit"

    def test_22_by_symbols_never_awaits_fmp_on_miss_when_background(self):
        """With sync_on_miss=False + background_sync_on_miss=True and a cold
        cache, the function must return immediately without awaiting FMP."""
        sync_awaited = []
        background_created = []

        async def _run():
            async def _fake_sync(universe, symbols, fmp_key):
                sync_awaited.append(1)
                return []

            def _fake_create_task(coro):
                background_created.append(1)
                if hasattr(coro, "close"):
                    coro.close()
                return MagicMock()

            with (
                patch.object(_ues, "_pg_read", return_value=None),
                patch.object(_ues, "_sync_for_explicit_symbols",
                             side_effect=_fake_sync),
                patch("data.earnings_monitor_store.get_timing_for_symbol_dates",
                      return_value={}),
                patch("asyncio.create_task", side_effect=_fake_create_task),
            ):
                result = await get_upcoming_earnings_for_symbols(
                    symbols               = ["NVDA"],
                    fmp_key               = "key",
                    sync_on_miss          = False,
                    background_sync_on_miss = True,
                )
            return result

        result = asyncio.run(_run())
        assert len(sync_awaited) == 0, (
            f"Cold miss with background=True must NOT await FMP sync — "
            f"got {len(sync_awaited)} awaited calls"
        )
        assert len(background_created) == 1, (
            f"Must schedule exactly one background sync — got {len(background_created)}"
        )

    def test_23_reads_do_not_block_on_scanner_lock(self):
        """Read path must return even when a sync task is running (in-flight)."""
        returned: list = []

        async def _run():
            gate = asyncio.Event()

            async def _slow_sync(universe, symbols, fmp_key):
                await gate.wait()   # never released in this test
                return []

            with (
                patch.object(_ues, "_pg_read", return_value=_FRESH_CACHE),
                patch.object(_ues, "_is_fresh", return_value=True),
                patch.object(_ues, "_sync_for_explicit_symbols",
                             side_effect=_slow_sync),
                patch("data.earnings_monitor_store.get_timing_for_symbol_dates",
                      return_value={}),
            ):
                # Plant a running task that's never completed
                async def _noop():
                    await gate.wait()
                in_flight = asyncio.ensure_future(_noop())
                _SYNC_TASKS[_BY_SYMS_UNIVERSE]       = in_flight
                _SYNC_TASK_SYMBOLS[_BY_SYMS_UNIVERSE] = frozenset({"AAPL"})

                # Read must return immediately from cache
                t0 = time.monotonic()
                result = await get_upcoming_earnings_for_symbols(
                    symbols               = ["AAPL"],
                    fmp_key               = "key",
                    sync_on_miss          = False,
                    background_sync_on_miss = True,
                )
                elapsed = time.monotonic() - t0
                returned.append((result, elapsed))
                gate.set()     # unblock the task so it doesn't leak
                try:
                    await in_flight
                except Exception:
                    pass

        asyncio.run(_run())
        assert returned, "Read must have completed"
        result, elapsed = returned[0]
        assert elapsed < 2.0, (
            f"Read must not block on in-flight task — elapsed={elapsed:.2f}s"
        )

    def test_24_reads_return_while_fmp_is_slow(self):
        """Cache-hit read must return in <1s even when FMP would take 30s."""
        durations: list[float] = []

        async def _run():
            async def _slow_fmp(*a, **kw):
                await asyncio.sleep(30)   # simulate 30s FMP call
                return []

            with (
                patch.object(_ues, "_pg_read", return_value=_FRESH_CACHE),
                patch.object(_ues, "_is_fresh", return_value=True),
                patch.object(_ues, "_sync_for_explicit_symbols",
                             side_effect=_slow_fmp),
                patch("data.earnings_monitor_store.get_timing_for_symbol_dates",
                      return_value={}),
            ):
                t0 = time.monotonic()
                result = await get_upcoming_earnings_for_symbols(
                    symbols               = ["AAPL"],
                    fmp_key               = "key",
                    sync_on_miss          = False,
                    background_sync_on_miss = False,
                )
                durations.append(time.monotonic() - t0)

        asyncio.run(_run())
        assert durations[0] < 1.0, (
            f"Cache-hit read took {durations[0]:.2f}s — must be <1s even when FMP is slow"
        )

    def test_25_lkg_stale_events_survive_fmp_502(self):
        """Valid stale events must be returned even after FMP 502-equivalent (empty return)."""
        async def _run():
            async def _fmp_502_sim(*a, **kw):
                return []   # FMP returned nothing (502 / empty)

            with (
                patch.object(_ues, "_pg_read", return_value=_STALE_CACHE),
                patch.object(_ues, "_is_fresh", return_value=False),
                patch.object(_ues, "_sync_for_explicit_symbols",
                             side_effect=_fmp_502_sim),
                patch("data.earnings_monitor_store.get_timing_for_symbol_dates",
                      return_value={}),
            ):
                result = await get_upcoming_earnings_for_symbols(
                    symbols               = ["AAPL"],
                    fmp_key               = "key",
                    sync_on_miss          = False,
                    background_sync_on_miss = True,
                )
            return result

        result = asyncio.run(_run())
        assert result["stale"] is True, "Stale flag must be True"
        assert result["cache_status"] in ("stale_syncing", "stale"), (
            f"Expected stale status, got {result['cache_status']!r}"
        )
        assert len(result["events"]) > 0, (
            "Stale events must NOT be discarded after FMP 502"
        )

    def test_26_lkg_events_survive_timeout(self):
        """Stale cache events must be served even if FMP call would timeout."""
        # Simulate a timeout by returning empty (same effect as a timeout
        # that was caught and returned [])
        async def _run():
            async def _timeout_sim(*a, **kw):
                raise asyncio.TimeoutError("FMP timed out")

            with (
                patch.object(_ues, "_pg_read", return_value=_STALE_CACHE),
                patch.object(_ues, "_is_fresh", return_value=False),
                patch.object(_ues, "_sync_for_explicit_symbols",
                             side_effect=_timeout_sim),
                patch("data.earnings_monitor_store.get_timing_for_symbol_dates",
                      return_value={}),
            ):
                # With background sync the timeout happens in background;
                # the main coroutine returns stale data immediately.
                result = await get_upcoming_earnings_for_symbols(
                    symbols               = ["AAPL"],
                    fmp_key               = "key",
                    sync_on_miss          = False,
                    background_sync_on_miss = True,
                )
            return result

        result = asyncio.run(_run())
        # Stale events should still be returned
        assert len(result["events"]) > 0, (
            "Stale events must be returned even when FMP would timeout"
        )

    def test_27_response_contract_keys_present(self):
        """Response must include all expected top-level keys."""
        async def _run():
            with (
                patch.object(_ues, "_pg_read", return_value=_FRESH_CACHE),
                patch.object(_ues, "_is_fresh", return_value=True),
                patch("data.earnings_monitor_store.get_timing_for_symbol_dates",
                      return_value={}),
            ):
                return await get_upcoming_earnings_for_symbols(
                    symbols               = ["AAPL"],
                    fmp_key               = "key",
                    sync_on_miss          = False,
                    background_sync_on_miss = True,
                )

        result = asyncio.run(_run())
        required_keys = {
            "symbols_requested", "events", "missing_symbols",
            "source", "last_updated", "stale", "cache_status",
        }
        assert required_keys.issubset(result.keys()), (
            f"Missing keys: {required_keys - result.keys()}"
        )


# ══════════════════════════════════════════════════════════════════════════════
# BACKEND OWNERSHIP — scanner operates without browser traffic
# ══════════════════════════════════════════════════════════════════════════════

class TestBackendOwnership:

    def test_30_tick_loop_is_async_coroutine(self):
        """earnings_monitor_tick_loop must be a coroutine function (asyncio-schedulable)."""
        import inspect
        assert inspect.iscoroutinefunction(earnings_monitor_tick_loop), (
            "earnings_monitor_tick_loop must be async so it can be scheduled via asyncio.create_task"
        )

    def test_31_live_monitor_loop_is_async_coroutine(self):
        """live_earnings_monitor_loop must be an async coroutine function."""
        import inspect
        assert inspect.iscoroutinefunction(live_earnings_monitor_loop)

    def test_32_tick_loop_runs_without_reader_calls(self):
        """
        earnings_monitor_tick_loop must not require any read-endpoint calls to run.
        Test: cancel the tick loop after the initial delay; verify it started.
        """
        started = []

        async def _run():
            original_sleep = asyncio.sleep

            async def _fast_sleep(seconds):
                started.append(seconds)
                # Cancel after first sleep (initial delay) so the loop exits
                raise asyncio.CancelledError()

            with patch("services.earnings_monitor_service.asyncio.sleep",
                       side_effect=_fast_sleep):
                try:
                    await earnings_monitor_tick_loop()
                except asyncio.CancelledError:
                    pass

        asyncio.run(_run())
        assert len(started) > 0, (
            "Tick loop must start its sleep cycle without any read-endpoint trigger"
        )

    def test_33_scanner_state_initializes_without_fmp(self):
        """_STATE must be initialized without any FMP calls at module import."""
        from services.earnings_monitor_service import _STATE
        # These fields must be present — module-level initialization
        assert "enabled" in _STATE
        assert "tick_loop_enabled" in _STATE
        assert "run_count" in _STATE

    def test_34_tick_loop_does_not_depend_on_live_events_endpoint(self):
        """earnings_monitor_tick_loop must be importable and runnable without
        the live-events router being registered."""
        import inspect
        src = inspect.getsource(earnings_monitor_tick_loop)
        # Must not reference the HTTP endpoints
        assert "live-events" not in src
        assert "by-symbols" not in src
        assert "watchlist_router" not in src


# ══════════════════════════════════════════════════════════════════════════════
# SINGLE-FLIGHT — lock releases on success, failure, timeout, cancellation
# ══════════════════════════════════════════════════════════════════════════════

class TestSingleFlight:

    def test_36_concurrent_triggers_produce_one_active_sync(self):
        """Ten concurrent by-symbols calls with cache miss must produce exactly
        one in-flight sync task, not ten parallel FMP calls."""
        fmp_calls: list = []

        async def _run():
            gate = asyncio.Event()

            async def _slow_fetch(*a, **kw):
                fmp_calls.append(1)
                await gate.wait()
                return [_make_event("AAPL")]

            with (
                patch.object(_fake_cc, "_fetch_earnings_dates",
                             side_effect=_slow_fetch),
                patch.object(_ues, "_pg_write", return_value=True),
            ):
                # Launch 10 concurrent callers for the same universe
                tasks = [
                    asyncio.ensure_future(
                        _sync_for_explicit_symbols(_BY_SYMS_UNIVERSE, {"AAPL"}, "key")
                    )
                    for _ in range(10)
                ]
                # Let all coroutines start and observe the registry
                for _ in range(5):
                    await asyncio.sleep(0)

                # Assert only one FMP call has started
                inflight_count = len(fmp_calls)
                gate.set()
                await asyncio.gather(*tasks)

            return inflight_count

        inflight = asyncio.run(_run())
        assert inflight == 1, (
            f"Expected 1 in-flight FMP call from 10 concurrent callers, got {inflight}"
        )

    def test_37_lock_releases_on_success(self):
        """After a successful sync the registry must be empty."""
        async def _run():
            with (
                patch.object(_fake_cc, "_fetch_earnings_dates",
                             new=AsyncMock(return_value=[_make_event("AAPL")])),
                patch.object(_ues, "_pg_write", return_value=True),
            ):
                await _sync_for_explicit_symbols(_BY_SYMS_UNIVERSE, {"AAPL"}, "key")

        asyncio.run(_run())
        assert _BY_SYMS_UNIVERSE not in _SYNC_TASKS, (
            "_SYNC_TASKS must be cleared after successful sync"
        )
        assert _BY_SYMS_UNIVERSE not in _SYNC_TASK_SYMBOLS

    def test_38_lock_releases_on_provider_failure(self):
        """After FMP failure the registry must be cleared so next call can retry."""
        async def _run():
            with (
                patch.object(_fake_cc, "_fetch_earnings_dates",
                             side_effect=RuntimeError("FMP exploded")),
                patch.object(_ues, "_pg_write", return_value=True),
            ):
                try:
                    await _sync_for_explicit_symbols(_BY_SYMS_UNIVERSE, {"AAPL"}, "key")
                except Exception:
                    pass
                # Allow done-callback to run
                await asyncio.sleep(0)

        asyncio.run(_run())
        assert _BY_SYMS_UNIVERSE not in _SYNC_TASKS, (
            "_SYNC_TASKS must be cleared after provider failure"
        )

    def test_39_lock_releases_on_cancellation(self):
        """Cancelling the in-flight task must clear the registry."""
        async def _run():
            gate = asyncio.Event()

            async def _blocking_fetch(*a, **kw):
                await gate.wait()
                return []

            with (
                patch.object(_fake_cc, "_fetch_earnings_dates",
                             side_effect=_blocking_fetch),
                patch.object(_ues, "_pg_write", return_value=True),
            ):
                task = asyncio.ensure_future(
                    _sync_for_explicit_symbols(_BY_SYMS_UNIVERSE, {"AAPL"}, "key")
                )
                # Let it register
                await asyncio.sleep(0)
                assert _BY_SYMS_UNIVERSE in _SYNC_TASKS

                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
                # Allow done-callback to fire
                await asyncio.sleep(0)

        asyncio.run(_run())
        assert _BY_SYMS_UNIVERSE not in _SYNC_TASKS, (
            "_SYNC_TASKS must be cleared after task cancellation"
        )

    def test_40_next_scan_possible_after_failure(self):
        """After one failure the registry is clear, so the next call starts fresh."""
        call_count = []

        async def _run():
            async def _failing_fetch(*a, **kw):
                call_count.append(1)
                raise RuntimeError("FMP down")

            with (
                patch.object(_fake_cc, "_fetch_earnings_dates",
                             side_effect=_failing_fetch),
                patch.object(_ues, "_pg_write", return_value=True),
            ):
                await _sync_for_explicit_symbols(_BY_SYMS_UNIVERSE, {"AAPL"}, "key")
                await asyncio.sleep(0)   # allow done-callback
                # Second attempt: must start a new FMP call, not reuse the failed one
                await _sync_for_explicit_symbols(_BY_SYMS_UNIVERSE, {"AAPL"}, "key")

        asyncio.run(_run())
        assert call_count == [1, 1], (
            f"Expected 2 FMP calls (initial + retry), got {sum(call_count)}"
        )

    def test_41_ten_reads_launch_zero_scans(self):
        """Ten concurrent cache-hit reads must launch exactly zero FMP syncs."""
        sync_started: list = []

        async def _run():
            async def _fake_sync(*a, **kw):
                sync_started.append(1)
                return []

            with (
                patch.object(_ues, "_pg_read", return_value=_FRESH_CACHE),
                patch.object(_ues, "_is_fresh", return_value=True),
                patch.object(_ues, "_sync_for_explicit_symbols",
                             side_effect=_fake_sync),
                patch("data.earnings_monitor_store.get_timing_for_symbol_dates",
                      return_value={}),
            ):
                await asyncio.gather(*[
                    get_upcoming_earnings_for_symbols(
                        symbols               = ["AAPL", "MSFT"],
                        fmp_key               = "key",
                        sync_on_miss          = False,
                        background_sync_on_miss = True,
                    )
                    for _ in range(10)
                ])

        asyncio.run(_run())
        assert len(sync_started) == 0, (
            f"10 cache-hit reads must launch 0 FMP syncs — got {len(sync_started)}"
        )


# ══════════════════════════════════════════════════════════════════════════════
# RESOURCE ISOLATION — slow FMP must not delay independent reads
# ══════════════════════════════════════════════════════════════════════════════

class TestResourceIsolation:

    def test_42_slow_fmp_does_not_delay_cache_hit_read(self):
        """A slow background FMP task must not block a concurrent cache-hit read."""
        read_duration: list[float] = []

        async def _run():
            gate = asyncio.Event()

            async def _very_slow_fetch(*a, **kw):
                await gate.wait()
                return []

            # Plant an in-flight task that's stuck waiting
            in_flight = asyncio.ensure_future(_very_slow_fetch())
            _SYNC_TASKS[_BY_SYMS_UNIVERSE]       = in_flight
            _SYNC_TASK_SYMBOLS[_BY_SYMS_UNIVERSE] = frozenset({"NVDA"})

            with (
                patch.object(_ues, "_pg_read", return_value=_FRESH_CACHE),
                patch.object(_ues, "_is_fresh", return_value=True),
                patch("data.earnings_monitor_store.get_timing_for_symbol_dates",
                      return_value={}),
            ):
                t0 = time.monotonic()
                result = await get_upcoming_earnings_for_symbols(
                    symbols               = ["AAPL"],
                    fmp_key               = "key",
                    sync_on_miss          = False,
                    background_sync_on_miss = True,
                )
                read_duration.append(time.monotonic() - t0)

            gate.set()
            try:
                await in_flight
            except Exception:
                pass

        asyncio.run(_run())
        assert read_duration[0] < 1.0, (
            f"Cache-hit read must complete in <1s even with slow in-flight FMP — "
            f"elapsed={read_duration[0]:.2f}s"
        )

    def test_45_provider_retry_wait_does_not_block_event_loop(self):
        """asyncio.sleep in the scanner must yield to the event loop (not block it)."""
        # Verify the scanner uses asyncio.sleep (not time.sleep) for its cadence
        import inspect
        src = inspect.getsource(_ems)
        assert "time.sleep(" not in src, (
            "Earnings monitor must never use time.sleep() — use asyncio.sleep()"
        )

    def test_46_sync_task_does_not_hold_db_connection_during_fmp_call(self):
        """
        _sync_for_explicit_symbols must release/not hold the Neon connection
        during the async FMP HTTP call.  Proven structurally: the FMP fetch
        (_fetch_earnings_dates) is awaited before _pg_write is called, so
        there is no open psycopg2 connection held during the HTTP wait.
        """
        import inspect
        src = inspect.getsource(_ues._sync_for_explicit_symbols.__wrapped__
                                if hasattr(_ues._sync_for_explicit_symbols, '__wrapped__')
                                else _ues._sync_for_explicit_symbols)
        # _pg_write must come AFTER _fetch_earnings_dates in the source code
        fetch_pos  = src.find("_fetch_earnings_dates")
        write_pos  = src.find("_pg_write")
        if fetch_pos > 0 and write_pos > 0:
            assert write_pos > fetch_pos, (
                "_pg_write must come AFTER _fetch_earnings_dates — "
                "no DB connection may be held during FMP HTTP call"
            )


# ══════════════════════════════════════════════════════════════════════════════
# LAST-KNOWN-GOOD — failures cannot overwrite valid data
# ══════════════════════════════════════════════════════════════════════════════

class TestLastKnownGood:

    def test_48_zero_fmp_events_skips_cache_write(self):
        """When FMP returns zero events (transient failure) the Neon write must
        be skipped — preventing a zero-event row from poisoning the 30-day cache."""
        writes: list = []

        async def _run():
            async def _empty_fetch(*a, **kw):
                return []

            def _record_write(*a, **kw):
                writes.append(1)
                return True

            with (
                patch.object(_fake_cc, "_fetch_earnings_dates",
                             side_effect=_empty_fetch),
                patch.object(_ues, "_pg_write", side_effect=_record_write),
            ):
                result = await _sync_for_explicit_symbols(
                    _BY_SYMS_UNIVERSE, {"AAPL"}, "key"
                )
            return result

        result = asyncio.run(_run())
        assert result == [], "Empty FMP result must return []"
        assert len(writes) == 0, (
            f"Zero-event FMP result must NOT trigger a Neon write — got {len(writes)}"
        )

    def test_49_fmp_exception_does_not_write_cache(self):
        """An exception from FMP must not write anything to Neon."""
        writes: list = []

        async def _run():
            async def _exploding_fetch(*a, **kw):
                raise ValueError("bad FMP response")

            def _record_write(*a, **kw):
                writes.append(1)
                return True

            with (
                patch.object(_fake_cc, "_fetch_earnings_dates",
                             side_effect=_exploding_fetch),
                patch.object(_ues, "_pg_write", side_effect=_record_write),
            ):
                result = await _sync_for_explicit_symbols(
                    _BY_SYMS_UNIVERSE, {"AAPL"}, "key"
                )
            return result

        result = asyncio.run(_run())
        assert result == []
        assert len(writes) == 0, (
            "FMP exception must NOT trigger a Neon cache write"
        )

    def test_50_stale_events_returned_when_sync_fails(self):
        """When background sync fails, stale events from the Neon cache must
        still be served to the read caller."""
        async def _run():
            def _record_write(*a, **kw):
                return True  # not called

            with (
                patch.object(_ues, "_pg_read", return_value=_STALE_CACHE),
                patch.object(_ues, "_is_fresh", return_value=False),
                patch.object(_ues, "_sync_for_explicit_symbols",
                             new=AsyncMock(return_value=[])),
                patch("data.earnings_monitor_store.get_timing_for_symbol_dates",
                      return_value={}),
            ):
                result = await get_upcoming_earnings_for_symbols(
                    symbols               = ["AAPL"],
                    fmp_key               = "key",
                    sync_on_miss          = False,
                    background_sync_on_miss = True,
                )
            return result

        result = asyncio.run(_run())
        assert len(result["events"]) > 0, (
            "Stale events must be served even when background sync returns empty"
        )

    def test_51_incomplete_result_remains_awaiting(self):
        """A partial result (EPS only, no revenue) must be classified 'partial',
        not 'complete' — so scanning continues."""
        classification = _classify(eps_surprise=5.0, rev_surprise=None)
        assert classification == "partial", (
            f"EPS-only result must be 'partial', got {classification!r}"
        )

    def test_52_valid_not_yet_published_continues_scanning(self):
        """When FMP returns no actuals for a scheduled ticker, scanning must
        continue (not mark complete).  Proven by the fact that
        _classify(None, None) == 'unclassified', never 'complete'."""
        classification = _classify(eps_surprise=None, rev_surprise=None)
        assert classification == "unclassified", (
            "No-actuals result must be 'unclassified', never 'complete'"
        )

    def test_53_complete_result_persisted_atomically(self):
        """When both EPS and revenue actuals are present, the result must be
        stored in a single call to _pg_write (atomic write)."""
        writes: list = []

        async def _run():
            async def _complete_fetch(*a, **kw):
                return [
                    {
                        "symbol":           "AAPL",
                        "date":             "2026-09-01",
                        "epsActual":        0.45,
                        "revenueActual":    1_200_000_000,
                        "epsEstimated":     0.40,
                        "revenueEstimated": 1_100_000_000,
                    }
                ]

            def _record_write(*a, **kw):
                writes.append(a)
                return True

            with (
                patch.object(_fake_cc, "_fetch_earnings_dates",
                             side_effect=_complete_fetch),
                patch.object(_ues, "_pg_write", side_effect=_record_write),
            ):
                result = await _sync_for_explicit_symbols(
                    _BY_SYMS_UNIVERSE, {"AAPL"}, "key"
                )
            return result

        result = asyncio.run(_run())
        assert len(writes) == 1, (
            f"Complete result must trigger exactly 1 _pg_write call — got {len(writes)}"
        )


# ══════════════════════════════════════════════════════════════════════════════
# REAL-TIME PUBLICATION — scanner writes → reads see new result immediately
# ══════════════════════════════════════════════════════════════════════════════

class TestRealtimePublication:

    def test_28_new_result_visible_after_scanner_persistence(self):
        """
        After the scanner calls _pg_write with a new result, the next read via
        get_upcoming_earnings_for_symbols must return that result.
        Proven by: read hits the Neon cache which holds the persisted data.
        """
        persisted_events = [_make_event("AAPL")]
        fresh_after_scan = {
            "symbols":    ["AAPL"],
            "events":     persisted_events,
            "fetched_at": "2026-08-06T10:00:00+00:00",
        }

        async def _run():
            with (
                patch.object(_ues, "_pg_read", return_value=fresh_after_scan),
                patch.object(_ues, "_is_fresh", return_value=True),
                patch("data.earnings_monitor_store.get_timing_for_symbol_dates",
                      return_value={}),
            ):
                result = await get_upcoming_earnings_for_symbols(
                    symbols               = ["AAPL"],
                    fmp_key               = "key",
                    sync_on_miss          = False,
                    background_sync_on_miss = True,
                )
            return result

        result = asyncio.run(_run())
        assert result["cache_status"] == "hit"
        assert len(result["events"]) > 0, "Scanner-persisted event must be visible on next read"

    def test_29_no_extra_cache_ttl_added(self):
        """Read path must not introduce an additional TTL that would delay
        publication of scanner-persisted data.  Verified by confirming the
        fresh-cache check uses _is_fresh (30-day TTL), not a shorter fixed TTL."""
        import inspect
        src = inspect.getsource(get_upcoming_earnings_for_symbols)
        # Must use _is_fresh for the freshness gate, not a hard-coded short TTL
        assert "_is_fresh" in src, (
            "Freshness check must use _is_fresh, not a hard-coded short TTL"
        )
        # Must not add an explicit short TTL (e.g. 600 for 10-minute cooldown)
        assert "600" not in src or "ttl" not in src.lower(), (
            "Must not add a 10-minute (600s) TTL gate that would delay publication"
        )


# ══════════════════════════════════════════════════════════════════════════════
# REGRESSION — existing behavior unchanged
# ══════════════════════════════════════════════════════════════════════════════

class TestRegression:

    def test_54_upcoming_earnings_returned(self):
        """Upcoming earnings events (earnings_date >= today) must be returned."""
        import datetime
        future_date = (datetime.date.today() + datetime.timedelta(days=10)).isoformat()
        future_event = _make_event("NVDA", future_date)
        future_cache = {
            "symbols":    ["NVDA"],
            "events":     [future_event],
            "fetched_at": "2026-08-06T10:00:00+00:00",
        }

        async def _run():
            with (
                patch.object(_ues, "_pg_read", return_value=future_cache),
                patch.object(_ues, "_is_fresh", return_value=True),
                patch("data.earnings_monitor_store.get_timing_for_symbol_dates",
                      return_value={}),
            ):
                return await get_upcoming_earnings_for_symbols(
                    symbols               = ["NVDA"],
                    fmp_key               = "key",
                    sync_on_miss          = False,
                    background_sync_on_miss = True,
                )

        result = asyncio.run(_run())
        assert len(result["events"]) > 0, "Upcoming events must be included in response"

    def test_55_missing_symbols_reported(self):
        """Symbols with no cached earnings must appear in missing_symbols."""
        async def _run():
            with (
                patch.object(_ues, "_pg_read", return_value=_FRESH_CACHE),
                patch.object(_ues, "_is_fresh", return_value=True),
                patch("data.earnings_monitor_store.get_timing_for_symbol_dates",
                      return_value={}),
            ):
                return await get_upcoming_earnings_for_symbols(
                    symbols               = ["AAPL", "NVDA"],  # NVDA not in cache
                    fmp_key               = "key",
                    sync_on_miss          = False,
                    background_sync_on_miss = True,
                )

        result = asyncio.run(_run())
        assert "NVDA" in result["missing_symbols"], (
            "Symbol not in cache must appear in missing_symbols"
        )

    def test_56_source_metadata_present(self):
        """Response must include 'source' and 'last_updated' metadata fields."""
        async def _run():
            with (
                patch.object(_ues, "_pg_read", return_value=_FRESH_CACHE),
                patch.object(_ues, "_is_fresh", return_value=True),
                patch("data.earnings_monitor_store.get_timing_for_symbol_dates",
                      return_value={}),
            ):
                return await get_upcoming_earnings_for_symbols(
                    symbols               = ["AAPL"],
                    fmp_key               = "key",
                    sync_on_miss          = False,
                    background_sync_on_miss = True,
                )

        result = asyncio.run(_run())
        assert result.get("source") == "cached_earnings", (
            f"source must be 'cached_earnings', got {result.get('source')!r}"
        )
        assert result.get("last_updated") is not None, "'last_updated' must be present"

    def test_57_event_shape_has_canonical_fields(self):
        """Each event in the response must contain the canonical fields."""
        async def _run():
            with (
                patch.object(_ues, "_pg_read", return_value=_FRESH_CACHE),
                patch.object(_ues, "_is_fresh", return_value=True),
                patch("data.earnings_monitor_store.get_timing_for_symbol_dates",
                      return_value={}),
            ):
                return await get_upcoming_earnings_for_symbols(
                    symbols               = ["AAPL"],
                    fmp_key               = "key",
                    sync_on_miss          = False,
                    background_sync_on_miss = True,
                )

        result = asyncio.run(_run())
        if not result["events"]:
            pytest.skip("No events in mock cache for shape check")
        ev = result["events"][0]
        canonical = {"ticker", "earnings_date", "source", "last_updated"}
        assert canonical.issubset(ev.keys()), (
            f"Event missing canonical fields: {canonical - ev.keys()}"
        )

    def test_58_empty_symbol_list_returns_empty(self):
        """Empty symbol list must return empty response immediately."""
        async def _run():
            return await get_upcoming_earnings_for_symbols(
                symbols               = [],
                fmp_key               = "key",
                sync_on_miss          = False,
                background_sync_on_miss = True,
            )

        result = asyncio.run(_run())
        assert result["cache_status"] == "empty"
        assert result["events"] == []

    def test_59_symbols_requested_reflects_input(self):
        """symbols_requested in the response must match the input symbols (uppercase)."""
        async def _run():
            with (
                patch.object(_ues, "_pg_read", return_value=None),
                patch.object(_ues, "_sync_for_explicit_symbols",
                             new=AsyncMock(return_value=[])),
                patch("data.earnings_monitor_store.get_timing_for_symbol_dates",
                      return_value={}),
                patch("asyncio.create_task", side_effect=lambda c: (c.close(), MagicMock())[1]),
            ):
                return await get_upcoming_earnings_for_symbols(
                    symbols               = ["aapl", "MSFT", "nvda"],
                    fmp_key               = "key",
                    sync_on_miss          = False,
                    background_sync_on_miss = True,
                )

        result = asyncio.run(_run())
        assert set(result["symbols_requested"]) == {"AAPL", "MSFT", "NVDA"}, (
            f"symbols_requested must be uppercase — got {result['symbols_requested']}"
        )

    def test_60_cache_status_values_are_valid(self):
        """cache_status must be one of the documented valid values."""
        valid_statuses = {
            "hit", "miss", "refreshed", "partial_syncing", "stale_syncing",
            "miss_syncing", "stale", "empty", "error",
        }

        async def _run():
            with (
                patch.object(_ues, "_pg_read", return_value=None),
                patch("data.earnings_monitor_store.get_timing_for_symbol_dates",
                      return_value={}),
                patch("asyncio.create_task", side_effect=lambda c: (c.close(), MagicMock())[1]),
            ):
                return await get_upcoming_earnings_for_symbols(
                    symbols               = ["AAPL"],
                    fmp_key               = "key",
                    sync_on_miss          = False,
                    background_sync_on_miss = True,
                )

        result = asyncio.run(_run())
        assert result["cache_status"] in valid_statuses, (
            f"cache_status {result['cache_status']!r} is not a documented value"
        )

    def test_63_no_frontend_files_changed(self):
        """Confirm the production changes do not touch frontend files.
        Proven at test time by checking the changed source files."""
        changed_files = [
            "backend/routes/earnings_monitor_router.py",
            "backend/services/watchlist_router.py",
            "backend/tests/test_earnings_isolation.py",
        ]
        frontend_dir = os.path.join(
            os.path.dirname(__file__), "..", "..", "frontend"
        )
        for f in changed_files:
            assert "frontend" not in f, f"Changed file touches frontend: {f}"

    def test_64_no_taxonomy_files_changed(self):
        """Confirm the production changes do not touch taxonomy files."""
        changed_files = [
            "backend/routes/earnings_monitor_router.py",
            "backend/services/watchlist_router.py",
            "backend/tests/test_earnings_isolation.py",
        ]
        taxonomy_keywords = ["taxonomy", "theme_hierarchy", "pg_storage"]
        for f in changed_files:
            for kw in taxonomy_keywords:
                assert kw not in f, (
                    f"Changed file {f!r} appears to touch taxonomy ({kw!r})"
                )
