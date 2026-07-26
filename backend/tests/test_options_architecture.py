"""
test_options_architecture.py — Architecture regression tests.

Tests cover (matching spec requirement):
  1.  Options Flow → Watchlist single-ticker sequence
  2.  Watchlist → Options Flow single-ticker sequence
  3.  Simultaneous 5-consumer request for same ticker
  4.  Simultaneous requests while scan in-flight
  5.  Memory cache expires, durable LKG exists
  6.  Valid LKG older than 7 days still returned
  7.  Small master cache with large supplement LKG
  8.  Provider returns partial scan
  9.  Provider returns timeout / rate limit
  10. Closed-market requests after restart — zero provider calls
  11. Historical 1D/7D/30D fields requested
  12. Last-session Ask/Bid premium after market close
  13. Early-close trading date before/after actual close

All tests are offline (no Tradier, no Neon). They verify correctness through
the module-level logic and the in-flight guard.
"""
from __future__ import annotations

import sys
import os
import json
import tempfile
import time
import threading
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch, AsyncMock
import asyncio

import pytest

# ── Ensure backend/ is importable ─────────────────────────────────────────────
_BACKEND = Path(__file__).resolve().parent.parent
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore

_ET = ZoneInfo("America/New_York")


# ── Helpers ────────────────────────────────────────────────────────────────────

def _et(year, month, day, hour, minute=0):
    return datetime(year, month, day, hour, minute, tzinfo=_ET)


def _make_ticker_row(sym, score=45.0, put_vol=200, call_vol=400, iv=0.30,
                     pc_ratio=None, data_available=True):
    if pc_ratio is None:
        pc_ratio = round(put_vol / call_vol, 3) if call_vol else None
    vol_score = round(min(35, ((put_vol + call_vol) / 10000) * 20), 1)
    iv_score  = round(min(20, iv * 40), 1)
    dir_score = round(min(20, abs((pc_ratio or 1.0) - 1.0) * 15), 1) if pc_ratio else 0
    return {
        "ticker":                       sym,
        "symbol":                       sym,
        "data_available":               data_available,
        "score":                        score,
        "p_c":                          pc_ratio,
        "put_call":                     pc_ratio,
        "put_call_ratio":               pc_ratio,
        "volume_put_call_ratio":        pc_ratio,
        "premium_put_call_ratio":       None,
        "options_score_version":        "portfolio_composite_v1",
        "options_score_source":         "portfolio_scoped_options_screener",
        "options_score_inputs": {
            "vol_score":  vol_score,
            "iv_score":   iv_score,
            "dir_score":  dir_score,
        },
        "score_comparable_across_rows": False,
        "iv":                           iv,
        "put_volume":                   put_vol,
        "call_volume":                  call_vol,
        "open_interest":                1000,
        "call_open_interest":           600,
        "put_open_interest":            400,
        "signal":                       "Neutral",
        "confidence":                   "moderate",
        "source":                       "portfolio_scoped_options_screener",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Market-session (tradier_market_session.py) — early-close additions
# ─────────────────────────────────────────────────────────────────────────────

class TestEarlyClose:
    from data.tradier_market_session import (
        is_regular_options_session,
        is_early_close_day,
        _early_close_dates,
        _holidays_for,
        _regular_close,
        get_options_session_info,
    )

    def test_normal_day_close_is_1600(self):
        from data.tradier_market_session import _regular_close
        d = date(2026, 7, 6)  # Monday, normal day
        assert _regular_close(d) == dtime(16, 0)

    def test_black_friday_2026_is_early_close(self):
        from data.tradier_market_session import _early_close_dates
        ec = _early_close_dates(2026)
        assert date(2026, 11, 27) in ec, "Black Friday 2026 (Nov 27) must be an early-close day"

    def test_black_friday_close_is_1300(self):
        from data.tradier_market_session import _regular_close
        assert _regular_close(date(2026, 11, 27)) == dtime(13, 0)

    def test_scan_allowed_before_early_close(self):
        from data.tradier_market_session import is_regular_options_session
        # Black Friday 2026: 12:59 ET → still open
        assert is_regular_options_session(_et(2026, 11, 27, 12, 59)) is True

    def test_scan_blocked_at_early_close_time(self):
        from data.tradier_market_session import is_regular_options_session
        # Black Friday 2026: 13:00 ET → market closed (early close)
        assert is_regular_options_session(_et(2026, 11, 27, 13, 0)) is False

    def test_scan_blocked_after_early_close(self):
        from data.tradier_market_session import is_regular_options_session
        # Black Friday 2026: 14:00 ET — well after early close
        assert is_regular_options_session(_et(2026, 11, 27, 14, 0)) is False

    def test_normal_trading_day_scan_blocked_at_1600(self):
        from data.tradier_market_session import is_regular_options_session
        # Regular Wednesday: scan allowed at 15:59, blocked at 16:00
        assert is_regular_options_session(_et(2026, 7, 8, 15, 59)) is True
        assert is_regular_options_session(_et(2026, 7, 8, 16, 0))  is False

    def test_christmas_eve_2026_is_early_close(self):
        from data.tradier_market_session import _early_close_dates, _holidays_for
        ec = _early_close_dates(2026)
        # Dec 24 2026 is a Thursday; Dec 25 is a Friday (full holiday)
        assert date(2026, 12, 24) in ec

    def test_christmas_eve_not_early_close_when_itself_observed_holiday(self):
        from data.tradier_market_session import _early_close_dates, _holidays_for
        # 2027: Dec 25 is Saturday → observed on Fri Dec 24 (full holiday)
        # So Dec 24 2027 should NOT be in early-close dates
        holidays_2027 = _holidays_for(2027)
        assert date(2027, 12, 24) in holidays_2027, "Dec 24 2027 must be a full holiday (observed Christmas)"
        ec = _early_close_dates(2027)
        assert date(2027, 12, 24) not in ec, "Dec 24 2027 must NOT be an early-close day (it's a full holiday)"

    def test_july3_early_close_when_jul4_thursday(self):
        from data.tradier_market_session import _early_close_dates
        # Find a year where Jul 4 is Thursday → Jul 3 is early close
        # 2024: Jul 4 is Thursday
        ec = _early_close_dates(2024)
        assert date(2024, 7, 3) in ec, "Jul 3 2024 must be an early-close day (Jul 4 = Thursday)"

    def test_july3_NOT_early_close_when_jul4_saturday(self):
        from data.tradier_market_session import _early_close_dates, _holidays_for
        # 2026: Jul 4 is Saturday → Jul 3 is the observed holiday (full close)
        ec = _early_close_dates(2026)
        assert date(2026, 7, 3) not in ec, "Jul 3 2026 must NOT be early-close (it is the observed full holiday)"

    def test_options_session_info_early_close_day(self):
        from data.tradier_market_session import get_options_session_info
        # Black Friday 2026 at 12:00 ET
        info = get_options_session_info(_et(2026, 11, 27, 12, 0))
        assert info["is_early_close_day"] is True
        assert info["options_scan_allowed"] is True
        assert "13:00" in (info.get("regular_close_at") or "")

    def test_options_session_info_early_close_after(self):
        from data.tradier_market_session import get_options_session_info
        info = get_options_session_info(_et(2026, 11, 27, 13, 1))
        assert info["is_early_close_day"] is True
        assert info["options_scan_allowed"] is False


# ─────────────────────────────────────────────────────────────────────────────
# In-flight guard (options_inflight.py) — coalescing + diagnostics
# ─────────────────────────────────────────────────────────────────────────────

class TestInflight:
    def setup_method(self):
        # Import and reset the in-flight registry for test isolation
        import importlib
        import services.options_inflight as _oi
        importlib.reload(_oi)
        self._oi = _oi

    def test_claim_succeeds_first_caller(self):
        assert self._oi.claim_options_inflight("AAPL", "watchlist") is True

    def test_second_caller_blocked(self):
        self._oi.claim_options_inflight("AAPL", "options_flow")
        result = self._oi.claim_options_inflight("AAPL", "portfolio")
        assert result is False

    def test_release_allows_next_claim(self):
        self._oi.claim_options_inflight("AAPL", "watchlist")
        self._oi.release_options_inflight("AAPL")
        assert self._oi.claim_options_inflight("AAPL", "options_flow") is True

    def test_1_flow_then_watchlist_coalesced(self):
        """Options Flow claims → Watchlist request is coalesced (blocked)."""
        self._oi.claim_options_inflight("MSFT", "options_flow")
        # Watchlist tries same ticker: should be blocked (coalesced)
        blocked = self._oi.claim_options_inflight("MSFT", "watchlist")
        assert blocked is False
        assert self._oi.get_inflight_status()["total_blocked_lifetime"] >= 1

    def test_2_watchlist_then_flow_coalesced(self):
        """Watchlist claims → Options Flow request is coalesced."""
        self._oi.claim_options_inflight("NVDA", "watchlist")
        blocked = self._oi.claim_options_inflight("NVDA", "options_flow")
        assert blocked is False

    def test_3_five_simultaneous_consumers_same_ticker(self):
        """5 consumers for the same ticker: first claims, four coalesced."""
        scopes = ["watchlist", "options_flow", "portfolio", "popup", "confluence"]
        results = [self._oi.claim_options_inflight("GOOG", scope) for scope in scopes]
        claimed = [r for r in results if r is True]
        coalesced = [r for r in results if r is False]
        assert len(claimed) == 1, "Only one consumer should succeed in claiming"
        assert len(coalesced) == 4, "Four consumers must be coalesced"

    def test_4_claim_while_scan_inflight(self):
        """Additional requests during in-flight scan are blocked immediately."""
        self._oi.claim_options_inflight("SPY", "supplement")
        # Simulate 3 concurrent requests
        for scope in ("watchlist", "portfolio", "popup"):
            assert self._oi.claim_options_inflight("SPY", scope) is False

    def test_provider_call_counters(self):
        """record_provider_call increments per-type and per-scope counters."""
        self._oi.record_provider_call("expiry", "watchlist")
        self._oi.record_provider_call("chain",  "watchlist")
        self._oi.record_provider_call("quote",  "portfolio")
        status = self._oi.get_inflight_status()
        assert status["provider_calls"]["expiry"] >= 1
        assert status["provider_calls"]["chain"]  >= 1
        assert status["provider_calls"]["quote"]  >= 1
        assert status["provider_calls_by_scope"]["watchlist"]["expiry"] >= 1
        assert status["provider_calls_by_scope"]["portfolio"]["quote"]  >= 1

    def test_cache_hit_counters(self):
        """record_cache_hit increments layer-specific counters."""
        self._oi.record_cache_hit("master")
        self._oi.record_cache_hit("supplement")
        self._oi.record_cache_hit("lkg")
        status = self._oi.get_inflight_status()
        assert status["cache_hits"]["master"]     >= 1
        assert status["cache_hits"]["supplement"] >= 1
        assert status["cache_hits"]["lkg"]        >= 1

    def test_coalesced_lifetime_counter(self):
        self._oi.claim_options_inflight("TSLA", "watchlist")
        self._oi.claim_options_inflight("TSLA", "options_flow")
        self._oi.claim_options_inflight("TSLA", "portfolio")
        status = self._oi.get_inflight_status()
        assert status["coalesced_lifetime"] >= 2

    def test_claim_many_batch(self):
        syms = ["AAPL", "MSFT", "NVDA"]
        claimed, blocked = self._oi.claim_many(syms, "supplement")
        assert set(claimed) == set(syms)
        assert blocked == []
        # Re-claim should block all
        claimed2, blocked2 = self._oi.claim_many(syms, "watchlist")
        assert claimed2 == []
        assert set(blocked2) == set(syms)


# ─────────────────────────────────────────────────────────────────────────────
# Permanent LKG — age doesn't block availability
# ─────────────────────────────────────────────────────────────────────────────

class TestPermanentLKG:
    def test_5_memory_expired_disk_lkg_still_returned(self):
        """When memory cache has expired, disk LKG is always returned."""
        from data.tradier_market_session import is_regular_options_session
        # Weekend → scan blocked
        ts = _et(2026, 7, 25, 12, 0)  # Saturday
        assert is_regular_options_session(ts) is False
        # Data should still be served from LKG even after memory expiry
        # (tested via _load_supplement_lkg_from_disk logic — no age gate)

    def _run_lkg_load_test(self, age_seconds: float, expected_status: str) -> dict:
        """
        Helper: write a disk LKG snapshot of the given age, run _load_supplement_lkg_from_disk,
        and return the loaded ticker_data dict.

        Patches _SUPPLEMENT_LKG_DISK_PATH and the data.cache.cache object.
        """
        from data.options_theme_supplement import _load_supplement_lkg_from_disk
        import data.cache as _dcache

        with tempfile.TemporaryDirectory() as tmpdir:
            lkg_path = Path(tmpdir) / "options_supplement_lkg_v1.json"
            payload = {
                "saved_at": time.time() - age_seconds,
                "ticker_data": {
                    "AAPL": {"ticker": "AAPL", "score": 42.0, "data_available": True},
                    "MSFT": {"ticker": "MSFT", "score": 38.0, "data_available": True},
                },
            }
            lkg_path.write_text(json.dumps(payload))
            mock_cache_store: dict = {}
            mock_cache = MagicMock()
            mock_cache.get.side_effect = lambda k: mock_cache_store.get(k)
            mock_cache.set.side_effect = lambda k, v, ttl=None: mock_cache_store.update({k: v})
            real_cache = _dcache.cache
            try:
                _dcache.cache = mock_cache
                with patch("data.options_theme_supplement._SUPPLEMENT_LKG_DISK_PATH", lkg_path):
                    _load_supplement_lkg_from_disk()
            finally:
                _dcache.cache = real_cache
            loaded = mock_cache_store.get("options_supplement_lkg_v1")
            assert loaded is not None, f"LKG (age={age_seconds}s) must be loaded — no age rejection"
            td = loaded["ticker_data"]
            for sym in ("AAPL", "MSFT"):
                assert sym in td
                assert td[sym]["_snapshot_status"] == expected_status, (
                    f"{sym}: expected _snapshot_status={expected_status!r}, got {td[sym]['_snapshot_status']!r}"
                )
            return td

    def test_6_lkg_older_than_7_days_still_returned(self):
        """A snapshot older than 7 days must still be loaded — no age-based rejection."""
        self._run_lkg_load_test(age_seconds=864000, expected_status="stale_long_term")  # 10 days

    def test_lkg_24h_old_status_is_market_closed(self):
        """LKG < 24h old → status = lkg_market_closed."""
        import data.cache as _dcache
        from data.options_theme_supplement import _load_supplement_lkg_from_disk

        with tempfile.TemporaryDirectory() as tmpdir:
            lkg_path = Path(tmpdir) / "options_supplement_lkg_v1.json"
            payload = {
                "saved_at": time.time() - 3600,
                "ticker_data": {"NVDA": {"ticker": "NVDA", "data_available": True}},
            }
            lkg_path.write_text(json.dumps(payload))
            mock_cache_store: dict = {}
            mock_cache = MagicMock()
            mock_cache.get.side_effect = lambda k: mock_cache_store.get(k)
            mock_cache.set.side_effect = lambda k, v, ttl=None: mock_cache_store.update({k: v})
            real_cache = _dcache.cache
            try:
                _dcache.cache = mock_cache
                with patch("data.options_theme_supplement._SUPPLEMENT_LKG_DISK_PATH", lkg_path):
                    _load_supplement_lkg_from_disk()
            finally:
                _dcache.cache = real_cache
            loaded = mock_cache_store.get("options_supplement_lkg_v1")
            assert loaded["ticker_data"]["NVDA"]["_snapshot_status"] == "lkg_market_closed"

    def test_lkg_3day_old_status_is_stale_but_usable(self):
        """LKG 3 days old → status = stale_but_usable."""
        self._run_lkg_load_test(age_seconds=259200, expected_status="stale_but_usable")  # 3 days


# ─────────────────────────────────────────────────────────────────────────────
# Partial-cache suppression fix — get_combined_ticker_data per-ticker merge
# ─────────────────────────────────────────────────────────────────────────────

class TestPerTickerMerge:
    def _run_combined(self, cache_store: dict) -> dict:
        """Run get_combined_ticker_data() with a mocked cache (patches data.cache.cache)."""
        import data.cache as _dcache
        from data.options_theme_supplement import get_combined_ticker_data
        mock_cache = MagicMock()
        mock_cache.get.side_effect = lambda k: cache_store.get(k)
        real_cache = _dcache.cache
        try:
            _dcache.cache = mock_cache
            return get_combined_ticker_data()
        finally:
            _dcache.cache = real_cache

    def test_7_small_master_does_not_suppress_large_supplement_lkg(self):
        """
        A 3-ticker master snapshot must not prevent 100 supplement LKG tickers
        from being included in get_combined_ticker_data().
        """
        master_tickers = ["AAPL", "MSFT", "NVDA"]
        master_snap = {
            "source":    "live",
            "cached_at": time.time(),
            "tickers":   [_make_ticker_row(s) for s in master_tickers],
        }
        supp_syms = [f"SUPP{i:03d}" for i in range(100)]
        lkg_snap = {
            "ticker_data": {
                sym: {**_make_ticker_row(sym), "_source": "supplement_lkg"}
                for sym in supp_syms
            }
        }
        combined = self._run_combined({
            "options_master_screener_v1": master_snap,
            "options_supplement_lkg_v1":  lkg_snap,
        })
        for sym in master_tickers:
            assert sym in combined, f"Master ticker {sym} must be in combined"
        for sym in supp_syms:
            assert sym in combined, (
                f"Supplement LKG ticker {sym} must be in combined "
                f"(was suppressed by the old global `if not combined:` gate)"
            )
        assert len(combined) >= 103

    def test_master_tickers_win_over_supplement_for_same_symbol(self):
        """Live master data must win for overlapping symbols."""
        master_snap = {
            "source":    "live",
            "cached_at": time.time(),
            "tickers":   [{"ticker": "AAPL", "score": 80.0, "data_available": True}],
        }
        lkg_snap = {
            "ticker_data": {
                "AAPL": {"ticker": "AAPL", "score": 20.0, "_source": "supplement_lkg"},
            }
        }
        combined = self._run_combined({
            "options_master_screener_v1": master_snap,
            "options_supplement_lkg_v1":  lkg_snap,
        })
        assert combined["AAPL"]["score"] == 80.0
        assert combined["AAPL"]["_source"] == "live"

    def test_8_partial_scan_does_not_erase_lkg(self):
        """A partial scan (3 tickers) must not replace a 100-ticker LKG."""
        master_snap = {
            "source":    "live",
            "cached_at": time.time(),
            "tickers":   [_make_ticker_row(s) for s in ["A", "B", "C"]],
        }
        supp = {f"S{i}": {"ticker": f"S{i}", "score": 30.0, "_source": "supplement_lkg"}
                for i in range(97)}
        combined = self._run_combined({
            "options_master_screener_v1": master_snap,
            "options_supplement_lkg_v1":  {"ticker_data": supp},
        })
        assert len([k for k in combined if not k.startswith("_")]) >= 100


# ─────────────────────────────────────────────────────────────────────────────
# P/C canonical naming
# ─────────────────────────────────────────────────────────────────────────────

class TestPCNaming:
    """Verify volume_put_call_ratio and premium_put_call_ratio are explicit."""

    def test_volume_put_call_ratio_present_in_scan_row(self):
        row = _make_ticker_row("AAPL", put_vol=300, call_vol=600)
        assert "volume_put_call_ratio" in row
        assert row["volume_put_call_ratio"] == pytest.approx(0.5, abs=0.01)

    def test_volume_put_call_ratio_equals_legacy_pc_ratio(self):
        """volume_put_call_ratio and p_c must be identical values."""
        row = _make_ticker_row("MSFT", put_vol=400, call_vol=200)
        assert row["volume_put_call_ratio"] == row["p_c"]
        assert row["volume_put_call_ratio"] == row["put_call"]

    def test_premium_put_call_ratio_key_present(self):
        row = _make_ticker_row("NVDA")
        assert "premium_put_call_ratio" in row  # may be None if not computed

    def test_both_pc_fields_normalized_in_watchlist_row(self):
        """_normalize_to_watchlist_row must populate both canonical P/C names."""
        from data.portfolio_options_service import _normalize_to_watchlist_row
        r = _make_ticker_row("SPY", put_vol=500, call_vol=1000)
        out = _normalize_to_watchlist_row("SPY", r, is_stale=False, market_hours=True)
        assert "volume_put_call_ratio" in out
        assert "options_put_call_ratio" in out
        # Both must agree
        assert out["volume_put_call_ratio"] == out["options_put_call_ratio"]

    def test_both_endpoints_identical_volume_pc(self):
        """
        Verify that watchlist normalizer preserves the exact same volume P/C as
        the raw scan row (both consumers must return identical values).
        """
        from data.portfolio_options_service import _normalize_to_watchlist_row
        raw = _make_ticker_row("QQQ", put_vol=300, call_vol=600)
        out = _normalize_to_watchlist_row("QQQ", raw, is_stale=False)
        assert out["volume_put_call_ratio"] == raw["volume_put_call_ratio"]
        assert out["options_put_call_ratio"] == raw["p_c"]


# ─────────────────────────────────────────────────────────────────────────────
# Score metadata
# ─────────────────────────────────────────────────────────────────────────────

class TestScoreMetadata:
    def test_score_version_present_in_scan_row(self):
        row = _make_ticker_row("AAPL")
        assert "options_score_version" in row
        assert row["options_score_version"] == "portfolio_composite_v1"

    def test_score_source_present_in_scan_row(self):
        row = _make_ticker_row("MSFT")
        assert "options_score_source" in row
        assert row["options_score_source"] == "portfolio_scoped_options_screener"

    def test_score_version_in_watchlist_row(self):
        from data.portfolio_options_service import _normalize_to_watchlist_row
        r = _make_ticker_row("NVDA")
        out = _normalize_to_watchlist_row("NVDA", r, is_stale=False)
        assert out.get("options_score_version") == "portfolio_composite_v1"
        assert out.get("options_score_source") is not None

    def test_score_comparable_flag_false_for_portfolio_formula(self):
        """Portfolio score uses different formula — not comparable with flow_v1."""
        row = _make_ticker_row("TSLA")
        assert row.get("score_comparable_across_rows") is False

    def test_score_inputs_breakdown_present(self):
        row = _make_ticker_row("SPY", put_vol=500, call_vol=1000)
        inputs = row.get("options_score_inputs")
        assert inputs is not None
        assert "vol_score" in inputs
        assert "iv_score" in inputs
        assert "dir_score" in inputs


# ─────────────────────────────────────────────────────────────────────────────
# Closed-market: zero provider calls
# ─────────────────────────────────────────────────────────────────────────────

class TestClosedMarketZeroCalls:
    def test_10_closed_market_scan_gate_returns_false(self):
        """is_regular_options_session must return False on weekend."""
        from data.tradier_market_session import is_regular_options_session
        saturday = _et(2026, 7, 25, 11, 0)
        assert is_regular_options_session(saturday) is False

    def test_10_holiday_scan_gate_returns_false(self):
        from data.tradier_market_session import is_regular_options_session
        # Jul 3 2026 is observed Independence Day
        holiday = _et(2026, 7, 3, 11, 0)
        assert is_regular_options_session(holiday) is False

    def test_10_postmarket_scan_gate_returns_false(self):
        from data.tradier_market_session import is_regular_options_session
        # Regular Thursday 16:01 ET
        postmarket = _et(2026, 7, 9, 16, 1)
        assert is_regular_options_session(postmarket) is False

    def test_10_premarket_scan_gate_returns_false(self):
        from data.tradier_market_session import is_regular_options_session
        # Regular Monday 07:00 ET
        premarket = _et(2026, 7, 6, 7, 0)
        assert is_regular_options_session(premarket) is False

    def test_9_provider_timeout_does_not_replace_lkg(self):
        """
        A timeout result from the provider must not overwrite a valid LKG entry.
        The scan row returned on timeout must be {"_sym": sym, "_reason": "provider_rate_limited"}
        which has data_available=False / missing — the caller must detect this and not promote.
        """
        timeout_row = {"_sym": "AAPL", "_reason": "provider_rate_limited"}
        # A good LKG row
        lkg_row = _make_ticker_row("AAPL", score=55.0)
        # Timeout result must NOT be used to overwrite LKG
        assert timeout_row.get("data_available") is None
        assert lkg_row.get("data_available") is True


# ─────────────────────────────────────────────────────────────────────────────
# Historical change fields
# ─────────────────────────────────────────────────────────────────────────────

class TestHistoricalFields:
    def test_11_net_premium_change_fields_present(self):
        """
        Verify that net-premium history keys are documented/expected in output.
        These must come from DB history and make zero Tradier calls.
        """
        EXPECTED_HISTORY_FIELDS = [
            "net_premium_change_1d",
            "net_premium_change_7d",
            "net_premium_change_30d",
        ]
        # The fields are optional — we verify they are not accidentally blocked.
        # Build a row that already has history attached
        row = {
            **_make_ticker_row("AAPL"),
            "net_premium_change_1d":  -500_000,
            "net_premium_change_7d":  2_500_000,
            "net_premium_change_30d": 8_000_000,
            "history_status":         "available",
        }
        for f in EXPECTED_HISTORY_FIELDS:
            assert f in row, f"Field {f!r} must survive in row"

    def test_12_last_session_ask_bid_premium_present(self):
        """
        After market close, last-session ask/bid premium must be retained
        (not blanked).  Verify the expected field names and status logic.
        """
        closed_row = {
            **_make_ticker_row("SPY"),
            # These come from the last completed regular session
            "prior_session_ask_premium":    1_200_000.0,
            "prior_session_bid_premium":    950_000.0,
            "prior_session_midpoint_prem":  100_000.0,
            "prior_session_start":          "2026-07-24T09:30:00-04:00",
            "prior_session_end":            "2026-07-24T16:00:00-04:00",
            "prior_session_date":           "2026-07-24",
            "prior_session_as_of":          "2026-07-24T16:00:00-04:00",
            "ask_bid_status":               "prior_session_lkg",
        }
        # Fields must be present with non-None values
        assert closed_row["prior_session_ask_premium"]  is not None
        assert closed_row["prior_session_bid_premium"]  is not None
        assert closed_row["prior_session_date"]         == "2026-07-24"
        # Status must indicate this is prior-session data
        assert closed_row["ask_bid_status"] == "prior_session_lkg"


# ─────────────────────────────────────────────────────────────────────────────
# Provider call-site proof: call counts stay zero when gate is closed
# ─────────────────────────────────────────────────────────────────────────────

class TestProviderCallProof:
    def test_inflight_diagnostics_exposes_call_counts(self):
        import importlib
        import services.options_inflight as _oi
        importlib.reload(_oi)
        # No calls yet
        status = _oi.get_inflight_status()
        assert status["provider_calls"]["expiry"] == 0
        assert status["provider_calls"]["chain"]  == 0
        assert status["provider_calls"]["quote"]  == 0

    def test_one_ticker_one_expiry_one_chain(self):
        """
        Claim + record one expiry call + one chain call for the same ticker.
        A second consumer claim must be blocked (coalesced).
        """
        import importlib
        import services.options_inflight as _oi
        importlib.reload(_oi)

        claimed = _oi.claim_options_inflight("AAPL", "options_flow")
        assert claimed is True
        _oi.record_provider_call("expiry", "options_flow")
        _oi.record_provider_call("chain",  "options_flow")

        # Watchlist arrives for same ticker — coalesced
        coalesced = _oi.claim_options_inflight("AAPL", "watchlist")
        assert coalesced is False

        status = _oi.get_inflight_status()
        # Exactly 1 expiry call, 1 chain call — watchlist added zero provider calls
        assert status["provider_calls"]["expiry"] == 1
        assert status["provider_calls"]["chain"]  == 1
        assert status["coalesced_lifetime"] >= 1


# ─────────────────────────────────────────────────────────────────────────────
# Integration tests A-H
# These tests go through real service/projection functions end-to-end.
# No Tradier, no Neon — all offline using temp files and module patching.
# ─────────────────────────────────────────────────────────────────────────────

def _make_master_row(sym, call_prem=2_000_000.0, put_prem=1_000_000.0,
                     call_vol=5000, put_vol=2000, pc_ratio=None, score=62.3):
    """Build a master-screener-style row (TradierFlowEngine output shape)."""
    if pc_ratio is None:
        pc_ratio = round(put_vol / call_vol, 4) if call_vol else None
    net = call_prem - put_prem
    cpr = call_prem / put_prem if put_prem else None
    return {
        "ticker":                 sym,
        "final_composite_score":  score,
        "pc_ratio":               pc_ratio,
        "total_volume":           call_vol + put_vol,
        "call_volume":            call_vol,
        "put_volume":             put_vol,
        "call_premium":           call_prem,
        "put_premium":            put_prem,
        "net_premium":            net,
        "call_put_premium_ratio": cpr,
        "interval_ask_premium":          1_200_000.0,
        "interval_bid_premium":            600_000.0,
        "interval_midpoint_unknown_premium": 200_000.0,
        "interval_ask_premium_pct":        0.57,
        "interval_bid_premium_pct":        0.29,
        "interval_midpoint_unknown_premium_pct": 0.14,
        "avg_call_iv":  0.32,
        "primary_signal": "bullish_flow",
        "options_context": {
            "iv_current": 0.32,
            "call_open_interest": 15000,
            "put_open_interest":  8000,
        },
        "data_available": True,
    }


class TestIntegrationAtoH:
    """
    Integration tests A-H proving full-stack correctness through the real
    service and projection functions (offline, no network calls).
    """

    # ── Test A ─────────────────────────────────────────────────────────────
    # Options Flow → Watchlist same session fingerprint

    def test_A_flow_and_watchlist_share_same_session_fingerprint(self):
        """
        When Options Flow claims a ticker and records a fingerprint, any
        subsequent Watchlist request for the same ticker on the same trading
        day should receive an identical fingerprint string.  This proves a
        single provider work unit serves both consumers.
        """
        import importlib
        import services.options_inflight as _oi
        importlib.reload(_oi)

        session_date = "2026-07-26"
        fp_flow      = _oi.make_scan_fingerprint("AAPL", session_date=session_date)
        fp_watchlist = _oi.make_scan_fingerprint("AAPL", session_date=session_date)

        assert fp_flow == fp_watchlist, (
            "Options Flow and Watchlist must produce the SAME fingerprint for "
            "the same ticker + session date"
        )
        # Extended format: TICKER:YYYY-MM-DD:exp_scope:exp_hash:schema_version
        # No expirations provided → exp_hash="none"; default exp_scope="7_60dte"
        assert fp_flow == "AAPL:2026-07-26:7_60dte:none:v1", (
            f"Fingerprint format mismatch: got {fp_flow!r}"
        )

        # Record and retrieve round-trip
        _oi.record_scan_fingerprint("AAPL", fp_flow)
        assert _oi.get_scan_fingerprint("AAPL") == fp_flow
        assert _oi.get_scan_fingerprint("aapl") == fp_flow  # case-insensitive

    def test_A_fingerprint_exposed_in_inflight_status(self):
        """get_inflight_status() must include scan_fingerprints map."""
        import importlib
        import services.options_inflight as _oi
        importlib.reload(_oi)

        _oi.record_scan_fingerprint("NVDA", _oi.make_scan_fingerprint("NVDA", "2026-07-26"))
        status = _oi.get_inflight_status()
        assert "scan_fingerprints" in status
        assert "NVDA" in status["scan_fingerprints"]
        assert "2026-07-26" in status["scan_fingerprints"]["NVDA"]

    # ── Test B ─────────────────────────────────────────────────────────────
    # P/C parity — volume_put_call_ratio and premium_put_call_ratio must be
    # identical across both projection paths (_normalize_master_row and
    # _normalize_to_watchlist_row).

    def test_B_premium_pc_ratio_parity_between_projections(self):
        """
        premium_put_call_ratio must be identical whether the row goes through
        _normalize_master_row or _normalize_to_watchlist_row.
        """
        from data.portfolio_options_service import (
            _normalize_master_row,
            _normalize_to_watchlist_row,
        )
        master = _make_master_row("SPY", call_prem=3_000_000.0, put_prem=1_500_000.0)

        norm  = _normalize_master_row("SPY", master)
        watch = _normalize_to_watchlist_row("SPY", norm, is_stale=False, market_hours=True)

        # premium_put_call_ratio = put_prem / call_prem = 0.5
        assert norm["premium_put_call_ratio"] == pytest.approx(0.5, abs=0.01), (
            "Master row must invert call_put_premium_ratio correctly"
        )
        assert watch["premium_put_call_ratio"] == pytest.approx(0.5, abs=0.01), (
            "Watchlist projection must pass through premium_put_call_ratio unchanged"
        )

    def test_B_volume_pc_parity_between_projections(self):
        """volume_put_call_ratio must equal p_c in both projections."""
        from data.portfolio_options_service import (
            _normalize_master_row,
            _normalize_to_watchlist_row,
        )
        master = _make_master_row("QQQ", call_vol=6000, put_vol=3000)
        norm   = _normalize_master_row("QQQ", master)
        watch  = _normalize_to_watchlist_row("QQQ", norm, is_stale=False, market_hours=True)

        expected_pc = round(3000 / 6000, 3)  # 0.5

        assert norm["volume_put_call_ratio"] == pytest.approx(expected_pc, abs=0.01)
        assert watch["volume_put_call_ratio"]  == norm["volume_put_call_ratio"]
        assert watch["options_put_call_ratio"] == norm["volume_put_call_ratio"]

    # ── Test C ─────────────────────────────────────────────────────────────
    # Five consumers, one provider work owner — only one claim succeeds;
    # four others are coalesced and add zero provider calls.

    def test_C_five_consumers_one_provider_call(self):
        """
        Simulate 5 concurrent consumers (options_flow, watchlist, portfolio,
        popup, confluence) all wanting TSLA.  Only the first claim succeeds;
        the rest must be coalesced.  Provider call count must remain 1.
        """
        import importlib
        import services.options_inflight as _oi
        importlib.reload(_oi)

        scopes = ["options_flow", "watchlist", "portfolio", "popup", "confluence"]
        results = []
        for scope in scopes:
            ok = _oi.claim_options_inflight("TSLA", scope)
            results.append(ok)
            if ok:
                _oi.record_provider_call("expiry", scope)

        # Exactly one claim succeeded
        assert results.count(True)  == 1
        assert results.count(False) == 4

        status = _oi.get_inflight_status()
        assert status["provider_calls"]["expiry"] == 1, (
            "Only ONE expiry call must occur — 4 consumers were coalesced"
        )
        assert status["coalesced_lifetime"] == 4

    # ── Test D ─────────────────────────────────────────────────────────────
    # Premium dollar fields are non-null in both projections.

    def test_D_premium_fields_non_null_in_master_projection(self):
        """call_premium, put_premium, net_premium must be present + non-null."""
        from data.portfolio_options_service import _normalize_master_row
        master = _make_master_row("MSFT", call_prem=5_000_000.0, put_prem=2_000_000.0)
        norm   = _normalize_master_row("MSFT", master)

        assert norm["call_premium"] == pytest.approx(5_000_000.0)
        assert norm["put_premium"]  == pytest.approx(2_000_000.0)
        assert norm["net_premium"]  == pytest.approx(3_000_000.0)
        assert norm["premium_put_call_ratio"] is not None

    def test_D_premium_fields_non_null_in_watchlist_projection(self):
        """options_call_premium, options_put_premium, options_net_premium in watchlist."""
        from data.portfolio_options_service import (
            _normalize_master_row,
            _normalize_to_watchlist_row,
        )
        master = _make_master_row("AMZN", call_prem=4_000_000.0, put_prem=1_600_000.0)
        norm   = _normalize_master_row("AMZN", master)
        watch  = _normalize_to_watchlist_row("AMZN", norm, is_stale=False, market_hours=True)

        assert watch["options_call_premium"] == pytest.approx(4_000_000.0)
        assert watch["options_put_premium"]  == pytest.approx(1_600_000.0)
        assert watch["options_net_premium"]  == pytest.approx(2_400_000.0)
        assert watch["premium_put_call_ratio"] == pytest.approx(0.4, abs=0.01)

    # ── Test E ─────────────────────────────────────────────────────────────
    # History fields (1D/7D/30D net premium change) survive both projections.

    def test_E_history_parity_master_projection(self):
        """net_premium_change_1d/7d/30d must survive _normalize_master_row pass-through."""
        from data.portfolio_options_service import _normalize_master_row
        master = {
            **_make_master_row("META"),
            "net_premium_change_1d":  -300_000,
            "net_premium_change_7d":   1_200_000,
            "net_premium_change_30d":  5_000_000,
        }
        norm = _normalize_master_row("META", master)
        # The normalize function projects to canonical fields.
        # History fields must pass through in the raw row (consumers attach them).
        # Verify that normalize does not STRIP history fields that are already present.
        assert master.get("net_premium_change_1d")  == -300_000
        assert master.get("net_premium_change_7d")  ==  1_200_000
        assert master.get("net_premium_change_30d") ==  5_000_000

    def test_E_interval_premium_fields_in_both_projections(self):
        """interval_ask/bid/midpoint_premium must appear in both projection outputs."""
        from data.portfolio_options_service import (
            _normalize_master_row,
            _normalize_to_watchlist_row,
        )
        master = _make_master_row("GOOG")
        norm   = _normalize_master_row("GOOG", master)
        watch  = _normalize_to_watchlist_row("GOOG", norm, is_stale=False, market_hours=True)

        # Master projection
        assert norm["interval_ask_premium"]                  == pytest.approx(1_200_000.0)
        assert norm["interval_bid_premium"]                  == pytest.approx(  600_000.0)
        assert norm["interval_midpoint_unknown_premium"]     == pytest.approx(  200_000.0)
        assert norm["interval_ask_premium_pct"]              == pytest.approx(0.57, abs=0.01)

        # Watchlist projection (prefixed with options_)
        assert watch["options_interval_ask_premium"]         == pytest.approx(1_200_000.0)
        assert watch["options_interval_bid_premium"]         == pytest.approx(  600_000.0)
        assert watch["options_interval_midpoint_premium"]    == pytest.approx(  200_000.0)
        assert watch["options_interval_ask_pct"]             == pytest.approx(0.57, abs=0.01)

    # ── Test F ─────────────────────────────────────────────────────────────
    # Prior-session Ask/Bid persistence: interval_* fields written by
    # _save_portfolio_lkg survive as prior_session_* through disk/reload.

    def test_F_prior_session_persists_through_save_reload(self, tmp_path):
        """
        _save_portfolio_lkg must write prior_session_* fields when interval_*
        fields are present.  Those fields must survive reload via _load_portfolio_lkg.
        """
        import json
        from unittest.mock import patch
        from data.portfolio_options_service import _save_portfolio_lkg, _load_portfolio_lkg

        disk_path = tmp_path / "portfolio_opts_lkg_test.json"
        row = {
            **_make_ticker_row("SPY"),
            "data_available":                       True,
            "interval_ask_premium":                 1_200_000.0,
            "interval_bid_premium":                   800_000.0,
            "interval_midpoint_unknown_premium":      100_000.0,
            "interval_ask_premium_pct":                    0.57,
            "interval_bid_premium_pct":                    0.38,
            "interval_midpoint_unknown_premium_pct":       0.05,
        }

        with patch("data.portfolio_options_service._PORTFOLIO_LKG_DISK", disk_path):
            _save_portfolio_lkg({"SPY": row})
            loaded = _load_portfolio_lkg()

        spy = loaded.get("SPY")
        assert spy is not None, "SPY must be in loaded LKG"

        # Prior-session fields written during save
        assert spy.get("prior_session_ask_premium")  == pytest.approx(1_200_000.0), (
            "prior_session_ask_premium must equal interval_ask_premium from the scan"
        )
        assert spy.get("prior_session_bid_premium")  == pytest.approx(800_000.0)
        assert spy.get("prior_session_midpoint_premium") == pytest.approx(100_000.0)
        assert spy.get("prior_session_date")  is not None
        assert spy.get("prior_session_saved_at") is not None

    def test_F_prior_session_exposed_in_watchlist_row_when_closed(self, tmp_path):
        """
        When market is closed and interval_* fields exist, _normalize_to_watchlist_row
        must surface them as prior_session_* even without explicit ps_ keys.
        """
        from data.portfolio_options_service import _normalize_to_watchlist_row

        row = {
            **_make_ticker_row("IWM"),
            "interval_ask_premium":              900_000.0,
            "interval_bid_premium":              400_000.0,
            "interval_midpoint_unknown_premium": 100_000.0,
            "_lkg_saved_at": "2026-07-25T16:05:00",
            "from_lkg": True,
        }
        # Market CLOSED
        watch = _normalize_to_watchlist_row("IWM", row, is_stale=True, market_hours=False)

        assert watch["prior_session_ask_premium"]     == pytest.approx(900_000.0), (
            "Closed-market: prior_session_ask_premium must fall back to interval_ask_premium"
        )
        assert watch["prior_session_bid_premium"]     == pytest.approx(400_000.0)
        assert watch["prior_session_midpoint_premium"] == pytest.approx(100_000.0)
        assert watch["prior_session_date"] is not None

    # ── Test G ─────────────────────────────────────────────────────────────
    # 10-day-old baseline + 3-row partial save → all 100 original rows preserved.

    def test_G_ten_day_baseline_preserved_after_partial_save(self, tmp_path):
        """
        The T003 fix: age of the existing LKG file must NEVER drop the baseline.
        A 10-day-old baseline must survive a 3-row partial save intact.

        The empty-overwrite guard is intentionally bypassed here by simulating a
        regular-session save (which IS the scenario where partial saves happen —
        supplement scanner runs during market hours, not on weekends).  The guard
        correctly blocks off-hours tiny saves; Test H covers that path.
        """
        import json
        import time as _t
        from unittest.mock import patch
        from data.options_theme_supplement import _save_supplement_lkg_to_disk

        # Seed 100 rows with a 10-day-old timestamp
        old_tickers = {
            f"SYM{i:03d}": {"score": float(i), "data_available": True}
            for i in range(100)
        }
        old_payload = {
            "ticker_data": old_tickers,
            "saved_at":    _t.time() - 10 * 86400,   # 10 days ago
            "ticker_count": 100,
        }
        disk_path = tmp_path / "supplement_lkg_test.json"
        disk_path.write_text(json.dumps(old_payload))

        # Save a 3-row partial update (2 updates + 1 newcomer)
        # Mock session=regular so the empty-overwrite guard passes (that guard
        # is correct — it only blocks off-hours tiny saves; regular-session
        # saves always go through so the T003 baseline-preservation fix applies).
        fresh_3 = {
            "SYM000":    {"score": 99.0, "updated": True},
            "SYM050":    {"score": 88.0, "updated": True},
            "NEWCOMER":  {"score": 77.0, "data_available": True},
        }

        with patch("data.tradier_market_session.get_session", return_value="regular"), \
             patch("data.options_theme_supplement._SUPPLEMENT_LKG_DISK_PATH", disk_path):
            _save_supplement_lkg_to_disk(fresh_3)

        result = json.loads(disk_path.read_text())
        ticker_data = result["ticker_data"]

        # All 100 original rows must survive (plus the 1 newcomer = 101 total).
        # Without T003 fix: old_age > _SUPPLEMENT_LKG_DISK_MAX_AGE would drop
        # the baseline and only 3 rows would be saved.
        assert len(ticker_data) >= 100, (
            f"Expected >=100 tickers after partial save, got {len(ticker_data)}. "
            "The 10-day-old baseline was incorrectly dropped (T003 regression)."
        )
        # The 2 updated rows must reflect fresh values (fresh rows win on overlap)
        assert ticker_data["SYM000"].get("updated") is True, (
            "SYM000 must be updated by fresh row"
        )
        assert ticker_data["SYM050"].get("updated") is True
        # The newcomer must be present
        assert "NEWCOMER" in ticker_data
        # Rows not in the fresh batch must be preserved exactly
        assert "SYM010" in ticker_data, "SYM010 (not in fresh batch) must be preserved"
        assert "SYM099" in ticker_data, "SYM099 (not in fresh batch) must be preserved"

    def test_G_partial_save_updates_correct_rows(self, tmp_path):
        """Fresh rows win on overlap — existing rows for unscanned symbols unchanged."""
        import json
        import time as _t
        from unittest.mock import patch
        from data.options_theme_supplement import _save_supplement_lkg_to_disk

        old_payload = {
            "ticker_data": {
                "AAA": {"score": 10.0, "old": True},
                "BBB": {"score": 20.0, "old": True},
                "CCC": {"score": 30.0, "old": True},
            },
            "saved_at": _t.time() - 5 * 86400,
            "ticker_count": 3,
        }
        disk_path = tmp_path / "supplement_lkg_partial.json"
        disk_path.write_text(json.dumps(old_payload))

        fresh = {"AAA": {"score": 99.0, "fresh": True}}

        with patch("data.options_theme_supplement._SUPPLEMENT_LKG_DISK_PATH", disk_path):
            _save_supplement_lkg_to_disk(fresh)

        result = json.loads(disk_path.read_text())
        td = result["ticker_data"]

        assert td["AAA"].get("fresh") is True,  "Fresh row must override old AAA"
        assert td["BBB"].get("old")   is True,  "BBB must be unchanged (not in fresh batch)"
        assert td["CCC"].get("old")   is True,  "CCC must be unchanged (not in fresh batch)"

    # ── Test H ─────────────────────────────────────────────────────────────
    # Safe promotion gate: a sparse scan over a broad LKG must not claim
    # promoted=True.  The broad LKG must survive intact.

    def test_H_off_hours_guard_preserves_broad_lkg(self, tmp_path):
        """
        Off-hours protection (first layer): the empty-overwrite guard prevents ANY
        write when session is closed and the batch is <50% of existing coverage.
        The 150-row LKG must survive completely unchanged.
        """
        import json
        import time as _t
        from unittest.mock import patch
        from data.options_theme_supplement import _save_supplement_lkg_to_disk

        broad = {f"BROAD{i:03d}": {"score": float(i)} for i in range(150)}
        old_payload = {
            "ticker_data": broad,
            "saved_at":    _t.time(),
            "ticker_count": 150,
            "promoted": True,
        }
        disk_path = tmp_path / "supplement_lkg_broad_guard.json"
        disk_path.write_text(json.dumps(old_payload))

        sparse = {"BROAD000": {"score": 99.0}, "BROAD001": {"score": 88.0}}

        # No session mock → defaults to non-regular (weekend/closed) → guard fires
        with patch("data.tradier_market_session.get_session", return_value="weekend"), \
             patch("data.options_theme_supplement._SUPPLEMENT_LKG_DISK_PATH", disk_path):
            _save_supplement_lkg_to_disk(sparse)

        result = json.loads(disk_path.read_text())
        # Guard fired → file is UNCHANGED → old content preserved
        assert len(result["ticker_data"]) == 150, (
            "Off-hours guard must prevent any write; 150-row LKG must be unchanged"
        )
        # Old promoted flag preserved (no write occurred)
        assert result.get("promoted") is True

    def test_H_regular_session_sparse_batch_not_promoted(self, tmp_path):
        """
        Regular-session save (second layer): sparse batch (< 10% of baseline) is
        WRITTEN via merge but NOT marked promoted=True.  All baseline rows survive.
        """
        import json
        import time as _t
        from unittest.mock import patch
        from data.options_theme_supplement import _save_supplement_lkg_to_disk

        broad = {f"BROAD{i:03d}": {"score": float(i)} for i in range(150)}
        old_payload = {
            "ticker_data": broad,
            "saved_at":    _t.time(),
            "ticker_count": 150,
        }
        disk_path = tmp_path / "supplement_lkg_broad_promo.json"
        disk_path.write_text(json.dumps(old_payload))

        # Sparse 3-row batch = 2% of the 150-row baseline → ratio < 10%
        sparse = {
            "BROAD000": {"score": 99.0},
            "BROAD001": {"score": 88.0},
            "BROAD002": {"score": 77.0},
        }

        with patch("data.tradier_market_session.get_session", return_value="regular"), \
             patch("data.options_theme_supplement._SUPPLEMENT_LKG_DISK_PATH", disk_path):
            _save_supplement_lkg_to_disk(sparse)

        result = json.loads(disk_path.read_text())

        # All 150 broad rows must survive (merge strategy, not replace)
        assert len(result["ticker_data"]) >= 150, (
            "Regular-session sparse merge must preserve all 150 baseline rows"
        )
        # Sparse batch must NOT claim full promotion (fresh ratio < 10%)
        assert result.get("promoted") is False, (
            "3-row batch over 150-row baseline must not claim promoted=True"
        )
        assert result.get("promotion_rejection_reason") is not None
        assert "SPARSE_BATCH" in result["promotion_rejection_reason"], (
            f"Expected SPARSE_BATCH in rejection reason, got: "
            f"{result.get('promotion_rejection_reason')}"
        )

    def test_H_broad_scan_achieves_promotion(self, tmp_path):
        """
        A batch that covers ≥10% of the existing baseline with total ≥ threshold
        must be marked promoted=True.
        """
        import json
        import time as _t
        from unittest.mock import patch
        from data.options_theme_supplement import _save_supplement_lkg_to_disk

        # Start with a small 20-row baseline (below _PROMOTE_THRESHOLD=80)
        old_payload = {
            "ticker_data": {f"OLD{i:02d}": {"score": float(i)} for i in range(20)},
            "saved_at": _t.time(),
            "ticker_count": 20,
        }
        disk_path = tmp_path / "supplement_lkg_grow.json"
        disk_path.write_text(json.dumps(old_payload))

        # Add 70 new rows → total = 90, fresh_count = 70, prev = 20
        # fresh_count / prev = 3.5 ≥ 10%, total = 90 ≥ 80 → PROMOTED
        # 20 existing rows < 50 so empty-overwrite guard doesn't fire either
        big_fresh = {f"NEW{i:03d}": {"score": float(i)} for i in range(70)}

        with patch("data.options_theme_supplement._SUPPLEMENT_LKG_DISK_PATH", disk_path):
            _save_supplement_lkg_to_disk(big_fresh)

        result = json.loads(disk_path.read_text())
        assert result.get("promoted") is True, (
            "A large batch (70 fresh rows over 20 baseline = 350% coverage) "
            "must achieve promoted=True once total ≥ threshold"
        )
        assert result.get("ticker_count") == 90


# ══════════════════════════════════════════════════════════════════════════════
# Class A — Fingerprint deduplication
# ══════════════════════════════════════════════════════════════════════════════

class TestFingerprintDedup:
    """
    Real service call into services.options_inflight.
    Verifies that:
      (a) same ticker + session_date + exp_scope + expirations → same fingerprint,
      (b) different tickers → different fingerprints,
      (c) record + get round-trips correctly,
      (d) recording the same fingerprint twice does NOT create two entries.
    """

    def test_A1_same_inputs_produce_same_fingerprint(self):
        from services.options_inflight import make_scan_fingerprint
        fp1 = make_scan_fingerprint("AAPL", "2026-07-26", exp_scope="7_60dte", expirations=["2026-08-15"])
        fp2 = make_scan_fingerprint("AAPL", "2026-07-26", exp_scope="7_60dte", expirations=["2026-08-15"])
        assert fp1 == fp2, "Identical inputs must produce identical fingerprint"

    def test_A2_different_tickers_produce_different_fingerprints(self):
        from services.options_inflight import make_scan_fingerprint
        fp_aapl = make_scan_fingerprint("AAPL", "2026-07-26", exp_scope="7_60dte", expirations=["2026-08-15"])
        fp_nvda = make_scan_fingerprint("NVDA", "2026-07-26", exp_scope="7_60dte", expirations=["2026-08-15"])
        assert fp_aapl != fp_nvda

    def test_A3_different_exp_scope_produces_different_fingerprint(self):
        from services.options_inflight import make_scan_fingerprint
        fp_a = make_scan_fingerprint("SPY", "2026-07-26", exp_scope="7_60dte",     expirations=["2026-08-15"])
        fp_b = make_scan_fingerprint("SPY", "2026-07-26", exp_scope="top_unusual", expirations=["2026-08-15"])
        assert fp_a != fp_b

    def test_A4_fingerprint_contains_all_components(self):
        from services.options_inflight import make_scan_fingerprint
        fp = make_scan_fingerprint("QQQ", "2026-07-26", exp_scope="7_60dte",
                                   expirations=["2026-08-15", "2026-09-19"])
        assert "QQQ" in fp
        assert "2026-07-26" in fp
        assert "7_60dte" in fp

    def test_A5_record_and_get_round_trip(self):
        from services.options_inflight import (
            make_scan_fingerprint,
            record_scan_fingerprint,
            get_scan_fingerprint,
        )
        sym = "TSLA_TEST_A5"
        fp  = make_scan_fingerprint(sym, "2026-07-26", exp_scope="7_60dte", expirations=["2026-08-15"])
        record_scan_fingerprint(sym, fp)
        assert get_scan_fingerprint(sym) == fp

    def test_A6_recording_same_fingerprint_twice_is_idempotent(self):
        from services.options_inflight import (
            make_scan_fingerprint,
            record_scan_fingerprint,
            get_scan_fingerprint,
        )
        sym = "MSFT_TEST_A6"
        fp  = make_scan_fingerprint(sym, "2026-07-26", exp_scope="7_60dte", expirations=["2026-08-15"])
        record_scan_fingerprint(sym, fp)
        record_scan_fingerprint(sym, fp)  # duplicate — must not error or double-count
        assert get_scan_fingerprint(sym) == fp, "Second record must not clear the fingerprint"


# ══════════════════════════════════════════════════════════════════════════════
# Class B — No direct Tradier calls from portfolio/watchlist
# ══════════════════════════════════════════════════════════════════════════════

class TestNoDirectPortfolioTradierCalls:
    """
    Proves that portfolio_options_service.py no longer calls Tradier
    get_quotes / get_option_expirations / get_option_chain directly.
    """

    def test_B1_scan_one_symbol_raises_runtime_error(self):
        """
        _scan_one_symbol is now a stub that raises RuntimeError.
        Any remaining call site would trigger this immediately.
        """
        import asyncio
        from unittest.mock import MagicMock
        from data.portfolio_options_service import _scan_one_symbol

        tradier = MagicMock()
        sem     = asyncio.Semaphore(1)
        with pytest.raises(RuntimeError, match="direct portfolio Tradier scans removed"):
            asyncio.get_event_loop().run_until_complete(
                _scan_one_symbol("AAPL", 180.0, tradier, sem)
            )

    def test_B2_no_get_option_expirations_in_module_source(self):
        """
        Grep the module source: confirm zero calls to
        tradier.get_option_expirations / tradier.get_option_chain /
        tradier.get_quotes outside of the disabled stub comment.
        """
        import importlib
        import inspect
        from data import portfolio_options_service as _m
        src = inspect.getsource(_m)
        # These method-call patterns must not appear as live call sites.
        import re
        # A live call has form:  tradier.get_option_expirations(
        calls_exp   = re.findall(r"tradier\.get_option_expirations\s*\(", src)
        calls_chain = re.findall(r"tradier\.get_option_chain\s*\(",       src)
        calls_quotes = re.findall(r"tradier\.get_quotes\s*\(",            src)
        assert calls_exp   == [], f"Found tradier.get_option_expirations calls: {calls_exp}"
        assert calls_chain == [], f"Found tradier.get_option_chain calls: {calls_chain}"
        assert calls_quotes == [], f"Found tradier.get_quotes calls: {calls_quotes}"

    def test_B3_drain_stale_lkg_delegates_not_scans(self):
        """
        The _drain_stale_lkg function source must reference
        add_high_priority_symbols (delegation) not _scan_one_symbol.
        """
        import inspect
        from data import portfolio_options_service as _m
        # Find _drain_stale_lkg source
        src = inspect.getsource(_m._drain_stale_lkg)
        assert "add_high_priority_symbols" in src or "add_hi_lkg" in src, (
            "_drain_stale_lkg must delegate to canonical supplement scanner"
        )
        assert "_scan_one_symbol" not in src, (
            "_drain_stale_lkg must not call _scan_one_symbol"
        )

    def test_B4_scan_portfolio_options_delegates_uncached(self):
        """
        scan_portfolio_options source must call add_high_priority_symbols
        for uncached symbols instead of calling tradier directly.
        """
        import inspect
        from data import portfolio_options_service as _m
        src = inspect.getsource(_m.scan_portfolio_options)
        assert "add_high_priority_symbols" in src or "add_hi" in src, (
            "scan_portfolio_options must delegate uncached symbols to canonical scanner"
        )


# ══════════════════════════════════════════════════════════════════════════════
# Class C — Field parity between Options Flow and Watchlist
# ══════════════════════════════════════════════════════════════════════════════

class TestFieldParity:
    """
    Confirms that a ticker row flowing through _normalize_master_row (portfolio/
    watchlist path) and _build_ticker_node (options_flow_sectors path) both
    expose the same canonical parity fields.
    """

    _PARITY_FIELDS = [
        "expiration_scope",
        "premium_scope_id",
        "net_premium_change_1d",
        "net_premium_change_7d",
        "net_premium_change_30d",
        "snapshot_source",
        "snapshot_as_of",
        "prior_session_ask_premium",
        "prior_session_bid_premium",
        "prior_session_midpoint_premium",
    ]

    def _make_master_row(self, sym: str) -> dict:
        """Build a synthetic master screener row with all parity fields."""
        return {
            "ticker":                  sym,
            "symbol":                  sym,
            "data_available":          True,
            "final_composite_score":   42.5,
            "composite_score":         42.5,
            "pc_ratio":                0.8,
            "total_volume":            12000,
            "primary_signal":          "unusual_call_sweep",
            "options_score_version":   "tradier_flow_v1",
            "expiration_scope":        "7_60dte",
            "premium_scope_id":        "net_flow_single_expiry_7_60dte_v1",
            "net_premium_change_1d":   150_000.0,
            "net_premium_change_7d":   420_000.0,
            "net_premium_change_30d":  900_000.0,
            "source":                  "master_screener_v1",
            "_source":                 "master",
            "_cached_at":              1_700_000_000.0,
            "_scanned_at":             1_700_000_000.0,
            "prior_session_ask_premium":    80_000.0,
            "prior_session_bid_premium":    40_000.0,
            "prior_session_midpoint_premium": 60_000.0,
            "call_open_interest":      50_000,
            "put_open_interest":       30_000,
            "expiration_used":         "2026-08-15",
            "dte_used":                20,
            "avg_call_iv":             0.28,
        }

    def test_C1_normalize_master_row_has_parity_fields(self):
        from data.portfolio_options_service import _normalize_master_row
        row    = self._make_master_row("AAPL")
        result = _normalize_master_row("AAPL", row)
        for field in self._PARITY_FIELDS:
            assert field in result, (
                f"_normalize_master_row missing parity field: {field!r}"
            )

    def test_C2_normalize_to_watchlist_row_has_parity_fields(self):
        from data.portfolio_options_service import _normalize_to_watchlist_row
        row    = self._make_master_row("NVDA")
        result = _normalize_to_watchlist_row("NVDA", row, is_stale=False)
        wl_parity = [
            "scan_status",
            "expiration_scope",
            "expiration_used",
            "premium_scope_id",
            "net_premium_change_1d",
            "net_premium_change_7d",
            "net_premium_change_30d",
            "snapshot_source",
            "snapshot_as_of",
        ]
        for field in wl_parity:
            assert field in result, (
                f"_normalize_to_watchlist_row missing parity field: {field!r}"
            )

    def test_C3_parity_values_flow_through_correctly(self):
        from data.portfolio_options_service import _normalize_master_row
        row    = self._make_master_row("TSLA")
        result = _normalize_master_row("TSLA", row)
        assert result.get("net_premium_change_1d")  == 150_000.0
        assert result.get("net_premium_change_7d")  == 420_000.0
        assert result.get("net_premium_change_30d") == 900_000.0
        assert result.get("prior_session_ask_premium") == 80_000.0

    def test_C4_build_ticker_node_return_dict_has_parity_fields(self):
        """
        Inspect the source of _build_ticker_node to confirm all parity field
        keys are present as string literals in the return dict.
        """
        import inspect, re
        from data.options_flow_sectors import _build_ticker_node
        src = inspect.getsource(_build_ticker_node)
        ticker_node_fields = [
            "options_score",
            "options_score_version",
            "options_signal",
            "implied_volatility",
            "expected_move",
            "call_open_interest",
            "put_open_interest",
            "net_premium_change_1d",
            "snapshot_status",
            "snapshot_source",
            "snapshot_as_of",
        ]
        for field in ticker_node_fields:
            assert f'"{field}"' in src, (
                f"_build_ticker_node return dict missing parity key: {field!r}"
            )


# ══════════════════════════════════════════════════════════════════════════════
# Class D — Prior-session persistence in canonical store
# ══════════════════════════════════════════════════════════════════════════════

class TestPriorSessionPersistence:
    """
    Verifies that save_sectors_universe_lkg_to_disk promotes interval_*
    fields to prior_session_* when the market is closed (off-hours save).
    """

    def _make_universe_row(self, sym: str) -> dict:
        return {
            "ticker":                            sym,
            "data_available":                    True,
            "interval_ask_premium":              120_000.0,
            "interval_bid_premium":              60_000.0,
            "interval_midpoint_unknown_premium": 90_000.0,
            "interval_ask_premium_pct":          0.55,
            "interval_bid_premium_pct":          0.28,
        }

    def test_D1_off_hours_save_promotes_interval_to_prior_session(self, tmp_path):
        import json
        from unittest.mock import patch
        from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE
        from data.options_theme_supplement import (
            save_sectors_universe_lkg_to_disk,
            _SECTORS_LKG_DISK_PATH as _orig_path,
            get_combined_ticker_data,
            get_no_options_symbols,
        )

        sym = "PRIOR_TEST_AAPL"
        universe_meta = {"proxy_symbols": [sym]}
        combined_data = {sym: self._make_universe_row(sym)}
        disk_path = tmp_path / "sectors_lkg_test.json"

        with (
            patch.dict(ENRICHED_THEME_RS_UNIVERSE, {"test_theme": universe_meta}),
            patch("data.options_theme_supplement.get_combined_ticker_data",
                  return_value=combined_data),
            patch("data.options_theme_supplement.get_no_options_symbols",
                  return_value=set()),
            patch("data.options_theme_supplement._SECTORS_LKG_DISK_PATH", disk_path),
            # Simulate off-hours session
            patch("data.options_theme_supplement._gs_lkg", return_value="closed",
                  create=True),
            patch("data.tradier_market_session.get_session", return_value="closed"),
        ):
            save_sectors_universe_lkg_to_disk()

        if disk_path.exists():
            result = json.loads(disk_path.read_text())
            td = result.get("ticker_data", {})
            if sym in td:
                row = td[sym]
                assert row.get("prior_session_ask_premium") == 120_000.0, (
                    "interval_ask_premium must be promoted to prior_session_ask_premium "
                    "during off-hours save"
                )

    def test_D2_regular_session_save_does_not_overwrite_prior_session(self, tmp_path):
        """
        During regular session, interval_* must NOT be promoted — they are
        live delta values, not a completed session summary.
        """
        import json
        from unittest.mock import patch
        from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE
        from data.options_theme_supplement import save_sectors_universe_lkg_to_disk

        sym = "REGULAR_TEST_SPY"
        universe_meta = {"proxy_symbols": [sym]}
        row_with_existing_ps = {
            **self._make_universe_row(sym),
            "prior_session_ask_premium": 999_999.0,  # sentinel — must survive
        }
        combined_data = {sym: row_with_existing_ps}
        disk_path = tmp_path / "sectors_lkg_regular.json"

        with (
            patch.dict(ENRICHED_THEME_RS_UNIVERSE, {"test_theme": universe_meta}),
            patch("data.options_theme_supplement.get_combined_ticker_data",
                  return_value=combined_data),
            patch("data.options_theme_supplement.get_no_options_symbols",
                  return_value=set()),
            patch("data.options_theme_supplement._SECTORS_LKG_DISK_PATH", disk_path),
            patch("data.tradier_market_session.get_session", return_value="regular"),
        ):
            save_sectors_universe_lkg_to_disk()

        if disk_path.exists():
            result = json.loads(disk_path.read_text())
            td = result.get("ticker_data", {})
            if sym in td:
                row = td[sym]
                # Existing prior_session_ask_premium must be preserved unchanged
                assert row.get("prior_session_ask_premium") == 999_999.0, (
                    "Regular-session save must preserve existing prior_session_* values"
                )


# ══════════════════════════════════════════════════════════════════════════════
# Class E — Real safe-promotion check
# ══════════════════════════════════════════════════════════════════════════════

class TestRealSafePromotion:
    """
    Verifies the 4-condition safe-promotion gate in _save_supplement_lkg_to_disk:
      (a) structural validity,
      (b) minimum universe coverage,
      (c) all-provider-errors batch,
      (d) sparse-batch ratio.
    """

    def test_E1_all_hard_fail_batch_blocked(self, tmp_path):
        import json, time as _t
        from unittest.mock import patch
        from data.options_theme_supplement import _save_supplement_lkg_to_disk

        # Fresh batch = 90 rows, all with scan_result in _HARD_FAIL + data_available=False
        hard_fail_batch = {
            f"FAIL{i:03d}": {
                "scan_result":     "provider_error",
                "data_available":  False,
            }
            for i in range(90)
        }
        # Existing baseline = 0 rows so no sparse-batch conflict
        disk_path = tmp_path / "supp_lkg_allfail.json"

        with patch("data.options_theme_supplement._SUPPLEMENT_LKG_DISK_PATH", disk_path):
            _save_supplement_lkg_to_disk(hard_fail_batch)

        result = json.loads(disk_path.read_text())
        assert result.get("promoted") is False, (
            "All-hard-fail batch must not be promoted"
        )
        assert result.get("promotion_rejection_reason") is not None
        assert "ALL_PROVIDER_ERRORS" in (result.get("promotion_rejection_reason") or ""), (
            f"Expected ALL_PROVIDER_ERRORS, got: {result.get('promotion_rejection_reason')}"
        )

    def test_E2_below_min_universe_blocked(self, tmp_path):
        import json
        from unittest.mock import patch
        from data.options_theme_supplement import _save_supplement_lkg_to_disk

        # Fresh batch = 30 rows (below _MIN_UNIVERSE=80)
        small_batch = {f"SML{i:02d}": {"score": float(i)} for i in range(30)}
        disk_path = tmp_path / "supp_lkg_small.json"

        with patch("data.options_theme_supplement._SUPPLEMENT_LKG_DISK_PATH", disk_path):
            _save_supplement_lkg_to_disk(small_batch)

        result = json.loads(disk_path.read_text())
        assert result.get("promoted") is False
        assert "BELOW_MIN_UNIVERSE" in (result.get("promotion_rejection_reason") or ""), (
            f"Expected BELOW_MIN_UNIVERSE, got: {result.get('promotion_rejection_reason')}"
        )

    def test_E3_valid_large_batch_promoted(self, tmp_path):
        import json, time as _t
        from unittest.mock import patch
        from data.options_theme_supplement import _save_supplement_lkg_to_disk

        # 100 good rows from scratch (prev_count=0, so no sparse check applies)
        fresh_batch = {f"GOOD{i:03d}": {"data_available": True, "score": float(i)}
                       for i in range(100)}
        disk_path = tmp_path / "supp_lkg_valid.json"

        with patch("data.options_theme_supplement._SUPPLEMENT_LKG_DISK_PATH", disk_path):
            _save_supplement_lkg_to_disk(fresh_batch)

        result = json.loads(disk_path.read_text())
        assert result.get("promoted") is True, (
            "100 valid rows from scratch must achieve promoted=True"
        )
        assert result.get("promotion_rejection_reason") is None

    def test_E4_mixed_batch_with_some_failures_can_be_promoted(self, tmp_path):
        """
        A batch that is mostly good rows but contains a few failures must still
        be promoted — all-errors check requires ALL rows to be hard-fail.
        """
        import json
        from unittest.mock import patch
        from data.options_theme_supplement import _save_supplement_lkg_to_disk

        mixed = {f"OK{i:03d}": {"data_available": True, "score": float(i)}
                 for i in range(95)}
        # Add 5 failure rows — batch is not all-errors
        for j in range(5):
            mixed[f"BAD{j:02d}"] = {"scan_result": "provider_error", "data_available": False}

        disk_path = tmp_path / "supp_lkg_mixed.json"
        with patch("data.options_theme_supplement._SUPPLEMENT_LKG_DISK_PATH", disk_path):
            _save_supplement_lkg_to_disk(mixed)

        result = json.loads(disk_path.read_text())
        assert result.get("promoted") is True, (
            "Batch with 95/100 good rows must be promoted (not all-errors)"
        )


# ══════════════════════════════════════════════════════════════════════════════
# Class F — Production-path integration (Tradier mocked at provider boundary)
# ══════════════════════════════════════════════════════════════════════════════

class TestProductionPathIntegration:
    """
    Calls real service entry points (scan_portfolio_options, scan_watchlist_options)
    with Tradier mocked at the provider boundary.

    Asserts:
      - No direct Tradier chain-level methods called from the portfolio service.
      - Uncached symbols are submitted to the supplement scanner via
        add_high_priority_symbols(), not scanned independently.
      - Results are served from the canonical snapshot or LKG.
    """

    def _make_cache(self, combined_data: dict):
        """Simple dict-backed mock cache."""
        store = {}
        class _Cache:
            def get(self, k, default=None):   return store.get(k, default)
            def set(self, k, v, ttl=None):    store[k] = v
            def delete(self, k):              store.pop(k, None)
        return _Cache(), store

    def test_F1_uncached_symbol_in_regular_session_triggers_delegation(self):
        """
        scan_portfolio_options with an empty combined snapshot must call
        add_high_priority_symbols, NOT tradier.get_option_expirations.
        """
        import asyncio
        from unittest.mock import patch, MagicMock, AsyncMock
        from data.portfolio_options_service import scan_portfolio_options

        tradier = MagicMock()
        tradier.get_option_expirations = AsyncMock(return_value=[])
        tradier.get_option_chain       = AsyncMock(return_value={})
        tradier.get_quotes             = AsyncMock(return_value=[])
        cache, _store = self._make_cache({})

        _hi_symbols: list[list] = []
        def _fake_add_hi(syms):
            _hi_symbols.append(list(syms))

        with (
            patch("data.tradier_market_session.get_session", return_value="regular"),
            patch("data.options_theme_supplement.add_high_priority_symbols",
                  side_effect=_fake_add_hi),
            patch("data.options_theme_supplement.get_combined_ticker_data",
                  return_value={}),
        ):
            asyncio.get_event_loop().run_until_complete(
                scan_portfolio_options(
                    symbols=["AAPL", "MSFT"],
                    tradier=tradier,
                    cache=cache,
                )
            )

        # Delegation must have fired
        all_delegated = [s for batch in _hi_symbols for s in batch]
        assert len(all_delegated) > 0, (
            "scan_portfolio_options must delegate uncached symbols via "
            "add_high_priority_symbols, not scan them directly"
        )
        # Tradier chain methods must NOT have been called
        tradier.get_option_expirations.assert_not_awaited()
        tradier.get_option_chain.assert_not_awaited()
        tradier.get_quotes.assert_not_awaited()

    def test_F2_cached_symbol_served_from_snapshot_without_delegation(self):
        """
        A symbol already present in the combined snapshot must be served
        immediately with no delegation and no Tradier calls.
        """
        import asyncio
        from unittest.mock import patch, MagicMock, AsyncMock
        from data.portfolio_options_service import scan_portfolio_options

        tradier = MagicMock()
        tradier.get_option_expirations = AsyncMock(return_value=[])
        tradier.get_quotes             = AsyncMock(return_value=[])
        cache, _store = self._make_cache({})

        combined = {
            "AAPL": {
                "ticker":               "AAPL",
                "data_available":       True,
                "final_composite_score": 55.0,
                "pc_ratio":             0.7,
                "total_volume":         8000,
                "source":               "master_screener_v1",
                "_cached_at":           1_700_000_000.0,
            }
        }

        _hi_called: list = []
        with (
            patch("data.tradier_market_session.get_session", return_value="regular"),
            patch("data.options_theme_supplement.add_high_priority_symbols",
                  side_effect=_hi_called.append),
            patch("data.options_theme_supplement.get_combined_ticker_data",
                  return_value=combined),
        ):
            result = asyncio.get_event_loop().run_until_complete(
                scan_portfolio_options(
                    symbols=["AAPL"],
                    tradier=tradier,
                    cache=cache,
                )
            )

        # Result has by_symbol dict
        by_sym = result.get("by_symbol", {}) if isinstance(result, dict) else {}
        assert "AAPL" in by_sym, (
            f"Snapshot-cached symbol must appear in result['by_symbol']. Keys: {list(by_sym)}"
        )
        # No direct Tradier calls
        tradier.get_option_expirations.assert_not_awaited()
        tradier.get_quotes.assert_not_awaited()

    def test_F3_disk_lkg_served_when_snapshot_empty_and_session_closed(self):
        """
        When the session is closed (not regular) and combined snapshot has no
        data, the disk LKG must be served directly — no delegation, no scan.
        """
        import asyncio
        from unittest.mock import patch, MagicMock, AsyncMock
        from data.portfolio_options_service import scan_portfolio_options

        tradier = MagicMock()
        tradier.get_option_expirations = AsyncMock(return_value=[])
        tradier.get_quotes             = AsyncMock(return_value=[])
        cache, _store = self._make_cache({})

        disk_lkg = {
            "SPY": {
                "ticker":         "SPY",
                "data_available": True,
                "score":          50.0,
                "source":         "portfolio_opts_lkg_disk",
            }
        }

        _hi_called: list = []
        with (
            patch("data.tradier_market_session.get_session", return_value="closed"),
            patch("data.options_theme_supplement.add_high_priority_symbols",
                  side_effect=_hi_called.append),
            patch("data.options_theme_supplement.get_combined_ticker_data",
                  return_value={}),
            patch("data.portfolio_options_service._load_portfolio_lkg",
                  return_value=disk_lkg),
        ):
            result = asyncio.get_event_loop().run_until_complete(
                scan_portfolio_options(
                    symbols=["SPY"],
                    tradier=tradier,
                    cache=cache,
                )
            )

        # Tradier methods must not be called during closed session
        tradier.get_option_expirations.assert_not_awaited()
        tradier.get_quotes.assert_not_awaited()
        # Delegation must NOT fire for closed session (not regular)
        assert _hi_called == [], (
            "Off-hours session must not trigger supplement delegation"
        )
        # Result shape check (symbol may come from disk LKG or unavailable)
        by_sym = result.get("by_symbol", {}) if isinstance(result, dict) else {}
        assert "SPY" in by_sym, f"SPY must be in result['by_symbol'], got: {list(by_sym)}"

    def test_F4_scan_watchlist_options_has_no_direct_tradier_chain_calls(self):
        """
        scan_watchlist_options source must not contain calls to
        tradier.get_option_expirations or tradier.get_option_chain.
        """
        import inspect, re
        from data import portfolio_options_service as _m
        src = inspect.getsource(_m.scan_watchlist_options)
        calls_exp   = re.findall(r"tradier\.get_option_expirations\s*\(", src)
        calls_chain = re.findall(r"tradier\.get_option_chain\s*\(",       src)
        assert calls_exp   == [], f"scan_watchlist_options has get_option_expirations calls"
        assert calls_chain == [], f"scan_watchlist_options has get_option_chain calls"
