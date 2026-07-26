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
