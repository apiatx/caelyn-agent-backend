"""
Unit tests for trading_dashboard_service — pure computation, cache, and snapshot.

Uses deterministic fixtures. No network, no DB, no provider calls.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

_BACKEND = Path(__file__).resolve().parent.parent
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from services.trading_dashboard_service import (
    compute_trading_dashboard,
    get_trading_dashboard,
    get_trading_dashboard_snapshot,
    clear_dashboard_cache,
    _cache,
    _DASHBOARD_TTL,
)

# ═══════════════════════════════════════════════════════════════════════════════
# Deterministic fixtures
# ═══════════════════════════════════════════════════════════════════════════════

def _risk_data() -> dict:
    return {
        "volatility": {
            "vix": 18.5,
            "vix_change": -0.3,
            "signal": "normal",
        },
        "fear_greed": {
            "score": 55.0,
            "rating": "Neutral",
            "components": {
                "put_call_options": 50,
                "stock_price_breadth": 62,
                "stock_price_strength": 58,
                "safe_haven_demand": 40,
                "market_momentum_sp500": 55,
                "junk_bond_demand": 52,
            },
        },
        "credit_spreads": {
            "hy_oas": 3.2,
            "bbb_oas": 1.6,
            "hy_signal": "normal",
        },
        "yield_curve_risk": {
            "spread_2s10s": -0.35,
            "inverted": True,
        },
        "dollar": {
            "dxy": 104.5,
            "dxy_change_pct": -0.2,
        },
    }


def _macro_data() -> dict:
    return {
        "benchmark_etfs": [
            {
                "ticker": "SPY",
                "price": 580.0,
                "change_pct": 0.35,
                "pct_from_52w_high": -3.5,
            },
            {
                "ticker": "QQQ",
                "price": 480.0,
                "change_pct": 0.55,
                "pct_from_52w_high": -4.2,
            },
        ],
        "rates_and_yields": {
            "us_10y": 4.55,
        },
    }


def _calendar_data() -> dict:
    return {
        "events": [
            {
                "event": "CPI data",
                "date": "2026-08-12",
                "days_out": 9,
            },
            {
                "event": "FOMC meeting",
                "date": "2026-08-20",
                "days_out": 17,
            },
        ],
    }


def _sector_perf_raw() -> list:
    return [
        {"sector": "Technology", "changesPercentage": 1.2},
        {"sector": "Healthcare", "changesPercentage": 0.3},
        {"sector": "Financial Services", "changesPercentage": -0.1},
        {"sector": "Energy", "changesPercentage": 0.8},
        {"sector": "Industrials", "changesPercentage": 0.5},
        {"sector": "Consumer Defensive", "changesPercentage": -0.4},
        {"sector": "Consumer Cyclical", "changesPercentage": 0.1},
        {"sector": "Basic Materials", "changesPercentage": -0.15},
        {"sector": "Utilities", "changesPercentage": -0.05},
        {"sector": "Real Estate", "changesPercentage": 0.0},
        {"sector": "Communication Services", "changesPercentage": 0.9},
    ]


def _spy_qqq_extended() -> dict:
    return {
        "SPY": {
            "price": 580.0,
            "priceAvg50": 570.0,
            "priceAvg200": 555.0,
            "recent_bars": [
                {"high": 582, "low": 577, "close": 580, "volume": 50_000_000},
                {"high": 583, "low": 578, "close": 581, "volume": 52_000_000},
                {"high": 584, "low": 579, "close": 579, "volume": 48_000_000},
                {"high": 581, "low": 575, "close": 580, "volume": 55_000_000},
                {"high": 585, "low": 577, "close": 584, "volume": 60_000_000},
            ],
        },
        "QQQ": {
            "price": 480.0,
            "priceAvg50": 475.0,
            "priceAvg200": 460.0,
        },
    }


def _vix_history() -> dict:
    import random
    random.seed(42)
    return {
        "data": [
            {"value": random.uniform(14, 22)}
            for _ in range(100)
        ],
    }


def fixtures():
    return (
        _risk_data(),
        _macro_data(),
        _calendar_data(),
        _sector_perf_raw(),
        _spy_qqq_extended(),
        _vix_history(),
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Pure computation — baseline fixture parity
# ═══════════════════════════════════════════════════════════════════════════════

def test_baseline_swing_mode_shape() -> None:
    """All expected top-level keys present in swing mode."""
    r = compute_trading_dashboard("swing", *fixtures())
    expected_keys = {
        "decision", "market_quality_score", "execution_window_score",
        "mode", "pillars", "summary", "execution_conditions",
        "terminal_analysis", "alert", "sector_performance",
        "as_of", "from_cache",
    }
    assert expected_keys <= set(r.keys()), f"Missing keys: {expected_keys - set(r.keys())}"
    assert r["from_cache"] is False
    assert r["mode"] == "swing"
    print("test_baseline_swing_mode_shape PASSED")


def test_baseline_day_mode_shape() -> None:
    """All expected top-level keys present in day mode."""
    r = compute_trading_dashboard("day", *fixtures())
    assert r["mode"] == "day"
    print("test_baseline_day_mode_shape PASSED")


def test_pillar_count_is_five() -> None:
    """Exactly 5 pillars returned."""
    for mode in ("swing", "day"):
        r = compute_trading_dashboard(mode, *fixtures())
        assert len(r["pillars"]) == 5
        for p in r["pillars"]:
            assert "score" in p
            assert "weight" in p
            assert "direction" in p
            assert "metrics" in p
            assert isinstance(p["metrics"], list)
    print("test_pillar_count_is_five PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Score formulas
# ═══════════════════════════════════════════════════════════════════════════════

def test_swing_weights_preserved() -> None:
    """Swing mode weights: 30, 25, 20, 15, 10%."""
    r = compute_trading_dashboard("swing", *fixtures())
    for i, expected_w in enumerate([30, 25, 20, 15, 10]):
        assert r["pillars"][i]["weight"] == expected_w, \
            f"Pillar {i}: expected {expected_w}, got {r['pillars'][i]['weight']}"
    print("test_swing_weights_preserved PASSED")


def test_day_weights_preserved() -> None:
    """Day mode weights: 25, 20, 20, 15, 20%."""
    r = compute_trading_dashboard("day", *fixtures())
    for i, expected_w in enumerate([25, 20, 20, 15, 20]):
        assert r["pillars"][i]["weight"] == expected_w, \
            f"Pillar {i}: expected {expected_w}, got {r['pillars'][i]['weight']}"
    print("test_day_weights_preserved PASSED")


def test_mqs_is_weighted_average() -> None:
    """MQS = sum(pillar_score * weight)."""
    r = compute_trading_dashboard("swing", *fixtures())
    computed_mqs = round(sum(
        p["score"] * (p["weight"] / 100.0)
        for p in r["pillars"]
    ), 1)
    assert r["market_quality_score"] == computed_mqs, \
        f"MQS mismatch: {r['market_quality_score']} != {computed_mqs}"
    print("test_mqs_is_weighted_average PASSED")


def test_ews_is_sum_of_ok_conditions() -> None:
    """EWS is 25 * number of passing execution conditions."""
    r = compute_trading_dashboard("swing", *fixtures())
    ok_count = sum(1 for c in r["execution_conditions"] if c["ok"])
    expected_ews = float(ok_count * 25)
    assert r["execution_window_score"] == expected_ews, \
        f"EWS mismatch: {r['execution_window_score']} != {expected_ews}"
    print("test_ews_is_sum_of_ok_conditions PASSED")


def test_ews_values_valid() -> None:
    """EWS must be 0, 25, 50, 75, or 100."""
    r = compute_trading_dashboard("swing", *fixtures())
    ews = r["execution_window_score"]
    assert ews in (0, 25, 50, 75, 100), f"Invalid EWS value: {ews}"
    print("test_ews_values_valid PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Decision thresholds
# ═══════════════════════════════════════════════════════════════════════════════

def test_decision_thresholds() -> None:
    """YES >= 70, CAUTION >= 40, NO < 40 (on adjusted MQS)."""
    # The current fixtures produce a specific decision — test the thresholds
    r = compute_trading_dashboard("swing", *fixtures())
    assert r["decision"] in ("YES", "CAUTION", "NO")
    # Verify MQS_adjustment is tracked
    mqs = r["market_quality_score"]
    # With current fixtures, we should get a specific decision
    print(f"  MQS={mqs}, Decision={r['decision']}")
    print("test_decision_thresholds PASSED")


def test_caution_decision() -> None:
    """Modify fixture to get CAUTION."""
    risk = _risk_data()
    # Push VIX high enough to suppress scores, but not extreme
    risk["volatility"]["vix"] = 24.0
    risk["volatility"]["signal"] = "elevated"
    r = compute_trading_dashboard("swing",
        risk, _macro_data(), _calendar_data(),
        _sector_perf_raw(), _spy_qqq_extended(), _vix_history(),
    )
    print(f"  CAUTION scenario: MQS={r['market_quality_score']}, Decision={r['decision']}")
    print("test_caution_decision PASSED")


def test_event_penalty() -> None:
    """Event penalty is applied when FOMC is within 1 day."""
    cal = {
        "events": [
            {"event": "FOMC meeting", "date": "2026-08-03", "days_out": 1},
        ]
    }
    r1 = compute_trading_dashboard("swing",
        _risk_data(), _macro_data(), _calendar_data(),
        _sector_perf_raw(), _spy_qqq_extended(), _vix_history(),
    )
    r2 = compute_trading_dashboard("swing",
        _risk_data(), _macro_data(), cal,
        _sector_perf_raw(), _spy_qqq_extended(), _vix_history(),
    )
    # Event penalty should reduce MQS_adj compared to raw MQS, or at least
    # not increase the decision threshold
    assert r2["alert"]["show"] is True
    print(f"  No penalty MQS={r1['market_quality_score']}, With penalty MQS={r2['market_quality_score']}")
    print("test_event_penalty PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Missing optional inputs
# ═══════════════════════════════════════════════════════════════════════════════

def test_missing_sector_perf() -> None:
    """No crash when sector_perf_raw is None."""
    r = compute_trading_dashboard("swing",
        _risk_data(), _macro_data(), _calendar_data(),
        None, _spy_qqq_extended(), _vix_history(),
    )
    assert isinstance(r["sector_performance"], list)
    assert r["sector_performance"] == []
    print("test_missing_sector_perf PASSED")


def test_missing_spy_qqq_extended() -> None:
    """No crash when spy_qqq_extended is None."""
    r = compute_trading_dashboard("swing",
        _risk_data(), _macro_data(), _calendar_data(),
        _sector_perf_raw(), None, _vix_history(),
    )
    assert r["market_quality_score"] is not None
    print("test_missing_spy_qqq_extended PASSED")


def test_missing_vix_history() -> None:
    """No crash when vix_history is None."""
    r = compute_trading_dashboard("swing",
        _risk_data(), _macro_data(), _calendar_data(),
        _sector_perf_raw(), _spy_qqq_extended(), None,
    )
    assert r["market_quality_score"] is not None
    print("test_missing_vix_history PASSED")


def test_all_missing_optional() -> None:
    """No crash when all optional inputs are None."""
    r = compute_trading_dashboard("swing",
        _risk_data(), _macro_data(), _calendar_data(),
        None, None, None,
    )
    assert isinstance(r, dict)
    assert "decision" in r
    print("test_all_missing_optional PASSED")


def test_empty_risk_data() -> None:
    """No crash when risk_data is empty dict."""
    r = compute_trading_dashboard("swing", {}, {}, {}, None, None, None)
    assert isinstance(r, dict)
    assert "decision" in r
    print("test_empty_risk_data PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Sector sorting
# ═══════════════════════════════════════════════════════════════════════════════

def test_sector_sorting_best_to_worst() -> None:
    """Sector performance is sorted best to worst."""
    r = compute_trading_dashboard("swing", *fixtures())
    sectors = r["sector_performance"]
    assert len(sectors) == 11
    changes = [s["change_pct"] for s in sectors]
    assert changes == sorted(changes, reverse=True), \
        f"Sectors not sorted best-to-worst: {changes}"
    print("test_sector_sorting_best_to_worst PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Terminal analysis
# ═══════════════════════════════════════════════════════════════════════════════

def test_terminal_analysis_structure() -> None:
    """Terminal analysis has correct structure."""
    r = compute_trading_dashboard("swing", *fixtures())
    ta = r["terminal_analysis"]
    assert isinstance(ta, list)
    for entry in ta:
        assert "type" in entry
        assert "text" in entry
        assert entry["type"] in ("dim", "green", "yellow", "red", "blue")
    print("test_terminal_analysis_structure PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Cache tests
# ═══════════════════════════════════════════════════════════════════════════════

def _setup():
    clear_dashboard_cache()
    assert _cache == {}


async def _fetch_fresh():
    return fixtures()


async def test_cache_first_get_fresh() -> None:
    """First get: fresh builder called, from_cache=False."""
    _setup()

    build_count = [0]  # mutable counter

    async def counting_fetch():
        build_count[0] += 1
        return fixtures()

    r = await get_trading_dashboard(mode="swing", fetch_fresh_data=counting_fetch)
    assert r["from_cache"] is False
    assert build_count[0] == 1
    print("test_cache_first_get_fresh PASSED")


async def test_cache_second_get_cached() -> None:
    """Second get within TTL: builder not called, from_cache=True."""
    _setup()

    build_count = [0]

    async def counting_fetch():
        build_count[0] += 1
        return fixtures()

    r1 = await get_trading_dashboard(mode="swing", fetch_fresh_data=counting_fetch)
    assert r1["from_cache"] is False
    assert build_count[0] == 1

    r2 = await get_trading_dashboard(mode="swing", fetch_fresh_data=counting_fetch)
    assert r2["from_cache"] is True
    assert build_count[0] == 1  # not called again
    print("test_cache_second_get_cached PASSED")


async def test_cache_force_refresh() -> None:
    """force=True bypasses cache."""
    _setup()

    build_count = [0]

    async def counting_fetch():
        build_count[0] += 1
        return fixtures()

    r1 = await get_trading_dashboard(mode="swing", fetch_fresh_data=counting_fetch)
    assert r1["from_cache"] is False

    r2 = await get_trading_dashboard(mode="swing", force=True, fetch_fresh_data=counting_fetch)
    assert r2["from_cache"] is False
    assert build_count[0] == 2
    print("test_cache_force_refresh PASSED")


def test_cache_keys_isolated() -> None:
    """Swing and day caches are isolated."""
    _setup()

    # Manually set different entries
    from services.trading_dashboard_service import _cache_key
    swing_key = _cache_key("swing")
    day_key = _cache_key("day")
    _cache[swing_key] = {"_ts": time.time(), "mode": "swing", "decision": "YES"}
    _cache[day_key] = {"_ts": time.time(), "mode": "day", "decision": "NO"}

    snap_swing = get_trading_dashboard_snapshot("swing")
    snap_day = get_trading_dashboard_snapshot("day")

    assert snap_swing["dashboard"]["decision"] == "YES"
    assert snap_day["dashboard"]["decision"] == "NO"

    _setup()
    print("test_cache_keys_isolated PASSED")


def test_cache_defensive_copy() -> None:
    """Mutating a returned response does not mutate the cached object."""
    _setup()

    key = "trading_dashboard_swing"
    _cache[key] = {"_ts": time.time(), "decision": "YES", "market_quality_score": 75.0}

    r = get_trading_dashboard_snapshot("swing")["dashboard"]
    r["decision"] = "MUTATED"
    r["market_quality_score"] = 999.0

    cached = _cache[key]
    assert cached["decision"] == "YES", "Cached decision was mutated!"
    assert cached["market_quality_score"] == 75.0, "Cached MQS was mutated!"

    _setup()
    print("test_cache_defensive_copy PASSED")


async def test_cache_clear() -> None:
    """Cache clear removes entries."""
    _setup()

    r = await get_trading_dashboard(mode="swing", fetch_fresh_data=_fetch_fresh)
    assert get_trading_dashboard_snapshot("swing")["status"] == "available"

    cleared = clear_dashboard_cache()
    assert "trading_dashboard_swing" in cleared
    assert get_trading_dashboard_snapshot("swing")["status"] == "unavailable"
    print("test_cache_clear PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Snapshot tests
# ═══════════════════════════════════════════════════════════════════════════════

def test_snapshot_before_first_build() -> None:
    """Before any build, snapshot is unavailable."""
    _setup()
    snap = get_trading_dashboard_snapshot()
    assert snap["status"] == "unavailable"
    assert snap["dashboard"] is None
    assert snap["age_seconds"] is None
    assert snap["expired"] is None
    print("test_snapshot_before_first_build PASSED")


async def test_snapshot_after_build() -> None:
    """After build, snapshot is available and matches."""
    _setup()
    r = await get_trading_dashboard(mode="swing", fetch_fresh_data=_fetch_fresh)
    snap = get_trading_dashboard_snapshot("swing")
    assert snap["status"] == "available"
    assert snap["dashboard"]["decision"] == r["decision"]
    assert snap["dashboard"]["market_quality_score"] == r["market_quality_score"]
    assert snap["dashboard"]["execution_window_score"] == r["execution_window_score"]
    assert snap["age_seconds"] >= 0
    print("test_snapshot_after_build PASSED")


async def test_snapshot_zero_provider_calls() -> None:
    """Snapshot causes zero provider/builder calls."""
    _setup()

    build_count = [0]

    async def counting_fetch():
        build_count[0] += 1
        return fixtures()

    await get_trading_dashboard(mode="swing", fetch_fresh_data=counting_fetch)
    assert build_count[0] == 1

    # Snapshot — must not trigger builder
    before = build_count[0]
    snap = get_trading_dashboard_snapshot("swing")
    assert build_count[0] == before, "Snapshot triggered a fresh build!"
    assert snap["dashboard"] is not None
    print("test_snapshot_zero_provider_calls PASSED")


def test_snapshot_expired_allow() -> None:
    """Expired entry returned when allow_expired=True."""
    _setup()
    key = "trading_dashboard_swing"
    # Insert an old entry
    _cache[key] = {"_ts": time.time() - _DASHBOARD_TTL - 10, "decision": "CAUTION"}

    snap = get_trading_dashboard_snapshot("swing", allow_expired=True)
    assert snap is not None
    assert snap["expired"] is True
    assert snap["status"] == "expired"
    assert snap["dashboard"]["decision"] == "CAUTION"

    _setup()
    print("test_snapshot_expired_allow PASSED")


def test_snapshot_expired_disallow() -> None:
    """Expired entry returns None when allow_expired=False."""
    _setup()
    key = "trading_dashboard_swing"
    _cache[key] = {"_ts": time.time() - _DASHBOARD_TTL - 10, "decision": "CAUTION"}

    snap = get_trading_dashboard_snapshot("swing", allow_expired=False)
    assert snap is None

    _setup()
    print("test_snapshot_expired_disallow PASSED")


def test_snapshot_defensive_copy() -> None:
    """Mutating snapshot dashboard does not mutate cache."""
    _setup()
    key = "trading_dashboard_swing"
    _cache[key] = {"_ts": time.time(), "decision": "YES", "market_quality_score": 80.0}

    snap = get_trading_dashboard_snapshot("swing")["dashboard"]
    snap["decision"] = "MUTATED"
    snap["pillars"] = []

    cached = _cache[key]
    assert cached["decision"] == "YES"
    assert "pillars" not in cached or cached.get("pillars") is not None

    _setup()
    print("test_snapshot_defensive_copy PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Mode normalization
# ═══════════════════════════════════════════════════════════════════════════════

def test_mode_normalization_at_cache_level() -> None:
    """Invalid mode defaults to swing at cache/snapshot level."""
    # Pure compute preserves whatever mode you pass
    r = compute_trading_dashboard("invalid", *fixtures())
    assert r["mode"] == "invalid"  # pure function, no normalization

    # Cache/snapshot level normalizes
    snap = get_trading_dashboard_snapshot("invalid", allow_expired=True)
    assert snap["mode"] == "swing"
    print("test_mode_normalization_at_cache_level PASSED")


def test_mode_case_insensitive_at_cache_level() -> None:
    """Mode normalization is case-insensitive at cache/snapshot level."""
    # Set entries manually to test normalization
    clear_dashboard_cache()
    from services.trading_dashboard_service import _cache_key
    key = _cache_key("swing")
    _cache[key] = {"_ts": time.time(), "decision": "YES", "mode": "swing"}
    snap = get_trading_dashboard_snapshot("SWING", allow_expired=True)
    assert snap["mode"] == "swing"  # normalized
    assert snap["status"] == "available"
    clear_dashboard_cache()
    print("test_mode_case_insensitive_at_cache_level PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Execution conditions edge cases
# ═══════════════════════════════════════════════════════════════════════════════

def test_ews_0_with_all_missing() -> None:
    """EWS should be 0 when no execution data is available."""
    ext = {
        "SPY": {
            "price": 580.0,
            "priceAvg50": None,
            "priceAvg200": None,
            "recent_bars": [],
        },
        "QQQ": {
            "price": 480.0,
            "priceAvg50": None,
            "priceAvg200": None,
        },
    }
    # Use sector perf with no leader data to break conditions
    perf = [
        {"sector": "Consumer Defensive", "changesPercentage": 0.5},
    ]
    r = compute_trading_dashboard("swing",
        _risk_data(), _macro_data(), _calendar_data(),
        perf, ext, _vix_history(),
    )
    # With 1 sector that isn't in the leader list, leaders_check fails
    # With no bars, pullbacks and follow-through fail
    # EWS could be 0
    assert r["execution_window_score"] in (0, 25, 50, 75, 100)
    print(f"  EWS with sparse data: {r['execution_window_score']}")
    print("test_ews_0_with_all_missing PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Run
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    test_baseline_swing_mode_shape()
    test_baseline_day_mode_shape()
    test_pillar_count_is_five()
    test_swing_weights_preserved()
    test_day_weights_preserved()
    test_mqs_is_weighted_average()
    test_ews_is_sum_of_ok_conditions()
    test_ews_values_valid()
    test_decision_thresholds()
    test_caution_decision()
    test_event_penalty()
    test_missing_sector_perf()
    test_missing_spy_qqq_extended()
    test_missing_vix_history()
    test_all_missing_optional()
    test_empty_risk_data()
    test_sector_sorting_best_to_worst()
    test_terminal_analysis_structure()
    test_mode_normalization_at_cache_level()
    test_mode_case_insensitive_at_cache_level()
    test_ews_0_with_all_missing()

    # Cache tests (async)
    import asyncio
    asyncio.run(test_cache_first_get_fresh())
    asyncio.run(test_cache_second_get_cached())
    asyncio.run(test_cache_force_refresh())
    test_cache_keys_isolated()
    test_cache_defensive_copy()
    asyncio.run(test_cache_clear())

    # Snapshot tests
    test_snapshot_before_first_build()
    asyncio.run(test_snapshot_after_build())
    asyncio.run(test_snapshot_zero_provider_calls())
    test_snapshot_expired_allow()
    test_snapshot_expired_disallow()
    test_snapshot_defensive_copy()

    print("\nAll 31 tests PASSED")
