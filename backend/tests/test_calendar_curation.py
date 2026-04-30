"""
Unit tests for services/calendar_curation.py.

These tests are mocked-data-only; they do NOT hit FMP or any network.
"""
from __future__ import annotations

import sys
from pathlib import Path

# Ensure backend/ is importable when running from repo root.
_BACKEND = Path(__file__).resolve().parent.parent
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from services.calendar_curation import (
    CURATED_TABS,
    DEFAULT_CAP_PER_SLICE,
    _canonical_symbol,
    _is_preferred_or_junk,
    curate_envelope,
    curate_events,
)


def test_preferred_filter_drops_pa_pb_etc():
    assert _is_preferred_or_junk("BAC-PA", "Bank of America Pref A")
    assert _is_preferred_or_junk("WFC-PR", None)
    assert _is_preferred_or_junk("XYZ", "Series A Preferred Stock")
    assert _is_preferred_or_junk("ABCWS", "ABC Warrants")
    assert not _is_preferred_or_junk("AAPL", "Apple Inc.")
    assert not _is_preferred_or_junk("BRK.B", "Berkshire Hathaway")  # share class


def test_canonical_symbol_remap():
    assert _canonical_symbol("FB") == "META"
    assert _canonical_symbol("AAPL") == "AAPL"
    assert _canonical_symbol("TWTR") is None  # delisted


def test_dividends_drops_preferred_and_microcap():
    raw = [
        {"symbol": "BAC-PA", "companyName": "BAC Pref", "eventType": "dividends", "date": "2026-05-01"},
        {"symbol": "AAPL", "companyName": "Apple", "eventType": "dividends",
         "date": "2026-05-02", "marketCap": 3e12},
        {"symbol": "TINY", "companyName": "Tiny Co", "eventType": "dividends",
         "date": "2026-05-02", "marketCap": 5_000_000},
        {"symbol": "MP", "companyName": "MP Materials", "eventType": "dividends",
         "date": "2026-05-03", "marketCap": 4e9, "sector": "Materials"},
    ]
    out = curate_events("dividends", raw, cap=10)
    syms = [e["symbol"] for e in out]
    assert "BAC-PA" not in syms
    assert "TINY" not in syms
    assert "AAPL" in syms
    assert "MP" in syms  # materials theme retained even though small-ish cap


def test_dividends_dedup_preserves_most_complete():
    raw = [
        {"symbol": "AAPL", "eventType": "dividends", "date": "2026-05-01",
         "companyName": "Apple Inc.", "marketCap": 3e12, "sector": "Tech",
         "industry": "Hardware", "exchange": "NASDAQ", "title": "AAPL Dividend",
         "importance": "high"},
        {"symbol": "AAPL", "eventType": "dividends", "date": "2026-05-01",
         "companyName": "Apple"},
    ]
    out = curate_events("dividends", raw, cap=10)
    assert len(out) == 1
    assert out[0]["sector"] == "Tech"


def test_economic_drops_minor_country_noise():
    raw = [
        {"eventType": "economic_releases", "eventName": "CPI", "country": "US",
         "date": "2026-05-01"},
        {"eventType": "economic_releases", "eventName": "Local Tractor Index",
         "country": "PE", "date": "2026-05-02"},
        {"eventType": "economic_releases", "eventName": "Nonfarm Payrolls",
         "country": "US", "date": "2026-05-03"},
    ]
    out = curate_events("economic_releases", raw, cap=50)
    names = [e["eventName"] for e in out]
    assert "Local Tractor Index" not in names
    assert "CPI" in names
    assert "Nonfarm Payrolls" in names
    # Top should be a high-impact US release.
    assert out[0]["country"].upper() in ("US", "USA", "UNITED STATES")


def test_treasury_keeps_only_key_maturities():
    raw = [
        {"eventType": "treasury_rate", "maturity": "10Y", "date": "2026-05-01"},
        {"eventType": "treasury_rate", "maturity": "50Y", "date": "2026-05-01"},
        {"eventType": "treasury_rate", "maturity": "2Y", "date": "2026-05-02"},
    ]
    out = curate_events("treasury_macro", raw, cap=50)
    mats = [e["maturity"] for e in out]
    assert "10Y" in mats and "2Y" in mats
    assert "50Y" not in mats


def test_splits_deprioritizes_reverse():
    raw = [
        {"symbol": "NVDA", "eventType": "splits", "date": "2026-05-15",
         "numerator": 10, "denominator": 1, "marketCap": 3e12,
         "sector": "Semiconductors"},
        {"symbol": "BAD", "eventType": "splits", "date": "2026-05-15",
         "numerator": 1, "denominator": 20, "marketCap": 50_000_000},
    ]
    out = curate_events("splits", raw, cap=10)
    # NVDA forward split should win the top slot.
    assert out[0]["symbol"] == "NVDA"


def test_ipos_drops_warrants_and_microcap_keeps_themed():
    raw = [
        {"symbol": "JNK-WT", "eventType": "ipos", "date": "2026-05-10",
         "companyName": "Junk Warrants"},
        {"symbol": "TINY", "eventType": "ipos", "date": "2026-05-10",
         "marketCap": 5_000_000, "exchange": "OTC"},
        {"symbol": "CHIP", "eventType": "ipos", "date": "2026-05-11",
         "marketCap": 5e9, "exchange": "NASDAQ", "sector": "Semiconductors"},
    ]
    out = curate_events("ipos", raw, cap=50)
    syms = {e["symbol"] for e in out}
    assert "JNK-WT" not in syms
    assert "TINY" not in syms
    assert "CHIP" in syms


def test_curate_envelope_preserves_meta_fields():
    env = {
        "current_week": [
            {"symbol": "AAPL", "eventType": "dividends", "date": "2026-05-01",
             "companyName": "Apple", "marketCap": 3e12},
        ],
        "previous_week": [],
        "last_updated": "2026-04-28T00:00:00Z",
        "status": "ready",
    }
    out = curate_envelope("dividends", env, cap=50)
    assert out["status"] == "ready"
    assert out["last_updated"] == "2026-04-28T00:00:00Z"
    assert isinstance(out["current_week"], list)
    assert isinstance(out["previous_week"], list)


def test_non_target_tab_passes_through_unchanged():
    env = {
        "current_week": [{"symbol": "X", "eventType": "earnings_dates"}],
        "previous_week": [],
        "last_updated": None,
        "status": "ready",
    }
    out = curate_envelope("earnings_dates", env)
    assert out is env  # short-circuit, no-op


def test_cap_trims_to_top_n():
    raw = [
        {"symbol": f"SYM{i}", "eventType": "dividends", "date": "2026-05-01",
         "companyName": f"Co{i}", "marketCap": 1e9 + i}
        for i in range(200)
    ]
    out = curate_events("dividends", raw, cap=50)
    assert len(out) == 50


if __name__ == "__main__":
    # Tiny self-running mode without pytest.
    fns = [v for k, v in globals().items() if k.startswith("test_") and callable(v)]
    failed = 0
    for fn in fns:
        try:
            fn()
            print(f"  PASS  {fn.__name__}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"  FAIL  {fn.__name__}: {e}")
    print(f"\n{len(fns) - failed}/{len(fns)} passed")
    raise SystemExit(1 if failed else 0)


# Top-level smoke assertion to keep the CURATED_TABS / cap exposed surface
# stable.
assert "dividends" in CURATED_TABS
assert isinstance(DEFAULT_CAP_PER_SLICE, int) and DEFAULT_CAP_PER_SLICE >= 25
