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
from services.catalyst_calendar_service import (
    _classify_event_family,
    _compute_signal_tier,
    _compute_signal_reason,
    _build_event,
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


# ═══════════════════════════════════════════════════════════════════════════════
# Macro signal classification tests
# ═══════════════════════════════════════════════════════════════════════════════

def _fml(event_name: str, event_type: str = "economic_release", country: str = "US") -> tuple:
    """Helper: classify and return (family, tier, reason)."""
    family = _classify_event_family(event_type, event_name, event_name, country)
    tier   = _compute_signal_tier(family)
    reason = _compute_signal_reason(family, country)
    return family, tier, reason


# ── FOMC / Fed ───────────────────────────────────────────────────────────────

def test_fomc_rate_decision():
    family, tier, reason = _fml("Fed Interest Rate Decision")
    assert family == "fomc_decision"
    assert tier == "critical"
    assert "FOMC" in reason or "rate decision" in reason


def test_fomc_rate_decision_alt_title():
    family, tier, reason = _fml("FOMC Interest Rate Decision")
    assert family == "fomc_decision"
    assert tier == "critical"


def test_fomc_minutes():
    family, tier, reason = _fml("FOMC Minutes")
    assert family == "fomc_minutes"
    assert tier == "major"
    assert "minutes" in reason.lower()


def test_fomc_minutes_alt():
    family, tier, reason = _fml("FOMC Meeting Minutes")
    assert family == "fomc_minutes"
    assert tier == "major"


def test_fed_chair_speech():
    family, tier, reason = _fml("Fed Chair Powell Speaks")
    assert family == "fed_chair_speech"
    assert tier == "secondary"


def test_fed_chair_speech_remarks():
    family, tier, reason = _fml("Fed Chair Powell Remarks")
    assert family == "fed_chair_speech"
    assert tier == "secondary"


def test_regional_fed_speech():
    family, tier, reason = _fml("Fed Barr Speech")
    assert family == "fed_speech"
    assert tier == "secondary"


def test_regional_fed_speaker():
    family, tier, reason = _fml("Fed Williams Speaks")
    assert family == "fed_speech"
    assert tier == "secondary"


def test_fed_speech_word_boundary():
    """'Federal Budget' must NOT match fed_speech."""
    family, tier, reason = _fml("Federal Budget Release")
    assert family != "fed_speech", f"got {family}"

    # FOMC without "minutes" should not match fomc_minutes
    family2, _, _ = _fml("FOMC Press Conference")
    assert family2 != "fomc_minutes"


# ── Major inflation indicators ──────────────────────────────────────────────

def test_cpi_headline():
    family, tier, _ = _fml("CPI YoY")
    assert family == "cpi"
    assert tier == "major"


def test_core_cpi_mom():
    family, tier, _ = _fml("Core CPI MoM")
    assert family == "cpi"
    assert tier == "major"


def test_cpi_consumer_price_index():
    family, tier, _ = _fml("Consumer Price Index")
    assert family == "cpi"
    assert tier == "major"


def test_ppi():
    family, tier, _ = _fml("PPI MoM")
    assert family == "ppi"
    assert tier == "major"


def test_core_ppi():
    family, tier, _ = _fml("Core PPI MoM")
    assert family == "ppi"
    assert tier == "major"


def test_pce_headline():
    family, tier, _ = _fml("PCE Price Index")
    assert family == "pce"
    assert tier == "major"


def test_core_pce():
    family, tier, _ = _fml("Core PCE Price Index")
    assert family == "pce"
    assert tier == "major"


def test_core_pce_mom():
    family, tier, _ = _fml("Core PCE MoM")
    assert family == "pce"
    assert tier == "major"


def test_eci():
    family, tier, _ = _fml("Employment Cost Index")
    assert family == "eci"
    assert tier == "major"


# ── Labor market ─────────────────────────────────────────────────────────────

def test_nonfarm_payrolls():
    family, tier, _ = _fml("Nonfarm Payrolls")
    assert family == "payrolls"
    assert tier == "major"


def test_nfp():
    family, tier, _ = _fml("NFP")
    assert family == "payrolls"
    assert tier == "major"


def test_non_farm_payroll():
    family, tier, _ = _fml("Non-Farm Payroll")
    assert family == "payrolls"
    assert tier == "major"


def test_jobless_claims():
    family, tier, _ = _fml("Initial Jobless Claims")
    assert family == "jobless_claims"
    assert tier == "secondary"


def test_jobless_claims_short():
    family, tier, _ = _fml("Initial Claims")
    assert family == "jobless_claims"
    assert tier == "secondary"


def test_continuing_claims():
    family, tier, _ = _fml("Continuing Jobless Claims")
    assert family == "jobless_claims"
    assert tier == "secondary"


def test_unemployment_rate():
    family, tier, _ = _fml("Unemployment Rate")
    assert family == "unemployment"
    assert tier == "secondary"


# ── Growth ───────────────────────────────────────────────────────────────────

def test_gdp():
    family, tier, _ = _fml("GDP YoY")
    assert family == "gdp"
    assert tier == "major"


def test_gdp_qoq():
    family, tier, _ = _fml("GDP QoQ")
    assert family == "gdp"
    assert tier == "major"


# ── Business surveys ─────────────────────────────────────────────────────────

def test_ism_manufacturing():
    family, tier, _ = _fml("ISM Manufacturing PMI")
    assert family == "ism"
    assert tier == "secondary"


def test_ism_services():
    family, tier, _ = _fml("ISM Services PMI")
    assert family == "ism"
    assert tier == "secondary"


def test_pmi_manufacturing():
    family, tier, _ = _fml("Manufacturing PMI")
    assert family == "pmi"
    assert tier == "secondary"


# ── Consumer / housing ───────────────────────────────────────────────────────

def test_retail_sales():
    family, tier, _ = _fml("Retail Sales")
    assert family == "retail_sales"
    assert tier == "secondary"


def test_consumer_sentiment():
    family, tier, _ = _fml("Consumer Sentiment")
    assert family == "consumer_sentiment"
    assert tier == "secondary"


def test_consumer_confidence():
    family, tier, _ = _fml("Consumer Confidence")
    assert family == "consumer_sentiment"
    assert tier == "secondary"


def test_housing_starts():
    family, tier, _ = _fml("Housing Starts")
    assert family == "housing"
    assert tier == "secondary"


def test_building_permits():
    family, tier, _ = _fml("Building Permits")
    assert family == "housing"
    assert tier == "secondary"


def test_existing_home_sales():
    family, tier, _ = _fml("Existing Home Sales")
    assert family == "housing"
    assert tier == "secondary"


# ── Treasury ─────────────────────────────────────────────────────────────────

def test_treasury_auction():
    family, tier, _ = _fml("10-Year Treasury Auction", country="US")
    assert family == "treasury_auction"
    assert tier == "major"


def test_treasury_auction_bill():
    family, tier, _ = _fml("Treasury Bill Auction", country="US")
    assert family == "treasury_auction"
    assert tier == "major"


def test_treasury_rate_10y():
    """Routine yield observation — context, not major."""
    family, tier, _ = _fml("10Y Treasury Rate", event_type="treasury_rate")
    assert family == "treasury_rate"
    assert tier == "context"


def test_treasury_rate_2y():
    family, tier, _ = _fml("2Y Treasury Rate", event_type="treasury_rate")
    assert family == "treasury_rate"
    assert tier == "context"


def test_treasury_yield_snapshot():
    family, tier, _ = _fml("Treasury Yield Snapshot", event_type="treasury_rate")
    assert family == "treasury_snapshot"
    assert tier == "context"


# ── Foreign ──────────────────────────────────────────────────────────────────

def test_foreign_cpi():
    family, tier, reason = _fml("CPI YoY", country="DE")
    assert family == "foreign"
    assert tier == "context"
    assert "(DE)" in reason


def test_foreign_eurozone():
    family, tier, _ = _fml("CPI YoY", country="EU")
    assert family == "foreign"
    assert tier == "context"


def test_foreign_no_country():
    family, tier, _ = _fml("Some Release", country="JP")
    assert family == "foreign"
    assert tier == "context"


# ── Catch-all ────────────────────────────────────────────────────────────────

def test_other_us():
    family, tier, _ = _fml("Some Lesser Follow-up Release")
    assert family == "other_us"
    assert tier == "secondary"


def test_other_us_tier_not_context():
    """US events that don't match any keyword still get secondary, not context."""
    family, tier, _ = _fml("State Level Industrial Data")
    assert family == "other_us"
    assert tier == "secondary"


# ── Existing importance and event contract ───────────────────────────────────

def test_importance_unchanged():
    """Verify the existing importance field is NOT changed by new fields."""
    ev = _build_event(
        id="test", eventType="economic_release", date="2026-05-01",
        title="CPI YoY", country="US", importance="high",
    )
    assert ev["importance"] == "high"
    assert "event_family" in ev
    assert "signal_tier" in ev
    assert "signal_reason" in ev


def test_existing_keys_present():
    """All canonical event keys must survive."""
    ev = _build_event(
        id="e1", eventType="economic_release", date="2026-05-01",
        title="Test", country="US", importance="medium",
    )
    for k in ("id", "eventType", "date", "title", "importance", "country"):
        assert k in ev, f"missing key {k}"
    for k in ("event_family", "signal_tier", "signal_reason"):
        assert k in ev, f"missing new key {k}"


# ── No provider, schema, cache, or scheduler changes ────────────────────────

def test_no_new_imports():
    """The classification helpers must not import httpx, cache, or DB modules."""
    import inspect
    src = inspect.getsource(_classify_event_family)
    assert "httpx" not in src
    assert "cache" not in src
    assert "psycopg" not in src
    assert "neon" not in src.lower()


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
