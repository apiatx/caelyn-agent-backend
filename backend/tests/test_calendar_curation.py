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
    curate_economic_logical_events,
    curate_envelope,
    curate_events,
    group_economic_events_to_families,
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
    titles = [
        e.get("display_title") or e.get("title") or e.get("eventName") or ""
        for e in out
    ]
    assert "Local Tractor Index" not in titles
    # CPI passes through as a discrete event; Nonfarm Payrolls becomes an
    # Employment Report package card through the shared canonical pipeline.
    assert any("CPI" in t for t in titles)
    assert any("Employment Report" in t for t in titles)
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


def test_fed_funds_rate_decision():
    family, tier, _ = _fml("Federal Funds Rate Decision")
    assert family == "fomc_decision"
    assert tier == "critical"


def test_fed_rate_decision():
    family, tier, _ = _fml("Fed Rate Decision")
    assert family == "fomc_decision"
    assert tier == "critical"


def test_fomc_rate_decision_no_interest():
    family, tier, _ = _fml("FOMC Rate Decision")
    assert family == "fomc_decision"
    assert tier == "critical"


def test_federal_reserve_rate_decision():
    family, tier, _ = _fml("Federal Reserve Rate Decision")
    assert family == "fomc_decision"
    assert tier == "critical"


def test_fomc_negative_not_decision():
    """Titles with FOMC/Fed but no rate decision must NOT classify as fomc_decision."""
    for title in ("FOMC Minutes", "FOMC Meeting Minutes",
                  "Fed Chair Powell Speaks", "Fed Chair Press Conference",
                  "Fed Barr Speech", "Federal Budget",
                  "Federal Reserve Beige Book"):
        family, _, _ = _fml(title)
        assert family != "fomc_decision", f"got fomc_decision for {title!r}"


def test_generic_rate_decision_not_fomc():
    """Generic rate decisions without Fed/FOMC context must NOT be fomc_decision."""
    for title in ("Prime Rate Decision", "Mortgage Rate Decision",
                  "Bank Lending Rate Decision"):
        family, _, _ = _fml(title)
        assert family != "fomc_decision", f"got fomc_decision for {title!r}"
        # They should fall through to other_us (US, no keyword match)
        assert family == "other_us", f"expected other_us, got {family} for {title!r}"


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


def test_treasury_auction_20y_bond():
    family, tier, _ = _fml("20-Year Bond Auction", country="US")
    assert family == "treasury_auction"
    assert tier == "major"


def test_treasury_auction_5y_note():
    family, tier, _ = _fml("5-Year Note Auction", country="US")
    assert family == "treasury_auction"
    assert tier == "major"


def test_treasury_auction_of_bills():
    family, tier, _ = _fml("Auction of 3-Month Treasury Bills", country="US")
    assert family == "treasury_auction"
    assert tier == "major"


def test_treasury_auction_of_notes():
    family, tier, _ = _fml("Auction of 10-Year Treasury Notes", country="US")
    assert family == "treasury_auction"
    assert tier == "major"


def test_treasury_auction_of_bonds():
    family, tier, _ = _fml("Auction of 30-Year Treasury Bonds", country="US")
    assert family == "treasury_auction"
    assert tier == "major"


def test_foreign_auction_german_bund():
    """Foreign sovereign debt auction must classify as foreign, not treasury_auction."""
    family, tier, _ = _fml("German 10-Year Bund Auction", country="DE")
    assert family == "foreign"
    assert tier == "context"


def test_foreign_auction_uk_gilt():
    family, tier, _ = _fml("UK Gilt Auction", country="GB")
    assert family == "foreign"
    assert tier == "context"


def test_foreign_auction_japan_govt_bond():
    family, tier, _ = _fml("Japanese Government Bond Auction", country="JP")
    assert family == "foreign"
    assert tier == "context"


def test_foreign_auction_european_bond():
    family, tier, _ = _fml("European Bond Auction", country="EU")
    assert family == "foreign"
    assert tier == "context"


def test_corporate_bond_auction_not_treasury():
    """US corporate bond auction must NOT classify as treasury_auction."""
    family, _, _ = _fml("Corporate Bond Auction", country="US")
    assert family != "treasury_auction", "corporate bond auction should not be treasury"
    assert family == "other_us"


def test_municipal_bond_auction_not_treasury():
    family, _, _ = _fml("Municipal Bond Auction", country="US")
    assert family != "treasury_auction"
    assert family == "other_us"


def test_generic_auction_not_treasury():
    """Generic US auctions without Treasury security semantics must NOT classify as treasury_auction."""
    for title in ("Oil Lease Auction", "Spectrum Auction", "Government Asset Auction"):
        family, _, _ = _fml(title, country="US")
        assert family != "treasury_auction", f"got treasury_auction for {title!r}"
        assert family == "other_us", f"expected other_us, got {family} for {title!r}"


def test_treasury_not_auction_yield():
    """Yield and rate observations must NOT classify as treasury_auction."""
    for title in ("10-Year Treasury Yield", "Treasury Bond Yield",
                  "2-Year Treasury Note Rate", "Treasury Bill Rate",
                  "Treasury Note", "Treasury Bond"):
        family, _, _ = _fml(title, country="US")
        assert family != "treasury_auction", f"got treasury_auction for {title!r}"


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


# ═══════════════════════════════════════════════════════════════════════════════
# Family-grouping tests
# ═══════════════════════════════════════════════════════════════════════════════

def _make_econ(**kw) -> dict:
    ev = _build_event(
        id=kw.get("id", "ev1"),
        eventType="economic_release",
        date=kw.get("date", "2026-05-01"),
        time=kw.get("time"),
        title=kw.get("title", "CPI MoM"),
        eventName=kw.get("eventName", kw.get("title", "CPI MoM")),
        country=kw.get("country", "US"),
        importance=kw.get("importance", "high"),
        actual=kw.get("actual"),
        estimate=kw.get("estimate"),
        previous=kw.get("previous"),
        event_family=kw.get("event_family"),
        signal_tier=kw.get("signal_tier"),
        signal_reason=kw.get("signal_reason"),
        source=kw.get("source", "fmp"),
        raw=kw.get("raw", {}),
    )
    if kw.get("unit") is not None:
        ev["unit"] = kw["unit"]
    return ev


def test_cpi_family_four_variants_become_one_card():
    cpi_evs = [
        _make_econ(id="c1", title="CPI MoM", eventName="CPI MoM (Jul)",
                   event_family="cpi", signal_tier="major",
                   signal_reason="Major consumer inflation release",
                   actual=0.2, estimate=0.3, previous=0.2, unit="%"),
        _make_econ(id="c2", title="Core CPI MoM", eventName="Core CPI MoM (Jul)",
                   event_family="cpi", signal_tier="major",
                   signal_reason="Major consumer inflation release",
                   actual=0.1, estimate=0.2, previous=0.1, unit="%"),
        _make_econ(id="c3", title="CPI YoY", eventName="CPI YoY (Jul)",
                   event_family="cpi", signal_tier="major",
                   signal_reason="Major consumer inflation release",
                   actual=3.0, estimate=3.1, previous=3.0, unit="%"),
        _make_econ(id="c4", title="Core CPI YoY", eventName="Core CPI YoY (Jul)",
                   event_family="cpi", signal_tier="major",
                   signal_reason="Major consumer inflation release",
                   actual=3.2, estimate=3.3, previous=3.2, unit="%"),
    ]
    result = group_economic_events_to_families(cpi_evs)
    family_cards = [e for e in result if e.get("type") == "macro_family"]
    assert len(family_cards) == 1, f"expected 1 family card, got {len(family_cards)}"
    card = family_cards[0]
    assert card["type"] == "macro_family"
    assert card["event_family"] == "cpi"
    assert card["country"] == "US"
    assert card["title"] == "CPI Inflation Report"
    assert card["children"] == cpi_evs
    assert card["event_count"] == 4
    assert card["source"] == "fmp"
    assert card["signal_tier"] == "major"


def test_core_cpi_mom_selected_as_lead():
    cpi_evs = [
        _make_econ(id="c1", title="CPI MoM", event_family="cpi",
                   signal_tier="major", actual=0.2, estimate=0.3, previous=0.2, unit="%"),
        _make_econ(id="c2", title="Core CPI MoM", event_family="cpi",
                   signal_tier="major", actual=0.1, estimate=0.2, previous=0.1, unit="%"),
        _make_econ(id="c3", title="CPI YoY", event_family="cpi",
                   signal_tier="major", actual=3.0, estimate=3.1, previous=3.0, unit="%"),
        _make_econ(id="c4", title="Core CPI YoY", event_family="cpi",
                   signal_tier="major", actual=3.2, estimate=3.3, previous=3.2, unit="%"),
    ]
    result = group_economic_events_to_families(cpi_evs)
    card = result[0]
    assert card["lead_metric"] == "Core Cpi Mom"
    assert card["actual"] == 0.1
    assert card["estimate"] == 0.2
    assert card["previous"] == 0.1
    assert card["unit"] == "%"


def test_pce_family_four_variants_become_one_card():
    pce_evs = [
        _make_econ(id="p1", title="PCE Price Index MoM", event_family="pce",
                   signal_tier="major", actual=0.2, estimate=0.3, previous=0.1, unit="%"),
        _make_econ(id="p2", title="PCE Price Index YoY", event_family="pce",
                   signal_tier="major", actual=2.5, estimate=2.5, previous=2.7, unit="%"),
        _make_econ(id="p3", title="Core PCE Price Index MoM", event_family="pce",
                   signal_tier="major", actual=0.1, estimate=0.2, previous=0.1, unit="%"),
        _make_econ(id="p4", title="Core PCE Price Index YoY", event_family="pce",
                   signal_tier="major", actual=2.6, estimate=2.6, previous=2.8, unit="%"),
    ]
    result = group_economic_events_to_families(pce_evs)
    family_cards = [e for e in result if e.get("type") == "macro_family"]
    assert len(family_cards) == 1
    card = family_cards[0]
    assert card["event_family"] == "pce"
    assert card["title"] == "PCE Inflation Report"
    assert card["event_count"] == 4


def test_core_pce_mom_selected_as_lead():
    pce_evs = [
        _make_econ(id="p1", title="PCE Price Index MoM", event_family="pce",
                   signal_tier="major", actual=0.2, unit="%"),
        _make_econ(id="p2", title="Core PCE MoM", event_family="pce",
                   signal_tier="major", actual=0.1, unit="%"),
    ]
    result = group_economic_events_to_families(pce_evs)
    card = result[0]
    assert card["lead_metric"] == "Core Pce Mom"
    assert card["actual"] == 0.1


def test_gdp_variants_become_one_card():
    gdp_evs = [
        _make_econ(id="g1", title="GDP Growth Rate QoQ", event_family="gdp",
                   signal_tier="major", actual=2.4, estimate=2.5, unit="%"),
        _make_econ(id="g2", title="GDP Sales QoQ", event_family="gdp",
                   signal_tier="major", actual=3.0, unit="%"),
        _make_econ(id="g3", title="GDP Price Index", event_family="gdp",
                   signal_tier="major", actual=3.1, unit="%"),
    ]
    result = group_economic_events_to_families(gdp_evs)
    family_cards = [e for e in result if e.get("type") == "macro_family"]
    assert len(family_cards) == 1
    card = family_cards[0]
    assert card["event_family"] == "gdp"
    assert card["title"] == "GDP Report"
    assert card["event_count"] == 3
    assert card["lead_metric"] == "Gdp Growth Rate Qoq"


def test_eci_variants_become_one_card():
    eci_evs = [
        _make_econ(id="e1", title="Employment Cost Index QoQ", event_family="eci",
                   signal_tier="major", actual=0.9, estimate=1.0, unit="%"),
        _make_econ(id="e2", title="Employment Cost Index YoY", event_family="eci",
                   signal_tier="major", actual=3.5, unit="%"),
    ]
    result = group_economic_events_to_families(eci_evs)
    family_cards = [e for e in result if e.get("type") == "macro_family"]
    assert len(family_cards) == 1
    card = family_cards[0]
    assert card["event_family"] == "eci"
    assert card["title"] == "Employment Cost Index"
    assert card["event_count"] == 2
    assert card["lead_metric"] == "Employment Cost Index Qoq"


def test_lead_actual_preserved():
    evs = [
        _make_econ(id="a1", title="CPI MoM", event_family="cpi",
                   signal_tier="major", actual=0.2, unit="%"),
        _make_econ(id="a2", title="Core CPI MoM", event_family="cpi",
                   signal_tier="major", actual=0.1, unit="%"),
    ]
    result = group_economic_events_to_families(evs)
    card = result[0]
    assert card["actual"] == 0.1


def test_lead_estimate_preserved():
    evs = [
        _make_econ(id="b1", title="CPI MoM", event_family="cpi",
                   signal_tier="major", actual=0.2, estimate=0.3, unit="%"),
        _make_econ(id="b2", title="Core CPI MoM", event_family="cpi",
                   signal_tier="major", actual=0.1, estimate=0.2, unit="%"),
    ]
    result = group_economic_events_to_families(evs)
    card = result[0]
    assert card["estimate"] == 0.2


def test_lead_previous_preserved():
    evs = [
        _make_econ(id="c1", title="CPI MoM", event_family="cpi",
                   signal_tier="major", actual=0.2, previous=0.2, unit="%"),
        _make_econ(id="c2", title="Core CPI MoM", event_family="cpi",
                   signal_tier="major", actual=0.1, previous=0.1, unit="%"),
    ]
    result = group_economic_events_to_families(evs)
    card = result[0]
    assert card["previous"] == 0.1


def test_lead_unit_preserved():
    evs = [
        _make_econ(id="d1", title="CPI MoM", event_family="cpi",
                   signal_tier="major", unit="%"),
        _make_econ(id="d2", title="Core CPI MoM", event_family="cpi",
                   signal_tier="major", unit="%"),
    ]
    result = group_economic_events_to_families(evs)
    card = result[0]
    assert card["unit"] == "%"


def test_every_source_row_preserved_in_children():
    cpi_evs = [
        _make_econ(id="x1", title="CPI MoM", event_family="cpi", signal_tier="major"),
        _make_econ(id="x2", title="Core CPI MoM", event_family="cpi", signal_tier="major"),
        _make_econ(id="x3", title="CPI YoY", event_family="cpi", signal_tier="major"),
        _make_econ(id="x4", title="Core CPI YoY", event_family="cpi", signal_tier="major"),
    ]
    result = group_economic_events_to_families(cpi_evs)
    card = result[0]
    assert len(card["children"]) == 4
    child_ids = [c["id"] for c in card["children"]]
    assert child_ids == ["x1", "x2", "x3", "x4"]


def test_event_count_is_exact():
    cpi_evs = [
        _make_econ(id="z1", title="CPI MoM", event_family="cpi", signal_tier="major"),
        _make_econ(id="z2", title="Core CPI MoM", event_family="cpi", signal_tier="major"),
    ]
    result = group_economic_events_to_families(cpi_evs)
    card = result[0]
    assert card["event_count"] == 2


def test_source_inputs_not_mutated():
    cpi_evs = [
        _make_econ(id="m1", title="CPI MoM", event_family="cpi", signal_tier="major"),
        _make_econ(id="m2", title="Core CPI MoM", event_family="cpi", signal_tier="major"),
    ]
    before = [dict(ev) for ev in cpi_evs]
    group_economic_events_to_families(cpi_evs)
    for i, ev in enumerate(cpi_evs):
        assert ev == before[i], f"event {i} was mutated"


def test_same_family_date_country_time_groups():
    evs = [
        _make_econ(id="s1", title="CPI MoM", event_family="cpi", signal_tier="major",
                   date="2026-05-01", time="08:30:00"),
        _make_econ(id="s2", title="Core CPI MoM", event_family="cpi", signal_tier="major",
                   date="2026-05-01", time="08:30:00"),
    ]
    result = group_economic_events_to_families(evs)
    family_cards = [e for e in result if e.get("type") == "macro_family"]
    assert len(family_cards) == 1
    assert family_cards[0]["event_count"] == 2


def test_different_dates_do_not_group():
    evs = [
        _make_econ(id="d1", title="CPI MoM", event_family="cpi", signal_tier="major",
                   date="2026-05-01"),
        _make_econ(id="d2", title="CPI MoM", event_family="cpi", signal_tier="major",
                   date="2026-05-02"),
    ]
    result = group_economic_events_to_families(evs)
    family_cards = [e for e in result if e.get("type") == "macro_family"]
    assert len(family_cards) == 2
    assert family_cards[0]["date"] == "2026-05-01"
    assert family_cards[1]["date"] == "2026-05-02"


def test_different_times_do_not_group():
    evs = [
        _make_econ(id="t1", title="CPI MoM", event_family="cpi", signal_tier="major",
                   date="2026-05-01", time="08:30:00"),
        _make_econ(id="t2", title="Core CPI MoM", event_family="cpi", signal_tier="major",
                   date="2026-05-01", time="10:00:00"),
    ]
    result = group_economic_events_to_families(evs)
    family_cards = [e for e in result if e.get("type") == "macro_family"]
    assert len(family_cards) == 2
    assert family_cards[0]["time"] == "08:30:00"
    assert family_cards[1]["time"] == "10:00:00"


def test_foreign_cpi_passes_through():
    evs = [
        _make_econ(id="f1", title="CPI YoY", event_family="cpi", signal_tier="major",
                   country="JP"),
    ]
    result = group_economic_events_to_families(evs)
    assert len(result) == 1
    assert result[0].get("type") != "macro_family"
    assert result[0]["country"] == "JP"


def test_fomc_decision_passes_through():
    evs = [
        _make_econ(id="fm1", title="FOMC Interest Rate Decision",
                   event_family="fomc_decision", signal_tier="critical",
                   country="US"),
    ]
    result = group_economic_events_to_families(evs)
    assert len(result) == 1
    assert result[0].get("type") != "macro_family"
    assert result[0]["event_family"] == "fomc_decision"


def test_payroll_and_unemployment_remain_separate():
    evs = [
        _make_econ(id="p1", title="Nonfarm Payrolls", event_family="payrolls",
                   signal_tier="major", country="US"),
        _make_econ(id="u1", title="Unemployment Rate", event_family="unemployment",
                   signal_tier="secondary", country="US"),
    ]
    result = group_economic_events_to_families(evs)
    assert len(result) == 2
    assert all(e.get("event_family") != "macro_family" and e.get("type") != "macro_family"
               for e in result)


def test_envelope_metadata_remains_unchanged():
    cpi_evs = [
        _make_econ(id="v1", title="CPI MoM", event_family="cpi", signal_tier="major"),
        _make_econ(id="v2", title="Core CPI MoM", event_family="cpi", signal_tier="major"),
    ]
    env = {
        "current_week": cpi_evs,
        "previous_week": [],
        "last_updated": "2026-05-01T00:00:00Z",
        "status": "ready",
        "is_stale": False,
        "diagnostics": {"event_count": 2},
        "window": {"stored_from": "2026-04-27", "stored_to": "2026-05-01"},
    }
    out = curate_envelope("economic_releases", env, cap=50)
    assert out["status"] == "ready"
    assert out["last_updated"] == "2026-05-01T00:00:00Z"
    assert out["is_stale"] is False
    assert out["diagnostics"] == {"event_count": 2}
    assert out["window"] == {"stored_from": "2026-04-27", "stored_to": "2026-05-01"}
    assert out["previous_week"] == []


def test_ordering_is_stable_and_deterministic():
    evs = [
        _make_econ(id="o1", title="CPI MoM", event_family="cpi", signal_tier="major",
                   date="2026-05-01"),
        _make_econ(id="o2", title="Core CPI MoM", event_family="cpi", signal_tier="major",
                   date="2026-05-01"),
        _make_econ(id="o3", title="PPI MoM", event_family="ppi", signal_tier="major",
                   date="2026-05-01"),
        _make_econ(id="o4", title="Core PPI MoM", event_family="ppi", signal_tier="major",
                   date="2026-05-01"),
    ]
    result1 = group_economic_events_to_families(evs)
    result2 = group_economic_events_to_families(evs)
    assert len(result1) == len(result2) == 2
    assert result1[0]["event_family"] == result2[0]["event_family"]
    assert result1[1]["event_family"] == result2[1]["event_family"]
    assert result1[0]["children"] == result2[0]["children"]


def test_family_id_is_stable():
    evs = [
        _make_econ(id="i1", title="CPI MoM", event_family="cpi", signal_tier="major",
                   date="2026-05-01"),
        _make_econ(id="i2", title="Core CPI MoM", event_family="cpi", signal_tier="major",
                   date="2026-05-01"),
    ]
    result1 = group_economic_events_to_families(evs)
    result2 = group_economic_events_to_families(evs)
    assert result1[0]["id"] == result2[0]["id"]


# ═══════════════════════════════════════════════════════════════════════════════
# PPI lead-metric precedence correction tests
# ═══════════════════════════════════════════════════════════════════════════════

def test_ppi_all_four_variants_selects_core_mom():
    evs = [
        _make_econ(id="pp1", title="Core PPI MoM", event_family="ppi",
                   signal_tier="major", actual=0.1, unit="%"),
        _make_econ(id="pp2", title="PPI MoM", event_family="ppi",
                   signal_tier="major", actual=0.2, unit="%"),
        _make_econ(id="pp3", title="Core PPI YoY", event_family="ppi",
                   signal_tier="major", actual=2.0, unit="%"),
        _make_econ(id="pp4", title="PPI YoY", event_family="ppi",
                   signal_tier="major", actual=2.5, unit="%"),
    ]
    result = group_economic_events_to_families(evs)
    card = result[0]
    assert card["lead_metric"] == "Core Ppi Mom"
    assert card["actual"] == 0.1


def test_ppi_without_core_mom_selects_ppi_mom():
    evs = [
        _make_econ(id="pp1", title="PPI MoM", event_family="ppi",
                   signal_tier="major", actual=0.2, unit="%"),
        _make_econ(id="pp2", title="Core PPI YoY", event_family="ppi",
                   signal_tier="major", actual=2.0, unit="%"),
        _make_econ(id="pp3", title="PPI YoY", event_family="ppi",
                   signal_tier="major", actual=2.5, unit="%"),
    ]
    result = group_economic_events_to_families(evs)
    card = result[0]
    assert card["lead_metric"] == "Ppi Mom"
    assert card["actual"] == 0.2


def test_ppi_only_yoy_selects_core_yoy():
    evs = [
        _make_econ(id="pp1", title="Core PPI YoY", event_family="ppi",
                   signal_tier="major", actual=2.0, unit="%"),
        _make_econ(id="pp2", title="PPI YoY", event_family="ppi",
                   signal_tier="major", actual=2.5, unit="%"),
    ]
    result = group_economic_events_to_families(evs)
    card = result[0]
    assert card["lead_metric"] == "Core Ppi Yoy"
    assert card["actual"] == 2.0


def test_ppi_children_unchanged():
    evs = [
        _make_econ(id="pp1", title="PPI MoM", event_family="ppi", signal_tier="major"),
        _make_econ(id="pp2", title="Core PPI YoY", event_family="ppi", signal_tier="major"),
    ]
    result = group_economic_events_to_families(evs)
    card = result[0]
    assert len(card["children"]) == 2
    assert card["children"][0]["id"] == "pp1"
    assert card["children"][1]["id"] == "pp2"


def test_ppi_grouping_deterministic():
    evs = [
        _make_econ(id="pp1", title="PPI MoM", event_family="ppi", signal_tier="major"),
        _make_econ(id="pp2", title="Core PPI YoY", event_family="ppi", signal_tier="major"),
    ]
    result1 = group_economic_events_to_families(evs)
    result2 = group_economic_events_to_families(evs)
    assert result1[0]["lead_metric"] == result2[0]["lead_metric"]
    assert result1[0]["actual"] == result2[0]["actual"]


def test_ppi_no_source_mutation():
    evs = [
        _make_econ(id="pp1", title="PPI MoM", event_family="ppi", signal_tier="major"),
        _make_econ(id="pp2", title="Core PPI YoY", event_family="ppi", signal_tier="major"),
    ]
    before = [dict(ev) for ev in evs]
    group_economic_events_to_families(evs)
    for i, ev in enumerate(evs):
        assert ev == before[i]


# ═══════════════════════════════════════════════════════════════════════════════
# Release-package grouping tests
# ═══════════════════════════════════════════════════════════════════════════════

from services.calendar_curation import group_events_to_release_packages


# ── Employment Report ───────────────────────────────────────────────────────

def test_employment_report_groups_payroll_unemployment_and_earnings():
    evs = [
        _make_econ(id="e1", title="Nonfarm Payrolls", eventName="Nonfarm Payrolls",
                   event_family="payrolls", signal_tier="major",
                   signal_reason="Monthly payroll and labor-market release",
                   date="2026-05-01", time="08:30:00"),
        _make_econ(id="e2", title="Unemployment Rate", eventName="Unemployment Rate",
                   event_family="unemployment", signal_tier="secondary",
                   signal_reason="Unemployment rate release",
                   date="2026-05-01", time="08:30:00"),
        _make_econ(id="e3", title="Average Hourly Earnings MoM", eventName="Average Hourly Earnings MoM",
                   event_family="payrolls", signal_tier="major",
                   signal_reason="Monthly payroll and labor-market release",
                   date="2026-05-01", time="08:30:00"),
    ]
    result = group_events_to_release_packages(evs)
    cards = [e for e in result if e.get("release_group") == "employment_report"]
    assert len(cards) == 1
    card = cards[0]
    assert card["display_title"] == "Employment Report"
    assert card["event_count"] == 3
    assert len(card["children"]) == 3
    child_ids = {c["id"] for c in card["children"]}
    assert child_ids == {"e1", "e2", "e3"}


def test_adp_does_not_merge_into_employment_report():
    evs = [
        _make_econ(id="pay", title="Nonfarm Payrolls", eventName="Nonfarm Payrolls",
                   event_family="payrolls", signal_tier="major",
                   signal_reason="Monthly payroll release",
                   date="2026-05-01", time="08:30:00"),
        _make_econ(id="adp", title="ADP Employment Change", eventName="ADP Employment Change",
                   event_family="payrolls", signal_tier="secondary",
                   signal_reason="ADP national employment report",
                   date="2026-05-01", time="08:15:00"),
    ]
    result = group_events_to_release_packages(evs)
    cards = [e for e in result if e.get("release_group") == "employment_report"]
    assert len(cards) >= 1
    if cards:
        child_ids = {c["id"] for c in cards[0]["children"]}
        assert "adp" not in child_ids


def test_jobless_claims_group_as_one_report():
    evs = [
        _make_econ(id="jc1", title="Initial Jobless Claims", eventName="Initial Jobless Claims",
                   event_family="jobless_claims", signal_tier="secondary",
                   signal_reason="Weekly jobless claims",
                   date="2026-05-01", time="08:30:00"),
        _make_econ(id="jc2", title="Continuing Jobless Claims", eventName="Continuing Jobless Claims",
                   event_family="jobless_claims", signal_tier="secondary",
                   signal_reason="Weekly jobless claims",
                   date="2026-05-01", time="08:30:00"),
        _make_econ(id="jc3", title="Jobless Claims 4-Week Average",
                   eventName="Jobless Claims 4-Week Average",
                   event_family="jobless_claims", signal_tier="secondary",
                   signal_reason="Weekly jobless claims",
                   date="2026-05-01", time="08:30:00"),
    ]
    result = group_events_to_release_packages(evs)
    cards = [e for e in result if e.get("release_group") == "jobless_claims_report"]
    assert len(cards) == 1
    card = cards[0]
    assert card["display_title"] == "Jobless Claims Report"
    assert card["event_count"] == 3


def test_jolts_groups_into_one_report():
    evs = [
        _make_econ(id="jo1", title="JOLTs Job Openings", eventName="JOLTs Job Openings",
                   event_family="other_us", signal_tier="secondary",
                   signal_reason="JOLTS job openings report",
                   date="2026-05-01", time="10:00:00"),
        _make_econ(id="jo2", title="JOLTs Job Quits", eventName="JOLTs Job Quits",
                   event_family="other_us", signal_tier="secondary",
                   signal_reason="JOLTS job quits report",
                   date="2026-05-01", time="10:00:00"),
        _make_econ(id="jo3", title="JOLTs Hires", eventName="JOLTs Hires",
                   event_family="other_us", signal_tier="secondary",
                   signal_reason="JOLTS hires report",
                   date="2026-05-01", time="10:00:00"),
    ]
    result = group_events_to_release_packages(evs)
    cards = [e for e in result if e.get("release_group") == "jolts_report"]
    assert len(cards) == 1
    card = cards[0]
    assert card["display_title"] == "JOLTS Report"
    assert card["event_count"] == 3


def test_ism_manufacturing_groups_components():
    evs = [
        _make_econ(id="im1", title="ISM Manufacturing PMI", eventName="ISM Manufacturing PMI",
                   event_family="ism", signal_tier="secondary",
                   signal_reason="ISM business survey",
                   date="2026-05-01", time="10:00:00"),
        _make_econ(id="im2", title="ISM Manufacturing Prices", eventName="ISM Manufacturing Prices",
                   event_family="ism", signal_tier="secondary",
                   signal_reason="ISM business survey",
                   date="2026-05-01", time="10:00:00"),
        _make_econ(id="im3", title="ISM Manufacturing Employment",
                   eventName="ISM Manufacturing Employment",
                   event_family="ism", signal_tier="secondary",
                   signal_reason="ISM business survey",
                   date="2026-05-01", time="10:00:00"),
        _make_econ(id="im4", title="ISM Manufacturing New Orders",
                   eventName="ISM Manufacturing New Orders",
                   event_family="ism", signal_tier="secondary",
                   signal_reason="ISM business survey",
                   date="2026-05-01", time="10:00:00"),
    ]
    result = group_events_to_release_packages(evs)
    cards = [e for e in result if e.get("release_group") == "ism_manufacturing_report"]
    assert len(cards) == 1
    card = cards[0]
    assert card["display_title"] == "ISM Manufacturing Report"
    assert card["event_count"] == 4


def test_ism_services_groups_services_and_non_manufacturing_aliases():
    evs = [
        _make_econ(id="is1", title="ISM Services PMI", eventName="ISM Services PMI",
                   event_family="ism", signal_tier="secondary",
                   signal_reason="ISM business survey",
                   date="2026-05-01", time="10:00:00"),
        _make_econ(id="is2", title="ISM Non-Manufacturing PMI",
                   eventName="ISM Non-Manufacturing PMI",
                   event_family="ism", signal_tier="secondary",
                   signal_reason="ISM business survey",
                   date="2026-05-01", time="10:00:00"),
        _make_econ(id="is3", title="ISM Services Business Activity",
                   eventName="ISM Services Business Activity",
                   event_family="ism", signal_tier="secondary",
                   signal_reason="ISM business survey",
                   date="2026-05-01", time="10:00:00"),
        _make_econ(id="is4", title="ISM Non-Manufacturing Employment",
                   eventName="ISM Non-Manufacturing Employment",
                   event_family="ism", signal_tier="secondary",
                   signal_reason="ISM business survey",
                   date="2026-05-01", time="10:00:00"),
    ]
    result = group_events_to_release_packages(evs)
    cards = [e for e in result if e.get("release_group") == "ism_services_report"]
    assert len(cards) == 1
    card = cards[0]
    assert card["display_title"] == "ISM Services Report"
    assert card["event_count"] == 4


def test_factory_orders_groups_mom_and_ex_transportation():
    evs = [
        _make_econ(id="fo1", title="Factory Orders MoM", eventName="Factory Orders MoM",
                   event_family="other_us", signal_tier="secondary",
                   signal_reason="Factory orders release",
                   date="2026-05-01", time="10:00:00"),
        _make_econ(id="fo2", title="Factory Orders ex Transportation",
                   eventName="Factory Orders ex Transportation",
                   event_family="other_us", signal_tier="secondary",
                   signal_reason="Factory orders release",
                   date="2026-05-01", time="10:00:00"),
    ]
    result = group_events_to_release_packages(evs)
    cards = [e for e in result if e.get("release_group") == "factory_orders_report"]
    assert len(cards) == 1
    card = cards[0]
    assert card["display_title"] == "Factory Orders Report"
    assert card["event_count"] == 2


def test_different_dates_do_not_merge_release_packages():
    evs = [
        _make_econ(id="d1", title="ISM Manufacturing PMI", eventName="ISM Manufacturing PMI",
                   event_family="ism", signal_tier="secondary",
                   signal_reason="ISM business survey",
                   date="2026-05-01", time="10:00:00"),
        _make_econ(id="d2", title="ISM Manufacturing Prices", eventName="ISM Manufacturing Prices",
                   event_family="ism", signal_tier="secondary",
                   signal_reason="ISM business survey",
                   date="2026-05-02", time="10:00:00"),
    ]
    result = group_events_to_release_packages(evs)
    cards = [e for e in result if e.get("release_group") == "ism_manufacturing_report"]
    assert len(cards) == 2


def test_different_times_do_not_merge_release_packages():
    evs = [
        _make_econ(id="t1", title="ISM Manufacturing PMI", eventName="ISM Manufacturing PMI",
                   event_family="ism", signal_tier="secondary",
                   signal_reason="ISM business survey",
                   date="2026-05-01", time="10:00:00"),
        _make_econ(id="t2", title="ISM Manufacturing Prices", eventName="ISM Manufacturing Prices",
                   event_family="ism", signal_tier="secondary",
                   signal_reason="ISM business survey",
                   date="2026-05-01", time="14:00:00"),
    ]
    result = group_events_to_release_packages(evs)
    cards = [e for e in result if e.get("release_group") == "ism_manufacturing_report"]
    assert len(cards) == 2


def test_foreign_events_do_not_group_into_release_packages():
    evs = [
        _make_econ(id="f1", title="Nonfarm Payrolls", eventName="Nonfarm Payrolls",
                   event_family="payrolls", signal_tier="major",
                   signal_reason="Monthly payroll release",
                   country="DE", date="2026-05-01", time="08:30:00"),
        _make_econ(id="f2", title="Unemployment Rate", eventName="Unemployment Rate",
                   event_family="unemployment", signal_tier="secondary",
                   signal_reason="Unemployment rate release",
                   country="DE", date="2026-05-01", time="08:30:00"),
    ]
    result = group_events_to_release_packages(evs)
    cards = [e for e in result if e.get("release_group") == "employment_report"]
    assert len(cards) == 0


def test_lead_precedence_employment_report():
    evs = [
        _make_econ(id="l1", title="Unemployment Rate", eventName="Unemployment Rate",
                   event_family="unemployment", signal_tier="secondary",
                   signal_reason="Unemployment rate release",
                   actual=4.0, date="2026-05-01", time="08:30:00"),
        _make_econ(id="l2", title="Nonfarm Payrolls", eventName="Nonfarm Payrolls",
                   event_family="payrolls", signal_tier="major",
                   signal_reason="Monthly payroll and labor-market release",
                   actual=250, date="2026-05-01", time="08:30:00"),
    ]
    result = group_events_to_release_packages(evs)
    card = [c for c in result if c.get("release_group") == "employment_report"][0]
    assert "nonfarm" in card["lead_metric"].lower() or "non farm" in card["lead_metric"].lower()
    assert card["actual"] == 250


def test_children_preserve_all_source_events():
    evs = [
        _make_econ(id="c1", title="Nonfarm Payrolls", eventName="Nonfarm Payrolls",
                   event_family="payrolls", signal_tier="major",
                   signal_reason="r1", actual=250, date="2026-05-01", time="08:30:00"),
        _make_econ(id="c2", title="Unemployment Rate", eventName="Unemployment Rate",
                   event_family="unemployment", signal_tier="secondary",
                   signal_reason="r2", actual=4.0, date="2026-05-01", time="08:30:00"),
        _make_econ(id="c3", title="Average Hourly Earnings MoM",
                   eventName="Average Hourly Earnings MoM",
                   event_family="payrolls", signal_tier="major",
                   signal_reason="r3", actual=0.3, date="2026-05-01", time="08:30:00"),
    ]
    result = group_events_to_release_packages(evs)
    card = [c for c in result if c.get("release_group") == "employment_report"][0]
    assert len(card["children"]) == 3
    assert {c["id"] for c in card["children"]} == {"c1", "c2", "c3"}


def test_event_count_is_exact_release_package():
    evs = [
        _make_econ(id="ec1", title="ISM Manufacturing PMI", eventName="ISM Manufacturing PMI",
                   event_family="ism", signal_tier="secondary",
                   signal_reason="r", date="2026-05-01", time="10:00:00"),
        _make_econ(id="ec2", title="ISM Manufacturing Prices", eventName="ISM Manufacturing Prices",
                   event_family="ism", signal_tier="secondary",
                   signal_reason="r", date="2026-05-01", time="10:00:00"),
    ]
    result = group_events_to_release_packages(evs)
    card = [c for c in result if c.get("release_group") == "ism_manufacturing_report"][0]
    assert card["event_count"] == 2
    assert len(card["children"]) == 2


def test_source_events_not_mutated_release_package():
    evs = [
        _make_econ(id="m1", title="Nonfarm Payrolls", eventName="Nonfarm Payrolls",
                   event_family="payrolls", signal_tier="major",
                   signal_reason="r", date="2026-05-01", time="08:30:00"),
        _make_econ(id="m2", title="Unemployment Rate", eventName="Unemployment Rate",
                   event_family="unemployment", signal_tier="secondary",
                   signal_reason="r", date="2026-05-01", time="08:30:00"),
    ]
    before = [dict(ev) for ev in evs]
    group_events_to_release_packages(evs)
    for i, ev in enumerate(evs):
        assert ev == before[i]


def test_release_package_ids_are_stable():
    evs = [
        _make_econ(id="s1", title="Nonfarm Payrolls", eventName="Nonfarm Payrolls",
                   event_family="payrolls", signal_tier="major",
                   signal_reason="r", date="2026-05-01", time="08:30:00"),
        _make_econ(id="s2", title="Unemployment Rate", eventName="Unemployment Rate",
                   event_family="unemployment", signal_tier="secondary",
                   signal_reason="r", date="2026-05-01", time="08:30:00"),
    ]
    result1 = group_events_to_release_packages(evs)
    result2 = group_events_to_release_packages(evs)
    card1 = [c for c in result1 if c.get("release_group") == "employment_report"][0]
    card2 = [c for c in result2 if c.get("release_group") == "employment_report"][0]
    assert card1["id"] == card2["id"]


def test_family_cards_pass_through_unchanged():
    cpi_evs = [
        _make_econ(id="c1", title="CPI MoM", event_family="cpi", signal_tier="major",
                   signal_reason="r", date="2026-05-01"),
        _make_econ(id="c2", title="Core CPI MoM", event_family="cpi", signal_tier="major",
                   signal_reason="r", date="2026-05-01"),
    ]
    cpi_card = group_economic_events_to_families(cpi_evs)[0]
    assert cpi_card["type"] == "macro_family"

    payroll_ev = _make_econ(id="p1", title="Nonfarm Payrolls", event_family="payrolls",
                            signal_tier="major", signal_reason="r",
                            date="2026-05-01", time="08:30:00")
    unemp_ev = _make_econ(id="p2", title="Unemployment Rate", event_family="unemployment",
                          signal_tier="secondary", signal_reason="r",
                          date="2026-05-01", time="08:30:00")
    mixed = [cpi_card, payroll_ev, unemp_ev]
    result = group_events_to_release_packages(mixed)

    cpi_cards = [c for c in result if c.get("event_family") == "cpi"]
    assert len(cpi_cards) == 1
    assert cpi_cards[0] is cpi_card

    emp_cards = [c for c in result if c.get("release_group") == "employment_report"]
    assert len(emp_cards) == 1
    assert emp_cards[0]["event_count"] == 2


def test_curate_economic_logical_events_groups_ism_and_jolts():
    raw = [
        {"eventType": "economic_release", "eventName": "ISM Manufacturing PMI",
         "country": "US", "date": "2026-08-03", "importance": "medium",
         "signal_tier": "secondary", "signal_reason": "ISM business survey"},
        {"eventType": "economic_release", "eventName": "ISM Manufacturing New Orders",
         "country": "US", "date": "2026-08-03", "importance": "medium",
         "signal_tier": "secondary", "signal_reason": "ISM business survey"},
        {"eventType": "economic_release", "eventName": "JOLTs Job Openings",
         "country": "US", "date": "2026-08-04", "importance": "medium",
         "signal_tier": "secondary", "signal_reason": "JOLTS report"},
        {"eventType": "economic_release", "eventName": "JOLTs Job Quits",
         "country": "US", "date": "2026-08-04", "importance": "medium",
         "signal_tier": "secondary", "signal_reason": "JOLTS report"},
    ]
    out = curate_economic_logical_events(raw, cap=50)
    titles = [e.get("display_title") or e.get("title") or "" for e in out]
    assert "ISM Manufacturing Report" in titles
    assert "JOLTS Report" in titles
    ism = [e for e in out if e.get("release_group") == "ism_manufacturing_report"][0]
    jolts = [e for e in out if e.get("release_group") == "jolts_report"][0]
    assert ism["event_count"] == 2
    assert jolts["event_count"] == 2


def test_curate_economic_logical_events_preserves_signal_reason():
    raw = [
        {"eventType": "economic_release", "eventName": "CPI MoM",
         "country": "US", "date": "2026-08-05", "importance": "high",
         "event_family": "cpi", "signal_tier": "major",
         "signal_reason": "Major consumer inflation release"},
    ]
    out = curate_economic_logical_events(raw, cap=50)
    assert len(out) == 1
    assert out[0]["signal_tier"] == "major"
    assert out[0]["signal_reason"] == "Major consumer inflation release"


def test_curate_economic_logical_events_major_child_establishes_parent_tier():
    raw = [
        {"eventType": "economic_release", "eventName": "Non Farm Payrolls",
         "country": "US", "date": "2026-08-07", "importance": "high",
         "event_family": "payrolls", "signal_tier": "major",
         "signal_reason": "Monthly payroll release"},
        {"eventType": "economic_release", "eventName": "Unemployment Rate",
         "country": "US", "date": "2026-08-07", "importance": "medium",
         "event_family": "unemployment", "signal_tier": "secondary",
         "signal_reason": "Unemployment rate release"},
    ]
    out = curate_economic_logical_events(raw, cap=50)
    emp = [e for e in out if e.get("release_group") == "employment_report"][0]
    assert emp["signal_tier"] == "major"
    assert "Monthly payroll release" in emp["signal_reason"]


# ── Canonical macro-window tests ───────────────────────────────────────────

def _make_test_econ(**kw) -> dict:
    defaults = {
        "eventType": "economic_release",
        "country": "US",
        "date": "2026-08-05",
        "signal_tier": "major",
        "signal_reason": "Test release",
    }
    defaults.update(kw)
    return _build_event(**defaults)


def _make_test_treasury(**kw) -> dict:
    defaults = {
        "eventType": "treasury_rate",
        "country": "US",
        "date": "2026-08-05",
        "maturity": "10Y",
    }
    defaults.update(kw)
    return _build_event(**defaults)


def test_get_canonical_macro_window_groups_families(monkeypatch):
    """Economic releases are grouped into family cards."""
    from services import calendar_snapshot_service as _snap_svc
    snap = {
        "economic_releases": {
            "events": [
                _make_test_econ(eventName="CPI MoM", title="CPI MoM",
                                event_family="cpi"),
                _make_test_econ(eventName="Core CPI MoM", title="Core CPI MoM",
                                event_family="cpi"),
            ],
            "last_updated": "2026-08-02T10:00:00Z",
            "status": "ready",
            "horizon": {"horizon_start": "2026-08-01", "horizon_end": "2026-08-31"},
        },
        "treasury_macro": {"events": [], "last_updated": None, "status": "empty"},
    }

    def _fake_get_snapshot(tab: str):
        return snap.get(tab, {"events": [], "last_updated": None, "status": "empty"})

    monkeypatch.setattr(_snap_svc, "get_snapshot", _fake_get_snapshot)
    from services.calendar_curation import get_canonical_macro_window

    out = get_canonical_macro_window("2026-08-01", "2026-08-31")
    assert len(out["macro_logical_events"]) == 1
    assert out["macro_logical_events"][0]["event_family"] == "cpi"
    assert out["source_counts"]["economic_source"] == 2
    assert out["source_counts"]["economic_logical"] == 1
    assert out["coverage_complete"] is True


def test_get_canonical_macro_window_cross_source_dedupe(monkeypatch):
    """Treasury events that duplicate economic releases are dropped."""
    from services import calendar_snapshot_service as _snap_svc
    snap = {
        "economic_releases": {
            "events": [
                _make_test_econ(eventName="10-Year Treasury Auction",
                                title="10-Year Treasury Auction",
                                event_family="treasury_auction"),
            ],
            "last_updated": "2026-08-02T10:00:00Z",
            "status": "ready",
            "horizon": {"horizon_start": "2026-08-01", "horizon_end": "2026-08-31"},
        },
        "treasury_macro": {
            "events": [
                _make_test_treasury(indicatorName="10-Year Treasury Auction"),
            ],
            "last_updated": "2026-08-02T10:00:00Z",
            "status": "ready",
        },
    }

    def _fake_get_snapshot(tab: str):
        return snap.get(tab, {"events": [], "last_updated": None, "status": "empty"})

    monkeypatch.setattr(_snap_svc, "get_snapshot", _fake_get_snapshot)
    from services.calendar_curation import get_canonical_macro_window

    out = get_canonical_macro_window("2026-08-01", "2026-08-31")
    assert out["source_counts"]["economic_source"] == 1
    assert out["source_counts"]["treasury_source"] == 1
    assert out["source_counts"]["economic_logical"] == 1
    assert out["source_counts"]["treasury_logical"] == 0
    assert len(out["macro_logical_events"]) == 1


def test_get_canonical_macro_window_legacy_snapshot_incomplete(monkeypatch):
    """Legacy snapshots without broad events report incomplete coverage."""
    from services import calendar_snapshot_service as _snap_svc

    def _fake_get_snapshot(tab: str):
        if tab == "economic_releases":
            return {
                "current_week": [
                    _make_test_econ(eventName="CPI MoM", title="CPI MoM",
                                    event_family="cpi"),
                ],
                "previous_week": [],
                "events": [],
                "last_updated": "2026-08-02T10:00:00Z",
                "status": "ready",
            }
        return {"events": [], "last_updated": None, "status": "empty"}

    monkeypatch.setattr(_snap_svc, "get_snapshot", _fake_get_snapshot)
    from services.calendar_curation import get_canonical_macro_window

    out = get_canonical_macro_window("2026-08-01", "2026-08-31")
    assert out["coverage_complete"] is False
    assert len(out["macro_logical_events"]) == 1


def test_get_canonical_macro_window_uses_preloaded_envelope(monkeypatch):
    """A preloaded economic envelope is used without calling get_snapshot again."""
    from services import calendar_snapshot_service as _snap_svc
    from services.calendar_curation import get_canonical_macro_window

    calls: list[str] = []

    def _fake_get_snapshot(tab: str):
        calls.append(tab)
        return {"events": [], "last_updated": None, "status": "empty"}

    monkeypatch.setattr(_snap_svc, "get_snapshot", _fake_get_snapshot)

    envelope = {
        "events": [
            _make_test_econ(eventName="CPI MoM", title="CPI MoM",
                            event_family="cpi", date="2026-08-05"),
        ],
        "last_updated": "2026-08-02T10:00:00Z",
        "status": "ready",
        "horizon": {"horizon_start": "2026-08-01", "horizon_end": "2026-08-31"},
        "coverage_complete": True,
    }

    out = get_canonical_macro_window(
        "2026-08-01", "2026-08-31",
        include_treasury_context=False,
        economic_envelope=envelope,
    )
    assert calls == []  # no second snapshot read
    assert len(out["macro_logical_events"]) == 1
    assert out["macro_logical_events"][0]["event_family"] == "cpi"
    assert out["coverage_complete"] is True  # trusts envelope verdict


def test_get_canonical_macro_window_preloaded_coverage_false(monkeypatch):
    """A preloaded envelope with coverage_complete=False is trusted."""
    from services import calendar_snapshot_service as _snap_svc
    from services.calendar_curation import get_canonical_macro_window

    envelope = {
        "events": [],
        "last_updated": "2026-08-02T10:00:00Z",
        "status": "ready",
        "horizon": {"horizon_start": "2026-08-01", "horizon_end": "2026-08-31"},
        "coverage_complete": False,
    }

    monkeypatch.setattr(_snap_svc, "get_snapshot", lambda tab: {"events": []})

    out = get_canonical_macro_window(
        "2026-08-01", "2026-08-31",
        include_treasury_context=False,
        economic_envelope=envelope,
    )
    assert out["coverage_complete"] is False


def test_preloaded_empty_events_authoritative(monkeypatch):
    """Explicit empty `events` in preloaded envelope does not fall back."""
    from services import calendar_snapshot_service as _snap_svc
    from services.calendar_curation import get_canonical_macro_window

    monkeypatch.setattr(_snap_svc, "get_snapshot", lambda tab: {"events": []})

    envelope = {
        "events": [],
        "current_week": [
            _make_test_econ(eventName="CPI MoM", title="CPI MoM",
                            event_family="cpi", date="2026-07-29"),
        ],
        "last_updated": "2026-08-02T10:00:00Z",
        "status": "ready",
        "coverage_complete": True,
        "empty_reason": "no_events_in_window",
    }

    out = get_canonical_macro_window(
        "2026-08-01", "2026-08-31",
        include_treasury_context=False,
        economic_envelope=envelope,
    )
    assert out["macro_logical_events"] == []
    assert out["empty_reason"] == "no_events_in_window"
    assert out["coverage_complete"] is True


def test_preloaded_populated_events_authoritative(monkeypatch):
    """Preloaded `events` are used exactly, ignoring current_week."""
    from services import calendar_snapshot_service as _snap_svc
    from services.calendar_curation import get_canonical_macro_window

    monkeypatch.setattr(_snap_svc, "get_snapshot", lambda tab: {"events": []})

    envelope = {
        "events": [
            _make_test_econ(eventName="PPI MoM", title="PPI MoM",
                            event_family="ppi", date="2026-08-05"),
        ],
        "current_week": [
            _make_test_econ(eventName="CPI MoM", title="CPI MoM",
                            event_family="cpi", date="2026-08-06"),
        ],
        "last_updated": "2026-08-02T10:00:00Z",
        "status": "ready",
        "coverage_complete": True,
    }

    out = get_canonical_macro_window(
        "2026-08-01", "2026-08-31",
        include_treasury_context=False,
        economic_envelope=envelope,
    )
    assert len(out["macro_logical_events"]) == 1
    assert out["macro_logical_events"][0]["event_family"] == "ppi"


def test_legacy_envelope_without_events_uses_current_week(monkeypatch):
    """Non-preloaded legacy envelope without `events` falls back to current_week."""
    from services import calendar_snapshot_service as _snap_svc
    from services.calendar_curation import get_canonical_macro_window

    def _fake_get_snapshot(tab: str):
        if tab == "economic_releases":
            return {
                "current_week": [
                    _make_test_econ(eventName="CPI MoM", title="CPI MoM",
                                    event_family="cpi", date="2026-08-05"),
                ],
                "events": [],
                "last_updated": "2026-08-02T10:00:00Z",
                "status": "ready",
            }
        return {"events": [], "last_updated": None, "status": "empty"}

    monkeypatch.setattr(_snap_svc, "get_snapshot", _fake_get_snapshot)

    out = get_canonical_macro_window(
        "2026-08-01", "2026-08-31",
        include_treasury_context=False,
    )
    assert len(out["macro_logical_events"]) == 1
    assert out["macro_logical_events"][0]["event_family"] == "cpi"


def test_horizon_coverage_ranges_survive_canonical_output(monkeypatch):
    """coverage_ranges from envelope.horizon are preserved."""
    from services import calendar_snapshot_service as _snap_svc
    from services.calendar_curation import get_canonical_macro_window

    monkeypatch.setattr(_snap_svc, "get_snapshot", lambda tab: {"events": []})

    envelope = {
        "events": [],
        "horizon": {
            "coverage_ranges": [
                {"from": "2026-08-01", "to": "2026-08-31", "status": "complete"},
            ],
        },
        "coverage_complete": True,
        "empty_reason": "no_events_in_window",
        "last_updated": "2026-08-02T10:00:00Z",
        "status": "ready",
    }

    out = get_canonical_macro_window(
        "2026-08-01", "2026-08-31",
        include_treasury_context=False,
        economic_envelope=envelope,
    )
    assert len(out["coverage_ranges"]) == 1
    assert out["coverage_ranges"][0]["from"] == "2026-08-01"


def test_authoritative_actual_bounds_survive_canonical_output(monkeypatch):
    """actual_start/actual_end from the envelope are preserved, not derived."""
    from services import calendar_snapshot_service as _snap_svc
    from services.calendar_curation import get_canonical_macro_window

    monkeypatch.setattr(_snap_svc, "get_snapshot", lambda tab: {"events": []})

    envelope = {
        "events": [],
        "actual_start": "2026-07-18",
        "actual_end": "2026-10-29",
        "horizon": {
            "actual_start": "2026-07-20",
            "actual_end": "2026-10-25",
        },
        "coverage_complete": True,
        "empty_reason": "no_events_in_window",
        "last_updated": "2026-08-02T10:00:00Z",
        "status": "ready",
    }

    out = get_canonical_macro_window(
        "2026-08-01", "2026-08-31",
        include_treasury_context=False,
        economic_envelope=envelope,
    )
    assert out["actual_start"] == "2026-07-18"
    assert out["actual_end"] == "2026-10-29"


def test_empty_selected_window_keeps_actual_bounds(monkeypatch):
    """A covered empty window does not replace actual bounds with None."""
    from services import calendar_snapshot_service as _snap_svc
    from services.calendar_curation import get_canonical_macro_window

    monkeypatch.setattr(_snap_svc, "get_snapshot", lambda tab: {"events": []})

    envelope = {
        "events": [],
        "actual_start": "2026-07-18",
        "actual_end": "2026-10-29",
        "coverage_complete": True,
        "empty_reason": "no_events_in_window",
        "last_updated": "2026-08-02T10:00:00Z",
        "status": "ready",
    }

    out = get_canonical_macro_window(
        "2026-08-01", "2026-08-31",
        include_treasury_context=False,
        economic_envelope=envelope,
    )
    assert out["macro_logical_events"] == []
    assert out["actual_start"] == "2026-07-18"
    assert out["actual_end"] == "2026-10-29"


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
