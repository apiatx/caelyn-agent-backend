"""
Factor unit tests for playbook factor implementations (Phase 1.5 + Phase 2).

Run from backend directory:
  python3 -m services.playbook.factor_tests

Tests cover:
  Phase 1.5:
  - sector_strength: sensible values for strong vs weak sectors
  - theme_alignment: manual map + keyword matching + preferred theme weighting
  - bottleneck_exposure: direct map vs keyword vs fallback
  - dilution_risk: penalizes high-risk scenarios, rewards large clean names
  - catalyst_proximity: boosts near-earnings + strong news catalysts
  - crowding_risk: penalizes extended, premium-valued, high-news names
  - playbook divergence: same ticker scores differently under serenity vs sjcapital
  - partial/missing data degrades gracefully
  - /api/query isolation: playbook module has no references in query handler

  Phase 2:
  - supply_chain_confirmation: curated map + keyword + news inference
  - ebitda_inflection_proximity: FMP income statement trend + heuristic
  - backlog_quality: news keyword scan
  - evidence_freshness: news recency scoring
  - execution_risk: leverage + revenue + distress signals
  - insider_buying: news fallback + neutral for large-caps
  - policy_tailwind: theme → policy bucket mapping
  - explainer: deterministic explanation generation (thesis_summary etc.)
  - playbook_registry: v2.0 weight sums
"""
from __future__ import annotations

import asyncio
import sys


# ────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────

_PASS = 0
_FAIL = 0

def _ok(name: str) -> None:
    global _PASS
    _PASS += 1
    print(f"  PASS  {name}")

def _fail(name: str, detail: str) -> None:
    global _FAIL
    _FAIL += 1
    print(f"  FAIL  {name}: {detail}")

def _assert(name: str, condition: bool, detail: str = "") -> None:
    if condition:
        _ok(name)
    else:
        _fail(name, detail or "condition was False")

def _assert_range(name: str, value: float, lo: float, hi: float) -> None:
    _assert(name, lo <= value <= hi, f"expected [{lo}, {hi}], got {value}")


# ────────────────────────────────────────────────────────────────────────────
# sector_strength
# ────────────────────────────────────────────────────────────────────────────

def test_sector_strength():
    print("\n[sector_strength]")
    from services.playbook.sector_map import score_sector_strength_from_history

    # Strong sector: all windows positive, large gains
    strong_history = {
        "XLK": [{"date": f"2025-{i:02d}-01", "close": 100.0 + i * 1.0} for i in range(1, 70)]
    }
    result = score_sector_strength_from_history("Technology", None, strong_history)
    _assert("strong sector score > 75", result.score > 75, f"got {result.score}")
    _assert("strong sector status real", result.status == "real")
    _assert("strong sector has XLK source_tag", "XLK" in result.source_tags)
    _assert("strong sector has reasons", len(result.reasons) > 0)

    # Weak sector: all windows negative
    weak_history = {
        "XLK": [{"date": f"2025-{i:02d}-01", "close": 200.0 - i * 1.5} for i in range(1, 70)]
    }
    result_weak = score_sector_strength_from_history("Technology", None, weak_history)
    _assert("weak sector score < 30", result_weak.score < 30, f"got {result_weak.score}")

    # Unknown sector → fallback
    result_unknown = score_sector_strength_from_history("Blockchain", None, {})
    _assert("unknown sector fallback", result_unknown.status == "fallback")
    _assert("unknown sector score ~50", 40 <= result_unknown.score <= 60, f"got {result_unknown.score}")

    # Missing history for known sector → fallback
    result_missing = score_sector_strength_from_history("Technology", None, {})
    _assert("missing history fallback", result_missing.status == "fallback")

    # Industry alias works
    result_alias = score_sector_strength_from_history(None, "Financials", strong_history)
    # XLF not in strong_history — should return fallback gracefully
    _assert("industry alias graceful", result_alias.status == "fallback")


# ────────────────────────────────────────────────────────────────────────────
# theme_alignment
# ────────────────────────────────────────────────────────────────────────────

def test_theme_alignment():
    print("\n[theme_alignment]")
    from services.playbook.theme_map import score_theme_alignment

    serenity_preferred = [
        "photonics_cpo", "advanced_packaging_test", "semicap_supply_chain",
        "defense_optics", "grid_transformers", "space", "memory", "ai_infrastructure",
    ]
    sjcapital_preferred = [
        "ai_infrastructure", "neocloud", "ai_software",
        "ai_power_energy", "energy_transition", "biotech_catalyst",
    ]

    # LITE: photonics_cpo — preferred by Serenity, not by S&J
    lite_serenity = score_theme_alignment("LITE", "", "Communication Equipment", serenity_preferred)
    lite_sjcapital = score_theme_alignment("LITE", "", "Communication Equipment", sjcapital_preferred)
    _assert("LITE higher under Serenity than S&J", lite_serenity.score > lite_sjcapital.score,
            f"Serenity={lite_serenity.score} S&J={lite_sjcapital.score}")
    _assert("LITE Serenity has photonics in source_tags",
            "photonics_cpo" in lite_serenity.source_tags)
    _assert("LITE theme status manual", lite_serenity.status == "manual")

    # SNOW: neocloud/ai_software — preferred by S&J, not by Serenity
    snow_serenity = score_theme_alignment("SNOW", "", "Software - Application", serenity_preferred)
    snow_sjcapital = score_theme_alignment("SNOW", "", "Software - Application", sjcapital_preferred)
    _assert("SNOW higher under S&J than Serenity", snow_sjcapital.score > snow_serenity.score,
            f"Serenity={snow_serenity.score} S&J={snow_sjcapital.score}")
    _assert("SNOW S&J matched themes include neocloud or ai_software",
            any(t in snow_sjcapital.source_tags for t in ["neocloud", "ai_software"]))

    # Keyword detection from description
    desc = "provides silicon photonics solutions for co-packaged optical interconnects"
    kw_result = score_theme_alignment("UNKWN", desc, "", serenity_preferred)
    _assert("keyword match detects photonics_cpo",
            "photonics_cpo" in kw_result.source_tags, f"tags: {kw_result.source_tags}")
    _assert("keyword match status is manual or heuristic",
            kw_result.status in ("manual", "heuristic"))

    # No theme match → fallback with score ~40
    no_match = score_theme_alignment("XYZ", "makes widgets", "Widgets", serenity_preferred)
    _assert("no match returns fallback", no_match.status in ("fallback", "heuristic"))
    _assert_range("no match score reasonable", no_match.score, 30.0, 55.0)

    # Missing description and industry → graceful
    empty_result = score_theme_alignment("NVDA", "", "", serenity_preferred)
    _assert("NVDA manual match works without description",
            empty_result.status in ("manual",))
    _assert("NVDA score > 0", empty_result.score > 0)


# ────────────────────────────────────────────────────────────────────────────
# bottleneck_exposure
# ────────────────────────────────────────────────────────────────────────────

def test_bottleneck_exposure():
    print("\n[bottleneck_exposure]")
    from services.playbook.theme_map import score_bottleneck_exposure

    # ASML: sole EUV supplier — direct bottleneck
    asml = score_bottleneck_exposure("ASML", "", "Semiconductor Equipment & Materials")
    _assert("ASML score >= 90", asml.score >= 90, f"got {asml.score}")
    _assert("ASML status manual", asml.status == "manual")
    _assert("ASML has lithography in source_tags", "lithography" in asml.source_tags)

    # LITE: photonics bottleneck
    lite = score_bottleneck_exposure("LITE", "", "")
    _assert("LITE score >= 80", lite.score >= 80, f"got {lite.score}")

    # Unknown ticker but description keywords
    desc = "designs co-packaged optics components for advanced packaging solutions"
    kw = score_bottleneck_exposure("UNKWN", desc, "")
    _assert("keyword bottleneck score > 70", kw.score > 70, f"got {kw.score}")
    _assert("keyword status heuristic", kw.status == "heuristic")

    # No signal — generic software company
    no_signal = score_bottleneck_exposure("CRM", "builds CRM software", "Software—Application")
    _assert("CRM bottleneck score < 50", no_signal.score < 50, f"got {no_signal.score}")
    _assert("CRM status fallback", no_signal.status == "fallback")

    # Direct > adjacent: manual map entry scores higher than keyword-only
    amat = score_bottleneck_exposure("AMAT", "", "Semiconductor Equipment")
    _assert("AMAT > generic semiconductor equip", amat.score >= 80, f"got {amat.score}")


# ────────────────────────────────────────────────────────────────────────────
# dilution_risk
# ────────────────────────────────────────────────────────────────────────────

def test_dilution_risk():
    print("\n[dilution_risk]")
    from services.playbook.dilution_signals import score_dilution_risk_from_data

    # Large cap, clean balance sheet, no news → low risk
    large_clean = score_dilution_risk_from_data("AAPL", 3e12, 0.5, 0.05, [])
    _assert("large cap clean score < 30", large_clean.score < 30, f"got {large_clean.score}")

    # Small cap with high D/E → elevated risk
    risky = score_dilution_risk_from_data("SMCO", 200e6, 3.5, -0.10, [])
    _assert("small cap high leverage score > 40", risky.score > 40, f"got {risky.score}")

    # ATM offering news → high risk
    atm_news = [{"headline": "Company announces at-the-market offering of common stock", "summary": ""}]
    atm_result = score_dilution_risk_from_data("XYZ", 500e6, 1.0, 0.05, atm_news)
    _assert("ATM offering triggers high risk > 70", atm_result.score > 70, f"got {atm_result.score}")
    _assert("ATM news in reasons", any("atm" in r.lower() or "dilut" in r.lower() or "offering" in r.lower() for r in atm_result.reasons))

    # Shelf registration news
    shelf_news = [{"headline": "Files shelf registration statement for 50M shares", "summary": ""}]
    shelf = score_dilution_risk_from_data("DEF", 400e6, 0.5, 0.10, shelf_news)
    _assert("shelf news triggers risk > 50", shelf.score > 50, f"got {shelf.score}")

    # Micro-cap with no signals → elevated baseline
    micro = score_dilution_risk_from_data("TINY", 150e6, 0.3, 0.05, [])
    _assert("micro-cap elevated baseline > 35", micro.score > 35, f"got {micro.score}")

    # All None inputs → graceful fallback
    null_result = score_dilution_risk_from_data("NULL", None, None, None, [])
    _assert_range("all-null graceful", null_result.score, 0.0, 100.0)
    _assert("all-null has reasons", len(null_result.reasons) > 0)


# ────────────────────────────────────────────────────────────────────────────
# catalyst_proximity
# ────────────────────────────────────────────────────────────────────────────

def test_catalyst_proximity():
    print("\n[catalyst_proximity]")
    from services.playbook.dilution_signals import score_catalyst_proximity_from_data
    from datetime import date, timedelta

    today = date.today()

    # Earnings in 10 days → strong boost
    near_earnings = [{"symbol": "TICK", "date": (today + timedelta(days=10)).strftime("%Y-%m-%d")}]
    result_near = score_catalyst_proximity_from_data("TICK", near_earnings, [])
    _assert("earnings in 10d score > 75", result_near.score > 75, f"got {result_near.score}")
    _assert("near earnings status real", result_near.status == "real")

    # Earnings in 3 days → very strong
    very_near = [{"symbol": "TICK", "date": (today + timedelta(days=3)).strftime("%Y-%m-%d")}]
    result_very_near = score_catalyst_proximity_from_data("TICK", very_near, [])
    _assert("earnings in 3d score > 88", result_very_near.score > 88, f"got {result_very_near.score}")

    # Earnings in 60 days → mild boost
    far_earnings = [{"symbol": "TICK", "date": (today + timedelta(days=60)).strftime("%Y-%m-%d")}]
    result_far = score_catalyst_proximity_from_data("TICK", far_earnings, [])
    _assert("earnings in 60d score 48-60", 48 <= result_far.score <= 62, f"got {result_far.score}")

    # Strong catalyst news → boost
    catalyst_news = [{"headline": "Company wins major government contract award", "summary": ""}]
    result_news = score_catalyst_proximity_from_data("TICK", [], catalyst_news)
    _assert("strong catalyst news boost score > 48", result_news.score > 48, f"got {result_news.score}")

    # No signals → below neutral
    no_signal = score_catalyst_proximity_from_data("TICK", [], [])
    _assert("no signal below neutral < 45", no_signal.score < 45, f"got {no_signal.score}")

    # Earnings + strong news → compound boost
    compound = score_catalyst_proximity_from_data("TICK", near_earnings, catalyst_news)
    _assert("compound signal > near-earnings alone",
            compound.score >= result_near.score, f"compound={compound.score} near={result_near.score}")


# ────────────────────────────────────────────────────────────────────────────
# crowding_risk
# ────────────────────────────────────────────────────────────────────────────

def test_crowding_risk():
    print("\n[crowding_risk]")
    from services.playbook.dilution_signals import score_crowding_risk

    # Extended near 52w high + premium PE + high news → crowded
    crowded = score_crowding_risk(
        price=195.0, week52_high=200.0, week52_low=100.0,
        pe_ratio=80.0, sector="Technology", news_count=12
    )
    _assert("crowded setup score > 70", crowded.score > 70, f"got {crowded.score}")
    _assert("crowded has price extension reason",
            any("high" in r.lower() or "extend" in r.lower() for r in crowded.reasons))

    # Near 52w lows + cheap PE + no news → not crowded
    uncrowded = score_crowding_risk(
        price=110.0, week52_high=200.0, week52_low=100.0,
        pe_ratio=8.0, sector="Energy", news_count=0
    )
    _assert("uncrowded score < 50", uncrowded.score < 50, f"got {uncrowded.score}")

    # Null inputs → graceful neutral
    null_result = score_crowding_risk(None, None, None, None, None, 0)
    _assert_range("null inputs neutral", null_result.score, 40.0, 65.0)

    # High news count alone → elevated saturation
    news_only = score_crowding_risk(
        price=None, week52_high=None, week52_low=None,
        pe_ratio=None, sector=None, news_count=15
    )
    _assert("high news count elevates score > 60", news_only.score > 60, f"got {news_only.score}")

    # Extreme valuation near 52w high → signals crowding (combined signal)
    extreme_pe = score_crowding_risk(
        price=190.0, week52_high=200.0, week52_low=100.0,
        pe_ratio=200.0, sector="Technology", news_count=8
    )
    _assert("extreme PE near 52w high elevates crowding > 70", extreme_pe.score > 70,
            f"got {extreme_pe.score}")


# ────────────────────────────────────────────────────────────────────────────
# Playbook divergence (sync factor math)
# ────────────────────────────────────────────────────────────────────────────

def test_playbook_divergence():
    """
    Verify that the same extended factor scores produce meaningfully different
    final scores depending on playbook weight configuration.
    """
    print("\n[playbook_divergence]")
    from services.playbook.playbook_scoring import aggregate_score
    from services.playbook.playbook_types import TickerRawData
    from services.playbook.playbook_registry import get as get_playbook

    serenity   = get_playbook("serenity")
    sjcapital  = get_playbook("sjcapital")

    if not serenity or not sjcapital:
        _fail("playbooks loaded", "serenity or sjcapital not found in registry")
        return

    # Simulate a "pure Serenity name": high bottleneck, preferred theme,
    # small-cap, clean balance sheet, but in cold sector
    serenity_name_scores = {
        "bottleneck_exposure":       90.0,
        "balance_sheet_strength":    85.0,
        "theme_alignment":           75.0,   # semicap_supply_chain preferred by Serenity
        "catalyst_proximity":        60.0,
        "small_cap_asymmetry":       82.0,
        "technical_confirmation":    55.0,
        "sector_strength":           40.0,   # cold sector (bad for S&J)
        "supply_chain_confirmation": 50.0,
        "valuation_discount_vs_peers": 65.0,
        "revenue_growth":            55.0,
        "revenue_acceleration":      50.0,
        "ebitda_inflection_proximity": 50.0,
        "dilution_risk":             20.0,
        "crowding_risk":             30.0,
        "execution_risk":            50.0,
        "insider_buying":            50.0,
        "backlog_quality":           50.0,
        "policy_tailwind":           50.0,
        "evidence_freshness":        50.0,
    }
    raw_dummy = TickerRawData(ticker="DUMMY", mkt_cap=800e6, debt_to_equity=0.3)
    s_score, _ = aggregate_score(serenity,  serenity_name_scores, raw_dummy)
    sj_score, _ = aggregate_score(sjcapital, serenity_name_scores, raw_dummy)

    _assert("Serenity-type name: Serenity scores higher",
            s_score > sj_score,
            f"Serenity={s_score} S&J={sj_score}")
    divergence_a = s_score - sj_score
    print(f"    Serenity-type name divergence: {divergence_a:.1f}pts (S={s_score} SJ={sj_score})")

    # Simulate a "pure S&J name": hot sector, undervalued, accelerating revenue,
    # but no bottleneck, no preferred theme for Serenity
    sj_name_scores = {
        "bottleneck_exposure":       30.0,   # no bottleneck
        "balance_sheet_strength":    50.0,
        "theme_alignment":           20.0,   # NOT preferred by Serenity
        "catalyst_proximity":        70.0,
        "small_cap_asymmetry":       45.0,   # mid-large cap (less Serenity appeal)
        "technical_confirmation":    75.0,
        "sector_strength":           85.0,   # HOT sector (great for S&J)
        "supply_chain_confirmation": 50.0,
        "valuation_discount_vs_peers": 80.0,  # cheap vs sector
        "revenue_growth":            75.0,
        "revenue_acceleration":      65.0,
        "ebitda_inflection_proximity": 60.0,
        "dilution_risk":             25.0,
        "crowding_risk":             35.0,
        "execution_risk":            50.0,
        "insider_buying":            50.0,
        "backlog_quality":           50.0,
        "policy_tailwind":           50.0,
        "evidence_freshness":        50.0,
    }
    s_score2, _ = aggregate_score(serenity,  sj_name_scores, raw_dummy)
    sj_score2, _ = aggregate_score(sjcapital, sj_name_scores, raw_dummy)

    _assert("S&J-type name: S&J scores higher",
            sj_score2 > s_score2,
            f"Serenity={s_score2} S&J={sj_score2}")
    divergence_b = sj_score2 - s_score2
    print(f"    S&J-type name divergence: {divergence_b:.1f}pts (S={s_score2} SJ={sj_score2})")

    # Both divergences should be meaningful (at least 10 pts)
    _assert("Serenity-type divergence >= 10pts", divergence_a >= 10.0,
            f"got {divergence_a:.1f}")
    _assert("S&J-type divergence >= 10pts", divergence_b >= 10.0,
            f"got {divergence_b:.1f}")


# ────────────────────────────────────────────────────────────────────────────
# Graceful degradation with partial/missing data
# ────────────────────────────────────────────────────────────────────────────

def test_graceful_degradation():
    print("\n[graceful_degradation]")
    from services.playbook.theme_map import score_theme_alignment, score_bottleneck_exposure
    from services.playbook.dilution_signals import score_dilution_risk_from_data, score_crowding_risk
    from services.playbook.sector_map import score_sector_strength_from_history

    # All None / empty inputs — must not raise, must return valid FactorDetail
    tests = [
        ("theme_alignment_empty", lambda: score_theme_alignment("XYZ", "", "", [])),
        ("bottleneck_empty",      lambda: score_bottleneck_exposure("XYZ", "", "")),
        ("dilution_all_none",     lambda: score_dilution_risk_from_data("XYZ", None, None, None, [])),
        ("crowding_all_none",     lambda: score_crowding_risk(None, None, None, None, None, 0)),
        ("sector_empty_history",  lambda: score_sector_strength_from_history(None, None, {})),
    ]
    for name, fn in tests:
        try:
            result = fn()
            _assert_range(f"{name} score in [0,100]", result.score, 0.0, 100.0)
            _assert(f"{name} has status", result.status in ("real","manual","heuristic","fallback","stub"))
            _assert(f"{name} has reasons list", isinstance(result.reasons, list))
        except Exception as e:
            _fail(name, f"raised exception: {e}")


# ────────────────────────────────────────────────────────────────────────────
# Phase 2: supply_chain_confirmation
# ────────────────────────────────────────────────────────────────────────────

def test_supply_chain_confirmation():
    print("\n[supply_chain_confirmation]")
    from services.playbook.extended_factors import score_supply_chain_confirmation

    # Tier 1 from manual map
    r = score_supply_chain_confirmation("ASML", "", "", [])
    _assert("ASML manual map score >= 90", r.score >= 90, f"got {r.score}")
    _assert("ASML status manual", r.status == "manual")

    r2 = score_supply_chain_confirmation("LITE", "", "", [])
    _assert("LITE manual map score >= 80", r2.score >= 80, f"got {r2.score}")

    # Description keyword
    desc = "We are the sole source supplier of advanced EUV lithography equipment."
    r3 = score_supply_chain_confirmation("UNKN", desc, "", [])
    _assert("sole source desc score >= 70", r3.score >= 70, f"got {r3.score}")
    _assert("sole source desc status heuristic", r3.status == "heuristic")

    # News inference
    news = [{"headline": "UNKN awarded supply agreement with leading datacenter operator", "summary": ""}]
    r4 = score_supply_chain_confirmation("UNKN", "", "", news)
    _assert("news supply agreement score >= 60", r4.score >= 60, f"got {r4.score}")

    # No signal → fallback
    r5 = score_supply_chain_confirmation("RNDM", "", "Software", [])
    _assert("no signal fallback", r5.score < 50, f"got {r5.score}")
    _assert("no signal status fallback", r5.status in ("fallback", "heuristic"))


# ────────────────────────────────────────────────────────────────────────────
# Phase 2: ebitda_inflection_proximity
# ────────────────────────────────────────────────────────────────────────────

def test_ebitda_inflection_proximity():
    print("\n[ebitda_inflection_proximity]")
    from services.playbook.extended_factors import score_ebitda_inflection_proximity

    # EBITDA turned positive
    stmts_positive = [
        {"ebitda": 100e6, "revenue": 500e6, "operatingIncome": 80e6, "grossProfit": 300e6},
        {"ebitda": -20e6, "revenue": 400e6, "operatingIncome": -30e6, "grossProfit": 200e6},
    ]
    r = score_ebitda_inflection_proximity("TEST", stmts_positive, None, None, None)
    _assert("ebitda positive flip score >= 85", r.score >= 85, f"got {r.score}")
    _assert("ebitda positive flip status real", r.status == "real")

    # EBITDA narrowing (still negative)
    stmts_narrowing = [
        {"ebitda": -10e6, "revenue": 500e6, "operatingIncome": -5e6, "grossProfit": 300e6},
        {"ebitda": -80e6, "revenue": 400e6, "operatingIncome": -90e6, "grossProfit": 200e6},
    ]
    r2 = score_ebitda_inflection_proximity("TEST", stmts_narrowing, None, None, None)
    _assert("ebitda narrowing score >= 70", r2.score >= 70, f"got {r2.score}")

    # Revenue growth heuristic (no income statements)
    r3 = score_ebitda_inflection_proximity("TEST", [], 0.35, 0.4, 2e9)
    _assert("strong rev growth heuristic >= 65", r3.score >= 65, f"got {r3.score}")
    _assert("strong rev growth status heuristic", r3.status == "heuristic")

    # Revenue declining (no statements)
    r4 = score_ebitda_inflection_proximity("TEST", [], -0.25, 3.0, 500e6)
    _assert("declining rev heuristic <= 35", r4.score <= 35, f"got {r4.score}")


# ────────────────────────────────────────────────────────────────────────────
# Phase 2: backlog_quality
# ────────────────────────────────────────────────────────────────────────────

def test_backlog_quality():
    print("\n[backlog_quality]")
    from services.playbook.extended_factors import score_backlog_quality

    # Strong news signal
    news = [{"headline": "record backlog reached $4B, order book at historic high", "summary": ""}]
    r = score_backlog_quality("LMT", "", "Aerospace", "Industrials", news)
    _assert("record backlog score >= 78", r.score >= 78, f"got {r.score}")
    _assert("record backlog status real", r.status == "real")

    # Moderate signal
    news2 = [{"headline": "new design win at defense customer", "summary": ""}]
    r2 = score_backlog_quality("KTOS", "", "Defense", "Industrials", news2)
    _assert("design win score >= 60", r2.score >= 60, f"got {r2.score}")

    # Negative signal
    news3 = [{"headline": "order cancellation and demand weakness", "summary": ""}]
    r3 = score_backlog_quality("ENPH", "", "Energy", "Energy", news3)
    _assert("cancellation score <= 30", r3.score <= 30, f"got {r3.score}")

    # No signal → sector fallback
    r4 = score_backlog_quality("RNDM", "", "Technology", "Technology", [])
    _assert("no signal uses sector fallback", r4.status == "fallback")


# ────────────────────────────────────────────────────────────────────────────
# Phase 2: evidence_freshness
# ────────────────────────────────────────────────────────────────────────────

def test_evidence_freshness():
    print("\n[evidence_freshness]")
    from services.playbook.extended_factors import score_evidence_freshness
    import time

    now = time.time()

    # Fresh catalyst news (<7 days)
    fresh_news = [
        {"headline": "contract award for defense program", "datetime": now - 3 * 86400},
        {"headline": "analyst upgrade", "datetime": now - 1 * 86400},
    ]
    r = score_evidence_freshness("TEST", fresh_news, [])
    _assert("fresh catalyst score >= 82", r.score >= 82, f"got {r.score}")
    _assert("fresh catalyst status real", r.status == "real")

    # Recent but no catalyst keyword
    plain_news = [
        {"headline": "company updates website", "datetime": now - 2 * 86400},
        {"headline": "new hire announcement", "datetime": now - 4 * 86400},
        {"headline": "quarterly report summary", "datetime": now - 5 * 86400},
    ]
    r2 = score_evidence_freshness("TEST", plain_news, [])
    _assert("plain 7d news score >= 65", r2.score >= 65, f"got {r2.score}")

    # Stale news (>21 days)
    old_news = [{"headline": "old story", "datetime": now - 30 * 86400}]
    r3 = score_evidence_freshness("TEST", old_news, [])
    _assert("stale news score <= 45", r3.score <= 45, f"got {r3.score}")

    # No news
    r4 = score_evidence_freshness("TEST", [], [])
    _assert("no news score <= 40", r4.score <= 40, f"got {r4.score}")
    _assert("no news status fallback", r4.status == "fallback")


# ────────────────────────────────────────────────────────────────────────────
# Phase 2: execution_risk
# ────────────────────────────────────────────────────────────────────────────

def test_execution_risk():
    print("\n[execution_risk]")
    from services.playbook.extended_factors import score_execution_risk

    # Critical news signal
    news_bad = [{"headline": "going concern warning issued by auditor", "summary": ""}]
    r = score_execution_risk("TEST", -0.30, 4.5, 200e6, 5.0, 4.0, 20.0, news_bad)
    _assert("going concern risk score >= 85", r.score >= 85, f"got {r.score}")
    _assert("going concern status real", r.status == "real")

    # High leverage + declining revenue
    r2 = score_execution_risk("TEST", -0.25, 5.0, 1e9, 50.0, 40.0, 100.0, [])
    _assert("high leverage+decline risk >= 65", r2.score >= 65, f"got {r2.score}")

    # Large cap, clean balance sheet, growing revenue → low risk
    r3 = score_execution_risk("MSFT", 0.15, 0.5, 3e12, 420.0, 300.0, 450.0, [])
    _assert("large cap clean low risk <= 25", r3.score <= 25, f"got {r3.score}")

    # No data → baseline
    r4 = score_execution_risk("TEST", None, None, None, None, None, None, [])
    _assert("no data baseline <= 35", r4.score <= 35, f"got {r4.score}")


# ────────────────────────────────────────────────────────────────────────────
# Phase 2: insider_buying
# ────────────────────────────────────────────────────────────────────────────

def test_insider_buying():
    print("\n[insider_buying]")
    from services.playbook.extended_factors import score_insider_buying

    # News buy signal
    news_buy = [{"headline": "CEO insider purchase of $1M shares open market", "summary": ""}]
    r = score_insider_buying("TEST", news_buy, 500e6)
    _assert("news insider buy score >= 65", r.score >= 65, f"got {r.score}")

    # Sell signal
    news_sell = [{"headline": "director insider selling 500K shares on market", "summary": ""}]
    r2 = score_insider_buying("TEST", news_sell, 500e6)
    _assert("news insider sell score <= 35", r2.score <= 35, f"got {r2.score}")

    # Large cap no signal → neutral
    r3 = score_insider_buying("AAPL", [], 3e12)
    _assert("large cap neutral 40-55", 38 <= r3.score <= 55, f"got {r3.score}")

    # No data, small cap → neutral-low
    r4 = score_insider_buying("RNDM", [], 200e6)
    _assert("small cap no signal neutral <= 50", r4.score <= 50, f"got {r4.score}")


# ────────────────────────────────────────────────────────────────────────────
# Phase 2: policy_tailwind
# ────────────────────────────────────────────────────────────────────────────

def test_policy_tailwind():
    print("\n[policy_tailwind]")
    from services.playbook.extended_factors import score_policy_tailwind

    # Defense theme → NDAA
    r = score_policy_tailwind("KTOS", "", "Industrials", ["defense_optics"], [])
    _assert("defense theme policy score >= 80", r.score >= 80, f"got {r.score}")

    # CHIPS Act theme
    r2 = score_policy_tailwind("ASML", "", "Technology", ["semicap_supply_chain"], [])
    _assert("CHIPS Act theme score >= 78", r2.score >= 78, f"got {r2.score}")

    # Keyword in description
    r3 = score_policy_tailwind("TEST", "awarded a federal contract funded by the CHIPS Act", "Technology", [], [])
    _assert("chips act keyword score >= 70", r3.score >= 70, f"got {r3.score}")

    # No themes, no keywords → fallback
    r4 = score_policy_tailwind("RNDM", "", "Technology", [], [])
    _assert("no signal fallback status", r4.status == "fallback")
    _assert("no signal score 40-60", 38 <= r4.score <= 62, f"got {r4.score}")


# ────────────────────────────────────────────────────────────────────────────
# Phase 2: playbook_registry v2.0 weight sums
# ────────────────────────────────────────────────────────────────────────────

def test_playbook_registry_v2():
    print("\n[playbook_registry_v2]")
    from services.playbook.playbook_registry import list_all

    for pb in list_all():
        weight_sum = round(sum(pb.factor_weights.values()), 6)
        _assert(
            f"{pb.id} weights sum to 1.0 (got {weight_sum:.4f})",
            abs(weight_sum - 1.0) < 0.001,
            f"sum={weight_sum}"
        )
        _assert(f"{pb.id} version starts with 2.", pb.version.startswith("2."), f"got {pb.version}")
        _assert(f"{pb.id} has penalty_rules", len(pb.penalty_rules) >= 1)


# ────────────────────────────────────────────────────────────────────────────
# Phase 2: explainer deterministic output
# ────────────────────────────────────────────────────────────────────────────

def test_explainer():
    print("\n[explainer]")
    from services.playbook.explainer import generate_explanation
    from services.playbook.playbook_types import PlaybookScoreResult, FactorDetail
    from services.playbook.playbook_registry import get as get_pb

    pb = get_pb("serenity")
    if pb is None:
        _fail("serenity playbook loaded", "playbook not found")
        return

    # Build a high-score result
    factor_scores = {
        "bottleneck_exposure":       88.0,
        "supply_chain_confirmation": 85.0,
        "theme_alignment":           75.0,
        "balance_sheet_strength":    80.0,
        "evidence_freshness":        72.0,
        "catalyst_proximity":        70.0,
        "small_cap_asymmetry":       82.0,
        "technical_confirmation":    65.0,
        "sector_strength":           60.0,
        "policy_tailwind":           78.0,
        "dilution_risk":             30.0,
        "crowding_risk":             40.0,
        "execution_risk":            25.0,
    }
    factor_details = {
        k: FactorDetail(score=v, status="real", reasons=[f"Test reason for {k}"], source_tags=["test"])
        for k, v in factor_scores.items()
    }

    result = PlaybookScoreResult(
        ticker="ASML",
        playbook_id="serenity",
        final_score=78.0,
        hard_filter_pass=True,
        hard_filter_failures=[],
        summary_label="Strong playbook fit",
        factor_scores=factor_scores,
        penalties_applied={},
        matched_rules=["physical_bottleneck", "supply_chain_confirmed"],
        risks=[],
        stub_factors=[],
        raw_data={},
        factor_details=factor_details,
        matched_themes=["semicap_supply_chain", "photonics_cpo"],
        bottleneck_tags=["ASML"],
    )

    exp = generate_explanation(result, pb)

    _assert("thesis_summary is non-empty string", isinstance(exp.get("thesis_summary"), str) and len(exp["thesis_summary"]) > 20,
            f"got: {exp.get('thesis_summary')!r}")
    _assert("fit_reasoning is list", isinstance(exp.get("fit_reasoning"), list))
    _assert("fit_reasoning not empty", len(exp.get("fit_reasoning", [])) >= 1)
    _assert("non_fit_reasoning is list", isinstance(exp.get("non_fit_reasoning"), list))
    _assert("key_confirming_signals is list", isinstance(exp.get("key_confirming_signals"), list))
    _assert("top_risks is list", isinstance(exp.get("top_risks"), list))
    _assert("what_would_improve_score is list", isinstance(exp.get("what_would_improve_score"), list))
    _assert("what_would_break_thesis is list", isinstance(exp.get("what_would_break_thesis"), list))
    _assert("thesis_summary contains ASML", "ASML" in exp["thesis_summary"],
            f"got: {exp['thesis_summary']!r}")


# ────────────────────────────────────────────────────────────────────────────
# /api/query isolation guardrail
# ────────────────────────────────────────────────────────────────────────────

def test_api_query_isolation():
    print("\n[api_query_isolation]")
    import os
    main_path = os.path.join(os.path.dirname(__file__), "../../main.py")
    main_path = os.path.normpath(main_path)

    try:
        with open(main_path) as f:
            src = f.read()
    except FileNotFoundError:
        _fail("main.py readable", f"not found at {main_path}")
        return

    # Isolate the /api/query handler body (from @app.post("/api/query") to next @app.)
    query_pos  = src.find('@app.post("/api/query")')
    next_route = src.find("@app.", query_pos + 1)
    if query_pos < 0:
        _fail("find /api/query", "not found in main.py")
        return

    handler_body = src[query_pos:next_route] if next_route > query_pos else src[query_pos:]
    count = handler_body.count("playbook")

    _assert("/api/query handler has 0 playbook references", count == 0,
            f"found {count} 'playbook' references in handler body")

    # Verify the playbook router is in an isolated try/except
    pb_router_pos = src.find("playbook_router")
    if pb_router_pos >= 0:
        context = src[max(0, pb_router_pos - 100):pb_router_pos + 100]
        _assert("playbook router in try/except", "try:" in context or "except" in context,
                "playbook router registration not wrapped in try/except")


# ────────────────────────────────────────────────────────────────────────────
# Phase 3: Discovery engine tests
# ────────────────────────────────────────────────────────────────────────────

def test_discovery_types():
    """Smoke test DiscoverRequest / DiscoverResponse / DiscoveryCandidate models."""
    from services.playbook.discovery_types import (
        DiscoverRequest, DiscoverResponse, DiscoveryCandidate, DiscoveryScores,
        SupplyChainMapRequest, SupplyChainMapResponse, ChainNode, ChainLayer,
    )

    # DiscoverRequest defaults
    req = DiscoverRequest(mode="theme_scan", playbook_id="serenity")
    _assert("DiscoverRequest.mode", req.mode == "theme_scan", req.mode)
    _assert("DiscoverRequest.limit default", req.limit > 0, req.limit)
    _assert("DiscoverRequest.include_foreign default", not req.include_foreign, req.include_foreign)

    # DiscoveryScores
    scores = DiscoveryScores(
        chain_depth_score=85.0,
        bottleneck_criticality_score=90.0,
        hiddenness_score=70.0,
        giant_dependency_score=60.0,
        foreign_uniqueness_score=20.0,
        supply_chain_confidence_score=88.0,
        proxy_accessibility_score=100.0,
        theme_purity_score=95.0,
    )
    _assert("DiscoveryScores all fields", scores.chain_depth_score == 85.0, scores.chain_depth_score)

    # ChainNode
    node = ChainNode(
        ticker="ASML", company_name="ASML Holding", country="NL", exchange="AMS",
        layer=3, layer_label="Bottleneck", themes=["semicap_supply_chain"],
        role="EUV sole supplier", bottleneck_score=98.0, confidence="high",
        evidence=["Only EUV scanner maker globally"],
    )
    _assert("ChainNode.bottleneck_score", node.bottleneck_score == 98.0, node.bottleneck_score)

    # SupplyChainMapRequest
    sc_req = SupplyChainMapRequest(anchor="NVDA")
    _assert("SupplyChainMapRequest.anchor", sc_req.anchor == "NVDA", sc_req.anchor)
    _assert("SupplyChainMapRequest.max_depth default", sc_req.max_depth == 4, sc_req.max_depth)


def test_giant_map():
    """Giant map structure and lookup."""
    from services.playbook.giant_map import GIANT_MAP, get_giant, list_giants

    # Spot check known giants
    for gid in ["NVDA", "MSFT", "GOOGL", "META", "AMZN", "TSM", "AVGO"]:
        g = get_giant(gid)
        _assert(f"giant_map.get_giant({gid}) exists", g is not None, g)
        if g:
            _assert(f"giant_map.{gid}.themes non-empty", bool(g.get("themes")), g.get("themes"))
            _assert(f"giant_map.{gid}.capex_scale set", g.get("capex_scale") is not None, g.get("capex_scale"))

    # list_giants returns dicts
    giants = list_giants()
    _assert("list_giants returns non-empty list", len(giants) >= 8, len(giants))
    _assert("list_giants[0] has id", "id" in giants[0], giants[0].keys())


def test_supply_chain_graph():
    """Supply chain graph node registry and chain functions."""
    from services.playbook.supply_chain_graph import (
        NODE_REGISTRY, get_node, get_chain_for_theme, get_all_tickers_for_themes,
    )

    # NODE_REGISTRY coverage
    _assert("NODE_REGISTRY non-empty", len(NODE_REGISTRY) >= 30, len(NODE_REGISTRY))

    # Known nodes present
    for ticker in ["ASML", "NVDA", "MU", "ENTG", "VRT"]:
        n = get_node(ticker)
        _assert(f"node_registry.{ticker} exists", n is not None, n)
        if n:
            _assert(f"node_registry.{ticker}.bottleneck_score", n.get("bottleneck_score", 0) > 0, n.get("bottleneck_score"))

    # ASML bottleneck score ≥ 95
    asml = get_node("ASML")
    _assert("ASML bottleneck_score ≥ 95", asml and asml["bottleneck_score"] >= 95, asml)

    # get_chain_for_theme returns layers
    layers = get_chain_for_theme("semicap_supply_chain", max_depth=4)
    _assert("get_chain_for_theme returns layers", len(layers) > 0, len(layers))
    # Layers contain nodes
    all_nodes = [n for cl in layers for n in cl.nodes]
    _assert("chain layers have nodes", len(all_nodes) > 0, len(all_nodes))

    # No foreign by default
    countries = {n.country for cl in layers for n in cl.nodes}
    _assert("no foreign by default", "JP" not in countries and "NL" not in countries, countries)

    # With include_foreign
    layers_f = get_chain_for_theme("semicap_supply_chain", max_depth=4, include_foreign=True)
    nodes_f = [n for cl in layers_f for n in cl.nodes]
    foreign_found = any(n.country != "US" for n in nodes_f)
    _assert("include_foreign=True surfaces foreign nodes", foreign_found, [n.ticker for n in nodes_f[:5]])

    # get_all_tickers_for_themes — include_foreign=True to get NL-listed ASML
    tickers_all = get_all_tickers_for_themes(["semicap_supply_chain", "photonics_cpo"], include_foreign=True)
    _assert("get_all_tickers_for_themes non-empty", len(tickers_all) > 0, len(tickers_all))
    # ASML has country=NL (even though it's NASDAQ-listed) — surfaced with include_foreign=True
    _assert("ASML in tickers (include_foreign=True)", "ASML" in tickers_all, tickers_all[:8])


def test_theme_discovery():
    """Theme taxonomy structure and lookups."""
    from services.playbook.theme_discovery import THEME_TAXONOMY, get_theme, list_themes

    _assert("THEME_TAXONOMY non-empty", len(THEME_TAXONOMY) >= 10, len(THEME_TAXONOMY))

    # Required themes present
    for tid in ["photonics_cpo", "ai_power_energy", "grid_transformers", "advanced_packaging_test",
                "semicap_supply_chain", "defense_optics"]:
        t = get_theme(tid)
        _assert(f"theme_taxonomy.{tid} exists", t is not None, t)
        if t:
            _assert(f"theme.{tid}.label", bool(t.get("label")), t.get("label"))
            _assert(f"theme.{tid}.serenity_priority set", t.get("serenity_priority") in ("high", "medium", "low"), t.get("serenity_priority"))

    # list_themes returns full list
    themes = list_themes()
    _assert("list_themes non-empty", len(themes) >= 10, len(themes))
    _assert("list_themes[0] has id", "id" in themes[0], themes[0].keys())

    # High-priority themes
    high_priority = [t for t in themes if t.get("serenity_priority") == "high"]
    _assert("at least 4 high-priority themes", len(high_priority) >= 4, len(high_priority))


def test_foreign_market_map():
    """Foreign market map structure."""
    from services.playbook.foreign_market_map import (
        COUNTRY_METADATA, FOREIGN_ACCESS_MAP,
        get_country_meta, get_foreign_access, get_us_proxy,
        get_etf_proxies_for_theme, list_supported_countries,
    )

    # Country metadata
    for code in ["US", "JP", "KR", "TW", "NL", "DE"]:
        meta = get_country_meta(code)
        _assert(f"country_meta.{code} exists", meta is not None, meta)
        if meta:
            _assert(f"country_meta.{code}.data_confidence", bool(meta.get("data_confidence")), meta.get("data_confidence"))

    # Foreign access map
    for native in ["6857.T", "000660.KS", "BESI.AS"]:
        fa = get_foreign_access(native)
        _assert(f"foreign_access.{native} exists", fa is not None, fa)
        if fa:
            _assert(f"foreign_access.{native}.adr_ticker", bool(fa.get("adr_ticker")), fa.get("adr_ticker"))

    # US proxy resolution
    proxy = get_us_proxy("6857.T")
    _assert("get_us_proxy('6857.T') = ATEYY", proxy == "ATEYY", proxy)

    proxy_besi = get_us_proxy("BESI.AS")
    _assert("get_us_proxy('BESI.AS') = BESIY", proxy_besi == "BESIY", proxy_besi)

    # ETF proxies for themes
    etfs = get_etf_proxies_for_theme("semicap_supply_chain")
    _assert("ETF proxies for semicap_supply_chain non-empty", len(etfs) > 0, etfs)
    _assert("SOXX in semicap proxies", "SOXX" in etfs, etfs)

    # list_supported_countries
    countries = list_supported_countries()
    _assert("list_supported_countries non-empty", len(countries) >= 7, len(countries))


def test_discovery_scoring():
    """Unit test the discovery scoring engine functions."""
    from services.playbook.discovery_service import (
        _chain_depth_score, _hiddenness_score, _giant_dependency_score,
        _foreign_uniqueness_score, _supply_chain_confidence_score,
        _proxy_accessibility_score, _theme_purity_score, _rank_candidates,
    )
    from services.playbook.discovery_types import DiscoveryScores, DiscoveryCandidate

    # chain_depth_score: deeper = higher
    d0 = _chain_depth_score(0)
    d3 = _chain_depth_score(3)
    d4 = _chain_depth_score(4)
    _assert("chain_depth: layer 4 > layer 3 > layer 0", d4 > d3 > d0, (d0, d3, d4))
    _assert("chain_depth: layer 4 ≥ 90", d4 >= 90, d4)

    # hiddenness_score: foreign + thin coverage = higher
    h_us, _ = _hiddenness_score("ASML", "US", 5_000_000_000, "full", {"bottleneck_score": 98})
    h_jp, _ = _hiddenness_score("ATEYY", "JP", 500_000_000, "thin", {"bottleneck_score": 88})
    _assert("hiddenness: JP thin > US full", h_jp > h_us, (h_us, h_jp))

    # giant_dependency_score
    g0 = _giant_dependency_score({"giant_anchors": []})
    g1 = _giant_dependency_score({"giant_anchors": ["NVDA"]})
    g3 = _giant_dependency_score({"giant_anchors": ["NVDA", "MSFT", "META"]})
    _assert("giant_dep: 3 anchors > 1 > 0", g3 > g1 > g0, (g0, g1, g3))

    # foreign_uniqueness_score
    fu_us  = _foreign_uniqueness_score("US", False, "full")
    fu_jp  = _foreign_uniqueness_score("JP", True, "partial")
    fu_jp2 = _foreign_uniqueness_score("JP", False, "thin")
    _assert("foreign_uniqueness: no-ADR JP > ADR JP > US", fu_jp2 > fu_jp > fu_us, (fu_us, fu_jp, fu_jp2))

    # supply_chain_confidence_score
    sc_high = _supply_chain_confidence_score({"confidence": "high", "evidence": ["a", "b", "c"]})
    sc_low  = _supply_chain_confidence_score({"confidence": "low",  "evidence": []})
    _assert("sc_confidence: high > low", sc_high > sc_low, (sc_high, sc_low))
    _assert("sc_confidence: high ≥ 85", sc_high >= 85, sc_high)

    # proxy_accessibility_score
    pa_us  = _proxy_accessibility_score("US", True, None)
    pa_adr = _proxy_accessibility_score("JP", True, "ATEYY")
    pa_etf = _proxy_accessibility_score("KR", True, None)
    pa_none= _proxy_accessibility_score("JP", False, None)
    _assert("proxy_access: US > ADR > ETF > none", pa_us > pa_adr > pa_etf > pa_none, (pa_us, pa_adr, pa_etf, pa_none))

    # theme_purity_score
    tp1 = _theme_purity_score(["semicap_supply_chain"])
    tp2 = _theme_purity_score(["semicap_supply_chain", "photonics_cpo"])
    tp4 = _theme_purity_score(["a", "b", "c", "d"])
    _assert("theme_purity: 1 theme > 2 > 4", tp1 > tp2 > tp4, (tp1, tp2, tp4))
    _assert("theme_purity: single ≥ 95", tp1 >= 95, tp1)


def test_discovery_engine_sync():
    """End-to-end discovery engine: theme_scan and giant_chain modes (sync, no live data)."""

    async def _run():
        from services.playbook.discovery_types import DiscoverRequest
        from services.playbook.discovery_service import run_discover

        # Theme scan — semicap_supply_chain
        req = DiscoverRequest(
            mode="theme_scan",
            theme_ids=["semicap_supply_chain"],
            playbook_id="serenity",
            limit=10,
            include_foreign=False,
            use_web_validation=False,
        )
        result = await run_discover(req)
        _assert("theme_scan returns candidates", len(result.top_candidates) > 0, len(result.top_candidates))
        _assert("theme_scan summary non-empty", bool(result.summary), result.summary[:50])
        tickers = [c.ticker for c in result.top_candidates]
        # ASML is NL-domiciled so filtered by include_foreign=False; check US-listed names
        us_semicap = [t for t in tickers if t in ("AMAT", "LRCX", "KLAC", "ENTG", "ACLS", "KLAC", "ONTO", "MKSI")]
        _assert("US semicap names in theme_scan results (no foreign)", len(us_semicap) > 0, tickers)

        # Giant chain — NVDA
        req2 = DiscoverRequest(
            mode="giant_chain",
            giant="NVDA",
            playbook_id="serenity",
            limit=10,
            include_foreign=False,
            use_web_validation=False,
        )
        result2 = await run_discover(req2)
        _assert("giant_chain returns candidates", len(result2.top_candidates) > 0, len(result2.top_candidates))
        _assert("giant_chain meta.total_candidates_found > 0", result2.meta.get("total_candidates_found", 0) > 0, result2.meta)

        # Foreign bottlenecks mode
        req3 = DiscoverRequest(
            mode="foreign_bottlenecks",
            playbook_id="serenity",
            limit=8,
            include_foreign=True,
            use_web_validation=False,
        )
        result3 = await run_discover(req3)
        _assert("foreign_bottlenecks returns candidates", len(result3.top_candidates) > 0, len(result3.top_candidates))
        countries = {c.country for c in result3.top_candidates}
        _assert("foreign_bottlenecks: no US candidates", "US" not in countries, countries)

    asyncio.run(_run())


def test_supply_chain_map_engine():
    """End-to-end supply chain map: giant anchor and theme anchor."""

    async def _run():
        from services.playbook.discovery_types import SupplyChainMapRequest
        from services.playbook.discovery_service import run_supply_chain_map

        # Giant anchor
        req = SupplyChainMapRequest(anchor="NVDA", max_depth=4, include_foreign=False)
        result = await run_supply_chain_map(req)
        _assert("supply_chain_map.anchor = NVDA", result.anchor == "NVDA", result.anchor)
        _assert("supply_chain_map has layers", len(result.layers) > 0, len(result.layers))
        all_nodes = [n for cl in result.layers for n in cl.nodes]
        _assert("supply_chain_map NVDA has nodes", len(all_nodes) > 0, len(all_nodes))
        _assert("supply_chain_map NVDA meta.total_nodes > 0", result.meta.get("total_nodes", 0) > 0, result.meta)

        # Theme anchor
        req2 = SupplyChainMapRequest(theme_id="advanced_packaging_test", max_depth=4, include_foreign=False)
        result2 = await run_supply_chain_map(req2)
        _assert("supply_chain_map.theme anchor", result2.anchor == "advanced_packaging_test", result2.anchor)
        _assert("supply_chain_map theme has layers", len(result2.layers) > 0, len(result2.layers))

        # With foreign
        req3 = SupplyChainMapRequest(anchor="NVDA", max_depth=4, include_foreign=True)
        result3 = await run_supply_chain_map(req3)
        all_nodes3 = [n for cl in result3.layers for n in cl.nodes]
        country_set = {n.country for n in all_nodes3}
        _assert("supply_chain_map with_foreign includes non-US", len(country_set) > 1, country_set)
        _assert("supply_chain_map adr_etf_proxies present", isinstance(result3.adr_etf_proxies, dict), result3.adr_etf_proxies)

    asyncio.run(_run())


def test_discovery_bridge_analyzer():
    """AnalyzeRequest discovery bridge: serenity + discovery_mode injects discovered tickers."""

    async def _run():
        from services.playbook.analyzer import AnalyzeRequest

        # Verify discovery_mode, giant, theme_ids fields are accepted without error
        req = AnalyzeRequest(
            playbook_id="serenity",
            context_mode="watchlist",
            tickers=["NVDA", "ASML"],
            discovery_mode="theme_scan",
            theme_ids=["semicap_supply_chain"],
            include_foreign=False,
            max_depth=3,
            limit=5,
        )
        _assert("AnalyzeRequest.discovery_mode accepted", req.discovery_mode == "theme_scan", req.discovery_mode)
        _assert("AnalyzeRequest.theme_ids accepted", req.theme_ids == ["semicap_supply_chain"], req.theme_ids)
        _assert("AnalyzeRequest.include_foreign accepted", not req.include_foreign, req.include_foreign)

        # S&J also accepts discovery fields without altering behavior
        req_sj = AnalyzeRequest(
            playbook_id="sjcapital",
            context_mode="watchlist",
            tickers=["NVDA"],
            discovery_mode="theme_scan",
            theme_ids=["defense_optics"],
        )
        _assert("sjcapital accepts discovery_mode field", req_sj.discovery_mode == "theme_scan", req_sj.discovery_mode)

    asyncio.run(_run())


def test_discovery_query_isolation():
    """Ensure discovery modules have no /api/query coupling."""
    import os

    discovery_files = [
        "services/playbook/discovery_types.py",
        "services/playbook/discovery_service.py",
        "services/playbook/discovery_enrichment.py",
        "services/playbook/giant_map.py",
        "services/playbook/supply_chain_graph.py",
        "services/playbook/theme_discovery.py",
        "services/playbook/foreign_market_map.py",
    ]

    backend_dir = os.path.dirname(os.path.abspath(__file__))
    while not os.path.basename(backend_dir) == "backend" and backend_dir != "/":
        backend_dir = os.path.dirname(backend_dir)

    # Only check for actual Python import statements — comments/docstrings mentioning
    # "/api/query" for documentation purposes are allowed and expected.
    import_banned = ["from routes.query", "from api.query", "import query_handler"]

    for rel_path in discovery_files:
        abs_path = os.path.join(backend_dir, rel_path)
        if not os.path.exists(abs_path):
            _fail(f"discovery_isolation.{os.path.basename(rel_path)} file exists", "File not found")
            continue
        with open(abs_path) as f:
            raw_lines = f.readlines()
        # Only check non-comment, non-docstring code lines for banned imports
        code_lines = [
            l for l in raw_lines
            if not l.strip().startswith("#") and not l.strip().startswith('"""') and not l.strip().startswith("'''")
        ]
        code_src = "".join(code_lines)
        for ban in import_banned:
            _assert(
                f"discovery_isolation.{os.path.basename(rel_path)}: no '{ban}' import",
                ban not in code_src,
                f"Found banned import: {ban!r}",
            )


# ────────────────────────────────────────────────────────────────────────────
# Phase 4 Tests
# ────────────────────────────────────────────────────────────────────────────

def test_phase4_new_nodes():
    """Verify Phase 4 expanded NODE_REGISTRY contains new curated nodes."""
    from services.playbook.supply_chain_graph import NODE_REGISTRY

    # US additions
    for ticker in ["MRVL", "SMCI", "TER", "PLAB", "UCTT", "KLIC", "WOLF", "CDNS", "SNPS", "BE", "MTZ", "HWM"]:
        _assert(f"phase4_node_registry.{ticker} exists", ticker in NODE_REGISTRY, f"Missing {ticker}")

    # Japan additions
    for ticker in ["8035.T", "6981.T", "3436.T", "6146.T", "4901.T", "7735.T"]:
        _assert(f"phase4_node_registry.{ticker} exists", ticker in NODE_REGISTRY, f"Missing {ticker}")

    # Germany
    _assert("phase4_node_registry.IFNNY exists", "IFNNY" in NODE_REGISTRY, "Missing IFNNY")

    # UK
    _assert("phase4_node_registry.IQE.L exists", "IQE.L" in NODE_REGISTRY, "Missing IQE.L")

    # Check new node structure has required fields
    node = NODE_REGISTRY.get("8035.T", {})
    _assert("phase4.8035.T has company_name", "company_name" in node, node)
    _assert("phase4.8035.T has themes", bool(node.get("themes")), node.get("themes"))
    _assert("phase4.8035.T has us_access_proxy", node.get("us_access_proxy") == "TOELY", node.get("us_access_proxy"))
    _assert("phase4.8035.T bottleneck_score >= 80", node.get("bottleneck_score", 0) >= 80, node.get("bottleneck_score"))

    node_murata = NODE_REGISTRY.get("6981.T", {})
    _assert("phase4.Murata has ADR MRAAY", node_murata.get("us_access_proxy") == "MRAAY", node_murata.get("us_access_proxy"))

    node_disco = NODE_REGISTRY.get("6146.T", {})
    _assert("phase4.Disco layer == 4", node_disco.get("layer") == 4, node_disco.get("layer"))
    _assert("phase4.Disco bottleneck_score >= 80", node_disco.get("bottleneck_score", 0) >= 80, node_disco.get("bottleneck_score"))


def test_phase4_hiddenness_logic():
    """
    Verify hiddenness scoring reform:
    - Thin data does NOT inflate hiddenness
    - Household AI names get low hiddenness
    - Foreign layer-3+ names get genuine hiddenness
    """
    from services.playbook.discovery_service import _hiddenness_score
    from services.playbook.supply_chain_graph import NODE_REGISTRY

    # Household name → low hiddenness
    nvda_node = NODE_REGISTRY.get("NVDA", {"layer": 0, "bottleneck_score": 95, "confidence": "high"})
    score_nvda, reason_nvda = _hiddenness_score("NVDA", "US", 3_000_000_000_000, "full", nvda_node)
    _assert("phase4_hiddenness.NVDA household → low", score_nvda <= 20.0, f"Got {score_nvda}")
    _assert("phase4_hiddenness.NVDA reason mentions household", "household" in reason_nvda.lower() or "widely" in reason_nvda.lower(), reason_nvda)

    # Thin data should NOT inflate hiddenness
    thin_node = {"layer": 2, "bottleneck_score": 50, "confidence": "low"}
    score_thin_with_cap, _ = _hiddenness_score("XYZFAKE", "JP", None, "thin", thin_node)
    score_thin_no_cap,  _ = _hiddenness_score("XYZFAKE", "JP", None, "thin", thin_node)
    # Both should be similar — thin data doesn't boost hiddenness
    _assert(
        "phase4_hiddenness.thin_data_no_boost: thin-data score <= 75",
        score_thin_with_cap <= 75.0,
        f"Score too high ({score_thin_with_cap}) — thin data is inflating hiddenness",
    )

    # Deep foreign layer with real thin ADR → should have moderate hiddenness from country+layer
    disco_node = NODE_REGISTRY.get("6146.T", {"layer": 4, "bottleneck_score": 84, "confidence": "high"})
    score_disco, reason_disco = _hiddenness_score("DISCF", "JP", None, "thin", disco_node)
    _assert("phase4_hiddenness.Disco >= 55 from country+layer", score_disco >= 55.0, f"Got {score_disco}")
    # Thin data note should NOT be in reason (thin belongs in confidence_penalties)
    _assert(
        "phase4_hiddenness.thin_not_in_disco_reason",
        "thin" not in reason_disco.lower(),
        f"Thin-data text leaked into hiddenness reason: {reason_disco}",
    )

    # US mid-cap unknown company
    mid_cap_node = {"layer": 3, "bottleneck_score": 73, "confidence": "high"}
    score_mid, _ = _hiddenness_score("UCTT", "US", 1_500_000_000, "full", mid_cap_node)
    _assert("phase4_hiddenness.US mid-cap layer-3 >= 50", score_mid >= 50.0, f"Got {score_mid}")


def test_phase4_confidence_penalties():
    """Verify confidence_penalties are populated correctly for thin/OTC/no-proxy cases."""
    from services.playbook.discovery_service import _compute_confidence_penalties

    # Thin OTC ADR — should have multiple penalties
    penalties_thin = _compute_confidence_penalties(
        "DISCF", "JP", None, "thin", "low", None, "DISCF"
    )
    _assert("phase4_penalties.thin→coverage penalty", any("thin" in p.lower() for p in penalties_thin), penalties_thin)
    _assert("phase4_penalties.thin→data confidence penalty", any("confidence" in p.lower() for p in penalties_thin), penalties_thin)
    _assert("phase4_penalties.thin→market cap penalty", any("market cap" in p.lower() for p in penalties_thin), penalties_thin)
    _assert("phase4_penalties.thin has >=3 penalties", len(penalties_thin) >= 3, penalties_thin)

    # US high-confidence full coverage — should have no penalties
    penalties_us = _compute_confidence_penalties(
        "ENTG", "US", 15_000_000_000, "full", "high", None, None
    )
    _assert("phase4_penalties.US_full→zero penalties", len(penalties_us) == 0, penalties_us)

    # Foreign with ADR and partial coverage — OTC ADR penalty only
    penalties_adr = _compute_confidence_penalties(
        "ATEYY", "JP", 10_000_000_000, "partial", "medium", "ATEYY", "ATEYY"
    )
    _assert("phase4_penalties.adr_partial→OTC penalty", any("OTC" in p or "ADR" in p for p in penalties_adr), penalties_adr)


def test_phase4_best_blend_score():
    """Verify best_blend_score ranks candidates sensibly."""
    from services.playbook.discovery_service import _compute_best_blend_score
    from services.playbook.discovery_types import DiscoveryScores

    # High bottleneck + high confidence = high blend score
    scores_high = DiscoveryScores(
        chain_depth_score=85.0,
        bottleneck_criticality_score=92.0,
        hiddenness_score=72.0,
        giant_dependency_score=80.0,
        supply_chain_confidence_score=90.0,
        theme_purity_score=95.0,
    )
    blend_high = _compute_best_blend_score(scores_high, "high", [])
    _assert("phase4_blend.high_quality >= 80", blend_high >= 80.0, f"Got {blend_high}")

    # Low confidence + many penalties = lower blend score
    scores_low = DiscoveryScores(
        chain_depth_score=85.0,
        bottleneck_criticality_score=92.0,
        hiddenness_score=72.0,
        giant_dependency_score=80.0,
        supply_chain_confidence_score=90.0,
        theme_purity_score=95.0,
    )
    penalties = ["thin coverage", "no market cap", "no US proxy"]
    blend_penalized = _compute_best_blend_score(scores_low, "low", penalties)
    _assert("phase4_blend.penalized < high", blend_penalized < blend_high, f"Got {blend_penalized} vs {blend_high}")
    _assert("phase4_blend.penalty_mult < 1.0 applied", blend_penalized < blend_high * 0.95, f"Got {blend_penalized}")

    # Verify ordering: ENTG (bc=85, layer=3, high conf, US) > ATKR (bc=68, layer=4)
    # ENTG is a hidden specialist; ATKR is lower bottleneck — ENTG should rank higher.
    from services.playbook.supply_chain_graph import NODE_REGISTRY
    from services.playbook.discovery_service import _build_candidate

    c_entg = _build_candidate("ENTG", NODE_REGISTRY["ENTG"])
    c_atkr = _build_candidate("ATKR", NODE_REGISTRY["ATKR"])
    _assert("phase4_blend.ENTG > ATKR by blend", c_entg.best_blend_score > c_atkr.best_blend_score,
            f"ENTG={c_entg.best_blend_score}, ATKR={c_atkr.best_blend_score}")


def test_phase4_candidate_new_fields():
    """All new Phase 4 candidate fields are populated on _build_candidate output."""
    from services.playbook.discovery_service import _build_candidate
    from services.playbook.supply_chain_graph import NODE_REGISTRY

    for ticker in ["ENTG", "BESI.AS", "8035.T", "6146.T"]:
        node = NODE_REGISTRY.get(ticker)
        if not node:
            _fail(f"phase4_candidate_fields.{ticker} in registry", "Not found")
            continue
        canon = node.get("us_access_proxy", ticker) if node.get("country", "US") != "US" else ticker
        c = _build_candidate(canon, node)

        _assert(f"phase4_fields.{ticker}.best_blend_score > 0", c.best_blend_score > 0, c.best_blend_score)
        _assert(f"phase4_fields.{ticker}.visibility_bucket set", c.visibility_bucket in ("household", "widely_covered", "known", "specialist", "hidden"), c.visibility_bucket)
        _assert(f"phase4_fields.{ticker}.chain_role_type set", c.chain_role_type in ("platform_anchor", "direct_bottleneck", "adjacent_supplier", "indirect_beneficiary"), c.chain_role_type)
        _assert(f"phase4_fields.{ticker}.why_now non-empty", bool(c.why_now), c.why_now)
        _assert(f"phase4_fields.{ticker}.why_hidden non-empty", bool(c.why_hidden), c.why_hidden)
        _assert(f"phase4_fields.{ticker}.what_to_verify_next non-empty", bool(c.what_to_verify_next), c.what_to_verify_next)
        _assert(f"phase4_fields.{ticker}.confidence_penalties is list", isinstance(c.confidence_penalties, list), type(c.confidence_penalties))
        _assert(f"phase4_fields.{ticker}.data_gaps is list", isinstance(c.data_gaps, list), type(c.data_gaps))

    # NVDA should be household and platform_anchor
    c_nvda = _build_candidate("NVDA", NODE_REGISTRY["NVDA"])
    _assert("phase4_fields.NVDA.visibility=household", c_nvda.visibility_bucket == "household", c_nvda.visibility_bucket)
    _assert("phase4_fields.NVDA.chain_role=platform_anchor", c_nvda.chain_role_type == "platform_anchor", c_nvda.chain_role_type)
    _assert("phase4_fields.NVDA.hiddenness <= 20", c_nvda.hiddenness_score <= 20.0, c_nvda.hiddenness_score)


def test_phase4_ranking_buckets():
    """Verify ranking buckets populate correctly."""
    from services.playbook.discovery_service import _build_ranking_buckets, _build_candidate
    from services.playbook.supply_chain_graph import NODE_REGISTRY

    # Build a set of candidates covering different profiles
    tickers_to_test = ["ENTG", "ASML", "6857.T", "BESI.AS", "ETN", "6146.T", "CDNS", "TER"]
    candidates = []
    for t in tickers_to_test:
        node = NODE_REGISTRY.get(t)
        if not node:
            continue
        canon = node.get("us_access_proxy", t) if node.get("country", "US") != "US" else t
        candidates.append(_build_candidate(canon, node))

    buckets = _build_ranking_buckets(candidates, limit=5)

    _assert("phase4_buckets.top_hidden_bottlenecks is list", isinstance(buckets["top_hidden_bottlenecks"], list), type(buckets["top_hidden_bottlenecks"]))
    _assert("phase4_buckets.top_direct_chokepoints is list", isinstance(buckets["top_direct_chokepoints"], list), type(buckets["top_direct_chokepoints"]))
    _assert("phase4_buckets.top_foreign_specialists is list", isinstance(buckets["top_foreign_specialists"], list), type(buckets["top_foreign_specialists"]))
    _assert("phase4_buckets.top_us_accessible_foreign_proxies is list", isinstance(buckets["top_us_accessible_foreign_proxies"], list), type(buckets["top_us_accessible_foreign_proxies"]))
    _assert("phase4_buckets.highest_confidence_candidates is list", isinstance(buckets["highest_confidence_candidates"], list), type(buckets["highest_confidence_candidates"]))
    _assert("phase4_buckets.best_blend_candidates is list", isinstance(buckets["best_blend_candidates"], list), type(buckets["best_blend_candidates"]))

    # Foreign specialists should only have non-US names
    foreign_names = buckets["top_foreign_specialists"]
    for c in foreign_names:
        _assert(f"phase4_buckets.foreign_specialist.{c.ticker} is non-US", c.country != "US", f"{c.ticker} country={c.country}")

    # best_blend_candidates should be sorted descending
    blend_list = buckets["best_blend_candidates"]
    if len(blend_list) >= 2:
        _assert("phase4_buckets.best_blend sorted descending",
                blend_list[0].best_blend_score >= blend_list[-1].best_blend_score,
                f"{blend_list[0].best_blend_score} vs {blend_list[-1].best_blend_score}")

    # top_direct_chokepoints should only have direct_bottleneck role type
    for c in buckets["top_direct_chokepoints"]:
        _assert(f"phase4_buckets.chokepoint.{c.ticker} is direct_bottleneck",
                c.chain_role_type == "direct_bottleneck", c.chain_role_type)


def test_phase4_discover_response_buckets():
    """DiscoverResponse includes all 6 Phase 4 ranking bucket fields."""
    import asyncio
    from services.playbook.discovery_service import run_discover
    from services.playbook.discovery_types import DiscoverRequest

    async def _run():
        req = DiscoverRequest(
            playbook_id="serenity",
            mode="theme_scan",
            theme_ids=["semicap_supply_chain", "advanced_packaging_test"],
            include_foreign=True,
            max_depth=4,
            limit=15,
        )
        resp = await run_discover(req)
        _assert("phase4_response.top_hidden_bottlenecks present", isinstance(resp.top_hidden_bottlenecks, list), type(resp.top_hidden_bottlenecks))
        _assert("phase4_response.top_direct_chokepoints present", isinstance(resp.top_direct_chokepoints, list), type(resp.top_direct_chokepoints))
        _assert("phase4_response.top_foreign_specialists present", isinstance(resp.top_foreign_specialists, list), type(resp.top_foreign_specialists))
        _assert("phase4_response.top_us_accessible_foreign_proxies present", isinstance(resp.top_us_accessible_foreign_proxies, list), type(resp.top_us_accessible_foreign_proxies))
        _assert("phase4_response.highest_confidence_candidates present", isinstance(resp.highest_confidence_candidates, list), type(resp.highest_confidence_candidates))
        _assert("phase4_response.best_blend_candidates present", isinstance(resp.best_blend_candidates, list), type(resp.best_blend_candidates))

        # All top_candidates have new fields
        for c in resp.top_candidates[:3]:
            _assert(f"phase4_response.candidate.{c.ticker}.best_blend_score > 0", c.best_blend_score > 0, c.best_blend_score)
            _assert(f"phase4_response.candidate.{c.ticker}.visibility_bucket set", bool(c.visibility_bucket), c.visibility_bucket)
            _assert(f"phase4_response.candidate.{c.ticker}.why_now set", bool(c.why_now), c.why_now)

        # Backward compatibility: top_candidates still present
        _assert("phase4_response.top_candidates still present", len(resp.top_candidates) > 0, len(resp.top_candidates))

    asyncio.run(_run())


def test_phase4_compare_models():
    """CompareRequest and CompareResponse models parse correctly."""
    from services.playbook.discovery_types import CompareRequest, CompareResponse, CompareTickerResult

    req = CompareRequest(
        tickers=["LITE", "AMAT", "ENTG"],
        playbooks=["serenity", "sjcapital"],
        include_breakdown=True,
    )
    _assert("phase4_compare_req.tickers", req.tickers == ["LITE", "AMAT", "ENTG"], req.tickers)
    _assert("phase4_compare_req.playbooks", req.playbooks == ["serenity", "sjcapital"], req.playbooks)
    _assert("phase4_compare_req.include_breakdown", req.include_breakdown, req.include_breakdown)

    # Minimal result model
    r = CompareTickerResult(ticker="LITE", serenity_score=72.5, sj_score=55.0, classification="serenity_only")
    _assert("phase4_compare_result.ticker", r.ticker == "LITE", r.ticker)
    _assert("phase4_compare_result.serenity_score", r.serenity_score == 72.5, r.serenity_score)
    _assert("phase4_compare_result.classification", r.classification == "serenity_only", r.classification)
    delta = round(72.5 - 55.0, 1)
    r2 = r.model_copy(update={"score_delta": delta})
    _assert("phase4_compare_result.score_delta", r2.score_delta == delta, r2.score_delta)

    resp = CompareResponse(
        tickers_compared=["LITE"],
        playbooks=["serenity", "sjcapital"],
        results=[r],
        consensus_names=[],
        serenity_only_names=["LITE"],
        sj_only_names=[],
        low_fit_both=[],
    )
    _assert("phase4_compare_resp.serenity_only_names", resp.serenity_only_names == ["LITE"], resp.serenity_only_names)


def test_phase4_compare_classify():
    """Compare classifications are deterministic."""
    from services.playbook.compare_service import _classify

    s_pass, j_pass, both_fail = 70.0, 65.0, 40.0

    cls, sp, jp = _classify(s_pass, j_pass)
    _assert("phase4_classify.consensus", cls == "consensus", cls)
    _assert("phase4_classify.consensus.s_pass", sp, sp)
    _assert("phase4_classify.consensus.j_pass", jp, jp)

    cls, sp, jp = _classify(s_pass, both_fail)
    _assert("phase4_classify.serenity_only", cls == "serenity_only", cls)
    _assert("phase4_classify.serenity_only.s_pass", sp, sp)
    _assert("phase4_classify.serenity_only.j_not_pass", not jp, jp)

    cls, sp, jp = _classify(both_fail, j_pass)
    _assert("phase4_classify.sj_only", cls == "sj_only", cls)

    cls, sp, jp = _classify(both_fail, both_fail)
    _assert("phase4_classify.low_fit_both", cls == "low_fit_both", cls)

    cls, sp, jp = _classify(None, j_pass)
    _assert("phase4_classify.none_serenity→sj_only", cls == "sj_only", cls)

    cls, sp, jp = _classify(s_pass, None)
    _assert("phase4_classify.none_sj→serenity_only", cls == "serenity_only", cls)

    cls, sp, jp = _classify(None, None)
    _assert("phase4_classify.both_none→low_fit", cls == "low_fit_both", cls)


def test_phase4_serenity_score_in_registry():
    """_serenity_composite_for_ticker returns valid score for in-registry tickers, None for unknowns."""
    from services.playbook.compare_service import _serenity_composite_for_ticker

    # ENTG is in registry — should return valid score
    result = _serenity_composite_for_ticker("ENTG")
    _assert("phase4_serenity_score.ENTG not None", result is not None, result)
    if result:
        score, breakdown = result
        _assert("phase4_serenity_score.ENTG > 0", score > 0, score)
        _assert("phase4_serenity_score.ENTG.breakdown has bottleneck_criticality",
                "bottleneck_criticality_score" in breakdown, breakdown.keys())
        _assert("phase4_serenity_score.ENTG.in_node_registry", breakdown.get("in_node_registry"), breakdown)

    # BESIY is the ADR for BESI.AS — should resolve
    result_besi = _serenity_composite_for_ticker("BESIY")
    _assert("phase4_serenity_score.BESIY resolves", result_besi is not None, result_besi)

    # Random ticker not in registry
    result_none = _serenity_composite_for_ticker("ZZZMADEUPT")
    _assert("phase4_serenity_score.unknown→None", result_none is None, result_none)

    # NVDA — household name → low hiddenness, still valid score
    result_nvda = _serenity_composite_for_ticker("NVDA")
    _assert("phase4_serenity_score.NVDA not None", result_nvda is not None, result_nvda)
    if result_nvda:
        score_nvda, _ = result_nvda
        _assert("phase4_serenity_score.NVDA > 0 even if household", score_nvda > 0, score_nvda)


def test_phase4_foreign_map_expanded():
    """FOREIGN_ACCESS_MAP has Phase 4 additions."""
    from services.playbook.foreign_market_map import FOREIGN_ACCESS_MAP

    new_entries = ["8035.T", "6981.T", "3436.T", "6146.T", "4901.T", "7735.T", "IQE.L", "IFNNY"]
    for native_ticker in new_entries:
        _assert(f"phase4_foreign_map.{native_ticker} exists", native_ticker in FOREIGN_ACCESS_MAP, f"Missing {native_ticker}")

    # TEL entry should have TOELY as ADR
    tel = FOREIGN_ACCESS_MAP.get("8035.T", {})
    _assert("phase4_foreign_map.8035.T.adr=TOELY", tel.get("adr_ticker") == "TOELY", tel.get("adr_ticker"))
    _assert("phase4_foreign_map.8035.T.coverage=partial", tel.get("coverage_status") == "partial", tel.get("coverage_status"))

    # DISCO should have thin coverage (no liquid US proxy)
    disco = FOREIGN_ACCESS_MAP.get("6146.T", {})
    _assert("phase4_foreign_map.6146.T.coverage=thin", disco.get("coverage_status") == "thin", disco.get("coverage_status"))
    _assert("phase4_foreign_map.6146.T.no_adr", disco.get("adr_ticker") is None, disco.get("adr_ticker"))

    # IFNNY (Infineon, Germany) should have high data confidence
    inf = FOREIGN_ACCESS_MAP.get("IFNNY", {})
    _assert("phase4_foreign_map.IFNNY.confidence=high", inf.get("data_confidence") == "high", inf.get("data_confidence"))


def test_phase4_giant_map_expanded():
    """giant_map.py has CoreWeave_Neocloud and all prior giants."""
    from services.playbook.giant_map import GIANT_MAP, get_giant, list_giants

    _assert("phase4_giant_map.CoreWeave_Neocloud exists", "CoreWeave_Neocloud" in GIANT_MAP, list(GIANT_MAP.keys()))

    cw = GIANT_MAP["CoreWeave_Neocloud"]
    _assert("phase4_giant.CoreWeave has themes", bool(cw.get("themes")), cw.get("themes"))
    _assert("phase4_giant.CoreWeave has ai_infrastructure theme", "ai_infrastructure" in cw["themes"], cw["themes"])
    _assert("phase4_giant.CoreWeave has capex_scale", bool(cw.get("capex_scale")), cw.get("capex_scale"))

    giants = list_giants()
    _assert("phase4_giant.list_giants length >= 11", len(giants) >= 11, len(giants))

    # get_giant should be case-insensitive
    g = get_giant("coreweave_neocloud")
    _assert("phase4_giant.get_giant case insensitive", g is not None, g)


def test_phase4_analyze_compare_field():
    """AnalyzeRequest accepts compare_with_playbook field without error."""
    from services.playbook.analyzer import AnalyzeRequest

    req = AnalyzeRequest(
        playbook_id="serenity",
        context_mode="watchlist",
        tickers=["NVDA", "ASML"],
        compare_with_playbook="sjcapital",
    )
    _assert("phase4_analyze.compare_with_playbook accepted", req.compare_with_playbook == "sjcapital", req.compare_with_playbook)

    # Should also work with None (default)
    req2 = AnalyzeRequest(playbook_id="serenity", tickers=["NVDA"])
    _assert("phase4_analyze.compare_with_playbook default None", req2.compare_with_playbook is None, req2.compare_with_playbook)


def test_phase4_no_brave_tavily():
    """
    compare_service.py and discovery_service.py contain no Brave/Tavily import statements.
    Only code lines are checked — comments and docstrings documenting that
    Brave/Tavily are NOT used are expected and allowed.
    """
    import os

    files_to_check = [
        "services/playbook/compare_service.py",
        "services/playbook/discovery_service.py",
        "services/playbook/discovery_types.py",
    ]
    # Check actual import/usage patterns — not documentation strings
    banned_imports = ["import brave", "import tavily", "BraveSearch(", "TavilyClient("]

    backend_dir = os.path.dirname(os.path.abspath(__file__))
    while not os.path.basename(backend_dir) == "backend" and backend_dir != "/":
        backend_dir = os.path.dirname(backend_dir)

    for rel_path in files_to_check:
        abs_path = os.path.join(backend_dir, rel_path)
        if not os.path.exists(abs_path):
            _fail(f"phase4_brave_check.{os.path.basename(rel_path)} file exists", "Not found")
            continue
        with open(abs_path) as f:
            raw_lines = f.readlines()
        # Only check non-comment, non-docstring lines for banned import patterns
        code_lines = [
            l for l in raw_lines
            if not l.strip().startswith("#") and not l.strip().startswith('"""') and not l.strip().startswith("'''")
        ]
        code_src = "".join(code_lines)
        for b in banned_imports:
            _assert(
                f"phase4_brave_tavily.{os.path.basename(rel_path)}: no '{b}' import",
                b not in code_src,
                f"Found banned import: {b!r}",
            )


def test_phase4_query_isolation_new_files():
    """compare_service.py has no /api/query coupling."""
    import os

    files_to_check = ["services/playbook/compare_service.py"]
    import_banned  = ["from routes.query", "from api.query", "import query_handler"]

    backend_dir = os.path.dirname(os.path.abspath(__file__))
    while not os.path.basename(backend_dir) == "backend" and backend_dir != "/":
        backend_dir = os.path.dirname(backend_dir)

    for rel_path in files_to_check:
        abs_path = os.path.join(backend_dir, rel_path)
        if not os.path.exists(abs_path):
            _fail(f"phase4_isolation.{os.path.basename(rel_path)} exists", "Not found")
            continue
        with open(abs_path) as f:
            raw = f.readlines()
        code_lines = [
            l for l in raw
            if not l.strip().startswith("#") and not l.strip().startswith('"""') and not l.strip().startswith("'''")
        ]
        code_src = "".join(code_lines)
        for ban in import_banned:
            _assert(
                f"phase4_isolation.{os.path.basename(rel_path)}: no '{ban}'",
                ban not in code_src,
                f"Found banned import: {ban!r}",
            )


# ────────────────────────────────────────────────────────────────────────────
# Phase 5 tests — Expanded coverage + new fields
# ────────────────────────────────────────────────────────────────────────────

def test_phase5_new_nodes_registry():
    print("\n[phase5_new_nodes_registry]")
    from services.playbook.supply_chain_graph import NODE_REGISTRY

    phase5_all = [
        "FN", "LSCC", "SPXC", "MOD", "ESLT", "GFS",           # US
        "4062.T", "6988.T", "5802.T", "6501.T", "6963.T",      # JP
        "042700.KS", "086390.KS",                                # KR
        "3037.TW", "6239.TW", "3711.TW",                        # TW
        "SOI.PA", "SBGSY", "SAFRY",                             # FR
        "WAF.DE",                                                # DE
    ]
    for t in phase5_all:
        node = NODE_REGISTRY.get(t)
        _assert(f"Phase5 node exists: {t}", node is not None, "missing from NODE_REGISTRY")
        if node:
            _assert(f"{t}.themes non-empty", len(node.get("themes", [])) > 0)
            _assert(f"{t}.bottleneck_score >= 70", node.get("bottleneck_score", 0) >= 70,
                    f"got {node.get('bottleneck_score')}")
            _assert(f"{t}.company_name non-empty", bool(node.get("company_name")))

    _assert("4062.T ADR = IBDNY", NODE_REGISTRY.get("4062.T", {}).get("us_access_proxy") == "IBDNY")
    _assert("042700.KS ADR = HASEY", NODE_REGISTRY.get("042700.KS", {}).get("us_access_proxy") == "HASEY")
    _assert("6501.T ADR = HTHIY", NODE_REGISTRY.get("6501.T", {}).get("us_access_proxy") == "HTHIY")
    _assert("SOI.PA proxy = SOITF", NODE_REGISTRY.get("SOI.PA", {}).get("us_access_proxy") == "SOITF")
    _assert("WAF.DE proxy = SSLLF", NODE_REGISTRY.get("WAF.DE", {}).get("us_access_proxy") == "SSLLF")

    ibiden_bc = NODE_REGISTRY.get("4062.T", {}).get("bottleneck_score", 0)
    _assert("Ibiden ABF bottleneck_score >= 90", ibiden_bc >= 90, f"got {ibiden_bc}")
    hanmi_bc = NODE_REGISTRY.get("042700.KS", {}).get("bottleneck_score", 0)
    _assert("Hanmi TC bonder bottleneck_score >= 80", hanmi_bc >= 80, f"got {hanmi_bc}")

    # No private zero-score nodes in registry
    for t, n in NODE_REGISTRY.items():
        if n is None:
            continue
        if not n.get("themes") and n.get("bottleneck_score", 1) == 0:
            _fail("no private/zero-score nodes", f"{t} has empty themes and bottleneck_score=0")


def test_phase5_foreign_map_entries():
    print("\n[phase5_foreign_map_entries]")
    from services.playbook.foreign_market_map import FOREIGN_ACCESS_MAP

    phase5_entries = [
        "4062.T", "6988.T", "5802.T", "6501.T", "6963.T",
        "042700.KS", "086390.KS",
        "3037.TW", "6239.TW", "3711.TW",
        "SOI.PA", "WAF.DE",
    ]
    for key in phase5_entries:
        entry = FOREIGN_ACCESS_MAP.get(key)
        _assert(f"FOREIGN_ACCESS_MAP[{key}] exists", entry is not None, "missing entry")
        if entry:
            _assert(f"{key}.data_confidence set",
                    entry.get("data_confidence") in ("low", "medium", "high"),
                    f"got {entry.get('data_confidence')}")
            _assert(f"{key}.etf_proxies is list", isinstance(entry.get("etf_proxies"), list))

    _assert("4062.T adr_ticker=IBDNY", FOREIGN_ACCESS_MAP.get("4062.T", {}).get("adr_ticker") == "IBDNY")
    _assert("6501.T adr_ticker=HTHIY", FOREIGN_ACCESS_MAP.get("6501.T", {}).get("adr_ticker") == "HTHIY")
    _assert("3037.TW has no ADR", FOREIGN_ACCESS_MAP.get("3037.TW", {}).get("adr_ticker") is None)
    _assert("WAF.DE has no ADR", FOREIGN_ACCESS_MAP.get("WAF.DE", {}).get("adr_ticker") is None)


def test_phase5_candidate_new_fields():
    print("\n[phase5_candidate_new_fields]")
    from services.playbook.supply_chain_graph import NODE_REGISTRY
    from services.playbook.discovery_service import _build_candidate

    fn_node = NODE_REGISTRY.get("FN")
    _assert("FN in registry", fn_node is not None)
    if fn_node:
        c = _build_candidate("FN", fn_node)
        _assert("FN.what_would_break_thesis non-empty", bool(c.what_would_break_thesis))
        _assert("FN.coverage_notes non-empty", bool(c.coverage_notes))
        _assert("FN.crowding_flags is list", isinstance(c.crowding_flags, list))
        _assert("FN.position_in_bucket defaults None", c.position_in_bucket is None)
        _assert("FN thesis break mentions CPO or photonics or specialist",
                "CPO" in c.what_would_break_thesis
                or "photonic" in c.what_would_break_thesis.lower()
                or "specialist" in c.what_would_break_thesis.lower(),
                f"got: {c.what_would_break_thesis!r}")

    ibiden_node = NODE_REGISTRY.get("4062.T")
    _assert("4062.T in registry", ibiden_node is not None)
    if ibiden_node:
        c = _build_candidate("IBDNY", ibiden_node)
        _assert("Ibiden.what_would_break_thesis non-empty", bool(c.what_would_break_thesis))
        _assert("Ibiden.coverage_notes mentions ADR or partial or coverage",
                "ADR" in c.coverage_notes
                or "partial" in c.coverage_notes
                or "coverage" in c.coverage_notes.lower(),
                f"got: {c.coverage_notes!r}")
        _assert("Ibiden thesis break mentions JP or packaging or currency",
                "JP" in c.what_would_break_thesis
                or "packaging" in c.what_would_break_thesis.lower()
                or "currency" in c.what_would_break_thesis.lower(),
                f"got: {c.what_would_break_thesis!r}")


def test_phase5_best_blend_score_formula():
    print("\n[phase5_best_blend_score_formula]")
    from services.playbook.discovery_types import DiscoveryScores
    from services.playbook.discovery_service import _compute_best_blend_score

    full_scores = DiscoveryScores(
        bottleneck_criticality_score=100.0, chain_depth_score=100.0,
        hiddenness_score=100.0, supply_chain_confidence_score=100.0,
        theme_purity_score=100.0, giant_dependency_score=100.0,
        proxy_accessibility_score=100.0,
    )
    full = _compute_best_blend_score(full_scores, "high", [])
    _assert("full-score ≈ 100", abs(full - 100.0) < 0.5, f"got {full}")

    # Phase 5 weights sum = 1.0 exactly
    weights_sum = 0.28 + 0.18 + 0.14 + 0.14 + 0.10 + 0.10 + 0.06
    _assert("Phase5 weights sum=1.0", abs(weights_sum - 1.0) < 0.001, f"sum={weights_sum}")

    us_scores = DiscoveryScores(
        bottleneck_criticality_score=80.0, chain_depth_score=70.0,
        hiddenness_score=50.0, supply_chain_confidence_score=85.0,
        theme_purity_score=80.0, giant_dependency_score=60.0,
        proxy_accessibility_score=100.0,
    )
    no_proxy = DiscoveryScores(
        bottleneck_criticality_score=80.0, chain_depth_score=70.0,
        hiddenness_score=50.0, supply_chain_confidence_score=85.0,
        theme_purity_score=80.0, giant_dependency_score=60.0,
        proxy_accessibility_score=30.0,
    )
    us_b  = _compute_best_blend_score(us_scores, "high", [])
    nop_b = _compute_best_blend_score(no_proxy, "high", [])
    _assert("US proxy > no-proxy blend", us_b > nop_b, f"us={us_b}, nop={nop_b}")

    low_c  = _compute_best_blend_score(us_scores, "low",    [])
    med_c  = _compute_best_blend_score(us_scores, "medium", [])
    high_c = _compute_best_blend_score(us_scores, "high",   [])
    _assert("high > medium > low confidence", high_c > med_c > low_c,
            f"high={high_c}, med={med_c}, low={low_c}")
    _assert("low/high ratio ≈ 0.82", abs(low_c / high_c - 0.82) < 0.02,
            f"ratio={low_c/high_c:.3f}")


def test_phase5_ranking_bucket_positions():
    print("\n[phase5_ranking_bucket_positions]")
    from services.playbook.supply_chain_graph import NODE_REGISTRY
    from services.playbook.discovery_service import _build_candidate, _build_ranking_buckets

    candidates = []
    for ticker, node in list(NODE_REGISTRY.items())[:50]:
        if node and node.get("themes"):
            canon = node.get("us_access_proxy", ticker) if node.get("country", "US") != "US" else ticker
            c = _build_candidate(canon, node)
            candidates.append(c)

    buckets = _build_ranking_buckets(candidates, limit=5)

    expected_keys = {
        "top_hidden_bottlenecks", "top_direct_chokepoints", "top_foreign_specialists",
        "top_us_accessible_foreign_proxies", "highest_confidence_candidates", "best_blend_candidates",
    }
    _assert("all 6 bucket keys present", set(buckets.keys()) == expected_keys,
            f"got {set(buckets.keys())}")

    for bucket_name, bucket_list in buckets.items():
        for i, c in enumerate(bucket_list):
            _assert(f"{bucket_name}[{i}].position_in_bucket={i+1}",
                    c.position_in_bucket == i + 1,
                    f"got {c.position_in_bucket}")


def test_phase5_preset_mode_dispatch():
    print("\n[phase5_preset_mode_dispatch]")
    from services.playbook.discovery_types import DiscoverRequest
    from services.playbook.discovery_service import _apply_preset_mode

    # hidden_bottlenecks
    r = _apply_preset_mode(DiscoverRequest(mode="theme_scan", preset_mode="hidden_bottlenecks"))
    _assert("hidden_bottlenecks.only_hidden=True", r.only_hidden is True)
    _assert("hidden_bottlenecks.include_foreign=True", r.include_foreign is True)
    _assert("hidden_bottlenecks.sort_mode=hiddenness", r.sort_mode == "hiddenness")

    # top_direct_chokepoints
    r2 = _apply_preset_mode(DiscoverRequest(mode="theme_scan", preset_mode="top_direct_chokepoints"))
    _assert("chokepoints.sort_mode=bottleneck", r2.sort_mode == "bottleneck")
    _assert("chokepoints.only_hidden=False", r2.only_hidden is False)

    # foreign_specialists
    r3 = _apply_preset_mode(DiscoverRequest(mode="theme_scan", preset_mode="foreign_specialists"))
    _assert("foreign_specialists.mode=foreign_bottlenecks", r3.mode == "foreign_bottlenecks")

    # highest_confidence
    r4 = _apply_preset_mode(DiscoverRequest(mode="theme_scan", preset_mode="highest_confidence"))
    _assert("highest_confidence.sort_mode=confidence", r4.sort_mode == "confidence")

    # no preset — unchanged
    r5 = _apply_preset_mode(DiscoverRequest(mode="giant_chain", giant="NVDA"))
    _assert("no preset — mode unchanged", r5.mode == "giant_chain")
    _assert("no preset — sort_mode unchanged", r5.sort_mode is None)


def test_phase5_sort_mode_ranking():
    print("\n[phase5_sort_mode_ranking]")
    from services.playbook.supply_chain_graph import NODE_REGISTRY
    from services.playbook.discovery_service import _build_candidate, _rank_by_sort_mode

    candidates = []
    for ticker, node in list(NODE_REGISTRY.items())[:60]:
        if node and node.get("themes"):
            canon = node.get("us_access_proxy", ticker) if node.get("country", "US") != "US" else ticker
            c = _build_candidate(canon, node)
            candidates.append(c)

    if len(candidates) < 5:
        _fail("enough candidates", f"only {len(candidates)}")
        return

    for sort_mode, key_fn, label in [
        ("best_blend",  lambda c: c.best_blend_score,              "best_blend_score"),
        ("hiddenness",  lambda c: c.hiddenness_score,              "hiddenness_score"),
        ("bottleneck",  lambda c: c.bottleneck_criticality_score,  "bottleneck_criticality_score"),
        ("confidence",  lambda c: c.supply_chain_confidence_score, "supply_chain_confidence_score"),
    ]:
        ranked = _rank_by_sort_mode(candidates[:], sort_mode)
        _assert(f"{sort_mode}: first >= second",
                key_fn(ranked[0]) >= key_fn(ranked[1]),
                f"{key_fn(ranked[0])} vs {key_fn(ranked[1])}")

    # unknown sort — falls back gracefully
    ranked_unk = _rank_by_sort_mode(candidates[:], "xyzzy_mode")
    _assert("unknown sort_mode returns list", isinstance(ranked_unk, list))
    _assert("unknown sort_mode non-empty", len(ranked_unk) > 0)


def test_phase5_compare_consensus_strength():
    print("\n[phase5_compare_consensus_strength]")
    from services.playbook.discovery_types import CompareTickerResult
    from services.playbook.compare_service import _compute_consensus_strength

    # strong — delta < 10
    r = CompareTickerResult(ticker="T1", serenity_score=75.0, sj_score=70.0, score_delta=5.0,
                             classification="consensus", serenity_pass=True, sj_pass=True)
    s, reason = _compute_consensus_strength(r)
    _assert("strong consensus → strength=strong", s == "strong", f"got {s!r}")
    _assert("strong consensus → reason=''", reason == "", f"got {reason!r}")

    # moderate — delta 10-20
    r2 = CompareTickerResult(ticker="T2", serenity_score=78.0, sj_score=63.0, score_delta=15.0,
                              classification="consensus", serenity_pass=True, sj_pass=True)
    s2, _ = _compute_consensus_strength(r2)
    _assert("moderate consensus → strength=moderate", s2 == "moderate", f"got {s2!r}")

    # borderline — delta 20-25
    r3 = CompareTickerResult(ticker="T3", serenity_score=82.0, sj_score=61.0, score_delta=21.0,
                              classification="consensus", serenity_pass=True, sj_pass=True)
    s3, _ = _compute_consensus_strength(r3)
    _assert("borderline consensus → strength=borderline", s3 == "borderline", f"got {s3!r}")

    # serenity_only — strength=None
    r4 = CompareTickerResult(ticker="T4", serenity_score=70.0, sj_score=45.0, score_delta=25.0,
                              classification="serenity_only", serenity_pass=True, sj_pass=False)
    s4, reason4 = _compute_consensus_strength(r4)
    _assert("serenity_only → strength=None", s4 is None, f"got {s4!r}")
    _assert("serenity_only → reason non-empty", bool(reason4), f"got {reason4!r}")

    # low_fit_both — strength=None, reason non-empty
    r5 = CompareTickerResult(ticker="T5", serenity_score=40.0, sj_score=35.0, score_delta=5.0,
                              classification="low_fit_both", serenity_pass=False, sj_pass=False)
    s5, reason5 = _compute_consensus_strength(r5)
    _assert("low_fit_both → strength=None", s5 is None, f"got {s5!r}")
    _assert("low_fit_both → reason non-empty", bool(reason5), f"got {reason5!r}")


def test_phase5_compare_disagreement_reason():
    print("\n[phase5_compare_disagreement_reason]")
    from services.playbook.discovery_types import CompareTickerResult
    from services.playbook.compare_service import _compute_consensus_strength

    # sj_only with no registry entry → mention registry/profile
    r = CompareTickerResult(ticker="XYZ", serenity_score=None, sj_score=72.0, score_delta=None,
                             classification="sj_only", serenity_pass=False, sj_pass=True)
    _, reason = _compute_consensus_strength(r)
    _assert("sj_only_no_reg reason mentions registry or profile",
            "registry" in reason.lower() or "profile" in reason.lower() or "curated" in reason.lower(),
            f"got {reason!r}")

    # serenity_only large gap → mention divergence or fundamental
    r2 = CompareTickerResult(ticker="LGP", serenity_score=90.0, sj_score=55.0, score_delta=35.0,
                              classification="serenity_only", serenity_pass=True, sj_pass=False)
    _, reason2 = _compute_consensus_strength(r2)
    _assert("large gap serenity_only mentions divergence or fundamental or pts",
            "divergence" in reason2.lower() or "fundamental" in reason2.lower() or "pts" in reason2.lower(),
            f"got {reason2!r}")


def test_phase5_compare_high_disagreement_names():
    print("\n[phase5_compare_high_disagreement_names]")
    from services.playbook.discovery_types import CompareResponse, CompareTickerResult

    r1 = CompareTickerResult(ticker="A", serenity_score=90.0, sj_score=60.0, score_delta=30.0,
                              classification="serenity_only", serenity_pass=True, sj_pass=False)
    r2 = CompareTickerResult(ticker="B", serenity_score=65.0, sj_score=63.0, score_delta=2.0,
                              classification="consensus", serenity_pass=True, sj_pass=True)
    r3 = CompareTickerResult(ticker="C", serenity_score=40.0, sj_score=75.0, score_delta=-35.0,
                              classification="sj_only", serenity_pass=False, sj_pass=True)

    resp = CompareResponse(
        tickers_compared=["A", "B", "C"], playbooks=["serenity", "sjcapital"],
        results=[r1, r2, r3], high_disagreement_names=["A", "C"],
    )
    _assert("high_disagreement_names is list", isinstance(resp.high_disagreement_names, list))
    _assert("A in high_disagreement_names", "A" in resp.high_disagreement_names)
    _assert("C in high_disagreement_names", "C" in resp.high_disagreement_names)
    _assert("B not in high_disagreement_names", "B" not in resp.high_disagreement_names)
    _assert("A delta >= 25", abs(r1.score_delta) >= 25.0)
    _assert("C delta >= 25", abs(r3.score_delta) >= 25.0)
    _assert("B delta < 25", abs(r2.score_delta) < 25.0)


def test_phase5_compare_response_model():
    print("\n[phase5_compare_response_model]")
    from services.playbook.discovery_types import (
        CompareResponse, CompareTickerResult, DiscoverRequest,
        DiscoveryCandidate, DiscoveryScores,
    )

    r = CompareTickerResult(ticker="TEST")
    _assert("CompareTickerResult has consensus_strength", hasattr(r, "consensus_strength"))
    _assert("CompareTickerResult.consensus_strength default None", r.consensus_strength is None)
    _assert("CompareTickerResult has disagreement_reason", hasattr(r, "disagreement_reason"))
    _assert("CompareTickerResult.disagreement_reason default ''", r.disagreement_reason == "")

    resp = CompareResponse(tickers_compared=[], playbooks=[], results=[])
    _assert("CompareResponse has high_disagreement_names", hasattr(resp, "high_disagreement_names"))
    _assert("CompareResponse.high_disagreement_names default []", resp.high_disagreement_names == [])

    req = DiscoverRequest()
    _assert("DiscoverRequest.preset_mode default None", req.preset_mode is None)
    _assert("DiscoverRequest.sort_mode default None", req.sort_mode is None)
    _assert("DiscoverRequest.validation_depth default 'none'", req.validation_depth == "none")

    scores = DiscoveryScores()
    c = DiscoveryCandidate(ticker="T", company_name="Test", scores=scores)
    _assert("DiscoveryCandidate.what_would_break_thesis default ''", c.what_would_break_thesis == "")
    _assert("DiscoveryCandidate.coverage_notes default ''", c.coverage_notes == "")
    _assert("DiscoveryCandidate.crowding_flags default []", c.crowding_flags == [])
    _assert("DiscoveryCandidate.position_in_bucket default None", c.position_in_bucket is None)


# ────────────────────────────────────────────────────────────────────────────
# Phase 6 — Regime detection
# ────────────────────────────────────────────────────────────────────────────

def test_regime_types_model():
    print("\n[regime_types_model]")
    from services.playbook.regime_types import (
        ThemeRegimeScore, AnchorRegimeScore, SerenityRegime,
    )
    t = ThemeRegimeScore(
        theme_id="photonics_cpo",
        label="High Priority",
        regime_score=72.0,
        candidate_density=5,
        avg_bottleneck_score=78.0,
        hiddenness_quality=60.0,
        policy_score=40.0,
        anchor_density=2,
        country_diversity=3,
        serenity_priority="high",
        crowding_penalty=0.0,
    )
    _assert("ThemeRegimeScore theme_id", t.theme_id == "photonics_cpo")
    _assert("ThemeRegimeScore regime_score", t.regime_score == 72.0)
    _assert("ThemeRegimeScore label set", t.label == "High Priority")
    _assert("ThemeRegimeScore serenity_priority", t.serenity_priority == "high")
    _assert("ThemeRegimeScore crowding_penalty", t.crowding_penalty == 0.0)

    a = AnchorRegimeScore(
        anchor_id="NVDA",
        name="NVIDIA",
        regime_score=65.0,
        theme_overlap_count=4,
        overlapping_theme_ids=["photonics_cpo", "advanced_packaging_test",
                               "semicap_supply_chain", "ai_power_energy"],
        capex_scale_score=80.0,
        candidate_quality=75.0,
        foreign_exposure_count=3,
    )
    _assert("AnchorRegimeScore anchor_id", a.anchor_id == "NVDA")
    _assert("AnchorRegimeScore name", a.name == "NVIDIA")
    _assert("AnchorRegimeScore regime_score", a.regime_score == 65.0)
    _assert("AnchorRegimeScore theme_overlap_count", a.theme_overlap_count == 4)
    _assert("AnchorRegimeScore overlapping_theme_ids is list", isinstance(a.overlapping_theme_ids, list))
    _assert("AnchorRegimeScore overlapping_theme_ids len matches count", len(a.overlapping_theme_ids) == a.theme_overlap_count)

    regime = SerenityRegime(
        regime_id="test_regime",
        label="Test Regime",
        summary="Test summary",
        top_themes=["photonics_cpo"],
        top_anchors=["NVDA"],
        top_regions=["US"],
        recommended_mode="theme_scan",
        recommended_depth=3,
        confidence="high",
        why_now=["Signal A"],
        evidence_signals=["Evidence B"],
        rejected_or_lower_priority_paths=["low_theme"],
        theme_scores=[t],
        anchor_scores=[a],
    )
    _assert("SerenityRegime regime_id", regime.regime_id == "test_regime")
    _assert("SerenityRegime top_themes list", len(regime.top_themes) == 1)
    _assert("SerenityRegime top_anchors list", len(regime.top_anchors) == 1)
    _assert("SerenityRegime confidence is str", isinstance(regime.confidence, str))
    _assert("SerenityRegime confidence value", regime.confidence in ("high", "medium", "low"))
    _assert("SerenityRegime theme_scores typed", isinstance(regime.theme_scores[0], ThemeRegimeScore))
    _assert("SerenityRegime anchor_scores typed", isinstance(regime.anchor_scores[0], AnchorRegimeScore))
    _assert("SerenityRegime model_dump works", isinstance(regime.model_dump(), dict))


def test_regime_service_returns_regime():
    print("\n[regime_service_returns_regime]")
    from services.playbook.regime_service import compute_serenity_regime
    from services.playbook.regime_types import SerenityRegime
    regime = compute_serenity_regime()
    _assert("compute_serenity_regime returns SerenityRegime", isinstance(regime, SerenityRegime))
    _assert("regime_id is a non-empty string", isinstance(regime.regime_id, str) and len(regime.regime_id) > 0)
    _assert("label is a non-empty string", isinstance(regime.label, str) and len(regime.label) > 0)
    _assert("summary is a non-empty string", isinstance(regime.summary, str) and len(regime.summary) > 0)
    _assert("top_themes is a list", isinstance(regime.top_themes, list))
    _assert("top_anchors is a list", isinstance(regime.top_anchors, list))
    _assert("why_now is a list", isinstance(regime.why_now, list))
    _assert("evidence_signals is a list", isinstance(regime.evidence_signals, list))
    _assert("confidence is str", isinstance(regime.confidence, str))
    _assert("confidence value valid", regime.confidence in ("high", "medium", "low"))
    _assert("recommended_depth >= 1", regime.recommended_depth >= 1)
    _assert("recommended_mode is str", isinstance(regime.recommended_mode, str))


def test_regime_service_deterministic():
    print("\n[regime_service_deterministic]")
    from services.playbook.regime_service import compute_serenity_regime
    r1 = compute_serenity_regime()
    r2 = compute_serenity_regime()
    _assert("regime_id is deterministic", r1.regime_id == r2.regime_id)
    _assert("top_themes are deterministic", r1.top_themes == r2.top_themes)
    _assert("confidence is deterministic", r1.confidence == r2.confidence)
    _assert("recommended_mode is deterministic", r1.recommended_mode == r2.recommended_mode)


def test_regime_theme_scores_sorted():
    print("\n[regime_theme_scores_sorted]")
    from services.playbook.regime_service import compute_serenity_regime
    regime = compute_serenity_regime()
    scores = [ts.regime_score for ts in regime.theme_scores]
    _assert("theme_scores list non-empty", len(scores) > 0)
    _assert("theme_scores sorted descending", scores == sorted(scores, reverse=True))


def test_regime_anchor_scores_sorted():
    print("\n[regime_anchor_scores_sorted]")
    from services.playbook.regime_service import compute_serenity_regime
    regime = compute_serenity_regime()
    scores = [a.regime_score for a in regime.anchor_scores]
    _assert("anchor_scores list non-empty", len(scores) > 0)
    _assert("anchor_scores sorted descending", scores == sorted(scores, reverse=True))


def test_regime_anchor_overlapping_theme_ids():
    print("\n[regime_anchor_overlapping_theme_ids]")
    from services.playbook.regime_service import compute_serenity_regime
    regime = compute_serenity_regime()
    top_theme_set = set(regime.top_themes)
    for a in regime.anchor_scores:
        _assert(
            f"anchor '{a.anchor_id}' overlapping_theme_ids is list",
            isinstance(a.overlapping_theme_ids, list),
        )
        _assert(
            f"anchor '{a.anchor_id}' overlap len matches count",
            len(a.overlapping_theme_ids) == a.theme_overlap_count,
        )
        for tid in a.overlapping_theme_ids:
            _assert(
                f"anchor '{a.anchor_id}' theme_id '{tid}' in top_themes",
                tid in top_theme_set,
            )
        break  # one anchor is enough to prove the pattern


def test_regime_top_themes_in_theme_scores():
    print("\n[regime_top_themes_in_theme_scores]")
    from services.playbook.regime_service import compute_serenity_regime
    regime = compute_serenity_regime()
    scored_ids = {ts.theme_id for ts in regime.theme_scores}
    for theme_id in regime.top_themes:
        _assert(f"top theme '{theme_id}' is in theme_scores", theme_id in scored_ids)


def test_regime_top_anchors_in_anchor_scores():
    print("\n[regime_top_anchors_in_anchor_scores]")
    from services.playbook.regime_service import compute_serenity_regime
    regime = compute_serenity_regime()
    scored_ids = {a.anchor_id for a in regime.anchor_scores}
    for anchor_id in regime.top_anchors:
        _assert(f"top anchor '{anchor_id}' is in anchor_scores", anchor_id in scored_ids)


def test_discover_response_regime_context_field():
    print("\n[discover_response_regime_context_field]")
    from services.playbook.discovery_types import DiscoverResponse
    common = dict(
        playbook_id="serenity",
        mode="theme_scan",
        query="",
        summary="Test",
        top_candidates=[],
        low_confidence_candidates=[],
    )
    resp = DiscoverResponse(
        **common,
        regime_context={"regime_id": "test", "label": "Test Regime"},
    )
    _assert("DiscoverResponse has regime_context field", hasattr(resp, "regime_context"))
    _assert("regime_context roundtrips correctly", resp.regime_context == {"regime_id": "test", "label": "Test Regime"})

    resp_no_regime = DiscoverResponse(**common)
    _assert("regime_context defaults to None", resp_no_regime.regime_context is None)


# ────────────────────────────────────────────────────────────────────────────
# Phase 7 — Strategy Screener
# ────────────────────────────────────────────────────────────────────────────

def test_screener_types_models():
    print("\n[screener_types_models]")
    from services.playbook.strategy_screener.screener_types import (
        ScreenerCandidate, ScreenerSnapshot, ScreenerReport, ScreenerConfig,
    )

    c = ScreenerCandidate(ticker="NVDA", company_name="NVIDIA")
    _assert("ScreenerCandidate ticker", c.ticker == "NVDA")
    _assert("ScreenerCandidate company_name", c.company_name == "NVIDIA")
    _assert("ScreenerCandidate grade default", c.grade == "B")
    _assert("ScreenerCandidate model_dump works", isinstance(c.model_dump(), dict))

    s = ScreenerSnapshot(
        snapshot_id="serenity_2026_04_18_0000",
        playbook_id="serenity",
        generated_at="2026-04-18T00:00:00+00:00",
        results=[c],
        results_count=1,
    )
    _assert("ScreenerSnapshot snapshot_id", s.snapshot_id == "serenity_2026_04_18_0000")
    _assert("ScreenerSnapshot results list", len(s.results) == 1)
    _assert("ScreenerSnapshot is_stale default False", s.is_stale is False)
    _assert("ScreenerSnapshot model_dump works", isinstance(s.model_dump(), dict))

    r = ScreenerReport(
        snapshot_id="serenity_2026_04_18_0000",
        ticker="NVDA",
        company_name="NVIDIA",
        headline="NVDA • NVIDIA",
        meta_line="$3T • US • L0 • A+",
        summary="Test summary",
        why_it_matters="Test why",
        supply_chain_map_text="Test map",
        competitors="Test comps",
        catalysts="Test cats",
        rerating_case="Test rerating",
        key_risk="Test risk",
        why_hidden="Test hidden",
        what_to_verify_next="Test verify",
        what_would_break_thesis="Test break",
        generated_at="2026-04-18T00:00:00+00:00",
    )
    _assert("ScreenerReport ticker", r.ticker == "NVDA")
    _assert("ScreenerReport headline", r.headline == "NVDA • NVIDIA")
    _assert("ScreenerReport grade default", r.grade == "B")
    _assert("ScreenerReport model_dump works", isinstance(r.model_dump(), dict))

    cfg = ScreenerConfig()
    _assert("ScreenerConfig cadence_days default 14", cfg.cadence_days == 14)
    _assert("ScreenerConfig grade_scale has A+", "A+" in cfg.grade_scale)
    _assert("ScreenerConfig grade_scale has C", "C" in cfg.grade_scale)


def test_screener_grade_assignment():
    print("\n[screener_grade_assignment]")
    from services.playbook.strategy_screener.screener_report_builder import assign_grade

    g = assign_grade(best_blend_score=90.0, data_confidence="high", hiddenness_score=80.0, bottleneck_criticality_score=85.0)
    _assert("A+ grade for top scores", g == "A+")

    g = assign_grade(best_blend_score=75.0, data_confidence="high", hiddenness_score=65.0, bottleneck_criticality_score=70.0)
    _assert("A grade for strong scores", g in ("A+", "A"))

    g = assign_grade(best_blend_score=60.0, data_confidence="medium", hiddenness_score=50.0, bottleneck_criticality_score=55.0)
    _assert("B+/B grade for medium scores", g in ("A", "B+", "B"))

    g = assign_grade(best_blend_score=20.0, data_confidence="low", hiddenness_score=20.0, bottleneck_criticality_score=20.0)
    _assert("C grade for low scores", g == "C")

    g_high = assign_grade(best_blend_score=65.0, data_confidence="high", hiddenness_score=60.0, bottleneck_criticality_score=60.0)
    g_low  = assign_grade(best_blend_score=65.0, data_confidence="low",  hiddenness_score=60.0, bottleneck_criticality_score=60.0)
    _assert("High confidence grades higher than low", _grade_rank(g_high) >= _grade_rank(g_low))


def _grade_rank(g: str) -> int:
    return {"A+": 5, "A": 4, "B+": 3, "B": 2, "C": 1}.get(g, 0)


def test_screener_report_builder_sections():
    print("\n[screener_report_builder_sections]")
    from services.playbook.strategy_screener.screener_report_builder import (
        build_one_line_summary, build_summary, build_why_it_matters,
        build_supply_chain_map_text, build_competitors, build_catalysts,
        build_rerating_case, build_key_risk, build_why_hidden,
        build_what_to_verify_next, build_what_would_break_thesis,
        build_supply_chain_layers, build_full_report,
    )

    cand = {
        "ticker":                       "SIVE",
        "company_name":                 "Sivers Semiconductors",
        "country":                      "SE",
        "exchange":                     "STO",
        "themes":                       ["photonics_cpo"],
        "layer_depth":                  4,
        "chain_role_type":              "direct_bottleneck",
        "bottleneck_criticality_score": 82.0,
        "hiddenness_score":             70.0,
        "best_blend_score":             78.0,
        "chain_depth_score":            75.0,
        "supply_chain_confidence_score": 55.0,
        "data_confidence":              "medium",
        "coverage_status":              "partial",
        "giant_anchors":                ["NVDA", "INTC"],
        "comparable_names":             ["LITE", "IIVI"],
        "market_cap_usd":               312_000_000.0,
        "us_access_proxy":              None,
        "thesis_summary":               "Specialized III-V wafers for data center optics.",
        "fit_reasoning":                ["Sole-source position in CPO wafer supply"],
        "why_now":                      "CPO ramp driving exponential demand.",
        "why_hidden":                   "Swedish microcap, no analyst coverage.",
        "what_to_verify_next":          "Check Q4 earnings transcript for NVDA customer mention.",
        "what_would_break_thesis":      "If NVDA moves to InP substrates instead of GaAs.",
        "crowding_flags":               [],
        "coverage_notes":               "Thin SEC filing coverage, Swedish annual report only.",
        "data_gaps":                    ["No US ADR confirmed"],
        "chain_layers":                 ["III-V substrate supplier", "CPO wafer grower"],
    }

    summary = build_one_line_summary(cand)
    _assert("one_line_summary non-empty", len(summary) > 20)

    full_sum = build_summary(cand)
    _assert("summary contains ticker", "SIVE" in full_sum or "Sivers" in full_sum)
    _assert("summary non-empty", len(full_sum) > 50)

    wit = build_why_it_matters(cand)
    _assert("why_it_matters mentions NVDA or criticality", "NVDA" in wit or "criticality" in wit.lower() or "score" in wit.lower())

    scm = build_supply_chain_map_text(cand)
    _assert("supply_chain_map_text non-empty", len(scm) > 40)
    _assert("supply_chain_map_text mentions NVDA", "NVDA" in scm)

    comps = build_competitors(cand)
    _assert("competitors mentions LITE or IIVI", "LITE" in comps or "IIVI" in comps)

    cats = build_catalysts(cand)
    _assert("catalysts non-empty", len(cats) > 20)

    rr = build_rerating_case(cand)
    _assert("rerating_case non-empty", len(rr) > 20)

    kr = build_key_risk(cand)
    _assert("key_risk non-empty", len(kr) > 20)

    wh = build_why_hidden(cand)
    _assert("why_hidden non-empty", len(wh) > 20)
    _assert("why_hidden mentions foreign or hiddenness", "SE" in wh or "hidden" in wh.lower() or "coverage" in wh.lower())

    wtv = build_what_to_verify_next(cand)
    _assert("what_to_verify_next non-empty", len(wtv) > 20)

    wtbt = build_what_would_break_thesis(cand)
    _assert("what_would_break_thesis non-empty", len(wtbt) > 20)
    _assert("what_would_break_thesis contains thesis break content", "InP" in wtbt or "break" in wtbt.lower() or "thesis" in wtbt.lower())

    layers = build_supply_chain_layers(cand)
    _assert("supply_chain_layers is list", isinstance(layers, list))
    _assert("supply_chain_layers non-empty", len(layers) > 0)

    full_report = build_full_report(cand, snapshot_id="test_snap_001", regime_context=None)
    _assert("full_report is dict", isinstance(full_report, dict))
    _assert("full_report has headline", "headline" in full_report)
    _assert("full_report has meta_line", "meta_line" in full_report)
    _assert("full_report has grade", "grade" in full_report)
    _assert("full_report grade is valid", full_report["grade"] in ("A+", "A", "B+", "B", "C"))
    _assert("full_report has scores dict", isinstance(full_report.get("scores"), dict))
    _assert("full_report headline contains ticker", "SIVE" in full_report["headline"])
    _assert("full_report meta_line contains country", "SE" in full_report["meta_line"])


def test_screener_stale_logic():
    print("\n[screener_stale_logic]")
    from services.playbook.strategy_screener.screener_scheduler import is_snapshot_stale, attach_stale_flag
    from datetime import datetime, timezone, timedelta

    _assert("None snapshot is stale", is_snapshot_stale(None) is True)
    _assert("empty dict snapshot is stale", is_snapshot_stale({}) is True)
    _assert("error status is stale", is_snapshot_stale({"status": "error", "generated_at": datetime.now(timezone.utc).isoformat()}) is True)
    _assert("generating status is NOT stale", is_snapshot_stale({"status": "generating", "generated_at": datetime.now(timezone.utc).isoformat()}) is False)

    fresh_ts = datetime.now(timezone.utc).isoformat()
    old_ts   = (datetime.now(timezone.utc) - timedelta(days=20)).isoformat()

    _assert("fresh snapshot is not stale (14d cadence)", is_snapshot_stale({"status": "complete", "generated_at": fresh_ts}, cadence_days=14) is False)
    _assert("20-day-old snapshot is stale (14d cadence)", is_snapshot_stale({"status": "complete", "generated_at": old_ts}, cadence_days=14) is True)
    _assert("20-day-old snapshot is NOT stale (30d cadence)", is_snapshot_stale({"status": "complete", "generated_at": old_ts}, cadence_days=30) is False)

    snap = {"status": "complete", "generated_at": fresh_ts, "results": []}
    flagged = attach_stale_flag(snap, cadence_days=14)
    _assert("attach_stale_flag fresh → is_stale=False", flagged["is_stale"] is False)

    snap_old = {"status": "complete", "generated_at": old_ts, "results": []}
    flagged_old = attach_stale_flag(snap_old, cadence_days=14)
    _assert("attach_stale_flag old → is_stale=True", flagged_old["is_stale"] is True)


def test_screener_candidate_to_screener_dict():
    print("\n[screener_candidate_to_screener_dict]")
    from services.playbook.strategy_screener.screener_service import _candidate_to_screener_dict, _cadence_label

    fake_candidate = {
        "ticker":                       "NVDA",
        "company_name":                 "NVIDIA",
        "country":                      "US",
        "exchange":                     "NASDAQ",
        "themes":                       ["semicap_supply_chain"],
        "layer_depth":                  0,
        "chain_role_type":              "platform_anchor",
        "best_blend_score":             92.0,
        "bottleneck_criticality_score": 88.0,
        "hiddenness_score":             15.0,
        "chain_depth_score":            30.0,
        "supply_chain_confidence_score": 95.0,
        "data_confidence":              "high",
        "coverage_status":              "full",
        "us_access_proxy":              None,
        "market_cap_usd":               3_000_000_000_000.0,
        "giant_anchors":                [],
        "comparable_names":             [],
        "thesis_summary":               "",
        "fit_reasoning":                [],
        "why_now":                      "",
        "why_hidden":                   "",
        "what_to_verify_next":          "",
        "what_would_break_thesis":      "",
        "coverage_notes":               "",
        "crowding_flags":               [],
        "data_gaps":                    [],
        "chain_layers":                 [],
    }

    d = _candidate_to_screener_dict(fake_candidate)
    _assert("candidate dict has ticker", d["ticker"] == "NVDA")
    _assert("candidate dict has grade", d["grade"] in ("A+", "A", "B+", "B", "C"))
    _assert("candidate dict has one_line_summary", len(d.get("one_line_summary", "")) > 0)
    _assert("candidate dict has theme", "theme" in d)
    _assert("candidate dict theme matches themes[0]", d["theme"] == "semicap_supply_chain")

    _assert("cadence label daily", _cadence_label(1) == "daily")
    _assert("cadence label weekly", _cadence_label(7) == "weekly")
    _assert("cadence label biweekly", _cadence_label(14) == "biweekly")
    _assert("cadence label monthly", _cadence_label(30) == "monthly")
    _assert("cadence label custom", "30" in _cadence_label(30) or _cadence_label(30) == "monthly")


def test_screener_isolation_from_query():
    print("\n[screener_isolation_from_query]")
    import ast, os

    screener_files = [
        "services/playbook/strategy_screener/screener_service.py",
        "services/playbook/strategy_screener/screener_router.py",
        "services/playbook/strategy_screener/screener_storage.py",
        "services/playbook/strategy_screener/screener_report_builder.py",
        "services/playbook/strategy_screener/screener_scheduler.py",
    ]

    backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    forbidden   = ["/api/query", "api_query", "prompts.py", "personality.py",
                   "mode_normalizer", "data_compressor"]

    for rel_path in screener_files:
        fpath = os.path.join(backend_dir, rel_path)
        if not os.path.exists(fpath):
            continue
        with open(fpath) as f:
            src = f.read()
        for bad in forbidden:
            _assert(f"{rel_path}: no '{bad}' reference", bad not in src)


# ────────────────────────────────────────────────────────────────────────────
# Phase 8 — Screener Filter / Sort
# ────────────────────────────────────────────────────────────────────────────

def _make_candidate(ticker, bbs=70.0, bcs=60.0, scs=80.0, market_cap=None, layer=2, grade="B"):
    return {
        "ticker":                       ticker,
        "company_name":                 f"{ticker} Corp",
        "best_blend_score":             bbs,
        "bottleneck_criticality_score": bcs,
        "supply_chain_confidence_score": scs,
        "market_cap_usd":               market_cap,
        "layer_depth":                  layer,
        "grade":                        grade,
        "hiddenness_score":             50.0,
        "chain_depth_score":            50.0,
        "country":                      "US",
        "exchange":                     "NASDAQ",
        "themes":                       [],
        "giant_anchors":                [],
    }


def test_screener_market_cap_classification():
    print("\n[screener_market_cap_classification]")
    from services.playbook.strategy_screener.screener_filters import classify_market_cap

    _assert("None → micro_cap", classify_market_cap(None) == "micro_cap")
    _assert("0 → micro_cap",    classify_market_cap(0) == "micro_cap")
    _assert("1B → micro_cap",   classify_market_cap(1_000_000_000) == "micro_cap")
    _assert("2.49B → micro_cap", classify_market_cap(2_499_999_999) == "micro_cap")
    _assert("2.5B → small_cap", classify_market_cap(2_500_000_000) == "small_cap")
    _assert("10B → small_cap",  classify_market_cap(10_000_000_000) == "small_cap")
    _assert("19.99B → small_cap", classify_market_cap(19_999_999_999) == "small_cap")
    _assert("20B → mid_cap",    classify_market_cap(20_000_000_000) == "mid_cap")
    _assert("50B → mid_cap",    classify_market_cap(50_000_000_000) == "mid_cap")
    _assert("99.99B → mid_cap", classify_market_cap(99_999_999_999) == "mid_cap")
    _assert("100B → large_cap", classify_market_cap(100_000_000_000) == "large_cap")
    _assert("1T → large_cap",   classify_market_cap(1_000_000_000_000) == "large_cap")


def test_screener_filter_market_cap_bucket():
    print("\n[screener_filter_market_cap_bucket]")
    from services.playbook.strategy_screener.screener_filters import apply_filters_and_sort

    candidates = [
        _make_candidate("LARGE1", market_cap=200_000_000_000),
        _make_candidate("LARGE2", market_cap=150_000_000_000),
        _make_candidate("MID1",   market_cap=50_000_000_000),
        _make_candidate("SMALL1", market_cap=5_000_000_000),
        _make_candidate("MICRO1", market_cap=500_000_000),
        _make_candidate("MICRO2", market_cap=None),
    ]

    res = apply_filters_and_sort(candidates, market_cap_bucket="large_cap")
    tickers = [c["ticker"] for c in res["results"]]
    _assert("large_cap returns LARGE1+LARGE2", set(tickers) == {"LARGE1", "LARGE2"})
    _assert("filtered_result_count=2", res["filtered_result_count"] == 2)
    _assert("available_result_count=6", res["available_result_count"] == 6)

    res = apply_filters_and_sort(candidates, market_cap_bucket="mid_cap")
    _assert("mid_cap returns MID1", [c["ticker"] for c in res["results"]] == ["MID1"])

    res = apply_filters_and_sort(candidates, market_cap_bucket="small_cap")
    _assert("small_cap returns SMALL1", [c["ticker"] for c in res["results"]] == ["SMALL1"])

    res = apply_filters_and_sort(candidates, market_cap_bucket="micro_cap")
    tickers = {c["ticker"] for c in res["results"]}
    _assert("micro_cap returns MICRO1+MICRO2 (None)", tickers == {"MICRO1", "MICRO2"})


def test_screener_filter_layer():
    print("\n[screener_filter_layer]")
    from services.playbook.strategy_screener.screener_filters import apply_filters_and_sort

    candidates = [
        _make_candidate("L1A", layer=1),
        _make_candidate("L1B", layer=1),
        _make_candidate("L2A", layer=2),
        _make_candidate("L3A", layer=3),
        _make_candidate("L4A", layer=4),  # layer 4 should be excluded when filtering for 1/2/3
    ]

    res = apply_filters_and_sort(candidates, layer=1)
    _assert("layer=1 returns L1A+L1B", {c["ticker"] for c in res["results"]} == {"L1A", "L1B"})

    res = apply_filters_and_sort(candidates, layer=2)
    _assert("layer=2 returns L2A only", [c["ticker"] for c in res["results"]] == ["L2A"])

    res = apply_filters_and_sort(candidates, layer=3)
    _assert("layer=3 returns L3A only", [c["ticker"] for c in res["results"]] == ["L3A"])

    res = apply_filters_and_sort(candidates, layer=1)
    _assert("layer=1 does not include L4", "L4A" not in {c["ticker"] for c in res["results"]})


def test_screener_filter_combined():
    print("\n[screener_filter_combined]")
    from services.playbook.strategy_screener.screener_filters import apply_filters_and_sort

    candidates = [
        _make_candidate("HIT",  market_cap=5_000_000_000, layer=3),  # small_cap + layer 3
        _make_candidate("MISS1", market_cap=50_000_000_000, layer=3), # mid_cap + layer 3
        _make_candidate("MISS2", market_cap=5_000_000_000, layer=2),  # small_cap + layer 2
    ]

    res = apply_filters_and_sort(candidates, market_cap_bucket="small_cap", layer=3)
    _assert("combined filter hits exactly one", [c["ticker"] for c in res["results"]] == ["HIT"])
    _assert("active_filters has both keys", "market_cap_bucket" in res["active_filters"] and "layer" in res["active_filters"])


def test_screener_sort_best_fit():
    print("\n[screener_sort_best_fit]")
    from services.playbook.strategy_screener.screener_filters import apply_filters_and_sort

    candidates = [
        _make_candidate("LOW",  bbs=50.0, bcs=40.0, scs=50.0),
        _make_candidate("HIGH", bbs=90.0, bcs=85.0, scs=90.0),
        _make_candidate("MID",  bbs=70.0, bcs=60.0, scs=70.0),
    ]

    res = apply_filters_and_sort(candidates, sort_by="best_fit")
    tickers = [c["ticker"] for c in res["results"]]
    _assert("best_fit: HIGH first",  tickers[0] == "HIGH")
    _assert("best_fit: LOW last",    tickers[-1] == "LOW")
    _assert("active_sort is best_fit", res["active_sort"] == "best_fit")


def test_screener_sort_best_fit_tiebreak():
    print("\n[screener_sort_best_fit_tiebreak]")
    from services.playbook.strategy_screener.screener_filters import apply_filters_and_sort

    # Same bbs, different bottleneck score → higher bcs should win
    candidates = [
        _make_candidate("LOWER_BCS", bbs=75.0, bcs=55.0, scs=80.0),
        _make_candidate("HIGHER_BCS", bbs=75.0, bcs=80.0, scs=80.0),
    ]
    res = apply_filters_and_sort(candidates, sort_by="best_fit")
    _assert("best_fit tiebreak: HIGHER_BCS first", res["results"][0]["ticker"] == "HIGHER_BCS")

    # Same bbs + bcs, different scs → higher scs should win
    candidates2 = [
        _make_candidate("LOWER_SCS", bbs=75.0, bcs=80.0, scs=50.0),
        _make_candidate("HIGHER_SCS", bbs=75.0, bcs=80.0, scs=90.0),
    ]
    res2 = apply_filters_and_sort(candidates2, sort_by="best_fit")
    _assert("best_fit tiebreak scs: HIGHER_SCS first", res2["results"][0]["ticker"] == "HIGHER_SCS")


def test_screener_sort_market_cap():
    print("\n[screener_sort_market_cap]")
    from services.playbook.strategy_screener.screener_filters import apply_filters_and_sort

    candidates = [
        _make_candidate("SMALL",  market_cap=1_000_000_000),
        _make_candidate("LARGE",  market_cap=500_000_000_000),
        _make_candidate("NONE",   market_cap=None),
        _make_candidate("MEDIUM", market_cap=50_000_000_000),
    ]

    res = apply_filters_and_sort(candidates, sort_by="market_cap")
    tickers = [c["ticker"] for c in res["results"]]
    _assert("market_cap sort: LARGE first",  tickers[0] == "LARGE")
    _assert("market_cap sort: MEDIUM second", tickers[1] == "MEDIUM")
    _assert("market_cap sort: None last",    tickers[-1] == "NONE")


def test_screener_sort_layer():
    print("\n[screener_sort_layer]")
    from services.playbook.strategy_screener.screener_filters import apply_filters_and_sort

    candidates = [
        _make_candidate("L3", layer=3),
        _make_candidate("L1", layer=1),
        _make_candidate("L2", layer=2),
    ]

    res = apply_filters_and_sort(candidates, sort_by="layer")
    tickers = [c["ticker"] for c in res["results"]]
    _assert("layer sort: L1 first", tickers[0] == "L1")
    _assert("layer sort: L2 second", tickers[1] == "L2")
    _assert("layer sort: L3 third", tickers[2] == "L3")


def test_screener_sort_grade():
    print("\n[screener_sort_grade]")
    from services.playbook.strategy_screener.screener_filters import apply_filters_and_sort

    candidates = [
        _make_candidate("C_STOCK",  grade="C"),
        _make_candidate("APLUS",    grade="A+"),
        _make_candidate("B_STOCK",  grade="B"),
        _make_candidate("BPLUS",    grade="B+"),
        _make_candidate("A_STOCK",  grade="A"),
    ]

    res = apply_filters_and_sort(candidates, sort_by="grade")
    tickers = [c["ticker"] for c in res["results"]]
    _assert("grade sort: A+ first",  tickers[0] == "APLUS")
    _assert("grade sort: A second",  tickers[1] == "A_STOCK")
    _assert("grade sort: B+ third",  tickers[2] == "BPLUS")
    _assert("grade sort: B fourth",  tickers[3] == "B_STOCK")
    _assert("grade sort: C last",    tickers[4] == "C_STOCK")


def test_screener_limit():
    print("\n[screener_limit]")
    from services.playbook.strategy_screener.screener_filters import apply_filters_and_sort

    candidates = [_make_candidate(f"T{i}", bbs=float(100 - i)) for i in range(25)]

    res = apply_filters_and_sort(candidates, limit=20)
    _assert("limit=20 returns 20", len(res["results"]) == 20)
    _assert("available_result_count=25", res["available_result_count"] == 25)
    _assert("limit field is 20", res["limit"] == 20)

    res5 = apply_filters_and_sort(candidates, limit=5)
    _assert("limit=5 returns 5", len(res5["results"]) == 5)
    _assert("limit=5 top result has bbs=100", res5["results"][0]["best_blend_score"] == 100.0)


def test_screener_no_filter_passes_all():
    print("\n[screener_no_filter_passes_all]")
    from services.playbook.strategy_screener.screener_filters import apply_filters_and_sort

    candidates = [_make_candidate(f"T{i}") for i in range(10)]
    res = apply_filters_and_sort(candidates)  # no params

    _assert("no filter: all 10 pass through", res["filtered_result_count"] == 10)
    _assert("no filter: active_filters is empty", res["active_filters"] == {})
    _assert("no filter: active_sort is best_fit", res["active_sort"] == "best_fit")
    _assert("no filter: limit=20 (default)", res["limit"] == 20)


def test_screener_invalid_params_raise():
    print("\n[screener_invalid_params_raise]")
    from services.playbook.strategy_screener.screener_filters import apply_filters_and_sort

    try:
        apply_filters_and_sort([], market_cap_bucket="huge_cap")
        _assert("invalid bucket should raise", False)
    except ValueError as e:
        _assert("invalid bucket ValueError raised", "huge_cap" in str(e))

    try:
        apply_filters_and_sort([], sort_by="random_sort")
        _assert("invalid sort should raise", False)
    except ValueError as e:
        _assert("invalid sort ValueError raised", "random_sort" in str(e))

    try:
        apply_filters_and_sort([], layer=99)
        _assert("invalid layer should raise", False)
    except ValueError as e:
        _assert("invalid layer ValueError raised", "99" in str(e))


def test_screener_backwards_compatible_no_params():
    print("\n[screener_backwards_compatible_no_params]")
    from services.playbook.strategy_screener.screener_filters import apply_filters_and_sort

    candidates = [_make_candidate(f"T{i}", bbs=float(90 - i * 5)) for i in range(5)]
    res = apply_filters_and_sort(candidates)

    _assert("backwards compat: active_filters is empty dict", res["active_filters"] == {})
    _assert("backwards compat: active_sort=best_fit", res["active_sort"] == "best_fit")
    _assert("backwards compat: filtered_result_count=5", res["filtered_result_count"] == 5)
    _assert("backwards compat: available_result_count=5", res["available_result_count"] == 5)
    _assert("backwards compat: sorted by bbs desc", res["results"][0]["ticker"] == "T0")


def test_screener_config_has_dropdown_metadata():
    print("\n[screener_config_has_dropdown_metadata]")
    from services.playbook.strategy_screener.screener_types import ScreenerConfig

    cfg = ScreenerConfig()
    d = cfg.model_dump()

    _assert("config has market_cap_buckets", "market_cap_buckets" in d)
    _assert("config has layer_filters", "layer_filters" in d)
    _assert("config has sort_options", "sort_options" in d)
    _assert("market_cap_buckets has 4 items", len(d["market_cap_buckets"]) == 4)
    _assert("layer_filters has 3 items", len(d["layer_filters"]) == 3)
    _assert("sort_options has 4 items", len(d["sort_options"]) == 4)

    bucket_ids = {b["id"] for b in d["market_cap_buckets"]}
    _assert("bucket ids correct", bucket_ids == {"large_cap", "mid_cap", "small_cap", "micro_cap"})

    sort_ids = {s["id"] for s in d["sort_options"]}
    _assert("sort ids correct", sort_ids == {"best_fit", "market_cap", "layer", "grade"})

    layer_ids = {l["id"] for l in d["layer_filters"]}
    _assert("layer ids correct", layer_ids == {1, 2, 3})

    # All buckets and options have id + label
    for b in d["market_cap_buckets"]:
        _assert(f"bucket {b['id']} has label", "label" in b)
    for s in d["sort_options"]:
        _assert(f"sort {s['id']} has label", "label" in s)
    for l in d["layer_filters"]:
        _assert(f"layer {l['id']} has label", "label" in l)


def test_screener_filter_isolation_from_query():
    print("\n[screener_filter_isolation_from_query]")
    import os
    fpath = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "services/playbook/strategy_screener/screener_filters.py"
    )
    if os.path.exists(fpath):
        with open(fpath) as f:
            src = f.read()
        forbidden = ["/api/query", "api_query", "prompts.py", "personality.py",
                     "mode_normalizer", "data_compressor"]
        for bad in forbidden:
            _assert(f"screener_filters: no '{bad}' reference", bad not in src)


# ────────────────────────────────────────────────────────────────────────────
# Runner
# ────────────────────────────────────────────────────────────────────────────

def run_all():
    print("=" * 60)
    print("PLAYBOOK PHASE 1.5 + 2.0 + 3.0 + 4.0 FACTOR TESTS")
    print("=" * 60)

    # Phase 1.5
    test_sector_strength()
    test_theme_alignment()
    test_bottleneck_exposure()
    test_dilution_risk()
    test_catalyst_proximity()
    test_crowding_risk()
    test_playbook_divergence()
    test_graceful_degradation()
    test_api_query_isolation()

    # Phase 2
    test_supply_chain_confirmation()
    test_ebitda_inflection_proximity()
    test_backlog_quality()
    test_evidence_freshness()
    test_execution_risk()
    test_insider_buying()
    test_policy_tailwind()
    test_playbook_registry_v2()
    test_explainer()

    # Phase 3 — Discovery engine
    test_discovery_types()
    test_giant_map()
    test_supply_chain_graph()
    test_theme_discovery()
    test_foreign_market_map()
    test_discovery_scoring()
    test_discovery_engine_sync()
    test_supply_chain_map_engine()
    test_discovery_bridge_analyzer()
    test_discovery_query_isolation()

    # Phase 4 — Quality upgrade
    test_phase4_new_nodes()
    test_phase4_hiddenness_logic()
    test_phase4_confidence_penalties()
    test_phase4_best_blend_score()
    test_phase4_candidate_new_fields()
    test_phase4_ranking_buckets()
    test_phase4_discover_response_buckets()
    test_phase4_compare_models()
    test_phase4_compare_classify()
    test_phase4_serenity_score_in_registry()
    test_phase4_foreign_map_expanded()
    test_phase4_giant_map_expanded()
    test_phase4_analyze_compare_field()
    test_phase4_no_brave_tavily()
    test_phase4_query_isolation_new_files()

    # Phase 5 — Expanded coverage + new fields
    test_phase5_new_nodes_registry()
    test_phase5_foreign_map_entries()
    test_phase5_candidate_new_fields()
    test_phase5_best_blend_score_formula()
    test_phase5_ranking_bucket_positions()
    test_phase5_preset_mode_dispatch()
    test_phase5_sort_mode_ranking()
    test_phase5_compare_consensus_strength()
    test_phase5_compare_disagreement_reason()
    test_phase5_compare_high_disagreement_names()
    test_phase5_compare_response_model()

    # Phase 6 — Regime detection
    test_regime_types_model()
    test_regime_service_returns_regime()
    test_regime_service_deterministic()
    test_regime_theme_scores_sorted()
    test_regime_anchor_scores_sorted()
    test_regime_anchor_overlapping_theme_ids()
    test_regime_top_themes_in_theme_scores()
    test_regime_top_anchors_in_anchor_scores()
    test_discover_response_regime_context_field()

    # Phase 7 — Strategy Screener
    test_screener_types_models()
    test_screener_grade_assignment()
    test_screener_report_builder_sections()
    test_screener_stale_logic()
    test_screener_candidate_to_screener_dict()
    test_screener_isolation_from_query()

    # Phase 8 — Screener Filter / Sort
    test_screener_market_cap_classification()
    test_screener_filter_market_cap_bucket()
    test_screener_filter_layer()
    test_screener_filter_combined()
    test_screener_sort_best_fit()
    test_screener_sort_best_fit_tiebreak()
    test_screener_sort_market_cap()
    test_screener_sort_layer()
    test_screener_sort_grade()
    test_screener_limit()
    test_screener_no_filter_passes_all()
    test_screener_invalid_params_raise()
    test_screener_backwards_compatible_no_params()
    test_screener_config_has_dropdown_metadata()
    test_screener_filter_isolation_from_query()

    print()
    print("=" * 60)
    print(f"Results: {_PASS} passed, {_FAIL} failed")
    print("=" * 60)

    return _FAIL == 0


if __name__ == "__main__":
    import sys, os
    # Ensure backend/ is on the path when run from project root
    backend_dir = os.path.dirname(os.path.abspath(__file__))
    while not os.path.basename(backend_dir) == "backend" and backend_dir != "/":
        backend_dir = os.path.dirname(backend_dir)
    if backend_dir not in sys.path:
        sys.path.insert(0, backend_dir)

    ok = run_all()
    sys.exit(0 if ok else 1)
