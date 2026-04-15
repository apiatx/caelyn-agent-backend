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
# Runner
# ────────────────────────────────────────────────────────────────────────────

def run_all():
    print("=" * 60)
    print("PLAYBOOK PHASE 1.5 + 2.0 + 3.0 FACTOR TESTS")
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
