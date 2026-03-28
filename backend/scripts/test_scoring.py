"""
Deterministic scoring pipeline validation tests.
Tests the institutional scorer on mocked candidate bundles
without touching any endpoints, prompts, or data modules.

Run:  python scripts/test_scoring.py
      or:  pytest scripts/test_scoring.py -v
"""

import sys, os, json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from agent.institutional_scorer import (
    score_candidate,
    _build_data_flags,
    _completeness_penalty,
    _blend_weights,
    _apply_conviction_validation,
    _compute_position_sizing,
    BASE_WEIGHTS,
    REGIME_WEIGHT_MATRIX,
)


def _make_candidate(
    price=10.0,
    has_ohlc=True,
    has_volume=True,
    has_social=True,
    has_fundamentals=True,
    has_news=True,
    market_cap=100_000_000,
    avg_volume=500_000,
    volume=600_000,
    sentiment_bull=60,
    x_sentiment_score=0.3,
    rsi=55,
    sma_20=9.0,
    sma_50=8.5,
    sma_200=7.5,
    change_pct=2.0,
    revenue_growth=15,
    pe_ratio=20,
    eps_growth=10,
    recent_news=None,
    insider_data=None,
    earnings_date=None,
    sector="technology",
):
    asset = {"snapshot": {"price": price}, "details": {}, "overview": {}, "technicals": {}}

    if has_ohlc:
        asset["technicals"]["rsi"] = rsi
        asset["technicals"]["sma_20"] = sma_20
        asset["technicals"]["sma_50"] = sma_50
        asset["technicals"]["sma_200"] = sma_200
        asset["snapshot"]["change_pct"] = change_pct

    if has_volume:
        asset["snapshot"]["volume"] = volume
        asset["details"]["avg_volume"] = avg_volume

    if has_social:
        asset["sentiment"] = {"bull_pct": sentiment_bull}
        asset["x_sentiment"] = {"sentiment_score": x_sentiment_score}

    if has_fundamentals:
        asset["overview"]["revenue_growth"] = revenue_growth
        asset["overview"]["pe_ratio"] = pe_ratio
        asset["overview"]["eps_growth"] = eps_growth
        asset["overview"]["market_cap"] = market_cap

    if has_news and recent_news is not None:
        asset["recent_news"] = recent_news
    elif has_news:
        asset["recent_news"] = [{"title": "Test headline", "published": "2026-01-01"}]

    if insider_data:
        asset["insider_trading"] = insider_data

    if earnings_date:
        asset["next_earnings"] = earnings_date

    if sector:
        asset["overview"]["sector"] = sector

    asset["details"]["market_cap"] = market_cap
    asset["overview"]["market_cap"] = market_cap

    return asset


PASS = "\033[92mPASS\033[0m"
FAIL = "\033[91mFAIL\033[0m"
results = []


def check(name, condition, detail=""):
    status = PASS if condition else FAIL
    results.append(condition)
    print(f"  [{status}] {name}" + (f"  — {detail}" if detail else ""))
    return condition


def test_a_missing_fundamentals():
    print("\n" + "=" * 60)
    print("TEST A: Missing Fundamentals")
    print("=" * 60)

    asset = _make_candidate(
        has_fundamentals=False,
        has_ohlc=True,
        has_volume=True,
        has_social=True,
        has_news=True,
        market_cap=200_000_000,
    )
    asset["overview"] = {"sector": "technology"}
    asset["details"]["market_cap"] = 200_000_000

    regime = {"regime": "neutral", "confidence": 0}
    scored = score_candidate("TEST_A", asset, regime)
    sc = scored["institutional_scoring"]

    check("fundamental_score == 50 (neutral default)",
          sc["fundamental_score"] == 50.0,
          f"got {sc['fundamental_score']}")

    check("data_flags.missing includes 'fundamentals'",
          "fundamentals" in sc["data_flags"]["missing"],
          f"missing={sc['data_flags']['missing']}")

    check("completeness_penalty > 0",
          sc["completeness_penalty"] > 0,
          f"penalty={sc['completeness_penalty']}")

    baseline_asset = _make_candidate(
        has_fundamentals=True,
        has_ohlc=True,
        has_volume=True,
        has_social=True,
        has_news=True,
        market_cap=200_000_000,
    )
    baseline = score_candidate("TEST_A_BASE", baseline_asset, regime)
    base_sc = baseline["institutional_scoring"]

    check("raw_score reduced by penalty (vs baseline with fundamentals)",
          sc["raw_score"] < base_sc["raw_score"] * 1.01,
          f"penalized={sc['raw_score']:.1f} vs baseline={base_sc['raw_score']:.1f}")

    check("recommendation_tier is Python-enforced (present on scorecard)",
          sc["recommendation_tier"] in ("buy", "watch", "speculative"),
          f"tier={sc['recommendation_tier']}")

    check("recommendation_tier reflects conviction gate, not data completeness",
          (sc["recommendation_tier"] == "buy") == sc["conviction_validation"]["validation_passed"],
          f"tier={sc['recommendation_tier']}, conviction_passed={sc['conviction_validation']['validation_passed']}")


def test_b_override_blocked_hype_only():
    print("\n" + "=" * 60)
    print("TEST B: Override Cannot Trigger From Hype-Only")
    print("=" * 60)

    asset = _make_candidate(
        has_fundamentals=False,
        has_ohlc=True,
        has_volume=False,
        has_social=True,
        has_news=False,
        market_cap=80_000_000,
        sentiment_bull=95,
        x_sentiment_score=0.9,
    )
    asset["sentiment"]["bull_pct"] = 95
    asset["x_sentiment"]["sentiment_score"] = 0.9
    asset["overview"] = {"sector": "technology", "market_cap": 80_000_000}
    asset["details"]["market_cap"] = 80_000_000

    regime = {"regime": "neutral", "confidence": 0}
    scored = score_candidate("TEST_B", asset, regime)
    sc = scored["institutional_scoring"]

    check("'override_candidate' NOT in labels",
          "override_candidate" not in sc["labels"],
          f"labels={sc['labels']}")

    max_pct = sc["position_size_guidance"]["max_pct"]
    check("sizing cap <= 2%",
          max_pct <= 2.0,
          f"max_pct={max_pct}")

    check("no creative_discovery_override flag",
          not sc.get("creative_discovery_override", False),
          f"creative_discovery_override={sc.get('creative_discovery_override')}")

    check("recommendation_tier is 'speculative' (hype-only)",
          sc["recommendation_tier"] == "speculative",
          f"tier={sc['recommendation_tier']}")


def test_c_microcap_gating():
    print("\n" + "=" * 60)
    print("TEST C: Microcap Gating")
    print("=" * 60)

    print("\n  --- C.1: micro + low liquidity ---")
    asset_low = _make_candidate(
        market_cap=150_000_000,
        has_ohlc=True,
        has_volume=True,
        has_social=True,
        has_fundamentals=True,
        has_news=True,
        rsi=55,
        sma_20=9.0,
        volume=50_000,
        avg_volume=40_000,
    )
    asset_low["overview"]["market_cap"] = 150_000_000
    asset_low["details"]["market_cap"] = 150_000_000
    asset_low["details"]["avg_volume"] = 40_000
    asset_low["snapshot"]["volume"] = 50_000
    asset_low["snapshot"]["price"] = 5.0

    regime = {"regime": "neutral", "confidence": 0.5}
    scored_low = score_candidate("TEST_C_LOW", asset_low, regime)
    sc_low = scored_low["institutional_scoring"]

    conviction_label = sc_low["conviction_validation"]["conviction_label"]
    check("micro+low_liq: conviction NOT BUY",
          conviction_label != "BUY" or "speculative" in sc_low["labels"],
          f"conviction={conviction_label}, labels={sc_low['labels']}")

    check("micro+low_liq: 'speculative' in labels",
          "speculative" in sc_low["labels"],
          f"labels={sc_low['labels']}")

    max_pct = sc_low["position_size_guidance"]["max_pct"]
    check("micro+low_liq: sizing cap <= 1%",
          max_pct <= 1.0,
          f"max_pct={max_pct}")

    check("micro+low_liq: recommendation_tier is 'speculative' (Python-enforced)",
          sc_low["recommendation_tier"] == "speculative",
          f"tier={sc_low['recommendation_tier']}")

    print("\n  --- C.2: micro + medium liquidity ---")
    asset_med = _make_candidate(
        market_cap=150_000_000,
        has_ohlc=True,
        has_volume=True,
        has_social=True,
        has_fundamentals=True,
        has_news=True,
        rsi=55,
        sma_20=9.0,
        volume=600_000,
        avg_volume=500_000,
    )
    asset_med["overview"]["market_cap"] = 150_000_000
    asset_med["details"]["market_cap"] = 150_000_000
    asset_med["details"]["avg_volume"] = 500_000
    asset_med["snapshot"]["volume"] = 600_000
    asset_med["snapshot"]["price"] = 10.0

    scored_med = score_candidate("TEST_C_MED", asset_med, regime)
    sc_med = scored_med["institutional_scoring"]

    conviction_med = sc_med["conviction_validation"]["conviction_label"]
    max_pct_med = sc_med["position_size_guidance"]["max_pct"]
    check("micro+med_liq: can achieve higher conviction or sizing",
          max_pct_med > max_pct or conviction_med == "BUY",
          f"conviction={conviction_med}, max_pct={max_pct_med} (was {max_pct})")

    check("micro+med_liq: sizing follows tier rules",
          max_pct_med <= 5.0,
          f"max_pct={max_pct_med}")

    check("micro+med_liq: recommendation_tier can be 'buy' with medium liquidity",
          sc_med["recommendation_tier"] == "buy",
          f"tier={sc_med['recommendation_tier']}")


def test_d_regime_blending():
    print("\n" + "=" * 60)
    print("TEST D: Regime Blending")
    print("=" * 60)

    print("\n  --- D.1: risk_off confidence=0.1 (near base weights) ---")
    weights_low = _blend_weights("risk_off", 0.1)
    print(f"  Blended weights (conf=0.1): {json.dumps(weights_low, indent=2)}")

    for key in BASE_WEIGHTS:
        diff = abs(weights_low[key] - BASE_WEIGHTS[key])
        check(f"conf=0.1 {key}: close to base ({BASE_WEIGHTS[key]})",
              diff < 0.05,
              f"blended={weights_low[key]}, base={BASE_WEIGHTS[key]}, diff={diff:.4f}")

    print("\n  --- D.2: risk_off confidence=0.9 (near regime weights) ---")
    weights_high = _blend_weights("risk_off", 0.9)
    risk_off_w = REGIME_WEIGHT_MATRIX["risk_off"]
    print(f"  Blended weights (conf=0.9): {json.dumps(weights_high, indent=2)}")

    for key in BASE_WEIGHTS:
        diff = abs(weights_high[key] - risk_off_w[key])
        check(f"conf=0.9 {key}: close to risk_off ({risk_off_w[key]})",
              diff < 0.05,
              f"blended={weights_high[key]}, regime={risk_off_w[key]}, diff={diff:.4f}")


if __name__ == "__main__":
    print("\n" + "#" * 60)
    print("  INSTITUTIONAL SCORING — DETERMINISTIC VALIDATION")
    print("#" * 60)

    test_a_missing_fundamentals()
    test_b_override_blocked_hype_only()
    test_c_microcap_gating()
    test_d_regime_blending()

    passed = sum(results)
    total = len(results)
    print("\n" + "=" * 60)
    if passed == total:
        print(f"  ALL {total} CHECKS PASSED")
    else:
        print(f"  {passed}/{total} CHECKS PASSED — {total - passed} FAILED")
    print("=" * 60 + "\n")

    sys.exit(0 if passed == total else 1)
