"""
Institutional Prior Scoring Engine.
Runs AFTER data gathering, BEFORE Claude sees candidates.

Regime-aware deterministic scoring pipeline:
  1. Build normalized scorecard (unified contract)
  2. Compute sub-scores with data completeness awareness
  3. Detect market regime → blend weights by confidence
  4. Apply cross-asset weight adjustment (bounded [0.75, 1.25])
  5. Enforce social→FA discipline + conviction validation
  6. Microcap guardrails: liquidity gating + sizing caps
  7. Creative discovery exception (requires verifiable catalysts)
  8. Attach structured scorecard + position sizing guidance

Claude receives the final scorecard and interprets — does NOT rescore.
"""

from data.scoring_engine import parse_pct, parse_market_cap_string, get_market_cap
from core.catalyst_engine import calculate_catalyst_score
from core.asset_weight_engine import (
    apply_asset_weights, compute_avg_dollar_volume,
    get_liquidity_tier, get_mcap_tier,
)


MCAP_SMALL_CEILING = 2_000_000_000
MCAP_MICRO_CEILING = 500_000_000

HOT_SECTORS = {
    "technology", "information technology", "software", "semiconductors",
    "ai", "artificial intelligence", "machine learning", "cloud",
    "defense", "aerospace", "aerospace & defense",
    "energy", "renewable energy", "solar", "uranium", "nuclear",
    "robotics", "automation",
    "biotech", "biotechnology", "pharmaceuticals", "healthcare",
    "critical minerals", "mining", "rare earth",
    "cybersecurity", "fintech", "blockchain",
    "quantum", "quantum computing",
    "space", "satellites",
    "electric vehicles", "ev", "battery",
}

CATALYST_KEYWORDS = [
    "fda", "approval", "partnership", "contract", "acquisition", "merger",
    "deal", "agreement", "license", "patent", "milestone", "breakthrough",
    "launch", "product launch", "revenue beat", "earnings beat", "guidance raised",
    "upgrade", "initiated", "coverage", "target raised", "buy rating",
    "government", "dod", "defense contract", "grant", "funding",
    "insider buying", "buyback", "repurchase",
    "trial", "phase 3", "phase 2", "clinical", "results", "data readout",
    "ai", "gpu", "infrastructure",
]

BASE_WEIGHTS = {
    "technical": 0.25,
    "sentiment": 0.20,
    "fundamentals": 0.30,
    "catalyst": 0.25,
}

REGIME_WEIGHT_MATRIX = {
    "risk_on": {
        "technical": 0.30,
        "sentiment": 0.30,
        "fundamentals": 0.20,
        "catalyst": 0.20,
    },
    "risk_off": {
        "technical": 0.20,
        "sentiment": 0.15,
        "fundamentals": 0.40,
        "catalyst": 0.25,
    },
    "inflationary": {
        "technical": 0.20,
        "sentiment": 0.15,
        "fundamentals": 0.35,
        "catalyst": 0.30,
    },
    "neutral": BASE_WEIGHTS.copy(),
}

COMPLETENESS_PENALTIES = {
    "has_fundamentals": 0.10,
    "has_ohlc": 0.08,
    "has_volume": 0.07,
    "has_news": 0.05,
    "has_social": 0.05,
}

POSITION_SIZING_BY_TIER = {
    "nano_low": {"max_pct": 0.5, "tier": "nano_low"},
    "nano_medium": {"max_pct": 1.0, "tier": "nano_medium"},
    "nano_high": {"max_pct": 1.5, "tier": "nano_high"},
    "micro_low": {"max_pct": 1.0, "tier": "micro_low"},
    "micro_medium": {"max_pct": 2.0, "tier": "micro_medium"},
    "micro_high": {"max_pct": 3.0, "tier": "micro_high"},
    "small_low": {"max_pct": 2.0, "tier": "small_low"},
    "small_medium": {"max_pct": 3.0, "tier": "small_medium"},
    "small_high": {"max_pct": 4.0, "tier": "small_high"},
    "large_low": {"max_pct": 3.0, "tier": "large_low"},
    "large_medium": {"max_pct": 4.0, "tier": "large_medium"},
    "large_high": {"max_pct": 5.0, "tier": "large_high"},
}

REGIME_MAX_PCT = {
    "risk_on": 8.0,
    "risk_off": 3.0,
    "inflationary": 5.0,
    "neutral": 5.0,
}


def _compute_technical_score(data: dict, has_ohlc: bool) -> float:
    if not has_ohlc:
        return 50.0

    score = 50.0
    snapshot = data.get("snapshot", {})
    technicals = data.get("technicals", {})

    rsi = technicals.get("rsi") or technicals.get("rsi_14")
    if rsi is not None:
        try:
            rsi = float(rsi)
            if 40 <= rsi <= 70:
                score += 15
            elif 30 <= rsi < 40 or 70 < rsi <= 80:
                score += 5
            elif rsi > 80:
                score -= 10
            elif rsi < 30:
                score += 8
        except (TypeError, ValueError):
            pass

    price = snapshot.get("price")
    sma_20 = technicals.get("sma_20")
    sma_50 = technicals.get("sma_50")
    sma_200 = technicals.get("sma_200")
    if price:
        try:
            p = float(price)
            above_count = 0
            for sma in [sma_20, sma_50, sma_200]:
                if sma and float(sma) > 0 and p > float(sma):
                    above_count += 1
            score += above_count * 5
        except (TypeError, ValueError):
            pass

    volume = snapshot.get("volume")
    details = data.get("details", {})
    avg_vol = details.get("avg_volume")
    if volume and avg_vol:
        try:
            ratio = float(volume) / float(avg_vol)
            if ratio >= 3.0:
                score += 15
            elif ratio >= 2.0:
                score += 10
            elif ratio >= 1.5:
                score += 5
        except (TypeError, ValueError, ZeroDivisionError):
            pass

    change = snapshot.get("change_pct")
    if change is not None:
        pct = parse_pct(change)
        if pct is not None:
            if 0.01 <= pct <= 0.10:
                score += 5
            elif pct > 0.15:
                score -= 5

    return max(0, min(100, score))


def _compute_fundamental_score(data: dict, has_fundamentals: bool) -> float:
    if not has_fundamentals:
        return 50.0

    score = 30.0

    news = data.get("recent_news", [])
    if isinstance(news, list) and news:
        news_text = " ".join(
            str(n.get("title", "") if isinstance(n, dict) else n)
            for n in news[:5]
        ).lower()
        catalyst_hits = sum(1 for kw in CATALYST_KEYWORDS if kw in news_text)
        score += min(catalyst_hits * 8, 40)
    elif isinstance(news, str) and news:
        catalyst_hits = sum(1 for kw in CATALYST_KEYWORDS if kw in news.lower())
        score += min(catalyst_hits * 8, 40)

    overview = data.get("overview", {})
    if isinstance(overview, dict):
        earnings_date = overview.get("earnings_date") or overview.get("next_earnings")
        if earnings_date:
            score += 10

        rev_growth = overview.get("revenue_growth") or overview.get("revenue_growth_yoy")
        if rev_growth:
            pct = parse_pct(rev_growth)
            if pct is not None and pct > 0.20:
                score += 10
            elif pct is not None and pct > 0.10:
                score += 5

    insider = data.get("insider_sentiment", {})
    if isinstance(insider, dict):
        mspr = insider.get("mspr") or insider.get("total_mspr")
        if mspr is not None:
            try:
                mspr_val = float(mspr)
                if mspr_val > 10:
                    score += 10
                elif mspr_val > 0:
                    score += 5
            except (TypeError, ValueError):
                pass

    return max(0, min(100, score))


def _compute_sector_score(data: dict) -> float:
    score = 50.0

    sector = None
    for source in [data.get("overview", {}), data.get("details", {}), data.get("profile", {})]:
        if isinstance(source, dict):
            sector = source.get("sector") or source.get("industry")
            if sector:
                break

    if sector and sector.lower() in HOT_SECTORS:
        score += 30

    x_sent = data.get("x_sentiment", {})
    if isinstance(x_sent, dict):
        themes = x_sent.get("key_themes", [])
        if isinstance(themes, list):
            theme_text = " ".join(str(t) for t in themes).lower()
            if any(hot in theme_text for hot in ["ai", "defense", "energy", "biotech", "quantum", "cyber"]):
                score += 10

    return max(0, min(100, score))


def _compute_social_score(data: dict, has_social: bool) -> float:
    if not has_social:
        return 50.0

    score = 30.0

    sentiment = data.get("sentiment", {})
    if isinstance(sentiment, dict):
        bull = sentiment.get("bull_pct") or sentiment.get("bullish_pct")
        if bull is not None:
            try:
                bull_val = float(bull)
                if bull_val > 75:
                    score += 25
                elif bull_val > 60:
                    score += 15
                elif bull_val > 50:
                    score += 5
            except (TypeError, ValueError):
                pass

        watchers = sentiment.get("watchers_change") or sentiment.get("volume")
        if watchers:
            try:
                w = float(watchers)
                if w > 0:
                    score += 10
            except (TypeError, ValueError):
                pass

    x_sent = data.get("x_sentiment", {})
    if isinstance(x_sent, dict):
        x_score = x_sent.get("sentiment_score")
        if x_score is not None:
            try:
                xs = float(x_score)
                if xs > 0.5:
                    score += 20
                elif xs > 0.2:
                    score += 10
                elif xs < -0.3:
                    score -= 10
            except (TypeError, ValueError):
                pass

        risk_flags = x_sent.get("risk_flags", [])
        if risk_flags and isinstance(risk_flags, list) and len(risk_flags) > 0:
            score -= 15

    return max(0, min(100, score))


def _compute_liquidity_score(data: dict) -> float:
    score = 50.0

    mc = get_market_cap(data)
    if mc is not None:
        if mc > 10e9:
            score += 30
        elif mc > 2e9:
            score += 20
        elif mc > 500e6:
            score += 10
        elif mc > 100e6:
            score += 5
        elif mc < 50e6:
            score -= 20

    snapshot = data.get("snapshot", {})
    details = data.get("details", {})
    volume = snapshot.get("volume")
    avg_vol = details.get("avg_volume")
    if volume and avg_vol:
        try:
            ratio = float(volume) / float(avg_vol)
            if ratio >= 2.0:
                score += 15
            elif ratio >= 1.0:
                score += 5
        except (TypeError, ValueError, ZeroDivisionError):
            pass

    return max(0, min(100, score))


def _build_data_flags(asset: dict) -> dict:
    snapshot = asset.get("snapshot", {})
    technicals = asset.get("technicals", {})
    overview = asset.get("overview", {})
    details = asset.get("details", {})

    has_price = bool(snapshot.get("price"))
    has_ohlc = bool(technicals.get("sma_20") or technicals.get("rsi") or technicals.get("macd"))
    has_fundamentals = bool(
        overview.get("revenue_growth") or overview.get("pe_ratio")
        or overview.get("market_cap") or overview.get("eps_growth")
    )
    has_news = bool(asset.get("recent_news"))
    has_social = bool(asset.get("sentiment") or asset.get("x_sentiment"))
    has_volume = bool(snapshot.get("volume") and details.get("avg_volume"))

    flags = {
        "has_price": has_price,
        "has_ohlc": has_ohlc,
        "has_fundamentals": has_fundamentals,
        "has_news": has_news,
        "has_social": has_social,
        "has_volume": has_volume,
    }
    flags["missing"] = [k.replace("has_", "") for k, v in flags.items() if k.startswith("has_") and not v]
    return flags


def _completeness_penalty(data_flags: dict) -> float:
    penalty = 0.0
    for flag_key, weight in COMPLETENESS_PENALTIES.items():
        if not data_flags.get(flag_key, True):
            penalty += weight
    return min(penalty, 0.25)


def _blend_weights(regime: str, confidence: float) -> dict:
    regime_weights = REGIME_WEIGHT_MATRIX.get(regime, BASE_WEIGHTS)
    if regime == "neutral" or confidence <= 0:
        return BASE_WEIGHTS.copy()
    conf = max(0.0, min(1.0, confidence))
    blended = {}
    for key in BASE_WEIGHTS:
        blended[key] = round(
            BASE_WEIGHTS[key] * (1 - conf) + regime_weights[key] * conf,
            4,
        )
    return blended


def _detect_asset_class(data: dict) -> str:
    for source in [data.get("overview", {}), data.get("details", {}), data.get("profile", {})]:
        if isinstance(source, dict):
            if source.get("asset_type"):
                return source["asset_type"].lower()
    ticker = data.get("ticker", "")
    if isinstance(ticker, str) and ticker.upper() in {
        "BTC", "ETH", "SOL", "DOGE", "ADA", "XRP", "AVAX", "LINK", "DOT",
        "MATIC", "NEAR", "ARB", "OP", "SUI", "APT", "SEI", "TIA", "INJ",
    }:
        return "crypto"
    return "equity"


def _extract_sector(data: dict) -> str:
    for source in [data.get("overview", {}), data.get("details", {}), data.get("profile", {})]:
        if isinstance(source, dict):
            sector = source.get("sector") or source.get("industry")
            if sector:
                return sector
    return ""


def _compute_position_sizing(mcap_tier: str, liq_tier: str, regime: str, labels: list, **kwargs) -> dict:
    tier_key = f"{mcap_tier}_{liq_tier}"
    base = POSITION_SIZING_BY_TIER.get(tier_key, {"max_pct": 3.0, "tier": tier_key})
    regime_cap = REGIME_MAX_PCT.get(regime, 5.0)
    max_pct = min(base["max_pct"], regime_cap)

    if "speculative" in labels or "lottery" in labels:
        max_pct = min(max_pct, 1.0)
    if "override_candidate" in labels:
        fundamental_score = kwargs.get("fundamental_score", 0)
        liq = kwargs.get("liquidity_tier", "medium")
        if fundamental_score >= 70 and liq == "high":
            pass
        elif fundamental_score >= 55:
            max_pct = min(max_pct, 3.0)
        else:
            max_pct = min(max_pct, 2.0)

    return {"max_pct": max_pct, "tier": base["tier"]}


def _apply_conviction_validation(
    technical: float, catalyst_result: dict,
    liq_tier: str, mcap_tier: str, avg_dollar_vol: float
) -> dict:
    confirmations = 0
    reasons = []

    ta_confirmed = technical >= 65
    if ta_confirmed:
        confirmations += 1
        reasons.append("TA_CONFIRMED")

    present_count = catalyst_result.get("present_count", 0)
    catalyst_confirmed = present_count >= 1
    if catalyst_confirmed:
        confirmations += 1
        reasons.append("CATALYST_PRESENT")

    liq_ok = avg_dollar_vol >= 2_000_000 or liq_tier in ("medium", "high")
    if liq_ok:
        confirmations += 1
        reasons.append("LIQUIDITY_OK")

    passed = confirmations >= 2

    is_micro = mcap_tier in ("nano", "micro")
    if is_micro and not liq_ok:
        passed = False
        if "LIQUIDITY_FAIL" not in reasons:
            reasons.append("LIQUIDITY_FAIL_MICRO")

    return {
        "validation_passed": passed,
        "confirmations": confirmations,
        "reasons": reasons,
        "conviction_label": "BUY" if passed else "WATCH",
        "validation_note": (
            None if passed
            else f"Only {confirmations}/3 confirmations — downgraded to WATCH"
        ),
    }


def score_candidate(ticker: str, asset: dict, regime_data: dict = None) -> dict:
    """
    Institutional-grade deterministic scoring for a single candidate.
    Produces a normalized scorecard with:
      - Data completeness penalties
      - Confidence-blended regime weights
      - Bounded cross-asset multipliers
      - Liquidity-aware position sizing
      - Conviction validation
      - Creative discovery with verifiable catalyst gates
    """
    regime = (regime_data or {}).get("regime", "neutral")
    regime_confidence = (regime_data or {}).get("confidence", 0)

    data_flags = _build_data_flags(asset)
    penalty = _completeness_penalty(data_flags)

    technical = _compute_technical_score(asset, data_flags["has_ohlc"])
    fundamental = _compute_fundamental_score(asset, data_flags["has_fundamentals"])
    social = _compute_social_score(asset, data_flags["has_social"])
    sector = _compute_sector_score(asset)
    liquidity = _compute_liquidity_score(asset)

    catalyst_result = calculate_catalyst_score(asset)
    catalyst_strength = catalyst_result["catalyst_score"]
    catalyst_present_count = catalyst_result["present_count"]

    weights = _blend_weights(regime, regime_confidence)

    raw_score = (
        weights["technical"] * technical +
        weights["sentiment"] * social +
        weights["fundamentals"] * fundamental +
        weights["catalyst"] * catalyst_strength
    )

    raw_score = raw_score * (1.0 - penalty)

    mc = get_market_cap(asset)
    mcap_tier = get_mcap_tier(mc)
    avg_dollar_vol = compute_avg_dollar_volume(asset)
    liq_tier = get_liquidity_tier(avg_dollar_vol)

    social_discipline_flag = None
    if social >= 60:
        if technical < 45 and fundamental < 45:
            raw_score *= 0.85
            social_discipline_flag = "SOCIAL_UNCONFIRMED"

    asset_class = _detect_asset_class(asset)
    asset_metadata = {
        "asset_class": asset_class,
        "market_cap_tier": mcap_tier,
        "liquidity_tier": liq_tier,
        "sector": _extract_sector(asset),
    }
    weight_result = apply_asset_weights(raw_score, asset_metadata, regime)
    adjusted_score = weight_result["adjusted_score"]

    labels = []

    cat_components = catalyst_result.get("components", {})
    vol_expansion_present = cat_components.get("volume_expansion", {}).get("present", False)
    vol_expansion_score = cat_components.get("volume_expansion", {}).get("score", 0)
    has_real_catalyst = (
        cat_components.get("news_density", {}).get("present", False)
        or cat_components.get("earnings_proximity", {}).get("present", False)
        or cat_components.get("fundamental_acceleration", {}).get("present", False)
    )

    creative_override = False
    if social >= 85 and catalyst_strength > 60:
        override_eligible = (
            vol_expansion_present and vol_expansion_score >= 10
            and has_real_catalyst
            and liq_tier != "low"
        )
        if override_eligible:
            creative_override = True
            labels.append("override_candidate")
            if fundamental < 55:
                labels.append("speculative")
        else:
            labels.append("speculative")
            labels.append("lottery")

    is_micro = mcap_tier in ("nano", "micro")
    if is_micro and liq_tier == "low":
        if "speculative" not in labels:
            labels.append("speculative")

    conviction = _apply_conviction_validation(
        technical, catalyst_result, liq_tier, mcap_tier, avg_dollar_vol
    )
    if not conviction["validation_passed"] and "speculative" not in labels:
        if is_micro:
            labels.append("speculative")

    position_guide = _compute_position_sizing(
        mcap_tier, liq_tier, regime, labels,
        fundamental_score=fundamental, liquidity_tier=liq_tier,
    )

    catalyst_components_out = {}
    for comp_name, comp_data in cat_components.items():
        if isinstance(comp_data, dict):
            catalyst_components_out[comp_name] = comp_data
        else:
            catalyst_components_out[comp_name] = {"score": comp_data, "present": comp_data > 0}

    snapshot = asset.get("snapshot", {})
    asset["prior_score"] = adjusted_score
    scorecard = {
        "ticker": ticker,
        "asset_class": asset_class,
        "market_cap": mc,
        "avg_dollar_volume": round(avg_dollar_vol, 0) if avg_dollar_vol else None,
        "price": snapshot.get("price"),

        "sentiment_score": round(social, 1),
        "technical_score": round(technical, 1),
        "fundamental_score": round(fundamental, 1),
        "catalyst_score": catalyst_strength,

        "data_flags": data_flags,
        "completeness_penalty": round(penalty, 3),

        "catalyst_components": catalyst_components_out,
        "catalyst_present_components": catalyst_present_count,

        "raw_score": round(raw_score, 1),
        "adjusted_final_score": adjusted_score,
        "prior_score": adjusted_score,

        "regime": regime,
        "regime_confidence": regime_confidence,
        "weight_matrix": weights,
        "asset_multiplier": weight_result["regime_multiplier"],

        "market_cap_category": mcap_tier,
        "liquidity_tier": liq_tier,
        "position_size_guidance": position_guide,

        "conviction_validation": conviction,
        "labels": labels,

        "sector_alignment_score": round(sector, 1),
        "liquidity_score": round(liquidity, 1),
    }
    if "speculative" in labels:
        recommendation_tier = "speculative"
    elif conviction["validation_passed"]:
        recommendation_tier = "buy"
    else:
        recommendation_tier = "watch"
    scorecard["recommendation_tier"] = recommendation_tier

    if social_discipline_flag:
        scorecard["social_discipline_flag"] = social_discipline_flag
    if creative_override:
        scorecard["creative_discovery_override"] = True

    asset["institutional_scoring"] = scorecard
    return asset


def apply_institutional_scoring(market_data: dict, regime_data: dict = None) -> dict:
    """
    Apply regime-aware institutional scoring to all candidates in market_data.
    Sorts by adjusted_final_score descending but does NOT remove any candidates.
    Attaches regime context, position sizing, scoring debug block, and summary log.
    """
    enriched = market_data.get("enriched_data")
    if not enriched or not isinstance(enriched, dict):
        return market_data

    if regime_data is None:
        regime_data = {"regime": "neutral", "confidence": 0}

    scored_count = 0
    partial_count = 0
    override_count = 0
    for ticker, data in enriched.items():
        if isinstance(data, dict) and "error" not in data:
            score_candidate(ticker.replace("FLAGGED_", ""), data, regime_data)
            scored_count += 1
            sc = data.get("institutional_scoring", {})
            df = sc.get("data_flags", {})
            missing = df.get("missing", [])
            if len(missing) >= 3:
                partial_count += 1
            if sc.get("creative_discovery_override"):
                override_count += 1

    budget_exhausted = market_data.get("data_completeness") == "partial"

    if scored_count > 0:
        sorted_tickers = sorted(
            enriched.keys(),
            key=lambda t: enriched[t].get("prior_score", 0) if isinstance(enriched[t], dict) else 0,
            reverse=True,
        )

        sorted_enriched = {t: enriched[t] for t in sorted_tickers}
        market_data["enriched_data"] = sorted_enriched

        top_10_debug = []
        for t in sorted_tickers[:10]:
            d = enriched[t]
            if not isinstance(d, dict):
                continue
            sc = d.get("institutional_scoring", {})
            top_10_debug.append({
                "ticker": t.replace("FLAGGED_", ""),
                "adjusted_final_score": sc.get("adjusted_final_score"),
                "technical_score": sc.get("technical_score"),
                "fundamental_score": sc.get("fundamental_score"),
                "sentiment_score": sc.get("sentiment_score"),
                "catalyst_score": sc.get("catalyst_score"),
                "asset_multiplier": sc.get("asset_multiplier"),
                "conviction_label": sc.get("conviction_validation", {}).get("conviction_label"),
                "recommendation_tier": sc.get("recommendation_tier"),
                "completeness_penalty": sc.get("completeness_penalty"),
                "labels": sc.get("labels", []),
                "missing": sc.get("data_flags", {}).get("missing", []),
            })

        regime_name = regime_data.get("regime", "neutral")
        regime_conf = regime_data.get("confidence", 0)

        top5_summary = " ".join(
            f"{e['ticker']}:{e['adjusted_final_score']}" +
            (f"(missing_{'_'.join(e['missing'][:2])})" if e.get("missing") else "")
            for e in top_10_debug[:5]
        )
        print(f"[SCORING] regime={regime_name}({regime_conf}) "
              f"full={scored_count - partial_count} partial={partial_count} "
              f"overrides={override_count} "
              f"top=[{top5_summary}]")

    else:
        top_10_debug = []
        regime_name = regime_data.get("regime", "neutral")
        regime_conf = regime_data.get("confidence", 0)

    market_data["regime_context"] = regime_data
    market_data["position_sizing"] = {
        "regime": regime_name,
        "regime_max_pct": REGIME_MAX_PCT.get(regime_name, 5.0),
    }

    market_data["scoring_debug"] = {
        "regime": {
            "name": regime_name,
            "confidence": regime_conf,
            "inputs_used": list((regime_data.get("signals") or {}).keys()),
        },
        "scoring_weights_used": _blend_weights(regime_name, regime_conf),
        "candidates_scored": scored_count,
        "candidates_partial": partial_count,
        "override_triggered": override_count > 0,
        "budget_exhausted": budget_exhausted,
        "top_10_candidates": top_10_debug,
    }

    market_data["scoring_summary"] = {
        "regime": regime_name,
        "confidence": regime_conf,
        "full_candidates": scored_count - partial_count,
        "partial_candidates": partial_count,
        "override_count": override_count,
        "top5": [
            {
                "ticker": e["ticker"],
                "score": e["adjusted_final_score"],
                "tier": e.get("recommendation_tier", "watch"),
                "missing": e.get("missing", []),
            }
            for e in top_10_debug[:5]
        ],
    }

    return market_data
