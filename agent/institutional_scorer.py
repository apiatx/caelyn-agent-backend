"""
Institutional Prior Scoring Engine.
Runs AFTER data gathering, BEFORE Claude sees candidates.

Regime-aware deterministic scoring pipeline:
  1. Compute sub-scores: technical, fundamental/catalyst, sentiment, catalyst_strength
  2. Detect market regime (risk_on / risk_off / inflationary / neutral)
  3. Apply regime-specific weight matrix to sub-scores
  4. Apply cross-asset weight adjustment (mcap tier, sector, asset class)
  5. Enforce social→FA discipline
  6. Attach structured scorecard + position sizing guidance

Claude receives the final scorecard and interprets — does NOT rescore.
"""

from data.scoring_engine import parse_pct, parse_market_cap_string, get_market_cap
from core.catalyst_engine import calculate_catalyst_score
from core.asset_weight_engine import apply_asset_weights


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


def _compute_technical_score(data: dict) -> float:
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


def _compute_catalyst_score(data: dict) -> float:
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


def _compute_social_score(data: dict) -> float:
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


def _get_market_cap_category(data: dict) -> str:
    mc = get_market_cap(data)
    if mc is None:
        return "unknown"
    if mc < MCAP_MICRO_CEILING:
        return "micro"
    if mc < MCAP_SMALL_CEILING:
        return "small"
    return "large"


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
        "technical": 0.25,
        "sentiment": 0.20,
        "fundamentals": 0.30,
        "catalyst": 0.25,
    },
    "neutral": {
        "technical": 0.30,
        "sentiment": 0.20,
        "fundamentals": 0.25,
        "catalyst": 0.25,
    },
}

POSITION_SIZING = {
    "risk_on": {"max_position_pct": "5-8%", "sizing_note": "Full conviction sizing allowed"},
    "risk_off": {"max_position_pct": "2-3%", "sizing_note": "Defensive sizing — protect capital"},
    "inflationary": {"max_position_pct": "3-5%", "sizing_note": "Moderate sizing — inflation hedges favored"},
    "neutral": {"max_position_pct": "3-5%", "sizing_note": "Standard sizing — no regime edge"},
}


def score_candidate(ticker: str, asset: dict, regime_data: dict = None) -> dict:
    """
    Regime-aware deterministic scoring for a single candidate.
    Computes sub-scores, applies regime weight matrix, cross-asset adjustment,
    social discipline, and position sizing guidance.
    Returns structured scorecard — Claude interprets, does NOT rescore.
    """
    regime = (regime_data or {}).get("regime", "neutral")
    weights = REGIME_WEIGHT_MATRIX.get(regime, REGIME_WEIGHT_MATRIX["neutral"])

    technical = _compute_technical_score(asset)
    sector = _compute_sector_score(asset)
    social = _compute_social_score(asset)
    liquidity = _compute_liquidity_score(asset)

    catalyst_result = calculate_catalyst_score(asset)
    catalyst_strength = catalyst_result["catalyst_score"]

    fundamental = _compute_catalyst_score(asset)

    raw_score = (
        weights["technical"] * technical +
        weights["sentiment"] * social +
        weights["fundamentals"] * fundamental +
        weights["catalyst"] * catalyst_strength
    )

    mcap_category = _get_market_cap_category(asset)
    if mcap_category in ("micro", "small"):
        if fundamental >= 70 and sector >= 65:
            raw_score += 5

    social_discipline_flag = None
    if social >= 60:
        if technical < 45 and fundamental < 45:
            raw_score *= 0.85
            social_discipline_flag = "SOCIAL_UNCONFIRMED"

    asset_metadata = {
        "asset_class": _detect_asset_class(asset),
        "market_cap_tier": mcap_category,
        "sector": _extract_sector(asset),
    }
    weight_result = apply_asset_weights(raw_score, asset_metadata, regime)
    adjusted_score = weight_result["adjusted_score"]

    creative_override = False
    if adjusted_score < 50 and social > 85 and catalyst_strength > 60:
        creative_override = True

    position_guide = POSITION_SIZING.get(regime, POSITION_SIZING["neutral"])

    asset["prior_score"] = adjusted_score
    scorecard = {
        "prior_score": adjusted_score,
        "technical_score": round(technical, 1),
        "fundamental_score": round(fundamental, 1),
        "sentiment_score": round(social, 1),
        "catalyst_score": catalyst_strength,
        "catalyst_breakdown": catalyst_result["components"],
        "sector_alignment_score": round(sector, 1),
        "liquidity_score": round(liquidity, 1),
        "market_cap_category": mcap_category,
        "regime": regime,
        "regime_confidence": (regime_data or {}).get("confidence", 0),
        "weight_matrix": weights,
        "regime_multiplier": weight_result["regime_multiplier"],
        "raw_score": weight_result["raw_score"],
        "adjusted_final_score": adjusted_score,
        "position_sizing": position_guide,
    }
    if social_discipline_flag:
        scorecard["social_discipline_flag"] = social_discipline_flag
    if creative_override:
        scorecard["creative_discovery_override"] = True
    asset["institutional_scoring"] = scorecard

    return asset


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


def apply_institutional_scoring(market_data: dict, regime_data: dict = None) -> dict:
    """
    Apply regime-aware institutional scoring to all candidates in market_data.
    Sorts by adjusted_final_score descending but does NOT remove any candidates.
    Attaches regime context and position sizing guidance to the market_data.
    """
    enriched = market_data.get("enriched_data")
    if not enriched or not isinstance(enriched, dict):
        return market_data

    if regime_data is None:
        regime_data = {"regime": "neutral", "confidence": 0}

    scored_count = 0
    for ticker, data in enriched.items():
        if isinstance(data, dict) and "error" not in data:
            score_candidate(ticker.replace("FLAGGED_", ""), data, regime_data)
            scored_count += 1

    if scored_count > 0:
        sorted_tickers = sorted(
            enriched.keys(),
            key=lambda t: enriched[t].get("prior_score", 0) if isinstance(enriched[t], dict) else 0,
            reverse=True,
        )

        sorted_enriched = {t: enriched[t] for t in sorted_tickers}
        market_data["enriched_data"] = sorted_enriched

        scores = [
            (t.replace("FLAGGED_", ""), enriched[t].get("prior_score", 0))
            for t in sorted_tickers[:10]
            if isinstance(enriched[t], dict)
        ]
        print(f"[SCORING] Regime={regime_data.get('regime')} | Scored {scored_count} | "
              f"Top: {scores[:5]}")

    market_data["regime_context"] = regime_data
    market_data["position_sizing"] = POSITION_SIZING.get(
        regime_data.get("regime", "neutral"), POSITION_SIZING["neutral"]
    )

    return market_data
