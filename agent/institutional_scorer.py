"""
Institutional Prior Scoring Engine.
Runs AFTER data gathering, BEFORE Claude sees candidates.

This is a SOFT scoring layer — enrichment only, no filtering.
It adds prior_score metadata to help Claude rank consistently
while preserving creative edge and discovery optionality.

Scoring Formula (institutional baseline):
  prior_score = 0.30 * technical + 0.30 * catalyst + 0.20 * sector
              + 0.10 * social + 0.10 * liquidity

Asymmetry boost: +5 for micro/small caps with strong catalyst + sector alignment.

Claude receives the breakdown and may re-rank with justification.
"""

from data.scoring_engine import parse_pct, parse_market_cap_string, get_market_cap


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


def score_candidate(ticker: str, asset: dict) -> dict:
    """
    Enrich a single candidate with institutional prior scoring metadata.
    Does NOT filter assets out. This is enrichment only.
    """
    technical = _compute_technical_score(asset)
    catalyst = _compute_catalyst_score(asset)
    sector = _compute_sector_score(asset)
    social = _compute_social_score(asset)
    liquidity = _compute_liquidity_score(asset)

    final_score = (
        0.30 * technical +
        0.30 * catalyst +
        0.20 * sector +
        0.10 * social +
        0.10 * liquidity
    )

    mcap_category = _get_market_cap_category(asset)
    if mcap_category in ("micro", "small"):
        if catalyst >= 70 and sector >= 65:
            final_score += 5

    asset["prior_score"] = round(final_score, 1)
    asset["institutional_scoring"] = {
        "prior_score": round(final_score, 1),
        "technical_score": round(technical, 1),
        "catalyst_score": round(catalyst, 1),
        "sector_alignment_score": round(sector, 1),
        "social_score": round(social, 1),
        "liquidity_score": round(liquidity, 1),
        "market_cap_category": mcap_category,
    }

    return asset


def apply_institutional_scoring(market_data: dict) -> dict:
    """
    Apply soft institutional scoring to all candidates in market_data.
    Works on the enriched_data dict returned by wide_scan_and_rank
    and similar data gathering functions.

    Sorts by prior_score descending but does NOT remove any candidates.
    Returns the market_data dict with scoring metadata injected.
    """
    enriched = market_data.get("enriched_data")
    if not enriched or not isinstance(enriched, dict):
        return market_data

    scored_count = 0
    for ticker, data in enriched.items():
        if isinstance(data, dict) and "error" not in data:
            score_candidate(ticker.replace("FLAGGED_", ""), data)
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
        print(f"[PRIOR SCORING] Scored {scored_count} candidates | "
              f"Top: {scores[:5]}")

    return market_data
