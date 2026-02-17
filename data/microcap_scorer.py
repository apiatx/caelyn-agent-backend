"""
Microcap Asymmetric Scoring Engine for trending pipeline.
Runs BEFORE Claude — pure math, no AI calls.

Two-tier conviction system:
1. Institutional Conviction: For large/mid caps (>$2B). Strict filters, liquidity-heavy, confirmed strength.
2. Asymmetric Opportunity: For small/micro caps (<$2B). Catalyst-driven, sector-aligned, early inflection.

Microcap Score Formula:
  FinalScore = 0.35 * CatalystStrength + 0.25 * SectorAlignment + 0.20 * EarlyTechnicalShift
             + 0.15 * SocialMomentum + 0.05 * Liquidity

Hard sanity filters:
  - Market cap floor: $50M (no OTC penny trash)
  - Must have at least one verifiable catalyst signal
  - Catalyst score >= 40 AND SectorAlignment >= 35 (relaxed for discovery, Claude validates)
"""

from data.scoring_engine import parse_pct, parse_market_cap_string, get_market_cap


MCAP_FLOOR = 50_000_000
MCAP_MICRO_CEILING = 500_000_000
MCAP_SMALL_CEILING = 2_000_000_000

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
    "launch", "product launch", "revenue", "earnings", "beat", "guidance",
    "upgrade", "initiated", "coverage", "target", "raised", "buy rating",
    "government", "dod", "defense contract", "grant", "funding",
    "ipo", "listing", "uplisting", "nasdaq", "nyse",
    "insider buying", "insider", "buyback", "repurchase",
    "trial", "phase 3", "phase 2", "clinical", "results", "data",
    "ai", "artificial intelligence", "llm", "gpu",
]


def score_microcap(ticker: str, enriched_data: dict, x_analysis: dict = None,
                   source_count: int = 0, sources: list = None) -> dict:
    overview = enriched_data.get("overview", {})
    if not isinstance(overview, dict):
        overview = {}
    st_sentiment = enriched_data.get("stocktwits_sentiment", {})
    if not isinstance(st_sentiment, dict):
        st_sentiment = {}
    analyst = enriched_data.get("analyst_ratings", {})
    if not isinstance(analyst, dict):
        analyst = {}

    mcap = get_market_cap({"overview": overview})
    if mcap is None:
        mc_str = overview.get("market_cap")
        if mc_str:
            mcap = parse_market_cap_string(mc_str)

    tier = _classify_tier(mcap)

    if tier == "institutional":
        return {
            "ticker": ticker,
            "tier": "institutional",
            "mcap": mcap,
            "microcap_score": None,
            "scoring_mode": "institutional",
            "reason": "Large/mid-cap — use standard institutional scoring",
        }

    if mcap is not None and mcap < MCAP_FLOOR:
        return {
            "ticker": ticker,
            "tier": "rejected",
            "mcap": mcap,
            "microcap_score": 0,
            "scoring_mode": "rejected",
            "reason": f"Below ${MCAP_FLOOR/1e6:.0f}M floor — OTC/penny risk",
            "disqualified": True,
        }

    catalyst_score, catalyst_details = _score_catalyst(ticker, overview, analyst, x_analysis)
    sector_score, sector_details = _score_sector_alignment(overview)
    technical_score, technical_details = _score_early_technical(overview, enriched_data)
    social_score, social_details = _score_social_momentum(st_sentiment, source_count, sources, x_analysis)
    liquidity_score, liquidity_details = _score_liquidity(overview)

    final_score = round(
        0.35 * catalyst_score +
        0.25 * sector_score +
        0.20 * technical_score +
        0.15 * social_score +
        0.05 * liquidity_score,
        1
    )

    passes_filters = catalyst_score >= 40 and sector_score >= 35

    return {
        "ticker": ticker,
        "tier": tier,
        "mcap": mcap,
        "mcap_formatted": _format_mcap(mcap),
        "microcap_score": final_score,
        "scoring_mode": "asymmetric_opportunity",
        "passes_sanity": passes_filters,
        "breakdown": {
            "catalyst": {"score": catalyst_score, "weight": "35%", "details": catalyst_details},
            "sector_alignment": {"score": sector_score, "weight": "25%", "details": sector_details},
            "early_technical": {"score": technical_score, "weight": "20%", "details": technical_details},
            "social_momentum": {"score": social_score, "weight": "15%", "details": social_details},
            "liquidity": {"score": liquidity_score, "weight": "5%", "details": liquidity_details},
        },
        "power_law_flag": final_score >= 65 and passes_filters,
        "disqualified": not passes_filters,
    }


def _classify_tier(mcap: float = None) -> str:
    if mcap is None:
        return "micro_cap"
    if mcap < MCAP_MICRO_CEILING:
        return "micro_cap"
    if mcap < MCAP_SMALL_CEILING:
        return "small_cap"
    return "institutional"


def _format_mcap(mcap: float = None) -> str:
    if mcap is None:
        return "Unknown"
    if mcap >= 1e9:
        return f"${mcap/1e9:.1f}B"
    return f"${mcap/1e6:.0f}M"


def _score_catalyst(ticker: str, overview: dict, analyst: dict,
                    x_analysis: dict = None) -> tuple[float, dict]:
    score = 0
    signals = []

    x_catalyst = ""
    x_why = ""
    if x_analysis:
        x_catalyst = (x_analysis.get("x_catalyst") or "").lower()
        x_why = (x_analysis.get("x_why_trending") or "").lower()
        x_narratives = x_analysis.get("x_narratives", [])
        x_combined = x_catalyst + " " + x_why + " " + " ".join(str(n).lower() for n in x_narratives)

        catalyst_hits = sum(1 for kw in CATALYST_KEYWORDS if kw in x_combined)
        if catalyst_hits >= 3:
            score += 45
            signals.append(f"Strong X catalyst ({catalyst_hits} signals)")
        elif catalyst_hits >= 2:
            score += 35
            signals.append(f"Moderate X catalyst ({catalyst_hits} signals)")
        elif catalyst_hits >= 1:
            score += 20
            signals.append(f"Weak X catalyst ({catalyst_hits} signal)")

        sentiment_score = x_analysis.get("x_sentiment_score", 0)
        if isinstance(sentiment_score, (int, float)) and sentiment_score >= 80:
            score += 10
            signals.append(f"High X sentiment ({sentiment_score})")

        intensity = x_analysis.get("x_mention_intensity", "").lower()
        if intensity in ("high", "very_high", "extreme"):
            score += 10
            signals.append(f"High mention intensity")

    rev_growth = parse_pct(overview.get("revenue_growth"))
    if rev_growth is not None:
        if rev_growth > 1.0:
            score += 15
            signals.append(f"Revenue +{rev_growth*100:.0f}% (hypergrowth)")
        elif rev_growth > 0.5:
            score += 12
            signals.append(f"Revenue +{rev_growth*100:.0f}% (strong)")
        elif rev_growth > 0.2:
            score += 8
            signals.append(f"Revenue +{rev_growth*100:.0f}%")
        elif rev_growth > 0:
            score += 4
            signals.append(f"Revenue +{rev_growth*100:.0f}% (moderate)")

    total_analysts = analyst.get("total_analysts")
    consensus = (analyst.get("consensus") or "").lower()
    if total_analysts and int(total_analysts) >= 3 and consensus in ("buy", "strong buy"):
        score += 10
        signals.append(f"Analyst consensus: {consensus} ({total_analysts} analysts)")

    upside = analyst.get("upside_downside")
    if upside:
        try:
            up_val = float(str(upside).replace("%", "").replace("+", ""))
            if up_val > 50:
                score += 10
                signals.append(f"Analyst upside: +{up_val:.0f}%")
            elif up_val > 25:
                score += 5
                signals.append(f"Analyst upside: +{up_val:.0f}%")
        except (TypeError, ValueError):
            pass

    return min(score, 100), {"signals": signals, "raw_score": score}


def _score_sector_alignment(overview: dict) -> tuple[float, dict]:
    score = 0
    details = {}

    sector = (overview.get("sector") or "").lower().strip()
    industry = (overview.get("industry") or "").lower().strip()
    company = (overview.get("company_name") or overview.get("name") or "").lower()

    combined = f"{sector} {industry} {company}"
    details["sector"] = sector or "unknown"
    details["industry"] = industry or "unknown"

    hot_matches = [s for s in HOT_SECTORS if s in combined]
    if hot_matches:
        if len(hot_matches) >= 2:
            score += 80
            details["alignment"] = f"Strong multi-match: {', '.join(hot_matches[:3])}"
        else:
            score += 60
            details["alignment"] = f"Sector match: {hot_matches[0]}"
    elif sector in ("consumer cyclical", "consumer defensive", "utilities", "real estate"):
        score += 20
        details["alignment"] = "Cold sector — limited re-rating potential"
    elif sector:
        score += 35
        details["alignment"] = f"Neutral sector: {sector}"
    else:
        score += 25
        details["alignment"] = "Unknown sector"

    return min(score, 100), details


def _score_early_technical(overview: dict, enriched_data: dict) -> tuple[float, dict]:
    score = 0
    signals = []

    price_str = overview.get("prev_close") or overview.get("open")
    w52_low_str = overview.get("week_52_low")
    w52_high_str = overview.get("week_52_high")

    if price_str and w52_low_str and w52_high_str:
        try:
            price = float(str(price_str).replace("$", "").replace(",", ""))
            w52_low = float(str(w52_low_str).replace("$", "").replace(",", ""))
            w52_high = float(str(w52_high_str).replace("$", "").replace(",", ""))

            if w52_high > w52_low:
                range_pct = (price - w52_low) / (w52_high - w52_low) * 100
                from_low = (price - w52_low) / w52_low * 100 if w52_low > 0 else 0
                from_high = (w52_high - price) / w52_high * 100 if w52_high > 0 else 0

                if 20 <= range_pct <= 50:
                    score += 35
                    signals.append(f"Mid-range ({range_pct:.0f}% of 52w) — inflection zone")
                elif 50 < range_pct <= 70:
                    score += 25
                    signals.append(f"Upper-mid range ({range_pct:.0f}% of 52w)")
                elif 10 <= range_pct < 20:
                    score += 20
                    signals.append(f"Near 52w low ({range_pct:.0f}%) — deep value or broken")
                elif range_pct > 85:
                    score += 10
                    signals.append(f"Near 52w high ({range_pct:.0f}%) — extended")
                else:
                    score += 15
                    signals.append(f"Low range ({range_pct:.0f}%)")

                if from_high > 30:
                    score += 10
                    signals.append(f"{from_high:.0f}% below 52w high — room to run")
        except (TypeError, ValueError):
            pass

    days_range = overview.get("days_range")
    if days_range and isinstance(days_range, str) and "-" in days_range:
        try:
            parts = days_range.replace("$", "").replace(",", "").split("-")
            day_low = float(parts[0].strip())
            day_high = float(parts[1].strip())
            if day_low > 0:
                intraday_range = (day_high - day_low) / day_low * 100
                if intraday_range > 10:
                    score += 20
                    signals.append(f"Wide intraday range ({intraday_range:.1f}%) — volatility surge")
                elif intraday_range > 5:
                    score += 12
                    signals.append(f"Elevated intraday range ({intraday_range:.1f}%)")
                elif intraday_range > 3:
                    score += 5
                    signals.append(f"Normal range ({intraday_range:.1f}%)")
        except (TypeError, ValueError):
            pass

    change_pct = overview.get("change_pct")
    if change_pct is None:
        price_now = overview.get("price") or overview.get("prev_close")
        prev_close = overview.get("prev_close") or overview.get("open")
        if price_now and prev_close:
            try:
                pn = float(str(price_now).replace("$", "").replace(",", ""))
                pc = float(str(prev_close).replace("$", "").replace(",", ""))
                if pc > 0:
                    change_pct = ((pn - pc) / pc) * 100
            except (TypeError, ValueError):
                pass

    if change_pct is not None:
        try:
            chg = float(change_pct)
            if 5 <= chg <= 25:
                score += 15
                signals.append(f"Strong day move (+{chg:.1f}%) — breakout potential")
            elif 25 < chg <= 60:
                score += 10
                signals.append(f"Extreme move (+{chg:.1f}%) — momentum but chase risk")
            elif 2 <= chg < 5:
                score += 8
                signals.append(f"Moderate move (+{chg:.1f}%)")
            elif chg > 60:
                score += 3
                signals.append(f"Parabolic (+{chg:.1f}%) — late entry risk")
        except (TypeError, ValueError):
            pass

    return min(score, 100), {"signals": signals, "raw_score": score}


def _score_social_momentum(st_sentiment: dict, source_count: int,
                           sources: list = None, x_analysis: dict = None) -> tuple[float, dict]:
    score = 0
    signals = []

    if source_count >= 4:
        score += 30
        signals.append(f"Cross-platform: {source_count} sources")
    elif source_count >= 3:
        score += 22
        signals.append(f"Multi-platform: {source_count} sources")
    elif source_count >= 2:
        score += 12
        signals.append(f"Dual-platform: {source_count} sources")
    elif source_count >= 1:
        score += 5
        signals.append(f"Single source")

    bull_pct = st_sentiment.get("bull_pct") or st_sentiment.get("bullish_pct")
    if bull_pct is not None:
        try:
            bp = float(bull_pct)
            if bp >= 80:
                score += 25
                signals.append(f"StockTwits {bp:.0f}% bullish")
            elif bp >= 65:
                score += 18
                signals.append(f"StockTwits {bp:.0f}% bullish")
            elif bp >= 50:
                score += 10
                signals.append(f"StockTwits {bp:.0f}% bullish (moderate)")
        except (TypeError, ValueError):
            pass

    if x_analysis:
        x_sent = (x_analysis.get("x_sentiment") or "").lower()
        if "bullish" in x_sent or "strong" in x_sent:
            score += 20
            signals.append("Bullish X sentiment")
        elif "positive" in x_sent or "mixed" in x_sent:
            score += 8
            signals.append("Mixed X sentiment")

        intensity = (x_analysis.get("x_mention_intensity") or "").lower()
        if intensity in ("high", "very_high", "extreme"):
            score += 15
            signals.append(f"High X mention intensity")
        elif intensity == "medium":
            score += 5

    has_reddit = sources and any("Reddit" in s for s in sources)
    has_st = sources and any("StockTwits" in s for s in sources)
    has_x = sources and any("X_Twitter" in s for s in sources)
    platform_coverage = sum([has_reddit, has_st, has_x])
    if platform_coverage >= 3:
        score += 10
        signals.append("Triple-platform buzz (X + StockTwits + Reddit)")

    return min(score, 100), {"signals": signals, "raw_score": score}


def _score_liquidity(overview: dict) -> tuple[float, dict]:
    score = 0
    details = {}

    avg_vol = overview.get("avg_volume")
    if avg_vol:
        try:
            vol = float(str(avg_vol).replace(",", ""))
            if vol >= 5_000_000:
                score += 100
                details["avg_volume"] = f"{vol/1e6:.1f}M (excellent)"
            elif vol >= 1_000_000:
                score += 80
                details["avg_volume"] = f"{vol/1e6:.1f}M (good)"
            elif vol >= 500_000:
                score += 60
                details["avg_volume"] = f"{vol/1e3:.0f}K (adequate)"
            elif vol >= 100_000:
                score += 40
                details["avg_volume"] = f"{vol/1e3:.0f}K (thin)"
            elif vol >= 50_000:
                score += 20
                details["avg_volume"] = f"{vol/1e3:.0f}K (very thin)"
            else:
                score += 5
                details["avg_volume"] = f"{vol/1e3:.0f}K (illiquid)"
        except (TypeError, ValueError):
            details["avg_volume"] = "unparseable"

    return min(score, 100), details


def score_trending_tickers(enriched_data: dict, xai_top_picks: list,
                           ticker_sources: dict) -> dict:
    xai_lookup = {p["ticker"]: p for p in xai_top_picks}
    results = {
        "asymmetric_opportunities": [],
        "institutional_plays": [],
        "rejected": [],
        "power_law_candidates": [],
    }

    for ticker, data in enriched_data.items():
        x_analysis = xai_lookup.get(ticker) or data.get("x_analysis")
        sources = list(ticker_sources.get(ticker, []))
        source_count = len(sources)

        result = score_microcap(ticker, data, x_analysis, source_count, sources)

        if result.get("disqualified"):
            if result["tier"] == "rejected":
                results["rejected"].append(result)
            else:
                result["reason"] = "Failed sanity filters (catalyst or sector too weak)"
                results["rejected"].append(result)
        elif result["tier"] == "institutional":
            results["institutional_plays"].append(result)
        else:
            results["asymmetric_opportunities"].append(result)
            if result.get("power_law_flag"):
                results["power_law_candidates"].append(result)

    results["asymmetric_opportunities"].sort(
        key=lambda x: x.get("microcap_score", 0), reverse=True
    )

    return results
