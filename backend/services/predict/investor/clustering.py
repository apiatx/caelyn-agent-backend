"""
Investor Mode — Theme Cluster Aggregation.

Takes a list of classified markets (output of classifier.classify_markets)
and aggregates them into consolidated per-theme clusters.

For each theme cluster, computes:
  - supporting_markets           list of relevant markets (slim format)
  - weighted_odds_shift_24h      volume-weighted avg price_change_1d
  - weighted_odds_shift_7d       volume-weighted avg price_change_1wk
  - weighted_volume              total 24h volume for theme
  - confidence_score             based on market count + theme match scores
  - consistency_score            how aligned markets are in direction
  - contradiction_score          how much they conflict
  - freshness_score              recency of volume activity
  - regime_signal_strength       weighted regime relevance
  - summary_direction            "rising" | "falling" | "mixed" | "unstable"

Pure function — no I/O, no caching, no HTTP.
"""

from __future__ import annotations

import math
from typing import Optional

from services.predict.investor.themes import THEMES, THEME_BY_ID, THEME_IDS

# How many supporting markets to keep per cluster
_MAX_SUPPORTING_MARKETS = 8
_MAX_MARKET_WEIGHT = 1_000_000  # cap individual market volume to avoid dominance

# Direction thresholds
_DIRECTION_THRESHOLD = 0.5     # weighted avg shift > +0.5 pp → rising, < -0.5 → falling
_CONSISTENCY_CONFLICT_GAP = 8  # pp difference for markets to "contradict"


# ── Semantic market classification ────────────────────────────────────────────
#
# Each market is tagged with a semantic_event_type (what kind of real-world
# event YES=1 represents) and a polarity (whether YES rising is directionally
# consistent with the cluster's headline equity read, or inverted).
#
# Polarity matters critically for geopolitics_war_trade:
#   • war/escalation market YES rising   → risk-off  → defense/energy bullish  → polarity "direct"
#   • peace/ceasefire market YES rising  → risk-on   → airlines/consumer bullish → polarity "inverted"
# Both can appear in the same cluster. Without polarity tagging the cluster's
# "Odds rising — 24h: +Xpp" headline is misleading — it conflates opposing reads.

# Ordered rules: first match wins. Patterns are lowercased substrings of question.
_SEMANTIC_KEYWORD_RULES: list[tuple[list[str], str]] = [
    (["ceasefire", "peace deal", "peace agreement", "peace talks", "truce", "armistice",
      "peace treaty", "end the war"], "peace_deal"),
    (["nuclear deal", "nuclear agreement", "jcpoa", "nuclear accord",
      "nuclear framework"], "nuclear_deal"),
    (["de-escalation", "deescalation", "de escalation", "stand down"], "de_escalation"),
    (["nuclear weapon", "nuclear strike", "nuclear bomb", "nuclear attack",
      "tactical nuke", "nuke "], "nuclear_escalation"),
    (["tariff", "trade war", "tariff rate", "tariff exceed", "import duty",
      "trade deficit", "trade deal collapse"], "tariff_escalation"),
    (["sanction", "embargo", "export ban", "asset freeze"], "sanctions_escalation"),
    (["invad", "invasion"], "war_escalation"),
    (["war ", " war", "armed conflict", "airstrike", "bombing", "military strike",
      "ground offensive", "troops enter", "cross the border"], "war_escalation"),
    (["rate cut", "cut rates", "lower rates", "pivot", "reduce rates",
      "ease policy", "rate reduction"], "rate_cut"),
    (["rate hike", "raise rates", "rate increase", "tighten policy"], "rate_hike"),
    (["cpi exceed", "cpi above", "inflation above", "inflation exceed",
      "core inflation above", "inflation hot", "high inflation"], "inflation_hot"),
    (["export control", "chip ban", "chip restriction", "entity list",
      "tech export"], "export_restriction"),
    (["bitcoin above", "btc above", "bitcoin reach", "bitcoin hit",
      "crypto rally", "ath", "all-time high"], "crypto_rally"),
    (["recession", "gdp below", "gdp contraction", "gdp negative",
      "technical recession", "slowdown"], "economic_slowdown"),
    (["supply chain", "shortage", "port disruption", "logistics disruption"], "supply_shock"),
]

# equity regime read implied when YES = 1 for each semantic type
_REGIME_READ_MAP: dict[str, str] = {
    "war_escalation":       "risk_off",
    "nuclear_escalation":   "risk_off",
    "sanctions_escalation": "inflationary",
    "tariff_escalation":    "inflationary",
    "peace_deal":           "risk_on",
    "nuclear_deal":         "risk_on",
    "de_escalation":        "risk_on",
    "rate_cut":             "growth_positive",
    "rate_hike":            "growth_negative",
    "inflation_hot":        "inflationary",
    "export_restriction":   "growth_negative",
    "crypto_rally":         "risk_on",
    "economic_slowdown":    "growth_negative",
    "supply_shock":         "inflationary",
    "general":              "mixed",
}

# semantic_event_types whose YES-rising direction conflicts with the theme's
# standard "rising" equity read.  These markets should be tagged polarity="inverted".
_INVERTED_BY_THEME: dict[str, set[str]] = {
    # geopolitics_war_trade "rising" = escalation; but peace/ceasefire YES rising = de-escalation
    "geopolitics_war_trade": {"peace_deal", "nuclear_deal", "de_escalation"},
    # macro_rates_inflation "rising" = higher-for-longer; but rate_cut YES rising = easing
    "macro_rates_inflation":  {"rate_cut"},
}

# Per-event-type one-sentence equity implication shown in the response
_WHY_DRIVES: dict[str, str] = {
    "peace_deal":           "Peace deal YES odds rising = de-escalation = risk-on; bullish airlines/consumer, bearish defense/gold — opposite of conflict-escalation framing on this card.",
    "nuclear_deal":         "Nuclear deal progress YES rising = geopolitical risk premium unwinding; opposite of the conflict-escalation framing.",
    "de_escalation":        "De-escalation YES odds rising = reduced risk premium; risk-on rotation away from defense/gold toward airlines and consumer names.",
    "war_escalation":       "War escalation YES rising = risk-off shock; defense, energy, gold outperform; airlines and consumer names face headwinds.",
    "nuclear_escalation":   "Nuclear escalation risk rising = extreme risk-off; gold and defensives are the only expected outperformers.",
    "tariff_escalation":    "Tariff/trade war YES rising = inflationary supply shock; pressures import-heavy retail and China-exposed semis.",
    "sanctions_escalation": "Sanctions escalation YES rising = supply/trade disruption; inflationary pressure on affected commodity chains.",
    "rate_cut":             "Rate cut YES rising = easing cycle incoming; bullish for duration assets, REITs, long-growth — inverted vs higher-for-longer framing.",
    "rate_hike":            "Rate hike YES rising = higher-for-longer confirmed; headwind for REITs, long-duration growth, and rate-sensitive names.",
    "inflation_hot":        "Hot inflation outcome rising = higher-for-longer pressure; bearish for duration assets and rate-sensitive growth.",
    "export_restriction":   "Export control tightening YES rising = revenue and supply-chain headwind for semis with China/Taiwan exposure.",
    "economic_slowdown":    "Slowdown/recession odds rising = defensive rotation; gold, utilities, bonds outperform; cyclicals and small caps face headwinds.",
    "crypto_rally":         "Crypto rally odds rising = risk appetite barometer; supportive for high-beta growth and crypto proxies.",
    "supply_shock":         "Supply shock YES rising = inflationary/stagflationary risk; energy and materials benefit; consumer margins at risk.",
    "general":              "This market's outcome direction has equity sector implications tracked via the cluster's weighted probability shift.",
}


def _infer_market_semantics(question: str, theme_id: str) -> tuple[str, str, str, str]:
    """
    Classify a market question into:
        (semantic_event_type, equity_regime_read, polarity, why_this_drives_signal)

    polarity: "direct"   — YES rising = consistent with the cluster's headline equity read
              "inverted" — YES rising = opposite equity implications vs the cluster headline
    """
    q = question.lower()
    event_type = "general"
    for patterns, etype in _SEMANTIC_KEYWORD_RULES:
        if any(p in q for p in patterns):
            event_type = etype
            break

    regime_read = _REGIME_READ_MAP.get(event_type, "mixed")
    inverted_set = _INVERTED_BY_THEME.get(theme_id, set())
    polarity = "inverted" if event_type in inverted_set else "direct"
    why = _WHY_DRIVES.get(event_type, _WHY_DRIVES["general"])
    return event_type, regime_read, polarity, why


def _slim_market(m: dict, theme_id: str = "") -> dict:
    """Return a compact representation of a market for embedding in cluster.

    Includes both legacy fields (for backward-compat) and new diagnostic fields
    (id, source, outcome_label, probabilities, semantic_event_type, polarity, etc.)
    needed to audit exactly what each contributing market is tracking.
    """
    yes_pct   = float(m.get("yes_pct")          or 0.0)
    price_24h = float(m.get("price_change_1d")   or 0.0)
    price_7d  = float(m.get("price_change_1wk")  or 0.0)
    vol_24h   = float(m.get("volume_24h")        or 0.0)
    question  = m.get("question", "")

    event_type, regime_read, polarity, why = _infer_market_semantics(question, theme_id)

    # contribution_score: volume × |24h move| — markets that are both active and
    # moving meaningfully rank as primary drivers
    contribution = round(min(vol_24h, _MAX_MARKET_WEIGHT) * abs(price_24h), 1)

    return {
        # ── legacy fields (unchanged keys, preserved for backward-compat) ──
        "condition_id":         m.get("condition_id", ""),
        "question":             question,
        "yes_pct":              yes_pct,
        "price_change_1d":      price_24h,
        "price_change_1wk":     price_7d,
        "volume_24h":           vol_24h,
        "composite_score":      m.get("composite_score"),
        "equity_relevance_score": m.get("equity_relevance_score"),
        "primary_theme_id":     m.get("primary_theme_id"),
        "impact_direction_hint": _get_direction_hint(m),
        "slug":                 m.get("slug", ""),
        "momentum_label":       m.get("momentum_label", "flat"),
        # ── diagnostic fields (new) ────────────────────────────────────────
        "id":                   m.get("condition_id", ""),
        "source":               "polymarket",
        "outcome_label":        "Yes",
        "current_probability":  round(yes_pct, 2),
        "probability_24h_ago":  round(yes_pct - price_24h, 2),
        "probability_7d_ago":   round(yes_pct - price_7d, 2),
        "delta_24h_pp":         round(price_24h, 2),
        "delta_7d_pp":          round(price_7d, 2),
        "direction":            _get_direction_hint(m),
        "semantic_event_type":  event_type,
        "equity_regime_read":   regime_read,
        "polarity":             polarity,
        "contribution_score":   contribution,
        "why_this_drives_signal": why,
        # mapped_bullish_sectors / mapped_bearish_sectors are added in investor_intel.py
        # where impact_engine is available
    }


def _get_direction_hint(m: dict) -> str:
    pc = m.get("price_change_1d", 0) or 0
    if pc > 1.5:
        return "rising"
    elif pc < -1.5:
        return "falling"
    return "neutral"


def _weighted_avg(values: list[float], weights: list[float]) -> float:
    """Volume-weighted average.  Returns 0 if total weight is 0."""
    total_w = sum(weights)
    if total_w == 0:
        return 0.0
    return sum(v * w for v, w in zip(values, weights)) / total_w


def _compute_consistency(shifts: list[float]) -> tuple[float, float]:
    """
    Given a list of 24h price shifts for markets in a cluster,
    return (consistency_score, contradiction_score) both 0-100.

    consistency_score: 100 if all markets move in same direction
    contradiction_score: 100 if markets actively disagree
    """
    if len(shifts) < 2:
        return (100.0, 0.0)

    n_rising = sum(1 for s in shifts if s > 1.0)
    n_falling = sum(1 for s in shifts if s < -1.0)
    n_neutral = len(shifts) - n_rising - n_falling
    n = len(shifts)

    dominant = max(n_rising, n_falling, n_neutral)
    consistency = round((dominant / n) * 100, 1)

    # Contradiction: both rising and falling markets present
    both_directions = min(n_rising, n_falling)
    contradiction = round(min(100.0, (both_directions / n) * 200), 1)

    return (consistency, contradiction)


def _freshness_score(markets: list[dict]) -> float:
    """
    Score 0-100 based on how active markets in the cluster are.
    Active = high 24h volume relative to 7d average.
    """
    if not markets:
        return 0.0
    scores = []
    for m in markets:
        vol_24h = m.get("volume_24h", 0) or 0
        vol_7d = m.get("volume_7d", 0) or 0
        if vol_7d > 0:
            ratio = vol_24h / (vol_7d / 7)   # 24h vs avg daily 7d
            scores.append(min(100.0, ratio * 50))  # ratio of 2x = 100
        elif vol_24h > 1000:
            scores.append(60.0)
        else:
            scores.append(10.0)
    return round(sum(scores) / len(scores), 1)


def _regime_signal_strength(markets: list[dict]) -> float:
    """Avg regime_relevance_score across supporting markets."""
    if not markets:
        return 0.0
    scores = [m.get("regime_relevance_score", 0) or 0 for m in markets]
    return round(sum(scores) / len(scores), 1)


def _summary_direction(
    weighted_shift: float,
    consistency: float,
    contradiction: float,
) -> str:
    """
    Determine the cluster direction label.

    Returns: "rising" | "falling" | "mixed" | "unstable"
    """
    if contradiction > 40:
        return "unstable"
    if consistency < 50:
        return "mixed"
    if weighted_shift > _DIRECTION_THRESHOLD:
        return "rising"
    elif weighted_shift < -_DIRECTION_THRESHOLD:
        return "falling"
    return "mixed"


def _confidence_score(
    market_count: int,
    consistency: float,
    avg_equity_relevance: float,
    contradiction: float,
) -> float:
    """
    Confidence in the cluster's signal.

    Factors:
    - More markets = more confidence (up to ~10)
    - High consistency = more confidence
    - High avg equity relevance = more confidence
    - High contradiction = less confidence
    """
    count_factor = min(40.0, market_count * 5)
    consistency_factor = consistency * 0.3
    relevance_factor = avg_equity_relevance * 0.2
    contradiction_penalty = contradiction * 0.3

    raw = count_factor + consistency_factor + relevance_factor - contradiction_penalty
    return round(max(0.0, min(100.0, raw)), 1)


def build_theme_clusters(classified_markets: list[dict]) -> list[dict]:
    """
    Aggregate equity-relevant classified markets into per-theme clusters.

    Returns a list of theme cluster dicts, sorted by (market_count desc,
    weighted_volume desc).  Only includes clusters with >= 1 supporting market.
    """
    # Group markets by theme
    theme_markets: dict[str, list[dict]] = {tid: [] for tid in THEME_IDS}

    for m in classified_markets:
        if not m.get("is_equity_relevant"):
            continue
        for theme_entry in m.get("themes", []):
            tid = theme_entry["theme_id"]
            if tid in theme_markets:
                theme_markets[tid].append(m)

    clusters: list[dict] = []

    for tid in THEME_IDS:
        markets = theme_markets[tid]
        if not markets:
            continue

        theme_def = THEME_BY_ID[tid]

        # Sort by equity_relevance * composite_score desc, take top N
        markets.sort(
            key=lambda m: (m.get("equity_relevance_score", 0) * (m.get("composite_score", 0) or 50)),
            reverse=True,
        )
        top_markets = markets[:_MAX_SUPPORTING_MARKETS]

        # Volume-weighted price shifts
        vol_weights = [min(_MAX_MARKET_WEIGHT, m.get("volume_24h", 0) or 0) for m in top_markets]
        shifts_24h = [m.get("price_change_1d", 0) or 0 for m in top_markets]
        shifts_7d = [m.get("price_change_1wk", 0) or 0 for m in top_markets]

        w_shift_24h = _weighted_avg(shifts_24h, vol_weights)
        w_shift_7d = _weighted_avg(shifts_7d, vol_weights)

        total_volume = sum(m.get("volume_24h", 0) or 0 for m in markets)

        consistency, contradiction = _compute_consistency(shifts_24h)

        avg_equity = sum(m.get("equity_relevance_score", 0) or 0 for m in top_markets) / len(top_markets)
        freshness = _freshness_score(top_markets)
        regime_strength = _regime_signal_strength(top_markets)

        direction = _summary_direction(w_shift_24h, consistency, contradiction)
        confidence = _confidence_score(len(markets), consistency, avg_equity, contradiction)

        slim_markets = [_slim_market(m, tid) for m in top_markets]

        # ── Signal integrity analysis ──────────────────────────────────────
        # Detect whether contributing markets have conflicting semantic types
        # or opposing equity polarities — both invalidate a clean directional call.
        event_types_present = {sm["semantic_event_type"] for sm in slim_markets}
        polarities_present  = {sm["polarity"] for sm in slim_markets}

        # Mixed semantics: more than one substantive event type in the cluster
        nontrivial_types = event_types_present - {"general"}
        has_mixed_semantics = len(nontrivial_types) > 1

        # Polarity conflict: cluster contains both "direct" and "inverted" markets
        # This means some markets' YES rising is risk-on while others' is risk-off
        has_polarity_conflict = len(polarities_present) > 1

        integrity_warning: Optional[str] = None
        if has_polarity_conflict:
            direct_types   = sorted({sm["semantic_event_type"] for sm in slim_markets if sm["polarity"] == "direct"   and sm["semantic_event_type"] != "general"})
            inverted_types = sorted({sm["semantic_event_type"] for sm in slim_markets if sm["polarity"] == "inverted" and sm["semantic_event_type"] != "general"})
            integrity_warning = (
                f"Cluster contains markets with opposing equity implications: "
                f"{direct_types or ['escalation/direct']} (risk-off/direct) vs "
                f"{inverted_types or ['de-escalation/inverted']} (risk-on/inverted). "
                f"Headline sector call may be directionally misleading."
            )

        # Primary driver: market with highest contribution_score (volume × |delta_24h|)
        primary_driver = max(slim_markets, key=lambda m: m.get("contribution_score", 0)) if slim_markets else {}

        clusters.append({
            "theme_id": tid,
            "theme_name": theme_def.theme_name,
            "theme_emoji": theme_def.emoji,
            "description": theme_def.description,
            "supporting_markets": slim_markets,
            "market_count": len(markets),
            "weighted_odds_shift_24h": round(w_shift_24h, 2),
            "weighted_odds_shift_7d": round(w_shift_7d, 2),
            "weighted_volume": round(total_volume, 0),
            "confidence_score": confidence,
            "consistency_score": round(consistency, 1),
            "contradiction_score": round(contradiction, 1),
            "freshness_score": round(freshness, 1),
            "regime_signal_strength": round(regime_strength, 1),
            "summary_direction": direction,
            "avg_equity_relevance": round(avg_equity, 1),
            # ── new integrity fields ───────────────────────────────────────
            "primary_driver_market":  primary_driver,
            "has_mixed_semantics":    has_mixed_semantics,
            "has_polarity_conflict":  has_polarity_conflict,
            "signal_integrity_warning": integrity_warning,
        })

    # Sort: first by market_count, then by volume
    clusters.sort(key=lambda c: (c["market_count"], c["weighted_volume"]), reverse=True)
    return clusters
