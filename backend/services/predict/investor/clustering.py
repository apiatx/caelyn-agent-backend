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


def _slim_market(m: dict) -> dict:
    """Return a compact representation of a market for embedding in cluster."""
    return {
        "condition_id": m.get("condition_id", ""),
        "question": m.get("question", ""),
        "yes_pct": m.get("yes_pct"),
        "price_change_1d": m.get("price_change_1d"),
        "price_change_1wk": m.get("price_change_1wk"),
        "volume_24h": m.get("volume_24h"),
        "composite_score": m.get("composite_score"),
        "equity_relevance_score": m.get("equity_relevance_score"),
        "primary_theme_id": m.get("primary_theme_id"),
        "impact_direction_hint": _get_direction_hint(m),
        "slug": m.get("slug", ""),
        "momentum_label": m.get("momentum_label", "flat"),
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

        clusters.append({
            "theme_id": tid,
            "theme_name": theme_def.theme_name,
            "theme_emoji": theme_def.emoji,
            "description": theme_def.description,
            "supporting_markets": [_slim_market(m) for m in top_markets],
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
        })

    # Sort: first by market_count, then by volume
    clusters.sort(key=lambda c: (c["market_count"], c["weighted_volume"]), reverse=True)
    return clusters
