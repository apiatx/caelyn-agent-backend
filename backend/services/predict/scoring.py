"""
Prophetik Signal Engine — Normalized Scoring Module.

Computes seven independent scoring dimensions for each prediction market:

    1. Conviction Score      (0-100)  — distance from 50/50
    2. Momentum Score        (0-100)  — price movement strength + persistence
    3. Flow Score            (0-100)  — volume intensity + burstiness
    4. Execution Quality     (0-100)  — spread tightness + liquidity depth
    5. Participation Quality (0-100)  — breadth of market participation
    6. Time Quality          (0-100)  — expiry profile vs signal type
    7. Trap Risk             (0-100)  — crowdedness / fake-move danger (higher = worse)

Also produces a composite_score (weighted blend) and a momentum_label.

All functions are pure — no I/O, no caching, no HTTP. Feed in an enriched
market dict (from PolymarketIntelligence._enrich_market) and get scores back.

Usage:
    from services.predict.scoring import score_market, score_markets

    scored = score_market(enriched_market_dict)
    # scored["scores"] = { conviction, momentum, flow, ... }
    # scored["composite_score"] = 72.3
    # scored["momentum_label"] = "strong_up"
"""

from __future__ import annotations

import logging
import math
from typing import Optional

logger = logging.getLogger("prophetik.scoring")

# ── Weight config for composite score ────────────────────────────────────────
WEIGHTS = {
    "conviction":           0.15,
    "momentum":             0.20,
    "flow":                 0.20,
    "execution_quality":    0.15,
    "participation_quality": 0.10,
    "time_quality":         0.10,
    "trap_risk":            0.10,  # subtracted, not added
}


# ── Individual Scoring Dimensions ────────────────────────────────────────────

def _conviction_score(yes_pct: float) -> float:
    """
    Conviction Score (0-100).
    Markets far from 50/50 have stronger consensus conviction.
    A market at 95% YES or 5% YES both get high conviction.
    Markets pinned at 0 or 100 with no volume get penalized later by trap_risk.
    """
    if yes_pct is None:
        return 0.0
    distance = abs(yes_pct - 50.0)
    # Scale: 0 distance = 0 conviction, 50 distance = 100 conviction
    return round(min(100.0, (distance / 50.0) * 100.0), 1)


def _momentum_score(
    price_change_1h: float,
    price_change_24h: float,
    price_change_1wk: float,
    volume_momentum: str,
    yes_pct: float,
) -> tuple[float, str]:
    """
    Momentum Score (0-100) + momentum_label.
    Rewards acceleration and persistence. Penalizes stale pinned markets.
    Returns (score, label).
    """
    # Avoid rewarding stale pinned markets at 0% or 100%
    if yes_pct is not None and (yes_pct <= 1.0 or yes_pct >= 99.0):
        # If there's no recent movement, this is stale, not momentum
        if abs(price_change_1h) < 0.5 and abs(price_change_24h) < 1.0:
            return (5.0, "stale_pinned")

    score = 0.0

    # 1h movement (most recent, highest weight)
    abs_1h = abs(price_change_1h)
    if abs_1h > 10:
        score += 35
    elif abs_1h > 5:
        score += 25
    elif abs_1h > 2:
        score += 15
    elif abs_1h > 0.5:
        score += 8

    # 24h movement
    abs_24h = abs(price_change_24h)
    if abs_24h > 20:
        score += 30
    elif abs_24h > 10:
        score += 22
    elif abs_24h > 5:
        score += 15
    elif abs_24h > 1:
        score += 8

    # 7d movement — persistence bonus
    abs_7d = abs(price_change_1wk)
    if abs_7d > 15:
        score += 20
    elif abs_7d > 8:
        score += 12
    elif abs_7d > 3:
        score += 6

    # Acceleration bonus: 1h move > half of 24h move = accelerating
    if abs_24h > 2 and abs_1h > abs_24h * 0.4:
        score += 10

    # Persistence bonus: 24h and 7d in the same direction
    if price_change_24h != 0 and price_change_1wk != 0:
        if (price_change_24h > 0) == (price_change_1wk > 0):
            score += 5

    # Volume momentum amplifier
    if volume_momentum == "surging":
        score = min(100, score * 1.2)
    elif volume_momentum == "accelerating":
        score = min(100, score * 1.1)
    elif volume_momentum == "fading":
        score *= 0.7

    score = round(max(0.0, min(100.0, score)), 1)

    # Label
    if score >= 70:
        label = "strong_up" if price_change_24h > 0 else "strong_down"
    elif score >= 45:
        label = "moderate_up" if price_change_24h > 0 else "moderate_down"
    elif score >= 20:
        label = "mild_up" if price_change_24h >= 0 else "mild_down"
    else:
        label = "flat"

    return (score, label)


def _flow_score(
    volume_24h: float,
    volume_1wk: float,
    vol_liq_ratio: float,
    volume_momentum: str,
) -> float:
    """
    Flow Score (0-100).
    Based on 24h volume magnitude, volume/liquidity ratio, and burstiness.
    """
    score = 0.0

    # Absolute volume tier
    if volume_24h > 1_000_000:
        score += 35
    elif volume_24h > 500_000:
        score += 28
    elif volume_24h > 100_000:
        score += 20
    elif volume_24h > 50_000:
        score += 14
    elif volume_24h > 10_000:
        score += 8
    elif volume_24h > 1_000:
        score += 3

    # Volume relative to weekly average (burstiness proxy)
    avg_daily_7d = volume_1wk / 7 if volume_1wk > 0 else 0
    if avg_daily_7d > 0:
        burst_ratio = volume_24h / avg_daily_7d
        if burst_ratio > 5.0:
            score += 30
        elif burst_ratio > 3.0:
            score += 22
        elif burst_ratio > 2.0:
            score += 15
        elif burst_ratio > 1.3:
            score += 8

    # Volume-to-liquidity ratio: higher = more aggressive flow
    if vol_liq_ratio > 10.0:
        score += 20
    elif vol_liq_ratio > 5.0:
        score += 15
    elif vol_liq_ratio > 2.0:
        score += 8
    elif vol_liq_ratio > 1.0:
        score += 4

    # Volume momentum amplifier
    if volume_momentum == "surging":
        score += 10
    elif volume_momentum == "accelerating":
        score += 5
    elif volume_momentum == "fading":
        score -= 10

    return round(max(0.0, min(100.0, score)), 1)


def _execution_quality_score(
    spread: float,
    spread_pct: float,
    liquidity: float,
    market_efficiency_score: float,
    best_bid: float,
    best_ask: float,
) -> float:
    """
    Execution Quality Score (0-100).
    Tighter spread + deeper liquidity + lower slippage = better execution.
    Down-ranks ugly-fill markets.
    """
    score = 0.0

    # Spread tightness (biggest factor)
    if spread <= 0.01:
        score += 35
    elif spread <= 0.02:
        score += 28
    elif spread <= 0.03:
        score += 22
    elif spread <= 0.05:
        score += 15
    elif spread <= 0.10:
        score += 8
    else:
        score += 2  # very wide spread

    # Liquidity depth
    if liquidity > 500_000:
        score += 30
    elif liquidity > 200_000:
        score += 22
    elif liquidity > 100_000:
        score += 16
    elif liquidity > 50_000:
        score += 10
    elif liquidity > 10_000:
        score += 5

    # Existing efficiency score blend (already considers competitive flag)
    score += market_efficiency_score * 0.2

    # Bid-ask presence bonus
    if best_bid > 0 and best_ask > 0:
        score += 5
        # Tight bid-ask spread bonus
        actual_spread = best_ask - best_bid
        if 0 < actual_spread <= 0.02:
            score += 5

    return round(max(0.0, min(100.0, score)), 1)


def _participation_quality_score(
    is_competitive: bool,
    liquidity: float,
    volume_24h: float,
    vol_liq_ratio: float,
    whale_activity: bool,
) -> float:
    """
    Participation Quality Score (0-100).
    Broad healthy participation > one-wallet domination.
    Without direct holder data from Polymarket, we use proxies:
    - Competitive flag (Polymarket's own breadth indicator)
    - Volume/liquidity ratio extremes as crowding signal
    - Absolute liquidity as participation breadth proxy
    """
    score = 50.0  # base = neutral

    # Competitive market = broader participation
    if is_competitive:
        score += 20

    # Healthy liquidity = more participants
    if liquidity > 500_000:
        score += 15
    elif liquidity > 100_000:
        score += 10
    elif liquidity > 50_000:
        score += 5
    elif liquidity < 5_000:
        score -= 15

    # Very high vol/liq ratio = potentially one large player, not broad
    if vol_liq_ratio > 15.0:
        score -= 20  # extreme concentration risk
    elif vol_liq_ratio > 10.0:
        score -= 12
    elif vol_liq_ratio > 5.0:
        score -= 5

    # Whale activity is a mixed signal — shows interest but also concentration
    if whale_activity:
        score -= 8

    # Very low volume with decent liquidity = broad but passive (okay)
    if volume_24h < 1_000 and liquidity > 50_000:
        score -= 5  # not enough active participation

    return round(max(0.0, min(100.0, score)), 1)


def _time_quality_score(
    days_to_expiry: Optional[int],
    hours_to_expiry: Optional[float],
    is_expired: bool,
    is_resolving: bool,
    yes_pct: float,
) -> float:
    """
    Time-Quality Score (0-100).
    Penalizes bad expiry profiles. Recognizes decay and endgame noise.
    Sweet spot: enough time for the market to be actionable but not so far
    out that the signal decays.
    """
    if is_expired:
        return 5.0
    if is_resolving:
        return 15.0

    if days_to_expiry is None:
        # No expiry data — perpetual-style or missing. Moderate penalty.
        return 50.0

    score = 50.0  # baseline

    # Sweet spot: 3-30 days
    if 3 <= days_to_expiry <= 30:
        score += 30
    elif 30 < days_to_expiry <= 90:
        score += 20
    elif 1 <= days_to_expiry < 3:
        # Near-term: still actionable but endgame noise risk
        score += 10
        # Penalize near-expiry markets that are far from settled
        if 20 < yes_pct < 80:
            score -= 10  # uncertain outcome near expiry = noisy
    elif days_to_expiry == 0:
        # Expiring today
        score -= 20
        if 20 < yes_pct < 80:
            score -= 15  # very uncertain at expiry = max noise
    elif days_to_expiry > 180:
        # Very long-dated: less actionable
        score -= 10

    # Hours-level granularity for same-day
    if hours_to_expiry is not None and hours_to_expiry < 6:
        score -= 15

    return round(max(0.0, min(100.0, score)), 1)


def _trap_risk_score(
    yes_pct: float,
    spread: float,
    spread_pct: float,
    liquidity: float,
    volume_24h: float,
    vol_liq_ratio: float,
    whale_activity: bool,
    days_to_expiry: Optional[int],
    price_change_24h: float,
    volume_momentum: str,
    is_competitive: bool,
) -> float:
    """
    Trap Risk / Crowdedness Score (0-100, higher = MORE dangerous).
    Detects:
    - Pinned markets with no real edge
    - Very wide spread (ugly fill)
    - Low-liquidity fake movers
    - Concentrated holder base proxy
    - Near-expiry noise
    - Volume without follow-through
    """
    risk = 0.0

    # Pinned at extremes with no movement = stale, possibly trap
    if yes_pct <= 2.0 or yes_pct >= 98.0:
        if abs(price_change_24h) < 1.0:
            risk += 25  # pinned and stale

    # Very wide spread = ugly execution, likely trap for retail
    if spread > 0.10:
        risk += 20
    elif spread > 0.07:
        risk += 12
    elif spread > 0.05:
        risk += 6

    # Low liquidity + any price move = fake mover
    if liquidity < 10_000 and abs(price_change_24h) > 3.0:
        risk += 20
    elif liquidity < 25_000 and abs(price_change_24h) > 5.0:
        risk += 12

    # High vol/liq ratio = concentrated, potential whale manipulation
    if vol_liq_ratio > 15.0:
        risk += 15
    elif vol_liq_ratio > 8.0:
        risk += 8

    # Whale activity with low liquidity = crowded
    if whale_activity and liquidity < 50_000:
        risk += 12

    # Near-expiry noise
    if days_to_expiry is not None:
        if days_to_expiry == 0:
            risk += 15
        elif days_to_expiry <= 1:
            risk += 8

    # Volume without follow-through: surging volume but no price move
    if volume_momentum in ("surging", "accelerating"):
        if abs(price_change_24h) < 1.0 and volume_24h > 50_000:
            risk += 15

    # Not competitive = fewer participants = easier to manipulate
    if not is_competitive:
        risk += 5

    return round(max(0.0, min(100.0, risk)), 1)


# ── Composite Scoring ────────────────────────────────────────────────────────

def score_market(market: dict) -> dict:
    """
    Score a single enriched market dict. Returns the original market dict
    with added `scores`, `composite_score`, and `momentum_label` fields.

    The input should be an enriched market from PolymarketIntelligence._enrich_market().
    """
    yes_pct = market.get("yes_pct", 50.0)
    price_change_1h = market.get("price_change_1h", 0.0)
    price_change_24h = market.get("price_change_1d", 0.0)  # field is named price_change_1d in enriched
    price_change_1wk = market.get("price_change_1wk", 0.0)
    volume_momentum = market.get("volume_momentum", "stable")
    volume_24h = market.get("volume_24h", 0.0)
    volume_1wk = market.get("volume_1wk", 0.0)
    vol_liq_ratio = market.get("vol_liq_ratio", 0.0)
    spread = market.get("spread", 0.0)
    spread_pct = market.get("spread_pct", 0.0)
    liquidity = market.get("liquidity", 0.0)
    efficiency = market.get("market_efficiency_score", 50.0)
    best_bid = market.get("best_bid", 0.0)
    best_ask = market.get("best_ask", 0.0)
    is_competitive = market.get("is_competitive", False)
    whale_activity = market.get("whale_activity", False)
    days_to_expiry = market.get("days_to_expiry")
    hours_to_expiry = market.get("hours_to_expiry")
    is_expired = market.get("is_expired", False)
    is_resolving = market.get("is_resolving", False)

    conviction = _conviction_score(yes_pct)

    momentum_val, momentum_label = _momentum_score(
        price_change_1h, price_change_24h, price_change_1wk,
        volume_momentum, yes_pct,
    )

    flow = _flow_score(volume_24h, volume_1wk, vol_liq_ratio, volume_momentum)

    execution = _execution_quality_score(
        spread, spread_pct, liquidity, efficiency, best_bid, best_ask,
    )

    participation = _participation_quality_score(
        is_competitive, liquidity, volume_24h, vol_liq_ratio, whale_activity,
    )

    time_quality = _time_quality_score(
        days_to_expiry, hours_to_expiry, is_expired, is_resolving, yes_pct,
    )

    trap_risk = _trap_risk_score(
        yes_pct, spread, spread_pct, liquidity, volume_24h,
        vol_liq_ratio, whale_activity, days_to_expiry,
        price_change_24h, volume_momentum, is_competitive,
    )

    scores = {
        "conviction": conviction,
        "momentum": momentum_val,
        "flow": flow,
        "execution_quality": execution,
        "participation_quality": participation,
        "time_quality": time_quality,
        "trap_risk": trap_risk,
    }

    # Composite: weighted sum of positive dimensions minus trap risk penalty
    composite = (
        WEIGHTS["conviction"] * conviction
        + WEIGHTS["momentum"] * momentum_val
        + WEIGHTS["flow"] * flow
        + WEIGHTS["execution_quality"] * execution
        + WEIGHTS["participation_quality"] * participation
        + WEIGHTS["time_quality"] * time_quality
        - WEIGHTS["trap_risk"] * trap_risk
    )
    composite = round(max(0.0, min(100.0, composite)), 1)

    logger.debug(
        "scored market=%s composite=%.1f conviction=%.1f momentum=%.1f flow=%.1f "
        "exec=%.1f participation=%.1f time=%.1f trap=%.1f label=%s",
        market.get("condition_id", "?")[:12],
        composite, conviction, momentum_val, flow,
        execution, participation, time_quality, trap_risk,
        momentum_label,
    )

    # Merge into market dict (non-destructive — adds new keys)
    result = {**market}
    result["scores"] = scores
    result["composite_score"] = composite
    result["momentum_label"] = momentum_label
    return result


def score_markets(markets: list[dict]) -> list[dict]:
    """Score a list of enriched markets. Returns scored copies sorted by composite_score desc."""
    scored = [score_market(m) for m in markets]
    scored.sort(key=lambda m: m["composite_score"], reverse=True)
    return scored
