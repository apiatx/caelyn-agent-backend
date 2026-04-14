"""
Prophetik Signal Engine — Recommendation Buckets & Explainability.

Sorts scored markets into actionable recommendation categories and generates
deterministic, data-driven explanation strings for each recommendation.

Buckets:
    - best_bet_now                       Top composite score, good execution
    - best_yes_setup                     High YES conviction + momentum
    - best_no_setup                      High NO conviction + momentum
    - best_momentum_continuation         Strong sustained directional move
    - best_mean_reversion_candidate      Overextended move, likely pullback
    - best_whale_follow                  Whale activity + good execution
    - avoid_or_trap_markets              High trap risk
    - best_execution_quality             Tightest spreads, deepest liquidity
    - strongest_flow_without_confirmation Volume surge without price follow-through
    - strongest_conviction_with_good_execution  High conviction + tight spread

Usage:
    from services.predict.recommendations import build_recommendations
    recs = build_recommendations(scored_markets)
"""

from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger("prophetik.recommendations")

_BUCKET_SIZE = 8  # max items per bucket


def _is_stale_pinned(m: dict) -> bool:
    """
    Return True if a market is stale/pinned near 0% or 100% with no meaningful
    recent movement. These should NOT appear in positive recommendation buckets.
    """
    yes_pct = m.get("yes_pct", 50)
    if not (yes_pct < 5 or yes_pct > 95):
        return False
    pc_1h = abs(m.get("price_change_1h", 0))
    pc_24h = abs(m.get("price_change_1d", 0))
    pc_1wk = abs(m.get("price_change_1wk", 0))
    return pc_1h < 2.0 and pc_24h < 2.0 and pc_1wk < 2.0


def _direction_for_bucket(bucket_name: str, market: dict) -> str:
    """Determine direction label for a recommendation item."""
    if bucket_name == "avoid_or_trap_markets":
        return "AVOID"
    if bucket_name == "best_no_setup":
        return "NO"
    if bucket_name == "best_yes_setup":
        return "YES"
    # For other buckets, infer from price direction
    pc_24h = market.get("price_change_1d", 0)
    yes_pct = market.get("yes_pct", 50)
    if bucket_name in ("best_mean_reversion_candidate",):
        # Mean reversion = opposite of the recent move
        return "NO" if pc_24h > 0 else "YES"
    if pc_24h > 0 or yes_pct > 55:
        return "YES"
    if pc_24h < 0 or yes_pct < 45:
        return "NO"
    return "YES"


def build_recommendations(scored_markets: list[dict]) -> dict:
    """
    Given a list of scored markets (output of scoring.score_markets),
    return a dict of recommendation buckets, each containing up to
    _BUCKET_SIZE markets with explanation strings.
    """
    # Pre-filter: only active, non-expired markets for recommendations
    active = [
        m for m in scored_markets
        if not m.get("is_expired") and not m.get("is_resolving")
    ]

    # Separate stale pinned markets — these should only appear in avoid bucket
    non_stale = [m for m in active if not _is_stale_pinned(m)]

    buckets = {
        "best_bet_now": _best_bet_now(non_stale),
        "best_yes_setup": _best_yes_setup(non_stale),
        "best_no_setup": _best_no_setup(non_stale),
        "best_momentum_continuation": _best_momentum_continuation(non_stale),
        "best_mean_reversion_candidate": _best_mean_reversion(non_stale),
        "best_whale_follow": _best_whale_follow(non_stale),
        "avoid_or_trap_markets": _avoid_or_trap(active),  # stale pinned CAN appear here
        "best_execution_quality": _best_execution(non_stale),
        "strongest_flow_without_confirmation": _flow_without_confirmation(non_stale),
        "strongest_conviction_with_good_execution": _conviction_with_execution(non_stale),
    }

    # Attach explanation strings and direction to each bucket's items
    for bucket_name, items in buckets.items():
        for item in items:
            item["reasons"] = _generate_reasons(item, bucket_name)
            # Ensure reasons always has at least 1 string
            if not item["reasons"]:
                item["reasons"] = ["Selected based on composite scoring"]
            item["direction"] = _direction_for_bucket(bucket_name, item)

    logger.info(
        "recommendations built: %s",
        {k: len(v) for k, v in buckets.items()},
    )

    return buckets


# ── Bucket Builders ──────────────────────────────────────────────────────────

def _best_bet_now(markets: list[dict]) -> list[dict]:
    """Top composite score with reasonable execution and low trap risk."""
    candidates = [
        m for m in markets
        if m.get("scores", {}).get("execution_quality", 0) >= 30
        and m.get("scores", {}).get("trap_risk", 100) < 50
        and m.get("volume_24h", 0) > 5_000
    ]
    candidates.sort(key=lambda m: m.get("composite_score", 0), reverse=True)
    return [_slim_rec(m) for m in candidates[:_BUCKET_SIZE]]


def _best_yes_setup(markets: list[dict]) -> list[dict]:
    """High YES probability + upward momentum + decent flow."""
    candidates = [
        m for m in markets
        if m.get("yes_pct", 50) > 55
        and m.get("price_change_1d", 0) > 0
        and m.get("scores", {}).get("momentum", 0) >= 20
        and m.get("scores", {}).get("flow", 0) >= 15
    ]
    # Sort by conviction * momentum blend
    candidates.sort(
        key=lambda m: (
            m.get("scores", {}).get("conviction", 0) * 0.5
            + m.get("scores", {}).get("momentum", 0) * 0.5
        ),
        reverse=True,
    )
    return [_slim_rec(m) for m in candidates[:_BUCKET_SIZE]]


def _best_no_setup(markets: list[dict]) -> list[dict]:
    """High NO probability (low YES) + downward momentum."""
    candidates = [
        m for m in markets
        if m.get("yes_pct", 50) < 45
        and m.get("price_change_1d", 0) < 0
        and m.get("scores", {}).get("momentum", 0) >= 20
        and m.get("scores", {}).get("flow", 0) >= 15
    ]
    candidates.sort(
        key=lambda m: (
            m.get("scores", {}).get("conviction", 0) * 0.5
            + m.get("scores", {}).get("momentum", 0) * 0.5
        ),
        reverse=True,
    )
    return [_slim_rec(m) for m in candidates[:_BUCKET_SIZE]]


def _best_momentum_continuation(markets: list[dict]) -> list[dict]:
    """Strong sustained directional move with volume support."""
    candidates = [
        m for m in markets
        if m.get("scores", {}).get("momentum", 0) >= 40
        and m.get("momentum_label", "").startswith("strong")
        and m.get("scores", {}).get("flow", 0) >= 25
    ]
    candidates.sort(key=lambda m: m.get("scores", {}).get("momentum", 0), reverse=True)
    return [_slim_rec(m) for m in candidates[:_BUCKET_SIZE]]


def _best_mean_reversion(markets: list[dict]) -> list[dict]:
    """
    Overextended move likely to pull back:
    - Big 24h move but 1h already fading
    - OR big 24h move into wide spread (execution cost limits follow-through)
    """
    candidates = []
    for m in markets:
        pc_24h = m.get("price_change_1d", 0)
        pc_1h = m.get("price_change_1h", 0)
        spread = m.get("spread", 0)
        if abs(pc_24h) < 5:
            continue
        # Direction reversal in last hour
        fading = (pc_24h > 0 and pc_1h < -0.5) or (pc_24h < 0 and pc_1h > 0.5)
        # Or big move into ugly spread
        wide_spread = spread > 0.06 and abs(pc_24h) > 8
        if fading or wide_spread:
            candidates.append(m)

    candidates.sort(key=lambda m: abs(m.get("price_change_1d", 0)), reverse=True)
    return [_slim_rec(m) for m in candidates[:_BUCKET_SIZE]]


def _best_whale_follow(markets: list[dict]) -> list[dict]:
    """Whale activity + decent execution quality (not a trap)."""
    candidates = [
        m for m in markets
        if m.get("whale_activity")
        and m.get("scores", {}).get("execution_quality", 0) >= 30
        and m.get("scores", {}).get("trap_risk", 100) < 60
    ]
    candidates.sort(key=lambda m: m.get("vol_liq_ratio", 0), reverse=True)
    return [_slim_rec(m) for m in candidates[:_BUCKET_SIZE]]


def _avoid_or_trap(markets: list[dict]) -> list[dict]:
    """High trap risk — steer clear. Also captures stale pinned markets."""
    candidates = [
        m for m in markets
        if m.get("scores", {}).get("trap_risk", 0) >= 40
        or _is_stale_pinned(m)
    ]
    candidates.sort(key=lambda m: m.get("scores", {}).get("trap_risk", 0), reverse=True)
    return [_slim_rec(m) for m in candidates[:_BUCKET_SIZE]]


def _best_execution(markets: list[dict]) -> list[dict]:
    """Tightest spreads, deepest liquidity, best fill quality."""
    candidates = [
        m for m in markets
        if m.get("scores", {}).get("execution_quality", 0) >= 50
        and m.get("volume_24h", 0) > 5_000
    ]
    candidates.sort(key=lambda m: m.get("scores", {}).get("execution_quality", 0), reverse=True)
    return [_slim_rec(m) for m in candidates[:_BUCKET_SIZE]]


def _flow_without_confirmation(markets: list[dict]) -> list[dict]:
    """High volume / flow but price hasn't moved much — unconfirmed signal."""
    candidates = [
        m for m in markets
        if m.get("scores", {}).get("flow", 0) >= 40
        and abs(m.get("price_change_1d", 0)) < 3.0
    ]
    candidates.sort(key=lambda m: m.get("scores", {}).get("flow", 0), reverse=True)
    return [_slim_rec(m) for m in candidates[:_BUCKET_SIZE]]


def _conviction_with_execution(markets: list[dict]) -> list[dict]:
    """Strong conviction + tight spread + good fill."""
    candidates = [
        m for m in markets
        if m.get("scores", {}).get("conviction", 0) >= 50
        and m.get("scores", {}).get("execution_quality", 0) >= 45
        and m.get("scores", {}).get("trap_risk", 100) < 50
    ]
    candidates.sort(
        key=lambda m: (
            m.get("scores", {}).get("conviction", 0) * 0.6
            + m.get("scores", {}).get("execution_quality", 0) * 0.4
        ),
        reverse=True,
    )
    return [_slim_rec(m) for m in candidates[:_BUCKET_SIZE]]


# ── Explainability ───────────────────────────────────────────────────────────

def _generate_reasons(market: dict, bucket: str) -> list[str]:
    """
    Generate concise, deterministic, data-driven explanation strings.
    No generic AI fluff — every reason must reference actual data.
    """
    reasons = []
    scores = market.get("scores", {})
    yes_pct = market.get("yes_pct", 50)
    pc_1h = market.get("price_change_1h", 0)
    pc_24h = market.get("price_change_1d", 0)
    pc_1wk = market.get("price_change_1wk", 0)
    volume_24h = market.get("volume_24h", 0)
    liquidity = market.get("liquidity", 0)
    spread_pct = market.get("spread_pct", 0)
    vol_liq = market.get("vol_liq_ratio", 0)
    days_exp = market.get("days_to_expiry")
    momentum_label = market.get("momentum_label", "flat")
    whale = market.get("whale_activity", False)

    # Price movement reasons
    if abs(pc_24h) > 1:
        direction = "up" if pc_24h > 0 else "down"
        reasons.append(f"24h odds {direction} {abs(pc_24h):.1f}%")
        if abs(pc_1h) > 0.5 and (pc_1h > 0) == (pc_24h > 0):
            reasons.append("move continues in last hour")
        elif abs(pc_1h) > 0.5 and (pc_1h > 0) != (pc_24h > 0):
            reasons.append(f"1h reversal of {abs(pc_1h):.1f}% — fading")

    if abs(pc_1wk) > 3 and (pc_1wk > 0) == (pc_24h > 0):
        reasons.append(f"7d trend persistent at {pc_1wk:+.1f}%")

    # Volume / flow reasons
    if volume_24h > 500_000:
        reasons.append(f"strong 24h volume (${volume_24h:,.0f})")
    elif volume_24h > 100_000:
        reasons.append(f"supportive 24h volume (${volume_24h:,.0f})")

    if market.get("volume_momentum") == "surging":
        reasons.append("volume surging vs 7d average")
    elif market.get("volume_momentum") == "accelerating":
        reasons.append("volume accelerating vs 7d average")

    # Execution reasons
    if spread_pct < 1.5:
        reasons.append("spread tight enough for clean entry")
    elif spread_pct > 5:
        reasons.append(f"wide spread ({spread_pct:.1f}%) — execution cost significant")

    if liquidity > 500_000:
        reasons.append(f"deep liquidity pool (${liquidity:,.0f})")

    # Whale reasons
    if whale:
        if vol_liq > 8:
            reasons.append("whale participation elevated and aggressive")
        else:
            reasons.append("whale participation elevated but not dangerously concentrated")

    # Conviction reasons
    if yes_pct > 85:
        reasons.append(f"market strongly favors YES at {yes_pct:.0f}%")
    elif yes_pct < 15:
        reasons.append(f"market strongly favors NO (YES only {yes_pct:.0f}%)")
    elif yes_pct > 70:
        reasons.append(f"moderate YES conviction at {yes_pct:.0f}%")
    elif yes_pct < 30:
        reasons.append(f"moderate NO conviction (YES at {yes_pct:.0f}%)")

    # Expiry reasons
    if days_exp is not None:
        if days_exp <= 1:
            reasons.append("expires within 24h — high endgame noise")
        elif days_exp <= 3:
            reasons.append(f"expires in {days_exp} days — time pressure")
        elif 7 <= days_exp <= 30:
            reasons.append(f"{days_exp} days to expiry — sweet spot for signal")

    # Trap / risk reasons
    trap = scores.get("trap_risk", 0)
    if trap >= 60:
        reasons.append("this move looks late and crowded; trap risk elevated")
    elif trap >= 40:
        reasons.append("moderate crowding risk — size accordingly")

    # Bucket-specific reasons
    if bucket == "best_mean_reversion_candidate":
        if pc_1h != 0 and pc_24h != 0 and (pc_1h > 0) != (pc_24h > 0):
            reasons.append("1h reversal suggests overextension fading")
    elif bucket == "strongest_flow_without_confirmation":
        reasons.append("high flow without price confirmation — watch for breakout or exhaustion")
    elif bucket == "best_momentum_continuation":
        reasons.append(f"momentum label: {momentum_label}")

    # Cap at 5 reasons for readability
    return reasons[:5]


# ── Helpers ──────────────────────────────────────────────────────────────────

def _slim_rec(market: dict) -> dict:
    """Return a recommendation-friendly subset of a scored market.
    Includes all fields the frontend needs for recommendation cards.
    """
    keep = [
        "condition_id", "question", "yes_pct", "no_pct",
        "yes_price", "no_price",
        "volume_24h", "liquidity", "spread", "spread_pct",
        "price_change_1h", "price_change_1d", "price_change_1wk",
        "volume_momentum", "whale_activity", "is_competitive",
        "days_to_expiry", "hours_to_expiry", "end_date", "tags", "image",
        "vol_liq_ratio", "momentum_label",
        "scores", "composite_score",
    ]
    result = {k: market[k] for k in keep if k in market}

    # Add price_change_24h alias for frontend compatibility
    result["price_change_24h"] = market.get("price_change_1d", 0)

    # Add end_date_iso alias
    result["end_date_iso"] = market.get("end_date")

    # Flatten score dimensions to top level for easy frontend access
    scores = market.get("scores", {})
    result["conviction_score"] = scores.get("conviction", 0)
    result["momentum_score"] = scores.get("momentum", 0)
    result["flow_score"] = scores.get("flow", 0)
    result["execution_quality_score"] = scores.get("execution_quality", 0)
    result["participation_quality_score"] = scores.get("participation_quality", 0)
    result["time_quality_score"] = scores.get("time_quality", 0)
    result["trap_risk_score"] = scores.get("trap_risk", 0)

    # Use slug from scored market if available, otherwise generate from question
    if market.get("slug"):
        result["slug"] = market["slug"]
    else:
        question = market.get("question", "")
        slug = question.lower().strip()
        for ch in ["?", "'", '"', ",", ".", "!", "(", ")", "[", "]", "{", "}", "&", "%", "$", "#", "@"]:
            slug = slug.replace(ch, "")
        slug = slug.replace(" ", "-").replace("--", "-").strip("-")
        result["slug"] = slug[:100]

    return result
