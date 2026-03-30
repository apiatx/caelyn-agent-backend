"""
Hyperliquid Screener — deterministic ranking engine.

The base ranking is fully feature-driven (no LLM calls per row).
Mode-specific weights adjust how component scores are combined.
An optional LLM summary layer can be layered on top of the top-N names.
"""
from __future__ import annotations

import time
from typing import Any, Optional

from .feature_engine import _clip, _compute_composite
from .models import AgentRankRequest, AgentRankResponse, ScreenerAsset
from .state import HyperliquidState

# ── Ranking mode weights ──────────────────────────────────────────────────────
# Each entry: {component_score_field: weight}
_MODE_WEIGHTS: dict[str, dict[str, float]] = {
    "balanced": {
        "momentum_score": 0.25,
        "flow_score": 0.25,
        "breakout_score": 0.20,
        "mean_reversion_score": 0.15,
        "liquidity_score": 0.10,
        "volatility_score": 0.05,
    },
    "momentum": {
        "momentum_score": 0.40,
        "flow_score": 0.30,
        "breakout_score": 0.20,
        "mean_reversion_score": 0.05,
        "liquidity_score": 0.05,
        "volatility_score": 0.00,
    },
    "breakout": {
        "breakout_score": 0.35,
        "momentum_score": 0.25,
        "flow_score": 0.20,
        "volatility_score": 0.15,
        "liquidity_score": 0.05,
        "mean_reversion_score": 0.00,
    },
    "mean_reversion": {
        "mean_reversion_score": 0.40,
        "flow_score": 0.20,
        "momentum_score": 0.10,
        "breakout_score": 0.05,
        "liquidity_score": 0.15,
        "volatility_score": 0.10,
    },
    "crowding_dislocation": {
        "mean_reversion_score": 0.35,
        "flow_score": 0.25,
        "momentum_score": 0.15,
        "breakout_score": 0.10,
        "liquidity_score": 0.10,
        "volatility_score": 0.05,
    },
}

_VALID_MODES = set(_MODE_WEIGHTS.keys())


# ─────────────────────────────────────────────────────────────────────────────
# Core ranking function
# ─────────────────────────────────────────────────────────────────────────────

def rank_assets(
    assets: list[ScreenerAsset],
    mode: str = "balanced",
    filters: Optional[dict[str, Any]] = None,
    prev_ranks: Optional[dict[str, int]] = None,
) -> list[ScreenerAsset]:
    """
    Rank assets from highest to lowest mode-specific signal score.

    Applies optional filters, recomputes mode-specific composite score,
    attaches rank/prev_rank/rank_change, and returns the sorted list.
    """
    if mode not in _VALID_MODES:
        mode = "balanced"

    weights = _MODE_WEIGHTS[mode]
    candidates = _apply_filters(assets, filters or {})

    scored: list[tuple[float, ScreenerAsset]] = []
    for asset in candidates:
        # Recompute composite under the chosen mode
        score = _composite_for_mode(asset, weights)
        scored.append((score, asset))

    # Sort descending by score
    scored.sort(key=lambda x: -x[0])

    prev_ranks = prev_ranks or {}
    ranked: list[ScreenerAsset] = []
    for rank_idx, (score, asset) in enumerate(scored, start=1):
        prev = prev_ranks.get(asset.coin)
        rank_change = (prev - rank_idx) if prev is not None else None
        ranked.append(asset.model_copy(update={
            "composite_signal_score": round(score, 1),
            "rank": rank_idx,
            "prev_rank": prev,
            "rank_change": rank_change,
            "score_components": {
                "momentum_score": asset.momentum_score,
                "flow_score": asset.flow_score,
                "breakout_score": asset.breakout_score,
                "mean_reversion_score": asset.mean_reversion_score,
                "liquidity_score": asset.liquidity_score,
                "volatility_score": asset.volatility_score,
            },
        }))

    return ranked


def _composite_for_mode(asset: ScreenerAsset, weights: dict[str, float]) -> float:
    total = 0.0
    for field, w in weights.items():
        val = getattr(asset, field, None) or 50.0
        total += val * w
    return _clip(total, 0, 100)


# ─────────────────────────────────────────────────────────────────────────────
# Filter application
# ─────────────────────────────────────────────────────────────────────────────

def _apply_filters(assets: list[ScreenerAsset], filters: dict) -> list[ScreenerAsset]:
    """
    Apply user-requested filters.

    Supported filter keys:
      market_type: "perp" | "spot" | "all"
      min_volume_usd: float
      max_spread_bps: float
      min_oi_usd: float
      tags: list[str]  — asset must have ALL specified tags
      exclude_flags: list[str] — e.g. ["avoid_due_to_spread", "illiquid_high_volatility"]
      min_funding_abs_pct_annual: float
      market_status: "active" | "all"
    """
    result = assets

    mtype = filters.get("market_type", "perp")
    if mtype and mtype != "all":
        result = [a for a in result if a.market_type == mtype]

    min_vol = filters.get("min_volume_usd")
    if min_vol:
        result = [a for a in result if (a.day_ntl_vlm or 0) >= min_vol]

    max_spread = filters.get("max_spread_bps")
    if max_spread is not None:
        result = [a for a in result if (a.spread_bps or 0) <= max_spread]

    min_oi = filters.get("min_oi_usd")
    if min_oi:
        result = [a for a in result if (a.open_interest_usd or 0) >= min_oi]

    tags_req = filters.get("tags")
    if tags_req:
        req_set = set(tags_req)
        result = [a for a in result if req_set.issubset(set(a.tags))]

    exclude_flags = filters.get("exclude_flags") or []
    for flag in exclude_flags:
        result = [a for a in result if not getattr(a, flag, False)]

    min_fund = filters.get("min_funding_abs_pct_annual")
    if min_fund is not None:
        result = [a for a in result if abs((a.funding or 0) * 8760) >= min_fund / 100]

    status = filters.get("market_status", "active")
    if status == "active":
        result = [a for a in result if a.market_status == "active"]

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Bucket builders
# ─────────────────────────────────────────────────────────────────────────────

def build_buckets(ranked: list[ScreenerAsset], top_n: int = 5) -> dict:
    """
    Extract summary buckets from a ranked list:
      top_long, top_short, top_dislocations, avoid_list
    """
    top_long = [a for a in ranked if a.signal_direction == "long"][:top_n]
    top_short = [a for a in ranked if a.signal_direction == "short"][:top_n]
    top_dislocations = sorted(
        [a for a in ranked if a.dislocated_vs_oracle],
        key=lambda a: -abs(a.distance_mark_oracle_pct or 0),
    )[:top_n]
    avoid_list = [
        a for a in ranked
        if a.avoid_due_to_spread or a.illiquid_high_volatility
    ][:top_n]
    return {
        "top_long": top_long,
        "top_short": top_short,
        "top_dislocations": top_dislocations,
        "avoid_list": avoid_list,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Rationale generator (deterministic, no LLM)
# ─────────────────────────────────────────────────────────────────────────────

def generate_rationale(asset: ScreenerAsset, mode: str) -> str:
    """
    Build a concise, human-readable rationale string for a ranked asset.
    Purely deterministic — describes why this asset ranks highly in the chosen mode.
    """
    parts: list[str] = []
    ann_fund = (asset.funding or 0) * 8760

    if mode in ("momentum", "balanced", "breakout"):
        if (asset.momentum_1h or 0) > 0.3:
            parts.append(f"strong 1h momentum (+{asset.momentum_1h:.2f}%)")
        elif (asset.momentum_1h or 0) < -0.3:
            parts.append(f"sharp 1h pullback ({asset.momentum_1h:.2f}%)")
        if (asset.momentum_4h or 0) > 0.5:
            parts.append(f"sustained 4h trend (+{asset.momentum_4h:.2f}%)")
        if (asset.recent_trade_imbalance or 0) > 0.3:
            parts.append(f"buy flow dominant ({asset.recent_trade_imbalance:.0%})")
        elif (asset.recent_trade_imbalance or 0) < -0.3:
            parts.append(f"sell flow dominant ({asset.recent_trade_imbalance:.0%})")

    if mode in ("mean_reversion", "crowding_dislocation", "balanced"):
        if abs(ann_fund) > 0.30:
            direction = "long" if ann_fund > 0 else "short"
            parts.append(f"extreme {direction} funding ({ann_fund:+.0%} ann.)")
        if asset.dislocated_vs_oracle:
            parts.append(f"oracle dislocation {asset.distance_mark_oracle_pct:+.2f}%")
        if asset.squeeze_candidate:
            parts.append("short squeeze setup")
        if asset.crowded_long:
            parts.append("crowded long — fade candidate")

    if mode == "breakout":
        if (asset.realized_volatility_short or 0) > 100:
            parts.append(f"high short-vol ({asset.realized_volatility_short:.0f}% ann.)")
        if (asset.orderbook_imbalance or 0) > 0.3:
            parts.append("bid-heavy orderbook")

    if not parts:
        score = asset.composite_signal_score or 0
        direction = asset.signal_direction or "neutral"
        parts.append(f"composite score {score:.0f}/100 ({direction})")

    if asset.liquidity_score is not None and asset.liquidity_score < 40:
        parts.append("⚠ low liquidity")

    return "; ".join(parts[:4])


# ─────────────────────────────────────────────────────────────────────────────
# Main agent-rank entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_agent_rank(state: HyperliquidState, req: AgentRankRequest) -> AgentRankResponse:
    """
    Execute a full ranking pass and return the AgentRankResponse payload.
    Deterministic — no LLM calls. Fast enough to run on every button click.
    """
    mode   = req.ranking_mode if req.ranking_mode in _VALID_MODES else "balanced"
    top_n  = max(1, min(req.top_n, 200))

    all_assets = state.all_assets()
    ranked = rank_assets(all_assets, mode=mode, filters=req.filters, prev_ranks=state.prev_ranks)

    # Attach rationales if requested
    if req.include_rationales:
        ranked = [
            a.model_copy(update={"agent_rationale": generate_rationale(a, mode)})
            for a in ranked
        ]

    # Strip score_components if not requested (saves bandwidth)
    if not req.include_score_components:
        ranked = [a.model_copy(update={"score_components": {}}) for a in ranked]

    # Update prev_ranks for next call
    state.prev_ranks = {a.coin: a.rank for a in ranked if a.rank is not None}

    buckets = build_buckets(ranked, top_n=min(5, top_n))

    # Summary stats
    scores = [a.composite_signal_score for a in ranked if a.composite_signal_score is not None]
    summary = {
        "total_assets": len(ranked),
        "mean_score": round(sum(scores) / len(scores), 1) if scores else None,
        "crowded_long_count": sum(1 for a in ranked if a.crowded_long),
        "crowded_short_count": sum(1 for a in ranked if a.crowded_short),
        "squeeze_candidate_count": sum(1 for a in ranked if a.squeeze_candidate),
        "dislocation_count": sum(1 for a in ranked if a.dislocated_vs_oracle),
        "avoid_count": sum(1 for a in ranked if a.avoid_due_to_spread or a.illiquid_high_volatility),
        "ranking_mode": mode,
        "data_freshness_seconds": state.freshness_seconds(),
    }

    return AgentRankResponse(
        request_ts=time.time(),
        ranking_mode=mode,
        total_ranked=len(ranked),
        ranked_rows=ranked[:top_n],
        top_long=buckets["top_long"],
        top_short=buckets["top_short"],
        top_dislocations=buckets["top_dislocations"],
        avoid_list=buckets["avoid_list"],
        summary=summary,
    )
