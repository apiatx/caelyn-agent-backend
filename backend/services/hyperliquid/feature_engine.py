"""
Hyperliquid Screener — feature / signal engineering.

Computes all derived market signals from the normalized asset data:
  - Realized volatility (short/medium term)
  - Multi-timeframe momentum
  - Spread quality
  - Book depth / imbalance
  - Trade flow imbalance
  - Funding/premium extremity
  - Composite scores (liquidity, volatility, momentum, flow, mean_reversion, breakout)
  - Qualitative flags (crowded_long, squeeze_candidate, etc.)
  - Universe percentile ranks
"""
from __future__ import annotations

import math
import time
from typing import Optional

from .models import ScreenerAsset
from .state import HyperliquidState


# ─────────────────────────────────────────────────────────────────────────────
# Math helpers
# ─────────────────────────────────────────────────────────────────────────────

def _safe_pct(new: float, old: float) -> Optional[float]:
    if old and old != 0:
        return (new - old) / old * 100
    return None

def _returns(closes: list[float]) -> list[float]:
    """Log returns from a close price series."""
    rets = []
    for i in range(1, len(closes)):
        if closes[i - 1] > 0:
            rets.append(math.log(closes[i] / closes[i - 1]))
    return rets

def _annualized_vol(rets: list[float], periods_per_year: int = 8760) -> Optional[float]:
    """Annualized volatility (%) from log returns."""
    if len(rets) < 3:
        return None
    n = len(rets)
    mean = sum(rets) / n
    variance = sum((r - mean) ** 2 for r in rets) / (n - 1)
    return math.sqrt(variance * periods_per_year) * 100

def _clip(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))

def _sigmoid(x: float, k: float = 1.0) -> float:
    """Maps any real to (0, 1)."""
    return 1 / (1 + math.exp(-k * x))

def _percentile_rank(value: float, population: list[float]) -> float:
    """Percentile rank of value in population (0..1)."""
    if not population:
        return 0.5
    below = sum(1 for v in population if v < value)
    return below / len(population)


# ─────────────────────────────────────────────────────────────────────────────
# Per-asset feature computation
# ─────────────────────────────────────────────────────────────────────────────

def compute_candle_features(
    asset: ScreenerAsset,
    candles_1h: list[dict],
    candles_5m: list[dict],
) -> dict:
    """
    Compute volatility and momentum features from candle data.
    Returns a partial dict suitable for model_copy(update=...).
    """
    updates: dict = {}

    # ── Realized volatility ────────────────────────────────────────────────
    # Short: from 5m candles over ~1h (12 bars of 5m)
    if len(candles_5m) >= 4:
        closes_5m = [float(c["c"]) for c in candles_5m[-24:] if c.get("c")]
        rets_5m = _returns(closes_5m)
        # 5m bars → periods_per_year = 365 * 24 * 12 = 105120
        rv_short = _annualized_vol(rets_5m, periods_per_year=105120)
        if rv_short is not None:
            updates["realized_volatility_short"] = round(rv_short, 2)

    # Medium: from 1h candles over ~24h (24 bars)
    if len(candles_1h) >= 4:
        closes_1h = [float(c["c"]) for c in candles_1h[-48:] if c.get("c")]
        rets_1h = _returns(closes_1h)
        rv_medium = _annualized_vol(rets_1h, periods_per_year=8760)
        if rv_medium is not None:
            updates["realized_volatility_medium"] = round(rv_medium, 2)

    # ── Momentum ───────────────────────────────────────────────────────────
    # 5m momentum: last close vs 1 bar ago (5m)
    if len(candles_5m) >= 2:
        c_now  = float(candles_5m[-1].get("c", 0) or 0)
        c_prev = float(candles_5m[-2].get("c", 0) or 0)
        if c_prev > 0:
            updates["momentum_5m"] = round((c_now - c_prev) / c_prev * 100, 4)

    # 1h momentum: last 1h close vs 1 bar ago
    if len(candles_1h) >= 2:
        c_now  = float(candles_1h[-1].get("c", 0) or 0)
        c_prev = float(candles_1h[-2].get("c", 0) or 0)
        if c_prev > 0:
            updates["momentum_1h"] = round((c_now - c_prev) / c_prev * 100, 4)

    # 4h momentum: last close vs close 4 bars ago in 1h candles
    if len(candles_1h) >= 5:
        c_now  = float(candles_1h[-1].get("c", 0) or 0)
        c_4ago = float(candles_1h[-5].get("c", 0) or 0)
        if c_4ago > 0:
            updates["momentum_4h"] = round((c_now - c_4ago) / c_4ago * 100, 4)

    return updates


def compute_scores(asset: ScreenerAsset) -> dict:
    """
    Compute all composite scores (0–100) and qualitative flags.
    Returns a partial dict for model_copy(update=...).
    """
    updates: dict = {}

    # ── Liquidity score ───────────────────────────────────────────────────
    # High volume + tight spread + deep book → high liquidity
    score_parts = []

    vlm = asset.day_ntl_vlm or 0
    # Volume component: log scale, $10B+ daily vol → 100
    if vlm > 0:
        vol_s = _clip(math.log10(vlm + 1) / math.log10(1e10) * 100, 0, 100)
        score_parts.append(("vol", vol_s, 1.0))

    # Spread component: <1bps = 100, >50bps = 0
    spread_bps = asset.spread_bps
    if spread_bps is not None:
        spread_s = _clip(100 - spread_bps * 2, 0, 100)
        score_parts.append(("spread", spread_s, 1.5))

    # Book depth component
    bid_d = asset.orderbook_bid_depth or 0
    ask_d = asset.orderbook_ask_depth or 0
    if bid_d + ask_d > 0:
        depth_s = _clip(math.log10(bid_d + ask_d + 1) / 6 * 100, 0, 100)
        score_parts.append(("depth", depth_s, 1.0))

    liq_score = _weighted_avg(score_parts, default=50.0)
    updates["liquidity_score"] = round(liq_score, 1)

    # ── Volatility score ──────────────────────────────────────────────────
    # Higher volatility = higher opportunity (but not always risk-adjusted)
    rv_s = asset.realized_volatility_short
    rv_m = asset.realized_volatility_medium
    if rv_s is not None or rv_m is not None:
        rv = rv_s if rv_s is not None else rv_m
        # 0% annualized → 0, 300%+ annualized → 100
        vol_score = _clip(rv / 3.0, 0, 100)
        updates["volatility_score"] = round(vol_score, 1)
    else:
        updates["volatility_score"] = 50.0

    # ── Momentum score ────────────────────────────────────────────────────
    # Positive = bullish momentum, negative = bearish; score 0–100 centered at 50
    mom_parts = []
    if asset.momentum_5m  is not None: mom_parts.append(asset.momentum_5m  * 4.0)   # weight recent
    if asset.momentum_1h  is not None: mom_parts.append(asset.momentum_1h  * 3.0)
    if asset.momentum_4h  is not None: mom_parts.append(asset.momentum_4h  * 2.0)
    if asset.momentum_24h is not None: mom_parts.append(asset.momentum_24h * 1.0)

    if mom_parts:
        total_weight = 4 + (3 if asset.momentum_1h else 0) + (2 if asset.momentum_4h else 0) + (1 if asset.momentum_24h else 0)
        avg_mom = sum(mom_parts) / total_weight
        # ±10% average momentum maps to 0–100
        mom_score = _clip(50 + avg_mom * 5, 0, 100)
        updates["momentum_score"] = round(mom_score, 1)
    else:
        updates["momentum_score"] = 50.0

    # ── Flow score ────────────────────────────────────────────────────────
    # Trade flow imbalance: +1 = all buys (bullish), -1 = all sells
    flow_imb = asset.recent_trade_imbalance
    book_imb = asset.orderbook_imbalance
    if flow_imb is not None or book_imb is not None:
        combined_imb = 0.0
        w = 0
        if flow_imb is not None:
            combined_imb += flow_imb * 2; w += 2
        if book_imb is not None:
            combined_imb += book_imb * 1; w += 1
        avg_imb = combined_imb / w if w > 0 else 0
        flow_score = _clip(50 + avg_imb * 50, 0, 100)
        updates["flow_score"] = round(flow_score, 1)
    else:
        updates["flow_score"] = 50.0

    # ── Mean reversion score ──────────────────────────────────────────────
    # High when: extreme funding, high premium, price far from oracle
    mr_parts = []

    fund = asset.funding
    if fund is not None:
        # Annualize: funding * 8760; extreme = ±50% annual
        ann_fund = fund * 8760
        fund_extremity = min(abs(ann_fund) / 0.5, 1.0)  # 0..1
        mr_parts.append(fund_extremity * 100)

    prem = asset.premium
    if prem is not None:
        # Premium > 0.5% is notable
        prem_extremity = min(abs(prem) / 0.005, 1.0)
        mr_parts.append(prem_extremity * 100)

    dist_mo = asset.distance_mark_oracle_pct
    if dist_mo is not None:
        # Mark-oracle > 1% is notable
        disloc = min(abs(dist_mo) / 1.0, 1.0)
        mr_parts.append(disloc * 100)

    mr_score = sum(mr_parts) / len(mr_parts) if mr_parts else 50.0
    updates["mean_reversion_score"] = round(_clip(mr_score, 0, 100), 1)

    # ── Breakout score ────────────────────────────────────────────────────
    # High when: accelerating momentum, expanding volatility, strong buy flow
    bo_parts = []
    mom_s = updates.get("momentum_score", 50.0)
    # Momentum skewed toward either end of scale signals breakout potential
    mo_breakout = abs(mom_s - 50) * 2  # 0..100, high = strong directional momentum
    bo_parts.append(mo_breakout)

    vol_s = updates.get("volatility_score", 50.0)
    bo_parts.append(vol_s)

    flow_s = updates.get("flow_score", 50.0)
    flow_breakout = abs(flow_s - 50) * 2
    bo_parts.append(flow_breakout)

    breakout_score = sum(bo_parts) / len(bo_parts)
    updates["breakout_score"] = round(_clip(breakout_score, 0, 100), 1)

    # ── Qualitative flags ─────────────────────────────────────────────────
    ann_fund = (asset.funding or 0) * 8760
    oi = asset.open_interest_usd or 0
    vlm = asset.day_ntl_vlm or 0

    # Crowded long: high positive funding AND significant OI
    crowded_long = ann_fund > 0.30 and oi > 1_000_000
    # Crowded short: highly negative funding
    crowded_short = ann_fund < -0.30 and oi > 1_000_000
    # Squeeze: crowded short + price already moving up
    squeeze_candidate = crowded_short and (asset.momentum_1h or 0) > 0.3
    # Dislocated: mark > oracle by meaningful amount
    dislocated = abs(asset.distance_mark_oracle_pct or 0) > 0.5
    # Trend continuation: aligned momentum across timeframes
    m1 = asset.momentum_1h or 0
    m4 = asset.momentum_4h or 0
    m24 = asset.momentum_24h or 0
    trend_cont = (m1 > 0.1 and m4 > 0.2 and m24 > 0.5) or (m1 < -0.1 and m4 < -0.2 and m24 < -0.5)
    # Mean reversion candidate: extremes in funding + dislocation
    mr_cand = (updates["mean_reversion_score"] or 0) > 65
    # Illiquid high vol: low volume + high volatility
    illiquid_hv = vlm < 5_000_000 and (asset.realized_volatility_short or 0) > 150
    # Avoid due to spread: spread > 20bps
    avoid_spread = (asset.spread_bps or 0) > 20

    updates.update({
        "crowded_long": crowded_long,
        "crowded_short": crowded_short,
        "squeeze_candidate": squeeze_candidate,
        "dislocated_vs_oracle": dislocated,
        "trend_continuation_candidate": trend_cont,
        "mean_reversion_candidate": mr_cand,
        "illiquid_high_volatility": illiquid_hv,
        "avoid_due_to_spread": avoid_spread,
    })

    return updates


# ─────────────────────────────────────────────────────────────────────────────
# Universe-wide computations (percentile ranks + composite score)
# ─────────────────────────────────────────────────────────────────────────────

def compute_universe_ranks(assets: list[ScreenerAsset]) -> list[ScreenerAsset]:
    """
    Compute percentile ranks across the universe for volume, OI, funding, volatility.
    Also finalize composite_signal_score and signal_direction.
    """
    perps = [a for a in assets if a.market_type == "perp"]
    spots = [a for a in assets if a.market_type == "spot"]

    result_map: dict[str, ScreenerAsset] = {}

    # Gather populations
    vlm_pop   = [a.day_ntl_vlm   for a in perps if a.day_ntl_vlm   is not None]
    oi_pop    = [a.open_interest_usd for a in perps if a.open_interest_usd is not None]
    fund_pop  = [abs(a.funding or 0) for a in perps]
    vol_pop   = [a.realized_volatility_medium for a in perps if a.realized_volatility_medium is not None]

    for asset in perps:
        updates: dict = {}

        # Percentile ranks
        if asset.day_ntl_vlm is not None and vlm_pop:
            updates["volume_percentile"] = round(_percentile_rank(asset.day_ntl_vlm, vlm_pop), 3)
        if asset.open_interest_usd is not None and oi_pop:
            updates["oi_percentile"] = round(_percentile_rank(asset.open_interest_usd, oi_pop), 3)
        if asset.funding is not None and fund_pop:
            updates["funding_percentile"] = round(_percentile_rank(abs(asset.funding), fund_pop), 3)
        if asset.realized_volatility_medium is not None and vol_pop:
            updates["volatility_percentile"] = round(_percentile_rank(asset.realized_volatility_medium, vol_pop), 3)

        # Composite signal score (balanced mode default weights)
        composite = _compute_composite(asset, updates, mode="balanced")
        updates["composite_signal_score"] = round(composite, 1)

        # Signal direction from momentum score and flow score
        mom_s  = asset.momentum_score  or 50
        flow_s = asset.flow_score      or 50
        avg_directional = (mom_s + flow_s) / 2
        if avg_directional >= 60:
            updates["signal_direction"] = "long"
            updates["signal_confidence"] = round((avg_directional - 50) / 50, 2)
        elif avg_directional <= 40:
            updates["signal_direction"] = "short"
            updates["signal_confidence"] = round((50 - avg_directional) / 50, 2)
        else:
            updates["signal_direction"] = "neutral"
            updates["signal_confidence"] = round(abs(avg_directional - 50) / 50, 2)

        result_map[asset.coin] = asset.model_copy(update=updates)

    # Spot: minimal scoring
    for asset in spots:
        pct = asset.pct_change_24h or 0
        composite = _clip(50 + pct * 2, 0, 100)
        result_map[asset.coin] = asset.model_copy(update={
            "composite_signal_score": round(composite, 1),
            "signal_direction": "long" if pct > 1 else ("short" if pct < -1 else "neutral"),
        })

    return list(result_map.values())


def _compute_composite(asset: ScreenerAsset, extra: dict, mode: str = "balanced") -> float:
    """Combine component scores into one composite signal score (0–100)."""
    WEIGHTS = {
        "balanced":           {"momentum": 0.25, "flow": 0.25, "breakout": 0.20, "mean_reversion": 0.15, "liquidity": 0.10, "volatility": 0.05},
        "momentum":           {"momentum": 0.40, "flow": 0.30, "breakout": 0.20, "mean_reversion": 0.05, "liquidity": 0.05, "volatility": 0.00},
        "breakout":           {"breakout": 0.35, "momentum": 0.25, "flow": 0.20, "volatility": 0.15, "liquidity": 0.05, "mean_reversion": 0.00},
        "mean_reversion":     {"mean_reversion": 0.40, "flow": 0.20, "momentum": 0.10, "breakout": 0.05, "liquidity": 0.15, "volatility": 0.10},
        "crowding_dislocation": {"mean_reversion": 0.35, "flow": 0.25, "momentum": 0.15, "breakout": 0.10, "liquidity": 0.10, "volatility": 0.05},
    }
    w = WEIGHTS.get(mode, WEIGHTS["balanced"])

    def _score(name: str) -> float:
        return extra.get(name) or getattr(asset, f"{name}_score") or 50.0

    total = sum(w[k] * _score(k) for k in w)
    return _clip(total, 0, 100)


def _weighted_avg(parts: list[tuple[str, float, float]], default: float = 50.0) -> float:
    """Compute a weighted average from (name, value, weight) tuples."""
    if not parts:
        return default
    total_w = sum(w for _, _, w in parts)
    if total_w == 0:
        return default
    return sum(v * w for _, v, w in parts) / total_w


# ─────────────────────────────────────────────────────────────────────────────
# Full feature pass over all state assets
# ─────────────────────────────────────────────────────────────────────────────

def run_full_feature_pass(state: HyperliquidState):
    """
    Compute all features for every asset in state.
    Mutates state.assets in place.
    Should be called:
      - after boot candle/book data is loaded
      - periodically (every ~60s) to refresh scores
    """
    updated: list[ScreenerAsset] = []

    for coin, asset in list(state.assets.items()):
        candles_1h = state.get_candles(coin, "1h", n=50)
        candles_5m = state.get_candles(coin, "5m", n=50)

        candle_feats = compute_candle_features(asset, candles_1h, candles_5m)
        if candle_feats:
            asset = asset.model_copy(update=candle_feats)

        score_feats = compute_scores(asset)
        if score_feats:
            asset = asset.model_copy(update=score_feats)

        state.assets[coin] = asset
        updated.append(asset)

    # Universe-wide pass (percentile ranks + composite)
    ranked = compute_universe_ranks(updated)
    for asset in ranked:
        state.assets[asset.coin] = asset

    return len(updated)
