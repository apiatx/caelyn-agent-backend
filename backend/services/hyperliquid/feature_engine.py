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

    # ── Volume impulse ────────────────────────────────────────────────────
    # Compare last 1h candle volume vs rolling average of prior N candles.
    # impulse > 1.0 = elevated volume, < 1.0 = quiet
    if len(candles_1h) >= 6:
        vol_series = [float(c.get("v", 0) or 0) for c in candles_1h if c.get("v")]
        if vol_series and vol_series[-1] > 0:
            prior_avg = sum(vol_series[-7:-1]) / max(len(vol_series[-7:-1]), 1)
            if prior_avg > 0:
                updates["volume_impulse"] = round(vol_series[-1] / prior_avg, 3)

    return updates


def compute_volume_impulse_5m(candles_5m: list[dict]) -> dict:
    """
    Compute short-term volume impulse from 5m candle data.
    Returns volume_impulse_5m (last bar vs rolling avg) and
    volume_impulse_15m (last 3 bars' sum vs rolling avg of prior 12 bars).
    """
    if len(candles_5m) < 6:
        return {}

    vols = [float(c.get("v", 0) or 0) for c in candles_5m if c.get("v") is not None]
    if not vols or len(vols) < 6:
        return {}

    result = {}
    # 5m impulse: last bar vs average of prior 5 bars
    last_bar = vols[-1]
    prior_5  = vols[-6:-1]
    if prior_5:
        avg_5 = sum(prior_5) / len(prior_5)
        if avg_5 > 0:
            result["volume_impulse_5m"] = round(last_bar / avg_5, 3)

    # 15m impulse: sum of last 3 bars vs average of prior 12 bars
    if len(vols) >= 15:
        last_3   = sum(vols[-3:])
        prior_12 = vols[-15:-3]
        if prior_12:
            avg_12 = sum(prior_12) / len(prior_12) * 3   # normalize to 3-bar window
            if avg_12 > 0:
                result["volume_impulse_15m"] = round(last_3 / avg_12, 3)

    return result


def compute_scores(asset: ScreenerAsset) -> dict:
    """
    Compute the full signal-engine score suite (0–100 each) and qualitative flags.

    9 component scores:
      liquidity_score, volatility_score, momentum_score, flow_score,
      trend_score, book_pressure_score, crowding_score, dislocation_score,
      tradability_penalty

    5 setup-specific scores:
      breakout_score, mean_reversion_score, trend_continuation_score,
      crowding_unwind_score, avoid_score

    Derived:
      composite_signal_score, overall_score, setup_type,
      signal_direction, signal_confidence

    Returns a partial dict for model_copy(update=...).
    """
    updates: dict = {}

    # Pre-compute frequently used values
    vlm          = asset.day_ntl_vlm or 0
    oi           = asset.open_interest_usd or 0
    fund         = asset.funding or 0
    ann_fund     = fund * 8760
    spread_bps   = asset.spread_bps or 0
    m5           = asset.momentum_5m   or 0
    m1h          = asset.momentum_1h   or 0
    m4h          = asset.momentum_4h   or 0
    m24h         = asset.momentum_24h  or 0
    flow_imb     = asset.recent_trade_imbalance or 0
    book_imb     = asset.orderbook_imbalance    or 0
    bid_d        = asset.orderbook_bid_depth    or 0
    ask_d        = asset.orderbook_ask_depth    or 0
    rv_short     = asset.realized_volatility_short or 0
    rv_medium    = asset.realized_volatility_medium or 0
    dist_oracle  = asset.distance_mark_oracle_pct or 0
    premium      = asset.premium or 0

    # ── 1. Liquidity score ────────────────────────────────────────────────
    liq_parts = []
    if vlm > 0:
        liq_parts.append(("vol",   _clip(math.log10(vlm + 1) / math.log10(1e10) * 100, 0, 100),   1.0))
    if asset.spread_bps is not None:
        liq_parts.append(("spread", _clip(100 - spread_bps * 2, 0, 100),                            1.5))
    if bid_d + ask_d > 0:
        liq_parts.append(("depth",  _clip(math.log10(bid_d + ask_d + 1) / 6 * 100, 0, 100),        1.0))
    liq_score = _weighted_avg(liq_parts, default=50.0)
    updates["liquidity_score"] = round(liq_score, 1)

    # ── 2. Volatility score ───────────────────────────────────────────────
    rv = rv_short if rv_short > 0 else rv_medium
    vol_score = _clip(rv / 3.0, 0, 100) if rv > 0 else 50.0
    updates["volatility_score"] = round(vol_score, 1)

    # ── 3. Momentum score ─────────────────────────────────────────────────
    # Weighted sum of multi-TF momentum → 0-100 centered at 50 (50 = flat)
    mom_raw = 0.0
    mom_wt  = 0.0
    if asset.momentum_5m  is not None: mom_raw += m5  * 4.0; mom_wt += 4.0
    if asset.momentum_1h  is not None: mom_raw += m1h * 3.0; mom_wt += 3.0
    if asset.momentum_4h  is not None: mom_raw += m4h * 2.0; mom_wt += 2.0
    if asset.momentum_24h is not None: mom_raw += m24h * 1.0; mom_wt += 1.0
    avg_mom = mom_raw / mom_wt if mom_wt > 0 else 0
    mom_score = _clip(50 + avg_mom * 5, 0, 100)   # ±10% avg maps to 0–100
    updates["momentum_score"] = round(mom_score, 1)

    # ── 4. Flow score ─────────────────────────────────────────────────────
    # Trade flow + book imbalance → direction of real-time demand pressure
    fl_raw = 0.0; fl_wt = 0.0
    if asset.recent_trade_imbalance is not None: fl_raw += flow_imb * 2.0; fl_wt += 2.0
    if asset.orderbook_imbalance    is not None: fl_raw += book_imb  * 1.0; fl_wt += 1.0
    avg_fl = fl_raw / fl_wt if fl_wt > 0 else 0
    flow_score = _clip(50 + avg_fl * 50, 0, 100)
    updates["flow_score"] = round(flow_score, 1)

    # ── 5. Trend score ────────────────────────────────────────────────────
    # Multi-timeframe alignment: are all TF pointing the same direction?
    # All positive or all negative → 75-100; mixed → 25-50
    trends = [x for x in [m1h, m4h, m24h] if x != 0]
    if len(trends) >= 2:
        same_dir = all(t > 0 for t in trends) or all(t < 0 for t in trends)
        base_dir  = 1 if all(t > 0 for t in trends) else -1
        strength  = sum(abs(t) for t in trends) / len(trends)
        # Strength: 0-5% average = weak, >5% = strong
        s_factor  = _clip(strength / 5.0, 0, 1)
        if same_dir:
            trend_score = _clip(50 + base_dir * s_factor * 50, 0, 100)
        else:
            # Conflicting timeframes → muted signal near 50
            trend_score = 50 + sum(trends) / len(trends) * 3
            trend_score = _clip(trend_score, 20, 80)
    else:
        trend_score = 50.0
    updates["trend_score"] = round(trend_score, 1)

    # ── 6. Book pressure score ────────────────────────────────────────────
    # Pure order book bid vs ask depth skew
    total_book = bid_d + ask_d
    if total_book > 0:
        bp_score = _clip(50 + (bid_d - ask_d) / total_book * 50, 0, 100)
    else:
        bp_score = 50.0
    updates["book_pressure_score"] = round(bp_score, 1)

    # ── 7. Crowding score ─────────────────────────────────────────────────
    # Extreme funding (abs) AND elevated OI → crowded market
    fund_extremity  = min(abs(ann_fund) / 0.5, 1.0)   # 0 = no funding, 1 = 50% annual
    oi_concentration = _clip(math.log10(oi + 1) / math.log10(5e9) * 100, 0, 100) if oi > 0 else 0
    crowding_score  = _clip((fund_extremity * 60 + oi_concentration * 0.4), 0, 100)
    updates["crowding_score"] = round(crowding_score, 1)

    # ── 8. Dislocation score ──────────────────────────────────────────────
    # How far is the market from fair value? (oracle, premium, mid)
    disloc_parts = []
    disloc_parts.append(min(abs(dist_oracle) / 1.5, 1.0))   # 1.5% oracle gap = max
    disloc_parts.append(min(abs(premium) / 0.01, 1.0))       # 1% premium = max
    dist_mid = asset.distance_mark_mid_pct or 0
    disloc_parts.append(min(abs(dist_mid) / 0.5, 1.0))       # 0.5% mid gap = max
    disloc_score = sum(disloc_parts) / len(disloc_parts) * 100
    updates["dislocation_score"] = round(_clip(disloc_score, 0, 100), 1)

    # ── 9. Tradability penalty ────────────────────────────────────────────
    # Wide spread, thin book, low OI → penalty (100 = untradeable)
    penalty_parts = []
    if asset.spread_bps is not None:
        penalty_parts.append(_clip(spread_bps / 30, 0, 1))   # 30bps = max penalty
    if bid_d + ask_d > 0:
        depth_penalty = 1.0 - _clip(math.log10(bid_d + ask_d + 1) / 6, 0, 1)
        penalty_parts.append(depth_penalty)
    if vlm > 0:
        vol_penalty = 1.0 - _clip(math.log10(vlm + 1) / math.log10(1e9), 0, 1)
        penalty_parts.append(vol_penalty * 0.5)
    tradability_penalty = (sum(penalty_parts) / len(penalty_parts) * 100) if penalty_parts else 30.0
    updates["tradability_penalty"] = round(_clip(tradability_penalty, 0, 100), 1)

    # ── Setup-specific scores ─────────────────────────────────────────────

    tp_factor = tradability_penalty / 100   # 0=tradeable, 1=untradeable

    # Breakout: positive momentum + buy flow + book bid-heavy + decent liquidity
    bo_directional = mom_score if mom_score >= 50 else (100 - mom_score)  # directional momentum magnitude
    breakout_score = (
        bo_directional   * 0.30 +
        flow_score       * 0.20 +
        bp_score         * 0.20 +
        liq_score        * 0.15 +
        vol_score        * 0.10 +
        trend_score      * 0.05
    ) - tradability_penalty * 0.10
    updates["breakout_score"] = round(_clip(breakout_score, 0, 100), 1)

    # Mean reversion: extreme dislocation + crowding + momentum stretched
    # Higher when market is far from fair value and momentum is extreme
    mom_stretch = abs(mom_score - 50) * 2   # 0-100: high = stretched momentum
    mr_score = (
        disloc_score     * 0.35 +
        crowding_score   * 0.30 +
        mom_stretch      * 0.20 +
        liq_score        * 0.15
    ) - tradability_penalty * 0.05
    updates["mean_reversion_score"] = round(_clip(mr_score, 0, 100), 1)

    # Trend continuation: sustained aligned trend + healthy flow + liquidity
    tc_score = (
        trend_score      * 0.35 +
        mom_score        * 0.25 +
        flow_score       * 0.20 +
        liq_score        * 0.20
    ) - tradability_penalty * 0.10
    updates["trend_continuation_score"] = round(_clip(tc_score, 0, 100), 1)

    # Crowding unwind: extreme funding + elevated OI + momentum adverse to crowded side
    # Long crowd + negative momentum = unwind candidate; short crowd + positive momentum = squeeze
    crowded_long_now  = ann_fund > 0.20 and oi > 500_000
    crowded_short_now = ann_fund < -0.20 and oi > 500_000
    if crowded_long_now:
        adverse_mom = _clip(50 - mom_score, 0, 50) * 2   # negative momentum = high
    elif crowded_short_now:
        adverse_mom = _clip(mom_score - 50, 0, 50) * 2   # positive momentum = high
    else:
        adverse_mom = 0.0
    cu_score = (
        crowding_score   * 0.40 +
        disloc_score     * 0.20 +
        adverse_mom      * 0.30 +
        liq_score        * 0.10
    )
    updates["crowding_unwind_score"] = round(_clip(cu_score, 0, 100), 1)

    # Avoid: wide spread, thin book, instability — high = stay away
    avoid_score = (
        tradability_penalty * 0.60 +
        (100 - liq_score)   * 0.30 +
        vol_score           * 0.10
    )
    updates["avoid_score"] = round(_clip(avoid_score, 0, 100), 1)

    # ── Derive overall_score and setup_type ───────────────────────────────
    setup_candidates = {
        "breakout":           updates["breakout_score"],
        "mean_reversion":     updates["mean_reversion_score"],
        "trend_continuation": updates["trend_continuation_score"],
        "crowding_unwind":    updates["crowding_unwind_score"],
    }
    best_setup  = max(setup_candidates, key=lambda k: setup_candidates[k])
    best_score  = setup_candidates[best_setup]

    # If avoid_score is dominant and tradability penalty is high → override
    if updates["avoid_score"] > 65 and tradability_penalty > 50:
        setup_type   = "avoid"
        overall_score = updates["avoid_score"]
    else:
        setup_type   = best_setup
        overall_score = best_score
    updates["setup_type"]    = setup_type
    updates["overall_score"] = round(overall_score, 1)

    # ── Composite signal score (balanced mode, backward-compatible) ───────
    composite = (
        mom_score    * 0.25 +
        flow_score   * 0.25 +
        updates["breakout_score"] * 0.20 +
        updates["mean_reversion_score"] * 0.15 +
        liq_score    * 0.10 +
        vol_score    * 0.05
    )
    updates["composite_signal_score"] = round(_clip(composite, 0, 100), 1)

    # ── Signal direction ──────────────────────────────────────────────────
    avg_directional = (mom_score + flow_score) / 2
    if avg_directional >= 60:
        updates["signal_direction"]  = "long"
        updates["signal_confidence"] = round((avg_directional - 50) / 50, 2)
    elif avg_directional <= 40:
        updates["signal_direction"]  = "short"
        updates["signal_confidence"] = round((50 - avg_directional) / 50, 2)
    else:
        updates["signal_direction"]  = "neutral"
        updates["signal_confidence"] = round(abs(avg_directional - 50) / 50, 2)

    # ── Qualitative flags ─────────────────────────────────────────────────
    crowded_long  = ann_fund > 0.30 and oi > 1_000_000
    crowded_short = ann_fund < -0.30 and oi > 1_000_000
    squeeze       = crowded_short and m1h > 0.3
    dislocated    = abs(dist_oracle) > 0.5
    trend_cont    = (m1h > 0.1 and m4h > 0.2 and m24h > 0.5) or (m1h < -0.1 and m4h < -0.2 and m24h < -0.5)
    mr_cand       = updates["mean_reversion_score"] > 65
    illiquid_hv   = vlm < 5_000_000 and rv_short > 150
    avoid_spread  = spread_bps > 20

    updates.update({
        "crowded_long":                 crowded_long,
        "crowded_short":                crowded_short,
        "squeeze_candidate":            squeeze,
        "dislocated_vs_oracle":         dislocated,
        "trend_continuation_candidate": trend_cont,
        "mean_reversion_candidate":     mr_cand,
        "illiquid_high_volatility":     illiquid_hv,
        "avoid_due_to_spread":          avoid_spread,
    })

    return updates


# ─────────────────────────────────────────────────────────────────────────────
# Universe-wide computations (percentile ranks + composite score)
# ─────────────────────────────────────────────────────────────────────────────

def compute_universe_ranks(assets: list[ScreenerAsset]) -> list[ScreenerAsset]:
    """
    Compute universe-wide percentile ranks (volume, OI, funding, volatility).
    compute_scores() already finalizes composite_signal_score/signal_direction/setup_type
    so we only add the percentile fields here.
    """
    perps = [a for a in assets if a.market_type == "perp"]
    spots = [a for a in assets if a.market_type == "spot"]

    result_map: dict[str, ScreenerAsset] = {}

    # Population vectors for percentile ranks
    vlm_pop  = [a.day_ntl_vlm         for a in perps if a.day_ntl_vlm         is not None]
    oi_pop   = [a.open_interest_usd   for a in perps if a.open_interest_usd   is not None]
    fund_pop = [abs(a.funding or 0)   for a in perps]
    vol_pop  = [a.realized_volatility_medium for a in perps if a.realized_volatility_medium is not None]

    for asset in perps:
        updates: dict = {}
        if asset.day_ntl_vlm            is not None and vlm_pop:
            updates["volume_percentile"]      = round(_percentile_rank(asset.day_ntl_vlm,           vlm_pop),  3)
        if asset.open_interest_usd       is not None and oi_pop:
            updates["oi_percentile"]          = round(_percentile_rank(asset.open_interest_usd,      oi_pop),   3)
        if asset.funding                 is not None and fund_pop:
            updates["funding_percentile"]     = round(_percentile_rank(abs(asset.funding),           fund_pop), 3)
        if asset.realized_volatility_medium is not None and vol_pop:
            updates["volatility_percentile"]  = round(_percentile_rank(asset.realized_volatility_medium, vol_pop), 3)
        result_map[asset.coin] = asset.model_copy(update=updates)

    # Spot: minimal scoring (score already set by compute_scores)
    for asset in spots:
        pct = asset.pct_change_24h or 0
        updates = {}
        if asset.composite_signal_score is None:
            updates["composite_signal_score"] = round(_clip(50 + pct * 2, 0, 100), 1)
        if asset.signal_direction is None:
            updates["signal_direction"] = "long" if pct > 1 else ("short" if pct < -1 else "neutral")
        result_map[asset.coin] = asset.model_copy(update=updates) if updates else asset

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

        # 5m volume impulse from 5m candle series
        vol_5m_feats = compute_volume_impulse_5m(candles_5m)
        if vol_5m_feats:
            asset = asset.model_copy(update=vol_5m_feats)

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
