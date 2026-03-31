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

def _linear_slope(values: list[float]) -> float:
    """
    OLS slope normalized as fractional change per bar relative to the series mean.
    Positive = uptrend, negative = downtrend.
    Returns 0.0 when fewer than 2 values or degenerate input.
    """
    n = len(values)
    if n < 2:
        return 0.0
    base = abs(sum(values) / n)
    if base == 0:
        return 0.0
    ys = [(v - values[0]) / base for v in values]
    xs = list(range(n))
    sx = sum(xs); sy = sum(ys)
    sxy = sum(x * y for x, y in zip(xs, ys))
    sx2 = sum(x * x for x in xs)
    denom = n * sx2 - sx * sx
    if denom == 0:
        return 0.0
    return (n * sxy - sx * sy) / denom


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

    # ── Exhaustion score ──────────────────────────────────────────────────
    # Signals a market that has run hard and may be topping:
    # strong multi-TF run + crowding + fading flow
    ex = 0.0
    if ann_fund > 0.30:    ex += 25    # longs paying huge carry
    elif ann_fund > 0.15:  ex += 12
    if m24h > 8:           ex += 25    # sharp 24h run-up
    elif m24h > 4:         ex += 12
    elif m24h > 2:         ex += 6
    if m24h > 3 and m1h < 0:  ex += 20  # extended but 1h now negative → topping
    elif m24h > 2 and m1h < -0.2: ex += 10
    if flow_score < 40:    ex += 15    # flow fading
    elif flow_score < 47:  ex += 7
    ex += crowding_score * 0.20        # crowding component
    exhaustion_score = _clip(ex, 0, 100)
    updates["exhaustion_score"] = round(exhaustion_score, 1)

    # ── Collapse risk score ───────────────────────────────────────────────
    # Signals imminent breakdown: exhaustion + deteriorating internals
    cr = 0.0
    if exhaustion_score > 55:   cr += 30
    elif exhaustion_score > 40: cr += 15
    oi_5m = asset.oi_change_5m or 0
    if oi_5m < -0.02:    cr += 25      # OI actively collapsing
    elif oi_5m < -0.01:  cr += 12
    if bp_score < 35:    cr += 20      # ask-heavy book (sellers in control)
    elif bp_score < 42:  cr += 10
    if flow_score < 35:  cr += 15      # hard sell flow
    elif flow_score < 42: cr += 7
    if m24h > 2 and m1h < -0.5: cr += 10  # extended + now turning over
    collapse_risk_score = _clip(cr, 0, 100)
    updates["collapse_risk_score"] = round(collapse_risk_score, 1)

    # ── Derive overall_score and setup_type ───────────────────────────────
    setup_candidates = {
        "breakout":           updates["breakout_score"],
        "mean_reversion":     updates["mean_reversion_score"],
        "trend_continuation": updates["trend_continuation_score"],
        "crowding_unwind":    updates["crowding_unwind_score"],
        "exhaustion":         exhaustion_score,
        "collapse_risk":      collapse_risk_score,
    }
    best_setup  = max(setup_candidates, key=lambda k: setup_candidates[k])
    best_score  = setup_candidates[best_setup]

    # Hard overrides (priority order: avoid > collapse_risk > exhaustion > best)
    if updates["avoid_score"] > 65 and tradability_penalty > 50:
        setup_type    = "avoid"
        overall_score = updates["avoid_score"]
    elif collapse_risk_score > 65:
        setup_type    = "collapse_risk"
        overall_score = collapse_risk_score
    elif exhaustion_score > 65 and best_setup not in ("breakout", "trend_continuation"):
        setup_type    = "exhaustion"
        overall_score = exhaustion_score
    else:
        setup_type    = best_setup
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
# Structural quality + regime classification
# ─────────────────────────────────────────────────────────────────────────────

def compute_structural_quality(asset: ScreenerAsset, candles_1h: list[dict]) -> dict:
    """
    Compute structural quality score and asset regime from 1h candle history.

    With up to 200 1h bars (~8 days) we approximate multi-day structure:
      - Long-window OLS slope (last 100 bars ≈ 4 days) → trend direction & strength
      - Short-window OLS slope (last 24 bars ≈ 1 day)  → recent momentum vs HT trend
      - Pct of bars above rolling median               → proxy for "above trend"
      - Higher-high / higher-low in synthetic 4h bars  → HH/HL persistence
      - Range tightening                               → base/consolidation quality
      - Momentum persistence (green bars ratio)        → sustained vs spike

    Returns a dict of new fields for model_copy(update=...).
    """
    ex   = asset.exhaustion_score    or 0
    cr   = asset.collapse_risk_score or 0
    liq  = asset.liquidity_score     or 50
    tp   = asset.tradability_penalty or 30
    flow = asset.flow_score          or 50
    mom  = asset.momentum_score      or 50
    vlm  = asset.day_ntl_vlm        or 0

    liq_quality = _clip(
        liq * 0.60 + (100 - tp) * 0.30 +
        (_clip(math.log10(vlm + 1) / math.log10(1e9) * 100, 0, 100)) * 0.10,
        0, 100
    )

    slope_long_norm  = 0.0
    slope_short_norm = 0.0
    range_tightening = 0.5
    hh_hl_score      = 0.5
    sq_score         = 50.0

    if len(candles_1h) >= 20:
        closes = [float(c["c"]) for c in candles_1h if c.get("c")]
        highs  = [float(c["h"]) for c in candles_1h if c.get("h")]
        lows   = [float(c["l"]) for c in candles_1h if c.get("l")]
        opens  = [float(c["o"]) for c in candles_1h if c.get("o")]
        n = len(closes)

        if n >= 20:
            # Factor 1: Long-window slope (last ≤100 bars ≈ 4 days)
            win_long = min(n, 100)
            slope_long = _linear_slope(closes[-win_long:])
            # Normalize: 0.0005/bar × 100 bars = 5% total move → norm=1
            slope_long_norm = _clip(slope_long / 0.0005, -1.0, 1.0)
            slope_factor = _clip(50 + slope_long_norm * 50, 0, 100)

            # Factor 2: Short-window slope (last ≤24 bars = 1 day)
            win_short = min(n, 24)
            slope_short = _linear_slope(closes[-win_short:])
            slope_short_norm = _clip(slope_short / 0.0005, -1.0, 1.0)

            # Factor 3: Pct above rolling median (proxy for "above trend")
            long_arr = closes[-win_long:]
            long_median = sorted(long_arr)[len(long_arr) // 2]
            pct_above = sum(1 for c in long_arr if c > long_median) / len(long_arr)
            above_factor = _clip((pct_above - 0.30) / 0.40 * 100, 0, 100)

            # Factor 4: Range tightening — recent vs broader range as % of price
            r_n = max(min(12, n // 4), 2)
            o_n = min(48, n)
            midpt = (max(closes[-o_n:]) + min(closes[-o_n:])) / 2 if closes else 1.0
            if midpt > 0:
                rng_recent = (max(closes[-r_n:]) - min(closes[-r_n:])) / midpt * 100
                rng_older  = (max(closes[-o_n:]) - min(closes[-o_n:])) / midpt * 100
                range_tightening = 1.0 - min(rng_recent / max(rng_older, 0.01), 1.0)
            range_factor = range_tightening * 100

            # Factor 5: Momentum persistence — fraction of last 12 bars that are green
            n_bars = min(12, n)
            pairs = list(zip(opens[-n_bars:], closes[-n_bars:])) if len(opens) >= n_bars and len(closes) >= n_bars else []
            pos_bars = sum(1 for o, c in pairs if c > o)
            mom_persistence = pos_bars / len(pairs) if pairs else 0.5
            persist_factor = _clip((mom_persistence - 0.30) / 0.40 * 100, 0, 100)

            # Factor 6: Higher-high / higher-low in synthetic 4h bars
            if len(highs) >= 20 and len(lows) >= 20:
                syn_h = [max(highs[i:i+4]) for i in range(0, len(highs) - 3, 4)]
                syn_l = [min(lows[i:i+4])  for i in range(0, len(lows)  - 3, 4)]
                if len(syn_h) >= 4:
                    k = min(7, len(syn_h) - 1)
                    hh = sum(1 for i in range(1, k + 1) if len(syn_h) > i and syn_h[-i] > syn_h[-i-1])
                    hl = sum(1 for i in range(1, k + 1) if len(syn_l) > i and syn_l[-i] > syn_l[-i-1])
                    hh_hl_score = (hh + hl) / (k * 2) if k > 0 else 0.5

            sq_raw = _weighted_avg([
                ("slope_long",   slope_factor,        0.30),
                ("above_median", above_factor,         0.20),
                ("hh_hl",        hh_hl_score * 100,   0.20),
                ("range_tight",  range_factor,         0.15),
                ("mom_persist",  persist_factor,       0.15),
            ], default=50.0)
            sq_score = _clip(sq_raw, 0, 100)

    # ── Asset regime classification ────────────────────────────────────────
    # Thresholds tuned so the regime is meaningful with 1h-bar proxies
    long_uptrend   = slope_long_norm  >  0.15   # clear multi-day uptrend
    long_downtrend = slope_long_norm  < -0.15   # clear multi-day downtrend
    short_spike    = slope_short_norm >  0.50   # sharp recent move on a downtrend = dead-cat

    if cr > 55:
        regime = "collapse_risk"
    elif ex > 60 and long_uptrend:
        regime = "late_extension_exhaustion"
    elif long_downtrend and short_spike:
        regime = "downtrend_dead_cat"
    elif sq_score >= 52 and not long_downtrend:
        if range_tightening > 0.55 and abs(slope_short_norm) < 0.30:
            regime = "structural_uptrend_breakout_watch"
        else:
            regime = "structural_uptrend_pullback"
    elif sq_score < 42 and (mom > 65 or flow > 65):
        regime = "speculative_reversal"
    else:
        regime = "chop_low_quality"

    # ── Score families ─────────────────────────────────────────────────────
    pq = 0.0
    if regime in ("structural_uptrend_pullback", "structural_uptrend_breakout_watch"):
        pq = sq_score * 0.50 + (100 - mom) * 0.30 + flow * 0.20
    pullback_quality = _clip(pq, 0, 100)

    vol_imp = asset.volume_impulse or 1.0
    bo_ready = _clip(
        range_tightening * 100 * 0.40 +
        sq_score              * 0.30 +
        _clip(100 - (vol_imp - 1.0) * 30, 0, 100) * 0.20 +
        (asset.breakout_score or 50) * 0.10,
        0, 100
    )

    cont = _clip(
        sq_score                                     * 0.35 +
        (asset.trend_continuation_score or 50)       * 0.25 +
        flow                                         * 0.20 +
        liq                                          * 0.10 +
        hh_hl_score * 100                            * 0.10,
        0, 100
    )

    spec = _clip(
        mom                                  * 0.30 +
        flow                                 * 0.25 +
        (asset.mean_reversion_score or 50)   * 0.25 +
        liq                                  * 0.10 +
        (100 - sq_score)                     * 0.10,
        0, 100
    )

    # ── Quality-adjusted overall_score ────────────────────────────────────
    # Penalize overall_score based on regime and structural quality so that
    # ALL downstream sorts (snapshot, auto-rank, hero) naturally rank
    # structurally sound assets above dead-cat bounces and meme spikes.
    #
    # Regime multipliers:
    #   structural_uptrend_pullback    → 1.00  (full credit)
    #   structural_uptrend_breakout_watch → 0.95
    #   chop_low_quality               → 0.68  (moderate penalty)
    #   late_extension_exhaustion      → 0.60
    #   collapse_risk                  → 0.50
    #   speculative_reversal           → 0.45
    #   downtrend_dead_cat             → 0.35  (severe — dead cats must not rank)
    #
    # SQ factor: 0.55 + (sq/100)*0.45 → ranges 0.55 (SQ=0) to 1.00 (SQ=100)
    _REGIME_MULT = {
        "structural_uptrend_pullback":       1.00,
        "structural_uptrend_breakout_watch": 0.95,
        "chop_low_quality":                  0.68,
        "late_extension_exhaustion":         0.60,
        "collapse_risk":                     0.50,
        "speculative_reversal":              0.45,
        "downtrend_dead_cat":                0.35,
    }
    raw_ov    = asset.overall_score or 50
    raw_comp  = asset.composite_signal_score or 50
    r_mult    = _REGIME_MULT.get(regime, 0.68)
    sq_norm   = sq_score / 100
    sq_factor = 0.55 + sq_norm * 0.45

    # Apply the same multiplier to BOTH scores so every sort/display path
    # (overallScore AND compositeSignal) reflects structural quality.
    qa_score  = _clip(raw_ov   * r_mult * sq_factor, 0, 100)
    qa_comp   = _clip(raw_comp * r_mult * sq_factor, 0, 100)

    return {
        "structural_quality_score":   round(sq_score, 1),
        "asset_regime":               regime,
        "liquidity_quality_score":    round(liq_quality, 1),
        "pullback_quality_score":     round(pullback_quality, 1),
        "breakout_readiness_score":   round(bo_ready, 1),
        "continuation_score":         round(cont, 1),
        "speculative_reversal_score": round(spec, 1),
        "overall_score":              round(qa_score, 1),   # quality-adjusted
        "composite_signal_score":     round(qa_comp, 1),   # quality-adjusted
    }


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

    Universe gate: only assets in state.universe_allowlist are scored.
    Any asset not in the allowlist (should not exist post-boot, but guarded
    defensively) is skipped and not included in percentile calculations.
    """
    updated: list[ScreenerAsset] = []
    skipped_non_universe = 0

    for coin, asset in list(state.assets.items()):
        # ── Universe gate ────────────────────────────────────────────────
        if state.universe_allowlist and coin not in state.universe_allowlist:
            skipped_non_universe += 1
            continue
        # Fetch more 1h candles for structural quality analysis (≤120 bars ≈ 5 days)
        candles_1h = state.get_candles(coin, "1h", n=120)
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

        # Structural quality + regime (uses existing scores + candle history)
        struct_feats = compute_structural_quality(asset, candles_1h)
        if struct_feats:
            asset = asset.model_copy(update=struct_feats)

        state.assets[coin] = asset
        updated.append(asset)

    # Universe-wide pass (percentile ranks + composite)
    ranked = compute_universe_ranks(updated)
    for asset in ranked:
        state.assets[asset.coin] = asset

    if skipped_non_universe:
        print(f"[HL][feature] Skipped {skipped_non_universe} non-universe assets during feature pass")

    return len(updated)
