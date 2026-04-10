"""
Hyperliquid Screener — Agent Briefing & Hero Signal Generator

Produces two outputs:
1. generate_agent_briefing()   → full "Agent Market Brief" payload (new /hero shape)
2. generate_hero_signals()     → legacy HeroSignal list (used by /sections)
3. build_signal_sections()     → section-oriented payload
4. build_summary_cards()       → top bar summary cards

Scoring is 100% deterministic. No LLM calls.
"""
from __future__ import annotations

import math
import time
from statistics import mean, median
from typing import Optional

from .models import HeroSignal, HeroSignalMetrics, ScreenerAsset
from .state import HyperliquidState


# ─────────────────────────────────────────────────────────────────────────────
# Market Regime
# ─────────────────────────────────────────────────────────────────────────────

def _compute_market_regime(perps: list[ScreenerAsset]) -> dict:
    """
    Classify current market environment from breadth + signal distribution.

    Returns:
      regime     : str label
      description: human-readable explanation
      metrics    : supporting numbers for transparency
    """
    if not perps:
        return {"regime": "unknown", "description": "Insufficient data", "metrics": {}}

    total = len(perps)
    long_ct   = sum(1 for a in perps if (a.signal_direction or "") == "long")
    short_ct  = sum(1 for a in perps if (a.signal_direction or "") == "short")
    neutral_ct = total - long_ct - short_ct

    long_pct  = long_ct  / total
    short_pct = short_ct / total

    ann_funds = [abs((a.funding or 0) * 8760) for a in perps]
    avg_ann_fund = mean(ann_funds) if ann_funds else 0
    med_ann_fund = median(ann_funds) if ann_funds else 0

    composites = [a.composite_signal_score for a in perps if a.composite_signal_score is not None]
    avg_composite = mean(composites) if composites else 50

    exhaustion_ct    = sum(1 for a in perps if (a.exhaustion_score or 0) > 55)
    collapse_ct      = sum(1 for a in perps if (a.collapse_risk_score or 0) > 55)
    crowded_long_ct  = sum(1 for a in perps if a.crowded_long)
    crowded_short_ct = sum(1 for a in perps if a.crowded_short)

    exhaustion_pct = (exhaustion_ct + collapse_ct) / total

    # ── Regime classification ──────────────────────────────────────────────
    if long_pct > 0.60 and avg_ann_fund > 0.15:
        regime      = "overcrowded upside"
        description = (
            f"{long_pct:.0%} of perps are signaling long while average funding is "
            f"{avg_ann_fund:.1%} annual — longs are paying an unsustainable carry. "
            f"Squeeze risk is elevated. Favor fading crowded longs or waiting for a flush."
        )
    elif exhaustion_pct > 0.25 or (long_pct > 0.55 and avg_ann_fund > 0.25):
        regime      = "fragile / reversal-prone"
        description = (
            f"{exhaustion_pct:.0%} of perps show exhaustion or collapse signals. "
            f"Momentum is present but internals are deteriorating — crowded positioning "
            f"and fading flow suggest a reversal is more likely than continuation."
        )
    elif long_pct > 0.55 and avg_composite > 57:
        regime      = "risk-on momentum"
        description = (
            f"Broad bullish breadth: {long_pct:.0%} of perps signaling long "
            f"with average composite score {avg_composite:.0f}/100. "
            f"Funding is contained ({avg_ann_fund:.1%} annual) — momentum is clean."
        )
    elif short_pct > 0.50:
        regime      = "weak breadth / defensive"
        description = (
            f"Bearish breadth: {short_pct:.0%} of perps are signaling short or fading. "
            f"Average composite score {avg_composite:.0f}/100 is below neutral. "
            f"Defensive posture warranted — long setups need to be high-conviction."
        )
    elif avg_composite < 47:
        regime      = "weak breadth / defensive"
        description = (
            f"Low signal strength across the board (avg {avg_composite:.0f}/100). "
            f"No clear directional bias — {long_pct:.0%} long vs {short_pct:.0%} short. "
            f"Best plays are mean-reversion and waiting for a cleaner breakout."
        )
    else:
        regime      = "mixed / rotational"
        description = (
            f"No dominant regime: {long_pct:.0%} long / {short_pct:.0%} short / "
            f"{neutral_ct/total:.0%} neutral. Signal strength avg {avg_composite:.0f}/100. "
            f"Markets are rotating — favor selective setups over broad exposure."
        )

    return {
        "regime": regime,
        "description": description,
        "metrics": {
            "long_pct":        round(long_pct, 3),
            "short_pct":       round(short_pct, 3),
            "neutral_pct":     round(neutral_ct / total, 3),
            "avg_ann_fund_pct": round(avg_ann_fund * 100, 2),
            "med_ann_fund_pct": round(med_ann_fund * 100, 2),
            "avg_composite":   round(avg_composite, 1),
            "exhaustion_pct":  round(exhaustion_pct, 3),
            "crowded_long_ct": crowded_long_ct,
            "crowded_short_ct": crowded_short_ct,
            "total_perps":     total,
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# Bucket classification
# ─────────────────────────────────────────────────────────────────────────────

def _classify_bucket(asset: ScreenerAsset, side: str) -> str:
    """
    Assign asset to one of five guidance buckets using the hierarchical pipeline.

    Priority: avoid > collapse_watch > speculative_reversals > buy_now > high_quality_watchlist

    New bucket names (with legacy aliases kept in generate_agent_briefing):
      buy_now              ← replaces trade_now
      high_quality_watchlist ← replaces watch_breakout
      speculative_reversals  ← new
      collapse_watch       ← replaces watch_collapse
      avoid                ← same
    """
    tp     = asset.tradability_penalty      or 30
    av     = asset.avoid_score              or 0
    ex     = asset.exhaustion_score         or 0
    cr     = asset.collapse_risk_score      or 0
    liq    = asset.liquidity_score          or 50
    ov     = asset.overall_score            or 0
    bo     = asset.breakout_score           or 0
    flow   = asset.flow_score               or 50
    regime = asset.asset_regime             or "chop_low_quality"
    sq     = asset.structural_quality_score or 50
    liq_q  = asset.liquidity_quality_score  or liq

    # ── 1. Avoid: untradeable / illiquid ──────────────────────────────────
    if tp > 55 or av > 68 or liq < 22:
        return "avoid"

    # ── 2. Collapse watch: active deterioration ────────────────────────────
    if regime in ("collapse_risk", "late_extension_exhaustion") or cr > 55 or ex > 62:
        return "collapse_watch"

    # ── 3. Speculative reversals: low structural quality, short-term bounce ─
    # These must NEVER appear as top longs — separated into their own bucket
    if regime in ("speculative_reversal", "downtrend_dead_cat"):
        return "speculative_reversals"
    if sq < 38 and side == "long":
        return "speculative_reversals"

    # ── 4. Buy now: highest quality, structurally sound, actionable ────────
    if (
        regime in ("structural_uptrend_pullback",)
        and sq >= 52
        and liq_q >= 38
        and ov >= 54
        and tp <= 45
        and flow >= 46
        and side in ("long",)
    ):
        return "buy_now"

    # Shorts with good flow signal as buy_now (action on the short side)
    if side == "short" and ov >= 56 and tp <= 42:
        return "buy_now"

    # ── 5. High-quality watchlist: good structure, not yet triggered ────────
    if sq >= 45 and regime in ("structural_uptrend_pullback", "structural_uptrend_breakout_watch"):
        return "high_quality_watchlist"
    if bo >= 52 and ex < 48 and tp <= 52:
        return "high_quality_watchlist"
    if ov >= 54 and tp <= 45 and side not in ("neutral_watch",):
        return "buy_now"

    return "high_quality_watchlist"


# ─────────────────────────────────────────────────────────────────────────────
# Side derivation (extended for new setup types)
# ─────────────────────────────────────────────────────────────────────────────

def _derive_side(asset: ScreenerAsset, setup: Optional[str] = None) -> str:
    st    = setup or asset.setup_type or "breakout"
    m24h  = asset.momentum_24h  or 0
    m1h   = asset.momentum_1h   or 0
    flow  = asset.flow_score    or 50
    ann_f = (asset.funding or 0) * 8760
    direction = asset.signal_direction or "neutral"

    if st == "exhaustion":
        # The asset ran up and is topping → short the exhaustion
        if m24h > 1: return "short"
        if m24h < -3: return "long"   # collapsed → may bounce
        return "short"

    if st == "collapse_risk":
        return "short"   # active deterioration → short

    if st == "crowding_unwind":
        if ann_f > 0:    return "short"   # longs crowded → fade longs
        elif ann_f < 0:  return "long"    # shorts crowded → squeeze
        return "neutral_watch"

    if st == "mean_reversion":
        m = m1h or 0
        if m > 0:    return "short"
        elif m < 0:  return "long"
        return "neutral_watch"

    if st in ("breakout", "trend_continuation"):
        if direction == "long":  return "long"
        if direction == "short": return "short"
        if (asset.momentum_1h or 0) > 0: return "long"
        if (asset.momentum_1h or 0) < 0: return "short"
        return "neutral_watch"

    return "neutral_watch"


# ─────────────────────────────────────────────────────────────────────────────
# Confidence derivation
# ─────────────────────────────────────────────────────────────────────────────

def _derive_confidence(asset: ScreenerAsset, setup: str) -> float:
    base = (asset.overall_score or 50) / 100
    boost = 0.0
    if abs(asset.recent_trade_imbalance or 0) > 0.4:  boost += 0.05
    if abs(asset.orderbook_imbalance or 0) > 0.3:     boost += 0.05
    if (asset.tradability_penalty or 50) < 20:        boost += 0.05
    if (asset.liquidity_score or 0) > 70:             boost += 0.03
    if setup in ("exhaustion", "collapse_risk"):
        # Exhaustion/collapse confidence boosted by convergence
        if (asset.exhaustion_score or 0) > 65 and (asset.collapse_risk_score or 0) > 50:
            boost += 0.06
    if (asset.liquidity_score or 0) < 30:             boost -= 0.10
    if (asset.tradability_penalty or 0) > 50:         boost -= 0.10
    return max(0.1, min(0.99, base + boost))


# ─────────────────────────────────────────────────────────────────────────────
# Narrative: reasons, what_to_watch, risk_flags, invalidation, thesis
# ─────────────────────────────────────────────────────────────────────────────

def _build_reasons(asset: ScreenerAsset, setup: str) -> list[str]:
    reasons: list[str] = []
    ann_fund = (asset.funding or 0) * 8760
    m1h  = asset.momentum_1h  or 0
    m4h  = asset.momentum_4h  or 0
    m24h = asset.momentum_24h or 0

    if setup == "breakout":
        if m1h > 0.2:  reasons.append(f"Strong 1h momentum: +{m1h:.2f}%")
        elif m1h < -0.2: reasons.append(f"Sharp 1h decline: {m1h:.2f}%")
        if m4h != 0:
            dir_ = "↑" if m4h > 0 else "↓"
            reasons.append(f"4h trend {dir_}: {m4h:+.2f}%")
        fi = asset.recent_trade_imbalance or 0
        if fi > 0.3:   reasons.append(f"Buy flow dominant: {fi:.0%} net buyers")
        elif fi < -0.3: reasons.append(f"Sell flow dominant: {abs(fi):.0%} net sellers")
        bi = asset.orderbook_imbalance or 0
        if bi > 0.3:   reasons.append(f"Bid-heavy order book: {bi:+.2f}")
        elif bi < -0.3: reasons.append(f"Ask-heavy order book: {bi:+.2f}")
        if (asset.volume_impulse or 0) > 1.5:
            reasons.append(f"Volume surge: {asset.volume_impulse:.1f}× normal")

    elif setup == "mean_reversion":
        if abs(ann_fund) > 0.10:
            side_str = "longs" if ann_fund > 0 else "shorts"
            reasons.append(f"Extreme funding: {side_str} paying {abs(ann_fund):.1%} annual")
        og = asset.distance_mark_oracle_pct or 0
        if abs(og) > 0.3:
            dir_ = "above" if og > 0 else "below"
            reasons.append(f"Mark {dir_} oracle by {abs(og):.2f}%")
        if abs(asset.premium or 0) > 0.003:
            prem_dir = "premium" if (asset.premium or 0) > 0 else "discount"
            reasons.append(f"Trading at {abs(asset.premium or 0):.3%} {prem_dir}")
        if abs(m24h) > 2:
            reasons.append(f"Stretched 24h move: {m24h:+.2f}% — mean reversion due")

    elif setup == "trend_continuation":
        tfs = [f for f in [
            (f"1h: {m1h:+.2f}%" if asset.momentum_1h else None),
            (f"4h: {m4h:+.2f}%" if asset.momentum_4h else None),
            (f"24h: {m24h:+.2f}%" if asset.momentum_24h else None),
        ] if f]
        if tfs: reasons.append(f"Aligned multi-TF momentum: {', '.join(tfs)}")
        fi = asset.recent_trade_imbalance or 0
        if abs(fi) > 0.2:
            reasons.append(f"Flow sustaining trend: {fi:+.2f} imbalance")
        if (asset.liquidity_score or 0) > 60:
            reasons.append("High-liquidity market supports trend")

    elif setup == "crowding_unwind":
        if ann_fund > 0:
            reasons.append(f"Longs heavily crowded: {ann_fund:.1%} annual funding")
        elif ann_fund < 0:
            reasons.append(f"Shorts heavily crowded: {ann_fund:.1%} annual funding")
        if (asset.open_interest_usd or 0) > 0:
            reasons.append(f"OI: ${asset.open_interest_usd/1e6:.0f}M — elevated positioning")
        if asset.squeeze_candidate:
            reasons.append("Short squeeze candidate: crowded short + upward momentum")
        og = asset.distance_mark_oracle_pct or 0
        if abs(og) > 0.5:
            reasons.append(f"Oracle dislocation: {og:+.2f}%")

    elif setup == "exhaustion":
        if m24h > 3:
            reasons.append(f"Extended 24h run-up: +{m24h:.2f}% — momentum at risk of stalling")
        if ann_fund > 0.15:
            reasons.append(f"Longs paying {ann_fund:.1%} annual — crowded after the move")
        fi = asset.recent_trade_imbalance or 0
        if fi < 0.1:
            reasons.append(f"Buy flow fading: only {max(fi, 0):.0%} net buyers remaining")
        if m1h < 0:
            reasons.append(f"1h momentum turning negative ({m1h:+.2f}%) despite extended 24h")
        if (asset.crowding_score or 0) > 55:
            reasons.append(f"Crowding score elevated ({asset.crowding_score:.0f}/100)")
        bi = asset.orderbook_imbalance or 0
        if bi < 0:
            reasons.append(f"Book skewing ask-heavy ({bi:+.2f}) — sellers emerging")

    elif setup == "collapse_risk":
        if m24h > 2:
            reasons.append(f"Asset ran +{m24h:.2f}% before stalling — distribution phase")
        if m1h < -0.3:
            reasons.append(f"1h momentum turning sharply negative: {m1h:+.2f}%")
        oi5m = asset.oi_change_5m or 0
        if oi5m < -0.01:
            reasons.append(f"OI dropping {oi5m:.2%} in 5m — active unwinding")
        bi = asset.orderbook_imbalance or 0
        if bi < -0.2:
            reasons.append(f"Book overwhelmed by asks: {bi:+.2f} imbalance")
        fi = asset.recent_trade_imbalance or 0
        if fi < -0.2:
            reasons.append(f"Aggressive sell flow: {fi:.0%} net sellers")
        if ann_fund > 0.20:
            reasons.append(f"Longs still paying {ann_fund:.1%} annual — exits will amplify drop")

    if asset.pct_change_24h is not None:
        reasons.append(f"24h change: {asset.pct_change_24h:+.2f}%")

    # Prepend structural quality context as the first reason for long setups
    regime = asset.asset_regime or "chop_low_quality"
    sq     = asset.structural_quality_score or 50
    structural_leads = []
    if regime == "structural_uptrend_pullback":
        structural_leads.append(
            f"Multi-day uptrend intact (structural quality {sq:.0f}/100) — "
            f"pullback offers better risk/reward than a bounce from downtrend"
        )
    elif regime == "structural_uptrend_breakout_watch":
        structural_leads.append(
            f"Tightening base on top of an uptrend (structural quality {sq:.0f}/100) — "
            f"coil before expansion rather than exhaustion top"
        )
    elif regime in ("downtrend_dead_cat", "speculative_reversal"):
        structural_leads.append(
            f"⚠ Structural quality low ({sq:.0f}/100) — "
            f"regime={regime.replace('_', ' ')}; short-term bounce only"
        )
    elif regime == "chop_low_quality":
        structural_leads.append(
            f"Choppy structure (structural quality {sq:.0f}/100) — "
            f"no clear trend bias; sizing down recommended"
        )

    return (structural_leads + reasons)[:6]


def _build_what_to_watch(asset: ScreenerAsset, setup: str, side: str) -> list[str]:
    """Generate setup-specific monitoring triggers for the trader."""
    watch: list[str] = []
    ann_fund = (asset.funding or 0) * 8760
    m24h = asset.momentum_24h or 0

    if setup == "breakout":
        if side == "long":
            watch.append("Volume sustaining above 1.3× rolling average on green candles")
            watch.append("Buy flow maintaining positive bias (trade imbalance > +0.15)")
            watch.append("Oracle price following mark higher (gap staying contained)")
        else:
            watch.append("Sell volume sustaining above average on red candles")
            watch.append("Bids not rebuilding — book stays ask-heavy")
        watch.append(f"Funding remaining below 15% annual (currently {ann_fund:.1%})")

    elif setup == "mean_reversion":
        watch.append("Funding rate normalizing toward 0 (current excess unwinding)")
        og = asset.distance_mark_oracle_pct or 0
        if abs(og) > 0.2:
            watch.append(f"Mark/oracle gap closing from {og:+.2f}% toward 0")
        watch.append("OI declining as trapped positions exit")
        watch.append("Price returning toward previous session levels")

    elif setup == "trend_continuation":
        if side == "long":
            watch.append("Flow remaining net positive on any pullback")
            watch.append("Bid support rebuilding on dips (book imbalance > 0)")
            watch.append("No abnormal OI build-up (crowding starting to show)")
        else:
            watch.append("Sell flow maintaining dominance on rallies")
            watch.append("Funding staying negative or near 0 (shorts not getting squeezed)")

    elif setup == "crowding_unwind":
        if ann_fund > 0:
            watch.append(f"Funding trending toward 0 from current {ann_fund:.1%} annual")
            watch.append("OI starting to decline as longs begin to exit")
            watch.append("Price breaking below recent support (triggering long liquidations)")
        else:
            watch.append(f"Funding spiking upward from current {ann_fund:.1%} annual")
            watch.append("OI declining as shorts cover (squeeze accelerating)")
            watch.append("Price holding above key breakout level")

    elif setup == "exhaustion":
        watch.append("Flow flipping to net selling (imbalance < 0) — confirms the top")
        watch.append("Volume spike WITHOUT further price gain — classic exhaustion signal")
        watch.append("Order book asks building, bids thinning below current price")
        watch.append(f"Funding starting to fall from elevated {ann_fund:.1%} annual")
        if m24h > 3:
            watch.append(f"1h close failing to exceed recent highs after +{m24h:.1f}% run")

    elif setup == "collapse_risk":
        watch.append("Aggressive sell prints in tape (large notional market sells)")
        watch.append("OI dropping faster than price — forced liquidations beginning")
        watch.append("Book bid depth evaporating below current price")
        watch.append("Funding normalizing sharply — longs exiting, carry trade unwinding")
        watch.append("Momentum spreading to 15m / 5m timeframes (acceleration phase)")

    elif setup == "avoid":
        watch.append("Spread narrowing below 10bps before re-evaluating entry")
        watch.append("Volume picking up (>$10M 24h notional) to support institutional participation")

    return watch[:5]


def _build_risk_flags(asset: ScreenerAsset) -> list[str]:
    flags: list[str] = []
    ann_fund = (asset.funding or 0) * 8760
    if (asset.spread_bps or 0) > 15:
        flags.append(f"Wide spread: {asset.spread_bps:.1f}bps — slippage risk")
    if (asset.liquidity_score or 0) < 30:
        flags.append("Low liquidity — large size will move market")
    if (asset.realized_volatility_short or 0) > 200:
        flags.append(f"High vol: {asset.realized_volatility_short:.0f}% ann — wide stops needed")
    if (asset.tradability_penalty or 0) > 50:
        flags.append("Thin orderbook — limit orders preferred")
    if asset.crowded_long and ann_fund > 0:
        flags.append(f"Crowded long: {ann_fund:.1%} ann funding cost against long")
    elif asset.crowded_short and ann_fund < 0:
        flags.append(f"Crowded short: squeeze risk if price moves against shorts")
    if abs(asset.distance_mark_oracle_pct or 0) > 1.0:
        flags.append(f"Large mark/oracle gap: {asset.distance_mark_oracle_pct:+.2f}% — may snap")
    if (asset.oi_change_5m or 0) < -0.02:
        flags.append(f"OI dropping 5m: {(asset.oi_change_5m or 0):.2%} — positioning unwinding")
    return flags[:5]


def _build_invalidation(asset: ScreenerAsset, setup: str, side: str) -> list[str]:
    notes: list[str] = []
    mark     = asset.mark_px or 0
    ann_fund = (asset.funding or 0) * 8760
    m24h     = asset.momentum_24h or 0

    if setup == "breakout":
        if side == "long":
            lvl = mark * 0.99
            notes.append(f"Invalidated if price drops back below ~${lvl:.4g} (−1%)")
            notes.append("Invalidated if buy flow reverses to net selling")
        else:
            lvl = mark * 1.01
            notes.append(f"Invalidated if price recovers above ~${lvl:.4g} (+1%)")

    elif setup == "mean_reversion":
        if ann_fund > 0:
            notes.append("Invalidated if funding stays elevated without price reversal for 2+ hours")
        notes.append("Invalidated if oracle gap widens further instead of closing")
        if m24h:
            notes.append(f"Invalidated if 24h trend accelerates past 2× current ({m24h:+.2f}%)")

    elif setup == "trend_continuation":
        if side == "long":
            lvl = mark * 0.98
            notes.append(f"Invalidated if 1h candle closes below ~${lvl:.4g} (−2%)")
        else:
            lvl = mark * 1.02
            notes.append(f"Invalidated if price recovers above ~${lvl:.4g} (+2%)")
        notes.append("Invalidated if flow imbalance flips to opposing direction")

    elif setup == "crowding_unwind":
        if ann_fund > 0:
            notes.append("Invalidated if funding normalizes below +20% annual before price falls")
        notes.append("Invalidated if OI continues building (crowd deepening, not exiting)")
        notes.append("Invalidated if price makes new highs — trend may dominate crowding")

    elif setup in ("exhaustion", "collapse_risk"):
        lvl_up = mark * 1.02
        notes.append(f"Invalidated if price reclaims ${lvl_up:.4g} (+2%) with strong volume")
        notes.append("Invalidated if buy flow returns to dominant positive")
        if setup == "collapse_risk":
            notes.append("Invalidated if OI stabilizes or increases (fresh buyers absorbing sellers)")

    if not notes:
        notes.append("Invalidated if market structure changes significantly")

    return notes[:4]


def _build_structural_context(asset: ScreenerAsset) -> str:
    """
    One-sentence higher-timeframe structural summary explaining WHY this is
    a better pick than a weak bounce candidate.  Used in every idea object.
    """
    regime = asset.asset_regime or "chop_low_quality"
    sq     = asset.structural_quality_score or 50
    coin   = asset.coin
    m24h   = asset.momentum_24h or 0
    cont   = asset.continuation_score or 50
    br     = asset.breakout_readiness_score or 50

    if regime == "structural_uptrend_pullback":
        q = "strong" if sq >= 65 else "healthy"
        return (
            f"{coin} is in a {q} multi-day uptrend (structural quality {sq:.0f}/100) "
            f"with a constructive pullback — the trend structure is intact and this is a "
            f"buyable dip rather than a dead-cat bounce from a long-term downtrend."
        )
    elif regime == "structural_uptrend_breakout_watch":
        return (
            f"{coin} has been consolidating in a tightening base (breakout readiness "
            f"{br:.0f}/100) on top of a multi-day uptrend — a breakout from this coil "
            f"has directional follow-through potential vs random meme bounces."
        )
    elif regime == "late_extension_exhaustion":
        return (
            f"{coin} is extended after a strong run — the uptrend structure is aging and "
            f"momentum internals are fading; risk/reward favors watching for a reversal "
            f"rather than chasing continuation."
        )
    elif regime == "downtrend_dead_cat":
        return (
            f"{coin} is bouncing sharply inside a confirmed multi-day downtrend — this is "
            f"a classic dead-cat bounce pattern with no structural base; structural quality "
            f"score {sq:.0f}/100 makes it unsuitable as a top long."
        )
    elif regime == "speculative_reversal":
        return (
            f"{coin} shows short-term reversal signals but lacks structural quality "
            f"({sq:.0f}/100) for a sustained trend — suitable only as a speculative "
            f"bounce trade with tight risk, not a core long."
        )
    elif regime == "collapse_risk":
        return (
            f"{coin} is in active deterioration: collapse risk is elevated and the trend "
            f"structure is breaking down — not a buy candidate."
        )
    else:
        return (
            f"{coin} is in a choppy / low-quality structural state ({sq:.0f}/100) — "
            f"no clear trend bias; wait for a better setup before committing."
        )


def _build_thesis(asset: ScreenerAsset, setup: str, side: str) -> tuple[str, str]:
    coin     = asset.coin
    ann_fund = (asset.funding or 0) * 8760
    m1h      = asset.momentum_1h   or 0
    m4h      = asset.momentum_4h   or 0
    m24h     = asset.momentum_24h  or 0
    liq      = asset.liquidity_score or 0

    titles = {
        "breakout": {
            "long":          f"{coin} Breakout Long",
            "short":         f"{coin} Breakdown Short",
            "neutral_watch": f"{coin} Directional Breakout",
        },
        "mean_reversion": {
            "long":          f"{coin} Mean Reversion — Long Dip",
            "short":         f"{coin} Mean Reversion — Fade Rip",
            "neutral_watch": f"{coin} Mean Reversion Setup",
        },
        "trend_continuation": {
            "long":          f"{coin} Trend Continuation — Long",
            "short":         f"{coin} Trend Continuation — Short",
            "neutral_watch": f"{coin} Trend Continuation Watch",
        },
        "crowding_unwind": {
            "long":          f"{coin} Short Squeeze Setup",
            "short":         f"{coin} Long Unwind — Fade Crowded Longs",
            "neutral_watch": f"{coin} Crowding Unwind Watch",
        },
        "exhaustion": {
            "short":         f"{coin} Exhaustion — Watch for Top",
            "long":          f"{coin} Crash Exhaustion — Watch for Bounce",
            "neutral_watch": f"{coin} Momentum Exhaustion",
        },
        "collapse_risk": {
            "short":         f"{coin} Collapse Risk — Short Candidate",
            "long":          f"{coin} Collapse — Potential Bounce Setup",
            "neutral_watch": f"{coin} Collapse Risk",
        },
        "avoid": {
            "neutral_watch": f"{coin} — Avoid (Low Quality Setup)",
            "long":          f"{coin} — Low-Confidence Long",
            "short":         f"{coin} — Low-Confidence Short",
        },
    }
    title = titles.get(setup, {}).get(side, f"{coin} Signal")

    if setup == "breakout":
        dir_str = "upside" if side == "long" else "downside"
        summary = (
            f"{coin} is showing {dir_str} momentum with 1h: {m1h:+.2f}%, 4h: {m4h:+.2f}%. "
        )
        fi = asset.recent_trade_imbalance or 0
        if abs(fi) > 0.2:
            summary += f"Trade flow ({fi:+.0%}) supports the move. "
        if (asset.volume_impulse or 0) > 1.3:
            summary += f"Volume at {asset.volume_impulse:.1f}× above average confirms acceleration."
        else:
            summary += f"Liquidity score {liq:.0f}/100 supports participation."

    elif setup == "mean_reversion":
        og = asset.distance_mark_oracle_pct or 0
        summary = (
            f"{coin} is stretched vs fair value — mark/oracle gap: {og:+.2f}%. "
            f"Funding at {ann_fund:+.1%} annual. "
        )
        if ann_fund > 0.20:
            summary += "Longs paying unsustainable carry — setup favors mean-reversion short."
        elif ann_fund < -0.20:
            summary += "Shorts paying unsustainable carry — setup favors mean-reversion long."
        else:
            summary += "Price dislocated from anchor levels — reversion to mean likely."

    elif setup == "trend_continuation":
        dir_str = "bullish" if side == "long" else "bearish"
        summary = (
            f"{coin} is in a confirmed {dir_str} trend across 1h/4h/24h timeframes. "
            f"24h move: {m24h:+.2f}%. Liquidity score {liq:.0f}/100 supports continued participation. "
        )
        fi = asset.recent_trade_imbalance or 0
        if abs(fi) > 0.15:
            summary += f"Flow ({fi:+.0%}) is sustaining the trend."

    elif setup == "crowding_unwind":
        if ann_fund > 0:
            summary = (
                f"{coin} longs are crowded — paying {ann_fund:.1%} annual funding. "
                f"OI: ${(asset.open_interest_usd or 0)/1e6:.0f}M. "
                f"With {m1h:+.2f}% 1h momentum turning, unwind pressure may build."
            )
        else:
            summary = (
                f"{coin} shorts are crowded — paying {abs(ann_fund):.1%} annual. "
                f"OI: ${(asset.open_interest_usd or 0)/1e6:.0f}M. "
                f"Price leadership ({m1h:+.2f}% 1h) could trigger a short squeeze."
            )

    elif setup == "exhaustion":
        summary = (
            f"{coin} has posted a strong {m24h:+.2f}% 24h move, but momentum shows early "
            f"signs of stalling. Flow is fading, funding at {ann_fund:.1%} annual means longs "
            f"are paying significant carry. This is a watch-for-top setup, not a fresh long."
        )

    elif setup == "collapse_risk":
        summary = (
            f"{coin} shows active breakdown signals: 1h momentum is {m1h:+.2f}%, "
            f"the book is ask-heavy, and flow is turning negative after a {m24h:+.2f}% 24h run. "
            f"Longs at {ann_fund:.1%} annual funding may accelerate exits if price slips further."
        )

    else:
        summary = f"{coin} setup score {asset.overall_score or 0:.0f}/100. Monitor for signal improvement."

    return title, summary


# ─────────────────────────────────────────────────────────────────────────────
# Core idea builder
# ─────────────────────────────────────────────────────────────────────────────

def _build_idea(asset: ScreenerAsset) -> dict:
    """Build a fully-formed idea object (dict) for a single asset."""
    setup      = asset.setup_type or "breakout"
    side       = _derive_side(asset, setup)
    confidence = _derive_confidence(asset, setup)
    reasons    = _build_reasons(asset, setup)
    what_watch = _build_what_to_watch(asset, setup, side)
    risk_flags = _build_risk_flags(asset)
    inval      = _build_invalidation(asset, setup, side)
    title, summary = _build_thesis(asset, setup, side)
    struct_ctx = _build_structural_context(asset)

    ann_fund = (asset.funding or 0) * 8760
    bucket   = _classify_bucket(asset, side)

    return {
        "coin":               asset.coin,
        "displayName":        asset.display_name,
        "side":               side,
        "setup_type":         setup,
        "asset_regime":       asset.asset_regime or "chop_low_quality",
        "score":              round(asset.overall_score or 0, 1),
        "confidence":         round(confidence, 2),
        "thesis_title":       title,
        "thesis_summary":     summary,
        "structural_context": struct_ctx,
        "reasons":            reasons,
        "what_to_watch":      what_watch,
        "invalidation_notes": inval,
        "risk_flags":         risk_flags,
        "metrics": {
            "mark_px":                   asset.mark_px,
            "pct_change_24h":            round(asset.pct_change_24h / 100, 6) if asset.pct_change_24h is not None else None,
            "funding":                   asset.funding,
            "funding_ann_pct":           round(ann_fund * 100, 3),
            "open_interest":             asset.open_interest_usd,
            "day_ntl_vlm":               asset.day_ntl_vlm,
            "premium":                   asset.premium,
            "mark_oracle_gap_pct":       asset.distance_mark_oracle_pct,
            "trade_flow_bias":           asset.recent_trade_imbalance,
            "book_imbalance":            asset.orderbook_imbalance,
            "volatility_score":          round((asset.volatility_score or 50) / 100, 3),
            "liquidity_score":           round((asset.liquidity_score  or 50) / 100, 3),
            "structural_quality_score":  round((asset.structural_quality_score or 50) / 100, 3),
            "liquidity_quality_score":   round((asset.liquidity_quality_score  or 50) / 100, 3),
            "continuation_score":        round((asset.continuation_score or 50) / 100, 3),
            "oi_change_5m":              asset.oi_change_5m,
            "oi_change_15m":             asset.oi_change_15m,
            "oi_change_1h":              asset.oi_change_1h,
            "volume_impulse":            asset.volume_impulse,
        },
        "scores": {
            "overall":                  round((asset.overall_score or 0) / 100, 3),
            "structural_quality":       round((asset.structural_quality_score or 50) / 100, 3),
            "continuation":             round((asset.continuation_score or 50) / 100, 3),
            "pullback_quality":         round((asset.pullback_quality_score or 0) / 100, 3),
            "breakout_readiness":       round((asset.breakout_readiness_score or 0) / 100, 3),
            "speculative_reversal":     round((asset.speculative_reversal_score or 0) / 100, 3),
            "liquidity_quality":        round((asset.liquidity_quality_score or 50) / 100, 3),
            "breakout":                 round((asset.breakout_score or 0) / 100, 3),
            "mean_reversion":           round((asset.mean_reversion_score or 0) / 100, 3),
            "trend_continuation":       round((asset.trend_continuation_score or 0) / 100, 3),
            "crowding_unwind":          round((asset.crowding_unwind_score or 0) / 100, 3),
            "exhaustion":               round((asset.exhaustion_score or 0) / 100, 3),
            "collapse_risk":            round((asset.collapse_risk_score or 0) / 100, 3),
            "avoid":                    round((asset.avoid_score or 0) / 100, 3),
            "momentum":                 round((asset.momentum_score or 50) / 100, 3),
            "flow":                     round((asset.flow_score or 50) / 100, 3),
            "trend":                    round((asset.trend_score or 50) / 100, 3),
            "liquidity":                round((asset.liquidity_score or 50) / 100, 3),
            "crowding":                 round((asset.crowding_score or 0) / 100, 3),
            "dislocation":              round((asset.dislocation_score or 0) / 100, 3),
        },
        "bucket": bucket,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Agent Briefing (primary output)
# ─────────────────────────────────────────────────────────────────────────────

def generate_agent_briefing(
    state: HyperliquidState,
    max_ideas: int = 20,
    min_volume_usd: float = 5_000_000,
) -> dict:
    """
    Generate the full Agent Market Brief payload (scoring pipeline v3).

    Hierarchical pipeline:
      1. structural_quality_score + asset_regime computed per asset
      2. Bucket classification: buy_now > high_quality_watchlist >
         speculative_reversals > collapse_watch > avoid
      3. Guardrails: best_long must be from buy_now/high_quality_watchlist
         with structural_quality_score >= 42 and not dead_cat/speculative_reversal

    Shape:
      market_regime, updated_at, best_long, best_short,
      best_breakout_watch, best_exhaustion_watch,
      actionable_ideas[], guidance{buy_now, high_quality_watchlist,
        speculative_reversals, collapse_watch, avoid},
      selected_thesis
    """
    perps = [
        a for a in state.perp_assets()
        if a.market_status == "active"
        and (a.day_ntl_vlm or 0) >= min_volume_usd
        and a.overall_score is not None
        and (not state.universe_allowlist or state.in_universe(a.coin))
    ]

    # Sort first by structural_quality_score (hierarchical), then overall_score
    # This ensures we evaluate structurally sound assets before weak bounces
    perps.sort(key=lambda a: (
        -(a.structural_quality_score or 50),
        -(a.overall_score or 0)
    ))
    candidates = perps[:max_ideas * 3]   # wider pool to fill all 5 buckets

    # ── Build all ideas ───────────────────────────────────────────────────
    ideas = [_build_idea(a) for a in candidates]

    # ── Split into new 5-bucket guidance structure ─────────────────────────
    buy_now               = [i for i in ideas if i["bucket"] == "buy_now"]
    high_quality_watchlist = [i for i in ideas if i["bucket"] == "high_quality_watchlist"]
    speculative_reversals  = [i for i in ideas if i["bucket"] == "speculative_reversals"]
    collapse_watch         = [i for i in ideas if i["bucket"] == "collapse_watch"]
    avoid                  = [i for i in ideas if i["bucket"] == "avoid"]

    # Each bucket sorted: buy_now/HQW by structural_quality first, then score
    def _quality_sort(idea_list: list) -> list:
        return sorted(idea_list, key=lambda i: (
            -i["scores"].get("structural_quality", 0.5),
            -i["score"]
        ))

    buy_now               = _quality_sort(buy_now)
    high_quality_watchlist = _quality_sort(high_quality_watchlist)
    speculative_reversals  = sorted(speculative_reversals, key=lambda i: -i["score"])
    collapse_watch         = sorted(collapse_watch,         key=lambda i: -(
        max(i["scores"].get("exhaustion", 0), i["scores"].get("collapse_risk", 0))
    ))
    avoid                  = sorted(avoid,  key=lambda i: -i["score"])

    # If buy_now is empty, promote top high_quality_watchlist items
    if not buy_now and high_quality_watchlist:
        buy_now                = high_quality_watchlist[:3]
        high_quality_watchlist = high_quality_watchlist[3:]

    # ── Regime ───────────────────────────────────────────────────────────
    regime_info = _compute_market_regime(perps)

    # ── Select top ideas for hero positions ───────────────────────────────
    # GUARDRAIL: best_long must come from buy_now or high_quality_watchlist only
    # It must NOT be from speculative_reversals — those are clearly separated
    LONG_QUAL_THRESHOLD = 42   # structural_quality_score minimum for hero long
    DEAD_CAT_REGIMES = {"downtrend_dead_cat", "speculative_reversal"}

    def _is_quality_long(idea: dict) -> bool:
        return (
            idea["side"] == "long"
            and idea["asset_regime"] not in DEAD_CAT_REGIMES
            and idea["scores"].get("structural_quality", 0.5) >= LONG_QUAL_THRESHOLD / 100
        )

    qualified_pool = buy_now + high_quality_watchlist
    quality_longs  = [i for i in qualified_pool if _is_quality_long(i)]
    quality_shorts = [i for i in qualified_pool if i["side"] == "short"]

    best_long  = quality_longs[0]  if quality_longs  else (
        # Final fallback: any long not in dead-cat (still not speculative_reversals)
        next((i for i in ideas
              if i["side"] == "long" and i["asset_regime"] not in DEAD_CAT_REGIMES), None)
    )
    best_short = quality_shorts[0] if quality_shorts else next(
        (i for i in ideas if i["side"] == "short"), None
    )

    # best_breakout_watch: highest breakout_readiness in high_quality_watchlist
    hqw_breakout = sorted(
        high_quality_watchlist,
        key=lambda i: -i["scores"].get("breakout_readiness", 0)
    )
    best_breakout_watch = hqw_breakout[0] if hqw_breakout else None

    # best_exhaustion_watch: highest exhaustion/collapse from collapse_watch
    best_exhaustion_watch = collapse_watch[0] if collapse_watch else None

    # selected_thesis: best_long from quality pool first
    selected_thesis = best_long or best_short or (buy_now[0] if buy_now else None)

    # ── Top actionable ideas (quality-first ordering, capped) ─────────────
    # Show buy_now + high_quality_watchlist first, then speculative_reversals
    quality_first = buy_now + high_quality_watchlist
    actionable_ideas = (quality_first + speculative_reversals)[:max_ideas]

    return {
        "market_regime":        regime_info["regime"],
        "regime_description":   regime_info["description"],
        "regime_metrics":       regime_info["metrics"],
        "updated_at":           _iso_now(),
        "score_version":        "3.0",
        "best_long":            best_long,
        "best_short":           best_short,
        "best_breakout_watch":  best_breakout_watch,
        "best_exhaustion_watch": best_exhaustion_watch,
        "actionable_ideas":     actionable_ideas,
        "guidance": {
            # New bucket names (v3)
            "buy_now":                buy_now[:8],
            "high_quality_watchlist": high_quality_watchlist[:8],
            "speculative_reversals":  speculative_reversals[:6],
            "collapse_watch":         collapse_watch[:8],
            "avoid":                  avoid[:6],
            # Legacy aliases for backward compat
            "trade_now":      buy_now[:8],
            "watch_breakout": high_quality_watchlist[:8],
            "watch_collapse": collapse_watch[:8],
        },
        "selected_thesis":      selected_thesis,
        # backward compat for frontend code expecting heroAgentSignals
        "heroAgentSignals":     actionable_ideas,
        "count":                len(actionable_ideas),
    }


def _iso_now() -> str:
    import datetime
    return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


# ─────────────────────────────────────────────────────────────────────────────
# Legacy hero signal list (used by /sections endpoint)
# ─────────────────────────────────────────────────────────────────────────────

def generate_hero_signals(
    state: HyperliquidState,
    top_n: int = 5,
    min_volume_usd: float = 10_000_000,
) -> list[HeroSignal]:
    """
    Legacy HeroSignal objects for the /sections endpoint.
    Filters to non-avoid, non-exhaustion assets only.
    """
    assets = state.perp_assets()
    assets = [
        a for a in assets
        if a.market_status == "active"
        and (a.day_ntl_vlm or 0) >= min_volume_usd
        and a.overall_score is not None
        and a.setup_type not in ("avoid", "exhaustion", "collapse_risk")
        and (not state.universe_allowlist or state.in_universe(a.coin))
    ]
    assets.sort(key=lambda a: -(a.overall_score or 0))
    top = assets[:top_n]
    return [_to_hero_signal(a) for a in top]


def _to_hero_signal(asset: ScreenerAsset) -> HeroSignal:
    setup = asset.setup_type or "breakout"
    side  = _derive_side(asset, setup)
    ann_f = (asset.funding or 0) * 8760

    metrics = HeroSignalMetrics(
        mark_px           = asset.mark_px,
        pct_change_24h    = round(asset.pct_change_24h / 100, 6) if asset.pct_change_24h is not None else None,
        funding           = asset.funding,
        funding_ann_pct   = round(ann_f * 100, 3),
        open_interest     = asset.open_interest_usd,
        day_ntl_vlm       = asset.day_ntl_vlm,
        premium           = asset.premium,
        mark_oracle_gap_pct = asset.distance_mark_oracle_pct,
        mark_mid_gap_pct  = asset.distance_mark_mid_pct,
        trade_flow_bias   = asset.recent_trade_imbalance,
        book_imbalance    = asset.orderbook_imbalance,
        volatility_score  = round((asset.volatility_score or 50) / 100, 3),
        liquidity_score   = round((asset.liquidity_score  or 50) / 100, 3),
        oi_change_5m      = asset.oi_change_5m,
        oi_change_15m     = asset.oi_change_15m,
        oi_change_1h      = asset.oi_change_1h,
        volume_impulse    = asset.volume_impulse,
    )

    title, summary = _build_thesis(asset, setup, side)
    return HeroSignal(
        coin               = asset.coin,
        side               = side,
        overall_score      = round(asset.overall_score or 0, 1),
        setup_type         = setup,
        confidence         = round(_derive_confidence(asset, setup), 2),
        thesis_title       = title,
        thesis_summary     = summary,
        reasons            = _build_reasons(asset, setup),
        risk_flags         = _build_risk_flags(asset),
        invalidation_notes = _build_invalidation(asset, setup, side),
        metrics            = metrics,
        score_components   = {
            "momentum":          round((asset.momentum_score or 50) / 100, 3),
            "flow":              round((asset.flow_score or 50) / 100, 3),
            "trend":             round((asset.trend_score or 50) / 100, 3),
            "book_pressure":     round((asset.book_pressure_score or 50) / 100, 3),
            "crowding":          round((asset.crowding_score or 0) / 100, 3),
            "dislocation":       round((asset.dislocation_score or 0) / 100, 3),
            "liquidity":         round((asset.liquidity_score or 50) / 100, 3),
            "volatility":        round((asset.volatility_score or 50) / 100, 3),
            "tradability_penalty": round((asset.tradability_penalty or 0) / 100, 3),
            "breakout":          round((asset.breakout_score or 50) / 100, 3),
            "mean_reversion":    round((asset.mean_reversion_score or 50) / 100, 3),
            "trend_continuation": round((asset.trend_continuation_score or 50) / 100, 3),
            "crowding_unwind":   round((asset.crowding_unwind_score or 0) / 100, 3),
            "exhaustion":        round((asset.exhaustion_score or 0) / 100, 3),
            "collapse_risk":     round((asset.collapse_risk_score or 0) / 100, 3),
            "avoid":             round((asset.avoid_score or 0) / 100, 3),
        },
        generated_at = time.time(),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Section builder (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def build_signal_sections(
    state: HyperliquidState,
    rows_per_section: int = 6,
) -> dict:
    perps = [
        a for a in state.perp_assets()
        if a.market_status == "active"
        and (not state.universe_allowlist or state.in_universe(a.coin))
    ]
    spots = [
        a for a in state.spot_assets()
        if a.market_status == "active"
        and (not state.universe_allowlist or state.in_universe(a.coin))
    ]
    all_active = perps + spots

    def _row(a: ScreenerAsset) -> dict:
        ann_fund = (a.funding or 0) * 8760
        return {
            "coin":            a.coin,
            "displayName":     a.display_name,
            "marketType":      a.market_type,
            "markPrice":       a.mark_px,
            "change24hPct":    round(a.pct_change_24h / 100, 6) if a.pct_change_24h is not None else None,
            "funding":         a.funding,
            "fundingAnnPct":   round(ann_fund * 100, 3),
            "openInterest":    a.open_interest_usd,
            "volume24h":       a.day_ntl_vlm,
            "compositeSignal": round((a.composite_signal_score or 50) / 100, 4),
            "overallScore":    round((a.overall_score or 50) / 100, 4),
            "setupType":       a.setup_type,
            "signalDirection": a.signal_direction,
            "markOracleGapPct": a.distance_mark_oracle_pct,
            "premium":         a.premium,
            "tradeImbalance":  a.recent_trade_imbalance,
            "bookImbalance":   a.orderbook_imbalance,
            "volatilityScore": round((a.volatility_score or 50) / 100, 3),
            "liquidityScore":  round((a.liquidity_score or 50) / 100, 3),
            "spreadBps":       a.spread_bps,
            "oiChange5m":      a.oi_change_5m,
            "oiChange15m":     a.oi_change_15m,
            "oiChange1h":      a.oi_change_1h,
            "volumeImpulse":   a.volume_impulse,
            "volumeImpulse5m": a.volume_impulse_5m,
            "exhaustionScore": round((a.exhaustion_score or 0) / 100, 3),
            "collapseRiskScore": round((a.collapse_risk_score or 0) / 100, 3),
            "crowdedLong":     a.crowded_long,
            "crowdedShort":    a.crowded_short,
            "squeezeCand":     a.squeeze_candidate,
        }

    def _sec(title, subtitle, assets_sorted, n=rows_per_section):
        return {
            "title":     title,
            "subtitle":  subtitle,
            "rows":      [_row(a) for a in assets_sorted[:n]],
            "available": True,
        }

    oi_history_ready = sum(1 for c in state.oi_history if len(state.oi_history[c]) >= 5) >= 10
    sections: dict = {}

    # Perps-only for gainers/losers — removes spot noise from signal board
    sections["top_gainers"]   = _sec("Top Gainers", "Strongest 24h perp price movers",
        sorted(perps, key=lambda a: -(a.pct_change_24h or 0)))
    sections["top_losers"]    = _sec("Top Losers", "Sharpest 24h perp declines",
        sorted(perps, key=lambda a: (a.pct_change_24h or 0)))
    sections["high_funding"]  = _sec("High Funding", "Longs paying — squeeze watch",
        sorted(perps, key=lambda a: -((a.funding or 0))))
    sections["negative_funding"] = _sec("Negative Funding", "Shorts paying — flush watch",
        sorted(perps, key=lambda a: (a.funding or 0)))
    sections["mark_oracle_gap"] = _sec("Mark/Oracle Gap", "Largest mark vs oracle delta",
        sorted(perps, key=lambda a: -abs(a.distance_mark_oracle_pct or 0)))
    sections["premium_discount"] = _sec("Premium/Discount", "Mark vs mid price dislocation",
        sorted(perps, key=lambda a: -abs(a.distance_mark_mid_pct or 0)))
    sections["volume_leaders"] = _sec("Volume Leaders", "Largest 24h notional volume (perps)",
        sorted(perps, key=lambda a: -(a.day_ntl_vlm or 0)))
    sections["trade_flow"]    = _sec("Trade Flow", "Buy vs sell trade pressure",
        sorted([a for a in perps if a.recent_trade_imbalance is not None],
               key=lambda a: -abs(a.recent_trade_imbalance or 0)))
    sections["book_imbalance"] = _sec("Book Imbalance", "Order book bid/ask skew",
        sorted([a for a in perps if a.orderbook_imbalance is not None],
               key=lambda a: -abs(a.orderbook_imbalance or 0)))
    sections["volatility_leaders"] = _sec("Volatility Leaders", "Highest realized vol",
        sorted(perps, key=lambda a: -(a.volatility_score or 0)))
    sections["oi_leaders"] = _sec("OI Leaders", "Largest open interest by USD",
        sorted(perps, key=lambda a: -(a.open_interest_usd or 0)))
    sections["breakout_watch"] = _sec("Breakout Watch", "High breakout probability",
        sorted(perps, key=lambda a: -(a.breakout_score or 0)))
    sections["mean_reversion"] = _sec("Mean Reversion", "Stretched vs mean — reversal",
        sorted(perps, key=lambda a: -(a.mean_reversion_score or 0)))
    sections["exhaustion_watch"] = _sec("Exhaustion Watch", "Extended moves — topping signs",
        sorted([a for a in perps if (a.exhaustion_score or 0) > 30],
               key=lambda a: -(a.exhaustion_score or 0)))
    sections["collapse_risk"]  = _sec("Collapse Risk", "Active breakdown candidates",
        sorted([a for a in perps if (a.collapse_risk_score or 0) > 25],
               key=lambda a: -(a.collapse_risk_score or 0)))
    sections["crowded_longs"]  = _sec("Crowded Longs", "High funding + bull OI",
        sorted([a for a in perps if a.crowded_long], key=lambda a: -((a.crowding_score or 0))))
    sections["crowded_shorts"] = _sec("Crowded Shorts", "Negative funding + bear OI",
        sorted([a for a in perps if a.crowded_short], key=lambda a: -((a.crowding_score or 0))))
    sections["short_squeeze"]  = _sec("Short Squeeze", "Crowded shorts + upward momentum",
        sorted([a for a in perps if a.squeeze_candidate], key=lambda a: -((a.crowding_unwind_score or 0))))
    sections["long_flush_watch"] = _sec("Long Flush Watch", "Crowded longs + momentum fading",
        sorted([a for a in perps if a.crowded_long and (a.momentum_1h or 0) < 0],
               key=lambda a: -((a.crowding_unwind_score or 0))))
    # Funding carry extremes: annualized rate > 20%
    sections["funding_extremes"] = _sec("Funding Extremes", "Annualized funding carry > 20%",
        sorted([a for a in perps if abs((a.funding or 0) * 8760) > 0.20],
               key=lambda a: -abs(a.funding or 0)))
    sections["illiquid_zone"]  = _sec("Illiquid Zone", "Low liquidity — dangerous OI",
        sorted([a for a in perps if a.illiquid_high_volatility or a.avoid_due_to_spread],
               key=lambda a: -(a.avoid_score or 0)))

    if oi_history_ready:
        oi_exp = [a for a in perps if (a.oi_change_5m or 0) > 0.01]
        oi_unw = [a for a in perps if (a.oi_change_5m or 0) < -0.01]
        sections["oi_expansion"] = _sec("OI Expansion", "Largest open interest build",
            sorted(oi_exp, key=lambda a: -(a.oi_change_5m or 0)))
        sections["oi_unwind"]    = _sec("OI Unwind", "Largest OI liquidation",
            sorted(oi_unw, key=lambda a: (a.oi_change_5m or 0)))
    else:
        sections["oi_expansion"] = {"title": "OI Expansion", "subtitle": "Building OI history...", "rows": [], "available": False}
        sections["oi_unwind"]    = {"title": "OI Unwind",    "subtitle": "Building OI history...", "rows": [], "available": False}

    vol_ready = sum(1 for a in perps if a.volume_impulse_5m is not None) >= 10
    if vol_ready:
        sections["volume_impulse_watch"] = _sec("Volume Impulse", "Highest volume acceleration",
            sorted(perps, key=lambda a: -(a.volume_impulse_5m or a.volume_impulse or 0)))
    else:
        sections["volume_impulse_watch"] = {
            "title": "Volume Impulse", "subtitle": "Collecting 5m candle data...",
            "rows": [], "available": False,
        }

    return sections


# ─────────────────────────────────────────────────────────────────────────────
# Summary cards (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def build_summary_cards(state: HyperliquidState) -> list[dict]:
    perps = [
        a for a in state.perp_assets()
        if a.market_status == "active"
        and a.pct_change_24h is not None
        and (not state.universe_allowlist or state.in_universe(a.coin))
    ]
    if not perps: return []

    top_gainer = max(perps, key=lambda a: a.pct_change_24h or 0)
    top_loser  = min(perps, key=lambda a: a.pct_change_24h or 0)

    oi_exp_assets = [a for a in perps if a.oi_change_5m is not None]
    oi_exp_coin = oi_exp_val = oi_unw_coin = oi_unw_val = None
    if oi_exp_assets:
        best  = max(oi_exp_assets, key=lambda a: a.oi_change_5m or 0)
        worst = min(oi_exp_assets, key=lambda a: a.oi_change_5m or 0)
        oi_exp_coin, oi_exp_val = best.coin, best.oi_change_5m
        if (worst.oi_change_5m or 0) < -0.005:
            oi_unw_coin, oi_unw_val = worst.coin, worst.oi_change_5m

    fund_sorted     = sorted(perps, key=lambda a: -(a.funding or 0))
    neg_fund_sorted = sorted(perps, key=lambda a: (a.funding or 0))
    high_fund = fund_sorted[0] if fund_sorted else None
    neg_fund  = neg_fund_sorted[0] if neg_fund_sorted else None

    gap_sorted = sorted(perps, key=lambda a: -abs(a.distance_mark_oracle_pct or 0))
    top_gap    = gap_sorted[0] if gap_sorted else None

    vol_sorted = sorted([a for a in perps if a.volume_impulse], key=lambda a: -(a.volume_impulse or 0))
    top_vol    = vol_sorted[0] if vol_sorted else None

    ex_sorted = sorted([a for a in perps if (a.exhaustion_score or 0) > 30],
                       key=lambda a: -(a.exhaustion_score or 0))
    top_ex = ex_sorted[0] if ex_sorted else None

    return [
        {"id": "top_gainer",     "label": "Top Gainer",   "coinRef": top_gainer.coin,
         "value": f"+{top_gainer.pct_change_24h:.2f}%",
         "subValue": f"${top_gainer.mark_px:.4g}" if top_gainer.mark_px else None},
        {"id": "top_loser",      "label": "Top Loser",    "coinRef": top_loser.coin,
         "value": f"{top_loser.pct_change_24h:.2f}%",
         "subValue": f"${top_loser.mark_px:.4g}" if top_loser.mark_px else None},
        {"id": "oi_expansion",   "label": "OI Expansion", "coinRef": oi_exp_coin,
         "value": f"{oi_exp_val:+.2%}" if oi_exp_val is not None else "—",
         "subValue": "5m change" if oi_exp_val else "Building history..."},
        {"id": "oi_unwind",      "label": "OI Unwind",    "coinRef": oi_unw_coin,
         "value": f"{oi_unw_val:.2%}" if oi_unw_val is not None else "—",
         "subValue": "5m change" if oi_unw_val else "Building history..."},
        {"id": "high_funding",   "label": "High Funding", "coinRef": high_fund.coin if high_fund else None,
         "value": f"{(high_fund.funding or 0):.4%}" if high_fund else "—",
         "subValue": f"${(high_fund.day_ntl_vlm or 0)/1e6:.1f}M vol" if high_fund else None},
        {"id": "neg_funding",    "label": "Neg Funding",  "coinRef": neg_fund.coin if neg_fund else None,
         "value": f"{(neg_fund.funding or 0):.4%}" if neg_fund else "—",
         "subValue": f"${(neg_fund.day_ntl_vlm or 0)/1e6:.1f}M vol" if neg_fund else None},
        {"id": "mark_oracle_gap","label": "Mk/Oracle Gap","coinRef": top_gap.coin if top_gap else None,
         "value": f"{top_gap.distance_mark_oracle_pct:+.4f}%" if top_gap else "—",
         "subValue": f"${top_gap.mark_px:.4g}" if (top_gap and top_gap.mark_px) else None},
        {"id": "vol_impulse",    "label": "Vol Impulse",  "coinRef": top_vol.coin if top_vol else None,
         "value": f"{top_vol.volume_impulse:.2f}×" if top_vol else "—",
         "subValue": f"${(top_vol.day_ntl_vlm or 0)/1e6:.0f}M" if top_vol else None},
        {"id": "exhaustion",     "label": "Exhaustion",   "coinRef": top_ex.coin if top_ex else None,
         "value": f"{(top_ex.exhaustion_score or 0):.0f}/100" if top_ex else "—",
         "subValue": f"{(top_ex.pct_change_24h or 0):+.1f}% 24h" if top_ex else None},
    ]
