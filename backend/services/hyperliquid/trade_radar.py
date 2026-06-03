"""
Trade Radar — high-level deterministic trade signal engine for Hyperliquid.

Sits on top of the existing feature_engine.py scores (momentum_score,
flow_score, crowding_score, structural_quality_score, etc.) without touching
raw data or recomputing any signals.

No LLM calls. No ML. 100% deterministic.

Entry point: build_trade_radar(state) → dict

Endpoint: GET /api/hyperliquid/screener/trade-radar
"""
from __future__ import annotations

import time
from typing import Any, Optional

from .models import ScreenerAsset
from .state import HyperliquidState


# ─── Constants ─────────────────────────────────────────────────────────────────

_MIN_VOLUME_USD       = 1_000_000   # $1M/day minimum to enter the radar universe
_TRADE_NOW_MIN_SCORE  = 70.0        # minimum score for TRADE_NOW classification
_TRADE_NOW_MIN_CONF   = 0.63        # minimum confidence for TRADE_NOW
_TOP_N                = 10          # entries in top_setups list


# ─── Component score builder ───────────────────────────────────────────────────

def _cs(score: float, label: str, explanation: str) -> dict:
    """Return a ComponentScore dict."""
    return {
        "score":       round(max(0.0, min(100.0, score)), 1),
        "label":       label,
        "explanation": explanation,
    }


# ─── Six component scorers ──────────────────────────────────────────────────────

def _momentum_component(a: ScreenerAsset, side: str) -> dict:
    mom  = a.momentum_score or 50.0
    m1h  = a.momentum_1h  or 0.0
    m4h  = a.momentum_4h  or 0.0
    m24h = a.momentum_24h or 0.0
    tf   = f"1h {m1h:+.2f}%, 4h {m4h:+.2f}%, 24h {m24h:+.2f}%"

    if side == "LONG":
        score = mom
        if score > 65:   return _cs(score, "BULLISH", f"Aligned multi-TF momentum: {tf}")
        elif score > 52: return _cs(score, "NEUTRAL",  f"Mild positive bias: {tf}")
        elif score > 40: return _cs(score, "NEUTRAL",  f"Momentum mixed/flat: {tf}")
        else:            return _cs(score, "BEARISH",  f"Momentum against long bias: {tf}")
    else:
        score = 100.0 - mom
        if score > 65:   return _cs(score, "BEARISH", f"Negative momentum: {tf}")
        elif score > 52: return _cs(score, "NEUTRAL",  f"Mild downside bias: {tf}")
        else:            return _cs(score, "BULLISH",  f"Momentum resisting short: {tf}")


def _positioning_component(a: ScreenerAsset, side: str) -> dict:
    ann_fund = (a.funding or 0.0) * 8760
    oi_1h    = a.oi_change_1h or 0.0
    oi_5m    = a.oi_change_5m or 0.0
    oi_usd   = (a.open_interest_usd or 0.0) / 1e6
    oi_str   = f"OI ${oi_usd:.1f}M, 1h Δ{oi_1h*100:+.2f}%"

    if side == "LONG":
        if a.crowded_long:
            return _cs(22.0, "CAUTION",
                       f"Longs crowded ({ann_fund:.1%} annual funding). {oi_str} — exits may be contested.")
        if oi_5m < -0.01:
            return _cs(28.0, "BEARISH",
                       f"OI dropping 5m ({oi_5m:.2%}) — active unwinding. {oi_str}")
        if oi_1h > 0.005:
            return _cs(74.0, "BULLISH",
                       f"OI building with price (+{oi_1h*100:.2f}% 1h). {oi_str}")
        return _cs(52.0, "NEUTRAL", oi_str)
    else:
        if a.crowded_long:
            return _cs(76.0, "BEARISH",
                       f"Longs crowded ({ann_fund:.1%} ann) — short fuel available. {oi_str}")
        if a.crowded_short:
            return _cs(22.0, "CAUTION",
                       f"Shorts crowded — squeeze risk ({ann_fund:.1%} ann). {oi_str}")
        if oi_5m < -0.01 or oi_1h < -0.005:
            return _cs(72.0, "BEARISH",
                       f"OI declining — longs unwinding. {oi_str}")
        return _cs(48.0, "NEUTRAL", oi_str)


def _funding_component(a: ScreenerAsset, side: str) -> dict:
    fund     = a.funding or 0.0
    ann_fund = fund * 8760
    fund_8h  = fund * 8 * 100
    fs       = f"8h {fund_8h:+.4f}%, ann {ann_fund:.1%}"

    if side == "LONG":
        if ann_fund > 0.50:
            return _cs(12.0, "RISK",    f"Extreme positive funding ({fs}) — carry cost unsustainable, crowded long.")
        elif ann_fund > 0.25:
            return _cs(30.0, "CAUTION", f"Elevated funding ({fs}) — longs paying high carry.")
        elif ann_fund > 0.10:
            return _cs(55.0, "NEUTRAL", f"Moderate positive funding ({fs}) — watch carry burden.")
        elif ann_fund < -0.20:
            return _cs(82.0, "BULLISH", f"Negative funding ({fs}) — shorts paying, long tailwind.")
        else:
            return _cs(66.0, "BULLISH", f"Contained funding ({fs}) — no carry headwind.")
    else:
        if ann_fund < -0.50:
            return _cs(12.0, "RISK",    f"Extreme negative funding ({fs}) — squeeze risk elevated.")
        elif ann_fund < -0.20:
            return _cs(28.0, "CAUTION", f"Shorts crowded ({fs}) — squeeze risk present.")
        elif abs(ann_fund) < 0.10:
            return _cs(64.0, "NEUTRAL", f"Neutral funding ({fs}) — no squeeze pressure.")
        elif ann_fund > 0.25:
            return _cs(80.0, "BEARISH", f"Longs paying heavy carry ({fs}) — tailwind for shorts.")
        else:
            return _cs(56.0, "NEUTRAL", f"Funding: {fs}")


def _flow_component(a: ScreenerAsset, side: str) -> dict:
    flow = a.flow_score or 50.0
    fi   = a.recent_trade_imbalance or 0.0
    bi   = a.orderbook_imbalance    or 0.0
    fs   = f"Trade imbalance {fi:+.2f}, book {bi:+.2f}"

    if side == "LONG":
        score = flow
        if score > 65:   return _cs(score, "BULLISH", f"Buy flow dominant. {fs}")
        elif score > 52: return _cs(score, "NEUTRAL",  f"Mild buy bias. {fs}")
        elif score > 40: return _cs(score, "NEUTRAL",  f"Flow mixed. {fs}")
        else:            return _cs(score, "BEARISH",  f"Sell flow dominant. {fs}")
    else:
        score = 100.0 - flow
        if score > 65:   return _cs(score, "BEARISH", f"Sell flow dominant. {fs}")
        elif score > 52: return _cs(score, "NEUTRAL",  f"Mild sell bias. {fs}")
        else:            return _cs(score, "BULLISH",  f"Buy flow resisting short. {fs}")


def _microstructure_component(a: ScreenerAsset, side: str) -> dict:
    bp   = a.book_pressure_score or 50.0
    vi   = a.volume_impulse      or 1.0
    vi15 = a.volume_impulse_15m  or 1.0
    prem = a.premium             or 0.0
    dist = a.distance_mark_oracle_pct or 0.0
    vol_elevated = vi >= 1.2 or vi15 >= 1.2
    micro_str = f"Vol impulse {max(vi, vi15):.2f}×, book {bp:.0f}/100, mark/oracle {dist:+.2f}%"

    if side == "LONG":
        score = (
            bp          * 0.40 +
            (65.0 if vol_elevated else 40.0) * 0.30 +
            (65.0 if prem >= 0 else 38.0)   * 0.30
        )
        if score > 65:   label = "BULLISH";  expl = f"Bid-heavy book + elevated volume. {micro_str}"
        elif score > 50: label = "NEUTRAL";  expl = f"Microstructure supportive. {micro_str}"
        else:            label = "BEARISH";  expl = f"Book ask-heavy or thin volume. {micro_str}"
    else:
        score = (
            (100.0 - bp) * 0.40 +
            (65.0 if vol_elevated else 40.0) * 0.30 +
            (65.0 if prem <= 0 else 38.0)   * 0.30
        )
        if score > 65:   label = "BEARISH"; expl = f"Ask-heavy book + elevated sell volume. {micro_str}"
        elif score > 50: label = "NEUTRAL"; expl = f"Microstructure neutral for short. {micro_str}"
        else:            label = "BULLISH"; expl = f"Book bid-heavy — short microstructure unfavorable. {micro_str}"

    return _cs(score, label, expl)


def _risk_component(a: ScreenerAsset) -> dict:
    """Risk is direction-agnostic; high risk → low score (bad for any trade)."""
    rs       = (a.risk_score or 0.0)            # 0-1
    tp       = (a.tradability_penalty or 30.0)  # 0-100
    av       = (a.avoid_score or 0.0)           # 0-100
    cr       = (a.collapse_risk_score or 0.0)
    ex       = (a.exhaustion_score or 0.0)
    spread   = (a.spread_bps or 0.0)
    ann_fund_abs = abs((a.funding or 0.0) * 8760)

    risk_raw = (rs * 35 + tp * 0.25 + av * 0.20 + cr * 0.12 + ex * 0.08) / 100.0
    score    = max(0.0, min(100.0, (1.0 - risk_raw) * 100))

    flags = []
    if spread > 20:          flags.append(f"wide spread {spread:.0f}bps")
    if ann_fund_abs > 0.30:  flags.append(f"high carry {ann_fund_abs:.1%}")
    if cr > 55:              flags.append("collapse risk elevated")
    if ex > 60:              flags.append("exhaustion signal")
    if tp > 50:              flags.append("thin orderbook")
    flag_str = "; ".join(flags) if flags else "No major risk flags"

    if score > 72:   label = "NEUTRAL"
    elif score > 52: label = "CAUTION"
    else:            label = "RISK"

    return _cs(score, label, flag_str)


# ─── Asset-level radar scores ──────────────────────────────────────────────────

def _long_score(a: ScreenerAsset) -> float:
    mom    = a.momentum_score          or 50.0
    flow   = a.flow_score              or 50.0
    trend  = a.trend_score             or 50.0
    bp     = a.book_pressure_score     or 50.0
    liq    = a.liquidity_score         or 50.0
    sq     = a.structural_quality_score or 50.0
    tp     = a.tradability_penalty     or 30.0
    ex     = a.exhaustion_score        or 0.0
    cr     = a.collapse_risk_score     or 0.0
    ann_fund = (a.funding or 0.0) * 8760

    base = (
        mom   * 0.28 +
        flow  * 0.22 +
        trend * 0.18 +
        bp    * 0.12 +
        liq   * 0.10 +
        sq    * 0.10
    )
    penalty  = 0.0
    penalty += max(0.0, ex - 38) * 0.30
    penalty += max(0.0, cr - 38) * 0.20
    penalty += max(0.0, tp - 38) * 0.15
    if ann_fund > 0.30:
        penalty += min(14.0, (ann_fund - 0.30) / 0.20 * 14)

    bonus = 0.0
    if a.squeeze_candidate:                    bonus += 5.0
    if ann_fund < -0.10:                       bonus += 4.0
    if (a.oi_change_1h  or 0) > 0.01:         bonus += 3.0
    if (a.volume_impulse or 1) > 1.3:         bonus += 3.0
    if (a.pullback_quality_score or 0) > 60:  bonus += 4.0

    return max(0.0, min(100.0, base - penalty + bonus))


def _short_score(a: ScreenerAsset) -> float:
    mom   = a.momentum_score       or 50.0
    flow  = a.flow_score           or 50.0
    trend = a.trend_score          or 50.0
    bp    = a.book_pressure_score  or 50.0
    liq   = a.liquidity_score      or 50.0
    ex    = a.exhaustion_score     or 0.0
    cr    = a.collapse_risk_score  or 0.0
    tp    = a.tradability_penalty  or 30.0
    ann_fund = (a.funding or 0.0) * 8760

    base = (
        (100 - mom)   * 0.25 +
        (100 - flow)  * 0.20 +
        (100 - trend) * 0.15 +
        (100 - bp)    * 0.10 +
        liq           * 0.08 +
        ex            * 0.12 +
        cr            * 0.10
    )
    penalty = 0.0
    penalty += max(0.0, tp - 38) * 0.15
    if a.crowded_short:
        penalty += 10.0
    if ann_fund < -0.30:
        penalty += min(12.0, (abs(ann_fund) - 0.30) / 0.20 * 12)

    bonus = 0.0
    if a.crowded_long:                   bonus += 5.0
    if ann_fund > 0.30:                  bonus += 4.0
    if (a.oi_change_5m or 0) < -0.01:   bonus += 4.0
    if ex > 55:                          bonus += 5.0
    if cr > 55:                          bonus += 5.0

    return max(0.0, min(100.0, base - penalty + bonus))


# ─── Side / setup / timing derivation ─────────────────────────────────────────

def _derive_side_setup_timing(
    a: ScreenerAsset,
    ls: float,
    ss: float,
) -> tuple[str, str, str]:
    """Return (side, setup_type, timing_state)."""
    tp       = a.tradability_penalty        or 30.0
    av       = a.avoid_score               or 0.0
    ex       = a.exhaustion_score          or 0.0
    cr       = a.collapse_risk_score       or 0.0
    ann_fund = (a.funding or 0.0) * 8760
    m1h      = a.momentum_1h               or 0.0
    m5m      = a.momentum_5m               or 0.0
    m24h     = a.momentum_24h              or 0.0
    flow     = a.flow_score                or 50.0
    regime   = a.asset_regime              or "chop_low_quality"
    sq_str   = a.structural_quality_score  or 50.0

    # ── 1. Hard avoid: untradeable / thin ─────────────────────────────────────
    if tp > 60 or av > 70:
        return ("AVOID", "CROWDED_AVOID", "NO_SETUP")

    # ── 2. Crowded avoid: extreme funding + exhaustion ────────────────────────
    if a.crowded_long and abs(ann_fund) > 0.40 and ex > 52:
        return ("AVOID", "CROWDED_AVOID", "EXHAUSTED")
    if a.crowded_short and abs(ann_fund) > 0.40 and cr > 52:
        return ("AVOID", "CROWDED_AVOID", "EXHAUSTED")

    # ── 3. Squeeze watch (short-crowded + upward momentum → LONG) ─────────────
    if a.squeeze_candidate:
        timing = "TRIGGERED" if m1h > 0.2 else "ARMING"
        return ("LONG", "SQUEEZE_WATCH", timing)

    # ── 4. Strong exhaustion / collapse → SHORT ───────────────────────────────
    if (ex > 60 or cr > 60) and ss > ls and ss > 52:
        timing = "TRIGGERED" if (cr > 65 and m1h < -0.2) else ("EXTENDED" if ex > 65 else "ARMING")
        return ("SHORT", "SHORT_WATCH", timing)

    # ── 5. Strong long setup ──────────────────────────────────────────────────
    if ls > 56 and ls > ss + 4:
        # Pullback in uptrend
        if regime == "structural_uptrend_pullback" and sq_str >= 46:
            timing = "TRIGGERED" if (m1h > 0.08 and flow > 56) else "ARMING"
            return ("LONG", "BUY_PULLBACK", timing)

        # Coiling for breakout
        if regime == "structural_uptrend_breakout_watch" and sq_str >= 46:
            timing = "ARMING" if m5m >= 0 else "EARLY"
            return ("LONG", "WATCH_BREAKOUT", timing)

        # TRADE_NOW threshold
        if ls >= _TRADE_NOW_MIN_SCORE and m1h > 0.1 and flow > 58:
            return ("LONG", "TRADE_NOW", "TRIGGERED")

        # Default: watch for confirmation
        timing = "EARLY" if m1h < 0.05 else "ARMING"
        return ("LONG", "WATCH_BREAKOUT", timing)

    # ── 6. Moderate short setup ───────────────────────────────────────────────
    if ss > 56 and ss > ls + 4:
        timing = "TRIGGERED" if (m1h < -0.2 and flow < 42) else "ARMING"
        return ("SHORT", "SHORT_WATCH", timing)

    # ── 7. Mild bias → WATCH ──────────────────────────────────────────────────
    if ls > 50 or ss > 50:
        return ("WATCH", "WATCH_BREAKOUT", "EARLY")

    return ("NEUTRAL", "NO_SETUP", "NO_SETUP")


# ─── Risk label ───────────────────────────────────────────────────────────────

def _risk_label(a: ScreenerAsset) -> str:
    rl = (a.risk_label or "").upper()
    if rl in ("LOW", "MED", "HIGH", "CROWDED"):
        return rl
    rs       = a.risk_score or 0.0
    ann_fund_abs = abs((a.funding or 0.0) * 8760)
    if a.crowded_long or a.crowded_short or ann_fund_abs > 0.40:
        return "CROWDED"
    if rs > 0.65: return "HIGH"
    if rs > 0.40: return "MED"
    return "LOW"


# ─── Narrative builders ───────────────────────────────────────────────────────

def _action_label(side: str, setup_type: str, timing: str) -> str:
    if setup_type == "TRADE_NOW":
        return "Trade now" if timing == "TRIGGERED" else "Setup arming — watch for trigger"
    if setup_type == "BUY_PULLBACK":
        return "Buy the pullback" if timing in ("TRIGGERED", "ARMING") else "Wait for pullback entry"
    if setup_type == "WATCH_BREAKOUT":
        return ("Watch for breakout confirmation" if side == "LONG"
                else "Watch for breakdown confirmation")
    if setup_type == "SQUEEZE_WATCH":
        return ("Squeeze triggered — momentum long" if timing == "TRIGGERED"
                else "Squeeze arming — size in on confirmation")
    if setup_type == "SHORT_WATCH":
        return ("Short triggered — exhaustion confirmed" if timing == "TRIGGERED"
                else "Fade strength — short setup developing")
    if setup_type == "CROWDED_AVOID":
        return "Avoid — crowded or untradeable conditions"
    return "No clear setup — monitor"


def _why_now(a: ScreenerAsset, side: str, setup_type: str, timing: str) -> str:
    ann_fund = (a.funding or 0.0) * 8760
    m1h  = a.momentum_1h  or 0.0
    m24h = a.momentum_24h or 0.0
    vi   = a.volume_impulse or 1.0
    oi   = (a.open_interest_usd or 0.0) / 1e6
    flow = a.flow_score or 50.0
    ex   = a.exhaustion_score or 0.0
    regime = a.asset_regime or ""

    if setup_type == "SQUEEZE_WATCH":
        return (f"Shorts crowded ({ann_fund:.1%} annual) with price bias turning up — "
                f"forced covering could accelerate the move. 1h: {m1h:+.2f}%.")

    if setup_type == "BUY_PULLBACK":
        regime_lbl = "Multi-day uptrend intact" if "uptrend" in regime else "Constructive structure"
        return (f"{regime_lbl} — pullback offers better risk/reward than chasing a breakout. "
                f"1h {m1h:+.2f}%, vol {vi:.1f}×. Bias is long until structure breaks.")

    if setup_type == "WATCH_BREAKOUT" and side == "LONG":
        return (f"Base forming near highs with {vi:.1f}× volume — watch for a 1h close above "
                f"resistance to confirm breakout bias. Current 1h: {m1h:+.2f}%.")

    if setup_type == "WATCH_BREAKOUT" and side == "SHORT":
        return (f"Momentum rolling over after a {m24h:+.2f}% 24h run — "
                f"watch for break of support to confirm short bias. 1h: {m1h:+.2f}%.")

    if setup_type == "TRADE_NOW" and side == "LONG":
        return (f"Aligned long signal: flow {flow:.0f}/100, 1h {m1h:+.2f}%, "
                f"OI ${oi:.1f}M. Funding contained at {ann_fund:.1%} ann.")

    if setup_type in ("SHORT_WATCH",) or side == "SHORT":
        return (f"Exhaustion building after {m24h:+.2f}% 24h run "
                f"(exhaustion {ex:.0f}/100). Longs paying {ann_fund:.1%} ann — "
                f"exits will amplify any drop once flow turns negative.")

    if setup_type == "CROWDED_AVOID":
        return (f"Avoid: extreme positioning ({ann_fund:.1%} ann funding) or thin orderbook. "
                f"Risk/reward unfavorable — wait for normalization.")

    return (f"Signal elevated. Monitor 1h momentum ({m1h:+.2f}%) "
            f"and flow ({flow:.0f}/100) for directional confirmation.")


def _entry_condition(a: ScreenerAsset, side: str, setup_type: str) -> str:
    vi   = a.volume_impulse or 1.0
    flow = a.flow_score or 50.0

    if setup_type in ("CROWDED_AVOID", "NO_SETUP"):
        return "No entry — wait for conditions to normalize."

    if side == "LONG":
        if setup_type == "SQUEEZE_WATCH":
            return ("Enter on 1h candle close with sustained buy flow (imbalance > +0.15) "
                    "and volume ≥ 1.3× rolling average.")
        if setup_type == "BUY_PULLBACK":
            return ("Enter on 1h close showing momentum turning positive again, "
                    "with buy flow re-establishing (imbalance > +0.10). Size conservatively.")
        if setup_type == "TRADE_NOW":
            return ("Momentum and flow aligned — enter near mid on limit. "
                    "Confirm flow imbalance > +0.10 at time of entry.")
        return ("Enter on confirmed 1h close above resistance with volume ≥ 1.3× "
                "rolling average and positive flow imbalance.")

    else:
        if setup_type == "SHORT_WATCH":
            return ("Enter short on 1h candle close turning negative with sell flow dominant "
                    "(imbalance < −0.15) and book skewing ask-heavy.")
        return ("Enter short on break of support with volume confirmation "
                "and book ask-heavy (imbalance < −0.10).")


def _invalidation_hint(a: ScreenerAsset, side: str, setup_type: str) -> str:
    px       = a.mark_px    or 0.0
    ann_fund = (a.funding or 0.0) * 8760

    if setup_type == "CROWDED_AVOID":
        return "No trade active — monitor for spread normalization and volume before reconsidering."
    if setup_type == "SQUEEZE_WATCH":
        return ("Invalidated if price fails to hold above current level — "
                "squeeze stalls and buy flow reverses to net selling.")
    if side == "LONG":
        lvl = f"~${round(px * 0.98, 4):.4g}" if px > 0 else "−2% from entry"
        return f"Invalidated on 1h close below {lvl}, or buy flow reversal to sustained net selling."
    else:
        lvl = f"~${round(px * 1.02, 4):.4g}" if px > 0 else "+2% from entry"
        return (f"Invalidated if price recovers above {lvl}, or funding starts dropping sharply "
                f"(shorts covering, squeeze risk). Currently: {ann_fund:.1%} ann.")


def _build_warnings(a: ScreenerAsset, side: str, setup_type: str) -> list[str]:
    warnings: list[str] = []
    ann_fund     = (a.funding or 0.0) * 8760
    ann_fund_abs = abs(ann_fund)
    spread = a.spread_bps or 0.0
    liq    = a.liquidity_score or 50.0
    rv     = a.realized_volatility_short or 0.0
    tp     = a.tradability_penalty or 30.0
    cr     = a.collapse_risk_score or 0.0
    ex     = a.exhaustion_score or 0.0

    if spread > 15:
        warnings.append(f"Wide spread ({spread:.0f}bps) — use limit orders; slippage risk.")
    if liq < 35:
        warnings.append("Low liquidity — large size will move the market.")
    if rv > 200:
        warnings.append(f"High realized vol ({rv:.0f}% ann) — wide stops required.")
    if tp > 48:
        warnings.append("Thin orderbook — limit orders strongly preferred.")
    if ann_fund > 0.30 and side == "LONG":
        warnings.append(f"High carry cost for longs ({ann_fund:.1%} ann) — time decay against position.")
    if ann_fund < -0.30 and side == "SHORT":
        warnings.append(f"Shorts crowded ({ann_fund_abs:.1%} ann neg funding) — squeeze risk present.")
    if cr > 55:
        warnings.append(f"Collapse risk elevated ({cr:.0f}/100) — do not add in weakness.")
    if ex > 55 and side == "LONG":
        warnings.append(f"Exhaustion signal ({ex:.0f}/100) — setup may already be extended.")
    if a.crowded_long and side == "LONG":
        warnings.append("Crowded long — exits will be contested; set stops and respect them.")
    return warnings[:4]


# ─── SetupCard assembler ──────────────────────────────────────────────────────

def _make_setup_card(
    a: ScreenerAsset,
    ls: float,
    ss: float,
    side: str,
    setup_type: str,
    timing: str,
) -> dict:
    # Raw score for this setup direction
    raw_score = ls if side in ("LONG", "WATCH") else (ss if side == "SHORT" else max(ls, ss))
    score     = round(raw_score, 1)

    # Confidence derived from score + signal quality boosts/penalties
    confidence = min(0.95, max(0.10, score / 100 * 0.88 + 0.06))
    rl = _risk_label(a)
    if rl in ("HIGH", "CROWDED"):    confidence = max(0.10, confidence - 0.12)
    if abs(a.recent_trade_imbalance or 0) > 0.35: confidence = min(0.95, confidence + 0.04)
    if (a.liquidity_score or 50) > 70:             confidence = min(0.95, confidence + 0.03)
    confidence = round(confidence, 3)

    # Downgrade TRADE_NOW if confidence too low
    if setup_type == "TRADE_NOW" and (confidence < _TRADE_NOW_MIN_CONF or score < _TRADE_NOW_MIN_SCORE):
        setup_type = "WATCH_BREAKOUT"

    # Component scores from the dominant side's perspective
    comp_side = "LONG" if side in ("LONG", "WATCH", "NEUTRAL") else "SHORT"

    ann_fund = (a.funding or 0.0) * 8760
    tags     = a.tags or []
    theme    = next((t for t in ["AI", "DeFi", "L1", "RWA", "meme", "gaming", "commodity", "pre-IPO"] if t in tags), None)

    return {
        "ticker":           a.coin,
        "name":             (a.display_name if a.display_name and a.display_name != a.coin else None),
        "category":         (tags[0] if tags else None),
        "theme":            theme,
        "side":             side,
        "setup_type":       setup_type,
        "score":            score,
        "confidence":       confidence,
        "risk_label":       rl,
        "timing_state":     timing,
        "action_label":     _action_label(side, setup_type, timing),
        "why_now":          _why_now(a, side, setup_type, timing),
        "entry_condition":  _entry_condition(a, side, setup_type),
        "invalidation_hint": _invalidation_hint(a, side, setup_type),
        "warnings":         _build_warnings(a, side, setup_type),
        "components": {
            "momentum":       _momentum_component(a, comp_side),
            "positioning":    _positioning_component(a, comp_side),
            "funding":        _funding_component(a, comp_side),
            "flow":           _flow_component(a, comp_side),
            "microstructure": _microstructure_component(a, comp_side),
            "risk":           _risk_component(a),
        },
        "raw_refs": {
            "price_change_24h":  round(a.pct_change_24h or 0.0, 4),
            "oi":                a.open_interest_usd,
            "oi_delta":          a.oi_change_1h,
            "volume":            a.day_ntl_vlm,
            "volume_velocity":   a.volume_impulse,
            "funding":           round(ann_fund, 6),
            "premium":           a.premium,
            "mark_oracle_delta": a.distance_mark_oracle_pct,
            "book_imbalance":    a.orderbook_imbalance,
            "risk_score":        a.risk_score,
        },
    }


# ─── Market regime ────────────────────────────────────────────────────────────

def _build_market_regime(assets: list[ScreenerAsset], all_cards: list[dict]) -> dict:
    from .signals import _compute_market_regime

    regime_data = _compute_market_regime(assets) if assets else {"regime": "unknown", "description": "No data", "metrics": {}}

    total_cards  = max(len(all_cards), 1)
    long_ct  = sum(1 for c in all_cards if c["side"] == "LONG")
    short_ct = sum(1 for c in all_cards if c["side"] == "SHORT")
    watch_ct = sum(1 for c in all_cards if c["side"] == "WATCH")
    avoid_ct = sum(1 for c in all_cards if c["side"] == "AVOID")

    return {
        "long_pct":             round(long_ct  / total_cards, 3),
        "short_pct":            round(short_ct / total_cards, 3),
        "watch_pct":            round(watch_ct / total_cards, 3),
        "avoid_pct":            round(avoid_ct / total_cards, 3),
        "total_assets_scanned": len(assets),
        "regime_label":          regime_data.get("regime", "unknown"),
        "summary":               regime_data.get("description", ""),
    }


# ─── Main entry point ─────────────────────────────────────────────────────────

def build_trade_radar(
    state: HyperliquidState,
    min_volume_usd: float = _MIN_VOLUME_USD,
) -> dict:
    """
    Build the full trade_radar payload.

    Returns:
    {
      "trade_radar": {
        "market_regime": {...},
        "cards": {"best_long", "best_short", "squeeze_watch", "pullback_buy", "crowded_avoid"},
        "top_setups": [SetupCard × 10],
        "selected_defaults": {"top_ticker": str | null},
        "explanation_version": "v1"
      },
      "meta": {"assets_scanned", "generated_at", "elapsed_ms"}
    }
    """
    t0 = time.time()

    # ── 1. Universe ────────────────────────────────────────────────────────────
    assets: list[ScreenerAsset] = [
        a for a in state.scored_assets()
        if a.market_type   == "perp"
        and a.market_status == "active"
        and (a.day_ntl_vlm or 0) >= min_volume_usd
    ]

    # ── 2. Score every asset ───────────────────────────────────────────────────
    ScoredTuple = tuple  # (asset, ls, ss, side, setup_type, timing)
    scored: list[ScoredTuple] = []
    for a in assets:
        ls = _long_score(a)
        ss = _short_score(a)
        side, setup_type, timing = _derive_side_setup_timing(a, ls, ss)
        scored.append((a, ls, ss, side, setup_type, timing))

    # ── 3. Build cards ─────────────────────────────────────────────────────────
    all_cards: list[dict] = []
    for a, ls, ss, side, setup_type, timing in scored:
        card = _make_setup_card(a, ls, ss, side, setup_type, timing)
        all_cards.append(card)
        if setup_type not in ("NO_SETUP",):
            print(
                f"[HL_RADAR] ticker={a.coin:<8s} side={side:<7s} "
                f"setup={setup_type:<18s} score={card['score']:.1f} "
                f"confidence={card['confidence']:.2f} risk={card['risk_label']}"
            )

    # ── 4. Top setups (actionable only, sorted by score) ─────────────────────
    actionable = [
        c for c in all_cards
        if c["side"] not in ("AVOID", "NEUTRAL") and c["setup_type"] != "NO_SETUP"
    ]
    actionable.sort(key=lambda c: (
        -(c["score"]),
        c["setup_type"] != "TRADE_NOW",
        c["setup_type"] != "BUY_PULLBACK",
    ))
    top_setups = actionable[:_TOP_N]

    # ── 5. Feature cards (best by category) ───────────────────────────────────
    longs    = sorted([c for c in actionable if c["side"] == "LONG"],  key=lambda c: -c["score"])
    shorts   = sorted([c for c in actionable if c["side"] == "SHORT"], key=lambda c: -c["score"])
    squeezes = sorted([c for c in actionable if c["setup_type"] == "SQUEEZE_WATCH"], key=lambda c: -c["score"])
    pullbacks = sorted([c for c in actionable if c["setup_type"] == "BUY_PULLBACK"],  key=lambda c: -c["score"])
    crowded  = sorted([c for c in all_cards  if c["setup_type"] == "CROWDED_AVOID"], key=lambda c: -c["score"])

    cards = {
        "best_long":     longs[0]    if longs    else None,
        "best_short":    shorts[0]   if shorts   else None,
        "squeeze_watch": squeezes[0] if squeezes else None,
        "pullback_buy":  pullbacks[0] if pullbacks else None,
        "crowded_avoid": crowded[0]  if crowded  else None,
    }

    # ── 6. Market regime ──────────────────────────────────────────────────────
    market_regime = _build_market_regime(assets, all_cards)

    elapsed_ms = round((time.time() - t0) * 1000)
    print(
        f"[HL_RADAR] assets={len(assets)} top_setups={len(top_setups)} "
        f"longs={len(longs)} shorts={len(shorts)} squeezes={len(squeezes)} "
        f"pullbacks={len(pullbacks)} elapsed_ms={elapsed_ms}"
    )

    return {
        "trade_radar": {
            "market_regime":   market_regime,
            "cards":           cards,
            "top_setups":      top_setups,
            "selected_defaults": {
                "top_ticker": top_setups[0]["ticker"] if top_setups else None,
            },
            "explanation_version": "v1",
        },
        "meta": {
            "assets_scanned": len(assets),
            "generated_at":   time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "elapsed_ms":     elapsed_ms,
        },
    }
