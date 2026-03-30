"""
Hyperliquid Screener — Hero Signal Generator

Builds the heroAgentSignals payload: top 3-5 fully-formed trade ideas.

Scoring flow (deterministic, no LLM):
  9 component scores → 5 setup scores → overall_score + setup_type
  Then: thesis_title, thesis_summary, reasons[], risk_flags[], invalidation_notes[]

The feature engine already computes all scores in ScreenerAsset.
This module assembles the narrative from those scores.
"""
from __future__ import annotations

import time
from typing import Optional

from .models import HeroSignal, HeroSignalMetrics, ScreenerAsset
from .state import HyperliquidState


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

def generate_hero_signals(
    state: HyperliquidState,
    top_n: int = 5,
    min_volume_usd: float = 10_000_000,
    min_oi_usd: float = 500_000,
) -> list[HeroSignal]:
    """
    Select top N hero trade ideas from the current screener universe.

    Candidates must be:
    - Perp market (has funding/OI)
    - Active market status
    - Minimum liquidity thresholds
    - Not setup_type = "avoid" (unless nothing else qualifies)

    Returns HeroSignal objects ranked by overall_score.
    """
    assets = state.perp_assets()
    assets = [
        a for a in assets
        if a.market_status == "active"
        and (a.day_ntl_vlm or 0) >= min_volume_usd
        and (a.open_interest_usd or 0) >= min_oi_usd
        and a.overall_score is not None
    ]

    # Prefer non-avoid setups; fall back to avoid if universe is too small
    non_avoid = [a for a in assets if a.setup_type != "avoid"]
    candidates = non_avoid if len(non_avoid) >= top_n else assets

    # Sort by overall_score descending
    candidates.sort(key=lambda a: -(a.overall_score or 0))
    top = candidates[:top_n]

    return [_build_hero(a) for a in top]


# ─────────────────────────────────────────────────────────────────────────────
# Hero signal builder
# ─────────────────────────────────────────────────────────────────────────────

def _build_hero(asset: ScreenerAsset) -> HeroSignal:
    setup_type = asset.setup_type or "breakout"
    side       = _derive_side(asset)
    confidence = _derive_confidence(asset)
    reasons    = _build_reasons(asset, setup_type)
    risk_flags = _build_risk_flags(asset)
    inval      = _build_invalidation(asset, setup_type, side)
    title, summary = _build_thesis(asset, setup_type, side)

    metrics = HeroSignalMetrics(
        mark_px           = asset.mark_px,
        pct_change_24h    = round(asset.pct_change_24h / 100, 6) if asset.pct_change_24h is not None else None,
        funding           = asset.funding,
        funding_ann_pct   = round((asset.funding or 0) * 8760 * 100, 3),
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

    score_components = {
        "momentum":           round((asset.momentum_score    or 50) / 100, 3),
        "flow":               round((asset.flow_score        or 50) / 100, 3),
        "trend":              round((asset.trend_score       or 50) / 100, 3),
        "book_pressure":      round((asset.book_pressure_score or 50) / 100, 3),
        "crowding":           round((asset.crowding_score    or 0)  / 100, 3),
        "dislocation":        round((asset.dislocation_score or 0)  / 100, 3),
        "liquidity":          round((asset.liquidity_score   or 50) / 100, 3),
        "volatility":         round((asset.volatility_score  or 50) / 100, 3),
        "tradability_penalty":round((asset.tradability_penalty or 0) / 100, 3),
        "breakout":           round((asset.breakout_score          or 50) / 100, 3),
        "mean_reversion":     round((asset.mean_reversion_score    or 50) / 100, 3),
        "trend_continuation": round((asset.trend_continuation_score or 50) / 100, 3),
        "crowding_unwind":    round((asset.crowding_unwind_score   or 0)  / 100, 3),
        "avoid":              round((asset.avoid_score             or 0)  / 100, 3),
    }

    return HeroSignal(
        coin               = asset.coin,
        side               = side,
        overall_score      = round(asset.overall_score or 0, 1),
        setup_type         = setup_type,
        confidence         = round(confidence, 2),
        thesis_title       = title,
        thesis_summary     = summary,
        reasons            = reasons,
        risk_flags         = risk_flags,
        invalidation_notes = inval,
        metrics            = metrics,
        score_components   = score_components,
        generated_at       = time.time(),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Narrative builders
# ─────────────────────────────────────────────────────────────────────────────

def _derive_side(asset: ScreenerAsset) -> str:
    setup = asset.setup_type or ""
    direction = asset.signal_direction or "neutral"

    if setup == "crowding_unwind":
        # Side is opposite to the crowded position
        ann_fund = (asset.funding or 0) * 8760
        if ann_fund > 0:    return "short"   # longs crowded → fade longs
        elif ann_fund < 0:  return "long"    # shorts crowded → squeeze
        return "neutral_watch"

    if setup == "mean_reversion":
        # Fade the current stretched move
        m1h = asset.momentum_1h or 0
        if m1h > 0:    return "short"
        elif m1h < 0:  return "long"
        return "neutral_watch"

    if setup in ("breakout", "trend_continuation"):
        if direction == "long":  return "long"
        if direction == "short": return "short"
        # Use momentum direction as tiebreaker
        if (asset.momentum_1h or 0) > 0: return "long"
        if (asset.momentum_1h or 0) < 0: return "short"
        return "neutral_watch"

    return "neutral_watch"


def _derive_confidence(asset: ScreenerAsset) -> float:
    """0-1 confidence from multiple signal convergence."""
    base = (asset.overall_score or 50) / 100
    # Boost if signals agree
    boost = 0.0
    if abs(asset.recent_trade_imbalance or 0) > 0.4:  boost += 0.05
    if abs(asset.orderbook_imbalance or 0) > 0.3:     boost += 0.05
    if (asset.tradability_penalty or 50) < 20:        boost += 0.05
    if (asset.liquidity_score or 0) > 70:             boost += 0.03
    # Penalize low liquidity
    if (asset.liquidity_score or 0) < 30:             boost -= 0.10
    if (asset.tradability_penalty or 0) > 50:         boost -= 0.10
    return max(0.1, min(0.99, base + boost))


def _build_reasons(asset: ScreenerAsset, setup: str) -> list[str]:
    reasons: list[str] = []
    ann_fund = (asset.funding or 0) * 8760

    if setup == "breakout":
        m1h = asset.momentum_1h or 0
        m4h = asset.momentum_4h or 0
        if m1h > 0.2:
            reasons.append(f"Strong 1h momentum: +{m1h:.2f}%")
        elif m1h < -0.2:
            reasons.append(f"Sharp 1h decline: {m1h:.2f}%")
        if m4h != 0:
            dir_ = "↑" if m4h > 0 else "↓"
            reasons.append(f"4h trend {dir_}: {m4h:+.2f}%")
        fi = asset.recent_trade_imbalance or 0
        if fi > 0.3:
            reasons.append(f"Buy flow dominant: {fi:.0%} net buyers")
        elif fi < -0.3:
            reasons.append(f"Sell flow dominant: {abs(fi):.0%} net sellers")
        bi = asset.orderbook_imbalance or 0
        if bi > 0.3:
            reasons.append(f"Bid-heavy order book: {bi:+.2f} imbalance")
        elif bi < -0.3:
            reasons.append(f"Ask-heavy order book: {bi:+.2f} imbalance")
        if (asset.volume_impulse or 0) > 1.5:
            reasons.append(f"Volume surge: {asset.volume_impulse:.1f}× normal")

    elif setup == "mean_reversion":
        if abs(ann_fund) > 0.10:
            side_str = "longs" if ann_fund > 0 else "shorts"
            reasons.append(f"Extreme funding: {side_str} paying {abs(ann_fund):.1%} annual")
        oracle_gap = asset.distance_mark_oracle_pct or 0
        if abs(oracle_gap) > 0.3:
            dir_ = "above" if oracle_gap > 0 else "below"
            reasons.append(f"Mark {dir_} oracle by {abs(oracle_gap):.2f}%")
        if abs(asset.premium or 0) > 0.003:
            prem_dir = "premium" if (asset.premium or 0) > 0 else "discount"
            reasons.append(f"Market trading at {abs(asset.premium or 0):.3%} {prem_dir}")
        m24h = asset.momentum_24h or 0
        if abs(m24h) > 2:
            reasons.append(f"Stretched 24h move: {m24h:+.2f}% — mean reversion due")

    elif setup == "trend_continuation":
        trends = [f for f in [
            (f"1h: {asset.momentum_1h:+.2f}%" if asset.momentum_1h else None),
            (f"4h: {asset.momentum_4h:+.2f}%" if asset.momentum_4h else None),
            (f"24h: {asset.momentum_24h:+.2f}%" if asset.momentum_24h else None),
        ] if f]
        if trends:
            reasons.append(f"Aligned multi-TF momentum: {', '.join(trends)}")
        fi = asset.recent_trade_imbalance or 0
        if abs(fi) > 0.2:
            reasons.append(f"Flow sustaining trend: {fi:+.2f} imbalance")
        if (asset.liquidity_score or 0) > 60:
            reasons.append(f"High-liquidity market supports trend")

    elif setup == "crowding_unwind":
        if ann_fund > 0:
            reasons.append(f"Longs heavily crowded: {ann_fund:.1%} annual funding")
        elif ann_fund < 0:
            reasons.append(f"Shorts heavily crowded: {ann_fund:.1%} annual funding")
        if (asset.open_interest_usd or 0) > 0:
            reasons.append(f"OI: ${asset.open_interest_usd/1e6:.0f}M — elevated positioning")
        if asset.squeeze_candidate:
            reasons.append("Short squeeze candidate: crowded short with upward price action")
        if asset.dislocated_vs_oracle:
            reasons.append(f"Oracle dislocation: {asset.distance_mark_oracle_pct:+.2f}%")

    # Universal context
    if asset.pct_change_24h is not None:
        reasons.append(f"24h change: {asset.pct_change_24h:+.2f}%")

    return reasons[:6]


def _build_risk_flags(asset: ScreenerAsset) -> list[str]:
    flags: list[str] = []
    ann_fund = (asset.funding or 0) * 8760

    if (asset.spread_bps or 0) > 15:
        flags.append(f"Wide spread: {asset.spread_bps:.1f}bps — slippage risk")
    if (asset.liquidity_score or 0) < 30:
        flags.append("Low liquidity — large size will move market")
    if (asset.realized_volatility_short or 0) > 200:
        flags.append(f"High volatility: {asset.realized_volatility_short:.0f}% ann. — wide stops needed")
    if (asset.tradability_penalty or 0) > 50:
        flags.append("Thin orderbook — limit orders preferred")
    if asset.crowded_long and ann_fund > 0:
        flags.append(f"Crowded long: {ann_fund:.1%} ann. funding cost against")
    elif asset.crowded_short and ann_fund < 0:
        flags.append(f"Crowded short: {abs(ann_fund):.1%} ann. funding benefit — squeeze risk")
    if abs(asset.distance_mark_oracle_pct or 0) > 1.0:
        flags.append(f"Large mark/oracle gap: {asset.distance_mark_oracle_pct:+.2f}% — may snap")
    if (asset.oi_change_5m or 0) < -0.02:
        flags.append(f"OI dropping 5m: {(asset.oi_change_5m or 0):.2%} — positioning unwinding")

    return flags[:5]


def _build_invalidation(asset: ScreenerAsset, setup: str, side: str) -> list[str]:
    notes: list[str] = []
    mark = asset.mark_px or 0

    if setup == "breakout":
        if side == "long":
            lvl = mark * 0.99
            notes.append(f"Invalidated if price drops back below ~${lvl:.4g} (−1% retracement)")
            if (asset.recent_trade_imbalance or 0) > 0:
                notes.append("Invalidated if buy flow reverses to net selling")
        else:
            lvl = mark * 1.01
            notes.append(f"Invalidated if price recovers above ~${lvl:.4g} (+1%)")

    elif setup == "mean_reversion":
        ann_fund = (asset.funding or 0) * 8760
        if ann_fund > 0:
            notes.append("Invalidated if funding remains elevated without price reversal for 2+ hours")
        notes.append(f"Invalidated if oracle gap widens further beyond −2%")
        if asset.momentum_24h:
            notes.append(f"Invalidated if 24h move continues unabated past 2× current move")

    elif setup == "trend_continuation":
        if side == "long":
            lvl = mark * 0.98
            notes.append(f"Invalidated if price closes 1h candle below ~${lvl:.4g} (−2%)")
        else:
            lvl = mark * 1.02
            notes.append(f"Invalidated if price recovers above ~${lvl:.4g} (+2%)")
        notes.append("Invalidated if flow imbalance flips direction")

    elif setup == "crowding_unwind":
        ann_fund = (asset.funding or 0) * 8760
        if ann_fund > 0:
            notes.append("Invalidated if funding normalizes below +20% annual before price reverses")
        notes.append("Invalidated if OI continues building (adding to crowd, not unwinding)")
        notes.append("Invalidated if price makes new highs — trend may be stronger than crowding")

    if not notes:
        notes.append("Invalidated if market structure changes significantly")

    return notes[:4]


def _build_thesis(asset: ScreenerAsset, setup: str, side: str) -> tuple[str, str]:
    coin     = asset.coin
    ann_fund = (asset.funding or 0) * 8760
    m1h      = asset.momentum_1h   or 0
    m4h      = asset.momentum_4h   or 0
    m24h     = asset.momentum_24h  or 0

    titles = {
        "breakout": {
            "long":  f"{coin} Breakout Long",
            "short": f"{coin} Breakdown Short",
            "neutral_watch": f"{coin} Directional Breakout",
        },
        "mean_reversion": {
            "long":  f"{coin} Mean Reversion — Long Dip",
            "short": f"{coin} Mean Reversion — Fade Rip",
            "neutral_watch": f"{coin} Mean Reversion Setup",
        },
        "trend_continuation": {
            "long":  f"{coin} Trend Continuation — Long",
            "short": f"{coin} Trend Continuation — Short",
            "neutral_watch": f"{coin} Trend Continuation Watch",
        },
        "crowding_unwind": {
            "long":  f"{coin} Short Squeeze Setup",
            "short": f"{coin} Long Unwind — Fade Crowded Longs",
            "neutral_watch": f"{coin} Crowding Unwind Watch",
        },
        "avoid": {
            "neutral_watch": f"{coin} — Avoid (Low Quality Setup)",
            "long": f"{coin} — Low-Confidence Long",
            "short": f"{coin} — Low-Confidence Short",
        },
    }
    title = titles.get(setup, {}).get(side, f"{coin} Signal")

    # Build summary
    if setup == "breakout":
        dir_str = "upside" if side == "long" else "downside"
        summary = (
            f"{coin} showing {dir_str} momentum with "
            f"1h: {m1h:+.2f}%, 4h: {m4h:+.2f}%. "
        )
        fi = asset.recent_trade_imbalance or 0
        if abs(fi) > 0.2:
            summary += f"Trade flow {fi:+.0%} supports the move. "
        if (asset.volume_impulse or 0) > 1.3:
            summary += f"Volume {asset.volume_impulse:.1f}× above average confirms acceleration."

    elif setup == "mean_reversion":
        oracle_gap = asset.distance_mark_oracle_pct or 0
        summary = (
            f"{coin} is stretched vs fair value. "
            f"Mark/oracle gap: {oracle_gap:+.2f}%. "
            f"Funding: {ann_fund:+.1%} annual — "
        )
        if ann_fund > 0.20:
            summary += "longs paying unsustainable carry. "
        elif ann_fund < -0.20:
            summary += "shorts paying unsustainable carry. "
        summary += "Setup favors reversion to mean."

    elif setup == "trend_continuation":
        dir_str = "bullish" if side == "long" else "bearish"
        summary = (
            f"{coin} in a confirmed {dir_str} trend across 1h/4h/24h. "
            f"24h move: {m24h:+.2f}%. "
            f"Liquidity ({asset.liquidity_score or 0:.0f}/100) supports continued participation."
        )

    elif setup == "crowding_unwind":
        if ann_fund > 0:
            summary = (
                f"{coin} longs crowded at {ann_fund:.1%} annual funding. "
                f"OI: ${(asset.open_interest_usd or 0)/1e6:.0f}M elevated. "
                f"Setup favors position unwind — fade the crowd."
            )
        else:
            summary = (
                f"{coin} shorts crowded at {ann_fund:.1%} annual funding (negative = shorts pay). "
                f"OI: ${(asset.open_interest_usd or 0)/1e6:.0f}M. "
                f"Setup favors short squeeze — buy the crowd out."
            )
    else:
        summary = f"{coin} showing {asset.setup_type} signal with overall score {asset.overall_score:.0f}/100."

    return title, summary


# ─────────────────────────────────────────────────────────────────────────────
# Section builder helpers
# ─────────────────────────────────────────────────────────────────────────────

def build_signal_sections(
    state: HyperliquidState,
    rows_per_section: int = 6,
) -> dict:
    """
    Build section-oriented data payload for the frontend.

    Always-available sections (from current market data):
      top_gainers, top_losers, high_funding, negative_funding,
      mark_oracle_gap, premium_discount, volume_leaders, trade_flow,
      book_imbalance, volatility_leaders, breakout_watch, mean_reversion,
      crowded_longs, crowded_shorts, short_squeeze, long_flush_watch

    Conditional sections (require rolling history):
      oi_expansion, oi_unwind, volume_impulse_watch

    Returns a dict of { section_key: {title, subtitle, rows, available} }
    """
    perps = [a for a in state.perp_assets() if a.market_status == "active"]
    spots = [a for a in state.spot_assets()  if a.market_status == "active"]
    all_active = perps + spots

    def _row(a: ScreenerAsset) -> dict:
        """Minimal row for section display."""
        ann_fund = (a.funding or 0) * 8760
        return {
            "coin":           a.coin,
            "displayName":    a.display_name,
            "marketType":     a.market_type,
            "markPrice":      a.mark_px,
            "change24hPct":   round(a.pct_change_24h / 100, 6) if a.pct_change_24h is not None else None,
            "funding":        a.funding,
            "fundingAnnPct":  round(ann_fund * 100, 3),
            "openInterest":   a.open_interest_usd,
            "volume24h":      a.day_ntl_vlm,
            "compositeSignal": round((a.composite_signal_score or 50) / 100, 4),
            "overallScore":   round((a.overall_score or 50) / 100, 4),
            "setupType":      a.setup_type,
            "signalDirection": a.signal_direction,
            "markOracleGapPct": a.distance_mark_oracle_pct,
            "premium":        a.premium,
            "tradeImbalance": a.recent_trade_imbalance,
            "bookImbalance":  a.orderbook_imbalance,
            "volatilityScore": round((a.volatility_score or 50) / 100, 3),
            "liquidityScore": round((a.liquidity_score or 50) / 100, 3),
            "spreadBps":      a.spread_bps,
            "oiChange5m":     a.oi_change_5m,
            "oiChange15m":    a.oi_change_15m,
            "oiChange1h":     a.oi_change_1h,
            "volumeImpulse":  a.volume_impulse,
            "volumeImpulse5m": a.volume_impulse_5m,
            "crowdedLong":    a.crowded_long,
            "crowdedShort":   a.crowded_short,
            "squeezeCand":    a.squeeze_candidate,
        }

    def _sec(title, subtitle, assets_sorted, n=rows_per_section, cond_field=None):
        available = True
        if cond_field:
            # Only expose if at least half the universe has this field populated
            filled = sum(1 for a in perps if getattr(a, cond_field) is not None)
            available = filled >= max(5, len(perps) * 0.3)
        return {
            "title": title,
            "subtitle": subtitle,
            "rows": [_row(a) for a in assets_sorted[:n]],
            "available": available,
        }

    # OI history availability check
    oi_history_ready = sum(1 for coin in state.oi_history if len(state.oi_history[coin]) >= 5) >= 10

    sections: dict = {}

    # ── Always-available sections ─────────────────────────────────────────
    sections["top_gainers"] = _sec(
        "Top Gainers", "Strongest 24h price movers",
        sorted(all_active, key=lambda a: -(a.pct_change_24h or 0)),
    )
    sections["top_losers"] = _sec(
        "Top Losers", "Sharpest 24h price declines",
        sorted(all_active, key=lambda a: (a.pct_change_24h or 0)),
    )
    sections["high_funding"] = _sec(
        "High Funding", "Longs paying — squeeze watch",
        sorted(perps, key=lambda a: -((a.funding or 0))),
    )
    sections["negative_funding"] = _sec(
        "Negative Funding", "Shorts paying — flush watch",
        sorted(perps, key=lambda a: (a.funding or 0)),
    )
    sections["mark_oracle_gap"] = _sec(
        "Mark/Oracle Gap", "Largest mark vs oracle delta",
        sorted(perps, key=lambda a: -abs(a.distance_mark_oracle_pct or 0)),
    )
    sections["premium_discount"] = _sec(
        "Premium/Discount", "Mark vs mid price dislocation",
        sorted(perps, key=lambda a: -abs(a.distance_mark_mid_pct or 0)),
    )
    sections["volume_leaders"] = _sec(
        "Volume Leaders", "Largest 24h notional volume",
        sorted(all_active, key=lambda a: -(a.day_ntl_vlm or 0)),
    )
    sections["trade_flow"] = _sec(
        "Trade Flow", "Buy vs sell trade pressure",
        sorted(
            [a for a in perps if a.recent_trade_imbalance is not None],
            key=lambda a: -abs(a.recent_trade_imbalance or 0),
        ),
    )
    sections["book_imbalance"] = _sec(
        "Book Imbalance", "Order book bid/ask skew",
        sorted(
            [a for a in perps if a.orderbook_imbalance is not None],
            key=lambda a: -abs(a.orderbook_imbalance or 0),
        ),
    )
    sections["volatility_leaders"] = _sec(
        "Volatility Leaders", "Highest realized vol score",
        sorted(perps, key=lambda a: -(a.volatility_score or 0)),
    )
    sections["breakout_watch"] = _sec(
        "Breakout Watch", "High breakout probability",
        sorted(perps, key=lambda a: -(a.breakout_score or 0)),
    )
    sections["mean_reversion"] = _sec(
        "Mean Reversion", "Stretched vs mean — reversal",
        sorted(perps, key=lambda a: -(a.mean_reversion_score or 0)),
    )
    sections["crowded_longs"] = _sec(
        "Crowded Longs", "High signal + bullish crowd",
        sorted([a for a in perps if a.crowded_long], key=lambda a: -((a.crowding_score or 0))),
    )
    sections["crowded_shorts"] = _sec(
        "Crowded Shorts", "Low signal + bearish crowd",
        sorted([a for a in perps if a.crowded_short], key=lambda a: -((a.crowding_score or 0))),
    )
    sections["short_squeeze"] = _sec(
        "Short Squeeze", "High funding + bearish OI",
        sorted([a for a in perps if a.squeeze_candidate], key=lambda a: -((a.crowding_unwind_score or 0))),
    )
    sections["long_flush_watch"] = _sec(
        "Long Flush Watch", "Negative funding + bull OI",
        sorted([a for a in perps if a.crowded_long and (a.momentum_1h or 0) < 0],
               key=lambda a: -((a.crowding_unwind_score or 0))),
    )
    sections["illiquid_zone"] = _sec(
        "Illiquid Zone", "Low liquidity — dangerous OI",
        sorted([a for a in perps if a.illiquid_high_volatility or a.avoid_due_to_spread],
               key=lambda a: -(a.avoid_score or 0)),
    )

    # ── Conditional sections (need rolling history) ───────────────────────
    if oi_history_ready:
        oi_exp = [a for a in perps if (a.oi_change_5m or 0) > 0.01]
        oi_unw = [a for a in perps if (a.oi_change_5m or 0) < -0.01]
        sections["oi_expansion"] = _sec(
            "OI Expansion", "Largest open interest build",
            sorted(oi_exp, key=lambda a: -(a.oi_change_5m or 0)),
        )
        sections["oi_unwind"] = _sec(
            "OI Unwind", "Largest OI liquidation",
            sorted(oi_unw, key=lambda a: (a.oi_change_5m or 0)),
        )
    else:
        sections["oi_expansion"] = {"title": "OI Expansion", "subtitle": "Building OI history...", "rows": [], "available": False}
        sections["oi_unwind"]    = {"title": "OI Unwind",    "subtitle": "Building OI history...", "rows": [], "available": False}

    vol_impulse_ready = sum(1 for a in perps if a.volume_impulse_5m is not None) >= 10
    if vol_impulse_ready:
        sections["volume_impulse_watch"] = _sec(
            "Volume Impulse", "Highest recent volume acceleration",
            sorted(perps, key=lambda a: -(a.volume_impulse_5m or a.volume_impulse or 0)),
        )
    else:
        sections["volume_impulse_watch"] = {
            "title": "Volume Impulse", "subtitle": "Collecting 5m candle data...",
            "rows": [], "available": False,
        }

    return sections


def build_summary_cards(state: HyperliquidState) -> list[dict]:
    """
    Build the top summary bar cards shown above the screener.
    Returns a list of { id, label, value, subValue, coinRef } dicts.
    """
    perps = [a for a in state.perp_assets() if a.market_status == "active" and a.pct_change_24h is not None]
    if not perps:
        return []

    # Top gainer
    top_gainer = max(perps, key=lambda a: a.pct_change_24h or 0)
    # Top loser
    top_loser  = min(perps, key=lambda a: a.pct_change_24h or 0)

    # OI expansion (need history)
    oi_exp_coin = None
    oi_exp_val  = None
    oi_exp_assets = [a for a in perps if a.oi_change_5m is not None]
    if oi_exp_assets:
        best = max(oi_exp_assets, key=lambda a: a.oi_change_5m or 0)
        oi_exp_coin = best.coin
        oi_exp_val  = best.oi_change_5m

    # OI unwind
    oi_unw_coin = None
    oi_unw_val  = None
    if oi_exp_assets:
        worst = min(oi_exp_assets, key=lambda a: a.oi_change_5m or 0)
        if (worst.oi_change_5m or 0) < -0.005:
            oi_unw_coin = worst.coin
            oi_unw_val  = worst.oi_change_5m

    # High funding
    fund_sorted = sorted(perps, key=lambda a: -(a.funding or 0))
    high_fund   = fund_sorted[0] if fund_sorted else None
    # Neg funding
    neg_fund_sorted = sorted(perps, key=lambda a: (a.funding or 0))
    neg_fund   = neg_fund_sorted[0] if neg_fund_sorted else None

    # Mark/oracle gap
    gap_sorted = sorted(perps, key=lambda a: -abs(a.distance_mark_oracle_pct or 0))
    top_gap    = gap_sorted[0] if gap_sorted else None

    # Volume impulse
    vol_sorted = sorted([a for a in perps if a.volume_impulse], key=lambda a: -(a.volume_impulse or 0))
    top_vol    = vol_sorted[0] if vol_sorted else None

    # Book imbalance
    book_sorted = sorted([a for a in perps if a.orderbook_imbalance is not None],
                         key=lambda a: -abs(a.orderbook_imbalance or 0))
    top_book = book_sorted[0] if book_sorted else None

    cards = [
        {
            "id":      "top_gainer",
            "label":   "Top Gainer",
            "coinRef": top_gainer.coin,
            "value":   f"+{top_gainer.pct_change_24h:.2f}%",
            "subValue": f"${top_gainer.mark_px:.4g}" if top_gainer.mark_px else None,
        },
        {
            "id":      "top_loser",
            "label":   "Top Loser",
            "coinRef": top_loser.coin,
            "value":   f"{top_loser.pct_change_24h:.2f}%",
            "subValue": f"${top_loser.mark_px:.4g}" if top_loser.mark_px else None,
        },
        {
            "id":      "oi_expansion",
            "label":   "OI Expansion",
            "coinRef": oi_exp_coin,
            "value":   f"{oi_exp_val:+.2%}" if oi_exp_val is not None else "—",
            "subValue": "5m change" if oi_exp_val else "Building history...",
        },
        {
            "id":      "oi_unwind",
            "label":   "OI Unwind",
            "coinRef": oi_unw_coin,
            "value":   f"{oi_unw_val:.2%}" if oi_unw_val is not None else "—",
            "subValue": "5m change" if oi_unw_val else "Building history...",
        },
        {
            "id":      "high_funding",
            "label":   "High Funding",
            "coinRef": high_fund.coin if high_fund else None,
            "value":   f"{(high_fund.funding or 0):.4%}" if high_fund else "—",
            "subValue": f"${(high_fund.day_ntl_vlm or 0)/1e6:.1f}M vol" if high_fund else None,
        },
        {
            "id":      "neg_funding",
            "label":   "Neg Funding",
            "coinRef": neg_fund.coin if neg_fund else None,
            "value":   f"{(neg_fund.funding or 0):.4%}" if neg_fund else "—",
            "subValue": f"${(neg_fund.day_ntl_vlm or 0)/1e6:.1f}M vol" if neg_fund else None,
        },
        {
            "id":      "mark_oracle_gap",
            "label":   "Mk/Oracle Gap",
            "coinRef": top_gap.coin if top_gap else None,
            "value":   f"{top_gap.distance_mark_oracle_pct:+.4f}%" if top_gap else "—",
            "subValue": f"${top_gap.mark_px:.4g}" if (top_gap and top_gap.mark_px) else None,
        },
        {
            "id":      "vol_impulse",
            "label":   "Vol Impulse",
            "coinRef": top_vol.coin if top_vol else None,
            "value":   f"{top_vol.volume_impulse:.2f}×" if top_vol else "—",
            "subValue": f"${(top_vol.day_ntl_vlm or 0)/1e6:.0f}M" if top_vol else None,
        },
        {
            "id":      "book_imbalance",
            "label":   "Book Imbalance",
            "coinRef": top_book.coin if top_book else None,
            "value":   f"{top_book.orderbook_imbalance:+.3f}" if top_book else "—",
            "subValue": f"${(top_book.open_interest_usd or 0)/1e6:.0f}M OI" if top_book else None,
        },
    ]

    return cards
