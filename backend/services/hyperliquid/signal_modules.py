"""
Hyperliquid Screener — Advanced Signal Modules

Three new signal sections for the dashboard:
  1. Relative Strength Leaders — outperformance vs BTC benchmark
  2. Order Book Pressure — directional pressure from L2 depth
  3. OI Regime Shift — classify price/OI dynamics into regimes

All computations are deterministic. No LLM calls.
"""
from __future__ import annotations

import math
import time
from datetime import datetime, timezone
from typing import Optional

from .models import ScreenerAsset
from .state import HyperliquidState


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _safe_div(a: float, b: float, default: float = 0.0) -> float:
    return a / b if b and b != 0 else default


def _pct_change(new: float, old: float) -> Optional[float]:
    if old and old != 0:
        return (new - old) / old * 100
    return None


def _close_from_candles(candles: list[dict], bars_ago: int) -> Optional[float]:
    """Get the close price `bars_ago` bars from the end of a candle list."""
    idx = len(candles) - 1 - bars_ago
    if idx < 0 or idx >= len(candles):
        return None
    c = candles[idx].get("c")
    if c is None:
        return None
    try:
        return float(c)
    except (TypeError, ValueError):
        return None


def _return_over_bars(candles: list[dict], n_bars: int) -> Optional[float]:
    """Compute % return over the last n_bars (close-to-close)."""
    now_close = _close_from_candles(candles, 0)
    old_close = _close_from_candles(candles, n_bars)
    if now_close is None or old_close is None:
        return None
    return _pct_change(now_close, old_close)


# ─────────────────────────────────────────────────────────────────────────────
# 1. Relative Strength Leaders
# ─────────────────────────────────────────────────────────────────────────────

def compute_relative_strength(
    state: HyperliquidState,
    benchmark: str = "BTC",
    top_n: int = 20,
) -> list[dict]:
    """
    Compute relative strength vs a benchmark (default BTC) for all perp assets.

    RS is measured at 1h, 4h, and 24h intervals using candle close prices.
    RS Score = 0.55 * rs_24h + 0.30 * rs_4h + 0.15 * rs_1h + bonuses

    Returns top_n assets sorted by rs_score descending.
    """
    # Get benchmark returns from 1h candles
    bench_candles_1h = state.get_candles(benchmark, "1h", n=50)
    bench_4h_candles = state.get_candles(benchmark, "4h", n=20)

    bench_1h = _return_over_bars(bench_candles_1h, 1)
    bench_24h = _return_over_bars(bench_candles_1h, 24)

    # 4h return: prefer 4h candles, fall back to 1h candles (4 bars back)
    bench_4h = _return_over_bars(bench_4h_candles, 1) if len(bench_4h_candles) >= 2 else None
    if bench_4h is None:
        bench_4h = _return_over_bars(bench_candles_1h, 4)

    # If benchmark returns are all unavailable, we can't compute RS
    if bench_1h is None and bench_4h is None and bench_24h is None:
        print(f"[HL][signals] RS: benchmark {benchmark} has no candle data")
        return []

    # Default missing benchmark returns to 0
    bench_1h = bench_1h or 0.0
    bench_4h = bench_4h or 0.0
    bench_24h = bench_24h or 0.0

    perps = [
        a for a in state.perp_assets()
        if a.market_status == "active"
        and a.coin != benchmark
        and (not state.universe_allowlist or state.in_universe(a.coin))
    ]

    results = []
    for asset in perps:
        candles_1h = state.get_candles(asset.coin, "1h", n=50)
        candles_4h = state.get_candles(asset.coin, "4h", n=20)

        # Compute asset returns
        ret_1h = _return_over_bars(candles_1h, 1)
        ret_24h = _return_over_bars(candles_1h, 24)
        ret_4h = _return_over_bars(candles_4h, 1) if len(candles_4h) >= 2 else None
        if ret_4h is None:
            ret_4h = _return_over_bars(candles_1h, 4)

        # Need at least 24h return to be meaningful
        if ret_24h is None:
            continue

        # Default missing shorter-term returns
        ret_1h = ret_1h or 0.0
        ret_4h = ret_4h or 0.0

        # Relative strength = asset return - benchmark return
        rs_1h = ret_1h - bench_1h
        rs_4h = ret_4h - bench_4h
        rs_24h = ret_24h - bench_24h

        # RS Score: weighted combination
        rs_score = 0.55 * rs_24h + 0.30 * rs_4h + 0.15 * rs_1h

        # Optional bonuses for OI change and volume impulse
        oi_bonus = 0.0
        oi_change_pct = asset.oi_change_1h
        if oi_change_pct is not None and oi_change_pct > 0.005:
            # OI expanding alongside relative strength — conviction signal
            oi_bonus = min(oi_change_pct * 100, 2.0)  # cap at 2 points
            rs_score += oi_bonus

        vol_bonus = 0.0
        vol_impulse = asset.volume_impulse
        if vol_impulse is not None and vol_impulse > 1.5:
            # Elevated volume — participation signal
            vol_bonus = min((vol_impulse - 1.0) * 0.5, 1.5)  # cap at 1.5 points
            rs_score += vol_bonus

        results.append({
            "symbol": asset.coin,
            "display_name": asset.display_name,
            "mark_price": asset.mark_px,
            "return_1h": round(ret_1h, 4),
            "return_4h": round(ret_4h, 4),
            "return_24h": round(ret_24h, 4),
            "rs_1h": round(rs_1h, 4),
            "rs_4h": round(rs_4h, 4),
            "rs_24h": round(rs_24h, 4),
            "rs_score": round(rs_score, 4),
            "benchmark": benchmark,
            "benchmark_return_24h": round(bench_24h, 4),
            "oi_change_pct": round(oi_change_pct, 6) if oi_change_pct is not None else None,
            "volume_impulse": round(vol_impulse, 3) if vol_impulse is not None else None,
            "open_interest_usd": asset.open_interest_usd,
            "volume_24h": asset.day_ntl_vlm,
        })

    # Sort by RS score descending, return top N
    results.sort(key=lambda r: r["rs_score"], reverse=True)
    return results[:top_n]


# ─────────────────────────────────────────────────────────────────────────────
# 2. Order Book Pressure
# ─────────────────────────────────────────────────────────────────────────────

def _compute_book_metrics(
    book: dict,
    depth_window_bps: float = 20.0,
) -> Optional[dict]:
    """
    Compute order book pressure metrics from an L2 book snapshot.

    Args:
        book: L2 book dict with {"levels": [[bids], [asks]]}
        depth_window_bps: how many basis points from mid to include

    Returns dict with pressure metrics, or None if book is too thin.
    """
    levels = book.get("levels", [])
    if not levels or len(levels) < 2:
        return None

    bids_raw = levels[0] if len(levels) > 0 else []
    asks_raw = levels[1] if len(levels) > 1 else []

    if not bids_raw or not asks_raw:
        return None

    def _parse_level(lvl):
        if isinstance(lvl, dict):
            px, sz = lvl.get("px"), lvl.get("sz")
        elif isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
            px, sz = lvl[0], lvl[1]
        else:
            return None, None
        try:
            return float(px), float(sz)
        except (TypeError, ValueError):
            return None, None

    # Parse bid and ask levels
    bids = []
    for lvl in bids_raw:
        px, sz = _parse_level(lvl)
        if px is not None and sz is not None and px > 0:
            bids.append((px, sz))

    asks = []
    for lvl in asks_raw:
        px, sz = _parse_level(lvl)
        if px is not None and sz is not None and px > 0:
            asks.append((px, sz))

    if not bids or not asks:
        return None

    # Sort: bids descending, asks ascending
    bids.sort(key=lambda x: -x[0])
    asks.sort(key=lambda x: x[0])

    best_bid_px, best_bid_sz = bids[0]
    best_ask_px, best_ask_sz = asks[0]

    if best_ask_px <= best_bid_px:
        return None  # crossed book

    mid = (best_bid_px + best_ask_px) / 2.0
    if mid <= 0:
        return None

    spread_abs = best_ask_px - best_bid_px
    spread_pct = spread_abs / mid
    spread_bps = spread_pct * 10_000

    # Depth within window (bps from mid)
    window_frac = depth_window_bps / 10_000
    bid_floor = mid * (1 - window_frac)
    ask_ceil = mid * (1 + window_frac)

    bid_depth = sum(px * sz for px, sz in bids if px >= bid_floor)
    ask_depth = sum(px * sz for px, sz in asks if px <= ask_ceil)

    total_depth = bid_depth + ask_depth
    if total_depth <= 0:
        return None  # empty book in window

    # Imbalance: -1 (all asks) to +1 (all bids)
    imbalance = (bid_depth - ask_depth) / total_depth

    # Microprice: weighted average of best bid/ask by opposite size
    microprice = _safe_div(
        best_ask_px * best_bid_sz + best_bid_px * best_ask_sz,
        best_bid_sz + best_ask_sz,
        default=mid,
    )
    microprice_bias = _safe_div(microprice - mid, mid, default=0.0)

    # Pressure Score: combine imbalance + microprice bias + spread penalty
    # Imbalance component: [-1, 1] scaled to [-50, 50]
    imbalance_component = imbalance * 50.0

    # Microprice bias: typically small (e.g. -0.001 to 0.001), scale up
    microprice_component = microprice_bias * 10_000  # in bps, then scale
    microprice_component = max(-30, min(30, microprice_component))

    # Spread penalty: wider spread = lower confidence in the signal
    # 0 bps = no penalty, 30+ bps = max penalty
    spread_penalty = min(spread_bps / 30.0, 1.0) * 15.0

    pressure_score = imbalance_component + microprice_component
    if pressure_score > 0:
        pressure_score = max(0, pressure_score - spread_penalty)
    else:
        pressure_score = min(0, pressure_score + spread_penalty)

    # Direction tag
    if pressure_score > 10:
        direction = "Bid Support"
    elif pressure_score < -10:
        direction = "Ask Pressure"
    else:
        direction = "Balanced"

    return {
        "mid_price": round(mid, 6),
        "bid_depth": round(bid_depth, 2),
        "ask_depth": round(ask_depth, 2),
        "imbalance": round(imbalance, 4),
        "spread_abs": round(spread_abs, 6),
        "spread_bps": round(spread_bps, 2),
        "microprice": round(microprice, 6),
        "microprice_bias": round(microprice_bias, 6),
        "pressure_score": round(pressure_score, 2),
        "direction": direction,
    }


def compute_order_book_pressure(
    state: HyperliquidState,
    depth_window_bps: float = 20.0,
    top_n: int = 20,
) -> list[dict]:
    """
    Compute order book pressure metrics for all assets with L2 data.

    Returns top_n assets sorted by absolute pressure_score descending.
    """
    perps = [
        a for a in state.perp_assets()
        if a.market_status == "active"
        and (not state.universe_allowlist or state.in_universe(a.coin))
    ]

    results = []
    for asset in perps:
        book = state.get_book(asset.coin)
        if not book:
            continue

        metrics = _compute_book_metrics(book, depth_window_bps)
        if metrics is None:
            continue

        results.append({
            "symbol": asset.coin,
            "display_name": asset.display_name,
            "mark_price": asset.mark_px,
            "pressure_score": metrics["pressure_score"],
            "bid_depth": metrics["bid_depth"],
            "ask_depth": metrics["ask_depth"],
            "imbalance": metrics["imbalance"],
            "spread_bps": metrics["spread_bps"],
            "microprice_bias": metrics["microprice_bias"],
            "direction": metrics["direction"],
            "open_interest_usd": asset.open_interest_usd,
            "volume_24h": asset.day_ntl_vlm,
        })

    # Sort by absolute pressure score (strongest signal first)
    results.sort(key=lambda r: abs(r["pressure_score"]), reverse=True)
    return results[:top_n]


# ─────────────────────────────────────────────────────────────────────────────
# 3. OI Regime Shift
# ─────────────────────────────────────────────────────────────────────────────

_REGIME_LABELS = {
    "fresh_longs":       "Fresh Longs",
    "short_covering":    "Short Covering",
    "fresh_shorts":      "Fresh Shorts",
    "long_liquidation":  "Long Liquidation",
}

# Thresholds to filter noise — below these, changes are insignificant
_PRICE_DEADZONE_PCT = 0.15   # 0.15% price change minimum
_OI_DEADZONE_PCT = 0.003     # 0.3% OI change minimum (decimal)


def _classify_regime(price_change_pct: float, oi_change_pct: float) -> Optional[str]:
    """
    Classify the OI regime based on price and OI direction.

    Returns regime key or None if within deadzone.
    """
    # Apply deadzones — if both are tiny, it's noise
    price_significant = abs(price_change_pct) >= _PRICE_DEADZONE_PCT
    oi_significant = abs(oi_change_pct) >= _OI_DEADZONE_PCT

    if not price_significant and not oi_significant:
        return None

    # Use the dominant signal for classification even if the other is in deadzone
    price_up = price_change_pct >= 0
    oi_up = oi_change_pct >= 0

    if price_up and oi_up:
        return "fresh_longs"
    elif price_up and not oi_up:
        return "short_covering"
    elif not price_up and oi_up:
        return "fresh_shorts"
    else:
        return "long_liquidation"


def compute_oi_regime_shift(
    state: HyperliquidState,
    top_n: int = 20,
) -> list[dict]:
    """
    Classify each asset's OI regime and rank by significance.

    Uses 1h price change from candles and OI change from state.
    Boosts score for assets with abnormal volume.

    Returns top_n assets sorted by regime_score descending.
    """
    perps = [
        a for a in state.perp_assets()
        if a.market_status == "active"
        and (not state.universe_allowlist or state.in_universe(a.coin))
    ]

    results = []
    for asset in perps:
        # Get price change from candles (1h)
        candles_1h = state.get_candles(asset.coin, "1h", n=50)
        price_change_1h = _return_over_bars(candles_1h, 1)
        price_change_24h = _return_over_bars(candles_1h, 24)

        # Get OI change from asset model
        oi_change_1h = asset.oi_change_1h     # decimal (e.g. 0.015 = 1.5%)
        oi_change_24h = asset.open_interest_change_pct  # decimal

        # Need at least 1h data for classification
        if price_change_1h is None or oi_change_1h is None:
            continue

        # Primary classification uses 1h intervals
        regime = _classify_regime(price_change_1h, oi_change_1h)
        if regime is None:
            continue

        # Regime score: magnitude of price + OI moves
        # Larger moves = more significant regime signal
        price_mag = abs(price_change_1h)
        oi_mag = abs(oi_change_1h) * 100  # convert to % for comparable scale

        regime_score = price_mag * 0.5 + oi_mag * 0.5

        # Volume impulse bonus: abnormal volume amplifies the signal
        vol_impulse = asset.volume_impulse
        if vol_impulse is not None and vol_impulse > 1.3:
            vol_bonus = min((vol_impulse - 1.0) * 2.0, 5.0)  # cap at 5 points
            regime_score += vol_bonus

        # 24h alignment bonus: if 24h confirms 1h direction, boost score
        if price_change_24h is not None and oi_change_24h is not None:
            regime_24h = _classify_regime(price_change_24h, oi_change_24h)
            if regime_24h == regime:
                regime_score *= 1.15  # 15% boost for multi-timeframe alignment

        results.append({
            "symbol": asset.coin,
            "display_name": asset.display_name,
            "mark_price": asset.mark_px,
            "regime": _REGIME_LABELS.get(regime, regime),
            "regime_key": regime,
            "price_change_1h_pct": round(price_change_1h, 4),
            "price_change_24h_pct": round(price_change_24h, 4) if price_change_24h is not None else None,
            "oi_change_1h_pct": round(oi_change_1h * 100, 4),  # display as %
            "oi_change_24h_pct": round(oi_change_24h * 100, 4) if oi_change_24h is not None else None,
            "volume_impulse": round(vol_impulse, 3) if vol_impulse is not None else None,
            "regime_score": round(regime_score, 4),
            "open_interest_usd": asset.open_interest_usd,
            "volume_24h": asset.day_ntl_vlm,
            "funding_ann_pct": round((asset.funding or 0) * 8760 * 100, 2),
        })

    # Sort by regime_score descending
    results.sort(key=lambda r: r["regime_score"], reverse=True)
    return results[:top_n]


# ─────────────────────────────────────────────────────────────────────────────
# Combined endpoint builder
# ─────────────────────────────────────────────────────────────────────────────

def build_signal_payload(
    state: HyperliquidState,
    benchmark: str = "BTC",
    depth_window_bps: float = 20.0,
    top_n: int = 20,
) -> dict:
    """
    Build the complete signals response payload with all 3 signal modules.

    Returns:
    {
        "relative_strength_leaders": [...],
        "order_book_pressure": [...],
        "oi_regime_shift": [...],
        "as_of": "ISO timestamp",
        "metadata": {
            "benchmark": "BTC",
            "depth_window_bps": 20,
            "intervals": ["1h", "4h", "24h"],
            "top_n": 20,
        }
    }
    """
    rs_leaders = compute_relative_strength(state, benchmark=benchmark, top_n=top_n)
    ob_pressure = compute_order_book_pressure(state, depth_window_bps=depth_window_bps, top_n=top_n)
    oi_regime = compute_oi_regime_shift(state, top_n=top_n)

    return {
        "relative_strength_leaders": rs_leaders,
        "order_book_pressure": ob_pressure,
        "oi_regime_shift": oi_regime,
        "as_of": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "metadata": {
            "benchmark": benchmark,
            "depth_window_bps": depth_window_bps,
            "intervals": ["1h", "4h", "24h"],
            "top_n": top_n,
        },
    }
