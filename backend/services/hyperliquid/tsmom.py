"""
Time-Series Momentum (TSMOM) signal engine.

Computes momentum-based trading signals for canonical Hyperliquid perps using
market-specific multi-lookback z-score returns, funding-adjusted for carry cost,
and vol-targeted for position sizing.

Signal computation:
  1. Fetch 1d candle close prices from state
  2. Compute log returns
  3. For each available lookback: cumulative return / vol → z-score, clip [-2, 2]
  4. Renormalize across available lookbacks → s_raw
  5. Subtract funding cost adjustment → s_adj
  6. Vol-target position sizing → w_scaled

Reference: Gajesh2007/momentum-trading (Hyperliquid TSMOM strategy)
"""
from __future__ import annotations

import math
import time
from typing import Optional

from .categorizer import categorize_asset
from .models import ScreenerAsset
from .state import HyperliquidState

# Signal config
_CRYPTO_LOOKBACKS = [10, 30, 90]  # preserve the existing crypto model
_STOCK_LOOKBACKS = [5, 15, 30]    # coverage-aware ladder for newer listings
_Z_CLIP       = 2.0            # clip z-scores to [-2, +2]
_VOL_TARGET   = 0.40           # 40% annualized target portfolio vol
_FUND_HORIZON = 10             # funding cost horizon in days
_CRYPTO_MIN_BARS = 20          # preserve existing crypto eligibility
_STOCK_MIN_BARS = 16           # 15 returns: guarantees 5d + 15d windows
_SIGNAL_THRESH = 0.15          # |s_adj| threshold for long/short vs flat


def _std(values: list[float]) -> float:
    """Population std of a list. Returns 0 if fewer than 2 elements."""
    n = len(values)
    if n < 2:
        return 0.0
    mean = sum(values) / n
    return math.sqrt(sum((x - mean) ** 2 for x in values) / n)


def _log_return(a: float, b: float) -> Optional[float]:
    """log(b/a). Returns None if a or b <= 0."""
    if a <= 0 or b <= 0:
        return None
    return math.log(b / a)


def get_tsmom_candidates(
    state: HyperliquidState,
    market: str,
) -> list[ScreenerAsset]:
    """Return canonical active perp candidates for one TSMOM market."""
    candidates = [
        asset
        for asset in state.perp_assets()
        if asset.market_status == "active"
        and state.in_universe(asset.coin)
    ]
    if market == "crypto":
        # Preserve the original TSMOM universe exactly: main-DEX perps only.
        # Some HIP-3 listings classify as crypto, but were never in this panel.
        candidates = [
            asset
            for asset in candidates
            if ":" not in asset.coin
        ]
    elif market == "stocks":
        candidates = [
            asset
            for asset in candidates
            if categorize_asset(asset)[0] == "stocks_etfs"
        ]
    else:
        raise ValueError(f"unsupported TSMOM market: {market}")
    return sorted(candidates, key=lambda asset: -(asset.day_ntl_vlm or 0))


def _completed_stock_candles(candles: list[dict], now_ms: int) -> list[dict]:
    """Exclude the currently forming daily bar from stock history coverage."""
    return [
        candle
        for candle in candles
        if int(candle.get("T") or 0) < now_ms
    ]


def compute_tsmom_signals(
    state: HyperliquidState,
    top_n: int = 60,
    market: str = "crypto",
) -> dict:
    """
    Compute TSMOM signals for canonical crypto or stock perps by volume.

    Returns:
      {
        "signals": [
          {
            "coin": str,
            "s_raw": float,           # raw avg z-score, clipped -2..+2
            "s_adj": float,           # funding-adjusted signal
            "sigma": float,           # annualized realized vol (%)
            "funding_bps": float,     # current hourly funding (bps)
            "funding_ann_pct": float, # annualized funding (%)
            "w_scaled": float,        # vol-targeted weight (%)
            "side": "long"|"short"|"flat",
            "momentum_10d": float|None,   # 10d cum log return (%)
            "momentum_30d": float|None,   # 30d cum log return (%)
            "bars_used": int,
          },
          ...
        ],
        "meta": {
          "total_signals": int,
          "long_count": int,
          "short_count": int,
          "flat_count": int,
          "generated_at": str,
        }
      }
    """
    lookbacks = _CRYPTO_LOOKBACKS if market == "crypto" else _STOCK_LOOKBACKS
    min_bars = _CRYPTO_MIN_BARS if market == "crypto" else _STOCK_MIN_BARS
    perps = get_tsmom_candidates(state, market)[:max(0, top_n)]

    results = []
    insufficient_history_count = 0
    no_signal_count = 0
    now_ms = int(time.time() * 1000)

    for asset in perps:
        candles = state.get_candles(asset.coin, "1d", n=120)
        if market == "stocks":
            candles = _completed_stock_candles(candles, now_ms)
        if len(candles) < min_bars:
            insufficient_history_count += 1
            continue

        closes = [float(c["c"]) for c in candles if c.get("c")]
        if len(closes) < min_bars:
            insufficient_history_count += 1
            continue

        # Log returns
        log_rets: list[float] = []
        for i in range(1, len(closes)):
            lr = _log_return(closes[i - 1], closes[i])
            if lr is not None:
                log_rets.append(lr)

        if len(log_rets) < min_bars - 1:
            insufficient_history_count += 1
            continue

        # Realized vol — use last 30 days (or all available)
        vol_window = min(30, len(log_rets))
        sigma_daily = _std(log_rets[-vol_window:])
        sigma_ann   = sigma_daily * math.sqrt(365)

        if sigma_ann < 1e-8:
            no_signal_count += 1
            continue

        # Multi-lookback z-scores
        z_scores: dict[int, float] = {}
        momentum_by_lookback: dict[int, float] = {}
        mom_10d: Optional[float] = None
        mom_30d: Optional[float] = None

        for lb in lookbacks:
            if len(log_rets) < lb:
                continue
            ret_lb = sum(log_rets[-lb:])
            vol_lb = _std(log_rets[-lb:]) * math.sqrt(lb) if len(log_rets[-lb:]) > 2 else sigma_daily * math.sqrt(lb)
            if vol_lb < 1e-10:
                continue
            z = max(-_Z_CLIP, min(_Z_CLIP, ret_lb / vol_lb))
            z_scores[lb] = z
            momentum_by_lookback[lb] = round(ret_lb * 100, 2)

            # Store human-readable momentum
            if lb == 10:
                mom_10d = round(ret_lb * 100, 2)
            elif lb == 30:
                mom_30d = round(ret_lb * 100, 2)

        if not z_scores:
            no_signal_count += 1
            continue

        # Equal intended horizon weights, renormalized over available windows.
        # An unavailable long window is omitted; it never contributes a zero.
        available_weight = 1.0 / len(z_scores)
        s_raw = sum(z * available_weight for z in z_scores.values())

        # Funding cost adjustment
        # Positive funding = longs pay shorts → penalizes long signal
        fund = asset.funding or 0.0
        # Daily funding cost for a long = fund * 24 (hourly rate × 24)
        # Over _FUND_HORIZON days: fund * 24 * _FUND_HORIZON
        # Normalized by daily vol: adjustment = (fund * 24 * horizon) / sigma_daily
        fund_10d = fund * 24 * _FUND_HORIZON
        fund_adj_raw = fund_10d / sigma_daily if sigma_daily > 0 else 0.0
        # Cap adjustment at 1 vol unit
        fund_adj = max(-1.0, min(1.0, fund_adj_raw))

        s_adj = s_raw - fund_adj

        # Vol-targeted weight: w = s_adj * vol_target / sigma_ann, capped at ±20%
        w_raw    = (s_adj * _VOL_TARGET / sigma_ann) if sigma_ann > 0 else 0.0
        w_scaled = max(-0.20, min(0.20, w_raw))

        side: str
        if s_adj > _SIGNAL_THRESH:
            side = "long"
        elif s_adj < -_SIGNAL_THRESH:
            side = "short"
        else:
            side = "flat"

        results.append({
            "coin":           asset.coin,
            "canonical_coin_id": asset.canonical_coin_id or asset.coin,
            "display_symbol": asset.display_symbol or asset.display_name or asset.coin,
            "name":           asset.display_name,
            "market":         market,
            "s_raw":          round(s_raw, 3),
            "s_adj":          round(s_adj, 3),
            "sigma":          round(sigma_ann * 100, 1),       # annualized vol as %
            "funding_bps":    round(fund * 10_000, 3),         # bps per hour
            "funding_ann_pct": round(fund * 8760 * 100, 2),    # annualized funding %
            "w_scaled":       round(w_scaled * 100, 2),        # target weight %
            "side":           side,
            "momentum_10d":   mom_10d,
            "momentum_30d":   mom_30d,
            "bars_used":      len(closes),
            "completed_bars": len(closes),
            "lookbacks_available": list(z_scores),
            "lookbacks_unavailable": [
                lb for lb in lookbacks if lb not in z_scores
            ],
            "lookback_weights": {
                str(lb): round(available_weight, 6) for lb in z_scores
            },
            "momentum_by_lookback": {
                str(lb): value for lb, value in momentum_by_lookback.items()
            },
            "z_scores_by_lookback": {
                str(lb): round(value, 6) for lb, value in z_scores.items()
            },
            "volume_usd": asset.day_ntl_vlm,
            "funding_available": asset.funding is not None,
        })

    # Sort by absolute adjusted signal strength (strongest first)
    results.sort(key=lambda x: abs(x["s_adj"]), reverse=True)

    long_count  = sum(1 for r in results if r["side"] == "long")
    short_count = sum(1 for r in results if r["side"] == "short")
    flat_count  = sum(1 for r in results if r["side"] == "flat")

    return {
        "signals": results,
        "meta": {
            "total_signals": len(results),
            "long_count":    long_count,
            "short_count":   short_count,
            "flat_count":    flat_count,
            "generated_at":  time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "market":        market,
            "lookbacks":     lookbacks,
            "minimum_bars":  min_bars,
            "candidate_count": len(perps),
            "insufficient_history_count": insufficient_history_count,
            "no_signal_count": no_signal_count,
        },
    }
