"""
backend/services/stage_analysis.py

Weinstein Stage Analysis — deterministic, no LLM calls.

Classifies any symbol or theme index into one of the four Weinstein stages
(1 Base, 2 Advance, 3 Top/Range, 4 Decline) plus transition labels
(1→2 Breakout Watch, 3→4 Danger Zone).

Core reference: 30-week moving average of weekly closing prices.
No new API calls — works entirely from cached daily price bars already
fetched by the theme RS service.
"""
from __future__ import annotations

import statistics
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Optional, Union

# ── Types & constants ───────────────────────────────────────────────────────────

StageKey = Union[int, str]   # 1, "12", "2b", 2, "3m", 3, "34", 4

# ── Compact taxonomy — 7 distinct human-readable labels ──────────────────────
# "2b" = S2 Breakout  (confirmed breakout from a base / downtrend)
# 2    = S2-S3 Advance (established advance, trend healthy)
# "3m" = S3 Momentum  (extended / late-stage advance, risk rising)
# 3    = S3-S4 Top    (topping / distribution — MA flattening, prior uptrend)
# "34" = S3-S4 Top    (broke below flat-to-falling MA after an uptrend)
STAGE_LABELS: dict[StageKey, str] = {
    1:    "S1 Base",
    "12": "S1-2 Watch",
    "2b": "S2 Breakout",
    2:    "S2-S3 Advance",
    "3m": "S3 Momentum",
    3:    "S3-S4 Top",
    "34": "S3-S4 Top",
    4:    "S4 Decline",
}

STAGE_INT: dict[StageKey, int] = {
    1: 1, "12": 1, "2b": 2, 2: 2, "3m": 3, 3: 3, "34": 3, 4: 4
}

_MIN_WEEKLY_BARS = 35    # 30w MA + 5 weeks of context minimum


# ── Weekly bar aggregation ──────────────────────────────────────────────────────

def weekly_bars_from_daily(daily_bars: list[dict]) -> list[dict]:
    """
    Aggregate daily close/volume bars into weekly bars.
    Weekly close = last trading day of each ISO calendar week.
    Returns list sorted oldest → newest.
    """
    if not daily_bars:
        return []

    weeks: OrderedDict[str, dict] = OrderedDict()

    for bar in sorted(daily_bars, key=lambda b: str(b.get("date", ""))[:10]):
        date_str = str(bar.get("date", ""))[:10]
        if len(date_str) < 10:
            continue
        try:
            d = datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            continue

        close = bar.get("close")
        if close is None:
            continue
        try:
            close = float(close)
        except (TypeError, ValueError):
            continue

        volume = float(bar.get("volume") or 0)
        year, week, _ = d.isocalendar()
        key = f"{year:04d}W{week:02d}"

        if key not in weeks:
            weeks[key] = {"date": date_str, "close": close, "volume": volume}
        else:
            w = weeks[key]
            w["date"]    = date_str   # keep last date of week
            w["close"]   = close      # weekly close = last trading day
            w["volume"] += volume

    return list(weeks.values())


# ── Math helpers ────────────────────────────────────────────────────────────────

def _sma(values: list[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


def _build_synthetic_basket(weekly_map: dict[str, list[dict]]) -> list[dict]:
    """
    Equal-weight synthetic weekly index from multiple symbols' weekly bar lists.
    Returns a rebased price series (starting near 100) sorted oldest → newest.
    """
    if not weekly_map:
        return []

    syms = list(weekly_map.keys())

    date_close: dict[str, dict[str, float]] = {}
    for sym, bars in weekly_map.items():
        for bar in bars:
            dt = bar["date"]
            if dt not in date_close:
                date_close[dt] = {}
            date_close[dt][sym] = float(bar["close"])

    full_dates = sorted(
        dt for dt, vals in date_close.items()
        if all(s in vals for s in syms)
    )

    if len(full_dates) < _MIN_WEEKLY_BARS:
        thresh = max(1, len(syms) // 2)
        full_dates = sorted(
            dt for dt, vals in date_close.items()
            if len(vals) >= thresh
        )

    if len(full_dates) < 2:
        return []

    result: list[dict] = [{"date": full_dates[0], "close": 100.0, "volume": 0.0}]
    current = 100.0

    for i in range(1, len(full_dates)):
        dt      = full_dates[i]
        prev_dt = full_dates[i - 1]
        returns = []
        for sym in syms:
            cur  = date_close[dt].get(sym)
            prev = date_close[prev_dt].get(sym)
            if cur and prev and prev > 0:
                returns.append((cur - prev) / prev)
        if returns:
            current = round(current * (1 + statistics.mean(returns)), 4)
        result.append({"date": dt, "close": current, "volume": 0.0})

    return result


# ── Stage classification ────────────────────────────────────────────────────────

def _classify_stage(
    price_vs_ma: float,
    ma_slope: float,
    prior_trend_pct: Optional[float],
    weeks_above_ma: int,
    rs_trend: str,
    volume_ratio: Optional[float],
) -> tuple[StageKey, bool, bool]:
    """
    Returns (stage_key, breakout_watch, danger_zone).

    Implements the six Weinstein stage/transition labels using:
      price_vs_ma   = % above/below 30w MA (positive = above)
      ma_slope      = 4-week % slope of 30w MA (positive = rising)
      prior_trend_pct = 26-week price change (prior trend context)
      weeks_above_ma  = # of last 8 weeks where close > that week's 30w MA
      rs_trend      = "rising" | "flat" | "falling"
      volume_ratio  = recent 4w avg vol / prior 13w avg vol (or None)
    """
    ma_rising  = ma_slope >  0.25
    ma_falling = ma_slope < -0.25
    ma_flat    = not ma_rising and not ma_falling

    price_above = price_vs_ma > 0
    price_near  = -6.0 <= price_vs_ma <= 8.0

    prior_up   = prior_trend_pct is not None and prior_trend_pct >  12.0
    prior_down = prior_trend_pct is not None and prior_trend_pct < -10.0

    # ── S4 Decline: price below a declining 30w MA ───────────────────────────
    if not price_above and ma_falling:
        return 4, False, False

    # ── S3-S4 Top ("34"): broke below flat-to-falling MA after an uptrend ───
    if not price_above and ma_flat and prior_up:
        return "34", False, True

    # ── S3-S4 Top (3): topping — prior uptrend, MA flattening ────────────────
    if price_near and ma_flat and prior_up:
        return 3, False, False

    if price_above and ma_flat and prior_up:
        return 3, False, False

    # ── S3 Momentum ("3m"): very extended above a rising MA, prior uptrend ───
    # Must come BEFORE the general S2-S3 Advance check so highly-extended
    # symbols are not miscalled S2-S3 Advance.
    # Threshold: > 20% above MA AND already had a prior uptrend = late-stage.
    if price_above and ma_rising and price_vs_ma > 20.0 and prior_up:
        return "3m", False, False

    # ── S2 Breakout ("2b"): confirmed breakout from a downtrend base ─────────
    # Price has crossed above the 30w MA coming from a prior downtrend context.
    # Replaces the old "12" (Watch) condition where price_above + prior_down.
    if price_above and (ma_flat or ma_rising) and prior_down:
        return "2b", True, False

    # ── S2-S3 Advance (2): established advance (rising MA, above, no base ctx)─
    if price_above and ma_rising:
        return 2, False, False

    # S2-S3 Advance continuation — flat MA, still above, not from downtrend
    if price_above and ma_flat and not prior_down and weeks_above_ma >= 5:
        return 2, False, False

    # ── S1-2 Watch ("12"): price near MA, watching for breakout ──────────────
    # Price is still near (but not clearly above) the MA — not yet confirmed.
    if price_near and ma_flat and prior_down and rs_trend in ("rising", "flat"):
        return "12", True, False

    if price_near and ma_rising and prior_down:
        return "12", True, False

    # ── S1 Base (1): basing after a decline ───────────────────────────────────
    if not price_above and (ma_flat or ma_slope > -0.5) and prior_down:
        return 1, False, False

    if price_near and ma_flat and not prior_up:
        return 1, False, False

    # ── Fallback: continuous score ────────────────────────────────────────────
    score = _compute_stage_score(price_vs_ma, ma_slope, weeks_above_ma, rs_trend)
    if score >= 72:
        return 2, False, False     # S2-S3 Advance
    if score >= 55:
        return "2b", True, False   # S2 Breakout (was "12" Watch in old code)
    if score >= 38:
        stage = 1 if not prior_up else 3
        return stage, False, False
    if score >= 22:
        return "34", False, True
    return 4, False, False


def _compute_stage_score(
    price_vs_ma: float,
    ma_slope: float,
    weeks_above_ma: int,
    rs_trend: str,
) -> float:
    """
    Continuous 0-100 score: higher = stronger uptrend (Stage 2-like).

    Components:
      Position (35 pts): price vs 30w MA
      Trend    (30 pts): 30w MA slope
      Consist  (15 pts): weeks above MA of last 8
      RS       (20 pts): RS vs SPY trend
    """
    # ── Position ─────────────────────────────────────────────────────────────
    if price_vs_ma >= 10:
        pos = 35.0
    elif price_vs_ma >= 5:
        pos = 28.0 + (price_vs_ma - 5) / 5 * 7
    elif price_vs_ma >= 0:
        pos = 17.5 + price_vs_ma / 5 * 10.5
    elif price_vs_ma >= -5:
        pos = 8.75 + (price_vs_ma + 5) / 5 * 8.75
    else:
        pos = max(0.0, 8.75 * max(0, (price_vs_ma + 15) / 10))

    # ── Trend ─────────────────────────────────────────────────────────────────
    if ma_slope >= 1.0:
        trend = 30.0
    elif ma_slope >= 0.3:
        trend = 21.0 + (ma_slope - 0.3) / 0.7 * 9
    elif ma_slope >= 0.0:
        trend = 15.0 + ma_slope / 0.3 * 6
    elif ma_slope >= -0.3:
        trend = 9.0 + (ma_slope + 0.3) / 0.3 * 6
    elif ma_slope >= -1.0:
        trend = max(0.0, 9.0 * max(0, (ma_slope + 1.0) / 0.7))
    else:
        trend = 0.0

    # ── Consistency ───────────────────────────────────────────────────────────
    consistency = (weeks_above_ma / 8) * 15.0

    # ── RS ────────────────────────────────────────────────────────────────────
    rs = 20.0 if rs_trend == "rising" else (10.0 if rs_trend == "flat" else 0.0)

    return round(min(100.0, max(0.0, pos + trend + consistency + rs)), 1)


def _confidence_level(
    n_bars: int,
    price_vs_ma: float,
    ma_slope: float,
    weeks_above_ma: int,
) -> str:
    if n_bars < 40:
        return "low"
    if abs(price_vs_ma) > 6.0 and abs(ma_slope) > 0.5:
        return "high"
    if abs(price_vs_ma) > 3.0 and abs(ma_slope) > 0.3:
        return "high" if (weeks_above_ma >= 6 or weeks_above_ma <= 2) else "medium"
    if abs(price_vs_ma) < 1.0 or abs(ma_slope) < 0.1:
        return "low"
    return "medium"


def _build_stage_reason(
    stage_key: StageKey,
    price_vs_ma: float,
    ma_slope: float,
    rs_trend: str,
    prior_trend_pct: Optional[float],
    weeks_above_ma: int,
    volume_ratio: Optional[float],
) -> str:
    sign   = "+" if price_vs_ma >= 0 else ""
    ma_dir = "rising" if ma_slope > 0.25 else ("falling" if ma_slope < -0.25 else "flat")
    vol_str = f" Vol {volume_ratio:.1f}× avg." if volume_ratio is not None else ""

    if stage_key == "2b":
        prior_str = f" Prior 26w: {prior_trend_pct:+.0f}%." if prior_trend_pct is not None else ""
        return (
            f"Confirmed breakout from base. Price {sign}{price_vs_ma:.1f}% above 30-week MA "
            f"({ma_dir} slope).{prior_str} RS vs SPY: {rs_trend}.{vol_str}"
        )
    if stage_key == 2:
        return (
            f"Established advance. Price {sign}{price_vs_ma:.1f}% above a {ma_dir} 30-week MA "
            f"({weeks_above_ma}/8 recent weeks above MA). RS vs SPY: {rs_trend}.{vol_str}"
        )
    if stage_key == "3m":
        return (
            f"Extended momentum. Price {sign}{price_vs_ma:.1f}% above rising 30-week MA — "
            f"late-stage advance ({weeks_above_ma}/8 weeks above MA). "
            f"RS vs SPY: {rs_trend}. Trend intact but risk rising.{vol_str}"
        )
    if stage_key == "12":
        prior_str = f" Prior 26w: {prior_trend_pct:+.0f}%." if prior_trend_pct is not None else ""
        return (
            f"Base tightening. Price {sign}{price_vs_ma:.1f}% vs 30-week MA ({ma_dir} slope)."
            f"{prior_str} RS vs SPY: {rs_trend}. Watch for confirmed breakout.{vol_str}"
        )
    if stage_key == 1:
        prior_str = f" Prior 26w: {prior_trend_pct:+.0f}%." if prior_trend_pct is not None else ""
        return (
            f"Basing / bottoming. Price {sign}{price_vs_ma:.1f}% vs flat 30-week MA.{prior_str}"
            f" RS vs SPY: {rs_trend}. No breakout signal yet."
        )
    if stage_key == 3:
        prior_str = f" Prior 26w gain: {prior_trend_pct:+.0f}%." if prior_trend_pct is not None else ""
        return (
            f"Topping / distribution. 30-week MA {ma_dir}, price {sign}{price_vs_ma:.1f}% vs MA.{prior_str}"
            f" RS vs SPY: {rs_trend}. Watch for breakdown."
        )
    if stage_key == "34":
        return (
            f"Distribution risk — broke below 30-week MA ({sign}{price_vs_ma:.1f}%). "
            f"MA slope: {ma_slope:+.2f}%/4w. RS vs SPY: {rs_trend}. Risk of S4 decline.{vol_str}"
        )
    # S4 Decline
    return (
        f"Downtrend confirmed. Price {sign}{price_vs_ma:.1f}% below a {ma_dir} 30-week MA. "
        f"RS vs SPY: {rs_trend}. Avoid new longs."
    )


# ── Fallback helpers ────────────────────────────────────────────────────────────

def _empty_signals() -> dict:
    return {
        "price_vs_30w_ma_pct":       None,
        "ma_30w_slope_pct":          None,
        "rs_vs_spy_trend":           None,
        "rs_vs_spy_8w_pct":          None,
        "volume_ratio":              None,
        "weeks_above_30w_ma_of_8":   None,
        "prior_26w_trend_pct":       None,
        "breakout_watch":            False,
        "danger_zone":               False,
        "breadth_above_30w_ma_pct":  None,
        "breadth_rising_30w_ma_pct": None,
    }


def _fallback_result(reason: str, now_ts: str) -> dict:
    return {
        "stage":            None,
        "stage_label":      "Unknown",
        "stage_score":      None,
        "stage_confidence": "low",
        "stage_reason":     f"Stage unavailable: {reason}.",
        "stage_signals":    _empty_signals(),
        "stage_updated_at": now_ts,
        "stage_source":     "fallback",
    }


def _breadth_only_result(
    breadth_above: float,
    breadth_rising: Optional[float],
    now_ts: str,
) -> dict:
    """Stage estimated from member breadth when no representative price series exists."""
    if breadth_above >= 65 and (breadth_rising or 0) >= 50:
        sk: StageKey = 2      # S2-S3 Advance
    elif breadth_above >= 55:
        sk = "2b"             # S2 Breakout
    elif breadth_above >= 40:
        sk = "12"             # S1-2 Watch
    elif breadth_above >= 28:
        sk = 3                # S3-S4 Top
    elif breadth_above >= 15:
        sk = "34"             # S3-S4 Top (distribution)
    else:
        sk = 4                # S4 Decline

    sigs = _empty_signals()
    sigs["breadth_above_30w_ma_pct"]  = breadth_above
    sigs["breadth_rising_30w_ma_pct"] = breadth_rising

    return {
        "stage":            STAGE_INT[sk],
        "stage_label":      STAGE_LABELS[sk],
        "stage_score":      round(breadth_above, 1),
        "stage_confidence": "low",
        "stage_reason":     (
            f"Stage estimated from member breadth: {breadth_above:.0f}% of members "
            f"above 30-week MA. No representative price index available."
        ),
        "stage_signals":    sigs,
        "stage_updated_at": now_ts,
        "stage_source":     "member_breadth_only",
    }


# ── Public analysis functions ───────────────────────────────────────────────────

def analyze_symbol_stage(
    weekly_bars: list[dict],
    spy_weekly_bars: Optional[list[dict]] = None,
    breadth_above_30w: Optional[float] = None,
    breadth_rising_30w: Optional[float] = None,
    source: str = "etf_proxy",
) -> dict:
    """
    Compute Weinstein stage analysis from weekly price bars.

    Parameters
    ----------
    weekly_bars       : weekly OHLCV bars (oldest → newest), each with {"date", "close"}
    spy_weekly_bars   : SPY weekly bars for RS calculation (optional)
    breadth_above_30w : % of basket members above their 30w MA (optional, for baskets)
    breadth_rising_30w: % of basket members with rising 30w MA (optional)
    source            : "etf_proxy" | "synthetic_basket" | "member_breadth_only" | "fallback"

    Returns structured stage dict.
    """
    now_ts = datetime.now(timezone.utc).isoformat()

    if len(weekly_bars) < _MIN_WEEKLY_BARS:
        return _fallback_result("insufficient_weekly_bars", now_ts)

    closes  = [float(b["close"]) for b in weekly_bars]
    volumes = [float(b.get("volume") or 0) for b in weekly_bars]
    N = len(closes)

    # ── 30-week MA ─────────────────────────────────────────────────────────────
    ma30w = _sma(closes, 30)
    if not ma30w or ma30w <= 0:
        return _fallback_result("ma30w_compute_failed", now_ts)

    # ── 4-week slope of 30w MA ─────────────────────────────────────────────────
    ma30w_4w_ago = _sma(closes[:-4], 30) if N >= 34 else None
    ma30w_slope = (
        round((ma30w - ma30w_4w_ago) / ma30w_4w_ago * 100, 2)
        if (ma30w_4w_ago and ma30w_4w_ago > 0) else 0.0
    )

    # ── Price vs 30w MA ────────────────────────────────────────────────────────
    current_close = closes[-1]
    price_vs_ma   = round((current_close - ma30w) / ma30w * 100, 2)

    # ── Weeks above MA (last 8) ────────────────────────────────────────────────
    weeks_above_ma = 0
    for i in range(1, 9):
        idx = N - i
        if idx < 30:
            break
        c  = closes[idx]
        ma = _sma(closes[:idx], 30)
        if ma and c > ma:
            weeks_above_ma += 1

    # ── Prior 26-week trend ────────────────────────────────────────────────────
    prior_trend_pct: Optional[float] = None
    if N >= 26 and closes[-26] > 0:
        prior_trend_pct = round((current_close - closes[-26]) / closes[-26] * 100, 2)

    # ── RS vs SPY (8-week relative performance) ────────────────────────────────
    rs_vs_spy_8w: Optional[float] = None
    rs_trend = "flat"
    if spy_weekly_bars and len(spy_weekly_bars) >= 8:
        spy_closes = [float(b["close"]) for b in spy_weekly_bars]
        if N >= 8 and closes[-8] > 0 and spy_closes[-8] > 0:
            theme_8w = (closes[-1] / closes[-8] - 1) * 100
            spy_8w   = (spy_closes[-1] / spy_closes[-8] - 1) * 100
            rs_vs_spy_8w = round(theme_8w - spy_8w, 2)
            rs_trend = (
                "rising"  if rs_vs_spy_8w >  2.0 else
                "falling" if rs_vs_spy_8w < -2.0 else
                "flat"
            )

    # ── Volume ratio (recent 4w vs prior 13w) ─────────────────────────────────
    volume_ratio: Optional[float] = None
    if any(v > 0 for v in volumes):
        v_recent = statistics.mean(volumes[-4:]) if len(volumes) >= 4 else None
        v_prior  = statistics.mean(volumes[-17:-4]) if len(volumes) >= 17 else None
        if v_recent and v_prior and v_prior > 0:
            volume_ratio = round(v_recent / v_prior, 2)

    # ── Classify stage ─────────────────────────────────────────────────────────
    stage_key, breakout_watch, danger_zone = _classify_stage(
        price_vs_ma, ma30w_slope, prior_trend_pct,
        weeks_above_ma, rs_trend, volume_ratio,
    )

    stage_score = _compute_stage_score(price_vs_ma, ma30w_slope, weeks_above_ma, rs_trend)
    confidence  = _confidence_level(N, price_vs_ma, ma30w_slope, weeks_above_ma)
    reason      = _build_stage_reason(
        stage_key, price_vs_ma, ma30w_slope, rs_trend,
        prior_trend_pct, weeks_above_ma, volume_ratio,
    )

    return {
        "stage":            STAGE_INT[stage_key],
        "stage_label":      STAGE_LABELS[stage_key],
        "stage_score":      stage_score,
        "stage_confidence": confidence,
        "stage_reason":     reason,
        "stage_signals": {
            "price_vs_30w_ma_pct":       price_vs_ma,
            "ma_30w_slope_pct":          ma30w_slope,
            "rs_vs_spy_trend":           rs_trend,
            "rs_vs_spy_8w_pct":          rs_vs_spy_8w,
            "volume_ratio":              volume_ratio,
            "weeks_above_30w_ma_of_8":   weeks_above_ma,
            "prior_26w_trend_pct":       prior_trend_pct,
            "breakout_watch":            breakout_watch,
            "danger_zone":               danger_zone,
            "breadth_above_30w_ma_pct":  breadth_above_30w,
            "breadth_rising_30w_ma_pct": breadth_rising_30w,
        },
        "stage_updated_at": now_ts,
        "stage_source":     source,
    }


def _empty_technical_metrics() -> dict:
    """Return all technical metric keys set to None / neutral defaults."""
    return {
        "sma_20": None, "sma_50": None, "sma_200": None,
        "pct_vs_sma_20": None, "pct_vs_sma_50": None, "pct_vs_sma_200": None,
        "ma_stack": None,
        "extension_risk": "neutral",
        "high_52w": None, "low_52w": None,
        "pct_from_52w_high": None, "pct_from_52w_low": None,
        "range_position_52w": None,
        "high_20d": None, "high_50d": None,
        "breakout_signal": None,
        "entry_zone": "neutral",
        "avg_volume_20d": None,
        "accumulation_days_20d": None, "distribution_days_20d": None,
        "accumulation_distribution_score": None,
        "accumulation_distribution_signal": "neutral",
        "atr_14": None, "atr_14_pct": None,
        "range_20d_pct": None,
        "squeeze_signal": None,
        "roc_20d": None, "roc_50d": None,
        "momentum_trend": "neutral",
        "technical_state": "neutral",
        "technical_timing_score": None,
        "missing_metric_reasons": [],
    }


def compute_technical_metrics(
    daily_bars: list[dict],
    current_price: Optional[float] = None,
) -> dict:
    """
    Compute daily-bar technical timing metrics from cached OHLCV history.

    daily_bars    : list of {date, close, [open, high, low, volume]} oldest→newest.
    current_price : live-quote override for "today's close" (e.g. from Tradier).

    Returns a flat dict with all metric keys.  Keys are always present; missing
    data yields None or the neutral label string.  Never raises.

    Guardrails
    ----------
    < 20 bars  → minimal metrics only (most are None)
    < 50 bars  → no 50D SMA / ROC
    < 200 bars → no 200D SMA
    < 252 bars → no 52W high/low metrics
    """
    null = _empty_technical_metrics()
    if not daily_bars:
        null["missing_metric_reasons"] = ["no_bars"]
        return null

    try:
        bars_s = sorted(daily_bars, key=lambda b: str(b.get("date", ""))[:10])
        n = len(bars_s)

        closes  = [float(b["close"]) for b in bars_s]
        highs   = [float(b["high"])   if b.get("high")   is not None else None for b in bars_s]
        lows    = [float(b["low"])    if b.get("low")    is not None else None for b in bars_s]
        volumes = [float(b["volume"]) if b.get("volume") is not None else None for b in bars_s]

        has_ohlcv  = any(h is not None for h in highs) and any(l is not None for l in lows)
        has_volume = any(v is not None for v in volumes)

        price = float(current_price) if current_price is not None else closes[-1]
        missing: list[str] = []
        out: dict = {}

        # ── SMA helpers ──────────────────────────────────────────────────────────
        def _sma_d(period: int) -> Optional[float]:
            if n < period:
                return None
            return round(sum(closes[-period:]) / period, 4)

        def _pct_vs(ma_val: Optional[float]) -> Optional[float]:
            if ma_val is None or ma_val <= 0:
                return None
            return round((price - ma_val) / ma_val * 100, 2)

        # ── Trend / MA ───────────────────────────────────────────────────────────
        s20  = _sma_d(20)  if n >= 20  else None
        s50  = _sma_d(50)  if n >= 50  else None
        s200 = _sma_d(200) if n >= 200 else None

        out["sma_20"]        = s20
        out["sma_50"]        = s50
        out["sma_200"]       = s200
        out["pct_vs_sma_20"] = _pct_vs(s20)
        out["pct_vs_sma_50"] = _pct_vs(s50)
        out["pct_vs_sma_200"]= _pct_vs(s200)

        if s20 is not None and s50 is not None and s200 is not None:
            out["ma_stack"] = "bull" if s20 > s50 > s200 else ("bear" if s20 < s50 < s200 else "mixed")
        elif s20 is not None and s50 is not None:
            out["ma_stack"] = "bull" if s20 > s50 else ("bear" if s20 < s50 else "mixed")
        else:
            out["ma_stack"] = None
        if n < 50:  missing.append("no_50d_sma")
        if n < 200: missing.append("no_200d_sma")

        pct20 = out["pct_vs_sma_20"]
        pct50 = out["pct_vs_sma_50"]

        # ── Extension Risk ───────────────────────────────────────────────────────
        if pct20 is None and pct50 is None:
            out["extension_risk"] = "neutral"
        elif s50 is not None and price < s50 and (s200 is None or price < s200):
            out["extension_risk"] = "broken"
        elif pct20 is not None and pct50 is not None:
            if pct20 > 15 or pct50 > 35:
                out["extension_risk"] = "overheated"
            elif pct20 > 8 or pct50 > 20:
                out["extension_risk"] = "extended"
            elif -8 <= pct20 <= 2 and out.get("ma_stack") in ("bull", "mixed"):
                out["extension_risk"] = "pullback_buy_zone"
            elif pct50 is not None and -10 <= pct50 <= 2 and out.get("ma_stack") in ("bull", "mixed"):
                out["extension_risk"] = "pullback_buy_zone"
            else:
                out["extension_risk"] = "healthy"
        else:
            out["extension_risk"] = "neutral"

        # ── 52W High/Low (highest/lowest close over 252 bars) ───────────────────
        if n >= 252:
            tail = closes[-252:]
            h52w, l52w = max(tail), min(tail)
            out["high_52w"] = round(h52w, 4)
            out["low_52w"]  = round(l52w, 4)
            if h52w > l52w > 0:
                out["pct_from_52w_high"]  = round((price - h52w) / h52w * 100, 2)
                out["pct_from_52w_low"]   = round((price - l52w) / l52w * 100, 2)
                out["range_position_52w"] = round((price - l52w) / (h52w - l52w) * 100, 1)
            else:
                out["pct_from_52w_high"] = out["pct_from_52w_low"] = out["range_position_52w"] = None
        else:
            out["high_52w"] = out["low_52w"] = None
            out["pct_from_52w_high"] = out["pct_from_52w_low"] = out["range_position_52w"] = None
            missing.append("no_52w_metrics")

        pct_h52 = out.get("pct_from_52w_high")

        # ── High 20D / 50D (close-based) ─────────────────────────────────────────
        out["high_20d"] = round(max(closes[-20:]), 4) if n >= 20 else None
        out["high_50d"] = round(max(closes[-50:]), 4) if n >= 50 else None

        # ── Breakout Signal ──────────────────────────────────────────────────────
        h20 = out["high_20d"]
        ext = out["extension_risk"]
        if h20 is None or h20 <= 0:
            out["breakout_signal"] = None
        else:
            pf20 = (price - h20) / h20 * 100
            rng20_raw = (max(closes[-20:]) - min(closes[-20:])) / price * 100 if n >= 20 else None
            if pf20 >= 0:
                if pf20 <= 3:
                    out["breakout_signal"] = "fresh_breakout"
                elif ext in ("extended", "overheated"):
                    out["breakout_signal"] = "extended_breakout"
                else:
                    out["breakout_signal"] = "confirmed_breakout"
            elif pf20 > -5:
                out["breakout_signal"] = "coiling" if (rng20_raw is not None and rng20_raw < 8) else "near_trigger"
            elif pf20 <= -10 and s20 is not None and price < s20:
                out["breakout_signal"] = "failed_breakout"
            else:
                out["breakout_signal"] = "no_setup"

        # ── Entry Zone ───────────────────────────────────────────────────────────
        ms   = out.get("ma_stack")
        bsig = out.get("breakout_signal")
        if s20 is None:
            out["entry_zone"] = "neutral"
        elif ext in ("overheated",) and (pct20 or 0) > 12:
            out["entry_zone"] = "overheated"
        elif ext == "extended":
            out["entry_zone"] = "extended"
        elif ext == "broken":
            out["entry_zone"] = "broken"
        elif ms in ("bull", "mixed") and pct20 is not None and -8 <= pct20 <= 2:
            out["entry_zone"] = "20d_pullback"
        elif s50 is not None and ms in ("bull", "mixed") and pct50 is not None and -10 <= pct50 <= 2:
            out["entry_zone"] = "50d_pullback"
        elif bsig in ("coiling", "near_trigger"):
            out["entry_zone"] = "breakout_watch"
        elif bsig in ("fresh_breakout", "confirmed_breakout") and ext == "healthy":
            out["entry_zone"] = "fresh_breakout"
        else:
            out["entry_zone"] = "neutral"

        # ── Volume / Accumulation (requires volume bars) ─────────────────────────
        if has_volume and n >= 20:
            vols20 = [v for v in volumes[-20:] if v is not None]
            if len(vols20) >= 10:
                avg_v20 = sum(vols20) / len(vols20)
                out["avg_volume_20d"] = round(avg_v20, 0)
                acc = dist = 0
                for i in range(max(1, n - 20), n):
                    c_cur, c_prv = closes[i], closes[i - 1]
                    v = volumes[i] if volumes[i] is not None else 0
                    vr = v / avg_v20 if avg_v20 > 0 else 1.0
                    if c_cur > c_prv and vr >= 1.2:
                        acc += 1
                    elif c_cur < c_prv and vr >= 1.2:
                        dist += 1
                out["accumulation_days_20d"]  = acc
                out["distribution_days_20d"]  = dist
                ad_score = acc - dist
                out["accumulation_distribution_score"] = ad_score
                if ad_score >= 4:
                    out["accumulation_distribution_signal"] = "heavy_accumulation"
                elif ad_score >= 2:
                    out["accumulation_distribution_signal"] = "accumulation"
                elif ad_score <= -4:
                    out["accumulation_distribution_signal"] = "distribution"
                else:
                    # Check for dry-up: price tight + volume contracting
                    prior_vols = [v for v in volumes[-40:-20] if v is not None]
                    prior_avg  = sum(prior_vols) / len(prior_vols) if prior_vols else avg_v20
                    rng20_v    = (max(closes[-20:]) - min(closes[-20:])) / price * 100
                    if rng20_v < 8 and avg_v20 < prior_avg * 0.75:
                        out["accumulation_distribution_signal"] = "dry_up"
                    else:
                        out["accumulation_distribution_signal"] = "neutral"
            else:
                out["avg_volume_20d"] = None
                out["accumulation_days_20d"] = out["distribution_days_20d"] = None
                out["accumulation_distribution_score"] = None
                out["accumulation_distribution_signal"] = "neutral"
                missing.append("insufficient_volume_data")
        else:
            out["avg_volume_20d"] = None
            out["accumulation_days_20d"] = out["distribution_days_20d"] = None
            out["accumulation_distribution_score"] = None
            out["accumulation_distribution_signal"] = "neutral"
            if not has_volume:
                missing.append("no_volume_in_bars")

        # ── ATR 14 (requires high/low) ───────────────────────────────────────────
        if has_ohlcv and n >= 15:
            tr_vals = []
            for i in range(max(1, n - 20), n):
                h, l, cp = highs[i], lows[i], closes[i - 1]
                if h is None or l is None:
                    continue
                tr_vals.append(max(h - l, abs(h - cp), abs(l - cp)))
            if len(tr_vals) >= 14:
                atr = round(sum(tr_vals[-14:]) / 14, 4)
                out["atr_14"]     = atr
                out["atr_14_pct"] = round(atr / price * 100, 2) if price > 0 else None
            else:
                out["atr_14"] = out["atr_14_pct"] = None
        else:
            out["atr_14"] = out["atr_14_pct"] = None
            if not has_ohlcv:
                missing.append("no_high_low_for_atr")

        # ── Range 20D % (OHLC true range if available, else close-based) ─────────
        if n >= 20:
            if has_ohlcv:
                h20v = [highs[i] for i in range(n - 20, n) if highs[i] is not None]
                l20v = [lows[i]  for i in range(n - 20, n) if lows[i]  is not None]
                if h20v and l20v:
                    out["range_20d_pct"] = round((max(h20v) - min(l20v)) / price * 100, 2)
                else:
                    out["range_20d_pct"] = round((max(closes[-20:]) - min(closes[-20:])) / price * 100, 2)
            else:
                out["range_20d_pct"] = round((max(closes[-20:]) - min(closes[-20:])) / price * 100, 2)
        else:
            out["range_20d_pct"] = None

        # ── Squeeze Signal ───────────────────────────────────────────────────────
        atr_pct = out.get("atr_14_pct")
        rng_20  = out.get("range_20d_pct")
        if atr_pct is not None and has_ohlcv and n >= 55:
            hist_trs = []
            for i in range(n - 55, n - 15):
                h, l, cp = highs[i], lows[i], closes[i - 1] if i > 0 else closes[i]
                if h is not None and l is not None:
                    hist_trs.append(max(h - l, abs(h - cp), abs(l - cp)))
            if hist_trs:
                hist_avg = sum(hist_trs) / len(hist_trs)
                ratio    = (out["atr_14"] or 0) / hist_avg if hist_avg > 0 else 1.0
                if ratio < 0.6 and (rng_20 or 99) < 12:
                    out["squeeze_signal"] = "tight"
                elif ratio < 0.75:
                    out["squeeze_signal"] = "coiling"
                elif ratio > 1.5:
                    out["squeeze_signal"] = "expansion"
                elif atr_pct > 5:
                    out["squeeze_signal"] = "volatile"
                else:
                    out["squeeze_signal"] = "normal"
            else:
                out["squeeze_signal"] = "normal"
        elif atr_pct is not None:
            if atr_pct < 1.5 and (rng_20 or 99) < 10:
                out["squeeze_signal"] = "tight"
            elif atr_pct < 2.5:
                out["squeeze_signal"] = "coiling"
            elif atr_pct > 5:
                out["squeeze_signal"] = "volatile"
            else:
                out["squeeze_signal"] = "normal"
        elif rng_20 is not None:
            if rng_20 < 8:
                out["squeeze_signal"] = "coiling"
            elif rng_20 > 25:
                out["squeeze_signal"] = "volatile"
            else:
                out["squeeze_signal"] = "normal"
        else:
            out["squeeze_signal"] = None

        # ── Momentum / ROC ───────────────────────────────────────────────────────
        def _roc(period: int) -> Optional[float]:
            if n <= period or closes[-(period + 1)] <= 0:
                return None
            return round((closes[-1] / closes[-(period + 1)] - 1) * 100, 2)

        r20 = _roc(20) if n > 20 else None
        r50 = _roc(50) if n > 50 else None
        out["roc_20d"] = r20
        out["roc_50d"] = r50
        if n <= 50: missing.append("no_50d_roc")

        if r20 is None:
            out["momentum_trend"] = "neutral"
        elif r20 > 0 and r50 is not None and r50 > 0 and r20 > r50:
            out["momentum_trend"] = "accelerating"
        elif r20 > 0 and (r50 is None or r50 >= 0):
            out["momentum_trend"] = "positive"
        elif r20 > 0 and r50 is not None and r50 < 0:
            out["momentum_trend"] = "cooling"
        elif r20 > 0 and (pct_h52 or 0) > -5:
            out["momentum_trend"] = "diverging"
        elif r20 < 0:
            out["momentum_trend"] = "negative"
        else:
            out["momentum_trend"] = "neutral"

        # ── Technical State ──────────────────────────────────────────────────────
        ext  = out.get("extension_risk", "neutral")
        ms   = out.get("ma_stack")
        bsig = out.get("breakout_signal")
        mom  = out.get("momentum_trend", "neutral")
        adst = out.get("accumulation_distribution_signal", "neutral")
        sqz  = out.get("squeeze_signal")

        if ext == "broken":
            out["technical_state"] = "broken"
        elif adst == "distribution" and ext in ("extended", "overheated"):
            out["technical_state"] = "distribution"
        elif ext == "overheated":
            out["technical_state"] = "overheated"
        elif ext == "extended":
            out["technical_state"] = "extended"
        elif bsig in ("fresh_breakout", "confirmed_breakout") and ext in ("healthy", "pullback_buy_zone"):
            out["technical_state"] = "breakout_trigger"
        elif sqz in ("tight", "coiling") and bsig in ("coiling", "near_trigger"):
            out["technical_state"] = "coiling"
        elif ext == "pullback_buy_zone" and ms in ("bull", "mixed"):
            out["technical_state"] = "pullback_entry"
        elif ms == "bull" and mom in ("accelerating", "positive"):
            out["technical_state"] = "trend_advance"
        else:
            out["technical_state"] = "neutral"

        # ── Technical Timing Score 0–100 ─────────────────────────────────────────
        sc = 50.0
        if ms == "bull":     sc += 20
        elif ms == "mixed":  sc += 5
        elif ms == "bear":   sc -= 20
        if ext == "pullback_buy_zone": sc += 10
        elif ext == "healthy":         sc += 5
        elif ext == "extended":        sc -= 5
        elif ext == "overheated":      sc -= 15
        elif ext == "broken":          sc -= 20
        if mom == "accelerating":  sc += 15
        elif mom == "positive":    sc += 8
        elif mom == "cooling":     sc += 2
        elif mom == "negative":    sc -= 10
        elif mom == "diverging":   sc -= 5
        if adst == "heavy_accumulation": sc += 10
        elif adst == "accumulation":     sc += 5
        elif adst == "distribution":     sc -= 10
        if sqz in ("tight", "coiling") and bsig in ("near_trigger", "coiling", "fresh_breakout"):
            sc += 8
        out["technical_timing_score"] = round(max(0.0, min(100.0, sc)), 1)

        out["missing_metric_reasons"] = missing
        return out

    except Exception as exc:
        null["missing_metric_reasons"] = [f"compute_error: {exc}"]
        return null


def analyze_theme_stage(
    proxy_type: str,
    proxy_daily_bars_map: dict[str, list[dict]],
    spy_daily_bars: Optional[list[dict]] = None,
    member_daily_bars: Optional[dict[str, list[dict]]] = None,
) -> dict:
    """
    Compute Weinstein stage for a theme.

    Parameters
    ----------
    proxy_type            : "etf" | "basket" | "hybrid" | "custom"
    proxy_daily_bars_map  : {symbol: daily_bars} for the theme's proxy symbols
    spy_daily_bars        : SPY daily bars (for RS calculation)
    member_daily_bars     : optional {symbol: daily_bars} for constituent stocks
                            used to compute breadth metrics

    Strategy by proxy_type:
      "etf"    → use first proxy ETF's weekly bars
      "basket" → use first proxy ETF's weekly bars (primary proxy)
      "hybrid" → use first proxy ETF's weekly bars
      "custom" → build equal-weight synthetic weekly basket from all proxy symbols;
                 also compute member breadth from all available symbol bars
    """
    now_ts = datetime.now(timezone.utc).isoformat()

    spy_weekly = weekly_bars_from_daily(spy_daily_bars) if spy_daily_bars else None

    # ── Member breadth (for baskets / custom themes) ───────────────────────────
    breadth_above_30w: Optional[float]  = None
    breadth_rising_30w: Optional[float] = None

    all_member_bars: dict[str, list[dict]] = {
        **proxy_daily_bars_map,
        **(member_daily_bars or {}),
    }

    if len(all_member_bars) >= 2:
        above_count  = 0
        rising_count = 0
        total        = 0
        for sym, daily_bars in all_member_bars.items():
            w = weekly_bars_from_daily(daily_bars)
            if len(w) < 32:
                continue
            c = [float(b["close"]) for b in w]
            ma30 = _sma(c, 30)
            if not ma30:
                continue
            total += 1
            if c[-1] > ma30:
                above_count += 1
            ma30_4w = _sma(c[:-4], 30) if len(c) >= 34 else None
            if ma30_4w and ma30_4w > 0 and (ma30 - ma30_4w) / ma30_4w * 100 > 0.2:
                rising_count += 1
        if total >= 2:
            breadth_above_30w  = round(above_count  / total * 100, 1)
            breadth_rising_30w = round(rising_count / total * 100, 1)

    # ── Build price series ─────────────────────────────────────────────────────
    if proxy_type == "custom":
        weekly_map = {
            sym: weekly_bars_from_daily(bars)
            for sym, bars in proxy_daily_bars_map.items()
        }
        weekly_map = {k: v for k, v in weekly_map.items() if len(v) >= 30}

        if not weekly_map:
            if breadth_above_30w is not None:
                return _breadth_only_result(breadth_above_30w, breadth_rising_30w, now_ts)
            return _fallback_result("no_price_data_for_custom_theme", now_ts)

        weekly_bars = _build_synthetic_basket(weekly_map)
        source      = "synthetic_basket"

    else:
        # Try each proxy in order; use the first with sufficient weekly bars
        weekly_bars = []
        for sym, daily in proxy_daily_bars_map.items():
            candidate = weekly_bars_from_daily(daily)
            if len(candidate) >= _MIN_WEEKLY_BARS:
                weekly_bars = candidate
                break
        if not weekly_bars:
            # Fall back to whatever is longest
            best = max(proxy_daily_bars_map.items(),
                       key=lambda kv: len(weekly_bars_from_daily(kv[1])),
                       default=(None, None))
            if best[0] is None:
                return _fallback_result("no_proxy_symbol", now_ts)
            weekly_bars = weekly_bars_from_daily(best[1])
        source = "etf_proxy"

    return analyze_symbol_stage(
        weekly_bars,
        spy_weekly_bars=spy_weekly,
        breadth_above_30w=breadth_above_30w,
        breadth_rising_30w=breadth_rising_30w,
        source=source,
    )
