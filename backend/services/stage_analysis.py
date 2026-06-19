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
