"""
entry_structure_v2.py — Deterministic Daily Swing-Structure Classification
============================================================================
Pure computation over already-fetched daily OHLCV bars. Zero provider calls.
Zero LLM calls.

This module adds real chart geometry (swing points, base/range detection,
rising trendline support, breakout-pivot / failed-breakout logic, and a
structural support hierarchy) that entry_state_service.py's classifier
consults BEFORE falling back to the older MA/rolling-high heuristics.

Design goals (per Entry Structure V2 spec):
  - Daily bars are the canonical timeframe (swing trader, days-to-months holds).
  - No new historical-bar requests — bars/tech are passed in from the existing
    Stage refresh pass.
  - Deterministic, reproducible — no LLM / AI involvement.
  - A ticker that has already confirmed a breakout above its base ceiling can
    never be classified as a pre-breakout high base (POST-BREAKOUT EXCLUSION).
  - A pullback that still holds a valid rising trendline / higher-low
    structure must not be forced into FAILED_BREAKOUT solely because a
    moving average or rolling 20d-high condition was lost.

Known simplifications (documented, not hidden):
  - Swing-point clustering uses a single ATR/percentage tolerance pass
    (greedy max-cluster), not a full hierarchical clustering.
  - Trendline candidate search considers the most recent handful of swing
    lows (last 6) rather than every possible anchor pair — sufficient for a
    swing-trading, days-to-months holding period.
  - `closes_below_pivot_count` counts closes below the pivot in the bars
    since the (most recent) confirmed breakout, not a strict "consecutive"
    counter reset on every single bar — documented in reason codes.
"""

from __future__ import annotations

import statistics
from typing import Optional


# ── Swing point detection ───────────────────────────────────────────────────

def find_swing_points(bars: list[dict], left: int = 3, right: int = 3) -> tuple[list[dict], list[dict]]:
    """
    Deterministic local swing-high / swing-low detection on daily bars.

    A swing high at index i requires high[i] to be the strict local max over
    the window [i-left, i+right]. Same logic (local min) for swing lows.
    Uses close as a fallback when high/low are unavailable.

    Returns (swing_highs, swing_lows), each a list of
    {date, price, bar_index, prominence}, oldest -> newest.
    """
    n = len(bars)
    if n < (left + right + 1):
        return [], []

    highs = [float(b["high"]) if b.get("high") is not None else float(b["close"]) for b in bars]
    lows = [float(b["low"]) if b.get("low") is not None else float(b["close"]) for b in bars]
    dates = [str(b.get("date", ""))[:10] for b in bars]

    swing_highs: list[dict] = []
    swing_lows: list[dict] = []

    for i in range(left, n - right):
        win_h = highs[i - left:i + right + 1]
        if highs[i] == max(win_h) and win_h.count(highs[i]) == 1:
            local_low = min(lows[max(0, i - left):i + right + 1])
            swing_highs.append({
                "date": dates[i],
                "price": round(highs[i], 4),
                "bar_index": i,
                "prominence": round(highs[i] - local_low, 4),
            })
        win_l = lows[i - left:i + right + 1]
        if lows[i] == min(win_l) and win_l.count(lows[i]) == 1:
            local_high = max(highs[max(0, i - left):i + right + 1])
            swing_lows.append({
                "date": dates[i],
                "price": round(lows[i], 4),
                "bar_index": i,
                "prominence": round(local_high - lows[i], 4),
            })

    return swing_highs, swing_lows


def _cluster_levels(points: list[dict], tolerance_pct: float) -> Optional[tuple[float, int, str, list[dict]]]:
    """
    Greedy max-membership price clustering (tolerance in %).
    Returns (level, touch_count, last_touch_date, cluster_members) or None.
    """
    if not points:
        return None
    best_cluster: list[dict] = []
    for anchor in points:
        if anchor["price"] <= 0:
            continue
        cluster = [
            p for p in points
            if abs(p["price"] - anchor["price"]) / anchor["price"] * 100.0 <= tolerance_pct
        ]
        if len(cluster) > len(best_cluster):
            best_cluster = cluster
    if not best_cluster:
        return None
    level = statistics.mean(p["price"] for p in best_cluster)
    touches = len(best_cluster)
    last_date = max(p["date"] for p in best_cluster if p.get("date"))
    return round(level, 4), touches, last_date, best_cluster


# ── Base / range / ceiling detection ────────────────────────────────────────

def detect_base(
    bars: list[dict],
    swing_highs: list[dict],
    swing_lows: list[dict],
    price: float,
    atr_pct: Optional[float],
    lookback: int = 90,
) -> dict:
    """
    Identify a recent consolidation/base from clustered swing highs/lows.
    Returns the full PART 1 field set. Never raises.
    """
    n = len(bars)
    empty = {
        "base_detected": False,
        "base_start_index": None, "base_start_date": None,
        "base_end_index": None, "base_end_date": None,
        "base_high": None, "base_low": None, "base_mid": None,
        "base_range_pct": None, "base_duration_bars": None,
        "distance_to_base_high_pct": None,
        "upper_range_position": None, "range_contraction": None,
        "higher_lows_count": 0, "resistance_touch_count": 0,
        "support_touch_count": 0, "base_breakout_status": "IN_RANGE",
        "resistance_cluster_tolerance_pct": None,
    }
    if n < 20 or price is None or price <= 0:
        return empty

    win_start = max(0, n - lookback)
    window_bars = bars[win_start:]
    window_closes = [float(b["close"]) for b in window_bars]
    window_high, window_low = max(window_closes), min(window_closes)
    if window_high <= 0 or window_high == window_low:
        return empty

    tol = max((atr_pct or 3.0) * 0.7, 2.5)  # ATR-normalized clustering tolerance

    # Only consider swing highs inside the lookback window, in the upper 55%
    # of the window's range (candidate base ceilings, not noise near lows).
    upper_zone = window_low + 0.55 * (window_high - window_low)
    win_highs = [h for h in swing_highs if h["bar_index"] >= win_start]
    relevant_highs = [h for h in win_highs if h["price"] >= upper_zone] or win_highs[-3:]

    res = _cluster_levels(relevant_highs, tol)
    if res is None:
        return empty
    resistance_level, resistance_touches, last_res_date, res_cluster = res

    base_start_index = min(p["bar_index"] for p in res_cluster)
    base_end_index = n - 1
    base_duration_bars = base_end_index - base_start_index + 1
    base_bars = bars[base_start_index:]
    base_closes = [float(b["close"]) for b in base_bars]

    win_lows = [lo for lo in swing_lows if lo["bar_index"] >= base_start_index]
    sres = _cluster_levels(win_lows, tol)
    if sres is not None:
        support_level, support_touches, _last_sup_date, sup_cluster = sres
    else:
        support_level, support_touches, sup_cluster = min(base_closes), 0, []

    base_high = resistance_level
    base_low = min(support_level, min(base_closes))  # never above actual traded lows
    if base_high <= base_low:
        return empty

    base_range_pct = round((base_high - base_low) / base_high * 100, 2)
    distance_to_base_high_pct = round((price - base_high) / base_high * 100, 2)
    upper_range_position = round((price - base_low) / (base_high - base_low) * 100, 1)

    # Higher-lows count: strictly ascending consecutive swing lows within base
    sl_sorted = sorted(win_lows, key=lambda p: p["bar_index"])
    higher_lows_count = sum(
        1 for i in range(1, len(sl_sorted)) if sl_sorted[i]["price"] > sl_sorted[i - 1]["price"]
    )

    # Range contraction: most-recent 10-bar range vs the prior 10-bar range
    def _rng(bs: list[dict]) -> Optional[float]:
        if not bs:
            return None
        cs = [float(b["close"]) for b in bs]
        return (max(cs) - min(cs)) / price * 100.0

    recent10 = base_bars[-10:]
    prior10 = base_bars[-20:-10] if len(base_bars) >= 20 else []
    r_recent, r_prior = _rng(recent10), _rng(prior10)
    range_contraction = bool(r_prior is not None and r_recent is not None and r_recent < r_prior)

    confirm_buf_pct = max(tol * 0.55, 1.5)
    confirmed_level = base_high * (1 + confirm_buf_pct / 100.0)
    fail_level = base_low * (1 - tol / 100.0)

    if price < fail_level:
        status = "BELOW_RANGE"
    elif price <= base_high:
        status = "PRESSING_CEILING" if distance_to_base_high_pct >= -3.0 else "IN_RANGE"
    elif price <= confirmed_level:
        status = "BREAKOUT_IN_PROGRESS"
    else:
        # Determine if the confirmed breakout has since failed (closed back
        # under the pivot) — detect_breakout_pivot() makes the authoritative
        # failed-breakout call; here we only distinguish in-progress vs
        # confirmed vs failed for base geometry purposes.
        breakout_idx = None
        for i in range(base_start_index, n):
            if bars[i]["close"] >= confirmed_level:
                breakout_idx = i
                break
        if breakout_idx is not None and bars[-1]["close"] < base_high * (1 - tol / 100.0):
            status = "BREAKOUT_FAILED"
        else:
            status = "BREAKOUT_CONFIRMED"

    base_detected = base_duration_bars >= 10 and resistance_touches >= 2

    return {
        "base_detected": base_detected,
        "base_start_index": base_start_index,
        "base_start_date": str(bars[base_start_index].get("date", ""))[:10],
        "base_end_index": base_end_index,
        "base_end_date": str(bars[base_end_index].get("date", ""))[:10],
        "base_high": round(base_high, 4),
        "base_low": round(base_low, 4),
        "base_mid": round((base_high + base_low) / 2, 4),
        "base_range_pct": base_range_pct,
        "base_duration_bars": base_duration_bars,
        "distance_to_base_high_pct": distance_to_base_high_pct,
        "upper_range_position": upper_range_position,
        "range_contraction": range_contraction,
        "higher_lows_count": higher_lows_count,
        "resistance_touch_count": resistance_touches,
        "support_touch_count": support_touches,
        "base_breakout_status": status,
        "resistance_cluster_tolerance_pct": round(tol, 2),
    }


# ── Rising trendline / diagonal support ─────────────────────────────────────

def detect_trendline(bars: list[dict], swing_lows: list[dict], price: float, atr_pct: Optional[float]) -> dict:
    """
    Detect a valid ascending trendline from >=2 recent swing-low anchors.
    """
    empty = {
        "ascending_trendline_detected": False, "trendline_slope_per_bar": None,
        "anchor_1_date": None, "anchor_1_price": None,
        "anchor_2_date": None, "anchor_2_price": None,
        "trendline_touch_count": 0, "projected_trendline_support": None,
        "distance_to_trendline_pct": None, "trendline_hold_state": None,
    }
    n = len(bars)
    if n < 20 or len(swing_lows) < 2 or price is None or price <= 0:
        return empty

    candidates = swing_lows[-6:]  # most recent handful of swing lows
    tol_pct = max((atr_pct or 3.0) * 0.8, 2.5)
    best: Optional[tuple[dict, dict, float]] = None

    for i in range(len(candidates)):
        for j in range(i + 1, len(candidates)):
            a, b = candidates[i], candidates[j]
            bar_gap = b["bar_index"] - a["bar_index"]
            if bar_gap < 5:
                continue
            slope = (b["price"] - a["price"]) / bar_gap
            if slope <= 0:
                continue
            # Reject if the projected line is materially violated by any
            # close between the anchors (line must be "under" the structure).
            violated = False
            for k in range(a["bar_index"] + 1, b["bar_index"]):
                proj_k = a["price"] + slope * (k - a["bar_index"])
                if bars[k]["close"] < proj_k * (1 - tol_pct / 100.0):
                    violated = True
                    break
            if violated:
                continue
            if best is None or b["bar_index"] > best[1]["bar_index"]:
                best = (a, b, slope)

    if best is None:
        return empty

    a, b, slope = best
    projected = b["price"] + slope * (n - 1 - b["bar_index"])
    if projected <= 0:
        return empty

    touches = 0
    violations = 0
    for i in range(b["bar_index"], n):
        proj_i = b["price"] + slope * (i - b["bar_index"])
        if proj_i <= 0:
            continue
        low_i = float(bars[i].get("low")) if bars[i].get("low") is not None else float(bars[i]["close"])
        close_i = float(bars[i]["close"])
        if abs(low_i - proj_i) / proj_i * 100.0 <= tol_pct:
            touches += 1
        if close_i < proj_i * (1 - tol_pct / 100.0):
            violations += 1

    distance_to_trendline_pct = round((price - projected) / projected * 100, 2)
    last_close = float(bars[-1]["close"])
    last_low = float(bars[-1].get("low")) if bars[-1].get("low") is not None else last_close

    if violations >= 2:
        hold_state = "BROKEN"
    elif last_close < projected * (1 - tol_pct / 100.0):
        hold_state = "BROKEN"
    elif last_low < projected * (1 - tol_pct / 100.0) and last_close >= projected * (1 - tol_pct / 100.0):
        hold_state = "UNDERCUT_RECLAIM"
    elif abs(distance_to_trendline_pct) <= tol_pct:
        hold_state = "TESTING"
    elif distance_to_trendline_pct > 0 and touches > 0:
        hold_state = "HELD_RECENTLY"
    else:
        hold_state = "ABOVE"

    return {
        "ascending_trendline_detected": True,
        "trendline_slope_per_bar": round(slope, 6),
        "anchor_1_date": a["date"], "anchor_1_price": a["price"],
        "anchor_2_date": b["date"], "anchor_2_price": b["price"],
        "trendline_touch_count": touches,
        "projected_trendline_support": round(projected, 4),
        "distance_to_trendline_pct": distance_to_trendline_pct,
        "trendline_hold_state": hold_state,
    }


# ── Breakout pivot / failed-breakout logic ──────────────────────────────────

def detect_breakout_pivot(bars: list[dict], base: dict, price: float, atr_pct: Optional[float]) -> dict:
    """
    Strict breakout-pivot + failed-breakout evaluation, gated on a REAL
    identified base ceiling (never a bare rolling 20d high).
    """
    empty = {
        "breakout_pivot": None, "breakout_date": None,
        "breakout_confirmed": False, "breakout_confirmation_pct": None,
        "days_since_breakout": None, "close_vs_breakout_pivot_pct": None,
        "closes_below_pivot_count": 0, "failed_breakout_confirmed": False,
        "failed_breakout_reason_codes": [],
    }
    if not base.get("base_detected") or base.get("base_high") is None:
        empty["failed_breakout_reason_codes"] = ["no_real_breakout_pivot_identified"]
        return empty

    n = len(bars)
    pivot = base["base_high"]
    base_start_index = base["base_start_index"]
    tol = max((atr_pct or 3.0) * 0.55, 1.5)
    confirmed_level = pivot * (1 + tol / 100.0)
    fail_level = pivot * (1 - tol / 100.0)

    breakout_idx = None
    for i in range(base_start_index, n):
        if float(bars[i]["close"]) >= confirmed_level:
            breakout_idx = i
            break

    if breakout_idx is None:
        empty["breakout_pivot"] = round(pivot, 4)
        empty["close_vs_breakout_pivot_pct"] = round((price - pivot) / pivot * 100, 2)
        empty["failed_breakout_reason_codes"] = ["breakout_never_confirmed_above_pivot"]
        return empty

    breakout_date = str(bars[breakout_idx].get("date", ""))[:10]
    days_since_breakout = (n - 1) - breakout_idx
    close_vs_pivot_pct = round((price - pivot) / pivot * 100, 2)

    closes_below = sum(
        1 for i in range(breakout_idx + 1, n) if float(bars[i]["close"]) < fail_level
    )

    consecutive_recent_below = 0
    for i in range(n - 1, breakout_idx, -1):
        if float(bars[i]["close"]) < fail_level:
            consecutive_recent_below += 1
        else:
            break

    reason_codes: list[str] = []
    failed = False
    if price < fail_level:
        if consecutive_recent_below >= 2:
            failed = True
            reason_codes.append(f"two_consecutive_closes_below_pivot(n={consecutive_recent_below})")
        elif consecutive_recent_below >= 1 and closes_below >= 1:
            # Single decisive close below pivot with no immediate reclaim.
            failed = True
            reason_codes.append("one_decisive_close_below_pivot_no_reclaim")
        else:
            reason_codes.append("price_below_pivot_but_no_confirmed_failure_yet")
    else:
        reason_codes.append("pivot_holding")

    return {
        "breakout_pivot": round(pivot, 4),
        "breakout_date": breakout_date,
        "breakout_confirmed": True,
        "breakout_confirmation_pct": round(tol, 2),
        "days_since_breakout": days_since_breakout,
        "close_vs_breakout_pivot_pct": close_vs_pivot_pct,
        "closes_below_pivot_count": closes_below,
        "failed_breakout_confirmed": failed,
        "failed_breakout_reason_codes": reason_codes,
    }


# ── Support hierarchy ────────────────────────────────────────────────────────

def build_support_hierarchy(
    price: float,
    base: dict,
    trendline: dict,
    breakout: dict,
    swing_lows: list[dict],
    sma20: Optional[float], sma50: Optional[float], sma200: Optional[float],
    ma30w: Optional[float],
    stage_int: int,
) -> tuple[list[dict], Optional[dict]]:
    """
    Expanded structural support candidates + primary-support selection.
    Returns (candidates, primary) — primary is the chosen candidate dict
    (with an added "reason" key) or None if nothing qualifies.
    """
    candidates: list[dict] = []

    def _add(support_type: str, level: Optional[float], touch_count: int,
             relevance: float, hold_state: str) -> None:
        if level is None or level <= 0 or level >= price:
            return
        candidates.append({
            "support_type": support_type,
            "level": round(level, 4),
            "distance_pct": round((level - price) / price * 100, 2),
            "structural_relevance_score": relevance,
            "touch_count": touch_count,
            "hold_state": hold_state,
        })

    if trendline.get("ascending_trendline_detected"):
        _add("ascending_trendline", trendline.get("projected_trendline_support"),
             trendline.get("trendline_touch_count", 0), 95.0,
             trendline.get("trendline_hold_state") or "TESTING")

    if base.get("base_detected"):
        _add("base_floor", base.get("base_low"), base.get("support_touch_count", 0), 85.0, "ABOVE")
        _add("base_midpoint", base.get("base_mid"), 0, 55.0, "ABOVE")

    if breakout.get("breakout_confirmed") and not breakout.get("failed_breakout_confirmed"):
        _add("prior_breakout_pivot", breakout.get("breakout_pivot"), 1, 80.0, "ABOVE")

    if swing_lows:
        recent_low = sorted(swing_lows, key=lambda p: p["bar_index"])[-1]
        _add("recent_swing_low_shelf", recent_low.get("price"), 1, 65.0, "ABOVE")

    _add("SMA20", sma20, 0, 40.0, "ABOVE")
    _add("SMA50", sma50, 0, 30.0, "ABOVE")
    _add("30w_MA", ma30w, 0, 25.0, "ABOVE")
    _add("SMA200", sma200, 0, 10.0, "ABOVE")

    # De-dup near-identical levels (within 1%), keep the higher-relevance one.
    deduped: list[dict] = []
    for c in sorted(candidates, key=lambda c: -c["structural_relevance_score"]):
        if any(abs(c["level"] - d["level"]) / d["level"] * 100.0 < 1.0 for d in deduped):
            continue
        deduped.append(c)
    deduped.sort(key=lambda c: c["distance_pct"], reverse=True)  # closest to price first

    primary = None
    if deduped:
        best = max(deduped, key=lambda c: c["structural_relevance_score"])
        primary = dict(best)
        reasons = {
            "ascending_trendline": "most relevant active structure for an advancing stock — rising swing-low trend support",
            "base_floor": "structural base floor from clustered swing lows takes precedence over distant long-term MAs",
            "prior_breakout_pivot": "former resistance pivot now acting as support (breakout not failed)",
            "recent_swing_low_shelf": "most recent swing-low shelf is the nearest relevant structure",
            "SMA20": "short-term MA support — no stronger structural level (trendline/base) identified",
            "SMA50": "intermediate MA support — no stronger structural level identified",
            "30w_MA": "30-week MA support — Stage 2 baseline support, no nearer structure identified",
            "SMA200": "long-term MA support only — no nearer structural level identified; may be distant and less actionable",
        }
        primary["primary_support_reason"] = reasons.get(primary["support_type"], "closest available structural level")

    return deduped, primary


# ── PART 1 — Base location / prior-move context ─────────────────────────────

def compute_base_location(bars: list[dict], base: dict, price: float) -> dict:
    """
    Deterministic diagnostics describing WHERE a detected base sits relative
    to the stock's prior 52w price structure. Pure computation over the same
    already-fetched daily bars — zero new history calls.
    """
    empty = {
        "base_position_in_52w_range": None,
        "base_position_in_prior_lookback_range": None,
        "distance_from_52w_high_pct": None,
        "distance_from_52w_low_pct": None,
        "drawdown_from_prior_major_high_pct": None,
        "prior_advance_pct": None,
        "prior_decline_pct": None,
        "base_start_vs_prior_high_pct": None,
        "base_start_vs_prior_low_pct": None,
        "base_midpoint_position": None,
    }
    if not bars or price is None or price <= 0:
        return empty

    lookback_52w = bars[-252:] if len(bars) >= 20 else bars
    closes_52w = [float(b["close"]) for b in lookback_52w]
    highs_52w = [float(b["high"]) if b.get("high") is not None else float(b["close"]) for b in lookback_52w]
    lows_52w = [float(b["low"]) if b.get("low") is not None else float(b["close"]) for b in lookback_52w]
    hi_52w, lo_52w = max(highs_52w), min(lows_52w)

    base_position_in_52w_range = None
    if hi_52w > lo_52w:
        base_position_in_52w_range = round((price - lo_52w) / (hi_52w - lo_52w) * 100, 1)

    distance_from_52w_high_pct = round((price - hi_52w) / hi_52w * 100, 2) if hi_52w > 0 else None
    distance_from_52w_low_pct = round((price - lo_52w) / lo_52w * 100, 2) if lo_52w > 0 else None
    drawdown_from_prior_major_high_pct = distance_from_52w_high_pct

    # Prior advance / decline: move from the 52w low to the 52w high (whichever
    # came first) gives the magnitude of the dominant prior directional move.
    hi_idx = highs_52w.index(hi_52w)
    lo_idx = lows_52w.index(lo_52w)
    prior_advance_pct = None
    prior_decline_pct = None
    if lo_idx < hi_idx and lo_52w > 0:
        prior_advance_pct = round((hi_52w - lo_52w) / lo_52w * 100, 2)
    elif hi_idx < lo_idx and hi_52w > 0:
        prior_decline_pct = round((lo_52w - hi_52w) / hi_52w * 100, 2)

    base_position_in_prior_lookback_range = None
    base_start_vs_prior_high_pct = None
    base_start_vs_prior_low_pct = None
    base_midpoint_position = None
    if base.get("base_detected"):
        b_start_idx = base.get("base_start_index")
        pre_base_bars = lookback_52w[: max(0, len(lookback_52w) - (len(bars) - (b_start_idx or 0)))] if b_start_idx is not None else []
        if pre_base_bars:
            pre_highs = [float(b["high"]) if b.get("high") is not None else float(b["close"]) for b in pre_base_bars]
            pre_lows = [float(b["low"]) if b.get("low") is not None else float(b["close"]) for b in pre_base_bars]
            prior_hi, prior_lo = max(pre_highs), min(pre_lows)
            if prior_hi > 0:
                base_start_vs_prior_high_pct = round((base.get("base_high", price) - prior_hi) / prior_hi * 100, 2)
            if prior_lo > 0:
                base_start_vs_prior_low_pct = round((base.get("base_low", price) - prior_lo) / prior_lo * 100, 2)
        if hi_52w > lo_52w and base.get("base_high") is not None:
            base_position_in_prior_lookback_range = round(
                (base["base_high"] - lo_52w) / (hi_52w - lo_52w) * 100, 1
            )
        if base.get("base_mid") is not None and hi_52w > lo_52w:
            base_midpoint_position = round((base["base_mid"] - lo_52w) / (hi_52w - lo_52w) * 100, 1)

    return {
        "base_position_in_52w_range": base_position_in_52w_range,
        "base_position_in_prior_lookback_range": base_position_in_prior_lookback_range,
        "distance_from_52w_high_pct": distance_from_52w_high_pct,
        "distance_from_52w_low_pct": distance_from_52w_low_pct,
        "drawdown_from_prior_major_high_pct": drawdown_from_prior_major_high_pct,
        "prior_advance_pct": prior_advance_pct,
        "prior_decline_pct": prior_decline_pct,
        "base_start_vs_prior_high_pct": base_start_vs_prior_high_pct,
        "base_start_vs_prior_low_pct": base_start_vs_prior_low_pct,
        "base_midpoint_position": base_midpoint_position,
    }


# ── PART 2 — Base archetype classification ──────────────────────────────────

def classify_base_archetype(base: dict, location: dict, low_base_floor: dict) -> str:
    """
    Classify a detected base as HIGH_BASE, LOW_BASE, MID_RANGE_BASE, or
    UNCLASSIFIED_BASE using existing base-geometry + location diagnostics.
    Never forces every base into HIGH or LOW.
    """
    if not base.get("base_detected"):
        return "UNCLASSIFIED_BASE"

    pos_52w = location.get("base_position_in_52w_range")
    prior_advance = location.get("prior_advance_pct")
    prior_decline = location.get("prior_decline_pct")
    dd_from_high = location.get("drawdown_from_prior_major_high_pct")
    upper_pos = base.get("upper_range_position")

    if pos_52w is None:
        return "UNCLASSIFIED_BASE"

    # HIGH_BASE: base sits in the upper region of a meaningful prior advance.
    is_high = (
        pos_52w >= 55.0 and
        (prior_advance is not None and prior_advance >= 20.0) and
        (upper_pos is None or upper_pos >= 40.0)
    )
    if is_high:
        return "HIGH_BASE"

    # LOW_BASE: meaningful prior decline/drawdown + base in lower region +
    # a real floor with repeat support interactions + no decisive breakdown.
    is_low = (
        pos_52w <= 45.0 and
        (dd_from_high is not None and dd_from_high <= -25.0) and
        low_base_floor.get("low_base_floor") is not None and
        (low_base_floor.get("low_base_floor_touch_count") or 0) >= 2 and
        low_base_floor.get("floor_held_recently") is True and
        base.get("base_breakout_status") != "BELOW_RANGE"
    )
    if is_low:
        return "LOW_BASE"

    return "MID_RANGE_BASE"


# ── PART 3 — Low-base floor detection ───────────────────────────────────────

def detect_low_base_floor(
    bars: list[dict],
    swing_lows: list[dict],
    price: float,
    atr_pct: Optional[float],
    base: dict,
) -> dict:
    """
    Derive floor/support evidence for LOW_BASE classification, reusing the
    existing swing-low detection. Never invents a floor for a falling knife —
    requires clustered swing lows + recent closes respecting the floor.
    """
    empty = {
        "low_base_floor": None,
        "low_base_floor_touch_count": 0,
        "low_base_floor_first_date": None,
        "low_base_floor_last_date": None,
        "distance_to_floor_pct": None,
        "floor_held_recently": False,
        "floor_break_count": 0,
        "low_base_support_quality": "NONE",
    }
    n = len(bars)
    if n < 20 or price is None or price <= 0 or not swing_lows:
        return empty

    lookback = min(n, 130)
    win_start = n - lookback
    win_lows = [lo for lo in swing_lows if lo["bar_index"] >= win_start]
    if not win_lows:
        return empty

    tol = max((atr_pct or 3.0) * 0.8, 2.5)
    res = _cluster_levels(win_lows, tol)
    if res is None:
        return empty
    floor_level, touch_count, last_touch_date, cluster = res
    first_touch_date = min(p["date"] for p in cluster if p.get("date"))

    fail_level = floor_level * (1 - tol / 100.0)
    recent_bars = bars[-15:]
    floor_break_count = sum(1 for b in recent_bars if float(b["close"]) < fail_level)
    floor_held_recently = floor_break_count == 0 and float(bars[-1]["close"]) >= fail_level

    distance_to_floor_pct = round((price - floor_level) / floor_level * 100, 2) if floor_level > 0 else None

    if touch_count >= 3 and floor_held_recently:
        quality = "STRONG"
    elif touch_count >= 2 and floor_held_recently:
        quality = "MODERATE"
    elif touch_count >= 2:
        quality = "WEAK"
    else:
        quality = "NONE"

    return {
        "low_base_floor": round(floor_level, 4),
        "low_base_floor_touch_count": touch_count,
        "low_base_floor_first_date": first_touch_date,
        "low_base_floor_last_date": last_touch_date,
        "distance_to_floor_pct": distance_to_floor_pct,
        "floor_held_recently": floor_held_recently,
        "floor_break_count": floor_break_count,
        "low_base_support_quality": quality,
    }


# ── Master structural classifier ────────────────────────────────────────────

# Allowed final entry states produced/consulted by this module.
HIGH_BASE_FORMING = "HIGH_BASE_FORMING"
HIGH_BASE_COILING = "HIGH_BASE_COILING"
HIGH_BASE_READY = "HIGH_BASE_READY"
BREAKOUT_CONFIRMED_STATE = "BREAKOUT_CONFIRMED"
WAIT_FOR_RETEST = "WAIT_FOR_RETEST"
BREAKOUT_RETEST = "BREAKOUT_RETEST"
TRENDLINE_SUPPORT_TEST = "TRENDLINE_SUPPORT_TEST"
CONSTRUCTIVE_DIP = "CONSTRUCTIVE_DIP"
BREAKOUT_PULLBACK = "BREAKOUT_PULLBACK"
REVERSAL_WATCH = "REVERSAL_WATCH"
FAILED_BREAKOUT = "FAILED_BREAKOUT"


def compute_structure(
    bars: list[dict],
    tech: dict,
    price: float,
    stage_int: int,
    sma20: Optional[float], sma50: Optional[float], sma200: Optional[float],
    ma30w: Optional[float],
) -> dict:
    """
    Compute the full structural diagnostic bundle (swings, base, trendline,
    breakout pivot, support hierarchy) for one symbol. Pure computation,
    zero provider calls. Stage-agnostic — caller decides which states to
    apply based on stage.
    """
    atr_pct = tech.get("atr_14_pct")
    swing_highs, swing_lows = find_swing_points(bars)
    base = detect_base(bars, swing_highs, swing_lows, price, atr_pct)
    trendline = detect_trendline(bars, swing_lows, price, atr_pct)
    breakout = detect_breakout_pivot(bars, base, price, atr_pct)
    support_candidates, primary_support = build_support_hierarchy(
        price, base, trendline, breakout, swing_lows,
        sma20, sma50, sma200, ma30w, stage_int,
    )
    base_location = compute_base_location(bars, base, price)
    low_base_floor = detect_low_base_floor(bars, swing_lows, price, atr_pct, base)
    base_archetype = classify_base_archetype(base, base_location, low_base_floor)
    return {
        "base": base,
        "trendline": trendline,
        "breakout": breakout,
        "support_candidates": support_candidates,
        "primary_support": primary_support,
        "swing_high_count": len(swing_highs),
        "swing_low_count": len(swing_lows),
        "base_location": base_location,
        "low_base_floor": low_base_floor,
        "base_archetype": base_archetype,
    }
