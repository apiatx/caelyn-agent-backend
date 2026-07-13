"""
fib_engine.py
=============
Pure Fibonacci level calculator from OHLC daily bars.

Zero provider calls. Zero LLM calls.
Inputs: sorted list of OHLC dicts (oldest → newest).
Outputs: Fibonacci retracement/extension levels + current-price proximity detection.

Anchor selection
----------------
1. Take the last 120 daily bars (or all available).
2. Find the bar with the highest HIGH in that window → impulse peak.
3. Look back up to 90 bars before the peak for the lowest LOW → swing base.
4. That base → peak is the "dominant impulse".
5. Retracement levels measure from peak back toward base.
6. Extension levels measure beyond the peak.

Retracement labels: FIB_0.382, FIB_0.500, FIB_0.618, FIB_0.786, FIB_1.000
Extension labels:   FIB_1.272, FIB_1.618, FIB_2.000, FIB_2.618
"""
from __future__ import annotations
from typing import Optional


# ── Level definitions ──────────────────────────────────────────────────────────
_FIB_RETRACE_RATIOS: list[tuple[str, float]] = [
    ("FIB_0.382", 0.382),
    ("FIB_0.500", 0.500),
    ("FIB_0.618", 0.618),
    ("FIB_0.786", 0.786),
    ("FIB_1.000", 1.000),  # prior high / prior resistance
]

_FIB_EXTEND_RATIOS: list[tuple[str, float]] = [
    ("FIB_1.272", 1.272),
    ("FIB_1.618", 1.618),
    ("FIB_2.000", 2.000),
    ("FIB_2.618", 2.618),
]

# Proximity threshold — price must be within this % of a level to count as a retest
_PROXIMITY_PCT: dict[str, float] = {
    "standard": 2.5,
    "volatile": 4.0,
}

# Minimum impulse size; below this the levels are meaningless noise
_MIN_IMPULSE_PCT = 10.0


# ── Bar field helpers ──────────────────────────────────────────────────────────

def _bar_high(bar: dict) -> float:
    v = bar.get("high")
    if v is not None:
        try:
            return float(v)
        except (TypeError, ValueError):
            pass
    try:
        return float(bar.get("close") or 0)
    except (TypeError, ValueError):
        return 0.0


def _bar_low(bar: dict) -> float:
    v = bar.get("low")
    if v is not None:
        try:
            return float(v)
        except (TypeError, ValueError):
            pass
    try:
        return float(bar.get("close") or 0)
    except (TypeError, ValueError):
        return 0.0


def _bar_close(bar: dict) -> float:
    try:
        return float(bar.get("close") or 0)
    except (TypeError, ValueError):
        return 0.0


# ── Main API ───────────────────────────────────────────────────────────────────

def compute_fib_levels(
    bars: list[dict],
    current_price: Optional[float] = None,
    ticker_type: str = "standard",
) -> dict:
    """
    Compute Fibonacci retracement and extension levels from OHLC bars.

    Parameters
    ----------
    bars          : Daily OHLCV bars oldest → newest.  Keys: date, open, high,
                    low, close, (volume optional).
    current_price : Live price override; uses last bar close when None.
    ticker_type   : "standard" (2.5 % proximity) | "volatile" (4.0 %).

    Returns
    -------
    Flat dict — all keys always present (None when data is insufficient).
    Key fields:
      fib_computed (bool)
      fib_anchor_low, fib_anchor_high, fib_impulse_pct
      fib_0382 … fib_1000  (retracement)
      fib_1272 … fib_2618  (extension)
      nearest_fib_label, nearest_fib_level, distance_to_nearest_fib_pct
      fib_retest_detected (bool), fib_retest_type
      fib_target_1 (1.272), fib_target_2 (1.618)
      fib_reason_codes (list[str])
    """
    null = _null_result()

    if not bars or len(bars) < 20:
        null["fib_reason_codes"] = ["INSUFFICIENT_BARS"]
        return null

    try:
        bars_s = sorted(bars, key=lambda b: str(b.get("date", "") or "")[:10])
        window = bars_s[-120:]
        n = len(window)

        # ── Current price ─────────────────────────────────────────────────────
        if current_price is not None:
            try:
                price = float(current_price)
            except (TypeError, ValueError):
                price = _bar_close(window[-1])
        else:
            price = _bar_close(window[-1])

        if price <= 0:
            null["fib_reason_codes"] = ["INVALID_CURRENT_PRICE"]
            return null

        # ── Step 1: impulse peak = highest HIGH in window ─────────────────────
        peak_idx   = max(range(n), key=lambda i: _bar_high(window[i]))
        peak_price = _bar_high(window[peak_idx])
        if peak_price <= 0:
            null["fib_reason_codes"] = ["INVALID_PEAK_PRICE"]
            return null

        # ── Step 2: swing base = lowest LOW in up to 90 bars before peak ──────
        look_back_start = max(0, peak_idx - 90)
        pre_peak = window[look_back_start:peak_idx]
        if len(pre_peak) < 5:
            null["fib_reason_codes"] = ["INSUFFICIENT_PRE_PEAK_BARS"]
            return null

        base_idx   = min(range(len(pre_peak)), key=lambda i: _bar_low(pre_peak[i]))
        base_price = _bar_low(pre_peak[base_idx])
        if base_price <= 0 or peak_price <= base_price:
            null["fib_reason_codes"] = ["INVALID_BASE_PRICE"]
            return null

        impulse_range = peak_price - base_price
        impulse_pct   = round(impulse_range / base_price * 100, 2)

        if impulse_pct < _MIN_IMPULSE_PCT:
            null["fib_reason_codes"] = [f"IMPULSE_TOO_SMALL_{impulse_pct:.1f}PCT"]
            return null

        # ── Step 3: retracement levels (from peak downward) ───────────────────
        retrace: dict[str, float] = {}
        for lbl, ratio in _FIB_RETRACE_RATIOS:
            retrace[lbl] = round(peak_price - impulse_range * ratio, 4)
        retrace["FIB_1.000"] = round(peak_price, 4)  # anchor at prior high

        # ── Step 4: extension levels (beyond peak) ────────────────────────────
        extend: dict[str, float] = {}
        for lbl, ratio in _FIB_EXTEND_RATIOS:
            extend[lbl] = round(base_price + impulse_range * ratio, 4)

        # ── Step 5: nearest level to current price ────────────────────────────
        all_levels: dict[str, float] = {**retrace, **extend}
        prox_thr = _PROXIMITY_PCT.get(ticker_type, 2.5)

        nearest_label: Optional[str] = None
        nearest_level: Optional[float] = None
        nearest_dist:  Optional[float] = None

        for lbl, lvl in all_levels.items():
            if lvl <= 0:
                continue
            d = abs(price - lvl) / lvl * 100
            if nearest_dist is None or d < nearest_dist:
                nearest_dist  = round(d, 3)
                nearest_label = lbl
                nearest_level = lvl

        # ── Step 6: retest classification ─────────────────────────────────────
        fib_retest_detected = (
            nearest_label is not None
            and nearest_dist is not None
            and nearest_dist <= prox_thr
        )

        fib_retest_type: Optional[str] = None
        if fib_retest_detected and nearest_label:
            if nearest_label == "FIB_1.000":
                fib_retest_type = "PRIOR_RESISTANCE_RETEST"
            elif nearest_label in ("FIB_0.618", "FIB_0.786"):
                fib_retest_type = "DEEP_RETRACEMENT_RETEST"
            elif nearest_label in ("FIB_0.382", "FIB_0.500"):
                fib_retest_type = "SHALLOW_RETRACEMENT_RETEST"
            elif nearest_label in ("FIB_1.272", "FIB_1.618"):
                fib_retest_type = "EXTENSION_TARGET_RETEST"
            else:
                fib_retest_type = "FIB_LEVEL_PROXIMITY"

        # ── Step 7: reason codes ──────────────────────────────────────────────
        reason_codes: list[str] = [f"IMPULSE_{impulse_pct:.0f}PCT"]
        if fib_retest_detected:
            reason_codes.append(
                f"RETEST_{nearest_label}_{nearest_dist:.1f}PCT_AWAY"
            )
        elif nearest_label and nearest_dist is not None:
            reason_codes.append(
                f"NEAREST_{nearest_label}_{nearest_dist:.1f}PCT_AWAY"
            )

        return {
            "fib_computed":                True,
            "fib_anchor_low":              round(base_price, 4),
            "fib_anchor_high":             round(peak_price, 4),
            "fib_impulse_pct":             impulse_pct,
            # Retracement levels
            "fib_0382":                    retrace["FIB_0.382"],
            "fib_0500":                    retrace["FIB_0.500"],
            "fib_0618":                    retrace["FIB_0.618"],
            "fib_0786":                    retrace["FIB_0.786"],
            "fib_1000":                    retrace["FIB_1.000"],
            # Extension levels
            "fib_1272":                    extend["FIB_1.272"],
            "fib_1618":                    extend["FIB_1.618"],
            "fib_2000":                    extend["FIB_2.000"],
            "fib_2618":                    extend["FIB_2.618"],
            # Nearest level
            "nearest_fib_label":           nearest_label,
            "nearest_fib_level":           nearest_level,
            "distance_to_nearest_fib_pct": nearest_dist,
            # Retest detection
            "fib_retest_detected":         fib_retest_detected,
            "fib_retest_type":             fib_retest_type,
            # Targets
            "fib_target_1":                extend["FIB_1.272"],
            "fib_target_2":                extend["FIB_1.618"],
            # Meta
            "fib_reason_codes":            reason_codes,
        }

    except Exception as exc:
        result = _null_result()
        result["fib_reason_codes"] = [f"FIB_ENGINE_ERROR_{type(exc).__name__}"]
        return result


# ── Null result skeleton ───────────────────────────────────────────────────────

def _null_result() -> dict:
    return {
        "fib_computed":                False,
        "fib_anchor_low":              None,
        "fib_anchor_high":             None,
        "fib_impulse_pct":             None,
        "fib_0382":                    None,
        "fib_0500":                    None,
        "fib_0618":                    None,
        "fib_0786":                    None,
        "fib_1000":                    None,
        "fib_1272":                    None,
        "fib_1618":                    None,
        "fib_2000":                    None,
        "fib_2618":                    None,
        "nearest_fib_label":           None,
        "nearest_fib_level":           None,
        "distance_to_nearest_fib_pct": None,
        "fib_retest_detected":         False,
        "fib_retest_type":             None,
        "fib_target_1":                None,
        "fib_target_2":                None,
        "fib_reason_codes":            [],
    }
