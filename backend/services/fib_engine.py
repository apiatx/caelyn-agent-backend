"""
fib_engine.py — Multi-Timeframe Fibonacci Anchor Engine
=========================================================
V4.2.5.1 — Replaced single 120-bar anchor with multi-candidate system.

Candidate types
---------------
  recent_daily_impulse        40–120 daily bars
  intermediate_daily_impulse  120–252 daily bars
  long_daily_impulse          252–756 daily bars  (if data available)
  weekly_impulse              52/104/156 weekly bars (resampled from daily)
  monthly_impulse             24/36/60 monthly bars  (resampled from daily)

Anchor strategy
---------------
  1. Find all confirmed pivot highs (N=5 lookback/lookahead).
  2. Select most prominent as anchor peak.
  3. Find confirmed pivot lows before peak; select lowest.
  4. Fall back to global extremes if no clean pivots.

Primary selection
-----------------
  Score all candidates by proximity + impulse size + pivot quality + recency.
  Candidate with highest score becomes the primary.
  FIB_1.000 (prior-resistance retest) gets a significant proximity bonus.

Backward compatibility
----------------------
  All V4.2.5 field names preserved as aliases to the primary candidate.
  New fields added with primary_fib_* prefix.

Zero provider calls.  Zero LLM calls.
"""
from __future__ import annotations

from datetime import datetime as _DT
from typing import Optional


# ── V4.2.5.2 depth-confidence helpers (no external imports) ───────────────────

def _hist_status_fib(bar_count: int, is_actual_limit: bool = False) -> str:
    if is_actual_limit:   return "actual_ticker_history_limit"
    if bar_count >= 1100: return "available_5y"
    if bar_count >= 700:  return "available_3y"
    if bar_count >= 504:  return "partial_history"
    if bar_count >= 252:  return "intermediate_only"
    if bar_count >= 40:   return "recent_only"
    return "insufficient_history"

def _depth_conf_fib(bar_count: int, is_actual_limit: bool = False) -> float:
    if is_actual_limit:
        return round(min(1.0, bar_count / 252 * 0.75), 2) if bar_count else 0.25
    if bar_count >= 1300: return 1.00
    if bar_count >= 756:  return 0.85
    if bar_count >= 504:  return 0.70
    if bar_count >= 252:  return 0.50
    return 0.25

def _fib_scope_str(bar_count: int, weekly: int = 0, monthly: int = 0) -> str:
    if bar_count >= 756:  return "multi_year"
    if bar_count >= 504:  return "long"
    if bar_count >= 252:  return "intermediate"
    if bar_count >= 40:   return "recent"
    return "insufficient"

def _depth_reason_fib(bar_count: int, source: str, is_actual_limit: bool) -> Optional[str]:
    if is_actual_limit:   return "actual_ticker_history_limit"
    if bar_count >= 1100: return None
    if bar_count >= 756:  return "below_5y_target"
    if bar_count >= 504:  return "partial_2_3y_range"
    if bar_count >= 252:  return "intermediate_only_1y"
    if bar_count >= 40:   return f"recent_only_{source}"
    return "insufficient_bars"


# ── Level ratios ───────────────────────────────────────────────────────────────

_FIB_RETRACE: list[tuple[str, float]] = [
    ("FIB_0.236", 0.236),
    ("FIB_0.382", 0.382),
    ("FIB_0.500", 0.500),
    ("FIB_0.618", 0.618),
    ("FIB_0.786", 0.786),
    ("FIB_1.000", 1.000),
]

_FIB_EXTEND: list[tuple[str, float]] = [
    ("FIB_1.272", 1.272),
    ("FIB_1.618", 1.618),
    ("FIB_2.000", 2.000),
    ("FIB_2.618", 2.618),
]

_PROX_PCT: dict[str, float] = {"standard": 2.5, "volatile": 4.0}
_MIN_IMPULSE_PCT = 12.0   # below this the levels are noise
_PIVOT_N          = 5     # lookback/lookahead bars for pivot detection


# ── Candidate window configurations ───────────────────────────────────────────

_DAILY_WINDOWS: list[tuple[str, list[int]]] = [
    ("recent_daily_impulse",        [40, 80, 120]),
    ("intermediate_daily_impulse",  [130, 180, 252]),
    ("long_daily_impulse",          [300, 504, 756]),
]

_WEEKLY_WINDOWS  = [52, 104, 156]   # in weekly bars
_MONTHLY_WINDOWS = [24, 36, 60]     # in monthly bars


# ── Bar field helpers ──────────────────────────────────────────────────────────

def _bar_high(b: dict) -> float:
    try:
        v = b.get("high")
        return float(v) if v is not None else float(b.get("close") or 0)
    except Exception:
        return 0.0


def _bar_low(b: dict) -> float:
    try:
        v = b.get("low")
        return float(v) if v is not None else float(b.get("close") or 0)
    except Exception:
        return 0.0


def _bar_close(b: dict) -> float:
    try:
        return float(b.get("close") or 0)
    except Exception:
        return 0.0


def _bar_open(b: dict) -> float:
    try:
        v = b.get("open")
        return float(v) if v is not None else _bar_close(b)
    except Exception:
        return 0.0


def _bar_date(b: dict) -> str:
    return str(b.get("date") or "")[:10]


# ── Resampling ────────────────────────────────────────────────────────────────

def _resample_weekly(bars: list[dict]) -> list[dict]:
    """Group daily OHLC bars into ISO-weekly bars (week end = Friday)."""
    weeks: dict[str, list[dict]] = {}
    for bar in bars:
        raw = _bar_date(bar)
        if not raw or len(raw) < 10:
            continue
        try:
            d    = _DT.strptime(raw, "%Y-%m-%d")
            iso  = d.isocalendar()
            key  = f"{iso[0]:04d}-{iso[1]:02d}"
            weeks.setdefault(key, []).append(bar)
        except Exception:
            continue
    result = []
    for key in sorted(weeks):
        grp = sorted(weeks[key], key=_bar_date)
        result.append({
            "date":   _bar_date(grp[-1]),
            "open":   _bar_open(grp[0]),
            "high":   max(_bar_high(b) for b in grp),
            "low":    min(_bar_low(b)  for b in grp),
            "close":  _bar_close(grp[-1]),
            "volume": sum(float(b.get("volume") or 0) for b in grp),
        })
    return result


def _resample_monthly(bars: list[dict]) -> list[dict]:
    """Group daily OHLC bars into calendar-monthly bars."""
    months: dict[str, list[dict]] = {}
    for bar in bars:
        raw = _bar_date(bar)
        if not raw or len(raw) < 7:
            continue
        key = raw[:7]   # "YYYY-MM"
        months.setdefault(key, []).append(bar)
    result = []
    for key in sorted(months):
        grp = sorted(months[key], key=_bar_date)
        result.append({
            "date":   _bar_date(grp[-1]),
            "open":   _bar_open(grp[0]),
            "high":   max(_bar_high(b) for b in grp),
            "low":    min(_bar_low(b)  for b in grp),
            "close":  _bar_close(grp[-1]),
            "volume": sum(float(b.get("volume") or 0) for b in grp),
        })
    return result


# ── Pivot helpers ──────────────────────────────────────────────────────────────

def _pivot_highs(bars: list[dict], N: int = _PIVOT_N) -> list[tuple[int, float, str]]:
    """Return (index, high, date) for confirmed pivot highs."""
    n   = len(bars)
    out = []
    for i in range(N, n - N):
        h = _bar_high(bars[i])
        if h <= 0:
            continue
        left  = max((_bar_high(bars[j]) for j in range(max(0, i - N), i)),        default=0.0)
        right = max((_bar_high(bars[j]) for j in range(i + 1, min(n, i + N + 1))), default=0.0)
        if h > left and h > right:
            out.append((i, h, _bar_date(bars[i])))
    return out


def _pivot_lows(bars: list[dict], N: int = _PIVOT_N) -> list[tuple[int, float, str]]:
    """Return (index, low, date) for confirmed pivot lows."""
    n   = len(bars)
    out = []
    for i in range(N, n - N):
        lo = _bar_low(bars[i])
        if lo <= 0:
            continue
        left  = min((_bar_low(bars[j]) for j in range(max(0, i - N), i)),         default=1e9)
        right = min((_bar_low(bars[j]) for j in range(i + 1, min(n, i + N + 1))), default=1e9)
        if lo < left and lo < right:
            out.append((i, lo, _bar_date(bars[i])))
    return out


# ── Anchor detection ──────────────────────────────────────────────────────────

def _find_anchor(bars: list[dict]) -> Optional[dict]:
    """
    Identify the most meaningful base → peak impulse anchor.

    Returns dict with keys:
      base_price, peak_price, base_date, peak_date,
      base_idx, peak_idx, pivot_quality ("pivot" | "extreme")
    or None if insufficient / no valid anchor.
    """
    n = len(bars)
    if n < _PIVOT_N * 2 + 2:
        return None

    # ── Peak: prefer highest confirmed pivot high ─────────────────────────────
    ph = _pivot_highs(bars)
    if ph:
        peak_idx, peak_price, peak_date = max(ph, key=lambda x: x[1])
        pivot_quality = "pivot"
    else:
        peak_idx   = max(range(n), key=lambda i: _bar_high(bars[i]))
        peak_price = _bar_high(bars[peak_idx])
        peak_date  = _bar_date(bars[peak_idx])
        pivot_quality = "extreme"

    if peak_price <= 0:
        return None

    # ── Base: lowest confirmed pivot low before peak ──────────────────────────
    pre = bars[:peak_idx]
    if len(pre) < _PIVOT_N * 2 + 2:
        if not pre:
            return None
        base_local = min(range(len(pre)), key=lambda i: _bar_low(pre[i]))
        base_price = _bar_low(pre[base_local])
        base_date  = _bar_date(pre[base_local])
    else:
        pl = _pivot_lows(pre)
        if pl:
            _, base_price, base_date = min(pl, key=lambda x: x[1])
        else:
            base_local = min(range(len(pre)), key=lambda i: _bar_low(pre[i]))
            base_price = _bar_low(pre[base_local])
            base_date  = _bar_date(pre[base_local])
            if pivot_quality == "pivot":
                pivot_quality = "mixed"

    if base_price <= 0 or peak_price <= base_price:
        return None

    return {
        "base_price":    base_price,
        "peak_price":    peak_price,
        "base_date":     base_date,
        "peak_date":     peak_date,
        "peak_idx":      peak_idx,
        "pivot_quality": pivot_quality,
    }


# ── Single-window candidate computation ───────────────────────────────────────

def _compute_one_candidate(
    bars_slice:    list[dict],
    context_type:  str,
    timeframe:     str,
    window_size:   int,
    current_price: float,
    prox_thr:      float,
    total_bar_n:   int,          # original daily bar count (for recency scoring)
) -> Optional[dict]:
    """
    Compute fib levels for one window slice.
    Returns a candidate dict or None if anchor / impulse invalid.
    """
    anchor = _find_anchor(bars_slice)
    if anchor is None:
        return None

    base_price = anchor["base_price"]
    peak_price = anchor["peak_price"]
    impulse    = peak_price - base_price
    imp_pct    = round(impulse / base_price * 100, 2)

    if imp_pct < _MIN_IMPULSE_PCT:
        return None

    # ── Levels ────────────────────────────────────────────────────────────────
    retrace: dict[str, float] = {}
    for lbl, ratio in _FIB_RETRACE:
        retrace[lbl] = round(peak_price - impulse * ratio, 4)
    retrace["FIB_1.000"] = round(peak_price, 4)

    extend: dict[str, float] = {}
    for lbl, ratio in _FIB_EXTEND:
        extend[lbl] = round(base_price + impulse * ratio, 4)

    all_levels = {**retrace, **extend}

    # ── Nearest level ─────────────────────────────────────────────────────────
    nearest_label: Optional[str]  = None
    nearest_level: Optional[float] = None
    nearest_dist:  Optional[float] = None

    for lbl, lvl in all_levels.items():
        if lvl <= 0:
            continue
        d = abs(current_price - lvl) / lvl * 100
        if nearest_dist is None or d < nearest_dist:
            nearest_dist  = round(d, 3)
            nearest_label = lbl
            nearest_level = lvl

    # ── Retest classification ─────────────────────────────────────────────────
    retest_detected = (
        nearest_label is not None
        and nearest_dist is not None
        and nearest_dist <= prox_thr
    )

    retest_type: Optional[str] = None
    if retest_detected and nearest_label:
        if nearest_label == "FIB_1.000":
            retest_type = "PRIOR_RESISTANCE_RETEST"
        elif nearest_label in ("FIB_0.618", "FIB_0.786"):
            retest_type = "DEEP_RETRACEMENT_RETEST"
        elif nearest_label in ("FIB_0.382", "FIB_0.500", "FIB_0.236"):
            retest_type = "SHALLOW_RETRACEMENT_RETEST"
        elif nearest_label in ("FIB_1.272", "FIB_1.618"):
            retest_type = "EXTENSION_TARGET_RETEST"
        elif nearest_label in ("FIB_2.000", "FIB_2.618"):
            retest_type = "FAR_EXTENSION_TARGET"
        else:
            retest_type = "FIB_LEVEL_PROXIMITY"

    # ── Candidate score (0–100) ────────────────────────────────────────────────
    # Proximity (0–40): primary driver
    if nearest_dist is not None and nearest_dist <= prox_thr:
        prox_score = 40.0 * max(0.0, 1.0 - nearest_dist / prox_thr)
        if nearest_label == "FIB_1.000":
            prox_score = min(prox_score + 15.0, 40.0)   # FIB_1.000 bonus
    elif nearest_dist is not None and nearest_dist <= prox_thr * 3:
        prox_score = 20.0 * max(0.0, 1.0 - (nearest_dist - prox_thr) / (prox_thr * 2))
    else:
        prox_score = 0.0

    # Impulse strength (0–20)
    imp_score = min(imp_pct / 200.0 * 20.0, 20.0)

    # Pivot quality (0–10)
    pq_map = {"pivot": 10.0, "mixed": 5.0, "extreme": 2.0}
    pq_score = pq_map.get(anchor["pivot_quality"], 2.0)

    # Timeframe preference (0–15): weekly > monthly > long_daily > intermediate > recent
    tf_map = {
        "weekly":   15.0,
        "monthly":  12.0,
        "daily":     5.0,
    }
    tf_base = tf_map.get(timeframe, 5.0)
    # Within daily, reward longer windows
    if timeframe == "daily":
        if "long" in context_type:
            tf_base = 10.0
        elif "intermediate" in context_type:
            tf_base = 7.0
        else:
            tf_base = 5.0

    # Recency: current price still within the structure range
    struct_min = base_price * 0.85
    struct_max = peak_price * 1.50
    in_range = struct_min <= current_price <= struct_max
    recency_score = 10.0 if in_range else 0.0

    candidate_score = round(prox_score + imp_score + pq_score + tf_base + recency_score, 2)

    return {
        "fib_context":               context_type,
        "fib_timeframe":             timeframe,
        "fib_window":                window_size,
        "fib_anchor_low":            round(base_price, 4),
        "fib_anchor_high":           round(peak_price, 4),
        "fib_anchor_low_date":       anchor["base_date"],
        "fib_anchor_high_date":      anchor["peak_date"],
        "fib_impulse_pct":           imp_pct,
        "fib_anchor_confidence":     anchor["pivot_quality"],
        # Retracement levels
        "fib_0236":                  retrace.get("FIB_0.236"),
        "fib_0382":                  retrace.get("FIB_0.382"),
        "fib_0500":                  retrace.get("FIB_0.500"),
        "fib_0618":                  retrace.get("FIB_0.618"),
        "fib_0786":                  retrace.get("FIB_0.786"),
        "fib_1000":                  retrace.get("FIB_1.000"),
        # Extension levels
        "fib_1272":                  extend.get("FIB_1.272"),
        "fib_1618":                  extend.get("FIB_1.618"),
        "fib_2000":                  extend.get("FIB_2.000"),
        "fib_2618":                  extend.get("FIB_2.618"),
        # Nearest
        "nearest_fib_label":         nearest_label,
        "nearest_fib_level":         nearest_level,
        "distance_to_nearest_fib_pct": nearest_dist,
        # Retest
        "fib_retest_detected":       retest_detected,
        "fib_retest_type":           retest_type,
        # Targets
        "fib_target_1":              extend.get("FIB_1.272"),
        "fib_target_2":              extend.get("FIB_1.618"),
        # Score
        "candidate_score":           candidate_score,
    }


# ── Multi-candidate builder ────────────────────────────────────────────────────

def _build_candidates(
    bars_daily:   list[dict],
    current_price: float,
    prox_thr:     float,
) -> list[dict]:
    """Generate all valid Fib candidates across timeframes and window sizes."""
    candidates: list[dict] = []
    n = len(bars_daily)

    # ── Daily candidates ──────────────────────────────────────────────────────
    for context_type, windows in _DAILY_WINDOWS:
        for w in windows:
            if n < max(w // 2, 20):
                continue
            actual_w    = min(w, n)
            bars_slice  = bars_daily[-actual_w:]
            cand = _compute_one_candidate(
                bars_slice, context_type, "daily", actual_w, current_price, prox_thr, n,
            )
            if cand:
                candidates.append(cand)

    # ── Weekly candidates ─────────────────────────────────────────────────────
    bars_weekly = _resample_weekly(bars_daily)
    nw = len(bars_weekly)
    for w in _WEEKLY_WINDOWS:
        if nw < w // 2:
            continue
        actual_w   = min(w, nw)
        wslice     = bars_weekly[-actual_w:]
        cand = _compute_one_candidate(
            wslice, "weekly_impulse", "weekly", actual_w, current_price, prox_thr, nw,
        )
        if cand:
            candidates.append(cand)

    # ── Monthly candidates ────────────────────────────────────────────────────
    bars_monthly = _resample_monthly(bars_daily)
    nm = len(bars_monthly)
    for w in _MONTHLY_WINDOWS:
        if nm < w // 2:
            continue
        actual_w   = min(w, nm)
        mslice     = bars_monthly[-actual_w:]
        cand = _compute_one_candidate(
            mslice, "monthly_impulse", "monthly", actual_w, current_price, prox_thr, nm,
        )
        if cand:
            candidates.append(cand)

    return candidates


# ── Primary selection ─────────────────────────────────────────────────────────

def _select_primary(candidates: list[dict]) -> Optional[dict]:
    if not candidates:
        return None
    return max(candidates, key=lambda c: c["candidate_score"])


# ── Confidence mapping ────────────────────────────────────────────────────────

def _confidence_score(primary: dict) -> float:
    """
    Convert candidate_score (0–100) to fib_confidence (0.0–1.0).
    Additional gate: must have at least 12% impulse.
    """
    score  = primary.get("candidate_score", 0.0)
    imp    = primary.get("fib_impulse_pct", 0.0) or 0.0
    conf   = round(min(score / 75.0, 1.0), 3)
    if imp < _MIN_IMPULSE_PCT:
        conf = min(conf, 0.3)
    return conf


# ── Null result ────────────────────────────────────────────────────────────────

def _null_result(reason_codes: Optional[list[str]] = None) -> dict:
    return {
        # Legacy fields
        "fib_computed":                  False,
        "fib_anchor_low":                None,
        "fib_anchor_high":               None,
        "fib_anchor_low_date":           None,
        "fib_anchor_high_date":          None,
        "fib_impulse_pct":               None,
        "fib_0236":                      None,
        "fib_0382":                      None,
        "fib_0500":                      None,
        "fib_0618":                      None,
        "fib_0786":                      None,
        "fib_1000":                      None,
        "fib_1272":                      None,
        "fib_1618":                      None,
        "fib_2000":                      None,
        "fib_2618":                      None,
        "nearest_fib_label":             None,
        "nearest_fib_level":             None,
        "distance_to_nearest_fib_pct":   None,
        "fib_retest_detected":           False,
        "fib_retest_type":               None,
        "fib_target_1":                  None,
        "fib_target_2":                  None,
        "fib_reason_codes":              reason_codes or [],
        # Primary context fields
        "primary_fib_context":           "unavailable",
        "primary_fib_timeframe":         None,
        "primary_fib_window":            None,
        "primary_fib_confidence":        0.0,
        "primary_fib_anchor_low":        None,
        "primary_fib_anchor_high":       None,
        "primary_fib_anchor_low_date":   None,
        "primary_fib_anchor_high_date":  None,
        "primary_nearest_fib_label":     None,
        "primary_nearest_fib_level":     None,
        "primary_distance_to_fib_pct":   None,
        "primary_fib_retest_detected":   False,
        "primary_fib_retest_type":       None,
        "primary_fib_target_1":          None,
        "primary_fib_target_2":          None,
        "fib_candidates_count":          0,
        "fib_candidates_summary":        [],
        # Bar depth metadata
        "fib_daily_bar_count":           None,
        "fib_weekly_bar_count":          None,
        "fib_monthly_bar_count":         None,
        "fib_years_available":           None,
        "fib_long_term_available":       False,
        # V4.2.5.2 depth fields
        "fib_history_status":            "insufficient_history",
        "fib_history_source":            "unknown",
        "fib_long_history_used":         False,
        "fib_data_depth_confidence":     0.25,
        "fib_data_limitation_reason":    "insufficient_bars",
        "fib_multi_year_available":      False,
        "fib_weekly_available":          False,
        "fib_monthly_available":         False,
        "fib_timeframe_scope":           "insufficient",
    }


# ── Public API ─────────────────────────────────────────────────────────────────

def compute_fib_levels(
    bars:                    list[dict],
    current_price:           Optional[float] = None,
    ticker_type:             str             = "standard",
    history_source:          str             = "unknown",
    is_actual_history_limit: bool            = False,
) -> dict:
    """
    Multi-timeframe Fibonacci level computation.

    Parameters
    ----------
    bars          : Daily OHLCV bars oldest → newest.  Keys: date, open, high, low, close.
    current_price : Live price override; uses last bar close when None.
    ticker_type   : "standard" (2.5 % proximity) | "volatile" (4.0 %).

    Returns
    -------
    Flat dict.  All legacy V4.2.5 keys preserved as aliases to primary candidate.
    New keys: primary_fib_context, primary_fib_timeframe, primary_fib_window,
              primary_fib_confidence, primary_fib_anchor_*, primary_nearest_fib_*,
              fib_candidates_count, fib_candidates_summary,
              fib_daily_bar_count, fib_weekly_bar_count, fib_monthly_bar_count,
              fib_years_available, fib_long_term_available.
    """
    null = _null_result

    if not bars or len(bars) < 20:
        return null(["INSUFFICIENT_BARS"])

    try:
        bars_s = sorted(bars, key=lambda b: _bar_date(b))
        n_daily = len(bars_s)

        # ── Current price ─────────────────────────────────────────────────────
        if current_price is not None:
            try:
                price = float(current_price)
            except (TypeError, ValueError):
                price = _bar_close(bars_s[-1])
        else:
            price = _bar_close(bars_s[-1])

        if price <= 0:
            return null(["INVALID_CURRENT_PRICE"])

        prox_thr    = _PROX_PCT.get(ticker_type, 2.5)
        bars_weekly  = _resample_weekly(bars_s)
        bars_monthly = _resample_monthly(bars_s)
        n_weekly     = len(bars_weekly)
        n_monthly    = len(bars_monthly)
        years_avail  = round(n_daily / 252, 1)
        long_avail   = n_daily >= 252

        # ── Build all candidates ──────────────────────────────────────────────
        candidates = _build_candidates(bars_s, price, prox_thr)

        if not candidates:
            r = null(["NO_VALID_ANCHOR_FOUND"])
            r["fib_daily_bar_count"]        = n_daily
            r["fib_weekly_bar_count"]       = n_weekly
            r["fib_monthly_bar_count"]      = n_monthly
            r["fib_years_available"]        = years_avail
            r["fib_long_term_available"]    = long_avail
            r["fib_history_status"]         = _hist_status_fib(n_daily, is_actual_history_limit)
            r["fib_history_source"]         = history_source
            r["fib_long_history_used"]      = n_daily >= 756
            r["fib_data_depth_confidence"]  = _depth_conf_fib(n_daily, is_actual_history_limit)
            r["fib_data_limitation_reason"] = _depth_reason_fib(n_daily, history_source, is_actual_history_limit)
            r["fib_multi_year_available"]   = n_daily >= 756
            r["fib_weekly_available"]       = n_weekly >= 26
            r["fib_monthly_available"]      = n_monthly >= 12
            r["fib_timeframe_scope"]        = _fib_scope_str(n_daily, n_weekly, n_monthly)
            if n_daily < 252:
                r["fib_reason_codes"].append("FIB_LONG_TERM_DATA_UNAVAILABLE")
            return r

        # ── Select primary ────────────────────────────────────────────────────
        primary  = _select_primary(candidates)
        conf     = _confidence_score(primary)

        # ── Reason codes ──────────────────────────────────────────────────────
        reason_codes: list[str] = [
            f"IMPULSE_{primary['fib_impulse_pct']:.0f}PCT",
            f"PRIMARY_{primary['fib_context'].upper()}",
        ]
        if len(candidates) > 1:
            reason_codes.append("FIB_MULTI_TIMEFRAME_PRIMARY_SELECTED")
        if primary["fib_retest_detected"]:
            lbl = primary["nearest_fib_label"] or ""
            dist = primary["distance_to_nearest_fib_pct"] or 0
            reason_codes.append(f"RETEST_{lbl}_{dist:.1f}PCT_AWAY")
            rt = primary["fib_retest_type"] or ""
            if lbl == "FIB_1.000":
                reason_codes.append("FIB_1000_RETEST_ENTRY")
            elif rt == "SHALLOW_RETRACEMENT_RETEST":
                reason_codes.append(f"FIB_{lbl.replace('FIB_', '').replace('.', '')}_RETEST_ENTRY")
            elif rt == "DEEP_RETRACEMENT_RETEST":
                reason_codes.append(f"FIB_{lbl.replace('FIB_', '').replace('.', '')}_RETEST_ENTRY")
            elif "EXTENSION" in rt:
                reason_codes.append(f"FIB_EXTENSION_TARGET_{lbl.replace('FIB_', '').replace('.', '')}")
        else:
            lbl = primary["nearest_fib_label"] or ""
            dist = primary["distance_to_nearest_fib_pct"] or 0
            reason_codes.append(f"NEAREST_{lbl}_{dist:.1f}PCT_AWAY")

        if n_daily < 252:
            reason_codes.append("FIB_LONG_TERM_DATA_UNAVAILABLE")
        if primary["fib_anchor_confidence"] in ("extreme", "mixed"):
            reason_codes.append("FIB_ANCHOR_LOW_CONFIDENCE")

        # ── Candidate summary (compact, for diagnostics) ──────────────────────
        cand_summary = [
            {
                "context":    c["fib_context"],
                "timeframe":  c["fib_timeframe"],
                "window":     c["fib_window"],
                "score":      c["candidate_score"],
                "impulse_pct": c["fib_impulse_pct"],
                "retest":     c["fib_retest_detected"],
                "nearest":    c["nearest_fib_label"],
                "dist_pct":   c["distance_to_nearest_fib_pct"],
            }
            for c in sorted(candidates, key=lambda x: x["candidate_score"], reverse=True)[:5]
        ]

        # ── Assemble result ───────────────────────────────────────────────────
        return {
            # ── Legacy fields (alias to primary) ─────────────────────────────
            "fib_computed":                  True,
            "fib_anchor_low":                primary["fib_anchor_low"],
            "fib_anchor_high":               primary["fib_anchor_high"],
            "fib_anchor_low_date":           primary["fib_anchor_low_date"],
            "fib_anchor_high_date":          primary["fib_anchor_high_date"],
            "fib_impulse_pct":               primary["fib_impulse_pct"],
            "fib_0236":                      primary["fib_0236"],
            "fib_0382":                      primary["fib_0382"],
            "fib_0500":                      primary["fib_0500"],
            "fib_0618":                      primary["fib_0618"],
            "fib_0786":                      primary["fib_0786"],
            "fib_1000":                      primary["fib_1000"],
            "fib_1272":                      primary["fib_1272"],
            "fib_1618":                      primary["fib_1618"],
            "fib_2000":                      primary["fib_2000"],
            "fib_2618":                      primary["fib_2618"],
            "nearest_fib_label":             primary["nearest_fib_label"],
            "nearest_fib_level":             primary["nearest_fib_level"],
            "distance_to_nearest_fib_pct":   primary["distance_to_nearest_fib_pct"],
            "fib_retest_detected":           primary["fib_retest_detected"],
            "fib_retest_type":               primary["fib_retest_type"],
            "fib_target_1":                  primary["fib_target_1"],
            "fib_target_2":                  primary["fib_target_2"],
            "fib_reason_codes":              reason_codes,
            # ── Primary context fields (new in V4.2.5.1) ─────────────────────
            "primary_fib_context":           primary["fib_context"],
            "primary_fib_timeframe":         primary["fib_timeframe"],
            "primary_fib_window":            primary["fib_window"],
            "primary_fib_confidence":        conf,
            "primary_fib_anchor_low":        primary["fib_anchor_low"],
            "primary_fib_anchor_high":       primary["fib_anchor_high"],
            "primary_fib_anchor_low_date":   primary["fib_anchor_low_date"],
            "primary_fib_anchor_high_date":  primary["fib_anchor_high_date"],
            "primary_nearest_fib_label":     primary["nearest_fib_label"],
            "primary_nearest_fib_level":     primary["nearest_fib_level"],
            "primary_distance_to_fib_pct":   primary["distance_to_nearest_fib_pct"],
            "primary_fib_retest_detected":   primary["fib_retest_detected"],
            "primary_fib_retest_type":       primary["fib_retest_type"],
            "primary_fib_target_1":          primary["fib_target_1"],
            "primary_fib_target_2":          primary["fib_target_2"],
            # ── Multi-candidate metadata ──────────────────────────────────────
            "fib_candidates_count":          len(candidates),
            "fib_candidates_summary":        cand_summary,
            # ── Bar depth metadata ────────────────────────────────────────────
            "fib_daily_bar_count":           n_daily,
            "fib_weekly_bar_count":          n_weekly,
            "fib_monthly_bar_count":         n_monthly,
            "fib_years_available":           years_avail,
            "fib_long_term_available":       long_avail,
            # ── V4.2.5.2 depth fields ─────────────────────────────────────────
            "fib_history_status":            _hist_status_fib(n_daily, is_actual_history_limit),
            "fib_history_source":            history_source,
            "fib_long_history_used":         n_daily >= 756,
            "fib_data_depth_confidence":     _depth_conf_fib(n_daily, is_actual_history_limit),
            "fib_data_limitation_reason":    _depth_reason_fib(n_daily, history_source, is_actual_history_limit),
            "fib_multi_year_available":      n_daily >= 756,
            "fib_weekly_available":          n_weekly >= 26,
            "fib_monthly_available":         n_monthly >= 12,
            "fib_timeframe_scope":           _fib_scope_str(n_daily, n_weekly, n_monthly),
        }

    except Exception as exc:
        r = _null_result([f"FIB_ENGINE_ERROR_{type(exc).__name__}"])
        return r
