"""
entry_state_service.py — Phase 4: Deterministic Entry State Engine
====================================================================
Pure computation — zero external API calls, zero provider calls.

Given already-fetched daily bars + a stage_result dict (from analyze_symbol_stage),
classify the entry opportunity into a canonical family + state, compute an
entry_score (0–100), and identify support levels.

Entry Families
--------------
  PRE_MOVE         — base forming / coiling before breakout (buy-side setup)
  CONTINUATION     — pullback or retest in an established uptrend (buy-side setup)
  CHASE_EXHAUSTION — extended / overheated / late-stage (avoid / reduce)
  BROKEN_OR_UNCLEAR — downtrend, broken support, or insufficient data

Entry States (per family)
-------------------------
  PRE_MOVE:
    BASE_FORMING      — Stage 1 consolidation, no clear trigger
    COILED            — Stage 1/1-2 with contraction (squeeze)
    BREAKOUT_READY    — 1-2 Watch: coiled with confirming signals
    EARLY_ACCUMULATION— early Stage 2 on light-volume accumulation
    SIGNALS_BUILDING  — Stage 2 early with multiple confirming signals

  CONTINUATION:
    PULLBACK_IN_UPTREND — Stage 2 healthy pullback, MA stacked bull
    BREAKOUT_RETEST     — recent breakout retesting breakout level
    SUPPORT_HOLD        — holding key support (50d / 30w MA)
    CONSTRUCTIVE_DIP    — above all MAs, dipping into 20d
    REACCELERATION      — new momentum thrust after constructive reset

  CHASE_EXHAUSTION:
    EXTENDED            — 20–35% above 30w MA, manageable but elevated risk
    EXTREME_EXTENSION   — >35% above 30w MA, high chase risk
    VERTICAL            — overheated, likely to consolidate or reverse
    VOLUME_CLIMAX       — possible blow-off top signature
    CROWDED_MOVE        — wide public participation, late-cycle signal
    FAILED_BREAKOUT     — breakout that reversed back below breakout level

  BROKEN_OR_UNCLEAR:
    DOWNTREND           — Stage 4 / broken MA structure
    SUPPORT_LOST        — active critical support broken with lower-low confirmed
    RANGE_SUPPORT_TEST  — prior breakout pivot lost (now overhead); testing lower active support zone
    NO_CLEAR_ENTRY      — transitional / undefined structure
    INSUFFICIENT_DATA   — not enough bars for reliable classification

Entry Score (0–100, integer)
----------------------------
  A+  ≥ 85 | A ≥ 70 | B ≥ 55 | C ≥ 35 | AVOID < 35

Support Levels
--------------
  Returns up to 3 support levels with distance_pct from current price.
  Sources: SMA-20, SMA-50, SMA-200 (daily), 30w MA (weekly stage input).
"""

from __future__ import annotations

import json
import os
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

# ── LKG persistence ────────────────────────────────────────────────────────────
_LKG_PATH = Path(__file__).parent.parent / "data" / "entry_state_lkg.json"

_ENTRY_STATE_LKG: dict[str, dict] = {}   # symbol → last result
_LKG_LOADED = False

# ── Entry analysis model version ────────────────────────────────────────────────
# Bump this whenever Entry classification semantics/schema change (new states,
# new archetype fields, precedence changes, etc). Every newly computed result
# persists this value as "entry_analysis_version". Rows with a missing or older
# version are considered DERIVED-ANALYSIS-stale (distinct from market-data/TTL
# freshness) and must be recomputed from already-available daily bars — see
# is_entry_version_current() below. This is the single authoritative version;
# do not hardcode version numbers elsewhere.
#   1 = pre-LOW_BASE (legacy HIGH_BASE-only Entry Structure V2)
#   2 = LOW_BASE taxonomy added (LOW_BASE_FORMING/COILING/READY + base_archetype,
#       base_location, low_base_floor diagnostics)
ENTRY_ANALYSIS_VERSION = 4

# ── Entry family / state / grade constants ─────────────────────────────────────
FAMILY_PRE_MOVE       = "PRE_MOVE"
FAMILY_CONTINUATION   = "CONTINUATION"
FAMILY_EXHAUSTION     = "CHASE_EXHAUSTION"
FAMILY_BROKEN         = "BROKEN_OR_UNCLEAR"

GRADE_A_PLUS = "A+"
GRADE_A      = "A"
GRADE_B      = "B"
GRADE_C      = "C"
GRADE_AVOID  = "AVOID"

_BASE_SCORES: dict[str, int] = {
    # PRE_MOVE
    "BASE_FORMING":      50,
    "COILED":            62,
    "BREAKOUT_READY":    72,
    "EARLY_ACCUMULATION":54,
    "SIGNALS_BUILDING":  66,
    # CONTINUATION
    "HIGH_BASE":           80,   # legacy — no longer assigned by V2, kept for old LKG reads
    "HIGH_BASE_FORMING":   68,   # Entry Structure V2 — early-stage base, still developing
    "HIGH_BASE_COILING":   79,   # Entry Structure V2 — contracting range beneath ceiling
    "HIGH_BASE_READY":     88,   # Entry Structure V2 — mature, tight, pressing the ceiling
    "LOW_BASE_FORMING":    38,   # Entry Structure V2 — immature lower-region floor, momentum unproven
    "LOW_BASE_COILING":    56,   # Entry Structure V2 — mature floor tightening, momentum still unproven
    "LOW_BASE_READY":      70,   # Entry Structure V2 — mature floor + reclaim pressure, asymmetric turn setup
    "BREAKOUT_CONFIRMED":  83,   # Entry Structure V2 — confirmed move above real base ceiling
    "WAIT_FOR_RETEST":     58,   # Entry Structure V2 — extended post-breakout, no retest yet
    "TRENDLINE_SUPPORT_TEST": 69,  # Entry Structure V2 — pullback testing valid rising trendline
    "BREAKOUT_PULLBACK":   65,   # Entry Structure V2 — pullback toward former breakout pivot, pivot holding
    "PULLBACK_IN_UPTREND": 76,
    "BREAKOUT_RETEST":     81,
    "SUPPORT_HOLD":        70,
    "CONSTRUCTIVE_DIP":    67,
    "REACCELERATION":      87,
    # CHASE_EXHAUSTION
    "EXTENDED":           20,
    "EXTREME_EXTENSION":   8,
    "VERTICAL":            4,
    "VOLUME_CLIMAX":       7,
    "CROWDED_MOVE":       12,
    "FAILED_BREAKOUT":    10,
    # BROKEN_OR_UNCLEAR
    "DOWNTREND":           5,
    "SUPPORT_LOST":       14,
    "NO_CLEAR_ENTRY":     30,
    "INSUFFICIENT_DATA":  20,
    "REVERSAL_WATCH":     22,   # Entry Structure V2 — pivot lost, structure ambiguous, watch not entry
    # ── Broken-support nuanced states (Bug 2 — replaces blanket SUPPORT_LOST) ──
    "SUPPORT_TEST":        52,   # Marginal break below SMA50 — testing support, break unconfirmed
    "LOWER_HIGH_WARNING":  38,   # Lower high formed; no lower-low yet — structural caution
    "LOWER_LOW_CONFIRMED": 12,   # Lower-low confirmed with buffer — structural break validated
    # ── Active support zone states ────────────────────────────────────────────
    "RANGE_SUPPORT_TEST":  36,   # Prior pivot lost (overhead); testing lower active support zone
}

_STATE_TO_FAMILY: dict[str, str] = {
    "BASE_FORMING":      FAMILY_PRE_MOVE,
    "COILED":            FAMILY_PRE_MOVE,
    "BREAKOUT_READY":    FAMILY_PRE_MOVE,
    "EARLY_ACCUMULATION":FAMILY_PRE_MOVE,
    "SIGNALS_BUILDING":  FAMILY_PRE_MOVE,
    "HIGH_BASE":              FAMILY_CONTINUATION,
    "HIGH_BASE_FORMING":      FAMILY_CONTINUATION,
    "HIGH_BASE_COILING":      FAMILY_CONTINUATION,
    "HIGH_BASE_READY":        FAMILY_CONTINUATION,
    "LOW_BASE_FORMING":       FAMILY_PRE_MOVE,
    "LOW_BASE_COILING":       FAMILY_PRE_MOVE,
    "LOW_BASE_READY":         FAMILY_PRE_MOVE,
    "BREAKOUT_CONFIRMED":     FAMILY_CONTINUATION,
    "WAIT_FOR_RETEST":        FAMILY_CONTINUATION,
    "TRENDLINE_SUPPORT_TEST": FAMILY_CONTINUATION,
    "BREAKOUT_PULLBACK":      FAMILY_CONTINUATION,
    "PULLBACK_IN_UPTREND": FAMILY_CONTINUATION,
    "BREAKOUT_RETEST":     FAMILY_CONTINUATION,
    "SUPPORT_HOLD":        FAMILY_CONTINUATION,
    "CONSTRUCTIVE_DIP":    FAMILY_CONTINUATION,
    "REACCELERATION":      FAMILY_CONTINUATION,
    "EXTENDED":            FAMILY_EXHAUSTION,
    "EXTREME_EXTENSION":   FAMILY_EXHAUSTION,
    "VERTICAL":            FAMILY_EXHAUSTION,
    "VOLUME_CLIMAX":       FAMILY_EXHAUSTION,
    "CROWDED_MOVE":        FAMILY_EXHAUSTION,
    "FAILED_BREAKOUT":     FAMILY_EXHAUSTION,
    "DOWNTREND":           FAMILY_BROKEN,
    "SUPPORT_LOST":        FAMILY_BROKEN,
    "NO_CLEAR_ENTRY":      FAMILY_BROKEN,
    "INSUFFICIENT_DATA":   FAMILY_BROKEN,
    "REVERSAL_WATCH":      FAMILY_BROKEN,
    "SUPPORT_TEST":        FAMILY_BROKEN,
    "LOWER_HIGH_WARNING":  FAMILY_BROKEN,
    "LOWER_LOW_CONFIRMED": FAMILY_BROKEN,
    "RANGE_SUPPORT_TEST":  FAMILY_BROKEN,
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _entry_depth_status(bar_count: int) -> str:
    if bar_count >= 1100: return "available_5y"
    if bar_count >= 700:  return "available_3y"
    if bar_count >= 504:  return "partial_history"
    if bar_count >= 252:  return "intermediate_only"
    if bar_count >= 40:   return "recent_only"
    return "insufficient_history"

def _entry_depth_conf(bar_count: int) -> float:
    if bar_count >= 1300: return 1.00
    if bar_count >= 756:  return 0.85
    if bar_count >= 504:  return 0.70
    if bar_count >= 252:  return 0.50
    return 0.25

def _entry_depth_reason(bar_count: int, provider: str) -> Optional[str]:
    if bar_count >= 1100: return None
    if bar_count >= 756:  return "below_5y_target"
    if bar_count >= 504:  return "partial_2_3y_range"
    if bar_count >= 252:  return "intermediate_only_1y"
    if bar_count >= 40:   return f"recent_only_{provider}"
    return "insufficient_bars"

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sma(values: list[float], n: int) -> Optional[float]:
    if len(values) < n:
        return None
    return statistics.mean(values[-n:])


def _pct_vs(current: float, level: Optional[float]) -> Optional[float]:
    if level is None or level <= 0:
        return None
    return round((current - level) / level * 100, 2)


def _grade(score: int) -> str:
    if score >= 85:
        return GRADE_A_PLUS
    if score >= 70:
        return GRADE_A
    if score >= 55:
        return GRADE_B
    if score >= 35:
        return GRADE_C
    return GRADE_AVOID


def _load_lkg() -> None:
    global _ENTRY_STATE_LKG, _LKG_LOADED
    if _LKG_LOADED:
        return
    try:
        if _LKG_PATH.exists():
            raw = json.loads(_LKG_PATH.read_text())
            if isinstance(raw, dict):
                _ENTRY_STATE_LKG = raw
    except Exception:
        pass
    _LKG_LOADED = True


def _save_lkg(symbol: str, result: dict) -> None:
    global _ENTRY_STATE_LKG
    _ENTRY_STATE_LKG[symbol] = result
    try:
        _LKG_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = _LKG_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(_ENTRY_STATE_LKG, default=str))
        tmp.replace(_LKG_PATH)
    except Exception:
        pass


def _update_lkg_memory(symbol: str, result: dict) -> None:
    """Update in-memory LKG only — no disk write (for batch use)."""
    global _ENTRY_STATE_LKG
    _ENTRY_STATE_LKG[symbol] = result


def flush_entry_state_lkg() -> None:
    """
    Write the full in-memory entry state LKG to disk atomically.

    Call once after a batch warmup pass instead of writing per-symbol.
    Safe to call concurrently — uses the same tmp-rename pattern as _save_lkg.
    """
    _load_lkg()
    try:
        _LKG_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = _LKG_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(_ENTRY_STATE_LKG, default=str))
        tmp.replace(_LKG_PATH)
    except Exception:
        pass

    # ── Fib/Wave health assertion ────────────────────────────────────────────
    # If a large warmup run produced >90% missing primary_fib_context it almost
    # certainly means fib_engine or wave_structure_engine failed to import
    # (e.g. wrong path prefix).  Surface a high-severity warning immediately so
    # the failure cannot go undetected again.
    try:
        eligible = [
            v for v in _ENTRY_STATE_LKG.values()
            if isinstance(v, dict) and v.get("entry_state")
        ]
        if len(eligible) > 50:
            missing_fib = sum(
                1 for v in eligible
                if not v.get("primary_fib_context")
                or v["primary_fib_context"] == "unavailable"
            )
            missing_pct = missing_fib / len(eligible) * 100
            if missing_pct > 90:
                print(
                    f"[ENTRY_STATE][CRITICAL] FIB/WAVE HEALTH CHECK FAILED — "
                    f"{missing_fib}/{len(eligible)} rows ({missing_pct:.0f}%) are missing "
                    f"primary_fib_context after flush. "
                    f"This almost always means 'from services.fib_engine' or "
                    f"'from services.wave_structure_engine' raised ImportError silently. "
                    f"Check server working directory and import paths immediately."
                )
    except Exception:
        pass


# ── Support level extraction ────────────────────────────────────────────────────

def _build_support_levels(
    price: float,
    sma20: Optional[float],
    sma50: Optional[float],
    sma200: Optional[float],
    ma30w: Optional[float],  # 30w MA in price units (if available from stage_result)
) -> list[dict]:
    """
    Return support levels below current price, closest first.
    Each entry: {"label", "level", "distance_pct"}
    """
    candidates: list[tuple[str, float]] = []
    for label, lvl in [
        ("SMA20",  sma20),
        ("SMA50",  sma50),
        ("30w_MA", ma30w),
        ("SMA200", sma200),
    ]:
        if lvl is not None and lvl > 0 and lvl < price:
            candidates.append((label, lvl))
    # Sort by distance (closest = most negative pct_vs value)
    candidates.sort(key=lambda t: -(t[1]))  # closest to price = highest level value
    result = []
    for label, lvl in candidates[:3]:
        dist = round((lvl - price) / price * 100, 2)  # negative = below
        result.append({"label": label, "level": round(lvl, 4), "distance_pct": dist})
    return result


# ── Active Support Zone Engine ─────────────────────────────────────────────────

def _find_bounce_zone(sorted_bars: list[dict], price: float) -> Optional[dict]:
    """
    Identify the most significant price cluster below current price where
    price has bounced one or more times in the recent 60 bars.

    Uses 3-point local minima detection, clusters within 3 % tolerance,
    and scores by touches × recency × proximity.

    Returns the best cluster dict or None.  Zero provider calls.
    """
    if not sorted_bars or len(sorted_bars) < 10:
        return None

    window = sorted_bars[-60:] if len(sorted_bars) >= 60 else sorted_bars

    raw_lows: list[dict] = []
    for i in range(1, len(window) - 1):
        try:
            prev_low = float(window[i - 1].get("low") or window[i - 1].get("close") or 0)
            curr_low = float(window[i].get("low")     or window[i].get("close")     or 0)
            next_low = float(window[i + 1].get("low") or window[i + 1].get("close") or 0)
        except (TypeError, ValueError):
            continue
        if curr_low > 0 and curr_low <= prev_low and curr_low <= next_low:
            raw_lows.append({"level": curr_low, "recency": (i + 1) / len(window)})

    below = [sl for sl in raw_lows if sl["level"] <= price * 1.01]
    if not below:
        return None

    TOLERANCE = 0.03
    clusters: list[dict] = []
    for sl in sorted(below, key=lambda x: x["level"]):
        merged = False
        for cl in clusters:
            if abs(sl["level"] - cl["level"]) / cl["level"] < TOLERANCE:
                n = cl["touches"]
                cl["level"]   = (cl["level"] * n + sl["level"]) / (n + 1)
                cl["touches"] += 1
                cl["recency"] = max(cl["recency"], sl["recency"])
                merged = True
                break
        if not merged:
            clusters.append({"level": sl["level"], "touches": 1, "recency": sl["recency"]})

    if not clusters:
        return None

    max_level = max(c["level"] for c in clusters)
    for cl in clusters:
        prox = cl["level"] / max_level if max_level > 0 else 0.0
        cl["score"] = (
            0.5 * min(cl["touches"] / 3.0, 1.0)
            + 0.3 * cl["recency"]
            + 0.2 * prox
        )
    clusters.sort(key=lambda c: -c["score"])
    best = clusters[0]
    return {
        "level":   round(best["level"], 4),
        "touches": best["touches"],
        "score":   round(best["score"], 3),
        "source":  "bars_clustered_lows",
    }


def _classify_active_support_zone(
    price:          float,
    sma50:          Optional[float],
    sma200:         Optional[float],
    ma30w:          Optional[float],
    base_low:       Optional[float],
    breakout_pivot: Optional[float],
    sorted_bars:    list[dict],
) -> dict:
    """
    Active Support Zone Engine.

    Key semantic change vs. old _classify_support_hierarchy
    --------------------------------------------------------
    A prior breakout pivot that price has fallen meaningfully below is
    classified as ``prior_pivot_status = "lost_now_overhead"`` — overhead
    resistance / reclaim target — NOT as active major support.

    Active support candidates are ranked by proximity × touches × recency:
      1. Swing-low cluster from recent bars  (most touches → highest priority)
      2. base_low from V2 structure          (structural floor)
      3. SMA200 / 30w_MA                    (long-term MA support)
      4. SMA50                               (medium-term MA)
      5. breakout_pivot ONLY if within 8 %  (still intact / recently reclaimed)

    SUPPORT_LOST semantics
    ----------------------
    ``major_support_lost = True`` ← active_support_status == "lost_confirmed"
      (price > 5 % below active zone lower bound)
    NOT triggered merely by price being below the breakout_pivot.

    Backward-compat keys
    --------------------
    All old major_support_* / minor_support_* / support_level_* / HH-HL keys
    are still returned so callers need no changes.  Their semantics now reflect
    the ACTIVE zone instead of the prior pivot.

    Zero provider calls.
    """
    out: dict = {}

    # ── 1. Prior pivot classification ─────────────────────────────────────────
    prior_pivot_level:  Optional[float] = None
    prior_pivot_status: Optional[str]   = None
    reclaim_level:      Optional[float] = None

    if breakout_pivot is not None and float(breakout_pivot) > 0:
        prior_pivot_level = round(float(breakout_pivot), 4)
        if price >= prior_pivot_level * 0.97:
            prior_pivot_status = "intact"
        else:
            prior_pivot_status = "lost_now_overhead"
            reclaim_level      = prior_pivot_level
            # Reclaim: last 3 closes all at/above pivot
            if len(sorted_bars) >= 3:
                recent_cls = []
                for b in sorted_bars[-3:]:
                    try:
                        c = float(b.get("close") or 0)
                        if c > 0:
                            recent_cls.append(c)
                    except (TypeError, ValueError):
                        pass
                if recent_cls and all(c >= prior_pivot_level * 0.99 for c in recent_cls):
                    prior_pivot_status = "reclaimed"
                    reclaim_level      = None

    out["prior_pivot_level"]  = prior_pivot_level
    out["prior_pivot_status"] = prior_pivot_status
    out["reclaim_level"]      = reclaim_level

    # ── 2. Build active support candidates ───────────────────────────────────
    # A level is active support only if it is at or near current price (not overhead).
    OVERHEAD_CUTOFF  = 1.08   # > 8 % above price → overhead, skip
    NEAR_GRACE       = 1.03   # within 3 % above price → may still be testing

    raw_candidates: list[dict] = []

    def _add(label: str, level: Optional[float], type_: str, source: str, bonus: float) -> None:
        if level is None or float(level) <= 0:
            return
        lv = float(level)
        if lv > price * OVERHEAD_CUTOFF:
            return
        if lv > price * NEAR_GRACE:
            return
        dist_pct   = (price - lv) / price * 100          # positive = price above level
        prox_score = max(0.0, 1.0 - dist_pct / 20.0)    # 1.0 = same level, 0 = 20% away
        raw_candidates.append({
            "label":    label,
            "level":    round(lv, 4),
            "type":     type_,
            "source":   source,
            "score":    prox_score + bonus,
            "touches":  1,
            "dist_pct": round(dist_pct, 2),
        })

    # Bar-derived bounce zone
    _bounce = _find_bounce_zone(sorted_bars, price)
    if _bounce:
        _bt  = "double_bounce_support" if _bounce["touches"] >= 2 else "range_low"
        _bl  = "double_bounce"         if _bounce["touches"] >= 2 else "swing_low_cluster"
        _bon = 0.30 * min(_bounce["touches"] / 2.0, 1.0)
        _add(_bl, _bounce["level"], _bt, "bars", _bon)
        for c in raw_candidates:
            if c["label"] == _bl:
                c["touches"] = _bounce["touches"]

    # Structural and MA levels
    _add("base_low", base_low,  "base_low",             "v2_structure", 0.25)
    _add("SMA200",   sma200,    "moving_average_zone",  "ma",           0.20)
    _add("30w_MA",   ma30w,     "moving_average_zone",  "ma",           0.15)
    _add("SMA50",    sma50,     "moving_average_zone",  "ma",           0.10)

    # Breakout pivot as active support ONLY when it has not been lost
    if prior_pivot_status in ("intact", "reclaimed"):
        _add("breakout_pivot", breakout_pivot, "prior_major_swing_low", "v2_structure", 0.25)

    raw_candidates.sort(key=lambda c: -c["score"])

    # ── 3. Primary active support ──────────────────────────────────────────────
    active_level:   Optional[float] = None
    active_type:    Optional[str]   = None
    active_label:   Optional[str]   = None
    active_source:  Optional[str]   = None
    active_touches: int             = 1
    next_downside:  Optional[dict]  = None

    if raw_candidates:
        pri            = raw_candidates[0]
        active_level   = pri["level"]
        active_type    = pri["type"]
        active_label   = pri["label"]
        active_source  = pri["source"]
        active_touches = pri.get("touches", 1)
        for c in raw_candidates[1:]:
            if c["level"] < active_level * 0.97:
                next_downside = {"level": c["level"], "type": c["type"], "label": c["label"]}
                break

    out["active_support_type"]         = active_type
    out["active_support_label"]        = active_label
    out["active_support_touch_count"]  = active_touches
    out["next_downside_support"]       = next_downside

    # ── 4. Active support zone (±2 % band) ────────────────────────────────────
    lb: Optional[float] = None
    ub: Optional[float] = None
    if active_level is not None:
        lb = round(active_level * 0.98, 4)
        ub = round(active_level * 1.02, 4)
        out["active_support_zone"]  = {"lower_bound": lb, "upper_bound": ub, "midpoint": round(active_level, 4)}
        out["critical_break_level"] = lb
    else:
        out["active_support_zone"]  = None
        out["critical_break_level"] = None

    # ── 5. Active support status ───────────────────────────────────────────────
    if active_level is None or lb is None:
        asst = "no_clear_support"
    else:
        recently_touched = False
        for b in sorted_bars[-5:]:
            try:
                b_low = float(b.get("low") or b.get("close") or 0)
                if lb * 0.99 <= b_low <= ub:
                    recently_touched = True
                    break
            except (TypeError, ValueError):
                pass

        if price < lb * 0.97:
            asst = "lost_confirmed"
        elif price < lb:
            asst = "broken_unconfirmed"
        elif price <= (ub or active_level) * 1.02:
            asst = "bounced_from_support" if recently_touched else "testing_support"
        else:
            asst = "above_support"

    out["active_support_status"] = asst

    # ── 6. Backward-compat major/minor fields (now track active zone) ──────────
    if active_level is not None:
        dist_to_active = round((price - active_level) / active_level * 100, 2)
        out["major_support_level"]           = round(active_level, 4)
        out["major_support_type"]            = active_type
        out["major_support_source"]          = active_source
        out["distance_to_major_support_pct"] = dist_to_active
        out["major_support_lost"]            = asst in ("lost_confirmed", "broken_unconfirmed")
    else:
        out["major_support_level"]           = None
        out["major_support_type"]            = None
        out["major_support_source"]          = None
        out["distance_to_major_support_pct"] = None
        out["major_support_lost"]            = None

    # Minor support: SMA50 (if not already primary), else next candidate
    minor_level: Optional[float] = None
    minor_type:  Optional[str]   = None
    if sma50 and float(sma50) > 0 and float(sma50) <= price * 1.01 and active_label != "SMA50":
        minor_level = round(float(sma50), 4)
        minor_type  = "moving_average_zone"
    if minor_level is None and len(raw_candidates) > 1:
        for c in raw_candidates[1:]:
            if c["level"] < (active_level or price) * 0.99:
                minor_level = c["level"]
                minor_type  = c["type"]
                break

    out["minor_support_level"] = minor_level
    out["minor_support_type"]  = minor_type
    out["minor_support_lost"]  = (price < minor_level) if (minor_level and price > 0) else None

    out["support_level_price"]  = active_level or minor_level
    out["support_level_type"]   = active_type  or minor_type
    out["support_level_source"] = (
        "active_zone" if active_level else ("minor" if minor_level else None)
    )

    # ── 7. HH/HL structure from the last 40 bars ──────────────────────────────
    _HH_HL_KEYS = (
        "prior_swing_high", "recent_swing_high", "prior_swing_low", "recent_swing_low",
        "higher_high_confirmed", "higher_low_confirmed",
        "lower_high_confirmed",  "lower_low_confirmed", "support_break_confirmed",
    )
    if len(sorted_bars) >= 40:
        window = sorted_bars[-40:]
        closes:  list[float] = []
        lows_w:  list[float] = []
        highs_w: list[float] = []
        for b in window:
            c = b.get("close")
            if c is None:
                continue
            try:
                cv = float(c)
                closes.append(cv)
                lows_w.append(float(b.get("low")  or cv))
                highs_w.append(float(b.get("high") or cv))
            except (TypeError, ValueError):
                pass

        if len(closes) >= 20:
            mid         = len(closes) // 2
            prior_high  = max(highs_w[:mid])
            recent_high = max(highs_w[mid:])
            prior_low   = min(lows_w[:mid])
            recent_low  = min(lows_w[mid:])
            out["prior_swing_high"]       = round(prior_high,  4)
            out["recent_swing_high"]      = round(recent_high, 4)
            out["prior_swing_low"]        = round(prior_low,   4)
            out["recent_swing_low"]       = round(recent_low,  4)
            out["higher_high_confirmed"]  = recent_high > prior_high  * 1.02
            out["higher_low_confirmed"]   = recent_low  > prior_low   * 1.02
            out["lower_high_confirmed"]   = recent_high < prior_high  * 0.97
            out["lower_low_confirmed"]    = recent_low  < prior_low   * 0.97
            out["support_break_confirmed"]= out["lower_low_confirmed"]
        else:
            for k in _HH_HL_KEYS:
                out[k] = None
    else:
        for k in _HH_HL_KEYS:
            out[k] = None

    return out


# Backward-compat alias — call sites that imported the old name still work.
_classify_support_hierarchy = _classify_active_support_zone


# ── Entry Risk/Reward State Engine ─────────────────────────────────────────────

_RR_SEVERE_EXT = {"EXTREME_EXTENSION", "VERTICAL", "CROWDED_MOVE", "VOLUME_CLIMAX"}
_RR_HARD_BREAK = {"SUPPORT_LOST", "LOWER_LOW_CONFIRMED", "FAILED_BREAKOUT", "DOWNTREND"}
_RR_STRUCTURAL_SUPPORT_TYPES = {"double_bounce_support", "base_low", "range_low"}
_RR_STRUCTURAL_ENTRY_STATES  = {
    "TRENDLINE_SUPPORT_TEST", "BREAKOUT_RETEST", "BREAKOUT_PULLBACK",
    "LOW_BASE_FORMING", "LOW_BASE_COILING", "LOW_BASE_READY",
    "SUPPORT_HOLD", "CONSTRUCTIVE_DIP",
}
_RR_PRE_RECOVERY_STATES = {"LOW_BASE_FORMING", "REVERSAL_WATCH", "RANGE_SUPPORT_TEST"}


def _compute_entry_risk_reward(
    entry_state:             str,
    entry_score:             int,
    active_support_status:   Optional[str],
    active_support_type:     Optional[str],
    active_support_touches:  int,
    dist_to_active_pct:      Optional[float],
    prior_pivot_status:      Optional[str],
    lower_low_confirmed:     Optional[bool],
    extension_state:         Optional[str],
    base_archetype:          Optional[str],
    support_touch_count:     Optional[int],
    critical_break_level:    Optional[float],
) -> dict:
    """
    Derive an Entry Risk/Reward state from Active Support Zone + Entry Structure.

    Returns:
        entry_risk_reward_state         — canonical RR state string
        entry_risk_reward_score         — 0–100 score for this state
        entry_risk_reward_reason_codes  — list[str] diagnostic codes
        distance_to_active_support_pct  — convenience alias for dist_to_active_pct
        entry_score_rr_adjusted         — entry_score after floor/cap from RR state

    States (priority order):
        BROKEN_SUPPORT_AVOID          — active support lost or structural break
        STRONG_ASSET_EXTENDED_WAIT    — severely extended / too far from support
        ASYMMETRIC_SUPPORT_ENTRY      — best RR: support holding, structural evidence
        SUPPORT_TEST_CONFIRMING       — support tested/bounced, needs confirmation
        PULLBACK_TO_SUPPORT           — continuation pullback toward support
        LOW_BASE_RISK_DEFINED         — pre-recovery with defined downside
        SUPPORT_TEST_NEEDS_CONFIRMATION — support nearby, insufficient confirmation
        NO_CLEAR_ENTRY                — default; no adjustment

    Zero provider calls. Pure computation.
    """
    reasons: list[str]  = []
    asst    = active_support_status or "no_clear_support"
    ext     = extension_state or "HEALTHY"
    llc     = bool(lower_low_confirmed)
    dist    = dist_to_active_pct  # positive = price above active support

    severely_extended = ext in _RR_SEVERE_EXT

    # ── 1. BROKEN_SUPPORT_AVOID ───────────────────────────────────────────────
    broken = (
        asst == "lost_confirmed"
        or llc
        or entry_state in _RR_HARD_BREAK
    )
    if broken:
        if asst == "lost_confirmed":
            reasons.append("ACTIVE_SUPPORT_LOST_CONFIRMED")
        if llc:
            reasons.append("LOWER_LOW_CONFIRMED")
        if entry_state in _RR_HARD_BREAK:
            reasons.append(f"ENTRY_STATE_{entry_state}")
        return {
            "entry_risk_reward_state":        "BROKEN_SUPPORT_AVOID",
            "entry_risk_reward_score":        10,
            "entry_risk_reward_reason_codes": reasons,
            "distance_to_active_support_pct": dist,
            "entry_score_rr_adjusted":        min(entry_score, 20),
        }

    # ── 2. STRONG_ASSET_EXTENDED_WAIT ─────────────────────────────────────────
    # Severely extended from 30w MA, or price far above any active support.
    far_from_support = dist is not None and dist > 35.0
    if severely_extended or far_from_support:
        if severely_extended:
            reasons.append(f"EXTENSION_STATE_{ext}")
        if far_from_support and dist is not None:
            reasons.append(f"DIST_TO_SUPPORT_{dist:.1f}PCT_TOO_FAR")
        return {
            "entry_risk_reward_state":        "STRONG_ASSET_EXTENDED_WAIT",
            "entry_risk_reward_score":        30,
            "entry_risk_reward_reason_codes": reasons,
            "distance_to_active_support_pct": dist,
            "entry_score_rr_adjusted":        min(entry_score, 45),
        }

    # ── Shared convenience flags ──────────────────────────────────────────────
    in_zone      = asst in ("testing_support", "bounced_from_support")
    near_support = asst == "above_support" and dist is not None and dist <= 12.0
    support_ok   = asst in ("above_support", "testing_support", "bounced_from_support")

    has_structural_support = (
        active_support_touches >= 2
        or (active_support_type in _RR_STRUCTURAL_SUPPORT_TYPES)
        or (base_archetype == "LOW_BASE")
        or (support_touch_count is not None and support_touch_count >= 2)
        or entry_state in _RR_STRUCTURAL_ENTRY_STATES
    )

    # ── 3. ASYMMETRIC_SUPPORT_ENTRY ───────────────────────────────────────────
    # Best risk/reward: active support is visible, nearby, and structurally
    # defined. Excludes pre-recovery states (need dedicated LOW_BASE path).
    if (
        support_ok
        and not llc
        and not severely_extended
        and entry_state not in _RR_HARD_BREAK
        and entry_state not in _RR_PRE_RECOVERY_STATES
        and (in_zone or near_support)
        and has_structural_support
    ):
        reasons.append("ACTIVE_SUPPORT_HOLDING")
        reasons.append(f"SUPPORT_TYPE_{active_support_type}")
        if active_support_touches >= 2:
            reasons.append(f"SUPPORT_TOUCHES_{active_support_touches}")
        if base_archetype == "LOW_BASE":
            reasons.append("LOW_BASE_ARCHETYPE")
        if in_zone:
            reasons.append("PRICE_IN_SUPPORT_ZONE")
        if near_support and dist is not None:
            reasons.append(f"NEAR_SUPPORT_{dist:.1f}PCT")
        if critical_break_level is not None:
            reasons.append("CRITICAL_BREAK_DEFINED")
        return {
            "entry_risk_reward_state":        "ASYMMETRIC_SUPPORT_ENTRY",
            "entry_risk_reward_score":        80,
            "entry_risk_reward_reason_codes": reasons,
            "distance_to_active_support_pct": dist,
            "entry_score_rr_adjusted":        max(entry_score, 70),
        }

    # ── 4. SUPPORT_TEST_CONFIRMING ────────────────────────────────────────────
    # Actively testing or recently bounced, but structural evidence not strong
    # enough for ASYMMETRIC (single touch, MA zone only, not near enough).
    # Excludes pre-recovery states.
    if (
        in_zone
        and support_ok
        and not llc
        and entry_state not in _RR_HARD_BREAK
        and entry_state not in _RR_PRE_RECOVERY_STATES
    ):
        reasons.append(f"SUPPORT_TESTING_{asst}")
        if critical_break_level is not None:
            reasons.append("CRITICAL_BREAK_DEFINED")
        reasons.append("CONFIRMATION_NEEDED")
        return {
            "entry_risk_reward_state":        "SUPPORT_TEST_CONFIRMING",
            "entry_risk_reward_score":        60,
            "entry_risk_reward_reason_codes": reasons,
            "distance_to_active_support_pct": dist,
            "entry_score_rr_adjusted":        min(max(entry_score, 60), 75),
        }

    # ── 5. PULLBACK_TO_SUPPORT ────────────────────────────────────────────────
    # Established uptrend pulling back toward an active support level.
    _continuation_states = {
        "PULLBACK_IN_UPTREND", "BREAKOUT_PULLBACK", "CONSTRUCTIVE_DIP",
        "TRENDLINE_SUPPORT_TEST", "BREAKOUT_RETEST",
    }
    if near_support and entry_state in _continuation_states:
        reasons.append("PULLBACK_TO_SUPPORT")
        if dist is not None:
            reasons.append(f"DIST_TO_SUPPORT_{dist:.1f}PCT")
        return {
            "entry_risk_reward_state":        "PULLBACK_TO_SUPPORT",
            "entry_risk_reward_score":        65,
            "entry_risk_reward_reason_codes": reasons,
            "distance_to_active_support_pct": dist,
            "entry_score_rr_adjusted":        max(entry_score, 60),
        }

    # ── 6. LOW_BASE_RISK_DEFINED ──────────────────────────────────────────────
    # Pre-recovery state: price forming a base near critical support with a
    # defined break level. Needs reclaim/bounce confirmation for higher state.
    if (
        entry_state in _RR_PRE_RECOVERY_STATES
        and support_ok
        and critical_break_level is not None
    ):
        reasons.append("LOW_BASE_RISK_DEFINED")
        reasons.append("CRITICAL_BREAK_DEFINED")
        if dist is not None:
            reasons.append(f"DIST_TO_SUPPORT_{dist:.1f}PCT")
        return {
            "entry_risk_reward_state":        "LOW_BASE_RISK_DEFINED",
            "entry_risk_reward_score":        50,
            "entry_risk_reward_reason_codes": reasons,
            "distance_to_active_support_pct": dist,
            "entry_score_rr_adjusted":        min(max(entry_score, 45), 65),
        }

    # ── 7. SUPPORT_TEST_NEEDS_CONFIRMATION ────────────────────────────────────
    # Support is nearby but evidence is insufficient for any higher state.
    if support_ok and dist is not None and dist <= 15.0:
        reasons.append("SUPPORT_NEARBY")
        reasons.append("CONFIRMATION_NEEDED")
        if dist is not None:
            reasons.append(f"DIST_TO_SUPPORT_{dist:.1f}PCT")
        return {
            "entry_risk_reward_state":        "SUPPORT_TEST_NEEDS_CONFIRMATION",
            "entry_risk_reward_score":        45,
            "entry_risk_reward_reason_codes": reasons,
            "distance_to_active_support_pct": dist,
            "entry_score_rr_adjusted":        min(max(entry_score, 45), 60),
        }

    # ── 8. Default — NO_CLEAR_ENTRY ───────────────────────────────────────────
    reasons.append("NO_CLEAR_RR_SETUP")
    if dist is not None:
        reasons.append(f"DIST_TO_SUPPORT_{dist:.1f}PCT")
    if asst == "no_clear_support":
        reasons.append("NO_ACTIVE_SUPPORT_IDENTIFIED")
    return {
        "entry_risk_reward_state":        "NO_CLEAR_ENTRY",
        "entry_risk_reward_score":        max(entry_score, 30),
        "entry_risk_reward_reason_codes": reasons,
        "distance_to_active_support_pct": dist,
        "entry_score_rr_adjusted":        entry_score,
    }


# ── Core classification ────────────────────────────────────────────────────────

def _classify_entry_state(
    stage_int:     int,
    stage_key:     str,  # "1", "12", "2", "2b", "3m", "3", "4", or fallback key
    extension_state: str,
    extension_pct:   float,
    ext_risk:        str,   # from compute_technical_metrics
    breakout_sig:    Optional[str],
    entry_zone_val:  str,
    squeeze_sig:     Optional[str],
    ma_stack:        Optional[str],
    pct20:           Optional[float],
    pct50:           Optional[float],
    ad_sig:          Optional[str],
    volume_ratio:    Optional[float],  # recent/prior weekly vol ratio
    weeks_above:     int,
    ma_slope:        float,
    bars_count:      int,
) -> tuple[str, int, list[str]]:
    """
    Returns (entry_state, score_adjustment, evidence_list).
    score_adjustment is added to the base score from _BASE_SCORES.
    """
    evidence: list[str] = []

    # ── Broken / insufficient data (always highest priority) ──────────────────
    if bars_count < 20:
        return "INSUFFICIENT_DATA", 0, ["bars_count_below_20"]

    if stage_int == 4 or ext_risk == "broken":
        if ext_risk == "broken":
            evidence.append("ext_risk=broken")
            return "SUPPORT_LOST", 0, evidence
        evidence.append("stage4_downtrend")
        return "DOWNTREND", 0, evidence

    # ── Chase / Exhaustion ────────────────────────────────────────────────────
    if ext_risk == "overheated":
        evidence.append("ext_risk=overheated")
        # Check for volume climax (very high volume surge at top)
        if volume_ratio is not None and volume_ratio >= 3.0:
            evidence.append(f"vol_ratio={volume_ratio:.1f}_climax")
            return "VOLUME_CLIMAX", 0, evidence
        return "VERTICAL", 0, evidence

    if extension_state == "EXTREME_EXTENSION":
        evidence.append(f"extension_pct={extension_pct:.1f}")
        # Check crowding proxy (high vol + long extension period)
        if weeks_above >= 6 and volume_ratio is not None and volume_ratio >= 2.0:
            evidence.append(f"weeks_above={weeks_above}_crowded")
            return "CROWDED_MOVE", 0, evidence
        return "EXTREME_EXTENSION", 0, evidence

    if extension_state == "EXTENDED" and (
        ad_sig == "distribution" or
        (weeks_above >= 5 and ma_slope < 1.0)
    ):
        evidence.append(f"extension_pct={extension_pct:.1f}")
        if ad_sig == "distribution":
            evidence.append("ad_sig=distribution")
        return "EXTENDED", 0, evidence

    if breakout_sig == "failed_breakout":
        # Fallback only — reached when Entry Structure V2 could not compute a
        # structural bundle (e.g. exception) for a Stage 2/2b/3m symbol, or
        # for stages outside V2's scope. See analyze_entry_state_from_bars()
        # for the structure-aware override that normally intercepts this.
        evidence.append("breakout_sig=failed_breakout_fallback_no_structure")
        return "FAILED_BREAKOUT", 0, evidence

    # ── Stage 3 (non-3m topping / flat MA) ───────────────────────────────────
    if stage_int == 3 and stage_key not in ("3m",):
        evidence.append("stage3_topping")
        return "NO_CLEAR_ENTRY", 0, evidence

    # ── Stage 3m still rising (extended but not yet EXTREME) — treat as EXTENDED
    if stage_key == "3m":
        evidence.append("stage3m_late_momentum")
        if extension_state == "EXTREME_EXTENSION":
            return "EXTREME_EXTENSION", 0, evidence
        return "EXTENDED", 0, evidence

    # ── Pre-Move (Stage 1 / 1-2 Watch) ───────────────────────────────────────
    if stage_int == 1 or stage_key in ("1", "12"):
        if squeeze_sig in ("tight", "coiling"):
            evidence.append(f"squeeze={squeeze_sig}")
            # 1-2 watch with squeeze = breakout ready
            if stage_key == "12" or breakout_sig in ("potential_breakout", "near_breakout"):
                evidence.append("1_2_watch_coiled")
                adj = 0
                if ad_sig == "accumulation":
                    adj += 5
                    evidence.append("ad_sig=accumulation")
                return "BREAKOUT_READY", adj, evidence
            return "COILED", 0, evidence
        # No squeeze — plain base
        if stage_key == "12":
            evidence.append("1_2_watch_no_squeeze")
            return "SIGNALS_BUILDING", 0, evidence
        evidence.append("stage1_base")
        return "BASE_FORMING", 0, evidence

    # ── Stage 2 ────────────────────────────────────────────────────────────────
    if stage_int == 2 or stage_key in ("2", "2b"):
        # Breakout retest (Stage 2b = fresh breakout from a base)
        if stage_key == "2b":
            evidence.append("stage2b_fresh_breakout")
            adj = 0
            if ext_risk == "pullback_buy_zone":
                evidence.append("ext_risk=pullback_buy_zone")
                adj += 5
            return "BREAKOUT_RETEST", adj, evidence

        # Recent confirmed breakout still in healthy zone
        if breakout_sig == "confirmed_breakout" and extension_state in ("HEALTHY", "MODERATELY_EXTENDED"):
            evidence.append("confirmed_breakout_healthy")
            adj = 5 if ma_stack == "bull" else 0
            return "REACCELERATION", adj, evidence

        # Pullback into entry zones
        if ext_risk == "pullback_buy_zone":
            evidence.append("pullback_in_uptrend")
            adj = 0
            if ad_sig == "accumulation":
                adj += 6
                evidence.append("ad_sig=accumulation")
            if ma_stack == "bull":
                adj += 4
                evidence.append("ma_stack=bull")
            return "PULLBACK_IN_UPTREND", adj, evidence

        if entry_zone_val == "20d_pullback" and pct20 is not None and -8 <= pct20 <= 2:
            evidence.append("20d_pullback_entry_zone")
            return "CONSTRUCTIVE_DIP", 0, evidence

        if entry_zone_val in ("50d_pullback", "30w_pullback") or (
            pct50 is not None and -6 <= pct50 <= 3
        ):
            evidence.append("50d_support_hold")
            return "SUPPORT_HOLD", 0, evidence

        # Early Stage 2 accumulation (just crossed above MA, light volume)
        if weeks_above <= 2 and (volume_ratio is None or volume_ratio < 1.3):
            evidence.append("early_stage2_accumulation")
            return "EARLY_ACCUMULATION", 0, evidence

        # Stage 2 with building signals but not clearly in a buy zone
        if ma_stack == "bull" and ad_sig == "accumulation":
            evidence.append("stage2_signals_building")
            return "SIGNALS_BUILDING", 5, evidence

        # Generic Stage 2 no clear entry zone
        evidence.append("stage2_no_clear_zone")
        return "NO_CLEAR_ENTRY", 0, evidence

    # ── Fallback ───────────────────────────────────────────────────────────────
    evidence.append("fallback_no_match")
    return "NO_CLEAR_ENTRY", 0, evidence


# ── Broken-support nuanced classifier (Bug 2) ──────────────────────────────────
def _classify_broken_support(
    sorted_bars:  list[dict],
    price:        float,
    sma50:        Optional[float],
    sma200:       Optional[float],
    pct_vs_sma50: Optional[float],
) -> Optional[tuple[str, int, list[str]]]:
    """
    Called only when ext_risk == 'broken' (price < SMA50 and price < SMA200).
    Uses the existing daily bars to produce a more nuanced classification
    before falling through to the legacy blanket SUPPORT_LOST signal.

    Strategy
    --------
    1.  Marginal break (within 5 % below SMA50) without a confirmed lower-low →
        SUPPORT_TEST (testing support, not a structural break).
    2.  Lower-high formed but no confirmed lower-low yet →
        LOWER_HIGH_WARNING (structural caution, momentum weakening).
    3.  Lower-low confirmed (second-half minimum > 3 % below first-half minimum)
        → LOWER_LOW_CONFIRMED (structural break validated).
    4.  Significant break, no confirmed lower-low yet → fall through (None)
        so the legacy SUPPORT_LOST signal fires.

    Returns (state, score_adj, evidence) or None to fall through.
    Zero new provider calls.  Operates on sorted_bars already in memory.
    """
    if not sorted_bars or len(sorted_bars) < 30:
        return None

    evidence: list[str] = ["broken_support_nuanced_classifier"]

    if pct_vs_sma50 is not None:
        evidence.append(f"pct_vs_sma50={pct_vs_sma50:.1f}")

    # ── Marginal break flag — within 5 % below SMA50 ─────────────────────────
    marginal_break = (pct_vs_sma50 is not None and pct_vs_sma50 >= -5.0)

    # ── HH/HL structure from the last 40 bars (close, high, low) ─────────────
    window = sorted_bars[-40:]
    closes: list[float] = []
    lows:   list[float] = []
    for b in window:
        c = b.get("close")
        if c is None:
            continue
        try:
            closes.append(float(c))
            lows.append(float(b.get("low") or c))
        except (TypeError, ValueError):
            pass

    if len(closes) < 20:
        return None

    mid          = len(closes) // 2
    first_high   = max(closes[:mid])
    second_high  = max(closes[mid:])
    first_low    = min(lows[:mid])
    second_low   = min(lows[mid:])

    # Lower-low: second-half minimum more than 3 % below first-half minimum
    lower_low_confirmed = second_low < first_low * 0.97

    # Lower-high: recent peak more than 3 % below prior peak
    lower_high = second_high < first_high * 0.97

    if lower_low_confirmed:
        evidence.append("lower_low_confirmed")
        evidence.append(f"prior_low={first_low:.2f}_recent_low={second_low:.2f}")
        return ("LOWER_LOW_CONFIRMED", 0, evidence)

    if marginal_break:
        evidence.append("marginal_break_below_sma50")
        if lower_high:
            evidence.append("lower_high_no_lower_low_yet")
            return ("LOWER_HIGH_WARNING", 0, evidence)
        return ("SUPPORT_TEST", 0, evidence)

    if lower_high:
        evidence.append("lower_high_no_lower_low_yet")
        return ("LOWER_HIGH_WARNING", 0, evidence)

    # Significant break, no confirmed lower-low — fall through to SUPPORT_LOST
    return None


# ── Public API ─────────────────────────────────────────────────────────────────

def analyze_entry_state_from_bars(
    symbol:           str,
    daily_bars:       list[dict],
    stage_result:     dict,
    *,
    precomputed_tech: Optional[dict] = None,
    cached_quote:     Optional[dict] = None,
    bars_provider:    str = "unknown",
    persist:          bool = True,
) -> dict:
    """
    Compute entry state from already-fetched daily bars + stage_result.

    Parameters
    ----------
    symbol           : ticker symbol (uppercase)
    daily_bars       : list of daily OHLCV dicts with at least {"date","close"}.
                       Bars must span ≥ 20 days; sorted oldest → newest.
                       These must be the exact bars already in memory from the
                       stage refresh pass — no new provider calls are made here.
    stage_result     : output of analyze_symbol_stage() — provides stage number,
                       extension_state, extension_pct_30w_ma, stage_signals.
    precomputed_tech : already-computed compute_technical_metrics() dict from the
                       same stage pass.  Skips the re-computation entirely when
                       provided (same bars → same result, and avoids duplicate CPU).
    cached_quote     : read-only in-memory quote cache entry for current price
                       (e.g. cache.get("tradier:quote:sym:{SYM}")).  Must be
                       retrieved with a bare cache.get() — never via get_quote()
                       which can fetch on miss.  When None, last bar close is used.
    bars_provider    : "tradier" | "fmp" | "unknown" — propagated from _fetch_bars.
    persist          : True  → write entry_state_lkg.json after this symbol.
                       False → update in-memory only; caller must call
                       flush_entry_state_lkg() after the batch completes.

    Returns
    -------
    Dict with keys: symbol, entry_family, entry_state, entry_score, entry_grade,
                    support_levels, evidence, stage_int, extension_state,
                    extension_pct, price, price_basis, price_as_of,
                    bars_last_date, bars_provider, computed_at.

    Provider call count delta: ZERO.
    """
    _load_lkg()
    t0 = time.time()

    # ── Extract stage inputs ──────────────────────────────────────────────────
    stage_int   = stage_result.get("stage", 0)
    stage_label = stage_result.get("stage_label", "")
    sigs        = stage_result.get("stage_signals", {})
    ext_state   = stage_result.get("extension_state", "HEALTHY")
    ext_pct     = float(stage_result.get("extension_pct_30w_ma") or
                        sigs.get("price_vs_30w_ma_pct") or 0.0)
    ma_slope    = float(sigs.get("ma_30w_slope_pct") or 0.0)
    weeks_above = int(sigs.get("weeks_above_30w_ma_of_8") or 0)
    vol_ratio_w = sigs.get("volume_ratio")

    # Map actual STAGE_LABELS strings → internal stage_key
    # These must match stage_analysis.STAGE_LABELS exactly.
    _label_to_key: dict[str, str] = {
        "S1 Base":       "1",
        "S1-2 Watch":    "12",
        "S2-S3 Advance": "2",
        "S2 Breakout":   "2b",
        "S3-S4 Top":     "3",
        "S3 Momentum":   "3m",
        "S4 Decline":    "4",
    }
    stage_key = _label_to_key.get(stage_label, str(stage_int))

    # ── Technical metrics — reuse precomputed when available ─────────────────
    if precomputed_tech is not None:
        tech = precomputed_tech
    else:
        try:
            from services.stage_analysis import compute_technical_metrics
            tech = compute_technical_metrics(daily_bars)
        except Exception:
            tech = {}

    ext_risk   = tech.get("extension_risk", "neutral")
    breakout_s = tech.get("breakout_signal")
    entry_zone = tech.get("entry_zone", "neutral")
    squeeze_s  = tech.get("squeeze_signal")
    ma_stack   = tech.get("ma_stack")
    ad_sig     = tech.get("accumulation_distribution_signal")
    sma20      = tech.get("sma_20")
    sma50      = tech.get("sma_50")
    sma200     = tech.get("sma_200")
    pct20      = tech.get("pct_vs_sma_20")
    pct50      = tech.get("pct_vs_sma_50")
    pct200     = tech.get("pct_vs_sma_200")

    # ── Price resolution: cached quote (read-only) → last bar close ───────────
    # cached_quote is obtained via cache.get() — no fetch on miss.
    # If caller holds no cached quote, last bar close is the canonical fallback.
    sorted_bars = sorted(daily_bars, key=lambda b: b["date"])
    bars_last_date: Optional[str] = (
        str(sorted_bars[-1].get("date", ""))[:10] if sorted_bars else None
    )

    price: Optional[float] = None
    price_basis: str = "LAST_DAILY_CLOSE"
    price_as_of: Optional[str] = None

    if cached_quote:
        _q_last = cached_quote.get("last") or cached_quote.get("close")
        if _q_last is not None:
            try:
                price = float(_q_last)
                price_basis = "CANONICAL_CACHED_QUOTE"
                price_as_of = str(
                    cached_quote.get("trade_date")
                    or cached_quote.get("last_volume_date")
                    or _now_iso()
                )
            except (TypeError, ValueError):
                price = None

    if price is None and sorted_bars:
        price = float(sorted_bars[-1]["close"])
        price_basis = "LAST_DAILY_CLOSE"
        price_as_of = bars_last_date

    # ── V4.2.5 — SMA30, drawdown_from_recent_high_pct, Fib, Wave ─────────────
    _fib_fields:  dict = {}
    _wave_fields: dict = {}
    _sma30:                       Optional[float] = None
    _pct30:                       Optional[float] = None
    _drawdown_30d_pct:            Optional[float] = None
    _dist_20dma_pct:              Optional[float] = None
    _dist_50dma_pct:              Optional[float] = None
    _dist_200dma_pct:             Optional[float] = None

    if sorted_bars and price is not None and price > 0:
        try:
            from services.fib_engine import compute_fib_levels as _fib_fn
            _fib_fields = _fib_fn(
                sorted_bars,
                current_price=price,
                history_source=bars_provider,
            )
        except ImportError as _e:
            print(
                f"[ENTRY_STATE][ERROR] fib_engine import failed for {symbol!r} — "
                f"primary_fib_context will be None. "
                f"Check that 'from services.fib_engine' resolves from backend/. "
                f"Exception: {_e}"
            )
            _fib_fields = {}
        except Exception as _e:
            print(f"[ENTRY_STATE][WARN] fib_engine compute error for {symbol!r}: {_e}")
            _fib_fields = {}

        try:
            from services.wave_structure_engine import classify_wave_structure as _wave_fn
            _wave_fields = _wave_fn(
                bars                  = sorted_bars,
                entry_state           = None,   # final entry_state not yet computed here
                entry_family          = None,
                extension_state       = ext_state,
                stage_alignment_score = None,
                prior_26w_trend_pct   = stage_result.get("prior_26w_trend_pct"),
            )
        except ImportError as _e:
            print(
                f"[ENTRY_STATE][ERROR] wave_structure_engine import failed for {symbol!r} — "
                f"wave_structure_label will be None. "
                f"Check that 'from services.wave_structure_engine' resolves from backend/. "
                f"Exception: {_e}"
            )
            _wave_fields = {}
        except Exception as _e:
            print(f"[ENTRY_STATE][WARN] wave_structure_engine compute error for {symbol!r}: {_e}")
            _wave_fields = {}

        # SMA-30
        _c30 = [float(b["close"]) for b in sorted_bars[-30:] if b.get("close")]
        if len(_c30) >= 25:
            _sma30 = round(sum(_c30) / len(_c30), 4)
            _pct30 = round((price - _sma30) / _sma30 * 100, 2)

        # Drawdown from recent 30-bar high
        _h30 = [float(b.get("high") or b.get("close") or 0) for b in sorted_bars[-30:]]
        _h30 = [v for v in _h30 if v > 0]
        if _h30:
            _peak30 = max(_h30)
            if _peak30 > 0:
                _drawdown_30d_pct = round((price - _peak30) / _peak30 * 100, 2)

        # Distance from key DMA levels in %
        if sma20 and sma20 > 0:
            _dist_20dma_pct  = round((price - sma20)  / sma20  * 100, 2)
        if sma50 and sma50 > 0:
            _dist_50dma_pct  = round((price - sma50)  / sma50  * 100, 2)
        if sma200 and sma200 > 0:
            _dist_200dma_pct = round((price - sma200) / sma200 * 100, 2)

    # 30w MA in price units (back-calculate from ext_pct if available)
    ma30w_price: Optional[float] = None
    if price is not None and ext_pct is not None:
        try:
            ma30w_price = price / (1 + ext_pct / 100.0)
        except ZeroDivisionError:
            pass

    # ── Entry Structure V2 — real chart-geometry override ─────────────────────
    # Runs BEFORE the generic classifier for Stage 2 / 2b (and Stage 3m, gated
    # off by the EXTREME_EXTENSION check below) symbols. Replaces the old
    # naive "within 8% of rolling 20d-high" HIGH_BASE pre-check (which could
    # not tell a pre-breakout base from a stock already trading well above a
    # breakout it already cleared — the SLAB bug) and the bare
    # SMA20/SMA50/rolling-high FAILED_BREAKOUT signal (which had no concept
    # of a rising trendline or higher-low structure — the ALGM bug).
    #
    # Zero new provider calls: operates only on the daily bars + technical
    # metrics already computed during this Stage refresh pass.
    # NOTE: extension_state is deliberately NOT part of this gate. Structural
    # geometry (base/trendline/breakout) and extension risk are separate
    # dimensions — a symbol can simultaneously be HIGH_BASE_READY and
    # EXTREME_EXTENSION. Extension is applied as a score/state risk modifier
    # AFTER structural classification below, never as a pre-filter that hides
    # the structure from the classifier.
    _v2_override: Optional[tuple[str, int, list[str]]] = None
    _v2_structure: Optional[dict] = None
    _structure_state_raw: Optional[str] = None
    _extension_risk_modifier = 0
    _extension_reason_codes: list[str] = []
    _is_v2_scope = (
        price is not None and
        len(sorted_bars) >= 20 and
        (stage_int in (2, 3) or stage_key in ("2", "2b", "3m")) and
        ext_risk not in ("broken", "overheated")
    )
    if _is_v2_scope:
        try:
            from services.entry_structure_v2 import compute_structure as _v2_compute
            _v2_structure = _v2_compute(
                bars=sorted_bars, tech=tech, price=price, stage_int=stage_int,
                sma20=sma20, sma50=sma50, sma200=sma200, ma30w=ma30w_price,
            )
            _base = _v2_structure["base"]
            _trendline = _v2_structure["trendline"]
            _breakout = _v2_structure["breakout"]

            def _surviving_structure() -> bool:
                if _trendline.get("trendline_hold_state") in (
                    "ABOVE", "TESTING", "HELD_RECENTLY", "UNDERCUT_RECLAIM"
                ):
                    return True
                if _base.get("higher_lows_count", 0) >= 1 and _base.get(
                    "base_breakout_status"
                ) != "BELOW_RANGE":
                    return True
                return False

            # ── 2. STRUCTURALLY_BROKEN — confirmed failed breakout ────────────
            if _breakout.get("failed_breakout_confirmed"):
                if _surviving_structure():
                    ev = ["v2_failed_breakout_but_structure_survives"]
                    hold = _trendline.get("trendline_hold_state")
                    if _trendline.get("ascending_trendline_detected") and hold in ("TESTING", "UNDERCUT_RECLAIM"):
                        ev.append(f"trendline_{hold.lower()}")
                        _v2_override = ("TRENDLINE_SUPPORT_TEST", 0, ev)
                    elif _trendline.get("ascending_trendline_detected") and hold == "HELD_RECENTLY":
                        ev.append("trendline_held_recently")
                        _v2_override = ("BREAKOUT_PULLBACK", 0, ev)
                    elif _base.get("higher_lows_count", 0) >= 2:
                        ev.append(f"higher_lows_count={_base['higher_lows_count']}")
                        _v2_override = ("CONSTRUCTIVE_DIP", 0, ev)
                    else:
                        ev.append("weak_surviving_structure")
                        _v2_override = ("REVERSAL_WATCH", 0, ev)
                else:
                    ev = ["v2_failed_breakout_confirmed"] + list(
                        _breakout.get("failed_breakout_reason_codes", [])
                    )
                    _v2_override = ("FAILED_BREAKOUT", 0, ev)
                _structure_state_raw = _v2_override[0]

            # ── 3. BREAKOUT_IN_PROGRESS — confirmed prior breakout, price still
            #       working through/around the pivot (neither a clean post-base
            #       confirmation nor a confirmed failure). Must be handled
            #       explicitly so a valid V2 bundle never falls through to the
            #       legacy naive failed-breakout heuristic (the CIFR bug).
            elif _base.get("base_breakout_status") == "BREAKOUT_IN_PROGRESS":
                hold = _trendline.get("trendline_hold_state")
                dist_hi = _base.get("distance_to_base_high_pct")
                days_since = _breakout.get("days_since_breakout")
                ev = [
                    "v2_breakout_in_progress",
                    f"breakout_confirmed={_breakout.get('breakout_confirmed')}",
                    f"failed_breakout_confirmed={_breakout.get('failed_breakout_confirmed')}",
                ]
                if _surviving_structure() and hold in ("HELD_RECENTLY", "TESTING", "UNDERCUT_RECLAIM"):
                    ev.append(f"trendline_{str(hold).lower()}")
                    _v2_override = ("BREAKOUT_PULLBACK", 0, ev)
                elif (
                    _breakout.get("breakout_confirmed") and
                    dist_hi is not None and dist_hi >= 0 and
                    days_since is not None and days_since <= 5
                ):
                    _v2_override = ("BREAKOUT_CONFIRMED", 0, ev + ["fresh_confirmed_breakout"])
                elif ext_pct is not None and ext_pct > 15:
                    _v2_override = ("WAIT_FOR_RETEST", 0, ev + ["extended_no_retest_yet"])
                else:
                    _v2_override = ("SIGNALS_BUILDING", 0, ev + ["breakout_developing_no_clear_confirmation"])
                _structure_state_raw = _v2_override[0]

            # ── 4. POST-BREAKOUT STRUCTURE — confirmed break above real ceiling
            elif _base.get("base_breakout_status") == "BREAKOUT_CONFIRMED":
                days_since = _breakout.get("days_since_breakout")
                dist_hi = _base.get("distance_to_base_high_pct")
                ev = ["v2_breakout_confirmed_post_base", f"base_high={_base.get('base_high')}"]
                if ext_state == "EXTREME_EXTENSION" and not (dist_hi is not None and abs(dist_hi) <= 4.0):
                    _structure_state_raw = "BREAKOUT_CONFIRMED"
                    _v2_override = ("WAIT_FOR_RETEST", -5, ev + ["extreme_extension_chase_risk"])
                elif days_since is not None and days_since <= 5:
                    _v2_override = ("BREAKOUT_CONFIRMED", 5, ev + ["fresh_confirmed_breakout"])
                elif dist_hi is not None and abs(dist_hi) <= 4.0:
                    _v2_override = ("BREAKOUT_RETEST", 3, ev + ["retesting_pivot_zone"])
                elif ext_pct is not None and ext_pct > 15:
                    _v2_override = ("WAIT_FOR_RETEST", 0, ev + ["extended_no_retest_yet"])
                else:
                    _v2_override = ("BREAKOUT_CONFIRMED", 0, ev)
                if _structure_state_raw is None:
                    _structure_state_raw = _v2_override[0]

            # ── 6a. PRE-BREAKOUT HIGH BASE — real base, still under the ceiling,
            #       located in the upper region of a meaningful prior advance.
            elif (
                _base.get("base_detected") and
                _base.get("base_breakout_status") in ("IN_RANGE", "PRESSING_CEILING") and
                _v2_structure.get("base_archetype") == "HIGH_BASE" and
                ad_sig != "distribution" and
                ma_stack in ("bull", "mixed")
            ):
                ev = [
                    "v2_high_base",
                    f"base_range_pct={_base.get('base_range_pct')}",
                    f"upper_range_position={_base.get('upper_range_position')}",
                ]
                duration = _base.get("base_duration_bars") or 0
                contraction = _base.get("range_contraction")
                pressing = _base.get("base_breakout_status") == "PRESSING_CEILING"
                tight = (_base.get("base_range_pct") or 99) <= 15.0
                if pressing and tight and duration >= 20:
                    _v2_override = (
                        "HIGH_BASE_READY",
                        5 if contraction else 0,
                        ev + ["pressing_ceiling", "tight_mature_base"],
                    )
                elif contraction and duration >= 15:
                    _v2_override = (
                        "HIGH_BASE_COILING",
                        3 if squeeze_s in ("tight", "coiling") else 0,
                        ev + ["range_contracting"],
                    )
                else:
                    _v2_override = ("HIGH_BASE_FORMING", 0, ev + ["base_still_developing"])
                _structure_state_raw = _v2_override[0]

            # ── 6b. LOW BASE — real base/floor in the lower region of a
            #       meaningful prior decline. Momentum is NOT required.
            elif (
                _base.get("base_detected") and
                _base.get("base_breakout_status") in ("IN_RANGE", "PRESSING_CEILING") and
                _v2_structure.get("base_archetype") == "LOW_BASE"
            ):
                _floor = _v2_structure.get("low_base_floor", {})
                ev = [
                    "v2_low_base",
                    f"base_range_pct={_base.get('base_range_pct')}",
                    f"low_base_support_quality={_floor.get('low_base_support_quality')}",
                ]
                low_reason_codes: list[str] = []
                if _floor.get("low_base_floor") is not None:
                    low_reason_codes.append("LOW_BASE_FLOOR_DEFINED")
                else:
                    low_reason_codes.append("LOW_BASE_FLOOR_WEAK")
                if (_floor.get("low_base_floor_touch_count") or 0) >= 3:
                    low_reason_codes.append("LOW_BASE_MULTIPLE_FLOOR_TESTS")
                contraction = _base.get("range_contraction")
                if contraction:
                    low_reason_codes.append("LOW_BASE_RANGE_CONTRACTING")
                higher_lows = _base.get("higher_lows_count", 0) or 0
                if higher_lows >= 1:
                    low_reason_codes.append("LOW_BASE_HIGHER_LOWS")
                duration = _base.get("base_duration_bars") or 0
                pressing = _base.get("base_breakout_status") == "PRESSING_CEILING"
                upper_pos = _base.get("upper_range_position") or 0
                mature_floor = (_floor.get("low_base_floor_touch_count") or 0) >= 2 and duration >= 20
                tightening = bool(contraction) and higher_lows >= 1

                if pressing or upper_pos >= 65:
                    low_reason_codes.append("LOW_BASE_PRESSING_RECLAIM")
                low_reason_codes.append("LOW_BASE_MOMENTUM_UNPROVEN")

                if mature_floor and tightening and (pressing or upper_pos >= 65):
                    _v2_override = ("LOW_BASE_READY", 0, ev + low_reason_codes)
                elif mature_floor and tightening:
                    _v2_override = ("LOW_BASE_COILING", 0, ev + low_reason_codes)
                else:
                    _v2_override = ("LOW_BASE_FORMING", 0, ev + low_reason_codes)
                _structure_state_raw = _v2_override[0]

            # ── 5. STRUCTURAL SUPPORT — sharp pullback still holding trendline
            elif (
                _trendline.get("ascending_trendline_detected") and
                _trendline.get("trendline_hold_state") in ("TESTING", "UNDERCUT_RECLAIM") and
                ext_risk == "pullback_buy_zone"
            ):
                _v2_override = (
                    "TRENDLINE_SUPPORT_TEST",
                    0,
                    [
                        "v2_trendline_support_test",
                        f"trendline_hold_state={_trendline['trendline_hold_state']}",
                    ],
                )
                _structure_state_raw = _v2_override[0]

            # ── FIX 2 — V2/LEGACY BOUNDARY ─────────────────────────────────────
            # A valid V2 structural bundle was computed but none of the explicit
            # branches above matched. Per the architectural rule, legacy naive
            # heuristics must NEVER override a successfully computed V2 bundle
            # just because a status/enum combination lacks an explicit branch.
            # Return a V2-safe neutral state instead of calling the legacy
            # classifier (which is reserved for "no usable V2 structure").
            if _v2_override is None:
                if ext_state == "EXTREME_EXTENSION" and not _base.get("base_detected"):
                    _v2_override = (
                        "EXTREME_EXTENSION", 0,
                        ["v2_safe_fallback_no_base_extreme_extension"],
                    )
                else:
                    _v2_override = (
                        "NO_CLEAR_ENTRY", 0,
                        ["v2_safe_fallback_no_structural_match"],
                    )
                _structure_state_raw = _v2_override[0]

            # ── FIX 1 — EXTENSION AS A RISK MODIFIER, APPLIED AFTER STRUCTURE ──
            # Extension answers "how stretched is this vs the 30w MA", not
            # "what structure exists". It never suppresses the structural
            # classification above; it only adjusts score/state risk here.
            _EXTENSION_MODIFIER = {
                "HEALTHY": 0,
                "MODERATELY_EXTENDED": -3,
                "EXTENDED": -8,
                "EXTREME_EXTENSION": -18,
            }
            _extension_risk_modifier = _EXTENSION_MODIFIER.get(ext_state, 0)
            if _extension_risk_modifier != 0:
                _extension_reason_codes.append(
                    f"extension_state={ext_state}_score_adjusted_{_extension_risk_modifier}"
                )
            _state_name, _adj, _ev = _v2_override
            _v2_override = (_state_name, _adj + _extension_risk_modifier, _ev + _extension_reason_codes)
        except Exception as _v2_exc:
            print(
                f"[ENTRY_STRUCTURE_V2] compute error for a symbol (non-fatal, "
                f"falls back to legacy classifier): {_v2_exc}"
            )
            _v2_structure = None
            _v2_override = None

    # ── Broken-support nuanced classifier (Bug 2) ────────────────────────────
    # ext_risk == "broken" excluded V2 scope above; run a bar-geometry check
    # before falling through to the legacy blanket SUPPORT_LOST signal.
    if _v2_override is None and ext_risk == "broken" and price is not None and len(sorted_bars) >= 30:
        _broken_nuance = _classify_broken_support(
            sorted_bars  = sorted_bars,
            price        = price,
            sma50        = sma50,
            sma200       = sma200,
            pct_vs_sma50 = pct50,
        )
        if _broken_nuance is not None:
            _v2_override         = _broken_nuance
            _structure_state_raw = _v2_override[0]

    # ── Classify ──────────────────────────────────────────────────────────────
    if _v2_override is not None:
        entry_state, adj, evidence = _v2_override
    else:
        entry_state, adj, evidence = _classify_entry_state(
            stage_int       = stage_int,
            stage_key       = stage_key,
            extension_state = ext_state,
            extension_pct   = ext_pct,
            ext_risk        = ext_risk,
            breakout_sig    = breakout_s,
            entry_zone_val  = entry_zone,
            squeeze_sig     = squeeze_s,
            ma_stack        = ma_stack,
            pct20           = pct20,
            pct50           = pct50,
            ad_sig          = ad_sig,
            volume_ratio    = (float(vol_ratio_w) if vol_ratio_w else None),
            weeks_above     = weeks_above,
            ma_slope        = ma_slope,
            bars_count      = len(daily_bars),
        )

    # ── Active Support Zone — computed early so it can correct entry_state ────
    _support_hier: dict = {}
    if price is not None:
        _sv2b = (_v2_structure or {}).get("base", {})
        _sv2o = (_v2_structure or {}).get("breakout", {})
        _support_hier = _classify_active_support_zone(
            price          = price,
            sma50          = sma50,
            sma200         = sma200,
            ma30w          = ma30w_price,
            base_low       = _sv2b.get("base_low"),
            breakout_pivot = _sv2o.get("breakout_pivot"),
            sorted_bars    = sorted_bars,
        )

    # ── Post-classification correction: SUPPORT_LOST gate ─────────────────────
    # SUPPORT_LOST requires the ACTIVE critical support to be broken, not merely
    # the prior breakout pivot.  If the active support zone is still holding
    # (testing / bouncing), reclassify to a nuanced state.
    if entry_state == "SUPPORT_LOST" and _support_hier:
        _asst = _support_hier.get("active_support_status", "no_clear_support")
        _pvst = _support_hier.get("prior_pivot_status")
        if _asst in ("above_support", "testing_support", "bounced_from_support"):
            if _pvst == "lost_now_overhead":
                # Prior pivot now overhead resistance; active lower support holds
                entry_state = "RANGE_SUPPORT_TEST"
                evidence    = list(evidence) + [
                    "prior_pivot_lost_now_overhead",
                    f"active_support_{_asst}",
                    "support_lost_corrected→range_support_test",
                ]
            else:
                # Active support is holding; prior pivot still near price
                entry_state = "SUPPORT_TEST"
                evidence    = list(evidence) + [
                    f"active_support_{_asst}",
                    "support_lost_corrected→support_test",
                ]

    entry_family = _STATE_TO_FAMILY.get(entry_state, FAMILY_BROKEN)
    base_score   = _BASE_SCORES.get(entry_state, 20)
    entry_score  = max(0, min(100, base_score + adj))
    entry_grade  = _grade(entry_score)

    # ── Entry Risk/Reward State ────────────────────────────────────────────────
    # Must run AFTER entry_state is final and _support_hier is computed.
    _rr: dict = {}
    if price is not None and _support_hier:
        _sv2b_rr = (_v2_structure or {}).get("base", {})
        _rr = _compute_entry_risk_reward(
            entry_state            = entry_state,
            entry_score            = entry_score,
            active_support_status  = _support_hier.get("active_support_status"),
            active_support_type    = _support_hier.get("active_support_type"),
            active_support_touches = _support_hier.get("active_support_touch_count") or 1,
            dist_to_active_pct     = _support_hier.get("distance_to_major_support_pct"),
            prior_pivot_status     = _support_hier.get("prior_pivot_status"),
            lower_low_confirmed    = _support_hier.get("lower_low_confirmed"),
            extension_state        = ext_state,
            base_archetype         = (_v2_structure or {}).get("base_archetype"),
            support_touch_count    = _sv2b_rr.get("support_touch_count"),
            critical_break_level   = _support_hier.get("critical_break_level"),
        )
        # Apply score adjustment from RR engine (floor or cap depending on state)
        _rr_adj = _rr.get("entry_score_rr_adjusted")
        if _rr_adj is not None and _rr_adj != entry_score:
            entry_score = max(0, min(100, int(_rr_adj)))
            entry_grade = _grade(entry_score)

    # ── Support levels (legacy, backward-compat) ──────────────────────────────
    support_levels: list[dict] = []
    if price is not None:
        support_levels = _build_support_levels(
            price, sma20, sma50, sma200, ma30w_price
        )

    result: dict[str, Any] = {
        "symbol":          symbol,
        "entry_family":    entry_family,
        "entry_state":     entry_state,
        "entry_score":     entry_score,
        "entry_grade":     entry_grade,
        "support_levels":  support_levels,
        "evidence":        evidence,
        "stage_int":       stage_int,
        "stage_label":     stage_label,
        "stage_key":       stage_key,
        "extension_state": ext_state,
        "extension_pct":   ext_pct,
        "ext_risk":        ext_risk,
        "ma_stack":        ma_stack,
        "squeeze_signal":  squeeze_s,
        "ad_signal":       ad_sig,
        "price":           round(price, 4) if price is not None else None,
        "price_basis":     price_basis,
        "price_as_of":     price_as_of,
        "bars_last_date":  bars_last_date,
        "bars_provider":   bars_provider,
        # ── V4.2.5.2 entry depth diagnostics ─────────────────────────────────
        "entry_bar_count":              len(sorted_bars),
        "entry_years_available":        round(len(sorted_bars) / 252, 1),
        "entry_history_status":         _entry_depth_status(len(sorted_bars)),
        "entry_long_history_used":      len(sorted_bars) >= 756,
        "entry_data_depth_confidence":  _entry_depth_conf(len(sorted_bars)),
        "entry_data_limitation_reason": _entry_depth_reason(len(sorted_bars), bars_provider),
        "structure_state": _structure_state_raw,
        "extension_risk_modifier": _extension_risk_modifier,
        "extension_reason_codes":  _extension_reason_codes,
        "elapsed_ms":      round((time.time() - t0) * 1000, 1),
        "computed_at":     _now_iso(),
        "entry_analysis_version": ENTRY_ANALYSIS_VERSION,
        **_support_hier,
        # ── Entry Risk/Reward State (Part 2/3 of support confluence spec) ────
        "entry_risk_reward_state":        _rr.get("entry_risk_reward_state"),
        "entry_risk_reward_score":        _rr.get("entry_risk_reward_score"),
        "entry_risk_reward_reason_codes": _rr.get("entry_risk_reward_reason_codes"),
        "distance_to_active_support_pct": _rr.get("distance_to_active_support_pct"),
        # ── Bar depth ────────────────────────────────────────────────────────
        "daily_bar_count":             len(sorted_bars),
        # ── V4.2.5 — Extended MA fields ──────────────────────────────────────
        "sma_30":                      _sma30,
        "pct_vs_sma_30":               _pct30,
        "pct_vs_sma_200":              pct200,
        "drawdown_from_recent_high_pct": _drawdown_30d_pct,
        "dist_from_20dma_pct":         _dist_20dma_pct,
        "dist_from_50dma_pct":         _dist_50dma_pct,
        "dist_from_200dma_pct":        _dist_200dma_pct,
        # ── V4.2.5 — Fibonacci levels ────────────────────────────────────────
        **_fib_fields,
        # ── V4.2.5 — Wave structure ──────────────────────────────────────────
        **_wave_fields,
    }

    # ── Entry Structure V2 diagnostics (present whenever computed) ───────────
    if _v2_structure is not None:
        _b, _t, _bo = _v2_structure["base"], _v2_structure["trendline"], _v2_structure["breakout"]
        result["structure_v2"] = {
            "base_detected":               _b.get("base_detected"),
            "base_start_date":             _b.get("base_start_date"),
            "base_end_date":               _b.get("base_end_date"),
            "base_high":                   _b.get("base_high"),
            "base_low":                    _b.get("base_low"),
            "base_duration_bars":          _b.get("base_duration_bars"),
            "base_range_pct":              _b.get("base_range_pct"),
            "distance_to_base_high_pct":   _b.get("distance_to_base_high_pct"),
            "base_breakout_status":        _b.get("base_breakout_status"),
            "upper_range_position":        _b.get("upper_range_position"),
            "range_contraction":           _b.get("range_contraction"),
            "higher_lows_count":           _b.get("higher_lows_count"),
            "resistance_touch_count":      _b.get("resistance_touch_count"),
            "support_touch_count":         _b.get("support_touch_count"),
            "ascending_trendline_detected":_t.get("ascending_trendline_detected"),
            "trendline_slope_per_bar":     _t.get("trendline_slope_per_bar"),
            "anchor_1_date":               _t.get("anchor_1_date"),
            "anchor_1_price":              _t.get("anchor_1_price"),
            "anchor_2_date":               _t.get("anchor_2_date"),
            "anchor_2_price":              _t.get("anchor_2_price"),
            "trendline_touch_count":       _t.get("trendline_touch_count"),
            "projected_trendline_support": _t.get("projected_trendline_support"),
            "distance_to_trendline_pct":   _t.get("distance_to_trendline_pct"),
            "trendline_hold_state":        _t.get("trendline_hold_state"),
            "breakout_pivot":              _bo.get("breakout_pivot"),
            "breakout_date":               _bo.get("breakout_date"),
            "breakout_confirmed":          _bo.get("breakout_confirmed"),
            "breakout_confirmation_pct":   _bo.get("breakout_confirmation_pct"),
            "days_since_breakout":         _bo.get("days_since_breakout"),
            "close_vs_breakout_pivot_pct": _bo.get("close_vs_breakout_pivot_pct"),
            "closes_below_pivot_count":    _bo.get("closes_below_pivot_count"),
            "failed_breakout_confirmed":   _bo.get("failed_breakout_confirmed"),
            "failed_breakout_reason_codes":_bo.get("failed_breakout_reason_codes"),
            "support_candidates":          _v2_structure.get("support_candidates"),
            "primary_support":             _v2_structure.get("primary_support"),
            "base_archetype":              _v2_structure.get("base_archetype"),
            "base_location":               _v2_structure.get("base_location"),
            "low_base_floor":              _v2_structure.get("low_base_floor", {}).get("low_base_floor"),
            "low_base_floor_touch_count":  _v2_structure.get("low_base_floor", {}).get("low_base_floor_touch_count"),
            "low_base_floor_first_date":   _v2_structure.get("low_base_floor", {}).get("low_base_floor_first_date"),
            "low_base_floor_last_date":    _v2_structure.get("low_base_floor", {}).get("low_base_floor_last_date"),
            "distance_to_floor_pct":       _v2_structure.get("low_base_floor", {}).get("distance_to_floor_pct"),
            "floor_held_recently":         _v2_structure.get("low_base_floor", {}).get("floor_held_recently"),
            "floor_break_count":           _v2_structure.get("low_base_floor", {}).get("floor_break_count"),
            "low_base_support_quality":    _v2_structure.get("low_base_floor", {}).get("low_base_support_quality"),
            "low_base_reason_codes":       [c for c in evidence if isinstance(c, str) and c.startswith("LOW_BASE_")] or None,
        }
        if _v2_structure.get("primary_support"):
            result["primary_support_level"]   = _v2_structure["primary_support"].get("level")
            result["primary_support_distance_pct"] = _v2_structure["primary_support"].get("distance_pct")
            result["primary_support_reason"]  = _v2_structure["primary_support"].get("primary_support_reason")

    if persist:
        _save_lkg(symbol, result)
    else:
        _update_lkg_memory(symbol, result)
    return result


def is_entry_version_current(symbol: str) -> bool:
    """
    Return True only when a cached Entry row exists AND its
    entry_analysis_version matches the current ENTRY_ANALYSIS_VERSION.

    This is the DERIVED-ENTRY-ANALYSIS freshness check — distinct from
    market-data/TTL freshness (which lives in watchlist_stage2_service).
    Missing, null, older, or unrecognized/future version values all return
    False (reason: ENTRY_ANALYSIS_VERSION_STALE) and must be recomputed from
    already-available daily bars — never treated as a market-data staleness
    signal.
    """
    _load_lkg()
    entry = _ENTRY_STATE_LKG.get(symbol.upper())
    if not entry:
        return False
    return entry.get("entry_analysis_version") == ENTRY_ANALYSIS_VERSION


def get_entry_state_lkg(symbol: str) -> Optional[dict]:
    """Return cached entry state for symbol, or None if not available."""
    _load_lkg()
    return _ENTRY_STATE_LKG.get(symbol.upper())


def get_all_entry_state_lkg() -> dict[str, dict]:
    """Return the full in-memory LKG store."""
    _load_lkg()
    return dict(_ENTRY_STATE_LKG)
