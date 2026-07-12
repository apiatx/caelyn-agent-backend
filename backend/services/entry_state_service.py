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
    SUPPORT_LOST        — recently broke below key support
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
ENTRY_ANALYSIS_VERSION = 2

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
}


# ── Helpers ────────────────────────────────────────────────────────────────────

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


# ── Support hierarchy classifier ───────────────────────────────────────────────

def _classify_support_hierarchy(
    price:          float,
    sma50:          Optional[float],
    sma200:         Optional[float],
    ma30w:          Optional[float],
    base_low:       Optional[float],
    breakout_pivot: Optional[float],
    sorted_bars:    list[dict],
) -> dict:
    """
    Classify support levels as MAJOR or MINOR and detect HH/HL structure.

    Major support — structural / long-term levels that require meaningful
    confirmation to declare lost:
      breakout_pivot → base_low → 200DMA → 30w_MA (highest first)

    Minor support — short-term levels that can be tested without structural damage:
      SMA50 → recent 20-bar swing low

    Returns a flat dict with all support hierarchy fields.  Zero provider calls.
    """
    out: dict = {}

    # ── Major support: take the highest level below or nearest to price ───────
    major_candidates: list[tuple[str, float]] = []
    for label, lvl in [
        ("breakout_pivot", breakout_pivot),
        ("base_low",       base_low),
        ("200DMA",         sma200),
        ("30w_MA",         ma30w),
    ]:
        if lvl is not None and lvl > 0:
            major_candidates.append((label, lvl))
    # Sort descending by level — closest to current price first
    major_candidates.sort(key=lambda t: t[1], reverse=True)

    major_level: Optional[float] = None
    major_type:  Optional[str]   = None
    if major_candidates:
        major_type, major_level = major_candidates[0]

    if major_level is not None and price > 0:
        dist_pct = round((price - major_level) / major_level * 100, 2)
        out["major_support_level"]            = round(major_level, 4)
        out["major_support_type"]             = major_type
        out["major_support_source"]           = "v2_structure" if major_type in ("breakout_pivot", "base_low") else "ma"
        out["distance_to_major_support_pct"]  = dist_pct
        # Lost = more than 3 % below (minor test alone does not count)
        out["major_support_lost"]             = dist_pct < -3.0
    else:
        out["major_support_level"]           = None
        out["major_support_type"]            = None
        out["major_support_source"]          = None
        out["distance_to_major_support_pct"] = None
        out["major_support_lost"]            = None

    # ── Minor support ─────────────────────────────────────────────────────────
    minor_level: Optional[float] = None
    minor_type:  Optional[str]   = None
    if sma50 and sma50 > 0:
        minor_level, minor_type = sma50, "SMA50"
    elif len(sorted_bars) >= 20:
        w = sorted_bars[-20:]
        lows = [float(b.get("low") or b.get("close") or 0) for b in w if (b.get("low") or b.get("close"))]
        if lows:
            minor_level, minor_type = min(lows), "recent_swing"

    out["minor_support_level"] = round(minor_level, 4) if minor_level else None
    out["minor_support_type"]  = minor_type
    out["minor_support_lost"]  = (price < minor_level) if (minor_level and price > 0) else None

    # ── Best display level ────────────────────────────────────────────────────
    out["support_level_price"]  = out["major_support_level"] or out["minor_support_level"]
    out["support_level_type"]   = out["major_support_type"]  or out["minor_support_type"]
    out["support_level_source"] = ("major" if out["major_support_level"] else
                                   ("minor" if out["minor_support_level"] else None))

    # ── HH/HL structure from the last 40 bars ────────────────────────────────
    if len(sorted_bars) >= 40:
        window = sorted_bars[-40:]
        closes: list[float] = []
        lows_w: list[float] = []
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
            mid = len(closes) // 2
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
            for k in ("prior_swing_high", "recent_swing_high", "prior_swing_low",
                      "recent_swing_low", "higher_high_confirmed", "higher_low_confirmed",
                      "lower_high_confirmed", "lower_low_confirmed", "support_break_confirmed"):
                out[k] = None
    else:
        for k in ("prior_swing_high", "recent_swing_high", "prior_swing_low",
                  "recent_swing_low", "higher_high_confirmed", "higher_low_confirmed",
                  "lower_high_confirmed", "lower_low_confirmed", "support_break_confirmed"):
            out[k] = None

    return out


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

    entry_family = _STATE_TO_FAMILY.get(entry_state, FAMILY_BROKEN)
    base_score   = _BASE_SCORES.get(entry_state, 20)
    entry_score  = max(0, min(100, base_score + adj))
    entry_grade  = _grade(entry_score)

    # ── Support levels (legacy, backward-compat) ──────────────────────────────
    support_levels: list[dict] = []
    if price is not None:
        support_levels = _build_support_levels(
            price, sma20, sma50, sma200, ma30w_price
        )

    # ── Support hierarchy (major/minor + HH/HL structure) ────────────────────
    _support_hier: dict = {}
    if price is not None:
        _sv2b = (_v2_structure or {}).get("base", {})
        _sv2o = (_v2_structure or {}).get("breakout", {})
        _support_hier = _classify_support_hierarchy(
            price          = price,
            sma50          = sma50,
            sma200         = sma200,
            ma30w          = ma30w_price,
            base_low       = _sv2b.get("base_low"),
            breakout_pivot = _sv2o.get("breakout_pivot"),
            sorted_bars    = sorted_bars,
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
        "structure_state": _structure_state_raw,
        "extension_risk_modifier": _extension_risk_modifier,
        "extension_reason_codes":  _extension_reason_codes,
        "elapsed_ms":      round((time.time() - t0) * 1000, 1),
        "computed_at":     _now_iso(),
        "entry_analysis_version": ENTRY_ANALYSIS_VERSION,
        **_support_hier,
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
