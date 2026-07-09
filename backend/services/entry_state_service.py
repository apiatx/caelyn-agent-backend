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
}

_STATE_TO_FAMILY: dict[str, str] = {
    "BASE_FORMING":      FAMILY_PRE_MOVE,
    "COILED":            FAMILY_PRE_MOVE,
    "BREAKOUT_READY":    FAMILY_PRE_MOVE,
    "EARLY_ACCUMULATION":FAMILY_PRE_MOVE,
    "SIGNALS_BUILDING":  FAMILY_PRE_MOVE,
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
        evidence.append("breakout_sig=failed_breakout")
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


# ── Public API ─────────────────────────────────────────────────────────────────

def analyze_entry_state_from_bars(
    symbol:       str,
    daily_bars:   list[dict],
    stage_result: dict,
) -> dict:
    """
    Compute entry state from already-fetched daily bars + stage_result.

    Parameters
    ----------
    symbol       : ticker symbol (uppercase)
    daily_bars   : list of daily OHLCV dicts with at least {"date", "close"}.
                   Bars must span ≥ 20 days; sorted oldest → newest.
    stage_result : output of analyze_symbol_stage() — provides stage number,
                   extension_state, extension_pct_30w_ma, stage_signals.

    Returns
    -------
    Dict with keys: symbol, entry_family, entry_state, entry_score, entry_grade,
                    support_levels, evidence, stage_int, extension_state,
                    extension_pct, computed_at.
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
    vol_ratio_w = sigs.get("volume_ratio")   # weekly vol ratio from stage

    # Derive stage_key from label
    _label_to_key = {
        "Stage 1: Base":            "1",
        "Stage 1-2: Watch":         "12",
        "Stage 2: Advance":         "2",
        "Stage 2b: Breakout":       "2b",
        "Stage 3: Top":             "3",
        "Stage 3m: Late Momentum":  "3m",
        "Stage 4: Decline":         "4",
    }
    stage_key = _label_to_key.get(stage_label, str(stage_int))

    # ── Compute daily technical metrics ──────────────────────────────────────
    try:
        from services.stage_analysis import compute_technical_metrics
        tech = compute_technical_metrics(daily_bars)
    except Exception:
        tech = {}

    ext_risk    = tech.get("extension_risk", "neutral")
    breakout_s  = tech.get("breakout_signal")
    entry_zone  = tech.get("entry_zone", "neutral")
    squeeze_s   = tech.get("squeeze_signal")
    ma_stack    = tech.get("ma_stack")
    ad_sig      = tech.get("accumulation_distribution_signal")
    sma20       = tech.get("sma_20")
    sma50       = tech.get("sma_50")
    sma200      = tech.get("sma_200")
    pct20       = tech.get("pct_vs_sma_20")
    pct50       = tech.get("pct_vs_sma_50")

    # Daily bars sorted for price lookup
    sorted_bars = sorted(daily_bars, key=lambda b: b["date"])
    price = float(sorted_bars[-1]["close"]) if sorted_bars else None

    # 30w MA in price units (back-calculate from ext_pct if available)
    ma30w_price: Optional[float] = None
    if price is not None and ext_pct is not None:
        try:
            ma30w_price = price / (1 + ext_pct / 100.0)
        except ZeroDivisionError:
            pass

    # ── Classify ──────────────────────────────────────────────────────────────
    entry_state, adj, evidence = _classify_entry_state(
        stage_int    = stage_int,
        stage_key    = stage_key,
        extension_state = ext_state,
        extension_pct   = ext_pct,
        ext_risk     = ext_risk,
        breakout_sig = breakout_s,
        entry_zone_val = entry_zone,
        squeeze_sig  = squeeze_s,
        ma_stack     = ma_stack,
        pct20        = pct20,
        pct50        = pct50,
        ad_sig       = ad_sig,
        volume_ratio = (float(vol_ratio_w) if vol_ratio_w else None),
        weeks_above  = weeks_above,
        ma_slope     = ma_slope,
        bars_count   = len(daily_bars),
    )

    entry_family = _STATE_TO_FAMILY.get(entry_state, FAMILY_BROKEN)
    base_score   = _BASE_SCORES.get(entry_state, 20)
    raw_score    = max(0, min(100, base_score + adj))
    entry_score  = raw_score
    entry_grade  = _grade(entry_score)

    # ── Support levels ────────────────────────────────────────────────────────
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
        "price":           round(price, 4) if price else None,
        "elapsed_ms":      round((time.time() - t0) * 1000, 1),
        "computed_at":     _now_iso(),
    }

    _save_lkg(symbol, result)
    return result


def get_entry_state_lkg(symbol: str) -> Optional[dict]:
    """Return cached entry state for symbol, or None if not available."""
    _load_lkg()
    return _ENTRY_STATE_LKG.get(symbol.upper())


def get_all_entry_state_lkg() -> dict[str, dict]:
    """Return the full in-memory LKG store."""
    _load_lkg()
    return dict(_ENTRY_STATE_LKG)
