"""
continuation_pattern_engine.py
================================
Deterministic continuation-pattern and extension-quality classifier.

Zero provider calls. Zero LLM calls. Zero bar fetching at request time.
Uses pre-computed fields already present on the confluence LKG snapshot
(entry_state, entry_family, extension_state, active_support_status,
stage_alignment_score, technical_metrics from stage2_row).

Pattern types (from spec):
  HIGH_TIGHT_FLAG               strong impulse + tight consolidation near highs
  BULL_FLAG                     healthy pullback in an uptrend
  BREAKOUT_SHELF_CONSOLIDATION  breakout that is now forming a shelf
  CUP_HANDLE                    rounding base with a handle
  VCP_CONTRACTION               volatility contraction pattern
  BREAKOUT_RETEST               retesting the prior breakout pivot
  LOW_BASE_REVERSAL             low-level base with reclaim potential
  WAVE_CONTINUATION_PROXY       Stage-2 continuation with defined support
  NO_PATTERN                    no structural evidence detected

Extension quality:
  CONSTRUCTIVE   big prior move, now consolidating near highs, support intact
  CHASE          vertical/parabolic with no consolidation, far from support
  NORMAL         moderate extension or healthy pullback
  BROKEN         active support lost / lower-low confirmed with lost support

Key WYFI-rule: a ticker should NOT be classified as TOO_EXTENDED / CHASE
merely because it is far above an OLD base, if all of the following hold:
  - entry_family is CONTINUATION (structural evidence exists)
  - entry_state suggests consolidation / retest (not pure exhaustion)
  - active support is intact (not lost_confirmed)
  - lower_low_confirmed is False
  - stage is S2 / S2-S3 (stage_alignment_score >= 55)
"""
from __future__ import annotations
from typing import Optional


# ── Pattern type constants ─────────────────────────────────────────────────────
PATTERN_HIGH_TIGHT_FLAG     = "HIGH_TIGHT_FLAG"
PATTERN_BULL_FLAG           = "BULL_FLAG"
PATTERN_BREAKOUT_SHELF      = "BREAKOUT_SHELF_CONSOLIDATION"
PATTERN_CUP_HANDLE          = "CUP_HANDLE"
PATTERN_VCP                 = "VCP_CONTRACTION"
PATTERN_BREAKOUT_RETEST     = "BREAKOUT_RETEST"
PATTERN_LOW_BASE_REVERSAL   = "LOW_BASE_REVERSAL"
PATTERN_WAVE_CONTINUATION   = "WAVE_CONTINUATION_PROXY"
PATTERN_NONE                = "NO_PATTERN"

# ── Pattern state constants ────────────────────────────────────────────────────
PSTATE_FORMING              = "PATTERN_FORMING"
PSTATE_READY                = "PATTERN_READY"
PSTATE_NEAR_TRIGGER         = "NEAR_BREAKOUT_TRIGGER"
PSTATE_CONTINUATION_READY   = "CONTINUATION_READY"
PSTATE_NOT_DETECTED         = "NOT_DETECTED"

# ── Extension quality constants ────────────────────────────────────────────────
EXT_CONSTRUCTIVE            = "CONSTRUCTIVE"
EXT_CHASE                   = "CHASE"
EXT_NORMAL                  = "NORMAL"
EXT_BROKEN                  = "BROKEN"

# ── Entry state sets ───────────────────────────────────────────────────────────
_CONTINUATION_ENTRY_STATES = {
    "WAIT_FOR_RETEST", "HIGH_BASE_FORMING", "HIGH_BASE_COILING", "HIGH_BASE_READY",
    "BREAKOUT_CONFIRMED", "BREAKOUT_PULLBACK", "BREAKOUT_RETEST",
    "TRENDLINE_SUPPORT_TEST", "CONSTRUCTIVE_DIP", "PULLBACK_IN_UPTREND",
    "SUPPORT_HOLD", "REACCELERATION",
}
_HIGH_BASE_STATES = {
    "HIGH_BASE_COILING", "HIGH_BASE_READY", "HIGH_BASE_FORMING",
    "WAIT_FOR_RETEST", "BREAKOUT_CONFIRMED",
}
_BASE_COILING_STATES = {
    "HIGH_BASE_COILING", "HIGH_BASE_READY", "COILED", "BREAKOUT_READY",
    "LOW_BASE_COILING", "LOW_BASE_READY",
}
_LOW_BASE_STATES = {"LOW_BASE_FORMING", "LOW_BASE_COILING", "LOW_BASE_READY"}
_EXHAUSTION_ONLY_STATES = {"VERTICAL", "VOLUME_CLIMAX", "CROWDED_MOVE"}

_SUPPORT_INTACT = {"above_support", "testing_support", "bounced_from_support"}
_SEVERE_EXT = {"EXTREME_EXTENSION", "EXTENDED", "VERTICAL", "VOLUME_CLIMAX", "CROWDED_MOVE"}

# Extension reset state constants
ERS_NOT_EXTENDED           = "NOT_EXTENDED"
ERS_CONSTRUCTIVE_RETEST    = "CONSTRUCTIVE_RETEST_AFTER_EXTENSION"
ERS_HEALTHY_PULLBACK       = "HEALTHY_TREND_PULLBACK"
ERS_HIGH_BASE_RESET        = "HIGH_BASE_RESET"
ERS_EARLY_RESET_FORMING    = "EARLY_RESET_FORMING"
ERS_EXTENDED_NO_RESET      = "EXTENDED_NO_RESET"
ERS_TRUE_CHASE             = "TRUE_CHASE_EXTENSION"
ERS_UNKNOWN                = "UNKNOWN"


def _sf(v: Optional[float], default: float = 0.0) -> float:
    return float(v) if v is not None else default


def _classify_extension_reset(
    extension_state:               str,
    extension_quality:             str,
    drawdown_from_recent_high_pct: Optional[float],
    dist_from_20dma_pct:           Optional[float],
    dist_from_50dma_pct:           Optional[float],
    dist_from_200dma_pct:          Optional[float],
    fib_retest_detected:           Optional[bool],
    fib_retest_type:               Optional[str],
    active_support_status:         str,
    lower_low_confirmed:           bool,
    has_cont_state:                bool,
    is_extended_state:             bool,
    primary_fib_confidence:        Optional[float] = None,
) -> tuple[str, float, list[str]]:
    """
    Classify the current extension reset state.

    Returns (extension_reset_state, extension_reset_score, reason_codes).
    Score is 0–100 indicating reset quality (higher = better opportunity).
    """
    reasons: list[str] = []
    support_ok = active_support_status in _SUPPORT_INTACT

    # ── NOT_EXTENDED ──────────────────────────────────────────────────────────
    if not is_extended_state:
        reasons.append(f"STATE_{extension_state}_NOT_SEVERE")
        return ERS_NOT_EXTENDED, 80.0, reasons

    # Hard fail: lower low with no support
    if lower_low_confirmed and not support_ok:
        reasons.append("LOWER_LOW_WITH_LOST_SUPPORT")
        return ERS_TRUE_CHASE, 0.0, reasons

    draw = drawdown_from_recent_high_pct  # negative = below high
    meaningful_pullback  = draw is not None and draw <= -8.0
    shallow_pullback     = draw is not None and -8.0 < draw <= -3.0
    still_near_high      = draw is None or draw > -3.0

    # Near MA checks
    near_20dma  = dist_from_20dma_pct is not None and abs(dist_from_20dma_pct) <= 4.0
    near_50dma  = dist_from_50dma_pct is not None and abs(dist_from_50dma_pct) <= 5.0
    near_200dma = dist_from_200dma_pct is not None and abs(dist_from_200dma_pct) <= 5.0
    near_any_ma   = near_20dma or near_50dma or near_200dma
    # V4.2.5.1: gate fib_hit by primary_fib_confidence (require >= 0.25)
    _fib_conf_ok  = (primary_fib_confidence is None or primary_fib_confidence >= 0.25)
    fib_hit       = bool(fib_retest_detected) and _fib_conf_ok

    # ── HIGH_BASE_RESET ───────────────────────────────────────────────────────
    # Extended but has continuation structure at elevated levels
    if has_cont_state and extension_quality == "CONSTRUCTIVE":
        score = 72.0 + (5.0 if fib_hit else 0.0)
        reasons.append("CONSTRUCTIVE_HIGH_BASE")
        if fib_hit:
            reasons.append(f"FIB_RETEST_{fib_retest_type}")
        return ERS_HIGH_BASE_RESET, score, reasons

    # ── CONSTRUCTIVE_RETEST_AFTER_EXTENSION ───────────────────────────────────
    # Extended, pulled back meaningfully, support intact, near a key level
    if (
        meaningful_pullback and
        support_ok and
        not lower_low_confirmed and
        (fib_hit or near_any_ma)
    ):
        score = 65.0
        if fib_hit:
            score += 10.0
            reasons.append(f"FIB_RETEST_{fib_retest_type}")
        if near_20dma:
            score += 5.0
            reasons.append(f"NEAR_20DMA_{dist_from_20dma_pct:.1f}PCT")
        elif near_50dma:
            score += 4.0
            reasons.append(f"NEAR_50DMA_{dist_from_50dma_pct:.1f}PCT")
        elif near_200dma:
            score += 3.0
            reasons.append(f"NEAR_200DMA_{dist_from_200dma_pct:.1f}PCT")
        reasons.append(f"DRAWDOWN_{draw:.1f}PCT")
        reasons.append("SUPPORT_INTACT")
        return ERS_CONSTRUCTIVE_RETEST, min(score, 90.0), reasons

    # ── HEALTHY_TREND_PULLBACK ────────────────────────────────────────────────
    # Not severely extended but pulling back constructively
    if meaningful_pullback and support_ok and not lower_low_confirmed:
        score = 70.0 + (5.0 if near_any_ma else 0.0)
        reasons.append(f"HEALTHY_PULLBACK_{draw:.1f}PCT")
        reasons.append("SUPPORT_INTACT")
        return ERS_HEALTHY_PULLBACK, score, reasons

    # ── EARLY_RESET_FORMING ───────────────────────────────────────────────────
    # Some pullback started but not at a key level yet
    if shallow_pullback and support_ok and not lower_low_confirmed:
        score = 50.0 + (10.0 if near_any_ma else 0.0)
        reasons.append(f"SHALLOW_PULLBACK_{draw:.1f}PCT")
        return ERS_EARLY_RESET_FORMING, score, reasons

    # ── TRUE_CHASE_EXTENSION ─────────────────────────────────────────────────
    # Still near high with no pullback, no Fib/MA proximity, pure chase
    if still_near_high and not fib_hit and not near_any_ma:
        score = 10.0
        reasons.append("NEAR_HIGH_NO_RESET")
        reasons.append(f"EXT_{extension_state}")
        if draw is not None:
            reasons.append(f"DRAWDOWN_ONLY_{draw:.1f}PCT")
        return ERS_TRUE_CHASE, score, reasons

    # ── EXTENDED_NO_RESET ────────────────────────────────────────────────────
    # Extended, some pullback, but not near a key level and no Fib retest
    score = 30.0
    reasons.append(f"EXT_{extension_state}")
    if draw is not None:
        reasons.append(f"DRAWDOWN_{draw:.1f}PCT")
    reasons.append("NO_KEY_LEVEL_PROXIMITY")
    return ERS_EXTENDED_NO_RESET, score, reasons


def detect_continuation_pattern(
    entry_state:                    Optional[str],
    entry_family:                   Optional[str],
    extension_state:                Optional[str],
    active_support_status:          Optional[str],
    lower_low_confirmed:            Optional[bool],
    distance_to_active_support_pct: Optional[float],
    active_support_touch_count:     Optional[int],
    stage_alignment_score:          Optional[float],
    trade_alignment_score:          Optional[float],
    entry_risk_reward_state:        Optional[str],
    base_archetype:                 Optional[str],
    actionability_state:            Optional[str],
    range_20d_pct:                  Optional[float] = None,
    prior_26w_trend_pct:            Optional[float] = None,
    squeeze_signal:                 Optional[str]   = None,
    pct_from_52w_high:              Optional[float] = None,
    # V4.2.5 — Extension Reset + Fib Retest fields
    drawdown_from_recent_high_pct:  Optional[float] = None,
    dist_from_20dma_pct:            Optional[float] = None,
    dist_from_50dma_pct:            Optional[float] = None,
    dist_from_200dma_pct:           Optional[float] = None,
    fib_retest_detected:            Optional[bool]  = None,
    fib_retest_type:                Optional[str]   = None,
    nearest_fib_label:              Optional[str]   = None,
    # V4.2.5.1 — Multi-timeframe Fib confidence gate
    primary_fib_confidence:         Optional[float] = None,
) -> dict:
    """
    Classify continuation pattern and extension quality from existing LKG fields.

    Returns:
        pattern_type, pattern_state, pattern_score (0–100),
        pattern_reason_codes, pattern_support_level (None — level not derivable
        without raw bars), pattern_breakout_trigger, pattern_invalidation_level,
        constructive_extension (bool), chase_extension (bool),
        extension_quality (str), extension_reason_codes,
        estimated_shelf_distance_pct (float or None — proxy for real shelf dist)
    """
    llc      = bool(lower_low_confirmed)
    asst     = active_support_status or "no_clear_support"
    ext      = extension_state or "HEALTHY"
    fam      = entry_family or "UNKNOWN"
    e_state  = entry_state or ""
    act      = actionability_state or "UNKNOWN"
    dist     = distance_to_active_support_pct
    stage_s  = _sf(stage_alignment_score)
    ta_s     = _sf(trade_alignment_score)
    touches  = int(active_support_touch_count or 0)

    support_ok         = asst in _SUPPORT_INTACT
    support_lost       = asst == "lost_confirmed"
    is_extended_state  = ext in _SEVERE_EXT
    is_extreme         = ext in ("EXTREME_EXTENSION", "VERTICAL", "VOLUME_CLIMAX")
    has_cont_state     = e_state in _CONTINUATION_ENTRY_STATES
    fam_cont           = fam == "CONTINUATION"
    fam_pre            = fam == "PRE_MOVE"
    fam_exhaust        = fam == "CHASE_EXHAUSTION"
    fam_broken         = fam == "BROKEN_OR_UNCLEAR"

    big_prior_move  = prior_26w_trend_pct is not None and prior_26w_trend_pct >= 40.0
    huge_prior_move = prior_26w_trend_pct is not None and prior_26w_trend_pct >= 80.0
    tight_consol    = range_20d_pct is not None and range_20d_pct <= 10.0
    very_tight      = range_20d_pct is not None and range_20d_pct <= 6.0
    near_52w_high   = pct_from_52w_high is not None and pct_from_52w_high >= -8.0

    reasons:     list[str] = []
    ext_reasons: list[str] = []

    # ── Early exit: support structurally lost ────────────────────────────────
    if support_lost:
        _d = _no_pattern(["SUPPORT_LOST_CONFIRMED"], EXT_BROKEN, ["MAJOR_SUPPORT_LOST"])
        _d["extension_reset_state"]        = ERS_TRUE_CHASE
        _d["extension_reset_score"]        = 0.0
        _d["extension_reset_reason_codes"] = ["SUPPORT_LOST_CONFIRMED"]
        return _d

    # ═══════════════════════════════════════════════════════════════════════
    # EXTENSION QUALITY — classify before pattern detection so patterns can
    # inherit the classification.
    # ═══════════════════════════════════════════════════════════════════════
    if llc and not support_ok:
        ext_quality = EXT_BROKEN
        ext_reasons.append("LOWER_LOW_WITH_WEAK_SUPPORT")

    elif (
        is_extended_state and
        fam_cont and
        has_cont_state and
        not llc and
        support_ok
    ):
        # Extended from OLD base, but continuation structure intact → CONSTRUCTIVE
        ext_quality = EXT_CONSTRUCTIVE
        ext_reasons.append(f"EXTENDED_CONTINUATION_{e_state}")
        if ext == "EXTREME_EXTENSION":
            ext_reasons.append("HIGH_TIGHT_CONSOLIDATION_ABOVE_OLD_BASE")
        if tight_consol:
            ext_reasons.append(f"TIGHT_RANGE_{range_20d_pct:.1f}PCT")
        if near_52w_high:
            ext_reasons.append("CONSOLIDATING_NEAR_52W_HIGH")

    elif (
        is_extended_state and
        (fam_exhaust or e_state in _EXHAUSTION_ONLY_STATES) and
        not has_cont_state and
        (dist is None or dist > 15.0)
    ):
        ext_quality = EXT_CHASE
        ext_reasons.append(f"PURE_EXHAUSTION_{ext}")
        if dist is not None and dist > 15.0:
            ext_reasons.append(f"FAR_FROM_SUPPORT_{dist:.1f}PCT")

    elif is_extended_state and not fam_cont and not has_cont_state:
        ext_quality = EXT_CHASE
        ext_reasons.append(f"EXTENDED_NO_STRUCTURE_{ext}_{fam}")

    else:
        ext_quality = EXT_NORMAL
        ext_reasons.append(f"STATE_{ext}_{fam}")

    constructive = (ext_quality == EXT_CONSTRUCTIVE)
    chase        = (ext_quality == EXT_CHASE)

    # ═══════════════════════════════════════════════════════════════════════
    # EXTENSION RESET STATE — V4.2.5
    # Computed from new bar-derived fields passed in from entry_state_service.
    # Independent of extension_quality; captures what the price is doing NOW
    # relative to Fibonacci levels, MAs, and recent high drawdown.
    # ═══════════════════════════════════════════════════════════════════════
    _ext_reset_state, _ext_reset_score, _ext_reset_reasons = _classify_extension_reset(
        extension_state               = ext,
        extension_quality             = ext_quality,
        drawdown_from_recent_high_pct = drawdown_from_recent_high_pct,
        dist_from_20dma_pct           = dist_from_20dma_pct,
        dist_from_50dma_pct           = dist_from_50dma_pct,
        dist_from_200dma_pct          = dist_from_200dma_pct,
        fib_retest_detected           = fib_retest_detected,
        fib_retest_type               = fib_retest_type,
        active_support_status         = asst,
        lower_low_confirmed           = llc,
        has_cont_state                = has_cont_state,
        is_extended_state             = is_extended_state,
        primary_fib_confidence        = primary_fib_confidence,
    )

    def _ers(d: dict) -> dict:
        d["extension_reset_state"]        = _ext_reset_state
        d["extension_reset_score"]        = round(_ext_reset_score, 1)
        d["extension_reset_reason_codes"] = _ext_reset_reasons
        return d

    # ═══════════════════════════════════════════════════════════════════════
    # ESTIMATED SHELF DISTANCE — proxy for actual shelf support proximity
    # when active_support was calibrated to old base, not current shelf.
    # Used by the confluence engine when constructive_extension=True.
    # ═══════════════════════════════════════════════════════════════════════
    estimated_shelf_distance_pct: Optional[float] = None
    if constructive and e_state in ("WAIT_FOR_RETEST", "HIGH_BASE_FORMING",
                                    "HIGH_BASE_COILING", "HIGH_BASE_READY"):
        if very_tight:
            estimated_shelf_distance_pct = _sf(range_20d_pct) * 0.5
        elif tight_consol:
            estimated_shelf_distance_pct = _sf(range_20d_pct) * 0.6
        else:
            estimated_shelf_distance_pct = 8.0   # conservative estimate

    # ═══════════════════════════════════════════════════════════════════════
    # PATTERN DETECTION — ordered by priority / specificity
    # ═══════════════════════════════════════════════════════════════════════

    # ── 1. HIGH_TIGHT_FLAG ────────────────────────────────────────────────
    # Strong prior impulse (pole) + now tight consolidation near highs
    if (
        ext in ("EXTREME_EXTENSION", "EXTENDED") and
        fam_cont and
        e_state in _HIGH_BASE_STATES and
        support_ok and
        not llc
    ):
        score = 62
        if huge_prior_move:
            score += 12
            reasons.append(f"POLE_HUGE_{prior_26w_trend_pct:.0f}PCT")
        elif big_prior_move:
            score += 6
            reasons.append(f"POLE_BIG_{prior_26w_trend_pct:.0f}PCT")
        else:
            reasons.append("POLE_MAGNITUDE_UNKNOWN_IMPLIED_BY_EXTENSION")

        if very_tight:
            score += 10
            reasons.append(f"VERY_TIGHT_FLAG_{range_20d_pct:.1f}PCT")
        elif tight_consol:
            score += 6
            reasons.append(f"TIGHT_FLAG_{range_20d_pct:.1f}PCT")

        if near_52w_high:
            score += 5
            reasons.append("FLAG_AT_52W_HIGH")
        if squeeze_signal in ("tight", "coiling"):
            score += 5
            reasons.append(f"SQUEEZE_{squeeze_signal}")
        if touches >= 2:
            score += 3
            reasons.append(f"FLAG_SUPPORT_TOUCHES_{touches}")

        reasons.append(f"ENTRY_{e_state}")
        reasons.append(f"SUPPORT_{asst}")

        if e_state in ("HIGH_BASE_READY",) or (very_tight and near_52w_high):
            p_state = PSTATE_NEAR_TRIGGER
        elif e_state in ("HIGH_BASE_COILING",) or tight_consol:
            p_state = PSTATE_READY
        else:
            p_state = PSTATE_FORMING

        return _ers(_pattern(
            PATTERN_HIGH_TIGHT_FLAG, p_state, min(score, 100), reasons,
            constructive, chase, ext_quality, ext_reasons,
            "ABOVE_FLAG_RESISTANCE", "BELOW_FLAG_LOW",
            estimated_shelf_distance_pct,
        ))

    # ── 2. BREAKOUT_SHELF_CONSOLIDATION ────────────────────────────────────
    # Recent breakout now forming a shelf / flat base at highs
    if (
        ext in ("EXTENDED", "MODERATELY_EXTENDED") and
        fam_cont and
        e_state in {"WAIT_FOR_RETEST", "BREAKOUT_PULLBACK", "BREAKOUT_RETEST",
                    "HIGH_BASE_FORMING", "HIGH_BASE_COILING"} and
        support_ok and
        not llc
    ):
        score = 60
        if tight_consol:
            score += 8
            reasons.append(f"SHELF_TIGHT_{range_20d_pct:.1f}PCT")
        if touches >= 2:
            score += 5
            reasons.append(f"SHELF_TOUCHES_{touches}")
        if near_52w_high:
            score += 5
            reasons.append("SHELF_NEAR_HIGH")
        reasons.append(f"SHELF_{e_state}")
        reasons.append(f"SUPPORT_{asst}")
        p_state = PSTATE_READY if (tight_consol or touches >= 2) else PSTATE_FORMING
        return _ers(_pattern(
            PATTERN_BREAKOUT_SHELF, p_state, min(score, 100), reasons,
            constructive, chase, ext_quality, ext_reasons,
            "ABOVE_SHELF_HIGH", "BELOW_SHELF_SUPPORT",
            estimated_shelf_distance_pct,
        ))

    # ── 3. VCP_CONTRACTION ─────────────────────────────────────────────────
    # Volatility contracting; multiple touches defining the support zone
    if (
        ext in ("HEALTHY", "MODERATELY_EXTENDED") and
        e_state in _BASE_COILING_STATES and
        support_ok and
        not llc and
        touches >= 2
    ):
        score = 68
        if e_state in ("HIGH_BASE_READY", "BREAKOUT_READY", "LOW_BASE_READY"):
            score += 8
            reasons.append("VCP_MATURE_READY")
        if squeeze_signal in ("tight", "coiling"):
            score += 8
            reasons.append(f"VCP_SQUEEZE_{squeeze_signal}")
        if touches >= 3:
            score += 5
            reasons.append(f"VCP_MULTI_TOUCH_{touches}")
        if very_tight:
            score += 5
            reasons.append(f"VCP_VERY_TIGHT_{range_20d_pct:.1f}PCT")
        reasons.append(f"VCP_{e_state}")
        p_state = PSTATE_NEAR_TRIGGER if score >= 82 else PSTATE_READY
        return _ers(_pattern(
            PATTERN_VCP, p_state, min(score, 100), reasons,
            False, False, ext_quality, ext_reasons,
            "ABOVE_CONTRACTION_HIGH", "BELOW_LAST_LOW",
            None,
        ))

    # ── 4. BULL_FLAG ───────────────────────────────────────────────────────
    # Clean healthy pullback in an uptrend; defined support/flag low
    if (
        ext in ("HEALTHY", "MODERATELY_EXTENDED") and
        fam_cont and
        e_state in {"BREAKOUT_PULLBACK", "TRENDLINE_SUPPORT_TEST",
                    "CONSTRUCTIVE_DIP", "PULLBACK_IN_UPTREND"} and
        support_ok and
        not llc
    ):
        score = 68
        if e_state == "CONSTRUCTIVE_DIP":
            score += 5
            reasons.append("SHALLOW_DIP_NEAR_MA")
        elif e_state == "TRENDLINE_SUPPORT_TEST":
            score += 10
            reasons.append("TRENDLINE_HOLDING")
        elif e_state == "BREAKOUT_PULLBACK":
            score += 5
            reasons.append("PULLBACK_TO_PIVOT")
        else:
            reasons.append("PULLBACK_IN_UPTREND")
        reasons.append(f"SUPPORT_{asst}")
        return _ers(_pattern(
            PATTERN_BULL_FLAG, PSTATE_CONTINUATION_READY, min(score, 100), reasons,
            False, False, ext_quality, ext_reasons,
            "ABOVE_FLAG_HIGH", "BELOW_FLAG_LOW",
            None,
        ))

    # ── 5. BREAKOUT_RETEST ─────────────────────────────────────────────────
    if e_state == "BREAKOUT_RETEST" and support_ok and not llc:
        score = 75
        reasons.append("RETESTING_BREAKOUT_PIVOT")
        reasons.append(f"SUPPORT_{asst}")
        if touches >= 2:
            score += 5
            reasons.append(f"PIVOT_TOUCHES_{touches}")
        return _ers(_pattern(
            PATTERN_BREAKOUT_RETEST, PSTATE_CONTINUATION_READY, min(score, 100), reasons,
            False, False, ext_quality, ext_reasons,
            "RECLAIM_ABOVE_PIVOT", "CLOSE_BELOW_PIVOT",
            None,
        ))

    # ── 6. WAVE_CONTINUATION_PROXY ─────────────────────────────────────────
    # Stage-2 continuation with healthy support — proxy for wave 3/5 in uptrend
    if (
        fam_cont and
        e_state in {"PULLBACK_IN_UPTREND", "SUPPORT_HOLD", "REACCELERATION",
                    "HIGH_BASE_FORMING"} and
        support_ok and
        not llc and
        stage_s >= 55.0
    ):
        score = 62
        if e_state == "REACCELERATION":
            score = 78
            reasons.append("REACCELERATION_AFTER_RESET")
        elif e_state == "SUPPORT_HOLD":
            score = 70
            reasons.append("HOLDING_KEY_SUPPORT")
        else:
            reasons.append(f"WAVE_CONTINUATION_{e_state}")
        reasons.append(f"STAGE_{stage_s:.0f}")
        reasons.append(f"SUPPORT_{asst}")
        return _ers(_pattern(
            PATTERN_WAVE_CONTINUATION, PSTATE_CONTINUATION_READY, min(score, 100), reasons,
            False, False, ext_quality, ext_reasons,
            "PRIOR_HIGH_RECLAIM", "BELOW_ACTIVE_SUPPORT",
            None,
        ))

    # ── 7. LOW_BASE_REVERSAL ───────────────────────────────────────────────
    if fam_pre and e_state in _LOW_BASE_STATES and not llc:
        score_map = {"LOW_BASE_READY": 68, "LOW_BASE_COILING": 55, "LOW_BASE_FORMING": 40}
        score = score_map.get(e_state, 40)
        if support_ok:
            score += 5
            reasons.append(f"SUPPORT_{asst}")
        reasons.append(f"LOW_BASE_{e_state}")
        p_state = PSTATE_READY if score >= 60 else PSTATE_FORMING
        return _ers(_pattern(
            PATTERN_LOW_BASE_REVERSAL, p_state, score, reasons,
            False, False, ext_quality, ext_reasons,
            "RECLAIM_BASE_CEILING", "BELOW_BASE_FLOOR",
            None,
        ))

    # ── No pattern detected ───────────────────────────────────────────────
    return _ers(_no_pattern(
        [f"NO_STRUCTURAL_PATTERN_{fam}_{e_state}_{ext}"],
        ext_quality,
        ext_reasons,
    ))


def _pattern(
    p_type, p_state, score, reasons,
    constructive, chase, ext_quality, ext_reasons,
    trigger, invalidation,
    estimated_shelf_dist,
) -> dict:
    return {
        "pattern_type":                  p_type,
        "pattern_state":                 p_state,
        "pattern_score":                 score,
        "pattern_reason_codes":          reasons,
        "constructive_extension":        constructive,
        "chase_extension":               chase,
        "extension_quality":             ext_quality,
        "extension_reason_codes":        ext_reasons,
        "current_shelf_support":         None,
        "pattern_breakout_trigger":      trigger,
        "pattern_invalidation_level":    invalidation,
        "estimated_shelf_distance_pct":  estimated_shelf_dist,
    }


def _no_pattern(reasons: list[str], ext_quality: str, ext_reasons: list[str]) -> dict:
    return {
        "pattern_type":                  PATTERN_NONE,
        "pattern_state":                 PSTATE_NOT_DETECTED,
        "pattern_score":                 0,
        "pattern_reason_codes":          reasons,
        "constructive_extension":        False,
        "chase_extension":               ext_quality == EXT_CHASE,
        "extension_quality":             ext_quality,
        "extension_reason_codes":        ext_reasons,
        "current_shelf_support":         None,
        "pattern_breakout_trigger":      None,
        "pattern_invalidation_level":    None,
        "estimated_shelf_distance_pct":  None,
    }
