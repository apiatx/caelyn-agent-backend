"""
actionability_service.py — Actionability V1
============================================
Pure deterministic decision layer combining EXISTING Trade Alignment
(THEME_ALIGNMENT archetype, confluence_v2_service) and EXISTING Entry
Structure V2 (entry_state_service) outputs into one setup-state
classification:

    "WHAT SHOULD I DO WITH THIS STOCK RIGHT NOW UNDER MY SWING-TRADING
     STRATEGY?"

Trade Alignment answers "is the opportunity aligned?"
Entry Structure answers "is the current price/chart structure actionable?"
Actionability combines those two EXISTING outputs. It recomputes nothing:
no Theme/Stage/Options/Catalyst/Entry-geometry recalculation, zero provider
calls, zero new cache, zero new scheduler.

ACTIONABILITY_VERSION = 1
"""

from __future__ import annotations

from typing import Any, Optional

ACTIONABILITY_VERSION = 1

# ── Canonical Actionability V1 states ──────────────────────────────────────────
READY               = "READY"
EARLY_WATCH         = "EARLY_WATCH"
WATCH               = "WATCH"
WAIT_FOR_BREAKOUT   = "WAIT_FOR_BREAKOUT"
WAIT_FOR_RETEST     = "WAIT_FOR_RETEST"
REVERSAL_WATCH      = "REVERSAL_WATCH"
TOO_EXTENDED        = "TOO_EXTENDED"
AVOID               = "AVOID"

ALL_STATES = [
    READY, EARLY_WATCH, WATCH, WAIT_FOR_BREAKOUT, WAIT_FOR_RETEST,
    REVERSAL_WATCH, TOO_EXTENDED, AVOID,
]

# ── Entry-state groupings (exact canonical Entry Structure V2 / legacy names,
#    verified against services/entry_state_service.py _STATE_TO_FAMILY) ───────
_HARD_BREAK_STATES = {"SUPPORT_LOST", "DOWNTREND", "FAILED_BREAKOUT"}
_SEVERE_EXTENSION_STATES = {"EXTREME_EXTENSION", "VERTICAL", "CROWDED_MOVE", "VOLUME_CLIMAX"}
_MILD_EXTENSION_STATE = "EXTENDED"

_READY_CONTEXT_STATES = {
    "HIGH_BASE_READY", "BREAKOUT_RETEST", "BREAKOUT_PULLBACK",
    "CONSTRUCTIVE_DIP", "TRENDLINE_SUPPORT_TEST", "LOW_BASE_READY",
}
_EARLY_WATCH_CONTEXT_STATES = {
    "HIGH_BASE_FORMING", "BASE_FORMING", "EARLY_ACCUMULATION", "SIGNALS_BUILDING",
}
_WAIT_FOR_BREAKOUT_CONTEXT_STATES = {
    "HIGH_BASE_COILING", "COILED", "LOW_BASE_COILING", "BREAKOUT_READY",
}
_WAIT_FOR_RETEST_CONTEXT_STATES = {"WAIT_FOR_RETEST"}
_REVERSAL_WATCH_CONTEXT_STATES = {"LOW_BASE_FORMING", "REVERSAL_WATCH"}

# FLOW_LEADING_PRICE eligible Entry states (pre-move / constructive contexts only;
# never severe-extension states — enforced separately as a hard exclusion).
_FLOW_LEADING_PRICE_ELIGIBLE_STATES = {
    "HIGH_BASE_FORMING", "HIGH_BASE_COILING", "HIGH_BASE_READY",
    "BREAKOUT_RETEST", "BREAKOUT_PULLBACK", "CONSTRUCTIVE_DIP",
    "TRENDLINE_SUPPORT_TEST", "LOW_BASE_COILING", "LOW_BASE_READY",
}

# ── Thresholds (deterministic, audited against live distributions in the
#    Part 17 shadow validation before being locked) ───────────────────────────
_READY_TA_MIN          = 70.0
_READY_ENTRY_SCORE_MIN = 65.0
_WAIT_RETEST_TA_MIN    = 55.0
_EARLY_WATCH_TA_MIN    = 65.0
_EARLY_WATCH_ENTRY_MIN = 45.0
_WAIT_BREAKOUT_TA_MIN  = 55.0
_REVERSAL_WATCH_TA_MIN = 45.0
_REVERSAL_WATCH_THEME_MIN = 75.0
_EXTENDED_WAIT_RETEST_TA_MIN = 55.0

_FLOW_LEADING_OPTIONS_MIN  = 75.0
_FLOW_LEADING_PRESSURE_MIN = 70.0


def _safe(v: Optional[float]) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _unavailable(reason: str, entry_family: Optional[str] = None) -> dict:
    return {
        "actionability_available":     False,
        "actionability_state":         None,
        "actionability_score":         None,
        "actionability_reason_codes":  [reason],
        "actionability_conflicts":     [],
        "actionability_strengths":     [],
        "actionability_entry_family":  entry_family,
        "actionability_version":       ACTIONABILITY_VERSION,
    }


def compute_actionability(
    entry_result:    Optional[dict],
    ta_fields:       dict,
    options_result:  Optional[dict] = None,
) -> dict:
    """
    Deterministic Actionability V1 classification.

    entry_result: full result dict from entry_state_service (per symbol),
                   or None if Entry unavailable.
    ta_fields:     the THEME_ALIGNMENT Trade Alignment fields dict already
                   computed by confluence_v2_service._compute_theme_alignment
                   (or the unavailable variant) for this symbol.
    options_result: raw per-symbol dict from options_alignment.get_options_alignment_bulk
                   (for premium_pressure_score, used only for the
                   FLOW_LEADING_PRICE reason code — NOT recomputed here).

    Zero recomputation of Theme/Stage/Options/Catalyst/Entry geometry.
    Zero provider calls. Zero new cache/scheduler.
    """
    # ── Part 16: minimum evidence ────────────────────────────────────────────
    if entry_result is None:
        return _unavailable("ENTRY_UNAVAILABLE")

    entry_state    = entry_result.get("entry_state")
    entry_score    = _safe(entry_result.get("entry_score"))
    entry_grade    = entry_result.get("entry_grade")
    entry_family   = entry_result.get("entry_family")
    structure_v2   = entry_result.get("structure_v2") or {}
    base_archetype = structure_v2.get("base_archetype")
    failed_breakout_confirmed = bool(structure_v2.get("failed_breakout_confirmed"))

    ta_available = bool(ta_fields.get("trade_alignment_available"))
    ta_score     = _safe(ta_fields.get("trade_alignment_score"))
    theme_score  = _safe(ta_fields.get("theme_alignment_score"))
    options_alignment_score = _safe(ta_fields.get("options_alignment_score"))
    options_pressure_state  = ta_fields.get("options_pressure_state")
    catalyst_available      = bool(ta_fields.get("catalyst_alignment_available"))

    premium_pressure_score = _safe((options_result or {}).get("premium_pressure_score"))

    strengths: list[str] = []
    conflicts: list[str] = []
    reasons:   list[str] = []

    # ── Actionability score (rank diagnostic only — NOT the decision layer) ──
    # 60% Trade Alignment + 40% Entry Score, per spec Part 3.
    if ta_available and ta_score is not None and entry_score is not None:
        actionability_score = round(0.60 * ta_score + 0.40 * entry_score, 1)
    else:
        actionability_score = None

    # ── FLOW_LEADING_PRICE interaction reason (not a state) ──────────────────
    flow_leading_price = (
        options_alignment_score is not None and options_alignment_score >= _FLOW_LEADING_OPTIONS_MIN
        and premium_pressure_score is not None and premium_pressure_score >= _FLOW_LEADING_PRESSURE_MIN
        and options_pressure_state == "BULLISH_ACCELERATING"
        and entry_state in _FLOW_LEADING_PRICE_ELIGIBLE_STATES
        and entry_state not in _SEVERE_EXTENSION_STATES
    )
    if flow_leading_price:
        reasons.append("FLOW_LEADING_PRICE")
        strengths.append("FLOW_LEADING_PRICE")

    # ── PART 4/24 — STATE PRECEDENCE (exact order, first match wins) ─────────

    # 1) Hard structural break → AVOID (overrides Trade Alignment strength).
    #    NOTE: failed_breakout_confirmed is a structure_v2 diagnostic flag that
    #    can remain True from an earlier phase of the same base even after the
    #    headline entry_state has moved on (e.g. into a fresh TRENDLINE_SUPPORT_TEST).
    #    The authoritative "is this currently broken" signal is entry_state
    #    itself (already the output of entry_state_service's own precedence
    #    logic) — only escalate on the flag when entry_state IS FAILED_BREAKOUT.
    if entry_state in _HARD_BREAK_STATES:
        conflicts.append("STRUCTURE_BROKEN")
        reasons.append("STRUCTURE_BROKEN")
        reasons.append(f"ENTRY_STATE_{entry_state}")
        return {
            "actionability_available":    True,
            "actionability_state":        AVOID,
            "actionability_score":        actionability_score,
            "actionability_reason_codes": reasons,
            "actionability_conflicts":    conflicts,
            "actionability_strengths":    strengths,
            "actionability_entry_family": entry_family,
            "actionability_version":      ACTIONABILITY_VERSION,
        }

    # 2) Severe extension / chase risk → TOO_EXTENDED (opportunity may remain
    #    excellent; do not hide behind a low score).
    if entry_state in _SEVERE_EXTENSION_STATES:
        conflicts.append("EXTREME_EXTENSION" if entry_state == "EXTREME_EXTENSION" else entry_state)
        conflicts.append("CHASE_RISK")
        reasons.append("CHASE_RISK")
        if ta_available and ta_score is not None and ta_score >= _READY_TA_MIN:
            strengths.append("STRONG_TRADE_ALIGNMENT")
            reasons.append("STRONG_ALIGNMENT_BAD_ENTRY")
        return {
            "actionability_available":    True,
            "actionability_state":        TOO_EXTENDED,
            "actionability_score":        actionability_score,
            "actionability_reason_codes": reasons,
            "actionability_conflicts":    conflicts,
            "actionability_strengths":    strengths,
            "actionability_entry_family": entry_family,
            "actionability_version":      ACTIONABILITY_VERSION,
        }

    # 3) Mild extension: TA-gated WAIT_FOR_RETEST vs TOO_EXTENDED.
    if entry_state == _MILD_EXTENSION_STATE:
        if ta_available and ta_score is not None and ta_score >= _EXTENDED_WAIT_RETEST_TA_MIN:
            strengths.append("STRONG_TRADE_ALIGNMENT")
            reasons.append("RETEST_REQUIRED")
            state = WAIT_FOR_RETEST
        else:
            conflicts.append("ENTRY_SCORE_CONFLICT")
            reasons.append("CHASE_RISK")
            state = TOO_EXTENDED
        return {
            "actionability_available":    True,
            "actionability_state":        state,
            "actionability_score":        actionability_score,
            "actionability_reason_codes": reasons,
            "actionability_conflicts":    conflicts,
            "actionability_strengths":    strengths,
            "actionability_entry_family": entry_family,
            "actionability_version":      ACTIONABILITY_VERSION,
        }

    # 4) Trade Alignment unavailable — Actionability degrades to WATCH,
    #    marked unavailable (Part 16). Never READY/EARLY_WATCH/FLOW_LEADING_PRICE.
    if not ta_available or ta_score is None:
        return {
            "actionability_available":    False,
            "actionability_state":        WATCH,
            "actionability_score":        None,
            "actionability_reason_codes": ["TRADE_ALIGNMENT_UNAVAILABLE"],
            "actionability_conflicts":    [],
            "actionability_strengths":    [],
            "actionability_entry_family": entry_family,
            "actionability_version":      ACTIONABILITY_VERSION,
        }

    # ── From here Entry is structurally sound and Trade Alignment is
    #    available — classify by Entry state context + Trade Alignment gate.

    # 5) READY.
    if entry_state in _READY_CONTEXT_STATES:
        if (ta_score >= _READY_TA_MIN and entry_score is not None
                and entry_score >= _READY_ENTRY_SCORE_MIN):
            strengths.append("STRONG_TRADE_ALIGNMENT")
            strengths.append("ACTIONABLE_ENTRY_STRUCTURE")
            strengths.append(entry_state)
            reasons.extend(["STRONG_TRADE_ALIGNMENT", "ACTIONABLE_ENTRY_STRUCTURE", entry_state])
            state = READY
        elif ta_score >= _WAIT_RETEST_TA_MIN:
            reasons.append("RETEST_REQUIRED")
            reasons.append("ENTRY_SCORE_LOW" if (entry_score or 0) < _READY_ENTRY_SCORE_MIN else "TRADE_ALIGNMENT_MODERATE")
            state = WAIT_FOR_RETEST
        else:
            reasons.append("TRADE_ALIGNMENT_MODERATE")
            state = WATCH
        return {
            "actionability_available":    True,
            "actionability_state":        state,
            "actionability_score":        actionability_score,
            "actionability_reason_codes": reasons,
            "actionability_conflicts":    conflicts,
            "actionability_strengths":    strengths,
            "actionability_entry_family": entry_family,
            "actionability_version":      ACTIONABILITY_VERSION,
        }

    # 6) WAIT_FOR_RETEST (explicit Entry state).
    if entry_state in _WAIT_FOR_RETEST_CONTEXT_STATES:
        if ta_score >= _WAIT_RETEST_TA_MIN:
            reasons.append("RETEST_REQUIRED")
            strengths.append("TRADE_ALIGNMENT_BUILDING")
            state = WAIT_FOR_RETEST
        else:
            reasons.append("RETEST_REQUIRED")
            state = WATCH
        return {
            "actionability_available":    True,
            "actionability_state":        state,
            "actionability_score":        actionability_score,
            "actionability_reason_codes": reasons,
            "actionability_conflicts":    conflicts,
            "actionability_strengths":    strengths,
            "actionability_entry_family": entry_family,
            "actionability_version":      ACTIONABILITY_VERSION,
        }

    # 7) WAIT_FOR_BREAKOUT contexts (mature coiling with defined ceiling).
    if entry_state in _WAIT_FOR_BREAKOUT_CONTEXT_STATES:
        if ta_score >= _WAIT_BREAKOUT_TA_MIN:
            reasons.append("WAITING_FOR_TRIGGER")
            strengths.append("TRADE_ALIGNMENT_BUILDING")
            state = WAIT_FOR_BREAKOUT
        else:
            reasons.append("WAITING_FOR_TRIGGER")
            state = WATCH
        return {
            "actionability_available":    True,
            "actionability_state":        state,
            "actionability_score":        actionability_score,
            "actionability_reason_codes": reasons,
            "actionability_conflicts":    conflicts,
            "actionability_strengths":    strengths,
            "actionability_entry_family": entry_family,
            "actionability_version":      ACTIONABILITY_VERSION,
        }

    # 8) REVERSAL_WATCH contexts (LOW_BASE turnaround structures).
    if entry_state in _REVERSAL_WATCH_CONTEXT_STATES or base_archetype == "LOW_BASE":
        evidence_ok = (
            (ta_score is not None and ta_score >= _REVERSAL_WATCH_TA_MIN)
            or options_pressure_state == "BULLISH_ACCELERATING"
            or options_pressure_state == "BEARISH_EASING"
            or (theme_score is not None and theme_score >= _REVERSAL_WATCH_THEME_MIN)
            or catalyst_available
        )
        if evidence_ok:
            reasons.append("DOWNSIDE_DEFINED")
            reasons.append("MOMENTUM_UNPROVEN")
            strengths.append("LOW_BASE_FLOOR_DEFINED")
            if options_pressure_state == "BEARISH_EASING":
                strengths.append("BEARISH_PRESSURE_EASING")
                reasons.append("BEARISH_PRESSURE_EASING")
            if options_pressure_state == "BULLISH_ACCELERATING":
                strengths.append("PREMIUM_PRESSURE_ACCELERATING")
                reasons.append("PREMIUM_PRESSURE_ACCELERATING")
            conflicts.append("MOMENTUM_UNPROVEN")
            state = REVERSAL_WATCH
        else:
            reasons.append("MOMENTUM_UNPROVEN")
            reasons.append("INSUFFICIENT_ALIGNMENT_EVIDENCE")
            state = WATCH
        return {
            "actionability_available":    True,
            "actionability_state":        state,
            "actionability_score":        actionability_score,
            "actionability_reason_codes": reasons,
            "actionability_conflicts":    conflicts,
            "actionability_strengths":    strengths,
            "actionability_entry_family": entry_family,
            "actionability_version":      ACTIONABILITY_VERSION,
        }

    # 9) EARLY_WATCH contexts (pre-move / early forming structure).
    if entry_state in _EARLY_WATCH_CONTEXT_STATES:
        if (ta_score >= _EARLY_WATCH_TA_MIN and entry_score is not None
                and entry_score >= _EARLY_WATCH_ENTRY_MIN):
            reasons.append("OPPORTUNITY_FORMING")
            reasons.append("PRE_MOVE_STRUCTURE")
            strengths.append("TRADE_ALIGNMENT_BUILDING")
            if theme_score is not None and theme_score >= _REVERSAL_WATCH_THEME_MIN:
                strengths.append("THEME_ALIGNMENT_STRONG")
                reasons.append("THEME_ALIGNMENT_STRONG")
            if options_pressure_state == "BULLISH_ACCELERATING":
                strengths.append("PREMIUM_PRESSURE_ACCELERATING")
                reasons.append("PREMIUM_PRESSURE_ACCELERATING")
            state = EARLY_WATCH
        else:
            reasons.append("PRE_MOVE_STRUCTURE")
            state = WATCH
        return {
            "actionability_available":    True,
            "actionability_state":        state,
            "actionability_score":        actionability_score,
            "actionability_reason_codes": reasons,
            "actionability_conflicts":    conflicts,
            "actionability_strengths":    strengths,
            "actionability_entry_family": entry_family,
            "actionability_version":      ACTIONABILITY_VERSION,
        }

    # 10) Default — WATCH (tracked, no clear action now).
    reasons.append("NO_CLEAR_ACTION")
    return {
        "actionability_available":    True,
        "actionability_state":        WATCH,
        "actionability_score":        actionability_score,
        "actionability_reason_codes": reasons,
        "actionability_conflicts":    conflicts,
        "actionability_strengths":    strengths,
        "actionability_entry_family": entry_family,
        "actionability_version":      ACTIONABILITY_VERSION,
    }
