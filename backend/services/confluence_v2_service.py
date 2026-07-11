"""
confluence_v2_service.py — Phase 7: Confluence V2 Shadow System
================================================================
Pure cache-read signal aggregator — zero external API calls, zero provider calls.

Reads from:
  • entry_state_service  — entry state + entry_score (Phase 4)
  • theme_rotation_service — theme rotation_score + phase (Phase 6)
  • backend/data/themes_rs_lkg.json       — stage + RS signals
  • backend/data/x_consensus_weekly.json  — social signals (bonus only)
  • backend/data/options_master_lkg_v1.json — options flow
  • backend/data/watchlist_stage2_lkg.json  — stage2 LKG (technical)

For each symbol on the watchlist (via Stage2 LKG), Confluence V2 assembles a
multi-signal verdict:

  base_trade_confluence_score (0–100) — 4-signal weighted base
  social_bonus_score           (0–10) — conditional additive bonus
  trade_confluence_score       (0–100) — base + social bonus (clamped)
  investment_confluence_score  (0–100) — base score (pure technical/structural)
  confluence_grade             (A+/A/B/C/AVOID) — from trade score
  confluence_verdict           (STRONG_BUY / BUY / WATCH / NEUTRAL / AVOID / SHORT_AVOID)
  signal_breakdown             — per-signal scores and metadata
  confidence                   — how many signals were available (0–1)

Base Weights (renormalized, sum to 1.0, social removed from denominator)
-------
  entry_state        0.353  (Phase 4 engine)
  theme_rotation     0.235  (Phase 6 engine)
  stage_quality      0.235  (from stage analysis)
  options_flow       0.177  (from options master LKG)

Social Bonus (additive, max +10 points)
-------
  Eligibility gate: entry_score >= 60 AND theme_sig >= 0.55
                    AND (stage_sig >= 0.60 OR options_sig >= 0.65)
  Blocking gate:    entry_grade == AVOID OR entry_family in CHASE/BROKEN → bonus = 0
  Tiers: VERY_STRONG (+8–10), STRONG (+5–7), MODERATE (+2–4), WEAK/ABSENT (0)
"""

from __future__ import annotations

import json
import statistics
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from services.actionability_service import (
    ALL_STATES as _ACTIONABILITY_ALL_STATES,
    ACTIONABILITY_VERSION as _ACTIONABILITY_VERSION,
    compute_actionability as _compute_actionability,
)

# ── Data paths ─────────────────────────────────────────────────────────────────
_BASE          = Path(__file__).parent.parent
_STAGE2_LKG    = _BASE / "data" / "watchlist_stage2_lkg.json"
_THEMES_RS_LKG = _BASE / "data" / "themes_rs_lkg.json"
_X_CONSENSUS   = _BASE / "data" / "x_consensus_weekly.json"
_OPTIONS_LKG   = _BASE / "data" / "options_master_lkg_v1.json"

# ── Base weights (4 non-social signals, sum to 1.0) ───────────────────────────
# Renormalized from original (entry=0.30, theme=0.20, stage=0.20, options=0.15)
# by dividing by 0.85 (the original non-social total).
_W_ENTRY   = 0.353
_W_THEME   = 0.235
_W_STAGE   = 0.235
_W_OPTIONS = 0.177
# _W_TOTAL  = 1.000  (exact sum: 0.353+0.235+0.235+0.177 = 1.000)

# Social is now a conditional additive bonus (0–10 pts), NOT a weight.
_SOCIAL_MAX_BONUS = 10.0

# ── Verdict thresholds ─────────────────────────────────────────────────────────
_VERDICTS = [
    (85, "STRONG_BUY"),
    (70, "BUY"),
    (55, "WATCH"),
    (40, "NEUTRAL"),
    (25, "AVOID"),
]

# ── THEME_ALIGNMENT Trade Alignment archetype (SHADOW, additive only) ─────────
# Base weights: Theme / Stage / Options / Catalyst — 25 / 25 / 25 / 25.
# Available-signal renormalization: when a signal is unavailable, its weight
# is excluded from BOTH numerator and denominator (never defaulted to 0/50).
_TA_W_THEME    = 25.0
_TA_W_STAGE    = 25.0
_TA_W_OPTIONS  = 25.0
_TA_W_CATALYST = 25.0

_TA_GRADES = [
    (85, "VERY_HIGH"),
    (70, "HIGH"),
    (55, "MODERATE"),
    (35, "LOW"),
]


def _ta_grade(score: float) -> str:
    for threshold, label in _TA_GRADES:
        if score >= threshold:
            return label
    return "VERY_LOW"

# ── Stage label → integer map (for hard cap logic) ─────────────────────────────
_LABEL_TO_STAGE_INT: dict[str, int] = {
    "S1 Base":       1,
    "S1-2 Watch":    1,
    "S2-S3 Advance": 2,
    "S2 Breakout":   2,
    "S3-S4 Top":     3,
    "S3 Momentum":   3,
    "S4 Decline":    4,
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clamp01(v: float) -> float:
    return max(0.0, min(1.0, v))


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


# ═════════════════════════════════════════════════════════════════════════════
# ELITE_ASSET_REBOUND V1 — second Trade Alignment archetype (SHADOW/additive).
#
# "Is an exceptional asset that experienced a meaningful reset beginning to
#  form a high-asymmetry long-biased rebound opportunity?"
#
# Zero provider calls. Reads only already-computed Investment Alignment V1,
# Entry Structure V2, Stage2 technical_metrics, and Options Alignment V2
# outputs. Does not modify THEME_ALIGNMENT, Investment Alignment, Entry
# Structure, Options Alignment, Catalyst Alignment, or Social Bonus.
# ═════════════════════════════════════════════════════════════════════════════

ELITE_ASSET_REBOUND_VERSION = 1

# Locked V1 outer weights (Part 2). Options is the only omit/renormalize slot.
_ER_W_INVESTMENT = 40.0
_ER_W_RESET      = 25.0
_ER_W_EVIDENCE   = 25.0
_ER_W_OPTIONS    = 10.0

# Elite Asset Gate (Part 3).
_ER_GATE_INVESTMENT_MIN         = 75.0
_ER_THESIS_FIN_ACCEL_MIN        = 45.0
_ER_THESIS_FWD_EXPECT_MIN       = 40.0

# Prior-leadership diagnostic threshold (Part 4) — informational only, never
# a hard blocking gate (Stage 2 / Long-Term Leadership is NOT mandatory).
_ER_PRIOR_LEADERSHIP_MIN = 40.0

# ── Reset Quality state buckets (Part 5) ──────────────────────────────────────
_ER_RESET_HARD_BREAK_STATES = {"SUPPORT_LOST", "DOWNTREND", "VERTICAL"}
_ER_RESET_FAVORABLE_STATES = {
    "LOW_BASE_FORMING", "LOW_BASE_COILING", "LOW_BASE_READY",
    "REVERSAL_WATCH", "CONSTRUCTIVE_DIP", "TRENDLINE_SUPPORT_TEST",
    "BREAKOUT_PULLBACK", "BREAKOUT_RETEST", "HIGH_BASE_FORMING",
    "HIGH_BASE_COILING", "WAIT_FOR_RETEST",
}

# ── Rebound Evidence state → score map (Part 6) ───────────────────────────────
# LOW_BASE_* / REVERSAL_WATCH / CONSTRUCTIVE_DIP / retest-family states carry
# genuine "reset turning into rebound" evidence. EXTREME_EXTENSION / VERTICAL
# are explicitly NOT rebound evidence (Part 19 false-positive control).
_ER_REBOUND_EVIDENCE_MAP: dict[str, float] = {
    "LOW_BASE_READY":         85.0,
    "BREAKOUT_RETEST":        80.0,
    "LOW_BASE_COILING":       72.0,
    "REVERSAL_WATCH":         68.0,
    "CONSTRUCTIVE_DIP":       66.0,
    "TRENDLINE_SUPPORT_TEST": 64.0,
    "BREAKOUT_PULLBACK":      62.0,
    "HIGH_BASE_COILING":      58.0,
    "LOW_BASE_FORMING":       55.0,
    "HIGH_BASE_FORMING":      50.0,
    "WAIT_FOR_RETEST":        48.0,
    "BASE_FORMING":           40.0,
    "NO_CLEAR_ENTRY":         35.0,
    "EXTREME_EXTENSION":      15.0,
    "FAILED_BREAKOUT":        12.0,
    "VERTICAL":               10.0,
    "SUPPORT_LOST":           10.0,
    "DOWNTREND":               8.0,
}
_ER_REBOUND_EVIDENCE_POSITIVE_STATES = {
    "LOW_BASE_FORMING", "LOW_BASE_COILING", "LOW_BASE_READY", "REVERSAL_WATCH",
    "CONSTRUCTIVE_DIP", "TRENDLINE_SUPPORT_TEST", "BREAKOUT_PULLBACK",
    "BREAKOUT_RETEST", "HIGH_BASE_FORMING", "HIGH_BASE_COILING",
}
_ER_REBOUND_EVIDENCE_WEAK_STATES    = {"NO_CLEAR_ENTRY", "BASE_FORMING", "WAIT_FOR_RETEST"}
_ER_REBOUND_EVIDENCE_NEGATIVE_STATES = {
    "SUPPORT_LOST", "DOWNTREND", "FAILED_BREAKOUT", "EXTREME_EXTENSION", "VERTICAL",
}

# ── Options Pressure Improvement state → score map (Part 7) ──────────────────
_ER_OPTIONS_IMPROVEMENT_MAP: dict[str, float] = {
    "BULLISH_ACCELERATING": 90.0,
    "BEARISH_EASING":       75.0,
    "MIXED":                50.0,
    "BULLISH_FADING":       40.0,
    "BEARISH_ACCELERATING": 15.0,
}


def _er_reset_depth_score(pct_from_52w_high: Optional[float]) -> Optional[float]:
    """
    Saturating reset-depth curve (Part 5). pct_from_52w_high is expected <= 0
    (negative = below the 52-week high). Depth = abs(pct_from_52w_high).

    Intentionally does NOT reward an extreme collapse more than a healthy
    meaningful reset — the curve rises through the "meaningful"/"deep" bands
    and then flattens/slightly recedes for extreme drawdowns, since very deep
    declines increasingly risk representing structural damage rather than a
    higher-quality reset (per spec: "do not reward an 85% collapse more than
    a healthy 35% reset merely because the number is larger").
    """
    if pct_from_52w_high is None:
        return None
    depth = abs(pct_from_52w_high)
    if depth < 10:
        score = (depth / 10.0) * 30.0                       # <10%: negligible reset
    elif depth < 20:
        score = 30.0 + (depth - 10.0) / 10.0 * 25.0          # 10-20%: mild
    elif depth < 35:
        score = 55.0 + (depth - 20.0) / 15.0 * 25.0          # 20-35%: meaningful
    elif depth < 60:
        score = 80.0 + (depth - 35.0) / 25.0 * 15.0          # 35-60%: deep
    else:
        score = 95.0 - min(30.0, (depth - 60.0) * 0.5)       # >60%: plateau/recede
    return round(_clamp(score, 0.0, 100.0), 1)


def _elite_reset_quality(entry_result: Optional[dict], stage2_row: Optional[dict]) -> dict:
    """
    ELITE_ASSET_REBOUND — Reset Quality (25% outer weight, Part 5).

    Uses ONLY existing Entry Structure V2 / Stage2 technical_metrics fields:
    pct_from_52w_high (drawdown), base_archetype, low_base_support_quality,
    floor_held_recently, floor_break_count, and entry_state (for hard
    structural-damage conflicts). No new indicators, no bar fetches.
    """
    tm = (stage2_row or {}).get("technical_metrics") or {}
    structure_v2 = (entry_result or {}).get("structure_v2") or {}
    entry_state  = (entry_result or {}).get("entry_state")

    pct_from_high = tm.get("pct_from_52w_high")
    depth_score = _er_reset_depth_score(pct_from_high)

    if depth_score is None:
        return {
            "elite_rebound_reset_quality_available": False,
            "elite_rebound_reset_quality_score":     None,
            "elite_rebound_reset_quality_components": {},
            "elite_rebound_reset_quality_reason_codes": ["NO_52W_DRAWDOWN_DATA"],
        }

    reason_codes: list[str] = []
    depth = abs(pct_from_high)
    if depth < 10:
        reason_codes.append("RESET_TOO_SHALLOW")
    elif depth < 20:
        reason_codes.append("MEANINGFUL_RESET")
    elif depth < 35:
        reason_codes.append("MEANINGFUL_RESET")
    elif depth < 60:
        reason_codes.append("DEEP_RESET")
    else:
        reason_codes.append("DEEP_RESET")

    components: dict[str, Any] = {
        "reset_depth": {"available": True, "score": depth_score, "pct_from_52w_high": pct_from_high},
    }
    num = _clamp01(1.0) * 0.0  # placeholder to keep structure explicit below
    num = 0.40 * depth_score
    den = 0.40

    # ── Support / floor quality component (30%) ─────────────────────────────
    base_archetype = structure_v2.get("base_archetype")
    support_quality_map = {"HIGH": 90.0, "MEDIUM": 60.0, "LOW": 30.0}
    support_score: Optional[float] = None
    if base_archetype == "LOW_BASE":
        low_base_support_quality = structure_v2.get("low_base_support_quality")
        support_score = support_quality_map.get(low_base_support_quality)
        if support_score is not None:
            if structure_v2.get("floor_held_recently"):
                support_score = min(100.0, support_score + 10.0)
                reason_codes.append("SUPPORT_RETAINED")
            floor_break_count = structure_v2.get("floor_break_count") or 0
            if floor_break_count and floor_break_count > 0:
                support_score = max(0.0, support_score - 15.0)
        components["support_quality"] = {
            "available": support_score is not None,
            "score": support_score,
            "base_archetype": base_archetype,
            "low_base_support_quality": low_base_support_quality,
        }
    else:
        components["support_quality"] = {"available": False, "score": None, "base_archetype": base_archetype}

    if support_score is not None:
        num += 0.30 * support_score
        den += 0.30

    # ── Trend-context / structural-damage component (30%) ───────────────────
    trend_score: Optional[float] = None
    if entry_state is not None:
        if entry_state in _ER_RESET_HARD_BREAK_STATES:
            trend_score = 12.0
            reason_codes.append("STRUCTURAL_DAMAGE")
            reason_codes.append(entry_state)
        elif entry_state == "FAILED_BREAKOUT" and structure_v2.get("failed_breakout_confirmed"):
            trend_score = 15.0
            reason_codes.append("FAILED_BREAKOUT")
        elif entry_state in _ER_RESET_FAVORABLE_STATES:
            trend_score = 75.0
            reason_codes.append("DOWNSIDE_DEFINED")
        else:
            trend_score = 45.0
        components["trend_context"] = {"available": True, "score": trend_score, "entry_state": entry_state}
    else:
        components["trend_context"] = {"available": False, "score": None, "entry_state": None}

    if trend_score is not None:
        num += 0.30 * trend_score
        den += 0.30

    reset_quality_score = round(num / den, 1) if den > 0 else None

    hard_conflict = (
        entry_state in _ER_RESET_HARD_BREAK_STATES or
        (entry_state == "FAILED_BREAKOUT" and structure_v2.get("failed_breakout_confirmed"))
    )
    if hard_conflict:
        # Materially reduce, never boost, on confirmed structural damage —
        # a falling knife is never classified as a high-quality reset.
        reset_quality_score = min(reset_quality_score, 35.0) if reset_quality_score is not None else None

    if depth >= 20 and not hard_conflict:
        reason_codes.append("ASYMMETRY_IMPROVED")
    if depth >= 15 and entry_state in _ER_RESET_FAVORABLE_STATES:
        reason_codes.append("RESET_FROM_PRIOR_LEADERSHIP")

    return {
        "elite_rebound_reset_quality_available": reset_quality_score is not None,
        "elite_rebound_reset_quality_score":     reset_quality_score,
        "elite_rebound_reset_quality_components": components,
        "elite_rebound_reset_quality_reason_codes": sorted(set(reason_codes)),
    }


def _elite_rebound_evidence(entry_result: Optional[dict]) -> dict:
    """
    ELITE_ASSET_REBOUND — Rebound Evidence (25% outer weight, Part 6).

    Uses ONLY the existing Entry Structure state/family — no new indicators.
    Entry Score asks "good place now?"; Rebound Evidence asks "is the reset
    beginning to turn?" — these are deliberately NOT equated.
    """
    entry_state = (entry_result or {}).get("entry_state")
    if entry_state is None:
        return {
            "elite_rebound_evidence_available": False,
            "elite_rebound_evidence_score":     None,
            "elite_rebound_evidence_components": {},
            "elite_rebound_evidence_reason_codes": ["ENTRY_STATE_UNAVAILABLE"],
        }

    score = _ER_REBOUND_EVIDENCE_MAP.get(entry_state, 30.0)
    reason_codes = [f"ENTRY_STATE_{entry_state}"]
    if entry_state in _ER_REBOUND_EVIDENCE_POSITIVE_STATES:
        reason_codes.append("REBOUND_EVIDENCE_POSITIVE")
    elif entry_state in _ER_REBOUND_EVIDENCE_WEAK_STATES:
        reason_codes.append("REBOUND_EVIDENCE_WEAK")
    elif entry_state in _ER_REBOUND_EVIDENCE_NEGATIVE_STATES:
        reason_codes.append("REBOUND_EVIDENCE_NEGATIVE")

    return {
        "elite_rebound_evidence_available": True,
        "elite_rebound_evidence_score":     score,
        "elite_rebound_evidence_components": {"entry_state": entry_state, "mapped_score": score},
        "elite_rebound_evidence_reason_codes": reason_codes,
    }


def _elite_options_improvement(options_result: Optional[dict]) -> dict:
    """
    ELITE_ASSET_REBOUND — Options Pressure Improvement (10% outer weight,
    Part 7). Uses ONLY existing Options Alignment V2 outputs (pressure state
    derived from premium deltas) — never raw Net Premium alone, never a
    fabricated zero for non-optionable names.
    """
    options_result = options_result or {}
    available = bool(options_result.get("options_signal_available"))
    pressure_state = options_result.get("options_pressure_state")

    if not available or pressure_state is None or pressure_state == "INSUFFICIENT_HISTORY":
        return {
            "elite_rebound_options_improvement_available": False,
            "elite_rebound_options_improvement_score":     None,
            "elite_rebound_options_reason_codes":          ["OPTIONS_IMPROVEMENT_UNAVAILABLE"],
        }

    score = _ER_OPTIONS_IMPROVEMENT_MAP.get(pressure_state, 50.0)
    reason_map = {
        "BULLISH_ACCELERATING": "BULLISH_PRESSURE_ACCELERATING",
        "BEARISH_EASING":       "BEARISH_PRESSURE_EASING",
        "BULLISH_FADING":       "BULLISH_PRESSURE_FADING",
        "BEARISH_ACCELERATING": "BEARISH_PRESSURE_ACCELERATING",
        "MIXED":                "OPTIONS_DIRECTION_MIXED",
    }
    reason_codes = [reason_map.get(pressure_state, pressure_state)]

    return {
        "elite_rebound_options_improvement_available": True,
        "elite_rebound_options_improvement_score":     score,
        "elite_rebound_options_reason_codes":          reason_codes,
    }


def _elite_asset_gate(investment_alignment_fields: dict) -> tuple[bool, bool, list[str]]:
    """
    Part 3 — Elite Asset Gate + thesis-integrity gate.

    Returns (elite_gate_passed, thesis_integrity_passed, reason_codes).
    """
    reason_codes: list[str] = []
    ia_available = bool(investment_alignment_fields.get("investment_alignment_available"))
    ia_score     = investment_alignment_fields.get("investment_alignment_score")

    if not ia_available or ia_score is None:
        return False, False, ["INVESTMENT_ALIGNMENT_UNAVAILABLE"]

    elite_gate = ia_score >= _ER_GATE_INVESTMENT_MIN
    if elite_gate:
        reason_codes.append("ELITE_INVESTMENT_ALIGNMENT" if ia_score >= 85 else "STRONG_INVESTMENT_ALIGNMENT")
    else:
        return False, False, ["INVESTMENT_ALIGNMENT_BELOW_THRESHOLD"]

    components = investment_alignment_fields.get("investment_alignment_components") or {}
    fa = components.get("financial_acceleration") or {}
    fe = components.get("forward_expectations") or {}

    fa_available = bool(fa.get("available"))
    fa_score     = fa.get("score")
    fe_available = bool(fe.get("available"))
    fe_score     = fe.get("score")

    thesis_ok = fa_available and fa_score is not None and fa_score >= _ER_THESIS_FIN_ACCEL_MIN
    if thesis_ok:
        reason_codes.append("FINANCIAL_TRAJECTORY_INTACT")
    else:
        reason_codes.append("FINANCIAL_TRAJECTORY_DAMAGED")

    if thesis_ok and fe_available and fe_score is not None:
        if fe_score >= _ER_THESIS_FWD_EXPECT_MIN:
            reason_codes.append("FORWARD_EXPECTATIONS_INTACT")
        else:
            reason_codes.append("FORWARD_EXPECTATIONS_DAMAGED")
            thesis_ok = False
    elif thesis_ok:
        reason_codes.append("FORWARD_EXPECTATIONS_UNAVAILABLE")

    return elite_gate, thesis_ok, reason_codes


def _elite_prior_leadership_diagnostic(investment_alignment_fields: dict) -> list[str]:
    """
    Part 4 — prior-leadership / asset-confirmation diagnostic. Uses the
    EXISTING Investment Alignment Long-Term Leadership component only.
    Stage 2 is explicitly NOT mandatory (a reset candidate may temporarily
    lose Stage 2) — this is informational, never a blocking gate.
    """
    components = investment_alignment_fields.get("investment_alignment_components") or {}
    ll = components.get("long_term_leadership") or {}
    if not ll.get("available") or ll.get("score") is None:
        return ["LONG_TERM_LEADERSHIP_UNAVAILABLE"]
    if ll["score"] >= _ER_PRIOR_LEADERSHIP_MIN:
        return ["PRIOR_LEADERSHIP_CONFIRMED"]
    return ["PRIOR_LEADERSHIP_WEAK"]


def _elite_asset_rebound_unavailable(reason_codes: list[str]) -> dict:
    return {
        "elite_asset_rebound_available":  False,
        "elite_asset_rebound_score":      None,
        "elite_asset_rebound_version":    ELITE_ASSET_REBOUND_VERSION,
        "elite_asset_rebound_components": {},
        "elite_asset_rebound_reason_codes": reason_codes,
        "elite_asset_rebound_strengths":  [],
        "elite_asset_rebound_conflicts":  [],
        "elite_rebound_reset_quality_available": False,
        "elite_rebound_reset_quality_score":     None,
        "elite_rebound_reset_quality_components": {},
        "elite_rebound_reset_quality_reason_codes": [],
        "elite_rebound_evidence_available": False,
        "elite_rebound_evidence_score":     None,
        "elite_rebound_evidence_components": {},
        "elite_rebound_evidence_reason_codes": [],
        "elite_rebound_options_improvement_available": False,
        "elite_rebound_options_improvement_score":     None,
        "elite_rebound_options_reason_codes":          [],
    }


def _compute_elite_asset_rebound(
    sym: str,
    investment_alignment_fields: dict,
    entry_result: Optional[dict],
    stage2_row: Optional[dict],
    options_result: Optional[dict],
) -> dict:
    """
    ELITE_ASSET_REBOUND V1 — full archetype (Parts 2-9). SHADOW/additive.
    """
    elite_gate, thesis_ok, gate_reasons = _elite_asset_gate(investment_alignment_fields)

    if not elite_gate:
        return _elite_asset_rebound_unavailable(gate_reasons)
    if not thesis_ok:
        return _elite_asset_rebound_unavailable(gate_reasons + ["THESIS_INTEGRITY_FAILED"])

    prior_leadership_codes = _elite_prior_leadership_diagnostic(investment_alignment_fields)

    reset = _elite_reset_quality(entry_result, stage2_row)
    evidence = _elite_rebound_evidence(entry_result)
    options_improve = _elite_options_improvement(options_result)

    if not reset["elite_rebound_reset_quality_available"]:
        return _elite_asset_rebound_unavailable(
            gate_reasons + prior_leadership_codes + ["RESET_QUALITY_UNAVAILABLE"]
        )
    if not evidence["elite_rebound_evidence_available"]:
        return _elite_asset_rebound_unavailable(
            gate_reasons + prior_leadership_codes + ["REBOUND_EVIDENCE_UNAVAILABLE"]
        )

    ia_score    = investment_alignment_fields.get("investment_alignment_score")
    reset_score = reset["elite_rebound_reset_quality_score"]
    evid_score  = evidence["elite_rebound_evidence_score"]
    opts_score  = options_improve["elite_rebound_options_improvement_score"]
    opts_avail  = options_improve["elite_rebound_options_improvement_available"]

    num = _ER_W_INVESTMENT * ia_score + _ER_W_RESET * reset_score + _ER_W_EVIDENCE * evid_score
    den = _ER_W_INVESTMENT + _ER_W_RESET + _ER_W_EVIDENCE
    if opts_avail and opts_score is not None:
        num += _ER_W_OPTIONS * opts_score
        den += _ER_W_OPTIONS

    final_score = round(_clamp(num / den, 0.0, 100.0), 1)

    strengths: list[str] = [c for c in gate_reasons if c in (
        "ELITE_INVESTMENT_ALIGNMENT", "STRONG_INVESTMENT_ALIGNMENT",
        "FINANCIAL_TRAJECTORY_INTACT", "FORWARD_EXPECTATIONS_INTACT",
    )]
    if "PRIOR_LEADERSHIP_CONFIRMED" in prior_leadership_codes:
        strengths.append("PRIOR_LEADERSHIP_CONFIRMED")
    strengths.extend(c for c in reset["elite_rebound_reset_quality_reason_codes"]
                      if c in ("MEANINGFUL_RESET", "DEEP_RESET", "ASYMMETRY_IMPROVED",
                               "RESET_FROM_PRIOR_LEADERSHIP", "SUPPORT_RETAINED"))
    strengths.extend(c for c in evidence["elite_rebound_evidence_reason_codes"]
                      if c == "REBOUND_EVIDENCE_POSITIVE")
    if opts_avail:
        strengths.extend(c for c in options_improve["elite_rebound_options_reason_codes"]
                          if c in ("BULLISH_PRESSURE_ACCELERATING", "BEARISH_PRESSURE_EASING"))

    conflicts: list[str] = [c for c in reset["elite_rebound_reset_quality_reason_codes"]
                             if c in ("STRUCTURAL_DAMAGE", "SUPPORT_LOST", "DOWNTREND",
                                      "FAILED_BREAKOUT", "RESET_TOO_SHALLOW")]
    conflicts.extend(c for c in evidence["elite_rebound_evidence_reason_codes"]
                      if c == "REBOUND_EVIDENCE_NEGATIVE")
    if opts_avail:
        conflicts.extend(c for c in options_improve["elite_rebound_options_reason_codes"]
                          if c in ("BEARISH_PRESSURE_ACCELERATING",))

    reason_codes = list(dict.fromkeys(
        gate_reasons + prior_leadership_codes +
        reset["elite_rebound_reset_quality_reason_codes"] +
        evidence["elite_rebound_evidence_reason_codes"] +
        (options_improve["elite_rebound_options_reason_codes"] if opts_avail else [])
    ))

    components = {
        "investment_alignment": {"available": True, "score": ia_score, "weight_pct": _ER_W_INVESTMENT},
        "reset_quality":        {"available": True, "score": reset_score, "weight_pct": _ER_W_RESET,
                                  "detail": reset["elite_rebound_reset_quality_components"]},
        "rebound_evidence":     {"available": True, "score": evid_score, "weight_pct": _ER_W_EVIDENCE,
                                  "detail": evidence["elite_rebound_evidence_components"]},
        "options_pressure_improvement": {"available": opts_avail, "score": opts_score,
                                          "weight_pct": _ER_W_OPTIONS if opts_avail else 0.0},
    }

    return {
        "elite_asset_rebound_available":  True,
        "elite_asset_rebound_score":      final_score,
        "elite_asset_rebound_version":    ELITE_ASSET_REBOUND_VERSION,
        "elite_asset_rebound_components": components,
        "elite_asset_rebound_reason_codes": reason_codes,
        "elite_asset_rebound_strengths":  strengths,
        "elite_asset_rebound_conflicts":  conflicts,
        "elite_rebound_reset_quality_available": True,
        "elite_rebound_reset_quality_score":     reset_score,
        "elite_rebound_reset_quality_components": reset["elite_rebound_reset_quality_components"],
        "elite_rebound_reset_quality_reason_codes": reset["elite_rebound_reset_quality_reason_codes"],
        "elite_rebound_evidence_available": True,
        "elite_rebound_evidence_score":     evid_score,
        "elite_rebound_evidence_components": evidence["elite_rebound_evidence_components"],
        "elite_rebound_evidence_reason_codes": evidence["elite_rebound_evidence_reason_codes"],
        "elite_rebound_options_improvement_available": opts_avail,
        "elite_rebound_options_improvement_score":     opts_score,
        "elite_rebound_options_reason_codes":          options_improve["elite_rebound_options_reason_codes"],
    }


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _grade(score: float) -> str:
    if score >= 85:
        return "A+"
    if score >= 70:
        return "A"
    if score >= 55:
        return "B"
    if score >= 35:
        return "C"
    return "AVOID"


def _verdict(score: float) -> str:
    for threshold, label in _VERDICTS:
        if score >= threshold:
            return label
    return "SHORT_AVOID"


# ── Loaders ────────────────────────────────────────────────────────────────────

def _load_stage2_lkg() -> dict[str, dict]:
    """Load stage2 LKG keyed by symbol.

    The on-disk format is:
        {"updated_at": "...", "symbol_count": N, "results": {"SYM": {...}, ...}}

    Must read from the "results" sub-dict, NOT from the top-level dict — otherwise
    the key "results" itself would be treated as a fake ticker symbol.

    Canonical field names per symbol entry:
        "score"  (0–100 float) — stage quality score
        "label"  (str)         — stage label e.g. "S2-S3 Advance"
    """
    try:
        if _STAGE2_LKG.exists():
            raw = json.loads(_STAGE2_LKG.read_text())
            if isinstance(raw, dict):
                entries = raw.get("results", {})
                if isinstance(entries, dict):
                    return {k.upper(): v for k, v in entries.items() if isinstance(v, dict)}
    except Exception:
        pass
    return {}


def _load_themes_rs_index() -> dict[str, dict]:
    """
    DEPRECATED / DEAD CODE PATH — kept only for backward-compat callers, if
    any exist. This was the ROOT CAUSE of 0/377 Theme coverage: `leaders` in
    themes_rs_lkg.json is a list of dicts ({"symbol": ..., "return_pct": ...})
    not a list of strings, so `sym.upper()` raised AttributeError on the very
    first row, was swallowed by the broad except below, and the function
    returned an empty dict for every ticker on every call.

    Theme signal computation no longer calls this function — see
    `services.theme_bridge` (canonical membership) +
    `services.theme_rotation_service.build_theme_rotation_snapshot()`
    (existing Theme Rotation engine) instead.
    """
    result: dict[str, dict] = {}
    try:
        if _THEMES_RS_LKG.exists():
            raw = json.loads(_THEMES_RS_LKG.read_text())
            rows = raw.get("rows") if isinstance(raw, dict) else raw
            if not isinstance(rows, list):
                return result
            for row in rows:
                raw_leaders  = row.get("leaders") or []
                leaders      = [
                    (e.get("symbol") if isinstance(e, dict) else e)
                    for e in raw_leaders
                    if e and (isinstance(e, str) or (isinstance(e, dict) and e.get("symbol")))
                ]
                proxy_syms   = row.get("proxy_symbols") or []
                for sym in leaders + proxy_syms:
                    if not sym:
                        continue
                    key = sym.upper()
                    if key not in result:
                        result[key] = row
    except Exception:
        pass
    return result


def _load_social_map() -> dict[str, dict]:
    """Return {TICKER: social_entry} with backend_score etc."""
    try:
        if _X_CONSENSUS.exists():
            raw = json.loads(_X_CONSENSUS.read_text())
            ranked = raw.get("_backend_ranked") or []
            return {e["ticker"].upper(): e for e in ranked if e.get("ticker")}
    except Exception:
        pass
    return {}


def _load_options_map() -> dict[str, dict]:
    """Return {TICKER: options_row} with composite_score, side_bias."""
    try:
        if _OPTIONS_LKG.exists():
            raw = json.loads(_OPTIONS_LKG.read_text())
            tickers = raw.get("tickers") or []
            return {t["ticker"].upper(): t for t in tickers if t.get("ticker")}
    except Exception:
        pass
    return {}


# ── Per-signal normalizers ─────────────────────────────────────────────────────

def _entry_signal(entry_result: Optional[dict]) -> tuple[float, str]:
    """Return (0–1 signal, state_label)."""
    if entry_result is None:
        return 0.5, "MISSING"
    score = _safe_float(entry_result.get("entry_score"), 50.0)
    return _clamp01(score / 100.0), entry_result.get("entry_state", "UNKNOWN")


def _theme_signal_v2(
    sym: str,
    ticker_theme_idx: dict[str, list[str]],
    rotation_idx: dict[str, dict],
) -> dict:
    """
    Theme signal via the canonical Theme Bridge (services.theme_bridge):
    ticker → canonical Theme membership (theme_merge_layer.ENRICHED_THEME_RS_UNIVERSE,
    manual overrides included) → EXISTING Theme Rotation result
    (theme_rotation_service.build_theme_rotation_snapshot()).

    Returns the full bridge dict (canonical_theme_memberships,
    theme_rotation_memberships, primary_rotation_theme,
    primary_theme_rotation_score/state/direction, theme_signal_available,
    theme_signal_reason).

    IMPORTANT: never returns a fabricated neutral 0.5 as a PRESENT signal.
    theme_signal_available=False means the caller must omit the Theme weight
    from the denominator, not substitute a fallback score.
    """
    from services.theme_bridge import get_ticker_rotation_bridge
    return get_ticker_rotation_bridge(sym, ticker_theme_idx, rotation_idx)


def _theme_sig_0_1(bridge: dict) -> float:
    """
    Normalize primary_theme_rotation_score to a 0-1 signal for use ONLY when
    theme_signal_available is True. rotation_score from theme_rotation_service
    is already produced as a weighted sum of 0-1 sub-signals (see
    theme_rotation_service._classify_phase / build_theme_rotation_snapshot),
    so its native scale is already 0-1 — verified by inspecting
    theme_rotation_service.py directly, not guessed. No /100 division needed.
    """
    score = bridge.get("primary_theme_rotation_score")
    if score is None:
        return 0.5
    return _clamp01(float(score))


def _stage_signal_from_lkg(stage2_row: Optional[dict]) -> tuple[float, str]:
    """Return (0–1 stage quality signal, stage_label) from stage2 LKG row.

    FIX (was reading wrong field names):
      Canonical Stage2 LKG fields are "score" (0–100) and "label" (str).
      Previously read "stage_score" / "stage_label" / "stage" — all returned None.
    """
    if stage2_row is None:
        return 0.5, "MISSING"

    # ── Correct canonical field names ─────────────────────────────────────────
    score = _safe_float(stage2_row.get("score"), 50.0)   # was: "stage_score" (bug)
    label = stage2_row.get("label") or ""                 # was: "stage_label" (bug)

    # Derive stage integer from label for cap logic (no "stage" int field in LKG)
    stage_int = _LABEL_TO_STAGE_INT.get(label, 2)

    # Normalise score → signal (0–1)
    sig = _clamp01(score / 100.0)

    # Hard caps by stage
    if stage_int == 4:
        sig = min(sig, 0.10)
    elif stage_int == 3 and "momentum" not in label.lower():
        sig = min(sig, 0.35)

    return sig, label if label else "UNKNOWN"


def _options_signal(sym: str, options_map: dict[str, dict]) -> tuple[float, str]:
    """Return (0–1 options signal, side_bias)."""
    row = options_map.get(sym)
    if row is None:
        return 0.5, "NO_DATA"
    bias = row.get("side_bias") or "neutral"
    comp = _safe_float(row.get("composite_score") or row.get("final_composite_score"), 50.0)
    sig  = _clamp01(comp / 100.0)
    if bias == "bearish":
        sig = _clamp01(1.0 - sig)
    return sig, bias


# ── Social bonus (conditional additive, max +10 pts) ──────────────────────────

def _compute_social_bonus(
    entry_score_raw: int,
    entry_grade:     str,
    entry_family:    str,
    theme_sig_available: bool,
    t_sig:           float,
    s_sig:           float,
    o_sig:           float,
    social_entry:    Optional[dict],
) -> tuple[float, bool, str, bool]:
    """
    Compute conditional social bonus (0–10 pts).

    Returns:
        (bonus_score, eligible, reason, risk_flag)

    CORE PRINCIPLE:
        Social CONFIRMS a strong setup — it does NOT define one.
        No social coverage = 0 bonus, 0 penalty.
        Quiet/weak social = 0 bonus.
        Strong fresh social alignment may boost an already-aligned setup.

    Eligibility gate:
        entry_score >= 60 AND theme_sig >= 0.55
        AND (stage_sig >= 0.60 OR options_sig >= 0.65)

    Blocking gate (overrides eligibility):
        entry_grade == AVOID OR entry_family in CHASE_EXHAUSTION/BROKEN_OR_UNCLEAR

    Social fields (from x_consensus_weekly.json _backend_ranked):
        backend_score     — composite social signal (empirical range 0–14)
        has_top_conviction — bool: top-tier account coverage
        breadth_score     — account breadth multiplier (1.0–1.5)
        raw_score         — raw mention-weighted score

    Bonus tiers (max +10):
        VERY_STRONG (+8–10): backend_score >= 9, has_top_conviction
        STRONG      (+5–7):  backend_score >= 5, (top_conviction optional)
        MODERATE    (+2–4):  backend_score >= 3
        WEAK/ABSENT (0):     backend_score < 3 or no coverage
    """
    # ── No social coverage → zero bonus, zero penalty ─────────────────────────
    if social_entry is None:
        return 0.0, False, "no_social_coverage", False

    backend_score = _safe_float(social_entry.get("backend_score"), 0.0)
    has_top       = bool(social_entry.get("has_top_conviction"))
    breadth       = _safe_float(social_entry.get("breadth_score"), 1.0)

    # ── Blocking gate: bad entry overrides everything ─────────────────────────
    if (entry_grade == "AVOID" or
            entry_family in ("CHASE_EXHAUSTION", "BROKEN_OR_UNCLEAR")):
        return 0.0, False, "bad_entry_blocked", False

    # ── Eligibility gate ───────────────────────────────────────────────────────
    # Theme signal now comes from the real canonical Theme Bridge (primary
    # rotation theme's rotation_score). When no canonical membership /
    # rotation result exists (theme_sig_available=False), we do NOT reward the
    # missing data with a neutral pass — fall back to the stage_sig proxy only.
    theme_or_stage_ok = (theme_sig_available and t_sig >= 0.55) or s_sig >= 0.65
    eligible = (
        entry_score_raw >= 60 and
        theme_or_stage_ok and
        (s_sig >= 0.60 or o_sig >= 0.65)
    )
    if not eligible:
        return 0.0, False, "setup_not_aligned", False

    # ── Social risk flag (bearish consensus) ───────────────────────────────────
    # x_consensus tracks bullish mentions only; no bearish indicator available.
    risk_flag = False

    # ── Weak social → zero bonus ───────────────────────────────────────────────
    if backend_score < 3.0:
        return 0.0, True, "weak_social", risk_flag

    # ── Bonus tiers ────────────────────────────────────────────────────────────
    # VERY_STRONG (8–10): strong score + top conviction
    if backend_score >= 12.0 and has_top and breadth >= 1.3:
        bonus = 10.0
        reason = "VERY_STRONG_FRESH"
    elif backend_score >= 9.0 and has_top:
        bonus = min(10.0, 8.0 + (backend_score - 9.0) * 0.667)  # 8–10
        reason = "VERY_STRONG"
    # STRONG (5–7): meaningful score, may have top conviction
    elif backend_score >= 6.0 and has_top:
        bonus = min(7.0, 5.0 + (backend_score - 6.0) * 0.667)   # 5–7
        reason = "STRONG_WITH_CONVICTION"
    elif backend_score >= 5.0:
        bonus = 5.0
        reason = "STRONG"
    # MODERATE (2–4): moderate positive confirmation
    elif backend_score >= 4.0:
        bonus = 4.0
        reason = "MODERATE_HIGH"
    elif backend_score >= 3.0:
        bonus = 2.0 + (backend_score - 3.0)                       # 2–3
        reason = "MODERATE"
    else:
        bonus = 0.0
        reason = "WEAK"

    return round(bonus, 1), True, reason, risk_flag


# ── THEME_ALIGNMENT social bonus (re-gated to base_trade_alignment_score) ─────

def _compute_social_bonus_ta(
    base_ta_score: Optional[float],
    entry_grade:   str,
    entry_family:  str,
    social_entry:  Optional[dict],
) -> tuple[float, bool, str, bool]:
    """
    Social Bonus for the THEME_ALIGNMENT archetype — identical tiers/backend
    to `_compute_social_bonus`, but eligibility is re-gated to the NEW
    base_trade_alignment_score (25/25/25/25 Theme/Stage/Options/Catalyst)
    instead of the legacy Entry-weighted base score.

    Per spec:
      - Entry Score must NOT be a prerequisite for Social Bonus eligibility.
      - Social must not create Trade Alignment eligibility (min-evidence gate
        is computed BEFORE this function is even called).
      - A sufficiently strong non-social Trade Alignment is still required
        (base_ta_score >= 60), so Social only ever CONFIRMS an already
        aligned setup — it never manufactures one.
      - The AVOID / CHASE_EXHAUSTION / BROKEN_OR_UNCLEAR blocking gate is
        preserved as a safety filter (blocking, not a prerequisite).
    """
    if social_entry is None:
        return 0.0, False, "no_social_coverage", False
    if base_ta_score is None:
        return 0.0, False, "trade_alignment_unavailable", False

    backend_score = _safe_float(social_entry.get("backend_score"), 0.0)
    has_top       = bool(social_entry.get("has_top_conviction"))
    breadth       = _safe_float(social_entry.get("breadth_score"), 1.0)

    if (entry_grade == "AVOID" or
            entry_family in ("CHASE_EXHAUSTION", "BROKEN_OR_UNCLEAR")):
        return 0.0, False, "bad_entry_blocked", False

    eligible = base_ta_score >= 60.0
    if not eligible:
        return 0.0, False, "trade_alignment_not_aligned", False

    risk_flag = False

    if backend_score < 3.0:
        return 0.0, True, "weak_social", risk_flag

    if backend_score >= 12.0 and has_top and breadth >= 1.3:
        bonus = 10.0
        reason = "VERY_STRONG_FRESH"
    elif backend_score >= 9.0 and has_top:
        bonus = min(10.0, 8.0 + (backend_score - 9.0) * 0.667)
        reason = "VERY_STRONG"
    elif backend_score >= 6.0 and has_top:
        bonus = min(7.0, 5.0 + (backend_score - 6.0) * 0.667)
        reason = "STRONG_WITH_CONVICTION"
    elif backend_score >= 5.0:
        bonus = 5.0
        reason = "STRONG"
    elif backend_score >= 4.0:
        bonus = 4.0
        reason = "MODERATE_HIGH"
    elif backend_score >= 3.0:
        bonus = 2.0 + (backend_score - 3.0)
        reason = "MODERATE"
    else:
        bonus = 0.0
        reason = "WEAK"

    return round(bonus, 1), True, reason, risk_flag


def _compute_theme_alignment(
    sym:              str,
    theme_align:      dict,
    s_sig:            float,
    s_available:      bool,
    options_result:   dict,
    catalyst_result:  dict,
    entry_grade_raw:  str,
    entry_family:     str,
    social_entry:     Optional[dict],
) -> dict:
    """
    THEME_ALIGNMENT Trade Alignment archetype (SHADOW / additive fields only).

    25/25/25/25 Theme/Stage/Options/Catalyst weights with available-signal
    renormalization. Minimum-evidence gate: Theme MUST be present AND at
    least one of Stage/Options/Catalyst MUST be present (>= 2 of 4 signals,
    Theme always one of them). Zero provider calls (all inputs are already
    cache-derived by the caller).
    """
    theme_available = bool(theme_align.get("theme_signal_available"))
    theme_score_0_100: Optional[float] = None
    if theme_available:
        raw = theme_align.get("primary_theme_rotation_score")
        if raw is not None:
            theme_score_0_100 = _clamp01(float(raw)) * 100.0

    stage_score_0_100 = round(s_sig * 100.0, 1) if s_available else None

    options_available = bool(options_result.get("options_signal_available"))
    options_score_0_100 = options_result.get("options_alignment_score") if options_available else None

    catalyst_available = bool(catalyst_result.get("catalyst_alignment_available"))
    catalyst_score_0_100 = catalyst_result.get("catalyst_alignment_score") if catalyst_available else None

    # ── Minimum evidence gate ────────────────────────────────────────────────
    non_theme_present = sum([
        s_available and stage_score_0_100 is not None,
        options_available and options_score_0_100 is not None,
        catalyst_available and catalyst_score_0_100 is not None,
    ])
    signal_count = (1 if (theme_available and theme_score_0_100 is not None) else 0) + non_theme_present

    if not (theme_available and theme_score_0_100 is not None):
        reason = "THEME_SIGNAL_UNAVAILABLE"
        return _theme_alignment_unavailable(sym, theme_align, stage_score_0_100, s_available,
                                             options_result, catalyst_result, reason)
    if non_theme_present < 1:
        reason = "INSUFFICIENT_ALIGNMENT_EVIDENCE"
        return _theme_alignment_unavailable(sym, theme_align, stage_score_0_100, s_available,
                                             options_result, catalyst_result, reason)

    # ── Available-signal weighted base (25/25/25/25, renormalized) ──────────
    num = _TA_W_THEME * theme_score_0_100
    den = _TA_W_THEME
    if stage_score_0_100 is not None:
        num += _TA_W_STAGE * stage_score_0_100
        den += _TA_W_STAGE
    if options_score_0_100 is not None:
        num += _TA_W_OPTIONS * options_score_0_100
        den += _TA_W_OPTIONS
    if catalyst_score_0_100 is not None:
        num += _TA_W_CATALYST * catalyst_score_0_100
        den += _TA_W_CATALYST

    base_ta_score = round(num / den, 1) if den > 0 else None

    # ── Social bonus (re-gated to base_trade_alignment_score) ───────────────
    social_bonus, soc_eligible, soc_reason, soc_risk = _compute_social_bonus_ta(
        base_ta_score = base_ta_score,
        entry_grade   = entry_grade_raw,
        entry_family  = entry_family,
        social_entry  = social_entry,
    )

    trade_alignment_score = round(_clamp(base_ta_score + social_bonus, 0.0, 100.0), 1)

    reason_codes = ["MINIMUM_EVIDENCE_MET"]
    if not s_available:
        reason_codes.append("STAGE_UNAVAILABLE")
    if not options_available or options_score_0_100 is None:
        reason_codes.append("OPTIONS_UNAVAILABLE")
    if not catalyst_available:
        reason_codes.append("CATALYST_UNAVAILABLE")

    return {
        "trade_alignment_available":     True,
        "trade_alignment_archetype":     "THEME_ALIGNMENT",
        "base_trade_alignment_score":    base_ta_score,
        "social_bonus_score":            social_bonus,
        "social_bonus_eligible":         soc_eligible,
        "social_bonus_reason":           soc_reason,
        "social_risk_flag":              soc_risk,
        "trade_alignment_score":         trade_alignment_score,
        "trade_alignment_grade":         _ta_grade(trade_alignment_score),
        "trade_alignment_signal_count":  signal_count,
        "trade_alignment_reason_codes":  reason_codes,
        "theme_alignment_score":         round(theme_score_0_100, 1),
        "theme_alignment_available":     True,
        "stage_alignment_score":         stage_score_0_100,
        "stage_alignment_available":     s_available,
        "options_alignment_score":       options_score_0_100,
        "options_alignment_available":   options_available and options_score_0_100 is not None,
        "options_pressure_state":        options_result.get("options_pressure_state"),
        "options_direction_available":   bool(options_result.get("options_direction_available")),
        "catalyst_alignment_score":      catalyst_score_0_100,
        "catalyst_alignment_available":  catalyst_available,
        "primary_catalyst":              catalyst_result.get("primary_catalyst"),
        "catalyst_events":               catalyst_result.get("catalyst_events") or [],
        # ── V2.1-promoted provenance fields (display/explainability only) ─
        "catalyst_model_version":        catalyst_result.get("catalyst_model_version"),
        "catalyst_primary_source":       catalyst_result.get("catalyst_primary_source"),
        "catalyst_primary_event":        catalyst_result.get("catalyst_primary_event"),
        "catalyst_scheduled_event":      catalyst_result.get("catalyst_scheduled_event"),
        "catalyst_rss_event":            catalyst_result.get("catalyst_rss_event"),
        "catalyst_bearish_conflict":     catalyst_result.get("catalyst_bearish_conflict"),
        # ── V2 additive shadow fields (do not affect any scoring) ─────────
        "catalyst_v2_available":         bool(catalyst_result.get("catalyst_v2_available")),
        "catalyst_v2_score":             catalyst_result.get("catalyst_v2_score"),
        "catalyst_v2_state":             catalyst_result.get("catalyst_v2_state") or "UNAVAILABLE",
        "catalyst_v2_primary_event":     catalyst_result.get("catalyst_v2_primary_event"),
        "catalyst_v2_conflicts":         catalyst_result.get("catalyst_v2_conflicts") or [],
    }


def _theme_alignment_unavailable(
    sym: str,
    theme_align: dict,
    stage_score_0_100: Optional[float],
    s_available: bool,
    options_result: dict,
    catalyst_result: dict,
    reason: str,
) -> dict:
    options_available = bool(options_result.get("options_signal_available"))
    options_score_0_100 = options_result.get("options_alignment_score") if options_available else None
    catalyst_available = bool(catalyst_result.get("catalyst_alignment_available"))
    catalyst_score_0_100 = catalyst_result.get("catalyst_alignment_score") if catalyst_available else None
    theme_available = bool(theme_align.get("theme_signal_available"))
    theme_score = theme_align.get("primary_theme_rotation_score")
    theme_score_0_100 = round(_clamp01(float(theme_score)) * 100.0, 1) if (theme_available and theme_score is not None) else None

    return {
        "trade_alignment_available":     False,
        "trade_alignment_archetype":     None,
        "base_trade_alignment_score":    None,
        "social_bonus_score":            None,
        "social_bonus_eligible":         False,
        "social_bonus_reason":           "trade_alignment_unavailable",
        "social_risk_flag":              False,
        "trade_alignment_score":         None,
        "trade_alignment_grade":         None,
        "trade_alignment_signal_count":  sum([
            theme_available and theme_score_0_100 is not None,
            s_available and stage_score_0_100 is not None,
            options_available and options_score_0_100 is not None,
            catalyst_available and catalyst_score_0_100 is not None,
        ]),
        "trade_alignment_reason_codes":  [reason],
        "theme_alignment_score":         theme_score_0_100,
        "theme_alignment_available":     theme_available and theme_score_0_100 is not None,
        "stage_alignment_score":         stage_score_0_100,
        "stage_alignment_available":     s_available,
        "options_alignment_score":       options_score_0_100,
        "options_alignment_available":   options_available and options_score_0_100 is not None,
        "options_pressure_state":        options_result.get("options_pressure_state"),
        "options_direction_available":   bool(options_result.get("options_direction_available")),
        "catalyst_alignment_score":      catalyst_score_0_100,
        "catalyst_alignment_available":  catalyst_available,
        "primary_catalyst":              catalyst_result.get("primary_catalyst"),
        "catalyst_events":               catalyst_result.get("catalyst_events") or [],
        # ── V2.1-promoted provenance fields (display/explainability only) ─
        "catalyst_model_version":        catalyst_result.get("catalyst_model_version"),
        "catalyst_primary_source":       catalyst_result.get("catalyst_primary_source"),
        "catalyst_primary_event":        catalyst_result.get("catalyst_primary_event"),
        "catalyst_scheduled_event":      catalyst_result.get("catalyst_scheduled_event"),
        "catalyst_rss_event":            catalyst_result.get("catalyst_rss_event"),
        "catalyst_bearish_conflict":     catalyst_result.get("catalyst_bearish_conflict"),
        # ── V2 additive shadow fields (do not affect any scoring) ─────────
        "catalyst_v2_available":         bool(catalyst_result.get("catalyst_v2_available")),
        "catalyst_v2_score":             catalyst_result.get("catalyst_v2_score"),
        "catalyst_v2_state":             catalyst_result.get("catalyst_v2_state") or "UNAVAILABLE",
        "catalyst_v2_primary_event":     catalyst_result.get("catalyst_v2_primary_event"),
        "catalyst_v2_conflicts":         catalyst_result.get("catalyst_v2_conflicts") or [],
    }


# ── Confidence factor ─────────────────────────────────────────────────────────

def _confidence(signals_available: list[bool]) -> float:
    """Fraction of non-missing signal slots (0–1)."""
    if not signals_available:
        return 0.0
    return round(sum(signals_available) / len(signals_available), 3)


# ── Core per-symbol confluence ─────────────────────────────────────────────────

def _compute_confluence(
    sym:          str,
    entry_result: Optional[dict],
    stage2_row:   Optional[dict],
    themes_idx:   dict[str, dict],
    options_map:  dict[str, dict],
    social_map:   dict[str, dict],
    ticker_theme_idx: Optional[dict[str, list[str]]] = None,
    rotation_idx:     Optional[dict[str, dict]] = None,
    theme_align_map:  Optional[dict[str, dict]] = None,
    options_align_map: Optional[dict[str, dict]] = None,
    catalyst_align_map: Optional[dict[str, dict]] = None,
    fundamentals_map: Optional[dict[str, dict]] = None,
) -> dict:
    t0 = time.time()

    # ── Individual signal normalizations ──────────────────────────────────────
    e_sig, e_state  = _entry_signal(entry_result)
    theme_bridge    = _theme_signal_v2(sym, ticker_theme_idx or {}, rotation_idx or {})
    theme_available = bool(theme_bridge.get("theme_signal_available"))
    t_sig           = _theme_sig_0_1(theme_bridge) if theme_available else 0.0
    t_phase         = theme_bridge.get("primary_theme_rotation_state") or "UNAVAILABLE"
    s_sig, s_label  = _stage_signal_from_lkg(stage2_row)
    o_sig, o_bias   = _options_signal(sym, options_map)

    social_entry = social_map.get(sym)

    # ── Base Trade score (4 signals; Theme weight OMITTED from the numerator
    #    AND denominator — not defaulted to 0.5 — when no canonical Theme
    #    membership / rotation result exists for this ticker) ────────────────
    if theme_available:
        base_raw = (
            _W_ENTRY   * e_sig +
            _W_THEME   * t_sig +
            _W_STAGE   * s_sig +
            _W_OPTIONS * o_sig
        ) * 100.0
    else:
        _denom = _W_ENTRY + _W_STAGE + _W_OPTIONS
        base_raw = (
            (_W_ENTRY   / _denom) * e_sig +
            (_W_STAGE   / _denom) * s_sig +
            (_W_OPTIONS / _denom) * o_sig
        ) * 100.0
    base_score = round(base_raw, 1)

    # ── Social bonus (conditional, 0–10 pts) ──────────────────────────────────
    entry_score_raw = entry_result.get("entry_score", 0) if entry_result else 0
    entry_grade_raw = entry_result.get("entry_grade", "AVOID") if entry_result else "AVOID"
    entry_family    = entry_result.get("entry_family", "BROKEN_OR_UNCLEAR") if entry_result else "BROKEN_OR_UNCLEAR"

    social_bonus, soc_eligible, soc_reason, soc_risk = _compute_social_bonus(
        entry_score_raw = int(entry_score_raw),
        entry_grade     = entry_grade_raw,
        entry_family    = entry_family,
        theme_sig_available = theme_available,
        t_sig           = t_sig,
        s_sig           = s_sig,
        o_sig           = o_sig,
        social_entry    = social_entry,
    )

    # ── Final trade score = base + bonus (clamped 0–100) ─────────────────────
    trade_score  = round(_clamp(base_score + social_bonus, 0.0, 100.0), 1)

    # ── Investment score = pure base (no social, no timing bonus) ────────────
    invest_score = base_score

    # ── REAL INVESTMENT ALIGNMENT V1 (SHADOW, additive-only) — separate
    #    deterministic long-horizon asset quality/acceleration model.
    #    Zero provider calls; zero effect on Trade Alignment / Actionability /
    #    Entry Structure / Options Alignment / legacy investment_confluence_score.
    try:
        from services.investment_alignment_v1 import compute_investment_alignment
        _fund_snap = (fundamentals_map or {}).get(sym) or {}
        investment_alignment_fields = compute_investment_alignment(
            symbol               = sym,
            fundamentals_fields  = _fund_snap.get("fields"),
            fundamentals_missing = _fund_snap.get("missing_fields"),
            stage2_row           = stage2_row,
        )
    except Exception:
        investment_alignment_fields = {
            "investment_alignment_available": False,
            "investment_alignment_score": None,
            "investment_alignment_state": "INSUFFICIENT_DATA",
            "investment_alignment_version": 1,
            "investment_alignment_components": {},
            "investment_alignment_reason_codes": ["INVESTMENT_ALIGNMENT_V1_ERROR"],
            "investment_alignment_strengths": [],
            "investment_alignment_risks": [],
            "minimum_evidence_met": False,
            "additional_components_available": 0,
        }

    # ── THEME_ALIGNMENT archetype (SHADOW, additive-only) ─────────────────────
    theme_align = (theme_align_map or {}).get(sym) or {"theme_signal_available": False}
    options_result = (options_align_map or {}).get(sym) or {"options_signal_available": False}
    catalyst_result = (catalyst_align_map or {}).get(sym) or {
        "catalyst_alignment_available": False,
        "catalyst_alignment_score": None,
        "primary_catalyst": None,
        "catalyst_events": [],
    }
    theme_alignment_fields = _compute_theme_alignment(
        sym             = sym,
        theme_align     = theme_align,
        s_sig           = s_sig,
        s_available     = stage2_row is not None,
        options_result  = options_result,
        catalyst_result = catalyst_result,
        entry_grade_raw = entry_grade_raw,
        entry_family    = entry_family,
        social_entry    = social_entry,
    )

    # ── ELITE_ASSET_REBOUND V1 (SHADOW, additive-only) — second Trade
    #    Alignment archetype. Independent of THEME_ALIGNMENT; never mixes or
    #    averages component scores with it. Zero provider calls. ─────────────
    try:
        elite_rebound_fields = _compute_elite_asset_rebound(
            sym                          = sym,
            investment_alignment_fields  = investment_alignment_fields,
            entry_result                 = entry_result,
            stage2_row                   = stage2_row,
            options_result               = options_result,
        )
    except Exception:
        elite_rebound_fields = _elite_asset_rebound_unavailable(["ELITE_ASSET_REBOUND_V1_ERROR"])

    # ── TRADE ARCHETYPE SELECTION (Part 10) — evaluate THEME_ALIGNMENT and
    #    ELITE_ASSET_REBOUND independently and select the higher-scoring
    #    available archetype. Never averages/mixes components. ───────────────
    ta_avail = bool(theme_alignment_fields.get("trade_alignment_available"))
    ta_score = theme_alignment_fields.get("trade_alignment_score")
    er_avail = bool(elite_rebound_fields.get("elite_asset_rebound_available"))
    er_score = elite_rebound_fields.get("elite_asset_rebound_score")

    archetype_scores = {
        "THEME_ALIGNMENT":     ta_score if ta_avail else None,
        "ELITE_ASSET_REBOUND": er_score if er_avail else None,
    }

    if ta_avail and er_avail:
        if er_score is not None and ta_score is not None and er_score > ta_score:
            selected_archetype, selected_reason = "ELITE_ASSET_REBOUND", "HIGHEST_AVAILABLE_ARCHETYPE_SCORE"
        else:
            selected_archetype, selected_reason = "THEME_ALIGNMENT", "HIGHEST_AVAILABLE_ARCHETYPE_SCORE"
    elif ta_avail:
        selected_archetype, selected_reason = "THEME_ALIGNMENT", "ONLY_THEME_ALIGNMENT_AVAILABLE"
    elif er_avail:
        selected_archetype, selected_reason = "ELITE_ASSET_REBOUND", "ONLY_ELITE_ASSET_REBOUND_AVAILABLE"
    else:
        selected_archetype, selected_reason = None, "NO_ARCHETYPE_AVAILABLE"

    # Base ta_fields on THEME_ALIGNMENT output (preserves theme/stage/options/
    # catalyst component fields Actionability generically reads regardless of
    # which archetype wins). Only override the top-level trade_alignment_*
    # decision fields when ELITE_ASSET_REBOUND is selected — the minimum
    # compatibility adjustment Actionability needs (Part 12).
    selected_ta_fields = dict(theme_alignment_fields)
    selected_ta_fields["theme_alignment_trade_score"]     = theme_alignment_fields.get("trade_alignment_score")
    selected_ta_fields["theme_alignment_trade_available"] = theme_alignment_fields.get("trade_alignment_available")
    selected_ta_fields["trade_alignment_archetype_scores"]  = archetype_scores
    selected_ta_fields["trade_alignment_selected_reason"]   = selected_reason
    if selected_archetype == "ELITE_ASSET_REBOUND":
        selected_ta_fields["trade_alignment_available"]    = True
        selected_ta_fields["trade_alignment_archetype"]    = "ELITE_ASSET_REBOUND"
        selected_ta_fields["trade_alignment_score"]        = er_score
        selected_ta_fields["trade_alignment_grade"]        = _ta_grade(er_score) if er_score is not None else None
        selected_ta_fields["trade_alignment_reason_codes"] = list(elite_rebound_fields.get("elite_asset_rebound_reason_codes") or [])
    selected_ta_fields.update(elite_rebound_fields)

    # ── ACTIONABILITY V1 (SHADOW, additive-only) — deterministic decision
    #    layer combining the SELECTED Trade Alignment archetype + Entry
    #    Structure V2 outputs above. Zero recomputation, zero provider
    #    calls, zero changes to Actionability state logic/thresholds. ───────
    actionability_fields = _compute_actionability(
        entry_result   = entry_result,
        ta_fields      = selected_ta_fields,
        options_result = options_result,
    )

    # ── Availability flags ────────────────────────────────────────────────────
    avail = [
        entry_result is not None,
        theme_available,
        stage2_row is not None,
        sym in options_map,
        social_entry is not None,
    ]
    conf = _confidence(avail)

    # ── Social raw fields for transparency ────────────────────────────────────
    soc_backend = _safe_float(social_entry.get("backend_score"), 0.0) if social_entry else 0.0
    soc_breadth  = _safe_float(social_entry.get("breadth_score"), 0.0) if social_entry else 0.0
    soc_fresh    = _safe_float(social_entry.get("freshness_score"), 0.0) if social_entry else 0.0
    soc_top      = bool(social_entry.get("has_top_conviction")) if social_entry else False

    return {
        "symbol":                       sym,
        # ── Score hierarchy ──────────────────────────────────────────────────
        "base_trade_confluence_score":  base_score,
        "social_bonus_score":           social_bonus,
        "trade_confluence_score":       trade_score,
        "investment_confluence_score":  invest_score,
        # ── Backward compat ──────────────────────────────────────────────────
        "confluence_score":             trade_score,
        "confluence_grade":             _grade(trade_score),
        "confluence_verdict":           _verdict(trade_score),
        "confidence":                   conf,
        # ── THEME_ALIGNMENT archetype (SHADOW, additive-only fields) ─────────
        "legacy_trade_confluence_score": trade_score,
        # ── REAL INVESTMENT ALIGNMENT V1 preserves legacy score under a new
        #    name (additive-only; legacy field above is untouched) ───────────
        "legacy_investment_confluence_score": invest_score,
        **selected_ta_fields,
        # ── ACTIONABILITY V1 (SHADOW, additive-only fields) ───────────────────
        **actionability_fields,
        # ── REAL INVESTMENT ALIGNMENT V1 (SHADOW, additive-only fields) ───────
        **investment_alignment_fields,
        # ── Legacy social bonus metadata (pre-THEME_ALIGNMENT; preserved
        #    under distinct keys since Part 3/4 of the THEME_ALIGNMENT spec
        #    redefine "social_bonus_score/eligible/reason" as the NEW
        #    archetype's own fields at the top level below) ─────────────────
        "legacy_social_bonus_eligible": soc_eligible,
        "legacy_social_bonus_reason":   soc_reason,
        "legacy_social_risk_flag":      soc_risk,
        "social_fields": {
            "covered":            social_entry is not None,
            "backend_score":      round(soc_backend, 3),
            "breadth_score":      round(soc_breadth, 3),
            "freshness_score":    round(soc_fresh, 3),
            "has_top_conviction": soc_top,
        },
        # ── Same-generation Entry fields (DEFECT 1 fix) ───────────────────────
        # Emitted directly from the entry_result already used to compute
        # Actionability and Elite Asset Rebound above — never reloaded,
        # never recomputed. Downstream consumers (alignment endpoint etc.)
        # must read Entry fields from here, not from a fresh LKG call, so
        # that every field in this row reflects one consistent Entry generation.
        "entry_available":   entry_result is not None,
        "entry_state":       (entry_result or {}).get("entry_state"),
        "entry_score":       entry_result.get("entry_score") if entry_result else None,
        "entry_grade":       entry_result.get("entry_grade") if entry_result else None,
        "entry_family":      entry_result.get("entry_family") if entry_result else None,
        "base_archetype":    ((entry_result or {}).get("structure_v2") or {}).get("base_archetype"),
        "extension_state":   (entry_result or {}).get("extension_state"),
        # ── Per-signal breakdown ─────────────────────────────────────────────
        "signal_breakdown": {
            "entry_state": {
                "signal":       round(e_sig, 4),
                "state":        e_state,
                "weight":       _W_ENTRY,
                "contribution": round(e_sig * _W_ENTRY * 100, 2),
                "available":    avail[0],
            },
            "theme_rotation": {
                "signal":       round(t_sig, 4),
                "phase":        t_phase,
                "weight":       _W_THEME,
                "contribution": round(t_sig * _W_THEME * 100, 2) if theme_available else 0.0,
                "available":    avail[1],
                "canonical_theme_memberships": theme_bridge.get("canonical_theme_memberships") or [],
                "theme_rotation_memberships":  theme_bridge.get("theme_rotation_memberships") or [],
                "primary_rotation_theme":          theme_bridge.get("primary_rotation_theme"),
                "primary_theme_rotation_score":    theme_bridge.get("primary_theme_rotation_score"),
                "primary_theme_rotation_state":     theme_bridge.get("primary_theme_rotation_state"),
                "primary_theme_rotation_direction": theme_bridge.get("primary_theme_rotation_direction"),
                "theme_signal_reason":         theme_bridge.get("theme_signal_reason"),
            },
            "stage_quality": {
                "signal":       round(s_sig, 4),
                "label":        s_label,
                "weight":       _W_STAGE,
                "contribution": round(s_sig * _W_STAGE * 100, 2),
                "available":    avail[2],
            },
            "options_flow": {
                "signal":       round(o_sig, 4),
                "bias":         o_bias,
                "weight":       _W_OPTIONS,
                "contribution": round(o_sig * _W_OPTIONS * 100, 2),
                "available":    avail[3],
            },
            "social_screener": {
                "bonus":        social_bonus,
                "eligible":     soc_eligible,
                "reason":       soc_reason,
                "available":    avail[4],
            },
        },
        "elapsed_ms":  round((time.time() - t0) * 1000, 1),
        "computed_at": _now_iso(),
    }


# ── Public API ─────────────────────────────────────────────────────────────────

def build_confluence_snapshot(
    symbols: Optional[list[str]] = None,
) -> dict:
    """
    Compute Confluence V2 for all watchlist symbols (or an explicit subset).

    If symbols is None, uses every ticker found in the Stage2 LKG.
    Returns full snapshot dict with ranked results.

    No external API calls.
    """
    t0 = time.time()

    # Load all caches once
    stage2_lkg  = _load_stage2_lkg()
    themes_idx  = {}  # legacy/dead field, kept only for the deprecated fallback fn
    options_map = _load_options_map()
    social_map  = _load_social_map()

    # Theme Bridge: canonical ticker→Theme membership + EXISTING Theme
    # Rotation result, built ONCE per snapshot (not per ticker).
    from services.theme_bridge import build_ticker_theme_index, get_theme_rotation_index
    ticker_theme_idx = build_ticker_theme_index()
    rotation_idx     = get_theme_rotation_index()

    # Load entry state LKG if available.
    # Also capture the canonical version constant so that stale-version rows are
    # treated as unavailable (same semantics as is_entry_version_current()).
    entry_lkg: dict[str, dict] = {}
    _current_entry_version: Optional[int] = None
    try:
        from services.entry_state_service import (
            get_all_entry_state_lkg,
            ENTRY_ANALYSIS_VERSION as _ENTRY_ANALYSIS_VERSION,
        )
        entry_lkg = get_all_entry_state_lkg()
        _current_entry_version = _ENTRY_ANALYSIS_VERSION
    except Exception:
        pass

    # Determine symbol universe
    universe = [s.upper() for s in symbols] if symbols else list(stage2_lkg.keys())
    if not universe:
        return {
            "ok":      False,
            "error":   "no_symbols_in_stage2_lkg",
            "results": [],
        }

    # ── THEME_ALIGNMENT archetype inputs — built ONCE per snapshot ──────────
    # Theme Alignment: canonical primary-theme resolver (services.theme_resolver
    # via services.theme_bridge.get_primary_theme_alignment), same resolver the
    # Watchlist UI uses. NOT the legacy get_ticker_rotation_bridge.
    theme_align_map: dict[str, dict] = {}
    try:
        from services.theme_bridge import get_primary_theme_alignment
        resolver_ctx = None
        try:
            from services.theme_resolver import build_theme_resolution_context
            resolver_ctx = build_theme_resolution_context()
        except Exception:
            resolver_ctx = None
        for sym in universe:
            theme_align_map[sym] = get_primary_theme_alignment(
                sym, resolver_ctx=resolver_ctx, rotation_idx=rotation_idx,
            )
    except Exception:
        theme_align_map = {}

    # Options Alignment — reuse services.options_alignment (zero provider calls).
    options_align_map: dict[str, dict] = {}
    try:
        from services.options_alignment import get_options_alignment_bulk
        options_align_map = get_options_alignment_bulk(universe)
    except Exception:
        options_align_map = {}

    # Catalyst Alignment — clean, zero-provider-call (services.catalyst_alignment).
    catalyst_align_map: dict[str, dict] = {}
    try:
        from services.catalyst_alignment import get_catalyst_alignment_bulk
        catalyst_align_map = get_catalyst_alignment_bulk(universe)
    except Exception:
        catalyst_align_map = {}

    # Fundamentals cache (Neon, weekly-refreshed, zero calls on read) — used
    # exclusively by REAL INVESTMENT ALIGNMENT V1 (SHADOW, additive-only).
    fundamentals_map: dict[str, dict] = {}
    try:
        from data.watchlist_fundamentals_store import get_snapshots_bulk
        fundamentals_map = get_snapshots_bulk(universe)
    except Exception:
        fundamentals_map = {}

    results: list[dict] = []
    social_bonus_counts = {"0": 0, "2_4": 0, "5_7": 0, "8_10": 0, "eligible": 0, "applied": 0}

    for sym in universe:
        # Normalize Entry: apply canonical current-version check before any
        # consumer sees the result.  A row that exists but carries a stale
        # entry_analysis_version is treated identically to a missing row — None.
        # This ensures emitted entry_available, Actionability, and Elite Rebound
        # all use the same normalized result without independent re-checks.
        _raw_entry = entry_lkg.get(sym)
        _entry_result: Optional[dict] = (
            _raw_entry
            if (
                _raw_entry is not None
                and _current_entry_version is not None
                and _raw_entry.get("entry_analysis_version") == _current_entry_version
            )
            else None
        )
        r = _compute_confluence(
            sym          = sym,
            entry_result = _entry_result,
            stage2_row   = stage2_lkg.get(sym),
            themes_idx   = themes_idx,
            options_map  = options_map,
            social_map   = social_map,
            ticker_theme_idx = ticker_theme_idx,
            rotation_idx     = rotation_idx,
            theme_align_map    = theme_align_map,
            options_align_map  = options_align_map,
            catalyst_align_map = catalyst_align_map,
            fundamentals_map   = fundamentals_map,
        )
        results.append(r)

        # Tally social bonus distribution (LEGACY bonus, unchanged behavior).
        # legacy bonus = trade_confluence_score - base_trade_confluence_score
        # (the top-level "social_bonus_score" key now holds the NEW
        # THEME_ALIGNMENT archetype's bonus — see legacy_social_bonus_* keys).
        bonus = round(r["trade_confluence_score"] - r["base_trade_confluence_score"], 1)
        if r.get("legacy_social_bonus_eligible"):
            social_bonus_counts["eligible"] += 1
        if bonus > 0:
            social_bonus_counts["applied"] += 1
        if bonus == 0:
            social_bonus_counts["0"] += 1
        elif bonus <= 4:
            social_bonus_counts["2_4"] += 1
        elif bonus <= 7:
            social_bonus_counts["5_7"] += 1
        else:
            social_bonus_counts["8_10"] += 1

    # Sort by trade_confluence_score descending
    results.sort(key=lambda r: r["trade_confluence_score"], reverse=True)
    for i, r in enumerate(results):
        r["confluence_rank"] = i + 1

    # Sort by investment_confluence_score for investment ranking
    invest_sorted = sorted(results, key=lambda r: r["investment_confluence_score"], reverse=True)
    for i, r in enumerate(invest_sorted):
        r["investment_rank"] = i + 1

    # Re-sort results by trade rank for default output
    results.sort(key=lambda r: r["confluence_rank"])

    # Verdict distribution
    verdict_dist: dict[str, int] = {}
    for r in results:
        v = r["confluence_verdict"]
        verdict_dist[v] = verdict_dist.get(v, 0) + 1

    # Stage signal coverage
    stage_covered   = sum(1 for r in results if r["signal_breakdown"]["stage_quality"]["available"])
    entry_covered   = sum(1 for r in results if r["signal_breakdown"]["entry_state"]["available"])
    theme_covered   = sum(1 for r in results if r["signal_breakdown"]["theme_rotation"]["available"])
    options_covered = sum(1 for r in results if r["signal_breakdown"]["options_flow"]["available"])
    social_covered  = sum(1 for r in results if r["signal_breakdown"]["social_screener"]["available"])

    # ── THEME_ALIGNMENT (Trade Alignment) archetype rank + diagnostics ──────
    ta_available_results = [r for r in results if r.get("trade_alignment_available")]
    ta_sorted = sorted(ta_available_results, key=lambda r: r["trade_alignment_score"], reverse=True)
    for i, r in enumerate(ta_sorted):
        r["trade_alignment_rank"] = i + 1
    for r in results:
        r.setdefault("trade_alignment_rank", None)

    ta_theme_covered    = sum(1 for r in results if r.get("theme_alignment_available"))
    ta_stage_covered    = sum(1 for r in results if r.get("stage_alignment_available"))
    ta_options_covered  = sum(1 for r in results if r.get("options_alignment_available"))
    ta_catalyst_covered = sum(1 for r in results if r.get("catalyst_alignment_available"))

    ta_signal_count_dist: dict[str, int] = {"0": 0, "1": 0, "2": 0, "3": 0, "4": 0}
    for r in results:
        cnt = r.get("trade_alignment_signal_count", 0) or 0
        ta_signal_count_dist[str(min(cnt, 4))] = ta_signal_count_dist.get(str(min(cnt, 4)), 0) + 1

    ta_grade_dist: dict[str, int] = {}
    ta_social_bonus_applied = 0
    ta_social_bonus_eligible = 0
    ta_scores_sorted: list[float] = []
    for r in results:
        if r.get("trade_alignment_available"):
            g = r.get("trade_alignment_grade")
            ta_grade_dist[g] = ta_grade_dist.get(g, 0) + 1
            sb = r.get("social_bonus_score") or 0
            if sb > 0:
                ta_social_bonus_applied += 1
            if r.get("social_bonus_eligible"):
                ta_social_bonus_eligible += 1
            ta_scores_sorted.append(r["trade_alignment_score"])
    ta_scores_sorted.sort()

    def _pct(sorted_vals: list[float], p: float) -> Optional[float]:
        if not sorted_vals:
            return None
        idx = min(int(len(sorted_vals) * p), len(sorted_vals) - 1)
        return sorted_vals[idx]

    trade_alignment_diagnostics = {
        "archetype":               "THEME_ALIGNMENT",
        "weights":                 {"theme": _TA_W_THEME, "stage": _TA_W_STAGE,
                                     "options": _TA_W_OPTIONS, "catalyst": _TA_W_CATALYST},
        "available_count":         len(ta_available_results),
        "unavailable_count":       len(results) - len(ta_available_results),
        "coverage": {
            "theme":    ta_theme_covered,
            "stage":    ta_stage_covered,
            "options":  ta_options_covered,
            "catalyst": ta_catalyst_covered,
        },
        "signal_count_distribution": ta_signal_count_dist,
        "grade_distribution":        ta_grade_dist,
        "social_bonus": {
            "eligible_count": ta_social_bonus_eligible,
            "applied_count":  ta_social_bonus_applied,
        },
        "score_percentiles": {
            "p10": _pct(ta_scores_sorted, 0.10),
            "p25": _pct(ta_scores_sorted, 0.25),
            "p50": _pct(ta_scores_sorted, 0.50),
            "p75": _pct(ta_scores_sorted, 0.75),
            "p90": _pct(ta_scores_sorted, 0.90),
        },
    }

    # ── ACTIONABILITY V1 diagnostics (SHADOW, additive-only) ─────────────────
    act_state_dist: dict[str, int] = {s: 0 for s in _ACTIONABILITY_ALL_STATES}
    act_available_count = 0
    act_scores_sorted: list[float] = []
    flow_leading_count = 0
    for r in results:
        st = r.get("actionability_state")
        if st in act_state_dist:
            act_state_dist[st] += 1
        if r.get("actionability_available"):
            act_available_count += 1
        sc = r.get("actionability_score")
        if sc is not None:
            act_scores_sorted.append(sc)
        if "FLOW_LEADING_PRICE" in (r.get("actionability_reason_codes") or []):
            flow_leading_count += 1
    act_scores_sorted.sort()

    actionability_diagnostics = {
        "version":            _ACTIONABILITY_VERSION,
        "available_count":    act_available_count,
        "unavailable_count":  len(results) - act_available_count,
        "state_distribution": act_state_dist,
        "flow_leading_price_count": flow_leading_count,
        "score_percentiles": {
            "p10": _pct(act_scores_sorted, 0.10),
            "p25": _pct(act_scores_sorted, 0.25),
            "p50": _pct(act_scores_sorted, 0.50),
            "p75": _pct(act_scores_sorted, 0.75),
            "p90": _pct(act_scores_sorted, 0.90),
        },
    }

    return {
        "ok":               True,
        "generated_at":     _now_iso(),
        "symbol_count":     len(results),
        "verdict_summary":  verdict_dist,
        "base_weights": {
            "entry_state":    _W_ENTRY,
            "theme_rotation": _W_THEME,
            "stage_quality":  _W_STAGE,
            "options_flow":   _W_OPTIONS,
        },
        "social_bonus": {
            "max_bonus_pts":        _SOCIAL_MAX_BONUS,
            "coverage_count":       social_covered,
            "eligible_count":       social_bonus_counts["eligible"],
            "applied_count":        social_bonus_counts["applied"],
            "bonus_0_count":        social_bonus_counts["0"],
            "bonus_2_4_count":      social_bonus_counts["2_4"],
            "bonus_5_7_count":      social_bonus_counts["5_7"],
            "bonus_8_10_count":     social_bonus_counts["8_10"],
        },
        "coverage": {
            "entry_state":    entry_covered,
            "stage_quality":  stage_covered,
            "theme_rotation": theme_covered,
            "options_flow":   options_covered,
            "social":         social_covered,
        },
        "theme_bridge_diagnostics": _theme_bridge_diagnostics(ticker_theme_idx, rotation_idx, universe),
        "trade_alignment_diagnostics": trade_alignment_diagnostics,
        "actionability_diagnostics": actionability_diagnostics,
        "elapsed_ms":  round((time.time() - t0) * 1000, 1),
        "results":     results,
    }


def _theme_bridge_diagnostics(
    ticker_theme_idx: dict[str, list[str]],
    rotation_idx:     dict[str, dict],
    universe:         list[str],
) -> dict:
    """Snapshot-level Theme Bridge stats for Part 6-8 validation of the spec."""
    try:
        from services.theme_bridge import get_ticker_theme_diagnostics
        base = get_ticker_theme_diagnostics(ticker_theme_idx)
        mapped_in_universe = sum(1 for s in universe if ticker_theme_idx.get(s))
        base["universe_size"] = len(universe)
        base["universe_mapped_count"] = mapped_in_universe
        base["universe_coverage_pct"] = round(100.0 * mapped_in_universe / len(universe), 2) if universe else 0.0
        base["rotation_snapshot_theme_count"] = len(rotation_idx)
        return base
    except Exception as e:
        return {"error": str(e)}


def get_confluence_for_symbol(symbol: str) -> dict:
    """Return Confluence V2 result for a single symbol."""
    snap = build_confluence_snapshot(symbols=[symbol.upper()])
    results = snap.get("results") or []
    if results:
        return results[0]
    return {
        "symbol":                      symbol.upper(),
        "base_trade_confluence_score": 0,
        "social_bonus_score":          0,
        "trade_confluence_score":      0,
        "investment_confluence_score": 0,
        "confluence_score":            0,
        "confluence_grade":            "AVOID",
        "confluence_verdict":          "AVOID",
        "confidence":                  0.0,
        "error":                       "compute_failed",
    }


# ── Retained snapshot (Watchlist Alignment Read Path V1) ────────────────────
# Process-local stale-while-revalidate cache over the canonical
# build_confluence_snapshot() producer. This layer performs NO scoring — it
# only stores/serves the completed return value of the existing producer and
# decides, from existing upstream freshness metadata, when to trigger exactly
# one background rebuild via that same producer. See spec:
# "WATCHLIST ALIGNMENT READ PATH V1" Parts 1/2/4/5.

_RETAINED_LOCK = threading.Lock()
_RETAINED: dict[str, Any] = {
    "snapshot":            None,
    "built_at":            None,
    "source_fingerprint":  None,
    "build_in_progress":   False,
    "stale_reasons":       [],
}

# Completion signal for the in-progress retained build.
# Set = no build running (or build just finished).
# Cleared = a build is actively in progress.
# Callers that detect already_building=True must wait() here instead of
# launching a second build_confluence_snapshot() call.
_RETAINED_BUILD_DONE = threading.Event()
_RETAINED_BUILD_DONE.set()   # initial state: no build in progress

# ── Stale-reason derivation (DEFECT 6 fix) ────────────────────────────────────
# Maps each fingerprint key to a human-readable reason code. Keys not listed
# here get a generic "FINGERPRINT_CHANGED" fallback so new keys are never
# silently swallowed.
_FP_KEY_TO_REASON: dict[str, str] = {
    "stage2_updated_at":           "STAGE2_CHANGED",
    "stage2_symbol_count":         "STAGE2_CHANGED",
    "entry_newest_computed_at":    "ENTRY_CHANGED",
    "entry_lkg_mtime":             "ENTRY_CHANGED",
    "options_lkg_mtime":           "RAW_OPTIONS_CHANGED",
    "options_supplement_lkg_mtime": "OPTIONS_ALIGNMENT_CHANGED",
    "theme_rs_refresh_ts_mtime":   "THEME_CHANGED",
    "x_consensus_mtime":           "SOCIAL_CHANGED",
}


def _derive_stale_reasons(
    old_fp: Optional[dict],
    new_fp: dict,
    age_s:  Optional[float],
    max_age_s: float,
) -> list[str]:
    """Return deduplicated reason codes for why the retained snapshot is stale."""
    reasons: list[str] = []
    seen: set[str] = set()

    def _add(code: str) -> None:
        if code not in seen:
            seen.add(code)
            reasons.append(code)

    if age_s is not None and age_s > max_age_s:
        _add("MAX_AGE_EXCEEDED")

    if old_fp is None:
        _add("NO_PRIOR_FINGERPRINT")
        return reasons

    for key, new_val in new_fp.items():
        old_val = old_fp.get(key)
        if old_val != new_val:
            _add(_FP_KEY_TO_REASON.get(key, "FINGERPRINT_CHANGED"))

    return reasons

# Conservative bounded max-age fallback (seconds). Applied in addition to the
# fingerprint check because Catalyst Alignment and Investment fundamentals do
# not expose a cheap, always-available freshness signal suitable for a fast
# per-request fingerprint (see Part 3 of the spec) — this bound catches drift
# in those two inputs without inventing a fake timestamp or a new scheduler.
_RETAINED_MAX_AGE_S = 1800  # 30 minutes


def _compute_source_fingerprint() -> dict[str, Any]:
    """
    Compact, deterministic fingerprint built ONLY from existing freshness/
    version metadata already produced by upstream sources. Answers:
    "Have any canonical Confluence inputs materially advanced since the
    retained snapshot was built?" No provider calls, no new DB writes.
    """
    fp: dict[str, Any] = {}

    # Stage2 — canonical top-level LKG "updated_at" (defines the universe).
    try:
        if _STAGE2_LKG.exists():
            raw = json.loads(_STAGE2_LKG.read_text())
            fp["stage2_updated_at"]  = raw.get("updated_at")
            fp["stage2_symbol_count"] = raw.get("symbol_count")
        else:
            fp["stage2_updated_at"] = None
            fp["stage2_symbol_count"] = None
    except Exception:
        fp["stage2_updated_at"] = None
        fp["stage2_symbol_count"] = None

    # Entry — newest per-symbol computed_at from the canonical entry LKG,
    # plus the LKG file mtime (the entry_state_service's own on-disk store).
    try:
        from services.entry_state_service import get_all_entry_state_lkg, _LKG_PATH as _ENTRY_LKG_PATH
        entry_lkg = get_all_entry_state_lkg()
        newest = None
        for row in entry_lkg.values():
            ca = row.get("computed_at")
            if ca and (newest is None or ca > newest):
                newest = ca
        fp["entry_newest_computed_at"] = newest
        fp["entry_lkg_mtime"] = _ENTRY_LKG_PATH.stat().st_mtime if _ENTRY_LKG_PATH.exists() else None
    except Exception:
        fp["entry_newest_computed_at"] = None
        fp["entry_lkg_mtime"] = None

    # Options signals — two independent sources tracked separately:
    #   1. options_lkg_mtime: raw composite signal (o_sig/o_bias) from
    #      _load_options_map() → options_master_lkg_v1.json
    #   2. options_supplement_lkg_mtime: Options Alignment result
    #      (options_result → Actionability / Trade Alignment /
    #      options.pressure_state) from get_options_alignment_bulk()
    #      → data.options_theme_supplement → options_supplement_lkg_v1.json
    #      These are DISTINCT files; collapsing them was DEFECT 2.
    try:
        fp["options_lkg_mtime"] = _OPTIONS_LKG.stat().st_mtime if _OPTIONS_LKG.exists() else None
    except Exception:
        fp["options_lkg_mtime"] = None
    try:
        from data.options_theme_supplement import _SUPPLEMENT_LKG_DISK_PATH as _SUPP_LKG_PATH
        fp["options_supplement_lkg_mtime"] = (
            _SUPP_LKG_PATH.stat().st_mtime if _SUPP_LKG_PATH.exists() else None
        )
    except Exception:
        fp["options_supplement_lkg_mtime"] = None

    # Theme Rotation / Theme RS — existing cadence file that Theme RS already
    # persists its per-timeframe refresh timestamps to.
    try:
        _theme_rs_refresh_ts_path = _BASE / "data" / "theme_rs_refresh_ts.json"
        fp["theme_rs_refresh_ts_mtime"] = (
            _theme_rs_refresh_ts_path.stat().st_mtime if _theme_rs_refresh_ts_path.exists() else None
        )
    except Exception:
        fp["theme_rs_refresh_ts_mtime"] = None

    # Social / X-consensus cache — existing weekly file mtime.
    try:
        fp["x_consensus_mtime"] = _X_CONSENSUS.stat().st_mtime if _X_CONSENSUS.exists() else None
    except Exception:
        fp["x_consensus_mtime"] = None

    # Catalyst Alignment source: no lightweight module-level freshness/version
    # field exists today (per audit — top_catalysts_service/calendar_snapshot_
    # service/rss_article_archive do not expose one cheap enough to read on
    # every request). NOT fabricated here. Covered instead by the conservative
    # bounded _RETAINED_MAX_AGE_S fallback applied in get_retained_confluence_
    # snapshot() below.
    #
    # Investment fundamentals: watchlist_fundamentals_store does have a real
    # "refreshed_at" column, but reading it requires a Neon query per symbol
    # batch — too costly to run on every fast warm-path request. Also covered
    # by the bounded max-age fallback rather than a per-request DB round trip.

    return fp


def _retained_is_stale(
    current_fp: dict[str, Any],
) -> tuple[bool, list[str]]:
    """
    True if fingerprint changed OR the bounded max-age has elapsed.
    Returns (stale: bool, reasons: list[str]) — reasons are empty when not stale.
    """
    with _RETAINED_LOCK:
        if _RETAINED["snapshot"] is None:
            return True, ["NO_SNAPSHOT"]
        old_fp  = _RETAINED["source_fingerprint"]
        built_at = _RETAINED["built_at"]

    age_s: Optional[float] = None
    if built_at:
        try:
            built_dt = datetime.fromisoformat(built_at)
            age_s = (datetime.now(timezone.utc) - built_dt).total_seconds()
        except Exception:
            pass

    fp_changed = (old_fp != current_fp)
    age_exceeded = (age_s is not None and age_s > _RETAINED_MAX_AGE_S) or (age_s is None)

    if not fp_changed and not age_exceeded:
        return False, []

    reasons = _derive_stale_reasons(old_fp, current_fp, age_s, _RETAINED_MAX_AGE_S)
    return True, reasons


def _start_background_rebuild() -> bool:
    """
    Atomically claim the builder slot then spawn the rebuild thread.

    Returns True  — a new build was started.
    Returns False — a build is already in progress; caller must not start another.

    INVARIANT ENFORCED:
      _RETAINED_BUILD_DONE.clear() and _RETAINED["build_in_progress"] = True
      happen under the same _RETAINED_LOCK acquisition.  No external observer can
      ever see (build_in_progress=True AND event=set) — that impossible state is
      permanently eliminated.

    The background thread only *sets* the event (in finally) — it never clears it.
    """
    with _RETAINED_LOCK:
        if _RETAINED["build_in_progress"]:
            return False
        # Atomic: clear event then mark in-progress, both under the lock.
        # Any waiter that subsequently reads build_in_progress=True is guaranteed
        # to find the event already cleared and will block on wait().
        _RETAINED_BUILD_DONE.clear()
        _RETAINED["build_in_progress"] = True

    def _rebuild() -> None:
        try:
            new_snap = build_confluence_snapshot()
            new_fp = _compute_source_fingerprint()
            with _RETAINED_LOCK:
                _RETAINED["snapshot"] = new_snap
                _RETAINED["built_at"] = _now_iso()
                _RETAINED["source_fingerprint"] = new_fp
                _RETAINED["build_in_progress"] = False
        except Exception as e:
            print(f"[CONFLUENCE_RETAINED] background rebuild failed (prior snapshot preserved): {e}")
            with _RETAINED_LOCK:
                _RETAINED["build_in_progress"] = False
        finally:
            # Always unblock waiters whether the build succeeded or failed.
            # Never clear the event here — that is the caller's job under the lock.
            _RETAINED_BUILD_DONE.set()

    threading.Thread(target=_rebuild, daemon=True, name="confluence-retained-rebuild").start()
    return True


def get_retained_confluence_snapshot() -> dict:
    """
    Stale-while-revalidate serving helper over the canonical
    build_confluence_snapshot() producer (no duplicate scoring logic):

      A. retained exists + fingerprint unchanged  -> return retained immediately
      B. retained exists + fingerprint changed     -> return retained immediately,
                                                       kick off exactly ONE background
                                                       rebuild (single-flight guarded)
      C. no retained snapshot yet, no build in progress -> become the single-flight
                                                           cold builder; build
                                                           synchronously on this thread
                                                           (routes call via
                                                           asyncio.to_thread — safe)
      D. no retained snapshot yet, build already in progress -> wait on
                                                                _RETAINED_BUILD_DONE;
                                                                return the result;
                                                                NEVER call the canonical
                                                                producer a second time.

    A failed background rebuild preserves the prior retained snapshot and
    clears the in-progress flag so a later request can retry.
    """
    with _RETAINED_LOCK:
        have_snapshot = _RETAINED["snapshot"] is not None

    if not have_snapshot:
        # Cold start — atomically claim the builder role or detect a racing build.
        with _RETAINED_LOCK:
            if _RETAINED["snapshot"] is not None:   # double-check under lock
                return _RETAINED["snapshot"]
            already_building = _RETAINED["build_in_progress"]
            if not already_building:
                # Spec-correct ordering: clear event BEFORE setting flag,
                # both under the same lock so no observer can see the
                # impossible state (build_in_progress=True, event=set).
                _RETAINED_BUILD_DONE.clear()
                _RETAINED["build_in_progress"] = True

        if already_building:
            # Single-flight wait — do NOT call build_confluence_snapshot() again.
            # This thread blocks here; because routes call this function via
            # asyncio.to_thread(), the Uvicorn event loop remains fully responsive
            # and Gunicorn heartbeat notifications continue uninterrupted.
            # Generous timeout: build is ~129 s; 300 s covers restarts/retries.
            _RETAINED_BUILD_DONE.wait(timeout=300)
            with _RETAINED_LOCK:
                snap = _RETAINED["snapshot"]
            if snap is not None:
                return snap
            # The in-progress build failed (snapshot still None after wait).
            # Allow exactly ONE waiter to become the next single-flight builder;
            # all other concurrent waiters raise so they do not pile up builds.
            with _RETAINED_LOCK:
                if _RETAINED["build_in_progress"]:
                    raise RuntimeError(
                        "[CONFLUENCE_RETAINED] startup build failed; "
                        "a concurrent retry is already in progress"
                    )
                _RETAINED["build_in_progress"] = True
                _RETAINED_BUILD_DONE.clear()
            # Fall through to the build block as the next single-flight builder.

        # Single builder: run the canonical producer synchronously on this thread.
        try:
            snap = build_confluence_snapshot()
            fp = _compute_source_fingerprint()
            with _RETAINED_LOCK:
                _RETAINED["snapshot"] = snap
                _RETAINED["built_at"] = _now_iso()
                _RETAINED["source_fingerprint"] = fp
                _RETAINED["build_in_progress"] = False
            _RETAINED_BUILD_DONE.set()
            return snap
        except Exception:
            with _RETAINED_LOCK:
                _RETAINED["build_in_progress"] = False
            _RETAINED_BUILD_DONE.set()
            raise

    current_fp = _compute_source_fingerprint()
    stale, reasons = _retained_is_stale(current_fp)

    if stale:
        # _start_background_rebuild() atomically clears the event and sets
        # build_in_progress under _RETAINED_LOCK — no separate state manipulation
        # needed here (which was the source of the race window).
        started = _start_background_rebuild()
        if started:
            with _RETAINED_LOCK:
                _RETAINED["stale_reasons"] = reasons
            print(f"[CONFLUENCE_RETAINED] stale — reasons={reasons} — starting background rebuild")

    with _RETAINED_LOCK:
        return _RETAINED["snapshot"]


def get_retained_confluence_meta() -> dict:
    """
    Lightweight metadata about the retained snapshot, for surfacing on the
    Watchlist Alignment endpoint. Does not trigger a rebuild — call
    get_retained_confluence_snapshot() first if a fresh/retained snapshot is
    needed.
    """
    # "stale" mirrors rebuild_in_progress: get_retained_confluence_snapshot()
    # already evaluates the fingerprint/max-age check and flips
    # build_in_progress=True the moment it decides the retained snapshot is
    # stale and starts a rebuild. Recomputing the fingerprint again here
    # would just duplicate that check on every request for no new signal.
    with _RETAINED_LOCK:
        built_at          = _RETAINED["built_at"]
        build_in_progress = _RETAINED["build_in_progress"]
        stale_reasons     = list(_RETAINED.get("stale_reasons") or [])
    return {
        "built_at":            built_at,
        "stale":               build_in_progress,
        "rebuild_in_progress": build_in_progress,
        "stale_reasons":       stale_reasons if build_in_progress else [],
    }
