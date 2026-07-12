"""
Caelyn Confluence Ranking Engine
=================================
Pure-computation, zero provider calls.

Adds three layers on top of the existing Confluence V2 row:

  Layer 1: caelyn_confluence_score / bucket / reason_codes
           Unified 0-100 display/ranking score for full-watchlist sorting.

  Layer 2: confluence_at_support_state / score / reason_codes
           Detects "confluence at support" — the product's primary signal.

  Layer 3: caelyn_confluence_bucket
           Pre-bucketed label for frontend filtering.

All inputs are already available in the in-process LKG — no new I/O.
"""

from __future__ import annotations
from typing import Optional

# ── Weights (must sum to 1.0) ──────────────────────────────────────────────────
_W_TA  = 0.30
_W_RR  = 0.25
_W_CAT = 0.20
_W_OPT = 0.15
_W_IA  = 0.10

# Neutral fallback values when a signal is unavailable
_NEUTRAL_IA  = 50.0   # Investment Alignment: neutral per spec
_NEUTRAL_OTH = 40.0   # Other signals: slightly cautious

# Extension states that disqualify support confluence
_SEVERE_EXT = {"EXTREME_EXTENSION", "VERTICAL", "CROWDED_MOVE", "VOLUME_CLIMAX"}

# Support statuses that mean support is intact
_SUPPORT_INTACT = {"above_support", "testing_support", "bounced_from_support"}

# Actionability states
_ACT_READY        = {"READY", "EARLY_WATCH"}
_ACT_NEAR_READY   = {"WAIT_FOR_RETEST", "WAIT_FOR_BREAKOUT", "REVERSAL_WATCH"}
_ACT_WATCH        = {"WATCH"}
_ACT_EXTENDED     = {"TOO_EXTENDED"}
_ACT_AVOID        = {"AVOID"}

# ── Buckets ───────────────────────────────────────────────────────────────────
BUCKET_ACTIONABLE          = "ACTIONABLE"
BUCKET_NEAR_ACTIONABLE     = "NEAR_ACTIONABLE"
BUCKET_CONFLUENCE_SUPPORT  = "CONFLUENCE_AT_SUPPORT"
BUCKET_INVESTMENT_QUALITY  = "INVESTMENT_QUALITY"
BUCKET_WATCH_FOR_RESET     = "WATCH_FOR_RESET"
BUCKET_SPECULATIVE_TRADE   = "SPECULATIVE_TRADE"
BUCKET_RISK_CONFLICT       = "RISK_CONFLICT"
BUCKET_NO_CLEAR            = "NO_CLEAR_CONFLUENCE"

# ── Support confluence states ─────────────────────────────────────────────────
CAS_HIGH                   = "HIGH_CONFLUENCE_AT_SUPPORT"
CAS_BUILDING               = "SUPPORT_CONFLUENCE_BUILDING"
CAS_SPECULATIVE            = "SPECULATIVE_SUPPORT_SETUP"
CAS_QUALITY                = "QUALITY_ASSET_AT_SUPPORT"
CAS_NEEDS_CONFIRM          = "SUPPORT_NEEDS_CONFIRMATION"
CAS_EXTENDED               = "EXTENDED_NOT_AT_SUPPORT"
CAS_BROKEN                 = "BROKEN_SUPPORT_AVOID"
CAS_NONE                   = "NO_SUPPORT_CONFLUENCE"


def _s(v: Optional[float]) -> float:
    """Safe float, returns 0.0 for None."""
    return float(v) if v is not None else 0.0


def _clamp(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


def _compute_caelyn_confluence_score(
    ta_score:  Optional[float],
    rr_score:  Optional[float],
    cat_score: Optional[float],
    opt_score: Optional[float],
    ia_score:  Optional[float],
    ia_available: bool,
) -> tuple[float, list[str]]:
    """
    Returns (raw_score 0-100, reason_codes).
    Before caps/penalties.
    """
    reasons: list[str] = []

    ta_val  = ta_score  if ta_score  is not None else _NEUTRAL_OTH
    rr_val  = rr_score  if rr_score  is not None else _NEUTRAL_OTH
    cat_val = cat_score if cat_score is not None else _NEUTRAL_OTH
    opt_val = opt_score if opt_score is not None else _NEUTRAL_OTH
    ia_val  = (ia_score if ia_score is not None else _NEUTRAL_IA) if ia_available else _NEUTRAL_IA

    if ta_score is None:
        reasons.append("TRADE_ALIGNMENT_UNAVAILABLE")
    if rr_score is None:
        reasons.append("ENTRY_RR_UNAVAILABLE")
    if cat_score is None:
        reasons.append("CATALYST_ALIGNMENT_UNAVAILABLE")
    if opt_score is None:
        reasons.append("OPTIONS_ALIGNMENT_UNAVAILABLE")
    if not ia_available:
        reasons.append("INVESTMENT_ALIGNMENT_NEUTRAL_50")

    raw = (
        _W_TA  * ta_val  +
        _W_RR  * rr_val  +
        _W_CAT * cat_val +
        _W_OPT * opt_val +
        _W_IA  * ia_val
    )
    return round(_clamp(raw), 1), reasons


def _compute_confluence_at_support(
    active_support_status:          Optional[str],
    distance_to_active_support_pct: Optional[float],
    lower_low_confirmed:            Optional[bool],
    extension_state:                Optional[str],
    entry_rr_state:                 Optional[str],
    ta_score:                       Optional[float],
    cat_score:                      Optional[float],
    theme_policy_boost:             float,
    opt_score:                      Optional[float],
    ia_score:                       Optional[float],
    ia_available:                   bool,
) -> dict:
    """
    Returns:
        confluence_at_support           bool
        confluence_at_support_state     str (one of CAS_*)
        confluence_at_support_score     int 0-100
        confluence_at_support_reason_codes list[str]
    """
    reasons: list[str] = []
    asst      = active_support_status or "no_clear_support"
    dist      = distance_to_active_support_pct
    llc       = bool(lower_low_confirmed)
    ext       = extension_state or "HEALTHY"
    near      = dist is not None and dist <= 12.0
    support_ok = asst in _SUPPORT_INTACT
    broken_rr  = entry_rr_state == "BROKEN_SUPPORT_AVOID"
    extended   = (ext in _SEVERE_EXT) or (entry_rr_state == "STRONG_ASSET_EXTENDED_WAIT")

    # ── BROKEN_SUPPORT_AVOID ─────────────────────────────────────────────────
    if llc or asst == "lost_confirmed" or broken_rr:
        if llc:
            reasons.append("LOWER_LOW_CONFIRMED")
        if asst == "lost_confirmed":
            reasons.append("ACTIVE_SUPPORT_LOST")
        if broken_rr:
            reasons.append("ENTRY_RR_BROKEN_SUPPORT")
        return {
            "confluence_at_support":            False,
            "confluence_at_support_state":      CAS_BROKEN,
            "confluence_at_support_score":      10,
            "confluence_at_support_reason_codes": reasons,
        }

    # ── EXTENDED_NOT_AT_SUPPORT ───────────────────────────────────────────────
    if extended:
        if ext in _SEVERE_EXT:
            reasons.append(f"EXTENSION_{ext}")
        if dist is not None and dist > 20.0:
            reasons.append(f"DIST_FROM_SUPPORT_{dist:.1f}PCT")
        return {
            "confluence_at_support":            False,
            "confluence_at_support_state":      CAS_EXTENDED,
            "confluence_at_support_score":      25,
            "confluence_at_support_reason_codes": reasons,
        }

    # ── Signal alignment helpers ──────────────────────────────────────────────
    ta_ok   = ta_score  is not None and ta_score  >= 60.0
    cat_ok  = cat_score is not None and cat_score >= 50.0
    opt_ok  = opt_score is not None and opt_score >= 55.0
    ia_ok   = ia_available and ia_score is not None and ia_score >= 70.0
    tp_ok   = theme_policy_boost > 0.0
    any_secondary = cat_ok or tp_ok or opt_ok or ia_ok

    # ── HIGH_CONFLUENCE_AT_SUPPORT ────────────────────────────────────────────
    if support_ok and near and ta_ok and any_secondary:
        reasons.append("ACTIVE_SUPPORT_HOLDING")
        reasons.append(f"SUPPORT_STATUS_{asst}")
        if ta_score is not None:
            reasons.append(f"TRADE_ALIGNMENT_{int(ta_score)}")
        if cat_ok:
            reasons.append(f"CATALYST_{int(cat_score or 0)}")
        if opt_ok:
            reasons.append(f"OPTIONS_{int(opt_score or 0)}")
        if ia_ok:
            reasons.append(f"INVESTMENT_{int(ia_score or 0)}")
        if tp_ok:
            reasons.append("THEME_POLICY_BOOST")
        if dist is not None:
            reasons.append(f"DIST_TO_SUPPORT_{dist:.1f}PCT")
        return {
            "confluence_at_support":            True,
            "confluence_at_support_state":      CAS_HIGH,
            "confluence_at_support_score":      85,
            "confluence_at_support_reason_codes": reasons,
        }

    # ── QUALITY_ASSET_AT_SUPPORT ──────────────────────────────────────────────
    # High investment quality, support intact, but trade/options not fully aligned
    if support_ok and near and ia_ok and not ta_ok:
        reasons.append("QUALITY_ASSET")
        reasons.append(f"INVESTMENT_{int(ia_score or 0)}")
        reasons.append(f"SUPPORT_STATUS_{asst}")
        if ta_score is not None:
            reasons.append(f"TRADE_ALIGNMENT_{int(ta_score)}_BELOW_60")
        return {
            "confluence_at_support":            True,
            "confluence_at_support_state":      CAS_QUALITY,
            "confluence_at_support_score":      65,
            "confluence_at_support_reason_codes": reasons,
        }

    # ── SPECULATIVE_SUPPORT_SETUP ─────────────────────────────────────────────
    # Investment weak/unavailable but trade + at least one secondary signal aligning
    ia_weak = not ia_available or ia_score is None or ia_score < 55.0
    trade_decent = ta_score is not None and ta_score >= 60.0
    secondary_ok = (cat_score is not None and cat_score >= 45.0) or opt_ok or tp_ok
    if support_ok and near and trade_decent and secondary_ok and ia_weak:
        reasons.append("SPECULATIVE_SETUP")
        reasons.append(f"SUPPORT_STATUS_{asst}")
        reasons.append(f"TRADE_ALIGNMENT_{int(ta_score or 0)}")
        if ia_weak:
            reasons.append("INVESTMENT_WEAK_OR_UNAVAILABLE")
        if cat_score is not None and cat_score >= 45.0:
            reasons.append(f"CATALYST_{int(cat_score)}")
        if opt_ok:
            reasons.append(f"OPTIONS_{int(opt_score or 0)}")
        return {
            "confluence_at_support":            True,
            "confluence_at_support_state":      CAS_SPECULATIVE,
            "confluence_at_support_score":      60,
            "confluence_at_support_reason_codes": reasons,
        }

    # ── SUPPORT_CONFLUENCE_BUILDING ───────────────────────────────────────────
    # Support nearby + some alignment, but not all gates met for higher states
    ta_building = ta_score is not None and ta_score >= 50.0
    if support_ok and near and ta_building:
        reasons.append("BUILDING")
        reasons.append(f"SUPPORT_STATUS_{asst}")
        if ta_score is not None:
            reasons.append(f"TRADE_ALIGNMENT_{int(ta_score)}")
        if not any_secondary:
            reasons.append("SECONDARY_SIGNALS_INSUFFICIENT")
        return {
            "confluence_at_support":            True,
            "confluence_at_support_state":      CAS_BUILDING,
            "confluence_at_support_score":      50,
            "confluence_at_support_reason_codes": reasons,
        }

    # ── SUPPORT_NEEDS_CONFIRMATION ────────────────────────────────────────────
    close_enough = dist is not None and dist <= 15.0
    if support_ok and close_enough:
        reasons.append("SUPPORT_NEARBY")
        reasons.append("SIGNALS_INSUFFICIENT")
        if dist is not None:
            reasons.append(f"DIST_TO_SUPPORT_{dist:.1f}PCT")
        return {
            "confluence_at_support":            False,
            "confluence_at_support_state":      CAS_NEEDS_CONFIRM,
            "confluence_at_support_score":      35,
            "confluence_at_support_reason_codes": reasons,
        }

    # ── NO_SUPPORT_CONFLUENCE ─────────────────────────────────────────────────
    reasons.append("NO_SUPPORT_CONFLUENCE")
    if not support_ok:
        reasons.append(f"SUPPORT_STATUS_{asst}")
    if dist is not None and dist > 15.0:
        reasons.append(f"DIST_FROM_SUPPORT_{dist:.1f}PCT")
    return {
        "confluence_at_support":            False,
        "confluence_at_support_state":      CAS_NONE,
        "confluence_at_support_score":      20,
        "confluence_at_support_reason_codes": reasons,
    }


def _assign_bucket(
    actionability_state:   Optional[str],
    entry_rr_state:        Optional[str],
    lower_low_confirmed:   Optional[bool],
    bearish_conflict:      bool,
    ta_score:              Optional[float],
    ia_score:              Optional[float],
    ia_available:          bool,
    cas_state:             str,
    cas_score:             int,
    support_ok:            bool,
) -> str:
    llc = bool(lower_low_confirmed)
    act = actionability_state or "UNKNOWN"
    rr  = entry_rr_state or ""

    # Structural support break — hard RISK_CONFLICT regardless of other signals
    structural_break = (
        llc
        or rr == "BROKEN_SUPPORT_AVOID"
        or cas_state == CAS_BROKEN
        or act in _ACT_AVOID
    )

    # Bearish conflict alone (without structural break) → penalty already applied
    # to caelyn_confluence_score; do NOT override bucket when support is still
    # intact and CAS signals are strong. Only escalate to RISK_CONFLICT when
    # bearish conflict is paired with a genuine support breakdown.
    if structural_break:
        return BUCKET_RISK_CONFLICT

    # Bearish conflict without structural break → demotes 1 level at most
    # (handled by the -10 score penalty; bucket stays at its natural position)
    bearish_only = bearish_conflict and not structural_break

    # 2) ACTIONABLE
    if act in _ACT_READY:
        return BUCKET_ACTIONABLE

    # 3) WATCH_FOR_RESET — extended or waiting
    if act in _ACT_EXTENDED or rr == "STRONG_ASSET_EXTENDED_WAIT":
        return BUCKET_WATCH_FOR_RESET

    # 4) NEAR_ACTIONABLE
    ta_decent = ta_score is not None and ta_score >= 65.0
    if act in _ACT_NEAR_READY:
        return BUCKET_NEAR_ACTIONABLE
    if act in _ACT_WATCH and ta_decent and rr not in ("BROKEN_SUPPORT_AVOID", "STRONG_ASSET_EXTENDED_WAIT"):
        return BUCKET_NEAR_ACTIONABLE

    # 5) CONFLUENCE_AT_SUPPORT
    if support_ok and cas_score >= 50:
        return BUCKET_CONFLUENCE_SUPPORT

    # 6) INVESTMENT_QUALITY — high investment but not ready
    if ia_available and ia_score is not None and ia_score >= 75.0:
        return BUCKET_INVESTMENT_QUALITY

    # 7) SPECULATIVE_TRADE — trade/catalyst/options strong, investment weak
    ia_weak = not ia_available or ia_score is None or ia_score < 55.0
    if ta_score is not None and ta_score >= 60.0 and ia_weak:
        return BUCKET_SPECULATIVE_TRADE

    # 8) NO_CLEAR_CONFLUENCE
    return BUCKET_NO_CLEAR


def compute_caelyn_confluence(
    trade_alignment_score:          Optional[float],
    trade_alignment_available:      bool,
    entry_risk_reward_score:        Optional[float],
    entry_risk_reward_state:        Optional[str],
    catalyst_alignment_score:       Optional[float],
    catalyst_alignment_available:   bool,
    theme_policy_boost:             float,
    options_alignment_score:        Optional[float],
    options_alignment_available:    bool,
    investment_alignment_score:     Optional[float],
    investment_alignment_available: bool,
    actionability_state:            Optional[str],
    active_support_status:          Optional[str],
    lower_low_confirmed:            Optional[bool],
    distance_to_active_support_pct: Optional[float],
    extension_state:                Optional[str],
    bearish_conflict:               bool,
) -> dict:
    """
    Master entry point.  Zero I/O, zero provider calls.

    Returns 8 new fields:
        caelyn_confluence_score
        caelyn_confluence_bucket
        caelyn_confluence_reason_codes
        confluence_at_support
        confluence_at_support_score
        confluence_at_support_state
        confluence_at_support_reason_codes
    """

    # ── Layer 1 — base score (before caps) ────────────────────────────────────
    base_score, base_reasons = _compute_caelyn_confluence_score(
        ta_score  = trade_alignment_score,
        rr_score  = entry_risk_reward_score,
        cat_score = catalyst_alignment_score,
        opt_score = options_alignment_score,
        ia_score  = investment_alignment_score,
        ia_available = investment_alignment_available,
    )

    # ── Layer 2 — confluence at support ───────────────────────────────────────
    cas = _compute_confluence_at_support(
        active_support_status          = active_support_status,
        distance_to_active_support_pct = distance_to_active_support_pct,
        lower_low_confirmed            = lower_low_confirmed,
        extension_state                = extension_state,
        entry_rr_state                 = entry_risk_reward_state,
        ta_score                       = trade_alignment_score,
        cat_score                      = catalyst_alignment_score,
        theme_policy_boost             = theme_policy_boost,
        opt_score                      = options_alignment_score,
        ia_score                       = investment_alignment_score,
        ia_available                   = investment_alignment_available,
    )

    # ── Caps and penalties ────────────────────────────────────────────────────
    score       = base_score
    cap_reasons = list(base_reasons)

    llc    = bool(lower_low_confirmed)
    asst   = active_support_status or "no_clear_support"
    act    = actionability_state or "UNKNOWN"
    rr_st  = entry_risk_reward_state or ""

    if llc or asst == "lost_confirmed":
        score = min(score, 45.0)
        cap_reasons.append("CAP_45_BROKEN_SUPPORT")

    if rr_st == "BROKEN_SUPPORT_AVOID":
        score = min(score, 45.0)
        cap_reasons.append("CAP_45_BROKEN_RR")

    if rr_st == "STRONG_ASSET_EXTENDED_WAIT":
        score = min(score, 65.0)
        cap_reasons.append("CAP_65_EXTENDED_FROM_SUPPORT")

    if act in _ACT_EXTENDED:
        score = min(score, 60.0)
        cap_reasons.append("CAP_60_TOO_EXTENDED_ACTIONABILITY")

    if bearish_conflict:
        score = _clamp(score - 10.0)
        cap_reasons.append("PENALTY_10_BEARISH_CONFLICT")

    # Positive: confluence at support boosts score by up to 5 pts
    if cas.get("confluence_at_support") and cas.get("confluence_at_support_score", 0) >= 65:
        score = min(score + 5.0, 100.0)
        cap_reasons.append("BOOST_5_HIGH_CONFLUENCE_AT_SUPPORT")

    score = round(_clamp(score), 1)

    # ── Bucket ────────────────────────────────────────────────────────────────
    support_ok_for_bucket = asst in _SUPPORT_INTACT
    bucket = _assign_bucket(
        actionability_state   = actionability_state,
        entry_rr_state        = entry_risk_reward_state,
        lower_low_confirmed   = lower_low_confirmed,
        bearish_conflict      = bearish_conflict,
        ta_score              = trade_alignment_score,
        ia_score              = investment_alignment_score,
        ia_available          = investment_alignment_available,
        cas_state             = cas.get("confluence_at_support_state", CAS_NONE),
        cas_score             = cas.get("confluence_at_support_score", 0),
        support_ok            = support_ok_for_bucket,
    )

    return {
        "caelyn_confluence_score":              score,
        "caelyn_confluence_bucket":             bucket,
        "caelyn_confluence_reason_codes":       cap_reasons,
        **cas,
    }
