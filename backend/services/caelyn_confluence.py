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

Formula (v3 — Catalyst double-count removed):
  CCS = 0.45 × trade_core_score_ex_catalyst
      + 0.35 × entry_risk_reward_score
      + 0.20 × catalyst_alignment_score

  trade_core_score_ex_catalyst = renormalized(Theme + Stage + Options) / available
    — Catalyst is explicit at 20% above; removing it from the trade core prevents
      it from being counted twice for tickers that have both a trade score and a
      direct catalyst term.

Major/Minor Lower-Low distinction:
  major_lower_low = lower_low_confirmed AND active_support NOT intact
    → CAP 45, RISK_CONFLICT, CAS_BROKEN
  minor_lower_low = lower_low_confirmed AND active_support still intact
    → Penalty -5 only; CAS proceeds normally; bucket NOT forced to RISK_CONFLICT

Pattern-aware extension caps (Part 3 of spec):
  constructive_extension=True (HIGH_TIGHT_FLAG / BREAKOUT_SHELF etc.)
    → TOO_EXTENDED cap lifted; STRONG_ASSET_EXTENDED_WAIT cap softened
    → Bucket: WAIT_FOR_RESET → NEAR_ACTIONABLE if trade_core >= 60
  chase_extension=True
    → Hard WATCH_FOR_RESET cap; TOO_EXTENDED triggers normally

All inputs are already available in the in-process LKG — no new I/O.
"""

from __future__ import annotations
from typing import Optional

# ── Weights (must sum to 1.0) ──────────────────────────────────────────────────
# v3 formula: Trade-Core 45% + Entry RR 35% + Catalyst 20%
# Options NOT double-counted (already embedded in trade_core_score_ex_catalyst)
# Investment Alignment NOT in CCS — separate column / tie-breaker
_W_TC  = 0.45   # trade_core_score_ex_catalyst (Theme+Stage+Options, ex Catalyst)
_W_RR  = 0.35   # entry_risk_reward_score
_W_CAT = 0.20   # catalyst_alignment_score

# Neutral fallback values when a signal is unavailable
_NEUTRAL_OTH = 40.0   # Slightly cautious neutral for all absent signals

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


def compute_trade_core_ex_catalyst(
    theme_alignment_score:   Optional[float],
    stage_alignment_score:   Optional[float],
    options_alignment_score: Optional[float],
) -> Optional[float]:
    """
    Renormalized (Theme + Stage + Options) / available signals.

    Excludes Catalyst to prevent double-counting when Catalyst is already an
    explicit term in the CCS formula at 20%.  Uses the same 25/25/25/25
    available-signal renormalization as the THEME_ALIGNMENT archetype — if a
    signal is absent its weight is excluded from both numerator and denominator.

    Returns None only when ALL three signals are unavailable.
    """
    W = 25.0
    num = 0.0
    den = 0.0
    if theme_alignment_score is not None:
        num += W * theme_alignment_score
        den += W
    if stage_alignment_score is not None:
        num += W * stage_alignment_score
        den += W
    if options_alignment_score is not None:
        num += W * options_alignment_score
        den += W
    if den == 0.0:
        return None
    return round(num / den, 1)


def _compute_caelyn_confluence_score(
    tc_score:  Optional[float],
    rr_score:  Optional[float],
    cat_score: Optional[float],
) -> tuple[float, list[str]]:
    """
    Returns (raw_score 0-100, reason_codes).  Before caps/penalties.

    Formula v3:
      Trade-Core 45% (ex Catalyst) + Entry RR 35% + Catalyst 20%
    Options: already embedded in trade_core_score_ex_catalyst (25% of that score)
    Investment: separate column, not a CCS driver
    """
    reasons: list[str] = []

    tc_val  = tc_score  if tc_score  is not None else _NEUTRAL_OTH
    rr_val  = rr_score  if rr_score  is not None else _NEUTRAL_OTH
    cat_val = cat_score if cat_score is not None else _NEUTRAL_OTH

    if tc_score is None:
        reasons.append("TRADE_CORE_UNAVAILABLE")
    if rr_score is None:
        reasons.append("ENTRY_RR_UNAVAILABLE")
    if cat_score is None:
        reasons.append("CATALYST_ALIGNMENT_UNAVAILABLE")

    raw = (
        _W_TC  * tc_val  +
        _W_RR  * rr_val  +
        _W_CAT * cat_val
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
    constructive_extension:         bool = False,
    estimated_shelf_distance_pct:   Optional[float] = None,
) -> dict:
    """
    Returns:
        confluence_at_support           bool
        confluence_at_support_state     str (one of CAS_*)
        confluence_at_support_score     int 0-100
        confluence_at_support_reason_codes list[str]

    major_lower_low: llc AND support NOT intact → CAS_BROKEN
    minor_lower_low: llc AND support intact → no CAS_BROKEN (proceeds to normal checks)

    constructive_extension: WYFI-style HIGH_TIGHT_FLAG — don't force CAS_EXTENDED.
    estimated_shelf_distance_pct: pattern-engine shelf estimate (overrides dist
        when constructive_extension=True and shelf is closer than old support).
    """
    reasons: list[str] = []
    asst      = active_support_status or "no_clear_support"
    dist      = distance_to_active_support_pct
    llc       = bool(lower_low_confirmed)
    ext       = extension_state or "HEALTHY"
    support_ok = asst in _SUPPORT_INTACT

    # major vs minor LLC
    major_llc = llc and not support_ok      # support actually lost → structural break
    minor_llc = llc and support_ok          # price bounced back above support → recoverable

    # broken_rr only qualifies as structural if support is also lost
    broken_rr = (entry_rr_state == "BROKEN_SUPPORT_AVOID") and not support_ok

    # constructive extension: don't classify as EXTENDED when pattern engine
    # identified a HIGH_TIGHT_FLAG / BREAKOUT_SHELF structure
    extended = (ext in _SEVERE_EXT) or (entry_rr_state == "STRONG_ASSET_EXTENDED_WAIT")
    if constructive_extension and extended:
        extended = False   # pattern overrides the naive extension block

    # Effective distance: use shelf estimate when constructive and available
    eff_dist = dist
    if constructive_extension and estimated_shelf_distance_pct is not None:
        eff_dist = estimated_shelf_distance_pct

    near        = eff_dist is not None and eff_dist <= 12.0
    close_enough = eff_dist is not None and eff_dist <= 15.0

    # ── BROKEN_SUPPORT_AVOID (major breaks only) ──────────────────────────────
    if major_llc or asst == "lost_confirmed" or broken_rr:
        if major_llc:
            reasons.append("MAJOR_LOWER_LOW_SUPPORT_LOST")
        elif llc:
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

    # minor LLC note (non-breaking, just add to reasons if we proceed)
    if minor_llc:
        reasons.append("MINOR_LOWER_LOW_SUPPORT_INTACT")

    # ── EXTENDED_NOT_AT_SUPPORT (only for non-constructive extensions) ─────────
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
            reasons.append(f"TRADE_CORE_{int(ta_score)}")
        if cat_ok:
            reasons.append(f"CATALYST_{int(cat_score or 0)}")
        if opt_ok:
            reasons.append(f"OPTIONS_{int(opt_score or 0)}")
        if ia_ok:
            reasons.append(f"INVESTMENT_{int(ia_score or 0)}")
        if tp_ok:
            reasons.append("THEME_POLICY_BOOST")
        if eff_dist is not None:
            reasons.append(f"DIST_TO_SUPPORT_{eff_dist:.1f}PCT")
        if constructive_extension:
            reasons.append("CONSTRUCTIVE_EXTENSION_SHELF_DIST")
        return {
            "confluence_at_support":            True,
            "confluence_at_support_state":      CAS_HIGH,
            "confluence_at_support_score":      85,
            "confluence_at_support_reason_codes": reasons,
        }

    # ── QUALITY_ASSET_AT_SUPPORT ──────────────────────────────────────────────
    if support_ok and near and ia_ok and not ta_ok:
        reasons.append("QUALITY_ASSET")
        reasons.append(f"INVESTMENT_{int(ia_score or 0)}")
        reasons.append(f"SUPPORT_STATUS_{asst}")
        if ta_score is not None:
            reasons.append(f"TRADE_CORE_{int(ta_score)}_BELOW_60")
        return {
            "confluence_at_support":            True,
            "confluence_at_support_state":      CAS_QUALITY,
            "confluence_at_support_score":      65,
            "confluence_at_support_reason_codes": reasons,
        }

    # ── SPECULATIVE_SUPPORT_SETUP ─────────────────────────────────────────────
    ia_weak      = not ia_available or ia_score is None or ia_score < 55.0
    trade_decent = ta_score is not None and ta_score >= 60.0
    secondary_ok = (cat_score is not None and cat_score >= 45.0) or opt_ok or tp_ok
    if support_ok and near and trade_decent and secondary_ok and ia_weak:
        reasons.append("SPECULATIVE_SETUP")
        reasons.append(f"SUPPORT_STATUS_{asst}")
        reasons.append(f"TRADE_CORE_{int(ta_score or 0)}")
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
    ta_building = ta_score is not None and ta_score >= 50.0
    if support_ok and near and ta_building:
        reasons.append("BUILDING")
        reasons.append(f"SUPPORT_STATUS_{asst}")
        if ta_score is not None:
            reasons.append(f"TRADE_CORE_{int(ta_score)}")
        if not any_secondary:
            reasons.append("SECONDARY_SIGNALS_INSUFFICIENT")
        return {
            "confluence_at_support":            True,
            "confluence_at_support_state":      CAS_BUILDING,
            "confluence_at_support_score":      50,
            "confluence_at_support_reason_codes": reasons,
        }

    # ── SUPPORT_NEEDS_CONFIRMATION ────────────────────────────────────────────
    if support_ok and close_enough:
        reasons.append("SUPPORT_NEARBY")
        reasons.append("SIGNALS_INSUFFICIENT")
        if eff_dist is not None:
            reasons.append(f"DIST_TO_SUPPORT_{eff_dist:.1f}PCT")
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
    if eff_dist is not None and eff_dist > 15.0:
        reasons.append(f"DIST_FROM_SUPPORT_{eff_dist:.1f}PCT")
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
    active_support_status: Optional[str],
    bearish_conflict:      bool,
    tc_score:              Optional[float],
    ia_score:              Optional[float],
    ia_available:          bool,
    cas_state:             str,
    cas_score:             int,
    support_ok:            bool,
    constructive_extension: bool = False,
) -> str:
    llc  = bool(lower_low_confirmed)
    asst = active_support_status or "no_clear_support"
    act  = actionability_state or "UNKNOWN"
    rr   = entry_rr_state or ""

    # ── Major vs Minor LLC ─────────────────────────────────────────────────────
    major_llc = llc and asst not in _SUPPORT_INTACT  # support actually gone
    minor_llc = llc and asst in _SUPPORT_INTACT       # temporary dip, bounced back

    # ── Structural support break — hard RISK_CONFLICT ─────────────────────────
    # Only when support is ACTUALLY lost or CAS engine confirmed broken.
    # A "BROKEN_SUPPORT_AVOID" RR state caused by a minor LLC (support still
    # intact) does NOT alone warrant RISK_CONFLICT.
    structural_break = (
        major_llc
        or (rr == "BROKEN_SUPPORT_AVOID" and asst not in _SUPPORT_INTACT)
        or (cas_state == CAS_BROKEN and asst not in _SUPPORT_INTACT)
        or (act in _ACT_AVOID and asst not in _SUPPORT_INTACT)
    )

    if structural_break:
        return BUCKET_RISK_CONFLICT

    # ── Minor LLC: act may be AVOID due to LLC, but support is intact ──────────
    # Override effective_act for bucket purposes — the price already recovered.
    effective_act = act
    if minor_llc and act in _ACT_AVOID:
        effective_act = "WATCH"   # demote to WATCH, not AVOID

    # ── Extension-aware bucket: CONSTRUCTIVE vs CHASE ─────────────────────────
    # Non-constructive extension → WATCH_FOR_RESET
    is_ext = effective_act in _ACT_EXTENDED or rr == "STRONG_ASSET_EXTENDED_WAIT"

    if is_ext:
        if constructive_extension:
            # Pattern engine confirmed HIGH_TIGHT_FLAG or BREAKOUT_SHELF —
            # not a CHASE; allow upward bucket classification
            tc_decent = tc_score is not None and tc_score >= 60.0
            if tc_decent and effective_act not in _ACT_AVOID:
                # Falls through to NEAR_ACTIONABLE check below
                pass
            else:
                return BUCKET_WATCH_FOR_RESET
        else:
            return BUCKET_WATCH_FOR_RESET

    # ── ACTIONABLE ────────────────────────────────────────────────────────────
    if effective_act in _ACT_READY:
        return BUCKET_ACTIONABLE

    # ── NEAR_ACTIONABLE ───────────────────────────────────────────────────────
    tc_decent = tc_score is not None and tc_score >= 65.0
    if effective_act in _ACT_NEAR_READY:
        return BUCKET_NEAR_ACTIONABLE
    # Constructive extension with decent trade core → near actionable
    if constructive_extension and tc_decent:
        return BUCKET_NEAR_ACTIONABLE
    if effective_act in _ACT_WATCH and tc_decent and rr not in ("BROKEN_SUPPORT_AVOID", "STRONG_ASSET_EXTENDED_WAIT"):
        return BUCKET_NEAR_ACTIONABLE

    # ── CONFLUENCE_AT_SUPPORT ─────────────────────────────────────────────────
    if support_ok and cas_score >= 50:
        return BUCKET_CONFLUENCE_SUPPORT

    # ── INVESTMENT_QUALITY ────────────────────────────────────────────────────
    if ia_available and ia_score is not None and ia_score >= 75.0:
        return BUCKET_INVESTMENT_QUALITY

    # ── SPECULATIVE_TRADE ─────────────────────────────────────────────────────
    ia_weak = not ia_available or ia_score is None or ia_score < 55.0
    if tc_score is not None and tc_score >= 60.0 and ia_weak:
        return BUCKET_SPECULATIVE_TRADE

    # ── NO_CLEAR_CONFLUENCE ───────────────────────────────────────────────────
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
    # v3 additions (pattern-aware, ex-catalyst trade core)
    trade_core_score_ex_catalyst:   Optional[float]  = None,
    constructive_extension:         bool             = False,
    chase_extension:                bool             = False,
    estimated_shelf_distance_pct:   Optional[float]  = None,
    theme_alignment_score:          Optional[float]  = None,
    stage_alignment_score:          Optional[float]  = None,
) -> dict:
    """
    Master entry point.  Zero I/O, zero provider calls.

    v3 changes vs v2:
    - Uses trade_core_score_ex_catalyst (Theme+Stage+Options, no Catalyst)
      instead of trade_alignment_score in the CCS formula to remove double-count.
    - major_lower_low (llc + support lost) → RISK_CONFLICT / CAS_BROKEN
    - minor_lower_low (llc + support intact) → -5 penalty only
    - constructive_extension (HIGH_TIGHT_FLAG / BREAKOUT_SHELF) lifts the
      TOO_EXTENDED / STRONG_ASSET_EXTENDED_WAIT WATCH_FOR_RESET cap
    - estimated_shelf_distance_pct allows CAS to use actual shelf proximity
      instead of old-base distance for constructive extension setups

    Returns 12 new fields:
        caelyn_confluence_score
        caelyn_confluence_bucket
        caelyn_confluence_reason_codes
        confluence_at_support
        confluence_at_support_score
        confluence_at_support_state
        confluence_at_support_reason_codes
        major_lower_low_confirmed
        minor_lower_low
        trade_core_score_ex_catalyst      (echo back for consumers)
        entry_pattern_rr_score            (= entry_risk_reward_score, renamed)
        catalyst_detail_status
    """
    llc  = bool(lower_low_confirmed)
    asst = active_support_status or "no_clear_support"
    act  = actionability_state or "UNKNOWN"
    rr_st = entry_risk_reward_state or ""

    major_llc = llc and asst not in _SUPPORT_INTACT
    minor_llc = llc and asst in _SUPPORT_INTACT

    # ── Derive trade_core_score_ex_catalyst if not supplied ───────────────────
    if trade_core_score_ex_catalyst is None:
        trade_core_score_ex_catalyst = compute_trade_core_ex_catalyst(
            theme_alignment_score   = theme_alignment_score,
            stage_alignment_score   = stage_alignment_score,
            options_alignment_score = options_alignment_score,
        )
    # Fall back to trade_alignment_score only when completely unavailable
    tc_score = trade_core_score_ex_catalyst

    # ── Layer 1 — base score (before caps) ────────────────────────────────────
    base_score, base_reasons = _compute_caelyn_confluence_score(
        tc_score  = tc_score,
        rr_score  = entry_risk_reward_score,
        cat_score = catalyst_alignment_score,
    )

    # ── Layer 2 — confluence at support ───────────────────────────────────────
    cas = _compute_confluence_at_support(
        active_support_status          = active_support_status,
        distance_to_active_support_pct = distance_to_active_support_pct,
        lower_low_confirmed            = lower_low_confirmed,
        extension_state                = extension_state,
        entry_rr_state                 = entry_risk_reward_state,
        ta_score                       = tc_score,
        cat_score                      = catalyst_alignment_score,
        theme_policy_boost             = theme_policy_boost,
        opt_score                      = options_alignment_score,
        ia_score                       = investment_alignment_score,
        ia_available                   = investment_alignment_available,
        constructive_extension         = constructive_extension,
        estimated_shelf_distance_pct   = estimated_shelf_distance_pct,
    )

    # ── Caps and penalties ────────────────────────────────────────────────────
    score       = base_score
    cap_reasons = list(base_reasons)

    # Major LLC (support actually gone)
    if major_llc or asst == "lost_confirmed":
        score = min(score, 45.0)
        cap_reasons.append("CAP_45_MAJOR_LLC_BROKEN_SUPPORT")
    # Minor LLC (support still intact) — penalty only
    elif minor_llc:
        score = _clamp(score - 5.0)
        cap_reasons.append("PENALTY_5_MINOR_LLC_SUPPORT_INTACT")

    # BROKEN_SUPPORT_AVOID RR only caps if support is also actually lost
    if rr_st == "BROKEN_SUPPORT_AVOID" and asst not in _SUPPORT_INTACT:
        score = min(score, 45.0)
        cap_reasons.append("CAP_45_BROKEN_RR_SUPPORT_LOST")

    # STRONG_ASSET_EXTENDED_WAIT: softer cap for constructive extension
    if rr_st == "STRONG_ASSET_EXTENDED_WAIT":
        if constructive_extension:
            score = min(score, 80.0)
            cap_reasons.append("CAP_80_CONSTRUCTIVE_EXTENSION_EXTENDED_WAIT")
        else:
            score = min(score, 65.0)
            cap_reasons.append("CAP_65_EXTENDED_FROM_SUPPORT")

    # TOO_EXTENDED actionability: lifted for constructive extension
    if act in _ACT_EXTENDED:
        if constructive_extension:
            score = min(score, 78.0)
            cap_reasons.append("CAP_78_CONSTRUCTIVE_TOO_EXTENDED")
        else:
            score = min(score, 60.0)
            cap_reasons.append("CAP_60_TOO_EXTENDED_ACTIONABILITY")

    # Bearish conflict — penalty
    if bearish_conflict:
        score = _clamp(score - 10.0)
        cap_reasons.append("PENALTY_10_BEARISH_CONFLICT")

    # Positive: high confluence at support boosts score by up to 5 pts
    if cas.get("confluence_at_support") and cas.get("confluence_at_support_score", 0) >= 65:
        score = min(score + 5.0, 100.0)
        cap_reasons.append("BOOST_5_HIGH_CONFLUENCE_AT_SUPPORT")

    score = round(_clamp(score), 1)

    # ── Bucket ────────────────────────────────────────────────────────────────
    bucket = _assign_bucket(
        actionability_state    = actionability_state,
        entry_rr_state         = entry_risk_reward_state,
        lower_low_confirmed    = lower_low_confirmed,
        active_support_status  = active_support_status,
        bearish_conflict       = bearish_conflict,
        tc_score               = tc_score,
        ia_score               = investment_alignment_score,
        ia_available           = investment_alignment_available,
        cas_state              = cas.get("confluence_at_support_state", CAS_NONE),
        cas_score              = cas.get("confluence_at_support_score", 0),
        support_ok             = asst in _SUPPORT_INTACT,
        constructive_extension = constructive_extension,
    )

    # ── catalyst_detail_status (display label) ────────────────────────────────
    cat_s = catalyst_alignment_score
    if cat_s is None:
        catalyst_detail_status = "neutral"
    elif cat_s >= 70:
        catalyst_detail_status = "strong_tailwind"
    elif cat_s >= 55:
        catalyst_detail_status = "moderate_tailwind"
    elif cat_s >= 45:
        catalyst_detail_status = "neutral"
    elif cat_s >= 30:
        catalyst_detail_status = "mild_headwind"
    else:
        catalyst_detail_status = "strong_headwind"

    # ── investment_quality_label ──────────────────────────────────────────────
    ia_s = investment_alignment_score
    if not investment_alignment_available or ia_s is None:
        investment_quality_label = "not_assessed"
    elif ia_s >= 80:
        investment_quality_label = "high_quality"
    elif ia_s >= 65:
        investment_quality_label = "above_average"
    elif ia_s >= 50:
        investment_quality_label = "average"
    elif ia_s >= 35:
        investment_quality_label = "below_average"
    else:
        investment_quality_label = "low_quality"

    return {
        "caelyn_confluence_score":              score,
        "caelyn_confluence_bucket":             bucket,
        "caelyn_confluence_reason_codes":       cap_reasons,
        # major/minor LLC classification
        "major_lower_low_confirmed":            major_llc,
        "minor_lower_low":                      minor_llc,
        # v3 trade-core echoed back for consumers
        "trade_core_score_ex_catalyst":         tc_score,
        "entry_pattern_rr_score":               entry_risk_reward_score,
        # display labels
        "catalyst_detail_status":               catalyst_detail_status,
        "investment_quality_label":             investment_quality_label,
        **cas,
    }
