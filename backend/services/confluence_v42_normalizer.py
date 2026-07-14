"""
confluence_v42_normalizer.py

Pure function: build_confluence_v42_object(row) -> clean normalized dict.
Also exports: derive_boolean_flags(row) -> corrected boolean dict.

No I/O, no scoring math, no provider calls.
All inputs come from the already-assembled flat confluence row dict.
"""

from __future__ import annotations
from typing import Any

_LABEL_DISPLAY_MAP: dict[str, str] = {
    "READY":             "Ready to Enter",
    "NEAR_ACTIONABLE":   "Near Entry",
    "WAIT_FOR_BREAKOUT": "Wait for Breakout",
    "WAIT_FOR_RETEST":   "Wait for Retest",
    "WATCH_FOR_RESET":   "Watch for Reset",
    "WATCH":             "Watch",
    "AVOID":             "Avoid",
    "INSUFFICIENT_DATA": "Insufficient Data",
}

_DEPRECATED_FIELDS: list[str] = [
    "caelyn_confluence_v4_score",
    "caelyn_confluence_v4_bucket",
    "caelyn_confluence_v4_actionability",
    "caelyn_confluence_v4_components",
    "caelyn_confluence_v4_bonus_breakdown",
    "caelyn_confluence_v4_reason_codes",
    "caelyn_confluence_v4_normalized_score",
    "caelyn_confluence_v4_raw_score",
    "caelyn_confluence_v4_total_score",
    "caelyn_confluence_v4_core_score",
    "caelyn_confluence_v4_bonus_score",
    "caelyn_confluence_v4_confidence_score",
    "caelyn_confluence_v4_available_max_pts",
    "trade_alignment_score",
    "legacy_trade_alignment_score",
    "legacy_trade_alignment_archetype",
    "legacy_actionability_state",
    "social_bonus_score",
    "base_trade_alignment_score",
    "confluence_verdict",
    "confluence_grade",
    "signal_breakdown",
    "prediction_market_bonus_points",
    "theme_policy_bonus_points",
]

_OPTIONS_UNAVAILABLE_STATUSES = {
    "confirmed_no_options",
    "no_options",
    "not_available",
    "insufficient_history",
    "unavailable",
    "foreign_exchange",
    "pending",
    "not_scanned",
    "missing_cache",
}

_CATALYST_UNAVAILABLE_STATUSES = {
    "unavailable",
    "no_catalyst",
    "insufficient_data",
    "missing",
}


def _fv(row: dict, *keys: str, default: Any = None) -> Any:
    for k in keys:
        v = row.get(k)
        if v is not None:
            return v
    return default


def _fvf(row: dict, *keys: str, default: float = 0.0) -> float:
    v = _fv(row, *keys, default=None)
    return float(v) if v is not None else default


def _fvb(row: dict, key: str) -> bool:
    return bool(row.get(key))


def build_fib_wave_status(row: dict) -> str:
    fib_computed = row.get("fib_computed")
    fib_ctx      = row.get("primary_fib_context")
    wave_label   = row.get("wave_structure_label")
    if fib_computed is True or fib_ctx is not None or wave_label is not None:
        return "available"
    return "pending_10y_backfill"


def build_risk_flags(row: dict) -> list[str]:
    """
    TRUE trade/risk-quality blockers only.
    Data/coverage/pending flags go in build_data_status_flags() instead.
    """
    flags: list[str] = []
    bucket      = str(row.get("caelyn_confluence_bucket") or "")
    act         = str(row.get("caelyn_confluence_v42_actionability") or "")
    ext_state   = str(row.get("extension_state") or "").upper()
    entry_st    = str(row.get("entry_state") or "").upper()
    entry_pts   = _fvf(row, "entry_exit_points")
    entry_ex_st = str(row.get("entry_exit_status") or "").lower()

    if ext_state == "EXTREME_EXTENSION":
        flags.append("EXTREME_EXTENSION")
    if _fvb(row, "chase_extension"):
        flags.append("CHASE_EXTENSION")
    if _fvb(row, "major_lower_low_confirmed"):
        flags.append("MAJOR_LOWER_LOW_CONFIRMED")
    elif _fvb(row, "lower_low_confirmed"):
        flags.append("LOWER_LOW_CONFIRMED")
    if bucket == "RISK_CONFLICT" or _fvb(row, "is_risk_conflict"):
        if "RISK_CONFLICT" not in flags:
            flags.append("RISK_CONFLICT")
    if act in {"AVOID"} or bucket in {"AVOID"}:
        flags.append("AVOID_SIGNAL")
    if (
        entry_pts < 3.0
        or act in {"WATCH", "AVOID"}
        or entry_ex_st in {"no_entry", "blocked"}
        or entry_st in {"FAILED_BREAKOUT", "SUPPORT_LOST", "UNDER_PRESSURE"}
    ):
        if entry_pts < 2.0:
            flags.append("NO_CLEAR_ENTRY")
    existing_risk_codes = [
        rc for rc in (row.get("caelyn_confluence_reason_codes") or [])
        if str(rc).startswith("RISK_")
    ]
    for rc in existing_risk_codes:
        if rc not in flags:
            flags.append(rc)
    return list(dict.fromkeys(flags))


def build_data_status_flags(row: dict) -> list[str]:
    """
    Data/coverage/pending status flags — NOT bearish trade signals.
    Frontend should display these as neutral informational badges,
    never as red risk warnings.
    """
    flags: list[str] = []
    opts_score   = row.get("options_alignment_score")
    opts_status  = str(row.get("options_status") or "").lower()
    cat_status   = str(row.get("catalyst_status") or "").lower()
    v42_bb       = row.get("caelyn_confluence_v42_bonus_breakdown") or {}
    whale_bb     = v42_bb.get("whale_insider") or {}
    whale_status = str(whale_bb.get("status") or "").lower()

    if build_fib_wave_status(row) == "pending_10y_backfill":
        flags.append("FIB_PENDING")
    if opts_status in {"confirmed_no_options", "no_options"}:
        flags.append("OPTIONS_UNAVAILABLE")
    elif opts_score is None and opts_status not in {"not_scanned", "pending", ""}:
        if opts_status in _OPTIONS_UNAVAILABLE_STATUSES:
            flags.append("OPTIONS_UNAVAILABLE")
    if cat_status in _CATALYST_UNAVAILABLE_STATUSES:
        flags.append("CATALYST_UNAVAILABLE")
    if whale_status in {"not_wired", ""}:
        flags.append("WHALE_INSIDER_NOT_WIRED")
    return list(dict.fromkeys(flags))


def build_why_now(row: dict) -> list[str]:
    reasons: list[str] = []
    bucket       = str(row.get("caelyn_confluence_bucket") or "")
    act          = str(row.get("caelyn_confluence_v42_actionability") or "")
    theme_pts    = _fvf(row, "theme_alignment_points")
    theme_score  = _fvf(row, "theme_alignment_score")
    stage_pts    = _fvf(row, "stage_quality_points")
    stage_score  = _fvf(row, "stage_quality_score", "stage_alignment_score")
    opts_score   = row.get("options_alignment_score")
    tech_raw     = _fvf(row, "technical_setup_raw_score")
    ext_quality  = str(row.get("extension_quality") or "").upper()
    cat_pts      = _fvf(row, "catalyst_alignment_points")
    cat_score    = _fvf(row, "catalyst_alignment_score")
    cat_status   = str(row.get("catalyst_status") or "").lower()
    inv_score    = _fvf(row, "investment_quality_score")
    is_iq        = _fvb(row, "is_investment_quality")
    social_pts   = _fvf(row, "social_bonus_points")
    social_hit   = _fv(row, "social_sections_hit", default=0)
    bottle_pts   = _fvf(row, "bottleneck_bonus_points")
    v42_bb       = row.get("caelyn_confluence_v42_bonus_breakdown") or {}
    social_bb    = v42_bb.get("social") or {}
    social_sects = social_bb.get("sections_hit", social_hit) or 0

    if bucket in {"ACTIONABLE", "NEAR_ACTIONABLE"} or act in {"READY", "NEAR_ACTIONABLE"}:
        if theme_score >= 55.0 and theme_pts >= 7.0:
            reasons.append("Theme alignment is positive")
        if stage_score >= 75.0 or stage_pts >= 11.0:
            reasons.append("Stage quality is strong")
        if opts_score is not None and opts_score >= 65.0:
            reasons.append("Options flow is supportive")
        if tech_raw >= 60.0 or ext_quality == "CONSTRUCTIVE":
            reasons.append("Technical setup is constructive")
        if cat_status not in _CATALYST_UNAVAILABLE_STATUSES and cat_score >= 50.0 and cat_pts >= 5.0:
            reasons.append("Catalyst support is present")
        if is_iq or inv_score >= 68.0:
            reasons.append("Investment quality is strong")
        if social_sects >= 2 or social_pts >= 10.0:
            reasons.append("Social momentum is active")
        if bottle_pts > 0:
            reasons.append("Bottleneck exposure adds bonus support")
    elif bucket == "INVESTMENT_QUALITY":
        if inv_score >= 68.0:
            reasons.append("Investment quality is strong")
        if stage_score >= 75.0:
            reasons.append("Stage quality is strong")
        if theme_score >= 55.0:
            reasons.append("Theme alignment is positive")
    return reasons


def build_why_wait(row: dict) -> list[str]:
    reasons: list[str] = []
    bucket      = str(row.get("caelyn_confluence_bucket") or "")
    act         = str(row.get("caelyn_confluence_v42_actionability") or "")
    ext_state   = str(row.get("extension_state") or "").upper()
    opts_score  = row.get("options_alignment_score")
    entry_pts   = _fvf(row, "entry_exit_points")
    is_risk     = _fvb(row, "is_risk_conflict") or bucket == "RISK_CONFLICT"
    major_llc   = _fvb(row, "major_lower_low_confirmed")
    conf_score  = _fvf(row, "caelyn_confluence_score")

    if is_risk:
        reasons.append("Risk conflict detected")
    if major_llc:
        reasons.append("Major lower low risk present")
    if act in {"WATCH_FOR_RESET"} or bucket == "WATCH_FOR_RESET":
        if ext_state == "EXTREME_EXTENSION" or _fvb(row, "chase_extension"):
            reasons.append("Extension risk is elevated")
        else:
            reasons.append("Waiting for a cleaner reset before entry")
    if act in {"WAIT_FOR_RETEST"}:
        reasons.append("Waiting for retest")
    if act in {"WAIT_FOR_BREAKOUT"}:
        reasons.append("Entry is not clean yet")
    elif act in {"NEAR_ACTIONABLE"} and entry_pts < 6.0:
        reasons.append("Entry is not clean yet")
    if build_fib_wave_status(row) == "pending_10y_backfill":
        reasons.append("Fib/Wave context pending 10Y history")
    if opts_score is None:
        reasons.append("Options data unavailable")
    if bucket in {"NO_CLEAR_CONFLUENCE", "WATCH"} or act in {"WATCH", "AVOID"}:
        if conf_score < 35.0:
            reasons.append("No clear confluence yet")
    return reasons


def build_invalidation_level(row: dict) -> float | None:
    v42_comps  = row.get("caelyn_confluence_v42_components") or {}
    entry_comp = v42_comps.get("entry_exit") or {}
    v = entry_comp.get("invalidation_level")
    if v is not None:
        return float(v)
    v = row.get("critical_break_level")
    if v is not None:
        return float(v)
    return None


def _safe_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def build_target_zone(row: dict) -> dict | None:
    v42_comps  = row.get("caelyn_confluence_v42_components") or {}
    entry_comp = v42_comps.get("entry_exit") or {}
    t1 = entry_comp.get("target_1")
    t2 = entry_comp.get("target_2")
    bt_raw = entry_comp.get("breakout_trigger") or row.get("pattern_breakout_trigger")
    rr = entry_comp.get("risk_reward_ratio")
    bt = _safe_float(bt_raw)
    t1f = _safe_float(t1)
    t2f = _safe_float(t2)
    rrf = _safe_float(rr)
    if t1f is None and t2f is None and bt is None and rrf is None:
        return None
    return {
        "target_1":          t1f,
        "target_2":          t2f,
        "breakout_trigger":  bt,
        "risk_reward_ratio": rrf,
    }


def _component_block(comp: dict, max_points: int, extra: dict | None = None) -> dict:
    out: dict = {
        "raw_score":   comp.get("raw_score"),
        "points":      comp.get("points"),
        "max_points":  max_points,
        "available":   comp.get("available", True),
        "status":      comp.get("status"),
        "reason_codes": list(comp.get("reason_codes") or []),
    }
    if extra:
        out.update(extra)
    return out


def build_confluence_v42_object(row: dict) -> dict:
    v42_comps = row.get("caelyn_confluence_v42_components") or {}
    v42_bb    = row.get("caelyn_confluence_v42_bonus_breakdown") or {}
    social_bb = v42_bb.get("social") or {}
    whale_bb  = v42_bb.get("whale_insider") or {}
    bottle_bb = v42_bb.get("bottleneck") or {}

    _theme = v42_comps.get("theme_alignment")     or {}
    _stage = v42_comps.get("stage_quality")       or {}
    _opts  = v42_comps.get("options_alignment")   or {}
    _tech  = v42_comps.get("technical_setup")     or {}
    _entry = v42_comps.get("entry_exit")           or {}
    _cat   = v42_comps.get("catalyst_alignment")  or {}
    _inv   = v42_comps.get("investment_alignment") or {}

    score_total = row.get("caelyn_confluence_score")
    score_core  = row.get("caelyn_confluence_core_score")
    score_bonus = row.get("caelyn_confluence_bonus_score")
    pct = round(float(score_total) / 125.0 * 100.0, 1) if score_total is not None else None

    act_raw     = str(row.get("caelyn_confluence_v42_actionability") or "WATCH")
    bucket_raw  = str(row.get("caelyn_confluence_bucket") or "NO_CLEAR_CONFLUENCE")
    label_disp  = _LABEL_DISPLAY_MAP.get(act_raw, act_raw.replace("_", " ").title())

    inv_level   = build_invalidation_level(row)
    target_zone = build_target_zone(row)
    why_now     = build_why_now(row)
    why_wait    = build_why_wait(row)
    risk_flags        = build_risk_flags(row)
    data_status_flags = build_data_status_flags(row)
    fib_status        = build_fib_wave_status(row)

    is_act  = row.get("is_actionable_setup", False)
    is_near = row.get("is_near_actionable", False)
    is_wfr  = row.get("is_watch_for_reset", False)
    is_risk = row.get("is_risk_conflict", False)
    is_iq   = row.get("is_investment_quality", False)

    opts_avail = (
        _opts.get("available", False) is True
        or row.get("options_alignment_score") is not None
    )

    social_sects  = social_bb.get("sections_hit", 0) or 0
    social_status = social_bb.get("status", "unavailable")
    whale_status  = whale_bb.get("status", "not_wired")
    bottle_status = bottle_bb.get("status", "unavailable")

    deprecated_present = any(row.get(f) is not None for f in _DEPRECATED_FIELDS)

    return {
        "score": {
            "total":                score_total,
            "core":                 score_core,
            "bonus":                score_bonus,
            "core_max":             100,
            "bonus_max":            25,
            "total_max":            125,
            "display_mode":         "CORE_100_PLUS_BONUS_25",
            "percent_of_total_max": pct,
        },
        "action": {
            "label":              act_raw,
            "bucket":             bucket_raw,
            "label_display":      label_disp,
            "invalidation_level": inv_level,
            "target_zone":        target_zone,
            "why_now":            why_now,
            "why_wait":           why_wait,
        },
        "components": {
            "theme": _component_block(_theme, 15),
            "stage": _component_block(_stage, 15, {
                "stage_label": _stage.get("stage_label"),
            }),
            "options": _component_block(_opts, 20, {
                "available": opts_avail,
            }),
            "technical_setup": _component_block(_tech, 8, {
                "label": _tech.get("technical_setup_label") or row.get("technical_setup_label"),
            }),
            "entry_exit": _component_block(_entry, 12),
            "catalyst": _component_block(_cat, 15),
            "investment": _component_block(_inv, 15, {
                "quality_label": (
                    row.get("investment_quality_label")
                    or row.get("investment_quality_rank_label")
                ),
                "pillar_count": row.get("investment_pillar_count"),
            }),
        },
        "bonuses": {
            "social": {
                "points":           social_bb.get("points", 0) or 0,
                "max_points":       15,
                "sections_hit":     social_sects,
                "confluence_hit":   bool(social_bb.get("consensus_hit") or social_bb.get("confluence_hit")),
                "acceleration_hit": bool(social_bb.get("acceleration_hit") or social_bb.get("hype_hit")),
                "fresh_hit":        bool(social_bb.get("fresh_hit") or social_bb.get("freshness_hit")),
                "status":           social_status,
            },
            "whale_insider": {
                "points":    whale_bb.get("points", 0) or 0,
                "max_points": 5,
                "status":    whale_status if whale_status else "not_wired",
            },
            "bottleneck": {
                "points":       bottle_bb.get("points", 0) or 0,
                "max_points":   5,
                "anchor_count": (
                    bottle_bb.get("anchor_count")
                    or row.get("bottleneck_anchor_count")
                    or 0
                ),
                "status": bottle_status,
            },
        },
        "technical": {
            "stage_label":          _stage.get("stage_label") or row.get("stage_label"),
            "stage_score":          _stage.get("raw_score") or row.get("stage_quality_score") or row.get("stage_alignment_score"),
            "technical_setup_label": row.get("technical_setup_label"),
            "entry_state":          row.get("entry_state"),
            "entry_score":          row.get("entry_score"),
            "extension_state":      row.get("extension_state"),
            "extension_quality":    row.get("extension_quality"),
            "fib_context":          row.get("primary_fib_context"),
            "fib_timeframe":        row.get("primary_fib_timeframe"),
            "nearest_fib_label":    row.get("primary_nearest_fib_label"),
            "nearest_fib_level":    row.get("primary_nearest_fib_level"),
            "distance_to_fib_pct":  row.get("primary_distance_to_fib_pct"),
            "fib_confidence":       row.get("primary_fib_confidence"),
            "fib_computed":         bool(row.get("fib_computed")),
            "fib_years_available":  row.get("fib_years_available"),
            "wave_structure":       row.get("wave_structure_label"),
            "wave_score":           row.get("wave_structure_score"),
            "fib_wave_status":      fib_status,
        },
        "risk": {
            "risk_flags":                    risk_flags,
            "major_lower_low_confirmed":     bool(row.get("major_lower_low_confirmed")),
            "lower_low_confirmed":           bool(row.get("lower_low_confirmed")),
            "chase_extension":               bool(row.get("chase_extension")),
            "constructive_extension":        bool(row.get("constructive_extension")),
            "critical_break_level":          row.get("critical_break_level"),
            "active_support_status":         row.get("active_support_status"),
            "active_support_type":           row.get("active_support_type"),
            "distance_to_active_support_pct": row.get("distance_to_active_support_pct"),
        },
        "booleans": {
            "is_actionable_setup":  bool(is_act),
            "is_near_actionable":   bool(is_near),
            "is_watch_for_reset":   bool(is_wfr),
            "is_risk_conflict":     bool(is_risk),
            "is_investment_quality": bool(is_iq),
        },
        "metadata": {
            "schema_version":           "v4.2",
            "confidence_score":         row.get("caelyn_confluence_confidence_score"),
            "reason_codes":             list(row.get("caelyn_confluence_reason_codes") or []),
            "data_status_flags":        data_status_flags,
            "deprecated_fields_present": deprecated_present,
            "deprecated_confluence_fields": _DEPRECATED_FIELDS,
        },
    }


def derive_boolean_flags(row: dict) -> dict:
    """
    Re-derive booleans from V4.2 canonical bucket + actionability.
    Must be called after V4.2 is promoted to caelyn_confluence_bucket.
    Fixes the ABCL bug: bucket=WATCH_FOR_RESET but is_watch_for_reset=False.
    """
    bucket = str(row.get("caelyn_confluence_bucket") or "")
    act    = str(row.get("caelyn_confluence_v42_actionability") or "")

    is_act  = bucket == "ACTIONABLE" or act == "READY"
    is_near = (bucket == "NEAR_ACTIONABLE") or (
        act == "NEAR_ACTIONABLE" and bucket not in {"ACTIONABLE", "WATCH_FOR_RESET", "RISK_CONFLICT"}
    )
    is_wfr  = bucket == "WATCH_FOR_RESET" or (
        act == "WATCH_FOR_RESET" and bucket not in {"ACTIONABLE", "NEAR_ACTIONABLE", "RISK_CONFLICT"}
    )
    is_risk = bucket == "RISK_CONFLICT" or bool(row.get("major_lower_low_confirmed"))
    is_iq   = bucket == "INVESTMENT_QUALITY" and not is_risk

    is_near = is_near and not is_act and not is_risk
    is_wfr  = is_wfr  and not is_act and not is_near and not is_risk

    return {
        "is_actionable_setup":   is_act,
        "is_near_actionable":    is_near,
        "is_watch_for_reset":    is_wfr,
        "is_risk_conflict":      is_risk,
        "is_investment_quality": is_iq,
    }
