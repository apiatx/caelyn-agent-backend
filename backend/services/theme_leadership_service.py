"""
theme_leadership_service.py
=============================
Cross-symbol theme leadership ranking computed from existing confluence snapshot
fields. Zero provider calls. Zero LLM calls.

Called at alignment-endpoint build time (all rows are in memory), not per-symbol
at snapshot time, because it requires a cross-symbol relative ranking within
each theme.

Leadership composite uses:
  50% trade_alignment_score  (trade setup quality)
  30% stage_alignment_score  (stage/technical quality)
  20% investment_alignment_score  (IA if available, else neutral 50)

Buckets:
  THEME_LEADER       leadership_score >= 75
  EMERGING_LEADER    leadership_score >= 62
  THEME_PARTICIPANT  leadership_score >= 48
  LAGGARD            otherwise

Leader-context labels (only for top-3 within theme):
  LEADER_AT_SUPPORT              near/testing/bounced from support, no LLC
  LEADER_CONTINUATION_READY      WAIT_FOR_RETEST/WAIT_FOR_BREAKOUT, no LLC
  LEADER_PULLBACK_BUY_ZONE       generally constructive position, no LLC
"""
from __future__ import annotations
from typing import Optional


_SUPPORT_INTACT  = {"above_support", "testing_support", "bounced_from_support"}
_NEAR_READY_ACTS = {"WAIT_FOR_RETEST", "WAIT_FOR_BREAKOUT", "REVERSAL_WATCH", "EARLY_WATCH"}
_READY_ACTS      = {"READY", "EARLY_WATCH"}


def _leadership_score(row: dict) -> float:
    ta   = float(row.get("trade_alignment_score") or 0)
    stg  = float(row.get("stage_alignment_score")  or 0)
    ia   = float(row.get("investment_alignment_score") or 50.0)
    ia   = max(0.0, min(100.0, ia))
    return 0.50 * ta + 0.30 * stg + 0.20 * ia


def _get_theme(row: dict) -> str:
    """
    Determine primary theme for this row from available fields.
    Uses theme_policy_theme → signal_breakdown.theme_rotation.primary_rotation_theme
    → "UNCLASSIFIED" as fallback.
    """
    tp = row.get("theme_policy_theme")
    if tp:
        return str(tp).upper()
    sb = row.get("signal_breakdown") or {}
    tr = sb.get("theme_rotation") or {}
    prt = tr.get("primary_rotation_theme")
    if prt:
        return str(prt).upper()
    # Try direct field
    prt2 = row.get("primary_rotation_theme")
    if prt2:
        return str(prt2).upper()
    return "UNCLASSIFIED"


def _leader_context(row: dict, rank: int, llc: bool) -> Optional[str]:
    if rank > 3 or llc:
        return None
    asst = row.get("active_support_status") or ""
    act  = row.get("actionability_state") or ""
    if asst in ("testing_support", "bounced_from_support"):
        return "LEADER_AT_SUPPORT"
    if act in _NEAR_READY_ACTS and asst in _SUPPORT_INTACT:
        return "LEADER_CONTINUATION_READY"
    if asst in _SUPPORT_INTACT:
        return "LEADER_PULLBACK_BUY_ZONE"
    return None


def compute_theme_leadership_for_rows(rows: list[dict]) -> dict[str, dict]:
    """
    Rank all symbols within their theme and return per-symbol leadership fields.

    Input:  list of confluence alignment rows (each must have 'symbol' key).
    Output: dict[symbol → leadership_fields]
    """
    # Group by theme
    by_theme: dict[str, list[dict]] = {}
    for row in rows:
        theme = _get_theme(row)
        by_theme.setdefault(theme, []).append(row)

    results: dict[str, dict] = {}

    for theme, theme_rows in by_theme.items():
        # Score each symbol
        scored = sorted(
            [(row, _leadership_score(row)) for row in theme_rows],
            key=lambda x: x[1],
            reverse=True,
        )
        total = len(scored)

        for rank_0, (row, ls) in enumerate(scored):
            rank = rank_0 + 1
            sym  = row.get("symbol", "")
            llc  = bool(row.get("lower_low_confirmed"))

            if   ls >= 75: bucket = "THEME_LEADER"
            elif ls >= 62: bucket = "EMERGING_LEADER"
            elif ls >= 48: bucket = "THEME_PARTICIPANT"
            else:          bucket = "LAGGARD"

            ctx = _leader_context(row, rank, llc)

            reason_codes = [
                f"THEME_{theme[:20]}",
                f"RANK_{rank}_OF_{total}",
                f"LS_{ls:.0f}",
            ]
            if ctx:
                reason_codes.append(ctx)
            if rank <= 3:
                reason_codes.append("TOP_3_IN_THEME")

            results[sym] = {
                "theme_leadership_score":    round(ls, 1),
                "theme_leadership_rank":     rank,
                "theme_leadership_total":    total,
                "theme_leadership_bucket":   bucket,
                "is_theme_leader":           rank <= 3,
                "is_top_3_theme_leader":     rank <= 3,
                "leader_context":            ctx,
                "theme_leader_reason_codes": reason_codes,
                "leadership_theme":          theme,
            }

    return results


def build_component_coverage(row: dict) -> dict:
    """
    Build component_coverage dict and confluence_confidence_score from existing
    alignment row fields. No provider calls.

    Status values:
      available          component data present
      no_signal          component ran, no signal found (e.g. no catalyst news)
      missing_cache      component cache not populated
      stale              cache exists but may be old
      confirmed_no_options  confirmed the ticker has no listed options
      not_scanned        not covered by the scan yet
    """
    # Trade core
    ta_avail = bool(row.get("trade_alignment_available"))
    ta_score = row.get("trade_alignment_score")
    if ta_avail and ta_score is not None:
        trade_status = "available"
    elif ta_avail:
        trade_status = "no_signal"
    else:
        trade_status = "missing_cache"

    # Entry / RR
    entry_avail = bool(row.get("entry_available"))
    if entry_avail:
        entry_status = "available"
    else:
        entry_status = "missing_cache"

    # Pattern
    pt = row.get("pattern_type")
    if pt and pt != "NO_PATTERN":
        pattern_status = "available"
    elif entry_avail:
        pattern_status = "no_signal"
    else:
        pattern_status = "missing_cache"

    # Catalyst
    cat_available = bool(row.get("catalyst_alignment_available") or
                         (row.get("catalyst") or {}).get("available"))
    cat_score = row.get("catalyst_alignment_score") or (row.get("catalyst") or {}).get("score")
    if cat_available and cat_score is not None:
        catalyst_status = "available"
    elif cat_available:
        catalyst_status = "no_signal"
    else:
        catalyst_status = "missing_cache"

    # Options
    opt_score = row.get("options_alignment_score")
    opt_pressure = row.get("options_pressure_state") or ""
    if opt_score is not None:
        options_status = "available"
    elif "NO_OPTIONS" in opt_pressure.upper() or "CONFIRMED_NO" in opt_pressure.upper():
        options_status = "confirmed_no_options"
    elif ta_avail:
        options_status = "not_scanned"
    else:
        options_status = "missing_cache"

    # Investment alignment
    ia_avail = bool(
        (row.get("investment_alignment") or {}).get("available") or
        row.get("investment_alignment_available")
    )
    ia_score = row.get("investment_alignment_score") or \
               (row.get("investment_alignment") or {}).get("score")
    if ia_avail and ia_score is not None:
        investment_status = "available"
    elif ia_avail:
        investment_status = "no_signal"
    else:
        investment_status = "missing_cache"

    # Confidence score: weighted by mandatory (trade+entry) vs optional
    mandatory_ok = (trade_status == "available") and (entry_status == "available")
    mandatory_partial = (trade_status == "available") or (entry_status == "available")
    opt_statuses = [catalyst_status, options_status, investment_status, pattern_status]
    opt_available = sum(1 for s in opt_statuses if s in ("available", "no_signal", "confirmed_no_options"))

    if mandatory_ok:
        base_conf = 0.50
    elif mandatory_partial:
        base_conf = 0.25
    else:
        base_conf = 0.0

    opt_conf = 0.50 * (opt_available / max(len(opt_statuses), 1))
    confidence_score = round(min((base_conf + opt_conf) * 100.0, 100.0), 1)

    return {
        "component_coverage": {
            "trade_core_status":   trade_status,
            "entry_status":        entry_status,
            "pattern_status":      pattern_status,
            "catalyst_status":     catalyst_status,
            "options_status":      options_status,
            "investment_status":   investment_status,
        },
        "confluence_confidence_score": confidence_score,
    }
