"""
CAELYN CONFLUENCE V4 — Unified Scoring Engine (Phase 1 Additive)

Formula:
  v4_core_score          = sum of 6 first-class components  (max 85)
  v4_social_bonus        = social confirmation bonus         (max 15)
  v4_pre_overlay_score   = min(100, core + social)
  v4_overlay_bonus       = theme_policy + prediction + whale/insider + bottleneck  (max 15)
  v4_total_score         = min(115, pre_overlay + overlay)

Components:
  Theme Alignment    15 pts
  Stage Quality      15 pts
  Options Alignment  15 pts
  Entry R/R          15 pts
  Catalyst Alignment 15 pts
  Investment Align   10 pts

Zero LLM calls. Zero provider calls. Pure read from existing snapshot + caches.
"""
from __future__ import annotations

import time
from typing import Optional

# ── Module-level bottleneck cache ──────────────────────────────────────────────
_BOTTLENECK_MAP_CACHE: Optional[dict] = None
_BOTTLENECK_MAP_TS: float = 0.0
_BOTTLENECK_MAP_TTL = 3600.0


def _get_bottleneck_map() -> dict[str, dict]:
    global _BOTTLENECK_MAP_CACHE, _BOTTLENECK_MAP_TS
    now = time.time()
    if _BOTTLENECK_MAP_CACHE is None or (now - _BOTTLENECK_MAP_TS) > _BOTTLENECK_MAP_TTL:
        try:
            from services.playbook.curated_anchor_bottlenecks import get_multi_anchor_screener
            rows = get_multi_anchor_screener(min_anchors=1)
            m: dict[str, dict] = {}
            for r in rows:
                sym = str(r.get("ticker") or r.get("symbol") or "").upper()
                if sym:
                    names = r.get("anchor_names") or []
                    m[sym] = {"anchor_count": len(names), "anchor_names": names}
            _BOTTLENECK_MAP_CACHE = m
            _BOTTLENECK_MAP_TS = now
        except Exception:
            _BOTTLENECK_MAP_CACHE = {}
    return _BOTTLENECK_MAP_CACHE or {}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _clamp(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


def _safe_float(v, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
    except Exception:
        return default


def _lerp(score: float, s0: float, s1: float, p0: float, p1: float) -> float:
    if score <= s0:
        return p0
    if score >= s1:
        return p1
    t = (score - s0) / (s1 - s0)
    return p0 + t * (p1 - p0)


def _score_to_pts(score: float, breakpoints: list[tuple[float, float]], max_pts: float) -> float:
    """
    Linear interpolation between (score_threshold, pts) breakpoints (sorted ascending).
    Clamps to max_pts.
    """
    if not breakpoints:
        return 0.0
    if score <= breakpoints[0][0]:
        return breakpoints[0][1]
    for i in range(len(breakpoints) - 1):
        s0, p0 = breakpoints[i]
        s1, p1 = breakpoints[i + 1]
        if s0 <= score < s1:
            t = (score - s0) / (s1 - s0) if s1 > s0 else 0.0
            return min(max_pts, p0 + t * (p1 - p0))
    return min(max_pts, breakpoints[-1][1])


# ── Component Scorers ──────────────────────────────────────────────────────────

def _score_theme_alignment(row: dict) -> dict:
    """
    Theme Alignment — 15 pts max.

    Uses canonical theme_alignment_score (0-100) from snapshot.
    Theme Policy is NOT included here; it goes in the overlay bonus layer.
    Returns a component dict with raw_score, points, available, status, reason_codes,
    and theme metadata.
    """
    raw_score    = row.get("theme_alignment_score")
    available    = bool(row.get("theme_alignment_available")) and raw_score is not None
    breakdown    = row.get("signal_breakdown") or {}
    theme_rot    = breakdown.get("theme_rotation") or {}
    theme_name   = theme_rot.get("primary_rotation_theme") or row.get("theme_policy_theme")
    rot_label    = theme_rot.get("primary_theme_rotation_state") or theme_rot.get("state")

    reason_codes: list[str] = []

    if not available or raw_score is None:
        reason_codes.append("THEME_SIGNAL_UNAVAILABLE")
        return {
            "raw_score":                None,
            "points":                   0,
            "available":                False,
            "status":                   "missing_cache",
            "theme_name":               theme_name,
            "theme_rotation_label":     rot_label,
            "theme_alignment_score":    None,
            "reason_codes":             reason_codes,
        }

    raw = _safe_float(raw_score)

    # Tiered mapping: top themes score proportionally better
    breakpoints = [
        (0,  0.0),
        (20, 2.0),
        (35, 4.0),
        (50, 7.0),
        (65, 10.0),
        (75, 12.0),
        (85, 13.5),
        (95, 15.0),
    ]
    pts = _score_to_pts(raw, breakpoints, 15.0)

    if raw >= 80:
        reason_codes.append("THEME_LEADING")
    elif raw >= 65:
        reason_codes.append("THEME_STRONG")
    elif raw >= 50:
        reason_codes.append("THEME_EMERGING")
    elif raw >= 35:
        reason_codes.append("THEME_NEUTRAL")
    else:
        reason_codes.append("THEME_LAGGING")

    return {
        "raw_score":                round(raw, 1),
        "points":                   round(pts, 2),
        "available":                True,
        "status":                   "available",
        "theme_name":               theme_name,
        "theme_rotation_label":     rot_label,
        "theme_alignment_score":    round(raw, 1),
        "reason_codes":             reason_codes,
    }


def _score_stage_quality(row: dict) -> dict:
    """
    Stage Quality — 15 pts max.

    Uses stage_alignment_score (0-100) from snapshot.
    Rewards early S1/S2 basing → breakout; penalises S3/S4.
    """
    raw_score = row.get("stage_alignment_score")
    available = bool(row.get("stage_alignment_available")) and raw_score is not None
    reason_codes: list[str] = []

    if not available or raw_score is None:
        reason_codes.append("STAGE_QUALITY_UNAVAILABLE")
        return {
            "raw_score":    None,
            "points":       0,
            "available":    False,
            "status":       "missing_cache",
            "stage_label":  None,
            "reason_codes": reason_codes,
        }

    raw = _safe_float(raw_score)

    # Infer stage label from score
    if raw >= 78:
        stage_label = "STAGE2_EARLY"
    elif raw >= 63:
        stage_label = "STAGE2"
    elif raw >= 48:
        stage_label = "STAGE2_3"
    elif raw >= 33:
        stage_label = "STAGE3"
    elif raw >= 20:
        stage_label = "STAGE4_TRANSITION"
    else:
        stage_label = "STAGE4_BREAKDOWN"

    breakpoints = [
        (0,  0.0),
        (20, 1.5),
        (33, 4.0),
        (48, 7.0),
        (55, 9.0),
        (63, 11.0),
        (72, 13.0),
        (82, 14.5),
        (92, 15.0),
    ]
    pts = _score_to_pts(raw, breakpoints, 15.0)

    if raw >= 72:
        reason_codes.append("STAGE2_QUALITY")
    elif raw >= 55:
        reason_codes.append("STAGE2_3_CONSTRUCTIVE")
    elif raw >= 40:
        reason_codes.append("STAGE3_CAUTION")
    else:
        reason_codes.append("STAGE4_AVOID")

    return {
        "raw_score":    round(raw, 1),
        "points":       round(pts, 2),
        "available":    True,
        "status":       "available",
        "stage_label":  stage_label,
        "reason_codes": reason_codes,
    }


def _score_options_alignment(row: dict) -> dict:
    """
    Options Alignment — 15 pts max.

    Reads options_alignment_score (0-100) + pressure_state for nuance.
    confirmed_no_options → status=confirmed_no_options, 0 pts (not a failure).
    not_scanned → status=not_scanned, 0 pts, lowers confidence.
    Foreign-exchange tickers (AIM:, ASX:, LON:, etc.) → confirmed_no_options
      (no US options market; treated as a known state, not a gap).
    """
    sym = str(row.get("symbol") or "").upper()

    # ── Foreign-exchange tickers: no US options market ────────────────────────
    # Detect by exchange prefix.  These are NEVER optionable on US markets so
    # classify immediately as confirmed_no_options — a KNOWN state that earns
    # confidence points rather than a gap that lowers them.
    _FOREIGN_PFX = (
        "AIM:", "ASX:", "CSE:", "EPA:", "ETR:", "FRA:", "KRX:",
        "LON:", "OSL:", "SHA:", "STO:", "SWX:", "TPE:", "TPEX:",
        "TSX:", "TSXV:", "TYO:", "WSE:", "XSAT:", "OTC:",
    )
    if any(sym.startswith(p) for p in _FOREIGN_PFX):
        return {
            "raw_score":                      None,
            "points":                         0,
            "available":                      False,
            "status":                         "confirmed_no_options",
            "options_pressure_state":         "confirmed_no_options_non_us_exchange",
            "net_premium_score":              None,
            "net_premium_acceleration_score": None,
            "reason_codes":                   ["NON_US_EXCHANGE", "CONFIRMED_NO_OPTIONS"],
        }
    # ─────────────────────────────────────────────────────────────────────────

    raw_score  = row.get("options_alignment_score")
    available  = bool(row.get("options_alignment_available")) and raw_score is not None
    pressure   = str(row.get("options_pressure_state") or "").lower()
    reason_codes: list[str] = []

    # Determine options coverage status
    if "no_options" in pressure or "confirmed_no" in pressure:
        reason_codes.append("CONFIRMED_NO_OPTIONS")
        return {
            "raw_score":                    None,
            "points":                       0,
            "available":                    False,
            "status":                       "confirmed_no_options",
            "options_pressure_state":       row.get("options_pressure_state"),
            "net_premium_score":            None,
            "net_premium_acceleration_score": None,
            "reason_codes":                 reason_codes,
        }

    if not available or raw_score is None:
        reason_codes.append("OPTIONS_NOT_SCANNED")
        return {
            "raw_score":                    None,
            "points":                       0,
            "available":                    False,
            "status":                       "not_scanned",
            "options_pressure_state":       row.get("options_pressure_state"),
            "net_premium_score":            None,
            "net_premium_acceleration_score": None,
            "reason_codes":                 reason_codes,
        }

    raw = _safe_float(raw_score)

    # Options pressure bonus: bullish pressure lifts score slightly
    pressure_boost = 0.0
    if "strong_bullish" in pressure or "heavy_call" in pressure:
        pressure_boost = 5.0
        reason_codes.append("OPTIONS_STRONG_BULLISH_PRESSURE")
    elif "bullish" in pressure or "call" in pressure:
        pressure_boost = 2.0
        reason_codes.append("OPTIONS_BULLISH_PRESSURE")
    elif "bearish" in pressure or "put" in pressure:
        pressure_boost = -3.0
        reason_codes.append("OPTIONS_BEARISH_PRESSURE")

    effective_raw = _clamp(raw + pressure_boost)

    breakpoints = [
        (0,  0.0),
        (20, 1.5),
        (35, 4.0),
        (50, 7.0),
        (60, 9.0),
        (70, 11.0),
        (80, 13.0),
        (90, 14.5),
        (97, 15.0),
    ]
    pts = _score_to_pts(effective_raw, breakpoints, 15.0)

    if effective_raw >= 75:
        reason_codes.append("OPTIONS_STRONGLY_ALIGNED")
    elif effective_raw >= 55:
        reason_codes.append("OPTIONS_ALIGNED")
    elif effective_raw >= 40:
        reason_codes.append("OPTIONS_NEUTRAL")
    else:
        reason_codes.append("OPTIONS_WEAK")

    # Derive richer status from the snapshot metadata propagated from options_alignment.py
    _snap_status = row.get("options_snapshot_status") or ""
    if "lkg_market_closed" in _snap_status or row.get("_options_source") == "disk_lkg":
        _opts_status = "lkg_market_closed"
    elif "stale" in _snap_status:
        _opts_status = "stale_but_usable"
    elif "cached" in _snap_status or _snap_status in ("available_cached",):
        _opts_status = "available_cached"
    else:
        _opts_status = "available_live"

    return {
        "raw_score":                    round(raw, 1),
        "points":                       round(pts, 2),
        "available":                    True,
        "status":                       _opts_status,
        "options_snapshot_status":      _snap_status or None,
        "options_as_of":                row.get("options_as_of"),
        "options_lkg_age_hours":        row.get("options_lkg_age_hours"),
        "options_pressure_state":       row.get("options_pressure_state"),
        "net_premium_score":            raw,
        "net_premium_acceleration_score": None,
        "reason_codes":                 reason_codes,
    }


def _score_entry_risk_reward(row: dict) -> dict:
    """
    Entry Risk/Reward — 15 pts max.

    Key overrides:
      - major_llc → 1 pt max (confirmed structural break)
      - chase_extension (no constructive) → capped at 8 pts
      - constructive_extension + HIGH_TIGHT_FLAG/BULL_FLAG/BREAKOUT_SHELF →
          score STRONGLY (10-15 pts), regardless of entry_rr_score.
          entry_rr_score being low is expected when extended; the PATTERN
          quality defines the entry quality for these setups.
    """
    raw_score    = row.get("entry_risk_reward_score")
    pattern      = row.get("pattern_type") or "NO_PATTERN"
    pat_score    = _safe_float(row.get("pattern_score"), 0)
    constructive = bool(row.get("constructive_extension"))
    chase        = bool(row.get("chase_extension"))
    ext_quality  = row.get("extension_quality") or "NORMAL"
    major_llc    = bool(row.get("major_lower_low_confirmed"))
    asst_status  = row.get("active_support_status") or ""
    reason_codes: list[str] = []

    # Valid pattern families
    VALID_PATTERNS = {
        "HIGH_TIGHT_FLAG", "BULL_FLAG", "BREAKOUT_SHELF", "VCP",
        "STAGE2_BREAKOUT", "BASE_BOTTOM", "CUP_HANDLE",
        "CUP_AND_HANDLE", "BREAKOUT_RETEST", "EMA_PULLBACK",
        "20DMA_PULLBACK", "30DMA_PULLBACK", "50DMA_PULLBACK",
        "200DMA_RECLAIM", "SUPPORT_BOUNCE", "LEADER_PULLBACK",
        "LOW_BASE_REVERSAL", "WAVE_CONTINUATION_PROXY",
    }
    # Tier-1 patterns: flag/shelf structures — constructive ext is EXPECTED for these
    TIER1_CONSTRUCTIVE = {"HIGH_TIGHT_FLAG", "BULL_FLAG", "BREAKOUT_SHELF", "VCP"}
    has_valid_pattern = pattern in VALID_PATTERNS

    # Hard gates first
    if major_llc:
        reason_codes.append("MAJOR_LLC_STRUCTURAL_BREAK")
        pts = 1.0 if (raw_score or 0) > 40 else 0.0
        return {
            "raw_score":                raw_score,
            "points":                   pts,
            "available":                raw_score is not None,
            "status":                   "structural_break",
            "entry_state":              row.get("entry_state"),
            "pattern_type":             pattern,
            "extension_quality":        ext_quality,
            "active_support_status":    asst_status,
            "critical_break_level":     row.get("critical_break_level"),
            "breakout_trigger":         row.get("pattern_breakout_trigger"),
            "reason_codes":             reason_codes,
        }

    if raw_score is None and not (constructive and has_valid_pattern):
        reason_codes.append("ENTRY_RR_UNAVAILABLE")
        return {
            "raw_score":                None,
            "points":                   0,
            "available":                False,
            "status":                   "missing_cache",
            "entry_state":              row.get("entry_state"),
            "pattern_type":             pattern,
            "extension_quality":        ext_quality,
            "active_support_status":    asst_status,
            "critical_break_level":     row.get("critical_break_level"),
            "breakout_trigger":         row.get("pattern_breakout_trigger"),
            "reason_codes":             reason_codes,
        }

    raw = _safe_float(raw_score, 0)

    # ── Determine scoring mode ────────────────────────────────────────────────
    # Phase 1.5 hardening: TIER1 constructive override requires all gates to pass:
    #   1. pattern_score >= 55 (minimum quality — 0 means no score from engine)
    #   2. active_support_status not in structural breakdown set
    #   3. minor lower_low not confirmed (major already handled above)
    # Shelf quality then caps pts:
    #   SHELF_CONFIRMED (current_shelf_support is not None):  full 15 pts
    #   SHELF_NOT_CONFIRMED_ESTIMATED (dist_active <= 20):    capped at 12 pts
    #   SHELF_ABSENT_WIDE_EXTENSION (dist_active > 20):       capped at 10 pts
    _tier1_constructive = False
    _shelf_quality: Optional[str] = None

    if constructive and pattern in TIER1_CONSTRUCTIVE:
        _constructive_denied = False
        if pat_score > 0 and pat_score < 55:
            reason_codes.append("CONSTRUCTIVE_OVERRIDE_DENIED_WEAK_PATTERN")
            _constructive_denied = True
        elif asst_status in ("support_lost", "breakdown", "major_breakdown"):
            reason_codes.append("CONSTRUCTIVE_OVERRIDE_DENIED_SUPPORT_LOST")
            _constructive_denied = True
        elif bool(row.get("lower_low_confirmed")):
            reason_codes.append("CONSTRUCTIVE_OVERRIDE_DENIED_MINOR_LLC")
            _constructive_denied = True
        if not _constructive_denied:
            _tier1_constructive = True

    if _tier1_constructive:
        effective_pat = pat_score if pat_score > 0 else 70.0
        has_shelf   = row.get("current_shelf_support") is not None
        dist_active = _safe_float(row.get("distance_to_active_support_pct"), 0)
        if has_shelf:
            _shelf_quality = "confirmed"
            max_pts_c = 15.0
            reason_codes.append("SHELF_CONFIRMED")
        elif 0 < dist_active <= 20.0:
            _shelf_quality = "estimated"
            max_pts_c = 12.0
            reason_codes.append("SHELF_NOT_CONFIRMED_ESTIMATED")
        else:
            _shelf_quality = "absent"
            max_pts_c = 10.0
            reason_codes.append("SHELF_ABSENT_WIDE_EXTENSION")
        breakpoints = [(0, 9.0), (50, 11.0), (65, 12.0), (80, 13.5), (92, 15.0)]
        pts = min(max_pts_c, _score_to_pts(effective_pat, breakpoints, 15.0))
        reason_codes.append(f"CONSTRUCTIVE_{pattern}_STRONG_ENTRY")

    # ── CONSTRUCTIVE + OTHER VALID PATTERN ───────────────────────────────────
    elif constructive and has_valid_pattern:
        breakpoints = [
            (0,  6.0),
            (40, 8.0),
            (60, 10.5),
            (75, 12.5),
            (88, 14.0),
            (95, 15.0),
        ]
        pts = _score_to_pts(raw, breakpoints, 15.0)
        reason_codes.append(f"CONSTRUCTIVE_{pattern}")

    # ── CHASE → cap at 8 pts ────────────────────────────────────────────────
    elif chase and not constructive:
        breakpoints = [
            (0,  0.0),
            (40, 2.0),
            (60, 5.0),
            (75, 8.0),
        ]
        pts = min(8.0, _score_to_pts(raw, breakpoints, 8.0))
        reason_codes.append("CHASE_EXTENSION_CAPPED")

    # ── VALID PATTERN, no extension override ────────────────────────────────
    elif has_valid_pattern:
        breakpoints = [
            (0,  2.0),
            (40, 5.5),
            (60, 8.5),
            (70, 11.0),
            (80, 13.0),
            (90, 14.5),
            (97, 15.0),
        ]
        pts = _score_to_pts(raw, breakpoints, 15.0)
        reason_codes.append(f"PATTERN_{pattern}")

    # ── No pattern — score from raw RR ──────────────────────────────────────
    else:
        breakpoints = [
            (0,  0.0),
            (30, 2.5),
            (50, 5.5),
            (65, 8.5),
            (78, 11.0),
            (88, 13.0),
            (95, 14.0),
        ]
        pts = _score_to_pts(raw, breakpoints, 14.0)
        reason_codes.append("NO_CLEAR_PATTERN")

    # Support intact bonus
    if asst_status in ("at_support", "above_support") and pts > 5:
        pts = min(15.0, pts + 0.5)
        reason_codes.append("SUPPORT_INTACT_BONUS")

    # ── Shelf detection proof fields ─────────────────────────────────────────
    _shelf_status_out = (
        "confirmed"                if _shelf_quality == "confirmed"
        else "estimated_support_based" if _shelf_quality == "estimated"
        else "absent_wide_extension"   if _shelf_quality == "absent"
        else "absent"
    )
    _shelf_lb      = row.get("current_shelf_support")
    _shelf_source  = (
        "current_shelf_support_field"           if _shelf_lb is not None
        else "distance_to_active_support_estimate" if _shelf_quality == "estimated"
        else None
    )
    _shelf_rng_pct = (
        _safe_float(row.get("distance_to_active_support_pct"), 0)
        if _shelf_quality is not None else None
    )

    return {
        "raw_score":                round(raw, 1),
        "points":                   round(pts, 2),
        "available":                True,
        "status":                   "available",
        "entry_state":              row.get("entry_state"),
        "pattern_type":             pattern,
        "extension_quality":        ext_quality,
        "shelf_quality":            _shelf_quality,
        "shelf_status":             _shelf_status_out,
        "current_shelf_lower_bound":  _shelf_lb,
        "current_shelf_upper_bound":  None,
        "current_shelf_source":       _shelf_source,
        "current_shelf_touch_count":  None,
        "current_shelf_duration_bars": None,
        "current_shelf_range_pct":    _shelf_rng_pct,
        "active_support_status":    asst_status,
        "critical_break_level":     row.get("critical_break_level"),
        "breakout_trigger":         row.get("pattern_breakout_trigger"),
        "reason_codes":             reason_codes,
    }


def _score_catalyst_alignment(row: dict) -> dict:
    """
    Catalyst Alignment — 15 pts max.

    Theme Policy is NOT included here — it belongs in the overlay layer.
    Catalysts that are only theme_policy_event are treated as 0 pts in this component.
    """
    raw_score     = row.get("catalyst_alignment_score")
    available     = bool(row.get("catalyst_alignment_available")) and raw_score is not None
    detail_status = row.get("catalyst_detail_status") or "no_catalyst"
    cat_source    = row.get("catalyst_primary_source") or ""
    bearish_conf  = bool(row.get("catalyst_bearish_conflict"))
    reason_codes: list[str] = []

    # If catalyst is theme_policy only → 0 pts here (goes to overlay)
    is_theme_policy_only = (
        "theme_policy" in cat_source.lower()
        or detail_status == "theme_policy_event"
    ) and detail_status not in ("scheduled_event", "rss_event")

    if is_theme_policy_only and not (row.get("catalyst_scheduled_event") or row.get("catalyst_rss_event")):
        reason_codes.append("THEME_POLICY_MOVED_TO_OVERLAY")
        return {
            "raw_score":            raw_score,
            "points":               0,
            "available":            False,
            "status":               "theme_policy_overlay",
            "detail_status":        detail_status,
            "catalyst_primary_event": row.get("catalyst_primary_event"),
            "catalyst_rss_event":     row.get("catalyst_rss_event"),
            "catalyst_scheduled_event": row.get("catalyst_scheduled_event"),
            "bearish_conflict":     bearish_conf,
            "reason_codes":         reason_codes,
        }

    # no_catalyst is a KNOWN empty state — the catalyst service ran and found nothing.
    # Sentinel: catalyst_alignment_available is explicitly False (set by V2 service after
    # running) AND raw_score is None.  We cannot use catalyst_detail_status for this
    # because that field carries the TRADE ALIGNMENT detail state ("neutral",
    # "moderate_tailwind", etc.), not the catalyst component's own status string.
    _cat_avail_field = row.get("catalyst_alignment_available")
    if not available and _cat_avail_field is False and raw_score is None:
        reason_codes.append("NO_ACTIVE_CATALYST")
        return {
            "raw_score":              None,
            "points":                 0,
            "available":              False,
            "status":                 "no_catalyst",
            "detail_status":          detail_status,
            "catalyst_primary_event": row.get("catalyst_primary_event"),
            "catalyst_rss_event":     row.get("catalyst_rss_event"),
            "catalyst_scheduled_event": row.get("catalyst_scheduled_event"),
            "bearish_conflict":       bearish_conf,
            "reason_codes":           reason_codes,
        }

    if not available or raw_score is None:
        reason_codes.append("CATALYST_UNAVAILABLE")
        return {
            "raw_score":            None,
            "points":               0,
            "available":            False,
            "status":               "missing_cache",
            "detail_status":        detail_status,
            "catalyst_primary_event": row.get("catalyst_primary_event"),
            "catalyst_rss_event":     row.get("catalyst_rss_event"),
            "catalyst_scheduled_event": row.get("catalyst_scheduled_event"),
            "bearish_conflict":     bearish_conf,
            "reason_codes":         reason_codes,
        }

    raw = _safe_float(raw_score)

    # Detail-status-adjusted breakpoints
    if detail_status == "scheduled_event":
        # Scheduled event: strong catalyst — score up to full 15
        breakpoints = [
            (0,  2.0),
            (40, 5.0),
            (60, 9.0),
            (75, 12.0),
            (85, 14.0),
            (95, 15.0),
        ]
        reason_codes.append("SCHEDULED_CATALYST")
    elif detail_status == "rss_event":
        # RSS/news event: moderate catalyst
        breakpoints = [
            (0,  1.0),
            (40, 4.0),
            (55, 7.0),
            (70, 10.0),
            (82, 12.0),
            (92, 13.5),
        ]
        reason_codes.append("RSS_CATALYST")
    elif detail_status == "score_only_missing_event":
        # Score present but no identifiable event
        breakpoints = [
            (0,  0.0),
            (50, 3.0),
            (65, 6.0),
            (80, 9.0),
        ]
        reason_codes.append("SCORE_ONLY_NO_EVENT")
    else:
        # No catalyst
        breakpoints = [
            (0,  0.0),
            (60, 2.0),
        ]
        reason_codes.append("NO_IDENTIFIED_CATALYST")

    pts = _score_to_pts(raw, breakpoints, 15.0)

    # Bearish conflict: expose but do not automatically zero — reduce
    if bearish_conf:
        pts = max(0.0, pts * 0.4)
        reason_codes.append("BEARISH_CATALYST_CONFLICT")

    return {
        "raw_score":            round(raw, 1),
        "points":               round(pts, 2),
        "available":            True,
        "status":               "available",
        "detail_status":        detail_status,
        "catalyst_primary_event": row.get("catalyst_primary_event"),
        "catalyst_rss_event":     row.get("catalyst_rss_event"),
        "catalyst_scheduled_event": row.get("catalyst_scheduled_event"),
        "bearish_conflict":     bearish_conf,
        "reason_codes":         reason_codes,
    }


def _score_investment_alignment(row: dict) -> dict:
    """
    Investment Alignment — 10 pts max (intentionally lower weight).
    """
    raw_score = row.get("investment_alignment_score")
    available = bool(row.get("investment_alignment_available")) and raw_score is not None
    inv_label = row.get("investment_quality_label")
    unavail_reason = row.get("investment_unavailable_reason")
    reason_codes: list[str] = []

    if not available or raw_score is None:
        reason_codes.append("INVESTMENT_UNAVAILABLE")
        return {
            "raw_score":                    None,
            "points":                       0,
            "available":                    False,
            "status":                       "missing_cache",
            "investment_quality_label":     inv_label,
            "investment_unavailable_reason": unavail_reason,
            "reason_codes":                 reason_codes,
        }

    raw = _safe_float(raw_score)

    breakpoints = [
        (0,  0.0),
        (35, 1.0),
        (45, 2.5),
        (55, 4.0),
        (65, 6.0),
        (75, 7.5),
        (82, 8.5),
        (88, 9.5),
        (95, 10.0),
    ]
    pts = _score_to_pts(raw, breakpoints, 10.0)

    if raw >= 85:
        reason_codes.append("HIGHEST_INVESTMENT_QUALITY")
    elif raw >= 70:
        reason_codes.append("STRONG_INVESTMENT_QUALITY")
    elif raw >= 55:
        reason_codes.append("MODERATE_INVESTMENT_QUALITY")
    else:
        reason_codes.append("WEAK_INVESTMENT_QUALITY")

    return {
        "raw_score":                    round(raw, 1),
        "points":                       round(pts, 2),
        "available":                    True,
        "status":                       "available",
        "investment_quality_label":     inv_label,
        "investment_unavailable_reason": None,
        "reason_codes":                 reason_codes,
    }


# ── Bonus Scorers ──────────────────────────────────────────────────────────────

def _score_social_bonus_v4(row: dict, social_map: dict) -> dict:
    """
    Social Bonus V4 — max 15 pts (re-scaled from existing 10-pt system).

    Social CONFIRMS a strong setup; it does not define one.
    Reads from social_map (x_consensus_weekly.json _backend_ranked)
    OR existing social_fields already in the snapshot row.
    """
    sym = str(row.get("symbol") or "").upper()
    social_entry = social_map.get(sym)
    social_snap  = row.get("social_fields") or {}
    reason_codes: list[str] = []

    # Read social signal — prefer live social_map over cached snapshot
    if social_entry:
        backend_score = _safe_float(social_entry.get("backend_score"))
        has_top       = bool(social_entry.get("has_top_conviction"))
        breadth       = _safe_float(social_entry.get("breadth_score"), 1.0)
        freshness     = _safe_float(social_entry.get("freshness_score"), 1.0)
        covered       = True
    elif social_snap.get("covered"):
        backend_score = _safe_float(social_snap.get("backend_score"))
        has_top       = bool(social_snap.get("has_top_conviction"))
        breadth       = _safe_float(social_snap.get("breadth_score"), 1.0)
        freshness     = _safe_float(social_snap.get("freshness_score"), 1.0)
        covered       = True
    else:
        reason_codes.append("NO_SOCIAL_COVERAGE")
        return {
            "points":               0,
            "available":            False,
            "status":               "no_social_coverage",
            "social_signal_source": "x_consensus",
            "backend_score":        None,
            "reason_codes":         reason_codes,
        }

    # Blocking gate: chase extension or confirmed structural break
    chase    = bool(row.get("chase_extension"))
    maj_llc  = bool(row.get("major_lower_low_confirmed"))
    ent_grade = row.get("entry_grade") or ""
    if ent_grade == "AVOID" or maj_llc or chase:
        reason_codes.append("SOCIAL_BONUS_BLOCKED_BAD_ENTRY")
        return {
            "points":               0,
            "available":            True,
            "status":               "blocked_bad_entry",
            "social_signal_source": "x_consensus",
            "backend_score":        round(backend_score, 3),
            "reason_codes":         reason_codes,
        }

    # Weak social → no bonus
    if backend_score < 3.0:
        reason_codes.append("SOCIAL_TOO_WEAK")
        return {
            "points":               0,
            "available":            True,
            "status":               "weak_social",
            "social_signal_source": "x_consensus",
            "backend_score":        round(backend_score, 3),
            "reason_codes":         reason_codes,
        }

    # Scale from 0-10 (existing) → 0-15 (V4)
    # VERY_STRONG (12-15): score >= 9 + top conviction
    # STRONG (7-11): score >= 5
    # MODERATE (3-6): score >= 3
    if backend_score >= 12.0 and has_top and breadth >= 1.3:
        pts = min(15.0, 12.0 + (backend_score - 12.0) * 1.5 * freshness)
        reason_codes.append("SOCIAL_VERY_STRONG_FRESH")
    elif backend_score >= 9.0 and has_top:
        pts = 12.0 + (backend_score - 9.0) * 1.0
        pts = min(15.0, pts)
        reason_codes.append("SOCIAL_VERY_STRONG")
    elif backend_score >= 6.0 and has_top:
        pts = 7.0 + (backend_score - 6.0) * 1.333
        pts = min(11.0, pts)
        reason_codes.append("SOCIAL_STRONG_WITH_CONVICTION")
    elif backend_score >= 5.0:
        pts = 7.0 + (backend_score - 5.0) * 0.5
        pts = min(9.0, pts)
        reason_codes.append("SOCIAL_STRONG")
    elif backend_score >= 4.0:
        pts = 5.0 + (backend_score - 4.0)
        reason_codes.append("SOCIAL_MODERATE_HIGH")
    else:
        pts = 3.0 + (backend_score - 3.0)
        pts = max(3.0, min(5.0, pts))
        reason_codes.append("SOCIAL_MODERATE")

    return {
        "points":               round(pts, 2),
        "available":            True,
        "status":               "available",
        "social_signal_source": "x_consensus",
        "backend_score":        round(backend_score, 3),
        "reason_codes":         reason_codes,
    }


def _score_theme_policy_bonus(row: dict) -> dict:
    """
    Theme Policy Bonus — max 2.5 pts (overlay layer).

    Theme Policy is a THEME-level catalyst, not a company-specific catalyst.
    Moved here from the catalyst component to avoid double-counting.
    """
    boost     = _safe_float(row.get("theme_policy_boost"))
    available = bool(row.get("theme_policy_available")) and boost > 0
    event     = row.get("theme_policy_event")
    theme     = row.get("theme_policy_theme")
    reason_codes: list[str] = []

    if not available or boost <= 0:
        reason_codes.append("NO_THEME_POLICY")
        return {
            "points":                   0,
            "available":                False,
            "status":                   "no_policy",
            "theme_policy_event":       event,
            "theme_policy_theme":       theme,
            "theme_policy_reason_codes": reason_codes,
        }

    # boost is 0-1 scale from catalyst_alignment; map to 0-2.5 pts
    pts = round(_clamp(boost, 0.0, 1.0) * 2.5, 2)
    reason_codes.append("THEME_POLICY_ACTIVE")

    return {
        "points":                   pts,
        "available":                True,
        "status":                   "available",
        "theme_policy_event":       event,
        "theme_policy_theme":       theme,
        "theme_policy_reason_codes": reason_codes,
    }


def _score_prediction_market_bonus(row: dict) -> dict:
    """
    Prediction Market Bonus — max 2.5 pts (overlay layer).

    Uses existing odds LKG (file-based). Theme-level mapping.
    If LKG has no active families, returns 0 pts (not_available).
    """
    import pathlib, json
    reason_codes: list[str] = []

    try:
        lkg_path = pathlib.Path(__file__).parent.parent / "data" / "predict_odds_live_lkg.json"
        if not lkg_path.exists():
            raise FileNotFoundError("LKG not found")
        raw = json.loads(lkg_path.read_text())
        # LKG currently stores metadata only (updated_at, scanned_at, cache_age_seconds)
        # Family snapshots would be in a different structure when available
        families = raw.get("families") or raw.get("results") or []
        if not families:
            reason_codes.append("PREDICT_MARKET_LKG_NO_FAMILIES")
            return {
                "points":                           0,
                "available":                        False,
                "status":                           "not_available",
                "prediction_market_event":          None,
                "prediction_market_theme_mapping":  None,
                "prediction_market_reason_codes":   reason_codes,
            }
        # TODO Phase 2: map ticker's theme to family probabilities
        reason_codes.append("PREDICT_MARKET_NOT_MAPPED_YET")
        return {
            "points":                           0,
            "available":                        False,
            "status":                           "not_mapped",
            "prediction_market_event":          None,
            "prediction_market_theme_mapping":  None,
            "prediction_market_reason_codes":   reason_codes,
        }
    except Exception:
        reason_codes.append("PREDICT_MARKET_LKG_UNAVAILABLE")
        return {
            "points":                           0,
            "available":                        False,
            "status":                           "not_available",
            "prediction_market_event":          None,
            "prediction_market_theme_mapping":  None,
            "prediction_market_reason_codes":   reason_codes,
        }


def _score_whale_insider_bonus(row: dict) -> dict:
    """
    Whale / Insider / Politician Bonus — max 5 pts (overlay layer).

    Phase 1: not wired (service is async/Neon; no sync file cache available).
    Returns 0 pts with status=not_wired_phase1.
    Phase 2: wire sync LKG or read from a pre-computed file cache.
    """
    return {
        "points":                   0,
        "available":                False,
        "status":                   "not_wired_phase1",
        "whale_insider_signal_type": None,
        "whale_insider_reason_codes": ["WHALE_INSIDER_NOT_WIRED_PHASE1"],
    }


def _score_bottleneck_bonus(row: dict, bottleneck_map: dict) -> dict:
    """
    Bottleneck / Supply Chain Bonus — max 5 pts (overlay layer).

    anchor_count * 3 / 39 * 5, capped at 5.
    Uses curated_anchor_bottlenecks.get_multi_anchor_screener() (sync, cached).
    """
    sym = str(row.get("symbol") or "").upper()
    reason_codes: list[str] = []
    entry = bottleneck_map.get(sym)

    if not entry:
        reason_codes.append("NOT_IN_BOTTLENECK_SCREENER")
        return {
            "points":                   0,
            "available":                False,
            "status":                   "not_in_screener",
            "bottleneck_anchor_count":  0,
            "bottleneck_raw_anchor_points": 0,
            "bottleneck_anchor_names":  [],
            "bottleneck_reason_codes":  reason_codes,
        }

    anchor_count  = int(entry.get("anchor_count") or 0)
    anchor_names  = entry.get("anchor_names") or []
    raw_pts       = anchor_count * 3
    pts           = round(min(5.0, raw_pts / 39.0 * 5.0), 2)
    reason_codes.append(f"BOTTLENECK_{anchor_count}_ANCHORS")

    return {
        "points":                   pts,
        "available":                True,
        "status":                   "available",
        "bottleneck_anchor_count":  anchor_count,
        "bottleneck_raw_anchor_points": raw_pts,
        "bottleneck_anchor_names":  anchor_names,
        "bottleneck_reason_codes":  reason_codes,
    }


# ── Bucket + Actionability ─────────────────────────────────────────────────────

_SUPPORT_INTACT = {"above_support", "at_support", "near_support"}


def _assign_v4_bucket(
    normalized_total: float,
    total_score: float,
    core_score: float,
    major_llc: bool,
    minor_llc: bool,
    chase: bool,
    constructive: bool,
    act_state: str,
    rr_state: str,
    asst_status: str,
    invest_available: bool,
    invest_score: Optional[float],
    pattern: str,
    ext_quality: str,
    entry_pts: float,
    invest_pts: float,
    confidence: float = 100.0,
) -> str:
    """
    V4 Bucket assignment.

    Uses `normalized_total` (core normalised to available-component max, then
    re-scaled to 0-115 range) for threshold comparisons so that missing
    options / catalyst do not unfairly push strong setups into NO_CLEAR.

    Hard gates (bonuses never override):
      major_llc OR (AVOID + support lost)        → RISK_CONFLICT
      chase_extension (not constructive)          → WATCH_FOR_RESET

    Score tiers (normalized_total):
      >= 80, entry_pts >= 10                      → ACTIONABLE
      >= 62, entry_pts >= 4                       → NEAR_ACTIONABLE
      >= 50, support intact, entry_pts >= 2       → CONFLUENCE_AT_SUPPORT
      invest >= 80, normalized >= 42              → INVESTMENT_QUALITY
      normalized >= 62, weak invest               → SPECULATIVE_TRADE
      normalized >= 42 or chase+constructive      → WATCH_FOR_RESET
      else                                        → NO_CLEAR_CONFLUENCE
    """
    # ── Hard risk gate: only confirmed structural break ───────────────────────
    # AVOID actionability alone is not sufficient — it could reflect extension,
    # pre-earnings, or other non-structural reasons. Only a major LLC (price
    # breaking the primary structural support level) is truly RISK_CONFLICT.
    if major_llc:
        return "RISK_CONFLICT"

    # ── Chase → WATCH_FOR_RESET ────────────────────────────────────────────
    if chase and not constructive:
        return "WATCH_FOR_RESET"

    # ── ACTIONABLE ────────────────────────────────────────────────────────────
    # Confidence >= 55 required: blocks low-coverage symbols from ACTIONABLE
    if normalized_total >= 72 and entry_pts >= 10 and not chase and confidence >= 55:
        return "ACTIONABLE"

    # ── NEAR_ACTIONABLE ───────────────────────────────────────────────────────
    # Confidence >= 45 required: very low coverage is a coverage gap, not a setup
    if normalized_total >= 55 and entry_pts >= 4 and not chase and confidence >= 45:
        return "NEAR_ACTIONABLE"

    # ── CONFLUENCE_AT_SUPPORT ─────────────────────────────────────────────────
    if normalized_total >= 50 and asst_status in _SUPPORT_INTACT and entry_pts >= 2:
        return "CONFLUENCE_AT_SUPPORT"

    # ── NEAR_ACTIONABLE (softer — decent normalized, no specific support req) ─
    if normalized_total >= 65 and entry_pts >= 3 and confidence >= 45:
        return "NEAR_ACTIONABLE"

    # ── INVESTMENT_QUALITY ────────────────────────────────────────────────────
    if invest_available and invest_score is not None and invest_score >= 80 and normalized_total >= 40:
        return "INVESTMENT_QUALITY"

    # ── SPECULATIVE_TRADE ─────────────────────────────────────────────────────
    if normalized_total >= 62 and (not invest_available or (invest_score or 0) < 50):
        return "SPECULATIVE_TRADE"

    # ── WATCH_FOR_RESET: chase with constructive OR decent score needing reset ─
    if (chase and constructive) or normalized_total >= 52:
        return "WATCH_FOR_RESET"

    # ── NO_CLEAR_CONFLUENCE ───────────────────────────────────────────────────
    return "NO_CLEAR_CONFLUENCE"


def _derive_actionability_v4(
    total_score: float,
    bucket: str,
    major_llc: bool,
    minor_llc: bool,
    chase: bool,
    constructive: bool,
    pattern: str,
    ext_quality: str,
    act_state: str,
    rr_state: str,
    asst_status: str,
    entry_rr_score: Optional[float],
    entry_score: Optional[float],
    confidence: float = 100.0,
) -> str:
    """
    Actionability V4 — decision layer, not a second score.

    Answers: "What should I do with this ticker now?"

    NOT a weighted score. Uses V4 Confluence + entry/pattern/support/risk gates.
    """
    rr  = _safe_float(entry_rr_score)
    ent = _safe_float(entry_score)
    SHELF_PATTERNS = {"BREAKOUT_SHELF", "VCP", "BASE_BOTTOM", "CUP_HANDLE", "CUP_AND_HANDLE"}
    support_intact = asst_status in _SUPPORT_INTACT

    # ── AVOID: hard structural break ──────────────────────────────────────────
    if major_llc or bucket == "RISK_CONFLICT":
        return "AVOID"

    # ── WATCH_FOR_RESET: chase or highly extended without pattern ─────────────
    if chase and not constructive:
        return "WATCH_FOR_RESET"

    # ── READY: high total + good entry + valid pattern + confidence guard ───────
    # confidence >= 70 required: READY must not fire for low-coverage setups
    if total_score >= 85 and rr >= 65 and bucket in ("ACTIONABLE", "NEAR_ACTIONABLE") and confidence >= 70:
        return "READY"

    # ── WAIT_FOR_BREAKOUT: shelf/VCP patterns near breakout trigger ───────────
    if pattern in SHELF_PATTERNS and total_score >= 62 and not chase:
        return "WAIT_FOR_BREAKOUT"

    # ── NEAR_ACTIONABLE: strong setup, entry close ────────────────────────────
    if total_score >= 72 and rr >= 50 and not chase:
        return "NEAR_ACTIONABLE"

    # ── WAIT_FOR_RETEST: at support, good total but needs confirmation ─────────
    if total_score >= 58 and support_intact and rr >= 40:
        return "WAIT_FOR_RETEST"

    # ── NEAR_ACTIONABLE (softer) ──────────────────────────────────────────────
    if bucket == "NEAR_ACTIONABLE":
        return "NEAR_ACTIONABLE"

    # ── WATCH_FOR_RESET: chase with constructive pattern (wait for base) ──────
    if bucket == "WATCH_FOR_RESET":
        return "WATCH_FOR_RESET"

    # ── WATCH: everything else — monitor, no clear action ────────────────────
    return "WATCH"


def _compute_v4_confidence(
    components: dict,
    social_available: bool,
    shelf_confirmed: bool = True,
    used_constructive_tier1: bool = False,
) -> float:
    """
    V4 confidence: % of components available, with structural penalties.

    Max 100. Each of 6 core components = 14 pts. Social = 8 pts.
    Max = 6*14 + 8 = 92 → normalised to 100.

    Structural penalties (deducted from earned before normalising):
      used_constructive_tier1 AND NOT shelf_confirmed: -6 pts
        Entry quality is estimated from extension pattern alone, not
        anchored to a confirmed shelf/consolidation zone.
    """
    comp_weights = {
        "theme_alignment":      14,
        "stage_quality":        14,
        "options_alignment":    14,
        "entry_risk_reward":    14,
        "catalyst_alignment":   14,
        "investment_alignment": 14,
    }
    # Statuses that represent a KNOWN empty state — the service ran and found
    # nothing, which is informative data.  These still earn confidence points
    # because the system has determined (not just "not checked") the answer.
    # missing_cache / not_scanned do NOT earn points: we simply don't know yet.
    _KNOWN_STATE_STATUSES = {
        "confirmed_no_options",  # Tradier confirmed no listed options
        "no_catalyst",           # catalyst service ran: no active event found
        "lkg_market_closed",     # disk LKG present, market is closed (weekend)
        "available_cached",      # supplement/LKG cache hit
        "available_live",        # fresh scan data
        "stale_but_usable",      # stale LKG data serving as weekend/off-hours fallback
    }

    total_possible = sum(comp_weights.values()) + 8  # 8 for social = 92
    earned = sum(
        w for k, w in comp_weights.items()
        if (
            (components.get(k) or {}).get("available")
            or (components.get(k) or {}).get("status") in _KNOWN_STATE_STATUSES
        )
    )
    if social_available:
        earned += 8
    if used_constructive_tier1 and not shelf_confirmed:
        earned = max(0, earned - 6)
    return round(earned / total_possible * 100, 1)


# ── Main Entry Point ──────────────────────────────────────────────────────────

def compute_confluence_v4(
    snapshot_row: dict,
    social_map: Optional[dict] = None,
    bottleneck_map: Optional[dict] = None,
) -> dict:
    """
    Compute V4 Unified Confluence Score from existing snapshot row fields.

    Pure read — zero provider calls, zero LLM calls.

    Args:
        snapshot_row:    Full confluence snapshot row for one symbol.
        social_map:      {SYM: social_entry} from x_consensus_weekly.json.
                         Pass None to use snapshot social_fields only.
        bottleneck_map:  {SYM: {anchor_count, anchor_names}}.
                         Pass None to auto-load from curated_anchor_bottlenecks.

    Returns full dict of caelyn_confluence_v4_* fields.
    """
    if social_map is None:
        social_map = {}
    if bottleneck_map is None:
        bottleneck_map = _get_bottleneck_map()

    sym = str(snapshot_row.get("symbol") or "").upper()

    # ── Core components ────────────────────────────────────────────────────────
    theme_comp   = _score_theme_alignment(snapshot_row)
    stage_comp   = _score_stage_quality(snapshot_row)
    opts_comp    = _score_options_alignment(snapshot_row)
    entry_comp   = _score_entry_risk_reward(snapshot_row)
    cat_comp     = _score_catalyst_alignment(snapshot_row)
    invest_comp  = _score_investment_alignment(snapshot_row)

    core_score = round(min(85.0, sum([
        theme_comp["points"],
        stage_comp["points"],
        opts_comp["points"],
        entry_comp["points"],
        cat_comp["points"],
        invest_comp["points"],
    ])), 1)

    # ── Social bonus ───────────────────────────────────────────────────────────
    soc_bonus = _score_social_bonus_v4(snapshot_row, social_map)
    pre_overlay_score = round(min(100.0, core_score + soc_bonus["points"]), 1)

    # ── Overlay bonuses ────────────────────────────────────────────────────────
    tp_bonus  = _score_theme_policy_bonus(snapshot_row)
    pm_bonus  = _score_prediction_market_bonus(snapshot_row)
    wi_bonus  = _score_whale_insider_bonus(snapshot_row)
    bn_bonus  = _score_bottleneck_bonus(snapshot_row, bottleneck_map)

    overlay_bonus = round(min(15.0, sum([
        tp_bonus["points"],
        pm_bonus["points"],
        wi_bonus["points"],
        bn_bonus["points"],
    ])), 1)

    total_score = round(min(115.0, pre_overlay_score + overlay_bonus), 1)
    bonus_score = round(soc_bonus["points"] + overlay_bonus, 1)

    # ── Components dict (spec shape) ──────────────────────────────────────────
    components = {
        "theme_alignment":      theme_comp,
        "stage_quality":        stage_comp,
        "options_alignment":    opts_comp,
        "entry_risk_reward":    entry_comp,
        "catalyst_alignment":   cat_comp,
        "investment_alignment": invest_comp,
    }

    # ── Normalised core score (for bucket assignment) ──────────────────────────
    # When options / catalyst are missing (common), raw core is unfairly deflated.
    # Normalize: (raw_core / max_possible_with_available_components) * 85
    # This preserves relative quality without penalising missing data sources.
    _COMP_MAX_PTS = {
        "theme_alignment":   15.0,
        "stage_quality":     15.0,
        "options_alignment": 15.0,
        "entry_risk_reward": 15.0,
        "catalyst_alignment": 15.0,
        "investment_alignment": 10.0,
    }
    available_max = sum(
        _COMP_MAX_PTS[k] for k, c in components.items() if c.get("available")
    )
    if available_max > 0:
        normalized_core = min(85.0, (core_score / available_max) * 85.0)
    else:
        normalized_core = 0.0

    normalized_total = round(
        min(115.0, min(100.0, normalized_core + soc_bonus["points"]) + overlay_bonus), 1
    )

    # ── Confidence (computed before bucket so bucket can guard on it) ──────────
    # Extract shelf quality from entry component (set by Phase 1.5 hardening)
    _entry_shelf_quality   = entry_comp.get("shelf_quality")
    _entry_shelf_confirmed = _entry_shelf_quality == "confirmed"
    _entry_used_tier1      = any(
        "CONSTRUCTIVE_" in rc and "_STRONG_ENTRY" in rc
        for rc in (entry_comp.get("reason_codes") or [])
    )
    v4_confidence = _compute_v4_confidence(
        components,
        soc_bonus.get("available", False),
        shelf_confirmed=_entry_shelf_confirmed,
        used_constructive_tier1=_entry_used_tier1,
    )

    # ── V4 bucket + actionability ──────────────────────────────────────────────
    v4_bucket = _assign_v4_bucket(
        normalized_total = normalized_total,
        total_score      = total_score,
        core_score       = core_score,
        major_llc    = bool(snapshot_row.get("major_lower_low_confirmed")),
        minor_llc    = bool(snapshot_row.get("minor_lower_low")),
        chase        = bool(snapshot_row.get("chase_extension")),
        constructive = bool(snapshot_row.get("constructive_extension")),
        act_state    = snapshot_row.get("actionability_state") or "",
        rr_state     = snapshot_row.get("entry_risk_reward_state") or "",
        asst_status  = snapshot_row.get("active_support_status") or "",
        invest_available = bool(snapshot_row.get("investment_alignment_available")),
        invest_score     = snapshot_row.get("investment_alignment_score"),
        pattern          = snapshot_row.get("pattern_type") or "NO_PATTERN",
        ext_quality      = snapshot_row.get("extension_quality") or "NORMAL",
        entry_pts        = entry_comp["points"],
        invest_pts       = invest_comp["points"],
        confidence       = v4_confidence,
    )

    v4_actionability = _derive_actionability_v4(
        total_score  = normalized_total,
        bucket       = v4_bucket,
        major_llc    = bool(snapshot_row.get("major_lower_low_confirmed")),
        minor_llc    = bool(snapshot_row.get("minor_lower_low")),
        chase        = bool(snapshot_row.get("chase_extension")),
        constructive = bool(snapshot_row.get("constructive_extension")),
        pattern      = snapshot_row.get("pattern_type") or "NO_PATTERN",
        ext_quality  = snapshot_row.get("extension_quality") or "NORMAL",
        act_state    = snapshot_row.get("actionability_state") or "",
        rr_state     = snapshot_row.get("entry_risk_reward_state") or "",
        asst_status  = snapshot_row.get("active_support_status") or "",
        entry_rr_score = snapshot_row.get("entry_risk_reward_score"),
        entry_score    = snapshot_row.get("entry_score"),
        confidence     = v4_confidence,
    )

    # ── Reason codes (flat union) ──────────────────────────────────────────────
    all_reason_codes: list[str] = []
    for c in components.values():
        all_reason_codes.extend(c.get("reason_codes") or [])
    all_reason_codes.extend(soc_bonus.get("reason_codes") or [])
    all_reason_codes.append(f"V4_BUCKET_{v4_bucket}")
    all_reason_codes.append(f"V4_ACT_{v4_actionability}")
    all_reason_codes.append("TRADE_ALIGNMENT_LEGACY_COMPAT_ONLY")

    # ── Bonus breakdown dict ───────────────────────────────────────────────────
    bonus_breakdown = {
        "social":               soc_bonus,
        "theme_policy":         tp_bonus,
        "prediction_markets":   pm_bonus,
        "whale_insider":        wi_bonus,
        "bottleneck":           bn_bonus,
        "total_overlay_bonus":  overlay_bonus,
        "pre_overlay_score":    pre_overlay_score,
    }

    return {
        # ── V4 primary fields ─────────────────────────────────────────────────
        "caelyn_confluence_v4_score":               total_score,
        "caelyn_confluence_v4_raw_score":           total_score,
        "caelyn_confluence_v4_core_score":          core_score,
        "caelyn_confluence_v4_bonus_score":         bonus_score,
        "caelyn_confluence_v4_total_score":         total_score,
        "caelyn_confluence_v4_normalized_score":    normalized_total,
        "caelyn_confluence_v4_available_max_pts":   round(available_max, 1),
        "caelyn_confluence_v4_bucket":              v4_bucket,
        "caelyn_confluence_v4_components":          components,
        "caelyn_confluence_v4_bonus_breakdown":     bonus_breakdown,
        "caelyn_confluence_v4_reason_codes":        all_reason_codes,
        "caelyn_confluence_v4_confidence_score":    v4_confidence,
        "caelyn_confluence_v4_actionability":       v4_actionability,
        # ── Legacy compat ─────────────────────────────────────────────────────
        "legacy_trade_alignment_score":             snapshot_row.get("trade_alignment_score"),
    }
