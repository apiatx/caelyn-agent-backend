"""
CAELYN CONFLUENCE V4.2 — Core Scoring Semantics Cleanup

Formula:
  core_score    = Theme + Stage + Options + TechSetup + EntryExit + Catalyst + Investment
                  max = 100
  bonus_score   = Social + WhaleInsider + Bottleneck
                  max = 25
  total_score   = min(125, core + bonus)

Core Components (max 100 pts):
  Theme Alignment     15 pts  (theme_policy folded in; no standalone bonus)
  Stage Quality       15 pts  (unchanged from V4)
  Options Alignment   20 pts  (60 net premium + 40 acceleration)
  Technical Setup      8 pts  (pattern quality)
  Entry/Exit Quality  12 pts  (support/RR quality)
  Catalyst Alignment  15 pts  (75 direct + 25 intelligence)
  Investment Align    15 pts  (3-pillar: financial health + current + forward growth)

Bonuses (max 25 pts):
  Social Bonus        up to +15  (3 sections x 5 pts each)
  Whale/Insider       up to +5   (buy_pressure ratio; unavailable = 0)
  Bottleneck          up to +5   (anchor_count / total_anchor_count * 5)

Removed from score:
  Prediction Markets (set to 0 pts, disabled)
  Standalone Theme Policy bonus (folded into Theme Alignment)

Zero LLM calls.  Zero provider calls.  Pure read from existing snapshot + caches.
"""
from __future__ import annotations

import json
import pathlib
import time
from typing import Optional

# ── Bottleneck map cache (1-hour TTL) ─────────────────────────────────────────
_BOTTLENECK_MAP_CACHE: Optional[dict] = None
_BOTTLENECK_MAP_TS:    float = 0.0
_BOTTLENECK_MAP_TTL:   float = 3600.0

_X_CONSENSUS = pathlib.Path(__file__).parent.parent / "data" / "x_consensus_weekly.json"

# ── Default total_anchor_count used by the bottleneck formula ─────────────────
_DEFAULT_TOTAL_ANCHORS = 13


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


# ── Helpers ───────────────────────────────────────────────────────────────────

def _clamp(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


def _safe_float(v, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
    except Exception:
        return default


def _score_to_pts(score: float, breakpoints: list[tuple[float, float]], max_pts: float) -> float:
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


def _parse_pct(v, default: Optional[float] = None) -> Optional[float]:
    """
    Parse a percentage value that may arrive as:
      - string "74.15%" → 74.15
      - float 74.15     → 74.15
      - None            → default
    Does NOT convert ratio-scaled floats (< 1) to percentage; callers
    are responsible for knowing the field's native scale.
    """
    if v is None:
        return default
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if s.endswith("%"):
        try:
            return float(s[:-1])
        except ValueError:
            return default
    try:
        return float(s)
    except ValueError:
        return default


# ── Social sections map builder ───────────────────────────────────────────────

def build_social_sections_map() -> dict[str, dict]:
    """
    Read x_consensus_weekly.json once and return:
        {SYM: {"confluence": bool, "acceleration": bool, "fresh": bool}}

    Section → x_consensus_weekly.json path:
      Social Confluence  →  raw.consensus_picks[*].ticker
      Social Acceleration →  raw.hype_radar[*].key_tickers[*]
      Fresh              →  raw.fresh_trades[*].ticker
    """
    try:
        if not _X_CONSENSUS.exists():
            return {}
        raw_json = json.loads(_X_CONSENSUS.read_text())
        raw = raw_json.get("raw") or {}

        confluence_syms: set[str] = set()
        for entry in raw.get("consensus_picks") or []:
            t = str(entry.get("ticker") or entry.get("symbol") or "").upper()
            if t:
                confluence_syms.add(t)

        acceleration_syms: set[str] = set()
        for entry in raw.get("hype_radar") or []:
            for kt in entry.get("key_tickers") or []:
                t = str(kt).upper().lstrip("$")
                if t:
                    acceleration_syms.add(t)

        fresh_syms: set[str] = set()
        for entry in raw.get("fresh_trades") or []:
            t = str(entry.get("ticker") or entry.get("symbol") or "").upper()
            if t:
                fresh_syms.add(t)

        all_syms = confluence_syms | acceleration_syms | fresh_syms
        return {
            sym: {
                "confluence":   sym in confluence_syms,
                "acceleration": sym in acceleration_syms,
                "fresh":        sym in fresh_syms,
            }
            for sym in all_syms
        }
    except Exception:
        return {}


# ── Component scorers ─────────────────────────────────────────────────────────

def _score_theme_alignment_v42(row: dict) -> dict:
    """
    Theme Alignment — 15 pts max.

    Theme Policy is folded in here (10 % weight) instead of being a
    standalone overlay bonus.

    V4.2 composite formula:
      The existing theme_alignment_score (0-100) already captures RS /
      Stage / Acceleration from the theme_rotation_service bridge.
      We treat it as the 90 % base; theme_policy_boost provides the 10 %.

      theme_raw_v42 = min(100, theme_alignment_score * 0.9 + theme_policy_score * 0.1)
      theme_pts     = theme_raw_v42 / 100 * 15
    """
    raw_score   = row.get("theme_alignment_score")
    available   = bool(row.get("theme_alignment_available")) and raw_score is not None
    breakdown   = row.get("signal_breakdown") or {}
    theme_rot   = breakdown.get("theme_rotation") or {}
    theme_name  = theme_rot.get("primary_rotation_theme") or row.get("theme_policy_theme")
    rot_label   = theme_rot.get("primary_theme_rotation_state") or theme_rot.get("state")
    reason_codes: list[str] = []

    if not available or raw_score is None:
        reason_codes.append("THEME_SIGNAL_UNAVAILABLE")
        return {
            "raw_score":                  None,
            "points":                     0,
            "available":                  False,
            "status":                     "missing_cache",
            "theme_name":                 theme_name,
            "theme_rotation_label":       rot_label,
            "theme_alignment_score":      None,
            "theme_rs_score":             None,
            "theme_stage_score":          None,
            "theme_acceleration_score":   None,
            "theme_policy_alignment_score": None,
            "reason_codes":               reason_codes,
        }

    base = _safe_float(raw_score)

    # Theme policy component (10 %)
    tp_boost        = _safe_float(row.get("theme_policy_boost"), 0.0)
    tp_available    = bool(row.get("theme_policy_available")) and tp_boost > 0
    tp_score        = _clamp(tp_boost, 0.0, 1.0) * 100.0 if tp_available else 0.0

    theme_raw_v42   = min(100.0, base * 0.9 + tp_score * 0.1)
    pts             = round(theme_raw_v42 / 100.0 * 15.0, 2)

    # Infer theme_rank from rotation label / raw score
    if base >= 80:
        reason_codes.append("THEME_LEADING")
        theme_rank = 1
    elif base >= 65:
        reason_codes.append("THEME_STRONG")
        theme_rank = 3
    elif base >= 50:
        reason_codes.append("THEME_EMERGING")
        theme_rank = 6
    elif base >= 35:
        reason_codes.append("THEME_NEUTRAL")
        theme_rank = 10
    else:
        reason_codes.append("THEME_LAGGING")
        theme_rank = 15

    if tp_available:
        reason_codes.append("THEME_POLICY_FOLDED_IN")

    return {
        "raw_score":                  round(theme_raw_v42, 1),
        "points":                     pts,
        "available":                  True,
        "status":                     "available",
        "theme_name":                 theme_name,
        "theme_rotation_label":       rot_label,
        "theme_alignment_score":      round(base, 1),
        "theme_rs_score":             round(base, 1),
        "theme_stage_score":          None,
        "theme_acceleration_score":   None,
        "theme_policy_alignment_score": round(tp_score, 1) if tp_available else None,
        "theme_rank":                 theme_rank,
        "reason_codes":               reason_codes,
    }


def _score_stage_quality_v42(row: dict) -> dict:
    """
    Stage Quality — 15 pts max.  Unchanged from V4.

    formula: stage_quality_points = stage_quality_raw_score / 100 * 15
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

    pts = round(raw / 100.0 * 15.0, 2)

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
        "points":       pts,
        "available":    True,
        "status":       "available",
        "stage_label":  stage_label,
        "stage_quality_raw_score": round(raw, 1),
        "stage_quality_points":    pts,
        "reason_codes": reason_codes,
    }


def _score_options_alignment_v42(row: dict) -> dict:
    """
    Options Alignment — 20 pts max.

    Formula:
      options_raw = 0.60 * net_premium_score + 0.40 * acceleration_score
      options_pts = options_raw / 100 * 20

    Inputs (from options_alignment service merged into snapshot row):
      options_current_composite_normalized  — net premium pressure (0-100)
      options_direction_score               — weighted-delta acceleration (0-100)

    No-options handling:
      confirmed_no_options → 0 pts (known state, not a failure)
      not_scanned          → 0 pts (confidence penalty)
    """
    sym     = str(row.get("symbol") or "").upper()
    pressure = str(row.get("options_pressure_state") or "").lower()
    reason_codes: list[str] = []

    _FOREIGN_PFX = (
        "AIM:", "AMS:", "ASX:", "CSE:", "EPA:", "ETR:", "FRA:", "KRX:",
        "LON:", "OSL:", "SHA:", "STO:", "SWX:", "TPE:", "TPEX:",
        "TSX:", "TSXV:", "TYO:", "WSE:", "XSAT:", "OTC:",
    )
    if any(sym.startswith(p) for p in _FOREIGN_PFX):
        return {
            "raw_score":              None,
            "points":                 0,
            "available":              False,
            "status":                 "confirmed_no_options",
            "options_pressure_score": None,
            "options_acceleration_score": None,
            "options_pressure_state": "confirmed_no_options_non_us_exchange",
            "net_premium":            None,
            "net_premium_1d_delta":   None,
            "net_premium_7d_delta":   None,
            "net_premium_30d_delta":  None,
            "reason_codes":           ["NON_US_EXCHANGE", "CONFIRMED_NO_OPTIONS"],
        }

    if "no_options" in pressure or "confirmed_no" in pressure:
        reason_codes.append("CONFIRMED_NO_OPTIONS")
        return {
            "raw_score":              None,
            "points":                 0,
            "available":              False,
            "status":                 "confirmed_no_options",
            "options_pressure_score": None,
            "options_acceleration_score": None,
            "options_pressure_state": row.get("options_pressure_state"),
            "net_premium":            None,
            "net_premium_1d_delta":   row.get("net_premium_delta_1d"),
            "net_premium_7d_delta":   row.get("net_premium_delta_7d"),
            "net_premium_30d_delta":  row.get("net_premium_delta_30d"),
            "reason_codes":           reason_codes,
        }

    # V4.2.1: The snapshot row stores options_alignment_score (already-weighted composite
    # from options_alignment.py) rather than the raw options_current_composite_normalized.
    # Precedence: options_alignment_score → options_current_composite_normalized → options_current_score
    _opts_nested   = row.get("options") or {}
    _align_score   = row.get("options_alignment_score")
    _np_raw        = (row.get("options_current_composite_normalized")
                      or row.get("options_current_score"))
    dir_score      = row.get("options_direction_score")
    _pressure_flat = row.get("options_pressure_state") or _opts_nested.get("pressure_state")

    _using_align = _align_score is not None
    if _using_align:
        np_norm = _align_score          # already-weighted composite (60/40 baked in internally)
    elif _np_raw is not None:
        np_norm = _np_raw
    else:
        np_norm = None

    # V4.2.1: options_alignment_available is absent from many row sources (mapper, disk LKG).
    # A non-None options_alignment_score is the definitive indicator that the options
    # service produced a valid composite — treat it as sufficient for availability.
    opts_avail = np_norm is not None and (
        bool(row.get("options_alignment_available"))
        or _align_score is not None    # score present → definitively available
    )

    if not opts_avail or np_norm is None:
        reason_codes.append("OPTIONS_NOT_SCANNED")
        return {
            "raw_score":              None,
            "points":                 0,
            "available":              False,
            "status":                 "not_scanned",
            "options_pressure_score": None,
            "options_acceleration_score": None,
            "options_pressure_state": _pressure_flat,
            "net_premium":            None,
            "net_premium_1d_delta":   row.get("net_premium_delta_1d"),
            "net_premium_7d_delta":   row.get("net_premium_delta_7d"),
            "net_premium_30d_delta":  row.get("net_premium_delta_30d"),
            "reason_codes":           reason_codes,
        }

    np_val = _safe_float(np_norm)

    if _using_align:
        # options_alignment_score already incorporates direction weighting internally;
        # use it as the 0-100 signal directly → scaled to 20 pts max.
        opts_raw = np_val
        dir_val  = None
        reason_codes.append("OPTIONS_USING_ALIGNMENT_SCORE_COMPOSITE")
    else:
        # Raw net-premium path: apply 60/40 if direction available, else full NP weight
        if dir_score is not None:
            dir_val  = _safe_float(dir_score, 0.0)
            opts_raw = 0.60 * np_val + 0.40 * dir_val
        else:
            dir_val  = None
            opts_raw = np_val
            reason_codes.append("OPTIONS_DIRECTION_UNAVAILABLE_FULL_NP_WEIGHT")

    opts_pts = round(min(20.0, opts_raw / 100.0 * 20.0), 2)

    if np_val >= 70:
        reason_codes.append("OPTIONS_STRONGLY_BULLISH")
    elif np_val >= 50:
        reason_codes.append("OPTIONS_BULLISH")
    elif np_val <= 30:
        reason_codes.append("OPTIONS_BEARISH")
    else:
        reason_codes.append("OPTIONS_NEUTRAL")

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
        "raw_score":               round(opts_raw, 1),
        "points":                  opts_pts,
        "available":               True,
        "status":                  _opts_status,
        "options_pressure_score":  round(np_val, 1),
        "options_acceleration_score": round(dir_val, 1) if dir_val is not None else None,
        "options_snapshot_status": _snap_status or None,
        "options_as_of":           row.get("options_as_of"),
        "options_lkg_age_hours":   row.get("options_lkg_age_hours"),
        "options_pressure_state":  row.get("options_pressure_state"),
        "net_premium":             row.get("net_premium"),
        "net_premium_1d_delta":    row.get("net_premium_delta_1d"),
        "net_premium_7d_delta":    row.get("net_premium_delta_7d"),
        "net_premium_30d_delta":   row.get("net_premium_delta_30d"),
        "reason_codes":            reason_codes,
    }


# ── Pattern registry for Technical Setup ─────────────────────────────────────
_PATTERN_BASE_SCORES: dict[str, float] = {
    "HIGH_TIGHT_FLAG":         95.0,
    "BULL_FLAG":               88.0,
    "BREAKOUT_SHELF":          85.0,
    "VCP":                     85.0,
    "CUP_HANDLE":              85.0,
    "CUP_AND_HANDLE":          85.0,
    "STAGE2_BREAKOUT":         83.0,
    "EMA_PULLBACK":            78.0,
    "20DMA_PULLBACK":          75.0,
    "30DMA_PULLBACK":          75.0,
    "50DMA_PULLBACK":          72.0,
    "SUPPORT_BOUNCE":          75.0,
    "LEADER_PULLBACK":         72.0,
    "BREAKOUT_RETEST":         70.0,
    "LOW_BASE_REVERSAL":       68.0,
    "WAVE_CONTINUATION_PROXY": 65.0,
    "BASE_BOTTOM":             65.0,
    "200DMA_RECLAIM":          60.0,
}
_VALID_PATTERNS       = set(_PATTERN_BASE_SCORES.keys())
_TIER1_CONSTRUCTIVE   = {"HIGH_TIGHT_FLAG", "BULL_FLAG", "BREAKOUT_SHELF", "VCP"}
_SHELF_PATTERNS       = {"BREAKOUT_SHELF", "VCP", "BASE_BOTTOM", "CUP_HANDLE", "CUP_AND_HANDLE"}
_SUPPORT_INTACT_STATUSES = {"above_support", "at_support", "near_support"}


def _score_technical_setup_v42(row: dict) -> dict:
    """
    Technical Setup — 8 pts max.

    technical_setup_points = technical_setup_raw_score / 100 * 8

    Inputs: pattern_type, pattern_score, constructive_extension,
            chase_extension, active_support_status,
            major_lower_low_confirmed, lower_low_confirmed.
    """
    pattern      = row.get("pattern_type") or "NO_PATTERN"
    pat_score    = _safe_float(row.get("pattern_score"), 0.0)
    constructive = bool(row.get("constructive_extension"))
    chase        = bool(row.get("chase_extension"))
    major_llc    = bool(row.get("major_lower_low_confirmed"))
    minor_llc    = bool(row.get("lower_low_confirmed"))
    asst_status  = str(row.get("active_support_status") or "").lower()
    reason_codes: list[str] = []

    # Hard gate: major structural break
    if major_llc:
        reason_codes.append("MAJOR_LLC_CONFIRMED_BREAKDOWN")
        return {
            "raw_score":              10.0,
            "points":                 round(10.0 / 100.0 * 8.0, 2),
            "available":              True,
            "status":                 "structural_break",
            "technical_setup_label":  "Confirmed Breakdown",
            "technical_setup_direction": "bearish",
            "reason_codes":           reason_codes,
        }

    has_valid  = pattern in _VALID_PATTERNS
    base_score = _PATTERN_BASE_SCORES.get(pattern, 0.0)

    # Use pattern_score when available and > 0 to refine quality
    if has_valid and pat_score > 0:
        quality = (base_score + pat_score) / 2.0
    elif has_valid:
        quality = base_score
    else:
        quality = 30.0  # no clear pattern

    # Modifiers
    if constructive and has_valid:
        quality = min(100.0, quality + 8.0)
        reason_codes.append(f"CONSTRUCTIVE_{pattern}")
    if asst_status in _SUPPORT_INTACT_STATUSES:
        quality = min(100.0, quality + 5.0)
        reason_codes.append("SUPPORT_INTACT_BONUS")
    if chase and not constructive:
        quality = max(0.0, quality - 22.0)
        reason_codes.append("CHASE_EXTENSION_PENALTY")
    if minor_llc:
        quality = max(0.0, quality - 5.0)
        reason_codes.append("MINOR_LLC_PENALTY")
    if asst_status in ("support_lost", "breakdown", "major_breakdown"):
        quality = max(0.0, quality - 15.0)
        reason_codes.append("SUPPORT_LOST_PENALTY")

    quality = _clamp(quality, 0.0, 100.0)
    pts     = round(quality / 100.0 * 8.0, 2)

    # Label
    if pattern in _PATTERN_BASE_SCORES:
        label_map = {
            "HIGH_TIGHT_FLAG":   "High-Tight Continuation",
            "BULL_FLAG":         "Bull Flag",
            "BREAKOUT_SHELF":    "Breakout Shelf",
            "VCP":               "VCP",
            "CUP_HANDLE":        "Cup & Handle",
            "CUP_AND_HANDLE":    "Cup & Handle",
            "STAGE2_BREAKOUT":   "Stage 2 Breakout",
            "EMA_PULLBACK":      "EMA Pullback",
            "20DMA_PULLBACK":    "20D MA Pullback",
            "30DMA_PULLBACK":    "30D MA Pullback",
            "50DMA_PULLBACK":    "50D MA Pullback",
            "SUPPORT_BOUNCE":    "Support Bounce",
            "LEADER_PULLBACK":   "Leader Pullback",
            "BREAKOUT_RETEST":   "Breakout Retest",
            "LOW_BASE_REVERSAL": "Low-Base Reversal",
            "BASE_BOTTOM":       "Base Bottom",
            "200DMA_RECLAIM":    "200D MA Reclaim",
            "WAVE_CONTINUATION_PROXY": "Wave Continuation",
        }
        setup_label = label_map.get(pattern, pattern)
    elif chase and not constructive:
        setup_label = "Chase Extension"
    else:
        setup_label = "No Clear Setup"

    direction = "bullish" if quality >= 50 else ("neutral" if quality >= 30 else "bearish")
    if has_valid:
        reason_codes.append(f"PATTERN_{pattern}")
    else:
        reason_codes.append("NO_CLEAR_PATTERN")

    return {
        "raw_score":                quality,
        "points":                   pts,
        "available":                True,
        "status":                   "available",
        "technical_setup_raw_score": round(quality, 1),
        "technical_setup_points":    pts,
        "technical_setup_label":     setup_label,
        "technical_setup_direction": direction,
        "pattern_type":              pattern,
        "reason_codes":              reason_codes,
    }


def _score_entry_exit_v42(row: dict) -> dict:
    """
    Entry/Exit Quality — 12 pts max.

    Formula:
      entry_exit_raw = 0.35 * support_score + 0.65 * rr_score
      entry_exit_pts = entry_exit_raw / 100 * 12

    Inputs: entry_risk_reward_score, active_support_status,
            distance_to_active_support_pct, chase_extension,
            major_lower_low_confirmed, critical_break_level,
            pattern_breakout_trigger.
    """
    rr_score     = row.get("entry_risk_reward_score")
    asst_status  = str(row.get("active_support_status") or "").lower()
    dist_active  = _safe_float(row.get("distance_to_active_support_pct"), 50.0)
    chase        = bool(row.get("chase_extension"))
    constructive = bool(row.get("constructive_extension"))
    major_llc    = bool(row.get("major_lower_low_confirmed"))
    entry_state  = row.get("entry_state") or ""
    reason_codes: list[str] = []

    # Hard gate: structural break
    if major_llc:
        reason_codes.append("MAJOR_LLC_ENTRY_BLOCKED")
        pts = round(max(0.0, _safe_float(rr_score, 0.0) / 100.0 * 4.0), 2)
        return {
            "raw_score":           10.0,
            "points":              pts,
            "available":           bool(rr_score is not None),
            "status":              "structural_break",
            "entry_exit_status":   "structural_break",
            "entry_exit_raw_score": 10.0,
            "entry_exit_points":   pts,
            "nearest_support":     row.get("current_shelf_support"),
            "nearest_resistance":  None,
            "entry_zone":          None,
            "breakout_trigger":    row.get("pattern_breakout_trigger"),
            "reclaim_level":       None,
            "critical_break_level": row.get("critical_break_level"),
            "invalidation_level":  row.get("critical_break_level"),
            "target_1":            None,
            "target_2":            None,
            "risk_reward_ratio":   None,
            "entry_exit_reason_codes": reason_codes,
            "reason_codes":        reason_codes,
        }

    if rr_score is None and not (constructive and (row.get("pattern_type") or "") in _VALID_PATTERNS):
        reason_codes.append("ENTRY_RR_UNAVAILABLE")
        return {
            "raw_score":           None,
            "points":              0,
            "available":           False,
            "status":              "missing_cache",
            "entry_exit_status":   "missing_cache",
            "entry_exit_raw_score": None,
            "entry_exit_points":   0,
            "nearest_support":     None,
            "nearest_resistance":  None,
            "entry_zone":          None,
            "breakout_trigger":    row.get("pattern_breakout_trigger"),
            "reclaim_level":       None,
            "critical_break_level": row.get("critical_break_level"),
            "invalidation_level":  None,
            "target_1":            None,
            "target_2":            None,
            "risk_reward_ratio":   None,
            "entry_exit_reason_codes": reason_codes,
            "reason_codes":        reason_codes,
        }

    rr_val = _safe_float(rr_score, 50.0)

    # Support proximity score
    if asst_status == "above_support":
        support_score = 95.0
        reason_codes.append("ABOVE_SUPPORT")
    elif asst_status == "at_support":
        support_score = 85.0
        reason_codes.append("AT_SUPPORT")
    elif asst_status == "near_support":
        support_score = max(50.0, 80.0 - dist_active * 1.5)
        reason_codes.append("NEAR_SUPPORT")
    elif asst_status == "below_support":
        support_score = 30.0
        reason_codes.append("BELOW_SUPPORT")
    elif asst_status in ("support_lost", "breakdown"):
        support_score = 10.0
        reason_codes.append("SUPPORT_LOST")
    elif asst_status == "major_breakdown":
        support_score = 0.0
        reason_codes.append("MAJOR_BREAKDOWN")
    else:
        support_score = 50.0  # neutral / unknown

    # Chase extension penalty
    if chase and not constructive:
        rr_val        = min(rr_val, 40.0)
        support_score = min(support_score, 40.0)
        reason_codes.append("CHASE_EXTENSION_ENTRY_PENALTY")

    entry_raw = 0.35 * support_score + 0.65 * rr_val
    entry_raw = _clamp(entry_raw, 0.0, 100.0)
    pts       = round(min(12.0, entry_raw / 100.0 * 12.0), 2)

    if entry_raw >= 75:
        entry_status = "great_entry"
    elif entry_raw >= 55:
        entry_status = "near_entry"
    elif entry_raw >= 35:
        entry_status = "wait"
    else:
        entry_status = "poor_entry"

    return {
        "raw_score":           round(entry_raw, 1),
        "points":              pts,
        "available":           True,
        "status":              "available",
        "entry_exit_status":   entry_status,
        "entry_exit_raw_score": round(entry_raw, 1),
        "entry_exit_points":   pts,
        "nearest_support":     row.get("current_shelf_support"),
        "nearest_resistance":  None,
        "entry_zone":          None,
        "breakout_trigger":    row.get("pattern_breakout_trigger"),
        "reclaim_level":       None,
        "critical_break_level": row.get("critical_break_level"),
        "invalidation_level":  row.get("critical_break_level"),
        "target_1":            None,
        "target_2":            None,
        "risk_reward_ratio":   None,
        "entry_exit_reason_codes": reason_codes,
        "reason_codes":        reason_codes,
    }


_DIRECT_CATALYST_EVENT_TYPES: frozenset = frozenset({
    "earnings", "fda", "regulatory", "biotech", "clinical", "trial",
    "ipo", "contract", "deal", "order", "award", "government",
    "launch", "product_launch", "investor_day", "conference",
    "strategic_partnership", "partnership", "merger", "acquisition",
    "split", "milestone", "technical_milestone", "presidential",
    "executive_order", "policy_announcement", "analyst_upgrade",
    "analyst_day", "guidance", "buyback", "restructuring", "spinoff",
})


def _event_is_direct_catalyst(evt: object) -> tuple[bool, str]:
    """Return (is_direct, event_type) for a catalyst event dict."""
    if not isinstance(evt, dict):
        return False, ""
    direction  = str(evt.get("direction") or "").lower()
    if direction == "bearish":
        return False, ""
    event_type = str(evt.get("event_type") or evt.get("type") or "").lower()
    for kw in _DIRECT_CATALYST_EVENT_TYPES:
        if kw in event_type:
            return True, event_type
    # High-materiality non-bearish event → also counts
    try:
        if float(evt.get("materiality_score") or 0) >= 0.5:
            return True, event_type or "high_materiality"
    except (TypeError, ValueError):
        pass
    return False, ""


def _score_catalyst_alignment_v42(row: dict) -> dict:
    """
    Catalyst Alignment — 15 pts max.

    Formula:
      catalyst_raw   = direct_catalyst_score * 0.75 + intelligence_score * 0.25
      catalyst_pts   = catalyst_raw / 100 * 15

    Direct Catalyst (75%):
      Any non-bearish qualified catalyst event present → score = 100
      No event → score = 0
      Bearish conflict → direct = 0, sets flag

    Catalyst Intelligence (25%):
      Uses news volume / change data. Unavailable in snapshot = 0.

    V4.2.1: extended event detection — checks flat fields AND nested catalyst
    dict AND primary_catalyst / catalyst_v2_primary_event / catalyst_events.
    SCORE_ONLY branch uses cat_score directly (no -25 deduction), threshold ≥ 40.
    """
    # ── Resolve all catalyst fields — flat AND nested dict ─────────────────
    _cat_nested   = row.get("catalyst") or {}
    cat_score     = row.get("catalyst_alignment_score") or _cat_nested.get("score")
    _avail_flat   = row.get("catalyst_alignment_available")
    cat_available = bool(_avail_flat if _avail_flat is not None else _cat_nested.get("available"))

    detail_status = (row.get("catalyst_detail_status")
                     or _cat_nested.get("detail_status") or "")

    # Bearish conflict
    bearish_conf = bool(row.get("catalyst_bearish_conflict")
                        or _cat_nested.get("bearish_conflict"))

    # Event candidates — priority order for direct detection
    _ev_scheduled = row.get("catalyst_scheduled_event") or _cat_nested.get("scheduled_event")
    _ev_rss       = row.get("catalyst_rss_event")       or _cat_nested.get("rss_event")
    _ev_primary   = (row.get("primary_catalyst")
                     or _cat_nested.get("primary_event")
                     or row.get("catalyst_primary_event"))
    _ev_v2        = (row.get("catalyst_v2_primary_event")
                     or _cat_nested.get("v2_primary_event"))
    _ev_list      = row.get("catalyst_events") or _cat_nested.get("events") or []

    has_scheduled  = bool(_ev_scheduled)
    has_rss        = bool(_ev_rss)
    reason_codes: list[str] = []

    # ── Determine direct catalyst presence ─────────────────────────────────
    direct_score   = 0.0
    direct_present = False
    direct_type    = None
    detected_event = None

    if bearish_conf:
        reason_codes.append("BEARISH_CATALYST_CONFLICT")

    elif has_scheduled:
        is_direct, etype = _event_is_direct_catalyst(_ev_scheduled)
        if is_direct or True:   # scheduled events are always direct
            direct_score   = 100.0
            direct_present = True
            direct_type    = "scheduled_event"
            detected_event = _ev_scheduled
            reason_codes.append("SCHEDULED_CATALYST_DIRECT")

    elif has_rss:
        is_direct, etype = _event_is_direct_catalyst(_ev_rss)
        if is_direct or True:   # RSS events already gated at catalyst service
            direct_score   = 100.0
            direct_present = True
            direct_type    = "rss_event"
            detected_event = _ev_rss
            reason_codes.append("RSS_CATALYST_DIRECT")

    else:
        # V4.2.1: scan additional event fields for qualified direct catalysts
        _candidates = [
            ("primary_catalyst",  _ev_primary),
            ("v2_primary_event",  _ev_v2),
        ]
        for _evlist_item in (_ev_list[:3] if isinstance(_ev_list, list) else []):
            _candidates.append(("catalyst_events", _evlist_item))

        for src, evt in _candidates:
            is_direct, etype = _event_is_direct_catalyst(evt)
            if is_direct:
                direct_score   = 100.0
                direct_present = True
                direct_type    = etype or src
                detected_event = evt
                reason_codes.append(f"DIRECT_CATALYST_FROM_{src.upper()}")
                break

        if not direct_present:
            if cat_available and cat_score is not None:
                # V4.2.1: use score directly (no -25 deduction); threshold ≥ 40
                raw_val       = _safe_float(cat_score, 0.0)
                direct_score  = raw_val
                direct_present = raw_val >= 40.0
                if direct_present:
                    direct_type = "score_based"
                    reason_codes.append("CATALYST_SCORE_BASED_DIRECT")
                else:
                    reason_codes.append("CATALYST_SCORE_BELOW_THRESHOLD")
            elif _avail_flat is False and cat_score is None:
                reason_codes.append("NO_ACTIVE_CATALYST")
            else:
                reason_codes.append("CATALYST_CACHE_MISSING")

    # ── Catalyst Intelligence (25 %) — unavailable in snapshot ─────────────
    intelligence_score  = 0.0
    intelligence_status = "unavailable"
    reason_codes.append("CATALYST_INTELLIGENCE_UNAVAILABLE")

    # ── Combined ────────────────────────────────────────────────────────────
    catalyst_raw = direct_score * 0.75 + intelligence_score * 0.25
    catalyst_pts = round(min(15.0, catalyst_raw / 100.0 * 15.0), 2)

    # ── Overall status ──────────────────────────────────────────────────────
    _any_signal = cat_available or has_scheduled or has_rss or bool(detected_event)
    if not _any_signal and cat_score is None:
        cat_status = "no_catalyst" if _avail_flat is False else "missing_cache"
    else:
        cat_status = "available"

    return {
        "raw_score":                   round(catalyst_raw, 1),
        "points":                      catalyst_pts,
        "available":                   _any_signal,
        "status":                      cat_status,
        "direct_catalyst_score":       round(direct_score, 1),
        "direct_catalyst_present":     direct_present,
        "direct_catalyst_type":        direct_type,
        "direct_catalyst_event":       detected_event or _ev_primary,
        "direct_catalyst_polarity":    "bearish" if bearish_conf else ("bullish" if direct_present else None),
        "catalyst_intelligence_score": intelligence_score,
        "news_volume_market_cap_48h":  None,
        "news_volume_market_cap_score": None,
        "news_change_48h_pct":         None,
        "news_change_48h_score":       None,
        "catalyst_intelligence_status": intelligence_status,
        "catalyst_intelligence_reason_codes": ["CATALYST_INTELLIGENCE_UNAVAILABLE"],
        "catalyst_alignment_raw_score": round(catalyst_raw, 1),
        "catalyst_alignment_points":    catalyst_pts,
        "catalyst_status":              cat_status,
        "catalyst_detail_status":       detail_status,
        "catalyst_bearish_conflict":    bearish_conf,
        "catalyst_reason_codes":        reason_codes,
        "reason_codes":                 reason_codes,
    }


def _compute_investment_pillars(fields: dict) -> dict:
    """
    Compute the 3 investment pillars from raw fundamentals fields.
    Returns a dict with pillar results.
    """

    def _pct(key: str) -> Optional[float]:
        return _parse_pct(fields.get(key))

    def _num(key: str) -> Optional[float]:
        v = fields.get(key)
        if v is None:
            return None
        try:
            return float(str(v).replace("%", "").replace(",", "").strip())
        except (ValueError, TypeError):
            return None

    # ── Pillar 1: Financial Health ─────────────────────────────────────────
    fh_checks: list[tuple[str, bool]] = []

    gm = _pct("Gross Margin")
    if gm is not None:
        fh_checks.append(("gross_margin_35", gm >= 35.0))

    fcf_m = _pct("FCF Margin")
    if fcf_m is not None:
        fh_checks.append(("fcf_margin_positive", fcf_m > 0.0))

    fcf_abs = _num("Free Cash Flow")
    if fcf_abs is not None:
        fh_checks.append(("free_cf_positive", fcf_abs > 0.0))

    op_inc = _num("Operating Income")
    if op_inc is not None:
        fh_checks.append(("op_income_positive", op_inc > 0.0))

    ebit = _num("EBIT")
    if ebit is not None:
        fh_checks.append(("ebit_positive", ebit > 0.0))

    de = _num("Debt / Equity")
    if de is not None:
        fh_checks.append(("de_below_1_5", de < 1.5))
    else:
        fh_checks.append(("de_unavailable_pass", True))

    nd_ebitda = _num("Net Debt / EBITDA")
    if nd_ebitda is not None:
        fh_checks.append(("nd_ebitda_below_3", nd_ebitda < 3.0))
    else:
        fh_checks.append(("nd_ebitda_unavailable_pass", True))

    fh_passed = sum(1 for _, p in fh_checks if p)
    fh_total  = len(fh_checks)
    fh_strong = fh_total > 0 and fh_passed >= 4
    fh_rcs    = [f"FH_{c}" for c, p in fh_checks if p]
    if not fh_strong and fh_rcs:
        fh_rcs.append(f"FH_{fh_passed}_OF_{fh_total}_CHECKS")

    # ── Pillar 2: Current Growth ───────────────────────────────────────────
    cg_checks: list[tuple[str, bool]] = []

    rev_q = _pct("Revenue Growth (Q)")
    if rev_q is not None:
        cg_checks.append(("rev_growth_q_20", rev_q >= 20.0))

    rev_y = _pct("Revenue Growth (YoY)")
    if rev_y is not None:
        cg_checks.append(("rev_growth_y_15", rev_y >= 15.0))

    eps_g = _pct("EPS Growth")
    eps_tq = _pct("EPS Growth This Quarter")
    if eps_g is not None or eps_tq is not None:
        eps_val = max(v for v in [eps_g, eps_tq] if v is not None)
        cg_checks.append(("eps_growth_15", eps_val >= 15.0))

    if fcf_m is not None:
        cg_checks.append(("fcf_margin_positive_cg", fcf_m > 0.0))

    if op_inc is not None or ebit is not None:
        prof = max(v for v in [op_inc, ebit] if v is not None)
        cg_checks.append(("profitability_positive", prof > 0.0))

    cg_passed = sum(1 for _, p in cg_checks if p)
    cg_total  = len(cg_checks)
    cg_strong = cg_total > 0 and cg_passed >= 3
    cg_rcs    = [f"CG_{c}" for c, p in cg_checks if p]

    # ── Pillar 3: Forward Growth ───────────────────────────────────────────
    fwd_checks: list[tuple[str, bool]] = []

    # V4.2.1: field names verified against Neon fundamentals cache.
    # "Rev Growth Next Quarter" is the primary forward revenue field.
    # Additional aliases cover alternative FMP naming conventions.
    rev_nq = (
        _pct("Rev Growth Next Quarter")
        or _pct("Revenue Growth Next Quarter")
        or _pct("Rev Growth NQ")
        or _pct("Rev Growth (NQ)")
    )
    if rev_nq is not None:
        fwd_checks.append(("rev_nq_20", rev_nq >= 20.0))

    rev_est = (
        _pct("Revenue Growth Est")
        or _pct("Rev Growth Est")
        or _pct("Revenue Growth (Est)")
        or _pct("Rev Growth (Est)")
    )
    if rev_est is not None:
        fwd_checks.append(("rev_est_20", rev_est >= 20.0))

    rev_ny = (
        _pct("Revenue Growth Next Year")
        or _pct("Rev Growth Next Year")
        or _pct("Rev Growth NY")
        or _pct("Revenue Growth (NY)")
        or _pct("Rev Growth (NY)")
    )
    if rev_ny is not None:
        fwd_checks.append(("rev_ny_15", rev_ny >= 15.0))

    eps_est = (
        _pct("EPS Growth Est")
        or _pct("EPS Growth NQ")
        or _pct("EPS Growth NY")
        or _pct("EPS Growth Next Year")
        or _pct("EPS Growth Next Quarter")
        or _pct("EPS Growth (Est)")
        or _pct("EPS Growth This Year")
    )
    if eps_est is not None:
        fwd_checks.append(("eps_fwd_20", eps_est >= 20.0))

    fwd_passed = sum(1 for _, p in fwd_checks if p)
    fwd_total  = len(fwd_checks)

    # Special speculative-growth rule: extreme forward revenue → strong
    _fwd_rev_extreme = (
        (rev_nq is not None and rev_nq >= 40.0)
        or (rev_est is not None and rev_est >= 40.0)
        or (rev_ny is not None and rev_ny >= 40.0)
    )
    fwd_strong = (fwd_total > 0 and fwd_passed >= 2) or _fwd_rev_extreme
    fwd_rcs    = [f"FWD_{c}" for c, p in fwd_checks if p]
    if _fwd_rev_extreme:
        fwd_rcs.append("FWD_SPECULATIVE_EXTREME_REVENUE")

    # ── Continuous 0–100 pillar scores (V4.2.2) ───────────────────────────────

    # Financial Health (max 100)
    _fh_pts = 0.0
    if gm is not None:
        if gm >= 60:      _fh_pts += 20
        elif gm >= 40:    _fh_pts += 14
        elif gm >= 25:    _fh_pts += 8
        elif gm >= 10:    _fh_pts += 3
    if fcf_m is not None:
        if fcf_m >= 20:   _fh_pts += 20
        elif fcf_m >= 10: _fh_pts += 14
        elif fcf_m >= 5:  _fh_pts += 8
        elif fcf_m > 0:   _fh_pts += 4
    if fcf_abs is not None and fcf_abs > 0:
        _fh_pts += 15
    if op_inc is not None and op_inc > 0:
        _fh_pts += 15
    if ebit is not None and ebit > 0:
        _fh_pts += 10
    if de is not None:
        if de < 0.5:      _fh_pts += 10
        elif de < 1.0:    _fh_pts += 7
        elif de < 1.5:    _fh_pts += 4
    else:
        _fh_pts += 5  # unavailable = neutral
    if nd_ebitda is not None:
        if nd_ebitda < 1.0:   _fh_pts += 10
        elif nd_ebitda < 2.0: _fh_pts += 7
        elif nd_ebitda < 3.0: _fh_pts += 4
    else:
        _fh_pts += 5  # unavailable = neutral
    fh_score_0_100 = int(min(100, round(_fh_pts)))

    # Current Growth (max 100)
    _cg_pts = 0.0
    if rev_q is not None:
        if rev_q >= 40:   _cg_pts += 25
        elif rev_q >= 20: _cg_pts += 18
        elif rev_q >= 10: _cg_pts += 12
        elif rev_q > 0:   _cg_pts += 6
    if rev_y is not None:
        if rev_y >= 30:   _cg_pts += 20
        elif rev_y >= 15: _cg_pts += 14
        elif rev_y >= 5:  _cg_pts += 8
        elif rev_y > 0:   _cg_pts += 3
    _eps_cg: Optional[float] = None
    if eps_g is not None or eps_tq is not None:
        _eps_cg = max(v for v in [eps_g, eps_tq] if v is not None)
    if _eps_cg is not None:
        if _eps_cg >= 40:   _cg_pts += 20
        elif _eps_cg >= 15: _cg_pts += 14
        elif _eps_cg >= 0:  _cg_pts += 6
    _op_pos  = op_inc is not None and op_inc > 0
    _ebt_pos = ebit   is not None and ebit   > 0
    if _op_pos and _ebt_pos:  _cg_pts += 20
    elif _op_pos or _ebt_pos: _cg_pts += 12
    if fcf_m is not None:
        if fcf_m >= 10:   _cg_pts += 15
        elif fcf_m > 0:   _cg_pts += 8
    cg_score_0_100 = int(min(100, round(_cg_pts)))

    # Forward Growth (max 100)
    _fwd_pts = 0.0
    if rev_est is not None:
        if rev_est >= 40:   _fwd_pts += 25
        elif rev_est >= 20: _fwd_pts += 18
        elif rev_est >= 10: _fwd_pts += 12
        elif rev_est > 0:   _fwd_pts += 6
    if rev_nq is not None:
        if rev_nq >= 40:    _fwd_pts += 25
        elif rev_nq >= 20:  _fwd_pts += 18
        elif rev_nq >= 10:  _fwd_pts += 12
        elif rev_nq > 0:    _fwd_pts += 6
    if rev_ny is not None:
        if rev_ny >= 30:    _fwd_pts += 20
        elif rev_ny >= 15:  _fwd_pts += 14
        elif rev_ny >= 5:   _fwd_pts += 8
        elif rev_ny > 0:    _fwd_pts += 3
    if eps_est is not None:
        if eps_est >= 40:   _fwd_pts += 20
        elif eps_est >= 20: _fwd_pts += 14
        elif eps_est >= 0:  _fwd_pts += 6
    _fwd_hyper = any(v is not None and v >= 60 for v in [rev_nq, rev_est, rev_ny])
    _fwd_ext   = any(v is not None and v >= 40 for v in [rev_nq, rev_est, rev_ny])
    if _fwd_hyper:     _fwd_pts += 10
    elif _fwd_ext:     _fwd_pts += 6
    fwd_score_0_100 = int(min(100, round(_fwd_pts)))

    return {
        "financial_health_strong":        fh_strong,
        "financial_health_checks_passed": fh_passed,
        "financial_health_checks_total":  fh_total,
        "financial_health_reason_codes":  fh_rcs,
        "current_growth_strong":          cg_strong,
        "current_growth_checks_passed":   cg_passed,
        "current_growth_checks_total":    cg_total,
        "current_growth_reason_codes":    cg_rcs,
        "forward_growth_strong":          fwd_strong,
        "forward_growth_checks_passed":   fwd_passed,
        "forward_growth_checks_total":    fwd_total,
        "forward_growth_reason_codes":    fwd_rcs,
        "financial_health_score_0_100":   fh_score_0_100,
        "current_growth_score_0_100":     cg_score_0_100,
        "forward_growth_score_0_100":     fwd_score_0_100,
    }


def _score_investment_alignment_v42(
    row: dict,
    fundamentals_map: Optional[dict] = None,
) -> dict:
    """
    Investment Alignment — 15 pts max.  3-pillar model.

    investment_alignment_points = strong_pillar_count * 5

    Pillars:
      1. Financial Health  (4/7 checks)
      2. Current Growth    (3/5 checks)
      3. Forward Growth    (2/4 checks)

    Data source: watchlist_fundamentals_cache via fundamentals_map.
    Zero provider calls.
    """
    sym = str(row.get("symbol") or "").upper()
    reason_codes: list[str] = []

    # Locate fundamentals snapshot
    fund_snap: Optional[dict] = None
    if fundamentals_map:
        fund_snap = fundamentals_map.get(sym)
    if fund_snap is None:
        try:
            from data.watchlist_fundamentals_store import get_snapshot as _get_snap
            fund_snap = _get_snap(sym)
        except Exception:
            pass

    _NO_FUND_DEFAULTS = {
        "financial_health_score_0_100":   0,
        "current_growth_score_0_100":     0,
        "forward_growth_score_0_100":     0,
        "investment_quality_score":       0,
        "investment_quality_rank_label":  "Weak / No Clear Investment Case",
        "investment_quality_reason_codes": [],
    }

    if not fund_snap:
        reason_codes.append("INVESTMENT_NO_FUNDAMENTALS_SNAPSHOT")
        return {
            "raw_score":                      None,
            "points":                         0,
            "available":                      False,
            "status":                         "missing_cache",
            "investment_pillar_count":        0,
            "investment_quality_label":       "No Clear Investment Case",
            "financial_health_strong":        False,
            "financial_health_checks_passed": 0,
            "financial_health_checks_total":  0,
            "financial_health_reason_codes":  [],
            "current_growth_strong":          False,
            "current_growth_checks_passed":   0,
            "current_growth_checks_total":    0,
            "current_growth_reason_codes":    [],
            "forward_growth_strong":          False,
            "forward_growth_checks_passed":   0,
            "forward_growth_checks_total":    0,
            "forward_growth_reason_codes":    [],
            "investment_alignment_raw_score": None,
            "investment_alignment_points":    0,
            "investment_reason_codes":        reason_codes,
            "reason_codes":                   reason_codes,
            **_NO_FUND_DEFAULTS,
        }

    fields = fund_snap.get("fields") or {}
    if not fields:
        reason_codes.append("INVESTMENT_EMPTY_FUNDAMENTALS")
        return {
            "raw_score":                      None,
            "points":                         0,
            "available":                      False,
            "status":                         "empty_fundamentals",
            "investment_pillar_count":        0,
            "investment_quality_label":       "No Clear Investment Case",
            "financial_health_strong":        False,
            "financial_health_checks_passed": 0,
            "financial_health_checks_total":  0,
            "financial_health_reason_codes":  [],
            "current_growth_strong":          False,
            "current_growth_checks_passed":   0,
            "current_growth_checks_total":    0,
            "current_growth_reason_codes":    [],
            "forward_growth_strong":          False,
            "forward_growth_checks_passed":   0,
            "forward_growth_checks_total":    0,
            "forward_growth_reason_codes":    [],
            "investment_alignment_raw_score": None,
            "investment_alignment_points":    0,
            "investment_reason_codes":        reason_codes,
            "reason_codes":                   reason_codes,
            **_NO_FUND_DEFAULTS,
        }

    pillars = _compute_investment_pillars(fields)

    fh_strong  = pillars["financial_health_strong"]
    cg_strong  = pillars["current_growth_strong"]
    fwd_strong = pillars["forward_growth_strong"]

    strong_count = int(fh_strong) + int(cg_strong) + int(fwd_strong)
    inv_pts      = round(float(strong_count) * 5.0, 2)
    inv_raw      = round(float(strong_count) / 3.0 * 100.0, 1)

    # Quality label
    if strong_count == 3:
        quality_label = "A+ Investment"
        reason_codes.append("INVESTMENT_A_PLUS")
    elif strong_count == 2:
        # More specific label based on which pillars
        if fh_strong and cg_strong:
            quality_label = "Quality Compounder"
        elif cg_strong and fwd_strong:
            quality_label = "High Growth Compounder"
        elif fh_strong and fwd_strong:
            quality_label = "Forward Growth"
        else:
            quality_label = "Strong Investment"
        reason_codes.append("INVESTMENT_2_PILLARS")
    elif strong_count == 1:
        if fh_strong:
            quality_label = "Financially Strong"
        elif cg_strong:
            quality_label = "Current Growth Leader"
        else:
            quality_label = "Speculative Growth" if fwd_strong else "Single-Pillar Investment"
        reason_codes.append("INVESTMENT_1_PILLAR")
    else:
        quality_label = "No Clear Investment Case"
        reason_codes.append("INVESTMENT_NO_PILLARS")

    reason_codes.extend(pillars["financial_health_reason_codes"])
    reason_codes.extend(pillars["current_growth_reason_codes"])
    reason_codes.extend(pillars["forward_growth_reason_codes"])

    # ── Continuous Investment Quality Score (V4.2.2) ──────────────────────────
    fh_score_0_100  = pillars.get("financial_health_score_0_100",  0)
    cg_score_0_100  = pillars.get("current_growth_score_0_100",    0)
    fwd_score_0_100 = pillars.get("forward_growth_score_0_100",    0)

    _best_pillar   = max(fh_score_0_100, cg_score_0_100, fwd_score_0_100)
    _strong_scored = sum(1 for s in [fh_score_0_100, cg_score_0_100, fwd_score_0_100] if s >= 50)
    _breadth_bonus = {0: 0, 1: 0, 2: 7, 3: 12}.get(_strong_scored, 0)
    inv_quality_score = int(min(100, round(_best_pillar + _breadth_bonus)))

    if inv_quality_score >= 90:   iq_rank_label = "A+ Investment"
    elif inv_quality_score >= 80: iq_rank_label = "Excellent Investment Quality"
    elif inv_quality_score >= 70: iq_rank_label = "Strong Investment Quality"
    elif inv_quality_score >= 60: iq_rank_label = "Watchlist Quality"
    elif inv_quality_score >= 40: iq_rank_label = "Mixed Investment Quality"
    else:                          iq_rank_label = "Weak / No Clear Investment Case"

    iq_reason_codes: list[str] = [f"IQ_SCORE_{inv_quality_score}"]
    if inv_quality_score >= 70:   iq_reason_codes.append("IQ_STRONG")
    elif inv_quality_score >= 40: iq_reason_codes.append("IQ_MIXED")
    else:                          iq_reason_codes.append("IQ_WEAK")

    return {
        "raw_score":                      inv_raw,
        "points":                         inv_pts,
        "available":                      True,
        "status":                         "available",
        "investment_pillar_count":        strong_count,
        "investment_quality_label":       quality_label,
        "financial_health_strong":        fh_strong,
        "financial_health_checks_passed": pillars["financial_health_checks_passed"],
        "financial_health_checks_total":  pillars["financial_health_checks_total"],
        "financial_health_reason_codes":  pillars["financial_health_reason_codes"],
        "current_growth_strong":          cg_strong,
        "current_growth_checks_passed":   pillars["current_growth_checks_passed"],
        "current_growth_checks_total":    pillars["current_growth_checks_total"],
        "current_growth_reason_codes":    pillars["current_growth_reason_codes"],
        "forward_growth_strong":          fwd_strong,
        "forward_growth_checks_passed":   pillars["forward_growth_checks_passed"],
        "forward_growth_checks_total":    pillars["forward_growth_checks_total"],
        "forward_growth_reason_codes":    pillars["forward_growth_reason_codes"],
        "investment_alignment_raw_score": inv_raw,
        "investment_alignment_points":    inv_pts,
        "investment_reason_codes":        reason_codes,
        "reason_codes":                   reason_codes,
        "financial_health_score_0_100":   fh_score_0_100,
        "current_growth_score_0_100":     cg_score_0_100,
        "forward_growth_score_0_100":     fwd_score_0_100,
        "investment_quality_score":       inv_quality_score,
        "investment_quality_rank_label":  iq_rank_label,
        "investment_quality_reason_codes": iq_reason_codes,
    }


# ── Bonus scorers ─────────────────────────────────────────────────────────────

def _score_social_bonus_v42(row: dict, social_sections_map: dict) -> dict:
    """
    Social Bonus — max 15 pts.

    3 sections x 5 pts each:
      Social Confluence   (ticker in consensus_picks)   → +5
      Social Acceleration (ticker in hype_radar tickers) → +5
      Fresh               (ticker in fresh_trades)       → +5

    If social cache unavailable → 0 pts, status=unavailable.
    """
    sym = str(row.get("symbol") or "").upper()
    reason_codes: list[str] = []

    sections = social_sections_map.get(sym)
    if sections is None:
        # Not in any section → no social coverage
        reason_codes.append("NO_SOCIAL_COVERAGE")
        return {
            "points":                  0,
            "available":               False,
            "status":                  "no_social_coverage",
            "social_sections_hit":     0,
            "social_confluence_hit":   False,
            "social_acceleration_hit": False,
            "social_fresh_hit":        False,
            "social_bonus_status":     "no_coverage",
            "reason_codes":            reason_codes,
        }

    conf_hit  = bool(sections.get("confluence"))
    accel_hit = bool(sections.get("acceleration"))
    fresh_hit = bool(sections.get("fresh"))
    hits      = int(conf_hit) + int(accel_hit) + int(fresh_hit)
    pts       = float(hits) * 5.0

    if conf_hit:
        reason_codes.append("SOCIAL_CONFLUENCE_HIT")
    if accel_hit:
        reason_codes.append("SOCIAL_ACCELERATION_HIT")
    if fresh_hit:
        reason_codes.append("SOCIAL_FRESH_HIT")
    if hits == 3:
        reason_codes.append("SOCIAL_ALL_SECTIONS")
    elif hits == 0:
        reason_codes.append("SOCIAL_NO_SECTIONS_HIT")

    return {
        "points":                  pts,
        "available":               True,
        "status":                  "available" if hits > 0 else "covered_no_hit",
        "social_sections_hit":     hits,
        "social_confluence_hit":   conf_hit,
        "social_acceleration_hit": accel_hit,
        "social_fresh_hit":        fresh_hit,
        "social_bonus_status":     "available" if hits > 0 else "no_sections_hit",
        "reason_codes":            reason_codes,
    }


def _score_whale_insider_bonus_v42(row: dict) -> dict:
    """
    Whale / Insider / Politician Bonus — max 5 pts.

    Formula: buy_pressure = buy_value / (buy_value + sell_value)
      80-100% → +5 pts
      65-79%  → +4 pts
      55-64%  → +3 pts
      45-54%  → +1 pt
      <45%    → 0 pts

    Phase 1: sync buy/sell pipeline not wired → 0 pts, status=not_wired.
    """
    return {
        "points":                    0,
        "available":                 False,
        "status":                    "not_wired",
        "whale_insider_buy_pressure": None,
        "whale_buy_value":           None,
        "whale_sell_value":          None,
        "insider_buy_value":         None,
        "insider_sell_value":        None,
        "politician_buy_value":      None,
        "politician_sell_value":     None,
        "whale_insider_status":      "not_wired",
        "whale_insider_reason_codes": ["WHALE_INSIDER_SYNC_NOT_WIRED"],
        "reason_codes":              ["WHALE_INSIDER_SYNC_NOT_WIRED"],
    }


def _score_bottleneck_bonus_v42(row: dict, bottleneck_map: dict) -> dict:
    """
    Bottleneck / Supply Chain Bonus — max 5 pts.

    Formula: min(5, anchor_count / total_anchor_count * 5)
    Default total_anchor_count = 13.
    """
    sym = str(row.get("symbol") or "").upper()
    reason_codes: list[str] = []
    entry = bottleneck_map.get(sym)

    if not entry:
        reason_codes.append("NOT_IN_BOTTLENECK_SCREENER")
        return {
            "points":                     0,
            "available":                  False,
            "status":                     "not_in_screener",
            "bottleneck_anchor_count":    0,
            "bottleneck_total_anchor_count": _DEFAULT_TOTAL_ANCHORS,
            "bottleneck_anchor_names":    [],
            "bottleneck_status":          "not_in_screener",
            "bottleneck_reason_codes":    reason_codes,
            "reason_codes":               reason_codes,
        }

    anchor_count = int(entry.get("anchor_count") or 0)
    anchor_names = entry.get("anchor_names") or []
    pts = round(min(5.0, anchor_count / _DEFAULT_TOTAL_ANCHORS * 5.0), 2)
    reason_codes.append(f"BOTTLENECK_{anchor_count}_ANCHORS")

    return {
        "points":                     pts,
        "available":                  True,
        "status":                     "available",
        "bottleneck_anchor_count":    anchor_count,
        "bottleneck_total_anchor_count": _DEFAULT_TOTAL_ANCHORS,
        "bottleneck_anchor_names":    anchor_names,
        "bottleneck_status":          "available",
        "bottleneck_reason_codes":    reason_codes,
        "reason_codes":               reason_codes,
    }


# ── Bucket + Actionability ────────────────────────────────────────────────────

def _assign_v42_bucket(
    normalized_total: float,
    core_score:       float,
    major_llc:        bool,
    chase:            bool,
    constructive:     bool,
    asst_status:      str,
    entry_exit_pts:   float,
    invest_pts:       float,
    confidence:       float = 100.0,
) -> str:
    """
    V4.2 Bucket assignment.

    normalized_total = min(125, normalized_core + bonus_score)
    Hard gates override everything.
    """
    if major_llc:
        return "RISK_CONFLICT"

    if chase and not constructive:
        return "WATCH_FOR_RESET"

    # ACTIONABLE
    if normalized_total >= 82 and entry_exit_pts >= 9 and confidence >= 55:
        return "ACTIONABLE"

    # NEAR_ACTIONABLE
    if normalized_total >= 65 and entry_exit_pts >= 4 and confidence >= 45:
        return "NEAR_ACTIONABLE"

    # CONFLUENCE_AT_SUPPORT
    if normalized_total >= 55 and asst_status in _SUPPORT_INTACT_STATUSES and entry_exit_pts >= 2:
        return "CONFLUENCE_AT_SUPPORT"

    # NEAR_ACTIONABLE (softer)
    if normalized_total >= 70 and entry_exit_pts >= 3 and confidence >= 45:
        return "NEAR_ACTIONABLE"

    # INVESTMENT_QUALITY
    if invest_pts >= 10 and normalized_total >= 48:
        return "INVESTMENT_QUALITY"

    # SPECULATIVE_TRADE
    if normalized_total >= 65 and invest_pts < 5:
        return "SPECULATIVE_TRADE"

    # WATCH_FOR_RESET
    if (chase and constructive) or normalized_total >= 55:
        return "WATCH_FOR_RESET"

    return "NO_CLEAR_CONFLUENCE"


def _derive_actionability_v42(
    normalized_total:  float,
    bucket:            str,
    major_llc:         bool,
    chase:             bool,
    constructive:      bool,
    pattern:           str,
    asst_status:       str,
    entry_exit_pts:    float,
    entry_rr_score:    Optional[float],
    confidence:        float = 100.0,
) -> str:
    rr  = _safe_float(entry_rr_score, 50.0)
    support_intact = asst_status in _SUPPORT_INTACT_STATUSES

    if major_llc or bucket == "RISK_CONFLICT":
        return "AVOID"

    if chase and not constructive:
        return "WATCH_FOR_RESET"

    if normalized_total >= 90 and entry_exit_pts >= 8 and bucket in ("ACTIONABLE", "NEAR_ACTIONABLE") and confidence >= 70:
        return "READY"

    if pattern in _SHELF_PATTERNS and normalized_total >= 65 and not chase:
        return "WAIT_FOR_BREAKOUT"

    if normalized_total >= 76 and entry_exit_pts >= 5 and not chase:
        return "NEAR_ACTIONABLE"

    if normalized_total >= 62 and support_intact and entry_exit_pts >= 4:
        return "WAIT_FOR_RETEST"

    if bucket == "NEAR_ACTIONABLE":
        return "NEAR_ACTIONABLE"

    if bucket == "WATCH_FOR_RESET":
        return "WATCH_FOR_RESET"

    return "WATCH"


def _compute_v42_confidence(
    components:         dict,
    social_available:   bool,
    shelf_confirmed:    bool = True,
    used_constructive_tier1: bool = False,
) -> float:
    """
    V4.2 confidence: % of components available, with structural penalties.

    7 core components x 12 pts = 84, social = 8 → total possible = 92.
    Normalise to 100.
    """
    comp_weights = {
        "theme_alignment":    12,
        "stage_quality":      12,
        "options_alignment":  12,
        "technical_setup":    12,
        "entry_exit":         12,
        "catalyst_alignment": 12,
        "investment_alignment": 12,
    }
    _KNOWN_STATE_STATUSES = {
        "confirmed_no_options",
        "no_catalyst",
        "lkg_market_closed",
        "available_cached",
        "available_live",
        "stale_but_usable",
        "available",
    }

    total_possible = sum(comp_weights.values()) + 8  # 92
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
    return round(earned / total_possible * 100.0, 1)


# ── Main Entry Point ──────────────────────────────────────────────────────────

def compute_confluence_v42(
    snapshot_row:        dict,
    social_sections_map: Optional[dict] = None,
    bottleneck_map:      Optional[dict] = None,
    fundamentals_map:    Optional[dict] = None,
) -> dict:
    """
    Compute V4.2 Confluence Score from existing snapshot row fields.

    Pure read — zero provider calls, zero LLM calls.

    Args:
        snapshot_row:        Full confluence snapshot row for one symbol.
        social_sections_map: {SYM: {confluence, acceleration, fresh}} booleans.
                             Build once with build_social_sections_map().
        bottleneck_map:      {SYM: {anchor_count, anchor_names}}.
                             Pass None to auto-load from curated_anchor_bottlenecks.
        fundamentals_map:    {SYM: {fields: {...}}} from watchlist_fundamentals_cache.
                             Pass None to fetch individually (one Neon call per ticker).

    Returns full dict of caelyn_confluence_v42_* promotion fields.
    """
    if social_sections_map is None:
        social_sections_map = {}
    if bottleneck_map is None:
        bottleneck_map = _get_bottleneck_map()

    sym = str(snapshot_row.get("symbol") or "").upper()

    # ── Core components ───────────────────────────────────────────────────────
    theme_comp  = _score_theme_alignment_v42(snapshot_row)
    stage_comp  = _score_stage_quality_v42(snapshot_row)
    opts_comp   = _score_options_alignment_v42(snapshot_row)
    tech_comp   = _score_technical_setup_v42(snapshot_row)
    entry_comp  = _score_entry_exit_v42(snapshot_row)
    cat_comp    = _score_catalyst_alignment_v42(snapshot_row)
    invest_comp = _score_investment_alignment_v42(snapshot_row, fundamentals_map)

    core_score = round(min(100.0, sum([
        theme_comp["points"],
        stage_comp["points"],
        opts_comp["points"],
        tech_comp["points"],
        entry_comp["points"],
        cat_comp["points"],
        invest_comp["points"],
    ])), 1)

    # ── Bonus scorers ─────────────────────────────────────────────────────────
    soc_bonus = _score_social_bonus_v42(snapshot_row, social_sections_map)
    wi_bonus  = _score_whale_insider_bonus_v42(snapshot_row)
    bn_bonus  = _score_bottleneck_bonus_v42(snapshot_row, bottleneck_map)

    bonus_score = round(min(25.0, sum([
        soc_bonus["points"],
        wi_bonus["points"],
        bn_bonus["points"],
    ])), 1)

    total_score = round(min(125.0, core_score + bonus_score), 1)

    # ── Prediction Markets: disabled from score ────────────────────────────────
    pm_disabled = {
        "points":                          0,
        "available":                       False,
        "status":                          "disabled",
        "prediction_market_bonus_points":  0,
        "prediction_market_bonus_status":  "disabled",
        "prediction_market_reason_codes":  ["PREDICTION_MARKETS_DISABLED_FROM_SCORE"],
    }

    # ── Components dict ───────────────────────────────────────────────────────
    components = {
        "theme_alignment":    theme_comp,
        "stage_quality":      stage_comp,
        "options_alignment":  opts_comp,
        "technical_setup":    tech_comp,
        "entry_exit":         entry_comp,
        "catalyst_alignment": cat_comp,
        "investment_alignment": invest_comp,
    }

    # ── Normalized core score for bucket assignment ────────────────────────────
    _COMP_MAX_PTS = {
        "theme_alignment":    15.0,
        "stage_quality":      15.0,
        "options_alignment":  20.0,
        "technical_setup":     8.0,
        "entry_exit":         12.0,
        "catalyst_alignment": 15.0,
        "investment_alignment": 15.0,
    }
    available_max = sum(
        _COMP_MAX_PTS[k] for k, c in components.items() if c.get("available")
    )
    if available_max > 0:
        normalized_core = min(100.0, (core_score / available_max) * 100.0)
    else:
        normalized_core = 0.0

    normalized_total = round(min(125.0, normalized_core + bonus_score), 1)

    # ── Confidence ────────────────────────────────────────────────────────────
    _used_tier1 = any(
        "CONSTRUCTIVE_" in rc and "_STRONG_ENTRY" in rc
        for rc in (tech_comp.get("reason_codes") or [])
    )
    v42_confidence = _compute_v42_confidence(
        components,
        soc_bonus.get("available", False),
        shelf_confirmed    = (snapshot_row.get("current_shelf_support") is not None),
        used_constructive_tier1 = _used_tier1,
    )

    # ── Bucket + Actionability ────────────────────────────────────────────────
    v42_bucket = _assign_v42_bucket(
        normalized_total = normalized_total,
        core_score       = core_score,
        major_llc    = bool(snapshot_row.get("major_lower_low_confirmed")),
        chase        = bool(snapshot_row.get("chase_extension")),
        constructive = bool(snapshot_row.get("constructive_extension")),
        asst_status  = str(snapshot_row.get("active_support_status") or ""),
        entry_exit_pts = entry_comp["points"],
        invest_pts     = invest_comp["points"],
        confidence     = v42_confidence,
    )

    v42_actionability = _derive_actionability_v42(
        normalized_total = normalized_total,
        bucket       = v42_bucket,
        major_llc    = bool(snapshot_row.get("major_lower_low_confirmed")),
        chase        = bool(snapshot_row.get("chase_extension")),
        constructive = bool(snapshot_row.get("constructive_extension")),
        pattern      = str(snapshot_row.get("pattern_type") or "NO_PATTERN"),
        asst_status  = str(snapshot_row.get("active_support_status") or ""),
        entry_exit_pts  = entry_comp["points"],
        entry_rr_score  = snapshot_row.get("entry_risk_reward_score"),
        confidence      = v42_confidence,
    )

    # ── Reason codes (flat union) ─────────────────────────────────────────────
    all_reason_codes: list[str] = []
    for c in components.values():
        all_reason_codes.extend(c.get("reason_codes") or [])
    all_reason_codes.extend(soc_bonus.get("reason_codes") or [])
    all_reason_codes.extend(wi_bonus.get("reason_codes") or [])
    all_reason_codes.extend(bn_bonus.get("reason_codes") or [])
    all_reason_codes.append("PREDICTION_MARKETS_DISABLED_FROM_SCORE")
    all_reason_codes.append(f"V42_BUCKET_{v42_bucket}")
    all_reason_codes.append(f"V42_ACT_{v42_actionability}")

    # ── Bonus breakdown dict ──────────────────────────────────────────────────
    bonus_breakdown = {
        "social":              soc_bonus,
        "whale_insider":       wi_bonus,
        "bottleneck":          bn_bonus,
        "prediction_markets":  pm_disabled,
        "theme_policy":        {"points": 0, "status": "folded_into_theme_alignment"},
    }

    return {
        # ── V4.2 debug fields ─────────────────────────────────────────────────
        "caelyn_confluence_v42_score":         total_score,
        "caelyn_confluence_v42_core_score":    core_score,
        "caelyn_confluence_v42_bonus_score":   bonus_score,
        "caelyn_confluence_v42_max_score":     125,
        "caelyn_confluence_v42_normalized_score": normalized_total,
        "caelyn_confluence_v42_available_max_pts": round(available_max, 1),
        "caelyn_confluence_v42_components":    components,
        "caelyn_confluence_v42_bonus_breakdown": bonus_breakdown,
        "caelyn_confluence_v42_reason_codes":  all_reason_codes,
        "caelyn_confluence_v42_confidence_score": v42_confidence,
        "caelyn_confluence_v42_bucket":        v42_bucket,
        "caelyn_confluence_v42_actionability": v42_actionability,
        # ── Canonical promotion fields ────────────────────────────────────────
        "caelyn_confluence_score":            total_score,
        "caelyn_confluence_raw_score":        total_score,
        "caelyn_confluence_core_score":       core_score,
        "caelyn_confluence_bonus_score":      bonus_score,
        "caelyn_confluence_max_score":        125,
        "caelyn_confluence_normalized_score": normalized_total,
        "caelyn_confluence_confidence_score": v42_confidence,
        "caelyn_confluence_bucket":           v42_bucket,
        "caelyn_confluence_reason_codes":     all_reason_codes,
        # ── Component first-class fields ─────────────────────────────────────
        "theme_alignment_points":       theme_comp["points"],
        "stage_quality_points":         stage_comp["points"],
        "stage_quality_score":          stage_comp.get("raw_score"),
        "options_alignment_points":     opts_comp["points"],
        "options_status":               opts_comp.get("status"),
        "technical_setup_points":       tech_comp["points"],
        "technical_setup_raw_score":    tech_comp.get("technical_setup_raw_score"),
        "technical_setup_label":        tech_comp.get("technical_setup_label"),
        "entry_exit_points":            entry_comp["points"],
        "entry_exit_raw_score":         entry_comp.get("entry_exit_raw_score"),
        "entry_exit_status":            entry_comp.get("entry_exit_status"),
        "catalyst_alignment_points":    cat_comp["points"],
        "catalyst_status":              cat_comp.get("status"),
        "direct_catalyst_present":      cat_comp.get("direct_catalyst_present"),
        "direct_catalyst_type":         cat_comp.get("direct_catalyst_type"),
        "catalyst_intelligence_score":  cat_comp.get("catalyst_intelligence_score"),
        "investment_alignment_points":    invest_comp["points"],
        "investment_pillar_count":        invest_comp.get("investment_pillar_count"),
        "investment_quality_label":       invest_comp.get("investment_quality_label"),
        "financial_health_strong":        invest_comp.get("financial_health_strong"),
        "current_growth_strong":          invest_comp.get("current_growth_strong"),
        "forward_growth_strong":          invest_comp.get("forward_growth_strong"),
        "financial_health_score_0_100":   invest_comp.get("financial_health_score_0_100"),
        "current_growth_score_0_100":     invest_comp.get("current_growth_score_0_100"),
        "forward_growth_score_0_100":     invest_comp.get("forward_growth_score_0_100"),
        "investment_quality_score":       invest_comp.get("investment_quality_score"),
        "investment_quality_rank_label":  invest_comp.get("investment_quality_rank_label"),
        "investment_quality_reason_codes": invest_comp.get("investment_quality_reason_codes"),
        "social_bonus_points":          soc_bonus["points"],
        "social_sections_hit":          soc_bonus.get("social_sections_hit"),
        "social_confluence_hit":        soc_bonus.get("social_confluence_hit"),
        "social_acceleration_hit":      soc_bonus.get("social_acceleration_hit"),
        "social_fresh_hit":             soc_bonus.get("social_fresh_hit"),
        "whale_insider_bonus_points":   wi_bonus["points"],
        "bottleneck_bonus_points":      bn_bonus["points"],
        "bottleneck_anchor_count":      bn_bonus.get("bottleneck_anchor_count"),
        "prediction_market_bonus_points": 0,
        "prediction_market_bonus_status": "disabled",
        "theme_policy_bonus_points":    0,
        "theme_policy_bonus_status":    "folded_into_theme_alignment",
    }
