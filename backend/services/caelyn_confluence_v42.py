"""
CAELYN CONFLUENCE V4.2 — Core Scoring Semantics Cleanup

Formula:
  core_score    = Theme + Stage + Options + TechSetup + EntryExit + Catalyst + Investment + Valuation
                  max = 100
  bonus_score   = Social + WhaleInsider + Bottleneck
                  max = 25
  total_score   = min(125, core + bonus)

Core Components (max 100 pts):
  Theme Alignment     15 pts  (theme_policy folded in; no standalone bonus)
  Stage Quality       15 pts  (unchanged from V4)
  Options Alignment   18 pts  (60 net premium + 40 acceleration)
  Technical Setup      8 pts  (pattern quality)
  Entry/Exit Quality  12 pts  (support/RR quality)
  Catalyst Alignment  12 pts  (75 direct + 25 intelligence)
  Investment Align    12 pts  (3-pillar: financial health + current + forward growth)
  Valuation            8 pts  (P/E + P/S + Forward P/E equally weighted)

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

    opts_pts = round(min(18.0, opts_raw / 100.0 * 18.0), 2)

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
    "HIGH_TIGHT_FLAG":              95.0,
    "BULL_FLAG":                    88.0,
    "BREAKOUT_SHELF":               85.0,
    "VCP":                          85.0,
    "CUP_HANDLE":                   85.0,
    "CUP_AND_HANDLE":               85.0,
    "VCP_CONTRACTION":              83.0,
    "STAGE2_BREAKOUT":              83.0,
    "BREAKOUT_SHELF_CONSOLIDATION": 80.0,
    "EMA_PULLBACK":                 78.0,
    "20DMA_PULLBACK":               75.0,
    "30DMA_PULLBACK":               75.0,
    "SUPPORT_BOUNCE":               75.0,
    "50DMA_PULLBACK":               72.0,
    "LEADER_PULLBACK":              72.0,
    "BREAKOUT_RETEST":              70.0,
    "LOW_BASE_REVERSAL":            68.0,
    "WAVE_CONTINUATION_PROXY":      65.0,
    "BASE_BOTTOM":                  65.0,
    "200DMA_RECLAIM":               60.0,
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

    # ── HIGH_BASE_CONSOLIDATION synthetic detection ───────────────────────────
    # Fires when pattern engine returned NO_PATTERN but structural evidence
    # indicates a high-base consolidation (base_archetype=HIGH_BASE + strong stage).
    # Uses stage_quality_points from the raw snapshot row (pre-computed upstream).
    if not has_valid and not major_llc and not (chase and not constructive):
        _hb_arch   = str(row.get("base_archetype") or "").upper()
        _hb_entry  = str(row.get("entry_state") or "").upper()
        # stage_quality_points is computed later in the V42 pipeline and is not yet
        # populated in snapshot_row at this stage.  Fall back to stage_alignment_score
        # which is always present in the raw row (same formula: score/100*15).
        _hb_stage_raw = _safe_float(row.get("stage_quality_points"), None)
        if _hb_stage_raw is None or _hb_stage_raw == 0.0:
            _stage_align  = _safe_float(row.get("stage_alignment_score"), 0.0)
            _hb_stage     = round(_stage_align / 100.0 * 15.0, 2)
        else:
            _hb_stage = _hb_stage_raw
        if _hb_arch == "HIGH_BASE" and _hb_stage >= 10.5 and asst_status not in ("support_lost", "breakdown", "major_breakdown"):
            _hb_q = 65.0
            if _hb_entry in ("HIGH_BASE_READY", "BREAKOUT_PULLBACK", "CONTINUATION"):
                _hb_q += 10.0
            if constructive:
                _hb_q += 5.0
            if asst_status in _SUPPORT_INTACT_STATUSES:
                _hb_q += 5.0
            if minor_llc:
                _hb_q -= 5.0
            _hb_q = _clamp(_hb_q, 0.0, 100.0)
            if _hb_q > quality:
                quality = _hb_q
                pts     = round(quality / 100.0 * 8.0, 2)
                reason_codes.append("HIGH_BASE_CONSOLIDATION_DETECTED")

    # Label
    _LABEL_MAP = {
        "HIGH_TIGHT_FLAG":              "High-Tight Continuation",
        "BULL_FLAG":                    "Bull Flag",
        "BREAKOUT_SHELF":               "Breakout Shelf",
        "VCP":                          "VCP",
        "VCP_CONTRACTION":              "VCP Contraction",
        "CUP_HANDLE":                   "Cup & Handle",
        "CUP_AND_HANDLE":               "Cup & Handle",
        "STAGE2_BREAKOUT":              "Stage 2 Breakout",
        "BREAKOUT_SHELF_CONSOLIDATION": "Breakout Shelf Consolidation",
        "EMA_PULLBACK":                 "EMA Pullback",
        "20DMA_PULLBACK":               "20D MA Pullback",
        "30DMA_PULLBACK":               "30D MA Pullback",
        "50DMA_PULLBACK":               "50D MA Pullback",
        "SUPPORT_BOUNCE":               "Support Bounce",
        "LEADER_PULLBACK":              "Leader Pullback",
        "BREAKOUT_RETEST":              "Breakout Retest",
        "LOW_BASE_REVERSAL":            "Low-Base Reversal",
        "BASE_BOTTOM":                  "Base Bottom",
        "200DMA_RECLAIM":               "200D MA Reclaim",
        "WAVE_CONTINUATION_PROXY":      "Wave Continuation",
    }
    if pattern in _LABEL_MAP:
        setup_label = _LABEL_MAP[pattern]
    elif "HIGH_BASE_CONSOLIDATION_DETECTED" in reason_codes:
        setup_label = "High-Base Consolidation"
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
    # V4.2.5 — Extension Reset + Fib Retest
    ext_reset    = str(row.get("extension_reset_state") or "").upper()
    is_constr_retest = (ext_reset == "CONSTRUCTIVE_RETEST_AFTER_EXTENSION")
    fib_retest   = bool(row.get("fib_retest_detected"))
    fib_retest_t = str(row.get("fib_retest_type") or "")
    nearest_fib  = str(row.get("nearest_fib_label") or "")
    dist_20dma   = row.get("dist_from_20dma_pct")
    dist_50dma   = row.get("dist_from_50dma_pct")
    dist_200dma  = row.get("dist_from_200dma_pct")
    # V4.2.5.1 — Multi-timeframe Fib confidence gate
    primary_fib_conf = float(row.get("primary_fib_confidence") or 0.0)
    fib_trusted      = fib_retest and primary_fib_conf >= 0.25
    fib_mtf_selected = primary_fib_conf >= 0.50 and bool(row.get("fib_candidates_count", 0) or 0) > 1
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

    # Chase extension penalty — reduced for CONSTRUCTIVE_RETEST_AFTER_EXTENSION
    if chase and not constructive:
        if is_constr_retest:
            # V4.2.5: pulled back to Fib/MA after extension — use softer cap
            rr_val        = min(rr_val, 60.0)
            support_score = min(support_score, 55.0)
            reason_codes.append("CHASE_EXTENSION_REDUCED_PENALTY_CONSTRUCTIVE_RETEST")
        else:
            rr_val        = min(rr_val, 40.0)
            support_score = min(support_score, 40.0)
            reason_codes.append("CHASE_EXTENSION_ENTRY_PENALTY")

    # Fib retest bonus (additive, not a gate)
    # V4.2.5.1: gated by primary_fib_confidence >= 0.25
    if fib_trusted and not major_llc:
        _fib_bonus = 0.0
        _fib_lbl   = nearest_fib.replace("FIB_", "").replace(".", "")
        if fib_retest_t == "PRIOR_RESISTANCE_RETEST":
            _fib_bonus = 5.0
            reason_codes.append("FIB_1000_RETEST_ENTRY")
            reason_codes.append("FIB_RETEST_ENTRY")
        elif fib_retest_t == "SHALLOW_RETRACEMENT_RETEST":
            _fib_bonus = 6.0
            reason_codes.append(f"FIB_{_fib_lbl}_RETEST_ENTRY")
            reason_codes.append("FIB_RETEST_ENTRY")
        elif fib_retest_t == "DEEP_RETRACEMENT_RETEST":
            _fib_bonus = 4.0
            reason_codes.append(f"FIB_{_fib_lbl}_RETEST_ENTRY")
            reason_codes.append("FIB_RETEST_ENTRY")
        elif fib_retest_t == "EXTENSION_TARGET_RETEST":
            _ext_lbl = nearest_fib.replace("FIB_", "").replace(".", "")
            _fib_bonus = 3.0
            reason_codes.append(f"FIB_EXTENSION_TARGET_{_ext_lbl}")
        elif fib_retest_t == "FAR_EXTENSION_TARGET":
            _ext_lbl = nearest_fib.replace("FIB_", "").replace(".", "")
            _fib_bonus = 2.0
            reason_codes.append(f"FIB_EXTENSION_TARGET_{_ext_lbl}")
        if fib_mtf_selected:
            reason_codes.append("FIB_MULTI_TIMEFRAME_PRIMARY_SELECTED")
        rr_val = min(rr_val + _fib_bonus, 100.0)

    # MA proximity bonus (additive, max once)
    if not fib_retest and not major_llc:
        _near_ma_bonus = 0.0
        if dist_20dma is not None and abs(dist_20dma) <= 3.0:
            _near_ma_bonus = 4.0
            reason_codes.append(f"NEAR_20DMA_{dist_20dma:.1f}PCT")
        elif dist_50dma is not None and abs(dist_50dma) <= 4.0:
            _near_ma_bonus = 3.0
            reason_codes.append(f"NEAR_50DMA_{dist_50dma:.1f}PCT")
        elif dist_200dma is not None and abs(dist_200dma) <= 4.0:
            _near_ma_bonus = 2.0
            reason_codes.append(f"NEAR_200DMA_{dist_200dma:.1f}PCT")
        rr_val = min(rr_val + _near_ma_bonus, 100.0)

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

    # ── Momentum / MA pullback detection (reason codes only, no score change) ──
    _asst_type  = str(row.get("active_support_type") or "").lower()
    _entry_st   = str(row.get("entry_state") or "").upper()
    _ma_entry_type: Optional[str] = None

    if _asst_type == "moving_average_zone":
        _ma_entry_type = "MOVING_AVERAGE_PULLBACK_ENTRY"
        reason_codes.append("MOVING_AVERAGE_PULLBACK_ENTRY")
    elif _asst_type in ("prior_high_level", "prior_high", "prior_high_zone"):
        _ma_entry_type = "PRIOR_HIGH_RETEST_ENTRY"
        reason_codes.append("PRIOR_HIGH_RETEST_ENTRY")
    elif _asst_type in ("breakout_shelf", "shelf", "shelf_support"):
        _ma_entry_type = "BREAKOUT_SHELF_RETEST_ENTRY"
        reason_codes.append("BREAKOUT_SHELF_RETEST_ENTRY")
    elif _entry_st in ("BREAKOUT_PULLBACK", "CONTINUATION_PULLBACK", "CONTINUATION"):
        _ma_entry_type = "MOMENTUM_PULLBACK_ENTRY"
        reason_codes.append("MOMENTUM_PULLBACK_ENTRY")

    # Distinguish DMA tier from moving_average_zone label when entry_state is specific
    if _ma_entry_type == "MOVING_AVERAGE_PULLBACK_ENTRY":
        if _entry_st in ("20DMA_PULLBACK", "EMA_20_PULLBACK"):
            _ma_entry_type = "DMA20_PULLBACK_ENTRY"; reason_codes.append("DMA20_PULLBACK_ENTRY")
        elif _entry_st in ("30DMA_PULLBACK", "EMA_30_PULLBACK"):
            _ma_entry_type = "DMA30_PULLBACK_ENTRY"; reason_codes.append("DMA30_PULLBACK_ENTRY")
        elif _entry_st in ("50DMA_PULLBACK", "EMA_50_PULLBACK"):
            _ma_entry_type = "DMA50_PULLBACK_ENTRY"; reason_codes.append("DMA50_PULLBACK_ENTRY")
        elif _entry_st in ("200DMA_PULLBACK", "EMA_200_PULLBACK", "200DMA_RECLAIM"):
            _ma_entry_type = "DMA200_PULLBACK_ENTRY"; reason_codes.append("DMA200_PULLBACK_ENTRY")

    if _ma_entry_type is None and _asst_type:
        reason_codes.append("MOVING_AVERAGE_DATA_UNAVAILABLE")

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
        "moving_average_entry_detected": _ma_entry_type is not None,
        "moving_average_entry_type":     _ma_entry_type,
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


def _catalyst_phb_direct_score(
    primary_event: dict,
    source: str,
    cat_score_fallback: float | None,
    is_scheduled: bool,
) -> tuple:
    """
    Phase B: graduated catalyst direct score (0–100 scale).

    Uses event_type tier + materiality + confidence + relevance +
    freshness + proximity + article_count modifiers sourced entirely
    from cached catalyst_primary_event fields — no provider calls.

    Returns: (score: float, tier: str, event_type_norm: str, detail: dict)
    """
    from datetime import datetime, timezone as _tz

    et_raw = (primary_event.get("event_type") or "").upper().strip()

    # ── Event-type tier → base score ─────────────────────────────────────
    # Tier A  80 — regulatory, hyperscaler, major defense/govt award
    # Tier B  70 — M&A, earnings guidance, analyst upgrade, investor day
    # Tier C1 58 — strategic partnership, technical milestone, product launch
    # Tier C2 48 — commercial contract (materiality unknown by default)
    # Tier D  28 — split/dividend, generic, unknown
    _TIER_A  = {"FDA_READOUT", "REGULATORY_DECISION", "HYPERSCALER_ANCHOR",
                "DEFENSE_MILITARY", "MAJOR_GOVERNMENT_AWARD"}
    _TIER_B  = {"MNA", "EARNINGS_GUIDANCE", "EARNINGS_DATE", "ANALYST_UPGRADE",
                "INVESTOR_DAY", "ANALYST_DAY"}
    _TIER_C1 = {"STRATEGIC_PARTNERSHIP", "TECHNICAL_MILESTONE", "PRODUCT_LAUNCH",
                "PRODUCT_UPDATE", "FINANCING"}
    _TIER_C2 = {"COMMERCIAL_CONTRACT"}
    _TIER_D  = {"SPLIT_THIS_WEEK", "DIVIDEND_THIS_WEEK", "GENERIC_RSS"}

    if et_raw in _TIER_A:
        base, tier = 80.0, "TIER_A"
    elif et_raw in _TIER_B:
        base, tier = 70.0, "TIER_B"
    elif et_raw in _TIER_C1:
        base, tier = 58.0, "TIER_C"
    elif et_raw in _TIER_C2:
        base, tier = 48.0, "TIER_C"
    elif et_raw in _TIER_D:
        base, tier = 28.0, "TIER_D"
    else:
        base, tier = 30.0, "TIER_D"   # unknown event type

    # ── Materiality modifier  [-10, +10] ─────────────────────────────────
    mat = primary_event.get("materiality_score")
    mat_mod = round((float(mat) - 0.5) * 20.0, 1) if mat is not None else 0.0

    # ── Confidence modifier  [-5, +5] ────────────────────────────────────
    conf = primary_event.get("confidence_score")
    conf_mod = round((float(conf) - 0.5) * 10.0, 1) if conf is not None else 0.0

    # ── Ticker relevance modifier ─────────────────────────────────────────
    rel = primary_event.get("ticker_relevance_score")
    if rel is None:
        rel_mod = 0.0
    elif float(rel) >= 0.95:
        rel_mod = 6.0
    elif float(rel) >= 0.70:
        rel_mod = 4.0
    elif float(rel) >= 0.30:
        rel_mod = 2.0
    else:
        rel_mod = -6.0

    # ── Freshness modifier (published_at / catalyst_date) ────────────────
    fresh_mod = 0.0
    age_days:  int | None = None
    dt_str = primary_event.get("published_at") or primary_event.get("catalyst_date")
    if dt_str:
        try:
            s = str(dt_str)
            if "T" in s:
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            else:
                dt = datetime.strptime(s[:10], "%Y-%m-%d").replace(tzinfo=_tz.utc)
            age_days = max(0, (datetime.now(_tz.utc) - dt).days)
            if age_days <= 3:
                fresh_mod = 6.0
            elif age_days <= 14:
                fresh_mod = 4.0
            elif age_days <= 45:
                fresh_mod = 2.0
        except Exception:
            pass

    # ── Proximity modifier (scheduled future event with days_until) ───────
    prox_mod = 0.0
    days_until = primary_event.get("days_until")
    if is_scheduled and days_until is not None:
        try:
            du = float(days_until)
            if du <= 7:
                prox_mod = 10.0
            elif du <= 30:
                prox_mod = 6.0
            elif du <= 90:
                prox_mod = 3.0
        except (TypeError, ValueError):
            pass

    # ── Article count (corroboration signal) ─────────────────────────────
    art_count = primary_event.get("article_count") or 1
    art_mod = 4.0 if art_count >= 3 else (2.0 if art_count >= 2 else 0.0)

    total = base + mat_mod + conf_mod + rel_mod + fresh_mod + prox_mod + art_mod
    final = max(0.0, min(100.0, total))

    detail = {
        "base": base,
        "tier": tier,
        "event_type_norm": et_raw or "UNKNOWN",
        "mat_mod": mat_mod,
        "conf_mod": conf_mod,
        "rel_mod": rel_mod,
        "fresh_mod": fresh_mod,
        "prox_mod": prox_mod,
        "art_mod": art_mod,
        "age_days": age_days,
    }
    return final, tier, et_raw or "UNKNOWN", detail


def _catalyst_phb_theme_policy_score(cat_score: float | None) -> tuple:
    """
    Phase B score for theme_policy-only rows that have no rich primary_event data.
    Maps cat_score to Tier D range (15–40) so theme items stay low-to-moderate.
    Returns: (score, tier, event_type_norm, detail)
    """
    if cat_score is None or cat_score < 1.0:
        return 0.0, "TIER_E", "THEME_POLICY", {"base": 0.0, "tier": "TIER_E"}
    # Spread within Tier D: 15 + fraction × 25 (gives 15–40 range)
    score = min(40.0, 15.0 + (float(cat_score) / 100.0) * 25.0)
    return score, "TIER_D", "THEME_POLICY", {"base": score, "tier": "TIER_D", "cat_score_input": cat_score}


def _catalyst_phb_explanation(tier: str, et_norm: str, bearish: bool,
                               age_days: int | None, source: str, pts: float) -> str:
    """Human-readable explanation for catalyst score."""
    if bearish:
        return "Bearish catalyst conflict detected; catalyst points suppressed to zero."
    if pts == 0:
        return "No qualifying catalyst signal found."
    tier_labels = {
        "TIER_A": "High-conviction catalyst",
        "TIER_B": "Moderate-high catalyst",
        "TIER_C": "Moderate catalyst",
        "TIER_D": "Low-confidence or thematic catalyst",
        "TIER_E": "Minimal/stale catalyst",
    }
    base_label = tier_labels.get(tier, "Catalyst")
    et_display = et_norm.replace("_", " ").title() if et_norm not in ("UNKNOWN", "THEME_POLICY") else ("theme-policy tailwind" if et_norm == "THEME_POLICY" else "unclassified event")
    if et_norm == "THEME_POLICY":
        return f"Theme-policy tailwind only; low direct catalyst confidence. Scored conservatively ({pts:.1f}/15)."
    freshness = ""
    if age_days is not None:
        if age_days <= 3:
            freshness = " Published within last 3 days."
        elif age_days <= 14:
            freshness = f" Published {age_days}d ago."
        elif age_days <= 45:
            freshness = f" Published {age_days}d ago — moderate freshness."
        else:
            freshness = f" Stale ({age_days}d ago)."
    return f"{base_label} ({et_display}) via {source}.{freshness} Scored {pts:.1f}/15."


def _score_catalyst_alignment_v42(row: dict) -> dict:
    """
    Catalyst Alignment — 15 pts max.

    Phase B (V4.2.7): graduated continuous scoring using event-type tier,
    materiality, confidence, relevance, freshness, proximity, and article-count
    modifiers sourced from cached catalyst_primary_event fields.

    No binary event→100 collapse. Generic RSS events no longer auto-max.
    Bearish conflict zeroing preserved.

    Formula:
      direct_score  = _catalyst_phb_direct_score(primary_event, ...)
      catalyst_pts  = direct_score / 100 * 15

    When catalyst intelligence data is available, restore the blended formula:
      catalyst_raw = direct_score * 0.75 + intelligence_score * 0.25
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

    # Event candidates
    _ev_scheduled = row.get("catalyst_scheduled_event") or _cat_nested.get("scheduled_event")
    _ev_rss       = row.get("catalyst_rss_event")       or _cat_nested.get("rss_event")
    _ev_primary   = (row.get("primary_catalyst")
                     or _cat_nested.get("primary_event")
                     or row.get("catalyst_primary_event"))
    _ev_v2        = (row.get("catalyst_v2_primary_event")
                     or _cat_nested.get("v2_primary_event"))
    _ev_list      = row.get("catalyst_events") or _cat_nested.get("events") or []

    # canonical rich event — always prefer catalyst_primary_event (146/146 populated)
    _rich_event   = row.get("catalyst_primary_event") or _ev_primary or _ev_v2 or {}

    has_scheduled = bool(_ev_scheduled)
    has_rss       = bool(_ev_rss)
    source        = str(row.get("catalyst_primary_source") or "unknown")
    reason_codes: list = []

    # ── Phase B: graduated direct score ────────────────────────────────────
    direct_score   = 0.0
    direct_present = False
    direct_type    = None
    detected_event = None
    phb_tier       = "TIER_E"
    phb_et_norm    = "UNKNOWN"
    phb_detail: dict = {}

    if bearish_conf:
        reason_codes.append("BEARISH_CATALYST_CONFLICT")
        direct_score  = 0.0
        direct_type   = "bearish_suppressed"

    elif _rich_event and _rich_event.get("event_type"):
        # Rich primary event available — use graduated Phase B scoring
        is_sched_ev = has_scheduled or source == "scheduled"
        direct_score, phb_tier, phb_et_norm, phb_detail = _catalyst_phb_direct_score(
            _rich_event, source, cat_score, is_sched_ev
        )
        if direct_score > 0:
            direct_present = True
            direct_type    = phb_et_norm.lower()
            detected_event = _rich_event
            reason_codes.append(f"PHB_GRADUATED_{phb_tier}")
        else:
            reason_codes.append("PHB_SCORE_ZERO")

    elif "theme_policy" in source and (cat_score is not None):
        # Theme-policy only row — no rich event data; conservative Tier D scoring
        direct_score, phb_tier, phb_et_norm, phb_detail = _catalyst_phb_theme_policy_score(cat_score)
        if direct_score > 0:
            direct_present = True
            direct_type    = "theme_policy"
            detected_event = _ev_primary
            reason_codes.append("PHB_THEME_POLICY_TIERD")
        else:
            reason_codes.append("PHB_THEME_POLICY_NO_SCORE")

    elif has_rss or has_scheduled:
        # Fallback: has RSS/scheduled event but primary_event has no event_type
        # Use legacy detection to avoid silent drop, but cap at Tier C max (73)
        for _cand_src, _cand_ev in [("rss", _ev_rss), ("scheduled", _ev_scheduled)]:
            if _cand_ev:
                is_direct, etype = _event_is_direct_catalyst(_cand_ev)
                if is_direct:
                    direct_score   = min(73.0, 55.0)  # Tier C cap for untyped events
                    direct_present = True
                    direct_type    = etype or _cand_src
                    detected_event = _cand_ev
                    phb_tier       = "TIER_C"
                    phb_et_norm    = (etype or "UNKNOWN").upper()
                    reason_codes.append("PHB_FALLBACK_LEGACY_DETECT")
                    break

    else:
        # Score-only path (no event, just a raw cat_score)
        if cat_available and cat_score is not None:
            raw_val = _safe_float(cat_score, 0.0)
            if raw_val >= 20.0:
                direct_score   = raw_val
                direct_present = True
                direct_type    = "score_based"
                phb_tier       = "TIER_D" if raw_val < 50 else "TIER_C"
                phb_et_norm    = "SCORE_BASED"
                reason_codes.append("PHB_SCORE_BASED")
            else:
                reason_codes.append("CATALYST_SCORE_BELOW_THRESHOLD")
        elif _avail_flat is False and cat_score is None:
            reason_codes.append("NO_ACTIVE_CATALYST")
        else:
            reason_codes.append("CATALYST_CACHE_MISSING")

    # ── Catalyst Intelligence (unavailable in snapshot) ─────────────────────
    intelligence_score  = 0.0
    intelligence_status = "unavailable"
    reason_codes.append("CATALYST_INTELLIGENCE_UNAVAILABLE")

    # ── Final score → points ─────────────────────────────────────────────────
    # When intelligence_score is implemented, restore blended formula:
    #   catalyst_raw = direct_score * 0.75 + intelligence_score * 0.25
    catalyst_raw = direct_score
    catalyst_pts = round(min(12.0, catalyst_raw / 100.0 * 12.0), 2)

    # ── Explainability ───────────────────────────────────────────────────────
    _age_days_for_expl = phb_detail.get("age_days")
    catalyst_explanation = _catalyst_phb_explanation(
        phb_tier, phb_et_norm, bearish_conf, _age_days_for_expl, source, catalyst_pts
    )

    # ── Overall status ───────────────────────────────────────────────────────
    _any_signal = cat_available or has_scheduled or has_rss or bool(detected_event)
    if not _any_signal and cat_score is None:
        cat_status = "no_catalyst" if _avail_flat is False else "missing_cache"
    else:
        cat_status = "available"

    return {
        "raw_score":                        round(catalyst_raw, 1),
        "points":                           catalyst_pts,
        "available":                        _any_signal,
        "status":                           cat_status,
        "direct_catalyst_score":            round(direct_score, 1),
        "direct_catalyst_present":          direct_present,
        "direct_catalyst_type":             direct_type,
        "direct_catalyst_event":            detected_event or _ev_primary,
        "direct_catalyst_polarity":         "bearish" if bearish_conf else ("bullish" if direct_present else None),
        "catalyst_intelligence_score":      intelligence_score,
        "news_volume_market_cap_48h":       None,
        "news_volume_market_cap_score":     None,
        "news_change_48h_pct":              None,
        "news_change_48h_score":            None,
        "catalyst_intelligence_status":     intelligence_status,
        "catalyst_intelligence_reason_codes": ["CATALYST_INTELLIGENCE_UNAVAILABLE"],
        "catalyst_alignment_raw_score":     round(catalyst_raw, 1),
        "catalyst_alignment_points":        catalyst_pts,
        "catalyst_status":                  cat_status,
        "catalyst_detail_status":           detail_status,
        "catalyst_bearish_conflict":        bearish_conf,
        "catalyst_reason_codes":            reason_codes,
        "reason_codes":                     reason_codes,
        # Phase B explainability fields
        "catalyst_direct_score":            round(direct_score, 1),
        "catalyst_event_type":              phb_et_norm,
        "catalyst_event_tier":              phb_tier,
        "catalyst_freshness_score":         phb_detail.get("fresh_mod", 0.0),
        "catalyst_relevance_score":         phb_detail.get("rel_mod", 0.0),
        "catalyst_materiality_score":       phb_detail.get("mat_mod", 0.0),
        "catalyst_phb_detail":              phb_detail,
        "catalyst_explanation":             catalyst_explanation,
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
    Investment Alignment — 12 pts max.  3-pillar model.

    investment_alignment_points = strong_pillar_count * 4

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

    # ── Continuous Investment Quality Score (V4.2.2) — computed first so
    #    inv_pts can use it (see Granularity Audit, fix P0-C) ────────────────
    fh_score_0_100  = pillars.get("financial_health_score_0_100",  0)
    cg_score_0_100  = pillars.get("current_growth_score_0_100",    0)
    fwd_score_0_100 = pillars.get("forward_growth_score_0_100",    0)

    _best_pillar   = max(fh_score_0_100, cg_score_0_100, fwd_score_0_100)
    _strong_scored = sum(1 for s in [fh_score_0_100, cg_score_0_100, fwd_score_0_100] if s >= 50)
    _breadth_bonus = {0: 0, 1: 0, 2: 7, 3: 12}.get(_strong_scored, 0)
    inv_quality_score = int(min(100, round(_best_pillar + _breadth_bonus)))

    # Points: continuous 0–15 derived from the continuous quality score.
    # Replaces the prior 4-bucket formula (strong_pillar_count × 5) which
    # produced only {0, 5, 10, 15} and caused ordering inversions where a
    # symbol with 1 pillar but IQ=88 scored below one with 3 pillars but IQ=63.
    inv_pts = round(inv_quality_score / 100.0 * 12.0, 2)
    inv_raw = float(inv_quality_score)

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

    # ── IQ rank label (inv_quality_score computed earlier, above inv_pts) ────
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


# ── Valuation scorer ──────────────────────────────────────────────────────────

def _pe_value_score(pe: float, rev_growth: Optional[float], eps_growth: Optional[float]) -> float:
    """
    P/E value score 0–100.  Lower positive P/E = higher score.
    Negative P/E treated as unavailable (returns neutral_missing handled by caller).

    Bands (absolute):
      0 < pe <=15  → 90 + (15-pe)/15*10      → 90–100
      15 < pe <=25 → 75 + (25-pe)/10*15      → 75–90
      25 < pe <=40 → 50 + (40-pe)/15*25      → 50–75
      40 < pe <=70 → 25 + (70-pe)/30*25      → 25–50
      pe > 70      → max(0, 25*(1-(pe-70)/80))→  0–25

    Growth adjustment (±10):
      strong (rev≥20% AND eps≥20%): +8
      good   (rev≥15% OR  eps≥15%): +4
      weak   (rev<5%  AND eps<5%):  −8  (only if pe>20)
      very weak (rev<0%):            −5  (additional, only if pe>15)
    """
    if pe <= 0:
        return -1.0  # sentinel = unavailable/negative

    if pe <= 15:
        base = 90.0 + (15.0 - pe) / 15.0 * 10.0
    elif pe <= 25:
        base = 75.0 + (25.0 - pe) / 10.0 * 15.0
    elif pe <= 40:
        base = 50.0 + (40.0 - pe) / 15.0 * 25.0
    elif pe <= 70:
        base = 25.0 + (70.0 - pe) / 30.0 * 25.0
    else:
        base = max(0.0, 25.0 * (1.0 - (pe - 70.0) / 80.0))

    adj = 0.0
    rg = rev_growth if rev_growth is not None else 0.0
    eg = eps_growth if eps_growth is not None else 0.0
    both_available = rev_growth is not None or eps_growth is not None

    if both_available:
        if rg >= 20.0 and eg >= 20.0:
            adj += 8.0
        elif rg >= 15.0 or eg >= 15.0:
            adj += 4.0
        if pe > 20.0 and rg < 5.0 and eg < 5.0:
            adj -= 8.0
        if pe > 15.0 and rg < 0.0:
            adj -= 5.0

    return _clamp(base + adj, 0.0, 100.0)


def _ps_value_score(ps: float, rev_growth: Optional[float], gross_margin: Optional[float]) -> float:
    """
    P/S value score 0–100.  Lower P/S = higher score.

    Bands:
      ps <= 1      → 90–100  (95 base)
      1 < ps <= 3  → 75–90   (lerp)
      3 < ps <= 6  → 55–75   (lerp)
      6 < ps <= 10 → 35–55   (lerp)
      10 < ps <=20 → 15–35   (lerp)
      ps > 20      → 0–15    (lerp, floor 0)

    Growth/margin adjustment (±10):
      strong growth (rev≥30%) + strong margin (gm≥50%): +10
      good growth   (rev≥20%) + ok margin    (gm≥35%):  +6
      good growth alone (rev≥20%):                       +4
      ok margin alone   (gm≥50%):                        +3
      weak growth (rev<5%) + high PS (ps>6):             −8
      very weak  (rev<0%):                               −5  (additional)
    """
    if ps is None or ps <= 0:
        return -1.0  # sentinel = unavailable

    if ps <= 1.0:
        base = 95.0
    elif ps <= 3.0:
        base = 75.0 + (3.0 - ps) / 2.0 * 15.0
    elif ps <= 6.0:
        base = 55.0 + (6.0 - ps) / 3.0 * 20.0
    elif ps <= 10.0:
        base = 35.0 + (10.0 - ps) / 4.0 * 20.0
    elif ps <= 20.0:
        base = 15.0 + (20.0 - ps) / 10.0 * 20.0
    else:
        base = max(0.0, 15.0 * (1.0 - (ps - 20.0) / 30.0))

    adj = 0.0
    rg = rev_growth   if rev_growth   is not None else 0.0
    gm = gross_margin if gross_margin is not None else 0.0
    rg_avail = rev_growth   is not None
    gm_avail = gross_margin is not None

    if rg_avail and gm_avail:
        if rg >= 30.0 and gm >= 50.0:
            adj += 10.0
        elif rg >= 20.0 and gm >= 35.0:
            adj += 6.0
        elif rg >= 20.0:
            adj += 4.0
        elif gm >= 50.0:
            adj += 3.0
    elif rg_avail:
        if rg >= 30.0: adj += 6.0
        elif rg >= 20.0: adj += 3.0
    elif gm_avail:
        if gm >= 50.0: adj += 3.0

    if ps > 6.0 and rg_avail and rg < 5.0:
        adj -= 8.0
    if rg_avail and rg < 0.0:
        adj -= 5.0

    return _clamp(base + adj, 0.0, 100.0)


def _fpe_value_score(fpe: float, eps_fwd: Optional[float], rev_fwd: Optional[float]) -> float:
    """
    Forward P/E value score 0–100.  Lower positive Forward P/E = higher score.

    Bands:
      0 < fpe <= 20 → 85–100  (lerp)
      20 < fpe <=35 → 65–85   (lerp)
      35 < fpe <=50 → 40–65   (lerp)
      50 < fpe <=80 → 20–40   (lerp)
      fpe > 80      → 0–20    (lerp, floor 0)

    Forward growth adjustment (±10):
      strong fwd eps (≥30%) OR strong fwd rev (≥30%): +8
      good fwd eps (≥20%) OR good fwd rev (≥20%):    +4
      weak fwd eps (<5%) AND weak fwd rev (<5%):      −8  (only if fpe>25)
      negative fwd:                                   −5  (additional)
    """
    if fpe <= 0:
        return -1.0  # sentinel = unavailable/negative

    if fpe <= 20.0:
        base = 85.0 + (20.0 - fpe) / 20.0 * 15.0
    elif fpe <= 35.0:
        base = 65.0 + (35.0 - fpe) / 15.0 * 20.0
    elif fpe <= 50.0:
        base = 40.0 + (50.0 - fpe) / 15.0 * 25.0
    elif fpe <= 80.0:
        base = 20.0 + (80.0 - fpe) / 30.0 * 20.0
    else:
        base = max(0.0, 20.0 * (1.0 - (fpe - 80.0) / 80.0))

    adj = 0.0
    ef = eps_fwd if eps_fwd is not None else 0.0
    rf = rev_fwd if rev_fwd is not None else 0.0
    ef_avail = eps_fwd is not None
    rf_avail = rev_fwd is not None

    if ef_avail or rf_avail:
        if ef >= 30.0 or rf >= 30.0:
            adj += 8.0
        elif ef >= 20.0 or rf >= 20.0:
            adj += 4.0
        if fpe > 25.0 and ef < 5.0 and rf < 5.0:
            adj -= 8.0
        if (ef_avail and ef < 0.0) or (rf_avail and rf < 0.0):
            adj -= 5.0

    return _clamp(base + adj, 0.0, 100.0)


def _score_valuation_v42(
    row: dict,
    fundamentals_map: Optional[dict] = None,
) -> dict:
    """
    Valuation — 8 pts max.

    Inputs (all from watchlist_fundamentals_cache, zero provider calls):
      PE Ratio       → pe_value_score  (0–100)
      PS Ratio       → ps_value_score  (0–100)
      Forward P/E    → forward_pe_value_score (0–100)

    valuation_quality_score = pe_w*pe_score + ps_w*ps_score + fpe_w*fpe_score
    valuation_alignment_points = valuation_quality_score / 100 * 8

    Missing data:
      All 3 missing  → points=0, coverage=unavailable
      2 missing      → use available score * 0.70 confidence haircut
      1 missing      → average of 2 available * 0.85 confidence haircut

    Negative P/E or Negative Forward P/E treated as unavailable (unprofitable).
    P/S is most useful when P/E is negative/missing.
    """
    sym = str(row.get("symbol") or "").upper()
    reason_codes: list[str] = []

    # ── Locate fundamentals snapshot ──────────────────────────────────────────
    fund_snap: Optional[dict] = None
    if fundamentals_map:
        fund_snap = fundamentals_map.get(sym)
    if fund_snap is None:
        try:
            from data.watchlist_fundamentals_store import get_snapshot as _get_snap
            fund_snap = _get_snap(sym)
        except Exception:
            pass

    _UNAVAIL = {
        "raw_score":                    None,
        "points":                       0,
        "available":                    False,
        "status":                       "missing_cache",
        "valuation_alignment_points":   0,
        "valuation_quality_score":      0,
        "valuation_label":              "Valuation Unavailable",
        "valuation_coverage_status":    "unavailable",
        "valuation_missing_fields":     ["pe_ratio", "ps_ratio", "forward_pe"],
        "pe_ratio":                     None,
        "ps_ratio":                     None,
        "forward_pe":                   None,
        "pe_value_score":               None,
        "ps_value_score":               None,
        "forward_pe_value_score":       None,
        "valuation_reason_codes":       ["VALUATION_UNAVAILABLE"],
        "valuation_explanation":        "No fundamentals cache available.",
        "reason_codes":                 ["VALUATION_UNAVAILABLE"],
    }

    if not fund_snap:
        reason_codes.append("VALUATION_NO_FUNDAMENTALS_SNAPSHOT")
        return {**_UNAVAIL, "reason_codes": reason_codes, "valuation_reason_codes": reason_codes}

    fields = fund_snap.get("fields") or {}
    if not fields:
        reason_codes.append("VALUATION_EMPTY_FUNDAMENTALS")
        return {**_UNAVAIL, "status": "empty_fundamentals",
                "reason_codes": reason_codes, "valuation_reason_codes": reason_codes}

    def _num(key: str) -> Optional[float]:
        v = fields.get(key)
        if v is None:
            return None
        try:
            return float(str(v).replace("%", "").replace(",", "").strip())
        except (ValueError, TypeError):
            return None

    def _pct(key: str) -> Optional[float]:
        return _parse_pct(fields.get(key))

    # ── Read raw valuation inputs ─────────────────────────────────────────────
    pe_raw  = _num("PE Ratio")
    ps_raw  = _num("PS Ratio")
    # Forward P/E: try multiple stored key variants
    fpe_raw = (
        _num("Forward P/E")
        or _num("Forward PE")
        or _num("Forward P/E Ratio")
        or _num("forwardPE")
    )

    # ── Read growth/quality context for adjustments ───────────────────────────
    rev_growth_q = _pct("Revenue Growth (Q)")
    rev_growth_y = _pct("Revenue Growth (YoY)")
    eps_growth   = _pct("EPS Growth") or _pct("EPS Growth This Quarter")
    gross_margin = _pct("Gross Margin")
    rev_fwd      = (
        _pct("Rev Growth Next Quarter")
        or _pct("Revenue Growth Next Quarter")
        or _pct("Revenue Growth Est")
    )
    eps_fwd      = (
        _pct("EPS Growth Est")
        or _pct("EPS Growth Next Quarter")
        or _pct("EPS Growth Next Year")
    )

    # Use best available growth proxy for adjustments
    rev_growth_best = max(
        (v for v in [rev_growth_q, rev_growth_y] if v is not None),
        default=None,
    )

    # ── Compute sub-scores ────────────────────────────────────────────────────
    missing_fields: list[str] = []

    pe_score:  Optional[float] = None
    ps_score:  Optional[float] = None
    fpe_score: Optional[float] = None

    if pe_raw is not None:
        raw_pe = _pe_value_score(pe_raw, rev_growth_best, eps_growth)
        if raw_pe < 0:
            # Negative P/E → unprofitable; treat as unavailable
            missing_fields.append("pe_ratio")
            reason_codes.append("PE_NEGATIVE_TREATED_AS_UNAVAILABLE")
        else:
            pe_score = raw_pe
            if pe_raw <= 15:
                reason_codes.append("PE_DEEP_VALUE")
            elif pe_raw <= 25:
                reason_codes.append("PE_REASONABLE")
            elif pe_raw <= 40:
                reason_codes.append("PE_ELEVATED")
            elif pe_raw <= 70:
                reason_codes.append("PE_HIGH")
            else:
                reason_codes.append("PE_VERY_HIGH")
    else:
        missing_fields.append("pe_ratio")
        reason_codes.append("PE_MISSING")

    if ps_raw is not None and ps_raw > 0:
        raw_ps = _ps_value_score(ps_raw, rev_growth_best, gross_margin)
        if raw_ps < 0:
            missing_fields.append("ps_ratio")
            reason_codes.append("PS_UNAVAILABLE")
        else:
            ps_score = raw_ps
            if ps_raw <= 3:
                reason_codes.append("PS_LOW")
            elif ps_raw <= 6:
                reason_codes.append("PS_MODERATE")
            elif ps_raw <= 10:
                reason_codes.append("PS_ELEVATED")
            else:
                reason_codes.append("PS_HIGH")
    else:
        missing_fields.append("ps_ratio")
        reason_codes.append("PS_MISSING")

    if fpe_raw is not None:
        raw_fpe = _fpe_value_score(fpe_raw, eps_fwd, rev_fwd)
        if raw_fpe < 0:
            missing_fields.append("forward_pe")
            reason_codes.append("FORWARD_PE_NEGATIVE_TREATED_AS_UNAVAILABLE")
        else:
            fpe_score = raw_fpe
            if fpe_raw <= 20:
                reason_codes.append("FORWARD_PE_REASONABLE")
            elif fpe_raw <= 35:
                reason_codes.append("FORWARD_PE_ELEVATED")
            elif fpe_raw <= 50:
                reason_codes.append("FORWARD_PE_HIGH")
            else:
                reason_codes.append("FORWARD_PE_VERY_HIGH")
    else:
        missing_fields.append("forward_pe")
        reason_codes.append("FORWARD_PE_MISSING")

    # ── Combine scores with confidence haircut for missing data ───────────────
    available_scores = [s for s in [pe_score, ps_score, fpe_score] if s is not None]
    available_count  = len(available_scores)

    if available_count == 0:
        # All three missing
        valuation_quality = 0.0
        coverage_status   = "unavailable"
        reason_codes.append("VALUATION_ALL_METRICS_MISSING")
        val_pts = 0.0
        val_label = "Valuation Unavailable"
        val_explanation = "No P/E, P/S, or Forward P/E data available."
    else:
        raw_avg = sum(available_scores) / available_count
        # Confidence haircut: 2 missing = ×0.70; 1 missing = ×0.85; 0 missing = ×1.00
        confidence = {3: 1.00, 2: 0.85, 1: 0.70}[available_count]
        valuation_quality = _clamp(raw_avg * confidence, 0.0, 100.0)

        if available_count == 3:
            coverage_status = "available"
        else:
            coverage_status = "partial"
            reason_codes.append(f"VALUATION_PARTIAL_{3 - available_count}_MISSING")

        val_pts = round(valuation_quality / 100.0 * 8.0, 2)

        # Label
        if valuation_quality >= 80:
            val_label = "Deep Value"
            reason_codes.append("VALUATION_DEEP_VALUE")
        elif valuation_quality >= 65:
            val_label = "Attractively Valued"
            reason_codes.append("VALUATION_ATTRACTIVE")
        elif valuation_quality >= 45:
            val_label = "Fair Value"
            reason_codes.append("VALUATION_FAIR")
        elif valuation_quality >= 25:
            val_label = "Elevated Valuation"
            reason_codes.append("VALUATION_ELEVATED")
        else:
            val_label = "Expensive"
            reason_codes.append("VALUATION_EXPENSIVE")

        # Explanation
        metric_parts = []
        if pe_score  is not None: metric_parts.append(f"P/E={pe_raw:.1f}(score={pe_score:.0f})")
        if ps_score  is not None: metric_parts.append(f"P/S={ps_raw:.1f}(score={ps_score:.0f})")
        if fpe_score is not None: metric_parts.append(f"FwdPE={fpe_raw:.1f}(score={fpe_score:.0f})")
        conf_str = "" if confidence == 1.0 else f" conf={confidence:.0%}"
        val_explanation = f"{val_label}: {', '.join(metric_parts)}{conf_str} → quality={valuation_quality:.0f}/100 pts={val_pts}"

    val_pts = round(valuation_quality / 100.0 * 8.0, 2) if available_count > 0 else 0.0

    # ── Forward P/E metadata ──────────────────────────────────────────────────
    _fpe_source      = fields.get("forward_pe_source")       if fields else None
    _fpe_approx      = fields.get("forward_pe_is_approximate") if fields else None
    _fpe_miss_reason: Optional[str] = None
    if fpe_raw is None:
        _fpe_miss_reason = "FORWARD_PE_NOT_IN_CACHE"
    elif fpe_raw < 0:
        _fpe_miss_reason = "FORWARD_PE_NEGATIVE"

    return {
        "raw_score":                            round(valuation_quality, 1),
        "points":                               val_pts,
        "available":                            available_count > 0,
        "status":                               coverage_status if available_count > 0 else "unavailable",
        "valuation_alignment_points":           val_pts,
        "valuation_quality_score":              round(valuation_quality, 1),
        "valuation_label":                      val_label,
        "valuation_coverage_status":            coverage_status if available_count > 0 else "unavailable",
        "valuation_missing_fields":             missing_fields,
        "pe_ratio":                             pe_raw,
        "ps_ratio":                             ps_raw,
        "forward_pe":                           fpe_raw,
        "pe_value_score":                       round(pe_score,  1) if pe_score  is not None else None,
        "ps_value_score":                       round(ps_score,  1) if ps_score  is not None else None,
        "forward_pe_value_score":               round(fpe_score, 1) if fpe_score is not None else None,
        "valuation_forward_pe_source":          _fpe_source,
        "valuation_forward_pe_is_approximate":  _fpe_approx,
        "valuation_forward_pe_missing_reason":  _fpe_miss_reason,
        "valuation_reason_codes":               reason_codes,
        "valuation_explanation":                val_explanation,
        "reason_codes":                         reason_codes,
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

    8 core components x 12 pts = 96, social = 8 → total possible = 104.
    Normalise to 100.
    """
    comp_weights = {
        "theme_alignment":      12,
        "stage_quality":        12,
        "options_alignment":    12,
        "technical_setup":      12,
        "entry_exit":           12,
        "catalyst_alignment":   12,
        "investment_alignment": 12,
        "valuation":            12,
    }
    _KNOWN_STATE_STATUSES = {
        "confirmed_no_options",
        "no_catalyst",
        "lkg_market_closed",
        "available_cached",
        "available_live",
        "stale_but_usable",
        "available",
        "partial",          # valuation: 1 or 2 of 3 metrics present
    }

    total_possible = sum(comp_weights.values()) + 8  # 104
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
    val_comp    = _score_valuation_v42(snapshot_row, fundamentals_map)

    core_score = round(min(100.0, sum([
        theme_comp["points"],
        stage_comp["points"],
        opts_comp["points"],
        tech_comp["points"],
        entry_comp["points"],
        cat_comp["points"],
        invest_comp["points"],
        val_comp["points"],
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
        "valuation":          val_comp,
    }

    # ── Normalized core score for bucket assignment ────────────────────────────
    _COMP_MAX_PTS = {
        "theme_alignment":      15.0,
        "stage_quality":        15.0,
        "options_alignment":    18.0,
        "technical_setup":       8.0,
        "entry_exit":           12.0,
        "catalyst_alignment":   12.0,
        "investment_alignment": 12.0,
        "valuation":             8.0,
    }
    available_max = sum(
        _COMP_MAX_PTS[k] for k, c in components.items() if c.get("available")
    )
    if available_max > 0:
        normalized_core = min(100.0, (core_score / available_max) * 100.0)
    else:
        normalized_core = 0.0

    normalized_total = round(min(125.0, normalized_core + bonus_score), 1)

    # ── Actionability gate score (excludes valuation) ─────────────────────────
    # Valuation affects ranking/display but NOT actionability gates (READY /
    # NEAR_ACTIONABLE / etc.).  A technically-ready setup must not be demoted
    # because valuation data is expensive or partially missing.
    # Gate uses the 7 non-valuation components normalized to their own
    # available_max so the threshold behaviour is stable across coverage states.
    _ACT_GATE_MAXES: dict = {
        "theme_alignment":      15.0,
        "stage_quality":        15.0,
        "options_alignment":    18.0,
        "technical_setup":       8.0,
        "entry_exit":           12.0,
        "catalyst_alignment":   12.0,
        "investment_alignment": 12.0,
    }
    _act_core_pts   = sum(components[k]["points"] for k in _ACT_GATE_MAXES)
    _act_avail_max  = sum(
        _ACT_GATE_MAXES[k] for k in _ACT_GATE_MAXES
        if components.get(k, {}).get("available")
    )
    if _act_avail_max > 0:
        _act_norm_core = min(100.0, (_act_core_pts / _act_avail_max) * 100.0)
    else:
        _act_norm_core = 0.0
    actionability_gate_score = round(min(125.0, _act_norm_core + bonus_score), 1)

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
    # Both use actionability_gate_score (7-component) so valuation dilution
    # cannot collapse READY symbols that remain technically actionable.
    #
    # V4.2.6 — CONSTRUCTIVE_RETEST_AFTER_EXTENSION gate override:
    # The entry_exit scorer (line ~677) and Phase 6 multi-path (line ~2112) both
    # recognise extension_reset_state="CONSTRUCTIVE_RETEST_AFTER_EXTENSION" as a
    # constructive signal even when constructive_extension=False in the snapshot.
    # The bucket / actionability hard-gate must apply the same override so a
    # valid breakout-floor retest is NOT misclassified as WATCH_FOR_RESET simply
    # because the raw snapshot field hasn't been promoted.
    _gate_ext_reset      = str(snapshot_row.get("extension_reset_state") or "").upper()
    _gate_is_constr_retest = (_gate_ext_reset == "CONSTRUCTIVE_RETEST_AFTER_EXTENSION")
    _gate_chase          = bool(snapshot_row.get("chase_extension"))
    _gate_constructive   = bool(snapshot_row.get("constructive_extension")) or _gate_is_constr_retest

    v42_bucket = _assign_v42_bucket(
        normalized_total = actionability_gate_score,
        core_score       = core_score,
        major_llc    = bool(snapshot_row.get("major_lower_low_confirmed")),
        chase        = _gate_chase,
        constructive = _gate_constructive,
        asst_status  = str(snapshot_row.get("active_support_status") or ""),
        entry_exit_pts = entry_comp["points"],
        invest_pts     = invest_comp["points"],
        confidence     = v42_confidence,
    )

    v42_actionability = _derive_actionability_v42(
        normalized_total = actionability_gate_score,
        bucket       = v42_bucket,
        major_llc    = bool(snapshot_row.get("major_lower_low_confirmed")),
        chase        = _gate_chase,
        constructive = _gate_constructive,
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
    if _gate_is_constr_retest:
        all_reason_codes.append("CONSTR_RETEST_GATE_OVERRIDE")
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
        "catalyst_direct_score":        cat_comp.get("catalyst_direct_score"),
        "catalyst_event_type":          cat_comp.get("catalyst_event_type"),
        "catalyst_event_tier":          cat_comp.get("catalyst_event_tier"),
        "catalyst_freshness_score":     cat_comp.get("catalyst_freshness_score"),
        "catalyst_relevance_score":     cat_comp.get("catalyst_relevance_score"),
        "catalyst_materiality_score":   cat_comp.get("catalyst_materiality_score"),
        "catalyst_explanation":         cat_comp.get("catalyst_explanation"),
        "catalyst_reason_codes":        cat_comp.get("catalyst_reason_codes"),
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
        "valuation_alignment_points":     val_comp["points"],
        "valuation_quality_score":        val_comp.get("valuation_quality_score"),
        "valuation_label":                val_comp.get("valuation_label"),
        "valuation_coverage_status":      val_comp.get("valuation_coverage_status"),
        "valuation_missing_fields":       val_comp.get("valuation_missing_fields"),
        "valuation_pe_ratio":             val_comp.get("pe_ratio"),
        "valuation_ps_ratio":             val_comp.get("ps_ratio"),
        "valuation_forward_pe":           val_comp.get("forward_pe"),
        "valuation_pe_value_score":       val_comp.get("pe_value_score"),
        "valuation_ps_value_score":       val_comp.get("ps_value_score"),
        "valuation_forward_pe_value_score":     val_comp.get("forward_pe_value_score"),
        "valuation_forward_pe_source":          val_comp.get("valuation_forward_pe_source"),
        "valuation_forward_pe_is_approximate":  val_comp.get("valuation_forward_pe_is_approximate"),
        "valuation_forward_pe_missing_reason":  val_comp.get("valuation_forward_pe_missing_reason"),
        "valuation_reason_codes":         val_comp.get("valuation_reason_codes"),
        "valuation_explanation":          val_comp.get("valuation_explanation"),
        "caelyn_confluence_actionability_gate_score": actionability_gate_score,
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
