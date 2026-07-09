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
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

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
    """Build a ticker → theme_row lookup from themes_rs_lkg leaders field."""
    result: dict[str, dict] = {}
    try:
        if _THEMES_RS_LKG.exists():
            raw = json.loads(_THEMES_RS_LKG.read_text())
            rows = raw.get("rows") if isinstance(raw, dict) else raw
            if not isinstance(rows, list):
                return result
            for row in rows:
                leaders      = row.get("leaders") or []
                proxy_syms   = row.get("proxy_symbols") or []
                for sym in leaders + proxy_syms:
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


def _theme_signal(sym: str, themes_idx: dict[str, dict]) -> tuple[float, str]:
    """Return (0–1 theme rotation signal, phase)."""
    row = themes_idx.get(sym)
    if row is None:
        return 0.5, "NOT_IN_THEME"
    rot_score = row.get("_rotation_score")
    if rot_score is not None:
        return _clamp01(float(rot_score)), row.get("_rotation_phase", "UNCLASSIFIED")

    # Fallback: derive from raw RS + stage
    rs = _safe_float(row.get("rs_score") or row.get("rs_vs_spy"))
    rs_sig = _clamp01((rs + 20.0) / 40.0)

    stage_label = row.get("stage") or row.get("stage_label") or ""
    _sl_map = {
        "Stage 2: Advance": 1.0, "Stage 2b: Breakout": 0.95,
        "Stage 1-2: Watch": 0.70, "Stage 1: Base": 0.45,
        "Stage 3m: Late Momentum": 0.55, "Stage 3: Top": 0.30, "Stage 4: Decline": 0.05,
    }
    stage_sig = _sl_map.get(stage_label, 0.40)

    combined = 0.6 * rs_sig + 0.4 * stage_sig
    phase = "LEADING" if combined >= 0.72 else ("CONFIRMING" if combined >= 0.58 else "LAGGING")
    return combined, phase


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
    # Theme signal defaults to 0.5 when not in theme index (0% coverage case).
    # Use stage_sig >= 0.65 as a data-availability-aware proxy when theme is missing.
    theme_or_stage_ok = t_sig >= 0.55 or s_sig >= 0.65
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
) -> dict:
    t0 = time.time()

    # ── Individual signal normalizations ──────────────────────────────────────
    e_sig, e_state  = _entry_signal(entry_result)
    t_sig, t_phase  = _theme_signal(sym, themes_idx)
    s_sig, s_label  = _stage_signal_from_lkg(stage2_row)
    o_sig, o_bias   = _options_signal(sym, options_map)

    social_entry = social_map.get(sym)

    # ── Base Trade score (4 signals, renormalized weights, no social) ─────────
    base_raw = (
        _W_ENTRY   * e_sig +
        _W_THEME   * t_sig +
        _W_STAGE   * s_sig +
        _W_OPTIONS * o_sig
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
        t_sig           = t_sig,
        s_sig           = s_sig,
        o_sig           = o_sig,
        social_entry    = social_entry,
    )

    # ── Final trade score = base + bonus (clamped 0–100) ─────────────────────
    trade_score  = round(_clamp(base_score + social_bonus, 0.0, 100.0), 1)

    # ── Investment score = pure base (no social, no timing bonus) ────────────
    invest_score = base_score

    # ── Availability flags ────────────────────────────────────────────────────
    avail = [
        entry_result is not None,
        sym in themes_idx,
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
        # ── Social bonus metadata ────────────────────────────────────────────
        "social_bonus_eligible":        soc_eligible,
        "social_bonus_reason":          soc_reason,
        "social_risk_flag":             soc_risk,
        "social_fields": {
            "covered":            social_entry is not None,
            "backend_score":      round(soc_backend, 3),
            "breadth_score":      round(soc_breadth, 3),
            "freshness_score":    round(soc_fresh, 3),
            "has_top_conviction": soc_top,
        },
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
                "contribution": round(t_sig * _W_THEME * 100, 2),
                "available":    avail[1],
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
    themes_idx  = _load_themes_rs_index()
    options_map = _load_options_map()
    social_map  = _load_social_map()

    # Load entry state LKG if available
    entry_lkg: dict[str, dict] = {}
    try:
        from services.entry_state_service import get_all_entry_state_lkg
        entry_lkg = get_all_entry_state_lkg()
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

    results: list[dict] = []
    social_bonus_counts = {"0": 0, "2_4": 0, "5_7": 0, "8_10": 0, "eligible": 0, "applied": 0}

    for sym in universe:
        r = _compute_confluence(
            sym          = sym,
            entry_result = entry_lkg.get(sym),
            stage2_row   = stage2_lkg.get(sym),
            themes_idx   = themes_idx,
            options_map  = options_map,
            social_map   = social_map,
        )
        results.append(r)

        # Tally social bonus distribution
        bonus = r.get("social_bonus_score", 0)
        if r.get("social_bonus_eligible"):
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
        "elapsed_ms":  round((time.time() - t0) * 1000, 1),
        "results":     results,
    }


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
