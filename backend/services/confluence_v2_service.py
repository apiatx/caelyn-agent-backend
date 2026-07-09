"""
confluence_v2_service.py — Phase 7: Confluence V2 Shadow System
================================================================
Pure cache-read signal aggregator — zero external API calls, zero provider calls.

Reads from:
  • entry_state_service  — entry state + entry_score (Phase 4)
  • theme_rotation_service — theme rotation_score + phase (Phase 6)
  • backend/data/themes_rs_lkg.json       — stage + RS signals
  • backend/data/x_consensus_weekly.json  — social signals
  • backend/data/options_master_lkg_v1.json — options flow
  • backend/data/watchlist_stage2_lkg.json  — stage2 LKG (technical)

For each symbol on the watchlist (via Stage2 LKG), Confluence V2 assembles a
multi-signal verdict:

  confluence_score (0–100) — weighted composite
  confluence_grade (A+/A/B/C/AVOID)
  confluence_verdict (STRONG_BUY / BUY / WATCH / NEUTRAL / AVOID / SHORT_AVOID)
  signal_breakdown — per-signal scores and metadata
  confidence       — how many signals were available (0–1)

Weights
-------
  entry_state        30%  (Phase 4 engine)
  theme_rotation     20%  (Phase 6 engine)
  stage_quality      20%  (from stage analysis)
  options_flow       15%  (from options master LKG)
  social_screener    15%  (from x_consensus)
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

# ── Weights (sum to 1.0) ───────────────────────────────────────────────────────
_W_ENTRY   = 0.30
_W_THEME   = 0.20
_W_STAGE   = 0.20
_W_OPTIONS = 0.15
_W_SOCIAL  = 0.15

# ── Verdict thresholds ─────────────────────────────────────────────────────────
_VERDICTS = [
    (85, "STRONG_BUY"),
    (70, "BUY"),
    (55, "WATCH"),
    (40, "NEUTRAL"),
    (25, "AVOID"),
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clamp01(v: float) -> float:
    return max(0.0, min(1.0, v))


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
    """Load stage2 LKG keyed by symbol."""
    try:
        if _STAGE2_LKG.exists():
            raw = json.loads(_STAGE2_LKG.read_text())
            if isinstance(raw, dict):
                return {k.upper(): v for k, v in raw.items() if isinstance(v, dict)}
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
    # Use rotation_score if pre-computed (won't be unless called after theme_rotation_service)
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
    """Return (0–1 stage quality, stage_label) from stage2 LKG row."""
    if stage2_row is None:
        return 0.5, "MISSING"
    stage    = stage2_row.get("stage")
    label    = stage2_row.get("stage_label") or str(stage)
    score    = _safe_float(stage2_row.get("stage_score"), 50.0)
    # stage_score is already 0–100; normalise
    sig = _clamp01(score / 100.0)
    # Hard caps by stage
    if stage == 4:
        sig = min(sig, 0.10)
    elif stage == 3 and "3m" not in (label or ""):
        sig = min(sig, 0.35)
    return sig, label


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


def _social_signal(sym: str, social_map: dict[str, dict]) -> tuple[float, str]:
    """Return (0–1 social signal, conviction_label)."""
    entry = social_map.get(sym)
    if entry is None:
        return 0.5, "NOT_IN_SOCIAL"
    bs   = _safe_float(entry.get("backend_score"), 0.0)
    sig  = _clamp01(bs / 14.0)          # empirical max ~14
    if entry.get("has_top_conviction"):
        sig = min(1.0, sig * 1.15)
    conviction = "HIGH" if entry.get("has_top_conviction") else ("MEDIUM" if sig >= 0.5 else "LOW")
    return sig, conviction


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

    e_sig,    e_state = _entry_signal(entry_result)
    t_sig,    t_phase = _theme_signal(sym, themes_idx)
    s_sig,    s_label = _stage_signal_from_lkg(stage2_row)
    o_sig,    o_bias  = _options_signal(sym, options_map)
    soc_sig, soc_conv = _social_signal(sym, social_map)

    # Availability flags (False = missing/defaulted)
    avail = [
        entry_result is not None,
        sym in themes_idx,
        stage2_row is not None,
        sym in options_map,
        sym in social_map,
    ]

    raw_score = (
        _W_ENTRY   * e_sig   +
        _W_THEME   * t_sig   +
        _W_STAGE   * s_sig   +
        _W_OPTIONS * o_sig   +
        _W_SOCIAL  * soc_sig
    ) * 100.0

    conf  = _confidence(avail)
    score = round(raw_score, 1)

    return {
        "symbol":             sym,
        "confluence_score":   score,
        "confluence_grade":   _grade(score),
        "confluence_verdict": _verdict(score),
        "confidence":         conf,
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
                "signal":       round(soc_sig, 4),
                "conviction":   soc_conv,
                "weight":       _W_SOCIAL,
                "contribution": round(soc_sig * _W_SOCIAL * 100, 2),
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

    # Sort by confluence_score descending
    results.sort(key=lambda r: r["confluence_score"], reverse=True)
    for i, r in enumerate(results):
        r["confluence_rank"] = i + 1

    # Verdict distribution
    verdict_dist: dict[str, int] = {}
    for r in results:
        v = r["confluence_verdict"]
        verdict_dist[v] = verdict_dist.get(v, 0) + 1

    return {
        "ok":               True,
        "generated_at":     _now_iso(),
        "symbol_count":     len(results),
        "verdict_summary":  verdict_dist,
        "weights": {
            "entry_state":    _W_ENTRY,
            "theme_rotation": _W_THEME,
            "stage_quality":  _W_STAGE,
            "options_flow":   _W_OPTIONS,
            "social_screener":_W_SOCIAL,
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
        "symbol":             symbol.upper(),
        "confluence_score":   0,
        "confluence_grade":   "AVOID",
        "confluence_verdict": "AVOID",
        "confidence":         0.0,
        "error":              "compute_failed",
    }
