"""
theme_rotation_service.py — Phase 6: Theme Rotation V2
========================================================
Pure cache-read service — zero external API calls, zero provider calls.

Reads from:
  • backend/data/themes_rs_lkg.json     — Stage analysis + RS scores per theme
  • backend/data/x_consensus_weekly.json — Social/sentiment signals
  • backend/data/options_master_lkg_v1.json — Options flow signals

Computes a rotation score for each theme, classifies themes into rotation phases,
and ranks them so the UI can display a "what's rotating now" summary.

Rotation Phases
---------------
  LEADING      — strong RS + rising momentum + breadth expanding
  CONFIRMING   — good RS, breadth solid, not yet at top velocity
  STALLING     — RS has been strong but showing deceleration signs
  LAGGING      — underperforming SPY, weak RS
  BOTTOMING    — Stage 1-2 transition candidates, improving breadth
  UNCLASSIFIED — insufficient data
"""

from __future__ import annotations

import json
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

# ── Data paths ─────────────────────────────────────────────────────────────────
_BASE = Path(__file__).parent.parent
_THEMES_RS_LKG = _BASE / "data" / "themes_rs_lkg.json"
_X_CONSENSUS   = _BASE / "data" / "x_consensus_weekly.json"
_OPTIONS_LKG   = _BASE / "data" / "options_master_lkg_v1.json"

# ── Rotation phase labels ──────────────────────────────────────────────────────
PHASE_LEADING      = "LEADING"
PHASE_CONFIRMING   = "CONFIRMING"
PHASE_STALLING     = "STALLING"
PHASE_LAGGING      = "LAGGING"
PHASE_BOTTOMING    = "BOTTOMING"
PHASE_UNCLASSIFIED = "UNCLASSIFIED"

# ── Rotation score weights (sum to 1.0) ─────────────────────────────────────
_W_RS          = 0.35   # 8w RS vs SPY (core signal)
_W_STAGE       = 0.20   # Stage quality (2 > 2b > 1/12 > 3 > 4)
_W_BREADTH     = 0.15   # % of members above 30w MA
_W_MOMENTUM    = 0.15   # 30-day return + trend_accel_20d
_W_OPTIONS     = 0.10   # net options flow signal from master screener
_W_SOCIAL      = 0.05   # social screener tickers matching this theme


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clamp01(v: float) -> float:
    return max(0.0, min(1.0, v))


def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


# ── File loaders ───────────────────────────────────────────────────────────────

def _load_themes_rs() -> list[dict]:
    try:
        if _THEMES_RS_LKG.exists():
            raw = json.loads(_THEMES_RS_LKG.read_text())
            rows = raw.get("rows") if isinstance(raw, dict) else raw
            if isinstance(rows, list):
                return rows
    except Exception:
        pass
    return []


def _load_social_map() -> dict[str, float]:
    """Return {TICKER_UPPER: backend_score} from x_consensus_weekly.json."""
    try:
        if _X_CONSENSUS.exists():
            raw = json.loads(_X_CONSENSUS.read_text())
            ranked = raw.get("_backend_ranked") or []
            return {
                e["ticker"].upper(): _safe_float(e.get("backend_score"))
                for e in ranked if e.get("ticker")
            }
    except Exception:
        pass
    return {}


def _load_options_bias_map() -> dict[str, str]:
    """Return {TICKER_UPPER: side_bias} from options master LKG."""
    try:
        if _OPTIONS_LKG.exists():
            raw = json.loads(_OPTIONS_LKG.read_text())
            tickers = raw.get("tickers") or []
            return {
                t["ticker"].upper(): (t.get("side_bias") or "neutral")
                for t in tickers if t.get("ticker")
            }
    except Exception:
        pass
    return {}


# ── Signal transformers ────────────────────────────────────────────────────────

def _rs_signal(row: dict) -> float:
    """Normalise rs_score (theme's 8w RS vs SPY) to 0–1."""
    rs = _safe_float(row.get("rs_score") or row.get("rs_vs_spy"))
    # rs_score in themes_rs_lkg is in pct units (−30 to +30 typical range)
    # Map: −20 → 0, 0 → 0.5, +20 → 1.0
    return _clamp01((rs + 20.0) / 40.0)


def _stage_signal(row: dict) -> float:
    """Map stage label to 0–1 quality score."""
    stage = row.get("stage") or row.get("stage_label") or ""
    _map = {
        "Stage 2: Advance":        1.00,
        "Stage 2b: Breakout":      0.95,
        "Stage 1-2: Watch":        0.70,
        "Stage 3m: Late Momentum": 0.55,
        "Stage 1: Base":           0.45,
        "Stage 3: Top":            0.30,
        "Stage 4: Decline":        0.05,
    }
    # Also handle integer stage field
    stage_int = row.get("stage_int")
    _int_map = {2: 0.90, 1: 0.45, 3: 0.30, 4: 0.05}
    if stage in _map:
        return _map[stage]
    if stage_int in _int_map:
        return _int_map[stage_int]
    return 0.40   # default / unclassified


def _breadth_signal(row: dict) -> float:
    """% of members above 30w MA → 0–1."""
    b = row.get("breadth_pct") or row.get("breadth_above_30w") or row.get("breadth_above_30w_ma_pct")
    if b is None:
        return 0.5   # neutral default
    v = _safe_float(b)
    # Treat as already a pct in 0–100 range
    if v > 1.0:
        v = v / 100.0
    return _clamp01(v)


def _momentum_signal(row: dict) -> float:
    """30d return + trend_accel normalised."""
    perf = row.get("performance") or {}
    ret_30d = _safe_float(perf.get("30D") or perf.get("30d"))
    accel   = _safe_float(row.get("trend_accel_20d"))
    # 30D return: −20 → 0, 0 → 0.5, +20 → 1.0
    ret_sig = _clamp01((ret_30d + 20.0) / 40.0)
    # trend_accel in rough pct units; −2 → 0, 0 → 0.5, +2 → 1.0
    acc_sig = _clamp01((accel + 2.0) / 4.0)
    return (ret_sig + acc_sig) / 2.0


def _options_signal(proxy_syms: list[str], options_bias: dict[str, str]) -> float:
    """Fraction of proxy symbols with bullish options bias."""
    if not proxy_syms:
        return 0.5
    bullish = sum(1 for s in proxy_syms if options_bias.get(s.upper()) == "bullish")
    bearish = sum(1 for s in proxy_syms if options_bias.get(s.upper()) == "bearish")
    total   = len(proxy_syms)
    # net bullish fraction mapped to 0–1 (neutral = 0.5)
    net = (bullish - bearish) / total
    return _clamp01(0.5 + net / 2.0)


def _social_signal(leaders: list[str], social_map: dict[str, float]) -> float:
    """Average backend_score of theme leaders that appear in social screener."""
    if not leaders:
        return 0.5
    _max_score = 14.0  # empirical max from x_consensus
    scores = [
        social_map[s.upper()] / _max_score
        for s in leaders if s.upper() in social_map
    ]
    if not scores:
        return 0.5
    return _clamp01(statistics.mean(scores))


# ── Phase classifier ────────────────────────────────────────────────────────────

def _classify_phase(
    rotation_score: float,
    rs_sig:        float,
    stage_sig:     float,
    breadth_sig:   float,
    momentum_sig:  float,
) -> str:
    # LEADING: top RS, good stage, expanding breadth, strong momentum
    if (rotation_score >= 0.72 and rs_sig >= 0.70 and
            stage_sig >= 0.70 and breadth_sig >= 0.60 and momentum_sig >= 0.60):
        return PHASE_LEADING

    # CONFIRMING: solid but not top-tier velocity
    if rotation_score >= 0.58 and rs_sig >= 0.58 and stage_sig >= 0.55:
        return PHASE_CONFIRMING

    # STALLING: was good, now decelerating
    if (rotation_score >= 0.45 and momentum_sig < 0.45 and rs_sig >= 0.50):
        return PHASE_STALLING

    # BOTTOMING: Stage 1 / 1-2 Watch, weak RS but improving breadth
    if stage_sig >= 0.50 and rs_sig < 0.50 and breadth_sig >= 0.50:
        return PHASE_BOTTOMING

    # LAGGING: below-par RS + weak momentum
    if rs_sig < 0.45 and momentum_sig < 0.50:
        return PHASE_LAGGING

    return PHASE_UNCLASSIFIED


# ── Public API ─────────────────────────────────────────────────────────────────

def build_theme_rotation_snapshot() -> dict:
    """
    Compute full theme rotation snapshot from cache files.
    Returns list of themes ranked by rotation_score descending.
    No external API calls.
    """
    t0 = time.time()

    themes      = _load_themes_rs()
    social_map  = _load_social_map()
    options_bias = _load_options_bias_map()

    if not themes:
        return {
            "ok":           False,
            "error":        "themes_rs_lkg_empty_or_missing",
            "generated_at": _now_iso(),
            "themes":       [],
        }

    results: list[dict] = []

    for row in themes:
        theme_id = row.get("theme_id") or row.get("id") or "unknown"

        proxy_syms: list[str] = list(row.get("proxy_symbols") or [])
        # leaders may be strings OR dicts {"symbol": "VRTX", "return_pct": ...}
        _raw_leaders = row.get("leaders") or []
        leaders: list[str] = [
            (e["symbol"] if isinstance(e, dict) else e)
            for e in _raw_leaders
            if e and (isinstance(e, str) or (isinstance(e, dict) and e.get("symbol")))
        ]
        all_syms = list(dict.fromkeys(proxy_syms + leaders))  # dedup, order preserved

        rs_sig       = _rs_signal(row)
        stage_sig    = _stage_signal(row)
        breadth_sig  = _breadth_signal(row)
        momentum_sig = _momentum_signal(row)
        options_sig  = _options_signal(all_syms, options_bias)
        social_sig   = _social_signal(leaders, social_map)

        rotation_score = (
            _W_RS       * rs_sig +
            _W_STAGE    * stage_sig +
            _W_BREADTH  * breadth_sig +
            _W_MOMENTUM * momentum_sig +
            _W_OPTIONS  * options_sig +
            _W_SOCIAL   * social_sig
        )

        phase = _classify_phase(
            rotation_score, rs_sig, stage_sig, breadth_sig, momentum_sig
        )

        perf = row.get("performance") or {}

        results.append({
            "theme_id":       theme_id,
            "rotation_score": round(rotation_score, 4),
            "rotation_phase": phase,
            "signals": {
                "rs_signal":       round(rs_sig, 4),
                "stage_signal":    round(stage_sig, 4),
                "breadth_signal":  round(breadth_sig, 4),
                "momentum_signal": round(momentum_sig, 4),
                "options_signal":  round(options_sig, 4),
                "social_signal":   round(social_sig, 4),
            },
            "raw": {
                "rs_score":       row.get("rs_score"),
                "stage":          row.get("stage"),
                "stage_label":    row.get("stage_label"),
                "breadth_pct":    row.get("breadth_pct"),
                "trend_accel_20d":row.get("trend_accel_20d"),
                "momentum_rank":  row.get("momentum_rank"),
                "perf_7d":        perf.get("7D"),
                "perf_30d":       perf.get("30D"),
                "perf_ytd":       perf.get("YTD"),
            },
            "proxy_symbols": proxy_syms[:8],
            "leaders":       leaders[:6],   # always strings after normalisation
        })

    # Sort by rotation_score descending
    results.sort(key=lambda r: r["rotation_score"], reverse=True)

    # Rank
    for i, r in enumerate(results):
        r["rotation_rank"] = i + 1

    # Phase summary counts
    phase_counts: dict[str, int] = {}
    for r in results:
        p = r["rotation_phase"]
        phase_counts[p] = phase_counts.get(p, 0) + 1

    return {
        "ok":            True,
        "generated_at":  _now_iso(),
        "theme_count":   len(results),
        "phase_summary": phase_counts,
        "elapsed_ms":    round((time.time() - t0) * 1000, 1),
        "themes":        results,
    }


def get_leading_themes(n: int = 10) -> list[dict]:
    """Return top-N leading/confirming themes by rotation_score."""
    snap = build_theme_rotation_snapshot()
    themes = snap.get("themes") or []
    return [
        t for t in themes
        if t["rotation_phase"] in (PHASE_LEADING, PHASE_CONFIRMING)
    ][:n]
