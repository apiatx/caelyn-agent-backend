"""
wave_structure_engine.py
========================
Lightweight deterministic wave structure classifier.

Zero provider calls. Zero LLM calls.
Inputs: OHLC bars + pre-computed stage/entry context fields.

Wave labels
-----------
IMPULSE_LEG             Strong up-move, price still near recent high.
FIRST_PULLBACK          First significant pullback after initial impulse.
SECOND_LEG_CONTINUATION Second leg up after a completed first pullback.
MOMENTUM_WAVE_PULLBACK  Pullback in a multi-wave momentum sequence.
RIGHT_SIDE_BASE         Consolidation at elevated levels (high base).
FAILED_WAVE             Structure broken: lower lows confirmed.
NO_CLEAR_WAVE           Insufficient data or ambiguous.
"""
from __future__ import annotations
from typing import Optional


WAVE_IMPULSE          = "IMPULSE_LEG"
WAVE_FIRST_PULLBACK   = "FIRST_PULLBACK"
WAVE_SECOND_LEG       = "SECOND_LEG_CONTINUATION"
WAVE_MOMENTUM_BACK    = "MOMENTUM_WAVE_PULLBACK"
WAVE_RIGHT_SIDE_BASE  = "RIGHT_SIDE_BASE"
WAVE_FAILED           = "FAILED_WAVE"
WAVE_NO_CLEAR         = "NO_CLEAR_WAVE"

# Entry states that force RIGHT_SIDE_BASE regardless of price location
_HIGH_BASE_STATES = frozenset({
    "HIGH_BASE_FORMING", "HIGH_BASE_COILING", "HIGH_BASE_READY",
    "WAIT_FOR_RETEST", "BREAKOUT_CONFIRMED",
})

# Entry states / families that force FAILED_WAVE
_FAILED_ENTRY_STATES = frozenset({
    "FAILED_BREAKOUT", "DOWNTREND", "SUPPORT_LOST",
    "LOWER_LOW_CONFIRMED", "REVERSAL_WATCH",
})
_FAILED_FAMILIES = frozenset({"BROKEN_OR_UNCLEAR"})

# Extension states considered "severe" (still in impulse or just past peak)
_SEVERE_EXT = frozenset({
    "EXTREME_EXTENSION", "EXTENDED", "VERTICAL", "VOLUME_CLIMAX", "CROWDED_MOVE",
})


def _safe_high(bar: dict) -> float:
    v = bar.get("high")
    if v is not None:
        try:
            return float(v)
        except (TypeError, ValueError):
            pass
    try:
        return float(bar.get("close") or 0)
    except (TypeError, ValueError):
        return 0.0


def _safe_low(bar: dict) -> float:
    v = bar.get("low")
    if v is not None:
        try:
            return float(v)
        except (TypeError, ValueError):
            pass
    try:
        return float(bar.get("close") or 0)
    except (TypeError, ValueError):
        return 0.0


def _safe_close(bar: dict) -> Optional[float]:
    try:
        v = float(bar.get("close") or 0)
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


def classify_wave_structure(
    bars:                  list[dict],
    entry_state:           Optional[str]   = None,
    entry_family:          Optional[str]   = None,
    extension_state:       Optional[str]   = None,
    stage_alignment_score: Optional[float] = None,
    prior_26w_trend_pct:   Optional[float] = None,
) -> dict:
    """
    Classify the wave structure of a ticker from daily bars + context fields.

    Returns
    -------
    dict with keys:
      wave_structure_label (str)
      wave_structure_score (int, 0-100)
      wave_structure_reason_codes (list[str])
      wave_structure_available (bool)
    """
    if not bars or len(bars) < 20:
        return _null_result(["INSUFFICIENT_BARS"])

    try:
        e_state  = (entry_state  or "").upper()
        e_family = (entry_family or "").upper()
        ext      = (extension_state or "HEALTHY").upper()
        stage_s  = float(stage_alignment_score or 0.0)
        big_move = (prior_26w_trend_pct is not None and prior_26w_trend_pct >= 40.0)

        # ── Hard overrides ────────────────────────────────────────────────────
        if e_family in _FAILED_FAMILIES or e_state in _FAILED_ENTRY_STATES:
            return {
                "wave_structure_label":        WAVE_FAILED,
                "wave_structure_score":        10,
                "wave_structure_reason_codes": [
                    f"FAILED_FAMILY_{e_family}", f"STATE_{e_state}",
                ],
                "wave_structure_available":    True,
            }

        # ── Sort and windowed bars ────────────────────────────────────────────
        bars_s = sorted(bars, key=lambda b: str(b.get("date", "") or "")[:10])
        w_bars = bars_s[-60:] if len(bars_s) >= 60 else bars_s
        n = len(w_bars)

        closes  = [_safe_close(b) for b in w_bars]
        closes  = [c for c in closes if c is not None]
        if len(closes) < 10:
            return _null_result(["NO_VALID_CLOSES"])

        highs  = [_safe_high(b) for b in w_bars]
        lows   = [_safe_low(b)  for b in w_bars]
        price  = closes[-1]

        recent_max = max(highs) if highs else price
        recent_min = min(lows)  if lows  else price

        drawdown_pct = round((price - recent_max) / recent_max * 100, 2) if recent_max > 0 else 0.0
        recovery_pct = round((price - recent_min) / recent_min * 100, 2) if recent_min > 0 else 0.0
        range_pct    = round((recent_max - recent_min) / recent_min * 100, 2) if recent_min > 0 else 0.0
        tight_consol = range_pct <= 10.0

        # 3-bar swing highs and lows
        swing_highs: list[int] = []
        swing_lows:  list[int] = []
        for i in range(1, n - 1):
            if highs[i] >= highs[i - 1] and highs[i] >= highs[i + 1]:
                swing_highs.append(i)
            if lows[i] <= lows[i - 1] and lows[i] <= lows[i + 1]:
                swing_lows.append(i)

        hh_count = sum(
            1 for i in range(1, len(swing_highs))
            if highs[swing_highs[i]] > highs[swing_highs[i - 1]]
        )
        hl_count = sum(
            1 for i in range(1, len(swing_lows))
            if lows[swing_lows[i]] > lows[swing_lows[i - 1]]
        )
        ll_count = sum(
            1 for i in range(1, len(swing_lows))
            if lows[swing_lows[i]] < lows[swing_lows[i - 1]]
        )

        reasons: list[str] = [
            f"SWING_H{len(swing_highs)}_L{len(swing_lows)}",
            f"HH{hh_count}_HL{hl_count}_LL{ll_count}",
        ]

        # ── FAILED_WAVE ───────────────────────────────────────────────────────
        if ll_count >= 2 and hh_count == 0:
            return {
                "wave_structure_label":        WAVE_FAILED,
                "wave_structure_score":        15,
                "wave_structure_reason_codes": reasons + [f"LOWER_LOWS_{ll_count}"],
                "wave_structure_available":    True,
            }

        # ── RIGHT_SIDE_BASE ───────────────────────────────────────────────────
        is_high_base = e_state in _HIGH_BASE_STATES
        near_highs   = drawdown_pct >= -10.0
        if is_high_base or (ext in _SEVERE_EXT and near_highs and stage_s >= 50):
            score = 75 if tight_consol else 70
            reasons.append(f"DRAWDOWN_{drawdown_pct:.1f}PCT")
            if is_high_base:
                reasons.append(f"HIGH_BASE_{e_state}")
            return {
                "wave_structure_label":        WAVE_RIGHT_SIDE_BASE,
                "wave_structure_score":        min(90, score),
                "wave_structure_reason_codes": reasons,
                "wave_structure_available":    True,
            }

        # ── IMPULSE_LEG ───────────────────────────────────────────────────────
        if ext in _SEVERE_EXT and drawdown_pct >= -5.0:
            score = 60 + (20 if big_move else 10)
            reasons.append(f"IMPULSE_{ext}")
            reasons.append(f"NEAR_HIGH_{drawdown_pct:.1f}PCT")
            return {
                "wave_structure_label":        WAVE_IMPULSE,
                "wave_structure_score":        min(80, score),
                "wave_structure_reason_codes": reasons,
                "wave_structure_available":    True,
            }

        # ── SECOND_LEG_CONTINUATION ───────────────────────────────────────────
        if hh_count >= 2 and hl_count >= 1 and drawdown_pct < -5.0 and recovery_pct >= 10.0:
            score = 72 + (8 if hh_count >= 3 else 0)
            reasons.append(f"MULTI_HH_{hh_count}_HL_{hl_count}")
            reasons.append(f"RECOVERY_{recovery_pct:.1f}PCT")
            return {
                "wave_structure_label":        WAVE_SECOND_LEG,
                "wave_structure_score":        min(85, score),
                "wave_structure_reason_codes": reasons,
                "wave_structure_available":    True,
            }

        # ── MOMENTUM_WAVE_PULLBACK ────────────────────────────────────────────
        if len(swing_highs) >= 3 and drawdown_pct < -5.0 and hh_count >= 1:
            score = 65
            reasons.append(f"MOMENTUM_SWINGS_{len(swing_highs)}")
            reasons.append(f"PULLBACK_{drawdown_pct:.1f}PCT")
            return {
                "wave_structure_label":        WAVE_MOMENTUM_BACK,
                "wave_structure_score":        score,
                "wave_structure_reason_codes": reasons,
                "wave_structure_available":    True,
            }

        # ── FIRST_PULLBACK ────────────────────────────────────────────────────
        if len(swing_highs) <= 2 and drawdown_pct < -5.0 and big_move:
            score = 68
            reasons.append(f"FIRST_PULLBACK_DRAW_{drawdown_pct:.1f}PCT")
            if prior_26w_trend_pct is not None:
                reasons.append(f"PRIOR_MOVE_{prior_26w_trend_pct:.0f}PCT")
            return {
                "wave_structure_label":        WAVE_FIRST_PULLBACK,
                "wave_structure_score":        score,
                "wave_structure_reason_codes": reasons,
                "wave_structure_available":    True,
            }

        # ── Default ───────────────────────────────────────────────────────────
        reasons.append(f"DRAWDOWN_{drawdown_pct:.1f}PCT_EXT_{ext}")
        return {
            "wave_structure_label":        WAVE_NO_CLEAR,
            "wave_structure_score":        30,
            "wave_structure_reason_codes": reasons,
            "wave_structure_available":    True,
        }

    except Exception as exc:
        return _null_result([f"WAVE_ENGINE_ERROR_{type(exc).__name__}"])


def _null_result(reason_codes: Optional[list[str]] = None) -> dict:
    return {
        "wave_structure_label":        WAVE_NO_CLEAR,
        "wave_structure_score":        0,
        "wave_structure_reason_codes": reason_codes or [],
        "wave_structure_available":    False,
    }
