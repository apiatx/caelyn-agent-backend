"""
Options Alignment — zero-provider-call read layer for Trade Alignment.

Answers: "How strongly does CURRENT options premium flow confirm a
swing-trade opportunity, and is that pressure strengthening or weakening?"

This module performs NO network/provider calls of any kind. It only reads:
  - the existing canonical options composite produced by
    backend/data/options_flow_engine.py (via the zero-call merge helper
    backend/data/options_theme_supplement.py::get_combined_ticker_data)
  - the existing canonical Net Premium history helpers in
    backend/data/options_net_premium_history.py (Neon
    public.options_net_premium_daily)

It does NOT modify the Options Flow producer, does NOT duplicate the
composite-score or delta-calculation formulas, and does NOT implement the
final four-component Trade Alignment score. It returns exactly ONE
0-100 "options_alignment_score" signal plus full transparency fields.
"""

from __future__ import annotations

from datetime import date
from typing import Optional

# Suggested (non-final) internal weighting, per Trade Alignment Foundation spec.
_CURRENT_WEIGHT = 0.70
_DIRECTION_WEIGHT = 0.30

# Suggested (non-final) direction-horizon mix when all three horizons exist.
_HORIZON_WEIGHTS = {"1d": 0.20, "7d": 0.35, "30d": 0.45}

# options_current_composite is already a 0-100 scale (see
# options_flow_engine.py composite_score / final_composite_score). Kept as a
# named constant so a future rescale only needs to change one place.
_CURRENT_COMPOSITE_SCALE_MAX = 100.0


def _normalize_current_composite(raw_composite: Optional[float]) -> Optional[float]:
    """
    options_flow_engine.py's composite_score / final_composite_score is
    already produced on a 0-100 scale. This is a pass-through clamp, not a
    reformulation — no re-derivation of the composite itself.
    """
    if raw_composite is None:
        return None
    try:
        val = float(raw_composite)
    except (TypeError, ValueError):
        return None
    return max(0.0, min(_CURRENT_COMPOSITE_SCALE_MAX, val))


def _fetch_net_premium_row(ticker: str, entity_type: str = "stock") -> tuple[Optional[dict], list[dict]]:
    """
    Read Net Premium current row + history for one ticker via the existing
    canonical bulk history helper (no duplication of the delta math).

    Returns (current_row_or_None, history_excluding_current).
    history is sorted newest-first (as returned by
    get_historical_snapshots_bulk); the first row *may* be today's snapshot,
    which is treated as "current" and excluded from the delta history list.
    """
    try:
        from datetime import timedelta
        from data.options_net_premium_history import get_historical_snapshots_bulk

        today = date.today()
        since = today - timedelta(days=35)
        rows_by_entity = get_historical_snapshots_bulk([(entity_type, ticker.upper())], since)
        history = rows_by_entity.get((entity_type, ticker.upper()), [])
    except Exception:
        return None, []

    if not history:
        return None, []

    # history is newest-first; the newest available snapshot is treated as
    # "current" (it may be yesterday's close if today's snapshot hasn't been
    # written yet — e.g. pre-market/weekend). The remainder feeds delta calc.
    current_row = history[0]
    rest = history[1:]
    return current_row, rest


def _derive_pressure_state(
    net_premium: Optional[float],
    deltas: dict,
    availability: dict,
) -> tuple[str, list[str]]:
    """
    Deterministic pressure-state derivation from canonical Net Premium sign
    + available delta direction only. No estimation of missing horizons.
    """
    reasons: list[str] = []

    if net_premium is None:
        reasons.append("NO_CURRENT_NET_PREMIUM")
        return "INSUFFICIENT_HISTORY", reasons

    available_horizons = [h for h in ("1d", "7d", "30d") if availability.get(h)]
    if not available_horizons:
        reasons.append("NO_DELTA_HISTORY_AVAILABLE")
        return "INSUFFICIENT_HISTORY", reasons

    weights = {h: _HORIZON_WEIGHTS[h] for h in available_horizons}
    weight_sum = sum(weights.values())
    weighted_direction = 0.0
    for h in available_horizons:
        d = deltas.get(f"net_premium_delta_{h}")
        if d is None:
            continue
        weighted_direction += (weights[h] / weight_sum) * d

    reasons.append(f"DIRECTION_HORIZONS_USED:{','.join(available_horizons)}")

    bullish = net_premium > 0
    bearish = net_premium < 0
    strengthening = weighted_direction > 0
    weakening = weighted_direction < 0

    if bullish and strengthening:
        return "BULLISH_ACCELERATING", reasons
    if bullish and weakening:
        return "BULLISH_FADING", reasons
    if bullish:
        return "BULLISH_STEADY", reasons
    if bearish and weakening:
        # Net Premium more negative == strengthening bearish pressure per spec examples.
        return "BEARISH_ACCELERATING", reasons
    if bearish and strengthening:
        return "BEARISH_EASING", reasons
    if bearish:
        return "BEARISH_STEADY", reasons
    return "MIXED", reasons


def get_options_alignment_for_ticker(
    ticker: str,
    combined_ticker_data: Optional[dict] = None,
) -> dict:
    """
    Zero-provider-call Options Alignment signal for one ticker.

    combined_ticker_data: optionally pass a pre-fetched
    options_theme_supplement.get_combined_ticker_data() dict to avoid
    re-reading the in-memory cache per ticker in a batch context.

    Returns a dict with:
      options_signal_available
      options_current_composite / options_current_composite_normalized
      options_alignment_score / options_pressure_state
      options_current_score / options_direction_score / options_direction_available
      net_premium / call_premium / put_premium
      net_premium_delta_1d/7d/30d + *_available flags
      options_alignment_reason_codes
      source (live/supplement/supplement_lkg/watchlist_cache/none)
    """
    sym = (ticker or "").upper().strip()
    reasons: list[str] = []

    if combined_ticker_data is None:
        try:
            from data.options_theme_supplement import get_combined_ticker_data
            combined_ticker_data = get_combined_ticker_data()
        except Exception:
            combined_ticker_data = {}

    row = (combined_ticker_data or {}).get(sym)

    if not row:
        reasons.append("NO_OPTIONS_DATA_FOR_TICKER")
        return {
            "ticker": sym,
            "options_signal_available": False,
            "options_current_composite": None,
            "options_current_composite_normalized": None,
            "options_alignment_score": None,
            "options_pressure_state": "INSUFFICIENT_HISTORY",
            "options_current_score": None,
            "options_direction_score": None,
            "options_direction_available": False,
            "net_premium": None,
            "call_premium": None,
            "put_premium": None,
            "net_premium_delta_1d": None,
            "net_premium_delta_7d": None,
            "net_premium_delta_30d": None,
            "net_premium_1d_available": False,
            "net_premium_7d_available": False,
            "net_premium_30d_available": False,
            "source": None,
            "options_alignment_reason_codes": reasons,
        }

    raw_composite = row.get("final_composite_score", row.get("composite_score"))
    current_norm = _normalize_current_composite(raw_composite)
    source = row.get("_source")

    # ── Net Premium (current + delta history) ──────────────────────────────
    current_row, history = _fetch_net_premium_row(sym)

    net_premium = current_row["net_premium"] if current_row else None
    call_premium = current_row["call_premium"] if current_row else None
    put_premium = current_row["put_premium"] if current_row else None

    if net_premium is None:
        reasons.append("NO_NET_PREMIUM_SNAPSHOT_FOR_TODAY")

    try:
        from data.options_net_premium_history import compute_delta_fields
        deltas = compute_delta_fields(net_premium, history, date.today())
    except Exception:
        deltas = {
            "net_premium_delta_1d": None,
            "net_premium_delta_7d": None,
            "net_premium_delta_30d": None,
        }

    availability = {
        "1d": deltas.get("net_premium_delta_1d") is not None,
        "7d": deltas.get("net_premium_delta_7d") is not None,
        "30d": deltas.get("net_premium_delta_30d") is not None,
    }
    direction_available = any(availability.values())
    if not direction_available:
        reasons.append("DELTA_HISTORY_UNAVAILABLE_SINGLE_SNAPSHOT")

    pressure_state, pressure_reasons = _derive_pressure_state(net_premium, deltas, availability)
    reasons.extend(pressure_reasons)

    # ── Direction score (only when >= 1 horizon available) ──────────────────
    direction_score: Optional[float] = None
    if direction_available:
        available_horizons = [h for h in ("1d", "7d", "30d") if availability[h]]
        weights = {h: _HORIZON_WEIGHTS[h] for h in available_horizons}
        weight_sum = sum(weights.values())
        weighted_direction = 0.0
        for h in available_horizons:
            d = deltas.get(f"net_premium_delta_{h}")
            weighted_direction += (weights[h] / weight_sum) * d
        # Map unbounded $ delta to a 0-100 confirmation score via sign +
        # saturating magnitude (bounded, deterministic — no provider calls).
        import math
        magnitude = min(1.0, math.log10(1 + abs(weighted_direction)) / 6.0)
        direction_score = 50.0 + (50.0 if weighted_direction >= 0 else -50.0) * magnitude

    # ── Options Alignment Score (single 0-100 signal) ───────────────────────
    if current_norm is None:
        reasons.append("NO_CURRENT_OPTIONS_COMPOSITE")
        alignment_score = None
    elif direction_score is not None:
        alignment_score = round(
            (_CURRENT_WEIGHT * current_norm) + (_DIRECTION_WEIGHT * direction_score), 2
        )
    else:
        alignment_score = round(current_norm, 2)
        reasons.append("ALIGNMENT_SCORE_IS_CURRENT_COMPOSITE_ONLY")

    return {
        "ticker": sym,
        "options_signal_available": current_norm is not None,
        "options_current_composite": raw_composite,
        "options_current_composite_normalized": current_norm,
        "options_alignment_score": alignment_score,
        "options_pressure_state": pressure_state,
        "options_current_score": current_norm,
        "options_direction_score": round(direction_score, 2) if direction_score is not None else None,
        "options_direction_available": direction_available,
        "net_premium": net_premium,
        "call_premium": call_premium,
        "put_premium": put_premium,
        "net_premium_delta_1d": deltas.get("net_premium_delta_1d"),
        "net_premium_delta_7d": deltas.get("net_premium_delta_7d"),
        "net_premium_delta_30d": deltas.get("net_premium_delta_30d"),
        "net_premium_1d_available": availability["1d"],
        "net_premium_7d_available": availability["7d"],
        "net_premium_30d_available": availability["30d"],
        "source": source,
        "options_alignment_reason_codes": reasons,
    }


def get_options_alignment_bulk(tickers: list[str]) -> dict[str, dict]:
    """
    Batch helper — fetches the combined ticker data cache once and computes
    Options Alignment for every requested ticker off that single snapshot.
    Still zero provider calls (cache reads only).
    """
    try:
        from data.options_theme_supplement import get_combined_ticker_data
        combined = get_combined_ticker_data()
    except Exception:
        combined = {}

    return {
        (t or "").upper().strip(): get_options_alignment_for_ticker(t, combined_ticker_data=combined)
        for t in tickers
    }
