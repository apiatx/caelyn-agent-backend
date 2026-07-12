"""
Options Alignment V2 — zero-provider-call read layer for Trade Alignment.

Answers: "How strongly does CURRENT normalized options premium PRESSURE
confirm a long-biased swing-trade opportunity, and is that pressure
strengthening or weakening?"

This module performs NO network/provider calls of any kind. It only reads:
  - the existing canonical per-ticker premium fields produced by
    backend/data/options_flow_sectors.py / backend/data/sectors_chain_summarizer.py
    (via the zero-call merge helper
    backend/data/options_theme_supplement.py::get_combined_ticker_data):
    net_premium, call_premium, put_premium, effective_premium_pcr,
    raw_premium_pcr, bias, ticker_state, scan_status
  - the existing canonical Net Premium history helpers in
    backend/data/options_net_premium_history.py (Neon
    public.options_net_premium_daily): net_premium_delta_1d/7d/30d

It does NOT modify the Options Flow producer, does NOT duplicate the
delta-calculation formulas, and does NOT implement the final four-component
Trade Alignment score. It returns exactly ONE 0-100 "options_alignment_score"
signal plus full transparency fields.

V2 CHANGE (Options Alignment V2 spec): the "current pressure" 70% input is no
longer sourced from the master-screener composite_score/final_composite_score/
heat_score (gamma/volatility/sentiment scoring only available for a narrow
Screener subset) nor from a raw-dollar log-magnitude fallback. It is now a
normalized premium_pressure_score computed uniformly for every canonical
All Stocks row from:
  - net_premium_pct   (net_premium / (call_premium+put_premium) * 100) —
    the same "% Net Premium" imbalance concept shown in the Options Flow
    product. No dedicated canonical field named "pct_net_premium" exists in
    the backend today, so this ratio is derived directly from the exact same
    canonical call_premium/put_premium/net_premium fields already displayed
    on Options Flow / Sectors / Watchlist ticker rows — not re-derived from
    a different source, not a new provider call.
  - net_premium / market_cap — sourced from the existing Watchlist
    Fundamental Screener's zero-provider-call Neon cache
    (backend/data/watchlist_fundamentals_store.py::get_snapshot, table
    public.watchlist_fundamentals_cache, field "Market Cap" — populated by
    the weekly FMP background refresher in
    backend/services/watchlist_fundamentals_refresh.py, never fetched here).
    No canonical "Net Premium / Market Cap" display field previously
    existed anywhere in this backend, so there is no pre-existing formula
    to reuse; this module defines the ratio itself
    (net_premium / market_cap * 100, saturating at +/-0.50%) purely from
    already-cached inputs. When no fundamentals snapshot exists for a
    ticker (not yet refreshed, or off the Watchlist), this component is
    omitted from the weighted average via available-component
    renormalization (see _PREMIUM_PRESSURE_WEIGHTS below) — never defaulted
    to a fake neutral value.
  - premium_balance   (call_premium share of total premium, 0-100)
  - effective_premium_pcr (falls back to raw_premium_pcr, spec-documented)
  - bias / ticker_state (secondary, deterministic confirmation only)
  - raw net_premium magnitude — bounded ±5pt confidence modifier only,
    applied post-weighting; never a standalone directional weight.
"""

from __future__ import annotations

from datetime import date
from typing import Optional

# Locked outer Trade-Alignment-facing weighting (Options Alignment V2 spec Part 6).
_CURRENT_WEIGHT = 0.70
_DIRECTION_WEIGHT = 0.30

# Direction-horizon mix when all three horizons exist (spec Part 5).
_HORIZON_WEIGHTS = {"1d": 0.20, "7d": 0.35, "30d": 0.45}

# ── Premium Pressure Score internal weights (spec Part 4) ──────────────────
# Highest importance: % Net Premium and Net Premium / Market Cap.
# Major confirmation: Call/Put balance and Effective Premium PCR.
# Secondary confirmation: canonical bias / ticker_state.
# Raw $ Net Premium magnitude is NOT a weighted component here — it is a
# bounded post-hoc confidence modifier only (see _magnitude_confidence_bonus).
_PREMIUM_PRESSURE_WEIGHTS = {
    "pct":      0.35,   # net_premium_pct_component
    "mc":       0.25,   # net_premium_market_cap_component (usually unavailable today)
    "balance":  0.15,   # premium_balance_component
    "pcr":      0.15,   # premium_pcr_component
    "bias":     0.10,   # premium_bias_component
}

_MAGNITUDE_CONFIDENCE_MAX = 5.0  # bounded +/- points, never dominant


def _pct_net_premium(net_premium: Optional[float], call_p: Optional[float], put_p: Optional[float]) -> Optional[float]:
    """% Net Premium: net_premium as a share of total premium flow, -100..100."""
    if call_p is None or put_p is None:
        return None
    total = (call_p or 0.0) + (put_p or 0.0)
    if total <= 0:
        return None
    np_val = net_premium if net_premium is not None else (call_p - put_p)
    return max(-100.0, min(100.0, round((np_val / total) * 100.0, 2)))


def _score_from_pct(pct: Optional[float]) -> Optional[float]:
    """Map -100..100 % Net Premium to 0..100 (50 = neutral/balanced)."""
    if pct is None:
        return None
    return max(0.0, min(100.0, 50.0 + (pct / 2.0)))


def _score_from_market_cap_ratio(net_premium: Optional[float], market_cap: Optional[float]) -> Optional[float]:
    """
    Net Premium / Market Cap, mapped to 0..100. Unavailable whenever no
    canonical market_cap is supplied (see module docstring) — never
    defaulted to a neutral 50, per spec Part 3.
    """
    if net_premium is None or not market_cap or market_cap <= 0:
        return None
    ratio_pct = (net_premium / market_cap) * 100.0
    # Saturating map: +/-0.50% of market cap in net premium ~= full-scale signal.
    scaled = max(-1.0, min(1.0, ratio_pct / 0.50))
    return max(0.0, min(100.0, 50.0 + 50.0 * scaled))


def _score_from_balance(call_p: Optional[float], put_p: Optional[float]) -> Optional[float]:
    """Call-premium share of total premium (0..100). 50 = balanced."""
    if call_p is None or put_p is None:
        return None
    total = (call_p or 0.0) + (put_p or 0.0)
    if total <= 0:
        return None
    return round((call_p / total) * 100.0, 2)


def _score_from_pcr(effective_pcr: Optional[float], raw_pcr: Optional[float]) -> Optional[float]:
    """
    Lower put/call premium ratio == more call-dominant == more bullish.
    Uses canonical effective_premium_pcr; falls back to raw_premium_pcr only
    when the log-safe clamp is unavailable (documented fallback, spec Part 2E).
    """
    pcr = effective_pcr if effective_pcr is not None else raw_pcr
    if pcr is None or pcr < 0:
        return None
    return max(0.0, min(100.0, round((1.0 / (1.0 + pcr)) * 100.0, 2)))


def _score_from_bias(bias: Optional[str]) -> Optional[float]:
    """Deterministic confirmation only — never overrides contradictory raw premium data."""
    if not bias:
        return None
    b = bias.lower()
    if b == "bullish":
        return 75.0
    if b == "bearish":
        return 25.0
    if b == "neutral":
        return 50.0
    return None


def _magnitude_confidence_bonus(net_premium: Optional[float], direction_toward: float) -> float:
    """
    Bounded +/-5pt confidence modifier from raw $ net_premium magnitude only.
    Pushes further in whichever direction the weighted average already points
    (direction_toward > 50 => bullish push, < 50 => bearish push); never
    flips or dominates the underlying normalized-pressure verdict.
    """
    if net_premium is None:
        return 0.0
    import math as _math
    magnitude = min(1.0, _math.log10(1 + abs(net_premium)) / 7.0)  # saturates ~$10M+
    if direction_toward > 50.0:
        return _MAGNITUDE_CONFIDENCE_MAX * magnitude
    if direction_toward < 50.0:
        return -_MAGNITUDE_CONFIDENCE_MAX * magnitude
    return 0.0


def _compute_premium_pressure_score(
    net_premium: Optional[float],
    call_premium: Optional[float],
    put_premium: Optional[float],
    effective_premium_pcr: Optional[float],
    raw_premium_pcr: Optional[float],
    bias: Optional[str],
    market_cap: Optional[float] = None,
) -> dict:
    """
    Options Alignment V2 "current premium pressure" score (spec Parts 2-4).
    Normalized-pressure-first; raw $ net_premium is a bounded confidence
    modifier only, never a standalone directional weight.
    """
    has_basis = net_premium is not None or ((call_premium or 0) + (put_premium or 0) > 0)
    if not has_basis:
        return {
            "premium_pressure_available": False,
            "premium_pressure_score": None,
            "net_premium_pct": None,
            "net_premium_pct_component": None,
            "net_premium_market_cap_component": None,
            "premium_balance_component": None,
            "premium_pcr_component": None,
            "premium_bias_component": None,
            "premium_magnitude_confidence": 0.0,
            "premium_pressure_reason_codes": ["NO_REAL_PREMIUM_BASIS"],
        }

    pct = _pct_net_premium(net_premium, call_premium, put_premium)
    components = {
        "pct":     _score_from_pct(pct),
        "mc":      _score_from_market_cap_ratio(net_premium, market_cap),
        "balance": _score_from_balance(call_premium, put_premium),
        "pcr":     _score_from_pcr(effective_premium_pcr, raw_premium_pcr),
        "bias":    _score_from_bias(bias),
    }

    reasons: list[str] = []
    available = {k: v for k, v in components.items() if v is not None}
    if "mc" not in available:
        reasons.append("NET_PREMIUM_MARKET_CAP_UNAVAILABLE_NO_CANONICAL_MC_CACHE")
    if not available:
        reasons.append("NO_USABLE_PREMIUM_PRESSURE_COMPONENT")
        return {
            "premium_pressure_available": False,
            "premium_pressure_score": None,
            "net_premium_pct": pct,
            "net_premium_pct_component": components["pct"],
            "net_premium_market_cap_component": components["mc"],
            "premium_balance_component": components["balance"],
            "premium_pcr_component": components["pcr"],
            "premium_bias_component": components["bias"],
            "premium_magnitude_confidence": 0.0,
            "premium_pressure_reason_codes": reasons,
        }

    weight_sum = sum(_PREMIUM_PRESSURE_WEIGHTS[k] for k in available)
    weighted = sum(_PREMIUM_PRESSURE_WEIGHTS[k] * v for k, v in available.items()) / weight_sum
    reasons.append(f"PRESSURE_COMPONENTS_USED:{','.join(sorted(available))}")

    bonus = _magnitude_confidence_bonus(net_premium, weighted)
    final_score = max(0.0, min(100.0, round(weighted + bonus, 2)))

    return {
        "premium_pressure_available": True,
        "premium_pressure_score": final_score,
        "net_premium_pct": pct,
        "net_premium_pct_component": components["pct"],
        "net_premium_market_cap_component": components["mc"],
        "premium_balance_component": components["balance"],
        "premium_pcr_component": components["pcr"],
        "premium_bias_component": components["bias"],
        "premium_magnitude_confidence": round(bonus, 2),
        "premium_pressure_reason_codes": reasons,
    }


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
    fundamentals_map: Optional[dict] = None,
    preloaded_net_premium: "Optional[dict]" = None,
) -> dict:
    """
    Zero-provider-call Options Alignment signal for one ticker.

    combined_ticker_data: optionally pass a pre-fetched
    options_theme_supplement.get_combined_ticker_data() dict to avoid
    re-reading the in-memory cache per ticker in a batch context.

    fundamentals_map: optionally pass a pre-fetched
    {SYMBOL: watchlist_fundamentals_store snapshot} dict (from
    get_snapshots_bulk) to avoid one Neon round-trip per ticker in a batch
    context. When None, this ticker's snapshot is fetched individually
    (single-symbol call path only) via the same zero-provider-call reader.

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

    _UNAVAILABLE_BASE = {
        "options_signal_available": False,
        "options_alignment_available": False,
        "premium_pressure_available": False,
        "premium_pressure_score": None,
        "net_premium_pct": None,
        "net_premium_pct_component": None,
        "net_premium_market_cap_component": None,
        "premium_balance_component": None,
        "premium_pcr_component": None,
        "premium_bias_component": None,
        "premium_magnitude_confidence": None,
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
        "effective_premium_pcr": None,
        "raw_premium_pcr": None,
        "bias": None,
        "ticker_state": None,
        "net_premium_delta_1d": None,
        "net_premium_delta_7d": None,
        "net_premium_delta_30d": None,
        "net_premium_1d_available": False,
        "net_premium_7d_available": False,
        "net_premium_30d_available": False,
    }

    if not row:
        reasons.append("NO_OPTIONS_DATA_FOR_TICKER")
        return {"ticker": sym, **_UNAVAILABLE_BASE, "source": None, "options_alignment_reason_codes": reasons}

    # ── UNSUPPORTED / UNAVAILABLE STATES (spec Part 8) ──────────────────────
    # ticker_state / scan_status are the canonical Sectors -> All Stocks
    # classification (data/options_flow_sectors.py::_ticker_state). Any of
    # these must return unavailable with an explicit reason — never a
    # fabricated score, never 0, never a neutral 50.
    _ticker_state = row.get("ticker_state") or row.get("scan_result") or ""
    _scan_status = row.get("scan_status") or ""
    if _ticker_state == "confirmed_no_options" or _scan_status == "no_options":
        reasons.append("CONFIRMED_NO_OPTIONS")
        return {"ticker": sym, **_UNAVAILABLE_BASE, "source": row.get("_source"),
                "ticker_state": _ticker_state, "options_alignment_reason_codes": reasons}
    if ":" in sym:
        reasons.append("FOREIGN_EXCHANGE_TICKER_UNSUPPORTED")
        return {"ticker": sym, **_UNAVAILABLE_BASE, "source": row.get("_source"),
                "ticker_state": _ticker_state, "options_alignment_reason_codes": reasons}
    if _ticker_state in ("generic_pending", "optionable_pending_chain", "deferred_retry") or _scan_status in ("pending", "missing_data"):
        _row_net_premium = row.get("net_premium")
        _row_call, _row_put = row.get("call_premium"), row.get("put_premium")
        _has_real_scan = _row_net_premium is not None or ((_row_call or 0) + (_row_put or 0) > 0)
        if not _has_real_scan:
            reasons.append("PENDING_BACKFILL")
            return {"ticker": sym, **_UNAVAILABLE_BASE, "source": row.get("_source"),
                    "ticker_state": _ticker_state, "options_alignment_reason_codes": reasons}

    source = row.get("_source")
    _snap_status  = row.get("_snapshot_status") or (
        "available_live" if source == "live" else "available_cached"
    )
    _options_as_of      = row.get("_as_of") or row.get("_cached_at")
    _lkg_age_s          = row.get("_lkg_age_s")
    _options_lkg_age_h  = round(_lkg_age_s / 3600, 1) if _lkg_age_s else None

    # ── Premium Pressure Score (spec Parts 2-4) — normalized, uniform for
    # every canonical All Stocks row. No composite_score/heat_score reliance.
    _market_cap = None
    try:
        if fundamentals_map is not None:
            _snap = fundamentals_map.get(sym)
        else:
            from data.watchlist_fundamentals_store import get_snapshot as _get_fund_snapshot
            _snap = _get_fund_snapshot(sym)
        if _snap:
            _mc_raw = (_snap.get("fields") or {}).get("Market Cap")
            if _mc_raw is not None:
                _market_cap = float(_mc_raw)
    except Exception:
        _market_cap = None

    _pressure = _compute_premium_pressure_score(
        net_premium=row.get("net_premium"),
        call_premium=row.get("call_premium"),
        put_premium=row.get("put_premium"),
        effective_premium_pcr=row.get("effective_premium_pcr"),
        raw_premium_pcr=row.get("raw_premium_pcr"),
        bias=row.get("bias"),
        market_cap=_market_cap,
    )
    reasons.extend(_pressure.pop("premium_pressure_reason_codes", []))
    current_norm = _pressure["premium_pressure_score"]
    if current_norm is None:
        reasons.append("UNEXPECTED_MISSING_OPTIONS_DATA" if row.get("net_premium") is None and not (row.get("call_premium") or row.get("put_premium")) else "NO_CURRENT_OPTIONS_COMPOSITE")

    # ── Net Premium (current + delta history) ──────────────────────────────
    # Use preloaded bulk data when available (avoids one Neon round-trip per
    # ticker in the bulk path — 379 individual calls replaced by 1).
    if preloaded_net_premium is not None and sym in preloaded_net_premium:
        current_row, history = preloaded_net_premium[sym]
    else:
        current_row, history = _fetch_net_premium_row(sym)

    net_premium = current_row["net_premium"] if current_row else row.get("net_premium")
    call_premium = current_row["call_premium"] if current_row else row.get("call_premium")
    put_premium = current_row["put_premium"] if current_row else row.get("put_premium")

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

    # ── Options Alignment V2 Score (single 0-100 signal, spec Part 6) ──────
    if current_norm is None:
        alignment_score = None
    elif direction_score is not None:
        alignment_score = round(
            (_CURRENT_WEIGHT * current_norm) + (_DIRECTION_WEIGHT * direction_score), 2
        )
    else:
        alignment_score = round(current_norm, 2)
        reasons.append("ALIGNMENT_SCORE_IS_PREMIUM_PRESSURE_ONLY")

    return {
        "ticker": sym,
        "options_signal_available": current_norm is not None,
        "options_alignment_available": current_norm is not None,
        "premium_pressure_available": _pressure["premium_pressure_available"],
        "premium_pressure_score": _pressure["premium_pressure_score"],
        "net_premium_pct": _pressure["net_premium_pct"],
        "net_premium_pct_component": _pressure["net_premium_pct_component"],
        "net_premium_market_cap_component": _pressure["net_premium_market_cap_component"],
        "premium_balance_component": _pressure["premium_balance_component"],
        "premium_pcr_component": _pressure["premium_pcr_component"],
        "premium_bias_component": _pressure["premium_bias_component"],
        "premium_magnitude_confidence": _pressure["premium_magnitude_confidence"],
        "options_current_composite": current_norm,
        "options_current_composite_normalized": current_norm,
        "options_current_score_derived": True,
        "options_alignment_score": alignment_score,
        "options_pressure_state": pressure_state,
        "options_current_score": current_norm,
        "options_direction_score": round(direction_score, 2) if direction_score is not None else None,
        "options_direction_available": direction_available,
        "net_premium": net_premium,
        "call_premium": call_premium,
        "put_premium": put_premium,
        "effective_premium_pcr": row.get("effective_premium_pcr"),
        "raw_premium_pcr": row.get("raw_premium_pcr"),
        "bias": row.get("bias"),
        "ticker_state": _ticker_state,
        "net_premium_delta_1d": deltas.get("net_premium_delta_1d"),
        "net_premium_delta_7d": deltas.get("net_premium_delta_7d"),
        "net_premium_delta_30d": deltas.get("net_premium_delta_30d"),
        "net_premium_1d_available": availability["1d"],
        "net_premium_7d_available": availability["7d"],
        "net_premium_30d_available": availability["30d"],
        "source": source,
        "options_primary_signal":    row.get("primary_signal"),
        "options_snapshot_status":   _snap_status,
        "options_as_of":             _options_as_of,
        "options_lkg_age_hours":     _options_lkg_age_h,
        "options_alignment_reason_codes": reasons,
    }


def get_options_alignment_bulk(
    tickers: list[str],
    preloaded_fundamentals: "Optional[dict]" = None,
) -> dict[str, dict]:
    """
    Batch helper — fetches the combined ticker data cache once and computes
    Options Alignment for every requested ticker off that single snapshot.
    Still zero provider calls (cache/Neon reads only).

    preloaded_fundamentals: if provided by the caller, skip the internal
    get_snapshots_bulk() Neon read and use this dict instead.  This avoids
    duplicate Neon round-trips when build_confluence_snapshot() already
    fetched fundamentals for the Investment Alignment step.

    Net premium history: fetched in ONE bulk Neon query for all tickers
    (instead of 379 sequential per-ticker queries — the root cause of
    the retained snapshot rebuild hang).  A 25-second timeout prevents
    indefinite blocking; tickers without history receive (None, []).
    """
    import concurrent.futures as _cft_b
    import time as _time_b

    try:
        from data.options_theme_supplement import get_combined_ticker_data
        combined = get_combined_ticker_data()
    except Exception:
        combined = {}

    upper_tickers = [(t or "").upper().strip() for t in tickers]

    if preloaded_fundamentals is not None:
        fundamentals_map = preloaded_fundamentals
    else:
        try:
            from data.watchlist_fundamentals_store import get_snapshots_bulk as _get_fund_bulk
            fundamentals_map = _get_fund_bulk(upper_tickers)
        except Exception:
            fundamentals_map = {}

    # ── SINGLE bulk net-premium Neon read (replaces N×per-ticker queries) ──
    # Root cause of the retained-snapshot rebuild hang: _fetch_net_premium_row
    # called get_historical_snapshots_bulk with a 1-ticker list PER TICKER,
    # producing 379 sequential Neon round-trips.  One IN-clause query instead.
    #
    # Timeout design: we use shutdown(wait=False) so a stuck Neon socket
    # does NOT block the calling thread beyond timeout_s.  The hung
    # background thread drains on its own once the socket unblocks.
    net_premium_preloaded: dict[str, tuple] = {}
    _t_npm = _time_b.time()
    try:
        from datetime import date as _d_b, timedelta as _td_b
        from data.options_net_premium_history import get_historical_snapshots_bulk as _gnpm
        _since = _d_b.today() - _td_b(days=35)
        _entities = [("stock", t) for t in upper_tickers if t]
        _npm_pool = _cft_b.ThreadPoolExecutor(max_workers=1, thread_name_prefix="npm-bulk")
        _npm_fut = _npm_pool.submit(_gnpm, _entities, _since)
        try:
            _npm_raw = _npm_fut.result(timeout=25) or {}
        except _cft_b.TimeoutError:
            _npm_raw = {}
            print(
                f"[OPTIONS_ALIGNMENT_BULK] net_premium_bulk TIMEOUT "
                f"elapsed_ms={int((_time_b.time()-_t_npm)*1000)} — skipping delta history"
            )
        except Exception as _ne:
            _npm_raw = {}
            print(f"[OPTIONS_ALIGNMENT_BULK] net_premium_bulk ERROR: {_ne}")
        finally:
            _npm_pool.shutdown(wait=False)   # never block on hung Neon socket

        for t in upper_tickers:
            _hist = _npm_raw.get(("stock", t), [])
            net_premium_preloaded[t] = (_hist[0] if _hist else None, _hist[1:] if _hist else [])
        print(
            f"[OPTIONS_ALIGNMENT_BULK] net_premium_bulk ok "
            f"tickers_with_data={sum(1 for v in net_premium_preloaded.values() if v[0])} "
            f"elapsed_ms={int((_time_b.time()-_t_npm)*1000)}"
        )
    except Exception as _setup_e:
        print(f"[OPTIONS_ALIGNMENT_BULK] net_premium_bulk setup ERROR: {_setup_e}")
    # ───────────────────────────────────────────────────────────────────────

    return {
        t: get_options_alignment_for_ticker(
            t,
            combined_ticker_data=combined,
            fundamentals_map=fundamentals_map,
            preloaded_net_premium=net_premium_preloaded if net_premium_preloaded else None,
        )
        for t in upper_tickers
    }
