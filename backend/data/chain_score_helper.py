"""
chain_score_helper.py
=====================
Pure scoring helpers for the sectors chain summarizer.

All functions are zero-I/O — they operate only on already-fetched raw
chain contract lists (Tradier format) and an optional underlying price.

Architecture
------------
Scoring logic LIVES in OptionsFlowEngine instance methods (_score_flow,
_score_gamma, _score_asymmetry, _score_volatility, _score_sentiment,
_normalize_contract, _score_contract_liquidity, _estimate_expected_move).

Rather than copying those formulas here, we create a minimal scorer
object via OptionsFlowEngine.__new__() — bypassing the constructor that
requires a data_service.  This gives us all scoring methods for free with
zero duplication.  The singleton is initialized once at first call.

Public API
----------
score_chain_summary(sym, underlying_price, calls_raw, puts_raw) -> dict
estimate_expected_move(underlying_price, calls_raw, puts_raw) -> dict | None
compute_chain_iv(calls_raw, puts_raw) -> dict
"""

from __future__ import annotations

import math
from typing import Optional

_SCORER = None  # lazy singleton — OptionsFlowEngine __new__()-only instance


def _get_scorer():
    """Return a minimal OptionsFlowEngine instance initialised without data_service."""
    global _SCORER
    if _SCORER is None:
        from data.options_flow_engine import (
            OptionsFlowEngine,
            OPTIONS_FLOW_WEIGHTS,
            OPTIONS_FLOW_DEFAULTS,
        )
        obj = OptionsFlowEngine.__new__(OptionsFlowEngine)
        obj.defaults = dict(OPTIONS_FLOW_DEFAULTS)
        obj.weights = dict(OPTIONS_FLOW_WEIGHTS)
        obj._shared_sem = None
        _SCORER = obj
    return _SCORER


def _sf(v) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except Exception:
        return None


def _si(v) -> int:
    try:
        return int(v) if v is not None else 0
    except Exception:
        return 0


# ── IV helpers ────────────────────────────────────────────────────────────────

def compute_chain_iv(calls_raw: list[dict], puts_raw: list[dict]) -> dict:
    """
    Compute IV statistics from raw Tradier chain contracts.

    Returns:
        call_iv       — mean IV across active call contracts (None if no data)
        put_iv        — mean IV across active put contracts (None if no data)
        combined_iv   — mean of call_iv and put_iv (None if both absent)
        iv_skew       — put_iv − call_iv (None if either absent)
        sample_n      — total contracts contributing to the estimate
    """
    call_ivs = [
        _sf(c.get("iv"))
        for c in (calls_raw or [])
        if _si(c.get("volume")) > 0 and _sf(c.get("iv")) is not None and _sf(c.get("iv")) > 0
    ]
    put_ivs = [
        _sf(p.get("iv"))
        for p in (puts_raw or [])
        if _si(p.get("volume")) > 0 and _sf(p.get("iv")) is not None and _sf(p.get("iv")) > 0
    ]

    call_iv = round(sum(call_ivs) / len(call_ivs), 4) if call_ivs else None
    put_iv  = round(sum(put_ivs)  / len(put_ivs),  4) if put_ivs  else None

    if call_iv is not None and put_iv is not None:
        combined_iv = round((call_iv + put_iv) / 2.0, 4)
        iv_skew     = round(put_iv - call_iv, 4)
    elif call_iv is not None:
        combined_iv = call_iv
        iv_skew     = None
    elif put_iv is not None:
        combined_iv = put_iv
        iv_skew     = None
    else:
        combined_iv = None
        iv_skew     = None

    return {
        "call_iv":     call_iv,
        "put_iv":      put_iv,
        "combined_iv": combined_iv,
        "iv_skew":     iv_skew,
        "sample_n":    len(call_ivs) + len(put_ivs),
    }


# ── Expected Move ─────────────────────────────────────────────────────────────

def estimate_expected_move(
    underlying_price: float,
    calls_raw: list[dict],
    puts_raw: list[dict],
) -> Optional[dict]:
    """
    Estimate expected move via ATM straddle (call_mid + put_mid at nearest strike).
    Delegates to OptionsFlowEngine._estimate_expected_move so the formula stays DRY.
    Returns None if underlying_price is missing or no ATM match found.
    """
    if not underlying_price or not calls_raw or not puts_raw:
        return None
    try:
        scorer = _get_scorer()
        return scorer._estimate_expected_move(underlying_price, calls_raw, puts_raw)
    except Exception:
        return None


# ── OI ────────────────────────────────────────────────────────────────────────

def compute_chain_oi(calls_raw: list[dict], puts_raw: list[dict]) -> dict:
    """
    Sum open interest across all call and put contracts.

    Does NOT filter by volume — OI is a structural market attribute independent
    of whether trades happened today.
    """
    call_oi = sum(_si(c.get("openInterest")) for c in (calls_raw or []))
    put_oi  = sum(_si(p.get("openInterest")) for p in (puts_raw or []))
    return {
        "call_oi":  call_oi,
        "put_oi":   put_oi,
        "total_oi": call_oi + put_oi,
    }


# ── Score ─────────────────────────────────────────────────────────────────────

def score_chain_summary(
    sym: str,
    underlying_price: Optional[float],
    calls_raw: list[dict],
    puts_raw: list[dict],
    *,
    expiration: Optional[str] = None,
    stock_context: Optional[dict] = None,
) -> dict:
    """
    Score a raw Tradier chain using the canonical OptionsFlowEngine pipeline.

    Unlike the full TradierFlowEngine scan, this path:
     - Uses ALL contracts (not just those filtered by _contract_filter).
     - Has no stock screening context (breakout/compression/catalyst).
     - Stock-context component therefore contributes 0 pts.
     - Gamma density is approximated from OI distribution alone.

    Returns:
        options_score     — composite score 0-100 (None if chain empty)
        options_signal    — signal string (None if chain empty)
        score_components  — dict of per-component scores
        score_method      — "chain_summary_v1"
        contracts_scored  — count of normalized contracts used
        no_unusual_flow   — True when chain non-empty but score is 0

    Sentinel outputs:
        chain_empty=True  — calls+puts yielded 0 normalised contracts
        score=None        — spot_price was None (cannot compute break-even / asymmetry)
    """
    if not underlying_price or underlying_price <= 0:
        return {
            "options_score":    None,
            "options_signal":   None,
            "score_components": {},
            "score_method":     "chain_summary_v1",
            "contracts_scored": 0,
            "chain_empty":      False,
            "no_unusual_flow":  False,
            "score_error":      "no_underlying_price",
        }

    try:
        scorer = _get_scorer()
    except Exception as exc:
        return {
            "options_score":    None,
            "options_signal":   None,
            "score_components": {},
            "score_method":     "chain_summary_v1",
            "contracts_scored": 0,
            "chain_empty":      False,
            "no_unusual_flow":  False,
            "score_error":      f"scorer_init_failed:{exc}",
        }

    ctx = dict(stock_context or {})

    # ── Normalise all contracts ───────────────────────────────────────────────
    exp = expiration or "unknown"
    norm_calls: list[dict] = []
    norm_puts:  list[dict] = []

    for raw in (calls_raw or []):
        n = scorer._normalize_contract(sym, "call", exp, raw, underlying_price)
        if n is not None:
            norm_calls.append(n)

    for raw in (puts_raw or []):
        n = scorer._normalize_contract(sym, "put", exp, raw, underlying_price)
        if n is not None:
            norm_puts.append(n)

    all_contracts = norm_calls + norm_puts

    if not all_contracts:
        return {
            "options_score":    None,
            "options_signal":   None,
            "score_components": {},
            "score_method":     "chain_summary_v1",
            "contracts_scored": 0,
            "chain_empty":      True,
            "no_unusual_flow":  False,
            "score_error":      None,
        }

    # ── Aggregate metrics ─────────────────────────────────────────────────────
    call_vol = sum(c["volume"] for c in norm_calls)
    put_vol  = sum(c["volume"] for c in norm_puts)
    call_oi  = sum(c["open_interest"] for c in norm_calls)
    put_oi   = sum(c["open_interest"] for c in norm_puts)

    cp_vol_ratio = round(call_vol / put_vol,  3) if put_vol  else None
    cp_oi_ratio  = round(call_oi  / put_oi,   3) if put_oi   else None

    # IV — volume-weighted mean over active contracts
    iv_vals = [
        c["implied_volatility"]
        for c in all_contracts
        if c["volume"] > 0 and c.get("implied_volatility") is not None
    ]
    iv_current = round(sum(iv_vals) / len(iv_vals), 4) if iv_vals else None

    # Near-spot OI density (fraction of OI within 5% of spot)
    near_oi  = sum(c["open_interest"] for c in all_contracts if (c.get("moneyness_pct") or 1) <= 0.05)
    total_oi_n = call_oi + put_oi
    near_spot_oi_density     = round(near_oi / total_oi_n, 4) if total_oi_n else None
    near_spot_gamma_density  = None  # requires exact greek values; approximate only

    # ── Per-contract scoring ──────────────────────────────────────────────────
    for c in all_contracts:
        c["flow_score"]      = round(scorer._score_flow(c, cp_vol_ratio),      1)
        c["asymmetry_score"] = round(scorer._score_asymmetry(c, underlying_price), 1)
        c["contract_score"]  = round(c["flow_score"] * 0.6 + c["asymmetry_score"] * 0.4, 1)

    # Sort descending by contract_score; take top-N as "top contracts"
    _top_n = scorer.defaults.get("top_contracts_per_ticker", 5)
    all_contracts.sort(key=lambda x: x.get("contract_score", 0), reverse=True)
    top_contracts = all_contracts[:_top_n]

    if not top_contracts:
        return {
            "options_score":    0.0,
            "options_signal":   "NO UNUSUAL FLOW",
            "score_components": {},
            "score_method":     "chain_summary_v1",
            "contracts_scored": len(all_contracts),
            "chain_empty":      False,
            "no_unusual_flow":  True,
            "score_error":      None,
        }

    best = top_contracts[0]

    # ── Composite score components ────────────────────────────────────────────
    flow_score       = round(sum(c["flow_score"]      for c in top_contracts) / len(top_contracts), 1)
    asymmetry_score  = round(sum(c["asymmetry_score"] for c in top_contracts) / len(top_contracts), 1)
    gamma_score      = round(scorer._score_gamma(ctx, near_spot_oi_density, near_spot_gamma_density, top_contracts), 1)
    volatility_score = round(scorer._score_volatility(ctx, iv_current, best), 1)
    sentiment_score  = round(scorer._score_sentiment(ctx, cp_vol_ratio, cp_oi_ratio), 1)
    stock_ctx_score  = 0.0  # no stock screening context available in chain-only path

    w = scorer.weights
    composite = round(
        flow_score      * w["flow_score"]      +
        gamma_score     * w["gamma_score"]      +
        asymmetry_score * w["asymmetry_score"]  +
        volatility_score * w["volatility_score"] +
        sentiment_score * w["sentiment_score"]   +
        stock_ctx_score * w["stock_context_score"],
        1,
    )

    # ── Signal classification ─────────────────────────────────────────────────
    # Use the canonical classifier when stock context is empty.
    # The signal falls back to the generic "asymmetric_rr" path for most
    # supplement-only rows since breakout/catalyst/squeeze context is absent.
    signal = scorer._classify_signal(ctx, best, gamma_score, volatility_score, sentiment_score)

    return {
        "options_score":    composite,
        "options_signal":   signal,
        "score_components": {
            "flow_score":          flow_score,
            "gamma_score":         gamma_score,
            "asymmetry_score":     asymmetry_score,
            "volatility_score":    volatility_score,
            "sentiment_score":     sentiment_score,
            "stock_context_score": stock_ctx_score,
        },
        "score_method":     "chain_summary_v1",
        "contracts_scored": len(all_contracts),
        "chain_empty":      False,
        "no_unusual_flow":  False,
        "score_error":      None,
    }
