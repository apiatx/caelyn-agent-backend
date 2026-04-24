"""
Options row enricher — adds premium analytics, OTM metrics, flow-direction
bias, and heat_score to master screener rows.

All added fields return None (never fake numbers) when data is unavailable.

Call enrich_ticker_rows(rows) after the engine scan and before caching.
It mutates each row dict in-place and returns the same list.
"""
from __future__ import annotations

import math
from typing import Optional

from data.options_screener_snapshot import get_deltas, update_state


# ── helpers ──────────────────────────────────────────────────────────────────

def _safe_float(v) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _safe_int(v) -> Optional[int]:
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _fmt_premium(p: float) -> str:
    if p >= 1_000_000:
        return f"${p/1_000_000:.1f}M"
    if p >= 1_000:
        return f"${p/1_000:.0f}K"
    return f"${p:.0f}"


def _midpoint(bid, ask, last) -> Optional[float]:
    b = _safe_float(bid)
    a = _safe_float(ask)
    if b is not None and a is not None and a > 0:
        return (b + a) / 2.0
    return _safe_float(last)


def _contract_premium(c: dict) -> Optional[float]:
    """premium = mid * volume * 100 for one contract row."""
    mid = _midpoint(c.get("bid"), c.get("ask"), c.get("last"))
    if mid is None or mid <= 0:
        mid = _safe_float(c.get("mid") or c.get("midpoint") or c.get("premium_traded_estimate"))
        if mid and mid > 100:
            return mid
        if mid is None:
            return None
    vol = _safe_int(c.get("volume"))
    if vol is None or vol <= 0:
        return None
    return mid * vol * 100


# ── per-row field derivation ──────────────────────────────────────────────────

def _pick_best_contract(top_contracts: list[dict]) -> Optional[dict]:
    """Return the highest-premium contract from the row's top_contracts."""
    best = None
    best_prem = -1.0
    for c in top_contracts:
        p = _contract_premium(c) or 0
        if p > best_prem:
            best_prem = p
            best = c
    return best


def _build_enriched_fields(row: dict) -> dict:
    symbol = row.get("ticker", "")
    underlying = _safe_float(row.get("underlying_price"))
    top_contracts: list[dict] = row.get("top_contracts") or []

    # ── Aggregate premium across ALL top_contracts, split by side ─────────────
    total_call_prem = 0.0
    total_put_prem = 0.0
    total_call_vol = 0
    total_put_vol = 0

    for c in top_contracts:
        side = (c.get("type") or c.get("side") or "").lower()
        prem = _contract_premium(c) or 0
        vol = _safe_int(c.get("volume")) or 0
        if side == "call":
            total_call_prem += prem
            total_call_vol += vol
        elif side == "put":
            total_put_prem += prem
            total_put_vol += vol

    total_premium = total_call_prem + total_put_prem
    total_vol = total_call_vol + total_put_vol

    premium: Optional[float] = total_premium if total_premium > 0 else None
    premium_display: Optional[str] = _fmt_premium(total_premium) if premium else None

    call_flow_pct: Optional[float] = None
    put_flow_pct: Optional[float] = None
    call_put_premium_ratio: Optional[float] = None
    call_put_volume_ratio: Optional[float] = None

    if total_premium > 0:
        call_flow_pct = round(total_call_prem / total_premium * 100, 1)
        put_flow_pct = round(total_put_prem / total_premium * 100, 1)
    if total_put_prem > 0:
        call_put_premium_ratio = round(total_call_prem / total_put_prem, 3)

    # Also use existing row-level volumes as fallback/supplement
    row_call_vol = _safe_int(row.get("call_volume")) or total_call_vol
    row_put_vol = _safe_int(row.get("put_volume")) or total_put_vol
    if row_put_vol > 0:
        call_put_volume_ratio = round(row_call_vol / row_put_vol, 3)
    elif total_put_vol > 0:
        call_put_volume_ratio = round(total_call_vol / total_put_vol, 3)

    # ── Side bias ─────────────────────────────────────────────────────────────
    side_bias: str = "unknown"
    if call_flow_pct is not None:
        if call_flow_pct >= 65:
            side_bias = "bullish"
        elif put_flow_pct is not None and put_flow_pct >= 65:
            side_bias = "bearish"
        elif call_flow_pct >= 40 and put_flow_pct is not None and put_flow_pct >= 40:
            side_bias = "mixed"

    # ── Best contract fields ──────────────────────────────────────────────────
    best = _pick_best_contract(top_contracts)

    strike: Optional[float] = None
    expiry: Optional[str] = None
    option_type: Optional[str] = None
    days_to_expiry: Optional[int] = None
    otm_pct: Optional[float] = None
    is_otm: bool = False
    is_unusual_otm: bool = False
    unusual_volume_ratio: Optional[float] = None
    sweep_like: Optional[bool] = None
    liquidity_score: Optional[float] = None

    if best:
        strike = _safe_float(best.get("strike"))
        expiry = best.get("expiration") or best.get("expiry")
        option_type = (best.get("type") or best.get("side") or "").lower() or None
        days_to_expiry = _safe_int(best.get("dte"))

        if underlying and strike and option_type:
            if option_type == "call":
                otm_pct = round((strike - underlying) / underlying * 100, 2)
                is_otm = strike > underlying
            elif option_type == "put":
                otm_pct = round((underlying - strike) / underlying * 100, 2)
                is_otm = strike < underlying

        best_vol = _safe_int(best.get("volume")) or 0
        best_oi = _safe_int(best.get("open_interest") or best.get("openInterest")) or 0
        if best_oi > 0:
            unusual_volume_ratio = round(best_vol / best_oi, 3)
        elif best_vol > 0:
            unusual_volume_ratio = 99.0  # OI=0 means entirely new position

        best_prem = _contract_premium(best) or 0
        is_unusual_otm = bool(
            is_otm
            and otm_pct is not None and abs(otm_pct) >= 5
            and best_prem >= 25_000
            and unusual_volume_ratio is not None and unusual_volume_ratio >= 1.5
        )

        rfs = _safe_float(best.get("repeated_flow_score"))
        if rfs is not None:
            sweep_like = rfs >= 50

        lq = best.get("contract_liquidity_quality")
        if lq is not None:
            liquidity_score = _safe_float(lq)

    # ── Snapshot deltas ───────────────────────────────────────────────────────
    oi_change_pct: Optional[float] = None
    premium_change_pct: Optional[float] = None

    if best and expiry and option_type:
        best_oi_val = _safe_int(best.get("open_interest") or best.get("openInterest"))
        best_prem_val = _safe_float(best.get("premium_traded_estimate")) or _contract_premium(best)
        oi_change_pct, premium_change_pct = get_deltas(
            symbol=symbol,
            expiry=expiry,
            strike=strike,
            otype=option_type,
            current_oi=best_oi_val,
            current_premium=best_prem_val,
        )

    # ── Heat score (additive composite, 0–100) ────────────────────────────────
    heat_score: Optional[float] = _compute_heat_score(
        existing_score=_safe_float(row.get("composite_score")),
        premium=premium,
        unusual_volume_ratio=unusual_volume_ratio,
        oi_change_pct=oi_change_pct,
        premium_change_pct=premium_change_pct,
        call_flow_pct=call_flow_pct,
        put_flow_pct=put_flow_pct,
        is_unusual_otm=is_unusual_otm,
        days_to_expiry=days_to_expiry,
    )

    return {
        "premium":                premium,
        "premium_display":        premium_display,
        "premium_change_pct":     premium_change_pct,
        "oi_change_pct":          oi_change_pct,
        "call_flow_pct":          call_flow_pct,
        "put_flow_pct":           put_flow_pct,
        "call_put_premium_ratio": call_put_premium_ratio,
        "call_put_volume_ratio":  call_put_volume_ratio,
        "otm_pct":                otm_pct,
        "is_otm":                 is_otm,
        "is_unusual_otm":         is_unusual_otm,
        "days_to_expiry":         days_to_expiry,
        "expiry":                 expiry,
        "strike":                 strike,
        "option_type":            option_type,
        "side_bias":              side_bias,
        "sweep_like":             sweep_like,
        "unusual_volume_ratio":   unusual_volume_ratio,
        "liquidity_score":        liquidity_score,
        "heat_score":             heat_score,
    }


def _compute_heat_score(
    existing_score: Optional[float],
    premium: Optional[float],
    unusual_volume_ratio: Optional[float],
    oi_change_pct: Optional[float],
    premium_change_pct: Optional[float],
    call_flow_pct: Optional[float],
    put_flow_pct: Optional[float],
    is_unusual_otm: bool,
    days_to_expiry: Optional[int],
) -> Optional[float]:
    """Additive composite heat score, normalized to 0–100."""
    if existing_score is None and premium is None:
        return None

    components: list[tuple[float, float]] = []  # (value_0_to_1, weight)

    # Existing composite score (0–100 → 0–1), highest weight
    if existing_score is not None:
        components.append((min(existing_score / 100.0, 1.0), 35.0))

    # Premium rank (log-normalised: $0 → 0, $10M → 1)
    if premium is not None and premium > 0:
        log_prem = math.log10(max(premium, 1)) / math.log10(10_000_000)
        components.append((min(log_prem, 1.0), 20.0))

    # Unusual volume ratio (capped at 5× = full score)
    if unusual_volume_ratio is not None:
        uvr = min(unusual_volume_ratio / 5.0, 1.0)
        components.append((uvr, 15.0))

    # OI change % (positive = new positions opening; >50% = max score)
    if oi_change_pct is not None and oi_change_pct > 0:
        oic = min(oi_change_pct / 50.0, 1.0)
        components.append((oic, 10.0))

    # Premium change % (momentum)
    if premium_change_pct is not None and premium_change_pct > 0:
        pc = min(premium_change_pct / 100.0, 1.0)
        components.append((pc, 8.0))

    # Directional concentration (>70% one-sided = max)
    if call_flow_pct is not None and put_flow_pct is not None:
        concentration = max(call_flow_pct, put_flow_pct or 0) / 100.0
        components.append((min(concentration / 0.7, 1.0), 7.0))

    # Unusual OTM bonus
    if is_unusual_otm:
        components.append((1.0, 8.0))

    # Short DTE urgency (<7 days = high urgency)
    if days_to_expiry is not None and days_to_expiry > 0:
        urgency = max(0.0, 1.0 - (days_to_expiry - 1) / 14.0)
        components.append((urgency, 5.0))

    if not components:
        return None

    total_weight = sum(w for _, w in components)
    weighted_sum = sum(v * w for v, w in components)
    raw = (weighted_sum / total_weight) * 100.0
    return round(min(raw, 100.0), 1)


# ── Public API ────────────────────────────────────────────────────────────────

def enrich_ticker_rows(rows: list[dict]) -> list[dict]:
    """
    Mutate each row in-place with enriched options fields.
    Update the snapshot store so next cycle can compute deltas.
    Returns the same list (for chaining convenience).
    """
    for row in rows:
        try:
            enriched = _build_enriched_fields(row)
            row.update(enriched)
        except Exception as exc:
            ticker = row.get("ticker", "?")
            print(f"[OPT_ENRICHER] {ticker}: enrichment error — {exc}")

    try:
        update_state(rows)
    except Exception as exc:
        print(f"[OPT_ENRICHER] snapshot update error — {exc}")

    return rows


def sort_ticker_rows(rows: list[dict]) -> list[dict]:
    """
    Sort rows server-side: score DESC → heat_score DESC → premium DESC → volume DESC.
    Returns a new sorted list.
    """
    return sorted(
        rows,
        key=lambda r: (
            -(r.get("composite_score") or 0),
            -(r.get("heat_score") or 0),
            -(r.get("premium") or 0),
            -(r.get("total_volume") or 0),
        ),
    )
