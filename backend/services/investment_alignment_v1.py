"""
Real Investment Alignment V1 — deterministic long-horizon asset quality /
acceleration model. SHADOW / ADDITIVE ONLY.

Answers: "Is this an exceptional asset with the financial trajectory, forward
expectations, competitive position, and structural tailwinds to deserve
long-duration ownership?"

Does NOT answer "is this a good swing trade today" (Trade Alignment) or
"is the current entry attractive" (Entry Structure) or "what should I do with
my current position" (Actionability). Does not consume or modify any of those.

GOVERNANCE (Part 1):
  Zero FMP / Tradier / Grok / LLM / other provider calls on read.
  Pure derived model over EXISTING cached data:
    - backend/data/watchlist_fundamentals_store.py  (Neon, weekly FMP refresh)
    - backend/data/watchlist_stage2_lkg.json        (Stage/technical LKG)
    - backend/data/themes_rs_lkg.json  (via theme_rotation_service, cache-only)
    - backend/services/playbook/curated_anchor_bottlenecks.py (static curated
      supply-chain/moat research — narrow, per-ticker availability)
    - backend/services/theme_resolver.py (canonical PRIMARY theme only)

  No scheduler, no warmer, no fetch-on-miss is added by this module.

AVAILABILITY MODEL:
  Every component carries its own `<name>_available` bool. When a component's
  underlying cached data is missing for a ticker, the component is OMITTED
  (never defaulted to 50) and the outer formula renormalizes over the
  components that ARE available for that ticker.

  Financial Acceleration is MANDATORY: investment_alignment_available is only
  True when financial_acceleration_available is True AND at least 2 of the
  remaining 5 components are available for that ticker.

KNOWN V1 GAPS (documented, not fabricated):
  - Analyst Expectations: no zero-provider-call cached analyst
    consensus/price-target/grade store currently exists in this codebase (the
    only analyst reads found are live StockAnalysis provider calls in
    market_data_service.py). analyst_expectations_available is therefore
    False for every ticker in V1. Recommended future work: extend the
    existing weekly watchlist_fundamentals_refresh cadence to also persist
    FMP grades-consensus / price-target-consensus fields.
  - Margin TREND (expansion/compression) is not computable: the fundamentals
    store only retains the latest snapshot per symbol, not a margin history
    series. Only margin LEVEL (not trajectory) is used as a quality overlay
    inside Financial Acceleration.
  - Moat / Market Role coverage is narrow by construction: only tickers
    present in the curated anchor bottleneck registries (12 static anchor
    supply chains) have moat_market_role_available = True.
"""

from __future__ import annotations

from typing import Any, Optional

INVESTMENT_ALIGNMENT_VERSION = 1

_OUTER_WEIGHTS = {
    "financial_acceleration":     30.0,
    "forward_expectations":       25.0,
    "moat_market_role":           20.0,
    "analyst_expectations":       10.0,
    "investment_theme_tailwind":  10.0,
    "long_term_leadership":        5.0,
}


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _linear_score(v: float, lo: float, hi: float) -> float:
    """Map v in [lo, hi] linearly to [0, 100], clamped."""
    if hi == lo:
        return 50.0
    pct = (v - lo) / (hi - lo)
    return round(_clamp(pct, 0.0, 1.0) * 100.0, 2)


def _to_pct_float(raw: Any) -> Optional[float]:
    """Fundamentals store stores growth/margin fields as formatted strings
    like '34.2%' or '-5.1%'. Parse to a plain float percent value."""
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    if isinstance(raw, str):
        s = raw.strip().replace("%", "").replace(",", "")
        if not s or s.lower() in ("n/a", "na", "-", "none"):
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


def _weighted_available(parts: list[tuple[str, Optional[float], float]]) -> tuple[Optional[float], list[str]]:
    """
    parts: list of (name, score_or_None, weight)
    Returns (renormalized weighted score 0-100 or None, list of used names).
    """
    used = [(n, s, w) for (n, s, w) in parts if s is not None]
    if not used:
        return None, []
    total_w = sum(w for _, _, w in used)
    if total_w <= 0:
        return None, []
    val = sum(s * w for _, s, w in used) / total_w
    return round(val, 2), [n for n, _, _ in used]


# ──────────────────────────────────────────────────────────────────────────
# PART 2 — FINANCIAL ACCELERATION (30%, MANDATORY)
# ──────────────────────────────────────────────────────────────────────────

def compute_financial_acceleration(symbol: str, fundamentals_fields: Optional[dict]) -> dict:
    reason_codes: list[str] = []
    components: dict[str, Any] = {}

    if not fundamentals_fields:
        return {
            "financial_acceleration_available": False,
            "financial_acceleration_score": None,
            "financial_acceleration_components": {},
            "financial_acceleration_reason_codes": ["NO_FUNDAMENTALS_SNAPSHOT"],
        }

    rev_yoy = _to_pct_float(fundamentals_fields.get("Revenue Growth (YoY)"))
    rev_q   = _to_pct_float(fundamentals_fields.get("Revenue Growth (Q)"))
    eps_g   = _to_pct_float(fundamentals_fields.get("EPS Growth"))
    gm      = _to_pct_float(fundamentals_fields.get("Gross Margin"))
    fcf_m   = _to_pct_float(fundamentals_fields.get("FCF Margin"))
    fcf_abs = fundamentals_fields.get("Free Cash Flow")

    parts: list[tuple[str, Optional[float], float]] = []

    if rev_yoy is not None:
        s = _linear_score(rev_yoy, -20.0, 60.0)
        parts.append(("revenue_growth_yoy", s, 0.30))
        components["revenue_growth_yoy_pct"] = rev_yoy
        if rev_yoy >= 25.0:
            reason_codes.append("REVENUE_GROWTH_STRONG")

    rev_accel = None
    if rev_yoy is not None and rev_q is not None:
        rev_accel = round(rev_q - rev_yoy, 2)
        s = _linear_score(rev_accel, -20.0, 20.0)
        parts.append(("revenue_acceleration", s, 0.25))
        components["revenue_acceleration_pts"] = rev_accel
        if rev_accel >= 5.0:
            reason_codes.append("REVENUE_GROWTH_ACCELERATING")
        elif rev_accel <= -5.0:
            reason_codes.append("REVENUE_GROWTH_DECELERATING")

    if eps_g is not None:
        s = _linear_score(eps_g, -30.0, 60.0)
        parts.append(("eps_growth", s, 0.25))
        components["eps_growth_pct"] = eps_g
        if eps_g >= 25.0:
            reason_codes.append("EPS_GROWTH_ACCELERATING")

    if gm is not None:
        s = _linear_score(gm, 0.0, 40.0)
        parts.append(("gross_margin_level", s, 0.10))
        components["gross_margin_pct"] = gm

    if fcf_m is not None:
        s = _linear_score(fcf_m, -10.0, 30.0)
        parts.append(("fcf_margin_level", s, 0.10))
        components["fcf_margin_pct"] = fcf_m
        if fcf_m > 0 and fcf_abs and float(fcf_abs) > 0:
            reason_codes.append("FCF_INFLECTING_POSITIVE")
        elif fcf_m < 0:
            reason_codes.append("CASH_GENERATION_DETERIORATING")

    score, used = _weighted_available(parts)

    # Mandatory anchor: need at least revenue OR EPS growth to be meaningful.
    available = score is not None and (rev_yoy is not None or eps_g is not None)

    return {
        "financial_acceleration_available":   available,
        "financial_acceleration_score":        score if available else None,
        "financial_acceleration_components":   components,
        "financial_acceleration_reason_codes": reason_codes,
    }


# ──────────────────────────────────────────────────────────────────────────
# PART 3 — FORWARD EXPECTATIONS (25%)
# ──────────────────────────────────────────────────────────────────────────

def compute_forward_expectations(symbol: str, fundamentals_fields: Optional[dict],
                                  missing_fields: Optional[list[str]]) -> dict:
    reason_codes: list[str] = []
    components: dict[str, Any] = {}

    if not fundamentals_fields:
        return {
            "forward_expectations_available": False,
            "forward_expectations_score": None,
            "forward_expectations_components": {},
            "forward_expectations_reason_codes": ["NO_FUNDAMENTALS_SNAPSHOT"],
        }

    missing = set(missing_fields or [])
    rev_next_q = None
    if "Rev Growth Next Quarter" not in missing:
        rev_next_q = _to_pct_float(fundamentals_fields.get("Rev Growth Next Quarter"))

    eps_this_q = None
    if "EPS Growth This Quarter" not in missing:
        eps_this_q = _to_pct_float(fundamentals_fields.get("EPS Growth This Quarter"))

    parts: list[tuple[str, Optional[float], float]] = []

    if rev_next_q is not None:
        s = _linear_score(rev_next_q, -20.0, 50.0)
        parts.append(("forward_revenue_growth", s, 0.55))
        components["forward_revenue_growth_next_q_pct"] = rev_next_q
        if rev_next_q >= 20.0:
            reason_codes.append("FORWARD_REVENUE_GROWTH_STRONG")

    if eps_this_q is not None:
        s = _linear_score(eps_this_q, -30.0, 60.0)
        parts.append(("forward_eps_growth", s, 0.45))
        components["forward_eps_growth_this_q_pct"] = eps_this_q
        if eps_this_q >= 25.0:
            reason_codes.append("FORWARD_EPS_GROWTH_STRONG")
        elif eps_this_q <= -10.0:
            reason_codes.append("FORWARD_EPS_GROWTH_WEAKENING")

    score, used = _weighted_available(parts)
    available = score is not None

    if not available:
        reason_codes.append("FORWARD_ESTIMATES_UNAVAILABLE")

    return {
        "forward_expectations_available":   available,
        "forward_expectations_score":        score,
        "forward_expectations_components":   components,
        "forward_expectations_reason_codes": reason_codes,
    }


# ──────────────────────────────────────────────────────────────────────────
# PART 4 — MOAT / MARKET ROLE (20%)
# ──────────────────────────────────────────────────────────────────────────

_MOAT_ROLE_CACHE: Optional[dict[str, dict]] = None


def _load_moat_role_map() -> dict[str, dict]:
    """Zero-call: curated static anchor bottleneck registry, built once and
    cached in-process for the life of the worker."""
    global _MOAT_ROLE_CACHE
    if _MOAT_ROLE_CACHE is not None:
        return _MOAT_ROLE_CACHE
    out: dict[str, dict] = {}
    try:
        from services.playbook.curated_anchor_bottlenecks import get_multi_anchor_screener
        rows = get_multi_anchor_screener(min_anchors=1, limit=1000)
        for r in rows:
            tkr = (r.get("ticker") or "").upper()
            if not tkr:
                continue
            out[tkr] = r
    except Exception:
        out = {}
    _MOAT_ROLE_CACHE = out
    return out


def compute_moat_market_role(symbol: str) -> dict:
    m = _load_moat_role_map().get(symbol.upper())
    if not m:
        return {
            "moat_market_role_available": False,
            "moat_market_role_score": None,
            "moat_market_role_components": {},
            "moat_market_role_reason_codes": ["NO_CURATED_MOAT_SOURCE"],
        }

    max_score = float(m.get("max_bottleneck_score") or 0.0)
    anchor_count = int(m.get("anchor_count") or 1)
    reason_codes = ["SUPPLY_CHAIN_CRITICALITY"]
    if anchor_count >= 3:
        reason_codes.append("MULTI_ANCHOR_STRUCTURAL_ROLE")
    if max_score >= 85:
        reason_codes.append("BOTTLENECK_SEVERITY_HIGH")

    return {
        "moat_market_role_available": True,
        "moat_market_role_score": round(_clamp(max_score, 0.0, 100.0), 2),
        "moat_market_role_components": {
            "max_bottleneck_score": max_score,
            "anchor_count": anchor_count,
            "anchors": m.get("anchors"),
        },
        "moat_market_role_reason_codes": reason_codes,
    }


# ──────────────────────────────────────────────────────────────────────────
# PART 5 — ANALYST EXPECTATIONS (10%) — PROVEN UNAVAILABLE FOR V1
# ──────────────────────────────────────────────────────────────────────────

def compute_analyst_expectations(symbol: str) -> dict:
    """
    AUDIT RESULT: no zero-provider-call cached analyst consensus / price
    target / grade store exists in this codebase. The only analyst-data
    reads found (backend/data/market_data_service.py) call
    stockanalysis.get_analyst_ratings(ticker) live, on every invocation —
    that is a live provider call, not a cache read, and Part 1 forbids
    provider calls on the Investment Alignment hot read path.

    Therefore this component is proven unavailable for every ticker in V1.
    Do not fabricate. See module docstring "KNOWN V1 GAPS".
    """
    return {
        "analyst_expectations_available": False,
        "analyst_expectations_score": None,
        "analyst_expectations_components": {},
        "analyst_expectations_reason_codes": ["NO_ZERO_CALL_ANALYST_CACHE"],
    }


# ──────────────────────────────────────────────────────────────────────────
# PART 6 — THEME / STRUCTURAL TAILWIND (10%)
# ──────────────────────────────────────────────────────────────────────────

_ROTATION_SNAPSHOT_CACHE: Optional[dict[str, dict]] = None


def _load_rotation_by_theme() -> dict[str, dict]:
    global _ROTATION_SNAPSHOT_CACHE
    if _ROTATION_SNAPSHOT_CACHE is not None:
        return _ROTATION_SNAPSHOT_CACHE
    out: dict[str, dict] = {}
    try:
        from services.theme_rotation_service import build_theme_rotation_snapshot
        snap = build_theme_rotation_snapshot()
        for row in snap.get("themes", []):
            out[row["theme_id"]] = row
    except Exception:
        out = {}
    _ROTATION_SNAPSHOT_CACHE = out
    return out


_THEME_RESOLVER_CTX_CACHE = None


def _get_theme_resolver_ctx():
    """Built ONCE per process and reused — resolve_primary_theme_for_ticker
    rebuilds a full theme resolution context on every call if ctx is
    omitted, which is far too expensive to do per-ticker across a universe
    scan. Zero provider calls either way (pure cache/config read)."""
    global _THEME_RESOLVER_CTX_CACHE
    if _THEME_RESOLVER_CTX_CACHE is not None:
        return _THEME_RESOLVER_CTX_CACHE
    try:
        from services.theme_resolver import build_theme_resolution_context
        _THEME_RESOLVER_CTX_CACHE = build_theme_resolution_context()
    except Exception:
        _THEME_RESOLVER_CTX_CACHE = None
    return _THEME_RESOLVER_CTX_CACHE


def compute_investment_theme_tailwind(symbol: str) -> dict:
    try:
        from services.theme_resolver import resolve_primary_theme_for_ticker
        resolution = resolve_primary_theme_for_ticker(symbol, ctx=_get_theme_resolver_ctx())
    except Exception:
        resolution = {}

    theme_id = resolution.get("theme_id")
    if not theme_id:
        return {
            "investment_theme_tailwind_available": False,
            "investment_theme_tailwind_score": None,
            "primary_theme_id": None,
            "investment_theme_tailwind_reason_codes": ["NO_PRIMARY_THEME"],
        }

    rot = _load_rotation_by_theme().get(theme_id)
    if not rot:
        return {
            "investment_theme_tailwind_available": False,
            "investment_theme_tailwind_score": None,
            "primary_theme_id": theme_id,
            "investment_theme_tailwind_reason_codes": ["NO_ROTATION_DATA_FOR_THEME"],
        }

    rotation_score = float(rot.get("rotation_score") or 0.0)  # 0..1
    phase = rot.get("rotation_phase")
    score = round(_clamp(rotation_score, 0.0, 1.0) * 100.0, 2)

    reason_codes = []
    if phase == "LEADING":
        reason_codes.append("THEME_LEADING")
    elif phase == "CONFIRMING":
        reason_codes.append("THEME_CONFIRMING")
    elif phase == "STALLING":
        reason_codes.append("THEME_STALLING")
    elif phase == "BOTTOMING":
        reason_codes.append("THEME_BOTTOMING")
    elif phase == "LAGGING":
        reason_codes.append("THEME_LAGGING")
    else:
        reason_codes.append("THEME_UNCLASSIFIED")

    return {
        "investment_theme_tailwind_available": True,
        "investment_theme_tailwind_score": score,
        "primary_theme_id": theme_id,
        "investment_theme_tailwind_reason_codes": reason_codes,
    }


# ──────────────────────────────────────────────────────────────────────────
# PART 7 — LONG-TERM LEADERSHIP (5%)
# ──────────────────────────────────────────────────────────────────────────

def compute_long_term_leadership(symbol: str, stage2_row: Optional[dict]) -> dict:
    if not stage2_row:
        return {
            "long_term_leadership_available": False,
            "long_term_leadership_score": None,
            "long_term_leadership_reason_codes": ["NO_STAGE2_ROW"],
        }

    tm = stage2_row.get("technical_metrics") or {}
    pct_vs_200 = tm.get("pct_vs_sma_200")
    range_pos  = tm.get("range_position_52w")
    ma_stack   = tm.get("ma_stack")

    parts: list[tuple[str, Optional[float], float]] = []
    if pct_vs_200 is not None:
        parts.append(("pct_vs_sma_200", _linear_score(float(pct_vs_200), -20.0, 80.0), 0.6))
    if range_pos is not None:
        parts.append(("range_position_52w", _linear_score(float(range_pos), 0.0, 100.0), 0.4))

    score, used = _weighted_available(parts)
    if score is None:
        return {
            "long_term_leadership_available": False,
            "long_term_leadership_score": None,
            "long_term_leadership_reason_codes": ["INSUFFICIENT_TECHNICAL_METRICS"],
        }

    reason_codes = []
    if ma_stack == "bullish":
        reason_codes.append("LONG_TERM_UPTREND_STRUCTURE")
        score = min(100.0, score + 5.0)
    if pct_vs_200 is not None and pct_vs_200 >= 20:
        reason_codes.append("ABOVE_LONG_TERM_TREND")
    if range_pos is not None and range_pos >= 80:
        reason_codes.append("NEAR_52W_HIGH_LEADERSHIP")

    return {
        "long_term_leadership_available": True,
        "long_term_leadership_score": round(score, 2),
        "long_term_leadership_reason_codes": reason_codes,
    }


# ──────────────────────────────────────────────────────────────────────────
# PART 8/9/10 — OUTER FORMULA, STATE, TRANSPARENT OUTPUT
# ──────────────────────────────────────────────────────────────────────────

def _state_from_score(score: Optional[float]) -> str:
    if score is None:
        return "INSUFFICIENT_DATA"
    if score >= 90:
        return "ELITE"
    if score >= 80:
        return "ELITE"
    if score >= 70:
        return "STRONG"
    if score >= 60:
        return "GOOD"
    if score >= 40:
        return "DEVELOPING" if score >= 50 else "MIXED"
    if score >= 20:
        return "WEAK"
    return "DETERIORATING"


def compute_investment_alignment(
    symbol: str,
    fundamentals_fields: Optional[dict],
    fundamentals_missing: Optional[list[str]],
    stage2_row: Optional[dict],
) -> dict:
    symbol = symbol.upper()

    fa = compute_financial_acceleration(symbol, fundamentals_fields)
    fe = compute_forward_expectations(symbol, fundamentals_fields, fundamentals_missing)
    mm = compute_moat_market_role(symbol)
    ae = compute_analyst_expectations(symbol)
    tt = compute_investment_theme_tailwind(symbol)
    ll = compute_long_term_leadership(symbol, stage2_row)

    component_map = {
        "financial_acceleration":    (fa["financial_acceleration_available"],    fa["financial_acceleration_score"]),
        "forward_expectations":      (fe["forward_expectations_available"],      fe["forward_expectations_score"]),
        "moat_market_role":          (mm["moat_market_role_available"],          mm["moat_market_role_score"]),
        "analyst_expectations":      (ae["analyst_expectations_available"],      ae["analyst_expectations_score"]),
        "investment_theme_tailwind": (tt["investment_theme_tailwind_available"], tt["investment_theme_tailwind_score"]),
        "long_term_leadership":      (ll["long_term_leadership_available"],      ll["long_term_leadership_score"]),
    }

    additional_available = sum(
        1 for name, (avail, _) in component_map.items()
        if name != "financial_acceleration" and avail
    )

    minimum_evidence_met = fa["financial_acceleration_available"] and additional_available >= 2

    reason_codes: list[str] = (
        fa["financial_acceleration_reason_codes"] +
        fe["forward_expectations_reason_codes"] +
        mm["moat_market_role_reason_codes"] +
        ae["analyst_expectations_reason_codes"] +
        tt["investment_theme_tailwind_reason_codes"] +
        ll["long_term_leadership_reason_codes"]
    )

    if not minimum_evidence_met:
        return {
            "investment_alignment_available": False,
            "investment_alignment_score": None,
            "investment_alignment_state": "INSUFFICIENT_DATA",
            "investment_alignment_version": INVESTMENT_ALIGNMENT_VERSION,
            "investment_alignment_components": component_map_to_dict(fa, fe, mm, ae, tt, ll),
            "investment_alignment_reason_codes": reason_codes,
            "investment_alignment_strengths": [],
            "investment_alignment_risks": [],
            "minimum_evidence_met": False,
            "additional_components_available": additional_available,
        }

    parts = [
        (name, score, _OUTER_WEIGHTS[name])
        for name, (avail, score) in component_map.items()
        if avail and score is not None
    ]
    total_w = sum(w for _, _, w in parts)
    score = round(sum(s * w for _, s, w in parts) / total_w, 2) if total_w else None

    state = _state_from_score(score)

    strengths = []
    risks = []
    if fa["financial_acceleration_available"] and fa["financial_acceleration_score"] is not None:
        if fa["financial_acceleration_score"] >= 70:
            strengths.append("Strong/accelerating financial trajectory")
        elif fa["financial_acceleration_score"] < 35:
            risks.append("Weak or decelerating financial trajectory")
    if fe["forward_expectations_available"] and fe["forward_expectations_score"] is not None:
        if fe["forward_expectations_score"] >= 70:
            strengths.append("Strong forward growth expectations")
        elif fe["forward_expectations_score"] < 35:
            risks.append("Weak forward growth expectations")
    if mm["moat_market_role_available"] and mm["moat_market_role_score"] is not None and mm["moat_market_role_score"] >= 75:
        strengths.append("Structurally critical supply-chain / market role")
    if tt["investment_theme_tailwind_available"] and tt["investment_theme_tailwind_score"] is not None:
        if tt["investment_theme_tailwind_score"] >= 70:
            strengths.append("Theme exhibiting durable leadership")
        elif tt["investment_theme_tailwind_score"] < 30:
            risks.append("Primary theme lagging")
    if ll["long_term_leadership_available"] and ll["long_term_leadership_score"] is not None and ll["long_term_leadership_score"] < 30:
        risks.append("No long-term price leadership confirmation yet")

    return {
        "investment_alignment_available": True,
        "investment_alignment_score": score,
        "investment_alignment_state": state,
        "investment_alignment_version": INVESTMENT_ALIGNMENT_VERSION,
        "investment_alignment_components": component_map_to_dict(fa, fe, mm, ae, tt, ll),
        "investment_alignment_reason_codes": reason_codes,
        "investment_alignment_strengths": strengths,
        "investment_alignment_risks": risks,
        "minimum_evidence_met": True,
        "additional_components_available": additional_available,
    }


def component_map_to_dict(fa, fe, mm, ae, tt, ll) -> dict:
    return {
        "financial_acceleration": {
            "available": fa["financial_acceleration_available"],
            "score": fa["financial_acceleration_score"],
            "weight_pct": _OUTER_WEIGHTS["financial_acceleration"],
            "components": fa["financial_acceleration_components"],
            "reason_codes": fa["financial_acceleration_reason_codes"],
        },
        "forward_expectations": {
            "available": fe["forward_expectations_available"],
            "score": fe["forward_expectations_score"],
            "weight_pct": _OUTER_WEIGHTS["forward_expectations"],
            "components": fe["forward_expectations_components"],
            "reason_codes": fe["forward_expectations_reason_codes"],
        },
        "moat_market_role": {
            "available": mm["moat_market_role_available"],
            "score": mm["moat_market_role_score"],
            "weight_pct": _OUTER_WEIGHTS["moat_market_role"],
            "components": mm["moat_market_role_components"],
            "reason_codes": mm["moat_market_role_reason_codes"],
        },
        "analyst_expectations": {
            "available": ae["analyst_expectations_available"],
            "score": ae["analyst_expectations_score"],
            "weight_pct": _OUTER_WEIGHTS["analyst_expectations"],
            "components": ae["analyst_expectations_components"],
            "reason_codes": ae["analyst_expectations_reason_codes"],
        },
        "investment_theme_tailwind": {
            "available": tt["investment_theme_tailwind_available"],
            "score": tt["investment_theme_tailwind_score"],
            "weight_pct": _OUTER_WEIGHTS["investment_theme_tailwind"],
            "primary_theme_id": tt.get("primary_theme_id"),
            "reason_codes": tt["investment_theme_tailwind_reason_codes"],
        },
        "long_term_leadership": {
            "available": ll["long_term_leadership_available"],
            "score": ll["long_term_leadership_score"],
            "weight_pct": _OUTER_WEIGHTS["long_term_leadership"],
            "reason_codes": ll["long_term_leadership_reason_codes"],
        },
    }


def reset_caches() -> None:
    """Test/debug helper — clears in-process moat + rotation caches."""
    global _MOAT_ROLE_CACHE, _ROTATION_SNAPSHOT_CACHE
    _MOAT_ROLE_CACHE = None
    _ROTATION_SNAPSHOT_CACHE = None


def compute_investment_alignment_bulk(symbols: list[str]) -> dict[str, dict]:
    """
    Bulk entry point — pure cache reads only, zero provider calls.
    Loads fundamentals + stage2 LKG in bulk once, then computes per symbol.
    """
    from data.watchlist_fundamentals_store import get_snapshots_bulk
    from services.watchlist_stage2_service import get_stage2

    symbols = [s.upper() for s in symbols]
    snaps = get_snapshots_bulk(symbols)

    out: dict[str, dict] = {}
    for sym in symbols:
        snap = snaps.get(sym) or {}
        fields = snap.get("fields") or {}
        missing = snap.get("missing_fields") or []
        stage2_row = None
        try:
            stage2_row = get_stage2(sym)
        except Exception:
            stage2_row = None
        out[sym] = compute_investment_alignment(sym, fields, missing, stage2_row)
    return out
