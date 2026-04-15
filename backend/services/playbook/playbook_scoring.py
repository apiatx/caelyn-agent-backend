"""
Playbook Scoring Engine — per-ticker factor computation and weighted scoring.

──────────────────────────────────────────────────────────────────────────────
FACTOR IMPLEMENTATION STATUS (v1.0)
──────────────────────────────────────────────────────────────────────────────
REAL (computed from FMP data):
  balance_sheet_strength      — debt/equity ratio
  valuation_discount_vs_peers — PE ratio vs sector median
  revenue_growth              — YoY revenue growth from financial-growth endpoint
  small_cap_asymmetry         — market cap tiers
  technical_confirmation      — 52-week price range position

STUBBED (neutral 50/100, no data source yet):
  bottleneck_exposure         — TODO: custom supply-chain tagging data
  supply_chain_confirmation   — TODO: custom supply-chain data
  dilution_risk               — TODO: share issuance history
  revenue_acceleration        — TODO: quarterly revenue series (2+ periods)
  ebitda_inflection_proximity — TODO: EBITDA margin trend
  backlog_quality             — TODO: 10-K/10-Q backlog parsing
  catalyst_proximity          — TODO: earnings calendar integration
  crowding_risk               — TODO: short interest / institutional concentration
  execution_risk              — TODO: qualitative scoring
  insider_buying              — TODO: insider_activity_service integration
  policy_tailwind             — TODO: thematic policy mapping
  evidence_freshness          — TODO: news event recency scoring
  theme_alignment             — TODO: sector/theme keyword mapping
  sector_strength             — TODO: existing sector_rotation analytics integration

Stubbed factors produce a neutral score and appear in result.stub_factors.
This ensures the engine is usable today while being clearly evolvable.
──────────────────────────────────────────────────────────────────────────────
"""
from __future__ import annotations

import asyncio
import os
from typing import Any, Dict, List, Optional, Tuple

import httpx

from services.playbook.playbook_types import (
    PlaybookDefinition,
    PlaybookScoreResult,
    TickerRawData,
)

# ── Constants ────────────────────────────────────────────────────────────────

_FMP_BASE = "https://financialmodelingprep.com/stable"
_FMP_TTL  = 900    # 15-minute cache — scoring data doesn't need real-time freshness

# Sector median PE ratios (FY2024 approximations — update periodically)
_SECTOR_PE_MEDIANS: Dict[str, float] = {
    "Technology":               28.0,
    "Healthcare":               22.0,
    "Consumer Cyclical":        18.0,
    "Financial Services":       12.0,
    "Energy":                   10.0,
    "Utilities":                16.0,
    "Basic Materials":          14.0,
    "Consumer Defensive":       20.0,
    "Communication Services":   20.0,
    "Industrials":              18.0,
    "Real Estate":              30.0,
}
_DEFAULT_PE_MEDIAN = 18.0    # fallback when sector unknown


# ── FMP data fetch ───────────────────────────────────────────────────────────

async def _fmp_get(
    client: httpx.AsyncClient,
    endpoint: str,
    params: Dict[str, str],
    api_key: str,
) -> Any:
    """Single FMP GET with basic error handling. Returns [] on failure."""
    try:
        from data.cache import cache as _cache
        cache_key = f"playbook:fmp:{endpoint}:{str(params)[:80]}"
        cached = _cache.get(cache_key)
        if cached is not None:
            return cached
    except Exception:
        pass  # cache unavailable — proceed without it

    try:
        params_with_key = {**params, "apikey": api_key}
        resp = await client.get(
            f"{_FMP_BASE}/{endpoint}",
            params=params_with_key,
            timeout=8.0,
        )
        if resp.status_code != 200:
            return []
        result = resp.json()
        try:
            from data.cache import cache as _cache
            from data.cache import FMP_TTL
            _cache.set(cache_key, result, FMP_TTL)
        except Exception:
            pass
        return result
    except Exception as e:
        print(f"[PLAYBOOK_SCORING] FMP fetch error ({endpoint}): {e}")
        return []


async def fetch_ticker_data(ticker: str, api_key: str) -> TickerRawData:
    """
    Fetch all scoring-relevant data for a single ticker from FMP.
    Returns TickerRawData with None fields on failure — scoring stubs fill gaps.
    """
    t = ticker.upper().strip()
    if not api_key:
        print(f"[PLAYBOOK_SCORING] No FMP_API_KEY — using all stubs for {t}")
        return TickerRawData(ticker=t, fetch_error="No FMP_API_KEY configured")

    async with httpx.AsyncClient(timeout=10.0) as client:
        profile_raw, metrics_raw, growth_raw = await asyncio.gather(
            _fmp_get(client, "profile", {"symbol": t}, api_key),
            _fmp_get(client, "key-metrics", {"symbol": t, "period": "annual", "limit": "1"}, api_key),
            _fmp_get(client, "financial-growth", {"symbol": t, "period": "annual", "limit": "1"}, api_key),
            return_exceptions=True,
        )

    # Unwrap exceptions (return empty lists so downstream stays None)
    if isinstance(profile_raw,  Exception): profile_raw  = []
    if isinstance(metrics_raw,  Exception): metrics_raw  = []
    if isinstance(growth_raw,   Exception): growth_raw   = []

    profile  = (profile_raw[0]  if isinstance(profile_raw,  list) and profile_raw  else {})
    metrics  = (metrics_raw[0]  if isinstance(metrics_raw,  list) and metrics_raw  else {})
    growth   = (growth_raw[0]   if isinstance(growth_raw,   list) and growth_raw   else {})

    # Parse 52-week range from "low-high" string e.g. "123.45-234.56"
    w52_high: Optional[float] = None
    w52_low:  Optional[float] = None
    range_str = profile.get("range", "") or ""
    if "-" in str(range_str):
        parts = str(range_str).split("-")
        try:
            w52_low  = float(parts[0])
            w52_high = float(parts[1])
        except (ValueError, IndexError):
            pass

    def _float(d: dict, *keys) -> Optional[float]:
        for k in keys:
            v = d.get(k)
            if v is not None:
                try:
                    return float(v)
                except (TypeError, ValueError):
                    pass
        return None

    return TickerRawData(
        ticker=t,
        price=             _float(profile, "price"),
        mkt_cap=           _float(profile, "mktCap"),
        sector=            profile.get("sector") or None,
        industry=          profile.get("industry") or None,
        pe_ratio=          _float(metrics, "peRatio") or _float(profile, "pe"),
        debt_to_equity=    _float(metrics, "debtToEquityRatio") or _float(profile, "debtToEquity"),
        revenue_growth_yoy=_float(growth, "revenueGrowth"),
        week52_high=       w52_high,
        week52_low=        w52_low,
        day_change_pct=    _float(profile, "changes"),
    )


# ── Factor computation ───────────────────────────────────────────────────────

_STUB_SCORE = 50.0    # neutral score used for uncomputable factors
_ALL_FACTORS = [
    "bottleneck_exposure", "supply_chain_confirmation", "balance_sheet_strength",
    "dilution_risk", "valuation_discount_vs_peers", "revenue_growth",
    "revenue_acceleration", "ebitda_inflection_proximity", "backlog_quality",
    "catalyst_proximity", "crowding_risk", "technical_confirmation",
    "execution_risk", "small_cap_asymmetry", "insider_buying",
    "policy_tailwind", "evidence_freshness", "theme_alignment", "sector_strength",
]


def _score_balance_sheet(d_e: Optional[float]) -> float:
    """Debt-to-equity ratio → 0-100. Lower D/E = stronger balance sheet."""
    if d_e is None:
        return _STUB_SCORE
    if d_e < 0:        # negative equity edge case
        return 20.0
    if d_e < 0.30:     return 92.0
    if d_e < 0.70:     return 78.0
    if d_e < 1.50:     return 60.0
    if d_e < 3.00:     return 38.0
    return 15.0


def _score_valuation_vs_peers(pe: Optional[float], sector: Optional[str]) -> float:
    """PE ratio relative to sector median → 0-100. Lower PE = better value."""
    if pe is None or pe <= 0:
        return _STUB_SCORE    # negative/N/A PE (growth stage) — neutral
    median = _SECTOR_PE_MEDIANS.get(sector or "", _DEFAULT_PE_MEDIAN)
    ratio = pe / median
    if ratio < 0.50:  return 92.0   # deep value
    if ratio < 0.75:  return 80.0   # significant discount
    if ratio < 0.90:  return 65.0   # mild discount
    if ratio < 1.10:  return 50.0   # fair value
    if ratio < 1.40:  return 35.0   # premium
    return 18.0                      # significant premium


def _score_revenue_growth(growth_yoy: Optional[float]) -> float:
    """YoY revenue growth (decimal) → 0-100."""
    if growth_yoy is None:
        return _STUB_SCORE
    pct = growth_yoy * 100    # convert to percentage
    if pct >= 40:   return 95.0
    if pct >= 25:   return 82.0
    if pct >= 15:   return 68.0
    if pct >= 5:    return 55.0
    if pct >= 0:    return 42.0
    if pct >= -10:  return 28.0
    return 12.0


def _score_small_cap_asymmetry(mkt_cap: Optional[float]) -> float:
    """Market cap (USD) → 0-100. Smaller cap = higher asymmetry potential."""
    if mkt_cap is None:
        return _STUB_SCORE
    b = mkt_cap / 1e9    # billions
    if b < 0.3:     return 92.0   # micro-cap
    if b < 1.0:     return 82.0   # small-cap
    if b < 5.0:     return 65.0   # mid-small
    if b < 15.0:    return 45.0   # mid-cap
    if b < 50.0:    return 28.0   # large-cap
    return 12.0                    # mega-cap


def _score_technical_confirmation(
    price: Optional[float],
    w52_high: Optional[float],
    w52_low: Optional[float],
) -> float:
    """
    Price position in 52-week range → 0-100.
    Near 52w high = technically strong; near 52w low = breakdown/weakness.
    """
    if price is None or w52_high is None or w52_low is None:
        return _STUB_SCORE
    rng = w52_high - w52_low
    if rng <= 0:
        return _STUB_SCORE
    position = (price - w52_low) / rng    # 0 = at 52w low, 1 = at 52w high
    if position >= 0.85:   return 88.0   # near highs — strong trend
    if position >= 0.65:   return 72.0   # upper range
    if position >= 0.45:   return 55.0   # middle range
    if position >= 0.25:   return 38.0   # lower range
    return 20.0                           # near 52w lows


def compute_factors(raw: TickerRawData) -> Tuple[Dict[str, float], List[str]]:
    """
    Compute all factor scores for a ticker.
    Returns (factor_scores dict, stub_factors list).
    """
    stubs: List[str] = []
    scores: Dict[str, float] = {}

    # ── Real factors ─────────────────────────────────────────────────────────
    bs = _score_balance_sheet(raw.debt_to_equity)
    scores["balance_sheet_strength"] = bs
    if raw.debt_to_equity is None:
        stubs.append("balance_sheet_strength")

    vd = _score_valuation_vs_peers(raw.pe_ratio, raw.sector)
    scores["valuation_discount_vs_peers"] = vd
    if raw.pe_ratio is None:
        stubs.append("valuation_discount_vs_peers")

    rg = _score_revenue_growth(raw.revenue_growth_yoy)
    scores["revenue_growth"] = rg
    if raw.revenue_growth_yoy is None:
        stubs.append("revenue_growth")

    sc = _score_small_cap_asymmetry(raw.mkt_cap)
    scores["small_cap_asymmetry"] = sc
    if raw.mkt_cap is None:
        stubs.append("small_cap_asymmetry")

    tc = _score_technical_confirmation(raw.price, raw.week52_high, raw.week52_low)
    scores["technical_confirmation"] = tc
    if raw.price is None or raw.week52_high is None or raw.week52_low is None:
        stubs.append("technical_confirmation")

    # ── Stub factors (neutral 50) ─────────────────────────────────────────────
    stub_factor_names = [
        "bottleneck_exposure",
        "supply_chain_confirmation",
        "dilution_risk",
        "revenue_acceleration",
        "ebitda_inflection_proximity",
        "backlog_quality",
        "catalyst_proximity",
        "crowding_risk",
        "execution_risk",
        "insider_buying",
        "policy_tailwind",
        "evidence_freshness",
        "theme_alignment",
        "sector_strength",
    ]
    for f in stub_factor_names:
        scores[f] = _STUB_SCORE
        stubs.append(f)

    return scores, stubs


# ── Hard filter evaluation ───────────────────────────────────────────────────

def _apply_filter(raw: TickerRawData, hf) -> Optional[str]:
    """Return failure reason string if filter fails, None if passes."""
    value = getattr(raw, hf.field, None)
    if value is None:
        return None    # can't evaluate — pass (don't penalize missing data)
    try:
        v = float(value)
        limit = float(hf.value)
        ops = {
            "gte": v >= limit,
            "lte": v <= limit,
            "gt":  v > limit,
            "lt":  v < limit,
            "eq":  v == limit,
            "neq": v != limit,
        }
        passed = ops.get(hf.op, True)
        return None if passed else hf.label
    except (TypeError, ValueError):
        return None


def evaluate_hard_filters(
    raw: TickerRawData,
    playbook: PlaybookDefinition,
) -> List[str]:
    """Return list of hard filter failure reasons (empty = all passed)."""
    failures: List[str] = []
    for hf in playbook.hard_filters:
        result = _apply_filter(raw, hf)
        if result:
            failures.append(result)
    return failures


# ── Score aggregation ────────────────────────────────────────────────────────

def _summary_label(score: float, hf_pass: bool) -> str:
    if not hf_pass:
        return "Hard filter failure"
    if score >= 80: return "Strong playbook fit"
    if score >= 65: return "Good playbook alignment"
    if score >= 50: return "Moderate alignment"
    if score >= 35: return "Weak alignment"
    return "Poor playbook fit"


def _matched_rules(factor_scores: Dict[str, float]) -> List[str]:
    """Return rule labels for factors with a strong signal (≥ 70)."""
    rules: List[str] = []
    _rule_map = {
        "balance_sheet_strength":     "clean_balance_sheet",
        "small_cap_asymmetry":        "small_cap_setup",
        "technical_confirmation":     "technical_confirmation",
        "revenue_growth":             "revenue_growth_confirmed",
        "valuation_discount_vs_peers":"valuation_discount",
        "bottleneck_exposure":        "physical_bottleneck",
        "sector_strength":            "hot_sector",
        "catalyst_proximity":         "event_window_open",
    }
    for factor, rule in _rule_map.items():
        if factor_scores.get(factor, 0) >= 70:
            rules.append(rule)
    return rules


def _risk_notes(raw: TickerRawData, factor_scores: Dict[str, float]) -> List[str]:
    """Generate human-readable risk observations."""
    risks: List[str] = []
    if raw.debt_to_equity is not None and raw.debt_to_equity > 2.0:
        risks.append(f"High leverage (D/E {raw.debt_to_equity:.1f})")
    if raw.revenue_growth_yoy is not None and raw.revenue_growth_yoy < 0:
        risks.append(f"Revenue declining ({raw.revenue_growth_yoy * 100:.1f}% YoY)")
    if factor_scores.get("technical_confirmation", 50) < 35:
        risks.append("Price near 52-week lows — downtrend risk")
    if raw.pe_ratio and raw.pe_ratio > 60:
        risks.append(f"Elevated valuation (PE {raw.pe_ratio:.0f}x)")
    return risks


def aggregate_score(
    playbook: PlaybookDefinition,
    factor_scores: Dict[str, float],
    raw: TickerRawData,
) -> Tuple[float, Dict[str, float]]:
    """
    Compute weighted final score and return (final_score, penalties_applied).
    """
    # Weighted sum — only factors present in playbook weights are used
    raw_score = sum(
        playbook.factor_weights.get(factor, 0.0) * score
        for factor, score in factor_scores.items()
    )

    # Apply penalty rules
    penalties_applied: Dict[str, float] = {}
    for rule in playbook.penalty_rules:
        factor_score = factor_scores.get(rule.factor, _STUB_SCORE)
        if factor_score > rule.threshold:
            raw_score -= rule.deduction
            penalties_applied[rule.factor] = rule.deduction

    final = max(0.0, min(100.0, raw_score))
    return round(final, 1), penalties_applied


# ── Main scoring entry point ─────────────────────────────────────────────────

async def score_ticker(
    ticker: str,
    playbook: PlaybookDefinition,
    fmp_api_key: str,
) -> PlaybookScoreResult:
    """Score a single ticker against a playbook. Never raises — fails safely."""
    t = ticker.upper().strip()
    print(f"[PLAYBOOK_SCORING] Scoring {t!r} against playbook={playbook.id!r}")

    try:
        raw = await fetch_ticker_data(t, fmp_api_key)
    except Exception as e:
        print(f"[PLAYBOOK_SCORING] Data fetch error for {t}: {e}")
        raw = TickerRawData(ticker=t, fetch_error=str(e))

    factor_scores, stubs = compute_factors(raw)
    hf_failures = evaluate_hard_filters(raw, playbook)
    final_score, penalties = aggregate_score(playbook, factor_scores, raw)
    hf_pass = len(hf_failures) == 0

    result = PlaybookScoreResult(
        ticker=t,
        playbook_id=playbook.id,
        final_score=final_score,
        hard_filter_pass=hf_pass,
        hard_filter_failures=hf_failures,
        summary_label=_summary_label(final_score, hf_pass),
        factor_scores=factor_scores,
        penalties_applied=penalties,
        matched_rules=_matched_rules(factor_scores),
        risks=_risk_notes(raw, factor_scores),
        stub_factors=stubs,
        raw_data={
            "price":              raw.price,
            "mkt_cap":            raw.mkt_cap,
            "sector":             raw.sector,
            "industry":           raw.industry,
            "pe_ratio":           raw.pe_ratio,
            "debt_to_equity":     raw.debt_to_equity,
            "revenue_growth_yoy": raw.revenue_growth_yoy,
            "week52_high":        raw.week52_high,
            "week52_low":         raw.week52_low,
            "fetch_error":        raw.fetch_error,
        },
    )

    print(
        f"[PLAYBOOK_SCORING] {t!r} score={final_score} "
        f"hf_pass={hf_pass} stubs={len(stubs)}"
    )
    return result


async def score_tickers_batch(
    tickers: List[str],
    playbook: PlaybookDefinition,
    fmp_api_key: str,
    max_concurrent: int = 8,
) -> List[PlaybookScoreResult]:
    """Score multiple tickers concurrently. Returns sorted by final_score desc."""
    semaphore = asyncio.Semaphore(max_concurrent)

    async def _guarded(ticker: str) -> PlaybookScoreResult:
        async with semaphore:
            return await score_ticker(ticker, playbook, fmp_api_key)

    results = await asyncio.gather(
        *[_guarded(t) for t in tickers],
        return_exceptions=True,
    )

    scored: List[PlaybookScoreResult] = []
    for i, res in enumerate(results):
        if isinstance(res, Exception):
            print(f"[PLAYBOOK_SCORING] Batch error for {tickers[i]}: {res}")
            scored.append(PlaybookScoreResult(
                ticker=tickers[i].upper(),
                playbook_id=playbook.id,
                final_score=0.0,
                hard_filter_pass=False,
                hard_filter_failures=[f"Scoring error: {res}"],
                summary_label="Scoring error",
                factor_scores={},
                penalties_applied={},
                matched_rules=[],
                risks=[],
                stub_factors=list(_ALL_FACTORS),
                raw_data={"fetch_error": str(res)},
            ))
        else:
            scored.append(res)

    scored.sort(key=lambda r: r.final_score, reverse=True)
    return scored
