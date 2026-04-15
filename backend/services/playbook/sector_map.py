"""
Sector strength scoring using existing sector rotation infrastructure.

Reuses (read-only):
  services/sector_rotation/providers.fetch_etf_history()  — yfinance + 1h cache
  services/sector_rotation/schemas.SECTOR_ETF_MAP          — canonical ETF labels

No new dependencies added.
"""
from __future__ import annotations

import asyncio
from typing import Dict, List, Optional

# ── Sector → ETF mapping ─────────────────────────────────────────────────────
# Canonical names come from FMP profile "sector" field.
# Aliases handle variant spellings seen in the wild.

SECTOR_TO_ETF: Dict[str, str] = {
    "Technology":                "XLK",
    "Information Technology":    "XLK",
    "Communication Services":    "XLC",
    "Consumer Cyclical":         "XLY",
    "Consumer Discretionary":    "XLY",
    "Consumer Defensive":        "XLP",
    "Consumer Staples":          "XLP",
    "Energy":                    "XLE",
    "Financial Services":        "XLF",
    "Financials":                "XLF",
    "Health Care":               "XLV",
    "Healthcare":                "XLV",
    "Industrials":               "XLI",
    "Basic Materials":           "XLB",
    "Materials":                 "XLB",
    "Real Estate":               "XLRE",
    "Utilities":                 "XLU",
}

# Trading-day window sizes for multi-window return calculation
_W1  = 5    # ~1 week
_W1M = 22   # ~1 month
_W3M = 65   # ~3 months

# Weight per window (short/medium/long)
_WINDOW_WEIGHTS = {_W1: 0.25, _W1M: 0.40, _W3M: 0.35}


def _pct_change(series: List[Dict], n_bars: int) -> Optional[float]:
    """Percentage change over the last n_bars trading days. Returns None if insufficient data."""
    if not series or len(series) <= n_bars:
        return None
    current = series[-1].get("close")
    past    = series[-(n_bars + 1)].get("close")
    if not current or not past or past == 0:
        return None
    return round((current - past) / past * 100, 2)


def _pct_to_score(pct: float) -> float:
    """Map -20%..+20% percent change to 0..100 score. Linear, capped at extremes."""
    capped = max(-20.0, min(20.0, pct))
    return (capped + 20.0) / 40.0 * 100.0


def score_sector_strength_from_history(
    sector: Optional[str],
    industry: Optional[str],
    history: Dict[str, List[Dict]],
) -> "FactorDetail":
    """
    Compute sector_strength from pre-fetched ETF history.

    Args:
        sector:   FMP "sector" string (e.g. "Technology")
        industry: FMP "industry" string (fallback for sector→ETF lookup)
        history:  {etf_ticker: [{date, close}, ...]}  — from fetch_sector_etf_history()

    Returns:
        FactorDetail with score 0-100, status, reasons, source_tags.
    """
    from services.playbook.playbook_types import FactorDetail

    etf = SECTOR_TO_ETF.get(sector or "") or SECTOR_TO_ETF.get(industry or "")

    if not etf or etf not in history:
        return FactorDetail(
            score=50.0,
            status="fallback",
            reasons=["Sector ETF mapping unavailable"],
            source_tags=["sector_etf_momentum"],
        )

    bars = history.get(etf, [])
    if not bars:
        return FactorDetail(
            score=50.0,
            status="fallback",
            reasons=[f"No price history for {etf}"],
            source_tags=["sector_etf_momentum", etf],
        )

    changes: Dict[int, Optional[float]] = {
        _W1:  _pct_change(bars, _W1),
        _W1M: _pct_change(bars, _W1M),
        _W3M: _pct_change(bars, _W3M),
    }

    valid = [(w, pct) for w, pct in [(w, changes[w]) for w in (_W1, _W1M, _W3M)] if pct is not None]

    if not valid:
        return FactorDetail(
            score=50.0,
            status="fallback",
            reasons=["Insufficient ETF history for multi-window calculation"],
            source_tags=["sector_etf_momentum", etf],
        )

    total_weight = sum(_WINDOW_WEIGHTS[w] for w, _ in valid)
    weighted_score = sum(_WINDOW_WEIGHTS[w] * _pct_to_score(pct) for w, pct in valid) / total_weight

    reasons: List[str] = []
    labels = {_W1: "1W", _W1M: "1M", _W3M: "3M"}
    for window, label in labels.items():
        pct = changes.get(window)
        if pct is not None:
            sign = "+" if pct >= 0 else ""
            reasons.append(f"{etf} {label}: {sign}{pct:.1f}%")

    # Characterize strength
    positive_windows = sum(1 for _, pct in valid if pct > 0)
    if positive_windows == len(valid) and weighted_score >= 70:
        reasons.append("All windows positive — hot sector")
    elif positive_windows == 0:
        reasons.append("All windows negative — cold sector")

    return FactorDetail(
        score=round(weighted_score, 1),
        status="real",
        reasons=reasons,
        source_tags=["sector_etf_momentum", etf],
    )


async def fetch_sector_etf_history(
    sector: Optional[str],
    industry: Optional[str],
) -> Dict[str, List[Dict]]:
    """
    Fetch price history for the sector's representative ETF.
    Uses the existing sector rotation yfinance + cache layer.
    Returns {etf_ticker: [{"date": "...", "close": ...}]}.
    """
    etf = SECTOR_TO_ETF.get(sector or "") or SECTOR_TO_ETF.get(industry or "")
    if not etf:
        return {}
    try:
        from services.sector_rotation.providers import fetch_etf_history
        bars = await fetch_etf_history(etf, days=90)
        return {etf: bars} if bars else {}
    except Exception as e:
        print(f"[SECTOR_MAP] fetch_sector_etf_history error for {etf}: {e}")
        return {}
