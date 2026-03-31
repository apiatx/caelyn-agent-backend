"""
Rotation scoring, regime tagging, and leaders/laggards logic.
"""
from __future__ import annotations
import statistics
from datetime import datetime, date
from typing import Optional

from services.sector_rotation.schemas import (
    SectorSnapshot,
    ETFSeries,
    RegimeSummary,
    SECTOR_ETF_MAP,
    CYCLICALS,
    DEFENSIVES,
)


def _pct_change(series: list[dict], n_bars: int) -> Optional[float]:
    """Return % change between series[-n_bars-1].close and series[-1].close."""
    if len(series) < n_bars + 1:
        return None
    try:
        base  = float(series[-(n_bars + 1)]["close"])
        last  = float(series[-1]["close"])
        return (last - base) / base * 100 if base != 0 else None
    except Exception:
        return None


def _ytd_change(series: list[dict]) -> Optional[float]:
    """Return YTD % change using the last close of prior calendar year."""
    if not series:
        return None
    year = date.today().year
    year_start = f"{year}-01-01"
    prev_year_closes = [r for r in series if r["date"] < year_start]
    if not prev_year_closes:
        return _pct_change(series, min(60, len(series) - 1))
    base = float(prev_year_closes[-1]["close"])
    last = float(series[-1]["close"])
    return (last - base) / base * 100 if base != 0 else None


def _sma(series: list[dict], n: int) -> Optional[float]:
    if len(series) < n:
        return None
    closes = [float(r["close"]) for r in series[-n:]]
    return statistics.mean(closes)


def _compact_series(series: list[dict], n: int) -> ETFSeries:
    sliced = series[-n:] if len(series) >= n else series
    return ETFSeries(
        dates=[r["date"] for r in sliced],
        prices=[round(float(r["close"]), 4) for r in sliced],
    )


def compute_rotation_score(
    change_30d: Optional[float],
    change_ytd: Optional[float],
    pct_from_50d: Optional[float],
    pct_from_200d: Optional[float],
    relative_vs_spy_30d: Optional[float],
    all_30d: list[Optional[float]],
    all_ytd: list[Optional[float]],
) -> Optional[float]:
    """
    Weighted rotation score (0–100):
      25% = 1M return rank
      25% = YTD return rank
      20% = pct above 50D MA (capped ±15%)
      15% = pct above 200D MA (capped ±20%)
      15% = relative perf vs SPY 30D
    """
    def rank_pct(value: Optional[float], universe: list[Optional[float]]) -> float:
        valid = [v for v in universe if v is not None]
        if value is None or not valid:
            return 0.5
        below = sum(1 for v in valid if v < value)
        return below / len(valid)

    score = 0.0

    r_30d = rank_pct(change_30d, all_30d)
    r_ytd = rank_pct(change_ytd, all_ytd)

    ma50_norm = 0.5
    if pct_from_50d is not None:
        capped = max(-15, min(15, pct_from_50d))
        ma50_norm = (capped + 15) / 30

    ma200_norm = 0.5
    if pct_from_200d is not None:
        capped = max(-20, min(20, pct_from_200d))
        ma200_norm = (capped + 20) / 40

    rel_norm = 0.5
    if relative_vs_spy_30d is not None:
        capped = max(-15, min(15, relative_vs_spy_30d))
        rel_norm = (capped + 15) / 30

    score = (
        r_30d    * 25 +
        r_ytd    * 25 +
        ma50_norm  * 20 +
        ma200_norm * 15 +
        rel_norm   * 15
    )
    return round(score, 1)


def regime_tag(rotation_score: Optional[float]) -> str:
    if rotation_score is None:
        return "Unknown"
    if rotation_score >= 70:
        return "Leading"
    if rotation_score >= 50:
        return "Improving"
    if rotation_score >= 30:
        return "Weakening"
    return "Lagging"


def build_sector_snapshots(
    quotes: dict[str, dict],
    histories: dict[str, list[dict]],
) -> list[SectorSnapshot]:
    """
    Build SectorSnapshot list for all 11 sector ETFs.
    SPY / QQQ histories are used for benchmarks but not returned as sectors.
    """
    spy_hist = histories.get("SPY", [])
    spy_30d  = _pct_change(spy_hist, 22)

    all_30d: list[Optional[float]] = []
    all_ytd: list[Optional[float]] = []
    raw: dict[str, dict] = {}

    for ticker in list(SECTOR_ETF_MAP.keys()):
        h = histories.get(ticker, [])
        q = quotes.get(ticker, {})
        c30 = _pct_change(h, 22)
        cytd = _ytd_change(h)
        all_30d.append(c30)
        all_ytd.append(cytd)
        raw[ticker] = {
            "hist": h, "quote": q,
            "change_30d": c30, "change_ytd": cytd,
        }

    snapshots: list[SectorSnapshot] = []
    for i, ticker in enumerate(SECTOR_ETF_MAP.keys()):
        info = raw[ticker]
        h    = info["hist"]
        q    = info["quote"]
        price = q.get("price") or (float(h[-1]["close"]) if h else None)

        c1d  = q.get("change_1d_pct")
        if c1d is None and len(h) >= 2:
            prev = float(h[-2]["close"])
            last = float(h[-1]["close"])
            c1d = (last - prev) / prev * 100 if prev else None

        c7d  = _pct_change(h, 5)
        c30d = info["change_30d"]
        cytd = info["change_ytd"]
        c1y  = _pct_change(h, 252)

        ma50  = _sma(h, 50)
        ma200 = _sma(h, 200)
        pct50  = ((price - ma50)  / ma50  * 100) if price and ma50  else None
        pct200 = ((price - ma200) / ma200 * 100) if price and ma200 else None

        rel_spy_30d = (
            (c30d - spy_30d)
            if c30d is not None and spy_30d is not None
            else None
        )

        rot = compute_rotation_score(
            c30d, cytd, pct50, pct200, rel_spy_30d,
            all_30d, all_ytd,
        )

        series: dict[str, ETFSeries] = {
            "1d":  _compact_series(h, 1),
            "7d":  _compact_series(h, 5),
            "30d": _compact_series(h, 22),
            "ytd": _compact_series(h, 65),
            "1y":  _compact_series(h, 252),
        }

        snapshots.append(SectorSnapshot(
            ticker=ticker,
            name=SECTOR_ETF_MAP[ticker],
            price=round(price, 2) if price else None,
            change_1d=round(c1d, 2) if c1d is not None else None,
            change_7d=round(c7d, 2) if c7d is not None else None,
            change_30d=round(c30d, 2) if c30d is not None else None,
            change_ytd=round(cytd, 2) if cytd is not None else None,
            change_1y=round(c1y, 2) if c1y is not None else None,
            ma_50d=round(ma50, 2) if ma50 else None,
            ma_200d=round(ma200, 2) if ma200 else None,
            pct_from_50d=round(pct50, 2) if pct50 is not None else None,
            pct_from_200d=round(pct200, 2) if pct200 is not None else None,
            rotation_score=rot,
            relative_strength_rank=None,
            regime_tag=regime_tag(rot),
            is_cyclical=ticker in CYCLICALS,
            series=series,
        ))

    snapshots.sort(key=lambda s: (s.rotation_score or 0), reverse=True)
    for rank, s in enumerate(snapshots, start=1):
        s.relative_strength_rank = rank

    return snapshots


def derive_regime(
    snapshots: list[SectorSnapshot],
    macro_overlay: dict,
) -> RegimeSummary:
    """Derive a simple market regime from sector dynamics + macro signals."""
    cyc_changes = [
        s.change_30d for s in snapshots
        if s.ticker in CYCLICALS and s.change_30d is not None
    ]
    def_changes = [
        s.change_30d for s in snapshots
        if s.ticker in DEFENSIVES and s.change_30d is not None
    ]

    cyc_avg = statistics.mean(cyc_changes) if cyc_changes else 0.0
    def_avg = statistics.mean(def_changes) if def_changes else 0.0
    cyc_vs_def = round(cyc_avg - def_avg, 2)

    spy_change = macro_overlay.get("spy_change_30d")
    above_spy = sum(
        1 for s in snapshots if s.change_30d is not None and spy_change is not None
        and s.change_30d > spy_change
    )
    breadth = round(above_spy / len(snapshots) * 100, 1) if snapshots else 0.0

    if cyc_vs_def > 2:
        posture = "Risk-On"
        style   = "Cyclicals"
    elif cyc_vs_def < -2:
        posture = "Risk-Off"
        style   = "Defensives"
    else:
        posture = "Neutral"
        style   = "Mixed"

    return RegimeSummary(
        market_posture=posture,
        cyclical_vs_defensive=cyc_vs_def,
        breadth_pct_above_spy=breadth,
        leadership_style=style,
        macro_overlay=macro_overlay,
    )
