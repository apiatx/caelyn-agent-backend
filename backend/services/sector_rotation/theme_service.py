"""
Theme ETF data service — fetches quotes + history for the theme universe,
computes performance and relative-strength scores, and caches aggressively.

No AI calls are triggered here.  AI context injection happens in gemini_analysis.py.

Caching strategy:
  - Market hours  (Mon-Fri 09:30-16:00 ET): 15-minute TTL
  - Off-hours / weekends:                   60-minute TTL
  History is always cached 1 hour (daily bars don't change intraday).
"""
from __future__ import annotations

import asyncio
import statistics
from datetime import datetime, date, timezone
from typing import Optional

from data.cache import cache
from services.sector_rotation.theme_universe import THEME_ETF_UNIVERSE, ALL_THEME_SYMBOLS
from services.sector_rotation.analytics import _pct_change, _ytd_change, _sma
from services.sector_rotation.providers import (
    fetch_etf_history,
    _tradier_quotes_batch,
    _finnhub_quote_single,
)
from services.sector_rotation.schemas import ThemeSnapshot

import httpx

_HIST_TTL   = 3600     # 1h — daily bars don't change intraday
_THEME_KEY  = "sr:theme_data:v2"


def _market_hours_ttl() -> int:
    """Return 900 (15min) during market hours, 3600 (60min) otherwise."""
    now = datetime.now(tz=timezone.utc)
    wd  = now.weekday()            # 0=Mon, 6=Sun
    if wd >= 5:
        return 3600                # weekend
    # ET = UTC-4 during EDT (Apr-Oct), UTC-5 during EST (Nov-Mar)
    # Approximate: treat as UTC-4 (EDT) Apr-Oct, UTC-5 otherwise
    month = now.month
    utc_offset = 4 if 4 <= month <= 10 else 5
    et_hour = (now.hour - utc_offset) % 24
    et_minute = now.minute
    et_minutes = et_hour * 60 + et_minute
    # Market hours 09:30 - 16:00 ET
    if 570 <= et_minutes <= 960:
        return 900                 # 15 min during market hours
    return 3600                    # 60 min outside market hours


async def _fetch_theme_quotes(symbols: list[str]) -> dict[str, dict]:
    """
    Batch-fetch real-time quotes for all theme ETF symbols.
    Tradier batch → Finnhub individual fallback (same pattern as sector providers).
    """
    result: dict[str, dict] = {}

    tradier_data = await _tradier_quotes_batch(symbols)
    if tradier_data:
        result.update(tradier_data)

    missing = [t for t in symbols if t not in result or not result[t].get("price")]
    if missing:
        async with httpx.AsyncClient() as session:
            fallbacks = await asyncio.gather(
                *[_finnhub_quote_single(t, session) for t in missing],
                return_exceptions=True,
            )
        for item in fallbacks:
            if not isinstance(item, Exception):
                t, q = item
                if q:
                    result[t] = q

    return {t: q for t, q in result.items() if q}


async def _fetch_all_theme_histories() -> dict[str, list[dict]]:
    """Fetch ~1Y of daily closes for every unique theme ETF symbol."""
    results = await asyncio.gather(
        *[fetch_etf_history(t, days=400) for t in ALL_THEME_SYMBOLS],
        return_exceptions=True,
    )
    out: dict[str, list[dict]] = {}
    for t, r in zip(ALL_THEME_SYMBOLS, results):
        if isinstance(r, list):
            out[t] = r
        else:
            print(f"[THEME] History error {t}: {r}")
            out[t] = []
    return out


def _median(values: list[float]) -> Optional[float]:
    valid = [v for v in values if v is not None]
    if not valid:
        return None
    return statistics.median(valid)


def _compute_theme_rs_score(
    perf_5d:  Optional[float],
    perf_22d: Optional[float],
    perf_65d: Optional[float],
    pct_from_50d:  Optional[float],
    change_20d_accel: Optional[float],
) -> Optional[float]:
    """
    Relative-strength score (0–100) for a single theme.
    Weights (from spec):
      35% = 1M performance (percentile among themes)   — computed by caller
      25% = 3M performance (percentile)                — computed by caller
      20% = 5D performance (percentile)                — computed by caller
      10% = distance above/below 50D MA (normalised)
      10% = 20D trend acceleration (normalised)
    
    This function receives pre-computed percentile values (0-1) for the first
    three components and raw values for the last two.  Caller computes
    percentiles across the full theme universe.
    """
    raise NotImplementedError("Use _score_themes() instead — it needs cross-theme percentiles.")


def _pct_rank(value: Optional[float], universe: list[Optional[float]]) -> float:
    valid = [v for v in universe if v is not None]
    if value is None or not valid:
        return 0.5
    below = sum(1 for v in valid if v < value)
    return below / len(valid)


def _score_themes(theme_rows: list[dict]) -> list[dict]:
    """
    Compute relative_strength_score (0-100) and derived fields for each theme.
    Mutates each dict in place; returns the same list for convenience.
    """
    perf_5d_all  = [r.get("perf_5d")  for r in theme_rows]
    perf_22d_all = [r.get("perf_1m")  for r in theme_rows]
    perf_65d_all = [r.get("perf_3m")  for r in theme_rows]

    for row in theme_rows:
        p5   = _pct_rank(row.get("perf_5d"),  perf_5d_all)
        p22  = _pct_rank(row.get("perf_1m"),  perf_22d_all)
        p65  = _pct_rank(row.get("perf_3m"),  perf_65d_all)

        ma50_norm = 0.5
        pct50 = row.get("pct_from_50d")
        if pct50 is not None:
            capped = max(-20.0, min(20.0, pct50))
            ma50_norm = (capped + 20.0) / 40.0

        accel_norm = 0.5
        accel = row.get("trend_accel_20d")
        if accel is not None:
            capped = max(-5.0, min(5.0, accel))
            accel_norm = (capped + 5.0) / 10.0

        rs = round(
            p22  * 35.0 +
            p65  * 25.0 +
            p5   * 20.0 +
            ma50_norm  * 10.0 +
            accel_norm * 10.0,
            1,
        )
        row["relative_strength_score"] = rs

    # Rank by RS score
    sorted_rows = sorted(theme_rows, key=lambda r: r.get("relative_strength_score", 0), reverse=True)
    for rank, row in enumerate(sorted_rows, start=1):
        row["momentum_rank"] = rank

    # Assign trend_state
    for row in theme_rows:
        rs = row.get("relative_strength_score", 50)
        if rs >= 75:
            row["trend_state"] = "Leadership"
        elif rs >= 58:
            row["trend_state"] = "Improving"
        elif rs >= 42:
            row["trend_state"] = "Neutral"
        elif rs >= 25:
            row["trend_state"] = "Weakening"
        else:
            row["trend_state"] = "Lagging"

        # rotation_state based on 20D accel
        accel = row.get("trend_accel_20d")
        if accel is None:
            row["rotation_state"] = "Stabilizing"
        elif accel > 1.5:
            row["rotation_state"] = "Accelerating"
        elif accel > 0:
            row["rotation_state"] = "Stabilizing"
        elif accel > -1.5:
            row["rotation_state"] = "Fading"
        else:
            row["rotation_state"] = "Reversing"

    return theme_rows


def _build_theme_row(
    theme_id: str,
    meta: dict,
    quotes: dict[str, dict],
    histories: dict[str, list[dict]],
) -> Optional[dict]:
    """Build one theme row from raw quote/history data."""
    symbols = meta["symbols"]

    # Collect valid performance readings for each timeframe from all symbols
    perfs: dict[str, list[float]] = {"1d": [], "5d": [], "1m": [], "3m": [], "6m": [], "ytd": [], "1y": []}
    prices: list[float] = []
    pct50s: list[float] = []
    accels: list[float] = []
    leader_sym: Optional[str] = None
    leader_price: Optional[float] = None
    best_data_count = -1

    for sym in symbols:
        hist = histories.get(sym, [])
        q    = quotes.get(sym, {})

        price = (
            q.get("price")
            or q.get("last")
            or q.get("close")
            or (round(float(hist[-1]["close"]), 2) if hist else None)
        )
        if price:
            prices.append(price)

        # Count how many bars we have — use the "richest" symbol as leader
        data_count = len(hist)
        if data_count > best_data_count:
            best_data_count = data_count
            leader_sym = sym
            leader_price = price

        # 1D from quote
        c1d = q.get("change_1d_pct")
        if c1d is None and len(hist) >= 2:
            prev = float(hist[-2]["close"])
            last = float(hist[-1]["close"])
            c1d = (last - prev) / prev * 100 if prev else None
        if c1d is not None:
            perfs["1d"].append(c1d)

        def _add(key: str, val: Optional[float]):
            if val is not None:
                perfs[key].append(val)

        _add("5d",  _pct_change(hist, 5))
        _add("1m",  _pct_change(hist, 22))
        _add("3m",  _pct_change(hist, 65))
        _add("6m",  _pct_change(hist, 130))
        _add("ytd", _ytd_change(hist))
        _add("1y",  _pct_change(hist, 252))

        # MA distance (50D)
        ma50 = _sma(hist, 50)
        if price and ma50:
            pct50s.append((price - ma50) / ma50 * 100)

        # 20D trend acceleration: (10D perf) - (prior 10D perf)  ≈ second derivative
        if len(hist) >= 21:
            recent = _pct_change(hist, 10)
            prior  = _pct_change(hist[-11:], 10)  # 10 bars from 11 bars ago
            if recent is not None and prior is not None:
                accels.append(recent - prior)

    if not any(perfs.values()) and not prices:
        return None  # no data at all — skip this theme

    def _med(key: str) -> Optional[float]:
        vals = perfs[key]
        return round(statistics.median(vals), 2) if vals else None

    pct_from_50d  = round(statistics.median(pct50s), 2) if pct50s else None
    trend_accel   = round(statistics.median(accels), 2) if accels else None

    return {
        "id":           theme_id,
        "label":        meta["label"],
        "parent_sector": meta["parent_sector"],
        "theme_type":   meta["theme_type"],
        "symbols":       symbols,
        "leader_symbol": leader_sym,
        "ticker":        leader_sym,
        "price":         round(leader_price, 2) if leader_price else None,
        "leader_price":  round(leader_price, 2) if leader_price else None,
        "current_price": round(leader_price, 2) if leader_price else None,
        "performance": {
            "1d":  _med("1d"),
            "5d":  _med("5d"),
            "1m":  _med("1m"),
            "3m":  _med("3m"),
            "6m":  _med("6m"),
            "ytd": _med("ytd"),
            "1y":  _med("1y"),
        },
        # convenience aliases used by scoring
        "perf_5d":  _med("5d"),
        "perf_1m":  _med("1m"),
        "perf_3m":  _med("3m"),
        "pct_from_50d": pct_from_50d,
        "trend_accel_20d": trend_accel,
        # filled in later by _score_themes
        "relative_strength_score": None,
        "momentum_rank":  None,
        "trend_state":    None,
        "rotation_state": None,
    }


async def get_theme_data(force: bool = False) -> list[dict]:
    """
    Return scored ThemeSnapshot dicts for all themes.
    Cached at 15-min (market hours) or 60-min (off-hours).
    No AI calls.
    """
    ttl = _market_hours_ttl()
    cached = cache.get(_THEME_KEY)
    if cached and not force:
        return cached

    # Fetch quotes + histories for all theme ETFs in parallel
    quotes, histories = await asyncio.gather(
        _fetch_theme_quotes(ALL_THEME_SYMBOLS),
        _fetch_all_theme_histories(),
    )

    rows: list[dict] = []
    for theme_id, meta in THEME_ETF_UNIVERSE.items():
        row = _build_theme_row(theme_id, meta, quotes, histories)
        if row:
            rows.append(row)
        else:
            print(f"[THEME] No data for theme '{theme_id}' — skipping")

    rows = _score_themes(rows)

    # Strip internal-only keys before caching
    clean = []
    for r in rows:
        c = {k: v for k, v in r.items() if k not in ("perf_5d", "perf_1m", "perf_3m")}
        clean.append(c)

    cache.set(_THEME_KEY, clean, ttl)
    return clean


def build_theme_context_for_prompt(theme_rows: list[dict], top_n: int = 6) -> str:
    """
    Build a compact text block describing theme leaders and laggers.
    Injected into the Gemini prompt as a Theme Rotation context block.
    """
    if not theme_rows:
        return "No theme ETF data available."

    sorted_rows = sorted(theme_rows, key=lambda r: r.get("relative_strength_score") or 0, reverse=True)
    leaders  = sorted_rows[:top_n]
    laggers  = sorted_rows[-3:]

    lines = ["THEME ETF ROTATION CONTEXT (granular subsector leaders/laggers):"]
    lines.append("\nTop Themes (highest relative strength):")
    for t in leaders:
        p = t.get("performance", {})
        rs = t.get("relative_strength_score", "?")
        p1m  = p.get("1m")
        p3m  = p.get("3m")
        state = t.get("trend_state", "")
        rotation = t.get("rotation_state", "")
        symbols = ", ".join(t.get("symbols", [])[:2])
        p1m_str  = f"{p1m:+.1f}%" if p1m is not None else "N/A"
        p3m_str  = f"{p3m:+.1f}%" if p3m is not None else "N/A"
        lines.append(
            f"  {t['label']} [{symbols}] — RS={rs} | 1M={p1m_str} 3M={p3m_str} "
            f"| {state} / {rotation} | {t.get('theme_type','')} | Parent: {t.get('parent_sector','')}"
        )

    lines.append("\nWeakest Themes (lowest relative strength):")
    for t in laggers:
        p = t.get("performance", {})
        rs = t.get("relative_strength_score", "?")
        p1m = p.get("1m")
        p1m_str = f"{p1m:+.1f}%" if p1m is not None else "N/A"
        symbols = ", ".join(t.get("symbols", [])[:2])
        lines.append(
            f"  {t['label']} [{symbols}] — RS={rs} | 1M={p1m_str} | {t.get('trend_state','')} "
            f"| Parent: {t.get('parent_sector','')}"
        )

    lines.append(
        "\nParent-sector / theme mapping:\n" +
        "\n".join(
            f"  {r['label']} → {r.get('parent_sector','')}"
            for r in sorted_rows
        )
    )
    return "\n".join(lines)
