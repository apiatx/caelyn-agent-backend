"""
Main sector rotation service — orchestrates providers, analytics, and AI analysis.
"""
from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime, date
from typing import Optional

from data.cache import cache
from services.sector_rotation.analytics import (
    build_sector_snapshots,
    derive_regime,
)
from services.sector_rotation.gemini_analysis import (
    get_or_generate_analysis,
    load_cached_analysis,
)
from services.sector_rotation.providers import (
    fetch_etf_quotes,
    fetch_all_histories,
    fetch_etf_history,
)
from services.sector_rotation.schemas import (
    SectorRotationDashboard,
    SectorSnapshot,
    RegimeSummary,
    AIAnalysis,
)

_DASHBOARD_TTL  = 300
_ANALYSIS_TTL   = 7 * 24 * 3600
_DASHBOARD_KEY  = "sr:dashboard:v1"
_GENERATING_KEY = "sr:generating"


def _fetch_macro_overlay() -> dict:
    """
    Pull key macro signals for the regime layer.
    Priority order:
    1. Reuse existing fred:quick_macro or macro:dashboard:v3 caches (already fetched by macro precompute loop)
    2. Fall back to direct FRED calls only if nothing cached
    Returns a compact dict with numeric values.
    """
    macro: dict = {}

    # ── 1. Try to reuse the already-cached quick_macro from the macro precompute loop ──
    try:
        quick = cache.get("fred:quick_macro")
        if quick and isinstance(quick, dict):
            fed_data = quick.get("fed_funds_rate", {})
            cpi_data = quick.get("inflation_cpi", {})
            if isinstance(fed_data, dict) and "current_rate" in fed_data:
                macro["fed_rate"] = fed_data["current_rate"]
            if isinstance(cpi_data, dict) and "yoy_inflation_pct" in cpi_data:
                macro["cpi_yoy"] = cpi_data["yoy_inflation_pct"]
            # yield curve from fred:quick_macro
            yc = quick.get("yield_curve_10y_2y", {})
            if isinstance(yc, dict):
                if "current_spread" in yc:
                    spread = yc["current_spread"]
                    # Try to extract 10Y and 2Y from the spread data
                    macro["yield_curve_spread"] = round(spread, 3)
    except Exception:
        pass

    # ── 2. Try macro:dashboard:v3 for yields if still missing ──
    if "yield_10y" not in macro:
        try:
            dash = cache.get("macro:dashboard:v3")
            if dash and isinstance(dash, dict):
                rates = dash.get("rates", {})
                if isinstance(rates, dict):
                    y10 = rates.get("10y_treasury") or rates.get("yield_10y")
                    y2  = rates.get("2y_treasury")  or rates.get("yield_2y")
                    if y10 is not None:
                        macro["yield_10y"] = round(float(y10), 2)
                    if y2 is not None:
                        macro["yield_2y"] = round(float(y2), 2)
                    if y10 is not None and y2 is not None:
                        macro["yield_curve_spread"] = round(float(y10) - float(y2), 3)
        except Exception:
            pass

    # ── 3. Fall back to direct FRED calls only if still incomplete ──
    if not macro.get("fed_rate") or not macro.get("cpi_yoy"):
        try:
            fred_key = os.getenv("FRED_API_KEY", "")
            if fred_key:
                from data.fred_provider import FredProvider
                fred = FredProvider(api_key=fred_key)

                if not macro.get("fed_rate"):
                    rates_data = fred.get_fed_funds_rate()
                    if "current_rate" in rates_data:
                        macro["fed_rate"] = rates_data["current_rate"]

                if not macro.get("cpi_yoy"):
                    cpi_data = fred.get_inflation_cpi()
                    if "yoy_inflation_pct" in cpi_data:
                        macro["cpi_yoy"] = cpi_data["yoy_inflation_pct"]
        except Exception as e:
            print(f"[SR] FRED fallback error: {e}")

    return macro


async def _enrich_macro_with_treasuries(macro: dict) -> dict:
    """Add treasury yield data — reuses existing FRED cache, falls back to direct fetch."""
    # Skip if already populated from cache in _fetch_macro_overlay
    if macro.get("yield_10y") and macro.get("yield_2y"):
        return macro

    try:
        fred_key = os.getenv("FRED_API_KEY", "")
        if not fred_key:
            return macro
        from data.fred_provider import FredProvider
        fred = FredProvider(api_key=fred_key)
        loop = asyncio.get_event_loop()
        y10_data, y2_data = await asyncio.gather(
            loop.run_in_executor(None, fred.get_ten_year_yield),
            loop.run_in_executor(None, fred.get_two_year_yield),
        )
        y10 = y10_data.get("current_yield") if isinstance(y10_data, dict) else None
        y2  = y2_data.get("current_yield")  if isinstance(y2_data, dict) else None
        if y10 is not None:
            macro["yield_10y"] = y10
        if y2 is not None:
            macro["yield_2y"] = y2
        if y10 is not None and y2 is not None:
            macro["yield_curve_spread"] = round(y10 - y2, 3)
    except Exception as e:
        print(f"[SR] Treasury yield error: {e}")
    return macro


async def get_dashboard(
    include_analysis: bool = True,
    force_analysis: bool = False,
) -> SectorRotationDashboard:
    """
    Full sector rotation dashboard.
    Market data cached 5 min; AI analysis cached 7 days.
    """
    cached = cache.get(_DASHBOARD_KEY)
    if cached and not force_analysis:
        return SectorRotationDashboard(**cached)

    quotes_task   = asyncio.create_task(fetch_etf_quotes())
    histories_task = asyncio.create_task(fetch_all_histories())

    macro = _fetch_macro_overlay()
    macro = await _enrich_macro_with_treasuries(macro)

    quotes, histories = await asyncio.gather(quotes_task, histories_task)

    spy_hist    = histories.get("SPY", [])
    from services.sector_rotation.analytics import _pct_change
    spy_30d = _pct_change(spy_hist, 22)
    if spy_30d is not None:
        macro["spy_change_30d"] = round(spy_30d, 2)

    snapshots = build_sector_snapshots(quotes, histories)
    regime    = derive_regime(snapshots, macro)

    analysis: Optional[AIAnalysis] = None
    if include_analysis:
        analysis = await _maybe_generate_analysis(snapshots, regime, macro, force_analysis)

    leaders  = [s for s in snapshots if s.regime_tag == "Leading"][:3]
    laggards = [s for s in sorted(snapshots, key=lambda x: x.rotation_score or 0)][:3]

    dashboard = SectorRotationDashboard(
        updated_at=datetime.utcnow().isoformat() + "Z",
        analysis_updated_at=analysis.generated_at if analysis else None,
        regime=regime,
        leaders=leaders,
        laggards=laggards,
        sectors=snapshots,
        analysis=analysis,
    )

    cache.set(_DASHBOARD_KEY, dashboard.model_dump(), _DASHBOARD_TTL)
    return dashboard


async def _maybe_generate_analysis(
    snapshots: list[SectorSnapshot],
    regime: RegimeSummary,
    macro: dict,
    force: bool = False,
) -> Optional[AIAnalysis]:
    """
    Return cached weekly AI analysis or trigger a new one.
    Never blocks the dashboard — returns stale cache if generation is running.
    """
    if cache.get(_GENERATING_KEY) and not force:
        print("[SR] Analysis generation in progress — returning stale cache")
        return load_cached_analysis()

    cache.set(_GENERATING_KEY, True, 120)
    try:
        analysis = await get_or_generate_analysis(snapshots, regime, macro, force=force)
        return analysis
    finally:
        cache.set(_GENERATING_KEY, False, 1)


async def get_analysis_only(force: bool = False) -> Optional[AIAnalysis]:
    """
    Return (or regenerate) just the AI analysis without recomputing market data.
    If market data is available in cache, pass it along; otherwise quick-fetch.
    """
    if not force:
        stale = load_cached_analysis()
        if stale:
            from services.sector_rotation.gemini_analysis import _load_disk_cache
            if _load_disk_cache():
                return stale

    quotes_task    = asyncio.create_task(fetch_etf_quotes())
    histories_task = asyncio.create_task(fetch_all_histories())
    macro = _fetch_macro_overlay()
    macro = await _enrich_macro_with_treasuries(macro)
    quotes, histories = await asyncio.gather(quotes_task, histories_task)

    from services.sector_rotation.analytics import _pct_change
    spy_hist = histories.get("SPY", [])
    spy_30d = _pct_change(spy_hist, 22)
    if spy_30d is not None:
        macro["spy_change_30d"] = round(spy_30d, 2)

    snapshots = build_sector_snapshots(quotes, histories)
    regime    = derive_regime(snapshots, macro)
    return await get_or_generate_analysis(snapshots, regime, macro, force=force)
