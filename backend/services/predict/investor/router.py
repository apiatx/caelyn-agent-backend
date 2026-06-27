"""
Investor Mode Router — Additive Prophetik endpoints.

These endpoints are ADDITIVE — they do not modify or replace any existing
Gambler mode endpoints.

Endpoints:
  GET /api/predict/investor/overview    → Full investor signal payload
  GET /api/predict/investor/themes      → Theme clusters with equity impacts (drill-down)
  GET /api/predict/investor/regime      → Regime scoreboard only (lightweight)
  GET /api/predict/investor/watchlists  → Stock watchlists derived from PM signals
"""

from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from services.predict.investor.investor_intel import investor_intel

router = APIRouter(tags=["predict-investor"])


@router.get("/api/predict/investor")
async def investor_root():
    """
    Root investor endpoint — alias for /api/predict/investor/overview.
    Returns the full Investor mode payload (same shape as /overview).
    """
    try:
        result = await investor_intel.get_overview()
        return JSONResponse(content=result)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=502, content={"error": str(e)})


@router.get("/api/predict/investor/overview")
async def investor_overview():
    """
    Full Investor mode payload for the Prophetik Investor tab.

    Returns:
        generated_at              ISO timestamp
        equity_relevant_market_count  int
        total_market_count            int

        top_equity_signals        list[dict] — top 5 equity-relevant signals:
            theme_id, title, summary, why_it_matters,
            supporting_markets[], odds_move_summary, summary_direction,
            bullish_sectors[], bearish_sectors[],
            bullish_stocks[], bearish_stocks[], asset_baskets[],
            regime_impact[], confidence, narrative, watchlist_priority

        sector_rotation           dict:
            strongest_positive_sectors[], strongest_negative_sectors[],
            emerging_leadership[], fading_leadership[],
            regime_context_notes[]

        watchlists                dict:
            bullish_watchlist[], bearish_watchlist[], conditional_watchlist[],
            watchlist_notes[]

        regime_scoreboard         dict of 7 regime indicators:
            {label, score, direction, confidence, supporting_themes, description}

        theme_clusters            list[dict] — all theme clusters with raw data
    """
    try:
        result = await investor_intel.get_overview()
        return JSONResponse(content=result)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=502, content={"error": str(e)})


@router.get("/api/predict/investor/themes")
async def investor_themes():
    """
    Theme clusters with full equity impact data.

    Use for expandable theme cards / drill-down tables in the Investor tab.

    Each cluster includes:
        theme_id, theme_name, theme_emoji, description
        supporting_markets[]           top markets for this theme
        market_count
        weighted_odds_shift_24h/7d     volume-weighted price shift
        confidence_score               0-100
        consistency_score              how aligned markets are
        contradiction_score            how much they conflict
        freshness_score                recency of volume activity
        regime_signal_strength         0-100
        summary_direction              rising | falling | mixed | unstable
        bullish_sectors[], bearish_sectors[]
        bullish_stocks[], bearish_stocks[]
        asset_baskets[]
        regime_implications[]
        narrative                      grounded implication text
    """
    try:
        result = await investor_intel.get_themes()
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(status_code=502, content={"error": str(e)})


@router.get("/api/predict/investor/regime")
async def investor_regime():
    """
    Regime scoreboard — 7 macro regime indicators derived from Polymarket odds.

    Indicators:
        risk_on_vs_risk_off
        inflationary_vs_disinflationary
        growth_vs_slowdown
        geopolitical_stress_vs_easing
        higher_for_longer_vs_easing
        commodity_pressure_vs_relief
        ai_capex_supportive_vs_restrictive

    Each returns:
        label        the current regime state (e.g. "risk_on", "inflationary")
        score        0-100 (50 = neutral, >60 = first label, <40 = second label)
        direction    rising | falling | neutral
        confidence   high | medium | low
        supporting_themes   list of theme names that inform this indicator
        description  plain-English explanation of what this indicator measures
    """
    try:
        result = await investor_intel.get_regime()
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(status_code=502, content={"error": str(e)})


@router.get("/api/predict/investor/intelligence")
async def investor_intelligence():
    """
    New normalized Predict intelligence payload.

    Returns a unified, event-family-centric view of current Polymarket odds,
    with permanent tracked macro/market families and grouped equity signals.

    Shape:
        updated_at          ISO timestamp
        cache_age_seconds   seconds since last build (0 on fresh)
        diagnostics         dict — coverage stats, build time, ticker resolution info
        tracked_odds        list[dict] — 19 permanent macro/market families:
            family_key, label, category, description
            yes_probability     float (0-1) or null if not live on Polymarket
            delta_1h_pp, delta_24h_pp, delta_7d_pp   pp shifts or null
            volume_24h, liquidity, condition_id, market_question
            driver_markets      up to 5 matching markets sorted by volume
        equity_signals      list[dict] — event-family-grouped equity implications:
            event_family_key    canonical family key (e.g. "hormuz_iran")
            title               human-readable event title
            primary_category    e.g. "Geopolitics / Energy / Shipping"
            primary_theme_id    macro theme (e.g. "geopolitics_war_trade")
            yes_probability     volume-weighted aggregated YES probability
            delta_24h_pp, delta_7d_pp   aggregated shifts
            direction           rising | falling | mixed
            signal_quality      high | moderate | low
            why_it_matters      one-sentence equity implication
            driver_markets      top 6 raw markets grouped under this event
            theme_impacts       list of {sector, direction, rationale}
            ticker_impacts      {bullish_watchlist, bearish_watchlist,
                                 conditional_watchlist, bullish_fallback, bearish_fallback}
            conflicts           list of intra-family conflict notes
            market_count        int — number of markets in this family group
            total_volume_24h    float — sum of 24h volume for the family
        raw_markets         list — reserved (empty in this version)

    Cache:
        Pre-warmed every 30 min by _investor_intelligence_loop() in main.py.
        TTL = 35 min.  Stale-reads return cached data with cache_age_seconds > 0.

    Backward-compat:
        This endpoint is ADDITIVE.  All existing /overview, /themes, /regime,
        /watchlists endpoints are unchanged.
    """
    try:
        result = await investor_intel.get_intelligence()
        return JSONResponse(content=result)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=502, content={"error": str(e)})


@router.get("/api/predict/investor/watchlists")
async def investor_watchlists():
    """
    Stock watchlists derived from current Polymarket signal themes.

    Returns:
        bullish_watchlist[]     stocks with multiple bullish theme tailwinds
        bearish_watchlist[]     stocks facing multiple bearish theme headwinds
        conditional_watchlist[] stocks appearing in both — direction depends on theme resolution
        sector_reference        full curated sector → stock mapping (static reference)

    Each watchlist entry:
        ticker        stock symbol
        themes[]      theme names driving the signal
        sectors[]     sector categories
        type          bullish | bearish | conditional
    """
    try:
        result = await investor_intel.get_watchlists()
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(status_code=502, content={"error": str(e)})
