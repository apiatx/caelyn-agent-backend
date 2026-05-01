"""
FastAPI router for the Sector Rotation dashboard.

Endpoints:
  GET  /api/sector-rotation/dashboard          — full dashboard (market data + cached AI analysis)
  GET  /api/sector-rotation/analysis           — just the AI analysis
  GET  /api/sector-rotation/history            — compact price series for chart widgets
  POST /api/sector-rotation/refresh-analysis   — force-regenerate AI analysis (admin/internal)
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request, Depends
from fastapi.responses import JSONResponse

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from subscription import require_subscription

import httpx

from services.sector_rotation.providers import fetch_etf_quotes, fetch_all_histories
from services.sector_rotation.schemas import (
    SectorRotationDashboard,
    AIAnalysis,
    ETFSeries,
    ThemeSnapshot,
    SECTOR_ETF_MAP,
)
from services.sector_rotation.service import get_dashboard, get_analysis_only
from services.sector_rotation.analytics import _compact_series

router = APIRouter(prefix="/api/sector-rotation", tags=["sector-rotation"])


@router.get("/dashboard", response_model=SectorRotationDashboard)
async def dashboard_endpoint(
    include_analysis: bool = Query(True, description="Include AI analysis in response"),
):
    """
    Full sector rotation dashboard.
    Market data refreshes every 5 minutes.
    AI analysis is cached for 7 days then auto-regenerated.
    """
    try:
        data = await get_dashboard(include_analysis=include_analysis)
        return data
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Dashboard error: {e}")


@router.get("/analysis", response_model=Optional[AIAnalysis])
async def analysis_endpoint():
    """Return the cached weekly AI sector rotation analysis."""
    from services.sector_rotation.gemini_analysis import load_cached_analysis, _load_disk_cache
    cached = _load_disk_cache()
    if cached:
        try:
            return AIAnalysis(**{k: v for k, v in cached.items() if not k.startswith("_")})
        except Exception:
            pass
    return JSONResponse(content=None)


@router.get("/history")
async def history_endpoint(
    range: str = Query("30d", description="Range: 1d | 7d | 30d | ytd | 1y"),
    tickers: Optional[str] = Query(None, description="Comma-separated tickers; defaults to all sectors"),
):
    """
    Return compact price series for the requested range.
    Suitable for frontend sparkline / chart widgets.
    """
    _range_bars: dict[str, int] = {
        "1d": 1, "7d": 5, "30d": 22, "ytd": 65, "1y": 252,
    }
    n_bars = _range_bars.get(range, 22)

    target = (
        [t.strip().upper() for t in tickers.split(",") if t.strip()]
        if tickers
        else list(SECTOR_ETF_MAP.keys())
    )
    unknown = [t for t in target if t not in SECTOR_ETF_MAP and t not in ("SPY", "QQQ")]
    if unknown:
        raise HTTPException(status_code=400, detail=f"Unknown tickers: {unknown}")

    histories = await fetch_all_histories()
    result: dict[str, ETFSeries] = {}
    for t in target:
        h = histories.get(t, [])
        result[t] = _compact_series(h, n_bars)

    return {
        "range": range,
        "tickers": target,
        "series": {t: s.model_dump() for t, s in result.items()},
    }


@router.post("/refresh-analysis")
async def refresh_analysis_endpoint(request: Request, _sub: None = Depends(require_subscription)):
    """
    Force-regenerate the AI analysis regardless of cache age.
    Intended for admin / scheduled use — Gemini call may take 15–30 seconds.
    """
    try:
        analysis = await get_analysis_only(force=True)
        if analysis is None:
            return JSONResponse(
                status_code=503,
                content={"status": "error", "detail": "AI generation failed — check logs"},
            )
        return {"status": "ok", "generated_at": analysis.generated_at}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── /api/sectors/* — Sectors page endpoints ──────────────────────────────────

sectors_router = APIRouter(prefix="/api/sectors", tags=["sectors"])


@sectors_router.get("/page-data")
async def sectors_page_data_endpoint(
    include_stocks: bool = Query(True, description="Include per-sector stock scan"),
    top_n: int = Query(2, description="Number of winning sectors to return stocks for"),
):
    """
    Unified Sectors page payload.

    Returns:
    - All 11 sector snapshots (sorted by rotation_score)
    - Winning-sector detection (multi-timeframe composite)
    - Stock scan for top N sectors (momentum, bottleneck, anchor roles)
    - Persisted AI analysis — survives page refresh until user manually regenerates
    """
    try:
        from services.sector_rotation.service import get_sectors_page_data
        data = await get_sectors_page_data(
            include_stocks=include_stocks,
            top_sectors_for_stocks=max(1, min(top_n, 3)),
        )
        return data
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Sectors page error: {e}")


@sectors_router.get("/etf/{symbol}")
async def etf_detail_endpoint(symbol: str):
    """
    ETF detail: price, multi-timeframe performance, and holdings.

    Works for both broad SPDR sector ETFs (XLK, XLE, …) and theme ETFs
    (SMH, SOXX, CIBR, URA, …).

    Holdings are lazy-loaded with a 7-day fresh / 30-day stale cache.
    No AI calls. No extra quote fetches — reuses existing Tradier/Finnhub providers.
    """
    sym = symbol.upper().strip()
    if not sym or not sym.isalpha() or len(sym) > 10:
        raise HTTPException(status_code=400, detail=f"Invalid ETF symbol: {symbol!r}")

    import datetime as _dt
    from services.sector_rotation.providers import (
        fetch_etf_history,
        _tradier_quotes_batch,
        _finnhub_quote_single,
    )
    from services.sector_rotation.analytics import _pct_change, _ytd_change
    from services.sector_rotation.etf_holdings_service import get_etf_holdings

    now_iso = _dt.datetime.utcnow().isoformat() + "Z"

    # ── 1. Quote (price + 1D change) ─────────────────────────────────────────
    price: float | None = None
    change_1d: float | None = None

    try:
        td_data = await _tradier_quotes_batch([sym])
        q = td_data.get(sym, {})
        if not q:
            async with httpx.AsyncClient() as session:
                _, q = await _finnhub_quote_single(sym, session)
        price     = q.get("price") or q.get("last")
        change_1d = q.get("change_1d_pct")
    except Exception as e:
        print(f"[ETF_DETAIL] Quote error for {sym}: {e}")

    # ── 2. History (performance periods) ─────────────────────────────────────
    hist: list[dict] = []
    try:
        hist = await fetch_etf_history(sym, days=400)
    except Exception as e:
        print(f"[ETF_DETAIL] History error for {sym}: {e}")

    if price is None and hist:
        price = round(float(hist[-1]["close"]), 2)

    if change_1d is None and len(hist) >= 2:
        prev = float(hist[-2]["close"])
        last = float(hist[-1]["close"])
        change_1d = round((last - prev) / prev * 100, 2) if prev else None

    def _pct(n: int) -> float | None:
        v = _pct_change(hist, n)
        return round(v, 2) if v is not None else None

    _ytd = _ytd_change(hist)
    performance = {
        "1d":  round(change_1d, 2) if change_1d is not None else None,
        "7d":  _pct(5),
        "30d": _pct(22),
        "ytd": round(_ytd, 2) if _ytd is not None else None,
        "1y":  _pct(252),
    }

    # ── 3. Holdings (lazy, aggressively cached) ───────────────────────────────
    holdings_data: dict = {}
    try:
        holdings_data = await get_etf_holdings(sym)
    except Exception as e:
        print(f"[ETF_DETAIL] Holdings error for {sym}: {e}")
        holdings_data = {
            "symbol": sym, "source": "none",
            "holding_count": 0, "holdings": [], "top_holdings": [],
        }

    return {
        "symbol":        sym,
        "price":         round(price, 2) if price else None,
        "performance":   performance,
        "holding_count": holdings_data.get("holding_count", 0),
        "top_holdings":  holdings_data.get("top_holdings") or holdings_data.get("holdings", [])[:10],
        "holdings":      holdings_data.get("holdings", []),
        "as_of":         holdings_data.get("as_of"),
        "updated_at":    holdings_data.get("updated_at") or now_iso,
        "source":        holdings_data.get("source", "none"),
    }


# ── Helpers for compatibility wrappers ───────────────────────────────────────

# Sectors that are economically cyclical (used to preserve is_cyclical flag)
_CYCLICAL_SECTOR_IDS = frozenset({
    "technology", "industrials", "materials", "energy",
    "consumer_discretionary", "financials", "communication_services",
})

# Map new theme_rs state → old SectorSnapshot regime_tag
_STATE_TO_REGIME: dict[str, str] = {
    "active":    "Leading",
    "emerging":  "Leading",
    "neutral":   "Neutral",
    "weakening": "Weakening",
    "dead_zone": "Lagging",
}

# Map new theme_rs state → old theme_service trend_state
_STATE_TO_TREND: dict[str, str] = {
    "active":    "Leadership",
    "emerging":  "Improving",
    "neutral":   "Neutral",
    "weakening": "Weakening",
    "dead_zone": "Lagging",
}


def _rs_row_to_sector_snapshot(row: dict) -> dict:
    """
    Map a new theme_rs sector row onto the SectorSnapshot field contract so
    the frontend receives an identical shape to what get_dashboard() produced.
    Additive fields (rs_score, state, rs_vs_spy, compatibility_source) are appended
    and do not conflict with existing field names.
    """
    perf  = row.get("performance") or {}
    state = row.get("state") or "neutral"
    return {
        # ── Legacy SectorSnapshot fields ─────────────────────────────────────
        "ticker":                 row.get("lead_proxy") or (row["proxy_symbols"][0] if row.get("proxy_symbols") else ""),
        "name":                   row.get("display_name", ""),
        "price":                  row.get("price"),
        "change_1d":              perf.get("1D"),
        "change_7d":              perf.get("7D"),
        "change_30d":             perf.get("30D"),
        "change_ytd":             perf.get("YTD"),
        "change_1y":              perf.get("1Y"),
        "ma_50d":                 None,
        "ma_200d":                None,
        "pct_from_50d":           row.get("pct_from_50d"),
        "pct_from_200d":          None,
        "rotation_score":         row.get("rs_score"),         # rs_score ≈ rotation_score
        "relative_strength_rank": row.get("momentum_rank"),
        "regime_tag":             _STATE_TO_REGIME.get(state, "Neutral"),
        "is_cyclical":            row.get("theme_id") in _CYCLICAL_SECTOR_IDS,
        "series":                 {},
        # ── Additive fields (non-breaking) ────────────────────────────────────
        "rs_score":               row.get("rs_score"),
        "rs_vs_spy":              row.get("rs_vs_spy"),
        "rs_vs_qqq":              row.get("rs_vs_qqq"),
        "state":                  state,
        "state_reason":           row.get("state_reason"),
        "breadth_pct":            row.get("breadth_pct"),
        "classification":         row.get("classification"),
        "proxy_symbols":          row.get("proxy_symbols", []),
        "compatibility_source":   "theme_rs_service",
    }


def _accel_to_rotation_state(accel) -> str:
    if accel is None:
        return "Stabilizing"
    if accel > 1.5:
        return "Accelerating"
    if accel > 0:
        return "Stabilizing"
    if accel > -1.5:
        return "Fading"
    return "Reversing"


def _rs_row_to_theme_snapshot(row: dict) -> dict:
    """
    Map a new theme_rs row onto the old theme_service ThemeSnapshot field contract.
    Performance keys are translated (1D→1d, 7D→5d, 30D→1m, YTD→ytd, 1Y→1y).
    3m/6m are unavailable in the new RS engine — set to None.
    Additive fields appended.
    """
    perf  = row.get("performance") or {}
    state = row.get("state") or "neutral"
    return {
        # ── Legacy ThemeSnapshot fields ───────────────────────────────────────
        "id":                      row.get("theme_id"),
        "label":                   row.get("display_name", ""),
        "parent_sector":           row.get("parent_sector"),
        "theme_type":              row.get("classification"),
        "symbols":                 row.get("proxy_symbols", []),
        "leader_symbol":           row.get("lead_proxy"),
        "ticker":                  row.get("lead_proxy"),
        "price":                   row.get("price"),
        "leader_price":            row.get("price"),
        "current_price":           row.get("price"),
        "performance": {
            "1d":  perf.get("1D"),
            "5d":  perf.get("7D"),   # closest available (7D ≈ 5 trading days)
            "1m":  perf.get("30D"),
            "3m":  None,             # not computed by theme_rs_service
            "6m":  None,             # not computed by theme_rs_service
            "ytd": perf.get("YTD"),
            "1y":  perf.get("1Y"),
        },
        "perf_5d":                 perf.get("7D"),
        "perf_1m":                 perf.get("30D"),
        "perf_3m":                 None,
        "pct_from_50d":            row.get("pct_from_50d"),
        "trend_accel_20d":         row.get("trend_accel_20d"),
        "relative_strength_score": row.get("rs_score"),
        "momentum_rank":           row.get("momentum_rank"),
        "trend_state":             _STATE_TO_TREND.get(state, "Neutral"),
        "rotation_state":          _accel_to_rotation_state(row.get("trend_accel_20d")),
        # ── Additive fields (non-breaking) ────────────────────────────────────
        "rs_score":                row.get("rs_score"),
        "rs_vs_spy":               row.get("rs_vs_spy"),
        "rs_vs_qqq":               row.get("rs_vs_qqq"),
        "state":                   state,
        "state_reason":            row.get("state_reason"),
        "breadth_pct":             row.get("breadth_pct"),
        "classification":          row.get("classification"),
        "proxy_symbols_used":      row.get("proxy_symbols_used", []),
        "compatibility_source":    "theme_rs_service",
    }


async def _get_rs_payload_all(timeframe: str = "30D") -> dict:
    """
    Fetch the full 55-row RS payload once and return it.
    Calling with classification='all' avoids a second cache/compute hit vs
    calling sector + theme separately.
    """
    from services.theme_rs_service import get_theme_rs_data
    return await get_theme_rs_data(timeframe=timeframe, force=False, classification="all")


@sectors_router.get("/performance")
async def sectors_performance_endpoint(
    mode: str = Query("sectors", description="Mode: sectors | themes"),
    timeframe: str = Query("30D", description="Timeframe: 1D | 7D | 30D | YTD | 1Y"),
):
    """
    Sector or Theme performance data — compatibility wrapper.

    mode=sectors → 11 SPDR sector rows from /api/themes/relative-strength?classification=sector
    mode=themes  → theme + sub_theme rows from /api/themes/relative-strength (non-sector rows)

    No AI calls. No old get_dashboard() or get_theme_data() provider calls.
    Data sourced entirely from theme_rs_service (FMP→Tradier→yfinance, cached per TF).
    Response shape is identical to the previous contract plus additive compatibility_source field.
    """
    import datetime as _dt
    now = _dt.datetime.utcnow().isoformat() + "Z"
    tf  = timeframe.upper()

    print(f"[COMPAT_WRAPPER] /api/sectors/performance mode={mode} tf={tf} → theme_rs_service classification={'sector' if mode == 'sectors' else 'non-sector'}")

    try:
        payload = await _get_rs_payload_all(timeframe=tf)
        all_rows = payload.get("themes", [])

        if mode == "themes":
            rows   = [r for r in all_rows if r.get("classification") != "sector"]
            items  = [_rs_row_to_theme_snapshot(r) for r in rows]
            return {
                "mode":                 "themes",
                "updated_at":          now,
                "items":               items,
                "compatibility_source": "theme_rs_service",
                "classification_used":  "theme+sub_theme",
                "cache_age_seconds":    payload.get("cache_age_seconds"),
            }

        # mode=sectors (default)
        rows  = [r for r in all_rows if r.get("classification") == "sector"]
        items = [_rs_row_to_sector_snapshot(r) for r in rows]
        return {
            "mode":                 "sectors",
            "updated_at":          now,
            "items":               items,
            "compatibility_source": "theme_rs_service",
            "classification_used":  "sector",
            "cache_age_seconds":    payload.get("cache_age_seconds"),
        }

    except Exception as e:
        import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Sector performance error: {e}")


@sectors_router.get("/relative-strength")
async def sectors_relative_strength_endpoint(
    mode: str = Query("sectors", description="Mode: sectors | themes"),
    timeframe: str = Query("30D", description="Timeframe: 1D | 7D | 30D | YTD | 1Y"),
):
    """
    Sector or Theme relative-strength rankings — compatibility wrapper.

    mode=sectors → 11 SPDR sectors ranked by rs_score desc
    mode=themes  → theme + sub_theme rows ranked by rs_score desc

    No AI calls. No old get_dashboard() or get_theme_data() provider calls.
    Data sourced entirely from theme_rs_service (FMP→Tradier→yfinance, cached per TF).
    Response shape is identical to the previous contract plus additive compatibility_source field.
    """
    import datetime as _dt
    now = _dt.datetime.utcnow().isoformat() + "Z"
    tf  = timeframe.upper()

    print(f"[COMPAT_WRAPPER] /api/sectors/relative-strength mode={mode} tf={tf} → theme_rs_service classification={'sector' if mode == 'sectors' else 'non-sector'}")

    try:
        payload = await _get_rs_payload_all(timeframe=tf)
        all_rows = payload.get("themes", [])

        if mode == "themes":
            rows   = [r for r in all_rows if r.get("classification") != "sector"]
            mapped = [_rs_row_to_theme_snapshot(r) for r in rows]
            ranked = sorted(mapped, key=lambda r: r.get("relative_strength_score") or 0, reverse=True)
            return {
                "mode":                 "themes",
                "updated_at":          now,
                "ranked":              ranked,
                "compatibility_source": "theme_rs_service",
                "classification_used":  "theme+sub_theme",
                "cache_age_seconds":    payload.get("cache_age_seconds"),
            }

        # mode=sectors (default) — already sorted by rs_score desc in theme_rs_service
        rows   = [r for r in all_rows if r.get("classification") == "sector"]
        mapped = [_rs_row_to_sector_snapshot(r) for r in rows]
        ranked = sorted(mapped, key=lambda r: r.get("rotation_score") or 0, reverse=True)
        return {
            "mode":                 "sectors",
            "updated_at":          now,
            "ranked":              ranked,
            "compatibility_source": "theme_rs_service",
            "classification_used":  "sector",
            "cache_age_seconds":    payload.get("cache_age_seconds"),
        }

    except Exception as e:
        import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Sector RS error: {e}")


@sectors_router.post("/generate-analysis")
async def sectors_generate_analysis_endpoint(
    request: Request,
    _sub: None = Depends(require_subscription),
):
    """
    User-triggered analysis generation for the Sectors page.
    Subscription required. Triggers a new Gemini analysis call that:
    - Includes stock-level context for winning sectors
    - Outputs a "top 10 stocks to watch" ranking
    - Persists to disk (survives page refresh indefinitely)
    Returns the new analysis on success.
    """
    try:
        from services.sector_rotation.service import get_analysis_only
        from services.sector_rotation.analytics import get_winning_sectors
        from services.sector_rotation.gemini_analysis import get_or_generate_analysis
        from services.sector_rotation.service import (
            get_dashboard, _fetch_macro_overlay, _enrich_macro_with_treasuries,
        )
        from services.sector_rotation.analytics import build_sector_snapshots
        from services.sector_rotation.providers import fetch_etf_quotes, fetch_all_histories
        from services.sector_rotation.analytics import _pct_change

        import asyncio

        # Gather fresh market data for the analysis prompt context
        quotes, histories = await asyncio.gather(
            fetch_etf_quotes(),
            fetch_all_histories(),
        )
        macro = _fetch_macro_overlay()
        macro = await _enrich_macro_with_treasuries(macro)
        spy_hist = histories.get("SPY", [])
        spy_30d = _pct_change(spy_hist, 22)
        if spy_30d is not None:
            macro["spy_change_30d"] = round(spy_30d, 2)

        snapshots = build_sector_snapshots(quotes, histories)

        from services.sector_rotation.analytics import derive_regime
        regime = derive_regime(snapshots, macro)

        # Detect winning sectors to focus the analysis
        winners = get_winning_sectors(snapshots, top_n=3)
        winning_etfs = [w.etf for w in winners]

        analysis = await get_or_generate_analysis(
            snapshots=snapshots,
            regime=regime,
            macro=macro,
            force=True,
            winning_etfs=winning_etfs,
        )

        if analysis is None:
            return JSONResponse(
                status_code=503,
                content={"status": "error", "detail": "AI analysis generation failed — check server logs"},
            )

        # Compute the canonical stock list using the same path as page-data —
        # both endpoints share the same live-signal-ranked source of truth.
        from services.sector_rotation.sector_stocks import (
            get_sector_stocks, build_ranked_top_stocks,
        )
        from services.sector_rotation.gemini_analysis import _save_disk_cache
        etfs_for_stocks = winning_etfs[:2]
        stock_groups = await get_sector_stocks(etfs_for_stocks)
        top_stocks_ranked = build_ranked_top_stocks(stock_groups, limit=15)
        structured_dicts = [s.model_dump() for s in top_stocks_ranked]

        # Inject into analysis object and re-persist so future page-loads have it
        analysis = analysis.model_copy(update={"top_stocks_to_watch": structured_dicts})
        _save_disk_cache(analysis.model_dump())

        return {
            "status": "ok",
            "generated_at": analysis.generated_at,
            "winning_sector_etfs": winning_etfs,
            # top-level convenience field mirrors page-data contract
            "top_stocks_in_winning_sectors": [s.model_dump() for s in top_stocks_ranked],
            "analysis": analysis.model_dump(),
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
