"""
Home dashboard aggregator.

Composes already-cached services used elsewhere in the app so the new Home
page can render from a single call with no net-new third-party API traffic.

Reused sources (all with their existing cache TTLs in backend/data/cache.py):
    - MacroProvider.get_dashboard()               → macro:dashboard:v3
    - sector_rotation.get_dashboard()             → SR dashboard cache (5 min)
    - fmp.get_gainers_losers()                    → FMP_TTL
    - fmp.get_market_news()                       → FMP_TTL
    - fear_greed.get_fear_greed_index()           → FEAR_GREED_TTL
    - stocktwits.get_trending()                   → STOCKTWITS_TTL
    - watchlist_service.list_watchlists()         → file/pg-backed
    - tradier.get_quotes()                        → tradier:quotes:{symbols} cache
    - options_screener_lkg_v1:{tab} caches        → precompute loop (30 min)

All tasks run in parallel with return_exceptions=True so one failure never
breaks the whole payload. The aggregated result itself is cached for 60 s
(shorter than every upstream TTL, so this endpoint never fans out more
than its upstream caches already do).
"""
from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from data.cache import cache

_HOME_CACHE_KEY = "home:dashboard:v3"
_HOME_CACHE_TTL = 60  # 1 minute — upstream caches do the heavy lifting

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def _greeting_for_now() -> str:
    # Eastern-time market-relative greeting. Use UTC offset approximation
    # via local server time; acceptable because existing code does the same.
    h = datetime.now().hour
    if 5 <= h < 12:
        return "Good morning"
    if 12 <= h < 17:
        return "Good afternoon"
    if 17 <= h < 22:
        return "Good evening"
    return "Working late"


def _us_market_status() -> dict:
    """Light-weight cash-session detector (America/New_York approx)."""
    from datetime import datetime as _dt
    try:
        import zoneinfo
        now_et = _dt.now(zoneinfo.ZoneInfo("America/New_York"))
    except Exception:
        now_et = _dt.utcnow()
    wd = now_et.weekday()  # 0 = Mon
    mins = now_et.hour * 60 + now_et.minute
    is_weekday = wd < 5
    open_m, close_m, pre_m, post_m = 9 * 60 + 30, 16 * 60, 4 * 60, 20 * 60
    if not is_weekday:
        status = "closed"
    elif open_m <= mins < close_m:
        status = "open"
    elif pre_m <= mins < open_m:
        status = "pre_market"
    elif close_m <= mins < post_m:
        status = "after_hours"
    else:
        status = "closed"
    return {
        "status": status,
        "label": {
            "open": "Markets Open",
            "pre_market": "Pre-Market",
            "after_hours": "After Hours",
            "closed": "Markets Closed",
        }.get(status, "Markets Closed"),
        "now_et": now_et.strftime("%a %H:%M ET") if hasattr(now_et, "strftime") else None,
    }


def _safe(value: Any, default: Any) -> Any:
    if isinstance(value, Exception) or value is None:
        return default
    return value


def _mk_ticker(symbol: str, price: Any, change_pct: Any, asset_class: str = "equity") -> dict:
    return {
        "symbol": symbol,
        "price": price,
        "change_pct": change_pct,
        "asset_class": asset_class,
    }


def _extract_macro_cards(macro_raw: dict) -> list[dict]:
    """Pull a compact 6-card macro snapshot from the transformed macro payload."""
    cards: list[dict] = []
    try:
        from data.macro_transforms import transform_dashboard
        tx = transform_dashboard(macro_raw or {})
    except Exception:
        tx = macro_raw or {}

    # Benchmark ETFs contain SPY / QQQ / etc with price + change
    bench = {e.get("ticker"): e for e in (tx.get("benchmark_etfs") or [])}

    def _card(label: str, symbol: str, price, change, kind="equity", note=None):
        if price is None and change is None:
            return None
        return {
            "label": label,
            "symbol": symbol,
            "price": price,
            "change_pct": change,
            "kind": kind,
            "note": note,
        }

    spy = bench.get("SPY") or {}
    qqq = bench.get("QQQ") or {}
    cards += [
        _card("S&P 500", "SPY", spy.get("price"), spy.get("change_pct"), "equity"),
        _card("Nasdaq 100", "QQQ", qqq.get("price"), qqq.get("change_pct"), "equity"),
    ]

    rates = tx.get("rates_and_yields") or {}
    ten_y = rates.get("us_10y")
    if ten_y is not None:
        cards.append(_card("US 10Y Yield", "US10Y", ten_y, rates.get("us_10y_chg_1d"), "rate"))

    vix = tx.get("vix") or {}
    if vix.get("current") is not None:
        cards.append(_card("VIX", "VIX", vix.get("current"), vix.get("change_pct"), "volatility"))

    comm = tx.get("commodities") or {}
    gold = comm.get("gold") if isinstance(comm, dict) else None
    if isinstance(gold, dict) and gold.get("price") is not None:
        cards.append(_card("Gold", "GOLD", gold.get("price"), gold.get("change_pct"), "commodity"))

    dollar = tx.get("dollar") or {}
    if dollar.get("dxy") is not None:
        cards.append(_card("US Dollar", "DXY", dollar.get("dxy"), dollar.get("dxy_chg_1d"), "fx"))

    return [c for c in cards if c]


def _extract_ticker_strip(macro_raw: dict) -> list[dict]:
    """A compact top-of-page ticker strip: mix of equities + rates + commodities."""
    try:
        from data.macro_transforms import transform_dashboard
        tx = transform_dashboard(macro_raw or {})
    except Exception:
        tx = macro_raw or {}

    bench = {e.get("ticker"): e for e in (tx.get("benchmark_etfs") or [])}
    rates = tx.get("rates_and_yields") or {}
    vix = tx.get("vix") or {}
    comm = tx.get("commodities") or {}
    dollar = tx.get("dollar") or {}

    strip: list[dict] = []
    for t in ("SPY", "QQQ", "TLT", "GLD", "USO", "HYG"):
        e = bench.get(t)
        if e:
            strip.append(_mk_ticker(t, e.get("price"), e.get("change_pct"), "equity"))

    if rates.get("us_10y") is not None:
        strip.append(_mk_ticker("US10Y", rates.get("us_10y"), rates.get("us_10y_chg_1d"), "rate"))
    if rates.get("us_2y") is not None:
        strip.append(_mk_ticker("US2Y", rates.get("us_2y"), rates.get("us_2y_chg_1d"), "rate"))
    if vix.get("current") is not None:
        strip.append(_mk_ticker("VIX", vix.get("current"), vix.get("change_pct"), "volatility"))
    if dollar.get("dxy") is not None:
        strip.append(_mk_ticker("DXY", dollar.get("dxy"), dollar.get("dxy_chg_1d"), "fx"))
    if isinstance(comm, dict):
        for key, lbl in (("gold", "GOLD"), ("oil", "OIL"), ("nat_gas", "NATGAS")):
            c = comm.get(key)
            if isinstance(c, dict) and c.get("price") is not None:
                strip.append(_mk_ticker(lbl, c.get("price"), c.get("change_pct"), "commodity"))

    return strip


def _extract_movers(fmp_movers: dict) -> dict:
    """Shape top gainers / losers from FMP payload already used in scan_market."""
    gainers = (fmp_movers or {}).get("gainers") or []
    losers = (fmp_movers or {}).get("losers") or []

    def _to_float(v):
        if v is None:
            return None
        try:
            return float(str(v).replace("%", "").replace("+", "").replace(",", "").strip())
        except Exception:
            return None

    def _norm(rows, direction: str) -> list[dict]:
        out = []
        for r in rows[:8]:
            if not isinstance(r, dict):
                continue
            # Raw provider may expose either 'change' (string like '+3.45%') or
            # 'change_pct' (float) — accept both.
            raw_change = r.get("change")
            raw_change_pct = r.get("change_pct")
            chg_num = _to_float(raw_change_pct)
            if chg_num is None:
                chg_num = _to_float(raw_change)
            # Preserve a human label regardless of which shape we got
            if raw_change:
                label = str(raw_change)
            elif chg_num is not None:
                label = f"{chg_num:+.2f}%"
            else:
                label = ""
            ticker = r.get("ticker") or r.get("symbol")
            if not ticker:
                continue
            out.append({
                "ticker": ticker,
                "company": r.get("company") or r.get("name") or "",
                "price": _to_float(r.get("price")) if r.get("price") is not None else None,
                "change_pct": chg_num,
                "change_label": label,
                "direction": direction,
            })
        return out

    return {"gainers": _norm(gainers, "up"), "losers": _norm(losers, "down")}


def _extract_highlighted_companies(watchlists: list[dict], scan: dict) -> list[dict]:
    """Curated row of tickers — prefer user's first watchlist, fall back to trending."""
    tickers: list[str] = []
    if isinstance(watchlists, list) and watchlists:
        first = watchlists[0] or {}
        for row in (first.get("csv_data") or [])[:10]:
            t = (row.get("Ticker") or row.get("ticker") or "").strip().upper()
            if t and t not in tickers:
                tickers.append(t)
    if not tickers:
        trending = (scan or {}).get("stocktwits_trending") or []
        for t in trending[:10]:
            sym = (t.get("ticker") if isinstance(t, dict) else None)
            if sym and sym not in tickers:
                tickers.append(sym.upper())
    return [{"ticker": t, "source": "watchlist_or_trending"} for t in tickers[:10]]


def _extract_theme_performance(sector_dashboard) -> dict:
    """Sector snapshots repackaged as 'themes' for the center chart panel."""
    if not sector_dashboard:
        return {"themes": [], "regime": None, "updated_at": None}

    # sector_dashboard is a pydantic model if it came straight from the service
    try:
        d = sector_dashboard.model_dump() if hasattr(sector_dashboard, "model_dump") else sector_dashboard
    except Exception:
        d = {}

    sectors = d.get("sectors") or []
    themes = []
    for s in sectors:
        if not isinstance(s, dict):
            continue
        themes.append({
            "name": s.get("name") or s.get("ticker"),
            "ticker": s.get("ticker"),
            "rotation_score": s.get("rotation_score"),
            "relative_strength_rank": s.get("relative_strength_rank"),
            # Actual SectorSnapshot schema fields: change_1d / change_7d / change_30d / change_ytd
            "change_1d": s.get("change_1d"),
            "change_7d": s.get("change_7d"),
            "change_30d": s.get("change_30d"),
            "change_ytd": s.get("change_ytd"),
            "regime_tag": s.get("regime_tag"),
        })
    # Sort by 30D change for a meaningful chart order
    themes.sort(key=lambda t: (t.get("change_30d") if t.get("change_30d") is not None else -999), reverse=True)
    return {
        "themes": themes,
        "regime": d.get("regime"),
        "updated_at": d.get("updated_at"),
        "leaders": d.get("leaders") or [],
        "laggards": d.get("laggards") or [],
    }


def _extract_trending_ideas(scan: dict) -> list[dict]:
    """Trending Ideas, sourced exclusively from the already-cached Stocktwits
    trending feed (STOCKTWITS_TTL, no net-new API calls).

    Returns up to 8 idea cards with a stable shape the frontend can render
    without any further enrichment.
    """
    out: list[dict] = []
    trending = (scan or {}).get("stocktwits_trending") or []
    for t in trending[:8]:
        if not isinstance(t, dict):
            continue
        ticker = (t.get("ticker") or t.get("symbol") or "").upper()
        if not ticker:
            continue
        out.append({
            "ticker": ticker,
            "title": t.get("title") or ticker,
            "watchlist_count": t.get("watchlist_count"),
            "source": "stocktwits",
        })
    return out


def _extract_fear_greed(fg_equity: dict, macro_raw: dict) -> dict:
    """Equities fear/greed from the dedicated provider. Crypto FG is sourced client-side."""
    fg = fg_equity if isinstance(fg_equity, dict) else {}
    # Some pipelines stash a copy inside macro too — take either.
    if not fg.get("current_score"):
        fg_macro = (macro_raw or {}).get("fear_greed") or {}
        if fg_macro.get("score"):
            fg = {
                "current_score": fg_macro.get("score"),
                "current_rating": fg_macro.get("rating"),
                "signal": fg_macro.get("signal"),
                "historical": None,
            }
    return {
        "equities": {
            "score": fg.get("current_score"),
            "rating": fg.get("current_rating"),
            "signal": fg.get("signal"),
            "historical": fg.get("historical"),
        },
        # Crypto FG already lives in /api/coinmarketcap/market-overview — the
        # frontend stitches it in without hitting any new API.
        "crypto": None,
    }


# ── Unusual Options Flows ──────────────────────────────────────────────────

def _build_options_lkg_index() -> dict[str, dict]:
    """
    Read ALL tab LKG caches from the precompute loop — pure in-memory reads,
    zero API calls. Returns a dict keyed by uppercase ticker symbol.

    Each entry has the per-ticker summary from the engine:
        composite_score, primary_signal, call_volume, put_volume, pc_ratio,
        price_change_pct, options_context_summary, source_tab.
    """
    tabs = ["megacap", "large_cap", "small_cap", "etf"]
    index: dict[str, dict] = {}
    for tab in tabs:
        lkg = cache.get(f"options_screener_lkg_v1:{tab}")
        if not lkg:
            continue
        for t in (lkg.get("tickers") or []):
            sym = (t.get("ticker") or "").upper()
            if not sym or sym in index:
                continue
            index[sym] = {**t, "source_tab": tab}
    return index


def _extract_unusual_options_flows(options_index: dict[str, dict], limit: int = 8) -> list[dict]:
    """
    From the precomputed options index, extract the most unusual flows sorted
    by composite_score descending. Normalises each row to a compact Home shape.

    Ranking: composite_score from the existing engine (blend of flow_score,
    gamma_score, asymmetry_score, volatility_score, sentiment_score) — the
    highest scorers ARE the most unusual/significant flow events.
    """
    sorted_tickers = sorted(
        options_index.values(),
        key=lambda x: (x.get("composite_score") or 0),
        reverse=True,
    )

    out = []
    for t in sorted_tickers[:limit]:
        ctx = t.get("options_context_summary")
        if isinstance(ctx, dict):
            ctx = ctx.get("summary") or ctx.get("label") or None
        out.append({
            "symbol": t.get("ticker"),
            "primary_signal": t.get("primary_signal"),
            "composite_score": t.get("composite_score"),
            "call_volume": t.get("call_volume"),
            "put_volume": t.get("put_volume"),
            "pc_ratio": t.get("pc_ratio"),
            "price_change_pct": t.get("price_change_pct"),
            "options_context": ctx,
            "source_tab": t.get("source_tab"),
        })
    return out


# ── Snapshot helpers (watchlist + portfolio) ───────────────────────────────

async def _batch_quotes(tickers: list[str], data_service) -> dict[str, dict]:
    """
    One Tradier batch-quote call for the given tickers (already cached at
    tradier:quotes:{symbols} TTL). Returns a dict keyed by uppercase symbol.
    """
    if not tickers or not data_service or not getattr(data_service, "tradier", None):
        return {}
    try:
        quotes = await data_service.tradier.get_quotes(tickers)
        return {(q.get("symbol") or "").upper(): q for q in (quotes or [])}
    except Exception as exc:
        print(f"[HOME] batch_quotes error (non-fatal): {exc}")
        return {}


def _snapshot_row(
    symbol: str,
    quote: dict,
    options_index: dict[str, dict],
    asset_type: str | None = None,
) -> dict:
    """Build a normalised snapshot row from a single Tradier quote dict."""
    last = quote.get("last")
    chg_pct = quote.get("change_percentage")
    vol = quote.get("volume")
    avg_vol = quote.get("average_volume")
    vol_ratio = round(vol / avg_vol, 2) if vol and avg_vol and avg_vol > 0 else None

    opts = options_index.get(symbol.upper(), {})
    row = {
        "symbol": symbol.upper(),
        "current_price": last,
        "change_1d_pct": chg_pct,
        "volume_vs_avg": vol_ratio,
        "options_signal": opts.get("primary_signal"),
    }
    if asset_type is not None:
        row["asset_type"] = asset_type
    return row


async def _fetch_watchlist_snapshot(
    watchlists: list[dict],
    data_service,
    options_index: dict[str, dict],
) -> list[dict]:
    """
    Compact snapshot for the user's primary watchlist.
    One Tradier batch-quote call, then overlay options signal from LKG cache.
    NEVER touches shares/cost/portfolio data.
    """
    tickers: list[str] = []
    if isinstance(watchlists, list) and watchlists:
        first = watchlists[0] or {}
        for row in (first.get("csv_data") or [])[:20]:
            t = (row.get("Ticker") or row.get("ticker") or "").strip().upper()
            if t and t not in tickers:
                tickers.append(t)

    if not tickers:
        return []

    quotes = await _batch_quotes(tickers, data_service)
    return [
        _snapshot_row(t, quotes.get(t, {}), options_index)
        for t in tickers
    ]


async def _fetch_portfolio_snapshot(
    data_service,
    options_index: dict[str, dict],
) -> list[dict]:
    """
    Compact snapshot for the user's portfolio holdings.
    Privacy: NEVER exposes shares, avg_cost, cost_basis, or market value.
    Exposes only: symbol, asset_type, current_price, change_1d_pct,
                  volume_vs_avg, options_signal.
    One Tradier batch-quote call, options signal from LKG cache.
    """
    try:
        pf_file = _DATA_DIR / "portfolio_holdings.json"
        if not pf_file.exists():
            return []
        raw = json.loads(pf_file.read_text())
        holdings = raw.get("holdings", [])
        if not isinstance(holdings, list) or not holdings:
            return []
    except Exception as exc:
        print(f"[HOME] portfolio read error (non-fatal): {exc}")
        return []

    # Deduplicate tickers, keep asset_type mapping
    seen: dict[str, str] = {}
    for h in holdings:
        if not isinstance(h, dict):
            continue
        sym = (h.get("ticker") or "").upper().strip()
        if not sym:
            continue
        asset_type = (h.get("asset_type") or h.get("type") or "stock").lower()
        if sym not in seen:
            seen[sym] = asset_type

    if not seen:
        return []

    tickers = list(seen.keys())
    quotes = await _batch_quotes(tickers, data_service)
    return [
        _snapshot_row(t, quotes.get(t, {}), options_index, asset_type=seen[t])
        for t in tickers
    ]


# ── Latest News ────────────────────────────────────────────────────────────

async def _fetch_latest_news(data_service, limit: int = 8) -> list[dict]:
    """
    General market news from FMP (already cached at FMP_TTL upstream).
    Returns up to `limit` normalised articles. Fails soft to [].
    """
    if not data_service or not getattr(data_service, "fmp", None):
        return []
    try:
        raw = await data_service.fmp.get_market_news(limit=limit)
        out = []
        for art in (raw or [])[:limit]:
            if not isinstance(art, dict):
                continue
            title = (art.get("title") or "").strip()
            if not title:
                continue
            out.append({
                "headline": title,
                "summary": (art.get("text") or "")[:200],
                "url": art.get("url") or "",
                "published_at": art.get("published") or art.get("publishedDate") or "",
                "source": art.get("source") or "",
                "symbol": art.get("symbol") or "",
            })
        return out
    except Exception as exc:
        print(f"[HOME] latest_news error (non-fatal): {exc}")
        return []


# ── Main aggregator ────────────────────────────────────────────────────────

async def build_home_dashboard(
    *,
    data_service,
    macro_provider,
    watchlists_loader=None,
    force: bool = False,
) -> dict:
    """Main entry — returns the normalized Home payload."""
    if not force:
        cached = cache.get(_HOME_CACHE_KEY)
        if cached is not None:
            return {**cached, "from_cache": True}

    t0 = time.time()

    # ── Tasks (all optional / fail-soft) ──────────────────────────────
    tasks: dict[str, Optional[asyncio.Task]] = {}

    # Macro dashboard (already cached for 10 min upstream)
    if macro_provider:
        tasks["macro"] = asyncio.create_task(macro_provider.get_dashboard())

    # Sector rotation dashboard (5 min cache upstream). Skip AI analysis to avoid
    # any chance of triggering a Gemini call from the home endpoint.
    try:
        from services.sector_rotation.service import get_dashboard as _sr_get
        tasks["sector"] = asyncio.create_task(_sr_get(include_analysis=False))
    except Exception:
        tasks["sector"] = None

    # FMP gainers/losers (FMP_TTL cache upstream). FMP free tier often 403s on
    # stock_market/gainers and quietly returns [] — we also kick off finviz as a
    # fallback and merge below, so Home never shows empty movers when either
    # source succeeds.
    if data_service and getattr(data_service, "fmp", None):
        tasks["movers"] = asyncio.create_task(data_service.fmp.get_gainers_losers())

    if data_service and getattr(data_service, "finviz", None):
        tasks["fv_gainers"] = asyncio.create_task(data_service.finviz.get_screener_results("ta_topgainers"))
        tasks["fv_losers"] = asyncio.create_task(data_service.finviz.get_screener_results("ta_toplosers"))

    # Fear & Greed (FEAR_GREED_TTL cache upstream)
    if data_service and getattr(data_service, "fear_greed", None):
        tasks["fg"] = asyncio.create_task(data_service.fear_greed.get_fear_greed_index())

    # Stocktwits trending (STOCKTWITS_TTL cache upstream) — only ticker symbols
    if data_service and getattr(data_service, "stocktwits", None):
        tasks["trending"] = asyncio.create_task(data_service.stocktwits.get_trending())

    # Latest news — FMP general market news (FMP_TTL cache upstream)
    tasks["news"] = asyncio.create_task(_fetch_latest_news(data_service))

    # ── Watchlists (local storage — blocking but fast) ─────────────────
    watchlists: list[dict] = []
    try:
        if watchlists_loader is None:
            from services.watchlist_service import list_watchlists as _lw
            watchlists_loader = _lw
        watchlists = await asyncio.to_thread(watchlists_loader)
    except Exception:
        watchlists = []

    # ── Options LKG index (pure in-memory read — must be synchronous) ──
    # This reads precomputed cache entries written by _options_precompute_loop
    # every 30 min. Zero API calls. Safe to do before awaiting other tasks.
    options_index: dict[str, dict] = {}
    try:
        options_index = await asyncio.to_thread(_build_options_lkg_index)
    except Exception as exc:
        print(f"[HOME] options_index build error (non-fatal): {exc}")

    # ── Watchlist + Portfolio snapshots (each needs one Tradier batch-quote) ─
    tasks["watchlist_snap"] = asyncio.create_task(
        _fetch_watchlist_snapshot(watchlists, data_service, options_index)
    )
    tasks["portfolio_snap"] = asyncio.create_task(
        _fetch_portfolio_snapshot(data_service, options_index)
    )

    # ── Await all tasks ────────────────────────────────────────────────
    results: dict[str, Any] = {}
    for name, task in tasks.items():
        if task is None:
            results[name] = None
            continue
        try:
            results[name] = await task
        except Exception as exc:
            results[name] = exc

    macro_raw = _safe(results.get("macro"), {}) or {}
    sector_dash = _safe(results.get("sector"), None)
    fmp_movers = _safe(results.get("movers"), {}) or {}
    fv_gainers = _safe(results.get("fv_gainers"), []) or []
    fv_losers = _safe(results.get("fv_losers"), []) or []
    fg_equity = _safe(results.get("fg"), {}) or {}
    trending = _safe(results.get("trending"), []) or []
    latest_news = _safe(results.get("news"), []) or []
    watchlist_snapshot = _safe(results.get("watchlist_snap"), []) or []
    portfolio_snapshot = _safe(results.get("portfolio_snap"), []) or []

    # Merge FMP + Finviz — prefer whichever source has rows. If FMP 403'd and
    # returned [], Finviz fills the gap. If both succeeded, FMP takes priority
    # for stability and we backfill to reach 8 rows.
    def _merge_movers(primary: list, fallback: list) -> list:
        seen = set()
        merged: list = []
        for r in (primary or []):
            if not isinstance(r, dict):
                continue
            sym = (r.get("ticker") or r.get("symbol") or "").upper()
            if not sym or sym in seen:
                continue
            seen.add(sym)
            merged.append(r)
        for r in (fallback or []):
            if len(merged) >= 8:
                break
            if not isinstance(r, dict):
                continue
            sym = (r.get("ticker") or r.get("symbol") or "").upper()
            if not sym or sym in seen:
                continue
            seen.add(sym)
            merged.append(r)
        return merged

    merged_movers = {
        "gainers": _merge_movers(fmp_movers.get("gainers") if isinstance(fmp_movers, dict) else [], fv_gainers),
        "losers": _merge_movers(fmp_movers.get("losers") if isinstance(fmp_movers, dict) else [], fv_losers),
    }

    scan_lite = {"stocktwits_trending": trending}

    # Trending on X — weekly cached snapshot. Never does a live X scan on page
    # load: if the disk cache is <7d old we return it, if it's stale/missing we
    # return what we have (or empty) and kick off one background refresh.
    trending_on_x: dict = {
        "generated_at": None,
        "top_tickers": [],
        "key_themes": [],
        "notable_accounts": [],
        "is_stale": True,
        "refresh_in_progress": False,
        "available": False,
    }
    try:
        from services.x_consensus_cache import get_weekly_snapshot
        trending_on_x = await get_weekly_snapshot(data_service=data_service, allow_refresh=True)
    except Exception as exc:
        print(f"[HOME] Trending on X snapshot failed soft: {exc}")

    # Unusual options flows from precomputed LKG cache — zero new API calls
    unusual_options_flows = _extract_unusual_options_flows(options_index)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "greeting": {
            "text": _greeting_for_now(),
            "market": _us_market_status(),
        },
        "ticker_strip": _extract_ticker_strip(macro_raw),
        "macro_cards": _extract_macro_cards(macro_raw),
        "highlighted_companies": _extract_highlighted_companies(watchlists, scan_lite),
        "theme_performance": _extract_theme_performance(sector_dash),
        "trending_ideas": _extract_trending_ideas(scan_lite),
        "movers": _extract_movers(merged_movers),
        "trending_on_x": trending_on_x,
        "fear_greed": _extract_fear_greed(fg_equity, macro_raw),
        # ── New panels ──────────────────────────────────────────────────
        "latest_news": latest_news,
        "unusual_options_flows": unusual_options_flows,
        "watchlist_snapshot": watchlist_snapshot,
        "portfolio_snapshot": portfolio_snapshot,
        # ────────────────────────────────────────────────────────────────
        "section_status": {
            "macro": "ok" if not isinstance(results.get("macro"), Exception) and results.get("macro") else "unavailable",
            "sector": "ok" if not isinstance(results.get("sector"), Exception) and results.get("sector") else "unavailable",
            "movers": (
                "ok"
                if (merged_movers.get("gainers") or merged_movers.get("losers"))
                else "unavailable"
            ),
            "fear_greed": "ok" if not isinstance(results.get("fg"), Exception) and results.get("fg") else "unavailable",
            "trending": "ok" if not isinstance(results.get("trending"), Exception) and results.get("trending") else "unavailable",
            "trending_on_x": "ok" if trending_on_x.get("available") else ("refreshing" if trending_on_x.get("refresh_in_progress") else "unavailable"),
            "latest_news": "ok" if latest_news else "unavailable",
            "unusual_options_flows": "ok" if unusual_options_flows else "precompute_pending",
            "watchlist_snapshot": "ok" if watchlist_snapshot else "unavailable",
            "portfolio_snapshot": "ok" if portfolio_snapshot else "unavailable",
        },
        "timing": {"total_seconds": round(time.time() - t0, 2)},
        "from_cache": False,
    }

    cache.set(_HOME_CACHE_KEY, payload, _HOME_CACHE_TTL)
    return payload
