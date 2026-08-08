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
    - pg_storage.watchlist_read()                 → PostgreSQL (primary watchlist)
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
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from data.cache import cache
from services.hyperliquid.router import get_state_optional as _hl_get_state

_HOME_CACHE_KEY     = "home:dashboard:v3"
_HOME_CACHE_LKG_KEY = "home:dashboard:v3:lkg"
_HOME_CACHE_TTL     = 60        # 1 min  — upstream caches do the heavy lifting
_HOME_CACHE_LKG_TTL = 4 * 3600  # 4 h    — survives cold restarts / redeploys
_HOME_REBUILD_ACTIVE = False    # single-flight guard for background LKG rebuild

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"


# ── Sub-theme taxonomy ─────────────────────────────────────────────────────
# Curated ticker sets per niche sub-theme. Cross-referenced against live
# Tradier quotes and the options LKG cache. Includes tickers from the user's
# Theme Performance on the Home page is driven entirely by THEME_RS_UNIVERSE.
# Themes with proxy_type == "custom" are displayed in the Theme Performance section.
# To add/remove a theme from the Home page, add/remove it from THEME_RS_UNIVERSE
# in backend/services/theme_rs_universe.py — no changes needed here.


# ── Utilities ──────────────────────────────────────────────────────────────

def _greeting_for_now() -> str:
    h = datetime.now().hour
    if 5 <= h < 12:
        return "Good morning"
    if 12 <= h < 17:
        return "Good afternoon"
    if 17 <= h < 22:
        return "Good evening"
    return "Working late"


def _us_market_status() -> dict:
    from datetime import datetime as _dt
    try:
        import zoneinfo
        now_et = _dt.now(zoneinfo.ZoneInfo("America/New_York"))
    except Exception:
        now_et = _dt.utcnow()
    wd = now_et.weekday()
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
    return {"symbol": symbol, "price": price, "change_pct": change_pct, "asset_class": asset_class}


def _parse_pct(s: Any) -> float | None:
    """Parse '112.66%' or '-5.34%' to a float. Returns None on failure."""
    if s is None:
        return None
    try:
        return float(str(s).replace("%", "").replace("+", "").strip())
    except Exception:
        return None


def _parse_float(s: Any) -> float | None:
    try:
        return float(s)
    except Exception:
        return None


# ── Macro helpers ──────────────────────────────────────────────────────────

def _extract_macro_cards(macro_raw: dict) -> list[dict]:
    cards: list[dict] = []
    try:
        from data.macro_transforms import transform_dashboard
        tx = transform_dashboard(macro_raw or {})
    except Exception:
        tx = macro_raw or {}

    bench = {e.get("ticker"): e for e in (tx.get("benchmark_etfs") or [])}

    def _card(label, symbol, price, change, kind="equity", note=None):
        if price is None and change is None:
            return None
        return {"label": label, "symbol": symbol, "price": price, "change_pct": change, "kind": kind, "note": note}

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
            raw_change = r.get("change")
            raw_change_pct = r.get("change_pct")
            chg_num = _to_float(raw_change_pct)
            if chg_num is None:
                chg_num = _to_float(raw_change)
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


def _extract_theme_performance(sector_dashboard) -> dict:
    """Broad sector snapshots — kept for backwards compatibility."""
    if not sector_dashboard:
        return {"themes": [], "regime": None, "updated_at": None}
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
            "change_1d": s.get("change_1d"),
            "change_7d": s.get("change_7d"),
            "change_30d": s.get("change_30d"),
            "change_ytd": s.get("change_ytd"),
            "regime_tag": s.get("regime_tag"),
        })
    themes.sort(key=lambda t: (t.get("change_30d") if t.get("change_30d") is not None else -999), reverse=True)
    return {
        "themes": themes,
        "regime": d.get("regime"),
        "updated_at": d.get("updated_at"),
        "leaders": d.get("leaders") or [],
        "laggards": d.get("laggards") or [],
    }


def _extract_trending_ideas(scan: dict) -> list[dict]:
    out: list[dict] = []
    trending = (scan or {}).get("stocktwits_trending") or []
    for t in trending:
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
    fg = fg_equity if isinstance(fg_equity, dict) else {}
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
        "crypto": None,
    }


# ── Watchlist loading — source of truth fix ───────────────────────────────

async def _load_primary_watchlist() -> dict | None:
    """
    Load the user's primary watchlist from PostgreSQL (or JSON fallback).
    Returns the FULL watchlist dict with keys: id, name, tickers[], csv_data[].

    BUG FIX: list_watchlists() returns metadata-only rows (no tickers/csv_data).
    We must call watchlist_read(id) to get the actual data.
    """
    try:
        from data.pg_storage import watchlist_list, watchlist_read, is_available
        if is_available():
            entries = await asyncio.to_thread(watchlist_list)
            if entries:
                wl_id = entries[0].get("id")
                if wl_id:
                    full = await asyncio.to_thread(watchlist_read, wl_id)
                    if full and full.get("tickers"):
                        return full
    except Exception as exc:
        print(f"[HOME] pg watchlist load failed (non-fatal): {exc}")

    # JSON fallback
    try:
        from services.watchlist_service import _read_store
        store = await asyncio.to_thread(_read_store)
        if store and store.get("tickers"):
            return store
    except Exception as exc:
        print(f"[HOME] JSON watchlist fallback failed (non-fatal): {exc}")

    return None


def _build_csv_index(watchlist: dict | None) -> dict[str, dict]:
    """
    Build {ticker: csv_row} from watchlist csv_data.
    Keys: RSI, Relative Volume, Price Change 20-Day MA, Stock Price, etc.
    """
    index: dict[str, dict] = {}
    if not watchlist:
        return index
    for row in (watchlist.get("csv_data") or []):
        sym = (row.get("Symbol") or row.get("Ticker") or row.get("ticker") or "").strip().upper()
        if sym:
            index[sym] = row
    return index


# ── Highlighted companies — ranked from actual watchlist ──────────────────

def _signal_label(chg_pct: float | None, rsi: float | None, rel_vol: float | None, opts_signal: str | None) -> str:
    """Derive a short human label for the Home watchlist card."""
    parts = []
    if opts_signal and opts_signal not in ("", "neutral", "none"):
        parts.append(opts_signal.replace("_", " "))
    if rel_vol is not None and rel_vol > 2.0:
        parts.append("vol spike")
    if rsi is not None and rsi >= 65:
        parts.append("momentum")
    if chg_pct is not None and chg_pct >= 3.0:
        parts.append("hot")
    elif chg_pct is not None and chg_pct >= 1.5:
        parts.append("trending up")
    return ", ".join(parts[:2]) if parts else ""


def _rank_watchlist_rows(
    tickers: list[str],
    quote_by_sym: dict[str, dict],
    csv_index: dict[str, dict],
    options_index: dict[str, dict],
    limit: int = 10,
) -> list[dict]:
    """
    Rank watchlist tickers by signal strength and return top `limit` rows.

    Ranking formula (all normalised 0-1):
      rank_score = 0.50 * change_1d_pct_norm
                 + 0.30 * rel_vol_norm
                 + 0.20 * rsi_momentum_norm

    Returns normalised rows: symbol, current_price, change_1d_pct,
    volume_vs_avg, options_signal, rsi, signal_label.
    """
    scored: list[tuple[float, dict]] = []

    for sym in tickers:
        sym_up = sym.upper()
        q = quote_by_sym.get(sym_up, {})
        csv = csv_index.get(sym_up, {})
        opts = options_index.get(sym_up, {})

        last = q.get("last")
        chg_pct = q.get("change_percentage")
        vol = q.get("volume")
        avg_vol = q.get("average_volume")
        vol_ratio = round(vol / avg_vol, 2) if vol and avg_vol and avg_vol > 0 else None

        # Parse RSI from csv_data — field name: 'Relative Strength Index (RSI)'
        rsi = _parse_float(csv.get("Relative Strength Index (RSI)") or csv.get("RSI"))
        # Parse relative volume from csv_data — field name: 'Relative Volume'
        rel_vol_csv = _parse_pct(csv.get("Relative Volume"))
        if rel_vol_csv is not None:
            rel_vol_csv = rel_vol_csv / 100.0  # convert 112.66% → 1.1266x

        effective_rel_vol = vol_ratio if vol_ratio is not None else rel_vol_csv

        # Normalise each signal 0-1 for composite ranking
        chg_norm = max(0.0, min((chg_pct or 0) / 15.0, 1.0))        # clamp at ±15%
        rel_vol_norm = max(0.0, min((effective_rel_vol or 1.0) / 5.0, 1.0))  # cap at 5x
        rsi_norm = max(0.0, min(((rsi or 50) - 30) / 70.0, 1.0))    # 30=0, 100=1

        rank_score = 0.50 * chg_norm + 0.30 * rel_vol_norm + 0.20 * rsi_norm

        label = _signal_label(chg_pct, rsi, effective_rel_vol, opts.get("primary_signal"))

        row = {
            "symbol": sym_up,
            "current_price": last,
            "change_1d_pct": chg_pct,
            "volume_vs_avg": vol_ratio,
            "options_signal": opts.get("primary_signal"),
            "rsi": round(rsi, 1) if rsi is not None else None,
            "signal_label": label,
        }
        scored.append((rank_score, row))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [row for _, row in scored[:limit]]


# ── Unusual Options Flows ──────────────────────────────────────────────────

# ── Home unusual-options constants ─────────────────────────────────────────
# _HOME_OPTIONS_CACHE_KEY kept for backward compat (old fast loop wrote here).
_HOME_OPTIONS_CACHE_KEY = "home:unusual_options:v1"
# All 4 maintained tab universes that Home derives from (Phase 3 arch).
_HOME_OPTIONS_TABS = ("megacap", "large_cap", "small_cap", "etf")


def _build_options_lkg_index() -> dict[str, dict]:
    """
    Build options ticker index for Home sections that need per-symbol options_signal
    (watchlist rows, sub-theme performance, portfolio snapshot).

    Priority order (pure in-memory reads, zero API calls):
      1. Master screener hot cache (options_master_screener_v1)
      2. Master screener LKG (options_master_lkg_v1)
      3. Legacy per-tab LKG fallback (options_screener_lkg_v1:{tab})

    Returns a dict keyed by uppercase ticker symbol.
    """
    index: dict[str, dict] = {}

    # ── Priority 1 + 2: Master screener (unified architecture) ───────
    master = cache.get("options_master_screener_v1") or cache.get("options_master_lkg_v1")
    if master:
        for t in (master.get("tickers") or []):
            sym = (t.get("ticker") or "").upper()
            if not sym or sym in index:
                continue
            index[sym] = {**t, "source_tab": "master"}
        return index

    # ── Priority 3: Legacy per-tab LKG (transition / cold start) ─────
    tabs = ["megacap", "large_cap", "small_cap", "etf"]
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


def _extract_unusual_options_flows(
    options_index: dict[str, dict],
    limit: int = 8,
    updated_at: str | None = None,
) -> list[dict]:
    """
    Extract the most unusual flows sorted by composite_score descending.
    Normalises each row to a compact, frontend-friendly Home shape.

    When the Home-dedicated fast cache (home:unusual_options:v1) is warm,
    options_index is populated from that 10-minute path (Tradier-only, no AI).
    Falls back to the full Options Flow LKG tabs otherwise.
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
        primary = t.get("primary_signal")
        composite = t.get("composite_score")
        out.append({
            "symbol": t.get("ticker"),
            # Primary names (spec-aligned)
            "flow_signal": primary,
            "unusual_score": composite,
            "call_volume": t.get("call_volume"),
            "put_volume": t.get("put_volume"),
            "call_put_skew": t.get("pc_ratio"),   # ratio < 1 → call-heavy, > 1 → put-heavy
            "price_change_pct": t.get("price_change_pct"),
            "rationale": ctx,
            "updated_at": updated_at,
            # Legacy aliases kept for any existing consumer
            "primary_signal": primary,
            "composite_score": composite,
            "pc_ratio": t.get("pc_ratio"),
            "options_context": ctx,
            "source_tab": t.get("source_tab"),
        })
    return out


def _get_options_meta(
    fast: dict | None,
    lkg_source: str | None = None,
    lkg_updated_at: str | None = None,
) -> dict:
    """
    Build the unusual_options_meta block that accompanies every Home response.

    data_state values:
      no_data_yet          — nothing in any cache layer (very first cold start)
      refresh_in_progress  — background loop is mid-scan, serving stale data
      stale_but_available  — serving LKG tab data (full precompute loop)
      live_ok              — fast cache is warm and scan found results
      true_zero_results    — most recent scan completed but found 0 unusual flows
    """
    import time as _t

    if fast:
        cached_at = fast.get("cached_at", _t.time())
        age = int(_t.time() - cached_at)
        refresh_in_progress = bool(fast.get("refresh_in_progress"))
        tickers = fast.get("tickers") or []
        data_state = fast.get("data_state", "live_ok")
        if refresh_in_progress and tickers:
            data_state = "refresh_in_progress"
        elif refresh_in_progress and not tickers:
            data_state = "refresh_in_progress"
        return {
            "source": "home_fast_cache",
            "updated_at": fast.get("updated_at"),
            "age_seconds": age,
            "stale": age > 300,
            "refresh_in_progress": refresh_in_progress,
            "data_state": data_state,
            "result_count": len(tickers),
            "scan_scope": fast.get("scan_scope"),
            "scan_elapsed_s": fast.get("scan_elapsed_s"),
        }

    if lkg_source:
        return {
            "source": lkg_source,
            "updated_at": lkg_updated_at,
            "age_seconds": None,
            "stale": True,
            "refresh_in_progress": False,
            "data_state": "stale_but_available",
            "result_count": None,
        }

    return {
        "source": "none",
        "updated_at": None,
        "age_seconds": None,
        "stale": True,
        "refresh_in_progress": False,
        "data_state": "no_data_yet",
        "result_count": 0,
    }


async def _fetch_unusual_options_live(
    data_service, limit: int = 8
) -> tuple[list[dict], dict]:
    """
    Return (flows_list, meta_dict) for the Home unusual-options panel.

    Priority order (all in-memory reads — zero blocking, no Tradier calls):
      1. Master screener hot cache (options_master_screener_v1) — single unified
         snapshot covering ETF + megacap + large_cap + small_cap together.
      2. Master screener LKG (options_master_lkg_v1) — 4-hr TTL, disk-backed.
      3. Legacy per-tab caches (backward compat during transition period).
      4. Cold (no_data_yet) — master screener loop populates within ~90s.

    Rank: globally by composite_score — abnormality wins, not cap size.
    No Tradier calls. No AI API. No background tasks fired from here.
    """
    from datetime import datetime as _dt2, timezone as _tz2

    # ── 1 + 2. Master screener (unified architecture) ────────────────────────
    master_hot = cache.get("options_master_screener_v1")
    master_lkg = cache.get("options_master_lkg_v1")
    master = master_hot or master_lkg

    if master:
        is_stale = master_hot is None
        merged_index: dict[str, dict] = {}
        for ticker in (master.get("tickers") or []):
            sym = (ticker.get("ticker") or "").upper()
            if sym and sym not in merged_index:
                merged_index[sym] = {**ticker, "source_tab": "master"}
        best_updated_at = master.get("updated_at") or master.get("cached_at")
        if isinstance(best_updated_at, (int, float)):
            best_updated_at = _dt2.fromtimestamp(best_updated_at, tz=_tz2.utc).isoformat()
        meta = {
            "source":              "master_screener" if not is_stale else "master_screener_lkg",
            "updated_at":          best_updated_at,
            "age_seconds":         None,
            "stale":               is_stale,
            "refresh_in_progress": is_stale,
            "data_state":          "live_ok" if not is_stale else "stale_but_available",
            "result_count":        len(merged_index),
        }
        flows = _extract_unusual_options_flows(merged_index, limit=limit, updated_at=best_updated_at)
        return flows, meta

    # ── 3. Legacy per-tab caches (backward compat / transition) ─────────────
    merged_index = {}
    best_updated_at = None
    tabs_hot: list[str] = []
    tabs_lkg: list[str] = []
    tabs_absent: list[str] = []

    for tab in ["megacap", "large_cap", "small_cap", "etf"]:
        hot = cache.get(f"options_screener_v9:{tab}")
        lkg = cache.get(f"options_screener_lkg_v1:{tab}")
        chosen = hot or lkg
        if not chosen:
            tabs_absent.append(tab)
            continue
        if hot:
            tabs_hot.append(tab)
        else:
            tabs_lkg.append(tab)
        tab_updated = chosen.get("updated_at") or chosen.get("cached_at")
        if isinstance(tab_updated, (int, float)):
            tab_updated = _dt2.fromtimestamp(tab_updated, tz=_tz2.utc).isoformat()
        if best_updated_at is None:
            best_updated_at = tab_updated
        for ticker in (chosen.get("tickers") or []):
            sym = (ticker.get("ticker") or "").upper()
            if sym and sym not in merged_index:
                merged_index[sym] = {**ticker, "source_tab": tab}

    if merged_index:
        n_tabs_live = len(tabs_hot) + len(tabs_lkg)
        data_state = "live_ok" if tabs_hot else "stale_but_available"
        source_desc = (
            f"all_tabs:{'+'.join(tabs_hot or tabs_lkg)}"
            if n_tabs_live == len(_HOME_OPTIONS_TABS)
            else f"partial_tabs:{n_tabs_live}/{len(_HOME_OPTIONS_TABS)}"
        )
        meta = {
            "source": source_desc,
            "updated_at": best_updated_at,
            "age_seconds": None,
            "stale": len(tabs_lkg) > 0 and not tabs_hot,
            "refresh_in_progress": bool(tabs_absent),
            "data_state": data_state,
            "result_count": len(merged_index),
            "tabs_hot": tabs_hot,
            "tabs_lkg": tabs_lkg,
            "tabs_absent": tabs_absent,
        }
        flows = _extract_unusual_options_flows(merged_index, limit=limit, updated_at=best_updated_at)
        return flows, meta

    # ── 3. Truly cold — no data in any cache layer yet ───────────────────────
    # Precompute loop will populate within ~120s on first cold start.
    # Disk-backed LKG means this state only lasts during first-ever deployment.
    print("[HOME_OPTIONS] All cache layers cold (no_data_yet) — precompute loop will populate shortly")
    meta = {
        "source": "none",
        "updated_at": None,
        "age_seconds": None,
        "stale": True,
        "refresh_in_progress": True,
        "data_state": "no_data_yet",
        "result_count": 0,
        "tabs_hot": [],
        "tabs_lkg": [],
        "tabs_absent": list(_HOME_OPTIONS_TABS),
    }
    return [], meta


# ── Batch-quote helper ─────────────────────────────────────────────────────

async def _batch_quotes(tickers: list[str], data_service) -> dict[str, dict]:
    """Tradier batch quote with LKG cache + FMP fallback.

    Precedence per symbol:
      1. Tradier live (batch)
      2. LKG Tradier (from previous successful call, TTL 72 h)
      3. FMP /stable/quote cached response (up to 30 min stale)
      4. Missing → absent from returned dict

    Returns a dict keyed by UPPERCASE symbol.
    Non-US tickers (containing ":") are filtered out before calling Tradier.
    Each returned quote dict contains Tradier-compatible field names plus
    quote_source, quote_cached_at, quote_is_stale, quote_fallback_reason.
    """
    import time as _time_mod
    _now_ts = _time_mod.time()
    _LKG_TTL = 72 * 3600
    _LKG_PFX = "home:wl_tradier_lkg:"

    us_tickers = [t for t in tickers if ":" not in t]
    # OTC tickers are handled via FMP below; split them out now.
    from services.otc_service import split_otc_us as _split_otc_h
    otc_tickers_h, _ = _split_otc_h(tickers)
    if not us_tickers and not otc_tickers_h:
        return {}

    out: dict[str, dict] = {}

    # ── Step 1: Tradier live batch ────────────────────────────────────────
    # Skip live call when the process-wide Tradier rate limiter is saturated.
    # Background jobs (THEME_RS, HL enrichment) consume all 100 slots/min after
    # a cold restart. Rather than blocking for up to 20s and then timing out,
    # skip straight to LKG — results are stale but instant.
    _tradier_saturated = False
    if data_service and getattr(data_service, "tradier", None):
        try:
            from data.tradier_provider import TRADIER_LIMITER as _TL
            _tradier_saturated = _TL.is_saturated()
        except Exception:
            pass

    if data_service and getattr(data_service, "tradier", None) and not _tradier_saturated:
        try:
            from data.tradier_budget import lane as _home_lane
            with _home_lane("quotes"):
                quotes = await data_service.tradier.get_quotes(us_tickers)
            for q in (quotes or []):
                sym = (q.get("symbol") or "").upper()
                last = q.get("last")
                if not sym or not last or last == 0:
                    continue
                row = {
                    **q,
                    "quote_source": "tradier",
                    "quote_cached_at": _now_ts,
                    "quote_is_stale": False,
                    "quote_fallback_reason": None,
                }
                out[sym] = row
                cache.set(f"{_LKG_PFX}{sym}", row, _LKG_TTL)
                cache.set(f"quote:lkg:{sym}", row, _LKG_TTL)   # shared canonical LKG
            print(f"[HOME] Tradier live: {len(out)} of {len(us_tickers)} covered")
        except Exception as exc:
            print(f"[HOME] batch_quotes Tradier error (non-fatal): {exc}")
    elif _tradier_saturated:
        # Tradier rate-limiter is saturated — check the shared per-symbol cache
        # (tradier:quote:sym:{SYM}, 60s TTL) before falling back to LKG.
        # Another page (Portfolio, screener) may have recently refreshed these.
        print(
            f"[HOME_PERF] batch_quotes=skipped_saturated tickers={len(us_tickers)} "
            f"→ per-sym cache then LKG fallback"
        )
        for sym in us_tickers:
            sym_upper = sym.upper()
            raw = cache.get(f"tradier:quote:sym:{sym_upper}")
            if raw and raw.get("last"):
                row = {
                    **raw,
                    "quote_source":          raw.get("quote_source", "tradier"),
                    "quote_cached_at":       _now_ts,
                    "quote_is_stale":        False,
                    "quote_fallback_reason": "per_sym_cache_saturated",
                }
                out[sym_upper] = row
                # Propagate to shared canonical LKG so Portfolio and Watchlist
                # can fall back to this data when their own Tradier calls miss.
                cache.set(f"quote:lkg:{sym_upper}", row, _LKG_TTL)

    # ── Step 2: LKG Tradier for tickers Tradier missed ────────────────────
    _SHARED_LKG_PFX = "quote:lkg:"
    for sym in us_tickers:
        sym_upper = sym.upper()
        if sym_upper in out:
            continue
        # Try path-specific LKG first, then shared canonical LKG
        lkg = cache.get(f"{_LKG_PFX}{sym_upper}") or cache.get(f"{_SHARED_LKG_PFX}{sym_upper}")
        # Accept both raw Tradier shape ("last") and portfolio-normalized shape ("price")
        if lkg and (lkg.get("last") or lkg.get("price")):
            out[sym_upper] = {
                **lkg,
                "quote_is_stale": True,
                "quote_fallback_reason": "tradier_lkg",
                "quote_cached_at": _now_ts,
            }

    # ── Step 3: FMP cached quote for tickers still missing ────────────────
    still_missing = [t for t in us_tickers if t.upper() not in out]
    if still_missing:
        import os, httpx as _httpx
        fmp_key = os.getenv("FMP_API_KEY", "")
        if fmp_key:
            try:
                async with _httpx.AsyncClient(timeout=6.0) as _c:
                    resp = await _c.get(
                        "https://financialmodelingprep.com/stable/quote",
                        params={"symbol": ",".join(still_missing), "apikey": fmp_key},
                    )
                if resp.status_code == 200:
                    for item in resp.json():
                        fsym = (item.get("symbol") or "").upper()
                        price = item.get("price")
                        if not fsym or not price:
                            continue
                        out[fsym] = {
                            "symbol": fsym,
                            "last": price,
                            "change": item.get("change"),
                            "change_percentage": item.get("changesPercentage"),
                            "volume": item.get("volume"),
                            "average_volume": item.get("avgVolume"),
                            "high": item.get("dayHigh"),
                            "low": item.get("dayLow"),
                            "description": item.get("name") or "",
                            "quote_source": "fmp",
                            "quote_cached_at": _now_ts,
                            "quote_is_stale": False,
                            "quote_fallback_reason": "tradier_miss",
                        }
            except Exception as exc:
                print(f"[HOME] batch_quotes FMP fallback error (non-fatal): {exc}")

    # ── Step 4: OTC symbols via FMP ──────────────────────────────────────────
    # OTC:BESIY is never routed through Tradier.  Individual stable/quote calls
    # are made via the shared fmp_governor so OTC traffic coexists with weekly
    # fundamentals and earnings workloads within the 300 calls/min plan limit.
    # Results are stored under the canonical key (OTC:BESIY) in both the return
    # dict and the shared LKG cache.  Fields absent from the FMP response remain
    # absent — no zeros are synthesised for missing values.
    if otc_tickers_h:
        import os as _os_otc
        _otc_fmp_key = _os_otc.getenv("FMP_API_KEY", "")
        if _otc_fmp_key:
            from services.otc_service import otc_to_fmp as _otc_to_fmp_h
            from services.fmp_governor import fmp_governor as _fmp_gov_h
            import asyncio as _aio_otc
            for _otc_can in otc_tickers_h:
                _otc_bare = _otc_to_fmp_h(_otc_can)
                if not _otc_bare:
                    continue
                _ok_h = await _fmp_gov_h.acquire(job_name="otc_quotes_home")
                if not _ok_h:
                    print(f"[HOME] batch_quotes OTC: governor budget hit")
                    break
                try:
                    async with _httpx.AsyncClient(timeout=8.0) as _c_otc:
                        _resp_otc = await _c_otc.get(
                            "https://financialmodelingprep.com/stable/quote",
                            params={"symbol": _otc_bare, "apikey": _otc_fmp_key},
                        )
                    _fmp_gov_h.record_call()
                    if _resp_otc.status_code == 200:
                        _data_otc = _resp_otc.json()
                        if _data_otc and isinstance(_data_otc, list):
                            _item_otc = _data_otc[0]
                            _price_otc = _item_otc.get("price")
                            if _price_otc is not None:
                                _canon_upper = _otc_can.upper()
                                _chg_otc  = _item_otc.get("change")
                                _pct_otc  = _item_otc.get("changePercentage")
                                _vol_otc  = _item_otc.get("volume")
                                _avg_otc  = _item_otc.get("avgVolume")
                                _prev_otc = _item_otc.get("previousClose")
                                if _prev_otc is None and _price_otc is not None and _chg_otc is not None:
                                    try:
                                        _prev_otc = round(float(_price_otc) - float(_chg_otc), 4)
                                    except (TypeError, ValueError):
                                        _prev_otc = None
                                _row_otc = {
                                    "symbol":            _canon_upper,
                                    "last":              _price_otc,
                                    "change":            _chg_otc,
                                    "change_percentage": _pct_otc,
                                    "volume":            _vol_otc,
                                    "average_volume":    _avg_otc,
                                    "high":              _item_otc.get("dayHigh"),
                                    "low":               _item_otc.get("dayLow"),
                                    "description":       _item_otc.get("name") or "",
                                    "previous_close":    _prev_otc,
                                    "quote_source":      "fmp_otc",
                                    "quote_cached_at":   _now_ts,
                                    "quote_is_stale":    False,
                                    "quote_fallback_reason": None,
                                }
                                out[_canon_upper] = _row_otc
                                # Write to shared canonical LKG under canonical key
                                cache.set(f"quote:lkg:{_canon_upper}", _row_otc, _LKG_TTL)
                except Exception as _exc_otc:
                    print(f"[HOME] batch_quotes OTC FMP error for {_otc_bare}: {_exc_otc}")

    return out


def _snapshot_row(
    symbol: str,
    quote: dict,
    options_index: dict[str, dict],
    asset_type: str | None = None,
    csv_row: dict | None = None,
) -> dict:
    # Accept both raw Tradier field names (last/change_percentage/average_volume)
    # and portfolio-normalised names (price/change_pct/avg_volume) so that any
    # quote:lkg entry — regardless of which page wrote it — renders correctly.
    last    = quote.get("last") or quote.get("price")
    chg_pct = quote.get("change_percentage") or quote.get("change_pct")
    vol     = quote.get("volume")
    avg_vol = quote.get("average_volume") or quote.get("avg_volume")
    vol_ratio = (
        round(vol / avg_vol, 2) if vol and avg_vol and avg_vol > 0
        else quote.get("relative_volume") or quote.get("rel_volume")
    )
    opts = options_index.get(symbol.upper(), {})

    # RSI from watchlist csv_data (watchlist rows only); rel_vol starts from live quote.
    rsi: float | None = None
    effective_rel_vol = vol_ratio  # live quote vol ratio; upgraded below if csv available
    if csv_row:
        rsi = _parse_float(
            csv_row.get("Relative Strength Index (RSI)") or csv_row.get("RSI")
        )
        rel_vol_csv = _parse_pct(csv_row.get("Relative Volume"))
        rel_vol_x = (rel_vol_csv / 100.0) if rel_vol_csv is not None else None
        # Prefer live vol_ratio; fall back to CSV rel_vol when live is unavailable
        if effective_rel_vol is None:
            effective_rel_vol = rel_vol_x
    # Always compute signal_label — works with rsi=None (portfolio rows included)
    signal_label = _signal_label(chg_pct, rsi, effective_rel_vol, opts.get("primary_signal")) or None

    row = {
        "symbol": symbol.upper(),
        "current_price": last,
        "change_1d_pct": chg_pct,
        "volume_vs_avg": vol_ratio,
        "options_signal": opts.get("primary_signal"),
        "rsi": round(rsi, 1) if rsi is not None else None,
        "signal_label": signal_label,
        # Quote provenance — lets frontend distinguish live vs LKG/stale data
        "quote_source": quote.get("quote_source"),        # "tradier" | "fmp" | None
        "quote_is_stale": quote.get("quote_is_stale"),    # True = LKG / not live
        "quote_fallback_reason": quote.get("quote_fallback_reason"),  # "tradier_lkg" | None
    }
    if asset_type is not None:
        row["asset_type"] = asset_type
    return row


def _overlay_portfolio_quotes(snapshot: list[dict]) -> list[dict]:
    """Re-overlay quote fields for each portfolio row from the canonical cache chain.

    Called at every cache-hit path (60 s TTL and 4 h LKG) so the Home Portfolio
    Snapshot always reflects the same quote data as the Portfolio page — even when
    the cached payload is up to 4 hours old.

    Canonical chain (same precedence as Portfolio page):
      1. tradier:quote:sym:{SYM}      — 60 s TTL per-symbol cache (freshest)
      2. quote:lkg:{SYM}              — shared 72 h LKG written by all Tradier callers
      3. portfolio:tradier_lkg:{SYM}  — Portfolio page 72 h LKG
      4. Embedded row fields          — last resort; marked stale + reason

    Pure synchronous cache reads — no I/O, no Tradier calls, no new cache keys.
    Normalises field names so entries written by any caller (Tradier raw or
    portfolio-normalised shape) render correctly.
    """
    import time as _t
    _now = _t.time()
    out: list[dict] = []
    for row in (snapshot or []):
        sym = (row.get("symbol") or "").upper()
        if not sym:
            out.append(row)
            continue

        # Walk canonical chain — first non-None entry with a usable price wins
        q: dict | None = None
        for cache_key in (
            f"tradier:quote:sym:{sym}",
            f"quote:lkg:{sym}",
            f"portfolio:tradier_lkg:{sym}",
        ):
            entry = cache.get(cache_key)
            if entry and (entry.get("last") or entry.get("price")):
                q = entry
                break

        if q:
            last    = q.get("last") or q.get("price")
            chg_pct = q.get("change_percentage") or q.get("change_pct")
            vol     = q.get("volume")
            avg_vol = q.get("average_volume") or q.get("avg_volume")
            vol_ratio = (
                round(vol / avg_vol, 2)
                if vol and avg_vol and avg_vol > 0
                else q.get("relative_volume") or q.get("rel_volume")
            )
            out.append({
                **row,
                "current_price":         last,
                "change_1d_pct":         chg_pct,
                "volume_vs_avg":         vol_ratio,
                "quote_source":          q.get("quote_source") or "tradier",
                "quote_is_stale":        bool(q.get("quote_is_stale")),
                "quote_fallback_reason": q.get("quote_fallback_reason"),
                "quote_fetched_at":      _now,
            })
        else:
            # No canonical data available — surface embedded fields as stale
            out.append({
                **row,
                "quote_is_stale":        True,
                "quote_fallback_reason": "home_lkg_no_canonical",
                "quote_fetched_at":      _now,
            })
    return out


# ── Watchlist snapshot ─────────────────────────────────────────────────────

async def _fetch_watchlist_data(
    watchlist: dict | None,
    data_service,
    options_index: dict[str, dict],
    csv_index: dict[str, dict],
) -> dict:
    """
    Fetch quotes for all watchlist tickers and return:
      - snapshot: all rows (for watchlist_snapshot panel)
      - highlighted: top-ranked rows (for highlighted_companies panel)

    Uses the FULL watchlist (tickers + csv_data) loaded by _load_primary_watchlist().
    Single Tradier batch-quote call, cached.
    """
    empty = {"snapshot": [], "highlighted": []}
    if not watchlist:
        return empty

    tickers = [t for t in (watchlist.get("tickers") or []) if ":" not in t]
    if not tickers:
        return empty

    quote_by_sym = await _batch_quotes(tickers, data_service)

    snapshot = [
        _snapshot_row(
            t,
            quote_by_sym.get(t.upper(), {}),
            options_index,
            csv_row=csv_index.get(t.upper()),
        )
        for t in tickers
    ]

    highlighted = _rank_watchlist_rows(
        tickers, quote_by_sym, csv_index, options_index, limit=10
    )

    return {"snapshot": snapshot, "highlighted": highlighted}


# ── Portfolio snapshot ─────────────────────────────────────────────────────

async def _fetch_portfolio_snapshot(
    data_service,
    options_index: dict[str, dict],
) -> list[dict]:
    """
    Compact snapshot for the user's portfolio holdings.
    Privacy: NEVER exposes shares, avg_cost, cost_basis, or market value.

    Uses portfolio_store.load_active_holdings() — the same authoritative
    source as the Portfolio page and every other service in the backend
    (Neon DB primary → data/portfolio/active_holdings.json fallback).
    """
    try:
        from data.portfolio_store import load_active_holdings as _load_holdings
        holdings = await asyncio.to_thread(_load_holdings)
        if not isinstance(holdings, list) or not holdings:
            return []
    except Exception as exc:
        print(f"[HOME] portfolio read error (non-fatal): {exc}")
        return []

    seen: dict[str, str] = {}
    for h in holdings:
        if not isinstance(h, dict):
            continue
        sym = (h.get("ticker") or "").upper().strip()
        if not sym or ":" in sym:
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


# ── Sub-theme performance ──────────────────────────────────────────────────

async def _fetch_sub_theme_performance(
    data_service,
    options_index: dict[str, dict],
    watchlist_quotes: dict[str, dict] | None = None,
) -> list[dict]:
    """
    Compute Theme Performance for the Home page directly from THEME_RS_UNIVERSE.

    Only themes with proxy_type == "custom" are included — these are the manually
    curated ticker-basket themes.  Adding or removing a theme from the Home page
    is done entirely in theme_rs_universe.py; no changes needed in this file.

    Data sources (in priority order):
      1. Watchlist quotes (already fetched — zero extra API calls for those tickers)
      2. Tradier batch quote for any missing tickers
      3. Options LKG cache fallback (price_change_pct available for ~70 tickers)
    """
    from services.theme_rs_universe import THEME_RS_UNIVERSE

    wq = watchlist_quotes or {}

    # Pull every custom-type theme from the universe
    effective_tickers: dict[str, list[str]] = {
        entry["display_name"]: [
            t for t in entry["candidate_symbols"] if ":" not in t
        ]
        for entry in THEME_RS_UNIVERSE.values()
        if entry.get("proxy_type") == "custom"
    }

    # All US tickers needed across all custom themes
    all_needed = sorted({sym for tickers in effective_tickers.values() for sym in tickers})

    # Find tickers not already in watchlist quotes
    missing = [sym for sym in all_needed if sym not in wq]

    # Fetch missing tickers in one Tradier batch call (cached via per-ticker cache)
    extra_quotes: dict[str, dict] = {}
    if missing and data_service and getattr(data_service, "tradier", None):
        _ts_bq = time.time()
        try:
            extra_quotes = await asyncio.wait_for(
                _batch_quotes(missing, data_service), timeout=4.0
            )
            print(f"[HOME_PERF] section=sub_theme_batch_quotes status=ok elapsed_ms={round((time.time()-_ts_bq)*1000)} tickers={len(missing)}")
        except asyncio.TimeoutError:
            print(f"[HOME_PERF] section=sub_theme_batch_quotes status=timeout elapsed_ms={round((time.time()-_ts_bq)*1000)} tickers={len(missing)}")
        except Exception as exc:
            print(f"[HOME_PERF] section=sub_theme_batch_quotes status=error elapsed_ms={round((time.time()-_ts_bq)*1000)} exc={type(exc).__name__}")

    # Merge quote sources: watchlist quotes take priority
    all_quotes = {**extra_quotes, **wq}

    theme_rows: list[dict] = []

    for sub_theme, us_only in effective_tickers.items():
        changes: list[float] = []
        leaders: list[tuple[float, str]] = []

        for sym in us_only:
            # Get 1D change from quotes first, then options_index as fallback
            q = all_quotes.get(sym)
            chg = None
            if q:
                chg = q.get("change_percentage")
            if chg is None:
                chg = (options_index.get(sym) or {}).get("price_change_pct")

            if chg is not None:
                try:
                    chg = float(chg)
                    changes.append(chg)
                    leaders.append((chg, sym))
                except Exception:
                    pass

        if not changes:
            continue

        avg_1d = round(sum(changes) / len(changes), 2)
        breadth = round(100.0 * sum(1 for c in changes if c > 0) / len(changes), 0)

        # Momentum score: weighted by avg_1d and breadth
        momentum_score = round(avg_1d * 0.6 + breadth * 0.4 / 10.0, 1)

        # Top 3 leaders by 1D change
        leaders.sort(key=lambda x: x[0], reverse=True)
        top_leaders = [sym for _, sym in leaders[:3]]

        # Pattern summary
        if avg_1d >= 3.0 and breadth >= 70:
            pattern = "broad strength — most names moving"
        elif avg_1d >= 1.5 and breadth >= 50:
            pattern = "positive breadth, leaders pulling"
        elif avg_1d >= 0 and breadth >= 40:
            pattern = "mixed — modest positive bias"
        elif avg_1d < 0 and breadth < 40:
            pattern = "broad weakness"
        else:
            pattern = "consolidating"

        theme_rows.append({
            "sub_theme": sub_theme,
            "avg_change_1d": avg_1d,
            "avg_change_7d": None,   # not yet available from this source
            "leader_symbols": top_leaders,
            "leader_count": len(changes),
            "breadth_score": breadth,
            "momentum_score": momentum_score,
            "pattern_summary": pattern,
        })

    # Sort by avg_change_1d descending — strongest 1D momentum first
    theme_rows.sort(key=lambda x: x["avg_change_1d"], reverse=True)
    return theme_rows


# ── Latest News ────────────────────────────────────────────────────────────

async def _fetch_latest_news(data_service, limit: int = 8) -> list[dict]:
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

async def _bg_rebuild_home(data_service, macro_provider) -> None:
    """Single-flight background rebuild — called after serving LKG stale data."""
    global _HOME_REBUILD_ACTIVE
    if _HOME_REBUILD_ACTIVE:
        return
    _HOME_REBUILD_ACTIVE = True
    try:
        await build_home_dashboard(
            data_service=data_service, macro_provider=macro_provider, force=True
        )
    except Exception as _bg_exc:
        print(f"[HOME_PERF] bg_rebuild error={type(_bg_exc).__name__}: {_bg_exc}")
    finally:
        _HOME_REBUILD_ACTIVE = False


async def build_home_dashboard(
    *,
    data_service,
    macro_provider,
    watchlists_loader=None,  # kept for signature compat, no longer used
    force: bool = False,
) -> dict:
    """Main entry — returns the normalized Home payload."""
    if not force:
        cached = cache.get(_HOME_CACHE_KEY)
        if cached is not None:
            payload = {**cached, "from_cache": True}
            # Re-overlay portfolio quote fields from canonical chain so the 60 s
            # cached snapshot always shows the same prices as the Portfolio page.
            if payload.get("portfolio_snapshot"):
                payload["portfolio_snapshot"] = _overlay_portfolio_quotes(
                    payload["portfolio_snapshot"]
                )
            return payload
        # Hot cache miss — serve LKG immediately and rebuild in background
        lkg = cache.get(_HOME_CACHE_LKG_KEY)
        if lkg is not None:
            asyncio.create_task(_bg_rebuild_home(data_service, macro_provider))
            print("[HOME_PERF] endpoint=/api/home/dashboard cache=lkg bg_rebuild=triggered")
            payload = {**lkg, "from_cache": True, "cache_status": "lkg"}
            # Re-overlay portfolio quote fields — LKG can be up to 4 h old;
            # canonical chain (tradier:quote:sym → quote:lkg → portfolio:tradier_lkg)
            # always has the freshest available price.
            if payload.get("portfolio_snapshot"):
                payload["portfolio_snapshot"] = _overlay_portfolio_quotes(
                    payload["portfolio_snapshot"]
                )
            return payload

    t0 = time.time()
    print("[HOME_PERF] endpoint=/api/home/dashboard cache=miss rebuild=start")

    # ── Tasks (all optional / fail-soft) ──────────────────────────────
    tasks: dict[str, Optional[asyncio.Task]] = {}

    # Per-task hard timeouts — prevents any single hung provider from blocking the page.
    # asyncio.TimeoutError is caught in the collect loop below and logged as [HOME_PERF].
    _t_task_start: dict[str, float] = {}

    def _task(name: str, coro, timeout: float) -> asyncio.Task:
        _t_task_start[name] = time.time()
        return asyncio.create_task(asyncio.wait_for(coro, timeout=timeout))

    if macro_provider:
        tasks["macro"] = _task("macro", macro_provider.get_dashboard(), 8.0)

    try:
        from services.sector_rotation.service import get_dashboard as _sr_get
        tasks["sector"] = _task("sector", _sr_get(include_analysis=False), 6.0)
    except Exception:
        tasks["sector"] = None

    if data_service and getattr(data_service, "fmp", None):
        tasks["movers"] = _task("movers", data_service.fmp.get_gainers_losers(), 10.0)
    if data_service and getattr(data_service, "finviz", None):
        tasks["fv_gainers"] = _task("fv_gainers", data_service.finviz.get_screener_results("ta_topgainers"), 5.0)
        tasks["fv_losers"]  = _task("fv_losers",  data_service.finviz.get_screener_results("ta_toplosers"),  5.0)
    if data_service and getattr(data_service, "fear_greed", None):
        tasks["fg"] = _task("fg", data_service.fear_greed.get_fear_greed_index(), 5.0)
    if data_service and getattr(data_service, "stocktwits", None):
        tasks["trending"] = _task("trending", data_service.stocktwits.get_trending(), 5.0)

    tasks["news"] = _task("news", _fetch_latest_news(data_service), 8.0)

    # ── Primary watchlist — load FULL data (fix: was metadata-only) ────
    watchlist: dict | None = None
    try:
        watchlist = await asyncio.wait_for(_load_primary_watchlist(), timeout=4.0)
    except asyncio.TimeoutError:
        print("[HOME_PERF] section=load_watchlist status=timeout")
    except Exception as exc:
        print(f"[HOME] watchlist load error (non-fatal): {exc}")

    csv_index = _build_csv_index(watchlist)

    # ── Options LKG index — pure in-memory read ────────────────────────
    options_index: dict[str, dict] = {}
    try:
        options_index = await asyncio.to_thread(_build_options_lkg_index)
    except Exception as exc:
        print(f"[HOME] options_index build error (non-fatal): {exc}")

    # ── Watchlist data (quotes + highlighted) — one Tradier batch call ─
    tasks["watchlist_data"] = _task(
        "watchlist_data",
        _fetch_watchlist_data(watchlist, data_service, options_index, csv_index),
        8.0,
    )

    # ── Portfolio snapshot ─────────────────────────────────────────────
    tasks["portfolio_snap"] = _task(
        "portfolio_snap",
        _fetch_portfolio_snapshot(data_service, options_index),
        8.0,
    )

    # ── Unusual options flows — served from fast-loop cache (pure read) ─
    # _home_options_fast_loop() in main.py refreshes every ~90s.
    # This call is instant (no Tradier calls). Returns (flows, meta).
    tasks["unusual_options"] = _task(
        "unusual_options",
        _fetch_unusual_options_live(data_service),
        3.0,
    )

    # ── Await all tasks ────────────────────────────────────────────────
    results: dict[str, Any] = {}
    for name, task in tasks.items():
        if task is None:
            results[name] = None
            continue
        try:
            results[name] = await task
            _elapsed = round((time.time() - _t_task_start.get(name, t0)) * 1000)
            print(f"[HOME_PERF] section={name} status=ok elapsed_ms={_elapsed}")
        except asyncio.TimeoutError:
            results[name] = None
            _elapsed = round((time.time() - _t_task_start.get(name, t0)) * 1000)
            print(f"[HOME_PERF] section={name} status=timeout elapsed_ms={_elapsed}")
        except Exception as exc:
            results[name] = exc
            _elapsed = round((time.time() - _t_task_start.get(name, t0)) * 1000)
            print(f"[HOME_PERF] section={name} status=error elapsed_ms={_elapsed} exc={type(exc).__name__}")

    macro_raw = _safe(results.get("macro"), {}) or {}
    sector_dash = _safe(results.get("sector"), None)
    fmp_movers = _safe(results.get("movers"), {}) or {}
    fv_gainers = _safe(results.get("fv_gainers"), []) or []
    fv_losers = _safe(results.get("fv_losers"), []) or []
    fg_equity = _safe(results.get("fg"), {}) or {}
    trending = _safe(results.get("trending"), []) or []
    latest_news = _safe(results.get("news"), []) or []
    watchlist_data = _safe(results.get("watchlist_data"), {"snapshot": [], "highlighted": []}) or {"snapshot": [], "highlighted": []}
    portfolio_snapshot = _safe(results.get("portfolio_snap"), []) or []

    watchlist_snapshot = watchlist_data.get("snapshot") or []
    highlighted_companies = watchlist_data.get("highlighted") or []

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

    trending_on_x: dict = {
        "generated_at": None, "top_tickers": [], "key_themes": [],
        "notable_accounts": [], "is_stale": True, "refresh_in_progress": False,
        "available": False,
    }
    try:
        from services.x_consensus_cache import get_weekly_snapshot
        trending_on_x = await get_weekly_snapshot(data_service=data_service, allow_refresh=True)
    except Exception as exc:
        print(f"[HOME] Trending on X snapshot failed soft: {exc}")

    # Unpack (flows, meta) tuple from _fetch_unusual_options_live
    _uof_result = _safe(results.get("unusual_options"), ([], {}))
    if isinstance(_uof_result, tuple) and len(_uof_result) == 2:
        unusual_options_flows, unusual_options_meta = _uof_result
    else:
        unusual_options_flows = _uof_result if isinstance(_uof_result, list) else []
        unusual_options_meta = {"data_state": "no_data_yet", "source": "none"}
    unusual_options_flows = unusual_options_flows or []

    # ── Sub-theme performance ─────────────────────────────────────────────
    # Reuse any quotes already fetched by the watchlist_data task via LKG cache.
    # _fetch_sub_theme_performance calls _batch_quotes internally, which
    # fast-falls to LKG when Tradier is saturated — no extra serial call needed.
    watchlist_quote_by_sym: dict[str, dict] = {}

    _ts_stp = time.time()
    try:
        sub_theme_performance = await asyncio.wait_for(
            _fetch_sub_theme_performance(
                data_service, options_index, watchlist_quotes=watchlist_quote_by_sym
            ),
            timeout=6.0,
        )
        print(f"[HOME_PERF] section=sub_theme_performance status=ok elapsed_ms={round((time.time()-_ts_stp)*1000)}")
    except asyncio.TimeoutError:
        sub_theme_performance = []
        print(f"[HOME_PERF] section=sub_theme_performance status=timeout elapsed_ms={round((time.time()-_ts_stp)*1000)}")
    except Exception as _stp_exc:
        sub_theme_performance = []
        print(f"[HOME_PERF] section=sub_theme_performance status=error elapsed_ms={round((time.time()-_ts_stp)*1000)} exc={type(_stp_exc).__name__}")

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "greeting": {
            "text": _greeting_for_now(),
            "market": _us_market_status(),
        },
        "ticker_strip": _extract_ticker_strip(macro_raw),
        "macro_cards": _extract_macro_cards(macro_raw),
        # Watchlist-sourced, signal-ranked — replaces old Stocktwits fallback
        "highlighted_companies": highlighted_companies,
        # Broad sector view — kept for compat
        "theme_performance": _extract_theme_performance(sector_dash),
        # New: niche sub-theme alpha view
        "sub_theme_performance": sub_theme_performance,
        "trending_ideas": _extract_trending_ideas(scan_lite),
        "movers": _extract_movers(merged_movers),
        "trending_on_x": trending_on_x,
        "fear_greed": _extract_fear_greed(fg_equity, macro_raw),
        "latest_news": latest_news,
        "unusual_options_flows": unusual_options_flows,
        "unusual_options_meta": unusual_options_meta,
        "watchlist_snapshot": watchlist_snapshot,
        "portfolio_snapshot": portfolio_snapshot,
        "section_status": {
            "macro": "ok" if not isinstance(results.get("macro"), Exception) and results.get("macro") else "unavailable",
            "sector": "ok" if not isinstance(results.get("sector"), Exception) and results.get("sector") else "unavailable",
            "movers": "ok" if (merged_movers.get("gainers") or merged_movers.get("losers")) else "unavailable",
            "fear_greed": "ok" if not isinstance(results.get("fg"), Exception) and results.get("fg") else "unavailable",
            "trending": "ok" if not isinstance(results.get("trending"), Exception) and results.get("trending") else "unavailable",
            "trending_on_x": "ok" if trending_on_x.get("available") else ("refreshing" if trending_on_x.get("refresh_in_progress") else "unavailable"),
            "latest_news": "ok" if latest_news else "unavailable",
            "unusual_options_flows": (
                unusual_options_meta.get("data_state", "no_data_yet")
                if isinstance(unusual_options_meta, dict)
                else ("ok" if unusual_options_flows else "no_data_yet")
            ),
            "watchlist_snapshot": "ok" if watchlist_snapshot else "unavailable",
            "highlighted_companies": "ok" if highlighted_companies else "unavailable",
            "portfolio_snapshot": "ok" if portfolio_snapshot else "unavailable",
            "sub_theme_performance": "ok" if sub_theme_performance else "unavailable",
        },
        "timing": {"total_seconds": round(time.time() - t0, 2)},
        "from_cache": False,
    }

    cache.set(_HOME_CACHE_KEY,     payload, _HOME_CACHE_TTL)
    cache.set(_HOME_CACHE_LKG_KEY, payload, _HOME_CACHE_LKG_TTL)
    print(f"[HOME_PERF] endpoint=/api/home/dashboard total_ms={round((time.time()-t0)*1000)} cache=written")
    return payload


# ── Home Movers by Category ────────────────────────────────────────────────
#
# Drives the Home page "Top Gainers / Top Losers" category toggle.
# Five categories: stocks | etfs | commodities | crypto | all
#
# Sources:
#   stocks      → FMP biggest-gainers/losers  (same as existing Home movers)
#   etfs        → FMP get_etf_quotes on curated ~30-ETF universe
#   commodities → Hyperliquid get_all_perps() filtered to commodity preset
#   crypto      → CMC get_listings_latest(250), rank by percent_change_24h
#   all         → run all 4 in parallel, merge + rank globally by % move
#
# Cached per category for 5 minutes (upstream caches do the heavy lifting).
# ─────────────────────────────────────────────────────────────────────────────

_MOVERS_CACHE_TTL = 300  # 5 minutes per category

# ── ETF universe (sector + broad + thematic + fixed income) ───────────────
_ETF_UNIVERSE: list[str] = [
    # Sector SPDR (11)
    "XLC", "XLY", "XLP", "XLE", "XLF", "XLV", "XLI", "XLB", "XLRE", "XLK", "XLU",
    # Broad market
    "SPY", "QQQ", "IWM", "DIA",
    # Thematic / factor
    "SMH", "SOXX", "XBI", "IBB", "HACK", "URA", "ARKK",
    # Fixed income
    "TLT", "HYG",
    # International
    "EEM", "EFA",
    # Commodity ETFs
    "GLD", "SLV", "USO", "UNG",
]

_ETF_NAMES: dict[str, str] = {
    "XLC": "Communication Services ETF",
    "XLY": "Consumer Discretionary ETF",
    "XLP": "Consumer Staples ETF",
    "XLE": "Energy ETF",
    "XLF": "Financials ETF",
    "XLV": "Health Care ETF",
    "XLI": "Industrials ETF",
    "XLB": "Materials ETF",
    "XLRE": "Real Estate ETF",
    "XLK": "Technology ETF",
    "XLU": "Utilities ETF",
    "SPY": "S&P 500 ETF",
    "QQQ": "Nasdaq 100 ETF",
    "IWM": "Russell 2000 ETF",
    "DIA": "Dow Jones ETF",
    "SMH": "Semiconductor ETF",
    "SOXX": "Semiconductor ETF",
    "XBI": "Biotech ETF",
    "IBB": "Biotech ETF",
    "HACK": "Cybersecurity ETF",
    "URA": "Uranium ETF",
    "ARKK": "ARK Innovation ETF",
    "TLT": "20+ Year Treasury ETF",
    "HYG": "High Yield Bond ETF",
    "EEM": "Emerging Markets ETF",
    "EFA": "EAFE ETF",
    "GLD": "Gold ETF",
    "SLV": "Silver ETF",
    "USO": "Crude Oil ETF",
    "UNG": "Natural Gas ETF",
}

# Commodity category — HIP-3 DEX display_name → (user-facing symbol, nice name).
# HL spells the metal "ALUMINIUM"; we map it to user-facing "ALUMINUM".
# "WTI" from cash: DEX is shown as "WTOIL" per the user's requested label.
_HL_COMMODITY_PRESET: dict[str, tuple[str, str]] = {
    "GOLD":      ("GOLD",      "Gold"),
    "SILVER":    ("SILVER",    "Silver"),
    "COPPER":    ("COPPER",    "Copper"),
    "PLATINUM":  ("PLATINUM",  "Platinum"),
    "PALLADIUM": ("PALLADIUM", "Palladium"),
    "BRENTOIL":  ("BRENTOIL",  "Brent Oil"),
    "NATGAS":    ("NATGAS",    "Natural Gas"),
    "WHEAT":     ("WHEAT",     "Wheat"),
    "ALUMINIUM": ("ALUMINUM",  "Aluminum"),
    "URNM":      ("URNM",      "Uranium Miners"),
    "SOY":       ("SOY",       "Soybean"),
    "OIL":       ("OIL",       "Crude Oil"),
    "GAS":       ("GAS",       "Gas"),
    "USOIL":     ("USOIL",     "US Oil"),
    "WTI":       ("WTOIL",     "WTI Crude"),
}

# DEX preference order for deduplication (same commodity on multiple DEXes)
_COMMODITY_DEX_PRIORITY = ["xyz", "km", "flx", "cash", "vntl", "hyna"]


# ── Normalisation helper ───────────────────────────────────────────────────

def _norm_mover_row(
    symbol: str,
    name: str | None,
    asset_type: str,
    price: float | None,
    change_percent: float | None,
    source: str,
    volume_24h: float | None = None,
    market_cap: float | None = None,
) -> dict:
    """Produce a stable, frontend-ready row for any category."""
    if change_percent is not None:
        label = f"{change_percent:+.2f}%"
    else:
        label = ""
    return {
        "symbol": symbol,
        "name": name or symbol,
        "asset_type": asset_type,
        "price": price,
        "change_percent": change_percent,
        "change_label": label,
        "source": source,
        "volume_24h": volume_24h,
        "market_cap": market_cap,
    }


def _safe_float(v) -> float | None:
    try:
        return float(v) if v is not None else None
    except Exception:
        return None


# ── Category helpers ───────────────────────────────────────────────────────

async def _movers_stocks(data_service) -> dict:
    """
    Stocks — FMP biggest-gainers / biggest-losers.
    Identical source family to existing Home dashboard movers.
    """
    key = "home:movers:stocks:v1"
    cached = cache.get(key)
    if cached is not None:
        return {**cached, "from_cache": True}

    gainers_raw: list = []
    losers_raw: list = []
    try:
        if data_service and getattr(data_service, "fmp", None):
            result = await data_service.fmp.get_gainers_losers()
            gainers_raw = (result or {}).get("gainers") or []
            losers_raw  = (result or {}).get("losers") or []
    except Exception as exc:
        print(f"[HOME_MOVERS] stocks fmp error (non-fatal): {exc}")

    def _to_row(r: dict) -> dict | None:
        sym = (r.get("ticker") or r.get("symbol") or "").upper()
        if not sym:
            return None
        chg_str = r.get("change_pct") or r.get("change") or ""
        chg_pct = _safe_float(str(chg_str).replace("%", "").replace("+", "").replace(",", "").strip() or None)
        return _norm_mover_row(
            symbol=sym,
            name=r.get("company") or r.get("name") or sym,
            asset_type="stock",
            price=_safe_float(r.get("price")),
            change_percent=chg_pct,
            source=r.get("source") or "fmp_gainers",
        )

    gainers = [row for r in gainers_raw[:8] if (row := _to_row(r)) is not None]
    losers  = [row for r in losers_raw[:8]  if (row := _to_row(r)) is not None]

    payload = {
        "category": "stocks",
        "gainers": gainers,
        "losers": losers,
        "updated_at": None,
        "from_cache": False,
    }
    cache.set(key, payload, _MOVERS_CACHE_TTL)
    return payload


async def _movers_etfs(data_service) -> dict:
    """
    ETFs — FMP quotes on a curated ~30-ETF universe, ranked by % change.
    Reuses FMP get_etf_quotes() which already caches individual quote calls.
    """
    key = "home:movers:etfs:v1"
    cached = cache.get(key)
    if cached is not None:
        return {**cached, "from_cache": True}

    quotes: dict = {}
    try:
        if data_service and getattr(data_service, "fmp", None):
            quotes = await data_service.fmp.get_etf_quotes(_ETF_UNIVERSE) or {}
    except Exception as exc:
        print(f"[HOME_MOVERS] etf fmp error (non-fatal): {exc}")

    rows: list[dict] = []
    for sym, q in quotes.items():
        chg_pct = _safe_float(q.get("change_pct"))
        if chg_pct is None:
            continue
        rows.append(_norm_mover_row(
            symbol=sym.upper(),
            name=_ETF_NAMES.get(sym.upper(), f"{sym} ETF"),
            asset_type="etf",
            price=_safe_float(q.get("price")),
            change_percent=chg_pct,
            source="fmp_etf",
            market_cap=_safe_float(q.get("market_cap")),
        ))

    rows.sort(key=lambda x: x["change_percent"], reverse=True)
    gainers = rows[:8]
    losers  = list(reversed(rows))[:8]

    payload = {
        "category": "etfs",
        "gainers": gainers,
        "losers": losers,
        "updated_at": None,
        "from_cache": False,
    }
    cache.set(key, payload, _MOVERS_CACHE_TTL)
    return payload


_MOVERS_COMM_LKG_KEY = "home:movers:commodities:lkg"
_MOVERS_COMM_LKG_TTL = 7 * 24 * 3600  # 7 days — survive long uptime gaps


async def _movers_commodities(data_service) -> dict:
    """
    Commodities — live HIP-3 DEX assets from the Hyperliquid screener state.

    Cache strategy (mirrors the HL screener page "never show empty" contract):
      1. Hot cache  (home:movers:commodities:v1, 5 min TTL) — served immediately.
      2. Live read  — scan state.assets for HIP-3 commodity coins.  No is_ready
                      guard: the HIP-3 disk cache is preloaded during boot so
                      assets are present before is_ready flips True.
      3. LKG cache  (home:movers:commodities:lkg, 7-day TTL) — written every
                      time a live read succeeds; served when state has no data
                      yet (e.g. fresh production deploy before first enrichment).

    De-duplication: same commodity on multiple DEXes resolved by
    _COMMODITY_DEX_PRIORITY (xyz > km > flx > cash > vntl > hyna).
    """
    hot_key = "home:movers:commodities:v1"
    lkg_key = _MOVERS_COMM_LKG_KEY

    # 1. Hot cache
    cached = cache.get(hot_key)
    if cached is not None:
        return {**cached, "from_cache": True}

    # 2. Live read from HL state — no is_ready guard
    rows: list[dict] = []
    try:
        state = _hl_get_state()
        if state is not None:
            best: dict[str, tuple[int, object]] = {}
            for coin, asset in state.assets.items():
                if ":" not in coin:
                    continue
                dn = asset.display_name.upper() if asset.display_name else ""
                if dn not in _HL_COMMODITY_PRESET:
                    continue
                if asset.mark_px is None or asset.pct_change_24h is None:
                    continue
                dex_prefix = coin.split(":")[0]
                priority = (
                    _COMMODITY_DEX_PRIORITY.index(dex_prefix)
                    if dex_prefix in _COMMODITY_DEX_PRIORITY
                    else 99
                )
                if dn not in best or priority < best[dn][0]:
                    best[dn] = (priority, asset)

            for dn, (_, asset) in best.items():
                user_sym, nice_name = _HL_COMMODITY_PRESET[dn]
                rows.append(_norm_mover_row(
                    symbol=user_sym,
                    name=nice_name,
                    asset_type="commodity",
                    price=asset.mark_px,
                    change_percent=asset.pct_change_24h,
                    source="hyperliquid",
                    volume_24h=asset.day_ntl_vlm,
                ))
    except Exception as exc:
        print(f"[HOME_MOVERS] commodity hl state error (non-fatal): {exc}")

    if rows:
        rows.sort(key=lambda x: x["change_percent"], reverse=True)
        payload = {
            "category": "commodities",
            "gainers": rows[:8],
            "losers": list(reversed(rows))[:8],
            "updated_at": None,
            "from_cache": False,
        }
        cache.set(hot_key, payload, _MOVERS_CACHE_TTL)   # 5-min hot
        cache.set(lkg_key, payload, _MOVERS_COMM_LKG_TTL)  # 7-day LKG
        return payload

    # 3. LKG fallback — serve last known good rather than empty
    lkg = cache.get(lkg_key)
    if lkg is not None:
        print("[HOME_MOVERS] commodities: HL state not ready — serving LKG data")
        return {**lkg, "from_cache": True, "stale": True}

    # 4. Truly empty (fresh deploy, no enrichment yet)
    return {
        "category": "commodities",
        "gainers": [],
        "losers": [],
        "updated_at": None,
        "from_cache": False,
    }


_MOVERS_CRYPTO_LKG_KEY = "home:movers:crypto:lkg"
_MOVERS_CRYPTO_LKG_TTL = 7 * 24 * 3600  # 7 days


async def _movers_crypto(data_service) -> dict:
    """
    Crypto — CMC top 500 by market cap, ranked by percent_change_24h.

    Cache strategy (mirrors 'never empty' contract):
      1. Hot cache  (home:movers:crypto:v1, 5 min TTL) — served immediately.
      2. Live CMC call — fetches top-500 listings.
      3. LKG cache  (home:movers:crypto:lkg, 7-day TTL) — written every time a
                      live read succeeds; served when CMC is unavailable so the
                      tab never shows empty.

    Never caches empty results so a CMC error can't lock out data for 5 min.
    """
    hot_key = "home:movers:crypto:v1"
    lkg_key = _MOVERS_CRYPTO_LKG_KEY

    # 1. Hot cache
    cached = cache.get(hot_key)
    if cached is not None:
        return {**cached, "from_cache": True}

    # 2. Live CMC read
    listings: list = []
    try:
        if data_service and getattr(data_service, "cmc", None):
            listings = await data_service.cmc.get_listings_latest(limit=500) or []
    except Exception as exc:
        print(f"[HOME_MOVERS] crypto cmc error (non-fatal): {exc}")

    rows: list[dict] = []
    for coin in listings:
        if not isinstance(coin, dict):
            continue
        sym  = (coin.get("symbol") or "").upper()
        name = coin.get("name") or sym
        q    = (coin.get("quote") or {}).get("USD") or {}
        chg_pct = _safe_float(q.get("percent_change_24h"))
        if chg_pct is None:
            continue
        rows.append(_norm_mover_row(
            symbol=sym,
            name=name,
            asset_type="crypto",
            price=_safe_float(q.get("price")),
            change_percent=chg_pct,
            source="cmc_top500",
            volume_24h=_safe_float(q.get("volume_24h")),
            market_cap=_safe_float(q.get("market_cap")),
        ))

    if rows:
        rows.sort(key=lambda x: x["change_percent"], reverse=True)
        payload = {
            "category": "crypto",
            "gainers": rows[:8],
            "losers": list(reversed(rows))[:8],
            "updated_at": None,
            "from_cache": False,
        }
        cache.set(hot_key, payload, _MOVERS_CACHE_TTL)    # 5-min hot
        cache.set(lkg_key, payload, _MOVERS_CRYPTO_LKG_TTL)  # 7-day LKG
        return payload

    # 3. LKG fallback — serve last known good rather than empty
    lkg = cache.get(lkg_key)
    if lkg is not None:
        print("[HOME_MOVERS] crypto: CMC unavailable — serving LKG data")
        return {**lkg, "from_cache": True, "stale": True}

    # 4. Truly empty (CMC never returned data in this process lifetime)
    return {
        "category": "crypto",
        "gainers": [],
        "losers": [],
        "updated_at": None,
        "from_cache": False,
    }


# ── Main dispatch ──────────────────────────────────────────────────────────

async def get_movers_by_category(category: str, data_service) -> dict:
    """
    Entry point for GET /api/home/movers?category=...

    category: "stocks" | "etfs" | "commodities" | "crypto" | "all"

    "all" runs all 4 categories in parallel, then merges gainers/losers
    globally and ranks by % move (largest move wins, across all asset types).
    Frontend row shape is identical for every category.
    """
    category = (category or "stocks").lower().strip()

    if category == "stocks":
        return await _movers_stocks(data_service)
    if category == "etfs":
        return await _movers_etfs(data_service)
    if category == "commodities":
        return await _movers_commodities(data_service)
    if category == "crypto":
        return await _movers_crypto(data_service)

    if category == "all":
        stocks, etfs, comms, crypto = await asyncio.gather(
            _movers_stocks(data_service),
            _movers_etfs(data_service),
            _movers_commodities(data_service),
            _movers_crypto(data_service),
            return_exceptions=True,
        )

        def _safe_rows(result, key: str) -> list:
            if isinstance(result, Exception) or not isinstance(result, dict):
                return []
            return result.get(key) or []

        all_gainers = (
            _safe_rows(stocks, "gainers")
            + _safe_rows(etfs, "gainers")
            + _safe_rows(comms, "gainers")
            + _safe_rows(crypto, "gainers")
        )
        all_losers = (
            _safe_rows(stocks, "losers")
            + _safe_rows(etfs, "losers")
            + _safe_rows(comms, "losers")
            + _safe_rows(crypto, "losers")
        )

        def _dedup_by_symbol(rows: list) -> list:
            seen: set[str] = set()
            out: list[dict] = []
            for r in rows:
                sym = r.get("symbol", "")
                if sym and sym not in seen:
                    seen.add(sym)
                    out.append(r)
            return out

        all_gainers = _dedup_by_symbol(
            sorted(all_gainers, key=lambda x: x.get("change_percent") or 0, reverse=True)
        )[:10]
        all_losers = _dedup_by_symbol(
            sorted(all_losers, key=lambda x: x.get("change_percent") or 0)
        )[:10]

        return {
            "category": "all",
            "gainers": all_gainers,
            "losers": all_losers,
            "updated_at": None,
            "from_cache": False,
        }

    # Unknown category — fallback to stocks
    return await _movers_stocks(data_service)
