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

_HOME_CACHE_KEY = "home:dashboard:v3"
_HOME_CACHE_TTL = 60  # 1 minute — upstream caches do the heavy lifting

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"


# ── Sub-theme taxonomy ─────────────────────────────────────────────────────
# Curated ticker sets per niche sub-theme. Cross-referenced against live
# Tradier quotes and the options LKG cache. Includes tickers from the user's
# watchlist universe plus key additional names per theme.
# Non-US tickers (symbol contains ":") are filtered before any API call.

THEME_MAP: dict[str, list[str]] = {
    "AI Networking": [
        "ALAB", "ANET", "CIEN", "CRDO", "MRVL", "SMCI", "VIAV", "COHR",
        "APH", "NOK", "INFN",
    ],
    "Photonics / Lasers": [
        "AAOI", "COHR", "IPGP", "LASR", "LITE", "LPTH", "LWLG", "POET",
        "GLW", "XTIA", "MTSI", "OPTX", "FNSR",
    ],
    "Substrates / Packaging": [
        "AMKR", "AXTI", "CAMT", "FN", "FORM", "GFS", "ICHR", "INTT",
        "KLIC", "ONTO", "PLAB", "TER", "TSEM", "UCTT", "VECO", "VSH",
        "ACMR", "MKSI",
    ],
    "Memory / Storage": [
        "MU", "WDC", "STX", "SNDK", "PSTG", "SIMO", "NTAP",
    ],
    "Datacenter / Compute": [
        "APLD", "ARM", "EQIX", "IREN", "SMCI", "TSM", "NVDA", "AMD",
        "INTC", "WULF", "CLSK", "MARA", "HUT",
    ],
    "Semi Materials": [
        "AXTI", "FORM", "ICHR", "KLAC", "KLIC", "MCHP", "ON", "SLAB",
        "TE", "UCTT", "VECO", "VSH", "ENTG", "AMAT", "LRCX",
    ],
    "Power / Cooling": [
        "ETN", "POWL", "VRT", "VICR", "SMTC", "HUBB", "AMPX",
    ],
    "Nuclear / Grid": [
        "BE", "CEG", "EQT", "FSLR", "TPL", "CCJ", "NNE", "SMR",
    ],
    "Aerospace / Defense": [
        "AVAV", "DRS", "ESLT", "KTOS", "LUNR", "RKLB", "ASTS", "MDA",
        "BKSY", "PL", "SPIR", "KRMN", "FJET",
    ],
    "Robotics / Automation": [
        "AEVA", "IONQ", "JOBY", "OUST", "OSS", "TER", "ISRG", "PATH",
    ],
    "Cybersecurity": [
        "KEYS", "CRWD", "PANW", "ZS", "NET", "FTNT", "CYBR",
    ],
    "Data Infra / Storage": [
        "EQIX", "PSTG", "STX", "WDC", "SNOW", "DDOG", "MDB", "SNDK",
    ],
}

# Tickers in the THEME_MAP that are US-exchange tradeable (no ":" in symbol)
_THEME_TICKERS_US: list[str] = sorted({
    sym for tickers in THEME_MAP.values()
    for sym in tickers
    if ":" not in sym
})


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

def _build_options_lkg_index() -> dict[str, dict]:
    """
    Build options ticker index for Home unusual-flows section.

    Priority order (all in-memory reads, zero API calls):
      1. home:unusual_options:v1  — Home-dedicated fast cache (10-min refresh,
         Tradier-only, no AI). Populated by _home_options_precompute_loop().
      2. options_screener_lkg_v1:{tab}  — Full Options Flow LKG fallback (4-hr
         TTL, refreshed every ~3-4 min by the main precompute loop).

    Returns a dict keyed by uppercase ticker symbol.
    """
    index: dict[str, dict] = {}

    # ── Priority 1: Home-dedicated fast cache ─────────────────────────
    home_cache = cache.get("home:unusual_options:v1")
    if home_cache:
        for t in (home_cache.get("tickers") or []):
            sym = (t.get("ticker") or "").upper()
            if not sym or sym in index:
                continue
            index[sym] = {**t, "source_tab": "home_fast_cache"}

    # ── Priority 2: LKG fallback from full options precompute tabs ────
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


# ── Batch-quote helper ─────────────────────────────────────────────────────

async def _batch_quotes(tickers: list[str], data_service) -> dict[str, dict]:
    """
    One Tradier batch-quote call for the given tickers (cached at _QUOTE_TTL).
    Returns a dict keyed by uppercase symbol. Filters out non-US tickers
    (those containing ":") before calling Tradier.
    """
    us_tickers = [t for t in tickers if ":" not in t]
    if not us_tickers or not data_service or not getattr(data_service, "tradier", None):
        return {}
    try:
        quotes = await data_service.tradier.get_quotes(us_tickers)
        return {(q.get("symbol") or "").upper(): q for q in (quotes or [])}
    except Exception as exc:
        print(f"[HOME] batch_quotes error (non-fatal): {exc}")
        return {}


def _snapshot_row(
    symbol: str,
    quote: dict,
    options_index: dict[str, dict],
    asset_type: str | None = None,
    csv_row: dict | None = None,
) -> dict:
    last = quote.get("last")
    chg_pct = quote.get("change_percentage")
    vol = quote.get("volume")
    avg_vol = quote.get("average_volume")
    vol_ratio = round(vol / avg_vol, 2) if vol and avg_vol and avg_vol > 0 else None
    opts = options_index.get(symbol.upper(), {})

    # RSI and relative volume from watchlist csv_data (optional — watchlist rows only)
    rsi: float | None = None
    signal_label: str | None = None
    if csv_row:
        rsi = _parse_float(
            csv_row.get("Relative Strength Index (RSI)") or csv_row.get("RSI")
        )
        rel_vol_csv = _parse_pct(csv_row.get("Relative Volume"))
        rel_vol_x = (rel_vol_csv / 100.0) if rel_vol_csv is not None else None
        effective_rel_vol = vol_ratio if vol_ratio is not None else rel_vol_x
        signal_label = _signal_label(chg_pct, rsi, effective_rel_vol, opts.get("primary_signal")) or None

    row = {
        "symbol": symbol.upper(),
        "current_price": last,
        "change_1d_pct": chg_pct,
        "volume_vs_avg": vol_ratio,
        "options_signal": opts.get("primary_signal"),
        "rsi": round(rsi, 1) if rsi is not None else None,
        "signal_label": signal_label,
    }
    if asset_type is not None:
        row["asset_type"] = asset_type
    return row


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
    Compute sub-theme performance from THEME_MAP tickers.

    Data sources (in priority order):
      1. Watchlist quotes (already fetched — zero extra API calls for those tickers)
      2. Tradier batch quote for THEME_MAP tickers NOT in the watchlist
      3. Options LKG cache fallback (price_change_pct available for ~70 tickers)

    Returns a list of sub-theme rows sorted by avg_change_1d descending,
    each with: sub_theme, avg_change_1d, leader_symbols, leader_count,
    breadth_score (% of tickers positive today), momentum_score, pattern_summary.
    """
    wq = watchlist_quotes or {}

    # Find THEME_MAP tickers not already in watchlist quotes
    missing = [sym for sym in _THEME_TICKERS_US if sym not in wq]

    # Fetch missing tickers in one Tradier batch call (cached)
    extra_quotes: dict[str, dict] = {}
    if missing and data_service and getattr(data_service, "tradier", None):
        try:
            extra_quotes = await _batch_quotes(missing, data_service)
        except Exception as exc:
            print(f"[HOME] sub_theme extra_quotes failed (non-fatal): {exc}")

    # Merge quote sources: watchlist quotes take priority
    all_quotes = {**extra_quotes, **wq}

    theme_rows: list[dict] = []

    for sub_theme, theme_tickers in THEME_MAP.items():
        us_only = [t for t in theme_tickers if ":" not in t]
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
            return {**cached, "from_cache": True}

    t0 = time.time()

    # ── Tasks (all optional / fail-soft) ──────────────────────────────
    tasks: dict[str, Optional[asyncio.Task]] = {}

    if macro_provider:
        tasks["macro"] = asyncio.create_task(macro_provider.get_dashboard())

    try:
        from services.sector_rotation.service import get_dashboard as _sr_get
        tasks["sector"] = asyncio.create_task(_sr_get(include_analysis=False))
    except Exception:
        tasks["sector"] = None

    if data_service and getattr(data_service, "fmp", None):
        tasks["movers"] = asyncio.create_task(data_service.fmp.get_gainers_losers())
    if data_service and getattr(data_service, "finviz", None):
        tasks["fv_gainers"] = asyncio.create_task(data_service.finviz.get_screener_results("ta_topgainers"))
        tasks["fv_losers"] = asyncio.create_task(data_service.finviz.get_screener_results("ta_toplosers"))
    if data_service and getattr(data_service, "fear_greed", None):
        tasks["fg"] = asyncio.create_task(data_service.fear_greed.get_fear_greed_index())
    if data_service and getattr(data_service, "stocktwits", None):
        tasks["trending"] = asyncio.create_task(data_service.stocktwits.get_trending())

    tasks["news"] = asyncio.create_task(_fetch_latest_news(data_service))

    # ── Primary watchlist — load FULL data (fix: was metadata-only) ────
    watchlist: dict | None = None
    try:
        watchlist = await _load_primary_watchlist()
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
    tasks["watchlist_data"] = asyncio.create_task(
        _fetch_watchlist_data(watchlist, data_service, options_index, csv_index)
    )

    # ── Portfolio snapshot ─────────────────────────────────────────────
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

    # Grab updated_at from the Home fast cache if it's the active source
    _home_opts_cache = cache.get("home:unusual_options:v1")
    _opts_updated_at = (_home_opts_cache or {}).get("updated_at") or None
    unusual_options_flows = _extract_unusual_options_flows(options_index, updated_at=_opts_updated_at)

    # ── Sub-theme performance — uses already-fetched watchlist quotes ──
    # Pass watchlist quotes so the sub-theme engine reuses them (no extra
    # Tradier calls for tickers already in the watchlist).
    watchlist_quote_by_sym: dict[str, dict] = {}
    if watchlist:
        wl_tickers = [t for t in (watchlist.get("tickers") or []) if ":" not in t]
        if wl_tickers and data_service and getattr(data_service, "tradier", None):
            try:
                raw_q = await data_service.tradier.get_quotes(wl_tickers)
                watchlist_quote_by_sym = {(q.get("symbol") or "").upper(): q for q in (raw_q or [])}
            except Exception:
                pass

    sub_theme_performance = await _fetch_sub_theme_performance(
        data_service, options_index, watchlist_quotes=watchlist_quote_by_sym
    )

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
                "ok_fast_cache" if (unusual_options_flows and _home_opts_cache)
                else "ok_lkg_fallback" if unusual_options_flows
                else "precompute_pending"
            ),
            "watchlist_snapshot": "ok" if watchlist_snapshot else "unavailable",
            "highlighted_companies": "ok" if highlighted_companies else "unavailable",
            "portfolio_snapshot": "ok" if portfolio_snapshot else "unavailable",
            "sub_theme_performance": "ok" if sub_theme_performance else "unavailable",
        },
        "timing": {"total_seconds": round(time.time() - t0, 2)},
        "from_cache": False,
    }

    cache.set(_HOME_CACHE_KEY, payload, _HOME_CACHE_TTL)
    return payload
