"""
Chart Radar Router — /api/chart-radar/*

Lightweight adapter over the existing watchlist and portfolio services.
Returns ticker metadata for organizing TradingView chart matrices.
Does NOT fetch OHLC data — TradingView widgets handle rendering client-side.
Does NOT duplicate watchlist or portfolio data — reads exclusively from
existing loaders (load_watchlist, load_active_holdings).

OHLC Snapshot Layer (added):
  GET /api/chart-radar/snapshots — cache-first OHLC bars for many symbols.
  GET /api/chart-radar/diagnostics — cache hit/miss counters.
  Sources: FMP /stable/historical-chart/{interval}/{symbol} (intraday)
           FMP /stable/historical-price-eod (daily, existing pattern)
  Cache keys follow fmp_utils.py convention:
    hot: fmp:ohlc:{interval}:{range}:{SYM}   TTL per-interval
    lkg: fmp:ohlc:lkg:{interval}:{range}:{SYM}  24h fallback
"""

from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import asyncio
import json
import logging
import time
import uuid
from datetime import date, datetime, timedelta
from typing import Any, Optional

import httpx
from fastapi import APIRouter, Query, Request, HTTPException

logger = logging.getLogger("chart_radar")
from pydantic import BaseModel

from services.watchlist_service import load_watchlist
from data.portfolio_store import load_active_holdings
from data.cache import cache, CANDLE_TTL

router = APIRouter(prefix="/api/chart-radar", tags=["chart-radar"])


# ── Market-cap bucketing and leader-tier classification ───────────────────────

def _market_cap_bucket(mc: float | None) -> str:
    if mc is None:
        return "Unknown"
    if mc >= 200e9:
        return "Mega Cap"
    if mc >= 25e9:
        return "Large Cap"
    if mc >= 2e9:
        return "Mid Cap"
    if mc >= 300e6:
        return "Small Cap"
    return "Micro Cap"


def _leader_tier(mc: float | None) -> str:
    if mc is None:
        return "Unknown"
    if mc >= 25e9:
        return "Leader"
    if mc >= 2e9:
        return "Emerging Leader"
    if mc >= 300e6:
        return "Niche Breakout"
    return "Speculative"


# ── Market-cap parser (handles raw floats, "12.7B", "$200M", etc.) ───────────

def _parse_mc(raw: Any) -> float | None:
    if raw is None:
        return None
    s = str(raw).strip().replace("$", "").replace(",", "")
    if not s or s in ("-", "N/A", "n/a", "na", ""):
        return None
    try:
        mult = 1.0
        if s[-1].upper() == "T":
            mult = 1e12; s = s[:-1]
        elif s[-1].upper() == "B":
            mult = 1e9;  s = s[:-1]
        elif s[-1].upper() == "M":
            mult = 1e6;  s = s[:-1]
        elif s[-1].upper() == "K":
            mult = 1e3;  s = s[:-1]
        return float(s) * mult
    except (ValueError, AttributeError, IndexError):
        return None


# ── Safe float parser ─────────────────────────────────────────────────────────

def _safe_float(raw: Any) -> float | None:
    if raw is None:
        return None
    try:
        cleaned = str(raw).replace("%", "").replace(",", "").replace("$", "").strip()
        if cleaned in ("", "-", "N/A", "n/a"):
            return None
        return round(float(cleaned), 4)
    except (ValueError, TypeError):
        return None


# ── TradingView symbol formatter ───────────────────────────────────────────────

_TV_EXCHANGES = {"NASDAQ", "NYSE", "AMEX", "TSX", "LSE", "AIM", "ASX", "STO", "FRA", "XETR", "CBOE"}


def _tv_symbol(ticker: str, exchange: str | None) -> str:
    """Return 'EXCHANGE:TICKER' for TradingView, or just 'TICKER' if unresolved."""
    t = ticker.strip().upper()
    if ":" in t:
        return t  # already prefixed
    ex = (exchange or "").strip().upper()
    if ex in _TV_EXCHANGES:
        return f"{ex}:{t}"
    return t  # TradingView will resolve automatically


# ── Theme mapper (cached import — no side-effects) ────────────────────────────

def _get_theme(ticker: str, row: dict) -> str | None:
    """Return theme for ticker; tries row first, then static theme mapper."""
    theme = (
        row.get("theme")
        or row.get("Theme")
        or row.get("canonical_theme_name")
    )
    if theme:
        return str(theme)
    try:
        from services.theme_ticker_mapper import map_ticker_to_primary_theme
        return map_ticker_to_primary_theme(ticker)
    except Exception:
        return None


# ── Symbol normalizer — builds one output row from any source dict ────────────

def _normalize_symbol(
    row: dict,
    source: str,
    portfolio_weight: float | None = None,
) -> dict | None:
    """
    Build a normalized symbol object from a watchlist CSV row or portfolio holding.
    Cache-first only — no external API calls made here.
    Returns None if no ticker can be resolved.
    """
    ticker = (
        row.get("ticker")
        or row.get("symbol")
        or row.get("Symbol")
        or ""
    ).strip().upper()
    if not ticker:
        return None

    # Company name — CSV files rarely have this; fall back to None
    company_name: str | None = (
        row.get("company_name")
        or row.get("Company")
        or row.get("Name")
        or row.get("name")
        or None
    )

    # Theme
    theme = _get_theme(ticker, row)

    # Sector (prefer "Sector" column; fall back to "Industry" which exists in Finviz exports)
    sector: str | None = (
        row.get("sector")
        or row.get("Sector")
        or row.get("Industry")
        or row.get("industry")
        or None
    )

    # Market cap — the Finviz export uses "Market Cap" (with space) as a raw numeric string
    raw_mc = (
        row.get("market_cap")
        or row.get("Market Cap")
        or row.get("MarketCap")
        or row.get("marketCap")
        or None
    )
    mc = _parse_mc(raw_mc)

    # Exchange
    exchange: str | None = (
        row.get("exchange")
        or row.get("Exchange")
        or None
    )

    # Price — "Stock Price" is the Finviz column name
    price = _safe_float(
        row.get("price")
        or row.get("Stock Price")
        or row.get("stock_price")
        or row.get("lastPrice")
    )

    # Daily change — not always in CSV; will be None for most rows
    daily_change_pct = _safe_float(
        row.get("daily_change_pct")
        or row.get("Change (1D)")
        or row.get("Price Change 1-Day")
        or row.get("Perf Day")
    )

    # Relative volume — "Relative Volume" is the Finviz column name
    relative_volume = _safe_float(
        row.get("relative_volume")
        or row.get("Relative Volume")
        or row.get("rel_vol")
        or row.get("relVol")
    )

    # Logo / asset type
    logo_url: str | None  = row.get("logo_url") or row.get("logo") or None
    asset_type: str       = str(row.get("asset_type") or row.get("Asset Type") or "stock")

    # Watchlist section — theme is the most meaningful proxy when no explicit section
    watchlist_section: str | None = row.get("watchlist_section") or theme or None

    return {
        "ticker":               ticker,
        "tradingview_symbol":   _tv_symbol(ticker, exchange),
        "company_name":         company_name,
        "source":               source,
        "theme":                theme,
        "sector":               sector,
        "market_cap":           mc,
        "market_cap_bucket":    _market_cap_bucket(mc),
        "leader_tier":          _leader_tier(mc),
        "exchange":             exchange,
        "price":                price,
        "daily_change_pct":     daily_change_pct,
        "relative_volume":      relative_volume,
        "portfolio_weight_pct": portfolio_weight,
        "watchlist_section":    watchlist_section,
        "logo_url":             logo_url,
        "asset_type":           asset_type,
    }


# ── Grouping engine ────────────────────────────────────────────────────────────

_GROUP_ORDER: dict[str, int] = {
    # market_cap_bucket
    "Mega Cap": 0, "Large Cap": 1, "Mid Cap": 2, "Small Cap": 3, "Micro Cap": 4,
    # leader_tier
    "Leader": 0, "Emerging Leader": 1, "Niche Breakout": 2, "Speculative": 3,
    # portfolio_weight buckets
    "≥10% Weight": 0, "5–10% Weight": 1, "2–5% Weight": 2, "<2% Weight": 3,
    # fallbacks
    "Unknown": 90, "Unknown Theme": 91, "Ungrouped": 92, "All": 0,
}


def _weight_bucket(weight: float | None) -> str:
    w = weight or 0.0
    if w >= 10:
        return "≥10% Weight"
    if w >= 5:
        return "5–10% Weight"
    if w >= 2:
        return "2–5% Weight"
    return "<2% Weight"


def _group_symbols(symbols: list[dict], group_by: str) -> list[dict]:
    """Partition and sort symbols into named groups."""
    bucket: dict[str, list[dict]] = {}

    for sym in symbols:
        if group_by == "theme":
            key = sym.get("theme") or "Unknown Theme"
        elif group_by == "market_cap":
            key = sym.get("market_cap_bucket") or "Unknown"
        elif group_by == "leader_tier":
            key = sym.get("leader_tier") or "Unknown"
        elif group_by == "watchlist_section":
            key = sym.get("watchlist_section") or "Ungrouped"
        elif group_by == "portfolio_weight":
            key = _weight_bucket(sym.get("portfolio_weight_pct"))
        else:  # "none"
            key = "All"
        bucket.setdefault(key, []).append(sym)

    sorted_keys = sorted(
        bucket.keys(),
        key=lambda k: (_GROUP_ORDER.get(k, 50), k),
    )

    groups = []
    for key in sorted_keys:
        syms = bucket[key]
        changes  = [s["daily_change_pct"]  for s in syms if s["daily_change_pct"]  is not None]
        rel_vols = [s["relative_volume"]   for s in syms if s["relative_volume"]   is not None]
        weights  = [s["portfolio_weight_pct"] for s in syms if s["portfolio_weight_pct"] is not None]
        groups.append({
            "key":   key,
            "label": key,
            "count": len(syms),
            "summary": {
                "avg_change_pct":             round(sum(changes)  / len(changes),  4) if changes  else None,
                "avg_relative_volume":        round(sum(rel_vols) / len(rel_vols), 4) if rel_vols else None,
                "total_portfolio_weight_pct": round(sum(weights),                  4) if weights  else None,
            },
            "symbols": syms,
        })
    return groups


# ── GET /api/chart-radar/universe ─────────────────────────────────────────────

@router.get("/universe")
async def universe_endpoint(
    request: Request,
    source:       str         = Query("watchlist", enum=["watchlist", "portfolio", "manual"]),
    group_by:     str         = Query("theme",     enum=["theme", "market_cap", "leader_tier", "watchlist_section", "portfolio_weight", "none"]),
    watchlist_id: Optional[str] = Query(None, description="Specific watchlist ID; omit for default"),
):
    """
    Build a ticker universe from the user's Watchlist or Portfolio, grouped
    and annotated with metadata needed to organize TradingView chart matrices.

    No external API calls are made — all data is drawn from existing cached
    sources (watchlist CSV rows, portfolio holdings, theme mapper).
    """
    warnings: list[str] = []
    user_id = _get_user_id(request)

    # ── Portfolio source ──────────────────────────────────────────────────────
    if source == "portfolio":
        holdings = load_active_holdings()
        if not holdings:
            return {
                "source": source, "group_by": group_by,
                "count": 0, "groups": [],
                "warnings": ["No portfolio holdings found"],
            }

        # Cost-basis weight (proxy for value weight; avoids live price calls)
        total_cost = sum(
            float(h.get("shares", 0) or 0) * float(h.get("avg_cost", 0) or 0)
            for h in holdings
        )

        symbols: list[dict] = []
        for h in holdings:
            shares   = float(h.get("shares", 0) or 0)
            avg_cost = float(h.get("avg_cost", 0) or 0)
            cost     = shares * avg_cost
            weight   = round(cost / total_cost * 100, 4) if total_cost > 0 else None
            sym = _normalize_symbol(h, source="portfolio", portfolio_weight=weight)
            if sym:
                symbols.append(sym)

        if not symbols:
            warnings.append("Holdings present but no tickers could be resolved")

    # ── Watchlist source ──────────────────────────────────────────────────────
    elif source == "watchlist":
        store = load_watchlist(watchlist_id) if watchlist_id else load_watchlist()
        if store is None:
            return {
                "source": source, "group_by": group_by,
                "count": 0, "groups": [],
                "warnings": ["No watchlist found — upload a CSV first"],
            }

        csv_data: list[dict] = store.get("csv_data", [])
        tickers:  list[str]  = store.get("tickers", [])

        # Build ticker → CSV row lookup map
        csv_map: dict[str, dict] = {}
        for row in csv_data:
            sym_key = (
                row.get("Symbol") or row.get("ticker") or row.get("symbol") or ""
            ).strip().upper()
            if sym_key:
                csv_map[sym_key] = row

        # ── Build ticker→section map from analysis.sections ───────────────────
        # This is the exact same source the Watchlist page uses for its visible
        # group labels.  Mirrors watchlist_router._enrich_store_with_quotes:
        #   1. Direct membership in named sections
        #   2. Industry fallback for "Other / Uncategorized" bucket
        #   3. Industry fallback for saved tickers absent from all sections
        # No external API calls — pure in-memory lookups.
        _UNCATEGORIZED = {"other / uncategorized", "other/uncategorized",
                          "uncategorized", "other"}
        section_map: dict[str, str] = {}   # ticker (upper) → section title
        _unc_syms: list[str] = []
        for _sec in store.get("analysis", {}).get("sections", []):
            _title = (_sec.get("title") or _sec.get("name") or "").strip()
            if _title.lower() in _UNCATEGORIZED or _sec.get("id") == "other_uncategorized":
                for _row in _sec.get("tickers", []):
                    _s = str(_row.get("symbol") or _row.get("ticker") or "").strip().upper()
                    if _s:
                        _unc_syms.append(_s)
            else:
                for _row in _sec.get("tickers", []):
                    _s = str(_row.get("symbol") or _row.get("ticker") or "").strip().upper()
                    if _s:
                        section_map[_s] = _title

        # Industry fallback — identical to watchlist_router reclassification
        try:
            from services.theme_ticker_mapper import map_industry_to_theme as _map_ind
        except ImportError:
            _map_ind = None  # type: ignore[assignment]

        def _sec_via_industry(sym: str) -> str | None:
            if not _map_ind:
                return None
            _r = csv_map.get(sym) or csv_map.get(
                sym.split(":")[-1] if ":" in sym else sym, {}
            )
            _ind = (_r.get("Industry") or _r.get("industry") or "").strip()
            if not _ind:
                return None
            _res = _map_ind(_ind)
            return _res[0] if _res else None

        for _sym in _unc_syms:
            if _sym not in section_map:
                _m = _sec_via_industry(_sym)
                if _m:
                    section_map[_sym] = _m

        for _t in tickers:
            _tu = str(_t).strip().upper()
            if _tu not in section_map:
                _m = _sec_via_industry(_tu)
                if _m:
                    section_map[_tu] = _m

        # Apply manual overrides — wins over all automated classification.
        # Same source of truth as the Watchlist page override layer.
        try:
            from services.category_overrides import apply_to_section_map as _apply_cat_overrides
            section_map = _apply_cat_overrides(section_map, user_id=user_id)
        except Exception:
            pass

        all_tickers = tickers or list(csv_map.keys())
        if not all_tickers:
            return {
                "source": source, "group_by": group_by,
                "count": 0, "groups": [],
                "warnings": ["Watchlist is empty"],
            }

        symbols = []
        for t in all_tickers:
            t_upper = str(t).strip().upper()
            row = dict(csv_map.get(t_upper, {}))
            row.setdefault("Symbol", t_upper)
            # Inject section-derived category so _get_theme() and
            # watchlist_section both use the Watchlist page's source of truth.
            sec_name = section_map.get(t_upper)
            if sec_name:
                row["canonical_theme_name"] = sec_name
                row["watchlist_section"]    = sec_name
            sym = _normalize_symbol(row, source="watchlist")
            if sym:
                symbols.append(sym)

        if not symbols:
            warnings.append("Tickers found but no symbols could be normalized")

        # Warn about market-cap / theme coverage gaps
        missing_mc    = sum(1 for s in symbols if s["market_cap"]    is None)
        missing_theme = sum(1 for s in symbols if s["theme"]         is None)
        if missing_mc > 0:
            warnings.append(f"{missing_mc} symbol(s) have no market_cap — bucketed as 'Unknown'")
        if missing_theme > 0:
            warnings.append(f"{missing_theme} symbol(s) have no theme mapping — grouped as 'Unknown Theme'")

    # ── Manual source (reserved) ──────────────────────────────────────────────
    else:
        return {
            "source": source, "group_by": group_by,
            "count": 0, "groups": [],
            "warnings": ["Manual source not yet implemented — use watchlist or portfolio"],
        }

    groups = _group_symbols(symbols, group_by)

    # ── Beacon log — easily grep-able proof this backend was actually hit ─────
    # Use print() so the line is guaranteed to appear in stdout regardless of
    # uvicorn's logging filter level for unconfigured loggers.
    print(
        f"[CHART_RADAR_UNIVERSE_HIT] source={source} group_by={group_by}"
        f" count={len(symbols)} groups={len(groups)}",
        flush=True,
    )

    # ── TradingView symbol coverage audit ────────────────────────────────────
    # Count how many symbols have an exchange prefix vs. bare ticker.
    # Bare-ticker fallback is safe for US equities (TradingView auto-resolves);
    # OTC / foreign tickers without a prefix may not resolve correctly.
    prefixed_ct  = sum(1 for s in symbols if ":" in s.get("tradingview_symbol", ""))
    bare_ct      = len(symbols) - prefixed_ct
    tv_coverage  = {
        "prefixed":    prefixed_ct,
        "bare_ticker": bare_ct,
        "note": (
            f"{bare_ct} of {len(symbols)} symbols use bare ticker (no exchange prefix). "
            "US equities resolve automatically in TradingView. "
            "OTC/foreign tickers (e.g. LPKFF, SIVEF) may need a manual EXCHANGE:TICKER prefix."
        ) if bare_ct > 0 else "All symbols have exchange prefixes.",
    }
    if bare_ct > 0:
        warnings.append(
            f"{bare_ct} tradingview_symbol(s) are bare tickers — "
            "fine for US equities, may not resolve for OTC/foreign."
        )

    return {
        "source":               source,
        "group_by":             group_by,
        "count":                len(symbols),
        "tradingview_coverage": tv_coverage,
        "groups":               groups,
        "warnings":             warnings,
    }


# ── User-ID resolver ──────────────────────────────────────────────────────────
# JWTAuthMiddleware is currently disabled (pure pass-through), so
# request.state.user_id is NEVER set by middleware.  The authoritative
# source is the Authorization: Bearer <token> header, which the frontend
# sends on every authenticated request.  We parse it directly here —
# the same approach used by subscription.py's require_subscription guard.

def _get_user_id(request: Request) -> str:
    """
    Resolve user_id for a request.

    Priority:
      1. request.state.user_id — populated if middleware is ever re-enabled
      2. Authorization: Bearer <JWT> → payload["sub"]
      3. "default" — unauthenticated / dev fallback
    """
    # 1. Middleware-set (future-proof)
    uid = getattr(request.state, "user_id", None)
    if uid:
        return str(uid)
    # 2. Parse Bearer token directly (current working path)
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        token = auth[7:]
        try:
            from auth import verify_token
            payload = verify_token(token)
            sub = payload.get("sub")
            if sub:
                return str(sub)
        except Exception:
            pass
    # 3. Unauthenticated / local dev
    return "default"


# ── Saved views Pydantic models ───────────────────────────────────────────────

class SaveViewRequest(BaseModel):
    name:         str            = "My Chart View"
    source:       str            = "watchlist"
    group_by:     str            = "theme"
    watchlist_id: Optional[str]  = None
    filters:      dict           = {}
    layout:       dict           = {}


class UpdateViewRequest(BaseModel):
    name:         Optional[str]  = None
    source:       Optional[str]  = None
    group_by:     Optional[str]  = None
    watchlist_id: Optional[str]  = None
    filters:      Optional[dict] = None
    layout:       Optional[dict] = None


# ── DB helper ─────────────────────────────────────────────────────────────────

def _db():
    """Return a live psycopg2 connection from the shared pool, or None."""
    try:
        from data.pg_storage import _get_conn
        return _get_conn()
    except Exception:
        return None


def _release(conn) -> None:
    try:
        from data.pg_storage import _put_conn
        _put_conn(conn)
    except Exception:
        pass


# ── POST /api/chart-radar/views ───────────────────────────────────────────────

@router.post("/views", status_code=201)
async def create_view(request: Request, body: SaveViewRequest):
    """Persist a saved chart-radar view for the current user."""
    user_id = _get_user_id(request)
    view_id = str(uuid.uuid4())
    conn = _db()
    if conn is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO public.chart_radar_views
                (id, user_id, name, source, group_by, watchlist_id, filters, layout)
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)
            """,
            (
                view_id, user_id, body.name, body.source, body.group_by,
                body.watchlist_id,
                json.dumps(body.filters),
                json.dumps(body.layout),
            ),
        )
        conn.commit()
        cur.close()
    except Exception as exc:
        print(f"[CHART_RADAR] create_view error: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail="Failed to save view")
    finally:
        _release(conn)

    return {
        "id":      view_id,
        "user_id": user_id,
        "name":    body.name,
        "created": True,
    }


# ── GET /api/chart-radar/views ────────────────────────────────────────────────

@router.get("/views")
async def list_views(request: Request):
    """Return all saved chart-radar views for the current user."""
    user_id = _get_user_id(request)
    conn = _db()
    if conn is None:
        return {"views": []}
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, user_id, name, source, group_by, watchlist_id,
                   filters, layout, created_at, updated_at
            FROM public.chart_radar_views
            WHERE user_id = %s
            ORDER BY updated_at DESC
            """,
            (user_id,),
        )
        rows = cur.fetchall()
        cur.close()
    except Exception as exc:
        print(f"[CHART_RADAR] list_views error: {exc}")
        return {"views": []}
    finally:
        _release(conn)

    views = []
    for r in rows:
        views.append({
            "id":           r[0],
            "user_id":      r[1],
            "name":         r[2],
            "source":       r[3],
            "group_by":     r[4],
            "watchlist_id": r[5],
            "filters":      r[6] if isinstance(r[6], dict) else {},
            "layout":       r[7] if isinstance(r[7], dict) else {},
            "created_at":   r[8].isoformat() if r[8] else None,
            "updated_at":   r[9].isoformat() if r[9] else None,
        })
    return {"views": views}


# ── PATCH /api/chart-radar/views/{view_id} ────────────────────────────────────

@router.patch("/views/{view_id}")
async def update_view(view_id: str, request: Request, body: UpdateViewRequest):
    """Update a saved chart-radar view. Only supplied fields are changed."""
    user_id = _get_user_id(request)
    conn = _db()
    if conn is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    try:
        cur = conn.cursor()
        # Ownership check
        cur.execute(
            "SELECT id FROM public.chart_radar_views WHERE id = %s AND user_id = %s",
            (view_id, user_id),
        )
        if not cur.fetchone():
            cur.close()
            raise HTTPException(status_code=404, detail="View not found")

        set_clauses: list[str] = []
        params: list           = []
        if body.name         is not None: set_clauses.append("name = %s");         params.append(body.name)
        if body.source       is not None: set_clauses.append("source = %s");       params.append(body.source)
        if body.group_by     is not None: set_clauses.append("group_by = %s");     params.append(body.group_by)
        if body.watchlist_id is not None: set_clauses.append("watchlist_id = %s"); params.append(body.watchlist_id)
        if body.filters      is not None: set_clauses.append("filters = %s::jsonb"); params.append(json.dumps(body.filters))
        if body.layout       is not None: set_clauses.append("layout = %s::jsonb");  params.append(json.dumps(body.layout))

        if not set_clauses:
            cur.close()
            return {"updated": False, "detail": "No fields to update"}

        set_clauses.append("updated_at = NOW()")
        params.extend([view_id, user_id])
        cur.execute(
            f"UPDATE public.chart_radar_views SET {', '.join(set_clauses)} WHERE id = %s AND user_id = %s",
            params,
        )
        conn.commit()
        cur.close()
    except HTTPException:
        raise
    except Exception as exc:
        print(f"[CHART_RADAR] update_view error: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail="Failed to update view")
    finally:
        _release(conn)

    return {"updated": True}


# ── DELETE /api/chart-radar/views/{view_id} ───────────────────────────────────

@router.delete("/views/{view_id}")
async def delete_view(view_id: str, request: Request):
    """Delete a saved chart-radar view (user-scoped)."""
    user_id = _get_user_id(request)
    conn = _db()
    if conn is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM public.chart_radar_views WHERE id = %s AND user_id = %s",
            (view_id, user_id),
        )
        conn.commit()
        deleted = cur.rowcount > 0
        cur.close()
    except Exception as exc:
        print(f"[CHART_RADAR] delete_view error: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail="Failed to delete view")
    finally:
        _release(conn)

    return {"deleted": deleted}


# ══════════════════════════════════════════════════════════════════════════════
# OHLC Snapshot Cache  —  GET /api/chart-radar/snapshots
# ══════════════════════════════════════════════════════════════════════════════
#
# Design:
#   • Cache-first always.  Never blocks a request on a live FMP call.
#   • On miss/stale: return whatever is in cache + trigger asyncio.create_task().
#   • Hot cache key  : fmp:ohlc:{interval}:{range}:{SYM}   TTL = per-interval
#   • LKG cache key  : fmp:ohlc:lkg:{interval}:{range}:{SYM}  TTL = 24 h
#   • Bar format     : [timestamp_ms, open, high, low, close, volume]
#   • FMP intraday   : /stable/historical-chart/{fmp_interval}/{symbol}
#   • FMP daily      : /stable/historical-price-eod  (same pattern as returns svc)
#   • Semaphore      : max 3 concurrent in-flight FMP calls (own pool, not governor)
#   • Concurrency    : background batch in groups of 5 concurrent per batch
# ─────────────────────────────────────────────────────────────────────────────

_FMP_BASE_OHLC = "https://financialmodelingprep.com/stable"
# _OHLC_SEM is created lazily via _ohlc_sem() to avoid event-loop binding issues

# Guard against concurrent duplicate refreshes for the same interval+range combo
_OHLC_REFRESH_ACTIVE: set[str] = set()

# Diagnostic counters (process-lifetime, never persisted)
_DIAG: dict[str, Any] = {
    "hits": 0, "lkg_hits": 0, "misses": 0,
    "refreshes_triggered": 0, "fmp_calls": 0, "fmp_errors": 0,
    "last_refresh_ms": 0.0, "last_refresh_ts": None,
}

# ── Interval → FMP path segment ───────────────────────────────────────────────
_INTERVAL_TO_FMP: dict[str, str] = {
    "5m":    "5min",
    "15m":   "15min",
    "30m":   "30min",
    "1h":    "1hour",
    "4h":    "4hour",
}

# ── Interval → hot-cache TTL (seconds) ───────────────────────────────────────
_INTERVAL_TTL: dict[str, int] = {
    "5m":    300,
    "15m":   CANDLE_TTL,   # 900 s — already defined in cache.py
    "30m":   1800,
    "1h":    3600,
    "4h":    14400,
    "daily": 3600,
}
_LKG_TTL   = 24 * 3600    # 24-hour LKG for all intervals
_MAX_SYMS  = 50            # hard cap per request

# ── Range string → calendar-day window for FMP from/to params ────────────────
_RANGE_DAYS: dict[str, int] = {
    "1d":  2,
    "5d":  8,
    "10d": 16,
    "1m":  32,
    "3m":  95,
    "6m":  185,
    "1y":  370,
}


def _fmp_key_ohlc() -> str:
    return os.getenv("FMP_API_KEY") or ""


def _ohlc_hot_key(interval: str, range_str: str, symbol: str) -> str:
    return f"fmp:ohlc:{interval}:{range_str}:{symbol.upper()}"


def _ohlc_lkg_key(interval: str, range_str: str, symbol: str) -> str:
    return f"fmp:ohlc:lkg:{interval}:{range_str}:{symbol.upper()}"


def _ohlc_ttl(interval: str) -> int:
    return _INTERVAL_TTL.get(interval, CANDLE_TTL)


def _ohlc_sem() -> asyncio.Semaphore:
    """Lazy per-loop semaphore — avoids module-level event-loop binding issues."""
    if not hasattr(_ohlc_sem, "_sem"):
        _ohlc_sem._sem = asyncio.Semaphore(3)  # type: ignore[attr-defined]
    return _ohlc_sem._sem  # type: ignore[attr-defined]


def _bars_from_fmp_response(raw: Any) -> list[list]:
    """
    Convert FMP OHLCV response to compact [[ts_ms, o, h, l, c, v], ...] bars,
    sorted oldest-first.  FMP returns newest-first — we reverse.

    Handles both intraday shape (list of dicts) and daily shape
    (dict with 'historical' key or plain list).
    """
    if isinstance(raw, dict):
        items: list[dict] = raw.get("historical") or []
    elif isinstance(raw, list):
        items = raw
    else:
        return []

    bars: list[list] = []
    for item in items:
        dt_str = item.get("date") or ""
        if not dt_str:
            continue
        try:
            if " " in dt_str:
                ts_ms = int(
                    datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S").timestamp() * 1000
                )
            else:
                ts_ms = int(
                    datetime.strptime(dt_str, "%Y-%m-%d").timestamp() * 1000
                )
        except Exception:
            continue
        bars.append([
            ts_ms,
            item.get("open"),
            item.get("high"),
            item.get("low"),
            item.get("close"),
            item.get("volume"),
        ])

    bars.reverse()   # oldest-first
    return bars


async def _fetch_ohlc_single(symbol: str, interval: str, range_str: str) -> bool:
    """
    Fetch OHLCV bars for one symbol from FMP, write hot + LKG cache.
    Returns True on a successful non-empty fetch, False otherwise.
    Never raises.

    FMP stable API — symbol is always a query param (not a path segment):
      Intraday: GET /stable/historical-chart/{fmp_interval}?symbol=SYM&from=...&to=...
      Daily:    GET /stable/historical-price-eod?symbol=SYM&from=...&to=...
    """
    key = _fmp_key_ohlc()
    if not key:
        return False

    sym = symbol.upper()
    cal_days  = _RANGE_DAYS.get(range_str, 16)
    from_date = (date.today() - timedelta(days=cal_days)).isoformat()
    to_date   = date.today().isoformat()

    if interval in _INTERVAL_TO_FMP:
        fmp_seg = _INTERVAL_TO_FMP[interval]
        url     = f"{_FMP_BASE_OHLC}/historical-chart/{fmp_seg}"
    else:
        # daily — identical to screener_returns_service.py pattern
        url     = f"{_FMP_BASE_OHLC}/historical-price-eod"

    params = {"symbol": sym, "from": from_date, "to": to_date, "apikey": key}

    async with _ohlc_sem():
        try:
            async with httpx.AsyncClient(timeout=12.0) as client:
                resp = await client.get(url, params=params)

            _DIAG["fmp_calls"] += 1

            if resp.status_code not in (200, 201):
                if resp.status_code not in (402, 403, 404):
                    print(
                        f"[CHART_RADAR_OHLC] FMP {sym}/{interval} "
                        f"HTTP {resp.status_code}"
                    )
                _DIAG["fmp_errors"] += 1
                return False

            bars = _bars_from_fmp_response(resp.json())
            if not bars:
                return False

            entry = {
                "bars":       bars,
                "updated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "stale":      False,
                "missing":    False,
                "interval":   interval,
                "range":      range_str,
                "source":     "fmp",
                "bar_count":  len(bars),
            }
            ttl = _ohlc_ttl(interval)
            cache.set(_ohlc_hot_key(interval, range_str, sym), entry, ttl)
            cache.set(_ohlc_lkg_key(interval, range_str, sym), entry, _LKG_TTL)
            return True

        except Exception as exc:
            print(
                f"[CHART_RADAR_OHLC] {sym}/{interval}/{range_str} "
                f"{type(exc).__name__}: {exc!r}"
            )
            _DIAG["fmp_errors"] += 1
            return False


async def _bg_refresh_ohlc(
    symbols: list[str],
    interval: str,
    range_str: str,
) -> None:
    """
    Background refresh for a batch of symbols.
    Guarded by _OHLC_REFRESH_ACTIVE to prevent duplicate concurrent jobs
    for the same interval+range combo.  Runs in groups of 5 parallel calls
    (semaphore inside _fetch_ohlc_single caps actual FMP concurrency to 3).
    """
    guard_key = f"{interval}:{range_str}"
    if guard_key in _OHLC_REFRESH_ACTIVE:
        return
    _OHLC_REFRESH_ACTIVE.add(guard_key)
    t0 = time.time()
    ok_count = 0
    try:
        _DIAG["refreshes_triggered"] += 1
        batch_size = 5
        for i in range(0, len(symbols), batch_size):
            batch  = symbols[i : i + batch_size]
            results = await asyncio.gather(
                *[_fetch_ohlc_single(sym, interval, range_str) for sym in batch]
            )
            ok_count += sum(1 for r in results if r)

        elapsed = round((time.time() - t0) * 1000, 1)
        _DIAG["last_refresh_ms"] = elapsed
        _DIAG["last_refresh_ts"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        print(
            f"[CHART_RADAR_OHLC] bg_refresh done: {ok_count}/{len(symbols)} ok "
            f"interval={interval} range={range_str} elapsed={elapsed}ms"
        )
    except Exception as exc:
        print(f"[CHART_RADAR_OHLC] bg_refresh error: {exc}")
    finally:
        _OHLC_REFRESH_ACTIVE.discard(guard_key)


def _read_ohlc_cache(symbol: str, interval: str, range_str: str) -> dict | None:
    """
    Hot cache → LKG fallback.  Updates diagnostic counters.
    Returns None only when neither layer has data.
    """
    sym = symbol.upper()

    entry = cache.get(_ohlc_hot_key(interval, range_str, sym))
    if entry is not None:
        _DIAG["hits"] += 1
        return entry

    lkg = cache.get(_ohlc_lkg_key(interval, range_str, sym))
    if lkg is not None:
        _DIAG["lkg_hits"] += 1
        return {**lkg, "stale": True, "source": "fmp_lkg"}

    _DIAG["misses"] += 1
    return None


# ── GET /api/chart-radar/snapshots ────────────────────────────────────────────

@router.get("/snapshots")
async def ohlc_snapshots(
    symbols:  str = Query(...,    description="Comma-separated tickers, e.g. AAPL,MSFT"),
    interval: str = Query("15m", description="Bar interval", enum=["5m","15m","30m","1h","4h","daily"]),
    range:    str = Query("10d", description="History window", enum=["1d","5d","10d","1m","3m","6m","1y"]),
):
    """
    Cache-first OHLC snapshots for Chart Radar.

    Returns immediately from cache.  Missing or stale symbols trigger a
    background FMP refresh — no live API calls are ever made during a request.

    Per-symbol response shape:
        bars       [[ts_ms, open, high, low, close, volume], ...]  oldest-first
        updated_at ISO timestamp of last successful FMP write
        stale      True when served from LKG (hot TTL expired)
        missing    True when no data at all in either cache layer
        interval   echoed from request
        range      echoed from request
        source     "fmp" | "fmp_lkg" | "missing"
        bar_count  number of bars (0 when missing)
    """
    t0 = time.time()

    sym_list = [s.strip().upper() for s in symbols.split(",") if s.strip()][:_MAX_SYMS]
    if not sym_list:
        return {"snapshots": {}, "meta": {"error": "No symbols provided"}}

    snapshots: dict[str, dict] = {}
    missing_syms: list[str]   = []
    stale_syms:   list[str]   = []

    for sym in sym_list:
        entry = _read_ohlc_cache(sym, interval, range)
        if entry is None:
            missing_syms.append(sym)
            snapshots[sym] = {
                "bars": [], "updated_at": None,
                "stale": False, "missing": True,
                "interval": interval, "range": range,
                "source": "missing", "bar_count": 0,
            }
        else:
            if entry.get("stale"):
                stale_syms.append(sym)
            snapshots[sym] = entry

    refresh_needed = missing_syms + stale_syms
    if refresh_needed:
        asyncio.create_task(
            _bg_refresh_ohlc(refresh_needed, interval, range)
        )

    elapsed_ms = round((time.time() - t0) * 1000, 1)

    print(
        f"[CHART_RADAR_SNAPSHOTS] syms={len(sym_list)} hit={len(sym_list)-len(missing_syms)-len(stale_syms)}"
        f" stale={len(stale_syms)} miss={len(missing_syms)}"
        f" interval={interval} range={range} elapsed={elapsed_ms}ms"
    )

    return {
        "snapshots": snapshots,
        "meta": {
            "requested":          len(sym_list),
            "hit":                len(sym_list) - len(missing_syms) - len(stale_syms),
            "stale":              len(stale_syms),
            "missing":            len(missing_syms),
            "refresh_triggered":  len(refresh_needed),
            "interval":           interval,
            "range":              range,
            "elapsed_ms":         elapsed_ms,
        },
    }


# ── GET /api/chart-radar/diagnostics ──────────────────────────────────────────

@router.get("/diagnostics")
async def ohlc_diagnostics():
    """
    OHLC cache diagnostics — hit rate, stale/miss counts, FMP call counters,
    and a live scan of currently warmed cache keys.
    """
    total_reads = _DIAG["hits"] + _DIAG["lkg_hits"] + _DIAG["misses"]
    hit_rate = (
        round((_DIAG["hits"] + _DIAG["lkg_hits"]) / total_reads * 100, 1)
        if total_reads else None
    )

    warmed_keys = [
        k for k in cache._store
        if k.startswith("fmp:ohlc:") and not k.startswith("fmp:ohlc:lkg:")
    ]
    lkg_keys = [
        k for k in cache._store
        if k.startswith("fmp:ohlc:lkg:")
    ]

    return {
        "counters": {
            "cache_hits":          _DIAG["hits"],
            "lkg_hits":            _DIAG["lkg_hits"],
            "misses":              _DIAG["misses"],
            "total_reads":         total_reads,
            "hit_rate_pct":        hit_rate,
            "refreshes_triggered": _DIAG["refreshes_triggered"],
            "fmp_calls":           _DIAG["fmp_calls"],
            "fmp_errors":          _DIAG["fmp_errors"],
        },
        "last_refresh": {
            "elapsed_ms": _DIAG["last_refresh_ms"],
            "at":         _DIAG["last_refresh_ts"],
        },
        "cache_inventory": {
            "hot_keys":  len(warmed_keys),
            "lkg_keys":  len(lkg_keys),
            "samples":   warmed_keys[:20],
        },
    }


# ── Exported warming helper (called from main.py lifespan) ────────────────────

async def warm_chart_radar_ohlc(
    symbols:  list[str],
    interval: str = "15m",
    range_str: str = "10d",
) -> None:
    """
    Pre-warm OHLC cache for the given symbol list at startup.
    Only fetches symbols not already in hot cache.
    Safe to call as asyncio.create_task() — never raises.
    """
    if not symbols:
        return
    cold = [
        s for s in symbols
        if cache.get(_ohlc_hot_key(interval, range_str, s.upper())) is None
    ]
    if not cold:
        print(
            f"[CHART_RADAR_OHLC] warm: all {len(symbols)} symbols already cached "
            f"({interval}/{range_str})"
        )
        return
    print(
        f"[CHART_RADAR_OHLC] warm: fetching {len(cold)}/{len(symbols)} cold symbols "
        f"({interval}/{range_str})"
    )
    await _bg_refresh_ohlc(cold, interval, range_str)
