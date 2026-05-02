"""
Screener Hub service.

Backs the four Screener Hub tabs:
  thematic              — one-or-more curated theme universes (THEME_RS_UNIVERSE keys)
  social                — tickers driven by the X/Grok consensus weekly snapshot
  bottlenecks           — Chain Reaction NODE_REGISTRY tickers
  watchlist_portfolio   — user watchlists + portfolio holdings

Layered cache design:
  - Universe symbols are persisted to screener_universe_snapshots (per tab/theme)
  - Fundamentals are persisted to screener_fundamentals_cache (weekly TTL, FMP)
  - Live quotes are persisted to screener_quote_cache (Tradier; short TTL)

Guardrails enforced here (see CLAUDE.md):
  - Never overwrite a valid cached row with an empty/failed API response.
  - Never blank the whole table because one row failed enrichment.
  - Tradier quote refresh is page-aware: only the symbols requested by the
    current /api/screener-hub call are re-fetched.
  - If FMP fails for a symbol, we keep its previous cached row.
"""
from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

import httpx

from data.screener_hub_store import (
    ensure_tables,
    upsert_fundamentals,
    get_fundamentals,
    fundamentals_fresh_symbols,
    fundamentals_table_stats,
    insert_universe_snapshot,
    get_latest_universe,
    universe_table_stats,
    upsert_quote,
    get_quotes,
    quote_table_stats,
    start_job_run,
    finish_job_run,
    latest_job_runs,
)


# ── Config ─────────────────────────────────────────────────────────────────────

FMP_BASE = "https://financialmodelingprep.com/stable"
_FMP_TIMEOUT = 10.0
_PER_THEME_CAP = 60
_GLOBAL_TICKER_CAP = 400  # safety net per request
_FUNDAMENTALS_TTL_DAYS = 7
_QUOTE_TTL_OPEN_S = 90        # ~90s during US market open
_QUOTE_TTL_CLOSED_S = 30 * 60 # 30min when market closed
_FMP_SLEEP_BETWEEN_S = 6.0   # 5-15s between FMP calls during warm jobs


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_market_open() -> bool:
    """Approximate US equity hours (NYSE) — used only to pick a quote TTL."""
    try:
        from zoneinfo import ZoneInfo
        et = ZoneInfo("America/New_York")
    except Exception:
        return False
    now = datetime.now(et)
    if now.weekday() >= 5:
        return False
    minutes = now.hour * 60 + now.minute
    return 9 * 60 + 30 <= minutes < 16 * 60


# ── Universe builders ─────────────────────────────────────────────────────────

def _theme_keys() -> list[str]:
    try:
        from services.theme_rs_universe import THEME_RS_UNIVERSE
        return sorted(THEME_RS_UNIVERSE.keys())
    except Exception:
        return []


def _theme_metadata() -> list[dict]:
    """All themes with display name + classification, for /api/screener-hub/themes."""
    out: list[dict] = []
    try:
        from services.theme_rs_universe import THEME_RS_UNIVERSE
        for key, meta in THEME_RS_UNIVERSE.items():
            label = meta.get("display_name") or key
            out.append({
                "theme_key":      key,
                "display_name":   label,
                # Aliases expected by the ScreenerHub frontend component
                "id":             key,
                "label":          label,
                "classification": meta.get("classification") or "theme",
                "parent_sector":  meta.get("parent_sector"),
                "proxy_symbols":  list(meta.get("proxy_symbols") or [])[:5],
            })
    except Exception as e:
        print(f"[SCREENER_HUB] theme registry load error: {e}")
    out.sort(key=lambda r: (r.get("classification") or "", r.get("display_name") or ""))
    return out


async def _build_thematic_universe(theme_key: Optional[str]) -> dict[str, list[str]]:
    """Return {theme_key: [symbols]} map.

    Uses the dynamic_thematic_universe service (ETF holdings + FMP peers + X
    consensus + static anchors). Fall back to THEME_RS_UNIVERSE
    representative tickers if the dynamic service is cold or fails.
    """
    keys = [theme_key] if theme_key else _theme_keys()
    out: dict[str, list[str]] = {}

    # Try dynamic universe first — already cached/refreshed by main.py loop.
    dyn_map: dict[str, list[str]] = {}
    try:
        from services.dynamic_thematic_universe import get_cached_thematic_universe
        snap = get_cached_thematic_universe()
        theme_map = (snap or {}).get("theme_map") or {}
        for ticker, info in theme_map.items():
            if not isinstance(info, dict):
                continue
            theme_name = info.get("theme_name") or ""
            # We don't always have a stable key; use display_name lookup below.
            dyn_map.setdefault(theme_name, []).append(ticker)
    except Exception as e:
        print(f"[SCREENER_HUB] dynamic thematic load error: {e}")

    try:
        from services.theme_rs_universe import THEME_RS_UNIVERSE
    except Exception:
        THEME_RS_UNIVERSE = {}

    for key in keys:
        meta = (THEME_RS_UNIVERSE or {}).get(key) or {}
        display = (meta.get("display_name") or key).strip()
        symbols: list[str] = []

        # Dynamic-universe matches by display_name first
        for cand_name, syms in dyn_map.items():
            if cand_name and cand_name.lower() == display.lower():
                symbols.extend(syms)

        # Augment / fall back to representative tickers from the registry.
        for s in (meta.get("representative_tickers") or []):
            if s and s.upper() not in symbols:
                symbols.append(s.upper())

        # Always include proxy ETFs at the bottom — useful for theme leaders.
        for s in (meta.get("proxy_symbols") or []):
            if s and s.upper() not in symbols:
                symbols.append(s.upper())

        cleaned = _dedupe_filter(symbols)[:_PER_THEME_CAP]
        if cleaned:
            out[key] = cleaned

    return out


def _build_social_universe() -> list[str]:
    """X consensus weekly top tickers + backend ranked tickers."""
    syms: list[str] = []
    try:
        import json
        from pathlib import Path
        path = Path(__file__).parent.parent / "data" / "x_consensus_weekly.json"
        if path.exists():
            data = json.loads(path.read_text())
            for item in (data.get("top_tickers") or []):
                if isinstance(item, dict):
                    s = item.get("symbol") or item.get("ticker")
                    if s:
                        syms.append(str(s).upper())
                elif isinstance(item, str):
                    syms.append(item.upper())
            for item in (data.get("_backend_ranked") or []):
                if isinstance(item, dict):
                    s = item.get("symbol") or item.get("ticker")
                    if s:
                        syms.append(str(s).upper())
    except Exception as e:
        print(f"[SCREENER_HUB] social universe load error: {e}")
    return _dedupe_filter(syms)[:_GLOBAL_TICKER_CAP]


def _build_bottlenecks_universe() -> list[str]:
    """Chain Reaction NODE_REGISTRY — ranked by bottleneck_score."""
    out: list[tuple[str, int]] = []
    try:
        from services.playbook.supply_chain_graph import NODE_REGISTRY
        for ticker, node in (NODE_REGISTRY or {}).items():
            if not isinstance(node, dict):
                continue
            score = int(node.get("bottleneck_score") or 0)
            # Prefer the US-listed proxy when the native ticker isn't tradeable here
            us_proxy = (node.get("us_access_proxy")
                        or node.get("adr_ticker")
                        or ticker)
            out.append((str(us_proxy).upper(), score))
    except Exception as e:
        print(f"[SCREENER_HUB] bottlenecks load error: {e}")
    out.sort(key=lambda r: r[1], reverse=True)
    seen: set[str] = set()
    syms: list[str] = []
    for s, _ in out:
        if s and s not in seen:
            seen.add(s)
            syms.append(s)
    return syms[:_GLOBAL_TICKER_CAP]


def _build_watchlist_portfolio_universe() -> list[str]:
    syms: set[str] = set()
    try:
        from services.watchlist_service import list_watchlists, load_watchlist
        for wl in (list_watchlists() or [])[:10]:
            wl_id = wl.get("id") if isinstance(wl, dict) else None
            if not wl_id:
                continue
            store = load_watchlist(wl_id)
            if isinstance(store, dict):
                for t in (store.get("tickers") or []):
                    if isinstance(t, str):
                        syms.add(t.upper())
                    elif isinstance(t, dict) and t.get("symbol"):
                        syms.add(str(t["symbol"]).upper())
    except Exception as e:
        print(f"[SCREENER_HUB] watchlist load error: {e}")
    try:
        import json
        from pathlib import Path
        for p in Path(__file__).parent.parent.joinpath("data").glob("portfolio_holdings*.json"):
            try:
                data = json.loads(p.read_text())
                holdings = data.get("holdings", data) if isinstance(data, dict) else data
                if isinstance(holdings, list):
                    for h in holdings:
                        if isinstance(h, dict):
                            s = h.get("symbol") or h.get("ticker")
                            if s:
                                syms.add(str(s).upper())
            except Exception:
                continue
    except Exception as e:
        print(f"[SCREENER_HUB] portfolio load error: {e}")
    return _dedupe_filter(sorted(syms))


_BAD_PREFIX = ("$", ".", "^")


def _dedupe_filter(symbols: Iterable[str]) -> list[str]:
    """Dedupe + strip obviously-non-equity tickers. Order-preserving."""
    seen: set[str] = set()
    out: list[str] = []
    for raw in symbols:
        if not isinstance(raw, str):
            continue
        s = raw.strip().upper()
        if not s or len(s) > 6:
            continue
        if s.startswith(_BAD_PREFIX):
            continue
        if not s.replace(".", "").replace("-", "").isalnum():
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


# ── FMP fundamentals fetch (one symbol at a time) ─────────────────────────────

async def _fmp_get(client: httpx.AsyncClient, endpoint: str, params: dict) -> Any:
    api_key = os.getenv("FMP_API_KEY") or ""
    if not api_key:
        return None
    qp = dict(params or {})
    qp["apikey"] = api_key
    try:
        r = await client.get(f"{FMP_BASE}/{endpoint}", params=qp, timeout=_FMP_TIMEOUT)
        if r.status_code == 200:
            return r.json()
        return None
    except Exception as e:
        print(f"[SCREENER_HUB] FMP {endpoint} error: {e}")
        return None


async def _fetch_fundamentals_for_symbol(
    client: httpx.AsyncClient, symbol: str
) -> Optional[dict]:
    """One symbol → profile + key-metrics-ttm + ratios-ttm. Returns None on hard failure."""
    profile = await _fmp_get(client, "profile", {"symbol": symbol})
    if isinstance(profile, list) and profile:
        profile = profile[0]
    if not isinstance(profile, dict):
        # Profile is the anchor; if missing we treat the row as unfetched.
        return None
    metrics = await _fmp_get(client, "key-metrics-ttm", {"symbol": symbol})
    if isinstance(metrics, list) and metrics:
        metrics = metrics[0]
    if not isinstance(metrics, dict):
        metrics = {}
    ratios = await _fmp_get(client, "ratios-ttm", {"symbol": symbol})
    if isinstance(ratios, list) and ratios:
        ratios = ratios[0]
    if not isinstance(ratios, dict):
        ratios = {}
    return {
        "profile": profile,
        "metrics": metrics,
        "ratios":  ratios,
        "market_cap": _to_float(profile.get("marketCap") or profile.get("mktCap")),
        "sector":   profile.get("sector"),
        "industry": profile.get("industry"),
        "country":  profile.get("country"),
        "exchange": profile.get("exchangeShortName") or profile.get("exchange"),
    }


def _to_float(v: Any) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


# ── Warm fundamentals (used by jobs + synchronous best-effort path) ───────────

async def warm_fundamentals(
    symbols: Iterable[str],
    *,
    job_name: str,
    force: bool = False,
    sleep_between_s: float = _FMP_SLEEP_BETWEEN_S,
    max_calls: int = 250,
) -> dict:
    """Sequentially fetch FMP fundamentals for each symbol, respecting cache.

    Returns a summary dict. Records a row in screener_job_runs.
    """
    deduped = _dedupe_filter(symbols)
    run_id = start_job_run(job_name, symbols_count=len(deduped),
                           metadata={"force": bool(force)})
    completed = 0
    failed = 0
    api_calls = 0
    error_msg: Optional[str] = None

    try:
        if not force:
            fresh = fundamentals_fresh_symbols(deduped, max_age_days=_FUNDAMENTALS_TTL_DAYS)
            queue = [s for s in deduped if s not in fresh]
            print(f"[SCREENER_HUB] {job_name}: {len(deduped)} total, {len(queue)} stale, {len(fresh)} fresh")
        else:
            queue = list(deduped)
            print(f"[SCREENER_HUB] {job_name}: force=True, processing all {len(queue)}")

        async with httpx.AsyncClient() as client:
            for idx, symbol in enumerate(queue):
                if api_calls >= max_calls:
                    print(f"[SCREENER_HUB] {job_name}: max_calls={max_calls} reached, stopping")
                    break
                try:
                    record = await _fetch_fundamentals_for_symbol(client, symbol)
                    api_calls += 3  # profile + metrics + ratios
                    if record is None:
                        failed += 1
                    else:
                        ok = upsert_fundamentals(
                            symbol,
                            profile=record["profile"],
                            metrics=record["metrics"],
                            ratios=record["ratios"],
                            market_cap=record["market_cap"],
                            sector=record["sector"],
                            industry=record["industry"],
                            country=record["country"],
                            exchange=record["exchange"],
                            ttl_days=_FUNDAMENTALS_TTL_DAYS,
                        )
                        if ok:
                            completed += 1
                        else:
                            failed += 1
                except Exception as e:
                    failed += 1
                    print(f"[SCREENER_HUB] {job_name} {symbol} error: {e}")

                # Polite delay between calls (skip after the last one)
                if idx < len(queue) - 1 and sleep_between_s > 0:
                    await asyncio.sleep(sleep_between_s)

        status = "ok" if failed == 0 else ("partial" if completed > 0 else "failed")
        finish_job_run(
            run_id, status=status,
            symbols_completed=completed, symbols_failed=failed,
            api_calls_used=api_calls,
            error=None,
            metadata={"queue_size": len(queue)},
        )
        return {
            "job_name": job_name,
            "status": status,
            "symbols_count": len(deduped),
            "symbols_completed": completed,
            "symbols_failed": failed,
            "api_calls_used": api_calls,
        }
    except Exception as e:
        error_msg = str(e)
        finish_job_run(
            run_id, status="failed",
            symbols_completed=completed, symbols_failed=failed,
            api_calls_used=api_calls, error=error_msg,
        )
        return {
            "job_name": job_name,
            "status": "failed",
            "symbols_count": len(deduped),
            "symbols_completed": completed,
            "symbols_failed": failed,
            "api_calls_used": api_calls,
            "error": error_msg,
        }


# ── Quote refresh (page-aware; Tradier only) ──────────────────────────────────

async def refresh_quotes_for_page(symbols: Iterable[str]) -> dict:
    """Refresh Tradier quotes only for the symbols on the active page.

    Honors a short TTL to coalesce multiple frontend hits. Never blanks rows;
    on Tradier failure we leave the existing cached row in place.
    """
    deduped = _dedupe_filter(symbols)
    if not deduped:
        return {"status": "ok", "refreshed": 0, "cached_used": 0}

    ttl = _QUOTE_TTL_OPEN_S if _is_market_open() else _QUOTE_TTL_CLOSED_S
    cached = get_quotes(deduped)
    now_ts = time.time()
    stale: list[str] = []
    for s in deduped:
        row = cached.get(s)
        if not row or not row.get("fetched_at"):
            stale.append(s)
            continue
        try:
            fetched = datetime.fromisoformat(row["fetched_at"].replace("Z", "+00:00"))
        except Exception:
            stale.append(s)
            continue
        age = now_ts - fetched.timestamp()
        if age > ttl:
            stale.append(s)

    if not stale:
        return {"status": "ok", "refreshed": 0, "cached_used": len(cached)}

    api_key = os.getenv("TRADIER_API_KEY") or ""
    sandbox = (os.getenv("TRADIER_SANDBOX", "false") or "false").lower() in ("1", "true", "yes")
    if not api_key:
        return {"status": "no_provider", "refreshed": 0, "cached_used": len(cached)}

    try:
        from data.tradier_provider import TradierProvider
        provider = TradierProvider(api_key, sandbox=sandbox)
    except Exception as e:
        print(f"[SCREENER_HUB] Tradier init error: {e}")
        return {"status": "error", "refreshed": 0, "cached_used": len(cached)}

    refreshed = 0
    # Tradier accepts comma-separated batches; chunk to keep URLs short.
    batch_size = 50
    for i in range(0, len(stale), batch_size):
        chunk = stale[i:i + batch_size]
        try:
            quotes = await provider.get_quotes(chunk)
        except Exception as e:
            print(f"[SCREENER_HUB] Tradier batch error ({i}): {e}")
            continue
        if not quotes:
            continue
        for q in quotes:
            sym = (q.get("symbol") or "").upper()
            if not sym:
                continue
            price = q.get("last") if q.get("last") is not None else q.get("close")
            change_pct = q.get("change_percentage")
            if price is None and change_pct is None:
                # Don't overwrite a valid cached row with empty data.
                continue
            ok = upsert_quote(
                sym,
                quote=q,
                price=_to_float(price),
                change_percent_1d=_to_float(change_pct),
                provider="tradier_sandbox" if sandbox else "tradier",
            )
            if ok:
                refreshed += 1

    return {"status": "ok", "refreshed": refreshed, "cached_used": len(deduped) - refreshed}


# ── Classification (Leading / Improving / Weakening / Lagging) ────────────────

def _classify_row(metrics: dict, quote: dict, *, score_mode: bool, coc_filter: bool) -> dict:
    """Compute screener category + score using whatever signals we have."""
    # Pull whatever metrics happen to be present in profile/metrics/quote.
    distance_52w_high: Optional[float] = None
    week_52_high = quote.get("week_52_high") if isinstance(quote, dict) else None
    last = quote.get("last") if isinstance(quote, dict) else None
    if week_52_high and last:
        try:
            if float(week_52_high) > 0:
                distance_52w_high = round(
                    (float(last) - float(week_52_high)) / float(week_52_high) * 100, 2
                )
        except (TypeError, ValueError):
            distance_52w_high = None

    chg_1d = _to_float(quote.get("change_percentage")) if isinstance(quote, dict) else None
    avg_vol = _to_float(quote.get("average_volume")) if isinstance(quote, dict) else None
    vol     = _to_float(quote.get("volume")) if isinstance(quote, dict) else None
    volume_surge: Optional[float] = None
    if vol and avg_vol and avg_vol > 0:
        volume_surge = round(vol / avg_vol, 3)

    # Relative strength placeholders — we don't have multi-week history here,
    # so we expose the cheapest proxy (1D change) so the frontend can render
    # *something*; the heavier RS rebuild lives in theme_rs_service.
    rs_0_2w = chg_1d
    rs_0_4w = chg_1d
    rs_0_10w = chg_1d
    rs_accel: Optional[float] = None

    accumulation: Optional[bool] = None
    if volume_surge is not None and chg_1d is not None:
        accumulation = bool(volume_surge >= 1.2 and chg_1d > 0)

    coc: Optional[bool] = None  # CoC filter not implemented; safe-null

    # Signal scoring
    signals = {
        "rs_positive":   bool(rs_0_4w is not None and rs_0_4w > 0),
        "near_52w_high": bool(distance_52w_high is not None and distance_52w_high > -10),
        "vol_surge":     bool(volume_surge is not None and volume_surge >= 1.5),
        "accumulation":  bool(accumulation),
    }
    score = sum(1 for v in signals.values() if v)

    # Category — Leading / Improving / Weakening / Lagging
    if score >= 3:
        category = "Leading"
    elif score == 2:
        category = "Improving"
    elif score == 1:
        category = "Weakening"
    else:
        category = "Lagging"

    return {
        "rs_0_2w":  rs_0_2w,
        "rs_0_4w":  rs_0_4w,
        "rs_0_10w": rs_0_10w,
        "rs_accel": rs_accel,
        "distance_52w_high": distance_52w_high,
        "volume_surge": volume_surge,
        "accumulation": accumulation,
        "coc": coc,
        "score": score if score_mode else None,
        "category": category,
        "_signals": signals,
    }


def _row_passes_filters(row: dict, *, category_filter: Optional[str],
                        coc_filter: bool) -> bool:
    if category_filter and row.get("category") != category_filter:
        return False
    if coc_filter and row.get("coc") is not True:
        return False
    return True


# ── Main page query ───────────────────────────────────────────────────────────

async def get_screener_hub(
    *,
    tab: str,
    theme: Optional[str] = None,
    category: Optional[str] = None,
    score_mode: bool = False,
    coc_filter: bool = False,
) -> dict:
    """Build the response payload for /api/screener-hub.

    The shape matches the contract requested by the frontend:
      { status, tab, theme, generated_at, fundamentals_cache_status,
        quote_cache_status, rows: [...] }

    Universe symbols come from the latest screener_universe_snapshots row.
    Fundamentals come from screener_fundamentals_cache (no live FMP fetch).
    Quotes come from screener_quote_cache; we refresh stale rows for *just*
    the active page before returning.
    """
    ensure_tables()

    tab = (tab or "").strip().lower()
    theme = (theme or "").strip().lower() or None
    if tab not in ("thematic", "social", "bottlenecks", "watchlist_portfolio"):
        return {
            "status": "error",
            "error": f"unknown tab '{tab}'",
            "tab": tab,
            "theme": theme,
            "generated_at": _now_iso(),
            "rows": [],
        }

    symbols: list[str] = []
    snap_status = "fresh"
    universe_source = "snapshot"

    # ── Resolve universe ──
    if tab == "thematic":
        if theme:
            snap = get_latest_universe("thematic", theme)
            if snap and snap.get("symbols"):
                symbols = list(snap.get("symbols") or [])
            else:
                # No snapshot yet — build one synchronously from registries.
                built = await _build_thematic_universe(theme)
                symbols = built.get(theme, [])
                snap_status = "live_fallback"
                universe_source = "live"
        else:
            # No theme → flatten symbols across all themes (de-dupe).
            built = await _build_thematic_universe(None)
            seen: set[str] = set()
            for syms in built.values():
                for s in syms:
                    if s not in seen:
                        seen.add(s)
                        symbols.append(s)
            snap_status = "live_aggregated"
            universe_source = "live"
    elif tab == "social":
        snap = get_latest_universe("social")
        symbols = list(snap.get("symbols") or []) if snap else []
        if not symbols:
            symbols = _build_social_universe()
            snap_status = "live_fallback"
            universe_source = "live"
    elif tab == "bottlenecks":
        snap = get_latest_universe("bottlenecks")
        symbols = list(snap.get("symbols") or []) if snap else []
        if not symbols:
            symbols = _build_bottlenecks_universe()
            snap_status = "live_fallback"
            universe_source = "live"
    elif tab == "watchlist_portfolio":
        # Always live — depends on the user's current watchlists.
        symbols = _build_watchlist_portfolio_universe()
        universe_source = "live"

    symbols = _dedupe_filter(symbols)[:_GLOBAL_TICKER_CAP]

    # ── Refresh page-aware quotes ──
    quote_cache_status = "skipped"
    if symbols:
        quote_summary = await refresh_quotes_for_page(symbols)
        quote_cache_status = quote_summary.get("status", "unknown")

    # ── Read fundamentals + quotes from cache ──
    fundamentals = get_fundamentals(symbols) if symbols else {}
    quotes_map   = get_quotes(symbols) if symbols else {}

    fund_total = len(fundamentals)
    fund_fresh = len(fundamentals_fresh_symbols(symbols, max_age_days=_FUNDAMENTALS_TTL_DAYS))
    fundamentals_cache_status = (
        "fresh" if fund_fresh == len(symbols) and len(symbols) > 0 else
        "partial" if fund_total > 0 else
        "cold"
    )

    # ── Build rows (NEVER omit a symbol just because enrichment failed) ──
    rows: list[dict] = []
    for sym in symbols:
        f = fundamentals.get(sym) or {}
        q_row = quotes_map.get(sym) or {}
        q = q_row.get("quote") if isinstance(q_row.get("quote"), dict) else {}

        profile = f.get("profile") or {}
        metrics = f.get("metrics") or {}
        ratios  = f.get("ratios")  or {}

        classification = _classify_row(
            metrics, q, score_mode=score_mode, coc_filter=coc_filter,
        )

        row = {
            "symbol": sym,
            "name":     profile.get("companyName") or profile.get("name") or sym,
            "history":  None,  # populated by frontend chart endpoint, if any
            "category": classification["category"],
            "rs_0_2w":  classification["rs_0_2w"],
            "rs_0_4w":  classification["rs_0_4w"],
            "rs_0_10w": classification["rs_0_10w"],
            "rs_accel": classification["rs_accel"],
            "distance_52w_high": classification["distance_52w_high"],
            "volume_surge":      classification["volume_surge"],
            "accumulation":      classification["accumulation"],
            "coc":               classification["coc"],
            "score":             classification["score"],
            "market_cap": f.get("market_cap") or profile.get("marketCap"),
            "sector":     f.get("sector"),
            "industry":   f.get("industry"),
            "price":      q_row.get("price"),
            "change_percent_1d": q_row.get("change_percent_1d"),
            "performance_7d":  None,
            "performance_30d": None,
            "performance_ytd": None,
            "performance_1y":  None,
            "_meta": {
                "country":  f.get("country"),
                "exchange": f.get("exchange"),
                "fundamentals_fetched_at": f.get("fetched_at"),
                "quote_fetched_at":        q_row.get("fetched_at"),
                "fundamentals_provider":   f.get("provider"),
                "quote_provider":          q_row.get("provider"),
                "ratios_pe":               ratios.get("priceEarningsRatioTTM"),
                "ratios_ps":               ratios.get("priceToSalesRatioTTM"),
                "key_metric_roe":          metrics.get("roeTTM"),
                "signals":                 classification.get("_signals"),
            },
        }
        if not _row_passes_filters(row, category_filter=category, coc_filter=coc_filter):
            continue
        rows.append(row)

    return {
        "status": "ok",
        "tab": tab,
        "theme": theme,
        "generated_at": _now_iso(),
        "fundamentals_cache_status": fundamentals_cache_status,
        "quote_cache_status":        quote_cache_status,
        "universe_source":           universe_source,
        "universe_status":           snap_status,
        "row_count":                 len(rows),
        "rows": rows,
    }


# ── Rebuild orchestration (universes + warm jobs) ─────────────────────────────

async def rebuild_universe(
    tab: str,
    *,
    theme: Optional[str] = None,
    force: bool = False,
) -> dict:
    """Rebuild a single tab's universe snapshot(s).

    For thematic, rebuilds *all* themes when theme is None.
    """
    tab = tab.strip().lower()
    out: dict[str, Any] = {"tab": tab, "theme": theme, "force": bool(force)}

    if tab == "thematic":
        built = await _build_thematic_universe(theme)
        out["themes_built"] = []
        for k, syms in built.items():
            ok = insert_universe_snapshot(
                universe_type="thematic", theme_key=k,
                symbols=syms, source="thematic_rebuild",
                status="ok", ttl_days=8,
            )
            out["themes_built"].append({"theme": k, "symbols_count": len(syms), "ok": ok})
    elif tab == "social":
        syms = _build_social_universe()
        ok = insert_universe_snapshot(
            universe_type="social", theme_key=None,
            symbols=syms, source="x_consensus", status="ok", ttl_days=2,
        )
        out["symbols_count"] = len(syms)
        out["snapshot_ok"]   = ok
    elif tab == "bottlenecks":
        syms = _build_bottlenecks_universe()
        ok = insert_universe_snapshot(
            universe_type="bottlenecks", theme_key=None,
            symbols=syms, source="chain_reaction", status="ok", ttl_days=10,
        )
        out["symbols_count"] = len(syms)
        out["snapshot_ok"]   = ok
    elif tab == "watchlist_portfolio":
        syms = _build_watchlist_portfolio_universe()
        ok = insert_universe_snapshot(
            universe_type="watchlist_portfolio", theme_key=None,
            symbols=syms, source="user_data", status="ok", ttl_days=2,
        )
        out["symbols_count"] = len(syms)
        out["snapshot_ok"]   = ok
    elif tab == "all":
        out["thematic"]            = await rebuild_universe("thematic", force=force)
        out["social"]              = await rebuild_universe("social", force=force)
        out["bottlenecks"]         = await rebuild_universe("bottlenecks", force=force)
        out["watchlist_portfolio"] = await rebuild_universe("watchlist_portfolio", force=force)
    else:
        out["error"] = f"unknown tab '{tab}'"

    return out


async def warm_tab_fundamentals(
    tab: str,
    *,
    theme: Optional[str] = None,
    force: bool = False,
    max_calls: int = 250,
) -> dict:
    """Run the fundamentals warm job for a tab's universe."""
    tab = tab.strip().lower()
    if tab == "thematic":
        if theme:
            snap = get_latest_universe("thematic", theme)
            symbols = list((snap or {}).get("symbols") or [])
            if not symbols:
                built = await _build_thematic_universe(theme)
                symbols = built.get(theme, [])
            return await warm_fundamentals(
                symbols, job_name=f"thematic_warm:{theme}", force=force, max_calls=max_calls,
            )
        else:
            # Aggregate all theme symbols (deduped)
            built = await _build_thematic_universe(None)
            agg: list[str] = []
            seen: set[str] = set()
            for k, syms in built.items():
                # Persist a snapshot per theme too (cheap, idempotent)
                insert_universe_snapshot(
                    universe_type="thematic", theme_key=k,
                    symbols=syms, source="warm_job", ttl_days=8,
                )
                for s in syms:
                    if s not in seen:
                        seen.add(s)
                        agg.append(s)
            return await warm_fundamentals(
                agg, job_name="thematic_warm:all", force=force, max_calls=max_calls,
            )
    if tab in ("social", "bottlenecks", "watchlist_portfolio"):
        snap = get_latest_universe(tab)
        symbols = list((snap or {}).get("symbols") or [])
        if not symbols:
            await rebuild_universe(tab, force=False)
            snap = get_latest_universe(tab)
            symbols = list((snap or {}).get("symbols") or [])
        return await warm_fundamentals(
            symbols, job_name=f"{tab}_warm", force=force, max_calls=max_calls,
        )
    if tab == "all":
        out: dict[str, Any] = {}
        for t in ("thematic", "social", "bottlenecks", "watchlist_portfolio"):
            out[t] = await warm_tab_fundamentals(t, force=force, max_calls=max_calls)
        return out
    return {"error": f"unknown tab '{tab}'"}


# ── Status / diagnostics ──────────────────────────────────────────────────────

def get_admin_status() -> dict:
    return {
        "as_of": _now_iso(),
        "fundamentals_cache": fundamentals_table_stats(),
        "universe_snapshots": universe_table_stats(),
        "quote_cache":        quote_table_stats(),
        "latest_job_runs":    latest_job_runs(limit=20),
    }
