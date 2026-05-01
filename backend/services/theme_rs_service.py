"""
Themes by Relative Strength — production-hardened canonical service.

Exposes:
    get_theme_rs_data(timeframe, force) → dict

Provider hierarchy (no LLM calls):
  1D quote:        Tradier batch → Finnhub individual fallback
  7D/30D/YTD/1Y:  FMP stable historical-price-eod primary
                   → Tradier daily history fallback
                   → yfinance emergency fallback

Leader/laggard universe (per theme, dynamic):
  1. ETF holdings from primary proxy ETF (etf_holdings_service, 7-day cache)
  2. X/Grok consensus tickers from disk snapshot (read-only, no new calls)
  3. Static candidate_symbols as last-resort fallback seeds

DRAM special handling:
  memory_storage tries DRAM first for theme return; falls back to SMH/SOXX.

State:
  active / emerging / neutral / weakening / dead_zone
  + state_reason human-readable text for every theme.

Cache:
  key:  themes:relative_strength:v1:{tf}
  TTL:  900s market hours / 3600s off-hours
  disk: backend/data/themes_rs_lkg.json (atomic write, never overwrite with bad data)
"""
from __future__ import annotations

import asyncio
import json
import os
import statistics
import time
from datetime import datetime, date, timezone, timedelta
from pathlib import Path
from typing import Optional

import httpx

from data.cache import cache
from services.theme_rs_universe import (
    THEME_RS_UNIVERSE,
    ALL_PROXY_SYMBOLS,
    ALL_CANDIDATE_SYMBOLS,
)
from services.sector_rotation.analytics import _pct_change, _ytd_change, _sma
from services.sector_rotation.providers import (
    fetch_etf_history,          # yfinance executor (emergency fallback)
    _tradier_quotes_batch,
    _finnhub_quote_single,
    _tradier_key,
    _tradier_base,
)


# ── Constants ──────────────────────────────────────────────────────────────────

_CACHE_KEY   = "themes:relative_strength:v1"
_LKG_PATH    = Path(__file__).parent.parent / "data" / "themes_rs_lkg.json"
_XC_PATH     = Path(__file__).parent.parent / "data" / "x_consensus_weekly.json"
_HIST_TTL    = 3600       # 1h — daily bars don't change intraday
_BENCHMARKS  = ["SPY", "QQQ"]

_TIMEFRAME_BARS: dict[str, int] = {
    "1D":  1,
    "7D":  5,
    "30D": 22,
    "YTD": 0,   # special: _ytd_change()
    "1Y":  252,
}

# Top N holdings to pull from primary proxy ETF for leader/laggard universe
_ETF_HOLDINGS_TOP_N = 10

# Semaphore for FMP history calls (parallel bursting within rate-limit)
_FMP_HIST_SEM = asyncio.Semaphore(25)

# ── TTL helpers ────────────────────────────────────────────────────────────────

def _market_hours_ttl() -> int:
    now = datetime.now(tz=timezone.utc)
    if now.weekday() >= 5:
        return 3600
    month = now.month
    utc_offset = 4 if 4 <= month <= 10 else 5
    et_min = (now.hour - utc_offset) % 24 * 60 + now.minute
    return 900 if 570 <= et_min <= 960 else 3600


def _is_market_hours() -> bool:
    return _market_hours_ttl() == 900


# ── LKG helpers ────────────────────────────────────────────────────────────────

def _load_lkg() -> Optional[list[dict]]:
    try:
        if not _LKG_PATH.exists():
            return None
        raw = json.loads(_LKG_PATH.read_text())
        if isinstance(raw, list) and raw:
            return raw
    except Exception as e:
        print(f"[THEME_RS] LKG load error: {e}")
    return None


def _save_lkg(data: list[dict]) -> None:
    """Atomic write — never overwrites valid snapshot with bad data."""
    if not data:
        print("[THEME_RS] LKG save skipped — empty result")
        return
    try:
        _LKG_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = _LKG_PATH.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(data, indent=2, default=str))
        tmp.replace(_LKG_PATH)
    except Exception as e:
        print(f"[THEME_RS] LKG save error: {e}")


# ── FMP historical price provider ──────────────────────────────────────────────

def _fmp_key() -> str:
    return os.getenv("FMP_API_KEY", "")


async def _fetch_fmp_daily_history(symbol: str) -> list[dict]:
    """
    FMP stable/historical-price-eod/full — primary historical provider.
    Returns sorted list of {date, close} bars, newest-last.
    Cached 1h in-process.
    """
    sym = symbol.upper()
    cache_key = f"fmp_hist:{sym}"
    hit = cache.get(cache_key)
    if hit is not None:
        return hit

    key = _fmp_key()
    if not key:
        return []

    async with _FMP_HIST_SEM:
        try:
            async with httpx.AsyncClient(timeout=12.0) as client:
                resp = await client.get(
                    "https://financialmodelingprep.com/stable/historical-price-eod/full",
                    params={"symbol": sym, "apikey": key},
                )
            if resp.status_code not in (200, 201):
                if resp.status_code not in (403, 402, 404):
                    print(f"[THEME_RS][FMP hist] {sym} HTTP {resp.status_code}")
                return []
            raw = resp.json()
            # FMP stable returns: list of {date, open, high, low, close, volume, ...}
            if isinstance(raw, list):
                bars_raw = raw
            elif isinstance(raw, dict):
                # Some variants wrap in {"historical": [...]}
                bars_raw = raw.get("historical") or []
            else:
                return []

            bars = []
            for b in bars_raw:
                if not isinstance(b, dict):
                    continue
                d = b.get("date") or b.get("formattedDate") or ""
                c = b.get("close") or b.get("adjClose")
                if d and c is not None:
                    try:
                        bars.append({"date": str(d)[:10], "close": float(c)})
                    except (TypeError, ValueError):
                        pass

            bars.sort(key=lambda r: r["date"])
            if bars:
                print(f"[THEME_RS][FMP hist] {sym}: {len(bars)} bars ✓")
                cache.set(cache_key, bars, _HIST_TTL)
            return bars

        except Exception as e:
            print(f"[THEME_RS][FMP hist] {sym}: {e}")
            return []


async def _fetch_tradier_daily_history(symbol: str, days: int = 400) -> list[dict]:
    """
    Tradier /markets/history — secondary historical provider.
    Returns sorted {date, close} bars.
    Cached 1h.
    """
    sym = symbol.upper()
    cache_key = f"tdier_hist:{sym}:{days}"
    hit = cache.get(cache_key)
    if hit is not None:
        return hit

    key = _tradier_key()
    if not key:
        return []

    start = (date.today() - timedelta(days=days)).isoformat()
    end   = date.today().isoformat()
    base  = _tradier_base()

    try:
        async with httpx.AsyncClient(timeout=12.0) as client:
            resp = await client.get(
                f"{base}/markets/history",
                headers={"Authorization": f"Bearer {key}", "Accept": "application/json"},
                params={"symbol": sym, "interval": "daily", "start": start, "end": end},
            )
        if resp.status_code != 200:
            return []
        data    = resp.json()
        history = data.get("history") or {}
        days_raw = history.get("day") or []
        if isinstance(days_raw, dict):
            days_raw = [days_raw]

        bars = []
        for d in days_raw:
            dt  = d.get("date") or ""
            cls = d.get("close")
            if dt and cls is not None:
                try:
                    bars.append({"date": str(dt)[:10], "close": float(cls)})
                except (TypeError, ValueError):
                    pass

        bars.sort(key=lambda r: r["date"])
        if bars:
            cache.set(cache_key, bars, _HIST_TTL)
        return bars

    except Exception as e:
        print(f"[THEME_RS][Tradier hist] {sym}: {e}")
        return []


async def _fetch_proxy_history(symbol: str) -> tuple[list[dict], str]:
    """
    FMP (primary) → Tradier daily (fallback) → yfinance (emergency).
    Returns (bars, source_name).
    """
    bars = await _fetch_fmp_daily_history(symbol)
    if bars:
        return bars, "fmp"

    bars = await _fetch_tradier_daily_history(symbol)
    if bars:
        return bars, "tradier_hist"

    bars = await fetch_etf_history(symbol, days=400)
    if bars:
        return bars, "yfinance"

    return [], "unavailable"


# ── Quote fetchers ─────────────────────────────────────────────────────────────

async def _fetch_all_quotes(symbols: list[str]) -> dict[str, dict]:
    """Tradier batch → Finnhub per-symbol fallback."""
    result: dict[str, dict] = {}

    tradier_data = await _tradier_quotes_batch(symbols)
    if tradier_data:
        result.update(tradier_data)

    missing = [t for t in symbols if t not in result or not result[t].get("price")]
    if missing:
        async with httpx.AsyncClient(timeout=10.0) as session:
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


# ── Dynamic leader/laggard universe ───────────────────────────────────────────

def _read_xc_tickers() -> list[str]:
    """Read X/Grok consensus tickers from disk snapshot — no new API calls."""
    try:
        if not _XC_PATH.exists():
            return []
        raw = json.loads(_XC_PATH.read_text())
        saved_at = float(raw.get("_saved_at", 0))
        if saved_at and (time.time() - saved_at) > 8 * 86400:
            return []   # stale — older than 8 days
        top = raw.get("top_tickers") or []
        tickers = []
        for entry in top:
            if isinstance(entry, str):
                tickers.append(entry.upper())
            elif isinstance(entry, dict):
                sym = (entry.get("ticker") or entry.get("symbol") or "").upper()
                if sym:
                    tickers.append(sym)
        return tickers[:30]
    except Exception as e:
        print(f"[THEME_RS] XC read error: {e}")
        return []


async def _etf_holdings_for_proxy(etf_symbol: str) -> list[str]:
    """
    Get top N tickers from ETF holdings service.
    Returns clean list of stock tickers (no ETF self, no cash).
    """
    try:
        from services.sector_rotation.etf_holdings_service import get_etf_holdings
        data = await asyncio.wait_for(get_etf_holdings(etf_symbol), timeout=10.0)
        holdings = data.get("holdings") or []
        out = []
        for h in holdings[:_ETF_HOLDINGS_TOP_N]:
            sym = (h.get("ticker") or "").upper()
            if sym and sym != etf_symbol and len(sym) <= 6 and sym not in ("CASH", "USD"):
                out.append(sym)
        return out
    except Exception as e:
        print(f"[THEME_RS] ETF holdings error {etf_symbol}: {e}")
        return []


async def _build_leader_universe(
    theme_id: str,
    meta: dict,
    proxy_symbols_used: list[str],
) -> tuple[list[str], dict[str, list[str]], str]:
    """
    Build the leader/laggard universe for a theme dynamically.

    Priority:
      1. ETF holdings from the PRIMARY proxy ETF (used first)
      2. ETF holdings from secondary proxy ETFs (if primary < 3 results)
      3. X/Grok consensus tickers matched to theme keywords (disk read-only)
      4. Static candidate_symbols as last-resort fallback seeds

    Returns:
      (universe_tickers, discovery_sources_by_ticker, universe_source_label)
    """
    sources_by_ticker: dict[str, list[str]] = {}
    universe: list[str] = []
    label_parts: list[str] = []

    # ── 1. ETF holdings from proxy ETFs ────────────────────────────────────────
    proxies_to_try = proxy_symbols_used[:3] if proxy_symbols_used else meta["proxy_symbols"][:3]

    for etf in proxies_to_try:
        holdings = await _etf_holdings_for_proxy(etf)
        if holdings:
            label_parts.append(f"etf_holding:{etf}")
            for sym in holdings:
                sources_by_ticker.setdefault(sym, [])
                src = f"etf_holding:{etf}"
                if src not in sources_by_ticker[sym]:
                    sources_by_ticker[sym].append(src)
                if sym not in universe:
                    universe.append(sym)
        if len(universe) >= _ETF_HOLDINGS_TOP_N:
            break

    # ── 2. X/Grok consensus filtered by theme keywords (disk, no API call) ─────
    if len(universe) < 5:
        xc_tickers  = _read_xc_tickers()
        keywords    = set(k.lower() for k in meta.get("keywords", []))
        sector_tags = set(s.lower() for s in meta.get("sector_tags", []))
        candidate_upper = set(s.upper() for s in meta.get("candidate_symbols", []))

        for sym in xc_tickers:
            if sym in candidate_upper or sym in (s.upper() for s in universe[:20]):
                sources_by_ticker.setdefault(sym, [])
                if "x_consensus" not in sources_by_ticker[sym]:
                    sources_by_ticker[sym].append("x_consensus")
                if sym not in universe:
                    universe.append(sym)
                    if not label_parts or "x_consensus" not in " ".join(label_parts):
                        label_parts.append("x_consensus")
            if len(universe) >= 20:
                break

    # ── 3. Static candidate_symbols as fallback seeds ──────────────────────────
    static_used = False
    for sym in meta.get("candidate_symbols", []):
        s = sym.upper()
        sources_by_ticker.setdefault(s, [])
        if "static_fallback" not in sources_by_ticker[s]:
            sources_by_ticker[s].append("static_fallback")
        if s not in universe:
            universe.append(s)
            static_used = True

    if static_used and not label_parts:
        label_parts.append("static_fallback")
    elif static_used:
        label_parts.append("static_fallback_seeds")

    source_label = " + ".join(label_parts) if label_parts else "static_fallback"
    return universe, sources_by_ticker, source_label


# ── Performance helpers ────────────────────────────────────────────────────────

def _perf_for_timeframe(
    hist: list[dict],
    tf: str,
    quote: dict,
) -> Optional[float]:
    if tf == "1D":
        v = (
            quote.get("change_percentage") or
            quote.get("change_pct") or
            quote.get("change_1d_pct") or
            quote.get("changesPercentage")
        )
        if v is not None:
            return round(float(v), 2)
        if len(hist) >= 2:
            try:
                prev = float(hist[-2]["close"])
                last = float(hist[-1]["close"])
                return round((last - prev) / prev * 100, 2) if prev else None
            except Exception:
                return None
        return None
    elif tf == "YTD":
        return _ytd_change(hist)
    else:
        return _pct_change(hist, _TIMEFRAME_BARS[tf])


def _compute_theme_perf(
    proxy_symbols: list[str],
    tf: str,
    quotes: dict[str, dict],
    histories: dict[str, tuple[list[dict], str]],
) -> tuple[Optional[float], list[str], dict[str, str]]:
    """
    Compute theme return for a timeframe.
    Returns (median_return_pct, used_proxies, source_health).
    """
    vals: list[float]        = []
    used: list[str]          = []
    health: dict[str, str]   = {}

    for sym in proxy_symbols:
        bars, src = histories.get(sym, ([], "unavailable"))
        q = quotes.get(sym, {})
        p = _perf_for_timeframe(bars, tf, q)
        health[sym] = src if bars else "unavailable"
        if p is not None:
            vals.append(p)
            used.append(sym)

    if not vals:
        return None, [], health

    return round(statistics.median(vals), 2), used, health


def _pct_rank(value: Optional[float], universe: list[Optional[float]]) -> float:
    valid = [v for v in universe if v is not None]
    if value is None or not valid:
        return 0.5
    below = sum(1 for v in valid if v < value)
    return below / len(valid)


def _assign_state(rs: float, accel: Optional[float]) -> str:
    if rs >= 70:
        return "active"
    if rs < 30:
        return "dead_zone"
    if 55 <= rs < 70 and (accel is None or accel >= 0):
        return "emerging"
    if 25 <= rs < 45 and accel is not None and accel < 0:
        return "weakening"
    return "neutral"


def _state_reason(
    state: str,
    rs_score: float,
    rs_vs_spy: Optional[float],
    rs_vs_qqq: Optional[float],
    breadth: Optional[float],
    tf_ret: Optional[float],
) -> str:
    """Deterministic human-readable explanation for every state label."""
    spy_str = f"{rs_vs_spy:+.1f}%" if rs_vs_spy is not None else "N/A"
    qqq_str = f"{rs_vs_qqq:+.1f}%" if rs_vs_qqq is not None else "N/A"
    brd_str = f"{breadth:.0f}%" if breadth is not None else "N/A"

    if state == "active":
        if breadth is not None and breadth >= 60:
            return (
                f"Top-percentile RS ({rs_score:.0f}/100) vs theme universe "
                f"with broad participation ({brd_str} advancing). "
                f"Outperforming SPY {spy_str}, QQQ {qqq_str}."
            )
        return (
            f"Top-percentile RS ({rs_score:.0f}/100) vs theme universe. "
            f"Outperforming SPY {spy_str}, QQQ {qqq_str}. "
            f"Breadth {brd_str}."
        )

    if state == "emerging":
        return (
            f"Rising RS ({rs_score:.0f}/100) with positive momentum acceleration. "
            f"Positive return but not yet top-tier vs theme universe. "
            f"SPY delta {spy_str}, QQQ delta {qqq_str}."
        )

    if state == "weakening":
        if rs_vs_spy is not None and rs_vs_spy < 0:
            return (
                f"Deteriorating RS ({rs_score:.0f}/100) with negative momentum. "
                f"Lagging SPY by {abs(rs_vs_spy):.1f}% and QQQ by {abs(rs_vs_qqq or 0):.1f}%. "
                f"Breadth {brd_str} — broad weakness."
            )
        return (
            f"RS score {rs_score:.0f}/100 with decelerating momentum. "
            f"Below-average performance vs peer themes. Breadth {brd_str}."
        )

    if state == "dead_zone":
        return (
            f"Bottom-quartile RS ({rs_score:.0f}/100) vs 39-theme universe. "
            f"SPY delta {spy_str}, QQQ delta {qqq_str}. "
            f"Breadth {brd_str} — minimal buying pressure."
        )

    # neutral
    if tf_ret is not None and tf_ret > 0:
        return (
            f"Positive return but mid-table RS ({rs_score:.0f}/100) — "
            f"lagging stronger themes. SPY delta {spy_str}, QQQ delta {qqq_str}."
        )
    return (
        f"Mid-range RS ({rs_score:.0f}/100) vs peer themes. "
        f"SPY delta {spy_str}, QQQ delta {qqq_str}. Breadth {brd_str}."
    )


# ── Theme row builder ──────────────────────────────────────────────────────────

async def _build_theme_row(
    theme_id: str,
    meta: dict,
    quotes: dict[str, dict],
    histories: dict[str, tuple[list[dict], str]],
    tf: str,
    stock_perfs: dict[str, Optional[float]],   # sym → tf-return (may be None)
    stock_sources: dict[str, str],             # sym → discovery_source
) -> Optional[dict]:
    """
    Build one theme row for the given timeframe.
    stock_perfs: pre-computed per-stock returns for the requested timeframe.
    """
    proxy_syms = meta["proxy_symbols"]

    # ── Resolve DRAM special handling for memory_storage ──────────────────────
    if theme_id == "memory_storage":
        dram_bars, dram_src = histories.get("DRAM", ([], "unavailable"))
        if dram_bars:
            # DRAM available — use as primary, keep SMH/SOXX as backup
            if "DRAM" not in proxy_syms:
                proxy_syms = ["DRAM"] + [s for s in proxy_syms if s != "DRAM"]
        else:
            # DRAM unavailable — use SMH/SOXX only
            proxy_syms = [s for s in proxy_syms if s != "DRAM"] or proxy_syms

    # ── Theme performance (primary timeframe + all others) ─────────────────────
    tf_ret, used_proxies, source_health = _compute_theme_perf(
        proxy_syms, tf, quotes, histories
    )
    # All-timeframe performance for RS scoring
    all_perf: dict[str, Optional[float]] = {}
    for frame in _TIMEFRAME_BARS:
        if frame == tf:
            all_perf[frame] = tf_ret
        else:
            ret, _, _ = _compute_theme_perf(proxy_syms, frame, quotes, histories)
            all_perf[frame] = ret

    if tf_ret is None and not any(v is not None for v in all_perf.values()):
        return None

    # ── Representative price ───────────────────────────────────────────────────
    lead_sym   = None
    lead_price = None
    best_count = -1
    for sym in (used_proxies or proxy_syms):
        bars, _ = histories.get(sym, ([], ""))
        if len(bars) > best_count:
            best_count = len(bars)
            lead_sym   = sym
            q          = quotes.get(sym, {})
            lead_price = (
                q.get("price") or q.get("last") or q.get("close") or
                (float(bars[-1]["close"]) if bars else None)
            )

    # ── 50d SMA distance + trend acceleration ─────────────────────────────────
    pct50s: list[float] = []
    accels: list[float] = []
    for sym in (used_proxies or proxy_syms):
        bars, _ = histories.get(sym, ([], ""))
        q       = quotes.get(sym, {})
        price   = q.get("price") or q.get("last") or (float(bars[-1]["close"]) if bars else None)
        if price and bars:
            ma50 = _sma(bars, 50)
            if ma50:
                pct50s.append((price - ma50) / ma50 * 100)
        if len(bars) >= 21:
            recent = _pct_change(bars, 10)
            prior  = _pct_change(bars[-11:], 10)
            if recent is not None and prior is not None:
                accels.append(recent - prior)

    pct_from_50d = round(statistics.median(pct50s), 2) if pct50s else None
    trend_accel  = round(statistics.median(accels), 2)  if accels else None

    # ── Dynamic leader/laggard universe ───────────────────────────────────────
    universe, disc_sources, universe_src_label = await _build_leader_universe(
        theme_id, meta, used_proxies
    )

    # ── Leaders/laggards ranked by selected timeframe return ───────────────────
    sym_perfs: list[tuple[str, float, str]] = []   # (sym, return_pct, disc_src)
    for sym in universe:
        ret = stock_perfs.get(sym)
        if ret is not None:
            srcs = disc_sources.get(sym, ["unknown"])
            sym_perfs.append((sym, ret, srcs[0] if srcs else "unknown"))

    sym_perfs.sort(key=lambda x: x[1], reverse=True)

    def _make_entry(sym: str, ret: float, src: str) -> dict:
        return {
            "symbol":            sym,
            "return_pct":        ret,
            "timeframe":         tf,
            "source":            stock_sources.get(sym, "tradier_batch"),
            "discovery_sources": disc_sources.get(sym, []),
        }

    leaders  = [_make_entry(s, r, ds) for s, r, ds in sym_perfs[:3]]
    laggards = [_make_entry(s, r, ds) for s, r, ds in sym_perfs[-3:][::-1]]

    # ── Breadth (% advancing in the requested timeframe) ──────────────────────
    breadth: Optional[float] = None
    if sym_perfs:
        up = sum(1 for _, r, _ in sym_perfs if r > 0)
        breadth = round(up / len(sym_perfs) * 100, 1)

    return {
        "theme_id":              theme_id,
        "display_name":          meta["display_name"],
        "proxy_type":            meta["proxy_type"],
        "proxy_symbols":         proxy_syms,
        "proxy_symbols_used":    used_proxies,
        "proxy_source_health":   source_health,
        "price":                 round(lead_price, 2) if lead_price else None,
        "lead_proxy":            lead_sym,
        "timeframe":             tf,
        "return_pct":            tf_ret,
        "performance": {
            "1D":  all_perf["1D"],
            "7D":  all_perf["7D"],
            "30D": all_perf["30D"],
            "YTD": all_perf["YTD"],
            "1Y":  all_perf["1Y"],
        },
        "breadth_pct":           breadth,
        "pct_from_50d":          pct_from_50d,
        "trend_accel_20d":       trend_accel,
        "leader_universe_source": universe_src_label,
        "leaders":               leaders,
        "laggards":              laggards,
        "last_updated":          datetime.now(timezone.utc).isoformat(),
        # Filled in by _score_and_state:
        "rs_score":              None,
        "rs_vs_spy":             None,
        "rs_vs_qqq":             None,
        "state":                 None,
        "state_reason":          None,
        "momentum_rank":         None,
    }


# ── RS scoring ─────────────────────────────────────────────────────────────────

def _score_and_state(
    rows: list[dict],
    tf: str,
    histories: dict[str, tuple[list[dict], str]],
    quotes: dict[str, dict],
) -> list[dict]:
    """Compute RS score (0-100), RS vs SPY/QQQ, state, state_reason for each row."""

    def _bench(sym: str) -> Optional[float]:
        bars, _ = histories.get(sym, ([], ""))
        q       = quotes.get(sym, {})
        return _perf_for_timeframe(bars, tf, q)

    spy_ret = _bench("SPY")
    qqq_ret = _bench("QQQ")

    perf_30d_all = [r["performance"].get("30D") for r in rows]
    perf_7d_all  = [r["performance"].get("7D")  for r in rows]
    perf_1y_all  = [r["performance"].get("1Y")  for r in rows]

    for row in rows:
        p = row["performance"]
        p30 = _pct_rank(p.get("30D"), perf_30d_all)
        p7  = _pct_rank(p.get("7D"),  perf_7d_all)
        p1y = _pct_rank(p.get("1Y"),  perf_1y_all)

        ma50_norm = 0.5
        pct50 = row.get("pct_from_50d")
        if pct50 is not None:
            capped    = max(-20.0, min(20.0, pct50))
            ma50_norm = (capped + 20.0) / 40.0

        accel_norm = 0.5
        accel = row.get("trend_accel_20d")
        if accel is not None:
            capped     = max(-5.0, min(5.0, accel))
            accel_norm = (capped + 5.0) / 10.0

        rs = round(
            p30 * 35.0 +
            p7  * 25.0 +
            p1y * 20.0 +
            ma50_norm  * 10.0 +
            accel_norm * 10.0,
            1,
        )
        row["rs_score"] = rs

        tf_ret = p.get(tf)
        row["rs_vs_spy"] = (
            round(tf_ret - spy_ret, 2)
            if tf_ret is not None and spy_ret is not None else None
        )
        row["rs_vs_qqq"] = (
            round(tf_ret - qqq_ret, 2)
            if tf_ret is not None and qqq_ret is not None else None
        )

        state = _assign_state(rs, accel)
        row["state"] = state
        row["state_reason"] = _state_reason(
            state, rs,
            row["rs_vs_spy"], row["rs_vs_qqq"],
            row.get("breadth_pct"), tf_ret,
        )

    sorted_rows = sorted(rows, key=lambda r: r.get("rs_score") or 0, reverse=True)
    for rank, row in enumerate(sorted_rows, start=1):
        row["momentum_rank"] = rank

    return rows


# ── Main compute pass ──────────────────────────────────────────────────────────

async def _compute(tf: str) -> list[dict]:
    """Full compute pass → quoted + history → scored theme rows."""
    print(f"[THEME_RS] Computing fresh data (tf={tf}) …")

    # ── 1. Collect all symbols needed ─────────────────────────────────────────
    # DRAM is now in ALL_PROXY_SYMBOLS (memory_storage primary proxy)
    all_proxy_with_bench = sorted(set(ALL_PROXY_SYMBOLS + _BENCHMARKS))
    proxy_syms_with_dram = all_proxy_with_bench   # alias for clarity below
    quote_syms = sorted(set(ALL_PROXY_SYMBOLS + ALL_CANDIDATE_SYMBOLS + _BENCHMARKS))

    # ── 2. Fetch quotes (Tradier batch → Finnhub fallback) ────────────────────
    print(f"[THEME_RS] Fetching quotes for {len(quote_syms)} symbols …")
    quotes = await _fetch_all_quotes(quote_syms)

    # ── 3. Fetch proxy + benchmark history (FMP primary → Tradier → yfinance) ─
    print(f"[THEME_RS] Fetching proxy history for {len(proxy_syms_with_dram)} symbols …")
    hist_tasks = [_fetch_proxy_history(s) for s in proxy_syms_with_dram]
    hist_results = await asyncio.gather(*hist_tasks, return_exceptions=True)
    histories: dict[str, tuple[list[dict], str]] = {}
    for sym, result in zip(proxy_syms_with_dram, hist_results):
        if isinstance(result, tuple):
            histories[sym] = result
        else:
            histories[sym] = ([], "unavailable")

    # ── 4. Discover all dynamic universe stocks across all themes ─────────────
    # We need ETF holdings per theme's primary proxy — gather now to dedup
    print("[THEME_RS] Discovering dynamic leader universes …")
    primary_proxies = []
    for meta in THEME_RS_UNIVERSE.values():
        proxies = meta["proxy_symbols"]
        if proxies:
            primary_proxies.append(proxies[0])
    # Also DRAM for memory_storage
    primary_proxies.append("DRAM")
    primary_proxies = list(dict.fromkeys(primary_proxies))  # dedup, preserve order

    holdings_tasks  = [_etf_holdings_for_proxy(p) for p in primary_proxies]
    holdings_results = await asyncio.gather(*holdings_tasks, return_exceptions=True)
    all_dynamic_stocks: set[str] = set()
    for res in holdings_results:
        if isinstance(res, list):
            all_dynamic_stocks.update(res)
    # Also include static candidate_symbols
    all_dynamic_stocks.update(ALL_CANDIDATE_SYMBOLS)
    all_dynamic_stocks -= set(proxy_syms_with_dram)   # already have proxy history
    all_dynamic_stocks_list = sorted(all_dynamic_stocks)

    # ── 5. Fetch history for all dynamic universe stocks ──────────────────────
    # For 1D: Tradier batch quotes sufficient — skip heavy history.
    # For 7D+: use yfinance thread-pool for individual stocks (fast, parallel).
    #   FMP primary is reserved for proxy ETFs (critical for theme performance).
    if tf == "1D":
        print(f"[THEME_RS] 1D: skipping per-stock history (Tradier quotes only)")
        for sym in all_dynamic_stocks_list:
            histories.setdefault(sym, ([], "unavailable"))
    else:
        print(f"[THEME_RS] {tf}: yfinance history for {len(all_dynamic_stocks_list)} dynamic stocks …")
        yf_tasks = [fetch_etf_history(s, days=400) for s in all_dynamic_stocks_list]
        yf_results = await asyncio.gather(*yf_tasks, return_exceptions=True)
        for sym, result in zip(all_dynamic_stocks_list, yf_results):
            if isinstance(result, list) and result:
                histories[sym] = (result, "yfinance")
            else:
                histories[sym] = ([], "unavailable")

    # Fetch quotes for dynamic stocks not yet covered
    missing_quotes = [s for s in all_dynamic_stocks_list if s not in quotes]
    if missing_quotes:
        extra_quotes = await _fetch_all_quotes(missing_quotes)
        quotes.update(extra_quotes)

    # ── 6. Pre-compute per-stock tf returns for leader/laggard ranking ────────
    # Cap at ±500% to filter yfinance adjusted-price anomalies from spin-offs etc.
    _STOCK_RET_CAP = 500.0
    stock_perfs: dict[str, Optional[float]] = {}
    stock_src_map: dict[str, str] = {}
    for sym in all_dynamic_stocks_list:
        bars, src = histories.get(sym, ([], "unavailable"))
        q   = quotes.get(sym, {})
        ret = _perf_for_timeframe(bars, tf, q)
        if ret is not None and abs(ret) > _STOCK_RET_CAP:
            ret = None   # discard obviously corrupt adjusted-price data
        stock_perfs[sym]   = ret
        stock_src_map[sym] = "tradier_batch" if tf == "1D" else src

    # ── 7. Build each theme row ────────────────────────────────────────────────
    rows: list[dict] = []
    for theme_id, meta in THEME_RS_UNIVERSE.items():
        try:
            row = await _build_theme_row(
                theme_id, meta, quotes, histories, tf, stock_perfs, stock_src_map
            )
            if row:
                rows.append(row)
            else:
                print(f"[THEME_RS] No data for '{theme_id}' — skipped")
        except Exception as e:
            print(f"[THEME_RS] Row error '{theme_id}': {e}")

    # ── 8. Score and sort ──────────────────────────────────────────────────────
    rows = _score_and_state(rows, tf, histories, quotes)
    rows.sort(key=lambda r: (r.get("rs_score") or 0, r["performance"].get(tf) or 0), reverse=True)

    print(f"[THEME_RS] Done: {len(rows)} themes")
    return rows


# ── Public entry point ─────────────────────────────────────────────────────────

async def get_theme_rs_data(
    timeframe: str = "30D",
    force: bool = False,
) -> dict:
    """
    Returns full themes-RS payload dict (cached, LKG disk fallback).
    """
    tf = timeframe.upper()
    if tf not in _TIMEFRAME_BARS:
        tf = "30D"

    cache_key = f"{_CACHE_KEY}:{tf}"
    ttl = _market_hours_ttl()

    if not force:
        hit = cache.get(cache_key)
        if hit is not None:
            return hit

    # Track which providers were actually used
    fmp_used      = False
    tradier_used  = False
    yf_used       = False

    try:
        rows = await _compute(tf)

        # Determine provider health from rows
        for row in rows:
            ph = row.get("proxy_source_health", {})
            for src in ph.values():
                if src == "fmp":
                    fmp_used = True
                elif src == "tradier_hist":
                    tradier_used = True
                elif src == "yfinance":
                    yf_used = True

        ts = datetime.now(timezone.utc).isoformat()
        payload = {
            "themes":            rows,
            "timeframe":         tf,
            "theme_count":       len(rows),
            "generated_at":      ts,
            "cache_ttl_s":       ttl,
            "is_market_hours":   _is_market_hours(),
            "source":            "live" if rows else "lkg",
            "source_health": {
                "tradier_quotes": any(
                    r["performance"].get("1D") is not None for r in rows
                ),
                "fmp_history":    fmp_used,
                "tradier_history": tradier_used,
                "yfinance":       yf_used,
            },
        }

        if rows:
            cache.set(cache_key, payload, ttl)
            _save_lkg(rows)

        return payload

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"[THEME_RS] Compute error ({e}), falling back to LKG")
        lkg = _load_lkg()
        if lkg:
            return {
                "themes":          lkg,
                "timeframe":       tf,
                "theme_count":     len(lkg),
                "generated_at":    None,
                "cache_ttl_s":     None,
                "is_market_hours": _is_market_hours(),
                "source":          "lkg",
                "source_health":   {
                    "tradier_quotes":  False,
                    "fmp_history":     False,
                    "tradier_history": False,
                    "yfinance":        False,
                },
            }
        raise
