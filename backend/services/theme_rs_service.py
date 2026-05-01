"""
Themes by Relative Strength — canonical performance + RS service.

Exposes:
    get_theme_rs_data(timeframe, force) → list[dict]

Data pipeline (no LLM calls):
1. Fetch live quotes for all proxy + candidate symbols via Tradier batch
2. Fetch ~400-bar daily history for proxy symbols + SPY + QQQ via yfinance
3. For each theme: compute 1D/7D/30D/YTD/1Y returns, RS vs SPY & QQQ,
   breadth (% candidates up on the day), per-theme leaders & laggards
4. Score RS 0-100 via cross-theme percentile rank; assign state label
5. Cache result at 15-min (market hours) / 60-min (off-hours) TTL
6. Persist LKG to disk so page loads work after restart

Cache:
    key  : themes:relative_strength:v1
    TTL  : _market_hours_ttl()
    disk : backend/data/themes_rs_lkg.json  (atomic write)

State labels (maps from RS score + acceleration):
    active    : RS ≥ 70
    emerging  : RS 55-69  AND accel > 0
    neutral   : RS 40-65  (residual)
    weakening : RS 25-45  AND accel < 0
    dead_zone : RS < 30
"""
from __future__ import annotations

import asyncio
import json
import statistics
import time
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Optional

from data.cache import cache
from services.theme_rs_universe import (
    THEME_RS_UNIVERSE,
    ALL_PROXY_SYMBOLS,
    ALL_CANDIDATE_SYMBOLS,
)
from services.sector_rotation.analytics import _pct_change, _ytd_change, _sma
from services.sector_rotation.providers import (
    fetch_etf_history,
    _tradier_quotes_batch,
    _finnhub_quote_single,
)

import httpx


_CACHE_KEY = "themes:relative_strength:v1"
_LKG_PATH  = Path(__file__).parent.parent / "data" / "themes_rs_lkg.json"
_HIST_TTL  = 3600    # 1 h — daily bars don't change intraday

_BENCHMARKS = ["SPY", "QQQ"]

_TIMEFRAME_BARS: dict[str, int] = {
    "1D":  1,
    "7D":  5,
    "30D": 22,
    "YTD": 0,   # special: use _ytd_change
    "1Y":  252,
}


def _market_hours_ttl() -> int:
    now = datetime.now(tz=timezone.utc)
    if now.weekday() >= 5:
        return 3600
    month = now.month
    utc_offset = 4 if 4 <= month <= 10 else 5
    et_hour    = (now.hour - utc_offset) % 24
    et_minutes = et_hour * 60 + now.minute
    return 900 if 570 <= et_minutes <= 960 else 3600


def _is_market_hours() -> bool:
    return _market_hours_ttl() == 900


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
    try:
        _LKG_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = _LKG_PATH.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(data, indent=2, default=str))
        tmp.replace(_LKG_PATH)
    except Exception as e:
        print(f"[THEME_RS] LKG save error: {e}")


async def _fetch_all_quotes(symbols: list[str]) -> dict[str, dict]:
    """Tradier batch + Finnhub fallback for individual misses."""
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


async def _fetch_histories(symbols: list[str]) -> dict[str, list[dict]]:
    """Async yfinance history for each symbol."""
    results = await asyncio.gather(
        *[fetch_etf_history(t, days=400) for t in symbols],
        return_exceptions=True,
    )
    out: dict[str, list[dict]] = {}
    for t, r in zip(symbols, results):
        if isinstance(r, list):
            out[t] = r
        else:
            print(f"[THEME_RS] History error {t}: {r}")
            out[t] = []
    return out


def _perf_for_timeframe(hist: list[dict], tf: str, quote: dict) -> Optional[float]:
    """Return pct change for the given timeframe string."""
    if tf == "1D":
        # Prefer live quote change_pct
        v = quote.get("change_percentage") or quote.get("change_pct") or quote.get("change_1d_pct")
        if v is not None:
            return round(float(v), 2)
        # Fallback to last two bars
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
        n = _TIMEFRAME_BARS[tf]
        return _pct_change(hist, n)


def _median_perf(
    theme_id: str,
    proxy_symbols: list[str],
    tf: str,
    quotes: dict[str, dict],
    histories: dict[str, list[dict]],
) -> Optional[float]:
    vals: list[float] = []
    for sym in proxy_symbols:
        q    = quotes.get(sym, {})
        hist = histories.get(sym, [])
        p    = _perf_for_timeframe(hist, tf, q)
        if p is not None:
            vals.append(p)
    if not vals:
        return None
    return round(statistics.median(vals), 2)


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
    if 25 <= rs < 45 and (accel is not None and accel < 0):
        return "weakening"
    return "neutral"


def _build_theme_row(
    theme_id: str,
    meta: dict,
    quotes: dict[str, dict],
    histories: dict[str, list[dict]],
    candidate_quotes: dict[str, dict],
) -> Optional[dict]:
    proxy_symbols = meta["proxy_symbols"]

    # ── Performance across all timeframes ─────────────────────────────────────
    perf: dict[str, Optional[float]] = {}
    for tf in _TIMEFRAME_BARS:
        perf[tf] = _median_perf(theme_id, proxy_symbols, tf, quotes, histories)

    # Representative price (lead proxy ETF — most history bars)
    leader_sym   = None
    leader_price = None
    best_count   = -1
    for sym in proxy_symbols:
        h = histories.get(sym, [])
        if len(h) > best_count:
            best_count   = len(h)
            leader_sym   = sym
            q            = quotes.get(sym, {})
            leader_price = (
                q.get("price") or q.get("last") or q.get("close") or
                (float(h[-1]["close"]) if h else None)
            )

    # ── 50D SMA distance (for RS scoring) ─────────────────────────────────────
    pct50s: list[float] = []
    accels: list[float] = []
    for sym in proxy_symbols:
        hist  = histories.get(sym, [])
        q     = quotes.get(sym, {})
        price = q.get("price") or q.get("last") or (float(hist[-1]["close"]) if hist else None)
        if price:
            ma50 = _sma(hist, 50)
            if ma50:
                pct50s.append((price - ma50) / ma50 * 100)
        if len(hist) >= 21:
            recent = _pct_change(hist, 10)
            prior  = _pct_change(hist[-11:], 10)
            if recent is not None and prior is not None:
                accels.append(recent - prior)

    pct_from_50d = round(statistics.median(pct50s), 2) if pct50s else None
    trend_accel  = round(statistics.median(accels), 2)  if accels else None

    # ── Per-theme leaders & laggards from candidate symbols ───────────────────
    cand_perfs: list[tuple[str, float]] = []
    for sym in meta.get("candidate_symbols", []):
        cq = candidate_quotes.get(sym, {})
        c1d = cq.get("change_percentage") or cq.get("change_pct") or cq.get("change_1d_pct")
        if c1d is not None:
            try:
                cand_perfs.append((sym, round(float(c1d), 2)))
            except Exception:
                pass

    cand_perfs.sort(key=lambda x: x[1], reverse=True)

    leaders  = [{"symbol": s, "change_1d_pct": p} for s, p in cand_perfs[:3]]
    laggards = [{"symbol": s, "change_1d_pct": p} for s, p in cand_perfs[-3:]]

    # ── Breadth: % candidates with positive 1D change ─────────────────────────
    breadth: Optional[float] = None
    if cand_perfs:
        up = sum(1 for _, p in cand_perfs if p > 0)
        breadth = round(up / len(cand_perfs) * 100, 1)

    if not any(v is not None for v in perf.values()) and leader_price is None:
        return None

    return {
        "theme_id":           theme_id,
        "display_name":       meta["display_name"],
        "proxy_type":         meta["proxy_type"],
        "proxy_symbols":      proxy_symbols,
        "candidate_symbols":  meta.get("candidate_symbols", []),
        "sector_tags":        meta.get("sector_tags", []),
        "keywords":           meta.get("keywords", []),
        "macro_sensitivities": meta.get("macro_sensitivities", []),
        "price":              round(leader_price, 2) if leader_price else None,
        "lead_proxy":         leader_sym,
        "performance": {
            "1D":  perf["1D"],
            "7D":  perf["7D"],
            "30D": perf["30D"],
            "YTD": perf["YTD"],
            "1Y":  perf["1Y"],
        },
        "breadth_pct":     breadth,
        "pct_from_50d":    pct_from_50d,
        "trend_accel_20d": trend_accel,
        "leaders":         leaders,
        "laggards":        laggards,
        # These are filled in by _score_and_state():
        "rs_score":     None,
        "rs_vs_spy":    None,
        "rs_vs_qqq":    None,
        "state":        None,
        "momentum_rank": None,
    }


def _score_and_state(
    rows: list[dict],
    tf: str,
    histories: dict[str, list[dict]],
    quotes: dict[str, dict],
) -> list[dict]:
    """Compute RS score (0-100), RS vs SPY/QQQ, state for each row."""

    # ── Benchmark returns ──────────────────────────────────────────────────────
    def _bench(sym: str) -> Optional[float]:
        return _median_perf("bench", [sym], tf, quotes, histories)

    spy_ret = _bench("SPY")
    qqq_ret = _bench("QQQ")

    # ── Cross-theme percentile universe ───────────────────────────────────────
    perf_30d_all = [r["performance"].get("30D") for r in rows]
    perf_7d_all  = [r["performance"].get("7D")  for r in rows]
    perf_1y_all  = [r["performance"].get("1Y")  for r in rows]

    for row in rows:
        p = row["performance"]
        p30 = _pct_rank(p.get("30D"), perf_30d_all)
        p7  = _pct_rank(p.get("7D"),  perf_7d_all)
        p1y = _pct_rank(p.get("1Y"),  perf_1y_all)

        ma50_norm  = 0.5
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
            p30 * 35.0 +
            p7  * 25.0 +
            p1y * 20.0 +
            ma50_norm  * 10.0 +
            accel_norm * 10.0,
            1,
        )
        row["rs_score"] = rs

        # RS vs benchmarks (excess return for the requested timeframe)
        tf_ret = p.get(tf)
        row["rs_vs_spy"] = (
            round(tf_ret - spy_ret, 2) if tf_ret is not None and spy_ret is not None else None
        )
        row["rs_vs_qqq"] = (
            round(tf_ret - qqq_ret, 2) if tf_ret is not None and qqq_ret is not None else None
        )

        # State label
        row["state"] = _assign_state(rs, accel)

    # ── Momentum rank ─────────────────────────────────────────────────────────
    sorted_rows = sorted(rows, key=lambda r: r.get("rs_score") or 0, reverse=True)
    for rank, row in enumerate(sorted_rows, start=1):
        row["momentum_rank"] = rank

    return rows


async def _compute(tf: str) -> list[dict]:
    """Full compute pass — quotes + history → scored rows."""
    print(f"[THEME_RS] Computing fresh data (tf={tf}) …")

    hist_symbols  = sorted(set(ALL_PROXY_SYMBOLS + _BENCHMARKS))
    quote_symbols = sorted(set(ALL_PROXY_SYMBOLS + ALL_CANDIDATE_SYMBOLS + _BENCHMARKS))

    quotes, histories = await asyncio.gather(
        _fetch_all_quotes(quote_symbols),
        _fetch_histories(hist_symbols),
    )

    # Also need benchmark quotes for 1D RS calculation
    candidate_quotes = {sym: quotes.get(sym, {}) for sym in ALL_CANDIDATE_SYMBOLS}

    rows: list[dict] = []
    for theme_id, meta in THEME_RS_UNIVERSE.items():
        row = _build_theme_row(theme_id, meta, quotes, histories, candidate_quotes)
        if row:
            rows.append(row)
        else:
            print(f"[THEME_RS] No data for '{theme_id}' — skipped")

    rows = _score_and_state(rows, tf, histories, quotes)

    # Sort by RS score desc (primary) then return_pct for tf (secondary)
    rows.sort(
        key=lambda r: (r.get("rs_score") or 0, r["performance"].get(tf) or 0),
        reverse=True,
    )

    print(f"[THEME_RS] Done: {len(rows)} themes computed")
    return rows


async def get_theme_rs_data(
    timeframe: str = "30D",
    force: bool = False,
) -> dict:
    """
    Public entry point.
    Returns the full themes-RS payload dict:
      {themes: [...], benchmarks: {...}, generated_at: ..., source: ..., ...}
    Cached with LKG disk fallback.
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

    try:
        rows = await _compute(tf)
        ts   = datetime.now(timezone.utc).isoformat()
        payload = {
            "themes":       rows,
            "timeframe":    tf,
            "theme_count":  len(rows),
            "generated_at": ts,
            "cache_ttl_s":  ttl,
            "is_market_hours": _is_market_hours(),
            "source":       "live" if rows else "lkg",
            "source_health": {
                "tradier": any(r["performance"].get("1D") is not None for r in rows),
                "yfinance": any(r["performance"].get("30D") is not None for r in rows),
            },
        }
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
                "themes":       lkg,
                "timeframe":    tf,
                "theme_count":  len(lkg),
                "generated_at": None,
                "cache_ttl_s":  None,
                "is_market_hours": _is_market_hours(),
                "source":       "lkg",
                "source_health": {"tradier": False, "yfinance": False},
            }
        raise
