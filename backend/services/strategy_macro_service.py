"""
Strategy Macro Service — three Strategy page tab payloads:

  build_vix_regime_payload(macro_provider)        → /api/strategy/vix-risk-regime
  build_weekly_price_movements_payload(...)        → /api/strategy/weekly-price-movements
  build_ten_year_spx_payload(macro_provider)       → /api/strategy/ten-year-spx

Design rules (matching existing app architecture):
  - Reuses macro_provider.get_dashboard() cache (key: macro:dashboard:v3, 15-min TTL)
    for all current-snapshot values — NO new real-time provider calls.
  - Uses macro_provider._get_series() (FRED, 4-h TTL) for VIX and 10Y yield history.
  - Uses yfinance ^GSPC in asyncio.to_thread for SPX daily history (cached 4 h in-process).
  - All heavy computation is cached in-process; endpoints return instantly on warm cache.
  - Neon/Postgres NOT used — weekly price scorecard is deterministic and in-memory-cached;
    historical raw prices are never stored, only derived statistics.
"""
from __future__ import annotations

import asyncio
import math
import statistics
from datetime import datetime, timezone, timedelta
from typing import Any

from data.cache import cache
from data.pg_storage import strategy_hist_read, strategy_hist_write

# ── Cache TTLs ─────────────────────────────────────────────────────────────
_SPX_HIST_TTL    = 21600   # 6 h — yfinance ^GSPC history (refreshed every 3 h by precompute loop)
_STRATEGY_HIST_TTL = 21600 # 6 h — FRED VIXCLS / DGS10 strategy history (same loop)
_REGIME_TTL      =   900   # 15 min — VIX regime payload (tracks live VIX from macro:dashboard)
_WEEKLY_TTL      = 21600   # 6 h — weekly scorecard (deterministic, historical)
_TEN_YEAR_TTL    =   900   # 15 min — 10Y vs SPX payload (tracks live yields from macro:dashboard)

# ── Neon stale-fallback warning registry ─────────────────────────────────────
# Set when a provider fetch fails and we fall back to a stale Neon snapshot.
# Cleared when a fresh fetch succeeds.  Checked by payload builders to include
# a freshness_warning field in the data_sources section of the response.
# Keys match cache_key strings e.g. "strategy:spx_hist:1830".
_NEON_STALE_WARN: dict[str, str] = {}


# ══════════════════════════════════════════════════════════════════════════════
# ── Shared helpers ────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

def _s(v: Any) -> float | None:
    """Safe float — returns None on bad/NaN input."""
    if v is None:
        return None
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except (TypeError, ValueError):
        return None


def _r(v: Any, n: int = 2) -> float | None:
    f = _s(v)
    return round(f, n) if f is not None else None


def _extract_current_snapshot(dashboard: dict) -> dict:
    """
    Pull live market values from the macro:dashboard:v3 cache
    (pre-warmed every 12 min by _macro_precompute_loop).

    SPX → dashboard.market_snapshot.sp500  (FMP ^GSPC, correct index level ~7350)
    VIX → dashboard.vix.current            (FMP ^VIX)
    10Y → dashboard.rates_and_yields.us_10y (FMP treasury real-time)
    DXY → dashboard.dollar.dxy             (Yahoo Finance)

    Never reads SPY benchmark_etfs — Tradier SPY price (~741) is a different
    scale from the ^GSPC index (~7350) and must not be used as SPX.
    """
    snap     = dashboard.get("market_snapshot") or {}
    vix_data = dashboard.get("vix") or {}
    rates    = dashboard.get("rates_and_yields") or {}
    dollar   = dashboard.get("dollar") or {}

    return {
        "spx_proxy":        "^GSPC (FMP real-time)",
        "spx_price":        _r(snap.get("sp500")),
        "spx_change_pct":   _r(snap.get("sp500_change_pct")),
        "vix":              _r(vix_data.get("current")),
        "vix_change_pct":   _r(vix_data.get("change_pct")),
        "vix_signal":       vix_data.get("signal"),
        "us_10y":           _r(rates.get("us_10y"), 3),
        "us_2y":            _r(rates.get("us_2y"), 3),
        "spread_2s10s":     _r(rates.get("spread_2s10s"), 3),
        "dxy":              _r(dollar.get("dxy"), 3),
        "dxy_change_pct":   _r(dollar.get("dxy_change_pct"), 3),
        "snapshot_source":  "macro:dashboard:v3 (FMP + Yahoo + FRED, 15-min TTL)",
    }


async def _get_spx_history(days: int) -> list[dict]:
    """
    ^GSPC daily closes — three-tier read path:
      1. In-memory TTL cache (6 h)  — instant, no I/O
      2. Neon strategy_hist_snapshots (max 24 h old)  — survives process restarts
      3. yfinance ^GSPC live fetch  — only on true cold start with no Neon data
    On fetch success, writes to both memory cache and Neon.
    On fetch failure, falls back to any-age Neon snapshot with a stale warning.
    """
    cache_key = f"strategy:spx_hist:{days}"

    # 1. Memory cache
    hit = cache.get(cache_key)
    if hit is not None:
        return hit

    # 2. Neon snapshot (max 24 h)
    neon_hit = await asyncio.to_thread(strategy_hist_read, cache_key, 86400)
    if neon_hit is not None:
        cache.set(cache_key, neon_hit, _SPX_HIST_TTL)
        _NEON_STALE_WARN.pop(cache_key, None)
        return neon_hit

    # 3. Live yfinance fetch
    def _fetch():
        import yfinance as yf
        end   = datetime.now()
        start = end - timedelta(days=days + 60)
        df = yf.Ticker("^GSPC").history(
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
            interval="1d",
            auto_adjust=True,
        )
        if df is None or df.empty:
            return []
        df = df.reset_index()
        result = []
        for _, row in df.iterrows():
            dt = row.get("Date") or row.get("Datetime")
            close = _s(row.get("Close"))
            if dt is None or close is None:
                continue
            date_str = str(dt.date()) if hasattr(dt, "date") else str(dt)[:10]
            result.append({"date": date_str, "close": round(close, 2)})
        return result

    try:
        data = await asyncio.to_thread(_fetch)
        if data:
            cache.set(cache_key, data, _SPX_HIST_TTL)
            _NEON_STALE_WARN.pop(cache_key, None)
            # Write to Neon in background — don't await
            asyncio.create_task(
                asyncio.to_thread(strategy_hist_write, cache_key, data, "yfinance ^GSPC", len(data))
            )
        return data or []
    except Exception as exc:
        print(f"[STRATEGY] SPX history fetch error: {exc}")
        # Fallback: any-age Neon snapshot
        stale = await asyncio.to_thread(strategy_hist_read, cache_key, None)
        if stale:
            cache.set(cache_key, stale, 3600)
            _NEON_STALE_WARN[cache_key] = f"yfinance fetch failed ({exc}); using stale Neon snapshot"
            print(f"[STRATEGY_HIST] stale Neon fallback for {cache_key}: {exc}")
            return stale
        return []


def _fetch_fred_hist(macro_provider, series_id: str, days: int, decimals: int = 2) -> list[dict]:
    """
    Fetch a FRED series — three-tier read path (synchronous; call via asyncio.to_thread):
      1. In-memory TTL cache (6 h)
      2. Neon strategy_hist_snapshots (max 24 h old)
      3. FRED via macro_provider._get_series() — only on true cold start
    On fetch success, writes to both memory cache and Neon.
    On fetch failure, falls back to any-age Neon snapshot with a stale warning.
    """
    import time as _time

    cache_key = f"strategy:hist:{series_id.lower()}:{days}"

    # 1. Memory cache
    hit = cache.get(cache_key)
    if hit is not None:
        return hit

    # 2. Neon snapshot (max 24 h)
    neon_hit = strategy_hist_read(cache_key, 86400)
    if neon_hit is not None:
        cache.set(cache_key, neon_hit, _STRATEGY_HIST_TTL)
        _NEON_STALE_WARN.pop(cache_key, None)
        return neon_hit

    # 3. Live FRED fetch
    last_exc = None
    for attempt in range(3):
        try:
            series = macro_provider._get_series(series_id, days)
            if series is None or (hasattr(series, "empty") and series.empty):
                if attempt < 2:
                    wait = 12 + attempt * 10
                    print(f"[STRATEGY_HIST] {series_id} empty, retry {attempt+1} in {wait}s")
                    _time.sleep(wait)
                    continue
                break
            out = []
            for idx, val in series.items():
                v = _s(float(val))
                if v is not None:
                    out.append({"date": str(idx.date()), "value": round(v, decimals)})
            if out:
                cache.set(cache_key, out, _STRATEGY_HIST_TTL)
                _NEON_STALE_WARN.pop(cache_key, None)
                strategy_hist_write(cache_key, out, f"FRED {series_id}", len(out))
            return out
        except Exception as exc:
            last_exc = exc
            if attempt < 2:
                wait = 12 + attempt * 10
                print(f"[STRATEGY_HIST] {series_id} error ({exc}), retry {attempt+1} in {wait}s")
                _time.sleep(wait)
                continue
            print(f"[STRATEGY_HIST] {series_id} failed after retries: {exc}")

    # Fallback: any-age Neon snapshot
    stale = strategy_hist_read(cache_key, None)
    if stale:
        cache.set(cache_key, stale, 3600)
        warn_msg = f"FRED {series_id} fetch failed ({last_exc}); using stale Neon snapshot"
        _NEON_STALE_WARN[cache_key] = warn_msg
        print(f"[STRATEGY_HIST] stale Neon fallback for {cache_key}: {last_exc}")
        return stale
    return []


def _get_vix_history_sync(macro_provider, days: int) -> list[dict]:
    """FRED VIXCLS — reads strategy:hist:vixcls:{days} first (pre-warmed by precompute loop)."""
    return _fetch_fred_hist(macro_provider, "VIXCLS", days, decimals=2)


def _get_10y_history_sync(macro_provider, days: int) -> list[dict]:
    """FRED DGS10 — reads strategy:hist:dgs10:{days} first (pre-warmed by precompute loop)."""
    return _fetch_fred_hist(macro_provider, "DGS10", days, decimals=3)


async def precompute_strategy_history(macro_provider) -> None:
    """
    Pre-warm strategy historical series caches every 3 hours.
    Writes to both in-memory cache AND Neon strategy_hist_snapshots so that
    the data survives process restarts (cold-start requests read from Neon
    before touching any external provider).

    Populates:
      strategy:spx_hist:1830    → yfinance ^GSPC daily closes
      strategy:hist:vixcls:1830 → FRED VIXCLS daily series
      strategy:hist:dgs10:1830  → FRED DGS10 daily series

    FRED calls are staggered 15 s after the SPX fetch to avoid colliding
    with _macro_precompute_loop which also touches FRED on its 12-min cycle.
    """
    import time as _time

    t0 = _time.time()

    # Force cache bypass so we always fetch fresh data (not the current memory hit)
    for k in ("strategy:spx_hist:1830", "strategy:hist:vixcls:1830", "strategy:hist:dgs10:1830"):
        cache.delete(k)

    # SPX first (yfinance, no rate-limit concerns) — _get_spx_history will write to Neon
    spx = await _get_spx_history(1830)

    # Stagger FRED calls to avoid hitting the rate limit alongside
    # the macro precompute loop (which calls FRED every 12 min)
    await asyncio.sleep(15)

    vix_hist, dgs10_hist = await asyncio.gather(
        asyncio.to_thread(_fetch_fred_hist, macro_provider, "VIXCLS", 1830, 2),
        asyncio.to_thread(_fetch_fred_hist, macro_provider, "DGS10",  1830, 3),
        return_exceptions=True,
    )
    spx_n   = len(spx)       if isinstance(spx,       list) else 0
    vix_n   = len(vix_hist)  if isinstance(vix_hist,  list) else 0
    dgs10_n = len(dgs10_hist) if isinstance(dgs10_hist, list) else 0

    elapsed = _time.time() - t0
    print(f"[STRATEGY_HIST] Precomputed in {elapsed:.1f}s — "
          f"SPX={spx_n} VIXCLS={vix_n} DGS10={dgs10_n} (written to memory + Neon)")


def _daily_pct_changes(series: list[dict], value_key: str = "close") -> list[tuple[str, float]]:
    """[(date, daily_pct_change), ...]"""
    result = []
    for i in range(1, len(series)):
        prev = _s(series[i - 1].get(value_key))
        curr = _s(series[i].get(value_key))
        if prev and curr and prev != 0:
            result.append((series[i]["date"], round((curr - prev) / prev * 100, 4)))
    return result


def _daily_bps_changes(series: list[dict]) -> list[tuple[str, float]]:
    """[(date, bps_change), ...] for yield series."""
    result = []
    for i in range(1, len(series)):
        prev = _s(series[i - 1].get("value"))
        curr = _s(series[i].get("value"))
        if prev is not None and curr is not None:
            result.append((series[i]["date"], round((curr - prev) * 100, 2)))
    return result


def _align_and_split(
    a_changes: list[tuple[str, float]],
    b_changes: list[tuple[str, float]],
) -> tuple[list[float], list[float]]:
    """Inner-join by date and return aligned float lists."""
    a_map = dict(a_changes)
    b_map = dict(b_changes)
    common = sorted(set(a_map) & set(b_map))
    return [a_map[d] for d in common], [b_map[d] for d in common]


def _pearson(xs: list[float], ys: list[float]) -> float | None:
    """Pearson correlation via numpy if available, else manual."""
    n = min(len(xs), len(ys))
    if n < 5:
        return None
    xs, ys = xs[-n:], ys[-n:]
    try:
        import numpy as np
        c = np.corrcoef(xs, ys)[0, 1]
        return None if (math.isnan(c) or math.isinf(c)) else round(float(c), 3)
    except Exception:
        try:
            xm = statistics.mean(xs)
            ym = statistics.mean(ys)
            num = sum((x - xm) * (y - ym) for x, y in zip(xs, ys))
            denom = math.sqrt(
                sum((x - xm) ** 2 for x in xs) * sum((y - ym) ** 2 for y in ys)
            )
            return round(num / denom, 3) if denom else None
        except Exception:
            return None


def _rolling_corr(a: list[float], b: list[float], window: int) -> float | None:
    """Correlation over the last `window` aligned observations."""
    return _pearson(a[-window:], b[-window:]) if len(a) >= window else None


# ══════════════════════════════════════════════════════════════════════════════
# ── 1. VIX Risk Regime ────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

def _vix_regime_signal(vix: float | None) -> dict:
    if vix is None:
        return {
            "current_vix": None, "current_zone": "unknown",
            "warning_level": "unknown", "signal_title": "Data unavailable",
            "signal_summary": "", "rules_used": [],
        }
    if vix < 20:
        zone, warning, title = "calm", "low", "Calm / Complacency Zone"
        summary = (
            f"VIX at {vix:.1f} — below the 20 threshold. Markets are pricing in low volatility. "
            "This is a complacency watch zone: consider profit protection and monitor for early regime shifts."
        )
        rules = ["VIX < 20: calm zone — profit protection monitoring recommended"]
    elif vix < 25:
        zone, warning, title = "elevated", "moderate", "Elevated Volatility — Watch Zone"
        summary = (
            f"VIX at {vix:.1f} — risk regime is worsening. Elevated uncertainty. "
            "Defensive awareness is warranted; this is not yet a stress regime but warrants attention."
        )
        rules = ["VIX 20–25: elevated — uncertainty rising above complacency threshold"]
    elif vix < 30:
        zone, warning, title = "elevated_high", "moderate-high", "Elevated Risk Regime"
        summary = (
            f"VIX at {vix:.1f} — in the 20–30 elevated zone. Meaningful stress signal. "
            "Risk-off positioning is more prevalent. Monitor credit spreads for confirmation."
        )
        rules = ["VIX 25–30: elevated risk — stress signal, watch credit spreads"]
    elif vix < 40:
        zone, warning, title = "stress", "high", "Stress Regime"
        summary = (
            f"VIX at {vix:.1f} — stress regime territory. Significant market dislocation. "
            "Historical patterns show elevated near-term volatility with episodic relief rebounds."
        )
        rules = ["VIX 30–40: stress regime — significant market dislocation"]
    else:
        zone, warning, title = "panic", "extreme", "Panic / Dislocation Regime"
        summary = (
            f"VIX at {vix:.1f} — extreme fear and dislocation. "
            "Historical episodes at this level have preceded significant long-term opportunities, "
            "though near-term volatility typically remains extreme. Not a timing signal."
        )
        rules = ["VIX > 40: panic/dislocation — long-term opportunity watch, not a timing signal"]

    return {
        "current_vix": round(vix, 2),
        "current_zone": zone,
        "warning_level": warning,
        "signal_title": title,
        "signal_summary": summary,
        "rules_used": rules,
    }


async def build_vix_regime_payload(macro_provider) -> dict:
    cache_key = "strategy:vix_regime:v1"
    hit = cache.get(cache_key)
    if hit is not None:
        return hit

    # Run dashboard (cached), FRED VIX history and SPX history concurrently
    dashboard_task   = macro_provider.get_dashboard()
    vix_hist_task    = asyncio.to_thread(_get_vix_history_sync, macro_provider, 1830)
    spx_hist_task    = _get_spx_history(1830)

    dashboard, vix_hist, spx_hist = await asyncio.gather(
        dashboard_task, vix_hist_task, spx_hist_task, return_exceptions=True,
    )
    if isinstance(dashboard,  Exception): dashboard  = {}
    if isinstance(vix_hist,   Exception): vix_hist   = []
    if isinstance(spx_hist,   Exception): spx_hist   = []

    current_snap = _extract_current_snapshot(dashboard)
    current_vix  = current_snap.get("vix")

    # Daily % changes
    vix_changes = _daily_pct_changes(vix_hist, "value")
    spx_changes = _daily_pct_changes(spx_hist, "close")

    aligned_vix, aligned_spx = _align_and_split(vix_changes, spx_changes)
    n = len(aligned_vix)

    corr_7d  = _rolling_corr(aligned_vix, aligned_spx, 7)
    corr_30d = _rolling_corr(aligned_vix, aligned_spx, 30)
    corr_63d = _rolling_corr(aligned_vix, aligned_spx, 63)

    # Recent moves side-by-side
    vix_1d = vix_changes[-1][1] if vix_changes else None
    spx_1d = spx_changes[-1][1] if spx_changes else None

    def _window_summary(v_hist: list, s_hist: list, label: str) -> dict:
        if not v_hist or not s_hist:
            return {"window": label, "insufficient_data": True}
        vv = [x.get("value") for x in v_hist if x.get("value") is not None]
        ss = [x.get("close") for x in s_hist if x.get("close") is not None]
        return {
            "window": label,
            "vix_min":      _r(min(vv)) if vv else None,
            "vix_max":      _r(max(vv)) if vv else None,
            "vix_avg":      _r(statistics.mean(vv)) if vv else None,
            "spx_first":    _r(ss[0])  if ss else None,
            "spx_last":     _r(ss[-1]) if ss else None,
            "spx_return_pct": _r((ss[-1] - ss[0]) / ss[0] * 100, 2) if len(ss) >= 2 and ss[0] else None,
            "data_points":  min(len(vv), len(ss)),
        }

    vix_sig = _vix_regime_signal(current_vix)
    result = {
        "generated_at":          datetime.now(timezone.utc).isoformat(),
        "cache_ttl_seconds":     _REGIME_TTL,
        "vix_zone":              vix_sig.get("current_zone"),
        "risk_regime":           vix_sig.get("warning_level"),
        "current_market_snapshot": current_snap,
        "vix_regime_signal":     vix_sig,
        "vix_spx_correlation": {
            "vix_1d_pct":         _r(vix_1d, 3),
            "spx_1d_pct":         _r(spx_1d, 3),
            "rolling_corr_7d":    corr_7d,
            "rolling_corr_30d":   corr_30d,
            "rolling_corr_63d":   corr_63d,
            "sample_size":        n,
            "calculation_window": "daily_pct_changes aligned by date",
            "correlation_basis":  "VIX daily % change vs S&P 500 (^GSPC) daily % return",
            "interpretation":     (
                "Negative correlation = VIX rises when SPX falls (expected inverse). "
                "Near -1.0 = strong inverse relationship."
            ),
            "last_updated":       datetime.now(timezone.utc).isoformat(),
        },
        "historical_windows": {
            "7d":      _window_summary(vix_hist[-10:],  spx_hist[-10:],  "7 trading days"),
            "quarter": _window_summary(vix_hist[-66:],  spx_hist[-66:],  "63 trading days (~1 quarter)"),
            "1y":      _window_summary(vix_hist[-252:], spx_hist[-252:], "252 trading days (~1 year)"),
            "5y":      _window_summary(vix_hist,        spx_hist,        "5 years"),
        },
        "data_sources": {
            "current_snapshot": "macro:dashboard:v3 (FMP ^GSPC + ^VIX real-time, 15-min TTL)",
            "vix_history":      "FRED VIXCLS → strategy:hist:vixcls:1830 (6-h TTL, pre-warmed every 3 h)",
            "spx_history":      "yfinance ^GSPC → strategy:spx_hist:1830 (6-h TTL, pre-warmed every 3 h)",
            "durable_cache":    "Neon strategy_hist_snapshots (24-h max age, written on every precompute)",
            **({
                "freshness_warning": " | ".join(
                    _NEON_STALE_WARN[k]
                    for k in ("strategy:hist:vixcls:1830", "strategy:spx_hist:1830")
                    if k in _NEON_STALE_WARN
                )
            } if any(k in _NEON_STALE_WARN for k in ("strategy:hist:vixcls:1830", "strategy:spx_hist:1830")) else {}),
        },
    }

    cache.set(cache_key, result, _REGIME_TTL)
    return result


# ══════════════════════════════════════════════════════════════════════════════
# ── 2. Weekly Price Movements ─────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

def _compute_weekly_scorecard(
    bars: list[dict],
    window_label: str,
    window_days: int,
) -> dict:
    """
    Given a list of {date, close} bars (oldest-first), compute the four
    Friday/Monday conditional-outcome scenarios over the tail `window_days` bars.
    """
    _MIN_SAMPLE = 5

    tail = bars[-window_days:] if len(bars) >= window_days else bars[:]

    if len(tail) < _MIN_SAMPLE:
        return {
            "window": window_label,
            "insufficient_sample": True,
            "sample_count": len(tail),
        }

    # Build a date→(weekday, close) lookup for fast prev-day access
    from datetime import date as _date
    import calendar

    def _parse(d: str):
        try:
            y, m, dy = d.split("-")
            return _date(int(y), int(m), int(dy))
        except Exception:
            return None

    dated = [((_parse(b["date"])), b["close"]) for b in tail if _parse(b["date"]) and b.get("close")]
    dated.sort(key=lambda x: x[0])

    by_date = {d: c for d, c in dated}

    # ── Scenario helpers ──────────────────────────────────────────────────────
    def _scenario_stats(returns: list[float], label: str) -> dict:
        if not returns:
            return {"scenario": label, "insufficient_sample": True, "sample_count": 0}
        n      = len(returns)
        green  = [r for r in returns if r > 0]
        red    = [r for r in returns if r <= 0]
        return {
            "scenario":            label,
            "sample_count":        n,
            "green_count":         len(green),
            "red_count":           len(red),
            "green_probability":   _r(len(green) / n * 100),
            "red_probability":     _r(len(red)   / n * 100),
            "average_return_pct":  _r(statistics.mean(returns), 3),
            "median_return_pct":   _r(statistics.median(returns), 3),
            "best_return_pct":     _r(max(returns), 3),
            "worst_return_pct":    _r(min(returns), 3),
            "std_dev_pct":         _r(statistics.stdev(returns), 3) if n >= 2 else None,
            "confidence_label":    (
                "high" if n >= 40 else
                "moderate" if n >= 15 else
                "low" if n >= _MIN_SAMPLE else
                "insufficient"
            ),
            "insufficient_sample": n < _MIN_SAMPLE,
        }

    # Collect returns for each scenario
    red_fri_mon_returns:   list[float] = []
    green_fri_mon_returns: list[float] = []
    red_mon_week_returns:  list[float] = []
    green_mon_week_returns:list[float] = []

    sorted_dates = sorted(by_date.keys())

    for i, d in enumerate(sorted_dates):
        wd = d.weekday()  # 0=Mon … 4=Fri

        # ── Red/Green Friday → next Monday ───────────────────────────────────
        if wd == 4:  # Friday
            prev = sorted_dates[i - 1] if i > 0 else None
            fri_close  = by_date[d]
            thu_close  = by_date.get(prev) if prev and prev.weekday() == 3 else None
            if thu_close is None:
                continue
            fri_is_red = fri_close < thu_close

            # Find next Monday
            next_mon = None
            for j in range(i + 1, min(i + 5, len(sorted_dates))):
                nd = sorted_dates[j]
                if nd.weekday() == 0:
                    next_mon = nd
                    break
            if next_mon is None:
                continue
            mon_close = by_date.get(next_mon)
            if mon_close is None:
                continue
            ret = (mon_close - fri_close) / fri_close * 100
            if fri_is_red:
                red_fri_mon_returns.append(ret)
            else:
                green_fri_mon_returns.append(ret)

        # ── Red/Green Monday → rest of week (Friday close vs Monday close) ────
        if wd == 0:  # Monday
            prev_fri = sorted_dates[i - 1] if i > 0 else None
            if prev_fri is None or prev_fri.weekday() != 4:
                continue
            mon_close      = by_date[d]
            prev_fri_close = by_date.get(prev_fri)
            if prev_fri_close is None:
                continue
            mon_is_red = mon_close < prev_fri_close

            # Find the Friday of this same week
            eow_fri = None
            for j in range(i + 1, min(i + 6, len(sorted_dates))):
                nd = sorted_dates[j]
                if nd.weekday() == 4:
                    eow_fri = nd
                    break
            if eow_fri is None:
                continue
            eow_close = by_date.get(eow_fri)
            if eow_close is None:
                continue
            ret = (eow_close - mon_close) / mon_close * 100
            if mon_is_red:
                red_mon_week_returns.append(ret)
            else:
                green_mon_week_returns.append(ret)

    return {
        "window":          window_label,
        "window_bars":     len(tail),
        "red_friday_to_monday":   _scenario_stats(red_fri_mon_returns,   "Red Friday → Monday"),
        "green_friday_to_monday": _scenario_stats(green_fri_mon_returns, "Green Friday → Monday"),
        "red_monday_to_friday":   _scenario_stats(red_mon_week_returns,  "Red Monday → Rest of Week"),
        "green_monday_to_friday": _scenario_stats(green_mon_week_returns,"Green Monday → Rest of Week"),
    }


async def build_weekly_price_movements_payload() -> dict:
    cache_key = "strategy:weekly_price_movements:v1"
    hit = cache.get(cache_key)
    if hit is not None:
        return hit

    bars = await _get_spx_history(1830)   # ~5 years

    if not bars:
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "error": "SPX history unavailable",
            "data_source": "yfinance ^GSPC",
        }

    now = datetime.now(timezone.utc)
    windows = {
        "5y":      _compute_weekly_scorecard(bars, "5 years",                1830),
        "1y":      _compute_weekly_scorecard(bars, "1 year",                  252),
        "quarter": _compute_weekly_scorecard(bars, "1 quarter (~63 days)",     63),
        "7d": (
            _compute_weekly_scorecard(bars, "7 trading days", 10)
            if len(bars) >= 10
            else {"window": "7 trading days", "insufficient_sample": True, "sample_count": len(bars)}
        ),
    }
    spx_key = "strategy:spx_hist:1830"
    result = {
        "generated_at":         now.isoformat(),
        "cache_ttl_seconds":    _WEEKLY_TTL,
        "scorecard_updated_at": now.isoformat(),
        "data_source":          "yfinance ^GSPC daily OHLCV",
        "durable_cache":        "Neon strategy_hist_snapshots (24-h max age, written on every precompute)",
        **({"freshness_warning": _NEON_STALE_WARN[spx_key]} if spx_key in _NEON_STALE_WARN else {}),
        "total_bars_loaded":    len(bars),
        "spx_proxy":            "^GSPC",
        "computation":          "deterministic Python — no AI",
        "windows":              windows,
        "intraweek_scenarios":  windows,
        "current_week_context": _current_week_context(bars),
        "cache_age_seconds":    0,
    }

    cache.set(cache_key, result, _WEEKLY_TTL)
    return result


def _current_week_context(bars: list[dict]) -> dict:
    """
    Identify today's day-of-week and which scenario (if any) is already
    triggered this week based on the most recent available data.
    """
    if not bars:
        return {"available": False}
    from datetime import date as _date

    def _p(d: str):
        try:
            y, m, dy = d.split("-")
            return _date(int(y), int(m), int(dy))
        except Exception:
            return None

    today = _date.today()
    wd    = today.weekday()  # 0=Mon … 6=Sun

    last3 = [(b["date"], b["close"]) for b in bars[-5:] if b.get("close")]
    if not last3:
        return {"available": False}

    last_date_str, last_close = last3[-1]
    last_date = _p(last_date_str)

    # 52-week high/low from the last 252 trading-day bars
    trailing_252 = [b.get("close") for b in bars[-252:] if b.get("close") is not None]
    spx_52w_high = _r(max(trailing_252)) if trailing_252 else None
    spx_52w_low  = _r(min(trailing_252)) if trailing_252 else None

    context: dict[str, Any] = {
        "available":       True,
        "today_weekday":   today.strftime("%A"),
        "last_bar_date":   last_date_str,
        "last_close":      _r(last_close),
        "spx_last_close":  _r(last_close),
        "spx_52w_high":    spx_52w_high,
        "spx_52w_low":     spx_52w_low,
    }

    # Detect if the most recent Friday was red/green
    for i in range(len(last3) - 1, -1, -1):
        d = _p(last3[i][0])
        if d and d.weekday() == 4 and i > 0:
            prev_close = last3[i - 1][1]
            fri_close  = last3[i][1]
            context["last_friday"] = {
                "date":       last3[i][0],
                "close":      _r(fri_close),
                "prev_close": _r(prev_close),
                "direction":  "red" if fri_close < prev_close else "green",
                "change_pct": _r((fri_close - prev_close) / prev_close * 100, 3) if prev_close else None,
            }
            break

    # Detect if today or the most recent Monday was red/green
    for i in range(len(last3) - 1, -1, -1):
        d = _p(last3[i][0])
        if d and d.weekday() == 0 and i > 0:
            prev_close = last3[i - 1][1]
            mon_close  = last3[i][1]
            context["last_monday"] = {
                "date":       last3[i][0],
                "close":      _r(mon_close),
                "prev_close": _r(prev_close),
                "direction":  "red" if mon_close < prev_close else "green",
                "change_pct": _r((mon_close - prev_close) / prev_close * 100, 3) if prev_close else None,
            }
            break

    return context


# ══════════════════════════════════════════════════════════════════════════════
# ── 3. 10Y Yield vs S&P 500 ───────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

def _yield_spx_regime_label(
    yield_changes: list[float],
    spx_changes: list[float],
    window: int,
) -> str:
    if not yield_changes or not spx_changes:
        return "insufficient_data"
    w = min(window, len(yield_changes), len(spx_changes))
    avg_y = statistics.mean(yield_changes[-w:]) if w else 0
    avg_s = statistics.mean(spx_changes[-w:])   if w else 0
    if avg_y > 2:       # rising yields (>2 bps avg/day over window)
        return "yields_rising_spx_rising" if avg_s > 0 else "yields_rising_spx_falling"
    elif avg_y < -2:    # falling yields
        return "yields_falling_spx_rising" if avg_s > 0 else "yields_falling_spx_falling"
    else:
        return "mixed_flat"


async def build_ten_year_spx_payload(macro_provider) -> dict:
    cache_key = "strategy:ten_year_spx:v1"
    hit = cache.get(cache_key)
    if hit is not None:
        return hit

    dashboard_task  = macro_provider.get_dashboard()
    ten_y_hist_task = asyncio.to_thread(_get_10y_history_sync, macro_provider, 1830)
    spx_hist_task   = _get_spx_history(1830)

    dashboard, ten_y_hist, spx_hist = await asyncio.gather(
        dashboard_task, ten_y_hist_task, spx_hist_task, return_exceptions=True,
    )
    if isinstance(dashboard,    Exception): dashboard    = {}
    if isinstance(ten_y_hist,   Exception): ten_y_hist   = []
    if isinstance(spx_hist,     Exception): spx_hist     = []

    current_snap = _extract_current_snapshot(dashboard)

    # Use ^GSPC history for both current and 7d-ago SPX — avoids SPY/GSPC scale mismatch
    # (SPY Tradier price ~741 vs ^GSPC ~7300 are different scales; use one source throughout)
    us10y_now   = current_snap.get("us_10y") or (_s(ten_y_hist[-1].get("value")) if ten_y_hist else None)
    spx_now     = _s(spx_hist[-1].get("close")) if spx_hist else None
    spx_1d_ago  = _s(spx_hist[-2].get("close")) if len(spx_hist) >= 2 else None

    us10y_7d_ago = _s(ten_y_hist[-8].get("value")) if len(ten_y_hist) >= 8 else None
    spx_7d_ago   = _s(spx_hist[-8].get("close"))   if len(spx_hist) >= 8  else None

    us10y_7d_bps = _r((us10y_now - us10y_7d_ago) * 100, 1) if us10y_now and us10y_7d_ago else None
    spx_1d_pct   = _r((spx_now - spx_1d_ago) / spx_1d_ago * 100, 2) if spx_now and spx_1d_ago else None
    spx_7d_pct   = _r((spx_now - spx_7d_ago) / spx_7d_ago * 100, 2) if spx_now and spx_7d_ago else None

    # Daily changes for correlation
    ten_y_bps    = _daily_bps_changes(ten_y_hist)
    spx_pct      = _daily_pct_changes(spx_hist, "close")

    aligned_y, aligned_s = _align_and_split(ten_y_bps, spx_pct)
    n = len(aligned_y)

    corr_7d  = _rolling_corr(aligned_y, aligned_s, 7)
    corr_30d = _rolling_corr(aligned_y, aligned_s, 30)
    corr_63d = _rolling_corr(aligned_y, aligned_s, 63)

    def _hist_window(t_hist, s_hist, label) -> dict:
        if not t_hist or not s_hist:
            return {"window": label, "insufficient_data": True}
        tv = [x.get("value") for x in t_hist if x.get("value") is not None]
        sv = [x.get("close") for x in s_hist if x.get("close") is not None]
        if not tv or not sv:
            return {"window": label, "insufficient_data": True}
        return {
            "window":            label,
            "data_points":       min(len(tv), len(sv)),
            "ten_y_start":       _r(tv[0], 3),
            "ten_y_end":         _r(tv[-1], 3),
            "ten_y_change_bps":  _r((tv[-1] - tv[0]) * 100, 1),
            "spx_start":         _r(sv[0]),
            "spx_end":           _r(sv[-1]),
            "spx_return_pct":    _r((sv[-1] - sv[0]) / sv[0] * 100, 2) if sv[0] else None,
        }

    result = {
        "generated_at":       datetime.now(timezone.utc).isoformat(),
        "cache_ttl_seconds":  _TEN_YEAR_TTL,
        "current_market_snapshot": current_snap,
        "ten_year_spx_tracker": {
            "us_10y_current":        _r(us10y_now, 3),
            "us_10y_1d_bps":         _r((us10y_now - _s(ten_y_hist[-2].get("value"))) * 100, 1) if us10y_now and len(ten_y_hist) >= 2 and _s(ten_y_hist[-2].get("value")) else None,
            "us_10y_7d_change_bps":  us10y_7d_bps,
            "spx_current":           _r(spx_now),
            "spx_1d_change_pct":     spx_1d_pct,
            "spx_7d_change_pct":     spx_7d_pct,
            "dxy_current":           current_snap.get("dxy"),
            "dxy_change_pct":        current_snap.get("dxy_change_pct"),
            "spx_proxy_note":        "^GSPC daily close (yfinance); current = last available bar",
            "data_note":             "10Y from FMP real-time (fallback: FRED DGS10)",
        },
        "rolling_correlation": {
            "correlation_basis":  "US 10Y daily bps change vs S&P 500 (^GSPC) daily % return",
            "rolling_corr_7d":    corr_7d,
            "rolling_corr_30d":   corr_30d,
            "rolling_corr_63d":   corr_63d,
            "sample_size":        n,
            "lookback_window":    "daily observations, inner-joined by date",
            "last_updated":       datetime.now(timezone.utc).isoformat(),
            "interpretation": (
                "Positive = yields and stocks rising together (growth optimism). "
                "Negative = yields rising while stocks fall (rate-fear regime)."
            ),
        },
        "regime_labels": {
            "7d":      _yield_spx_regime_label(aligned_y, aligned_s, 7),
            "30d":     _yield_spx_regime_label(aligned_y, aligned_s, 30),
            "63d":     _yield_spx_regime_label(aligned_y, aligned_s, 63),
        },
        "historical_windows": {
            "7d":      _hist_window(ten_y_hist[-10:],  spx_hist[-10:],  "7 trading days"),
            "quarter": _hist_window(ten_y_hist[-66:],  spx_hist[-66:],  "63 trading days (~1 quarter)"),
            "1y":      _hist_window(ten_y_hist[-252:], spx_hist[-252:], "252 trading days (~1 year)"),
            "5y":      _hist_window(ten_y_hist,        spx_hist,        "5 years"),
        },
        "data_sources": {
            "current_snapshot": "macro:dashboard:v3 (FMP treasury real-time + ^GSPC, 15-min TTL)",
            "ten_y_history":    "FRED DGS10 → strategy:hist:dgs10:1830 (6-h TTL, pre-warmed every 3 h)",
            "spx_history":      "yfinance ^GSPC → strategy:spx_hist:1830 (6-h TTL, pre-warmed every 3 h)",
            "durable_cache":    "Neon strategy_hist_snapshots (24-h max age, written on every precompute)",
            **({
                "freshness_warning": " | ".join(
                    _NEON_STALE_WARN[k]
                    for k in ("strategy:hist:dgs10:1830", "strategy:spx_hist:1830")
                    if k in _NEON_STALE_WARN
                )
            } if any(k in _NEON_STALE_WARN for k in ("strategy:hist:dgs10:1830", "strategy:spx_hist:1830")) else {}),
        },
    }

    cache.set(cache_key, result, _TEN_YEAR_TTL)
    return result
