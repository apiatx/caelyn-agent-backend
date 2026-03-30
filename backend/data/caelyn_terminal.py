"""
Caelyn Terminal — portfolio analytics provider.

Produces the full JSON payload for GET /api/caelyn-terminal.
Data sources: Tradier (quotes + history + clock), Finnhub (earnings — sync),
              FMP (news — async), Yahoo (DXY ticker tape).
"""
from __future__ import annotations

import asyncio
import json
import math
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from data.cache import cache

try:
    from langsmith import traceable
except ImportError:
    def traceable(*a, **kw):
        def _d(fn): return fn
        return _d if not (a and callable(a[0])) else a[0]

# ─── Asset-class taxonomy ────────────────────────────────────────────────────

_US_EQUITY = "US Equity"
_INTL_DEV  = "Intl Developed"
_EM        = "Emerging Markets"
_FIXED     = "Fixed Income"
_STOCK     = "Individual Stocks"
_REAL      = "Real Estate"
_COMM      = "Commodities"
_OTHER     = "Other"

ASSET_CLASS_MAP: dict[str, str] = {
    # Broad US equity
    "SCHB": _US_EQUITY, "VTI": _US_EQUITY, "ITOT": _US_EQUITY,
    "SPY": _US_EQUITY, "IVV": _US_EQUITY, "VOO": _US_EQUITY,
    "QQQ": _US_EQUITY, "QQQM": _US_EQUITY, "IWM": _US_EQUITY,
    "MDY": _US_EQUITY, "IJH": _US_EQUITY, "SCHA": _US_EQUITY,
    "DIA": _US_EQUITY, "RSP": _US_EQUITY,
    # Sector / dividend / factor
    "DGRO": _US_EQUITY, "VYM": _US_EQUITY, "SCHD": _US_EQUITY,
    "VIG": _US_EQUITY, "SDY": _US_EQUITY, "HDV": _US_EQUITY,
    "NOBL": _US_EQUITY, "DGRW": _US_EQUITY,
    "XLK": _US_EQUITY, "XLF": _US_EQUITY, "XLV": _US_EQUITY,
    "XLE": _US_EQUITY, "XLI": _US_EQUITY, "XLP": _US_EQUITY,
    "XLY": _US_EQUITY, "XLB": _US_EQUITY, "XLU": _US_EQUITY,
    "XLRE": _REAL,     "XLC": _US_EQUITY,
    # International developed
    "SCHF": _INTL_DEV, "VEA": _INTL_DEV, "EFA": _INTL_DEV,
    "IEFA": _INTL_DEV, "SPDW": _INTL_DEV, "VGK": _INTL_DEV,
    "EWJ": _INTL_DEV, "HEDJ": _INTL_DEV,
    # Emerging markets
    "VWO": _EM, "IEMG": _EM, "EEM": _EM, "SCHE": _EM,
    "SPEM": _EM, "DEM": _EM, "GXC": _EM, "MCHI": _EM,
    # Fixed income
    "AGG": _FIXED, "BND": _FIXED, "BNDX": _FIXED,
    "LQD": _FIXED, "HYG": _FIXED, "JNK": _FIXED,
    "TLT": _FIXED, "IEF": _FIXED, "SHY": _FIXED,
    "VTEB": _FIXED, "VCIT": _FIXED, "MUB": _FIXED,
    "SCHZ": _FIXED, "SCHI": _FIXED, "SCHS": _FIXED,
    # Real estate
    "VNQ": _REAL, "IYR": _REAL,
    # Commodities
    "GLD": _COMM, "IAU": _COMM, "SLV": _COMM,
    "USO": _COMM, "DJP": _COMM, "PDBC": _COMM,
}

ASSET_CLASS_COLORS: dict[str, str] = {
    _US_EQUITY: "#38bdf8",
    _INTL_DEV:  "#6366f1",
    _EM:        "#f59e0b",
    _FIXED:     "#22c55e",
    _STOCK:     "#a78bfa",
    _REAL:      "#f43f5e",
    _COMM:      "#fb923c",
    _OTHER:     "#94a3b8",
}

TICKER_TAPE_SYMS = ["SPY", "QQQ", "IWM", "GLD", "DIA"]

# ─── Helpers ─────────────────────────────────────────────────────────────────

def _sf(v: Any) -> float | None:
    try:
        return float(v) if v not in (None, "", "-") else None
    except Exception:
        return None


def _safe_round(v: float | None, n: int = 2) -> float | None:
    return round(v, n) if v is not None else None


def _returns(closes: list[float]) -> list[float]:
    """Compute daily percentage returns from a list of closing prices."""
    r = []
    for i in range(1, len(closes)):
        if closes[i - 1] and closes[i - 1] != 0:
            r.append((closes[i] - closes[i - 1]) / closes[i - 1])
    return r


def _annualized_vol(closes: list[float]) -> float | None:
    rets = _returns(closes)
    if len(rets) < 10:
        return None
    n = len(rets)
    mean = sum(rets) / n
    variance = sum((r - mean) ** 2 for r in rets) / (n - 1)
    return round(math.sqrt(variance * 252) * 100, 2)


def _annualized_return(closes: list[float]) -> float | None:
    if len(closes) < 2 or not closes[0] or closes[0] == 0:
        return None
    total = (closes[-1] - closes[0]) / closes[0]
    years = len(closes) / 252
    if years <= 0:
        return None
    return (1 + total) ** (1 / years) - 1


def _max_drawdown(closes: list[float]) -> float | None:
    if len(closes) < 2:
        return None
    peak = closes[0]
    max_dd = 0.0
    for c in closes:
        if c > peak:
            peak = c
        if peak > 0:
            dd = (peak - c) / peak
            if dd > max_dd:
                max_dd = dd
    return round(max_dd * 100, 2)


def _correlation(a: list[float], b: list[float]) -> float | None:
    n = min(len(a), len(b))
    if n < 10:
        return None
    a, b = a[-n:], b[-n:]
    ma = sum(a) / n
    mb = sum(b) / n
    cov = sum((a[i] - ma) * (b[i] - mb) for i in range(n))
    va  = sum((x - ma) ** 2 for x in a)
    vb  = sum((x - mb) ** 2 for x in b)
    if va <= 0 or vb <= 0:
        return None
    return round(cov / math.sqrt(va * vb), 4)


def _std(vals: list[float]) -> float:
    if not vals:
        return 0.0
    n = len(vals)
    mean = sum(vals) / n
    return math.sqrt(sum((v - mean) ** 2 for v in vals) / max(n - 1, 1))


def _market_status_et() -> str:
    """Return market status based on Eastern time, no API call needed."""
    import zoneinfo
    et = datetime.now(zoneinfo.ZoneInfo("America/New_York"))
    wd = et.weekday()   # 0=Mon 6=Sun
    h, m = et.hour, et.minute
    mins = h * 60 + m
    if wd >= 5:
        return "CLOSED"
    if 0 <= mins < 240:        # 00:00-04:00 ET
        return "CLOSED"
    if 240 <= mins < 570:      # 04:00-09:30 ET
        return "PRE-MARKET"
    if 570 <= mins < 960:      # 09:30-16:00 ET
        return "OPEN"
    if 960 <= mins < 1200:     # 16:00-20:00 ET
        return "AFTER-HOURS"
    return "CLOSED"


def _month_label(dt: date) -> str:
    return dt.strftime("%b '%y")


def _asset_class(ticker: str) -> str:
    return ASSET_CLASS_MAP.get(ticker.upper(), _STOCK)


# ─── Core provider ───────────────────────────────────────────────────────────

class CaelynTerminalProvider:
    """Assembles the full /api/caelyn-terminal payload."""

    def __init__(self, tradier, finnhub, fmp, yahoo):
        self.tradier = tradier
        self.finnhub = finnhub
        self.fmp = fmp
        self.yahoo = yahoo

    @traceable(name="caelyn_terminal.get")
    async def get(self, portfolio_file: Path) -> dict:
        cache_key = "caelyn:terminal:v3"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        result = await self._build(portfolio_file)
        cache.set(cache_key, result, 90)   # 90-second cache
        return result

    async def _build(self, portfolio_file: Path) -> dict:
        # ── 1. Load holdings ────────────────────────────────────────────
        holdings_raw = self._load_holdings(portfolio_file)
        if not holdings_raw:
            return self._empty()

        tickers = [h["ticker"].upper() for h in holdings_raw]

        # ── 2. Parallel data fetches ─────────────────────────────────────
        tape_extras = [t for t in TICKER_TAPE_SYMS if t not in tickers]
        quote_syms  = list(dict.fromkeys(tickers + tape_extras))  # deduplicated

        hist_start = (date.today() - timedelta(days=400)).isoformat()

        tasks = [
            self._fetch_quotes(quote_syms),
            self._fetch_histories(tickers + ["SPY"], hist_start),
            self._fetch_earnings_calendar(tickers),
            self._fetch_news(tickers[:4]),
        ]
        quotes_list, histories, earnings_raw, news_raw = await asyncio.gather(
            *tasks, return_exceptions=True
        )

        quotes_list  = quotes_list  if not isinstance(quotes_list, Exception)  else []
        histories    = histories    if not isinstance(histories, Exception)     else {}
        earnings_raw = earnings_raw if not isinstance(earnings_raw, Exception)  else []
        news_raw     = news_raw     if not isinstance(news_raw, Exception)      else []

        # index quotes by symbol
        quotes: dict[str, dict] = {q["symbol"]: q for q in quotes_list if q.get("symbol")}

        # ── 3. Portfolio value + allocations ────────────────────────────
        positions: list[dict] = []
        total_value = 0.0
        total_cost  = 0.0

        for h in holdings_raw:
            sym    = h["ticker"].upper()
            shares = float(h.get("shares", 0) or 0)
            cost   = float(h.get("avg_cost", 0) or 0)
            q      = quotes.get(sym, {})
            price  = _sf(q.get("last")) or 0.0
            chg    = _sf(q.get("change")) or 0.0
            chgpct = _sf(q.get("change_percentage")) or 0.0
            market_val = shares * price

            total_value += market_val
            total_cost  += shares * cost

            positions.append({
                "_sym":       sym,
                "_shares":    shares,
                "_cost":      cost,
                "ticker":     sym,
                "price":      _safe_round(price),
                "change":     _safe_round(chg),
                "change_pct": _safe_round(chgpct, 3),
                "market_val": market_val,
                "w52_high":   _safe_round(_sf(q.get("week_52_high"))),
                "w52_low":    _safe_round(_sf(q.get("week_52_low"))),
            })

        # Set allocations
        for p in positions:
            p["allocation_pct"] = round(
                p["market_val"] / total_value * 100, 1
            ) if total_value else 0.0

        # Sort by allocation descending
        positions.sort(key=lambda x: x["allocation_pct"], reverse=True)

        # ── 4. Portfolio-level change today ──────────────────────────────
        change_today = sum(
            p["_shares"] * (p["change"] or 0) for p in positions
        )
        prev_total = total_value - change_today
        change_pct_today = (
            round(change_today / prev_total * 100, 2) if prev_total else 0.0
        )

        # ── 5. Performance chart ─────────────────────────────────────────
        perf_chart = self._build_perf_chart(positions, histories)

        # ── 6. Asset allocation ──────────────────────────────────────────
        alloc = self._build_allocation(positions, total_value)

        # ── 7. Correlation matrix (top 5) ────────────────────────────────
        top5 = [p["ticker"] for p in positions[:5]]
        corr = self._build_correlation(top5, histories)

        # ── 8. Risk metrics ──────────────────────────────────────────────
        risk = self._build_risk(positions, histories, total_value)

        # ── 9. Volatility per holding ────────────────────────────────────
        vol_list = self._build_volatility(positions, histories)

        # ── 10. Risk suggestions ─────────────────────────────────────────
        suggestions = self._build_suggestions(positions, alloc, risk)

        # ── 11. Performance periods (1d/5d/1m/6m/1y) ────────────────────
        periods = self._build_periods(positions, histories, change_pct_today)

        # ── 12. Sentiment ────────────────────────────────────────────────
        sentiment = self._sentiment(change_pct_today, risk)

        # ── 13. Top movers ───────────────────────────────────────────────
        top_movers = self._top_movers(positions)

        # ── 14. Earnings calendar ────────────────────────────────────────
        earnings_cal = self._build_earnings(earnings_raw)

        # ── 15. Ticker tape ──────────────────────────────────────────────
        ticker_tape = self._build_tape(quotes)

        # ── 16. News ticker ──────────────────────────────────────────────
        news_ticker = self._build_news(news_raw, positions)

        # ── 17. Total return ─────────────────────────────────────────────
        total_return_val = total_value - total_cost
        total_return_pct = round(total_return_val / total_cost * 100, 1) if total_cost else 0.0

        return {
            "portfolio": {
                "value":           round(total_value, 2),
                "change_today":    round(change_today, 2),
                "change_pct_today": change_pct_today,
                "perf_1d":         periods["perf_1d"],
                "perf_5d":         periods["perf_5d"],
                "perf_1m":         periods["perf_1m"],
                "perf_6m":         periods["perf_6m"],
                "perf_1y":         periods["perf_1y"],
                "total_return_pct": total_return_pct,
                "total_return_value": round(total_return_val, 2),
                "sentiment":       sentiment,
                "market_status":   _market_status_et(),
            },
            "positions_count":  len(positions),
            "holdings":         self._format_holdings(positions),
            "performance_chart": perf_chart,
            "asset_allocation":  alloc,
            "correlation_matrix": corr,
            "risk_metrics":      risk,
            "volatility":        vol_list,
            "risk_suggestions":  suggestions,
            "top_movers":        top_movers,
            "earnings_calendar": earnings_cal,
            "ticker_tape":       ticker_tape,
            "news_ticker":       news_ticker,
            "as_of":             datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

    # ── Data fetchers ─────────────────────────────────────────────────────

    async def _fetch_quotes(self, syms: list[str]) -> list[dict]:
        if not self.tradier:
            return []
        try:
            return await asyncio.wait_for(self.tradier.get_quotes(syms), timeout=12.0)
        except Exception as e:
            print(f"[CAELYN] Tradier quotes error: {e}")
            return []

    async def _fetch_histories(
        self, syms: list[str], start: str
    ) -> dict[str, list[dict]]:
        """Fetch 1Y+ daily bars for each symbol. Returns {sym: [bars]}."""
        if not self.tradier:
            return {}
        tasks = [
            self.tradier.get_history(sym, "daily", start)
            for sym in syms
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return {
            sym: (res if not isinstance(res, Exception) else [])
            for sym, res in zip(syms, results)
        }

    async def _fetch_earnings_calendar(self, tickers: list[str]) -> list[dict]:
        """Market-wide Finnhub earnings calendar filtered to our holdings."""
        if not self.finnhub:
            return []
        try:
            data = await asyncio.wait_for(
                asyncio.to_thread(self.finnhub.get_earnings_calendar),
                timeout=10.0,
            )
            holding_set = set(t.upper() for t in tickers)
            return [e for e in data if (e.get("ticker") or "").upper() in holding_set]
        except Exception as e:
            print(f"[CAELYN] Earnings calendar error: {e}")
            return []

    async def _fetch_news(self, tickers: list[str]) -> list[dict]:
        """Finnhub company news for top holdings (synchronous SDK — run via thread)."""
        if not self.finnhub or not tickers:
            return []
        try:
            tasks = [
                asyncio.to_thread(self.finnhub.get_company_news, t, 7)
                for t in tickers[:4]
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            combined = []
            for sym, res in zip(tickers[:4], results):
                if isinstance(res, list):
                    for item in res:
                        item["_sym"] = sym
                        combined.append(item)
            return combined
        except Exception as e:
            print(f"[CAELYN] News fetch error: {e}")
            return []

    # ── Builders ──────────────────────────────────────────────────────────

    def _format_holdings(self, positions: list[dict]) -> list[dict]:
        return [
            {
                "ticker":       p["ticker"],
                "price":        p["price"],
                "change":       p["change"],
                "change_pct":   p["change_pct"],
                "allocation_pct": p["allocation_pct"],
            }
            for p in positions
        ]

    def _build_perf_chart(
        self,
        positions: list[dict],
        histories: dict[str, list[dict]],
    ) -> list[dict]:
        """
        Compute portfolio value at each common trading date, normalized to 0%.
        SPY is normalized in parallel as benchmark.
        Sample ~8 points: start + 6 quarterly-ish + end.
        """
        spy_bars = histories.get("SPY", [])
        if not spy_bars:
            return []

        # Build a date → price map for each holding
        price_maps: dict[str, dict[str, float]] = {}
        for p in positions:
            sym = p["_sym"]
            bars = histories.get(sym, [])
            price_maps[sym] = {b["date"]: b["close"] for b in bars if b.get("close")}

        spy_map = {b["date"]: b["close"] for b in spy_bars if b.get("close")}
        all_dates = sorted(spy_map.keys())
        if not all_dates:
            return []

        # Compute portfolio value on each SPY trading date
        series_dates: list[str] = []
        port_vals:    list[float] = []
        spy_vals:     list[float] = []

        for dt in all_dates:
            total = 0.0
            for p in positions:
                pm = price_maps.get(p["_sym"], {})
                px = pm.get(dt)
                if px is None:
                    # use closest previous price
                    closest = max((d for d in pm if d <= dt), default=None)
                    px = pm.get(closest, p["price"] or 0)
                total += p["_shares"] * (px or 0)
            series_dates.append(dt)
            port_vals.append(total)
            spy_vals.append(spy_map[dt])

        if not port_vals or port_vals[0] == 0:
            return []

        # Normalize to 0% at start
        p0 = port_vals[0]
        s0 = spy_vals[0]

        def norm_p(v): return round((v - p0) / p0 * 100, 2) if p0 else 0.0
        def norm_s(v): return round((v - s0) / s0 * 100, 2) if s0 else 0.0

        # Sample: first, then ~6 evenly-spaced interior + last
        n = len(series_dates)
        if n <= 8:
            idxs = list(range(n))
        else:
            step = n // 7
            idxs = [0] + [step * i for i in range(1, 7)] + [n - 1]
            idxs = sorted(set(idxs))

        result = []
        for i in idxs:
            dt_obj = datetime.strptime(series_dates[i], "%Y-%m-%d").date()
            result.append({
                "date":      _month_label(dt_obj),
                "portfolio": norm_p(port_vals[i]),
                "sp500":     norm_s(spy_vals[i]),
            })
        return result

    def _build_allocation(
        self, positions: list[dict], total_value: float
    ) -> list[dict]:
        class_totals: dict[str, float] = {}
        for p in positions:
            ac = _asset_class(p["ticker"])
            class_totals[ac] = class_totals.get(ac, 0) + p["market_val"]

        result = []
        for ac, val in sorted(class_totals.items(), key=lambda x: -x[1]):
            pct = round(val / total_value * 100, 1) if total_value else 0.0
            result.append({
                "label": ac,
                "pct":   pct,
                "color": ASSET_CLASS_COLORS.get(ac, ASSET_CLASS_COLORS[_OTHER]),
            })
        return result

    def _build_correlation(
        self, tickers: list[str], histories: dict[str, list[dict]]
    ) -> dict:
        # Build aligned returns for each ticker
        returns_map: dict[str, dict[str, float]] = {}
        for t in tickers:
            bars = histories.get(t, [])
            closes = [(b["date"], b["close"]) for b in bars if b.get("close")]
            if len(closes) < 10:
                continue
            rets = {}
            for i in range(1, len(closes)):
                d, c = closes[i]
                prev = closes[i - 1][1]
                if prev and prev != 0:
                    rets[d] = (c - prev) / prev
            returns_map[t] = rets

        valid = [t for t in tickers if t in returns_map]
        n = len(valid)
        if n == 0:
            return {"tickers": [], "values": []}

        # Common dates
        common_dates = sorted(
            set.intersection(*[set(returns_map[t].keys()) for t in valid])
        )
        if len(common_dates) < 10:
            return {"tickers": valid, "values": [[1.0] * n for _ in range(n)]}

        vecs: dict[str, list[float]] = {
            t: [returns_map[t][d] for d in common_dates] for t in valid
        }

        mat = []
        for i, ti in enumerate(valid):
            row = []
            for j, tj in enumerate(valid):
                if i == j:
                    row.append(1.0)
                elif j < i:
                    row.append(mat[j][i])   # symmetric
                else:
                    c = _correlation(vecs[ti], vecs[tj])
                    row.append(c if c is not None else 0.0)
            mat.append(row)

        return {"tickers": valid, "values": mat}

    def _build_risk(
        self,
        positions: list[dict],
        histories: dict[str, list[dict]],
        total_value: float,
    ) -> dict:
        spy_bars = histories.get("SPY", [])
        spy_closes = [b["close"] for b in spy_bars if b.get("close")]
        spy_rets = _returns(spy_closes)
        spy_std = _std(spy_rets)

        weighted_vol = 0.0
        weighted_beta = 0.0
        all_port_rets: dict[str, float] = {}

        for p in positions:
            sym = p["_sym"]
            bars = histories.get(sym, [])
            closes = [b["close"] for b in bars if b.get("close")]
            if len(closes) < 20:
                continue
            w = p["allocation_pct"] / 100
            vol = _annualized_vol(closes) or 0.0
            weighted_vol += w * vol

            rets_map = {}
            for i in range(1, len(bars)):
                if bars[i].get("close") and bars[i - 1].get("close") and bars[i - 1]["close"] != 0:
                    rets_map[bars[i]["date"]] = (bars[i]["close"] - bars[i - 1]["close"]) / bars[i - 1]["close"]

            # Portfolio return contribution
            for d, r in rets_map.items():
                all_port_rets[d] = all_port_rets.get(d, 0) + w * r

            # Beta
            if spy_std and spy_std > 0:
                spy_dates = {b["date"]: i for i, b in enumerate(spy_bars)}
                common_rets = []
                common_spy  = []
                for d, r in rets_map.items():
                    si = spy_dates.get(d)
                    if si is not None and si > 0:
                        spy_prev = spy_bars[si - 1].get("close")
                        spy_cur  = spy_bars[si].get("close")
                        if spy_prev and spy_cur and spy_prev != 0:
                            common_rets.append(r)
                            common_spy.append((spy_cur - spy_prev) / spy_prev)
                if len(common_rets) >= 20:
                    c = _correlation(common_rets, common_spy)
                    sr = _std(common_rets)
                    ss = _std(common_spy)
                    beta_i = c * (sr / ss) if (c and ss and ss > 0) else 1.0
                    weighted_beta += w * beta_i

        # Portfolio-level stats
        port_rets_list = [all_port_rets[d] for d in sorted(all_port_rets)]
        port_vol = _std(port_rets_list) * math.sqrt(252) * 100 if port_rets_list else weighted_vol
        ann_ret = (
            (sum(all_port_rets.values()) / len(all_port_rets) * 252)
            if all_port_rets else 0.0
        )

        rf = 0.043   # risk-free rate proxy (10Y treasury %)
        sharpe = round((ann_ret - rf) / (port_vol / 100), 2) if port_vol else None

        neg_rets = [r for r in port_rets_list if r < 0]
        down_std = _std(neg_rets) * math.sqrt(252) * 100 if neg_rets else port_vol
        sortino = round((ann_ret - rf) / (down_std / 100), 2) if down_std else None

        # Max drawdown: reconstruct from portfolio daily returns
        sorted_dates = sorted(all_port_rets.keys())
        port_val = 100.0
        port_val_series = [port_val]
        for d in sorted_dates:
            port_val *= (1 + all_port_rets[d])
            port_val_series.append(port_val)
        max_dd = _max_drawdown(port_val_series)

        # Top concentration
        top_pos = max(positions, key=lambda x: x["allocation_pct"], default=None)
        top_conc = int(round(top_pos["allocation_pct"])) if top_pos else 0
        top_conc_label = _asset_class(top_pos["ticker"]) if top_pos else ""

        return {
            "weighted_volatility": round(weighted_vol, 1),
            "max_drawdown":        max_dd,
            "top_concentration":   top_conc,
            "top_concentration_label": top_conc_label,
            "portfolio_beta":      round(weighted_beta, 2) if weighted_beta else None,
            "sharpe_ratio":        sharpe,
            "sortino_ratio":       sortino,
        }

    def _build_volatility(
        self, positions: list[dict], histories: dict[str, list[dict]]
    ) -> list[dict]:
        vols = []
        for p in positions:
            bars = histories.get(p["_sym"], [])
            closes = [b["close"] for b in bars if b.get("close")]
            v = _annualized_vol(closes)
            if v is not None:
                vols.append({"ticker": p["ticker"], "vol": v})
        return sorted(vols, key=lambda x: -x["vol"])

    def _build_suggestions(
        self,
        positions: list[dict],
        alloc: list[dict],
        risk: dict,
    ) -> list[dict]:
        suggestions = []

        alloc_map = {a["label"]: a["pct"] for a in alloc}

        # High single-holding concentration
        for p in positions:
            if p["allocation_pct"] >= 40:
                suggestions.append({
                    "level": "RISK",
                    "title": f"High Concentration in {p['ticker']}",
                    "body": (
                        f"{p['ticker']} ({p['allocation_pct']}%) represents nearly half your portfolio. "
                        "Consider trimming 5–10% and redeploying into other asset classes."
                    ),
                })

        # No fixed income
        fi_pct = alloc_map.get(_FIXED, 0)
        if fi_pct < 8:
            suggestions.append({
                "level": "WARNING",
                "title": "Minimal Fixed Income Exposure",
                "body": (
                    f"Fixed income is {fi_pct}% of your portfolio. "
                    "Adding AGG or BND can reduce drawdowns during equity sell-offs."
                ),
            })

        # Overweight EM
        em_pct = alloc_map.get(_EM, 0)
        if em_pct > 28:
            suggestions.append({
                "level": "WARNING",
                "title": "Elevated Emerging Market Allocation",
                "body": (
                    f"Emerging markets are {em_pct}% of your portfolio. "
                    "EM equities carry currency and political risk beyond standard equity vol."
                ),
            })

        # High beta
        beta = risk.get("portfolio_beta")
        if beta and beta > 1.15:
            suggestions.append({
                "level": "INFO",
                "title": "Portfolio Beta Above 1.0",
                "body": (
                    f"Your weighted portfolio beta is {beta:.2f}. "
                    "The portfolio will amplify both market gains and drawdowns relative to SPY."
                ),
            })

        # Low diversification
        if len(positions) < 5:
            suggestions.append({
                "level": "WARNING",
                "title": "Limited Position Diversification",
                "body": "Fewer than 5 holdings — consider broadening to reduce idiosyncratic risk.",
            })

        return suggestions[:5]   # cap at 5

    def _build_periods(
        self,
        positions: list[dict],
        histories: dict[str, list[dict]],
        change_pct_1d: float,
    ) -> dict:
        """Compute portfolio % return for 5d, 1m, 6m, 1y lookback."""
        today = date.today().isoformat()

        def _days_ago(n: int) -> str:
            return (date.today() - timedelta(days=n)).isoformat()

        def _port_value_at(target: str) -> float:
            total = 0.0
            for p in positions:
                bars = histories.get(p["_sym"], [])
                # find closest bar on or before target
                eligible = [b for b in bars if b.get("date", "") <= target and b.get("close")]
                px = eligible[-1]["close"] if eligible else (p["price"] or 0)
                total += p["_shares"] * px
            return total

        current_val = _port_value_at(today)

        def _perf(days: int) -> float | None:
            past_val = _port_value_at(_days_ago(days))
            if not past_val:
                return None
            return round((current_val - past_val) / past_val * 100, 1)

        return {
            "perf_1d": round(change_pct_1d, 1),
            "perf_5d": _perf(5),
            "perf_1m": _perf(30),
            "perf_6m": _perf(182),
            "perf_1y": _perf(365),
        }

    def _sentiment(self, change_pct: float, risk: dict) -> str:
        beta = risk.get("portfolio_beta") or 1.0
        if change_pct > 0.4:
            return "BULLISH"
        if change_pct < -0.4:
            return "BEARISH"
        if abs(change_pct) <= 0.1:
            return "NEUTRAL"
        return "UNCERTAIN"

    def _top_movers(self, positions: list[dict]) -> dict:
        sorted_pos = sorted(
            [p for p in positions if p.get("change_pct") is not None],
            key=lambda x: x["change_pct"],
        )
        losers  = sorted_pos[:2]
        gainers = sorted_pos[-2:][::-1]

        def _fmt(p: dict) -> dict:
            return {
                "ticker":     p["ticker"],
                "change_pct": p["change_pct"],
                "price":      p["price"],
                "w52_low":    p.get("w52_low"),
                "w52_high":   p.get("w52_high"),
            }

        return {
            "gainers": [_fmt(p) for p in gainers],
            "losers":  [_fmt(p) for p in losers],
        }

    def _build_earnings(self, raw: list[dict]) -> list[dict]:
        results = []
        seen = set()
        for e in raw:
            ticker = (e.get("ticker") or "").upper()
            if not ticker or ticker in seen:
                continue
            seen.add(ticker)
            dt_str = e.get("date", "")
            display_date = ""
            if dt_str:
                try:
                    dt = datetime.strptime(dt_str, "%Y-%m-%d")
                    display_date = dt.strftime("%b %-d")
                except Exception:
                    display_date = dt_str
            results.append({
                "ticker":    ticker,
                "company":   ticker,      # simplified; no profile fetch here
                "next_date": display_date,
                "est_eps":   e.get("eps_estimate"),
                "last_eps":  None,
                "wtd":       None,
            })
        return results[:8]

    def _build_tape(self, quotes: dict[str, dict]) -> list[dict]:
        tape = []
        for sym in TICKER_TAPE_SYMS:
            q = quotes.get(sym, {})
            price  = _sf(q.get("last"))
            chgpct = _sf(q.get("change_percentage"))
            if price is not None:
                tape.append({
                    "symbol":     sym,
                    "price":      _safe_round(price),
                    "change_pct": _safe_round(chgpct, 3),
                })
        return tape

    def _build_news(self, raw: list[dict], positions: list[dict]) -> list[dict]:
        """Build news ticker from Finnhub company_news items."""
        news = []
        seen_headlines: set[str] = set()

        # Sort by datetime descending (Finnhub uses Unix timestamps)
        def _ts(item):
            try: return int(item.get("datetime") or 0)
            except Exception: return 0

        for item in sorted(raw, key=_ts, reverse=True):
            sym   = (item.get("_sym") or "").upper()
            title = item.get("title", "")
            ts    = item.get("datetime")
            if not title or title in seen_headlines:
                continue
            seen_headlines.add(title)

            time_ago = ""
            if ts:
                try:
                    dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
                    delta = datetime.now(timezone.utc) - dt
                    mins = int(delta.total_seconds() / 60)
                    if mins < 60:
                        time_ago = f"{mins}m ago"
                    elif mins < 1440:
                        time_ago = f"{mins // 60}h ago"
                    else:
                        time_ago = f"{mins // 1440}d ago"
                except Exception:
                    time_ago = ""

            news.append({
                "symbol":   sym,
                "headline": title,
                "time_ago": time_ago,
            })
            if len(news) >= 8:
                break
        return news

    # ── Helpers ───────────────────────────────────────────────────────────

    def _load_holdings(self, portfolio_file: Path) -> list[dict]:
        # Try user-specific file first, then fall back to legacy file
        candidates = [portfolio_file, Path("data/portfolio_holdings.json")]
        for path in candidates:
            try:
                if not path.exists():
                    continue
                with open(path) as f:
                    data = json.load(f)
                holdings = data.get("holdings", []) if isinstance(data, dict) else []
                result = [
                    h for h in holdings
                    if isinstance(h, dict)
                    and h.get("ticker")
                    and float(h.get("shares", 0) or 0) > 0
                ]
                if result:
                    return result
            except Exception as e:
                print(f"[CAELYN] Holdings load error ({path}): {e}")
        return []

    def _empty(self) -> dict:
        return {
            "portfolio": {
                "value": 0, "change_today": 0, "change_pct_today": 0,
                "perf_1d": None, "perf_5d": None, "perf_1m": None,
                "perf_6m": None, "perf_1y": None,
                "total_return_pct": 0, "total_return_value": 0,
                "sentiment": "NEUTRAL", "market_status": _market_status_et(),
            },
            "positions_count": 0,
            "holdings": [], "performance_chart": [], "asset_allocation": [],
            "correlation_matrix": {"tickers": [], "values": []},
            "risk_metrics": {
                "weighted_volatility": None, "max_drawdown": None,
                "top_concentration": 0, "top_concentration_label": "",
                "portfolio_beta": None, "sharpe_ratio": None, "sortino_ratio": None,
            },
            "volatility": [], "risk_suggestions": [],
            "top_movers": {"gainers": [], "losers": []},
            "earnings_calendar": [], "ticker_tape": [], "news_ticker": [],
            "as_of": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
