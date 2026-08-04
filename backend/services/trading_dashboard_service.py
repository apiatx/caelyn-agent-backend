"""
Trading Dashboard Service — canonical computation, cache, and snapshot API.

Pure computation module:
  - compute_trading_dashboard() performs zero I/O
  - No provider requests, database reads/writes, or main.py imports

Service cache:
  - Single in-memory cache owned by this module
  - 720-second TTL
  - get_trading_dashboard_snapshot() provides read-only access

Import direction:
  main.py  →  trading_dashboard_service.py
  Never:    trading_dashboard_service.py  →  main.py
"""
from __future__ import annotations

import asyncio
import copy
import logging
import time as _time
from datetime import datetime as _dt, timezone as _tz, timedelta as _td
from typing import Any

_logger = logging.getLogger(__name__)


# ── Sector ETF mapping ───────────────────────────────────────────────────────

_SECTOR_ETF_MAP = {
    "Technology":               ("XLK",  "Technology"),
    "Healthcare":               ("XLV",  "Health Care"),
    "Financial Services":       ("XLF",  "Financials"),
    "Energy":                   ("XLE",  "Energy"),
    "Industrials":              ("XLI",  "Industrials"),
    "Consumer Defensive":       ("XLP",  "Cons Staples"),
    "Consumer Cyclical":        ("XLY",  "Cons Disc"),
    "Basic Materials":          ("XLB",  "Materials"),
    "Utilities":                ("XLU",  "Utilities"),
    "Real Estate":              ("XLRE", "Real Estate"),
    "Communication Services":   ("XLC",  "Comm Svcs"),
}

# ── Cache ─────────────────────────────────────────────────────────────────────

_DASHBOARD_TTL = 720  # 12 min — aligned with macro precompute loop interval
_cache: dict[str, dict] = {}


def _cache_key(mode: str) -> str:
    return f"trading_dashboard_{mode}"


def _defensive_copy(data: dict) -> dict:
    return copy.deepcopy(data)


# ── Score helpers ─────────────────────────────────────────────────────────────

def _vix_score(vix: float | None) -> float:
    if vix is None:
        return 50.0
    if vix <= 13:
        return 95.0
    if vix <= 15:
        return 85.0
    if vix <= 18:
        return 72.0
    if vix <= 20:
        return 58.0
    if vix <= 25:
        return 40.0
    if vix <= 30:
        return 22.0
    return 8.0


def _fg_score(fg: float | None) -> float:
    """Fear & Greed (0-100) -> pillar score. Contrarian: greed = risky."""
    if fg is None:
        return 50.0
    if fg >= 75:
        return 35.0
    if fg >= 60:
        return 65.0
    if fg >= 45:
        return 75.0
    if fg >= 30:
        return 65.0
    if fg >= 20:
        return 40.0
    return 20.0


def _spread_score(spread: float | None) -> float:
    """2s10s yield spread -> score. Negative = inverted = bad."""
    if spread is None:
        return 50.0
    if spread > 0.5:
        return 85.0
    if spread > 0:
        return 70.0
    if spread > -0.5:
        return 55.0
    if spread > -1.0:
        return 38.0
    return 20.0


def _hy_oas_score(hy_oas: float | None) -> float:
    """HY credit spread (OAS in %). Lower = healthier."""
    if hy_oas is None:
        return 60.0
    if hy_oas < 2.5:
        return 90.0
    if hy_oas < 3.5:
        return 75.0
    if hy_oas < 4.5:
        return 55.0
    if hy_oas < 6.0:
        return 35.0
    return 15.0


def _pct_from_high_score(pct: float | None) -> float:
    """% from 52-week high (negative = below high)."""
    if pct is None:
        return 60.0
    if pct >= -2:
        return 90.0
    if pct >= -5:
        return 78.0
    if pct >= -10:
        return 60.0
    if pct >= -15:
        return 42.0
    if pct >= -20:
        return 25.0
    return 10.0


def _change_pct_score(chg: float | None) -> float:
    """Daily change % -> momentum component."""
    if chg is None:
        return 50.0
    if chg > 1.5:
        return 85.0
    if chg > 0.5:
        return 75.0
    if chg > 0:
        return 62.0
    if chg > -0.5:
        return 48.0
    if chg > -1.5:
        return 30.0
    return 12.0


# ── Pure computation ──────────────────────────────────────────────────────────

def compute_trading_dashboard(
    mode: str,
    risk_data: dict,
    macro_data: dict,
    calendar_data: dict,
    sector_perf_raw: list | None = None,
    spy_qqq_extended: dict | None = None,
    vix_history: dict | None = None,
) -> dict:
    """Pure computation: produce the full Trading Dashboard response dict.

    Zero I/O. Zero provider calls. Zero cache access.
    Identical logic to the original _compute_dashboard in main.py.
    """

    def _s(d, *keys, default=None):
        for k in keys:
            if not isinstance(d, dict):
                return default
            d = d.get(k, None)
            if d is None:
                return default
        return d

    # ── Raw value helpers ──────────────────────────────────────────────────
    def _fg_comp(key):
        comps = _s(risk_data, "fear_greed", "components") or {}
        v = comps.get(key)
        if isinstance(v, dict):
            return v.get("score", 50)
        return 50

    def _fmt(v, suffix="", precision=2, signed=False):
        if v is None:
            return "N/A"
        fmt = f"{v:+.{precision}f}" if signed else f"{v:.{precision}f}"
        return f"{fmt}{suffix}"

    def _ma_signal(price, sma, threshold_pct=1.0):
        if price is None or sma is None or sma == 0:
            return None, "N/A"
        diff = ((price - sma) / sma) * 100
        if abs(diff) <= threshold_pct:
            return 0, f"At MA ({diff:+.1f}%)"
        elif diff > 0:
            return 1, f"Above ({diff:+.1f}%)"
        else:
            return -1, f"Below ({diff:+.1f}%)"

    def _next_event_label(ev_types: list[str], events: list) -> str:
        today = _dt.utcnow().date()
        for ev in events:
            ev_name = (ev.get("event", "") or ev.get("name", "") or "").lower()
            if any(k in ev_name for k in ev_types):
                ev_date_str = ev.get("date", "")
                if not ev_date_str:
                    continue
                try:
                    ev_date = _dt.strptime(ev_date_str[:10], "%Y-%m-%d").date()
                except Exception:
                    continue
                if ev_date < today:
                    continue
                delta = (ev_date - today).days
                if delta == 0:
                    return "TODAY"
                elif delta == 1:
                    return "Tomorrow"
                else:
                    return ev_date.strftime("%b %-d")
        return "N/A"

    # ── Core risk data ─────────────────────────────────────────────────────
    vix = _s(risk_data, "volatility", "vix")
    vix_change = _s(risk_data, "volatility", "vix_change")
    vix_signal = _s(risk_data, "volatility", "signal", default="normal")
    fg_score_raw = _s(risk_data, "fear_greed", "score")
    fg_rating = _s(risk_data, "fear_greed", "rating", default="Neutral")
    hy_oas = _s(risk_data, "credit_spreads", "hy_oas")
    bbb_oas = _s(risk_data, "credit_spreads", "bbb_oas")
    hy_signal = _s(risk_data, "credit_spreads", "hy_signal", default="normal")
    spread_2s10s = _s(risk_data, "yield_curve_risk", "spread_2s10s")
    curve_inverted = _s(risk_data, "yield_curve_risk", "inverted", default=False)
    dxy = _s(risk_data, "dollar", "dxy")
    dxy_chg = _s(risk_data, "dollar", "dxy_change_pct")

    benchmark_etfs = macro_data.get("benchmark_etfs", []) if isinstance(macro_data, dict) else []
    etf_map = {e.get("ticker"): e for e in benchmark_etfs if isinstance(e, dict)}
    spy_bench = etf_map.get("SPY", {})
    qqq_bench = etf_map.get("QQQ", {})
    spy_chg = spy_bench.get("change_pct") or spy_bench.get("change") or 0.0
    qqq_chg = qqq_bench.get("change_pct") or qqq_bench.get("change") or 0.0
    spy_from_high = spy_bench.get("pct_from_52w_high")
    qqq_from_high = qqq_bench.get("pct_from_52w_high")
    us10y = _s(macro_data, "rates_and_yields", "us_10y")

    # ── VIX enrichment ─────────────────────────────────────────────────────
    if vix_change is not None:
        vix_trend = "Rising" if vix_change > 0.3 else ("Falling" if vix_change < -0.3 else "Stable")
    else:
        vix_trend = "Stable"

    vix_pctile_str = "N/A"
    if vix_history and vix:
        try:
            hist_vals = [d.get("value") for d in (vix_history.get("data") or []) if d.get("value") is not None]
            if hist_vals:
                rank = sum(1 for v in hist_vals if v < vix) / len(hist_vals) * 100
                ordinal = "st" if int(rank) % 10 == 1 and int(rank) != 11 else \
                          "nd" if int(rank) % 10 == 2 and int(rank) != 12 else \
                          "rd" if int(rank) % 10 == 3 and int(rank) != 13 else "th"
                vix_pctile_str = f"{round(rank)}{ordinal} %ile"
        except Exception:
            pass

    # ── Put/Call ratio (approximated from Fear & Greed component) ──────────
    pc_comp = _fg_comp("put_call_options")
    pc_ratio = round(max(0.5, min(2.0, 1.5 - (pc_comp / 100))), 2)
    pc_status = "Elevated" if pc_ratio > 1.15 else ("Low" if pc_ratio < 0.8 else "Neutral")

    # ── SPY/QQQ vs Moving Averages ─────────────────────────────────────────
    ext = spy_qqq_extended or {}
    spy_ext = ext.get("SPY", {})
    qqq_ext = ext.get("QQQ", {})

    spy_price = spy_bench.get("price") or spy_ext.get("price")
    spy_sma50 = spy_ext.get("priceAvg50")
    spy_sma200 = spy_ext.get("priceAvg200")
    qqq_price = qqq_bench.get("price") or qqq_ext.get("price")
    qqq_sma50 = qqq_ext.get("priceAvg50")
    qqq_sma200 = qqq_ext.get("priceAvg200")

    spy_vs50_dir, spy_vs50_str = _ma_signal(spy_price, spy_sma50)
    spy_vs200_dir, spy_vs200_str = _ma_signal(spy_price, spy_sma200)
    qqq_vs50_dir, qqq_vs50_str = _ma_signal(qqq_price, qqq_sma50)
    qqq_vs200_dir, qqq_vs200_str = _ma_signal(qqq_price, qqq_sma200)

    spx_regime = "Uptrend" if (spy_vs200_dir or 0) > 0 and (spy_vs50_dir or 0) > 0 else \
                 "Downtrend" if (spy_vs200_dir or 0) < 0 else "Mixed"

    # ── Sector performance ─────────────────────────────────────────────────
    sector_list = []
    for item in (sector_perf_raw or []):
        sector_name = item.get("sector", "")
        if sector_name in _SECTOR_ETF_MAP:
            ticker, display_name = _SECTOR_ETF_MAP[sector_name]
            chg = item.get("changesPercentage", 0)
            if isinstance(chg, str):
                try:
                    chg = float(chg.strip("%").strip())
                except Exception:
                    chg = 0.0
            sector_list.append({
                "ticker": ticker,
                "name": display_name,
                "change_pct": round(float(chg), 2),
            })
    sector_list.sort(key=lambda x: x["change_pct"], reverse=True)
    sectors_positive = sum(1 for s in sector_list if s["change_pct"] > 0)
    sectors_total = len(sector_list)
    sector_leader = sector_list[0] if sector_list else None
    sector_laggard = sector_list[-1] if sector_list else None
    participation_pct = round(sectors_positive / sectors_total * 100) if sectors_total else 0

    # ── Calendar: FOMC + CPI next dates ────────────────────────────────────
    upcoming_events = calendar_data.get("events", []) if isinstance(calendar_data, dict) else []
    fomc_next = _next_event_label(["fomc", "federal funds", "interest rate decision"], upcoming_events)
    cpi_next = _next_event_label(["cpi", "consumer price"], upcoming_events)

    # ── Pillar score computation ────────────────────────────────────────────
    vix_s = _vix_score(vix)
    hy_s = _hy_oas_score(hy_oas)
    fg_vol_s = _fg_score(fg_score_raw)
    p1_score = round(vix_s * 0.5 + hy_s * 0.3 + fg_vol_s * 0.2, 1)
    p1_dir = "up" if vix_s >= 70 else ("down" if vix_s < 40 else "sideways")

    spy_high_s = _pct_from_high_score(spy_from_high)
    qqq_high_s = _pct_from_high_score(qqq_from_high)
    spy_chg_s = _change_pct_score(spy_chg)
    ma_bonus = 10.0 if (spy_vs50_dir or 0) > 0 and (spy_vs200_dir or 0) > 0 else \
               -10.0 if (spy_vs200_dir or 0) < 0 else 0.0
    p2_score = round(min(100.0, max(0.0, spy_high_s * 0.35 + qqq_high_s * 0.30 + spy_chg_s * 0.20 + 50.0 * 0.15 + ma_bonus)), 1)
    p2_dir = "up" if spy_chg > 0.2 else ("down" if spy_chg < -0.2 else "sideways")

    breadth_fg = _fg_comp("stock_price_breadth")
    strength_fg = _fg_comp("stock_price_strength")
    safe_haven = _fg_comp("safe_haven_demand")
    participation_bonus = ((sectors_positive - sectors_total / 2) / max(sectors_total, 1)) * 20 if sectors_total else 0
    p3_score = round(min(100.0, max(0.0, breadth_fg * 0.35 + strength_fg * 0.35 + (100 - safe_haven) * 0.20 + 50 * 0.10 + participation_bonus)), 1)
    p3_dir = "up" if p3_score >= 60 else ("down" if p3_score < 40 else "sideways")

    spread_s = _spread_score(spread_2s10s)
    dxy_s = 70.0 if (dxy_chg or 0) < 0 else (55.0 if abs(dxy_chg or 0) < 0.3 else 40.0)
    bbb_s = _hy_oas_score((bbb_oas or 0) * 2 if bbb_oas else None)
    p4_score = round(spread_s * 0.4 + bbb_s * 0.3 + dxy_s * 0.3, 1)
    p4_dir = "up" if p4_score >= 60 else ("down" if p4_score < 40 else "sideways")

    momentum_fg = _fg_comp("market_momentum_sp500")
    junk_bond = _fg_comp("junk_bond_demand")
    put_call_fg = _fg_comp("put_call_options")
    p5_score = round(momentum_fg * 0.4 + junk_bond * 0.3 + put_call_fg * 0.3, 1)
    p5_dir = "up" if p5_score >= 60 else ("down" if p5_score < 40 else "sideways")

    weights = [0.30, 0.25, 0.20, 0.15, 0.10] if mode == "swing" else [0.25, 0.20, 0.20, 0.15, 0.20]
    pillar_scores = [p1_score, p2_score, p3_score, p4_score, p5_score]
    mqs = round(sum(s * w for s, w in zip(pillar_scores, weights)), 1)

    # ── EWS + calendar penalties ───────────────────────────────────────────
    ews_penalty = 0
    alert_events = []
    for ev in upcoming_events[:20]:
        days_out = ev.get("days_out", 99)
        ev_type = (ev.get("event", "") or ev.get("name", "") or "").lower()
        if days_out is not None and days_out <= 1:
            if any(k in ev_type for k in ["fomc", "fed", "interest rate"]):
                ews_penalty += 15
                alert_events.append(f"FOMC/Fed Decision in {days_out}d — elevated volatility expected")
            elif "cpi" in ev_type or "inflation" in ev_type:
                ews_penalty += 10
                alert_events.append(f"CPI Release in {days_out}d — price action may spike")
            elif any(k in ev_type for k in ["nfp", "payroll", "jobs"]):
                ews_penalty += 10
                alert_events.append(f"Jobs Report in {days_out}d — directional risk elevated")

    _mqs_adj = max(0.0, round(mqs - ews_penalty, 1))
    decision = "YES" if _mqs_adj >= 70 else ("CAUTION" if _mqs_adj >= 40 else "NO")

    # ── Pillar metric blocks ───────────────────────────────────────────────
    vix_str = _fmt(vix, precision=1)
    fg_str = f"{fg_score_raw:.0f} ({fg_rating})" if fg_score_raw else "N/A"
    hy_str = _fmt(hy_oas, suffix="%")
    spread_str = _fmt(spread_2s10s, suffix="%", signed=True)
    spy_str = _fmt(spy_chg, suffix="%", signed=True)
    qqq_str = _fmt(qqq_chg, suffix="%", signed=True)
    dxy_str = _fmt(dxy, precision=2)
    dxy_chg_str = _fmt(dxy_chg, suffix="%", signed=True)

    pillars = [
        {
            "title": "VOLATILITY / RISK",
            "score": p1_score,
            "weight": int(weights[0] * 100),
            "direction": p1_dir,
            "metrics": [
                {"label": "VIX Level", "value": vix_str, "status": vix_signal.replace("_", " ").title(),
                 "ok": (vix or 99) < 20},
                {"label": "VIX Trend", "value": vix_trend,
                 "status": "Improving" if vix_trend == "Falling" else ("Worsening" if vix_trend == "Rising" else "Neutral"),
                 "ok": vix_trend == "Falling"},
                {"label": "VIX 1Y %ile", "value": vix_pctile_str,
                 "status": "Elevated" if "7" in vix_pctile_str or "8" in vix_pctile_str or "9" in vix_pctile_str else "Normal",
                 "ok": vix_pctile_str not in ("N/A",) and not vix_pctile_str.startswith(("7", "8", "9"))},
                {"label": "Put/Call Ratio", "value": f"{pc_ratio:.2f}", "status": pc_status,
                 "ok": pc_ratio <= 1.0},
                {"label": "HY OAS", "value": hy_str,
                 "status": hy_signal.upper(),
                 "ok": hy_s >= 60},
                {"label": "Fear & Greed", "value": fg_str,
                 "status": fg_rating,
                 "ok": 30 <= (fg_score_raw or 50) <= 70},
            ],
        },
        {
            "title": "TREND & STRUCTURE",
            "score": p2_score,
            "weight": int(weights[1] * 100),
            "direction": p2_dir,
            "metrics": [
                {"label": "SPX vs 50d MA", "value": spy_vs50_str,
                 "status": "N/A" if spy_vs50_dir is None else ("Above" if spy_vs50_dir > 0 else ("At" if spy_vs50_dir == 0 else "Below")),
                 "ok": spy_vs50_dir is None or spy_vs50_dir >= 0},
                {"label": "SPX vs 200d MA", "value": spy_vs200_str,
                 "status": "N/A" if spy_vs200_dir is None else ("Above" if spy_vs200_dir > 0 else ("At" if spy_vs200_dir == 0 else "Below")),
                 "ok": spy_vs200_dir is None or spy_vs200_dir >= 0},
                {"label": "Market Regime", "value": spx_regime,
                 "status": spx_regime,
                 "ok": spx_regime == "Uptrend"},
                {"label": "QQQ vs 50d MA", "value": qqq_vs50_str,
                 "status": "N/A" if qqq_vs50_dir is None else ("Above" if qqq_vs50_dir > 0 else ("At" if qqq_vs50_dir == 0 else "Below")),
                 "ok": qqq_vs50_dir is None or qqq_vs50_dir >= 0},
                {"label": "QQQ vs 200d MA", "value": qqq_vs200_str,
                 "status": "N/A" if qqq_vs200_dir is None else ("Above" if qqq_vs200_dir > 0 else ("At" if qqq_vs200_dir == 0 else "Below")),
                 "ok": qqq_vs200_dir is None or qqq_vs200_dir >= 0},
                {"label": "SPY vs 52w High", "value": _fmt(spy_from_high, suffix="%", signed=True),
                 "status": "Healthy" if (spy_from_high or -100) >= -5 else "Extended",
                 "ok": (spy_from_high or -100) >= -5},
            ],
        },
        {
            "title": "MARKET BREADTH",
            "score": p3_score,
            "weight": int(weights[2] * 100),
            "direction": p3_dir,
            "metrics": [
                {"label": "Price Breadth", "value": f"{breadth_fg:.0f}/100",
                 "status": "Positive" if breadth_fg >= 60 else ("Neutral" if breadth_fg >= 40 else "Weak"),
                 "ok": breadth_fg >= 50},
                {"label": "Price Strength", "value": f"{strength_fg:.0f}/100",
                 "status": "Strong" if strength_fg >= 60 else ("Neutral" if strength_fg >= 40 else "Weak"),
                 "ok": strength_fg >= 50},
                {"label": "Sectors Positive", "value": f"{sectors_positive}/{sectors_total}",
                 "status": f"{participation_pct}% Participation",
                 "ok": participation_pct >= 55},
                {"label": "Participation", "value": f"{participation_pct}%",
                 "status": "Broad" if participation_pct >= 70 else ("Mixed" if participation_pct >= 40 else "Narrow"),
                 "ok": participation_pct >= 55},
                {"label": "Safe Haven Dem", "value": f"{safe_haven:.0f}/100",
                 "status": "Elevated" if safe_haven >= 60 else ("Low" if safe_haven < 40 else "Normal"),
                 "ok": safe_haven < 50},
                {"label": "HYG Signal", "value": hy_signal.upper(),
                 "status": hy_signal.title(),
                 "ok": hy_signal == "normal"},
            ],
        },
        {
            "title": "MACRO / LIQUIDITY",
            "score": p4_score,
            "weight": int(weights[3] * 100),
            "direction": p4_dir,
            "metrics": [
                {"label": "10Y Yield", "value": _fmt(us10y, suffix="%") if us10y else "N/A",
                 "status": "Elevated" if (us10y or 0) >= 4.8 else ("Rising" if (us10y or 0) >= 4.5 else "Moderate"),
                 "ok": bool(us10y and us10y < 4.8)},
                {"label": "DXY", "value": f"{dxy_str} ({dxy_chg_str})",
                 "status": "Falling" if (dxy_chg or 0) < -0.1 else ("Rising" if (dxy_chg or 0) > 0.1 else "Stable"),
                 "ok": (dxy_chg or 0) < 0.3},
                {"label": "2s10s Spread", "value": spread_str,
                 "status": "INVERTED" if curve_inverted else "NORMAL",
                 "ok": (spread_2s10s or -1) > -0.5},
                {"label": "Next FOMC", "value": fomc_next,
                 "status": "Today" if fomc_next == "TODAY" else ("Soon" if fomc_next not in ("N/A",) else "Distant"),
                 "ok": fomc_next not in ("TODAY", "Tomorrow")},
                {"label": "Fed Stance", "value": "Easing" if (us10y or 5) < 4.5 else "Restrictive",
                 "status": "Easing" if (us10y or 5) < 4.5 else "Restrictive",
                 "ok": bool(us10y and us10y < 4.8)},
                {"label": "Next CPI", "value": cpi_next,
                 "status": "Today" if cpi_next == "TODAY" else "Upcoming",
                 "ok": cpi_next not in ("TODAY", "Tomorrow")},
            ],
        },
        {
            "title": "MOMENTUM / SENTIMENT",
            "score": p5_score,
            "weight": int(weights[4] * 100),
            "direction": p5_dir,
            "metrics": [
                {"label": "Mkt Momentum", "value": f"{momentum_fg:.0f}/100",
                 "status": "Positive" if momentum_fg >= 55 else ("Neutral" if momentum_fg >= 45 else "Negative"),
                 "ok": momentum_fg >= 50},
                {"label": "Put/Call Ratio", "value": f"{pc_ratio:.2f}",
                 "status": pc_status,
                 "ok": pc_ratio <= 1.05},
                {"label": "Junk Bond Dem", "value": f"{junk_bond:.0f}/100",
                 "status": "High" if junk_bond >= 60 else ("Low" if junk_bond < 40 else "Normal"),
                 "ok": junk_bond >= 50},
                {"label": "Sector Leader", "value": sector_leader["ticker"] if sector_leader else "N/A",
                 "status": f"{sector_leader['change_pct']:+.2f}%" if sector_leader else "N/A",
                 "ok": bool(sector_leader and sector_leader["change_pct"] > 0)},
                {"label": "Sector Laggard", "value": sector_laggard["ticker"] if sector_laggard else "N/A",
                 "status": f"{sector_laggard['change_pct']:+.2f}%" if sector_laggard else "N/A",
                 "ok": bool(sector_laggard and sector_laggard["change_pct"] > -1.5)},
                {"label": "DXY Trend", "value": "FALLING" if (dxy_chg or 0) < 0 else "RISING",
                 "status": "Bullish" if (dxy_chg or 0) < 0 else "Headwind",
                 "ok": (dxy_chg or 0) < 0},
            ],
        },
    ]

    # ── Execution condition 1: Breakouts working? ──────────────────────────
    _breakout_ok = breadth_fg > 55
    _breakout_val = "Yes" if breadth_fg > 55 else ("Mixed" if breadth_fg >= 35 else "No")
    _breakout_status = "Working" if breadth_fg > 55 else ("Inconsistent" if breadth_fg >= 35 else "Failing")

    # ── Execution condition 2: Leaders holding? ────────────────────────────
    _leader_tickers = {"XLK", "XLY", "XLC"}
    _leader_chgs = [s["change_pct"] for s in sector_list if s["ticker"] in _leader_tickers]
    _spy_chg_today = spy_bench.get("change_pct") or 0.0
    if _leader_chgs:
        _leaders_vs_spy = (sum(_leader_chgs) / len(_leader_chgs)) - _spy_chg_today
    else:
        _leaders_vs_spy = None
    if _leaders_vs_spy is None:
        _leaders_ok = False
        _leaders_val = "N/A"
        _leaders_status = "No data"
    elif _leaders_vs_spy > 0.5:
        _leaders_ok = True
        _leaders_val = "Yes"
        _leaders_status = "Holding"
    elif _leaders_vs_spy >= -1.5:
        _leaders_ok = False
        _leaders_val = "Mixed"
        _leaders_status = "Fading"
    else:
        _leaders_ok = False
        _leaders_val = "No"
        _leaders_status = "Breaking down"

    # ── Execution condition 3: Pullbacks bought? ───────────────────────────
    _spy_bars = (spy_ext.get("recent_bars") or [])[-5:]
    _recovered_days = 0
    for _b in _spy_bars:
        _h, _l, _c = _b.get("high"), _b.get("low"), _b.get("close")
        if _h and _l and _c and (_h - _l) > 0:
            _rec = (_c - _l) / (_h - _l)
            if _rec > 0.50:
                _recovered_days += 1
    if _spy_bars:
        _pullback_ok = _recovered_days >= 3
        _pullback_val = "Yes" if _recovered_days >= 3 else ("Mixed" if _recovered_days == 2 else "No")
        _pullback_status = "Support" if _recovered_days >= 3 else ("Inconsistent" if _recovered_days == 2 else "Selling into rallies")
    else:
        _pullback_ok = False
        _pullback_val = "N/A"
        _pullback_status = "No data"

    # ── Execution condition 4: Follow-through? ─────────────────────────────
    _spy_bars10 = (spy_ext.get("recent_bars") or [])[-10:]
    _ft_ok = False
    _ft_val = "N/A"
    _ft_status = "No data"
    if len(_spy_bars10) >= 4:
        _closes10 = [_b["close"] for _b in _spy_bars10 if _b.get("close")]
        _vols10 = [_b.get("volume") or 0 for _b in _spy_bars10]
        _green_days, _ft_days = [], []
        _up_vols, _dn_vols = [], []
        for _i in range(1, len(_closes10)):
            _is_green = _closes10[_i] > _closes10[_i - 1]
            if _is_green:
                _green_days.append(_i)
                _up_vols.append(_vols10[_i])
                if _i + 1 < len(_closes10) and _closes10[_i + 1] > _closes10[_i]:
                    _ft_days.append(_i)
            else:
                _dn_vols.append(_vols10[_i])
        _ft_rate = len(_ft_days) / len(_green_days) if _green_days else 0
        _avg_up_vol = sum(_up_vols) / len(_up_vols) if _up_vols else 0
        _avg_dn_vol = sum(_dn_vols) / len(_dn_vols) if _dn_vols else 1
        _vol_ratio = _avg_up_vol / _avg_dn_vol if _avg_dn_vol else 1
        if _ft_rate > 0.5 and _vol_ratio > 1.2:
            _ft_ok = True
            _ft_val = "Strong"
            _ft_status = "Confirming"
        elif _ft_rate > 0.5:
            _ft_ok = False
            _ft_val = "Weak"
            _ft_status = "Low conviction"
        else:
            _ft_ok = False
            _ft_val = "No"
            _ft_status = "Reversing"

    exec_conditions = [
        {"label": "Breakouts working?", "value": _breakout_val, "status": _breakout_status, "ok": _breakout_ok},
        {"label": "Leaders holding?", "value": _leaders_val, "status": _leaders_status, "ok": _leaders_ok},
        {"label": "Pullbacks bought?", "value": _pullback_val, "status": _pullback_status, "ok": _pullback_ok},
        {"label": "Follow-through?", "value": _ft_val, "status": _ft_status, "ok": _ft_ok},
    ]

    ews = float(sum(25 for c in exec_conditions if c["ok"]))

    decision_text = {
        "YES": "Market conditions are favorable. Volatility is controlled, trend is intact, and breadth supports participation. Trade your plan with normal position sizing.",
        "CAUTION": "Mixed signals across pillars. Consider reducing position size by 30-50%, tightening stops, and avoiding aggressive new entries until conditions clarify.",
        "NO": "Risk environment is elevated. High VIX, poor breadth, or a major macro event is pending. Stay flat or reduce/hedge existing positions.",
    }[decision]

    terminal = [
        {"type": "dim", "text": f"$ caelyn --mode={mode} --analyze --pillars=5"},
        {"type": "dim", "text": ""},
        {"type": "green" if decision == "YES" else ("yellow" if decision == "CAUTION" else "red"),
         "text": f"DECISION: {decision}"},
        {"type": "dim", "text": decision_text},
        {"type": "dim", "text": ""},
        {"type": "blue", "text": f"[VOLATILITY/RISK]    {p1_score:.0f}/100 | VIX: {vix_str} ({vix_trend}) | 1Y%ile: {vix_pctile_str} | P/C: {pc_ratio:.2f} | HY OAS: {hy_str}"},
        {"type": "blue", "text": f"[TREND/STRUCTURE]    {p2_score:.0f}/100 | SPX vs 50d: {spy_vs50_str} | vs 200d: {spy_vs200_str} | Regime: {spx_regime}"},
        {"type": "blue", "text": f"[MARKET BREADTH]     {p3_score:.0f}/100 | {sectors_positive}/{sectors_total} sectors positive | Breadth: {breadth_fg:.0f} | Strength: {strength_fg:.0f}"},
        {"type": "blue", "text": f"[MACRO/LIQUIDITY]    {p4_score:.0f}/100 | 10Y: {_fmt(us10y, suffix='%') if us10y else 'N/A'} | DXY: {dxy_str} | 2s10s: {spread_str} | FOMC: {fomc_next}"},
        {"type": "blue", "text": f"[MOMENTUM/SENT]      {p5_score:.0f}/100 | Momentum: {momentum_fg:.0f} | Leader: {sector_leader['ticker']} ({sector_leader['change_pct']:+.2f}%)" if sector_leader else f"[MOMENTUM/SENT]      {p5_score:.0f}/100 | Momentum: {momentum_fg:.0f}"},
        {"type": "dim", "text": ""},
        {"type": "green" if mqs >= 70 else ("yellow" if mqs >= 40 else "red"),
         "text": f"Market Quality Score (MQS): {mqs:.0f}/100"},
        {"type": "green" if ews >= 70 else ("yellow" if ews >= 40 else "red"),
         "text": f"Execution Window Score (EWS): {ews:.0f}/100" + (f"  [event penalty: -{ews_penalty}]" if ews_penalty else "")},
    ]
    if alert_events:
        terminal.append({"type": "yellow", "text": f"ALERT: {alert_events[0]}"})

    return {
        "decision": decision,
        "market_quality_score": mqs,
        "execution_window_score": ews,
        "mode": mode,
        "pillars": pillars,
        "summary": decision_text,
        "execution_conditions": exec_conditions,
        "terminal_analysis": terminal,
        "alert": {
            "show": bool(alert_events),
            "text": alert_events[0] if alert_events else "",
        },
        "sector_performance": sector_list,
        "as_of": _dt.now(_tz.utc).isoformat(),
        "from_cache": False,
    }


# ── Canonical async getter (with cache) ──────────────────────────────────────

async def get_trading_dashboard(
    *,
    mode: str,
    force: bool = False,
    fetch_fresh_data=None,
) -> dict:
    """Return the Trading Dashboard from cache or build fresh.

    Parameters
    ----------
    mode : str
        "swing" or "day".
    force : bool
        If True, skip cache and rebuild immediately.
    fetch_fresh_data : async callable | None
        Called when a fresh build is needed.
        Must return a tuple:
          (risk_data, macro_data, calendar_data, sector_perf_raw,
           spy_qqq_extended, vix_history)

    Returns
    -------
    dict
        Full dashboard response with from_cache flag set.
    """
    mode = mode.lower() if mode.lower() in ("swing", "day") else "swing"
    key = _cache_key(mode)

    if not force:
        entry = _cache.get(key)
        if entry and (_time.time() - entry.get("_ts", 0)) < _DASHBOARD_TTL:
            result = _defensive_copy(entry)
            result.pop("_ts", None)
            result["from_cache"] = True
            return result

    if fetch_fresh_data is None:
        from_cache_entry = _cache.get(key)
        if from_cache_entry:
            result = _defensive_copy(from_cache_entry)
            result.pop("_ts", None)
            result["from_cache"] = True
            return result
        raise RuntimeError(
            "Trading Dashboard: no fetch_fresh_data callback and cache empty"
        )

    risk_data, macro_data, calendar_data, sector_perf_raw, spy_qqq_extended, vix_history = (
        await fetch_fresh_data()
    )

    result = compute_trading_dashboard(
        mode=mode,
        risk_data=risk_data,
        macro_data=macro_data,
        calendar_data=calendar_data,
        sector_perf_raw=sector_perf_raw,
        spy_qqq_extended=spy_qqq_extended,
        vix_history=vix_history,
    )

    _cache[key] = {**result, "_ts": _time.time()}
    result_copy = _defensive_copy(result)
    result_copy["from_cache"] = False
    return result_copy


# ── Cache management ─────────────────────────────────────────────────────────

def clear_dashboard_cache() -> list[str]:
    """Clear all trading dashboard cache entries. Returns list of cleared keys."""
    cleared = [k for k in list(_cache.keys()) if k.startswith("trading_dashboard_")]
    for k in cleared:
        del _cache[k]
    # Clear failure state as well
    _refresh_outcome.clear()
    _refresh_failure_count.clear()
    _refresh_last_attempt.clear()
    return cleared


# ── Snapshot API (read-only, zero I/O) ───────────────────────────────────────

def get_trading_dashboard_snapshot(
    mode: str = "swing",
    allow_expired: bool = True,
) -> dict | None:
    """Return a read-only snapshot of the cached Trading Dashboard.

    Zero provider calls. Zero cache refresh. Zero mutation.

    Returns
    -------
    dict or None
        Snapshot wrapper:
        {
          "dashboard": dict | None,
          "mode": str,
          "age_seconds": float | None,
          "expired": bool | None,
          "status": "available" | "expired" | "unavailable"
        }
        Returns None when allow_expired=False and the cached entry is expired.
    """
    mode = mode.lower() if mode.lower() in ("swing", "day") else "swing"
    key = _cache_key(mode)
    entry = _cache.get(key)

    if entry is None:
        return {
            "dashboard": None,
            "mode": mode,
            "age_seconds": None,
            "expired": None,
            "status": "unavailable",
            "refresh_state": _refresh_state(mode),
            "refresh_failure_count": _refresh_failure_count.get(mode, 0),
        }

    age = _time.time() - entry.get("_ts", _time.time())
    expired = age >= _DASHBOARD_TTL

    if expired and not allow_expired:
        return None

    dashboard_copy = _defensive_copy(entry)
    dashboard_copy.pop("_ts", None)
    dashboard_copy["from_cache"] = True

    return {
        "dashboard": dashboard_copy,
        "mode": mode,
        "age_seconds": round(age, 1),
        "expired": expired,
        "status": "available" if not expired else "expired",
        "refresh_state": _refresh_state(mode),
        "refresh_failure_count": _refresh_failure_count.get(mode, 0),
    }


# ── Singleflight background refresh ───────────────────────────────────────────

# In-flight registry per mode: True when a background task is active
_inflight: dict[str, asyncio.Task | None] = {}
# Last refresh outcome per mode
_refresh_outcome: dict[str, str] = {}  # "succeeded" | "failed"
_refresh_failure_count: dict[str, int] = {}
_refresh_last_attempt: dict[str, float] = {}
_FAILURE_BACKOFF = 30  # seconds before retrying after a failed refresh


def _refresh_state(mode: str) -> str:
    """Return the current refresh lifecycle state for *mode*: idle | running | succeeded | failed."""
    task = _inflight.get(mode)
    if task is not None and not task.done():
        return "running"
    outcome = _refresh_outcome.get(mode)
    if outcome == "succeeded":
        return "succeeded"
    if outcome == "failed":
        return "failed"
    return "idle"


def schedule_trading_dashboard_refresh(
    *,
    mode: str,
    fetch_fresh_data=None,
) -> dict:
    """Schedule a nonblocking canonical refresh of the Trading Dashboard.

    Parameters
    ----------
    mode : str
        "swing" or "day".
    fetch_fresh_data : async callable
        Same callback used by get_trading_dashboard().

    Returns
    -------
    dict
        {"status": "not_needed" | "scheduled" | "already_running" | "backoff", "mode": str}
    """
    mode = mode.lower() if mode.lower() in ("swing", "day") else "swing"

    # Check if cache is already fresh
    key = _cache_key(mode)
    entry = _cache.get(key)
    if entry and (_time.time() - entry.get("_ts", 0)) < _DASHBOARD_TTL:
        return {"status": "not_needed", "mode": mode}

    # Check if already in flight
    task = _inflight.get(mode)
    if task is not None and not task.done():
        return {"status": "already_running", "mode": mode}

    if fetch_fresh_data is None:
        return {"status": "not_needed", "mode": mode}

    # Backoff guard after a failed refresh
    if _refresh_outcome.get(mode) == "failed":
        last_attempt = _refresh_last_attempt.get(mode, 0)
        if _time.time() - last_attempt < _FAILURE_BACKOFF:
            return {"status": "backoff", "mode": mode}
        # Reset failure counter so next attempt can proceed
        _refresh_outcome.pop(mode, None)

    # Create background task
    async def _refresh():
        try:
            result = await get_trading_dashboard(
                mode=mode,
                force=True,
                fetch_fresh_data=fetch_fresh_data,
            )
            _refresh_outcome[mode] = "succeeded"
            _refresh_failure_count.pop(mode, None)
            return result
        except Exception as exc:
            _refresh_outcome[mode] = "failed"
            _refresh_failure_count[mode] = _refresh_failure_count.get(mode, 0) + 1
            _logger.warning(
                "[TRADING_DASHBOARD] background refresh failed (mode=%s): %s",
                mode, exc,
            )
            raise
        finally:
            _inflight.pop(mode, None)
            _refresh_last_attempt[mode] = _time.time()

    _inflight[mode] = asyncio.ensure_future(_refresh())
    _refresh_outcome.pop(mode, None)  # clear previous outcome while running
    return {"status": "scheduled", "mode": mode}


def _clear_inflight():
    """Remove completed tasks from the in-flight registry (used in tests)."""
    for mode in list(_inflight.keys()):
        task = _inflight.get(mode)
        if task is not None and task.done():
            _inflight.pop(mode, None)
