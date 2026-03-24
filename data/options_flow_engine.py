from __future__ import annotations

import asyncio
import math
import os
from collections import defaultdict
from datetime import date, datetime
from typing import Any

try:
    from langsmith import traceable
except ImportError:
    def traceable(*args, **kwargs):
        def _noop(fn):
            return fn
        if args and callable(args[0]):
            return args[0]
        return _noop

from data.options_history_store import (
    get_contract_flow_history_summary,
    store_options_flow_snapshots,
    get_latest_technicals,
)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except Exception:
        return default


OPTIONS_FLOW_DEFAULTS = {
    "prefilter_target": _env_int("OPTIONS_FLOW_PREFILTER_TARGET", 24),
    "options_inspection_limit": _env_int("OPTIONS_FLOW_INSPECTION_LIMIT", 15),
    "min_stock_price": _env_float("OPTIONS_FLOW_MIN_STOCK_PRICE", 8.0),
    "min_stock_liquidity": _env_float("OPTIONS_FLOW_MIN_STOCK_LIQUIDITY", 15_000_000.0),
    # Market-cap ranges are hardcoded per tier — NOT user-editable.
    # See TIER_MCAP_RANGES below for the authoritative gate values.
    "relative_volume_threshold": _env_float("OPTIONS_FLOW_RELATIVE_VOLUME_THRESHOLD", 1.5),
    "min_dte": _env_int("OPTIONS_FLOW_MIN_DTE", 7),
    "max_dte": _env_int("OPTIONS_FLOW_MAX_DTE", 45),
    "max_expirations_per_ticker": _env_int("OPTIONS_FLOW_MAX_EXPIRATIONS", 2),
    "max_spread_pct": _env_float("OPTIONS_FLOW_MAX_SPREAD_PCT", 18.0),
    "min_contract_volume": _env_int("OPTIONS_FLOW_MIN_CONTRACT_VOLUME", 10),
    "min_open_interest": _env_int("OPTIONS_FLOW_MIN_OPEN_INTEREST", 25),
    "preferred_delta_min": _env_float("OPTIONS_FLOW_PREFERRED_DELTA_MIN", 0.2),
    "preferred_delta_max": _env_float("OPTIONS_FLOW_PREFERRED_DELTA_MAX", 0.6),
    "min_premium_traded_estimate": _env_float("OPTIONS_FLOW_MIN_PREMIUM_TRADED", 5_000.0),
    "max_moneyness_pct": _env_float("OPTIONS_FLOW_MAX_MONEYNESS_PCT", 0.15),
    "max_contracts_per_ticker": _env_int("OPTIONS_FLOW_MAX_CONTRACTS_PER_TICKER", 60),
    "top_contracts_per_ticker": _env_int("OPTIONS_FLOW_TOP_CONTRACTS_PER_TICKER", 5),
}


OPTIONS_FLOW_WEIGHTS = {
    "flow_score": _env_float("OPTIONS_FLOW_WEIGHT_FLOW", 0.25),
    "gamma_score": _env_float("OPTIONS_FLOW_WEIGHT_GAMMA", 0.20),
    "asymmetry_score": _env_float("OPTIONS_FLOW_WEIGHT_ASYMMETRY", 0.20),
    "volatility_score": _env_float("OPTIONS_FLOW_WEIGHT_VOLATILITY", 0.15),
    "sentiment_score": _env_float("OPTIONS_FLOW_WEIGHT_SENTIMENT", 0.10),
    "stock_context_score": _env_float("OPTIONS_FLOW_WEIGHT_STOCK_CONTEXT", 0.10),
}


# ── Hardcoded market-cap tiers (not user-editable) ────────────────────────
# Each tab has a fixed min/max mcap range.  Scan-defaults only control
# options-level screening params (OI, volume, spread, DTE, etc.).
TIER_MCAP_RANGES: dict[str, tuple[float, float | None]] = {
    "megacap":   (1_000_000_000_000.0, None),               # $1 T+
    "large_cap": (100_000_000_000.0, 999_999_999_999.0),    # $100 B – $999 B
    "small_cap": (500_000_000.0, 99_999_999_999.0),         # $500 M – $99 B
    "etf":       (0, None),                                  # No mcap filter — ETFs only
}


ETF_SET = {
    # ── Index ETFs ────────────────────────────────────────────────────────
    "SPY", "QQQ", "IWM", "DIA", "VTI", "VOO", "IVV", "RSP",
    "MDY", "IJH", "IJR", "VB", "VTV", "VUG", "IWF", "IWD",
    # ── Sector ETFs ───────────────────────────────────────────────────────
    "XLF", "XLK", "XLE", "XLV", "XLC", "XLI", "XLB", "XLP",
    "XLU", "XLRE", "XBI", "XHB", "XOP", "XRT",
    # ── Semis ─────────────────────────────────────────────────────────────
    "SMH", "SOXX", "SOXL", "SOXS",
    # ── Fixed income / commodities ────────────────────────────────────────
    "TLT", "TBT", "SHY", "IEF", "LQD", "HYG", "JNK", "AGG", "BND",
    "GLD", "GDX", "GDXJ", "SLV", "USO", "UNG",
    # ── Thematic / specialty ──────────────────────────────────────────────
    "ARKK", "ARKG", "ARKW", "ARKF",
    "EEM", "EFA", "FXI", "EWZ", "MCHI", "KWEB",
    "IBIT", "BITO", "GBTC",
    "TAN", "LIT", "JETS", "HACK", "BOTZ",
    "KRE", "OIH", "IBB",
    "SMCX", "SMLF", "SCHA",
    # ── Volatility / leveraged ────────────────────────────────────────────
    "VXX", "UVXY", "SVXY",
    "TQQQ", "SQQQ", "SPXL", "SPXS", "UPRO", "SH", "SDS",
    "LABU", "LABD", "FNGU", "FNGD",
}


def _clip(value: float | None, lo: float = 0.0, hi: float = 100.0) -> float:
    if value is None or isinstance(value, bool):
        return lo
    return max(lo, min(hi, float(value)))


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, "", "-"):
            return None
        return float(value)
    except Exception:
        return None


def _safe_int(value: Any) -> int:
    try:
        if value in (None, "", "-"):
            return 0
        return int(float(value))
    except Exception:
        return 0


def _parse_percent(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace("%", "").replace(",", "")
    try:
        return float(text)
    except Exception:
        return None


def _parse_money_string(value: Any) -> float | None:
    if value in (None, "", "-"):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().upper().replace("$", "").replace(",", "")
    mult = 1.0
    if text.endswith("B"):
        mult = 1_000_000_000.0
        text = text[:-1]
    elif text.endswith("M"):
        mult = 1_000_000.0
        text = text[:-1]
    elif text.endswith("K"):
        mult = 1_000.0
        text = text[:-1]
    try:
        return float(text) * mult
    except Exception:
        return None


def _parse_price(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace("$", "").replace(",", "")
    try:
        return float(text)
    except Exception:
        return None


def _days_to_expiration(expiration: str | None) -> int | None:
    if not expiration:
        return None
    try:
        exp = datetime.strptime(expiration, "%Y-%m-%d").date()
        return (exp - date.today()).days
    except Exception:
        return None


def _midpoint(bid: float | None, ask: float | None, last: float | None) -> float | None:
    if bid is not None and ask is not None and bid > 0 and ask > 0 and ask >= bid:
        return round((bid + ask) / 2.0, 4)
    if last is not None and last > 0:
        return round(last, 4)
    if bid is not None and bid > 0:
        return round(bid, 4)
    if ask is not None and ask > 0:
        return round(ask, 4)
    return None


def _spread_pct(bid: float | None, ask: float | None, midpoint: float | None) -> float | None:
    if bid is None or ask is None or midpoint in (None, 0):
        return None
    spread = ask - bid
    if spread < 0:
        return None
    return round((spread / midpoint) * 100.0, 2)


def _normalize_technicals(stored: dict | None, fallback: dict | None) -> dict:
    result: dict[str, float | None] = {}
    if isinstance(fallback, dict):
        result.update(fallback)
    if isinstance(stored, dict):
        for key, value in stored.items():
            if isinstance(value, dict):
                result[key] = value.get("value")
            elif key != "ticker":
                result[key] = value
    if "rsi_14" in result and result.get("rsi") is None:
        result["rsi"] = result.get("rsi_14")
    return result


class OptionsFlowEngine:
    def __init__(self, data_service, overrides: dict | None = None):
        self.data = data_service
        self.defaults = dict(OPTIONS_FLOW_DEFAULTS)
        if overrides:
            for k, v in overrides.items():
                if k in self.defaults and v is not None:
                    self.defaults[k] = type(self.defaults[k])(v)
        self.weights = dict(OPTIONS_FLOW_WEIGHTS)

    @traceable(name="options_flow_engine.build_prefilter_snapshot")
    async def build_prefilter_snapshot(
        self,
        seed_tickers: list[str] | None = None,
        tab: str = "megacap",
        exclude_tickers: set[str] | None = None,
    ) -> dict:
        prefilter_data = await self._build_prefilter(seed_tickers or [], tab=tab, exclude_tickers=exclude_tickers)
        return {
            "generated_at": datetime.utcnow().isoformat(),
            "filter_defaults": self.defaults,
            "tab": tab,
            **prefilter_data,
        }

    @traceable(name="options_flow_engine.run_live_scan")
    async def run_live_scan(
        self,
        seed_tickers: list[str] | None = None,
        prefilter_snapshot: dict | None = None,
        tab: str = "megacap",
    ) -> dict:
        if prefilter_snapshot and isinstance(prefilter_snapshot, dict):
            candidates = list(prefilter_snapshot.get("candidates") or [])
            degraded_sources = list(prefilter_snapshot.get("degraded_sources") or [])
            macro = prefilter_snapshot.get("macro", {}) or {}
        else:
            prefilter_data = await self.build_prefilter_snapshot(seed_tickers or [], tab=tab)
            candidates = list(prefilter_data.get("candidates") or [])
            degraded_sources = list(prefilter_data.get("degraded_sources") or [])
            macro = prefilter_data.get("macro", {}) or {}

        inspectable = candidates[: self.defaults["options_inspection_limit"]]
        print(f"[OPTIONS_FLOW] [{tab}] Pipeline: {len(candidates)} prefilter → {len(inspectable)} inspectable")
        results = await self._inspect_shortlist(inspectable, macro, tab=tab)
        dropped_tickers = [inspectable[i].get("ticker") for i, r in enumerate(results) if r is None]
        results = [r for r in results if r]
        results.sort(key=lambda row: row.get("composite_score", 0), reverse=True)
        print(f"[OPTIONS_FLOW] [{tab}] Pipeline: {len(inspectable)} inspected → {len(results)} scored (dropped: {dropped_tickers})")

        snapshot_rows: list[dict] = []
        all_contracts: list[dict] = []
        for row in results:
            for contract in row.pop("ranked_contracts", row.get("top_contracts", [])):
                flat = {
                    **contract,
                    "underlying": row["ticker"],
                    "category": row.get("category"),
                    "primary_signal": row.get("primary_signal"),
                    "confidence": row.get("confidence"),
                    "composite_score": row.get("composite_score"),
                    "side": contract.get("type") or contract.get("side"),
                    "openInterest": contract.get("openInterest", contract.get("open_interest")),
                    "iv": contract.get("iv", contract.get("implied_volatility")),
                    "mid": contract.get("mid"),
                    "vol_oi_ratio": contract.get("vol_oi_ratio", contract.get("option_volume_to_oi_ratio")),
                }
                all_contracts.append(flat)
            snapshot_rows.extend(row.pop("snapshot_rows", []))

        all_contracts.sort(key=lambda x: x.get("contract_score", 0), reverse=True)
        history_rows = 0
        try:
            history_rows = store_options_flow_snapshots(snapshot_rows)
        except Exception as exc:
            degraded_sources.append(f"snapshot_store:{type(exc).__name__}")

        total_call_vol = sum(_safe_int(r.get("call_volume")) for r in results)
        total_put_vol = sum(_safe_int(r.get("put_volume")) for r in results)
        market_summary = {
            "tickers_prefiltered": len(candidates),
            "tickers_inspected": len(inspectable),
            "tickers_ranked": len(results),
            "total_call_volume": total_call_vol,
            "total_put_volume": total_put_vol,
            "market_pc_ratio": round(total_put_vol / total_call_vol, 3) if total_call_vol else None,
            "total_contracts": len(all_contracts),
            "most_active_ticker": results[0]["ticker"] if results else None,
            "macro_context": self._macro_context_summary(macro),
            "history_snapshots_written": history_rows,
            "history_metrics_live": history_rows > 0,
        }

        return {
            "display_type": "options_screener",
            "scan_type": "options_flow",
            "tab": tab,
            "filter_defaults": self.defaults,
            "score_weights": self.weights,
            "pipeline_stats": {
                "prefilter_candidate_count": len(candidates),
                "options_inspection_count": len(inspectable),
                "ranked_result_count": len(results),
                "degraded_sources": sorted(set(degraded_sources)),
                "history_snapshot_rows": history_rows,
            },
            "tickers": results,
            "all_contracts": all_contracts[:500],
            "market_summary": market_summary,
        }

    @traceable(name="options_flow_engine.run_scan")
    async def run_scan(
        self,
        seed_tickers: list[str] | None = None,
        prefilter_snapshot: dict | None = None,
        tab: str = "megacap",
    ) -> dict:
        return await self.run_live_scan(seed_tickers, prefilter_snapshot=prefilter_snapshot, tab=tab)

    async def _build_prefilter(self, seed_tickers: list[str], tab: str = "megacap", exclude_tickers: set[str] | None = None) -> dict:
        degraded_sources: list[str] = []

        if tab == "small_cap":
            # Signal-focused screens targeting $500M–$99B market cap
            finviz_tasks = {
                "midcap_unusual_volume": self.data.finviz.get_midcap_unusual_volume(),
                "midcap_breakouts": self.data.finviz.get_midcap_breakouts(),
                "midcap_momentum": self.data.finviz.get_midcap_momentum(),
                "midcap_high_short": self.data.finviz.get_midcap_high_short(),
                "growth_earnings_catalyst": self.data.finviz.get_growth_earnings_catalyst(),
                "midlarge_volume_breakout": self.data.finviz.get_midlarge_volume_breakout(),
                "volume_breakouts": self.data.finviz.get_volume_breakouts(),
                "stage2_breakouts": self.data.finviz.get_stage2_breakouts(),
                "revenue_growth_leaders": self.data.finviz.get_revenue_growth_leaders(),
                "earnings_growth_leaders": self.data.finviz.get_earnings_growth_leaders(),
            }
        elif tab == "large_cap":
            # Blend of market-flow + growth screens — $100B–$999B lands in both
            finviz_tasks = {
                "unusual_volume": self.data.finviz.get_unusual_volume(),
                "most_active": self.data.finviz.get_most_active(),
                "new_highs": self.data.finviz.get_new_highs(),
                "top_losers": self.data.finviz.get_top_losers(),
                "high_short_float": self.data.finviz.get_high_short_float(),
                "earnings_this_week": self.data.finviz.get_earnings_this_week(),
                "midlarge_volume_breakout": self.data.finviz.get_midlarge_volume_breakout(),
                "midcap_breakouts": self.data.finviz.get_midcap_breakouts(),
            }
        elif tab == "etf":
            # ETFs — broad market flow screens (same as megacap but filtered to ETFs only)
            finviz_tasks = {
                "unusual_volume": self.data.finviz.get_unusual_volume(),
                "most_active": self.data.finviz.get_most_active(),
            }
        else:
            # megacap ($1T+) — pure mega-cap market flow screens
            finviz_tasks = {
                "unusual_volume": self.data.finviz.get_unusual_volume(),
                "most_active": self.data.finviz.get_most_active(),
                "new_highs": self.data.finviz.get_new_highs(),
                "top_losers": self.data.finviz.get_top_losers(),
                "oversold": self.data.finviz.get_oversold_stocks(),
                "overbought": self.data.finviz.get_overbought_stocks(),
                "high_short_float": self.data.finviz.get_high_short_float(),
                "earnings_this_week": self.data.finviz.get_earnings_this_week(),
            }

        tasks = [*finviz_tasks.values()]
        labels = list(finviz_tasks.keys())

        if self.data.fmp:
            tasks.extend([
                self.data.fmp.get_stock_market_actives(),
                self.data.fmp.get_stock_market_gainers(),
                self.data.fmp.get_stock_market_losers(),
            ])
            labels.extend(["fmp_actives", "fmp_gainers", "fmp_losers"])

        tasks.extend([
            asyncio.to_thread(self.data.finnhub.get_upcoming_earnings),
            asyncio.to_thread(self.data.fred.get_quick_macro),
        ])
        labels.extend(["finnhub_earnings", "fred_macro"])

        raw_results = await asyncio.gather(*tasks, return_exceptions=True)
        source_map: dict[str, Any] = {}
        for label, result in zip(labels, raw_results):
            if isinstance(result, Exception):
                degraded_sources.append(label)
                source_map[label] = [] if label != "fred_macro" else {}
            else:
                source_map[label] = result

        macro = source_map.get("fred_macro", {}) or {}
        upcoming_earnings = source_map.get("finnhub_earnings", []) or []
        earnings_by_symbol = {
            (row.get("ticker") or row.get("symbol") or "").upper(): row
            for row in upcoming_earnings
            if isinstance(row, dict) and (row.get("ticker") or row.get("symbol"))
        }

        candidates: dict[str, dict] = {}

        def ensure(symbol: str) -> dict:
            sym = (symbol or "").upper().strip()
            if not sym:
                return {}
            row = candidates.setdefault(sym, {
                "ticker": sym,
                "source_score": 0.0,
                "source_hits": [],
                "reasons": set(),
                "price_hint": None,
                "change_hint": None,
                "short_squeeze_flag": False,
                "catalyst_hint": None,
            })
            return row

        def add_rows(rows: list[dict], label: str, weight: float, reason: str):
            for item in rows or []:
                if not isinstance(item, dict):
                    continue
                symbol = (item.get("ticker") or item.get("symbol") or "").upper().strip()
                if not symbol:
                    continue
                row = ensure(symbol)
                if not row:
                    continue
                row["source_score"] += weight
                row["source_hits"].append(label)
                row["reasons"].add(reason)
                if row.get("price_hint") is None:
                    row["price_hint"] = _parse_price(item.get("price"))
                if row.get("change_hint") is None:
                    row["change_hint"] = _parse_percent(item.get("change"))
                if label == "high_short_float":
                    row["short_squeeze_flag"] = True
                if "earnings" in label:
                    row["catalyst_hint"] = "earnings"

        if tab == "small_cap":
            add_rows(source_map.get("midcap_unusual_volume", []), "midcap_unusual_volume", 24, "mid-cap unusual volume")
            add_rows(source_map.get("midcap_breakouts", []), "midcap_breakouts", 20, "mid-cap breakout")
            add_rows(source_map.get("midcap_momentum", []), "midcap_momentum", 18, "mid-cap momentum")
            add_rows(source_map.get("midcap_high_short", []), "midcap_high_short", 16, "mid-cap short squeeze")
            add_rows(source_map.get("growth_earnings_catalyst", []), "growth_earnings_catalyst", 18, "growth earnings catalyst")
            add_rows(source_map.get("midlarge_volume_breakout", []), "midlarge_volume_breakout", 22, "institutional volume breakout")
            add_rows(source_map.get("volume_breakouts", []), "volume_breakouts", 16, "volume breakout")
            add_rows(source_map.get("stage2_breakouts", []), "stage2_breakouts", 18, "stage 2 breakout")
            add_rows(source_map.get("revenue_growth_leaders", []), "revenue_growth_leaders", 14, "revenue growth")
            add_rows(source_map.get("earnings_growth_leaders", []), "earnings_growth_leaders", 14, "earnings growth")
        elif tab == "large_cap":
            add_rows(source_map.get("unusual_volume", []), "unusual_volume", 22, "relative stock volume")
            add_rows(source_map.get("most_active", []), "most_active", 14, "stock liquidity")
            add_rows(source_map.get("new_highs", []), "new_highs", 16, "breakout setup")
            add_rows(source_map.get("top_losers", []), "top_losers", 10, "reversal watch")
            add_rows(source_map.get("high_short_float", []), "high_short_float", 14, "short squeeze context")
            add_rows(source_map.get("earnings_this_week", []), "earnings_this_week", 12, "earnings catalyst")
            add_rows(source_map.get("midlarge_volume_breakout", []), "midlarge_volume_breakout", 20, "institutional volume breakout")
            add_rows(source_map.get("midcap_breakouts", []), "midcap_breakouts", 16, "breakout setup")
        elif tab == "etf":
            # ETF tab — seed tickers are the primary source; Finviz just adds volume context
            add_rows(source_map.get("unusual_volume", []), "unusual_volume", 22, "relative volume")
            add_rows(source_map.get("most_active", []), "most_active", 14, "liquidity")
        else:
            # megacap ($1T+)
            add_rows(source_map.get("unusual_volume", []), "unusual_volume", 22, "relative stock volume")
            add_rows(source_map.get("most_active", []), "most_active", 14, "stock liquidity")
            add_rows(source_map.get("new_highs", []), "new_highs", 16, "breakout setup")
            add_rows(source_map.get("top_losers", []), "top_losers", 10, "reversal watch")
            add_rows(source_map.get("oversold", []), "oversold", 12, "oversold reversal")
            add_rows(source_map.get("overbought", []), "overbought", 10, "exhaustion watch")
            add_rows(source_map.get("high_short_float", []), "high_short_float", 14, "short squeeze context")
            add_rows(source_map.get("earnings_this_week", []), "earnings_this_week", 12, "earnings catalyst")

        # FMP sources apply to both tabs
        add_rows(source_map.get("fmp_actives", []), "fmp_actives", 12, "stock liquidity")
        add_rows(source_map.get("fmp_gainers", []), "fmp_gainers", 12, "momentum move")
        add_rows(source_map.get("fmp_losers", []), "fmp_losers", 8, "reversal watch")

        for symbol, item in earnings_by_symbol.items():
            row = ensure(symbol)
            row["source_score"] += 10
            row["source_hits"].append("finnhub_earnings")
            row["reasons"].add("earnings catalyst")
            row["catalyst_hint"] = "earnings"

        for seed in seed_tickers:
            row = ensure(seed)
            row["source_score"] += 5
            row["source_hits"].append("seed_watchlist")
            row["reasons"].add("watchlist inclusion")

        # Cross-tab exclusion: remove tickers that belong to the other tab's universe
        if exclude_tickers:
            for sym in list(candidates.keys()):
                if sym in exclude_tickers:
                    del candidates[sym]

        # ── Tab-specific candidate filtering (BEFORE preliminary sort/cut) ────
        # This ensures the right instrument types compete for top slots.
        seed_set = set(seed_tickers)
        if tab == "etf":
            # ETF tab: only keep ETFs — don't let stocks crowd them out
            for sym in list(candidates.keys()):
                if sym not in ETF_SET:
                    del candidates[sym]
        else:
            # Stock tabs: remove ETFs early so they don't waste slots
            for sym in list(candidates.keys()):
                if sym in ETF_SET:
                    del candidates[sym]

        # Adjust preliminary count per tab — no need to enrich 48 candidates
        # when only a handful will pass the mcap/ETF gate.
        if tab == "small_cap":
            prefilter_multiplier = 3
            preliminary_cap = 60
        elif tab == "etf":
            # ETFs already filtered — candidate pool IS the final pool
            prefilter_multiplier = 1
            preliminary_cap = 40
        elif tab == "megacap":
            # Very few $1T+ companies exist — don't waste time enriching 48
            prefilter_multiplier = 1
            preliminary_cap = 20
        else:
            prefilter_multiplier = 2
            preliminary_cap = 40
        total_raw = len(candidates)
        preliminary = sorted(candidates.values(), key=lambda x: x["source_score"], reverse=True)
        preliminary = preliminary[: max(self.defaults["prefilter_target"] * prefilter_multiplier, preliminary_cap)]
        print(f"[OPTIONS_FLOW] [{tab}] Prefilter: {total_raw} raw candidates → {len(preliminary)} preliminary (sources degraded: {degraded_sources})")

        quote_tasks = []
        for row in preliminary:
            quote_tasks.append(self._enrich_stock_candidate(row, earnings_by_symbol.get(row["ticker"]), macro))
        enriched_rows = await asyncio.gather(*quote_tasks, return_exceptions=True)

        final_rows = []
        for base, enriched in zip(preliminary, enriched_rows):
            if isinstance(enriched, Exception):
                degraded_sources.append(f"stock_enrichment:{base['ticker']}")
                continue
            if not enriched:
                continue
            if enriched.get("price") is None or enriched.get("price", 0) < self.defaults["min_stock_price"]:
                continue
            liquidity_dollars = enriched.get("liquidity_dollars")
            liquidity_supported = enriched.get("liquidity_supported", False)
            if (
                liquidity_supported
                and liquidity_dollars is not None
                and liquidity_dollars < self.defaults["min_stock_liquidity"]
                and base.get("source_score", 0) < 28
            ):
                continue

            # Post-enrichment ETF check: enrichment may have dynamically
            # detected new ETFs (via profile signals) that weren't in ETF_SET
            # during the early filter.  Enforce tab separation again.
            is_etf = enriched.get("category") == "etf"
            if tab == "etf" and not is_etf:
                continue
            if tab != "etf" and is_etf:
                continue

            # Market cap gates — enforce separate universes per tab
            if tab != "etf":
                profile = enriched.get("profile") or {}
                mcap = _safe_float(profile.get("market_cap"))
                tier_min, tier_max = TIER_MCAP_RANGES.get(tab, (0, None))
                if mcap is not None:
                    if mcap < tier_min:
                        continue
                    if tier_max is not None and mcap > tier_max:
                        continue
                elif base.get("ticker") not in seed_set:
                    # Unknown mcap: only keep if ticker is a seed for this tab.
                    # Prevents random tickers from leaking into wrong tabs.
                    continue

            merged = {**base, **enriched}
            merged["reasons"] = sorted(list(base.get("reasons", set())))
            merged["prefilter_score"] = round(self._score_stock_context(merged), 1)
            final_rows.append(merged)

        final_rows.sort(key=lambda x: x.get("prefilter_score", 0), reverse=True)
        final_cut = final_rows[: self.defaults["prefilter_target"]]
        print(f"[OPTIONS_FLOW] [{tab}] Prefilter: {len(preliminary)} preliminary → {len(final_rows)} enriched → {len(final_cut)} final candidates")
        return {
            "candidates": final_cut,
            "degraded_sources": degraded_sources,
            "macro": macro,
        }

    async def _enrich_stock_candidate(self, row: dict, earnings_event: dict | None, macro: dict) -> dict | None:
        symbol = row["ticker"]
        stored_technicals = get_latest_technicals(symbol)
        technicals_task = asyncio.to_thread(self.data.finnhub.get_technicals, symbol)
        profile_task = asyncio.to_thread(self.data.finnhub.get_company_profile, symbol)
        quote_tasks = [asyncio.to_thread(self.data.finnhub.get_quote, symbol)]
        if self.data.fmp:
            quote_tasks.append(self.data.fmp.get_quote(symbol))
        else:
            quote_tasks.append(asyncio.sleep(0, result={}))

        finnhub_quote, fmp_quote, technicals_fallback, profile = await asyncio.gather(
            *quote_tasks,
            technicals_task,
            profile_task,
            return_exceptions=True,
        )

        finnhub_quote = {} if isinstance(finnhub_quote, Exception) else (finnhub_quote or {})
        fmp_quote = {} if isinstance(fmp_quote, Exception) else (fmp_quote or {})
        technicals_fallback = {} if isinstance(technicals_fallback, Exception) else (technicals_fallback or {})
        profile = {} if isinstance(profile, Exception) else (profile or {})

        technicals = _normalize_technicals(stored_technicals, technicals_fallback)
        price = _safe_float(finnhub_quote.get("price")) or _safe_float(fmp_quote.get("price")) or row.get("price_hint")
        change_pct = _safe_float(finnhub_quote.get("change_pct"))
        if change_pct is None:
            change_pct = _safe_float(fmp_quote.get("changesPercentage"))
        if change_pct is None:
            change_pct = row.get("change_hint")

        volume = _safe_float(fmp_quote.get("volume"))
        avg_volume = _safe_float(technicals.get("avg_volume"))
        relative_volume = round(volume / avg_volume, 2) if volume and avg_volume else None
        liquidity_dollars = (price or 0) * volume if price and volume else None

        sma20 = _safe_float(technicals.get("sma_20"))
        sma50 = _safe_float(technicals.get("sma_50"))
        rsi = _safe_float(technicals.get("rsi"))
        macd = _safe_float(technicals.get("macd"))
        macd_signal = _safe_float(technicals.get("macd_signal"))
        macd_hist = _safe_float(technicals.get("macd_histogram"))

        breakout = bool(price and sma20 and sma50 and price > sma20 and price > sma50 and ((macd is not None and macd_signal is not None and macd > macd_signal) or (macd_hist is not None and macd_hist > 0))) or ("breakout setup" in row.get("reasons", []))
        compression = bool(sma20 and sma50 and price and abs(sma20 - sma50) / price <= 0.025 and abs(change_pct or 0) <= 3.5)
        reversal = bool((rsi is not None and rsi < 38 and (change_pct or 0) < 0) or ("oversold reversal" in row.get("reasons", [])))
        exhaustion = bool((rsi is not None and rsi > 68 and (change_pct or 0) > 3) or ("exhaustion watch" in row.get("reasons", [])))

        earnings_days = None
        catalyst_context = None
        if isinstance(earnings_event, dict):
            earnings_date = earnings_event.get("date")
            earnings_days = _days_to_expiration(earnings_date)
            if earnings_days is not None:
                catalyst_context = f"earnings in {earnings_days}d"
            else:
                catalyst_context = "earnings scheduled"

        short_squeeze_context = "high short-float screen hit" if row.get("short_squeeze_flag") else None
        liquidity_context = "strong stock liquidity" if (liquidity_dollars is not None and liquidity_dollars >= self.defaults["min_stock_liquidity"] * 2) else "adequate stock liquidity"
        breakout_context = "breakout / trend continuation" if breakout else ""
        reversal_context = "reversal / exhaustion watch" if reversal or exhaustion else ""

        return {
            "category": "etf" if symbol in ETF_SET else "stock",
            "price": round(price, 4) if price is not None else None,
            "change_pct": round(change_pct, 2) if change_pct is not None else None,
            "volume": int(volume) if volume else 0,
            "avg_volume": round(avg_volume, 0) if avg_volume else None,
            "stock_relative_volume": relative_volume,
            "liquidity_dollars": float(liquidity_dollars) if liquidity_dollars is not None else None,
            "liquidity_supported": liquidity_dollars is not None,
            "technicals": technicals,
            "profile": profile,
            "breakout_context": breakout_context or None,
            "compression_context": "compression / coil setup" if compression else None,
            "reversal_context": reversal_context or None,
            "catalyst_context": catalyst_context,
            "earnings_days": earnings_days,
            "liquidity_context": liquidity_context,
            "short_squeeze_context": short_squeeze_context,
            "macro_context": self._macro_context_summary(macro),
        }

    def _score_stock_context(self, row: dict) -> float:
        score = row.get("source_score", 0.0)
        rvol = _safe_float(row.get("stock_relative_volume"))
        if rvol is not None:
            score += _clip((rvol - 1.0) * 18, 0, 28)
        change_pct = abs(_safe_float(row.get("change_pct")) or 0)
        score += _clip(change_pct * 2.8, 0, 18)
        if row.get("breakout_context"):
            score += 12
        if row.get("compression_context"):
            score += 8
        if row.get("reversal_context"):
            score += 8
        if row.get("catalyst_context"):
            score += 10
        if row.get("short_squeeze_context"):
            score += 7
        return _clip(score)

    async def _inspect_shortlist(self, candidates: list[dict], macro: dict, *, tab: str = "megacap") -> list[dict | None]:
        sem = asyncio.Semaphore(6)

        async def _bounded(candidate: dict):
            async with sem:
                try:
                    return await self._inspect_one_ticker(candidate, macro, tab=tab)
                except Exception as exc:
                    print(f"[OPTIONS_FLOW] ticker inspect failed for {candidate.get('ticker')}: {exc}")
                    return None

        return await asyncio.gather(*[_bounded(c) for c in candidates])

    async def _inspect_one_ticker(self, candidate: dict, macro: dict, *, tab: str = "megacap") -> dict | None:
        symbol = candidate["ticker"]
        price = _safe_float(candidate.get("price"))
        if not price:
            return None

        expirations = await self.data.public_com.get_option_expirations(symbol)
        valid_expirations = []
        for exp in expirations:
            dte = _days_to_expiration(exp)
            if dte is not None and self.defaults["min_dte"] <= dte <= self.defaults["max_dte"]:
                valid_expirations.append(exp)
        valid_expirations = valid_expirations[: self.defaults["max_expirations_per_ticker"]]
        if not valid_expirations and expirations:
            valid_expirations = expirations[:1]
        if not valid_expirations:
            return None

        chain_list = await asyncio.gather(
            *[self.data.public_com.get_full_chain_with_greeks(symbol, exp) for exp in valid_expirations],
            return_exceptions=True,
        )

        contracts: list[dict] = []
        expected_move_candidates = []
        approximate_metrics = []
        missing_flags = []
        total_normalized = 0

        for exp, chain in zip(valid_expirations, chain_list):
            if isinstance(chain, Exception) or not isinstance(chain, dict):
                missing_flags.append(f"chain_unavailable:{exp}")
                continue
            calls = chain.get("calls", []) or []
            puts = chain.get("puts", []) or []
            expected_move = self._estimate_expected_move(price, calls, puts)
            if expected_move is not None:
                expected_move_candidates.append(expected_move)

            for side, rows in (("call", calls), ("put", puts)):
                for raw in rows:
                    contract = self._normalize_contract(symbol, side, exp, raw, price)
                    if contract:
                        total_normalized += 1
                        if self._contract_filter(contract, candidate, tab=tab):
                            contracts.append(contract)

        if not contracts:
            print(f"[OPTIONS_FLOW] {symbol}: 0/{total_normalized} contracts passed filter")
            return None

        call_volume = sum(c["volume"] for c in contracts if c["type"] == "call")
        put_volume = sum(c["volume"] for c in contracts if c["type"] == "put")
        call_oi = sum(c["open_interest"] for c in contracts if c["type"] == "call")
        put_oi = sum(c["open_interest"] for c in contracts if c["type"] == "put")
        total_oi = call_oi + put_oi

        near_spot_contracts = [c for c in contracts if abs((c["strike"] - price) / price) <= 0.03]
        near_spot_oi_density = round(sum(c["open_interest"] for c in near_spot_contracts) / total_oi, 4) if total_oi else None
        near_spot_gamma_density = None
        gamma_values = [max((c.get("gamma") or 0) * c["open_interest"] * 100.0, 0) for c in near_spot_contracts if c.get("gamma") is not None]
        if gamma_values:
            near_spot_gamma_density = round(sum(gamma_values), 2)
            approximate_metrics.append("near_spot_gamma_density")

        call_put_volume_ratio = round(call_volume / put_volume, 3) if put_volume else None
        call_put_oi_ratio = round(call_oi / put_oi, 3) if put_oi else None
        iv_values = [c["implied_volatility"] for c in contracts if c.get("implied_volatility") is not None]
        iv_current = round(sum(iv_values) / len(iv_values), 4) if iv_values else None
        if iv_current is None:
            missing_flags.append("iv_missing")

        for contract in contracts:
            contract["flow_score"] = round(self._score_flow(contract, call_put_volume_ratio), 1)
            contract["asymmetry_score"] = round(self._score_asymmetry(contract, price), 1)
            contract["contract_score"] = round(contract["flow_score"] * 0.6 + contract["asymmetry_score"] * 0.4, 1)

        ranked_for_history = sorted(contracts, key=lambda x: x.get("contract_score", 0), reverse=True)
        for contract in ranked_for_history[:8]:
            history = get_contract_flow_history_summary(contract["contract_symbol"], days=30)
            if history:
                contract["history"] = history
                contract["repeated_flow_score"] = history.get("repeated_flow_score")
                contract["iv_percentile"] = history.get("iv_percentile")
                if history.get("repeated_flow_score") is not None:
                    contract["flow_score"] = round(min(100.0, contract["flow_score"] + history["repeated_flow_score"] * 0.35), 1)
                if history.get("iv_percentile") is not None:
                    contract["iv_rank"] = history.get("iv_percentile")
                contract["contract_score"] = round(contract["flow_score"] * 0.6 + contract["asymmetry_score"] * 0.4, 1)
            else:
                contract["repeated_flow_score"] = None
                contract["iv_percentile"] = None

        contracts.sort(key=lambda x: x.get("contract_score", 0), reverse=True)
        top_contracts = contracts[: self.defaults["top_contracts_per_ticker"]]
        best_contract = top_contracts[0]
        stock_context_score = round(self._score_stock_context(candidate), 1)
        gamma_score = round(self._score_gamma(candidate, near_spot_oi_density, near_spot_gamma_density, top_contracts), 1)
        volatility_score = round(self._score_volatility(candidate, iv_current, best_contract), 1)
        sentiment_score = round(self._score_sentiment(candidate, call_put_volume_ratio, call_put_oi_ratio), 1)
        asymmetry_score = round(sum(c["asymmetry_score"] for c in top_contracts) / len(top_contracts), 1)
        flow_score = round(sum(c["flow_score"] for c in top_contracts) / len(top_contracts), 1)
        composite_score = round(
            flow_score * self.weights["flow_score"] +
            gamma_score * self.weights["gamma_score"] +
            asymmetry_score * self.weights["asymmetry_score"] +
            volatility_score * self.weights["volatility_score"] +
            sentiment_score * self.weights["sentiment_score"] +
            stock_context_score * self.weights["stock_context_score"],
            1,
        )

        primary_signal = self._classify_signal(candidate, best_contract, gamma_score, volatility_score, sentiment_score)
        confidence, confidence_score = self._confidence_label(top_contracts, missing_flags, candidate)
        thesis = self._build_ticker_thesis(candidate, best_contract, primary_signal, expected_move_candidates)
        risks = self._build_risks(candidate, best_contract, missing_flags)

        snapshot_rows = [self._build_snapshot_row(symbol, price, c, expected_move_candidates[0] if expected_move_candidates else None) for c in contracts[:25]]

        options_context_summary = self._options_context_summary(call_put_volume_ratio, call_put_oi_ratio, near_spot_oi_density, iv_current, expected_move_candidates)
        stock_context_summary = self._stock_context_summary(candidate)
        data_quality_flags = sorted(set(missing_flags + (["greeks_partial"] if any(c.get("delta") is None for c in top_contracts) else [])))
        if not any(c.get("history") for c in top_contracts):
            data_quality_flags.append("history_not_ready")
        if approximate_metrics:
            data_quality_flags.append("approximate_gamma_density")

        call_contracts = [c for c in top_contracts if c["type"] == "call"]
        put_contracts = [c for c in top_contracts if c["type"] == "put"]

        return {
            "ticker": symbol,
            "category": candidate.get("category", "stock"),
            "underlying_price": price,
            "price_change_pct": candidate.get("change_pct"),
            "expiration_focus": valid_expirations,
            "call_volume": call_volume,
            "put_volume": put_volume,
            "total_volume": call_volume + put_volume,
            "pc_ratio": round(put_volume / call_volume, 3) if call_volume else None,
            "call_oi": call_oi,
            "put_oi": put_oi,
            "total_oi": total_oi,
            "avg_call_iv": self._avg_iv(call_contracts),
            "avg_put_iv": self._avg_iv(put_contracts),
            "iv_skew": round((self._avg_iv(put_contracts) or 0) - (self._avg_iv(call_contracts) or 0), 4) if call_contracts and put_contracts else None,
            "max_pain": self._max_pain(contracts),
            "top_calls": [self._contract_response(symbol, c, primary_signal) for c in call_contracts[:3]],
            "top_puts": [self._contract_response(symbol, c, primary_signal) for c in put_contracts[:3]],
            "primary_signal": primary_signal,
            "confidence": confidence,
            "confidence_score": confidence_score,
            "composite_score": composite_score,
            "modular_scores": {
                "flow_score": flow_score,
                "gamma_score": gamma_score,
                "asymmetry_score": asymmetry_score,
                "volatility_score": volatility_score,
                "sentiment_score": sentiment_score,
                "stock_context_score": stock_context_score,
            },
            "stock_context_summary": stock_context_summary,
            "options_context_summary": options_context_summary,
            "stock_context": {
                "stock_relative_volume": candidate.get("stock_relative_volume"),
                "stock_intraday_move_pct": candidate.get("change_pct"),
                "breakout_context": candidate.get("breakout_context"),
                "compression_context": candidate.get("compression_context"),
                "reversal_context": candidate.get("reversal_context"),
                "catalyst_context": candidate.get("catalyst_context"),
                "liquidity_context": candidate.get("liquidity_context"),
                "short_squeeze_context": candidate.get("short_squeeze_context"),
                "macro_context": candidate.get("macro_context"),
            },
            "options_context": {
                "call_put_volume_ratio": call_put_volume_ratio,
                "call_put_oi_ratio": call_put_oi_ratio,
                "near_spot_oi_density": near_spot_oi_density,
                "near_spot_gamma_density": near_spot_gamma_density,
                "iv_current": iv_current,
                "expected_move_from_atm_straddle": expected_move_candidates[0] if expected_move_candidates else None,
                "gamma_score_is_approximation": True,
            },
            "top_contracts": [self._contract_response(symbol, c, primary_signal) for c in top_contracts],
            "ranked_contracts": [self._contract_response(symbol, c, primary_signal) for c in contracts[:25]],
            "thesis": thesis,
            "risks": risks,
            "data_quality": {
                "confidence": confidence,
                "confidence_score": confidence_score,
                "flags": data_quality_flags,
                "missing_data_flags": missing_flags,
                "approximate_metrics": sorted(set(approximate_metrics)),
                "history_metrics_ready": any(c.get("history") for c in top_contracts),
            },
            "snapshot_rows": snapshot_rows,
        }

    def _normalize_contract(self, symbol: str, side: str, expiration: str, raw: dict, spot_price: float) -> dict | None:
        strike = _safe_float(raw.get("strike"))
        bid = _safe_float(raw.get("bid"))
        ask = _safe_float(raw.get("ask"))
        last = _safe_float(raw.get("last"))
        midpoint = _midpoint(bid, ask, last)
        volume = _safe_int(raw.get("volume"))
        open_interest = _safe_int(raw.get("openInterest"))
        dte = _days_to_expiration(expiration)
        if strike is None or midpoint is None or dte is None:
            return None
        spread_pct = _spread_pct(bid, ask, midpoint)
        implied_vol = _safe_float(raw.get("iv"))
        delta = _safe_float(raw.get("delta"))
        gamma = _safe_float(raw.get("gamma"))
        theta = _safe_float(raw.get("theta"))
        vega = _safe_float(raw.get("vega"))
        vol_oi_ratio = round(volume / open_interest, 2) if open_interest else None
        premium_traded_estimate = round(volume * midpoint * 100.0, 2)
        if side == "call":
            break_even = strike + midpoint
        else:
            break_even = strike - midpoint
        break_even_distance_pct = round(((break_even - spot_price) / spot_price) * 100.0, 2) if spot_price else None
        moneyness_pct = abs(strike - spot_price) / spot_price if spot_price else None
        abs_delta = abs(delta) if delta is not None else None

        return {
            "contract_symbol": raw.get("symbol"),
            "type": side,
            "strike": strike,
            "expiration": expiration,
            "dte": dte,
            "bid": bid,
            "ask": ask,
            "last": last,
            "midpoint": midpoint,
            "volume": volume,
            "open_interest": open_interest,
            "implied_volatility": implied_vol,
            "delta": delta,
            "gamma": gamma,
            "theta": theta,
            "vega": vega,
            "option_volume_to_oi_ratio": vol_oi_ratio,
            "spread_pct": spread_pct,
            "premium_traded_estimate": premium_traded_estimate,
            "break_even": round(break_even, 4),
            "break_even_distance_pct": break_even_distance_pct,
            "moneyness_pct": round(moneyness_pct, 4) if moneyness_pct is not None else None,
            "abs_delta": abs_delta,
            "liquidity_quality": round(self._score_contract_liquidity(volume, open_interest, spread_pct), 1),
        }

    def _contract_filter(self, contract: dict, candidate: dict, *, tab: str = "megacap") -> bool:
        # Smaller-cap tiers have inherently lower options liquidity,
        # so we relax contract-level thresholds progressively.
        if tab == "small_cap":
            min_volume = max(self.defaults["min_contract_volume"] // 3, 3)
            min_oi = max(self.defaults["min_open_interest"] // 5, 5)
            min_premium = self.defaults["min_premium_traded_estimate"] * 0.2
            max_spread = self.defaults["max_spread_pct"] * 1.67  # ~30%
            min_liquidity_quality = 10
        elif tab == "large_cap":
            min_volume = max(self.defaults["min_contract_volume"] // 2, 5)
            min_oi = max(self.defaults["min_open_interest"] // 3, 8)
            min_premium = self.defaults["min_premium_traded_estimate"] * 0.5
            max_spread = self.defaults["max_spread_pct"] * 1.33  # ~24%
            min_liquidity_quality = 15
        else:
            min_volume = self.defaults["min_contract_volume"]
            min_oi = self.defaults["min_open_interest"]
            min_premium = self.defaults["min_premium_traded_estimate"]
            max_spread = self.defaults["max_spread_pct"]
            min_liquidity_quality = 20

        if contract["volume"] < min_volume:
            return False
        if contract["open_interest"] < min_oi:
            return False
        if contract["premium_traded_estimate"] < min_premium:
            return False
        if contract.get("spread_pct") is not None and contract["spread_pct"] > max_spread:
            return False
        if contract.get("moneyness_pct") is not None and contract["moneyness_pct"] > self.defaults["max_moneyness_pct"]:
            return False
        abs_delta = contract.get("abs_delta")
        if abs_delta is not None:
            if abs_delta < self.defaults["preferred_delta_min"] * 0.7:
                return False
            if abs_delta > 0.9:
                return False
        if candidate.get("category") != "etf" and contract["liquidity_quality"] < min_liquidity_quality:
            return False
        return True

    def _score_contract_liquidity(self, volume: int, open_interest: int, spread_pct: float | None) -> float:
        score = 0.0
        score += _clip(math.log10(max(volume, 1)) * 18, 0, 35)
        score += _clip(math.log10(max(open_interest, 1)) * 15, 0, 28)
        if spread_pct is None:
            score += 5
        elif spread_pct <= 5:
            score += 32
        elif spread_pct <= 10:
            score += 24
        elif spread_pct <= 18:
            score += 14
        else:
            score -= 15
        return _clip(score)

    def _score_flow(self, contract: dict, call_put_volume_ratio: float | None) -> float:
        score = 0.0
        ratio = contract.get("option_volume_to_oi_ratio")
        if ratio is not None:
            score += _clip(ratio * 28, 0, 35)
        premium = contract.get("premium_traded_estimate") or 0
        score += _clip(math.log10(max(premium, 1)) * 12 - 36, 0, 28)
        score += contract.get("liquidity_quality", 0) * 0.22
        if contract["type"] == "call" and call_put_volume_ratio is not None and call_put_volume_ratio > 1:
            score += _clip((call_put_volume_ratio - 1) * 8, 0, 12)
        if contract["type"] == "put" and call_put_volume_ratio is not None and call_put_volume_ratio < 1:
            score += _clip((1 - call_put_volume_ratio) * 10, 0, 12)
        return _clip(score)

    def _score_asymmetry(self, contract: dict, spot_price: float) -> float:
        score = contract.get("liquidity_quality", 0) * 0.35
        be_dist = abs(contract.get("break_even_distance_pct") or 999)
        if be_dist <= 3:
            score += 28
        elif be_dist <= 6:
            score += 22
        elif be_dist <= 10:
            score += 14
        else:
            score += 5
        dte = contract.get("dte") or 0
        if 10 <= dte <= 35:
            score += 18
        elif 7 <= dte <= 45:
            score += 12
        premium_pct = ((contract.get("midpoint") or 0) / spot_price) * 100 if spot_price else 0
        if premium_pct <= 2:
            score += 14
        elif premium_pct <= 4:
            score += 10
        else:
            score += 4
        abs_delta = contract.get("abs_delta")
        if abs_delta is not None:
            if self.defaults["preferred_delta_min"] <= abs_delta <= self.defaults["preferred_delta_max"]:
                score += 16
            elif 0.15 <= abs_delta <= 0.75:
                score += 10
        return _clip(score)

    def _score_gamma(self, candidate: dict, near_spot_oi_density: float | None, near_spot_gamma_density: float | None, top_contracts: list[dict]) -> float:
        score = 0.0
        if near_spot_oi_density is not None:
            score += _clip(near_spot_oi_density * 120, 0, 32)
        if near_spot_gamma_density is not None:
            score += _clip(math.log10(max(near_spot_gamma_density, 1)) * 12 - 10, 0, 28)
        if any(c["type"] == "call" for c in top_contracts):
            score += 8
        if candidate.get("breakout_context"):
            score += 12
        if candidate.get("compression_context"):
            score += 10
        if candidate.get("short_squeeze_context"):
            score += 10
        return _clip(score)

    def _score_volatility(self, candidate: dict, iv_current: float | None, best_contract: dict) -> float:
        score = 0.0
        if candidate.get("compression_context"):
            score += 24
        if candidate.get("catalyst_context"):
            score += 22
        if iv_current is not None:
            if iv_current <= 0.35:
                score += 18
            elif iv_current <= 0.60:
                score += 12
            else:
                score += 8
        if best_contract.get("iv_percentile") is not None:
            pct = best_contract["iv_percentile"]
            score += _clip(abs(50 - pct) * 0.6, 0, 20)
        else:
            score += 6
        return _clip(score)

    def _score_sentiment(self, candidate: dict, cp_vol_ratio: float | None, cp_oi_ratio: float | None) -> float:
        score = 0.0
        if cp_vol_ratio is not None:
            score += _clip(abs(cp_vol_ratio - 1.0) * 22, 0, 30)
        if cp_oi_ratio is not None:
            score += _clip(abs(cp_oi_ratio - 1.0) * 16, 0, 24)
        if candidate.get("reversal_context"):
            score += 18
        if candidate.get("short_squeeze_context"):
            score += 10
        return _clip(score)

    def _classify_signal(self, candidate: dict, best_contract: dict, gamma_score: float, vol_score: float, sentiment_score: float) -> str:
        if candidate.get("catalyst_context") and (best_contract.get("dte") or 0) <= 21:
            return "earnings_implied_move"
        if gamma_score >= 70 and best_contract["type"] == "call":
            return "gamma_setup"
        if best_contract.get("flow_score", 0) >= 72 and best_contract.get("repeated_flow_score") is not None:
            return "unusual_flow"
        if best_contract.get("asymmetry_score", 0) >= 72:
            return "asymmetric_rr"
        if vol_score >= 68:
            return "vol_expansion_watch"
        if sentiment_score >= 65:
            return "sentiment_extreme"
        if candidate.get("breakout_context") and best_contract["type"] == "call":
            return "breakout_confirmation"
        return "asymmetric_rr"

    def _confidence_label(self, top_contracts: list[dict], missing_flags: list[str], candidate: dict) -> tuple[str, int]:
        score = 50
        if top_contracts and top_contracts[0].get("liquidity_quality", 0) >= 55:
            score += 15
        if top_contracts and top_contracts[0].get("option_volume_to_oi_ratio") is not None:
            score += 10
        if any(c.get("delta") is not None and c.get("gamma") is not None for c in top_contracts):
            score += 10
        if any(c.get("history") for c in top_contracts):
            score += 10
        if candidate.get("catalyst_context"):
            score += 5
        score -= len(set(missing_flags)) * 6
        if candidate.get("short_squeeze_context") is None:
            score -= 2
        score = int(_clip(score, 0, 100))
        if score >= 75:
            return "high", score
        if score >= 55:
            return "medium", score
        return "low", score

    def _build_ticker_thesis(self, candidate: dict, best_contract: dict, primary_signal: str, expected_moves: list[dict]) -> str:
        side_word = "upside" if best_contract["type"] == "call" else "downside"
        parts = [f"{primary_signal.replace('_', ' ')} setup with {side_word} exposure centered on {best_contract['expiration']}."]
        if candidate.get("stock_relative_volume"):
            parts.append(f"Stock is trading at roughly {candidate['stock_relative_volume']}x normal volume")
        if candidate.get("breakout_context"):
            parts.append("while price/technical context supports a continuation move")
        elif candidate.get("reversal_context"):
            parts.append("with stock-side reversal pressure building")
        if expected_moves:
            parts.append(f"ATM straddle implies about a {expected_moves[0]['expected_move_pct']}% move into expiry")
        return " ".join(parts)

    def _build_risks(self, candidate: dict, best_contract: dict, missing_flags: list[str]) -> list[str]:
        risks = []
        if best_contract.get("spread_pct") and best_contract["spread_pct"] > 10:
            risks.append("option spread is wide enough to increase slippage")
        if abs(best_contract.get("break_even_distance_pct") or 0) > 8:
            risks.append("break-even requires a larger-than-average move")
        if candidate.get("catalyst_context") is None:
            risks.append("no hard catalyst detected, so follow-through could fade")
        if any(flag.startswith("chain_unavailable") for flag in missing_flags):
            risks.append("partial chain data lowered signal confidence")
        if best_contract.get("repeated_flow_score") is None:
            risks.append("history-backed repeated flow confirmation is not built out yet")
        return risks[:4]

    def _build_snapshot_row(self, symbol: str, price: float, contract: dict, expected_move: dict | None) -> dict:
        return {
            "underlying": symbol,
            "contract_symbol": contract.get("contract_symbol"),
            "expiration": contract.get("expiration"),
            "option_type": contract.get("type"),
            "strike": contract.get("strike"),
            "underlying_price": price,
            "bid": contract.get("bid"),
            "ask": contract.get("ask"),
            "last": contract.get("last"),
            "midpoint": contract.get("midpoint"),
            "volume": contract.get("volume"),
            "open_interest": contract.get("open_interest"),
            "openInterest": contract.get("open_interest"),
            "implied_volatility": contract.get("implied_volatility"),
            "iv": contract.get("implied_volatility"),
            "delta": contract.get("delta"),
            "gamma": contract.get("gamma"),
            "theta": contract.get("theta"),
            "vega": contract.get("vega"),
            "spread_pct": contract.get("spread_pct"),
            "premium_traded_estimate": contract.get("premium_traded_estimate"),
            "expected_move_pct": expected_move.get("expected_move_pct") if expected_move else None,
        }

    def _contract_response(self, symbol: str, contract: dict, primary_signal: str) -> dict:
        thesis = f"{primary_signal.replace('_', ' ')} candidate via {contract['type']}s around {contract['strike']} with {contract['dte']} DTE."
        return {
            "contract_symbol": contract.get("contract_symbol"),
            "symbol": contract.get("contract_symbol"),
            "type": contract.get("type"),
            "side": contract.get("type"),
            "strike": contract.get("strike"),
            "expiration": contract.get("expiration"),
            "dte": contract.get("dte"),
            "bid": contract.get("bid"),
            "ask": contract.get("ask"),
            "last": contract.get("last"),
            "mid": contract.get("midpoint"),
            "midpoint": contract.get("midpoint"),
            "volume": contract.get("volume"),
            "open_interest": contract.get("open_interest"),
            "openInterest": contract.get("open_interest"),
            "implied_volatility": contract.get("implied_volatility"),
            "iv": contract.get("implied_volatility"),
            "delta": contract.get("delta"),
            "gamma": contract.get("gamma"),
            "theta": contract.get("theta"),
            "vega": contract.get("vega"),
            "greeks": {
                "delta": contract.get("delta"),
                "gamma": contract.get("gamma"),
                "theta": contract.get("theta"),
                "vega": contract.get("vega"),
            },
            "option_volume_to_oi_ratio": contract.get("option_volume_to_oi_ratio"),
            "vol_oi_ratio": contract.get("option_volume_to_oi_ratio"),
            "spread_pct": contract.get("spread_pct"),
            "premium_traded_estimate": contract.get("premium_traded_estimate"),
            "break_even": contract.get("break_even"),
            "break_even_distance_pct": contract.get("break_even_distance_pct"),
            "contract_liquidity_quality": contract.get("liquidity_quality"),
            "repeated_flow_score": contract.get("repeated_flow_score"),
            "iv_rank": contract.get("iv_rank"),
            "iv_percentile": contract.get("iv_percentile"),
            "contract_score": contract.get("contract_score"),
            "flow_score": contract.get("flow_score"),
            "asymmetry_score": contract.get("asymmetry_score"),
            "short_thesis": thesis,
            "underlying": symbol,
        }

    def _estimate_expected_move(self, spot_price: float, calls: list[dict], puts: list[dict]) -> dict | None:
        if not calls or not puts or not spot_price:
            return None
        calls_by_strike = {round(_safe_float(c.get("strike")) or 0, 3): c for c in calls if _safe_float(c.get("strike")) is not None}
        puts_by_strike = {round(_safe_float(p.get("strike")) or 0, 3): p for p in puts if _safe_float(p.get("strike")) is not None}
        common = sorted(set(calls_by_strike.keys()) & set(puts_by_strike.keys()), key=lambda s: abs(s - spot_price))
        if not common:
            return None
        strike = common[0]
        call = calls_by_strike[strike]
        put = puts_by_strike[strike]
        call_mid = _midpoint(_safe_float(call.get("bid")), _safe_float(call.get("ask")), _safe_float(call.get("last")))
        put_mid = _midpoint(_safe_float(put.get("bid")), _safe_float(put.get("ask")), _safe_float(put.get("last")))
        if call_mid is None or put_mid is None:
            return None
        implied_dollar = round(call_mid + put_mid, 4)
        return {
            "atm_strike": strike,
            "expected_move_dollars": implied_dollar,
            "expected_move_pct": round((implied_dollar / spot_price) * 100.0, 2),
        }

    def _options_context_summary(self, cp_vol_ratio: float | None, cp_oi_ratio: float | None, near_spot_oi_density: float | None, iv_current: float | None, expected_moves: list[dict]) -> str:
        parts = []
        if cp_vol_ratio is not None:
            parts.append(f"call/put volume ratio {cp_vol_ratio}")
        if cp_oi_ratio is not None:
            parts.append(f"OI ratio {cp_oi_ratio}")
        if near_spot_oi_density is not None:
            parts.append(f"{round(near_spot_oi_density * 100, 1)}% of retained OI sits near spot")
        if iv_current is not None:
            parts.append(f"current IV averages {round(iv_current * 100, 1)}%")
        if expected_moves:
            parts.append(f"ATM straddle implies ~{expected_moves[0]['expected_move_pct']}% move")
        return "; ".join(parts)

    def _stock_context_summary(self, candidate: dict) -> str:
        parts = []
        if candidate.get("stock_relative_volume") is not None:
            parts.append(f"RVOL {candidate['stock_relative_volume']}x")
        if candidate.get("change_pct") is not None:
            parts.append(f"move {candidate['change_pct']}%")
        if candidate.get("breakout_context"):
            parts.append(candidate["breakout_context"])
        if candidate.get("compression_context"):
            parts.append(candidate["compression_context"])
        if candidate.get("reversal_context"):
            parts.append(candidate["reversal_context"])
        if candidate.get("catalyst_context"):
            parts.append(candidate["catalyst_context"])
        if candidate.get("short_squeeze_context"):
            parts.append(candidate["short_squeeze_context"])
        return "; ".join(parts)

    def _macro_context_summary(self, macro: dict) -> str | None:
        if not isinstance(macro, dict):
            return None
        vix_block = (macro.get("vix_fear_index") or {}) if isinstance(macro.get("vix_fear_index"), dict) else {}
        vix = _safe_float(vix_block.get("vix_index")) or _safe_float(vix_block.get("current_vix"))
        yc_block = macro.get("yield_curve_10y_2y") or {}
        yc = _safe_float(yc_block.get("spread_pct")) or _safe_float(yc_block.get("current_spread"))
        parts = []
        if vix is not None:
            parts.append("elevated macro vol" if vix >= 20 else "calmer macro vol")
        if yc is not None:
            parts.append("inverted curve" if yc < 0 else "positive curve")
        return ", ".join(parts) if parts else None

    def _avg_iv(self, contracts: list[dict]) -> float | None:
        ivs = [c.get("implied_volatility") for c in contracts if c.get("implied_volatility") is not None]
        if not ivs:
            return None
        return round(sum(ivs) / len(ivs), 4)

    def _max_pain(self, contracts: list[dict]) -> float | None:
        by_strike: dict[float, int] = defaultdict(int)
        for contract in contracts:
            if contract.get("strike") is not None:
                by_strike[contract["strike"]] += contract.get("open_interest", 0)
        if not by_strike:
            return None
        return max(by_strike, key=by_strike.get)
