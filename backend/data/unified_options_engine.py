"""
Unified master unusual-options screener.

Replaces the 4-tab (megacap / large_cap / small_cap / etf) architecture
with a single globally ranked leaderboard that scans all universes in one
pass.  Every result row is tagged with:

    asset_type       : "etf" | "stock"
    market_cap_bucket: "megacap" | "large" | "small" | "etf" | "unknown"

Backward-compatible tab filtering is done at the endpoint level (main.py)
by filtering the master snapshot — no separate pipelines required.

Key design decisions
────────────────────
• All Finviz screens from every tab run in ONE asyncio.gather call — zero
  redundant HTTP round-trips that the old architecture paid for each tab.

• Market-cap gating is replaced with tagging — every enriched candidate is
  kept regardless of mcap bucket.  This lets small signals (e.g. a $20 B
  mid-cap with extreme options flow) compete globally against megacaps.

• Stage 1.5 trim (base class, master_stage2_limit): after Stage 1 confirms
  valid expirations for up to 100 candidates, we trim to the top-N by
  prefilter_score before Stage 2 chain-fetches.  Keeps Tradier calls ≤ 120/min.

• _contract_filter uses the candidate's market_cap_bucket (tagged during
  prefilter) to apply the same per-tier relaxation as before.
"""

from __future__ import annotations

import asyncio
import math
import time as _ts
from typing import Any

from data.tradier_flow_engine import TradierFlowEngine
from data.options_flow_engine import ETF_SET, TIER_MCAP_RANGES, _safe_float


# ── Bucket thresholds ────────────────────────────────────────────────────────
# Aligned with TIER_MCAP_RANGES from the base engine.
_MCAP_MEGACAP  = 1_000_000_000_000   # $1 T+
_MCAP_LARGE    =   100_000_000_000   # $100 B – $999.9 B
_MCAP_SMALL    =       500_000_000   # $500 M – $99.9 B


def _classify_candidate(ticker: str, market_cap: float | None, category: str) -> tuple[str, str]:
    """
    Return (asset_type, market_cap_bucket) for a prefiltered candidate.

    asset_type       : "etf" | "stock"
    market_cap_bucket: "megacap" | "large" | "small" | "etf" | "unknown"
    """
    if ticker in ETF_SET or category == "etf":
        return "etf", "etf"
    if market_cap is None or market_cap <= 0:
        return "stock", "unknown"
    if market_cap >= _MCAP_MEGACAP:
        return "stock", "megacap"
    if market_cap >= _MCAP_LARGE:
        return "stock", "large"
    if market_cap >= _MCAP_SMALL:
        return "stock", "small"
    return "stock", "unknown"


class UnifiedOptionsEngine(TradierFlowEngine):
    """
    Single-pass unusual-options screener covering all asset classes.

    Use by passing tab="master" to run_live_scan / build_prefilter_snapshot.
    The returned screener data is identical in schema to per-tab scans, but
    every ticker row additionally carries `asset_type` and `market_cap_bucket`.
    """

    # ── Overrides injected at construction ───────────────────────────────────
    # options_inspection_limit → how many candidates run through Stage 1 expiry sweep
    # master_stage2_limit      → trim Stage 2 (chain-fetch) to top-N by prefilter_score
    # prefilter_target         → final candidate cap after Finnhub enrichment
    _MASTER_DEFAULTS = {
        "options_inspection_limit": 60,    # Stage 1 sweeps top-60 by prefilter_score
        "master_stage2_limit":      30,    # Stage 2 inspects top-30 by prefilter_score
        "prefilter_target":         80,    # keep up to 80 enriched candidates
    }

    def __init__(self, data_service, overrides: dict | None = None):
        merged = {**self._MASTER_DEFAULTS, **(overrides or {})}
        super().__init__(data_service, overrides=merged)

    # ── Prefilter override ────────────────────────────────────────────────────

    async def _build_prefilter(
        self,
        seed_tickers: list[str],
        tab: str = "master",
        exclude_tickers: set[str] | None = None,
    ) -> dict:
        """
        Run ALL Finviz screens (every tab's signals combined), add ALL seed
        tickers, then tag each enriched candidate with asset_type +
        market_cap_bucket instead of filtering by mcap range.
        """
        degraded_sources: list[str] = []

        # ── 1. ALL Finviz screens in one batch ───────────────────────────────
        # Union of every tab's screens — each screen fires once per cycle.
        finviz_tasks = {
            # megacap / large-cap market-flow
            "unusual_volume":           self.data.finviz.get_unusual_volume(),
            "most_active":              self.data.finviz.get_most_active(),
            "new_highs":                self.data.finviz.get_new_highs(),
            "top_losers":               self.data.finviz.get_top_losers(),
            "oversold":                 self.data.finviz.get_oversold_stocks(),
            "overbought":               self.data.finviz.get_overbought_stocks(),
            "high_short_float":         self.data.finviz.get_high_short_float(),
            "earnings_this_week":       self.data.finviz.get_earnings_this_week(),
            # small-cap / mid-cap signals
            "midcap_unusual_volume":    self.data.finviz.get_midcap_unusual_volume(),
            "midcap_breakouts":         self.data.finviz.get_midcap_breakouts(),
            "midcap_momentum":          self.data.finviz.get_midcap_momentum(),
            "midcap_high_short":        self.data.finviz.get_midcap_high_short(),
            "growth_earnings_catalyst": self.data.finviz.get_growth_earnings_catalyst(),
            "midlarge_volume_breakout": self.data.finviz.get_midlarge_volume_breakout(),
            "volume_breakouts":         self.data.finviz.get_volume_breakouts(),
            "stage2_breakouts":         self.data.finviz.get_stage2_breakouts(),
            "revenue_growth_leaders":   self.data.finviz.get_revenue_growth_leaders(),
            "earnings_growth_leaders":  self.data.finviz.get_earnings_growth_leaders(),
        }

        tasks  = list(finviz_tasks.values())
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

        macro              = source_map.get("fred_macro", {}) or {}
        upcoming_earnings  = source_map.get("finnhub_earnings", []) or []
        earnings_by_symbol = {
            (row.get("ticker") or row.get("symbol") or "").upper(): row
            for row in upcoming_earnings
            if isinstance(row, dict) and (row.get("ticker") or row.get("symbol"))
        }

        # ── 2. Build candidate pool ──────────────────────────────────────────
        candidates: dict[str, dict] = {}

        def ensure(symbol: str) -> dict:
            sym = (symbol or "").upper().strip()
            if not sym:
                return {}
            return candidates.setdefault(sym, {
                "ticker":        sym,
                "source_score":  0.0,
                "source_hits":   [],
                "reasons":       set(),
                "price_hint":    None,
                "change_hint":   None,
                "short_squeeze_flag": False,
                "catalyst_hint": None,
            })

        def _parse_price(v) -> float | None:
            try:
                return float(str(v).replace("$", "").replace(",", "").strip()) if v else None
            except Exception:
                return None

        def _parse_percent(v) -> float | None:
            try:
                return float(str(v).replace("%", "").replace("+", "").strip()) if v else None
            except Exception:
                return None

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
                if label in ("high_short_float", "midcap_high_short"):
                    row["short_squeeze_flag"] = True
                if "earnings" in label:
                    row["catalyst_hint"] = "earnings"

        # ── 3. Apply source scores for ALL screens ───────────────────────────
        # Large/megacap market-flow screens
        add_rows(source_map.get("unusual_volume",   []), "unusual_volume",   22, "relative stock volume")
        add_rows(source_map.get("most_active",      []), "most_active",      14, "stock liquidity")
        add_rows(source_map.get("new_highs",        []), "new_highs",        16, "breakout setup")
        add_rows(source_map.get("top_losers",       []), "top_losers",       10, "reversal watch")
        add_rows(source_map.get("oversold",         []), "oversold",         12, "oversold reversal")
        add_rows(source_map.get("overbought",       []), "overbought",       10, "exhaustion watch")
        add_rows(source_map.get("high_short_float", []), "high_short_float", 14, "short squeeze context")
        add_rows(source_map.get("earnings_this_week",[]),"earnings_this_week",12, "earnings catalyst")
        # Small/mid-cap growth screens
        add_rows(source_map.get("midcap_unusual_volume",   []), "midcap_unusual_volume",    24, "mid-cap unusual volume")
        add_rows(source_map.get("midcap_breakouts",        []), "midcap_breakouts",          20, "mid-cap breakout")
        add_rows(source_map.get("midcap_momentum",         []), "midcap_momentum",           18, "mid-cap momentum")
        add_rows(source_map.get("midcap_high_short",       []), "midcap_high_short",         16, "mid-cap short squeeze")
        add_rows(source_map.get("growth_earnings_catalyst",[]), "growth_earnings_catalyst",  18, "growth earnings catalyst")
        add_rows(source_map.get("midlarge_volume_breakout",[]), "midlarge_volume_breakout",  22, "institutional volume breakout")
        add_rows(source_map.get("volume_breakouts",        []), "volume_breakouts",          16, "volume breakout")
        add_rows(source_map.get("stage2_breakouts",        []), "stage2_breakouts",          18, "stage 2 breakout")
        add_rows(source_map.get("revenue_growth_leaders",  []), "revenue_growth_leaders",    14, "revenue growth")
        add_rows(source_map.get("earnings_growth_leaders", []), "earnings_growth_leaders",   14, "earnings growth")
        # FMP market-wide
        add_rows(source_map.get("fmp_actives", []), "fmp_actives", 12, "stock liquidity")
        add_rows(source_map.get("fmp_gainers", []), "fmp_gainers", 12, "momentum move")
        add_rows(source_map.get("fmp_losers",  []), "fmp_losers",   8, "reversal watch")

        # Finnhub earnings catalyst boost
        for symbol, item in earnings_by_symbol.items():
            row = ensure(symbol)
            if row:
                row["source_score"] += 10
                row["source_hits"].append("finnhub_earnings")
                row["reasons"].add("earnings catalyst")
                row["catalyst_hint"] = "earnings"

        # ALL seed tickers guaranteed entry (ETFs + megacap + large + small)
        for seed in seed_tickers:
            row = ensure(seed)
            if row:
                row["source_score"] += 5
                row["source_hits"].append("seed_watchlist")
                row["reasons"].add("watchlist inclusion")

        # ── 4. Preliminary cut before enrichment ─────────────────────────────
        # No ETF/stock split — every candidate competes globally.
        # Use a generous cap (100) to ensure broad universe coverage before
        # Finnhub enrichment narrows it down.
        total_raw = len(candidates)
        preliminary = sorted(candidates.values(), key=lambda x: x["source_score"], reverse=True)
        preliminary_cap = 100
        preliminary = preliminary[:preliminary_cap]

        # Guarantee seeds survive the cut (seeds may have low source_score=5)
        preliminary_tickers = {row["ticker"] for row in preliminary}
        for seed_sym in seed_tickers:
            if seed_sym not in preliminary_tickers and seed_sym in candidates:
                preliminary.append(candidates[seed_sym])

        print(
            f"[UNIFIED_FLOW] Prefilter: {total_raw} raw candidates → {len(preliminary)} preliminary "
            f"({len(seed_tickers)} seeds; degraded: {degraded_sources})"
        )

        # ── 5. Finnhub enrichment (Sem(5)+0.4s — same as per-tab engine) ─────
        enrich_sem = asyncio.Semaphore(5)

        async def _throttled_enrich(row):
            async with enrich_sem:
                result = await self._enrich_stock_candidate(
                    row, earnings_by_symbol.get(row["ticker"]), macro
                )
                await asyncio.sleep(0.4)
                return result

        enriched_rows = await asyncio.gather(
            *[_throttled_enrich(row) for row in preliminary],
            return_exceptions=True,
        )

        # ── 6. Post-enrichment: tag instead of filter ─────────────────────────
        final_rows: list[dict] = []
        for base, enriched in zip(preliminary, enriched_rows):
            if isinstance(enriched, Exception):
                degraded_sources.append(f"stock_enrichment:{base['ticker']}")
                continue
            if not enriched:
                continue
            if enriched.get("price") is None or enriched.get("price", 0) < self.defaults["min_stock_price"]:
                continue
            liquidity_dollars    = enriched.get("liquidity_dollars")
            liquidity_supported  = enriched.get("liquidity_supported", False)
            if (
                liquidity_supported
                and liquidity_dollars is not None
                and liquidity_dollars < self.defaults["min_stock_liquidity"]
                and base.get("source_score", 0) < 28
            ):
                continue

            merged = {**base, **enriched}
            merged["reasons"] = sorted(list(base.get("reasons", set())))
            merged["prefilter_score"] = round(self._score_stock_context(merged), 1)

            # Tag with asset_type + market_cap_bucket
            ticker   = merged["ticker"]
            category = merged.get("category", "stock")
            profile  = merged.get("profile") or {}
            mcap     = _safe_float(profile.get("market_cap"))
            asset_type, mcap_bucket = _classify_candidate(ticker, mcap, category)
            merged["asset_type"]        = asset_type
            merged["market_cap_bucket"] = mcap_bucket

            final_rows.append(merged)

        final_rows.sort(key=lambda x: x.get("prefilter_score", 0), reverse=True)
        final_cut = final_rows[: self.defaults["prefilter_target"]]
        print(
            f"[UNIFIED_FLOW] Prefilter: {len(preliminary)} preliminary → "
            f"{len(final_rows)} enriched → {len(final_cut)} final candidates"
        )
        return {
            "candidates":       final_cut,
            "degraded_sources": degraded_sources,
            "macro":            macro,
        }

    # ── Contract filter: per-bucket relaxation ────────────────────────────────

    def _contract_filter(self, contract: dict, candidate: dict, *, tab: str = "master") -> bool:
        """
        Derive tier-specific thresholds from the candidate's market_cap_bucket
        tag (set during prefilter) rather than from the scan tab name.

        This preserves the existing per-tier liquidity relaxation:
          small  → 1/3 volume, 1.67× spread
          large  → 1/2 volume, 1.33× spread
          megacap/etf → full thresholds
        """
        bucket = candidate.get("market_cap_bucket", "unknown")
        if bucket == "small":
            effective_tab = "small_cap"
        elif bucket == "large":
            effective_tab = "large_cap"
        else:
            effective_tab = "megacap"
        return super()._contract_filter(contract, candidate, tab=effective_tab)

    # ── Inspect override: tag results with asset_type + market_cap_bucket ─────

    async def _inspect_one_ticker(
        self,
        candidate: dict,
        macro: dict,
        *,
        tab: str = "master",
        preloaded_expirations=None,
    ):
        """Delegate to parent and stamp asset_type / market_cap_bucket onto result."""
        result = await super()._inspect_one_ticker(
            candidate, macro, tab=tab, preloaded_expirations=preloaded_expirations,
        )
        if result is not None:
            result["asset_type"]        = candidate.get("asset_type", "stock")
            result["market_cap_bucket"] = candidate.get("market_cap_bucket", "unknown")
        return result
