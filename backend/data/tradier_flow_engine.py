"""
Tradier-powered options flow engine.

Subclasses OptionsFlowEngine and:
  1. Calls Tradier directly for expirations + chains (no monkey-patching).
  2. Preserves Tradier-specific enrichments: bid_iv, ask_iv, smv_vol, rho,
     change, average_volume, intraday OHLC on each contract.
  3. Integrates Polygon Massive historical data from PostgreSQL into the
     volatility and flow scoring pipeline (IV percentile from longer history,
     historical volume baselines).
  4. Serves as the sole engine for the Options Flow dashboard.
"""
from __future__ import annotations

import asyncio
from typing import Any

from data.options_flow_engine import (
    ETF_SET,
    OptionsFlowEngine,
    _clip,
    _days_to_expiration,
    _midpoint,
    _safe_float,
    _safe_int,
    _spread_pct,
)
from data.options_history_store import (
    get_contract_flow_history_summary,
    get_latest_technicals,
    get_options_volume_summary,
)



class _TradierCountingProxy:
    """
    Transparent proxy around the Tradier provider that:
      1. Counts every async API call for budget logging.
      2. Enforces a token-bucket rate limit (default: 100 req/min, burst 6)
         so scans never exceed Tradier's 120 req/min vendor cap.

    Token bucket behaviour
    ──────────────────────
    Tokens refill at `_RL_RATE` per second.  Each call consumes one token.
    If the bucket is empty, the caller awaits until a token is available.
    Burst capacity (`_RL_BUCKET`) allows a short burst of concurrent calls
    at start-up without paying the per-call wait for every request.
    """

    _RL_RATE  = 115.0 / 60.0   # tokens per second  ≈ 1.917  (115 req/min)
    _RL_BUCKET = 12.0           # burst capacity — 12 free calls before throttling kicks in

    __slots__ = (
        "_wrapped", "call_count", "call_log",
        "_rl_tokens", "_rl_last", "_rl_lock",
    )

    def __init__(self, wrapped: Any) -> None:
        import time as _t
        self._wrapped    = wrapped
        self.call_count: int       = 0
        self.call_log:   list[str] = []
        self._rl_tokens: float     = self._RL_BUCKET
        self._rl_last:   float     = _t.monotonic()
        self._rl_lock:   asyncio.Lock = asyncio.Lock()

    async def _rate_limit(self) -> None:
        import time as _t
        while True:
            async with self._rl_lock:
                now = _t.monotonic()
                elapsed = now - self._rl_last
                self._rl_tokens = min(
                    self._RL_BUCKET,
                    self._rl_tokens + elapsed * self._RL_RATE,
                )
                self._rl_last = now
                if self._rl_tokens >= 1.0:
                    self._rl_tokens -= 1.0
                    return
                wait_s = (1.0 - self._rl_tokens) / self._RL_RATE
            await asyncio.sleep(wait_s)

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._wrapped, name)
        if asyncio.iscoroutinefunction(attr):
            proxy = self

            async def _counted(*args: Any, **kwargs: Any) -> Any:
                await proxy._rate_limit()
                proxy.call_count += 1
                proxy.call_log.append(name)
                return await attr(*args, **kwargs)

            return _counted
        return attr


class TradierFlowEngine(OptionsFlowEngine):
    """
    Production options flow engine backed by Tradier for live data
    and Polygon Massive (via PostgreSQL) for historical context.

    Overrides:
      - _inspect_one_ticker: calls Tradier directly, enriches with Polygon DB
      - _normalize_contract: preserves Tradier-specific IV/greek fields
      - _contract_response: exposes richer fields to the frontend
      - _score_volatility: uses Polygon historical IV when available
    """

    def __init__(self, data_service, overrides: dict | None = None):
        super().__init__(data_service, overrides=overrides)
        if not data_service.tradier:
            raise RuntimeError(
                "TradierFlowEngine requires data_service.tradier to be configured (set TRADIER_API_KEY)"
            )
        self._tradier = data_service.tradier
        # Populated after each run_live_scan — reflects actual Tradier call count
        self.last_tradier_req_count: int = 0
        self.last_tradier_req_per_min: float = 0.0

    # ── Live scan ──────────────────────────────────────────────────────
    async def run_live_scan(
        self,
        seed_tickers: list[str] | None = None,
        prefilter_snapshot: dict | None = None,
        tab: str = "megacap",
    ) -> dict:
        """
        Run the full pipeline using Tradier for options data.

        Wraps the Tradier client in a counting proxy so every API call is
        tallied and logged.  The observed requests/min is stored in
        self.last_tradier_req_per_min for the caller to inspect.
        """
        import time as _t
        proxy = _TradierCountingProxy(self._tradier)

        # Store proxy as a per-instance attribute so base-class methods can use
        # it directly — avoids a shared data_service.public_com race condition
        # when all 4 tabs run concurrently (each engine is its own instance).
        self._scan_proxy = proxy
        # Also replace self._tradier so direct calls in _inspect_one_ticker
        # (e.g. get_quote for price backfill) are counted too.
        self._tradier = proxy

        t0 = _t.time()
        try:
            result = await super().run_live_scan(
                seed_tickers=seed_tickers,
                prefilter_snapshot=prefilter_snapshot,
                tab=tab,
            )
            result["data_source"] = "tradier"
            return result
        finally:
            elapsed = max(_t.time() - t0, 0.001)
            self.last_tradier_req_count = proxy.call_count
            self.last_tradier_req_per_min = round(proxy.call_count / elapsed * 60, 1)
            # Clear proxy and restore original tradier ref
            self._scan_proxy = None
            self._tradier = proxy._wrapped
            # Method-level breakdown for auditing unexpected call volumes
            from collections import Counter as _Counter
            method_counts = _Counter(proxy.call_log)
            breakdown = ", ".join(f"{m}×{n}" for m, n in sorted(method_counts.items()))
            print(
                f"[TRADIER_RATE] [{tab}] {proxy.call_count} API requests in {elapsed:.1f}s "
                f"≈ {self.last_tradier_req_per_min} req/min "
                f"(limit: 120/min, budget used: {round(self.last_tradier_req_per_min/120*100)}%) "
                f"[{breakdown}]"
            )

    # ── Prefilter (unchanged — uses Finviz/FMP/Finnhub/FRED) ─────────
    async def build_prefilter_snapshot(
        self,
        seed_tickers: list[str] | None = None,
        tab: str = "megacap",
        exclude_tickers: set[str] | None = None,
    ) -> dict:
        seeds = seed_tickers or []

        # Pre-fetch Tradier quotes for ALL seeds in one batch call.
        # This ensures seeds have prices even when Finnhub/FMP are
        # rate-limited during cold starts.
        seed_quotes: dict[str, dict] = {}
        if seeds:
            try:
                from data.tradier_budget import lane as _fe_lane
                with _fe_lane("options_flow"):
                    raw_quotes = await self._tradier.get_quotes(seeds)
                for q in raw_quotes or []:
                    sym = (q.get("symbol") or "").upper()
                    if sym and _safe_float(q.get("last")):
                        seed_quotes[sym] = q
            except Exception as exc:
                print(f"[TRADIER_FLOW] Seed quote pre-fetch failed: {exc}")

        result = await super().build_prefilter_snapshot(
            seed_tickers=seeds, tab=tab, exclude_tickers=exclude_tickers,
        )

        candidates = result.get("candidates", [])
        existing_tickers = {c["ticker"] for c in candidates}

        # Backfill prices for candidates that survived but have price=None
        for c in candidates:
            if c.get("price") is None and c["ticker"] in seed_quotes:
                q = seed_quotes[c["ticker"]]
                c["price"] = _safe_float(q["last"])
                c["change_pct"] = _safe_float(q.get("change_percentage"))

        # Re-add seed tickers that were dropped by the base pipeline
        # (e.g. price check filtered them out because Finnhub was down).
        # Cap at prefilter_target to prevent inflating the candidate list —
        # a large seed list (24+ tickers) would otherwise cause all of them to be
        # re-added and then inspected, multiplying Tradier calls.
        # Prioritise by trading volume so the most active seeds survive.
        dropped_seeds = [
            s for s in seeds
            if s not in existing_tickers and s in seed_quotes
        ]
        dropped_seeds.sort(
            key=lambda s: _safe_int(seed_quotes[s].get("volume")) or 0,
            reverse=True,
        )
        dropped_seeds = dropped_seeds[: self.defaults["prefilter_target"]]

        for seed in dropped_seeds:
            q = seed_quotes[seed]
            candidates.append({
                "ticker": seed,
                "price": _safe_float(q["last"]),
                "change_pct": _safe_float(q.get("change_percentage")),
                "volume": _safe_int(q.get("volume")) or 0,
                "avg_volume": _safe_int(q.get("average_volume")),
                "category": "etf" if seed in ETF_SET else "stock",
                "source_score": 5.0,
                "source_hits": ["seed_watchlist"],
                "reasons": ["watchlist inclusion"],
                "prefilter_score": 5.0,
                "profile": {},
                "technicals": {},
                "stock_relative_volume": None,
                "liquidity_dollars": None,
                "liquidity_supported": False,
            })

        if seed_quotes:
            added = len(candidates) - len(existing_tickers)
            backfilled = sum(1 for c in candidates if c["ticker"] in seed_quotes and c["ticker"] in existing_tickers)
            print(f"[TRADIER_FLOW] [{tab}] Seed quotes: {len(seed_quotes)} fetched, {backfilled} backfilled, {added} re-added")

        result["candidates"] = candidates
        return result

    # ── Contract normalisation — preserves Tradier-specific fields ────

    def _normalize_contract(
        self, symbol: str, side: str, expiration: str, raw: dict, spot_price: float
    ) -> dict | None:
        """
        Override parent to keep Tradier's richer IV/greeks fields:
        bid_iv, ask_iv, smv_vol, rho, change, average_volume, intraday OHLC.
        """
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

        spread_pct_val = _spread_pct(bid, ask, midpoint)

        # Core IV — Tradier provides mid_iv as "iv", plus bid_iv, ask_iv, smv_vol
        implied_vol = _safe_float(raw.get("iv"))
        bid_iv = _safe_float(raw.get("bid_iv"))
        ask_iv = _safe_float(raw.get("ask_iv"))
        smv_vol = _safe_float(raw.get("smv_vol"))

        delta = _safe_float(raw.get("delta"))
        gamma = _safe_float(raw.get("gamma"))
        theta = _safe_float(raw.get("theta"))
        vega = _safe_float(raw.get("vega"))
        rho = _safe_float(raw.get("rho"))

        vol_oi_ratio = round(volume / open_interest, 2) if open_interest else None
        premium_traded_estimate = round(volume * midpoint * 100.0, 2)

        if side == "call":
            break_even = strike + midpoint
        else:
            break_even = strike - midpoint
        break_even_distance_pct = (
            round(((break_even - spot_price) / spot_price) * 100.0, 2) if spot_price else None
        )
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
            # IV — richer than Public.com (4 variants vs 1)
            "implied_volatility": implied_vol,
            "bid_iv": bid_iv,
            "ask_iv": ask_iv,
            "smv_vol": smv_vol,
            # Greeks — includes rho (Tradier-only)
            "delta": delta,
            "gamma": gamma,
            "theta": theta,
            "vega": vega,
            "rho": rho,
            # Derived metrics
            "option_volume_to_oi_ratio": vol_oi_ratio,
            "spread_pct": spread_pct_val,
            "premium_traded_estimate": premium_traded_estimate,
            "break_even": round(break_even, 4),
            "break_even_distance_pct": break_even_distance_pct,
            "moneyness_pct": round(moneyness_pct, 4) if moneyness_pct is not None else None,
            "abs_delta": abs_delta,
            "liquidity_quality": round(self._score_contract_liquidity(volume, open_interest, spread_pct_val), 1),
            # Tradier extras
            "change": _safe_float(raw.get("change")),
            "change_percentage": _safe_float(raw.get("change_percentage")),
            "average_volume": _safe_int(raw.get("average_volume")),
            "last_volume": _safe_int(raw.get("last_volume")),
            "open": _safe_float(raw.get("open")),
            "high": _safe_float(raw.get("high")),
            "low": _safe_float(raw.get("low")),
            "close": _safe_float(raw.get("close")),
            "greeks_updated_at": raw.get("greeks_updated_at"),
        }

    # ── Contract response — expose Tradier-specific fields to frontend ─

    def _contract_response(self, symbol: str, contract: dict, primary_signal: str) -> dict:
        """Override parent to include Tradier-specific IV/greeks + Polygon history."""
        base = super()._contract_response(symbol, contract, primary_signal)
        # ── Tradier IV fields ─────────────────────────────────────────────
        # Tradier's "iv" field is the mid IV; expose it under all three names
        # so the frontend can use whichever key it expects.
        mid_iv_val = contract.get("implied_volatility")
        base["bid_iv"]  = contract.get("bid_iv")
        base["mid_iv"]  = mid_iv_val
        base["ask_iv"]  = contract.get("ask_iv")
        base["smv_vol"] = contract.get("smv_vol")
        # ── Tradier Greeks extras ─────────────────────────────────────────
        base["rho"]    = contract.get("rho")
        base["change"] = contract.get("change")
        base["change_percentage"]  = contract.get("change_percentage")
        base["average_volume"]     = contract.get("average_volume")
        base["greeks_updated_at"]  = contract.get("greeks_updated_at")
        # greeks_source: "tradier" when at least one Greek is populated;
        # null-safe — contracts without greeks data still get the key.
        has_greeks = (
            contract.get("delta") is not None
            or contract.get("bid_iv") is not None
        )
        base["greeks_source"] = "tradier" if has_greeks else None
        # Include rho + greeks_source in the nested greeks block too
        if base.get("greeks"):
            base["greeks"]["rho"]          = contract.get("rho")
            base["greeks"]["greeks_source"] = base["greeks_source"]
        # Polygon-sourced historical context (if available in DB)
        base["polygon_history"] = contract.get("_polygon_history")
        return base

    # ── Volatility scoring — enhanced with Polygon historical IV ──────

    def _score_volatility(
        self, candidate: dict, iv_current: float | None, best_contract: dict
    ) -> float:
        """
        Override parent to use richer IV data from Tradier (smv_vol, bid/ask IV spread)
        and longer-horizon IV history from Polygon when available.
        """
        score = 0.0

        # Context bonuses (same as parent)
        if candidate.get("compression_context"):
            score += 24
        if candidate.get("catalyst_context"):
            score += 22

        # Use smv_vol (smoothed volatility from ORATS via Tradier) when available
        # — more reliable than raw mid_iv for scoring
        effective_iv = best_contract.get("smv_vol") or iv_current
        if effective_iv is not None:
            if effective_iv <= 0.35:
                score += 18
            elif effective_iv <= 0.60:
                score += 12
            else:
                score += 8

        # IV spread signal: wide bid_iv-ask_iv spread suggests uncertainty/opportunity
        bid_iv = best_contract.get("bid_iv")
        ask_iv = best_contract.get("ask_iv")
        if bid_iv is not None and ask_iv is not None and bid_iv > 0:
            iv_spread_ratio = (ask_iv - bid_iv) / bid_iv
            if iv_spread_ratio > 0.15:
                score += 6  # Wide IV spread — potential mispricing

        # IV percentile from flow snapshot history (Polygon enrichment applied post-scoring)
        iv_percentile = best_contract.get("iv_percentile")
        used_pctile = iv_percentile

        if used_pctile is not None:
            # Distance from 50th percentile — extremes in either direction are interesting
            score += _clip(abs(50 - used_pctile) * 0.6, 0, 20)
        else:
            score += 6  # Default when no history

        return _clip(score)

    # ── Ticker inspection — Tradier + Polygon enrichment ─────────────

    async def _inspect_one_ticker(
        self,
        candidate: dict,
        macro: dict,
        *,
        tab: str = "megacap",
        preloaded_expirations: list[str] | None = None,
        _skip_polygon: bool = False,
    ) -> dict | None:
        """
        Override parent to:
        1. Backfill missing price from Tradier (seed tickers may lack Finnhub/FMP price)
        2. Call Tradier directly for expirations + chains
        3. Unless _skip_polygon=True, enrich with Polygon historical data from DB
        4. Re-score volatility with the enriched data

        preloaded_expirations: When provided (2-stage path), the parent skips
            get_option_expirations — it was already called in Stage 1.
        _skip_polygon: When True, skip the synchronous Polygon DB enrichment so
            the Stage-2 semaphore slot can be released faster.  Caller is
            responsible for calling _enrich_polygon_async(result) afterwards.
        """
        # Backfill price from Tradier if enrichment didn't provide one
        if not _safe_float(candidate.get("price")):
            try:
                quote = await self._tradier.get_quote(candidate["ticker"])
                if quote and _safe_float(quote.get("last")):
                    candidate["price"] = _safe_float(quote["last"])
                    candidate["change_pct"] = _safe_float(quote.get("change_percentage"))
            except Exception:
                pass  # Will fail in parent if still no price

        # Call parent — which now hits Tradier via the swap in run_live_scan
        result = await super()._inspect_one_ticker(
            candidate, macro, tab=tab, preloaded_expirations=preloaded_expirations,
        )
        if result is None:
            return None

        result["data_source"] = "tradier"

        if _skip_polygon:
            # Caller will enrich asynchronously outside the semaphore
            return result

        return await self._enrich_polygon_async(result)

    async def _enrich_polygon_async(self, result: dict) -> dict:
        """
        Enrich a scored ticker result with Polygon historical DB data.

        Designed to be called OUTSIDE the Stage-2 Tradier semaphore. The caller
        applies the options-history DB concurrency bound.

        Non-fatal: if any DB call fails the result is returned unmodified.
        """
        symbol = result["ticker"]

        polygon_vol_summary: dict = {}
        polygon_technicals: dict = {}
        volume_result: dict = {}
        technicals_result: dict = {}
        initial_read_failed = False
        try:
            volume_result = await asyncio.to_thread(
                get_options_volume_summary,
                symbol,
                30,
            )
        except Exception:
            initial_read_failed = True
        try:
            technicals_result = await asyncio.to_thread(
                get_latest_technicals,
                symbol,
            )
        except Exception:
            initial_read_failed = True
        if not initial_read_failed:
            polygon_vol_summary = volume_result or {}
            polygon_technicals = technicals_result or {}

        # Attach Polygon historical data to the result
        if polygon_technicals and len(polygon_technicals) > 1:
            result["technicals"] = polygon_technicals
        if polygon_vol_summary and polygon_vol_summary.get("call_total_volume"):
            result["historic_volume"] = polygon_vol_summary

        # ── Enrich top contracts with Polygon historical IV context ──
        top_contracts_data = result.get("top_contracts", [])
        if top_contracts_data:
            occ_syms = [
                c.get("contract_symbol") or c.get("symbol")
                for c in top_contracts_data
            ]

            async def _fetch_polygon_history(occ_sym: str | None) -> dict | None:
                if not occ_sym:
                    return None
                try:
                    return await asyncio.to_thread(_polygon_iv_context, occ_sym)
                except Exception:
                    return None

            histories = []
            for occ_sym in occ_syms:
                histories.append(await _fetch_polygon_history(occ_sym))
            for contract_resp, polygon_history in zip(top_contracts_data, histories):
                if polygon_history:
                    contract_resp["polygon_history"] = polygon_history

        # ── Refine volatility score using Polygon 30-day volume history ──
        if polygon_vol_summary:
            call_avg = polygon_vol_summary.get("call_avg_daily_vol", 0)
            put_avg = polygon_vol_summary.get("put_avg_daily_vol", 0)
            hist_avg_total = (call_avg or 0) + (put_avg or 0)
            current_total = (result.get("call_volume", 0) or 0) + (result.get("put_volume", 0) or 0)
            if hist_avg_total > 0 and current_total > 0:
                volume_surge_ratio = current_total / hist_avg_total
                if volume_surge_ratio > 2.0:
                    surge_bonus = min(volume_surge_ratio * 2, 8)
                    old_composite = result.get("composite_score", 0)
                    result["composite_score"] = round(min(100, old_composite + surge_bonus), 1)
                    result["polygon_volume_surge_ratio"] = round(volume_surge_ratio, 2)

        return result

    # ── Ticker-level IV averaging — use smv_vol when available ───────

    def _avg_iv(self, contracts: list[dict]) -> float | None:
        """Prefer Tradier's smv_vol (smoothed) for averages, fall back to mid_iv."""
        ivs = []
        for c in contracts:
            iv = c.get("smv_vol") or c.get("implied_volatility")
            if iv is not None:
                ivs.append(iv)
        if not ivs:
            return None
        return round(sum(ivs) / len(ivs), 4)


def _polygon_iv_context(contract_symbol: str) -> dict | None:
    """
    Pull historical IV data from the Polygon-ingested options_history table
    to compute a longer-horizon IV percentile (90-day lookback).
    """
    try:
        from data.options_history_store import _get_conn, _put_conn
    except ImportError:
        return None

    conn = _get_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        # Polygon stores option_ticker as 'O:AAPL250321C00200000'
        # Tradier/OCC uses 'AAPL250321C00200000' — try both formats
        polygon_ticker = f"O:{contract_symbol}" if not contract_symbol.startswith("O:") else contract_symbol
        cur.execute("""
            SELECT trade_date, volume, close
            FROM public.options_history
            WHERE option_ticker = %s
              AND trade_date >= CURRENT_DATE - INTERVAL '90 days'
            ORDER BY trade_date DESC
            LIMIT 90
        """, (polygon_ticker,))
        rows = cur.fetchall()
        cur.close()

        if len(rows) < 5:
            return None

        volumes = [int(r[1]) for r in rows if r[1] is not None and r[1] > 0]
        closes = [float(r[2]) for r in rows if r[2] is not None and r[2] > 0]

        result = {"trading_days": len(rows)}

        if len(volumes) >= 5:
            latest_vol = volumes[0]
            avg_vol = sum(volumes[1:]) / len(volumes[1:])
            result["avg_daily_volume_90d"] = round(avg_vol, 0)
            result["volume_vs_avg"] = round(latest_vol / avg_vol, 2) if avg_vol > 0 else None

        if len(closes) >= 20:
            latest_close = closes[0]
            pctile = sum(1 for c in closes if c <= latest_close) / len(closes)
            result["iv_percentile_90d"] = round(pctile * 100, 1)

        return result if len(result) > 1 else None

    except Exception:
        return None
    finally:
        _put_conn(conn)
