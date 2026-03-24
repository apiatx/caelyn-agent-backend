"""
Tradier-specific options flow engine.

Subclasses OptionsFlowEngine and redirects the two options-data calls
(expirations + chain) from Public.com → Tradier. All scoring, signal
classification, prefilter, and pipeline logic is inherited unchanged.

This also exposes Tradier-specific enrichments: contract history,
time-and-sales, and richer greeks/IV fields.
"""
from __future__ import annotations

from data.options_flow_engine import OptionsFlowEngine

try:
    from langsmith import traceable
except ImportError:
    def traceable(*args, **kwargs):
        def _noop(fn):
            return fn
        if args and callable(args[0]):
            return args[0]
        return _noop


class TradierFlowEngine(OptionsFlowEngine):
    """
    Drop-in replacement for OptionsFlowEngine that uses Tradier
    instead of Public.com for options data.

    The parent class calls:
      self.data.public_com.get_option_expirations(symbol)
      self.data.public_com.get_full_chain_with_greeks(symbol, exp)

    We intercept _inspect_one_ticker to use self.data.tradier instead,
    while keeping all scoring/ranking logic identical.
    """

    def __init__(self, data_service, overrides: dict | None = None):
        super().__init__(data_service, overrides=overrides)
        if not data_service.tradier:
            raise RuntimeError("TradierFlowEngine requires data_service.tradier to be configured (set TRADIER_API_KEY)")

    @traceable(name="tradier_flow_engine.run_live_scan")
    async def run_live_scan(
        self,
        seed_tickers: list[str] | None = None,
        prefilter_snapshot: dict | None = None,
        tab: str = "megacap",
    ) -> dict:
        # Temporarily swap public_com → tradier so parent methods use Tradier
        original_public_com = self.data.public_com
        self.data.public_com = self.data.tradier
        try:
            result = await super().run_live_scan(
                seed_tickers=seed_tickers,
                prefilter_snapshot=prefilter_snapshot,
                tab=tab,
            )
            # Tag the response so the frontend knows this is Tradier-backed
            result["data_source"] = "tradier"
            return result
        finally:
            # Restore original to avoid side effects on Options Flow page
            self.data.public_com = original_public_com

    @traceable(name="tradier_flow_engine.build_prefilter_snapshot")
    async def build_prefilter_snapshot(
        self,
        seed_tickers: list[str] | None = None,
        tab: str = "megacap",
        exclude_tickers: set[str] | None = None,
    ) -> dict:
        # Prefilter doesn't use Public.com — it uses Finviz/FMP/Finnhub/FRED
        # So we can just call super() directly
        return await super().build_prefilter_snapshot(
            seed_tickers=seed_tickers,
            tab=tab,
            exclude_tickers=exclude_tickers,
        )
