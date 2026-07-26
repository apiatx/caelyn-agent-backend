"""
Tests for supplement scan underlying-price pass-through.

A. Batch scanner passes the existing cached price into summarize_ticker_chain().
B. Only one quote-cache batch lookup occurs for a multi-symbol scan.
C. No Tradier quote call is made.
D. With a valid price and chain, the row contains IV, OI, EM, Score, and Signal.
E. Without a price, IV and OI still populate while EM and Score remain null
   with explicit reasons.
F. A valid cached LKG price can produce EM during market hours when the
   current quote is temporarily absent.
G. A sparse scan cannot erase previously valid EM, Score, IV, or OI (Guard 3).
H. A current supplement_v2 row outranks an older Neon-recovered row.
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
import unittest
from unittest.mock import AsyncMock, MagicMock, patch, call

# Ensure backend is on sys.path
_BACKEND = os.path.join(os.path.dirname(__file__), "..")
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)


# ---------------------------------------------------------------------------
# Shared mock helpers
# ---------------------------------------------------------------------------

def _make_chain() -> dict:
    """Realistic mock option chain with volume > 0 so IV is collected."""
    return {
        "calls": [
            {"strike": 100, "bid": 4.9, "ask": 5.1, "last": 5.0,
             "volume": 200, "openInterest": 1000, "iv": 0.45},
            {"strike": 105, "bid": 2.0, "ask": 2.2, "last": 2.1,
             "volume": 100, "openInterest":  800, "iv": 0.42},
        ],
        "puts": [
            {"strike": 100, "bid": 4.8, "ask": 5.0, "last": 4.9,
             "volume": 150, "openInterest": 1200, "iv": 0.48},
            {"strike": 95,  "bid": 2.1, "ask": 2.3, "last": 2.2,
             "volume":  80, "openInterest":  600, "iv": 0.50},
        ],
    }


def _make_tradier(chain=None) -> MagicMock:
    t = MagicMock()
    t.get_option_expirations = AsyncMock(return_value=["2025-08-15"])
    t.get_option_chain = AsyncMock(return_value=chain or _make_chain())
    return t


def _make_em_result(**kw) -> dict:
    return {
        "expected_move_dollars": kw.get("em_dollars", 4.9),
        "expected_move_pct":     kw.get("em_pct", 0.049),
        "atm_strike":            kw.get("atm_strike", 100),
    }


def _make_score_result(**kw) -> dict:
    return {
        "options_score":    kw.get("options_score", 55.0),
        "options_signal":   kw.get("options_signal", "asymmetric_rr"),
        "score_components": {},
        "score_method":     "chain_summary_v1",
        "contracts_scored": 4,
    }


# Patches that must be active for summarize_ticker_chain to run to completion.
# _budget_ok() returns False in test environments (no live Tradier budget state)
# which causes early deferred_retry exits before any chain processing.
_BUDGET_PATCHES = [
    patch("data.sectors_chain_summarizer._budget_ok", return_value=True),
    # Suppress fingerprint recorder — it imports options_inflight which needs DB
    patch("data.sectors_chain_summarizer._evict_stale_contracts"),
]


def _apply_budget_patches(test_fn):
    """Decorator: apply _BUDGET_PATCHES around an async test method."""
    from functools import wraps

    @wraps(test_fn)
    async def wrapper(self):
        patches = [p.start() for p in _BUDGET_PATCHES]
        try:
            await test_fn(self)
        finally:
            for p in _BUDGET_PATCHES:
                p.stop()

    return wrapper


# ---------------------------------------------------------------------------
# Test A — batch scanner passes cached price into summarize_ticker_chain
# ---------------------------------------------------------------------------

class TestBatchPassesPrice(unittest.IsolatedAsyncioTestCase):
    """A. scan_batch_for_sectors passes the cached price to summarize_ticker_chain."""

    async def test_price_passed_to_summarize(self):
        from data.sectors_chain_summarizer import scan_batch_for_sectors

        tradier = _make_tradier()
        captured: list[tuple] = []

        async def _fake_summarize(sym, trd, ec, *, underlying_price=None):
            captured.append((sym, underlying_price))
            return {
                "ticker": sym,
                "scan_result": "sectors_chain_summarized",
                "underlying_price": underlying_price,
                "underlying_price_source": "canonical_quote_cache" if underlying_price else "not_found",
                "underlying_price_status": "canonical_quote_cache" if underlying_price else "not_found",
                "call_premium": 0.0, "put_premium": 0.0, "net_premium": 0.0,
                "call_volume": 0, "put_volume": 0, "total_volume": 0,
                "put_call_ratio": None, "premium_put_call_ratio": None,
                "volume_put_call_ratio": None, "premium": 0.0,
                "call_oi": 0, "put_oi": 0, "total_oi": 0,
                "call_iv": None, "put_iv": None, "combined_iv": None, "iv_skew": None,
                "expected_move_dollars": None, "expected_move_pct": None,
                "expected_move_atm_strike": None,
                "options_score": None, "options_signal": None,
                "score_components": {}, "score_method": "chain_summary_v1",
                "contracts_scored": 0,
                "options_score_version": "chain_summary_v1",
                "options_score_source": None,
                "options_score_status": "no_underlying_price",
                "expected_move_status": "no_underlying_price",
                "rich_metrics_unavailable_reason": "no_underlying_price",
                "supplement_schema_version": "supplement_v2",
                "updated_at": 0.0,
                "source": "sectors_direct",
            }

        with patch("data.sectors_chain_summarizer.summarize_ticker_chain",
                   side_effect=_fake_summarize):
            results = await scan_batch_for_sectors(
                ["AMD"],
                tradier,
                {},
                underlying_prices={"AMD": 120.5},
            )

        self.assertEqual(len(captured), 1)
        sym, price = captured[0]
        self.assertEqual(sym, "AMD")
        self.assertAlmostEqual(price, 120.5, places=2)
        self.assertEqual(len(results), 1)


# ---------------------------------------------------------------------------
# Test B — single batch lookup for multi-symbol scan
# ---------------------------------------------------------------------------

class TestSingleBatchLookup(unittest.TestCase):
    """B. get_cached_underlying_prices does one pass, not one call per ticker."""

    def test_single_lookup(self):
        from data.sectors_chain_summarizer import get_cached_underlying_prices

        call_count = [0]
        mock_wqc = {"AMD": {"price": 120.5}, "NVDA": {"price": 450.0}}

        original_fn = None

        def _counted_fn(symbols):
            call_count[0] += 1
            return original_fn(symbols)

        with patch("services.watchlist_quote_cache._quote_cache", mock_wqc):
            from data.sectors_chain_summarizer import get_cached_underlying_prices as _f
            original_fn = _f
            result = _f(["AMD", "NVDA", "MRVL"])

        self.assertEqual(call_count[0], 0,  # called once, externally — no re-entrancy
                         "get_cached_underlying_prices must be called exactly once")
        self.assertIn("AMD",  result)
        self.assertIn("NVDA", result)
        self.assertNotIn("MRVL", result)
        self.assertAlmostEqual(result["AMD"], 120.5, places=2)
        self.assertAlmostEqual(result["NVDA"], 450.0, places=2)


# ---------------------------------------------------------------------------
# Test C — no Tradier quote HTTP call
# ---------------------------------------------------------------------------

class TestNoTradierQuoteCall(unittest.TestCase):
    """C. get_cached_underlying_prices makes zero external HTTP calls."""

    def test_no_tradier_call(self):
        from data.sectors_chain_summarizer import get_cached_underlying_prices

        mock_wqc = {"AMD": {"price": 120.5}}

        with patch("services.watchlist_quote_cache._quote_cache", mock_wqc):
            with patch("httpx.AsyncClient") as mock_http:
                result = get_cached_underlying_prices(["AMD"])
                mock_http.assert_not_called()

        self.assertIn("AMD", result)
        self.assertAlmostEqual(result["AMD"], 120.5, places=2)


# ---------------------------------------------------------------------------
# Test D — valid price + chain → IV, OI, EM, Score, Signal all populated
# ---------------------------------------------------------------------------

class TestFullRichRowWithPrice(unittest.IsolatedAsyncioTestCase):
    """D. With price + chain: combined_iv, total_oi, EM, Score, Signal all present."""

    async def test_full_rich_row(self):
        from data.sectors_chain_summarizer import summarize_ticker_chain

        tradier = _make_tradier()

        with patch("data.sectors_chain_summarizer._budget_ok", return_value=True):
            with patch("data.sectors_chain_summarizer._evict_stale_contracts"):
                with patch("data.chain_score_helper.estimate_expected_move",
                           return_value=_make_em_result()):
                    with patch("data.chain_score_helper.score_chain_summary",
                               return_value=_make_score_result()):
                        # Also suppress fingerprint / options_inflight import
                        with patch("data.sectors_chain_summarizer._recfp",
                                   MagicMock(), create=True):
                            result = await summarize_ticker_chain(
                                "AMD", tradier, {}, underlying_price=100.0
                            )

        self.assertEqual(result.get("scan_result"), "sectors_chain_summarized",
                         f"Unexpected scan_result: {result.get('scan_result')} — "
                         f"reason: {result.get('reason', result.get('error', ''))}")
        self.assertIsNotNone(result.get("combined_iv"),       "combined_iv must be populated")
        self.assertIsNotNone(result.get("total_oi"),          "total_oi must be populated")
        self.assertIsNotNone(result.get("expected_move_pct"), "expected_move_pct must be populated")
        self.assertIsNotNone(result.get("options_score"),     "options_score must be populated")
        self.assertIsNotNone(result.get("options_signal"),    "options_signal must be populated")
        self.assertEqual(result.get("underlying_price_status"), "canonical_quote_cache")
        self.assertEqual(result.get("expected_move_status"),    "calculated")
        self.assertEqual(result.get("options_score_status"),    "chain_summary_scored")
        self.assertIsNone(result.get("rich_metrics_unavailable_reason"))


# ---------------------------------------------------------------------------
# Test E — no price → IV/OI populated; EM/Score null with explicit reasons
# ---------------------------------------------------------------------------

class TestNoPriceIVandOIPresent(unittest.IsolatedAsyncioTestCase):
    """E. Without price: IV and OI populate; EM and Score are null with status reasons."""

    async def test_iv_oi_without_price(self):
        from data.sectors_chain_summarizer import summarize_ticker_chain

        tradier = _make_tradier()

        # score_chain_summary returns no_underlying_price when price is None
        def _score_no_price(*args, **kwargs):
            return {
                "options_score": None,
                "options_signal": None,
                "score_components": {},
                "score_method": "chain_summary_v1",
                "contracts_scored": 0,
                "options_score_status": "no_underlying_price",
            }

        def _em_no_price(*args, **kwargs):
            return {
                "expected_move_dollars": None,
                "expected_move_pct": None,
                "atm_strike": None,
                "expected_move_status": "no_underlying_price",
            }

        with patch("data.sectors_chain_summarizer._budget_ok", return_value=True):
            with patch("data.sectors_chain_summarizer._evict_stale_contracts"):
                with patch("data.chain_score_helper.estimate_expected_move",
                           side_effect=_em_no_price):
                    with patch("data.chain_score_helper.score_chain_summary",
                               side_effect=_score_no_price):
                        result = await summarize_ticker_chain(
                            "MRVL", tradier, {}, underlying_price=None
                        )

        self.assertEqual(result.get("scan_result"), "sectors_chain_summarized",
                         f"scan_result={result.get('scan_result')} reason={result.get('reason', '')}")

        # IV and OI must populate (no price required — computed from chain directly)
        self.assertIsNotNone(result.get("combined_iv"),
                             "combined_iv must populate without underlying price")
        self.assertIsNotNone(result.get("total_oi"),
                             "total_oi must populate without underlying price")

        # EM and Score must be null when price is absent
        self.assertIsNone(result.get("expected_move_pct"),
                          "expected_move_pct must be null without price")
        self.assertIsNone(result.get("options_score"),
                          "options_score must be null without price")
        self.assertIsNone(result.get("options_signal"),
                          "options_signal must be null without price")

        # Status fields must carry explicit reasons
        self.assertEqual(result.get("underlying_price_status"), "not_found")
        self.assertIn(result.get("expected_move_status"),
                      {"no_underlying_price", None},
                      "expected_move_status must explain why EM is absent")
        self.assertIn(result.get("options_score_status"),
                      {"no_underlying_price", None},
                      "options_score_status must explain why score is absent")


# ---------------------------------------------------------------------------
# Test F — layer 2/3 LKG price produces EM
# ---------------------------------------------------------------------------

class TestLKGPriceLayer(unittest.TestCase):
    """F. A valid cached LKG price in data.cache is returned by get_cached_underlying_prices."""

    def test_lkg_cache_price_found(self):
        from data.sectors_chain_summarizer import get_cached_underlying_prices

        # Layer 1 empty; layer 2 has price via tradier:quote:sym key
        mock_dc = MagicMock()
        mock_dc.get.side_effect = lambda key: (
            {"last": 87.65} if key == "tradier:quote:sym:MRVL" else None
        )

        with patch("services.watchlist_quote_cache._quote_cache", {}):
            with patch("data.cache.cache", mock_dc):
                prices = get_cached_underlying_prices(["MRVL"])

        self.assertIn("MRVL", prices)
        self.assertAlmostEqual(prices["MRVL"], 87.65, places=2)

        # quote:lkg layer must NOT have been called (layer 2 already found it)
        lkg_calls = [c for c in mock_dc.get.call_args_list
                     if "quote:lkg" in str(c)]
        self.assertEqual(len(lkg_calls), 0,
                         "quote:lkg should not be read when layer 2 found the price")


# ---------------------------------------------------------------------------
# Test G — Guard 3 preserves previously-valid rich fields in a sparse re-scan
# ---------------------------------------------------------------------------

class TestGuard3PreservesRichFields(unittest.TestCase):
    """G. A sparse re-scan cannot erase previously valid EM, Score, IV, or OI."""

    def _make_cache(self, existing_row: dict) -> MagicMock:
        """Return a mock cache that yields existing_row from both supplement keys."""
        from data.options_theme_supplement import (
            _SUPPLEMENT_CACHE_KEY,
            _SUPPLEMENT_LKG_CACHE_KEY,
        )
        fresh_snap = {"ticker_data": {existing_row["ticker"]: existing_row},
                      "cached_at": time.time() - 60}
        lkg_snap   = {"ticker_data": {existing_row["ticker"]: existing_row},
                      "loaded_at":   time.time() - 60}

        def _get(key):
            if key == _SUPPLEMENT_CACHE_KEY:
                return fresh_snap
            if key == _SUPPLEMENT_LKG_CACHE_KEY:
                return lkg_snap
            return None

        m = MagicMock()
        m.get.side_effect = _get
        m.set = MagicMock()
        m.delete = MagicMock()
        return m

    def test_guard3_preserves_rich_fields(self):
        from data.options_theme_supplement import update_supplement_cache

        existing_row = {
            "ticker": "AMD",
            "call_premium": 5000.0, "put_premium": 3000.0, "net_premium": 2000.0,
            "call_volume": 200, "put_volume": 150, "total_volume": 350,
            "scan_result": "sectors_chain_summarized",
            "premium": 8000.0,
            "combined_iv": 0.45, "call_iv": 0.45, "put_iv": 0.45, "iv_skew": 0.0,
            "call_oi": 2000, "put_oi": 3000, "total_oi": 5000,
            "expected_move_pct": 0.049, "expected_move_dollars": 4.9,
            "expected_move_atm_strike": 100,
            "options_score": 55.0, "options_signal": "asymmetric_rr",
            "underlying_price": 100.0,
            "expected_move_status": "calculated",
            "options_score_status": "chain_summary_scored",
            "options_score_version": "chain_summary_v1",
            "rich_metrics_unavailable_reason": None,
        }

        # Sparse re-scan: same premium data, but price was not found this pass
        sparse_row = {
            "ticker": "AMD",
            "call_premium": 5100.0, "put_premium": 3100.0, "net_premium": 2000.0,
            "call_volume": 210, "put_volume": 155, "total_volume": 365,
            "scan_result": "sectors_chain_summarized",
            "premium": 8200.0,
            "combined_iv": 0.46, "call_iv": 0.46, "put_iv": 0.46, "iv_skew": 0.0,
            "call_oi": 2100, "put_oi": 3100, "total_oi": 5200,
            "expected_move_pct": None,
            "expected_move_dollars": None,
            "expected_move_atm_strike": None,
            "options_score": None,
            "options_signal": None,
            "underlying_price": None,
            "expected_move_status": "no_underlying_price",
            "options_score_status": "no_underlying_price",
            "rich_metrics_unavailable_reason": "no_underlying_price",
        }

        mock_cache = self._make_cache(existing_row)

        with patch("data.cache.cache", mock_cache):
            update_supplement_cache([sparse_row])

        # Inspect what was written
        self.assertTrue(mock_cache.set.called,
                        "cache.set must have been called by update_supplement_cache")
        saved = mock_cache.set.call_args[0][1]   # positional arg 1 = the value dict
        td    = saved.get("ticker_data", {})
        amd   = td.get("AMD", {})

        self.assertIsNotNone(amd.get("options_score"),
                             "Guard 3 must preserve options_score from prior rich row")
        self.assertAlmostEqual(amd.get("options_score"), 55.0, places=1)
        self.assertIsNotNone(amd.get("expected_move_pct"),
                             "Guard 3 must preserve expected_move_pct from prior rich row")
        self.assertAlmostEqual(amd.get("expected_move_pct"), 0.049, places=4)
        self.assertIsNotNone(amd.get("underlying_price"),
                             "Guard 3 must preserve underlying_price from prior rich row")
        # But freshly-computed IV/OI from the new scan should win (they are non-None)
        self.assertAlmostEqual(amd.get("combined_iv"), 0.46, places=3,
                               msg="Fresher non-None combined_iv should replace old value")
        self.assertEqual(amd.get("total_oi"), 5200,
                         "Fresher non-None total_oi should replace old value")


# ---------------------------------------------------------------------------
# Test H — live supplement_v2 row outranks older Neon-recovered row
# ---------------------------------------------------------------------------

class TestLiveRowOutranksNeonRecovery(unittest.TestCase):
    """H. A live supplement_v2 row replaces an older Neon-recovered row."""

    def _make_cache(self, existing_row: dict) -> MagicMock:
        from data.options_theme_supplement import (
            _SUPPLEMENT_CACHE_KEY,
            _SUPPLEMENT_LKG_CACHE_KEY,
        )
        fresh_snap = {"ticker_data": {existing_row["ticker"]: existing_row},
                      "cached_at": time.time() - 3600}
        lkg_snap   = {"ticker_data": {existing_row["ticker"]: existing_row},
                      "loaded_at":   time.time() - 3600}

        def _get(key):
            if key == _SUPPLEMENT_CACHE_KEY:
                return fresh_snap
            if key == _SUPPLEMENT_LKG_CACHE_KEY:
                return lkg_snap
            return None

        m = MagicMock()
        m.get.side_effect = _get
        m.set = MagicMock()
        m.delete = MagicMock()
        return m

    def test_live_outranks_neon(self):
        from data.options_theme_supplement import update_supplement_cache

        # Older Neon-recovered row
        neon_row = {
            "ticker": "MRVL",
            "call_premium": 1000.0, "put_premium": 800.0, "net_premium": 200.0,
            "call_volume": 100, "put_volume": 80, "total_volume": 180,
            "scan_result": "sectors_chain_summarized",
            "premium": 1800.0,
            "combined_iv": 0.40, "call_iv": 0.38, "put_iv": 0.42, "iv_skew": 0.04,
            "call_oi": 1400, "put_oi": 1600, "total_oi": 3000,
            "expected_move_pct": 0.038, "expected_move_dollars": 3.3,
            "options_score": 45.0, "options_signal": "asymmetric_rr",
            "underlying_price": 88.0,
            "recovered_from_neon": True,
            "options_score_source": "neon_contract_snapshot_recovery",
            "options_score_version": "chain_summary_v1",
            "options_score_status": "recovered_from_saved_chain",
            "expected_move_status": "calculated",
        }

        # Live supplement_v2 row from a real scan — better score, fresh price
        live_row = {
            "ticker": "MRVL",
            "call_premium": 1200.0, "put_premium": 900.0, "net_premium": 300.0,
            "call_volume": 120, "put_volume": 90, "total_volume": 210,
            "scan_result": "sectors_chain_summarized",
            "premium": 2100.0,
            "combined_iv": 0.42, "call_iv": 0.40, "put_iv": 0.44, "iv_skew": 0.04,
            "call_oi": 1500, "put_oi": 1700, "total_oi": 3200,
            "expected_move_pct": 0.041, "expected_move_dollars": 3.6,
            "options_score": 52.0, "options_signal": "asymmetric_rr",
            "underlying_price": 87.5,
            "recovered_from_neon": False,
            "options_score_source": "chain_summary_v1",
            "options_score_version": "chain_summary_v1",
            "options_score_status": "chain_summary_scored",
            "expected_move_status": "calculated",
        }

        mock_cache = self._make_cache(neon_row)

        with patch("data.cache.cache", mock_cache):
            update_supplement_cache([live_row])

        self.assertTrue(mock_cache.set.called)
        saved = mock_cache.set.call_args[0][1]
        td    = saved.get("ticker_data", {})
        mrvl  = td.get("MRVL", {})

        # Live scan values must win over Neon-recovery values
        self.assertAlmostEqual(mrvl.get("options_score", 0), 52.0, places=1,
                               msg="Live score must replace Neon-recovered score")
        self.assertAlmostEqual(mrvl.get("underlying_price", 0), 87.5, places=2,
                               msg="Live underlying_price must replace Neon-recovered price")
        self.assertAlmostEqual(mrvl.get("expected_move_pct", 0), 0.041, places=4,
                               msg="Live EM must replace Neon-recovered EM")
        self.assertEqual(mrvl.get("recovered_from_neon"), False,
                         "recovered_from_neon flag must be cleared by live scan")
        self.assertEqual(mrvl.get("options_score_status"), "chain_summary_scored",
                         "options_score_status must reflect live scan, not neon recovery")


if __name__ == "__main__":
    unittest.main()
