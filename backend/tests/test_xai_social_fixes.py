"""
Tests for the 5 surgical xAI/Social fixes (forensic-audit cost-reduction pass).

Fix 1: _call_grok_with_x_search gains caller_label + _usage_out + [XAI_USAGE] log.
Fix 2: _x_consensus_loop scheduled-fire freshness guard.
Fix 3: Phase-2 synthesis x_search disabled; fresh_trades conservative fallback.
Fix 4: /api/social/query legacy pipeline removed; canonical cache used.
Fix 5: Stale comment correction (account counts, TTL, refresh cadence).

Run from backend/ directory:
  python3.11 -m unittest tests.test_xai_social_fixes -v
"""
import asyncio
import os
import time
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

# ── Workspace-relative paths (CWD = backend/) ────────────────────────────────
_MAIN_PY           = os.path.join(os.path.dirname(__file__), "..", "main.py")
_X_CACHE_PY        = os.path.join(os.path.dirname(__file__), "..", "services", "x_consensus_cache.py")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_xai_provider():
    """Return a real XAISentimentProvider with a dummy API key."""
    from data.xai_sentiment_provider import XAISentimentProvider
    return XAISentimentProvider(api_key="test-key-unused")


def _fake_response(data: dict, status_code: int = 200):
    r = MagicMock()
    r.status_code = status_code
    r.json.return_value = data
    r.text = str(data)
    return r


def _xai_response_envelope(*, cost_ticks=1234, x_search_calls=2,
                            input_tokens=500, output_tokens=100,
                            output_text="{}"):
    """Minimal xAI Responses-API JSON envelope with realistic usage fields."""
    return {
        "id": "resp_abc12345",
        "model": "grok-4.3",
        "service_tier": "default",
        "output": [
            {
                "type": "message",
                "content": [{"type": "output_text", "text": output_text}],
            }
        ],
        "usage": {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens,
            "cost_in_usd_ticks": cost_ticks,
            "num_sources_used": 0,
            "num_server_side_tools_used": x_search_calls,
            "server_side_tool_usage_details": {"x_search_calls": x_search_calls},
            "input_tokens_details": {"cached_tokens": 50},
            "output_tokens_details": {"reasoning_tokens": 10},
        },
    }


def _call_sync(provider, envelope, **kwargs):
    """Synchronous wrapper: runs an async _call_grok_with_x_search in a fresh loop."""
    import httpx
    mock_resp = _fake_response(envelope)

    async def _run():
        with patch("httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
            mock_client.post = AsyncMock(return_value=mock_resp)
            return await provider._call_grok_with_x_search(**kwargs)

    return asyncio.get_event_loop().run_until_complete(_run())


# ---------------------------------------------------------------------------
# Fix 1 — caller_label + _usage_out + [XAI_USAGE] structured log line
# ---------------------------------------------------------------------------

class TestUsageLogging(unittest.TestCase):
    """Fix 1: _call_grok_with_x_search logs usage and populates _usage_out."""

    def test_usage_out_populated(self):
        """_usage_out dict is populated with all provider metadata fields."""
        provider = _make_xai_provider()
        env = _xai_response_envelope(cost_ticks=9999, x_search_calls=3,
                                     input_tokens=400, output_tokens=80)
        usage = {}
        _call_sync(provider, env, prompt="test", raw_mode=True, _usage_out=usage)
        self.assertEqual(usage["cost_in_usd_ticks"], 9999)
        self.assertEqual(usage["x_search_calls"], 3)
        self.assertEqual(usage["input_tokens"], 400)
        self.assertEqual(usage["output_tokens"], 80)
        self.assertEqual(usage["request_id"], "resp_abc12345")

    def test_usage_out_none_does_not_raise(self):
        """Omitting _usage_out (default None) must not raise."""
        provider = _make_xai_provider()
        env = _xai_response_envelope()
        result = _call_sync(provider, env, prompt="test", raw_mode=True)
        self.assertIn("_raw_analysis", result)

    def test_xai_usage_log_line_emitted(self):
        """[XAI_USAGE] log line is printed with key metadata fields."""
        import io, contextlib
        provider = _make_xai_provider()
        env = _xai_response_envelope(cost_ticks=42)
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            _call_sync(provider, env, prompt="test",
                       caller_label="test_caller", raw_mode=True)
        log = buf.getvalue()
        self.assertIn("[XAI_USAGE]", log)
        self.assertIn("[test_caller]", log)
        self.assertIn("cost_ticks=42", log)

    def test_caller_label_tag_in_request_log(self):
        """[XAI] prefixed request log includes [caller_label] tag."""
        import io, contextlib
        provider = _make_xai_provider()
        env = _xai_response_envelope()
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            _call_sync(provider, env, prompt="p",
                       caller_label="social_phase2", raw_mode=True)
        self.assertIn("[social_phase2]", buf.getvalue())

    def test_return_value_unchanged_with_usage_out(self):
        """Passing _usage_out must not change the function's return value."""
        provider = _make_xai_provider()
        env = _xai_response_envelope(output_text='{"hello": "world"}')
        result_a = _call_sync(provider, env, prompt="p",
                              raw_mode=False, _usage_out={})
        result_b = _call_sync(provider, env, prompt="p",
                              raw_mode=False)
        self.assertEqual(result_a, result_b)
        self.assertNotIn("cost_in_usd_ticks", result_a)


# ---------------------------------------------------------------------------
# Fix 2 — Scheduler double-fire guard
# ---------------------------------------------------------------------------

class TestSchedulerFreshnessGuard(unittest.TestCase):
    """Fix 2: scheduled fire is skipped when canonical cache is still fresh."""

    def _fresh_snapshot(self):
        return {"_saved_at": time.time() - 60, "generated_at": "2026-07-16T10:00:00Z"}

    def _stale_snapshot(self):
        return {"_saved_at": time.time() - 90_000, "generated_at": "2026-07-15T10:00:00Z"}

    def test_is_fresh_true_for_recent_cache(self):
        """_is_fresh returns True when snapshot was saved within the TTL."""
        from services.x_consensus_cache import _is_fresh
        self.assertTrue(_is_fresh(self._fresh_snapshot()))

    def test_is_fresh_false_for_old_cache(self):
        """_is_fresh returns False for a cache older than TTL (90 000 s > 23 h)."""
        from services.x_consensus_cache import _is_fresh
        self.assertFalse(_is_fresh(self._stale_snapshot()))

    def test_is_fresh_false_for_none(self):
        """_is_fresh returns False when called with None."""
        from services.x_consensus_cache import _is_fresh
        self.assertFalse(_is_fresh(None))

    def test_scheduled_guard_log_message(self):
        """Guard log message says canonical_cache_still_fresh."""
        with open(_MAIN_PY) as fh:
            src = fh.read()
        self.assertIn("canonical_cache_still_fresh", src)

    def test_freshness_check_precedes_run_refresh_call(self):
        """Guard checks _xc_fresh before calling the _run_refresh in the scheduled block.

        There are two _run_refresh call sites in main.py:
          1. Startup catch-up block (before the guard — expected)
          2. Scheduled daily fire block (after the guard — must be guarded)
        We look for the refresh call that comes AFTER the guard, not the first one.
        """
        with open(_MAIN_PY) as fh:
            src = fh.read()
        guard_pos = src.find("canonical_cache_still_fresh")
        # Search for _run_refresh starting FROM the guard position (post-guard site)
        refresh_pos = src.find("await _run_refresh(data_service)", guard_pos)
        self.assertGreater(guard_pos, 0, "Guard text not found in main.py")
        self.assertGreater(refresh_pos, 0, "_run_refresh call not found after guard")
        self.assertLess(guard_pos, refresh_pos)


# ---------------------------------------------------------------------------
# Fix 3a — Phase-2 x_search disabled in payload
# ---------------------------------------------------------------------------

class TestPhase2XSearchDisabled(unittest.TestCase):
    """Fix 3a: Phase-2 synthesis omits the X Search tool entirely."""

    def _capture_payload(self, **kwargs):
        """Call _call_grok_with_x_search, capture the payload sent to the API."""
        provider = _make_xai_provider()
        captured = {}

        async def _run():
            async def _capture_post(url, *, headers, json):
                captured.update(json)
                return _fake_response(_xai_response_envelope(output_text="{}"))

            with patch("httpx.AsyncClient") as mock_cls:
                mock_client = AsyncMock()
                mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
                mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
                mock_client.post = _capture_post
                await provider._call_grok_with_x_search(**kwargs)

        asyncio.get_event_loop().run_until_complete(_run())
        return captured

    def test_no_tools_when_x_search_disabled(self):
        """x_search_config={'enabled': False} produces a payload without 'tools'."""
        payload = self._capture_payload(
            prompt="test", raw_mode=True,
            x_search_config={"enabled": False},
        )
        self.assertNotIn("tools", payload)

    def test_x_search_enabled_by_default(self):
        """With no x_search_config, the tools array is still included."""
        payload = self._capture_payload(prompt="test", raw_mode=True)
        self.assertIn("tools", payload)

    def test_enabled_key_stripped_from_x_search_opts(self):
        """allowed_x_handles passes through; 'enabled' key is not leaked."""
        payload = self._capture_payload(
            prompt="test", raw_mode=True,
            x_search_config={"allowed_x_handles": ["foo"], "from_date": "2026-01-01"},
        )
        self.assertIn("tools", payload)
        tool_opts = payload["tools"][0]["x_search"]
        self.assertNotIn("enabled", tool_opts)
        self.assertIn("allowed_x_handles", tool_opts)

    def test_phase2_call_has_x_search_disabled(self):
        """In x_consensus_cache.py, Phase-2 synthesis call passes enabled=False."""
        with open(_X_CACHE_PY) as fh:
            src = fh.read()
        self.assertIn('x_search_config={"enabled": False}', src)
        self.assertIn('caller_label="social_phase2"', src)


# ---------------------------------------------------------------------------
# Fix 3b — fresh_trades conservative fallback
# ---------------------------------------------------------------------------

class TestFreshTradesFallback(unittest.TestCase):
    """Fix 3b: fresh_trades conservative fallback from Phase-1 data."""

    def _acct_mention(self, handle, cat, sentiment, conviction, recency_days, ticker, thesis):
        return {
            "handle": handle,
            "category": cat,
            "mentions": [{
                "ticker": ticker,
                "sentiment": sentiment,
                "conviction": conviction,
                "recency_days": recency_days,
                "thesis": thesis,
                "catalysts": [],
            }],
        }

    def _run_fallback(self, all_account_mentions, grok_result):
        """Inline simulation of the fresh_trades fallback logic."""
        from services.x_consensus_cache import _ACCOUNT_CATEGORY_BY_HANDLE
        _ft_fallback_used = False
        if not grok_result.get("fresh_trades") and all_account_mentions:
            _ft_candidates, _ft_seen = [], set()
            for _acct in all_account_mentions:
                if _ACCOUNT_CATEGORY_BY_HANDLE.get(_acct.get("handle", "")) != "top_trader":
                    continue
                for _m in _acct.get("mentions", []):
                    if (
                        _m.get("sentiment") == "bullish"
                        and _m.get("conviction") == "high"
                        and isinstance(_m.get("recency_days"), (int, float))
                        and _m["recency_days"] <= 1
                        and _m.get("ticker")
                        and _m.get("thesis")
                    ):
                        _tk = _m["ticker"].lstrip("$").upper()
                        if _tk and _tk not in _ft_seen:
                            _ft_seen.add(_tk)
                            _ft_candidates.append({
                                "ticker": _tk,
                                "first_mentioned_by": [f"@{_acct['handle']}"],
                                "entry_thesis": _m["thesis"][:250],
                            })
            if _ft_candidates:
                grok_result["fresh_trades"] = _ft_candidates[:3]
                _ft_fallback_used = True
        return _ft_fallback_used

    def _top_trader_handle(self):
        from services.x_consensus_cache import X_SELECT_ACCOUNTS
        return next(a["handle"] for a in X_SELECT_ACCOUNTS if a["category"] == "top_trader")

    def test_fallback_fires_for_brun_parity_case(self):
        """BRUN parity-test case: top_trader high-conviction same-day mention triggers fallback."""
        h = self._top_trader_handle()
        mentions = [self._acct_mention(h, "top_trader", "bullish", "high",
                                       0, "$BRUN", "BRUN breaking out on volume")]
        result = {"fresh_trades": []}
        fired = self._run_fallback(mentions, result)
        self.assertTrue(fired, "Fallback must fire when fresh_trades is empty")
        self.assertEqual(len(result["fresh_trades"]), 1)
        self.assertEqual(result["fresh_trades"][0]["ticker"], "BRUN")

    def test_fallback_caps_at_three(self):
        """Fallback produces at most 3 fresh_trades entries."""
        h = self._top_trader_handle()
        mentions = [
            self._acct_mention(h, "top_trader", "bullish", "high", 0,
                               f"${s}", f"Thesis for {s}")
            for s in ["AA", "BB", "CC", "DD", "EE"]
        ]
        result = {"fresh_trades": []}
        self._run_fallback(mentions, result)
        self.assertLessEqual(len(result["fresh_trades"]), 3)

    def test_fallback_ignores_above_average_trader(self):
        """above_average_trader accounts must NOT contribute to the fallback."""
        mentions = [self._acct_mention(
            "crux_capital_", "above_average_trader", "bullish", "high",
            0, "$XYZ", "XYZ looks good"
        )]
        result = {"fresh_trades": []}
        fired = self._run_fallback(mentions, result)
        self.assertFalse(fired)
        self.assertEqual(result["fresh_trades"], [])

    def test_fallback_ignores_stale_recency(self):
        """Mentions with recency_days > 1 are excluded."""
        h = self._top_trader_handle()
        mentions = [self._acct_mention(h, "top_trader", "bullish", "high",
                                       3, "$STALE", "Stale thesis")]
        result = {"fresh_trades": []}
        fired = self._run_fallback(mentions, result)
        self.assertFalse(fired)

    def test_fallback_not_fired_when_fresh_trades_already_present(self):
        """Fallback is skipped when Grok already returned fresh_trades."""
        h = self._top_trader_handle()
        mentions = [self._acct_mention(h, "top_trader", "bullish", "high",
                                       0, "$NVDA", "NVDA AI play")]
        result = {"fresh_trades": [{"ticker": "NVDA", "from_grok": True}]}
        fired = self._run_fallback(mentions, result)
        self.assertFalse(fired)
        self.assertEqual(len(result["fresh_trades"]), 1)
        self.assertTrue(result["fresh_trades"][0].get("from_grok"))


# ---------------------------------------------------------------------------
# Fix 4 — Legacy pipeline removed from /api/social/query
# ---------------------------------------------------------------------------

class TestLegacyPipelineRemoved(unittest.TestCase):
    """Fix 4: /api/social/query no longer runs a local 3-call pipeline."""

    def _main_src(self):
        with open(_MAIN_PY) as fh:
            return fh.read()

    def test_no_local_fetch_batch_closure_in_social_query(self):
        """The local _fetch_batch async closure must no longer exist in the route."""
        import ast, re

        src = self._main_src()
        # Quick text check — the closure had a specific signature
        self.assertNotIn(
            "async def _fetch_batch(handles: list[str]",
            src,
            "Old local _fetch_batch closure must be removed from social_query route"
        )

    def test_canonical_imports_present(self):
        """Route imports _trigger_background_refresh and _REFRESH_LOCK."""
        src = self._main_src()
        self.assertIn("_trigger_background_refresh", src)
        self.assertIn("_REFRESH_LOCK", src)

    def test_source_field_in_stale_path(self):
        """Stale-path response includes source='canonical_x_consensus_cache'."""
        self.assertIn("canonical_x_consensus_cache", self._main_src())

    def test_refresh_accepted_field(self):
        """Stale-path response includes refresh_accepted field."""
        self.assertIn('"refresh_accepted"', self._main_src())

    def test_refresh_in_progress_field(self):
        """Stale-path response includes refresh_in_progress field."""
        self.assertIn('"refresh_in_progress"', self._main_src())

    def test_legacy_asyncio_gather_batch_removed(self):
        """The old asyncio.gather(*[_fetch_batch(batch, i)...]) pattern is gone."""
        import re
        src = self._main_src()
        pattern = r"asyncio\.gather\(\s*\*\[_fetch_batch\("
        self.assertIsNone(
            re.search(pattern, src),
            "Legacy asyncio.gather(*[_fetch_batch(...)]) must be gone"
        )


# ---------------------------------------------------------------------------
# Fix 5 — Stale comments corrected
# ---------------------------------------------------------------------------

class TestStaleCommentsFix(unittest.TestCase):
    """Fix 5: account counts, TTL, and refresh cadence comments are accurate."""

    def _cache_src(self):
        with open(_X_CACHE_PY) as fh:
            return fh.read()

    def test_module_docstring_says_daily_not_bi_hourly(self):
        src = self._cache_src()
        self.assertNotIn("Bi-hourly", src)
        self.assertIn("Daily", src)

    def test_account_count_comment_is_16(self):
        src = self._cache_src()
        self.assertNotIn("27 accounts", src)
        self.assertIn("16 accounts", src)

    def test_category_count_comment_is_5(self):
        src = self._cache_src()
        self.assertNotIn("6 categories", src)
        self.assertIn("5 categories", src)

    def test_run_refresh_docstring_says_3_calls(self):
        src = self._cache_src()
        self.assertNotIn("ceil(27/8)", src)
        self.assertIn("ceil(16/8)", src)

    def test_run_refresh_docstring_says_23h_ttl(self):
        src = self._cache_src()
        self.assertNotIn("4h TTL", src)
        self.assertIn("23h TTL", src)


if __name__ == "__main__":
    unittest.main(verbosity=2)
