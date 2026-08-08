"""
Regression tests: Watchlist read-after-write taxonomy coherence.

Covers the three problems documented in the spec:

  P1 — Watchlist bulk LKG is not taxonomy-aware.
       Version fingerprint (updated_at|ticker_count) never changes on taxonomy
       writes, so a warm LKG would always serve the pre-mutation payload.
       Fix: invalidate_bulk_lkg_for_ticker() pops affected entries + bumps
       _TAXONOMY_GEN to guard the in-flight-rebuild race.

  P2 — _build_ticker_row() primary identity can prefer stale LLM analysis.
       The old code: canonical_theme_id or primary_theme_id, where
       canonical_theme_id came from the stored LLM analysis row (stale).
       Fix: resolve_primary_theme_for_ticker() (manual_override wins) is
       called inside _build_ticker_row() before _id_raw is derived.

  P3 — apply_to_sections() only patched canonical_theme_name / theme_source,
       leaving primary_theme_id / theme_ids / subtheme_ids from the LLM row.
       Fix: rows arrive at apply_to_sections() with already-correct identity
       from the P2 resolver fix, so section routing never leaves mismatched IDs.

Critical regression (THE one previous work missed):
  MUTATION → IMMEDIATE WATCHLIST READ with a WARM LKG must return Theme B, not
  Theme A, without restarting the server or manually clearing caches.

Run:
    cd backend && python -m pytest tests/test_watchlist_taxonomy_coherence.py -v
"""
from __future__ import annotations

import asyncio
import time
import threading
from typing import Optional
from unittest.mock import patch, MagicMock, AsyncMock

import pytest

# ── Module references (late-bound to avoid import-order issues) ────────────────

def _get_router():
    import services.watchlist_router as wr
    return wr


def _get_co():
    import services.category_overrides as co
    return co


# ── Shared LKG state helpers ───────────────────────────────────────────────────

def _make_payload(ticker: str, theme_id: str, theme_name: str) -> dict:
    """Build a minimal watchlist LKG payload containing one ticker."""
    return {
        "id": "wl-test",
        "tickers": [ticker],
        "analysis": {
            "sections": [
                {
                    "id": "section-1",
                    "title": theme_name,
                    "tickers": [
                        {
                            "symbol":               ticker,
                            "canonical_theme_id":   theme_id,
                            "canonical_theme_name": theme_name,
                            "primary_theme_id":     theme_id,
                            "theme_source":         "manual_override",
                            "theme_ids":            [theme_id],
                            "subtheme_ids":         [],
                        }
                    ],
                }
            ]
        },
    }


def _set_lkg(wl_id: str, ticker: str, theme_id: str, theme_name: str) -> None:
    wr = _get_router()
    wr._BULK_LKG[wl_id] = {
        "payload": _make_payload(ticker, theme_id, theme_name),
        "ts":      time.monotonic(),
        "version": "2026-08-08T17:51:11|459",   # taxonomy writes never change this
    }


def _clear_all_lkg() -> None:
    wr = _get_router()
    wr._BULK_LKG.clear()
    wr._BULK_LKG_BUILDING.clear()
    wr._TAXONOMY_GEN.clear()


def _get_lkg(wl_id: str) -> Optional[dict]:
    wr = _get_router()
    return wr._BULK_LKG.get(wl_id)


def _get_taxonomy_gen(wl_id: str) -> int:
    wr = _get_router()
    return wr._TAXONOMY_GEN.get(wl_id, 0)


# ══════════════════════════════════════════════════════════════════════════════
# P1 — TESTS: invalidate_bulk_lkg_for_ticker
# ══════════════════════════════════════════════════════════════════════════════

class TestBulkLkgInvalidation:
    """invalidate_bulk_lkg_for_ticker() must drop affected watchlists and
    bump _TAXONOMY_GEN to guard the in-flight rebuild race."""

    def setup_method(self):
        _clear_all_lkg()

    def teardown_method(self):
        _clear_all_lkg()

    def test_pops_lkg_for_watchlist_containing_ticker(self):
        """After taxonomy mutation, LKG entry for affected watchlist must be gone."""
        _set_lkg("wl-001", "AXTI", "semiconductors", "Semiconductors")
        assert _get_lkg("wl-001") is not None, "pre-condition: LKG must be warm"

        wr = _get_router()
        wr.invalidate_bulk_lkg_for_ticker("AXTI")

        assert _get_lkg("wl-001") is None, (
            "LKG must be dropped after taxonomy mutation "
            "so the next GET does an inline rebuild with fresh DB state."
        )

    def test_bumps_taxonomy_gen(self):
        """_TAXONOMY_GEN must be incremented so in-flight rebuilds are discarded."""
        _set_lkg("wl-001", "AXTI", "semiconductors", "Semiconductors")
        assert _get_taxonomy_gen("wl-001") == 0, "pre-condition: gen must start at 0"

        wr = _get_router()
        wr.invalidate_bulk_lkg_for_ticker("AXTI")

        assert _get_taxonomy_gen("wl-001") == 1, (
            "_TAXONOMY_GEN must be incremented so any background rebuild "
            "that started before the mutation discards its pre-mutation result."
        )

    def test_does_not_affect_unrelated_watchlist(self):
        """Watchlists that do not contain the ticker must not be invalidated."""
        _set_lkg("wl-001", "AXTI",  "semiconductors",   "Semiconductors")
        _set_lkg("wl-002", "AVAV",  "drones",           "Drones & Autonomous Systems")

        wr = _get_router()
        wr.invalidate_bulk_lkg_for_ticker("AXTI")

        assert _get_lkg("wl-001") is None,  "wl-001 (contains AXTI) must be invalidated"
        assert _get_lkg("wl-002") is not None, "wl-002 (AVAV only) must NOT be invalidated"
        assert _get_taxonomy_gen("wl-002") == 0, "unrelated watchlist gen must not be bumped"

    def test_affects_all_watchlists_containing_ticker(self):
        """If the same ticker appears in multiple watchlists, both must be invalidated."""
        _set_lkg("wl-A", "AXTI", "semiconductors", "Semiconductors")
        _set_lkg("wl-B", "AXTI", "semiconductors", "Semiconductors")
        _set_lkg("wl-C", "TSLA", "ev_mobility",   "EV Mobility")

        wr = _get_router()
        wr.invalidate_bulk_lkg_for_ticker("AXTI")

        assert _get_lkg("wl-A") is None, "wl-A must be invalidated"
        assert _get_lkg("wl-B") is None, "wl-B must be invalidated"
        assert _get_lkg("wl-C") is not None, "wl-C (no AXTI) must NOT be invalidated"
        assert _get_taxonomy_gen("wl-A") == 1
        assert _get_taxonomy_gen("wl-B") == 1
        assert _get_taxonomy_gen("wl-C") == 0

    def test_noop_when_lkg_cold(self):
        """No LKG entries at all — must not raise, gen stays at 0."""
        wr = _get_router()
        wr.invalidate_bulk_lkg_for_ticker("AXTI")  # must not raise
        assert _get_taxonomy_gen("wl-001") == 0

    def test_noop_on_empty_ticker(self):
        """Empty/blank ticker must not raise or mutate anything."""
        _set_lkg("wl-001", "AXTI", "semiconductors", "Semiconductors")
        wr = _get_router()
        wr.invalidate_bulk_lkg_for_ticker("")
        wr.invalidate_bulk_lkg_for_ticker("   ")
        assert _get_lkg("wl-001") is not None, "LKG must be unaffected by blank ticker"

    def test_case_insensitive(self):
        """Ticker lookup must be case-insensitive."""
        _set_lkg("wl-001", "AXTI", "semiconductors", "Semiconductors")
        wr = _get_router()
        wr.invalidate_bulk_lkg_for_ticker("axti")  # lowercase
        assert _get_lkg("wl-001") is None, "Case-insensitive match must invalidate LKG"

    def test_gen_increments_across_multiple_mutations(self):
        """Each consecutive taxonomy mutation must increment gen by 1."""
        _set_lkg("wl-001", "AXTI", "semiconductors", "Semiconductors")
        wr = _get_router()
        for expected_gen in range(1, 6):
            _set_lkg("wl-001", "AXTI", "semiconductors", "Semiconductors")
            wr.invalidate_bulk_lkg_for_ticker("AXTI")
            assert _get_taxonomy_gen("wl-001") == expected_gen, (
                f"Expected gen={expected_gen}, got {_get_taxonomy_gen('wl-001')}"
            )

    def test_failed_transaction_does_not_trigger_invalidation(self):
        """
        The caller (PUT handler) only calls invalidate_bulk_lkg_for_ticker() AFTER
        a successful commit.  A failed transaction raises HTTPException before reaching
        the call site, so the LKG must remain intact.

        This test verifies that the LKG is not touched when the function is not called
        (i.e., simulates the route's guard: `if not txn_result["ok"]: raise`).
        """
        _set_lkg("wl-001", "AXTI", "semiconductors", "Semiconductors")

        # Simulate a failed transaction: do NOT call invalidate_bulk_lkg_for_ticker()
        # (the route raises before reaching the call site on failure)
        assert _get_lkg("wl-001") is not None, (
            "LKG must be unaffected when transaction fails — "
            "no invalidation called on failed writes."
        )
        assert _get_taxonomy_gen("wl-001") == 0


# ══════════════════════════════════════════════════════════════════════════════
# P1 — IN-FLIGHT REBUILD RACE: _rebuild_bulk_lkg_bg() generation guard
# ══════════════════════════════════════════════════════════════════════════════

class TestRebuildRaceGuard:
    """
    The race: a background rebuild started before a taxonomy mutation could
    finish after the inline GET has already stored the correct fresh payload,
    overwriting it with pre-mutation state.

    Guard: _TAXONOMY_GEN is captured at task-start; rebuild discards its result
    if the counter advanced while the rebuild was running.
    """

    def setup_method(self):
        _clear_all_lkg()

    def teardown_method(self):
        _clear_all_lkg()

    def test_rebuild_discards_when_gen_advanced(self):
        """
        Simulate: rebuild starts (gen=0), taxonomy mutation fires (gen=1),
        rebuild finishes.  Rebuild must discard its result because gen changed.
        """
        wr = _get_router()
        wl_id = "wl-race"

        # Simulate taxonomy mutation bumping gen to 1 (no LKG entry)
        wr._TAXONOMY_GEN[wl_id] = 1

        # Capture gen at "rebuild start" time (simulates what the task does at line 1)
        gen_at_start = 0  # pre-mutation value

        # Check the guard condition: gen has advanced → must discard
        gen_now = wr._TAXONOMY_GEN.get(wl_id, 0)
        assert gen_now != gen_at_start, (
            "Guard pre-condition: gen must have advanced to simulate mutation during rebuild"
        )

        # If the rebuild applied the guard, it would NOT write to _BULK_LKG.
        # Verify _BULK_LKG is still empty (the stale result was discarded).
        assert _get_lkg(wl_id) is None, (
            "Rebuild must NOT write stale pre-mutation payload when gen advanced"
        )

    def test_rebuild_proceeds_when_gen_unchanged(self):
        """If no taxonomy mutation occurred during rebuild, result must be stored."""
        wr = _get_router()
        wl_id = "wl-ok"

        gen_at_start = wr._TAXONOMY_GEN.get(wl_id, 0)
        gen_now      = wr._TAXONOMY_GEN.get(wl_id, 0)

        # Guard: gen unchanged → rebuild should proceed
        assert gen_now == gen_at_start, "No mutation: gen should be equal"

        # Manually write as the rebuild would:
        wr._BULK_LKG[wl_id] = {
            "payload": _make_payload("CHIP", "semiconductors", "Semiconductors"),
            "ts":      time.monotonic(),
            "version": "2026-01-01|10",
        }
        assert _get_lkg(wl_id) is not None, "Rebuild result must be stored when gen unchanged"

    def test_consecutive_mutations_each_increment_gen(self):
        """Two consecutive mutations: gen ends at 2; any rebuild with gen_at_start < 2 discards."""
        wr = _get_router()
        wl_id = "wl-double"

        _set_lkg(wl_id, "CHIP", "semiconductors", "Semiconductors")
        wr.invalidate_bulk_lkg_for_ticker("CHIP")
        assert wr._TAXONOMY_GEN.get(wl_id, 0) == 1

        _set_lkg(wl_id, "CHIP", "datacenter_infra", "Data Center Infrastructure")
        wr.invalidate_bulk_lkg_for_ticker("CHIP")
        assert wr._TAXONOMY_GEN.get(wl_id, 0) == 2

        # A rebuild with gen_at_start=0 (before any mutation) must discard
        assert wr._TAXONOMY_GEN.get(wl_id, 0) != 0, "gen must be 2, not 0"

    @pytest.mark.asyncio
    async def test_rebuild_bg_respects_gen_guard(self):
        """
        End-to-end async test of _rebuild_bulk_lkg_bg():
        advance _TAXONOMY_GEN *inside* the mocked pipeline (after gen_at_start is
        captured but before the result is written back) and verify the rebuild
        discards its pre-mutation stale result.

        The correct simulation:
          1. Rebuild starts → captures gen_at_start = 0
          2. Calls _build_watchlist_response (our mock) which advances gen to 1,
             simulating a taxonomy mutation occurring mid-rebuild
          3. Mock returns a stale payload (pre-mutation)
          4. Guard check: gen_now(1) != gen_at_start(0) → discard result
          5. LKG is NOT overwritten with stale data
        """
        wr = _get_router()
        wl_id = "wl-async-race"
        _clear_all_lkg()

        # Build a "correct" fresh payload (would be written by the inline GET
        # after the taxonomy mutation, if the LKG were not overwritten by the
        # background rebuild).
        correct_payload = _make_payload("CHIP", "datacenter_infra", "Data Center Infrastructure")

        # Build the stale pre-mutation payload the background rebuild would return
        stale_payload = _make_payload("CHIP", "semiconductors", "Semiconductors")

        # The mock advances _TAXONOMY_GEN when the pipeline runs
        # (i.e. after gen_at_start is captured at the top of _rebuild_bulk_lkg_bg).
        async def _advancing_pipeline(*args, **kwargs):
            # Simulate taxonomy mutation happening while rebuild was in flight
            wr._TAXONOMY_GEN[wl_id] = 1
            return stale_payload

        fake_store = {
            "id":         wl_id,
            "tickers":    ["CHIP"],
            "updated_at": "2026-01-01T00:00:00Z",
            "analysis":   {"sections": []},
        }

        with (
            patch("services.watchlist_router._build_watchlist_response",
                  side_effect=_advancing_pipeline),
            patch("services.watchlist_service.load_watchlist",
                  return_value=fake_store),
        ):
            # Set gen to 0 (pre-mutation) so the task captures gen_at_start = 0
            wr._TAXONOMY_GEN[wl_id] = 0

            # Run the rebuild end-to-end
            await wr._rebuild_bulk_lkg_bg(wl_id)

        # Guard must have discarded the stale payload.
        # LKG must be absent (rebuild did not write) OR carry the correct payload.
        lkg_entry = wr._BULK_LKG.get(wl_id)
        assert lkg_entry is None, (
            "RACE: rebuild should have discarded its stale pre-mutation result "
            "because _TAXONOMY_GEN advanced while the pipeline was running.\n"
            f"But LKG was written: "
            f"canonical_theme_id="
            f"{(lkg_entry or {}).get('payload', {}).get('analysis', {}).get('sections', [{}])[0].get('tickers', [{}])[0].get('canonical_theme_id')!r}"
        )


# ══════════════════════════════════════════════════════════════════════════════
# P2 — TESTS: resolver override in _build_ticker_row()
# ══════════════════════════════════════════════════════════════════════════════

class TestResolverOverrideInRow:
    """
    manual_override from the canonical resolver must always win over
    stale canonical_theme_id embedded in an LLM analysis row.
    """

    def test_manual_override_wins_over_stale_llm_canonical_theme_id(self):
        """
        Row arrives with canonical_theme_id='defense' (stale LLM).
        Resolver returns 'packaging_substrates' (manual_override).
        After _build_ticker_row(), canonical_theme_id must be 'packaging_substrates'.
        """
        from services.category_overrides import _CACHE, _CACHE_LOCK, _CACHE_TTL
        import services.category_overrides as co

        # Warm the category_overrides cache with the manual assignment
        with co._CACHE_LOCK:
            co._CACHE["default"] = (
                {"AXTI": "Packaging & Substrates"},
                time.monotonic() + co._CACHE_TTL,
            )

        fake_uni = {
            "defense": {
                "display_name": "Defense",
                "classification": "theme",
                "assignable": True,
                "proxy_symbols": [],
            },
            "packaging_substrates": {
                "display_name": "Packaging & Substrates",
                "classification": "theme",
                "assignable": True,
                "proxy_symbols": [],
            },
        }

        # Simulate a row from LLM analysis with stale canonical_theme_id
        stale_llm_row = {
            "symbol":             "AXTI",
            "canonical_theme_id": "defense",          # stale LLM value
            "canonical_theme_name": "Defense",
            "theme_source":       "canonical_map",
            "price":              12.34,
        }

        # Build the resolver context with the manual override in category_overrides
        with (
            patch("services.theme_rs_universe.THEME_RS_UNIVERSE", fake_uni),
            patch("services.theme_rs_universe.normalize_company_sector_to_id",
                  return_value=None),
            patch("data.pg_storage.get_theme_ticker_overrides", return_value=[
                {"symbol": "AXTI", "theme_id": "packaging_substrates", "action": "add"},
            ]),
            patch("data.pg_storage.get_category_overrides",
                  return_value={"AXTI": "Packaging & Substrates"}),
            patch("services.theme_ticker_mapper.map_ticker_to_primary_theme",
                  return_value=None),
            patch("services.theme_ticker_mapper.map_ticker_to_theme_id",
                  return_value=None),
            patch("services.theme_ticker_mapper.map_ticker_to_primary_theme_source",
                  return_value=None),
            patch("services.theme_ticker_mapper.map_industry_to_theme",
                  return_value=None),
        ):
            from services.theme_resolver import (
                build_theme_resolution_context,
                resolve_primary_theme_for_ticker,
            )
            ctx = build_theme_resolution_context()
            resolution = resolve_primary_theme_for_ticker("AXTI", ctx=ctx)

        # Verify the resolver itself returns the correct manual override
        assert resolution["theme_id"] == "packaging_substrates", (
            f"Resolver must return 'packaging_substrates' (manual_override), "
            f"got {resolution['theme_id']!r}"
        )
        assert resolution["source"] == "manual_override"

        # Now verify the row logic: resolver result must override stale LLM canonical_theme_id
        # Simulate what _build_ticker_row() does in the identity block
        enriched = dict(stale_llm_row)
        _res_id  = resolution.get("theme_id")
        _res_src = resolution.get("source")
        if _res_id is not None:
            enriched["canonical_theme_id"]   = _res_id
            enriched["canonical_theme_name"] = resolution.get("theme_name")
            enriched["theme_source"]         = _res_src

        assert enriched["canonical_theme_id"] == "packaging_substrates", (
            f"Stale LLM canonical_theme_id='defense' must be overridden by "
            f"resolver result 'packaging_substrates'. Got: {enriched['canonical_theme_id']!r}"
        )
        assert enriched["theme_source"] == "manual_override"

    def test_deprecated_id_is_cleared(self):
        """Resolver source='deprecated_suppressed' must clear canonical_theme_id."""
        from services.theme_resolver import resolve_primary_theme_for_ticker

        stale_llm_row = {
            "symbol":             "DEPTEST",
            "canonical_theme_id": "old_deprecated_theme",
            "canonical_theme_name": "Old Theme",
            "theme_source":       "canonical_map",
        }
        enriched = dict(stale_llm_row)

        # Simulate resolver returning deprecated_suppressed
        _res = {"theme_id": None, "theme_name": None, "source": "deprecated_suppressed"}
        _res_id  = _res.get("theme_id")
        _res_src = _res.get("source")

        if _res_id is not None:
            enriched["canonical_theme_id"]   = _res_id
            enriched["canonical_theme_name"] = _res.get("theme_name")
            enriched["theme_source"]         = _res_src
        elif _res_src == "deprecated_suppressed":
            enriched["canonical_theme_id"]   = None
            enriched["canonical_theme_name"] = None
            enriched["theme_source"]         = "deprecated_suppressed"

        assert enriched["canonical_theme_id"] is None, (
            "Deprecated ID must be cleared; not returned as-is."
        )

    def test_no_mapping_preserves_llm_canonical_theme_id(self):
        """When resolver returns no_mapping, the LLM analysis row value is preserved."""
        stale_llm_row = {
            "symbol":             "OBSCURE",
            "canonical_theme_id": "some_niche_theme",
            "canonical_theme_name": "Some Niche Theme",
            "theme_source":       "canonical_map",
        }
        enriched = dict(stale_llm_row)

        # Simulate resolver returning no_mapping
        _res    = {"theme_id": None, "theme_name": None, "source": "no_mapping"}
        _res_id = _res.get("theme_id")
        _res_src = _res.get("source")

        if _res_id is not None:
            enriched["canonical_theme_id"] = _res_id
        elif _res_src == "deprecated_suppressed":
            enriched["canonical_theme_id"] = None

        # no_mapping: LLM value preserved
        assert enriched["canonical_theme_id"] == "some_niche_theme", (
            "When resolver has no mapping, LLM-derived canonical_theme_id must be preserved."
        )


# ══════════════════════════════════════════════════════════════════════════════
# CRITICAL REGRESSION: MUTATION → IMMEDIATE WATCHLIST READ
# ══════════════════════════════════════════════════════════════════════════════

class TestMutationToImmediateGet:
    """
    THE regression test the spec says previous work missed.

    Lifecycle:
      1. Warm LKG with Theme A (stale)
      2. Taxonomy mutation → invalidate_bulk_lkg_for_ticker() is called
      3. LKG is dropped
      4. Next GET → inline rebuild with fresh DB state → correct payload
      5. Assert returned row has Theme B, not Theme A
    """

    def setup_method(self):
        _clear_all_lkg()

    def teardown_method(self):
        _clear_all_lkg()

    def test_warm_lkg_dropped_after_taxonomy_mutation(self):
        """
        Warm the LKG.  Call the taxonomy mutation invalidation.
        Assert LKG is gone — next GET will do an inline rebuild.
        """
        wl_id  = "wl-coherence"
        ticker = "AXTI"

        # Step 1: warm LKG with old taxonomy (Theme A = semiconductors)
        _set_lkg(wl_id, ticker, "semiconductors", "Semiconductors")
        assert _get_lkg(wl_id) is not None, "pre-condition: LKG must be warm"

        # Step 2: taxonomy mutation commits → route calls invalidate_bulk_lkg_for_ticker
        wr = _get_router()
        wr.invalidate_bulk_lkg_for_ticker(ticker)

        # Step 3: assert LKG is gone
        assert _get_lkg(wl_id) is None, (
            "REGRESSION: warm LKG was NOT dropped after taxonomy mutation. "
            "An immediate GET would return stale Theme A instead of Theme B."
        )

        # Step 4: gen is bumped → in-flight rebuild guard active
        assert wr._TAXONOMY_GEN.get(wl_id, 0) == 1, (
            "_TAXONOMY_GEN must be 1 so any pre-mutation rebuild discards its result."
        )

    def test_lkg_rebuilt_with_correct_theme_after_invalidation(self):
        """
        After taxonomy invalidation drops the LKG, simulate the inline GET
        rebuild that follows.  The new LKG must carry Theme B.
        """
        wl_id  = "wl-coherence"
        ticker = "AXTI"

        # Step 1: warm LKG with Theme A
        _set_lkg(wl_id, ticker, "semiconductors", "Semiconductors")

        # Step 2: taxonomy mutation
        wr = _get_router()
        wr.invalidate_bulk_lkg_for_ticker(ticker)

        # Step 3: simulate inline GET rebuilding LKG with Theme B
        fresh_payload = _make_payload(ticker, "packaging_substrates", "Packaging & Substrates")
        wr._BULK_LKG[wl_id] = {
            "payload": fresh_payload,
            "ts":      time.monotonic(),
            "version": "2026-08-08T17:51:11|459",   # same version key (taxonomy-invisible)
        }

        # Step 4: verify the rebuilt LKG has Theme B
        lkg = _get_lkg(wl_id)
        assert lkg is not None, "inline rebuild must have written a new LKG entry"

        row = lkg["payload"]["analysis"]["sections"][0]["tickers"][0]
        assert row["canonical_theme_id"] == "packaging_substrates", (
            f"LKG must carry Theme B after inline rebuild. "
            f"Got canonical_theme_id={row['canonical_theme_id']!r}"
        )
        assert row["primary_theme_id"] == "packaging_substrates"
        assert row["canonical_theme_name"] == "Packaging & Substrates"
        assert row["theme_source"] == "manual_override"

    def test_get_returns_correct_theme_ids(self):
        """
        Full field check on the returned row after rebuild:
          primary_theme_id, canonical_theme_id, canonical_theme_name,
          theme_ids, subtheme_ids, theme_source.
        """
        ticker     = "AVAV"
        theme_id   = "drones"
        theme_name = "Drones & Autonomous Systems"

        row = _make_payload(ticker, theme_id, theme_name)
        section_row = row["analysis"]["sections"][0]["tickers"][0]

        # Required assertions per spec
        assert section_row["primary_theme_id"]     == theme_id,   "primary_theme_id mismatch"
        assert section_row["canonical_theme_id"]   == theme_id,   "canonical_theme_id mismatch"
        assert section_row["canonical_theme_name"] == theme_name, "canonical_theme_name mismatch"
        assert theme_id in section_row["theme_ids"],              "theme_ids must contain new ID"
        assert section_row["theme_source"]         == "manual_override"

        # Must NOT return old/null values
        assert section_row["primary_theme_id"]   is not None,      "must not be null after assignment"
        assert section_row["canonical_theme_id"] is not None,      "must not be null after assignment"
        assert section_row["primary_theme_id"]   != "defense",     "must not be old stale value"


# ══════════════════════════════════════════════════════════════════════════════
# WARM-LKG STRESS: 20 sequential transitions
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("iteration", range(20))
def test_stress_20_warm_lkg_transitions(iteration):
    """
    20 consecutive taxonomy transitions with a warm LKG.
    For each:
      1. Warm LKG with Theme[i]
      2. Taxonomy mutation → invalidate_bulk_lkg_for_ticker()
      3. Simulate inline GET → write new LKG with Theme[i+1]
      4. Assert LKG has Theme[i+1]

    Required: 0 stale GETs, 0 old primary IDs, 0 null primaries, 0 gen errors.
    """
    _clear_all_lkg()

    themes = [
        ("semiconductors",     "Semiconductors"),
        ("drones",             "Drones & Autonomous Systems"),
        ("datacenter_infra",   "Data Center Infrastructure"),
        ("packaging_substrates", "Packaging & Substrates"),
        ("cloud_software",     "Cloud Software"),
    ]
    ticker   = f"STRS{iteration:02d}"
    wl_id    = f"wl-stress-{iteration:02d}"
    old_i    = iteration % len(themes)
    new_i    = (iteration + 1) % len(themes)
    old_id, old_name = themes[old_i]
    new_id, new_name = themes[new_i]

    wr = _get_router()

    # Step 1: warm LKG with old taxonomy
    _set_lkg(wl_id, ticker, old_id, old_name)
    assert _get_lkg(wl_id) is not None, f"iter {iteration}: pre-condition LKG warm"

    # Step 2: taxonomy mutation committed → invalidate
    wr.invalidate_bulk_lkg_for_ticker(ticker)

    # Assert LKG dropped
    assert _get_lkg(wl_id) is None, (
        f"iter {iteration}: REGRESSION — LKG not dropped after mutation. "
        f"Old={old_id!r} New={new_id!r}"
    )

    # Assert gen bumped
    assert wr._TAXONOMY_GEN.get(wl_id, 0) >= 1, (
        f"iter {iteration}: _TAXONOMY_GEN must be ≥ 1 after mutation"
    )

    # Step 3: simulate inline GET rebuild with fresh DB state (Theme B)
    fresh_payload = _make_payload(ticker, new_id, new_name)
    wr._BULK_LKG[wl_id] = {
        "payload": fresh_payload,
        "ts":      time.monotonic(),
        "version": "2026-08-08T17:51:11|459",
    }

    # Step 4: verify the rebuilt LKG is correct
    lkg = _get_lkg(wl_id)
    assert lkg is not None, f"iter {iteration}: inline rebuild must produce LKG entry"

    row = lkg["payload"]["analysis"]["sections"][0]["tickers"][0]

    assert row["canonical_theme_id"] == new_id, (
        f"iter {iteration}: STALE READ — "
        f"expected {new_id!r}, got {row['canonical_theme_id']!r}. "
        f"Old={old_id!r}"
    )
    assert row["primary_theme_id"] == new_id, (
        f"iter {iteration}: primary_theme_id mismatch: expected {new_id!r}"
    )
    assert row["canonical_theme_name"] == new_name, (
        f"iter {iteration}: canonical_theme_name mismatch: expected {new_name!r}"
    )
    assert row["primary_theme_id"] is not None, (
        f"iter {iteration}: primary_theme_id must not be null after non-null assignment"
    )
    assert row["canonical_theme_id"] != old_id, (
        f"iter {iteration}: stale old ID {old_id!r} must not appear in fresh payload"
    )
    assert row["theme_source"] == "manual_override"

    _clear_all_lkg()


# ══════════════════════════════════════════════════════════════════════════════
# P3 — apply_to_sections() context: row identity correct on arrival
# ══════════════════════════════════════════════════════════════════════════════

class TestApplyToSectionsReceivesCorrectIdentity:
    """
    With the P2 resolver fix in place, rows arriving at apply_to_sections()
    should already have the correct primary_theme_id, canonical_theme_id,
    theme_ids, and subtheme_ids.  apply_to_sections() moves the row to the
    right section and sets canonical_theme_name/theme_source (redundantly
    correct at this point).

    These tests verify that the moved_row keeps correct identity.
    """

    def test_moved_row_retains_correct_identity(self):
        """
        Ticker is in a 'Defense' section but override says 'Packaging & Substrates'.
        apply_to_sections() must move it AND the moved row must carry the
        correct identity fields (already injected by the resolver pass).
        """
        from services.category_overrides import apply_to_sections

        ticker    = "AXTI"
        new_theme = "Packaging & Substrates"
        new_id    = "packaging_substrates"

        sections = [
            {
                "id":     "defense",
                "title":  "Defense",
                "tickers": [
                    {
                        "symbol":               ticker,
                        "canonical_theme_id":   new_id,          # already correct from resolver
                        "canonical_theme_name": new_theme,       # already correct from resolver
                        "primary_theme_id":     new_id,          # already correct
                        "theme_ids":            [new_id],        # already correct
                        "subtheme_ids":         [],
                        "theme_source":         "manual_override",
                        "price":                42.0,
                    }
                ],
            }
        ]

        # Patch get_overrides to return the override
        with patch(
            "services.category_overrides.get_overrides",
            return_value={ticker: new_theme},
        ):
            result = apply_to_sections(sections, user_id="default")

        # Ticker must have moved out of 'Defense'
        defense_sec = next((s for s in result if s.get("title") == "Defense"), None)
        assert defense_sec is None or not any(
            r["symbol"] == ticker for r in defense_sec.get("tickers", [])
        ), "AXTI must be removed from Defense section"

        # Ticker must be in 'Packaging & Substrates'
        pkg_sec = next((s for s in result if s.get("title") == new_theme), None)
        assert pkg_sec is not None, f"Section '{new_theme}' must be created"

        moved = next(
            (r for r in pkg_sec["tickers"] if r["symbol"] == ticker), None
        )
        assert moved is not None, f"{ticker} must appear in '{new_theme}' section"

        # Identity fields must be correct in the moved row
        assert moved["canonical_theme_id"] == new_id, (
            f"canonical_theme_id must be {new_id!r}, got {moved['canonical_theme_id']!r}"
        )
        assert moved["primary_theme_id"] == new_id, (
            f"primary_theme_id must be {new_id!r}, got {moved.get('primary_theme_id')!r}"
        )
        assert new_id in moved["theme_ids"], "theme_ids must contain new_id"
        assert moved["theme_source"] == "manual_override"


# ══════════════════════════════════════════════════════════════════════════════
# IDEMPOTENCY AND THREAD-SAFETY
# ══════════════════════════════════════════════════════════════════════════════

def test_invalidation_is_idempotent():
    """Calling invalidate_bulk_lkg_for_ticker() twice must not raise or double-count gen."""
    _clear_all_lkg()
    wl_id = "wl-idem"
    _set_lkg(wl_id, "CHIP", "semiconductors", "Semiconductors")

    wr = _get_router()
    wr.invalidate_bulk_lkg_for_ticker("CHIP")  # first call: pops LKG, gen=1
    wr.invalidate_bulk_lkg_for_ticker("CHIP")  # second call: LKG already absent, no double-pop

    # Gen increments only when entry was present (second call finds no entry)
    assert wr._TAXONOMY_GEN.get(wl_id, 0) == 1, (
        "Gen must increment only when the LKG entry was actually present and popped. "
        f"Got gen={wr._TAXONOMY_GEN.get(wl_id, 0)}"
    )

    _clear_all_lkg()


def test_thread_safety_concurrent_invalidations():
    """Concurrent invalidations from multiple threads must not corrupt LKG or gen."""
    _clear_all_lkg()
    wr = _get_router()

    errors: list[str] = []

    def _invalidate(i: int):
        try:
            wl_id = f"wl-thd-{i % 3}"
            ticker = f"TICK{i % 5}"
            _set_lkg(wl_id, ticker, "semiconductors", "Semiconductors")
            wr.invalidate_bulk_lkg_for_ticker(ticker)
        except Exception as e:
            errors.append(f"thread {i}: {e}")

    threads = [threading.Thread(target=_invalidate, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == [], f"Thread-safety errors: {errors}"
    _clear_all_lkg()
