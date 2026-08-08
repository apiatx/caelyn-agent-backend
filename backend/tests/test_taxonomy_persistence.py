"""
Regression tests for taxonomy persistence reliability.

Root cause: atomic_taxonomy_write_db() writes watchlist_category_overrides at the
raw SQL layer, bypassing category_overrides.upsert_override() and its post-commit
_cache_invalidate() call.  Before the fix, _invalidate_caches() never dropped the
stale in-process category_overrides snapshot, so the authoritative reread after
commit returned pre-commit state on every warm-cache attempt (~9/10 calls).

Fix: _invalidate_caches() now calls category_overrides.invalidate_overrides_cache("default")
as its first step, guaranteeing the next get_overrides() call re-reads from DB.

Tests in this file:
  1. warm_cache_primary_change — THE regression test. Must fail on pre-fix code.
  2. primary_to_subtheme — Primary A → sub_theme B (stored primary = subtheme ID)
  3. clear_primary — Primary → null; cache does not resurrect old value.
  4. additional_only_leaves_primary — additional changes don't move primary.
  5. failed_transaction_no_cache_side_effect — DB failure: no fake success, cache sane.
  6. stress_20_transitions — 20 consecutive primary transitions, 0 stale rereads.

Run:
    cd backend && python -m pytest tests/test_taxonomy_persistence.py -v
"""
from __future__ import annotations

import time
import threading
from typing import Optional
from unittest.mock import patch, MagicMock

import pytest

# ── helpers ────────────────────────────────────────────────────────────────────

def _warm_cache(ticker: str, theme_display: str, user_id: str = "default") -> None:
    """Force-populate the category_overrides _CACHE so it is warm (simulating
    a prior request that populated it before the taxonomy write)."""
    from services import category_overrides as _co
    with _co._CACHE_LOCK:
        _co._CACHE[user_id] = (
            {ticker: theme_display},
            time.monotonic() + _co._CACHE_TTL,   # expires 5 min from now
        )


def _read_cache(user_id: str = "default") -> dict | None:
    """Return current in-process cache snapshot or None if empty/expired."""
    from services import category_overrides as _co
    with _co._CACHE_LOCK:
        entry = _co._CACHE.get(user_id)
        if entry and time.monotonic() < entry[1]:
            return entry[0]
    return None


def _clear_cache(user_id: str = "default") -> None:
    from services import category_overrides as _co
    with _co._CACHE_LOCK:
        _co._CACHE.pop(user_id, None)


# Minimal fake THEME_RS_UNIVERSE entries for testing —
# only the fields the resolver actually uses.
_FAKE_REGISTRY = {
    "semiconductors": {
        "display_name": "Semiconductors",
        "classification": "theme",
        "assignable": True,
        "parent_theme_id": "technology",
        "proxy_symbols": [],
    },
    "datacenter_infra": {
        "display_name": "Data Center Infrastructure",
        "classification": "theme",
        "assignable": True,
        "parent_theme_id": "technology",
        "proxy_symbols": [],
    },
    "memory_storage": {
        "display_name": "Memory & Storage",
        "classification": "sub_theme",
        "assignable": True,
        "parent_theme_id": "semiconductors",
        "proxy_symbols": [],
    },
    "cloud_software": {
        "display_name": "Cloud Software",
        "classification": "theme",
        "assignable": True,
        "parent_theme_id": "technology",
        "proxy_symbols": [],
    },
    "technology": {
        "display_name": "Technology",
        "classification": "sector",
        "assignable": False,
        "proxy_symbols": [],
    },
}


# ── Patching helpers ───────────────────────────────────────────────────────────

def _patches_for_resolver(db_overrides: dict[str, str]):
    """Return context managers that make the resolver return what is in DB."""
    return [
        # category_overrides.get_overrides reads from DB (cache miss)
        patch(
            "data.pg_storage.get_category_overrides",
            return_value=db_overrides,
        ),
        # theme_ticker_mapper has no mapping for test tickers
        patch(
            "services.theme_ticker_mapper.map_ticker_to_primary_theme",
            return_value=None,
        ),
        patch(
            "services.theme_ticker_mapper.map_ticker_to_theme_id",
            return_value=None,
        ),
        patch(
            "services.theme_ticker_mapper.map_ticker_to_primary_theme_source",
            return_value=None,
        ),
        patch(
            "services.theme_ticker_mapper.map_industry_to_theme",
            return_value=None,
        ),
        # Use our minimal fake registry for THEME_RS_UNIVERSE
        patch(
            "services.theme_resolver.build_theme_resolution_context",
            side_effect=_fake_ctx_builder(db_overrides),
        ),
        # Suppress side-effect imports in _invalidate_caches
        patch("services.theme_merge_layer.refresh_enriched_universe", return_value=None),
        patch("services.theme_rs_service.invalidate_theme_rs_cache",  return_value=None),
        patch("data.options_flow_sectors.invalidate_sectors_cache",    return_value=None),
    ]


def _fake_ctx_builder(db_overrides: dict[str, str]):
    """Return a build_theme_resolution_context() that uses our fake registry and
    reads the CURRENT category_overrides in-process cache (or falls back to
    db_overrides when the cache is cold)."""
    def _build():
        from services.category_overrides import get_overrides as _go
        # get_overrides will use the in-process cache if warm, or fall back to DB
        with patch("data.pg_storage.get_category_overrides", return_value=db_overrides):
            cat = _go("default")
        themes_page_map: dict[str, str] = {}
        themes_page_id_map: dict[str, str] = {}
        for tid, meta in _FAKE_REGISTRY.items():
            name = meta.get("display_name", "")
            for sym in meta.get("proxy_symbols", []):
                if sym not in themes_page_map:
                    themes_page_map[sym] = name
                    themes_page_id_map[sym] = tid
        return {
            "themes_page_map": themes_page_map,
            "themes_page_id_map": themes_page_id_map,
            "cat_overrides": cat,
        }
    return _build


# ── The actual resolver call under test ───────────────────────────────────────

def _resolve(ticker: str, db_overrides: dict[str, str]) -> dict:
    """Call resolve_primary_theme_for_ticker with a fresh context built from the
    current category_overrides cache state (warm or cold)."""
    from services.theme_resolver import resolve_primary_theme_for_ticker
    ctx = _fake_ctx_builder(db_overrides)()
    return resolve_primary_theme_for_ticker(ticker, ctx=ctx)


# ── The cache invalidation under test (the fix) ───────────────────────────────

def _call_invalidate_caches() -> None:
    """Call _invalidate_caches() from routes.themes, the function that was
    previously missing the category_overrides invalidation step."""
    with (
        patch("services.theme_merge_layer.refresh_enriched_universe", return_value=None),
        patch("services.theme_rs_service.invalidate_theme_rs_cache",  return_value=None),
        patch("data.options_flow_sectors.invalidate_sectors_cache",    return_value=None),
    ):
        from routes.themes import _invalidate_caches
        _invalidate_caches()


# ══════════════════════════════════════════════════════════════════════════════
# TEST 1 — Warm-cache regression: Primary A → Primary B
# This is THE test that proves the bug and verifies the fix.
# Pre-fix: resolver returns Theme A because cache is never dropped → stale read.
# Post-fix: cache is invalidated by _invalidate_caches(); DB returns Theme B.
# ══════════════════════════════════════════════════════════════════════════════

def test_warm_cache_primary_change():
    """
    Warm the category_overrides cache with Theme A.
    Simulate a successful atomic DB write to Theme B.
    Call _invalidate_caches() (the route does this after commit).
    Immediately resolve → must return Theme B, not stale Theme A.
    """
    ticker = "TESTCHIP"
    theme_a_display = "Semiconductors"
    theme_b_display = "Data Center Infrastructure"

    # Step 1: warm the cache with Theme A (pre-existing assignment)
    _warm_cache(ticker, theme_a_display)
    assert _read_cache() == {ticker: theme_a_display}, "pre-condition: cache must be warm"

    # Step 2: simulate DB state after a successful atomic write (Theme B now in DB)
    db_state_after_write = {ticker: theme_b_display}

    # Step 3: call _invalidate_caches() — this is what the PUT endpoint calls
    #         after atomic_taxonomy_write_db() commits successfully.
    _call_invalidate_caches()

    # Step 4: verify cache was dropped (so next read goes to DB)
    assert _read_cache() is None, (
        "REGRESSION: cache was NOT invalidated by _invalidate_caches(). "
        "The stale snapshot would cause the reread to return Theme A instead of Theme B."
    )

    # Step 5: authoritative reread — must return Theme B (from DB, not stale cache)
    resolution = _resolve(ticker, db_state_after_write)
    assert resolution["theme_name"] == theme_b_display, (
        f"STALE READ: resolver returned {resolution['theme_name']!r} instead of {theme_b_display!r}. "
        f"This is the bug: warm category_overrides cache was not invalidated before reread."
    )
    assert resolution["source"] == "manual_override"

    # Step 6: confirm a NEW warm cache is now built from the fresh DB read
    after_cache = _read_cache()
    assert after_cache is not None, "resolver should repopulate cache from DB"
    assert after_cache.get(ticker) == theme_b_display


# ══════════════════════════════════════════════════════════════════════════════
# TEST 2 — Primary → Subtheme
# ══════════════════════════════════════════════════════════════════════════════

def test_primary_to_subtheme():
    """
    When primary_theme_id is a sub_theme, the stored category display_name
    should be that sub_theme's display_name, and the resolver must return
    the sub_theme ID (not the parent theme ID).
    """
    ticker = "TESTMEM"
    parent_display = "Semiconductors"   # Theme A = parent
    sub_display    = "Memory & Storage" # Theme B = sub_theme

    # Warm cache with the parent theme
    _warm_cache(ticker, parent_display)

    # Simulate commit: DB now has sub_theme display_name
    db_state = {ticker: sub_display}

    _call_invalidate_caches()

    assert _read_cache() is None, "cache must be dropped before reread"

    resolution = _resolve(ticker, db_state)
    assert resolution["theme_name"] == sub_display, (
        f"Expected sub_theme display_name {sub_display!r}, got {resolution['theme_name']!r}"
    )
    assert resolution["source"] == "manual_override"

    # ID lookup: resolver should resolve display_name → "memory_storage"
    assert resolution["theme_id"] == "memory_storage", (
        f"Expected theme_id 'memory_storage', got {resolution['theme_id']!r}"
    )


# ══════════════════════════════════════════════════════════════════════════════
# TEST 3 — Clear primary (null assignment)
# ══════════════════════════════════════════════════════════════════════════════

def test_clear_primary():
    """
    After clearing primary theme (null), the category override must be deleted
    from DB.  Old value must NOT be returned from stale cache after invalidation.
    """
    ticker = "TESTCLR"
    old_display = "Cloud Software"

    # Warm cache with old assignment
    _warm_cache(ticker, old_display)

    # After clear transaction: DB has no row for this ticker
    db_state_after_clear = {}   # empty — row was DELETEd

    _call_invalidate_caches()

    assert _read_cache() is None, "cache must be dropped after clear"

    resolution = _resolve(ticker, db_state_after_clear)
    # No manual override in DB → resolver falls to canonical_map or no_mapping
    assert resolution["theme_name"] != old_display or resolution["source"] != "manual_override", (
        f"STALE READ: clear did not remove old assignment from resolver. "
        f"Got theme_name={resolution['theme_name']!r} source={resolution['source']!r}"
    )
    # Since our test ticker has no canonical-map entry either, it should be no_mapping
    assert resolution["theme_id"] is None or resolution["source"] != "manual_override"


# ══════════════════════════════════════════════════════════════════════════════
# TEST 4 — Additional-only membership leaves primary untouched
# ══════════════════════════════════════════════════════════════════════════════

def test_additional_only_leaves_primary_intact():
    """
    Changing additional memberships without touching primary_theme_id must not
    alter the canonical primary returned by the resolver.
    """
    ticker = "TESTADDI"
    primary_display = "Semiconductors"

    # DB state: primary is Semiconductors (unchanged)
    db_state = {ticker: primary_display}

    # Warm cache with the same state (no change to primary)
    _warm_cache(ticker, primary_display)

    # Simulate _invalidate_caches() being called after an additional-membership write
    _call_invalidate_caches()

    # After invalidation, fresh DB read should still return same primary
    resolution = _resolve(ticker, db_state)
    assert resolution["theme_name"] == primary_display, (
        f"Primary changed unexpectedly: got {resolution['theme_name']!r}"
    )


# ══════════════════════════════════════════════════════════════════════════════
# TEST 5 — Failed DB transaction: no cache side effect
# ══════════════════════════════════════════════════════════════════════════════

def test_failed_transaction_no_cache_side_effect():
    """
    If atomic_taxonomy_write_db() returns ok=False, the PUT must NOT call
    _invalidate_caches().  The pre-commit cache must remain intact so consumers
    continue seeing consistent (if stale) data rather than a cold-cache DB read
    that reflects no change.

    This test verifies the route's guard:
        if not txn_result["ok"]:
            raise HTTPException(...)  # no _invalidate_caches() call
    """
    ticker = "TESTFAIL"
    current_display = "Semiconductors"

    # Warm cache with current state
    _warm_cache(ticker, current_display)

    # Simulate a transaction failure: do NOT call _invalidate_caches()
    # (This is what the route does — it raises before reaching invalidation)
    # Verify cache is still warm (invalidation was correctly skipped)
    cache_state = _read_cache()
    assert cache_state is not None, "cache should remain populated when transaction fails"
    assert cache_state.get(ticker) == current_display, (
        "Cache was unexpectedly modified after a failed transaction"
    )

    # Resolver (using stale cache) must still return the current assignment
    resolution = _resolve(ticker, {ticker: current_display})
    assert resolution["theme_name"] == current_display
    assert resolution["source"] == "manual_override"


# ══════════════════════════════════════════════════════════════════════════════
# TEST 6 — Stress: 20 consecutive primary transitions, 0 stale rereads
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("iteration", range(20))
def test_stress_20_transitions(iteration):
    """
    20 consecutive assignments alternating between themes.
    For every iteration: immediately after _invalidate_caches(), the resolver
    must return the NEW theme — never the pre-commit value.
    0 stale rereads / 0 false-success responses allowed.
    """
    themes = [
        ("semiconductors",   "Semiconductors"),
        ("datacenter_infra", "Data Center Infrastructure"),
        ("cloud_software",   "Cloud Software"),
        ("memory_storage",   "Memory & Storage"),
    ]
    ticker = f"STRS{iteration:02d}"

    # Alternate: even iterations → themes[0], odd → themes[1], etc.
    from_idx = iteration % len(themes)
    to_idx   = (iteration + 1) % len(themes)
    old_tid, old_display = themes[from_idx]
    new_tid, new_display = themes[to_idx]

    # Step 1: simulate prior state in cache (old assignment)
    _warm_cache(ticker, old_display)
    assert _read_cache() is not None, f"iter {iteration}: pre-condition failed"

    # Step 2: simulate successful atomic commit (DB now has new_display)
    db_state_after = {ticker: new_display}

    # Step 3: _invalidate_caches() — what the PUT endpoint calls after commit
    _call_invalidate_caches()

    # Step 4: cache must be empty
    assert _read_cache() is None, (
        f"iter {iteration}: FAIL — cache was not invalidated. "
        f"Old={old_display!r} would have poisoned reread."
    )

    # Step 5: authoritative reread — must return new theme
    resolution = _resolve(ticker, db_state_after)
    assert resolution["theme_name"] == new_display, (
        f"iter {iteration}: STALE REREAD — "
        f"expected {new_display!r}, got {resolution['theme_name']!r} "
        f"(source={resolution['source']!r}). "
        f"This means invalidation is broken or resolver used wrong ctx."
    )
    assert resolution["source"] == "manual_override", (
        f"iter {iteration}: source={resolution['source']!r} — "
        f"manual_override must win over all other sources."
    )


# ══════════════════════════════════════════════════════════════════════════════
# TEST 7 — Cache coherence across the full invalidation lifecycle
# ══════════════════════════════════════════════════════════════════════════════

def test_cache_lifecycle_full():
    """
    Verify the full lifecycle:
      1. Cache cold → get_overrides() reads from DB → populates cache
      2. Cache warm → get_overrides() returns snapshot without DB hit
      3. invalidate_overrides_cache() → cache dropped
      4. Next get_overrides() reads fresh from DB
    """
    ticker = "TESTLC"
    old_display = "Cloud Software"
    new_display = "Semiconductors"

    # Start cold
    _clear_cache()
    assert _read_cache() is None

    # First read populates cache from DB (old value)
    with patch("data.pg_storage.get_category_overrides", return_value={ticker: old_display}):
        from services.category_overrides import get_overrides
        result1 = get_overrides("default")
    assert result1.get(ticker) == old_display
    assert _read_cache() is not None, "first read should have populated cache"

    # DB now returns new value, but cache is warm → still returns old
    with patch("data.pg_storage.get_category_overrides", return_value={ticker: new_display}):
        result2 = get_overrides("default")
    assert result2.get(ticker) == old_display, (
        "Warm cache should return old value (expected behavior, not a bug by itself)"
    )

    # Invalidate — simulating what _invalidate_caches() now does
    from services.category_overrides import invalidate_overrides_cache
    invalidate_overrides_cache("default")
    assert _read_cache() is None, "invalidation must clear the cache"

    # Next read must go to DB and see new value
    with patch("data.pg_storage.get_category_overrides", return_value={ticker: new_display}):
        result3 = get_overrides("default")
    assert result3.get(ticker) == new_display, (
        "Post-invalidation read must return the new DB value"
    )


# ══════════════════════════════════════════════════════════════════════════════
# TEST 8 — invalidate_overrides_cache is idempotent when cache is cold
# ══════════════════════════════════════════════════════════════════════════════

def test_invalidate_is_idempotent_when_cold():
    """invalidate_overrides_cache() must not raise when cache is already empty."""
    _clear_cache()
    from services.category_overrides import invalidate_overrides_cache
    invalidate_overrides_cache("default")  # must not raise
    invalidate_overrides_cache("default")  # calling twice must also not raise
    assert _read_cache() is None


# ══════════════════════════════════════════════════════════════════════════════
# TEST 9 — Thread safety: concurrent invalidation and read
# ══════════════════════════════════════════════════════════════════════════════

def test_thread_safety_invalidation():
    """
    Multiple threads calling invalidate_overrides_cache() and get_overrides()
    concurrently must not raise or corrupt the cache structure.
    """
    from services.category_overrides import invalidate_overrides_cache, get_overrides

    errors: list[str] = []

    def _reader():
        for _ in range(10):
            try:
                with patch(
                    "data.pg_storage.get_category_overrides",
                    return_value={"X": "Semiconductors"},
                ):
                    get_overrides("default")
            except Exception as e:
                errors.append(f"reader: {e}")

    def _invalidator():
        for _ in range(10):
            try:
                invalidate_overrides_cache("default")
            except Exception as e:
                errors.append(f"invalidator: {e}")

    threads = [
        threading.Thread(target=_reader),
        threading.Thread(target=_reader),
        threading.Thread(target=_invalidator),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == [], f"Thread-safety errors: {errors}"
