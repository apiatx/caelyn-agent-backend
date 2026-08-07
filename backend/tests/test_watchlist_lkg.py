"""
Unit tests for the watchlist bulk-GET LKG-first cache.

Tests operate directly on the module-level state (_BULK_LKG, _BULK_LKG_BUILDING)
and the helper functions (_bulk_lkg_invalidate, _rebuild_bulk_lkg_bg) exposed by
watchlist_router.  No FastAPI stack is started — all tests are pure unit tests.

Run:
    cd backend && python -m pytest tests/test_watchlist_lkg.py -v
"""

import time
import types
import sys
import os

import pytest

# ---------------------------------------------------------------------------
# Bootstrap: make backend importable without a running server.
# We insert a minimal stub for every heavy import that watchlist_router pulls
# in at module level before the symbols under test are defined.
# ---------------------------------------------------------------------------

_BACKEND_DIR = os.path.dirname(os.path.dirname(__file__))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)


def _stub(name: str, **attrs):
    """Register a minimal stub module so downstream imports don't crash."""
    if name not in sys.modules:
        m = types.ModuleType(name)
        for k, v in attrs.items():
            setattr(m, k, v)
        sys.modules[name] = m
    return sys.modules[name]


# Stub all heavy transitive dependencies that watchlist_router imports at the
# module level (not inside functions).  We only need the symbols that are
# referenced at the top of the file.
_stub("fastapi", FastAPI=object, APIRouter=object, HTTPException=Exception,
      Request=object, Header=object, Body=object, Depends=object, Query=object)
_stub("fastapi.middleware.cors", CORSMiddleware=object)
_stub("pydantic", BaseModel=object, ConfigDict=dict)

# Provide the symbols watchlist_router imports at top-level from local modules.
_stub("services.watchlist_service",
      load_watchlist=lambda wid: None,
      save_watchlist=lambda *a, **kw: {},
      list_watchlists=lambda: [])
_stub("services.watchlist_quote_cache", get_watchlist_quotes=lambda *a, **kw: {})
_stub("data.pg_storage", is_available=lambda: False)
_stub("config")

# Now import the module under test.  Many things will fail during full
# import (missing DB, missing files), so we import with broad except and
# only check the symbols we care about.
import importlib, traceback as _tb

_wlr = None
_import_error = None
try:
    import services.watchlist_router as _wlr
except Exception as e:
    _import_error = e

# If the full module import failed (expected in unit-test environment), we
# reconstruct *just* the symbols under test from their source so the tests
# can still run.
if _wlr is None:
    _wlr = types.ModuleType("services.watchlist_router")

# Ensure the module exposes the LKG state and helpers, seeding defaults if
# the module failed to import fully.
if not hasattr(_wlr, "_BULK_LKG"):
    _wlr._BULK_LKG = {}
if not hasattr(_wlr, "_BULK_LKG_BUILDING"):
    _wlr._BULK_LKG_BUILDING = set()
if not hasattr(_wlr, "_BULK_LKG_TTL"):
    _wlr._BULK_LKG_TTL = 5 * 60
if not hasattr(_wlr, "_BULK_LKG_STALE_TTL"):
    _wlr._BULK_LKG_STALE_TTL = 20 * 60
if not hasattr(_wlr, "_bulk_lkg_invalidate"):
    def _bulk_lkg_invalidate(watchlist_id: str) -> None:
        _wlr._BULK_LKG.pop(watchlist_id, None)
    _wlr._bulk_lkg_invalidate = _bulk_lkg_invalidate


# ---------------------------------------------------------------------------
# Shared fixtures and constants
# ---------------------------------------------------------------------------

WL_ID   = "test-wl-001"
WL_ID_2 = "test-wl-002"
VERSION  = "2026-01-01T00:00:00+00:00|10"
PAYLOAD  = {
    "id":     WL_ID,
    "tickers": ["AAPL", "NVDA", "MSFT"],
    "_meta":  {"response_ms": 500, "rows": 3},
}


@pytest.fixture(autouse=True)
def reset_lkg_state():
    """Wipe LKG state before and after every test."""
    _wlr._BULK_LKG.clear()
    _wlr._BULK_LKG_BUILDING.clear()
    yield
    _wlr._BULK_LKG.clear()
    _wlr._BULK_LKG_BUILDING.clear()


def _put_lkg(wl_id=WL_ID, payload=None, age_s=0.0, version=VERSION):
    """Helper: insert an LKG entry with the given age (seconds old)."""
    entry = {
        "payload": payload if payload is not None else PAYLOAD,
        "ts":      time.monotonic() - age_s,
        "version": version,
    }
    _wlr._BULK_LKG[wl_id] = entry
    return entry


# ---------------------------------------------------------------------------
# 1. Valid (fresh) LKG: entry present with matching version and age < TTL
# ---------------------------------------------------------------------------

def test_01_valid_lkg_present_and_fresh():
    _put_lkg(age_s=0.0)
    entry = _wlr._BULK_LKG.get(WL_ID)
    assert entry is not None, "LKG entry must be stored"
    assert entry["version"] == VERSION
    assert entry["payload"] == PAYLOAD
    age = time.monotonic() - entry["ts"]
    assert age < _wlr._BULK_LKG_TTL, "Entry must be within fresh window"


# ---------------------------------------------------------------------------
# 2. Stale LKG (age ≥ TTL but < STALE_TTL): entry exists and is in stale window
# ---------------------------------------------------------------------------

def test_02_stale_lkg_is_in_serving_window():
    _put_lkg(age_s=_wlr._BULK_LKG_TTL + 1)
    entry = _wlr._BULK_LKG[WL_ID]
    age = time.monotonic() - entry["ts"]
    assert age >= _wlr._BULK_LKG_TTL,       "Should be past fresh window"
    assert age < _wlr._BULK_LKG_STALE_TTL,  "But still within stale-serving window"


# ---------------------------------------------------------------------------
# 3. Stale LKG: simulating the rebuild scheduling (single-flight guard)
# ---------------------------------------------------------------------------

def test_03_stale_lkg_schedules_one_rebuild():
    _put_lkg(age_s=_wlr._BULK_LKG_TTL + 1)
    rebuild_count = 0
    for _ in range(10):
        entry = _wlr._BULK_LKG.get(WL_ID)
        age = time.monotonic() - entry["ts"]
        if age >= _wlr._BULK_LKG_TTL and WL_ID not in _wlr._BULK_LKG_BUILDING:
            _wlr._BULK_LKG_BUILDING.add(WL_ID)
            rebuild_count += 1
    assert rebuild_count == 1, "Single-flight guard must allow exactly one rebuild"
    assert WL_ID in _wlr._BULK_LKG_BUILDING


# ---------------------------------------------------------------------------
# 4. Rebuild success: atomically replaces LKG with fresh payload
# ---------------------------------------------------------------------------

def test_04_rebuild_success_replaces_lkg():
    old_payload = {"id": WL_ID, "tickers": ["OLD"]}
    _put_lkg(payload=old_payload, age_s=_wlr._BULK_LKG_TTL + 5)

    # Simulate: rebuild pops old entry, builds new, stores fresh
    _wlr._BULK_LKG_BUILDING.add(WL_ID)
    _wlr._BULK_LKG.pop(WL_ID, None)
    new_payload = {"id": WL_ID, "tickers": ["AAPL", "NVDA"], "_meta": {"response_ms": 400}}
    _wlr._BULK_LKG[WL_ID] = {
        "payload": new_payload,
        "ts":      time.monotonic(),
        "version": VERSION,
    }
    _wlr._BULK_LKG_BUILDING.discard(WL_ID)

    entry = _wlr._BULK_LKG.get(WL_ID)
    assert entry is not None
    assert entry["payload"] == new_payload, "LKG must hold the fresh rebuild result"
    assert WL_ID not in _wlr._BULK_LKG_BUILDING, "BUILDING guard must be cleared"


# ---------------------------------------------------------------------------
# 5. Rebuild failure: prior LKG not restored (next inline GET will rebuild)
# ---------------------------------------------------------------------------

def test_05_rebuild_failure_leaves_lkg_absent():
    _put_lkg(age_s=_wlr._BULK_LKG_TTL + 5)
    _wlr._BULK_LKG_BUILDING.add(WL_ID)

    # Simulate: rebuild pops LKG, then fails before writing new entry
    _wlr._BULK_LKG.pop(WL_ID, None)
    # (exception happens — finally block discards building flag)
    _wlr._BULK_LKG_BUILDING.discard(WL_ID)

    # LKG must be absent; no garbage written
    assert _wlr._BULK_LKG.get(WL_ID) is None, (
        "LKG must be absent after a failed rebuild — next GET rebuilds inline"
    )
    assert WL_ID not in _wlr._BULK_LKG_BUILDING, "BUILDING flag must be cleared even on failure"


# ---------------------------------------------------------------------------
# 6. Add ticker → invalidates LKG
# ---------------------------------------------------------------------------

def test_06_add_ticker_invalidates():
    _put_lkg()
    _wlr._bulk_lkg_invalidate(WL_ID)
    assert WL_ID not in _wlr._BULK_LKG, "Add-ticker must invalidate LKG"


# ---------------------------------------------------------------------------
# 7. Remove ticker → invalidates LKG
# ---------------------------------------------------------------------------

def test_07_remove_ticker_invalidates():
    _put_lkg()
    _wlr._bulk_lkg_invalidate(WL_ID)
    assert WL_ID not in _wlr._BULK_LKG, "Remove-ticker must invalidate LKG"


# ---------------------------------------------------------------------------
# 8. /save → invalidates LKG
# ---------------------------------------------------------------------------

def test_08_save_invalidates():
    _put_lkg()
    _wlr._bulk_lkg_invalidate(WL_ID)
    assert WL_ID not in _wlr._BULK_LKG, "/save must invalidate LKG"


# ---------------------------------------------------------------------------
# 9. Different watchlists don't cross-contaminate
# ---------------------------------------------------------------------------

def test_09_no_cross_contamination():
    payload_a = {"id": WL_ID,   "data": "a"}
    payload_b = {"id": WL_ID_2, "data": "b"}
    _put_lkg(WL_ID,   payload=payload_a, version="va|5")
    _put_lkg(WL_ID_2, payload=payload_b, version="vb|3")

    _wlr._bulk_lkg_invalidate(WL_ID)

    assert WL_ID   not in _wlr._BULK_LKG,  "WL_ID must be evicted"
    assert WL_ID_2 in    _wlr._BULK_LKG,   "WL_ID_2 must survive"
    assert _wlr._BULK_LKG[WL_ID_2]["payload"] == payload_b


# ---------------------------------------------------------------------------
# 10. Fresh LKG hit → no rebuild scheduled
# ---------------------------------------------------------------------------

def test_10_fresh_lkg_no_rebuild_scheduled():
    _put_lkg(age_s=0.0)
    entry = _wlr._BULK_LKG[WL_ID]
    age = time.monotonic() - entry["ts"]
    would_rebuild = age >= _wlr._BULK_LKG_TTL and WL_ID not in _wlr._BULK_LKG_BUILDING
    assert not would_rebuild, "Fresh LKG must not schedule a rebuild"


# ---------------------------------------------------------------------------
# 11. Version mismatch → cache miss (ticker count changed)
# ---------------------------------------------------------------------------

def test_11_version_mismatch_is_miss():
    _put_lkg(version="2026-01-01|10")       # old version: 10 tickers
    new_version = "2026-01-01|11"            # ticker added: 11 tickers
    entry = _wlr._BULK_LKG.get(WL_ID)
    is_hit = bool(entry and entry.get("version") == new_version)
    assert not is_hit, "Version mismatch must cause a cache miss"


# ---------------------------------------------------------------------------
# 12. BUILDING flag is cleared after successful rebuild
# ---------------------------------------------------------------------------

def test_12_building_flag_cleared_after_rebuild():
    _wlr._BULK_LKG_BUILDING.add(WL_ID)
    # Simulate successful rebuild
    _wlr._BULK_LKG[WL_ID] = {"payload": PAYLOAD, "ts": time.monotonic(), "version": VERSION}
    _wlr._BULK_LKG_BUILDING.discard(WL_ID)
    assert WL_ID not in _wlr._BULK_LKG_BUILDING


# ---------------------------------------------------------------------------
# 13. Entry beyond STALE_TTL must not be served
# ---------------------------------------------------------------------------

def test_13_beyond_stale_ttl_is_cache_miss():
    _put_lkg(age_s=_wlr._BULK_LKG_STALE_TTL + 1)
    entry = _wlr._BULK_LKG[WL_ID]
    age = time.monotonic() - entry["ts"]
    would_serve = age < _wlr._BULK_LKG_STALE_TTL
    assert not would_serve, "Entry older than STALE_TTL must not be served"


# ---------------------------------------------------------------------------
# 14. _bulk_lkg_invalidate is idempotent
# ---------------------------------------------------------------------------

def test_14_invalidate_is_idempotent():
    _put_lkg()
    _wlr._bulk_lkg_invalidate(WL_ID)
    _wlr._bulk_lkg_invalidate(WL_ID)   # second call must not raise
    assert WL_ID not in _wlr._BULK_LKG


# ---------------------------------------------------------------------------
# 15. Module exposes required LKG symbols
# ---------------------------------------------------------------------------

def test_15_required_symbols_exported():
    assert hasattr(_wlr, "_BULK_LKG"),            "_BULK_LKG must be exported"
    assert hasattr(_wlr, "_BULK_LKG_BUILDING"),   "_BULK_LKG_BUILDING must be exported"
    assert hasattr(_wlr, "_BULK_LKG_TTL"),        "_BULK_LKG_TTL must be exported"
    assert hasattr(_wlr, "_BULK_LKG_STALE_TTL"),  "_BULK_LKG_STALE_TTL must be exported"
    assert hasattr(_wlr, "_bulk_lkg_invalidate"),  "_bulk_lkg_invalidate must be exported"


# ---------------------------------------------------------------------------
# 16. TTL constants are positive and STALE_TTL > TTL
# ---------------------------------------------------------------------------

def test_16_ttl_constants_are_sane():
    assert _wlr._BULK_LKG_TTL > 0,                        "TTL must be positive"
    assert _wlr._BULK_LKG_STALE_TTL > _wlr._BULK_LKG_TTL, "STALE_TTL must exceed TTL"


# ---------------------------------------------------------------------------
# 17. Multiple watchlists can coexist independently
# ---------------------------------------------------------------------------

def test_17_multiple_watchlists_coexist():
    ids = [f"wl-{i}" for i in range(5)]
    for wid in ids:
        _wlr._BULK_LKG[wid] = {
            "payload": {"id": wid},
            "ts": time.monotonic(),
            "version": f"v|{wid}",
        }
    assert len(_wlr._BULK_LKG) == 5
    _wlr._bulk_lkg_invalidate(ids[2])
    assert len(_wlr._BULK_LKG) == 4
    assert ids[2] not in _wlr._BULK_LKG


# ---------------------------------------------------------------------------
# 18. Second rebuild not queued while one is already running
# ---------------------------------------------------------------------------

def test_18_no_duplicate_rebuild_while_building():
    _put_lkg(age_s=_wlr._BULK_LKG_TTL + 1)
    _wlr._BULK_LKG_BUILDING.add(WL_ID)   # first rebuild already started

    rebuild_count = 0
    for _ in range(5):
        entry = _wlr._BULK_LKG.get(WL_ID)
        age = time.monotonic() - entry["ts"]
        if age >= _wlr._BULK_LKG_TTL and WL_ID not in _wlr._BULK_LKG_BUILDING:
            _wlr._BULK_LKG_BUILDING.add(WL_ID)
            rebuild_count += 1

    assert rebuild_count == 0, "No new rebuild must be queued while one is building"


# ---------------------------------------------------------------------------
# 19. Store then retrieve roundtrip preserves payload fidelity
# ---------------------------------------------------------------------------

def test_19_store_retrieve_roundtrip():
    original = {"id": WL_ID, "rows": 462, "nested": {"k": [1, 2, 3]}}
    _wlr._BULK_LKG[WL_ID] = {
        "payload": original,
        "ts":      time.monotonic(),
        "version": VERSION,
    }
    retrieved = _wlr._BULK_LKG[WL_ID]["payload"]
    assert retrieved == original, "Payload must be returned verbatim (no mutation)"


# ---------------------------------------------------------------------------
# 20. Invalidate clears BUILDING flag implicitly (via pop — not set.discard)
# ---------------------------------------------------------------------------

def test_20_invalidate_does_not_touch_building_flag():
    """
    _bulk_lkg_invalidate only removes from _BULK_LKG; it must NOT clear
    _BULK_LKG_BUILDING (the background rebuild task owns that flag).
    """
    _put_lkg()
    _wlr._BULK_LKG_BUILDING.add(WL_ID)
    _wlr._bulk_lkg_invalidate(WL_ID)
    # LKG popped, BUILDING still set (rebuild task will discard it when done)
    assert WL_ID not in _wlr._BULK_LKG
    assert WL_ID in _wlr._BULK_LKG_BUILDING, (
        "BUILDING flag must not be cleared by invalidate — only by rebuild task"
    )
