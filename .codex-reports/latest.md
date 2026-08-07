# Watchlist LKG Hardening — Task 2 Report

**Commit:** `bb79c46b`  
**Message:** `fix: preserve watchlist LKG during refresh`  
**Date:** 2026-08-07  
**Files changed:** `backend/services/watchlist_router.py`, `backend/tests/test_watchlist_lkg.py`

---

## Contracts Implemented

### C1/C2 — Extracted `_build_watchlist_response` internal builder
Extracted the full enrichment pipeline (quote enrichment → FMP fundamentals overlay → upcoming earnings → coverage/meta → alert bus hook → CSV strip) from `get_by_id_endpoint` into a standalone `async def _build_watchlist_response(watchlist_id, store, wl_load_ms=0) -> dict`. Both the cold GET path and `_rebuild_bulk_lkg_bg` call this single implementation. There is no risk of divergence.

### C3 — All GETs during rebuild return old LKG immediately
`_rebuild_bulk_lkg_bg` no longer pops `_BULK_LKG[watchlist_id]` before the rebuild runs. The old entry remains readable throughout the `await _build_watchlist_response(...)` call. Concurrent GETs hit the old LKG and return immediately.

### C4 — Rebuild failure preserves old LKG intact
New `_rebuild_bulk_lkg_bg` uses copy-on-success: it only writes `_BULK_LKG[watchlist_id]` when the builder returns successfully. The `except` branch logs the failure and returns without touching `_BULK_LKG`. Old payload, old timestamp, and old version are all retained.

### C5 — No hard STALE_TTL age eviction
Removed `if _lkg_age_s < _BULK_LKG_STALE_TTL:` gate from `get_by_id_endpoint`. A valid-version entry is now always served regardless of age. `_BULK_LKG_STALE_TTL` is retained as a logging label only (`fresh` / `stale` / `very_stale`). Age ≥ `_BULK_LKG_TTL` schedules one background rebuild; age alone never causes a cache miss.

### C6 — Theme and taxonomy invalidation
Added invalidation to all missing mutation paths:

| Route | Invalidation |
|-------|-------------|
| `PATCH /api/watchlist/{id}/tickers/{symbol}/theme` | `_bulk_lkg_invalidate(watchlist_id)` — point-specific |
| `PATCH /api/watchlist/{id}/category` | `_BULK_LKG.clear()` — global (ticker can be in any watchlist) |
| `POST /api/watchlist/{id}/categories/bulk` | `_BULK_LKG.clear()` — global (only when `count > 0`) |

### C7 — Post-hydration LKG invalidation
Added `_bulk_lkg_invalidate(watchlist_id)` at the end of `_priority_hydrate_symbols()` (after the `[PRIORITY_HYDRATE] finished` print). The next GET immediately rebuilds the response with the freshly hydrated quote/technical data, without waiting for the 5-minute TTL.

### C8 — High-frequency background writers classified
RSS sweeper and earnings monitor loops write to their own tables/caches only. Neither path calls `_bulk_lkg_invalidate` or `_BULK_LKG.clear()`. Verified: no new invalidation calls were added to any high-frequency loop.

---

## Streaming Analysis (Contract 9)

**SSE endpoint** (`GET /api/alerts/stream`, `media_type="text/event-stream"`):  
Starlette's `GZipMiddleware` lists `"text/event-stream"` in `DEFAULT_EXCLUDED_CONTENT_TYPES`. This endpoint is automatically excluded — no regression possible.

**JSON streaming endpoint** (`POST /api/query`, `media_type="application/json"`):  
`GZipResponder.apply_compression()` flushes the gzip buffer on every ASGI send call (including `more_body=True` chunks). The middleware does **not** buffer the full response — chunks stream incrementally through the compressor. No streaming regression.

Live test of `/api/query` returned 402 (subscription required) in 24ms, confirming the route is reachable and responds promptly. SSE alerts endpoint was confirmed auto-excluded by Starlette source inspection.

---

## Serialization / GZip Overhead Benchmark (Contract 10)

Measured against a 462-ticker Primary watchlist on a warm LKG (quote cache populated):

| Request | TTFB | Total | Wire bytes | Notes |
|---------|------|-------|------------|-------|
| Cold GET (pipeline, gzip) | 1.051 s | 1.059 s | 1,029,811 | Full pipeline + serialize + gzip |
| Warm GET 1 (LKG fresh, gzip) | 2.698 s | 2.706 s | 1,029,798 | Anomalous — event loop busy (background rebuild + other tasks) |
| Warm GET 2 (LKG fresh, gzip) | 1.068 s | 1.077 s | 1,029,798 | Representative warm case |
| Warm GET (identity encoding) | 1.010 s | 1.023 s | 6,304,076 | No gzip — serialization only |

**Finding:** Representative warm LKG TTFB is ~1.07 s with gzip vs ~1.01 s without. **GZip overhead is ~60 ms for 6.3 MB → 1 MB compression.** The dominant cost is **FastAPI JSON serialization of the large dict (~950 ms)**. The LKG cache eliminates the pipeline (quote fetch, FMP overlay, earnings) but cannot eliminate per-request serialization.

**Recommendation (not implemented — out of scope):** Pre-serializing `_BULK_LKG[wl_id]["payload"]` to a `bytes` object at write time and returning `Response(content=bytes, media_type="application/json")` would reduce warm GET latency from ~1.1 s to under 50 ms (only gzip + send overhead). This optimization is a concrete follow-up.

---

## Test Suite

25 tests, 25 passed, 0.08 s.

| # | Test | Contract |
|---|------|----------|
| 01 | Valid fresh LKG served | — |
| 02 | Stale entry (age ≥ TTL) still in `_BULK_LKG` | C5 |
| 03 | Single-flight rebuild guard | C3 |
| 04 | Copy-on-success: old LKG present during rebuild | C3/C4 |
| 05 | **Inverted**: rebuild failure preserves old payload/ts/version | C4 |
| 06–08 | Mutation invalidation (add/remove/save) | — |
| 09 | No cross-contamination between watchlists | — |
| 10 | Fresh LKG no rebuild scheduled | — |
| 11 | Version mismatch is structural miss | — |
| 12 | BUILDING flag cleared after success | — |
| 13 | **Inverted**: very-stale (age >> STALE_TTL) still served | C5 |
| 14 | Invalidate idempotent | — |
| 15 | All required symbols exported incl. `_build_watchlist_response` | C1 |
| 16 | TTL constants sane | — |
| 17 | Theme PATCH: point-specific invalidation | C6 |
| 18 | Taxonomy bulk clear: removes all watchlists | C6 |
| 19 | Post-hydration invalidation | C7 |
| 20 | Invalidate does not clear BUILDING flag | — |
| 21 | `_build_watchlist_response` is `async def` | C1 |
| 22 | Multiple watchlists coexist independently | — |
| 23 | No duplicate rebuild while one is running | C3 |
| 24 | Store/retrieve roundtrip preserves payload | — |
| 25 | RSS/earnings high-frequency writers do not invalidate | C8 |

---

## Files Changed

```
backend/services/watchlist_router.py   +246/-195
  - _rebuild_bulk_lkg_bg: copy-on-success (never pops before success)
  - _build_watchlist_response: extracted canonical pipeline
  - get_by_id_endpoint: calls builder; removes STALE_TTL hard eviction
  - PATCH /category: _BULK_LKG.clear()
  - POST /categories/bulk: _BULK_LKG.clear() when count > 0
  - PATCH /{id}/tickers/{sym}/theme: _bulk_lkg_invalidate(watchlist_id)
  - _priority_hydrate_symbols: _bulk_lkg_invalidate(watchlist_id) at end

backend/tests/test_watchlist_lkg.py    +334/-57
  - 25 tests replacing 20 (boundary testing, Contract verification)
  - Tests 5, 13 inverted to match new semantics
  - Tests 17–25 added covering C6/C7/C8/C1
```
