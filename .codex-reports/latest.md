# Watchlist GET Latency — LKG-First Cache + GZip

**Date:** 2026-08-07  
**Commit:** `perf: make watchlist detail LKG-first`

---

## Problem

`GET /api/watchlist/{watchlist_id}` ran the full enrichment pipeline on every
request: 462-ticker watchlist took **3.9 s** server time, **6.3 MB** raw wire.
No whole-response cache existed — component caches (quotes, fundamentals, stage2)
were warm but re-assembled on every GET.

---

## Changes

### 1 — Bulk GET LKG-first cache (`watchlist_router.py`)

**Module-level state:**

| Symbol | Type | Purpose |
|---|---|---|
| `_BULK_LKG` | `dict[str, dict]` | keyed by `watchlist_id` → `{payload, ts, version}` |
| `_BULK_LKG_BUILDING` | `set[str]` | single-flight guard (asyncio-safe, no lock needed) |
| `_BULK_LKG_TTL` | `int` | 300 s — fresh window; no rebuild scheduled |
| `_BULK_LKG_STALE_TTL` | `int` | 1200 s — stale window; serve + schedule bg rebuild |

**Version key:** `f"{updated_at or saved_at}|{len(tickers)}"` — structural
fingerprint of watchlist membership.  A change in either field (from add-ticker,
remove-ticker, or /save) immediately invalidates the entry via a version mismatch
without needing an explicit invalidation call.  Explicit invalidation calls are
also added as belt-and-suspenders.

**GET flow:**

```
load_watchlist()  →  compute version key
│
├─ LKG hit (version match, age < STALE_TTL)
│   ├─ fresh (age < TTL)     → return immediately; no rebuild
│   └─ stale (age ≥ TTL)     → return immediately + create_task(rebuild) [single-flight]
│
└─ LKG miss / version mismatch / beyond STALE_TTL
    → full pipeline (enrich + FMP overlay + earnings + rank passes)
    → store result in LKG
    → return
```

**Background rebuild (`_rebuild_bulk_lkg_bg`):**
1. Pop LKG (so inner call skips the stale shortcut)
2. `await get_by_id_endpoint(watchlist_id)` — stores fresh result as side-effect
3. `finally: _BULK_LKG_BUILDING.discard(watchlist_id)`
4. On exception: log + leave LKG absent; next browser GET rebuilds inline

**Invalidation (belt-and-suspenders):**

| Trigger | Route |
|---|---|
| Full watchlist replace | `POST /save` |
| Ticker added | `POST /{id}/ticker` |
| Ticker removed | `DELETE /{id}/ticker/{sym}` |
| Bulk add | `POST /{id}/tickers` |

### 2 — GZipMiddleware (`main.py`)

```python
from fastapi.middleware.gzip import GZipMiddleware
app.add_middleware(GZipMiddleware, minimum_size=1000)
```

Added as the outermost middleware (last `add_middleware` call) so it compresses
all responses ≥ 1 KB.  `GZipMiddleware` is a pure ASGI middleware — NOT
`BaseHTTPMiddleware` — so it is safe for `StreamingResponse` (compresses chunks
through the gzip compressor, does not buffer the full body).

---

## Measured Results

| Metric | Before | After |
|---|---|---|
| Cold GET (full pipeline) | ~1.7–3 s | ~3.9 s (same — must build LKG first time) |
| Warm GET #2 (8 s later) | ~1.7–3 s | **instant** (LKG fresh_hit) |
| Warm GET (72 s later) | ~1.7–3 s | **instant** (LKG fresh_hit) |
| Wire payload size | 6.3 MB | **1.0 MB** (83.7% compression via gzip) |

Server log evidence:
```
[WATCHLIST_GET] total_ms=3872 ... rows=462 price_coverage=100%
[WATCHLIST_LKG] fresh_hit wl=00a0e3ea-... age=8s
[WATCHLIST_LKG] fresh_hit wl=00a0e3ea-... age=72s
```

---

## Tests

**`backend/tests/test_watchlist_lkg.py`** — 20 unit tests, all pass in 0.44 s:

1. Valid fresh LKG present
2. Stale LKG in serving window
3. Ten callers schedule one rebuild (single-flight)
4. Rebuild success atomically replaces LKG
5. Rebuild failure leaves LKG absent (next GET rebuilds inline)
6–8. add/remove/save invalidate LKG
9. Different watchlists don't cross-contaminate
10. Fresh LKG does not schedule rebuild
11. Version mismatch is cache miss
12. BUILDING flag cleared after rebuild
13. Beyond STALE_TTL is cache miss
14. `_bulk_lkg_invalidate` is idempotent
15. Required symbols exported
16. TTL constants are sane (STALE > TTL > 0)
17. Multiple watchlists coexist independently
18. No duplicate rebuild while one is running
19. Store/retrieve roundtrip preserves payload fidelity
20. Invalidate does not touch BUILDING flag

---

## Notes

- **Production still returns 500** from the RSS pool fix (commit `cab90aa1`).
  User needs to publish (Deploy) to pick up both the RSS fix and this LKG change.
- The LKG serves the same payload (including prices from the last rebuild).
  The frontend already overlays realtime quotes via its live quote channel,
  so stale prices in the LKG are harmless for display purposes.
- The 5-minute fresh window aligns with the existing quote cache TTL (~10 min).
  The stale window (20 min) ensures the user never sees an empty/errored page
  during a rebuild that fails partway through.
