---
name: Autoscale startup timeout fix
description: Deployment healthcheck times out when lifespan Neon DB calls block the yield. Rule: ALL sync Neon calls go in _deferred_sync_startup(), zero exceptions.
---

## The rule
**Zero synchronous Neon/DB calls in the lifespan body before `yield`.** Every table-create, probe, or audit call must go into `_deferred_sync_startup()`.

**Why:** The autoscale health check probes `GET /` from T=0. The worker cannot serve any request until the lifespan `yield` is reached. Each cold Neon call takes 5-8s. Three such calls = 15-24s startup, which exceeds the deployment healthcheck timeout, causing rollback even though the app eventually comes up. Measured: lifespan yield at 0.55s after fix (was 17s before).

**How to apply:** If you add new startup initialization in `main.py` lifespan:
- Synchronous Neon/DB calls, `ensure_table()`, audit functions → **must** go into `_deferred_sync_startup()` (runs in a background thread AFTER yield)
- `asyncio.create_task()` loop registrations → safe in lifespan body (non-blocking)
- Synchronous disk reads (LKG loads, JSON index preloads) → OK in lifespan body if the file is small; move to deferred if uncertain
- Never add module-level Neon/DB calls outside of functions
- A startup timer `_lifespan_t0 = time.monotonic()` is set before the thread start, and `[STARTUP] lifespan yield reached in Xs` is logged right before `yield` — if you see X > 3s, a new blocking call crept in

## Root cause history

### Round 1 — 2026-07-13 (50s startup)
1. Module-level Neon call at import time: 30-38s cold connection.
2. Dense synchronous lifespan block: ~12s of sequential Neon calls.
Fix: removed module-level call; extracted block into `_deferred_sync_startup()`.

### Round 2 — 2026-07-23 (~17s startup, 10 failed deployments in one day)
Three table-create calls left inline in the lifespan body. Fix: moved all three into `_deferred_sync_startup()`. Lifespan yield: 0.55s after fix. But deployment STILL timed out (~15s).

### Round 3 — 2026-07-23 (15s startup after Round 2)
Dev showed 3.89s yield — not the 0.55s we expected. Root causes:
1. **`_lifespan_t0` was set AFTER the three lazy imports** (lines 423-436) so the timer missed them entirely. The real yield time was higher than measured.
2. **`insider_activity_service.py` had `from edgar import ...; set_identity(...)` at MODULE LEVEL** — edgartools is a heavy library, taking ~3.7s to import cold.

Fix: 
- Moved `_lifespan_t0 = time.monotonic()` to the very first line of lifespan (before any imports).
- Made edgar import lazy via `_ensure_edgar()` in `insider_activity_service.py`; all three call sites updated.
- Added granular `[STARTUP_TIMING]` checkpoints A→S to pin remaining cost; used them to confirm `_load_earn_snaps()` is the biggest disk read (~0.47s), which is acceptable.

Final dev timing: **0.64s lifespan yield** (was 3.89s → 50s across all rounds).

## Regression detection
The startup timer log line is: `[STARTUP] lifespan yield reached in Xs — healthcheck now active`
A WARNING line fires if X > 5s. Check this after any new lifespan initialization is added.
