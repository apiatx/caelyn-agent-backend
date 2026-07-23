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
Three table-create calls were left inline in the lifespan body because the previous note said "fast since Neon warmed by deferred thread." In practice the thread had microseconds to warm before line 649 ran — Neon was still cold.

Culprits:
- `_whale_create_tables()` — inline before `asyncio.create_task(_seed_whales())`
- `_fund_ensure_table()` — inline before `_fund_weekly_lock = asyncio.Lock()`
- `_rss_ensure_table()` — inline before `asyncio.create_task(_rss_sweeper_loop())`

Fix: moved all three into `_deferred_sync_startup()`. All background loops that consume these tables have ≥ 60s startup delays, so there is no race condition. Lifespan yield: 0.55s after fix.

## Regression detection
The startup timer log line is: `[STARTUP] lifespan yield reached in Xs — healthcheck now active`
A WARNING line fires if X > 5s. Check this after any new lifespan initialization is added.
