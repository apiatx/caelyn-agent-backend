---
name: Autoscale startup timeout fix
description: Two-part fix for gunicorn+uvicorn worker taking ~50s to start, causing autoscale health check to time out before GET / could respond.
---

## The rule
Never run synchronous Neon DB calls at module import time in main.py. Never run dense blocking I/O synchronously in the FastAPI lifespan event before the yield.

**Why:** The autoscale health check probes `GET /` from T=0. With gunicorn `-w 1 -k UvicornWorker`, the single worker cannot serve any request until the lifespan `yield` is reached. A 50s startup means the health check times out, the promote step fails, and the deployment rolls back even though the app eventually comes up.

**How to apply:** If you add new startup initialization in `main.py` lifespan:
- Synchronous DB calls, disk reads, or anything potentially slow → add to `_deferred_sync_startup()` (runs in background thread)
- `asyncio.create_task()` loops → keep in the lifespan body (non-blocking)
- Never add module-level Neon/DB calls outside of functions

## Root cause (from failed builds at 16:22 and 16:55 UTC 2026-07-13)

Two blockers combined to push startup to ~50s:

1. **Module-level Neon call** (`_init_postgres_chat_storage_on_startup("module_import")` at line 134): Ran at import time in every worker, blocking the import phase for 30–38 seconds while Neon established a cold connection.

2. **Dense synchronous lifespan block** (lines 411–522): Portfolio audit, manual anchor table init, theme merge refresh, watchlist loads, LKG disk reads, instrument-type warm-ups — all sequential Neon calls adding ~12s.

Total: ~50s. The autoscale health check started probing at T=0, timed out before `Application startup complete`.

## Fix applied

1. Removed the module-level call — imports now near-instant.
2. Extracted the dense sync block into `_deferred_sync_startup()` inside the lifespan, fired as `threading.Thread(target=_deferred_sync_startup, daemon=True, name="startup-sync").start()` at the top of the lifespan.
3. All `asyncio.create_task()` calls and function definitions remain in the lifespan body unchanged.
4. The three remaining table-create calls (`_whale_create_tables`, `_fund_ensure_table`, `_rss_ensure_table`) stayed inline but are fast since Neon is already warmed by the background thread's PG init.

Result: `Application startup complete` at log line 97 (vs ~50s before). `_deferred_sync_startup complete` logged at line 163, after the server is already serving.
