---
name: Post-yield bootstrap architecture
description: How the 6 deferred startup disk-reads are structured after yield, and what tracking infrastructure exists for them.
---

## Rule
All synchronous disk reads that do NOT need to complete before the first HTTP request must live inside `_post_yield_bootstrap()` — an async coroutine defined inside `lifespan()` and scheduled with `asyncio.create_task()` immediately before `yield`.

The 6 steps moved out of the pre-yield body:
1. `d2x` — Defiance 2X catalog load_lkg + daily refresh loop start
2. `canon_preload` — canonical 5-year history index preload
3. `stage2_lkg` — watchlist Stage 2 disk LKG load + warmup task
4. `confluence_warm` — retained Confluence snapshot background rebuild
5. `opt_snapshot` — options screener snapshot load_state
6. `earn_snaps` — earnings curated snapshots disk load + precompute loop

## Status tracking
`_BOOTSTRAP_STATE` is a module-level dict (near `_init_done`):
```python
{"done": False, "started_at": None, "elapsed_ms": None, "steps": {}}
```
Each step writes `{"ok": True/False, "ms": N, "error": str}` into `steps[key]`.

`GET /api/admin/startup-status` — returns the full dict + `init_complete` + `uptime_s`.

## What stays in pre-yield
- 3 lazy service imports (insider, congressional, whale)
- Router registration
- `_deferred_sync_startup()` thread start (Neon table-creates)
- Non-blocking `asyncio.create_task()` calls for loops
- `asyncio.create_task(_dynamic_thematic_universe_loop())`
- `asyncio.create_task(_theme_rs_warmup())`
- `asyncio.create_task(_earnings_calendar_warmup())`
- `asyncio.create_task(_post_yield_bootstrap())` — the bootstrap itself

## Measured results
- Pre-yield elapsed: **0.23s** (down from 0.64s, target <3s)
- Bootstrap total:   **2.70s** post-yield (earn_snaps dominates at 2.09s)

**Why:** FastAPI/uvicorn deployment healthchecks fire immediately after lifespan yields. Any blocking work before yield can cause deployment to fail with a timeout. The 6 sync disk reads collectively took ~0.5-1s in dev but can spike on cold deployment with cold disk.

**How to apply:** Any future synchronous startup work (disk reads, optional imports) should go into `_post_yield_bootstrap()` as a new numbered step, NOT directly in the lifespan body before yield. Non-blocking `asyncio.create_task()` calls are still fine before yield.
