---
name: Autoscale startup timeout fix
description: Root causes and fixes for production deployment health-check timeouts on autoscale
---

## Rule
The autoscale health-check window is **60 seconds from container start**. Any code path that runs synchronously before the lifespan `yield` must complete in < 52s (after the ~8s module import phase).

## Root Causes Found (in chronological order)

### 1. Pre-yield blocking imports (fixed)
`insider_activity_service`, `congressional_trading_service`, `whale_watch_service` were awaited before yield. Fix: `asyncio.create_task(asyncio.to_thread(_import_heavy_services))` — fires without awaiting.

### 2. Post-yield blocking sync calls (fixed)
`_post_yield_bootstrap()` ran six synchronous disk reads directly on event loop. Fix: wrapped each in `asyncio.to_thread()`.

### 3. `.pythonlibs` compileall targeting wrong directory (fixed Aug 4 2026)
Build step ran `python3.11 -m compileall -q .pythonlibs` from inside `backend/`, targeting nonexistent `backend/.pythonlibs/`. Real directory is `/home/runner/workspace/.pythonlibs`. So ALL `.pythonlibs` packages were compiled from source at lifespan time (~50s for the 6-8 lazy inline imports in the lifespan body). Fix: absolute path `/home/runner/workspace/.pythonlibs` in build step.

**Why:** The lifespan body contains synchronous `from services.X import Y` calls (canonical_history_backfill, bittensor, thematic_context_provider, theme_rs_service, etc.) that each trigger deep `.pythonlibs` sub-import chains. Without `.pyc`, each chain is slow; together they sum to ~50s. This plus 8s module imports = 58s → exceeds 60s window by 2s.

**How to apply:** Never use a relative path for `.pythonlibs` in the build step. Always use the absolute path `/home/runner/workspace/.pythonlibs`. Verify with `ls /home/runner/workspace/.pythonlibs` (has `bin/`, `lib/`, etc.).

## Current State (post-fix)
- Build step compiles `.pythonlibs` to `.pyc` during build (~21s one-time cost)
- Lifespan yields in < 1s in production
- First `[STARTUP]` print appears immediately at lifespan entry (diagnostic added)
- 60s window: 8s module imports + <1s lifespan yield = well within budget
