---
name: Lifespan import event-loop block
description: Heavy service imports inside async lifespan block the health-check window — must fire as background task, not awaited before yield
---

## The Rule
Never `await asyncio.to_thread(heavy_import)` before the `yield` in an `async def lifespan()`. Even though `asyncio.to_thread` runs the import in a thread (keeping the event loop technically free), Starlette does NOT serve HTTP requests until the lifespan yields. So the health-check probe at `GET /` gets no response for the entire await duration.

**Correct pattern**: fire the import as `asyncio.create_task(asyncio.to_thread(...))` before yield, then `await` the task inside `_post_yield_bootstrap()` and register routers there.

## Why
- `await asyncio.to_thread(f)` before yield: event loop is free but Starlette holds incoming connections until yield. Health probe times out.
- `asyncio.create_task(asyncio.to_thread(f))` before yield: same import runs in background; lifespan yields immediately; GET / responds 200 in <1s; routers available a few seconds later (acceptable).
- In dev, warm `.pyc` files made the import take ~0.68s — health check passed by luck. In production cold start, the same import takes ~6s — health check deadline exceeded every time.

## Symptom Pattern
Deploy logs (deployment_*.log):
- "status 500" for first 30-40s (proxy returning 500 before uvicorn binds)
- "context deadline exceeded" immediately after uvicorn starts
- No `[UNHANDLED_500]` in app logs (FastAPI never got the request)
- Build status: `failed` at promote step

## How to Apply

```python
# WRONG — awaiting before yield blocks Starlette from serving GET /
await asyncio.to_thread(_import_heavy_services)
app.include_router(...)
yield

# CORRECT — fire without awaiting; yield immediately; register routers post-yield
_heavy_import_task = asyncio.create_task(asyncio.to_thread(_import_heavy_services))
# ... other fast setup ...
asyncio.create_task(_post_yield_bootstrap())
yield

async def _post_yield_bootstrap():
    _ir, _bg, ... = await _heavy_import_task   # awaits here, after yield
    app.include_router(_ir, prefix="/api")
    asyncio.create_task(_bg())
    # ... rest of bootstrap ...
```

**Result**: lifespan yields in 0.01s. Bootstrap (including import) completes in ~2.74s. All routes available within ~3s of startup.

**Why:** Any service with background loops must have those loops started in the same post-yield block where the router is registered — the loop functions are local to the import and unavailable outside that scope.
