---
name: Lifespan import event-loop block
description: Heavy service imports inside async lifespan block asyncio event loop, causing autoscale health-check failures
---

## The Rule
Any synchronous `import` statement inside an `async def lifespan()` function blocks the asyncio event loop for its full duration. During that time, uvicorn cannot serve any HTTP requests — including the health-check probe at `GET /`.

## Why
Python module imports are synchronous and CPU/IO-bound. They acquire the import lock on the calling thread (the event loop thread). While the import runs, no other coroutines can be scheduled. The autoscale proxy at port 1104 sees no HTTP response → returns 500 → opens its circuit breaker → continues returning 500 even after the import finishes, exhausting the health-check budget.

`insider_activity_service` (pulls in `edgartools`) takes ~8.5s to import cold. Combined with other imports, the lifespan blocked for ~9s before yielding. The health-check window is ~40-77s total, but the proxy circuit-breaker cooldown consumed most of that budget.

## How to Apply
Any service imported in the lifespan for route registration must be imported via `asyncio.to_thread()`:

```python
def _import_heavy_services():
    from services.insider_activity_service import router as _ir, ...
    from services.congressional_trading_service import router as _cr, ...
    from services.whale_watch_service import router as _wr, ...
    return _ir, ..., _cr, ..., _wr, ...

(
    _insider_router, _insider_bg_loop,
    _cong_router,   _cong_bg_loop,
    _whale_router,  _whale_bg_loop,
    _whale_create_tables, _seed_whales,
) = await asyncio.to_thread(_import_heavy_services)
```

Result: lifespan yields in 0.68s instead of ~9s. GET / responds within ~1s of uvicorn binding port 5000.

**Symptom pattern**: deploy logs show "status 500" for entire promote window, no `[UNHANDLED_500]` log lines (the 500 comes from the proxy, not FastAPI), health-check never passes.
