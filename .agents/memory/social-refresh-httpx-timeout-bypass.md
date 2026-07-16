---
name: Social refresh httpx timeout bypass
description: grok-4.3 x_search streams periodic chunks that bypass httpx per-chunk timeout; asyncio.wait_for() required for hard wall-clock enforcement
---

## Rule
`httpx.AsyncClient(timeout=float)` sets a **per-read-chunk** timeout, NOT a total wall-clock timeout.  grok-4.3 with `x_search` enabled sends periodic heartbeat/reasoning chunks during long calls, so the httpx timeout never fires.  The Phase-1 `asyncio.gather` blocked for 26+ minutes until the server crashed.

## Fix
Wrap both Phase-1 gather and Phase-2 call in `asyncio.wait_for()`:

```python
# Phase-1 (services/x_consensus_cache.py ~line 1079)
batch_texts = await asyncio.wait_for(
    asyncio.gather(*[_guarded_batch(b, i+1) for i, b in enumerate(batches)]),
    timeout=_PHASE1_TIMEOUT + 15.0,  # 135s hard wall-clock
)

# Phase-2 (~line 1218)
result = await asyncio.wait_for(
    data_service.xai._call_grok_with_x_search(..., timeout=120.0),
    timeout=135.0,  # hard wall-clock; 120s httpx is per-chunk
)
```

**Why:** `asyncio.wait_for()` cancels the awaitable via `asyncio.CancelledError` after the wall-clock limit, regardless of httpx's chunk-level timing.

**How to apply:** Any long-running httpx call to a streaming LLM API (Grok, Gemini, Claude with streaming) that must respect a hard wall-clock deadline needs `asyncio.wait_for()`, not just `httpx.AsyncClient(timeout=N)`.

## Diagnosis signal
- LLM "Calling" log appears but no usage/completion log follows
- No timeout log either (confirming httpx timeout never fired)
- Server eventually crashes (OOM or health-check kill) while stuck in the gather
- On restart, startup catch-up is skipped (cache still within TTL from last good run)
- Social page shows stale/empty sections
