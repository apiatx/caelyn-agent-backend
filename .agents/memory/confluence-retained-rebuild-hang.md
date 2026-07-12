---
name: Confluence retained-snapshot rebuild hang
description: Root cause and fix for the multi-minute hang in build_confluence_snapshot() / get_options_alignment_bulk().
---

## The root cause

`get_options_alignment_for_ticker()` called `_fetch_net_premium_row(sym)` → `get_historical_snapshots_bulk([(entity_type, sym)], since)` — ONE Neon round-trip per ticker. With 379 tickers that was **379 sequential Neon queries**, causing a multi-minute (∞ in practice) hang.

The fundamentals Neon call was a separate but smaller duplicate — 196ms, not the real problem.

## The fix (options_alignment.py)

1. Added `preloaded_net_premium: Optional[dict]` param to `get_options_alignment_for_ticker()`. When provided, the per-ticker Neon call is skipped entirely.

2. In `get_options_alignment_bulk()`: pre-fetch ALL tickers' net premium history in ONE `get_historical_snapshots_bulk(entities_list, since)` call before the per-ticker loop. Wrapped in a `ThreadPoolExecutor` with `shutdown(wait=False)` + 25s timeout so a wedged Neon socket cannot block the caller.

3. The pre-fetch produces a dict `{ticker: (current_row, history_list)}` passed to each `get_options_alignment_for_ticker` call.

## Timing after fix

| Step | Before | After |
|---|---|---|
| `net_premium_bulk` (single query) | — | 259ms, 300/379 tickers with data |
| `options_alignment_bulk` | hung > 3 min | 285ms |
| **Total rebuild (379 tickers)** | **∞** | **~10.6s** |

## shutdown(wait=False) pattern — critical

`ThreadPoolExecutor.__exit__` calls `shutdown(wait=True)`. If used as a context manager and the future times out, the `with` block's exit will **still block indefinitely** waiting for the hung Neon socket. Always use the explicit pool + `shutdown(wait=False)` pattern for Neon queries with timeouts:

```python
_pool = ThreadPoolExecutor(max_workers=1)
_fut = _pool.submit(fn, *args)
try:
    result = _fut.result(timeout=N)
except TimeoutError:
    result = fallback
finally:
    _pool.shutdown(wait=False)  # never blocks
```

This pattern is now used in both `_timed_neon()` (confluence_v2_service.py) and `get_options_alignment_bulk()` (options_alignment.py).

## snap_built_at cosmetic fix

V4 endpoint was reading `snap.get("built_at")` but the snapshot dict key is `generated_at`. Fixed to `snap.get("generated_at") or snap.get("built_at")`.
