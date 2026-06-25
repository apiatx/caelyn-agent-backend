---
name: Tradier budget lane tagging
description: Which Tradier call types are per-HTTP-request vs batched, and which files needed non-obvious lane tags during Phase 3.
---

## The rule
Every `tradier_provider._get()` call is one budget-counted HTTP request.
`get_quotes([N tickers])` batches in groups of 50 → few calls.
`get_history(sym)` is **always one call per symbol** — no batching.
`get_option_expirations(sym)` and `get_option_chain(sym, exp)` are also one-per-call.

## Non-obvious tagged sites (Phase 3 completion)

| File | Function | Lane | Why |
|------|----------|------|-----|
| `data/caelyn_terminal.py` | `_fetch_tradier_histories()` | `quotes` | Fetches history for 100+ tickers (holdings + closed trades + option underlyings) — 107 individual HTTP calls at startup |
| `services/whale_watch_service.py` | Tradier history gather | `quotes` | Per-whale-holding history fetch |
| `data/portfolio_options_service.py` | `_scan_one_live()` — expirations | `saved_options` | Portfolio/watchlist option scan |
| `data/portfolio_options_service.py` | `_scan_one_live()` — chain gather | `saved_options` | Wrap entire list comprehension + gather inside `with lane(...)` |
| `services/screener_hub_service.py` | `_fetch_oi_from_tradier()` — expirations | `quotes` | OI enrichment for screener display |
| `services/screener_hub_service.py` | `_fetch_oi_from_tradier()` — chain loop | `quotes` | Same function, per-expiration chain fetch |

## Diagnostic technique
Added `asyncio.current_task().get_name()` log in `tradier_provider._get()` when
`lane == "reserved"` → revealed `/markets/history` as the 108-call culprit.
Remove the probe after identification.

## ContextVar propagation with asyncio.gather
`asyncio.gather(*[coro1, coro2, ...])` creates tasks via `ensure_future()` which
copies the current context. So wrapping the ENTIRE list comprehension + gather call
inside `with lane("X"):` correctly propagates the lane to all child tasks.
This is safe in Python 3.11+.

**Why:** ContextVar is task-scoped. Tasks created by `ensure_future`/`create_task`
copy the parent context at creation time. If the coroutine list is built and gather
called inside the `with lane(...)` block, all child tasks inherit the lane.

## Final result
- `reserved`: 1 (single incidental user-triggered call — correct)
- `quotes`: 109 (all terminal history + quote warmers)
- `total`: 110 — matches global TRADIER_LIMITER cap exactly
