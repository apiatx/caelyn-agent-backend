---
name: Tradier budget lane tagging
description: Lane assignments for every Tradier call type; cache/coalescing accounting; deferred-call safety.
---

## Final lane map

| Call type | Lane | Rationale |
|-----------|------|-----------|
| `get_quotes` for live screener/watchlist/portfolio | `quotes` | live price/vol/bid/ask data |
| `get_history` (caelyn_terminal, whale_watch) | `maintenance` | startup-only burst, not live data |
| `get_option_expirations` + `get_option_chain` (screener OI) | `maintenance` | background enrichment |
| `get_option_expirations` + `get_option_chain` (portfolio scan) | `saved_options` | watchlist/portfolio options |
| `TradierFlowEngine.run_live_scan` | `options_flow` | master options screener |
| supplement loop `get_quotes` | `maintenance` | background stale-LKG refresh |
| popup / user-triggered routes | `reserved` | default ContextVar, do NOT tag |

## Key rule: get_history is never quotes
`get_history(sym)` = one HTTP call per symbol, no batching. 107 tickers at startup
= 107 budget-counted calls. Tag as `maintenance` so history bursts don't starve
live quote refresh during active sessions.

## Cache/coalescing accounting (no double-counting)
- `get_quotes`: cache-first; only missing tickers reach `_get()` → zero extra budget
- `get_option_expirations`: cache + `_EXPIRY_FUTURES` coalescing via `asyncio.shield()`;
  waiters short-circuit before `_get()` → zero budget for coalesced waiters
- `get_option_chain`: cache + `_CHAIN_FUTURES` coalescing via `asyncio.shield()`;
  same pattern → only the first physical HTTP call consumes budget

## Deferred call safety
All three callers handle `None` from a deferred `_get()` gracefully:
- `get_history`: `if not data: return []`
- `get_option_expirations`: `if not data: return []` (then callers check falsy)
- `get_option_chain`: `if not data: return empty_dict`

## Active-session enforcement (FORCE_ENFORCE validated)
With `TRADIER_BUDGET_FORCE_ENFORCE=1`:
- `maintenance: 10/10 SATURATED, deferred=541` — 107-call history burst deferred ✅
- `quotes: 3/30` — live quote lane fully protected ✅
- `reserved: 0/5` — all startup warmers tagged ✅
- `last_429_at: None` — no 429s under enforcement ✅
FORCE_ENFORCE env var reverted; use `TRADIER_BUDGET_FORCE_ENFORCE=1` to re-enable.

## ContextVar propagation
Wrap ENTIRE list comprehension + `asyncio.gather()` inside `with lane("X"):`.
Tasks created by `ensure_future()` (inside gather) copy parent context at
creation time. If created inside the `with` block, they inherit the lane. ✅

## Off-hours vs active-session behavior
Off-hours: `budget_enforcement_active=False`, warmers run free (expected).
`maintenance: 108/10 OVER_BUDGET` with `deferred=0` is correct off-hours output —
informational only. During active sessions, 98 of 108 maintenance calls defer.
