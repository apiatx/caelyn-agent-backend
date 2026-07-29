# Codex Report — Watchlist Earnings by-symbols architecture fix

## Task requested

Fix shared Watchlist earnings and ticker-detail loading architecture per spec:
- (A) Additive `wait_for_sync: bool = True` field on `EarningsBySymbolsRequest`
- (B) Stale-cache branch fix (returned empty instead of stale events)
- (C) In-flight guard on `_sync_for_explicit_symbols`
- (D) `recent: [...]` in by-symbols response
- (E) Ticker-detail audit across 5 representative symbols
- (F) Error semantics

## Completion status

COMPLETE — all 6 spec items addressed in exactly 2 production files.

## Proven root causes

### A — Blocking POST
`EarningsBySymbolsRequest` had no `wait_for_sync` field. The endpoint
hard-coded `sync_on_miss=True, background_sync_on_miss=False` for all callers,
blocking on FMP for every cache miss.

### B — Stale cache discards usable events
`get_upcoming_earnings_for_symbols` (user_earnings_service.py:771-786):
the `else` branch covering both cache-miss and stale-cache returned
`_empty_response("miss_syncing")` unconditionally, discarding
`cached["events"]` when a row existed but its TTL had expired.

### C — No in-flight guard
No `_SYNC_INFLIGHT` set, lock, or guard existed anywhere in
`user_earnings_service.py` (confirmed by grep). Concurrent polls/tab-switches
launched duplicate FMP jobs.

### D — No `recent` field
`get_recent_complete_events_for_symbols` was imported and used in the
watchlist-by-ID path (line 3553) but was never called from POST /by-symbols.

## Existing paths preserved

- All `sync_on_miss=True` callers unchanged (default `wait_for_sync=True`)
- `get_earnings_for_symbols` (older internal function) untouched
- Timing overlays, normalization, FMP budget, per-symbol isolation all intact
- Existing `GET /{watchlist_id}/earnings` route untouched
- Existing global Recent path untouched (additive addition to by-symbols only)
- Ticker-detail route and payload fully preserved

## Exact files changed

| File | Change |
|------|--------|
| `backend/services/user_earnings_service.py` | `_SYNC_INFLIGHT` set; `_sync_for_explicit_symbols` in-flight check + try/finally cleanup; stale-cache branch distinguishes cold miss vs stale row |
| `backend/services/watchlist_router.py` | `wait_for_sync` field on model; endpoint wires `body.wait_for_sync` / `not body.wait_for_sync` to call args; `recent` block using `get_recent_complete_events_for_symbols` |
| `backend/tests/test_by_symbols_earnings.py` | New — 8 focused scenarios, asyncio.run() pattern, no pytest-asyncio dependency |

## Behavior changed

| Scenario | Before | After |
|---|---|---|
| `wait_for_sync=false`, cold cache | Blocked on FMP | Returns `miss_syncing` in <1s |
| `wait_for_sync=false`, stale cache | Discarded events, returned empty | Returns stale events + `stale_syncing` |
| Concurrent polls | Spawned duplicate FMP jobs | Second call skips immediately |
| By-symbols response | No `recent` key | `recent: [...]` always present |
| Default callers (omit field) | `sync_on_miss=True` blocked | Unchanged — default=True preserved |

## Ticker-detail audit results

| Symbol | HTTP | Elapsed | Size | EI | Notes |
|--------|------|---------|------|----|-------|
| EOSE | 200 | 2 670 ms | 93 KB | ✓ | equity, standard path |
| AAPL | 200 | 2 122 ms | 515 KB | ✓ | rich EI, large snapshot |
| BAND | 200 | 3 086 ms | 137 KB | ✓ | confirmed Q2 2026 EI |
| TM | 200 | 6 482 ms | 29 KB | ✓ | foreign ADR, slowest |
| SPY | 200 | 2 723 ms | 31 KB | ✓ | ETF — EI gated correctly |

Zero provider calls confirmed by code inspection and consistent timing.
No shared code bottleneck proven across all 5 symbols. No ticker-detail code changed.

## Validation results

```
# Focused test suite (8 scenarios)
cd backend && python3.11 -m pytest tests/test_by_symbols_earnings.py -v
→ 8 passed in 0.16s

# Existing regression suite (56 scenarios)
cd backend && python3.11 -m pytest tests/test_earnings_revenue_validation.py -v
→ 56 passed in 0.37s

# git diff --check
→ (no output) exit=0

# wait_for_sync=false live timing
→ 943ms elapsed, cache_status=partial_syncing, recent_key=True, stale=False

# wait_for_sync=True (default) live
→ 2814ms elapsed, cache_status=hit, recent_key=True, sync_preserved=True
```

## Risks and remaining issues

- `_SYNC_INFLIGHT` is module-level (in-process). Multi-worker deployments would
  have one set per worker. Acceptable for current single-worker architecture.
- TM's 6.5s ticker-detail latency may warrant a future investigation but was
  not a proven shared bottleneck across the 5-symbol audit.

## AGENTS.md compliance

- Production files changed: 2 of ≤2 allowed
- No new endpoints, tables, workers, schedulers, or queues
- No runtime JSON/cache files staged
- git diff --check: clean
