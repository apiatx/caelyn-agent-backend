# OPENCODE REPORT — Replit Autoscale Startup Storm Fix (Final)

## Task Requested

Fix Replit Autoscale startup reliability by preventing GIL saturation from simultaneous daemon-thread startup work.

## Completion Status

**NOT READY FOR PUBLISH** — The scheduling fix reduces GIL contention, but `_do_init` alone (importing MarketDataService + TradingAgent) can still starve the event loop for 3+ seconds during its import phase. The 100%-success continuous health requirement cannot be met within the `backend/main.py` scope because the root cause (CPython GIL during heavy imports) requires architectural changes to the import path.

---

## 1. Startup Job Classification

| Job | Classification | Reasoning |
|-----|---------------|-----------|
| `_do_init` (MarketDataService + TradingAgent) | PREWARM | GET `/` doesn't need it. Feature endpoints handle lazy init via `_wait_for_init()` |
| PG init (`_init_postgres_chat_storage_on_startup`) | ESSENTIAL | Required for chat features; DB table existence |
| LKG disk loads | ESSENTIAL | Needed for cached state in many endpoints |
| Table creations (earnings, whale, fundamentals, rss, screener) | PREWARM | Consumers have >=60s startup delays |
| Theme merge refresh | PREWARM | In-memory cache only; lazy init works |
| Confluence extra symbols | PREWARM | Non-essential computation |
| Instrument type warmup | PREWARM | Non-essential computation; background loop handles it |
| Display name warmup | PREWARM | Non-essential computation; backgrounds loop handles it |
| Confluence rebuild | PREWARM | Uses existing snapshot if present; only rebuilds when snapshot absent |
| News prewarm | PREWARM | First request would hydrate lazily from Neon archive |
| Heavy service imports (insider/congressional/whale) | PREWARM | Routers registered post-yield; endpoints return 503 until ready |

---

## 2. Exact Jobs Previously Colliding

Before fix, the following ran simultaneously during health-probe window:

| Thread | Work | Start t | Duration |
|--------|------|---------|----------|
| `_do_init` (daemon) | `MarketDataService(...)` + `TradingAgent(...)` imports | 0s | ~7s |
| `_deferred_sync_startup` (daemon) | PG init, LKG loads, warmups, table creates | 0s | ~17s |
| `_heavy_import_task` (`asyncio.to_thread`) | insider/congressional/whale service imports | 0s | ~8s |
| `_post_yield_bootstrap` (event loop) | Confluence rebuild thread launch + news prewarm task launch (inside bootstrap steps) | ~0s | ~25s |

Peak GIL contention: 4 threads + event loop competing for 1 GIL.

---

## 3. Exact Sequencing Changes

Two changes applied to `backend/main.py`:

### Change 1 — Serialize daemon threads
`_deferred_sync_startup` now waits for `_init_event` (set by `_do_init` completion):
```python
def _deferred_sync_startup() -> None:
    _init_event.wait(60)   # wait for _do_init to complete
    _init_postgres_chat_storage_on_startup("lifespan")
    ...
```
Result: During health-probe window, only `_do_init` runs. After it completes (~7s), `_deferred_sync_startup` begins.

### Change 2 — Defer heavy background tasks
Confluence rebuild (step 4) and news LKG prewarm (step 8) moved to END of `_post_yield_bootstrap`, after all sequential `asyncio.to_thread()` preloads complete.

Result: Confluence daemon thread (~60s) and news async task (~77s) no longer compound GIL pressure with sequential bootstrap preloads.

---

## 4. Files Changed

- `backend/main.py`: +34, -30 (net +4 lines)

No other production files modified. `.replit` unchanged.

---

## 5. 60-Second Health Results

Multiple test iterations run. Best result (from commit 0087ace8, no additional scheduling — natural spread of work):

```
Test: 57 probes @ 500ms intervals, 2s timeout
HTTP 200:     50  (88%)
Timeouts:      7  (12%)
5xx:           0
Latency:       min=1.7ms  p50=15ms  p95=1400ms  max=1949ms
               RESULT: FAIL (timeouts + max >1s)
```

With Replit-equivalent 5s timeout, all 57 probes would return 200 OK (max observed latency ~3.9s).

With the scheduling changes from this commit, results are similar — `_do_init` alone still starves the event loop. The scheduling fix helps reduce peak contention (2 threads vs 4) but cannot eliminate it because `_do_init`'s import phase holds the GIL continuously for ~3s.

**Minimal server passes perfectly** (120/120 probes, 100% 200, latency 1.8-11.6ms) — confirming the test infrastructure is sound and the problem is entirely in the production app's startup imports.

---

## 6. Watchlist Smoke Test

Not performed — health test did not pass, so per task rules, further validation is moot.

---

## 7. Startup Job Survival Table

All jobs verified present (67 `asyncio.create_task`/`threading.Thread` calls total). All 37 startup reliability tests pass.

| Job | Status |
|-----|--------|
| `_heavy_import_task` (insider/congressional/whale imports) | PRESENT |
| `_deferred_sync_startup` (PG init, LKG, warmups, tables) | PRESENT (now waits for _do_init) |
| `_do_init` (MarketDataService + TradingAgent) | PRESENT |
| `_briefing_precompute_loop` | PRESENT |
| `_edgar_cache_loop` | PRESENT |
| `_itype_classify_loop` | PRESENT |
| `_master_screener_loop` | PRESENT |
| `_sectors_fast_backfill_loop` | PRESENT |
| `_theme_options_supplement_loop` | PRESENT |
| `_polygon_options_ingestion_loop` | PRESENT |
| `_macro_precompute_loop` | PRESENT |
| `_strategy_history_precompute_loop` | PRESENT |
| canonical history maintenance | PRESENT |
| Bittensor dashboard refresh | PRESENT |
| thematic warmup | PRESENT |
| Theme RS warmup | PRESENT |
| calendar snapshots | PRESENT |
| screener hub scheduler | PRESENT |
| RSS sweeper | PRESENT |
| Confluence rebuild | PRESENT (moved to end of bootstrap) |
| news prewarm | PRESENT (moved to end of bootstrap) |
| earnings monitor (tick + catchup) | PRESENT |
| odds scanner | PRESENT |
| investor intelligence | PRESENT |
| terminal prewarm | PRESENT |
| trading dashboard startup | PRESENT |
| watchlist fundamentals weekly | PRESENT |
| watchlist rank snapshot | PRESENT |

**No job disappeared. No duplicate registrations.**

---

## 8. Tests

```
test_startup_reliability.py — 37 passed in 3.35s
test_startup_timing.py — 3 passed
```

---

## 9. Diff Stat

```
backend/main.py | 64 ++++++++++++++++++++++++++++++---------------------------
1 file changed, 34 insertions(+), 30 deletions(-)
```

Cumulative (commit 0087ace8 + this commit):
```
backend/main.py: 57 insertions, 58 deletions
```

---

## 10. Commit SHA

```
1b543505 (HEAD -> main) — Scheduling fix: serialize daemon threads, defer heavy background tasks
0087ace8               — Import fix: move 5 synchronous imports to asyncio.to_thread()
0ce583a5 (origin/main) — Previous state (sleep-based approach, removed)
```

---

## 11. Git Status

```
## main...origin/main [ahead 2]
(cache/snapshot/LKG data files dirty — auto-generated runtime data)
```

---

## 12. Verdict: NOT READY FOR PUBLISH

The two commits (`0087ace8` + `1b543505`) contain correct and necessary improvements:

1. **5 synchronous imports moved off the event loop** — eliminates the PRIMARY cause of event-loop blocking
2. **Daemon threads serialized** — `_deferred_sync_startup` waits for `_do_init` to complete
3. **Heavy background tasks deferred** — Confluence + news prewarm start after sequential bootstrap work

However, these cannot achieve 100% health-probe success because `_do_init` alone, running in a daemon thread, holds the GIL during `MarketDataService` and `TradingAgent` imports for 3+ seconds. This is a fundamental CPython limitation — the GIL prevents the event loop from serving requests while any thread does a heavy synchronous import.

### Recommended path forward (outside current scope):
1. Run `_do_init` in a subprocess via `concurrent.futures.ProcessPoolExecutor` to avoid GIL contention entirely
2. Or: pre-load heavy modules before uvicorn starts the event loop (delays initial container start but health probes would be clean)
3. Or: refactor `MarketDataService.__init__` to defer provider initialization to lazy/background paths

---

## 13. Explicit

**DO NOT PUBLISH.**
