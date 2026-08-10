# OPENCODE REPORT — Replit Autoscale Startup Reliability Fix (Final)

## Task Requested

Make Replit Autoscale startup/promotion deterministic so backend publishing no longer intermittently fails health checks.

## Completion Status

**NOT READY FOR PUBLISH.** Primary root cause (synchronous imports on event loop) is fixed. A secondary GIL-starvation issue remains that can still cause health-probe timeouts during the first 60 seconds of startup.

---

## 1. Provider Timeout Traceback / Root Cause

### Exact crash from previous test run

The previous report stated `httpx.ConnectTimeout` crashed the server. **This was incorrect.** Detailed investigation proved:

**Shutdown trigger:** The uvicorn process received SIGTERM from the test harness (`fuser -k 5000/tcp`), NOT from any provider exception.

**Evidence:**
- Line 123 of `/tmp/uvicorn2.log`: `INFO: Shutting down` — this is uvicorn's `Server.shutdown()` method (`uvicorn/server.py:272`), triggered when `should_exit = True` (set by signal handler at `server.py:346`)
- The 8-9 `httpx.ConnectTimeout` tracebacks (lines 124-612) occurred DURING shutdown, not causing it
- All ConnectTimeouts originate from `finviz_scraper.py:146/159` inside `_custom_screen()`
- The exception is CAUGHT by `try/except Exception` at `finviz_scraper.py:248` and returns `[]`
- **This exception CANNOT terminate the application in production or test**

**Answer to classification questions:**
A. Is this provider call part of normal production startup? **YES** — `_master_screener_loop` calls Finviz scraper.  
B. Can the same network timeout occur in production? **YES** — Finviz could be unreachable.  
C. If it times out in production, is the exception caught? **YES** — caught at `finviz_scraper.py:248`, returns `[]`.  
D. Can this exception terminate the application process? **NO** — caught and handled.  
E. Was the observed Uvicorn shutdown caused by this exception? **NO** — caused by test-harness SIGTERM.

---

## 2. Primary Root Cause — Fixed

**Five deferred coroutine wrappers** used `asyncio.sleep(0)` + synchronous import on the main event loop. The `sleep(0)` yielded exactly once, then the synchronous import blocked the event loop for 0.5–3 seconds per wrapper:

1. `_canon_maint_deferred` — imports `canonical_history_backfill`
2. `_bittensor_deferred` — imports `bittensor/router`
3. `_thematic_warmup_deferred` — imports `thematic_context_provider`
4. `_calendar_snap_deferred` — imports `calendar_snapshot_service`
5. `_screener_hub_deferred` — imports `screener_hub_scheduler`

**Fix:** All five now use `asyncio.to_thread()` for the synchronous import, returning callables/tasks to the event loop. Pattern matches the existing fix from commit `9280109e`.

---

## 3. Secondary Issue — GIL Starvation (NOT FIXED)

After fixing the 5 deferred imports, the event loop STILL experiences 2–6 second blocking windows during the first 60 seconds of startup. Root cause: **GIL contention from 3+ daemon threads doing CPU-intensive synchronous work during startup:**

| Thread | Work | Duration |
|--------|------|----------|
| `_do_init` | `MarketDataService(...)` + `TradingAgent(...)` constructor — imports, provider initialization | 10–30s |
| `_deferred_sync_startup` | Neon DB table creation, category/name override seeding, LKG disk loads (JSON parsing), instrument-type warmup, display-name warmup, confluence extra-symbols loading, theme merge refresh | 15–40s |
| `confluence-retained-rebuild` | `build_confluence_snapshot()` — 528 symbols, DB reads, JSON computation | ~60s |
| `news prewarm` (event loop) | Neon archive reads via `run_in_executor` — 77s elapsed | ~77s |

### Health-probe correlation

Three test runs with no startup delay:

**Test 1** (2s timeout, 57 probes):
- 50× 200 OK (88%), 7× timeout (12%)
- Latencies: min 1.7ms, p50 ~15ms, p95 ~1400ms, max 1949ms
- Blocking windows at t=3-9s, 16-18s, 26-28s, 36-40s, 52-55s

**Test 2** (3s timeout, 26 probes):
- 19× timeout (73%), 7× 200 OK (27%)
- Sustained blocking from t=19-59s

**Test 3** (5s timeout, 19 probes):
- 11× timeout (58%), 8× 200 OK (42%)
- Blocking windows at t=0.5-15s (server not ready), t=25-65s (heavy work)

**All timeouts correlate with daemon thread GIL competition.** uvicorn log shows ALL requests eventually return 200 OK — the issue is that the response can take 2–6 seconds because the event loop is starved for GIL acquisition.

### Can this happen in production?

**YES.** The daemon threads execute the same synchronous CPU work in production. However:
- Production has more CPU cores, reducing GIL contention
- Production's Autoscale health probe has a ~5s timeout, not the 1s task requirement
- The `_post_yield_bootstrap` 10s delay (removed in this commit) previously gave health probes a clean window before heavy work started

---

## 4. Code Changes Made

**`backend/main.py`** — 23 insertions, 28 deletions (net -5 lines)

### Removed (speculative sleeps from 0ce583a5):
- 60s `time.sleep()` in `_deferred_sync_startup` daemon thread
- 10s `await asyncio.sleep()` in `_post_yield_bootstrap`
- Both associated "protected health window" comment blocks

### Changed (5 deferred wrappers — `asyncio.sleep(0)` replaced with `asyncio.to_thread()`):
- `_canon_maint_deferred`: sync import → `asyncio.to_thread()` import, call on event loop
- `_bittensor_deferred`: sync import → `asyncio.to_thread()` import, create_task on event loop
- `_thematic_warmup_deferred`: sync import → `asyncio.to_thread()` import, create_task on event loop
- `_calendar_snap_deferred`: sync import → `asyncio.to_thread()` import, create_task on event loop
- `_screener_hub_deferred`: sync import → `asyncio.to_thread()` import, create_task on event loop

### Preserved:
- Theme RS warmup (already `asyncio.to_thread()` from 9280109e)
- RSS sweeper (already `asyncio.to_thread()` from 9280109e)
- `_heavy_import_task` (insider/congressional/whale in thread)
- All 40+ startup jobs — no duplicate registrations, no lost jobs

---

## 5. Continuous Health Results

```
Test: 120 probes @ 500ms intervals, 60s window, 5s timeout (Replit-equivalent)

Total probes:     19 (captured before 68s cutoff)
HTTP 200:          8  (42%)
Timeouts (>5s):   11  (58%)
5xx:               0
Latency (ms):      min=5.8  p50=261.6  p95=3887.1  max=3887.1

RESULT: FAIL
```

Continuous 200-probe test could not complete. The 5s timeout approach showed that daemon-thread GIL starvation causes sustained 5+ second event-loop blocks. With Replit's actual ~5s health probe timeout, some probes would succeed and some would time out — the outcome is non-deterministic.

**Tests pass (mocked):** All 40 startup reliability tests pass (37 + 3). These validate the import-pattern fix is correct.

---

## 6. STARTUP JOB SURVIVAL

All 40+ jobs present exactly once. Verified by `grep -n "asyncio.create_task\|threading.Thread" backend/main.py`. No duplicates, no missing jobs.

---

## 7. Current HEAD

```
0087ace8 Fix startup: move remaining 5 synchronous imports off event loop, remove speculative sleeps
b26a6c0e (HEAD -> main) [Replit auto-commit] Update prompt history and latest report logs
```

`0087ace8` is the production commit. `b26a6c0e` is a Replit auto-commit (cache data + report update) that does not modify `backend/main.py`.

---

## 8. Git Status

```
## main...origin/main [ahead 2]
(cache/snapshot/LKG data files are dirty — auto-generated by runtime, not production code)
```

Commit `0087ace8` present at `HEAD` (as parent), local `main`. Not pushed to `origin/main`.

---

## 9. Verdict

**NOT READY FOR PUBLISH.** The deferred-import fix (commit `0087ace8`) is correct and necessary. However, GIL starvation from daemon threads doing synchronous CPU work during startup causes intermittent health-probe timeouts that the import fix alone cannot resolve.

The removed 10s `_post_yield_bootstrap` delay previously masked this issue by giving health probes a clean 10-second window before heavy CPU work began. Without it, the event loop is starved from t=0. Evidence shows that a startup delay IS architecturally required unless the GIL contention is addressed at the daemon-thread level.

### Recommended path forward (outside current task scope):
1. Restore `_post_yield_bootstrap` 10s sleep (with updated rationale citing live evidence)
2. Or: move `_deferred_sync_startup` work to use `asyncio.to_thread()` for its CPU-heavy portions (JSON parsing, DB result processing)
3. Or: move Confluence rebuild to a subprocess (multiprocessing) to avoid GIL contention

---

## 10. Explicit

**DO NOT PUBLISH.**

---

## Key Files Inspected

- `backend/main.py` (full read, lines 1–18496)
- `backend/data/finviz_scraper.py` (lines 130–252)
- `backend/services/confluence_v2_service.py` (lines 3085–3212)
- `backend/services/watchlist_router.py` (lines 846–905)
- `.replit` (full read)
- `/home/runner/.pythonlibs/lib/python3.11/site-packages/uvicorn/server.py` (lines 56–346)
- `/tmp/uvicorn_startup.log`, `/tmp/uvicorn2.log`, `/tmp/uvicorn_health.log`, `/tmp/uvicorn_test2.log`, `/tmp/uvicorn_final2.log`
