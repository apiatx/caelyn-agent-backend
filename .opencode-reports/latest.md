# Emergency Backend Performance / Availability Incident

Date: 2026-09-01
Runtime: existing Replit Uvicorn workflow on port 5000

## 1. Exact reproduction

Probed the already-running process directly at `127.0.0.1:5000` while startup
and background work were active. No duplicate app was started.

## 2. Before timings

| Endpoint | Result | Elapsed | TTFB | Bytes |
|---|---:|---:|---:|---:|
| `/` | 200 | 19.720s | 19.719s | 58 |
| `/health` | timeout | >20s | none | 0 |
| `/ping` | timeout | >20s | none | 0 |
| `/api/home/dashboard` | timeout | >20s | none | 0 |
| `/api/home/risk-intelligence` | timeout | >20s | none | 0 |
| `/api/home/daily-alpha-board` | timeout | >20s | none | 0 |
| `/api/themes/rotation` | timeout | >20s | none | 0 |
| `/api/polymarket/events` | timeout | >20s | none | 0 |
| `/api/news/feed` | 200 | 4.460s | 4.459s | 144 |
| `/api/watchlist/list` | 200 | 12.717s | 12.712s | 177 |

The Replit development proxy also timed out after 15s on `/openapi.json`.

## 3. Proven root cause

Theme RS startup detached all six timeframe rebuilds concurrently. Five
historical rebuilds share the same ~610 proxy/benchmark symbols and stock-history
dependencies. Cold startup therefore duplicated thousands of FMP-guard,
fallback, and yfinance operations. The resulting main-thread CPU/GIL and callback
pressure starved Uvicorn's single event loop.

Evidence: trivial non-DB handlers timed out; the Python main thread was
continuously runnable at roughly 41-55% CPU; 63 threads and many yfinance cache
handles were active; logs showed `theme_rs_warmup_loop` and all timeframe
refreshes in progress together.

## 4. Why unrelated Home modules failed simultaneously

They share one Uvicorn event loop. Their independent handlers could not be
scheduled before client/frontend timeouts; this was not six independent data
source failures.

## 5. PostgreSQL pool involvement

Not the first bottleneck. No pool exhaustion log occurred. Once the event loop
could serve diagnostics, the pool reported:

```json
{"held_connections":0,"exhaustion_events":0,"callers":[],"pool_active":true,"pool_available":true,"last_conn_error":null}
```

## 20. Watchlist LKG-first correction validation

The authorized Watchlist correction was implemented and pushed as a single
clean commit containing only:

- `backend/services/watchlist_router.py`
- `backend/tests/test_watchlist_lkg.py`

Focused regression coverage passed: 32 tests.

The fresh-process warm-up rebuilt the Primary Watchlist from its canonical
source and returned 489 rows: 6,906,746 bytes uncompressed and 1,141,981
bytes with gzip. Five consecutive warm requests were then run while the
background workload remained active. Canonical `load_watchlist()` reads
increased by zero across those five requests. Four requests returned 200 with
TTFB/total times of 4.668/4.670s, 2.570/2.572s, 7.533/7.536s, and
1.164/1.165s; one request timed out at 120s before receiving bytes.

This separates the fixed Watchlist regression from the remaining shared
contention: `/ping` during the matrix averaged 813ms, reached 3.003s, and had
18 failed probes. The PostgreSQL diagnostic reported no exhaustion during the
successful matrix checks; a later snapshot showed 20 cumulative exhaustion
events while other background work was running. No background overlap fix was
made in this change.

The read-only overlap audit found no repository evidence of a recent commit
that newly synchronized the jobs. The proven condition is uncoordinated
post-yield startup scheduling: RSS and Whale both start at 30s, earnings
catch-up starts at 25s while its tick starts at 30s, and Theme RS/Confluence
run independently across shared event-loop, executor, PostgreSQL, and provider
capacity.

## 6. Event-loop starvation involvement

Yes. Before the fix, `/health` and `/ping` could not return within 20s despite
performing no DB/provider work. After warmup, `/health` returned in 8ms and
`/ping` in 7ms.

## 7. Startup/background overlap involvement

Yes. The six-way Theme RS startup fanout overlapped the other existing startup
systems and amplified shared CPU/thread pressure.

## 8. Existing path preserved

The existing `_locked_refresh()`, locks, cadence guards, cache/LKG writes,
providers, and endpoint contracts are unchanged. Only the initial scheduling
order changed.

## 9. Files changed

- Production: `backend/services/theme_rs_service.py`
- Test: `backend/tests/test_theme_rs_startup_scheduling.py`

No frontend, endpoint, DB, provider, cache, store, or fallback was added.

## 10. Precise fix

The existing background `_warmup_loop()` now awaits each timeframe refresh
sequentially instead of detaching six concurrent full rebuilds.

## 11. Tests

Focused scheduling regression plus existing PG pool exhaustion suite:

```text
24 passed in 0.10s
```

`git diff --check` passes.

## 12. After timings

| Endpoint | Result | Elapsed | TTFB | Bytes |
|---|---:|---:|---:|---:|
| `/` | 200 | 0.542s | 0.542s | 58 |
| `/health` | 200 | 0.008s | 0.008s | 162 |
| `/ping` | 200 | 0.007s | 0.007s | 181 |
| `/api/home/dashboard` | 200 | 0.010s | 0.010s | 164,499 |
| `/api/home/risk-intelligence` | 200 | 14.525s | 14.525s | 127,393 |
| `/api/home/daily-alpha-board` | 200 | 2.118s | 2.117s | 9,366 |
| `/api/themes/rotation` | 200 | 1.552s | 1.552s | 14,860 |
| `/api/polymarket/events` | 200 | 1.043s | 0.627s | 6,571,900 |
| `/api/news/feed` | 200 | 5.205s | 5.203s | 144 |
| `/api/watchlist/list` | 200 | 0.762s | 0.761s | 177 |

## 13. Primary Watchlist cold/warm timing

| Request | Result | Elapsed | TTFB | Bytes | Rows |
|---|---:|---:|---:|---:|---:|
| cold-ish | 200 | 9.675s | 9.608s | 6,910,950 | 489 |
| immediate repeat | 200 | 3.792s | 3.751s | 6,910,950 | 489 |

## 14. Home endpoint before/after

| Endpoint | Before | After |
|---|---:|---:|
| Home dashboard | timeout >20s | 200 in 0.010s |
| Risk intelligence | timeout >20s | 200 in 14.525s |
| Daily Alpha Board | timeout >20s | 200 in 2.118s |
| Theme rotation | timeout >20s | 200 in 1.552s |
| Prediction markets | timeout >20s | 200 in 1.043s |

## 15. PG pool before/after diagnostics

No exhaustion was logged before. The diagnostic route itself could not be
scheduled during the worst starvation. After: active and available, zero held
connections, zero exhaustion events, no last error.

## 16. Event-loop latency before/after

Before: trivial root 19.720s; health/ping >20s timeout.
After steady state: health 8ms; ping 7ms. A 40-probe sample averaged 245ms and
had one 2.002s timeout, so aggregate background pressure is improved but not
fully eliminated.

## 17. Provider-call effects

No provider calls were added to interactive paths. Existing FMP blocking and
Tradier rate lanes remain intact. The fix reduces duplicate startup fallback and
provider work.

## 18. Remaining risks

1. Risk intelligence remains endpoint-specifically slow at 14.5s.
2. Primary Watchlist detail remains 3.8s warm with a ~6.9MB body.
3. Deprecated `/api/news/feed` remains slow despite its tiny response.
4. One repeated ping probe exceeded 2s during aggregate background work.
5. Other expensive background jobs remain, but the retained Confluence rebuild
   observed in logs already executes in a worker thread.

The simultaneous whole-site outage is repaired, but these endpoint-specific
latencies still warrant separate, measured follow-up work.

## 19. Complete git diff

```diff
diff --git a/backend/services/theme_rs_service.py b/backend/services/theme_rs_service.py
@@
-    # Initial kick: compute all timeframes in background
+    # Initial kick: refresh timeframes sequentially inside this background task.
+    #
+    # Do not launch all six rebuilds at once. Historical timeframes share the
+    # same proxy and stock-history dependencies, so concurrent cold rebuilds
+    # duplicate work and can starve the event loop serving unrelated requests.
     for tf in list(_TIMEFRAME_BARS.keys()):
-        asyncio.create_task(_locked_refresh(tf))
+        await _locked_refresh(tf)

diff --git a/backend/tests/test_theme_rs_startup_scheduling.py b/backend/tests/test_theme_rs_startup_scheduling.py
new file mode 100644
--- /dev/null
+++ b/backend/tests/test_theme_rs_startup_scheduling.py
@@
+import ast
+from pathlib import Path
+
+THEME_RS_SERVICE = (
+    Path(__file__).resolve().parents[1] / "services" / "theme_rs_service.py"
+)
+
+def _warmup_loop_node() -> ast.AsyncFunctionDef:
+    tree = ast.parse(THEME_RS_SERVICE.read_text())
+    for node in tree.body:
+        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_warmup_loop":
+            return node
+    raise AssertionError("_warmup_loop not found")
+
+def test_initial_theme_refreshes_are_awaited_not_fanned_out():
+    warmup = _warmup_loop_node()
+    initial_loop = next(
+        node for node in warmup.body
+        if isinstance(node, ast.For)
+        and isinstance(node.iter, ast.Call)
+        and isinstance(node.iter.func, ast.Name)
+        and node.iter.func.id == "list"
+    )
+    assert any(isinstance(node, ast.Await) for node in ast.walk(initial_loop))
+    assert not any(
+        isinstance(node, ast.Call)
+        and isinstance(node.func, ast.Attribute)
+        and isinstance(node.func.value, ast.Name)
+        and node.func.value.id == "asyncio"
+        and node.func.attr == "create_task"
+        for node in ast.walk(initial_loop)
+    )
```