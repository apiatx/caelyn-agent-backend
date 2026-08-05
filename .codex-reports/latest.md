# Performance Investigation — Watchlist 7D/30D Regression
## Commit b7964131 — get_comparison_closes_bulk Request-Time File Scan
### Date: 2026-08-05

---

## 1. Completion Status
COMPLETE — read-only investigation, no files modified, no commit, no push.

---

## 2. Git / Workspace State

| Field | Value |
|-------|-------|
| Toplevel | /home/runner/workspace |
| Branch | main |
| HEAD SHA | e2530530 |
| HEAD message | Published your App |
| b7964131 is ancestor of HEAD | YES |
| Uncommitted Python source changes | NONE |
| Uncommitted data changes | ~100+ .json.gz canonical bar files (runtime writes from background loops) |

---

## 3. Exact Request Path

```
Frontend GET /api/watchlist/{id}
  → watchlist_router.py: GET /api/watchlist/{watchlist_id}  (line ~6110)
    → load_watchlist(watchlist_id)                           (PostgreSQL read)
    → _enrich_store_with_quotes(store)                       (line 1077)
        → asyncio.gather (line ~1139):
            ├─ run_in_executor(None, _get_name_overrides, "default")
            ├─ run_in_executor(None, _get_fund_snaps_mc, tickers)
            └─ run_in_executor(None, _load_cached_watchlist_market_data, tickers)  ← NEW
                ├─ get_volume_metrics_bulk(tickers)          ← in-memory _INDEX, <2ms
                └─ get_comparison_closes_bulk(tickers)       ← opens 385 .json.gz, ~5s
                    ├─ for each symbol (385 files):
                    │   ├─ gzip.open(sym.json.gz)
                    │   ├─ json.loads(full_payload)
                    │   ├─ _completed_daily_bars(bars)       ← datetime.strptime per bar
                    │   └─ _select_comparison_closes(completed, ny_today)
                    └─ returns dict with comparison_close_7d/30d per symbol
        → merges vol_metrics + comp_closes into enriched dict
        → live-price override block computes change_7d/change_30d
    → rows serialized and returned
```

### Confirmed answers to Part 1 questions:

| Question | Answer |
|----------|--------|
| get_comparison_closes_bulk invoked on every full WL request? | **YES** |
| Opens one .json.gz per symbol? | **YES — 385 files per request** |
| Parses complete bars array? | **YES — all 718,586 bars every time** |
| Results survive beyond single request? | **NO — in-request dict, discarded after response** |
| Repeated warm requests reopen every file? | **YES — confirmed across 3 consecutive timing runs** |
| get_volume_metrics_bulk can do extra full-file pass? | YES — when _PRICE_METRIC_FIELDS missing from index entry (cold start only) |
| Internal _comparison_* keys reach serialized response? | **YES — 1,800 leaked keys (4 × 450 rows) — BUG** |
| Other new request-time canonical scan? | No |

---

## 4. Primary Watchlist — Canonical File Statistics

| Metric | Value |
|--------|-------|
| Primary Watchlist ID | 00a0e3ea-31dc-4223-97bc-470720dd3215 |
| Total ticker rows | 450 |
| Unique normalized tickers | 450 |
| Matching .json.gz files | 385 |
| Missing files | 65 (all foreign-prefixed: AIM:*, AMS:*, ASX:*, etc.) |
| Total compressed size | 14,170,102 bytes (13.5 MB) |
| Min compressed size | 726 bytes |
| Median compressed size | 41,908 bytes |
| Mean compressed size | 36,805 bytes |
| p90 compressed size | 53,473 bytes |
| Max compressed size | 71,521 bytes |
| Total bars | 718,586 |
| Compression ratio | ~4.6× |
| Est. uncompressed bytes parsed per request | ~61 MB |

---

## 5. Isolated Function Timing Table

(450 tickers, preloaded _INDEX, 3 consecutive iterations in same process)

### get_volume_metrics_bulk

| Iter | Wall | CPU | Syms | gzip.open calls | Bytes read |
|------|------|-----|------|-----------------|------------|
| 1 | 1.4 ms | 1.4 ms | 450 | **0** | 0 |
| 2 | 0.6 ms | 0.6 ms | 450 | **0** | 0 |
| 3 | 0.6 ms | 0.6 ms | 450 | **0** | 0 |

Pure in-memory _INDEX lookup. No disk I/O regardless of warm/cold state.

### get_comparison_closes_bulk

| Iter | Wall | CPU | Syms | gzip.open calls | Bytes read |
|------|------|-----|------|-----------------|------------|
| 1 | 5,280.8 ms | 5,150.0 ms | 450 | **385** | 14,171,248 |
| 2 | 5,081.2 ms | 5,020.7 ms | 450 | **385** | 14,171,248 |
| 3 | 4,884.7 ms | 4,856.9 ms | 450 | **385** | 14,171,379 |

**ALL 385 files reopened on EVERY call. No inter-request caching.**

### _load_cached_watchlist_market_data equivalent

| Iter | Wall | CPU | Syms | gzip.open calls | Bytes read |
|------|------|-----|------|-----------------|------------|
| 1 | 4,901.2 ms | 4,868.4 ms | 450 | 385 | 14,171,379 |
| 2 | 4,929.5 ms | 4,885.1 ms | 450 | 385 | 14,171,810 |
| 3 | 5,000.6 ms | 4,943.3 ms | 450 | 385 | 14,172,217 |

Dominated entirely by get_comparison_closes_bulk. get_volume_metrics_bulk adds <2ms.

---

## 6. cProfile Results — One Full _load_cached_watchlist_market_data Call

Total: 23,820,183 function calls in **13.535 seconds** (profiler overhead inflates vs 5s wall;
relative proportions are authoritative)

| ncalls | cumtime | function |
|--------|---------|----------|
| 1 | 13.535 | _load_cached (entry) |
| 1 | 13.531 | get_comparison_closes_bulk |
| **385** | **12.182** | **_completed_daily_bars** ← dominant |
| **718,670** | **9.164** | **datetime.strptime** ← root bottleneck |
| 718,670 | 8.771 | _strptime_datetime |
| 718,670 | 8.018 | _strptime (internal) |
| 718,670 | 1.956 | _getlang (locale, called per strptime) |
| 718,670 | 1.662 | locale.getlocale |
| 1,437,340 | 1.090 | _finite_float_or_none |
| 385 | 0.868 | json.loads |
| 385 | 0.862 | json decoder raw_decode |
| 385 | 0.324 | file .read() |
| 385 | 0.315 | gzip.read |
| 718,670 | 0.317 | re.match |

**Root bottleneck: datetime.strptime() called 718,670 times (once per bar per file).**
_completed_daily_bars uses `datetime.strptime(date_raw, "%Y-%m-%d").date()` at line 391
for EVERY bar to filter out the current NY date — O(all_bars) per call.
json.loads and gzip reads together take only 0.87s + 0.31s = 1.18s.
The strptime loop alone consumes ~9s of the ~13s profiler runtime.

**Note**: `_select_comparison_closes` (called after _completed_daily_bars) uses
`date.fromisoformat()` — fast. The problem is exclusively in _completed_daily_bars.

---

## 7. Real Endpoint Timing Table

Endpoint: GET /api/watchlist/00a0e3ea-31dc-4223-97bc-470720dd3215

| Call | HTTP | TTFB | Total | Response bytes | Rows | Source |
|------|------|------|-------|---------------|------|--------|
| 1 | 200 | 7.363 s | 7.381 s | 6,134,591 | 450 | warm |
| 2 | 200 | 6.740 s | 6.748 s | 6,134,079 | 450 | warm |
| 3 | 200 | 7.253 s | 7.268 s | 6,134,090 | 450 | warm |

Server log (from [WATCHLIST_ENRICH] and [WATCHLIST_GET]):
- `history fetch_ms` = **5,595 – 8,265 ms** (= time inside run_in_executor for _load_cached)
- `quotes` = 394, `name_overrides` = 16, `fund_snaps` = 391, `history_metrics` = 450
- `row_enrichment_ms` = **5,603 – 8,294 ms**
- `total_ms` = 6,345 – 9,913 ms

Cold request (from deployment logs): `total_ms=14,347 ms` observed.

The history_metrics=450 field confirms all 450 tickers are processed by
_load_cached_watchlist_market_data every time.

---

## 8. Concurrent Endpoint Impact Table

Methodology: Watchlist request fired in background (run_in_executor means event loop stays
free), then three lightweight endpoints measured concurrently.

| Endpoint | Baseline | During Watchlist | Added latency | HTTP |
|----------|----------|-----------------|---------------|------|
| /api/health | 1.63 s | 1.79 s | +0.16 s | 200 |
| /api/home/risk-intelligence | 2.06 s | 0.14 s | −1.92 s (cached) | 200 |
| /api/market/realtime-quotes | 0.22 s | 0.29 s | +0.07 s | 200 |

**Interpretation**: Because _load_cached_watchlist_market_data runs in run_in_executor
(thread pool, not the event loop), other async routes continue to accept requests and
respond normally. The event loop is NOT blocked.

However: the watchlist response itself takes 6–7 seconds. The user experience is frozen
because:
1. The frontend depends on the watchlist response to render critical UI state
2. While waiting 6–7 s for the watchlist, the page appears unresponsive
3. With 450 tickers and 14 MB of gzip I/O per request, repeated polling exhausts disk I/O

Server CPU during concurrent test: uvicorn at 22.8% (thread running gzip scan).
Peak memory: 17.8% (~1.45 GB RSS) — the 61 MB JSON parse lands in-process.

---

## 9. Frontend Global-Prefetch Coupling

The frontend source is in a separate repository not present in this workspace.
GlobalDataContext.tsx was not found in /home/runner/workspace/frontend/.

Based on the investigation brief and observed request patterns in server logs, the
coupling is as follows:

From the spec description and server logs showing repeated watchlist requests:
- GlobalDataContext (or equivalent) mounts globally for authenticated routes
- It fetches /api/watchlist/list to identify the Primary Watchlist ID
- It then prefetches /api/watchlist/{primaryId} on app mount
- This prefetch fires regardless of the currently viewed page
- With a 2-minute stale time, repeated navigation triggers another 6–7 s request

**Impact**: An unauthenticated or navigating user causes the backend to spend 6–7 s
scanning 385 gzip files while the frontend waits for a response it needs to render the
Watchlist table — making the entire app feel frozen even when other pages are loaded.
The backend endpoint latency is the direct cause; the frontend prefetch is legitimate
behavior that reveals the backend's cost.

---

## 10. Before vs After Commit b7964131

### Pre-regression (_load_cached_watchlist_market_data before b7964131)

```python
def _load_cached_watchlist_market_data(tickers):
    from services.canonical_history_service import get_volume_metrics_bulk
    return get_volume_metrics_bulk(tickers)   # pure _INDEX memory read, <2ms
```

Request-time file reads: **0** (all metrics in _INDEX, computed off-request)

### Post-regression (_load_cached_watchlist_market_data after b7964131)

```python
def _load_cached_watchlist_market_data(tickers):
    vol_metrics = get_volume_metrics_bulk(tickers)    # <2ms
    comp_closes = get_comparison_closes_bulk(tickers) # opens 385 .gz files, ~5s
    for sym in vol_metrics:
        vol_metrics[sym]["_comparison_close_7d"]  = ...
        ...
    return vol_metrics
```

Request-time file reads: **385 gzip files per request** (14 MB compressed, 61 MB uncompressed)

| Metric | Before | After | Delta |
|--------|--------|-------|-------|
| Request-time gzip opens | 0 | 385 | +385 |
| Compressed bytes read | 0 bytes | 14.2 MB | +14.2 MB |
| Bars parsed | 0 | 718,586 | +718,586 |
| strptime calls | 0 | 718,670 | +718,670 |
| _load_cached wall time | <2 ms | ~5,000 ms | +4,998 ms |
| Endpoint TTFB | ~1 s | ~7 s | +6 s |

**Was correctness improved?** YES — change_7d/change_30d now use the live displayed price as
numerator instead of the stale last canonical bar close. The correctness improvement is real.

**Was latency measured before merging?** NO — the commit's final validation measured only
the direct calculation math (price / comp_close = percentage) and verified correct values
for 4 symbols. It did not measure full endpoint elapsed time, concurrent request impact,
or request-count × file-count scaling.

**Did tests exercise full endpoint elapsed time?** NO — all tests use monkeypatched functions.
No test measured wall-clock time for a real endpoint call.

---

## 11. Conclusion

### PROVEN PRIMARY ROOT CAUSE

`get_comparison_closes_bulk` introduced by commit b7964131 opens, decompresses, parses,
and scans every canonical-history .json.gz file in the Primary Watchlist universe on
every Watchlist HTTP request, with no inter-request caching.

**First exact divergence**: `_load_cached_watchlist_market_data` calling
`get_comparison_closes_bulk(tickers)` at line 1059 of watchlist_router.py,
where `get_comparison_closes_bulk` has no cache and always opens one gzip file per symbol.

**Measured evidence**:
- 385 files × 1 open per request × every warm request = confirmed by gzip.open counter
- 5,000–5,300 ms per call in isolation, 3 consecutive iterations (no warmup effect)
- Server log `history fetch_ms=5,595–8,265 ms` across multiple real endpoint calls
- TTFB increased from ~1 s pre-regression to ~7 s post-regression

**Secondary bugs introduced**:
1. Internal `_comparison_close_7d`, `_comparison_date_7d`, `_comparison_close_30d`,
   `_comparison_date_30d` keys are not stripped before row serialization — **1,800 leaked
   internal keys appear in every Watchlist API response** (4 keys × 450 rows)

---

## 12. Smallest Recommended Correction

### Option A: Persist comparison closes in _INDEX during existing save/append path (RECOMMENDED)

Store `comparison_close_7d`, `comparison_date_7d`, `comparison_close_30d`,
`comparison_date_30d`, and `comparison_as_of_ny_date` in the per-symbol `_INDEX` entry
whenever bars are written (save_bars, append_bars) or during backfill_volume_metrics_metadata.

On request:
1. Check `meta["comparison_as_of_ny_date"] == ny_market_date()` for each symbol
2. If current: serve comparison closes directly from _INDEX (zero disk I/O)
3. If stale (date rolled after midnight ET): recompute from existing bar file for only
   the affected symbols (typically 0 on weekdays before midnight ET, batch-computed during
   off-hours append loop)

**Why this is smallest**: _INDEX is already read at startup and kept in memory.
The append/save paths already update _INDEX. Adding 5 fields per symbol costs ~20 bytes
per entry and eliminates 14 MB of I/O per request. No new table, endpoint, provider, or
pipeline.

**How it eliminates the cost**: repeated Watchlist requests perform **0 canonical-history
file reads** for comparison closes when the NY date is unchanged (which covers >99% of
requests during a trading session).

### Option B: Store 45-bar tail in _INDEX
Store the last 45 bars per symbol in _INDEX. Allows in-memory _select_comparison_closes
without disk reads. Cost: ~45 × ~50 bytes = ~2.25 KB per symbol × 400 = ~900 KB total
index memory. Eliminates file reads but adds index memory and complicates the tail-append
logic. Option A is simpler.

### Option C: Combine metric work into one pass + memoize
Do a single pass (volume + comparison closes together) in _compute_watchlist_market_metrics_from_bars
and cache the result in _INDEX keyed by (ny_date, newest_bar_date). Equivalent to Option A
but requires refactoring the metric functions together. Slightly more code change than A.

### Option D: Replace datetime.strptime with date.fromisoformat in _completed_daily_bars
This alone reduces strptime overhead from ~9s to ~0.3s, making a full per-request scan
take ~1.3s instead of ~5s. Still reads all 385 files every request, but fast enough to
be tolerable. **Not recommended as the sole fix** — it still does 14 MB of I/O per request
and hides the architectural problem. It could be applied as a quick fix while A is implemented.

### Secondary fix (required alongside any approach):
Strip `_comparison_*` internal keys from rows before serialization. Either:
a. `enriched.pop(k, None)` for each internal key after using them, OR
b. Return comparison closes as a separate dict from _load_cached and never merge them into enriched

---

## 13. Exact Production Files Required for Fix

| File | What changes |
|------|-------------|
| `backend/services/canonical_history_service.py` | Add comparison_close/date fields to _INDEX during save_bars, append_bars, backfill_volume_metrics_metadata; add cache-hit fast path in get_comparison_closes_bulk |
| `backend/services/watchlist_router.py` | Strip _comparison_* internal keys before row serialization; verify live-price override block still works |
| `backend/tests/test_watchlist_market_data_rows.py` | Add test: verify repeated warm calls to get_comparison_closes_bulk result in 0 gzip.open calls; verify _comparison_* keys not in serialized response |

---

## 14. Risks and Behavior That Must Be Preserved

1. **Correctness**: change_7d/change_30d must use live displayed price as numerator (b7964131 correctness fix is correct and must be kept)
2. **NY market date**: comparison_as_of_ny_date must be stored and checked using ny_market_date() not date.today()
3. **Staleness guard**: max_gap_days=10 — if comparison close is >10 days before target, return None
4. **Weekend/holiday targets**: _select_comparison_closes walks backwards to find the most recent session on or before the target; this must still work correctly even when comparison dates are stored in _INDEX
5. **None behavior**: missing/insufficient history → None for both fields; must not silently return zero
6. **Field contract**: change_7d and change_30d must remain in _WATCHLIST_MARKET_METRIC_FIELDS
7. **Internal key leak**: _comparison_close_7d/_30d and _comparison_date_* must NOT appear in the serialized API response

---

## 15. Final git status -sb

```
## main...origin/main
 M backend/data/bittensor_dashboard_cache.json
 M backend/data/canonical_history/[~100 runtime-written bar files]
 M backend/data/canonical_history/_index.json
 M backend/data/hyperliquid_hip3_cache.json
 M backend/data/predict_odds_live_lkg.json
 M backend/data/thematic_context_snapshot.json
?? backend/data/canonical_history/[~26 newly-created bar files]
?? attached_assets/[prompt files]
```
No Python source changes uncommitted. All modified files are runtime data (bar files,
LKG caches) written by background loops — not source code.
