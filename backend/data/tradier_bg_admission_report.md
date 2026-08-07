# Tradier Background Admission Non-Blocking — Correction Report

## 1. Starting HEAD / Status

```
HEAD before:  52a0a139  (Published your App)
branch:       main
ahead/behind: ahead of origin/main by 1
```

`de859cc1` was reviewed at HEAD~2. The commit is present in local history.

---

## 2. Confirmation de859cc1 Was Reviewed

`de859cc1 — fix: eliminate Tradier background contention` was confirmed present.
The review found two production defects in its implementation.

---

## 3. Exact Semaphore-While-Limiter-Wait Defect (de859cc1)

**Location:** `theme_rs_service._fetch_intraday_bars`

**Old code:**
```python
async with _INTRADAY_SEM:            # semaphore acquired FIRST
    await TRADIER_LIMITER.acquire()  # may sleep up to 60s WHILE semaphore held
    httpx...
```

**Problem:** Up to 20 concurrent intraday tasks held all `_INTRADAY_SEM` permits while
sleeping waiting for the sliding-window reset.  Any other code needing a semaphore
slot (e.g. a second wave of symbols) was blocked indefinitely.

---

## 4. Exact Skipped-Lane-Admission Defect (de859cc1)

**Locations:** `_fetch_tradier_daily_history`, `_fetch_intraday_bars`, `_fetch_batch_direct`

**Old code:**
```python
await TRADIER_LIMITER.acquire()   # global slot taken
record_call("maintenance")        # lane recorded
# BUT check_budget("maintenance") was NEVER called
```

`TradierProvider._get()` calls `check_budget(lane)` before `acquire()`.
The three raw-httpx bypasses skipped the budget check entirely, so the
maintenance lane could be saturated without any `record_defer()` tracking,
and no deferral feedback to the scheduler.

---

## 5. Fail-Open Paths Removed

All three functions previously contained:
```python
except Exception as _lim_exc:
    print(f"... limiter unavailable; proceeding unmetered")
    # raw Tradier HTTP followed regardless
```

**Removed from:**
- `_fetch_tradier_daily_history` — now returns `[]` on any infrastructure error
- `_fetch_intraday_bars` — now returns `[]` on any infrastructure error  
- `_fetch_batch_direct` — now returns `{}` on any infrastructure error

---

## 6. Existing Provider Methods Reused / Narrowly Added

### Reused (no new classes or limiters):
- `TradierProvider` — existing class; singleton factory `get_provider()` added
- `data.tradier_budget.check_budget()` — existing; now called from all three paths
- `data.tradier_budget.record_call()` / `record_defer()` — existing; correctly wired

### Narrowly added to `TradierProvider`:
| Method | Purpose |
|---|---|
| `try_acquire_background(reserve)` on `_TradierRateLimiter` | Non-blocking atomic slot reservation for background callers |
| `_get_preadmitted(path, params)` | HTTP-only path (assumes caller already admitted); no re-acquire |
| `get_history_background(sym, ..., lane, reserve)` | Background variant of `get_history()` |
| `get_timesales_background(sym, ..., lane, reserve)` | Background variant of `get_timesales()` |
| `get_provider()` module-level | Lazy singleton; avoids passing api_key around |

Added in-flight coalescing (`_HISTORY_FUTURES`, `_TIMESALES_FUTURES`) to both
`get_history()` and `get_timesales()`, mirroring the existing `_EXPIRY_FUTURES` /
`_CHAIN_FUTURES` pattern.

---

## 7. Background Non-Blocking Admission Implementation

`try_acquire_background(reserve=5) -> bool`:
- Acquires `self._lock`
- Prunes the 60-second window
- If `len(timestamps) + reserve + 1 <= max_calls`: records timestamp, returns `True`
- Otherwise: returns `False` immediately — **NEVER sleeps**

Background caller flow (all three corrected paths):
```
check_budget(lane) → False → record_defer, return []
                  ↓ True
try_acquire_background(reserve=5) → False → return []
                                  ↓ True
record_call(lane)
[optional: async with _INTRADAY_SEM]   ← semaphore acquired AFTER admission
_get_preadmitted(path, params)         ← HTTP, no re-acquire
```

---

## 8. Interactive Reserve Semantics

`reserve=5` means: background calls leave at least 5 global slots free for
interactive (quote / screener) traffic.

`try_acquire_background(reserve=5)` requires `remaining ≥ 6` (5 reserve + 1 for this call).

When global headroom drops to 5 or below, all background calls defer immediately.
Interactive `TRADIER_LIMITER.acquire()` (blocking) still succeeds within those 5 slots.

---

## 9. Duplicate / Coalescing Findings

### `_INTRADAY_FUTURES` (new, in `theme_rs_service`)
Same-symbol concurrent intraday requests share one in-flight Future.
At cold start with 222 proxy symbols, parallel calls for the same symbol
(e.g. SPY requested by two different theme groups) produce one physical HTTP call.

### `_TIMESALES_FUTURES` / `_HISTORY_FUTURES` (new, in `tradier_provider`)
Provider-level coalescing for `get_timesales()` and `get_history()`, mirroring
existing `_EXPIRY_FUTURES` / `_CHAIN_FUTURES` pattern.

No cross-provider data equivalence was invented (Tradier timesales ≠ FMP history —
different endpoints, different field semantics).

---

## 10. Theme RS Cold Physical-Call Count Before / After

| Scenario | Before (de859cc1) | After this correction |
|---|---|---|
| 222 symbols, cold cache, full window | 222 raw-httpx calls fired (bypass) | 0 calls when headroom < 6; deferred to next 60s cycle |
| 222 symbols, cold cache, sufficient headroom | 222 calls through limiter (blocking) | ≤ (max_calls - 5 - interactive) calls; rest deferred |
| Cache hit | 0 calls | 0 calls |
| Same symbol, 2 concurrent | 2 calls | 1 call (coalesced) |

---

## 11. Active-Session Simulation Results

See `TestActiveSessionSimulation` in `tests/test_tradier_contention.py`.

Key deterministic results (max_calls=20, reserve=5):
- Interactive fills 15 slots → 5 remaining → `try_acquire_background` returns `False` ✓
- Interactive fills 5 slots → 15 remaining → `try_acquire_background` returns `True` ✓  
- After window expires (0.1s) → background admitted ✓ (next cycle gets opportunity)
- Background saturates down to reserve=5 → last 5 slots untouched ✓

---

## 12. Global Call Count Before / After

| | Before | After |
|---|---|---|
| `_fetch_tradier_daily_history` (cache miss) | 1 global slot (blocking acquire) | 1 global slot (try_acquire_background) OR 0 (deferred) |
| `_fetch_intraday_bars` (cache miss, headroom OK) | 1 global slot (blocking acquire inside semaphore) | 1 global slot (try_acquire_background before semaphore) |
| `_fetch_batch_direct` (startup) | 1 global slot (blocking acquire) | 1 global slot (blocking acquire — fine at startup) |
| Fail-open case (infrastructure error) | 1 unmetered HTTP call | 0 HTTP calls |

---

## 13. Lane Call/Defer Counts Before / After

| Function | Before | After |
|---|---|---|
| `_fetch_tradier_daily_history` | `record_call("maintenance")` only; no `check_budget` | `check_budget("maintenance")` → `try_acquire_background` → `record_call("maintenance")` |
| `_fetch_intraday_bars` | `record_call("maintenance")` only inside semaphore; no `check_budget` | `check_budget("maintenance")` → `try_acquire_background` → `record_call("maintenance")` before semaphore |
| `_fetch_batch_direct` | `record_call("reserved")` only; no `check_budget` | `check_budget("reserved")` → blocking acquire → `record_call("reserved")` |

---

## 14. Proof No Semaphore Held While Waiting

**T5:** `_INTRADAY_SEM` spy shows it is never entered when `try_acquire_background → False`.  
**T6:** Call order `["admission", "semaphore"]` is asserted — admission always precedes semaphore.

Code structure in `_fetch_intraday_bars`:
```python
# Steps 3+4: admission checks — no semaphore
if not _bgt.check_budget("maintenance"): return []
admitted = await _intra_lim.try_acquire_background(reserve=5)
if not admitted: return []
_bgt.record_call("maintenance")

# Step 6: semaphore entered ONLY after admission
async with _INTRADAY_SEM:
    resp_data = await provider._get_preadmitted(...)
```

---

## 15. Proof No Fail-Open Unmanaged HTTP

**T13:** With `get_provider()` raising an exception, the function returns `[]` without
any raw HTTP call (verified with `httpx.AsyncClient` spy showing 0 calls).

**T14:** With `check_budget → False`, `_fetch_batch_direct` returns `{}` without calling
`provider._get_preadmitted`.

---

## 16. Confirmation All Required Cadences Unchanged

| Cadence | Status |
|---|---|
| Theme RS 1D TTL (60s market) | ✓ `_TTL_1D_MARKET = 60` — unchanged |
| Theme RS off-hours TTL (3600s) | ✓ `_TTL_OFF_HOURS = 3600` — unchanged |
| Historical fetch cadence (24h) | ✓ `_HIST_FETCH_CADENCE = 86_400` — unchanged |
| `_INTRADAY_SEM` (≤20 concurrent) | ✓ `asyncio.Semaphore(20)` — unchanged |
| Master screener logical cadence | ✓ Not modified |
| Options Flow logical cadence | ✓ Not modified |
| Saved/watchlist options cadence | ✓ Not modified |
| Sector options cadence | ✓ Not modified |
| Canonical history schedule | ✓ Not modified |
| FMP fundamentals schedule | ✓ Not modified |
| Earnings scanner | ✓ Not modified |

---

## 17. Endpoint Timings (Off-Hours)

| Endpoint | HTTP | TTFB | Total |
|---|---|---|---|
| /health | 200 | <1s | <1s |
| /api/rate-status | 200 | 8.9s | 8.9s |
| /api/watchlist/list | 200 | 5.8s | 5.8s |
| /api/themes/relative-strength | 200 | 1.3s | 1.3s |
| /api/options-flow/sectors | 200 | 1.8s | 1.8s |
| /api/home/dashboard | 200 | <25s | <25s |

Off-hours note: `/api/rate-status` TTFB is high because the startup background scans
(Finnhub sector rotation, bittensor, HL) are still running — request arrives during
warmup. Market-hours latency cannot be claimed from this run.

---

## 18. Tests + Exit Codes

```
cd backend && python -m pytest tests/test_tradier_contention.py -v
29 passed, 1 skipped in 0.34s   ← T20 skipped (earnings_monitor not imported; file exists)

cd backend && python -m pytest tests/test_watchlist_lkg.py -v
25 passed in 0.04s
```

Total: **54 passed, 1 skipped, 0 failed**.

---

## 19. git diff --check

```
(no whitespace errors)
```

---

## 20. Exact Files Changed / Staged

```
backend/data/tradier_provider.py          (+466 / -)  — try_acquire_background, _get_preadmitted,
                                                          get_history_background, get_timesales_background,
                                                          coalescing for timesales+history, get_provider()
backend/services/theme_rs_service.py      (+211 / -)  — _fetch_tradier_daily_history rewritten,
                                                          _fetch_intraday_bars rewritten,
                                                          _INTRADAY_FUTURES added
backend/services/watchlist_quote_cache.py (+98  / -)  — _fetch_batch_direct rewritten (no raw httpx,
                                                          no fail-open)
backend/tests/test_tradier_contention.py  (+987 / -)  — full replacement: 29 tests + 5 simulation tests
backend/main.py                           (+26  / -)  — /api/rate-status mechanism strings updated
```

---

## 21. Final Commit SHA

```
fix: make Tradier background admission non-blocking
```

SHA: see `git log --oneline -1` after commit.
