# Task: Per-chunk coverage ranges and exception-safe bootstrap

**Completed:** 2026-08-02
**Commit:** `44439ec7` — `fix(calendar): per-chunk coverage ranges and exception-safe bootstrap`
**Push:** `origin/main` — confirmed at HEAD, local `main`, `origin/main`, `origin/HEAD`

---

## 1. Issues proven and fixed

### Issue 1 — actual outer bounds do not prove complete internal coverage

**Root cause:** `get_snapshot_window()` used `_actual_bounds(pool)` which returns [earliest_event_date, latest_event_date]. This correctly rejects windows before/after the stored data but treats ALL dates between as covered — even when individual provider chunks failed.

**Example of the bug:**
- August chunk succeeds (events in Aug)
- September chunk fails (FMP 402 / timeout)
- October chunk succeeds (events in Oct)
- actual_bounds = Aug-1 to Oct-31
- A Sep-15 request → `coverage_complete=True`, `empty_reason=no_events_in_window`
- This is FALSE — there is NO data for September

**Fix:** `_chunked_economic_fetch()` now returns a third value: per-chunk `coverage_ranges` with status `complete`/`failed`/`empty`. These are stored in meta and persisted across refreshes via `_merge_coverage_ranges()`. `get_snapshot_window()` now checks `_window_in_any_range()` — a window is covered only if at least one successful chunk range fully contains it.

### Issue 2 — bootstrap state is not exception-safe

**Root cause:** `_bootstrapping[tab] = True` was set before the main refresh body, and cleared with `if is_bootstrap_start: _bootstrapping[tab] = False` after the body. Any exception between these lines left the flag permanently True, blocking all future refreshes.

**Fix:** The entire refresh body (fetch + merge + lock + write) is now wrapped in `try:/finally: if is_bootstrap_start: _bootstrapping[tab] = False`. The flag always clears regardless of exception.

### Issue 3 — refresh read path less defensive

**Root cause:** `refresh_tab()` had two `_neon_read()` calls without try/except guards. A transient Neon failure (pool exhaustion, read timeout) would crash the entire coroutine.

**Fix:** Both `_neon_read()` calls are now wrapped in try/except with disk fallback.

## 2. Files changed

| File | Lines changed | Description |
|------|--------------|-------------|
| `backend/services/calendar_snapshot_service.py` | +414/-179 | Coverage ranges, bootstrap try/finally, hardened reads |
| `backend/tests/test_calendar_snapshot.py` | +140 | 14 new tests |

## 3. New coverage range model

```python
# Stored in snapshot meta.coverage_ranges:
[
    {"from": "2025-09-02", "to": "2025-10-31", "status": "complete"},
    {"from": "2025-11-01", "to": "2025-12-31", "status": "failed"},
    {"from": "2026-01-01", "to": "2026-02-28", "status": "complete"},
]
```

### Semantics
- `complete` — provider returned events (chunk successfully fetched)
- `empty` — provider returned zero events (successful query, no releases in period)
- `failed` — provider raised/returned error (genuine gap)
- A window is covered ONLY if it falls inside a `complete` or `empty` range
- A `failed` incoming range NEVER overwrites a prior `complete` range
- A `complete` incoming range OVERWRITES a prior `failed` range
- Adjacent same-status ranges are compacted for storage efficiency
- Legacy snapshots without coverage_ranges fall back to `_actual_bounds()`

## 4. Bootstrap lifecycle

**Before:**
```python
_bootstrapping[tab] = True
# ... body (fetch, merge, lock, write) ...
# ANY exception here leaves flag stuck
if is_bootstrap_start:
    _bootstrapping[tab] = False
```

**After:**
```python
try:
    # ... body (fetch, merge, lock, write) ...
finally:
    if is_bootstrap_start:
        _bootstrapping[tab] = False
```

## 5. Tests

```
backend$ pytest tests/test_calendar_snapshot.py tests/test_calendar_curation.py \
  tests/test_home_top_catalysts.py tests/test_top_catalysts.py -q
412 passed in 0.39s
```

New tests (14):
- `test_window_inside_complete_range`
- `test_window_outside_any_range`
- `test_window_in_failed_range_is_not_covered`
- `test_window_in_empty_range_is_covered`
- `test_window_straddling_multiple_ranges`
- `test_window_inside_single_range_across_two`
- `test_merge_adds_new_ranges`
- `test_new_complete_overwrites_prior_failed`
- `test_failed_does_not_overwrite_prior_complete`
- `test_compact_adjacent_same_status`
- `test_no_compact_across_different_status`
- `test_gap_between_chunks_is_not_covered`
- `test_coverage_ranges_in_get_snapshot_window`
- `test_coverage_uses_ranges_not_bounds_when_ranges_present`

## 6. Route verification

All four Economic Releases views return HTTP 200:
```
week   status=200 events=50  coverage=True  empty_reason=None
month  status=200 events=500 coverage=True  empty_reason=None
day    status=200 events=48  coverage=True  empty_reason=None
recent status=200 events=50  coverage=True  empty_reason=None
```

## 7. Architecture preserved

- Single `public.calendar_snapshots` table — no new table
- Single endpoint `GET /api/catalysts/events` — no new endpoint
- Existing scheduler (Sunday + hourly stale-check) — unchanged
- Existing curation, frontend contract — unchanged
- No request-path provider call — unchanged
- Coverage metadata added to existing snapshot `meta` object only

## 8. Commit and push

- **Commit:** `44439ec711a49c93d251c17be8fe18d8f0705c2f`
- **Push:** `860ea410..44439ec7  main -> main`
- **Verified at:** HEAD, local `main`, `origin/main`, `origin/HEAD`
