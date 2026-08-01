# OpenCode Report — Horizon-Tab Staleness Fix (rolling 90-day macro horizon)

**Task requested:** Continue the rolling 90-day macro-horizon task from the
current HEAD (`c587e300`) after deployment commit `8495f7a6` was pushed to
origin/main. Do not touch the deployment commit or its files.

**Completion status:** Completed — one focused defect fix committed.

---

## 1. Verified Git state

```
repo top-level: /home/runner/workspace
branch:         main
main...origin/main [ahead 2]  (c587e300 + c5bc9183, both local-only)
origin/main:    8495f7a6  (pushed deployment commit)
```

`8495f7a6` contains only runtime/cache/OpenCode report artifacts
(`.opencode-persistent/`, `.opencode-reports/`, `backend/data/*`). It was
neither reverted, reset, amended, nor modified. No file from that commit was
staged or touched. The task resumed from HEAD `c587e300`.

## 2. Proven root cause

`_snapshot_is_stale()` in `backend/services/calendar_snapshot_service.py`
applied the legacy week-rotation check to horizon tabs:

```python
if tab in _TABS_WITH_HORIZON:
    evts = slot.get("events") or []
    if evts:
        horizon_end = meta.get("horizon_end") or ""
        if not horizon_end or horizon_end < friday.strftime("%Y-%m-%d"):
            return True
        if not cw:
            return True
        return stored_from < expected_from.strftime("%Y-%m-%d")
```

For horizon tabs, `meta.window.from` is the **horizon start** (`today - 14d`),
not the current week Monday. Since the horizon start is always before the
current ET Monday, `stored_from < expected_from` was always `True` — so a
**freshly refreshed** `economic_releases` snapshot whose `horizon_end`
covered the current Friday was still reported stale.

### Verified with actual values (2026-08-01 ET)

```
today           = 2026-08-01   monday = 2026-07-27
horizon window  = 2026-07-18 → 2026-10-29  (103-day span)
horizon_end     = 2026-10-29 >= friday 2026-07-31  → coverage complete
stored_from     = 2026-07-18 < expected_from 2026-07-27  → stale=True (BUG)
```

### Impact chain (consumers of `is_stale`)

1. `get_snapshot()` returns `is_stale=True` in the envelope for
   `economic_releases` even when the horizon is complete.
2. `backend/routes/catalyst_calendar.py:221` fires a background
   `refresh_tab(tab, FMP_API_KEY)` on **every** `/api/catalysts/events`
   request for a stale snapshot → FMP call storm.
3. `check_and_refresh_stale()` (startup, `calendar_snapshot_service.py:793`)
   refreshed the horizon tab on every boot.
4. `weekly_scheduler_loop()` daily stale-check
   (`calendar_snapshot_service.py:858`) refreshed the horizon tab each week
   outside the Sunday window.

All of these contradicted the task's goals of preserving FMP rate limits and
making no provider calls inside request paths.

## 3. Exact fix

`_snapshot_is_stale()` horizon branch now keys staleness **only** on
`horizon_end` covering the current ET Friday:

```python
if tab in _TABS_WITH_HORIZON:
    evts = slot.get("events") or []
    if evts:
        horizon_end = meta.get("horizon_end") or ""
        if not horizon_end or horizon_end < friday.strftime("%Y-%m-%d"):
            return True
        return False
    if not cw:
        return True
```

Rationale: a complete horizon spans `today - 14d → today + 89d`, so the
current Mon–Fri week is always inside the cached collection; the legacy
week-rotation comparison does not apply.

The `coverage.complete` logic in `get_snapshot()` was already correct and is
unchanged (it compares `horizon_end` against the requested Friday).

## 4. Files changed

| File | Type |
|------|------|
| `backend/services/calendar_snapshot_service.py` | Production (1 function + docstring) |
| `backend/tests/test_calendar_snapshot.py` | Test (4 new regression tests) |

2 files changed, 94 insertions(+), 9 deletions(-).

No change to `catalyst_calendar.py`, `home_top_catalysts.py`,
`pg_storage.py`, frontend, providers, or endpoints.

## 5. Behavior changed

- `economic_releases` snapshot with a complete horizon (`horizon_end` ≥
  current ET Friday) is no longer reported stale. Only a snapshot whose
  `horizon_end` is missing or before the current Friday is stale.
- No more request-time background FMP refresh storm for the horizon tab.

## 6. Behavior deliberately preserved

- Non-horizon tabs (dividends/ipos/splits/treasury_macro) keep the legacy
  `stored_from < expected_from` week-rotation staleness check.
- `horizon_end` insufficient (before Friday) → still stale.
- Horizon tab with events but no `horizon_end` meta → still stale.
- `coverage`, `horizon`, `events` envelope fields unchanged.
- Sunday scheduled full refresh and hourly scheduler cadence unchanged.
- No new provider calls, endpoints, or persistence changes.

## 7. Validation

```
pytest tests/test_calendar_snapshot.py -v           → 33 passed (was 29; +4 new)
pytest tests/test_calendar_snapshot.py \
       tests/test_calendar_curation.py \
       tests/test_home_top_catalysts.py \
       tests/test_top_catalysts.py -q               → 291 passed
```

Direct envelope validation (mocked Neon slot, fresh horizon):
`get_snapshot('economic_releases')` → `is_stale=False`, `status=ready`,
`coverage={'complete': True, 'horizon_end': '2026-10-29', ...}`.

`git diff --check` → no whitespace errors.

## 8. Database, provider, cache, runtime effects

- No database writes, schema changes, or migrations.
- No FMP provider calls were made during validation.
- No server started/stopped; no curl; pytest-only.
- Runtime artifacts (`backend/data/*`, `.opencode-persistent/`,
  `.opencode-reports/`) left untouched and unstaged.

## 9. Risks / remaining issues

- The `economic_releases` horizon snapshot will only be refreshed when its
  `horizon_end` no longer covers the current Friday or on the weekly Sunday
  scheduled refresh (unchanged cadence). This is the intended behavior.
- Frontend consumption of `is_stale` for the horizon tab is unchanged in
  contract but now reports `false` for complete horizons; this is the
  correction of a defect, not a contract break.

## 10. Final git status

```
## main...origin/main [ahead 2]
 M .opencode-persistent/state/model.json
 M .opencode-persistent/state/prompt-history.jsonl
 M .opencode-reports/latest.md
 M backend/data/*                    (runtime artifacts — not staged)
?? backend/data/canonical_history/*.json.gz  (runtime artifacts — not staged)
```

## 11. Commit

```
c5bc9183 fix(calendar): horizon-tab staleness no longer fires request-time FMP refresh
```

Complete task commit diff (staged files only):

```
 backend/services/calendar_snapshot_service.py | 22 +++++---
 backend/tests/test_calendar_snapshot.py       | 81 +++++++++++++++++++++++++++
 2 files changed, 94 insertions(+), 9 deletions(-)
```

Nothing pushed. `git push` not executed.
