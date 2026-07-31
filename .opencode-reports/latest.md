# Calendar Macro Signal Fields — Snapshot Refresh Verification

**Date:** 2026-07-31  
**Agent:** DeepSeek via OpenCode  
**Task:** Refresh `economic_releases` and `treasury_macro` snapshots to populate `event_family`, `signal_tier`, and `signal_reason` fields

---

## Task Requested

Prove whether the currently served snapshot events contain `event_family`, `signal_tier`, and `signal_reason` fields, then refresh the existing `economic_releases` and `treasury_macro` snapshots through the existing refresh path only.

## Completion Status

**Success.** Both snapshots refreshed. All representative events verified with correct macro signal fields. Zero production code changes.

## Proven Root Cause

The Neon snapshot data for `economic_releases` (last updated 2026-07-27T04:00:15Z) and `treasury_macro` (last updated 2026-07-28T04:26:42Z) were generated **before** the macro signal classification code (`_classify_event_family`, `_compute_signal_tier`, `_compute_signal_reason`) was added to `_fetch_economic_releases()` and `_fetch_treasury_macro()` on 2026-07-30.

The source code at `catalyst_calendar_service.py:1541-1566` (economic) and `catalyst_calendar_service.py:1624-1680` (treasury) correctly calls `_build_event()` with all three fields, but the persisted snapshot events predated these code additions. The snapshot staleness checker only compares date windows, not code freshness, so the stale data was served without these fields.

## Existing Path Preserved

The existing CLI backfill mechanism in `calendar_snapshot_service.py:714-763` (`_manual_backfill`) was used. This calls the same `refresh_tab()` function used by the Sunday scheduler and startup stale-check. No new endpoint, cache, scheduler, table, column, provider, or refresh path was created.

## Files Changed

Zero production code files changed. The following runtime data files were modified by the backfill as a side effect:

| File | Change | Notes |
|---|---|---|
| `backend/data/calendar_snapshots.json` | Updated (disk fallback mirror) | Runtime emergency cache, never source of truth |
| Neon `public.calendar_snapshots` (rows `economic_releases`, `treasury_macro`) | Updated | Source of truth |

## Behavior Changed

- **`economic_releases` current_week events now include** `event_family`, `signal_tier`, `signal_reason` for all 662 events
- **`treasury_macro` current_week events now include** `event_family`, `signal_tier`, `signal_reason` for all 10 events
- Previous_week events for both tabs still lack these fields (expected — those were promoted from the pre-refresh snapshot and will be populated on the next weekly refresh)

## Behavior Deliberately Preserved

- `Atlanta Fed GDPNow (Q3)` classifies as `other_us / secondary` rather than `gdp / major`. This is a pre-existing classification edge case: the regex `\bgdp\b` cannot match word-internal "gdp" in "GDPNow". This matches the existing classification rule behavior — no change made.
- Snapshot staleness check still uses date-window comparison only (does not check code freshness)
- Treasury Yield Snapshot events classify as `treasury_snapshot / context`
- Foreign events (EU CPI, BoJ Rate Decision) classify as `foreign / context`
- Curation layer (`calendar_curation.curate_envelope`) still caps events to 50 per slice

## Command Executed

```bash
# Working directory: /home/runner/workspace/backend
python -m services.calendar_snapshot_service --backfill --tabs economic_releases,treasury_macro
```

### Output Summary

```
[backfill] manual run starting; tabs=['economic_releases', 'treasury_macro']
[backfill] Neon connectivity OK; calendar_snapshots table ensured.
[backfill] → refreshing tab=economic_releases ...
[catalyst] FMP economic-calendar status=200 rows=662 ms=869
[calendar_snapshot] neon write tab=economic_releases ok=True status=ready current_week=662 previous_week=556
[backfill] ✓ tab=economic_releases status=ready current_week=662 previous_week=556 last_updated=2026-07-31T14:24:25.646058+00:00
[backfill] → refreshing tab=treasury_macro ...
[catalyst] FMP treasury-rates status=200 rows=62 ms=136
[calendar_snapshot] neon write tab=treasury_macro ok=True status=ready current_week=10 previous_week=8
[backfill] ✓ tab=treasury_macro status=ready current_week=10 previous_week=8 last_updated=2026-07-31T14:24:27.934838+00:00
[backfill] DONE — all 2 tabs refreshed successfully.
```

Return code: 0

## Before and After Snapshot Timestamps

| Tab | Before (UTC) | After (UTC) | Age |
|---|---|---|---|
| economic_releases | 2026-07-27T04:00:15 | 2026-07-31T14:24:25 | < 1 min |
| treasury_macro | 2026-07-28T04:26:42 | 2026-07-31T14:24:27 | < 1 min |

## Representative Event Verification Table

All checked against live API response `GET /api/catalysts/events?tab=economic_releases&scope=all` and `GET /api/catalysts/events?tab=treasury_macro&scope=all` after refresh.

| Event | Country | Importance | event_family | signal_tier | signal_reason | Status |
|---|---|---|---|---|---|---|
| Fed Interest Rate Decision | US | high | fomc_decision | critical | Scheduled FOMC rate decision | ✓ |
| Initial Jobless Claims (Jul/25) | US | high | jobless_claims | secondary | Weekly jobless claims | ✓ |
| Core PCE Price Index YoY (Jun) | US | high | pce | major | Fed-preferred inflation measure | ✓ |
| CPI (Jul) | EU | high | foreign | context | Foreign macro release (EU) | ✓ |
| BoJ Interest Rate Decision | JP | high | foreign | context | Foreign macro release (JP) | ✓ |
| Chicago PMI (Jul) | US | high | pmi | secondary | Purchasing managers index | ✓ |
| Atlanta Fed GDPNow (Q3) | US | high | other_us | secondary | US economic release | ✓ (correct per existing rules) |
| 2Y Treasury Rate | — | high | treasury_rate | context | Routine Treasury yield observation | ✓ |
| 10Y Treasury Rate | — | high | treasury_rate | context | Routine Treasury yield observation | ✓ |
| 30Y Treasury Rate | — | high | treasury_rate | context | Routine Treasury yield observation | ✓ |
| Treasury Yield Snapshot | — | medium | treasury_snapshot | context | Treasury yield snapshot | ✓ |

**Chinese PMI:** Not present in the current FMP data window (2026-07-27 to 2026-07-31). No Chinese PMI events were returned by FMP for this week. This is a data availability issue, not a code issue.

## Database, Provider, Cache, and Runtime Effects

### Neon Postgres (`public.calendar_snapshots`)
- `economic_releases` row: current_week updated from 556→662 events, previous_week promoted from old current_week (556), last_updated set to `2026-07-31T14:24:25.646058+00:00`
- `treasury_macro` row: current_week updated from 8→10 events, previous_week promoted from old current_week (8), last_updated set to `2026-07-31T14:24:27.934838+00:00`

### FMP Provider Calls
- 1 call to `economic-calendar` endpoint (200 OK, 662 rows, 869ms)
- 1 call to `treasury-rates` endpoint (200 OK, 62 rows, 136ms)

### Disk Fallback
- `backend/data/calendar_snapshots.json` updated as best-effort emergency mirror (3.2 MB)
- This file is NOT committed per instructions

### In-memory Cache
- The existing FMP endpoint caches (`cat:econ:*`, `cat:treasury:latest`) were populated by the refresh

## Risks and Remaining Issues

1. **Previous_week events lack macro signal fields:** 50 `economic_releases` and 6 `treasury_macro` events in `previous_week` still have `event_family`, `signal_tier`, `signal_reason` as MISSING. These were promoted from the pre-refresh snapshot. They will be populated on the next weekly refresh (Sunday scheduler) or next stale-check refresh.

2. **Atlanta Fed GDPNow classification edge case:** The regex `\bgdp\b` does not match "GDPNow" (no word boundary between "GDP" and "Now"). This is a pre-existing classification rule behavior and was not changed per instructions.

3. **Code-freshness not tracked:** The snapshot staleness checker only compares date windows, not code version. Future code changes to classification rules would similarly require a manual backfill to take effect.

4. **Chinese PMI absence:** No Chinese PMI events appear in the current FMP data window. Not a bug — just no such events scheduled for this week.

## Git Status

```
## main...origin/main [ahead 1]
 M backend/data/calendar_snapshots.json
 (plus other pre-existing dirty files unrelated to this task)
```

## Commit

**Not applicable.** No commit was created per instructions. The only file changed by this task is `backend/data/calendar_snapshots.json` (runtime snapshot data), which is not be committed.

## Confirmation Summary

- Zero production code files modified
- Zero schema changes
- Zero new endpoints, caches, schedulers, tables, columns, or providers
- Existing refresh path (`python -m services.calendar_snapshot_service --backfill`) used exclusively
- `event_family`, `signal_tier`, `signal_reason` now present in all current_week events
- Representative events verified against live API response

---

**Final economic_releases last_updated:** `2026-07-31T14:24:25.646058+00:00`  
**Final treasury_macro last_updated:** `2026-07-31T14:24:27.934838+00:00`
