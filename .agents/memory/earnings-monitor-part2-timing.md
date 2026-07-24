---
name: Earnings Monitor Part 2 — timing, polling cadence, EI refresh
description: Key constraints and decisions for the Live Earnings Monitor Part 2 implementation
---

## FMP Starter plan constraints
- `/stable/earnings-calendar?includeReportTimes=true` returns `time` (amc/bmo/null) and `confirmed` (bool) only — NO exact clock time on Starter plan.
- FMP ignores per-symbol filter on earnings-calendar — always returns the full date window. Must filter client-side.
- `get_earnings_calendar_with_times()` uses 1800s TTL; one batch call for the entire universe.

**Why:** FMP Starter doesn't expose clock-level timing, so BMO→08:00ET and AMC→16:30ET anchors are used as the `expected_at` timestamp.

## DB schema
10 new columns added via `ADD COLUMN IF NOT EXISTS` in `init_earnings_monitor_tables()`:
`expected_at, expected_time_local, expected_timezone, report_time_status, report_period, schedule_source, schedule_updated_at, fmp_check_stage, results_first_detected_at, monitoring_expires_at`

**Why:** All idempotent — safe to call on existing DB with already-populated rows.

## Polling cadence (compute_next_fmp_check)
Stages relative to `expected_at`:
- `pre_release_m30`: t ∈ (-∞, -15m) → wake at expected_at - 30m
- `pre_release_m15`: t ∈ [-30m, -5m) → wake at expected_at - 15m
- `pre_release_m5`:  t ∈ [-15m, -1m) → wake at expected_at - 5m
- `at_release`:      t ∈ [-5m, 0) → wake at expected_at
- `post_release_m1`: t ∈ [0, 1m) → wake at expected_at + 1m
- `polling_60s`:     t ∈ [0, 60m) → 60s interval
- `polling_120s`:    t ∈ [60m, 120m) → 120s interval
- `polling_300s`:    t ∈ [2h, 6h) → 300s interval
- `expired`:         t ≥ 6h → 6h interval (backfill only)

## get_due_targets() SQL contract
Targets are due when: `next_fmp_check_at <= NOW() OR next_sec_check_at <= NOW() OR (both are NULL)`.
NULL check times = always due (first-pass targets before any check runs).

**Why:** This prevents all 54 targets from being polled every 60s; only targets whose scheduled check time has arrived get processed.

## _refresh_schedule batch pattern
One batch FMP calendar call for the full universe (not N per-symbol HTTP calls). FMP response is a dict keyed by symbol after client-side filtering. If symbol is absent from the calendar response, the service uses the old timing (fallback_count incremented but no extra HTTP call).

## EI refresh trigger
`_trigger_ei_refresh(symbol)` calls `FmpFundamentalsRefresher(fmp_api_key)._fetch_earnings_intelligence(symbol)` then `merge_fields(symbol, {"earnings_intelligence": ei_data})`. Fire-and-forget via `asyncio.create_task()` — result not awaited, errors are swallowed to avoid blocking `_process_target`.

## Router pattern
`_target_to_dict(t)` is a module-level function (not inside a route handler) so both `GET /monitor/targets` and `GET /monitor/targets/{symbol}` can reuse it without import.
