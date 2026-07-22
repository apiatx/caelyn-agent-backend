# Earnings Intelligence — Refresh Cadence

## Existing Scheduler Architecture (Preserved)

earnings_intelligence is refreshed automatically by the existing weekly
fundamentals scheduler (`_watchlist_fundamentals_weekly_loop` in `main.py`).

No new scheduled tasks were created. No per-ticker scheduled tasks were added.
No popup-triggered background refreshes were added.

## Weekly Refresh

- **Window**: Sunday 02:00–05:00 ET (unchanged — same as all other FMP fundamentals)
- **Entry point**: `FmpFundamentalsRefresher.refresh_symbols()` — same function
  used by the existing weekly loop (line ~807 in `main.py`)
- **What runs**: `_fetch_earnings_intelligence(sym)` is called inside
  `refresh_symbols()` for every symbol in each weekly batch, BEFORE
  `upsert_snapshot()` writes to Neon. This means earnings_intelligence is
  always written atomically together with all other fundamentals fields in a
  single `upsert_snapshot()` call (no merge_fields race possible during
  normal operation).
- **Budget**: Governed by `WATCHLIST_FUNDAMENTALS_MAX_PER_RUN` env var
  (default 50 symbols/run). The weekly window can run multiple batches of 50
  until all due symbols are processed.
- **FMP calls per symbol**: 7 guaranteed (2 parallel in batch A + 5 parallel
  in batch B). 1 optional (FMP bars fallback only when canonical_history_service
  returns no data for a symbol).

## `reactions_final = false` Handling

When a quarter's price reaction is still incomplete (less than 5 trading sessions
have elapsed since the earnings date), `reactions_final = false` is stored.

On the NEXT weekly refresh cycle, `_fetch_earnings_intelligence()` re-fetches
fresh bars from canonical_history_service and recomputes the reaction. Once 5
trading sessions are available, `reactions_final` becomes `true` and the
reaction is never recomputed again (it is considered final).

There is no separate event-aware post-earnings trigger. Reactions finalize
within 7–8 calendar days of the earnings date (5 trading sessions), which
is within the next weekly refresh cycle.

## No Targeted Post-Earnings Refresh

No safe event-aware earnings refresh path exists in the current scheduler
architecture. The weekly cycle is the canonical refresh mechanism. All
earnings intelligence (history, ratings, reactions) is refreshed weekly.

**Limitation**: A symbol that reports earnings on Monday morning will have
stale `reactions_final = false` until the next Sunday window. The reaction
data will then be finalized in that single weekly pass.

## Ratings and Price Targets

Both are refreshed weekly with the same `_fetch_earnings_intelligence()` call.
There is no separate ratings-only or price-target-only refresh path.

## One-Time Initial Backfill

Performed via `POST /api/watchlist/debug/earnings-intelligence/backfill`.
This endpoint uses `merge_fields()` (PostgreSQL `||` JSONB merge) rather than
the full `upsert_snapshot()`, because:
- It only adds `earnings_intelligence` to existing snapshots without re-running
  the full 10-call fundamentals normalize step for each symbol.
- It is safe for a controlled one-time pass (no competing writers during the
  manual backfill).

Subsequent refreshes use `upsert_snapshot()` (via `refresh_symbols()`) for
atomic writes.
