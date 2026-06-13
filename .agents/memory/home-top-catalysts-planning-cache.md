---
name: Home Top Catalysts Saturday planning cache
description: Why planning-window data is NOT stored in calendar snapshot Neon slots — separate in-memory cache is required
---

## The rule
Sat/Sun next-week planning data for `/api/home/top-catalysts` must be stored
in the process-local `_planning_cache` dict in `home_top_catalysts.py`, NOT
written to Neon's `calendar_snapshots` table.

**Why:** If you write next-week data to the Neon snapshot slot, Sunday's
`weekly_scheduler_loop` will overwrite it with prior-week data (the loop calls
`refresh_tab(tab, fmp_key)` which uses `_week_window_for()` → `_et_week_monday()`
→ returns the PAST Monday on Saturday/Sunday, not next Monday). The Neon slots
are single-slot-per-tab and shared with the Calendar page.

**How to apply:**
- `_fetch_planning_tab(tab, monday, friday, fmp_key)` fetches directly via
  `_fetch_tab()` from `catalyst_calendar_service` with explicit dates.
- Results are cached in `_planning_cache` dict with 23h TTL.
- `asyncio.Lock()` per cache-key prevents concurrent duplicate fetches.
- `build_home_top_catalysts()` is async; route handler does `await`.
- Saturday proactive warmup: `_home_planning_warmup_loop()` in `main.py`
  fires hourly, calls `warm_planning_window()` on weekday==5.

**What is NOT changed:**
- `calendar_snapshot_service.py` — not modified
- Calendar tab endpoints — not modified
- Earnings clean service — not modified
