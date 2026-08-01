# Agent report — DeepSeek through OpenCode

Task: Make the existing Economic Releases Calendar endpoint expose selectable
day/week/month windows from the persisted rolling macro horizon, and ensure Home
Saturday planning selects the same cached future events. No frontend changes.

Completion status: COMPLETE (implementation + validation + commit).

---

## 1. Proven first divergence

Ground truth (repository code + live Neon database read, not inference):

- The stored `public.calendar_snapshots` row for `economic_releases` was a
  **legacy snapshot** — `events=[]`, no `horizon_start`/`horizon_end` meta,
  window `2026-07-27..2026-07-31`, `last_updated=2026-07-31T14:24:25Z`
  (written before the rolling-horizon code shipped; no horizon refresh had run
  since deployment). The disk fallback file was identical. **The broad horizon
  did NOT exist in storage at task start** — this contradicts the task premise
  and is the first divergence.
- The Calendar route `GET /api/catalysts/events` (backend/routes/catalyst_calendar.py:104)
  has **no requested-window contract**: for `tab=economic_releases` it short-
  circuits to `get_snapshot()` and dumps `current_week`/`previous_week`/`events`
  (the whole 90-day collection) with no `view`/`date` window selection. Even if
  broad events existed, a future-week request could not be served.
- Home (`build_home_top_catalysts`) already prefers the broad `events` collection,
  but its legacy-snapshot coverage detection was broken: for a legacy snapshot
  without broad events it left `coverage_complete=True` (the `_parse_date("")`
  guard was a no-op), so a Saturday planning call reported a false empty state
  instead of requesting a refresh.

## 2. Exact endpoint / query contract — before and after

Before: `GET /api/catalysts/events?tab=economic_releases&mode=...&from=...&to=...`
accepted `from`/`to` but **ignored them for snapshot tabs**; no `view`/`date`.

After (additive, backward compatible):
- Added `view` (`recent|day|week|month`) and `date` (`YYYY-MM-DD`, ET) query
  params on the existing `/api/catalysts/events` route.
- For horizon tabs (`economic_releases`) with a requested window
  (`view`/`date`/`from`/`to`), the route serves a windowed slice via the new
  `calendar_snapshot_service.get_snapshot_window()`.
- Existing `from`/`to` are still honored as an explicit date-range override.
- No window requested → the existing full envelope is served byte-for-byte
  unchanged (verified: `view` key absent, `events` = full collection).

## 3. Exact source collection used — before and after

Before: snapshot path used `current_week`/`previous_week` only (legacy data) and
dumped the whole `events` collection; no selection.

After (in `get_snapshot_window`):
- Prefers the canonical broad `events[]` when present (`source=horizon`).
- Falls back to legacy `current_week` + `previous_week` for old snapshots
  (`source=legacy`).
- `view=recent` uses the envelope `previous_week` slice directly (preserves
  current Recent semantics even when the broad collection is capped).
- Selection = `_select_events_for_window(pool, win_start, win_end)` — the same
  inclusive date filter already used by the rolling-horizon implementation.

## 4. Week selection behavior

`view=week&date=YYYY-MM-DD` → Monday–Friday of the ET week containing `date`.
Current, previous, and future weeks inside the cached horizon all work.
Validated on real Neon data: `week&date=2026-08-04` → window `2026-08-03..2026-08-07`,
233 events, `coverage_complete=True`, `empty_reason=None`.

## 5. Month selection behavior

`view=month&date=YYYY-MM-DD` → first..last calendar day of that month, selected
from the whole rolling horizon (NOT restricted to `current_week`). Validated on
real data: `month&date=2026-08-15` → window `2026-08-01..2026-08-31`, 777 raw /
500 curated events, `coverage_complete=True`.

## 6. Home Aug 3–7 selection behavior

- `_planning_window` (unchanged) still selects the following Mon–Fri on
  Sat/Sun: `date(2026,8,1)` → `(2026-08-03, 2026-08-07, next_week_planning)`.
- Home reads the broad cached horizon (prefers `events`, falls back to
  `current_week`), applies US-only filtering, CPI/PPI/PCE/GDP/ECI family
  grouping, and release-package grouping — all already present.
- Fix: legacy snapshots (no broad events) now correctly report
  `coverage_complete=False` for a future planning week (only for horizon tabs,
  so point-in-time `treasury_macro` cannot flip the flag), so a refresh is
  requested instead of a false empty state.
- Validated on real data for Sat Aug 1: `coverage_complete=True`,
  `empty_reason=None`, 3 catalysts (Labor Market, Growth/Demand, Treasury),
  233 source events, window `2026-08-03..2026-08-07`.

## 7. Coverage and empty-state semantics

`get_snapshot_window` returns truthful metadata. Coverage is judged against the
**actual persisted event span** (ground truth), not the optimistic meta horizon:

- events present → `empty_reason=None`, `coverage_complete=True` when the
  persisted span covers the window.
- covered window, no events → `empty_reason="no_events_in_window"`.
- window beyond persisted span → `coverage_complete=False`,
  `empty_reason="outside_horizon"`.
- legacy snapshot without broad events, window outside stored weeks →
  `empty_reason="legacy_snapshot_without_horizon"`.
- snapshot with no data at all → `empty_reason="snapshot_empty"`.
- `view=recent` → covered when `previous_week` data exists.
- No fabricated data, no provider calls; response fields preserved additively
  (`view`, `requested_date`, `window_start`, `window_end`, `event_count`,
  `coverage_complete`, `horizon_start`, `horizon_end`, `empty_reason`).

## 8. Exact files changed

Production (3, within authorization):
- `backend/routes/catalyst_calendar.py`
- `backend/services/calendar_snapshot_service.py`
- `backend/services/home_top_catalysts.py`

Tests (2 authorized files):
- `backend/tests/test_calendar_snapshot.py`
- `backend/tests/test_home_top_catalysts.py`

## 9. Exact test commands and totals

Baseline before changes: `test_calendar_snapshot.py + test_home_top_catalysts.py
+ test_calendar_curation.py` → 248 passed.

After changes (focused, run separately):
- `python3 -m pytest tests/test_calendar_snapshot.py -q` → **71 passed**
- `python3 -m pytest tests/test_home_top_catalysts.py -q` → **103 passed**
- `python3 -m pytest tests/test_calendar_curation.py -q` → **123 passed**
- `python3 -m pytest tests/test_top_catalysts.py -q` → **43 passed**
- Combined relevant suite:
  `pytest tests/test_calendar_snapshot.py tests/test_home_top_catalysts.py
  tests/test_calendar_curation.py tests/test_top_catalysts.py -q` → **340 passed**

Full-suite note: `pytest tests/` cannot fully collect due to a **pre-existing,
unrelated** test-isolation defect: `tests/test_by_symbols_earnings.py` inserts a
fake `types.ModuleType("services.catalyst_calendar_service")` into `sys.modules`,
poisoning later collection of files that import the real module. This file and
the affected test files are untouched by this task (confirmed via `git diff`),
and the failure reproduces on import order alone. The 55 failures in the rest of
the full suite (`test_best_trades`, `test_screener_presets`, `test_sec_edgar`,
`test_xai_social_fixes`, `test_options_architecture`, `test_startup_timing`) are
network/async-timing/live-data dependent, none of them import the changed
modules, and all are pre-existing in this environment.

## 10. git diff --check result

`git diff --check` → clean (no whitespace errors). Result: OK.

## 11. Exact staged files

```
backend/routes/catalyst_calendar.py
backend/services/calendar_snapshot_service.py
backend/services/home_top_catalysts.py
backend/tests/test_calendar_snapshot.py
backend/tests/test_home_top_catalysts.py
```

## 12. Final Git status

```
## main...origin/main [ahead 2]
 M .opencode-persistent/state/prompt-history.jsonl
 M backend/data/bittensor_dashboard_cache.json
 M backend/data/calendar_snapshots.json
 M backend/data/hyperliquid_hip3_cache.json
 M backend/data/hyperliquid_signal_snapshots.json
 M backend/data/thematic_context_snapshot.json
```

The remaining dirty files are pre-existing runtime/cache/generated files.
`backend/data/calendar_snapshots.json` was updated by the manual horizon backfill
(see §Database effects); it is a cache file and was deliberately **not** staged.

## 13. Commit SHA and message

SHA: `a3a997e3669ca9455bf932d2fa4acb7573200502`
Message: `fix(calendar): serve requested windows from macro horizon`

## 14. Database, provider, cache, and runtime effects

- Database: the manual backfill refreshed the existing canonical
  `public.calendar_snapshots` row for `economic_releases` via the existing
  `refresh_tab()` path (FMP economic-calendar for the rolling window
  `2026-07-18..2026-10-29`). Stored: `events=1000`, `current_week=93`,
  `previous_week=556` (preserved), meta `horizon_start=2026-07-18`,
  `horizon_end=2026-10-29`. No schema change, no new table, no other rows.
  This is the same write the Sunday scheduler performs.
- Provider: the backfill used the existing FMP fetcher through the existing
  refresh path. **No new provider-call code path was added.** The request/read
  path makes zero FMP calls; when horizon coverage is complete no refresh fires
  (validated: `is_stale=False`, `refresh_attempted=False`).
- Cache: process-local caches untouched; the disk fallback JSON was mirrored by
  the backfill (best-effort emergency cache).
- Runtime: no server started/stopped; no scheduler modified; no new
  scheduler/job; earnings and treasury behavior untouched.

## 15. Risks and remaining issues

1. The rolling-horizon fetch caps the stored `events` at `limit=1000`
   (`refresh_tab` → `_fetch_tab`), so the persisted horizon spans
   `2026-07-31..2026-09-03` instead of the intended `07-18..10-29`. This was
   pre-existing (committed write path, outside the authorized read-path scope)
   and does not affect the required Aug 3–7 coverage. Impact: the current-week
   slice is narrower than the legacy current_week, and windows past 09-03 report
   `outside_horizon` truthfully. Fixing it means raising the horizon fetch limit
   in the write path (out of scope; reported for approval).
2. The stored legacy `previous_week` data spans the current week (data quirk of
   the pre-horizon seeding); `view=recent` returns it directly, preserving the
   pre-existing Recent display unchanged.
3. Month view applies the existing curation cap of 500 for display (`_MONTH_VIEW_CAP`);
   raw selection returns all matching horizon events.
4. Pre-existing full-suite collection collision (`test_by_symbols_earnings`
   fake-module poisoning) is unrelated and unfixed.

## 16. Confirmations

- No server was started or stopped.
- `curl` was not used.
- No new provider calls were added to any code path; the read path performs no
  FMP call and triggers no refresh when coverage is complete.
- Nothing was pushed. The user runs `git push origin main`.

## Complete task commit diff

Below is the full committed patch (`git show a3a997e3`).

```diff
commit a3a997e3669ca9455bf932d2fa4acb7573200502
Author: apiatx <aidanpilon@gmail.com>
Date:   Sat Aug 1 21:49:45 2026 +0000

    fix(calendar): serve requested windows from macro horizon

diff --git a/backend/routes/catalyst_calendar.py b/backend/routes/catalyst_calendar.py
index 4d186424..6349939f 100644
--- a/backend/routes/catalyst_calendar.py
+++ b/backend/routes/catalyst_calendar.py
@@ -37,9 +37,11 @@ from services.catalyst_calendar_service import (
     get_overview,
 )
 from services.calendar_snapshot_service import (
+    HORIZON_TABS as _HORIZON_TABS,
     TARGET_TABS as _SNAPSHOT_TABS,
     get_read_source as _get_read_source,
     get_snapshot as _get_snapshot,
+    get_snapshot_window as _get_snapshot_window,
 )
 from services.calendar_curation import (
     CURATED_TABS as _CURATED_TABS,
@@ -58,6 +60,10 @@ router = APIRouter(tags=["catalyst_calendar"])
 
 _AUTH_HEADER = "X-API-Key"
 
+# Month view keeps a larger curation cap so all matching horizon events are
+# returned (per-view; week/day/recent keep the normal display cap).
+_MONTH_VIEW_CAP = 500
+
 
 def _check_key(api_key: Optional[str]) -> Optional[JSONResponse]:
     """Return a 401 response if the API key is invalid, else None."""
@@ -128,6 +134,18 @@ async def catalyst_events(
         description="End date YYYY-MM-DD. Overrides mode default window.",
         alias="to",
     ),
+    view: Optional[str] = Query(
+        default=None,
+        description=(
+            "Requested window for horizon tabs (economic_releases): "
+            "'recent' | 'day' | 'week' | 'month'. Anchored at `date` (ET). "
+            "Ignored for non-horizon snapshot tabs."
+        ),
+    ),
+    date: Optional[str] = Query(
+        default=None,
+        description="Anchor date YYYY-MM-DD for the requested view window (ET).",
+    ),
     symbols: Optional[str] = Query(
         default=None,
         description="Comma-separated symbol list for explicit filtering.",
@@ -195,7 +213,21 @@ async def catalyst_events(
     # exclusively from the persistent weekly snapshot. They MUST NOT trigger
     # a live FMP fetch on request, regardless of mode. Earnings is excluded.
     if tab in _SNAPSHOT_TABS:
-        snap = _get_snapshot(tab)
+        # For horizon tabs (economic_releases) a requested window (view/date/
+        # from/to) selects the matching day/week/month slice from the persisted
+        # rolling `events` horizon, with truthful coverage metadata. Without a
+        # requested window the existing full envelope is served unchanged.
+        window_requested = bool(view or date or from_date or to_date)
+        if window_requested and tab in _HORIZON_TABS:
+            snap = _get_snapshot_window(
+                tab,
+                view=view,
+                date=date,
+                from_date=from_date,
+                to_date=to_date,
+            )
+        else:
+            snap = _get_snapshot(tab)
         # Display-layer curation. Raw Neon storage is unchanged; this only
         # trims/dedupes/re-ranks the response payload. Uses already-cached
         # event fields only — no FMP, no profile enrichment, no DB lookups
@@ -214,6 +246,21 @@ async def catalyst_events(
             snap = _curate_envelope(
                 tab, snap, cap=_CURATION_CAP, watchlist=wl, portfolio=pf,
             )
+            if window_requested and tab in _HORIZON_TABS:
+                # Curate the selected window the same way current_week is
+                # curated (hard filter, dedup, family grouping, scoring).
+                # Month keeps a larger cap so it returns all matching events.
+                from services.calendar_curation import curate_events as _curate_events
+                win_cap = (
+                    _MONTH_VIEW_CAP
+                    if (view or "").strip().lower() == "month"
+                    else _CURATION_CAP
+                )
+                snap["events"] = _curate_events(
+                    tab, snap.get("events") or [],
+                    cap=win_cap, watchlist=wl, portfolio=pf,
+                )
+                snap["event_count"] = len(snap["events"])
 
         # If the snapshot is stale (wrong week), trigger a background refresh
         # so the NEXT request gets current data. This handles restarts that
@@ -227,9 +274,26 @@ async def catalyst_events(
             except Exception as _rte:
                 print(f"[catalyst] request-time stale refresh error tab={tab}: {_rte}")
 
+        # Additive window metadata (horizon tabs, requested-window path only).
+        _window_fields = (
+            {
+                "view":              snap.get("view"),
+                "requested_date":    snap.get("requested_date"),
+                "window_start":      snap.get("window_start"),
+                "window_end":        snap.get("window_end"),
+                "event_count":       snap.get("event_count"),
+                "coverage_complete": snap.get("coverage_complete"),
+                "horizon_start":     snap.get("horizon_start"),
+                "horizon_end":       snap.get("horizon_end"),
+                "empty_reason":      snap.get("empty_reason"),
+            }
+            if window_requested and tab in _HORIZON_TABS else {}
+        )
+
         return JSONResponse(content={
             "tab":           tab,
             "mode":          mode,
+            **_window_fields,
             "current_week":  snap["current_week"],
             "previous_week": snap["previous_week"],
             "last_updated":  snap["last_updated"],
diff --git a/backend/services/calendar_snapshot_service.py b/backend/services/calendar_snapshot_service.py
index ba6318f6..02812e2d 100644
--- a/backend/services/calendar_snapshot_service.py
+++ b/backend/services/calendar_snapshot_service.py
@@ -76,6 +76,10 @@ TARGET_TABS: list[str] = [
 # snapshot window rotates.
 _TABS_WITH_HORIZON: frozenset[str] = frozenset({"economic_releases"})
 
+# Public alias for routes/consumers that need to know which tabs support
+# requested-window serving (day / week / month) from the broad horizon.
+HORIZON_TABS = _TABS_WITH_HORIZON
+
 _HORIZON_PAST_DAYS   = 14
 _HORIZON_FUTURE_DAYS = 89
 
@@ -242,6 +246,11 @@ def get_snapshot(tab: str) -> dict:
         cw = _select_events_for_window(evts, req_from_str, req_to_str)
         prev_from, prev_to = _previous_week_window_for(tab)
         pw = _select_events_for_window(evts, prev_from, prev_to)
+        # The rolling-horizon fetch caps the stored collection; if the derived
+        # previous-week slice is empty, keep the persisted previous_week so the
+        # Recent view does not lose its historical events.
+        if not pw:
+            pw = slot.get("previous_week") or []
 
     # Cache age in hours
     cache_age_hours: Optional[float] = None
@@ -518,6 +527,244 @@ def _select_events_for_window(events: list[dict], from_date: str, to_date: str)
     ]
 
 
+# ── Requested-window read API (horizon tabs) ────────────────────────────────
+
+_HORIZON_VIEWS = ("recent", "day", "week", "month")
+
+
+def _parse_iso_date(s: Optional[str]) -> Optional[date]:
+    """Parse a YYYY-MM-DD anchor. Returns None on any parse error."""
+    if not s:
+        return None
+    try:
+        return date.fromisoformat(s[:10])
+    except (TypeError, ValueError):
+        return None
+
+
+def _resolve_window(
+    view: Optional[str],
+    anchor: Optional[str],
+    from_date: Optional[str],
+    to_date: Optional[str],
+    tab: str,
+) -> tuple[str, str]:
+    """
+    Derive the inclusive [window_start, window_end] (ISO YYYY-MM-DD) for a request.
+
+    An explicit from/to override wins.  Otherwise the `view` is resolved around
+    the ET anchor date (`date`, defaulting to today):
+
+      • day    → the anchor day
+      • week   → Monday–Friday of the anchor week (current/previous/future)
+      • month  → first calendar day through last calendar day of the anchor month
+      • recent → the snapshot's existing previous-week window (current Recent view)
+
+    Unknown views fall back to the Mon–Fri week containing the anchor.  All
+    conventions are America/New_York, matching the rest of the snapshot service.
+    """
+    today = _et_now().date()
+
+    if from_date or to_date:
+        f = _parse_iso_date(from_date) or _parse_iso_date(anchor) or today
+        t = _parse_iso_date(to_date) or f
+        if t < f:
+            f, t = t, f
+        return f.isoformat(), t.isoformat()
+
+    v = (view or "week").strip().lower()
+    if v == "recent":
+        return _previous_week_window_for(tab)
+
+    anchor_date = _parse_iso_date(anchor) or today
+
+    if v == "day":
+        return anchor_date.isoformat(), anchor_date.isoformat()
+
+    if v == "month":
+        first = anchor_date.replace(day=1)
+        if first.month == 12:
+            nxt = date(first.year + 1, 1, 1)
+        else:
+            nxt = date(first.year, first.month + 1, 1)
+        return first.isoformat(), (nxt - timedelta(days=1)).isoformat()
+
+    # week (default)
+    monday = anchor_date - timedelta(days=anchor_date.weekday())
+    friday = monday + timedelta(days=4)
+    return monday.isoformat(), friday.isoformat()
+
+
+def _window_horizon_bounds(
+    env: dict, broad: list[dict],
+) -> tuple[Optional[str], Optional[str]]:
+    """
+    Return the stored horizon [start, end] used for coverage reporting.
+    Prefers the horizon meta fields; falls back to the stored window and to the
+    broad events' actual date span so old rows remain truthful.
+    """
+    horizon = env.get("horizon") or {}
+    stored_window = env.get("window") or {}
+    h_start = horizon.get("horizon_start") or stored_window.get("stored_from")
+    h_end = horizon.get("horizon_end") or stored_window.get("stored_to")
+    if broad:
+        dates = sorted(
+            (e.get("date") or "")[:10] for e in broad
+            if (e.get("date") or "")[:10]
+        )
+        if dates:
+            h_start = h_start or dates[0]
+            h_end = h_end or dates[-1]
+    return (h_start or None), (h_end or None)
+
+
+def _actual_bounds(events: list[dict]) -> tuple[Optional[str], Optional[str]]:
+    """
+    Return the [min_date, max_date] span actually covered by the persisted
+    events. This is the ground-truth horizon used for coverage: the meta
+    horizon_start/horizon_end describe the intended fetch window, while the
+    persisted collection may be narrower (e.g. capped fetches).
+    """
+    dates = sorted(
+        (e.get("date") or "")[:10] for e in events
+        if (e.get("date") or "")[:10]
+    )
+    if not dates:
+        return None, None
+    return dates[0], dates[-1]
+
+
+def _window_covered(
+    horizon_start: Optional[str],
+    horizon_end: Optional[str],
+    win_from: str,
+    win_to: str,
+) -> bool:
+    """True when the stored horizon covers [win_from, win_to] inclusive."""
+    if not horizon_end:
+        return False
+    if horizon_start and win_from < horizon_start:
+        return False
+    return win_to <= horizon_end
+
+
+def _window_empty_reason(
+    coverage_complete: bool,
+    selected: list[dict],
+    source: str,
+) -> Optional[str]:
+    """
+    Truthful empty-state classification for a requested window.
+
+      • events present                     → None
+      • snapshot has no events at all      → "snapshot_empty"
+      • window outside cached horizon      → "outside_horizon"
+      • old snapshot without broad events  → "legacy_snapshot_without_horizon"
+      • covered window with no events      → "no_events_in_window"
+    """
+    if selected:
+        return None
+    if source == "none":
+        return "snapshot_empty"
+    if not coverage_complete:
+        if source == "legacy":
+            return "legacy_snapshot_without_horizon"
+        return "outside_horizon"
+    return "no_events_in_window"
+
+
+def get_snapshot_window(
+    tab: str,
+    view: Optional[str] = None,
+    date: Optional[str] = None,
+    from_date: Optional[str] = None,
+    to_date: Optional[str] = None,
+) -> dict:
+    """
+    Serve a requested day/week/month window from the persisted rolling horizon.
+
+    Additive to get_snapshot(): every existing envelope field is preserved, and
+    `events` carries the SELECTED window (never the full rolling horizon), so a
+    Week/Day response does not include all ~90 days.  Window and coverage
+    metadata describe the requested window truthfully.
+
+    Selection:
+      • Prefers the canonical broad `events` collection when present.
+      • Falls back to legacy `current_week` / `previous_week` for old snapshots
+        that predate the rolling horizon.
+      • No provider calls. No fabricated data. Windows outside the stored
+        horizon report incomplete coverage truthfully.
+    """
+    env = get_snapshot(tab)
+
+    broad = env.get("events") or []
+    legacy = (env.get("current_week") or []) + (env.get("previous_week") or [])
+
+    win_from, win_to = _resolve_window(view, date, from_date, to_date, tab)
+    v = (view or "week").strip().lower()
+
+    if v == "recent":
+        # Recent preserves the existing previous-week semantics exactly.  It
+        # returns the envelope's previous_week slice directly (derived from the
+        # broad horizon when possible, else the persisted previous_week) so the
+        # historical display is unchanged even when the broad collection is
+        # capped.  Window metadata reflects the actual span of that data.
+        source = "previous_week"
+        pool = env.get("previous_week") or []
+        selected = list(pool)
+        if pool:
+            coverage_complete = True
+            empty_reason = None if selected else "no_events_in_window"
+            r_start, r_end = _actual_bounds(pool)
+            if r_start:
+                win_from, win_to = r_start, r_end
+        else:
+            coverage_complete = False
+            empty_reason = "snapshot_empty"
+    else:
+        source = "horizon" if broad else ("legacy" if legacy else "none")
+        pool = broad if broad else legacy
+        selected = _select_events_for_window(pool, win_from, win_to)
+        # Coverage is judged against the ACTUAL persisted event span (ground
+        # truth).  The meta horizon fields describe the intended fetch window;
+        # the persisted collection may be narrower (e.g. capped fetches), and
+        # that narrower span is what truly covers a requested window.
+        actual_start, actual_end = _actual_bounds(pool)
+        coverage_complete = _window_covered(actual_start, actual_end, win_from, win_to)
+        empty_reason = _window_empty_reason(coverage_complete, selected, source)
+
+    horizon_start, horizon_end = _window_horizon_bounds(env, broad)
+
+    out = dict(env)
+    if from_date or to_date:
+        out["view"] = (view or "range").strip().lower()
+    else:
+        out["view"] = (view or "week").strip().lower()
+    out["requested_date"] = date
+    out["window_start"] = win_from
+    out["window_end"] = win_to
+    out["events"] = selected
+    out["event_count"] = len(selected)
+    out["coverage_complete"] = coverage_complete
+    out["horizon_start"] = horizon_start
+    out["horizon_end"] = horizon_end
+    out["empty_reason"] = empty_reason
+
+    coverage = dict(out.get("coverage") or {})
+    coverage["complete"] = coverage_complete
+    coverage["requested_start"] = win_from
+    coverage["requested_end"] = win_to
+    out["coverage"] = coverage
+
+    print(
+        f"[calendar_snapshot] get_snapshot_window tab={tab} view={out['view']} "
+        f"window={win_from}→{win_to} source={source} "
+        f"selected={len(selected)} coverage_complete={coverage_complete} "
+        f"empty_reason={empty_reason} horizon={horizon_start}→{horizon_end}"
+    )
+    return out
+
+
 async def refresh_tab(tab: str, fmp_key: str) -> dict:
     """
     Run the existing FMP fetcher for `tab`, promote current→previous,
diff --git a/backend/services/home_top_catalysts.py b/backend/services/home_top_catalysts.py
index a3baf28a..5c81797e 100644
--- a/backend/services/home_top_catalysts.py
+++ b/backend/services/home_top_catalysts.py
@@ -28,6 +28,7 @@ from services.calendar_curation import (
     group_economic_events_to_families,
     group_events_to_release_packages,
 )
+from services.calendar_snapshot_service import HORIZON_TABS as _HORIZON_TABS
 
 
 # ── Tier ordering (reused from calendar_curation; local copy for independence) ─
@@ -798,8 +799,12 @@ async def build_home_top_catalysts(
                 if stored_horizon_end and (not horizon_end or stored_horizon_end > horizon_end):
                     horizon_end = stored_horizon_end
 
-                # Check coverage: horizon must cover the planning Friday.
-                if stored_horizon_end and stored_horizon_end < week_end:
+                # Check coverage: horizon must cover the planning week.
+                h_start = (horizon.get("horizon_start") or "")
+                if (
+                    (stored_horizon_end and stored_horizon_end < week_end)
+                    or (h_start and h_start > week_start)
+                ):
                     coverage_complete = False
             else:
                 # Fall back to current_week for old snapshots without events.
@@ -811,7 +816,14 @@ async def build_home_top_catalysts(
                     if d and monday <= d <= friday:
                         found_legacy.append(ev)
                 macro_raw.extend(found_legacy)
-                if not found_legacy and _parse_date(stored_horizon_end):
+                if not found_legacy and tab in _HORIZON_TABS:
+                    # A legacy snapshot without a broad horizon only covers the
+                    # current (and previous) week.  A planning window outside
+                    # that span is not covered by cached data — report it so a
+                    # refresh is requested instead of a false empty state.
+                    # Non-horizon tabs (e.g. treasury_macro) are point-in-time
+                    # and never cover a future planning week, so they must not
+                    # flip the coverage flag.
                     coverage_complete = False
 
             stored = (env.get("window") or {})
diff --git a/backend/tests/test_calendar_snapshot.py b/backend/tests/test_calendar_snapshot.py
index bab01cd2..a6d8699d 100644
--- a/backend/tests/test_calendar_snapshot.py
+++ b/backend/tests/test_calendar_snapshot.py
@@ -23,6 +23,8 @@ from services.calendar_snapshot_service import (
     _snapshot_is_stale,
     _empty_slot,
     _normalize_slot,
+    _resolve_window,
+    get_snapshot_window,
 )
 
 
@@ -402,3 +404,434 @@ def test_derive_previous_week_from_broad():
     assert "pw2" in pw_ids
     assert "before" not in pw_ids
     assert "after" not in pw_ids
+
+
+# ═══════════════════════════════════════════════════════════════════════════════
+# Requested-window serving (get_snapshot_window)
+# ═══════════════════════════════════════════════════════════════════════════════
+
+from unittest import mock
+
+
+def _horizon_env(events, horizon_start="2026-07-18", horizon_end="2026-10-29",
+                 status="ready", last_updated="2026-08-01T12:00:00+00:00"):
+    """Fixture envelope returned by get_snapshot for a horizon snapshot."""
+    return {
+        "current_week":  [],
+        "previous_week": [],
+        "last_updated":  last_updated,
+        "status":        status,
+        "is_stale":      False,
+        "window": {
+            "requested_from": "2026-07-27",
+            "requested_to":   "2026-07-31",
+            "stored_from":    horizon_start,
+            "stored_to":      horizon_end,
+        },
+        "diagnostics": {},
+        "events": events,
+        "horizon": {
+            "horizon_start": horizon_start,
+            "horizon_end":   horizon_end,
+            "past_days":     14,
+            "future_days":   89,
+            "event_count":   len(events),
+        },
+        "coverage": {
+            "complete":     True,
+            "horizon_end":  horizon_end,
+            "requested_end": "2026-07-31",
+        },
+    }
+
+
+def _legacy_env(cw, pw, status="ready", stored_from="2026-07-27", stored_to="2026-07-31"):
+    """Fixture envelope returned by get_snapshot for a pre-horizon snapshot."""
+    return {
+        "current_week":  cw,
+        "previous_week": pw,
+        "last_updated":  "2026-07-31T14:24:25+00:00",
+        "status":        status,
+        "is_stale":      False,
+        "window": {
+            "requested_from": "2026-07-27",
+            "requested_to":   "2026-07-31",
+            "stored_from":    stored_from,
+            "stored_to":      stored_to,
+        },
+        "diagnostics": {},
+        "coverage": {
+            "complete":     False,
+            "horizon_end":  stored_to,
+            "requested_end": "2026-07-31",
+        },
+    }
+
+
+def _window(view=None, date=None, from_date=None, to_date=None, env=None):
+    """Call get_snapshot_window with get_snapshot mocked to `env`."""
+    with mock.patch(
+        "services.calendar_snapshot_service.get_snapshot", return_value=env,
+    ) as m:
+        out = get_snapshot_window(
+            "economic_releases", view=view, date=date,
+            from_date=from_date, to_date=to_date,
+        )
+    return out, m
+
+
+# ── Window derivation ───────────────────────────────────────────────────────
+
+def test_resolve_week_monday_friday():
+    assert _resolve_window("week", "2026-08-04", None, None, "economic_releases") == ("2026-08-03", "2026-08-07")
+
+
+def test_resolve_week_monday_itself():
+    assert _resolve_window("week", "2026-08-03", None, None, "economic_releases") == ("2026-08-03", "2026-08-07")
+
+
+def test_resolve_week_friday():
+    assert _resolve_window("week", "2026-08-07", None, None, "economic_releases") == ("2026-08-03", "2026-08-07")
+
+
+def test_resolve_week_previous():
+    assert _resolve_window("week", "2026-07-28", None, None, "economic_releases") == ("2026-07-27", "2026-07-31")
+
+
+def test_resolve_week_future():
+    assert _resolve_window("week", "2026-08-04", None, None, "economic_releases") == ("2026-08-03", "2026-08-07")
+
+
+def test_resolve_day():
+    assert _resolve_window("day", "2026-08-05", None, None, "economic_releases") == ("2026-08-05", "2026-08-05")
+
+
+def test_resolve_month_august():
+    assert _resolve_window("month", "2026-08-15", None, None, "economic_releases") == ("2026-08-01", "2026-08-31")
+
+
+def test_resolve_month_january_boundary():
+    assert _resolve_window("month", "2026-01-10", None, None, "economic_releases") == ("2026-01-01", "2026-01-31")
+
+
+def test_resolve_month_december_boundary():
+    assert _resolve_window("month", "2026-12-10", None, None, "economic_releases") == ("2026-12-01", "2026-12-31")
+
+
+def test_resolve_range_override():
+    assert _resolve_window("week", "2026-08-04", "2026-08-10", None, "economic_releases") == ("2026-08-10", "2026-08-10")
+    assert _resolve_window(None, None, "2026-08-03", "2026-08-07", "economic_releases") == ("2026-08-03", "2026-08-07")
+
+
+def test_resolve_range_swaps_inverted():
+    assert _resolve_window(None, None, "2026-08-07", "2026-08-03", "economic_releases") == ("2026-08-03", "2026-08-07")
+
+
+def test_resolve_recent_matches_previous_week_window():
+    from services.calendar_snapshot_service import _previous_week_window_for
+    assert _resolve_window("recent", None, None, None, "economic_releases") == _previous_week_window_for("economic_releases")
+
+
+def test_resolve_invalid_date_falls_back_to_week():
+    win = _resolve_window("week", "not-a-date", None, None, "economic_releases")
+    assert len(win[0]) == 10 and len(win[1]) == 10
+
+
+# ── Day view ────────────────────────────────────────────────────────────────
+
+def test_day_selects_requested_date_from_broad_events():
+    events = [
+        _make_econ("2026-08-04", id="d1"),
+        _make_econ("2026-08-05", id="d2"),
+        _make_econ("2026-08-06", id="d3"),
+    ]
+    out, _ = _window(view="day", date="2026-08-05", env=_horizon_env(events))
+    assert out["window_start"] == "2026-08-05"
+    assert out["window_end"] == "2026-08-05"
+    assert [e["id"] for e in out["events"]] == ["d2"]
+    assert out["event_count"] == 1
+    assert out["coverage_complete"] is True
+    assert out["empty_reason"] is None
+
+
+def test_day_does_not_return_all_90_days():
+    events = [
+        _make_econ(d.strftime("%Y-%m-%d"), id=f"e{i}")
+        for i, d in enumerate([date.fromisoformat("2026-08-05")])
+    ]
+    out, _ = _window(view="day", date="2026-08-05", env=_horizon_env(events))
+    assert out["event_count"] == 1
+
+
+# ── Week view ───────────────────────────────────────────────────────────────
+
+def test_current_week_selects_monday_friday():
+    events = [
+        _make_econ("2026-07-26", id="sun"),
+        _make_econ("2026-07-27", id="mon"),
+        _make_econ("2026-07-29", id="wed"),
+        _make_econ("2026-07-31", id="fri"),
+        _make_econ("2026-08-01", id="sat"),
+    ]
+    out, _ = _window(view="week", date="2026-07-29", env=_horizon_env(events))
+    assert out["window_start"] == "2026-07-27"
+    assert out["window_end"] == "2026-07-31"
+    assert {e["id"] for e in out["events"]} == {"mon", "wed", "fri"}
+
+
+def test_future_week_aug_3_7_selected_from_broad_events():
+    events = [
+        _make_econ("2026-07-31", id="prev"),
+        _make_econ("2026-08-03", id="mon"),
+        _make_econ("2026-08-05", id="wed"),
+        _make_econ("2026-08-07", id="fri"),
+        _make_econ("2026-08-10", id="later"),
+    ]
+    out, _ = _window(view="week", date="2026-08-04", env=_horizon_env(events))
+    assert out["window_start"] == "2026-08-03"
+    assert out["window_end"] == "2026-08-07"
+    assert {e["id"] for e in out["events"]} == {"mon", "wed", "fri"}
+    assert out["coverage_complete"] is True
+    assert out["empty_reason"] is None
+
+
+def test_previous_week_remains_selectable():
+    events = [
+        _make_econ("2026-07-20", id="p1"),
+        _make_econ("2026-07-22", id="p2"),
+        _make_econ("2026-07-24", id="p3"),
+        _make_econ("2026-07-27", id="cw"),
+    ]
+    out, _ = _window(view="week", date="2026-07-22", env=_horizon_env(events))
+    assert out["window_start"] == "2026-07-20"
+    assert out["window_end"] == "2026-07-24"
+    assert {e["id"] for e in out["events"]} == {"p1", "p2", "p3"}
+
+
+def test_week_does_not_return_all_90_days():
+    events = [_make_econ("2026-08-04", id="e")]
+    out, _ = _window(view="week", date="2026-08-04", env=_horizon_env(events))
+    assert out["event_count"] == 1
+    assert len(out["events"]) == 1
+
+
+# ── Month view ──────────────────────────────────────────────────────────────
+
+def test_month_selects_all_august_events_inside_coverage():
+    events = [
+        _make_econ("2026-07-29", id="jul"),
+        _make_econ("2026-08-03", id="a1"),
+        _make_econ("2026-08-05", id="a2"),
+        _make_econ("2026-08-15", id="a3"),
+        _make_econ("2026-08-31", id="a4"),
+        _make_econ("2026-09-01", id="sep"),
+    ]
+    out, _ = _window(view="month", date="2026-08-15", env=_horizon_env(events))
+    assert out["window_start"] == "2026-08-01"
+    assert out["window_end"] == "2026-08-31"
+    assert {e["id"] for e in out["events"]} == {"a1", "a2", "a3", "a4"}
+    assert "jul" not in {e["id"] for e in out["events"]}
+    assert "sep" not in {e["id"] for e in out["events"]}
+
+
+def test_month_not_restricted_to_current_week():
+    """Month reads the broad horizon, not the snapshot's current_week slice."""
+    current_week_only = [_make_econ("2026-07-29", id="cw_only")]
+    events = [_make_econ("2026-08-05", id="aug_ev")]
+    env = _horizon_env(events)
+    env["current_week"] = current_week_only
+    out, _ = _window(view="month", date="2026-08-15", env=env)
+    assert {e["id"] for e in out["events"]} == {"aug_ev"}
+
+
+# ── Signal metadata / dedup ─────────────────────────────────────────────────
+
+def test_selected_events_preserve_signal_metadata():
+    ev = _make_econ("2026-08-05", id="sig", time="08:30:00", actual="3.2",
+                    estimate="3.1", previous="3.0", event_family="cpi",
+                    signal_tier="major", signal_reason="Inflation report",
+                    country="US", source="fmp")
+    out, _ = _window(view="day", date="2026-08-05", env=_horizon_env([ev]))
+    got = out["events"][0]
+    assert got["signal_tier"] == "major"
+    assert got["signal_reason"] == "Inflation report"
+    assert got["importance"] == "high"
+    assert got["actual"] == "3.2"
+    assert got["estimate"] == "3.1"
+    assert got["previous"] == "3.0"
+    assert got["time"] == "08:30:00"
+    assert got["country"] == "US"
+    assert got["source"] == "fmp"
+
+
+def test_exact_dates_and_times_preserved():
+    ev = _make_econ("2026-08-05T08:30:00", id="dt")
+    out, _ = _window(view="day", date="2026-08-05", env=_horizon_env([ev]))
+    assert out["events"][0]["date"] == "2026-08-05T08:30:00"
+
+
+def test_no_duplicate_canonical_events():
+    """Selection never duplicates a canonical event across pools."""
+    dup = _make_econ("2026-08-05", id="same", title="CPI MoM", eventName="CPI MoM")
+    env = _horizon_env([dup])
+    env["current_week"] = [dict(dup)]
+    env["previous_week"] = [dict(dup)]
+    out, _ = _window(view="day", date="2026-08-05", env=env)
+    assert len([e for e in out["events"] if e["id"] == "same"]) == 1
+
+
+# ── Legacy fallback (old snapshots without broad events) ────────────────────
+
+def test_legacy_snapshot_without_broad_events_readable():
+    cw = [_make_econ("2026-07-27", id="c1"), _make_econ("2026-07-29", id="c2")]
+    env = _legacy_env(cw=cw, pw=[])
+    out, _ = _window(view="week", date="2026-07-28", env=env)
+    assert out["event_count"] == 2
+    assert out["status"] == "ready"
+
+
+def test_legacy_current_week_day_selectable():
+    cw = [_make_econ("2026-07-29", id="c1")]
+    out, _ = _window(view="day", date="2026-07-29", env=_legacy_env(cw=cw, pw=[]))
+    assert [e["id"] for e in out["events"]] == ["c1"]
+
+
+def test_legacy_future_week_reports_incomplete_truthfully():
+    cw = [_make_econ("2026-07-29", id="c1")]
+    out, _ = _window(view="week", date="2026-08-04", env=_legacy_env(cw=cw, pw=[]))
+    assert out["event_count"] == 0
+    assert out["coverage_complete"] is False
+    assert out["empty_reason"] == "legacy_snapshot_without_horizon"
+
+
+def test_legacy_recent_selects_previous_week():
+    pw = [_make_econ("2026-07-15", id="p1")]
+    out, _ = _window(view="recent", env=_legacy_env(cw=[], pw=pw))
+    assert {e["id"] for e in out["events"]} == {"p1"}
+
+
+def test_recent_preserves_persisted_previous_week_for_capped_horizon():
+    """Recent uses the envelope previous_week, so a capped horizon cannot empty it."""
+    pw = [_make_econ("2026-07-20", id="hist1")]
+    env = _horizon_env(events=[_make_econ("2026-08-05", id="broad_only")])
+    env["previous_week"] = pw
+    out, _ = _window(view="recent", env=env)
+    assert {e["id"] for e in out["events"]} == {"hist1"}
+    assert out["coverage_complete"] is True
+    assert out["empty_reason"] is None
+
+
+def test_get_snapshot_previous_week_fallback_when_horizon_capped():
+    """
+    get_snapshot keeps the persisted previous_week when the broad horizon's
+    derived previous-week slice is empty (capped fetch). Prevents the Recent
+    view from going empty after a rolling-horizon refresh.
+    """
+    import services.calendar_snapshot_service as _svc
+    from services.calendar_snapshot_service import _et_now
+    today = _et_now().date()
+    monday = today - timedelta(days=today.weekday())
+    hfrom, hto = _horizon_window_for()
+    slot = {
+        "economic_releases": {
+            "current_week": [_make_econ(monday.isoformat(), id="cw")],
+            "previous_week": [_make_econ("2026-07-20", id="hist")],
+            "events": [_make_econ(monday.isoformat(), id="cw"), _make_econ("2026-08-05", id="aug")],
+            "meta": {
+                "window": {"from": hfrom, "to": hto},
+                "horizon_start": hfrom,
+                "horizon_end": hto,
+                "last_updated": "2026-08-01T12:00:00+00:00",
+                "status": "ready",
+            },
+        },
+    }
+    with mock.patch.object(_svc, "_neon_read", return_value=None), \
+         mock.patch.object(_svc, "_read_disk", return_value=slot):
+        env = _svc.get_snapshot("economic_releases")
+    assert any(e["id"] == "hist" for e in env.get("previous_week") or [])
+
+
+# ── Coverage / empty-state semantics ────────────────────────────────────────
+
+def test_window_outside_horizon_incomplete_truthfully():
+    events = [_make_econ("2026-08-05", id="e")]
+    out, _ = _window(view="week", date="2026-12-15", env=_horizon_env(events))
+    assert out["event_count"] == 0
+    assert out["coverage_complete"] is False
+    assert out["empty_reason"] == "outside_horizon"
+
+
+def test_covered_window_no_events_genuine_empty():
+    """A week inside the persisted span but with no scheduled events is empty."""
+    events = [
+        _make_econ("2026-09-01", id="before"),
+        _make_econ("2026-09-30", id="after"),
+    ]
+    out, _ = _window(view="week", date="2026-09-15", env=_horizon_env(events))
+    assert out["event_count"] == 0
+    assert out["coverage_complete"] is True
+    assert out["empty_reason"] == "no_events_in_window"
+
+
+def test_window_inside_meta_horizon_but_beyond_actual_events_is_incomplete():
+    """A capped horizon (meta end beyond actual data) reports incomplete truthfully."""
+    events = [_make_econ("2026-09-03", id="last")]
+    env = _horizon_env(events)  # meta horizon_end stays 2026-10-29
+    out, _ = _window(view="week", date="2026-09-14", env=env)
+    assert out["event_count"] == 0
+    assert out["coverage_complete"] is False
+    assert out["empty_reason"] == "outside_horizon"
+
+
+def test_covered_window_with_events_not_empty():
+    events = [
+        _make_econ("2026-08-03", id="e1"),
+        _make_econ("2026-08-05", id="e2"),
+        _make_econ("2026-08-07", id="e3"),
+    ]
+    out, _ = _window(view="week", date="2026-08-04", env=_horizon_env(events))
+    assert out["event_count"] == 3
+    assert out["empty_reason"] is None
+    assert out["coverage_complete"] is True
+
+
+def test_snapshot_empty_reports_snapshot_empty():
+    env = _legacy_env(cw=[], pw=[])
+    env["status"] = "empty"
+    out, _ = _window(view="week", date="2026-08-04", env=env)
+    assert out["event_count"] == 0
+    assert out["empty_reason"] == "snapshot_empty"
+
+
+# ── Envelope backward compatibility ─────────────────────────────────────────
+
+def test_window_envelope_preserves_existing_fields():
+    events = [_make_econ("2026-08-05", id="e")]
+    out, _ = _window(view="week", date="2026-08-04", env=_horizon_env(events))
+    for k in ("current_week", "previous_week", "last_updated", "status",
+              "is_stale", "window", "diagnostics", "horizon", "coverage"):
+        assert k in out
+
+
+def test_window_envelope_has_narrow_metadata():
+    events = [_make_econ("2026-08-05", id="e")]
+    out, _ = _window(view="week", date="2026-08-04", env=_horizon_env(events))
+    for k in ("view", "requested_date", "window_start", "window_end",
+              "event_count", "coverage_complete", "horizon_start",
+              "horizon_end", "empty_reason"):
+        assert k in out
+    assert out["view"] == "week"
+    assert out["requested_date"] == "2026-08-04"
+    assert out["horizon_start"] == "2026-07-18"
+    assert out["horizon_end"] == "2026-10-29"
+
+
+def test_no_provider_call_when_coverage_complete():
+    events = [_make_econ("2026-08-05", id="e")]
+    with mock.patch(
+        "services.calendar_snapshot_service.get_snapshot", return_value=_horizon_env(events),
+    ) as m:
+        out = get_snapshot_window("economic_releases", view="week", date="2026-08-04")
+    assert out["event_count"] == 1
+    m.assert_called_once_with("economic_releases")
diff --git a/backend/tests/test_home_top_catalysts.py b/backend/tests/test_home_top_catalysts.py
index c988fe77..3dd51f12 100644
--- a/backend/tests/test_home_top_catalysts.py
+++ b/backend/tests/test_home_top_catalysts.py
@@ -32,6 +32,8 @@ from services.home_top_catalysts import (
     _MACRO_CATEGORIES,
     _CATEGORY_BY_ID,
     _TIER_ORDER,
+    _planning_window,
+    build_home_top_catalysts,
 )
 
 
@@ -1246,3 +1248,270 @@ def test_logical_child_counts_not_inflated_by_source_grandchildren():
     assert len(labor_logical) == 1
     assert labor_logical[0].get("release_group") == "employment_report"
     assert labor_logical[0]["event_count"] == 3
+
+
+# ═══════════════════════════════════════════════════════════════════════════════
+# Home next-week planning integration (Sat Aug 1 → Mon Aug 3 – Fri Aug 7)
+# ═══════════════════════════════════════════════════════════════════════════════
+
+import asyncio
+from unittest import mock
+
+
+def _home_snapshot(events, horizon_start="2026-07-18", horizon_end="2026-10-29"):
+    """Fixture get_snapshot envelope for a horizon tab (economic_releases)."""
+    return {
+        "current_week":  [],
+        "previous_week": [],
+        "last_updated":  "2026-08-01T12:00:00+00:00",
+        "status":        "ready",
+        "is_stale":      False,
+        "window": {
+            "requested_from": "2026-07-27",
+            "requested_to":   "2026-07-31",
+            "stored_from":    horizon_start,
+            "stored_to":      horizon_end,
+        },
+        "diagnostics": {},
+        "events": events,
+        "horizon": {
+            "horizon_start": horizon_start,
+            "horizon_end":   horizon_end,
+            "event_count":   len(events),
+        },
+        "coverage": {
+            "complete":     True,
+            "horizon_end":  horizon_end,
+            "requested_end": "2026-07-31",
+        },
+    }
+
+
+def _home_legacy_snapshot(cw, pw, horizon_end=""):
+    """Fixture get_snapshot envelope for a legacy (pre-horizon) snapshot."""
+    return {
+        "current_week":  cw,
+        "previous_week": pw,
+        "last_updated":  "2026-07-31T14:24:25+00:00",
+        "status":        "ready",
+        "is_stale":      False,
+        "window": {
+            "requested_from": "2026-07-27",
+            "requested_to":   "2026-07-31",
+            "stored_from":    "2026-07-27",
+            "stored_to":      "2026-07-31",
+        },
+        "diagnostics": {},
+        "coverage": {
+            "complete":     False,
+            "horizon_end":  "2026-07-31",
+            "requested_end": "2026-07-31",
+        },
+    }
+
+
+def _empty_top_catalysts():
+    return {
+        "tab": "top_catalysts", "mode": "weekly", "week": "2026-07-27/2026-07-31",
+        "days": [], "current_week": [], "previous_week": [],
+        "last_updated": None, "status": "empty",
+    }
+
+
+def _patch_home_sources(econ_env, treasury_env=None):
+    """Patch get_top_catalysts (empty) and get_snapshot per planning tab."""
+    snap_getter = mock.MagicMock()
+    snap_getter.side_effect = lambda tab: {
+        "economic_releases": econ_env,
+        "treasury_macro": treasury_env if treasury_env is not None else _home_legacy_snapshot([], []),
+    }.get(tab, _home_legacy_snapshot([], []))
+    patches = [
+        mock.patch("services.top_catalysts_service.get_top_catalysts", return_value=_empty_top_catalysts()),
+        mock.patch("services.calendar_snapshot_service.get_snapshot", side_effect=snap_getter.side_effect),
+    ]
+    return patches
+
+
+def _run_home(today, econ_env, treasury_env=None):
+    patches = _patch_home_sources(econ_env, treasury_env)
+    for p in patches:
+        p.start()
+    try:
+        return asyncio.run(build_home_top_catalysts(today_override=today))
+    finally:
+        for p in patches:
+            p.stop()
+
+
+# ── Planning window rule ─────────────────────────────────────────────────────
+
+def test_saturday_aug_1_selects_aug_3_7():
+    monday, friday, mode = _planning_window(date(2026, 8, 1))
+    assert mode == "next_week_planning"
+    assert monday.isoformat() == "2026-08-03"
+    assert friday.isoformat() == "2026-08-07"
+
+
+def test_sunday_selects_following_monday_friday():
+    monday, friday, mode = _planning_window(date(2026, 8, 2))
+    assert mode == "next_week_planning"
+    assert monday.isoformat() == "2026-08-03"
+    assert friday.isoformat() == "2026-08-07"
+
+
+def test_weekday_selects_current_week():
+    monday, friday, mode = _planning_window(date(2026, 8, 5))
+    assert mode == "current_week"
+    assert monday.isoformat() == "2026-08-03"
+    assert friday.isoformat() == "2026-08-07"
+
+
+# ── Home reads broad horizon (not current_week) ─────────────────────────────
+
+def test_home_selects_aug_3_7_events_from_broad_horizon():
+    events = [
+        _make_econ(id="nfp", title="Nonfarm Payrolls", eventName="Nonfarm Payrolls",
+                   event_family="payrolls", signal_tier="major",
+                   signal_reason="Monthly payroll release",
+                   country="US", date="2026-08-07", time="08:30:00"),
+        _make_econ(id="cpi", title="CPI MoM", eventName="CPI MoM",
+                   event_family="cpi", signal_tier="major",
+                   signal_reason="Consumer inflation report",
+                   country="US", date="2026-08-05", time="08:30:00"),
+    ]
+    result = _run_home(date(2026, 8, 1), _home_snapshot(events))
+    assert result["window_mode"] == "next_week_planning"
+    assert result["window_start"] == "2026-08-03"
+    assert result["window_end"] == "2026-08-07"
+    assert result["coverage_complete"] is True
+    assert result["empty_reason"] is None
+    assert len(result["catalysts"]) > 0
+
+
+def test_home_does_not_depend_on_current_week_equaling_aug_3_7():
+    """Home selects from the broad horizon even when current_week is another week."""
+    events = [
+        _make_econ(id="cpi", title="CPI MoM", eventName="CPI MoM",
+                   event_family="cpi", signal_tier="major",
+                   signal_reason="Consumer inflation report",
+                   country="US", date="2026-08-05", time="08:30:00"),
+    ]
+    env = _home_snapshot(events)
+    env["current_week"] = [
+        _make_econ(id="jul_only", title="CPI MoM", eventName="CPI MoM",
+                   event_family="cpi", signal_tier="major",
+                   signal_reason="r", country="US", date="2026-07-29"),
+    ]
+    result = _run_home(date(2026, 8, 1), env)
+    assert result["empty_reason"] is None
+    assert result["coverage_complete"] is True
+    assert len(result["catalysts"]) > 0
+    all_ids = [c["id"] for c in result["catalysts"]]
+    assert not any("jul_only" in cid for cid in all_ids)
+
+
+# ── US-only filtering ────────────────────────────────────────────────────────
+
+def test_foreign_events_excluded_from_home_catalysts():
+    events = [
+        _make_econ(id="us_cpi", title="CPI MoM", eventName="CPI MoM",
+                   event_family="cpi", signal_tier="major",
+                   signal_reason="Consumer inflation report",
+                   country="US", date="2026-08-05", time="08:30:00"),
+        _make_econ(id="de_cpi", title="CPI YoY", eventName="CPI YoY",
+                   event_family="cpi", signal_tier="major",
+                   signal_reason="Foreign release",
+                   country="DE", date="2026-08-05", time="08:30:00"),
+    ]
+    result = _run_home(date(2026, 8, 1), _home_snapshot(events))
+    assert result["empty_reason"] is None
+    assert len(result["catalysts"]) > 0
+
+
+# ── Grouping on Home catalysts ───────────────────────────────────────────────
+
+def test_home_family_grouping_intact():
+    events = [
+        _make_econ(id="c1", title="CPI MoM", eventName="CPI MoM",
+                   event_family="cpi", signal_tier="major",
+                   signal_reason="Consumer inflation report",
+                   country="US", date="2026-08-05", time="08:30:00"),
+        _make_econ(id="c2", title="Core CPI MoM", eventName="Core CPI MoM",
+                   event_family="cpi", signal_tier="major",
+                   signal_reason="Consumer inflation report",
+                   country="US", date="2026-08-05", time="08:30:00"),
+    ]
+    result = _run_home(date(2026, 8, 1), _home_snapshot(events))
+    inflation = [c for c in result["catalysts"] if c.get("category") == "inflation"]
+    assert inflation, result["catalysts"]
+    assert inflation[0]["event_count"] == 1  # one logical family card child
+    assert inflation[0]["children"][0]["type"] == "macro_family"
+    assert inflation[0]["children"][0]["event_family"] == "cpi"
+
+
+def test_home_release_package_grouping_intact():
+    events = [
+        _make_econ(id="p1", title="Nonfarm Payrolls", eventName="Nonfarm Payrolls",
+                   event_family="payrolls", signal_tier="major",
+                   signal_reason="Monthly payroll release",
+                   country="US", date="2026-08-07", time="08:30:00"),
+        _make_econ(id="p2", title="Unemployment Rate", eventName="Unemployment Rate",
+                   event_family="unemployment", signal_tier="secondary",
+                   signal_reason="Unemployment rate release",
+                   country="US", date="2026-08-07", time="08:30:00"),
+    ]
+    result = _run_home(date(2026, 8, 1), _home_snapshot(events))
+    labor = [c for c in result["catalysts"] if c.get("category") == "labor"]
+    assert labor, result["catalysts"]
+    pkg = labor[0]["children"][0]
+    assert pkg.get("release_group") == "employment_report"
+    assert pkg["event_count"] == 2
+
+
+def test_home_count_semantics_preserved():
+    events = [
+        _make_econ(id="c1", title="CPI MoM", eventName="CPI MoM",
+                   event_family="cpi", signal_tier="major",
+                   signal_reason="Consumer inflation report",
+                   country="US", date="2026-08-05", time="08:30:00"),
+        _make_econ(id="nfp", title="Nonfarm Payrolls", eventName="Nonfarm Payrolls",
+                   event_family="payrolls", signal_tier="major",
+                   signal_reason="Monthly payroll release",
+                   country="US", date="2026-08-07", time="08:30:00"),
+    ]
+    result = _run_home(date(2026, 8, 1), _home_snapshot(events))
+    assert result["total_source_events"] >= 2
+    assert result["total_grouped_events"] == len(result["catalysts"])
+
+
+# ── Coverage / empty-state / refresh ────────────────────────────────────────
+
+def test_home_legacy_snapshot_incomplete_for_future_week():
+    """A legacy snapshot without broad events cannot cover a future planning week."""
+    cw = [_make_econ(id="jul", title="CPI MoM", eventName="CPI MoM",
+                     event_family="cpi", signal_tier="major",
+                     signal_reason="r", country="US", date="2026-07-29")]
+    econ_env = _home_legacy_snapshot(cw=cw, pw=[])
+    with mock.patch("config.FMP_API_KEY", None):
+        result = _run_home(date(2026, 8, 1), econ_env)
+    assert result["coverage_complete"] is False
+    assert result["empty_reason"] == "snapshot_horizon_incomplete"
+    assert len(result["catalysts"]) == 0
+
+
+def test_home_no_provider_fetch_when_horizon_complete():
+    """No refresh_tab (provider) call is made when the horizon covers planning."""
+    events = [
+        _make_econ(id="cpi", title="CPI MoM", eventName="CPI MoM",
+                   event_family="cpi", signal_tier="major",
+                   signal_reason="Consumer inflation report",
+                   country="US", date="2026-08-05", time="08:30:00"),
+    ]
+    with mock.patch(
+        "services.calendar_snapshot_service.refresh_tab",
+        new=mock.AsyncMock(),
+    ) as rt:
+        result = _run_home(date(2026, 8, 1), _home_snapshot(events))
+    assert rt.call_count == 0
+    assert result["refresh_attempted"] is False
+    assert result["coverage_complete"] is True

```
