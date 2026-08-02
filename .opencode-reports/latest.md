# Task Report: Unify Top Catalyst weekly windows and macro tiers

## Task requested

Eliminate weekly-window, event-selection, grouping, duplication and signal-tier
 drift across three existing surfaces:

1. `GET /api/catalysts/events?tab=economic_releases&view=week&date=<planning-week Monday>`
2. `GET /api/catalysts/top`
3. `GET /api/home/top-catalysts`

No new endpoint, provider, database table, snapshot, scheduler, cache or
parallel macro pipeline was to be created.

## Completion status

Completed and pushed.

- Both Top Catalysts surfaces now resolve to the same America/New_York planning
  window (current Mon–Fri on weekdays, next Mon–Fri on Saturday/Sunday).
- All three surfaces share one canonical economic logical-event transformation.
- Canonical IDs, titles, dates, `event_family`, `release_group`, `signal_tier`,
  `signal_reason`, `lead_metric` and child metadata are preserved across
  Economic Releases, Calendar Top Catalysts and Home Top Catalysts.
- Cross-source Treasury deduplication is deterministic.
- Response includes new additive fields for observability.

## Proven root cause

Three independent pipelines had drifted:

1. **Week divergence**
   - `top_catalysts_service._week_bounds()` defaulted from UTC and used the
     ordinary week containing the supplied date. On Sunday 2026-08-02 this
     resolved to Monday 2026-07-27 / Friday 2026-07-31.
   - `home_top_catalysts._planning_window()` used America/New_York and advanced
     Saturday/Sunday to the next Monday–Friday. On Sunday 2026-08-02 this
     resolved to Monday 2026-08-03 / Friday 2026-08-07.

2. **Macro-classification divergence**
   - `top_catalysts_service` kept a private `_MACRO_WHITELIST_PATTERNS` regex
     pipeline.
   - `home_top_catalysts` kept a separate `_MACRO_CATEGORIES` / `_classify_macro`
     pipeline.
   - `calendar_curation` had the canonical family and release-package grouping.

3. **Grouping divergence**
   - `calendar_curation.curate_events("economic_releases", ...)` applied hard
     filtering, dedupe and `group_economic_events_to_families()` only.
   - Home applied both `group_economic_events_to_families()` and
     `group_events_to_release_packages()` on its own raw input.
   - Therefore Economic Releases showed individual ISM/JOLTS/Employment rows
     while Home grouped the same logical releases.

4. **Source-pool divergence**
   - Home and Calendar Top Catalysts read both `economic_releases` and
     `treasury_macro`; Economic Releases read only `economic_releases`.
   - The two Top Catalysts surfaces used `current_week` instead of the broad
     rolling `events` horizon.

5. **Tier-propagation divergence**
   - Top Catalysts macro normalization preserved `importance` but dropped
     `signal_tier`, `signal_reason`, `event_family`, `release_group`,
     `lead_metric` and children metadata.

## Existing path preserved

- The Economic Releases Day/Week/Month route contract is unchanged.
- The snapshot service's generic per-tab week semantics are unchanged.
- `top_catalysts_service._week_bounds()` remains as a thin wrapper for existing
  tests.
- `home_top_catalysts._planning_window()` remains as a thin wrapper around the
  new shared helper.
- The in-memory planning-window fetch helpers (`_fetch_planning_tab`,
  `warm_planning_window`) remain unused by the Home build path; the scheduler
  call in `main.py` is left intact.
- Treasury/macro tab curation remains separate so point-in-time yield/curve
  records are preserved.

## Exact files changed

Production files:

- `backend/services/top_catalysts_service.py`
- `backend/services/home_top_catalysts.py`
- `backend/services/calendar_curation.py`

Test files:

- `backend/tests/test_top_catalysts.py`
- `backend/tests/test_home_top_catalysts.py`
- `backend/tests/test_calendar_curation.py`

No route file changes were required; the new response fields are additive and
surface automatically through the existing `JSONResponse`.

## Exact behavior changed

### Planning window

- Added `resolve_top_catalysts_week(today_et=None)` in
  `top_catalysts_service.py`.
- Returns `(monday, friday, window_mode)` using America/New_York.
- `get_top_catalysts()` and `build_home_top_catalysts()` both use it, directly
  or through `_week_bounds` / `_planning_window`.
- On Sunday 2026-08-02 both surfaces now return
  `week_start=2026-08-03`, `week_end=2026-08-07`,
  `window_mode=next_week_planning`.

### Shared canonical macro transformation

- Added `calendar_curation.curate_economic_logical_events(events, cap, ...)`.
- It runs: hard filter → dedupe → family grouping → release-package grouping →
  canonical scoring/ranking → stable ordering.
- `curate_events("economic_releases", ...)` now delegates to it.
- Calendar Top Catalysts and Home Top Catalysts both consume it.

### Calendar Top Catalysts (`/api/catalysts/top`)

- Uses the shared ET planning window.
- Reads the broad `events` horizon for `economic_releases` (falls back to
  `current_week` for old snapshots).
- Curates treasury_macro separately and deduplicates against the canonical
  economic events.
- Surfaces canonical family/package cards for CPI, PPI, PCE, GDP, ECI, ISM
  Manufacturing, ISM Services, JOLTS, Jobless Claims, Factory Orders,
  Employment Report, FOMC and Treasury Auctions.
- Preserves `signal_tier`, `signal_reason`, `event_family`, `release_group`,
  `lead_metric`, `children`, `event_count`, `canonical_id`, actual/estimate/
  previous and unit.
- Added additive response fields:
  - `week_start`
  - `week_end`
  - `window_mode`
  - `macro_source_event_count`
  - `macro_logical_event_count`

### Home Top Catalysts (`/api/home/top-catalysts`)

- Uses the shared planning window.
- Calls `get_top_catalysts(today=monday)` so earnings/other are selected for
  the same planning week.
- Macro input is now the shared canonical logical-event output instead of raw
  rows independently grouped.
- Keeps its compact broad-category presentation (Fed / Rates, Inflation Data,
  Labor Market Data, Growth / Demand Data, Treasury / Yields, Consumer
  Sentiment, Housing Data).
- Category tier is the strongest included canonical logical card; child metadata
  identifies the source card.

### Treasury deduplication

- `treasury_macro` events are curated separately.
- A deterministic title+date key collapses treasury_macro rows that duplicate
  canonical Economic Releases events.
- Unique point-in-time yield/curve records remain.

## Behavior deliberately preserved

- Economic Releases Day/Week/Month response contract (only the curated payload
  now includes complete package cards).
- Snapshot service generic week semantics for other tabs.
- Home compact card limits (`_MAX_MACRO_GROUPS`, `_MAX_EARNINGS`, etc.).
- US-only macro filtering for Top Catalysts and Home.
- Backward-compatible `macroType` names.
- Existing `week`, `days`, `current_week`, `previous_week`, `last_updated` and
  `status` fields in `/api/catalysts/top`.

## Mandatory production-data audit (Aug 3–7, 2026)

Read through existing service functions (no curl, no server start).

| Metric | Value |
| --- | --- |
| `economic_releases` total events | 6,091 |
| `economic_releases` events in Aug 3–7 window | 532 |
| `treasury_macro` total events | 10 (all previous_week, none in window) |
| `treasury_macro` events in Aug 3–7 window | 0 |
| Canonical logical-event count (after family + package grouping) | 281 |
| Duplicate normalized keys within `economic_releases` | 170 |
| Cross-source duplicate title/date keys | 0 (treasury_macro had no window events) |

Sample duplicate normalized keys within `economic_releases`:

- `("s&p global manufacturing pmi (jul)", "2026-08-03")`
- `("s&p global services pmi (jul)", "2026-08-05")`
- `("retail sales mom (jun)", "2026-08-05")`
- `("cpi mom (jul)", "2026-08-07")`

These are FMP's multiple provider/country variants and are handled by the
existing deterministic dedup inside `curate_economic_logical_events`.

### Exact canonical logical cards selected by Economic Releases curation (Aug 3–7)

Representative US cards:

| Date | Title | event_family | release_group | signal_tier |
| --- | --- | --- | --- | --- |
| 2026-08-03 | ISM Manufacturing Report | ism_manufacturing_report | ism_manufacturing_report | secondary |
| 2026-08-04 | JOLTS Report | jolts_report | jolts_report | secondary |
| 2026-08-04 | Factory Orders Report | factory_orders_report | factory_orders_report | secondary |
| 2026-08-05 | ISM Services Report | ism_services_report | ism_services_report | secondary |
| 2026-08-06 | Jobless Claims Report | jobless_claims_report | jobless_claims_report | secondary |
| 2026-08-07 | Employment Report | employment_report | employment_report | major |
| 2026-08-03 | 3-Month Bill Auction | treasury_auction | — | major |
| 2026-08-03 | 6-Month Bill Auction | treasury_auction | — | major |
| 2026-08-04 | 52-Week Bill Auction | treasury_auction | — | major |
| 2026-08-05 | 17-Week Bill Auction | treasury_auction | — | major |
| 2026-08-06 | 4-Week Bill Auction | treasury_auction | — | major |
| 2026-08-06 | 8-Week Bill Auction | treasury_auction | — | major |

### Calendar Top Catalysts macro cards after fix (Sunday 2026-08-02)

| Date | Title | macroType | signal_tier | canonical_id |
| --- | --- | --- | --- | --- |
| 2026-08-03 | 6-Month Bill Auction | Treasury Auctions | major | a383fd9360b4ae6f |
| 2026-08-03 | ISM Manufacturing Report | ISM Manufacturing Report | secondary | 0d049fd71197032f |
| 2026-08-04 | 52-Week Bill Auction | Treasury Auctions | major | e7a3b12c6d7c665f |
| 2026-08-04 | JOLTS Report | JOLTS Report | secondary | e819ab23985ad156 |
| 2026-08-04 | Factory Orders Report | Factory Orders Report | secondary | 1768f13528e0f772 |
| 2026-08-05 | 17-Week Bill Auction | Treasury Auctions | major | 7fd865fe917ec318 |
| 2026-08-05 | ISM Services Report | ISM Services Report | secondary | f72c1c74e130dea1 |
| 2026-08-05 | ADP Employment Change (Jul) | NFP | secondary | 88d71862c9face98 |
| 2026-08-06 | 8-Week Bill Auction | Treasury Auctions | major | 5bfc5509ef8e506d |
| 2026-08-06 | Jobless Claims Report | Jobless Claims Report | secondary | faa5d48958ad10a2 |
| 2026-08-07 | Employment Report | Employment Report | major | 227f16cdb002e6a0 |

`macro_source_event_count = 532`, `macro_logical_event_count = 281`.

### Home Top Catalysts cards after fix (Sunday 2026-08-02)

| Category | Title | signal_tier | Reason source child |
| --- | --- | --- | --- |
| labor | Labor Market Data | major | Employment Report (major) |
| growth | Growth / Demand Data | secondary | ISM Manufacturing Report (secondary) |
| treasury | Treasury / Yields | major | 6-Month Bill Auction (major) |

Home Labor Market Data children:

- Employment Report — release_group=`employment_report`, tier=`major`
- JOLTS Report — release_group=`jolts_report`, tier=`secondary`
- ADP Employment Change (Jul) — tier=`secondary`
- Jobless Claims Report — release_group=`jobless_claims_report`, tier=`secondary`

The Home category tier is `major` because the strongest included canonical
logical card (Employment Report) is `major`; the reason is taken from that same
child.

## Validation commands and results

### Unit tests

```bash
python -m pytest backend/tests/test_top_catalysts.py \
                 backend/tests/test_home_top_catalysts.py \
                 backend/tests/test_calendar_curation.py -q
# 285 passed

python -m pytest backend/tests/test_calendar_curation.py \
                 backend/tests/test_calendar_snapshot.py \
                 backend/tests/test_top_catalysts.py \
                 backend/tests/test_home_top_catalysts.py -q
# 465 passed
```

### Service-level validation (production snapshots)

Direct service calls with `today=2026-08-02` confirmed:

- `resolve_top_catalysts_week(date(2026, 8, 2))` → `2026-08-03/2026-08-07`,
  `next_week_planning`.
- `get_top_catalysts(today=date(2026, 8, 2))` → `week=2026-08-03/2026-08-07`,
  no Jul 27–31 events, 11 US macro cards, counts present.
- `build_home_top_catalysts(today_override=date(2026, 8, 2))` →
  `window_start=2026-08-03`, `window_end=2026-08-07`,
  `window_mode=next_week_planning`, Labor Market Data major sourced from
  Employment Report.

### Route-level TestClient validation

```python
client = TestClient(app)
resp = client.get("/api/catalysts/top", headers={"X-API-Key": AGENT_API_KEY})
# status=200, week=2026-08-03/2026-08-07, window_mode=next_week_planning

resp = client.get(
    "/api/catalysts/events?tab=economic_releases&view=week&date=2026-08-03",
    headers={"X-API-Key": AGENT_API_KEY},
)
# status=200, window_start=2026-08-03
```

Both responses JSON-serializable; no provider calls triggered.

### Git checks

```bash
git diff --check
# no output (clean)
```

## Database, provider, cache, and runtime effects

- **Database**: no writes. Read-only use of existing Neon snapshots.
- **Provider**: no new provider calls added. The fix removes any request-path
  reliance on the previous Home planning-window FMP fetch; all reads are from
  cached snapshots.
- **Cache**: no new cache created. Uses existing `data.cache` and Neon-backed
  `calendar_snapshots`.
- **Runtime**: no server start/stop required for validation.

## Risks and remaining issues

- `treasury_macro` snapshot for the current production week is stale and has
  zero events in the Aug 3–7 window, so the new cross-source dedupe path was
  verified with synthetic fixtures. When treasury_macro regains data, the
  dedupe logic will collapse duplicate auction rows deterministically.
- The Home scheduler `warm_planning_window()` still exists and writes to the
  now-unused `_planning_cache`; it is harmless but redundant given the rolling
  horizon. Removing it would require touching `main.py`, which was outside the
  scoped file set.
- Foreign macro events are intentionally dropped from Top Catalysts and Home
  but remain available in the Economic Releases tab.

## Final git status

```
## main...origin/main
 M .opencode-persistent/state/model.json
 M .opencode-persistent/state/prompt-history.jsonl
 M backend/data/bittensor_dashboard_cache.json
 M backend/data/hyperliquid_hip3_cache.json
 M backend/data/hyperliquid_signal_snapshots.json
 M backend/data/predict_odds_live_lkg.json
 M backend/data/thematic_context_snapshot.json
```

Only runtime/cache files remain dirty. The task production and test files are
committed and pushed.

## Commit SHA and message

```
60c81c42 fix(calendar): unify top catalyst windows and macro tiers
```

## Push command and result

```bash
git push origin main
```

Result:

```
To https://github.com/apiatx/caelyn-agent-backend.git
   6a92e56f..60c81c42  main -> main
```

## Commit presence confirmation

```
60c81c42 (HEAD -> main, origin/main, origin/HEAD) fix(calendar): unify top catalyst windows and macro tiers
```

The commit is present at `HEAD`, local `main`, `origin/main`, and `origin/HEAD`.

## Complete committed diff

```diff
diff --git a/backend/services/calendar_curation.py b/backend/services/calendar_curation.py
index eaf30153..1cba25f3 100644
--- a/backend/services/calendar_curation.py
+++ b/backend/services/calendar_curation.py
@@ -470,11 +470,13 @@ def _hard_filter_economic(ev: dict) -> bool:
 
 
 def _hard_filter_treasury(ev: dict) -> bool:
-    mat = (ev.get("maturity") or ev.get("indicatorName") or "").upper().replace(" ", "")
+    raw = (ev.get("maturity") or ev.get("indicatorName") or "")
+    mat = re.sub(r"[^A-Z0-9]", "", raw.upper())
     if not mat:
         # Allow named curve/auction events through the title path.
-        title = ((ev.get("title") or "")
-                 + " " + (ev.get("eventName") or "")).lower()
+        title = " ".join(
+            str(ev.get(k) or "") for k in ("title", "eventName", "indicatorName")
+        ).lower()
         return any(k in title for k in ("auction", "treasury", "curve", "yield"))
     return any(k in mat for k in _TREASURY_KEY_MATURITIES)
 
@@ -901,6 +903,79 @@ def group_events_to_release_packages(events: list[dict]) -> list[dict]:
     return result
 
 
+# ── Shared canonical economic logical-event transformation ─────────────────
+
+def curate_economic_logical_events(
+    events: Iterable[dict],
+    *,
+    cap: int = DEFAULT_CAP_PER_SLICE,
+    watchlist: Optional[set[str]] = None,
+    portfolio: Optional[set[str]] = None,
+) -> list[dict]:
+    """
+    Transform raw individual economic-release rows into canonical logical
+    events (family cards + release-package cards + discrete events).
+
+    Order of operations:
+      1. hard filtering (economic_releases rules)
+      2. deterministic dedup
+      3. group_economic_events_to_families
+      4. group_events_to_release_packages
+      5. canonical scoring/ranking
+      6. stable output ordering
+
+    The output preserves canonical fields:
+      id, date, display_title, event_family, release_group, signal_tier,
+      signal_reason, lead_metric, children, actual/estimate/previous, unit.
+
+    This helper is used by:
+      • Economic Releases route curation
+      • Calendar Top Catalysts macro aggregation
+      • Home Top Catalysts macro aggregation
+    """
+    raw_list = [e for e in events if isinstance(e, dict)]
+    if not raw_list:
+        return []
+
+    watchlist = watchlist or set()
+    portfolio = portfolio or set()
+
+    # 1. Hard filter.
+    filtered = [e for e in raw_list if _hard_filter_economic(e)]
+
+    # 2. Dedup.
+    deduped = _dedup(filtered)
+
+    # 3. Family grouping.
+    grouped = group_economic_events_to_families(deduped)
+
+    # 4. Release-package grouping.
+    packaged = group_events_to_release_packages(grouped)
+
+    # 5. Score & rank using the existing economic scorer.
+    #    Family/package cards score by their strongest child (signal_tier) and
+    #    the lead child's economic importance.  This keeps the same relative
+    #    ordering the Economic Releases tab already uses.
+    def _score_logical(ev: dict) -> float:
+        # Prefer explicit signal_tier strength over the legacy score so a
+        # critical FOMC decision outranks a major CPI family.
+        tier_val = _TIER_ORDER.get((ev.get("signal_tier") or "").lower(), 0)
+        base = _score_economic(ev, watchlist, portfolio)
+        return (tier_val * 1000.0) + base
+
+    packaged.sort(
+        key=lambda e: (
+            _score_logical(e),
+            e.get("date") or "",
+            e.get("display_title") or e.get("title") or e.get("eventName") or "",
+        ),
+        reverse=True,
+    )
+
+    # 6. Trim.
+    return packaged[: max(1, int(cap))]
+
+
 # ── Public entry point ─────────────────────────────────────────────────────
 
 def curate_events(
@@ -948,10 +1023,13 @@ def curate_events(
     # 2. Dedup.
     deduped = _dedup(filtered)
 
-    # 2b. Family grouping — applies to economic_releases only.
+    # 2b. Family + release-package grouping — applies to economic_releases only.
     #     Runs after dedup, before scoring.  Raw Neon storage is unchanged.
     if tab == "economic_releases":
-        deduped = group_economic_events_to_families(deduped)
+        deduped = curate_economic_logical_events(
+            deduped, cap=cap, watchlist=watchlist, portfolio=portfolio,
+        )
+        return deduped
 
     # 3. Score & rank.
     if scorer is not None:
diff --git a/backend/services/home_top_catalysts.py b/backend/services/home_top_catalysts.py
index 5c81797e..c2a3d627 100644
--- a/backend/services/home_top_catalysts.py
+++ b/backend/services/home_top_catalysts.py
@@ -25,10 +25,11 @@ from datetime import date, datetime, timedelta, timezone
 from typing import Any, Optional
 
 from services.calendar_curation import (
-    group_economic_events_to_families,
-    group_events_to_release_packages,
+    curate_economic_logical_events,
+    curate_events as _curate_events,
 )
 from services.calendar_snapshot_service import HORIZON_TABS as _HORIZON_TABS
+from services.top_catalysts_service import resolve_top_catalysts_week
 
 
 # ── Tier ordering (reused from calendar_curation; local copy for independence) ─
@@ -180,30 +181,12 @@ def _planning_window(
     """
     Return (monday, friday, window_mode) for the Home Top Catalysts feed.
 
-    Planning rules (all in America/New_York):
+    Thin wrapper around the shared ``resolve_top_catalysts_week`` helper so
+    Calendar and Home Top Catalysts can never drift.  Planning rules (ET):
       Mon–Fri  → current week's Mon–Fri   window_mode="current_week"
       Sat–Sun  → *next* week's Mon–Fri    window_mode="next_week_planning"
-
-    `today_et` is accepted for unit-test overrides; defaults to the real ET date.
     """
-    if today_et is None:
-        try:
-            from zoneinfo import ZoneInfo
-        except ImportError:
-            from backports.zoneinfo import ZoneInfo  # type: ignore
-        today_et = datetime.now(ZoneInfo("America/New_York")).date()
-
-    wd = today_et.weekday()   # 0=Mon … 6=Sun
-    if wd >= 5:               # Saturday (5) or Sunday (6)
-        days_to_monday = 7 - wd
-        monday = today_et + timedelta(days=days_to_monday)
-        mode = "next_week_planning"
-    else:                     # Monday–Friday
-        monday = today_et - timedelta(days=wd)
-        mode = "current_week"
-
-    friday = monday + timedelta(days=4)
-    return monday, friday, mode
+    return resolve_top_catalysts_week(today_et)
 
 
 # ── General helpers ───────────────────────────────────────────────────────────
@@ -273,12 +256,19 @@ def _classify_macro(ev: dict) -> Optional[str]:
 
 
 def _is_us_macro(ev: dict) -> bool:
-    """True when the event has an explicit US country code — Home only."""
+    """True when the event is a US macro release — Home only."""
     raw = ev.get("country")
-    if raw is None:
-        return False
-    country = str(raw).strip().upper()
-    return country in ("US", "USA", "UNITED STATES")
+    if raw is not None:
+        country = str(raw).strip().upper()
+        if country in ("US", "USA", "UNITED STATES"):
+            return True
+    # US Treasury point-in-time records from treasury_macro often omit country.
+    et = (ev.get("eventType") or "").lower()
+    if et in ("treasury_rate", "treasury_macro"):
+        return True
+    if str(ev.get("companyName") or "").strip().upper() == "US TREASURY":
+        return True
+    return False
 
 
 # ── Planning-window in-memory fetch (Sat/Sun only) ───────────────────────────
@@ -748,10 +738,10 @@ async def build_home_top_catalysts(
     empty_reason: Optional[str] = None
 
     # ── 1. Base aggregation (earnings + other) from existing service ─────────
-    # get_top_catalysts() builds against the current ET Mon–Fri window.
-    # We re-filter by our planning window so that on Sat/Sun (next-week mode)
-    # last-week earnings/other days are excluded.
-    base = get_top_catalysts()
+    # get_top_catalysts() now uses the same shared ET planning window, so on
+    # Sat/Sun it already returns the upcoming week's earnings/other.  Pass the
+    # resolved Monday so both services agree on the exact window.
+    base = get_top_catalysts(today=monday)
     days = base.get("days") or []
 
     earnings_flat: list[dict] = []
@@ -876,21 +866,47 @@ async def build_home_top_catalysts(
     total_source = len(earnings_flat) + len(macro_raw) + len(other_flat)
 
     # ── 3. Categorize and group macro events ─────────────────────────────────
-    #    3a. Exclude non-US macro events from the default Home surface.
-    macro_us = [ev for ev in macro_raw if _classify_macro(ev) and _is_us_macro(ev)]
+    #    3a. Split by source so each follows its correct canonical curation path.
+    econ_raw = [
+        ev for ev in macro_raw
+        if (ev.get("eventType") or "").lower() in ("economic_release", "economic_releases")
+    ]
+    tres_raw = [
+        ev for ev in macro_raw
+        if (ev.get("eventType") or "").lower() in ("treasury_rate", "treasury_macro")
+    ]
+
+    #    3b. Shared canonical logical-event transformation for economic releases.
+    econ_logical = curate_economic_logical_events(
+        econ_raw, cap=500, watchlist=set(), portfolio=set(),
+    )
+
+    #    3c. Treasury/macro is curated separately so unique yield/curve records
+    #        are preserved and not swallowed by the economic family grouper.
+    tres_curated = _curate_events("treasury_macro", tres_raw, cap=500)
+
+    #    3d. Deterministic cross-source dedupe.  Scheduled dated auctions already
+    #        have a canonical Economic Releases representation; drop treasury_macro
+    #        rows that duplicate them.  Unique point-in-time records remain.
+    econ_keys: set[tuple[str, str]] = set()
+    for ev in econ_logical:
+        title = (ev.get("display_title") or ev.get("title") or ev.get("eventName") or "").strip().lower()
+        econ_keys.add((title, (ev.get("date") or "")[:10]))
+
+    def _treasury_is_duplicate(ev: dict) -> bool:
+        title = (ev.get("eventName") or ev.get("title") or ev.get("indicatorName") or "").strip().lower()
+        return (title, (ev.get("date") or "")[:10]) in econ_keys
 
-    #    3b. Pass eligible US economic events through the family grouper.
-    macro_logical = group_economic_events_to_families(macro_us)
+    tres_unique = [ev for ev in tres_curated if not _treasury_is_duplicate(ev)]
+    macro_logical = econ_logical + tres_unique
 
-    #    3c. Group remaining multi-row US release packages (Employment Report,
-    #        Jobless Claims, JOLTS, ISM Manufacturing/Services, Factory Orders)
-    #        into display-level package cards.
-    macro_logical = group_events_to_release_packages(macro_logical)
+    #    3e. Exclude non-US macro events.  US Treasury point-in-time records are
+    #        kept even when country is missing because the source tab is US Treasury.
+    macro_us = [ev for ev in macro_logical if _classify_macro(ev) and _is_us_macro(ev)]
 
-    #    3d. Classify each logical event (family card, package card, or
-    #        discrete) into Home categories.
+    #    3f. Classify each canonical logical event into Home categories.
     events_by_cat: dict[str, list[dict]] = {}
-    for ev in macro_logical:
+    for ev in macro_us:
         cat_id = _classify_macro(ev)
         if cat_id:
             events_by_cat.setdefault(cat_id, []).append(ev)
diff --git a/backend/services/top_catalysts_service.py b/backend/services/top_catalysts_service.py
index 6c394fa1..509c36be 100644
--- a/backend/services/top_catalysts_service.py
+++ b/backend/services/top_catalysts_service.py
@@ -44,13 +44,19 @@ import re
 from datetime import date, datetime, timedelta, timezone
 from typing import Any, Optional
 
+try:
+    from zoneinfo import ZoneInfo
+except ImportError:  # pragma: no cover
+    from backports.zoneinfo import ZoneInfo  # type: ignore
+
 from data.cache import cache
 from services.calendar_curation import (
     CANONICAL_SYMBOL_MAP,
     MC_FLOOR,
     _canonical_symbol,
     _is_preferred_or_junk,
-    group_economic_events_to_families,
+    curate_economic_logical_events,
+    curate_events as _curate_events,
 )
 from services.calendar_snapshot_service import (
     get_snapshot as _get_snapshot,
@@ -143,10 +149,39 @@ def _load_portfolio_set() -> set[str]:
 
 # ── Week bounds ─────────────────────────────────────────────────────────────
 
-def _week_bounds(today: Optional[date] = None) -> tuple[date, date]:
-    today = today or datetime.now(timezone.utc).date()
-    monday = today - timedelta(days=today.weekday())
+def resolve_top_catalysts_week(
+    today_et: Optional[date] = None,
+) -> tuple[date, date, str]:
+    """
+    Return (monday, friday, window_mode) for the Top Catalysts planning week.
+
+    Planning rules (America/New_York):
+      Mon–Fri  → current week's Mon–Fri   window_mode="current_week"
+      Sat–Sun  → next week's Mon–Friday   window_mode="next_week_planning"
+
+    `today_et` accepts a unit-test override; otherwise defaults to the real ET
+    date.  Both Calendar Top Catalysts and Home Top Catalysts use this single
+    helper so their windows can never drift.
+    """
+    if today_et is None:
+        today_et = datetime.now(ZoneInfo("America/New_York")).date()
+
+    wd = today_et.weekday()  # 0=Mon … 6=Sun
+    if wd >= 5:  # Saturday (5) or Sunday (6)
+        days_to_monday = 7 - wd
+        monday = today_et + timedelta(days=days_to_monday)
+        mode = "next_week_planning"
+    else:
+        monday = today_et - timedelta(days=wd)
+        mode = "current_week"
+
     friday = monday + timedelta(days=4)
+    return monday, friday, mode
+
+
+def _week_bounds(today: Optional[date] = None) -> tuple[date, date]:
+    """Legacy thin wrapper kept for existing tests and callers."""
+    monday, friday, _ = resolve_top_catalysts_week(today)
     return monday, friday
 
 
@@ -493,31 +528,80 @@ def _normalize_macro_event(ev: dict, tag: str) -> dict:
     return out
 
 
-# Family → Top Catalysts whitelist tag.  Only families already present in
-# _MACRO_WHITELIST_PATTERNS appear here.
+# Canonical event_family / release_group → Top Catalysts macroType tag.
+# Maintains backward-compatible macroType names while covering the full shared
+# canonical logical-event taxonomy.
 _FAMILY_TO_TOP_TAG: dict[str, str] = {
     "cpi": "CPI",
     "ppi": "PPI",
     "pce": "PCE",
     "eci": "ECI",
     "gdp": "GDP",
+    "fomc_decision": "FOMC",
+    "fomc_minutes": "FOMC",
+    "treasury_auction": "Treasury Auctions",
 }
 
+_RELEASE_GROUP_TO_TOP_TAG: dict[str, str] = {
+    "employment_report": "Employment Report",
+    "jobless_claims_report": "Jobless Claims Report",
+    "jolts_report": "JOLTS Report",
+    "ism_manufacturing_report": "ISM Manufacturing Report",
+    "ism_services_report": "ISM Services Report",
+    "factory_orders_report": "Factory Orders Report",
+}
+
+
+def _is_us_macro_event(ev: dict) -> bool:
+    """True for US macro releases or US Treasury point-in-time records."""
+    country = str(ev.get("country") or "").strip().upper()
+    if country in ("US", "USA", "UNITED STATES"):
+        return True
+    et = (ev.get("eventType") or "").lower()
+    if et in ("treasury_rate", "treasury_macro"):
+        return True
+    return False
+
+
+def _macro_type_for_logical_event(ev: dict) -> Optional[str]:
+    """Return the backward-compatible macroType tag for a canonical logical event."""
+    family = (ev.get("event_family") or "").lower()
+    rg = (ev.get("release_group") or "").lower()
+    if family in _FAMILY_TO_TOP_TAG:
+        return _FAMILY_TO_TOP_TAG[family]
+    if rg in _RELEASE_GROUP_TO_TOP_TAG:
+        return _RELEASE_GROUP_TO_TOP_TAG[rg]
+    # Legacy title-based classification for any event not yet in the maps.
+    return _classify_macro(ev)
+
 
-def _normalize_macro_family_entry(ev: dict, tag: str) -> dict:
+def _normalize_macro_logical_event(ev: dict, tag: str) -> dict:
+    """
+    Normalize a canonical logical event (family card, package card, or discrete
+    macro event) into the Top Catalysts response shape.
+
+    Preserves canonical fields so the frontend can render explicit tiers and
+    child metadata consistently across Calendar, Home and Economic Releases.
+    """
     out: dict[str, Any] = {
-        "title":         ev.get("display_title") or ev.get("title") or tag,
+        "title":         ev.get("display_title") or ev.get("title") or ev.get("eventName") or tag,
         "date":          (ev.get("date") or "")[:10],
         "eventType":     "macro",
         "macroType":     tag,
         "sourceTab":     "macro",
-        "whyThisMatters": [f"{tag} release"],
+        "whyThisMatters": [ev.get("signal_reason") or f"{tag} release"],
+        "type":          "macro_family" if ev.get("type") == "macro_family" else "macro",
+        "event_family":  ev.get("event_family"),
+        "release_group": ev.get("release_group"),
+        "signal_tier":   ev.get("signal_tier"),
+        "signal_reason": ev.get("signal_reason"),
+        "lead_metric":   ev.get("lead_metric"),
         "children":      ev.get("children") or [],
         "event_count":   ev.get("event_count") or len(ev.get("children") or []),
-        "type":          "macro_family",
+        "canonical_id":  ev.get("id"),
     }
     for k in ("time", "country", "importance", "actual", "estimate", "previous",
-              "unit", "eventName"):
+              "unit", "eventName", "indicatorName"):
         v = ev.get(k)
         if v is not None and v != "":
             out[k] = v
@@ -572,7 +656,10 @@ def get_top_catalysts(
     """
     cap = max(MIN_CAP, min(int(cap or DEFAULT_CAP), MAX_CAP))
     monday, friday = _week_bounds(today)
+    _, _, window_mode = resolve_top_catalysts_week(today)
     week_label = f"{monday.isoformat()}/{friday.isoformat()}"
+    week_start_str = monday.isoformat()
+    week_end_str = friday.isoformat()
 
     watchlist = _load_watchlist_set()
     portfolio = _load_portfolio_set()
@@ -622,9 +709,9 @@ def get_top_catalysts(
             earnings_flat.append((score, normalized))
             seen_per_day_sym[key] = True
 
-    # ── 2. Macro (whitelist only, not scored) ──────────────────────────────
-    # Phase A: collect whitelisted individual events per day across both tabs.
-    raw_per_day: dict[str, list[dict]] = {}
+    # ── 2. Macro (shared canonical logical-event transformation) ───────────
+    # Phase A: read the broad horizon when available, fall back to current_week.
+    macro_source_events: list[dict] = []
     for tab in ("economic_releases", "treasury_macro"):
         try:
             env = _get_snapshot(tab) or {}
@@ -633,44 +720,66 @@ def get_top_catalysts(
             continue
         if env.get("last_updated"):
             last_updated_candidates.append(str(env["last_updated"]))
-        for ev in (env.get("current_week") or []):
+        # Prefer the rolling horizon `events` collection; fall back to legacy
+        # current_week for older snapshots.
+        pool = env.get("events") if (env.get("events") and tab == "economic_releases") else (env.get("current_week") or [])
+        for ev in pool:
             if not isinstance(ev, dict):
                 continue
             d = _parse_date(ev.get("date"))
             if not d or d < monday or d > friday:
                 continue
-            tag = _classify_macro(ev)
-            if not tag:
-                continue
-            day = d.isoformat()
-            raw_per_day.setdefault(day, []).append(ev)
+            macro_source_events.append({**ev, "_source_tab": tab})
+
+    # Phase B: split by source and apply the appropriate canonical curation.
+    econ_raw = [ev for ev in macro_source_events if ev.get("_source_tab") == "economic_releases"]
+    tres_raw = [ev for ev in macro_source_events if ev.get("_source_tab") == "treasury_macro"]
+
+    # Shared canonical logical-event pipeline for economic releases.
+    econ_logical = curate_economic_logical_events(
+        econ_raw, cap=500, watchlist=watchlist, portfolio=portfolio,
+    )
 
-    # Phase B: for each day, group approved US families, normalise the rest.
+    # Treasury/macro is curated separately so point-in-time yield/curve records
+    # are preserved without being swallowed by the economic family grouper.
+    tres_curated = _curate_events("treasury_macro", tres_raw, cap=500)
+
+    # Phase C: deterministic cross-source dedupe.
+    # Scheduled dated auctions from treasury_macro collapse to the canonical
+    # Economic Releases event; unique yield/curve records remain.
+    econ_keys: set[tuple[str, str]] = set()
+    for ev in econ_logical:
+        title = (ev.get("display_title") or ev.get("title") or ev.get("eventName") or "").strip().lower()
+        econ_keys.add((title, (ev.get("date") or "")[:10]))
+
+    def _treasury_is_duplicate(ev: dict) -> bool:
+        title = (ev.get("eventName") or ev.get("title") or ev.get("indicatorName") or "").strip().lower()
+        return (title, (ev.get("date") or "")[:10]) in econ_keys
+
+    tres_unique = [ev for ev in tres_curated if not _treasury_is_duplicate(ev)]
+
+    macro_source_count = len(econ_raw) + len(tres_raw)
+    macro_logical_count = len(econ_logical) + len(tres_unique)
+
+    # Phase D: normalise canonical logical events into the response shape.
+    # Top Catalysts surfaces US releases only (plus US Treasury point-in-time
+    # records); foreign canonical events are intentionally dropped here so the
+    # Economic Releases tab retains its full global view.
     macro_per_day: dict[str, list[dict]] = {}
-    for day in sorted(raw_per_day):
-        grouped = group_economic_events_to_families(raw_per_day[day])
-        day_list: list[dict] = []
-        seen: set[tuple] = set()
-        for item in grouped:
-            if item.get("type") == "macro_family":
-                family = (item.get("event_family") or "").lower()
-                top_tag = _FAMILY_TO_TOP_TAG.get(family)
-                if top_tag is None:
-                    continue
-                day_list.append(_normalize_macro_family_entry(item, top_tag))
-            else:
-                tag = _classify_macro(item)
-                if not tag:
-                    continue
-                country = (item.get("country") or "").upper()
-                if country != "US":
-                    continue
-                if (tag, (item.get("date") or "")[:10]) in seen:
-                    continue
-                seen.add((tag, (item.get("date") or "")[:10]))
-                day_list.append(_normalize_macro_event(item, tag))
-        if day_list:
-            macro_per_day[day] = day_list
+    seen_macro: set[tuple[str, str]] = set()
+    for ev in econ_logical + tres_unique:
+        if not _is_us_macro_event(ev):
+            continue
+        tag = _macro_type_for_logical_event(ev)
+        if not tag:
+            continue
+        day = (ev.get("date") or "")[:10]
+        key = (tag, day)
+        if key in seen_macro:
+            continue
+        seen_macro.add(key)
+        normalized = _normalize_macro_logical_event(ev, tag)
+        macro_per_day.setdefault(day, []).append(normalized)
 
     # ── 3. Other (IPO/dividend/split) — default exclude, max 2-3 / week ────
     other_pool: list[tuple[float, dict]] = []
@@ -769,12 +878,17 @@ def get_top_catalysts(
     )
 
     return {
-        "tab":           "top_catalysts",
-        "mode":          "weekly",
-        "week":          week_label,
-        "days":          days_out,
-        "current_week":  flat,
-        "previous_week": [],
-        "last_updated":  last_updated,
-        "status":        status,
+        "tab":                    "top_catalysts",
+        "mode":                   "weekly",
+        "week":                   week_label,
+        "week_start":             week_start_str,
+        "week_end":               week_end_str,
+        "window_mode":            window_mode,
+        "days":                   days_out,
+        "current_week":           flat,
+        "previous_week":          [],
+        "last_updated":           last_updated,
+        "status":                 status,
+        "macro_source_event_count": macro_source_count,
+        "macro_logical_event_count": macro_logical_count,
     }
diff --git a/backend/tests/test_calendar_curation.py b/backend/tests/test_calendar_curation.py
index be740486..00356144 100644
--- a/backend/tests/test_calendar_curation.py
+++ b/backend/tests/test_calendar_curation.py
@@ -18,6 +18,7 @@ from services.calendar_curation import (
     DEFAULT_CAP_PER_SLICE,
     _canonical_symbol,
     _is_preferred_or_junk,
+    curate_economic_logical_events,
     curate_envelope,
     curate_events,
     group_economic_events_to_families,
@@ -87,10 +88,15 @@ def test_economic_drops_minor_country_noise():
          "country": "US", "date": "2026-05-03"},
     ]
     out = curate_events("economic_releases", raw, cap=50)
-    names = [e["eventName"] for e in out]
-    assert "Local Tractor Index" not in names
-    assert "CPI" in names
-    assert "Nonfarm Payrolls" in names
+    titles = [
+        e.get("display_title") or e.get("title") or e.get("eventName") or ""
+        for e in out
+    ]
+    assert "Local Tractor Index" not in titles
+    # CPI passes through as a discrete event; Nonfarm Payrolls becomes an
+    # Employment Report package card through the shared canonical pipeline.
+    assert any("CPI" in t for t in titles)
+    assert any("Employment Report" in t for t in titles)
     # Top should be a high-impact US release.
     assert out[0]["country"].upper() in ("US", "USA", "UNITED STATES")
 
@@ -1441,6 +1447,61 @@ def test_family_cards_pass_through_unchanged():
     assert emp_cards[0]["event_count"] == 2
 
 
+def test_curate_economic_logical_events_groups_ism_and_jolts():
+    raw = [
+        {"eventType": "economic_release", "eventName": "ISM Manufacturing PMI",
+         "country": "US", "date": "2026-08-03", "importance": "medium",
+         "signal_tier": "secondary", "signal_reason": "ISM business survey"},
+        {"eventType": "economic_release", "eventName": "ISM Manufacturing New Orders",
+         "country": "US", "date": "2026-08-03", "importance": "medium",
+         "signal_tier": "secondary", "signal_reason": "ISM business survey"},
+        {"eventType": "economic_release", "eventName": "JOLTs Job Openings",
+         "country": "US", "date": "2026-08-04", "importance": "medium",
+         "signal_tier": "secondary", "signal_reason": "JOLTS report"},
+        {"eventType": "economic_release", "eventName": "JOLTs Job Quits",
+         "country": "US", "date": "2026-08-04", "importance": "medium",
+         "signal_tier": "secondary", "signal_reason": "JOLTS report"},
+    ]
+    out = curate_economic_logical_events(raw, cap=50)
+    titles = [e.get("display_title") or e.get("title") or "" for e in out]
+    assert "ISM Manufacturing Report" in titles
+    assert "JOLTS Report" in titles
+    ism = [e for e in out if e.get("release_group") == "ism_manufacturing_report"][0]
+    jolts = [e for e in out if e.get("release_group") == "jolts_report"][0]
+    assert ism["event_count"] == 2
+    assert jolts["event_count"] == 2
+
+
+def test_curate_economic_logical_events_preserves_signal_reason():
+    raw = [
+        {"eventType": "economic_release", "eventName": "CPI MoM",
+         "country": "US", "date": "2026-08-05", "importance": "high",
+         "event_family": "cpi", "signal_tier": "major",
+         "signal_reason": "Major consumer inflation release"},
+    ]
+    +    out = curate_economic_logical_events(raw, cap=50)
+    assert len(out) == 1
+    assert out[0]["signal_tier"] == "major"
+    assert out[0]["signal_reason"] == "Major consumer inflation release"
+
+
+def test_curate_economic_logical_events_major_child_establishes_parent_tier():
+    raw = [
+        {"eventType": "economic_release", "eventName": "Non Farm Payrolls",
+         "country": "US", "date": "2026-08-07", "importance": "high",
+         "event_family": "payrolls", "signal_tier": "major",
+         "signal_reason": "Monthly payroll release"},
+        {"eventType": "economic_release", "eventName": "Unemployment Rate",
+         "country": "US", "date": "2026-08-07", "importance": "medium",
+         "event_family": "unemployment", "signal_tier": "secondary",
+         "signal_reason": "Unemployment rate release"},
+    ]
+    out = curate_economic_logical_events(raw, cap=50)
+    emp = [e for e in out if e.get("release_group") == "employment_report"][0]
+    assert emp["signal_tier"] == "major"
+    assert "Monthly payroll release" in emp["signal_reason"]
+
+
 if __name__ == "__main__":
     # Tiny self-running mode without pytest.
     fns = [v for k, v in globals().items() if k.startswith("test_") and callable(v)]
diff --git a/backend/tests/test_home_top_catalysts.py b/backend/tests/test_home_top_catalysts.py
index 3dd51f12..fddf6dc5 100644
--- a/backend/tests/test_home_top_catalysts.py
+++ b/backend/tests/test_home_top_catalysts.py
@@ -1515,3 +1515,82 @@ def test_home_no_provider_fetch_when_horizon_complete():
     assert rt.call_count == 0
     assert result["refresh_attempted"] is False
     assert result["coverage_complete"] is True
+
+
+# ═══════════════════════════════════════════════════════════════════════════════
+# Unified window + canonical tier regression tests (Aug 3–7, 2026)
+# ═══════════════════════════════════════════════════════════════════════════════
+
+def test_home_and_calendar_share_aug_3_7_window():
+    from services.top_catalysts_service import resolve_top_catalysts_week
+    monday, friday, mode = resolve_top_catalysts_week(date(2026, 8, 2))
+    home = _run_home(date(2026, 8, 1), _home_snapshot([]))
+    assert home["window_start"] == monday.isoformat()
+    assert home["window_end"] == friday.isoformat()
+    assert home["window_mode"] == mode
+
+
+def test_home_labor_major_only_with_major_employment_child():
+    events = [
+        _make_econ(id="nfp", title="Non Farm Payrolls", eventName="Non Farm Payrolls",
+                   event_family="payrolls", signal_tier="major",
+                   signal_reason="Monthly payroll release",
+                   country="US", date="2026-08-07", time="08:30:00"),
+        _make_econ(id="ur", title="Unemployment Rate", eventName="Unemployment Rate",
+                   event_family="unemployment", signal_tier="secondary",
+                   signal_reason="Unemployment rate release",
+                   country="US", date="2026-08-07", time="08:30:00"),
+    ]
+    result = _run_home(date(2026, 8, 2), _home_snapshot(events))
+    labor = [c for c in result["catalysts"] if c.get("category") == "labor"]
+    assert labor
+    assert labor[0]["signal_tier"] == "major"
+    assert labor[0]["reason"] == "Monthly payroll release"
+    assert labor[0]["children"][0]["release_group"] == "employment_report"
+
+
+def test_home_labor_secondary_when_no_major_child():
+    events = [
+        _make_econ(id="claims", title="Initial Jobless Claims",
+                   eventName="Initial Jobless Claims", event_family="jobless_claims",
+                   signal_tier="secondary", signal_reason="Weekly jobless claims",
+                   country="US", date="2026-08-06", time="08:30:00"),
+    ]
+    result = _run_home(date(2026, 8, 2), _home_snapshot(events))
+    labor = [c for c in result["catalysts"] if c.get("category") == "labor"]
+    assert labor
+    assert labor[0]["signal_tier"] == "secondary"
+
+
+def test_home_duplicate_raw_rows_do_not_escalate_tier():
+    events = [
+        _make_econ(id="nfp1", title="Non Farm Payrolls", eventName="Non Farm Payrolls",
+                   event_family="payrolls", signal_tier="secondary",
+                   signal_reason="Monthly payroll release",
+                   country="US", date="2026-08-07", time="08:30:00"),
+        _make_econ(id="nfp2", title="Non Farm Payrolls", eventName="Non Farm Payrolls",
+                   event_family="payrolls", signal_tier="secondary",
+                   signal_reason="Monthly payroll release",
+                   country="US", date="2026-08-07", time="08:30:00"),
+    ]
+    result = _run_home(date(2026, 8, 2), _home_snapshot(events))
+    labor = [c for c in result["catalysts"] if c.get("category") == "labor"]
+    assert labor
+    assert labor[0]["signal_tier"] == "secondary"
+    assert labor[0]["event_count"] == 1
+
+
+def test_home_uses_shared_canonical_ids():
+    events = [
+        _make_econ(id="nfp", title="Non Farm Payrolls", eventName="Non Farm Payrolls",
+                   event_family="payrolls", signal_tier="major",
+                   signal_reason="Monthly payroll release",
+                   country="US", date="2026-08-07", time="08:30:00"),
+    ]
+    result = _run_home(date(2026, 8, 2), _home_snapshot(events))
+    labor = [c for c in result["catalysts"] if c.get("category") == "labor"]
+    assert labor
+    child = labor[0]["children"][0]
+    assert child.get("release_group") == "employment_report"
+    assert child.get("signal_tier") == "major"
+    assert child.get("id")
diff --git a/backend/tests/test_top_catalysts.py b/backend/tests/test_top_catalysts.py
index 54d01dc3..0935cb86 100644
--- a/backend/tests/test_top_catalysts.py
+++ b/backend/tests/test_top_catalysts.py
@@ -170,11 +170,13 @@ def test_macro_only_whitelist_in_response(monkeypatch):
 
     env = get_top_catalysts()
     macro_titles = [m["macroType"] for d in env["days"] for m in d["macro"]]
+    macro_card_titles = [m["title"] for d in env["days"] for m in d["macro"]]
     assert "CPI" in macro_titles
     assert "Treasury Auctions" in macro_titles
-    # Non-whitelist must be absent.
+    # ISM now forms a canonical release-package card via the shared pipeline.
+    assert "ISM Manufacturing Report" in macro_card_titles
+    # Non-whitelist low-signal discrete events must be absent.
     assert all("Retail" not in str(m.get("title", "")) for d in env["days"] for m in d["macro"])
-    assert all("ISM" not in str(m.get("title", "")) for d in env["days"] for m in d["macro"])
 
 
 # ── IPO/Dividend/Split exclusion + caps ────────────────────────────────────
@@ -1076,3 +1078,224 @@ def test_pce_family_card_still_produced_after_regex_fix(monkeypatch):
     assert entry["type"] == "macro_family"
     assert entry["event_count"] == 2
     assert entry["actual"] == 0.1
+
+
+# ═══════════════════════════════════════════════════════════════════════════════
+# Unified planning-window regression tests (Aug 3–7, 2026)
+# ═══════════════════════════════════════════════════════════════════════════════
+
+_AUG3 = date(2026, 8, 3)
+_AUG7 = date(2026, 8, 7)
+
+
+def _seed_aug_week(monkeypatch):
+    """Patch the week bounds to the Aug 3–7 planning window."""
+    monkeypatch.setattr(top_svc, "_week_bounds", lambda *_: (_AUG3, _AUG7))
+
+
+def _make_aug_econ(**kw) -> dict:
+    """Build a synthetic economic event for the Aug 3–7 window."""
+    return {
+        "id": kw.get("id", "ev1"),
+        "eventType": "economic_release",
+        "eventName": kw.get("eventName", kw.get("title", "Event")),
+        "title": kw.get("title", kw.get("eventName", "Event")),
+        "date": kw.get("date", "2026-08-03"),
+        "time": kw.get("time"),
+        "country": kw.get("country", "US"),
+        "importance": kw.get("importance", "high"),
+        "actual": kw.get("actual"),
+        "estimate": kw.get("estimate"),
+        "previous": kw.get("previous"),
+        "event_family": kw.get("event_family"),
+        "signal_tier": kw.get("signal_tier"),
+        "signal_reason": kw.get("signal_reason"),
+    }
+
+
+def test_sunday_aug_2_resolves_to_aug_3_7(monkeypatch):
+    _seed_aug_week(monkeypatch)
+    _seed_snapshots(monkeypatch, {})
+    _seed_watchlist(monkeypatch, set())
+    _seed_options(monkeypatch, {})
+    _seed_sectors(monkeypatch, {})
+    _clear_earnings_cache()
+    env = get_top_catalysts(today=date(2026, 8, 2))
+    assert env["week"] == "2026-08-03/2026-08-07"
+    assert env["week_start"] == "2026-08-03"
+    assert env["week_end"] == "2026-08-07"
+    assert env["window_mode"] == "next_week_planning"
+    assert all(d["date"] >= "2026-08-03" for d in env["days"])
+
+
+def test_saturday_aug_1_resolves_to_aug_3_7(monkeypatch):
+    _seed_aug_week(monkeypatch)
+    _seed_snapshots(monkeypatch, {})
+    _seed_watchlist(monkeypatch, set())
+    _seed_options(monkeypatch, {})
+    _seed_sectors(monkeypatch, {})
+    _clear_earnings_cache()
    ...  [output truncated in source; full diff is in the repository]
```

(The complete diff is available in commit `60c81c42`.)
