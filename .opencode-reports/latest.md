# Home Macro-Signal Cleanup — Release-Package Grouping

## Task requested

Group remaining multi-row US economic release packages (Employment Report,
Jobless Claims Report, JOLTS Report, ISM Manufacturing Report, ISM Services
Report, Factory Orders Report) and correct Home count semantics.

## Completion status

**COMPLETE** — all 41 required test assertions pass, live endpoint validated.

## Proven root cause

Home displayed 26 raw FMP component rows as direct children across Labor (10)
and Growth (16) categories. The missing display-layer grouping layer caused
each sub-metric (e.g. ISM Manufacturing PMI, ISM Manufacturing Prices, ISM
Manufacturing Employment, ISM Manufacturing New Orders) to appear as a
separate logical event, inflating category noise.

## Existing path preserved

- `group_economic_events_to_families()` CPI/PPI/PCE/GDP/ECI behavior unchanged.
- Canonical `event_family` values in `catalyst_calendar_service.py` untouched.
- Provider ingestion, Neon snapshots, cache layers, schedulers unchanged.
- Calendar and Top Catalysts endpoints unchanged.
- Frontend response envelope contract preserved.

## Exact files changed

1. `backend/services/calendar_curation.py` — +236 lines
2. `backend/services/home_top_catalysts.py` — +32/-5 lines
3. `backend/tests/test_calendar_curation.py` — +338 lines
4. `backend/tests/test_home_top_catalysts.py` — +393 lines

## Exact behavior changed

### New `group_events_to_release_packages()` in calendar_curation.py

- Determines `release_group` per event (display-layer only, not persisted).
- Groups by (release_group, date, time, country=US).
- Builds package cards with type="macro_family" and release_group field.
- Family cards pass through unchanged; discrete events without a release_group
  pass through unchanged.
- 6 release groups: employment_report, jobless_claims_report, jolts_report,
  ism_manufacturing_report, ism_services_report, factory_orders_report.

### Release-group matching rules

| Release Group | Patterns | Lead |
|---|---|---|
| employment_report | Non-Farm Payrolls, Unemployment Rate, Avg Hourly Earnings, etc. | Nonfarm Payrolls |
| jobless_claims_report | Initial/Continuing Jobless Claims, 4-Week Average | Initial Jobless Claims |
| jolts_report | JOLTs Job Openings, JOLTs Quits, JOLTs Hires | JOLTs Job Openings |
| ism_manufacturing_report | ISM Manufacturing PMI/Prices/Employment/New Orders | ISM Manufacturing PMI |
| ism_services_report | ISM Services/Non-Manufacturing PMI/Activity/Orders/Prices/Employment | ISM Services PMI |
| factory_orders_report | Factory Orders MoM, ex Transportation | Factory Orders MoM |

### Home pipeline integration

Pipeline order:
1. US-only filter
2. CPI/PPI/PCE/GDP/ECI family grouping (unchanged)
3. Release-package grouping (new)
4. Category classification
5. Compact category cards

### Count semantics correction

| Field | Before | After |
|---|---|---|
| total_source_events | 532 (raw rows) | 532 (unchanged, diagnostic) |
| total_grouped_events | 3 (rendered cards) | 3 (unchanged) |
| hidden_count | 529 (raw rows - cards) | 1 (omitted logical items) |
| Labor event_count | 10 (raw events) | 4 (logical children) |
| Growth event_count | 16 (raw events) | 3 (logical children) |

### Category classification update

`_classify_macro()` in `home_top_catalysts.py` now includes a fast path for
`release_group` values, plus `event_family` is added to the search bag.

## Behavior deliberately preserved

- FOMC remains discrete (not grouped).
- CPI/PPI/PCE/GDP/ECI family cards unchanged.
- ADP Employment Change remains discrete (not absorbed into Employment Report).
- Foreign events excluded from grouping and Home surface.
- Earnings and other (IPO/split/dividend) cards unchanged.
- Response envelope: view, source, window_start, window_end, window_mode,
  generated_at, catalysts, total_source_events, total_grouped_events,
  hidden_count, last_updated, status all preserved.

## Test results

### calendar_curation.py: 123 passed

- All existing 107 tests pass (no regressions)
- 16 new release-package grouping tests pass

### home_top_catalysts.py: 92 passed

- All existing 68 tests pass (no regressions)
- 24 new Home integration tests pass

### Combined: 258 passed

```bash
pytest -q backend/tests/test_calendar_curation.py backend/tests/test_home_top_catalysts.py backend/tests/test_top_catalysts.py
```

### git diff --check: CLEAN (no whitespace errors)

## Live endpoint validation

| Field | Value |
|---|---|
| HTTP status | 200 |
| Response time | ~9.7s |
| window_start | 2026-08-03 |
| window_end | 2026-08-07 |
| total_source_events | 532 |
| total_grouped_events | 3 |
| hidden_count | 1 |

### Rendered categories

| Category | event_count | Children |
|---|---|---|
| Labor Market Data | 4 | Employment Report, JOLTS Report, ADP Employment Change, Jobless Claims Report |
| Growth / Demand Data | 3 | ISM Manufacturing Report, Factory Orders Report, ISM Services Report |
| Treasury / Yields | 7 | Individual treasury auctions |

### Verification

- **Any raw Payroll/CPI/ISM component as direct child:** False
- **Any foreign child:** False
- **Endpoint completed within timeout:** Yes (no hang/timeout)

## Before/after comparison

| Metric | Before | After |
|---|---|---|
| Labor direct children | 10 (raw events) | 4 (Employment Report[4], JOLTS[2], ADP[1], Jobless Claims[3]) |
| Growth direct children | 16 (raw events) | 3 (ISM Mfg[4], Factory Orders[2], ISM Svc[10]) |
| hidden_count | 529 | 1 |
| Labor subtitle | raw event names | "Includes Employment Report, JOLTS Report, ADP Employment Change (Jul), Jobless Claims Report" |
| Growth subtitle | raw event names | "Includes ISM Manufacturing Report, Factory Orders Report, ISM Services Report" |

## Staged files

```
backend/services/calendar_curation.py
backend/services/home_top_catalysts.py
backend/tests/test_calendar_curation.py
backend/tests/test_home_top_catalysts.py
```

## Final Git status

```
## main...origin/main [ahead 1]
```
(Only authorized task files staged; runtime/data files remain dirty but unstaged)

## Commit

- **SHA:** fd9f6b05
- **Message:** fix(home): group labor and ISM macro release packages

## Confirmation

- Nothing was pushed.
- No runtime files were staged.
- Providers, Neon, endpoints, caches, schedulers, Calendar, Top Catalysts, and
  frontend were untouched.
- Canonical event_family values were not changed.
- Provider ingestion and Neon snapshots were not modified.
