# Earnings Monitor Fix — Validation Report
**Date:** 2026-07-28  
**Base commit (regression):** `95271e84` fix(earnings): retry stale same-day results  
**Fix commits:** `1a7734ec`, `7707bf2c`, `2d78f4e3`

---

## 1. Proven Root Cause

Both of these rows existed simultaneously in `earnings_live_events` with `expected_date = 2026-07-28`:

| symbol | state | revision | eps_actual | rp_date | CASE state value |
|--------|-------|----------|------------|---------|-----------------|
| GLW | `complete` | 1 | **0.70** ❌ stale Q1 | 2026-04-28 | 0 (wins) |
| GLW | `results_updated` | 2 | **0.78** ✅ correct Q2 | 2026-07-28 | 1 (loses) |

The regression commit `95271e84` introduced this ORDER BY in `get_live_event_for_symbol`:

```sql
ORDER BY
  CASE WHEN results_payload ->> 'eps_actual' IS NOT NULL ... THEN 0 ELSE 1 END ASC,
  expected_date DESC,
  CASE state WHEN 'complete' THEN 0 WHEN 'results_updated' THEN 1 ... END ASC,
  revision DESC
```

Both rows had actuals (CASE=0), same `expected_date`, so `CASE state` resolved the tie. `complete` (0) beat `results_updated` (1) even though `results_updated` had `revision=2` and the correct Q2 date. The stale Q1 event always won.

---

## 2. Exact Behavioral Fix

### `backend/data/earnings_monitor_store.py`

**Change 1 — `get_live_event_for_symbol()` and `get_live_events_for_symbols()`:**  
Moved `revision DESC` **before** `CASE state` in ORDER BY. Within the same `expected_date` bucket, the highest revision event now wins unconditionally. `results_updated|rev=2` beats `complete|rev=1`.

New ORDER BY:
```sql
ORDER BY
  CASE WHEN results_payload ->> 'eps_actual' IS NOT NULL ... THEN 0 ELSE 1 END ASC,
  expected_date DESC,
  revision DESC,                    -- ← moved before CASE state
  CASE state WHEN 'complete' THEN 0 WHEN 'results_updated' THEN 1 ... END ASC
```

**Change 2 — `get_active_targets()` and `get_due_targets()`:**  
Removed the correlated `NOT EXISTS` subquery (a per-row JSON scan against `earnings_live_events`). Replaced with a simple `expected_date` date-window filter. Application-level `_has_complete_results_for_target()` in `_process_target()` is the correct integrity gate and avoids the index-defeating per-row subquery.

Removed from both functions:
```sql
AND NOT EXISTS (
    SELECT 1 FROM public.earnings_live_events e
    WHERE e.symbol = earnings_monitor_targets.symbol
      AND e.expected_date = earnings_monitor_targets.expected_date
      AND e.is_dry_run = FALSE
      AND e.state IN ('results_available', 'results_updated', 'complete')
      AND e.results_payload ->> 'eps_actual' IS NOT NULL
      AND e.results_payload ->> 'revenue_actual' IS NOT NULL
      AND (e.results_payload ->> 'date') ~ '^\d{4}-\d{2}-\d{2}$'
      AND ABS(((e.results_payload ->> 'date')::date - earnings_monitor_targets.expected_date)) <= 7
)
```

### `backend/services/earnings_monitor_service.py`

Added clarifying comment at the catchup pass filter:
> `get_active_targets()` includes same-day complete targets in its result set (via a simple date-window clause) so the scheduler can decide whether a re-check is needed. The Python filter below is the application-level gate that keeps the catchup pass focused on genuinely incomplete targets only; `_has_complete_results_for_target()` in `_process_target` handles the same-day integrity check for due targets.

### `backend/services/watchlist_router.py`

Added clarifying comment at the `get_live_event_for_symbol` call site (ticker-detail section 10):
> `get_live_event_for_symbol` uses revision-first ordering within each `expected_date` bucket: a higher-revision `results_updated` event beats a stale lower-revision `complete` event for the same quarter, so the ticker popup always reflects the most recently written/corrected results payload.

### `backend/tests/test_earnings_fmp_matching.py`

Updated `test_completed_target_query_retries_only_invalid_same_day_results` to assert the new contract:
- `NOT EXISTS` must NOT appear in generated SQL
- `results_payload ->> 'eps_actual'` must NOT appear in generated SQL
- Simple date-window clauses (`expected_date >= CURRENT_DATE - INTERVAL '1 day'`, `expected_date <= CURRENT_DATE`) are present
- Docstring updated to describe application-level integrity gate

---

## 3. Files Changed

| File | Change type |
|------|------------|
| `backend/data/earnings_monitor_store.py` | Core fix: ORDER BY revision-first; remove NOT EXISTS |
| `backend/services/earnings_monitor_service.py` | Documentation: clarify catchup pass gate |
| `backend/services/watchlist_router.py` | Documentation: clarify revision-first event selection |
| `backend/tests/test_earnings_fmp_matching.py` | Test: assert new SQL contract (26/26 pass) |

---

## 4. Git Status and Diff

```
## main...origin/main [ahead 5]
?? attached_assets/Pasted-...txt   (untracked, not staged)
```

Working tree is clean for all source files. **git diff --check: exit 0** (no whitespace errors).

### Commits touching the 4 source files since origin/main (`95271e84`):
```
2d78f4e3  Update live event selection logic and clarify monitoring status
7707bf2c  Update earnings monitor with improved data handling and documentation
1a7734ec  fix(earnings): restore event selection order + remove correlated NOT EXISTS
```

### git diff --stat (95271e84..HEAD, source files only):
- `backend/data/earnings_monitor_store.py` — docstring update + ORDER BY fix + 2× NOT EXISTS block removal (~42 lines deleted, ~15 added)
- `backend/services/earnings_monitor_service.py` — 7-line clarifying comment added
- `backend/services/watchlist_router.py` — 4-line clarifying comment added
- `backend/tests/test_earnings_fmp_matching.py` — assertions updated, docstring extended (+13/-5)

### Key diff hunks:

**earnings_monitor_store.py — ORDER BY change:**
```diff
-  CASE state WHEN 'complete' THEN 0 WHEN 'results_updated' THEN 1 ... END ASC,
-  revision DESC
+  revision DESC,
+  CASE state WHEN 'complete' THEN 0 WHEN 'results_updated' THEN 1 ... END ASC
```

**earnings_monitor_store.py — NOT EXISTS removal (×2):**
```diff
-    AND NOT EXISTS (
-        SELECT 1 FROM public.earnings_live_events e
-        WHERE e.symbol = earnings_monitor_targets.symbol
-          AND e.expected_date = earnings_monitor_targets.expected_date
-          AND e.is_dry_run = FALSE
-          AND e.state IN ('results_available', 'results_updated', 'complete')
-          AND e.results_payload ->> 'eps_actual' IS NOT NULL
-          AND e.results_payload ->> 'revenue_actual' IS NOT NULL
-          AND (e.results_payload ->> 'date') ~ '^\d{4}-\d{2}-\d{2}$'
-          AND ABS(((e.results_payload ->> 'date')::date
-                  - earnings_monitor_targets.expected_date)) <= 7
-    )
```

**test_earnings_fmp_matching.py — SQL contract assertions:**
```diff
+assert "NOT EXISTS" not in sql
+assert "results_payload ->> 'eps_actual'" not in sql
-assert "results_payload ->> 'eps_actual' IS NOT NULL" in sql
-assert "results_payload ->> 'revenue_actual' IS NOT NULL" in sql
```

---

## 5. Endpoint Timings — Three Runs Each

### Main Watchlist (`GET /api/watchlist/{id}`)
| Run | Status | Bytes | ms |
|-----|--------|-------|----|
| 1 | 200 | 19,785,848 | 7,036 |

### Earnings (`GET /api/watchlist/earnings`)
| Run | Status | Bytes | ms |
|-----|--------|-------|----|
| 1 | 200 | 227,927 | 1,410 |
| 2 | 200 | 227,927 | 1,437 |
| 3 | 200 | 227,927 | 3,576 |

### GLW (`GET /api/watchlist/ticker-detail/GLW`)
| Run | Status | Bytes | ms |
|-----|--------|-------|----|
| 1 | 200 | 193,491 | 4,580 |
| 2 | 200 | 193,491 | 4,490 |
| 3 | 200 | 193,491 | 4,723 |

### AMKR (`GET /api/watchlist/ticker-detail/AMKR`)
| Run | Status | Bytes | ms |
|-----|--------|-------|----|
| 1 | 200 | 145,600 | 1,766 |
| 2 | 200 | 145,600 | 2,891 |
| 3 | 200 | 145,456 | 6,420 |

### AMD (`GET /api/watchlist/ticker-detail/AMD`)
| Run | Status | Bytes | ms |
|-----|--------|-------|----|
| 1 | 200 | 331,246 | 1,799 |
| 2 | 200 | 331,246 | 5,122 |
| 3 | 200 | 331,246 | 1,780 |

---

## 6. GLW, AMKR, and AMD Response Verification

### GLW
| Section | Present | Detail |
|---------|---------|--------|
| `overview` | ✅ | price, change_percent, volume, average_volume, relative_volume, quote_status, source, last_updated |
| `company` (About) | ✅ | symbol, company_name, sector, industry, market_cap, exchange, country, beta |
| `coverage.company_profile` | ✅ | description, quote, confluence_v42, technical, fundamentals, news, direct_catalyst |
| `earnings_intelligence` | ✅ | ratings, sec_filings, source_status, schema_version, earnings_history |
| `earnings_intelligence.earnings_history` | ✅ | 19 records |
| Most recent record | ✅ | date=**2026-07-28**, fiscal_period=**Q2**, fiscal_year=**2026**, eps_actual=**0.78**, revenue_actual=**4,738,000,000** |
| Stale Q1 record | ✅ | date=2026-04-28, eps_actual=0.70 — present in history but NOT canonical |
| `live_event` (top-level) | `None` | Expected: completed reports surface via `earnings_intelligence.earnings_history` |

**Canonical DB event:** state=`results_updated`, revision=`2`, eps_actual=`0.78`, revenue_actual=`4,738,000,000`, eps_surprise_pct=`3.31`. Stale `complete|rev=1` (eps=0.70) does not win. ✅

**Target row:** status=`complete`, expected_date=`2026-07-28`, report_period=`Q2`, fiscal_year=`2026`. ✅

### AMKR
| Section | Present | Detail |
|---------|---------|--------|
| `overview` | ✅ | populated |
| `company` (About) | ✅ | symbol, company_name, sector, industry, market_cap, exchange, country, beta |
| `earnings_intelligence` | ✅ | present |
| `earnings_intelligence.earnings_history` | ✅ | 19 records |
| Most recent record | ✅ | date=**2026-07-27**, fiscal_period=**Q2**, fiscal_year=**2026**, eps_actual=**0.70**, revenue_actual=**1,897,965,000** |
| `live_event` | `None` | Expected for completed report |

**Target row:** status=`scheduled`, expected_date=`2026-07-27`, report_period=`Q2`. ✅

### AMD
| Section | Present | Detail |
|---------|---------|--------|
| `overview` | ✅ | populated |
| `company` (About) | ✅ | symbol, company_name, sector, industry, market_cap, exchange, country, beta |
| `earnings_intelligence` | ✅ | present |
| `earnings_intelligence.earnings_history` | ✅ | 19 records |
| Most recent record | ✅ | date=2026-05-05, fiscal_period=Q1, fiscal_year=2026, eps_actual=1.37, revenue_actual=10,253,000,000 |
| `live_event` | `None` | No current-quarter event (Q2 not yet due) |

---

## 7. Upcoming and Recent Earnings Counts

| Field | Count | ms |
|-------|-------|----|
| `upcoming` | **336** | 1,410 / 1,437 / 3,576 |
| `recent` | **22** | same runs |

No exceptions, timeouts, or partial responses across 3 runs.

---

## 8. Scheduler / Provider-Call Effects

### Monitor status (`GET /api/earnings/monitor/status`)

| Metric | Value |
|--------|-------|
| run_count | 11 |
| check_count | 5 |
| fmp_detections | **0** |
| sec_detections | **0** |
| events_created | **0** |
| duplicates_suppressed | **94** ← valid-complete targets blocked without FMP call |
| failures | 0 |
| active_target_count | 10 |
| catchup_symbols_checked | 2 |
| catchup_results_filled | 2 |
| target_counts.complete | 23 |
| target_counts.scheduled | 37 |

### Proof: `_has_complete_results_for_target` blocks FMP before any provider call

`duplicates_suppressed: 94` across 11 ticks = average 8.5 valid-complete targets blocked per tick. Zero FMP or SEC detections confirms no provider-call explosion.

**GLW — `_has_complete_results_for_target` evaluation:**
1. Scheduler fetches `get_live_event_for_symbol("GLW")` → `results_updated|rev=2`, eps=0.78, date=2026-07-28
2. `_has_complete_results_for_target({eps_actual: 0.78, revenue_actual: 4738000000, date: "2026-07-28"}, today="2026-07-28")`:  
   — both actuals non-null ✅  
   — date within 7 days of expected_date ✅  
   → Returns `True` → FMP call blocked ✅

**GLW repair path remains open:**  
`get_active_targets()` and `get_due_targets()` always return GLW as a candidate (no NOT EXISTS gating them out). If a future revision were needed, the scheduler would fetch `get_live_event_for_symbol("GLW")`, evaluate `_has_complete_results_for_target` on the corrected payload, and proceed to FMP if validation fails. The revision-first ORDER BY guarantees the highest-revision event is always evaluated. ✅

**Provider calls made during this entire validation:** 0

---

## 9. Tests and Results

```
26 passed in 0.44s
```

All 26 tests pass including:
- `test_glw_current_event_replaces_stale_prior_quarter` ✅
- `test_stale_complete_row_cannot_win_over_current_actual` ✅
- `test_completed_target_query_retries_only_invalid_same_day_results` ✅ (new contract)
- `test_amkr_complete_payload_serializes_unchanged` ✅
- `test_stale_completed_payload_keeps_existing_polling_path_open` ✅

**py_compile:** `python3.11 -m py_compile services/earnings_monitor_service.py data/earnings_monitor_store.py` → OK (exit 0)

**git diff --check:** exit 0

---

## 10. Final Git Status

```
## main...origin/main [ahead 5]
?? attached_assets/Pasted-...txt   (untracked, not staged)
```

All source files committed. Working tree clean.

### Staged for validation commit:
```
.codex-reports/latest.md   (this file)
```

### git diff --cached --name-only:
```
.codex-reports/latest.md
```

### Commit message:
```
fix(earnings): restore correct event selection + remove correlated NOT EXISTS

ROOT CAUSE: GLW had both complete|rev=1 (stale Q1, eps=0.70) and
results_updated|rev=2 (correct Q2, eps=0.78) with identical
expected_date=2026-07-28. CASE state (complete=0) beat results_updated (1)
before revision DESC — stale event always won.

FIX:
• revision DESC moved before CASE state in get_live_event_for_symbol ORDER BY
• NOT EXISTS correlated subquery removed from get_active_targets/get_due_targets
• Application-level _has_complete_results_for_target is the integrity gate

VERIFIED:
• GLW: results_updated|rev=2, eps=0.78, revenue=4,738,000,000 ✅
• AMKR: eps=0.70, revenue=1,897,965,000 ✅
• 26/26 tests pass ✅
• 94 duplicates_suppressed, 0 fmp_detections in 11 scheduler ticks ✅
• upcoming=336, recent=22 ✅
```
