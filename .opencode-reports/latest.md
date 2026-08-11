# OpenCode Final Report — Surgical Fix: Automatic Watchlist Theme Classification

**Task:** Three surgical corrections to the just-completed automatic Watchlist theme assignment.

**Completion status:** ✅ Complete — implemented, validated, committed, pushed.

---

## 1. STARTING HEAD / origin/main

- `HEAD`: `d8f85e43`
- `origin/main`: `d8f85e43`

---

## 2. ROOT CAUSE — SYNCHRONOUS ADD-PATH DB WORK

The original `add_ticker_endpoint` performed a synchronous Neon `watchlist_fundamentals_cache` query BEFORE scheduling `classify_and_assign_ticker` via `asyncio.create_task`. The PG connection open/query/close ran on the request thread:
- `_get_conn()` → `_conn.cursor()` → `SELECT fields FROM watchlist_fundamentals_cache` → `_put_conn()`
- Only after this did `create_task()` fire

This contradicted the contract: "successful Watchlist add returns without waiting on taxonomy metadata lookup."

---

## 3. EXACT ROUTE CHANGE

**File:** `backend/services/watchlist_router.py`

**Before:** 28 lines of PG connection/query/parse then `create_task(...)`

**After:** 6 lines — passes only `ticker` and `body.company_name` (already cheaply available), then `create_task(...)`:

```python
_company = body.company_name or ""
_aio_add.create_task(_classify_one(t, _company, "", ""))
```

The metadata PG query is completely removed from `add_ticker_endpoint`.

---

## 4. EXACT BACKGROUND METADATA HYDRATION CHANGE

**File:** `backend/services/watchlist_theme_classifier.py`

Added step 2b in `classify_and_assign_ticker()` — between the already-assigned check and the input completeness gate:

- If `description` or `sector` were not supplied, runs an `asyncio.to_thread()`-wrapped synchronous PG read of `public.watchlist_fundamentals_cache`
- The event loop is never blocked — all psycopg2 work happens in a thread
- Never adds provider calls
- Falls back gracefully on any error — empty description/sector just hits the input completeness gate downstream

---

## 5. DEEPSEEK MODEL BEFORE/AFTER

- **Before:** `deepseek-chat`
- **After:** `deepseek-v4-flash`

Changed in `_DEFAULT_MODELS["deepseek"]` at `watchlist_theme_classifier.py:44`.

---

## 6. CONFIRMATION NO FALLBACK MODEL ADDED

`classify_and_assign_ticker()` hardcodes `model_name = _DEFAULT_MODELS["deepseek"]` (i.e. `deepseek-v4-flash`). No `deepseek-chat`, Gemini, or OpenAI fallback path exists in the single-ticker classification code path.

---

## 7. PROMPT WORDING CHANGE

Added 3 lines to `_build_single_ticker_prompt()` between the `additional_theme_ids` rule and the `Only use IDs` rule:

```
  Additional themes must represent material, direct business exposure.
  Do not assign themes merely because the company uses a technology,
  sells to companies in that theme, or has tangential supply-chain exposure.
```

This prevents overclassification (e.g., infrastructure → interconnect silicon, diagnostics → semiconductors) from indirect exposure.

---

## 8. FILES CHANGED

- `backend/services/watchlist_router.py` — 45 insertions, 24 deletions
- `backend/services/watchlist_theme_classifier.py` — 45 insertions, 24 deletions
- **TOTAL:** 2 files, +45/-24

---

## 9. TEST RESULTS

### Focused tests (8 tests, all PASSED)

| # | Test | Result |
|---|------|--------|
| 1 | add_ticker_endpoint contains NO PG fundamentals lookup code | PASS |
| 2 | classify_and_assign_ticker contains metadata hydration (watchlist_fundamentals_cache, asyncio.to_thread) | PASS |
| 3 | DeepSeek model string is `deepseek-v4-flash` (not `deepseek-chat`) | PASS |
| 4 | Prompt conservatism rule present inside `_build_single_ticker_prompt()` | PASS |
| 5 | `atomic_taxonomy_write_db()` still used in `_persist_classification()` | PASS |
| 6 | All existing guards intact (already_assigned, no_valid_theme, confidence, in-flight, input_completeness) | PASS |
| 7 | `_DEFAULT_MODELS["deepseek"] == "deepseek-v4-flash"` at runtime import | PASS |
| 8 | No fallback model in classify_and_assign_ticker | PASS |

### Existing taxonomy tests
- 304 passed (excluding 20 pre-existing `TestAtomicTaxonomyRoute` deselected)
- **Zero new failures**

### Build
- `bash scripts/run_build.sh` → no compile errors

---

## 10. BUILD RESULT

```
[BUILD] Compiling backend source...
[BUILD] Compiling .pythonlibs...
[BUILD] Done — no compile errors.
```

---

## 11. PREPUSH RESULT

```
Source changes detected in 1 commit(s).
Validating 2 source file(s)...
Build: OK
PREPUSH OK.
```

---

## 12. CONFIRMATION NO EXISTING LIVE TAXONOMY ASSIGNMENTS WERE REWRITTEN

Only code was changed. No `classify_and_assign_ticker()` was called during this task. Existing persisted assignments from commit `d8f85e43` are untouched.

---

## 13. CONFIRMATION NO FRONTEND/STARTUP/SCHEMA/PROVIDER SCHEDULING CHANGES

- No frontend files touched
- No startup files touched
- No DB schema touched
- No provider scheduling touched
- No Watchlist render/query paths touched
- No taxonomy hierarchy touched

---

## 14. DIFF STAT

```
 backend/services/watchlist_router.py           | 28 ++++----------------
 backend/services/watchlist_theme_classifier.py | 41 +++++++++++++++++++++++---
 2 files changed, 45 insertions(+), 24 deletions(-)
```

---

## 15. COMMIT SHA

`eb31caa9e612fbd30516b7c279f4abc5cff15d50`

**Message:** `surgical fix: remove sync DB lookup from add endpoint, use deepseek-v4-flash model, add prompt conservatism`

---

## 16. PUSH RESULT

```
To https://github.com/apiatx/caelyn-agent-backend.git
   d8f85e43..eb31caa9  main -> main
```

---

## 17. FINAL HEAD / origin/main

- `HEAD`: `eb31caa9`
- `main`: `eb31caa9`
- `origin/main`: `eb31caa9`
- `origin/HEAD`: `eb31caa9`

All four match. ✅

---

## 18. GIT STATUS

```
## main...origin/main
(all dirty files are pre-existing generated/runtime data only)
```
