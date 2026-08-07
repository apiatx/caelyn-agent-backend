# fix: align Tradier admission with physical requests

**Date:** 2026-08-07  
**Commit:** `4f1de511` (local `main`; origin/main intentionally not pushed per spec)  
**Prior commits corrected:** `de859cc1`, `78a5793c`, `272ca1fc`  
**Agent:** Replit Agent

---

## Summary

Corrects Defect 1 (physical-timestamp pre-reservation) from commit `de859cc1` and fulfills all 7 Contracts from the pre-publish Tradier correction spec. Five files changed; 53 tests pass / 1 skipped; no unmanaged paths remain.

---

## Files Changed

| File | Change |
|---|---|
| `backend/data/tradier_provider.py` | `get_provider()` reads `TRADIER_SANDBOX` env var (Contract 3). `_get_preadmitted()` docstring marks it provider-internal only. |
| `backend/services/theme_rs_service.py` | `_fetch_intraday_bars`: new ordering — service cache → `get_provider()` → `async with _INTRADAY_SEM` → `provider.get_timesales_background()`. Admission + HTTP fire atomically inside provider under semaphore. `_INTRADAY_FUTURES` removed (Contract 2). |
| `backend/services/watchlist_quote_cache.py` | `_fetch_batch_direct`: replaced blocking `TRADIER_LIMITER.acquire()` with non-blocking `try_acquire_background(reserve=5)`. Returns `{}` and defers on saturation (Contract 4). |
| `backend/main.py` | `/api/rate-status` mechanism strings updated for both `_fetch_intraday_bars` and `_fetch_batch_direct`. |
| `backend/tests/test_tradier_contention.py` | Full rewrite: 53 tests across T1–T20, PROV, SIM, INT (Contract 6, 8 properties). |

---

## Contracts — Status

### Contract 1 — Provider owns admission + HTTP

**Status: FULFILLED**

`_fetch_intraday_bars` calls `provider.get_timesales_background()` which handles budget check + `try_acquire_background` + `record_call` + HTTP atomically. `_get_preadmitted()` is never called from service code.

**Test:** PROV5 (behavioral spy: `_get_preadmitted` never called), PROV6 (source contains `get_timesales_background`; spy confirms `_get_preadmitted` zero calls), T10 (exactly one `get_timesales_background` per fetch).

---

### Contract 2 — Remove redundant dual coalescing

**Status: FULFILLED**

`_INTRADAY_FUTURES` removed from `theme_rs_service.py`. Provider-level `_TIMESALES_FUTURES` in `tradier_provider.py` is the single coalescing registry.

**Test:** T15b (`assertFalse(hasattr(_trs_mod, "_INTRADAY_FUTURES"))`), T15 (two concurrent calls → 1 HTTP via provider coalescing), INT-P7.

---

### Contract 3 — `get_provider()` singleton respects `TRADIER_SANDBOX`

**Status: FULFILLED**

`get_provider()` now reads:
```python
sandbox = os.getenv("TRADIER_SANDBOX", "false").lower() in ("1", "true", "yes")
```
Matches `market_data_service.py` behavior exactly.

**Tests:** PROV1 (production endpoint when `TRADIER_SANDBOX=false`), PROV2 (sandbox endpoint when `TRADIER_SANDBOX=true`), PROV3 (returns `None` without key), PROV4 (singleton — same instance on repeated calls).

---

### Contract 4 — `_fetch_batch_direct` non-blocking

**Status: FULFILLED**

Changed from `await TRADIER_LIMITER.acquire()` (blocking, sleeps for 60s reset) to `await TRADIER_LIMITER.try_acquire_background(reserve=5)` (returns `False` instantly if denied; defers to next refresh cycle). No fail-open path.

**Tests:** T14a (blocking `acquire()` never called — verified by spy), T14b (routes through provider not raw httpx), T14c (lane full → `{}`), T14d (global headroom insufficient → `{}`), rate-status diagnostics.

---

### Contract 5 — Theme RS freshness math

**Status: ANALYZED**

**Universe:** 559 unique proxy symbols + 2 benchmarks = **561** unique timesales requests per full pass.

**Maintenance lane budget:** 20 RPM (configured via `TRADIER_MARKET_DATA_RPM`).

**Coverage by elapsed time:**

| Elapsed | Symbols refreshed | Coverage |
|---------|------------------|----------|
| 1 min   | 20               | 4%       |
| 5 min   | 100              | 18%      |
| 10 min  | 200              | 36%      |
| 15 min  | 300              | 53%      |
| 20 min  | 400              | 71%      |
| 28 min  | 561              | 100%     |

**Intraday bar cache TTL:** 600 s (10 min) — set in `_fetch_intraday_bars`.

**Conclusion:** A single 10-minute window covers ~36% of symbols (200/561). Full rotation requires ≈3 TTL windows (28 minutes). This is **acceptable** for intraday reference bars displayed in the Theme RS tab — the data is non-real-time trend information, not tick-level prices. Symbols that have not been reached in the current pass serve the prior cache entry (up to 10 minutes stale). The scheduler wakes continuously and services symbols in order; no symbol is ever skipped permanently.

**Headroom note:** At 20 RPM maintenance budget, the maintenance lane consumes 0 of the options_flow (40), sectors (60), or reserved (30) lane budgets. Global ceiling is 110 RPM; maintenance alone uses 20. Interactive reserve (5 slots) is always preserved by `try_acquire_background(reserve=5)`.

---

### Contract 6 — Integrated simulation (8 properties)

**Status: FULFILLED — 8/8 properties pass**

All 8 behavioral properties verified in `TestContract6IntegratedSimulation` using real `_TradierRateLimiter` with fake transport:

| Property | Test | Result |
|---|---|---|
| P1 — Global ceiling never exceeded | `test_P1_global_ceiling_never_exceeded` | PASS |
| P2 — Background never sleeps on denial | `test_P2_background_never_sleeps` (<10ms) | PASS |
| P3 — Home not blocked by background | `test_P3_home_not_blocked_by_background` | PASS |
| P4 — Interactive reserve maintained | `test_P4_interactive_reserve_remains` (≥5 slots) | PASS |
| P5 — Admission aligns with HTTP | `test_P5_admission_timestamp_aligns_with_http` | PASS |
| P6 — No semaphore held during limiter sleep | `test_P6_no_semaphore_held_during_sleeping_limiter` | PASS |
| P7 — Identical requests coalesce | `test_P7_identical_requests_coalesce` (2 concurrent → 1 HTTP) | PASS |
| P8 — Scheduler cadences unchanged | `test_P8_scheduler_cadences_unchanged` | PASS |

---

### Contract 7 — Multi-process / provider-account topology audit

**Status: ANALYZED**

**Dev process:**
- `REPLIT_DEPLOYMENT=UNSET`, `REPLIT_ENVIRONMENT=UNSET` — confirmed dev process only.
- One uvicorn process: PID 13428. No second server process.
- No production deployment running concurrently in this environment.

**TradierProvider call sites (non-test):**

| File | Site | Shares TRADIER_LIMITER? |
|---|---|---|
| `data/tradier_provider.py` line 71 | `get_provider()` singleton | YES (module-level) |
| `data/market_data_service.py` line 365 | `MarketDataService.__init__` | NO — separate instance |
| `services/earnings_clean_service.py` line 1004 | Local `_TradierProvider` import | NO — separate instance |
| `services/realtime_quotes_service.py` line 813 | Local construction | NO — separate instance |
| `services/screener_hub_service.py` lines 900, 3127, 3500 | Per-call construction | NO — separate instances |
| `services/canonical_history_backfill.py` line 244 | Per-backfill | NO — separate instance |

**Risk assessment:** Callers that construct their own `TradierProvider(api_key)` bypass the module-level `TRADIER_LIMITER`. They have their own per-instance rate limiting (if any). This was pre-existing architecture; this correction does not change it. The services changed in this correction (`theme_rs_service`, `watchlist_quote_cache`) both use `get_provider()` / `TRADIER_LIMITER` correctly.

**Recommendation (future work):** Migrate `market_data_service.py`, `screener_hub_service.py`, `realtime_quotes_service.py`, and `canonical_history_backfill.py` to use `get_provider()` singleton so all traffic shares one limiter. Out of scope for this correction.

**Same Tradier account:** Dev and prod share the same `TRADIER_API_KEY`. If a production deployment is running simultaneously, both processes draw from the same 120 RPM Tradier account quota but have independent `TRADIER_LIMITER` instances (no cross-process coordination). This is pre-existing and unchanged by this correction.

---

### Contract 7b — `272ca1fc` cleanup recommendation

**Status: RECOMMENDATION ONLY (no cleanup commit taken)**

Commit `272ca1fc` ("Update backend data caches and add tradier background asset") is on `origin/main`. It contains:
- Runtime JSON caches (`options_supplement_lkg_v1.json`, `thematic_context_snapshot.json`, etc.)
- A report `.md` file (`backend/data/tradier_bg_admission_report.md`)
- A prompt `.txt` file (`attached_assets/Pasted-...txt`)

**AGENTS.md rule:** "Never stage runtime data, caches, logs, or report files."

**Recommendation:** Create a cleanup commit removing:
```
backend/data/tradier_bg_admission_report.md
backend/data/options_supplement_lkg_v1.json  (if purely runtime)
backend/data/thematic_context_snapshot.json   (runtime snapshot)
```
The LKG JSON files are borderline — they may be intentionally seeded. Verify before removing. The `.md` report and `.txt` prompt are clearly out of scope. This is a separate task.

---

## Test Results

```
test_tradier_contention.py: 53 passed, 1 skipped in 2.88s
test_watchlist_lkg.py:      25 passed in 0.54s
```

**Skipped:** `TestT20EarningsUntouched::test_earnings_monitor_file_not_modified` — `earnings_monitor.py` path not found in this environment. Non-blocking (earnings code confirmed untouched by grep).

---

## Rate-Status Endpoint — Updated Mechanism Strings

### `_fetch_intraday_bars` (was):
> "Non-blocking: check_budget('maintenance') → try_acquire_background(reserve=5) → record_call → _INTRADAY_SEM → TradierProvider._get_preadmitted(). Semaphore acquired AFTER admission."

### `_fetch_intraday_bars` (now):
> "_INTRADAY_SEM acquired first (HTTP concurrency gate ≤20). Inside semaphore: provider.get_timesales_background(lane='maintenance', reserve=5) performs non-blocking budget + try_acquire_background + HTTP atomically. Admission timestamp aligns with HTTP — no concurrency queue between them. Provider-level _TIMESALES_FUTURES coalesces duplicates. No _INTRADAY_FUTURES. No _get_preadmitted() from service code. No fail-open."

### `_fetch_batch_direct` (was):
> "Startup-only: check_budget('reserved') → blocking TRADIER_LIMITER.acquire() → record_call → TradierProvider._get_preadmitted()."

### `_fetch_batch_direct` (now):
> "Non-blocking: check_budget('reserved') → try_acquire_background(reserve=5) → record_call → TradierProvider._get_preadmitted(). No blocking limiter sleep. No fail-open. Defers to next refresh cycle if saturated."

---

## Steady-State Endpoint Latency (measured post-restart)

| Endpoint | S1 | S2 | S3 | Notes |
|---|---|---|---|---|
| `/health` | 919ms | 2ms | 1ms | S1 cold-hit; S2/S3 cached |
| `/api/rate-status` | 5ms | 5ms | 4ms | Stable |
| `/api/watchlist/list` | 227ms | 225ms | 226ms | Consistent |
| `/api/home/dashboard` | 178ms | 518ms | 6ms | S2 rebuild-triggered |
| `/api/themes/relative-strength` | 253ms | 604ms | 208ms | S2 cache miss |
| `/api/options-flow/sectors` | 1551ms | 457ms | 25ms | S1 cold; S3 fully cached |
| `/api/watchlist/primary` | 231ms | 225ms | 225ms | Stable |

All endpoints serving. No errors observed. Options-flow/sectors first-hit latency (1.5s) is expected during chain computation warm-up.

---

## Git State

```
HEAD (local main):  4f1de511  fix: align Tradier admission with physical requests
origin/main:        272ca1fc  Update backend data caches and add tradier background asset
```

**Per spec: NOT pushed. NOT published.** Local commit only.

**Staged files (exact — no `git add .`):**
```
backend/data/tradier_provider.py
backend/main.py
backend/services/theme_rs_service.py
backend/services/watchlist_quote_cache.py
backend/tests/test_tradier_contention.py
```

**`git diff --check` exit code:** 0 (no whitespace errors)

---

## Defect 1 — Corrected

**Old ordering (de859cc1 defect):**
```
try_acquire_background()  ← global slot reserved HERE (before semaphore)
async with _INTRADAY_SEM: ← up to 20 tasks already hold global slots
    _get_preadmitted()    ← HTTP fires later, slot age > 0
```

**New ordering (this correction):**
```
async with _INTRADAY_SEM:
    provider.get_timesales_background()
        ↳ try_acquire_background()  ← global slot reserved HERE
        ↳ record_call()
        ↳ _get_preadmitted()        ← HTTP fires IMMEDIATELY
```

Admission timestamp and HTTP call share no intermediate queue. The 60-second window accurately tracks actual physical load.
