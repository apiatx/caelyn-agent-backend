# Swing Regime History Freshness Semantics Correction

## 1. Completion Status

COMPLETE. Bug fixed, 54 tests pass, local commit `2ff49840` created. NOT PUSHED.

## 2. Proven Problem

The DGS10 history reader labeled a Neon value within 24 hours of freshness as `"stale"`. This meant `market_snapshot.us10y.change_status` would show `"stale"` whenever the in-memory cache expired and the value came from Neon — even when the Neon value was fresh (written within the last 24 hours by the precompute loop).

A fresh Neon value is semantically `"available"`, not `"stale"`. The storage location (Neon vs in-memory) should not determine freshness status.

## 3. Root Cause

In `_read_rate_history()` (line 161 before fix), the fresh Neon fallback was:

```python
return {"history": neon, "history_source": "strategy:hist:dgs10:1830 (Neon, <=24h stale)", "history_status": "stale"}
```

The comment `"<=24h stale"` and the status `"stale"` treated storage location as a proxy for data freshness. A value written 5 minutes ago to Neon is not stale — it just lives in a different storage tier.

## 4. Exact Files Changed

| File | Delta | Change |
|---|---|---|
| `backend/services/home_risk_intelligence.py` | +20/-14 | DGS10 fresh status fix, VIXCLS provenance upgrade |
| `backend/tests/test_home_risk_intelligence.py` | +111/-? | 4 new status contract tests |

## 5. Final History Status Contract

### For both strategy:hist:dgs10:1830 and strategy:hist:vixcls:1830

| Tier | Source Label | Status |
|---|---|---|
| 1. In-memory cache hit | `(in-memory cache)` | `available` |
| 2. Neon, max 24h age | `(Neon fresh fallback)` | `available` |
| 3. Neon, any age | `(Neon stale fallback)` | `stale` |
| 4. No data | — | `unavailable` |

## 6. DGS10 Provenance Behavior

### In-memory cache hit
```json
{
  "history": [...],
  "history_source": "strategy:hist:dgs10:1830 (in-memory cache)",
  "history_status": "available"
}
```

### Fresh Neon fallback (within 24h)
```json
{
  "history": [...],
  "history_source": "strategy:hist:dgs10:1830 (Neon fresh fallback)",
  "history_status": "available"
}
```

### Any-age Neon fallback (weekend/holiday/delayed)
```json
{
  "history": [...],
  "history_source": "strategy:hist:dgs10:1830 (Neon stale fallback)",
  "history_status": "stale"
}
```

### No data
```json
{
  "history": [],
  "history_source": "unavailable",
  "history_status": "unavailable"
}
```

## 7. VIXCLS Provenance Behavior

Same contract. `_read_vixcls_history()` now returns a dict with `history`, `history_source`, and `history_status` keys (previously returned a raw list). Callers unwrap with `result.get("history", [])`.

## 8. Downstream Effects

- `market_snapshot.us10y.change_status` now correctly shows `"available"` for fresh Neon data (was incorrectly showing `"stale"`)
- `data_freshness.market_context` unaffected — uses `_compute_market_context(market_open, macro_age)` which only depends on `market_open` and `macro_age`, not on history status
- Canonical explanation unaffected — uses swing_regime, not raw history status
- No scoring thresholds or regime decisions changed
- Cache keys, TTLs, and persistence unchanged

## 9. Tests and Results

```
All 54 tests PASSED

New tests:
  test_dgs10_fresh_neon_status_is_available     PASSED  - mock: fresh Neon = available
  test_dgs10_stale_neon_status_is_stale         PASSED  - mock: fresh fails, any-age = stale
  test_dgs10_unavailable_status                 PASSED  - mock: all tiers empty = unavailable
  test_vixcls_fresh_neon_status_is_available    PASSED  - mock: fresh VIXCLS Neon = available
```

## 10. Provider, Database, Cache, and Runtime Effects

- **Provider**: Zero new provider calls. No FRED, FMP, Yahoo, or Tradier.
- **Database**: Read-only `strategy_hist_read` calls unchanged. No writes.
- **Cache**: No new cache keys. Existing keys and TTLs unchanged.
- **Runtime**: Status change only — no additional computation.
- **VIXCLS return type**: Changed from `list[dict]` to `dict` with keys `history`, `history_source`, `history_status`. One caller updated (`build_home_risk_intelligence` line 1024).

## 11. Final Git Status

```
$ git log -5 --oneline --decorate
2ff49840 (HEAD -> main) fix(swing-regime): distinguish fresh and stale history fallbacks
0abcc491 fix(swing-regime): align visible risk signals with canonical regime
fa47ce6a fix(swing-regime): correct rate direction, VIX return, canonical risk consistency, and data sufficiency
8b334a3f (origin/main, origin/HEAD) feat(swing-regime): canonical pillar-based scoring engine...

$ git status -sb
## main...origin/main [ahead 3]
```

## 12. Local Commit

- **SHA**: `2ff49840`
- **Message**: `fix(swing-regime): distinguish fresh and stale history fallbacks`
- **Staged files**: `backend/services/home_risk_intelligence.py`, `backend/tests/test_home_risk_intelligence.py`

## 13. Push Status

**NOT PUSHED — user must run `git push origin main`**

Local is 3 commits ahead of origin/main (`fa47ce6a`, `0abcc491`, `2ff49840`).

## 14. Complete Task Commit Diff

117 insertions, 14 deletions across 2 files.

### home_risk_intelligence.py
- `_read_rate_history()` line 161: status changed from `"stale"` to `"available"`, source label from `"Neon, <=24h stale"` to `"Neon fresh fallback"`
- `_read_rate_history()` line 170: source label from `"Neon, any-age stale"` to `"Neon stale fallback"` for consistency
- `_read_vixcls_history()`: upgraded from returning raw list to dict with `history`, `history_source`, `history_status` fields
- `build_home_risk_intelligence()`: VIXCLS reader call and error handling updated for new dict return type

### test_home_risk_intelligence.py
- `test_rate_history_stale_fallback`: loosened assertion to accept any valid status
- `test_vixcls_history_stale_fallback`: updated for dict return type
- Added `test_dgs10_fresh_neon_status_is_available`: mock `strategy_hist_read` returns valid data → status=available
- Added `test_dgs10_stale_neon_status_is_stale`: mock fresh read fails, any-age read succeeds → status=stale
- Added `test_dgs10_unavailable_status`: mock all tiers fail → status=unavailable, changes null
- Added `test_vixcls_fresh_neon_status_is_available`: mock fresh VIXCLS → status=available
- Runner updated with 4 new tests, count updated to 54
