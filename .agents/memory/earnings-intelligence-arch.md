---
name: Earnings Intelligence Architecture
description: How earnings_intelligence is fetched, stored, and served in the ticker-detail popup; EDGAR limitation; timing heuristic.
---

## Write path
`FmpFundamentalsRefresher._fetch_earnings_intelligence(sym)` is called from
`refresh_symbols()` AFTER the substantive-payload check and BEFORE `upsert_snapshot()`.
The result is added to `result["fields"]["earnings_intelligence"]` so it is persisted
atomically in the single JSONB write — no separate `merge_fields()` call needed.

**Why:** `upsert_snapshot()` replaces the entire `fields` JSONB column (`fields = EXCLUDED.fields`).
Adding earnings_intelligence to `result["fields"]` before the upsert ensures it is never erased
by subsequent normal fundamentals refreshes. A separate `merge_fields()` write would create a
race window between the two writes.

**Carry-forward:** If `_fetch_earnings_intelligence()` returns None (FMP error, rate-limit, etc.),
the existing `_existing_snap["fields"]["earnings_intelligence"]` is carried into `result["fields"]`
before the upsert — same pattern as profile carry-forward.

## Read path (ticker-detail popup)
Section 8 of `ticker_detail_endpoint()` reads `raw_fields.get("earnings_intelligence")` from
the already-loaded Neon snapshot (zero extra I/O). Returns as top-level `earnings_intelligence`
key with `coverage["earnings_intelligence"]` flag.

## FMP calls per symbol (weekly refresh)
- Batch A (parallel 2): `earnings` + `income-statement` (for history + timing)
- Batch B (parallel 5): `grades-consensus` + `grades-historical` + `grades`
  + `price-target-consensus` + `price-target-summary`
- Optional (1): `historical-price-eod/dividend-adjusted` (FMP bars fallback only when
  `canonical_history_service.get_bars(sym, require_fresh=False)` returns None)

## Timing heuristic
SEC EDGAR `acceptedDate` (income-statement) is Eastern Time per documented standard and
confirmed by observation (COIN "16:29 ET" = AMC). Classification: hour>=16 → amc,
hour<10 → bmo, else → during_market. Confidence: always `inferred_low`.

## EDGAR limitation — SEC filings omitted
`edgar_cache` stores filings keyed by CIK (not ticker symbol). No safe symbol-level
cached read path exists without a CIK resolution step. `sec_filings` field returns `null`
with an explanatory string in `source_status.sec_filings_omitted_reason`.

**How to apply:** A separate narrow background-cache task should build a
ticker→CIK→filing index to enable symbol-level reads in the future.

## EPS growth semantics
`_eps_growth_info(current, prior)` returns `{"raw_growth_pct": float|None, "transition_type": str}`.
Transition types: profit_increased, profit_decreased, turned_profitable, turned_negative,
loss_narrowed, loss_widened, flat (< 0.05%), unavailable (prior==0 or None).
