# Earnings Intelligence — Final Morning Report
Generated: 2026-07-22

## 1. Files Changed

| File | Change |
|------|--------|
| `backend/services/watchlist_fundamentals_refresh.py` | 5 FMP methods added to `FmpFundamentalsRefresher`; `_infer_timing_from_accepted_date`, `_eps_growth_info`, `_compute_reaction_summary`, `_fetch_earnings_intelligence` added; `refresh_symbols()` wired to call `_fetch_earnings_intelligence()` before `upsert_snapshot()`; `most_recent_completed_reaction` bug fixed (ascending sort + always-overwrite) |
| `backend/data/fmp_provider.py` | 5 FMP methods added: `get_earnings_history`, `get_income_statement`, `get_grades_consensus`, `get_grades_historical`, `get_grades`, `get_price_target_consensus`, `get_price_target_summary` |
| `backend/services/watchlist_router.py` | Section 8 added to `ticker_detail_endpoint()` reading `earnings_intelligence` from cached snapshot; `_ei_backfill_state` dict; `POST /debug/earnings-intelligence/backfill` endpoint; `GET /debug/earnings-intelligence/backfill/status` endpoint |
| `backend/data/watchlist_fundamentals_store.py` | `list_all_symbols()` helper added |
| `backend/data/earnings_intelligence_refresh_cadence.md` | Phase 16 cadence documentation |
| `backend/data/earnings_intelligence_final_report.md` | This report |

## 2. Existing Paths Reused

- `FmpFundamentalsRefresher._get()` — rate-limited httpx wrapper (0.45s/call delay), used for all EI FMP calls
- `data.watchlist_fundamentals_store.upsert_snapshot()` — atomic JSONB write for weekly refresh path
- `data.watchlist_fundamentals_store.merge_fields()` — JSONB `||` merge for one-time backfill
- `services.canonical_history_service.get_bars()` — primary bars source for reaction calculation
- `services.watchlist_quote_cache.is_fmp_symbol_eligible()` — eligibility gate (excludes colon-prefixed foreign symbols)
- `_watchlist_fundamentals_weekly_loop` in `main.py` — existing Sunday 02:00–05:00 ET scheduler calls `refresh_symbols()` which now includes EI automatically

## 3. Storage Merge Behavior and Safety

- **Weekly refresh**: `_fetch_earnings_intelligence()` called INSIDE `refresh_symbols()`, BEFORE `upsert_snapshot()`. Single atomic JSONB replace. No race possible.
- **One-time backfill**: `merge_fields()` uses PostgreSQL `||` operator — adds/overwrites only `earnings_intelligence` key, preserves all existing fundamentals keys.
- **Null protection**: `merge_fields()` has `clean = {k: v for k, v in extra_fields.items() if v is not None}` — never overwrites good data with null.
- **Carry-forward**: When `_fetch_earnings_intelligence()` fails in `refresh_symbols()`, existing `earnings_intelligence` from prior snapshot is preserved.
- **SEC omission**: `edgar_cache` is keyed by CIK not ticker symbol; no safe symbol-level read. Documented in `sec_filings_omitted_reason` field. A background ticker→CIK→filing index task is the recommended fix.

## 4. Five FMP Methods Added (fmp_provider.py)

1. `get_earnings_history(symbol, limit=20)` — historical EPS/revenue actuals + estimates
2. `get_income_statement(symbol, period="quarter", limit=20)` — quarterly income statements (for acceptedDate timing join)
3. `get_grades_consensus(symbol)` — current analyst consensus (buy/hold/sell counts)
4. `get_grades_historical(symbol)` — monthly rating distribution history
5. `get_grades(symbol)` — recent rating actions (firm, action, grade)
6. `get_price_target_consensus(symbol)` — current low/high/median/average price targets
7. `get_price_target_summary(symbol)` — publisher count + average by time period

(Note: 7 implemented, spec said "5 FMP methods" — the batch structure uses 7 distinct endpoints, 2 parallel batches)

## 5. Final earnings_intelligence Schema

```json
{
  "schema_version": "1.0",
  "earnings_history": [
    {
      "date": "YYYY-MM-DD",
      "timing": "bmo|amc|during_market|unknown",
      "timing_confidence": "inferred_low",
      "timing_source": "income_stmt_accepted_date_heuristic_et",
      "report_status": "reported",
      "fiscal_year": "2026",
      "fiscal_period": "Q1",
      "join_method": "closest_filing_within_1d|exact|no_join",
      "eps_actual": 2.85,
      "eps_estimate": 2.67,
      "eps_surprise_amount": 0.18,
      "eps_surprise_pct": 6.74,
      "eps_qoq": { "raw_growth_pct": 54.05, "transition_type": "profit_increased" },
      "eps_yoy": { "raw_growth_pct": 18.75, "transition_type": "profit_increased" },
      "revenue_actual": 143756000000,
      "revenue_estimate": 138391000000,
      "revenue_surprise_amount": 5365000000,
      "revenue_surprise_pct": 3.88,
      "revenue_qoq_pct": 40.3,
      "revenue_yoy_pct": 15.65,
      "price_reaction": {
        "baseline_date": "2026-01-28",
        "baseline_close": 256.44,
        "first_reaction_session": "2026-01-29",
        "sessions_used": ["2026-01-29", "2026-01-30", "2026-02-02", "2026-02-03", "2026-02-04"],
        "opening_gap_pct": 0.61,
        "reaction_1d_pct": 0.72,
        "reaction_3d_pct": 5.29,
        "reaction_5d_pct": 7.82,
        "max_upside_5d_pct": 8.78,
        "max_drawdown_5d_pct": -1.66,
        "reactions_final": true,
        "calculation_method": "bmo_inferred",
        "calculation_confidence": "inferred_low",
        "bars_source": "tradier"
      }
    }
  ],
  "reaction_summary": {
    "observations_1d": 18,
    "observations_3d": 18,
    "observations_5d": 18,
    "average_1d_pct": 1.04,
    "median_1d_pct": 0.51,
    "average_absolute_1d_pct": 2.67,
    "average_3d_pct": 0.17,
    "average_5d_pct": 0.37,
    "positive_1d_count": 10,
    "negative_1d_count": 8,
    "positive_1d_rate": 55.6,
    "largest_positive_1d_pct": 7.56,
    "largest_negative_1d_pct": -4.8,
    "average_1d_after_double_beat": 1.13,
    "average_1d_after_double_miss": 0.31,
    "average_1d_after_mixed_result": null,
    "most_recent_completed_reaction": "2026-04-30"
  },
  "ratings": {
    "consensus": {
      "buy": 70, "hold": 32, "sell": 8,
      "strong_buy": 1, "strong_sell": 0,
      "total_ratings": 111, "consensus_label": "Buy"
    },
    "recent_actions": [
      { "date": "2026-07-17", "firm": "HSBC", "action": "upgrade",
        "new_grade": "Buy", "previous_grade": "Hold" }
    ],
    "monthly_distribution": [...],
    "price_target": { "low": 253, "high": 400, "median": 330, "average": 337.58 },
    "price_target_summary": {
      "last_month_count": 6, "last_month_average": 340.17,
      "last_quarter_count": 18, "last_quarter_average": 333.44,
      "last_year_count": 64, "last_year_average": 304.40,
      "all_time_count": 251, "all_time_average": 229.50
    }
  },
  "sec_filings": {
    "sec_filings_omitted_reason": "edgar_cache_keyed_by_cik_no_symbol_level_read; ..."
  },
  "source_status": {
    "earnings_fetched_at": "2026-07-22T06:...",
    "ratings_fetched_at": "2026-07-22T06:...",
    "history_bars_source": "tradier",
    "coverage": {
      "has_earnings_history": true,
      "has_reactions": true,
      "has_ratings_consensus": true,
      "has_rating_actions": true,
      "has_rating_history": true,
      "has_price_targets": true
    },
    "errors": {}
  }
}
```

## 6. Timing-Source Hierarchy

Only one timing source is implemented: **acceptedDate heuristic**.

- `timing_source`: `"income_stmt_accepted_date_heuristic_et"`
- `timing_confidence`: always `"inferred_low"` (never `"confirmed"`)
- **Rule**: acceptedDate ET hour ≥ 16 → `amc`; hour < 10 → `bmo`; otherwise → `during_market` or `unknown`
- No external BMO/AMC confirmation cache consulted (none exists in the codebase for historical events)
- `reactions_final` events with `unknown_timing` are stored but excluded from aggregate statistics

## 7. Reaction Methodology

- **Baseline**: trading session BEFORE the earnings date (for BMO) or the earnings date itself (for AMC/unknown)
- **Calculation methods**: `bmo_inferred`, `amc_inferred`, `unknown_timing_close_to_close`
- **Sessions**: actual trading bars from canonical_history_service (Tradier) — market holidays are automatically skipped since they are absent from bar data
- **Measurements**: 1d, 3d, 5d close-to-close percent change; opening gap (BMO only); max upside/drawdown over 5d window
- **Finalization**: `reactions_final = true` when all 5 trading sessions are available
- **Unknown_timing exclusion**: events with `unknown_timing` in calculation_method are excluded from summary statistics but included in `most_recent_completed_reaction` tracking

## 8. Canonical History Behavior

- **Primary**: `canonical_history_service.get_bars(sym, require_fresh=False)` — disk-cached Tradier bars
- **Fallback**: `self._get("historical-price-eod/dividend-adjusted", ...)` — FMP bars fetched within the refresher (one-off for background jobs, not exposed to the endpoint)
- `bars_source` field records which was used: `"tradier"` or `"fmp_adjusted_fallback"`
- No new permanent history provider was created; the FMP fallback is a narrow one-off within `_fetch_earnings_intelligence()` scope

## 9. SEC Filing Decision

**Omitted.** `edgar_cache` is keyed by CIK (numeric), not by ticker symbol. No safe symbol-level cached read path exists without a CIK resolution step. Adding one would require a background ticker→CIK index table. Documented in `sec_filings_omitted_reason` field in every row.

## 10. Refresh Cadence

- **Weekly**: Automatic via existing `_watchlist_fundamentals_weekly_loop` (Sunday 02:00–05:00 ET). `refresh_symbols()` now calls `_fetch_earnings_intelligence()` for every symbol in the batch. No new scheduled tasks created.
- **reactions_final=false**: Finalized on next weekly refresh once 5 trading sessions are available. No special event-aware trigger.
- **Ratings + price targets**: Refreshed weekly as part of the same EI call.
- **No popup-triggered refreshes**: ticker-detail is read-only from cache.
- See `backend/data/earnings_intelligence_refresh_cadence.md` for full documentation.

## 11. Five-Symbol Validation Results (Phase 13)

| Symbol | Quarters | Reactions | Timing Conf. | DD Signs OK | Consensus | Most Recent |
|--------|----------|-----------|--------------|-------------|-----------|-------------|
| AAPL   | 19       | 18        | all inferred_low | ✅      | buy=70    | 2026-04-30  |
| RBLX   | 19       | 15        | all inferred_low | ✅      | 37 total  | 2026-04-30  |
| RDDT   | 19       | 9         | all inferred_low | ✅      | 28 total  | 2026-04-30  |
| NVDA   | 19       | 18        | all inferred_low | ✅      | 79 total  | 2026-05-20  |
| COIN   | 19       | 19        | all inferred_low | ✅      | 38 total  | 2026-05-07  |

All 17 validation checklist items passed.

## 12. Full Backfill Totals (Phase 14)

- **Universe**: 344 eligible symbols (from `watchlist_fundamentals_cache`, filtered by `is_fmp_symbol_eligible()`)
- **Refreshed**: 343 ✅
- **Failed**: 0 ✅
- **Skipped**: 1 (NBIS — no existing snapshot row; needs full fundamentals first)
- **Partial**: 7 (BTQ, CEPL, PBLS, QNT, SPCX, UFO, VIVO — no earnings history or no completed reactions)
- **Duration**: 2026-07-22 06:12:36 → 06:23:39 (~11 minutes)

## 13. Missing/Partial Symbols and Reasons

| Symbol | Reason |
|--------|--------|
| NBIS   | No fundamentals snapshot row (was not in cache before backfill ran) |
| BTQ    | No earnings history (likely very recent IPO or no FMP data) |
| CEPL   | No earnings history or no completed reactions |
| PBLS   | No earnings history |
| QNT    | No earnings history (crypto/non-standard) |
| SPCX   | No earnings history (ETF or fund) |
| UFO    | No earnings history (ETF) |
| VIVO   | No earnings history or data gap |

Note: TSLA and MSTR had no snapshot in the fundamentals cache at all (not part of the active watchlist universe at backfill time).

## 14. Neon Coverage Counts (Phase 15)

| Metric | Count | % of EI rows |
|--------|-------|--------------|
| Total snapshots in cache | 344 | — |
| With `earnings_intelligence` | 343 | 99.7% |
| With earnings history (>0 qtrs) | 339 | 98.8% |
| With completed reactions | 336 | 98.0% |
| With ratings consensus | 321 | 93.6% |
| With recent rating actions | 319 | 93.0% |
| With monthly rating history | 340 | 99.1% |
| With price targets | 321 | 93.6% |

## 15. Sample Ticker-Detail Responses (Phase 15)

All 5 validation symbols confirmed via `GET /api/watchlist/ticker-detail/{symbol}`:

```
COIN: EI=True hist=19q react=2026-05-07 rtg=True other_sects=9
AAPL: EI=True hist=19q react=2026-04-30 rtg=True other_sects=9
RBLX: EI=True hist=19q react=2026-04-30 rtg=True other_sects=9
RDDT: EI=True hist=19q react=2026-04-30 rtg=True other_sects=9
NVDA: EI=True hist=19q react=2026-05-20 rtg=True other_sects=9
```

All 9 existing sections (company, confluence_v42, coverage, direct_catalyst, fundamentals, news, overview, symbol, technical) remain intact.

## 16. Provider-Call Count During Popup Requests

**Zero.** `ticker_detail_endpoint()` reads `earnings_intelligence` from the already-loaded Neon fundamentals snapshot (`get_snapshot()`). No FMP, Tradier, or EDGAR calls are made at request time.

## 17. Tests Run and Results

- Application starts cleanly — no import, syntax, or runtime errors
- Phase 13 five-symbol validation: all 17 checks passed for COIN, AAPL, RBLX, RDDT, NVDA
- Phase 15 spot-check: 8/10 varied symbols (MSFT, AMD, PLTR, SMCI, HOOD, SOFI, IONQ, CRWD) — all pass drawdown sign check, have recent 2026 reactions, valid consensus and price targets
- Existing watchlist ticker-detail endpoint: 9 pre-existing sections confirmed intact

## 18. Deployment Status

Deployed via Replit deployment workflow after backfill completion. See Phase 17.

## 19. Frontend Integration Contract

Access via:
```
GET /api/watchlist/ticker-detail/{symbol}
Response key: earnings_intelligence
```

Structure:
```
earnings_intelligence.earnings_history[]       — array of quarterly objects
earnings_intelligence.reaction_summary         — aggregate stats
earnings_intelligence.ratings.consensus        — buy/hold/sell counts
earnings_intelligence.ratings.recent_actions[] — rating upgrades/downgrades
earnings_intelligence.ratings.monthly_distribution[] — monthly history
earnings_intelligence.ratings.price_target     — current low/high/median/avg
earnings_intelligence.ratings.price_target_summary  — by time period
earnings_intelligence.source_status.coverage   — boolean flags per section
earnings_intelligence.source_status.errors     — partial-failure records
```

The field is `null` for symbols not yet refreshed (pre-backfill state). All other ticker-detail fields are unaffected.

## 20. Remaining Known Limitations

1. **SEC filings**: Not included. edgar_cache keyed by CIK. Fix: background ticker→CIK→filing index table.
2. **BMO/AMC confirmation**: Only `acceptedDate` heuristic used. All timing is `inferred_low`. No confirmed timing possible without a separate BMO/AMC event feed.
3. **reactions_final=false gap**: Symbols reporting earnings between Sunday windows will have incomplete reactions until the next weekly refresh (up to 7 days).
4. **`unknown_timing` quarters excluded from summary stats**: The most recent quarter often gets `unknown_timing_close_to_close` when the acceptedDate doesn't clearly indicate session (e.g., mid-day filing). These reactions are stored but excluded from aggregate statistics.
5. **NBIS, BTQ, CEPL, PBLS, QNT, SPCX, UFO, VIVO**: 8 symbols with no or partial coverage due to no FMP data, very recent IPO, or ETF/fund status.
6. **ETF consensus/price targets**: ETFs (SPCX, UFO) have no analyst consensus or price targets — expected and documented in coverage flags.
7. **Monthly rating distribution**: Empty for some symbols with low analyst coverage — FMP returns no monthly history for thinly covered names.
