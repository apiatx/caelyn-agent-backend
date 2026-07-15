# TICKER DETAIL CONTRACT BLOCKER FIX REPORT
**Date:** 2026-07-15  
**Endpoint:** `GET /api/watchlist/ticker-detail/{symbol}`  
**File changed:** `backend/services/watchlist_router.py`

---

## FUNDAMENTALS SOURCE TRACE

**Endpoint used by Watchlist Fundamental toggle:** `GET /api/watchlist/{watchlist_id}` → `get_by_id_endpoint()`  
**Service/function:** `FmpFundamentalsRefresher.normalize_symbol()` (background task, never on page render)  
**Neon table:** `public.watchlist_fundamentals_cache` — JSONB `fields` column  
**Merge precedence:** FMP non-null > CSV value > blank (no-null-overwrite rule)

**Exact JSONB key names stored vs. frontend column names:**

| Frontend Toggle Column | Stored JSONB Key       | Source                  |
|------------------------|------------------------|-------------------------|
| Revenue                | `"Revenue"`            | FMP income-statement Q (TTM) |
| Rev Grwth (Q)          | `"Revenue Growth (Q)"` | FMP income-statement Q (YoY vs same Q prior yr) |
| Rev Grwth (Y)          | `"Revenue Growth (YoY)"` | FMP income-statement Q (TTM vs prior TTM) |
| Gross Mgn              | `"Gross Margin"`       | FMP ratios-ttm |
| FCF Mgn                | `"FCF Margin"`         | Derived: TTM FCF / TTM Rev |
| Free CF                | `"Free Cash Flow"`     | FMP cash-flow-statement Q (TTM) |
| Op. Income             | `"Operating Income"`   | FMP income-statement Q (TTM) |
| EBIT                   | `"EBIT"`               | FMP income-statement Q (TTM) |
| P/E                    | `"PE Ratio"`           | FMP ratios-ttm |
| P/S                    | `"PS Ratio"`           | FMP ratios-ttm |
| EV/EBITDA              | `"EV/EBITDA"`          | FMP key-metrics-ttm |
| EPS Grwth              | `"EPS Growth"`         | FMP income-statement-growth Q |
| D/E                    | `"Debt / Equity"`      | FMP ratios-ttm |
| ND/EBITDA              | `"Net Debt / EBITDA"`  | FMP key-metrics-ttm |
| Insider %              | `"Shares Insiders"`    | **CSV only** — FMP Starter 404s |
| Earn. Date             | `"Earnings Date"`      | FMP earnings (first future date) |
| Rev Grwth Est          | `"Revenue Growth Est."` | **CSV only** — FMP Premium (402) |
| Rev Grwth NQ           | `"Rev Growth Next Quarter"` | FMP earnings estimate |
| Rev Grwth NY           | `"Rev Growth Next Year"` | **CSV only** — FMP Premium (402) |
| EPS Grwth Est          | `"EPS Growth Est."`    | **CSV only** — FMP Premium (402) |
| EPS Grwth TQ           | `"EPS Growth This Quarter"` | FMP earnings estimate |
| EPS Grwth NQ           | `"EPS Growth Next Quarter"` | **CSV only** — only 1 future Q from Starter |
| EPS Grwth TY           | `"EPS Growth This Year"` | **CSV only** — FMP Premium (402) |
| EPS Grwth NY           | `"EPS Growth Next Year"` | **CSV only** — FMP Premium (402) |

**Root cause of original `revenue=None`:**  
The previous endpoint used `**fields` spread which exposed title-case keys (`"Revenue"`, `"PE Ratio"`) directly. The validation check `fund.get('revenue')` with lowercase returned None. The data was always there — the key name was wrong. Now normalized with `_FUND_NORM` map.

**Was any upload/CSV data used?** No. The Neon snapshot stores the post-merge result; fields populated from CSV upload retain their values but are only present for symbols where upload data was provided. No CSV file is read at request time.

---

## FUNDAMENTALS CONTRACT FIX

Added module-level `_FUND_NORM` dict mapping all 27 title-case storage keys to snake_case.  
The `fundamentals` section now returns:

```
ticker, theme (from confluence_v42.theme_name),
market_cap, revenue, revenue_growth_q, revenue_growth_y,
gross_margin, fcf_margin, free_cash_flow, operating_income, ebit,
pe_ratio, ps_ratio, ev_ebitda, eps_growth, debt_equity, net_debt_ebitda,
insider_percent, earnings_date, revenue_growth_est, revenue_growth_next_quarter,
revenue_growth_next_year, revenue_growth_this_year, eps_growth_est,
eps_growth_this_quarter, eps_growth_next_quarter, eps_growth_this_year,
eps_growth_next_year
```

Plus `fundamentals_source`:
```json
{
  "source_table": "watchlist_fundamentals_cache",
  "source_service": "FmpFundamentalsRefresher",
  "last_updated": "2026-07-12T...",
  "next_refresh_at": "...",
  "freshness_status": "fresh",
  "age_days": 3,
  "missing_fields": ["insider_percent", "revenue_growth_est", ...],
  "fmp_call_count": 7
}
```

Fields genuinely absent (not key-mapping bugs):
- `insider_percent` — FMP Starter has no insider ownership endpoint
- `revenue_growth_est/next_year/this_year` — FMP Premium (402 on Starter)
- `eps_growth_est/next_quarter/this_year/next_year` — FMP Premium (402 on Starter)

---

## QUOTE CONTRACT FIX

Replaced the bare price dict with:

```json
{
  "price": 138.69,
  "change_percent": -1.38,
  "volume": 1234567,
  "average_volume": 2000000,
  "relative_volume": 0.62,
  "quote_status": "available",
  "source": "quote_cache",
  "last_updated": "2026-07-15T..."
}
```

`quote_status` values:
- `available` — price in active `watchlist_quote_cache`
- `row_fallback_recommended` — symbol not in current poll; price=null; frontend should use stale row price from the watchlist table

Renamed `change_pct_1d` → `change_percent` to match spec.

---

## ABCL SNAPSHOT CONSISTENCY

| Source | Score | Bucket | Actionability | Method |
|--------|-------|--------|---------------|--------|
| `GET /api/alpha/confluence/ABCL` (warm) | 49.9 | NEAR_ACTIONABLE | NEAR_ACTIONABLE | fresh build |
| `GET /api/watchlist/ticker-detail/ABCL` (warm) | 49.9 | NEAR_ACTIONABLE | NEAR_ACTIONABLE | fresh build |
| `GET /api/watchlist/ticker-detail/ABCL` (cold restart) | 37.8 | NEAR_ACTIONABLE | NEAR_ACTIONABLE | fresh build, empty caches |
| Retained snapshot | N/A | — | — | ABCL absent from retained |

**Is ticker-detail reading the same retained snapshot as the Confluence table?**  
Yes, for symbols present in the retained snapshot. For ABCL specifically: ABCL is NOT in the current retained snapshot, so both `ticker-detail` and `/api/alpha/confluence/ABCL` fall back to the same fresh build path — they are consistent.

**Cold-start score variance (37.8 vs 49.9):**  
Fresh build calls `build_confluence_snapshot()` which reads from several in-memory caches: `get_combined_ticker_data()` (options), `_news_lkg` (news signals), `_STAGE2_LKG` (stage2). On cold restart, these caches are empty, reducing scored components. On a warm production server, ABCL scores ~49.9. No code fix needed.

---

## VALIDATION TABLE

| Symbol | Revenue | Fund Keys Present | Fund Source Matches Toggle | Rev_OK | Quote Status | C42 Score | C42 Bucket | News | DC Tier | Provider Calls |
|--------|---------|-------------------|---------------------------|--------|-------------|-----------|------------|------|---------|----------------|
| ENTG   | 3.2B    | 19/27¹            | YES                        | YES    | available   | 55.6      | ACTIONABLE | 12   | —       | NO             |
| MU     | 90.3B   | 19/27¹            | YES                        | YES    | row_fbk_rec | 53.3      | ACTIONABLE | 293  | TIER_E  | NO             |
| TSM    | 4.1T NTD| 19/27¹            | YES                        | YES    | row_fbk_rec | 73.0      | ACTIONABLE | 140  | TIER_A  | NO             |
| ABCL   | 79M     | 19/27¹            | YES                        | YES    | row_fbk_rec | 49.9²     | NEAR_ACT.  | 2    | —       | NO             |
| ALGM   | 890M    | 18/27¹³           | YES                        | YES    | row_fbk_rec | 63.6      | ACTIONABLE | 3    | TIER_C  | NO             |
| VRT    | 10.8B   | 19/27¹            | YES                        | YES    | row_fbk_rec | 66.5      | ACTIONABLE | 29   | TIER_B  | NO             |
| HIMS   | 2.4B    | 19/27¹            | YES                        | YES    | row_fbk_rec | 43.3      | NEAR_ACT.  | 18   | —       | NO             |
| CRWD   | 5.1B    | 19/27¹            | YES                        | YES    | row_fbk_rec | 63.7      | NEAR_ACT.  | 65   | TIER_C  | NO             |

¹ 8 missing fields are ALL genuine data gaps (FMP Premium/CSV-only): `insider_percent`, `revenue_growth_est`, `revenue_growth_next_year`, `revenue_growth_this_year`, `eps_growth_est`, `eps_growth_next_quarter`, `eps_growth_this_year`, `eps_growth_next_year`. No key-mapping bugs.  
² Warm-server score. Cold-restart score = 37.8 due to empty in-memory caches.  
³ ALGM missing `market_cap` additionally — FMP profile endpoint returned null for this ticker.

---

## FILES CHANGED

- `backend/services/watchlist_router.py`
  - Added `_FUND_NORM` module-level normalization map (27 canonical → snake_case mappings)
  - Fixed fundamentals section: `**fields` spread replaced with normalized output + `fundamentals_source` metadata
  - Added `theme` injection from `confluence_v42.theme_name`  
  - Fixed quote section: added `quote_status`, `source`, `last_updated`; renamed `change_pct_1d` → `change_percent`

---

# TICKER DETAIL CONTRACT READINESS VERDICT

**FUNDAMENTALS_MATCH_WATCHLIST_TOGGLE:**  
YES — same Neon table (`watchlist_fundamentals_cache`), same FmpFundamentalsRefresher write path, same field values.

**REVENUE_FIELD_FIXED:**  
YES — revenue now returns TTM values for all 8 symbols. Root cause was title-case key `"Revenue"` stored in JSONB; fixed with `_FUND_NORM` normalization map.

**STALE_UPLOAD_DATA_AVOIDED:**  
YES — no CSV/upload file is read at request time. All data from Neon cache written by FmpFundamentalsRefresher. Fields populated from CSV (insider_percent etc.) are only present where upload data exists; no stale source is introduced.

**QUOTE_STATUS_CONTRACT_ADDED:**  
YES — `quote_status` (available/row_fallback_recommended), `source` (quote_cache/unavailable), `last_updated` added. `change_pct_1d` renamed to `change_percent`.

**ABCL_SNAPSHOT_CONSISTENT:**  
YES — ticker-detail and /api/alpha/confluence/ABCL both use fresh-build fallback (ABCL absent from retained snapshot). Score on warm server = 49.9 for both. Cold-start variance (37.8) is a cache-warmth artifact, not a code divergence.

**NO_PROVIDER_CALLS_ON_MODAL_OPEN:**  
YES — all 7 data sections (company, quote, confluence, technical, fundamentals, news, catalyst) read exclusively from in-memory/disk/Neon caches.

**FRONTEND_READY_TO_IMPLEMENT:**  
YES
