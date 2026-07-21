---
name: Fundamentals & Quality Audit Corrections
description: EPS Growth (YoY diluted fiscal-exact), IC strict TTM, Net Debt/EBITDA post-BS, live valuation overlay, raw input storage, invalid multiple gates, N/M reasons, frontend sorting contract.
---

## Key architectural decisions

### EPS Growth — fiscal-exact YoY diluted
- **Rule:** Must use `is_rows[0].epsDiluted` vs `_get_fiscal_period_row(is_rows, str(int(fy)-1), per)` — exact fiscal year and period match.
- **Why:** FMP's `epsgrowth` ratio and any QoQ logic produce wrong values; the only correct source is diluted EPS rows from the Income Statement.
- **Method tag:** `_eps_growth_method = "diluted_eps_yoy_fiscal_exact"` always stored.
- **Status tags:** `turned_profitable` / `turned_unprofitable` / `negative_eps_basis` / `missing_prior_fiscal_period` — never silently null.

### Interest Coverage — strict TTM EBIT / |IE|
- **Rule:** N/M when: (a) deposit-funded bank/insurer, (b) zero interest expense, (c) negative interest expense (net interest income). Negative IC from negative EBIT is VALID and stored.
- **Why:** FMP's convenience ratio uses inconsistent TTM windows and sign conventions.
- **Method tag:** `_interest_coverage_method = "strict_ttm_ebit_over_absolute_interest_expense"`

### Net Debt / EBITDA — post-BS computed
- **Rule:** `(total_debt − cash_stinv) / ttm_ebitda` using stored BS inputs. N/M when: nonpositive EBITDA, deposit-funded bank/insurer.
- **Why:** FMP's `netDebtToEBITDATTM` has an inverted sign convention for net-cash companies (AMD showed -1.05 correctly via post-BS computation; FMP shows wrong sign).
- **`_is_leverage_metrics_not_meaningful()`:** blocks "credit services", "bank", "savings", "insurer" industries ONLY. Capital markets firms (HOOD), crypto (IREN), REITs (EQIX) are NOT blocked — their IC and ND/EBITDA ARE meaningful.

### Raw valuation input storage
- `_valuation_ttm_ni/rev/ebitda/ebit/ie/fcf/ev/ccy/total_debt/cash_stinv/implied_shares` stored in snapshot.
- `_valuation_fy1_rev/ebitda/eps` stored from estimates.
- These allow the live overlay to recompute all multiples at GET time without any provider calls.

### Live valuation overlay — pure function, never stored
- `compute_live_valuation_overlay(fund_fields, live_mc, live_px)` in watchlist_fundamentals_refresh.py.
- Returns dict of live multiples: PE/PS/EV-EBITDA/PFCF/FCFYield/FwdPE/FwdPS/FwdEVSales/FwdEVEBITDA.
- Includes `_enterprise_value_method = "live_market_cap_plus_debt_minus_cash_stinv"`.
- FCF yield outlier: `_fcf_yield_outlier = True` when |FCFYield| > 100%.
- Called from `_build_ticker_row()` in watchlist_router.py — NOT stored to Neon. GET is read-only.
- Provenance injected from market-cap resolver: `_valuation_market_cap_source`, `_valuation_is_live`, `_valuation_price_used`, `_valuation_price_timestamp`.

### Backfill universe source
- `get_all_cached_symbols()` in watchlist_fundamentals_store.py — returns all rows from `public.watchlist_fundamentals_cache`.
- The watchlist's `tickers` field is empty; tickers are embedded in `csv_data` as a 15k-element JSON blob. NEVER use `store.get("tickers")` as the backfill universe.
- Eligible universe = symbols already present in the fundamentals cache table (341 as of July 2026).

### Invalid multiple gates
- PE Ratio: `ttm_ni > 0` AND `shares > 0` AND `result > 0`
- PS Ratio: `ttm_rev > 0` AND `mc > 0`
- EV/EBITDA: `ttm_ebitda > 0` AND `ev > 0`
- P/FCF: `ttm_fcf > 0` AND `mc > 0`
- Forward EV/Sales, Forward EV/EBITDA: fy1 estimate > 0, ev > 0, result > 0
- Never return zero, negative, or sentinel for any multiple — N/M must be null + explicit reason key.

### N/M reason key naming convention
`_<field_snake_case>_not_meaningful_reason` e.g. `_interest_coverage_not_meaningful_reason`.
Values: `not_meaningful_for_financial_company`, `zero_interest_expense`, `net_interest_income`, `nonpositive_ebitda`, `negative_eps_basis`, `turned_profitable`, `turned_unprofitable`, `missing_prior_fiscal_period`.
