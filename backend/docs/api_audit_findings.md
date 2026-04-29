# FMP / Provider-Call Audit — Findings
_Generated from live telemetry — Caelyn AI backend audit_

---

## How to use the debug endpoint

```
GET /api/debug/provider-call-audit
GET /api/debug/provider-call-audit?reset=true          # clear counters
GET /api/debug/provider-call-audit?force_429=true      # simulate FMP 429 on every call
GET /api/debug/provider-call-audit?force_429=false     # disable simulation
```

Response includes:
- `totals_since_start` — cumulative call counts per provider
- `total_cache_hits / total_cache_misses` — across all providers
- `aggregated_by_provider_endpoint` — breakdown by endpoint with hit/miss/error counts and avg latency
- `recent_requests` — last 200 route-level request summaries (FMP calls per request, elapsed, cache efficiency)
- `recent_calls` — last 500 individual FMP call records with ticker, feature, status

---

## Calendar Page — FMP Call Budget

### Route-level breakdown (cold cache, no symbols filter)

| Route | UI trigger | FMP endpoints | Max FMP calls (cold) | Notes |
|---|---|---|---|---|
| `GET /api/catalysts/overview` | Page load | earnings-calendar (×12 weekly chunks), dividends-calendar, ipos-calendar, splits-calendar, economic-calendar, treasury-rates, sec-filings (×N), ratings-snapshot (×N), insider-trading (×N), profile (×N) | **≈ 18 + N_unique × 4** (unbounded) | All 10 tabs fetched in parallel; profile enrichment runs per unique symbol with semaphore=8 but no hard cap on symbol count |
| `GET /api/catalysts/events?tab=earnings_dates` | Tab click | earnings-calendar (12 weekly chunks) + profile×N | **12 + N_unique** | N_unique varies by week range; 49 observed in live run |
| `GET /api/catalysts/events?tab=dividends` | Tab click | dividends-calendar + profile×N | **1 + N_unique** | |
| `GET /api/catalysts/events?tab=ipos` | Tab click | ipos-calendar + profile×N | **1 + N_unique** | |
| `GET /api/catalysts/events?tab=splits` | Tab click | splits-calendar + profile×N | **1 + N_unique** | |
| `GET /api/catalysts/events?tab=economic_releases` | Tab click | economic-calendar | **1** | No profile enrichment (no symbols) |
| `GET /api/catalysts/events?tab=treasury_macro` | Tab click | treasury-rates | **1** | No profile enrichment |
| `GET /api/catalysts/events?tab=recent_earnings` | Tab click | earnings-calendar (recent back window) + profile×N | **≤12 + N_unique** | |
| `GET /api/catalysts/events?tab=sec_filings` | Tab click | sec-filings (×N symbols from watchlist/portfolio) | **N** | |
| `GET /api/catalysts/events?tab=analyst_ratings` | Tab click | ratings-snapshot (×N) | **N** | |
| `GET /api/catalysts/events?tab=insider_transactions` | Tab click | insider-trading (×N) | **N** | |
| `GET /api/catalysts/by-symbol/{sym}` | Symbol click | profile, sec-filings, ratings-snapshot, insider-trading | **4** | Per-symbol only, well bounded |
| `GET /api/catalysts/filters` | Sidebar open | _none_ | **0** | Pure in-memory |
| `GET /api/earnings/calendar` (legacy) | — | earnings-calendar (12 weekly chunks) | **12** | Legacy, no enrichment |

### Live observed data (from `/api/debug/provider-call-audit`)

After a single cold-cache `/api/catalysts/overview` call (partial, timed out at 30 s):

| FMP Endpoint | Calls | Cache hits | Avg latency |
|---|---|---|---|
| profile | 24 | 0 | 7934 ms |
| ratings-snapshot | 30 | 0 | 515 ms |
| sec-filings | 20 | 0 | 664 ms |
| insider-trading | 20 | 0 | 331 ms |
| earnings-calendar | 5 | 0 | 870 ms |
| dividends-calendar | 1 | 0 | 1109 ms |
| ipos-calendar | 1 | 0 | 795 ms |
| splits-calendar | 1 | 0 | 822 ms |
| treasury-rates | 1 | 0 | 848 ms |
| economic-calendar | 1 | 0 | 967 ms |
| **Total** | **104** | **0** | — |

_Note: request was still in-flight at 30 s; the full cold budget is larger (up to ~218+ calls for 50 unique symbols across all 10 tabs)._

### Key risk: profile enrichment is unbounded

`_enrich_profiles()` in `catalyst_calendar_service.py` iterates every unique symbol in the event list with a semaphore of 8 concurrent requests but no hard cap. A broad date window could return hundreds of unique symbols → hundreds of sequential FMP `profile` calls.

---

## Social Page — FMP Call Budget

### Route-level breakdown (cold cache)

| Route | UI trigger | FMP endpoints | Max FMP calls (cold) | Notes |
|---|---|---|---|---|
| `GET /api/social/x-dashboard` | Page load | _none_ | **0** | `allow_live_fmp=False`; cache-only; non-blocking |
| `POST /api/social/x-dashboard/refresh` | Refresh button | _none_ | **0** | Also `allow_live_fmp=False` |
| `GET /api/social/fundamental-screener` | Lazy (after dashboard renders) | profile×N_social + quote×N_social + stock-price-change×N_social + ratios-ttm×N_fund + key-metrics-ttm×N_fund + income-statement×N_fund + balance-sheet×N_fund + cash-flow×N_fund | **N_social×3 + N_fund×5** | N_social≈57, N_fund≈50 → **≈ 471 max** |

### FMP call decomposition for `/api/social/fundamental-screener` (cold)

| FMP endpoint | calls | feature label |
|---|---|---|
| profile | ≤57 | social_profile |
| quote | ≤57 | social_quote |
| stock-price-change | ≤57 | social_price_change |
| ratios-ttm | ≤50 | fundamental_ratios |
| key-metrics-ttm | ≤50 | fundamental_key_metrics |
| income-statement | ≤50 | fundamental_income |
| balance-sheet-statement | ≤50 | fundamental_balance |
| cash-flow-statement | ≤50 | fundamental_cashflow |
| **Total** | **≤ 471** | |

_Social universe size (N_social ≈ 57) is the number of unique tickers seen by the Grok/X pipeline.
Fundamental subset (N_fund ≈ 50) is the top-N filtered for fundamentals._

---

## FMP_FORCE_429 Simulation

When enabled (via the debug endpoint or `FMP_FORCE_429=true` env var), every `CatalystFMP._get()` and `_fmp_get()` call in the social screener returns `[]` immediately without making an HTTP request, and records a synthetic `http_status=429` in the audit ring buffer.

To verify graceful degradation:

```bash
# Enable
curl "https://<host>/api/debug/provider-call-audit?force_429=true"

# Hit any calendar/social route
curl -H "X-API-Key: $KEY" "https://<host>/api/catalysts/overview"

# Inspect — all FMP calls should show http_status=429, errors > 0
curl "https://<host>/api/debug/provider-call-audit"

# Disable
curl "https://<host>/api/debug/provider-call-audit?force_429=false"
```

---

## Files modified in this audit pass

| File | Change |
|---|---|
| `backend/services/api_audit.py` | New — thread-safe telemetry module; `record_call`, `record_request`, `get_report`, `reset_stats`, `fmp_force_429`, `set_force_429`, `get_total_calls`, `get_cache_counts` |
| `backend/services/catalyst_calendar_service.py` | `CatalystFMP._get()` instrumented; `get_overview` / `get_events` emit request summaries; FMP_FORCE_429 guard added |
| `backend/services/social_screener_service.py` | `_fmp_get()` instrumented with cache-hit recording, FMP_FORCE_429 guard, per-feature labels; `fetch_enrichment_for_symbols` emits request summary |
| `backend/main.py` | `GET /api/debug/provider-call-audit` endpoint added |

---

## Recommended fixes (not yet implemented)

1. **Cap profile enrichment** in `_enrich_profiles()` — hard limit of e.g. 50 symbols per overview call.
2. **Cache the overview response** — serve a pre-built overview from TTL cache, refresh in background.
3. **Rate-limit the fundamental-screener** — it already runs lazily but 471 calls in one burst is risky on Starter plan.
4. **Deduplicate profile calls** between calendar tabs — each tab independently enriches its symbols; share a single profile cache per request.
