---
name: OTC FMP coverage architecture
description: How OTC watchlist symbols (OTC:BESIY) are routed through FMP for quotes, fundamentals, and earnings — provider boundary rules, canonical key discipline, and rate-limit integration.
---

## The rule

`OTC:BESIY` is the canonical symbol everywhere in membership, stores, caches, and output.
The bare FMP provider symbol (`BESIY`) is derived ONLY at the HTTP call boundary.
`services/otc_service.py` is the single source of truth: `is_otc_symbol()`, `otc_to_fmp()`, `split_otc_us()`.

## Provider boundary

| Symbol type       | Tradier eligible | FMP eligible | Path                         |
|-------------------|-----------------|--------------|------------------------------|
| `NVDA` (US)       | Yes             | Yes          | Tradier quote; FMP fundamentals |
| `OTC:BESIY`       | No              | Yes          | FMP only for all three paths |
| `LON:VOD` (foreign) | No            | No           | Excluded from all paths      |

`is_tradier_quote_eligible` — unchanged, rejects any `:` including `OTC:`.
`is_fmp_symbol_eligible` — updated: returns True for `OTC:` prefix, False for all other colon symbols.

## Quote path (watchlist_quote_cache + home_service)

- `_fetch_otc_quotes_fmp(otc_canonical)` in `watchlist_quote_cache.py` fetches individual `stable/quote` calls.
- Uses `fmp_governor.acquire("otc_quotes")` + `record_call()` — NOT a homemade limiter.
- Results stored under canonical key (`OTC:BESIY`), never bare key.
- Fields absent from FMP response are absent in result — no zeros synthesised.
- `_do_refresh()` calls OTC path in addition to existing Tradier path; both merge into `_quote_cache`.
- `home_service._batch_quotes()` Step 4: same pattern; writes to `quote:lkg:OTC:BESIY`.

## Fundamentals path (watchlist_fundamentals_refresh.py)

- `refresh_symbols()` builds `_fmp_sym_map = {canonical.upper(): bare_fmp}`.
- `normalize_symbol(_fmp_call_sym)` and `_fetch_earnings_intelligence(_fmp_call_sym)` use bare symbol.
- `upsert_snapshot(sym, ...)` always uses canonical `sym` as store key.
- Goes through same Sunday window, same 7-day TTL, same `refresh_symbols()` call as US tickers.

## Earnings path (earnings_monitor_service.py)

- `_is_eligible()`: accepts `OTC:` prefix; all other colon formats still rejected.
- `_refresh_schedule()` builds:
  - `_otc_bare_to_canonical` = `{"BESIY": "OTC:BESIY"}`
  - `_otc_canonical_to_bare` = `{"OTC:BESIY": "BESIY"}`
  - Adds bare syms to `sym_set` so FMP calendar client-side filter matches them.
- After building `cal_by_sym`, remaps bare OTC entries to canonical before main loop.
- Per-symbol fallback: `fmp.get_earnings_history(_otc_canonical_to_bare.get(sym, sym))` — bare for API.
- `upsert_target(sym, ...)` always receives canonical `OTC:BESIY`.

## Rate limit

OTC quote workload is tiny (≤15 symbols typical). At fmp_governor default 120 RPM, 15 symbols takes ~15s. Plan limit is 300 RPM. The governor serializes OTC quote calls alongside fundamentals/earnings traffic — no separate budget needed.

## FMP data availability (observed)

- BESIY (BE Semiconductor): price=256, change=7.47, changePercentage=3.0, volume present, avgVolume=None
- DSCSY (Disco Corp): price=37.61, change=-1.12, changePercentage=-2.89, volume present, avgVolume=None
- CSCUF: empty response — FMP has no data; no fake record written (correct behavior)

**Why:** `avgVolume=None` for some OTC tickers is genuine FMP non-coverage. These fields remain null in the cache and output — never substituted with 0.
