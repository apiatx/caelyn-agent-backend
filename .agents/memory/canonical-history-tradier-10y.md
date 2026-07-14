---
name: Canonical History Tradier 10Y Precision
description: Root cause of false 5Y cap claim; proven Tradier depth; classification rules for _classify_limit_type(); safe-mode flags.
---

## Rule
Tradier returns full lifetime coverage. There is no 5Y cap. Use `_LONG_HIST_DAYS=3650` always.

**Why:** V4.2.5.3 used `_LONG_HIST_DAYS=1825` (5Y window). ~1253 bars returned was correct for 1825
calendar days. The false "Tradier caps at ~5Y" claim was never tested with a 3650-day window.

**Live test proof (2026-07-14, capability-test endpoint):**
- SPY/AAPL/MSFT/NVDA/TSM 3650d window → 2510 bars, oldest 2016-07-18
- Same symbols start=2010-01-01 → 4155 bars, oldest 2010-01-04
- Young tickers (ABCL/ALGM/CRDO etc.): BOTH windows return identical bar counts and oldest dates
  → Tradier returned full lifetime data, not a capped subset

**How to apply:** `_LONG_HIST_DAYS=3650` in canonical_history_backfill.py.
FMP fallback ONLY when Tradier returns 0 bars (complete failure), not for partial results.

## Classification rules for `_classify_limit_type()` (single 3650d Tradier request)
| bars | status | reason |
|------|--------|--------|
| ≥ 2200 | `available_10y` | Tradier confirmed 10Y depth |
| 130–2199 | `available_lifetime_under_10y` | Tradier returned all available; ticker < 10Y old |
| 1–129 | `actual_ticker_history_limit` | brand-new ticker, < ~6 months |
| 0 | no_bars / fetch_failed | Tradier returned nothing |

`provider_cap_detected` is NOT set from a single 3650d request. Only set if explicit external
evidence proves the ticker is older than the returned oldest_bar AND another provider has more.

`oldest_gap_days` (calendar days between window-start and oldest returned bar) is informational
only — it does NOT drive classification. Early assumption that gap > 180 = provider cap was WRONG.

## Capability-test endpoint
- POST /api/admin/canonical-history/capability-test?symbols_csv=SPY,AAPL&test_days=3650
- GET  /api/admin/canonical-history/capability-test  (poll; `_CAPTEST_STATE` module dict in main.py)
- First Tradier probe after cold start takes 50-60s (large JSON + TCP handshake); subsequent ~300ms

## httpx timeout behaviour for large payloads
`httpx.AsyncClient(timeout=12)` sets 12s per-read, NOT total-transfer. A 2510-row OHLCV response
(~250KB) arrives via multiple reads and can take 55s total while no single read exceeds 12s.

## Safe-mode flags (canonical_history_backfill)
```
CANONICAL_HISTORY_BACKFILL_ENABLED=false          # master gate for full backfill
CANONICAL_HISTORY_FULL_BACKFILL_ENABLED=false      # first-time full scan
CANONICAL_HISTORY_INCREMENTAL_APPEND_ENABLED=true  # append-only (always safe to keep on)
CANONICAL_HISTORY_ALLOW_MARKET_HOURS=false         # off-hours only
```

## Validation results (19 symbols, 2026-07-14)
available_10y (≥2200 bars): SPY AAPL MSFT NVDA TSM MARA SMCI ENTG ONTO CGNX
available_lifetime_under_10y: ABCL ALGM VRT CRDO WYFI SOFI HOOD HIMS OUST
19/19 usable; 19/19 no 400-bar fallback; 10/19 Tradier 10Y capable (≥2200 bars)

## OUST edge case
OUST returns 1656 bars from 3650d window (oldest 2016-07-18) but 2180 bars from 2010-01-01
(oldest 2014-06-17). This is a ticker-recycling case — "OUST" was used by a different entity
pre-2021 before Ouster Inc (SPAC IPO Mar 2021). _classify_limit_type() correctly returns
available_lifetime_under_10y for 1656 bars (within our 10Y cap by design).
