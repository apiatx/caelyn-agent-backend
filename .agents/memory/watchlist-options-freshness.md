---
name: Watchlist options freshness
description: Why stale LKG rows require a direct live-scan drain, not scan_portfolio_options. Covers all TTL buckets, stale/fresh thresholds, not_in_tradier_coverage pitfall, foreign/OTC preflight, and force_refresh eviction.
---

## Foreign/OTC preflight — classify before any Tradier call
`_is_otc_or_foreign(sym)` returns True for:
  - ":" in sym  — any EXCHANGE:TICKER format (AIM:ENSI, ASX:AXE, OTC:ATEYY, TSX:FLT, …)
  - len(sym) >= 5 and sym[-1] in ("F","K","Y")  — OTC pink-sheet suffixes

Applied in THREE places:
  1. scan_watchlist_options cache-first loop — at top of per-symbol loop, before cache read
  2. _drain_stale_lkg batch prep — skip before quote batch
  3. _do_scan in scan_portfolio_options — after price=0 fallback

**Why:** EXCHANGE:TICKER format symbols are never served by Tradier options APIs. Before this fix, they reached the quote batch and wasted rate-limit budget (54 colon-prefix symbols confirmed unsupported out of 302 watchlist symbols).

## TTL buckets (module constants)
```
_UNAVAIL_TTL_CONFIRMED = 86400  (24h): otc_or_foreign_unsupported, no_expirations, no_options
_UNAVAIL_TTL_TRANSIENT = 1800   (30min): provider_rate_limited, no_chain_returned,
                                          not_in_tradier_coverage, unknown_provider_error
```

## CRITICAL: not_in_tradier_coverage is TRANSIENT, not confirmed
`not_in_tradier_coverage` means Tradier's batch quote endpoint returned no quote for the symbol.
This is TRANSIENT — it happens during cold-start burst scans when Tradier rate-limits
the quote batch and returns partial results. Valid US optionable stocks (AMD, AAOI, KLAC, TXN, etc.)
can get this reason during burst scans.

**Why this matters:** Giving `not_in_tradier_coverage` a 24h TTL caused 113 legitimate US stocks
to be stuck as `unsupported_foreign_or_otc` for 24h, hiding valid disk LKG data.
Fix: always use _UNAVAIL_TTL_TRANSIENT (30min) for `not_in_tradier_coverage`.

**Never** add `not_in_tradier_coverage` to the confirmed-TTL bucket:
```python
# WRONG:
ttl = _UNAVAIL_TTL_CONFIRMED if reason in (..., "not_in_tradier_coverage", ...) ...
# RIGHT:
ttl = _UNAVAIL_TTL_CONFIRMED if reason in ("no_options", "no_expirations",
                                            "otc_or_foreign_unsupported") ...
```

## Classification mapping (_classify_watchlist_sym)
```
data_available=True, LKG age < 1800s  → fresh_option_data
data_available=True, LKG age > 1800s  → stale_option_data
no_expirations / no_options           → confirmed_no_options
otc_or_foreign_unsupported            → unsupported_foreign_or_otc
not_in_tradier_coverage               → transient_failure  (NOT unsupported!)
scan_in_progress                      → inflight_refresh
scan_pending / stale_lkg_queued       → queued_for_refresh
provider_rate_limited / no_chain_returned / unknown_provider_error → transient_failure
```

## Diagnostics counters
`confirmed_unsupported_count` — ONLY counts `otc_or_foreign_unsupported` (genuine foreign/OTC)
  Do NOT include `not_in_tradier_coverage` here.
`transient_failure_count` — counts: provider_rate_limited, no_chain_returned, not_in_tradier_coverage
`confirmed_no_options_count` — counts: no_options, no_expirations

## force_refresh eviction list
`not_in_tradier_coverage` is included in the force_refresh eviction list:
```python
_is_transient = hit.get("unavailable_reason") in (
    "provider_rate_limited", "no_chain_returned", "scan_pending",
    "not_in_tradier_coverage"
)
```

## Stale LKG detection and drain
_STALE_LKG_REFRESH_AGE = 3600 (1h): LKG rows older than 1h → added to stale_lkg_to_refresh
_FRESH_LKG_THRESHOLD = 1800 (30min): LKG entries < 30min old → fresh_option_data (not stale)

_drain_stale_lkg: DIRECT live-scan path (bypasses scan_portfolio_options cache-first + disk LKG)
  - Evicts memory cache entry before scan
  - Writes fresh results with _CACHE_PER_TICKER_TTL * 2 (600s) — 2× so entries outlast drain duration

## Expiration retry in _scan_one_symbol
Tries up to 4 expirations in groups of 2 (stops early when any contracts found).
Near-term expirations can be thin/empty on small-cap names.

## no_expirations vs no_chain_returned
  no_expirations   — Tradier returned 0 expirations → confirmed no options chain, 24h TTL
  no_chain_returned — expirations exist but all tried chains empty → transient, 30min TTL

## Refresh tracking (module-level globals)
_WL_LAST_REFRESH_STARTED_AT, _WL_LAST_REFRESH_COMPLETED_AT, _WL_LATEST_SUCCESSFUL_REFRESH_AT
Exposed in options_meta as last_full_watchlist_refresh_started/completed_at, latest_successful_refresh_at.
