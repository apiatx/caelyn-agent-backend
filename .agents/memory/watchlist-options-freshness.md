---
name: Watchlist options freshness
description: Why stale LKG rows require a direct live-scan drain, not scan_portfolio_options. Covers the three TTL buckets, no_expirations vs no_chain_returned, and the force_refresh path.
---

## The problem
scan_portfolio_options is cache-first with a disk-LKG fallback:
  per-ticker memory cache → master snap → disk LKG → uncached/live scan

After evicting memory cache, scan_portfolio_options re-loads from disk LKG.
_drain_stale_lkg MUST bypass this by calling _scan_one_symbol directly
(quote batch → semaphore-gated chain scans → write to cache + disk LKG).

## TTL buckets (module constants)
_UNAVAIL_TTL_CONFIRMED = 86400  (24h): no_options, no_expirations, not_in_tradier_coverage, otc_or_foreign_unsupported
_UNAVAIL_TTL_TRANSIENT = 1800   (30min): provider_rate_limited, no_chain_returned, unknown_provider_error

## no_expirations vs no_chain_returned
_scan_one_symbol returns:
  no_expirations   — Tradier returned 0 expirations (confirmed no options chain, 24h TTL)
  no_chain_returned — expirations exist but chain fetch returned empty (transient, 30min TTL)

## Stale LKG detection
_STALE_LKG_REFRESH_AGE = 3600 (1h)
LKG rows older than 1h are collected in stale_lkg_to_refresh and enqueued to
_drain_stale_lkg at the end of scan_watchlist_options.

## force_refresh=True
In scan_watchlist_options cache-first pass:
  if hit is LKG entry (from_lkg=True or source==portfolio_opts_lkg_disk): hit = None
  if hit is transient failure (provider_rate_limited, no_chain_returned, scan_pending): hit = None
Symbol falls through to uncached → re-queued for live scan.

## Classification labels (options_classification field)
fresh_option_data, stale_option_data, confirmed_no_options, unsupported_foreign_or_otc,
transient_failure, queued_for_refresh, inflight_refresh, needs_retry

## Refresh tracking (module-level globals)
_WL_LAST_REFRESH_STARTED_AT, _WL_LAST_REFRESH_COMPLETED_AT, _WL_LATEST_SUCCESSFUL_REFRESH_AT
Exposed in options_meta as last_full_watchlist_refresh_started/completed_at, latest_successful_refresh_at.

## After-hours behavior
After market close, ~40% of US symbols get no_chain_returned (transient, 30-min TTL).
They are correctly classified as transient_failure and will retry after TTL.
This is expected — options chains are thin/absent after hours.
