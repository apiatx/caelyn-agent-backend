---
name: Watchlist options freshness
description: Why stale LKG rows require a direct live-scan drain, not scan_portfolio_options. Covers the three TTL buckets, no_expirations vs no_chain_returned, force_refresh path, and the foreign/OTC preflight.
---

## Foreign/OTC preflight — classify before any Tradier call
_is_otc_or_foreign(sym) returns True for:
  • ":" in sym  — any EXCHANGE:TICKER format (AIM:ENSI, ASX:AXE, OTC:ATEYY, TSX:FLT, …)
  • len(sym) >= 5 and sym[-1] in ("F","K","Y")  — OTC pink-sheet suffixes

Applied in THREE places:
  1. scan_watchlist_options cache-first loop — at top of per-symbol loop, before cache read
  2. _drain_stale_lkg batch prep — skip before quote batch
  3. _do_scan in scan_portfolio_options — after price=0 fallback

**Why:** EXCHANGE:TICKER format symbols are never served by Tradier options APIs. Before this fix, they reached the quote batch and wasted rate-limit budget (54 colon-prefix + 113 OTC-suffix = 167 confirmed unsupported out of 302 symbols).

## TTL buckets (module constants)
_UNAVAIL_TTL_CONFIRMED = 86400  (24h): no_options, no_expirations, not_in_tradier_coverage, otc_or_foreign_unsupported
_UNAVAIL_TTL_TRANSIENT = 1800   (30min): provider_rate_limited, no_chain_returned, unknown_provider_error

## Expiration retry in _scan_one_symbol
Tries up to 4 expirations in groups of 2 (stops early when any contracts found).
Near-term expirations can be thin/empty on small-cap names — old code only tried 2.
first_chain is set in the inner loop to the first chain that has contracts.

## no_expirations vs no_chain_returned
  no_expirations   — Tradier returned 0 expirations (confirmed no options chain, 24h TTL)
  no_chain_returned — expirations exist but all tried chains returned empty (transient, 30min TTL)

## Stale LKG detection and drain
_STALE_LKG_REFRESH_AGE = 3600 (1h): LKG rows older than 1h are added to stale_lkg_to_refresh
_drain_stale_lkg: DIRECT live-scan path (bypasses scan_portfolio_options cache-first + disk LKG fallback)
  - Evicts memory cache entry before scan
  - Writes fresh results with _CACHE_PER_TICKER_TTL * 2 (600s) — 2× so entries outlast drain duration

## Stale vs fresh classification
_FRESH_LKG_THRESHOLD = 1800 (30 min): LKG entries written within last 30 min are fresh_option_data
stale_set = uncached | inflight | {s: from_lkg AND lkg_age > 1800s}
**Why:** Memory cache TTL is 300s. Drain writes batch-1 entries then takes 2-3 min to complete. By the time user re-calls after drain, batch-1 entries may have expired from memory and disk LKG re-serves them as from_lkg=True. Using age-based stale detection (>30 min) avoids false stale classification for recently drained entries.

## force_refresh=True
In scan_watchlist_options cache-first pass:
  if hit is LKG entry (from_lkg=True or source==portfolio_opts_lkg_disk): hit = None
  if hit is transient failure (provider_rate_limited, no_chain_returned, scan_pending): hit = None
Symbol falls through to uncached → re-queued for live scan.
Foreign/OTC symbols are NOT evicted by force_refresh (preflight runs before force_refresh check).

## Classification labels (options_classification field)
fresh_option_data, stale_option_data, confirmed_no_options, unsupported_foreign_or_otc,
transient_failure, queued_for_refresh, inflight_refresh, needs_retry

## Refresh tracking (module-level globals)
_WL_LAST_REFRESH_STARTED_AT, _WL_LAST_REFRESH_COMPLETED_AT, _WL_LATEST_SUCCESSFUL_REFRESH_AT
Exposed in options_meta as last_full_watchlist_refresh_started/completed_at, latest_successful_refresh_at.
