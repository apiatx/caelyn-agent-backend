---
name: Watchlist RSS news data path
description: Architecture for parallel Yahoo+Google RSS collection, Neon archive, and the three /news additive fields (ticker_activity, hyperscaler_articles, rss_activity_meta)
---

## Rule
Watchlist Live News uses a parallel Yahoo+Google RSS path — never a fallback chain for
the activity archive. FMP is only a display fallback (empty RSS) and is never called for
24h activity counts. fmp_requests_for_activity is always 0.

## Key files
- `backend/data/rss_article_archive.py` — Neon module, table `watchlist_rss_article_archive`
- `backend/services/watchlist_rss_sweeper.py` — background sweeper loop (~120s cadence)
- `backend/services/watchlist_service.py` — `_fetch_merged_rss_for_ticker` (concurrent Yahoo+Google), `_fetch_yahoo_rss`, `_fetch_google_news_rss`
- `backend/services/watchlist_router.py` — `_attach_live_fields`, `_build_ticker_activity_list`, `_build_hyperscaler_articles`, `_coverage_status`

## Architecture
- Sweeper: `asyncio.Semaphore(15)`, full universe ~22s for 370 tickers
- DB table: `(ticker, article_key) PRIMARY KEY`; `rss_providers TEXT[]` unioned on conflict
- `article_key` = `_cluster_key(title, url)` from `news_major_service` (shared canonical dedup)
- FMP fallback articles: `rss_providers=[]` → excluded from upsert by gate in `upsert_article_associations`
- Prune every 10 sweeps (72h retention)

## /news response new fields
- `ticker_activity` — list of {ticker, articles_24h, previous_articles_24h, delta_count, delta_pct, delta_label, coverage_status} per watchlist ticker. Neon SELECT per request (cheap, no provider call).
- `hyperscaler_articles` — filtered from enriched_map by catalyst_type=="hyperscaler_anchor", deduped by cluster_key, sorted by score desc.
- `rss_activity_meta` — from `get_sweeper_meta()`: providers, windows, last sweep ts, duration.

## delta_pct semantics
- prev > 0: `((cur - prev) / prev) * 100`
- prev == 0 and cur == 0: `0.0`
- prev == 0 and cur > 0: `delta_pct=None`, `delta_label="new"`

## coverage_status
- `complete`: oldest_pub_ts >= 48h ago (full previous window)
- `provider_partial`: 24h–48h of history
- `warming`: < 24h of history or no archive rows

**Why:** To support three frontend toggles (NEWS ACTIVITY, ALL NEWS, HYPERSCALER DEALS)
from one shared response. RSS-only constraint ensures activity counts are
reproducible and FMP never inflates 24h counts with non-RSS articles.

**How to apply:** When modifying the news path — keep `fmp_requests_for_activity=0` as
a hard constraint. The sweeper is registered exactly once in lifespan. Never add FMP calls
to `run_rss_sweep()` or `_sweep_ticker()`. The LKG cache stores only articles; the three
new fields are always computed fresh on each request via `_attach_live_fields`.
