---
name: Tracked Odds Registry architecture
description: Full-catalog Polymarket discovery, Neon persistence (catalog + snapshots), CLOB price enrichment, and Prophetik integration.
---

## The rule
Four-layer pipeline:
1. `odds_registry.py` — 26 family definitions (keyword patterns, categories, priorities)
2. `odds_scanner.py` — full catalog crawl → match → CLOB enrich → snapshot persist
3. `predict_odds_store.py` — Neon table `prediction_market_odds_snapshots` (7-day retention)
4. `predict_market_catalog_store.py` — Neon table `prediction_market_catalog` (full crawl persistence)

**Why:** Top-N and tag-limited scans cannot define the search universe — they miss lower-volume macro/finance markets dominated by sports when sorted by volume.

## Critical: Gamma /events page cap = 100 (not 500)
The Gamma `/events` endpoint silently caps responses at **100 events per page** regardless of the `limit` parameter you send. Always use `PAGE_SIZE=100` for correct pagination detection:
```python
if len(events_page) < PAGE_SIZE:
    break   # last page — no more data
```
With `limit=500`, the page always contains ≤100 events, so `len < 500` is always True and you stop after page 1 — getting only the top 100 events (all FIFA during World Cup season).

**Why this matters:** FIFA World Cup 2026 fills the top 100+ events by volume. Finance/macro markets (NVDA, gold, recession, bitcoin) only appear on pages 2-21.

## Critical: Gamma /events q= query param is non-functional
`GET /markets?q=<query>` completely ignores query text and returns top-by-volume regardless. DO NOT attempt text-search via Gamma. Tag-based search (`/events?tag_slug=X`) works but coverage varies by tag.

## Full catalog crawl (current implementation)
```
fetch_full_active_catalog()  in polymarket_intelligence.py
  GET /events?active=true&closed=false&limit=100&offset=0
  GET /events?active=true&closed=false&limit=100&offset=100
  ...until len(page) < 100 (last page)
  Safety cap: MAX_PAGES=150 (15 000 events maximum)
```
- Flattens nested `markets[]` from each event
- Injects `event_slug` and `tags` (from parent event) onto each market dict
- Returns (flat_market_list, stats_dict)
- Current results: ~21 pages / ~2100 events / ~28,984 markets flattened / ~10,011 non-sports

## Active market filter (_is_active_raw)
Run on raw Gamma event-nested market dicts BEFORE keyword matching:
- **Exclude if `closed=True`** (hard stop)
- **Exclude if `acceptingOrders` is EXPLICITLY False** — do NOT exclude if null/missing; Gamma often omits this field from event-nested dicts (missing = treating as accepting)
- **Exclude if `endDate` is within 72h** (resolving) or in the past (expired)

## CLOB price enrichment
```
GET https://clob.polymarket.com/midpoint?token_id=<YES_token_id>
Response: {"mid": "0.42"}
```
- YES token = `clob_token_ids[0]` from the market dict
- Public read, no authentication required
- Runs in parallel (asyncio.gather) for all matched families simultaneously
- Overrides Gamma outcomePrices when successful
- Track: `clob_price_success_count` / `clob_price_fail_count` / `gamma_price_fallback_count`

## Neon catalog table
`public.prediction_market_catalog` — condition_id PK, question, description, tags, active, closed, accepting_orders, end_date, volume_24h, liquidity, clob_token_ids, outcome_prices, raw_json, discovered_at, updated_at, last_seen_at, is_currently_discovered.

Upsert: chunked executemany (500/batch), INSERT ON CONFLICT DO UPDATE, `last_seen_at=NOW()`.
Stale detection: `UPDATE SET is_currently_discovered=false WHERE last_seen_at < crawl_started_at - 60s`.
Fallback: `get_active_catalog_rows()` reads last-good crawl if live crawl fails.

## Diagnostics spec (all 16 required fields)
```
catalog_rows_total, catalog_rows_current_active,
catalog_events_pages_fetched, catalog_markets_flattened,
catalog_last_full_crawl_at, catalog_full_crawl_success,
registry_family_count, live_family_count, missing_family_count,
family_matches_from_full_catalog, family_matches_from_tag_fast_path,
candidate_pool_size, sports_excluded_count,
snapshots_written, snapshots_retained_days,
clob_price_success_count, clob_price_fail_count,
gamma_price_fallback_count, hardcoded_sector_stocks_used
```

## Live endpoint response shape (spec)
```json
{
  "updated_at": "ISO",
  "cache_age_seconds": N,
  "live_count": N,
  "tracked_count": 26,
  "odds": [...live matched families only...],
  "missing_families": [...tracked but unmatched...],
  "diagnostics": {...all 16+ fields...}
}
```
Backward-compat aliases also present: `scanned_at`, `total_families`, `matched_families`.

## Why families go missing (correct behavior)
1. **End-of-month expiry**: Markets with `endDate` ≤72h from now are filtered as resolving (e.g., June 30 markets filtered on June 28)
2. **No active market**: SPX/Nasdaq/Dow daily direction, AMD milestones, earnings (NVDA/TSLA), AI export controls — only created near their specific events
3. **True absence**: Some families have no corresponding Polymarket market in any event

## Match count history
- Original (2 lane): 2/26
- Two-lane (Lane A top-400 + Lane B 6 tags): 15/26
- Full catalog crawl (correct pagination): **17/26**

## File locations
- Registry: `backend/services/predict/odds_registry.py`
- Snapshot store: `backend/data/predict_odds_store.py`
- Catalog store: `backend/data/predict_market_catalog_store.py`
- Scanner: `backend/services/predict/odds_scanner.py`
- Intelligence methods: `backend/services/predict/polymarket_intelligence.py` (fetch_full_active_catalog, get_clob_midpoint)
- Endpoints: `backend/services/predict/router.py`
- Loop: `_odds_scanner_loop()` in `backend/main.py` (90s startup, 30min cadence)
- Intelligence integration: `backend/services/predict/investor/investor_intel.py` (~line 266)

## How to apply
- **Add new family**: Edit `ODDS_REGISTRY` in `odds_registry.py` only.
- **Fix missing keywords**: Probe real markets with `curl "https://gamma-api.polymarket.com/events?active=true&closed=false&limit=100" | python3 -c "import json,sys; [print(m['question']) for ev in json.load(sys.stdin) for m in ev.get('markets',[])]"`. Use exact substring patterns.
- **Add new tags to scanner**: Not needed — full catalog crawl covers all tags.
- **Debugging missing families**: Check `/api/predict/odds/diagnostics` for `families_still_missing`, `catalog_events_pages_fetched`, `candidate_pool_size`.
- **Extend catalog schema**: Add columns to DDL in `predict_market_catalog_store.py`, bump `_DDL_APPLIED=False` temporarily to re-apply.
