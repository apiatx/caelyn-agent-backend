---
name: Tracked Odds Registry architecture
description: Two-lane Polymarket discovery, Neon persistence, delta history, and integration with investor intelligence.
---

## The rule
Three-layer pipeline: `odds_registry.py` (26 families) → `odds_scanner.py` (two-lane fetch/match/persist) → `predict_odds_store.py` (Neon table). `get_intelligence()` consumes the scanner payload and falls back to `build_tracked_odds()` only on cold start.

**Why:** Single broad scan only matched 2/26 families — FIFA World Cup 2026 dominated the top-400-by-volume pool, burying macro/finance markets below the cut.

## Critical: Gamma API q= param is non-functional
`GET /markets?q=<query>` completely ignores the query text and returns the same top-by-volume results regardless. Do NOT attempt text-search via Gamma. The only working targeted approach is tag-based: `GET /events?tag_slug=<slug>` via `get_top_markets(limit=N, tag=X)`.

## Two-lane discovery (current implementation)
- **Lane A**: `get_top_markets(limit=400)` — untagged, sorted by 24h volume
- **Lane B**: 6 parallel `get_top_markets(limit=N, tag=X)` calls:
  - `economy(300)` → Fed decisions, recession, CPI, Hormuz
  - `finance(300)` → stocks, oil, gold, earnings, Fed cuts
  - `crypto(200)` → Bitcoin milestones
  - `geopolitics(200)` → Russia/Ukraine, China/Taiwan, Israel
  - `tech(150)` → AI/chip export controls
  - `politics(150)` → macro policy, tariffs
- Merge by `condition_id` dedup (Lane A wins), then sports-filter, then keyword match
- Result: 2/26 → 15/26 families matched; 3 via broad, 12 via targeted

## Payload structure (after two-lane upgrade)
- `odds[]` — **live matched families only** (no null stubs); sorted by priority
- `missing_families[]` — unmatched family metadata (family_key, label, category, etc.)
- `_scan_diag{}` — full diagnostics: broad/targeted/merged counts, per-lane match lists, sports_excluded_count, snapshots_written

## Keyword pattern gotchas
Real Polymarket question phrasing often doesn't match naive keyword assumptions:
- "no change in Fed interest rates after the July meeting" → need "fed interest rate", "interest rates after", "no change in fed"
- "Will no Fed rate cuts happen in 2026?" → "fed rate cut" (IS substring of "fed rate cuts") works; "cuts in 2026" does NOT match "cuts happen in 2026"
- "Gold (GC) hit (HIGH) $8,000" → need "gold (gc)" or "(gc) hit" — "gold hit" fails because "(GC)" sits between "gold" and "hit"
- "WTI Crude Oil (WTI) hit (LOW) $20" → need "wti hit", "crude (wti)", "crude oil (wti)"
- "annual inflation be 3.6% or less" → need "annual inflation"; "inflation above/below" don't match
- "NVIDIA be the largest company by market cap" → need "nvidia largest", "nvidia market cap", "nvidia be the"
- "Tariff increase on Canada in effect by June 30" → need "tariff increase", "tariff on", "tariff in effect"

## Why families still go missing
Low match counts after two-lane discovery are correct behavior in these cases:
1. **June-end expiry**: Markets for "June" events are `is_resolving` (within 72h of end date) by late June and get correctly filtered
2. **No live market**: Earnings, SPX/Nasdaq daily direction, AMD/GOOGL milestones only exist near their events
3. **Volume below tag limits**: A market may exist but not appear in the top N for its tag

## Loop timing
`_odds_scanner_loop` starts at 90s; `_investor_intelligence_loop` at 120s. Keep gap ≥30s.

## Delta fallback chain
1. DB history (`get_snapshots_before` with window tolerance)
2. Polymarket's own `price_change_1h/1d/1wk` API fields
3. None (returned as null)

## Neon table
`public.prediction_market_odds_snapshots` — BIGSERIAL PK, indexed on (family_key, captured_at DESC). 7-day retention on every scan cycle.

## File locations
- Registry: `backend/services/predict/odds_registry.py`
- Store: `backend/data/predict_odds_store.py`
- Scanner: `backend/services/predict/odds_scanner.py`
- Endpoints: `backend/services/predict/router.py`
- Loop: `_odds_scanner_loop()` in `backend/main.py`
- Intelligence integration: `backend/services/predict/investor/investor_intel.py` (~line 266)
- Old pure-function stub preserved: `backend/services/predict/investor/tracked_odds.py`

## How to apply
- **Add new families**: Edit `ODDS_REGISTRY` list in `odds_registry.py` only. Scanner picks it up automatically.
- **Fix missing patterns**: Probe real Gamma API questions for the family's tag, then add exact substring patterns. Test with: `curl "https://gamma-api.polymarket.com/events?active=true&tag_slug=<tag>&limit=50"`.
- **Add new Lane B tag**: Append to `_LANE_B_TAGS` in `odds_scanner.py` — format `(tag_label, max_markets)`.
- **Debugging missing families**: Check `/api/predict/odds/diagnostics` for `families_still_missing`, `merged_candidates_seen`, `sports_excluded_count`.
- **Delta windows**: Edit `_DELTA_*` constants in `odds_scanner.py`.
