---
name: Confluence Purity + Catalyst Intelligence Audit
description: Full static audit results — Confluence call graph, Tradier attribution, Catalyst V1 behavior, RSS pipeline, and V2 architecture proposal
---

## Confluence Call Graph — Purity Verdict
`build_confluence_snapshot()` is PURE: zero HTTP/provider calls.

All reads are:
- Disk JSON: Stage2 LKG, Options LKG, Social Map (`x_consensus_weekly.json`)
- In-memory: ENRICHED_THEME_RS_UNIVERSE, Entry State LKG, `options_master_screener_v1` cache, `earnings:curated:week:*` cache
- Neon SELECTs: `watchlist_category_overrides`, `theme_ticker_overrides`, `watchlist_fundamentals_cache`, `options_net_premium_daily`, `calendar_snapshots`, `watchlist_rss_article_archive`
- Pure math: compute_investment_alignment, _compute_theme_alignment, _compute_elite_asset_rebound, _compute_actionability

## Tradier Throttling Attribution (CORRECTED)
`[TRADIER_LIMITER] X/110 calls in window` is emitted from `backend/data/tradier_provider.py:85`.
The throttling during startup is caused by concurrent background loops:
- `_master_screener_loop()` (main.py:576) — options chains, `options_flow` lane
- `_theme_options_supplement_loop()` (main.py:578) — options chains, `maintenance` lane
- `warmup_theme_rs()` (main.py:931) — bypasses TRADIER_LIMITER via `_TRADIER_GLOBAL_SEM`
- `warmup_stage2_all_watchlists()` (main.py:942) — daily bars, `quotes` lane

**Why:** None of these loops are called from inside `build_confluence_snapshot()`. Attribution "Tradier throttling = Catalyst Alignment" is INCORRECT.

## Catalyst Alignment V1 — Key Facts
- File: `backend/services/catalyst_alignment.py`
- 3 data sources (all zero-HTTP): earnings cache (memory), calendar_snapshots (Neon), watchlist_rss_article_archive (Neon)
- 6-regex `_NEWS_CATEGORIES`: MA_NEWS(80), REGULATORY_NEWS(75), GUIDANCE_NEWS(60), LEGAL_NEWS(55), CORPORATE_ACTION(50), ANALYST_ACTION(45)
- Score assembly: `max(event.score)` — NOT a weighted average
- News lookback: 96 hours
- Catalyst weight in THEME_ALIGNMENT archetype: 25% (_TA_W_CATALYST=25.0)
- `news_signal_scorer.py` (11-type business-aware classifier) is NOT connected to catalyst_alignment — only used in watchlist_router.py display path

## RSS Pipeline
- Only ONE RSS pipeline: `watchlist_rss_sweeper.py` (Yahoo + Google, ~120s cycle)
- Archive: Neon `watchlist_rss_article_archive`
- Two readers: (1) `catalyst_alignment.py`, (2) `watchlist_router.py` → `news_signal_scorer.py`
- NotifAI uses `web_search.get_market_news()` (Perplexity/Claude) — NOT an RSS pipeline

## Catalyst V1 Gaps (8 documented)
1. No hyperscaler deal detection (18-entity detector exists in news_signal_scorer.py but unwired)
2. No government/defense contract detection
3. No technical milestone detection
4. No financing/dilution event detection
5. No direction field (bullish/bearish) on events
6. No materiality scoring (flat category constant, no volume/recency weighting)
7. No confidence field
8. No days-until-catalyst for non-earnings events

## V2 Architecture — Zero New Provider Calls
2-file change: `catalyst_alignment.py` + add `classify_articles_bulk()` to `news_signal_scorer.py`
- Replace inline 6-regex with news_signal_scorer classifier
- Add direction lookup table per event type
- Add materiality = category_base × recency_factor × article_count_factor
- Score assembly → weighted average (not max)
- Sector multiplier via fundamentals_map already loaded in build_confluence_snapshot (step 10)
- Shadow V2 fields under `catalyst_v2_*` prefix for A/B validation

## Full audit report location
`.local/state/confluence_audit_report.md`
