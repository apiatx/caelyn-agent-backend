---
name: Predict investor intelligence layer
description: Event-family grouping, tracked odds, Hormuz misclassification fix, new /api/predict/investor/intelligence endpoint
---

## Root cause of Hormuz → China/Taiwan misclassification
`themes.py::china_taiwan_supply_chain.question_keywords` contained bare `"strait"` which substring-matched "Strait of Hormuz". Fix: changed to `"taiwan strait"`. Result: Hormuz markets now only land in `geopolitics_war_trade`, not both clusters simultaneously.

## New files
- `services/predict/investor/event_grouping.py` — 25 canonical event family rules; `make_event_family_key(question)` collapses "Hormuz by Jul 15" + "Hormuz by Jul 31" into single `hormuz_iran` key. Stem-hash fallback for unmatched questions.
- `services/predict/investor/tracked_odds.py` — 19 permanent macro/market families (Fed, recession, SPX, NVDA/TSLA/AAPL milestones, BTC, oil, gold, tariffs, CPI, Hormuz, Russia-Ukraine). `build_tracked_odds(all_markets)` needs **unfiltered** market list (`get_top_markets(300)`) not scored (sports-filtered) list.

## investor_intel.py additions (module-level)
- `_INTELLIGENCE_CACHE_TTL = 2100` (35 min, pre-warmed every 30 min)
- `_TICKER_TO_SECTORS` reverse map built at import from `SECTOR_STOCKS`
- `_get_watchlist_symbols()` — reads `watchlist_store.json` via `_read_store()` + `extract_tickers()`; returns empty set on error (graceful degrade)
- `_resolve_ticker_impacts(bullish_sectors, bearish_sectors, watchlist_syms)` — watchlist symbols appearing in SECTOR_STOCKS for affected sectors take priority; SECTOR_STOCKS static lists are fallback only
- `_build_equity_signals(equity_relevant, watchlist_syms)` — groups equity-relevant markets by event_family_key, one signal per family; different from theme-cluster-centric `_build_top_equity_signals`

## New endpoint
`GET /api/predict/investor/intelligence` — in `services/predict/investor/router.py`. Returns: `tracked_odds` (19 families, null-stubbed if not live), `equity_signals` (event-family-grouped), `diagnostics` (watchlist_hits, duplicate_markets_collapsed, build_time_ms, theme_universe_source).

## Background loop
`_investor_intelligence_loop()` in `main.py` — 2-min startup delay (lets Polymarket scored-markets cache warm first), then 30-min cadence. Registered with `asyncio.create_task()` in startup block after `_home_planning_warmup_loop`.

**Why:** Existing endpoints (/overview, /themes, /regime, /watchlists) are all on-demand with 90-150s TTL. The new intelligence endpoint is heavier (parallel fetches) so needs a background pre-warm loop.

## Backward-compat
All existing investor endpoints unchanged. The new endpoint is purely additive.

## How to apply
When adding new Polymarket event families: add to `_FAMILY_RULES` in `event_grouping.py` (for grouping) AND `_TRACKED_FAMILIES` in `tracked_odds.py` (for permanent monitoring). Both files use OR-logic keyword matching — first match wins.
