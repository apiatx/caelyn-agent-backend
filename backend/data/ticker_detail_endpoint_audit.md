# Ticker Detail Endpoint — Full 9-Part Audit & Validation Report
**Date:** 2026-07-15  
**Endpoint:** `GET /api/watchlist/ticker-detail/{symbol}`  
**Status:** IMPLEMENTED AND VALIDATED

---

## Part 1 — Existing Endpoint Inventory

| Route | File | Relevant data |
|-------|------|---------------|
| `GET /api/watchlist/{watchlist_id}` | `watchlist_router.py` | Full ticker rows (stage2, quotes, themes, options overlay) |
| `GET /api/alpha/confluence/{symbol}` | `watchlist_router.py` | V4.2 snapshot for single symbol (live build) |
| `GET /api/watchlist/{watchlist_id}/news` | `watchlist_router.py` | All news for a watchlist |
| `GET /api/watchlist/{watchlist_id}/news/ticker/{ticker}` | `watchlist_router.py` | Per-ticker news from LKG only |

No single endpoint previously exposed company + confluence + technical + fundamentals + news + catalyst for one symbol.

---

## Part 2 — Company Profile & Description Sources

| Field | Source | Function | Notes |
|-------|--------|----------|-------|
| name, sector, industry, market_cap, exchange, country, beta | Neon `screener_fundamentals_cache` | `get_company_profile_cached(sym)` | 7-day TTL |
| description (up to ~1763 chars) | Neon `screener_fundamentals_cache` | `get_fundamentals_cached(sym).get('profile',{}).get('description')` | Confirmed for VRT, ENTG, etc. |
| website, image, ceo | Same Neon profile record | Same `get_fundamentals_cached` path | |

**Gap:** MU and TSM have no `description` in the FMP profile cache — upstream FMP profile fetch did not store it for those tickers. Not a code bug; data-completeness issue.

---

## Part 3 — Quote / Overview Source

Source: `get_watchlist_quotes([sym])` (async, in-memory, zero Tradier calls if already cached).  
Coverage: Only symbols currently being polled by the active watchlist. Non-watchlist symbols (or after restart) return `price=None`. Expected behavior for a zero-provider-call endpoint.

---

## Part 4 — Confluence V4.2

**Fast path:** `get_retained_confluence_snapshot()` → extract by symbol (dict lookup, microseconds).  
**Fallback:** `get_confluence_for_symbol(sym)` → fresh per-symbol build (~5s).

Previously the endpoint called `get_confluence_for_symbol` directly, causing 10s+ timeouts for symbols not pre-built. Fixed to use retained snapshot first.

All V4.2 component fields are passed through: score, bucket, actionability, options_alignment_points, catalyst_alignment_points, catalyst_event_type, catalyst_event_tier, all bonus fields.

---

## Part 5 — Technical Data Source

Source: `get_stage2(sym)` — pure in-memory dict lookup from `_STAGE2_LKG` (413 symbols, 20h TTL).

Fields exposed:
- Weinstein stage: score, label, reason, stage_confidence
- Core TA: ma_stack, pct_vs_{20d/50d/200d}, extension_risk, range_position_52w, pct_from_52w_{high/low}, high/low_52w, sma_{20/50/200}
- Entry: entry_zone, breakout_signal, high_{20d/50d}
- Momentum: momentum_trend, roc_{20d/50d}, squeeze_signal
- Volume: avg_volume_20d, accumulation_distribution_{signal/score}, atr_14_pct
- State: technical_state, technical_timing_score, technical_state (canonical)
- Options overlay from V4.2: opt_score, opt_signal
- Live price/vol from quote cache: price, change_percent, volume, relative_volume

---

## Part 6 — Fundamentals Source

Source: `watchlist_fundamentals_store.get_snapshot(sym)` → Neon `watchlist_fundamentals_cache` table.  
Returns `.fields` dict (all FMP fundamental fields refreshed on cadence-controlled schedule).

`revenue=None` observed for all validation symbols — this is because the FMP fundamentals refresh stores fields like `pe_ratio`, `pb_ratio`, `debt_to_equity`, `roe`, etc. in the `fields` JSONB column; `revenue` as a raw field is not part of the current schema. The fundamentals section correctly exposes all fields stored.

---

## Part 7 — News Sources

Three layers, in priority order:
1. **Module-level `_news_lkg`** — in-memory dict keyed by watchlist_id. Lookup: `_news_lkg["default"]["data"]["articles"][sym]`.
2. **`_HYP_CACHE`** — hyperscaler article cache, filtered to articles where `sym in article["watchlist_symbols"]`.
3. **Neon fallback** — `rss_article_archive.query_ticker_activity_articles(sym, 96)` when in-memory LKG is empty (96h window, 2 SQL queries, sync via `to_thread`).

---

## Part 8 — Direct Catalyst Source

Two sources merged:
1. **Raw event fields**: `_read_catalyst_lkg_sym(sym)` — reads `data/catalyst_alignment_lkg.json` (disk, lazy-cached path). Fields: title, url, published_at, materiality_score, confidence_score, ticker_relevance_score, why_it_matters, primary_subject, catalyst_date, days_until.
2. **Phase B scored fields** from V4.2 result (retained snapshot): catalyst_event_tier, catalyst_freshness_score, catalyst_relevance_score, catalyst_materiality_score, catalyst_reason_codes, catalyst_explanation, direct_catalyst_present.

**Article annotation**: News articles matching catalyst title/url are annotated with `is_direct_catalyst=True` and all Phase B fields. If the catalyst article is outside the 96h news window, a pseudo-article is synthesized from the LKG record so the frontend always has something to display.

---

## Part 9 — Validation Results (2026-07-15)

| Symbol | C42 Score | Stage | News Arts | DC Tier | Coverage OK | Coverage FAIL |
|--------|-----------|-------|-----------|---------|-------------|----------------|
| ENTG | 55.6 | S2-S3 Advance | 12 | — (no catalyst) | company_profile, description, quote, confluence_v42, technical, fundamentals, news | direct_catalyst |
| MU | 53.3 | S3 Momentum | 292 | TIER_E | company_profile, confluence_v42, technical, fundamentals, news, direct_catalyst | description¹, quote² |
| TSM | 73.0 | S2-S3 Advance | 141 | TIER_A | company_profile, confluence_v42, technical, fundamentals, news, direct_catalyst | description¹, quote² |
| ABCL | 37.8 | S3 Momentum | 2 | — | company_profile, description, confluence_v42, technical, fundamentals, news | quote², direct_catalyst |
| ALGM | 63.6 | S2-S3 Advance | 3 | TIER_C | company_profile, description, confluence_v42, technical, fundamentals, news, direct_catalyst | quote² |
| VRT | 66.5 | S2-S3 Advance | 29 | TIER_B | company_profile, description, confluence_v42, technical, fundamentals, news, direct_catalyst | quote² |
| HIMS | 43.3 | S2 Breakout | 18 | — | company_profile, description, confluence_v42, technical, fundamentals, news | quote², direct_catalyst |
| CRWD | 63.7 | S3 Momentum | 65 | TIER_C | company_profile, description, confluence_v42, technical, fundamentals, news, direct_catalyst | quote² |

¹ `description: False` — FMP profile cache did not store description for MU/TSM (data gap, not a code bug).  
² `quote: False` — expected; quote cache only populated for symbols actively polled in current watchlist session.

---

## Implementation Notes

- Route registered at line 3523 in `services/watchlist_router.py`, before the `/{watchlist_id}` catch-all (line ~3910+).
- `_CAT_LKG_PATH` is a module-level lazy variable (not re-resolved on every request).
- All blocking I/O (Neon queries, disk reads) wrapped in `asyncio.to_thread`.
- All sections are independently try/except guarded — one data source failure never blocks the others.
- Zero provider calls (Tradier, FMP, Gemini, etc.) by design.
