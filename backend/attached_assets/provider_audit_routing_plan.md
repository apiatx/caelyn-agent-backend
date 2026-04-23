# Provider Audit & Routing Plan
## CaelynAI Backend — April 2026

---

## PHASE 1 — FULL AUDIT

### 1. ALPHA VANTAGE

**File**: `data/alphavantage_provider.py` (134 lines)
**Provider method**: `get_news_sentiment(ticker=None, topics=None)` — AI-powered news sentiment, 25 req/day free tier

**All call sites in active code** (`data/market_data_service.py`):

| Line | Function | Usage | Verdict |
|------|----------|-------|---------|
| 602 | `get_market_news_context` | `if self.fmp: …fmp.get_market_news()… else: alphavantage.get_news_sentiment(topics="financial_markets")` | DEAD-CODE FALLBACK — FMP always configured |
| 613 | `get_market_news_context` | Same else-branch | DEAD-CODE FALLBACK |
| 829 | `research_ticker` | `self.fmp.get_stock_news(ticker) if self.fmp else self.alphavantage.get_news_sentiment(ticker)` | DEAD-CODE FALLBACK |
| 2291 | `get_social_buzz` | `self.fmp.get_stock_news(ticker) if self.fmp else self.alphavantage.get_news_sentiment(ticker)` | DEAD-CODE FALLBACK |
| 2374 | `get_social_momentum` | Same if/else pattern | DEAD-CODE FALLBACK |

**Conclusion**: Alpha Vantage is 100% dead-code fallback. Every call site uses `if self.fmp … else self.alphavantage`. Since FMP_API_KEY is always configured, the `else` branch never executes. Alpha Vantage has effectively already been retired. No live data flows through it.

**Action**: DOCUMENT only. Optionally clean up dead else-branches in a separate pass. No routing change needed.

---

### 2. TWELVEDATA

**File**: `data/twelvedata_provider.py` (109 lines)
**Provider method**: `get_daily_bars(symbol, days=120)` — OHLCV daily bars, 8 req/min, 15-min circuit breaker

**All call sites in active code** (`data/market_data_service.py`):

| Lines | Function | Usage | Verdict |
|-------|----------|-------|---------|
| 455–484 | `get_candles` | **Primary** in the candle cascade: TwelveData → Finnhub candles → Polygon | KEEP — narrow candle use case |

**Cascade today**: TwelveData (primary, 8/min) → Finnhub `get_stock_candles` (fallback, 60-min circuit breaker on 403) → Polygon bars (final fallback).

**Can Tradier replace?** Tradier `get_history(symbol, interval="daily")` returns daily OHLCV bars with format `{date, open, high, low, close, volume}`. Output format is NOT the same key shape as the candle store format `{o, h, l, c, v, t}` used by TA calculations (ta_utils.py is commented "Used by Finnhub candles (primary) and Polygon bars (fallback)"). Switching the primary candle source to Tradier would require a format adapter and could silently break TA pipeline. **Risk: HIGH for LOW gain** since TwelveData already has a proper circuit breaker and rate limit guard.

**Action**: KEEP TwelveData as narrow primary candle source. Exactly the "narrow technical" niche the spec carves out.

---

### 3. FINNHUB

**Provider methods** (`data/finnhub_provider.py`, 466 lines):
- `get_quote(ticker)` — live quote `{price, change, change_pct, high, low, prev_close}`
- `get_company_profile(ticker)` — profile `{name, sector, industry, market_cap, logo, exchange, ipo_date}`
- `get_insider_sentiment(ticker)` — MSPR aggregate monthly insider sentiment (unique metric)
- `get_insider_transactions(ticker)` — raw insider transactions list
- `get_earnings_calendar(ticker)` — upcoming earnings
- `get_earnings_surprises(ticker)` — historical EPS surprises
- `get_recommendation_trends(ticker)` — historical analyst buy/hold/sell trend (4 quarters)
- `get_social_sentiment(ticker)` — Reddit + Twitter/X aggregate sentiment scores (unique)
- `get_company_peers(ticker)` — peer ticker list
- `get_upcoming_earnings()` — full market-wide earnings calendar for current week
- `get_stock_candles(ticker, days)` — daily OHLCV (candle fallback)
- `get_technicals(ticker)` — RSI/MACD/SMA from Finnhub scan
- `get_company_news(ticker, days)` — recent company-specific news articles

---

#### 3a. Finnhub usage — data/market_data_service.py (6460 lines)

| Usage | Lines | Function | Purpose | Verdict |
|-------|-------|----------|---------|---------|
| `get_stock_candles` | 486–503 | `get_candles` | Candle fallback after TwelveData | **KEEP FALLBACK** |
| `get_quote` | 753–758 | `research_ticker` | PRIMARY live quote for ticker deep-dive | **REASSIGN → Tradier** |
| `get_company_profile` | 759–764 | `research_ticker` | PRIMARY profile for ticker deep-dive | **REASSIGN → FMP** |
| `get_technicals` | 786 | `research_ticker` | RSI/MACD/SMA | **KEEP** (Polygon fallback exists) |
| `get_insider_sentiment` | 797 | `research_ticker` | MSPR aggregate | **KEEP — unique** |
| `get_insider_transactions` | 798 | `research_ticker` | Raw insider list | **KEEP** (acceptable — SEC Edgar has this too but Finnhub is already integrated) |
| `get_earnings_surprises` | 800 | `research_ticker` | Historical EPS beat/miss | **REASSIGN → FMP** (requires adding FMP earnings-surprises method) |
| `get_earnings_calendar` | 801 | `research_ticker` | Next earnings date | **REASSIGN → FMP** (requires adding FMP earnings-calendar per-ticker method) |
| `get_recommendation_trends` | 803 | `research_ticker` | Historical analyst trend | **KEEP — unique** (FMP has current ratings but not historical trend chart) |
| `get_social_sentiment` | 804 | `research_ticker` | Reddit/Twitter score | **KEEP — unique** |
| `get_company_peers` | 805 | `research_ticker` | Peer tickers | **REASSIGN → FMP** (requires adding FMP peers method) |
| `get_company_news` | 812 | `research_ticker` | Ticker-specific news (PRIMARY) | **REASSIGN → FMP** (`get_stock_news` already has title-compatible format) |
| `get_social_sentiment` | 2304 | `get_social_buzz` | Reddit/Twitter per trending ticker | **KEEP — unique** |
| `get_social_sentiment` | 2380 | `get_social_momentum` | Reddit/Twitter per trending ticker | **KEEP — unique** |

**Note on `research_ticker` news**: Finnhub `get_company_news` already maps `headline → title` in the provider. FMP `get_stock_news` also returns `title`. Both are compatible with the downstream sentiment keyword scan. FMP is a safer primary because it's the structured backbone provider.

---

#### 3b. Finnhub usage — data/caelyn_terminal.py (AI Terminal backend, 1449 lines)

| Usage | Lines | Function | Purpose | Verdict |
|-------|-------|----------|---------|---------|
| `get_earnings_surprises` | 648 | `_fetch_earnings_calendar` | Last EPS actual for portfolio holdings | **REASSIGN → FMP** (add `get_earnings_surprises` to FMP provider) |
| `get_earnings_calendar` | 656 | `_fetch_earnings_calendar` | Next earnings date for portfolio holdings | **REASSIGN → FMP** (add `get_earnings_calendar_for_ticker` to FMP provider) |
| `get_company_news` | 691 | `_fetch_news` | Portfolio ticker news (up to 4 symbols) | **REASSIGN → FMP** `get_stock_news` |

The AI Terminal is already correctly using **Tradier** for all equity quotes and daily history (`_fetch_tradier_quotes`, `_fetch_tradier_histories`). The Finnhub usage is limited to news and earnings — both can move to FMP.

---

#### 3c. Finnhub usage — data/options_flow_engine.py

| Usage | Lines | Function | Purpose | Verdict |
|-------|-------|----------|---------|---------|
| `get_upcoming_earnings()` | 413 | `_build_prefilter_context` | Options catalyst calendar (full market week) | **KEEP** — Finnhub's weekly earnings scan format is already integrated. Adding FMP earnings calendar here is a separate refactor with higher risk. |
| `get_technicals` | 651 | `_enrich_stock_candidate` | RSI/MACD for option candidate scoring | **KEEP** |
| `get_company_profile` | 652 | `_enrich_stock_candidate` | Sector/industry for candidate enrichment | **KEEP for now** — options flow enrichment is sensitive; Finnhub + FMP are already run in parallel here (safe) |
| `get_quote` | 653 | `_enrich_stock_candidate` | Live quote (already Finnhub + FMP parallel) | **KEEP** — already correct hybrid (both run, Tradier not yet wired here) |

---

#### 3d. Finnhub usage — services/sector_rotation/providers.py

| Usage | Function | Purpose | Verdict |
|-------|----------|---------|---------|
| `_finnhub_quote(ticker)` × 15 | `fetch_etf_quotes` | Real-time quotes for 13 sector ETFs + SPY + QQQ | **REASSIGN → Tradier** — exact use case the spec defines: "live multi-symbol quote panels" |

This is 15 individual Finnhub HTTP calls per refresh cycle, each with a rate-limit risk. Tradier's `get_quotes(symbols_list)` replaces all 15 with one batched call.

---

#### 3e. Finnhub usage — services/playbook/dilution_signals.py

| Usage | Lines | Function | Purpose | Verdict |
|-------|-------|----------|---------|---------|
| `fetch_company_news(ticker, finnhub_key)` | 372 | Direct httpx to Finnhub company-news API | 14-day news for playbook scoring (catalyst/dilution keyword scan) | **REASSIGN → FMP** — add FMP as primary, Finnhub as fallback |
| `fetch_earnings_calendar(ticker, finnhub_key)` | 402 | Direct httpx to Finnhub earnings calendar API | 90-day earnings proximity for catalyst scoring | **KEEP** — FMP per-ticker earnings calendar requires new endpoint and format normalization; the playbook test suite relies on the exact output; defer |

---

#### 3f. Finnhub usage — services/playbook/discovery_enrichment.py

| Usage | Function | Purpose | Verdict |
|-------|----------|---------|---------|
| `finnhub_profile` | batch profile enrichment for discovery candidates | Company profile including international tickers | **KEEP — INTENTIONAL** per spec ("international coverage") |
| `finnhub_news_recent` | recent news for shortlisted candidates | Discovery candidate news | **KEEP — INTENTIONAL** per spec |
| `finnhub_peers` | peer company list for candidates | Discovery peer context | **KEEP — INTENTIONAL** per spec |

The file header explicitly states: "Finnhub — primary: company profile, news, international metadata". This is correct design.

---

#### 3g. Finnhub usage — services/playbook/strategy_screener/screener_enrichment.py

| Usage | Function | Purpose | Verdict |
|-------|----------|---------|---------|
| `_try_finnhub(ticker, finnhub_key)` | Market cap lookup for screener candidates | **International ticker fallback** after FMP fails | **KEEP** — intentional international fallback |

Already correctly structured: FMP primary → Finnhub fallback for international market caps.

---

#### 3h. Finnhub usage — services/notifai_service.py

| Usage | Lines | Function | Purpose | Verdict |
|-------|-------|----------|---------|---------|
| `agent.data.finnhub.client.earnings_calendar` | 96, 269 | Weekly earnings digest | Full market earnings calendar for weekly briefing | **KEEP** — same as options_flow_engine rationale; the `finnhub.client` direct access pattern here is already established and the format is well-integrated |

---

#### 3i. Finnhub usage — services/insider_activity_service.py

| Usage | Lines | Function | Purpose | Verdict |
|-------|-------|----------|---------|---------|
| Finnhub quote + stock/metric | 727–761 | `_backfill_prices` | Price backfill: **FALLBACK 1** after FMP/Polygon fail | **KEEP** — already correctly positioned as a fallback |
| Finnhub earnings calendar | 947 | `_check_earnings_nearby` | Check if earnings are within 14 days of insider transaction | **KEEP** — narrow, correct use |

---

#### 3j. Finnhub usage — agent/claude_agent.py

- Only in prompts/comments (lines 2705, 2883): references to "finnhub" as a named data source in system prompt context strings
- No actual API calls
- **Action**: NONE

---

### 4. FMP (Financial Modeling Prep)

Already correctly serving as the structured backbone across:
- Gainers/losers/actives (home_service, market_data_service)
- Market news, stock news (market_data_service)
- Macro: DXY, commodities, forex, treasury rates, economic calendar, sector ETF performance
- Playbook scoring: profile, ratios, financial-growth, income statement
- Fundamentals enricher: quote, profile, ratios
- Options flow: quote alongside Finnhub

**No routing changes needed for FMP. Need to ADD methods**: `get_company_profile(ticker)`, `get_earnings_surprises(ticker)`, `get_earnings_calendar_for_ticker(ticker)`.

---

### 5. TRADIER

Already correctly serving as:
- All equity quotes and daily history in AI Terminal (`caelyn_terminal.py`)
- Options chain, option expirations, Greeks, full option chain
- Intraday timesales
- `tradier_flow_engine.py` — options flow screening
- Discovery enrichment for US/ADR names

Missing from its correct domain:
- Sector ETF live quotes (currently Finnhub — should be Tradier)
- `research_ticker` live quote (currently Finnhub — should be Tradier primary)

---

### 6. FINVIZ

Already correctly scoped to discovery/screener lanes:
- `home_service.py`: FMP primary for movers → Finviz supplement/fallback (correct architecture)
- `tradier_flow_engine.py`: prefilter for options flow candidates
- `options_flow_engine.py`: prefilter universe
- `market_data_service.py`: screener-specific pages only

**No routing changes needed.**

---

### 7. HYPERLIQUID

Correctly scoped to crypto-native lanes. Not being misused. **No changes.**

---

## Answers to Specific Audit Questions

**Where are we still relying on Finnhub, and is any of it unique/useful?**
- UNIQUE/KEEP: `get_social_sentiment` (Reddit+Twitter aggregate), `get_insider_sentiment` (MSPR), `get_recommendation_trends` (historical trend), `get_technicals` (RSI/MACD from Finnhub scan), `get_upcoming_earnings` (weekly market-wide calendar), candle fallback, international profile fallback, discovery pipeline (intentional)
- REDUNDANT/REPLACEABLE: quote (→ Tradier), company profile (→ FMP), company news (→ FMP), earnings surprises/calendar (→ FMP), peer companies (→ FMP)

**Where are we still relying on TwelveData, and does Tradier/FMP already cover that need?**
- Daily bars candle primary. Tradier can provide daily bars but format is different and swapping would touch TA calculations. Narrow legitimate use. KEEP.

**Are any routes incorrectly using Finviz scraping for data that should come from FMP?**
- No. Finviz is consistently used as a supplemental/fallback for screener/movers only, with FMP as primary.

**Are any live multi-symbol panels currently powered by providers too weak/slow/limited?**
- YES: Sector rotation ETF panel uses 15 individual Finnhub calls. Should be one Tradier batch call.

**Are any intraday technical paths using FMP where Tradier would be better?**
- No — FMP is not used for intraday data. Tradier is correctly handling options and intraday where needed.

**Are popup chatbox / agent endpoints still calling stale provider helpers?**
- `research_ticker` (the primary AI terminal data enrichment) uses Finnhub as primary for quote and profile where Tradier/FMP are correct. This is the main stale path.
- All other agent helpers are correctly using FMP/Tradier as backbone.

---

## PHASE 2 — BACKEND ROUTING NORMALIZATION PLAN

### Final Backend Routing Matrix

| Data Lane | Primary | Fallback | Files/Features Affected |
|-----------|---------|----------|------------------------|
| Stock live quote | **Tradier** | Finnhub | `research_ticker`, sector rotation |
| Multi-symbol live quote panel | **Tradier** | Finnhub | sector_rotation/providers.py |
| Stock company profile | **FMP** | Finnhub | `research_ticker` |
| Stock news / ticker news | **FMP** `get_stock_news` | Finnhub `get_company_news` | `research_ticker`, `caelyn_terminal._fetch_news`, `dilution_signals.fetch_company_news` |
| Market news | **FMP** `get_market_news` | (Perplexity if web_search_allowed) | `get_market_news_context` |
| Gainers / losers / actives | **FMP** | Finviz supplement | home_service, market_data_service |
| Earnings surprises (per ticker) | **FMP** (new method) | Finnhub | `research_ticker`, `caelyn_terminal._fetch_earnings_calendar` |
| Earnings calendar (per ticker) | **FMP** (new method) | Finnhub | `research_ticker`, `caelyn_terminal._fetch_earnings_calendar` |
| Earnings calendar (market-wide) | **Finnhub** `get_upcoming_earnings` | — | options_flow_engine, notifai_service |
| Daily OHLCV candles | **TwelveData** | Finnhub candles → Polygon | `get_candles` |
| Fundamentals / ratios / profile | **FMP** | — | playbook_scoring, fundamentals_enricher |
| Social/Reddit/Twitter sentiment | **Finnhub** `get_social_sentiment` | — | UNIQUE — no replacement |
| Insider sentiment (MSPR) | **Finnhub** `get_insider_sentiment` | — | UNIQUE — no replacement |
| Analyst recommendation trend | **Finnhub** `get_recommendation_trends` | — | UNIQUE — no replacement |
| Technical indicators | **Finnhub** `get_technicals` | Polygon | `research_ticker`, options_flow_engine |
| Options chain / Greeks | **Tradier** | Public.com | all options routes |
| Intraday timesales / short candles | **Tradier** | — | tradier_flow_engine |
| Macro (DXY, oil, gold, rates, calendar) | **FMP** | FRED | market_data_service |
| Crypto / perps / funding | **Hyperliquid** | CoinGecko | all crypto routes |
| Screener / discovery candidates | **Finviz** | FMP | discovery/screener services |
| International profile / news | **Finnhub** (intentional) | — | discovery_enrichment |
| International market cap fallback | **Finnhub** (intentional fallback) | — | screener_enrichment |

---

## PHASE 3 — PROPOSED CHANGES (Smallest Safe Set)

The following 5 changes implement the routing matrix. Listed in priority order (highest impact / lowest risk first).

---

### CHANGE 1 — `services/sector_rotation/providers.py`
**What**: Replace 15 individual Finnhub ETF quote calls with one Tradier `get_quotes(symbols)` batch call.
**Why**: Tradier is explicitly the "live multi-symbol quote panel" provider. 15 Finnhub individual calls is 15× the quota burn for something Tradier does in 1 call.
**Format mapping**:
- Tradier `last` → `price`
- Tradier `change` → `change` (same)
- Tradier `change_percentage` → `change_1d_pct`
- Tradier `prevclose` → `prev_close`
- Tradier `high` → `day_high`
- Tradier `low` → `day_low`
**Fallback**: Keep Finnhub as fallback if Tradier is not available (no `TRADIER_API_KEY`).
**Risk**: LOW — display-only ETF quote data, no TA/scoring downstream.

---

### CHANGE 2 — `data/fmp_provider.py`
**What**: Add 3 new FMP methods:
1. `get_company_profile(ticker)` — `/profile/{ticker}` → normalize to `{name, sector, industry, market_cap, logo, exchange, ipo_date, country, description}`
2. `get_earnings_surprises(ticker)` — `/earnings-surprises/{ticker}` → `[{date, actual_eps, estimated_eps, surprise_pct}]`
3. `get_earnings_calendar_for_ticker(ticker)` — `/historical/earning_calendar/{ticker}` → `[{date, eps_estimate}]`

**Why**: These FMP endpoints exist and are included in standard FMP tiers. Without these methods, several reassignments can't be made.
**Risk**: LOW — additive only. No existing code changes.

---

### CHANGE 3 — `data/market_data_service.py` — `research_ticker`
**What**: Make Tradier primary for live quote, FMP primary for company profile. Keep Finnhub as fallback for both.
**Current path**: `if daily_budget.can_spend("finnhub", 2): [finnhub.get_quote, finnhub.get_company_profile]`
**New path**:
1. Try `self.tradier.get_quote(ticker)` → normalize to `{price, change, change_pct, high, low, prev_close}` format
2. Fall back to `self.finnhub.get_quote(ticker)` if Tradier fails or returns empty
3. Try `self.fmp.get_company_profile(ticker)` → normalize to `{name, sector, industry, market_cap, ...}` format
4. Fall back to `self.finnhub.get_company_profile(ticker)` if FMP fails
5. Replace `finnhub.get_company_news` (primary news) with `fmp.get_stock_news` as primary, Finnhub as fallback
6. Replace `finnhub.get_earnings_surprises` with `fmp.get_earnings_surprises` (new method)
7. Replace `finnhub.get_earnings_calendar` with `fmp.get_earnings_calendar_for_ticker` (new method)
8. Replace `finnhub.get_company_peers` with FMP `/stock_peers/{ticker}` (new method)

**Format contract**: `sync_data["quote"]` and `sync_data["company_profile"]` variable names kept the same; only data source changes. All downstream AI context paths see the same keys.
**Risk**: MEDIUM — this is the primary AI Terminal data path. Must validate with 876 tests + app smoke test.

---

### CHANGE 4 — `data/caelyn_terminal.py`
**What**: In `_fetch_news`, replace `self.finnhub.get_company_news(t, 7)` with `self.fmp.get_stock_news(t, limit=5)`.
**Why**: AI Terminal news feed should use FMP (structured backbone). FMP `get_stock_news` returns `{title, text, source, published, url}` — already compatible with all news display logic.
**Fallback**: Keep a fallback to Finnhub if FMP returns empty.
**Risk**: LOW — terminal news display only. No scoring/TA downstream.
**Note**: `_fetch_earnings_calendar` — DEFER. Requires the new FMP `get_earnings_surprises` and `get_earnings_calendar_for_ticker` methods from Change 2 to be solid first. If those are stable, this becomes low-risk.

---

### CHANGE 5 — `services/playbook/dilution_signals.py`
**What**: In `fetch_company_news(ticker, finnhub_key)`, add FMP `get_stock_news` as primary path. Add `fmp_api_key` parameter. If FMP returns articles, use them; otherwise fall back to Finnhub.
**Why**: Playbook scoring for catalyst/dilution risk uses news keyword scanning. News from FMP is higher quality and more structured. Both sources use `{"title": ...}` key so the scoring functions work without change.
**Propagation**: `compute_extended_factors(raw, playbook, finnhub_key, fmp_api_key)` in `playbook_scoring.py` already passes `fmp_api_key` — the call at line 394 just needs to be updated to pass it through to `fetch_company_news`.
**Risk**: MEDIUM — scoring output could differ if FMP returns different articles than Finnhub. Keyword scan is fuzzy (word matching), so format difference is low risk. 876 tests will catch any regression.

---

### What is intentionally NOT changed

| Provider | Usage | Reason Preserved |
|----------|-------|-----------------|
| Finnhub `get_social_sentiment` | `research_ticker`, `get_social_buzz`, `get_social_momentum` | UNIQUE — Reddit/Twitter aggregate. No FMP/Tradier equivalent. |
| Finnhub `get_insider_sentiment` | `research_ticker` | UNIQUE — MSPR monthly aggregate. SEC Edgar has raw transactions but not this metric. |
| Finnhub `get_recommendation_trends` | `research_ticker` | UNIQUE — historical analyst trend (4 quarters). FMP has current ratings only. |
| Finnhub `get_technicals` | `research_ticker`, options_flow_engine | Computed from Finnhub scan; Polygon fallback exists. Acceptable. |
| Finnhub `get_upcoming_earnings()` | options_flow_engine, notifai_service | Market-wide weekly earnings calendar — integrated format, high value. |
| Finnhub candle fallback | `get_candles` | Already positioned as fallback after TwelveData. Correct. |
| Finnhub in discovery_enrichment | all 3 functions | Spec explicitly designates Finnhub for international profile/news/peers in discovery. |
| Finnhub in screener_enrichment | `_try_finnhub` | Already FMP-primary, Finnhub for international market cap fallback. |
| Finnhub in notifai_service | `earnings_calendar` | Acceptable — narrow display use, earnings digest. |
| Finnhub in insider_activity | fallback quote/metrics | Already correctly positioned as FALLBACK 1 after FMP/Polygon. |
| TwelveData | `get_candles` primary | Narrow candle technical use case the spec carves out. Rate-limited but circuit-breaker protected. |
| Alpha Vantage | all | Already dead-code fallback. No live calls. No change needed. |
| Finviz | discovery/screener | Intentionally scoped to discovery/screener per spec. Correct. |

---

## Files That Will Change (Phase 3 Implementation)

| File | Change |
|------|--------|
| `data/fmp_provider.py` | Add `get_company_profile`, `get_earnings_surprises`, `get_earnings_calendar_for_ticker`, `get_stock_peers` methods |
| `data/market_data_service.py` | `research_ticker`: Tradier→quote, FMP→profile, FMP→news, FMP→earnings, FMP→peers; all with Finnhub fallback |
| `data/caelyn_terminal.py` | `_fetch_news`: FMP `get_stock_news` primary, Finnhub fallback |
| `services/sector_rotation/providers.py` | `fetch_etf_quotes`: Tradier batch primary, Finnhub fallback |
| `services/playbook/dilution_signals.py` | `fetch_company_news`: FMP primary, Finnhub fallback; add `fmp_api_key` param |
| `services/playbook/playbook_scoring.py` | Pass `fmp_api_key` through to `fetch_company_news` call |

---

## Verification Plan (after implementation)

1. **876-test suite**: `cd backend && python3.11 -m services.playbook.factor_tests`
2. **Import sanity**: `python3.11 -c "from data.market_data_service import MarketDataService; print('OK')"`
3. **App startup**: Restart workflow, confirm no startup errors
4. **Smoke tests** (targeted endpoint checks):
   - AI Terminal path: GET /api/caelyn-terminal → verify Tradier quotes, FMP news
   - Research ticker: GET /api/research/AAPL → verify quote from Tradier, profile from FMP
   - Sector rotation: verify ETF quotes via Tradier batch
   - Options flow: verify Finnhub earnings calendar still intact
   - Social buzz: verify Finnhub social_sentiment still present
   - Playbook scoring: score a known ticker and confirm factor_count unchanged
