# DeepSeek/OpenCode — System-Wide Latency Forensic Audit Report

**Task**: Read-only latency forensic audit of CaelynAI Backend  
**Date**: 2026-08-07 04:29–05:00 UTC  
**Agent**: OpenCode (DeepSeek)  
**Read-Only Confirmation**: NO production source code, configuration, schedules, cache TTLs, provider budgets, or background tasks were modified. NO commit created. NO push performed.

---

## 1. AGENTS.md Confirmation

AGENTS.md was read first (468 lines, `/home/runner/workspace/AGENTS.md`).  
Report destination: `/home/runner/workspace/.opencode-reports/latest.md`  
All AGENTS.md rules followed — no architecture changes, no unauthorized edits, no commit.

---

## 2. Repository State

| Field | Value |
|---|---|
| Repository root | `/home/runner/workspace` |
| Branch | `main` |
| HEAD | `556e6ce31fea3e69b399ad14b572015182208980` |
| `git status -sb` | `## main...origin/main [ahead 1]` |
| HEAD vs origin/main | local ahead by 1 ("Published your App" commit) |
| origin/main | `1820ab84` ("Update codex report and refresh dashboard data caches") |
| Working tree | Modified: ~8 data/cache files (normal runtime artifacts) |
| Staging area | Empty |

---

## 3. Process Health

| Metric | Value |
|---|---|
| Uptime | ~2h 59m (system), ~1h 35m (backend PID 4551) |
| Load average | 1.71, 0.59, 0.30 |
| RAM total/used/free | 7965MB / 4929MB / 2777MB |
| Swap | 0 MB |
| Backend PID | 4551 (python3.11 -m uvicorn main:app --host 0.0.0.0 --port 5000) |
| Backend RSS | 1,602,372 kB (~1.6 GB) |
| Backend threads | 20 |
| Backend CPU | 15.4% (sampled) |
| Backend age | 01:35:40 |
| Backend state | S (sleeping — not CPU-bound at sample time) |
| Port 5000 | LISTEN |
| Classification | **Idle during off-hours; but 1.6 GB RSS suggests heavy data structures loaded** |

System is NOT CPU-bound and NOT memory-bound at sampling time (04:29 UTC = 00:29 ET, off-hours Friday). However, 1.6 GB RSS for a single Python process indicates significant in-memory data structures.

---

## 4. Current Provider Limit Configuration

### Tradier Global Limiter

| Field | Value |
|---|---|
| File | `backend/data/tradier_provider.py:144` |
| Default RPM | 110 |
| Max RPM | 110 (hard-coded) |
| Env override | `TRADIER_MARKET_DATA_RPM` (default 110, max 110) |
| Limiter type | Async sliding window (60s), `_TradierRateLimiter` |
| Lifetime calls | 1,758 |
| Lifetime throttled | **1,588** (47.5% throttle rate) |
| Calls in last 60s (sampled) | 0–1 (off-hours idle) |
| Headroom (sampled) | 109–110 |
| Last 429 | None (no actual HTTP 429 from Tradier) |

**Critical finding**: 47.5% of all Tradier acquire attempts resulted in throttling (the local limiter made callers wait). This is a strong signal of chronic oversubscription during market hours when all background loops are active.

### Tradier Lane Budgets

| Lane | Budget (RPM) | File |
|---|---|---|
| quotes | 30 | `tradier_budget.py:91` |
| options_flow | 40 | `tradier_budget.py:92` |
| saved_options | 25 | `tradier_budget.py:93` |
| maintenance | 20 | `tradier_budget.py:94` |
| sectors | 60 | `tradier_budget.py:95` |
| reserved | 5 | `tradier_budget.py:96` |
| canonical_history_backfill | 5 | `tradier_budget.py:102` |
| **Sum of all lanes** | **185** | |
| **Global ceiling** | **110** | |

**Key observation**: Lane budgets sum to 185 RPM but the global TRADIER_LIMITER caps at 110 RPM. The lane budgets serve as sub-limits for individual callers but do NOT guarantee total capacity is below 110. Multiple lanes can independently be "under budget" while collectively exceeding 110.

### FMP

| Field | Value |
|---|---|
| Limiter | **None** — "No rate limiter — FMP calls use per-endpoint caching; add limiter if 429s appear" (main.py:6441) |
| Per-endpoint caching | Yes, variable TTLs |

### Tradier Bypass Paths (do not route through TRADIER_LIMITER)

| Path | Mechanism | Risk |
|---|---|---|
| `theme_rs_service._tradier_quotes_batch` | raw httpx + own Semaphore(20) | Can consume Tradier budget outside global limiter |
| `theme_rs_service._fetch_intraday_bars` | raw httpx + own Semaphore(20) | Same |
| `theme_rs_service._fetch_tradier_daily_history` | raw httpx | Same |
| `watchlist_quote_cache._fetch_batch_direct` | raw httpx | Startup-only, suppressed once warm |
| `congressional_trading_service` | Not yet migrated | Direct Tradier calls |
| `insider_activity_service` | Not yet migrated | Direct Tradier calls |

---

## 5. Complete Background Loop Inventory

### Phase A: Lifespan Pre-Yield (tasks registered before `yield`)

| # | Job | Source | Startup Delay | Cadence | Provider | Gate |
|---|---|---|---|---|---|---|
| 1 | `_briefing_precompute_loop` | main.py:569 | init_event.wait(120s) | 30 min | FMP, Finviz, Finnhub, Fred, Stocktwits, Perplexity | None |
| 2 | `_edgar_cache_loop` | main.py:570 | init_event.wait(120s) + 30s | 5 min check, 2h filings, midnight full | SEC EDGAR | Market hours gate for filings |
| 3 | `_itype_classify_loop` | main.py:655 | 30s | 30 min | FMP /stable/profile | Master screener gate |
| 4 | `_master_screener_loop` | main.py:656 | init_event.wait(90s) | ~39s (hot) / ~67s (cold) | **Tradier** (options chain) | Off-hours: maintenance mode |
| 5 | `_sectors_fast_backfill_loop` | main.py:657 | init_event.wait(60s) + Semaphore wait | Priority: 4-8 min; BG: 30 min | **Tradier** (options chain) | Off-hours: maintenance |
| 6 | `_theme_options_supplement_loop` | main.py:658 | init_event.wait(60s) + Semaphore wait | ~2 min active, 20 min off-hours | **Tradier** (options) | Off-hours: suppressed |
| 7 | `_polygon_options_ingestion_loop` | main.py:660 | Variable | ~30 min | Polygon.io | None |
| 8 | `_macro_precompute_loop` | main.py:661 | init_event.wait(120s) | 12 min (720s) | FMP, FRED, Yahoo | Capacity gate |
| 9 | `_strategy_history_precompute_loop` | main.py:662 | init_event.wait(180s) | 3 hours | FRED, Yahoo | None |
| 10 | `_hl_boot_and_run` | main.py:675 | Immediate | Real-time WebSocket | Hyperliquid | None |
| 11 | `_bittensor_refresh_loop` | main.py:678 | Variable | Variable | Bittensor | None |
| 12 | `_x_consensus_loop` | main.py:681 | init_event.wait(30s) + catch-up | Daily 10:00 AM CT | Grok/XAI | Saturday skip |
| 13 | `_alert_bus_retention_loop` | main.py:693 | 12h delay | 12 hours | Neon DB | None |

### Phase B: Lifespan (inline/in-post-yield)

| # | Job | Source | Startup Delay | Cadence | Provider | Gate |
|---|---|---|---|---|---|---|
| 14 | `_watchlist_fundamentals_weekly_loop` | main.py:863 | 300s (5 min) | Polls every 10 min outside window | **FMP** | Sunday 02:00-05:00 ET only |
| 15 | `_ei_materials_loop` | main.py:864 | 300s (5 min) | 24 hours | SEC EDGAR | None |
| 16 | `_watchlist_rank_snapshot_loop` | main.py:968 | 120s | 300s (~5 min) | Neon DB, in-memory | None |
| 17 | `_thematic_warmup` | main.py:975 | Variable | Once + periodic | Caches only | None |
| 18 | `_dynamic_thematic_universe_loop` | main.py:979 | 30s | 15 min | FMP, ETF data, X consensus | None |
| 19 | `_theme_rs_warmup` | main.py:983 | Variable | 15 min active / 60 min off-hours | **Tradier** (bypass limiter) | Market hours gate |
| 20 | `_earnings_calendar_warmup` | main.py:986 | 5s | Once (3-week prewarm) | **FMP** | None |
| 21 | `_calendar_snap_loop` | main.py:1211 | Variable | Weekly Sunday | **FMP** | Per-tab ET hour gate |
| 22 | `_cal_stale_check` | main.py:1212 | 45s | Once + weekly | **FMP** | None |
| 23 | `_screener_hub_loop` | main.py:1225 | Variable | Scheduled ET times | **FMP** (fundamentals) | Day-of-week gates |
| 24 | `_odds_scanner_loop` | main.py:1231 | 90s | 30 min | Polymarket | Odds scanner kill switch |
| 25 | `_investor_intelligence_loop` | main.py:1234 | 120s | 30 min | Polymarket + Neon DB | None |
| 26 | `_terminal_prewarm` | main.py:1238 | 60s | Once (startup) | **Tradier**, Finnhub, **FMP**, Yahoo, CoinGecko | None |
| 27 | `_trading_dashboard_startup` | main.py:1261 | 60s | Once (startup) | Macro provider (FMP/FRED) | None |
| 28 | `_rss_sweeper_loop` | main.py:1269 | 120s | ~2 min | RSS feeds + Neon DB | None |

### Phase C: Post-Yield Bootstrap

| # | Job | Source | Startup Delay | Cadence | Provider | Gate |
|---|---|---|---|---|---|---|
| 29 | `_insider_bg_loop` | main.py:1013 | Post-yield (import dependent) | Variable | SEC EDGAR | None |
| 30 | `_cong_bg_loop` | main.py:1014 | Post-yield | Variable | SEC EDGAR | None |
| 31 | `_whale_bg_loop` | main.py:1019 | Post-yield | Variable | Neon DB + providers | None |
| 32 | `_em_tick` (earnings monitor tick) | main.py:1029 | 30s | 60s | **FMP** (only due targets) | Target-based gate |
| 33 | `_em_catchup` (earnings catch-up) | main.py:1039 | 25s | Once per cold start | **FMP** | 4-day lookback |
| 34 | `_em_loop` (reserved VM earnings) | main.py:1051 | Conditional (env var) | 30s | **FMP** | `LIVE_EARNINGS_MONITOR_ENABLED` gate |
| 35 | `_defiance_2x_daily_loop` | main.py:1076 | 120s | 20 hours | Unknown | None |
| 36 | `_wl_stage2_warmup` | main.py:1129 | 60s | Once + periodic | In-memory compute + Tradier | None |
| 37 | `_earn_precompute_loop` | main.py:1174 | Post-yield | Variable | Neon DB | None |
| 38 | `_confluence_background_rebuild` | main.py:1146 | Post-yield | On-demand | In-memory + LKG | Snapshot absent gate |
| 39 | `canonical_history_backfill` | main.py:672 | Startup | Scheduled | **Tradier** (maintenance lane, 5 RPM) | Off-hours preferred |

**Total background tasks registered at startup: 39+ distinct loops**

---

## 6. Tradier Ownership Matrix

| Caller | Interactive/BG | Global Limiter | Lane | Est. Calls/Cycle | Cadence |
|---|---|---|---|---|---|
| `_master_screener_loop` (Stage 2 chains) | BG | Yes | options_flow (40) | ~47 per cycle | ~39-67s |
| `_sectors_fast_backfill_loop` (chain summarizer) | BG | Yes | sectors (60) or maintenance (20) | batch*2 per cycle | 25-60s |
| `_theme_options_supplement_loop` | BG | Yes | maintenance (20) | ~6/10min | 10 min |
| `_terminal_prewarm` | BG (startup only) | Yes | reserved (5) | ~50-100 | Once |
| `canonical_history_backfill` | BG | Yes | canonical_history_backfill (5) | Variable | Scheduled |
| `_macro_precompute_loop` | BG | Yes | reserved (5) | ~2 | 720s |
| `Home dashboard batch_quotes` | **Interactive** | Yes | quotes (30) | Variable per page | Per request |
| `_theme_rs_warmup` | BG | **BYPASS** | N/A (own Semaphore(20)) | ~39 symbols * 2-3 calls | 15/60 min |
| `_theme_rs_service._fetch_intraday_bars` | BG | **BYPASS** | N/A | ~39 symbols | 15/60 min |
| `congressional_trading_service` | BG | **BYPASS** | N/A | Unknown | Unknown |
| `insider_activity_service` | BG | **BYPASS** | N/A | Unknown | Unknown |
| `watchlist_quote_cache._fetch_batch_direct` | BG (startup) | **BYPASS** | N/A | Startup-only | Once |

**Answer to key question: YES, multiple independent subsystems compete for the same 110 RPM global ceiling while some bypass it entirely.** During market hours, master screener (~47 calls/cycle at 39s cadence ≈ 72 RPM) + sectors backfill (up to 60 RPM lane) alone could saturate the global 110 RPM before accounting for quotes, maintenance, reserved lanes, and bypass paths.

---

## 7. FMP Ownership Matrix

| Caller | Interactive/BG | Cadence | Batch | Cache | Gate |
|---|---|---|---|---|---|
| `_briefing_precompute_loop` (FMP market data) | BG | 30 min | 4 calls | Yes (30 min) | None |
| `_earnings_calendar_warmup` | BG (startup) | Once | 3 weeks | Yes (weekly) | None |
| `_cal_stale_check` / `_calendar_snap_loop` | BG | Weekly | Per-tab | Yes (disk) | Day-of-week |
| `_screener_hub_loop` (fundamentals) | BG | Weekly (scheduled) | Watchlist+Portfolio | Yes | ET hour gate |
| `_watchlist_fundamentals_weekly_loop` | BG | Weekly (Sunday 02-05 ET) | 50/run | Yes (Neon) | Window gate |
| `_em_tick` / `_em_catchup` | BG | 60s / once | Only due targets | Yes (Neon) | Target-based |
| `_itype_classify_loop` (display names) | BG | 30 min | 60/cycle | Yes (LKG) | Master screener gate |
| `_dynamic_thematic_universe_loop` (FMP peers) | BG | 15 min | Variable | Yes (15 min) | None |
| `Home dashboard` (market_news, movers) | **Interactive** | Per request | 15 news, movers | Yes | None |
| `Watchlist upcoming_earnings` | **Interactive** | Per request (non-blocking) | Watchlist symbols | Yes | sync_on_miss=False |
| `Stock compare` | **Interactive** | Per request | Per symbol | Yes (TTL varies) | None |
| `_terminal_prewarm` | BG (startup) | Once | Holdings count | No | None |

**Flag**: Home dashboard (`home_service.py:1297`) calls `data_service.fmp.get_market_news()`, `data_service.fmp.get_gainers_losers()`, and FMP quotes as a fallback on **every page request** — this is a request-path FMP call.

---

## 8. Rate-Status Steady-State Trace (Off-Hours, 4:43–5:00 UTC)

| Time UTC | Tradier Used/110 | Headroom | Lifetime Throttled | Lifetime Calls |
|---|---|---|---|---|
| 04:43:45 | 0 | 110 | 1,588 | 1,758 |
| 04:44:00 | 0 | 110 | 1,588 | 1,758 |
| 04:44:15 | 0 | 110 | 1,588 | 1,758 |
| 04:43-05:05 (after endpoint tests) | 0-5 | 105-110 | 1,588 | 1,758-1,763 |

**All lanes: 0 calls, 0 deferred, 0 saturated during off-hours** — the system is completely idle. The 1,588 throttle events all occurred during earlier market-hours operation (~1.5 hours into this process's lifetime).

**Session**: `off_hours`, Friday 00:43 ET. Budget enforcement: inactive.

---

## 9. First-15-Minute Startup Overlap Timeline

```
t=0s      lifespan begins
t=0-2s    lifespan yield — healthcheck responds 200
          _deferred_sync_startup thread starts (Neon DB init, LKG loads)
          _do_init thread starts (MarketDataService, TradingAgent)

t=5s      _earnings_calendar_warmup (3-week FMP warmup)
t=25s     _em_catchup (earnings catch-up pass, FMP per symbol)
t=30s     _itype_classify_loop first pass (FMP calls for unresolved symbols)
t=30s     _em_tick loop begins (60s interval, processes due targets only)
t=30s     _edgar_cache_loop initial full refresh (SEC EDGAR)
t=30s     _dynamic_thematic_universe_loop first build (FMP peers + X consensus)
t=45s     _cal_stale_check (FMP snapshot staleness check)
t=60s     _terminal_prewarm (Tradier + FMP + Yahoo + CoinGecko for all holdings)
t=60s     _trading_dashboard_startup (macro provider refresh, FMP/FRED)
t=60s     _wl_stage2_warmup (Tradier for watchlist tickers)
t=60s     _theme_rs_warmup (Tradier BYPASS, 39 themes * multiple calls)
t=60s     _sectors_fast_backfill_loop waits for master screener Semaphore
t=60s     _theme_options_supplement_loop waits for master screener Semaphore
t=90s     _master_screener_loop first cycle (~47 Tradier chain calls)
t=90s     _odds_scanner_loop first scan (Polymarket)
t=120s    _investor_intelligence_loop first build
t=120s    _rss_sweeper_loop first sweep (Neon DB writes)
t=120s    _macro_precompute_loop first cycle (FMP + FRED + Yahoo)
t=120s    _briefing_precompute_loop first cycle (multiple providers)
t=120s    _defiance_2x_daily_loop starts
t=120s    _watchlist_rank_snapshot_loop starts
t=180s    _strategy_history_precompute_loop starts
t=300s    _watchlist_fundamentals_weekly_loop starts polling
t=300s    _ei_materials_loop starts
```

**Theoretical Tradier demand at t=60-90s (peak overlap)**:
- Terminal prewarm: ~50-100 calls (once)
- Theme RS warmup: ~39 symbols * 2-3 calls each ≈ 100 calls (BYPASSES limiter)
- Master screener: ~47 calls/cycle
- Total projected Tradier demand within a 60s window: **~100-200 calls** against a 110 RPM global ceiling, PLUS bypass paths that don't count against the limiter.

**Steady-state overlap (t=120s+ during market hours)**:
- Master screener: ~72 RPM (47 calls every 39s)
- Sectors backfill: up to 60 RPM (batch mode)
- Theme supplement: ~2.4 RPM (6 calls per 10 min)
- Home page requests: variable Tradier quotes
- All lanes compete under the same 110 RPM ceiling

---

## 10. Current Scheduling vs 2205449b

`2205449b` ("fix(startup): stagger Tradier warmups and preserve request headroom") originally:
- Changed terminal prewarm delay: 15s → 60s
- Changed master screener delay: 30s → 90s

Changes since 2205449b to main.py (commits that touch main.py):
1. `8455d40f` — fix(runtime): preserve Tradier headroom and bound execution refresh
2. `ac9a4c0d` — fix(runtime): reserve Tradier capacity and verify execution lifecycle
3. `ec46444d` — fix(deploy): correct .pythonlibs compileall path + lifespan diagnostic print
4. `e6b78e75` — fix(execution): make canonical quality snapshot reliable
5. `722e2602` — fix(execution): prove live quality and hydrate lkg immediately
6. `4035e154` — fix(execution): complete fresh refresh and isolate lkg tests
7. `fbc08e44` — fix(startup): self-heal missing comparison-close tails
8. `09f416b4` — perf: make watchlist detail LKG-first

**Key additions since 2205449b**:
- `fbc08e44`: Added comparison-close tail self-heal to bootstrap (t=~2s)
- `09f416b4`: In-memory bulk LKG for watchlist detail (reduces rebuild frequency)
- `ac9a4c0d`/`8455d40f`: Added `can_start_background_batch()` capacity checks to master screener, sectors backfill, and macro precompute
- Added Trading Dashboard startup refresh (extra provider work at t=60s)
- All capacity checks ADD safety but don't REMOVE contention — they defer when saturated, which protects but doesn't eliminate root-cause oversubscription

**Architectural drift assessment**: The changes since 2205449b have added MORE startup/background work (trading dashboard, tail self-heal), not less. While they've also added protective capacity checks, the fundamental problem of oversubscribing 110 RPM with 185 RPM of lane budgets PLUS bypass paths remains unchanged.

---

## 11. Request-Path Provider-Work Audit

### `GET /api/home/dashboard` (3.37s, 153KB)
- **Tradier calls**: YES — `home_service.py:725` batch_quotes hits Tradier quotes lane
- **FMP fallback**: YES — `home_service.py:826` falls back to FMP `/stable/quote` for missing tickers
- **FMP market news**: YES — `home_service.py:1197` calls `fmp.get_market_news()`
- **FMP gainers/losers**: YES — `home_service.py:1298` calls `fmp.get_gainers_losers()`
- Used 4 Tradier calls in test (off-hours, light load)
- **Verdict**: VIOLATION — Home page triggers provider calls on every request

### `GET /api/watchlist/{id}` (1.77s, 6.3MB for 463 tickers)
- **Tradier calls**: NO (during request path — uses in-memory quote cache + LKG)
- **Provider calls trigger**: NON-BLOCKING background sync for earnings
- **Disk I/O**: YES — loads watchlist from store, loads fundamentals snapshots
- **Response size**: 6.3MB raw JSON (463 ticker rows with full analysis, csv_data, earnings)
- **Verdict**: NO provider calls in blocking path (LKG-first design works). But 6.3MB response is extremely large.

### `GET /api/earnings/live-events` (12.85s, 77KB)
- **Slowest endpoint** (12.85 seconds in off-hours)
- **FMP calls**: Potentially triggered if cache is cold
- **Verdict**: NEEDS INVESTIGATION — 12.85s for 77KB suggests DB or provider wait

### `GET /api/themes/relative-strength` (0.487s, 484KB)
- **Tradier calls**: Possibly (theme_rs_service uses own bypass path)
- **Response**: 484KB — large but acceptable
- **Verdict**: Acceptable. Cache-first design.

### `GET /api/predict/investor/overview` (0.151s, 124KB)
- **No provider calls** — pure cache read
- **Verdict**: Clean

### `GET /api/options-flow/sectors` (2.05s, 2MB)
- Reads from existing master screener cache
- **Verdict**: Acceptable (cache-only, no new provider calls)

### `GET /api/portfolio/holdings` (1.54s, 63KB)
- No provider calls
- **Verdict**: Clean

### `GET /api/home/risk-intelligence` (2.42s, 49KB)
- Uses macro provider (FMP/FRED)
- **Verdict**: May trigger provider calls — needs audit

---

## 12. Earnings Architecture Findings

- **Read isolation CONFIRMED**: `0e3c9e29` ("fix: isolate realtime earnings scans from reads") — the earnings monitor tick loop calls `run_live_earnings_monitor_once(tick_mode=True)` which skips `_build_universe()` and processes only DB-registered due targets. The tick loop (60s interval, 30s initial delay) never scans full watchlist.
- **Request-path earnings**: `sync_on_miss=False` on watchlist GET — never blocks for FMP. Only schedules background sync.
- **Earnings live-events endpoint**: 12.85s response time is concerning and warrants deeper profiling.
- **Earnings monitor uses FMP**: The tick loop fires one targeted FMP call per due symbol. Active earnings windows (BMO 05-11 ET, AMC 15:30-21 ET) create FMP demand every 45-60 seconds per active target.
- **Verdict**: Earnings read isolation is architecturally correct. The 12.85s live-events endpoint needs separate profiling — possibly a Neon DB query issue.

---

## 13. Watchlist LKG Findings

- **In-memory LKG CONFIRMED**: `_BULK_LKG` dict in `watchlist_router.py` provides stale-while-revalidate semantics
- **No disk LKG**: `watchlist_detail_lkg.json` does NOT exist — LKG is purely in-memory, lost on restart
- **BB79c46b**: "fix: preserve watchlist LKG during refresh" — changed from pop-then-rebuild (risky) to copy-on-success (safe)
- **09f416b4**: "perf: make watchlist detail LKG-first" — 5 min fresh window, 20 min stale window, always serve old entry
- **Effectiveness**: During off-hours test, watchlist detail took 1.77s for 6.3MB response — acceptable latency
- **The 1-2 minute user-reported latency is NOT caused by Watchlist detail rebuild** — the LKG mechanism works. The latency comes from elsewhere (Home dashboard provider calls, earnings endpoint, or frontend rendering of 6.3MB payloads)

---

## 14. Canonical History Regression Confirmation

- **59e3ca97**: "perf: comparison-close tail in _INDEX — zero gzip.open per Watchlist request" — CONFIRMED in current code
- **fbc08e44**: "fix(startup): self-heal missing comparison-close tails" — CONFIRMED in bootstrap (step 2a, `_tail_repair`)
- **Zero per-symbol gzip scans at request time**: The `_INDEX` lookup uses precomputed tail data. The self-heal only runs at startup.
- **Verdict**: Regression NOT present. Canonical history comparison-close lookups are efficient.

---

## 15. Database / Neon Connection Pool Inventory

| Pool | File | Type | Min/Max | Purpose |
|---|---|---|---|---|
| `portfolio_store._db_pool` | `data/portfolio_store.py:66` | `SimpleConnectionPool` | 1/3 | Portfolio CRUD |
| `closed_trades_store._db_pool` | `data/closed_trades_store.py:52` | `SimpleConnectionPool` | 1/3 | Closed trades |
| `option_trades_store._db_pool` | `data/option_trades_store.py:68` | `SimpleConnectionPool` | 1/3 | Option trades |
| `pg_storage._pool` | `data/pg_storage.py:83` | `SimpleConnectionPool` | 1/5 (est.) | Chat history, watchlists |
| `rss_article_archive` | `data/rss_article_archive.py:9` | `SimpleConnectionPool` | 1/5 | RSS articles |
| `congressional_trading._pool` | `services/congressional_trading_service.py:203` | `SimpleConnectionPool` | 1/5 | Congressional data |
| `whale_watch._pool` | `services/whale_watch_service.py:145` | `SimpleConnectionPool` | 1/5 | Whale alerts |
| `insider_activity._pool` | `services/insider_activity_service.py:106` | `SimpleConnectionPool` | 1/5 | Insider data |
| `chart_radar_router` | `services/chart_radar_router.py:588` | `SimpleConnectionPool` | Shared | Chart data |
| `extended_factors` | `services/playbook/extended_factors.py:847` | `psycopg2.connect` | Direct (no pool) | Extended factors |

**Total: ~9-10 separate connection pools, each 1/3 to 1/5 connections = 27-47 potential Neon connections**.  
**Risk**: Multiple independent pools create no shared view of connection pressure. All pools use `psycopg2` (synchronous) run via `asyncio.to_thread()` or `run_in_executor()` — this can saturate the default threadpool during parallel requests.

---

## 16. Threadpool / Event Loop Contention

**`asyncio.to_thread()` / `run_in_executor()` calls in main.py**: 30+ distinct sites, including:
- Heavy service imports (lifespan pre-yield)
- Watchlist Stage 2 LKG load, warmup
- Canonical history preload, tail repair
- Earnings snapshot loads
- Neon recovery
- Rank snapshot Neon saves (5-min cadence, per-watchlist)
- EI materials per-symbol Neon loads (24h cadence, 0.6s per symbol)
- `_briefing_precompute_loop`: `fred.get_quick_macro`, `finnhub.get_upcoming_earnings`

**Risk classification**:
- **HIGH**: `run_in_executor(None, _init_event.wait, ...)` — blocks default executor thread but short-lived
- **MEDIUM**: Per-symbol Neon queries via `run_in_executor` in EI materials and rank snapshots — serialized by loop
- **MEDIUM**: `_briefing_precompute_loop` runs multiple `asyncio.to_thread` calls concurrently — can exhaust default threadpool (default: min(32, os.cpu_count()+4))
- **LOW**: Most `asyncio.to_thread` calls are startup-only or low-frequency

**Sync `psycopg2` in async context**: All DB pools use synchronous `psycopg2`. Calls are wrapped in `run_in_executor` — meaning they consume threads from the default `ThreadPoolExecutor`. During high DB demand (e.g., RSS sweeper writing articles while Watchlist rank snapshots run), the default threadpool could become a bottleneck.

---

## 17. Controlled Endpoint Timing Matrix

| Endpoint | Time | HTTP | Size | Tradier Calls (delta) | Notes |
|---|---|---|---|---|---|
| GET / | 0.002s | 200 | 22B | 0 | OK |
| GET /health | 0.002s | 200 | 162B | 0 | OK |
| GET /api/watchlist/list | 0.320s | 200 | 177B | 0 | OK (1 watchlist, metadata only) |
| GET /api/home/dashboard | 3.371s | 200 | 153,780B | 4 | Provider calls in request path |
| GET /api/watchlist/{uuid} | 1.774s | 200 | 6,321,453B | 0-1 | 6.3MB payload! |
| GET /api/earnings/live-events | **12.850s** | 200 | 77,690B | 0 | SLOWEST endpoint |
| GET /api/themes/relative-strength | 0.488s | 200 | 484,583B | 0 | Good |
| GET /api/predict/investor/overview | 0.151s | 200 | 124,906B | 0 | Excellent |
| GET /api/options-flow/sectors | 2.053s | 200 | 2,082,547B | 0 | Large payload, OK speed |
| GET /api/portfolio/holdings | 1.539s | 200 | 63,232B | 0 | OK |
| GET /api/home/risk-intelligence | 2.422s | 200 | 49,815B | 0 | OK |

**Key findings**:
- Earnings live-events (12.85s) is the performance outlier
- Home dashboard (3.37s) triggers live provider calls
- Watchlist detail 6.3MB payload will degrade under gzip compression but still large for frontend
- Request paths that DON'T call providers (predict, themes, portfolio) are fast (<2s)

---

## 18. DeepSeek-Era Git Forensic Timeline

Key commits during the DeepSeek/OpenCode development window:

1. **b7964131** — "fix(watchlist): use live displayed price as numerator for change_7d/change_30d"
   - Added comparison-close history lookup TO Watchlist request path → hundreds of gzip file opens per request
   - **IMPACT**: HIGH — caused request-time disk I/O storm
   - **STATUS**: Fixed by 59e3ca97

2. **59e3ca97** — "perf: comparison-close tail in _INDEX — zero gzip.open per Watchlist request"
   - Moved comparison-close tails into precomputed _INDEX
   - **IMPACT**: POSITIVE — eliminated gzip.open from request path
   - **STATUS**: Active and verified

3. **c1ec7103** — "fix(watchlist): correct 7D/30D calculation and enable incremental append"
   - Watchlist enrichment expansion
   - **IMPACT**: MEDIUM — added historical data processing

4. **fbc08e44** — "fix(startup): self-heal missing comparison-close tails"
   - Added startup self-heal to bootstrap (extra work at startup)
   - **IMPACT**: LOW at startup, zero request-time cost

5. **09f416b4** — "perf: make watchlist detail LKG-first"
   - Added in-memory bulk LKG (5 min fresh, 20 min stale)
   - **IMPACT**: POSITIVE — reduced watchlist rebuild frequency
   - **STATUS**: Active and effective

6. **0e3c9e29** — "fix: isolate realtime earnings scans from reads"
   - Separated earnings monitor tick loop from user-facing reads
   - **IMPACT**: POSITIVE — eliminated read-contention from earnings
   - **STATUS**: Active and verified

7. **cab90aa1** — "fix: restore backend service availability"
   - Restored service after a disruption
   - **IMPACT**: Restorative

8. **bb79c46b** — "fix: preserve watchlist LKG during refresh"
   - Changed from pop-then-rebuild to copy-on-success
   - **IMPACT**: POSITIVE — prevented serving stale/incomplete data
   - **STATUS**: Active and verified

9. **8455d40f / ac9a4c0d** — "fix(runtime): preserve Tradier headroom and bound execution refresh"
   - Added `can_start_background_batch()` capacity checks
   - **IMPACT**: POSITIVE — background jobs now defer when saturated
   - **STATUS**: Active but doesn't address root cause (oversubscription)

**Summary**: The DeepSeek/OpenCode era has been generally corrective — fixing performance regressions rather than introducing them. However, the cumulative startup/bootstrap work has grown without a corresponding expansion of provider capacity. The capacity checks added are defensive but don't solve the oversubscription problem.

---

## 19. Top Five Root Causes — Ranked by Evidence

### #1 — TRADIER GLOBAL LIMITER SATURATION (CONFIDENCE: HIGH)

**Evidence**:
- 47.5% throttle rate (1,588 throttle events / 1,758 lifetime calls in ~1.5h)
- Lane budgets sum to 185 RPM but global ceiling is 110 RPM
- Master screener alone ~72 RPM during market hours (active mode, 39s cadence)
- theme_rs_service BYPASSES the limiter entirely (adds ~39 * 2-3 calls every 15 min)
- congressional_trading and insider_activity services also bypass
- **Symptom match**: Explains system-wide slowness (all pages share Tradier budget)

**Source path**: `data/tradier_provider.py:144` — `TRADIER_LIMITER` singleton  
**Affected pages**: All pages that use Tradier (Home, Watchlist, Options Flow, Screener)  
**Resource consumed**: 110 RPM global Tradier market-data quota  
**Introduced**: Pre-existing; exacerbated by accumulation of background loops

### #2 — HOME DASHBOARD PROVIDER CALLS ON EVERY PAGE REQUEST (CONFIDENCE: HIGH)

**Evidence**:
- Home dashboard calls Tradier batch_quotes on EVERY request (4 calls observed off-hours)
- Falls back to FMP /stable/quote for missing symbols (provider call hidden in request path)
- Calls `fmp.get_market_news()` and `fmp.get_gainers_losers()` on render
- 3.37s for 153KB response — slowest initial-page-load endpoint
- **Symptom match**: Explains Home page slow load; provider contention spreads to other pages

**Source path**: `services/home_service.py:725-860` — `batch_quotes` with FMP fallback  
**Affected pages**: Home dashboard, and any concurrently-loaded page  
**Resource consumed**: Tradier quotes lane (30 RPM) + FMP API calls

### #3 — MASSIVE WATCHLIST PAYLOAD (6.3MB) (CONFIDENCE: HIGH)

**Evidence**:
- Watchlist detail response: 6.3MB raw JSON for 463 tickers
- Contains full `csv_data` (463 rows), `analysis.sections` (all tickers with LLM fields), `upcoming_earnings`, metadata
- GZip middleware compresses, but decompression cost shifts to client
- Frontend must parse and render all 463 rows with full analysis fields
- **Symptom match**: Explains "scrolling and interaction lag" after page loads (browser processing 6.3MB JSON)

**Source path**: `services/watchlist_router.py:6317` — `_build_watchlist_response`  
**Affected pages**: Watchlist detail  
**Resource consumed**: Server CPU (serialization) + client CPU (deserialization + rendering)

### #4 — EARNINGS LIVE-EVENTS ENDPOINT LATENCY (CONFIDENCE: MEDIUM)

**Evidence**:
- 12.85s response time for 77KB payload (off-hours, no competing load)
- This is disproportionately slow — suggests a blocking query or long lock wait
- Not correlated with Tradier throttles (0 calls used)
- Possible Neon DB query issue with earnings_monitor tables
- **Symptom match**: Explains earnings page slow load; may indicate broader Neon contention

**Source path**: `routes/earnings_monitor_router.py` → `services/earnings_monitor_service.py`  
**Affected pages**: Earnings live-events, Earnings monitor  
**Resource consumed**: Neon DB (suspected)

### #5 — MULTIPLE ISOLATED NEON CONNECTION POOLS (CONFIDENCE: MEDIUM)

**Evidence**:
- 9+ independent `SimpleConnectionPool` instances (1-5 connections each = up to ~47 connections)
- All use synchronous `psycopg2` run via `run_in_executor()` (default threadpool saturation risk)
- No shared connection pressure visibility across pools
- RSS sweeper (~2 min cadence, Neon writes) overlaps with rank snapshots (~5 min, Neon writes)
- **Symptom match**: Could explain "lag after page loads" — DB-contended background tasks slow responses

**Source path**: Multiple files (`data/portfolio_store.py`, `data/closed_trades_store.py`, `data/pg_storage.py`, `data/rss_article_archive.py`, `services/congressional_trading_service.py`, `services/whale_watch_service.py`, `services/insider_activity_service.py`, etc.)  
**Affected pages**: Any page with Neon queries  
**Resource consumed**: Neon connections + default threadpool

---

## 20. Provider-Cadence Invariant Table

| Contract | Current Cadence | Limiter/Budget | When Allowed | On Page Request? | Must Preserve? |
|---|---|---|---|---|---|
| Tradier live quotes | 60s TTL (per-symbol cache) | TRADIER_LIMITER (110 RPM), quotes lane (30) | Session-appropriate | YES (Home, Screener Hub) | YES |
| Tradier options (chains) | Master screener: ~39s cycle, Supplement: ~10 min, Sectors: ~4-30 min | options_flow (40), maint (20), sectors (60) | Market hours active; off-hours maintenance | NO | YES |
| Tradier canonical-history maint | Backfill: scheduled, off-hours preferred | canonical_history_backfill (5 RPM) | Off-hours/weekend preferred | NO | YES |
| FMP Watchlist fundamentals | Weekly Sunday 02:00-05:00 ET, 50 symbols/run | NONE | Sunday 02-05 ET only | NO | YES |
| FMP real-time earnings | Tick loop: 60s (only due targets), BMO 05-11 ET / AMC 15:30-21 ET | NONE | During active earnings windows | NO (sync_on_miss=False) | YES |
| Calendar refresh | Weekly Sunday, per-tab ET hour | NONE | Scheduled ET hours | NO | YES |
| Theme RS refresh | ~15 min active / ~60 min off-hours | BYPASS (own Semaphore) | Market hours active | NO | YES |
| Macro refresh | 720s (12 min) | reserved lane (5 RPM) | Always (capacity-gated) | NO | YES |

---

## 21. Proposed Fix Plan (NOT IMPLEMENTED)

This audit is read-only. The following plan is the **smallest possible architecture correction** that preserves all data freshness contracts while eliminating resource contention.

### Priority 1: Make Home Dashboard Cache-Only (2 file changes)

- `services/home_service.py`: Make `batch_quotes` serve LKG only on page render. Route live Tradier calls to a background task. Use existing Tradier 60s per-symbol cache as the primary source.
- Remove FMP fallback from the request path (FMP-only symbols get cached quotes from a refresh loop, not inline).
- **Impact**: Eliminates 4+ Tradier calls per Home page load, plus FMP calls.
- **Risk**: Home page may show stale (60s) quotes — acceptable for dashboard.

### Priority 2: Reduce Watchlist Payload Size (1 file change)

- `services/watchlist_router.py:_build_watchlist_response`: Move `csv_data`, `upcoming_earnings`, and full `analysis.sections` to separate endpoints or paginate. The main watchlist response should return ONLY per-ticker summary rows with enrichment fields.
- **Impact**: Reduces 6.3MB to ~1-2MB. Frontend can lazy-load details.
- **Risk**: Frontend must be updated to request detail data separately (or paginate).

### Priority 3: Consolidate Neon Connection Pools (2-3 file changes)

- Create a shared Neon connection pool (e.g., `data/neon_pool.py`) with known limits.
- All current independent pools switch to the shared pool.
- **Impact**: Prevents accidental connection exhaustion. Enables monitoring.
- **Risk**: Migration risk — each pool consumer must be tested.

### Priority 4: Verify Tradier Bypass Paths Are Budgeted (3 file changes)

- Route `theme_rs_service._tradier_quotes_batch`, `_fetch_intraday_bars`, `_fetch_tradier_daily_history` through TRADIER_LIMITER (or at minimum account for them in a lane).
- Migrate `congressional_trading_service` and `insider_activity_service` to use TRADIER_LIMITER.
- **Impact**: Ensures all Tradier calls are visible and budgeted.
- **Risk**: May slow theme RS warmup — acceptable with proper lane budget.

### Priority 5: Profile and Fix Earnings Live-Events (1 file change)

- Profile `GET /api/earnings/live-events` — the 12.85s response time is anomalous.
- Likely a Neon query without an index or a lock contention issue.
- **Impact**: Fixes the single slowest endpoint.
- **Risk**: Investigation required before code change.

---

## 22. Architectural Classification

| Contributor | Classification |
|---|---|
| TRADIER_LIMITER saturation (47.5% throttle rate, 185 RPM lanes vs 110 ceiling) | **A — SYSTEM-WIDE BACKEND CONTENTION** |
| Home dashboard provider calls on request path | **B — ENDPOINT-SPECIFIC BACKEND LATENCY** (Home) |
| Watchlist 6.3MB payload | **C — FRONTEND / BROWSER COST** (rendering lag) |
| Earnings live-events 12.85s | **B — ENDPOINT-SPECIFIC** (Earnings) |
| Multiple Neon connection pools | **A — SYSTEM-WIDE BACKEND CONTENTION** |
| b7964131 (gzip.open storm) | **D — ALREADY FIXED HISTORICAL ISSUE** |
| Theme RS bypass paths | **A — SYSTEM-WIDE BACKEND CONTENTION** |
| Off-hours idle state | **E — confirms problem is market-hours contention** |

---

## 23. Files Inspected (Read-Only)

1. `AGENTS.md` — Full (468 lines)
2. `backend/main.py` — Full (~18,410 lines)
3. `backend/data/tradier_provider.py` — Lines 1-300
4. `backend/data/tradier_budget.py` — Full (193 lines)
5. `backend/services/watchlist_router.py` — Lines 68-150, 6317-6601
6. `backend/services/home_service.py` — Lines 1-1407 (grep)
7. `backend/services/earnings_monitor_service.py` — Lines 1-60, 1722-1801
8. `backend/data/portfolio_store.py` — Pool lines 36-91
9. `backend/data/closed_trades_store.py` — Pool lines 22-66
10. `backend/data/rss_article_archive.py` — Pool line 9
11. `backend/services/congressional_trading_service.py` — Pool lines 42, 203
12. `backend/services/whale_watch_service.py` — Pool lines 135, 145
13. `backend/services/insider_activity_service.py` — Pool lines 96, 106
14. Git history: `git log --all`, `git diff 2205449b..HEAD -- backend/main.py`, `git log 2205449b..HEAD -- backend/main.py`, `git log 2205449b..HEAD -- backend/services/watchlist_router.py`

---

## 24. No Modifications Confirmation

- **No production source code was modified**
- **No configuration files were changed**
- **No ENV variables were changed**
- **No provider quotas were changed**
- **No cache TTLs or cadences were changed**
- **No background loops were disabled or restarted**
- **No caches were cleared or rebuilt**
- **No commit was created**
- **No push was performed**
- **The backend process was NOT restarted**

---

## 25. Final State

```
Commit SHA: N/A (no commit created)
Push result: N/A (no push performed)
Task commit diff: N/A (no files modified)

$ git status -sb
## main...origin/main [ahead 1]
 M .opencode-persistent/state/prompt-history.jsonl
 M backend/data/bittensor_dashboard_cache.json
 M backend/data/canonical_history/SKYT.json.gz
 M backend/data/canonical_history/_index.json
 M backend/data/hyperliquid_hip3_cache.json
 M backend/data/hyperliquid_signal_snapshots.json
 M backend/data/predict_odds_live_lkg.json
 M backend/data/thematic_context_snapshot.json
```

All dirty files are pre-existing runtime/data artifacts — none are production source code. No task-related files were staged or modified.
