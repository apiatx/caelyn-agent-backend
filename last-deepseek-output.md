Here is the complete read-only architecture audit for the **Caelyn AI Backend**.

---

## ARCHITECTURE AUDIT — Caelyn AI Backend

**Audit date:** 2026-07-30  
**Working directory:** `/home/runner/workspace`  
**Git branch:** `main` (even with `origin/main` — 0 ahead, 0 behind)  
**No code changes, file edits, or mutations performed.**

---

### 1. REPOSITORY OVERVIEW

**Application:** Caelyn AI — a FastAPI-based platform for real-time trading analysis, multi-asset screening, AI-driven market intelligence, and portfolio analytics.

**Runtime:** Python 3.11 (via `uvicorn` on port 5000), deployed to Replit Autoscale via `.replit` config.

**Entrypoint:** `backend/main.py` (~18,800 lines, all routes and lifespan logic in a single file).

**Dependencies:** Listed in `backend/pyproject.toml` (no `requirements.txt`). Key deps: FastAPI, Pydantic, Anthropic, OpenAI, Finnhub, httpx, slowapi, psycopg2-binary, gunicorn, uvicorn.

---

### 2. DIRECTORY LAYOUT

```
/home/runner/workspace/
├── backend/
│   ├── main.py              # Single-file FastAPI app (all routes)
│   ├── config.py             # Environment variable bindings (API keys)
│   ├── auth.py               # JWT + bcrypt authentication
│   ├── subscription.py       # Paywall guard (402 for non-owner)
│   ├── password_reset.py     # Email-based password reset via Resend
│   ├── screener_definitions.py # Deterministic screener preset definitions
│   ├── api_budget.py         # Per-provider daily API call budget tracker
│   ├── gunicorn.conf.py      # Gunicorn config (timeout=300s)
│   ├── pyproject.toml        # Python project config + dependencies
│   ├── agent/                # AI agent layer (Claude, routing, prompts)
│   ├── core/                 # Core engines (regime, TA, catalyst, asset weight)
│   ├── data/                 # Data providers (~50 provider modules), caches, stores
│   ├── routes/               # Router modules (9 route files)
│   ├── services/             # ~120 service modules across sub-packages
│   ├── migrations/           # SQL migration (1 file: screener_hub)
│   ├── static/               # Static HTML (subscribe, reset-password, whale-watch)
│   ├── tests/                # 18 test files
│   ├── tools/                # Cold-start test tool
│   └── scripts/              # Backfill, reliability, and scoring scripts
├── data/                     # Portfolio holdings (active_holdings.json)
├── logs/                     # Quality backfill logs
├── reports/                  # Elite asset rebound validation report
├── audit/                    # Quality metric audit CSV/JSON
├── attached_assets/          # (undetermined)
├── scripts/                  # db_retention_cleanup.py
├── AGENTS.md                 # Codex operating rules (305 lines)
├── CONFLUENCE_AUDIT.md       # Prior architecture audit (873 lines)
├── replit.md                 # Replit workspace documentation
└── .replit                   # Replit deployment configuration
```

---

### 3. ARCHITECTURE PATTERN

**Hybrid monolithic + microservice-like:**

- **Single-process FastAPI** — all routes live in one giant `main.py` (no router files for the main app; sub-routers exist in `backend/routes/` and within sub-packages of `services/`).
- **Service-per-domain** — `backend/services/` contains ~120 modules, each encapsulating one analytical domain (watchlist, earnings, sector rotation, options, themes, playbook, predictions, etc.).
- **Data provider layer** — `backend/data/` contains ~50 provider modules (FMP, Polygon, Finnhub, Tradier, Yahoo, Coingecko, etc.) plus cache stores, all aggregated into `market_data_service.py` (~6,520 lines).
- **Agent layer** — `backend/agent/` (15 files) handles multi-model AI orchestration (Claude, Grok, Gemini, GPT-4o, Perplexity, DeepSeek) with routing, preset management, and prompt templating.
- **Core engines** — `backend/core/` has regime, TA signal, catalyst, and asset-weight engines.

---

### 4. KEY ARCHITECTURAL CHARACTERISTICS

#### 4a. Single-file routing — `main.py` size risk

All routes (~50+ endpoints) are defined as `@app.get/post` decorators in `backend/main.py`. The file is ~18,800 lines. While some sub-routers are registered (Hyperliquid, sector rotation, watchlist, etc.), the core query endpoint, auth, earnings, health, news, debug, and admin routes are all inline. This is a **maintainability risk** — the file is very large, making navigation, code review, and parallel development difficult.

#### 4b. Aggressive background task architecture

The `lifespan` context manager spawns **20+ background asyncio tasks** at startup:
- X Consensus daily loop (Grok/XAI refresh)
- Terminal cache pre-warm
- Home planning warmup (Saturday only)
- Odds scanner (30-min Polymarket scan)
- Investor intelligence (30-min refresh)
- Briefing precompute (30-min)
- Edgar cache loop (full nightly + intraday filings)
- EI materials loop (24-h SEC refresh)
- Master screener loop
- Sectors backfill loop
- Theme options supplement loop
- Polygon options ingestion loop
- Macro precompute loop
- Strategy history precompute loop
- Insider activity loop
- Congressional trading loop
- Canonical history maintenance
- Hyperliquid websocket
- Whale watch (seed + loop)
- Bittensor dashboard refresh
- Alert bus retention cleanup
- Watchlist fundamentals weekly (Sunday 02:00–05:00 ET)
- Watchlist rank snapshot (5-min cadence)
- Thematic context warmup
- Dynamic thematic universe (15-min)
- Theme RS warmup
- Earnings calendar warmup
- Earnings monitor tick loop
- Earnings catch-up pass
- Defiance 2X daily loop
- Post-yield bootstrap (15+ deferred steps)

This creates a **significant cold-start risk**. The lifespan explicitly acknowledges this: it uses a background thread (`_deferred_sync_startup`) to move Neon DB initialization and disk reads out of the synchronous yield path so the health check responds in < 2s. The file contains extensive comments documenting previous startup failures (Neon connections blocking for 20–30s, pushing past autoscale health check timeouts).

#### 4c. Multi-layered caching

- **In-memory** (`data.cache`): per-symbol Tradier quotes (60s TTL), macro data, briefing precompute, etc.
- **Disk JSON files** (`backend/data/`): LKG (last-known-good) files for options, themes, earnings snapshots, X consensus, sector rotation, etc. Loaded at startup into in-memory state.
- **Neon PostgreSQL**: Chat/prompt history, whale watch, screener fundamentals, options instrument types, category overrides, name overrides, RSS article archive.
- **File-based gzip** (`backend/data/canonical_history/`): 5-year price history per symbol as `.json.gz`.

The system explicitly prioritizes cache reads over provider calls, documented in the FMP cost control and provider-call audit endpoint.

#### 4d. Provider diversity and failover

`market_data_service.py` defines a `DATA_SOURCES` dict with primary/secondary providers per domain:
- Equity prices: Finnhub (primary) → FMP (secondary)
- Fundamentals: FMP → Finnhub
- Crypto: CoinGecko → CMC
- Macro: FRED → (none)
- Real-time quotes: Tradier → Public.com → FMP → Twelve Data → LKG fallback

API budget tracking (`api_budget.py`) enforces per-provider daily caps with warnings at 70% and hard-stops at 90%.

#### 4e. Auth — currently disabled

`JWTAuthMiddleware` is a pass-through (`__call__` directly invokes `self.app`). The `_jwt_or_key` function always returns `True` (auth is disabled). The comment states: "Re-enable when login page is ready." However, the auth endpoints (`/api/auth/login`, `/api/auth/verify`) themselves work — they validate credentials and issue JWT tokens. This is a **security gap**: the middleware that should enforce token validation on protected routes is explicitly disabled, while the login mechanism functions.

#### 4f. Subscription paywall

`subscription.py` implements a 402 paywall via FastAPI Depends. Only the `OWNER_USERNAME` (fallback to `AUTH_USERNAME`) or requests with a valid `X-API-Key` header matching `AGENT_API_KEY` pass through. The `/api/query` endpoint uses `require_subscription`.

#### 4g. Testing coverage

18 test files exist in `backend/tests/`, covering:
- Earnings FMP matching and revenue validation
- Options architecture and market session
- Realtime quotes
- Screener presets
- SEC EDGAR
- Startup timing
- XAI social fixes
- Calendar curation
- Web news routing
- Watchlist market data
- Etc.

No test runner config visible; tests appear to use `pytest` (listed in dependencies).

---

### 5. DATA PROVIDERS (backend/data/)

~50 modules categorized as:

| Category | Providers |
|---|---|
| **Equity/Market** | `fmp_provider.py`, `polygon_provider.py`, `finnhub_provider.py`, `tradier_provider.py`, `yahoo_finance_provider.py`, `twelvedata_provider.py`, `alphavantage_provider.py`, `public_com_provider.py`, `stockanalysis_scraper.py` |
| **Crypto** | `coingecko_provider.py`, `cmc_provider.py` |
| **Macro** | `fred_provider.py`, `fear_greed_provider.py`, `macro_provider.py` |
| **SEC/EDGAR** | `edgar_provider.py`, `sec_edgar_provider.py`, `ei_materials_cache.py` |
| **Options** | `polygon_options_provider.py`, `options_scraper.py`, `options_ingestion.py`, `options_flow_engine.py`, `options_history_store.py` etc. |
| **Social/Sentiment** | `stocktwits_provider.py`, `reddit_provider.py`, `xai_sentiment_provider.py` |
| **Web Search** | `tavily_provider.py`, `web_search_provider.py`, `brave_provider.py` |
| **Prediction Markets** | `polymarket_provider.py` |
| **Storage** | `pg_storage.py`, `cache.py`, `chat_history.py`, `prompt_history.py`, `user_settings.py` |

---

### 6. SERVICE PACKAGES (backend/services/)

| Sub-package | Purpose |
|---|---|
| `playbook/` | Bottleneck discovery, regime service, strategy screener, playbook scoring, comparison, theme maps |
| `predict/` | Polymarket/kalshi odds scanning, investor intelligence, event grouping, regime classification |
| `hyperliquid/` | Trade radar, signals, websocket manager, ranking engine, TSMOM |
| `sector_rotation/` | Theme universe, ETF holdings, sector stocks, analytics, Gemini analysis |
| `bittensor/` | Bittensor dashboard router |
| `playbook/prompts/` | LLM prompts for playbook engine |
| `playbook/strategy_screener/` | Strategy screener sub-service |

Standalone services (under `services/` directly): watchlist service, earnings monitor, options alignment, catalyst alignment, theme RS, confluence (v2, v4, v42), entry state, canonical history, news major, social X, insider activity, congressional trading, whale watch, FMP governance, actionability, investment alignment, notifAI, screener hub, and many more.

---

### 7. DATABASE SCHEMA (Neon PostgreSQL)

No ORM — raw SQL via `psycopg2-binary`. Tables created via `CREATE TABLE IF NOT EXISTS` in service modules. Known tables from codebase inspection:

- `public.conversations`, `public.messages` — chat/prompt history
- `public.screener_fundamentals_cache` — weekly FMP fundamentals snapshot
- `public.screener_universe_snapshots` — per-tab/per-theme universe snapshots
- `public.screener_quote_cache` — Tradier quote cache
- `public.screener_job_runs` — job audit trail
- `public.watchlist_fundamentals_cache` — fundamentals refresh cache
- `public.whale_watch_positions`, `public.whale_watch_filings` — whale 13F data
- `public.earnings_monitor_targets`, `public.earnings_monitor_results` — earnings monitoring
- `public.screener_options_oi_cache` — options OI data
- `public.category_overrides` — user-corrected category assignments
- `public.name_overrides` — user-corrected company names
- `public.rss_article_archive` — watchlist RSS articles
- `public.manual_anchor_bottlenecks` — manual anchor bottlenecks table
- `public.alert_signals` — alert signal bus
- `public.tracked_odds_history` — Polymarket odds history (7-day retention)
- `public.portfolio_holdings` — portfolio holdings
- `public.watchlist_rank_previous_snapshots` — rank snapshot persistence

---

### 8. NOTABLE OBSERVATIONS & RISKS

| # | Finding | Category |
|---|---|---|
| 1 | **`main.py` is ~18,800 lines** — all routes in one file. Hard to navigate, review, or refactor. | Maintainability |
| 2 | **Auth middleware disabled** — `JWTAuthMiddleware` is a pass-through despite login/verify routes working. Token validation on protected routes is absent. | Security |
| 3 | **20+ background loops** all spawned in the lifespan — high complexity and cold-start risk. System has history of deployment healthcheck timeouts. | Reliability |
| 4 | **Multiple LKG files written to `backend/data/`** — mixed in with source code. These are runtime state files, not code. | Hygiene |
| 5 | **`fix_main.py`** in the `backend/` directory — this is a script that patches `main.py` (the workspace root `main.py`, not `backend/main.py`). Appears to be a one-off repair script that was left in the codebase. | Dead code |
| 6 | **`fix_main.py` references `/home/runner/workspace/main.py`** (not `backend/main.py`) — there is no `main.py` at the workspace root. This script would fail to find its anchors. | Dead/broken code |
| 7 | **No `requirements.txt`** — only `pyproject.toml` with some deps. Missing `uv.lock` contents for reproducibility. | Build/reproducibility |
| 8 | **Dirty working tree** — 34 runtime data files modified (caches, LKGs, gz history files). Expected for running system, but makes `git status` noisy. | Hygiene |
| 9 | **Single SQL migration** — all other table creation is inline in service modules via `CREATE TABLE IF NOT EXISTS`. No migration history or versioning for most tables. | Database management |
| 10 | **Password reset emails credentials in code** — `reset_email.py` constructs HTML emails inline with hardcoded sender "noreply@caelynai.com". Minor, but injects product identity into code. | Minor |
| 11 | **`password_reset.py` sends password hash to server logs** — `print(f"[RESET] {new_hash}")` on line 1633 of `main.py` intentionally prints the new bcrypt hash to logs with instructions to paste into a secret. This is a design choice for serverless deployment without persistent env vars but is a security concern if logs are exposed. | Security |
| 12 | **Confluence audit already exists** — `CONFLUENCE_AUDIT.md` (873 lines) is a prior read-only audit of the Confluence scoring system, dated 2026-07-08. Confirms "Golden Zone" / "High Conviction" labels are LLM-generated, not algorithmic engines. | Prior audit evidence |
| 13 | **No CI/CD configuration visible** — no GitHub Actions, no `Dockerfile`, no deployment scripts in the repo. Deployment relies entirely on Replit's Autoscale platform. | Operations |
| 14 | **`codex.sh` at workspace root** — appears to be a launcher script for the Codex agent itself. | Agent infrastructure |

---

### 9. GIT STATE SUMMARY

```
Branch: main
Status: even with origin/main (0 ahead, 0 behind)
Dirty files: 34 (all runtime data: caches, LKGs, gz history, JSON snapshots)
No staged changes
Last commit: ... (see git log for details)
```

---

### 10. AUDIT SCOPE COMPLIANCE

- ✅ **Read-only**: No files edited, created, or committed
- ✅ **No application start/restart**
- ✅ **No database or cache mutation**
- ✅ **No dependency installation**
- ✅ **No branch/clone/worktree creation**
- ✅ **Ground truth from local files only**

---

### 11. SUMMARY

The Caelyn AI backend is a **feature-rich trading intelligence platform** with deep provider integration, multi-agent AI orchestration, and aggressive caching. Its strengths are provider diversity, layered caching, extensive background precomputation, and a service-per-domain organization. Its primary risks are the monolithic `main.py` file size, disabled auth middleware, synchronization complexity from 20+ concurrent background loops, and the tension between Replit Autoscale's health-check requirements and the heavy cold-start initialization path. The codebase has strong inline documentation explaining past architectural decisions and failure modes, suggesting active development with experienced operators.
