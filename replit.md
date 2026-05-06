# Caelyn AI

Caelyn AI is a FastAPI-based platform for real-time trading analysis, offering actionable insights across diverse asset classes through advanced screening, AI-driven analysis, and proprietary scoring engines.

## Run & Operate

```bash
# Run the FastAPI application
uvicorn backend.main:app --host 0.0.0.0 --port 5000 --reload

# Placeholder for build, typecheck, codegen, db push commands
_Populate as you build_
```

**Required Environment Variables:**
_Populate as you build_

## Stack

**Frameworks:** FastAPI
**Runtime Versions:** Python 3.x (specific version not provided, assume latest compatible)
**ORM:** _Populate as you build_ (Neon PostgreSQL mentioned for persistence, but ORM not specified)
**Validation:** Pydantic
**Build Tool:** _Populate as you build_

## Where things live

- **Backend Entrypoint:** `backend/main.py`
- **Thematic Context Provider:** `backend/services/thematic_context_provider.py`
- **Dynamic Thematic Universe Builder:** `backend/services/dynamic_thematic_universe.py`
- **Regime Engine:** `backend/core/regime_engine.py`
- **Context Broker (AI Agent Integration):** `backend/agent/context_broker.py`
- **Screener Hub Services:** `backend/services/playbook/strategy_screener/screener_router.py`, `backend/screener_hub_store.py`
- **Market Data Service:** `backend/data/market_data_service.py`
- **Data Schemas:** Defined via Pydantic models throughout `backend/` services.
- **DB Schema:** Managed by Neon PostgreSQL. Table `screener_options_oi_cache` for options data. (Specific schema file not provided)
- **API Contracts:** Defined implicitly by FastAPI routes and Pydantic models in `backend/` routers and services.
- **Theme Files:** `backend/services/thematic_context_provider.py` (`_KEYWORD_ETF_MAP`), `data/x_consensus_weekly.json`, `data/sector_rotation_analysis.json`.

## Architecture decisions

- **Microservices-like Structure:** Major analytical features are implemented as self-contained services.
- **Hybrid Caching Strategy:** In-memory state for real-time market data, disk-persistent caching for frequently accessed or static datasets for fast restarts.
- **Hierarchical Scoring Engines:** Utilized in screeners for multi-factor analysis and robust signal generation.
- **Multi-Agent AI Pipeline:** Prediction Markets feature uses a 6-agent Gemini pipeline for comprehensive market analysis.
- **Deterministic Regime Detection:** The Playbook Engine's `serenity-regime` ensures consistent results without external API calls for core detection.
- **Snapshot-First Calendar Pipeline:** Earnings calendar eliminates month-view hangs by pre-computing and serving weekly snapshots from cache/disk.
- **Non-blocking Options OI Enrichment:** Live options data is fetched in background tasks, cached, and served without blocking request threads.

## Product

- Real-time Hyperliquid perpetuals and spot market screener with multi-factor scoring.
- Comprehensive sector rotation analysis and AI-generated market commentary.
- Deep dive into SEC Form 4 insider activity with conviction scoring.
- Institutional 13F whale tracking and AI-summarized investment themes.
- Polymarket prediction market intelligence with edge detection and mispricing signals.
- Deterministic supply chain bottleneck discovery and strategic investment playbooks.
- Fundamental stock comparison tool and catalyst calendar.
- Real portfolio analytics.
- Thematic context layer and dynamic thematic universe building for enhanced screening.

## User preferences

No explicit user preferences were provided in the original `replit.md` file.

## Gotchas

- **FMP API Usage:** Excessive sequential calls to FMP for earnings data historically caused hangs; now mitigated by snapshot architecture.
- **External API Rate Limits:** Implement robust error handling and back-off strategies for all external API calls (Hyperliquid, Tradier, Finnhub, FMP, Gemini, Claude, Perplexity AI) to avoid rate limiting.
- **Cache Invalidation:** Ensure proper TTLs and invalidation mechanisms for cached data, especially for real-time market information.
- **`yfinance` and `edgartools` Reliability:** Monitor for potential changes or unreliability in these less formal data sources.

## Pointers

- **FastAPI Documentation:** [https://fastapi.tiangolo.com/](https://fastapi.tiangolo.com/)
- **Pydantic Documentation:** [https://pydantic-docs.helpmanual.io/](https://pydantic-docs.helpmanual.io/)
- **Hyperliquid API Docs:** _(Link not provided)_
- **Tradier API Docs:** _(Link not provided)_
- **FMP API Docs:** _(Link not provided)_
- **Google Gemini API Docs:** _(Link not provided)_
- **Anthropic Claude API Docs:** _(Link not provided)_