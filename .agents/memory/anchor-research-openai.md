---
name: Anchor Research — OpenAI Responses API
description: How the monthly anchor supply-chain research pipeline works after Claude was replaced with OpenAI web search.
---

## Architecture

- **Service**: `backend/services/anchor_research_service.py`
- **Prompt**: `backend/services/playbook/prompts/serenity_anchor_bottleneck_research_v1.py`
- **Store**: `backend/data/screener_hub_store.py` — `quarantine_anchor_research_nodes`, `upsert_anchor_research_nodes`
- **Routes**: `backend/routes/screener_hub.py` — POST `/api/admin/bottlenecks/research-anchor`

## Model

- OpenAI Responses API (`client.responses.create`)
- Tool: `{"type": "web_search_preview"}`, `tool_choice="required"` (forces web search)
- Default model: `gpt-4o` (env var: `OPENAI_ANCHOR_RESEARCH_MODEL`)
- Claude/Anthropic: completely removed from this path

## Quarantine Flow

1. `quarantine_anchor_research_nodes(anchor_key)` — moves `approved → pending_review` (preserves audit trail)
2. Unique index is **partial**: `WHERE research_status = 'approved'` — allows pending_review rows to coexist
3. `upsert_anchor_research_nodes(...)` — DELETEs only `approved` rows (no-op after quarantine), then INSERTs new approved

**Why:** Full unique index on (anchor_key, ticker, company_name) caused INSERT conflicts when pending_review and new approved rows had the same ticker. Partial index resolves this.

## DDL Changes

- `tradingview_symbol TEXT`
- `source_titles JSONB`
- `web_search_sources JSONB`
- `ticker_validated BOOLEAN`
- Old `idx_arcn_anchor_ticker` (full) → removed from DDL
- New `idx_arcn_anchor_ticker_approved` (partial, WHERE research_status='approved') → in `_arcn_migrate_sql()`

## Source URL Behavior

- `web_search_sources` (from Responses API annotations): usually empty for structured JSON requests — the model doesn't emit URL citation annotations when asked to produce JSON
- `source_urls` / `source_titles`: populated from model's JSON fields — these are URLs the model included based on its web search context, not formal citation metadata
- Web search IS confirmed to fire (web_searches_fired > 0 in response)

## Ticker Validation

- Batch call `get_quotes` → price found = validated
- Fallback `get_fundamentals` for cache misses
- Cold-cache tickers (real but not in cache) → `ticker_validated=False`, still `approved`
- All nodes approved regardless of validation status; `ticker_validated` field surfaces the gap

## Hard Rules

- Prompt enforces public-companies-only
- `is_public` always set True for approved rows
- `_looks_valid_ticker()` rejects empty, CONFIRM, PRIVATE, N/A patterns
- Minimum 1 evidence string per node
- Minimum 1 source URL per node
- Minimum 5 nodes total or research fails

## Monthly Cadence

- One call per anchor per 30 days (freshness gate)
- Anchors: SPCX, OPENAI, ANTHROPIC (sequential, never parallel)
- Weekly Chain Reaction scoring: zero LLM calls (reads cached approved rows only)
