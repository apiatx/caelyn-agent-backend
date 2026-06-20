---
name: Bottlenecks anchor research pipeline
description: Monthly LLM-driven overlay for private anchors (SPCX/OPENAI/ANTHROPIC) in the Chain Reaction / Bottlenecks feature
---

## Architecture

Three new anchors (SPCX, OPENAI, ANTHROPIC) have no entries in NODE_REGISTRY.
Their supply-chain nodes come from monthly LLM research stored in Neon.

### Key files
- `backend/services/playbook/prompts/serenity_anchor_bottleneck_research_v1.py` — canonical prompt (must not be edited without bumping PROMPT_VERSION)
- `backend/services/anchor_research_service.py` — async LLM runner; uses claude-3-5-sonnet-20241022 via asyncio.to_thread
- `backend/data/screener_hub_store.py` — bottom of file: anchor_supply_chain_research_nodes + anchor_research_runs DDL and CRUD

### giant_anchors filter (critical rule)
`build_anchor_top()` now filters by `row["giant_anchors"]` containment, NOT by theme intersection.

**Why:** Theme-based filtering gave OPENAI and ANTHROPIC the same rows (both mapped to ai_infrastructure). giant_anchors is set per-node in NODE_REGISTRY and in LLM-researched overlay nodes.

**GOOG → GOOGL alias:** GOOG appears in MULTI_ANCHOR_CONFIGS but NODE_REGISTRY uses "GOOGL" in giant_anchors lists. `_ANCHOR_GA_ALIAS = {"GOOG": "GOOGL"}` in chain_reaction_weekly_service.py handles this.

### Overlay anchor flow
1. Admin calls `POST /api/admin/bottlenecks/research-anchor?anchor=OPENAI`
2. LLM returns JSON array of nodes; validated, written to `anchor_supply_chain_research_nodes`
3. Next Sunday weekly job picks up the DB overlay and merges into `chain_reaction_weekly_outputs`
4. `build_anchor_top()` reads from weekly output first; if empty (pre-weekly-job), falls back to in-memory scoring of raw DB nodes
5. If no research exists at all → `{"status": "needs_research"}` (not an error)

### DB tables (Neon)
- `anchor_supply_chain_research_nodes` — approved researched nodes per anchor
- `anchor_research_runs` — audit log per LLM call
- Unique index on (anchor_key, ticker, company_name)
- `ensure_anchor_research_tables()` has its own `_ARCN_DDL_APPLIED` flag (separate from main ensure_tables())

### Admin routes (all require X-API-Key)
- `POST /api/admin/bottlenecks/research-anchor?anchor=SPCX&force=false`
- `POST /api/admin/bottlenecks/research-anchors-monthly?force=false`
- `GET  /api/admin/bottlenecks/research-status`

### LLM rules (invariants)
- Zero LLM calls in weekly job or on any page load
- One call per anchor per 30-day cycle (freshness gate)
- Calls run sequentially — never in parallel
- Model: claude-3-5-sonnet-20241022 (MODEL constant in anchor_research_service.py)

### MULTI_ANCHOR_CONFIGS
Added `is_overlay_anchor: bool` flag. Overlay anchors: SPCX, OPENAI, ANTHROPIC.
`anchor_themes` kept for informational metadata only — no longer used for filtering.

### needs_research status
`bottlenecks_multi_anchor` route treats `needs_research` as informational (not a partial_failure). Frontend should show a "Run research" CTA for those anchors.
