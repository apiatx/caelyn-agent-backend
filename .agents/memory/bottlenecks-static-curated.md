---
name: Bottlenecks static curated anchors
description: Chain Reaction / Bottlenecks anchor maps switched from LLM/web-search research to deterministic static curated data + Postgres manual overlay.
---

## What changed

The 4 LLM admin routes (`research-anchor`, `revalidate-anchor`, `research-anchors-monthly`, `research-status`) all return `{"status":"disabled_static_curated_mode"}`.

Static curated maps live in `backend/services/playbook/curated_anchor_bottlenecks.py` — 6 anchors: SPCX(28), ANTHROPIC(24), NVDA(18), OPENAI(25), TSM(19), GOOG(22). Each row has the full Chain Reaction–compatible shape plus curated-specific fields (`evidence_grade`, `relationship_specificity`, `source_type`, `last_curated_at`, etc.).

Manual overlay CRUD (admin-only) uses `backend/data/manual_anchor_bottlenecks_store.py`, which uses `data.pg_storage._get_conn()` / `_put_conn()` — **not** `data.db` which does not exist.

## Public endpoints added

- `GET /api/bottlenecks/anchors` — list all 6 anchors with row counts
- `GET /api/bottlenecks/anchor/{anchor_key}` — full curated rows for one anchor (merges manual overlay)
- `GET /api/bottlenecks/anchor-overlap` — tickers appearing in 2+ anchors, sorted by count

## Admin CRUD endpoints (X-API-Key required)

- `GET /api/admin/bottlenecks/manual-nodes`
- `POST /api/admin/bottlenecks/manual-node`
- `PUT /api/admin/bottlenecks/manual-node/{id}`
- `DELETE /api/admin/bottlenecks/manual-node/{id}` — soft-delete (is_active=False)

## Key patterns

- `_get_conn()` / `_put_conn()` from `data.pg_storage` — always use try/finally for put_conn
- `manual_node_to_cr_row()` converts a DB row to the Chain Reaction row shape for merging into anchor detail responses
- `/api/bottlenecks/current` is untouched — still returns `data_source: chain_reaction_weekly_outputs`

**Why:** LLM research was slow, expensive, and produced inconsistent results; static maps are deterministic and zero-latency.
