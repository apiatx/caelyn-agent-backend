"""
Serenity Strategy Screener subsystem.

Generates proactive supply-chain bottleneck snapshots on a configurable cadence,
persists them to Neon PostgreSQL, and serves a frontend-ready list + deep-report API.

Completely isolated from /api/query and the AI terminal.
Reuses: run_discover, run_analyze, compute_serenity_regime (no duplication).
"""
