---
name: Tracked Odds Registry architecture
description: Architecture and integration for the durable Polymarket Tracked Odds Registry with Neon snapshot history.
---

## The rule
Three-layer pipeline: `odds_registry.py` defines families → `odds_scanner.py` fetches/matches/persists → `predict_odds_store.py` owns the Neon table. `get_intelligence()` consumes the scanner payload and falls back to `build_tracked_odds()` only on cold start.

**Why:** The old `build_tracked_odds()` in `tracked_odds.py` was a pure in-memory function — no persistence, no real delta history (used Polymarket's own price_change fields). The new pipeline stores one snapshot per 30-min scan cycle and computes true 1h/24h/7d deltas from DB history once enough rows accumulate.

## Key design decisions

### Loop timing
`_odds_scanner_loop` starts at 90 s; `_investor_intelligence_loop` starts at 120 s. The 30-second gap ensures the scanner is warm before `get_intelligence()` fires its first build. Never shrink this gap below 30 s.

### Delta fallback chain
1. DB history (get_snapshots_before with window tolerance)
2. Polymarket's own `price_change_1h/1d/1wk` API fields
3. None (returned as null in payload)

### Match count vs live markets
Low match count (e.g. 2/26 on a quiet day) is correct behavior. Most families (Fed meetings, Bitcoin milestones, SPX daily) only have active Polymarket markets near their relevant events. Do not treat low match count as a bug.

### Neon table
`public.prediction_market_odds_snapshots` — BIGSERIAL PK, indexed on (family_key, captured_at DESC). Retention runs on every scan cycle (DELETE WHERE captured_at < NOW() - INTERVAL '7 days').

### File locations
- Registry: `backend/services/predict/odds_registry.py`
- Store: `backend/data/predict_odds_store.py`
- Scanner: `backend/services/predict/odds_scanner.py`
- Endpoints: added to `backend/services/predict/router.py`
- Loop: `_odds_scanner_loop()` in `backend/main.py`
- Intelligence integration: `backend/services/predict/investor/investor_intel.py` lines ~269-284
- Old pure-function stub preserved: `backend/services/predict/investor/tracked_odds.py`

## How to apply
- When adding new tracked families: edit `ODDS_REGISTRY` list in `odds_registry.py` only. Scanner and intelligence pick it up automatically.
- When changing delta windows: edit the `_DELTA_*` constants in `odds_scanner.py`.
- When debugging missing families: check `/api/predict/odds/diagnostics` for `matched_families` and DB row count.
