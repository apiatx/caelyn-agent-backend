---
name: Entry State / Theme Rotation / Confluence V2 architecture
description: Phase 4/6/7 service files, endpoint prefixes, and key data-shape quirks discovered during implementation
---

## Endpoint prefix rule: /api/alpha/ not /api/watchlist/

The watchlist router (`services/watchlist_router.py`, prefix `/api/watchlist`) has a `@router.get("/{watchlist_id}")` catch-all route that silently intercepts any GET under `/api/watchlist/<anything>` and returns `{"empty": True}` when no matching watchlist is found.

**All new Phase 4/7 endpoints must live under `/api/alpha/` (or any non-watchlist prefix).**

Existing watchlist-router routes include: `/{watchlist_id}`, `/{watchlist_id}/news`, `/{watchlist_id}/stock/{ticker}`, etc.

## themes_rs_lkg.json leaders shape

`leaders` field is a **list of dicts** `{"symbol": "VRTX", "return_pct": 155.97, "timeframe": "5Y", "source": ...}` — NOT a list of strings.

Any code consuming `leaders` must normalise:
```python
leaders = [e["symbol"] if isinstance(e, dict) else e for e in raw_leaders if e]
```

## themes_rs_lkg.json RS score units

`rs_score` in `themes_rs_lkg.json` is in pct units relative to SPY (e.g. +15.3 = outperformed SPY by 15.3%).
Normalize to 0–1: `clamp01((rs_score + 20) / 40)`.
This is different from the stage-analysis `rs_vs_spy_8w_pct` field (same units but different lookback).

## entry_state_lkg.json

LKG file at `backend/data/entry_state_lkg.json`. Populated only when `analyze_entry_state_from_bars()` is called with actual daily bars. On a cold start it is empty — `/api/alpha/entry-state` correctly returns `symbol_count: 0` until bars are supplied.

**Why:** The engine is pure-computation: it needs daily bars + stage_result as inputs. These come from the watchlist stage2 refresh pipeline, not from a periodic background task. Wire `analyze_entry_state_from_bars()` into the stage2 warmup path to populate the LKG.

## Social backend_score max

Empirical max observed from `_backend_ranked` in `x_consensus_weekly.json`: ~14.0. Used as normaliser ceiling.
Entry: `social_sig = clamp01(backend_score / 14.0)`. Has-top-conviction applies a 1.15x boost (capped at 1.0).
