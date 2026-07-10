---
name: Entry State / Theme Rotation / Confluence V2 architecture
description: Phase 4/6/7 service files, endpoint prefixes, and key data-shape quirks discovered during implementation
---

## Endpoint prefix rule: /api/alpha/ not /api/watchlist/

The watchlist router (`services/watchlist_router.py`, prefix `/api/watchlist`) has a `@router.get("/{watchlist_id}")` catch-all route that silently intercepts any GET under `/api/watchlist/<anything>` and returns `{"empty": True}` when no matching watchlist is found.

**All new Phase 4/7 endpoints must live under `/api/alpha/` (or any non-watchlist prefix).**

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

## Stage2 LKG canonical field names (Fix 1)

`_stage_signal_from_lkg` must read `"score"` (0–100 float) and `"label"` (string e.g. "S2-S3 Advance") from the Stage2 LKG dict. Fields `"stage_score"`, `"stage_label"`, `"stage"` do NOT exist — reading them always returns None → default 0.5. After fix: signal range 0.000–1.000, stdev=0.310.

**Why:** Stage2 LKG is written by `watchlist_stage2_service._process_one` which serialises via `WatchlistStage2Result` Pydantic model. The canonical output fields are `score` and `label` at the top level of the dict.

## Confluence V2 weight normalization (Fix 3)

Base weights (4 signals, sum=1.0000): entry=0.353, theme=0.235, stage=0.235, options=0.177. Social is NOT a weighted signal — it is a conditional additive bonus (0–10 pts) applied AFTER the base score is computed. Outputs: `base_trade_confluence_score`, `social_bonus_score`, `trade_confluence_score`, `investment_confluence_score`, `confluence_score` (= trade, backward compat).

## Social bonus eligibility gate (data-availability-aware)

`_compute_social_bonus()` eligibility gate requires: entry_score >= 60 AND (theme_sig >= 0.55 OR stage_sig >= 0.65) AND (stage_sig >= 0.60 OR options_sig >= 0.65).

**Why:** Theme rotation coverage is chronically 0% (all symbols default to theme_sig=0.5). Using `stage_sig >= 0.65` as a proxy for theme confirmation prevents the gate from being permanently blocked when theme data is absent. Without this proxy, eligible_count is always 0 regardless of entry quality.

**How to apply:** If theme coverage remains 0%, the effective gate is `entry_score >= 60 AND stage_sig >= 0.65`. With full theme data, theme_sig >= 0.55 becomes the primary confirmation signal.

## HIGH_BASE entry state (Fix 4)

New CONTINUATION family state in `entry_state_service.py`. Detection requires: stage_int==2, ext_state not EXTREME_EXTENSION, breakout_state not in {failed, fresh_breakout, confirmed_breakout}, range_20d <= 20%, pct_from_20d_hi >= -8%. Base score=80 (max 90). Bonuses: coiling+5, range_contraction+3, near_high+3, accumulation+3, bull_ma+2, healthy_ext+2. Added to `_BASE_SCORES`, `_STATE_TO_FAMILY` (CONTINUATION), and checked before SIGNALS_BUILDING pre-check.

**Why:** Distinguishes mature Stage 2 bases near recent highs (low-risk continuation entries) from earlier-stage signal setups. SLAB and AME archetypal examples (both score 89, A+). ANET correctly remains SIGNALS_BUILDING (fresh breakout gate fires).

## force_warmup_stage2 admin endpoint

`POST /api/admin/stage2/force-warmup?force_all=true` — loads all watchlist tickers, calls `warmup_stage2(tickers, force=True)` bypassing all freshness TTLs. Added `force: bool = False` param to `warmup_stage2()` in `watchlist_stage2_service.py`. Always trigger via HTTP admin endpoint, never subprocess (subprocess destroys disk LKG — see stage2-lkg-subprocess.md).

## Actionability V1 layer
- New `actionability_service.py` combines Trade Alignment (THEME_ALIGNMENT) + Entry Structure V2 into 8 canonical states (READY/EARLY_WATCH/WATCH/WAIT_FOR_BREAKOUT/WAIT_FOR_RETEST/REVERSAL_WATCH/TOO_EXTENDED/AVOID); pure function of already-computed fields, no recomputation.
- Gotcha: `structure_v2.failed_breakout_confirmed` is a lingering diagnostic flag from an earlier phase of the same base and can stay True even after entry_state moves to a fresh state (e.g. TRENDLINE_SUPPORT_TEST) — do NOT use it as a standalone hard-AVOID trigger; only escalate when entry_state itself is FAILED_BREAKOUT.
- Additive fields `actionability_*` merged into confluence_v2_service results dict + `actionability_diagnostics` block in build_confluence_snapshot, no schema removal.
