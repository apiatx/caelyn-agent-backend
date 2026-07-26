---
name: chain_score_helper — supplement_v2 scoring architecture
description: How chain_score_helper.py uses OptionsFlowEngine without duplication, and how supplement_v2 enriches sectors backfill rows.
---

## Rule
`chain_score_helper.py` must use OptionsFlowEngine scoring methods via a `__new__()`-bypassed singleton — never copy scoring formulas.

**Why:** The scoring formulas (flow, asymmetry, gamma, volatility, sentiment) live in one place: `OptionsFlowEngine`. Duplicating them creates drift. The singleton (`_SCORER`) is initialised once via `OptionsFlowEngine.__new__()`, setting `defaults` and `weights` manually; `data_service` is never needed for scoring.

**How to apply:** When adding new scoring helpers, always check if OptionsFlowEngine has the method first. Call it via the singleton. Never inline the formula.

## supplement_v2 schema fields (sectors_chain_summarizer output)
New fields added by `summarize_ticker_chain()` in supplement_v2:
- `call_oi`, `put_oi`, `total_oi` — summed from ALL chain contracts (not volume-filtered)
- `call_iv`, `put_iv`, `combined_iv`, `iv_skew` — mean IV from contracts with volume > 0
- `expected_move_dollars`, `expected_move_pct`, `expected_move_atm_strike` — ATM straddle
- `underlying_price` — from cache lookup (master screener), no extra API calls
- `options_score`, `options_signal`, `score_components`, `score_method` — chain scoring
- `premium_put_call_ratio` = put_prem/call_prem (dollar flow)
- `volume_put_call_ratio` = put_vol/call_vol (contract count)
- `put_call_ratio` — backward-compat alias = premium_put_call_ratio
- `supplement_schema_version` = "supplement_v2"

## EM unit convention
The `expected_move_pct` field in supplement_v2 is in **percent form** (e.g., 4.33 = 4.33%).
In `portfolio_options_service._merge_options_sources()`, `em_f = _s_em_pct` (no division by 100).
The LKG `em` field also stores in percent form; `em_out = round(em_f * 100, 2)` scales to display (e.g., 4.33 → 433 basis-point-scale display).

## Guard 3 — non-destructive merge
`update_supplement_cache()` Guard 3: when a new scan row has None for rich fields (OI/IV/EM/score) but existing LKG row has them populated, preserve the existing values.
Applies after Guard 1 (no false confirmed_no_options) and Guard 2 (no zero-premium coverage overwrite).

## Supplement fallback in _merge_options_sources()
After supplement_v2, `_s` (supplement row) is now a fallback source for:
- score/signal: `(_s or {}).get("options_score")` / `options_signal`
- IV: `combined_iv`, `call_iv`, `put_iv`
- OI: `call_oi`, `put_oi`, `total_oi`
- EM: `expected_move_pct` (via `_s_em_pct`)
Priority: master > LKG > supplement (same as always; supplement is last resort).
