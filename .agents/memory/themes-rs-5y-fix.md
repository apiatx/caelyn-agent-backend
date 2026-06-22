---
name: Theme RS 5Y history fix
description: Why 5Y returns were null and how the fallback chain was fixed when FMP_BLOCK_FULL_HISTORICAL=true
---

## Rule
When FMP_BLOCK_FULL_HISTORICAL=true, the proxy history fallback chain (Tradier → yfinance) must use enough days to cover the requested TF bar count.

## Why
- _TIMEFRAME_BARS["5Y"] = 1250 — needs 1251 bars for _pct_change
- With FMP blocked, yfinance is the history source via `fetch_etf_history(symbol, days)`
- providers.py mapped days>252 → "2y" period (only ~504 bars) — insufficient
- _fetch_proxy_history hardcoded days=400 regardless of TF

## How to apply
- providers.py period mapping: `"5y" if days > 1200 else "2y" if days > 252 else "1y"`
- _fetch_proxy_history(symbol, days=400) — now accepts days param
- _compute() passes proxy_hist_days=1900 for tf=="5Y", else 400
- _FMP_HIST_RANGE_DAYS set to 1900 (applies when FMP is unblocked; 1900 cal days ≈ 1311 trading bars)
- Cadence guard: if 5Y is still null after code fix, reset theme_rs_refresh_ts.json 5Y to 0.0
