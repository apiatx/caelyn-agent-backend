---
name: Options Sectors neutral coverage architecture
description: How sectors achieves 99%+ optionable-symbol coverage; three bugs fixed and the correct dual-path architecture.
---

## The core problem
`run_live_scan()` only emits rows for symbols with **unusual/bullish/bearish** flow. Symbols with valid options but neutral/no unusual activity got no row and stayed "pending" forever — the root cause of 2-5% sector coverage.

## Fix 1 — Neutral rows from supplement loop (main.py)
After `run_live_scan()`, check `_local_expiry` (populated by Stage-1):
- `(exps, ts)` with non-empty exps → options confirmed, no unusual flow → write neutral row via `_update_supp()`
- `([], ts)` → no options → handled by `_upd_no_opts`
- absent → fetch failed/deferred → leave pending

Neutral row shape: `{ticker, _source:"supplement", premium:0.0, call_flow_pct:50.0, put_flow_pct:50.0, total_volume:0, heat_score:0.0, side_bias:"neutral", scan_result:"neutral_no_unusual_flow", cached_at, updated_at}`.

## Fix 2 — False no_options from budget deferrals (options_flow_engine.py Stage-1)
When `TradierProvider._get()` defers a call (budget exhausted), `get_option_expirations()` returns `None`. Before this fix, Stage-1 wrote `expiry_cache[symbol] = ([], ts)` for None responses — indistinguishable from "truly no options" — causing `_upd_no_opts` to permanently mark optionable stocks (AMAT, DELL, INTC, WDC, TER) as no_options.

**Fix**: Check `if all_exps is None: return symbol, None` BEFORE writing to expiry_cache. Only confirmed empty lists (real Tradier response) write `([], ts)`.

## Fix 3 — Master screener neutral rows (main.py after _upd_no_opts)
The master screener processes 60 symbols per cycle using the options_flow lane (fast, uncontested). After each cycle, scan `_master_expiry_cache` for theme symbols with non-empty expirations not in the unusual-flow results → write neutral rows. This fills coverage for theme symbols in the master prefilter without extra Tradier calls.

Theme universe is in `services.theme_merge_layer.ENRICHED_THEME_RS_UNIVERSE` (NOT `data.options_flow_sectors`). Symbol set: `meta.get("proxy_symbols")` across all values.

## Fix 4 — _build_ticker_node neutral bias (options_flow_sectors.py)
Added `scan_result` field to ticker node output. For rows with `scan_result == "neutral_no_unusual_flow"` and `has_prem=False`, set `bias="neutral"` instead of `None`.

## Steady-state architecture
- Master screener (every ~70s): 60 symbols/cycle → unusual flow rows + neutral rows for theme symbols
- Supplement loop (every 5min): covers theme-only symbols not in master prefilter; 6 effective symbols/batch
- LKG disk persistence: neutral rows saved to supplement_lkg on disk; survive restarts
- Coverage builds to ~100% in <60min; LKG from previous sessions loads instantly at startup

**Why:**
Budget deferrals from maintenance lane saturation (startup history warmup, whale watch) were silently corrupting the no_options set. The master screener neutral path bypasses the maintenance lane entirely (uses options_flow lane which has dedicated headroom).
