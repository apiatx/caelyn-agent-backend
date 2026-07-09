---
name: Confluence V2 Theme Rotation bridge
description: How canonical Theme membership connects to Theme Rotation and Confluence V2 theme_sig; the 0% coverage root cause.
---

`confluence_v2_service._load_themes_rs_index()` was dead code: `themes_rs_lkg.json`'s `leaders` field is a list of dicts (`{"symbol":..,"return_pct":..}`), not strings. The old code did `sym.upper()` on raw leaders, threw on the first row, was swallowed by a broad try/except wrapping the whole function, and returned `{}` — so 100% of tickers silently got `theme_sig=0.5 / "NOT_IN_THEME"`.

**Why:** a single try/except around an entire index-build function hides total failure as a legitimate empty result — the caller can't distinguish "no themes exist" from "index build crashed."

**How to apply:** don't trust `themes_rs_lkg.json`'s `leaders` field to be homogenous; always normalize dict-or-string before calling `.upper()`. For canonical ticker→Theme membership, use `services.theme_merge_layer.ENRICHED_THEME_RS_UNIVERSE` (union of `candidate_symbols`+`proxy_symbols` per theme; already includes manual Neon overrides). For theme-level rotation state (LEADING/CONFIRMING/STALLING/BOTTOMING/LAGGING/UNCLASSIFIED), reuse `services.theme_rotation_service.build_theme_rotation_snapshot()` — it already normalizes the leaders-as-dicts case correctly and is cache/LKG-only (no provider calls). New adapter connecting the two lives in `services/theme_bridge.py`. When a ticker has no canonical membership or its themes have no rotation result, treat the signal as **unavailable** (omit from the weighted-average denominator) rather than defaulting to a neutral 0.5 — a defaulted neutral silently dilutes/inflates every downstream score and hides the true coverage gap.
