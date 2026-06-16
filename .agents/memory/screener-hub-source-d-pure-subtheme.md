---
name: Screener Hub Source D peers suppressed for pure_subtheme
description: FMP peer algorithm is unreliable for niche themes; must be gated off for pure_subtheme theme type
---

**Rule:** Source D (FMP peers) must be skipped when `_theme_type == "pure_subtheme"`.

Gate at line ~2295 of screener_hub_service.py:
```python
if with_fmp_peers and _theme_type != "pure_subtheme":
```

**Why:** `seen_dynamic` tracks only dynamically-discovered symbols (ETF holdings, LKG, FMP screener). Seed tickers are loaded via Source F (static), so `len(seen_dynamic)` is always 0 for seed-only themes. This means the `< _MIN_DYN_BEFORE_PEERS` guard always fires for pure_subthemes, triggering FMP peer lookup. FMP's peer algorithm anchors on industry peers of the candidate_syms — not keyword-matched companies — so for niche themes (e.g. photonics_lasers) it returns completely unrelated companies (COMM, BILI, FFIV, KSPI, etc.).

**How to apply:** Any time a pure_subtheme theme shows unexpected non-seed rows with `membership_source="fmp_screener"` and `membership_reason="FMP peer match"`, check if Source D gate is intact. Seeds + Source C/C2 (FMP industry screener + adjacent industries) are the correct discovery path for pure_subtheme.
