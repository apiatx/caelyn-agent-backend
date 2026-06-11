---
name: Screener Hub hardcoded mcap pre-filter removal
description: The row-build loop must NOT apply a hardcoded $50M-$10B mcap range; user filters at post-hydration stage do the filtering
---

## Rule
Do NOT add a hardcoded market cap range filter inside the row-build loop of `get_screener_hub`.

**Why:** A `if mcap < 50M or mcap > 10B: continue` inside the loop ran BEFORE `rows_before_filters` was set, and eliminated all symbols for themes whose universe is entirely mega-cap (e.g. ai_networking: ANET $193B, AVGO $2.28T). User-specified `market_cap_min/max` filters at the post-hydration stage (after `rows_before_filters`) are the correct place.

**How to apply:** Market cap filtering belongs only at lines 3443-3458 (user filter block). If a default range is ever needed, it must be at that same post-hydration stage and must be skipped when the user supplies an explicit filter.
