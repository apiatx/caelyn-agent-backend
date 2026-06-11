---
name: Screener Hub proxy_type=custom bug
description: _build_all_proxy_etfs must only include proxy_symbols from themes with proxy_type="etf", not "custom" (stock tickers)
---

## Rule
`_build_all_proxy_etfs()` must filter to `proxy_type == "etf"` themes only.

**Why:** Some themes (e.g. ai_networking) set `proxy_type="custom"` and use real stock tickers (ANET, AVGO) as proxy_symbols. If those tickers are in `_ALL_PROXY_ETFS`, every snapshot for that theme is treated as "ETF-only" → triggers a live rebuild on every request, and the tickers are stripped from screener rows.

**How to apply:** Any new theme added to `THEME_RS_UNIVERSE` with `proxy_type="custom"` or `proxy_type="stock"` will NOT pollute the ETF exclusion set. When adding themes, ensure proxy_type is set correctly.
