---
name: Screener Hub symbol identity sanity check
description: Before adding a ticker to exclude_tickers, verify the current company identity via Tradier or yfinance — FMP legacy profile API is not reliable
---

## The rule
Never add a ticker to `exclude_tickers` based on an assumed company identity.
Always verify the **current** company name against a live data source before excluding.

## Why
VISN was added to `ai_networking.exclude_tickers` under the incorrect assumption
that it was "Vision Marine Technologies" (electric boats — clearly unrelated to
networking). The actual company is **Vistance Networks, Inc.** — a legitimate
Technology/Communication Equipment company providing data-center and enterprise
networking infrastructure. The exclusion removed a relevant company from its correct theme.

## How to verify identity
1. **Tradier** (most reliable for current name):
   ```bash
   curl "https://api.tradier.com/v1/markets/quotes?symbols=VISN" \
     -H "Authorization: Bearer $TRADIER_TOKEN" -H "Accept: application/json"
   # → quote.description gives current company name
   ```
2. **yfinance** (good for sector/industry/summary):
   ```python
   import yfinance as yf
   info = yf.Ticker("VISN").info
   print(info.get("longName"), info.get("industry"), info.get("longBusinessSummary"))
   ```
3. **FMP profile API** — currently broken for non-legacy subscribers; do NOT rely on it.

## How to apply
Before adding any ticker to `exclude_tickers` in `theme_fmp_industry_map.json`:
1. Query Tradier `quote.description` for the current company name
2. If name doesn't match the exclusion reason → do NOT exclude; add to `seed_tickers` if it belongs
3. Document the exclusion reason as a JSON comment next to the ticker

## Reversal pattern
If a wrongly excluded ticker is identified:
1. Remove from `exclude_tickers` in `backend/data/theme_fmp_industry_map.json`
2. Add to `seed_tickers` in same JSON (if it belongs in the theme)
3. Add to `candidate_symbols` in `backend/services/theme_rs_universe.py`
4. Rebuild theme: `POST /api/admin/screener-hub/rebuild?tab=thematic&theme=<name>&force=true`
5. Verify via API: check `membership_source=seed`, `membership_confidence=high`, `theme_role=core`
