# Caelyn AI — Data Provider Source Map (Read-Only Audit)

**Scope**: Which provider supplies price, volume, bid/ask, 1D%, and intraday movement
for every front-end page. No code was changed. Findings are read-only observations.

**Audit date**: 2026-04-29  
**Codebase files inspected**: tradier_provider.py, macro_provider.py, home_service.py,
social_screener_service.py, sector_rotation/providers.py, sector_stocks.py,
discovery_enrichment.py, insider_activity_service.py, whale_watch_service.py,
tradier_flow_engine.py, unified_options_engine.py, options_flow_engine.py,
stock_compare_service.py, fundamentals_enricher.py, caelyn_terminal.py,
market_data_service.py, main.py (portfolio/quotes route ~L4392)

---

## 1. HOME PAGE

**Route**: `GET /api/home`  
**Code**: `services/home_service.py`

| Data element | Provider | Fields returned | Cache TTL | Real-time? |
|---|---|---|---|---|
| Watchlist tickers (snapshot + highlighted) | **Tradier** `get_quotes()` batch | last, change_percentage, volume, average_volume | 60 s (home) | ✅ Yes |
| Portfolio holdings snapshot | **Tradier** `get_quotes()` batch (same call) | last, change_percentage, volume | 60 s | ✅ Yes |
| Sub-theme stocks (THEME_MAP) | **Tradier** `get_quotes()` batch (same call) | last, change_percentage | 60 s | ✅ Yes |
| Benchmark ETFs (SPY/QQQ/TLT/GLD/USO/HYG) | **Tradier** via `MacroProvider.tradier.get_quotes()` | last, change_percentage, prevclose | 15 min (macro) | ✅ Yes |
| Market gainers / losers | **FMP** `biggest-gainers` / `biggest-losers` | price, changesPercentage | 5 min (FMP_TTL) | ~5 min lag |
| Macro cards (VIX, 10Y rates, Gold, DXY) | FRED + Yahoo (DXY) + FMP + Tradier (GLD/HYG) | varies by indicator | 15 min | ~15 min lag |
| Unusual options flows panel | Cached options screener result (Tradier-sourced) | composite_score, call/put vol, price_change_pct | 10 min (fast cache) | ✅ Yes (via screener) |

**Bid/Ask on Home**: ❌ Not exposed  
**Intraday movement**: `change_1d_pct` from Tradier `change_percentage`  
**Missing field**: `volume=null` never occurs here — Tradier always returns volume

---

## 2. CALENDAR PAGE

**Routes**: `GET /api/catalysts/overview`, `GET /api/catalysts/events`, `GET /api/catalysts/by-symbol`  
**Code**: `services/catalyst_calendar_service.py` + `data/fmp_provider.py`

| Data element | Provider | Fields returned | Cache TTL | Real-time? |
|---|---|---|---|---|
| Earnings calendar | **FMP** `earnings-calendar` | symbol, date, epsEstimated | 30 min | No |
| Dividend calendar | **FMP** `dividend-calendar` | symbol, date, dividend | 30 min | No |
| IPO calendar | **FMP** `ipo-calendar` | symbol, date, priceRange | 30 min | No |
| Economic calendar | **FMP** `economic-calendar` | event, date, actual, forecast | 30 min | No |
| SEC filings | **FMP** `sec-filings` | type, date, url | 30 min | No |
| Analyst ratings | **FMP** `analyst-stock-recommendations` | date, strongBuy/Buy/Sell | 30 min | No |

**Price / Volume / Bid-Ask on Calendar**: ❌ None — event data only  
**Tradier replacement opportunity**: N/A — Tradier has no calendar endpoints

---

## 3. SOCIAL PAGE

**Routes**: `GET /api/social/x-dashboard`, `GET /api/social/fundamental-screener`  
**Code**: `services/social_screener_service.py`, `services/social_x_service.py`

| Data element | Provider | Fields returned | Cache TTL | Real-time? |
|---|---|---|---|---|
| Current price | **FMP** `quote` | price | **30 min** | ⚠️ No |
| Volume | **FMP** `quote` | volume | **30 min** | ⚠️ No |
| 1D % change | **FMP** `quote` | changesPercentage | **30 min** | ⚠️ No |
| 5D / 1M / YTD / 1Y % changes | **FMP** `stock-price-change` | 5D, 1M, YTD, 1Y pct | **30 min** | No (OK for these) |
| Company profile (sector, mkt cap) | **FMP** `profile` | sector, mktCap, description | 24 h | No (OK) |
| Fundamental ratios (P/E, P/S, etc.) | **FMP** `ratios-ttm`, `key-metrics-ttm` | pe, ps, roe | 12 h | No (OK) |
| Social sentiment / trending | Stocktwits scraper | ticker, watchlist_count | 2 min | ✅ Approx |

**Bid/Ask on Social**: ❌ Not exposed  
**⚠️ Tradier gap**: Current price, volume, and 1D% are stale up to 30 minutes due to FMP quote TTL.
Tradier replacement recommendation: use `tradier.get_quotes()` for `price`, `volume`, `change_percentage` (1D%) on social screener tickers, keeping FMP for 5D+ historical % changes and fundamental ratios (Tradier provides neither).

---

## 4. FUNDAMENTALS PAGE (Stock Compare)

**Routes**: `GET /api/portfolio/compare-watchlist/latest`, `POST /api/portfolio/compare-watchlist/run`  
**Code**: `services/stock_compare_service.py`, `services/fundamentals_enricher.py`

| Data element | Provider | Fields returned | Cache TTL | Real-time? |
|---|---|---|---|---|
| Current price (top card) | **FMP** `quote` | price | 5 min (FMP_TTL) | ~5 min lag |
| 1D % change | **FMP** `quote` | changesPercentage | 5 min | ~5 min lag |
| Historical price series | **FMP** `historical-price-eod/full` | close, adjClose | ~1 h (per call) | No (EOD) |
| Income statement | **FMP** `income-statement` | revenue, netIncome | 12 h | No (quarterly) |
| Balance sheet | **FMP** `balance-sheet-statement` | totalDebt, equity | 12 h | No |
| Cash flow | **FMP** `cash-flow-statement` | freeCashFlow, capex | 12 h | No |
| Key metrics & ratios | **FMP** `key-metrics`, `ratios-ttm` | pe, ps, ev/ebitda | 12 h | No |

**Volume on Fundamentals**: FMP `quote.volume` (5 min)  
**Bid/Ask**: ❌ Not exposed  
**⚠️ Note**: FMP is correct for historical fundamentals and EOD series. Current price/volume could be served by Tradier for intraday freshness (5 min FMP lag acceptable for most fundamental-analysis use cases).

---

## 5. WATCHLIST PAGE

**Routes**: `GET /api/watchlist`, `POST /api/watchlist`, `POST /api/watchlist/:id/refresh`  
**Code**: `services/watchlist_router.py`, `services/watchlist_service.py`

| Data element | Provider | Fields returned | Cache TTL | Real-time? |
|---|---|---|---|---|
| Live price / change (Home watchlist card) | **Tradier** `get_quotes()` (via home_service) | last, change_percentage, volume | 60 s | ✅ Yes |
| Fundamental data (CSV or enriched) | **FMP** `quote` + `profile` + `ratios` (via fundamentals_enricher) | price, mktCap, pe | FMP_TTL 5 min | ~5 min lag |
| AI analysis (watchlist refresh) | market_data_service: Tradier → Finnhub for snapshot | last, change_pct | request-time | ✅ Yes |

**Bid/Ask**: ❌ Not on the watchlist page  
**Status**: ✅ Tradier already primary for live price on the home watchlist panel

---

## 6. PORTFOLIO PAGE

**Route**: `POST /api/portfolio/quotes`  
**Code**: `main.py` ~L4392

| Data element | Provider | Fields returned | Cache TTL | Real-time? |
|---|---|---|---|---|
| Stock price (PRIMARY) | **Finnhub** `/api/v1/quote` | c (price), d (change), dp (change%), h, l | 60 s | ✅ Yes |
| Stock volume | **Finnhub** | **`null` — always missing** | 60 s | ❌ Never returned |
| Stock price (fallback 1) | **Yahoo Finance** chart API | price, change, change_pct, volume | 60 s | ✅ Approx |
| Stock price (fallback 2) | **FMP** `stable/quote` | price, change, changesPercentage, volume, avgVolume | 60 s | ~5 min lag |
| Crypto | **CoinGecko** (dynamic ID lookup) → CMC fallback | price, change_24h | 60 s | ✅ Yes |
| Commodities | **FMP** commodity quotes | price | 60 s | ~5 min lag |
| Indices (SPY, VIX, etc.) | **Yahoo** chart API, VIX from **FRED** | price, change, change_pct | 60 s | ✅ Approx |

**Bid/Ask**: ❌ Not exposed  
**⚠️ Major Tradier gap**: Stocks use Finnhub as primary — Finnhub never returns volume (`volume: null`). Tradier is not in this route at all. Recommended: add Tradier `get_quotes()` as primary (matches rest of app), move Finnhub to fallback. Tradier returns volume + average_volume natively.

---

## 7. SECTORS / THEMES PAGE

**Routes**: `GET /api/sector-rotation/dashboard`, `GET /api/sector-rotation/sectors`  
**Code**: `services/sector_rotation/providers.py`, `services/sector_rotation/sector_stocks.py`

| Data element | Provider | Fields returned | Cache TTL | Real-time? |
|---|---|---|---|---|
| Sector ETF quotes (SPY, XLK, XLF, etc.) | **Tradier** `_tradier_quotes_batch()` | price, change_1d_pct, prev_close, day_high, day_low | 120 s | ✅ Yes |
| Sector ETF quotes (fallback) | **Finnhub** `/quote` single-ticker | price (derived from c, pc) | 120 s | ✅ Yes |
| Individual sector stocks (momentum) | **Tradier** `_tradier_quotes_batch()` | price, change_1d_pct | 120 s | ✅ Yes |
| Historical bars (7D / 30D / YTD / 1Y) | **yfinance** daily OHLCV | close prices | 1 h | EOD only |

**Volume on Sectors**: Tradier provides volume but sector page only surfaces `change_1d_pct` for sorting  
**Bid/Ask**: ❌ Not exposed  
**Status**: ✅ Tradier is primary for all live price data

---

## 8. SCREENER PAGE (Strategy Screener / Playbook)

**Routes**: `POST /api/strategy-screener/discover`, `POST /api/strategy-screener/analyze`  
**Code**: `services/playbook/discovery_enrichment.py`, `services/playbook/discovery_service.py`

| Data element | Provider | Fields returned | Cache TTL | Real-time? |
|---|---|---|---|---|
| US / ADR tickers — price | **Tradier** `tradier_quote()` (per-ticker) | price, change_pct, volume, bid, ask, week52_high, week52_low | **1 min** | ✅ Yes |
| US / ADR tickers — price (fallback) | **Finnhub** quote | price | 10 min (FINNHUB_TTL) | ✅ Yes |
| Non-US tickers | Finnhub only | price | 10 min | ✅ Yes |
| Market cap | **FMP** `profile` (sparing, 250/day cap) | marketCap | 5 min | No (OK) |

**Bid/Ask**: ✅ Tradier `bid` + `ask` fully populated  
**Volume**: ✅ Tradier `volume`  
**Status**: ✅ Best configured page — Tradier primary with bid/ask and volume

---

## 9. INSIDER ACTIVITY PAGE

**Routes**: `GET /api/insider-activity`, `GET /api/congressional-trading`  
**Code**: `services/insider_activity_service.py`, `services/congressional_trading_service.py`

| Data element | Provider | Fields returned | Cache TTL | Real-time? |
|---|---|---|---|---|
| Current price (PRIMARY) | **Tradier** batch `/markets/quotes` | last (as current_price), 52w_high, 52w_low | request-time | ✅ Yes |
| Current price (fallback 1) | **Finnhub** `/quote` | c (as current_price) | request-time | ✅ Yes |
| Current price (fallback 2) | **yfinance** `Ticker.info` | currentPrice / regularMarketPrice | request-time | ✅ Approx |
| Price at filing date | **yfinance** historical | close price on transaction date | request-time | EOD historical |
| % from 52w high | Derived from Tradier last + 52w_high | computed | — | ✅ |

**Bid/Ask**: ❌ Not exposed  
**Volume**: Not tracked (page shows % from 52w high, not volume)  
**Status**: ✅ Tradier is primary for current price

---

## 10. WHALE WATCH PAGE

**Routes**: `GET /api/whale-watch/*`, `GET /api/whale-watch/positions`  
**Code**: `services/whale_watch_service.py`

| Data element | Provider | Fields returned | Cache TTL | Real-time? |
|---|---|---|---|---|
| Current holding price (PRIMARY) | **Tradier** batch `/markets/quotes` | last, 52w_high, 52w_low | request-time | ✅ Yes |
| Current holding price (fallback) | **yfinance** `Ticker.info` | currentPrice / regularMarketPrice | request-time | ✅ Approx |
| Historical price for return calc | **yfinance** historical daily | close price | request-time | EOD historical |

**Bid/Ask**: ❌ Not exposed  
**Volume**: Not tracked (page shows portfolio positions, not stock volume)  
**1D%**: Derived from Tradier `last` vs. `prevclose`  
**Status**: ✅ Tradier is primary

---

## 11. OPTIONS FLOW PAGE

**Routes**: `GET /api/options-flow`, options screener master scan  
**Code**: `data/tradier_flow_engine.py`, `data/options_flow_engine.py`, `data/unified_options_engine.py`

| Data element | Provider | Fields returned | Cache TTL | Real-time? |
|---|---|---|---|---|
| Equity spot price (seed tickers) | **Tradier** `get_quotes()` batch | last, change_percentage, volume, average_volume | ~60 s | ✅ Yes |
| Equity spot price (per-candidate backfill) | **Tradier** `get_quote()` single | last, change_percentage | ~60 s | ✅ Yes |
| Option chain (all strikes/expiries) | **Tradier** option chains with greeks | bid, ask, volume, open_interest, IV, delta, gamma, theta, vega | ~90 s | ✅ Yes |
| Seed ticker list (initial price_hint) | **Finviz** unusual-volume screener | ticker, price, volume | ~5 min | Approx |

**Bid/Ask**: ✅ Both equity (Tradier `bid`/`ask`) and options (Tradier chain `bid`/`ask`) fully populated  
**Volume**: ✅ Tradier stock volume and options contract volume  
**1D%**: ✅ Tradier `change_percentage`  
**Status**: ✅ Tradier exclusive for all options data; Finviz only seeds the ticker universe

---

## Summary: Tradier Coverage vs. Gaps

| Page | Live Price Source | Volume Source | Bid/Ask | 1D% | Gap? |
|---|---|---|---|---|---|
| Home | ✅ Tradier | ✅ Tradier | ❌ None | ✅ Tradier | Minor: gainers/losers via FMP |
| Calendar | N/A | N/A | N/A | N/A | — |
| Social | ⚠️ FMP (30 min) | ⚠️ FMP (30 min) | ❌ None | ⚠️ FMP (30 min) | **Add Tradier for price/vol/1D%** |
| Fundamentals | ⚠️ FMP (5 min) | ⚠️ FMP (5 min) | ❌ None | ⚠️ FMP (5 min) | Optional Tradier for current price |
| Watchlist | ✅ Tradier | ✅ Tradier | ❌ None | ✅ Tradier | — |
| Portfolio | ⚠️ Finnhub (primary) | ❌ **Null from Finnhub** | ❌ None | ⚠️ Finnhub | **Replace with Tradier — volume missing** |
| Sectors/Themes | ✅ Tradier | ✅ Tradier | ❌ None | ✅ Tradier | History via yfinance (OK) |
| Screener | ✅ Tradier | ✅ Tradier | ✅ **Tradier** | ✅ Tradier | Best in app |
| Insider Activity | ✅ Tradier | ❌ Not displayed | ❌ None | — | — |
| Whale Watch | ✅ Tradier | ❌ Not displayed | ❌ None | ✅ Derived | — |
| Options Flow | ✅ Tradier | ✅ Tradier | ✅ **Tradier** | ✅ Tradier | Best in app |

---

## Prioritised Tradier Replacement Recommendations

### Priority 1 — HIGH (data quality issue, volume broken today)

**Portfolio page (`POST /api/portfolio/quotes`) — `main.py` ~L4392**

- **Problem**: Finnhub primary never returns `volume` (field is hardcoded `null`). Users see empty volume on all portfolio holdings.
- **Fix**: Move `data_service.tradier.get_quotes(stock_tickers)` to the top of the stock branch. On success, map `last → price`, `change_percentage → change_pct`, `volume → volume`, `average_volume → avg_volume`. Retain Finnhub as fallback (non-US names Tradier doesn't cover).
- **Benefit**: Real-time price, real volume, consistent with Home/Screener/Sectors.

### Priority 2 — MEDIUM (staleness issue, up to 30 min lag)

**Social screener price/volume (`GET /api/social/x-dashboard` + `GET /api/social/fundamental-screener`) — `social_screener_service.py` `_fetch_quote()`**

- **Problem**: FMP quote TTL = 30 minutes. Intraday price/volume on the Social page can be 30 minutes stale.
- **Fix**: Before calling `_fmp_get("quote", ...)`, attempt `data_service.tradier.get_quotes(tickers)` for US-listed tickers. Map `last → price`, `change_percentage → changesPercentage`, `volume → volume`. Fall back to FMP if Tradier returns nothing. Keep FMP for `stock-price-change` (5D/1M/YTD/1Y historical %) — Tradier has no multi-period % endpoint.
- **Benefit**: Near-real-time price on social panel without touching FMP quota.

### Priority 3 — LOW (acceptable lag for use-case)

**Fundamentals / stock-compare current price (`stock_compare_service.py`)**

- **Problem**: FMP `quote` TTL = 5 minutes (acceptable for fundamental analysis view but inconsistent with rest of app).
- **Fix**: Optional — add a Tradier single-ticker quote for the "current price" card at top of the compare view. Underlying historical charts and fundamental metrics remain FMP.
- **Benefit**: Consistent real-time price header; low urgency since the page is for deep analysis, not trading signals.

---

## Provider Capabilities Quick Reference

| Provider | Price | Change % | Volume | Bid/Ask | Historical % | Fundamental | Options |
|---|---|---|---|---|---|---|---|
| **Tradier** | ✅ Real-time | ✅ | ✅ | ✅ | ❌ (daily bars only) | ❌ | ✅ Full chains |
| **FMP** | ~5 min | ~5 min | ~5 min | ❌ | ✅ Multi-period | ✅ Full | ❌ |
| **Finnhub** | ✅ Real-time | ✅ | ❌ **Null** | ❌ | ❌ | Partial | ❌ |
| **Yahoo Finance** | ✅ Quasi | ✅ | ✅ | ❌ | EOD history | ❌ | ❌ |
| **CoinGecko** | ✅ Crypto | ✅ 24h | ✅ | ❌ | ❌ | ❌ | ❌ |
| **FRED** | Macro indices | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ |
| **Finviz** | ✅ Approx | ✅ | ✅ Rel vol | ❌ | ❌ | Partial | ❌ |
