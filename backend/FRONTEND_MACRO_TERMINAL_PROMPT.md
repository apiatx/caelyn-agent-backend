# Macro Terminal — Full Rebuild Prompt

## Context

The Macro Terminal page (`/app/macro-terminal`) currently has:
- A live `MacroMarketSnapshot` React component (reads from `GET /api/macro/dashboard`)
- A legacy iframe for 5 remaining tabs (RATES, INFLATION, GROWTH, LABOR, RISK) — **hardcoded, no source code, must be completely replaced**

The backend now provides **6 dedicated API endpoints** with live data. Your job is to rebuild the entire Macro Terminal as native React components — **no iframe at all**. Delete the iframe entirely.

---

## Design Reference

Replicate the Perplexity Finance Macro Terminal layout:
- Dark theme (zinc-900/950 backgrounds, consistent with existing app theme)
- **6 horizontal tabs** across the top: SNAPSHOT | RATES | INFLATION | GROWTH | LABOR | RISK
- Each tab loads data from its own API endpoint
- Compact data cards with colored signals (green = bullish, red = bearish, yellow = neutral)
- Mini sparkline charts using historical data arrays
- Auto-refresh every 2 minutes via react-query

---

## API Endpoints (all on the FastAPI backend, proxied via Express)

All endpoints return `{ response: {...}, structured: true, preset: "..." }`. The actual data is in `.response`.

### Tab 1: SNAPSHOT — `GET /api/macro/dashboard`

Response shape:
```json
{
  "last_updated": "2026-03-25T14:30:00Z",
  "market_snapshot": {
    "sp500": 5234.45,
    "sp500_change_pct": 0.82,
    "dow": 42123.00,
    "nasdaq": 16234.56,
    "russell_2000": 2045.67
  },
  "benchmark_etfs": [
    { "ticker": "SPY", "price": 523.44, "change_pct": 0.82, "week_52_high": 548.23, "pct_from_52w_high": -4.5 },
    { "ticker": "QQQ", "price": 441.20, "change_pct": 1.12, "week_52_high": 475.00, "pct_from_52w_high": -7.1 },
    { "ticker": "TLT", "price": 88.50, "change_pct": -0.32, "week_52_high": 102.00, "pct_from_52w_high": -13.2 },
    { "ticker": "GLD", "price": 198.00, "change_pct": 0.15, "week_52_high": 205.00, "pct_from_52w_high": -3.4 },
    { "ticker": "USO", "price": 72.30, "change_pct": -1.20, "week_52_high": 85.00, "pct_from_52w_high": -14.9 },
    { "ticker": "HYG", "price": 76.80, "change_pct": 0.05, "week_52_high": 79.50, "pct_from_52w_high": -3.4 }
  ],
  "vix": { "current": 16.23, "change": -0.70, "change_pct": -2.81, "signal": "low fear" },
  "fear_greed": { "score": 42.5, "rating": "Fear", "signal": "FEAR — Market sentiment is negative..." },
  "dollar": { "dxy": 104.25, "dxy_change_pct": 0.15 },
  "fed": { "funds_rate": 4.33, "funds_rate_range": "4.33-4.58", "commentary": "..." },
  "inflation": { "cpi_yoy": 2.5, "core_pce_yoy": 2.3, "trend": "declining", "commentary": "..." },
  "labor": { "nfp_last": 272000, "unemployment_rate": 3.9, "commentary": "..." },
  "gdp": { "quarterly_data": [...], "gdp_now_estimate": 2.1, "commentary": "..." },
  "rates_and_yields": { "us_10y": 4.23, "us_2y": 4.45, "spread_2s10s": -0.22, "commentary": "..." },
  "commodities": { "wti_oil": 78.45, "gold": 2134.56, "natural_gas": 2.98, "commentary": "..." },
  "scenarios": { "bull": [...], "bear": [...], "base": [...] }
}
```

**UI for SNAPSHOT tab:**
- **Ticker bar** (horizontal scroll): SPY, QQQ, TLT, GLD, USO, HYG — each card shows: ticker, price, change_pct (green/red), pct_from_52w_high
- **VIX card**: current value + signal badge + change
- **Fear & Greed gauge**: score (0-100) with color gradient, rating text
- **DXY card**: price + change_pct
- **Major Indices row**: S&P 500, Dow, Nasdaq, Russell 2000 (price + change)
- **Summary cards**: Fed Funds Rate, CPI YoY, Unemployment, GDP, 10Y Yield, Oil, Gold — each showing value + commentary
- **Scenarios section**: Bull/Bear/Base case bullets

---

### Tab 2: RATES — `GET /api/macro/rates`

Response shape:
```json
{
  "last_updated": "2026-03-25T14:30:00Z",
  "data_source": "FMP (real-time)",
  "yield_curve": [
    { "tenor": "1M", "yield_pct": 5.32 },
    { "tenor": "3M", "yield_pct": 5.28 },
    { "tenor": "6M", "yield_pct": 5.15 },
    { "tenor": "1Y", "yield_pct": 4.85 },
    { "tenor": "2Y", "yield_pct": 4.45 },
    { "tenor": "5Y", "yield_pct": 4.25 },
    { "tenor": "10Y", "yield_pct": 4.23 },
    { "tenor": "20Y", "yield_pct": 4.40 },
    { "tenor": "30Y", "yield_pct": 4.35 }
  ],
  "key_rates": { "us_2y": 4.45, "us_5y": 4.25, "us_10y": 4.23, "us_30y": 4.35 },
  "fed_policy": { "funds_rate": 4.33, "funds_rate_range": "4.33-4.58" },
  "spreads": {
    "spread_2s10s": -0.22,
    "spread_10y3m": -1.05,
    "curve_status": "inverted",
    "inversion_signal": true
  },
  "mortgage": { "rate_30y": 6.87 },
  "credit_spreads": { "hy_oas": 3.45, "bbb_oas": 1.62 },
  "history": {
    "us_10y": [{ "date": "2024-03-25", "value": 4.23 }, ...],
    "us_2y": [{ "date": "2024-03-25", "value": 4.45 }, ...],
    "spread_2s10s": [{ "date": "2024-03-25", "value": -0.22 }, ...]
  }
}
```

**UI for RATES tab:**
- **Yield Curve Chart** (LINE CHART, the hero visual): X-axis = tenors (1M to 30Y), Y-axis = yield_pct. Plot all 9 points. If curve inverts (short > long), highlight the inversion in red.
- **Key Rates grid** (2x2 cards): 2Y, 5Y, 10Y, 30Y — each with yield value
- **Fed Policy card**: Funds rate + range
- **Spreads section**: 2s10s spread (big number, red if negative with "INVERTED" badge), 10Y-3M spread, curve_status
- **Mortgage card**: 30Y rate
- **Credit Spreads**: HY OAS + BBB OAS
- **Historical charts** (small sparklines): 10Y yield (24 months), 2s10s spread (24 months)

---

### Tab 3: INFLATION — `GET /api/macro/inflation`

Response shape:
```json
{
  "last_updated": "2026-03-25T14:30:00Z",
  "headline": {
    "cpi_yoy": 2.5,
    "cpi_mom": 0.3,
    "core_cpi_yoy": 2.8,
    "ppi_yoy": 1.8
  },
  "fed_preferred": {
    "core_pce_yoy": 2.3,
    "target": 2.0,
    "target_status": "above_target"
  },
  "alternative_measures": {
    "trimmed_mean_pce": 2.6,
    "sticky_cpi": 4.1
  },
  "market_expectations": {
    "breakeven_5y": 2.25,
    "breakeven_10y": 2.30
  },
  "trend": "declining",
  "commentary": "CPI 2.5% YoY, Core PCE 2.3% (Fed target 2%). Inflation declining. 5Y breakeven: 2.25%.",
  "history": {
    "cpi": [{ "date": "2023-03-01", "value": 5.0 }, ...],
    "core_pce": [{ "date": "2023-03-01", "value": 4.6 }, ...],
    "breakeven_5y": [{ "date": "2023-03-01", "value": 2.4 }, ...]
  }
}
```

**UI for INFLATION tab:**
- **Headline Inflation row** (4 cards): CPI YoY, CPI MoM, Core CPI YoY, PPI YoY — each with value and trend arrow
- **Fed's Preferred Measure** (hero card): Core PCE YoY (big number), with a horizontal bar showing distance from 2% target. Badge: "AT TARGET" / "ABOVE TARGET" / "WELL ABOVE TARGET"
- **Alternative Measures row**: Trimmed Mean PCE (Dallas Fed), Sticky CPI (Atlanta Fed)
- **Market Expectations row**: 5Y Breakeven, 10Y Breakeven — these are forward-looking inflation expectations priced by the bond market
- **Trend badge**: "DECLINING" (green) / "STICKY" (yellow) / "ELEVATED" (red)
- **Commentary**: rendered as text below
- **Historical charts**: CPI (36 months), Core PCE (36 months), 5Y Breakeven (36 months) — overlaid or as 3 separate sparklines

---

### Tab 4: GROWTH — `GET /api/macro/growth`

Response shape:
```json
{
  "last_updated": "2026-03-25T14:30:00Z",
  "gdp": {
    "quarterly_data": [
      { "quarter": "Q1 2024", "gdp": 1.6 },
      { "quarter": "Q2 2024", "gdp": 2.8 },
      { "quarter": "Q3 2024", "gdp": 3.1 },
      { "quarter": "Q4 2024", "gdp": 2.5 },
      { "quarter": "Q1 2025", "gdp": 2.1 }
    ],
    "latest": 2.1,
    "recession_signal": false
  },
  "manufacturing": {
    "ism_manufacturing": 51.3,
    "signal": "expansion",
    "threshold": 50.0
  },
  "consumer": {
    "retail_sales_yoy": 3.2,
    "consumer_sentiment": 67.8
  },
  "production": {
    "industrial_production_yoy": 1.5
  },
  "liquidity": {
    "m2_current_trillion": 20.5,
    "m2_yoy_growth": -2.3,
    "m2_trend": "contracting"
  },
  "leading_indicators": {
    "leading_index": -0.3
  },
  "commentary": "GDP at 2.1% annualized. ISM Mfg above 50 (51.3).",
  "history": {
    "gdp": [{ "date": "2022-Q1", "value": 3.7 }, ...],
    "ism_manufacturing": [{ "date": "2023-03-01", "value": 49.2 }, ...],
    "consumer_sentiment": [{ "date": "2023-03-01", "value": 62.0 }, ...]
  }
}
```

**UI for GROWTH tab:**
- **GDP Bar Chart** (hero visual): Quarterly GDP bars. Green bars for positive, red for negative. If `recession_signal` is true, show "RECESSION SIGNAL" red badge.
- **ISM Manufacturing card**: Value with a gauge-style indicator. Green zone (>50 = expansion), Red zone (<50 = contraction). Show the 50 threshold line.
- **Consumer section** (2 cards): Retail Sales YoY, Consumer Sentiment (U of Michigan)
- **Industrial Production card**: YoY change
- **Liquidity card**: M2 (in trillions), M2 YoY growth, trend badge (expanding/contracting/stable)
- **Leading Index card**: LEI value (negative = recession leading indicator)
- **Historical charts**: GDP (bar chart, 48 months), ISM (line, 36 months), Consumer Sentiment (line, 36 months)

---

### Tab 5: LABOR — `GET /api/macro/labor`

Response shape:
```json
{
  "last_updated": "2026-03-25T14:30:00Z",
  "employment": {
    "nfp_mom_change": 272000,
    "nfp_3m_avg": 215000,
    "unemployment_rate": 3.9,
    "u6_rate": 7.2,
    "participation_rate": 62.5
  },
  "claims": {
    "initial_claims": 215000,
    "continued_claims": 1850000
  },
  "wages": {
    "avg_hourly_earnings_yoy": 4.1
  },
  "job_openings": {
    "jolts_millions": 8.2
  },
  "labor_market_status": "tight",
  "commentary": "Unemployment at 3.9%, U-6 at 7.2%. NFP added 272000 jobs last month (3-mo avg: 215000). Labor market tight.",
  "history": {
    "unemployment": [{ "date": "2023-03-01", "value": 3.6 }, ...],
    "nfp": [{ "date": "2023-03-01", "value": 152500 }, ...],
    "wages": [{ "date": "2023-03-01", "value": 4.3 }, ...],
    "jobless_claims": [{ "date": "2024-03-25", "value": 218000 }, ...]
  }
}
```

**UI for LABOR tab:**
- **NFP hero card**: Big number for last month's change (+272K), smaller text for 3-month average. Color: green if positive, red if negative.
- **Employment grid** (3 cards): Unemployment Rate (U-3), U-6 Rate, Participation Rate
- **Claims section** (2 cards): Initial Claims (weekly), Continued Claims
- **Wages card**: Average Hourly Earnings YoY
- **JOLTS card**: Job openings in millions
- **Labor Market Status badge**: "TIGHT" (green) / "SOFTENING" (yellow) / "WEAK" (red)
- **Historical charts**: Unemployment (36 months), NFP (36 months, bar chart with monthly changes implied), Wages (36 months), Jobless Claims (24 months)

---

### Tab 6: RISK — `GET /api/macro/risk`

Response shape:
```json
{
  "last_updated": "2026-03-25T14:30:00Z",
  "volatility": {
    "vix": 16.23,
    "vix_change": -0.70,
    "signal": "normal",
    "interpretation": "Normal volatility environment"
  },
  "credit_spreads": {
    "hy_oas": 3.45,
    "bbb_oas": 1.62,
    "hy_signal": "normal"
  },
  "fear_greed": {
    "score": 42.5,
    "rating": "Fear",
    "signal": "FEAR — Market sentiment is negative...",
    "previous_close": 45.0,
    "one_week_ago": 38.0,
    "components": {
      "market_momentum_sp500": { "score": 45, "rating": "Neutral" },
      "stock_price_strength": { "score": 35, "rating": "Fear" },
      "stock_price_breadth": { "score": 40, "rating": "Fear" },
      "put_call_options": { "score": 55, "rating": "Neutral" },
      "market_volatility_vix": { "score": 50, "rating": "Neutral" },
      "safe_haven_demand": { "score": 30, "rating": "Fear" },
      "junk_bond_demand": { "score": 42, "rating": "Fear" }
    },
    "momentum_shift": "Sentiment relatively stable"
  },
  "dollar": { "dxy": 104.25, "dxy_change_pct": 0.15 },
  "yield_curve_risk": {
    "spread_2s10s": -0.22,
    "inverted": true
  },
  "commentary": "VIX at 16.23 (normal). HY OAS: 3.45bps. Fear & Greed: 42.5 (Fear). DXY: 104.25.",
  "history": {
    "vix": [{ "date": "2025-03-25", "value": 18.5 }, ...],
    "hy_spread": [{ "date": "2024-03-25", "value": 3.2 }, ...]
  }
}
```

**UI for RISK tab:**
- **VIX hero card**: Big number, change (green if down, red if up), signal badge (color-coded: green=low_vol, yellow=normal, orange=elevated, red=high_fear, blue=complacency)
- **Fear & Greed gauge** (the centerpiece): Semi-circular gauge 0-100. Color gradient: red(0) -> orange(25) -> yellow(50) -> green(75) -> deep green(100). Show score, rating, and the signal text. Below: 7 component bars (horizontal bars 0-100 each, labeled).
- **Credit Spreads section** (2 cards): HY OAS, BBB OAS. Signal badge for HY (normal/elevated/stress).
- **Dollar card**: DXY + change_pct
- **Yield Curve Risk card**: 2s10s spread, "INVERTED" badge if negative
- **Momentum Shift**: text from fear_greed.momentum_shift
- **Historical charts**: VIX (12 months), HY Spread (24 months)

---

## Technical Requirements

1. **Delete the iframe entirely.** Remove `/macro-terminal/index.html` and all references to it.
2. **File structure:**
   - `client/src/pages/macro-terminal.tsx` — Page with tab navigation
   - `client/src/components/macro/macro-snapshot-tab.tsx`
   - `client/src/components/macro/macro-rates-tab.tsx`
   - `client/src/components/macro/macro-inflation-tab.tsx`
   - `client/src/components/macro/macro-growth-tab.tsx`
   - `client/src/components/macro/macro-labor-tab.tsx`
   - `client/src/components/macro/macro-risk-tab.tsx`
   - `client/src/components/macro/macro-chart.tsx` — Reusable sparkline/chart component
   - `client/src/components/macro/macro-card.tsx` — Reusable data card component
   - `client/src/hooks/use-macro-data.ts` — react-query hooks for all 6 endpoints
3. **Data fetching:** Use react-query (`@tanstack/react-query`). Each tab fetches its own endpoint. Stale time: 2 minutes. Refetch interval: 2 minutes.
4. **Charts:** Use recharts (already in project) for:
   - Yield curve (line chart)
   - GDP (bar chart)
   - Sparklines (area chart, minimal, used in cards for history arrays)
   - Fear & Greed gauge (custom SVG or use a radial chart)
5. **Colors & signals:**
   - `bullish` / positive change: `text-emerald-400`
   - `bearish` / negative change: `text-red-400`
   - `neutral`: `text-yellow-400`
   - Signal badges: rounded pill with bg-opacity
6. **Responsive:** Mobile-first. Cards stack vertically on small screens, grid on desktop.
7. **Loading states:** Skeleton cards while data loads.
8. **Error states:** "Failed to load [tab] data. Retrying..." with retry button.
9. **The existing `MacroMarketSnapshot` component** (`client/src/components/macro-market-snapshot.tsx`) can be deleted — its functionality is now part of the SNAPSHOT tab.

---

## Express Proxy Routes Needed

The backend endpoints are on the FastAPI server (port 8000). Add these proxy routes in `server/routes.ts`:

```typescript
// Macro Terminal tab endpoints
app.get("/api/macro/rates", proxyToFastAPI);
app.get("/api/macro/inflation", proxyToFastAPI);
app.get("/api/macro/growth", proxyToFastAPI);
app.get("/api/macro/labor", proxyToFastAPI);
app.get("/api/macro/risk", proxyToFastAPI);
```

The `/api/macro/dashboard` route already exists — update it to also proxy to FastAPI instead of using the Express-local Yahoo Finance implementation, since the FastAPI version is richer (includes Fear & Greed, DXY, scenarios, etc.).

---

## react-query Hook Example

```typescript
// client/src/hooks/use-macro-data.ts
import { useQuery } from "@tanstack/react-query";

const MACRO_STALE_TIME = 2 * 60 * 1000; // 2 minutes

function useMacroTab<T>(tab: string) {
  return useQuery<T>({
    queryKey: ["macro", tab],
    queryFn: async () => {
      const res = await fetch(`/api/macro/${tab}`);
      if (!res.ok) throw new Error(`Failed to fetch ${tab}`);
      const json = await res.json();
      return json.response; // unwrap the { response, structured, preset } wrapper
    },
    staleTime: MACRO_STALE_TIME,
    refetchInterval: MACRO_STALE_TIME,
  });
}

export const useMacroDashboard = () => useMacroTab<DashboardData>("dashboard");
export const useMacroRates = () => useMacroTab<RatesData>("rates");
export const useMacroInflation = () => useMacroTab<InflationData>("inflation");
export const useMacroGrowth = () => useMacroTab<GrowthData>("growth");
export const useMacroLabor = () => useMacroTab<LaborData>("labor");
export const useMacroRisk = () => useMacroTab<RiskData>("risk");
```

---

## Summary Checklist

- [ ] Delete iframe and its HTML/JS bundle entirely
- [ ] Delete `macro-market-snapshot.tsx` (replaced by Snapshot tab)
- [ ] Create 6 tab components + shared chart/card components
- [ ] Create react-query hooks for all 6 endpoints
- [ ] Add Express proxy routes for the 5 new endpoints
- [ ] Update `/api/macro/dashboard` to proxy to FastAPI
- [ ] Tab navigation with URL state (optional: `/app/macro-terminal?tab=rates`)
- [ ] All data variables rendered — nothing hardcoded
- [ ] Auto-refresh every 2 minutes
- [ ] Dark theme matching rest of app
- [ ] Loading skeletons + error states
- [ ] Responsive grid layout
