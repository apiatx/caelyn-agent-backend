---
name: Kalshi second prediction-market provider
description: How Kalshi is wired into odds_scanner.py alongside Polymarket; which families it owns; public API notes
---

## Integration pattern

`kalshi_scanner.scan_kalshi()` runs concurrently with the Polymarket catalog crawl via `asyncio.create_task()` launched right after `crawl_started_at`. Result is awaited after step 2.

Step 5b (after the Polymarket family-matching loop) iterates `_KALSHI_PRIMARY_FAMILIES` and injects rows only if Polymarket didn't match that family first. This means Kalshi is effectively a fallback-that-wins for its families.

## Kalshi-primary families
- `spx_daily_direction` → KXINXDUD series
- `nasdaq_daily_direction` → KXNASDAQDUD series  
- `spx_dec31_milestone` → KXINXDIRY-26DEC31H1600 ladder (highest-strike above-market)

## Public API — no auth for reads
Base URL: `https://external-api.kalshi.com/trade-api/v2`
`GET /events?series_ticker=KXINXDUD&status=open` → paginated events list
`GET /events/{event_ticker}` → event detail with markets

Auth (API key + RSA signature) is only needed for trading. Reading is public.
`kalshi_auth_error_type: credentials_present_unverified` is normal and expected.

## Row shape
`_make_kalshi_live_entry()` produces the same shape as the Polymarket matching loop.
Key differences:
- `provider: "kalshi"` 
- `clob_token_ids: []` → CLOB gather step exits immediately (no Kalshi CLOB)
- `_kalshi_market_ticker` / `_kalshi_event_ticker` / `_kalshi_series_ticker` fields
- `_snap_row.source: "kalshi"` persisted to 7-day history DB

## New families (no Kalshi, no reliable Polymarket match yet)
- `wti_daily_direction` (id=30) — Polymarket has WTI markets sporadically
- `gold_daily_direction` (id=31) — Polymarket has gold markets sporadically
- `nvda_daily_direction` (id=32) — Polymarket has NVDA markets sporadically
- These show in `missing_families` which is expected until Polymarket publishes them

**Why:** Kalshi has higher-quality, higher-liquidity contracts for index dailies vs Polymarket which rarely lists SPX/Nasdaq direction. Kalshi public API is stable and doesn't require auth for reads.
