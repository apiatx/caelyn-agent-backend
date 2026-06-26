---
name: Watchlist Stage2 LKG guards
description: Status-aware freshness TTLs, bulk null-overwrite guard, and force-recovery path for watchlist_stage2_service
---

## Root cause of blank Stage tabs
When `_fetch_tradier_daily_history` is temporarily broken (e.g. during a Tradier pacing change transition), `warmup_stage2` writes `{score:None, label:None}` for ALL tickers and the old 20h freshness gate treated those as "fresh", blocking re-computation for 20h. The scheduler at 3:30 AM ET also skipped them if they were <20h old. Result: all stage tabs blank for up to 24h.

## Status field (added)
Every LKG entry now carries `"status"`:
- `"ok"` — stage computed from valid bars
- `"no_bars"` — provider returned no usable bars (foreign/OTC symbol)
- `"fetch_failed"` — provider call failed, timed out, or returned unexpectedly empty

Legacy entries (no status field) are treated as `fetch_failed`.

## Freshness TTLs
| status | TTL |
|--------|-----|
| ok | 20h |
| no_bars | 20h |
| fetch_failed | 2h |
| legacy (none) | 2h |

`_is_fresh()` reads `_ttl_hours_for_entry(entry)` to get the right TTL per symbol.

## Bulk overwrite guard
Before persisting after a bulk warmup run, checks:
- `new_total >= 50` and `new_valid_coverage < 20%` and `prev_valid_coverage >= 20%`
- If true → degraded mode: only write new valid labels; existing valid labels preserved with `status=fetch_failed` so they retry on 2h TTL

## Force recovery endpoint
`force_warmup_stage2_nulls()` — bypasses freshness gate for all entries where `label is None or score is None`. Leaves valid-label entries untouched.
- HTTP: `POST /api/admin/stage2/force-warmup` (fires as background asyncio task, returns immediately)
- Poll: `GET /api/admin/stage2/status`

**Why:** A transient provider failure at startup can silently nuke all stage data. Both the 2h retry TTL and the overwrite guard prevent recurrence. The force-warmup allows immediate manual recovery without a server restart.

**How to apply:** If Stage tabs are blank after a restart, check `/api/admin/stage2/status`. If coverage_pct=0 and status_counts shows all "legacy" or "fetch_failed", POST to force-warmup and poll status for progress.
