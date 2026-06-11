---
name: Screener Hub theme selection
description: How theme=None resolves to daily default; per-theme 24h refresh cap; new response fields
---

## Rule
`GET /api/screener-hub?tab=thematic` with no `theme` param must use `_get_daily_default_theme()` — never flatten all themes.

**Why:** Flattening all 60 themes produces 200+ unrelated symbols and is unusable as a screener view. The daily default is the highest-RS theme with ≥15 snapshot rows and ≥20% small/mid-cap metadata coverage.

**How to apply:**
- `theme=None` → call `_get_daily_default_theme()` → set `theme=_def_theme`, `is_default_theme=True`.
- `_DAILY_DEFAULT` dict caches result keyed by UTC date; recomputes on date rollover.
- Invalid theme → return `error_code: UNKNOWN_THEME` immediately (validated against THEME_RS_UNIVERSE).
- Per-theme 24h refresh cap: stale snapshot (>24h) → `asyncio.create_task(_background_refresh_theme(theme))` once per 24h. Uses fast path (no FMP screener/peers). Sets `theme_refresh_status: refresh_scheduled|stale_cap_active|fresh`.
- Response always includes: `selected_theme`, `is_default_theme`, `default_theme_reason`, `theme_refresh_status`, `theme_last_refreshed_at`, `theme_next_refresh_allowed_at`, `filter_policy`.
- Social/bottlenecks/watchlist tabs: `theme_refresh_status: not_applicable`, `selected_theme: null`.
