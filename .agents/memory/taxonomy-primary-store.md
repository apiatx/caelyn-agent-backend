---
name: Taxonomy primary-store semantics
description: How canonical primary theme is set vs. membership add; seed override restart risk; resolver ordering bug for multi-theme tickers.
---

## The rule

A `theme_ticker_overrides` row with `action='add'` adds a **membership** only.
It does NOT set the canonical primary theme displayed on the watchlist.

To set the canonical primary, `atomic_taxonomy_write_db` must be called with:
```python
primary_operation = {
    "action": "set",
    "user_id": "default",
    "ticker": TICKER,
    "category": DISPLAY_NAME,   # must match active theme's display_name
    "source": SOURCE,
    "reason": REASON,
}
```
This writes to `watchlist_category_overrides` which is Step 4 (always wins) in the resolver.

**Why:** The migration script named the field `new_primary_theme_id` but for many rows
it only passed `primary_operation=None`, so only the membership was added.
The resolver's Step 3 (ENRICHED iteration order) then determined the displayed primary,
and iteration order does not respect migration intent.

## _SEED_OVERRIDES restart risk

`category_overrides._SEED_OVERRIDES` runs `bulk_upsert` on every server restart.
Any DB `watchlist_category_overrides` row for a seeded ticker will be **overwritten**
by the seed value on the next restart.

Consequence: fixing a DB cat_override for a seeded ticker is not enough —
the seed entry must also be updated in source, or the fix is reverted on restart.

Affected tickers historically: ASPI ("Uranium & Nuclear Energy" → "Uranium Mining & Nuclear Fuel"),
IMSR ("Uranium & Nuclear Energy" → "SMRs & Advanced Reactors").

## Resolver ordering bug for multi-theme tickers

`build_theme_resolution_context()` builds `themes_page_map` by iterating
`ENRICHED_THEME_RS_UNIVERSE` and assigning first-seen wins per ticker.
For tickers in multiple themes, iteration dict order (not semantic priority) wins.

**Affected pattern:** ticker in `nuclear_energy` (early in dict) + `smr_advanced_reactors`
(later) → resolver returns nuclear_energy even when smr_advanced_reactors is intended primary.

**Fix:** Always set an explicit `watchlist_category_overrides` row (primary_operation="set")
for any ticker whose intended primary differs from the canonical_map or first-iteration
themes_page result. Do NOT rely on membership add alone.
