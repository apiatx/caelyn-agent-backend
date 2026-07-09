---
name: Confluence V2 field-name collision when adding shadow archetypes
description: Adding a new scoring archetype's fields into the same per-symbol dict as an existing legacy archetype can silently clobber legacy fields if names match.
---

`backend/services/confluence_v2_service.py` returns one flat per-symbol dict merging multiple archetypes. The legacy "trade_confluence" flow already owns top-level keys `social_bonus_score`, `social_bonus_eligible`, `social_bonus_reason`, `social_risk_flag`.

When a new archetype (e.g. THEME_ALIGNMENT / Trade Alignment) is spec'd to expose fields with the *same* names, spreading the new dict before vs. after the legacy assignments changes which value wins — a silent, hard-to-notice bug since both look valid.

**Why:** dict literals in Python resolve key collisions by last-write-wins; there's no error, so a legacy-value overwrite of a new archetype's fields (or vice versa) won't surface as an exception, only as wrong-looking output.

**How to apply:** when a spec says "add field X" but X already exists for a different meaning, rename the *legacy* copy to `legacy_<field>` (since spec authors usually intend new fields to own the canonical name going forward for spec-covered work), spread the new dict where the canonical name should end up, and never re-assign the same literal key again later in the same dict literal.

## Related: options/combined-ticker-data cache is in-memory only

`data.options_theme_supplement.get_combined_ticker_data()` (used by `services/options_alignment.py`) reads an in-process, in-memory cache populated by the running server's background scan loops.

**Why:** a fresh `python3.11 -c "..."` subprocess (or any standalone script) starts with an empty in-memory cache, so options-derived signals will show 0% coverage there even though the feature works correctly.

**How to apply:** validate any code that depends on `get_combined_ticker_data()` (or similarly in-memory-only caches) by hitting the live server's HTTP endpoint, not a subprocess script — the subprocess result will look like a bug but isn't.
