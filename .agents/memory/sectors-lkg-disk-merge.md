---
name: Sectors supplement LKG disk merge strategy
description: _save_supplement_lkg_to_disk must merge with existing disk content; overwrite-only caused LKG to shrink on each restart before a full pass completed.
---

## The rule
`_save_supplement_lkg_to_disk(ticker_data)` now reads the existing disk file before writing, merges old entries as the base, and lets fresh supplement rows override matching old entries. The merged result is written atomically.

**Why:** The supplement LKG disk is saved after each batch (8 symbols). If the server is restarted before a full 29-minute pass, only the current session's batches are in memory. Without merging, the disk LKG gets overwritten with just 8 symbols, erasing all prior-session coverage. With merging, each save accumulates coverage — restarts start with all previously-scanned symbols available as stale_lkg.

**How to apply:** If you ever modify `_save_supplement_lkg_to_disk`, preserve the read-then-merge pattern. The age guard (`_SUPPLEMENT_LKG_DISK_MAX_AGE`) prunes very old disk entries naturally.

## Related
The `_SUPPLEMENT_LKG_DISK_MAX_AGE` is 24h. Old entries are pruned on any save that happens more than 24h after the disk file was written. Do not reduce this TTL — it's the right balance between freshness and coverage persistence.
