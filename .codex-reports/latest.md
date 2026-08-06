# Task: fix: make ticker taxonomy updates truly atomic

## 1. AGENTS.md confirmation
Read `AGENTS.md` completely before any edits. Report path: `.codex-reports/latest.md` (Codex CLI).

## 2. Starting HEAD and status
- Starting HEAD: `f996d837` (Published your App — Replit auto-commit)
- Previous task commit: `22545317` (fix: harden taxonomy assignments and classification inputs)
- Branch: `main`
- git status at start: clean except unrelated runtime data files (LKG, canonical history caches)

## 3. Proof old route did NOT use atomic_taxonomy_write_db
The old `admin_put_ticker_taxonomy()` body contained **no import of `atomic_taxonomy_write_db`**. Instead it called:
- `_perform_theme_membership_only_write(...)` in a loop for each removal
- `_perform_membership_write(...)` for the primary add (only when `primary_theme_id in to_add`)
- `_perform_theme_membership_only_write(...)` for each additional add

`atomic_taxonomy_write_db` was added in commit `22545317` but was never wired into the route.

## 4. Old repeated-invalidation count
Every call to `_perform_membership_write` or `_perform_theme_membership_only_write` called `_invalidate_caches()` internally. For a request with N removes + 1 primary add + M additional adds, `_invalidate_caches()` was called **(N + 1 + M + 1)** times — once per helper invocation plus once explicitly at the end.

## 5. Old partial-success path
Additional membership failures were caught and appended to `errors_list`; the route continued without raising. The final response returned `"ok": len(errors_list) == 0` with a non-empty `errors` list, leaving the database in partially-mutated state with some additions missing.

## 6. Old promote-existing-addition defect
The critical guard:
```python
if body.primary_theme_id and body.primary_theme_id in to_add:
    _perform_membership_write(...)
```
Only called the primary write when the theme was a **net new** addition (`in to_add`). When promoting an existing additional membership (already in `current_all`, so NOT in `to_add`), the `watchlist_category_overrides` update was **silently skipped** — the category store was never updated, so the resolver continued returning the old primary.

## 7. Old clear-primary defect
`primary_theme_id = None` → the route skipped the primary write entirely. `watchlist_category_overrides` was **never deleted**, so `resolve_primary_theme_for_ticker` continued returning the stale old primary (category_overrides resolver takes highest precedence). Additionally, the old code used `current_memberships.get("theme_ids", [])` but `_get_ticker_theme_memberships()` does not return a `"theme_ids"` key — it returns `"theme_memberships"`. So `current_all` was always `set()`, meaning nothing was ever detected as needing removal.

## 8. Transaction primitive changes (pg_storage.py)

### Added `primary_operation` parameter
```python
def atomic_taxonomy_write_db(
    ticker_overrides: list[dict],
    primary_operation: dict | None = None,   # NEW
    category_override: dict | None = None,   # legacy alias preserved
) -> dict:
```

### action="set" (unchanged behavior for existing callers)
Issues an upsert INSERT ON CONFLICT DO UPDATE into `watchlist_category_overrides`.

### action="clear" (NEW)
Issues a DELETE from `watchlist_category_overrides` for `(user_id, ticker)` in the same transaction before COMMIT.

### Legacy alias
`category_override` is still accepted; internally converted to `{"action": "set", ...}` when `primary_operation` is None. Backward compat preserved for any existing callers.

### Transaction flow (unchanged guarantee)
```
BEGIN (implicit — psycopg2 autobegin)
  for each ticker_override: INSERT ... ON CONFLICT DO UPDATE
  if primary_operation:
    SET → INSERT ON CONFLICT DO UPDATE into watchlist_category_overrides
    CLEAR → DELETE FROM watchlist_category_overrides WHERE user_id=? AND ticker=?
COMMIT
— or on any exception —
ROLLBACK
_put_conn() always called in finally
```

## 9. Exact route rewrite (routes/themes.py)

The entire body of `admin_put_ticker_taxonomy()` was replaced. Key structural changes:

**Removed:**
- `undo_stack` and all compensating-write loops
- `errors_list` and partial-success behavior
- All calls to `_perform_membership_write`
- All calls to `_perform_theme_membership_only_write`
- The buggy `body.primary_theme_id in to_add` guard
- The buggy `current_memberships.get("theme_ids", [])` (wrong key)

**Added:**
- `import logging; _log = logging.getLogger(__name__)` at module level
- Step 5: reads current state correctly: `{m["theme_id"] for m in current_memberships["theme_memberships"]}`
- Step 5: reads current primary correctly: `current_memberships["primary_theme"]["theme_id"]`
- Step 7: builds `membership_edits` (ALL desired memberships upserted, all removes included) + `primary_op`
- Step 8: single `atomic_taxonomy_write_db(ticker_overrides=membership_edits, primary_operation=primary_op)` call
- Step 9: `if not txn_result["ok"]: raise HTTPException(500, ...)` immediately
- Step 10: single `_invalidate_caches()` after commit
- Step 11: non-fatal post-commit hints (options priority + LLM mapper) using `_log.warning` on failure
- Step 12-14: authoritative reread via `_get_ticker_theme_memberships()`, validation, response

## 10. Primary-set and primary-clear behavior

**Set:** When `body.primary_theme_id` is non-null, the transaction includes:
```python
primary_op = {
    "action": "set", "user_id": "default", "ticker": ticker,
    "category": display_name, "source": "themes_page_manual",
    "reason": f"themes_page:{body.primary_theme_id}",
}
```
Fires regardless of whether the membership row already exists (fixes promote defect).

**Clear:** When `body.primary_theme_id` is null and `current_primary` is not None:
```python
primary_op = {"action": "clear", "user_id": "default", "ticker": ticker}
```
Issues a DELETE in the same transaction (fixes clear defect).

**No-op:** When `body.primary_theme_id` is null and `current_primary` is already None: `primary_op = None` (no statement issued).

## 11. Proof per-membership helpers absent from route

`_perform_membership_write` and `_perform_theme_membership_only_write` are only called from:
- `admin_upsert_membership` (POST /admin/memberships)
- `admin_assign_primary_theme` (POST /admin/assign-primary-theme)
- `admin_additional_membership` (POST /admin/additional-memberships)
- `admin_bulk_memberships` (POST /admin/memberships/bulk)

They are NOT imported or called from `admin_put_ticker_taxonomy`. Verified by `TestAtomicTaxonomyRoute.test_route_never_calls_per_membership_helpers`.

## 12. Commit/rollback failure-injection evidence (TestAtomicTaxonomyPrimitive)

| Test | Inject point | commit | rollback | ok |
|------|--------------|--------|----------|----|
| test_successful_all_memberships_commit_once | none | 1 | 0 | True |
| test_failure_first_membership_rolls_back | stmt 0 | 0 | 1 | False |
| test_failure_middle_membership_rolls_back_prior | stmt 1 of 3 | 0 | 1 | False |
| test_failure_primary_set_rolls_back_memberships | primary-set | 0 | 1 | False |
| test_failure_primary_clear_rolls_back_memberships | primary-clear | 0 | 1 | False |
| test_no_commit_after_any_failure | each of 3 stmts | 0 | 1 | False |
| test_rollback_exactly_once | stmt 0 | 0 | 1 | False |

## 13. Cache invalidation evidence (TestAtomicTaxonomyRoute)

- `test_cache_invalidated_exactly_once_after_success`: `invalidate_calls == [1]` (exactly one)
- `test_cache_not_invalidated_after_failure`: `invalidate_calls == []` (zero)

## 14. Authoritative reread evidence (TestAtomicTaxonomyRoute)

- `test_response_primary_from_reread_not_body`: response `primary_theme_id` matches the value from the second `_get_ticker_theme_memberships` call, not `body.primary_theme_id`
- `test_reread_mismatch_raises_server_error`: when mock reread returns a different primary, route raises HTTP 500

## 15. Tests and exit codes

```
cd /home/runner/workspace/backend
python3.11 -m pytest tests/test_theme_hierarchy.py::TestAtomicTaxonomyPrimitive -q
→ 10 passed  exit 0

python3.11 -m pytest tests/test_theme_hierarchy.py::TestAtomicTaxonomyRoute -q
→ 20 passed  exit 0

python3.11 -m pytest tests/test_theme_hierarchy.py -q
→ 271 passed  exit 0

git diff --check
→ (no output)  exit 0
```

## 16. Exact files changed and staged

| File | Change |
|------|--------|
| `backend/data/pg_storage.py` | Extended `atomic_taxonomy_write_db()` with `primary_operation` param (set+clear); kept `category_override` legacy alias |
| `backend/routes/themes.py` | Added `import logging; _log = ...`; rewrote `admin_put_ticker_taxonomy()` to call `atomic_taxonomy_write_db()` exactly once |
| `backend/tests/test_theme_hierarchy.py` | Added `TestAtomicTaxonomyPrimitive` (10 tests) and `TestAtomicTaxonomyRoute` (20 tests) |

## 17. Confirmation — no taxonomy/classifier/frontend changes

- Taxonomy v2 hierarchy: NOT modified
- Registry metadata/rollups: NOT modified  
- AI classifier / provider selection: NOT modified
- Frontend files: NOT modified
- No new tables created
- No new dependencies installed
- No AI model calls made

## 18. Final status

All 271 tests pass. `git diff --check` clean. Commit created successfully.

Push to `origin/main` failed: GitHub HTTPS authentication is not configured in this Replit environment. The commit `2b0bddd4` is present at `HEAD` and local `main` but could not be pushed to `origin/main`.

## 19. Commit SHA and message

```
2b0bddd43938f51ef1b956f23624657e7442c3cc
fix: make ticker taxonomy updates truly atomic
```

## 20. Complete correction-commit diff

```
 backend/data/pg_storage.py            |  83 ++--
 backend/routes/themes.py              | 250 +++++++-----
 backend/tests/test_theme_hierarchy.py | 747 ++++++++++++++++++++++++++++++++++
 3 files changed, 948 insertions(+), 132 deletions(-)
```

### pg_storage.py: atomic_taxonomy_write_db changes
- Signature: added `primary_operation: dict | None = None` before existing `category_override`
- New "set" path: existing upsert logic (was category_override, now routed through _eff_primary_op)
- New "clear" path: `DELETE FROM watchlist_category_overrides WHERE user_id = %s AND ticker = %s`
- Legacy alias: when `primary_operation is None and category_override is not None`, category_override is treated as `{"action": "set", ...}`
- Both paths inside the same `with conn.cursor() as cur:` block before the single `conn.commit()`

### routes/themes.py: admin_put_ticker_taxonomy rewrite
- Removed: `undo_stack`, `errors_list`, per-membership loops, `_perform_membership_write`, `_perform_theme_membership_only_write`, buggy `body.primary_theme_id in to_add` guard, buggy `current_memberships.get("theme_ids",[])` read
- Added: correct current-state read (`{m["theme_id"] for m in current_memberships["theme_memberships"]}`), `primary_op` with explicit `action="set"|"clear"`, single `atomic_taxonomy_write_db()` call, immediate raise on failure, single `_invalidate_caches()` after success, non-fatal post-commit hints, authoritative reread + mismatch guard

### tests/test_theme_hierarchy.py: new test classes
- `TestAtomicTaxonomyPrimitive`: 10 tests verifying commit/rollback behavior via mocked psycopg2 connections
- `TestAtomicTaxonomyRoute`: 20 tests verifying route behavior via monkeypatched storage/cache dependencies
