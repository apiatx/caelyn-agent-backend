# CaelynAI Backend — Final Surgical DB Pool Safety Correction

## Task Requested

Correct two remaining destructive paths in `pg_storage._get_conn()` identified
after the initial pool exhaustion fix (`f3347ba6`):
1. Health-check failure on one stale connection called `_destroy_pool()` →
   `closeall()`, destroying healthy in-flight connections owned by other callers.
2. `_is_pool_exhausted_error()` was too broad — matched any exception containing
   "exhausted" regardless of type, misclassifying unrelated errors.

## Completion Status

**FIXED, TESTED, PUSHED.** Commit `f80b2211` at HEAD, local main, origin/main,
and origin/HEAD.

## Proven Root Cause

### Fix 1 — Health check path

The health-check failure handler (lines 197-206 before correction):

```python
except Exception as e:
    _pool.putconn(conn, close=True)   # discards one stale conn
    _destroy_pool()                    # ← also closes ALL other conns
    continue
```

One stale idle connection from the pool would trigger `pool.closeall()`,
closing every connection in `self._pool + list(self._used.values())` —
including connections currently checked out by unrelated callers. This
could recreate the same cascade as the original exhaustion bug.

### Fix 2 — Exhaustion detection

```python
def _is_pool_exhausted_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "exhausted" in msg or "pool exhausted" in msg
```

Would match `RuntimeError("worker budget exhausted")` as pool exhaustion,
treating an unrelated error as capacity exhaustion and returning None
without proper pool-level cleanup.

## Exact Behavior Changed

### Health check before/after

| Aspect | Before | After |
|--------|--------|-------|
| One stale conn | `putconn(close=True)` + `_destroy_pool()` | `putconn(close=True)` only |
| closeall() called? | YES | NO |
| Pool survives? | NO (set to None) | YES |
| Retry source | New pool created | Same surviving pool |
| Other callers' conns | Closed (broken) | Unaffected |

### Exhaustion detection before/after

| Input | Before | After |
|-------|--------|-------|
| `PoolError("connection pool exhausted")` | exhaustion | exhaustion |
| `PoolError("pool is closed")` | exhaustion | NOT exhaustion |
| `RuntimeError("worker budget exhausted")` | exhaustion | NOT exhaustion |
| `OperationalError("timeout")` | NOT | NOT |
| `Exception("connection pool exhausted")` | exhaustion | NOT exhaustion |

## Confirmation — No pool-size change

```
minconn=1, maxconn=5  — unchanged
```

## Confirmation — Instrumentation stores no connection objects

Tracked entries contain only `{"acquired_s": float, "caller": str}` keyed
by `id(conn)`. The connection object itself is never stored in `_conn_checkouts`.
Test `test_instrumentation_stores_no_connection_objects` verifies this.

## Files Changed

1. **`backend/data/pg_storage.py`** (+30/-3)
   - `_get_conn()` docstring updated: "discard ONLY that one connection"
   - Health-check failure: removed `_destroy_pool()` call, kept
     `putconn(conn, close=True)`, added comment explaining why
   - `_is_pool_exhausted_error()`: now requires `isinstance(exc, PoolError)`
     AND message contains "connection pool exhausted"

2. **`backend/tests/test_pg_pool_exhaustion.py`** (+206/-41)
   - Extended from 11 to 19 tests
   - New: `TestHealthCheckFailurePreservesPool` (3 tests)
     - `test_stale_conn_does_not_call_closeall`
     - `test_stale_then_healthy_retry_same_pool`
     - `test_stale_conn_pool_survives_for_next_caller`
   - New: `test_instrumentation_stores_no_connection_objects`
   - New: `test_non_poolerror_with_exhausted_word_not_exhaustion`
   - New: `test_poolerror_wrong_message_not_exhaustion`
   - Updated: `TestIsPoolExhaustedError` — now expects narrow semantics
     (PoolError type required, keyword-only match rejected)

## Tests

```
19 passed in 0.82s (test_pg_pool_exhaustion.py)
20 passed in 0.56s (test_bulk_detail_cache_isolation.py)
67 passed in 0.53s (test_earnings_isolation.py)
```

## Build

```
bash scripts/run_build.sh
[BUILD] Done — no compile errors.
```

## Prepush

```
python3.11 scripts/workspace_guard.py prepush
Build: OK
PREPUSH OK.
```

## Commit SHA

```
f80b2211 fix: narrow pool exhaustion detection, stop health-check from destroying pool
```

## Push Result

```
GIT_ASKPASS=replit-git-askpass git push origin main
56d4b821..f80b2211  main -> main
Pre-push hook: build OK, PREPUSH OK.
```

## Final HEAD / Origin

```
HEAD:        f80b2211
local main:  f80b2211
origin/main: f80b2211
origin/HEAD: f80b2211
f3347ba6 is ancestor of origin/main: YES
```

## Git Status

```
## main...origin/main
 (local and origin in sync)
```

## DO NOT PUBLISH

Per instructions: committed and pushed to origin/main. No Replit publish.
