---
name: RSS archive pool thread-safety crash
description: psycopg2 SimpleConnectionPool is not thread-safe; concurrent asyncio.to_thread workers corrupt its C internals causing malloc SIGABRT in production.
---

# RSS archive pool thread-safety crash

## The rule
`data/rss_article_archive.py` (and any module whose pool is hit from concurrent threads) must use `ThreadedConnectionPool`, not `SimpleConnectionPool`.

**Why:** The RSS sweeper dispatches `upsert_with_cache` for every watchlist ticker via `asyncio.to_thread`, putting many OS threads into the pool simultaneously. `SimpleConnectionPool` has no internal locking — concurrent `getconn`/`putconn` calls corrupt its C-level linked-list structures, producing `malloc(): unsorted double linked list corrupted → SIGABRT`. This killed the production process during the deploy promote step, causing repeated healthcheck 500s → deployment failure.

**How to apply:** Any psycopg2 connection pool that is accessed from more than one thread must be `ThreadedConnectionPool`. Also add a `threading.Lock` around pool creation and the swap-on-error reset (`closeall → _pool = None`) so the reset path is also race-free. Pattern:

```python
import threading
_pool = None
_pool_lock = threading.Lock()

def _get_conn():
    global _pool
    for _ in range(2):
        with _pool_lock:
            if _pool is None:
                _pool = ThreadedConnectionPool(1, 5, _DB_URL)
            local_pool = _pool
        try:
            conn = local_pool.getconn()
            ...
            return conn
        except Exception:
            with _pool_lock:
                if _pool is local_pool:
                    _pool.closeall()
                    _pool = None
    return None
```

## Deployment failure pattern
The previous deploy at 18:20 succeeded because the RSS sweeper has a 120-second startup delay — the promote window completed before the first concurrent batch fired. The 22:13 deploy failed because the promote step was slower, the sweeper was already running, and the crash happened during the healthcheck window.
