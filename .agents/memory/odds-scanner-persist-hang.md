---
name: Odds scanner _persist_catalog_bg hang
description: Why _persist_catalog_bg in run_in_executor can hang indefinitely and how to prevent it.
---

## Rule
Never call `_get_conn()` / `SimpleConnectionPool()` from inside `_persist_catalog_bg` (executor thread) unless `connect_timeout` is embedded in the DSN URL string. As a belt-and-suspenders guard, always wrap the `run_in_executor` call in `asyncio.wait_for(timeout=90.0)`.

## Why
`SimpleConnectionPool(1, 5, DSN_URL, connect_timeout=10)` — the `connect_timeout` kwarg is NOT reliably honored by libpq when the DSN is a full URL (`postgresql://...`). libpq only reliably applies `connect_timeout` when it appears as a query parameter in the URL itself (`?connect_timeout=10`) or as a named key in a key=value connection string. Result: pool creation blocks indefinitely, trapping the executor thread and causing `await loop.run_in_executor(...)` to never resolve.

## How to apply
1. `_sanitize_database_url()` in `pg_storage.py` now injects `connect_timeout=10` into the URL query string if absent — this covers ALL pool creation paths.
2. `_crawl_and_persist_catalog()` wraps its `run_in_executor` call in `asyncio.wait_for(timeout=90.0)` — if the DB persist stalls, the scan loop continues and the in-memory cache is still populated.
3. `_persist_catalog_bg` must NOT call `_ensure_catalog_ddl_bg()` — that is redundant because `upsert_catalog_rows()` already calls `ensure_catalog_table()` internally.
