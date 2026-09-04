---
name: Theme RS startup concurrency
description: Why Theme RS timeframe rebuilds must not fan out concurrently at cold startup.
---

Run the existing Theme RS timeframe refreshes sequentially during the initial
warmup rather than detaching every timeframe rebuild at once.

**Why:** Historical timeframes share the same large proxy and stock-history
dependencies. Concurrent cold rebuilds duplicate fallback/provider work and can
starve the single HTTP event loop even though each individual I/O operation is
nominally asynchronous.

**How to apply:** Keep per-timeframe locks and cadence guards, but bound initial
startup to one full timeframe rebuild at a time. Do not reintroduce a startup
`create_task()` fanout across all timeframes.