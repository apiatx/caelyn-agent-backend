---
name: Fib/Wave import path bug in entry_state_service
description: Silent ModuleNotFoundError caused primary_fib_context and wave_structure_label to always be None — fix and validation procedure documented.
---

## The Bug

`entry_state_service.py` lines 1277 and 1287 used:
```python
from backend.services.fib_engine import ...
from backend.services.wave_structure_engine import ...
```

The `backend.` prefix causes a silent `ModuleNotFoundError` at runtime because the server is launched from the `backend/` directory (uvicorn runs `main:app` from `backend/`). The correct import is:
```python
from services.fib_engine import ...
from services.wave_structure_engine import ...
```

**Why:** The `backend/` directory is the working directory for the process, so all internal service imports must use `from services.xxx` not `from backend.services.xxx`. Any new service import added to `entry_state_service.py` must follow this pattern.

**How to apply:** Any time a new module import is added to `entry_state_service.py`, verify it uses the `services.` prefix, not `backend.services.`. The same applies to all files under `backend/services/` that import sibling modules.

## Validation After Fix

After fixing the import, the warmup + flush procedure populates fib/wave:
1. Fix the import in `entry_state_service.py`
2. Restart the server
3. POST `/api/admin/stage2/force-warmup?force_all=true&symbols_csv=<extras>` — adds non-watchlist symbols (NVDA, ETFs, etc.)
4. Wait ~3–5 min for `asyncio.gather` to complete → `flush_entry_state_lkg()` fires
5. Entry state LKG flush triggers Confluence retained snapshot to detect stale fingerprint (`entry_lkg_mtime` changed)
6. Background Confluence rebuild runs (~27s), serving stale snapshot until complete
7. Next GET `/api/alpha/confluence` returns the fresh snapshot with fib/wave

## Post-Fix Coverage (2026-07-15)
- Entry LKG: 344/347 `primary_fib_context` + `wave_structure_label` populated (99.1%)
- Confluence snapshot: 199/200 fib/wave populated (99%)
- 3 symbols missing fib: very short history / recent IPO (unavailable by design)
- `bars_provider = canonical_tradier` for 344/347 entries
