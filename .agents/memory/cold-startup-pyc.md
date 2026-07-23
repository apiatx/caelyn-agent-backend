---
name: Autoscale cold-startup Python bytecode fix
description: Why deployment startup took ~50s (failing 60s health check) and how the build step fixes it
---

## The problem
`import main` takes **6.7s** in a warm environment (`.pyc` bytecode cached in `__pycache__`) but **~24s** locally cold (no `.pyc`).
In Replit autoscale deployment containers there are no pre-compiled `.pyc` files, so Python must tokenize + compile every `.py` file from scratch.
The cloud container is slower than the dev VM: cold startup reaches **~50s**, barely within the 60s health-check deadline. Any variance causes the promote step to fail.

## Root cause confirmed via
- `python3.11 -X importtime` showed total cumulative = **7.4s** (matches warm 6.7s local)
- Cold `import main` measured at 24.3s locally vs 7s warm — delta = 17s from compilation + cold disk I/O
- Cloud amplifies this 2–7× → ~50s before uvicorn binds on a bad day

## Two-layer fix (both in place)

### Layer 1: Build step — pre-compile .pyc bytecode
```
build = ["bash", "-c", "cd /home/runner/workspace/backend && python3.11 -m compileall -q agent core data routes services scripts *.py; python3.11 -m compileall -q .pythonlibs; true"]
```
- `;` not `&&` — both steps always run regardless of exit code
- `; true` at end — build step always exits 0 (syntax errors in .pythonlibs are harmless)
- Both verified to exit 0 even with Python 2 syntax errors
- Build step takes ~21s total (source dirs 6s + .pythonlibs 15s)
- With .pyc files: cold import ~7s → total startup ~12s → well within 60s

### Layer 2: Lazy-load edgar/psycopg2 chain
`insider_activity_service`, `congressional_trading_service`, `whale_watch_service` moved from module-level imports to **top of lifespan()** in `main.py`.
- `app.include_router()` for these three also moved inside lifespan (before yield) — valid FastAPI pattern
- Saves ~3-5s (edgar._filings is 3s cumulative) from cold module import
- Fallback: even WITHOUT .pyc files → import ~21s + lifespan ~8s = ~29s → still within 60s

## Key file counts
- Source dirs (agent/core/data/routes/services/scripts): 287 .py files
- .pythonlibs (installed packages): 5,774 .py, 5,796 .pyc (may not survive container rebuild — build step handles this)
- `compileall .` from backend root also hits `.cache`/`.config`/`.upm` (~5k extra files) → avoid! Use targeted dirs.

**Why:**
- `.pythonlibs` may be re-installed fresh in the container without .pyc files
- Targeted dirs avoids wasted time compiling hidden tool directories
