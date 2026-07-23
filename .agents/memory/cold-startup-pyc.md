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

## The fix
Added a deployment **build step** to pre-compile all Python bytecode:
```
build = ["bash", "-c", "cd /home/runner/workspace/backend && python3.11 -m compileall -q agent core data routes services scripts *.py && python3.11 -m compileall -q .pythonlibs"]
```

**Why:**
- `compileall` creates `__pycache__/*.pyc` files; they're included in the container image
- On startup Python reads pre-compiled bytecode (smaller files, no compilation step)
- Build step takes ~21s total (source dirs 6s + .pythonlibs 15s)
- Import on next cold start drops from ~50s → ~7s → well within 60s deadline

## Key file counts
- Source dirs (agent/core/data/routes/services/scripts): 287 .py files
- .pythonlibs (installed packages): 5,774 .py, 5,796 .pyc (already compiled but may not survive container rebuild)
- `compileall .` from backend root also hits `.cache`/`.config`/`.upm` (~5k extra files) → avoid! Use targeted dirs.

**Why:**
- `.pythonlibs` may be re-installed fresh in the container without .pyc files
- Targeted dirs avoids wasted time compiling hidden tool directories
- The SyntaxError in .pythonlibs (Python 2 file) is harmless — compileall `-q` skips it and continues
