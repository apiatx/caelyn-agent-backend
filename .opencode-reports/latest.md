# Backend Deployment Startup Failure

## 1. Status

**CONDITIONAL — STARTUP ISSUE REMAINS**

No deployment logs were provided. The server starts and binds correctly in local testing. Root cause cannot be proven without the actual deployment traceback. The latest Home commits (8db8705e, d89428b5, 0a03046f) do not affect startup lifecycle.

## 2. Deployment Log Failure

**No deployment logs were pasted.** The task template contained `[PASTE LOGS HERE]` with no actual log content. Analysis was performed based on:
- Local reproduction of the deployment command
- Code inspection of startup path
- Previous controlled runs

## 3. Exact Start Command

From `.replit` deployment section:
```
cd /home/runner/workspace/backend && python3.11 -m uvicorn main:app --host=0.0.0.0 --port=5000
```

Port: 5000 (hardcoded, not from `$PORT` env). Host: 0.0.0.0.

## 4. Root Cause

**Cannot be proven without deployment logs.** Local reproduction shows:

1. `python3.11 -m uvicorn main:app` imports successfully
2. Lifespan yields in 0.03s — server is HTTP-ready immediately
3. `_do_init` completes in 30-45s — all providers initialize
4. `/ping` returns 200 during startup
5. `/health` returns 200 during startup (does not block on init)
6. No fatal tracebacks during import or startup

Possible deployment-specific causes (require logs to confirm):
- Replit Promote step health check timing out before init completes
- Port binding conflict with previous deployment instance
- Environment variable mismatch between dev and deployment

## 5. Whether Latest Home Commits Caused It

**No.** The three most recent commits (8db8705e, d89428b5, 0a03046f) only changed:

| Commit | Files | Scope |
|--------|-------|-------|
| 8db8705e | cache.py, coingecko_provider.py, home_risk_intelligence.py, tests | BTC reader, tests |
| d89428b5 | home_risk_intelligence.py | BTC source selection, decision summaries |
| 0a03046f | cache.py, home_risk_intelligence.py | Cache-only BTC, completeness reasons |

None of these files are imported during startup's critical path. `home_risk_intelligence.py` is only imported when `/api/home/risk-intelligence` is called. `coingecko_provider.py` is imported via `market_data_service.py` which already existed before these commits. `cache.py` changes only added/removed a method — no import-time side effects.

Pre-existing commits (ac9a4c0d, 2205449b, etc.) changed `main.py` startup timing and `tradier_provider.py` limiter behavior. These have been stable through multiple controlled runs.

## 6. Exact Correction

**No code correction is warranted without deployment logs.** The server:
- Imports correctly
- Binds correctly
- Responds to health checks
- All 193 tests pass
- Home endpoint works (proven in prior controlled runs)

If deployment logs reveal a specific error (e.g., missing environment variable, package import failure), a narrow fix would be applied to that specific cause.

## 7. Files Changed

**NONE.** No files were modified during this diagnosis.

## 8. Local Startup Validation

```
Command: cd /home/runner/workspace/backend && python3.11 -m uvicorn main:app --host=0.0.0.0 --port=5000
Result: Started successfully. Lifespan yielded in 0.03s. All services initialized.
Exit: Killed after 60s timeout wrapper (not a crash).
```

## 9. Host and Port Validation

- Port 5000 bound successfully (confirmed via uvicorn startup message)
- Host 0.0.0.0 — accessible from localhost

## 10. Health Endpoint Validation

`/health` returns immediately without `_wait_for_init()`:
```json
{"status": "starting", "init_complete": false, ...}
```

After init completes:
```json
{"status": "ok", "init_complete": true, ...}
```

## 11. Home Endpoint Validation

Not validated in this diagnosis run. Previously proven in controlled run (commit 2205449b): HTTP 200 in 2-4 seconds with real SPY/QQQ/VIX data.

## 12. Tests

```
$ python -m pytest -q tests/test_home_decision.py
74 passed, 0 failed

$ python -m pytest -q tests/test_home_risk_intelligence.py
119 passed, 0 failed

Total: 193 passed, 0 failed, 0 skipped
```

## 13. Remaining Risks

1. Without deployment logs, the specific Promote failure reason is unknown
2. Server dies after ~120-180s from Replit SIGTERM in local testing — may also happen in deployment
3. Tradier rate limit exhaustion during startup may delay provider availability
4. Finviz timeouts during startup may slow background task completion

## 14. Final Git Status

```
## main...origin/main [ahead 23]
8db8705e (HEAD -> main) fix(home): test exact BTC cache selection and freshness
```

No uncommitted production file changes.

## 15. Local Commit

**NOT APPLICABLE** — no files were modified during this diagnosis.

## 16. Push Status

**NOT PUSHED** — user must run `git push origin main`.
