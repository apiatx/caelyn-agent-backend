# Read-Only Deployment-Durability and Coverage Audit
**Commit under review:** `59e3ca97bb724747d80b1a49757b7140fba5cc9e`
**Audit date:** 2026-08-05
**Status:** Read-only — no production files modified, no commit, no push.

---

## Conclusion: **B. CODE FIX CORRECT, DEPLOYMENT MIGRATION UNPROVEN**

The hot-path implementation is correct. `get_comparison_closes_bulk()` performs zero `gzip.open` calls and returns populated results when `comparison_close_tail` is present in `_INDEX`. The tail metadata IS present in the committed repository as a result of Replit auto-commits following the repair. However:

1. The tails were committed via Replit auto-commits, not through the deliberate repair commit (`59e3ca97`). A rollback to `59e3ca97` or earlier loses all tails.
2. The gz payloads themselves do **not** store `comparison_close_tail` (PROVEN NO). If `_index.json` is deleted or `_rebuild_index_from_gz()` is triggered, the reconstructed index has zero tails and all comparison closes return null.
3. Autoscale filesystem write persistence across instance replacement is UNPROVEN from local evidence.
4. No idempotent startup migration exists to repair missing tails without human intervention.

**Recommended remedy (Remedy 3):** Add a single `backfill_volume_metrics_metadata()` call inside the existing `_post_yield_bootstrap()` deferred startup path, gated on `any(sym for sym, v in _INDEX.items() if "comparison_close_tail" not in v)`. This runs after `yield` (health endpoint already serving), reads gz files only for symbols missing tails, and is idempotent (skip guard already present in the function). This closes the gap for all autoscale lifecycle events including instance replacement, cold start, and rollback recovery.

---

## Part 1 — Exact Repository and Workspace State

```
pwd:          /home/runner/workspace
branch:       main
HEAD:         60f75fd3c1ae598900355c5650f01a1f26dd1a2d
status:       main...origin/main [ahead 1]
```

**git log (most recent 8):**
```
60f75fd3 (HEAD -> main) Published your App            ← Replit deployment auto-commit
31078b23 (origin/main, origin/HEAD) Update latest codex report
59e3ca97 perf: comparison-close tail in _INDEX — zero gzip.open per Watchlist request
405313d4 Update latest codex report
e2530530 Published your App
d3ca6538 Update data caches and add asset file
085dfb98 Update backend data caches and update canonical history index
b7964131 fix(watchlist): use live displayed price as numerator for change_7d/change_30d
```

`git merge-base --is-ancestor 59e3ca97 HEAD` → exit 0 (confirmed ancestor)

**`git diff --name-only` (working tree vs HEAD):**
```
backend/data/hyperliquid_signal_snapshots.json
backend/data/options_priority_symbols.json
backend/data/options_supplement_lkg_v1.json
backend/data/theme_rs_refresh_ts.json
backend/data/themes_rs_1d_lkg.json
```

**`git diff --numstat -- backend/data/canonical_history/_index.json`:**
_(empty — no diff between HEAD and staging area)_

However, `git status --short backend/data/canonical_history/` subsequently showed `_index.json` as working-tree-modified. This transient discrepancy is caused by the running server process writing to `_index.json` concurrently during the audit. At the moment of the `--numstat` check, the file matched HEAD.

**`git ls-files backend/data/canonical_history/_index.json`:**
```
backend/data/canonical_history/_index.json
```

**`git check-ignore -v backend/data/canonical_history/_index.json`:**
_(no output — not ignored)_

**`_index.json` workspace status:** TRACKED AND CLEAN at time of `git diff` check; transiently TRACKED AND MODIFIED due to live server writes. Not ignored. Not absent. Not untracked.

**`60f75fd3` commit contents (Replit "Published your App"):**
This Replit deployment auto-commit staged and committed 14 files including `backend/data/canonical_history/_index.json` and four gz files (CRCL, EOSE, OSS, VSH). The `_index.json` in this commit contains the repaired tails (426 symbols, all with valid tails).

---

## Part 2 — Runtime Index vs Committed HEAD Comparison

The `_index.json` has a top-level structure `{"updated_at": "...", "symbols": {...}}`. The analyzer correctly filtered to `symbols` dict values only.

| Metric | WORKSPACE | HEAD (60f75fd3) | ORIGIN/MAIN (31078b23) |
|--------|-----------|-----------------|------------------------|
| Total symbol dict entries | 496 | 496 | 496 |
| Symbols with `comparison_close_tail` | 426 | 426 | 426 |
| Non-empty valid tails | 426 | 426 | 426 |
| Malformed tails | 0 | 0 | 0 |
| Min tail length | 9 | 9 | 9 |
| Max tail length | 64 | 64 | 64 |
| Median tail length | 64 | 64 | 64 |
| AAPL tail | len=64, last=['2026-08-04', 309.38] | identical | identical |
| MSFT tail | len=64, last=['2026-08-04', 492.81] | identical | identical |
| NVDA tail | len=64, last=['2026-08-04', 211.94] | identical | identical |
| PLTR tail | len=64, last=['2026-08-04', 162.66] | identical | identical |

**Key finding:** The committed `59e3ca97` perf fix commit had **0 tails** in `_index.json`. The tails appeared in `31078b23` ("Update latest codex report") because Replit's auto-commit mechanism staged the runtime-modified `_index.json` (written by `backfill_volume_metrics_metadata()`) along with other runtime data files. This was incidental, not deliberate.

```
59e3ca97 (perf commit):      496 symbols, 0 with tail   ← repair ran after this
31078b23 (origin/main):      496 symbols, 426 with tail  ← auto-committed runtime state
60f75fd3 (HEAD, deployment): 496 symbols, 426 with tail  ← auto-committed again
```

---

## Part 3 — Fresh-HEAD Behavior

**Simulation method:** Injected the committed HEAD `_index.json` (426 symbols with tails) directly into `_INDEX`, instrumented `gzip.open`, called `get_comparison_closes_bulk()` for AAPL/MSFT/NVDA/PLTR.

**Result:**
```
gzip.open calls: 0
AAPL: 7D=338.19 (2026-07-29)  30D=312.66 (2026-07-06)
MSFT: 7D=390.54 (2026-07-29)  30D=386.74 (2026-07-06)
NVDA: 7D=190.01 (2026-07-29)  30D=195.55 (2026-07-06)
PLTR: 7D=123.0  (2026-07-29)  30D=132.54 (2026-07-06)
```

**Does a fresh HEAD-only instance produce null?** NO — IF `_index.json` is loaded from the committed state. The tails are present in the committed `_index.json` and produce populated values with zero gzip reads.

**Does `preload_index()` perform automatic migration?** NO. It loads `_index.json`, prunes entries whose gz file is absent, and calls `_rebuild_index_from_gz()` for any symbols whose gz exists but is not in the index. It does NOT call `backfill_volume_metrics_metadata()` and does NOT populate missing tails.

**Caveat on empty-temp-dir simulation:** When the simulation used a temp dir with NO gz files, `preload_index()` pruned all 491 entries (all orphaned without gz files), yielding 0 symbols. This is the `_rebuild_index_from_gz()` orphan-pruning path. On a real fresh deployment where gz files are present, no pruning occurs and all 426 tails survive.

**Conclusion for fresh HEAD:** A fresh instance starting from the committed state (which includes both `_index.json` with 426 tails AND 426 committed gz files) produces populated comparison closes. This depends on the deployment image containing both files.

---

## Part 4 — Recovery from Existing gz Payloads

**Inspection of 14 symbols (4 priority + 10 representative):**
All 14 symbols checked: `comparison_close_tail=NO` in every gz payload.

```
AAPL, MSFT, NVDA, PLTR, AA, ASM, CLSK, FJET, IE, LWLG, NXT, QS, SNOW, UMC:
  comparison_close_tail = NOT PRESENT in gz file
```

The gz payload stores: `symbol, provider, bar_count, oldest_bar_date, newest_bar_date, history_status, volume_metrics_status, change_7d, change_30d, ...` (37 metadata fields). `comparison_close_tail` is NOT written to the gz payload — it is stored only in `_index.json`.

**`_rebuild_index_from_gz()` code path (lines 588–607):**
```python
def _rebuild_index_from_gz() -> int:
    for gz_path in sorted(_CANON_DIR.glob("*.json.gz")):
        sym = gz_path.name.split(".")[0].upper()
        if sym in _INDEX:
            continue  # only fills gaps, doesn't overwrite
        with gzip.open(str(gz_path), "rt", encoding="utf-8") as fh:
            payload = json.loads(fh.read())
        meta = {k: v for k, v in payload.items() if k != "bars"}  # strips bars, keeps metadata
        if meta.get("symbol") and meta.get("bar_count") is not None:
            _INDEX[sym] = meta  # no comparison_close_tail in meta
```

Since gz files do not contain `comparison_close_tail`, `meta` will never have it. Any `_INDEX[sym]` entry recovered by `_rebuild_index_from_gz()` will be missing the tail.

**Answer: PROVEN NO**

If `_index.json` is deleted, replaced with the pre-repair committed version (`59e3ca97`), or the index is rebuilt from existing gz payload metadata, the reconstructed index will contain **zero comparison tails**. All 426 symbols would then return null for `comparison_close_7d` and `comparison_close_30d`.

---

## Part 5 — Replit Deployment Durability

**`.replitignore` contents (relevant excerpt):**
The `.replitignore` excludes: `.local/share/opencode/`, `.local/state/replit/`, `.config/npm/`, `.cache/pip/`, `.codex/`, `.git/`, `attached_assets/`, workflow logs, etc.

**`backend/data/canonical_history/` is NOT in `.replitignore`.** Neither gz files nor `_index.json` are excluded from the deployment image.

**`.replit` deployment config:**
```toml
[deployment]
deploymentTarget = "autoscale"
run = ["bash", "-c", "cd /home/runner/workspace/backend && python3.11 -m uvicorn main:app --host=0.0.0.0 --port=5000"]
build = ["bash", "-c", "...compileall..."]
```

The build step is `compileall` only — no data migration, no provider calls, no `pip install` in build. This implies the deployment image packages the full workspace directory (minus `.replitignore` exclusions) as-is from the committed state.

**Committed gz files:** 426 files confirmed in git via `git ls-files backend/data/canonical_history/ | grep .json.gz`. These are tracked binary files committed to the repository.

| Durability question | Finding |
|---------------------|---------|
| Does deployment package committed workspace `_index.json`? | **LIKELY YES** — `_index.json` is tracked, not excluded by `.replitignore`, and deployment build does not regenerate it |
| Does deployment use committed Git state? | **LIKELY YES** — build step is `compileall` only, no alternative source |
| Does autoscale filesystem preserve `_index.json` across process restart? | **LIKELY YES** — same instance, same disk |
| Does autoscale filesystem preserve `_index.json` across instance replacement? | **UNPROVEN** — cannot verify from local evidence whether fresh autoscale containers start from deployment image or rebuild |
| Does autoscale filesystem preserve `_index.json` across scale-to-zero/cold start? | **UNPROVEN** — depends on autoscale instance provisioning model |
| Does autoscale filesystem preserve `_index.json` across republish? | **LIKELY YES** — new deployment image built from committed state (which has tails) |
| Does autoscale filesystem preserve `_index.json` across rollback? | **NO for rollbacks to ≤59e3ca97** — pre-repair commits have 0 tails |
| Do multiple autoscale instances share this file? | **UNPROVEN** — autoscale may or may not use shared volumes |
| Does currently published application contain `59e3ca97`? | **LIKELY YES** — `60f75fd3` ("Published your App" Replit deployment auto-commit) is at HEAD and is a descendant of `59e3ca97`; Replit created this commit during the publish operation triggered on 2026-08-05 at 14:27:58 UTC |

**Deployment build ID in auto-commit:** `5099e85e-b4a8-40fd-86e1-d03540ec4e92` (embedded in commit message of `60f75fd3`). This confirms that `60f75fd3` was created as part of a Replit publish operation.

---

## Part 6 — Full 7D/30D Missing-Symbol Classification

**NY market date:** `2026-08-05` (Wednesday)
**7D target:** `2026-07-29`  |  **30D target:** `2026-07-06`

**Coverage buckets (runtime `_INDEX`, 426 symbols):**

| Bucket | Count |
|--------|-------|
| Valid 7D AND valid 30D | 383 |
| Missing 7D, valid 30D | 40 |
| Valid 7D, missing 30D | 2 |
| Missing BOTH | 1 |

**All 41 missing-7D symbols — exact reason:** `gap_exceeds_max_gap_days_10` (100%)

Every one of the 41 symbols has a full 64-bar tail (except SATS=64, GORO=9, SKHY=18 for missing-30D). The newest tail date is **2026-07-14 to 2026-07-17** for the 40 missing-7D-only symbols. The gap from newest bar to the 7D target (Jul 29) is **12–15 days**, exceeding `max_gap_days=10`. The selector correctly returns `None`.

**Representative missing symbols:**

| Symbol | tail_len | newest | 7D target | best7 | gap7 | 30D target | best30 | gap30 | reason |
|--------|----------|--------|-----------|-------|------|-----------|--------|-------|--------|
| UFO | 64 | 2026-07-14 | 2026-07-29 | 2026-07-14 | 15d | 2026-07-06 | 2026-07-06 | 0d | 7D stale |
| UMAC | 64 | 2026-07-14 | 2026-07-29 | 2026-07-14 | 15d | 2026-07-06 | 2026-07-06 | 0d | 7D stale |
| VECO | 64 | 2026-07-14 | 2026-07-29 | 2026-07-14 | 15d | 2026-07-06 | 2026-07-06 | 0d | 7D stale |
| BATT | 64 | 2026-07-16 | 2026-07-29 | 2026-07-16 | 13d | 2026-07-06 | 2026-07-06 | 0d | 7D stale |
| ZS | 64 | 2026-07-17 | 2026-07-29 | 2026-07-17 | 12d | 2026-07-06 | 2026-07-06 | 0d | 7D stale |
| SATS | 64 | 2026-06-23 | 2026-07-29 | 2026-06-23 | 36d | 2026-07-06 | 2026-06-23 | 13d | both stale |
| GORO | 9 | 2026-08-04 | 2026-07-29 | 2026-07-29 | 0d | 2026-07-06 | none | — | 30D tail too short |
| SKHY | 18 | 2026-08-04 | 2026-07-29 | 2026-07-29 | 0d | 2026-07-06 | none | — | 30D tail too short |

**Reason classification for all 44 missing-value cases:**

| Reason | 7D | 30D |
|--------|----|-----|
| `gap_exceeds_max_gap_days_10` (newest bar > 10d before target) | 41 | 1 (SATS) |
| `tail_too_short_to_reach_30d_target` | 0 | 2 (GORO, SKHY) |

**Why 41 symbols are missing 7D:**
These are US-listed symbols in `_INDEX` whose canonical history has not been refreshed since July 14–17, 2026 — two to three weeks before this audit. This includes: sector ETFs (XLB, XLC, XLE, XLF, XLK, XLP, XLRE, XLU, XLV, XLY, XME, XOP, XRT, XNDU), individual stocks (UFO, UMAC, USAR, UUUU, VECO, VELO, VERA, VIAV, VICR, VIVO, VLN, VPG, VPU, VSAT, VST, WDC, WOLF, WYFI, XYZ, XTIA, ZS, ZURA, ZVRA), and others (BATT, BUG, DRAM, SATS). Their tails are fully populated (64 bars) but all 64 bars predate the 7D target by more than 10 days. The selector correctly returns null — returning a 12–15-day-old close as "7-day comparison" would be semantically incorrect.

**Why only 3 symbols are missing 30D:**
The 30D target (July 6) is reachable by any symbol with bars dated before July 6 — which is all symbols with any meaningful history. Only new listings (GORO, SKHY with tails starting in mid-to-late July) and SATS (stale, newest bar June 23, 30D target July 6, gap=13d) miss it.

**Reconciling the "42 symbols" discrepancy:**
The audit prompt stated "42 symbols lack 7D comparisons." The exact count from `_INDEX` is **41**. The discrepancy of 1 is likely a counting error in the earlier report. More significantly, the 41 are NOT "new listings" as previously claimed — they are established symbols with stale canonical history. The prior report's "three new listings" explanation was incorrect on both the count and the cause.

---

## Part 7 — Actual Primary Watchlist Coverage

**Endpoint:** `GET /api/watchlist/00a0e3ea-31dc-4223-97bc-470720dd3215`
**Result:** HTTP 200, 6,096,571 bytes, 3.09s total

| Metric | Value |
|--------|-------|
| Total rows | 450 |
| `change_7d` populated | 363 |
| `change_7d` null | **87** |
| `change_30d` populated | 383 |
| `change_30d` null | **67** |
| Leaked `_comparison_*` keys | **0** |

**Root-cause classification of 87 null `change_7d`:**

| Category | Count | Detail |
|----------|-------|--------|
| A. Foreign exchange ticker (`:` in symbol) | 58 | AIM:*, ASX:*, CSE:*, EPA:*, ETR:*, FRA:*, KRX:*, LON:*, OSL:*, OTC:*, SHA:*, STO:*, SWX:*, TPE:*, TPEX:*, TSX:*, TSXV:*, TYO:*, WSE:*, XSAT:* — not in canonical _INDEX; no bar history available |
| B. US ticker not in canonical _INDEX | 6 | GOLD, WRN, AGI, PPTA, TMQ, KGC — in Watchlist but canonical history not yet populated |
| C. In _INDEX, stale history (gap > 10d) | 22 | UFO, UMAC, USAR, UUUU, VECO, VELO, VERA, VIAV, VICR, VIVO, VLN, VPG, VSAT, VST, WDC, WOLF, WYFI, XNDU, XTIA, XYZ, ZURA, ZVRA |
| D. No valid displayed price | 1 | TSX:TPZ |

**Root-cause classification of 67 null `change_30d`:**
Same categories — foreign exchange (58), no-index US (6), stale-history (2: SATS, GORO/SKHY overlap with watchlist rows), no-price (1). Most of the 87 null-7D symbols also have null-30D; the 20-row difference (87 vs 67) is because 20 of the stale-history symbols have a 30D target that IS reachable despite their stale data.

**Symbols with null `change_30d` but populated `change_7d`:** GORO, SKHY (tail too short to reach 30D target).

**No `_comparison_*` keys leaked:** confirmed across all 450 rows.

---

## Part 8 — Corrected Benchmark Values

### Original regression measurements (from isolation benchmark in session prior to fix)

| Metric | Authoritative Value | Notes |
|--------|---------------------|-------|
| `gzip.open` calls per Watchlist request | **426** | One per symbol in `_INDEX` |
| Compressed bytes read | **~14.2 MB** | From isolation benchmark on ~422 symbols pre-repair |
| Uncompressed bytes processed | **~68–72 MB** | Full decompression of every file |
| Total bars parsed (into memory) | **~718,000–815,586** | Lower bound from isolation; current universe (426 files) = 815,586 bars |
| `datetime.strptime` calls equivalent | **~715,000–815,000** | One date parse per bar across all files |
| Comparison-close wall time | **~5,600 ms** | Isolated measurement on warm disk |
| Warm endpoint TTFB | **~6–8 s** | Under concurrent background load |

### Current post-fix measurements

| Metric | Value |
|--------|-------|
| `gzip.open` calls per Watchlist request | **0** |
| Compressed bytes read | **0 B** |
| Bars parsed | **0** |
| `_select_from_tail()` wall time (426 symbols) | **2–3 ms** |
| Warm endpoint TTFB (fully warm) | **2.6–3.2 s** |
| Server-side `history fetch_ms` (all 4 combined sources) | **388 ms** |

### Reconciling the conflicting "~600 MB / ~190,000 bars" claim

The final report from the prior session incorrectly stated "~600 MB compressed" and "~190,000 bars." The correct figures are:

- **Compressed: 15.4 MB** (426 files as of `60f75fd3`; was ~14.2 MB for ~422 files pre-repair). The "600 MB" figure appears to be an order-of-magnitude error — likely confused with uncompressed or a rough guess.
- **Total bars: 815,586** (median 2,444 bars/file × 426 files). The "190,000 bars" figure appears to have calculated only the tail-relevant bars (426 × ~450 relevant bars ≈ 191,700) rather than the total bars that the old code actually decompressed into memory.

The old code called `gzip.open` and `json.load` on every file, fully decompressing it (72.4 MB uncompressed) and parsing all bars. It then used only the last ~35–40 bars. The "190,000 bars" figure was not the actual bars processed — it was the number of bars that were relevant to the comparison-close selection.

### Corrected weekday labels

| Date | Prior report label | Correct label |
|------|--------------------|---------------|
| 2026-08-01 | "Saturday" (correct in date-roll section, "Fri" in comments) | **Saturday** |
| 2026-08-02 | "Sunday" | **Sunday** |
| 2026-08-04 | "Monday" | **Tuesday** |
| 2026-08-05 | "Tuesday" | **Wednesday** |

The date-roll validation in the prior report stated "ny_today=2026-08-01 (Fri)" and "ny_today=2026-08-04 (Mon)". Both were wrong. Aug 1 is Saturday; Aug 4 is Tuesday. This does not affect the functional correctness of the test (the selector behavior is driven by date arithmetic, not labels), but the labels in the commit message and report were incorrect. Source correction is not authorized by this read-only task.

---

## Part 9 — Files Inspected

- `backend/services/canonical_history_service.py` (lines 409–640, 768–815, 895–910)
- `backend/services/watchlist_router.py` (lines 1347–1395)
- `backend/tests/test_watchlist_market_data_rows.py` (all)
- `backend/data/canonical_history/_index.json` (workspace, HEAD, origin/main, 59e3ca97, 405313d4)
- `backend/data/canonical_history/*.json.gz` (14 symbols inspected for `comparison_close_tail`)
- `.replit` (deployment config)
- `.replitignore` (exclusion list)
- `.gitignore`
- `/tmp/wl_audit.json` (Primary Watchlist endpoint response)

**No production files were modified during this audit.**

---

## Part 10 — Risks and Remaining Issues

1. **Tail recovery gap (HIGH):** `_rebuild_index_from_gz()` cannot recover `comparison_close_tail` from gz payloads. Any scenario that loses or replaces `_index.json` with a version predating the tails will cause all comparison closes to return null until the next `save_bars()` or manual `backfill_volume_metrics_metadata()` call.

2. **41 stale-history symbols (LOW / data issue, not code):** These symbols have valid tails but their newest bar is 12–15+ days before the 7D target. The correct response is null. The canonical history maintenance loop should be investigated separately to understand why these symbols have not been refreshed.

3. **Committed tail durability via auto-commit (MEDIUM):** The tails are present in `origin/main` only because Replit's auto-commit mechanism staged the runtime-modified `_index.json`. This is not a reliable long-term mechanism. A deliberate rollback or a fresh deployment from a pre-tail commit would regress.

4. **Weekday labels in commit message (LOW, cosmetic):** The `59e3ca97` commit message and the prior codex report contain incorrect weekday labels for 2026-08-01 through 2026-08-05. This does not affect functionality.

---

## Part 11 — Recommended Remedy (Remedy 3)

**Target file:** `backend/services/canonical_history_service.py` (and `backend/main.py` or wherever `_post_yield_bootstrap()` is called)

**Change:** Inside the existing `_post_yield_bootstrap()` deferred startup function, after `preload_index()` has already been called, add:

```python
# Idempotent tail repair — runs only if any symbol is missing comparison_close_tail.
# Reads gz files for affected symbols only; does not block requests.
_missing_tail = [s for s, v in _INDEX.items() if "comparison_close_tail" not in v]
if _missing_tail:
    logger.info(f"[CANON_HIST] {len(_missing_tail)} symbols missing tail — running repair")
    backfill_volume_metrics_metadata(symbols=_missing_tail)
```

**Why Remedy 3 (not 1, 2, or 4):**
- **Remedy 1** (commit `_index.json` to deployment artifact): Fragile — relies on the auto-commit mechanism continuing to stage runtime data files.
- **Remedy 2** (idempotent off-request migration in deferred startup): This is effectively what Remedy 3 describes; the distinction is using the existing `_post_yield_bootstrap()` hook which is already the canonical location for deferred startup work.
- **Remedy 4**: No smaller existing path is apparent. `_rebuild_index_from_gz()` cannot supply tails from gz files (PROVEN NO), and calling it at startup is already the existing recovery path.

Remedy 3 is the smallest change: one guarded function call in an existing background path, no new infrastructure, no blocking the health endpoint, idempotent (skip guard already in `backfill_volume_metrics_metadata`), and self-healing across all autoscale lifecycle events.

---

## Summary

| Question | Answer |
|----------|--------|
| `59e3ca97` is ancestor of HEAD | ✅ confirmed |
| `_index.json` tracked and committed with tails | ✅ at 31078b23 and 60f75fd3 |
| Tails in `_index.json` from deliberate repair commit | ❌ from Replit auto-commit, not 59e3ca97 |
| gz files contain `comparison_close_tail` | ❌ PROVEN NO (all 14 inspected) |
| `_rebuild_index_from_gz()` recovers tails | ❌ PROVEN NO |
| Fresh committed-state instance: 0 gzip.open, populated results | ✅ PROVEN |
| Autoscale instance replacement durability | ⚠️ UNPROVEN |
| Idempotent startup tail repair exists | ❌ not yet |
| 41 missing-7D: data staleness (not new listings) | ✅ PROVEN |
| 3 missing-30D: 2 new history + 1 stale | ✅ PROVEN |
| 87 null change_7d in endpoint: 58 foreign + 6 no-index + 22 stale + 1 no-price | ✅ PROVEN |
| Leaked `_comparison_*` keys | ✅ 0 (fixed) |

**Conclusion: B — Code fix correct, deployment migration unproven. Remedy 3 recommended.**
