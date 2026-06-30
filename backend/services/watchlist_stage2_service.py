"""
Watchlist Stage 2 Analysis — disk-backed LKG + controlled off-hours warmup.

Architecture
============
  warmup_stage2(tickers)
      Fetches 400 days of Tradier daily bars for each watchlist ticker
      (batched, 4 concurrent, rate-controlled), computes analyze_symbol_stage(),
      and persists results to disk.  Never called on page render.

  get_stage2(sym) -> dict
      Reads from the in-memory LKG dict — zero I/O, safe to call inside
      _build_ticker_row() on every request.

  load_lkg()
      Called once at startup.  Reads the disk file into the in-memory dict so
      values are immediately available after a server restart.

  force_warmup_stage2_nulls()
      Admin/recovery path.  Bypasses the freshness gate for all entries where
      label is None and score is None (including fetch_failed and legacy nulls).
      Also re-processes entries with status="fetch_failed" even if computed recently.
      Leaves valid-label entries untouched.  Called via POST /api/admin/stage2/force-warmup.

Disk LKG : backend/data/watchlist_stage2_lkg.json
Format    : {updated_at: ISO, results: {SYM: {score, label, reason, status, computed_at}}}

Status field (added for freshness discrimination):
  "ok"           — stage computed successfully from valid bars
  "no_bars"      — provider returned no usable historical bars for this symbol
  "fetch_failed" — provider call failed, timed out, or returned unexpectedly empty
  (legacy null)  — entries written before status field existed; treated as fetch_failed

Freshness TTLs:
  ok           → 20h  (normal recompute cadence)
  no_bars      → 20h  (symbol has no Tradier-tradeable history; retry same cadence)
  fetch_failed → 2h   (transient failure; retry quickly)
  legacy       → 2h   (unknown outcome; assume transient failure)

LKG Overwrite Guard:
  If a bulk warmup run computes > 50 symbols but produces < 20% valid labels
  while the existing LKG had ≥ 20% valid, the run is treated as degraded.
  In degraded mode: only entries with new valid labels are written; existing
  valid labels are NOT overwritten with nulls from this failed run.
  fetch_failed status is still recorded so those symbols retry sooner.
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ── Disk LKG path ────────────────────────────────────────────────────────────
_LKG_PATH = Path(__file__).parent.parent / "data" / "watchlist_stage2_lkg.json"

# Normal freshness TTL for successfully computed or no_bars entries
_FRESH_HOURS = 20

# Short retry TTL for fetch_failed and legacy-null entries
_FRESH_HOURS_FAILED = 2

# Max Tradier calls in flight simultaneously (conserv. — market data budget)
_CONCURRENCY = 4

# Days of history to fetch (same as theme_rs_service)
_HIST_DAYS = 400

# Cache TTL for bar entries (1h, same as tdier_hist)
_BAR_TTL = 3600

# Bulk degraded-run guard thresholds
_GUARD_MIN_SYMBOLS   = 50    # only apply guard when batch is large enough
_GUARD_MIN_COVERAGE  = 0.20  # < 20% valid labels in new run → degraded

# ── In-memory LKG ────────────────────────────────────────────────────────────
# Keyed by uppercase symbol → {score, label, reason, status, computed_at}
_STAGE2_LKG: dict[str, dict] = {}
_lkg_loaded_at: float = 0.0


# ── Startup ───────────────────────────────────────────────────────────────────

def load_lkg() -> None:
    """
    Load the disk LKG into _STAGE2_LKG.  Called once at server startup.
    Safe to call multiple times (idempotent).
    """
    global _lkg_loaded_at
    if not _LKG_PATH.exists():
        print("[STAGE2_WL] no disk LKG found — starting cold")
        return
    try:
        data = json.loads(_LKG_PATH.read_text())
        results = data.get("results") or {}
        _STAGE2_LKG.clear()
        _STAGE2_LKG.update({s.upper(): v for s, v in results.items()})
        _lkg_loaded_at = time.time()
        updated_at = data.get("updated_at", "unknown")
        non_null = sum(1 for v in _STAGE2_LKG.values() if v.get("score") is not None)
        null_count = len(_STAGE2_LKG) - non_null
        status_counts: dict[str, int] = {}
        for v in _STAGE2_LKG.values():
            st = v.get("status") or "legacy"
            status_counts[st] = status_counts.get(st, 0) + 1
        print(
            f"[STAGE2_WL] disk LKG loaded: {len(_STAGE2_LKG)} symbols "
            f"({non_null} valid, {null_count} null) "
            f"status_counts={status_counts} updated_at={updated_at}"
        )
    except Exception as exc:
        print(f"[STAGE2_WL] disk LKG load error (non-fatal): {exc}")


# ── Read path (zero I/O) ──────────────────────────────────────────────────────

def get_stage2(sym: str) -> dict:
    """
    Return the cached stage2_breakout dict for *sym*.
    Returns {"score": None, "label": None, "reason": None} when not found.
    Never issues any I/O.

    Includes expanded technical metrics and provenance fields when available
    (populated by the Phase-2 warmup).  Old entries that pre-date Phase 2 will
    have None for those fields — callers must handle None gracefully.
    """
    entry = _STAGE2_LKG.get(sym.upper())
    if entry is None:
        return {"score": None, "label": None, "reason": None}
    return {
        # ── Backward-compat (always present) ─────────────────────────────────
        "score":  entry.get("score"),
        "label":  entry.get("label"),
        "reason": entry.get("reason"),
        # ── Stage internals ───────────────────────────────────────────────────
        "signals":                 entry.get("signals"),
        "stage_confidence":        entry.get("stage_confidence"),
        "stage_confidence_reason": entry.get("stage_confidence_reason"),
        # ── Technical metrics ─────────────────────────────────────────────────
        "technical_metrics":      entry.get("technical_metrics"),
        "technical_state":        entry.get("technical_state"),
        "technical_timing_score": entry.get("technical_timing_score"),
        # ── Provenance ────────────────────────────────────────────────────────
        "history_source":     entry.get("history_source"),
        "bars_count":         entry.get("bars_count"),
        "history_start_date": entry.get("history_start_date"),
        "history_end_date":   entry.get("history_end_date"),
        "has_ohlcv":          entry.get("has_ohlcv"),
        "has_200d":           entry.get("has_200d"),
        "has_252d":           entry.get("has_252d"),
        "computed_at":        entry.get("computed_at"),
    }


# ── Warmup helpers ────────────────────────────────────────────────────────────

def _ttl_hours_for_entry(entry: dict) -> float:
    """
    Return the freshness TTL (hours) for a given LKG entry based on its status.

    Status rules:
      "ok"           → 20h  (normal cadence)
      "no_bars"      → 20h  (no tradeable history; same cadence is fine)
      "fetch_failed" → 2h   (transient failure; retry soon)
      missing/legacy → 2h   (unknown; treat as potentially failed)
    """
    status = entry.get("status") or ""
    if status in ("ok", "no_bars"):
        return float(_FRESH_HOURS)
    return float(_FRESH_HOURS_FAILED)


def _is_fresh(sym: str) -> bool:
    """
    Return True if the in-memory entry is within its status-appropriate TTL.

    TTLs:
      ok / no_bars   → _FRESH_HOURS     (20h)
      fetch_failed   → _FRESH_HOURS_FAILED (2h)
      legacy (none)  → _FRESH_HOURS_FAILED (2h)
    """
    entry = _STAGE2_LKG.get(sym.upper())
    if not entry:
        return False
    computed_at_str = entry.get("computed_at") or ""
    if not computed_at_str:
        return False
    try:
        computed_at = datetime.fromisoformat(computed_at_str.replace("Z", "+00:00"))
        age_h = (datetime.now(timezone.utc) - computed_at).total_seconds() / 3600
        ttl = _ttl_hours_for_entry(entry)
        return age_h < ttl
    except Exception:
        return False


def _is_null_entry(sym: str) -> bool:
    """Return True if the LKG entry has no valid stage label (null or fetch_failed)."""
    entry = _STAGE2_LKG.get(sym.upper())
    if not entry:
        return True
    return entry.get("label") is None or entry.get("score") is None


async def _fetch_bars(sym: str) -> tuple[list[dict], str, str]:
    """
    Return (daily_bars, fetch_status, history_source) for *sym*.

    fetch_status   : "ok" | "no_bars" | "fetch_failed"
    history_source : "fmp" | "tradier" | "unknown"

    Probe order:
      1. In-memory cache: fmp_hist:{sym}  (FMP bars, ~4h TTL, set by theme_rs)
      2. In-memory cache: tdier_hist:{sym}:400  (Tradier bars, 1h TTL)
      3. Live FMP /stable/historical-price-eod via theme_rs_service._fetch_fmp_daily_history
      4. Tradier daily history via theme_rs_service._fetch_tradier_daily_history

    Reuses theme_rs_service providers so rate-limiting and caching are shared.
    Returns ([], "fetch_failed", "unknown") on any unexpected exception.
    """
    s = sym.upper()
    try:
        from data.cache import cache as _cache
        fmp_cached = _cache.get(f"fmp_hist:{s}")
        if fmp_cached:
            return fmp_cached, "ok", "fmp"
        tdier_cached = _cache.get(f"tdier_hist:{s}:{_HIST_DAYS}")
        if tdier_cached:
            return tdier_cached, "ok", "tradier"
    except Exception:
        pass

    try:
        from services.theme_rs_service import (
            _fetch_fmp_daily_history,
            _fetch_tradier_daily_history,
        )
        bars = await _fetch_fmp_daily_history(s)
        if bars:
            return bars, "ok", "fmp"
        bars = await _fetch_tradier_daily_history(s, days=_HIST_DAYS)
        if bars:
            return bars, "ok", "tradier"
        return [], "no_bars", "unknown"
    except Exception as exc:
        print(f"[STAGE2_WL] bar fetch exception {sym}: {exc}")
        return [], "fetch_failed", "unknown"


def _persist_lkg() -> None:
    """Write _STAGE2_LKG atomically to disk."""
    try:
        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "symbol_count": len(_STAGE2_LKG),
            "results": dict(_STAGE2_LKG),
        }
        tmp = _LKG_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, indent=2))
        tmp.replace(_LKG_PATH)
    except Exception as exc:
        print(f"[STAGE2_WL] disk write error (non-fatal): {exc}")


# ── Public warmup ─────────────────────────────────────────────────────────────

async def warmup_stage2(
    tickers: list[str],
    *,
    force_nulls: bool = False,
) -> dict:
    """
    Fetch daily bars + compute Weinstein stage for every ticker in *tickers*.

    force_nulls=True  — bypasses the freshness gate for all entries where
                        label is None or score is None (recovery mode).
                        Entries with valid labels are still skipped if fresh.

    Skips symbols whose cached result is within its status-appropriate TTL
    (ok/no_bars → 20h; fetch_failed/legacy → 2h) unless force_nulls overrides.

    Runs _CONCURRENCY concurrent Tradier calls with a small sleep between batches.

    LKG Overwrite Guard:
      If ≥ _GUARD_MIN_SYMBOLS processed and new valid coverage < _GUARD_MIN_COVERAGE
      while existing LKG has ≥ _GUARD_MIN_COVERAGE valid labels → degraded mode.
      In degraded mode, existing valid labels are preserved; only new valid labels
      and fetch_failed status updates are written.

    Returns a detailed summary dict.
    """
    started_at = datetime.now(timezone.utc).isoformat()

    if not tickers:
        return {"status": "skipped", "reason": "no_tickers", "started_at": started_at}

    deduped = list(dict.fromkeys(s.strip().upper() for s in tickers if s.strip()))

    def _should_skip(sym: str) -> bool:
        if force_nulls and _is_null_entry(sym):
            return False
        return _is_fresh(sym)

    skip_count = sum(1 for s in deduped if _should_skip(s))
    to_process = [s for s in deduped if not _should_skip(s)]

    print(
        f"[STAGE2_WL] warmup starting{'[FORCE-NULL]' if force_nulls else ''}: "
        f"{len(deduped)} unique tickers, {skip_count} fresh (skip), "
        f"{len(to_process)} to compute"
    )

    if not to_process:
        return {
            "status": "all_fresh",
            "skipped": skip_count,
            "computed": 0,
            "started_at": started_at,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }

    # Snapshot existing LKG coverage for the overwrite guard
    prev_valid_count = sum(
        1 for v in _STAGE2_LKG.values() if v.get("label") is not None and v.get("score") is not None
    )
    prev_total = len(_STAGE2_LKG)
    prev_coverage = (prev_valid_count / prev_total) if prev_total > 0 else 0.0

    # SPY bars for RS calculation (optional)
    spy_weekly = None
    try:
        from services.stage_analysis import weekly_bars_from_daily
        spy_bars_raw, _, _spy_src = await _fetch_bars("SPY")
        if spy_bars_raw:
            spy_weekly = weekly_bars_from_daily(spy_bars_raw)
    except Exception as _spy_err:
        print(f"[STAGE2_WL] SPY bar fetch failed (non-fatal): {_spy_err}")

    from services.stage_analysis import weekly_bars_from_daily, analyze_symbol_stage

    sem = asyncio.Semaphore(_CONCURRENCY)
    now_ts = datetime.now(timezone.utc).isoformat()

    computed              = 0
    no_bars_count         = 0
    fetch_failed_count    = 0
    too_short             = 0
    errors                = 0
    preserved_valid_count = 0
    overwritten_valid_count = 0

    # Per-symbol new results — collected before bulk overwrite guard applies
    new_results: dict[str, dict] = {}

    async def _process_one(sym: str) -> None:
        nonlocal computed, no_bars_count, fetch_failed_count, too_short, errors
        async with sem:
            try:
                await asyncio.sleep(0.3)
                bars, fetch_status, hist_source = await _fetch_bars(sym)

                if fetch_status == "fetch_failed" or (not bars and fetch_status == "no_bars"):
                    if fetch_status == "fetch_failed":
                        fetch_failed_count += 1
                    else:
                        no_bars_count += 1
                    new_results[sym] = {
                        "score": None, "label": None, "reason": None,
                        "status": fetch_status,
                        "computed_at": now_ts,
                    }
                    return

                weekly = weekly_bars_from_daily(bars)
                if len(weekly) < 35:
                    too_short += 1
                    new_results[sym] = {
                        "score": None, "label": None, "reason": None,
                        "status": "no_bars",
                        "computed_at": now_ts,
                    }
                    return

                result = analyze_symbol_stage(
                    weekly_bars=weekly,
                    spy_weekly_bars=spy_weekly,
                    source="watchlist_stage2_warmup",
                )

                # ── Technical metrics from daily bars ─────────────────────────
                from services.stage_analysis import compute_technical_metrics
                tech = compute_technical_metrics(bars)

                # ── Provenance / diagnostics ──────────────────────────────────
                has_ohlcv = any(b.get("high") is not None for b in bars)
                bar_dates = sorted(
                    str(b.get("date", ""))[:10] for b in bars if b.get("date")
                )

                def _conf_reason(bc: int, ohlcv: bool, conf: str) -> str:
                    if bc < 175:
                        return "insufficient_weekly_bars_for_stage"
                    if bc < 200:
                        return "no_200d_tradier_path"
                    if bc < 252:
                        return "no_52w_tradier_path"
                    if bc >= 700:
                        return "fmp_multi_year_history"
                    if bc >= 350:
                        return "adequate_fmp_or_tradier_history"
                    return "tradier_400d_history"

                bar_count = len(bars)
                conf_reason = _conf_reason(bar_count, has_ohlcv, result.get("stage_confidence", "low"))

                computed += 1
                new_results[sym] = {
                    # ── Backward-compat fields (always present) ────────────────
                    "score":  result.get("stage_score"),
                    "label":  result.get("stage_label"),
                    "reason": result.get("stage_reason"),
                    # ── Stage internals ────────────────────────────────────────
                    "signals": result.get("stage_signals"),
                    "stage_confidence":        result.get("stage_confidence"),
                    "stage_confidence_reason": conf_reason,
                    # ── Technical metrics ──────────────────────────────────────
                    "technical_metrics":       tech,
                    "technical_state":         tech.get("technical_state"),
                    "technical_timing_score":  tech.get("technical_timing_score"),
                    # ── Provenance ────────────────────────────────────────────
                    "history_source":     hist_source,
                    "bars_count":         bar_count,
                    "history_start_date": bar_dates[0]  if bar_dates else None,
                    "history_end_date":   bar_dates[-1] if bar_dates else None,
                    "has_ohlcv":          has_ohlcv,
                    "has_200d":           bar_count >= 200,
                    "has_252d":           bar_count >= 252,
                    "status":             "ok",
                    "computed_at":        now_ts,
                }
            except Exception as exc:
                errors += 1
                fetch_failed_count += 1
                new_results[sym] = {
                    "score": None, "label": None, "reason": None,
                    "status": "fetch_failed",
                    "computed_at": now_ts,
                }
                print(f"[STAGE2_WL] error processing {sym}: {exc}")

    tasks = [_process_one(sym) for sym in to_process]
    await asyncio.gather(*tasks, return_exceptions=True)

    # ── Overwrite guard ───────────────────────────────────────────────────────
    new_valid_count = sum(
        1 for v in new_results.values() if v.get("label") is not None and v.get("score") is not None
    )
    new_total = len(new_results)
    new_coverage = (new_valid_count / new_total) if new_total > 0 else 0.0

    degraded = (
        new_total >= _GUARD_MIN_SYMBOLS
        and new_coverage < _GUARD_MIN_COVERAGE
        and prev_coverage >= _GUARD_MIN_COVERAGE
    )

    if degraded:
        print(
            f"[STAGE2_WL] DEGRADED RUN DETECTED: "
            f"new_coverage={new_coverage:.1%} ({new_valid_count}/{new_total}) "
            f"prev_coverage={prev_coverage:.1%} ({prev_valid_count}/{prev_total}) "
            f"— applying per-symbol guard (valid labels preserved)"
        )

    for sym, new_entry in new_results.items():
        new_has_valid = new_entry.get("label") is not None and new_entry.get("score") is not None
        existing = _STAGE2_LKG.get(sym)
        existing_has_valid = (
            existing is not None
            and existing.get("label") is not None
            and existing.get("score") is not None
        )

        if degraded and not new_has_valid and existing_has_valid:
            # Preserve the previous valid label; only update status so retry fires sooner
            preserved = dict(existing)
            preserved["status"] = "fetch_failed"
            _STAGE2_LKG[sym] = preserved
            preserved_valid_count += 1
        else:
            if new_has_valid and existing_has_valid:
                overwritten_valid_count += 1
            _STAGE2_LKG[sym] = new_entry

    _persist_lkg()

    non_null_total = sum(1 for v in _STAGE2_LKG.values() if v.get("score") is not None)
    finished_at = datetime.now(timezone.utc).isoformat()

    summary = {
        "status":                   "degraded" if degraded else "done",
        "force_nulls":              force_nulls,
        "total_required":           len(deduped),
        "skipped_fresh":            skip_count,
        "to_process":               len(to_process),
        "valid_stage_count":        computed,
        "null_stage_count":         no_bars_count + fetch_failed_count + too_short + errors,
        "fetch_failed_count":       fetch_failed_count,
        "no_bars_count":            no_bars_count + too_short,
        "error_count":              errors,
        "preserved_previous_valid": preserved_valid_count,
        "overwritten_valid_count":  overwritten_valid_count,
        "degraded_run":             degraded,
        "lkg_total_non_null":       non_null_total,
        "lkg_path":                 str(_LKG_PATH),
        "started_at":               started_at,
        "finished_at":              finished_at,
    }
    print(f"[STAGE2_WL] warmup done: {summary}")
    return summary


async def force_warmup_stage2_nulls() -> dict:
    """
    Recovery entrypoint — bypasses freshness gate for all null/failed entries.

    Loads all watchlist tickers from Neon, then calls warmup_stage2 with
    force_nulls=True so entries with label=None or score=None are recomputed
    regardless of when they were last attempted.

    Entries that already have valid labels and are within their TTL are skipped.
    """
    tickers: list[str] = []
    try:
        from data.pg_storage import watchlist_list, watchlist_read
        wl_metas = watchlist_list()
        for meta in wl_metas:
            wl_id = meta.get("id")
            if not wl_id:
                continue
            try:
                store = watchlist_read(wl_id)
                if store:
                    tickers.extend(store.get("tickers") or [])
            except Exception as _re:
                print(f"[STAGE2_WL] read error wl={wl_id}: {_re}")
    except Exception as exc:
        print(f"[STAGE2_WL] watchlist_list error: {exc}")

    if not tickers:
        return {"status": "skipped", "reason": "no_tickers_in_any_watchlist"}

    return await warmup_stage2(tickers, force_nulls=True)


async def warmup_stage2_all_watchlists(startup_delay_s: float = 0.0) -> dict:
    """
    Load all watchlist tickers from Neon and run warmup_stage2 on the union.
    Called by the scheduler and at startup.

    At startup we wait *startup_delay_s* seconds before fetching any bars so
    the main screener loop (which saturates the Tradier rate-limiter at boot)
    has time to settle before we add more API calls.
    """
    if startup_delay_s > 0:
        print(f"[STAGE2_WL] startup delay {startup_delay_s:.0f}s — waiting for other loops to settle")
        await asyncio.sleep(startup_delay_s)

    tickers: list[str] = []
    try:
        from data.pg_storage import watchlist_list, watchlist_read
        wl_metas = watchlist_list()
        for meta in wl_metas:
            wl_id = meta.get("id")
            if not wl_id:
                continue
            try:
                store = watchlist_read(wl_id)
                if store:
                    tickers.extend(store.get("tickers") or [])
            except Exception as _re:
                print(f"[STAGE2_WL] read error wl={wl_id}: {_re}")
    except Exception as exc:
        print(f"[STAGE2_WL] watchlist_list error: {exc}")

    if not tickers:
        return {"status": "skipped", "reason": "no_tickers_in_any_watchlist"}

    return await warmup_stage2(tickers)
