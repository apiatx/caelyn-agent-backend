"""
Watchlist RSS Sweeper — continuous near-real-time RSS article archive maintenance.

Registered exactly once via asyncio.create_task(rss_sweeper_loop()) in main.py lifespan.
Runs a full sweep of all active Watchlist tickers approximately every 120 seconds.

Architecture:
  - Yahoo Finance RSS and Google News RSS fetched concurrently per ticker
  - Bounded ticker concurrency: asyncio.Semaphore(_SWEEP_SEM_SIZE=8)
  - Independent archive-write concurrency: asyncio.Semaphore(_DB_WRITE_SEM_SIZE=4)
  - Results merged and cross-feed deduplicated using _cluster_key from news_major_service
  - diff-aware upsert_with_cache: new → INSERT, provider changed → UPDATE, unchanged → skip
  - Rows older than 72 hours pruned every _PRUNE_EVERY_N sweeps

No LLM calls. No FMP calls. fmp_requests_for_activity is always 0.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from datetime import datetime
from typing import Any

import httpx

from services.news_major_service import _cluster_key as _ck
from services.watchlist_service import (
    _parse_rss_xml,
    list_watchlists,
    load_watchlist,
)

# ── Sweeper configuration ─────────────────────────────────────────────────────

_TARGET_INTERVAL_S       = 120    # target full-sweep interval seconds
_MIN_POST_SWEEP_IDLE_S   = 60     # guaranteed breathing room after a full sweep
_SWEEP_SEM_SIZE          = 8      # max concurrent ticker workers
_DB_WRITE_SEM_SIZE       = 4      # max concurrent synchronous archive writes
_PRUNE_EVERY_N           = 10     # prune old rows every N sweeps
_FETCH_TIMEOUT_S         = 8.0    # per-provider request timeout
_RETAIN_HOURS            = 120    # archive retention: 96h comparison + 24h buffer
_STARTUP_DELAY_S         = 120    # delay before the default startup pass

_USER_AGENT = "Mozilla/5.0 (compatible; CaelynAI/1.0)"


def _rss_provider_symbol(ticker: str) -> str:
    """
    Return the provider query symbol for a canonical Watchlist ticker.

    Exchange-prefixed tickers (e.g. AIM:ENSI, ETR:AIXA, TSX:MAL) must be
    queried using only the bare symbol portion so Yahoo Finance and Google News
    can resolve them.  The canonical ticker (AIM:ENSI) is preserved everywhere
    else: archive rows, ticker_activity keys, Watchlist scoping.

    Examples:
        "AIM:ENSI"    → "ENSI"
        "ETR:AIXA"    → "AIXA"
        "NASDAQ:NVDA" → "NVDA"
        "NVDA"        → "NVDA"
    """
    t = ticker.strip().upper()
    return t.rsplit(":", 1)[1].strip() if ":" in t else t

# ── Sweeper diagnostics ────────────────────────────────────────────────────────

_SWEEPER_DIAG: dict[str, Any] = {
    "loop_registered":                         False,
    "sweep_id":                                None,
    "sweep_started_at":                        None,
    "sweep_completed_at":                      None,
    "sweep_duration_ms":                       None,
    "target_interval_seconds":                 _TARGET_INTERVAL_S,
    "total_watchlists":                        0,
    "total_unique_tickers":                    0,
    "tickers_scanned":                         0,
    "tickers_successful":                      0,
    "tickers_partial":                         0,
    "tickers_failed":                          0,
    # Per-provider HTTP outcome counters
    "yahoo_requests":                          0,
    "yahoo_success":                           0,
    "yahoo_failures":                          0,
    "yahoo_429":                               0,
    "yahoo_403":                               0,
    "yahoo_timeout":                           0,
    "yahoo_5xx":                               0,
    "google_requests":                         0,
    "google_success":                          0,
    "google_failures":                         0,
    "google_429":                              0,
    "google_403":                              0,
    "google_timeout":                          0,
    "google_5xx":                              0,
    # Article volume
    "rss_articles_raw_yahoo":                  0,
    "rss_articles_raw_google":                 0,
    "merged_articles_before_dedupe":           0,
    "cross_feed_duplicates_removed":           0,
    # Granular DB write counters (replaces ambiguous "upserted")
    "articles_observed":                       0,
    "db_insert_attempts":                      0,
    "db_update_attempts":                      0,
    "new_rows_inserted":                       0,
    "provider_sets_updated":                   0,
    "unchanged_existing_articles_skipped":     0,
    # Prune
    "archive_rows_before_prune":               0,
    "archive_rows_pruned":                     0,
    "archive_rows_after_prune":                0,
    # Timing
    "last_full_sweep_at":                      None,
    "next_sweep_target_at":                    None,
    "collector_started_at":                    None,
    "sweep_count":                             0,
    # Hard constraint
    "fmp_requests_for_activity":               0,
}

_SWEEP_LOCK = asyncio.Lock()


def get_sweeper_meta(tickers: list[str] | None = None) -> dict[str, Any]:
    """Return the rss_activity_meta block for the /news response."""
    d = _SWEEPER_DIAG
    return {
        "providers":                ["yahoo_rss", "google_news_rss"],
        "window_hours":             48,
        "comparison_window_hours":  48,
        "retention_hours":          _RETAIN_HOURS,
        "collector_started_at":     d["collector_started_at"],
        "last_full_sweep_at":       d["last_full_sweep_at"],
        "sweep_in_progress":        _SWEEP_LOCK.locked(),
        "current_sweep_started_at": d["sweep_started_at"] if _SWEEP_LOCK.locked() else None,
        "last_sweep_duration_ms":   d["sweep_duration_ms"],
        "ticker_count":             len(tickers) if tickers else d["total_unique_tickers"],
    }


# ── Per-provider fetch helpers ────────────────────────────────────────────────
# Return (articles, error_type) — never raise.
# error_type: None=success, "429", "403", "5xx", "timeout", "error"

async def _sweep_yahoo_rss(
    ticker: str, client: httpx.AsyncClient
) -> tuple[list[dict], str | None]:
    provider_sym = _rss_provider_symbol(ticker)   # "AIM:ENSI" → "ENSI"
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={provider_sym}&region=US&lang=en-US"
    try:
        resp = await client.get(url, timeout=_FETCH_TIMEOUT_S)
        sc = resp.status_code
        if sc == 200:
            arts = _parse_rss_xml(resp.text, "Yahoo Finance")
            for a in arts:
                a["rss_provider"] = "yahoo"
            return arts, None
        if sc == 429:
            return [], "429"
        if sc == 403:
            return [], "403"
        if sc >= 500:
            return [], "5xx"
        return [], f"http_{sc}"
    except httpx.TimeoutException:
        return [], "timeout"
    except Exception:
        return [], "error"


async def _sweep_google_rss(
    ticker: str, client: httpx.AsyncClient
) -> tuple[list[dict], str | None]:
    provider_sym = _rss_provider_symbol(ticker)   # "AIM:ENSI" → "ENSI"
    url = f"https://news.google.com/rss/search?q={provider_sym}+stock+news&hl=en-US&gl=US&ceid=US:en"
    try:
        resp = await client.get(url, timeout=_FETCH_TIMEOUT_S)
        sc = resp.status_code
        if sc == 200:
            arts = _parse_rss_xml(resp.text, "Google News")
            for a in arts:
                a["rss_provider"] = "google"
            return arts, None
        if sc == 429:
            return [], "429"
        if sc == 403:
            return [], "403"
        if sc >= 500:
            return [], "5xx"
        return [], f"http_{sc}"
    except httpx.TimeoutException:
        return [], "timeout"
    except Exception:
        return [], "error"


def _merge_and_dedupe(
    yahoo_arts: list[dict], google_arts: list[dict]
) -> list[dict]:
    """
    Merge Yahoo + Google articles, deduplicate by cluster_key, union rss_providers.
    Returns the deduplicated list with rss_providers and _article_key set on each item.
    """
    merged: list[dict] = []
    seen_key_idx: dict[str, int] = {}

    for arts, provider in [(yahoo_arts, "yahoo"), (google_arts, "google")]:
        for a in arts:
            ck = _ck(a.get("title") or "", a.get("url") or "")
            if ck in seen_key_idx:
                existing  = merged[seen_key_idx[ck]]
                providers = list(existing.get("rss_providers") or [])
                if provider not in providers:
                    providers.append(provider)
                existing["rss_providers"] = providers
            else:
                copy = dict(a)
                copy["rss_providers"] = [provider]
                copy["_article_key"]  = ck
                seen_key_idx[ck] = len(merged)
                merged.append(copy)

    return merged


# ── Per-ticker sweep worker ───────────────────────────────────────────────────

async def _sweep_ticker(
    ticker: str,
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    db_write_sem: asyncio.Semaphore,
    sweep_diag: dict,
) -> dict:
    """
    Fetch Yahoo + Google concurrently, merge, dedup, write via cache-aware upsert.
    Updates sweep_diag counters (per-sweep local dict merged into _SWEEPER_DIAG at end).
    """
    async with sem:
        sweep_diag["yahoo_requests"]  += 1
        sweep_diag["google_requests"] += 1

        (yahoo_arts, yahoo_err), (google_arts, google_err) = await asyncio.gather(
            _sweep_yahoo_rss(ticker, client),
            _sweep_google_rss(ticker, client),
        )

        # Track Yahoo outcome
        if yahoo_err is None:
            sweep_diag["yahoo_success"]          += 1
            sweep_diag["rss_articles_raw_yahoo"] += len(yahoo_arts)
            yahoo_ok = True
        else:
            sweep_diag["yahoo_failures"] += 1
            err_key = f"yahoo_{yahoo_err}" if yahoo_err in ("429", "403", "timeout", "5xx") else "yahoo_failures"
            if err_key in sweep_diag:
                sweep_diag[err_key] += 1
            yahoo_ok = False
            yahoo_arts = []

        # Track Google outcome
        if google_err is None:
            sweep_diag["google_success"]          += 1
            sweep_diag["rss_articles_raw_google"] += len(google_arts)
            google_ok = True
        else:
            sweep_diag["google_failures"] += 1
            err_key = f"google_{google_err}" if google_err in ("429", "403", "timeout", "5xx") else "google_failures"
            if err_key in sweep_diag:
                sweep_diag[err_key] += 1
            google_ok = False
            google_arts = []

        raw_count = len(yahoo_arts) + len(google_arts)
        sweep_diag["merged_articles_before_dedupe"] += raw_count

        merged       = _merge_and_dedupe(yahoo_arts, google_arts)
        dups_removed = raw_count - len(merged)
        sweep_diag["cross_feed_duplicates_removed"] += dups_removed

        # Cache-aware write: new→INSERT, changed providers→UPDATE, unchanged→skip
        write_stats: dict = {}
        if merged:
            try:
                from data.rss_article_archive import upsert_with_cache
                loop = asyncio.get_event_loop()
                async with db_write_sem:
                    write_stats = await loop.run_in_executor(
                        None, upsert_with_cache, ticker, merged
                    )
                sweep_diag["articles_observed"]                    += write_stats.get("articles_observed", 0)
                sweep_diag["db_insert_attempts"]                   += write_stats.get("db_insert_attempts", 0)
                sweep_diag["db_update_attempts"]                   += write_stats.get("db_update_attempts", 0)
                sweep_diag["new_rows_inserted"]                    += write_stats.get("new_rows_inserted", 0)
                sweep_diag["provider_sets_updated"]                += write_stats.get("provider_sets_updated", 0)
                sweep_diag["unchanged_existing_articles_skipped"]  += write_stats.get("unchanged_existing_articles_skipped", 0)
            except Exception as e:
                print(f"[RSS_SWEEPER] upsert_with_cache error ticker={ticker}: {e}")

        sweep_diag["tickers_scanned"] += 1
        if yahoo_ok and google_ok:
            sweep_diag["tickers_successful"] += 1
            status = "ok"
        elif yahoo_ok or google_ok:
            sweep_diag["tickers_partial"] += 1
            status = "partial"
        else:
            sweep_diag["tickers_failed"] += 1
            status = "failed"

        return {
            "ticker":       ticker,
            "status":       status,
            "yahoo":        len(yahoo_arts),
            "google":       len(google_arts),
            "merged":       len(merged),
            "dups_removed": dups_removed,
            **write_stats,
        }


# ── Full sweep ────────────────────────────────────────────────────────────────

async def run_rss_sweep(*, await_hyperscaler_rebuild: bool = False) -> dict:
    """
    One full sweep of all active Watchlist RSS feeds.
    Guarded by _SWEEP_LOCK — never overlaps with itself.
    By default, trigger the hyperscaler rebuild in the background as before.
    Startup callers may await that rebuild for deterministic completion.
    Returns the per-sweep diagnostic dict.
    """
    sweep_id    = str(uuid.uuid4())[:8]
    sweep_start = time.time()
    sweep_ts    = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    _SWEEPER_DIAG.update({
        "sweep_id":           sweep_id,
        "sweep_started_at":   sweep_ts,
        "sweep_completed_at": None,
    })

    # Per-sweep counters (reset each sweep, merged into _SWEEPER_DIAG at end)
    sweep_diag: dict[str, Any] = {
        "tickers_scanned":                         0,
        "tickers_successful":                      0,
        "tickers_partial":                         0,
        "tickers_failed":                          0,
        "yahoo_requests":                          0,
        "yahoo_success":                           0,
        "yahoo_failures":                          0,
        "yahoo_429":                               0,
        "yahoo_403":                               0,
        "yahoo_timeout":                           0,
        "yahoo_5xx":                               0,
        "google_requests":                         0,
        "google_success":                          0,
        "google_failures":                         0,
        "google_429":                              0,
        "google_403":                              0,
        "google_timeout":                          0,
        "google_5xx":                              0,
        "rss_articles_raw_yahoo":                  0,
        "rss_articles_raw_google":                 0,
        "merged_articles_before_dedupe":           0,
        "cross_feed_duplicates_removed":           0,
        "articles_observed":                       0,
        "db_insert_attempts":                      0,
        "db_update_attempts":                      0,
        "new_rows_inserted":                       0,
        "provider_sets_updated":                   0,
        "unchanged_existing_articles_skipped":     0,
    }

    # ── 1. Collect active watchlist tickers ──────────────────────────────────
    all_tickers: set[str] = set()
    watchlists: list = []
    try:
        watchlists = await asyncio.to_thread(list_watchlists) or []
    except Exception as e:
        print(f"[RSS_SWEEPER] list_watchlists error: {e}")

    for wl_meta in watchlists:
        try:
            wl = await asyncio.to_thread(load_watchlist, wl_meta.get("id"))
            if wl:
                for t in (wl.get("tickers") or []):
                    if t and isinstance(t, str):
                        all_tickers.add(t.upper())
        except Exception as e:
            print(f"[RSS_SWEEPER] load_watchlist error wl={wl_meta.get('id')}: {e}")

    tickers = sorted(all_tickers)
    _SWEEPER_DIAG["total_watchlists"]     = len(watchlists)
    _SWEEPER_DIAG["total_unique_tickers"] = len(tickers)

    if not tickers:
        elapsed_ms   = round((time.time() - sweep_start) * 1000)
        completed_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        _SWEEPER_DIAG.update({
            "sweep_completed_at": completed_ts,
            "sweep_duration_ms":  elapsed_ms,
            "last_full_sweep_at": completed_ts,
            "sweep_count":        _SWEEPER_DIAG["sweep_count"] + 1,
        })
        print(f"[RSS_SWEEPER] sweep_id={sweep_id} no tickers — skipping")
        return sweep_diag

    # ── 2. Bounded concurrent RSS sweep ─────────────────────────────────────
    sem = asyncio.Semaphore(_SWEEP_SEM_SIZE)
    db_write_sem = asyncio.Semaphore(_DB_WRITE_SEM_SIZE)

    async with httpx.AsyncClient(
        follow_redirects=True,
        headers={"User-Agent": _USER_AGENT},
        timeout=_FETCH_TIMEOUT_S + 2.0,
    ) as client:
        tasks = [
            _sweep_ticker(t, client, sem, db_write_sem, sweep_diag)
            for t in tickers
        ]
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)

    results: list[dict] = [r for r in raw_results if isinstance(r, dict)]

    # ── 3. Prune >72h rows (every _PRUNE_EVERY_N sweeps) ────────────────────
    sweep_count  = _SWEEPER_DIAG["sweep_count"] + 1
    rows_before  = rows_pruned = rows_after = 0
    if sweep_count % _PRUNE_EVERY_N == 0:
        try:
            from data.rss_article_archive import count_all_rows, prune_old_rows
            loop = asyncio.get_event_loop()
            rows_before = await loop.run_in_executor(None, count_all_rows)
            rows_pruned = await loop.run_in_executor(None, prune_old_rows, _RETAIN_HOURS)
            # prune_old_rows re-warms the in-memory cache after deleting old rows
            rows_after  = rows_before - rows_pruned
            print(
                f"[RSS_SWEEPER] prune sweep_id={sweep_id} "
                f"before={rows_before} pruned={rows_pruned} after={rows_after}"
            )
        except Exception as e:
            print(f"[RSS_SWEEPER] prune error: {e}")

    # ── 4. Update global diagnostics ─────────────────────────────────────────
    elapsed_ms   = round((time.time() - sweep_start) * 1000)
    completed_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    _SWEEPER_DIAG.update({
        "sweep_completed_at":                      completed_ts,
        "sweep_duration_ms":                       elapsed_ms,
        "last_full_sweep_at":                      completed_ts,
        "sweep_count":                             sweep_count,
        "archive_rows_before_prune":               rows_before,
        "archive_rows_pruned":                     rows_pruned,
        "archive_rows_after_prune":                rows_after,
        **sweep_diag,
    })

    # ── 5. Log top-20 tickers by 48h activity and positive delta ─────────────
    try:
        from data.rss_article_archive import query_ticker_activity
        loop = asyncio.get_event_loop()
        act  = await loop.run_in_executor(None, query_ticker_activity, tickers)
        top_48h = sorted(act.items(), key=lambda kv: -kv[1]["articles_48h"])[:20]
        top_delta = sorted(
            [
                (t, d) for t, d in act.items()
                if d["articles_48h"] > d["previous_articles_48h"]
            ],
            key=lambda kv: -(kv[1]["articles_48h"] - kv[1]["previous_articles_48h"]),
        )[:20]
        if top_48h:
            print(
                f"[RSS_SWEEPER] sweep_id={sweep_id} top-48h: "
                + " ".join(f"{t}={d['articles_48h']}" for t, d in top_48h[:10])
            )
        if top_delta:
            print(
                f"[RSS_SWEEPER] sweep_id={sweep_id} top-delta: "
                + " ".join(
                    f"{t}=+{d['articles_48h']-d['previous_articles_48h']}"
                    for t, d in top_delta[:10]
                )
            )
    except Exception:
        pass

    # ── 6. Notify hyperscaler cache to rebuild with latest archive data ──────
    # After each successful sweep new articles are in Neon; the hyperscaler
    # cache needs to incorporate them.  Mark it stale so the next GET /news
    # fires a background rebuild, OR trigger one immediately if no request is
    # imminent.  Uses lazy import to avoid circular dependencies.
    try:
        from services import watchlist_router as _wr
        if hasattr(_wr, "_HYP_CACHE") and not getattr(_wr, "_HYP_CACHE_BUILDING", False):
            rebuild = _wr._rebuild_hyperscaler_cache(tickers)
            if await_hyperscaler_rebuild:
                await rebuild
            else:
                asyncio.ensure_future(rebuild)
    except Exception:
        pass

    print(
        f"[RSS_SWEEPER] sweep_id={sweep_id} "
        f"tickers={len(tickers)} "
        f"ok={sweep_diag['tickers_successful']} "
        f"partial={sweep_diag['tickers_partial']} "
        f"failed={sweep_diag['tickers_failed']} "
        f"y_ok={sweep_diag['yahoo_success']} y_429={sweep_diag['yahoo_429']} "
        f"y_403={sweep_diag['yahoo_403']} y_to={sweep_diag['yahoo_timeout']} "
        f"y_5xx={sweep_diag['yahoo_5xx']} "
        f"g_ok={sweep_diag['google_success']} g_429={sweep_diag['google_429']} "
        f"g_403={sweep_diag['google_403']} g_to={sweep_diag['google_timeout']} "
        f"g_5xx={sweep_diag['google_5xx']} "
        f"raw_y={sweep_diag['rss_articles_raw_yahoo']} "
        f"raw_g={sweep_diag['rss_articles_raw_google']} "
        f"merged={sweep_diag['merged_articles_before_dedupe']} "
        f"dups={sweep_diag['cross_feed_duplicates_removed']} "
        f"observed={sweep_diag['articles_observed']} "
        f"ins={sweep_diag['new_rows_inserted']} "
        f"upd={sweep_diag['provider_sets_updated']} "
        f"skip={sweep_diag['unchanged_existing_articles_skipped']} "
        f"fmp=0 duration_ms={elapsed_ms}"
    )

    return sweep_diag


# ── Main loop (registered once in main.py lifespan) ──────────────────────────
async def rss_sweeper_loop(*, skip_initial: bool = False) -> None:
    """
    Continuous RSS archive sweep loop.
    Registered via asyncio.create_task(rss_sweeper_loop()) in main.py lifespan.
    Never starts a second sweep while one is running (_SWEEP_LOCK).
    Target interval: ~120 seconds.

    When skip_initial=True, process restart does not trigger a full-universe
    catch-up; recurring work waits the normal target interval before first use.
    """
    _SWEEPER_DIAG["collector_started_at"] = time.time()
    _SWEEPER_DIAG["loop_registered"]      = True

    if not skip_initial:
        await asyncio.sleep(_STARTUP_DELAY_S)   # let PG init settle
        try:
            from data.rss_article_archive import warm_seen_cache
            n = await asyncio.to_thread(warm_seen_cache)
            print(f"[RSS_SWEEPER] seen cache warmed: {n} rows loaded from Neon")
        except Exception as e:
            print(f"[RSS_SWEEPER] seen cache warm error (non-fatal): {e}")
        startup_sweep_start = time.time()
        async with _SWEEP_LOCK:
            try:
                await run_rss_sweep()
            except Exception as exc:
                print(f"[RSS_SWEEPER] unhandled sweep error: {exc}")
        startup_elapsed = time.time() - startup_sweep_start
        initial_wait = max(
            _MIN_POST_SWEEP_IDLE_S,
            _TARGET_INTERVAL_S - startup_elapsed,
        )
    else:
        initial_wait = _TARGET_INTERVAL_S

    next_ts = time.time() + initial_wait
    _SWEEPER_DIAG["next_sweep_target_at"] = datetime.utcfromtimestamp(next_ts).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    await asyncio.sleep(initial_wait)

    while True:
        sweep_start = time.time()

        if _SWEEP_LOCK.locked():
            print("[RSS_SWEEPER] prior sweep still running — skipping cycle")
            await asyncio.sleep(30)
            continue

        async with _SWEEP_LOCK:
            try:
                await run_rss_sweep()
            except Exception as exc:
                print(f"[RSS_SWEEPER] unhandled sweep error: {exc}")

        elapsed = time.time() - sweep_start
        wait = max(_MIN_POST_SWEEP_IDLE_S, _TARGET_INTERVAL_S - elapsed)
        next_ts = time.time() + wait
        _SWEEPER_DIAG["next_sweep_target_at"] = datetime.utcfromtimestamp(next_ts).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

        if wait > 0:
            await asyncio.sleep(wait)
