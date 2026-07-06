"""
Watchlist RSS Sweeper — continuous near-real-time RSS article archive maintenance.

Registered exactly once via asyncio.create_task(rss_sweeper_loop()) in main.py lifespan.
Runs a full sweep of all active Watchlist tickers approximately every 120 seconds.

Architecture:
  - Yahoo Finance RSS and Google News RSS fetched concurrently per ticker
  - Bounded concurrency: asyncio.Semaphore(_SWEEP_SEM_SIZE=15)
  - Results merged and cross-feed deduplicated using the same _cluster_key function
    as news_major_service (shared canonical dedup logic — not a second normalizer)
  - Unique (ticker, article_key) rows upserted to Neon via rss_article_archive
  - Rows older than 72 hours pruned every _PRUNE_EVERY_N sweeps
  - FMP fallback articles are NEVER written to the archive

No LLM calls, no FMP calls, no Perplexity calls. fmp_requests_for_activity is always 0.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import httpx

from services.news_major_service import _cluster_key as _ck
from services.watchlist_service import (
    _parse_rss_xml,
    list_watchlists,
    load_watchlist,
)

# ── Sweeper configuration ─────────────────────────────────────────────────────

_TARGET_INTERVAL_S   = 120    # target full-sweep interval seconds
_SWEEP_SEM_SIZE      = 15     # max concurrent ticker workers
_PRUNE_EVERY_N       = 10     # prune old rows every N sweeps
_FETCH_TIMEOUT_S     = 8.0    # per-provider request timeout
_RETAIN_HOURS        = 72     # archive retention window
_STARTUP_DELAY_S     = 30     # delay before first sweep

_USER_AGENT = "Mozilla/5.0 (compatible; CaelynAI/1.0)"

# ── Sweeper diagnostics (in-memory, served by /news response) ─────────────────

_SWEEPER_DIAG: dict[str, Any] = {
    "loop_registered":                     False,
    "sweep_id":                            None,
    "sweep_started_at":                    None,
    "sweep_completed_at":                  None,
    "sweep_duration_ms":                   None,
    "target_interval_seconds":             _TARGET_INTERVAL_S,
    "total_watchlists":                    0,
    "total_unique_tickers":                0,
    "tickers_scanned":                     0,
    "tickers_successful":                  0,
    "tickers_partial":                     0,
    "tickers_failed":                      0,
    "yahoo_requests":                      0,
    "yahoo_success":                       0,
    "yahoo_failures":                      0,
    "google_requests":                     0,
    "google_success":                      0,
    "google_failures":                     0,
    "fmp_requests_for_activity":           0,   # always 0 — constraint enforced here
    "rss_articles_raw_yahoo":              0,
    "rss_articles_raw_google":             0,
    "merged_articles_before_dedupe":       0,
    "cross_feed_duplicates_removed":       0,
    "unique_article_associations_upserted": 0,
    "archive_rows_before_prune":           0,
    "archive_rows_pruned":                 0,
    "archive_rows_after_prune":            0,
    "last_full_sweep_at":                  None,
    "next_sweep_target_at":                None,
    "collector_started_at":                None,
    "sweep_count":                         0,
}

_SWEEP_LOCK = asyncio.Lock()


def get_sweeper_meta(tickers: list[str] | None = None) -> dict[str, Any]:
    """Return the rss_activity_meta block for the /news response."""
    d = _SWEEPER_DIAG
    return {
        "providers":                ["yahoo_rss", "google_news_rss"],
        "window_hours":             24,
        "comparison_window_hours":  24,
        "retention_hours":          _RETAIN_HOURS,
        "collector_started_at":     d["collector_started_at"],
        "last_full_sweep_at":       d["last_full_sweep_at"],
        "sweep_in_progress":        _SWEEP_LOCK.locked(),
        "current_sweep_started_at": d["sweep_started_at"] if _SWEEP_LOCK.locked() else None,
        "last_sweep_duration_ms":   d["sweep_duration_ms"],
        "ticker_count":             len(tickers) if tickers else d["total_unique_tickers"],
    }


# ── Per-provider fetch helpers (archive path — no truncation, no cache) ───────
# These are pure fetch functions: return a list of articles or raise on error.
# Diagnostics are tracked only in _sweep_ticker, never here.

async def _sweep_yahoo_rss(ticker: str, client: httpx.AsyncClient) -> list[dict]:
    """Fetch Yahoo Finance RSS for ticker. Tags articles rss_provider=yahoo."""
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
    resp = await client.get(url, timeout=_FETCH_TIMEOUT_S)
    if resp.status_code == 200:
        arts = _parse_rss_xml(resp.text, "Yahoo Finance")
        for a in arts:
            a["rss_provider"]  = "yahoo"
        return arts
    return []


async def _sweep_google_rss(ticker: str, client: httpx.AsyncClient) -> list[dict]:
    """Fetch Google News RSS for ticker. Tags articles rss_provider=google."""
    url = f"https://news.google.com/rss/search?q={ticker}+stock+news&hl=en-US&gl=US&ceid=US:en"
    resp = await client.get(url, timeout=_FETCH_TIMEOUT_S)
    if resp.status_code == 200:
        arts = _parse_rss_xml(resp.text, "Google News")
        for a in arts:
            a["rss_provider"]  = "google"
        return arts
    return []


def _merge_and_dedupe(yahoo_arts: list[dict], google_arts: list[dict]) -> list[dict]:
    """
    Merge Yahoo + Google articles, deduplicate by cluster_key, union rss_providers.

    Returns the deduplicated list with:
      rss_providers: list[str]  — which providers surfaced this article
      _article_key:  str        — canonical cluster key (DB primary key component)
    """
    merged: list[dict] = []
    seen_key_idx: dict[str, int] = {}   # cluster_key → index in merged

    for arts, provider in [(yahoo_arts, "yahoo"), (google_arts, "google")]:
        for a in arts:
            ck = _ck(a.get("title") or "", a.get("url") or "")
            if ck in seen_key_idx:
                # Duplicate article — union provider, don't add a second row
                existing = merged[seen_key_idx[ck]]
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
    sweep_diag: dict,
) -> dict:
    """
    Fetch Yahoo + Google concurrently, merge, dedup, upsert to archive.
    Increments sweep_diag counters (per-sweep local dict, merged into
    _SWEEPER_DIAG at the end of run_rss_sweep).
    Returns per-ticker result summary.
    """
    async with sem:
        sweep_diag["yahoo_requests"]  += 1
        sweep_diag["google_requests"] += 1

        yahoo_ok = google_ok = False
        yahoo_arts: list[dict] = []
        google_arts: list[dict] = []

        # Start both RSS fetches concurrently then gather results
        yahoo_result, google_result = await asyncio.gather(
            _sweep_yahoo_rss(ticker, client),
            _sweep_google_rss(ticker, client),
            return_exceptions=True,
        )

        if isinstance(yahoo_result, Exception):
            sweep_diag["yahoo_failures"] += 1
        else:
            yahoo_arts = yahoo_result or []
            yahoo_ok   = True
            sweep_diag["yahoo_success"]           += 1
            sweep_diag["rss_articles_raw_yahoo"]  += len(yahoo_arts)

        if isinstance(google_result, Exception):
            sweep_diag["google_failures"] += 1
        else:
            google_arts = google_result or []
            google_ok   = True
            sweep_diag["google_success"]           += 1
            sweep_diag["rss_articles_raw_google"]  += len(google_arts)

        raw_count = len(yahoo_arts) + len(google_arts)
        sweep_diag["merged_articles_before_dedupe"] += raw_count

        merged = _merge_and_dedupe(yahoo_arts, google_arts)
        dups_removed = raw_count - len(merged)
        sweep_diag["cross_feed_duplicates_removed"] += dups_removed

        # Upsert to Neon archive via executor (blocking sync call)
        upserted = 0
        if merged:
            try:
                from data.rss_article_archive import upsert_article_associations
                loop = asyncio.get_event_loop()
                upserted = await loop.run_in_executor(
                    None, upsert_article_associations, ticker, merged
                )
                sweep_diag["unique_article_associations_upserted"] += upserted
            except Exception as e:
                print(f"[RSS_SWEEPER] upsert error ticker={ticker}: {e}")

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
            "upserted":     upserted,
        }


# ── Full sweep ────────────────────────────────────────────────────────────────

async def run_rss_sweep() -> dict:
    """
    One full sweep of all active Watchlist RSS feeds.
    Guarded by _SWEEP_LOCK — never overlaps with itself.
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

    # Per-sweep accumulator — reset each sweep, merged into _SWEEPER_DIAG at end
    sweep_diag: dict[str, Any] = {
        "tickers_scanned":                      0,
        "tickers_successful":                   0,
        "tickers_partial":                      0,
        "tickers_failed":                       0,
        "yahoo_requests":                       0,
        "yahoo_success":                        0,
        "yahoo_failures":                       0,
        "google_requests":                      0,
        "google_success":                       0,
        "google_failures":                      0,
        "rss_articles_raw_yahoo":               0,
        "rss_articles_raw_google":              0,
        "merged_articles_before_dedupe":        0,
        "cross_feed_duplicates_removed":        0,
        "unique_article_associations_upserted": 0,
    }

    # ── 1. Collect active watchlist tickers ──────────────────────────────────
    all_tickers: set[str] = set()
    watchlists: list = []
    try:
        watchlists = list_watchlists() or []
    except Exception as e:
        print(f"[RSS_SWEEPER] list_watchlists error: {e}")

    for wl_meta in watchlists:
        try:
            wl = load_watchlist(wl_meta.get("id"))
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
        elapsed_ms = round((time.time() - sweep_start) * 1000)
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

    async with httpx.AsyncClient(
        follow_redirects=True,
        headers={"User-Agent": _USER_AGENT},
        timeout=_FETCH_TIMEOUT_S + 2.0,
    ) as client:
        tasks = [
            _sweep_ticker(t, client, sem, sweep_diag)
            for t in tickers
        ]
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)

    results: list[dict] = [r for r in raw_results if isinstance(r, dict)]

    # ── 3. Prune >72h rows (every _PRUNE_EVERY_N sweeps) ────────────────────
    sweep_count = _SWEEPER_DIAG["sweep_count"] + 1
    rows_before = rows_pruned = rows_after = 0
    if sweep_count % _PRUNE_EVERY_N == 0:
        try:
            from data.rss_article_archive import count_all_rows, prune_old_rows
            loop = asyncio.get_event_loop()
            rows_before = await loop.run_in_executor(None, count_all_rows)
            rows_pruned = await loop.run_in_executor(None, prune_old_rows, _RETAIN_HOURS)
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
        "sweep_completed_at":            completed_ts,
        "sweep_duration_ms":             elapsed_ms,
        "last_full_sweep_at":            completed_ts,
        "sweep_count":                   sweep_count,
        "archive_rows_before_prune":     rows_before,
        "archive_rows_pruned":           rows_pruned,
        "archive_rows_after_prune":      rows_after,
        **sweep_diag,
    })

    # ── 5. Log top-20 tickers by current 24h activity and by positive delta ──
    try:
        from data.rss_article_archive import query_ticker_activity
        loop = asyncio.get_event_loop()
        act = await loop.run_in_executor(None, query_ticker_activity, tickers)
        top_24h = sorted(act.items(), key=lambda kv: -kv[1]["articles_24h"])[:20]
        top_delta = sorted(
            [
                (t, d) for t, d in act.items()
                if d["articles_24h"] > d["previous_articles_24h"]
            ],
            key=lambda kv: -(kv[1]["articles_24h"] - kv[1]["previous_articles_24h"]),
        )[:20]
        if top_24h:
            print(
                f"[RSS_SWEEPER] sweep_id={sweep_id} top-24h: "
                + " ".join(f"{t}={d['articles_24h']}" for t, d in top_24h[:10])
            )
        if top_delta:
            print(
                f"[RSS_SWEEPER] sweep_id={sweep_id} top-delta: "
                + " ".join(
                    f"{t}=+{d['articles_24h']-d['previous_articles_24h']}"
                    for t, d in top_delta[:10]
                )
            )
    except Exception:
        pass

    print(
        f"[RSS_SWEEPER] sweep_id={sweep_id} "
        f"tickers={len(tickers)} "
        f"ok={sweep_diag['tickers_successful']} "
        f"partial={sweep_diag['tickers_partial']} "
        f"failed={sweep_diag['tickers_failed']} "
        f"yahoo_ok={sweep_diag['yahoo_success']} "
        f"google_ok={sweep_diag['google_success']} "
        f"raw_y={sweep_diag['rss_articles_raw_yahoo']} "
        f"raw_g={sweep_diag['rss_articles_raw_google']} "
        f"merged={sweep_diag['merged_articles_before_dedupe']} "
        f"dups={sweep_diag['cross_feed_duplicates_removed']} "
        f"upserted={sweep_diag['unique_article_associations_upserted']} "
        f"fmp_requests=0 "
        f"duration_ms={elapsed_ms}"
    )

    return sweep_diag


# ── Main loop (registered exactly once in main.py lifespan) ──────────────────

async def rss_sweeper_loop() -> None:
    """
    Continuous RSS archive sweep loop.

    Registered via asyncio.create_task(rss_sweeper_loop()) in main.py lifespan.
    Never starts a second sweep while one is still running (_SWEEP_LOCK).
    Target full-sweep interval: ~120 seconds.
    """
    _SWEEPER_DIAG["collector_started_at"] = time.time()
    _SWEEPER_DIAG["loop_registered"]      = True

    await asyncio.sleep(_STARTUP_DELAY_S)   # brief startup delay — let PG init settle

    while True:
        sweep_start = time.time()

        if _SWEEP_LOCK.locked():
            # Prior sweep still running (very slow network?) — skip this cycle
            print("[RSS_SWEEPER] prior sweep still running — skipping cycle")
            await asyncio.sleep(30)
            continue

        async with _SWEEP_LOCK:
            try:
                await run_rss_sweep()
            except Exception as exc:
                print(f"[RSS_SWEEPER] unhandled sweep error: {exc}")

        elapsed = time.time() - sweep_start
        wait    = max(0.0, _TARGET_INTERVAL_S - elapsed)
        next_ts = time.time() + wait
        _SWEEPER_DIAG["next_sweep_target_at"] = datetime.utcfromtimestamp(next_ts).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

        if wait > 0:
            await asyncio.sleep(wait)
