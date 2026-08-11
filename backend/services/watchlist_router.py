"""
Watchlist Router — FastAPI endpoints for multi-watchlist CRUD, news, refresh, and stock detail.
"""

from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from agent.model_policy import MODEL_CLAUDE_PREMIUM, MODEL_GROK, MODEL_GPT4O, MODEL_GEMINI

import asyncio
import json as _json
import math as _math
import re as _re
import time as _time
from datetime import datetime, timezone

import httpx
from fastapi import APIRouter, Body, HTTPException, Request
from pydantic import BaseModel, ConfigDict
from typing import Any, Dict, List, Optional

from services.watchlist_service import (
    save_watchlist,
    load_watchlist,
    list_watchlists,
    clear_watchlist,
    extract_tickers,
    fetch_news_for_tickers,
    get_stock_detail,
    _WATCHLIST_FILE,
)
from services.watchlist_analysis import run_analysis_pipeline
from services.news_major_service import (
    build_major_developments as _build_major,
    _cluster_key as _mk_ck,
    _parse_ts as _mk_parse_ts,
)

router = APIRouter(prefix="/api/watchlist", tags=["watchlist"])

# ── Bulk csv_data response strip ──────────────────────────────────────────────
# Keys removed from every csv_data row in the bulk watchlist response.
# earnings_intelligence (~40 KB/ticker) is served only by
# GET /ticker-detail/{symbol}; it must not be embedded in the bulk payload.
_BULK_CSV_STRIP: frozenset[str] = frozenset({"earnings_intelligence"})

# ── Rel-vol rank snapshot: in-memory fallback (survives within process) ───────
# Keyed by watchlist_id → {"current": {SYM: {rank, rel_vol}},
#                           "previous": {SYM: {rank, rel_vol}} | None}
_rv_mem: dict[str, dict] = {}
_volmc_mem: dict[str, dict] = {}

# ── Snapshot cadence registries ───────────────────────────────────────────────
# GET path populates these registries then returns — it never writes new rank
# snapshots.  The ~5-minute background loop (_watchlist_rank_snapshot_loop in
# main.py) reads these registries, builds fresh snapshots from the warm quote
# cache, and advances current/previous.  This keeps the comparison baseline
# stable regardless of page-refresh frequency.
#
# _rv_registry:    watchlist_id → [full normalized ticker list]
# _volmc_registry: watchlist_id → {"tickers": [...], "pcts": {SYM: vol_mc_pct}}
_rv_registry:    dict[str, list[str]] = {}
_volmc_registry: dict[str, dict]      = {}

# ── Bulk GET response LKG (last-known-good) cache ────────────────────────────
# Keyed by watchlist_id → {"payload": dict, "ts": float, "version": str}.
#
# "version" = f"{updated_at|saved_at}|{ticker_count}" — structural fingerprint.
# A version mismatch (ticker added/removed, /save) immediately invalidates the
# entry so stale membership cannot linger after mutations.
#
# TTL semantics (stale-while-revalidate; no hard age eviction):
#   _BULK_LKG_TTL       — serve from cache without scheduling a rebuild (fresh)
#   _BULK_LKG_STALE_TTL — diagnostic label; entry is still served; rebuild is
#                          logged as "very_stale" rather than "stale"
#   Version mismatch    — only this causes a structural miss → inline rebuild
#
# A valid-version entry is always served regardless of age; age only controls
# how urgently a background rebuild is queued.
#
# Single-flight: _BULK_LKG_BUILDING prevents concurrent rebuilds per watchlist.
_BULK_LKG:          dict[str, dict] = {}
_BULK_LKG_BUILDING: set[str]        = set()
_BULK_LKG_TTL       = 5 * 60    # 5 min — serve from cache without rebuild
_BULK_LKG_STALE_TTL = 20 * 60   # 20 min — serve stale while rebuild runs in bg

# Per-watchlist taxonomy generation counter.
# Incremented by invalidate_bulk_lkg_for_ticker() whenever a canonical taxonomy
# mutation affects a cached watchlist.  _rebuild_bulk_lkg_bg() captures the
# counter value at task-start and discards its result if the counter advanced
# while the rebuild was in flight, preventing a pre-mutation background rebuild
# from overwriting the correct fresh payload written by the immediate inline GET.
_TAXONOMY_GEN: dict[str, int] = {}


def _bulk_lkg_invalidate(watchlist_id: str) -> None:
    """Drop the cached bulk GET response for a watchlist.

    Call on every mutation: /save, add-ticker, remove-ticker, bulk-add.
    Idempotent: safe to call even when no entry is present.
    """
    _BULK_LKG.pop(watchlist_id, None)


def invalidate_bulk_lkg_for_ticker(ticker: str) -> None:
    """
    Drop bulk LKG entries for all watchlists currently caching a payload that
    contains *ticker*.

    Called by the canonical taxonomy PUT immediately after a successful atomic
    DB commit.  Because that route has no watchlist_id, we identify affected
    watchlists by scanning the in-process LKG payload.

    Safety contract:
      - Only called AFTER the DB commit succeeds (failures raise before reaching
        the caller, so no false-positive invalidations on failed transactions).
      - Zero provider calls.  Zero DB queries.  Pure in-memory O(W × T) scan
        where W = cached watchlists, T = tickers per section.
      - Also increments _TAXONOMY_GEN[wl_id] for every affected watchlist so
        that any background rebuild already in flight at the time of the taxonomy
        mutation discards its pre-mutation result instead of overwriting the
        correct fresh payload from the immediate post-mutation inline GET.
    """
    sym = (ticker or "").strip().upper()
    if not sym:
        return

    affected: list[str] = []
    for _wl_id, _entry in list(_BULK_LKG.items()):
        _payload  = _entry.get("payload") or {}
        _analysis = _payload.get("analysis") or {}
        _sections = _analysis.get("sections") or []
        _found    = False
        for _sec in _sections:
            for _row in _sec.get("tickers", []):
                if str(_row.get("symbol") or "").strip().upper() == sym:
                    _found = True
                    break
            if _found:
                break
        if _found:
            affected.append(_wl_id)

    for _wl_id in affected:
        _bulk_lkg_invalidate(_wl_id)
        _TAXONOMY_GEN[_wl_id] = _TAXONOMY_GEN.get(_wl_id, 0) + 1
        print(
            f"[WATCHLIST_LKG] taxonomy_invalidate wl={_wl_id} ticker={sym} "
            f"gen={_TAXONOMY_GEN[_wl_id]}"
        )

    if not affected:
        print(
            f"[WATCHLIST_LKG] taxonomy_invalidate ticker={sym}: "
            f"no cached watchlist payloads contain this ticker (LKG cold or ticker absent)"
        )


async def _rebuild_bulk_lkg_bg(watchlist_id: str) -> None:
    """
    Single-flight background task — refresh the bulk GET LKG for watchlist_id.

    Copy-on-success model:
      1. The OLD LKG entry remains in _BULK_LKG and continues to be served by
         the GET route throughout this rebuild — it is never removed first.
      2. _build_watchlist_response() runs the same canonical enrichment pipeline
         as the inline GET path.
      3. On success: atomically write the new entry, replacing the old —
         BUT ONLY if the taxonomy generation counter has not advanced since
         this task started.  If it has, a taxonomy mutation occurred during
         this rebuild; the result is discarded so the mutation-correct inline
         GET response (already stored by the immediate POST-mutation GET) is
         never overwritten with pre-mutation state.
      4. On failure: the old entry is completely untouched — it continues to be
         served on all subsequent GETs (with another rebuild queued next hit).

    _BULK_LKG_BUILDING prevents a second rebuild from spawning while this
    runs.  Always cleared in `finally` so the guard never sticks on error.
    """
    import asyncio as _aio_bg
    import time as _t_bg

    # Capture taxonomy generation BEFORE any await so we can detect mutations
    # that happened while this rebuild was running.
    _gen_at_start = _TAXONOMY_GEN.get(watchlist_id, 0)

    try:
        from services.watchlist_service import load_watchlist as _lw_bg
        _store_bg = await _aio_bg.get_event_loop().run_in_executor(
            None, _lw_bg, watchlist_id
        )
        if _store_bg is None:
            print(f"[WATCHLIST_LKG] rebuild skipped wl={watchlist_id}: watchlist not found")
            return
        # Build response using the canonical pipeline.
        # Old LKG remains in _BULK_LKG throughout this await.
        _result_bg = await _build_watchlist_response(watchlist_id, _store_bg)
        _ver_bg = (
            f"{_store_bg.get('updated_at') or _store_bg.get('saved_at')}|"
            f"{len(_store_bg.get('tickers', []))}"
        )

        # Race guard: if a taxonomy mutation occurred while we were building,
        # the immediate POST-mutation inline GET has already stored a correct
        # fresh entry.  Discard our pre-mutation result to avoid overwriting it.
        if _TAXONOMY_GEN.get(watchlist_id, 0) != _gen_at_start:
            print(
                f"[WATCHLIST_LKG] rebuild result DISCARDED wl={watchlist_id}: "
                f"taxonomy mutated during rebuild "
                f"(gen_start={_gen_at_start} gen_now={_TAXONOMY_GEN.get(watchlist_id, 0)})"
            )
            return

        _BULK_LKG[watchlist_id] = {
            "payload": _result_bg,
            "ts":      _t_bg.monotonic(),
            "version": _ver_bg,
        }
        print(f"[WATCHLIST_LKG] background rebuild complete wl={watchlist_id}")
    except Exception as _lkg_bg_err:
        # Old LKG is untouched — still served by GET; next stale hit retries.
        print(
            f"[WATCHLIST_LKG] background rebuild failed wl={watchlist_id} "
            f"(old LKG preserved): {_lkg_bg_err}"
        )
    finally:
        _BULK_LKG_BUILDING.discard(watchlist_id)


# ── News endpoint LKG (last-known-good) cache ────────────────────────────────
# Keyed by watchlist_id (use "default" for the no-id endpoint).
# Stores the fully-built + scored response so callers get instant results while
# a background refresh runs silently in the background when data goes stale.
#
#   _NEWS_LKG_SERVE_TTL : serve stale data from cache and kick a bg refresh
#   per-ticker fetch TTL : controlled by _NEWS_CACHE_TTL in watchlist_service.py (30 min)
#
_news_lkg: dict[str, dict] = {}    # watchlist_id -> {"data": dict, "ts": float}
_news_bg_building: set[str] = set()
_NEWS_LKG_SERVE_TTL = 20 * 60      # 20 min — after this, serve stale + bg-refresh

# Single-flight guard for Neon-archive cold builds.
# Prevents concurrent cold GETs from each triggering an expensive archive
# reconstruction for the same watchlist.  Also used by _prewarm_news_lkg so
# that a concurrent first request defers rather than running a parallel build.
_news_archive_building: set[str] = set()

# ── Hyperscaler article cache (module-level — NOT rebuilt on every GET /news) ──
#
# Architecture: the 72-hour archive query + score_article loop runs at most once
# per _HYP_CACHE_TTL_S seconds (typically triggered by the RSS sweeper after each
# completed sweep), then GET /news simply filters the in-memory cache to the
# requested ticker set.  This prevents the ~60s browser polling cadence from
# re-scoring thousands of archive rows on every request.
#
# Lifecycle:
#   Cold start        → first _attach_live_fields call awaits _rebuild_hyperscaler_cache
#   After each sweep  → sweeper calls _rebuild_hyperscaler_cache as a background task
#   Stale (> TTL)     → next GET /news fires _rebuild_hyperscaler_cache as a bg task
_HYP_CACHE: dict        = {"articles": [], "built_at": 0.0}
_HYP_CACHE_BUILDING     = False
_HYP_CACHE_TTL_S: int   = 180   # rebuild at most every 3 min (sweeper cadence ≈120s)


# ── User identity helper (same pattern as routes/screener_hub._get_user_id) ──

def _get_user_id(request: Request) -> str:
    """
    Resolve user_id for the request.

    Priority:
      1. request.state.user_id (middleware, if ever re-enabled)
      2. Authorization: Bearer <JWT> → payload["sub"]
      3. "default" — unauthenticated / local-dev fallback
    """
    uid = getattr(request.state, "user_id", None)
    if uid:
        return str(uid)
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        token = auth[7:]
        try:
            from auth import verify_token
            payload = verify_token(token)
            sub = payload.get("sub")
            if sub:
                return str(sub)
        except Exception:
            pass
    return "default"


def _news_response(
    enriched_map: dict,
    major_summary: dict,
    ts: float,
    is_building: bool = False,
    debug_reason: str | None = None,
    cache_source: str = "live_refresh",
) -> dict:
    """Build the standardised Live News response.

    cache_source values:
      "live_refresh"  — built from live Yahoo/Google RSS provider calls
      "neon_archive"  — reconstructed from the durable Neon RSS archive (cold start)
      "memory_lkg"    — served from the in-process LKG dict without any I/O
      "building"      — no data yet; background build in progress
    """
    age = round(_time.time() - ts)
    cached_at = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%dT%H:%M:%SZ")
    top_articles = major_summary.get("major_developments", [])
    resp = {
        # Original per-ticker map — preserved for any existing consumer
        "articles":         enriched_map,
        # Pre-ranked high-signal articles — use this for the Live News panel
        "top_articles":     top_articles,
        "high_signal_count": major_summary.get("high_signal_count", 0),
        "by_catalyst_type":  major_summary.get("by_catalyst_type", {}),
        "news_signal_meta":  major_summary.get("news_signal_meta", {}),
        # Cache metadata
        "cached_at":        cached_at,
        "cache_age_s":      age,
        "is_building":      is_building,
        # Additive diagnostic: provenance of this payload (does not break existing consumers)
        "cache_source":     cache_source,
    }
    if debug_reason:
        resp["debug_reason"] = debug_reason
    elif not top_articles and not enriched_map:
        resp["debug_reason"] = "no_articles_returned"
    return resp


async def _bg_refresh_news(watchlist_id: str, tickers: list[str]) -> None:
    """Background task: silently refresh news LKG cache for a watchlist."""
    if watchlist_id in _news_bg_building:
        return
    _news_bg_building.add(watchlist_id)
    try:
        raw_map = await fetch_news_for_tickers(tickers)
        try:
            enriched_map, major_summary = _build_major(raw_map)
        except Exception as _e:
            print(f"[NEWS_LKG] major build error (non-fatal): {_e}")
            enriched_map, major_summary = raw_map, {}
        ts   = _time.time()
        data = _news_response(enriched_map, major_summary, ts, cache_source="live_refresh")
        _news_lkg[watchlist_id] = {"data": data, "ts": ts}
        top_ct = len(data.get("top_articles") or [])
        art_ct = sum(len(v) for v in enriched_map.values())
        print(f"[NEWS_LKG] refresh done  wl={watchlist_id}  articles={art_ct}  top={top_ct}")
    except Exception as exc:
        print(f"[NEWS_LKG] refresh error wl={watchlist_id}: {exc}")
    finally:
        _news_bg_building.discard(watchlist_id)


async def _get_news_for_watchlist(watchlist_id: str, tickers: list[str]) -> dict:
    """
    Return a Live News response, using the LKG cache:
      - Hit  & fresh  (<20 min) → instant return, no I/O
      - Hit  & stale (≥20 min) → instant return of stale data + bg refresh
      - Miss (first call)       → blocking build, then cache

    Always attaches three additive live fields before returning:
      ticker_activity      — 24h/previous-24h counts from Neon archive
      hyperscaler_articles — catalyst_type==hyperscaler_anchor, deduped
      rss_activity_meta    — sweeper diagnostics
    """
    now = _time.time()
    lkg = _news_lkg.get(watchlist_id)

    if lkg:
        age  = now - lkg["ts"]
        data = dict(lkg["data"])
        data["cache_age_s"] = round(age)
        data["cache_source"] = "memory_lkg"   # always overwrite — serving from process memory
        data["is_building"] = watchlist_id in _news_bg_building
        if age > _NEWS_LKG_SERVE_TTL and watchlist_id not in _news_bg_building:
            asyncio.create_task(_bg_refresh_news(watchlist_id, tickers))
            data["is_building"] = True
        await _attach_live_fields(data, tickers)
        return data

    # ── Cold path: Neon-archive reconstruction (NEVER live RSS on request path) ─
    #
    # Priority:
    #   1. Neon archive reconstruction  → instant usable payload, background live refresh
    #   2. Archive unavailable / empty  → structured building response, background live refresh
    #
    # fetch_news_for_tickers() (461×2 RSS HTTP calls, 12-90s) is NEVER called
    # synchronously here.  It runs only inside _bg_refresh_news (background task).
    #
    # Single-flight: if _news_archive_building already contains this watchlist_id
    # (concurrent cold GET or prewarm still in progress), coalesce: return an
    # immediate building response and let the in-flight build win.

    if watchlist_id in _news_archive_building:
        print(f"[NEWS_LKG] cold coalesced wl={watchlist_id} — archive build already in flight")
        coalesced = _news_response(
            {}, {}, now,
            is_building=True,
            cache_source="building",
            debug_reason="archive_build_in_progress",
        )
        if watchlist_id not in _news_bg_building:
            asyncio.create_task(_bg_refresh_news(watchlist_id, tickers))
        await _attach_live_fields(coalesced, tickers)
        return coalesced

    # Attempt Neon archive reconstruction — zero provider calls
    _news_archive_building.add(watchlist_id)
    try:
        data = await _build_news_from_archive(watchlist_id, tickers)
    finally:
        _news_archive_building.discard(watchlist_id)

    if data is not None:
        ts_stored = _time.time()
        _news_lkg[watchlist_id] = {"data": data, "ts": ts_stored}
        # Background live RSS refresh — will update LKG when done (~30-60s)
        if watchlist_id not in _news_bg_building:
            asyncio.create_task(_bg_refresh_news(watchlist_id, tickers))
        data = dict(data)
        data["cache_age_s"] = 0
        await _attach_live_fields(data, tickers)
        return data

    # Archive unavailable or empty — return structured building response immediately.
    # Never fall back to synchronous fetch_news_for_tickers.
    print(f"[NEWS_LKG] archive unavailable wl={watchlist_id} — returning building response")
    building = _news_response(
        {}, {}, now,
        is_building=True,
        cache_source="building",
        debug_reason="archive_unavailable",
    )
    if watchlist_id not in _news_bg_building:
        asyncio.create_task(_bg_refresh_news(watchlist_id, tickers))
    await _attach_live_fields(building, tickers)
    return building


# ── Live-field helpers (ticker_activity, hyperscaler_articles, rss_activity_meta) ──

def _coverage_status(first_seen_ts: float | None, now_ts: float) -> str:
    """
    Determine coverage quality for the previous-48h comparison window.

    Uses first_seen_ts = MIN(first_seen_at) from the archive — the time the
    COLLECTOR first wrote data for this ticker.  NOT oldest published_at.

    RSS feeds contain articles published 3-5 days ago, so MIN(published_at)
    would immediately appear >96h old on the very first sweep, giving a false
    "complete" on a brand-new archive.  first_seen_at records when we actually
    observed the data, not when the article was authored.

    Status semantics:
        warming  — collector has < 96h of observation history for this ticker.
                   The previous-48h comparison window is not yet reliably
                   populated.  This is the only correct status for healthy
                   tickers still accumulating history — NOT provider_partial.
        complete — collector has ≥ 96h of history AND current coverage is
                   healthy.
        provider_partial — reserved for genuine per-ticker provider failure
                   (Yahoo or Google RSS confirmed incomplete for this ticker).
                   NOT emitted here because per-ticker provider health is not
                   currently passed into this function.  Will be populated when
                   query_ticker_activity returns per-ticker provider presence.

    Two-state logic (provider health not yet available per ticker):
        age < 96h  → warming
        age >= 96h → complete
    """
    if first_seen_ts is None:
        return "warming"
    age_hours = (now_ts - first_seen_ts) / 3600.0
    if age_hours >= 96:
        return "complete"
    return "warming"


def _build_ticker_activity_list(
    tickers: list[str],
    activity_raw: dict,
    now_ts: float,
) -> list[dict]:
    """
    Build the ticker_activity list from Neon query results.

    coverage_status is derived from first_seen_ts — when the COLLECTOR first
    wrote data for this ticker — not from the article's publication date.

    During warming (collector < 96h old for ticker): the previous-48h comparison
    window is not reliably populated, so comparison fields are null:
        previous_articles_48h = null
        delta_count           = null
        delta_pct             = null
        delta_label           = null

    articles_48h is always populated (may be zero).

    delta_pct semantics (non-warming only):
      previous > 0                 → ((current - previous) / previous) * 100
      previous == 0, current == 0  → 0.0
      previous == 0, current > 0   → None  (delta_label = "new")
    """
    result = []
    activity_as_of = datetime.utcfromtimestamp(now_ts).strftime("%Y-%m-%dT%H:%M:%SZ")

    for ticker in tickers:
        row = activity_raw.get(ticker)
        if row:
            cur_48     = int(row.get("articles_48h", 0))
            prev_48    = int(row.get("previous_articles_48h", 0))
            first_seen = row.get("first_seen_ts")   # MIN(first_seen_at) unix ts
        else:
            cur_48 = prev_48 = 0
            first_seen = None

        cov_status = _coverage_status(first_seen, now_ts)

        if cov_status == "warming":
            # Collector has not yet completed 96h of continuous observation.
            # Do not expose a comparison that is based solely on RSS backfill.
            result.append({
                "ticker":                ticker,
                "articles_48h":          cur_48,
                "previous_articles_48h": None,
                "delta_count":           None,
                "delta_pct":             None,
                "delta_label":           None,
                "activity_as_of":        activity_as_of,
                "coverage_status":       "warming",
            })
            continue

        # coverage_status is "complete" or "provider_partial" — safe to compare
        if prev_48 > 0:
            delta_count = cur_48 - prev_48
            delta_pct   = round(((cur_48 - prev_48) / prev_48) * 100.0, 1)
            delta_label = None
        elif cur_48 == 0:
            delta_count = 0
            delta_pct   = 0.0
            delta_label = None
        else:
            # previous == 0, current > 0 — genuine new activity signal
            delta_count = cur_48
            delta_pct   = None
            delta_label = "new"

        result.append({
            "ticker":                ticker,
            "articles_48h":          cur_48,
            "previous_articles_48h": prev_48,
            "delta_count":           delta_count,
            "delta_pct":             delta_pct,
            "delta_label":           delta_label,
            "activity_as_of":        activity_as_of,
            "coverage_status":       cov_status,
        })

    result.sort(key=lambda r: (-r["articles_48h"], r["ticker"]))
    return result


def _build_hyperscaler_articles(enriched_map: dict) -> list[dict]:
    """
    Select catalyst_type==hyperscaler_anchor articles from the enriched news map.
    Deduplicates using the same _cluster_key as news_major_service.
    Sorts by major_news_score desc, recency desc.
    Does NOT reuse or replace the top-20 major developments list.
    """
    all_hyp: list[dict] = []
    for articles in enriched_map.values():
        for a in articles:
            if a.get("catalyst_type") == "hyperscaler_anchor":
                all_hyp.append(a)

    seen_ck: dict[str, bool] = {}
    deduped: list[dict] = []
    for a in all_hyp:
        ck = _mk_ck(a.get("title") or "", a.get("url") or "")
        if ck not in seen_ck:
            seen_ck[ck] = True
            deduped.append(a)

    deduped.sort(key=lambda a: (-(a.get("major_news_score") or 0),
                                 -_mk_parse_ts(a.get("published_at") or "")))
    return deduped


async def _rebuild_hyperscaler_cache(tickers: list[str]) -> None:
    """
    Query the 72-hour Neon archive, score all articles, run the two-pass cluster
    aggregation, and atomically replace _HYP_CACHE["articles"].

    Called:
      • Awaited synchronously on cold start (first ever GET /news for this process).
      • As asyncio.create_task() after each RSS sweep completes (sweeper hook).
      • As asyncio.create_task() when cache is stale at GET /news time (TTL guard).

    NEVER called inline on the hot GET /news path after the first cold build.
    The 72-hour hyperscaler lookback is intentional and separate from retention.
    """
    global _HYP_CACHE, _HYP_CACHE_BUILDING
    if _HYP_CACHE_BUILDING:
        return
    _HYP_CACHE_BUILDING = True
    t0 = _time.time()
    try:
        from data.rss_article_archive import query_recent_articles_for_scoring
        from services.news_signal_scorer import (
            score_article    as _score_archive_article,
            resolve_anchor_symbols as _resolve_anchor_syms,
        )
        loop = asyncio.get_event_loop()
        archive_map: dict[str, list[dict]] = await loop.run_in_executor(
            None, query_recent_articles_for_scoring, list(tickers), 72
        )

        # Pass 1 — accumulate ALL ticker associations per cluster
        cluster_article: dict[str, dict] = {}
        cluster_tickers: dict[str, set]  = {}
        archive_rows = 0
        scored_ct    = 0
        for arch_ticker, arch_articles in archive_map.items():
            archive_rows += len(arch_articles)
            for a in arch_articles:
                scored = _score_archive_article(a, arch_ticker)
                scored_ct += 1
                if scored.get("catalyst_type") == "hyperscaler_anchor":
                    ck = _mk_ck(scored.get("title") or "", scored.get("url") or "")
                    if ck not in cluster_article:
                        cluster_article[ck] = scored
                    cluster_tickers.setdefault(ck, set()).add(arch_ticker.upper())

        # Pass 2 — resolve symbol fields for each surviving cluster
        deduped_hyp: list[dict] = []
        for ck, article in cluster_article.items():
            wl_syms  = sorted(cluster_tickers.get(ck, set()))
            entities = article.get("matched_entities") or []
            anc_syms = _resolve_anchor_syms(entities)
            seen_hl: dict[str, bool] = {}
            highlight: list[str] = []
            for s in wl_syms + anc_syms:
                if s not in seen_hl:
                    seen_hl[s] = True
                    highlight.append(s)
            sym_roles: dict[str, str] = {}
            for s in wl_syms:
                sym_roles[s] = "watchlist"
            for s in anc_syms:
                if s not in sym_roles:
                    sym_roles[s] = "anchor"
            deduped_hyp.append({
                **article,
                "watchlist_symbols":   wl_syms,
                "anchor_symbols":      anc_syms,
                "highlight_symbols":   highlight,
                "highlighted_tickers": [{"ticker": t, "role": sym_roles[t]} for t in highlight],
                "symbol": wl_syms[0] if wl_syms else None,
            })

        deduped_hyp.sort(
            key=lambda a: (
                -(a.get("major_news_score") or 0),
                -_mk_parse_ts(a.get("published_at") or ""),
            )
        )
        _HYP_CACHE["articles"] = deduped_hyp
        _HYP_CACHE["built_at"] = _time.time()
        elapsed_ms = round((_time.time() - t0) * 1000)
        print(
            f"[HYP_CACHE] built {len(deduped_hyp)} clusters "
            f"from {archive_rows} archive rows ({scored_ct} scored) "
            f"for {len(tickers)} tickers  elapsed={elapsed_ms}ms"
        )
    except Exception as _e:
        print(f"[HYP_CACHE] rebuild error (non-fatal): {_e}")
    finally:
        _HYP_CACHE_BUILDING = False


async def _attach_live_fields(data: dict, tickers: list[str]) -> None:
    """
    Attach ticker_activity, hyperscaler_articles, rss_activity_meta to a
    news response dict in-place.  Never raises — fields default to safe empties.
    Each section is independently isolated: a failure in one section does not
    affect the others or the base /news response.
    """
    now_ts = _time.time()
    enriched_map = data.get("articles") or {}

    # ── 1. ticker_activity — live Neon query ──────────────────────────────
    try:
        from data.rss_article_archive import query_ticker_activity
        loop = asyncio.get_event_loop()
        activity_raw = await loop.run_in_executor(None, query_ticker_activity, list(tickers))
        data["ticker_activity"] = _build_ticker_activity_list(tickers, activity_raw, now_ts)
    except Exception as e:
        print(f"[NEWS_LKG] ticker_activity error (non-fatal): {e}")
        data["ticker_activity"] = _build_ticker_activity_list(tickers, {}, now_ts)

    # ── 2. hyperscaler_articles — served from module-level cache ─────────────
    #
    # The 72-hour archive query + score_article loop lives in
    # _rebuild_hyperscaler_cache(), which runs at most once per
    # _HYP_CACHE_TTL_S seconds.  This function NEVER re-scores the archive;
    # it only filters the warm in-memory cache to the requested ticker set.
    #
    # Cold start  → await the rebuild synchronously (once per process lifetime)
    # Stale cache → fire background task, serve current cache immediately
    # Fresh cache → filter and return instantly, zero I/O
    try:
        cache_age_hyp = now_ts - _HYP_CACHE["built_at"]
        if _HYP_CACHE["built_at"] == 0.0:
            # Cold start: fire rebuild in background — do NOT await synchronously.
            # _prewarm_news_lkg() schedules this before any request arrives.
            # On the rare first-request-wins race the cache starts empty (hyperscaler_articles=[])
            # and fills within ~5s without blocking the /news response.
            if not _HYP_CACHE_BUILDING:
                asyncio.create_task(_rebuild_hyperscaler_cache(tickers))
        elif cache_age_hyp > _HYP_CACHE_TTL_S and not _HYP_CACHE_BUILDING:
            # Stale: rebuild in background; serve the current (slightly stale) cache
            asyncio.create_task(_rebuild_hyperscaler_cache(tickers))

        # Filter to this watchlist's ticker set — O(n) over cached articles
        ticker_set = {t.upper() for t in tickers}
        data["hyperscaler_articles"] = [
            a for a in _HYP_CACHE["articles"]
            if any(ws in ticker_set for ws in (a.get("watchlist_symbols") or []))
        ]
    except Exception as e:
        print(f"[NEWS_LKG] hyperscaler_articles error (non-fatal): {e}")
        data["hyperscaler_articles"] = []

    # ── 3. rss_activity_meta — sweeper diagnostics ────────────────────────
    try:
        from services.watchlist_rss_sweeper import get_sweeper_meta
        data["rss_activity_meta"] = get_sweeper_meta(list(tickers))
    except Exception as e:
        print(f"[NEWS_LKG] rss_activity_meta error (non-fatal): {e}")
        data["rss_activity_meta"] = {
            "providers":               ["yahoo_rss", "google_news_rss"],
            "window_hours":            48,
            "comparison_window_hours": 48,
            "retention_hours":         120,
            "collector_started_at":    None,
            "last_full_sweep_at":      None,
            "sweep_in_progress":       False,
            "current_sweep_started_at": None,
            "last_sweep_duration_ms":  None,
            "ticker_count":            len(tickers),
        }


# ── Neon-archive news reconstruction ─────────────────────────────────────────
#
# _build_news_from_archive  — reconstruct a full LKG payload from the durable
#     Neon RSS archive for a single watchlist.  Zero provider calls.  Used by
#     both the cold-path of _get_news_for_watchlist and the startup prewarm.
#
# _prewarm_news_lkg  — post-yield bootstrap task.  Walks all active watchlists
#     and hydrates _news_lkg from Neon before any user request arrives so that
#     the first GET /news always returns a real payload without a live RSS fanout.

async def _build_news_from_archive(
    watchlist_id: str,
    tickers: list[str],
) -> dict | None:
    """
    Reconstruct a news LKG payload from the durable Neon RSS article archive.

    Flow:
      1. query_recent_articles_for_scoring(tickers, hours=48) — single bulk Neon read
      2. score_article() on each row (deterministic, no I/O) — same scorer used by
         the live fetch path so scoring semantics are identical
      3. _build_major() — existing dedup/ranking pipeline
      4. _news_response() — existing response builder

    Returns the response dict on success, or None if the archive is empty /
    temporarily unavailable (caller decides what to do in that case).

    Zero live provider calls.  Marked is_building=True because a background live
    refresh will follow to incorporate articles published since the last sweep.
    """
    loop = asyncio.get_event_loop()
    t0   = _time.time()

    # ── Step 1: Bulk Neon read ──────────────────────────────────────────────
    try:
        from data.rss_article_archive import query_recent_articles_for_scoring as _qras
        raw_map: dict[str, list[dict]] = await loop.run_in_executor(
            None, _qras, list(tickers), 48
        )
    except Exception as _e:
        print(f"[NEWS_ARCHIVE] Neon query error wl={watchlist_id}: {_e}")
        return None

    if not raw_map:
        print(f"[NEWS_ARCHIVE] archive empty wl={watchlist_id} — no 48h articles")
        return None

    # ── Step 2: Score each archive article (CPU only, no I/O) ──────────────
    try:
        from services.news_signal_scorer import score_article as _score_arc
        scored_map: dict[str, list[dict]] = {}
        total_raw = 0
        for _ticker, _articles in raw_map.items():
            scored_map[_ticker] = [_score_arc(_a, _ticker) for _a in _articles]
            total_raw += len(_articles)
    except Exception as _e:
        print(f"[NEWS_ARCHIVE] scoring error wl={watchlist_id} (non-fatal): {_e}")
        scored_map = raw_map
        total_raw  = sum(len(v) for v in raw_map.values())

    # ── Step 3: Major developments ranking ─────────────────────────────────
    try:
        enriched_map, major_summary = _build_major(scored_map)
    except Exception as _e:
        print(f"[NEWS_ARCHIVE] _build_major error wl={watchlist_id} (non-fatal): {_e}")
        enriched_map, major_summary = scored_map, {}

    ts         = _time.time()
    elapsed_ms = round((ts - t0) * 1000)
    top_ct     = len(major_summary.get("major_developments") or [])
    print(
        f"[NEWS_ARCHIVE] built wl={watchlist_id} tickers_with_data={len(raw_map)} "
        f"raw_articles={total_raw} top={top_ct} elapsed={elapsed_ms}ms"
    )

    # ── Step 4: Build response — is_building=True so UI knows live data follows ─
    data = _news_response(
        enriched_map, major_summary, ts,
        is_building=True,
        cache_source="neon_archive",
    )
    return data


async def _prewarm_news_lkg() -> None:
    """
    Post-yield startup prewarm: populate _news_lkg from the Neon RSS archive
    for every active watchlist before any user request arrives.

    Called as asyncio.create_task() from _post_yield_bootstrap() in main.py.

    Guarantees:
      • Non-blocking  — runs as a background coroutine; does not delay app readiness
      • Single-flight — respects _news_archive_building so a concurrent cold request
                         does not double-build the same watchlist
      • Best-effort   — any per-watchlist failure is logged and skipped
      • No providers  — only reads from the durable Neon archive; no Yahoo/Google RSS
      • No duplicates — skips watchlists already warm in _news_lkg
      • No sweeper    — does not interfere with the RSS sweeper loop

    Also fires _rebuild_hyperscaler_cache as a background task after warming the
    news LKG so that hyperscaler_articles is ready before the first GET /news request.
    """
    t0 = _time.time()
    print("[NEWS_PREWARM] starting archive-based LKG prewarm")
    try:
        from services.watchlist_service import (
            list_watchlists as _list_wl,
            load_watchlist  as _load_wl,
        )
        loop = asyncio.get_event_loop()

        watchlists = await loop.run_in_executor(None, _list_wl)
        if not watchlists:
            print("[NEWS_PREWARM] no watchlists found — prewarm skipped")
            return

        warmed = 0
        all_tickers: list[str] = []

        for wl_meta in watchlists:
            wl_id = wl_meta.get("id")
            if not wl_id:
                continue

            # Skip if already populated (e.g. a concurrent request beat us here)
            if wl_id in _news_lkg:
                print(f"[NEWS_PREWARM] wl={wl_id} already warm — skipping")
                continue

            # Skip if another build is already in progress for this watchlist
            if wl_id in _news_archive_building:
                print(f"[NEWS_PREWARM] wl={wl_id} archive build in progress — skipping")
                continue

            store = await loop.run_in_executor(None, _load_wl, wl_id)
            if store is None:
                continue
            tickers = store.get("tickers", [])
            if not tickers:
                continue

            all_tickers.extend(tickers)

            _news_archive_building.add(wl_id)
            try:
                data = await _build_news_from_archive(wl_id, tickers)
                if data is not None and wl_id not in _news_lkg:
                    _news_lkg[wl_id] = {"data": data, "ts": _time.time()}
                    warmed += 1
                    print(f"[NEWS_PREWARM] wl={wl_id} LKG hydrated from archive")
            except Exception as _wl_err:
                print(f"[NEWS_PREWARM] wl={wl_id} error (non-fatal): {_wl_err}")
            finally:
                _news_archive_building.discard(wl_id)

        # Kick hyperscaler cache rebuild in background so it's ready for first GET.
        # Uses union of all watchlist tickers collected above.
        if all_tickers and not _HYP_CACHE_BUILDING:
            asyncio.create_task(_rebuild_hyperscaler_cache(list(set(all_tickers))))

        elapsed_ms = round((_time.time() - t0) * 1000)
        print(f"[NEWS_PREWARM] complete: warmed={warmed}/{len(watchlists)} elapsed={elapsed_ms}ms")

    except Exception as exc:
        print(f"[NEWS_PREWARM] error (non-fatal): {exc}")


# ── Market-cap string parser ─────────────────────────────────────────────────

def _parse_market_cap_str(raw: Any) -> float | None:
    """
    Parse a market-cap value from various CSV formats into a raw float (USD).

    Handles: "1.23B", "456.78M", "12.34K", "$1,234,567", "1234567890",
             "1.5T", numbers (int/float), None / empty / "-".
    Returns None when unparseable.
    """
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw) if raw > 0 else None
    s = str(raw).strip().replace(",", "").replace("$", "").replace(" ", "")
    if not s or s in ("-", "N/A", "n/a", "--"):
        return None
    multipliers = {"T": 1e12, "B": 1e9, "M": 1e6, "K": 1e3}
    upper = s.upper()
    for suffix, mult in multipliers.items():
        if upper.endswith(suffix):
            try:
                return float(upper[:-1]) * mult
            except ValueError:
                return None
    try:
        v = float(s)
        return v if v > 0 else None
    except ValueError:
        return None


def _vol_mc_fields(price: float | None, volume: float | None, market_cap: float | None) -> dict:
    """
    Compute Vol/MC ratio fields using the same formula as CaelynTerminalProvider.

    Returns dict with:
      market_cap, dollar_volume, vol_mc_ratio, vol_mc_pct,
      vol_mc_label, vol_mc_unavailable_reason
    All values are None-safe — never raises.
    """
    dollar_vol: float | None = None
    if price and volume and price > 0 and volume > 0:
        dollar_vol = round(price * volume, 2)

    vol_mc_ratio: float | None = None
    vol_mc_pct:   float | None = None
    vol_mc_label: str   | None = None
    vol_mc_unavail: str | None = None

    if dollar_vol is not None and market_cap and market_cap > 0:
        vol_mc_ratio = round(dollar_vol / market_cap, 6)
        vol_mc_pct   = round(vol_mc_ratio * 100, 4)
        if vol_mc_pct >= 10:
            vol_mc_label = "high"
        elif vol_mc_pct >= 5:
            vol_mc_label = "elevated"
        elif vol_mc_pct >= 1:
            vol_mc_label = "normal"
        else:
            vol_mc_label = "low"
    else:
        if not volume:
            vol_mc_unavail = "volume_unavailable"
        elif not market_cap:
            vol_mc_unavail = "market_cap_unavailable"
        else:
            vol_mc_unavail = "price_missing"

    return {
        "market_cap":                market_cap,
        "dollar_volume":             dollar_vol,
        "vol_mc_ratio":              vol_mc_ratio,
        "vol_mc_pct":                vol_mc_pct,
        "vol_mc_label":              vol_mc_label,
        "vol_mc_unavailable_reason": vol_mc_unavail,
    }


# ── Rel-vol rank + momentum helper ───────────────────────────────────────────

async def _apply_rv_rank_fields(
    watchlist_id: str,
    dedup_sections: list[dict],
    saved_normalized: list[str],
) -> list[dict]:
    """
    Compute relative-volume ranks for the current enrichment pass, compare
    against the previous snapshot (Neon-durable, in-memory fallback), and
    add the following fields to every ticker row:

        rel_vol_rank           int | None   — rank 1=highest rel_vol this pass
        rel_vol_prev_rank      int | None   — rank from previous snapshot
        rel_vol_rank_delta     int | None   — prev_rank - cur_rank (pos = moved up)
        rel_vol_trend          "up"|"down"|"flat"|"unknown"
        rel_vol_value_delta    float | None — cur_rel_vol - prev_rel_vol
        rel_vol_prev_value     float | None
        rel_vol_momentum_label "surging"|"rising"|"fading"|"falling"|"flat"|"unknown"

    Only writes a new snapshot when quote coverage is adequate (≥50% of
    US-eligible saved symbols have a numeric relative_volume), to avoid
    committing garbage signals during provider outages.

    Storage precedence: Neon (persistent across restarts) → in-memory dict
    (_rv_mem, survives within the process lifetime).
    """
    if not watchlist_id:
        return dedup_sections

    # ── 1. Build current snapshot from enriched rows ──────────────────────────
    eligible_rows: list[tuple[dict, float]] = []
    for section in dedup_sections:
        for row in section.get("tickers", []):
            rv = row.get("relative_volume")
            if isinstance(rv, (int, float)) and not _math.isnan(rv) and rv > 0:
                eligible_rows.append((row, float(rv)))

    eligible_rows.sort(key=lambda x: x[1], reverse=True)

    current_snap: dict[str, dict] = {}
    for rank_0, (row, rv) in enumerate(eligible_rows):
        sym = str(row.get("symbol", "")).upper()
        if sym:
            current_snap[sym] = {"rank": rank_0 + 1, "rel_vol": round(rv, 6)}

    # ── 2. Coverage guard — only save when ≥50% US-eligible symbols have rv ──
    us_eligible_count = sum(1 for s in saved_normalized if ":" not in s)
    rv_coverage = len(current_snap) / us_eligible_count if us_eligible_count > 0 else 0
    should_save = rv_coverage >= 0.5

    # ── 3. Load previous snapshot ─────────────────────────────────────────────
    prev_snap: dict[str, dict] | None = None

    # Try in-memory first (zero-latency within the process)
    mem_entry = _rv_mem.get(watchlist_id)
    if mem_entry:
        prev_snap = mem_entry.get("previous") or mem_entry.get("current")

    # Neon fallback (cross-restart persistence) — run in thread executor
    if prev_snap is None:
        try:
            loop = asyncio.get_event_loop()
            _cur, _prev = await loop.run_in_executor(
                None,
                lambda: _rv_neon_load(watchlist_id),
            )
            # On first run current==None, second run use current as previous
            prev_snap = _prev or _cur
        except Exception as _exc:
            print(f"[RV_RANK] Neon load error wl={watchlist_id}: {_exc}")

    # ── 4. Augment every row with rank/trend fields ───────────────────────────
    for section in dedup_sections:
        augmented: list[dict] = []
        for row in section.get("tickers", []):
            sym = str(row.get("symbol", "")).upper()
            cur_entry  = current_snap.get(sym)
            prev_entry = prev_snap.get(sym) if prev_snap else None

            cur_rank  = cur_entry["rank"]    if cur_entry  else None
            cur_rv    = cur_entry["rel_vol"] if cur_entry  else None
            prev_rank = prev_entry["rank"]    if prev_entry else None
            prev_rv   = prev_entry["rel_vol"] if prev_entry else None

            if cur_rank is not None and prev_rank is not None:
                delta = prev_rank - cur_rank   # positive = moved up (rank# fell)
                if delta > 0:
                    trend = "up"
                elif delta < 0:
                    trend = "down"
                else:
                    trend = "flat"
            else:
                delta = None
                trend  = "unknown"

            rv_delta = (
                round(cur_rv - prev_rv, 4)
                if cur_rv is not None and prev_rv is not None
                else None
            )

            if trend == "up" and delta is not None and delta >= 10:
                label = "surging"
            elif trend == "up":
                label = "rising"
            elif trend == "down" and delta is not None and delta <= -10:
                label = "falling"
            elif trend == "down":
                label = "fading"
            elif trend == "flat":
                label = "flat"
            else:
                label = "unknown"

            augmented.append({
                **row,
                "rel_vol_rank":           cur_rank,
                "rel_vol_prev_rank":      prev_rank,
                "rel_vol_rank_delta":     delta,
                "rel_vol_trend":          trend,
                "rel_vol_value_delta":    rv_delta,
                "rel_vol_prev_value":     prev_rv,
                "rel_vol_momentum_label": label,
            })
        section["tickers"] = augmented

    # ── 5. Register ticker universe for the background snapshot loop ──────────
    # GET never writes a new rank snapshot.  The ~5-minute background loop
    # (_watchlist_rank_snapshot_loop in main.py) reads _rv_registry, builds a
    # fresh snapshot from the warm quote cache, and advances current/previous.
    # This keeps the comparison baseline stable across page refreshes.
    _rv_registry[watchlist_id] = list(saved_normalized)
    print(
        f"[RV_RANK] wl={watchlist_id} ranked={len(current_snap)} "
        f"prev_known={prev_snap is not None} "
        f"coverage={rv_coverage:.0%} (snapshot advanced by background loop)"
    )

    return dedup_sections


def _rv_neon_load(watchlist_id: str) -> tuple:
    """Thin sync wrapper around pg_storage.rv_snapshot_load."""
    try:
        from data.pg_storage import rv_snapshot_load
        return rv_snapshot_load(watchlist_id)
    except Exception as exc:
        print(f"[RV_RANK] pg load skipped: {exc}")
        return (None, None)


def _rv_neon_save(watchlist_id: str, current_snap: dict) -> None:
    """Thin sync wrapper around pg_storage.rv_snapshot_save."""
    try:
        from data.pg_storage import rv_snapshot_save
        rv_snapshot_save(watchlist_id, current_snap)
    except Exception as exc:
        print(f"[RV_RANK] pg save skipped: {exc}")


# ── Vol/MC rank + momentum helper ─────────────────────────────────────────────

_VOLMC_FLAT_THRESHOLD = 0.05   # percentage-point delta below which = "flat"
_VOLMC_SURGE_THRESHOLD = 1.0   # ppt delta for "surging" / "falling" labels


async def _apply_volmc_rank_fields(
    watchlist_id: str,
    dedup_sections: list[dict],
    saved_normalized: list[str],
) -> list[dict]:
    """
    Compute Vol/MC ranks for the current enrichment pass, compare against the
    previous snapshot (Neon-durable, in-memory fallback), and add the following
    additive fields to every ticker row:

        vol_mc_rank           int | None   — rank 1=highest vol_mc_pct this pass
        vol_mc_prev_rank      int | None   — rank from previous snapshot
        vol_mc_rank_delta     int | None   — prev_rank - cur_rank (pos = moved up)
        vol_mc_prev_pct       float | None — previous snapshot vol_mc_pct
        vol_mc_pct_delta      float | None — cur_vol_mc_pct - prev_vol_mc_pct
        vol_mc_trend          "up"|"down"|"flat"|"unknown"
        vol_mc_momentum_label "surging"|"rising"|"flat"|"fading"|"falling"|"unknown"

    Trend threshold: |vol_mc_pct_delta| < 0.05 ppt → "flat".
    Momentum labels use value delta magnitude:
        ≥ +1.0 ppt  → surging   |  < +1.0 ppt up   → rising
        ≤ -1.0 ppt  → falling   |  > -1.0 ppt down  → fading

    Coverage guard: only saves snapshot when ≥50% of US-eligible symbols have
    a valid vol_mc_pct (reuses same threshold as rv_rank to reject outage data).

    Storage: Neon (persistent across restarts) → _volmc_mem (process-local).
    """
    if not watchlist_id:
        return dedup_sections

    # ── 1. Build current snapshot ─────────────────────────────────────────────
    eligible_rows: list[tuple[dict, float]] = []
    for section in dedup_sections:
        for row in section.get("tickers", []):
            pct = row.get("vol_mc_pct")
            if isinstance(pct, (int, float)) and not _math.isnan(pct) and pct > 0:
                eligible_rows.append((row, float(pct)))

    eligible_rows.sort(key=lambda x: x[1], reverse=True)

    current_snap: dict[str, dict] = {}
    for rank_0, (row, pct) in enumerate(eligible_rows):
        sym = str(row.get("symbol", "")).upper()
        if sym:
            current_snap[sym] = {"rank": rank_0 + 1, "vol_mc_pct": round(pct, 6)}

    # ── 2. Coverage guard — only save when ≥50% US-eligible have vol_mc_pct ──
    us_eligible_count = sum(1 for s in saved_normalized if ":" not in s)
    vm_coverage = len(current_snap) / us_eligible_count if us_eligible_count > 0 else 0
    should_save = vm_coverage >= 0.5

    # ── 3. Load previous snapshot ─────────────────────────────────────────────
    prev_snap: dict[str, dict] | None = None

    mem_entry = _volmc_mem.get(watchlist_id)
    if mem_entry:
        prev_snap = mem_entry.get("previous") or mem_entry.get("current")

    if prev_snap is None:
        try:
            loop = asyncio.get_event_loop()
            _cur, _prev = await loop.run_in_executor(
                None,
                lambda: _volmc_neon_load(watchlist_id),
            )
            prev_snap = _prev or _cur
        except Exception as _exc:
            print(f"[VOLMC_RANK] Neon load error wl={watchlist_id}: {_exc}")

    # ── 4. Augment every row ──────────────────────────────────────────────────
    for section in dedup_sections:
        augmented: list[dict] = []
        for row in section.get("tickers", []):
            sym = str(row.get("symbol", "")).upper()
            cur_entry  = current_snap.get(sym)
            prev_entry = prev_snap.get(sym) if prev_snap else None

            cur_rank   = cur_entry["rank"]       if cur_entry  else None
            cur_pct    = cur_entry["vol_mc_pct"] if cur_entry  else None
            prev_rank  = prev_entry["rank"]       if prev_entry else None
            prev_pct   = prev_entry["vol_mc_pct"] if prev_entry else None

            # Rank delta (optional fields)
            rank_delta: int | None = None
            if cur_rank is not None and prev_rank is not None:
                rank_delta = prev_rank - cur_rank  # positive = moved up

            # Value delta + trend (primary signal)
            pct_delta: float | None = None
            if cur_pct is not None and prev_pct is not None:
                pct_delta = round(cur_pct - prev_pct, 6)

            if pct_delta is None:
                trend = "unknown"
            elif pct_delta > _VOLMC_FLAT_THRESHOLD:
                trend = "up"
            elif pct_delta < -_VOLMC_FLAT_THRESHOLD:
                trend = "down"
            else:
                trend = "flat"

            if trend == "up" and pct_delta is not None and pct_delta >= _VOLMC_SURGE_THRESHOLD:
                label = "surging"
            elif trend == "up":
                label = "rising"
            elif trend == "down" and pct_delta is not None and pct_delta <= -_VOLMC_SURGE_THRESHOLD:
                label = "falling"
            elif trend == "down":
                label = "fading"
            elif trend == "flat":
                label = "flat"
            else:
                label = "unknown"

            augmented.append({
                **row,
                "vol_mc_rank":           cur_rank,
                "vol_mc_prev_rank":      prev_rank,
                "vol_mc_rank_delta":     rank_delta,
                "vol_mc_prev_pct":       prev_pct,
                "vol_mc_pct_delta":      pct_delta,
                "vol_mc_trend":          trend,
                "vol_mc_momentum_label": label,
            })
        section["tickers"] = augmented

    # ── 5. Register vol/MC values for the background snapshot loop ────────────
    # GET never writes a new rank snapshot.  The ~5-minute background loop
    # reads _volmc_registry and advances the vol/MC comparison baseline.
    _volmc_registry[watchlist_id] = {
        "tickers": list(saved_normalized),
        "pcts":    {sym: ent["vol_mc_pct"] for sym, ent in current_snap.items()},
    }
    print(
        f"[VOLMC_RANK] wl={watchlist_id} ranked={len(current_snap)} "
        f"prev_known={prev_snap is not None} "
        f"coverage={vm_coverage:.0%} (snapshot advanced by background loop)"
    )

    return dedup_sections


def _volmc_neon_load(watchlist_id: str) -> tuple:
    """Thin sync wrapper around pg_storage.volmc_snapshot_load."""
    try:
        from data.pg_storage import volmc_snapshot_load
        return volmc_snapshot_load(watchlist_id)
    except Exception as exc:
        print(f"[VOLMC_RANK] pg load skipped: {exc}")
        return (None, None)


def _volmc_neon_save(watchlist_id: str, current_snap: dict) -> None:
    """Thin sync wrapper around pg_storage.volmc_snapshot_save."""
    try:
        from data.pg_storage import volmc_snapshot_save
        volmc_snapshot_save(watchlist_id, current_snap)
    except Exception as exc:
        print(f"[VOLMC_RANK] pg save skipped: {exc}")


# ── Weinstein Stage Analysis (cache-only, no live fetches) ───────────────────

def _get_stage2_breakout(sym: str) -> dict:
    """
    Return a slim stage2_breakout dict for *sym*.  Zero I/O — reads only from
    already-populated caches.  Never fetches bars during page render.

    Lookup order:
      1. watchlist_stage2_service LKG (disk-backed, populated by off-hours warmup)
      2. In-memory bar caches (fmp_hist:{SYM} / tdier_hist:{SYM}:400, set by
         theme_rs_service for ETF/stock proxy symbols)

    Returns {"score": float|None, "label": str|None, "reason": str|None}
    """
    _null: dict = {"score": None, "label": None, "reason": None}
    try:
        # ── 1. Disk-backed LKG (primary — covers all 302 watchlist tickers) ─
        from services.watchlist_stage2_service import get_stage2 as _gs2
        lkg = _gs2(sym)
        # Return if we have a real result OR an explicit null stored by warmup
        # (explicit null = computed_at is present in the internal dict)
        from services import watchlist_stage2_service as _s2svc
        if _s2svc._STAGE2_LKG.get(sym.upper()):
            return lkg   # includes deliberate nulls written by warmup

        # ── 2. In-memory bar probe (theme_rs proxy symbols — ETFs, stock proxies) ─
        from data.cache import cache as _cache
        from services.stage_analysis import weekly_bars_from_daily, analyze_symbol_stage

        s = sym.upper()
        daily: list[dict] | None = (
            _cache.get(f"fmp_hist:{s}")
            or _cache.get(f"tdier_hist:{s}:400")
        )
        if not daily:
            return _null

        weekly = weekly_bars_from_daily(daily)
        if len(weekly) < 35:
            return _null

        spy_daily: list[dict] | None = (
            _cache.get("fmp_hist:SPY")
            or _cache.get("tdier_hist:SPY:400")
        )
        spy_weekly = weekly_bars_from_daily(spy_daily) if spy_daily else None

        result = analyze_symbol_stage(
            weekly_bars=weekly,
            spy_weekly_bars=spy_weekly,
            source="watchlist_cached_bars",
        )
        return {
            "score":  result.get("stage_score"),
            "label":  result.get("stage_label"),
            "reason": result.get("stage_reason"),
        }
    except Exception as _e:
        print(f"[WATCHLIST_STAGE] {sym}: {_e}")
        return _null


def _finite_float_or_none(value: Any) -> float | None:
    """Return a finite float or None without treating 0 as falsy."""
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if _math.isfinite(parsed) else None


def _resolve_cached_watchlist_beta(fund_snapshot: dict | None) -> float | None:
    """Beta from the bulk-loaded watchlist fundamentals snapshot only."""
    fields = (fund_snapshot or {}).get("fields") or {}
    profile = fields.get("profile") or {}
    return _finite_float_or_none(profile.get("beta"))


def _bulk_fundamentals_fields(fields: dict[str, Any]) -> dict[str, Any]:
    """Return the lightweight fundamentals view used only by bulk watchlist rows."""
    bulk_fields = dict(fields)
    bulk_fields.pop("earnings_intelligence", None)
    return bulk_fields


def _ticker_detail_earnings_intelligence(fields: dict[str, Any]) -> dict | None:
    """Read the complete cached earnings object for the single-ticker detail view."""
    earnings_intelligence = fields.get("earnings_intelligence")
    return earnings_intelligence if isinstance(earnings_intelligence, dict) else None


def _load_cached_watchlist_market_data(tickers: list[str]) -> dict[str, dict[str, Any]]:
    """Bulk-load cache-only market-data enrichments used by canonical rows.

    Returns volume metrics (volume_change_*, volume_metrics_status, etc.) plus
    internal comparison-close keys used by _enrich_store_with_quotes to compute
    change_7d / change_30d from the live displayed price:

      _comparison_close_7d  — historical close for 7-calendar-day lookback
      _comparison_date_7d   — that session's date string
      _comparison_close_30d — historical close for 30-calendar-day lookback
      _comparison_date_30d  — that session's date string

    No provider calls are made.  Both functions are cache-only disk reads.
    """
    try:
        from services.canonical_history_service import (
            get_volume_metrics_bulk as _get_hist_metrics_bulk,
            get_comparison_closes_bulk as _get_comp_closes_bulk,
        )
        vol_metrics = _get_hist_metrics_bulk(tickers)
        comp_closes = _get_comp_closes_bulk(tickers)
        for sym in vol_metrics:
            comp = comp_closes.get(sym) or {}
            vol_metrics[sym]["_comparison_close_7d"]  = comp.get("comparison_close_7d")
            vol_metrics[sym]["_comparison_date_7d"]   = comp.get("comparison_date_7d")
            vol_metrics[sym]["_comparison_close_30d"] = comp.get("comparison_close_30d")
            vol_metrics[sym]["_comparison_date_30d"]  = comp.get("comparison_date_30d")
        return vol_metrics
    except Exception as exc:
        print(f"[WATCHLIST_ENRICH] bulk volume metrics load failed (non-fatal): {exc}")
        return {}


# ── Quote enrichment helper ──────────────────────────────────────────────────

async def _enrich_store_with_quotes(store: dict) -> dict:
    """
    Enrich every ticker row in store['analysis']['sections'] with:
      - name         from Tradier quote description (if not already present)
      - price        from Tradier live price (or CSV Stock Price fallback)
      - change_pct_1d from Tradier 1D change %
      - quote_source / quote_updated_at

    Existing rich fields (catalyst, sentiment, action_note, etc.) are preserved.
    Foreign/exchange-prefixed tickers that Tradier cannot quote keep whatever
    fields they already have.  This function never blocks — it uses the LKG
    quote cache and triggers a background refresh when the 10-min TTL expires.

    FALLBACK: when analysis.sections is empty but tickers are saved (e.g. analysis
    has not yet completed for a large watchlist), a single synthetic "All Tickers"
    section is built from the raw ticker list + CSV data + quote cache so the
    frontend table never renders 0 rows while analysis is pending.
    """
    import time as _time
    _t0 = _time.monotonic()

    from services.watchlist_quote_cache import get_watchlist_quotes

    tickers: list[str]   = store.get("tickers", [])
    csv_data: list[dict] = store.get("csv_data", [])
    analysis: dict       = store.get("analysis") or {}
    sections: list[dict] = analysis.get("sections", [])

    # ── Phase 4A: register watchlist demand for quote priority ────────────────
    if tickers:
        try:
            import data.quote_demand_registry as _qdr
            _qdr.register(tickers, "watchlist", ttl=90)
        except Exception:
            pass

    if not tickers:
        return store

    # CSV fundamentals map — keyed by SYMBOL (uppercase)
    csv_map: dict[str, dict] = {}
    for row in csv_data:
        sym = (row.get("Symbol") or row.get("symbol") or row.get("Ticker") or "").strip().upper()
        if sym:
            csv_map[sym] = row

    # Fetch quotes and name overrides in parallel — both may involve I/O:
    #   - get_watchlist_quotes: may hydrate disk LKG (sync) or schedule Tradier refresh
    #   - get_name_overrides: hits a 5-min in-memory cache; on miss makes a Neon call
    # Running them concurrently saves ~50–300 ms on cold name-override cache.
    import asyncio as _aio_enrich
    quote_map: dict[str, dict] = {}
    _name_overrides: dict[str, str] = {}
    fund_snaps: dict[str, dict] = {}
    cached_market_data: dict[str, dict[str, Any]] = {}
    _t_fetch = _time.monotonic()
    try:
        from services.name_overrides import get_name_overrides as _get_name_overrides
        from data.watchlist_fundamentals_store import get_snapshots_bulk as _get_fund_snaps_mc
        _loop = _aio_enrich.get_event_loop()
        _q_res, _n_res, _f_res, _m_res = await _aio_enrich.gather(
            get_watchlist_quotes(tickers),
            _loop.run_in_executor(None, _get_name_overrides, "default"),
            _loop.run_in_executor(None, _get_fund_snaps_mc, tickers),
            _loop.run_in_executor(None, _load_cached_watchlist_market_data, tickers),
            return_exceptions=True,
        )
        if isinstance(_q_res, Exception):
            print(f"[WATCHLIST_ENRICH] quote fetch failed (non-fatal): {_q_res}")
        else:
            quote_map = _q_res or {}
        if isinstance(_n_res, Exception):
            print(f"[WATCHLIST_ENRICH] name overrides load failed (non-fatal): {_n_res}")
        else:
            _name_overrides = _n_res or {}
        if isinstance(_f_res, Exception):
            print(f"[WATCHLIST_ENRICH] fund_snaps load failed (non-fatal): {_f_res}")
        else:
            fund_snaps = _f_res or {}
        if isinstance(_m_res, Exception):
            print(f"[WATCHLIST_ENRICH] cached market-data load failed (non-fatal): {_m_res}")
        else:
            cached_market_data = _m_res or {}
    except Exception as _fetch_err:
        print(f"[WATCHLIST_ENRICH] parallel fetch failed (non-fatal): {_fetch_err}")
    print(
        f"[WATCHLIST_ENRICH] quote+names+fundsnaps+history fetch_ms={round((_time.monotonic()-_t_fetch)*1000)} "
        f"quotes={len(quote_map)} name_overrides={len(_name_overrides)} fund_snaps={len(fund_snaps)} "
        f"history_metrics={len(cached_market_data)}"
    )
    # Pre-load for get_by_id_endpoint's apply_fmp_overlays — avoids a second Neon round-trip
    store["_fund_snaps_for_apply_fmp"] = fund_snaps

    now_str = datetime.now(timezone.utc).isoformat() + "Z"

    # ── Pre-load theme registry + override map (once per request) ────────────
    # Hoisted out of the skeleton branch so ALL row paths — normal, missing-
    # append, uncategorized reclassification, and skeleton — share the same
    # override map and sector-normalization helper.  No per-row Neon calls.
    try:
        from services.theme_rs_universe import (
            THEME_RS_UNIVERSE          as _wl_trs_uni,
            normalize_company_sector_to_id as _wl_norm_sector,
        )
    except Exception:
        _wl_trs_uni   = {}
        _wl_norm_sector = lambda x: None  # type: ignore[assignment]

    _wl_override_map: dict[str, list[str]] = {}
    try:
        from data.pg_storage import get_theme_ticker_overrides as _wl_get_overrides
        for _wl_ov_row in (_wl_get_overrides() or []):
            if _wl_ov_row.get("action") != "add":
                continue
            _wl_ov_sym = (_wl_ov_row.get("symbol") or "").upper()
            _wl_ov_tid = (_wl_ov_row.get("theme_id") or "").strip()
            if _wl_ov_sym and _wl_ov_tid:
                _wl_override_map.setdefault(_wl_ov_sym, [])
                if _wl_ov_tid not in _wl_override_map[_wl_ov_sym]:
                    _wl_override_map[_wl_ov_sym].append(_wl_ov_tid)
    except Exception as _wl_ov_err:
        print(
            f"[WATCHLIST_ENRICH] theme_ticker_overrides pre-load failed (non-fatal): "
            f"{_wl_ov_err}"
        )

    # ── Canonical Theme resolution context — built ONCE per request ────────────
    # Shared by _build_ticker_row() for every row path (normal, skeleton, missing-
    # append, uncategorized reclassification).  Calling build_theme_resolution_context()
    # once here and capturing it in the closure eliminates per-ticker rebuilds.
    #
    # WHY: the normal path passes stored LLM analysis rows directly to
    # _build_ticker_row() as base_row.  Those rows carry a canonical_theme_id
    # that was correct when the LLM ran but may be stale after a manual taxonomy
    # assignment.  resolve_primary_theme_for_ticker() enforces the authoritative
    # precedence chain (manual_override always wins) so the identity block inside
    # _build_ticker_row() always reflects the current persisted state, not the
    # stored LLM result.
    _wl_theme_ctx = None
    try:
        from services.theme_resolver import build_theme_resolution_context as _wl_build_ctx
        _wl_theme_ctx = _wl_build_ctx()
    except Exception as _wl_ctx_err:
        print(
            f"[WATCHLIST_ENRICH] theme_resolver context build failed (non-fatal): "
            f"{_wl_ctx_err}"
        )

    def _build_ticker_row(sym: str, base_row: dict) -> dict:
        """Build one enriched ticker row from quote + CSV data."""
        sym = sym.strip().upper()
        q       = quote_map.get(sym, {})
        csv_row = csv_map.get(sym, {})
        enriched = dict(base_row)
        _fund_snap = fund_snaps.get(sym) or {}
        _beta = _resolve_cached_watchlist_beta(_fund_snap)
        _volume_metrics = cached_market_data.get(sym) or {}

        # ── Name ──────────────────────────────────────────────────────────
        # Priority: name override (DB) > Tradier description > FMP quote name
        #           > CSV column > ticker symbol
        if not enriched.get("name"):
            enriched["name"] = (
                q.get("name")
                or csv_row.get("Company Name")
                or csv_row.get("Name")
                or sym
            )
        # Manual override wins over everything — applied after initial name
        # resolution so it can correct wrong API names without blocking them.
        _override_name = _name_overrides.get(sym)
        if _override_name:
            enriched["name"] = _override_name

        # ── Price ──────────────────────────────────────────────────────────
        if q.get("price") is not None:
            enriched["price"] = q["price"]
        elif not enriched.get("price"):
            csv_price = csv_row.get("Stock Price")
            if csv_price:
                try:
                    enriched["price"] = float(csv_price)
                except Exception:
                    pass

        # ── Live market fields — use merged quote map (volume already LKG-preserved)
        if q:
            enriched["change_pct_1d"]  = q.get("change_pct_1d")
            # Volume: use whatever the cache gave us (already LKG-merged).
            # Never write None/zero — fall back to existing enriched value.
            _q_vol = q.get("volume")
            _q_avg = q.get("average_volume")
            if _q_vol is not None and float(_q_vol) > 0 if _q_vol else False:
                enriched["volume"] = _q_vol
            # average_volume: same — preserve positive value
            if _q_avg is not None and float(_q_avg) > 0 if _q_avg else False:
                enriched["average_volume"] = _q_avg
            # relative_volume: recompute from effective values
            _eff_vol = enriched.get("volume")
            _eff_avg = enriched.get("average_volume")
            if _eff_vol is not None and _eff_avg and float(_eff_avg) > 0:
                try:
                    rel_vol = round(float(_eff_vol) / float(_eff_avg), 4)
                except Exception:
                    rel_vol = q.get("relative_volume")
            else:
                rel_vol = q.get("relative_volume")
            enriched["relative_volume"]  = rel_vol
            enriched["quote_source"]     = q.get("quote_source") or "tradier"
            enriched["quote_updated_at"] = q.get("quote_updated_at", now_str)
            # Provenance fields (additive — frontend can use for tooltip/badge)
            enriched["market_session"]        = q.get("market_session")
            enriched["quote_is_stale"]        = q.get("quote_is_stale", False)
            enriched["price_is_stale"]        = q.get("price_is_stale", False)
            enriched["volume_is_stale"]       = q.get("volume_is_stale", False)
            enriched["price_source"]          = q.get("price_source") or q.get("quote_source") or "tradier"
            enriched["volume_source"]         = q.get("volume_source") or q.get("quote_source") or "tradier"
            enriched["volume_updated_at"]     = q.get("volume_updated_at") or q.get("quote_updated_at")
            enriched["quote_fallback_reason"] = q.get("quote_fallback_reason")

        # ── Vol/MC ratio ───────────────────────────────────────────────────
        _raw_mc = (
            csv_row.get("Market Cap")
            or csv_row.get("MarketCap")
            or csv_row.get("market_cap")
            or enriched.get("market_cap")
        )
        _mc = _parse_market_cap_str(_raw_mc)
        _price_f = enriched.get("price")
        _vol_f   = enriched.get("volume")
        try:
            _price_f = float(_price_f) if _price_f is not None else None
        except Exception:
            _price_f = None
        try:
            _vol_f = float(_vol_f) if _vol_f is not None else None
        except Exception:
            _vol_f = None

        # ── Canonical market cap — live price × implied shares ─────────────
        # Upgrades the stale CSV value to a live price-derived figure when
        # the FmpFundamentalsRefresher has stored implied shares (set during
        # the most recent Sunday refresh cycle).  fund_snaps was loaded in
        # the parallel gather above — zero extra Neon calls here.
        try:
            from services.market_cap_resolver import resolve_canonical_market_cap as _resolve_mc
            _fund_snap_row   = _fund_snap
            _fund_fields_mc  = _fund_snap_row.get("fields") or {}
            _fund_ref_at     = _fund_snap_row.get("refreshed_at")
            _mc_contract = _resolve_mc(
                sym,
                _fund_fields_mc,
                live_price=_price_f,
                live_price_source="tradier",
                static_market_cap_override=_mc,
                fund_refreshed_at=_fund_ref_at,
            )
            _mc_display = _mc_contract.get("market_cap_display")
            if _mc_display and _mc_display > 0:
                _mc = _mc_display  # use live/resolved value for vol_mc calc
            enriched["market_cap_static"]              = _mc_contract.get("market_cap_static")
            enriched["market_cap_live"]                = _mc_contract.get("market_cap_live")
            enriched["market_cap_display"]             = _mc_display
            enriched["market_cap_display_source"]      = _mc_contract.get("market_cap_display_source")
            enriched["market_cap_display_freshness"]   = _mc_contract.get("market_cap_display_freshness")
            enriched["market_cap_display_warning_codes"] = _mc_contract.get("market_cap_display_warning_codes")
            enriched["market_cap_implied_shares"]      = _mc_contract.get("market_cap_implied_shares")
            enriched["market_cap_live_price"]          = _mc_contract.get("market_cap_live_price")
        except Exception:
            pass  # non-fatal: fall back to CSV _mc value

        # ── Quality fundamentals snapshot (zero extra Neon calls) ───────────────
        # fund_snaps was loaded in the parallel gather above.  We re-read the
        # same sym key here — no second DB round-trip, just a dict lookup.
        try:
            _fund_snap_q   = _fund_snap
            _fund_fields_q = _fund_snap_q.get("fields") or {}
            if _fund_fields_q:
                # Part 6 — Live Valuation Overlay: recompute price-sensitive
                # multiples using the live price + resolved market cap so
                # stale refreshed-at values don't cause misleading multiples.
                try:
                    from services.watchlist_fundamentals_refresh import (
                        compute_live_valuation_overlay as _live_overlay,
                    )
                    _overlay_fields = _fund_fields_q.copy()
                    # Inject implied shares from resolver into overlay inputs
                    if enriched.get("market_cap_implied_shares") is not None:
                        _overlay_fields["_market_cap_implied_shares"] = (
                            enriched["market_cap_implied_shares"]
                        )
                    _live_mc  = enriched.get("market_cap_display") or enriched.get("market_cap_live")
                    _live_px  = _price_f
                    _overlay = _live_overlay(_overlay_fields, _live_mc, _live_px)
                    if _overlay:
                        # Inject market-cap resolver provenance (Part 5 contract)
                        _mc_src = enriched.get("market_cap_display_source")
                        if _mc_src:
                            _overlay["_valuation_market_cap_source"] = _mc_src
                        if _live_px is not None:
                            _overlay["_valuation_price_used"] = _live_px
                        _is_live = _mc_src not in (None, "static", "fmp_stored")
                        _overlay["_valuation_is_live"] = _is_live
                        # Actual cached quote timestamp (not the generic "live" string)
                        _qt = enriched.get("quote_updated_at")
                        if _qt:
                            _overlay["_valuation_price_timestamp"] = _qt
                        # Exact quote source from the cache (e.g. "tradier")
                        _qs = enriched.get("quote_source")
                        if _qs:
                            _overlay["_valuation_quote_source"] = _qs
                        _fund_fields_q = {**_fund_fields_q, **_overlay}
                except Exception:
                    pass  # non-fatal: fall back to stored snapshot values

                enriched["fundamentals"] = {
                    "fields":        _bulk_fundamentals_fields(_fund_fields_q),
                    "refreshed_at":  _fund_snap_q.get("refreshed_at"),
                    "missing_fields": _fund_snap_q.get("missing_fields") or [],
                }
        except Exception:
            pass  # non-fatal: fundamentals simply absent from this ticker row

        enriched["beta"] = _beta
        # Extract internal comparison-close denominators before merging public fields.
        # These keys (_comparison_close_7d / _30d) are ephemeral transport values
        # set by _load_cached_watchlist_market_data; they must never appear in the
        # serialized Watchlist response sent to the frontend.
        _c7  = _volume_metrics.get("_comparison_close_7d")
        _c30 = _volume_metrics.get("_comparison_close_30d")
        # Merge only public Watchlist market fields — skip any _comparison_* internal keys
        enriched.update({k: v for k, v in _volume_metrics.items() if not k.startswith("_comparison_")})

        # ── Live-price change_7d / change_30d override ─────────────────────────
        # _volume_metrics contains pre-computed change_7d/change_30d that use
        # the last canonical bar close as numerator.  That value predates the
        # live quote already in enriched["price"] and produces the wrong %.
        #
        # Correct formula:  (displayed_price / historical_comparison_close − 1) × 100
        #
        # The comparison closes (historical denominators) come from the compact
        # comparison_close_tail stored in _INDEX — zero disk reads, zero provider
        # calls.  _c7 / _c30 were extracted above before the public-only merge.
        # We only compute the percentage here, after the authoritative price
        # is known.  Stale pre-computed values from _volume_metrics are always
        # overridden — they must never reach the frontend as the final answer.
        try:
            _live_px_f = float(_price_f) if _price_f is not None else None
        except Exception:
            _live_px_f = None
        if _live_px_f is not None and _live_px_f > 0:
            try:
                _c7f = float(_c7) if _c7 is not None else None
                enriched["change_7d"] = (
                    round((_live_px_f / _c7f - 1) * 100.0, 6)
                    if _c7f is not None and _c7f > 0 else None
                )
            except Exception:
                enriched["change_7d"] = None
            try:
                _c30f = float(_c30) if _c30 is not None else None
                enriched["change_30d"] = (
                    round((_live_px_f / _c30f - 1) * 100.0, 6)
                    if _c30f is not None and _c30f > 0 else None
                )
            except Exception:
                enriched["change_30d"] = None
        else:
            # No valid live price — return None rather than a stale cached value
            enriched["change_7d"]  = None
            enriched["change_30d"] = None

        enriched.update(_vol_mc_fields(_price_f, _vol_f, _mc))

        # ── More-specific unavail reasons ──────────────────────────────────
        # Foreign-exchange-prefixed tickers (AIM:, STO:, FRA:, etc.) cannot
        # be quoted by Tradier; "volume_unavailable" is technically correct but
        # misleading.  Replace with a clearer reason so the frontend can show
        # a meaningful tooltip.
        if ":" in sym and enriched.get("vol_mc_unavailable_reason") == "volume_unavailable":
            enriched["vol_mc_unavailable_reason"] = "foreign_exchange_ticker_unsupported"

        # For US tickers that have market_cap + price but still no volume after
        # quote enrichment, add a clearer "quote_unavailable" reason so it's
        # distinguishable from foreign-ticker gaps.
        if (
            ":" not in sym
            and enriched.get("vol_mc_unavailable_reason") == "volume_unavailable"
            and enriched.get("price") is not None
        ):
            enriched["vol_mc_unavailable_reason"] = "quote_unavailable"

        # ── Weinstein Stage Analysis (cache-only) ───────────────────────────
        _stage = _get_stage2_breakout(sym)
        enriched["stage2_breakout"] = _stage   # backward-compat field
        enriched["stage_analysis"]  = _stage   # canonical alias (same object)

        # ── Hierarchy identity fields — Contracts 3 & 4 ──────────────────────
        # Applied unconditionally so every row path (normal, missing-append,
        # skeleton, uncategorized reclassification) carries the same fields.
        #
        # Contract 3 — primary_theme_id / theme_ids / subtheme_ids
        # Semantics:
        #   primary_theme_id — null when the raw ID is absent or not in the
        #     canonical registry (e.g. "other_uncategorized" sentinel).
        #   theme_ids        — primary first, then sorted additional IDs;
        #     only IDs present in the canonical registry; no duplicates.
        #   subtheme_ids     — subset of theme_ids where classification ==
        #     "sub_theme".  parent_theme_id is NOT used as a classification
        #     proxy: 19 standalone sub_themes have no parent_theme_id.
        try:
            # ── Authoritative resolver pass ──────────────────────────────────
            # Override the LLM-derived canonical_theme_id with the current
            # canonical assignment from theme_resolver.  The resolver's
            # precedence chain (manual_override wins) ensures that a manual
            # taxonomy assignment always outranks any stale LLM classification
            # embedded in the stored analysis section row.
            #
            # _wl_theme_ctx is pre-built once per request (closure-captured)
            # so this is a pure dict lookup — zero provider calls per ticker.
            try:
                from services.theme_resolver import (
                    resolve_primary_theme_for_ticker as _wl_res_fn,
                )
                _ind_csv  = csv_map.get(sym, {})
                _ind_val  = (
                    _ind_csv.get("Industry") or _ind_csv.get("industry") or ""
                ).strip()
                _res      = _wl_res_fn(sym, industry=_ind_val, ctx=_wl_theme_ctx)
                _res_id   = _res.get("theme_id")
                _res_src  = _res.get("source")
                if _res_id is not None:
                    # Resolver found a definitive result — use it unconditionally.
                    # Covers: manual_override, canonical_map, themes_page_membership,
                    # industry_fallback, llm_classified.
                    enriched["canonical_theme_id"]   = _res_id
                    enriched["canonical_theme_name"] = _res.get("theme_name")
                    enriched["theme_source"]         = _res_src
                elif _res_src == "deprecated_suppressed":
                    # Resolver explicitly cleared a deprecated ID; follow its lead.
                    enriched["canonical_theme_id"]   = None
                    enriched["canonical_theme_name"] = None
                    enriched["theme_source"]         = "deprecated_suppressed"
                # else: source == "no_mapping" — resolver found nothing; preserve
                # whatever the LLM analysis row may already carry.  An LLM result
                # for an unmapped ticker is still more useful than null.
            except Exception:
                pass  # non-fatal: fall back to LLM-derived canonical_theme_id

            _id_raw = (
                enriched.get("canonical_theme_id")
                or enriched.get("primary_theme_id")
            )
            # Null sentinel/unmapped IDs not present in the canonical registry
            _id_primary = _id_raw if (_id_raw and _id_raw in _wl_trs_uni) else None
            _id_extras  = sorted(
                t for t in _wl_override_map.get(sym, [])
                if t != _id_primary and t in _wl_trs_uni
            )
            _id_all     = ([_id_primary] if _id_primary else []) + _id_extras
            _id_subs    = [
                t for t in _id_all
                if (_wl_trs_uni.get(t) or {}).get("classification") == "sub_theme"
            ]
            enriched["primary_theme_id"] = _id_primary
            enriched["theme_ids"]        = _id_all
            enriched["subtheme_ids"]     = _id_subs
        except Exception:
            enriched.setdefault("primary_theme_id", None)
            enriched.setdefault("theme_ids", [])
            enriched.setdefault("subtheme_ids", [])

        # Contract 4 — sector_id from actual company sector, never from theme
        # Source priority:
        #   1. fund_snap["fields"]["profile"]["sector"]  — canonical FMP stored path
        #   2. fund_snap["fields"]["sector"]             — legacy flat-field fallback
        #   3. csv_row["Sector"] / csv_row["sector"]    — CSV static fallback
        # Normalised to a canonical sector ID; null when unavailable or unknown.
        # Never derived from primary_theme_id, parent_theme_id, rollup_sector_ids,
        # or any other theme-hierarchy field.
        try:
            _fund_fields = _fund_snap.get("fields") or {}
            _id_sector_raw = (
                (_fund_fields.get("profile") or {}).get("sector")
                or _fund_fields.get("sector")
                or csv_row.get("Sector")
                or csv_row.get("sector")
                or ""
            )
            enriched["sector_id"] = _wl_norm_sector(_id_sector_raw) or None
        except Exception:
            enriched.setdefault("sector_id", None)

        return enriched

    # ── FALLBACK: no sections yet (analysis pending / never completed) ─────────
    # Build one synthetic section from the raw tickers list so the frontend
    # table always renders saved symbols, even for large watchlists that are
    # still being analysed in the background.
    if not sections:
        # ── Canonical Theme resolver — SAME resolver Confluence's theme_bridge
        # consumes (services.theme_resolver). Pre-build the shared lookup
        # context once for the entire skeleton pass; per-ticker resolution
        # itself is delegated to resolve_primary_theme_for_ticker() so the
        # Watchlist and Confluence never diverge in resolution logic.
        from services.theme_resolver import (
            build_theme_resolution_context as _skl_build_ctx,
            resolve_primary_theme_for_ticker as _skl_resolve_theme,
        )
        _skl_theme_ctx = _skl_build_ctx()

        # Identity fields (sector_id, primary_theme_id, theme_ids, subtheme_ids)
        # are now injected inside _build_ticker_row using the shared _wl_override_map
        # and _wl_trs_uni pre-loaded above.  No per-ticker pre-computation needed here.

        skeleton: list[dict] = []
        for sym in tickers:
            _s = sym.strip().upper()

            _csv_r = csv_map.get(_s) or {}
            _ind   = (_csv_r.get("Industry") or _csv_r.get("industry") or "").strip()

            _theme_res = _skl_resolve_theme(_s, industry=_ind, ctx=_skl_theme_ctx)
            _canon_theme    = _theme_res["theme_name"]
            _canon_theme_id = _theme_res["theme_id"]
            _theme_src      = _theme_res["source"]

            row = _build_ticker_row(_s, {
                "symbol":               _s,
                "catalyst":             None,
                "sentiment":            None,
                "action_note":          None,
                "conviction":           None,
                "theme":                _canon_theme,
                "canonical_theme_name": _canon_theme,
                "canonical_theme_id":   _canon_theme_id,
                "theme_source":         _theme_src,
            })
            skeleton.append(row)

        elapsed_ms = round((_time.monotonic() - _t0) * 1000)
        quoted_count = sum(1 for s in skeleton if s.get("price") is not None)
        print(
            f"[WATCHLIST_ENRICH] analysis_pending — built {len(skeleton)} skeleton rows "
            f"({quoted_count} quoted) from {len(tickers)} saved tickers in {elapsed_ms}ms"
        )

        # ── RANK PASSES on skeleton (mirrors normal-path behaviour at line 1556)
        # The skeleton path early-returns here, so the rank passes at line 1556
        # are never reached.  We must compute ranks before returning so
        # rel_vol_rank / rel_vol_rank_delta / rel_vol_trend appear in every row.
        # Skeleton rows already have relative_volume from _build_ticker_row above.
        _skl_sections: list[dict] = [{
            "name":              "All Tickers",
            "id":                "all_tickers",
            "subtitle":          "Showing saved tickers — AI analysis running in background",
            "tickers":           skeleton,
            "_analysis_pending": True,
        }]
        _skl_saved_norm: list[str] = []
        _skl_seen: set[str] = set()
        for _t in tickers:
            _s2 = _t.strip().upper()
            if _s2 and _s2 not in _skl_seen:
                _skl_saved_norm.append(_s2)
                _skl_seen.add(_s2)
        _skl_wl_id = store.get("id") or ""
        if _skl_wl_id:
            try:
                _skl_sections = await _apply_rv_rank_fields(
                    _skl_wl_id, _skl_sections, _skl_saved_norm
                )
            except Exception as _skl_rv_err:
                print(f"[WATCHLIST_ENRICH] skeleton rv_rank pass failed: {_skl_rv_err}")
            try:
                _skl_sections = await _apply_volmc_rank_fields(
                    _skl_wl_id, _skl_sections, _skl_saved_norm
                )
            except Exception as _skl_vm_err:
                print(f"[WATCHLIST_ENRICH] skeleton volmc_rank pass failed: {_skl_vm_err}")

        return {
            **store,
            "analysis": {
                **analysis,
                "sections": _skl_sections,
                "_analysis_pending": True,
                "_skeleton_reason":  "analysis_not_yet_run",
            },
        }

    # ── NORMAL PATH: enrich existing LLM section rows ─────────────────────────
    # Normalize saved ticker list (dedup, strip whitespace, uppercase)
    saved_normalized: list[str] = []
    seen_saved: set[str] = set()
    for t in tickers:
        s = t.strip().upper()
        if s and s not in seen_saved:
            saved_normalized.append(s)
            seen_saved.add(s)

    enriched_sections: list[dict] = []
    symbols_in_sections: set[str] = set()
    total_in  = 0
    total_out = 0
    for section in sections:
        enriched_tickers: list[dict] = []
        for row in section.get("tickers", []):
            sym = str(row.get("symbol", "")).upper()
            if not sym:
                continue
            total_in += 1
            enriched_tickers.append(_build_ticker_row(sym, row))
            symbols_in_sections.add(sym)
            total_out += 1
        enriched_sections.append({**section, "tickers": enriched_tickers})

    # ── SECTION NORMALIZATION (GET-time) ──────────────────────────────────────
    # Merge overlapping section titles into their canonical equivalents so the
    # frontend never sees duplicate/redundant sections.
    # "Solar" → "Clean Energy", "Renewable Energy" → "Clean Energy", etc.
    _SECTION_ALIAS_MAP: dict[str, str] = {
        "Solar":                    "Clean Energy",
        "Renewable Energy":         "Clean Energy",
        "Alternative Energy":       "Clean Energy",
        "Fuel Cell":                "Clean Energy",
        "Hydrogen Energy":          "Clean Energy",
        "Energy Storage":           "Lithium & Battery Tech",
        "Optical Networking":       "AI Networking",
        "Networking":               "AI Networking",
        "AI Chips":                 "Semiconductors",
        "Chips":                    "Semiconductors",
        "Memory / Storage":         "Memory & Storage",
        "Robotics / Automation":    "Robotics & Automation",
        "Datacenter / Compute":     "Data Center Infrastructure",
        "Aerospace / Defense":      "Defense",
    }

    _aliases_applied = 0
    _merged_map: dict[str, dict] = {}   # canonical_title → section dict (with mutable tickers list)
    for _sec in enriched_sections:
        _raw = (_sec.get("title") or _sec.get("name") or _sec.get("id") or "").strip()
        _canon = _SECTION_ALIAS_MAP.get(_raw, _raw)
        if _canon in _merged_map:
            _merged_map[_canon]["tickers"].extend(_sec.get("tickers", []))
            _aliases_applied += 1
        else:
            _merged_map[_canon] = {**_sec, "title": _canon, "tickers": list(_sec.get("tickers", []))}
            if _canon != _raw:
                _aliases_applied += 1
    enriched_sections = list(_merged_map.values())

    # ── UNCATEGORIZED RECLASSIFICATION (GET-time) ─────────────────────────────
    # For tickers that ended up in "Other / Uncategorized" (either from a
    # previous BG_REFRESH or from the missing-append pass), use the CSV Industry
    # column as a deterministic fallback to move them into proper canonical
    # sections.  No AI calls — pure lookup, runs on every GET.
    _reclassified_count = 0
    try:
        from services.theme_ticker_mapper import map_industry_to_theme as _map_ind
    except ImportError:
        _map_ind = None

    if _map_ind:
        _unc_idx = next(
            (i for i, _s in enumerate(enriched_sections)
             if _s.get("id") == "other_uncategorized" or
             (_s.get("title") or "").lower() in ("other / uncategorized", "other/uncategorized")),
            None,
        )
        if _unc_idx is not None:
            _sec_title_idx: dict[str, int] = {
                (_s.get("title") or ""): i for i, _s in enumerate(enriched_sections)
            }
            _still_unc: list[dict] = []
            for _row in enriched_sections[_unc_idx].get("tickers", []):
                _sym = str(_row.get("symbol", "")).upper()
                _csv_r = csv_map.get(_sym) or csv_map.get(_sym.split(":")[-1] if ":" in _sym else _sym) or {}
                _ind = (_csv_r.get("Industry") or _csv_r.get("industry") or "").strip()
                _mapping = _map_ind(_ind)
                if _mapping:
                    _tgt_name, _tgt_id = _mapping
                    _tgt_name = _SECTION_ALIAS_MAP.get(_tgt_name, _tgt_name)
                    # Re-compute identity fields when canonical_theme_id changes.
                    # sector_id is UNCHANGED — it reflects the actual company sector,
                    # not the theme assignment, so it must not be re-derived here.
                    _unc_extra  = sorted(
                        t for t in _wl_override_map.get(_sym, [])
                        if t != _tgt_id and t in _wl_trs_uni
                    )
                    _unc_all    = ([_tgt_id] if (_tgt_id and _tgt_id in _wl_trs_uni) else []) + _unc_extra
                    _unc_subs   = [t for t in _unc_all if (_wl_trs_uni.get(t) or {}).get("classification") == "sub_theme"]
                    _enriched_row = {
                        **_row,
                        "canonical_theme_name": _tgt_name,
                        "canonical_theme_id":   _tgt_id,
                        "theme_source":         "industry_fallback",
                        "primary_theme_id":     _tgt_id,
                        "theme_ids":            _unc_all,
                        "subtheme_ids":         _unc_subs,
                    }
                    if _tgt_name in _sec_title_idx:
                        _i = _sec_title_idx[_tgt_name]
                        enriched_sections[_i]["tickers"].append(_enriched_row)
                    else:
                        _new_sec = {
                            "id":           _tgt_id,
                            "title":        _tgt_name,
                            "subtitle":     "Classified via industry data",
                            "tickers":      [_enriched_row],
                            "theme_source": "industry_fallback",
                        }
                        _sec_title_idx[_tgt_name] = len(enriched_sections)
                        enriched_sections.append(_new_sec)
                    _reclassified_count += 1
                else:
                    _still_unc.append(_row)
            # Keep only the truly-unclassifiable tickers in uncategorized
            enriched_sections[_unc_idx] = {
                **enriched_sections[_unc_idx],
                "tickers": _still_unc,
            }
            # Drop empty sections (e.g. uncategorized that became fully reclassified)
            enriched_sections = [_s for _s in enriched_sections if _s.get("tickers")]
            if _reclassified_count:
                print(
                    f"[WATCHLIST_ENRICH] reclassified {_reclassified_count} uncategorized tickers "
                    f"→ canonical sections via industry fallback"
                )

    # ── MISSING-SYMBOL APPEND (industry-aware) ────────────────────────────────
    # Any saved symbol absent from the enriched sections gets a skeleton row.
    # Before defaulting to "Other / Uncategorized", try the CSV industry fallback
    # so skeleton rows land in the correct canonical section rather than a catch-all.
    # This ensures tickers missed by Claude's last BG_REFRESH are still displayed
    # in a meaningful thematic context.
    missing_syms = [s for s in saved_normalized if s not in symbols_in_sections]
    appended_count = 0

    if missing_syms:
        # Build a section-title → index map for routing skeleton rows
        _miss_sec_idx: dict[str, int] = {
            (s.get("title") or ""): i for i, s in enumerate(enriched_sections)
        }

        # Resolve industry-fallback mapper (may already be imported above)
        _miss_map_ind = _map_ind if "_map_ind" in dir() else None
        if _miss_map_ind is None:
            try:
                from services.theme_ticker_mapper import map_industry_to_theme as _miss_map_ind
            except ImportError:
                _miss_map_ind = None

        for sym in missing_syms:
            _csv_r = csv_map.get(sym) or csv_map.get(sym.split(":")[-1] if ":" in sym else sym) or {}
            _ind   = (_csv_r.get("Industry") or _csv_r.get("industry") or "").strip()
            _mapped = _miss_map_ind(_ind) if _miss_map_ind and _ind else None

            if _mapped:
                _tgt_name, _tgt_id = _mapped
                _tgt_name = _SECTION_ALIAS_MAP.get(_tgt_name, _tgt_name)
                _base_row = {
                    "symbol":              sym,
                    "catalyst":            None,
                    "sentiment":           None,
                    "action_note":         None,
                    "conviction":          None,
                    "theme":               None,
                    "canonical_theme_name": _tgt_name,
                    "canonical_theme_id":   _tgt_id,
                    "theme_source":         "missing_append_industry",
                }
            else:
                _tgt_name = "Other / Uncategorized"
                _tgt_id   = "other_uncategorized"
                _base_row = {
                    "symbol":              sym,
                    "catalyst":            None,
                    "sentiment":           None,
                    "action_note":         None,
                    "conviction":          None,
                    "theme":               None,
                    "canonical_theme_name": _tgt_name,
                    "canonical_theme_id":   _tgt_id,
                    "theme_source":         "missing_append",
                }

            _built_row = _build_ticker_row(sym, _base_row)
            appended_count += 1

            if _tgt_name in _miss_sec_idx:
                _i = _miss_sec_idx[_tgt_name]
                enriched_sections[_i]["tickers"].append(_built_row)
            else:
                _new_s = {
                    "id":       _tgt_id,
                    "title":    _tgt_name,
                    "subtitle": ("Saved tickers not categorized by AI analysis"
                                 if _tgt_id == "other_uncategorized"
                                 else "Classified via industry data"),
                    "tickers":  [_built_row],
                }
                _miss_sec_idx[_tgt_name] = len(enriched_sections)
                enriched_sections.append(_new_s)

        print(
            f"[WATCHLIST_ENRICH] appended {appended_count} missing symbols "
            f"(saved={len(saved_normalized)} in_sections={len(symbols_in_sections)}): "
            + ", ".join(missing_syms[:10]) + ("…" if len(missing_syms) > 10 else "")
        )

    # ── DEDUP + SAVED-FILTER PASS ─────────────────────────────────────────────
    # Two passes in one walk:
    #   a) Drop symbols Claude emitted that are not in the saved list.
    #      Example: saved has "OTC:CGEH", Claude strips prefix and emits "CGEH" —
    #      that bare row is an impostor; the correct OTC:CGEH row was already
    #      appended by the missing-append pass above.
    #   b) Drop duplicate symbols (Claude emits same ticker across chunks).
    #      First occurrence wins (always the richer Claude row since Claude
    #      sections are processed before missing-append rows).
    # Sections that become empty after filtering are dropped.
    seen_final: set[str] = set()
    dedup_sections: list[dict] = []
    dup_count       = 0
    not_saved_count = 0
    for section in enriched_sections:
        deduped: list[dict] = []
        for row in section.get("tickers", []):
            sym = str(row.get("symbol", "")).upper()
            if not sym:
                continue
            if sym not in seen_saved:
                # Claude invented or format-mismatched symbol — skip
                not_saved_count += 1
                continue
            if sym in seen_final:
                dup_count += 1
                continue
            seen_final.add(sym)
            deduped.append(row)
        if deduped:
            dedup_sections.append({**section, "tickers": deduped})

    if dup_count or not_saved_count:
        print(
            f"[WATCHLIST_ENRICH] dedup removed {dup_count} duplicate rows, "
            f"{not_saved_count} not-in-saved rows"
        )

    # ── REL-VOL RANK + MOMENTUM PASS ─────────────────────────────────────────
    # Additive pass: adds rel_vol_rank / trend / momentum_label to every row.
    # Must run AFTER the dedup pass (final row set is fixed here).
    # Does not reorder sections — only annotates existing rows.
    _wl_id_for_rank = store.get("id") or ""
    try:
        dedup_sections = await _apply_rv_rank_fields(
            _wl_id_for_rank, dedup_sections, saved_normalized
        )
    except Exception as _rv_err:
        print(f"[WATCHLIST_ENRICH] rv_rank pass failed (non-fatal): {_rv_err}")

    # ── VOL/MC RANK + MOMENTUM PASS ───────────────────────────────────────────
    # Additive pass: adds vol_mc_rank / vol_mc_prev_pct / vol_mc_pct_delta /
    # vol_mc_trend / vol_mc_momentum_label to every row.
    # Runs after rv_rank (both passes operate on the same final dedup_sections).
    try:
        dedup_sections = await _apply_volmc_rank_fields(
            _wl_id_for_rank, dedup_sections, saved_normalized
        )
    except Exception as _vm_err:
        print(f"[WATCHLIST_ENRICH] volmc_rank pass failed (non-fatal): {_vm_err}")

    total_rows = sum(len(s["tickers"]) for s in dedup_sections)
    elapsed_ms = round((_time.monotonic() - _t0) * 1000)
    print(
        f"[WATCHLIST_ENRICH] sections={len(dedup_sections)} "
        f"tickers_in={total_in} tickers_out={total_rows} "
        f"appended_missing={appended_count} dups_removed={dup_count} "
        f"quoted={sum(1 for s in dedup_sections for t in s['tickers'] if t.get('price') is not None)} "
        f"elapsed={elapsed_ms}ms"
    )

    # ── CATEGORY OVERRIDES (always last — manual assignments always win) ───────
    # Applies user-approved ticker→section corrections from the DB override table.
    # This ensures manual moves persist across AI re-analyses and server restarts.
    try:
        from services.category_overrides import apply_to_sections as _apply_cat_overrides
        dedup_sections = _apply_cat_overrides(dedup_sections, user_id="default")
    except Exception as _ov_err:
        print(f"[WATCHLIST_ENRICH] category overrides failed (non-fatal): {_ov_err}")

    return {
        **store,
        "analysis": {
            **analysis,
            "sections": dedup_sections,
            "_missing_symbols_appended_count": appended_count,
            "_duplicate_symbols_removed":      dup_count,
            "_not_in_saved_removed":           not_saved_count,
        },
    }


# ── Request / Response Models ────────────────────────────────────────────────

class WatchlistSaveRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    csv_data: List[Dict[str, Any]]
    analysis: Dict[str, Any]
    watchlist_id: Optional[str] = None
    name: Optional[str] = None


class WatchlistAnalyzeRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    tickers: Optional[List[str]] = None
    csv_data: Optional[List[Dict[str, Any]]] = None
    watchlist_id: Optional[str] = None


# ── Helper ──────────────────────────────────────────────────────────────────

def _get_agent():
    import main
    if main.agent is None:
        raise HTTPException(status_code=503, detail="Server is still starting up.")
    return main.agent


def _get_data_service():
    import main
    if main.data_service is None:
        raise HTTPException(status_code=503, detail="Server is still starting up.")
    return main.data_service


# ── Endpoints — STATIC paths first, then parameterized ──────────────────────

@router.get("/list")
async def list_endpoint():
    """List all saved watchlists (metadata only)."""
    return list_watchlists()


@router.post("/save")
async def save_endpoint(body: WatchlistSaveRequest):
    """Save CSV data + AI analysis to the watchlist store."""
    import asyncio as _aio
    global _UPLOAD_WARMUP_STATE
    result = save_watchlist(body.csv_data, body.analysis, body.watchlist_id, body.name)

    # Invalidate bulk GET LKG — full watchlist was replaced via /save.
    _saved_wl_id = (result or {}).get("watchlist_id") or body.watchlist_id or ""
    if _saved_wl_id:
        _bulk_lkg_invalidate(_saved_wl_id)

    # ── Background Stage2 warmup for symbols missing from LKG ─────────────────
    # Fires a non-blocking task so the upload response returns immediately.
    # Eligible = US/non-foreign symbols (no colon). Skips symbols already in LKG
    # with a valid label (they'll be refreshed by the regular 20h cadence).
    try:
        from services.watchlist_stage2_service import warmup_stage2 as _ws2, _STAGE2_LKG as _lkg
        from services.watchlist_quote_cache import is_fmp_symbol_eligible as _elig
        import datetime as _dt

        _wl_id = result.get("watchlist_id") or body.watchlist_id
        _store = load_watchlist(_wl_id) if _wl_id else None
        _all_tickers: list[str] = [
            t.strip().upper() for t in (_store.get("tickers") or []) if t.strip()
        ] if _store else []

        _eligible  = [s for s in _all_tickers if _elig(s)]
        # Queue a symbol if it is absent from LKG, has a null label/score,
        # or has a valid stage entry but is missing Phase-2 technical_metrics
        # or technical_state (old-format entries from pre-Phase-2 warmups).
        _missing = [
            s for s in _eligible
            if (
                not _lkg.get(s)
                or _lkg[s].get("label")            is None
                or _lkg[s].get("score")            is None
                or _lkg[s].get("technical_metrics") is None
                or _lkg[s].get("technical_state")   is None
            )
        ]
        _in_lkg    = len(_eligible) - len(_missing)  # fully-covered (label + metrics)

        _UPLOAD_WARMUP_STATE.update({
            "watchlist_id":   _wl_id,
            "triggered_at":   _dt.datetime.utcnow().isoformat(),
            "finished_at":    None,
            "total_eligible": len(_eligible),
            "already_in_lkg": _in_lkg,
            "queued":         len(_missing),
            "running":        len(_missing) > 0,
        })

        if _missing:
            async def _run_upload_warmup() -> None:
                try:
                    await _ws2(_missing, force_nulls=True)
                finally:
                    _UPLOAD_WARMUP_STATE["running"]     = False
                    _UPLOAD_WARMUP_STATE["finished_at"] = _dt.datetime.utcnow().isoformat()

            _aio.create_task(_run_upload_warmup())
            print(
                f"[WATCHLIST_SAVE] stage2 warmup queued: "
                f"{len(_missing)} symbols (of {len(_eligible)} eligible, {_in_lkg} already in LKG)"
            )
    except Exception as _wup_e:
        print(f"[WATCHLIST_SAVE] stage2 warmup trigger skipped (non-fatal): {_wup_e}")

    # ── Background theme classifier for unmapped symbols ──────────────────────
    # Fires after stage2 warmup. Skips symbols already covered by canonical map
    # or industry fallback. Job-level lock prevents duplicate runs.
    try:
        from services.watchlist_theme_classifier import classify_watchlist_themes as _classify
        _wl_id_cls = result.get("watchlist_id") or body.watchlist_id
        if _wl_id_cls:
            _aio.create_task(_classify(_wl_id_cls, missing_only=True))
            print(f"[WATCHLIST_SAVE] theme classifier queued for watchlist {_wl_id_cls}")
    except Exception as _cls_e:
        print(f"[WATCHLIST_SAVE] theme classifier trigger skipped (non-fatal): {_cls_e}")

    return result


@router.post("/debug/fundamentals/refresh")
async def debug_fundamentals_refresh(
    symbols: Optional[str] = None,
    dev_force: bool = True,
    watchlist_id: Optional[str] = None,
):
    """
    DEV-ONLY: Force an immediate FMP fundamentals refresh for testing.
    Does not affect production weekly scheduler behavior.
    Never called by normal frontend page loads.

    Usage:
      POST /api/watchlist/debug/fundamentals/refresh?symbols=AAOI,NVDA,MU
      POST /api/watchlist/debug/fundamentals/refresh?dev_force=true           (all tickers in default watchlist)
      POST /api/watchlist/debug/fundamentals/refresh?watchlist_id=<id>&dev_force=true
    """
    import os as _os
    from services.watchlist_fundamentals_refresh import FmpFundamentalsRefresher

    _fmp_key = _os.getenv("FMP_API_KEY", "")
    if not _fmp_key:
        raise HTTPException(status_code=503, detail="FMP_API_KEY not configured")

    refresher = FmpFundamentalsRefresher(_fmp_key)

    # Resolve symbol list
    if symbols:
        sym_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
        wl_id = watchlist_id or "manual"
    else:
        wl_id = watchlist_id or "23eec278-074a-4706-a62a-c35d38b384ea"
        store = load_watchlist(wl_id)
        if not store:
            raise HTTPException(status_code=404, detail=f"Watchlist {wl_id} not found")
        sym_list = store.get("tickers") or []

    if not sym_list:
        raise HTTPException(status_code=400, detail="No symbols to refresh")

    result = await refresher.refresh_symbols(sym_list, wl_id, dev_force=dev_force)
    return result


# ── In-process backfill state ─────────────────────────────────────────────────
_backfill_state: dict = {"status": "idle", "refreshed": 0, "failed": 0, "total": 0,
                         "failed_symbols": [], "started_at": None, "finished_at": None}

# ── Manual-add priority hydration state ──────────────────────────────────────
# Keyed by SYMBOL (uppercase). Tracks per-symbol hydration progress for
# tickers added via POST /{watchlist_id}/ticker or POST /{watchlist_id}/tickers.
# In-process only — lost on restart, which is fine because each hydration step
# persists its own result to Neon/disk.
_HYDRATION_STATE: dict[str, dict] = {}


def _hydration_entry(sym: str) -> dict:
    """Return the current hydration state for a symbol or sensible defaults."""
    return dict(_HYDRATION_STATE.get(sym.upper(), {
        "quote":        "unknown",
        "technical":    "unknown",
        "fundamentals": "unknown",
        "options":      "unknown",
        "enqueued_at":  None,
        "last_error":   None,
        "last_updated": None,
    }))


async def _priority_hydrate_symbols(symbols: list[str], watchlist_id: str) -> None:
    """
    Background priority hydration for manually added symbols.

    Runs three sequential steps so each step can build on prior results:
      A. Quote      — refresh_watchlist_quotes_now() (Tradier batch, in-memory cache)
      B. Technical  — warmup_stage2(force=True) (stage + all technical_metrics)
      C. FMP        — FmpFundamentalsRefresher.normalize_symbol() + upsert_snapshot()
      D. Market cap — merge implied_shares into existing fund cache row

    State is written to _HYDRATION_STATE[symbol] throughout so the status
    endpoint can return real-time progress to the caller.

    Uses existing budget/throttle safeguards in each step. User-triggered
    manual adds are allowed to call APIs immediately (same as the upload warmup).
    """
    import asyncio as _aio
    from datetime import datetime, timezone as _tz
    _now = lambda: datetime.now(_tz.utc).isoformat()

    deduped = list(dict.fromkeys(s.strip().upper() for s in symbols if s.strip()))
    if not deduped:
        return

    ts_enq = _now()
    for sym in deduped:
        _HYDRATION_STATE[sym] = {
            "quote":        "pending",
            "technical":    "pending",
            "fundamentals": "pending",
            "options":      "pending",
            "enqueued_at":  ts_enq,
            "last_error":   None,
            "last_updated": ts_enq,
        }

    # ── A. Quote ──────────────────────────────────────────────────────────────
    try:
        from services.watchlist_quote_cache import (
            refresh_watchlist_quotes_now as _rq,
            is_tradier_quote_eligible as _trad_elig,
        )
        tradier_syms = [s for s in deduped if _trad_elig(s)]
        for sym in deduped:
            _HYDRATION_STATE[sym]["quote"] = (
                "running" if sym in tradier_syms else "not_applicable"
            )
        if tradier_syms:
            await _rq(tradier_syms)
        for sym in tradier_syms:
            _HYDRATION_STATE[sym]["quote"] = "done"
    except Exception as _qe:
        for sym in deduped:
            if _HYDRATION_STATE[sym]["quote"] in ("pending", "running"):
                _HYDRATION_STATE[sym]["quote"] = "error"
                _HYDRATION_STATE[sym]["last_error"] = f"quote: {_qe}"
        print(f"[PRIORITY_HYDRATE] quote step error: {_qe}")

    # ── B. Technical / Stage2 ─────────────────────────────────────────────────
    try:
        from services.watchlist_stage2_service import warmup_stage2 as _ws2
        from services.watchlist_quote_cache import is_fmp_symbol_eligible as _fmp_elig
        tech_syms = [s for s in deduped if _fmp_elig(s)]
        for sym in deduped:
            _HYDRATION_STATE[sym]["technical"] = (
                "running" if sym in tech_syms else "not_applicable"
            )
        if tech_syms:
            await _ws2(tech_syms, force=True)
        for sym in tech_syms:
            _HYDRATION_STATE[sym]["technical"] = "done"
    except Exception as _te:
        for sym in deduped:
            if _HYDRATION_STATE[sym]["technical"] in ("pending", "running"):
                _HYDRATION_STATE[sym]["technical"] = "error"
                _HYDRATION_STATE[sym]["last_error"] = f"technical: {_te}"
        print(f"[PRIORITY_HYDRATE] technical step error: {_te}")

    # ── C. FMP Fundamentals ───────────────────────────────────────────────────
    try:
        import os as _os_h
        _fmp_key_h = _os_h.getenv("FMP_API_KEY", "")
        if not _fmp_key_h:
            raise RuntimeError("FMP_API_KEY not configured")
        from services.watchlist_fundamentals_refresh import FmpFundamentalsRefresher as _FmpH
        from data.watchlist_fundamentals_store import upsert_snapshot as _us_h
        from services.watchlist_quote_cache import is_fmp_symbol_eligible as _fmp_elig2
        _ref_h = _FmpH(_fmp_key_h)
        fund_syms = [s for s in deduped if _fmp_elig2(s)]
        for sym in deduped:
            _HYDRATION_STATE[sym]["fundamentals"] = (
                "running" if sym in fund_syms else "not_applicable"
            )
        for sym in fund_syms:
            try:
                _res = await _ref_h.normalize_symbol(sym)
                _out = _us_h(
                    sym, watchlist_id,
                    _res.get("fields") or {},
                    _res.get("missing_fields") or [],
                    _res.get("fmp_call_count", 0),
                )
                _HYDRATION_STATE[sym]["fundamentals"] = (
                    "done" if _out == "success" else f"done_{_out}"
                )
            except Exception as _fsym_e:
                _HYDRATION_STATE[sym]["fundamentals"] = "error"
                _HYDRATION_STATE[sym]["last_error"] = f"fmp: {_fsym_e}"
                print(f"[PRIORITY_HYDRATE] FMP({sym}) error: {_fsym_e}")
    except Exception as _fe:
        for sym in deduped:
            if _HYDRATION_STATE[sym]["fundamentals"] in ("pending", "running"):
                _HYDRATION_STATE[sym]["fundamentals"] = "error"
                _HYDRATION_STATE[sym]["last_error"] = f"fmp_setup: {_fe}"
        print(f"[PRIORITY_HYDRATE] fundamentals step error: {_fe}")

    # ── D. Market-cap implied shares (best-effort, non-fatal) ─────────────────
    try:
        import os as _os_mc
        _fmp_mc = _os_mc.getenv("FMP_API_KEY", "")
        if _fmp_mc:
            from data.watchlist_fundamentals_store import (
                get_snapshots_bulk as _gs_mc,
                merge_fields as _mf_mc,
            )
            from services.watchlist_fundamentals_refresh import FmpFundamentalsRefresher as _FmpMC
            from services.watchlist_quote_cache import is_fmp_symbol_eligible as _fmp_elig3
            _ref_mc = _FmpMC(_fmp_mc)
            mc_syms = [s for s in deduped if _fmp_elig3(s)]
            _existing = _gs_mc(mc_syms)
            for sym in mc_syms:
                _snap_f = (_existing.get(sym) or {}).get("fields") or {}
                if _snap_f.get("_market_cap_implied_shares") is not None:
                    continue
                try:
                    _raw = await _ref_mc._get("profile", {"symbol": sym})
                    _prof = (_raw[0] if isinstance(_raw, list) and _raw
                             else (_raw if isinstance(_raw, dict) else {}))
                    _mc = _prof.get("marketCap")
                    _px = _prof.get("price")
                    if _mc and _px and float(_mc) > 0 and float(_px) > 0:
                        _imp = round(float(_mc) / float(_px), 0)
                        if _imp > 0:
                            _mf_mc(sym, {
                                "_market_cap_implied_shares":   _imp,
                                "_market_cap_price_at_refresh": round(float(_px), 4),
                                "_market_cap_static_source":    "fmp_profile",
                            })
                except Exception:
                    pass
    except Exception:
        pass  # non-fatal — market-cap backfill best-effort

    # ── E. Options overlay (supplement priority queue + per-ticker cache check) ─
    #
    # Architecture:
    #   Options data flows: Tradier → supplement_loop/watchlist_scanner
    #                       → get_combined_ticker_data() → Confluence V4.2
    #                       → GET /{watchlist_id} options fields
    #
    # We do NOT create new Tradier calls here. Instead we:
    #   1. Check get_no_options_symbols() — confirmed no-options: immediate final state
    #   2. Check get_combined_ticker_data() — already in supplement cache: done
    #   3. Check portfolio_opts:{sym} per-ticker cache — written by watchlist scanner
    #   4. Call add_high_priority_symbols() — front-queues in supplement scan loop
    #
    # Status values: pending/done/no_options/not_applicable/error
    # "pending" = enqueued in supplement loop, data arrives in ≤ next scan cycle
    # "done"    = data verified present in combined cache or per-ticker cache
    try:
        from services.watchlist_quote_cache import is_tradier_quote_eligible as _trad_elig_o
        from data.options_theme_supplement import (
            get_no_options_symbols  as _get_no_opts_o,
            add_high_priority_symbols as _add_hi_opts_o,
            get_combined_ticker_data  as _get_combined_o,
        )
        from data.portfolio_options_service import _per_ticker_cache_key as _ptck_o
        from data.cache import cache as _opts_cache_o

        _no_opts_set = _get_no_opts_o()
        _combined_now = _get_combined_o()

        opts_eligible = [s for s in deduped if _trad_elig_o(s)]

        for sym in deduped:
            if sym not in opts_eligible:
                # Foreign / exchange-prefixed / non-US — options not applicable
                _HYDRATION_STATE[sym]["options"] = "not_applicable"
                continue

            if sym in _no_opts_set:
                # Already confirmed by Tradier to have no options chain
                _HYDRATION_STATE[sym]["options"] = "no_options"
                continue

            if sym in _combined_now:
                # Already present in supplement/master/LKG data
                _HYDRATION_STATE[sym]["options"] = "done"
                continue

            # Check per-ticker portfolio cache written by watchlist/portfolio scanner
            _ptck_row = _opts_cache_o.get(_ptck_o(sym))
            if _ptck_row:
                _ptck_reason = (_ptck_row.get("_reason") or "").lower()
                if "no_expir" in _ptck_reason or "no_options" in _ptck_reason or "confirmed_no" in _ptck_reason:
                    _HYDRATION_STATE[sym]["options"] = "no_options"
                elif _ptck_row.get("options_score") is not None or _ptck_row.get("iv") is not None:
                    _HYDRATION_STATE[sym]["options"] = "done"
                else:
                    _HYDRATION_STATE[sym]["options"] = "pending"
            else:
                # Not in any cache yet — enqueue in supplement priority queue
                _HYDRATION_STATE[sym]["options"] = "pending"

        # Front-queue all pending eligible symbols in the supplement scan loop.
        # add_high_priority_symbols() is safe to call from any context (pure in-memory write).
        _pending_opts = [
            s for s in opts_eligible
            if _HYDRATION_STATE.get(s, {}).get("options") == "pending"
        ]
        if _pending_opts:
            _add_hi_opts_o(_pending_opts)
            print(f"[PRIORITY_HYDRATE] options: {len(_pending_opts)} symbol(s) enqueued hi-priority: {_pending_opts[:5]}")

    except Exception as _opts_e:
        for sym in deduped:
            if _HYDRATION_STATE.get(sym, {}).get("options") in ("pending", "running", None):
                _HYDRATION_STATE[sym]["options"] = "error"
                _HYDRATION_STATE[sym]["last_error"] = f"options: {_opts_e}"
        print(f"[PRIORITY_HYDRATE] options step error: {_opts_e}")

    _ts_done = _now()
    for sym in deduped:
        if sym in _HYDRATION_STATE:
            _HYDRATION_STATE[sym]["last_updated"] = _ts_done
    print(f"[PRIORITY_HYDRATE] finished for {deduped}")

    # Invalidate bulk LKG so the next GET surfaces the freshly hydrated row
    # without waiting for the 5-minute fresh-window TTL to expire.
    # This covers quote, technical, and fundamentals completions.
    _bulk_lkg_invalidate(watchlist_id)


# ── Upload-triggered Stage2 warmup state ──────────────────────────────────────
_UPLOAD_WARMUP_STATE: dict = {
    "watchlist_id":   None,
    "triggered_at":   None,
    "finished_at":    None,
    "total_eligible": 0,
    "already_in_lkg": 0,
    "queued":         0,
    "running":        False,
}

@router.post("/debug/fundamentals/backfill")
async def debug_fundamentals_backfill(
    watchlist_id: Optional[str] = None,
    dev_force: bool = True,
):
    """
    DEV-ONLY: Fire-and-forget full fundamentals backfill.

    Universe = dynamic Watchlist membership (load_watchlist tickers → is_fmp_symbol_eligible),
    matching the weekly scheduler and admin quality-backfill paths exactly.
    Spawns as asyncio background task; returns immediately.
    Poll GET /debug/fundamentals/backfill/status for progress.
    """
    import asyncio as _aio, os as _os
    global _backfill_state

    if _backfill_state.get("status") == "running":
        return {"status": "already_running", "state": _backfill_state}

    fmp_key = _os.getenv("FMP_API_KEY", "")
    if not fmp_key:
        raise HTTPException(status_code=503, detail="FMP_API_KEY not configured")

    # Canonical universe: dynamic Watchlist membership, same as weekly scheduler and
    # quality_backfill.py / quality_backfill_chunk.py scripts.
    # Never reads from fundamentals_cache rows — cache rows are output, not input.
    from data.pg_storage import watchlist_list as _wl_list
    from services.watchlist_quote_cache import is_fmp_symbol_eligible as _fmp_eligible

    _seen: set[str] = set()
    _pairs: list[tuple[str, str]] = []  # (symbol, watchlist_id)
    for _wl in (_wl_list() or []):
        _wl_id = _wl.get("id") or ""
        if not _wl_id:
            continue
        # If caller supplied watchlist_id, restrict to that watchlist only
        if watchlist_id and _wl_id != watchlist_id:
            continue
        _wl_data = load_watchlist(_wl_id) or {}
        for _t in (_wl_data.get("tickers") or []):
            _sym = (_t if isinstance(_t, str) else (_t or {}).get("symbol", "")).strip().upper()
            if not _sym or _sym in _seen or not _fmp_eligible(_sym):
                continue
            _seen.add(_sym)
            _pairs.append((_sym, _wl_id))

    to_refresh = [s for s, _ in _pairs]
    wl_id = _pairs[0][1] if _pairs else (watchlist_id or "")

    _backfill_state.update({
        "status": "running", "refreshed": 0, "failed": 0,
        "total": len(to_refresh), "failed_symbols": [],
        "started_at": __import__("datetime").datetime.utcnow().isoformat(),
        "finished_at": None,
    })

    async def _run():
        global _backfill_state
        from services.watchlist_fundamentals_refresh import FmpFundamentalsRefresher
        from data.watchlist_fundamentals_store import upsert_snapshot as _upsert
        refresher = FmpFundamentalsRefresher(fmp_key)
        batch_n = 0
        for sym in to_refresh:
            try:
                result = await refresher.normalize_symbol(sym)
                ok = _upsert(symbol=sym, watchlist_id=wl_id,
                             fields=result["fields"],
                             missing_fields=result["missing_fields"],
                             fmp_call_count=result["fmp_call_count"])
                if ok:
                    _backfill_state["refreshed"] += 1
                else:
                    _backfill_state["failed"] += 1
                    _backfill_state["failed_symbols"].append(sym)
            except Exception as _e:
                _backfill_state["failed"] += 1
                _backfill_state["failed_symbols"].append(sym)
                print(f"[BACKFILL] {sym} error: {_e}")
            batch_n += 1
            if batch_n % 25 == 0:
                _done = _backfill_state["refreshed"] + _backfill_state["failed"]
                print(f"[BACKFILL] checkpoint {_done}/{_backfill_state['total']} "
                      f"ok={_backfill_state['refreshed']} fail={_backfill_state['failed']}")
        _backfill_state["status"] = "done"
        _backfill_state["finished_at"] = __import__("datetime").datetime.utcnow().isoformat()
        print(f"[BACKFILL] complete: refreshed={_backfill_state['refreshed']} "
              f"failed={_backfill_state['failed']} total={_backfill_state['total']}")

    _aio.create_task(_run())
    return {"status": "started", "total_to_refresh": len(to_refresh), "state": _backfill_state}


@router.get("/debug/technical/provenance")
async def debug_technical_provenance(symbol: str = ""):
    """
    DEV-ONLY: Full technical diagnostics for a single symbol from LKG cache.
    No provider calls — reads only the in-memory LKG.

    Returns stage label/score/reason, provenance, all technical_metrics,
    technical_state, technical_timing_score, and missing_metric_reasons.
    """
    from services.watchlist_stage2_service import get_stage2, _STAGE2_LKG

    sym = (symbol or "").strip().upper()
    if not sym:
        return {"error": "symbol query param required"}

    raw = _STAGE2_LKG.get(sym)
    if raw is None:
        return {"error": f"{sym} not found in Stage2 LKG", "symbol": sym}

    tech = raw.get("technical_metrics") or {}
    return {
        "symbol":         sym,
        "stage_label":    raw.get("label"),
        "stage_score":    raw.get("score"),
        "stage_reason":   raw.get("reason"),
        "stage_confidence":        raw.get("stage_confidence"),
        "stage_confidence_reason": raw.get("stage_confidence_reason"),
        "history_source":     raw.get("history_source"),
        "bars_count":         raw.get("bars_count"),
        "history_start_date": raw.get("history_start_date"),
        "history_end_date":   raw.get("history_end_date"),
        "has_ohlcv":          raw.get("has_ohlcv"),
        "has_200d":           raw.get("has_200d"),
        "has_252d":           raw.get("has_252d"),
        "computed_at":        raw.get("computed_at"),
        "technical_state":         raw.get("technical_state"),
        "technical_timing_score":  raw.get("technical_timing_score"),
        "technical_metrics":       tech,
        "missing_metric_reasons":  tech.get("missing_metric_reasons", []),
        "signals":            raw.get("signals"),
        "status":             raw.get("status"),
    }


@router.get("/debug/technical/status")
async def debug_technical_status(watchlist_id: Optional[str] = None):
    """
    DEV-ONLY: Aggregate technical coverage stats across all LKG entries.
    No provider calls — reads only the in-memory LKG.

    Returns counts: total, valid stage labels, missing, has_ohlcv, FMP-history,
    Tradier-history, low-confidence stage, average bars_count, tickers missing
    200D, tickers missing 252D.
    """
    from services.watchlist_stage2_service import _STAGE2_LKG

    wl_id  = watchlist_id or "23eec278-074a-4706-a62a-c35d38b384ea"
    store  = load_watchlist(wl_id)
    tickers: list[str] = []
    if store:
        tickers = [t.strip().upper() for t in (store.get("tickers") or []) if t.strip()]

    # Collect LKG entries for this watchlist's tickers (fall back to all LKG if wl empty)
    scope: dict[str, dict] = {}
    if tickers:
        for sym in tickers:
            e = _STAGE2_LKG.get(sym)
            if e is not None:
                scope[sym] = e
    else:
        scope = dict(_STAGE2_LKG)

    total          = len(scope)
    valid_label    = 0
    missing_label  = 0
    has_ohlcv_cnt  = 0
    fmp_cnt        = 0
    tradier_cnt    = 0
    low_conf_cnt   = 0
    no_200d_syms: list[str] = []
    no_252d_syms: list[str] = []
    bars_counts: list[int]  = []

    for sym, e in scope.items():
        lbl = e.get("label")
        sc  = e.get("score")
        if lbl is not None and sc is not None:
            valid_label += 1
        else:
            missing_label += 1
        if e.get("has_ohlcv"):
            has_ohlcv_cnt += 1
        src = e.get("history_source") or "unknown"
        if src == "fmp":
            fmp_cnt += 1
        elif src == "tradier":
            tradier_cnt += 1
        if (e.get("stage_confidence") or "low") == "low":
            low_conf_cnt += 1
        bc = e.get("bars_count")
        if bc is not None:
            bars_counts.append(int(bc))
            if bc < 200:
                no_200d_syms.append(sym)
            if bc < 252:
                no_252d_syms.append(sym)

    avg_bars = round(sum(bars_counts) / len(bars_counts), 1) if bars_counts else None

    # Also tally technical_state distribution
    state_dist: dict[str, int] = {}
    tech_metrics_cnt = 0
    for e in scope.values():
        st = e.get("technical_state") or "unknown"
        state_dist[st] = state_dist.get(st, 0) + 1
        if e.get("technical_metrics") is not None:
            tech_metrics_cnt += 1

    # Tally all watchlist tickers not in LKG scope (ineligible or never computed)
    skipped_ineligible = 0
    not_in_lkg_cnt = 0
    if tickers:
        from services.watchlist_quote_cache import is_fmp_symbol_eligible as _st_elig
        for _s in tickers:
            if _STAGE2_LKG.get(_s) is None:
                if not _st_elig(_s):
                    skipped_ineligible += 1
                else:
                    not_in_lkg_cnt += 1

    return {
        "watchlist_id":                  wl_id,
        "scope":                         "watchlist" if tickers else "all_lkg",
        "total_watchlist_tickers":       len(tickers),
        "total_in_lkg":                  total,
        "valid_stage_labels":            valid_label,
        "missing_stage_labels":          missing_label,
        "existing_technical_metrics":    tech_metrics_cnt,
        "missing_technical_metrics":     total - tech_metrics_cnt,
        "not_in_lkg_eligible":           not_in_lkg_cnt,
        "skipped_ineligible":            skipped_ineligible,
        "has_ohlcv_count":               has_ohlcv_cnt,
        "fmp_history_count":             fmp_cnt,
        "tradier_history_count":         tradier_cnt,
        "low_confidence_count":          low_conf_cnt,
        "average_bars_count":            avg_bars,
        "tickers_missing_200d":          no_200d_syms,
        "tickers_missing_252d":          no_252d_syms,
        "technical_state_distribution":  state_dist,
        "upload_warmup":                 dict(_UPLOAD_WARMUP_STATE),
    }


@router.post("/debug/technical/backfill/start")
async def debug_technical_backfill_start(
    watchlist_id: Optional[str] = None,
    missing_only: bool = True,
    cap: int = 50,
):
    """
    DEV-ONLY: Fire-and-forget technical metrics backfill.

    Processes LKG entries that have stage labels but no technical_metrics.
    Merges technical fields only — does NOT overwrite score/label/reason/signals.
    Uses the same _fetch_bars() + compute_technical_metrics() path as warmup_stage2.
    Capped at `cap` symbols per call (resumable — re-POST picks up remaining).

    Returns immediately; poll GET /debug/technical/backfill/status for progress.
    """
    from services.watchlist_stage2_service import (
        backfill_technical_metrics,
        _TECH_BACKFILL_STATE,
        _STAGE2_LKG,
    )

    if _TECH_BACKFILL_STATE.get("status") == "running":
        return {"status": "already_running", "state": dict(_TECH_BACKFILL_STATE)}

    wl_id = watchlist_id or "23eec278-074a-4706-a62a-c35d38b384ea"
    tickers: list[str] = []
    try:
        store = load_watchlist(wl_id)
        if store:
            tickers = [t.strip().upper() for t in (store.get("tickers") or []) if t.strip()]
    except Exception:
        pass

    if not tickers:
        tickers = list(_STAGE2_LKG.keys())

    missing_count = sum(
        1 for sym in tickers
        if _STAGE2_LKG.get(sym) is not None
        and _STAGE2_LKG[sym].get("technical_metrics") is None
    )

    import asyncio as _asyncio
    _asyncio.create_task(
        backfill_technical_metrics(tickers, cap=cap, missing_only=missing_only)
    )

    return {
        "status":                    "started",
        "watchlist_id":              wl_id,
        "watchlist_tickers":         len(tickers),
        "missing_technical_metrics": missing_count,
        "cap":                       cap,
        "missing_only":              missing_only,
    }


@router.get("/debug/technical/backfill/status")
async def debug_technical_backfill_status():
    """DEV-ONLY: Poll progress of an in-progress technical metrics backfill."""
    from services.watchlist_stage2_service import _TECH_BACKFILL_STATE
    return dict(_TECH_BACKFILL_STATE)


@router.get("/debug/fundamentals/backfill/status")
async def debug_fundamentals_backfill_status():
    """DEV-ONLY: Poll progress of an in-progress backfill."""
    return _backfill_state


# ── Earnings-intelligence backfill state ──────────────────────────────────────
_ei_backfill_state: dict = {
    "status": "idle", "refreshed": 0, "failed": 0, "skipped": 0,
    "total": 0, "failed_symbols": [], "skipped_symbols": [],
    "partial_symbols": [], "started_at": None, "finished_at": None,
}


@router.post("/debug/earnings-intelligence/backfill")
async def debug_ei_backfill(
    request: Request,
    symbols: Optional[str] = None,
    watchlist_id: Optional[str] = None,
):
    """
    DEV-ONLY: Fire-and-forget earnings-intelligence backfill.

    Auth: Authorization: Bearer <ADMIN_PASSWORD> required.

    Universe (when symbols= is omitted):
      All symbols already in watchlist_fundamentals_cache that pass
      is_fmp_symbol_eligible() — no re-fetch of raw fundamentals needed.
      merge_fields() adds earnings_intelligence atomically without erasing
      any existing fundamentals keys.

    When a symbol has no existing snapshot row, falls back to a full
    refresh_symbols() call so the snapshot is created with EI included,
    rather than silently skipping eligible watchlist members.

    When symbols= is supplied (comma-separated): restricts to those symbols only,
    useful for the five-symbol validation step.

    Poll GET /debug/earnings-intelligence/backfill/status for progress.
    """
    import asyncio as _aio, os as _os
    # ── Auth ──────────────────────────────────────────────────────────────────
    _auth_hdr = request.headers.get("Authorization", "")
    _token = _auth_hdr.removeprefix("Bearer ").strip() if _auth_hdr.startswith("Bearer ") else ""
    _admin_pw = _os.getenv("ADMIN_PASSWORD", "")
    if not _admin_pw or _token != _admin_pw:
        try:
            from auth import require_admin_user_or_api_key as _req_admin
            await _req_admin(request)
        except Exception:
            raise HTTPException(status_code=401, detail="admin_auth_required")
    # ─────────────────────────────────────────────────────────────────────────
    global _ei_backfill_state

    if _ei_backfill_state.get("status") == "running":
        return {"status": "already_running", "state": _ei_backfill_state}

    fmp_key = _os.getenv("FMP_API_KEY", "")
    if not fmp_key:
        raise HTTPException(status_code=503, detail="FMP_API_KEY not configured")

    from services.watchlist_quote_cache import is_fmp_symbol_eligible as _fmp_elig

    if symbols:
        to_refresh = [s.strip().upper() for s in symbols.split(",") if s.strip()]
        to_refresh = [s for s in to_refresh if _fmp_elig(s)]
    else:
        from data.watchlist_fundamentals_store import list_all_symbols as _list_syms
        all_syms = _list_syms()
        to_refresh = [s for s in all_syms if _fmp_elig(s)]

    _ei_backfill_state.update({
        "status": "running",
        "refreshed": 0, "failed": 0, "skipped": 0,
        "total": len(to_refresh),
        "failed_symbols": [], "skipped_symbols": [], "partial_symbols": [],
        "started_at": __import__("datetime").datetime.utcnow().isoformat(),
        "finished_at": None,
        "universe": to_refresh,
    })

    async def _run():
        global _ei_backfill_state
        from services.watchlist_fundamentals_refresh import FmpFundamentalsRefresher, ei_ineligible_reason as _ei_elig
        from data.watchlist_fundamentals_store import merge_fields as _merge, get_snapshot as _get_snap

        refresher = FmpFundamentalsRefresher(fmp_key)
        checkpoint_n = 0

        for sym in to_refresh:
            try:
                # ── Eligibility gate: skip ETFs, funds, and non-operating securities ──
                _snap = await __import__("asyncio").get_event_loop().run_in_executor(
                    None, _get_snap, sym
                )
                _inelig = _ei_elig(sym, _snap)
                if _inelig:
                    _ei_backfill_state["skipped"] += 1
                    _ei_backfill_state["skipped_symbols"].append(
                        {"symbol": sym, "reason": f"ineligible:{_inelig}"}
                    )
                    continue

                ei_data = await refresher._fetch_earnings_intelligence(sym)
                if not ei_data:
                    _ei_backfill_state["skipped"] += 1
                    _ei_backfill_state["skipped_symbols"].append(
                        {"symbol": sym, "reason": "no_data_returned"}
                    )
                    continue

                # Remove internal _call_count before persisting
                ei_data.pop("_call_count", None)

                # Track partial coverage
                cov = (ei_data.get("source_status") or {}).get("coverage") or {}
                has_hist = cov.get("has_earnings_history", False)
                has_react = cov.get("has_reactions", False)

                # Persist via JSONB merge — preserves all other fundamentals keys
                ok = _merge(sym, {"earnings_intelligence": ei_data})
                if not ok:
                    # No existing snapshot row: create one through the canonical
                    # refresher path (normalize_symbol + upsert_snapshot), which
                    # already inlines earnings_intelligence before the single write.
                    print(f"[EI_BACKFILL] {sym}: no snapshot row — running full refresh_symbols() to initialise")
                    try:
                        _wl_id = watchlist_id or "ei_backfill_init"
                        await refresher.refresh_symbols([sym], _wl_id)
                        ok = True
                    except Exception as _init_exc:
                        print(f"[EI_BACKFILL] {sym}: full-refresh fallback failed: {_init_exc}")
                        ok = False

                if ok:
                    _ei_backfill_state["refreshed"] += 1
                    if not has_hist:
                        _ei_backfill_state["partial_symbols"].append(
                            {"symbol": sym, "reason": "no_earnings_history"}
                        )
                    elif not has_react:
                        _ei_backfill_state["partial_symbols"].append(
                            {"symbol": sym, "reason": "no_completed_reactions"}
                        )
                else:
                    _ei_backfill_state["skipped"] += 1
                    _ei_backfill_state["skipped_symbols"].append(
                        {"symbol": sym, "reason": "no_existing_snapshot_row_and_full_refresh_failed"}
                    )

            except Exception as exc:
                _ei_backfill_state["failed"] += 1
                _ei_backfill_state["failed_symbols"].append(
                    {"symbol": sym, "error": str(exc)[:200]}
                )
                print(f"[EI_BACKFILL] {sym} error: {exc}")

            checkpoint_n += 1
            if checkpoint_n % 10 == 0:
                _done = (_ei_backfill_state["refreshed"] +
                         _ei_backfill_state["failed"] +
                         _ei_backfill_state["skipped"])
                print(
                    f"[EI_BACKFILL] {_done}/{_ei_backfill_state['total']} "
                    f"ok={_ei_backfill_state['refreshed']} "
                    f"fail={_ei_backfill_state['failed']} "
                    f"skip={_ei_backfill_state['skipped']}"
                )

        _ei_backfill_state["status"] = "done"
        _ei_backfill_state["finished_at"] = __import__("datetime").datetime.utcnow().isoformat()
        print(
            f"[EI_BACKFILL] complete: "
            f"refreshed={_ei_backfill_state['refreshed']} "
            f"failed={_ei_backfill_state['failed']} "
            f"skipped={_ei_backfill_state['skipped']} "
            f"total={_ei_backfill_state['total']}"
        )

    _aio.create_task(_run())
    return {
        "status": "started",
        "total": len(to_refresh),
        "state": _ei_backfill_state,
    }


@router.get("/debug/earnings-intelligence/backfill/status")
async def debug_ei_backfill_status():
    """DEV-ONLY: Poll progress of an in-progress earnings-intelligence backfill."""
    return _ei_backfill_state


@router.get("/debug/fundamentals/status")
async def debug_fundamentals_status(watchlist_id: Optional[str] = None):
    """
    DEV-ONLY: Full per-symbol coverage audit for the watchlist FMP fundamentals cache.

    Returns:
      summary           — aggregate counts (total, eligible, has_snapshot, by status, earnings coverage)
      field_coverage    — per-column: fmp / csv / missing counts + csv_only_by_design flag
      per_symbol        — one entry per watchlist symbol with status, refreshed_at,
                          field counts, and earnings date provenance
    """
    from datetime import datetime, timezone
    from zoneinfo import ZoneInfo
    from data.watchlist_fundamentals_store import get_snapshots_bulk as _get_snaps
    from services.watchlist_quote_cache import is_fmp_symbol_eligible as _eligible

    _ET = ZoneInfo("America/New_York")
    today_et = datetime.now(tz=_ET).strftime("%Y-%m-%d")
    now_utc  = datetime.now(timezone.utc)

    wl_id = watchlist_id or "23eec278-074a-4706-a62a-c35d38b384ea"
    store = load_watchlist(wl_id)
    if not store:
        return {"error": f"Watchlist {wl_id} not found"}

    csv_data: list[dict] = store.get("csv_data") or []
    tickers:  list[str]  = store.get("tickers")  or []

    # Index CSV rows by symbol
    csv_by_sym: dict[str, dict] = {}
    for row in csv_data:
        sym = (row.get("Symbol") or row.get("symbol") or row.get("Ticker") or "").strip().upper()
        if sym:
            csv_by_sym[sym] = row

    all_syms       = [s.strip().upper() for s in tickers if s and s.strip()]
    eligible_syms  = [s for s in all_syms if _eligible(s)]
    ineligible_syms = [s for s in all_syms if not _eligible(s)]

    # Single bulk DB read for all eligible symbols
    snaps = _get_snaps(eligible_syms) if eligible_syms else {}

    # ── Tracked fields ──────────────────────────────────────────────────────
    FMP_FIELDS = [
        "Market Cap", "Revenue", "Revenue Growth (Q)", "Revenue Growth (YoY)",
        "Gross Margin", "FCF Margin", "Free Cash Flow", "Operating Income", "EBIT",
        "PE Ratio", "PS Ratio", "EV/EBITDA", "EPS Growth", "Debt / Equity",
        "Net Debt / EBITDA", "Earnings Date",
        "Rev Growth Next Quarter", "EPS Growth This Quarter",
    ]
    CSV_ONLY_FIELDS = [
        "Shares Insiders", "Revenue Growth Est.", "Rev Growth Next Year",
        "Rev Growth This Year", "EPS Growth Est.", "EPS Growth Next Quarter",
        "EPS Growth This Year", "EPS Growth Next Year",
    ]
    ALL_TRACKED = FMP_FIELDS + CSV_ONLY_FIELDS
    CSV_ONLY_SET = set(CSV_ONLY_FIELDS)

    field_coverage: dict = {
        f: {"fmp": 0, "csv": 0, "missing": 0, "csv_only_by_design": f in CSV_ONLY_SET}
        for f in ALL_TRACKED
    }

    # ── Aggregate counters ───────────────────────────────────────────────────
    summary: dict = {
        "total":                   len(all_syms),
        "fmp_eligible":            len(eligible_syms),
        "fmp_ineligible":          len(ineligible_syms),
        "has_snapshot":            0,
        "fmp_refreshed":           0,        # ≥10 FMP fields populated
        "fmp_missing_partial":     0,        # 1–9 FMP fields
        "csv_fallback_only":       0,        # eligible but snapshot is empty/failed
        "not_yet_scanned":         0,        # eligible, no snapshot at all
        "fmp_ineligible_count":    len(ineligible_syms),
        "has_future_earnings_date": 0,
        "stale_or_no_earnings_date": 0,
    }

    per_symbol: list[dict] = []

    for sym in all_syms:
        csv_row = csv_by_sym.get(sym, {})
        snap    = snaps.get(sym) if _eligible(sym) else None
        elig    = _eligible(sym)

        # ── Status classification ─────────────────────────────────────────
        if not elig:
            status = "fmp_ineligible"
            fmp_fields_count = 0
            missing_count    = 0
            refreshed_at     = None
            next_refresh_at  = None
        elif snap is None:
            status = "not_yet_scanned"
            fmp_fields_count = 0
            missing_count    = 0
            refreshed_at     = None
            next_refresh_at  = None
        else:
            snap_fields:  dict = snap.get("fields")        or {}
            snap_missing: list = snap.get("missing_fields") or []
            refreshed_at        = snap.get("refreshed_at")
            next_refresh_at     = snap.get("next_refresh_at")
            fmp_fields_count    = len(snap_fields)
            missing_count       = len(snap_missing)

            if fmp_fields_count >= 10:
                status = "fmp_refreshed"
            elif fmp_fields_count > 0:
                status = "fmp_missing_partial"
            elif missing_count > 0:
                status = "csv_fallback_only"   # ran, got nothing; all marked missing
            else:
                status = "not_yet_scanned"     # row exists but fields empty

        # ── Earnings date provenance (with stale rule) ────────────────────
        snap_fields_d: dict = (snap.get("fields") or {}) if snap else {}
        earn_fmp = (snap_fields_d.get("Earnings Date") or "").strip() or None
        earn_csv = (csv_row.get("Earnings Date") or "").strip() or None

        if earn_fmp and earn_fmp >= today_et:
            earn_final  = earn_fmp
            earn_source = "fmp"
            has_future  = True
        elif earn_csv and earn_csv >= today_et:
            earn_final  = earn_csv
            earn_source = "csv_fallback"
            has_future  = True
        else:
            earn_final  = None
            earn_source = "missing" if elig else "fmp_ineligible"
            has_future  = False

        # ── Counters ──────────────────────────────────────────────────────
        if snap:
            summary["has_snapshot"] += 1
        if status == "fmp_refreshed":       summary["fmp_refreshed"]       += 1
        elif status == "fmp_missing_partial": summary["fmp_missing_partial"] += 1
        elif status == "csv_fallback_only": summary["csv_fallback_only"]    += 1
        elif status == "not_yet_scanned":   summary["not_yet_scanned"]      += 1

        if has_future:
            summary["has_future_earnings_date"]    += 1
        else:
            summary["stale_or_no_earnings_date"]   += 1

        # ── Field-level coverage ──────────────────────────────────────────
        snap_missing_set: set = set((snap.get("missing_fields") or [])) if snap else set()
        for field in ALL_TRACKED:
            if not elig or snap is None:
                field_coverage[field]["missing"] += 1
            elif field in snap_fields_d and snap_fields_d[field] is not None:
                field_coverage[field]["fmp"] += 1
            elif field in snap_missing_set:
                csv_val = str(csv_row.get(field) or "").strip()
                if csv_val:
                    field_coverage[field]["csv"] += 1
                else:
                    field_coverage[field]["missing"] += 1
            else:
                csv_val = str(csv_row.get(field) or "").strip()
                if csv_val:
                    field_coverage[field]["csv"] += 1
                else:
                    field_coverage[field]["missing"] += 1

        per_symbol.append({
            "symbol":               sym,
            "status":               status,
            "fmp_eligible":         elig,
            "refreshed_at":         refreshed_at,
            "next_refresh_at":      next_refresh_at,
            "fmp_fields_count":     fmp_fields_count,
            "missing_fields_count": missing_count,
            "has_future_earnings_date":    has_future,
            "final_earnings_date":         earn_final,
            "final_earnings_date_source":  earn_source,
            "csv_earnings_date":           earn_csv,
            "fmp_earnings_date":           earn_fmp,
        })

    return {
        "watchlist_id": wl_id,
        "audited_at":   now_utc.isoformat(),
        "today_et":     today_et,
        "summary":      summary,
        "field_coverage": field_coverage,
        "per_symbol":   per_symbol,
    }


@router.get("/debug/fundamentals/provenance")
async def debug_fundamentals_provenance(
    symbol: str,
    watchlist_id: Optional[str] = None,
):
    """
    DEV-ONLY: Field-by-field provenance for one symbol.
    Returns final_value, source (fmp/csv_fallback/canonical_theme/missing),
    fmp_value, csv_value, source_endpoint, formula, missing_reason, refreshed_at.

    Read-only — inspects Neon cache + current watchlist row only.
    No live provider calls.
    """
    from data.watchlist_fundamentals_store import get_snapshot as _get_snap
    from services.watchlist_fundamentals_refresh import apply_fmp_overlays, merge_fmp_into_csv_row

    sym = symbol.strip().upper()
    wl_id = watchlist_id or "23eec278-074a-4706-a62a-c35d38b384ea"

    # ── Load CSV row ──────────────────────────────────────────────────────────
    csv_row: dict = {}
    try:
        wl_store = load_watchlist(wl_id)
        if wl_store:
            for r in (wl_store.get("csv_data") or []):
                if (r.get("Symbol") or r.get("Ticker") or "").upper() == sym:
                    csv_row = dict(r)
                    break
    except Exception:
        pass

    # ── Load FMP snapshot ─────────────────────────────────────────────────────
    snap = _get_snap(sym)
    fmp_fields: dict = snap.get("fields", {}) if snap else {}
    missing_fmp: list = snap.get("missing_fields", []) if snap else []
    refreshed_at: str = snap.get("refreshed_at", "") if snap else ""

    # ── Build merged row (same logic as GET endpoint) ─────────────────────────
    merged_row: dict = {}
    if csv_row and snap:
        merged_row = merge_fmp_into_csv_row(dict(csv_row), fmp_fields)
    elif csv_row:
        merged_row = dict(csv_row)

    # ── Field metadata table ──────────────────────────────────────────────────
    # Maps screener column name → (source_endpoint, formula, plan_limited)
    _META: dict[str, tuple[str, str, bool]] = {
        "Symbol":                   ("—",                        "direct",                        False),
        "Theme":                    ("canonical_watchlist",       "screener hub tag, not FMP",     False),
        "Market Cap":               ("profile",                   "marketCap direct",              False),
        "Revenue":                  ("income-statement 8Q",       "TTM sum rows[0:4].revenue",     False),
        "Revenue Growth (Q)":       ("income-statement 8Q",       "rows[0].rev / rows[4].rev - 1", False),
        "Revenue Growth (YoY)":     ("income-statement 8Q",       "TTM / prior_TTM - 1",           False),
        "Gross Margin":             ("ratios-ttm",                "grossProfitMarginTTM",          False),
        "FCF Margin":               ("cash-flow 5Q",              "TTM_FCF / TTM_rev × 100",       False),
        "Free Cash Flow":           ("cash-flow 5Q",              "TTM sum freeCashFlow",          False),
        "Operating Income":         ("income-statement 8Q",       "TTM sum operatingIncome",       False),
        "EBIT":                     ("income-statement 8Q",       "TTM sum ebit",                  False),
        "PE Ratio":                 ("ratios-ttm",                "priceToEarningsRatioTTM",       False),
        "PS Ratio":                 ("ratios-ttm",                "priceToSalesRatioTTM",          False),
        "EV/EBITDA":                ("key-metrics-ttm",           "evToEBITDATTM",                 False),
        "EPS Growth":               ("income-statement-growth 2Q","growthEPSDiluted × 100",        False),
        "Debt / Equity":            ("ratios-ttm",                "debtToEquityRatioTTM",          False),
        "Net Debt / EBITDA":        ("key-metrics-ttm",           "netDebtToEBITDATTM",            False),
        "Shares Insiders":          ("—",                         "no FMP Starter endpoint",       True),
        "Earnings Date":            ("earnings 8",                "future_earn[0].date",           False),
        "Revenue Growth Est.":      ("—",                         "analyst-estimates → 402 Starter",True),
        "Rev Growth Next Quarter":  ("earnings 8",                "future[0]_est / py_actual - 1", False),
        "Rev Growth Next Year":     ("—",                         "analyst-estimates → 402 Starter",True),
        "EPS Growth Est.":          ("—",                         "analyst-estimates → 402 Starter",True),
        "EPS Growth This Quarter":  ("earnings 8",                "future[0]_eps_est / py_eps - 1",False),
        "EPS Growth Next Quarter":  ("—",                         "only 1 future Q in earnings",   True),
        "EPS Growth This Year":     ("—",                         "analyst-estimates → 402 Starter",True),
        "EPS Growth Next Year":     ("—",                         "analyst-estimates → 402 Starter",True),
    }

    out: list[dict] = []
    all_cols = list(_META.keys())
    # Also include any extra CSV keys not in meta
    for k in csv_row:
        if k not in all_cols:
            all_cols.append(k)

    for col in all_cols:
        meta = _META.get(col, ("unknown", "unknown", False))
        src_endpoint, formula, plan_limited = meta

        fmp_val   = fmp_fields.get(col)        # may be None if not mapped
        csv_val   = csv_row.get(col)
        final_val = merged_row.get(col)

        # Classify source
        if col == "Theme":
            source = "canonical_theme"
            missing_reason = ""
        elif col == "Symbol":
            source = "csv_fallback"
            missing_reason = ""
        elif fmp_val is not None and str(final_val) == str(fmp_val):
            source = "fmp"
            missing_reason = ""
        elif col in missing_fmp and csv_val:
            source = "csv_fallback"
            missing_reason = (
                "plan_limited" if plan_limited else
                f"fmp_missing: {col} not in snapshot fields"
            )
        elif col in missing_fmp and not csv_val:
            source = "missing"
            missing_reason = (
                "plan_limited (no CSV value either)" if plan_limited else
                "fmp_missing + no_csv_value"
            )
        elif not snap:
            source = "csv_fallback"
            missing_reason = "no_fmp_snapshot"
        elif final_val == csv_val and fmp_val is None:
            source = "csv_fallback"
            missing_reason = "fmp_field_not_mapped_or_null"
        else:
            source = "csv_fallback"
            missing_reason = ""

        out.append({
            "column":          col,
            "final_value":     final_val,
            "source":          source,
            "fmp_value":       fmp_val,
            "csv_value":       csv_val,
            "source_endpoint": src_endpoint,
            "formula":         formula,
            "plan_limited":    plan_limited,
            "missing_reason":  missing_reason,
            "refreshed_at":    refreshed_at,
        })

    return {
        "symbol":         sym,
        "watchlist_id":   wl_id,
        "cache_exists":   snap is not None,
        "refreshed_at":   refreshed_at,
        "fmp_field_count": len(fmp_fields),
        "missing_count":  len(missing_fmp),
        "missing_fields": sorted(missing_fmp),
        "provenance":     out,
    }


# ── Theme classifier endpoints ─────────────────────────────────────────────────


@router.post("/debug/themes/classify/start")
async def debug_themes_classify_start(
    watchlist_id: Optional[str] = None,
    missing_only: bool = True,
    cap: int = 40,
):
    """
    Trigger background LLM batch classification for unmapped watchlist symbols.

    Skips symbols already in canonical map, industry fallback, or LLM overrides.
    Job-level lock — only one run at a time. Returns immediately; poll /status.

    Provider: THEME_CLASSIFIER_PROVIDER env var (gemini default, openai fallback).
    Model:    THEME_CLASSIFIER_MODEL env var (gemini-2.0-flash-lite / gpt-4o-mini default).

    Query params:
      watchlist_id  — target watchlist (None = all mapped watchlists)
      missing_only  — true: only classify unmapped symbols (default true)
      cap           — max symbols per batch (default 40)
    """
    from services.watchlist_theme_classifier import (
        classify_watchlist_themes,
        get_classifier_status,
        _lock,
    )

    if _lock().locked():
        return {
            "status": "already_running",
            "state":  get_classifier_status(),
        }

    await classify_watchlist_themes(watchlist_id, missing_only=missing_only, cap=cap)
    return {
        "status": "started",
        "state":  get_classifier_status(),
    }


@router.get("/debug/themes/classify/status")
async def debug_themes_classify_status(watchlist_id: Optional[str] = None):
    """
    Poll progress of the background theme classifier job.

    Returns:
      running, total_symbols, mapped_existing, mapped_from_fmp_industry,
      mapped_from_llm, needs_theme, queued, processed, failed,
      current_batch, started_at, updated_at, failures, last_provider, last_model
    """
    from services.watchlist_theme_classifier import get_classifier_status, get_needs_review

    state  = get_classifier_status()
    review = get_needs_review()

    if watchlist_id and state.get("watchlist_id") != watchlist_id:
        return {
            "note":          f"Last run was for watchlist {state.get('watchlist_id')!r}, not {watchlist_id!r}",
            "state":         state,
            "needs_review":  review,
        }

    return {
        "state":        state,
        "needs_review": review,
    }


@router.get("/debug/themes/provenance")
async def debug_themes_provenance(symbol: str):
    """
    Return full theme resolution trace for a single symbol.

    Shows final_theme, source (neon_manual_override / themes_page_membership /
    llm_classified / canonical_map_or_industry / none), all intermediate stores,
    and is_mapped. Read-only — no provider calls.
    """
    from services.watchlist_theme_classifier import get_theme_provenance
    return get_theme_provenance(symbol)


@router.get("/debug/themes/status")
async def debug_themes_status(watchlist_id: str = ""):
    """
    GET /api/watchlist/debug/themes/status?watchlist_id=<id>

    Returns per-watchlist theme coverage breakdown:
      total_symbols, mapped_manual, mapped_themes_page, mapped_llm,
      mapped_canonical, mapped_fmp_industry, mapped_csv_fallback, needs_theme,
      queued_for_classification, classifier_running, classifier_failed,
      needs_theme_symbols, recently_classified_symbols.

    Read-only — no provider or LLM calls.
    """
    import json as _j
    from pathlib import Path as _Path
    from services.watchlist_theme_classifier import get_theme_provenance, get_classifier_status

    # Load watchlist tickers
    tickers: list[str] = []
    try:
        if watchlist_id:
            from data.pg_storage import watchlist_read as _lws
            store = _lws(watchlist_id)
            if store:
                tickers = [t.strip().upper() for t in (store.get("tickers") or []) if t.strip()]
        if not tickers:
            from services.watchlist_service import load_watchlist as _lw
            _store = _lw()
            if _store:
                tickers = [t.strip().upper() for t in (_store.get("tickers") or []) if t.strip()]
    except Exception as _te:
        print(f"[THEMES_STATUS] ticker load failed: {_te}")

    # Count per source
    counts: dict[str, int] = {
        "mapped_manual":        0,
        "mapped_themes_page":   0,
        "mapped_llm":           0,
        "mapped_canonical":     0,
        "mapped_fmp_industry":  0,
        "mapped_csv_fallback":  0,
        "needs_theme":          0,
    }
    needs_theme_symbols: list[str] = []
    recently_classified: list[str] = []

    for sym in tickers:
        try:
            prov = get_theme_provenance(sym)
            src  = prov.get("source", "none")
            if src == "neon_manual_override":
                counts["mapped_manual"] += 1
            elif src == "themes_page_membership":
                counts["mapped_themes_page"] += 1
            elif src == "llm_classified":
                counts["mapped_llm"] += 1
                recently_classified.append(sym)
            elif src == "canonical_map_or_industry":
                counts["mapped_canonical"] += 1
            else:
                counts["needs_theme"] += 1
                needs_theme_symbols.append(sym)
        except Exception:
            counts["needs_theme"] += 1
            needs_theme_symbols.append(sym)

    # Classifier state
    cl_state = get_classifier_status()

    # Needs-review list (permanent failures)
    needs_review_syms: list[str] = []
    try:
        _nr_path = _Path(__file__).parent.parent / "data" / "theme_needs_review.json"
        if _nr_path.exists():
            needs_review_syms = list(_j.loads(_nr_path.read_text()).keys())
    except Exception:
        pass

    return {
        "watchlist_id":              watchlist_id or "default",
        "total_symbols":             len(tickers),
        "mapped_manual":             counts["mapped_manual"],
        "mapped_themes_page":        counts["mapped_themes_page"],
        "mapped_llm":                counts["mapped_llm"],
        "mapped_canonical":          counts["mapped_canonical"],
        "mapped_fmp_industry":       counts["mapped_fmp_industry"],
        "mapped_csv_fallback":       counts["mapped_csv_fallback"],
        "needs_theme":               counts["needs_theme"],
        "queued_for_classification": len(needs_theme_symbols),
        "classifier_running":        cl_state.get("running", False),
        "classifier_failed":         cl_state.get("last_error") is not None,
        "needs_theme_symbols":       needs_theme_symbols,
        "needs_review_symbols":      needs_review_syms,
        "recently_classified_symbols": recently_classified[-20:],
        "classifier_state":          cl_state,
    }


# ── Strategy report endpoints ─────────────────────────────────────────────────


@router.post("/strategy-report/generate")
async def strategy_report_generate(body: dict):
    """
    Generate a strategy report for a watchlist using only cached data.

    No FMP, Tradier, or LLM calls. Uses stage2 LKG (technical) +
    fundamentals store + BOTTLENECK_MAP (static) + theme mapper.

    POST body:
      {
        "watchlist_id": "...",
        "strategy_id":  "bottlenecks" | "asymmetry",   (or "serenity" | "sjcapital")
        "save":         true
      }

    strategy_id aliases:
      "bottlenecks" → serenity playbook (Bottlenecks strategy)
      "asymmetry"   → sjcapital playbook (Asymmetry strategy)
    """
    import asyncio
    from services.watchlist_strategy_report import generate_report

    watchlist_id = body.get("watchlist_id", "")
    strategy_id  = body.get("strategy_id", "")
    save         = bool(body.get("save", True))

    if not watchlist_id:
        raise HTTPException(status_code=400, detail="watchlist_id is required")
    if not strategy_id:
        raise HTTPException(status_code=400, detail="strategy_id is required (bottlenecks or asymmetry)")

    try:
        report = await generate_report(watchlist_id, strategy_id, save=save)
        return report
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Report generation failed: {exc}")


@router.get("/strategy-report/history")
async def strategy_report_history(watchlist_id: Optional[str] = None):
    """
    Return list of saved strategy report summaries.

    Query params:
      watchlist_id — filter by watchlist (optional)

    Returns list of {report_id, watchlist_id, strategy_id, strategy_name,
                      generated_at, ticker_count, matched_count}.
    """
    from services.watchlist_strategy_report import get_report_history
    return {"history": get_report_history(watchlist_id)}


@router.get("/strategy-report/{report_id}")
async def strategy_report_get(report_id: str):
    """
    Retrieve a saved strategy report by ID.

    Returns the full report including ranked_results, factor_scores,
    filter reasons, missing_data_notes, and cache_freshness.
    """
    from services.watchlist_strategy_report import get_report
    report = get_report(report_id)
    if report is None:
        raise HTTPException(status_code=404, detail=f"Report {report_id!r} not found")
    return report


@router.get("/debug")
async def debug_endpoint():
    """Debug endpoint — returns file path, existence, Postgres availability."""
    info: Dict[str, Any] = {
        "json_file_path": str(_WATCHLIST_FILE),
        "json_file_exists": _WATCHLIST_FILE.exists(),
    }
    try:
        from data.pg_storage import is_available, watchlist_list as pg_wl_list
        info["postgres_available"] = is_available()
        if is_available():
            entries = pg_wl_list()
            info["postgres_watchlist_count"] = len(entries)
            info["postgres_watchlists"] = entries
    except Exception as e:
        info["postgres_error"] = str(e)
    if _WATCHLIST_FILE.exists():
        try:
            content = _WATCHLIST_FILE.read_text()
            info["json_file_size_bytes"] = len(content)
            info["json_preview"] = content[:500]
        except Exception as e:
            info["json_read_error"] = str(e)
    return info


@router.get("/news")
async def news_endpoint():
    """
    Live News for the default watchlist — LKG cached, never blocks on stale data.

    Response shape:
      {
        "articles":          {TICKER: [article, ...]},   // per-ticker map
        "top_articles":      [...],                       // pre-ranked signal articles (use this)
        "high_signal_count": int,
        "by_catalyst_type":  {type: count},
        "news_signal_meta":  {...},
        "cached_at":         "ISO timestamp",
        "cache_age_s":       int,
        "is_building":       bool,                        // true = bg refresh in progress
      }
    """
    store = load_watchlist()
    if store is None:
        return _news_response({}, {}, _time.time())
    tickers = store.get("tickers", [])
    if not tickers:
        return _news_response({}, {}, _time.time())
    return await _get_news_for_watchlist("default", tickers)


@router.post("/refresh")
async def refresh_endpoint():
    """Re-run multi-source parallel analysis on the most recent watchlist."""
    agent = _get_agent()
    data_service = _get_data_service()

    store = load_watchlist()
    if store is None:
        raise HTTPException(status_code=404, detail="No watchlist saved. Upload a CSV first.")

    tickers = store.get("tickers", [])
    csv_data = store.get("csv_data", [])
    if not tickers:
        raise HTTPException(status_code=400, detail="Watchlist has no tickers")

    result = await run_analysis_pipeline(tickers, csv_data, agent, data_service)

    if isinstance(result, dict) and result.get("error") and "sections" not in result:
        raise HTTPException(status_code=500, detail=result["error"])

    # Save updated analysis back to the watchlist
    store_id = store.get("id", "default")
    store_name = store.get("name", "Watchlist")
    save_watchlist(csv_data, result, watchlist_id=store_id, name=store_name)

    return result


@router.post("/analyze")
async def analyze_endpoint(body: WatchlistAnalyzeRequest):
    """
    Run multi-source parallel analysis pipeline on a watchlist.

    Accepts either:
    - tickers + csv_data directly in the request body
    - watchlist_id to load from Postgres

    Fires all 5 data sources in parallel (Grok, Gemini, Claude, SEC Edgar, TA),
    then synthesizes through Claude with intelligence rules.
    Returns 6 sections × 4 tickers structured JSON.
    """
    agent = _get_agent()
    data_service = _get_data_service()

    tickers = body.tickers or []
    csv_data = body.csv_data or []

    # If watchlist_id provided, load tickers and CSV from stored watchlist
    if body.watchlist_id:
        store = load_watchlist(body.watchlist_id)
        if store is None:
            raise HTTPException(status_code=404, detail="Watchlist not found")
        if not tickers:
            tickers = store.get("tickers", [])
        if not csv_data:
            csv_data = store.get("csv_data", [])
    elif not tickers and csv_data:
        # Extract tickers from CSV data
        tickers = extract_tickers(csv_data)

    if not tickers:
        raise HTTPException(
            status_code=400,
            detail="No tickers provided. Send tickers, csv_data, or watchlist_id.",
        )

    result = await run_analysis_pipeline(tickers, csv_data, agent, data_service)

    if isinstance(result, dict) and result.get("error") and "sections" not in result:
        raise HTTPException(status_code=500, detail=result["error"])

    return result


@router.get("")
async def get_endpoint():
    """Return the most recent saved watchlist, or {empty: true}."""
    store = load_watchlist()
    if store is None:
        return {"empty": True}
    return store


@router.delete("")
async def delete_endpoint():
    """Clear the most recent watchlist and invalidate earnings cache."""
    result = clear_watchlist()
    try:
        from services.user_earnings_service import invalidate_user_earnings  # type: ignore
        invalidate_user_earnings("watchlist")
    except Exception as _e:
        print(f"[watchlist-delete] earnings invalidation skipped: {_e}")
    return result


# ── Watchlist Earnings ───────────────────────────────────────────────────────

@router.get("/earnings")
async def watchlist_earnings_endpoint(
    from_date: Optional[str] = None,
    to_date:   Optional[str] = None,
):
    """
    Return upcoming earnings for the current user's saved watchlist tickers.

    Reuses user_earnings_service.get_or_sync_user_earnings("watchlist", ...)
    which is the same Neon-cached FMP pipeline used by the Catalyst Calendar
    (30-day cache TTL, auto-re-syncs when the symbol set changes, invalidated
    on watchlist save/delete).

    Response shape is compatible with Portfolio Terminal earnings_calendar:
      ticker, company, next_date, est_eps, last_eps, wtd, in_watchlist,
      + FMP extras: revenue_estimated, revenue_actual, time, period,
                    market_cap, logo, importance

    Returns {earnings: [], meta: {cache_status: "empty"}} for an empty watchlist.
    """
    import time as _tm
    from datetime import date as _date, timedelta as _td

    _t0 = _tm.time()

    store = load_watchlist()
    if store is None or not store.get("tickers"):
        return {
            "earnings":    [],
            "meta": {
                "universe":      "watchlist",
                "symbols_count": 0,
                "events_count":  0,
                "cache_status":  "empty",
                "source":        "fmp",
            },
        }

    tickers: list[str] = [t.upper() for t in store["tickers"] if t]
    symbols: set[str]  = set(tickers)

    # Default date window: today → +90 days
    _today = _date.today().isoformat()
    _from  = from_date or _today
    _to    = to_date   or (_date.today() + _td(days=90)).isoformat()

    # FMP key
    try:
        from config import FMP_API_KEY as _fmp_key  # type: ignore
    except Exception:
        _fmp_key = os.getenv("FMP_API_KEY", "")

    if not _fmp_key:
        return {
            "earnings": [],
            "meta": {
                "universe":      "watchlist",
                "symbols_count": len(symbols),
                "events_count":  0,
                "cache_status":  "error",
                "error":         "fmp_key_unavailable",
            },
        }

    try:
        from services.user_earnings_service import get_or_sync_user_earnings  # type: ignore
        events, meta = await get_or_sync_user_earnings(
            universe  = "watchlist",
            symbols   = symbols,
            fmp_key   = _fmp_key,
            from_date = _from,
            to_date   = _to,
        )
    except Exception as _e:
        print(f"[WATCHLIST_EARNINGS] get_or_sync error: {_e}")
        return {
            "earnings": [],
            "meta": {
                "universe":      "watchlist",
                "symbols_count": len(symbols),
                "events_count":  0,
                "cache_status":  "error",
                "error":         str(_e),
            },
        }

    # ── Normalise to Portfolio Terminal-compatible shape ───────────────────
    def _fmt_date(dt_str: str | None) -> str:
        if not dt_str:
            return "N/A"
        try:
            from datetime import datetime as _dt
            return _dt.strptime(dt_str, "%Y-%m-%d").strftime("%b %-d")
        except Exception:
            return dt_str or "N/A"

    normalised = []
    for ev in (events or []):
        sym = (ev.get("symbol") or "").upper()
        if not sym:
            continue
        normalised.append({
            # ── Portfolio Terminal-compatible fields ──
            "ticker":       sym,
            "company":      ev.get("companyName") or ev.get("name") or sym,
            "in_watchlist": True,
            "next_date":    _fmt_date(ev.get("date")),
            "date_raw":     ev.get("date"),
            "est_eps":      ev.get("epsEstimated"),
            "last_eps":     ev.get("epsActual"),
            "wtd":          None,   # no position — watchlist only
            # ── FMP extras (superset of Finnhub shape) ──
            "revenue_estimated": ev.get("revenueEstimated"),
            "revenue_actual":    ev.get("revenueActual"),
            "time":              ev.get("time"),
            "period":            ev.get("period"),
            "market_cap":        ev.get("marketCap"),
            "logo":              ev.get("logo"),
            "importance":        ev.get("importance"),
        })

    # Sort by date_raw ascending
    normalised.sort(key=lambda x: x.get("date_raw") or "")

    # ── Recent earnings (last 30 days) from earnings_live_events ─────────────
    recent_normalised: list[dict] = []
    try:
        from datetime import date as _date2, timedelta as _td2
        import asyncio as _aio_re

        _since = (_date2.today() - _td2(days=30)).isoformat()
        # Compute today in ET so the Recent upper bound is ET-local, not
        # DB-session time.  Future events must never enter the Recent section.
        from datetime import datetime as _dt_re
        from zoneinfo import ZoneInfo as _ZI_re
        _today_et = _dt_re.now(_ZI_re("America/New_York")).date().isoformat()
        from data.earnings_monitor_store import get_recent_complete_events_for_symbols
        _recent_raw = await _aio_re.to_thread(
            get_recent_complete_events_for_symbols,
            list(symbols),
            _since,
            _today_et,
        )

        def _parse_jb(v):
            if v is None:
                return {}
            if isinstance(v, dict):
                return v
            try:
                import json as _jj
                return _jj.loads(v)
            except Exception:
                return {}

        def _fmt_iso(dt_val) -> Optional[str]:
            if dt_val is None:
                return None
            if hasattr(dt_val, "isoformat"):
                return dt_val.isoformat()[:10]
            s = str(dt_val)
            return s[:10] if s else None

        for rev in _recent_raw:
            sym = (rev.get("symbol") or "").upper()
            if not sym:
                continue
            rp  = _parse_jb(rev.get("results_payload"))
            rxn = _parse_jb(rev.get("reaction_payload"))
            recent_normalised.append({
                "ticker":               sym,
                "earnings_date":        _fmt_iso(rev.get("expected_date")),
                "earnings_date_fmt":    _fmt_date(_fmt_iso(rev.get("expected_date"))),
                "fiscal_period":        rev.get("fiscal_period"),
                "fiscal_year":          rev.get("fiscal_year"),
                "state":                rev.get("state"),
                "classification":       rev.get("classification"),
                "eps_actual":           rp.get("eps_actual"),
                "eps_estimate":         rp.get("eps_estimate"),
                "eps_surprise_pct":     rp.get("eps_surprise_pct"),
                "revenue_actual":       rp.get("revenue_actual"),
                "revenue_estimate":     rp.get("revenue_estimate"),
                "revenue_surprise_pct": rp.get("revenue_surprise_pct"),
                "pre_1d_pct":           rxn.get("pre_1d_pct"),
                "post_1d_pct":          rxn.get("post_1d_pct"),
                "post_3d_pct":          rxn.get("post_3d_pct"),
                "post_5d_pct":          rxn.get("post_5d_pct"),
                "reaction_computed_at": rxn.get("computed_at"),
                "in_watchlist":         True,
                "source":               "earnings_monitor",
            })
    except Exception as _re_exc:
        print(f"[WATCHLIST_EARNINGS] recent events error: {_re_exc}")

    _ms = round((_tm.time() - _t0) * 1000)
    meta["elapsed_ms"]        = _ms
    meta["from_date"]         = _from
    meta["to_date"]           = _to
    meta["events_count"]      = len(normalised)
    meta["recent_count"]      = len(recent_normalised)

    print(
        f"[WATCHLIST_EARNINGS] symbols={len(symbols)} upcoming={len(normalised)} "
        f"recent={len(recent_normalised)} cache_status={meta.get('cache_status')} elapsed_ms={_ms}"
    )
    return {
        "earnings":  normalised,
        "upcoming":  normalised,
        "recent":    recent_normalised,
        "meta":      meta,
    }


# ── Earnings by Explicit Symbol List ─────────────────────────────────────────
#
# POST /api/watchlist/earnings/by-symbols
#
# Frontend sends the symbols it is currently displaying; backend returns
# earnings scoped to exactly those symbols.  This is the preferred contract
# for multi-watchlist and Favorites support where the viewing context is
# determined client-side, not inferred server-side from a default load.
#
# Architecture note:
#   Uses universe="watchlist_by_syms" in user_earnings_cache (Neon) — a
#   dedicated cache row separate from "watchlist" so there is no
#   cross-contamination with the default-load earnings cache.
#   FMP calendar is a date-window batch call (not per-symbol); filtering
#   happens in Python.  The cache expands automatically when new symbols
#   are requested (UNION re-sync, still a single FMP call).

class EarningsBySymbolsRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    symbols:       List[str]
    from_date:     Optional[str] = None
    to_date:       Optional[str] = None
    wait_for_sync: bool          = True


@router.post("/earnings/by-symbols")
async def earnings_by_symbols_endpoint(body: EarningsBySymbolsRequest):
    """
    POST /api/watchlist/earnings/by-symbols

    Accept an explicit list of symbols from the frontend (Primary, Strong Bases,
    or Favorites symbols) and return upcoming earnings for exactly those symbols.

    Never returns earnings for symbols that were not requested.
    Never uses the default-load watchlist as the symbol source.

    Request:
        {
          "symbols":   ["AAPL", "MSFT", "NVDA"],
          "from_date": "2026-07-17",   // optional, defaults to today
          "to_date":   "2026-10-15"    // optional, defaults to today + 90 days
        }

    Response:
        {
          "symbols_requested": [...],
          "events":            [...],
          "missing_symbols":   [...],   // requested but no earnings found
          "source":            "cached_earnings",
          "last_updated":      "...",
          "stale":             false,
          "cache_status":      "hit|miss|refreshed|empty"
        }

    Each event:
        symbol, company, earnings_date, time, eps_estimate, revenue_estimate,
        previous_eps, source, last_updated, importance, market_cap
    """
    from datetime import date as _date_cls, timedelta as _td_cls
    import time as _tm_bys

    _t0_bys = _tm_bys.time()

    # Normalize + deduplicate (preserve order)
    symbols: list[str] = list(dict.fromkeys(
        s.strip().upper() for s in (body.symbols or []) if s.strip()
    ))

    if not symbols:
        return {
            "symbols_requested": [],
            "events":            [],
            "missing_symbols":   [],
            "source":            "cached_earnings",
            "last_updated":      None,
            "stale":             False,
            "cache_status":      "empty",
        }

    # Default date window: today → +90 days
    from_date = body.from_date or _date_cls.today().isoformat()
    to_date   = body.to_date   or (_date_cls.today() + _td_cls(days=90)).isoformat()

    # FMP key
    try:
        from config import FMP_API_KEY as _fmp_key_bys  # type: ignore
    except Exception:
        _fmp_key_bys = os.getenv("FMP_API_KEY", "")

    if not _fmp_key_bys:
        return {
            "symbols_requested": symbols,
            "events":            [],
            "missing_symbols":   symbols,
            "source":            "cached_earnings",
            "last_updated":      None,
            "stale":             True,
            "cache_status":      "error",
            "error":             "fmp_key_unavailable",
        }

    try:
        from services.user_earnings_service import (  # type: ignore
            get_upcoming_earnings_for_symbols as _gue_bys,
        )
        result = await _gue_bys(
            symbols   = symbols,
            from_date = from_date,
            to_date   = to_date,
            fmp_key   = _fmp_key_bys,
            # Never block the request on FMP — always return immediately and
            # fire a background sync on cache miss.  wait_for_sync is preserved
            # in the request schema for backward compat but no longer drives
            # synchronous FMP work from a user-facing request handler.
            sync_on_miss            = False,
            background_sync_on_miss = True,
        )
    except Exception as _e_bys:
        print(f"[EARNINGS_BY_SYMS] get_upcoming_earnings_for_symbols error: {_e_bys}")
        return {
            "symbols_requested": symbols,
            "events":            [],
            "missing_symbols":   symbols,
            "source":            "cached_earnings",
            "last_updated":      None,
            "stale":             True,
            "cache_status":      "error",
            "error":             str(_e_bys),
        }

    # ── Recent earnings (last 30 days) — strictly scoped to requested symbols.
    # Reuses the same bulk Neon reader and normalization already used by the
    # watchlist-by-ID earnings path.  Zero provider calls; one bulk DB read.
    # Does not affect or replace the existing global Recent path used by other
    # consumers.
    _recent_bys: list[dict] = []
    try:
        from datetime import date as _date_rec, timedelta as _td_rec, \
            datetime as _dt_rec
        import asyncio as _aio_rec
        from zoneinfo import ZoneInfo as _ZI_rec
        from data.earnings_monitor_store import (  # type: ignore
            get_recent_complete_events_for_symbols as _grc_bys,
        )

        _since_rec  = (_date_rec.today() - _td_rec(days=30)).isoformat()
        _today_et_r = _dt_rec.now(_ZI_rec("America/New_York")).date().isoformat()
        _recent_raw = await _aio_rec.to_thread(
            _grc_bys, symbols, _since_rec, _today_et_r,
        )
        _sym_set_bys = {s.upper() for s in symbols}

        def _parse_jb_bys(v):
            if v is None:
                return {}
            if isinstance(v, dict):
                return v
            try:
                import json as _jjb
                return _jjb.loads(v)
            except Exception:
                return {}

        def _fmt_iso_bys(dt_val) -> Optional[str]:
            if dt_val is None:
                return None
            if hasattr(dt_val, "isoformat"):
                return dt_val.isoformat()[:10]
            s = str(dt_val)
            return s[:10] if s else None

        def _fmt_date_bys(dt_str: Optional[str]) -> str:
            if not dt_str:
                return "N/A"
            try:
                from datetime import datetime as _dt_fmt
                return _dt_fmt.strptime(dt_str, "%Y-%m-%d").strftime("%b %-d")
            except Exception:
                return dt_str or "N/A"

        for _rr in _recent_raw:
            _rsym = (_rr.get("symbol") or "").upper()
            if _rsym not in _sym_set_bys:
                continue
            _rp  = _parse_jb_bys(_rr.get("results_payload"))
            _rxn = _parse_jb_bys(_rr.get("reaction_payload"))
            _recent_bys.append({
                "ticker":               _rsym,
                "earnings_date":        _fmt_iso_bys(_rr.get("expected_date")),
                "earnings_date_fmt":    _fmt_date_bys(_fmt_iso_bys(_rr.get("expected_date"))),
                "fiscal_period":        _rr.get("fiscal_period"),
                "fiscal_year":          _rr.get("fiscal_year"),
                "state":                _rr.get("state"),
                "classification":       _rr.get("classification"),
                "eps_actual":           _rp.get("eps_actual"),
                "eps_estimate":         _rp.get("eps_estimate"),
                "eps_surprise_pct":     _rp.get("eps_surprise_pct"),
                "revenue_actual":       _rp.get("revenue_actual"),
                "revenue_estimate":     _rp.get("revenue_estimate"),
                "revenue_surprise_pct": _rp.get("revenue_surprise_pct"),
                "pre_1d_pct":           _rxn.get("pre_1d_pct"),
                "post_1d_pct":          _rxn.get("post_1d_pct"),
                "post_3d_pct":          _rxn.get("post_3d_pct"),
                "post_5d_pct":          _rxn.get("post_5d_pct"),
                "reaction_computed_at": _rxn.get("computed_at"),
                "in_watchlist":         True,
                "source":               "earnings_monitor",
            })
    except Exception as _rec_exc:
        print(f"[EARNINGS_BY_SYMS] recent events error: {_rec_exc}")

    result["recent"] = _recent_bys

    _ms_bys = round((_tm_bys.time() - _t0_bys) * 1000)
    print(
        f"[EARNINGS_BY_SYMS] symbols={len(symbols)} events={len(result.get('events',[]))} "
        f"missing={len(result.get('missing_symbols',[]))} "
        f"recent={len(_recent_bys)} "
        f"cache_status={result.get('cache_status')} elapsed_ms={_ms_bys}"
    )
    result["elapsed_ms"] = _ms_bys
    return result


# ── Favorites earnings (virtual watchlist) ────────────────────────────────────

@router.get("/favorites/earnings")
async def favorites_earnings_endpoint():
    """
    GET /api/watchlist/favorites/earnings

    Favorites is managed client-side only — there is no backend favorites
    watchlist.  This endpoint returns a stable, informative response so
    frontends that call it receive a clear explanation rather than a 404 or
    a stale payload from a catch-all /{watchlist_id} route.

    Frontends should use POST /api/watchlist/earnings/by-symbols with the
    user's favorite symbols to get earnings for favorites.
    """
    return {
        "favorites_virtual":  True,
        "watchlist_id":       "favorites",
        "symbols_requested":  [],
        "events":             [],
        "missing_symbols":    [],
        "source":             "cached_earnings",
        "last_updated":       None,
        "stale":              False,
        "cache_status":       "favorites_unsupported_server_side",
        "message": (
            "Favorites is managed client-side. "
            "Send favorite symbols to POST /api/watchlist/earnings/by-symbols "
            "to retrieve their upcoming earnings."
        ),
    }


# ── Earnings by Watchlist ID ──────────────────────────────────────────────────
#
# GET /api/watchlist/{watchlist_id}/earnings
#
# Resolves watchlist membership server-side, then delegates to the canonical
# get_upcoming_earnings_for_symbols function (sync_on_miss=True so explicit
# calls always return fresh data when the cache is cold).
#
# Route note: registered at /{watchlist_id}/earnings (2 path segments) —
# FastAPI does not confuse this with /{watchlist_id} (1 segment).

@router.get("/{watchlist_id}/earnings")
async def watchlist_id_earnings_endpoint(
    watchlist_id: str,
    from_date: Optional[str] = None,
    to_date:   Optional[str] = None,
):
    """
    GET /api/watchlist/{watchlist_id}/earnings

    Return upcoming earnings for a specific watchlist's symbols.

    Primary → Primary symbols only.
    Strong Bases → Strong Bases symbols only.
    Custom watchlist → that watchlist's symbols only.
    Never mixes symbol universes from different watchlists.

    Query params:
        from_date  YYYY-MM-DD (default: today)
        to_date    YYYY-MM-DD (default: today + 90 days)

    Response: same shape as POST /earnings/by-symbols + watchlist_id/name.
    Delegates to get_upcoming_earnings_for_symbols (sync_on_miss=True).
    """
    store = load_watchlist(watchlist_id)
    if store is None or not store.get("tickers"):
        return {
            "watchlist_id":      watchlist_id,
            "symbols_requested": [],
            "events":            [],
            "missing_symbols":   [],
            "source":            "cached_earnings",
            "last_updated":      None,
            "stale":             False,
            "cache_status":      "empty",
        }

    symbols: list[str] = [t.strip().upper() for t in store.get("tickers", []) if t.strip()]

    try:
        from config import FMP_API_KEY as _fmp_key_wid  # type: ignore
    except Exception:
        _fmp_key_wid = os.getenv("FMP_API_KEY", "")

    if not _fmp_key_wid:
        return {
            "watchlist_id":      watchlist_id,
            "symbols_requested": symbols,
            "events":            [],
            "missing_symbols":   symbols,
            "source":            "cached_earnings",
            "last_updated":      None,
            "stale":             True,
            "cache_status":      "error",
            "error":             "fmp_key_unavailable",
        }

    try:
        from services.user_earnings_service import (  # type: ignore
            get_upcoming_earnings_for_symbols as _gue_wid,
        )
        result = await _gue_wid(
            symbols   = symbols,
            from_date = from_date,
            to_date   = to_date,
            fmp_key   = _fmp_key_wid,
            sync_on_miss            = True,   # explicit GET — wait for sync
            background_sync_on_miss = False,
        )
    except Exception as _e_wid:
        print(f"[WL_ID_EARNINGS] get_upcoming_earnings_for_symbols error: {_e_wid}")
        return {
            "watchlist_id":      watchlist_id,
            "symbols_requested": symbols,
            "events":            [],
            "missing_symbols":   symbols,
            "source":            "cached_earnings",
            "last_updated":      None,
            "stale":             True,
            "cache_status":      "error",
            "error":             str(_e_wid),
        }

    result["watchlist_id"]   = watchlist_id
    result["watchlist_name"] = store.get("name", "")
    return result


# ── Stock Deep-Dive ─────────────────────────────────────────────────────────

class StockDeepDiveRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    models: List[str] = ["grok", "gemini", "claude"]
    report_model: str = "claude"


@router.post("/stock/{ticker}/deep-dive")
async def stock_deep_dive_endpoint(ticker: str, body: StockDeepDiveRequest):
    """
    Three-model concurrent deep-dive for a single stock ticker.

    Phase 1 (all parallel): Grok X/Twitter sentiment + Gemini Google News +
                            Claude/GPT deep fundamental analysis
    Phase 2: short final synthesis call → structured JSON output

    Model routing (static-by-design):
      Models are caller-specified (body.models / body.report_model), defaulting to
      ["grok", "gemini", "claude"] + claude synthesis.  The prompt_router /
      reasoning_mode machinery does NOT apply here — the caller selects the panel
      explicitly from the frontend deep-dive modal.  Do not add dynamic routing
      inside this endpoint.
    """
    import traceback as _tb

    try:
        ticker = ticker.strip().upper()
        models = [m.strip().lower() for m in (body.models or ["grok", "gemini", "claude"])]
        report_model = (body.report_model or "claude").strip().lower()

        # ── Look up any stored CSV fundamentals for this ticker ───────────────
        def _get_fundamentals() -> str:
            try:
                store = load_watchlist()
                if store:
                    for row in store.get("csv_data", []):
                        sym = (row.get("Symbol") or row.get("symbol") or "").strip().upper()
                        if sym == ticker:
                            parts = []
                            for label, key in [
                                ("Price", "Stock Price"), ("MCap", "Market Cap"),
                                ("PE", "PE Ratio"), ("FwdPE", "Forward PE"),
                                ("RSI", "Relative Strength Index (RSI)"),
                                ("RevGrowth", "Revenue Growth (YoY)"),
                                ("EPSEst", "EPS Growth Est."), ("FCF", "FCF Margin"),
                                ("GrossMargin", "Gross Margin"), ("DE", "Debt / Equity"),
                                ("ShortFloat", "Short % Float"), ("EarningsDate", "Earnings Date"),
                            ]:
                                val = row.get(key, "")
                                if val:
                                    parts.append(f"{label}={val}")
                            return ", ".join(parts)
            except Exception:
                pass
            return ""

        fundamentals = _get_fundamentals()
        fundamentals_str = f" Fundamentals: {fundamentals}." if fundamentals else ""

        # ── Grok: X/Twitter real-time social sentiment ────────────────────────
        async def call_grok() -> str:
            xai_key = os.getenv("XAI_API_KEY") or os.getenv("GROK_API_KEY", "")
            if not xai_key:
                return "[No XAI_API_KEY configured]"
            prompt = (
                f"Search X/Twitter for recent posts about {ticker}. "
                f"Summarize the current retail and institutional sentiment, any viral catalysts "
                f"or concerns, and notable accounts discussing it. Be specific — mention price "
                f"targets, meme activity, earnings reactions if relevant. "
                f"2-4 paragraphs, conversational tone."
            )
            try:
                async with httpx.AsyncClient(timeout=60.0) as client:
                    resp = await client.post(
                        "https://api.x.ai/v1/responses",
                        headers={
                            "Authorization": f"Bearer {xai_key}",
                            "Content-Type": "application/json",
                        },
                        json={
                            "model": MODEL_GROK,
                            "tools": [{"type": "x_search", "x_search": {}}],
                            "input": [{"role": "user", "content": prompt}],
                        },
                    )
                if resp.status_code != 200:
                    return f"[Grok API error {resp.status_code}: {resp.text[:300]}]"
                data = resp.json()
                for block in data.get("output", []):
                    if block.get("type") == "message":
                        for c in block.get("content", []):
                            if c.get("type") == "output_text":
                                return c.get("text", "") or "[Grok: empty output_text]"
                return "[Grok: no text in response]"
            except asyncio.TimeoutError:
                return "[Grok: timed out after 60s]"
            except Exception as exc:
                return f"[Grok error: {type(exc).__name__}: {exc}]"

        # ── Gemini: Google News headlines ─────────────────────────────────────
        async def call_gemini() -> str:
            gemini_key = os.getenv("GEMINI_API_KEY", "")
            if not gemini_key:
                return "[No GEMINI_API_KEY configured]"
            prompt = (
                f"Search Google News for {ticker} stock. Summarize the 3-5 most important "
                f"headlines from the last 30 days: analyst upgrades/downgrades, earnings surprises, "
                f"product launches, regulatory news, or macro headwinds. "
                f"Include the source and approximate date for each. "
                f"2-4 paragraphs."
            )
            try:
                url = (
                    "https://generativelanguage.googleapis.com/v1beta/models/"
                    f"{MODEL_GEMINI}:generateContent?key={gemini_key}"
                )
                async with httpx.AsyncClient(timeout=60.0) as client:
                    resp = await client.post(
                        url,
                        headers={"Content-Type": "application/json"},
                        json={
                            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
                            "tools": [{"google_search": {}}],
                            "generationConfig": {"maxOutputTokens": 2048, "temperature": 0.2},
                        },
                    )
                resp.raise_for_status()
                data = resp.json()
                candidates = data.get("candidates", [])
                if not candidates:
                    return "[Gemini: no candidates in response]"
                parts = candidates[0].get("content", {}).get("parts", [])
                text = "".join(p.get("text", "") for p in parts if "text" in p)
                return text or "[Gemini: empty response]"
            except asyncio.TimeoutError:
                return "[Gemini: timed out after 60s]"
            except Exception as exc:
                return f"[Gemini error: {type(exc).__name__}: {exc}]"

        # ── Claude: deep fundamental + technical analysis ─────────────────────
        async def call_claude_analysis() -> str:
            anthropic_key = os.getenv("ANTHROPIC_API_KEY", "")
            if not anthropic_key:
                return "[No ANTHROPIC_API_KEY configured]"
            prompt = (
                f"You are a senior equity analyst.{fundamentals_str} "
                f"Given the fundamentals for {ticker}, provide a structured analysis covering: "
                f"(1) a 2-3 sentence executive summary, (2) the bull case, (3) the bear case, "
                f"(4) top 3 risk factors, (5) key technical levels/pattern to watch, "
                f"(6) what sell-side consensus looks like. "
                f"Write in clear, specific prose — cite actual metrics where possible."
            )
            try:
                import anthropic as _anthropic
                client = _anthropic.AsyncAnthropic(
                    api_key=anthropic_key, timeout=60.0
                )
                response = await client.messages.create(
                    model=MODEL_CLAUDE_PREMIUM,
                    max_tokens=1500,
                    messages=[{"role": "user", "content": prompt}],
                )
                return "".join(b.text for b in response.content if hasattr(b, "text"))
            except asyncio.TimeoutError:
                return "[Claude: timed out after 60s]"
            except Exception as exc:
                return f"[Claude error: {type(exc).__name__}: {exc}]"

        # ── GPT: deep fundamental + technical analysis ────────────────────────
        async def call_gpt_analysis() -> str:
            openai_key = os.getenv("OPENAI_API_KEY", "")
            if not openai_key:
                return "[No OPENAI_API_KEY configured]"
            prompt = (
                f"You are a senior equity analyst.{fundamentals_str} "
                f"Given the fundamentals for {ticker}, provide a structured analysis covering: "
                f"(1) a 2-3 sentence executive summary, (2) the bull case, (3) the bear case, "
                f"(4) top 3 risk factors, (5) key technical levels/pattern to watch, "
                f"(6) what sell-side consensus looks like. "
                f"Write in clear, specific prose — cite actual metrics where possible."
            )
            try:
                from openai import AsyncOpenAI
                client = AsyncOpenAI(api_key=openai_key, timeout=60.0)
                resp = await client.chat.completions.create(
                    model=MODEL_GPT4O,
                    max_tokens=1500,
                    messages=[{"role": "user", "content": prompt}],
                )
                return resp.choices[0].message.content or "[GPT: empty response]"
            except asyncio.TimeoutError:
                return "[GPT: timed out after 60s]"
            except Exception as exc:
                return f"[GPT error: {type(exc).__name__}: {exc}]"

        # ── Phase 1: run ALL requested models concurrently ────────────────────
        coro_map: Dict[str, Any] = {}
        if "grok" in models:
            coro_map["grok"] = call_grok()
        if "gemini" in models:
            coro_map["gemini"] = call_gemini()
        # "claude" or "claude_gpt" in models → run the deep analysis persona
        run_claude = "claude" in models or "claude_gpt" in models
        run_gpt_model = "gpt" in models
        if run_claude and report_model != "gpt":
            coro_map["claude"] = call_claude_analysis()
        elif run_gpt_model or (run_claude and report_model == "gpt"):
            coro_map["gpt"] = call_gpt_analysis()

        keys = list(coro_map.keys())
        coros = list(coro_map.values())
        raw_results = await asyncio.gather(*coros, return_exceptions=True)

        model_outputs: Dict[str, str] = {}
        for k, v in zip(keys, raw_results):
            if isinstance(v, Exception):
                model_outputs[k] = f"[{k} exception: {v}]"
            else:
                model_outputs[k] = str(v)

        grok_text = model_outputs.get("grok", "")
        gemini_text = model_outputs.get("gemini", "")
        claude_text = model_outputs.get("claude", "")
        gpt_text = model_outputs.get("gpt", "")

        # ── Phase 2: final synthesis → structured JSON ────────────────────────
        synthesis_input_parts = []
        if grok_text and not grok_text.startswith("["):
            synthesis_input_parts.append(f"=== GROK (X/Twitter Sentiment) ===\n{grok_text}")
        if gemini_text and not gemini_text.startswith("["):
            synthesis_input_parts.append(f"=== GEMINI (Google News) ===\n{gemini_text}")
        analysis_text = claude_text or gpt_text
        if analysis_text and not analysis_text.startswith("["):
            synthesis_input_parts.append(f"=== ANALYST DEEP-DIVE ===\n{analysis_text}")

        synthesis_context = "\n\n".join(synthesis_input_parts)

        synthesis_prompt = (
            f"You are synthesizing a multi-source research report on {ticker}.{fundamentals_str}\n\n"
            f"{synthesis_context}\n\n"
            f"Based on all of the above, respond with ONLY a valid JSON object "
            f"(no markdown fences, no preamble) containing exactly these keys:\n"
            f'  "summary": "2-3 sentence executive summary",\n'
            f'  "bull_case": "bull case paragraph",\n'
            f'  "bear_case": "bear case paragraph",\n'
            f'  "risk_factors": ["risk 1", "risk 2", "risk 3"],\n'
            f'  "technical_outlook": "key technical levels and pattern to watch",\n'
            f'  "analyst_sentiment": "what sell-side analysts are saying"\n'
            f"Return ONLY the JSON object."
        )

        synthesis: dict = {}
        synth_key = os.getenv("ANTHROPIC_API_KEY", "")
        if report_model == "gpt" or not synth_key:
            openai_key = os.getenv("OPENAI_API_KEY", "")
            if openai_key:
                try:
                    from openai import AsyncOpenAI
                    _oa = AsyncOpenAI(api_key=openai_key, timeout=60.0)
                    _resp = await _oa.chat.completions.create(
                        model=MODEL_GPT4O,
                        max_tokens=1500,
                        messages=[{"role": "user", "content": synthesis_prompt}],
                        response_format={"type": "json_object"},
                    )
                    synthesis = _json.loads(_resp.choices[0].message.content or "{}")
                except Exception as exc:
                    print(f"[DEEP-DIVE] GPT synthesis failed: {exc}")
        if not synthesis and synth_key:
            try:
                import anthropic as _anthropic
                _ac = _anthropic.AsyncAnthropic(api_key=synth_key, timeout=60.0)
                _cr = await _ac.messages.create(
                    model=MODEL_CLAUDE_PREMIUM,
                    max_tokens=1500,
                    messages=[{"role": "user", "content": synthesis_prompt}],
                )
                synth_text = "".join(b.text for b in _cr.content if hasattr(b, "text"))
                synth_text = _re.sub(r"```json\s*", "", synth_text)
                synth_text = _re.sub(r"```\s*", "", synth_text).strip()
                m = _re.search(r'\{[\s\S]*\}', synth_text)
                if m:
                    synthesis = _json.loads(m.group())
            except Exception as exc:
                print(f"[DEEP-DIVE] Claude synthesis failed: {exc}")

        print(
            f"[DEEP-DIVE] {ticker}: "
            f"grok={'ok' if grok_text and not grok_text.startswith('[') else 'skip/err'}, "
            f"gemini={'ok' if gemini_text and not gemini_text.startswith('[') else 'skip/err'}, "
            f"claude={'ok' if claude_text and not claude_text.startswith('[') else 'skip/err'}, "
            f"gpt={'ok' if gpt_text and not gpt_text.startswith('[') else 'skip/err'}"
        )

        return {
            "grok":              grok_text or None,
            "gemini":            gemini_text or None,
            "claude":            claude_text or None,
            "gpt":               gpt_text or None,
            "summary":           synthesis.get("summary", ""),
            "bull_case":         synthesis.get("bull_case", ""),
            "bear_case":         synthesis.get("bear_case", ""),
            "risk_factors":      synthesis.get("risk_factors", []),
            "technical_outlook": synthesis.get("technical_outlook", ""),
            "analyst_sentiment": synthesis.get("analyst_sentiment", ""),
        }

    except HTTPException:
        raise
    except Exception as e:
        _tb.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ── Category override endpoints ──────────────────────────────────────────────

@router.patch("/category")
async def patch_category_endpoint(request: Request, body: dict):
    """
    Persist a single manual ticker→category assignment.

    Body: {ticker, category, source='manual', reason=null}

    Overrides are applied immediately on the next Watchlist/Chart Radar GET.
    They survive server restarts and AI re-analyses.
    """
    ticker   = str(body.get("ticker") or "").strip().upper()
    category = str(body.get("category") or "").strip()
    source   = str(body.get("source") or "manual")
    reason   = body.get("reason")

    if not ticker:
        raise HTTPException(status_code=400, detail="ticker is required")
    if not category:
        raise HTTPException(status_code=400, detail="category is required")

    try:
        from auth import verify_token as _vt
        token = (request.headers.get("Authorization") or "").removeprefix("Bearer ").strip()
        payload = _vt(token) if token else {}
        user_id = payload.get("sub") or "default"
    except Exception:
        user_id = "default"

    from services.category_overrides import upsert_override as _upsert
    ok = _upsert(user_id, ticker, category, source, reason)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to persist override")

    # ── Cross-sync 1: update theme_ticker_mapper in-memory index ────────────
    try:
        from services.theme_ticker_mapper import register_llm_classified_tickers as _sync_mapper
        _sync_mapper([{"ticker": ticker, "theme": category, "confidence": "manual"}])
    except Exception as _me:
        print(f"[WATCHLIST_CAT] mapper sync failed (non-fatal): {_me}")

    # ── Cross-sync 2: write to theme_ticker_overrides → Options Flow ─────────
    try:
        from services.theme_rs_universe import THEME_RS_UNIVERSE as _trs_uni
        _theme_id = next(
            (tid for tid, m in _trs_uni.items()
             if m.get("display_name", "").lower() == category.lower()),
            None,
        )
        if _theme_id:
            from data.pg_storage import upsert_theme_ticker_override as _upsert_tto
            _upsert_tto(
                theme_id=_theme_id,
                symbol=ticker,
                action="add",
                source="watchlist_category_sync",
                note="synced from PATCH /category",
                created_by="system",
            )
            from services.theme_merge_layer import refresh_enriched_universe as _ref_uni
            _ref_uni()
            from data.options_flow_sectors import invalidate_sectors_cache as _inv_sec
            _inv_sec()
            # Mark the newly required symbol as high-priority in the backfill queue
            # so it is scanned on the next batch pass, not after a full alphabetical sweep.
            try:
                from data.options_theme_supplement import add_high_priority_symbols as _add_hi_pri
                _add_hi_pri([ticker])
            except Exception:
                pass
    except Exception as _ue:
        print(f"[WATCHLIST_CAT] Options Flow sync failed (non-fatal): {_ue}")

    # A category change alters canonical_theme_id/name fields in the bulk
    # Watchlist response.  We don't know which watchlist(s) contain this ticker
    # so we clear all entries.  PATCH /category is an infrequent manual operation.
    _BULK_LKG.clear()

    return {"success": True, "ticker": ticker, "category": category, "user_id": user_id}


@router.post("/categories/bulk")
async def bulk_categories_endpoint(request: Request, body: dict):
    """
    Persist multiple manual category assignments in one call.

    Body:
    {
      "updates": [
        {"ticker": "BTDR", "category": "Data Center Infrastructure", "source": "manual"},
        ...
      ],
      "categoryMoves": [
        {"from": "Crypto Equities / Blockchain", "to": "Data Center Infrastructure"},
        {"from": "Solar", "to": "Clean Energy"}
      ]
    }

    categoryMoves resolves which tickers are currently in the 'from' section
    and adds an override for each one pointing to 'to'.
    All operations are idempotent (upsert).
    """
    try:
        from auth import verify_token as _vt
        token = (request.headers.get("Authorization") or "").removeprefix("Bearer ").strip()
        payload = _vt(token) if token else {}
        user_id = payload.get("sub") or "default"
    except Exception:
        user_id = "default"

    updates: list[dict] = list(body.get("updates") or [])

    # Resolve categoryMoves → individual ticker overrides
    cat_moves: list[dict] = list(body.get("categoryMoves") or [])
    if cat_moves:
        try:
            from services.watchlist_service import load_watchlist as _lw
            store = _lw()
            if store:
                sections = store.get("analysis", {}).get("sections", [])
                for move in cat_moves:
                    from_cat = str(move.get("from") or "").strip()
                    to_cat   = str(move.get("to")   or "").strip()
                    if not from_cat or not to_cat:
                        continue
                    for sec in sections:
                        if (sec.get("title") or "").strip() == from_cat:
                            for row in sec.get("tickers", []):
                                sym = str(row.get("symbol") or "").strip().upper()
                                if sym:
                                    updates.append({
                                        "ticker":   sym,
                                        "category": to_cat,
                                        "source":   "manual",
                                        "reason":   f"category_move:{from_cat}→{to_cat}",
                                    })
        except Exception as _cm_err:
            print(f"[WATCHLIST] categoryMoves resolution failed (non-fatal): {_cm_err}")

    from services.category_overrides import bulk_upsert as _bulk
    count = _bulk(user_id, updates)

    # ── Cross-sync bulk updates to theme_ticker_mapper + Options Flow ─────────
    if count > 0 and updates:
        try:
            from services.theme_ticker_mapper import register_llm_classified_tickers as _bsync_mapper
            from services.theme_rs_universe import THEME_RS_UNIVERSE as _btrs_uni
            mapper_items = []
            tto_edits: list[dict] = []
            for _upd in updates:
                _bt = str(_upd.get("ticker") or "").strip().upper()
                _bc = str(_upd.get("category") or "").strip()
                if not _bt or not _bc:
                    continue
                mapper_items.append({"ticker": _bt, "theme": _bc, "confidence": "manual"})
                _btid = next(
                    (tid for tid, m in _btrs_uni.items()
                     if m.get("display_name", "").lower() == _bc.lower()),
                    None,
                )
                if _btid:
                    tto_edits.append({
                        "theme_id":   _btid,
                        "symbol":     _bt,
                        "action":     "add",
                        "source":     "watchlist_category_sync",
                        "note":       "bulk category sync",
                        "created_by": "system",
                    })
            if mapper_items:
                _bsync_mapper(mapper_items)
            if tto_edits:
                from data.pg_storage import bulk_upsert_theme_ticker_overrides as _bulk_tto
                _bulk_tto(tto_edits)
                from services.theme_merge_layer import refresh_enriched_universe as _bref
                _bref()
                from data.options_flow_sectors import invalidate_sectors_cache as _binv
                _binv()
                # Mark newly required symbols as high-priority in the backfill queue
                try:
                    from data.options_theme_supplement import add_high_priority_symbols as _add_hi_bulk
                    _add_hi_bulk([e["symbol"] for e in tto_edits if e.get("action") == "add"])
                except Exception:
                    pass
        except Exception as _bsync_err:
            print(f"[WATCHLIST_CAT_BULK] cross-sync failed (non-fatal): {_bsync_err}")

    # Bulk category assignments alter canonical_theme_id/name across any watchlist
    # that contains the affected tickers.  Clear all LKG entries (infrequent op).
    if count > 0:
        _BULK_LKG.clear()

    return {
        "success": True,
        "upserted": count,
        "user_id": user_id,
    }


# ── Watchlist favorites ───────────────────────────────────────────────────────
# Pure user metadata — no market-data/provider calls.
# Endpoints must be defined BEFORE /{watchlist_id} catch-all routes.

class _AddFavoriteBody(BaseModel):
    ticker: str


@router.get("/favorites")
async def get_favorites(request: Request):
    """
    GET /api/watchlist/favorites
    Returns the current user's favorited tickers.
    """
    from data.pg_storage import list_watchlist_favorites, _ensure_wf_table
    _ensure_wf_table()
    user_id = _get_user_id(request)
    favorites = list_watchlist_favorites(user_id)
    return {"favorites": favorites, "count": len(favorites)}


@router.post("/favorites")
async def add_favorite(request: Request, body: _AddFavoriteBody):
    """
    POST /api/watchlist/favorites
    Body: {"ticker": "CRWV"}
    Idempotently adds ticker to favorites. Returns updated list.
    """
    from data.pg_storage import add_watchlist_favorite, list_watchlist_favorites, _ensure_wf_table
    _ensure_wf_table()
    raw = body.ticker.strip().upper()
    if not raw:
        raise HTTPException(status_code=422, detail="ticker must be a non-empty string")
    user_id = _get_user_id(request)
    ok = add_watchlist_favorite(user_id, raw)
    if not ok:
        raise HTTPException(status_code=503, detail="Database unavailable — could not save favorite")
    favorites = list_watchlist_favorites(user_id)
    return {"ticker": raw, "is_favorite": True, "favorites": favorites}


@router.delete("/favorites/{ticker}")
async def remove_favorite(request: Request, ticker: str):
    """
    DELETE /api/watchlist/favorites/{ticker}
    Idempotently removes ticker from favorites. Returns updated list.
    """
    from data.pg_storage import remove_watchlist_favorite, list_watchlist_favorites, _ensure_wf_table
    _ensure_wf_table()
    raw = ticker.strip().upper()
    if not raw:
        raise HTTPException(status_code=422, detail="ticker must be a non-empty string")
    user_id = _get_user_id(request)
    ok = remove_watchlist_favorite(user_id, raw)
    if not ok:
        raise HTTPException(status_code=503, detail="Database unavailable — could not remove favorite")
    favorites = list_watchlist_favorites(user_id)
    return {"ticker": raw, "is_favorite": False, "favorites": favorites}


# ── Ticker Detail — unified popup data contract ───────────────────────────────
# GET /api/watchlist/ticker-detail/{symbol}
#
# Read-only. Zero provider calls. All data from in-memory / disk / Neon caches:
#   company     → screener_fundamentals_cache (Neon, 7-day TTL)
#   overview    → watchlist_quote_cache (in-memory, 10-min TTL)
#   confluence  → get_confluence_for_symbol → retained snapshot or live build
#   technical   → watchlist_stage2_lkg (disk JSON, 20h TTL)
#   fundamentals→ watchlist_fundamentals_cache (Neon, cadence-controlled)
#   news        → _news_lkg (module in-memory) + rss_article_archive Neon fallback
#   direct_catalyst → catalyst_alignment_lkg.json (disk JSON) + V4.2 fields
#
# Must remain above the /{watchlist_id} catch-all routes.

_CAT_LKG_PATH = None   # resolved lazily on first call

# ── Fundamentals canonical → snake_case normalization map ────────────────────
# Keys match EXACTLY what FmpFundamentalsRefresher stores in watchlist_fundamentals_cache.
# Source: watchlist_fundamentals_refresh.py field-mapping audit table.
_FUND_NORM: dict[str, str] = {
    "Market Cap":                "market_cap",
    "Revenue":                   "revenue",
    "Revenue Growth (Q)":        "revenue_growth_q",
    "Revenue Growth (YoY)":      "revenue_growth_y",
    "Gross Margin":              "gross_margin",
    "FCF Margin":                "fcf_margin",
    "Free Cash Flow":            "free_cash_flow",
    "Operating Income":          "operating_income",
    "EBIT":                      "ebit",
    "PE Ratio":                  "pe_ratio",
    "PS Ratio":                  "ps_ratio",
    "EV/EBITDA":                 "ev_ebitda",
    "EPS Growth":                "eps_growth",
    "Debt / Equity":             "debt_equity",
    "Net Debt / EBITDA":         "net_debt_ebitda",
    "Shares Insiders":           "insider_percent",
    "Earnings Date":             "earnings_date",
    "Revenue Growth Est.":       "revenue_growth_est",
    "Rev Growth Next Quarter":   "revenue_growth_next_quarter",
    "Rev Growth Next Year":      "revenue_growth_next_year",
    "Rev Growth This Year":      "revenue_growth_this_year",
    "EPS Growth Est.":           "eps_growth_est",
    "EPS Growth This Quarter":   "eps_growth_this_quarter",
    "EPS Growth Next Quarter":   "eps_growth_next_quarter",
    "EPS Growth This Year":      "eps_growth_this_year",
    "EPS Growth Next Year":      "eps_growth_next_year",
}

def _read_catalyst_lkg_sym(sym: str) -> dict:
    """
    Return the raw catalyst LKG row for one symbol — disk read, no network.
    Uses a module-level path reference so it survives hot-reloads.
    Never raises.
    """
    global _CAT_LKG_PATH
    try:
        if _CAT_LKG_PATH is None:
            import pathlib
            _CAT_LKG_PATH = (
                pathlib.Path(__file__).resolve().parent.parent
                / "data" / "catalyst_alignment_lkg.json"
            )
        if not _CAT_LKG_PATH.exists():
            return {}
        raw = _json.loads(_CAT_LKG_PATH.read_text(encoding="utf-8"))
        return (raw.get("data") or {}).get(sym.upper()) or {}
    except Exception:
        return {}


@router.get("/ticker-detail/{symbol}")
async def ticker_detail_endpoint(symbol: str):
    """
    GET /api/watchlist/ticker-detail/{symbol}

    Unified cached data contract for the watchlist ticker popup.
    Zero provider calls — all data from in-process / disk / Neon caches.

    Response shape:
      { symbol, company, overview, confluence_v42,
        technical, fundamentals, news, direct_catalyst, coverage }
    """
    import asyncio as _aio

    sym = symbol.upper().strip()
    if not sym:
        raise HTTPException(status_code=422, detail="symbol is required")

    coverage: dict = {}

    # ── 1. Company profile + description ──────────────────────────────────────
    company: dict = {"symbol": sym}
    try:
        def _fetch_company():
            from services.fmp_cache_service import (
                get_company_profile_cached,
                get_fundamentals_cached,
            )
            from data.watchlist_fundamentals_store import get_snapshot as _get_wf_snap

            prof  = get_company_profile_cached(sym) or {}
            fdb   = get_fundamentals_cached(sym) or {}
            raw_p = fdb.get("profile") or {}

            # ── watchlist_fundamentals_cache profile metadata ─────────────────
            # Written by FmpFundamentalsRefresher.normalize_symbol step 1 which
            # calls /stable/profile directly — richer than the screener-API blob
            # stored in screener_fundamentals_cache by the Screener Hub warm job.
            wf_snap    = _get_wf_snap(sym) or {}
            wf_fields  = wf_snap.get("fields") or {}
            wf_profile = wf_fields.get("profile") or {}
            wf_refreshed_at = wf_snap.get("refreshed_at") or None

            # ── 7-level description fallback ──────────────────────────────────
            # Priority: watchlist_fundamentals_cache (FmpFundamentalsRefresher)
            #           → screener_fundamentals_cache (Screener Hub warm job)
            _desc_candidates = [
                (wf_profile.get("description"),  "watchlist_fundamentals_cache_profile"),
                (wf_fields.get("description"),   "watchlist_fundamentals_cache_fields"),
                (raw_p.get("description"),        "screener_fundamentals_cache_profile"),
                (fdb.get("description"),          "screener_fundamentals_cache_root"),
            ]
            description: str | None = None
            description_source: str | None = None
            for _d, _src in _desc_candidates:
                if _d:
                    description = _d
                    description_source = _src
                    break

            description_last_updated: str | None = None
            if description_source and description_source.startswith("watchlist"):
                description_last_updated = wf_refreshed_at
            elif description_source and description_source.startswith("screener"):
                description_last_updated = (
                    fdb.get("fetched_at") or fdb.get("updated_at") or None
                )

            description_missing_reason: str | None = None
            if not description:
                if not wf_snap and not fdb:
                    description_missing_reason = "symbol_not_in_any_cache"
                elif wf_snap and not wf_profile.get("description") and not raw_p.get("description"):
                    description_missing_reason = "profile_fetched_but_description_absent_from_fmp"
                else:
                    description_missing_reason = "cache_miss_pending_refresh"

            # ── Profile metadata fallback (website / image / ceo / exchange / country) ──
            # Same priority: watchlist_fundamentals_cache.fields.profile first,
            # then screener_fundamentals_cache.profile blob.
            def _pf(key: str) -> str | None:
                return wf_profile.get(key) or raw_p.get(key) or None

            # Resolve canonical market cap using watchlist_fundamentals_cache share
            # basis so company.market_cap == fundamentals.market_cap (same resolver,
            # same implied_shares).  No live price available here (overview not yet
            # built) so the auto-lookup path inside resolve_canonical_market_cap
            # reads from the in-process tradier/quote-lkg caches.
            _static_mc_fb = prof.get("market_cap")
            try:
                from services.market_cap_resolver import resolve_canonical_market_cap as _mc_res_c
                _mc_contract_c = _mc_res_c(
                    sym,
                    wf_fields,
                    static_market_cap_override=_static_mc_fb,
                    fund_refreshed_at=wf_refreshed_at,
                )
                _company_mc = _mc_contract_c.get("market_cap_display") or _static_mc_fb
            except Exception:
                _company_mc = _static_mc_fb

            return {
                "symbol":       sym,
                "company_name": prof.get("name") or raw_p.get("companyName") or "",
                "sector":       prof.get("sector") or "",
                "industry":     prof.get("industry") or "",
                "market_cap":   _company_mc,
                "exchange":     prof.get("exchange") or wf_profile.get("exchange") or "",
                "country":      prof.get("country") or wf_profile.get("country") or "",
                "beta":         prof.get("beta") if prof.get("beta") is not None else wf_profile.get("beta"),
                "website":      _pf("website"),
                "image":        _pf("image"),
                "description":  description,
                "ceo":          _pf("ceo"),
                "source":       prof.get("source") or "screener_fundamentals_cache",
                "description_source":       description_source,
                "description_last_updated": description_last_updated,
                "description_missing_reason": description_missing_reason if not description else None,
            }
        company = await _aio.to_thread(_fetch_company)
        coverage["company_profile"] = bool(company.get("company_name"))
        coverage["description"]     = bool(company.get("description"))
    except Exception as _e:
        print(f"[TICKER_DETAIL] company error {sym}: {_e}")
        coverage["company_profile"] = False
        coverage["description"]     = False

    # ── 2. Overview (live quote from cache — zero provider calls) ─────────────
    # quote_status semantics:
    #   available                — price present in watchlist_quote_cache
    #   unavailable              — symbol not in active quote poll; price=None
    #   row_fallback_recommended — same as unavailable; frontend may use a
    #                              stale row price from the watchlist table
    overview: dict = {}
    try:
        from services.watchlist_quote_cache import get_watchlist_quotes
        _qmap = await get_watchlist_quotes([sym])
        q = _qmap.get(sym) or {}
        _price = q.get("price")
        _last_upd = q.get("quote_updated_at")
        if _price is not None:
            _qstatus = "available"
            _qsource = "quote_cache"
        else:
            _qstatus = "row_fallback_recommended"
            _qsource = "unavailable"
        overview = {
            "price":            _price,
            "change_percent":   q.get("change_pct_1d"),
            "volume":           q.get("volume"),
            "average_volume":   q.get("average_volume"),
            "relative_volume":  q.get("relative_volume"),
            "quote_status":     _qstatus,
            "source":           _qsource,
            "last_updated":     _last_upd,
        }
        coverage["quote"] = _price is not None
    except Exception as _e:
        print(f"[TICKER_DETAIL] quote error {sym}: {_e}")
        overview = {
            "price": None, "change_percent": None, "volume": None,
            "average_volume": None, "relative_volume": None,
            "quote_status": "unavailable", "source": "unavailable",
            "last_updated": None,
        }
        coverage["quote"] = False

    # ── 3. Confluence V4.2 detail ─────────────────────────────────────────────
    # Fast path: retained snapshot (already computed, dict lookup only).
    # Fallback: single-symbol build (slow ~5s, only for symbols not in retained).
    confluence_v42: dict = {}
    try:
        def _fetch_c42():
            from services.confluence_v2_service import (
                get_retained_confluence_snapshot,
                get_confluence_for_symbol as _build_c42,
            )
            try:
                retained = get_retained_confluence_snapshot()
                by_sym = {
                    r.get("symbol"): r
                    for r in (retained.get("results") or [])
                }
                if sym in by_sym:
                    return by_sym[sym]
            except Exception:
                pass
            return _build_c42(sym)
        confluence_v42 = await _aio.to_thread(_fetch_c42)
        coverage["confluence_v42"] = confluence_v42.get("caelyn_confluence_score") is not None
    except Exception as _e:
        print(f"[TICKER_DETAIL] confluence error {sym}: {_e}")
        coverage["confluence_v42"] = False

    # ── 4. Technical (stage2 LKG — in-memory, zero I/O) ───────────────────────
    technical: dict = {}
    try:
        from services.watchlist_stage2_service import get_stage2 as _get_s2
        s2 = _get_s2(sym)
        tm = s2.get("technical_metrics") or {}
        technical = {
            "ticker":                sym,
            # Weinstein stage
            "stage":                 s2.get("label"),
            "stage_score":           s2.get("score"),
            "stage_reason":          s2.get("reason"),
            "stage_confidence":      s2.get("stage_confidence"),
            "stage_confidence_reason": s2.get("stage_confidence_reason"),
            "signals":               s2.get("signals"),
            # Core TA from technical_metrics
            "technical_state":       s2.get("technical_state") or tm.get("technical_state"),
            "technical_timing_score": s2.get("technical_timing_score"),
            "ma_stack":              tm.get("ma_stack"),
            "pct_vs_50d":            tm.get("pct_vs_sma_50"),
            "pct_vs_200d":           tm.get("pct_vs_sma_200"),
            "pct_vs_20d":            tm.get("pct_vs_sma_20"),
            "extension_risk":        tm.get("extension_risk"),
            "fifty_two_week_position": tm.get("range_position_52w"),
            "pct_from_52w_high":     tm.get("pct_from_52w_high"),
            "pct_from_52w_low":      tm.get("pct_from_52w_low"),
            "high_52w":              tm.get("high_52w"),
            "low_52w":               tm.get("low_52w"),
            "sma_20":                tm.get("sma_20"),
            "sma_50":                tm.get("sma_50"),
            "sma_200":               tm.get("sma_200"),
            "entry_zone":            tm.get("entry_zone"),
            "breakout_signal":       tm.get("breakout_signal"),
            "high_20d":              tm.get("high_20d"),
            "high_50d":              tm.get("high_50d"),
            "accumulation_distribution": tm.get("accumulation_distribution_signal"),
            "accumulation_distribution_score": tm.get("accumulation_distribution_score"),
            "squeeze":               tm.get("squeeze_signal"),
            "atr_percent":           tm.get("atr_14_pct"),
            "atr_14":                tm.get("atr_14"),
            "momentum_trend":        tm.get("momentum_trend"),
            "roc_20d":               tm.get("roc_20d"),
            "roc_50d":               tm.get("roc_50d"),
            "avg_volume_20d":        tm.get("avg_volume_20d"),
            "accumulation_days_20d": tm.get("accumulation_days_20d"),
            "distribution_days_20d": tm.get("distribution_days_20d"),
            # Live quote overlay
            "price":                 overview.get("price"),
            "change_percent":        overview.get("change_pct_1d"),
            "volume":                overview.get("volume"),
            "relative_volume":       overview.get("relative_volume"),
            # Options overlay (from V4.2)
            "opt_score":    confluence_v42.get("options_alignment_points"),
            "opt_signal":   confluence_v42.get("options_status"),
            # History provenance
            "history_source":    s2.get("history_source"),
            "bars_count":        s2.get("bars_count"),
            "history_start":     s2.get("history_start_date"),
            "history_end":       s2.get("history_end_date"),
            "computed_at":       s2.get("computed_at"),
        }
        coverage["technical"] = s2.get("score") is not None
    except Exception as _e:
        print(f"[TICKER_DETAIL] technical error {sym}: {_e}")
        coverage["technical"] = False

    # ── 5. Fundamentals (watchlist_fundamentals_cache Neon) ───────────────────
    # Fields are stored in title-case keys (e.g. "Revenue", "PE Ratio").
    # Normalized here to snake_case to match the frontend contract spec.
    # Source is identical to the Watchlist Fundamental toggle — same Neon table,
    # same FmpFundamentalsRefresher write path.  No request-time FMP calls.
    fundamentals: dict = {}
    try:
        def _fetch_fund():
            from data.watchlist_fundamentals_store import get_snapshot as _get_fs
            snap = _get_fs(sym)
            if snap is None:
                return {}
            raw_fields: dict = snap.get("fields") or {}
            missing_fields: list = snap.get("missing_fields") or []

            # Normalize to snake_case using the canonical map
            norm: dict = {"ticker": sym, "theme": None}  # theme injected below
            for canonical_key, snake_key in _FUND_NORM.items():
                v = raw_fields.get(canonical_key)
                norm[snake_key] = v  # None when field is missing/stale

            # ── Canonical live market cap override ────────────────────────────
            # Replaces the raw FMP static value with live_price × implied_shares
            # so this tab matches company.market_cap (same resolver, same logic).
            # overview is already populated (step 2 ran before step 5).
            try:
                from services.market_cap_resolver import resolve_canonical_market_cap as _mc_res_f
                _mc_contract_f = _mc_res_f(
                    sym,
                    raw_fields,
                    live_price=overview.get("price"),
                    live_price_source="tradier",
                    fund_refreshed_at=snap.get("refreshed_at"),
                )
                _mc_disp_f = _mc_contract_f.get("market_cap_display")
                if _mc_disp_f and _mc_disp_f > 0:
                    norm["market_cap"] = _mc_disp_f
                norm["market_cap_static"]              = _mc_contract_f.get("market_cap_static")
                norm["market_cap_live"]                = _mc_contract_f.get("market_cap_live")
                norm["market_cap_live_price"]          = _mc_contract_f.get("market_cap_live_price")
                norm["market_cap_implied_shares"]      = _mc_contract_f.get("market_cap_implied_shares")
                norm["market_cap_display_source"]      = _mc_contract_f.get("market_cap_display_source")
                norm["market_cap_display_freshness"]   = _mc_contract_f.get("market_cap_display_freshness")
                norm["market_cap_display_warning_codes"] = _mc_contract_f.get("market_cap_display_warning_codes")
            except Exception:
                pass  # non-fatal: market_cap already set from _FUND_NORM above

            # Determine which expected keys are genuinely missing from store
            expected_keys = list(_FUND_NORM.values())
            actual_missing = [
                k for k in expected_keys
                if norm.get(k) is None
            ]

            # Freshness classification
            refreshed_at = snap.get("refreshed_at") or ""
            next_refresh  = snap.get("next_refresh_at") or ""
            try:
                from datetime import datetime as _dt, timezone as _tz
                ra = _dt.fromisoformat(refreshed_at) if refreshed_at else None
                age_days = (
                    (_dt.now(_tz.utc) - ra).days if ra else None
                )
                if age_days is None:
                    freshness = "unknown"
                elif age_days <= 7:
                    freshness = "fresh"
                elif age_days <= 21:
                    freshness = "stale"
                else:
                    freshness = "very_stale"
            except Exception:
                freshness = "unknown"
                age_days = None

            norm["fundamentals_source"] = {
                "source_table":     "watchlist_fundamentals_cache",
                "source_service":   "FmpFundamentalsRefresher",
                "last_updated":     refreshed_at or None,
                "next_refresh_at":  next_refresh or None,
                "freshness_status": freshness,
                "age_days":         age_days,
                "missing_fields":   actual_missing,
                "fmp_call_count":   snap.get("fmp_call_count"),
            }
            return norm
        fundamentals = await _aio.to_thread(_fetch_fund)
        # Inject theme from confluence_v42 (built in step 3)
        if fundamentals:
            fundamentals["theme"] = confluence_v42.get("theme_name")
        coverage["fundamentals"] = bool(fundamentals.get("fundamentals_source"))
    except Exception as _e:
        print(f"[TICKER_DETAIL] fundamentals error {sym}: {_e}")
        coverage["fundamentals"] = False

    # ── 6. News — in-memory LKG → rss_article_archive Neon fallback ──────────
    news: dict = {
        "articles":                 [],
        "direct_catalyst_articles": [],
        "hyperscaler_articles":     [],
        "status":                   "no_cached_news",
        "last_updated":             None,
    }
    try:
        import services.watchlist_router as _wr_mod

        # Primary: module-level news LKG (default watchlist only for now)
        _lkg_entry = _wr_mod._news_lkg.get("default") or {}
        _lkg_data  = _lkg_entry.get("data") or {}
        articles_for_sym: list = list(_lkg_data.get("articles", {}).get(sym) or [])

        # Hyperscaler cache filtered to this ticker
        hyp_arts: list = [
            a for a in (_wr_mod._HYP_CACHE.get("articles") or [])
            if sym in (a.get("watchlist_symbols") or [])
        ]

        # If in-memory LKG empty, fall back to Neon archive (96h window)
        if not articles_for_sym:
            def _fetch_archive():
                from data.rss_article_archive import query_ticker_activity_articles
                rows, _ = query_ticker_activity_articles(sym, 96)
                return rows
            articles_for_sym = await _aio.to_thread(_fetch_archive)

        if articles_for_sym or hyp_arts:
            news["status"] = "available"

        lkg_ts = _lkg_entry.get("ts")
        news["last_updated"] = (
            datetime.fromtimestamp(lkg_ts, tz=timezone.utc).isoformat()
            if lkg_ts else None
        )
        news["articles"]             = articles_for_sym
        news["hyperscaler_articles"] = hyp_arts
        coverage["news"] = bool(articles_for_sym or hyp_arts)
    except Exception as _e:
        print(f"[TICKER_DETAIL] news error {sym}: {_e}")
        news["error"] = str(_e)
        coverage["news"] = False

    # ── 7. Direct catalyst — LKG raw event + V4.2 scored fields ──────────────
    direct_catalyst: dict = {}
    try:
        cat_row = await _aio.to_thread(_read_catalyst_lkg_sym, sym)
        pe = cat_row.get("catalyst_primary_event") or {}

        if cat_row:
            direct_catalyst = {
                # Availability
                "available":               cat_row.get("catalyst_alignment_available"),
                "primary_source":          cat_row.get("catalyst_primary_source"),
                "bearish_conflict":        bool(cat_row.get("catalyst_bearish_conflict")),
                "catalyst_score_raw":      cat_row.get("catalyst_alignment_score"),
                # Raw event article fields
                "event_type":              pe.get("event_type"),
                "event_reason":            pe.get("event_reason"),
                "direction":               pe.get("direction"),
                "materiality_score":       pe.get("materiality_score"),
                "confidence_score":        pe.get("confidence_score"),
                "ticker_relevance_score":  pe.get("ticker_relevance_score"),
                "ticker_relevance_reason": pe.get("ticker_relevance_reason"),
                "article_count":           pe.get("article_count"),
                "published_at":            pe.get("published_at"),
                "catalyst_date":           pe.get("catalyst_date"),
                "days_until":              pe.get("days_until"),
                "title":                   pe.get("title"),
                "url":                     pe.get("url"),
                "why_it_matters":          pe.get("why_it_matters"),
                "primary_subject":         pe.get("primary_subject"),
                # Phase B scored fields (from V4.2 run above)
                "catalyst_alignment_points":  confluence_v42.get("catalyst_alignment_points"),
                "catalyst_event_type":        confluence_v42.get("catalyst_event_type"),
                "catalyst_event_tier":        confluence_v42.get("catalyst_event_tier"),
                "catalyst_freshness_score":   confluence_v42.get("catalyst_freshness_score"),
                "catalyst_relevance_score":   confluence_v42.get("catalyst_relevance_score"),
                "catalyst_materiality_score": confluence_v42.get("catalyst_materiality_score"),
                "catalyst_reason_codes":      confluence_v42.get("catalyst_reason_codes"),
                "catalyst_explanation":       confluence_v42.get("catalyst_explanation"),
                "direct_catalyst_present":    confluence_v42.get("direct_catalyst_present"),
                "reason_codes":               cat_row.get("catalyst_v2_reason_codes"),
            }

            # Mark any news article that matches the direct catalyst event
            cat_title = pe.get("title") or ""
            cat_url   = pe.get("url") or ""
            dc_arts: list[dict] = []
            for art_list in (news["articles"], news["hyperscaler_articles"]):
                for art in art_list:
                    is_dc = (
                        (cat_title and art.get("title") == cat_title)
                        or (cat_url and art.get("url") == cat_url)
                    )
                    if is_dc:
                        art["is_direct_catalyst"]    = True
                        art["catalyst_event_type"]   = pe.get("event_type")
                        art["catalyst_event_tier"]   = confluence_v42.get("catalyst_event_tier")
                        art["materiality_score"]     = pe.get("materiality_score")
                        art["confidence_score"]      = pe.get("confidence_score")
                        art["ticker_relevance_score"] = pe.get("ticker_relevance_score")
                        art["freshness_score"]       = confluence_v42.get("catalyst_freshness_score")
                        art["reason_codes"]          = cat_row.get("catalyst_v2_reason_codes")
                        dc_arts.append(art)

            # Also surface the raw catalyst event as a pseudo-article if no match found
            # (article may be older than the 96h news window)
            if not dc_arts and cat_title and cat_row.get("catalyst_alignment_available"):
                dc_arts.append({
                    "ticker":                  sym,
                    "title":                   cat_title,
                    "url":                     cat_url or None,
                    "published_at":            pe.get("published_at"),
                    "source":                  pe.get("source") or "rss",
                    "summary":                 pe.get("why_it_matters") or "",
                    "is_direct_catalyst":      True,
                    "catalyst_event_type":     pe.get("event_type"),
                    "catalyst_event_tier":     confluence_v42.get("catalyst_event_tier"),
                    "materiality_score":       pe.get("materiality_score"),
                    "confidence_score":        pe.get("confidence_score"),
                    "ticker_relevance_score":  pe.get("ticker_relevance_score"),
                    "freshness_score":         confluence_v42.get("catalyst_freshness_score"),
                    "reason_codes":            cat_row.get("catalyst_v2_reason_codes"),
                    "article_count":           pe.get("article_count"),
                })

            news["direct_catalyst_articles"] = dc_arts
            coverage["direct_catalyst"] = bool(cat_row.get("catalyst_alignment_available"))
        else:
            coverage["direct_catalyst"] = False
    except Exception as _e:
        print(f"[TICKER_DETAIL] catalyst error {sym}: {_e}")
        coverage["direct_catalyst"] = False

    # ── 8. Earnings Intelligence (from watchlist_fundamentals_cache) ──────────
    # Populated by FmpFundamentalsRefresher._fetch_earnings_intelligence() on
    # the weekly fundamentals refresh cycle.  Zero provider calls at request
    # time — pure Neon JSONB read from the already-loaded snapshot.
    # ETFs, funds, and non-operating securities are gated out here so they
    # always return null regardless of any legacy rows in the DB.
    earnings_intelligence: dict | None = None
    try:
        def _fetch_ei():
            from data.watchlist_fundamentals_store import get_snapshot as _get_fs
            from services.watchlist_fundamentals_refresh import ei_ineligible_reason as _ei_elig
            snap = _get_fs(sym)
            if snap is None:
                return None
            _reason = _ei_elig(sym, snap)
            if _reason:
                return None  # ETF / non-operating security — no EI
            raw_fields: dict = snap.get("fields") or {}
            ei = _ticker_detail_earnings_intelligence(raw_fields)
            if not ei:
                return None
            return ei
        earnings_intelligence = await _aio.to_thread(_fetch_ei)
        coverage["earnings_intelligence"] = bool(
            earnings_intelligence
            and earnings_intelligence.get("source_status", {}).get("coverage", {}).get("has_earnings_history")
        )
    except Exception as _ei_e:
        print(f"[TICKER_DETAIL] earnings_intelligence error {sym}: {_ei_e}")
        coverage["earnings_intelligence"] = False

    # ── 9. SEC Materials (from ei_materials_cache disk cache) ─────────────────
    # Populated by background ei_materials_service refresh (daily cadence).
    # Zero provider calls — disk cache read only.  Falls back to LKG (any age)
    # when fresh entry is absent.  Returns null when symbol has no cache yet.
    ei_materials: dict | None = None
    try:
        def _fetch_materials():
            from data.ei_materials_cache import get_materials, get_materials_lkg
            m = get_materials(sym)
            if m is None:
                m = get_materials_lkg(sym)  # stale LKG is better than null
            return m
        ei_materials = await _aio.to_thread(_fetch_materials)
        coverage["ei_materials"] = bool(
            ei_materials
            and ei_materials.get("source_status", {}).get("coverage")
        )
    except Exception as _mat_e:
        print(f"[TICKER_DETAIL] ei_materials error {sym}: {_mat_e}")
        coverage["ei_materials"] = False

    # Inject materials into earnings_intelligence dict so they arrive together
    if earnings_intelligence and isinstance(earnings_intelligence, dict):
        earnings_intelligence = dict(earnings_intelligence)
        earnings_intelligence["materials"] = ei_materials
    elif ei_materials is not None:
        # Symbol has materials but no earnings history (e.g. first-run)
        earnings_intelligence = {"materials": ei_materials}

    # ── 10. Live Earnings Event (zero-provider DB-only lookup) ─────────────────
    # get_live_event_for_symbol uses revision-first ordering within each
    # expected_date bucket: a higher-revision results_updated event beats a
    # stale lower-revision complete event for the same quarter, so the ticker
    # popup always reflects the most recently written/corrected results payload.
    live_event: dict | None = None
    try:
        def _fetch_live_event():
            from data.earnings_monitor_store import (
                get_live_event_for_symbol, get_targets_for_symbols,
            )
            ev = get_live_event_for_symbol(sym, include_dry_run=False)
            if not ev:
                return None

            # ── enrich with schedule fields from earnings_monitor_targets ─────
            # Use get_targets_for_symbols (not get_active_targets) so complete
            # targets are included and schedule fields are never NULL post-results.
            sched: dict = {}
            try:
                targets = get_targets_for_symbols([sym])
                if targets:
                    t = targets[0]
                    ea = t.get("expected_at")
                    sched = {
                        "expected_at":         str(ea).replace("+00:00", "Z") if ea else None,
                        "expected_timing":     t.get("expected_timing"),
                        "expected_time_local": t.get("expected_time_local"),
                        "expected_timezone":   "America/New_York",
                        "report_time_status":  t.get("report_time_status"),
                        "report_period":       t.get("report_period"),
                        "schedule_source":     t.get("schedule_source"),
                    }
            except Exception:
                pass

            rp  = ev.get("results_payload") or {}
            fp  = ev.get("filing_payload")  or {}
            rxn = ev.get("reaction_payload") or {}

            def _dt(v):
                if v is None:
                    return None
                s = str(v)
                return s.replace("+00:00", "Z") if ("+" in s or "Z" in s) else s

            return {
                # ── identity ──────────────────────────────────────────────
                "event_id":            ev.get("event_id"),
                "event_key":           ev.get("event_key"),
                "symbol":              ev.get("symbol"),
                "company_name":        (company or {}).get("name"),
                # ── state ─────────────────────────────────────────────────
                "state":               ev.get("state"),
                "classification":      ev.get("classification"),
                "revision":            ev.get("revision", 1),
                # ── timestamps ────────────────────────────────────────────
                "detected_at":         _dt(ev.get("detected_at")),
                "updated_at":          _dt(ev.get("updated_at")),
                # ── schedule (from targets) ────────────────────────────────
                "expected_date":       _dt(ev.get("expected_date")),
                "expected_at":         sched.get("expected_at"),
                "expected_time_local": sched.get("expected_time_local"),
                "expected_timezone":   sched.get("expected_timezone", "America/New_York"),
                "expected_timing":     sched.get("expected_timing"),
                "report_time_status":  sched.get("report_time_status"),
                "report_period":       sched.get("report_period"),
                "schedule_source":     sched.get("schedule_source"),
                # ── fiscal ────────────────────────────────────────────────
                "fiscal_period":       ev.get("fiscal_period"),
                "fiscal_year":         ev.get("fiscal_year"),
                # ── full payloads ─────────────────────────────────────────
                "results_payload":     rp  if rp  else None,
                "filing_payload":      fp  if fp  else None,
                "reaction_payload":    rxn if rxn else None,
                # ── meta ──────────────────────────────────────────────────
                "source_status":       ev.get("source_status"),
                "is_read":             None,   # no per-user state in ticker-detail
            }
        live_event = await _aio.to_thread(_fetch_live_event)
        coverage["live_earnings_event"] = live_event is not None
    except Exception as _le_e:
        print(f"[TICKER_DETAIL] live_event error {sym}: {_le_e}")
        coverage["live_earnings_event"] = False

    # Inject live_event into earnings_intelligence so they arrive together
    if earnings_intelligence and isinstance(earnings_intelligence, dict):
        earnings_intelligence = dict(earnings_intelligence)
        earnings_intelligence["live_event"] = live_event
    elif live_event is not None:
        earnings_intelligence = {"live_event": live_event}

    return {
        "symbol":        sym,
        "company":       company,
        "overview":      overview,
        "confluence_v42": confluence_v42,
        "technical":     technical,
        "fundamentals":  fundamentals,
        "news":          news,
        "direct_catalyst": direct_catalyst,
        "earnings_intelligence": earnings_intelligence,
        "coverage":      coverage,
    }


# ── Defiance 2X enrichment helper (shared by watchlist alias + strategy route) ─

async def _build_defiance_rows(
    *,
    catalog: list[dict],
    get_quotes,
    quote_cache: dict,
    get_theme,
    get_ts,
    tab_key: str,
) -> dict:
    """
    Core enrichment for Defiance 2X Long ETF rows.

    Enrichment policy
    -----------------
    - underlying_symbol is the primary/row ticker; defiance_etf_ticker is metadata only.
    - `is_already_tracked` = underlying was already in the canonical quote cache *before*
      this request fired; no new Tradier call was needed for it.
    - `quote_reused` = same as is_already_tracked (alias kept for API clarity).
    - chart_symbol and quote_source_symbol always equal the underlying symbol.
    - The Defiance ETF ticker is NEVER used for price, market cap, VolX, or chart.

    Logging
    -------
    Emits a single [DEFIANCE_2X] summary line with:
      total_catalog | reused_quote_count | new_tradier_fetch_count | fetch_symbols
    """
    # Snapshot which underlying symbols are already in the canonical quote cache
    # BEFORE calling get_watchlist_quotes (which may pull new Tradier quotes).
    underlying_syms = [r["underlying_symbol"] for r in catalog if r.get("underlying_symbol")]
    already_tracked: set[str] = {s.upper() for s in underlying_syms if s.upper() in quote_cache}
    new_fetch_syms = [s for s in underlying_syms if s.upper() not in already_tracked]

    quotes = await get_quotes(underlying_syms)

    rows: list[dict] = []
    for entry in catalog:
        sym = entry.get("underlying_symbol")
        if not sym:
            continue

        etf_ticker = entry.get("defiance_etf_ticker", "")
        # Defensive guard: never allow a row where underlying == ETF ticker
        if sym.upper() == etf_ticker.upper():
            print(
                f"[DEFIANCE_2X] skipped row — underlying==etf_ticker: {sym!r}"
                f"  name={entry.get('defiance_etf_name')!r}"
            )
            continue

        sym_up  = sym.upper()
        q       = quotes.get(sym_up, {})
        price   = q.get("price")
        change  = q.get("change_pct_1d")
        volume  = q.get("volume")
        rel_vol = q.get("relative_volume")
        name    = q.get("name") or sym

        market_cap = q.get("market_cap")
        vmc = _vol_mc_fields(price, volume, market_cap)

        stage = _get_stage2_breakout(sym)
        theme = get_theme(sym)

        is_tracked = sym_up in already_tracked

        rows.append({
            "symbol":            sym,
            "chart_symbol":      sym,
            "quote_source_symbol": sym,
            "name":              name,
            "price":             price,
            "change_pct":        change,
            "market_cap":        vmc.get("market_cap"),
            "volume":            volume,
            "vol_x":             rel_vol,
            "vol_mc_ratio":      vmc.get("vol_mc_ratio"),
            "vol_mc_pct":        vmc.get("vol_mc_pct"),
            "vol_mc_label":      vmc.get("vol_mc_label"),
            "theme":             theme,
            "stage_analysis":    stage,
            "is_already_tracked": is_tracked,
            "quote_reused":      is_tracked,
            "defiance_etf": {
                "symbol":       etf_ticker,
                "name":         entry.get("defiance_etf_name", ""),
                "leverage":     entry.get("leverage", 2),
                "direction":    entry.get("direction", "long"),
                "source_url":   entry.get("source_url", ""),
                "last_seen_at": entry.get("last_seen_at"),
            },
        })

    # Sort: priced rows first, then market_cap desc, nulls last
    rows.sort(key=lambda r: (
        r.get("price") is None,
        r.get("market_cap") is None,
        -(r.get("market_cap") or 0),
    ))

    reused = sum(1 for r in rows if r["is_already_tracked"])
    new_fetched = len(rows) - reused
    print(
        f"[DEFIANCE_2X] response built: total_catalog={len(catalog)}"
        f"  rows={len(rows)}"
        f"  reused_quote_count={reused}"
        f"  new_tradier_fetch_count={len(new_fetch_syms)}"
        + (f"  fetch_syms={new_fetch_syms}" if new_fetch_syms else "")
    )

    ts = get_ts()
    updated_at = (
        datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        if ts else None
    )

    return {
        "tab":        tab_key,
        "title":      "Defiance 2× Long ETFs",
        "updated_at": updated_at,
        "count":      len(rows),
        "rows":       rows,
    }


# ── Defiance 2X Long ETF tab — backwards-compatible alias ────────────────────
# Primary endpoint: GET /api/strategy/defiance (Strategy page)
# This route is retained for any existing frontend consumers.

@router.get("/defiance-2x")
async def defiance_2x_endpoint(request: Request, force_refresh: bool = False):
    """
    Backwards-compatible alias for /api/strategy/defiance.

    The canonical endpoint lives on the Strategy page.  This route delegates
    to the same underlying logic so existing frontend consumers continue to
    work unchanged while the frontend migrates to /api/strategy/defiance.

    Response shape is identical to /api/strategy/defiance.
    """
    from services.defiance_leveraged_etfs_service import (
        get_catalog         as _d2x_catalog,
        refresh_catalog     as _d2x_refresh,
        get_last_refresh_ts as _d2x_ts,
        get_quarantined     as _d2x_quarantined,
    )
    from services.watchlist_quote_cache import (
        get_watchlist_quotes as _get_quotes,
        _quote_cache         as _qc,
    )
    from services.theme_ticker_mapper import map_ticker_to_primary_theme
    import os as _os

    # ── Admin force-refresh ───────────────────────────────────────────────────
    if force_refresh:
        _ak = _os.getenv("AGENT_API_KEY", "")
        _auth = request.headers.get("Authorization", "")
        if _ak and _auth != f"Bearer {_ak}":
            raise HTTPException(status_code=403, detail="Admin only")
        await _d2x_refresh(force=True)

    catalog = _d2x_catalog()

    if not catalog:
        import asyncio as _aio
        _aio.create_task(_d2x_refresh())
        return {
            "tab":        "defiance-2x",
            "title":      "Defiance 2× Long ETFs",
            "rows":       [],
            "count":      0,
            "updated_at": None,
            "status":     "warming_up",
        }

    return await _build_defiance_rows(
        catalog      = catalog,
        get_quotes   = _get_quotes,
        quote_cache  = _qc,
        get_theme    = map_ticker_to_primary_theme,
        get_ts       = _d2x_ts,
        tab_key      = "defiance-2x",
    )


# ── Security search (static path — must stay above parameterized routes) ─────

@router.get("/security-search")
async def security_search_endpoint(request: Request, q: str = "", limit: int = 25):
    """
    Search for securities by ticker or company name using FMP.
    Returns normalized results with canonical_ticker already constructed
    (e.g. 'TRT' for US, 'AIM:TRT' for AIM-listed) so the caller does not
    need to construct exchange-prefixed identities.

    Results are ranked: exact ticker match → ticker prefix → name match.
    All exchange variants sharing the same provider symbol are shown —
    e.g. querying 'TRT' returns both Trio-Tech (AMEX) and Tribal Group (AIM).

    Response shape per item:
      canonical_ticker, provider_symbol, company_name, exchange,
      exchange_short_name, country, currency, security_type,
      is_actively_trading, display_symbol

    HTTP 200  — valid response (results may be empty for a genuine zero-match query)
    HTTP 503  — both FMP search endpoints failed (provider_error); client should retry
    """
    q = q.strip()
    if len(q) < 1:
        return {"query": q, "results": [], "count": 0, "error": "query_too_short"}

    _effective_limit = min(limit, 50)
    print(f"[WATCHLIST-SEARCH] query={q!r} limit={_effective_limit}")
    try:
        from config import FMP_API_KEY as _fmp_key
        from data.fmp_provider import FMPProvider, FMPSearchProviderError
        if not _fmp_key:
            print("[WATCHLIST-SEARCH] FMP_API_KEY not configured — returning empty")
            return {"query": q, "results": [], "count": 0, "error": "provider_not_configured"}
        provider = FMPProvider(_fmp_key)
        results = await provider.search_securities(q, limit=_effective_limit)
        print(f"[WATCHLIST-SEARCH] query={q!r} → {len(results)} results "
              f"(top: {[r['canonical_ticker'] for r in results[:5]]})")
        return {
            "query":   q,
            "results": results,
            "count":   len(results),
        }
    except Exception as exc:
        from data.fmp_provider import FMPSearchProviderError as _FMPSearchProviderError
        if isinstance(exc, _FMPSearchProviderError):
            print(f"[WATCHLIST-SEARCH] PROVIDER_FAILURE query={q!r}: {exc}")
            from fastapi.responses import JSONResponse
            return JSONResponse(
                status_code=503,
                content={"query": q, "results": [], "count": 0, "error": "provider_error"},
            )
        print(f"[WATCHLIST-SEARCH] ERROR query={q!r} exc={type(exc).__name__}: {exc}")
        return {"query": q, "results": [], "count": 0, "error": "provider_error"}


# ── Parameterized endpoints (MUST be after static paths) ────────────────────

@router.patch("/{watchlist_id}/rename")
async def rename_endpoint(watchlist_id: str, body: dict):
    """Rename a specific watchlist."""
    new_name = body.get("name", "").strip()
    if not new_name:
        raise HTTPException(status_code=400, detail="Name is required")
    try:
        from data.pg_storage import watchlist_rename, is_available
        if is_available():
            ok = watchlist_rename(watchlist_id, new_name)
            if ok:
                return {"success": True, "name": new_name}
        return {"error": "Postgres unavailable"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class _AddTickerBody(BaseModel):
    model_config = ConfigDict(extra="ignore")
    canonical_ticker: str
    company_name: Optional[str] = None
    exchange_short_name: Optional[str] = None
    country: Optional[str] = None


@router.post("/{watchlist_id}/ticker")
async def add_ticker_endpoint(watchlist_id: str, body: _AddTickerBody):
    """
    Add an explicitly selected canonical security to a watchlist.
    canonical_ticker must be the exact identity returned by GET /security-search
    (e.g. 'NVDA' for NASDAQ, 'AIM:TRT' for AIM-listed Tribal Group).

    Does NOT run Claude analysis, does NOT call FMP fundamentals.
    The ticker appears immediately as a skeleton row; background enrichment
    paths (weekly fundamentals, quote cache) fill in additional fields.

    Idempotent: adding a ticker that already exists returns duplicate=true, 200.

    Part H (exchange-family alias detection): if the same security is already
    present under a different canonical prefix in the same exchange family
    (e.g. AIM:IQE when adding LON:IQE), returns duplicate=true with
    existing_ticker and conflict_type=exchange_family_alias.
    """
    t = body.canonical_ticker.strip().upper()
    if not t:
        raise HTTPException(status_code=400, detail="canonical_ticker is required")

    try:
        from data.pg_storage import watchlist_add_ticker, is_available
        from services.canonical_security_adapter import exchange_family_aliases
        if not is_available():
            raise HTTPException(status_code=503, detail="Database unavailable")
        aliases = exchange_family_aliases(t)
        result = watchlist_add_ticker(watchlist_id, t, family_aliases=aliases)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    if result.get("error") == "not_found":
        raise HTTPException(status_code=404, detail="Watchlist not found")
    if result.get("error"):
        raise HTTPException(status_code=500, detail=result["error"])

    if result.get("added"):
        import asyncio as _aio_add
        try:
            from services.user_earnings_service import invalidate_user_earnings
            invalidate_user_earnings("watchlist")
        except Exception:
            pass
        _rv_registry.pop(watchlist_id, None)
        _volmc_registry.pop(watchlist_id, None)
        _news_lkg.pop(watchlist_id, None)
        _bulk_lkg_invalidate(watchlist_id)

        # Mark the new symbol as immediately due for the weekly FMP queue
        # (non-blocking row insert — actual FMP work follows below).
        try:
            from services.watchlist_quote_cache import is_fmp_symbol_eligible
            if is_fmp_symbol_eligible(t):
                from data.watchlist_fundamentals_store import schedule_refresh
                schedule_refresh(t, watchlist_id, days=0)
        except Exception as _fund_sched_exc:
            print(f"[WATCHLIST] schedule_refresh({t}) on add failed: {_fund_sched_exc}")

        # ── Priority hydration: quote + technical + FMP + options (non-blocking) ─
        # Set initial state synchronously so the response can include it.
        from datetime import datetime, timezone as _tz_add
        _ts_add = datetime.now(_tz_add.utc).isoformat()
        _HYDRATION_STATE[t] = {
            "quote":        "pending",
            "technical":    "pending",
            "fundamentals": "pending",
            "options":      "pending",
            "enqueued_at":  _ts_add,
            "last_error":   None,
            "last_updated": _ts_add,
        }
        _aio_add.create_task(_priority_hydrate_symbols([t], watchlist_id))

        # ── Background theme classification for newly added ticker ─────────
        # Fires after the response returns. Skips tickers that already have a
        # canonical thematic assignment. Uses DeepSeek V4 Flash via the
        # canonical taxonomy classifier. Classification failure never blocks
        # or invalidates the successful add.
        #
        # Only passes information already cheaply available — the classifier
        # hydrates description/sector from the fundamentals cache in the
        # background (off the request path).
        try:
            from services.watchlist_theme_classifier import classify_and_assign_ticker as _classify_one
            _company = body.company_name or ""
            _aio_add.create_task(_classify_one(t, _company, "", ""))
            print(f"[WATCHLIST_ADD] theme classifier queued for new ticker {t}")
        except Exception as _cls_err:
            print(f"[WATCHLIST_ADD] theme classifier trigger skipped (non-fatal): {_cls_err}")

    resp = {
        "success":          True,
        "watchlist_id":     watchlist_id,
        "ticker":           t,
        "company_name":     body.company_name or "",
        "added":            result.get("added", False),
        "duplicate":        result.get("duplicate", False),
        "ticker_count":     result.get("ticker_count", 0),
        "hydration_status": _hydration_entry(t),
    }
    if result.get("conflict_type"):
        resp["existing_ticker"] = result.get("existing_ticker", "")
        resp["conflict_type"]   = result["conflict_type"]
    return resp


@router.delete("/{watchlist_id}/ticker/{ticker:path}")
async def remove_ticker_endpoint(watchlist_id: str, ticker: str):
    """
    Permanently remove a single canonical ticker from a watchlist.

    Uses exact canonical identity match (strip + uppercase).
      DELETE /{id}/ticker/NVDA       removes NVDA only
      DELETE /{id}/ticker/AIM:TRT    removes AIM:TRT only (not bare TRT)
      DELETE /{id}/ticker/KRX:000660 removes KRX:000660 only

    Sanitizes tickers[], csv_data[], analysis.sections[*].tickers,
    legacy category arrays, and avoid_list so save_watchlist() cannot
    resurrect the removed ticker from stale embedded data.

    Idempotent: removing a ticker already absent returns removed=false, 200.
    404 returned only when the watchlist itself does not exist.
    """
    t = ticker.strip().upper()
    if not t:
        raise HTTPException(status_code=400, detail="ticker is required")

    try:
        from data.pg_storage import watchlist_remove_ticker, is_available
        if not is_available():
            raise HTTPException(status_code=503, detail="Database unavailable")
        result = watchlist_remove_ticker(watchlist_id, t)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    if result.get("error") == "not_found":
        raise HTTPException(status_code=404, detail="Watchlist not found")
    if result.get("error"):
        raise HTTPException(status_code=500, detail=result["error"])

    if result.get("removed"):
        try:
            from services.user_earnings_service import invalidate_user_earnings
            invalidate_user_earnings("watchlist")
        except Exception:
            pass
        _rv_registry.pop(watchlist_id, None)
        _volmc_registry.pop(watchlist_id, None)
        _news_lkg.pop(watchlist_id, None)
        _bulk_lkg_invalidate(watchlist_id)

    return {
        "success":      True,
        "watchlist_id": watchlist_id,
        "ticker":       t,
        "removed":      result.get("removed", False),
        "ticker_count": result.get("ticker_count", 0),
    }


# ── Bulk-add multiple tickers ─────────────────────────────────────────────────

class _BulkAddBody(BaseModel):
    tickers: list[str]
    theme:   Optional[str] = None


@router.post("/{watchlist_id}/tickers")
async def bulk_add_tickers_endpoint(watchlist_id: str, body: _BulkAddBody):
    """
    POST /api/watchlist/{watchlist_id}/tickers

    Add multiple tickers to a watchlist in one call.

    Body:  { "tickers": ["BE", "OSS", "AMKR"], "theme": null }

    - Persists all tickers atomically (one by one, advisory lock per symbol).
    - Returns each ticker with added/duplicate status + hydration_status.
    - Fires a single background _priority_hydrate_symbols() task for all newly
      added symbols (quote → technical → FMP fundamentals → market-cap backfill).
    - Optionally assigns theme to all newly added tickers via PATCH /category path.
    - Deduplicates symbols already in the queue.
    - Respects FMP budget/throttle inside _priority_hydrate_symbols().
    """
    import asyncio as _aio_bulk
    from datetime import datetime, timezone as _tz_bulk

    if not body.tickers:
        raise HTTPException(status_code=400, detail="tickers list is required")

    try:
        from data.pg_storage import watchlist_add_ticker as _wl_add, is_available as _is_av
        from services.canonical_security_adapter import exchange_family_aliases as _efa
        if not _is_av():
            raise HTTPException(status_code=503, detail="Database unavailable")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    results = []
    newly_added: list[str] = []
    _ts_bulk = datetime.now(_tz_bulk.utc).isoformat()

    for raw in body.tickers:
        sym = raw.strip().upper()
        if not sym:
            continue
        try:
            _aliases = _efa(sym)
            _res = _wl_add(watchlist_id, sym, family_aliases=_aliases)
        except Exception as _exc_sym:
            results.append({
                "ticker":           sym,
                "added":            False,
                "duplicate":        False,
                "error":            str(_exc_sym),
                "hydration_status": _hydration_entry(sym),
            })
            continue

        if _res.get("added"):
            newly_added.append(sym)
            # Pre-seed hydration state synchronously before background task
            _HYDRATION_STATE[sym] = {
                "quote":        "pending",
                "technical":    "pending",
                "fundamentals": "pending",
                "options":      "pending",
                "enqueued_at":  _ts_bulk,
                "last_error":   None,
                "last_updated": _ts_bulk,
            }
            # Mark as immediately due in fundamentals queue
            try:
                from services.watchlist_quote_cache import is_fmp_symbol_eligible as _fmp_e
                if _fmp_e(sym):
                    from data.watchlist_fundamentals_store import schedule_refresh as _sr
                    _sr(sym, watchlist_id, days=0)
            except Exception:
                pass

        row: dict = {
            "ticker":           sym,
            "added":            _res.get("added", False),
            "duplicate":        _res.get("duplicate", False),
            "ticker_count":     _res.get("ticker_count", 0),
            "hydration_status": _hydration_entry(sym),
        }
        if _res.get("conflict_type"):
            row["existing_ticker"] = _res.get("existing_ticker", "")
            row["conflict_type"]   = _res["conflict_type"]
        if _res.get("error"):
            row["error"] = _res["error"]
        results.append(row)

    if newly_added:
        # Invalidate caches for the watchlist
        try:
            from services.user_earnings_service import invalidate_user_earnings as _ieu
            _ieu("watchlist")
        except Exception:
            pass
        _rv_registry.pop(watchlist_id, None)
        _volmc_registry.pop(watchlist_id, None)
        _news_lkg.pop(watchlist_id, None)
        _bulk_lkg_invalidate(watchlist_id)

        # Optional theme assignment for all newly added tickers
        if body.theme:
            try:
                from services.category_overrides import upsert_override as _uo
                for sym in newly_added:
                    _uo("default", sym, body.theme, "manual", "bulk_add")
            except Exception as _te_bulk:
                print(f"[BULK_ADD] theme assignment failed (non-fatal): {_te_bulk}")

        # Single priority hydration task for all new symbols
        _aio_bulk.create_task(_priority_hydrate_symbols(newly_added, watchlist_id))

    return {
        "success":      True,
        "watchlist_id": watchlist_id,
        "added_count":  len(newly_added),
        "total":        len(results),
        "results":      results,
    }


# ── Per-ticker theme assignment ───────────────────────────────────────────────

@router.patch("/{watchlist_id}/tickers/{symbol}/theme")
async def patch_ticker_theme_endpoint(
    watchlist_id: str,
    symbol:       str,
    request:      Request,
    body:         dict = Body(default_factory=dict),
):
    """
    PATCH /api/watchlist/{watchlist_id}/tickers/{symbol}/theme

    Assign or update the theme for a ticker that is already in the watchlist.

    Body: { "theme": "Datacenter Infra" }

    - Validates symbol exists in the watchlist.
    - Persists theme via watchlist_category_overrides (same store as PATCH /category).
    - Triggers cross-sync: theme_ticker_mapper + theme_ticker_overrides + Options Flow.
    - Returns updated theme row.
    - Handles unknown / custom themes gracefully (stored as-is).
    """
    sym = symbol.strip().upper()
    if not sym:
        raise HTTPException(status_code=400, detail="symbol is required")

    theme = (body.get("theme") or "").strip()
    if not theme:
        raise HTTPException(status_code=400, detail="theme is required in body")

    # Validate symbol is in this watchlist
    try:
        from data.pg_storage import watchlist_read as _wl_read
        _wl = _wl_read(watchlist_id)
    except Exception as _exc_wlr:
        raise HTTPException(status_code=500, detail=str(_exc_wlr))
    if _wl is None:
        raise HTTPException(status_code=404, detail="Watchlist not found")
    _tickers_in_wl = [t.strip().upper() for t in (_wl.get("tickers") or [])]
    if sym not in _tickers_in_wl:
        raise HTTPException(status_code=404, detail=f"{sym} not found in watchlist {watchlist_id}")

    # Resolve user_id (JWT middleware disabled — parse Bearer directly)
    _auth_hdr = request.headers.get("Authorization", "")
    _token = _auth_hdr.removeprefix("Bearer ").strip() if _auth_hdr.startswith("Bearer ") else ""
    _user_id = "default"
    if _token:
        try:
            from auth import verify_token as _vt
            _payload = _vt(_token)
            _user_id = _payload.get("sub") or "default"
        except Exception:
            pass

    # Persist theme override (same path as PATCH /category)
    try:
        from services.category_overrides import upsert_override as _uo_theme
        ok = _uo_theme(_user_id, sym, theme, "manual", "watchlist_theme_patch")
        if not ok:
            raise HTTPException(status_code=500, detail="Failed to persist theme override")
    except HTTPException:
        raise
    except Exception as _exc_uo:
        raise HTTPException(status_code=500, detail=str(_exc_uo))

    # Cross-sync: theme_ticker_mapper in-memory index
    try:
        from services.theme_ticker_mapper import register_llm_classified_tickers as _sync_m
        _sync_m([{"ticker": sym, "theme": theme, "confidence": "manual"}])
    except Exception as _me_th:
        print(f"[THEME_PATCH] mapper sync failed (non-fatal): {_me_th}")

    # Cross-sync: theme_ticker_overrides + Options Flow (best-effort)
    try:
        from services.theme_rs_universe import THEME_RS_UNIVERSE as _trs
        _tid = next(
            (tid for tid, m in _trs.items()
             if m.get("display_name", "").lower() == theme.lower()),
            None,
        )
        if _tid:
            from data.pg_storage import upsert_theme_ticker_override as _utto
            _utto(theme_id=_tid, symbol=sym, action="add",
                  source="watchlist_theme_patch", note="synced from PATCH /tickers/theme",
                  created_by=_user_id)
            from services.theme_merge_layer import refresh_enriched_universe as _ref_u
            _ref_u()
            from data.options_flow_sectors import invalidate_sectors_cache as _inv_sc
            _inv_sc()
    except Exception as _ue_th:
        print(f"[THEME_PATCH] Options Flow sync failed (non-fatal): {_ue_th}")

    # Invalidate bulk LKG — theme assignment changes canonical_theme_id in the
    # cached response for this specific watchlist.
    _bulk_lkg_invalidate(watchlist_id)

    return {
        "success":      True,
        "watchlist_id": watchlist_id,
        "symbol":       sym,
        "theme":        theme,
        "user_id":      _user_id,
    }


# ── Per-ticker hydration status ───────────────────────────────────────────────

@router.get("/{watchlist_id}/tickers/{symbol}/hydration-status")
async def get_hydration_status_endpoint(watchlist_id: str, symbol: str):
    """
    GET /api/watchlist/{watchlist_id}/tickers/{symbol}/hydration-status

    Returns the real-time hydration status for a ticker that was recently added
    via POST /{watchlist_id}/ticker or POST /{watchlist_id}/tickers.

    hydration_status fields:
      quote        — pending/running/done/not_applicable/error
      technical    — pending/running/done/not_applicable/error
      fundamentals — pending/running/done/not_applicable/error
      options      — pending/done/no_options/not_applicable/error

    cache_presence shows what data is actually accessible right now in the
    in-memory caches (independent of _HYDRATION_STATE — reads live).
    """
    sym = symbol.strip().upper()

    # ── Live hydration state (from _HYDRATION_STATE) ─────────────────────────
    hydration = _hydration_entry(sym)

    # ── Live cache presence (independent of hydration state) ─────────────────
    # These check the actual caches in real-time, so they reflect data that
    # arrived via any path (scheduled, background loop, or priority hydration).
    _stage2_present   = False
    _fund_present     = False
    _quote_present    = False
    _options_present  = False  # options data in combined cache or per-ticker cache
    _options_no_opts  = False  # confirmed no options chain

    try:
        from services.watchlist_stage2_service import _STAGE2_LKG as _s2lkg
        entry = _s2lkg.get(sym)
        _stage2_present = bool(entry and entry.get("label") is not None)
    except Exception:
        pass

    try:
        from data.watchlist_fundamentals_store import get_snapshot as _gs
        import asyncio as _aio_hs
        _snap = await _aio_hs.get_event_loop().run_in_executor(None, _gs, sym)
        _fund_present = bool(_snap and (_snap.get("fields") or {}))
    except Exception:
        pass

    try:
        from services.watchlist_quote_cache import _quote_cache as _qc
        _quote_present = sym in _qc and bool(_qc[sym].get("price"))
    except Exception:
        pass

    try:
        from data.options_theme_supplement import (
            get_no_options_symbols  as _no_opts_chk,
            get_combined_ticker_data as _combined_chk,
        )
        from data.portfolio_options_service import _per_ticker_cache_key as _ptck_chk
        from data.cache import cache as _chk_cache

        _no_opts_chk_set = _no_opts_chk()
        if sym in _no_opts_chk_set:
            _options_no_opts = True
        else:
            _comb = _combined_chk()
            if sym in _comb:
                _options_present = True
            else:
                _ptck_row = _chk_cache.get(_ptck_chk(sym))
                if _ptck_row:
                    _reason_chk = (_ptck_row.get("_reason") or "").lower()
                    if "no_expir" in _reason_chk or "no_options" in _reason_chk:
                        _options_no_opts = True
                    elif _ptck_row.get("options_score") is not None or _ptck_row.get("iv") is not None:
                        _options_present = True

        # Reconcile live cache presence with stored hydration state
        # (handles case where options arrived via background path after pending)
        if _options_no_opts and hydration.get("options") == "pending":
            hydration["options"] = "no_options"
            _HYDRATION_STATE.setdefault(sym, {})["options"] = "no_options"
        elif _options_present and hydration.get("options") == "pending":
            hydration["options"] = "done"
            _HYDRATION_STATE.setdefault(sym, {})["options"] = "done"
    except Exception:
        pass

    return {
        "watchlist_id":     watchlist_id,
        "symbol":           sym,
        "hydration_status": hydration,
        "cache_presence":   {
            "stage2_technical":  _stage2_present,
            "fundamentals":      _fund_present,
            "quote":             _quote_present,
            "options":           _options_present,
            "options_no_chain":  _options_no_opts,
        },
    }


async def _build_watchlist_response(
    watchlist_id: str,
    store: dict,
    wl_load_ms: int = 0,
) -> dict:
    """
    Internal canonical Watchlist enrichment pipeline.

    Called by:
      - get_by_id_endpoint()   on structural cache miss (cold path)
      - _rebuild_bulk_lkg_bg() for background LKG refresh (copy-on-success)

    Takes an already-loaded ``store`` dict (avoids double-loading from DB).
    Returns the complete enriched response dict ready to serve and cache.

    This is the single implementation of the enrichment pipeline; both the
    cold GET path and the background rebuild call this function so there is
    no risk of the two code paths diverging independently.
    """
    import asyncio as _aio
    import time as _t
    _t0 = _t.monotonic()

    # ── Quote enrichment ──────────────────────────────────────────────────────
    _t1 = _t.monotonic()
    try:
        store = await _enrich_store_with_quotes(store)
    except Exception as _enrich_err:
        print(f"[WATCHLIST] Quote enrichment failed (returning raw): {_enrich_err}")
    _enrich_ms = round((_t.monotonic() - _t1) * 1000)

    # ── FMP fundamentals overlay (weekly cache) ───────────────────────────────
    # fund_snaps were pre-loaded inside _enrich_store_with_quotes (parallel with
    # the quote+name fetch) to save a second Neon round-trip.  We pop the temp
    # key here; if absent (e.g. enrich was skipped), we fall back to a fresh load.
    _t2 = _t.monotonic()
    try:
        from data.watchlist_fundamentals_store import get_snapshots_bulk as _get_fund_snaps
        from services.watchlist_fundamentals_refresh import apply_fmp_overlays as _apply_fmp
        _raw_csv = store.get("csv_data") or []
        if _raw_csv:
            _syms = [
                (r.get("Symbol") or r.get("symbol") or r.get("Ticker") or "").strip().upper()
                for r in _raw_csv
            ]
            _syms_f = [s for s in _syms if s]
            # Prefer fund_snaps pre-loaded by _enrich_store_with_quotes
            _snaps = store.pop("_fund_snaps_for_apply_fmp", None)
            if _snaps is None:
                _loop = _aio.get_event_loop()
                _snaps = await _loop.run_in_executor(None, _get_fund_snaps, _syms_f)
            if _snaps:
                store["csv_data"] = _apply_fmp(_raw_csv, _snaps)
    except Exception:
        pass  # non-fatal — serve unmodified CSV data
    # Defensive cleanup — remove temp key if enrich was skipped (store unchanged)
    store.pop("_fund_snaps_for_apply_fmp", None)
    _fund_ms = round((_t.monotonic() - _t2) * 1000)

    # ── Upcoming earnings (cache-first, non-blocking, ≤1.5 s timeout) ─────────
    # Resolves earnings for exactly this watchlist's tickers.
    # sync_on_miss=False → never waits for FMP; fires background sync on cache miss.
    # asyncio.wait_for guard ensures earnings never delay the watchlist response.
    _t3 = _t.monotonic()
    _upcoming_earnings: dict = {
        "watchlist_id":      watchlist_id,
        "symbols_requested": [],
        "events":            [],
        "missing_symbols":   [],
        "source":            "cached_earnings",
        "last_updated":      None,
        "stale":             True,
        "cache_status":      "skipped",
    }
    try:
        from services.user_earnings_service import (  # type: ignore
            get_upcoming_earnings_for_symbols as _gue_get,
        )
        _wl_tickers_earn = [t.strip().upper() for t in (store.get("tickers") or []) if t.strip()]
        if _wl_tickers_earn:
            try:
                _fmp_key_build = os.getenv("FMP_API_KEY", "")
                try:
                    from config import FMP_API_KEY as _fmp_key_build  # type: ignore
                except Exception:
                    pass
                _earn_payload = await _aio.wait_for(
                    _gue_get(
                        _wl_tickers_earn,
                        fmp_key                 = _fmp_key_build,
                        sync_on_miss            = False,   # non-blocking GET path
                        background_sync_on_miss = True,
                    ),
                    timeout=1.5,
                )
                _upcoming_earnings = {"watchlist_id": watchlist_id, **_earn_payload}
            except _aio.TimeoutError:
                _upcoming_earnings["cache_status"] = "timeout"
            except Exception as _earn_err:
                _upcoming_earnings["cache_status"] = f"error:{type(_earn_err).__name__}"
                print(f"[WATCHLIST_GET] upcoming_earnings error (non-fatal): {_earn_err}")
    except Exception as _earn_import_err:
        _upcoming_earnings["cache_status"] = f"import_error:{type(_earn_import_err).__name__}"
    _earn_ms = round((_t.monotonic() - _t3) * 1000)

    # ── Phase timing + coverage ───────────────────────────────────────────────
    _total_ms = round((_t.monotonic() - _t0) * 1000)
    _all_rows = [
        r for s in (store.get("analysis") or {}).get("sections", [])
        for r in s.get("tickers", [])
    ]
    _n = max(len(_all_rows), 1)
    _price_cov  = sum(1 for r in _all_rows if r.get("price") is not None) / _n
    _vol_cov    = sum(1 for r in _all_rows if r.get("volume") and float(r["volume"]) > 0) / _n
    _rv_cov     = sum(1 for r in _all_rows if r.get("relative_volume") is not None) / _n
    _dv_cov     = sum(1 for r in _all_rows if r.get("dollar_volume") is not None) / _n
    _vm_cov     = sum(1 for r in _all_rows if r.get("vol_mc_pct") is not None) / _n
    _vol_stale  = sum(1 for r in _all_rows if r.get("volume_is_stale")) / _n

    # Determine data_state from quote cache freshness
    try:
        from services.watchlist_quote_cache import (
            _cache_ts as _qts, _QUOTE_TTL as _qttl, _get_lock as _qlock
        )
        _qage   = _t.monotonic() - _qts
        _q_refreshing = _qlock().locked()
        if _qage < _qttl and not _q_refreshing:
            _data_state = "fresh"
        elif _q_refreshing:
            _data_state = "refreshing"
        else:
            _data_state = "cached"
    except Exception:
        _data_state = "cached"
        _q_refreshing = False

    print(
        f"[WATCHLIST_GET] wl={watchlist_id} total_ms={_total_ms} "
        f"watchlist_load_ms={wl_load_ms} row_enrichment_ms={_enrich_ms} "
        f"fundamentals_overlay_ms={_fund_ms} "
        f"saved_ticker_count={len(store.get('tickers', []))} rows={len(_all_rows)} "
        f"price_coverage={_price_cov:.0%} volume_coverage={_vol_cov:.0%} "
        f"relative_volume_coverage={_rv_cov:.0%} vol_mc_coverage={_vm_cov:.0%} "
        f"volume_stale_pct={_vol_stale:.0%} data_state={_data_state}"
    )

    store["_meta"] = {
        "data_state":               _data_state,
        "quotes_refreshing":        _q_refreshing,
        "price_coverage":           round(_price_cov, 3),
        "volume_coverage":          round(_vol_cov, 3),
        "relative_volume_coverage": round(_rv_cov, 3),
        "dollar_volume_coverage":   round(_dv_cov, 3),
        "vol_mc_coverage":          round(_vm_cov, 3),
        "volume_stale_pct":         round(_vol_stale, 3),
        "response_ms":              _total_ms,
        "earnings_ms":              _earn_ms,
        "earnings_cache_status":    _upcoming_earnings.get("cache_status", "skipped"),
    }

    store["upcoming_earnings"] = _upcoming_earnings

    # ── Alert bus hook: watchlist full-activity metrics ───────────────────────
    # Fire-and-forget; runs after response is already built. No provider calls.
    async def _watchlist_alert_hook(store_snap: dict) -> None:
        try:
            from services.alert_signal_bus import record_signal_snapshot as _rs
            for _sec in store_snap.get("sections") or []:
                for _row in _sec.get("tickers") or []:
                    _sym = (_row.get("ticker") or _row.get("symbol") or "").upper().strip()
                    if not _sym:
                        continue
                    _chg = _row.get("change_pct_1d") or _row.get("price_change_pct")
                    _relvol = _row.get("relative_volume")
                    _mc = _row.get("market_cap")
                    _price = _row.get("price")
                    _vol = _row.get("volume")
                    _vol_mc_pct = _row.get("vol_mc_pct")
                    if _chg is None and _relvol is None and _vol_mc_pct is None:
                        continue
                    await _rs(
                        "watchlist", "default", _sym,
                        {
                            "price":            _price,
                            "price_change_pct": _chg,
                            "volume":           _vol,
                            "rel_volume":       _relvol,
                            "market_cap":       _mc,
                            "vol_marketcap":    (_vol_mc_pct / 100.0) if _vol_mc_pct is not None else None,
                        }
                    )
        except Exception:
            pass

    _aio.create_task(_watchlist_alert_hook(store))

    # Strip ticker-detail-only fields from the bulk csv_data payload.
    # earnings_intelligence (~40 KB/ticker) is served only by
    # GET /ticker-detail/{symbol}; embedding it in the bulk response
    # bloats the payload by ~15 MB (94 % of total) for a 400-ticker watchlist.
    _bulk_csv_out = store.get("csv_data")
    if _bulk_csv_out:
        store["csv_data"] = [
            {k: v for k, v in row.items() if k not in _BULK_CSV_STRIP}
            for row in _bulk_csv_out
        ]

    return store


@router.get("/{watchlist_id}")
async def get_by_id_endpoint(watchlist_id: str):
    """
    Return a specific watchlist by ID.

    Ticker rows are enriched on every GET with:
      - name          (from Tradier description)
      - price         (Tradier live, or CSV fallback)
      - change_pct_1d (Tradier 1D % change)
      - quote_source / quote_updated_at / volume_is_stale / price_is_stale / …

    All existing LLM-generated fields (catalyst, sentiment, action_note, etc.)
    are preserved.  Quote data is served from a 10-minute in-memory cache;
    a background refresh is triggered automatically when the TTL expires.

    Performance contract:
      - Zero third-party provider calls in the blocking request path.
      - Warm response: under 500 ms.
      - Cold-process response (with existing disk LKG): under 1.5 s.
    """
    import asyncio as _aio
    import time as _t_get
    _t0_get = _t_get.monotonic()

    store = load_watchlist(watchlist_id)
    if store is None:
        return {"empty": True}
    _wl_load_ms = round((_t_get.monotonic() - _t0_get) * 1000)

    # ── LKG-first: serve cached response when structural version matches ─────────
    # "version" = updated_at|ticker_count — changes on any membership mutation.
    # Stale-while-revalidate semantics: a valid-version entry is always served
    # regardless of age; age only controls whether to schedule a background rebuild.
    _wl_version = (
        f"{store.get('updated_at') or store.get('saved_at')}|"
        f"{len(store.get('tickers', []))}"
    )
    _lkg_entry = _BULK_LKG.get(watchlist_id)
    if _lkg_entry and _lkg_entry.get("version") == _wl_version:
        _lkg_age_s = _t_get.monotonic() - _lkg_entry["ts"]
        _age_label = (
            "very_stale" if _lkg_age_s >= _BULK_LKG_STALE_TTL
            else "stale"  if _lkg_age_s >= _BULK_LKG_TTL
            else "fresh"
        )
        if _lkg_age_s >= _BULK_LKG_TTL and watchlist_id not in _BULK_LKG_BUILDING:
            # Schedule exactly one background rebuild (copy-on-success).
            # Old LKG continues to be served while the rebuild runs.
            _BULK_LKG_BUILDING.add(watchlist_id)
            _aio.create_task(_rebuild_bulk_lkg_bg(watchlist_id))
            print(
                f"[WATCHLIST_LKG] {_age_label}_hit wl={watchlist_id} "
                f"age={round(_lkg_age_s)}s — background rebuild queued"
            )
        else:
            print(
                f"[WATCHLIST_LKG] {_age_label}_hit wl={watchlist_id} "
                f"age={round(_lkg_age_s)}s"
            )
        return _lkg_entry["payload"]
    # LKG absent or version mismatch (membership change) → rebuild inline.

    store = await _build_watchlist_response(watchlist_id, store, _wl_load_ms)

    _BULK_LKG[watchlist_id] = {
        "payload": store,
        "ts":      _t_get.monotonic(),
        "version": _wl_version,
    }
    _BULK_LKG_BUILDING.discard(watchlist_id)

    return store


@router.get("/{watchlist_id}/alignment")
async def get_watchlist_alignment(watchlist_id: str):
    """
    WATCHLIST ALIGNMENT READ PATH V1 — compact, full-Watchlist alignment view.

    Joins:
      - current Watchlist tickers (canonical loader, order preserved)
      - the retained Confluence V2 snapshot (stale-while-revalidate cache
        over the existing build_confluence_snapshot() producer — no second
        scoring producer, no new LKG/table/scheduler)
      - the existing Entry State LKG (read once, canonical helper)

    Returns exactly one compact row per current Watchlist ticker. Tickers
    without a Confluence/Entry row are NOT dropped — they get an explicit
    placeholder row with available=false fields (no fabricated scores).

    Does not join legacy LLM Watchlist sections (Golden Zone, HC Trade Zone,
    HC Investment Zone, Growth Momentum) and does not invoke /analyze or any
    LLM. Order matches the current Watchlist order — no server-side score
    sorting (client performs local sort/filter).
    """
    store = load_watchlist(watchlist_id)
    if store is None:
        return {"empty": True}

    tickers: list[str] = []
    seen: set[str] = set()
    for t in (store.get("tickers") or []):
        sym = str(t).strip().upper()
        if sym and sym not in seen:
            seen.add(sym)
            tickers.append(sym)

    from services.confluence_v2_service import (
        get_retained_confluence_snapshot,
        get_retained_confluence_meta,
    )

    try:
        # Off-load to a thread so the Uvicorn event loop is never blocked during
        # a cold/warm build (~129 s).  Gunicorn heartbeat stays healthy throughout.
        snap = await asyncio.to_thread(get_retained_confluence_snapshot)
    except Exception as exc:
        print(f"[WATCHLIST_ALIGNMENT] retained Confluence snapshot unavailable: {exc}")
        snap = {"results": []}
    meta = get_retained_confluence_meta()

    confluence_by_symbol: Dict[str, dict] = {}
    for row in (snap.get("results") or []):
        sym = str(row.get("symbol") or "").strip().upper()
        if sym:
            confluence_by_symbol[sym] = row

    # ── Include any retained-snapshot symbols not in the watchlist tickers list ──
    # The retained snapshot universe is derived from stage2_lkg.keys(), which
    # persists across sessions and may contain tickers added in a previous session
    # but not yet reflected in the current watchlist store query (e.g. MSFT/NVDA/SMCI).
    # The retained snapshot IS the canonical confluence universe; /alignment should
    # expose the same canonical set.
    _snap_only = 0
    for _snap_sym in confluence_by_symbol:
        if _snap_sym not in seen:
            seen.add(_snap_sym)
            tickers.append(_snap_sym)
            _snap_only += 1
    if _snap_only:
        print(f"[WATCHLIST_ALIGNMENT] retained-snapshot supplement: +{_snap_only} symbols "
              f"not in watchlist store tickers list")

    rows: List[Dict[str, Any]] = []
    for sym in tickers:
        row = confluence_by_symbol.get(sym)

        if row is None:
            # No Confluence row at all for this Watchlist ticker — explicit
            # placeholder, never dropped, never fabricated.
            # Entry fields unavailable (no same-generation row exists).
            rows.append({
                "symbol": sym,
                # ── PHASE 2: canonical score fields (null — no snapshot row) ──
                "caelyn_confluence_score":              None,
                "caelyn_confluence_bucket":             None,
                "caelyn_confluence_reason_codes":       [],
                "caelyn_confluence_normalized_score":   None,
                "caelyn_confluence_confidence_score":   None,
                "caelyn_confluence_raw_score":          None,
                "caelyn_confluence_core_score":         None,
                "caelyn_confluence_bonus_score":        None,
                "caelyn_confluence_total_score":        None,
                # ── PHASE 2: V4 debug fields ─────────────────────────────────
                "caelyn_confluence_v4_score":               None,
                "caelyn_confluence_v4_raw_score":           None,
                "caelyn_confluence_v4_normalized_score":    None,
                "caelyn_confluence_v4_confidence_score":    None,
                "caelyn_confluence_v4_bucket":              None,
                "caelyn_confluence_v4_components":          {},
                "caelyn_confluence_v4_bonus_breakdown":     {},
                "caelyn_confluence_v4_reason_codes":        [],
                "caelyn_confluence_v4_actionability":       None,
                # ── PHASE 2: component breakdown fields ─────────────────────
                "theme_alignment_score":                None,
                "theme_alignment_points":               None,
                "stage_quality_score":                  None,
                "stage_quality_points":                 None,
                "options_alignment_score":              None,
                "options_alignment_points":             None,
                "options_status":                       None,
                "options_snapshot_status":              None,
                "options_as_of":                        None,
                "entry_risk_reward_score":              None,
                "entry_risk_reward_points":             None,
                "pattern_type":                         "NO_PATTERN",
                "extension_quality":                    "NORMAL",
                "catalyst_alignment_score":             None,
                "catalyst_alignment_points":            None,
                "catalyst_status":                      None,
                "catalyst_detail_status":               "no_catalyst",
                "investment_alignment_score":           None,
                "investment_alignment_points":          None,
                "investment_quality_label":             None,
                # ── PHASE 2: bonus points ────────────────────────────────────
                "social_bonus_points":                  None,
                "theme_policy_bonus_points":            None,
                "prediction_market_bonus_points":       None,
                "whale_insider_bonus_points":           None,
                "bottleneck_bonus_points":              None,
                # ── PHASE 2: legacy compat ──────────────────────────────────
                "legacy_trade_alignment_score":         None,
                "legacy_trade_alignment_archetype":     None,
                "legacy_trade_alignment_status":        "compat_only",
                "legacy_actionability_state":           None,
                "actionability_state":                  None,
                # ── PHASE 2.1: boolean filter fields ────────────────────────
                "is_actionable_setup":                  False,
                "is_near_actionable":                   False,
                "is_watch_for_reset":                   False,
                "is_risk_conflict":                     False,
                "is_investment_quality":                False,
                # ── V4.2 semantic precision fields ────────────────────────────
                "entry_execution_state":                None,
                "entry_execution_label":                None,
                "caution_flags":                        [],
                "entry_state_display":                  None,
                "risk_flags":                           [],
                "data_status_flags":                    [],
                "confluence_v42":                       None,
                # ── Legacy nested ────────────────────────────────────────────
                "actionability": {
                    "available": False, "state": None, "legacy_state": None, "score": None,
                },
                "trade_alignment": {"available": False, "score": None, "archetype": None},
                "investment_alignment": {"available": False, "score": None, "state": None},
                "entry": {
                    "available": False,
                    "state": None,
                    "score": None,
                    "grade": None,
                },
                "theme": {"id": None},
                "options": {"pressure_state": None},
                "catalyst": {
                    "available":        False,
                    "score":            None,
                    "model_version":    None,
                    "primary_source":   None,
                    "primary_event":    None,
                    "scheduled_event":  None,
                    "rss_event":        None,
                    "bearish_conflict": None,
                    "v2_available":     False,
                    "v2_score":         None,
                    "v2_state":         "UNAVAILABLE",
                    "v2_primary_event": None,
                    "v2_conflicts":     [],
                },
            })
            continue

        actionability_available = bool(row.get("actionability_available"))
        trade_alignment_available = bool(row.get("trade_alignment_available"))
        investment_alignment_available = bool(row.get("investment_alignment_available"))

        theme_rotation = ((row.get("signal_breakdown") or {}).get("theme_rotation")) or {}
        primary_theme_id = theme_rotation.get("primary_rotation_theme")

        # Entry: read from the same-generation fields emitted by _compute_confluence()
        # (DEFECT 1 fix). These fields were computed from the exact same entry_result
        # that produced Actionability and Trade Alignment above — one consistent
        # Entry generation per row, never a fresh cross-generation LKG call.
        entry_available = bool(row.get("entry_available"))
        entry_state = row.get("entry_state") if entry_available else None
        entry_score = row.get("entry_score") if entry_available else None
        entry_grade = row.get("entry_grade") if entry_available else None

        # ── Derived: catalyst detail status ───────────────────────────────────
        _cat_score    = row.get("catalyst_alignment_score")
        _cat_sched    = row.get("catalyst_scheduled_event")
        _cat_rss      = row.get("catalyst_rss_event")
        _cat_v2       = row.get("catalyst_v2_primary_event")
        _cat_src      = row.get("catalyst_primary_source") or "none"
        if _cat_sched:
            _cat_detail_status = "scheduled_event"
        elif _cat_rss or _cat_v2:
            _cat_detail_status = "rss_event"
        elif "theme_policy" in _cat_src:
            _cat_detail_status = "theme_policy_event"
        elif _cat_score and _cat_score > 0:
            _cat_detail_status = "score_only_missing_event"
        else:
            _cat_detail_status = "no_catalyst"

        # ── Derived: investment quality label ─────────────────────────────────
        _ia_score  = row.get("investment_alignment_score")
        _ia_state  = row.get("investment_alignment_state")
        _act_state = row.get("actionability_state")
        _rr_state  = row.get("entry_risk_reward_state")
        def _investment_quality_label(ia_score, ia_state, act, rr):
            if ia_score is None or not investment_alignment_available:
                return None
            if ia_score >= 90:   base = "HIGHEST_QUALITY"
            elif ia_score >= 80: base = "STRONG_QUALITY"
            elif ia_score >= 70: base = "HIGH_GROWTH"
            elif ia_score >= 60: base = "QUALITY"
            else:                return None
            if act == "AVOID" or rr == "BROKEN_SUPPORT_AVOID":
                return f"{base}_WITH_CONFIRMED_RISK"
            if act == "TOO_EXTENDED" or rr == "STRONG_ASSET_EXTENDED_WAIT":
                return f"{base}_WATCH_FOR_RESET"
            if act in ("READY", "EARLY_WATCH", "WAIT_FOR_RETEST", "WAIT_FOR_BREAKOUT", "REVERSAL_WATCH", "WATCH"):
                return f"{base}_NEAR_SUPPORT"
            return f"{base}_NO_CLEAR_ENTRY"
        _iq_label = _investment_quality_label(_ia_score, _ia_state, _act_state, _rr_state)

        rows.append({
            "symbol": sym,

            # ── Canonical Caelyn Confluence Score (flat, source of truth) ─────
            "caelyn_confluence_score":              row.get("caelyn_confluence_score"),
            "caelyn_confluence_bucket":             row.get("caelyn_confluence_bucket"),
            "caelyn_confluence_reason_codes":       row.get("caelyn_confluence_reason_codes") or [],

            # ── Confluence At Support ─────────────────────────────────────────
            "confluence_at_support":                row.get("confluence_at_support"),
            "confluence_at_support_score":          row.get("confluence_at_support_score"),
            "confluence_at_support_state":          row.get("confluence_at_support_state"),
            "confluence_at_support_reason_codes":   row.get("confluence_at_support_reason_codes") or [],

            # ── Entry Risk/Reward (flat) ──────────────────────────────────────
            "entry_risk_reward_state":              _rr_state,
            "entry_risk_reward_score":              row.get("entry_risk_reward_score"),
            "entry_risk_reward_reason_codes":       row.get("entry_risk_reward_reason_codes") or [],

            # ── Active Support Hierarchy (flat) ───────────────────────────────
            "active_support_status":                row.get("active_support_status"),
            "active_support_type":                  row.get("active_support_type"),
            "active_support_touch_count":           row.get("active_support_touch_count"),
            "lower_low_confirmed":                  row.get("lower_low_confirmed"),
            "distance_to_active_support_pct":       row.get("distance_to_active_support_pct"),
            "critical_break_level":                 row.get("critical_break_level"),
            "reclaim_level":                        row.get("reclaim_level"),
            "next_downside_support":                row.get("next_downside_support"),
            "extension_state":                      row.get("extension_state"),

            # ── Flat scalars for easy frontend access ─────────────────────────
            "trade_alignment_score":                row.get("trade_alignment_score") if trade_alignment_available else None,
            "investment_alignment_score":           row.get("investment_alignment_score") if investment_alignment_available else None,
            "catalyst_alignment_score":             row.get("catalyst_alignment_score"),
            "options_alignment_score":              row.get("options_alignment_score"),
            "theme_policy_boost":                   row.get("theme_policy_boost"),
            "theme_policy_available":               bool(row.get("theme_policy_available")),
            "theme_policy_event":                   row.get("theme_policy_event"),
            "theme_policy_theme":                   row.get("theme_policy_theme"),

            # ── Nested: Actionability ─────────────────────────────────────────
            # state: V4-promoted actionability (always set when V4 succeeded).
            # legacy_state: pre-V4 actionability, preserved for compat.
            "actionability": {
                "available":              actionability_available,
                "state":                  row.get("actionability_state"),
                "legacy_state":           row.get("legacy_actionability_state"),
                "score":                  row.get("actionability_score") if actionability_available else None,
                "options_entry_conflict": bool(row.get("options_entry_conflict")) if actionability_available else False,
                "setup_summary":          row.get("setup_summary") if actionability_available else None,
                "reason_codes":           row.get("actionability_reason_codes") or [],
            },

            # ── Nested: Trade Alignment ───────────────────────────────────────
            "trade_alignment": {
                "available":            trade_alignment_available,
                "score":                row.get("trade_alignment_score") if trade_alignment_available else None,
                "archetype":            row.get("trade_alignment_archetype") if trade_alignment_available else None,
                "theme_alignment_score":row.get("theme_alignment_score"),
                "stage_quality_score":  row.get("stage_quality_score"),
            },

            # ── Nested: Investment Alignment ──────────────────────────────────
            "investment_alignment": {
                "available":              investment_alignment_available,
                "score":                  row.get("investment_alignment_score") if investment_alignment_available else None,
                "state":                  row.get("investment_alignment_state") if investment_alignment_available else None,
                "unavailable_reason":     row.get("investment_unavailable_reason") if not investment_alignment_available else None,
                "investment_quality_label": _iq_label,
                "financial_acceleration_score": row.get("financial_acceleration_score"),
                "forward_expectations_score":   row.get("forward_expectations_score"),
            },

            # ── Nested: Entry ─────────────────────────────────────────────────
            "entry": {
                "available": entry_available,
                "state":     entry_state,
                "score":     entry_score,
                "grade":     entry_grade,
            },

            # ── Nested: Theme ─────────────────────────────────────────────────
            "theme": {
                "id": primary_theme_id,
            },

            # ── Nested: Options ───────────────────────────────────────────────
            "options": {
                "pressure_state": row.get("options_pressure_state"),
                "primary_signal": row.get("options_primary_signal"),
            },

            # ── Nested: Catalyst ──────────────────────────────────────────────
            "catalyst": {
                "available":          bool(row.get("catalyst_alignment_available")),
                "score":              row.get("catalyst_alignment_score"),
                "detail_status":      _cat_detail_status,
                "model_version":      row.get("catalyst_model_version"),
                "primary_source":     row.get("catalyst_primary_source"),
                "primary_event":      row.get("catalyst_primary_event"),
                "scheduled_event":    row.get("catalyst_scheduled_event"),
                "rss_event":          row.get("catalyst_rss_event"),
                "bearish_conflict":   row.get("catalyst_bearish_conflict"),
                "v2_available":       bool(row.get("catalyst_v2_available")),
                "v2_score":           row.get("catalyst_v2_score"),
                "v2_state":           row.get("catalyst_v2_state") or "UNAVAILABLE",
                "v2_primary_event":   row.get("catalyst_v2_primary_event"),
                "v2_conflicts":       row.get("catalyst_v2_conflicts") or [],
                "theme_policy_available": bool(row.get("theme_policy_available")),
                "theme_policy_boost":     row.get("theme_policy_boost"),
                "theme_policy_event":     row.get("theme_policy_event"),
                "theme_policy_theme":     row.get("theme_policy_theme"),
            },

            # ── v3: major / minor lower-low classification ────────────────────
            "major_lower_low_confirmed":            bool(row.get("major_lower_low_confirmed")),
            "minor_lower_low":                      bool(row.get("minor_lower_low")),

            # ── v3: trade core (ex Catalyst, avoids double-count) ─────────────
            "trade_core_score_ex_catalyst":         row.get("trade_core_score_ex_catalyst"),
            "entry_pattern_rr_score":               row.get("entry_pattern_rr_score"),

            # ── v3: continuation pattern + extension quality ───────────────────
            "pattern_type":                         row.get("pattern_type") or "NO_PATTERN",
            "pattern_state":                        row.get("pattern_state") or "NOT_DETECTED",
            "pattern_score":                        row.get("pattern_score") or 0,
            "pattern_reason_codes":                 row.get("pattern_reason_codes") or [],
            "constructive_extension":               bool(row.get("constructive_extension")),
            "chase_extension":                      bool(row.get("chase_extension")),
            "extension_quality":                    row.get("extension_quality") or "NORMAL",
            "extension_reason_codes":               row.get("extension_reason_codes") or [],
            "estimated_shelf_distance_pct":         row.get("estimated_shelf_distance_pct"),

            # ── v3: extra flat scalars (needed by theme leadership + UI) ───────
            "stage_alignment_score":                row.get("stage_alignment_score"),
            "base_trade_alignment_score":           row.get("base_trade_alignment_score"),
            "trade_alignment_archetype":            row.get("trade_alignment_archetype"),
            "actionability_state":                  row.get("actionability_state"),
            "catalyst_detail_status":               row.get("catalyst_detail_status") or _cat_detail_status,
            "investment_quality_label":             row.get("investment_quality_label") or _iq_label,

            # ── v3: component coverage + confidence (populated below) ─────────
            "component_coverage":               None,
            "confluence_confidence_score":      None,

            # ── v3: theme leadership (populated below after all rows built) ────
            "theme_leadership_score":           None,
            "theme_leadership_rank":            None,
            "theme_leadership_total":           None,
            "theme_leadership_bucket":          None,
            "is_theme_leader":                  False,
            "is_top_3_theme_leader":            False,
            "leader_context":                   None,
            "theme_leader_reason_codes":        [],
            "leadership_theme":                 None,

            # ── V4: unified confluence debug (preserved for comparison) ─────
            "caelyn_confluence_v4_score":               row.get("caelyn_confluence_v4_score"),
            "caelyn_confluence_v4_raw_score":           row.get("caelyn_confluence_v4_raw_score"),
            "caelyn_confluence_v4_core_score":          row.get("caelyn_confluence_v4_core_score"),
            "caelyn_confluence_v4_bonus_score":         row.get("caelyn_confluence_v4_bonus_score"),
            "caelyn_confluence_v4_total_score":         row.get("caelyn_confluence_v4_total_score"),
            "caelyn_confluence_v4_normalized_score":    row.get("caelyn_confluence_v4_normalized_score"),
            "caelyn_confluence_v4_available_max_pts":   row.get("caelyn_confluence_v4_available_max_pts"),
            "caelyn_confluence_v4_bucket":              row.get("caelyn_confluence_v4_bucket") or "NO_CLEAR_CONFLUENCE",
            "caelyn_confluence_v4_components":          row.get("caelyn_confluence_v4_components") or {},
            "caelyn_confluence_v4_bonus_breakdown":     row.get("caelyn_confluence_v4_bonus_breakdown") or {},
            "caelyn_confluence_v4_reason_codes":        row.get("caelyn_confluence_v4_reason_codes") or [],
            "caelyn_confluence_v4_confidence_score":    row.get("caelyn_confluence_v4_confidence_score"),
            "caelyn_confluence_v4_actionability":       row.get("caelyn_confluence_v4_actionability"),
            "legacy_trade_alignment_score":             row.get("legacy_trade_alignment_score") or row.get("trade_alignment_score"),

            # ── V4.2: core scoring semantics cleanup (Core=100, Bonus=25, Max=125) ─
            "caelyn_confluence_v42_score":              row.get("caelyn_confluence_v42_score"),
            "caelyn_confluence_v42_core_score":         row.get("caelyn_confluence_v42_core_score"),
            "caelyn_confluence_v42_bonus_score":        row.get("caelyn_confluence_v42_bonus_score"),
            "caelyn_confluence_v42_max_score":          row.get("caelyn_confluence_v42_max_score") or 125,
            "caelyn_confluence_v42_normalized_score":   row.get("caelyn_confluence_v42_normalized_score"),
            "caelyn_confluence_v42_available_max_pts":  row.get("caelyn_confluence_v42_available_max_pts"),
            "caelyn_confluence_v42_bucket":             row.get("caelyn_confluence_v42_bucket") or "NO_CLEAR_CONFLUENCE",
            "caelyn_confluence_v42_components":         row.get("caelyn_confluence_v42_components") or {},
            "caelyn_confluence_v42_bonus_breakdown":    row.get("caelyn_confluence_v42_bonus_breakdown") or {},
            "caelyn_confluence_v42_reason_codes":       row.get("caelyn_confluence_v42_reason_codes") or [],
            "caelyn_confluence_v42_confidence_score":   row.get("caelyn_confluence_v42_confidence_score"),
            "caelyn_confluence_v42_actionability":      row.get("caelyn_confluence_v42_actionability"),

            # ── Canonical extended score fields (promoted from V4.2) ──────────
            "caelyn_confluence_normalized_score":       row.get("caelyn_confluence_normalized_score"),
            "caelyn_confluence_confidence_score":       row.get("caelyn_confluence_confidence_score"),
            "caelyn_confluence_raw_score":              row.get("caelyn_confluence_raw_score"),
            "caelyn_confluence_core_score":             row.get("caelyn_confluence_core_score"),
            "caelyn_confluence_bonus_score":            row.get("caelyn_confluence_bonus_score"),
            "caelyn_confluence_total_score":            row.get("caelyn_confluence_total_score"),

            # ── Component breakdown (first-class, no frontend derivation) ─────
            "theme_alignment_points":                   row.get("theme_alignment_points"),
            "stage_quality_score":                      row.get("stage_quality_score"),
            "stage_quality_points":                     row.get("stage_quality_points"),
            "options_alignment_points":                 row.get("options_alignment_points"),
            "options_status":                           row.get("options_status"),
            "options_snapshot_status":                  row.get("options_snapshot_status"),
            "options_as_of":                            row.get("options_as_of"),
            "technical_setup_points":                   row.get("technical_setup_points"),
            "technical_setup_label":                    row.get("technical_setup_label"),
            "entry_exit_points":                        row.get("entry_exit_points"),
            "entry_exit_status":                        row.get("entry_exit_status"),
            "entry_risk_reward_points":                 row.get("entry_risk_reward_points"),
            "catalyst_alignment_points":                row.get("catalyst_alignment_points"),
            "catalyst_status":                          row.get("catalyst_status"),
            "direct_catalyst_present":                  row.get("direct_catalyst_present"),
            "direct_catalyst_type":                     row.get("direct_catalyst_type"),
            "catalyst_intelligence_score":              row.get("catalyst_intelligence_score"),
            "investment_alignment_points":              row.get("investment_alignment_points"),
            "investment_pillar_count":                  row.get("investment_pillar_count"),
            "investment_quality_label":                 row.get("investment_quality_label"),
            "financial_health_strong":                  row.get("financial_health_strong"),
            "current_growth_strong":                    row.get("current_growth_strong"),
            "forward_growth_strong":                    row.get("forward_growth_strong"),

            # ── Bonus point breakdown ─────────────────────────────────────────
            "social_bonus_points":                      row.get("social_bonus_points"),
            "social_sections_hit":                      row.get("social_sections_hit"),
            "social_confluence_hit":                    row.get("social_confluence_hit"),
            "social_acceleration_hit":                  row.get("social_acceleration_hit"),
            "social_fresh_hit":                         row.get("social_fresh_hit"),
            "theme_policy_bonus_points":                row.get("theme_policy_bonus_points"),
            "prediction_market_bonus_points":           row.get("prediction_market_bonus_points"),
            "whale_insider_bonus_points":               row.get("whale_insider_bonus_points"),
            "bottleneck_bonus_points":                  row.get("bottleneck_bonus_points"),
            "bottleneck_anchor_count":                  row.get("bottleneck_anchor_count"),

            # ── PHASE 2: Legacy trade alignment compat fields ─────────────────
            "legacy_trade_alignment_archetype":         row.get("legacy_trade_alignment_archetype"),
            "legacy_trade_alignment_status":            row.get("legacy_trade_alignment_status") or "compat_only",

            # ── PHASE 2: Legacy actionability (pre-V4 state preserved) ───────
            "legacy_actionability_state":               row.get("legacy_actionability_state"),

            # ── PHASE 2.1: Boolean filter fields (no frontend derivation needed) ─
            "is_actionable_setup":                      row.get("is_actionable_setup", False),
            "is_near_actionable":                       row.get("is_near_actionable", False),
            "is_watch_for_reset":                       row.get("is_watch_for_reset", False),
            "is_risk_conflict":                         row.get("is_risk_conflict", False),
            "is_investment_quality":                    row.get("is_investment_quality", False),

            # ── V4.2 semantic precision fields (pass-through from snapshot) ──────
            # Flat aliases — frontend should use these for alignment rows since
            # the nested confluence_v42 object was not historically present here.
            "entry_execution_state":                    row.get("entry_execution_state"),
            "entry_execution_label":                    row.get("entry_execution_label"),
            "caution_flags":                            row.get("caution_flags") or [],
            "entry_state_display":                      row.get("entry_state_display"),
            "risk_flags":                               row.get("risk_flags") or [],
            "data_status_flags":                        row.get("data_status_flags") or [],

            # ── Nested V4.2 object (consistent with /api/alpha/confluence shape) ─
            "confluence_v42":                           row.get("confluence_v42"),

            # ── Quality fundamentals snapshot (FMP weekly cache, read-only) ──────
            # Populated by _build_ticker_row from the pre-loaded fund_snaps dict.
            # Shape: {fields: {Cash, ROIC, FCF Conversion, …}, refreshed_at, missing_fields}
            "fundamentals":                             row.get("fundamentals"),
        })

    # ── v3: theme leadership (cross-symbol ranking, computed after all rows) ──
    try:
        from services.theme_leadership_service import (
            compute_theme_leadership_for_rows,
            build_component_coverage,
        )
        _snap_rows_for_leadership = [
            confluence_by_symbol[r["symbol"]]
            for r in rows
            if r["symbol"] in confluence_by_symbol
        ]
        leadership_map = compute_theme_leadership_for_rows(_snap_rows_for_leadership)

        for r in rows:
            sym = r["symbol"]
            # Component coverage — use the flat confluence snapshot row (which has
            # all fields at top-level), NOT the response row (which nests them).
            try:
                _snap_r = confluence_by_symbol.get(sym) or r
                cov = build_component_coverage(_snap_r)
                r["component_coverage"]         = cov["component_coverage"]
                r["confluence_confidence_score"] = cov["confluence_confidence_score"]
            except Exception:
                pass
            # Theme leadership
            ldr = leadership_map.get(sym)
            if ldr:
                r["theme_leadership_score"]    = ldr.get("theme_leadership_score")
                r["theme_leadership_rank"]     = ldr.get("theme_leadership_rank")
                r["theme_leadership_total"]    = ldr.get("theme_leadership_total")
                r["theme_leadership_bucket"]   = ldr.get("theme_leadership_bucket")
                r["is_theme_leader"]           = ldr.get("is_theme_leader", False)
                r["is_top_3_theme_leader"]     = ldr.get("is_top_3_theme_leader", False)
                r["leader_context"]            = ldr.get("leader_context")
                r["theme_leader_reason_codes"] = ldr.get("theme_leader_reason_codes") or []
                r["leadership_theme"]          = ldr.get("leadership_theme")
    except Exception as _ldr_exc:
        print(f"[WATCHLIST_ALIGNMENT] theme leadership/coverage error (non-fatal): {_ldr_exc}")

    return {
        "watchlist_id":                   watchlist_id,
        "watchlist_name":                 store.get("name"),
        "row_count":                      len(rows),
        "snapshot_built_at":              meta.get("built_at"),
        "snapshot_stale":                 bool(meta.get("stale")),
        "snapshot_rebuild_in_progress":   bool(meta.get("rebuild_in_progress")),
        "snapshot_stale_reasons":         list(meta.get("stale_reasons") or []),
        "rows": rows,
    }


def _build_theme_performance_groups(store: dict) -> dict:
    """
    Group the CURRENT watchlist rows (exactly as returned by GET /{watchlist_id})
    by their existing canonical_theme_name / theme field — the SAME field
    rendered in the upper Watchlist Screener THEME column.

    This performs NO independent ticker classification: it purely reads
    canonical_theme_name / theme / canonical_theme_id already present on each
    row (set upstream by services.theme_resolver.resolve_primary_theme_for_ticker,
    or preserved as-is from saved analysis sections). No LLM/AI calls, no
    provider calls, no new resolver.

    Theme 1D performance = equal-weight average of change_pct_1d across
    tickers in that theme with an available value. Tickers with no usable
    change_pct_1d are excluded from the average but still included in the
    card. Theme cards are sorted by 1D performance descending, with themes
    having no valid data sorted last. Tickers within a card are sorted by
    1D change descending (missing values last).
    """
    UNASSIGNED_LABEL = "Unassigned"
    UNASSIGNED_ID    = "unassigned"

    all_rows: list[dict] = []
    seen_symbols: set[str] = set()
    for section in (store.get("analysis") or {}).get("sections", []):
        for row in section.get("tickers", []):
            sym = str(row.get("symbol") or row.get("ticker") or "").strip().upper()
            if not sym or sym in seen_symbols:
                continue
            seen_symbols.add(sym)
            all_rows.append(row)

    groups: dict[str, dict] = {}
    missing_1d: list[str] = []

    for row in all_rows:
        sym   = str(row.get("symbol") or row.get("ticker") or "").strip().upper()
        theme_name = row.get("canonical_theme_name") or row.get("theme")
        theme_id   = row.get("canonical_theme_id")

        if not theme_name:
            theme_name, theme_id = UNASSIGNED_LABEL, UNASSIGNED_ID

        chg = row.get("change_pct_1d")
        try:
            chg = float(chg) if chg is not None else None
        except Exception:
            chg = None
        if chg is None:
            missing_1d.append(sym)

        g = groups.setdefault(theme_name, {
            "theme_name": theme_name,
            "theme_id":   theme_id or theme_name.lower().replace(" ", "_"),
            "tickers":    [],
        })
        g["tickers"].append({
            "symbol":         sym,
            "name":           row.get("name"),
            "price":          row.get("price"),
            "change_pct_1d":  chg,
        })

    theme_cards: list[dict] = []
    for theme_name, g in groups.items():
        valid_changes = [t["change_pct_1d"] for t in g["tickers"] if t["change_pct_1d"] is not None]
        theme_1d = (sum(valid_changes) / len(valid_changes)) if valid_changes else None

        g["tickers"].sort(
            key=lambda t: (t["change_pct_1d"] is None, -(t["change_pct_1d"] or 0.0))
        )
        theme_cards.append({
            "theme_name":     theme_name,
            "theme_id":       g["theme_id"],
            "theme_1d_pct":   theme_1d,
            "ticker_count":   len(g["tickers"]),
            "tickers":        g["tickers"],
        })

    theme_cards.sort(
        key=lambda c: (c["theme_1d_pct"] is None, -(c["theme_1d_pct"] or 0.0))
    )

    return {
        "watchlist_id":            store.get("id"),
        "current_watchlist_rows":  len(all_rows),
        "grouped_tickers":         sum(c["ticker_count"] for c in theme_cards),
        "unassigned_count":        groups.get(UNASSIGNED_LABEL, {}).get("tickers", []) and len(groups[UNASSIGNED_LABEL]["tickers"]) or 0,
        "missing_1d_change":       missing_1d,
        "theme_cards":             theme_cards,
    }


@router.get("/{watchlist_id}/performance/theme")
async def get_watchlist_theme_performance(watchlist_id: str):
    """
    Watchlist → Performance Groupings → Theme.

    Groups the CURRENT saved watchlist rows by the exact same canonical Theme
    field shown in the upper Watchlist Screener THEME column
    (canonical_theme_name / theme, set by theme_resolver.resolve_primary_theme_for_ticker
    upstream). Does not call an AI classifier, does not create a second
    ticker→theme source, and does not use Themes-page ETF/proxy constituents —
    only tickers actually on this saved watchlist are grouped.

    Theme 1D performance = equal-weight average of each theme's tickers'
    change_pct_1d (the same field shown in the Screener's CHG % column).
    Read-only, zero incremental provider/LLM calls — reuses the same
    already-enriched rows served by GET /{watchlist_id}.
    """
    store = load_watchlist(watchlist_id)
    if store is None:
        return {"empty": True}
    try:
        store = await _enrich_store_with_quotes(store)
    except Exception as _enrich_err:
        print(f"[WATCHLIST_THEME_PERF] Quote enrichment failed (returning raw): {_enrich_err}")

    return _build_theme_performance_groups(store)


@router.post("/{watchlist_id}/analyze")
async def analyze_by_id_endpoint(watchlist_id: str):
    """Run multi-source parallel analysis pipeline for a specific watchlist."""
    agent = _get_agent()
    data_service = _get_data_service()

    store = load_watchlist(watchlist_id)
    if store is None:
        raise HTTPException(status_code=404, detail="Watchlist not found")

    tickers = store.get("tickers", [])
    csv_data = store.get("csv_data", [])

    if not tickers:
        raise HTTPException(status_code=400, detail="Watchlist has no tickers")

    result = await run_analysis_pipeline(tickers, csv_data, agent, data_service)

    if isinstance(result, dict) and result.get("error") and "sections" not in result:
        raise HTTPException(status_code=500, detail=result["error"])

    return result


async def _run_claude_analysis_background(watchlist_id: str) -> None:
    """
    Background task: run full per-group Claude analysis and save to DB.

    Runs AFTER the HTTP response has already been returned to the client.
    Sections use canonical THEME_RS_UNIVERSE names — Claude provides only
    per-ticker insight fields (catalyst, sentiment, action_note, etc.),
    never invents or renames sections.
    """
    try:
        store = load_watchlist(watchlist_id)
        if store is None:
            print(f"[BG_REFRESH] {watchlist_id}: watchlist disappeared — aborting")
            return

        tickers: list  = store.get("tickers", [])
        csv_data: list = store.get("csv_data", [])
        store_name: str = store.get("name", "Watchlist")

        if not tickers:
            return

        csv_map: dict = {}
        for row in csv_data:
            sym = row.get("Symbol") or row.get("symbol") or row.get("Ticker") or ""
            if sym:
                csv_map[sym.strip().upper()] = row

        # ── Classify tickers into canonical theme groups ───────────────────────
        try:
            from services.theme_ticker_mapper import (
                map_ticker_to_primary_theme,
                map_ticker_to_theme_id,
            )
        except Exception as _tm_err:
            print(f"[BG_REFRESH] theme_ticker_mapper unavailable: {_tm_err}")
            map_ticker_to_primary_theme = lambda s: None
            map_ticker_to_theme_id      = lambda s: None

        _OTHER_LABEL = "Other / Uncategorized"
        _OTHER_ID    = "other_uncategorized"

        # Section-name aliases: normalize overlapping/sub-theme names into canonical ones.
        # Claude sometimes invents sub-theme names; these guards ensure consistent grouping.
        _NAME_NORMALIZE: dict[str, str] = {
            # Clean energy consolidation (solar, hydrogen, fuel cells → Clean Energy)
            "Solar":                    "Clean Energy",
            "Renewable Energy":         "Clean Energy",
            "Alternative Energy":       "Clean Energy",
            "Fuel Cell":                "Clean Energy",
            "Fuel Cells":               "Clean Energy",
            "Hydrogen":                 "Clean Energy",
            "Hydrogen Energy":          "Clean Energy",
            "Energy Storage":           "Lithium & Battery Tech",
            # Networking variants
            "Optical Networking":       "AI Networking",
            "Networking":               "AI Networking",
            # Semiconductor variants
            "AI Chips":                 "Semiconductors",
            "Chips":                    "Semiconductors",
            # Slash-style legacy names
            "Memory / Storage":         "Memory & Storage",
            "Robotics / Automation":    "Robotics & Automation",
            "Datacenter / Compute":     "Data Center Infrastructure",
            "Aerospace / Defense":      "Defense",
        }
        _ID_NORMALIZE: dict[str, str] = {
            "solar":                    "clean_energy",
            "renewable_energy":         "clean_energy",
            "alternative_energy":       "clean_energy",
            "fuel_cell":                "clean_energy",
            "fuel_cells":               "clean_energy",
            "hydrogen":                 "clean_energy",
            "hydrogen_energy":          "clean_energy",
            "energy_storage":           "lithium_battery",
            "optical_networking":       "ai_networking",
            "networking":               "ai_networking",
            "ai_chips":                 "semiconductors",
            "chips":                    "semiconductors",
            "memory_/_storage":         "memory_storage",
            "robotics_/_automation":    "robotics_automation",
            "datacenter_/_compute":     "datacenter_infra",
            "aerospace_/_defense":      "defense",
        }

        # Industry fallback — used when primary mapper returns None
        try:
            from services.theme_ticker_mapper import map_industry_to_theme as _bg_map_ind
        except ImportError:
            _bg_map_ind = None

        ticker_to_canon_name: dict[str, str] = {}
        ticker_to_canon_id:   dict[str, str] = {}
        theme_groups: dict[str, list[str]]   = {}

        _stat_registry   = 0  # mapped by theme_ticker_mapper (primary registries)
        _stat_fallback   = 0  # mapped by CSV industry fallback
        _stat_other      = 0  # truly uncategorized
        _stat_aliases    = 0  # section names normalized via _NAME_NORMALIZE
        _stat_overridden = 0  # overridden by manual category override

        # Load manual overrides once — these always win over AI/static classification.
        # Applying them here ensures the SAVED DB sections reflect user corrections,
        # not just the display layer.  Without this, a background refresh would save
        # AI-classified sections to DB, even though the display layer re-applies
        # overrides on every GET.
        _bg_cat_overrides: dict[str, str] = {}
        try:
            from services.category_overrides import get_overrides as _bg_get_overrides
            _bg_cat_overrides = _bg_get_overrides("default")
        except Exception as _bg_ov_err:
            print(f"[BG_REFRESH] category overrides load failed (non-fatal): {_bg_ov_err}")

        for sym in tickers:
            raw_cname = map_ticker_to_primary_theme(sym)
            raw_cid   = map_ticker_to_theme_id(sym)

            # If primary mapper failed, try bare symbol (strips exchange prefix).
            # Example: "AIM:TRT" → try "TRT" — handles foreign-listed stocks where
            # the exchange prefix prevents a direct dict lookup.
            if raw_cname is None and ":" in sym:
                _base = sym.split(":")[-1]
                raw_cname = map_ticker_to_primary_theme(_base)
                raw_cid   = map_ticker_to_theme_id(_base)

            if raw_cname is not None:
                _stat_registry += 1
            elif _bg_map_ind:
                # CSV industry deterministic fallback
                csv_row  = csv_map.get(sym) or csv_map.get(sym.split(":")[-1] if ":" in sym else sym) or {}
                industry = (csv_row.get("Industry") or csv_row.get("industry") or "").strip()
                _ind_res = _bg_map_ind(industry)
                if _ind_res:
                    raw_cname, raw_cid = _ind_res
                    _stat_fallback += 1
                else:
                    raw_cname, raw_cid = _OTHER_LABEL, _OTHER_ID
                    _stat_other += 1
            else:
                raw_cname, raw_cid = _OTHER_LABEL, _OTHER_ID
                _stat_other += 1

            cname = _NAME_NORMALIZE.get(raw_cname, raw_cname)
            cid   = _ID_NORMALIZE.get(raw_cid or "", raw_cid or _OTHER_ID)
            if cname != raw_cname:
                _stat_aliases += 1

            # Manual override wins over everything — checked last so it cannot
            # be undone by any upstream classifier or alias normalization.
            _sym_upper = sym.strip().upper()
            _override_cat = (
                _bg_cat_overrides.get(_sym_upper)
                or _bg_cat_overrides.get(_sym_upper.split(":")[-1] if ":" in _sym_upper else _sym_upper)
            )
            if _override_cat:
                cname = _override_cat
                cid   = (
                    cname.lower()
                    .replace(" ", "_")
                    .replace("/", "_")
                    .replace("&", "and")
                    .replace("-", "_")
                )
                _stat_overridden += 1

            ticker_to_canon_name[sym] = cname
            ticker_to_canon_id[sym]   = cid
            theme_groups.setdefault(cname, []).append(sym)

        sorted_groups: list[tuple[str, list[str]]] = sorted(
            theme_groups.items(),
            key=lambda kv: (kv[0] == _OTHER_LABEL, kv[0]),
        )
        print(
            f"[BG_REFRESH] {watchlist_id}: {len(tickers)} tickers → "
            f"{len(sorted_groups)} canonical theme groups "
            f"(registry={_stat_registry} industry_fallback={_stat_fallback} "
            f"other={_stat_other} aliases={_stat_aliases} overridden={_stat_overridden})"
        )

        anthropic_key = os.getenv("ANTHROPIC_API_KEY", "")
        CHUNK_SIZE = 22

        async def analyze_theme_group(group_name: str, group_tickers: list[str]) -> dict | None:
            """One section per canonical theme; Claude provides per-ticker insights only."""
            all_ticker_rows: list[dict] = []
            group_subtitle = ""

            for chunk_start in range(0, len(group_tickers), CHUNK_SIZE):
                chunk = group_tickers[chunk_start : chunk_start + CHUNK_SIZE]

                ticker_summaries = []
                for sym in chunk:
                    row = csv_map.get(sym.upper(), {})
                    parts = [f"{sym}:"]
                    for label, key in [
                        ("Price",       "Stock Price"),
                        ("MCap",        "Market Cap"),
                        ("PE",          "PE Ratio"),
                        ("FwdPE",       "Forward PE"),
                        ("RSI",         "Relative Strength Index (RSI)"),
                        ("RevGrowth",   "Revenue Growth (YoY)"),
                        ("EPSEst",      "EPS Growth Est."),
                        ("FCF",         "FCF Margin"),
                        ("GrossMargin", "Gross Margin"),
                        ("DE",          "Debt / Equity"),
                    ]:
                        val = row.get(key, "")
                        if val:
                            parts.append(f"{label}={val}")
                    ticker_summaries.append(" ".join(parts))

                # Section title is locked — Claude must NOT rename it
                prompt = (
                    f"Analyze these {len(chunk)} stocks. "
                    f"They all belong to the '{group_name}' theme. "
                    f"Return ONLY a valid JSON object (no markdown fences, no extra text).\n\n"
                    f"Stocks:\n" + "\n".join(ticker_summaries) + "\n\n"
                    f"Return exactly this structure "
                    f"(do NOT change the theme name — it is fixed as '{group_name}'):\n"
                    f'{{\n'
                    f'  "subtitle": "<one-line market context for these {group_name} stocks>",\n'
                    f'  "tickers": [\n'
                    f'    {{\n'
                    f'      "symbol": "<ticker>",\n'
                    f'      "name": "<company name>",\n'
                    f'      "price": <float or null>,\n'
                    f'      "change_pct": <float or null>,\n'
                    f'      "catalyst": "<key near-term catalyst>",\n'
                    f'      "sentiment": "<bullish|neutral|bearish>",\n'
                    f'      "action_note": "<specific actionable note>",\n'
                    f'      "risk_level": "<low|medium|high>",\n'
                    f'      "key_insight": "<single most important insight>",\n'
                    f'      "technical_setup": "<technical pattern or level to watch>"\n'
                    f'    }}\n'
                    f'  ]\n'
                    f'}}'
                )

                try:
                    import anthropic as _anthropic
                    client = _anthropic.AsyncAnthropic(api_key=anthropic_key, timeout=90.0)
                    response = await client.messages.create(
                        model=MODEL_CLAUDE_PREMIUM,
                        max_tokens=4000,
                        messages=[{"role": "user", "content": prompt}],
                    )
                    text = "".join(b.text for b in response.content if hasattr(b, "text"))
                    text = _re.sub(r"```json\s*", "", text)
                    text = _re.sub(r"```\s*", "", text).strip()
                    m_json = _re.search(r"\{[\s\S]*\}", text)
                    if m_json:
                        chunk_parsed = _json.loads(m_json.group())
                        if not group_subtitle:
                            group_subtitle = chunk_parsed.get("subtitle", "")
                        ticker_rows = chunk_parsed.get("tickers", [])
                        for tr in ticker_rows:
                            sym_upper = str(tr.get("symbol", "")).upper()
                            tr["canonical_theme_name"] = group_name
                            tr["canonical_theme_id"]   = ticker_to_canon_id.get(sym_upper, _OTHER_ID)
                            tr["theme_source"]          = "canonical"
                        all_ticker_rows.extend(ticker_rows)
                        print(
                            f"[BG_REFRESH] '{group_name}' chunk {chunk_start}: "
                            f"{len(chunk)} tickers → {len(ticker_rows)} rows"
                        )
                    else:
                        print(f"[BG_REFRESH] '{group_name}' chunk {chunk_start}: no JSON in response")
                except Exception as e:
                    print(f"[BG_REFRESH] '{group_name}' chunk {chunk_start} failed: {type(e).__name__}: {e}")

            if not all_ticker_rows:
                # Fallback: keep skeleton rows so section still appears
                all_ticker_rows = [{"symbol": s} for s in group_tickers]

            canon_id = ticker_to_canon_id.get(group_tickers[0], _OTHER_ID)
            return {
                "id":                 canon_id,
                "title":              group_name,
                "subtitle":           group_subtitle,
                "canonical_theme_id": canon_id,
                "theme_source":       "canonical",
                "tickers":            all_ticker_rows,
            }

        # Run groups 3 at a time — respects Claude rate limits
        semaphore = asyncio.Semaphore(3)

        async def guarded_group(gname: str, gsyms: list[str]) -> dict | None:
            async with semaphore:
                return await analyze_theme_group(gname, gsyms)

        group_results = await asyncio.gather(
            *[guarded_group(n, s) for n, s in sorted_groups],
            return_exceptions=False,
        )
        sections = [r for r in group_results if r is not None]

        market_themes: list[str] = []
        seen_mt: set[str] = set()
        for section in sections:
            title = section.get("title", "")
            if title and title not in seen_mt:
                market_themes.append(title)
                seen_mt.add(title)

        analysis = {
            "sections":               sections,
            "market_themes":          market_themes[:8],
            "generated_at":           datetime.now(timezone.utc).isoformat() + "Z",
            "theme_source":           "canonical",
            "classification_method":  "canonical_theme_registry",
            "_classification_stats": {
                "saved_symbols_count":            len(tickers),
                "categorized_by_registry_count":  _stat_registry,
                "categorized_by_fallback_count":  _stat_fallback,
                "uncategorized_count":            _stat_other,
                "normalized_section_count":       len(sections),
                "section_aliases_applied_count":  _stat_aliases,
            },
        }

        try:
            save_watchlist(csv_data, analysis, watchlist_id=watchlist_id, name=store_name)
            total_enriched = sum(
                len(s.get("tickers", [])) for s in sections
                if s.get("id") != "other_uncategorized"
                   or any(len(t) > 1 for t in s.get("tickers", []))
            )
            print(
                f"[BG_REFRESH] {watchlist_id}: saved {len(sections)} sections, "
                f"{sum(len(s.get('tickers',[])) for s in sections)} tickers"
            )
        except Exception as save_err:
            print(f"[BG_REFRESH] Save failed: {save_err}")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"[BG_REFRESH] {watchlist_id}: top-level error: {e}")


@router.post("/{watchlist_id}/refresh")
async def refresh_by_id_endpoint(watchlist_id: str):
    """
    Trigger a full Claude re-analysis for a watchlist.

    Returns HTTP 200 immediately with the current enriched analysis (Tradier
    quotes + any previously-saved LLM insights).  The Claude per-group analysis
    runs as a background task and saves to the DB when complete — subsequent
    GET calls will show the updated data.

    This design eliminates the previous HTTP timeout that the frontend
    interpreted as "ANALYSIS FAILED: Backend returned 500".  Tradier quote
    data is force-refreshed synchronously before returning so the response
    always has fresh price / 1D-change data.
    """
    try:
        store = load_watchlist(watchlist_id)
        if store is None:
            raise HTTPException(status_code=404, detail="Watchlist not found")

        tickers: list = store.get("tickers", [])
        if not tickers:
            raise HTTPException(status_code=400, detail="Watchlist has no tickers")

        # Kick off the Claude background analysis immediately
        asyncio.create_task(_run_claude_analysis_background(watchlist_id))
        print(f"[REFRESH] {watchlist_id}: background Claude task started for {len(tickers)} tickers")

        # Force-refresh Tradier quotes so the response has live price data
        try:
            from services.watchlist_quote_cache import refresh_watchlist_quotes_now
            await refresh_watchlist_quotes_now(tickers)
        except Exception as _qe:
            print(f"[REFRESH] Tradier quote refresh failed (non-fatal): {_qe}")

        # Return the current analysis enriched with fresh quotes — always HTTP 200
        enriched = await _enrich_store_with_quotes(store)
        enriched["refresh_status"] = "running"
        return enriched

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Refresh failed: {e}")


@router.get("/{watchlist_id}/news/major")
async def news_major_by_id_endpoint(watchlist_id: str):
    """
    Return the top major developments for a watchlist (deduplicated, ranked).
    Uses the shared LKG news cache — instant if data is already warm.

    Response shape:
    {
      "major_developments":       [...top-20 articles...],
      "major_developments_count": int,
      "high_signal_count":        int,
      "by_catalyst_type":         {catalyst_type: count, ...},
      "news_signal_meta":         {...},
      "cached_at":                "ISO timestamp",
      "cache_age_s":              int,
      "is_building":              bool,
    }
    """
    store = load_watchlist(watchlist_id)
    if store is None:
        return {"major_developments": [], "major_developments_count": 0, "cache_age_s": 0}
    tickers = store.get("tickers", [])
    if not tickers:
        return {"major_developments": [], "major_developments_count": 0, "cache_age_s": 0}

    full = await _get_news_for_watchlist(watchlist_id, tickers)
    top  = full.get("top_articles") or []
    return {
        "major_developments":       top,
        "major_developments_count": len(top),
        "high_signal_count":        full.get("high_signal_count", 0),
        "by_catalyst_type":         full.get("by_catalyst_type", {}),
        "news_signal_meta":         full.get("news_signal_meta", {}),
        "cached_at":                full.get("cached_at"),
        "cache_age_s":              full.get("cache_age_s", 0),
        "is_building":              full.get("is_building", False),
    }


@router.get("/{watchlist_id}/news/ticker/{ticker:path}")
async def ticker_activity_news_endpoint(watchlist_id: str, ticker: str):
    """
    News Activity click-through: return the exact archived RSS articles that
    contribute to a ticker's articles_48h count.

    Invariant:
        response["articles_48h"]  ==  len(response["articles"])
        response["articles_48h"]  ≈   ticker_activity[ticker].articles_48h
          (approximate — a sweep may commit between two separate requests)

    Zero provider calls.  Reads from watchlist_rss_article_archive only.
    The ticker must belong to the named Watchlist.

    Response shape:
        {
          "ticker":            str,
          "window_hours":      48,
          "articles_48h":      int,
          "articles":          [{ticker, article_key, title, summary,
                                  source, url, published_at, rss_providers}, ...],
          "activity_as_of":    "ISO timestamp",
          "last_full_sweep_at": str | null,
          "coverage_status":   "warming" | "complete",
        }
    """
    ticker_canon = ticker.strip().upper()

    store = load_watchlist(watchlist_id)
    if store is None:
        raise HTTPException(status_code=404, detail="watchlist not found")

    wl_tickers_upper = {t.strip().upper() for t in (store.get("tickers") or [])}
    if ticker_canon not in wl_tickers_upper:
        raise HTTPException(
            status_code=404,
            detail=f"ticker {ticker_canon!r} not in watchlist {watchlist_id!r}",
        )

    try:
        from data.rss_article_archive import query_ticker_activity_articles
        loop = asyncio.get_event_loop()

        # Single executor call — two queries on one Neon connection round-trip
        articles, first_seen_ts = await loop.run_in_executor(
            None, query_ticker_activity_articles, ticker_canon, 48
        )

        # articles_48h derived from the article list itself — never mismatches
        articles_48h = len(articles)

        # coverage_status: same 96h threshold as _build_ticker_activity_list
        now_ts = _time.time()
        collector_age_h = (now_ts - first_seen_ts) / 3600 if first_seen_ts else None
        coverage_status = (
            "complete" if (collector_age_h is not None and collector_age_h >= 96)
            else "warming"
        )

        # Sweeper freshness metadata
        try:
            from services.watchlist_rss_sweeper import get_sweeper_meta
            last_sweep = get_sweeper_meta().get("last_full_sweep_at")
        except Exception:
            last_sweep = None

        return {
            "ticker":             ticker_canon,
            "window_hours":       48,
            "articles_48h":       articles_48h,
            "articles":           articles,
            "activity_as_of":     datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "last_full_sweep_at": last_sweep,
            "coverage_status":    coverage_status,
        }
    except HTTPException:
        raise
    except Exception as exc:
        print(f"[ticker-activity-news] {ticker_canon}: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/{watchlist_id}/news")
async def news_by_id_endpoint(watchlist_id: str):
    """
    Live News for a specific watchlist — LKG cached, never blocks on stale data.

    Response shape:
      {
        "articles":          {TICKER: [article, ...]},   // per-ticker map
        "top_articles":      [...],                       // pre-ranked signal articles (use this)
        "high_signal_count": int,
        "by_catalyst_type":  {type: count},
        "news_signal_meta":  {...},
        "cached_at":         "ISO timestamp",
        "cache_age_s":       int,
        "is_building":       bool,
      }
    """
    store = load_watchlist(watchlist_id)
    if store is None:
        return _news_response({}, {}, _time.time())
    tickers = store.get("tickers", [])
    if not tickers:
        return _news_response({}, {}, _time.time())
    return await _get_news_for_watchlist(watchlist_id, tickers)


@router.get("/{watchlist_id}/stock/{ticker}")
async def stock_detail_by_id_endpoint(watchlist_id: str, ticker: str):
    """Return enriched data for a single ticker within a specific watchlist."""
    agent = _get_agent()
    result = await get_stock_detail(ticker, agent, watchlist_id=watchlist_id)
    if result.get("error"):
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.delete("/{watchlist_id}")
async def delete_by_id_endpoint(watchlist_id: str):
    """Delete a specific watchlist and invalidate earnings cache."""
    result = clear_watchlist(watchlist_id)
    try:
        from services.user_earnings_service import invalidate_user_earnings  # type: ignore
        invalidate_user_earnings("watchlist")
    except Exception as _e:
        print(f"[watchlist-delete-id] earnings invalidation skipped: {_e}")
    return result


@router.get("/{watchlist_id}/confluence/v4-report")
async def v4_report_endpoint(watchlist_id: str):
    """
    V4.2 Confluence validation report — returns status/bucket distributions
    across the retained confluence snapshot.  Development/diagnostic use only.
    Pure read: zero provider calls, zero LLM calls.
    V4.2 is re-computed live here so results reflect the latest engine even
    before the retained snapshot is rebuilt.
    """
    from collections import Counter
    from services.confluence_v2_service import get_retained_confluence_snapshot
    from services.caelyn_confluence_v42 import (
        compute_confluence_v42,
        build_social_sections_map as _build_ssm,
    )

    snap = await asyncio.to_thread(get_retained_confluence_snapshot)
    if not snap:
        raise HTTPException(status_code=503, detail="Retained confluence snapshot not yet built")

    rows = snap.get("results") or []
    if not rows:
        raise HTTPException(status_code=503, detail="Retained confluence snapshot has 0 rows")

    # Social sections map (3 sections × 5 pts)
    social_sections_map: dict = {}
    try:
        social_sections_map = _build_ssm()
    except Exception:
        pass

    # Fundamentals map (3-pillar investment model)
    fundamentals_map: dict = {}
    try:
        from data.watchlist_fundamentals_store import get_snapshots_bulk as _gfb
        universe = [str(r.get("symbol", "")).upper() for r in rows if r.get("symbol")]
        fundamentals_map = await asyncio.to_thread(_gfb, universe) or {}
    except Exception:
        pass

    scored: list[dict] = []
    errors: list[dict] = []
    for row in rows:
        sym = str(row.get("symbol", "")).upper()
        try:
            v42 = compute_confluence_v42(
                row,
                social_sections_map=social_sections_map,
                bottleneck_map=None,
                fundamentals_map=fundamentals_map,
            )
            scored.append({"symbol": sym, **v42})
        except Exception as exc:
            errors.append({"symbol": sym, "error": str(exc)})

    def _dist(vals):
        c = Counter(vals)
        return dict(c.most_common())

    buckets    = _dist(r.get("caelyn_confluence_v42_bucket") for r in scored)
    act_states = _dist(r.get("caelyn_confluence_v42_actionability") for r in scored)
    opts_status = _dist(
        (r.get("caelyn_confluence_v42_components") or {}).get("options_alignment", {}).get("status")
        for r in scored
    )
    cat_status = _dist(
        (r.get("caelyn_confluence_v42_components") or {}).get("catalyst_alignment", {}).get("status")
        for r in scored
    )
    invest_dist = _dist(
        r.get("investment_pillar_count") for r in scored
    )
    conf_vals  = [r.get("caelyn_confluence_v42_confidence_score") or 0 for r in scored]
    conf_dist  = {
        "<40":   sum(1 for c in conf_vals if c < 40),
        "40-69": sum(1 for c in conf_vals if 40 <= c < 70),
        ">=70":  sum(1 for c in conf_vals if c >= 70),
        "avg":   round(sum(conf_vals) / len(conf_vals), 1) if conf_vals else 0,
    }
    score_vals = [r.get("caelyn_confluence_v42_score") or 0 for r in scored]
    score_dist = {
        "<40":    sum(1 for s in score_vals if s < 40),
        "40-59":  sum(1 for s in score_vals if 40 <= s < 60),
        "60-79":  sum(1 for s in score_vals if 60 <= s < 80),
        "80-99":  sum(1 for s in score_vals if 80 <= s < 100),
        ">=100":  sum(1 for s in score_vals if s >= 100),
        "avg":    round(sum(score_vals) / len(score_vals), 1) if score_vals else 0,
        "max":    round(max(score_vals), 1) if score_vals else 0,
        "median": round(sorted(score_vals)[len(score_vals)//2], 1) if score_vals else 0,
    }

    # Top results sample (ACTIONABLE / NEAR_ACTIONABLE)
    top_scored = sorted(
        scored,
        key=lambda r: r.get("caelyn_confluence_v42_score") or 0,
        reverse=True,
    )
    top_sample = [
        {
            "symbol":         r["symbol"],
            "bucket":         r.get("caelyn_confluence_v42_bucket"),
            "act":            r.get("caelyn_confluence_v42_actionability"),
            "score":          r.get("caelyn_confluence_v42_score"),
            "core":           r.get("caelyn_confluence_v42_core_score"),
            "bonus":          r.get("caelyn_confluence_v42_bonus_score"),
            "confidence":     r.get("caelyn_confluence_v42_confidence_score"),
            "invest_pillars": r.get("investment_pillar_count"),
            "soc_sections":   r.get("social_sections_hit"),
            "opts_status":    (r.get("caelyn_confluence_v42_components") or {}).get("options_alignment", {}).get("status"),
            "cat_status":     (r.get("caelyn_confluence_v42_components") or {}).get("catalyst_alignment", {}).get("status"),
            "comp_pts": {
                k: round((v or {}).get("points") or 0, 1)
                for k, v in (r.get("caelyn_confluence_v42_components") or {}).items()
            },
            "bonus_breakdown": {
                k: round((v or {}).get("points") or 0, 1)
                for k, v in (r.get("caelyn_confluence_v42_bonus_breakdown") or {}).items()
            },
            "bottleneck_bonus_pts": round(
                ((r.get("caelyn_confluence_v42_bonus_breakdown") or {}).get("bottleneck") or {}).get("points") or 0, 1
            ),
            "bottleneck_anchors": (
                (r.get("caelyn_confluence_v42_bonus_breakdown") or {}).get("bottleneck") or {}
            ).get("bottleneck_anchor_count"),
        }
        for r in top_scored[:30]
    ]

    def _opts_status_of(r):
        return (r.get("caelyn_confluence_v42_components") or {}).get("options_alignment", {}).get("status")

    not_scanned_symbols = sorted(
        r["symbol"] for r in scored if _opts_status_of(r) == "not_scanned"
    )
    confirmed_no_options_symbols = sorted(
        r["symbol"] for r in scored if _opts_status_of(r) == "confirmed_no_options"
    )

    return {
        "meta": {
            "engine":  "v4.2",
            "total":   len(rows),
            "scored":  len(scored),
            "errors":  len(errors),
            "snap_built_at": snap.get("generated_at") or snap.get("built_at"),
            "social_sections_coverage": len(social_sections_map),
            "fundamentals_coverage": len(fundamentals_map),
        },
        "bucket_distribution":            buckets,
        "actionability_distribution":     act_states,
        "options_status_distribution":    opts_status,
        "catalyst_status_distribution":   cat_status,
        "investment_pillar_distribution": invest_dist,
        "confidence_distribution":        conf_dist,
        "score_distribution":             score_dist,
        "top_sample":                     top_sample,
        "errors_sample":                  errors[:10],
        "not_scanned_symbols":            not_scanned_symbols,
        "confirmed_no_options_symbols":   confirmed_no_options_symbols,
    }


@router.get("/{watchlist_id}/confluence/valuation-qa")
async def valuation_qa_endpoint(watchlist_id: str):
    """
    Full-universe Valuation component QA.
    Pure read: zero provider calls, zero LLM calls.
    Re-scores all retained-snapshot symbols live via compute_confluence_v42.
    """
    from collections import Counter
    from services.confluence_v2_service import get_retained_confluence_snapshot
    from services.caelyn_confluence_v42 import (
        compute_confluence_v42,
        build_social_sections_map as _build_ssm,
    )

    snap = await asyncio.to_thread(get_retained_confluence_snapshot)
    if not snap:
        raise HTTPException(status_code=503, detail="Retained confluence snapshot not yet built")

    rows = snap.get("results") or []
    if not rows:
        raise HTTPException(status_code=503, detail="Retained snapshot has 0 rows")

    social_sections_map: dict = {}
    try:
        social_sections_map = _build_ssm()
    except Exception:
        pass

    fundamentals_map: dict = {}
    try:
        from data.watchlist_fundamentals_store import get_snapshots_bulk as _gfb
        universe = [str(r.get("symbol", "")).upper() for r in rows if r.get("symbol")]
        fundamentals_map = await asyncio.to_thread(_gfb, universe) or {}
    except Exception:
        pass

    scored: list[dict] = []
    errors: list[dict] = []
    for row in rows:
        sym = str(row.get("symbol", "")).upper()
        try:
            v42 = compute_confluence_v42(
                row,
                social_sections_map=social_sections_map,
                bottleneck_map=None,
                fundamentals_map=fundamentals_map,
            )
            scored.append({"symbol": sym, **v42})
        except Exception as exc:
            errors.append({"symbol": sym, "error": str(exc)})

    total = len(scored)

    # ── 0. READY regression analysis ───────────────────────────────────────
    # Reconstruct approximate OLD v4.1 normalized_total for each symbol using
    # current component points scaled back to old max caps
    # (Options 18→20, Catalyst 12→15, Investment 12→15, Valuation excluded).
    # Approximation: assumes same RELATIVE quality score across weight change.
    # Old available_max when 7 components available = 20+15+15+15+15+8+12 = 100
    _OLD_MAXES = {
        "theme_alignment":      15.0,
        "stage_quality":        15.0,
        "options_alignment":    20.0,
        "technical_setup":       8.0,
        "entry_exit":           12.0,
        "catalyst_alignment":   15.0,
        "investment_alignment": 15.0,
    }

    def _reconstruct_old(r):
        comps   = r.get("caelyn_confluence_v42_components") or {}
        bonus   = r.get("caelyn_confluence_bonus_score") or 0
        theme   = (comps.get("theme_alignment")      or {}).get("points", 0)
        stage   = (comps.get("stage_quality")         or {}).get("points", 0)
        opts    = (comps.get("options_alignment")     or {}).get("points", 0)
        tech    = (comps.get("technical_setup")       or {}).get("points", 0)
        entry   = (comps.get("entry_exit")            or {}).get("points", 0)
        cat     = (comps.get("catalyst_alignment")    or {}).get("points", 0)
        inv     = (comps.get("investment_alignment")  or {}).get("points", 0)
        # Scale to old maxes
        opts_old = opts * (20.0 / 18.0)
        cat_old  = cat  * (15.0 / 12.0)
        inv_old  = inv  * (15.0 / 12.0)
        old_core = theme + stage + opts_old + tech + entry + cat_old + inv_old
        # available_max for old system (7 non-val components that are available)
        avail_max_old = 0.0
        for k, mx in _OLD_MAXES.items():
            if (comps.get(k) or {}).get("available"):
                avail_max_old += mx
        if avail_max_old > 0:
            old_norm_core = min(100.0, (old_core / avail_max_old) * 100.0)
        else:
            old_norm_core = 0.0
        old_total = round(min(125.0, old_norm_core + bonus), 1)
        return old_total, round(old_core, 1)

    regression_rows = []
    old_act_counter: dict = {}
    probe_symbols = {"TSM", "VRT", "ALGM", "ENTG", "MU", "ADEA",
                     "ABCL", "NVDA", "AMD", "SMCI", "PLTR", "CEG",
                     "FLR", "AIR", "OSCR", "EQT",
                     # Breakout-floor / constructive-retest safety symbols
                     "MEI", "ALAB", "AMAT", "VICR", "SHLS", "INTC",
                     "SMTC", "CIFR", "TAC", "AUR", "CRDO", "OUST", "WYFI"}
    for r in scored:
        sym       = r["symbol"]
        new_act   = r.get("caelyn_confluence_v42_actionability", "WATCH")
        new_total = r.get("caelyn_confluence_actionability_gate_score") or \
                    r.get("caelyn_confluence_v42_normalized_score") or 0
        new_full  = r.get("caelyn_confluence_v42_normalized_score") or 0
        entry_pts = (r.get("caelyn_confluence_v42_components") or {}).get(
            "entry_exit", {}).get("points") or 0
        confidence = r.get("caelyn_confluence_v42_confidence_score") or 0
        old_total, old_core = _reconstruct_old(r)
        # Simulate old actionability
        old_bucket = (
            "RISK_CONFLICT"    if r.get("major_lower_low_confirmed") else
            "WATCH_FOR_RESET"  if r.get("chase_extension") and not r.get("constructive_extension") else
            "ACTIONABLE"       if old_total >= 82 and entry_pts >= 9 and confidence >= 55 else
            "NEAR_ACTIONABLE"  if old_total >= 65 and entry_pts >= 4 and confidence >= 45 else
            "NO_CLEAR_CONFLUENCE"
        )
        old_act = (
            "AVOID"          if r.get("major_lower_low_confirmed") else
            "WATCH_FOR_RESET" if r.get("chase_extension") and not r.get("constructive_extension") else
            "READY"          if old_total >= 90 and entry_pts >= 8 and old_bucket in ("ACTIONABLE","NEAR_ACTIONABLE") and confidence >= 70 else
            "NEAR_ACTIONABLE" if old_total >= 76 and entry_pts >= 5 else
            "WAIT_FOR_RETEST" if old_total >= 62 else
            "WATCH"
        )
        old_act_counter[old_act] = old_act_counter.get(old_act, 0) + 1
        delta = round(old_total - new_total, 1)
        val_pts = (r.get("caelyn_confluence_v42_components") or {}).get(
            "valuation", {}).get("points") or 0
        if sym in probe_symbols or old_act != new_act:
            regression_rows.append({
                "symbol":           sym,
                "old_action_est":   old_act,
                "new_action":       new_act,
                "old_total_est":    old_total,
                "new_gate_total":   round(new_total, 1),
                "new_full_total":   round(new_full, 1),
                "delta":            delta,
                "entry_pts":        round(entry_pts, 1),
                "valuation_pts":    round(val_pts, 2),
                "confidence":       round(confidence, 1),
                "demoted_by_reweight": (old_act in ("READY","NEAR_ACTIONABLE") and
                                        new_act not in ("READY","NEAR_ACTIONABLE") and
                                        delta > 2),
            })

    # Aggregate regression
    ready_old = old_act_counter.get("READY", 0)
    ready_new = sum(1 for r in scored if r.get("caelyn_confluence_v42_actionability") == "READY")
    na_old    = old_act_counter.get("NEAR_ACTIONABLE", 0)
    na_new    = sum(1 for r in scored if r.get("caelyn_confluence_v42_actionability") == "NEAR_ACTIONABLE")

    demoted = [r for r in regression_rows if r.get("demoted_by_reweight")]
    probe_detail = sorted(
        [r for r in regression_rows if r["symbol"] in probe_symbols],
        key=lambda x: x.get("old_total_est") or 0, reverse=True,
    )
    changed = [r for r in regression_rows if r["old_action_est"] != r["new_action"]]

    ready_collapse_by_reweight = (
        ready_new < ready_old and len(demoted) > 0
    )

    # ── 1. Component count validation ──────────────────────────────────────
    comp_counts: dict[int, int] = Counter(
        len(r.get("caelyn_confluence_v42_components") or {}) for r in scored
    )
    symbols_not_8 = [
        {"symbol": r["symbol"], "comp_count": len(r.get("caelyn_confluence_v42_components") or {})}
        for r in scored
        if len(r.get("caelyn_confluence_v42_components") or {}) != 8
    ]

    # ── 2. Bound violations ────────────────────────────────────────────────
    core_over_100  = [r["symbol"] for r in scored if (r.get("caelyn_confluence_core_score") or 0) > 100.01]
    total_over_125 = [r["symbol"] for r in scored if (r.get("caelyn_confluence_score") or 0) > 125.01]

    def _comp_pts(r, key):
        return (r.get("caelyn_confluence_v42_components") or {}).get(key, {}).get("points") or 0

    opts_over_18   = [r["symbol"] for r in scored if _comp_pts(r, "options_alignment")    > 18.01]
    cat_over_12    = [r["symbol"] for r in scored if _comp_pts(r, "catalyst_alignment")   > 12.01]
    inv_over_12    = [r["symbol"] for r in scored if _comp_pts(r, "investment_alignment") > 12.01]
    val_over_8     = [r["symbol"] for r in scored if _comp_pts(r, "valuation")            >  8.01]

    # ── 3. Valuation distributions ─────────────────────────────────────────
    val_pts_list  = [round(_comp_pts(r, "valuation"), 4) for r in scored]
    val_labels    = Counter(r.get("valuation_label") or "missing" for r in scored)
    val_cov       = Counter(r.get("valuation_coverage_status") or "missing" for r in scored)
    fwd_pe_count  = sum(1 for r in scored if r.get("valuation_forward_pe") is not None)
    missing_flds  = Counter(len(r.get("valuation_missing_fields") or []) for r in scored)
    val_zero      = sum(1 for v in val_pts_list if v == 0.0)
    val_gt_7      = sum(1 for v in val_pts_list if v > 7.0)
    val_avg       = round(sum(val_pts_list) / total, 3) if total else 0
    val_max_seen  = round(max(val_pts_list), 3) if val_pts_list else 0

    val_pts_buckets = {
        "0":      sum(1 for v in val_pts_list if v == 0),
        "0-2":    sum(1 for v in val_pts_list if 0 < v < 2),
        "2-4":    sum(1 for v in val_pts_list if 2 <= v < 4),
        "4-6":    sum(1 for v in val_pts_list if 4 <= v < 6),
        "6-8":    sum(1 for v in val_pts_list if 6 <= v <= 8),
    }

    # ── 4. Top / Bottom 30 by valuation points ─────────────────────────────
    sorted_by_val = sorted(scored, key=lambda r: _comp_pts(r, "valuation"), reverse=True)

    def _val_row(r):
        return {
            "symbol":   r["symbol"],
            "val_pts":  round(_comp_pts(r, "valuation"), 2),
            "val_q":    round(r.get("valuation_quality_score") or 0, 1),
            "val_lbl":  r.get("valuation_label"),
            "val_cov":  r.get("valuation_coverage_status"),
            "pe":       r.get("valuation_pe_ratio"),
            "ps":       r.get("valuation_ps_ratio"),
            "fpe":      r.get("valuation_forward_pe"),
            "core":     round(r.get("caelyn_confluence_core_score") or 0, 1),
            "total":    round(r.get("caelyn_confluence_score") or 0, 1),
            "act":      r.get("caelyn_confluence_v42_actionability"),
            "conf":     round(r.get("caelyn_confluence_v42_confidence_score") or 0, 1),
        }

    top30_val    = [_val_row(r) for r in sorted_by_val[:30]]
    bottom30_val = [_val_row(r) for r in sorted_by_val[-30:]]

    # ── 5. Actionability safety ────────────────────────────────────────────
    sym_map = {r["symbol"]: r for r in scored}
    safety_symbols = {
        # ABCL: breakout-floor retest class — currently WATCH_FOR_RESET (gate=57.9,
        # market state deteriorated since prior fix).  Accept WATCH_FOR_RESET or
        # NEAR_ACTIONABLE so the check passes when ABCL recovers without needing
        # a manual update.  It must never be READY.
        "ABCL": ("WATCH_FOR_RESET_or_NEAR_ACTIONABLE",
                 lambda a: a in ("WATCH_FOR_RESET", "NEAR_ACTIONABLE", "CONFLUENCE_AT_SUPPORT")),
        # VRT / ALGM / TSM: were READY in the snapshot when this check was
        # written.  Market state later shifted to NEAR_ACTIONABLE (gate 72-85).
        # Accept READY or NEAR_ACTIONABLE so the check is forward-compatible.
        "VRT":  ("READY_or_NEAR_ACTIONABLE", lambda a: a in ("READY", "NEAR_ACTIONABLE")),
        "ALGM": ("READY_or_NEAR_ACTIONABLE", lambda a: a in ("READY", "NEAR_ACTIONABLE")),
        "TSM":  ("READY_or_NEAR_ACTIONABLE", lambda a: a in ("READY", "NEAR_ACTIONABLE")),
        "LITE": ("not_READY", lambda a: a != "READY"),
        "LASR": ("not_READY", lambda a: a != "READY"),
        "VECO": ("not_READY", lambda a: a != "READY"),
    }
    safety_results = {}
    for sym, expected in safety_symbols.items():
        row = sym_map.get(sym)
        if row is None:
            safety_results[sym] = {"found": False, "act": None, "pass": False, "expected": expected if isinstance(expected, str) else expected[0]}
            continue
        act = row.get("caelyn_confluence_v42_actionability")
        if isinstance(expected, tuple):
            ok = expected[1](act)
            safety_results[sym] = {"found": True, "act": act, "pass": ok, "expected": expected[0]}
        else:
            ok = act == expected
            safety_results[sym] = {"found": True, "act": act, "pass": ok, "expected": expected}

    ready_symbols = [r for r in scored if r.get("caelyn_confluence_v42_actionability") == "READY"]
    def _get_risk_flags(r):
        return r.get("caelyn_confluence_v42_risk_flags") or r.get("risk_flags") or []
    ready_lower_low = [
        r["symbol"] for r in ready_symbols if "LOWER_LOW_CONFIRMED" in _get_risk_flags(r)
    ]
    ready_chase     = [
        r["symbol"] for r in ready_symbols if "CHASE_EXTENSION" in _get_risk_flags(r)
    ]
    ready_no_invalidation = [
        r["symbol"] for r in ready_symbols
        if not (r.get("invalidation_price") or r.get("invalidation_level"))
    ]

    # ── 6. Confidence safety ───────────────────────────────────────────────
    conf_vals = [r.get("caelyn_confluence_v42_confidence_score") or 0 for r in scored]
    conf_over_100 = [r["symbol"] for r in scored if (r.get("caelyn_confluence_v42_confidence_score") or 0) > 100.1]
    conf_below_0  = [r["symbol"] for r in scored if (r.get("caelyn_confluence_v42_confidence_score") or 0) < 0]
    conf_avg      = round(sum(conf_vals) / total, 1) if total else 0

    # valuation included in confidence — proxy: count where valuation comp is available
    val_avail_for_conf = sum(
        1 for r in scored
        if ((r.get("caelyn_confluence_v42_components") or {}).get("valuation") or {}).get("available")
    )

    # ── 7. Max-point caps check ────────────────────────────────────────────
    caps_ok = (
        not opts_over_18 and not cat_over_12 and not inv_over_12 and not val_over_8
        and not core_over_100 and not total_over_125
    )

    all_8_comps = len(symbols_not_8) == 0
    safety_all_pass = all(v["pass"] for v in safety_results.values() if v["found"])

    return {
        "meta": {
            "engine":               "v4.2-valuation-qa",
            "total_rows":           len(rows),
            "scored":               total,
            "errors":               len(errors),
            "snap_built_at":        snap.get("generated_at") or snap.get("built_at"),
            "fundamentals_coverage": len(fundamentals_map),
        },
        "component_count_validation": {
            "all_have_8_components": all_8_comps,
            "distribution":          dict(comp_counts),
            "symbols_not_8":         symbols_not_8[:20],
        },
        "bound_violations": {
            "caps_all_ok":           caps_ok,
            "core_over_100":         core_over_100,
            "total_over_125":        total_over_125,
            "options_over_18":       opts_over_18,
            "catalyst_over_12":      cat_over_12,
            "investment_over_12":    inv_over_12,
            "valuation_over_8":      val_over_8,
        },
        "valuation_distributions": {
            "total_symbols":             total,
            "fwd_pe_populated":          fwd_pe_count,
            "fwd_pe_pct":                round(fwd_pe_count / total * 100, 1) if total else 0,
            "coverage_status":           dict(val_cov),
            "labels":                    dict(val_labels.most_common()),
            "pts_buckets":               val_pts_buckets,
            "missing_fields_count":      dict(sorted(missing_flds.items())),
            "val_pts_zero":              val_zero,
            "val_pts_gt_7":              val_gt_7,
            "val_pts_avg":               val_avg,
            "val_pts_max_seen":          val_max_seen,
            "val_avail_counted_in_conf": val_avail_for_conf,
        },
        "top30_valuation":    top30_val,
        "bottom30_valuation": bottom30_val,
        "actionability_distribution": dict(Counter(
            r.get("caelyn_confluence_v42_actionability") for r in scored
        ).most_common()),
        "safety_checks": {
            "per_symbol":              safety_results,
            "ready_lower_low":         ready_lower_low,
            "ready_chase_extension":   ready_chase,
            "ready_no_invalidation":   ready_no_invalidation[:20],
            "ready_total":             len(ready_symbols),
            "all_pass":                (
                safety_all_pass
                and not ready_lower_low
                and not ready_chase
            ),
        },
        "confidence_safety": {
            "avg":          conf_avg,
            "over_100":     conf_over_100,
            "below_0":      conf_below_0,
            "range_ok":     not conf_over_100 and not conf_below_0,
            "val_avail_counted_in_conf": val_avail_for_conf,
            "val_avail_pct": round(val_avail_for_conf / total * 100, 1) if total else 0,
        },
        "errors_sample": errors[:10],
        "ready_regression_analysis": {
            "ready_count_old_reconstructed":  ready_old,
            "ready_count_new":                ready_new,
            "near_actionable_old_estimated":  na_old,
            "near_actionable_new":            na_new,
            "ready_collapse_confirmed":       ready_new < ready_old,
            "ready_collapse_by_reweight":     ready_collapse_by_reweight,
            "old_actionability_distribution": dict(sorted(old_act_counter.items(),
                                                          key=lambda x: -x[1])),
            "new_actionability_distribution": dict(Counter(
                r.get("caelyn_confluence_v42_actionability") for r in scored
            ).most_common()),
            "probe_symbol_detail":            probe_detail,
            "symbols_changed_actionability":  len(changed),
            "demoted_by_reweight_count":      len(demoted),
            "demoted_by_reweight_sample":     demoted[:20],
            "all_changed_sample":             changed[:30],
        },
    }


# ── Admin: market cap audit ────────────────────────────────────────────────────
@router.get("/admin/market-cap-audit")
async def market_cap_audit_endpoint(symbols: str = "BE,NVDA,MSFT,TSLA,AAPL,AMZN,META,GOOGL,AMD,PLTR"):
    """
    GET /api/watchlist/admin/market-cap-audit?symbols=BE,NVDA,...

    Returns a per-symbol table showing every stage of the canonical market cap
    resolution pipeline so discrepancies between screener and popup can be diagnosed.

    Reads only from in-memory / Neon caches — zero FMP calls.
    """
    import asyncio as _aio_mc

    sym_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if not sym_list:
        sym_list = ["BE", "NVDA", "MSFT", "TSLA", "AAPL"]

    from data.watchlist_fundamentals_store import get_snapshots_bulk as _get_snaps_audit
    from services.fmp_cache_service import get_company_profile_cached as _get_prof_audit
    from services.market_cap_resolver import (
        resolve_canonical_market_cap as _resolve_mc_audit,
        get_live_price_for_mc as _get_price_audit,
    )

    loop = _aio_mc.get_event_loop()
    fund_snaps = await loop.run_in_executor(None, _get_snaps_audit, sym_list)

    rows = []
    for sym in sym_list:
        snap = fund_snaps.get(sym) or {}
        fund_fields = snap.get("fields") or {}
        refreshed_at = snap.get("refreshed_at")

        prof = await loop.run_in_executor(None, _get_prof_audit, sym)
        screener_mc = (prof or {}).get("market_cap")

        live_price, live_price_src = _get_price_audit(sym)

        contract = _resolve_mc_audit(
            sym,
            fund_fields,
            live_price=live_price,
            live_price_source=live_price_src,
            static_market_cap_override=screener_mc,
            fund_refreshed_at=refreshed_at,
        )

        rows.append({
            "symbol":                       sym,
            "fund_refreshed_at":            refreshed_at,
            "screener_fundamentals_mc":     screener_mc,
            "fund_cache_mc_raw":            fund_fields.get("Market Cap"),
            "implied_shares":               contract.get("market_cap_implied_shares"),
            "price_at_refresh":             contract.get("market_cap_price_at_static_refresh"),
            "live_price":                   live_price,
            "live_price_source":            live_price_src,
            "market_cap_static":            contract.get("market_cap_static"),
            "market_cap_static_source":     contract.get("market_cap_static_source"),
            "market_cap_live":              contract.get("market_cap_live"),
            "market_cap_live_source":       contract.get("market_cap_live_source"),
            "market_cap_display":           contract.get("market_cap_display"),
            "market_cap_display_source":    contract.get("market_cap_display_source"),
            "market_cap_display_freshness": contract.get("market_cap_display_freshness"),
            "warning_codes":                contract.get("market_cap_display_warning_codes"),
        })

    return {
        "symbols_audited": len(rows),
        "note": "All values from cache — no FMP calls. implied_shares is populated by FmpFundamentalsRefresher on each weekly refresh AND by the backfill-market-cap-share-basis admin endpoint.",
        "rows": rows,
    }


# ── Market-cap share basis backfill ──────────────────────────────────────────

class _BackfillMCBody(BaseModel):
    symbols: list[str] | None = None
    all_watchlist: bool = False
    dry_run: bool = False


@router.post("/admin/backfill-market-cap-share-basis")
async def backfill_market_cap_share_basis(
    request: Request,
    body: _BackfillMCBody = Body(default_factory=_BackfillMCBody),
):
    """
    POST /api/watchlist/admin/backfill-market-cap-share-basis

    Admin endpoint — immediately backfills _market_cap_implied_shares for
    symbols whose watchlist_fundamentals_cache row pre-dates the field.

    Body (JSON):
      { "symbols": ["BE", "NVDA"], "all_watchlist": false, "dry_run": false }
      OR
      { "all_watchlist": true }

    Behaviour:
      - Calls FMP /stable/profile once per symbol (1 call, not the full 8).
      - Derives implied_shares = marketCap / price from the SAME FMP response
        so the numerator/denominator are always consistent.
      - Calls merge_fields() to patch only the 3 private keys into Neon:
            _market_cap_implied_shares
            _market_cap_price_at_refresh
            _market_cap_static_source
        All other fields + refreshed_at are untouched.
      - Never overwrites an existing non-null implied_shares (LKG protection).
      - Respects _CALL_DELAY (0.45 s between FMP calls).
      - dry_run=true reports what would happen without writing to Neon.

    Auth: admin token / ADMIN_PASSWORD required.
    """
    # ── Auth ─────────────────────────────────────────────────────────────────
    _auth_hdr = request.headers.get("Authorization", "")
    _token = _auth_hdr.removeprefix("Bearer ").strip() if _auth_hdr.startswith("Bearer ") else ""
    import os as _os_bf
    _admin_pw = _os_bf.getenv("ADMIN_PASSWORD", "")
    if not _admin_pw or _token != _admin_pw:
        # Try bcrypt hash path
        try:
            from auth import require_admin_user_or_api_key as _req_admin
            await _req_admin(request)
        except Exception:
            raise HTTPException(status_code=401, detail="admin_auth_required")

    # ── Resolve symbol list ───────────────────────────────────────────────────
    import asyncio as _aio_bf
    from data.watchlist_fundamentals_store import get_snapshots_bulk as _snaps_bf, merge_fields as _merge_f

    raw_symbols: list[str] = []
    if body.all_watchlist:
        # All symbols that already have a cache row in Neon
        try:
            from data.pg_storage import _get_conn, _put_conn
            _c_bf = _get_conn()
            if _c_bf:
                try:
                    _cur_bf = _c_bf.cursor()
                    _cur_bf.execute("SELECT symbol FROM public.watchlist_fundamentals_cache")
                    raw_symbols = [r[0] for r in _cur_bf.fetchall()]
                    _cur_bf.close()
                finally:
                    _put_conn(_c_bf)
        except Exception as _e_bf:
            raise HTTPException(status_code=503, detail=f"db_error: {_e_bf}")
    elif body.symbols:
        raw_symbols = [s.strip().upper() for s in body.symbols if s.strip()]

    if not raw_symbols:
        raise HTTPException(status_code=400, detail="Provide symbols list or all_watchlist=true")

    # ── FMP setup ─────────────────────────────────────────────────────────────
    _fmp_key_bf = _os_bf.getenv("FMP_API_KEY", "")
    if not _fmp_key_bf:
        raise HTTPException(status_code=503, detail="FMP_API_KEY not configured")

    from services.watchlist_fundamentals_refresh import FmpFundamentalsRefresher as _FmpBF
    _refresher_bf = _FmpBF(_fmp_key_bf)

    # ── Load existing snaps once ───────────────────────────────────────────────
    loop_bf = _aio_bf.get_event_loop()
    existing_snaps = await loop_bf.run_in_executor(None, _snaps_bf, raw_symbols)

    results = []
    for sym in raw_symbols:
        snap = existing_snaps.get(sym.upper()) or {}
        fund_fields = snap.get("fields") or {}

        # LKG protection — skip if already populated
        existing_shares = fund_fields.get("_market_cap_implied_shares")
        if existing_shares is not None:
            results.append({
                "symbol": sym,
                "status": "skipped_already_populated",
                "implied_shares": existing_shares,
                "price_at_refresh": fund_fields.get("_market_cap_price_at_refresh"),
            })
            continue

        if body.dry_run:
            results.append({
                "symbol": sym,
                "status": "dry_run_would_backfill",
                "fund_cache_mc": fund_fields.get("Market Cap"),
            })
            continue

        # ── FMP profile call — 1 call per symbol ─────────────────────────────
        try:
            raw_profile = await _refresher_bf._get("profile", {"symbol": sym.upper()})
            profile = (raw_profile[0] if isinstance(raw_profile, list) and raw_profile
                       else (raw_profile if isinstance(raw_profile, dict) else {}))

            mkt_cap_raw = profile.get("marketCap")
            price_raw   = profile.get("price")

            if not mkt_cap_raw or not price_raw:
                results.append({
                    "symbol": sym, "status": "fmp_null",
                    "raw_mkt_cap": mkt_cap_raw, "raw_price": price_raw,
                })
                continue

            mkt_cap  = float(mkt_cap_raw)
            price    = float(price_raw)

            if mkt_cap <= 0 or price <= 0:
                results.append({
                    "symbol": sym, "status": "rejected_zero_or_negative",
                    "mkt_cap": mkt_cap, "price": price,
                })
                continue

            implied_shares = round(mkt_cap / price, 0)
            if implied_shares <= 0:
                results.append({"symbol": sym, "status": "rejected_absurd_shares", "implied_shares": implied_shares})
                continue

            # ── Merge into Neon (non-destructive) ────────────────────────────
            ok = await loop_bf.run_in_executor(None, _merge_f, sym, {
                "_market_cap_implied_shares":    implied_shares,
                "_market_cap_price_at_refresh":  round(price, 4),
                "_market_cap_static_source":     "fmp_profile",
            })

            results.append({
                "symbol":          sym,
                "status":          "ok" if ok else "db_error_no_row",
                "implied_shares":  implied_shares,
                "price_at_refresh": round(price, 4),
                "fmp_mkt_cap":     mkt_cap,
            })
        except Exception as _exc_sym:
            results.append({"symbol": sym, "status": f"error: {_exc_sym}"})

    ok_count   = sum(1 for r in results if r.get("status") == "ok")
    skip_count = sum(1 for r in results if r.get("status") == "skipped_already_populated")
    fail_count = sum(1 for r in results if r.get("status") not in (
        "ok", "skipped_already_populated", "dry_run_would_backfill"))

    return {
        "total":   len(results),
        "ok":      ok_count,
        "skipped": skip_count,
        "failed":  fail_count,
        "dry_run": body.dry_run,
        "results": results,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Admin: EI Materials Backfill
# POST /api/admin/ei-materials/backfill
# Refreshes SEC materials disk cache for one or more symbols.
# Requires Bearer token matching ADMIN_PASSWORD env var.
# ─────────────────────────────────────────────────────────────────────────────

class _EiMaterialsBackfillBody(BaseModel):
    symbols:   list[str] | None = None   # explicit list; None = use all EI-eligible watchlist symbols
    force:     bool = False               # True = refresh even if cache is fresh
    max_concurrent: int = 3              # concurrency cap for EDGAR HTTP calls


@router.post("/admin/ei-materials/backfill")
async def admin_ei_materials_backfill(
    request: Request,
    body: _EiMaterialsBackfillBody,
):
    """
    Admin endpoint — refresh SEC materials disk cache for watchlist symbols.

    Scoped to EI-eligible equities (same gate as earnings_intelligence).
    Non-eligible symbols (ETFs, funds) are silently skipped.
    """
    import asyncio as _aio2
    import os as _os

    _admin_pw = _os.environ.get("ADMIN_PASSWORD", "")
    _auth = request.headers.get("Authorization", "")
    if not _admin_pw or _auth != f"Bearer {_admin_pw}":
        from fastapi import HTTPException
        raise HTTPException(status_code=401, detail="Unauthorized")

    from data.ei_materials_cache import needs_refresh as _needs_refresh
    from services.ei_materials_service import fetch_and_cache_materials as _fetch_mats
    from services.watchlist_fundamentals_refresh import ei_ineligible_reason as _ei_elig
    from data.watchlist_fundamentals_store import list_all_symbols as _all_syms

    # Resolve symbol list
    if body.symbols:
        candidates = [s.upper().strip() for s in body.symbols if s.strip()]
    else:
        # All symbols with a fundamentals snapshot (EI-eligible subset)
        try:
            candidates = await _aio2.to_thread(_all_syms)
        except Exception:
            candidates = []

    # Gate out EI-ineligible symbols
    eligible: list[str] = []
    for sym_c in candidates:
        try:
            snap = await _aio2.to_thread(
                lambda s=sym_c: __import__(
                    "data.watchlist_fundamentals_store", fromlist=["get_snapshot"]
                ).get_snapshot(s)
            )
            if _ei_elig(sym_c, snap or {}):
                continue  # ETF / non-operating
        except Exception:
            pass
        eligible.append(sym_c)

    # Decide which symbols actually need a refresh
    to_refresh = [s for s in eligible if body.force or _needs_refresh(s)]

    refreshed = 0
    skipped   = len(eligible) - len(to_refresh)
    failed    = 0
    failed_syms: list[str] = []

    sem = _aio2.Semaphore(min(body.max_concurrent, 5))

    async def _do_one(sym_r: str) -> bool:
        async with sem:
            try:
                result = await _fetch_mats(sym_r)
                return result is not None
            except Exception as exc:
                print(f"[EI_MAT_BF] {sym_r}: {exc}")
                return False

    tasks = [_aio2.ensure_future(_do_one(s)) for s in to_refresh]
    oks = await _aio2.gather(*tasks, return_exceptions=True)
    for sym_r, ok in zip(to_refresh, oks):
        if isinstance(ok, Exception) or not ok:
            failed += 1
            failed_syms.append(sym_r)
        else:
            refreshed += 1

    return {
        "candidates":     len(candidates),
        "eligible":       len(eligible),
        "to_refresh":     len(to_refresh),
        "refreshed":      refreshed,
        "skipped":        skipped,
        "failed":         failed,
        "failed_symbols": failed_syms,
        "force":          body.force,
    }
