"""
Strategy Screener Router — /api/strategy-screener

All routes are completely isolated from /api/query and the AI terminal.
Reads from persisted snapshots; regenerates only when stale or manually triggered.

Routes:
  GET  /api/strategy-screener              → alias for /latest
  GET  /api/strategy-screener/latest       → latest snapshot (filter/sort/limit support)
  GET  /api/strategy-screener/snapshots    → list of recent snapshot metadata
  GET  /api/strategy-screener/config       → cadence, grade scale, dropdown metadata
  GET  /api/strategy-screener/report/{snapshot_id}/{ticker}  → full candidate report
  POST /api/strategy-screener/refresh      → force manual snapshot regeneration

── GET /latest query params ──────────────────────────────────────────────────

All params are optional. No params = same behaviour as before (backwards compatible).

  market_cap_bucket = large_cap | mid_cap | small_cap | micro_cap
  layer             = 1 | 2 | 3
  sort_by           = best_fit (default) | market_cap | layer | grade
  limit             = 1–100  (default 20)

When any filter/sort param is present the response includes extra fields:
  active_filters, active_sort, filtered_result_count, available_result_count

── GET /latest status field ──────────────────────────────────────────────────

  status: "ready" | "generating" | "stale" | "error"

── POST /refresh status field ────────────────────────────────────────────────

  202  {"status": "accepted"}           — refresh enqueued
  409  {"status": "already_generating"} — already running
"""
from __future__ import annotations

from typing import Optional

import services.playbook.strategy_screener.screener_service as _svc

from fastapi import APIRouter, BackgroundTasks, HTTPException, Path, Query
from fastapi.responses import JSONResponse

from services.playbook.strategy_screener.screener_service import (
    CADENCE_DAYS,
    PLAYBOOK_ID,
    SHORTLIST_SIZE,
    VERSION,
    generate_snapshot,
    generate_snapshot_from_cr,
    _cadence_label,
)
from services.playbook.strategy_screener.screener_storage import (
    get_latest_snapshot,
    get_latest_report,
    get_report,
    get_snapshot_by_id,
    list_snapshots,
    init_screener_tables,
    patch_snapshot_market_caps,
)
from services.playbook.strategy_screener.screener_scheduler import (
    enqueue_background_refresh,
    is_snapshot_stale,
)
from services.playbook.strategy_screener.screener_types import ScreenerConfig
from services.playbook.strategy_screener.screener_filters import (
    apply_filters_and_sort,
    classify_market_cap,
    VALID_BUCKETS,
    VALID_LAYERS,
    VALID_SORTS,
)

router = APIRouter(prefix="/api/strategy-screener", tags=["strategy-screener"])

# Table initialization is performed in _deferred_sync_startup() inside main.py
# so that importing this router never touches Neon or performs DDL.

_FILTER_PARAMS_DOC = (
    "Optional filters (applied to stored snapshot, no regeneration): "
    "market_cap_bucket=large_cap|mid_cap|small_cap|micro_cap, "
    "layer=1|2|3, "
    "sort_by=best_fit|market_cap|layer|grade, "
    "limit=1-100"
)


# ── GET /api/strategy-screener/config ────────────────────────────────────────

@router.get("/config")
async def screener_config():
    """
    Return cadence config, grade scale, and frontend dropdown metadata.
    Always fast — no DB read, no generation.
    """
    cfg = ScreenerConfig(
        playbook_id=PLAYBOOK_ID,
        cadence=_cadence_label(CADENCE_DAYS),
        cadence_days=CADENCE_DAYS,
        shortlist_size=SHORTLIST_SIZE,
        version=VERSION,
    )
    return cfg.model_dump()


# ── GET /api/strategy-screener/snapshots ─────────────────────────────────────

@router.get("/snapshots")
async def list_screener_snapshots(limit: int = Query(default=10, ge=1, le=50)):
    """
    Return the N most recent snapshot metadata entries (no candidate payload).
    Use for archive/history UI.
    """
    snaps = list_snapshots(limit=limit)
    return {
        "count": len(snaps),
        "snapshots": [
            {
                "snapshot_id":   s["snapshot_id"],
                "generated_at":  s["generated_at"],
                "status":        _effective_status(s),
                "results_count": s["results_count"],
                "cadence":       s["cadence"],
                "cadence_days":  s.get("cadence_days", CADENCE_DAYS),
                "summary":       s["summary"],
            }
            for s in snaps
        ],
    }


# ── GET /api/strategy-screener/latest  (and root alias) ──────────────────────

@router.get("/latest")
@router.get("")
async def get_latest_screener(
    market_cap_bucket: Optional[str] = Query(
        default=None,
        description="Filter by market cap bucket: large_cap | mid_cap | small_cap | micro_cap",
    ),
    layer: Optional[int] = Query(
        default=None,
        description="Filter by supply chain layer depth: 1 | 2 | 3",
        ge=1, le=4,
    ),
    sort_by: Optional[str] = Query(
        default=None,
        description="Sort order: best_fit (default) | market_cap | layer | grade",
    ),
    limit: int = Query(
        default=30,
        description="Max results to return (1–100)",
        ge=1, le=100,
    ),
):
    """
    Return the latest Serenity Strategy Screener snapshot.

    No query params → same response as before (backwards compatible).
    With filter/sort params → apply to stored results; never triggers regeneration.

    Always returns a top-level `status` field:
      "ready"      → 200  fresh snapshot, results populated
      "stale"      → 200  old snapshot, background refresh started
      "generating" → 202  no usable snapshot, generation in progress
      "error"      → 200  last generation failed, auto-retry enqueued
    """
    # Validate params early — 422 before any DB touch
    filter_active = (
        market_cap_bucket is not None
        or layer is not None
        or sort_by is not None
        or limit != 30
    )

    if market_cap_bucket is not None and market_cap_bucket not in VALID_BUCKETS:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "invalid_market_cap_bucket",
                "value": market_cap_bucket,
                "valid": sorted(VALID_BUCKETS),
            },
        )
    if sort_by is not None and sort_by not in VALID_SORTS:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "invalid_sort_by",
                "value": sort_by,
                "valid": sorted(VALID_SORTS),
            },
        )

    effective_sort = sort_by if sort_by is not None else "best_fit"

    snapshot = get_latest_snapshot()

    # ── Case 1: No snapshot has ever been generated ──────────────────────────
    if not snapshot:
        enqueue_background_refresh()
        return JSONResponse(
            status_code=202,
            content={
                "status":           "generating",
                "is_stale":         True,
                "snapshot_id":      None,
                "generated_at":     None,
                "results":          [],
                "results_count":    0,
                "cadence":          _cadence_label(CADENCE_DAYS),
                "cadence_days":     CADENCE_DAYS,
                "summary":          "",
                "message":          "First snapshot generation started. Poll again in ~30 seconds.",
                "regime_context":   None,
                "generation_notes": "",
                "version":          VERSION,
            },
        )

    # ── Case 2: Snapshot row exists — compute effective status ───────────────
    db_status = snapshot.get("status", "complete")
    stale     = is_snapshot_stale(snapshot)

    if db_status == "generating":
        status  = "generating"
        message = "Snapshot generation in progress. Poll again in ~30 seconds."
    elif db_status == "error":
        status  = "error"
        message = "Last generation failed. Automatic retry has been enqueued."
        enqueue_background_refresh()
    elif stale:
        status  = "stale"
        message = f"Snapshot is older than {CADENCE_DAYS} days. Refreshing in background."
        enqueue_background_refresh()
    else:
        status  = "ready"
        message = None

    result = dict(snapshot)
    result["status"]   = status
    result["is_stale"] = stale
    result["message"]  = message
    result.setdefault("cadence_days", CADENCE_DAYS)

    # ── Apply filters/sort if any param was supplied ─────────────────────────
    if filter_active and status in ("ready", "stale"):
        raw_candidates = result.get("results", [])
        try:
            filtered = apply_filters_and_sort(
                candidates=raw_candidates,
                market_cap_bucket=market_cap_bucket,
                layer=layer,
                sort_by=effective_sort,
                limit=limit,
            )
        except ValueError as e:
            raise HTTPException(status_code=422, detail={"error": "filter_error", "message": str(e)})

        result["results"]                  = filtered["results"]
        result["results_count"]            = len(filtered["results"])
        result["active_filters"]           = filtered["active_filters"]
        result["active_sort"]              = filtered["active_sort"]
        result["filtered_result_count"]    = filtered["filtered_result_count"]
        result["available_result_count"]   = filtered["available_result_count"]
        result["unknown_market_cap_count"] = filtered["unknown_market_cap_count"]
        result["limit"]                    = filtered["limit"]
    elif filter_active:
        # Snapshot not ready yet — still include the filter metadata so frontend
        # knows the params were received, but results is empty
        result["active_filters"]           = _build_active_filters(market_cap_bucket, layer)
        result["active_sort"]              = effective_sort
        result["filtered_result_count"]    = 0
        result["available_result_count"]   = 0
        result["unknown_market_cap_count"] = 0
        result["limit"]                    = limit
    else:
        # No filter params — backwards compat: add market_cap_bucket per result
        # and unknown count so frontend always has the metadata, even on the default view
        raw = result.get("results", [])
        result["results"] = [
            {**c, "market_cap_bucket": classify_market_cap(c.get("market_cap_usd"))}
            for c in raw
        ]
        result["unknown_market_cap_count"] = sum(
            1 for c in raw
            if c.get("market_cap_usd") is None or c.get("market_cap_usd", 0) < 1_000_000
        )

    # ── Additive thematic overlay: regime_alignment per row + regime_context ──
    # Reads pre-populated caches only. Never raises. Does not modify base scores.
    # Reused sources: regime:current_v1, sr:dashboard:v1, sr:theme_data:v2,
    # x_consensus_weekly.json (same caches as main agent context_broker).
    try:
        from services.thematic_context_provider import get_shared_thematic_context
        from services.theme_ticker_mapper import get_ticker_theme_alignment
        _tc       = get_shared_thematic_context()
        _active   = _tc.get("active_themes", [])
        _emerging = _tc.get("emerging_themes", [])
        _dead     = _tc.get("dead_zones", [])
        _sec_lead = {e["ticker"] for e in _tc.get("sector_leaders", []) if e.get("ticker")}
        _mac_reg  = _tc.get("macro_regime")

        # Populate the regime_context top-level field (was always None)
        result["regime_context"] = {
            "macro_regime":   _mac_reg,
            "sector_leaders": list(_sec_lead),
            "active_themes":  [t["name"] for t in _active[:3]],
            "emerging_themes":[t["name"] for t in _emerging[:2]],
            "dead_zones":     [t["name"] for t in _dead[:3]],
            "source_health":  _tc.get("source_health", {}),
        }

        # Annotate each result row
        for row in (result.get("results") or []):
            sym   = (row.get("ticker") or "").upper()
            base  = float(row.get("score") or row.get("fit_score") or row.get("best_fit_score") or 0)
            align = get_ticker_theme_alignment(sym, _active, _emerging, _dead)
            boost = align["regime_alignment_score"]
            row["theme_name"]             = align["theme_name"]
            row["theme_state"]            = align["theme_state"]
            row["regime_alignment_score"] = boost
            row["regime_alignment_label"] = align["regime_alignment_label"]
            row["thematic_badges"]        = align["thematic_badges"]
            row["dead_zone_warning"]      = align["dead_zone_warning"]
            row["base_score"]             = base
            row["final_score"]            = round(base + boost, 2)
    except Exception as _tc_err:
        print(f"[SCREENER_ROUTER] thematic overlay error: {_tc_err}")
    # ── end thematic overlay ─────────────────────────────────────────────────

    http_status = 202 if status == "generating" and not snapshot.get("results") else 200
    if http_status == 202:
        return JSONResponse(status_code=202, content=result)
    return result


def _build_active_filters(
    market_cap_bucket: Optional[str],
    layer: Optional[int],
) -> dict:
    f: dict = {}
    if market_cap_bucket is not None:
        f["market_cap_bucket"] = market_cap_bucket
    if layer is not None:
        f["layer"] = layer
    return f


# ── GET /api/strategy-screener/report/{snapshot_id}/{ticker} ─────────────────

@router.get("/report/{snapshot_id}/{ticker}")
async def get_screener_report(
    snapshot_id: str = Path(..., description="Snapshot ID e.g. serenity_2026_04_18_0400"),
    ticker:      str = Path(..., description="Ticker symbol e.g. NVDA or SIVE.ST"),
):
    """
    Return the full deep-dive report for one candidate.

    Lookup order:
      1. Exact (snapshot_id, ticker) match
      2. Most recent report for this ticker across all snapshots (fallback)

    404 if the ticker was never in any shortlist.
    """
    ticker = ticker.upper()
    report = get_report(snapshot_id, ticker)

    if not report:
        report = get_latest_report(ticker)

    if not report:
        raise HTTPException(
            status_code=404,
            detail={
                "error":       "report_not_found",
                "ticker":      ticker,
                "snapshot_id": snapshot_id,
                "message":     (
                    f"No report found for {ticker} in snapshot {snapshot_id}. "
                    "Either the snapshot is still generating, the ticker was not "
                    "in the shortlist, or the snapshot_id is incorrect."
                ),
            },
        )

    return report


# ── POST /api/strategy-screener/refresh ──────────────────────────────────────

@router.post("/refresh")
async def manual_refresh(background_tasks: BackgroundTasks):
    """
    Force a manual snapshot regeneration.

    Returns:
      202  {"status": "accepted"}           — refresh started, poll GET /latest
      409  {"status": "already_generating"} — already running
    """
    if _svc._generation_in_progress:
        return JSONResponse(
            status_code=409,
            content={
                "status":       "already_generating",
                "message":      "A snapshot is already being generated. Poll GET /latest for status.",
                "cadence":      _cadence_label(CADENCE_DAYS),
                "cadence_days": CADENCE_DAYS,
            },
        )

    background_tasks.add_task(_run_refresh_task)
    return JSONResponse(
        status_code=202,
        content={
            "status":       "accepted",
            "message":      "Manual snapshot regeneration started. Poll GET /latest — it will return status='generating' while running.",
            "poll_url":     "/api/strategy-screener/latest",
            "cadence":      _cadence_label(CADENCE_DAYS),
            "cadence_days": CADENCE_DAYS,
        },
    )


async def _run_refresh_task():
    try:
        await generate_snapshot_from_cr(manual_override=True)
    except Exception as e:
        print(f"[SCREENER] Manual refresh task error: {e}")


# ── POST /api/strategy-screener/enrich-market-caps ───────────────────────────

@router.post("/enrich-market-caps")
async def enrich_market_caps():
    """
    Backfill market_cap_usd for candidates in the latest snapshot that are missing it.

    This is a safe, targeted operation:
      - Loads the current stored snapshot (no regeneration)
      - Calls FMP (first) then Finnhub (fallback) for any candidate with missing market_cap_usd
      - Patches only the results JSONB in DB — no other snapshot fields change
      - Candidates that already have a valid market_cap_usd are left untouched

    Returns a summary of how many were enriched.
    Use this after the initial snapshot is stored to fix ADR/foreign market caps.
    """
    import os
    from services.playbook.strategy_screener.screener_enrichment import enrich_candidates

    snapshot = get_latest_snapshot()
    if not snapshot:
        raise HTTPException(status_code=404, detail={"error": "no_snapshot", "message": "No snapshot found to enrich"})

    if snapshot.get("status") == "generating":
        raise HTTPException(
            status_code=409,
            detail={"error": "generating", "message": "Cannot enrich while snapshot is generating. Try again after generation completes."},
        )

    candidates = snapshot.get("results", [])
    if not candidates:
        return {"status": "ok", "enriched_count": 0, "message": "Snapshot has no candidates"}

    missing_before = [c for c in candidates if not (c.get("market_cap_usd") and c["market_cap_usd"] >= 1_000_000)]

    fmp_key     = os.environ.get("FMP_API_KEY", "")
    finnhub_key = os.environ.get("FINNHUB_API_KEY", "")

    if not fmp_key and not finnhub_key:
        raise HTTPException(
            status_code=503,
            detail={"error": "no_providers", "message": "Neither FMP_API_KEY nor FINNHUB_API_KEY is configured"},
        )

    enriched = await enrich_candidates(candidates, fmp_key, finnhub_key)

    # Persist the backfilled results
    patched = patch_snapshot_market_caps(snapshot["snapshot_id"], enriched)

    missing_after  = [c for c in enriched if not (c.get("market_cap_usd") and c["market_cap_usd"] >= 1_000_000)]
    n_fixed        = len(missing_before) - len(missing_after)

    return {
        "status":          "ok",
        "snapshot_id":     snapshot["snapshot_id"],
        "candidates_total": len(candidates),
        "missing_before":  len(missing_before),
        "missing_after":   len(missing_after),
        "enriched_count":  n_fixed,
        "persisted":       patched,
        "message": (
            f"Enriched {n_fixed}/{len(missing_before)} previously-unknown market caps. "
            f"{len(missing_after)} remain unknown after enrichment."
            if missing_before else "All candidates already had market cap data."
        ),
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _effective_status(snapshot: dict) -> str:
    """Map DB status → frontend-facing status string."""
    db_status = snapshot.get("status", "complete")
    if db_status == "generating":
        return "generating"
    if db_status == "error":
        return "error"
    if is_snapshot_stale(snapshot):
        return "stale"
    return "ready"
