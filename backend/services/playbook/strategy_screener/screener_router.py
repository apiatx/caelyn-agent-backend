"""
Strategy Screener Router — /api/strategy-screener

All routes are completely isolated from /api/query and the AI terminal.
Reads from persisted snapshots; regenerates only when stale or manually triggered.

Routes:
  GET  /api/strategy-screener              → alias for /latest
  GET  /api/strategy-screener/latest       → latest snapshot (list page payload)
  GET  /api/strategy-screener/snapshots    → list of recent snapshot metadata
  GET  /api/strategy-screener/config       → cadence info + grade scale
  GET  /api/strategy-screener/report/{snapshot_id}/{ticker}  → full candidate report
  POST /api/strategy-screener/refresh      → force manual snapshot regeneration

── GET /latest status field contract ─────────────────────────────────────────

Always present on the response:

  status: "ready" | "generating" | "stale" | "error"

  "ready"      — snapshot is fresh and complete. Use results array.
  "generating" — no usable snapshot yet (or first-run). Poll again in ~30s.
                 HTTP 202. results is empty.
  "stale"      — snapshot exists but older than cadence_days.
                 Background refresh has been enqueued.
                 results is populated (show it, but note the freshness).
                 HTTP 200. is_stale: true.
  "error"      — last generation failed. Auto-retry enqueued.
                 HTTP 200. results is empty.

Additional fields always present: snapshot_id, generated_at, is_stale, message.
message is null on "ready"; explains state on all other statuses.

── POST /refresh status field contract ───────────────────────────────────────

  202  {"status": "accepted",          ...}  — refresh enqueued
  409  {"status": "already_generating",...}  — generation already running; don't duplicate
"""
from __future__ import annotations

import services.playbook.strategy_screener.screener_service as _svc

from fastapi import APIRouter, BackgroundTasks, HTTPException, Path, Query
from fastapi.responses import JSONResponse

from services.playbook.strategy_screener.screener_service import (
    CADENCE_DAYS,
    PLAYBOOK_ID,
    SHORTLIST_SIZE,
    VERSION,
    generate_snapshot,
    _cadence_label,
)
from services.playbook.strategy_screener.screener_storage import (
    get_latest_snapshot,
    get_latest_report,
    get_report,
    get_snapshot_by_id,
    list_snapshots,
    init_screener_tables,
)
from services.playbook.strategy_screener.screener_scheduler import (
    enqueue_background_refresh,
    is_snapshot_stale,
)
from services.playbook.strategy_screener.screener_types import ScreenerConfig

router = APIRouter(prefix="/api/strategy-screener", tags=["strategy-screener"])

# Ensure tables exist on module load
try:
    init_screener_tables()
except Exception as _e:
    print(f"[SCREENER] Table init deferred (DB may not be ready): {_e}")


# ── GET /api/strategy-screener/config ────────────────────────────────────────

@router.get("/config")
async def screener_config():
    """
    Return cadence config and grade scale.
    This endpoint is always fast — no DB read, no generation.
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
    Use for archive/history UI; the results array is omitted to keep this fast.
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
async def get_latest_screener():
    """
    Return the latest Serenity Strategy Screener snapshot.

    Always returns a JSON object with a top-level `status` field:

      "ready"      → 200  fresh snapshot, results populated
      "stale"      → 200  old snapshot served, background refresh started
      "generating" → 202  no usable snapshot yet, generation in progress
      "error"      → 200  last run failed, auto-retry enqueued

    Additional fields always present regardless of status:
      snapshot_id, generated_at, is_stale, message, cadence, cadence_days
    """
    snapshot = get_latest_snapshot()

    # ── Case 1: No snapshot has ever been generated ──────────────────────────
    if not snapshot:
        enqueue_background_refresh()
        return JSONResponse(
            status_code=202,
            content={
                "status":        "generating",
                "is_stale":      True,
                "snapshot_id":   None,
                "generated_at":  None,
                "results":       [],
                "results_count": 0,
                "cadence":       _cadence_label(CADENCE_DAYS),
                "cadence_days":  CADENCE_DAYS,
                "summary":       "",
                "message":       "First snapshot generation started. Poll again in ~30 seconds.",
                "regime_context": None,
                "generation_notes": "",
                "version":       VERSION,
            },
        )

    # ── Case 2: A snapshot row exists — compute effective status ─────────────
    db_status = snapshot.get("status", "complete")
    stale     = is_snapshot_stale(snapshot)

    if db_status == "generating":
        status   = "generating"
        message  = "Snapshot generation in progress. Poll again in ~30 seconds."
    elif db_status == "error":
        status   = "error"
        message  = "Last generation failed. Automatic retry has been enqueued."
        enqueue_background_refresh()
    elif stale:
        status   = "stale"
        message  = f"Snapshot is older than {CADENCE_DAYS} days. Refreshing in background."
        enqueue_background_refresh()
    else:
        status   = "ready"
        message  = None

    result = dict(snapshot)          # copy, don't mutate the dict from DB
    result["status"]   = status      # overwrite DB's "complete" with our enum
    result["is_stale"] = stale
    result["message"]  = message

    # Ensure cadence_days is always in the payload
    result.setdefault("cadence_days", CADENCE_DAYS)

    http_status = 202 if status == "generating" and not snapshot.get("results") else 200
    if http_status == 202:
        return JSONResponse(status_code=202, content=result)
    return result


# ── GET /api/strategy-screener/report/{snapshot_id}/{ticker} ─────────────────

@router.get("/report/{snapshot_id}/{ticker}")
async def get_screener_report(
    snapshot_id: str = Path(..., description="Snapshot ID, e.g. serenity_2026_04_18_0400"),
    ticker:      str = Path(..., description="Ticker symbol, e.g. NVDA or SIVE.ST"),
):
    """
    Return the full deep-dive report for one candidate.

    Lookup order:
      1. Exact (snapshot_id, ticker) match
      2. Most recent report for this ticker across all snapshots (fallback)

    404 if the ticker was never in any shortlist or snapshot is still generating.
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
                    "Either the snapshot is still generating, the ticker was not in the "
                    "shortlist, or the snapshot_id is incorrect."
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
      202  {"status": "accepted"}         — refresh started, poll GET /latest
      409  {"status": "already_generating"} — generation already running
    """
    if _svc._generation_in_progress:
        return JSONResponse(
            status_code=409,
            content={
                "status":      "already_generating",
                "message":     "A snapshot is already being generated. Poll GET /latest for status.",
                "cadence":     _cadence_label(CADENCE_DAYS),
                "cadence_days": CADENCE_DAYS,
            },
        )

    background_tasks.add_task(_run_refresh_task)
    return JSONResponse(
        status_code=202,
        content={
            "status":      "accepted",
            "message":     "Manual snapshot regeneration started. Poll GET /latest — it will return status='generating' while running.",
            "poll_url":    "/api/strategy-screener/latest",
            "cadence":     _cadence_label(CADENCE_DAYS),
            "cadence_days": CADENCE_DAYS,
        },
    )


async def _run_refresh_task():
    """Background task wrapper for manual refresh."""
    try:
        await generate_snapshot(manual_override=True)
    except Exception as e:
        print(f"[SCREENER] Manual refresh task error: {e}")


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
