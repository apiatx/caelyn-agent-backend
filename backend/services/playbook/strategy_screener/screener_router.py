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
"""
from __future__ import annotations

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
    attach_stale_flag,
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
    Return cadence config and grade scale for the strategy screener.
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
    """
    snaps = list_snapshots(limit=limit)
    return {
        "count": len(snaps),
        "snapshots": [
            {
                "snapshot_id":   s["snapshot_id"],
                "generated_at":  s["generated_at"],
                "status":        s["status"],
                "results_count": s["results_count"],
                "cadence":       s["cadence"],
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

    If the snapshot is stale (older than cadence_days), a background refresh
    is triggered automatically. The existing snapshot is returned immediately
    with is_stale=True while generation runs in the background.

    If no snapshot exists yet, returns 202 Accepted and starts generation.
    """
    snapshot = get_latest_snapshot()

    if not snapshot:
        # First run — kick off generation, return 202
        enqueue_background_refresh()
        return JSONResponse(
            status_code=202,
            content={
                "status":   "generating",
                "message":  "First-run snapshot generation started. Try again in ~30 seconds.",
                "cadence":  _cadence_label(CADENCE_DAYS),
                "snapshot": None,
            },
        )

    if is_snapshot_stale(snapshot):
        enqueue_background_refresh()

    return attach_stale_flag(snapshot)


# ── GET /api/strategy-screener/report/{snapshot_id}/{ticker} ─────────────────

@router.get("/report/{snapshot_id}/{ticker}")
async def get_screener_report(
    snapshot_id: str = Path(..., description="Snapshot ID, e.g. serenity_2026_04_18_0400"),
    ticker:      str = Path(..., description="Ticker symbol, e.g. NVDA or SIVE.ST"),
):
    """
    Return the full deep-dive report for one candidate in a snapshot.

    Falls back to the most recent report for this ticker across all snapshots
    if the specified snapshot_id is not found.
    """
    ticker = ticker.upper()
    report = get_report(snapshot_id, ticker)

    if not report:
        # Try latest snapshot for this ticker
        report = get_latest_report(ticker)

    if not report:
        raise HTTPException(
            status_code=404,
            detail=f"No report found for {ticker} in snapshot {snapshot_id}. "
                   f"The snapshot may still be generating, or the ticker was not in the shortlist.",
        )

    return report


# ── POST /api/strategy-screener/refresh ──────────────────────────────────────

@router.post("/refresh")
async def manual_refresh(background_tasks: BackgroundTasks):
    """
    Trigger a manual snapshot regeneration.

    The new snapshot is generated in the background. Returns immediately with
    202 Accepted. Poll GET /latest to check status.
    """
    background_tasks.add_task(_run_refresh_task)
    return JSONResponse(
        status_code=202,
        content={
            "status":   "accepted",
            "message":  "Manual snapshot regeneration started. Poll GET /latest for status.",
            "cadence":  _cadence_label(CADENCE_DAYS),
        },
    )


async def _run_refresh_task():
    """Background task wrapper for manual refresh."""
    try:
        await generate_snapshot(manual_override=True)
    except Exception as e:
        print(f"[SCREENER] Manual refresh task error: {e}")
