"""
Strategy Screener — stale-refresh scheduler.

Uses the "refresh-if-stale" pattern:
  - On GET /latest or /: check if the most recent snapshot is older than cadence_days
  - If stale (or no snapshot exists), trigger background regeneration via asyncio.create_task()
  - Return the existing snapshot immediately (with is_stale=True flag) while background
    generation runs
  - POST /refresh forces immediate synchronous regeneration

No external cron required. Works reliably in any FastAPI deployment environment.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from services.playbook.strategy_screener.screener_service import CADENCE_DAYS


def is_snapshot_stale(snapshot: Optional[Dict[str, Any]], cadence_days: int = CADENCE_DAYS) -> bool:
    """Return True if snapshot is missing, in error state, or older than cadence_days."""
    if not snapshot:
        return True
    if snapshot.get("status") in ("generating",):
        return False   # generation already running — not stale, just pending
    if snapshot.get("status") == "error":
        return True    # always retry after error

    generated_at = snapshot.get("generated_at", "")
    if not generated_at:
        return True

    try:
        if isinstance(generated_at, str):
            # Handle both offset-aware and naive ISO strings
            if generated_at.endswith("Z"):
                generated_at = generated_at[:-1] + "+00:00"
            dt = datetime.fromisoformat(generated_at)
            if dt.tzinfo is None:
                from datetime import timezone as _tz
                dt = dt.replace(tzinfo=_tz.utc)
        else:
            dt = generated_at
        age_days = (datetime.now(timezone.utc) - dt).total_seconds() / 86400
        return age_days > cadence_days
    except Exception:
        return True


def enqueue_background_refresh():
    """
    Trigger a background snapshot generation via asyncio.create_task().
    Safe to call from async route handlers — does not await.
    """
    import services.playbook.strategy_screener.screener_service as _svc
    if _svc._generation_in_progress:
        print("[SCREENER][SCHEDULER] Generation already in progress — not enqueuing")
        return

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.create_task(_svc.generate_snapshot_from_cr(manual_override=False))
            print("[SCREENER][SCHEDULER] Background CR snapshot generation enqueued (30 candidates)")
        else:
            print("[SCREENER][SCHEDULER] No running event loop — cannot enqueue background task")
    except RuntimeError:
        print("[SCREENER][SCHEDULER] Event loop not available for background task")


def attach_stale_flag(snapshot: Dict[str, Any], cadence_days: int = CADENCE_DAYS) -> Dict[str, Any]:
    """Return a copy of the snapshot dict with is_stale set correctly."""
    import copy
    s = copy.copy(snapshot)
    s["is_stale"] = is_snapshot_stale(snapshot, cadence_days)
    return s
