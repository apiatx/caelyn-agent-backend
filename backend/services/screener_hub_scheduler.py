"""
Scheduler loop for Screener Hub warm jobs.

Runs in the background as an asyncio task. Wake every minute, check the
Eastern-time clock, and fire any jobs whose target slot we've crossed since
the last tick. A small in-memory "last fired" map prevents the same slot
firing twice on the same calendar day.

Schedule (ET, all >=30 min apart):
    Sun 00:30 ET   thematic universe rebuild
    Sun 01:15 ET   thematic fundamentals warm
    Sun 03:15 ET   bottlenecks fundamentals warm
    Mon-Fri 00:00 ET   social X/Grok scan trigger (rebuild social universe)
    Mon-Fri 00:45 ET   social fundamentals warm
    Fri 02:00 ET   watchlist+portfolio fundamentals warm

Chain Reaction's existing weekly source job is left alone — bottlenecks_warm
re-uses whatever Chain Reaction (NODE_REGISTRY) has already published. The
3:15 ET slot ensures Chain Reaction has finished any earlier weekly work.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Awaitable, Callable, Optional

try:
    from zoneinfo import ZoneInfo
    _ET = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover
    _ET = None


# (weekday_pred, hour, minute, slot_id, runner)
# weekday_pred takes a Python weekday (Mon=0..Sun=6) and returns bool.
def _is_sunday(d: int) -> bool: return d == 6
def _is_friday(d: int) -> bool: return d == 4
def _is_weekday(d: int) -> bool: return 0 <= d <= 4


async def _run_thematic_rebuild() -> None:
    from services.screener_hub_service import rebuild_universe
    print("[SCREENER_HUB][SCHED] thematic universe rebuild starting")
    out = await rebuild_universe("thematic", force=False)
    print(f"[SCREENER_HUB][SCHED] thematic universe rebuild done: {out}")


async def _run_thematic_warm() -> None:
    from services.screener_hub_service import warm_tab_fundamentals
    print("[SCREENER_HUB][SCHED] thematic fundamentals warm starting")
    out = await warm_tab_fundamentals("thematic", force=False, max_calls=300)
    print(f"[SCREENER_HUB][SCHED] thematic fundamentals warm done: {out}")


async def _run_bottlenecks_warm() -> None:
    from services.screener_hub_service import (
        rebuild_universe, warm_tab_fundamentals,
    )
    # Refresh the universe snapshot first so any NODE_REGISTRY changes flow in.
    await rebuild_universe("bottlenecks", force=False)
    print("[SCREENER_HUB][SCHED] bottlenecks fundamentals warm starting")
    out = await warm_tab_fundamentals("bottlenecks", force=False, max_calls=200)
    print(f"[SCREENER_HUB][SCHED] bottlenecks fundamentals warm done: {out}")


async def _run_social_scan() -> None:
    """Refresh the social universe snapshot from latest X/Grok consensus."""
    from services.screener_hub_service import rebuild_universe
    print("[SCREENER_HUB][SCHED] social universe rebuild starting")
    out = await rebuild_universe("social", force=False)
    print(f"[SCREENER_HUB][SCHED] social universe rebuild done: {out}")


async def _run_social_warm() -> None:
    from services.screener_hub_service import warm_tab_fundamentals
    print("[SCREENER_HUB][SCHED] social fundamentals warm starting")
    out = await warm_tab_fundamentals("social", force=False, max_calls=120)
    print(f"[SCREENER_HUB][SCHED] social fundamentals warm done: {out}")


async def _run_watchlist_portfolio_warm() -> None:
    from services.screener_hub_service import (
        rebuild_universe, warm_tab_fundamentals,
    )
    await rebuild_universe("watchlist_portfolio", force=False)
    print("[SCREENER_HUB][SCHED] watchlist+portfolio fundamentals warm starting")
    out = await warm_tab_fundamentals("watchlist_portfolio", force=False, max_calls=150)
    print(f"[SCREENER_HUB][SCHED] watchlist+portfolio fundamentals warm done: {out}")


_SLOTS: list[tuple[Callable[[int], bool], int, int, str, Callable[[], Awaitable[None]]]] = [
    (_is_sunday,  0, 30, "thematic_rebuild",   _run_thematic_rebuild),
    (_is_sunday,  1, 15, "thematic_warm",      _run_thematic_warm),
    (_is_sunday,  3, 15, "bottlenecks_warm",   _run_bottlenecks_warm),
    (_is_weekday, 0,  0, "social_scan",        _run_social_scan),
    (_is_weekday, 0, 45, "social_warm",        _run_social_warm),
    (_is_friday,  2,  0, "watchlist_portfolio_warm", _run_watchlist_portfolio_warm),
]


async def scheduler_loop(tick_seconds: int = 60) -> None:
    """Run forever. Cheap clock-checks; fires each slot at most once per day."""
    if _ET is None:
        print("[SCREENER_HUB][SCHED] zoneinfo unavailable — scheduler disabled")
        return

    last_fired: dict[str, str] = {}

    print("[SCREENER_HUB][SCHED] scheduler loop started")
    while True:
        try:
            now = datetime.now(_ET)
            day_key = now.strftime("%Y-%m-%d")
            for pred, hour, minute, slot_id, runner in _SLOTS:
                if not pred(now.weekday()):
                    continue
                target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                # Fire if we're at-or-past the target this minute and haven't fired today
                if now >= target and last_fired.get(slot_id) != day_key:
                    # Only fire when the target is within the past 5 minutes —
                    # avoids firing immediately on startup if the target was earlier.
                    if (now - target) <= timedelta(minutes=5):
                        last_fired[slot_id] = day_key
                        try:
                            print(f"[SCREENER_HUB][SCHED] firing slot={slot_id} at {now.isoformat()}")
                            await runner()
                        except Exception as e:
                            print(f"[SCREENER_HUB][SCHED] slot={slot_id} runner error: {e}")
                    else:
                        # Past the window — mark as fired to avoid repeat checks today
                        last_fired[slot_id] = day_key
        except Exception as e:
            print(f"[SCREENER_HUB][SCHED] tick error: {e}")
        await asyncio.sleep(tick_seconds)
