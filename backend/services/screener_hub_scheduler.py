"""
Scheduler loop for Screener Hub warm jobs.

Runs in the background as an asyncio task. Wake every minute, check the
Eastern-time clock, and fire any jobs whose target slot we've crossed since
the last tick. A small in-memory "last fired" map prevents the same slot
firing twice on the same calendar day.

Schedule (ET, all >=30 min apart):
    Sun 00:30 ET   thematic universe rebuild
    Sun 01:15 ET   thematic fundamentals warm
    Sun 01:45 ET   historical returns warm (2w/4w/10w for thematic universe)
    Sun 02:15 ET   chain reaction dynamic weekly output generation
    Sun 03:15 ET   bottlenecks fundamentals warm (uses dynamic CR output if available)
    Sun-Fri 11:10 ET   social universe rebuild (= 10:10 AM Central; skips Saturday)
                       Intentionally 10 min after _x_consensus_loop fires at 10:00 AM CT
                       so the rebuild always reads the freshly written Grok/XAI cache.
    Sun-Fri 11:45 ET   social fundamentals warm
    Fri 02:00 ET   watchlist+portfolio fundamentals warm
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
def _not_saturday(d: int) -> bool: return d != 5   # Sun(6)+Mon-Fri(0-4); skips Sat(5)


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


async def _run_returns_warm() -> None:
    """Fetch real 2w/4w/10w historical returns for the thematic universe."""
    print("[SCREENER_HUB][SCHED] returns warm starting")
    try:
        from services.screener_hub_service import _build_thematic_universe
        symbols_map, _ = await _build_thematic_universe(None, with_fmp_peers=False)
        all_syms: list[str] = []
        seen: set[str] = set()
        for syms in symbols_map.values():
            for s in syms:
                if s not in seen:
                    seen.add(s)
                    all_syms.append(s)
        from services.screener_returns_service import fetch_and_cache_returns
        out = await fetch_and_cache_returns(
            all_syms, force=False, sleep_between_s=2.0,
            max_calls=200, job_name="returns_warm_thematic",
        )
        print(f"[SCREENER_HUB][SCHED] returns warm done: {out}")
    except Exception as e:
        print(f"[SCREENER_HUB][SCHED] returns warm error: {e}")


async def _run_chain_reaction_dynamic() -> None:
    """Generate weekly dynamic Chain Reaction scoring output and write to DB."""
    print("[SCREENER_HUB][SCHED] chain_reaction_dynamic starting")
    try:
        from services.chain_reaction_weekly_service import generate_chain_reaction_weekly
        import json
        from pathlib import Path

        # Load social + options overlap sets for enrichment
        social_set: set[str] = set()
        options_set: set[str] = set()
        try:
            sp = Path(__file__).parent.parent / "data" / "x_consensus_weekly.json"
            if sp.exists():
                d = json.loads(sp.read_text())
                for item in (d.get("top_tickers") or []):
                    sym = item.get("symbol") if isinstance(item, dict) else None
                    if sym:
                        social_set.add(str(sym).upper())
        except Exception as se:
            print(f"[SCREENER_HUB][SCHED] cr_dynamic social load error: {se}")

        try:
            for fname in [
                "options_master_lkg_v1.json",
                "options_lkg_v1_large_cap.json",
                "options_lkg_v1_small_cap.json",
            ]:
                op = Path(__file__).parent.parent / "data" / fname
                if op.exists():
                    d = json.loads(op.read_text())
                    for t in (d.get("tickers") or []):
                        sym = t.get("ticker") if isinstance(t, dict) else None
                        if sym:
                            options_set.add(str(sym).upper())
        except Exception as oe:
            print(f"[SCREENER_HUB][SCHED] cr_dynamic options load error: {oe}")

        out = generate_chain_reaction_weekly(
            social_symbols=social_set,
            options_symbols=options_set,
        )
        print(f"[SCREENER_HUB][SCHED] chain_reaction_dynamic done: {out}")
    except Exception as e:
        print(f"[SCREENER_HUB][SCHED] chain_reaction_dynamic error: {e}")


async def _run_screener_snapshot_rebuild() -> None:
    """Rebuild strategy screener snapshot from the fresh CR weekly data (30 candidates)."""
    print("[SCREENER_HUB][SCHED] screener_snapshot_rebuild starting")
    try:
        from services.playbook.strategy_screener.screener_service import generate_snapshot_from_cr
        snap = await generate_snapshot_from_cr(manual_override=False)
        count = (snap or {}).get("results_count", 0)
        print(f"[SCREENER_HUB][SCHED] screener_snapshot_rebuild done: {count} candidates")
    except Exception as e:
        print(f"[SCREENER_HUB][SCHED] screener_snapshot_rebuild error: {e}")


async def _run_bottlenecks_warm() -> None:
    from services.screener_hub_service import (
        rebuild_universe, warm_tab_fundamentals,
    )

    # ── Self-healing: regenerate CR weekly data if it is stale ───────────────
    # The chain_reaction_dynamic slot (Sun 2:15 ET) can be missed if the server
    # restarts after the 5-minute fire window. This guard ensures bottlenecks_warm
    # always has fresh CR data, even when the earlier slot was skipped.
    try:
        from services.chain_reaction_weekly_service import (
            get_latest_cr_weekly_output,
            generate_chain_reaction_weekly,
        )
        import json
        from pathlib import Path

        cr_row = get_latest_cr_weekly_output(max_age_days=7)
        if cr_row is None:
            print("[SCREENER_HUB][SCHED] bottlenecks_warm: CR output stale or missing — generating fresh CR data now")
            social_set: set = set()
            options_set: set = set()
            try:
                sp = Path(__file__).parent.parent / "data" / "x_consensus_weekly.json"
                if sp.exists():
                    d = json.loads(sp.read_text())
                    for item in (d.get("top_tickers") or []):
                        sym = item.get("symbol") if isinstance(item, dict) else None
                        if sym:
                            social_set.add(str(sym).upper())
            except Exception as _se:
                print(f"[SCREENER_HUB][SCHED] bottlenecks_warm social load error: {_se}")
            try:
                for fname in ["options_master_lkg_v1.json", "options_lkg_v1_large_cap.json", "options_lkg_v1_small_cap.json"]:
                    op = Path(__file__).parent.parent / "data" / fname
                    if op.exists():
                        d = json.loads(op.read_text())
                        for t in (d.get("tickers") or []):
                            sym = t.get("ticker") if isinstance(t, dict) else None
                            if sym:
                                options_set.add(str(sym).upper())
            except Exception as _oe:
                print(f"[SCREENER_HUB][SCHED] bottlenecks_warm options load error: {_oe}")

            cr_result = generate_chain_reaction_weekly(
                social_symbols=social_set,
                options_symbols=options_set,
            )
            print(f"[SCREENER_HUB][SCHED] bottlenecks_warm: inline CR generation done: {cr_result.get('status')} rows={cr_result.get('rows_written')}")
        else:
            print(f"[SCREENER_HUB][SCHED] bottlenecks_warm: CR output is fresh (generated_at={cr_row.get('generated_at')}) — skipping inline regeneration")
    except Exception as _cr_err:
        print(f"[SCREENER_HUB][SCHED] bottlenecks_warm CR self-heal error (non-fatal): {_cr_err}")

    # ── Refresh universe snapshot then warm fundamentals ──────────────────────
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


async def _run_watchlist_stage2_warm() -> None:
    """Daily off-hours: fetch bars + compute Weinstein stage for all watchlist tickers."""
    print("[SCREENER_HUB][SCHED] watchlist stage2 warm starting")
    try:
        from services.watchlist_stage2_service import warmup_stage2_all_watchlists
        result = await warmup_stage2_all_watchlists()
        print(f"[SCREENER_HUB][SCHED] watchlist stage2 warm done: {result}")
    except Exception as exc:
        print(f"[SCREENER_HUB][SCHED] watchlist stage2 warm error: {exc}")


_SLOTS: list[tuple[Callable[[int], bool], int, int, str, Callable[[], Awaitable[None]]]] = [
    (_is_sunday,    0, 30, "thematic_rebuild",          _run_thematic_rebuild),
    (_is_sunday,    1, 15, "thematic_warm",             _run_thematic_warm),
    (_is_sunday,    1, 45, "returns_warm",              _run_returns_warm),
    (_is_sunday,    2, 15, "chain_reaction_dynamic",    _run_chain_reaction_dynamic),
    (_is_sunday,    3, 15, "bottlenecks_warm",          _run_bottlenecks_warm),
    (_is_sunday,    3, 45, "screener_snapshot_rebuild", _run_screener_snapshot_rebuild),
    (_not_saturday, 11, 10, "social_scan",              _run_social_scan),
    (_not_saturday, 11, 45, "social_warm",              _run_social_warm),
    (_is_friday,    2,  0, "watchlist_portfolio_warm",  _run_watchlist_portfolio_warm),
    # Daily 3:30 AM ET — Weinstein stage2 for all watchlist tickers.
    # Off-hours only: bars are daily-precision, no intraday updates needed.
    (_not_saturday, 3, 30, "watchlist_stage2_warm",    _run_watchlist_stage2_warm),
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
