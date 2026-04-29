"""
Calendar Snapshot Service — persistent weekly cache for catalyst calendar tabs.

Target tabs: dividends, ipos, splits, economic_releases, treasury_macro.

Read path (request/route)
─────────────────────────
Routes for the target tabs read ONLY from this snapshot. They never call FMP
on a request. The service exposes get_snapshot(tab) which returns:

    {
      "current_week":  [...events...],
      "previous_week": [...events...],
      "last_updated":  iso8601 | null,
      "status":        "ready" | "stale" | "empty",
    }

Fallback rules:
  • current_week populated  → status="ready"
  • only previous_week      → return previous_week as current_week, status="stale"
  • neither                 → empty arrays, status="empty"

Write path (scheduler)
──────────────────────
refresh_tab(tab, fmp_key) is called by the weekly scheduler in lifespan.
It uses the EXISTING fetchers in services.catalyst_calendar_service (no new
FMP code), promotes current_week → previous_week, stores fresh events as
current_week, writes meta.last_updated, and persists to disk.

Persistence
───────────
File: data/calendar_snapshots.json (follows smart_earnings_scanner.CACHE_FILE
precedent — single JSON dict, atomic-ish overwrite, corrupt/missing safe).
"""
from __future__ import annotations

import asyncio
import json
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

# Anchor the snapshot file to an absolute path derived from this module's
# location so the write path (manual backfill, scheduler) and read path (API
# request handler) cannot diverge based on the process's current working
# directory. backend/services/calendar_snapshot_service.py → backend/ →
# backend/data/calendar_snapshots.json.
_BACKEND_DIR = Path(__file__).resolve().parent.parent
SNAPSHOT_PATH: Path = _BACKEND_DIR / "data" / "calendar_snapshots.json"
# Backwards-compatible alias for any external reference.
CACHE_FILE = SNAPSHOT_PATH


def get_snapshot_path() -> Path:
    """Single source of truth for the snapshot file location (absolute)."""
    return SNAPSHOT_PATH

TARGET_TABS: list[str] = [
    "dividends",
    "ipos",
    "splits",
    "economic_releases",
    "treasury_macro",
]

_lock = asyncio.Lock()


def _read_disk() -> dict:
    path = get_snapshot_path()
    print(f"[calendar_snapshot] read path={path} exists={path.exists()}")
    if path.exists():
        try:
            with open(path, "r") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
        except Exception as e:
            print(f"[calendar_snapshot] read error: {e} — starting empty")
    return {}


def _write_disk(data: dict) -> None:
    path = get_snapshot_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        with open(tmp, "w") as f:
            json.dump(data, f, separators=(",", ":"))
        tmp.replace(path)
        print(f"[calendar_snapshot] write path={path} bytes={path.stat().st_size}")
    except Exception as e:
        print(f"[calendar_snapshot] write error path={path}: {e}")


def _empty_tab() -> dict:
    return {"current_week": [], "previous_week": [], "meta": {"last_updated": None, "status": "empty"}}


def _ensure_tab(store: dict, tab: str) -> dict:
    if tab not in store or not isinstance(store.get(tab), dict):
        store[tab] = _empty_tab()
    else:
        slot = store[tab]
        slot.setdefault("current_week", [])
        slot.setdefault("previous_week", [])
        meta = slot.setdefault("meta", {})
        meta.setdefault("last_updated", None)
        meta.setdefault("status", "empty")
    return store[tab]


def get_snapshot(tab: str) -> dict:
    """
    Return the response envelope for a target tab. Never triggers FMP.
    """
    store = _read_disk()
    slot = _ensure_tab(store, tab)
    cw = slot.get("current_week") or []
    pw = slot.get("previous_week") or []
    last_updated = (slot.get("meta") or {}).get("last_updated")

    if cw:
        return {
            "current_week":  cw,
            "previous_week": pw,
            "last_updated":  last_updated,
            "status":        "ready",
        }
    if pw:
        # Expose previous_week data as current_week for graceful display,
        # mark stale so the frontend can warn.
        return {
            "current_week":  pw,
            "previous_week": pw,
            "last_updated":  last_updated,
            "status":        "stale",
        }
    return {
        "current_week":  [],
        "previous_week": [],
        "last_updated":  last_updated,
        "status":        "empty",
    }


def _week_window_for(tab: str) -> tuple[str, str]:
    """
    Date range for a single week's worth of fresh data for the given tab.

    Uses Sunday→Saturday for tabs that align to a calendar week. For
    economic_releases and treasury_macro we use a wider centred window
    (existing service default) so the snapshot still contains meaningful
    rows even when the calendar week itself is sparse.
    """
    today = datetime.now(timezone.utc).date()
    days_since_sunday = (today.weekday() + 1) % 7  # Sun=0 in this scheme
    sunday = today - timedelta(days=days_since_sunday)
    saturday = sunday + timedelta(days=6)
    if tab in ("economic_releases", "treasury_macro"):
        # Fetch a bit either side so the snapshot includes adjacent releases
        # the user expects to see in the current-week view.
        start = sunday - timedelta(days=7)
        end   = saturday + timedelta(days=14)
    else:
        start = sunday
        end   = saturday
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


async def refresh_tab(tab: str, fmp_key: str) -> dict:
    """
    Run the existing FMP fetcher for `tab`, promote current→previous,
    save the new current_week and meta. Returns the new envelope.
    """
    if tab not in TARGET_TABS:
        raise ValueError(f"refresh_tab: unsupported tab {tab!r}")
    if not fmp_key:
        print(f"[calendar_snapshot] refresh_tab({tab}) skipped: missing FMP key")
        return get_snapshot(tab)

    # Import lazily to avoid circular imports during module load.
    # NOTE: We intentionally do NOT import _enrich_profiles / _apply_enrichment.
    # Snapshot refresh (scheduler + manual backfill) must skip per-symbol FMP
    # profile calls — those caused 429 rate-limit storms (e.g. dividends with
    # hundreds of symbols) that prevented other tabs from refreshing. The base
    # tab fetchers already supply the visible fields the frontend needs
    # (symbol, date, dividend amount, IPO price range, split ratio, etc.).
    from services.catalyst_calendar_service import (
        CatalystFMP,
        _fetch_tab,
        _load_watchlist_symbols,
        _load_portfolio_symbols,
    )

    fmp = CatalystFMP(fmp_key)
    watchlist = _load_watchlist_symbols()
    portfolio = _load_portfolio_symbols()

    from_date, to_date = _week_window_for(tab)
    t0 = time.monotonic()
    try:
        events, err = await _fetch_tab(
            fmp, tab, from_date, to_date, watchlist, portfolio,
            limit=1000, mode="upcoming",
        )
    except Exception as e:
        print(f"[calendar_snapshot] refresh_tab({tab}) fetch error: {e}")
        events, err = [], str(e)

    ms = int((time.monotonic() - t0) * 1000)
    print(
        f"[calendar_snapshot] refresh tab={tab} window={from_date}→{to_date} "
        f"events={len(events)} err={err} ms={ms}"
    )

    async with _lock:
        store = _read_disk()
        slot = _ensure_tab(store, tab)
        # Promote: current_week → previous_week, then write new current.
        slot["previous_week"] = slot.get("current_week") or slot.get("previous_week") or []
        slot["current_week"]  = events or []
        slot["meta"] = {
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "status":       "ready" if events else ("stale" if slot["previous_week"] else "empty"),
            "window":       {"from": from_date, "to": to_date},
            "fetch_error":  err,
        }
        store[tab] = slot
        _write_disk(store)

    return get_snapshot(tab)


# ── Scheduler ────────────────────────────────────────────────────────────────
# Simple background loop. Avoids new infra (no APScheduler dep). Wakes hourly
# and runs a refresh job for any (Sunday, hour) slot that hasn't run yet this
# week. Schedule (America/New_York):
#   01:00 dividends, 02:00 ipos, 03:00 splits,
#   04:00 economic_releases, 05:00 treasury_macro

# (tab, hour_local_et, minute) — minute=0 keeps the spec; hour is ET.
_SCHEDULE: list[tuple[str, int, int]] = [
    ("dividends",         1, 0),
    ("ipos",              2, 0),
    ("splits",            3, 0),
    ("economic_releases", 4, 0),
    ("treasury_macro",    5, 0),
]


def _et_now() -> datetime:
    try:
        from zoneinfo import ZoneInfo  # py3.9+
    except ImportError:  # pragma: no cover
        from backports.zoneinfo import ZoneInfo  # type: ignore
    return datetime.now(ZoneInfo("America/New_York"))


def _iso_year_week(dt: datetime) -> str:
    iso = dt.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def _last_run_marker(tab: str) -> Optional[str]:
    store = _read_disk()
    slot = store.get(tab) or {}
    return ((slot.get("meta") or {}).get("last_run_week"))


def _set_last_run_marker(tab: str, week_marker: str) -> None:
    store = _read_disk()
    slot = _ensure_tab(store, tab)
    meta = slot.setdefault("meta", {})
    meta["last_run_week"] = week_marker
    store[tab] = slot
    _write_disk(store)


async def weekly_scheduler_loop(fmp_key_provider) -> None:
    """
    Background loop. Pass a callable returning the FMP key (so a missing key
    at startup can still recover later without restart).

    Runs every Sunday in America/New_York at the per-tab hour from _SCHEDULE.
    Uses a per-tab `last_run_week` marker (ISO yr-week) so a process restart
    won't double-fetch and a missed slot will be picked up on the next loop.

    Important: this loop does NOT fetch on startup for these target tabs.
    It only fires inside the Sunday window.
    """
    print(f"[calendar_snapshot] scheduler loop started; tabs={TARGET_TABS}")

    while True:
        try:
            now_et = _et_now()
            week_marker = _iso_year_week(now_et)
            # Only fire on Sunday (weekday()==6) and only inside each tab's hour
            if now_et.weekday() == 6:
                for tab, hour, minute in _SCHEDULE:
                    if now_et.hour != hour:
                        continue
                    if now_et.minute < minute:
                        continue
                    if _last_run_marker(tab) == week_marker:
                        continue
                    fmp_key = fmp_key_provider() if callable(fmp_key_provider) else fmp_key_provider
                    if not fmp_key:
                        print(f"[calendar_snapshot] scheduler: missing FMP key, skipping {tab}")
                        continue
                    try:
                        print(f"[calendar_snapshot] scheduler firing tab={tab} et={now_et.isoformat()}")
                        await refresh_tab(tab, fmp_key)
                        _set_last_run_marker(tab, week_marker)
                    except Exception as e:
                        print(f"[calendar_snapshot] scheduler error tab={tab}: {e}")
        except Exception as e:
            print(f"[calendar_snapshot] scheduler loop error: {e}")

        # Sleep until the top of the next minute. Cheap polling — the heavy
        # work only happens when an hour slot matches.
        try:
            now_et = _et_now()
            secs_to_next_min = 60 - now_et.second
            await asyncio.sleep(max(5, secs_to_next_min))
        except Exception:
            await asyncio.sleep(60)


# ── Manual backfill (CLI-only) ───────────────────────────────────────────────
# Manual-only entrypoint to populate the weekly snapshots NOW for the five
# non-Earnings target tabs. Reuses refresh_tab — the same function the Sunday
# scheduler invokes — so there is no duplicate fetch logic. Earnings is NOT in
# TARGET_TABS and is therefore never touched by this command.
#
# This block runs only under `python -m services.calendar_snapshot_service`
# (or equivalent direct invocation). It is gated by `if __name__ == "__main__"`
# and is NEVER triggered on import, request, page load, or app startup.


async def _manual_backfill(tabs: list[str]) -> int:
    """
    Run refresh_tab for each requested tab sequentially. Returns a process
    exit code: 0 on full success, 1 if any tab failed or returned no events
    AND no fallback existed.
    """
    import os
    fmp_key = os.getenv("FMP_API_KEY")
    if not fmp_key:
        print("[backfill] ERROR: FMP_API_KEY not set in environment; aborting.")
        return 2

    failures: list[str] = []
    for tab in tabs:
        if tab not in TARGET_TABS:
            print(f"[backfill] skipping unsupported tab: {tab!r}")
            failures.append(tab)
            continue
        print(f"[backfill] → refreshing tab={tab} ...")
        try:
            envelope = await refresh_tab(tab, fmp_key)
            cw = len(envelope.get("current_week") or [])
            pw = len(envelope.get("previous_week") or [])
            status = envelope.get("status")
            last = envelope.get("last_updated")
            print(
                f"[backfill] ✓ tab={tab} status={status} "
                f"current_week={cw} previous_week={pw} last_updated={last}"
            )
            if status == "empty":
                failures.append(tab)
        except Exception as e:
            print(f"[backfill] ✗ tab={tab} ERROR: {e}")
            failures.append(tab)

    if failures:
        print(f"[backfill] DONE with failures: {failures}")
        return 1
    print(f"[backfill] DONE — all {len(tabs)} tabs refreshed successfully.")
    return 0


def _cli_main() -> int:
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        prog="python -m services.calendar_snapshot_service",
        description=(
            "Manual-only backfill for weekly catalyst calendar snapshots. "
            "Refreshes Dividends, IPOs, Splits, Economic Releases, and "
            "Treasury/Macro by calling the same refresh_tab() function the "
            "Sunday ET scheduler uses. Earnings is never touched. Requires "
            "FMP_API_KEY in the environment. Safe to re-run; idempotent."
        ),
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Run a manual backfill for the five target tabs.",
    )
    parser.add_argument(
        "--tabs",
        type=str,
        default=",".join(TARGET_TABS),
        help=(
            "Comma-separated subset of target tabs to backfill. "
            f"Default: {','.join(TARGET_TABS)}. "
            "Earnings is NOT a valid tab here and will never be touched."
        ),
    )
    parser.add_argument(
        "--list-tabs",
        action="store_true",
        help="Print the supported target tabs and exit (no fetches).",
    )
    args = parser.parse_args()

    if args.list_tabs:
        print("Target tabs (Earnings is intentionally excluded):")
        for t in TARGET_TABS:
            print(f"  - {t}")
        return 0

    if not args.backfill:
        parser.print_help()
        return 0

    requested = [t.strip() for t in args.tabs.split(",") if t.strip()]
    print(f"[backfill] manual run starting; tabs={requested}")
    return asyncio.run(_manual_backfill(requested))


if __name__ == "__main__":
    import sys, os
    # Ensure backend/ is on sys.path when invoked from project root
    backend_dir = os.path.dirname(os.path.abspath(__file__))
    while os.path.basename(backend_dir) != "backend" and backend_dir != "/":
        backend_dir = os.path.dirname(backend_dir)
    if backend_dir not in sys.path:
        sys.path.insert(0, backend_dir)

    sys.exit(_cli_main())
