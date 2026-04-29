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

Write path (scheduler / manual backfill)
────────────────────────────────────────
refresh_tab(tab, fmp_key) is called by the weekly scheduler in lifespan, or
manually via `python -m services.calendar_snapshot_service --backfill`.
It uses the EXISTING fetchers in services.catalyst_calendar_service (no new
FMP code), promotes current_week → previous_week, stores fresh events as
current_week, writes meta.last_updated, and persists to Neon Postgres.

Persistence
───────────
Source of truth: Neon Postgres table `public.calendar_snapshots`, accessed via
`data.pg_storage` (the same module used by chat history, watchlists, etc.).
This is required because the deployed backend's filesystem is ephemeral —
disk JSON written by a manual backfill does NOT persist into the API
container that serves /api/catalysts/events.

Disk JSON at backend/data/calendar_snapshots.json is an emergency-only
fallback: read if Neon is unreachable, written best-effort alongside Neon
writes so a local-dev process without DB credentials can still serve a
recent snapshot. Never the source of truth.
"""
from __future__ import annotations

import asyncio
import json
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

# Disk fallback path. Anchored to backend/ so writer and reader cannot diverge
# based on cwd. Used only as an emergency cache when Neon is unreachable.
_BACKEND_DIR = Path(__file__).resolve().parent.parent
SNAPSHOT_PATH: Path = _BACKEND_DIR / "data" / "calendar_snapshots.json"
# Backwards-compatible alias for any external reference.
CACHE_FILE = SNAPSHOT_PATH


def get_snapshot_path() -> Path:
    """Disk fallback file location (absolute). Neon is the source of truth."""
    return SNAPSHOT_PATH

TARGET_TABS: list[str] = [
    "dividends",
    "ipos",
    "splits",
    "economic_releases",
    "treasury_macro",
]

_lock = asyncio.Lock()


# ── Neon-backed primary persistence ─────────────────────────────────────────

def _neon_read(tab: str) -> Optional[dict]:
    """Read a snapshot row from Neon. Returns None if Neon unavailable or empty."""
    try:
        from data.pg_storage import calendar_snapshot_read
    except Exception as e:
        print(f"[calendar_snapshot] pg_storage import failed: {e}")
        return None
    snap = calendar_snapshot_read(tab)
    if snap is None:
        return None
    cw = snap.get("current_week") or []
    pw = snap.get("previous_week") or []
    print(
        f"[calendar_snapshot] neon read tab={tab} status={snap.get('status')} "
        f"current_week={len(cw)} previous_week={len(pw)} "
        f"last_updated={snap.get('last_updated')}"
    )
    return {
        "current_week": cw,
        "previous_week": pw,
        "meta": {
            "last_updated": snap.get("last_updated"),
            "status": snap.get("status") or "empty",
            **(snap.get("meta") or {}),
        },
    }


def _neon_write(tab: str, slot: dict) -> bool:
    """Persist a tab slot dict to Neon. Returns True on success."""
    try:
        from data.pg_storage import calendar_snapshot_write
    except Exception as e:
        print(f"[calendar_snapshot] pg_storage import failed: {e}")
        return False
    meta = slot.get("meta") or {}
    cw = slot.get("current_week") or []
    pw = slot.get("previous_week") or []
    status = meta.get("status") or "empty"
    last_updated = meta.get("last_updated")
    # Pass non-status meta fields through so window/fetch_error/last_run_week persist.
    extra_meta = {k: v for k, v in meta.items() if k not in ("last_updated", "status")}
    ok = calendar_snapshot_write(
        tab=tab,
        current_week=cw,
        previous_week=pw,
        last_updated=last_updated,
        status=status,
        meta=extra_meta,
    )
    print(
        f"[calendar_snapshot] neon write tab={tab} ok={ok} status={status} "
        f"current_week={len(cw)} previous_week={len(pw)}"
    )
    return ok


# ── Disk emergency fallback (NEVER source of truth) ─────────────────────────

def _read_disk() -> dict:
    path = get_snapshot_path()
    if path.exists():
        try:
            with open(path, "r") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    print(f"[calendar_snapshot] disk fallback read path={path} keys={list(data.keys())}")
                    return data
        except Exception as e:
            print(f"[calendar_snapshot] disk read error: {e} — starting empty")
    else:
        print(f"[calendar_snapshot] disk fallback miss path={path}")
    return {}


def _write_disk(data: dict) -> None:
    """Best-effort emergency cache write. Does not raise on failure."""
    path = get_snapshot_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        with open(tmp, "w") as f:
            json.dump(data, f, separators=(",", ":"))
        tmp.replace(path)
        print(f"[calendar_snapshot] disk fallback write path={path} bytes={path.stat().st_size}")
    except Exception as e:
        print(f"[calendar_snapshot] disk fallback write error path={path}: {e}")


def _empty_slot() -> dict:
    return {"current_week": [], "previous_week": [], "meta": {"last_updated": None, "status": "empty"}}


def _normalize_slot(slot: Any) -> dict:
    if not isinstance(slot, dict):
        return _empty_slot()
    slot.setdefault("current_week", [])
    slot.setdefault("previous_week", [])
    meta = slot.setdefault("meta", {})
    meta.setdefault("last_updated", None)
    meta.setdefault("status", "empty")
    return slot


# ── Public read API ─────────────────────────────────────────────────────────

def get_snapshot(tab: str) -> dict:
    """
    Return the response envelope for a target tab. Never triggers FMP.
    Reads Neon first; falls back to disk JSON if Neon is unavailable.
    """
    slot: Optional[dict] = _neon_read(tab)
    source = "neon"
    if slot is None:
        # Neon unreachable or row missing — try disk emergency fallback.
        store = _read_disk()
        slot = _normalize_slot(store.get(tab))
        source = "disk"

    cw = slot.get("current_week") or []
    pw = slot.get("previous_week") or []
    last_updated = (slot.get("meta") or {}).get("last_updated")

    if cw:
        envelope = {
            "current_week":  cw,
            "previous_week": pw,
            "last_updated":  last_updated,
            "status":        "ready",
        }
    elif pw:
        envelope = {
            "current_week":  pw,
            "previous_week": pw,
            "last_updated":  last_updated,
            "status":        "stale",
        }
    else:
        envelope = {
            "current_week":  [],
            "previous_week": [],
            "last_updated":  last_updated,
            "status":        "empty",
        }
    print(
        f"[calendar_snapshot] get_snapshot tab={tab} source={source} "
        f"status={envelope['status']} current_week={len(envelope['current_week'])} "
        f"previous_week={len(envelope['previous_week'])}"
    )
    return envelope


# ── Refresh / write path ────────────────────────────────────────────────────

def _week_window_for(tab: str) -> tuple[str, str]:
    """
    Date range for a single week's worth of fresh data for the given tab.
    """
    today = datetime.now(timezone.utc).date()
    days_since_sunday = (today.weekday() + 1) % 7  # Sun=0 in this scheme
    sunday = today - timedelta(days=days_since_sunday)
    saturday = sunday + timedelta(days=6)
    if tab in ("economic_releases", "treasury_macro"):
        start = sunday - timedelta(days=7)
        end   = saturday + timedelta(days=14)
    else:
        start = sunday
        end   = saturday
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


async def refresh_tab(tab: str, fmp_key: str) -> dict:
    """
    Run the existing FMP fetcher for `tab`, promote current→previous,
    save the new current_week and meta. Persists to Neon (source of truth)
    and best-effort to disk (emergency fallback). Returns the new envelope.
    """
    if tab not in TARGET_TABS:
        raise ValueError(f"refresh_tab: unsupported tab {tab!r}")
    if not fmp_key:
        print(f"[calendar_snapshot] refresh_tab({tab}) skipped: missing FMP key")
        return get_snapshot(tab)

    # NOTE: We intentionally do NOT import _enrich_profiles / _apply_enrichment.
    # Snapshot refresh must skip per-symbol FMP profile calls — those caused
    # 429 rate-limit storms (e.g. dividends with hundreds of symbols).
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
        # Read current row from Neon to figure out what to promote into previous_week.
        prior = _neon_read(tab)
        if prior is None:
            # No DB row yet (or Neon unreachable for read) — fall back to disk
            # for the prior, so we don't lose previous_week if Neon was just
            # transiently down.
            store = _read_disk()
            prior = _normalize_slot(store.get(tab))

        prior_current = prior.get("current_week") or []
        prior_previous = prior.get("previous_week") or []

        new_slot = {
            "current_week": events or [],
            "previous_week": prior_current or prior_previous or [],
            "meta": {
                "last_updated": datetime.now(timezone.utc).isoformat(),
                "status":       "ready" if events else (
                    "stale" if (prior_current or prior_previous) else "empty"
                ),
                "window":       {"from": from_date, "to": to_date},
                "fetch_error":  err,
                # Preserve schedule marker if it was set previously.
                **{k: v for k, v in (prior.get("meta") or {}).items()
                   if k in ("last_run_week",)},
            },
        }

        # Primary persistence: Neon.
        neon_ok = _neon_write(tab, new_slot)

        # Best-effort disk mirror so a local dev process can still serve a
        # snapshot if Neon is unavailable. NEVER source of truth.
        try:
            store = _read_disk()
            store[tab] = new_slot
            _write_disk(store)
        except Exception as e:
            print(f"[calendar_snapshot] disk mirror failed tab={tab}: {e}")

        if not neon_ok:
            print(
                f"[calendar_snapshot] WARN tab={tab} Neon write FAILED — "
                f"deployed API will not see this refresh until Neon is back"
            )

    return get_snapshot(tab)


# ── Scheduler ────────────────────────────────────────────────────────────────

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
    """Read schedule marker from Neon; fall back to disk if Neon unavailable."""
    try:
        from data.pg_storage import calendar_snapshot_get_meta_field
        marker = calendar_snapshot_get_meta_field(tab, "last_run_week")
        if marker is not None:
            return marker
    except Exception as e:
        print(f"[calendar_snapshot] last_run_marker neon read failed tab={tab}: {e}")
    store = _read_disk()
    slot = store.get(tab) or {}
    return ((slot.get("meta") or {}).get("last_run_week"))


def _set_last_run_marker(tab: str, week_marker: str) -> None:
    """Persist schedule marker to Neon and best-effort disk."""
    try:
        from data.pg_storage import calendar_snapshot_set_meta_field
        calendar_snapshot_set_meta_field(tab, "last_run_week", week_marker)
    except Exception as e:
        print(f"[calendar_snapshot] last_run_marker neon write failed tab={tab}: {e}")
    try:
        store = _read_disk()
        slot = _normalize_slot(store.get(tab))
        meta = slot.setdefault("meta", {})
        meta["last_run_week"] = week_marker
        store[tab] = slot
        _write_disk(store)
    except Exception:
        pass


async def weekly_scheduler_loop(fmp_key_provider) -> None:
    """
    Background loop. Pass a callable returning the FMP key (so a missing key
    at startup can still recover later without restart).
    """
    print(f"[calendar_snapshot] scheduler loop started; tabs={TARGET_TABS}")

    while True:
        try:
            now_et = _et_now()
            week_marker = _iso_year_week(now_et)
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

        try:
            now_et = _et_now()
            secs_to_next_min = 60 - now_et.second
            await asyncio.sleep(max(5, secs_to_next_min))
        except Exception:
            await asyncio.sleep(60)


# ── Manual backfill (CLI-only) ───────────────────────────────────────────────

async def _manual_backfill(tabs: list[str]) -> int:
    import os
    fmp_key = os.getenv("FMP_API_KEY")
    if not fmp_key:
        print("[backfill] ERROR: FMP_API_KEY not set in environment; aborting.")
        return 2

    # Best-effort: ensure the Neon table exists before we start, so the first
    # write isn't fighting an undefined-table self-heal under contention.
    try:
        from data.pg_storage import init_tables, is_available, get_last_conn_error
        if is_available():
            init_tables()
            print("[backfill] Neon connectivity OK; calendar_snapshots table ensured.")
        else:
            print(
                f"[backfill] WARN: Neon not available — backfill will only "
                f"update disk fallback. last_conn_error={get_last_conn_error()!r}"
            )
    except Exception as e:
        print(f"[backfill] WARN: Neon init check failed: {e}")

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

    parser = argparse.ArgumentParser(
        prog="python -m services.calendar_snapshot_service",
        description=(
            "Manual-only backfill for weekly catalyst calendar snapshots. "
            "Refreshes Dividends, IPOs, Splits, Economic Releases, and "
            "Treasury/Macro by calling the same refresh_tab() function the "
            "Sunday ET scheduler uses. Earnings is never touched. Requires "
            "FMP_API_KEY in the environment. Snapshots are persisted to Neon "
            "(NEON_DATABASE_URL); disk JSON is an emergency-only fallback."
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
    backend_dir = os.path.dirname(os.path.abspath(__file__))
    while os.path.basename(backend_dir) != "backend" and backend_dir != "/":
        backend_dir = os.path.dirname(backend_dir)
    if backend_dir not in sys.path:
        sys.path.insert(0, backend_dir)

    sys.exit(_cli_main())
