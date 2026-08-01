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

# Tabs that support a broad rolling forward horizon.  These tabs store
# an additive `events` collection spanning past_days→future_days so
# future-week and Calendar Month views have data before the weekly
# snapshot window rotates.
_TABS_WITH_HORIZON: frozenset[str] = frozenset({"economic_releases"})

_HORIZON_PAST_DAYS   = 14
_HORIZON_FUTURE_DAYS = 89

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
    evts = snap.get("events") or []
    print(
        f"[calendar_snapshot] neon read tab={tab} status={snap.get('status')} "
        f"current_week={len(cw)} previous_week={len(pw)} events={len(evts)} "
        f"last_updated={snap.get('last_updated')}"
    )
    return {
        "current_week": cw,
        "previous_week": pw,
        "events": evts,
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
    evts = slot.get("events") or []
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
        events=evts,
    )
    print(
        f"[calendar_snapshot] neon write tab={tab} ok={ok} status={status} "
        f"current_week={len(cw)} previous_week={len(pw)} events={len(evts)}"
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
    return {"current_week": [], "previous_week": [], "events": [], "meta": {"last_updated": None, "status": "empty"}}


def _normalize_slot(slot: Any) -> dict:
    if not isinstance(slot, dict):
        return _empty_slot()
    slot.setdefault("current_week", [])
    slot.setdefault("previous_week", [])
    slot.setdefault("events", [])
    meta = slot.setdefault("meta", {})
    meta.setdefault("last_updated", None)
    meta.setdefault("status", "empty")
    return slot


# ── Public read API ─────────────────────────────────────────────────────────

def get_snapshot(tab: str) -> dict:
    """
    Return the response envelope for a target tab. Never triggers FMP.
    Reads Neon first; falls back to disk JSON if Neon is unavailable.

    Envelope includes:
      current_week, previous_week, last_updated, status (existing fields)
      is_stale   — True if the stored window does not cover the current ET week
      window     — requested vs. stored date ranges
      diagnostics — per-tab metrics for debugging
      events     — broad event collection (horizon tabs only)
      horizon    — rolling-horizon metadata (horizon tabs only)
      coverage   — horizon-completeness info
    """
    slot: Optional[dict] = _neon_read(tab)
    source = "neon"
    if slot is None:
        store = _read_disk()
        slot = _normalize_slot(store.get(tab))
        source = "disk"

    cw = slot.get("current_week") or []
    pw = slot.get("previous_week") or []
    evts = slot.get("events") or []
    meta = (slot.get("meta") or {})
    last_updated = meta.get("last_updated")
    stored_window = meta.get("window") or {}

    # Compute current ET Mon–Fri window for staleness and diagnostics.
    # ALL tabs (including economic_releases and treasury_macro) use the
    # same Mon–Fri window. Wider lookback belongs only in Recent view.
    monday = _et_week_monday()
    friday = monday + timedelta(days=4)
    req_from = monday
    req_to   = friday
    req_from_str = req_from.strftime("%Y-%m-%d")
    req_to_str   = req_to.strftime("%Y-%m-%d")

    is_stale = _snapshot_is_stale(slot, tab)

    # If broad horizon events exist, derive current_week from them (preferred).
    # This ensures the week view reflects the freshest canonical source.
    # Fall back to existing current_week / previous_week for old snapshots.
    if evts and tab in _TABS_WITH_HORIZON:
        cw = _select_events_for_window(evts, req_from_str, req_to_str)
        prev_from, prev_to = _previous_week_window_for(tab)
        pw = _select_events_for_window(evts, prev_from, prev_to)

    # Cache age in hours
    cache_age_hours: Optional[float] = None
    if last_updated:
        try:
            lu_dt = datetime.fromisoformat(last_updated.replace("Z", "+00:00"))
            cache_age_hours = round((datetime.now(timezone.utc) - lu_dt).total_seconds() / 3600, 1)
        except Exception:
            pass

    if cw:
        status = "ready"
    elif pw:
        status = "stale"
        cw = pw   # serve previous as current for backward compat
    else:
        status = "empty"

    # Build diagnostics block
    diag_cw = cw  # the list being served
    sample_titles = []
    for ev in diag_cw[:5]:
        t = (ev.get("display_title") or ev.get("title") or
             ev.get("eventName") or ev.get("companyName") or "")
        if t:
            sample_titles.append(t)

    diagnostics = {
        "requested_from":    req_from_str,
        "requested_to":      req_to_str,
        "stored_from":       stored_window.get("from"),
        "stored_to":         stored_window.get("to"),
        "is_stale":          is_stale,
        "source":            source,
        "cache_age_hours":   cache_age_hours,
        "cache_hit":         True,          # snapshot read = always a cache hit
        "event_count":       len(diag_cw),
        "min_event_date":    min((e.get("date", "") for e in diag_cw), default=None) or None,
        "max_event_date":    max((e.get("date", "") for e in diag_cw), default=None) or None,
        "sample_titles":     sample_titles,
    }

    # Horizon metadata (additive — presented for horizon tabs only)
    horizon: dict[str, Any] = {}
    if evts and tab in _TABS_WITH_HORIZON:
        horizon = {
            "horizon_start": meta.get("horizon_start") or stored_window.get("from"),
            "horizon_end":   meta.get("horizon_end")   or stored_window.get("to"),
            "past_days":     meta.get("past_days", _HORIZON_PAST_DAYS),
            "future_days":   meta.get("future_days", _HORIZON_FUTURE_DAYS),
            "event_count":   meta.get("event_count", len(evts)),
        }

    coverage: dict[str, Any] = {}
    if tab in _TABS_WITH_HORIZON:
        horizon_end = meta.get("horizon_end") or stored_window.get("to") or ""
        coverage["complete"] = (
            bool(horizon_end) and horizon_end >= req_to_str
        ) if evts else False
        coverage["horizon_end"]   = horizon_end
        coverage["requested_end"] = req_to_str

    envelope = {
        "current_week":  diag_cw,
        "previous_week": pw if not (status == "stale") else pw,
        "last_updated":  last_updated,
        "status":        status,
        "is_stale":      is_stale,
        "window": {
            "requested_from": req_from_str,
            "requested_to":   req_to_str,
            "stored_from":    stored_window.get("from"),
            "stored_to":      stored_window.get("to"),
        },
        "diagnostics": diagnostics,
    }

    # Additive horizon fields (only for horizon tabs)
    if evts and tab in _TABS_WITH_HORIZON:
        envelope["events"]  = evts
        envelope["horizon"] = horizon

    envelope["coverage"] = coverage

    print(
        f"[calendar_snapshot] get_snapshot tab={tab} source={source} "
        f"status={status} is_stale={is_stale} "
        f"current_week={len(diag_cw)} events={len(evts)} "
        f"stored_from={stored_window.get('from')!r} req_from={req_from_str!r}"
    )
    return envelope


def get_read_source(tab: str) -> dict:
    """
    Diagnostic helper: report which backing store the read path resolves to
    for a given tab, without mutating state. Used by the debug endpoint.

    Returns a dict with:
      - source: "neon" | "disk_fallback" | "empty"
      - status: snapshot status if known
      - current_week_count / previous_week_count
      - last_updated
      - neon_error: optional string if Neon read raised
    """
    info: dict[str, Any] = {
        "source": "empty",
        "status": "empty",
        "current_week_count": 0,
        "previous_week_count": 0,
        "events_count": 0,
        "last_updated": None,
    }
    slot: Optional[dict] = None
    try:
        slot = _neon_read(tab)
    except Exception as e:
        info["neon_error"] = str(e)
        slot = None

    if slot is not None:
        info["source"] = "neon"
    else:
        try:
            store = _read_disk()
            disk_slot = store.get(tab)
            if disk_slot:
                slot = _normalize_slot(disk_slot)
                info["source"] = "disk_fallback"
        except Exception as e:
            info["disk_error"] = str(e)

    if slot is not None:
        cw = slot.get("current_week") or []
        pw = slot.get("previous_week") or []
        evts = slot.get("events") or []
        meta = slot.get("meta") or {}
        info["current_week_count"] = len(cw)
        info["previous_week_count"] = len(pw)
        info["events_count"] = len(evts)
        info["last_updated"] = meta.get("last_updated")
        if cw:
            info["status"] = "ready"
        elif pw:
            info["status"] = "stale"
        else:
            info["status"] = meta.get("status") or "empty"
    return info


# ── Refresh / write path ────────────────────────────────────────────────────

def _et_week_monday() -> "date":
    """Return Monday of the current week in America/New_York timezone."""
    return _et_now().date() - timedelta(days=_et_now().date().weekday())


def _snapshot_is_stale(slot: Optional[dict], tab: str) -> bool:
    """
    Return True if the slot's stored window does not cover the current ET week.

    Rules:
    • No slot or empty current_week → always stale.
    • For horizon tabs: if events exist, staleness is driven ONLY by
      horizon_end covering the current ET Friday.  A snapshot with a recent
      last_updated but an insufficient horizon_end is stale.  A horizon tab
      that covers Friday is NOT stale even though its stored window.from is
      the horizon start (today - past_days), which is intentionally before
      the current week Monday.
    • For all other tabs: expected window starts on ET Monday (the current
      Mon–Fri calendar week). Stale if stored_from is before that date.
    Do NOT compare against last_updated / snapshot date — compare only
    against the window.from date that was recorded when the data was fetched.
    """
    if not slot:
        return True
    cw = slot.get("current_week") or []
    meta = slot.get("meta") or {}
    window = meta.get("window") or {}
    stored_from = window.get("from", "")

    monday = _et_week_monday()
    friday = monday + timedelta(days=4)
    expected_from = monday

    # Horizon-tab coverage check: must also cover the current Friday.
    if tab in _TABS_WITH_HORIZON:
        evts = slot.get("events") or []
        if evts:
            horizon_end = meta.get("horizon_end") or ""
            if not horizon_end or horizon_end < friday.strftime("%Y-%m-%d"):
                return True
            # Rolling horizon covers Friday → fresh.  For horizon tabs,
            # stored_from is the horizon START (today - past_days), not the
            # current week Monday, so the legacy week-rotation comparison
            # below does NOT apply here.  A complete horizon means the
            # current week is always within the cached collection.
            return False
        # No events — fall through to legacy check.
        if not cw:
            return True

    if not stored_from:
        return True
    return stored_from < expected_from.strftime("%Y-%m-%d")


def _week_window_for(tab: str) -> tuple[str, str]:
    """
    Date range for a single week's worth of fresh data for the given tab.
    Uses America/New_York timezone and a Monday–Friday window.

    ALL tabs (including economic_releases and treasury_macro) use the
    current ET Mon–Fri window. Wider lookback for "Recent" view is handled
    separately by _previous_week_window_for.
    """
    monday = _et_week_monday()
    friday = monday + timedelta(days=4)
    return monday.strftime("%Y-%m-%d"), friday.strftime("%Y-%m-%d")


def _previous_week_window_for(tab: str) -> tuple[str, str]:
    """
    Date range for the prior week (Recent view seed). Used by refresh_tab to
    seed previous_week directly when there is no prior snapshot to promote.

    Securities tabs (dividends/ipos/splits): Mon–Fri one week before the
    current week. Economic releases: a 7-day backward window relative to last
    Monday. Treasury_macro is excluded — its FMP feed is point-in-time.
    """
    monday = _et_week_monday()
    last_monday   = monday - timedelta(days=7)
    last_friday   = monday - timedelta(days=3)
    if tab == "economic_releases":
        start = last_monday - timedelta(days=7)
        end   = last_friday
    else:
        start = last_monday
        end   = last_friday
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def _horizon_window_for() -> tuple[str, str]:
    """Rolling forward horizon: 14d back, 89d forward (103d total span)."""
    today = _et_now().date()
    from_date = today - timedelta(days=_HORIZON_PAST_DAYS)
    to_date   = today + timedelta(days=_HORIZON_FUTURE_DAYS)
    return from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d")


def _horizon_is_complete(slot: Optional[dict], requested_end: str) -> bool:
    """
    Return True if the stored horizon covers at least through `requested_end`.
    A snapshot without the events field is considered incomplete for horizon purposes.
    """
    if not slot:
        return False
    evts = slot.get("events") or []
    if not evts:
        return False
    meta = slot.get("meta") or {}
    horizon_end = meta.get("horizon_end") or ""
    if not horizon_end:
        return False
    return horizon_end >= requested_end


def _select_events_for_window(events: list[dict], from_date: str, to_date: str) -> list[dict]:
    """Return events whose date falls inside [from_date, to_date] inclusive.
    Compares only the first 10 characters (YYYY-MM-DD) to handle datetime strings."""
    return [
        e for e in events
        if from_date <= (e.get("date") or "")[:10] <= to_date
    ]


async def refresh_tab(tab: str, fmp_key: str) -> dict:
    """
    Run the existing FMP fetcher for `tab`, promote current→previous,
    save the new current_week and meta. Persists to Neon (source of truth)
    and best-effort to disk (emergency fallback). Returns the new envelope.

    For tabs in _TABS_WITH_HORIZON, fetches a broad rolling horizon
    (14d back, 89d forward), stores all events, and derives current_week
    / previous_week from that collection.
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

    use_horizon = tab in _TABS_WITH_HORIZON

    if use_horizon:
        fetch_from, fetch_to = _horizon_window_for()
    else:
        fetch_from, fetch_to = _week_window_for(tab)

    t0 = time.monotonic()
    try:
        events, err = await _fetch_tab(
            fmp, tab, fetch_from, fetch_to, watchlist, portfolio,
            limit=1000, mode="upcoming",
        )
    except Exception as e:
        print(f"[calendar_snapshot] refresh_tab({tab}) fetch error: {e}")
        events, err = [], str(e)

    ms = int((time.monotonic() - t0) * 1000)

    # Post-filter to the requested window. Some fetchers (e.g. treasury_macro)
    # ignore date params and return historical rows beyond the requested range;
    # those belong in previous_week (Recent view), not current_week (Week view).
    before_filter = len(events)
    events = [
        e for e in events
        if fetch_from <= (e.get("date") or "")[:10] <= fetch_to
    ]
    if len(events) != before_filter:
        print(
            f"[calendar_snapshot] refresh tab={tab} date-filter: "
            f"{before_filter} → {len(events)} (dropped {before_filter - len(events)} outside {fetch_from}→{fetch_to})"
        )

    print(
        f"[calendar_snapshot] refresh tab={tab} window={fetch_from}→{fetch_to} "
        f"events={len(events)} err={err} ms={ms}"
    )

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

    if use_horizon:
        # ── Rolling horizon: derive current / previous week from the broad
        #     collection.  Prior-slot promotion is not needed — one fetch
        #     covers the full horizon.  Preserve prior horizon on fetch error.
        all_events = events or []
        monday_str, friday_str = _week_window_for(tab)
        cw_events = _select_events_for_window(all_events, monday_str, friday_str)

        prev_from, prev_to = _previous_week_window_for(tab)
        pw_events = _select_events_for_window(all_events, prev_from, prev_to)

        # On fetch error, keep prior valid horizon so consumers never see
        # a suddenly empty state.
        if err and prior:
            prior_evts = prior.get("events") or []
            if prior_evts:
                print(
                    f"[calendar_snapshot] refresh tab={tab} fetch error — "
                    f"preserving prior horizon events={len(prior_evts)}"
                )
                all_events = prior_evts
                cw_events = _select_events_for_window(all_events, monday_str, friday_str)
                pw_events = _select_events_for_window(all_events, prev_from, prev_to)

        promoted_previous = pw_events or prior_previous or []
        prev_events: list[dict] = []

        horizon_start = fetch_from
        horizon_end   = fetch_to
    else:
        # ── Existing per-week flow (non-horizon tabs) ──────────────────────
        all_events = events or []
        promoted_previous = prior_current or prior_previous or []

        # Seed previous_week directly when there is nothing to promote. This
        # handles the first refresh ever (and any subsequent refresh that lost
        # both slots) so the Recent tab is non-empty without waiting an extra
        # week. Treasury_macro is intentionally skipped — its feed is
        # point-in-time and a prior-week range is meaningless. Done outside the
        # lock to avoid holding it across an HTTP fetch.
        prev_events = []
        if not promoted_previous and tab != "treasury_macro":
            pw_from, pw_to = _previous_week_window_for(tab)
            t1 = time.monotonic()
            try:
                prev_events, prev_err = await _fetch_tab(
                    fmp, tab, pw_from, pw_to, watchlist, portfolio,
                    limit=1000, mode="upcoming",
                )
            except Exception as e:
                print(f"[calendar_snapshot] previous_week seed fetch error tab={tab}: {e}")
                prev_events, prev_err = [], str(e)
            print(
                f"[calendar_snapshot] seed previous_week tab={tab} "
                f"window={pw_from}→{pw_to} events={len(prev_events)} "
                f"err={prev_err} ms={int((time.monotonic() - t1) * 1000)}"
            )

        horizon_start = fetch_from
        horizon_end   = fetch_to

    async with _lock:
        if use_horizon:
            new_previous = promoted_previous or []
            new_current = cw_events or []
        else:
            new_previous = promoted_previous or prev_events or []
            new_current = events or []

        new_meta: dict[str, Any] = {
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "status":       "ready" if new_current else (
                "stale" if new_previous else "empty"
            ),
            "window":       {
                "from": fetch_from,
                "to":   fetch_to,
            },
            "fetch_error":  err,
            # Preserve schedule marker if it was set previously.
            **{k: v for k, v in (prior.get("meta") or {}).items()
               if k in ("last_run_week",)},
        }

        if use_horizon:
            new_meta["horizon_start"] = horizon_start
            new_meta["horizon_end"]   = horizon_end
            new_meta["past_days"]     = _HORIZON_PAST_DAYS
            new_meta["future_days"]   = _HORIZON_FUTURE_DAYS
            new_meta["event_count"]   = len(all_events)

        new_slot = {
            "current_week": new_current,
            "previous_week": new_previous,
            "events": all_events if use_horizon else [],
            "meta": new_meta,
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


async def check_and_refresh_stale(fmp_key: str, delay_secs: int = 45) -> None:
    """
    Startup task: wait `delay_secs` then refresh any tab whose snapshot is
    stale (stored window doesn't cover the current ET week).  Safe to run
    concurrently with weekly_scheduler_loop — the async lock in refresh_tab
    prevents duplicate writes.

    Call as `asyncio.create_task(check_and_refresh_stale(fmp_key))` from
    the application lifespan hook.
    """
    if not fmp_key:
        print("[calendar_snapshot] check_and_refresh_stale: no FMP key, skipping")
        return
    await asyncio.sleep(delay_secs)
    print(f"[calendar_snapshot] startup stale-check: et_monday={_et_week_monday()}")
    for tab in TARGET_TABS:
        try:
            slot = _neon_read(tab)
            if slot is None:
                store = _read_disk()
                slot = _normalize_slot(store.get(tab))
            if _snapshot_is_stale(slot, tab):
                stored_from = ((slot.get("meta") or {}).get("window") or {}).get("from", "N/A")
                print(
                    f"[calendar_snapshot] startup stale-check: tab={tab} STALE "
                    f"(stored_from={stored_from!r}) — refreshing now"
                )
                await refresh_tab(tab, fmp_key)
            else:
                stored_from = ((slot.get("meta") or {}).get("window") or {}).get("from", "N/A")
                print(f"[calendar_snapshot] startup stale-check: tab={tab} current (stored_from={stored_from!r})")
        except Exception as e:
            print(f"[calendar_snapshot] startup stale-check error tab={tab}: {e}")


async def weekly_scheduler_loop(fmp_key_provider) -> None:
    """
    Background loop. Pass a callable returning the FMP key (so a missing key
    at startup can still recover later without restart).

    Two firing modes:
    1. Sunday scheduled refresh (existing): fires each tab at its configured
       ET hour once per ISO week (last_run_week marker prevents re-runs).
    2. Daily stale-check (new): Mon–Sat, every 60 minutes, refreshes any tab
       whose stored window doesn't cover the current ET week.  Uses a
       "stale:<week>" marker so each tab is refreshed at most once per week
       outside the Sunday window.
    """
    print(f"[calendar_snapshot] scheduler loop started; tabs={TARGET_TABS}")

    while True:
        try:
            now_et = _et_now()
            week_marker = _iso_year_week(now_et)

            # ── Sunday: scheduled full refresh ────────────────────────────────
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

            # ── Mon–Sat: stale-tab check (hourly cadence via sleep below) ────
            else:
                stale_marker = f"stale:{week_marker}"
                for tab in TARGET_TABS:
                    if _last_run_marker(tab) in (week_marker, stale_marker):
                        continue  # already refreshed this week (Sunday run or prior stale check)
                    try:
                        slot = _neon_read(tab)
                        if slot is None:
                            store = _read_disk()
                            slot = _normalize_slot(store.get(tab))
                        if _snapshot_is_stale(slot, tab):
                            fmp_key = fmp_key_provider() if callable(fmp_key_provider) else fmp_key_provider
                            if not fmp_key:
                                continue
                            stored_from = ((slot.get("meta") or {}).get("window") or {}).get("from", "N/A")
                            print(
                                f"[calendar_snapshot] stale-check (daily) firing "
                                f"tab={tab} stored_from={stored_from!r} "
                                f"et={now_et.isoformat()}"
                            )
                            await refresh_tab(tab, fmp_key)
                            _set_last_run_marker(tab, stale_marker)
                    except Exception as e:
                        print(f"[calendar_snapshot] stale-check error tab={tab}: {e}")

        except Exception as e:
            print(f"[calendar_snapshot] scheduler loop error: {e}")

        # Sleep ~60 minutes Mon–Sat, ~1 min on Sunday (to hit hourly slots).
        try:
            now_et = _et_now()
            if now_et.weekday() == 6:
                secs_to_next_min = 60 - now_et.second
                await asyncio.sleep(max(5, secs_to_next_min))
            else:
                await asyncio.sleep(3600)
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
