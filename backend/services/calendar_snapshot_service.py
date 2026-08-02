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

# Public alias for routes/consumers that need to know which tabs support
# requested-window serving (day / week / month) from the broad horizon.
HORIZON_TABS = _TABS_WITH_HORIZON

_HORIZON_PAST_DAYS   = 334  # FMP Starter ~11-month historical boundary (~Sep 2025)
_HORIZON_FUTURE_DAYS = 90   # 3 calendar months ahead

_lock = asyncio.Lock()

# Bootstrap state: True while a full first-run backfill is in progress for a
# given tab.  Used to prevent duplicate full backfill tasks and to report
# truthfully to the frontend.
_bootstrapping: dict[str, bool] = {}

# In-flight refresh coalescing.  One task per tab; concurrent callers await
# the same result so duplicate provider work cannot happen.
_refresh_tasks: dict[str, asyncio.Task] = {}
_refresh_coordinator_lock = asyncio.Lock()


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
      events     — broad event collection (horizon tabs only, always present)
      horizon    — rolling-horizon metadata (horizon tabs only)
      coverage   — horizon-completeness info
    """
    slot: Optional[dict] = None
    source = "neon"
    try:
        slot = _neon_read(tab)
    except Exception as e:
        print(f"[calendar_snapshot] neon read exception tab={tab}: {e}")
        slot = None
    if slot is None:
        store = _read_disk()
        if isinstance(store, dict):
            slot = _normalize_slot(store.get(tab))
        else:
            slot = _empty_slot()
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
        # The rolling-horizon fetch caps the stored collection; if the derived
        # previous-week slice is empty, keep the persisted previous_week so the
        # Recent view does not lose its historical events.
        if not pw:
            pw = slot.get("previous_week") or []

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
        actual_start, actual_end = _actual_bounds(evts)
        horizon = {
            "horizon_start": meta.get("horizon_start") or stored_window.get("from"),
            "horizon_end":   meta.get("horizon_end")   or stored_window.get("to"),
            "actual_start":  actual_start,
            "actual_end":    actual_end,
            "coverage_ranges": meta.get("coverage_ranges") or [],
            "past_days":     meta.get("past_days", _HORIZON_PAST_DAYS),
            "future_days":   meta.get("future_days", _HORIZON_FUTURE_DAYS),
            "event_count":   meta.get("event_count", len(evts)),
        }

    coverage: dict[str, Any] = {}
    if tab in _TABS_WITH_HORIZON:
        horizon_end = meta.get("horizon_end") or stored_window.get("to") or ""
        actual_start, actual_end = _actual_bounds(evts) if evts else (None, None)
        coverage["complete"] = (
            bool(actual_end) and actual_end >= req_to_str
        ) if evts else False
        coverage["horizon_end"]   = actual_end or horizon_end
        coverage["requested_end"] = req_to_str

    envelope = {
        "current_week":  diag_cw,
        "previous_week": pw if not (status == "stale") else pw,
        "last_updated":  last_updated,
        "status":        "initializing" if _bootstrapping.get(tab) else status,
        "is_stale":      is_stale,
        "bootstrapping": _bootstrapping.get(tab, False),
        "window": {
            "requested_from": req_from_str,
            "requested_to":   req_to_str,
            "stored_from":    stored_window.get("from"),
            "stored_to":      stored_window.get("to"),
        },
        "diagnostics": diagnostics,
    }

    # Additive horizon fields (only for horizon tabs)
    if tab in _TABS_WITH_HORIZON:
        envelope["events"]  = evts
    if evts and tab in _TABS_WITH_HORIZON:
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
    """Rolling forward horizon: ~11mo back, 3mo forward (FMP Starter boundary)."""
    today = _et_now().date()
    from_date = today - timedelta(days=_HORIZON_PAST_DAYS)
    to_date   = today + timedelta(days=_HORIZON_FUTURE_DAYS)
    return from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d")


_ECON_CHUNK_DAYS = 62  # ~2-month chunks stay well under FMP's ~7000 row cap

_MERGE_ID_KEYS = (
    "date", "eventName", "country",
)  # stable identity keys so historical dedup works across fetches


def _event_stable_id(ev: dict) -> str:
    """Deterministic dedup key: MD5(date + eventName + country)."""
    import hashlib
    raw = "|".join(
        str(ev.get(k, "") or "") for k in _MERGE_ID_KEYS
    )
    return hashlib.md5(raw.encode()).hexdigest()


def _merge_horizon_events(
    existing: list[dict],
    incoming: list[dict],
) -> list[dict]:
    """
    Merge incoming events into an existing horizon collection.

    • Stable dedup via MD5(date + eventName + country).
    • Incoming events overwrite existing events with the same stable id
      (e.g. a future CPI release gets its actual filled in later).
    • Earlier events from the existing collection are preserved unchanged.
    • The merged list is returned sorted by date ASC.
    """
    seen: dict[str, dict] = {}
    for ev in existing:
        sid = _event_stable_id(ev)
        seen[sid] = ev
    for ev in incoming:
        sid = _event_stable_id(ev)
        seen[sid] = ev
    return sorted(seen.values(), key=lambda e: e.get("date", ""))


def _normalize_coverage_ranges(ranges: list) -> list[dict]:
    """
    Filter and normalize a list of coverage-range dicts to a canonical form.
    Reject or ignore:
      • non-dict entries
      • missing or malformed from/to dates
      • inverted ranges (from > to)
      • unsupported status values
    Returns a cleaned, sorted list.
    """
    valid_statuses = frozenset({"complete", "empty", "failed"})
    clean: list[dict] = []
    for r in (ranges or []):
        if not isinstance(r, dict):
            continue
        fr = r.get("from", "")
        to = r.get("to", "")
        st = r.get("status", "")
        if not fr or not to or st not in valid_statuses:
            continue
        try:
            d_fr = date.fromisoformat(fr[:10])
            d_to = date.fromisoformat(to[:10])
        except (TypeError, ValueError):
            continue
        if d_fr > d_to:
            continue
        clean.append({"from": fr[:10], "to": to[:10], "status": st})
    clean.sort(key=lambda r: r["from"])
    return clean


def _coverage_union(ranges: list[dict]) -> list[dict]:
    """
    Build the continuous union of all successful (complete or empty) ranges.
    Overlapping and directly adjacent ranges are merged into maximal spans.

    Returns a list of {"from", "to"} dicts sorted by from ASC.
    """
    successful = [
        {"from": r["from"], "to": r["to"]}
        for r in ranges
        if r.get("status") in ("complete", "empty")
    ]
    if not successful:
        return []
    successful.sort(key=lambda r: r["from"])
    union: list[dict] = [dict(successful[0])]
    for r in successful[1:]:
        prev = union[-1]
        # Merge if overlapping or directly adjacent (touching, no gap)
        if prev["to"] >= _day_before(r["from"]):
            if r["to"] > prev["to"]:
                prev["to"] = r["to"]
        else:
            union.append(dict(r))
    return union


def _window_covered_by_ranges(
    ranges: list[dict],
    win_from: str,
    win_to: str,
) -> bool:
    """
    True when [win_from, win_to] is fully contained within the continuous
    union of successful (complete + empty) coverage ranges.
    """
    union = _coverage_union(ranges)
    for span in union:
        if span["from"] <= win_from and win_to <= span["to"]:
            return True
    return False


def _coverage_gap_kind(
    ranges: list[dict],
    win_from: str,
    win_to: str,
) -> str:
    """
    Classify why a window is uncovered.

    Returns:
      • "outside_horizon" — entirely before earliest or after latest range
      • "coverage_gap"    — inside the outer span but intersecting a failed range
      • ""                — covered (no gap)
    """
    clean = _normalize_coverage_ranges(ranges)
    if not clean:
        return "outside_horizon"
    first = clean[0]["from"]
    last  = clean[-1]["to"]
    if win_to < first or win_from > last:
        return "outside_horizon"
    if _window_covered_by_ranges(clean, win_from, win_to):
        return ""
    return "coverage_gap"


def _day_before(d: str) -> str:
    """Return the day before `d` as YYYY-MM-DD."""
    try:
        return (date.fromisoformat(d) - timedelta(days=1)).isoformat()
    except (TypeError, ValueError):
        return d


def _day_after(d: str) -> str:
    """Return the day after `d` as YYYY-MM-DD."""
    try:
        return (date.fromisoformat(d) + timedelta(days=1)).isoformat()
    except (TypeError, ValueError):
        return d


def _dates_overlap(a_from: str, a_to: str, b_from: str, b_to: str) -> bool:
    """True when two inclusive date intervals overlap."""
    return a_from <= b_to and b_from <= a_to


def _overlay_coverage_range(
    result: list[dict],
    inc: dict,
) -> None:
    """
    Overlay one incoming coverage range onto the result list with proper
    date-interval splitting. Non-overlapping left and right remainder
    portions of prior ranges are preserved.

    Phase 1: cut out overlapping sections from all prior ranges.
    Phase 2: insert the incoming range (or remainder portions) with
             precedence applied.
    """
    inc_from = inc["from"]
    inc_to = inc["to"]
    inc_status = inc["status"]
    is_successful = inc_status in ("complete", "empty")

    # If the incoming is failed and the overlap area was previously covered by
    # any successful prior range, the prior is authoritative for the overlap.
    # Do NOT cut prior ranges (Phase 1).  Only insert uncovered tail portion
    # of the failed incoming (Phase 2).
    skip_phase1 = False
    if not is_successful and any(
        _dates_overlap(r["from"], r["to"], inc_from, inc_to)
        and r.get("status") in ("complete", "empty")
        for r in result
    ):
        skip_phase1 = True

    # ── Phase 1: cut remaining ranges ──────────────────────────────────────
    if not skip_phase1:
        cut: list[dict] = []
        for r in result:
            r_from = r["from"]
            r_to = r["to"]
            if not _dates_overlap(r_from, r_to, inc_from, inc_to):
                cut.append(r)
                continue

            # Left remainder (before the incoming)
            if r_from < inc_from:
                cut.append({
                    "from": r_from,
                    "to": _day_before(inc_from),
                    "status": r["status"],
                })

            # Right remainder (after the incoming)
            if r_to > inc_to:
                cut.append({
                    "from": _day_after(inc_to),
                    "to": r_to,
                    "status": r["status"],
                })
        result[:] = cut

    # ── Phase 2: insert incoming (or applicable portions) ──────────────────
    if is_successful:
        result.append({"from": inc_from, "to": inc_to, "status": inc_status})
        return

    # Incoming is failed.  Only insert portions NOT already covered by
    # range (complete or empty) that survives after Phase 1.
    success_spans = sorted(
        [(r["from"], r["to"]) for r in result if r.get("status") in ("complete", "empty")],
        key=lambda s: s[0],
    )

    # Merge adjacent success spans for clean gap detection
    merged_spans: list[tuple[str, str]] = []
    for sf, st in success_spans:
        if merged_spans and sf <= _day_after(merged_spans[-1][1]):
            if st > merged_spans[-1][1]:
                merged_spans[-1] = (merged_spans[-1][0], st)
        else:
            merged_spans.append((sf, st))

    # Add failed portions that are not inside any success span
    cur = inc_from
    for sf, st in merged_spans:
        if cur < sf and cur <= inc_to:
            result.append({
                "from": cur,
                "to": _day_before(sf),
                "status": inc_status,
            })
        cur = max(cur, _day_after(st))
    if cur <= inc_to:
        result.append({"from": cur, "to": inc_to, "status": inc_status})


def _merge_coverage_ranges(
    prior_ranges: list[dict],
    new_ranges: list[dict],
) -> list[dict]:
    """
    Merge incoming chunk-level coverage ranges into the prior set using
    proper date-interval overlay semantics.

    Each incoming range replaces prior status only for the dates it covers.
    Non-overlapping left and right portions of prior ranges are preserved.

    Precedence:
      • incoming complete/empty  → replaces any prior for its date span
      • incoming failed          → does not downgrade prior complete/empty
      • incoming failed          → supersedes prior failed or uncovered dates
    """
    prior = _normalize_coverage_ranges(prior_ranges)
    incoming = _normalize_coverage_ranges(new_ranges)

    result: list[dict] = list(prior)
    for inc in incoming:
        _overlay_coverage_range(result, inc)

    result.sort(key=lambda r: r["from"])

    # Compact adjacent ranges with the same status.
    compact: list[dict] = []
    for r in result:
        if (
            compact
            and compact[-1]["status"] == r["status"]
            and compact[-1]["to"] >= _day_before(r["from"])
        ):
            if r["to"] > compact[-1]["to"]:
                compact[-1]["to"] = r["to"]
        else:
            compact.append(dict(r))
    return compact


async def _chunked_economic_fetch(
    fmp,
    from_date: str,
    to_date: str,
) -> tuple[list[dict], Optional[str], list[dict], dict]:
    """
    Fetch economic releases in ~2-month chunks so each FMP call stays
    well under the provider's ~7000-row response cap.

    Deduplication across chunks uses the stable MD5 identity so events
    that appear at chunk boundaries only count once.

    Returns (events, error_message, coverage_ranges, diagnostics) where:
      • coverage_ranges: list of {"from", "to", "status"} per chunk
      • diagnostics: {"coverage_status", "chunks_succeeded", "chunks_failed"}
    """
    from services.catalyst_calendar_service import _fetch_economic_releases

    try:
        from_dt = date.fromisoformat(from_date)
        to_dt   = date.fromisoformat(to_date)
    except (TypeError, ValueError):
        return [], f"invalid dates: {from_date} / {to_date}", [], {
            "coverage_status": "unavailable", "chunks_succeeded": 0, "chunks_failed": 1,
        }

    all_events: list[dict] = []
    seen_ids: set[str] = set()
    ranges: list[dict] = []
    succeeded = 0
    failed = 0
    cur = from_dt

    while cur <= to_dt:
        chunk_end = min(cur + timedelta(days=_ECON_CHUNK_DAYS), to_dt)
        c_from = cur.strftime("%Y-%m-%d")
        c_to   = chunk_end.strftime("%Y-%m-%d")

        try:
            chunk = await _fetch_economic_releases(fmp, c_from, c_to)
        except Exception as e:
            print(f"[calendar_snapshot] chunked fetch error {c_from}→{c_to}: {e}")
            ranges.append({"from": c_from, "to": c_to, "status": "failed"})
            failed += 1
            cur = chunk_end + timedelta(days=1)
            continue

        succeeded += 1
        added = 0
        for ev in chunk:
            sid = _event_stable_id(ev)
            if sid not in seen_ids:
                seen_ids.add(sid)
                all_events.append(ev)
                added += 1

        chunk_status = "complete" if chunk else "empty"
        ranges.append({"from": c_from, "to": c_to, "status": chunk_status})

        print(
            f"[calendar_snapshot] chunk {c_from}→{c_to}: "
            f"raw={len(chunk)} added={added} total={len(all_events)} status={chunk_status}"
        )
        cur = chunk_end + timedelta(days=1)

    if failed == 0 and succeeded == 0:
        coverage_status = "unavailable"
    elif failed == 0:
        coverage_status = "complete"
    elif succeeded > 0:
        coverage_status = "partial"
    else:
        coverage_status = "unavailable"

    diag = {
        "coverage_status":     coverage_status,
        "chunks_succeeded":    succeeded,
        "chunks_failed":       failed,
    }
    err_msg = None if failed == 0 else f"{failed}/{succeeded + failed} chunks failed"

    return all_events, err_msg, ranges, diag


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


# ── Requested-window read API (horizon tabs) ────────────────────────────────

_HORIZON_VIEWS = ("recent", "day", "week", "month")


def _parse_iso_date(s: Optional[str]) -> Optional[date]:
    """Parse a YYYY-MM-DD anchor. Returns None on any parse error."""
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except (TypeError, ValueError):
        return None


def _resolve_window(
    view: Optional[str],
    anchor: Optional[str],
    from_date: Optional[str],
    to_date: Optional[str],
    tab: str,
) -> tuple[str, str]:
    """
    Derive the inclusive [window_start, window_end] (ISO YYYY-MM-DD) for a request.

    An explicit from/to override wins.  Otherwise the `view` is resolved around
    the ET anchor date (`date`, defaulting to today):

      • day    → the anchor day
      • week   → Monday–Friday of the anchor week (current/previous/future)
      • month  → first calendar day through last calendar day of the anchor month
      • recent → the snapshot's existing previous-week window (current Recent view)

    Unknown views fall back to the Mon–Fri week containing the anchor.  All
    conventions are America/New_York, matching the rest of the snapshot service.
    """
    today = _et_now().date()

    if from_date or to_date:
        f = _parse_iso_date(from_date) or _parse_iso_date(anchor) or today
        t = _parse_iso_date(to_date) or f
        if t < f:
            f, t = t, f
        return f.isoformat(), t.isoformat()

    v = (view or "week").strip().lower()
    if v == "recent":
        return _previous_week_window_for(tab)

    anchor_date = _parse_iso_date(anchor) or today

    if v == "day":
        return anchor_date.isoformat(), anchor_date.isoformat()

    if v == "month":
        first = anchor_date.replace(day=1)
        if first.month == 12:
            nxt = date(first.year + 1, 1, 1)
        else:
            nxt = date(first.year, first.month + 1, 1)
        return first.isoformat(), (nxt - timedelta(days=1)).isoformat()

    # week (default)
    monday = anchor_date - timedelta(days=anchor_date.weekday())
    friday = monday + timedelta(days=4)
    return monday.isoformat(), friday.isoformat()


def _window_horizon_bounds(
    env: dict, broad: list[dict],
) -> tuple[Optional[str], Optional[str]]:
    """
    Return the stored horizon [start, end] used for coverage reporting.
    Prefers the horizon meta fields; falls back to the stored window and to the
    broad events' actual date span so old rows remain truthful.
    """
    horizon = env.get("horizon") or {}
    stored_window = env.get("window") or {}
    h_start = horizon.get("horizon_start") or stored_window.get("stored_from")
    h_end = horizon.get("horizon_end") or stored_window.get("stored_to")
    if broad:
        dates = sorted(
            (e.get("date") or "")[:10] for e in broad
            if (e.get("date") or "")[:10]
        )
        if dates:
            h_start = h_start or dates[0]
            h_end = h_end or dates[-1]
    return (h_start or None), (h_end or None)


def _actual_bounds(events: list[dict]) -> tuple[Optional[str], Optional[str]]:
    """
    Return the [min_date, max_date] span actually covered by the persisted
    events. This is the ground-truth horizon used for coverage: the meta
    horizon_start/horizon_end describe the intended fetch window, while the
    persisted collection may be narrower (e.g. capped fetches).
    """
    dates = sorted(
        (e.get("date") or "")[:10] for e in events
        if (e.get("date") or "")[:10]
    )
    if not dates:
        return None, None
    return dates[0], dates[-1]


def _window_covered(
    horizon_start: Optional[str],
    horizon_end: Optional[str],
    win_from: str,
    win_to: str,
) -> bool:
    """True when the stored horizon covers [win_from, win_to] inclusive."""
    if not horizon_end:
        return False
    if horizon_start and win_from < horizon_start:
        return False
    return win_to <= horizon_end


def _window_empty_reason(
    coverage_complete: bool,
    selected: list[dict],
    source: str,
    status: str = "",
    gap_kind: str = "",
) -> Optional[str]:
    """
    Truthful empty-state classification for a requested window.

      • events present                     → None
      • snapshot has no events at all      → "snapshot_empty"
      • snapshot initializing/refreshing   → "snapshot_initializing"
      • window outside all known coverage  → "outside_horizon"
      • internal provider gap              → "coverage_gap"
      • old snapshot without broad events  → "legacy_snapshot_without_horizon"
      • covered window with no events      → "no_events_in_window"
    """
    if selected:
        return None
    if source == "none":
        if status in ("initializing", "refreshing"):
            return "snapshot_initializing"
        return "snapshot_empty"
    if not coverage_complete:
        if source == "legacy":
            return "legacy_snapshot_without_horizon"
        if gap_kind == "coverage_gap":
            return "coverage_gap"
        return gap_kind or "outside_horizon"
    return "no_events_in_window"


def get_snapshot_window(
    tab: str,
    view: Optional[str] = None,
    date: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
) -> dict:
    """
    Serve a requested day/week/month window from the persisted rolling horizon.

    Additive to get_snapshot(): every existing envelope field is preserved, and
    `events` carries the SELECTED window (never the full rolling horizon), so a
    Week/Day response does not include all ~90 days.  Window and coverage
    metadata describe the requested window truthfully.

    Selection:
      • Prefers the canonical broad `events` collection when present.
      • Falls back to legacy `current_week` / `previous_week` for old snapshots
        that predate the rolling horizon.
      • No provider calls. No fabricated data. Windows outside the stored
        horizon report incomplete coverage truthfully.
    """
    env = get_snapshot(tab)

    broad = env.get("events") or []
    legacy = (env.get("current_week") or []) + (env.get("previous_week") or [])

    win_from, win_to = _resolve_window(view, date, from_date, to_date, tab)
    v = (view or "week").strip().lower()

    horizon_start, horizon_end = _window_horizon_bounds(env, broad)

    out = dict(env)

    if v == "recent":
        # Recent preserves the existing previous-week semantics exactly.  It
        # returns the envelope's previous_week slice directly (derived from the
        # broad horizon when possible, else the persisted previous_week) so the
        # historical display is unchanged even when the broad collection is
        # capped.  Window metadata reflects the actual span of that data.
        source = "previous_week"
        pool = env.get("previous_week") or []
        selected = list(pool)
        if pool:
            coverage_complete = True
            empty_reason = None if selected else "no_events_in_window"
            r_start, r_end = _actual_bounds(pool)
            if r_start:
                win_from, win_to = r_start, r_end
        else:
            coverage_complete = False
            empty_reason = "snapshot_empty"
    else:
        source = "horizon" if broad else ("legacy" if legacy else "none")
        pool = broad if broad else legacy
        selected = _select_events_for_window(pool, win_from, win_to)
        # Coverage uses explicit provider-chunk coverage ranges when available,
        # falling back to _actual_bounds() for legacy snapshots that predate
        # the ranges model.  The continuous union of all successful ranges
        # (complete + empty) is the authoritative coverage.
        ranges = (env.get("horizon") or {}).get("coverage_ranges") or []
        if ranges:
            ranges = _normalize_coverage_ranges(ranges)
            coverage_complete = _window_covered_by_ranges(ranges, win_from, win_to)
            gap_kind = _coverage_gap_kind(ranges, win_from, win_to) if not coverage_complete else ""
        else:
            actual_start, actual_end = _actual_bounds(pool)
            coverage_complete = _window_covered(
                actual_start, actual_end, win_from, win_to,
            )
            gap_kind = ""
        empty_reason = _window_empty_reason(coverage_complete, selected, source,
                                               status=env.get("status", ""),
                                               gap_kind=gap_kind)
    if from_date or to_date:
        out["view"] = (view or "range").strip().lower()
    else:
        out["view"] = (view or "week").strip().lower()
    out["requested_date"] = date
    out["window_start"] = win_from
    out["window_end"] = win_to
    out["events"] = selected
    out["event_count"] = len(selected)
    out["coverage_complete"] = coverage_complete
    out["horizon_start"] = horizon_start
    out["horizon_end"] = horizon_end
    out["empty_reason"] = empty_reason

    coverage = dict(out.get("coverage") or {})
    coverage["complete"] = coverage_complete
    coverage["requested_start"] = win_from
    coverage["requested_end"] = win_to
    out["coverage"] = coverage

    print(
        f"[calendar_snapshot] get_snapshot_window tab={tab} view={out['view']} "
        f"window={win_from}→{win_to} source={source} "
        f"selected={len(selected)} coverage_complete={coverage_complete} "
        f"empty_reason={empty_reason} horizon={horizon_start}→{horizon_end}"
    )
    return out


async def refresh_tab(tab: str, fmp_key: str) -> dict:
    """
    Public refresh entry point.  Coordinates concurrent callers so that only
    one refresh task per tab runs at a time and all awaiters share its result.

    This is the macro refresh coordinator: multiple triggers (startup stale
    check, weekly scheduler, manual backfill) may request the same tab, but
    the first caller starts the work and later callers await the in-flight
    task instead of running duplicate provider fetches.
    """
    if tab not in TARGET_TABS:
        raise ValueError(f"refresh_tab: unsupported tab {tab!r}")
    if not fmp_key:
        print(f"[calendar_snapshot] refresh_tab({tab}) skipped: missing FMP key")
        return get_snapshot(tab)

    async with _refresh_coordinator_lock:
        task = _refresh_tasks.get(tab)
        if task is None or task.done():
            task = asyncio.create_task(_refresh_tab_core(tab, fmp_key))
            _refresh_tasks[tab] = task

    try:
        return await task
    finally:
        async with _refresh_coordinator_lock:
            if _refresh_tasks.get(tab) is task:
                _refresh_tasks.pop(tab, None)


async def _refresh_tab_core(tab: str, fmp_key: str) -> dict:
    """
    Core refresh implementation for `tab`.  Do not call directly; use
    `refresh_tab` so concurrent work is coalesced.

    Runs the existing FMP fetcher, promotes current→previous, saves the new
    current_week and meta, and persists to Neon (source of truth) and
    best-effort to disk (emergency fallback). Returns the new envelope.

    For tabs in _TABS_WITH_HORIZON, fetches the broad rolling horizon in
    ~2-month chunks (to avoid FMP's ~7000-row response cap), merges new
    events with the existing collection so historical events are never
    discarded, and derives current_week/previous_week from the merged set.
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
        _fetch_economic_releases,
    )

    fmp = CatalystFMP(fmp_key)
    watchlist = _load_watchlist_symbols()
    portfolio = _load_portfolio_symbols()

    use_horizon = tab in _TABS_WITH_HORIZON

    # Read prior slot first so we can decide delta vs full-backfill.
    # Guarded — a transient Neon read failure must not crash the refresh.
    if use_horizon:
        try:
            prior_check = _neon_read(tab)
        except Exception as e:
            print(f"[calendar_snapshot] refresh_tab neon read error: {e} — falling back to disk")
            prior_check = _normalize_slot(_read_disk().get(tab))
        prior_events = (prior_check.get("events") or []) if prior_check else []
        prior_meta = (prior_check.get("meta") or {}) if prior_check else {}
        prior_ranges = prior_meta.get("coverage_ranges") or []
        is_bootstrap_start = False
        if prior_events:
            # Existing history present — fetch only a delta window so the
            # weekly refresh stays fast (most old chunks are already cached).
            # Delta: 30d past covers any late-reported revisions.
            delta_past = 30
            et_today = _et_now().date()
            delta_from = (et_today - timedelta(days=delta_past)).strftime("%Y-%m-%d")
            delta_to   = (et_today + timedelta(days=_HORIZON_FUTURE_DAYS)).strftime("%Y-%m-%d")
            fetch_from, fetch_to = delta_from, delta_to
            print(
                f"[calendar_snapshot] refresh tab={tab} delta window "
                f"(prior events={len(prior_events)}, past={delta_past}d, future={_HORIZON_FUTURE_DAYS}d)"
            )
        else:
            # First run: full horizon backfill.
            if _bootstrapping.get(tab):
                print(
                    f"[calendar_snapshot] refresh tab={tab} bootstrap already "
                    f"running — skipping duplicate"
                )
                return get_snapshot(tab)
            _bootstrapping[tab] = True
            is_bootstrap_start = True
            fetch_from, fetch_to = _horizon_window_for()
            print(
                f"[calendar_snapshot] refresh tab={tab} full backfill "
                f"window={fetch_from}→{fetch_to}"
            )
    else:
        is_bootstrap_start = False
        prior_ranges = []
        fetch_from, fetch_to = _week_window_for(tab)

    try:
        t0 = time.monotonic()
        chunk_ranges: list[dict] = []

        if use_horizon:
            # ── Horizon tabs: chunked fetch + merge with existing collection ──
            events, err, chunk_ranges, _chunk_diag = await _chunked_economic_fetch(fmp, fetch_from, fetch_to)
            if err:
                print(f"[calendar_snapshot] refresh_tab({tab}) chunked fetch error: {err}")
            print(
                f"[calendar_snapshot] refresh tab={tab} chunk diag: "
                f"coverage_status={_chunk_diag.get('coverage_status', '?')} "
                f"succeeded={_chunk_diag.get('chunks_succeeded', 0)} "
                f"failed={_chunk_diag.get('chunks_failed', 0)}"
            )
        else:
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
        if not use_horizon:
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
        # Guarded — transient Neon read failure must not crash the refresh coroutine.
        try:
            prior = _neon_read(tab)
        except Exception as e:
            print(f"[calendar_snapshot] refresh_tab second neon read error tab={tab}: {e}")
            prior = None
        if prior is None:
            # No DB row yet (or Neon unreachable for read) — fall back to disk
            # for the prior, so we don't lose previous_week if Neon was just
            # transiently down.
            store = _read_disk()
            prior = _normalize_slot(store.get(tab))

        prior_current = prior.get("current_week") or []
        prior_previous = prior.get("previous_week") or []

        if use_horizon:
            # ── Horizon tabs: merge with existing collection ──────────────────
            prior_evts = prior.get("events") or []
            if err and prior_evts:
                # On fetch error, keep prior valid horizon so consumers never see
                # a suddenly empty state.
                print(
                    f"[calendar_snapshot] refresh tab={tab} fetch error — "
                    f"preserving prior horizon events={len(prior_evts)}"
                )
                all_events = prior_evts
            else:
                all_events = _merge_horizon_events(prior_evts, events or [])
                dropped = (len(prior_evts) + len(events or [])) - len(all_events)
                if dropped:
                    print(
                        f"[calendar_snapshot] refresh tab={tab} merge: "
                        f"prior={len(prior_evts)} new={len(events or [])} "
                        f"merged={len(all_events)} (dedup dropped {dropped})"
                    )

            monday_str, friday_str = _week_window_for(tab)
            cw_events = _select_events_for_window(all_events, monday_str, friday_str)

            prev_from, prev_to = _previous_week_window_for(tab)
            pw_events = _select_events_for_window(all_events, prev_from, prev_to)
            # If derived pw is empty (e.g. capped horizon), preserve persisted pw.
            if not pw_events:
                pw_events = prior_previous or []

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
                # horizon_start reflects the actual earliest stored event date,
                # not the intended fetch window (history accumulates over time).
                actual_start, actual_end = _actual_bounds(all_events)
                merged_ranges = _merge_coverage_ranges(prior_ranges, chunk_ranges)
                new_meta["horizon_start"]    = actual_start or horizon_start
                new_meta["horizon_end"]      = horizon_end   # intended future boundary
                new_meta["past_days"]        = _HORIZON_PAST_DAYS
                new_meta["future_days"]      = _HORIZON_FUTURE_DAYS
                new_meta["event_count"]      = len(all_events)
                new_meta["coverage_ranges"]  = merged_ranges
                new_meta["coverage_status"]  = _chunk_diag.get("coverage_status")

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

    finally:
        if is_bootstrap_start:
            _bootstrapping[tab] = False

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
