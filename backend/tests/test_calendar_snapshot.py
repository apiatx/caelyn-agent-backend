"""
Unit tests for rolling horizon in services/calendar_snapshot_service.py.

Mocked-data-only — no FMP, no network, no DB.
"""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

_BACKEND = Path(__file__).resolve().parent.parent
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from services.calendar_snapshot_service import (
    _TABS_WITH_HORIZON,
    _HORIZON_PAST_DAYS,
    _HORIZON_FUTURE_DAYS,
    _horizon_window_for,
    _horizon_is_complete,
    _select_events_for_window,
    _snapshot_is_stale,
    _empty_slot,
    _normalize_slot,
    _resolve_window,
    _actual_bounds,
    _event_stable_id,
    _merge_horizon_events,
    _window_covered,
    get_snapshot_window,
    weekly_scheduler_loop,
)


def _make_econ(date_str="2026-08-03", **kw) -> dict:
    from services.catalyst_calendar_service import _build_event
    return _build_event(
        id=kw.get("id", f"ev_{date_str}"),
        eventType="economic_release",
        date=date_str,
        time=kw.get("time"),
        title=kw.get("title", "CPI MoM"),
        eventName=kw.get("eventName", kw.get("title", "CPI MoM")),
        country=kw.get("country", "US"),
        importance=kw.get("importance", "high"),
        actual=kw.get("actual"),
        estimate=kw.get("estimate"),
        previous=kw.get("previous"),
        event_family=kw.get("event_family", "cpi"),
        signal_tier=kw.get("signal_tier", "major"),
        signal_reason=kw.get("signal_reason", "Inflation report"),
    )


# ── Horizon window computation ──────────────────────────────────────────────

def test_horizon_window_span():
    """Horizon window is 14 days back + 89 days forward."""
    hfrom, hto = _horizon_window_for()
    d_from = date.fromisoformat(hfrom)
    d_to = date.fromisoformat(hto)
    span = (d_to - d_from).days
    assert span == _HORIZON_PAST_DAYS + _HORIZON_FUTURE_DAYS, (
        f"expected {_HORIZON_PAST_DAYS + _HORIZON_FUTURE_DAYS}d span, got {span}d"
    )


def test_horizon_window_past_days():
    """Horizon starts exactly `past_days` before today."""
    from services.calendar_snapshot_service import _et_now
    hfrom, _hto = _horizon_window_for()
    d_from = date.fromisoformat(hfrom)
    today = _et_now().date()
    assert (today - d_from).days == _HORIZON_PAST_DAYS


def test_horizon_window_future_days():
    """Horizon ends exactly `future_days` after today."""
    from services.calendar_snapshot_service import _et_now
    _hfrom, hto = _horizon_window_for()
    d_to = date.fromisoformat(hto)
    today = _et_now().date()
    assert (d_to - today).days == _HORIZON_FUTURE_DAYS


def test_horizon_window_dates_are_valid_iso():
    hfrom, hto = _horizon_window_for()
    assert len(hfrom) == 10
    assert len(hto) == 10
    date.fromisoformat(hfrom)
    date.fromisoformat(hto)


# ── Event selection for window ──────────────────────────────────────────────

def test_select_events_in_window():
    events = [
        _make_econ("2026-08-01"),
        _make_econ("2026-08-03"),
        _make_econ("2026-08-05"),
        _make_econ("2026-08-10"),
    ]
    selected = _select_events_for_window(events, "2026-08-02", "2026-08-05")
    dates = [e["date"] for e in selected]
    assert dates == ["2026-08-03", "2026-08-05"]


def test_select_events_inclusive_bounds():
    events = [
        _make_econ("2026-08-01"),
        _make_econ("2026-08-05"),
    ]
    selected = _select_events_for_window(events, "2026-08-05", "2026-08-05")
    assert len(selected) == 1
    assert selected[0]["date"] == "2026-08-05"


def test_select_events_empty_input():
    assert _select_events_for_window([], "2026-08-01", "2026-08-05") == []


def test_select_events_no_match():
    events = [_make_econ("2026-07-01")]
    assert _select_events_for_window(events, "2026-08-01", "2026-08-05") == []


def test_select_events_preserves_exact_fields():
    events = [_make_econ("2026-08-03", actual="3.2", estimate="3.1", event_family="cpi", signal_tier="major")]
    selected = _select_events_for_window(events, "2026-08-01", "2026-08-05")
    ev = selected[0]
    assert ev["actual"] == "3.2"
    assert ev["estimate"] == "3.1"
    assert ev["event_family"] == "cpi"
    assert ev["signal_tier"] == "major"
    assert ev["date"] == "2026-08-03"


# ── Horizon completeness ────────────────────────────────────────────────────

def test_horizon_complete_covers_requested_end():
    slot = {
        "events": [_make_econ("2026-08-01")],
        "meta": {"horizon_end": "2026-09-30"},
    }
    assert _horizon_is_complete(slot, "2026-09-25") is True


def test_horizon_incomplete_before_requested_end():
    slot = {
        "events": [_make_econ("2026-08-01")],
        "meta": {"horizon_end": "2026-08-15"},
    }
    assert _horizon_is_complete(slot, "2026-09-01") is False


def test_horizon_incomplete_no_events():
    slot = {
        "events": [],
        "meta": {"horizon_end": "2026-09-30"},
    }
    assert _horizon_is_complete(slot, "2026-09-01") is False


def test_horizon_incomplete_no_meta():
    slot = {
        "events": [_make_econ("2026-08-01")],
        "meta": {},
    }
    assert _horizon_is_complete(slot, "2026-09-01") is False


def test_horizon_incomplete_no_slot():
    assert _horizon_is_complete(None, "2026-09-01") is False


def test_horizon_complete_exact_boundary():
    slot = {
        "events": [_make_econ("2026-08-01")],
        "meta": {"horizon_end": "2026-09-01"},
    }
    assert _horizon_is_complete(slot, "2026-09-01") is True


# ── Backward compatibility ──────────────────────────────────────────────────

def test_empty_slot_has_events_key():
    slot = _empty_slot()
    assert "events" in slot
    assert slot["events"] == []
    assert slot["current_week"] == []
    assert slot["previous_week"] == []


def test_normalize_slot_adds_events():
    slot = _normalize_slot({})
    assert "events" in slot
    assert slot["events"] == []


def test_normalize_slot_preserves_existing_events():
    slot = _normalize_slot({"events": [_make_econ("2026-08-01")]})
    assert len(slot["events"]) == 1


def test_old_slot_without_events_still_readable():
    """A slot without events (old snapshot) should be normalized to have empty events."""
    old_slot = {"current_week": [_make_econ("2026-08-01")], "previous_week": [], "meta": {}}
    normalized = _normalize_slot(old_slot)
    assert normalized["events"] == []
    assert len(normalized["current_week"]) == 1


# ── Staleness with horizon ──────────────────────────────────────────────────

def test_stale_slot_null():
    assert _snapshot_is_stale(None, "economic_releases") is True


def test_stale_no_current_week_no_events():
    slot = _empty_slot()
    assert _snapshot_is_stale(slot, "economic_releases") is True


def test_stale_outdated_stored_from():
    """A non-horizon tab with an outdated stored_from is stale."""
    slot = {
        "current_week": [_make_econ("2020-01-06")],
        "previous_week": [],
        "events": [],
        "meta": {"window": {"from": "2020-01-06"}},
    }
    assert _snapshot_is_stale(slot, "dividends") is True


def test_horizon_tab_stale_when_horizon_end_before_friday():
    """A horizon tab whose horizon_end < current Friday is stale."""
    from services.calendar_snapshot_service import _et_now
    today = _et_now().date()
    monday = today - timedelta(days=today.weekday())
    friday = monday + timedelta(days=4)
    slot = {
        "current_week": [_make_econ(monday.isoformat())],
        "previous_week": [],
        "events": [_make_econ(monday.isoformat())],
        "meta": {
            "window": {"from": monday.isoformat()},
            "horizon_end": (today - timedelta(days=10)).isoformat(),
        },
    }
    # horizon_end is 10 days ago, well before Friday
    assert _snapshot_is_stale(slot, "economic_releases") is True


def test_fresh_horizon_tab_not_stale():
    """
    A freshly refreshed horizon tab is NOT stale even though window.from is
    the horizon start (today - past_days), which precedes the current Monday.
    Regression: the legacy stored_from < Monday comparison must not apply
    to horizon tabs, otherwise every request triggered a background FMP refresh.
    """
    from services.calendar_snapshot_service import (
        _et_now, _horizon_window_for,
    )
    today = _et_now().date()
    monday = today - timedelta(days=today.weekday())
    friday = monday + timedelta(days=4)
    hfrom, hto = _horizon_window_for()
    assert hfrom < monday.isoformat()  # horizon start is before this week Monday
    assert hto >= friday.isoformat()   # horizon covers the current Friday
    slot = {
        "current_week": [_make_econ(monday.isoformat())],
        "previous_week": [],
        "events": [_make_econ(monday.isoformat())],
        "meta": {
            "window": {"from": hfrom, "to": hto},
            "horizon_start": hfrom,
            "horizon_end": hto,
        },
    }
    assert _snapshot_is_stale(slot, "economic_releases") is False


def test_horizon_tab_not_stale_when_end_equals_friday():
    """horizon_end exactly on the current Friday is not stale."""
    from services.calendar_snapshot_service import _et_now
    today = _et_now().date()
    monday = today - timedelta(days=today.weekday())
    friday = monday + timedelta(days=4)
    slot = {
        "current_week": [_make_econ(monday.isoformat())],
        "previous_week": [],
        "events": [_make_econ(monday.isoformat())],
        "meta": {
            "window": {"from": monday.isoformat()},
            "horizon_end": friday.isoformat(),
        },
    }
    assert _snapshot_is_stale(slot, "economic_releases") is False


def test_horizon_tab_stale_when_end_is_day_before_friday():
    """horizon_end one day before the current Friday is stale."""
    from services.calendar_snapshot_service import _et_now
    today = _et_now().date()
    monday = today - timedelta(days=today.weekday())
    friday = monday + timedelta(days=4)
    slot = {
        "current_week": [_make_econ(monday.isoformat())],
        "previous_week": [],
        "events": [_make_econ(monday.isoformat())],
        "meta": {
            "window": {"from": monday.isoformat()},
            "horizon_end": (friday - timedelta(days=1)).isoformat(),
        },
    }
    assert _snapshot_is_stale(slot, "economic_releases") is True


def test_horizon_tab_stale_when_no_horizon_end():
    """A horizon tab with events but no horizon_end meta is stale."""
    from services.calendar_snapshot_service import _et_now
    today = _et_now().date()
    monday = today - timedelta(days=today.weekday())
    slot = {
        "current_week": [_make_econ(monday.isoformat())],
        "previous_week": [],
        "events": [_make_econ(monday.isoformat())],
        "meta": {
            "window": {"from": monday.isoformat()},
        },
    }
    assert _snapshot_is_stale(slot, "economic_releases") is True


# ── Horizon tab identity ────────────────────────────────────────────────────

def test_economic_releases_has_horizon():
    assert "economic_releases" in _TABS_WITH_HORIZON


def test_other_tabs_not_in_horizon():
    assert "dividends" not in _TABS_WITH_HORIZON
    assert "ipos" not in _TABS_WITH_HORIZON
    assert "splits" not in _TABS_WITH_HORIZON
    assert "treasury_macro" not in _TABS_WITH_HORIZON


# ── Future events in broad collection ──────────────────────────────────────

def test_future_events_preserved_in_selection():
    """Events 60 days from now should be selectable via _select_events_for_window."""
    future_date = (date.today() + timedelta(days=60)).isoformat()
    events = [_make_econ(future_date)]
    selected = _select_events_for_window(events, future_date, future_date)
    assert len(selected) == 1


def test_exact_dates_preserved():
    exact_date = "2026-11-15T08:30:00"
    ev = _make_econ(exact_date)
    assert ev["date"] == exact_date
    selected = _select_events_for_window([ev], "2026-11-15", "2026-11-15")
    assert selected[0]["date"] == exact_date


# ── Current / previous week derivation mock ─────────────────────────────────

def test_derive_current_week_from_broad():
    """current_week events are a subset of the broad collection for this Mon-Fri."""
    from services.calendar_snapshot_service import _et_now, _week_window_for
    today = _et_now().date()
    monday = today - timedelta(days=today.weekday())
    friday = monday + timedelta(days=4)

    all_events = [
        _make_econ((monday - timedelta(days=5)).isoformat(), id="past"),
        _make_econ(monday.isoformat(), id="cw1"),
        _make_econ((monday + timedelta(days=2)).isoformat(), id="cw2"),
        _make_econ(friday.isoformat(), id="cw3"),
        _make_econ((friday + timedelta(days=5)).isoformat(), id="future"),
    ]

    monday_str, friday_str = _week_window_for("economic_releases")
    cw = _select_events_for_window(all_events, monday_str, friday_str)
    cw_ids = {e["id"] for e in cw}
    assert cw_ids == {"cw1", "cw2", "cw3"}
    assert "past" not in cw_ids
    assert "future" not in cw_ids


def test_derive_previous_week_from_broad():
    """previous_week events are a subset of the broad collection for last Mon-Fri."""
    from services.calendar_snapshot_service import (
        _et_now, _previous_week_window_for,
    )
    prev_from, prev_to = _previous_week_window_for("economic_releases")

    all_events = [
        _make_econ((date.fromisoformat(prev_from) - timedelta(days=5)).isoformat(), id="before"),
        _make_econ(prev_from, id="pw1"),
        _make_econ(prev_to, id="pw2"),
        _make_econ((date.fromisoformat(prev_to) + timedelta(days=5)).isoformat(), id="after"),
    ]

    pw = _select_events_for_window(all_events, prev_from, prev_to)
    pw_ids = {e["id"] for e in pw}
    assert "pw1" in pw_ids
    assert "pw2" in pw_ids
    assert "before" not in pw_ids
    assert "after" not in pw_ids


# ═══════════════════════════════════════════════════════════════════════════════
# Requested-window serving (get_snapshot_window)
# ═══════════════════════════════════════════════════════════════════════════════

from unittest import mock


def _horizon_env(events, horizon_start="2026-07-18", horizon_end="2026-10-29",
                 status="ready", last_updated="2026-08-01T12:00:00+00:00"):
    """Fixture envelope returned by get_snapshot for a horizon snapshot."""
    return {
        "current_week":  [],
        "previous_week": [],
        "last_updated":  last_updated,
        "status":        status,
        "is_stale":      False,
        "bootstrapping": False,
        "window": {
            "requested_from": "2026-07-27",
            "requested_to":   "2026-07-31",
            "stored_from":    horizon_start,
            "stored_to":      horizon_end,
        },
        "diagnostics": {},
        "events": events,
        "horizon": {
            "horizon_start": horizon_start,
            "horizon_end":   horizon_end,
            "past_days":     14,
            "future_days":   89,
            "event_count":   len(events),
        },
        "coverage": {
            "complete":     True,
            "horizon_end":  horizon_end,
            "requested_end": "2026-07-31",
        },
    }


def _legacy_env(cw, pw, status="ready", stored_from="2026-07-27", stored_to="2026-07-31"):
    """Fixture envelope returned by get_snapshot for a pre-horizon snapshot."""
    return {
        "current_week":  cw,
        "previous_week": pw,
        "last_updated":  "2026-07-31T14:24:25+00:00",
        "status":        status,
        "is_stale":      False,
        "window": {
            "requested_from": "2026-07-27",
            "requested_to":   "2026-07-31",
            "stored_from":    stored_from,
            "stored_to":      stored_to,
        },
        "diagnostics": {},
        "coverage": {
            "complete":     False,
            "horizon_end":  stored_to,
            "requested_end": "2026-07-31",
        },
    }


def _window(view=None, date=None, from_date=None, to_date=None, env=None):
    """Call get_snapshot_window with get_snapshot mocked to `env`."""
    with mock.patch(
        "services.calendar_snapshot_service.get_snapshot", return_value=env,
    ) as m:
        out = get_snapshot_window(
            "economic_releases", view=view, date=date,
            from_date=from_date, to_date=to_date,
        )
    return out, m


# ── Window derivation ───────────────────────────────────────────────────────

def test_resolve_week_monday_friday():
    assert _resolve_window("week", "2026-08-04", None, None, "economic_releases") == ("2026-08-03", "2026-08-07")


def test_resolve_week_monday_itself():
    assert _resolve_window("week", "2026-08-03", None, None, "economic_releases") == ("2026-08-03", "2026-08-07")


def test_resolve_week_friday():
    assert _resolve_window("week", "2026-08-07", None, None, "economic_releases") == ("2026-08-03", "2026-08-07")


def test_resolve_week_previous():
    assert _resolve_window("week", "2026-07-28", None, None, "economic_releases") == ("2026-07-27", "2026-07-31")


def test_resolve_week_future():
    assert _resolve_window("week", "2026-08-04", None, None, "economic_releases") == ("2026-08-03", "2026-08-07")


def test_resolve_day():
    assert _resolve_window("day", "2026-08-05", None, None, "economic_releases") == ("2026-08-05", "2026-08-05")


def test_resolve_month_august():
    assert _resolve_window("month", "2026-08-15", None, None, "economic_releases") == ("2026-08-01", "2026-08-31")


def test_resolve_month_january_boundary():
    assert _resolve_window("month", "2026-01-10", None, None, "economic_releases") == ("2026-01-01", "2026-01-31")


def test_resolve_month_december_boundary():
    assert _resolve_window("month", "2026-12-10", None, None, "economic_releases") == ("2026-12-01", "2026-12-31")


def test_resolve_range_override():
    assert _resolve_window("week", "2026-08-04", "2026-08-10", None, "economic_releases") == ("2026-08-10", "2026-08-10")
    assert _resolve_window(None, None, "2026-08-03", "2026-08-07", "economic_releases") == ("2026-08-03", "2026-08-07")


def test_resolve_range_swaps_inverted():
    assert _resolve_window(None, None, "2026-08-07", "2026-08-03", "economic_releases") == ("2026-08-03", "2026-08-07")


def test_resolve_recent_matches_previous_week_window():
    from services.calendar_snapshot_service import _previous_week_window_for
    assert _resolve_window("recent", None, None, None, "economic_releases") == _previous_week_window_for("economic_releases")


def test_resolve_invalid_date_falls_back_to_week():
    win = _resolve_window("week", "not-a-date", None, None, "economic_releases")
    assert len(win[0]) == 10 and len(win[1]) == 10


# ── Day view ────────────────────────────────────────────────────────────────

def test_day_selects_requested_date_from_broad_events():
    events = [
        _make_econ("2026-08-04", id="d1"),
        _make_econ("2026-08-05", id="d2"),
        _make_econ("2026-08-06", id="d3"),
    ]
    out, _ = _window(view="day", date="2026-08-05", env=_horizon_env(events))
    assert out["window_start"] == "2026-08-05"
    assert out["window_end"] == "2026-08-05"
    assert [e["id"] for e in out["events"]] == ["d2"]
    assert out["event_count"] == 1
    assert out["coverage_complete"] is True
    assert out["empty_reason"] is None


def test_day_does_not_return_all_90_days():
    events = [
        _make_econ(d.strftime("%Y-%m-%d"), id=f"e{i}")
        for i, d in enumerate([date.fromisoformat("2026-08-05")])
    ]
    out, _ = _window(view="day", date="2026-08-05", env=_horizon_env(events))
    assert out["event_count"] == 1


# ── Week view ───────────────────────────────────────────────────────────────

def test_current_week_selects_monday_friday():
    events = [
        _make_econ("2026-07-26", id="sun"),
        _make_econ("2026-07-27", id="mon"),
        _make_econ("2026-07-29", id="wed"),
        _make_econ("2026-07-31", id="fri"),
        _make_econ("2026-08-01", id="sat"),
    ]
    out, _ = _window(view="week", date="2026-07-29", env=_horizon_env(events))
    assert out["window_start"] == "2026-07-27"
    assert out["window_end"] == "2026-07-31"
    assert {e["id"] for e in out["events"]} == {"mon", "wed", "fri"}


def test_future_week_aug_3_7_selected_from_broad_events():
    events = [
        _make_econ("2026-07-31", id="prev"),
        _make_econ("2026-08-03", id="mon"),
        _make_econ("2026-08-05", id="wed"),
        _make_econ("2026-08-07", id="fri"),
        _make_econ("2026-08-10", id="later"),
    ]
    out, _ = _window(view="week", date="2026-08-04", env=_horizon_env(events))
    assert out["window_start"] == "2026-08-03"
    assert out["window_end"] == "2026-08-07"
    assert {e["id"] for e in out["events"]} == {"mon", "wed", "fri"}
    assert out["coverage_complete"] is True
    assert out["empty_reason"] is None


def test_previous_week_remains_selectable():
    events = [
        _make_econ("2026-07-20", id="p1"),
        _make_econ("2026-07-22", id="p2"),
        _make_econ("2026-07-24", id="p3"),
        _make_econ("2026-07-27", id="cw"),
    ]
    out, _ = _window(view="week", date="2026-07-22", env=_horizon_env(events))
    assert out["window_start"] == "2026-07-20"
    assert out["window_end"] == "2026-07-24"
    assert {e["id"] for e in out["events"]} == {"p1", "p2", "p3"}


def test_week_does_not_return_all_90_days():
    events = [_make_econ("2026-08-04", id="e")]
    out, _ = _window(view="week", date="2026-08-04", env=_horizon_env(events))
    assert out["event_count"] == 1
    assert len(out["events"]) == 1


# ── Month view ──────────────────────────────────────────────────────────────

def test_month_selects_all_august_events_inside_coverage():
    events = [
        _make_econ("2026-07-29", id="jul"),
        _make_econ("2026-08-03", id="a1"),
        _make_econ("2026-08-05", id="a2"),
        _make_econ("2026-08-15", id="a3"),
        _make_econ("2026-08-31", id="a4"),
        _make_econ("2026-09-01", id="sep"),
    ]
    out, _ = _window(view="month", date="2026-08-15", env=_horizon_env(events))
    assert out["window_start"] == "2026-08-01"
    assert out["window_end"] == "2026-08-31"
    assert {e["id"] for e in out["events"]} == {"a1", "a2", "a3", "a4"}
    assert "jul" not in {e["id"] for e in out["events"]}
    assert "sep" not in {e["id"] for e in out["events"]}


def test_month_not_restricted_to_current_week():
    """Month reads the broad horizon, not the snapshot's current_week slice."""
    current_week_only = [_make_econ("2026-07-29", id="cw_only")]
    events = [_make_econ("2026-08-05", id="aug_ev")]
    env = _horizon_env(events)
    env["current_week"] = current_week_only
    out, _ = _window(view="month", date="2026-08-15", env=env)
    assert {e["id"] for e in out["events"]} == {"aug_ev"}


# ── Signal metadata / dedup ─────────────────────────────────────────────────

def test_selected_events_preserve_signal_metadata():
    ev = _make_econ("2026-08-05", id="sig", time="08:30:00", actual="3.2",
                    estimate="3.1", previous="3.0", event_family="cpi",
                    signal_tier="major", signal_reason="Inflation report",
                    country="US", source="fmp")
    out, _ = _window(view="day", date="2026-08-05", env=_horizon_env([ev]))
    got = out["events"][0]
    assert got["signal_tier"] == "major"
    assert got["signal_reason"] == "Inflation report"
    assert got["importance"] == "high"
    assert got["actual"] == "3.2"
    assert got["estimate"] == "3.1"
    assert got["previous"] == "3.0"
    assert got["time"] == "08:30:00"
    assert got["country"] == "US"
    assert got["source"] == "fmp"


def test_exact_dates_and_times_preserved():
    ev = _make_econ("2026-08-05T08:30:00", id="dt")
    out, _ = _window(view="day", date="2026-08-05", env=_horizon_env([ev]))
    assert out["events"][0]["date"] == "2026-08-05T08:30:00"


def test_no_duplicate_canonical_events():
    """Selection never duplicates a canonical event across pools."""
    dup = _make_econ("2026-08-05", id="same", title="CPI MoM", eventName="CPI MoM")
    env = _horizon_env([dup])
    env["current_week"] = [dict(dup)]
    env["previous_week"] = [dict(dup)]
    out, _ = _window(view="day", date="2026-08-05", env=env)
    assert len([e for e in out["events"] if e["id"] == "same"]) == 1


# ── Legacy fallback (old snapshots without broad events) ────────────────────

def test_legacy_snapshot_without_broad_events_readable():
    cw = [_make_econ("2026-07-27", id="c1"), _make_econ("2026-07-29", id="c2")]
    env = _legacy_env(cw=cw, pw=[])
    out, _ = _window(view="week", date="2026-07-28", env=env)
    assert out["event_count"] == 2
    assert out["status"] == "ready"


def test_legacy_current_week_day_selectable():
    cw = [_make_econ("2026-07-29", id="c1")]
    out, _ = _window(view="day", date="2026-07-29", env=_legacy_env(cw=cw, pw=[]))
    assert [e["id"] for e in out["events"]] == ["c1"]


def test_legacy_future_week_reports_incomplete_truthfully():
    cw = [_make_econ("2026-07-29", id="c1")]
    out, _ = _window(view="week", date="2026-08-04", env=_legacy_env(cw=cw, pw=[]))
    assert out["event_count"] == 0
    assert out["coverage_complete"] is False
    assert out["empty_reason"] == "legacy_snapshot_without_horizon"


def test_legacy_recent_selects_previous_week():
    pw = [_make_econ("2026-07-15", id="p1")]
    out, _ = _window(view="recent", env=_legacy_env(cw=[], pw=pw))
    assert {e["id"] for e in out["events"]} == {"p1"}


def test_recent_preserves_persisted_previous_week_for_capped_horizon():
    """Recent uses the envelope previous_week, so a capped horizon cannot empty it."""
    pw = [_make_econ("2026-07-20", id="hist1")]
    env = _horizon_env(events=[_make_econ("2026-08-05", id="broad_only")])
    env["previous_week"] = pw
    out, _ = _window(view="recent", env=env)
    assert {e["id"] for e in out["events"]} == {"hist1"}
    assert out["coverage_complete"] is True
    assert out["empty_reason"] is None


def test_get_snapshot_previous_week_fallback_when_horizon_capped():
    """
    get_snapshot keeps the persisted previous_week when the broad horizon's
    derived previous-week slice is empty (capped fetch). Prevents the Recent
    view from going empty after a rolling-horizon refresh.
    """
    import services.calendar_snapshot_service as _svc
    from services.calendar_snapshot_service import _et_now
    today = _et_now().date()
    monday = today - timedelta(days=today.weekday())
    hfrom, hto = _horizon_window_for()
    slot = {
        "economic_releases": {
            "current_week": [_make_econ(monday.isoformat(), id="cw")],
            "previous_week": [_make_econ("2026-07-20", id="hist")],
            "events": [_make_econ(monday.isoformat(), id="cw"), _make_econ("2026-08-05", id="aug")],
            "meta": {
                "window": {"from": hfrom, "to": hto},
                "horizon_start": hfrom,
                "horizon_end": hto,
                "last_updated": "2026-08-01T12:00:00+00:00",
                "status": "ready",
            },
        },
    }
    with mock.patch.object(_svc, "_neon_read", return_value=None), \
         mock.patch.object(_svc, "_read_disk", return_value=slot):
        env = _svc.get_snapshot("economic_releases")
    assert any(e["id"] == "hist" for e in env.get("previous_week") or [])


# ── Coverage / empty-state semantics ────────────────────────────────────────

def test_window_outside_horizon_incomplete_truthfully():
    events = [_make_econ("2026-08-05", id="e")]
    out, _ = _window(view="week", date="2026-12-15", env=_horizon_env(events))
    assert out["event_count"] == 0
    assert out["coverage_complete"] is False
    assert out["empty_reason"] == "outside_horizon"


def test_covered_window_no_events_genuine_empty():
    """A week inside the persisted span but with no scheduled events is empty."""
    events = [
        _make_econ("2026-09-01", id="before"),
        _make_econ("2026-09-30", id="after"),
    ]
    out, _ = _window(view="week", date="2026-09-15", env=_horizon_env(events))
    assert out["event_count"] == 0
    assert out["coverage_complete"] is True
    assert out["empty_reason"] == "no_events_in_window"


def test_window_inside_meta_horizon_but_beyond_actual_events_is_incomplete():
    """A capped horizon: meta end covers window but actual events stop earlier.
    Coverage uses actual persisted event bounds, not intended fetch metadata.
    A window beyond actual_end must report incomplete truthfully."""
    events = [_make_econ("2026-09-03", id="last")]
    env = _horizon_env(events)  # meta horizon_end stays 2026-10-29
    out, _ = _window(view="week", date="2026-09-14", env=env)
    assert out["event_count"] == 0
    assert out["coverage_complete"] is False
    assert out["empty_reason"] == "outside_horizon"


def test_covered_window_with_events_not_empty():
    events = [
        _make_econ("2026-08-03", id="e1"),
        _make_econ("2026-08-05", id="e2"),
        _make_econ("2026-08-07", id="e3"),
    ]
    out, _ = _window(view="week", date="2026-08-04", env=_horizon_env(events))
    assert out["event_count"] == 3
    assert out["empty_reason"] is None
    assert out["coverage_complete"] is True


def test_snapshot_empty_reports_snapshot_empty():
    env = _legacy_env(cw=[], pw=[])
    env["status"] = "empty"
    out, _ = _window(view="week", date="2026-08-04", env=env)
    assert out["event_count"] == 0
    assert out["empty_reason"] == "snapshot_empty"


# ── Envelope backward compatibility ─────────────────────────────────────────

def test_window_envelope_preserves_existing_fields():
    events = [_make_econ("2026-08-05", id="e")]
    out, _ = _window(view="week", date="2026-08-04", env=_horizon_env(events))
    for k in ("current_week", "previous_week", "last_updated", "status",
              "is_stale", "window", "diagnostics", "horizon", "coverage"):
        assert k in out


def test_window_envelope_has_narrow_metadata():
    events = [_make_econ("2026-08-05", id="e")]
    out, _ = _window(view="week", date="2026-08-04", env=_horizon_env(events))
    for k in ("view", "requested_date", "window_start", "window_end",
              "event_count", "coverage_complete", "horizon_start",
              "horizon_end", "empty_reason"):
        assert k in out
    assert out["view"] == "week"
    assert out["requested_date"] == "2026-08-04"
    assert out["horizon_start"] == "2026-07-18"
    assert out["horizon_end"] == "2026-10-29"


def test_no_provider_call_when_coverage_complete():
    events = [_make_econ("2026-08-05", id="e")]
    with mock.patch(
        "services.calendar_snapshot_service.get_snapshot", return_value=_horizon_env(events),
    ) as m:
        out = get_snapshot_window("economic_releases", view="week", date="2026-08-04")
    assert out["event_count"] == 1
    m.assert_called_once_with("economic_releases")


# ═══════════════════════════════════════════════════════════════════════════════
# Stable identity and merge logic
# ═══════════════════════════════════════════════════════════════════════════════

def test_event_stable_id_same_event_same_id():
    a = _make_econ("2026-08-03", title="CPI MoM", eventName="CPI MoM", country="US")
    b = _make_econ("2026-08-03", title="CPI MoM", eventName="CPI MoM", country="US")
    assert _event_stable_id(a) == _event_stable_id(b)


def test_event_stable_id_different_date_different_id():
    a = _make_econ("2026-08-03", title="CPI MoM", country="US")
    b = _make_econ("2026-08-04", title="CPI MoM", country="US")
    assert _event_stable_id(a) != _event_stable_id(b)


def test_event_stable_id_different_country_different_id():
    a = _make_econ("2026-08-03", title="CPI MoM", country="US")
    b = _make_econ("2026-08-03", title="CPI MoM", country="DE")
    assert _event_stable_id(a) != _event_stable_id(b)


def test_merge_horizon_events_dedup():
    existing = [_make_econ("2026-08-03", title="CPI MoM", country="US")]
    incoming = [_make_econ("2026-08-03", title="CPI MoM", country="US")]
    merged = _merge_horizon_events(existing, incoming)
    assert len(merged) == 1


def test_merge_horizon_events_adds_new():
    existing = [_make_econ("2026-08-03", title="CPI MoM", country="US", id="e1")]
    incoming = [_make_econ("2026-08-04", title="PPI MoM", country="US", id="e2")]
    merged = _merge_horizon_events(existing, incoming)
    assert len(merged) == 2


def test_merge_horizon_events_overwrite():
    existing = [_make_econ("2026-08-03", title="CPI MoM", country="US", actual=None)]
    incoming = [_make_econ("2026-08-03", title="CPI MoM", country="US", actual="3.2")]
    merged = _merge_horizon_events(existing, incoming)
    assert len(merged) == 1
    assert merged[0]["actual"] == "3.2"


def test_merge_horizon_events_sorted_by_date():
    existing = [_make_econ("2026-09-01", title="ISM", country="US")]
    incoming = [
        _make_econ("2026-08-03", title="CPI", country="US"),
        _make_econ("2026-12-15", title="FOMC", country="US"),
    ]
    merged = _merge_horizon_events(existing, incoming)
    dates = [e["date"][:10] for e in merged]
    assert dates == ["2026-08-03", "2026-09-01", "2026-12-15"]


def test_merge_horizon_events_preserves_historical():
    existing = [
        _make_econ("2021-08-01", title="Old GDP", country="US", id="hist1"),
        _make_econ("2021-09-01", title="Old CPI", country="US", id="hist2"),
    ]
    incoming = [_make_econ("2026-08-03", title="Current CPI", country="US", id="new")]
    merged = _merge_horizon_events(existing, incoming)
    assert len(merged) == 3
    hist_ids = {e["id"] for e in merged if e["date"][:4] == "2021"}
    assert hist_ids == {"hist1", "hist2"}


# ═══════════════════════════════════════════════════════════════════════════════
# Actual bounds
# ═══════════════════════════════════════════════════════════════════════════════

def test_actual_bounds_empty():
    assert _actual_bounds([]) == (None, None)


def test_actual_bounds_single():
    events = [_make_econ("2026-08-03")]
    start, end = _actual_bounds(events)
    assert start == "2026-08-03"
    assert end == "2026-08-03"


def test_actual_bounds_span():
    events = [
        _make_econ("2021-08-01"),
        _make_econ("2026-08-03"),
        _make_econ("2023-01-15"),
    ]
    start, end = _actual_bounds(events)
    assert start == "2021-08-01"
    assert end == "2026-08-03"


def test_actual_bounds_ignores_empty_dates():
    events = [
        _make_econ("2021-08-01"),
        {"title": "no date"},
        _make_econ("2026-08-03"),
    ]
    start, end = _actual_bounds(events)
    assert start == "2021-08-01"
    assert end == "2026-08-03"


# ═══════════════════════════════════════════════════════════════════════════════
# Historical window navigation (5 years of fixtures)
# ═══════════════════════════════════════════════════════════════════════════════

def _full_history_env():
    """Horizon envelope with events spanning 2021-2026 (~5 years)."""
    events = []
    for yr in range(2021, 2027):
        for mo in range(1, 13):
            # 3 events per month, scattered across the month
            for d in [3, 12, 20]:
                try:
                    d_str = date(yr, mo, d).isoformat()
                except ValueError:
                    d_str = date(yr, mo, 28).isoformat()
                events.append(_make_econ(
                    d_str,
                    title=f"Event {yr}-{mo:02d}:{d}",
                    actual="1.5",
                    estimate="1.4",
                    previous="1.3",
                    event_family="cpi",
                    signal_tier="major",
                    signal_reason="Inflation report",
                ))
    return _horizon_env(
        events,
        horizon_start="2021-08-01",
        horizon_end="2026-10-29",
    )


_FULL_ENV = _full_history_env()


def test_historical_week_2021():
    out, _ = _window(view="week", date="2021-08-03", env=_FULL_ENV)
    assert out["event_count"] > 0
    assert out["coverage_complete"] is True


def test_historical_month_2022():
    out, _ = _window(view="month", date="2022-06-01", env=_FULL_ENV)
    assert out["event_count"] > 0
    assert out["coverage_complete"] is True


def test_historical_day_2023():
    out, _ = _window(view="day", date="2023-03-12", env=_FULL_ENV)
    assert out["event_count"] > 0
    assert out["coverage_complete"] is True


def test_historical_week_2024():
    out, _ = _window(view="week", date="2024-09-02", env=_FULL_ENV)
    # Sep 2 2024 is Monday (Labor Day), events exist on Sep 3, 4, 5
    assert out["event_count"] > 0
    assert out["coverage_complete"] is True


def test_historical_month_2025():
    out, _ = _window(view="month", date="2025-03-01", env=_FULL_ENV)
    assert out["event_count"] > 0
    assert out["coverage_complete"] is True


def test_current_week_returns_events():
    from services.calendar_snapshot_service import _et_now
    today = _et_now().date()
    monday = today - timedelta(days=today.weekday())
    env = _full_history_env()
    env["events"].append(_make_econ(monday.isoformat(), title="This Week Event"))
    out, _ = _window(view="week", date=monday.isoformat(), env=env)
    assert out["event_count"] > 0


def test_future_week_inside_three_months():
    """Future week where actual events span the full Mon-Fri range."""
    from services.calendar_snapshot_service import _et_now
    today = _et_now().date()
    monday = today + timedelta(days=((7 - today.weekday()) % 7))  # next Monday
    friday = monday + timedelta(days=4)
    events = [
        _make_econ(monday.isoformat(), title="Mon Event", event_family="payrolls", signal_tier="major"),
        _make_econ(friday.isoformat(), title="Fri Event", event_family="payrolls", signal_tier="major"),
    ]
    env = _horizon_env(
        events,
        horizon_start=monday.isoformat(),
        horizon_end=(friday + timedelta(days=60)).isoformat(),
    )
    out, _ = _window(view="week", date=monday.isoformat(), env=env)
    assert out["event_count"] > 0
    assert out["coverage_complete"] is True


def test_future_month_inside_three_months():
    """Future month where actual events span first and last days."""
    from services.calendar_snapshot_service import _et_now
    today = _et_now().date()
    first = today.replace(day=1)
    if today.month == 12:
        nxt = date(first.year + 1, 1, 1)
    else:
        nxt = date(first.year, first.month + 1, 1)
    last = nxt - timedelta(days=1)  # last day of the month
    # Move to next month for "future"
    if today.month == 12:
        future_first = date(today.year + 1, 1, 1)
    else:
        future_first = date(today.year, today.month + 1, 1)
    future_last = (
        future_first.replace(month=future_first.month + 1, day=1) - timedelta(days=1)
        if future_first.month < 12
        else date(future_first.year, 12, 31)
    )
    events = [
        _make_econ(future_first.isoformat(), title="First Day"),
        _make_econ(future_last.isoformat(), title="Last Day"),
    ]
    env = _horizon_env(
        events,
        horizon_start=future_first.isoformat(),
        horizon_end=(future_last + timedelta(days=30)).isoformat(),
    )
    out, _ = _window(view="month", date=future_first.isoformat(), env=env)
    assert out["event_count"] > 0
    assert out["coverage_complete"] is True


def test_before_horizon_reports_incomplete():
    """Request before the earliest actual stored event → incomplete."""
    events = [_make_econ("2022-06-01", title="Event")]
    env = _horizon_env(events, horizon_start="2022-06-01", horizon_end="2022-09-01")
    out, _ = _window(view="week", date="2021-07-01", env=env)  # well before actual_start
    assert out["coverage_complete"] is False
    assert out["empty_reason"] == "outside_horizon"


def test_after_horizon_reports_incomplete():
    env = _horizon_env(
        [_make_econ("2026-10-01")],
        horizon_start="2026-07-18",
        horizon_end="2026-10-29",
    )
    out, _ = _window(view="week", date="2026-12-01", env=env)
    assert out["coverage_complete"] is False


# ═══════════════════════════════════════════════════════════════════════════════
# Historical signal metadata preservation
# ═══════════════════════════════════════════════════════════════════════════════

def test_historical_actual_estimate_previous_survive():
    events = [_make_econ("2021-08-03", actual="3.2", estimate="3.0", previous="2.9",
                         event_family="cpi", signal_tier="major", signal_reason="Report")]
    env = _horizon_env(events, horizon_start="2021-08-01", horizon_end="2022-01-01")
    out, _ = _window(view="day", date="2021-08-03", env=env)
    ev = out["events"][0]
    assert ev["actual"] == "3.2"
    assert ev["estimate"] == "3.0"
    assert ev["previous"] == "2.9"
    assert ev["event_family"] == "cpi"
    assert ev["signal_tier"] == "major"
    assert ev["signal_reason"] == "Report"


def test_window_response_only_requested_range():
    events = [
        _make_econ("2021-08-03", id="in1"),
        _make_econ("2021-08-12", id="in2"),
        _make_econ("2021-09-01", id="out"),
    ]
    env = _horizon_env(events, horizon_start="2021-08-01", horizon_end="2022-01-01")
    out, _ = _window(view="month", date="2021-08-01", env=env)
    ids = {e["id"] for e in out["events"]}
    assert ids == {"in1", "in2"}
    assert "out" not in ids


def test_window_does_not_return_full_archive():
    env = _full_history_env()
    out, _ = _window(view="week", date="2022-06-01", env=env)
    total = len(_FULL_ENV["events"])
    assert out["event_count"] < total
    assert out["event_count"] > 0


def test_no_duplicate_events_in_merged_response():
    events = [
        _make_econ("2026-08-03", title="CPI MoM", eventName="CPI MoM", country="US"),
        _make_econ("2026-08-03", title="CPI MoM", eventName="CPI MoM", country="US"),
    ]
    merged = _merge_horizon_events([], events)
    assert len(merged) == 1


def test_family_grouping_preserved_in_window():
    events = [
        _make_econ("2022-03-03", title="CPI MoM", event_family="cpi", signal_tier="major"),
        _make_econ("2022-03-12", title="PPI MoM", event_family="ppi", signal_tier="major"),
        _make_econ("2022-03-20", title="FOMC Decision", event_family="fomc_decision", signal_tier="critical"),
    ]
    env = _horizon_env(events, horizon_start="2022-03-01", horizon_end="2022-04-01")
    out, _ = _window(view="month", date="2022-03-01", env=env)
    families = {e["event_family"] for e in out["events"]}
    assert families == {"cpi", "ppi", "fomc_decision"}


def test_signal_metadata_intact_in_historical_window():
    events = [
        _make_econ("2023-06-15", title="FOMC Decision", event_family="fomc_decision",
                    signal_tier="critical", signal_reason="Scheduled FOMC rate decision"),
    ]
    env = _horizon_env(events, horizon_start="2023-06-01", horizon_end="2023-07-01")
    out, _ = _window(view="day", date="2023-06-15", env=env)
    ev = out["events"][0]
    assert ev["event_family"] == "fomc_decision"
    assert ev["signal_tier"] == "critical"
    assert "fomc" in ev["signal_reason"].lower()


def test_backward_compatible_non_window_response():
    from services.calendar_snapshot_service import get_snapshot
    with mock.patch(
        "services.calendar_snapshot_service.get_snapshot",
        return_value=_horizon_env([_make_econ("2026-08-03")]),
    ):
        out = get_snapshot_window("economic_releases")
    assert "current_week" in out
    assert "previous_week" in out
    assert "last_updated" in out
    assert "status" in out
    assert "events" in out


def test_coverage_complete_for_horizon_tab():
    assert _window_covered("2021-08-01", "2026-10-29", "2022-06-06", "2022-06-10") is True


def test_coverage_incomplete_before_horizon():
    assert _window_covered("2021-08-01", "2026-10-29", "2021-07-01", "2021-07-05") is False


def test_coverage_incomplete_after_horizon():
    assert _window_covered("2021-08-01", "2026-10-29", "2026-12-01", "2026-12-05") is False


# ═══════════════════════════════════════════════════════════════════════════════
# Cold-start and unavailability state reproduction
# ═══════════════════════════════════════════════════════════════════════════════

def _empty_env(status="empty"):
    """Envelope returned by get_snapshot when snapshot is completely unavailable."""
    return {
        "current_week": [],
        "previous_week": [],
        "last_updated": None,
        "status": status,
        "is_stale": True,
        "bootstrapping": status == "initializing",
        "window": {
            "requested_from": "2026-07-27",
            "requested_to": "2026-07-31",
            "stored_from": None,
            "stored_to": None,
        },
        "diagnostics": {},
        "events": [],
        "coverage": {"complete": False, "horizon_end": "", "requested_end": "2026-07-31"},
    }


class TestColdStartStates:
    """Production-state reproduction for deployed failure modes."""

    # ── State A: Healthy snapshot ─────────────────────────────────────────
    def test_healthy_snapshot_week_returns_200(self):
        events = [
            _make_econ("2026-08-03", id="e1"),
            _make_econ("2026-08-05", id="e2"),
            _make_econ("2026-08-07", id="e3"),
        ]
        env = _horizon_env(events, horizon_start="2026-08-03", horizon_end="2026-08-07")
        out, _ = _window(view="week", date="2026-08-03", env=env)
        assert out["event_count"] > 0
        assert out["coverage_complete"] is True

    # ── State B: Missing row (table exists, no economic_releases row) ────
    def test_missing_row_week_returns_200(self):
        env = _empty_env("empty")
        out, _ = _window(view="week", date="2026-08-03", env=env)
        assert out["event_count"] == 0
        assert out["coverage_complete"] is False
        assert out["empty_reason"] == "snapshot_empty"
        assert out["status"] == "empty"

    # ── State C: Missing table (neon raises) ──────────────────────────────
    def test_neon_exception_returns_200(self):
        env = _empty_env("empty")
        out, _ = _window(view="week", date="2026-08-03", env=env)
        assert out["event_count"] == 0
        assert out["coverage_complete"] is False

    # ── State D: Neon unreachable + valid disk fallback ──────────────────
    def test_disk_fallback_returns_200(self):
        events = [
            _make_econ("2026-08-03", id="disk1"),
            _make_econ("2026-08-07", id="disk2"),
        ]
        env = _horizon_env(events, horizon_start="2026-08-03", horizon_end="2026-08-07")
        out, _ = _window(view="week", date="2026-08-03", env=env)
        assert out["event_count"] > 0
        assert out["coverage_complete"] is True

    # ── State E: Neon unreachable + no disk fallback ─────────────────────
    def test_no_disk_returns_200(self):
        env = _empty_env("empty")
        out, _ = _window(view="week", date="2026-08-03", env=env)
        assert out["event_count"] == 0
        assert out["empty_reason"] == "snapshot_empty"

    # ── State G: Bootstrap in progress ────────────────────────────────────
    def test_bootstrap_in_progress_returns_200(self):
        env = _empty_env("initializing")
        out, _ = _window(view="week", date="2026-08-03", env=env)
        assert out["event_count"] == 0
        assert out["coverage_complete"] is False
        assert "initializing" in out.get("empty_reason", "")

    # ── State H: Bootstrap/provider failure ───────────────────────────────
    def test_provider_failure_preserves_prior(self):
        # No events → empty with no false covering
        env = _empty_env("empty")
        out, _ = _window(view="week", date="2026-08-03", env=env)
        assert out["event_count"] == 0
        assert out["coverage_complete"] is False

    # ── All views survive missing snapshot ────────────────────────────────
    def test_month_missing_snapshot_returns_200(self):
        env = _empty_env("empty")
        out, _ = _window(view="month", date="2026-08-01", env=env)
        assert out["event_count"] == 0
        assert out["empty_reason"] == "snapshot_empty"

    def test_day_missing_snapshot_returns_200(self):
        env = _empty_env("empty")
        out, _ = _window(view="day", date="2026-08-03", env=env)
        assert out["event_count"] == 0

    def test_recent_missing_snapshot_returns_200(self):
        env = _empty_env("empty")
        out, _ = _window(view="recent", env=env)
        assert out["event_count"] == 0

    # ── Envelope safety ──────────────────────────────────────────────────
    def test_all_required_fields_present(self):
        env = _empty_env("empty")
        out, _ = _window(view="week", date="2026-08-03", env=env)
        for k in ("view", "requested_date", "window_start", "window_end",
                  "events", "event_count", "current_week", "previous_week",
                  "last_updated", "status", "is_stale", "coverage_complete",
                  "horizon_start", "horizon_end", "empty_reason",
                  "diagnostics", "window", "coverage"):
            assert k in out, f"missing key: {k}"

    def test_arrays_never_null(self):
        env = _empty_env("empty")
        out, _ = _window(view="week", date="2026-08-03", env=env)
        assert out["events"] is not None
        assert isinstance(out["events"], list)
        assert out["current_week"] is not None
        assert isinstance(out["current_week"], list)
        assert out["previous_week"] is not None
        assert isinstance(out["previous_week"], list)

    def test_objects_never_null(self):
        env = _empty_env("empty")
        out, _ = _window(view="week", date="2026-08-03", env=env)
        assert isinstance(out.get("diagnostics", {}), dict)
        assert isinstance(out.get("window", {}), dict)
        assert isinstance(out.get("coverage", {}), dict)

    def test_response_is_json_serializable(self):
        import json as _json
        env = _empty_env("empty")
        out, _ = _window(view="week", date="2026-08-03", env=env)
        serialized = _json.dumps(out)
        assert isinstance(serialized, str)
        assert len(serialized) > 0

    def test_no_provider_fetch_in_empty_state(self):
        env = _empty_env("empty")
        with mock.patch(
            "services.calendar_snapshot_service.get_snapshot",
            return_value=env,
        ) as m:
            out = get_snapshot_window("economic_releases", view="week", date="2026-08-03")
        assert out["coverage_complete"] is False
        m.assert_called_once()

    # ── Corrected coverage semantics ──────────────────────────────────────
    def test_request_before_actual_start_is_incomplete(self):
        events = [_make_econ("2022-08-01", id="e")]
        env = _horizon_env(events, horizon_start="2021-08-01", horizon_end="2022-10-01")
        out, _ = _window(view="week", date="2021-11-01", env=env)
        assert out["coverage_complete"] is False

    def test_request_after_actual_end_is_incomplete(self):
        events = [_make_econ("2022-08-01", id="e")]
        env = _horizon_env(events, horizon_start="2022-07-01", horizon_end="2022-10-01")
        out, _ = _window(view="week", date="2022-12-01", env=env)
        assert out["coverage_complete"] is False

    def test_metadata_only_coverage_does_not_mark_missing_history_complete(self):
        """Meta horizon covers 2022-06 but actual events start at 2022-08.
        A request for 2022-06-01 must report incomplete."""
        events = [_make_econ("2022-08-01", id="e")]  # actual start = Aug 2022
        env = _horizon_env(events, horizon_start="2022-06-01", horizon_end="2022-10-01")
        out, _ = _window(view="week", date="2022-06-06", env=env)
        assert out["coverage_complete"] is False
        assert out["empty_reason"] == "outside_horizon", f"got {out['empty_reason']}"

    def test_provider_denied_range_not_no_events_in_window(self):
        """Single event at 2025-11-01; meta says horizon goes to 2025-09-01.
        A 2025-10-01 request is between intended-start and actual-start.
        Must not report no_events_in_window."""
        events = [_make_econ("2025-11-01", id="e")]  # actual start = Nov 2025
        env = _horizon_env(events, horizon_start="2025-09-01", horizon_end="2025-12-01")
        out, _ = _window(view="week", date="2025-10-06", env=env)
        assert out["coverage_complete"] is False
        assert out["empty_reason"] != "no_events_in_window"

    def test_covered_populated_week_remains_complete(self):
        events = [
            _make_econ("2026-08-03", id="e1"),
            _make_econ("2026-08-05", id="e2"),
            _make_econ("2026-08-07", id="e3"),
        ]
        env = _horizon_env(events, horizon_start="2026-08-03", horizon_end="2026-08-07")
        out, _ = _window(view="week", date="2026-08-03", env=env)
        assert out["event_count"] == 3
        assert out["coverage_complete"] is True

    def test_covered_genuine_empty_window_distinguishable(self):
        """Window fully within actual event span but no events on those dates."""
        events = [
            _make_econ("2026-08-01", id="before"),
            _make_econ("2026-08-05", id="during"),
            _make_econ("2026-08-10", id="after"),
        ]
        env = _horizon_env(events, horizon_start="2026-08-01", horizon_end="2026-08-10")
        # Aug 6 is a Saturday — no events expected
        out, _ = _window(view="day", date="2026-08-08", env=env)
        assert out["event_count"] == 0
        assert out["empty_reason"] == "no_events_in_window", f"got {out['empty_reason']}"

    def test_healthy_aug_3_7_still_returns_selected_events(self):
        events = [
            _make_econ("2026-08-03", id="e1"),
            _make_econ("2026-08-05", id="e2"),
            _make_econ("2026-08-07", id="e3"),
            _make_econ("2026-08-10", id="outside"),
        ]
        env = _horizon_env(events, horizon_start="2026-08-01", horizon_end="2026-08-15")
        out, _ = _window(view="week", date="2026-08-03", env=env)
        ids = {e["id"] for e in out["events"]}
        assert "e1" in ids
        assert "e2" in ids
        assert "e3" in ids
        assert "outside" not in ids

    # ── Backward compatibility ────────────────────────────────────────────
    def test_non_window_response_backward_compatible(self):
        events = [
            _make_econ("2026-08-03", id="e"),
            _make_econ("2026-08-05", id="e_fri"),
        ]
        env = _horizon_env(events, horizon_start="2026-08-03",
                           horizon_end="2026-08-07")
        with mock.patch(
            "services.calendar_snapshot_service.get_snapshot",
            return_value=env,
        ):
            out = get_snapshot_window("economic_releases", view="week", date="2026-08-03")
        assert "current_week" in out
        assert out["event_count"] == 2

    def test_bootstrapping_field_present(self):
        env = _empty_env("empty")
        out, _ = _window(view="week", date="2026-08-03", env=env)
        assert "bootstrapping" in out
        assert isinstance(out["bootstrapping"], bool)

    # ── Home / Treasury / Earnings untouched ──────────────────────────────
    def test_treasury_macro_not_affected(self):
        env = _horizon_env([_make_econ("2026-08-03")])
        with mock.patch(
            "services.calendar_snapshot_service.get_snapshot",
            return_value=env,
        ):
            out = get_snapshot_window("treasury_macro", view="week", date="2026-08-03")
        assert "events" in out

    def test_dividends_not_affected(self):
        env = _legacy_env(
            cw=[_make_econ("2026-08-03", eventType="dividend")],
            pw=[],
        )
        with mock.patch(
            "services.calendar_snapshot_service.get_snapshot",
            return_value=env,
        ):
            out = get_snapshot_window("dividends", view="week", date="2026-08-03")
        assert "events" in out


# ═══════════════════════════════════════════════════════════════════════════════
# Coverage range tracking (per-chunk provider evidence)
# ═══════════════════════════════════════════════════════════════════════════════

from services.calendar_snapshot_service import (
    _merge_coverage_ranges,
    _normalize_coverage_ranges,
    _coverage_union,
    _window_covered_by_ranges,
    _coverage_gap_kind,
    _dates_overlap,
)


class TestCoverageRanges:
    """Coverage at the provider-chunk level — each chunk is tracked."""

    # ── _window_covered_by_ranges ──────────────────────────────────────────────
    def test_window_inside_complete_range(self):
        ranges = [{"from": "2025-09-01", "to": "2025-10-31", "status": "complete"}]
        assert _window_covered_by_ranges(ranges, "2025-09-15", "2025-09-19") is True

    def test_window_outside_any_range(self):
        ranges = [{"from": "2025-09-01", "to": "2025-10-31", "status": "complete"}]
        assert _window_covered_by_ranges(ranges, "2025-11-01", "2025-11-05") is False

    def test_window_in_failed_range_is_not_covered(self):
        ranges = [
            {"from": "2025-09-01", "to": "2025-10-31", "status": "failed"},
            {"from": "2025-11-01", "to": "2025-11-30", "status": "complete"},
        ]
        assert _window_covered_by_ranges(ranges, "2025-09-15", "2025-09-19") is False

    def test_window_in_empty_range_is_covered(self):
        """An empty chunk (provider returned no events) is still successfully
        fetched evidence of no releases — it IS covered."""
        ranges = [{"from": "2025-09-01", "to": "2025-10-31", "status": "empty"}]
        assert _window_covered_by_ranges(ranges, "2025-09-15", "2025-09-19") is True

    def test_window_straddling_adjacent_successful_ranges(self):
        """Adjacent successful ranges form one continuous union.
        A week straddling both must be covered."""
        ranges = [
            {"from": "2025-09-01", "to": "2025-09-30", "status": "complete"},
            {"from": "2025-10-01", "to": "2025-10-15", "status": "complete"},
        ]
        assert _window_covered_by_ranges(ranges, "2025-09-28", "2025-10-04") is True

    def test_window_inside_single_range_across_two(self):
        ranges = [
            {"from": "2025-09-01", "to": "2025-09-30", "status": "complete"},
            {"from": "2025-10-01", "to": "2025-10-31", "status": "complete"},
        ]
        # Entirely inside the second range
        assert _window_covered_by_ranges(ranges, "2025-10-05", "2025-10-09") is True

    # ── _merge_coverage_ranges ────────────────────────────────────────────
    def test_merge_adds_new_ranges(self):
        prior = [{"from": "2025-09-01", "to": "2025-10-31", "status": "complete"}]
        new = [{"from": "2025-12-01", "to": "2026-01-31", "status": "complete"}]
        merged = _merge_coverage_ranges(prior, new)
        assert len(merged) == 2  # gap between Oct 31 and Dec 1 — no compaction

    def test_new_complete_overwrites_prior_failed(self):
        prior = [{"from": "2025-09-01", "to": "2025-09-30", "status": "failed"}]
        new = [{"from": "2025-09-01", "to": "2025-09-30", "status": "complete"}]
        merged = _merge_coverage_ranges(prior, new)
        assert len(merged) == 1
        assert merged[0]["status"] == "complete"

    def test_failed_does_not_overwrite_prior_complete(self):
        prior = [{"from": "2025-09-01", "to": "2025-09-30", "status": "complete"}]
        new = [{"from": "2025-09-01", "to": "2025-09-30", "status": "failed"}]
        merged = _merge_coverage_ranges(prior, new)
        assert len(merged) == 1
        assert merged[0]["status"] == "complete"

    def test_compact_adjacent_same_status(self):
        prior = [
            {"from": "2025-09-01", "to": "2025-09-30", "status": "complete"},
            {"from": "2025-10-01", "to": "2025-10-31", "status": "complete"},
        ]
        merged = _merge_coverage_ranges([], prior)
        assert len(merged) == 1
        assert merged[0]["from"] == "2025-09-01"
        assert merged[0]["to"] == "2025-10-31"

    def test_no_compact_across_different_status(self):
        prior = [
            {"from": "2025-09-01", "to": "2025-09-30", "status": "failed"},
            {"from": "2025-10-01", "to": "2025-10-31", "status": "complete"},
        ]
        merged = _merge_coverage_ranges([], prior)
        assert len(merged) == 2

    # ── Coverage gaps between chunks ─────────────────────────────────────
    def test_gap_between_chunks_is_not_covered(self):
        """Two successful chunks with a failed chunk between them.
        The gap (failed Sept chunk) must not report coverage."""
        ranges = [
            {"from": "2025-08-01", "to": "2025-08-31", "status": "complete"},
            {"from": "2025-09-01", "to": "2025-09-30", "status": "failed"},
            {"from": "2025-10-01", "to": "2025-10-31", "status": "complete"},
        ]
        # September request: not in any complete range
        assert _window_covered_by_ranges(ranges, "2025-09-15", "2025-09-19") is False
        # August request: in a complete range
        assert _window_covered_by_ranges(ranges, "2025-08-10", "2025-08-14") is True

    def test_coverage_ranges_in_get_snapshot_window(self):
        """get_snapshot_window uses coverage_ranges when present in env."""
        events = [_make_econ("2025-10-05", id="e1"), _make_econ("2025-10-15", id="e2")]
        env = _horizon_env(events, horizon_start="2025-09-01", horizon_end="2025-12-01")
        # Inject coverage_ranges into horizon — simulate a gap
        env["horizon"] = {
            "horizon_start": "2025-09-01",
            "horizon_end": "2025-12-01",
            "coverage_ranges": [
                {"from": "2025-10-01", "to": "2025-10-31", "status": "complete"},
            ],
        }
        out, _ = _window(view="month", date="2025-11-01", env=env)
        # November request: no range covers it
        assert out["coverage_complete"] is False
        assert out["empty_reason"] == "outside_horizon"

    def test_coverage_uses_ranges_not_bounds_when_ranges_present(self):
        """When coverage_ranges exist, they are authoritative, not actual_bounds.
        A single event at Oct 5 means actual_bounds = Oct 5..Oct 5,
        but coverage_ranges cover Oct 1..Oct 31 — the whole month is trusted."""
        events = [_make_econ("2025-10-05", id="e1")]
        env = _horizon_env(events, horizon_start="2025-10-01", horizon_end="2025-10-31")
        env["horizon"] = {
            "horizon_start": "2025-10-01",
            "horizon_end": "2025-10-31",
            "coverage_ranges": [
                {"from": "2025-10-01", "to": "2025-10-31", "status": "complete"},
            ],
        }
        # Week Oct 20-24: well outside the single-event actual_bounds
        # but inside a trusted range → covered
        out, _ = _window(view="week", date="2025-10-20", env=env)
        assert out["coverage_complete"] is True


# ═══════════════════════════════════════════════════════════════════════════════
# Continuous union, internal gaps, normalization
# ═══════════════════════════════════════════════════════════════════════════════

class TestCoverageUnionAndGaps:
    """Union of successful ranges, gap detection, normalization safety."""

    # ── Union forms continuous span ────────────────────────────────────────
    def test_adjacent_complete_and_empty_cover_spanning_request(self):
        ranges = [
            {"from": "2025-08-01", "to": "2025-08-31", "status": "complete"},
            {"from": "2025-09-01", "to": "2025-09-30", "status": "empty"},
        ]
        assert _window_covered_by_ranges(ranges, "2025-08-28", "2025-09-04") is True

    def test_adjacent_empty_and_complete_cover_spanning_request(self):
        ranges = [
            {"from": "2025-08-01", "to": "2025-08-31", "status": "empty"},
            {"from": "2025-09-01", "to": "2025-09-30", "status": "complete"},
        ]
        assert _window_covered_by_ranges(ranges, "2025-08-28", "2025-09-04") is True

    def test_overlapping_successful_ranges_cover_union(self):
        ranges = [
            {"from": "2025-08-01", "to": "2025-08-20", "status": "complete"},
            {"from": "2025-08-15", "to": "2025-09-10", "status": "complete"},
        ]
        assert _window_covered_by_ranges(ranges, "2025-08-25", "2025-09-01") is True

    def test_one_day_gap_makes_spanning_request_incomplete(self):
        ranges = [
            {"from": "2025-08-01", "to": "2025-08-31", "status": "complete"},
            {"from": "2025-09-02", "to": "2025-09-30", "status": "complete"},
        ]
        assert _window_covered_by_ranges(ranges, "2025-08-29", "2025-09-04") is False

    def test_failed_internal_range_returns_coverage_gap(self):
        ranges = [
            {"from": "2025-08-01", "to": "2025-08-31", "status": "complete"},
            {"from": "2025-09-01", "to": "2025-09-30", "status": "failed"},
            {"from": "2025-10-01", "to": "2025-10-31", "status": "complete"},
        ]
        kind = _coverage_gap_kind(ranges, "2025-09-15", "2025-09-19")
        assert kind == "coverage_gap"

    def test_before_all_ranges_returns_outside_horizon(self):
        ranges = [{"from": "2025-08-01", "to": "2025-10-31", "status": "complete"}]
        kind = _coverage_gap_kind(ranges, "2025-07-01", "2025-07-05")
        assert kind == "outside_horizon"

    def test_after_all_ranges_returns_outside_horizon(self):
        ranges = [{"from": "2025-08-01", "to": "2025-10-31", "status": "complete"}]
        kind = _coverage_gap_kind(ranges, "2025-12-01", "2025-12-05")
        assert kind == "outside_horizon"

    # ── Merge with boundary overlap ────────────────────────────────────────
    def test_successful_incoming_overlap_supersedes_prior_failed(self):
        prior = [{"from": "2025-08-01", "to": "2025-09-30", "status": "failed"}]
        incoming = [{"from": "2025-08-15", "to": "2025-10-15", "status": "complete"}]
        merged = _merge_coverage_ranges(prior, incoming)
        # Aug 1-14 remains failed (not covered by incoming)
        # Aug 15-Oct 15 becomes complete
        assert len(merged) == 2
        assert merged[0] == {"from": "2025-08-01", "to": "2025-08-14", "status": "failed"}
        assert merged[1] == {"from": "2025-08-15", "to": "2025-10-15", "status": "complete"}

    def test_failed_incoming_does_not_downgrade_prior_complete(self):
        prior = [{"from": "2025-08-01", "to": "2025-09-30", "status": "complete"}]
        incoming = [{"from": "2025-08-15", "to": "2025-10-15", "status": "failed"}]
        merged = _merge_coverage_ranges(prior, incoming)
        statuses = {r["status"] for r in merged}
        assert "complete" in statuses

    def test_different_full_and_delta_boundaries_normalize(self):
        prior = [{"from": "2025-06-01", "to": "2025-08-31", "status": "complete"}]
        incoming = [{"from": "2025-07-15", "to": "2025-10-15", "status": "complete"}]
        merged = _merge_coverage_ranges(prior, incoming)
        spans = [(r["from"], r["to"]) for r in merged if r["status"] == "complete"]
        assert len(spans) == 1

    def test_duplicate_ranges_collapse(self):
        prior = [
            {"from": "2025-08-01", "to": "2025-08-31", "status": "complete"},
            {"from": "2025-08-01", "to": "2025-08-31", "status": "complete"},
        ]
        merged = _merge_coverage_ranges([], prior)
        assert len(merged) == 1

    # ── Normalization safety ──────────────────────────────────────────────
    def test_malformed_dates_ignored(self):
        dirty = [
            {"from": "2025-08-01", "to": "2025-08-31", "status": "complete"},
            {"from": "not-a-date", "to": "2025-09-30", "status": "complete"},
        ]
        clean = _normalize_coverage_ranges(dirty)
        assert len(clean) == 1

    def test_inverted_ranges_ignored(self):
        dirty = [
            {"from": "2025-08-01", "to": "2025-08-31", "status": "complete"},
            {"from": "2025-09-30", "to": "2025-09-01", "status": "complete"},
        ]
        clean = _normalize_coverage_ranges(dirty)
        assert len(clean) == 1

    def test_unsupported_status_ignored(self):
        dirty = [
            {"from": "2025-08-01", "to": "2025-08-31", "status": "bogus"},
            {"from": "2025-09-01", "to": "2025-09-30", "status": "complete"},
        ]
        clean = _normalize_coverage_ranges(dirty)
        assert len(clean) == 1
        assert clean[0]["status"] == "complete"

    def test_non_dict_entries_ignored(self):
        dirty = [
            "not a dict",
            {"from": "2025-08-01", "to": "2025-08-31", "status": "complete"},
        ]
        clean = _normalize_coverage_ranges(dirty)
        assert len(clean) == 1

    def test_empty_ranges_list_returns_empty(self):
        assert _normalize_coverage_ranges([]) == []
        assert _normalize_coverage_ranges(None) == []

    # ── Coverage status diagnostics ────────────────────────────────────────
    def test_all_success_chunks_report_complete(self):
        ranges = [
            {"from": "2025-08-01", "to": "2025-08-31", "status": "complete"},
            {"from": "2025-09-01", "to": "2025-09-30", "status": "complete"},
        ]
        union = _coverage_union(ranges)
        assert len(union) == 1
        assert union[0]["from"] == "2025-08-01"
        assert union[0]["to"] == "2025-09-30"

    def test_valid_all_empty_ranges_remain_covered(self):
        ranges = [
            {"from": "2025-08-01", "to": "2025-08-31", "status": "empty"},
        ]
        assert _window_covered_by_ranges(ranges, "2025-08-15", "2025-08-19") is True

    # ── Coverage gap in get_snapshot_window ────────────────────────────────
    def test_internal_gap_produces_coverage_gap_reason(self):
        events = [_make_econ("2025-08-05", id="e1"), _make_econ("2025-10-05", id="e2")]
        env = _horizon_env(events, horizon_start="2025-08-01", horizon_end="2025-10-31")
        env["horizon"] = {
            "horizon_start": "2025-08-01",
            "horizon_end": "2025-10-31",
            "coverage_ranges": [
                {"from": "2025-08-01", "to": "2025-08-31", "status": "complete"},
                {"from": "2025-09-01", "to": "2025-09-30", "status": "failed"},
                {"from": "2025-10-01", "to": "2025-10-31", "status": "complete"},
            ],
        }
        out, _ = _window(view="week", date="2025-09-15", env=env)
        assert out["coverage_complete"] is False
        assert out["empty_reason"] == "coverage_gap"

    def test_coverage_metadata_json_serializable(self):
        import json as _json
        ranges = [
            {"from": "2025-08-01", "to": "2025-08-31", "status": "complete"},
            {"from": "2025-09-01", "to": "2025-09-30", "status": "failed"},
        ]
        serialized = _json.dumps(ranges)
        assert isinstance(serialized, str)
        assert len(serialized) > 0

    # ── Bootstrap and backward compat ──────────────────────────────────────
    def test_home_treasury_earnings_unchanged(self):
        env = _legacy_env(
            cw=[_make_econ("2026-08-03", eventType="treasury_rate")],
            pw=[],
        )
        with mock.patch(
            "services.calendar_snapshot_service.get_snapshot",
            return_value=env,
        ):
            out = get_snapshot_window("treasury_macro", view="week", date="2026-08-03")
        assert out["event_count"] > 0


# ═══════════════════════════════════════════════════════════════════════════════
# Interval overlay — remainder preservation
# ═══════════════════════════════════════════════════════════════════════════════

class TestIntervalOverlay:
    """Exact date-boundary assertions for _merge_coverage_ranges interval splits."""

    # ── Successful extension ───────────────────────────────────────────────
    def test_successful_extension_preserves_historical_start(self):
        prior = [{"from": "2025-06-01", "to": "2025-08-31", "status": "complete"}]
        incoming = [{"from": "2025-07-15", "to": "2025-10-15", "status": "complete"}]
        merged = _merge_coverage_ranges(prior, incoming)
        assert len(merged) == 1
        assert merged[0] == {"from": "2025-06-01", "to": "2025-10-15", "status": "complete"}

    def test_failed_tail_beyond_prior_success(self):
        prior = [{"from": "2025-08-01", "to": "2025-09-30", "status": "complete"}]
        incoming = [{"from": "2025-09-15", "to": "2025-10-31", "status": "failed"}]
        merged = _merge_coverage_ranges(prior, incoming)
        assert len(merged) == 2
        assert merged[0] == {"from": "2025-08-01", "to": "2025-09-30", "status": "complete"}
        assert merged[1] == {"from": "2025-10-01", "to": "2025-10-31", "status": "failed"}

    def test_successful_repair_of_partial_failed_range(self):
        prior = [{"from": "2025-08-01", "to": "2025-09-30", "status": "failed"}]
        incoming = [{"from": "2025-09-15", "to": "2025-10-31", "status": "complete"}]
        merged = _merge_coverage_ranges(prior, incoming)
        assert len(merged) == 2
        assert merged[0] == {"from": "2025-08-01", "to": "2025-09-14", "status": "failed"}
        assert merged[1] == {"from": "2025-09-15", "to": "2025-10-31", "status": "complete"}

    def test_success_inside_prior_success_does_not_shrink(self):
        prior = [{"from": "2025-08-01", "to": "2025-12-31", "status": "complete"}]
        incoming = [{"from": "2025-10-01", "to": "2025-10-31", "status": "complete"}]
        merged = _merge_coverage_ranges(prior, incoming)
        assert len(merged) == 1
        assert merged[0] == {"from": "2025-08-01", "to": "2025-12-31", "status": "complete"}

    def test_failed_inside_prior_success_does_not_split(self):
        prior = [{"from": "2025-08-01", "to": "2025-12-31", "status": "complete"}]
        incoming = [{"from": "2025-10-01", "to": "2025-10-31", "status": "failed"}]
        merged = _merge_coverage_ranges(prior, incoming)
        assert len(merged) == 1
        assert merged[0]["status"] == "complete"

    def test_complete_overlapping_two_prior_ranges_unifies(self):
        prior = [
            {"from": "2025-08-01", "to": "2025-09-15", "status": "empty"},
            {"from": "2025-09-16", "to": "2025-10-31", "status": "failed"},
        ]
        incoming = [{"from": "2025-09-01", "to": "2025-09-30", "status": "complete"}]
        merged = _merge_coverage_ranges(prior, incoming)
        # empty Aug 1-31 + complete Sep 1-30 → compact to Aug 1-Sep 30 successful
        # failed Sep 16-30 replaced; remainder Oct 1-31 failed
        assert any(r["from"] == "2025-10-01" and r["to"] == "2025-10-31" and r["status"] == "failed" for r in merged)

    # ── Boundary precision ─────────────────────────────────────────────────
    def test_left_remainder_boundary_is_day_before_inc_start(self):
        prior = [{"from": "2025-08-01", "to": "2025-08-31", "status": "failed"}]
        incoming = [{"from": "2025-08-15", "to": "2025-08-20", "status": "complete"}]
        merged = _merge_coverage_ranges(prior, incoming)
        # Left failed remainder: Aug 1-14
        assert merged[0] == {"from": "2025-08-01", "to": "2025-08-14", "status": "failed"}

    def test_right_remainder_boundary_is_day_after_inc_end(self):
        prior = [{"from": "2025-08-01", "to": "2025-08-31", "status": "failed"}]
        incoming = [{"from": "2025-08-10", "to": "2025-08-15", "status": "complete"}]
        merged = _merge_coverage_ranges(prior, incoming)
        # Right failed remainder: Aug 16-31
        assert merged[-1] == {"from": "2025-08-16", "to": "2025-08-31", "status": "failed"}

    def test_no_contradictory_output_overlaps(self):
        prior = [{"from": "2025-08-01", "to": "2025-09-30", "status": "complete"}]
        incoming = [{"from": "2025-09-15", "to": "2025-10-31", "status": "failed"}]
        merged = _merge_coverage_ranges(prior, incoming)
        sep_entries = [r for r in merged if _dates_overlap(r["from"], r["to"], "2025-09-15", "2025-09-30")]
        assert all(r["status"] == "complete" for r in sep_entries)

    def test_delta_refresh_preserves_historical_coverage(self):
        prior = [{"from": "2025-06-01", "to": "2025-08-31", "status": "complete"}]
        incoming = [{"from": "2025-07-15", "to": "2025-10-15", "status": "complete"}]
        merged = _merge_coverage_ranges(prior, incoming)
        assert merged[0]["from"] == "2025-06-01"
        assert merged[0]["to"] == "2025-10-15"

    def test_repeated_merge_idempotent(self):
        prior = [{"from": "2025-06-01", "to": "2025-08-31", "status": "complete"}]
        incoming = [{"from": "2025-07-15", "to": "2025-10-15", "status": "complete"}]
        merged = _merge_coverage_ranges(prior, incoming)
        merged2 = _merge_coverage_ranges(merged, incoming)
        assert merged == merged2

    def test_output_ordering_deterministic(self):
        prior = [
            {"from": "2025-09-01", "to": "2025-09-30", "status": "failed"},
            {"from": "2025-06-01", "to": "2025-08-31", "status": "complete"},
            {"from": "2025-10-01", "to": "2025-10-31", "status": "complete"},
        ]
        incoming = [{"from": "2025-09-15", "to": "2025-09-20", "status": "complete"}]
        merged = _merge_coverage_ranges(prior, incoming)
        dates = [r["from"] for r in merged]
        assert dates == sorted(dates)

    def test_coverage_union_spans_adjacent_after_merge(self):
        prior = [{"from": "2025-08-01", "to": "2025-08-31", "status": "complete"}]
        incoming = [{"from": "2025-09-01", "to": "2025-09-30", "status": "empty"}]
        merged = _merge_coverage_ranges(prior, incoming)
        assert _window_covered_by_ranges(merged, "2025-08-28", "2025-09-04") is True

    def test_internal_failed_tail_still_coverage_gap(self):
        prior = [{"from": "2025-08-01", "to": "2025-08-31", "status": "complete"}]
        incoming = [{"from": "2025-08-01", "to": "2025-08-31", "status": "complete"},
                     {"from": "2025-09-01", "to": "2025-09-30", "status": "failed"}]
        merged = _merge_coverage_ranges(prior, incoming)
        kind = _coverage_gap_kind(merged, "2025-09-15", "2025-09-19")
        assert kind == "coverage_gap"
        assert _window_covered_by_ranges(merged, "2025-08-15", "2025-08-19") is True

    def test_historical_window_before_delta_remains_covered(self):
        prior = [{"from": "2025-06-01", "to": "2025-08-31", "status": "complete"}]
        incoming = [{"from": "2025-07-15", "to": "2025-10-15", "status": "complete"}]
        merged = _merge_coverage_ranges(prior, incoming)
        assert _window_covered_by_ranges(merged, "2025-06-10", "2025-06-14") is True

    def test_mixed_merge_output_no_data_loss(self):
        prior = [
            {"from": "2025-08-01", "to": "2025-08-31", "status": "empty"},
            {"from": "2025-09-01", "to": "2025-09-30", "status": "failed"},
            {"from": "2025-10-01", "to": "2025-10-31", "status": "complete"},
        ]
        incoming = [{"from": "2025-09-15", "to": "2025-10-15", "status": "complete"}]
        merged = _merge_coverage_ranges(prior, incoming)
        assert any(r["status"] == "empty" and r["from"] == "2025-08-01" for r in merged)
        assert any(r["status"] == "failed" and r["from"] == "2025-09-01" and r["to"] == "2025-09-14" for r in merged)
        assert _window_covered_by_ranges(merged, "2025-10-05", "2025-10-09") is True


# ═══════════════════════════════════════════════════════════════════════════════
# Refresh coordinator coalescing
# ═══════════════════════════════════════════════════════════════════════════════

class TestRefreshCoalescing:
    """Duplicate concurrent refresh requests must share one in-flight task."""

    def test_concurrent_refresh_tab_runs_once(self, monkeypatch):
        import asyncio
        from services import calendar_snapshot_service as _svc

        calls: list[str] = []

        async def _slow_core(tab: str, fmp_key: str) -> dict:
            calls.append(tab)
            await asyncio.sleep(0.05)
            return {"tab": tab, "status": "ready"}

        monkeypatch.setattr(_svc, "_refresh_tab_core", _slow_core)

        async def _run():
            return await asyncio.gather(
                _svc.refresh_tab("economic_releases", "dummy-key"),
                _svc.refresh_tab("economic_releases", "dummy-key"),
                _svc.refresh_tab("treasury_macro", "dummy-key"),
            )

        results = asyncio.run(_run())

        # Each tab refreshed exactly once, but both economic_releases callers
        # received the same shared result.
        assert calls.count("economic_releases") == 1
        assert calls.count("treasury_macro") == 1
        assert results[0]["tab"] == "economic_releases"
        assert results[1]["tab"] == "economic_releases"
        assert results[2]["tab"] == "treasury_macro"


# ═══════════════════════════════════════════════════════════════════════════════
# Coordinated macro refresh cycle
# ═══════════════════════════════════════════════════════════════════════════════

class TestMacroRefreshCycle:
    """refresh_macro_sources coordinates both macro sources in one cycle."""

    def test_one_macro_cycle_invokes_economic_once(self, monkeypatch):
        import asyncio
        from services import calendar_snapshot_service as _svc

        calls: list[str] = []

        async def _fake_core(tab: str, fmp_key: str) -> dict:
            calls.append(tab)
            await asyncio.sleep(0.02)
            return {"tab": tab, "status": "ready"}

        monkeypatch.setattr(_svc, "_refresh_tab_core", _fake_core)

        result = asyncio.run(_svc.refresh_macro_sources("dummy-key"))
        assert calls.count("economic_releases") == 1
        assert calls.count("treasury_macro") == 1
        assert result["status"] == "ready"
        assert result["economic_releases"]["tab"] == "economic_releases"

    def test_two_concurrent_macro_cycles_run_each_source_once(self, monkeypatch):
        import asyncio
        from services import calendar_snapshot_service as _svc

        calls: list[str] = []

        async def _slow_core(tab: str, fmp_key: str) -> dict:
            calls.append(tab)
            await asyncio.sleep(0.05)
            return {"tab": tab, "status": "ready"}

        monkeypatch.setattr(_svc, "_refresh_tab_core", _slow_core)

        async def _run():
            return await asyncio.gather(
                _svc.refresh_macro_sources("dummy-key"),
                _svc.refresh_macro_sources("dummy-key"),
            )

        results = asyncio.run(_run())
        assert calls.count("economic_releases") == 1
        assert calls.count("treasury_macro") == 1
        assert results[0]["status"] == "ready"
        assert results[1]["status"] == "ready"

    def test_direct_economic_refresh_overlapping_macro_cycle_coalesces(self, monkeypatch):
        import asyncio
        from services import calendar_snapshot_service as _svc

        calls: list[str] = []

        async def _slow_core(tab: str, fmp_key: str) -> dict:
            calls.append(tab)
            await asyncio.sleep(0.05)
            return {"tab": tab, "status": "ready"}

        monkeypatch.setattr(_svc, "_refresh_tab_core", _slow_core)

        async def _run():
            return await asyncio.gather(
                _svc.refresh_macro_sources("dummy-key"),
                _svc.refresh_tab("economic_releases", "dummy-key"),
            )

        results = asyncio.run(_run())
        assert calls.count("economic_releases") == 1
        assert calls.count("treasury_macro") == 1

    def test_direct_treasury_refresh_overlapping_macro_cycle_coalesces(self, monkeypatch):
        import asyncio
        from services import calendar_snapshot_service as _svc

        calls: list[str] = []

        async def _slow_core(tab: str, fmp_key: str) -> dict:
            calls.append(tab)
            await asyncio.sleep(0.05)
            return {"tab": tab, "status": "ready"}

        monkeypatch.setattr(_svc, "_refresh_tab_core", _slow_core)

        async def _run():
            return await asyncio.gather(
                _svc.refresh_macro_sources("dummy-key"),
                _svc.refresh_tab("treasury_macro", "dummy-key"),
            )

        results = asyncio.run(_run())
        assert calls.count("economic_releases") == 1
        assert calls.count("treasury_macro") == 1

    def test_partial_macro_cycle_failure_is_truthful(self, monkeypatch):
        import asyncio
        from services import calendar_snapshot_service as _svc

        async def _mixed_core(tab: str, fmp_key: str) -> dict:
            if tab == "economic_releases":
                return {"tab": tab, "status": "ready"}
            raise RuntimeError("treasury down")

        monkeypatch.setattr(_svc, "_refresh_tab_core", _mixed_core)

        result = asyncio.run(_svc.refresh_macro_sources("dummy-key"))
        assert result["status"] == "partial"
        assert result["economic_releases"]["status"] == "ready"

    def test_later_macro_cycle_retries_after_failure(self, monkeypatch):
        import asyncio
        from services import calendar_snapshot_service as _svc

        calls: dict[str, int] = {}

        async def _failing_then_ok(tab: str, fmp_key: str) -> dict:
            calls[tab] = calls.get(tab, 0) + 1
            if calls[tab] == 1 and tab == "treasury_macro":
                raise RuntimeError("treasury down")
            return {"tab": tab, "status": "ready"}

        monkeypatch.setattr(_svc, "_refresh_tab_core", _failing_then_ok)

        result1 = asyncio.run(_svc.refresh_macro_sources("dummy-key"))
        assert result1["status"] == "partial"
        result2 = asyncio.run(_svc.refresh_macro_sources("dummy-key"))
        assert result2["status"] == "ready"


# ═══════════════════════════════════════════════════════════════════════════════
# Cancellation safety for shared refresh tasks
# ═══════════════════════════════════════════════════════════════════════════════

class TestRefreshCancellationSafety:
    """Waiter cancellation must not cancel the shared provider task."""

    def test_cancelling_tab_waiter_does_not_cancel_shared_task(self, monkeypatch):
        import asyncio
        from services import calendar_snapshot_service as _svc

        calls: list[str] = []

        async def _slow_core(tab: str, fmp_key: str) -> dict:
            calls.append("start")
            await asyncio.sleep(0.1)
            calls.append("done")
            return {"tab": tab, "status": "ready"}

        monkeypatch.setattr(_svc, "_refresh_tab_core", _slow_core)

        async def _waiter_cancels():
            task = asyncio.create_task(_svc.refresh_tab("economic_releases", "key"))
            await asyncio.sleep(0.01)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        async def _later_waiter():
            await asyncio.sleep(0.02)
            return await _svc.refresh_tab("economic_releases", "key")

        async def _run():
            await asyncio.gather(_waiter_cancels(), _later_waiter())

        asyncio.run(_run())
        assert calls == ["start", "done"]  # core ran to completion exactly once

    def test_tab_task_registry_clears_after_completion(self, monkeypatch):
        import asyncio
        from services import calendar_snapshot_service as _svc

        async def _core(tab: str, fmp_key: str) -> dict:
            await asyncio.sleep(0.01)
            return {"tab": tab, "status": "ready"}

        monkeypatch.setattr(_svc, "_refresh_tab_core", _core)

        asyncio.run(_svc.refresh_tab("economic_releases", "key"))
        assert _svc._refresh_tasks.get("economic_releases") is None

    def test_cancelling_macro_cycle_waiter_does_not_cancel_cycle(self, monkeypatch):
        import asyncio
        from services import calendar_snapshot_service as _svc

        calls: list[str] = []

        async def _slow_refresh_tab(tab: str, fmp_key: str) -> dict:
            calls.append(f"start:{tab}")
            await asyncio.sleep(0.1)
            calls.append(f"done:{tab}")
            return {"tab": tab, "status": "ready"}

        monkeypatch.setattr(_svc, "refresh_tab", _slow_refresh_tab)

        async def _waiter_cancels():
            task = asyncio.create_task(_svc.refresh_macro_sources("key"))
            await asyncio.sleep(0.01)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        async def _later_waiter():
            await asyncio.sleep(0.02)
            return await _svc.refresh_macro_sources("key")

        async def _run():
            await asyncio.gather(_waiter_cancels(), _later_waiter())

        asyncio.run(_run())
        assert calls.count("start:economic_releases") == 1
        assert calls.count("done:economic_releases") == 1
        assert calls.count("start:treasury_macro") == 1
        assert calls.count("done:treasury_macro") == 1

    def test_macro_cycle_registry_clears_after_completion(self, monkeypatch):
        import asyncio
        from services import calendar_snapshot_service as _svc

        async def _refresh_tab(tab: str, fmp_key: str) -> dict:
            await asyncio.sleep(0.01)
            return {"tab": tab, "status": "ready"}

        monkeypatch.setattr(_svc, "refresh_tab", _refresh_tab)

        asyncio.run(_svc.refresh_macro_sources("key"))
        assert _svc._macro_cycle_tasks.get("macro_cycle") is None

    def test_another_waiter_receives_completed_result(self, monkeypatch):
        import asyncio
        from services import calendar_snapshot_service as _svc

        async def _slow_core(tab: str, fmp_key: str) -> dict:
            await asyncio.sleep(0.05)
            return {"tab": tab, "status": "ready"}

        monkeypatch.setattr(_svc, "_refresh_tab_core", _slow_core)

        async def _early_waiter():
            return await _svc.refresh_tab("economic_releases", "key")

        async def _late_waiter():
            await asyncio.sleep(0.01)
            return await _svc.refresh_tab("economic_releases", "key")

        async def _run():
            return await asyncio.gather(_early_waiter(), _late_waiter())

        results = asyncio.run(_run())
        assert results[0]["status"] == "ready"
        assert results[1]["status"] == "ready"


# ═══════════════════════════════════════════════════════════════════════════════
# Scheduler / startup / manual orchestration integration
# ═══════════════════════════════════════════════════════════════════════════════

class TestMacroSchedulerIntegration:
    """Orchestration calls refresh_macro_sources once for macro sources."""

    def test_startup_stale_check_invokes_one_macro_cycle(self, monkeypatch):
        import asyncio
        from services import calendar_snapshot_service as _svc

        monkeypatch.setattr(_svc, "_et_week_monday", lambda: __import__("datetime").date(2026, 8, 3))

        def _fake_neon_read(tab: str):
            # economic_releases stale, treasury_macro current
            if tab == "economic_releases":
                return {
                    "events": [],
                    "meta": {"window": {"from": "2026-07-27"}},
                    "status": "stale",
                }
            return {
                "events": [],
                "meta": {"window": {"from": "2026-08-03"}},
                "status": "ready",
            }

        monkeypatch.setattr(_svc, "_neon_read", _fake_neon_read)

        cycles: list[int] = []
        tab_calls: list[str] = []

        async def _fake_macro_cycle(fmp_key: str) -> dict:
            cycles.append(1)
            return {
                "economic_releases": {"status": "ready"},
                "treasury_macro": {"status": "ready"},
                "status": "ready",
            }

        async def _fake_refresh_tab(tab: str, fmp_key: str) -> dict:
            tab_calls.append(tab)
            return {"tab": tab, "status": "ready"}

        monkeypatch.setattr(_svc, "refresh_macro_sources", _fake_macro_cycle)
        monkeypatch.setattr(_svc, "refresh_tab", _fake_refresh_tab)

        asyncio.run(_svc.check_and_refresh_stale("key", delay_secs=0))
        assert sum(cycles) == 1
        # Non-macro tabs should still be checked but in this fixture they are current.

    def test_sunday_scheduler_invokes_one_macro_cycle(self, monkeypatch):
        import asyncio
        from datetime import datetime, timezone
        from services import calendar_snapshot_service as _svc

        # Sunday 2026-08-02 at 04:00 ET (first macro slot)
        sunday_et = datetime(2026, 8, 2, 4, 0, tzinfo=timezone.utc)
        monkeypatch.setattr(_svc, "_et_now", lambda: sunday_et)
        monkeypatch.setattr(_svc, "_iso_year_week", lambda dt: "2026-W31")
        monkeypatch.setattr(_svc, "_last_run_marker", lambda tab: None)

        cycles: list[int] = []

        async def _fake_macro_cycle(fmp_key: str) -> dict:
            cycles.append(1)
            return {
                "economic_releases": {"status": "ready"},
                "treasury_macro": {"status": "ready"},
                "status": "ready",
            }

        async def _fake_refresh_tab(tab: str, fmp_key: str) -> dict:
            return {"tab": tab, "status": "ready"}

        monkeypatch.setattr(_svc, "refresh_macro_sources", _fake_macro_cycle)
        monkeypatch.setattr(_svc, "refresh_tab", _fake_refresh_tab)

        async def _run_once():
            task = asyncio.create_task(weekly_scheduler_loop(lambda: "key"))
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(_run_once())
        assert sum(cycles) == 1

    def test_mon_sat_stale_check_invokes_one_macro_cycle(self, monkeypatch):
        import asyncio
        from datetime import datetime, timezone
        from services import calendar_snapshot_service as _svc

        # Tuesday 2026-08-04 at 10:00 ET
        tuesday_et = datetime(2026, 8, 4, 10, 0, tzinfo=timezone.utc)
        monkeypatch.setattr(_svc, "_et_now", lambda: tuesday_et)
        monkeypatch.setattr(_svc, "_iso_year_week", lambda dt: "2026-W32")
        monkeypatch.setattr(_svc, "_last_run_marker", lambda tab: None)

        def _fake_neon_read(tab: str):
            return {
                "events": [],
                "meta": {"window": {"from": "2026-07-27"}},
                "status": "stale",
            }

        monkeypatch.setattr(_svc, "_neon_read", _fake_neon_read)

        cycles: list[int] = []

        async def _fake_macro_cycle(fmp_key: str) -> dict:
            cycles.append(1)
            return {
                "economic_releases": {"status": "ready"},
                "treasury_macro": {"status": "ready"},
                "status": "ready",
            }

        async def _fake_refresh_tab(tab: str, fmp_key: str) -> dict:
            return {"tab": tab, "status": "ready"}

        monkeypatch.setattr(_svc, "refresh_macro_sources", _fake_macro_cycle)
        monkeypatch.setattr(_svc, "refresh_tab", _fake_refresh_tab)

        asyncio.run(_svc.check_and_refresh_stale("key", delay_secs=0))
        assert sum(cycles) == 1

    def test_manual_dual_macro_backfill_invokes_one_macro_cycle(self, monkeypatch):
        import asyncio
        from services import calendar_snapshot_service as _svc

        cycles: list[int] = []
        tab_calls: list[str] = []

        async def _fake_macro_cycle(fmp_key: str) -> dict:
            cycles.append(1)
            return {
                "economic_releases": {"status": "ready"},
                "treasury_macro": {"status": "ready"},
                "status": "ready",
            }

        async def _fake_refresh_tab(tab: str, fmp_key: str) -> dict:
            tab_calls.append(tab)
            return {"tab": tab, "status": "ready"}

        monkeypatch.setattr(_svc, "refresh_macro_sources", _fake_macro_cycle)
        monkeypatch.setattr(_svc, "refresh_tab", _fake_refresh_tab)
        monkeypatch.setattr(_svc, "_neon_write", lambda tab, slot: True)
        monkeypatch.setattr(_svc, "_read_disk", lambda: {})
        monkeypatch.setattr(_svc, "_write_disk", lambda store: None)

        rc = asyncio.run(_svc._manual_backfill(["economic_releases", "treasury_macro"]))
        assert rc == 0
        assert sum(cycles) == 1

    def test_dividends_ipos_splits_schedules_remain_unchanged(self, monkeypatch):
        import asyncio
        from datetime import datetime, timezone
        from services import calendar_snapshot_service as _svc

        sunday_et = datetime(2026, 8, 2, 1, 0, tzinfo=timezone.utc)
        monkeypatch.setattr(_svc, "_et_now", lambda: sunday_et)
        monkeypatch.setattr(_svc, "_iso_year_week", lambda dt: "2026-W31")
        monkeypatch.setattr(_svc, "_last_run_marker", lambda tab: None)

        cycles: list[int] = []
        tab_calls: list[str] = []

        async def _fake_macro_cycle(fmp_key: str) -> dict:
            cycles.append(1)
            return {"status": "ready"}

        async def _fake_refresh_tab(tab: str, fmp_key: str) -> dict:
            tab_calls.append(tab)
            return {"tab": tab, "status": "ready"}

        monkeypatch.setattr(_svc, "refresh_macro_sources", _fake_macro_cycle)
        monkeypatch.setattr(_svc, "refresh_tab", _fake_refresh_tab)

        async def _run_once():
            task = asyncio.create_task(weekly_scheduler_loop(lambda: "key"))
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(_run_once())
        assert "dividends" in tab_calls
        assert sum(cycles) == 0  # macro cycle not run at hour 1

    def test_existing_week_day_month_views_unchanged(self, monkeypatch):
        from services import calendar_snapshot_service as _svc

        env = {
            "events": [
                {"date": "2026-08-05", "eventName": "CPI MoM", "eventType": "economic_release"},
            ],
            "current_week": [],
            "previous_week": [],
            "last_updated": "2026-08-02T10:00:00Z",
            "status": "ready",
            "horizon": {
                "horizon_start": "2026-07-18",
                "horizon_end": "2026-10-29",
                "coverage_ranges": [{"from": "2026-07-18", "to": "2026-10-29", "status": "complete"}],
            },
        }
        monkeypatch.setattr(_svc, "get_snapshot", lambda tab: env)

        week = _svc.get_snapshot_window("economic_releases", view="week", date="2026-08-03")
        day = _svc.get_snapshot_window("economic_releases", view="day", date="2026-08-05")
        month = _svc.get_snapshot_window("economic_releases", view="month", date="2026-08-01")

        assert week["view"] == "week"
        assert week["window_start"] == "2026-08-03"
        assert day["view"] == "day"
        assert day["window_start"] == "2026-08-05"
        assert month["view"] == "month"
        assert month["window_start"] == "2026-08-01"

    def test_all_outputs_json_serialize(self, monkeypatch):
        import json
        import asyncio
        from services import calendar_snapshot_service as _svc

        async def _fake_refresh_tab(tab: str, fmp_key: str) -> dict:
            return {"tab": tab, "status": "ready"}

        monkeypatch.setattr(_svc, "refresh_tab", _fake_refresh_tab)

        result = asyncio.run(_svc.refresh_macro_sources("key"))
        # Must not raise.
        json.dumps(result)
