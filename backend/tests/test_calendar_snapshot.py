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
    get_snapshot_window,
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
    """A capped horizon (meta end beyond actual data) reports incomplete truthfully."""
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
