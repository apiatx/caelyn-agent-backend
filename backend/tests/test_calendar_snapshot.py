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
