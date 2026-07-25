"""
Regression tests for data.tradier_market_session.
Covers Part 13 requirements from the options cache / market-hours fix spec.

Run with:
    cd backend && python -m pytest tests/test_options_market_session.py -v
    # OR directly:
    cd backend && python tests/test_options_market_session.py
"""
from __future__ import annotations

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from data.tradier_market_session import (
    get_session,
    is_active_session,
    is_regular_options_session,
    get_options_session_info,
    _is_trading_day,
    _us_market_holidays,
    _ET,
)

_FAIL = []

def _assert(cond: bool, msg: str) -> None:
    if not cond:
        _FAIL.append(f"FAIL: {msg}")
        print(f"  ✗ {msg}")
    else:
        print(f"  ✓ {msg}")


def _dt(y, mo, d, h, m=0) -> datetime:
    return datetime(y, mo, d, h, m, tzinfo=_ET)


# ── Helper: find a non-holiday Monday ────────────────────────────────────────
def _next_trading_monday(year: int = 2026) -> date:
    d = date(year, 1, 5)  # First Monday of 2026
    while not _is_trading_day(d):
        d += timedelta(days=7)
    return d


_MON = _next_trading_monday()   # a known non-holiday Monday
_FRI = _MON + timedelta(days=4) # the corresponding Friday


def test_session_labels() -> None:
    print("\n=== Session label tests ===")
    _assert(get_session(_dt(_MON.year, _MON.month, _MON.day,  5, 0))  == "premarket",  "Mon 05:00 → premarket")
    _assert(get_session(_dt(_MON.year, _MON.month, _MON.day,  9, 30)) == "regular",    "Mon 09:30 → regular")
    _assert(get_session(_dt(_MON.year, _MON.month, _MON.day, 14, 0))  == "regular",    "Mon 14:00 → regular")
    _assert(get_session(_dt(_MON.year, _MON.month, _MON.day, 15, 59)) == "regular",    "Mon 15:59 → regular")
    _assert(get_session(_dt(_MON.year, _MON.month, _MON.day, 16, 0))  == "postmarket", "Mon 16:00 → postmarket")
    _assert(get_session(_dt(_MON.year, _MON.month, _MON.day, 20, 0))  == "off_hours",  "Mon 20:00 → off_hours")
    _assert(get_session(_dt(_MON.year, _MON.month, _MON.day,  2, 0))  == "off_hours",  "Mon 02:00 → off_hours")
    # Weekend
    sat = _MON + timedelta(days=5)
    sun = _MON + timedelta(days=6)
    _assert(get_session(_dt(sat.year, sat.month, sat.day, 12, 0)) == "weekend", "Saturday → weekend")
    _assert(get_session(_dt(sun.year, sun.month, sun.day, 12, 0)) == "weekend", "Sunday → weekend")


def test_scan_allowed_regular_session() -> None:
    print("\n=== Scan allowed: regular session ===")
    # Friday 3:59 PM ET — scans allowed
    _assert(is_regular_options_session(_dt(_FRI.year, _FRI.month, _FRI.day, 15, 59)),
            "Friday 3:59 PM ET — scans allowed")
    # Friday 4:00 PM ET — scans blocked
    _assert(not is_regular_options_session(_dt(_FRI.year, _FRI.month, _FRI.day, 16, 0)),
            "Friday 4:00 PM ET — scans blocked")
    # Monday 9:30 AM ET on a normal trading day — scans allowed
    _assert(is_regular_options_session(_dt(_MON.year, _MON.month, _MON.day,  9, 30)),
            "Monday 9:30 AM ET — scans allowed")
    # Monday 9:29 AM ET — scans blocked (premarket)
    _assert(not is_regular_options_session(_dt(_MON.year, _MON.month, _MON.day,  9, 29)),
            "Monday 9:29 AM ET — scans blocked (premarket)")


def test_scan_blocked_outside_regular() -> None:
    print("\n=== Scan blocked: outside regular session ===")
    # Friday evening
    _assert(not is_regular_options_session(_dt(_FRI.year, _FRI.month, _FRI.day, 18, 0)),
            "Friday evening — LKG served, no scan")
    # Saturday
    sat = _FRI + timedelta(days=1)
    _assert(not is_regular_options_session(_dt(sat.year, sat.month, sat.day, 12, 0)),
            "Saturday — no scan")
    # Sunday
    sun = _FRI + timedelta(days=2)
    _assert(not is_regular_options_session(_dt(sun.year, sun.month, sun.day, 12, 0)),
            "Sunday — no scan")
    # Premarket
    _assert(not is_regular_options_session(_dt(_MON.year, _MON.month, _MON.day, 5, 0)),
            "Premarket — no scan")
    # Postmarket
    _assert(not is_regular_options_session(_dt(_MON.year, _MON.month, _MON.day, 17, 0)),
            "Postmarket — no scan")


def test_holiday_blocking() -> None:
    print("\n=== Holiday blocking ===")
    # Independence Day 2026: Jul 4 is Saturday → observed Fri Jul 3
    jul3 = _dt(2026, 7, 3, 11, 0)
    _assert(not is_regular_options_session(jul3),
            "Jul 3 2026 (Jul 4 observed) 11:00 ET — holiday, no scan")
    _assert(not _is_trading_day(date(2026, 7, 3)),
            "Jul 3 2026 is not a trading day")

    # Christmas 2026: Dec 25 is Friday → observed Dec 25
    xmas = _dt(2026, 12, 25, 11, 0)
    _assert(not is_regular_options_session(xmas),
            "Dec 25 2026 (Christmas) 11:00 ET — holiday, no scan")

    # New Year's Day 2026: Jan 1 is Thursday (no observance needed)
    ny = _dt(2026, 1, 1, 11, 0)
    _assert(not is_regular_options_session(ny),
            "Jan 1 2026 (New Year's) 11:00 ET — holiday, no scan")

    # Day after holiday is normal trading
    jan2 = _dt(2026, 1, 2, 11, 0)
    _assert(is_regular_options_session(jan2),
            "Jan 2 2026 (day after New Year's) 11:00 ET — trading day, scan allowed")

    # MLK Day 2026: Jan 19
    mlk = _dt(2026, 1, 19, 11, 0)
    _assert(not is_regular_options_session(mlk),
            "Jan 19 2026 (MLK Day) 11:00 ET — holiday, no scan")


def test_is_active_session_legacy() -> None:
    print("\n=== Legacy is_active_session (budget enforcement) ===")
    # is_active_session should still return True for premarket/regular/postmarket
    _assert(is_active_session(_dt(_MON.year, _MON.month, _MON.day,  5, 0)),
            "Premarket → is_active_session True (budget enforcement)")
    _assert(is_active_session(_dt(_MON.year, _MON.month, _MON.day, 11, 0)),
            "Regular → is_active_session True")
    _assert(is_active_session(_dt(_MON.year, _MON.month, _MON.day, 17, 0)),
            "Postmarket → is_active_session True")
    sat = _MON + timedelta(days=5)
    _assert(not is_active_session(_dt(sat.year, sat.month, sat.day, 12, 0)),
            "Weekend → is_active_session False")


def test_session_info_shape() -> None:
    print("\n=== Session info shape ===")
    info = get_options_session_info(_dt(_MON.year, _MON.month, _MON.day, 11, 0))
    required_keys = [
        "options_market_session", "options_scan_allowed", "regular_open_at",
        "regular_close_at", "next_regular_open_at", "market_calendar_status", "reason",
    ]
    for k in required_keys:
        _assert(k in info, f"get_options_session_info has key '{k}'")
    _assert(info["options_scan_allowed"] is True,  "scan_allowed True during regular")
    _assert(info["market_calendar_status"] == "trading_day", "calendar_status=trading_day")

    # Off-hours
    info2 = get_options_session_info(_dt(_MON.year, _MON.month, _MON.day, 20, 0))
    _assert(info2["options_scan_allowed"] is False, "scan_allowed False during off_hours")

    # Weekend
    sat = _MON + timedelta(days=5)
    info3 = get_options_session_info(_dt(sat.year, sat.month, sat.day, 12, 0))
    _assert(info3["market_calendar_status"] == "weekend", "weekend → calendar_status=weekend")

    # Holiday
    info4 = get_options_session_info(_dt(2026, 1, 1, 11, 0))
    _assert(info4["market_calendar_status"] == "holiday", "New Year's → calendar_status=holiday")
    _assert(info4["options_scan_allowed"] is False, "Holiday → scan_allowed False")


def test_next_open_is_trading_day() -> None:
    print("\n=== Next regular open is always a trading day ===")
    from data.tradier_market_session import _next_regular_open
    cases = [
        _dt(_FRI.year, _FRI.month, _FRI.day, 17, 0),  # Friday after close
        _dt(_FRI.year, _FRI.month, _FRI.day + 1, 12, 0) if _FRI.day + 1 <= 28 else _dt(_FRI.year, _FRI.month, 28, 12, 0),  # Saturday
        _dt(2025, 12, 25, 12, 0),  # Christmas
    ]
    for c in cases:
        nxt = _next_regular_open(c)
        d = nxt.date()
        _assert(_is_trading_day(d), f"Next open after {c.strftime('%Y-%m-%d %H:%M')} → {d} is trading day")
        _assert(nxt.hour == 9 and nxt.minute == 30, f"Next open is at 09:30 ET (got {nxt.hour}:{nxt.minute})")


def test_holiday_list_sanity() -> None:
    print("\n=== Holiday list sanity checks ===")
    h2026 = _us_market_holidays(2026)
    _assert(date(2026, 1,  1) in h2026, "2026 New Year's")
    _assert(date(2026, 1, 19) in h2026, "2026 MLK Day")
    _assert(date(2026, 2, 16) in h2026, "2026 Presidents' Day")
    _assert(date(2026, 4,  3) in h2026, "2026 Good Friday")
    _assert(date(2026, 5, 25) in h2026, "2026 Memorial Day")
    _assert(date(2026, 6, 19) in h2026, "2026 Juneteenth")
    _assert(date(2026, 7,  3) in h2026, "2026 Independence Day (observed Fri Jul 3)")
    _assert(date(2026, 9,  7) in h2026, "2026 Labor Day")
    _assert(date(2026,11, 26) in h2026, "2026 Thanksgiving")
    _assert(date(2026,12, 25) in h2026, "2026 Christmas")
    # No holiday should be on a weekend
    for d in h2026:
        _assert(d.weekday() < 5, f"Holiday {d} is not a weekend day")


if __name__ == "__main__":
    print("=== Options Market Session Regression Tests ===")
    test_session_labels()
    test_scan_allowed_regular_session()
    test_scan_blocked_outside_regular()
    test_holiday_blocking()
    test_is_active_session_legacy()
    test_session_info_shape()
    test_next_open_is_trading_day()
    test_holiday_list_sanity()

    print(f"\n{'='*50}")
    if _FAIL:
        print(f"FAILED: {len(_FAIL)} assertion(s)")
        for f in _FAIL:
            print(f"  {f}")
        sys.exit(1)
    else:
        print(f"ALL TESTS PASSED")
