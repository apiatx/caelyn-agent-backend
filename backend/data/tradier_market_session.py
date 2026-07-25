"""
tradier_market_session.py — Eastern-Time market session classifier.

Session definitions (Eastern Time):
  premarket  : Monday–Friday 04:00–09:30
  regular    : Monday–Friday 09:30–16:00
  postmarket : Monday–Friday 16:00–20:00
  off_hours  : Monday–Friday 20:00–04:00 (next weekday)
  weekend    : Saturday 00:00 – Sunday 23:59

Active sessions (Tradier quotes/budget enforcement — budget is enforced):
  premarket, regular, postmarket

Regular session only (options chain scans are ONLY permitted here):
  regular (09:30–16:00 ET on valid US trading days, excluding holidays)

Scan gate:
  is_regular_options_session() — use this to gate ALL Tradier options chain scans.
  is_active_session()         — legacy: pre/regular/post, used only for budget enforcement.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Literal

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore

_ET = ZoneInfo("America/New_York")

TradierSession = Literal["premarket", "regular", "postmarket", "off_hours", "weekend"]

_PREMARKET_START = time(4,  0)
_MARKET_OPEN     = time(9, 30)
_MARKET_CLOSE    = time(16, 0)
_POSTMARKET_END  = time(20, 0)

_ACTIVE_SESSIONS: frozenset[TradierSession] = frozenset(
    {"premarket", "regular", "postmarket"}
)


# ── US Market Holiday Calendar ────────────────────────────────────────────────

def _easter(year: int) -> date:
    """Anonymous Gregorian Easter algorithm."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month, day = divmod(h + l - 7 * m + 114, 31)
    return date(year, month, day + 1)


def _observed(d: date) -> date:
    """NYSE observance: Saturday → Friday, Sunday → Monday."""
    wd = d.weekday()
    if wd == 5:   # Saturday → Friday
        return d - timedelta(days=1)
    if wd == 6:   # Sunday → Monday
        return d + timedelta(days=1)
    return d


def _nth_weekday(year: int, month: int, n: int, weekday: int) -> date:
    """n-th occurrence (1-indexed) of weekday (0=Mon) in (year, month)."""
    first = date(year, month, 1)
    delta = (weekday - first.weekday()) % 7
    return first + timedelta(days=delta + (n - 1) * 7)


def _last_weekday(year: int, month: int, weekday: int) -> date:
    """Last occurrence of weekday (0=Mon) in (year, month)."""
    if month == 12:
        last = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last = date(year, month + 1, 1) - timedelta(days=1)
    delta = (last.weekday() - weekday) % 7
    return last - timedelta(days=delta)


def _us_market_holidays(year: int) -> frozenset[date]:
    """
    Return the set of NYSE market closure dates for *year*.
    Covers: New Year's Day, MLK Jr. Day, Presidents' Day, Good Friday,
    Memorial Day, Juneteenth, Independence Day, Labor Day,
    Thanksgiving, Christmas Day.
    """
    h: list[date] = []

    # New Year's Day — Jan 1, observed
    h.append(_observed(date(year, 1, 1)))
    # New Year's Day observed can land on previous year's Dec 31 —
    # also account for Jan 1 on Sunday → Jan 2 observed
    # (already handled by _observed)

    # MLK Jr. Day — 3rd Monday in January
    h.append(_nth_weekday(year, 1, 3, 0))

    # Presidents' Day — 3rd Monday in February
    h.append(_nth_weekday(year, 2, 3, 0))

    # Good Friday — Easter minus 2 days
    h.append(_easter(year) - timedelta(days=2))

    # Memorial Day — last Monday in May
    h.append(_last_weekday(year, 5, 0))

    # Juneteenth — June 19, observed (added from 2022)
    if year >= 2022:
        h.append(_observed(date(year, 6, 19)))

    # Independence Day — July 4, observed
    h.append(_observed(date(year, 7, 4)))

    # Labor Day — 1st Monday in September
    h.append(_nth_weekday(year, 9, 1, 0))

    # Thanksgiving — 4th Thursday in November
    h.append(_nth_weekday(year, 11, 4, 3))

    # Christmas — December 25, observed
    h.append(_observed(date(year, 12, 25)))

    # Drop any dates that fell on a weekend after observance adjustment
    # (shouldn't happen with _observed, but defensive check)
    return frozenset(d for d in h if d.weekday() < 5)


# Cache up to 3 years of holidays in memory
_HOLIDAY_CACHE: dict[int, frozenset[date]] = {}


def _holidays_for(year: int) -> frozenset[date]:
    if year not in _HOLIDAY_CACHE:
        _HOLIDAY_CACHE[year] = _us_market_holidays(year)
    return _HOLIDAY_CACHE[year]


def _is_trading_day(d: date) -> bool:
    """True if d is a valid US equity market trading day (Mon–Fri, not a holiday)."""
    if d.weekday() >= 5:
        return False
    return d not in _holidays_for(d.year)


# ── Session classification ────────────────────────────────────────────────────

def get_session(now_et: datetime | None = None) -> TradierSession:
    """Return the current Tradier market session label."""
    if now_et is None:
        now_et = datetime.now(_ET)
    wd = now_et.weekday()   # 0=Mon … 6=Sun
    if wd >= 5:             # Saturday=5, Sunday=6
        return "weekend"
    t = now_et.time().replace(second=0, microsecond=0)
    if _PREMARKET_START <= t < _MARKET_OPEN:
        return "premarket"
    if _MARKET_OPEN <= t < _MARKET_CLOSE:
        return "regular"
    if _MARKET_CLOSE <= t < _POSTMARKET_END:
        return "postmarket"
    return "off_hours"


def is_active_session(now_et: datetime | None = None) -> bool:
    """
    True when the budget should be enforced (pre/regular/post-market, weekday).
    LEGACY — used only for Tradier budget-enforcement gate and tradier_provider.py.
    For options chain scan gating use is_regular_options_session() instead.
    """
    return get_session(now_et) in _ACTIVE_SESSIONS


def is_regular_options_session(now_et: datetime | None = None) -> bool:
    """
    True ONLY during the regular U.S. options trading session:
      - America/New_York timezone
      - Valid US trading day (Mon–Fri, not a market holiday)
      - 09:30 ET ≤ time < 16:00 ET

    This is the canonical gate for ALL Tradier options chain scans.
    Returns False during premarket, postmarket, off-hours, weekends, and
    all US market holidays.  When False, callers must serve cached LKG data
    and make zero Tradier options calls.
    """
    if now_et is None:
        now_et = datetime.now(_ET)
    # Weekday check
    if now_et.weekday() >= 5:
        return False
    # Holiday check
    today = now_et.date()
    if today in _holidays_for(today.year):
        return False
    # Clock check: regular session only
    t = now_et.time()
    return time(9, 30) <= t < time(16, 0)


def _next_regular_open(now_et: datetime) -> datetime:
    """Return the datetime of the next regular session open (09:30 ET)."""
    candidate = now_et.date()
    # If we're before today's open, today might still be valid
    if now_et.time() < time(9, 30) and _is_trading_day(candidate) and now_et.weekday() < 5:
        return datetime(candidate.year, candidate.month, candidate.day, 9, 30, tzinfo=_ET)
    # Otherwise look forward
    candidate += timedelta(days=1)
    for _ in range(14):  # search up to 2 weeks ahead
        if _is_trading_day(candidate):
            return datetime(candidate.year, candidate.month, candidate.day, 9, 30, tzinfo=_ET)
        candidate += timedelta(days=1)
    # Fallback: next Monday
    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)
    return datetime(candidate.year, candidate.month, candidate.day, 9, 30, tzinfo=_ET)


def get_session_info(now_et: datetime | None = None) -> dict:
    """Return a dict with session classification and current ET metadata (legacy)."""
    if now_et is None:
        now_et = datetime.now(_ET)
    session = get_session(now_et)
    return {
        "tradier_market_session":    session,
        "is_tradier_active_session": session in _ACTIVE_SESSIONS,
        "session_time_et":           now_et.strftime("%H:%M:%S"),
        "session_weekday_et":        now_et.strftime("%A"),
    }


def get_options_session_info(now_et: datetime | None = None) -> dict:
    """
    Return full options-scan session metadata.

    Fields:
      options_market_session   — session label (premarket/regular/postmarket/off_hours/weekend)
      options_scan_allowed     — True only during regular session on a trading day
      regular_open_at          — today's regular open (ISO, ET) or None if not a trading day
      regular_close_at         — today's regular close (ISO, ET) or None if not a trading day
      next_regular_open_at     — next regular open datetime (ISO, ET)
      market_calendar_status   — "trading_day" | "holiday" | "weekend"
      reason                   — human-readable explanation
    """
    if now_et is None:
        now_et = datetime.now(_ET)

    session     = get_session(now_et)
    scan_ok     = is_regular_options_session(now_et)
    today       = now_et.date()
    is_weekend  = today.weekday() >= 5
    is_holiday  = (not is_weekend) and (today in _holidays_for(today.year))
    is_trading  = _is_trading_day(today)

    if is_weekend:
        cal_status = "weekend"
        reason     = f"Weekend ({now_et.strftime('%A')}); options scans blocked"
    elif is_holiday:
        cal_status = "holiday"
        reason     = f"US market holiday ({today.isoformat()}); options scans blocked"
    else:
        cal_status = "trading_day"
        t = now_et.time()
        if t < time(9, 30):
            reason = "Before regular open (09:30 ET); LKG served"
        elif t < time(16, 0):
            reason = "Regular session active; scans permitted"
        else:
            reason = "After regular close (16:00 ET); LKG served"

    regular_open  = None
    regular_close = None
    if is_trading:
        regular_open  = datetime(today.year, today.month, today.day,  9, 30, tzinfo=_ET).isoformat()
        regular_close = datetime(today.year, today.month, today.day, 16,  0, tzinfo=_ET).isoformat()

    next_open = _next_regular_open(now_et)

    return {
        "options_market_session":  session,
        "options_scan_allowed":    scan_ok,
        "regular_open_at":         regular_open,
        "regular_close_at":        regular_close,
        "next_regular_open_at":    next_open.isoformat(),
        "market_calendar_status":  cal_status,
        "reason":                  reason,
        "session_time_et":         now_et.strftime("%H:%M:%S"),
        "session_date_et":         today.isoformat(),
        "is_us_market_holiday":    is_holiday,
    }


# ── Self-test ─────────────────────────────────────────────────────────────────

def _simulate(hour: int, minute: int = 0, weekday: int = 0,
              d: date | None = None) -> dict:
    """Create a fake ET datetime for testing."""
    if d is None:
        from datetime import date as _date
        today = _date.today()
        delta = (weekday - today.weekday()) % 7
        d = today + timedelta(days=delta)
    fake = datetime(d.year, d.month, d.day, hour, minute, tzinfo=_ET)
    return {
        "session":  get_session(fake),
        "scan_ok":  is_regular_options_session(fake),
        "info":     get_options_session_info(fake),
    }


def _run_self_test() -> None:
    """Quick sanity-check across all session states."""
    print("[TRADIER_SESSION_TEST] Running session self-test:")

    cases: list[tuple] = [
        # (wd, h,  m,  expected_session, scan_allowed)
        (0,  5,  0,  "premarket",  False),
        (0,  9, 30,  "regular",    True),   # must check non-holiday Mon
        (0, 14,  0,  "regular",    True),
        (0, 16,  0,  "postmarket", False),
        (0, 19, 59,  "postmarket", False),
        (0, 20,  0,  "off_hours",  False),
        (0,  2,  0,  "off_hours",  False),
        (5, 12,  0,  "weekend",    False),  # Saturday
        (6, 12,  0,  "weekend",    False),  # Sunday
        (0,  9, 29,  "premarket",  False),  # 1 min before open
        (4, 15, 59,  "regular",    True),   # Friday 3:59 PM — must use real non-holiday date
        (4, 16,  0,  "postmarket", False),  # Friday 4:00 PM
    ]

    all_ok = True
    for wd, h, m, exp_sess, exp_scan in cases:
        r = _simulate(h, m, weekday=wd)
        got_sess = r["session"]
        got_scan = r["scan_ok"]
        ok = (got_sess == exp_sess) and (got_scan == exp_scan)
        if not ok:
            all_ok = False
        flag = "✓" if ok else "✗"
        print(
            f"  {flag} wd={wd} {h:02d}:{m:02d} ET → sess={got_sess!r} "
            f"scan={got_scan} (expected {exp_sess!r}/{exp_scan})"
        )

    # Holiday test — Independence Day 2026 falls on Saturday, observed Fri Jul 3
    jul3_2026 = date(2026, 7, 3)
    fake_holiday = datetime(2026, 7, 3, 11, 0, tzinfo=_ET)
    h_scan = is_regular_options_session(fake_holiday)
    flag = "✓" if not h_scan else "✗"
    print(f"  {flag} Jul 3 2026 (Jul 4 observed) 11:00 ET → scan={h_scan} (expected False)")
    if h_scan:
        all_ok = False

    print(f"[TRADIER_SESSION_TEST] {'All passed' if all_ok else 'FAILURES DETECTED'}")


if __name__ == "__main__":
    _run_self_test()
