"""
tradier_market_session.py — Eastern-Time market session classifier with
NYSE early-close support.

Session definitions (Eastern Time):
  premarket  : Monday–Friday 04:00–09:30
  regular    : Monday–Friday 09:30–close (16:00 normal, 13:00 early-close)
  postmarket : Monday–Friday close–20:00
  off_hours  : Monday–Friday 20:00–04:00 (next weekday)
  weekend    : Saturday 00:00 – Sunday 23:59

Active sessions (Tradier budget enforcement):
  premarket, regular, postmarket

Regular options scan gate:
  is_regular_options_session() — use this to gate ALL Tradier options chain scans.
  Returns False during premarket, postmarket, off-hours, weekends, holidays, and
  at/after the actual market close (which is 13:00 ET on early-close days).

  is_active_session() — legacy: pre/regular/post, used only for budget enforcement.

NYSE early-close days (1:00 PM ET close):
  * Black Friday (day after Thanksgiving) — every year
  * Christmas Eve (Dec 24) when it is a weekday and not itself a holiday
  * Day before Independence Day when July 4 falls on a Thursday or Friday
    (NOT when July 3 is already an observed holiday)
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

_PREMARKET_START  = time(4,  0)
_MARKET_OPEN      = time(9, 30)
_MARKET_CLOSE     = time(16, 0)   # normal close — overridden on early-close days
_EARLY_CLOSE_TIME = time(13, 0)   # NYSE early-close at 1:00 PM ET
_POSTMARKET_END   = time(20, 0)

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
    Memorial Day, Juneteenth (2022+), Independence Day, Labor Day,
    Thanksgiving, Christmas Day.
    """
    h: list[date] = []

    h.append(_observed(date(year, 1, 1)))        # New Year's Day
    h.append(_nth_weekday(year, 1, 3, 0))        # MLK Jr. Day
    h.append(_nth_weekday(year, 2, 3, 0))        # Presidents' Day
    h.append(_easter(year) - timedelta(days=2))  # Good Friday
    h.append(_last_weekday(year, 5, 0))          # Memorial Day
    if year >= 2022:
        h.append(_observed(date(year, 6, 19)))   # Juneteenth
    h.append(_observed(date(year, 7, 4)))        # Independence Day
    h.append(_nth_weekday(year, 9, 1, 0))        # Labor Day
    h.append(_nth_weekday(year, 11, 4, 3))       # Thanksgiving
    h.append(_observed(date(year, 12, 25)))      # Christmas

    return frozenset(d for d in h if d.weekday() < 5)


_HOLIDAY_CACHE:     dict[int, frozenset[date]] = {}
_EARLY_CLOSE_CACHE: dict[int, frozenset[date]] = {}


def _holidays_for(year: int) -> frozenset[date]:
    if year not in _HOLIDAY_CACHE:
        _HOLIDAY_CACHE[year] = _us_market_holidays(year)
    return _HOLIDAY_CACHE[year]


def _early_close_dates(year: int) -> frozenset[date]:
    """
    Return NYSE early-close dates for *year* (1:00 PM ET close).

    Three categories:
    1. Black Friday — always: the Friday after Thanksgiving (4th Thursday, November).
    2. Christmas Eve (Dec 24) — when Dec 24 is a regular trading weekday
       (i.e. it is not itself an observed holiday such as when Dec 25 falls on Saturday).
    3. Day before Independence Day (July 3) — when July 4 falls on Thursday or Friday,
       making July 3 a trading day that NYSE closes early.
       NOT when July 3 is the observed holiday (July 4 on Saturday → July 3 observed Fri).
    """
    ec: list[date] = []
    holidays = _holidays_for(year)

    # 1. Black Friday (Friday after Thanksgiving)
    thanksgiving   = _nth_weekday(year, 11, 4, 3)
    black_friday   = thanksgiving + timedelta(days=1)   # always a Friday
    ec.append(black_friday)

    # 2. Christmas Eve (Dec 24) when it is a valid non-holiday weekday
    xmas_eve = date(year, 12, 24)
    if xmas_eve.weekday() < 5 and xmas_eve not in holidays:
        ec.append(xmas_eve)

    # 3. July 3 early close when July 4 falls on Thursday (Jul3=Wed) or Friday (Jul3=Thu)
    jul4 = date(year, 7, 4)
    jul3 = date(year, 7, 3)
    if jul4.weekday() in (3, 4) and jul3.weekday() < 5 and jul3 not in holidays:
        ec.append(jul3)

    return frozenset(ec)


def _early_close_for(year: int) -> frozenset[date]:
    if year not in _EARLY_CLOSE_CACHE:
        _EARLY_CLOSE_CACHE[year] = _early_close_dates(year)
    return _EARLY_CLOSE_CACHE[year]


def _is_trading_day(d: date) -> bool:
    """True if d is a valid US equity market trading day (Mon–Fri, not a holiday)."""
    if d.weekday() >= 5:
        return False
    return d not in _holidays_for(d.year)


def _regular_close(d: date) -> time:
    """
    Return the market close time (ET) for trading date d.
    Returns 13:00 ET on NYSE early-close days, 16:00 ET otherwise.
    """
    if d in _early_close_for(d.year):
        return _EARLY_CLOSE_TIME
    return _MARKET_CLOSE


def is_early_close_day(d: date | None = None) -> bool:
    """True if d (defaults to today ET) is an NYSE early-close trading day."""
    if d is None:
        d = datetime.now(_ET).date()
    return d in _early_close_for(d.year)


# ── Session classification ────────────────────────────────────────────────────

def get_session(now_et: datetime | None = None) -> TradierSession:
    """Return the current Tradier market session label."""
    if now_et is None:
        now_et = datetime.now(_ET)
    wd = now_et.weekday()
    if wd >= 5:
        return "weekend"
    t  = now_et.time().replace(second=0, microsecond=0)
    close = _regular_close(now_et.date())
    if _PREMARKET_START <= t < _MARKET_OPEN:
        return "premarket"
    if _MARKET_OPEN <= t < close:
        return "regular"
    if close <= t < _POSTMARKET_END:
        return "postmarket"
    return "off_hours"


def is_active_session(now_et: datetime | None = None) -> bool:
    """
    True when the Tradier budget should be enforced (pre/regular/post-market, weekday).
    LEGACY — used only for budget-enforcement gate and tradier_provider.py.
    For options chain scan gating use is_regular_options_session() instead.
    """
    return get_session(now_et) in _ACTIVE_SESSIONS


def is_regular_options_session(now_et: datetime | None = None) -> bool:
    """
    True ONLY during the regular US options trading session:
      - America/New_York timezone
      - Valid US trading day (Mon–Fri, not a market holiday)
      - 09:30 ET ≤ time < market close ET
        (market close is 13:00 ET on early-close days, 16:00 ET otherwise)

    This is the canonical gate for ALL Tradier options chain scans.
    Returns False during premarket, postmarket, off-hours, weekends,
    US market holidays, and at/after the actual early-close time.
    When False, callers must serve cached LKG data and make zero Tradier calls.
    """
    if now_et is None:
        now_et = datetime.now(_ET)
    if now_et.weekday() >= 5:
        return False
    today = now_et.date()
    if today in _holidays_for(today.year):
        return False
    t     = now_et.time()
    close = _regular_close(today)
    return time(9, 30) <= t < close


def _next_regular_open(now_et: datetime) -> datetime:
    """Return the datetime of the next regular session open (09:30 ET)."""
    candidate = now_et.date()
    if now_et.time() < time(9, 30) and _is_trading_day(candidate):
        return datetime(candidate.year, candidate.month, candidate.day, 9, 30, tzinfo=_ET)
    candidate += timedelta(days=1)
    for _ in range(14):
        if _is_trading_day(candidate):
            return datetime(candidate.year, candidate.month, candidate.day, 9, 30, tzinfo=_ET)
        candidate += timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)
    return datetime(candidate.year, candidate.month, candidate.day, 9, 30, tzinfo=_ET)


def get_session_info(now_et: datetime | None = None) -> dict:
    """Legacy session info dict (backward compat)."""
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
      options_market_session   — session label
      options_scan_allowed     — True only during regular session on a trading day
      regular_open_at          — today's open (ISO, ET) or None
      regular_close_at         — today's actual close (ISO, ET) — 13:00 on early-close days
      is_early_close_day       — True when today is an NYSE early-close day
      next_regular_open_at     — next regular open datetime (ISO, ET)
      market_calendar_status   — "trading_day" | "holiday" | "weekend"
      reason                   — human-readable explanation
      is_us_market_holiday     — True if today is a full NYSE holiday
    """
    if now_et is None:
        now_et = datetime.now(_ET)

    session    = get_session(now_et)
    scan_ok    = is_regular_options_session(now_et)
    today      = now_et.date()
    is_weekend = today.weekday() >= 5
    is_holiday = (not is_weekend) and (today in _holidays_for(today.year))
    is_trading = _is_trading_day(today)
    early_cls  = is_trading and (today in _early_close_for(today.year))
    close_time = _regular_close(today) if is_trading else _MARKET_CLOSE

    if is_weekend:
        cal_status = "weekend"
        reason = f"Weekend ({now_et.strftime('%A')}); options scans blocked"
    elif is_holiday:
        cal_status = "holiday"
        reason = f"US market holiday ({today.isoformat()}); options scans blocked"
    else:
        cal_status = "trading_day"
        t = now_et.time()
        if t < time(9, 30):
            reason = "Before regular open (09:30 ET); LKG served"
        elif t < close_time:
            suffix = f" (early close {close_time.strftime('%H:%M')} ET)" if early_cls else ""
            reason = f"Regular session active{suffix}; scans permitted"
        else:
            suffix = f" early close ({close_time.strftime('%H:%M')} ET)" if early_cls else " (16:00 ET)"
            reason = f"After{suffix}; LKG served"

    regular_open  = None
    regular_close = None
    if is_trading:
        regular_open  = datetime(today.year, today.month, today.day,  9, 30, tzinfo=_ET).isoformat()
        regular_close = datetime(
            today.year, today.month, today.day,
            close_time.hour, close_time.minute, tzinfo=_ET,
        ).isoformat()

    next_open = _next_regular_open(now_et)

    return {
        "options_market_session":  session,
        "options_scan_allowed":    scan_ok,
        "regular_open_at":         regular_open,
        "regular_close_at":        regular_close,
        "is_early_close_day":      early_cls,
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
    print("[TRADIER_SESSION_TEST] Running session self-test:")
    cases = [
        (0,  5,  0,  "premarket",  False),
        (0,  9, 30,  "regular",    True),
        (0, 14,  0,  "regular",    True),
        (0, 16,  0,  "postmarket", False),
        (0, 19, 59,  "postmarket", False),
        (0, 20,  0,  "off_hours",  False),
        (0,  2,  0,  "off_hours",  False),
        (5, 12,  0,  "weekend",    False),
        (6, 12,  0,  "weekend",    False),
        (0,  9, 29,  "premarket",  False),
        (4, 15, 59,  "regular",    True),
        (4, 16,  0,  "postmarket", False),
    ]
    all_ok = True
    for wd, h, m, exp_sess, exp_scan in cases:
        r  = _simulate(h, m, weekday=wd)
        ok = r["session"] == exp_sess and r["scan_ok"] == exp_scan
        if not ok:
            all_ok = False
        print(f"  {'✓' if ok else '✗'} wd={wd} {h:02d}:{m:02d} → {r['session']!r} scan={r['scan_ok']}")

    # Early-close: Black Friday 2026 (Nov 27)
    bf2026 = date(2026, 11, 27)
    fake_bf_before = datetime(2026, 11, 27, 12, 59, tzinfo=_ET)
    fake_bf_after  = datetime(2026, 11, 27, 13,  0, tzinfo=_ET)
    ok_before = is_regular_options_session(fake_bf_before)
    ok_after  = not is_regular_options_session(fake_bf_after)
    flag1 = "✓" if ok_before else "✗"
    flag2 = "✓" if ok_after  else "✗"
    print(f"  {flag1} Black Friday 2026 12:59 ET → scan={ok_before} (expected True)")
    print(f"  {flag2} Black Friday 2026 13:00 ET → scan={not ok_after} (expected False)")
    if not (ok_before and ok_after):
        all_ok = False

    # Holiday: Jul 3 2026 (observed full holiday)
    fake_holiday = datetime(2026, 7, 3, 11, 0, tzinfo=_ET)
    h_scan = is_regular_options_session(fake_holiday)
    flag = "✓" if not h_scan else "✗"
    print(f"  {flag} Jul 3 2026 (observed holiday) 11:00 ET → scan={h_scan} (expected False)")
    if h_scan:
        all_ok = False

    print(f"[TRADIER_SESSION_TEST] {'All passed' if all_ok else 'FAILURES DETECTED'}")


if __name__ == "__main__":
    _run_self_test()
