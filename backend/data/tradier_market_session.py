"""
tradier_market_session.py — Eastern-Time market session classifier.

Session definitions (Eastern Time):
  premarket  : Monday–Friday 04:00–09:30
  regular    : Monday–Friday 09:30–16:00
  postmarket : Monday–Friday 16:00–20:00
  off_hours  : Monday–Friday 20:00–04:00 (next weekday)
  weekend    : Saturday 00:00 – Sunday 23:59

Active sessions (Tradier options data is meaningful and options loops run normally):
  premarket, regular, postmarket

Inactive sessions (options-chain loops run at reduced maintenance cadence):
  off_hours, weekend
"""
from __future__ import annotations

from datetime import datetime, time
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
    """True when options data is meaningful (pre/regular/post-market, weekday)."""
    return get_session(now_et) in _ACTIVE_SESSIONS


def get_session_info(now_et: datetime | None = None) -> dict:
    """Return a dict with session classification and current ET metadata."""
    if now_et is None:
        now_et = datetime.now(_ET)
    session = get_session(now_et)
    return {
        "tradier_market_session":    session,
        "is_tradier_active_session": session in _ACTIVE_SESSIONS,
        "session_time_et":           now_et.strftime("%H:%M:%S"),
        "session_weekday_et":        now_et.strftime("%A"),
    }


# ── Self-test (unit-level, run directly or from a test harness) ───────────────

def _simulate_session(hour: int, minute: int = 0, weekday: int = 0) -> dict:
    """Create a fake datetime for the given ET hour/minute and weekday (0=Mon)."""
    from datetime import date
    today = date.today()
    # Find the nearest date with the right weekday
    from datetime import timedelta
    delta = (weekday - today.weekday()) % 7
    target_date = today + timedelta(days=delta)
    fake = datetime(target_date.year, target_date.month, target_date.day,
                    hour, minute, tzinfo=_ET)
    return get_session_info(fake)


def _run_self_test() -> None:
    """Quick sanity-check across all five session states."""
    cases = [
        (0,  5,  0, 0, "premarket"),   # 05:00 ET Mon — premarket window
        (0,  9, 30, 0, "regular"),
        (0, 14,  0, 0, "regular"),
        (0, 16,  0, 0, "postmarket"),
        (0, 19, 59, 0, "postmarket"),
        (0, 20,  0, 0, "off_hours"),
        (0,  2,  0, 0, "off_hours"),
        (5,  12, 0, 5, "weekend"),
        (6,  12, 0, 6, "weekend"),
    ]
    print("[TRADIER_SESSION_TEST] Running session classification self-test:")
    all_ok = True
    for wd, h, m, _, expected in cases:
        info = _simulate_session(h, m, weekday=wd)
        got = info["tradier_market_session"]
        ok = got == expected
        if not ok:
            all_ok = False
        print(f"  {'✓' if ok else '✗'} weekday={wd} {h:02d}:{m:02d} ET → {got!r} (expected {expected!r})")
    print(f"[TRADIER_SESSION_TEST] {'All passed' if all_ok else 'FAILURES DETECTED'}")


if __name__ == "__main__":
    _run_self_test()
