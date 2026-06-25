"""
tradier_budget.py — Per-lane RPM budget guardrail for the Tradier provider.

Architecture
------------
Budget is a HARD CAP per lane (no cross-lane borrowing in Phase 3).
Accounting is centralised at the physical-HTTP boundary inside
TradierProvider._get() — cache hits and coalesced waiters never call _get(),
so they never consume budget.

Lane identity flows through an asyncio ContextVar.  Call sites wrap their
engine/provider calls in the lane() context manager; _get() reads the ContextVar.
asyncio.create_task() copies the calling context, so engine subtasks inherit
the correct lane automatically.

Lanes
-----
  quotes         TRADIER_QUOTE_RPM_BUDGET=30   equity quote calls
  options_flow   TRADIER_OPTIONS_FLOW_RPM_BUDGET=40   master screener chains
  saved_options  TRADIER_SAVED_OPTIONS_RPM_BUDGET=25  watchlist/portfolio options
  maintenance    TRADIER_MAINTENANCE_RPM_BUDGET=20    supplement/stale-LKG/gap-fill
  reserved       TRADIER_RESERVED_RPM_BUDGET=5        popup/manual/untagged (default)

Sum of defaults = 120 = global TRADIER_MARKET_DATA_RPM cap.
"""
from __future__ import annotations

import os
import time
from collections import deque
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator

LANE_NAMES: tuple[str, ...] = (
    "quotes",
    "options_flow",
    "saved_options",
    "maintenance",
    "reserved",
)
WINDOW_S: float = 60.0  # sliding window duration (seconds)

# Test/ops override: set TRADIER_BUDGET_FORCE_ENFORCE=1 to enforce budget
# regardless of market session (useful for active-session validation off-hours).
# Read once at import — requires server restart to take effect.
FORCE_ENFORCE: bool = os.environ.get("TRADIER_BUDGET_FORCE_ENFORCE", "").lower() in (
    "1", "true", "yes"
)

# ── ContextVar: active lane for the current asyncio task ─────────────────────
_CURRENT_LANE: ContextVar[str] = ContextVar("tradier_lane", default="reserved")


@contextmanager
def lane(name: str) -> Iterator[None]:
    """Set the Tradier budget lane for all _get() calls inside this scope.

    Works correctly with async code: ContextVar is per-asyncio-task, so
    concurrent tasks from other loops do not interfere.

    Usage::

        with lane("options_flow"):
            result = await engine.run_live_scan(...)
    """
    token = _CURRENT_LANE.set(name)
    try:
        yield
    finally:
        _CURRENT_LANE.reset(token)


def get_current_lane() -> str:
    """Return the lane name for the current asyncio task (default: 'reserved')."""
    return _CURRENT_LANE.get()


# ── Budget config (env-configurable, read once at import) ────────────────────

def _env_int(key: str, default: int) -> int:
    try:
        return max(0, int(os.environ.get(key, default)))
    except (ValueError, TypeError):
        return default


BUDGETS: dict[str, int] = {
    "quotes":        _env_int("TRADIER_QUOTE_RPM_BUDGET",         30),
    "options_flow":  _env_int("TRADIER_OPTIONS_FLOW_RPM_BUDGET",  40),
    "saved_options": _env_int("TRADIER_SAVED_OPTIONS_RPM_BUDGET", 25),
    "maintenance":   _env_int("TRADIER_MAINTENANCE_RPM_BUDGET",   20),
    "reserved":      _env_int("TRADIER_RESERVED_RPM_BUDGET",       5),
}

# ── Per-lane sliding-window state ─────────────────────────────────────────────
# All mutations happen on the asyncio event loop — no locks needed.
_timestamps: dict[str, deque[float]] = {ln: deque() for ln in LANE_NAMES}
_deferred:   dict[str, int]          = {ln: 0       for ln in LANE_NAMES}
_oldest_deferred: dict[str, float | None] = {ln: None for ln in LANE_NAMES}


def _normalise(ln: str) -> str:
    return ln if ln in LANE_NAMES else "reserved"


def _prune(ln: str) -> None:
    """Evict timestamps older than WINDOW_S from the lane's deque."""
    cutoff = time.monotonic() - WINDOW_S
    d = _timestamps[ln]
    while d and d[0] < cutoff:
        d.popleft()


# ── Public API (called from TradierProvider._get()) ───────────────────────────

def check_budget(ln: str) -> bool:
    """Non-blocking.  True if the lane has RPM headroom remaining.

    Called BEFORE TRADIER_LIMITER.acquire().  If False, the caller should
    record a deferral and return None — the upstream code falls back to LKG.
    """
    ln = _normalise(ln)
    _prune(ln)
    return len(_timestamps[ln]) < BUDGETS[ln]


def record_call(ln: str) -> None:
    """Record one physical Tradier HTTP call against the lane's budget window.

    Called AFTER TRADIER_LIMITER.acquire() succeeds, before the HTTP request
    fires.  Cache hits and coalesced waiters never reach this point.
    """
    ln = _normalise(ln)
    _timestamps[ln].append(time.monotonic())


def record_defer(ln: str) -> None:
    """Record a budget-induced deferral for diagnostics."""
    ln = _normalise(ln)
    _deferred[ln] += 1
    if _oldest_deferred[ln] is None:
        _oldest_deferred[ln] = time.time()


# ── Diagnostics snapshot ───────────────────────────────────────────────────────

def diagnostics() -> dict:
    """Return all Phase 3 budget fields for /api/rate-status."""
    now_mono = time.monotonic()
    cutoff   = now_mono - WINDOW_S
    calls_60s: dict[str, int] = {
        ln: sum(1 for t in _timestamps[ln] if t > cutoff)
        for ln in LANE_NAMES
    }
    saturation: dict[str, bool] = {
        ln: calls_60s[ln] >= BUDGETS[ln]
        for ln in LANE_NAMES
    }
    try:
        from data.tradier_market_session import is_active_session as _ia
        _enforcing = FORCE_ENFORCE or _ia()
    except Exception:
        _enforcing = True
    return {
        "tradier_budget_enabled":          True,
        "budget_enforcement_active":       _enforcing,
        "budget_by_lane":                  dict(BUDGETS),
        "calls_last_60s_by_lane":          calls_60s,
        "deferred_by_lane":                dict(_deferred),
        "lane_saturation":                 saturation,
        "options_flow_budget_used":        calls_60s["options_flow"],
        "quote_budget_used":               calls_60s["quotes"],
        "saved_options_budget_used":       calls_60s["saved_options"],
        "maintenance_budget_used":         calls_60s["maintenance"],
        "reserved_budget_used":            calls_60s["reserved"],
        "budget_rejections_or_deferrals":  sum(_deferred.values()),
        "oldest_deferred_by_lane":         dict(_oldest_deferred),
        "last_budget_reset_at":            None,
    }
