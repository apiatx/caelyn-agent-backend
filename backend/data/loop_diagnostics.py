"""
loop_diagnostics.py — Shared mutable state for background-loop health reporting.

Written to by _master_screener_loop and _theme_options_supplement_loop in main.py.
Read by /api/rate-status.

All mutations happen on the asyncio event loop — no thread-safety locks needed.
"""
from __future__ import annotations

import time
from collections import deque
from typing import Deque, Literal

LoopMode = Literal["active", "maintenance", "sleeping", "initializing"]

# ── Master screener ────────────────────────────────────────────────────────────
_ms_mode:         LoopMode   = "initializing"
_ms_last_run_at:  float | None = None
_ms_next_run_at:  float | None = None

# ── Supplement loop ───────────────────────────────────────────────────────────
_supp_mode:        LoopMode   = "initializing"
_supp_last_run_at: float | None = None
_supp_next_run_at: float | None = None

# ── Off-hours suppression counter (lifetime, reset on restart) ────────────────
_offhours_suppressed_count: int = 0

# ── Options-loop Tradier call timestamps — last-60s window ───────────────────
_options_call_ts: Deque[float] = deque(maxlen=2000)


# ── Mutation helpers ──────────────────────────────────────────────────────────

def update_master_loop(
    mode: LoopMode,
    last_run_at: float | None = None,
    next_run_at: float | None = None,
) -> None:
    global _ms_mode, _ms_last_run_at, _ms_next_run_at
    _ms_mode = mode
    if last_run_at is not None:
        _ms_last_run_at = last_run_at
    if next_run_at is not None:
        _ms_next_run_at = next_run_at


def update_supplement_loop(
    mode: LoopMode,
    last_run_at: float | None = None,
    next_run_at: float | None = None,
) -> None:
    global _supp_mode, _supp_last_run_at, _supp_next_run_at
    _supp_mode = mode
    if last_run_at is not None:
        _supp_last_run_at = last_run_at
    if next_run_at is not None:
        _supp_next_run_at = next_run_at


def increment_suppressed(count: int = 1) -> None:
    """Record that <count> off-hours loop cycles were suppressed."""
    global _offhours_suppressed_count
    _offhours_suppressed_count += count


def record_options_calls(n: int = 1) -> None:
    """Record that n Tradier options calls were made by a background loop."""
    now = time.time()
    for _ in range(n):
        _options_call_ts.append(now)


# ── Read-only snapshot ────────────────────────────────────────────────────────

def get_loop_diag() -> dict:
    """Return a snapshot of all loop diagnostics for /api/rate-status."""
    now = time.time()
    cutoff = now - 60
    calls_last_60s = sum(1 for t in _options_call_ts if t > cutoff)
    return {
        "master_screener_loop_mode":              _ms_mode,
        "master_screener_last_run_at":            _ms_last_run_at,
        "master_screener_next_run_at":            _ms_next_run_at,
        "theme_supplement_loop_mode":             _supp_mode,
        "theme_supplement_last_run_at":           _supp_last_run_at,
        "theme_supplement_next_run_at":           _supp_next_run_at,
        "offhours_options_calls_suppressed_count": _offhours_suppressed_count,
        "options_loop_calls_last_60s":            calls_last_60s,
    }
