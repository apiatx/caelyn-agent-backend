"""
FMP API Budget Governor — Screener Hub + scheduled-job paths only.

Opt-in wrapper. Does NOT touch:
  - data.fmp_provider.FMPProvider  (Fundamentals page, Stock Compare, etc.)
  - Any existing FMP calls outside the Screener Hub service layer

Env vars (all optional, safe defaults):
  FMP_RPM_LIMIT                 default 120   soft RPM cap
  FMP_CALL_SPACING_SECONDS      default 0.75  min gap between successive calls
  FMP_JOB_SOFT_LIMIT            default 500   max calls per job run
  FMP_DAILY_SOFT_LIMIT          default 2500  soft daily cap (advisory)
  FMP_MONTHLY_SOFT_CALL_LIMIT   default 50000 soft monthly cap (advisory)

Usage (in screener hub paths only):
    from services.fmp_governor import fmp_governor

    ok = await fmp_governor.acquire(job_name="returns_warm")
    if not ok:
        # budget exceeded — skip call, serve stale cache
        break
    try:
        resp = await client.get(...)
    finally:
        fmp_governor.record_call()
"""
from __future__ import annotations

import asyncio
import os
import time
from collections import deque
from datetime import date, datetime, timezone
from typing import Optional


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _today() -> str:
    return date.today().isoformat()


def _month() -> str:
    return date.today().strftime("%Y-%m")


class FMPGovernor:
    """Thread-safe (asyncio) FMP budget governor."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()

        # RPM sliding window
        self._rpm_window: deque[float] = deque()

        # Spacing
        self._last_call_at: float = 0.0

        # Daily counter (reset at UTC midnight on first call of the day)
        self._daily_calls: int = 0
        self._daily_date: str = ""

        # Monthly counter
        self._monthly_calls: int = 0
        self._monthly_month: str = ""

        # Per-job counter (reset per start_job call)
        self._job_calls: int = 0
        self._job_name: str = ""
        self._job_budget_hit: bool = False

        # History rings
        self._latest_jobs: list[dict] = []
        self._budget_limited_jobs: list[dict] = []

    # ── Config (read live from env so Replit secrets override immediately) ──

    @property
    def rpm_limit(self) -> int:
        return max(1, int(os.getenv("FMP_RPM_LIMIT", "120")))

    @property
    def call_spacing_s(self) -> float:
        return max(0.0, float(os.getenv("FMP_CALL_SPACING_SECONDS", "0.75")))

    @property
    def job_soft_limit(self) -> int:
        return max(1, int(os.getenv("FMP_JOB_SOFT_LIMIT", "500")))

    @property
    def daily_soft_limit(self) -> int:
        return max(1, int(os.getenv("FMP_DAILY_SOFT_LIMIT", "2500")))

    @property
    def monthly_soft_limit(self) -> int:
        return max(1, int(os.getenv("FMP_MONTHLY_SOFT_CALL_LIMIT", "50000")))

    # ── Job lifecycle ──────────────────────────────────────────────────────

    def start_job(self, job_name: str) -> None:
        self._job_calls = 0
        self._job_name = job_name
        self._job_budget_hit = False

    def finish_job(self, job_name: str, *, budget_limited: bool = False) -> None:
        entry = {
            "job_name": job_name,
            "calls_used": self._job_calls,
            "budget_limited": budget_limited,
            "finished_at": _iso_now(),
        }
        self._latest_jobs.insert(0, entry)
        self._latest_jobs = self._latest_jobs[:20]
        if budget_limited:
            self._budget_limited_jobs.insert(0, entry)
            self._budget_limited_jobs = self._budget_limited_jobs[:10]
        self._job_calls = 0
        self._job_name = ""
        self._job_budget_hit = False

    # ── Per-call API ────────────────────────────────────────────────────────

    async def acquire(self, job_name: str = "") -> bool:
        """
        Enforce spacing + RPM limit. Sleep if necessary.

        Returns True when a call slot is granted.
        Returns False immediately if any soft budget ceiling is hit
        (job limit, daily limit) — caller should stop gracefully.

        Safe to call from multiple concurrent coroutines.
        """
        async with self._lock:
            # ── Check daily cap ──
            today = _today()
            if self._daily_date != today:
                self._daily_date = today
                self._daily_calls = 0
            if self._daily_calls >= self.daily_soft_limit:
                print(f"[FMP_GOV] daily soft limit {self.daily_soft_limit} reached "
                      f"(today={self._daily_calls}); skipping call")
                return False

            # ── Check per-job cap ──
            if self._job_calls >= self.job_soft_limit:
                if not self._job_budget_hit:
                    self._job_budget_hit = True
                    print(f"[FMP_GOV] job soft limit {self.job_soft_limit} reached "
                          f"for job='{self._job_name or job_name}'")
                return False

            # ── RPM sliding window ──
            now = time.monotonic()
            while self._rpm_window and self._rpm_window[0] < now - 60.0:
                self._rpm_window.popleft()
            if len(self._rpm_window) >= self.rpm_limit:
                oldest = self._rpm_window[0]
                wait_s = 60.0 - (now - oldest) + 0.05
                if wait_s > 0:
                    await asyncio.sleep(wait_s)
                # Re-purge after sleep
                now = time.monotonic()
                while self._rpm_window and self._rpm_window[0] < now - 60.0:
                    self._rpm_window.popleft()

            # ── Spacing ──
            elapsed = time.monotonic() - self._last_call_at
            if elapsed < self.call_spacing_s:
                await asyncio.sleep(self.call_spacing_s - elapsed)

            return True

    def record_call(self) -> None:
        """Call immediately after a successful FMP HTTP request."""
        now = time.monotonic()
        self._last_call_at = now
        self._rpm_window.append(now)

        self._job_calls += 1

        today = _today()
        if self._daily_date != today:
            self._daily_date = today
            self._daily_calls = 0
        self._daily_calls += 1

        month = _month()
        if self._monthly_month != month:
            self._monthly_month = month
            self._monthly_calls = 0
        self._monthly_calls += 1

    # ── Status ──────────────────────────────────────────────────────────────

    def status(self) -> dict:
        today = _today()
        if self._daily_date != today:
            self._daily_calls = 0

        # Estimate RPM from sliding window
        now = time.monotonic()
        recent_rpm = sum(1 for t in self._rpm_window if t >= now - 60.0)

        return {
            "enabled": True,
            "rpm_limit": self.rpm_limit,
            "call_spacing_seconds": self.call_spacing_s,
            "job_soft_limit": self.job_soft_limit,
            "daily_soft_limit": self.daily_soft_limit,
            "monthly_soft_limit": self.monthly_soft_limit,
            "daily_calls_today": self._daily_calls,
            "monthly_calls": self._monthly_calls,
            "rpm_last_60s": recent_rpm,
            "job_calls_current": self._job_calls,
            "current_job": self._job_name,
            "historical_calls_today_estimate": self._daily_calls,
            "latest_jobs": self._latest_jobs[:5],
            "budget_limited_jobs": self._budget_limited_jobs[:5],
        }


# ── Module-level singleton — import this in Screener Hub paths only ──────────
fmp_governor = FMPGovernor()
