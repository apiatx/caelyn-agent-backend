"""
FMP /stable/historical-price-eod/full guard.

Environment variables
─────────────────────
FMP_BLOCK_FULL_HISTORICAL=true
    Block every call to /stable/historical-price-eod/full.
    Affected callers return their in-process cache (if warm) or an empty list.
    No crash, no page breakage.

FMP_DIAGNOSTIC_DRY_RUN=true
    Log every attempted /full call (symbol, caller, short stack trace) but
    do NOT block the request.  Use this to measure call volume before blocking.

When both are set, FMP_BLOCK_FULL_HISTORICAL takes precedence.

Safe to import anywhere — no network calls, no heavy deps.
"""
from __future__ import annotations

import os
import traceback
from datetime import datetime, timezone


def _env_true(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in ("1", "true", "yes", "on")


def is_full_historical_blocked() -> bool:
    """Return True when FMP_BLOCK_FULL_HISTORICAL=true is set."""
    return _env_true("FMP_BLOCK_FULL_HISTORICAL")


def is_diagnostic_dry_run() -> bool:
    """Return True when FMP_DIAGNOSTIC_DRY_RUN=true is set."""
    return _env_true("FMP_DIAGNOSTIC_DRY_RUN")


def log_and_check(
    symbol: str,
    caller_func: str = "",
    caller_file: str = "",
    job_name: str = "",
    extra: dict | None = None,
) -> bool:
    """
    Check whether a /stable/historical-price-eod/full call should be blocked,
    log it if either guard env-var is active, and return whether to block.

    Returns
    -------
    True  → caller should NOT issue the FMP request (return cached data / [])
    False → caller may proceed normally
    """
    blocked = is_full_historical_blocked()
    dry_run = is_diagnostic_dry_run()

    if not blocked and not dry_run:
        return False

    ts = datetime.now(timezone.utc).isoformat()

    # Short stack trace: skip log_and_check itself, show up to 5 caller frames
    raw_frames = traceback.format_stack(limit=10)[:-1]
    short_stack = "".join(raw_frames[-5:]).rstrip()

    extra_str = (
        " ".join(f"{k}={v}" for k, v in extra.items()) if extra else ""
    )

    print(
        f"[FMP_FULL_GUARD] WOULD_CALL_FULL_HISTORICAL=true"
        f" blocked={str(blocked).lower()}"
        f" ts={ts}"
        f" endpoint=/stable/historical-price-eod/full"
        f" symbol={symbol}"
        f" caller={caller_func}"
        f" caller_file={caller_file}"
        f" job_name={job_name}"
        + (f" {extra_str}" if extra_str else "")
    )
    if short_stack:
        print(f"[FMP_FULL_GUARD] stack:\n{short_stack}")

    return blocked
