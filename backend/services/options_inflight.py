"""
Global options-symbol in-flight guard.

Replaces the watchlist-only _WL_INFLIGHT_SYMS set with a shared,
scope-tagged registry used by every component that runs live Tradier
options scans: Watchlist, Portfolio, and the theme supplement loop.

The Sectors/Themes aggregation endpoint (options_flow_sectors.py) is already
a zero-scan pure aggregation layer and does not need to claim symbols here.

API
---
is_options_inflight(sym)             -> bool
claim_options_inflight(sym, scope)   -> bool  (True = claimed, False = already running)
release_options_inflight(sym, scope) -> None
claim_many(syms, scope)              -> (claimed: list, blocked: list)
release_many(syms, scope)            -> None
get_inflight_status()                -> dict   (for diagnostics endpoint)

Thread-safety
-------------
Uses a threading.Lock because asyncio tasks and executor threads may both
read/write the registry.  All operations are O(n) or better.
"""
from __future__ import annotations

import threading

_lock = threading.Lock()

# sym.upper() -> scope string ("watchlist" | "portfolio" | "supplement" | ...)
_inflight: dict[str, str] = {}

_total_claimed_lifetime: int  = 0
_total_blocked_lifetime: int  = 0


# ── Public API ─────────────────────────────────────────────────────────────────

def is_options_inflight(sym: str) -> bool:
    """Non-blocking check — True if a live scan for sym is already running."""
    return sym.upper() in _inflight


def claim_options_inflight(sym: str, scope: str) -> bool:
    """
    Attempt to claim sym for a live scan under scope.

    Returns True if the claim succeeded (caller should proceed with scan).
    Returns False if sym is already claimed by any scope (caller should skip).
    """
    global _total_claimed_lifetime, _total_blocked_lifetime
    sym = sym.upper()
    with _lock:
        if sym in _inflight:
            _total_blocked_lifetime += 1
            return False
        _inflight[sym] = scope
        _total_claimed_lifetime += 1
        return True


def release_options_inflight(sym: str, scope: str | None = None) -> None:
    """Release a previously claimed symbol.  scope is ignored (any scope releases)."""
    with _lock:
        _inflight.pop(sym.upper(), None)


def claim_many(syms: list[str], scope: str) -> tuple[list[str], list[str]]:
    """
    Batch-claim a list of symbols.

    Returns (claimed, blocked):
      claimed — symbols successfully claimed; caller must scan + release these.
      blocked — symbols already in-flight from any scope; caller must skip these.
    """
    global _total_claimed_lifetime, _total_blocked_lifetime
    claimed: list[str] = []
    blocked: list[str] = []
    with _lock:
        for s in syms:
            su = s.upper()
            if su in _inflight:
                _total_blocked_lifetime += 1
                blocked.append(su)
            else:
                _inflight[su] = scope
                _total_claimed_lifetime += 1
                claimed.append(su)
    return claimed, blocked


def release_many(syms: list[str], scope: str | None = None) -> None:
    """Batch-release a list of previously claimed symbols."""
    with _lock:
        for s in syms:
            _inflight.pop(s.upper(), None)


def get_inflight_status() -> dict:
    """Return a diagnostics snapshot suitable for the /api/rate-status endpoint."""
    with _lock:
        snap = dict(_inflight)
    by_scope: dict[str, list[str]] = {}
    for sym, scope in snap.items():
        by_scope.setdefault(scope, []).append(sym)
    return {
        "total_inflight":           len(snap),
        "by_scope":                 by_scope,
        "total_claimed_lifetime":   _total_claimed_lifetime,
        "total_blocked_lifetime":   _total_blocked_lifetime,
    }
