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

Call-type counters
------------------
record_provider_call(call_type, scope) — increment per-type call counter.
Call types: "expiry", "chain", "quote"
Scopes:     "watchlist" | "portfolio" | "supplement" | "sectors" | other

Scan fingerprints
-----------------
make_scan_fingerprint(ticker, session_date, schema_version) -> str
  Returns a canonical string that identifies ONE unit of provider work:
  ticker + session date (ET) + schema version.
  Format: "AAPL:2026-07-26:v1"
  All consumers that need data for a ticker in a given session share the
  same fingerprint — requesting page / scope / product area are NOT part
  of the fingerprint so that one in-flight claim serves all callers.

record_scan_fingerprint(sym, fingerprint) — store last-used fingerprint.
get_scan_fingerprint(sym)                 -> str | None

Thread-safety
-------------
Uses a threading.Lock because asyncio tasks and executor threads may both
read/write the registry.  All operations are O(n) or better.
"""
from __future__ import annotations

import datetime as _datetime
import threading
import time as _time

_lock = threading.Lock()

# sym.upper() -> scope string ("watchlist" | "portfolio" | "supplement" | ...)
_inflight: dict[str, str] = {}

# ── Lifetime counters ───────────────────────────────────────────────────────
_total_claimed_lifetime:  int = 0
_total_blocked_lifetime:  int = 0

# Per call-type counters (expiry, chain, quote)
_call_counts:  dict[str, int] = {
    "expiry": 0,
    "chain":  0,
    "quote":  0,
}

# Per scope counters (nested: scope -> call_type -> count)
_scope_call_counts: dict[str, dict[str, int]] = {}

# Coalescing: calls blocked because a scan was already in-flight
_coalesced_lifetime: int = 0

# Cache-hit counters (populated by callers via record_cache_hit)
_cache_hits:      dict[str, int] = {"master": 0, "supplement": 0, "lkg": 0, "canonical": 0}
_cache_misses:    int = 0

# Session start time for rate calculations
_session_start: float = _time.time()

# ── Scan fingerprints ────────────────────────────────────────────────────────
# Maps sym.upper() -> fingerprint string used for the most-recent scan.
# A fingerprint is the canonical identity of ONE provider-work unit:
#   "{TICKER}:{YYYY-MM-DD}:{schema_version}"   e.g. "AAPL:2026-07-26:v1"
# Session date is in US/Eastern (approximate -5 h offset for portability).
_fingerprints: dict[str, str] = {}

_ET_OFFSET = _datetime.timezone(_datetime.timedelta(hours=-5))


def make_scan_fingerprint(
    ticker: str,
    session_date: str | None = None,
    schema_version: str = "v1",
) -> str:
    """
    Return the canonical scan fingerprint for ticker + session + schema.

    session_date defaults to today in ET (approximate -5 h).
    All consumers that request options data for the same ticker on the same
    trading day receive the same fingerprint, regardless of scope (Watchlist,
    Portfolio, popup …).  This lets the diagnostics endpoint prove that only
    one provider call occurred per ticker per day.
    """
    if session_date is None:
        session_date = _datetime.datetime.now(_ET_OFFSET).strftime("%Y-%m-%d")
    return f"{ticker.upper()}:{session_date}:{schema_version}"


def record_scan_fingerprint(sym: str, fingerprint: str) -> None:
    """Store the fingerprint used for the most-recent scan of sym."""
    with _lock:
        _fingerprints[sym.upper()] = fingerprint


def get_scan_fingerprint(sym: str) -> str | None:
    """Return the fingerprint for the last scan of sym, or None."""
    return _fingerprints.get(sym.upper())


# ── Public API ─────────────────────────────────────────────────────────────────

def is_options_inflight(sym: str) -> bool:
    """Non-blocking check — True if a live scan for sym is already running."""
    return sym.upper() in _inflight


def claim_options_inflight(sym: str, scope: str) -> bool:
    """
    Attempt to claim sym for a live scan under scope.

    Returns True if the claim succeeded (caller should proceed with scan).
    Returns False if sym is already claimed by any scope (caller should skip).
    When False the symbol is coalesced — another scan is already producing
    the result; the caller should read from cache when it is ready.
    """
    global _total_claimed_lifetime, _total_blocked_lifetime, _coalesced_lifetime
    sym = sym.upper()
    with _lock:
        if sym in _inflight:
            _total_blocked_lifetime += 1
            _coalesced_lifetime      += 1
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
    global _total_claimed_lifetime, _total_blocked_lifetime, _coalesced_lifetime
    claimed: list[str] = []
    blocked: list[str] = []
    with _lock:
        for s in syms:
            su = s.upper()
            if su in _inflight:
                _total_blocked_lifetime += 1
                _coalesced_lifetime      += 1
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


def record_provider_call(call_type: str, scope: str = "unknown") -> None:
    """
    Increment per-type and per-scope Tradier provider call counters.

    call_type : "expiry" | "chain" | "quote"
    scope     : "watchlist" | "portfolio" | "supplement" | "sectors" | other

    Call this immediately before each Tradier options API call so the
    deduplication diagnostics endpoint can prove coalescing is working.
    """
    with _lock:
        _call_counts[call_type] = _call_counts.get(call_type, 0) + 1
        scope_bucket = _scope_call_counts.setdefault(scope, {})
        scope_bucket[call_type] = scope_bucket.get(call_type, 0) + 1


def record_cache_hit(layer: str = "canonical") -> None:
    """
    Increment cache-hit counter for a given layer.
    layer: "master" | "supplement" | "lkg" | "canonical"
    """
    with _lock:
        _cache_hits[layer] = _cache_hits.get(layer, 0) + 1


def record_cache_miss() -> None:
    """Increment cache-miss counter (ticker not found in any cache layer)."""
    global _cache_misses
    with _lock:
        _cache_misses += 1


def get_inflight_status() -> dict:
    """
    Return a diagnostics snapshot suitable for the /api/rate-status endpoint.

    Fields:
      total_inflight          — symbols currently in-flight
      by_scope                — {scope: [sym, ...]} breakdown
      total_claimed_lifetime  — total successful claims since process start
      total_blocked_lifetime  — total blocked/coalesced claims since start
      coalesced_lifetime      — subset of blocked: coalesced to existing scan
      provider_calls          — {expiry, chain, quote} lifetime call counts
      provider_calls_by_scope — {scope: {expiry, chain, quote}} breakdown
      cache_hits              — {master, supplement, lkg, canonical} hit counts
      cache_misses            — total cache misses (scan needed)
      uptime_seconds          — seconds since process start
    """
    with _lock:
        snap        = dict(_inflight)
        call_snap   = dict(_call_counts)
        scope_snap  = {s: dict(v) for s, v in _scope_call_counts.items()}
        hit_snap    = dict(_cache_hits)
        missed      = _cache_misses
        claimed     = _total_claimed_lifetime
        blocked     = _total_blocked_lifetime
        coalesced   = _coalesced_lifetime

    by_scope: dict[str, list[str]] = {}
    for sym, scope in snap.items():
        by_scope.setdefault(scope, []).append(sym)

    with _lock:
        fp_snap = dict(_fingerprints)

    return {
        "total_inflight":           len(snap),
        "by_scope":                 by_scope,
        "total_claimed_lifetime":   claimed,
        "total_blocked_lifetime":   blocked,
        "coalesced_lifetime":       coalesced,
        "provider_calls":           call_snap,
        "provider_calls_by_scope":  scope_snap,
        "cache_hits":               hit_snap,
        "cache_misses":             missed,
        "uptime_seconds":           round(_time.time() - _session_start, 1),
        "scan_fingerprints":        fp_snap,
    }
