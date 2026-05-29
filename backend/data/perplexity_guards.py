"""
Centralized Perplexity safety guards and diagnostic counters.

Three flags (all default False — Perplexity disabled by default):
  PERPLEXITY_BACKGROUND_ENABLED  — background loops, startup jobs, 24h refresh cycles
  PERPLEXITY_PAGELOAD_ENABLED    — per-page-load enrichment (e.g. Pre-IPO Watchlist)
  PERPLEXITY_FALLBACK_ENABLED    — silent fallback paths (e.g. watchlist ticker news)

Manual AI Terminal / agent-collab paths are NOT governed here — they are user-triggered
and handled by the existing caelyn_routing + api_budget systems.

Usage:
    from data.perplexity_guards import pplx_background_allowed, pplx_blocked
    if not pplx_background_allowed():
        pplx_blocked("background", "my_function:my_call_site")
        return default_value
"""
import os
import time
from threading import Lock


_lock = Lock()


def _bool_env(name: str) -> bool:
    return os.getenv(name, "false").lower() in ("true", "1", "yes")


BACKGROUND_ENABLED = _bool_env("PERPLEXITY_BACKGROUND_ENABLED")
PAGELOAD_ENABLED   = _bool_env("PERPLEXITY_PAGELOAD_ENABLED")
FALLBACK_ENABLED   = _bool_env("PERPLEXITY_FALLBACK_ENABLED")


_counters: dict[str, int] = {
    "background_blocked": 0,
    "pageload_blocked":   0,
    "fallback_blocked":   0,
    "total_allowed":      0,
}
_last_blocked: dict = {"site": None, "kind": None, "at": None}
_last_allowed: dict = {"site": None, "kind": None, "at": None}


def pplx_background_allowed() -> bool:
    return BACKGROUND_ENABLED


def pplx_pageload_allowed() -> bool:
    return PAGELOAD_ENABLED


def pplx_fallback_allowed() -> bool:
    return FALLBACK_ENABLED


def pplx_blocked(kind: str, call_site: str) -> None:
    """Record a blocked Perplexity call attempt."""
    with _lock:
        key = f"{kind}_blocked"
        if key in _counters:
            _counters[key] += 1
        _last_blocked["site"] = call_site
        _last_blocked["kind"] = kind
        _last_blocked["at"]   = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print(f"[PPLX_GUARD] blocked kind={kind} site={call_site}")


def pplx_allowed(kind: str, call_site: str) -> None:
    """Record an allowed (not blocked) Perplexity call."""
    with _lock:
        _counters["total_allowed"] += 1
        _last_allowed["site"] = call_site
        _last_allowed["kind"] = kind
        _last_allowed["at"]   = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def guard_diagnostics() -> dict:
    """Return current guard state and counters for the diagnostic endpoint."""
    with _lock:
        return {
            "flags": {
                "PERPLEXITY_BACKGROUND_ENABLED": BACKGROUND_ENABLED,
                "PERPLEXITY_PAGELOAD_ENABLED":   PAGELOAD_ENABLED,
                "PERPLEXITY_FALLBACK_ENABLED":   FALLBACK_ENABLED,
            },
            "blocked_since_boot": {
                "background": _counters["background_blocked"],
                "pageload":   _counters["pageload_blocked"],
                "fallback":   _counters["fallback_blocked"],
                "total":      sum(v for k, v in _counters.items() if k.endswith("_blocked")),
            },
            "allowed_since_boot": _counters["total_allowed"],
            "last_blocked_call":  dict(_last_blocked),
            "last_allowed_call":  dict(_last_allowed),
        }
