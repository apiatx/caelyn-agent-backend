"""
API call audit / telemetry — lightweight, zero-dependency, in-process.

Usage
─────
from services.api_audit import record_call, record_request, get_report

# Inside a provider HTTP wrapper:
record_call(
    provider="fmp",
    endpoint="earnings-calendar",
    page="calendar",
    feature="earnings",
    cache_status="miss",   # "hit" | "miss" | "lkg" | "none"
    http_status=200,
    elapsed_ms=312,
    ticker=None,           # optional
)

# After a full route handler finishes:
record_request(
    route="/api/catalysts/overview",
    page="calendar",
    feature="overview",
    provider_calls={"fmp": 12, "finnhub": 0},
    cache_hits=8,
    cache_misses=4,
    elapsed_ms=1820,
    http_status=200,
    extra={"tabs": 10},
)

# Debug endpoint:
GET /api/debug/provider-call-audit  →  get_report()
"""
from __future__ import annotations

import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Optional

# ── Known providers ───────────────────────────────────────────────────────────

PROVIDERS = ("fmp", "finnhub", "finviz", "polygon", "alpha_vantage",
             "twelvedata", "tradier", "edgar", "xai", "polymarket", "other")

# ── FMP_FORCE_429 simulation flag ─────────────────────────────────────────────
# Set FMP_FORCE_429=true in environment to simulate FMP 429 responses, OR call
# set_force_429(True) via the /api/debug/provider-call-audit?force_429=true endpoint.
# CatalystFMP._get and social_screener_service._fmp_get check this flag.

_runtime_force_429: Optional[bool] = None   # None = defer to env var


def fmp_force_429() -> bool:
    """Return True if FMP 429 simulation is active (env var OR runtime override)."""
    global _runtime_force_429
    if _runtime_force_429 is not None:
        return _runtime_force_429
    return os.getenv("FMP_FORCE_429", "").lower() in ("1", "true", "yes")


def set_force_429(value: bool) -> None:
    """Programmatically enable/disable FMP 429 simulation at runtime."""
    global _runtime_force_429
    _runtime_force_429 = value


# ── Internal stores ───────────────────────────────────────────────────────────

_lock = Lock()

# Total counters since server start
_totals: dict[str, int] = {p: 0 for p in PROVIDERS}
_total_cache_hits:   int = 0
_total_cache_misses: int = 0
_server_start: float = time.time()

# Recent individual call log (ring buffer, max 500 entries)
_MAX_RECENT = 500
_recent_calls: list[dict] = []

# Aggregated stats keyed by (provider, endpoint, page, feature)
# value: {count, cache_hits, cache_misses, lkg_hits, errors, total_elapsed_ms,
#         last_status, last_error, last_ts}
_agg: dict[tuple, dict] = defaultdict(lambda: {
    "count": 0, "cache_hits": 0, "cache_misses": 0, "lkg_hits": 0,
    "errors": 0, "total_elapsed_ms": 0.0,
    "last_status": None, "last_error": None, "last_ts": None,
})

# Request-level summaries (ring buffer, max 200)
_MAX_REQUESTS = 200
_recent_requests: list[dict] = []


# ── Public API ────────────────────────────────────────────────────────────────

def record_call(
    provider: str,
    endpoint: str,
    page: str,
    feature: str,
    cache_status: str,       # "hit" | "miss" | "lkg" | "none"
    http_status: Optional[int] = None,
    elapsed_ms: float = 0.0,
    ticker: Optional[str] = None,
    error: Optional[str] = None,
    success: bool = True,
) -> None:
    """Record one outbound provider HTTP call (or cache hit)."""
    ts = datetime.now(timezone.utc).isoformat()
    agg_key = (provider, endpoint, page, feature)

    with _lock:
        global _total_cache_hits, _total_cache_misses

        # Totals
        if provider in _totals:
            _totals[provider] += 1
        else:
            _totals["other"] += 1

        if cache_status == "hit":
            _total_cache_hits += 1
        elif cache_status in ("miss", "none"):
            _total_cache_misses += 1

        # Aggregated
        agg = _agg[agg_key]
        agg["count"] += 1
        if cache_status == "hit":
            agg["cache_hits"] += 1
        elif cache_status == "lkg":
            agg["lkg_hits"] += 1
        else:
            agg["cache_misses"] += 1
        if not success or (http_status and http_status >= 400):
            agg["errors"] += 1
        agg["total_elapsed_ms"] += elapsed_ms
        agg["last_status"] = http_status
        agg["last_error"] = error
        agg["last_ts"] = ts

        # Recent ring buffer
        entry = {
            "ts": ts, "provider": provider, "endpoint": endpoint,
            "page": page, "feature": feature,
            "cache_status": cache_status, "http_status": http_status,
            "elapsed_ms": round(elapsed_ms, 1), "ticker": ticker,
            "success": success, "error": error,
        }
        _recent_calls.append(entry)
        if len(_recent_calls) > _MAX_RECENT:
            _recent_calls.pop(0)


def record_request(
    route: str,
    page: str,
    feature: str,
    provider_calls: dict[str, int],
    cache_hits: int = 0,
    cache_misses: int = 0,
    elapsed_ms: float = 0.0,
    http_status: int = 200,
    extra: Optional[dict] = None,
) -> None:
    """Record one complete request summary and emit a [API_AUDIT] log line."""
    ts = datetime.now(timezone.utc).isoformat()
    summary = {
        "ts": ts, "route": route, "page": page, "feature": feature,
        "provider_calls": provider_calls,
        "cache_hits": cache_hits, "cache_misses": cache_misses,
        "elapsed_ms": round(elapsed_ms, 1),
        "http_status": http_status,
        **(extra or {}),
    }

    # Structured log line — grep-friendly
    calls_str = ",".join(f'{k}:{v}' for k, v in sorted(provider_calls.items()) if v)
    extra_str = " ".join(f'{k}={v}' for k, v in (extra or {}).items())
    print(
        f"[API_AUDIT] route={route} page={page} feature={feature} "
        f"provider_calls={{{calls_str}}} "
        f"cache_hits={cache_hits} cache_misses={cache_misses} "
        f"elapsed_ms={round(elapsed_ms,1)} status={http_status}"
        + (f" {extra_str}" if extra_str else "")
    )

    with _lock:
        _recent_requests.append(summary)
        if len(_recent_requests) > _MAX_REQUESTS:
            _recent_requests.pop(0)


def get_report() -> dict:
    """Return the full audit report for the debug endpoint."""
    with _lock:
        uptime_s = round(time.time() - _server_start)

        # Aggregate rows sorted by call count desc
        rows = []
        for (provider, endpoint, page, feature), agg in _agg.items():
            cnt = agg["count"]
            avg_ms = round(agg["total_elapsed_ms"] / cnt, 1) if cnt else 0.0
            rows.append({
                "provider":       provider,
                "endpoint":       endpoint,
                "page":           page,
                "feature":        feature,
                "count":          cnt,
                "cache_hits":     agg["cache_hits"],
                "cache_misses":   agg["cache_misses"],
                "lkg_hits":       agg["lkg_hits"],
                "errors":         agg["errors"],
                "avg_elapsed_ms": avg_ms,
                "last_status":    agg["last_status"],
                "last_error":     agg["last_error"],
                "last_ts":        agg["last_ts"],
            })
        rows.sort(key=lambda r: -r["count"])

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "uptime_seconds": uptime_s,
            "fmp_force_429_active": fmp_force_429(),
            "totals_since_start": dict(_totals),
            "total_cache_hits":   _total_cache_hits,
            "total_cache_misses": _total_cache_misses,
            "aggregated_by_provider_endpoint": rows,
            "recent_requests":    list(_recent_requests[-50:]),
            "recent_calls":       list(_recent_calls[-100:]),
        }


def get_total_calls(provider: str) -> int:
    """Return total call count for a provider since server start (snapshot)."""
    with _lock:
        return _totals.get(provider, 0)


def get_cache_counts() -> tuple[int, int]:
    """Return (total_cache_hits, total_cache_misses) snapshot."""
    with _lock:
        return _total_cache_hits, _total_cache_misses


def reset_stats() -> None:
    """Clear all counters. Useful for tests."""
    with _lock:
        global _total_cache_hits, _total_cache_misses
        for p in PROVIDERS:
            _totals[p] = 0
        _total_cache_hits = 0
        _total_cache_misses = 0
        _recent_calls.clear()
        _recent_requests.clear()
        _agg.clear()
