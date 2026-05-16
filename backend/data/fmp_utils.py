"""
Shared FMP utilities — single source of truth for cross-service concerns.

Import from here rather than defining per-service copies:

    from data.fmp_utils import fmp_hist_ttl

Cache key convention (cross-service, Tradier-aligned):
    fmp:{endpoint}:{SYMBOL}       ← hot TTL cache  (service-neutral)
    fmp:lkg:{endpoint}:{SYMBOL}   ← last-known-good (7-day fallback)

Any service that reads or writes a FMP {endpoint}/{symbol} pair MUST use
these keys so results are shared across pages, just as Tradier uses the
flat  tradier:quote:sym:{SYM}  key shared by all services.
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta


def fmp_hist_ttl() -> int:
    """Global TTL rule for FMP 7D/30D/YTD/1Y price-change data.

    Weekend (Sat/Sun): cache until Monday 09:30 ET — zero weekend FMP calls.
                       A single Friday-EOD fetch carries all the way to open.
    Weekday:           3600 s (60 min flat) — data moves at EOD only; no
                       intraday value in refreshing faster than once per hour.

    1D% is served from Tradier in near-real-time and is NOT subject to this
    rule.  Only apply this function to FMP 7D+ historical price-change calls.

    Usage:
        ttl = fmp_hist_ttl()
        cache.set(cache_key, data, ttl)
    """
    try:
        now = datetime.now(tz=timezone.utc)
        wd  = now.weekday()             # 0=Mon … 6=Sun
        if wd >= 5:                     # Saturday or Sunday
            # Monday 09:30 ET: EDT offset = 4h (Apr–Oct), EST = 5h (Nov–Mar)
            utc_off = 4 if 4 <= now.month <= 10 else 5
            days_to_monday = 7 - wd    # Sat → 2, Sun → 1
            monday_open_utc = (now + timedelta(days=days_to_monday)).replace(
                hour=9 + utc_off, minute=30, second=0, microsecond=0,
            )
            secs = int((monday_open_utc - now).total_seconds())
            return max(secs, 3600)     # floor at 1h against clock-skew edge
        return 3600                    # weekday — 60 min flat
    except Exception:
        return 3600


def fmp_cache_key(endpoint: str, symbol: str) -> str:
    """Canonical hot-cache key for a FMP endpoint + symbol pair.

    All services must use this function (or the string pattern it produces)
    so that the same ticker requested by different pages shares one cache
    entry — identical to how Tradier uses tradier:quote:sym:{SYM}.

    Example: fmp_cache_key("stock-price-change", "AAPL")
             → "fmp:stock-price-change:AAPL"
    """
    return f"fmp:{endpoint}:{symbol.upper()}"


def fmp_lkg_key(endpoint: str, symbol: str) -> str:
    """Canonical last-known-good key for a FMP endpoint + symbol pair."""
    return f"fmp:lkg:{endpoint}:{symbol.upper()}"
