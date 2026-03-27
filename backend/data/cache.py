"""
Simple in-memory TTL cache.
Stores API responses with expiration times.
Different data types get different TTLs based on how fast they change.
"""
import time
from typing import Any

try:
    from langsmith import traceable
except ImportError:
    def traceable(*args, **kwargs):
        def _noop(fn):
            return fn
        if args and callable(args[0]):
            return args[0]
        return _noop



class TTLCache:
    def __init__(self):
        self._store: dict[str, tuple[Any, float]] = {}

    @traceable(name="get")
    def get(self, key: str) -> Any | None:
        """Get a value if it exists and hasn't expired."""
        if key in self._store:
            value, expires_at = self._store[key]
            if time.time() < expires_at:
                return value
            else:
                del self._store[key]
        return None

    @traceable(name="set")
    def set(self, key: str, value: Any, ttl_seconds: int):
        """Store a value with a TTL in seconds."""
        self._store[key] = (value, time.time() + ttl_seconds)

    @traceable(name="clear")
    def clear(self):
        """Clear all cached values."""
        self._store.clear()

    @traceable(name="cleanup")
    def cleanup(self):
        """Remove expired entries."""
        now = time.time()
        expired = [k for k, (_, exp) in self._store.items() if now >= exp]
        for k in expired:
            del self._store[k]

    @property
    @traceable(name="size")
    def size(self):
        return len(self._store)


cache = TTLCache()

FINVIZ_TTL = 300
POLYGON_SNAPSHOT_TTL = 60
POLYGON_TECHNICALS_TTL = 300
POLYGON_DETAILS_TTL = 3600
STOCKTWITS_TTL = 120
STOCKANALYSIS_TTL = 900
FINNHUB_TTL = 600
ALPHAVANTAGE_TTL = 600
FMP_TTL = 300
FRED_TTL = 600
FEAR_GREED_TTL = 300
POLYGON_NEWS_TTL = 300
EARNINGS_TTL = 3600
MACRO_TTL = 600
SECTOR_ETF_TTL = 300
XAI_CROSS_ASSET_TTL = 180
XAI_THEMATIC_TTL = 3600
CANDLE_TTL = 900
REGIME_CANDLE_TTL = 600
EDGAR_CIK_TTL = 604800
EDGAR_FILINGS_TTL = 900
EDGAR_INSIDER_TTL = 1800
EDGAR_CATALYST_TTL = 900
TAVILY_TTL = 300
TAVILY_NEWS_TTL = 600
BRAVE_TTL = 300
BRAVE_NEWS_TTL = 600
BRIEFING_PRECOMPUTE_TTL = 1800  # 30 min for background briefing cache
