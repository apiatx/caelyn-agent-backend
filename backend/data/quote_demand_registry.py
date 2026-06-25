"""
quote_demand_registry.py — Active quote-demand tracking for visible-page priority.

Phase 4A: Records which symbols are actively viewed so realtime_quotes_service
can sort cache-miss symbols by priority before batching Tradier requests.

Rules:
  • register() is a cheap synchronous side-effect — no I/O, no Tradier calls.
  • Expiry is lazy: stale entries are pruned on every register() call (O(n) scan).
  • Multi-scope merging: same symbol from two pages → scopes merged, priority boosted.
  • Diagnostics are written by realtime_quotes_service after each batch.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field

# ── Priority table by scope ───────────────────────────────────────────────────
_SCOPE_BASE: dict[str, float] = {
    "popup":        0.95,
    "watchlist":    0.90,
    "portfolio":    0.90,
    "options_flow": 0.85,
    "screener":     0.85,
    "social":       0.80,
    "themes":       0.80,
    "sectors":      0.75,
    "strategy":     0.75,
    "page":         0.80,
}
_MULTI_SCOPE_BOOST = 0.05   # per additional scope beyond the first
_PRIORITY_CAP      = 1.0

_DEFAULT_TTL_PAGE  = 90     # seconds — active page demand
_DEFAULT_TTL_POPUP = 30     # seconds — single-symbol popup demand

# ── Registry state (module-level singleton) ───────────────────────────────────
@dataclass
class _DemandEntry:
    symbol:        str
    scopes:        set[str] = field(default_factory=set)
    priority:      float    = 0.0
    expires_at:    float    = 0.0   # monotonic seconds
    registered_at: float    = 0.0

_registry:      dict[str, _DemandEntry] = {}
_expired_count: int = 0     # lifetime entries that expired without being refreshed
_merged_count:  int = 0     # lifetime multi-scope merge events

# ── Diagnostics snapshot (written by realtime_quotes_service) ─────────────────
_last_queue_depth:    int        = 0
_last_refresh_order:  list[str]  = []
_last_cache_hits:     int        = 0
_last_live_fetches:   int        = 0


# ── Internal helpers ──────────────────────────────────────────────────────────

def _recalc_priority(entry: _DemandEntry) -> float:
    """Max scope base + multi-scope boost, capped at 1.0."""
    if not entry.scopes:
        return 0.0
    base  = max(_SCOPE_BASE.get(s, 0.70) for s in entry.scopes)
    boost = _MULTI_SCOPE_BOOST * (len(entry.scopes) - 1)
    return min(base + boost, _PRIORITY_CAP)


def _prune_expired() -> None:
    """Remove expired entries; increment lifetime counter."""
    global _expired_count
    now  = time.monotonic()
    dead = [sym for sym, e in _registry.items() if e.expires_at <= now]
    for sym in dead:
        del _registry[sym]
        _expired_count += 1


# ── Public API ────────────────────────────────────────────────────────────────

def register(symbols: list[str], scope: str, ttl: int) -> None:
    """
    Upsert all symbols for a scope.

    • Creates a new entry if the symbol has no active demand.
    • If already registered: merges scope, extends TTL, recalculates priority.
    • Prunes expired entries on every call (lazy GC — no background task).
    """
    global _merged_count
    _prune_expired()
    now     = time.monotonic()
    expires = now + ttl

    for sym in symbols:
        if not sym or not isinstance(sym, str):
            continue
        sym = sym.upper()
        entry = _registry.get(sym)
        if entry is None:
            entry = _DemandEntry(
                symbol=sym,
                scopes={scope},
                registered_at=now,
                expires_at=expires,
            )
            entry.priority = _recalc_priority(entry)
            _registry[sym] = entry
        else:
            if scope not in entry.scopes:
                entry.scopes.add(scope)
                _merged_count += 1
            entry.expires_at = max(entry.expires_at, expires)   # extend, never shrink
            entry.priority   = _recalc_priority(entry)


def get_priority(symbol: str, default: float = 0.0) -> float:
    """Return current demand priority; returns *default* if absent or expired."""
    entry = _registry.get(symbol)
    if entry is None:
        return default
    if entry.expires_at <= time.monotonic():
        return default
    return entry.priority


def active_symbols() -> list[str]:
    """All non-expired symbols sorted by priority desc."""
    now   = time.monotonic()
    alive = [(sym, e) for sym, e in _registry.items() if e.expires_at > now]
    alive.sort(key=lambda t: -t[1].priority)
    return [sym for sym, _ in alive]


def record_refresh_stats(
    *,
    queue_depth: int,
    order_sample: list[str],
    cache_hits: int,
    live_fetches: int,
) -> None:
    """Called by realtime_quotes_service after each Tradier batch to update diagnostics."""
    global _last_queue_depth, _last_refresh_order, _last_cache_hits, _last_live_fetches
    _last_queue_depth   = queue_depth
    _last_refresh_order = list(order_sample[:10])
    _last_cache_hits    = cache_hits
    _last_live_fetches  = live_fetches


def diagnostics() -> dict:
    """Return a snapshot dict for /api/rate-status."""
    _prune_expired()
    now   = time.monotonic()
    alive = {sym: e for sym, e in _registry.items() if e.expires_at > now}

    by_scope: dict[str, list[str]] = {}
    for sym, e in alive.items():
        for s in e.scopes:
            by_scope.setdefault(s, []).append(sym)

    top = sorted(alive.values(), key=lambda e: -e.priority)[:10]

    return {
        "active_quote_demand_count":      len(alive),
        "active_quote_symbols_by_scope":  {s: sorted(syms) for s, syms in sorted(by_scope.items())},
        "top_active_quote_symbols": [
            {
                "symbol":   e.symbol,
                "priority": round(e.priority, 3),
                "scopes":   sorted(e.scopes),
            }
            for e in top
        ],
        "active_quote_queue_depth":       _last_queue_depth,
        "active_quote_expired_count":     _expired_count,
        "duplicate_quote_demands_merged": _merged_count,
        "quote_refresh_order_sample":     _last_refresh_order,
        "active_quote_hits_from_cache":   _last_cache_hits,
        "active_quote_live_fetches":      _last_live_fetches,
    }
