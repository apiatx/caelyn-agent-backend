"""
canonical_history_service.py — Disk-persistent canonical 10-year price history cache.
======================================================================================
V4.2.5.4 — Zero provider calls in this module.  Pure disk read/write.

Target: up to 10 years of daily candles (~2520 trading bars) per symbol.
Symbols with genuine IPO/listing < 10 years ago are classified
actual_ticker_history_limit — not a provider failure.

Storage layout
--------------
  backend/data/canonical_history/
    _index.json            — lightweight metadata per symbol (no bars)
    {SYM}.json.gz          — gzipped full OHLCV bars + metadata per symbol

Lifecycle
---------
  preload_index()          — startup: reads _index.json into _INDEX, no fetches
  get_bars(symbol)         — read path; available_10y always returned even if stale
  save_bars(...)           — write path; called by backfill job only
  append_bars(...)         — incremental daily merge; called by nightly job only
  is_fresh(symbol)         — staleness check (stale = needs append, not unusable)
  needs_append(symbol)     — True when newest_bar_date is > 2 trading days old

History status values
---------------------
  available_10y            >= 2200 bars (~8.7 years, "10Y complete")
  available_10y_fresh      available_10y + newest bar within 3 calendar days
  available_10y_needs_append  available_10y + newest bar > 3 days old
  available_10y_stale_but_usable  available_10y + newest bar > 10 days old
  available_5y_partial_long_history  >= 1100 bars (~4.4 years)
  available_3y_partial_history    >= 700 bars
  partial_history          >= 504 bars (~2 years)
  intermediate_only        >= 252 bars (~1 year)
  recent_only              >= 40 bars
  actual_ticker_history_limit  — genuinely new ticker (not a data gap)
  insufficient_history     < 40 bars
  fetch_failed             provider returned error
  not_yet_backfilled       no attempt yet
  excluded_prefixed_symbol contains ":" or otherwise ineligible
  cache_corrupt_needs_rebuild  corrupt/unreadable gz file

TTL semantics (V4.2.5.4)
------------------------
  available_10y*            NEVER expires → stale_but_usable; only triggers append check.
                            get_bars() always returns bars for these statuses.
  available_5y_partial_long_history   48h → append check
  available_3y_partial_history        24h → append check
  partial_history                     12h → full refetch eligible
  intermediate_only                    6h
  recent_only                          6h
  actual_ticker_history_limit         96h → existence check only
  fetch_failed                         4h
  not_yet_backfilled                   0h
  excluded_prefixed_symbol           999h

Depth confidence (V4.2.5.4)
---------------------------
  ~10y / 2200+ bars  = 1.00
  ~5y  / 1100-2199   = 0.85
  ~3y  / 756-1099    = 0.75
  ~2y  / 504-755     = 0.65
  ~1y  / 252-503     = 0.50
  <252 bars          = 0.25 (unless actual_ticker_history_limit)
  actual_ticker_limit → linear from 0.25 to 0.85 based on months available
"""
from __future__ import annotations

import gzip
import json
import os
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

_CANON_DIR   = Path(__file__).parent.parent / "data" / "canonical_history"
_INDEX_FILE  = _CANON_DIR / "_index.json"
_INDEX: dict[str, dict] = {}   # in-memory metadata; no bars stored here

# 10Y target bar count
_TARGET_BAR_COUNT   = 2520   # ~10 trading years
_10Y_MIN_BARS       = 2200   # ≥ this → available_10y
_5Y_MIN_BARS        = 1100   # ≥ this → available_5y_partial_long_history
_3Y_MIN_BARS        = 700
_2Y_MIN_BARS        = 504
_1Y_MIN_BARS        = 252
_RECENT_MIN_BARS    = 40


# ── Status classification ─────────────────────────────────────────────────────

def classify_history_status(bar_count: int, is_actual_limit: bool = False) -> str:
    """
    Classify history completeness from bar_count.

    Note: available_10y_fresh / available_10y_needs_append / available_10y_stale_but_usable
    are append-freshness sub-states of available_10y.  Use classify_append_freshness()
    to compute those — they require newest_bar_date knowledge.
    """
    if is_actual_limit:
        return "actual_ticker_history_limit"
    if bar_count >= _10Y_MIN_BARS:
        return "available_10y"
    if bar_count >= _5Y_MIN_BARS:
        return "available_5y_partial_long_history"
    if bar_count >= _3Y_MIN_BARS:
        return "available_3y_partial_history"
    if bar_count >= _2Y_MIN_BARS:
        return "partial_history"
    if bar_count >= _1Y_MIN_BARS:
        return "intermediate_only"
    if bar_count >= _RECENT_MIN_BARS:
        return "recent_only"
    return "insufficient_history"


def classify_append_freshness(
    base_status: str,
    newest_bar_date: Optional[str],
) -> str:
    """
    Refine available_10y into append-freshness sub-states.
    Returns base_status unchanged for non-10Y statuses.
    """
    if not base_status.startswith("available_10y"):
        return base_status
    if not newest_bar_date:
        return "available_10y_stale_but_usable"
    try:
        newest = datetime.strptime(newest_bar_date[:10], "%Y-%m-%d").date()
        age_d  = (date.today() - newest).days
        if age_d <= 3:
            return "available_10y_fresh"
        if age_d <= 10:
            return "available_10y_needs_append"
        return "available_10y_stale_but_usable"
    except Exception:
        return "available_10y_stale_but_usable"


def is_10y_complete(status: str) -> bool:
    """True for any available_10y* variant."""
    return status.startswith("available_10y") or status == "available_10y"


def depth_confidence(bar_count: int, is_actual_limit: bool = False) -> float:
    """
    Data depth confidence (0.0–1.0).

    Actual-ticker-history-limit tickers are not penalised for genuinely
    having less history — confidence scales linearly to their real depth.
    """
    if is_actual_limit:
        # Linear 0.25→0.85 over 0→252 bars (approx 1 year)
        return round(min(0.85, 0.25 + (bar_count / 252) * 0.60), 2) if bar_count > 0 else 0.25
    if bar_count >= _10Y_MIN_BARS:
        return 1.00
    if bar_count >= _5Y_MIN_BARS:
        return 0.85
    if bar_count >= _3Y_MIN_BARS:
        return 0.75
    if bar_count >= _2Y_MIN_BARS:
        return 0.65
    if bar_count >= _1Y_MIN_BARS:
        return 0.50
    return 0.25


def fib_timeframe_scope(
    bar_count: int,
    weekly_bar_count: int = 0,
    monthly_bar_count: int = 0,
) -> str:
    """
    fib_timeframe_scope — which Fib candidate classes are meaningful given bar depth.

    full_10y       weekly + monthly Fib contexts available  (bars >= 2200)
    multi_year     weekly + monthly Fib contexts available  (bars >= 756)
    long           long daily + weekly possible             (bars >= 504)
    intermediate   intermediate daily candidates            (bars >= 252)
    recent         recent daily only                        (bars >= 40)
    insufficient   too few bars for any Fib work            (bars < 40)
    actual_ticker_history_limited  — new ticker, not a data failure
    """
    if bar_count >= _10Y_MIN_BARS:
        return "full_10y"
    if bar_count >= 756 and (weekly_bar_count >= 26 or weekly_bar_count == 0):
        return "multi_year"
    if bar_count >= _2Y_MIN_BARS:
        return "long"
    if bar_count >= _1Y_MIN_BARS:
        return "intermediate"
    if bar_count >= _RECENT_MIN_BARS:
        return "recent"
    return "insufficient"


def depth_limitation_reason(
    bar_count: int,
    history_source: str,
    is_actual_limit: bool = False,
) -> Optional[str]:
    if is_actual_limit:
        return "actual_ticker_history_limit"
    if bar_count >= _10Y_MIN_BARS:
        return None
    if bar_count >= _5Y_MIN_BARS:
        return "below_10y_target_partial_long"
    if bar_count >= _3Y_MIN_BARS:
        return "below_5y_target"
    if bar_count >= _2Y_MIN_BARS:
        return "partial_2_3y_range"
    if bar_count >= _1Y_MIN_BARS:
        return "intermediate_only_1y"
    if bar_count >= _RECENT_MIN_BARS:
        return f"recent_only_{history_source}"
    return "insufficient_bars"


# ── Internal helpers ──────────────────────────────────────────────────────────

def _stale_hours(status: str) -> float:
    """
    Hours until stale. For available_10y* statuses, staleness means "needs append"
    only — bars are always returned.  Use a very long window so the append check
    fires approximately once per day.
    """
    return {
        # available_10y* → very long TTL; stale = append check only
        "available_10y":                        48.0,
        "available_10y_fresh":                  48.0,
        "available_10y_needs_append":           48.0,
        "available_10y_stale_but_usable":       48.0,
        # partial long history → append check
        "available_5y_partial_long_history":    36.0,
        "available_3y_partial_history":         24.0,
        # shorter histories → full refetch eligible when stale
        "partial_history":                      12.0,
        "intermediate_only":                     6.0,
        "recent_only":                           6.0,
        # special
        "actual_ticker_history_limit":          96.0,
        "insufficient_history":                  4.0,
        "fetch_failed":                          4.0,
        "not_yet_backfilled":                    0.0,
        "excluded_prefixed_symbol":            999.0,
        "cache_corrupt_needs_rebuild":           0.0,
    }.get(status, 12.0)


def _now_ts() -> str:
    return datetime.now(timezone.utc).isoformat()


def _age_hours(fetched_at: str) -> float:
    try:
        dt = datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0
    except Exception:
        return 9999.0


def _age_days_since_newest_bar(newest_bar_date: Optional[str]) -> float:
    if not newest_bar_date:
        return 9999.0
    try:
        newest = datetime.strptime(newest_bar_date[:10], "%Y-%m-%d").date()
        return float((date.today() - newest).days)
    except Exception:
        return 9999.0


def _ensure_dir() -> None:
    _CANON_DIR.mkdir(parents=True, exist_ok=True)


def _bar_file(symbol: str) -> Path:
    return _CANON_DIR / f"{symbol.upper()}.json.gz"


def _write_index() -> None:
    """
    Write index atomically.  Always MERGES with existing disk content so that
    concurrent processes do not clobber each other's entries.
    In-memory _INDEX wins on symbol-level conflicts.
    """
    try:
        _ensure_dir()
        disk_index: dict = {}
        if _INDEX_FILE.exists():
            try:
                disk = json.loads(_INDEX_FILE.read_text())
                disk_index = {k.upper(): v for k, v in disk.get("symbols", {}).items()}
            except Exception:
                pass
        merged = {**disk_index, **_INDEX}
        tmp = _INDEX_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps({"updated_at": _now_ts(), "symbols": merged}, indent=2))
        tmp.replace(_INDEX_FILE)
    except Exception as exc:
        print(f"[CANON_HIST] index write error (non-fatal): {exc}")


# ── Public API ────────────────────────────────────────────────────────────────

def _rebuild_index_from_gz() -> int:
    added = 0
    try:
        _ensure_dir()
        for gz_path in sorted(_CANON_DIR.glob("*.json.gz")):
            sym = gz_path.name.split(".")[0].upper()
            if sym in _INDEX:
                continue
            try:
                with gzip.open(str(gz_path), "rt", encoding="utf-8") as fh:
                    payload = json.loads(fh.read())
                meta = {k: v for k, v in payload.items() if k != "bars"}
                if meta.get("symbol") and meta.get("bar_count") is not None:
                    _INDEX[sym] = meta
                    added += 1
            except Exception:
                pass
    except Exception as exc:
        print(f"[CANON_HIST] rebuild scan error (non-fatal): {exc}")
    return added


def preload_index() -> None:
    """
    Load _index.json into in-memory _INDEX.
    Called at server startup — reads disk only, no provider calls.
    Also scans for gz files not present in the index (e.g. after index clobber).
    """
    global _INDEX
    try:
        _ensure_dir()
        if _INDEX_FILE.exists():
            data = json.loads(_INDEX_FILE.read_text())
            _INDEX.clear()
            _INDEX.update({
                k.upper(): v
                for k, v in data.get("symbols", {}).items()
                if "." not in k and k.upper() == k.upper().strip()
            })
        orphaned = [s for s in list(_INDEX) if not _bar_file(s).exists()]
        for s in orphaned:
            del _INDEX[s]
        recovered = _rebuild_index_from_gz()
        if recovered or orphaned:
            _write_index()
            if recovered:
                print(f"[CANON_HIST] recovered {recovered} symbols from gz files")
            if orphaned:
                print(f"[CANON_HIST] pruned {len(orphaned)} orphaned index entries")
        fresh_10y = sum(1 for v in _INDEX.values()
                        if is_10y_complete(v.get("history_status", "")))
        fresh_5y  = sum(1 for v in _INDEX.values()
                        if v.get("history_status") == "available_5y_partial_long_history")
        print(
            f"[CANON_HIST] index loaded: {len(_INDEX)} symbols, "
            f"{fresh_10y} available_10y, {fresh_5y} available_5y_partial"
        )
        if not _INDEX:
            print("[CANON_HIST] no index on disk — will be created on first backfill")
    except Exception as exc:
        print(f"[CANON_HIST] index load error (non-fatal): {exc}")


def get_metadata(symbol: str) -> Optional[dict]:
    """Return index entry for *symbol* (no bars)."""
    return _INDEX.get(symbol.upper())


def get_all_status() -> dict:
    """Return full in-memory index (no bars)."""
    return dict(_INDEX)


def is_fresh(symbol: str, max_age_h: Optional[float] = None) -> bool:
    """
    Return True when the canonical cache for *symbol* is within its freshness window.

    For available_10y* symbols, staleness only means "needs append check" —
    the bars are always usable.  is_fresh() returning False for these symbols
    should trigger an incremental append, not a full refetch.
    """
    meta = _INDEX.get(symbol.upper())
    if not meta:
        return False
    status = meta.get("history_status", "not_yet_backfilled")
    if status in ("not_yet_backfilled", "excluded_prefixed_symbol"):
        return False
    threshold = max_age_h if max_age_h is not None else _stale_hours(status)
    return _age_hours(meta.get("fetched_at", "")) <= threshold


def needs_append(symbol: str, max_bar_age_days: float = 3.0) -> bool:
    """
    True when the newest cached bar is older than max_bar_age_days.
    Used to schedule incremental appends without triggering full refetch.
    """
    meta = _INDEX.get(symbol.upper())
    if not meta:
        return False
    status = meta.get("history_status", "")
    if not status or status in ("not_yet_backfilled", "fetch_failed",
                                "excluded_prefixed_symbol", "insufficient_history"):
        return False
    return _age_days_since_newest_bar(meta.get("newest_bar_date")) > max_bar_age_days


def is_stale_but_usable(symbol: str) -> bool:
    """
    True for available_10y* or actual_ticker_history_limit symbols where the
    cache is stale but bars are still valid for analysis.
    """
    meta = _INDEX.get(symbol.upper())
    if not meta:
        return False
    status = meta.get("history_status", "")
    return is_10y_complete(status) or status == "actual_ticker_history_limit"


def get_bars(symbol: str, require_fresh: bool = True) -> Optional[dict]:
    """
    Return canonical history payload dict (includes 'bars' key) or None.

    V4.2.5.4 TTL semantics:
      - available_10y* symbols: bars are ALWAYS returned regardless of staleness.
        Stale only means an incremental append is due.
      - actual_ticker_history_limit: same — always return bars.
      - Other statuses: staleness means cache is invalid; returns None when stale
        (caller falls back to 400-bar Tradier and enqueues backfill).

    Use require_fresh=False to read stale bars as last resort for any symbol.

    Lazy-loads the index from disk on first call if preload_index() was not yet called.
    """
    sym = symbol.upper()
    if not _INDEX:
        try:
            preload_index()
        except Exception:
            pass

    meta = _INDEX.get(sym)
    if meta:
        status = meta.get("history_status", "")
        # available_10y* and actual_ticker_history_limit are always usable
        if require_fresh and not is_10y_complete(status) and status != "actual_ticker_history_limit":
            if not is_fresh(sym):
                return None
    elif require_fresh:
        return None

    f = _bar_file(sym)
    if not f.exists():
        return None
    try:
        with gzip.open(str(f), "rt", encoding="utf-8") as fh:
            payload = json.loads(fh.read())
        bars = payload.get("bars") or []
        if not bars:
            # Flag as corrupt so rebuild can be triggered
            if meta and meta.get("history_status", "") not in (
                "fetch_failed", "not_yet_backfilled"
            ):
                _INDEX[sym] = {**meta, "history_status": "cache_corrupt_needs_rebuild"}
            return None
        return payload
    except Exception as exc:
        print(f"[CANON_HIST] read error {sym}: {exc}")
        if meta:
            _INDEX[sym] = {**meta, "history_status": "cache_corrupt_needs_rebuild"}
        return None


def _compute_quality(
    bar_count: int,
    provider: str,
    is_actual_limit: bool,
    tradier_capability: Optional[str] = None,
) -> str:
    """
    canonical_history_quality — describes the confidence and provenance of bars.

    full_10y_tradier_verified    Tradier >= 2200 bars, capability confirmed
    full_10y_tradier_unverified  Tradier >= 2200 bars, not yet cross-verified
    full_10y_fmp                 FMP >= 2200 bars
    available_5y_partial_long_history  1100-2199 bars
    available_3y_partial_history       700-1099 bars
    actual_ticker_history_limit        Genuinely new ticker (not a data gap)
    partial_tradier                    Tradier < 700 bars
    partial_fmp                        FMP < 700 bars
    stage_cache_fallback               Served from 400-bar stage cache (last resort)
    provider_failed                    All providers returned empty
    not_yet_backfilled                 No attempt made
    """
    if is_actual_limit:
        return "actual_ticker_history_limit"
    if bar_count == 0:
        return "provider_failed"
    if provider == "tradier":
        if bar_count >= _10Y_MIN_BARS:
            return ("full_10y_tradier_verified"
                    if tradier_capability == "TRADIER_FULL_HISTORY_OK"
                    else "full_10y_tradier_unverified")
        if bar_count >= _5Y_MIN_BARS:
            return "available_5y_partial_long_history"
        if bar_count >= _3Y_MIN_BARS:
            return "available_3y_partial_history"
        return "partial_tradier"
    if provider == "fmp":
        if bar_count >= _10Y_MIN_BARS:
            return "full_10y_fmp"
        if bar_count >= _5Y_MIN_BARS:
            return "available_5y_partial_long_history"
        if bar_count >= _3Y_MIN_BARS:
            return "available_3y_partial_history"
        return "partial_fmp"
    if provider == "stage_cache":
        return "stage_cache_fallback"
    return "full_10y_tradier_unverified"


def _provider_rank(provider: str) -> int:
    return {"tradier": 1, "fmp": 2, "stage_cache": 4}.get(provider, 3)


def save_bars(
    symbol:             str,
    bars:               list[dict],
    provider:           str,
    is_actual_limit:    bool          = False,
    error_reason:       Optional[str] = None,
    refresh_mode:       str           = "initial_full_backfill",
    tradier_capability: Optional[str] = None,
) -> dict:
    """
    Persist canonical history to disk and update _INDEX.
    Returns the metadata entry (without bars).
    Called by the backfill job only — never at request time.

    refresh_mode values:
      initial_full_backfill       first-ever 10Y fetch
      incremental_daily_append    appended new bars to existing history
      manual_rebuild              forced full re-fetch (requires confirm=true)
      cache_read_only             no provider call made (should not reach save_bars)
      weekly_health_check         metadata check only (no bars change expected)
      monthly_full_refresh        monthly re-fetch (admin only, confirm required)
    """
    sym = symbol.upper()
    _ensure_dir()

    bar_count = len(bars)
    if bars:
        dates   = sorted(str(b.get("date", ""))[:10] for b in bars if b.get("date"))
        oldest  = dates[0]  if dates else None
        newest  = dates[-1] if dates else None
    else:
        oldest = newest = None

    years_avail = round(bar_count / 252, 1) if bar_count > 0 else 0.0
    base_status = (
        "fetch_failed" if error_reason
        else classify_history_status(bar_count, is_actual_limit)
    )
    # Refine to append-freshness sub-state for 10Y symbols
    status   = classify_append_freshness(base_status, newest)
    dep_conf = depth_confidence(bar_count, is_actual_limit)
    stale_h  = _stale_hours(status)
    now      = _now_ts()
    stale_after = (
        (datetime.now(timezone.utc) + timedelta(hours=stale_h)).isoformat()
        if stale_h > 0 else None
    )
    wk_approx = round(bar_count / 5)
    mo_approx = round(bar_count / 21)

    # Adjustment status — Tradier/FMP do not expose this explicitly.
    # Conservative posture: always "unknown".
    adjusted_status = "unknown"

    meta: dict = {
        "symbol":                         sym,
        "provider":                       provider,
        "canonical_history_provider":     provider,
        "canonical_history_provider_rank": _provider_rank(provider),
        "canonical_history_quality":      _compute_quality(
                                              bar_count, provider,
                                              is_actual_limit, tradier_capability),
        "canonical_history_adjusted_status": adjusted_status,
        "canonical_history_refresh_mode": refresh_mode,
        "bar_count":                      bar_count,
        "oldest_bar_date":                oldest,
        "newest_bar_date":                newest,
        "years_available":                years_avail,
        "fetched_at":                     now,
        "stale_after":                    stale_after,
        "history_status":                 status,
        "base_history_status":            base_status,
        "error_reason":                   error_reason,
        "source_priority":                _provider_rank(provider),
        "is_actual_limit":                is_actual_limit,
        "depth_confidence":               dep_conf,
        "fib_scope":                      fib_timeframe_scope(bar_count, wk_approx, mo_approx),
        "last_attempt_at":                now,
        "next_retry_at":                  stale_after,
        # Computed convenience flags
        "cache_usable":                   bar_count > 0,
        "needs_append":                   _age_days_since_newest_bar(newest) > 3.0 if newest else False,
        "is_10y_complete":                bar_count >= _10Y_MIN_BARS,
    }

    if bars:
        payload = {**meta, "bars": bars}
        f = _bar_file(sym)
        try:
            with gzip.open(str(f), "wt", encoding="utf-8", compresslevel=6) as fh:
                fh.write(json.dumps(payload, separators=(",", ":")))
        except Exception as exc:
            print(f"[CANON_HIST] write error {sym}: {exc}")

    _INDEX[sym] = meta
    _write_index()
    return meta


def append_bars(
    symbol:   str,
    new_bars: list[dict],
    provider: str,
) -> Optional[dict]:
    """
    Incremental daily append — merge *new_bars* into the existing cached bars.

    Always 1 provider call per symbol in the calling job:
      start_date = newest_bar_date - 2 days (2-day overlap for safety)
      This 2-day overlap is within the SAME single HTTP request.

    Strategy:
      1. Load existing bars from disk (stale OK for append).
      2. Merge by date — new_bars overwrite on date collision.
      3. Truncate to last 3650 days to prevent unbounded growth.
      4. Save the merged set via save_bars() with refresh_mode='incremental_daily_append'.

    Returns updated metadata, or None if the existing cache is missing.
    """
    sym = symbol.upper()
    existing = get_bars(sym, require_fresh=False)
    if not existing:
        return None
    old_bars: list[dict] = existing.get("bars") or []

    by_date: dict[str, dict] = {
        str(b.get("date", ""))[:10]: b
        for b in old_bars if b.get("date")
    }
    for b in new_bars:
        d = str(b.get("date", ""))[:10]
        if d:
            by_date[d] = b

    merged = sorted(by_date.values(), key=lambda x: x.get("date", ""))

    # Trim to 3650 most recent calendar days to prevent unbounded growth
    if merged:
        from datetime import date as _date
        cutoff = (_date.today() - timedelta(days=3660)).isoformat()
        merged = [b for b in merged if str(b.get("date", ""))[:10] >= cutoff]

    return save_bars(
        sym, merged, provider,
        is_actual_limit=existing.get("is_actual_limit", False),
        refresh_mode="incremental_daily_append",
    )


def mark_failed(symbol: str, error_reason: str, provider: str = "unknown") -> None:
    """Record a failed attempt without overwriting existing bars."""
    sym = symbol.upper()
    now = _now_ts()
    stale = (datetime.now(timezone.utc) + timedelta(hours=4)).isoformat()
    existing = _INDEX.get(sym, {})
    _INDEX[sym] = {
        **existing,
        "history_status":  "fetch_failed",
        "error_reason":    error_reason,
        "last_attempt_at": now,
        "next_retry_at":   stale,
        "provider":        provider,
    }
    _write_index()


def mark_excluded(symbol: str) -> None:
    """Mark ineligible (contains ':' etc.) — no future backfill attempts."""
    sym = symbol.upper()
    _INDEX[sym] = {
        "symbol":         sym,
        "history_status": "excluded_prefixed_symbol",
        "fetched_at":     _now_ts(),
    }
    _write_index()
