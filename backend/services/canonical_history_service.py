"""
canonical_history_service.py — Disk-persistent canonical 5-year price history cache.
=====================================================================================
V4.2.5.2 — Zero provider calls in this module.  Pure disk read/write.

Storage layout
--------------
  backend/data/canonical_history/
    _index.json            — lightweight metadata per symbol (no bars)
    {SYM}.json.gz          — gzipped full OHLCV bars + metadata per symbol

Lifecycle
---------
  preload_index()          — startup: reads _index.json into _INDEX, no fetches
  get_bars(symbol)         — read path; returns None when stale/missing
  save_bars(...)           — write path; called by backfill job only
  is_fresh(symbol)         — staleness check

History status values
---------------------
  available_5y             >= 1100 bars (~4.4 years)
  available_3y             >= 700  bars
  partial_history          >= 504  bars (~2 years)
  intermediate_only        >= 252  bars (~1 year)
  recent_only              >= 40   bars
  actual_ticker_history_limit  — genuinely new ticker
  insufficient_history     < 40 bars
  fetch_failed             provider returned error
  not_yet_backfilled       no attempt yet
  excluded_prefixed_symbol contains ":" or otherwise ineligible
"""
from __future__ import annotations

import gzip
import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

_CANON_DIR   = Path(__file__).parent.parent / "data" / "canonical_history"
_INDEX_FILE  = _CANON_DIR / "_index.json"
_INDEX: dict[str, dict] = {}   # in-memory metadata; no bars stored here


# ── Status classification ─────────────────────────────────────────────────────

def classify_history_status(bar_count: int, is_actual_limit: bool = False) -> str:
    if is_actual_limit:
        return "actual_ticker_history_limit"
    if bar_count >= 1100:
        return "available_5y"
    if bar_count >= 700:
        return "available_3y"
    if bar_count >= 504:
        return "partial_history"
    if bar_count >= 252:
        return "intermediate_only"
    if bar_count >= 40:
        return "recent_only"
    return "insufficient_history"


def depth_confidence(bar_count: int, is_actual_limit: bool = False) -> float:
    """
    Data depth confidence (0.0–1.0).

    Actual-ticker-history-limit tickers are not penalised for genuinely
    having less history — confidence scales linearly to their real depth.
    """
    if is_actual_limit:
        return round(min(1.0, bar_count / 252 * 0.75), 2) if bar_count > 0 else 0.25
    if bar_count >= 1300:
        return 1.00
    if bar_count >= 756:
        return 0.85
    if bar_count >= 504:
        return 0.70
    if bar_count >= 252:
        return 0.50
    return 0.25


def fib_timeframe_scope(bar_count: int, weekly_bar_count: int = 0, monthly_bar_count: int = 0) -> str:
    """
    fib_timeframe_scope — which Fib candidate classes are meaningful given bar depth.

    multi_year    weekly + monthly Fib contexts available  (bars >= 756)
    long          long daily + weekly possible             (bars >= 504)
    intermediate  intermediate daily candidates            (bars >= 252)
    recent        recent daily only                        (bars >= 40)
    insufficient  too few bars for any Fib work            (bars < 40)
    actual_ticker_history_limited  — new ticker, not a data failure
    """
    if bar_count >= 756 and (weekly_bar_count >= 26 or weekly_bar_count == 0):
        return "multi_year"
    if bar_count >= 504:
        return "long"
    if bar_count >= 252:
        return "intermediate"
    if bar_count >= 40:
        return "recent"
    return "insufficient"


def depth_limitation_reason(
    bar_count: int,
    history_source: str,
    is_actual_limit: bool = False,
) -> Optional[str]:
    if is_actual_limit:
        return "actual_ticker_history_limit"
    if bar_count >= 1100:
        return None
    if bar_count >= 756:
        return "below_5y_target"
    if bar_count >= 504:
        return "partial_2_3y_range"
    if bar_count >= 252:
        return "intermediate_only_1y"
    if bar_count >= 40:
        return f"recent_only_{history_source}"
    return "insufficient_bars"


# ── Internal helpers ──────────────────────────────────────────────────────────

def _stale_hours(status: str) -> float:
    return {
        "available_5y":                20.0,
        "available_3y":                16.0,
        "partial_history":             12.0,
        "intermediate_only":            6.0,
        "recent_only":                  6.0,
        "actual_ticker_history_limit": 24.0,
        "insufficient_history":         4.0,
        "fetch_failed":                 4.0,
        "not_yet_backfilled":           0.0,
        "excluded_prefixed_symbol":   999.0,
    }.get(status, 12.0)


def _now_ts() -> str:
    return datetime.now(timezone.utc).isoformat()


def _age_hours(fetched_at: str) -> float:
    try:
        dt = datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0
    except Exception:
        return 9999.0


def _ensure_dir() -> None:
    _CANON_DIR.mkdir(parents=True, exist_ok=True)


def _bar_file(symbol: str) -> Path:
    return _CANON_DIR / f"{symbol.upper()}.json.gz"


def _write_index() -> None:
    """
    Write index atomically.  Always MERGES with existing disk content so that
    concurrent processes (e.g. admin backfill + test subprocess) do not clobber
    each other's entries.  In-memory _INDEX wins on symbol-level conflicts.
    """
    try:
        _ensure_dir()
        # Read existing disk index to preserve symbols not in current _INDEX
        disk_index: dict = {}
        if _INDEX_FILE.exists():
            try:
                disk = json.loads(_INDEX_FILE.read_text())
                disk_index = {k.upper(): v for k, v in disk.get("symbols", {}).items()}
            except Exception:
                pass
        merged = {**disk_index, **_INDEX}   # in-memory wins on conflict
        tmp = _INDEX_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps({"updated_at": _now_ts(), "symbols": merged}, indent=2))
        tmp.replace(_INDEX_FILE)
    except Exception as exc:
        print(f"[CANON_HIST] index write error (non-fatal): {exc}")


# ── Public API ────────────────────────────────────────────────────────────────

def _rebuild_index_from_gz() -> int:
    """
    Scan _CANON_DIR for *.json.gz files and (re-)populate _INDEX from their
    embedded metadata.  Used to recover when _index.json is stale or missing
    entries that have valid bar files on disk.  Returns number of symbols added.
    """
    added = 0
    try:
        _ensure_dir()
        for gz_path in sorted(_CANON_DIR.glob("*.json.gz")):
            sym = gz_path.name.split(".")[0].upper()   # AAPL.json.gz → AAPL
            if sym in _INDEX:
                continue   # already known — skip to avoid rewriting expensive gzip
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
            # Only accept symbol keys that look like real tickers (no dots / file extensions)
            _INDEX.update({
                k.upper(): v
                for k, v in data.get("symbols", {}).items()
                if "." not in k and k.upper() == k.upper().strip()
            })
        # Prune index entries whose gz file was deleted (orphaned metadata)
        orphaned = [s for s in list(_INDEX) if not _bar_file(s).exists()]
        for s in orphaned:
            del _INDEX[s]
        # Recover any gz files on disk that are missing from the index
        recovered = _rebuild_index_from_gz()
        if recovered or orphaned:
            _write_index()   # persist the cleaned/merged index
            if recovered:
                print(f"[CANON_HIST] recovered {recovered} symbols from gz files")
            if orphaned:
                print(f"[CANON_HIST] pruned {len(orphaned)} orphaned index entries")
        fresh = sum(1 for v in _INDEX.values() if v.get("history_status") == "available_5y")
        print(
            f"[CANON_HIST] index loaded: {len(_INDEX)} symbols, "
            f"{fresh} available_5y"
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
    """Return True when the canonical cache for *symbol* is within its freshness window."""
    meta = _INDEX.get(symbol.upper())
    if not meta:
        return False
    status = meta.get("history_status", "not_yet_backfilled")
    if status in ("not_yet_backfilled", "excluded_prefixed_symbol"):
        return False
    threshold = max_age_h if max_age_h is not None else _stale_hours(status)
    return _age_hours(meta.get("fetched_at", "")) <= threshold


def get_bars(symbol: str, require_fresh: bool = True) -> Optional[dict]:
    """
    Return canonical history payload dict (includes 'bars' key) or None.

    When require_fresh=True (default) returns None for stale / missing cache.
    Use require_fresh=False to read even stale cached bars as last resort.

    Lazy-loads the index from disk on first call if preload_index() was not
    yet called (guards against subprocess / deferred startup ordering).
    """
    sym = symbol.upper()
    # ── Lazy index population (no-op after first call) ────────────────────────
    if not _INDEX:
        try:
            preload_index()
        except Exception:
            pass
    if require_fresh and not is_fresh(sym):
        return None
    f = _bar_file(sym)
    if not f.exists():
        return None
    try:
        with gzip.open(str(f), "rt", encoding="utf-8") as fh:
            payload = json.loads(fh.read())
        bars = payload.get("bars") or []
        if not bars:
            return None
        return payload
    except Exception as exc:
        print(f"[CANON_HIST] read error {sym}: {exc}")
        return None


def save_bars(
    symbol:           str,
    bars:             list[dict],
    provider:         str,
    is_actual_limit:  bool          = False,
    error_reason:     Optional[str] = None,
) -> dict:
    """
    Persist canonical history to disk and update _INDEX.
    Returns the metadata entry (without bars).
    Called by the backfill job only — never at request time.
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
    status      = (
        "fetch_failed" if error_reason
        else classify_history_status(bar_count, is_actual_limit)
    )
    dep_conf    = depth_confidence(bar_count, is_actual_limit)
    stale_h     = _stale_hours(status)
    now         = _now_ts()
    stale_after = (
        (datetime.now(timezone.utc) + timedelta(hours=stale_h)).isoformat()
        if stale_h > 0 else None
    )
    # Approximate weekly/monthly bar counts from bar_count
    wk_approx = round(bar_count / 5)
    mo_approx = round(bar_count / 21)

    meta: dict = {
        "symbol":            sym,
        "provider":          provider,
        "bar_count":         bar_count,
        "oldest_bar_date":   oldest,
        "newest_bar_date":   newest,
        "years_available":   years_avail,
        "fetched_at":        now,
        "stale_after":       stale_after,
        "history_status":    status,
        "error_reason":      error_reason,
        "source_priority":   1 if provider == "fmp" else 2,
        "is_actual_limit":   is_actual_limit,
        "depth_confidence":  dep_conf,
        "fib_scope":         fib_timeframe_scope(bar_count, wk_approx, mo_approx),
        "last_attempt_at":   now,
        "next_retry_at":     stale_after,
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
