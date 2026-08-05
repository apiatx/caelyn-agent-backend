"""
canonical_history_service.py — Disk-persistent canonical 10-year price history cache.
======================================================================================
V4.2.5.5 — Tradier 10Y precision fix + partial-history usability.

Target: up to 10 years of daily candles (~2520 trading bars) per symbol via Tradier.
FMP is fallback only (Tradier is primary for ALL depth including 10Y).

Storage layout
--------------
  backend/data/canonical_history/
    _index.json            — lightweight metadata per symbol (no bars)
    {SYM}.json.gz          — gzipped full OHLCV bars + metadata per symbol

Lifecycle
---------
  preload_index()          — startup: reads _index.json into _INDEX, no fetches
  get_bars(symbol)         — read path; returns bars for any usable cache (>= 40 bars)
  save_bars(...)           — write path; called by backfill job only
  append_bars(...)         — incremental daily merge; called by nightly job only
  is_fresh(symbol)         — staleness check (stale = needs append, not unusable)
  needs_append(symbol)     — True when newest_bar_date is > 2 trading days old

Daily-bar contract
------------------
Canonical history stores provider-supplied daily OHLCV bars: Tradier is requested
with ``interval="daily"`` and FMP is an EOD fallback.  Ingestion normalizes the
date and numeric OHLCV values but does not receive or invent a per-bar regular-
session marker.  Volume metrics therefore trust prior-date canonical daily bars
and deliberately exclude every bar dated today or later in America/New_York.  A
same-day provider bar is not used, whether it is partial or final.

History status values
---------------------
  available_10y                    >= 2200 bars (covers ~10 calendar years)
  available_10y_fresh              available_10y + newest bar within 3 calendar days
  available_10y_needs_append       available_10y + newest bar 4–10 days old
  available_10y_stale_but_usable   available_10y + newest bar > 10 days old
  available_lifetime_under_10y     ticker < 10Y old; all available public history cached
  available_5y_partial_long_history  >= 1100 bars (~4.4Y), ticker is older than 5Y
  available_3y_partial_history     >= 700 bars
  partial_history                  >= 504 bars
  intermediate_only                >= 252 bars
  recent_only                      >= 40 bars
  actual_ticker_history_limit      genuinely new ticker (IPO very recent, < ~6 months)
  provider_cap_detected            correct 10Y request sent; provider returned truncated range;
                                   ticker is known to be older than the returned range
  insufficient_history             < 40 bars (not usable)
  fetch_failed                     provider returned error
  not_yet_backfilled               no attempt yet
  excluded_prefixed_symbol         contains ":" or otherwise ineligible
  cache_corrupt_needs_rebuild      corrupt/unreadable gz file

TTL / staleness semantics (V4.2.5.5)
--------------------------------------
  available_10y*:                   48h → stale = needs append; bars always returned
  available_lifetime_under_10y:     96h → stale = check for new bars; bars always returned
  provider_cap_detected:            48h → stale = retry with deeper provider; bars returned
  available_5y_partial_long_history: 48h → stale = needs append / upgrade; bars returned
  available_3y_partial_history:     36h → bars returned even stale
  partial_history:                  24h → bars returned even stale
  intermediate_only:                12h → bars returned even stale
  recent_only:                       6h → bars returned even stale
  actual_ticker_history_limit:      96h
  fetch_failed:                      4h
  NOT_YET/excluded/corrupt:         never returned

  CRITICAL: get_bars() returns bars for ANY status with bar_count >= 40 regardless of
  freshness. Staleness only triggers an incremental append or upgrade — it does NOT
  make the cache unusable for Stage/Entry/Fib analysis.

Depth confidence (V4.2.5.5)
---------------------------
  2200+ bars (10Y)     = 1.00
  1100–2199 (5Y range) = 0.85
  756–1099 (3Y range)  = 0.75
  504–755  (2Y range)  = 0.65
  252–503  (1Y range)  = 0.50
  <252 bars            = 0.25 (unless available_lifetime_under_10y → linear)
  actual_ticker_history_limit → linear from 0.25 to 0.85
"""
from __future__ import annotations

import gzip
import json
import os
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

_CANON_DIR   = Path(__file__).parent.parent / "data" / "canonical_history"
_INDEX_FILE  = _CANON_DIR / "_index.json"
_INDEX: dict[str, dict] = {}   # in-memory metadata; no bars stored here

# Bar-count thresholds
_10Y_MIN_BARS   = 2200
_5Y_MIN_BARS    = 1100
_3Y_MIN_BARS    = 700
_2Y_MIN_BARS    = 504
_1Y_MIN_BARS    = 252
_RECENT_MIN     = 40

# Usable statuses — get_bars() always returns bars for these
_ALWAYS_USABLE = frozenset({
    "available_10y",
    "available_10y_fresh",
    "available_10y_needs_append",
    "available_10y_stale_but_usable",
    "available_lifetime_under_10y",
    "provider_cap_detected",
    "available_5y_partial_long_history",
    "available_3y_partial_history",
    "partial_history",
    "intermediate_only",
    "recent_only",
    "actual_ticker_history_limit",
})

# Statuses that count as "complete" — skip in full-backfill priority builder
_COMPLETE_STATUSES = frozenset({
    "available_10y",
    "available_10y_fresh",
    "available_10y_needs_append",
    "available_10y_stale_but_usable",
    "available_lifetime_under_10y",
    "actual_ticker_history_limit",
})

_VOLUME_METRIC_FIELDS = (
    "volume_change_1d_pct",
    "volume_change_7d_pct",
    "volume_change_30d_pct",
    "volume_acceleration_pp",
    "volume_metrics_as_of",
    "volume_metrics_status",
)

_PRICE_METRIC_FIELDS = (
    "change_7d",
    "change_30d",
)

_WATCHLIST_MARKET_METRIC_FIELDS = _VOLUME_METRIC_FIELDS + _PRICE_METRIC_FIELDS


# ── Status helpers ─────────────────────────────────────────────────────────────

def is_10y_complete(status: str) -> bool:
    return status in _COMPLETE_STATUSES


def is_always_usable(status: str) -> bool:
    return status in _ALWAYS_USABLE


# ── Status classification ─────────────────────────────────────────────────────

def classify_history_status(
    bar_count:            int,
    is_actual_limit:      bool = False,
    is_lifetime_under_10y: bool = False,
    is_provider_cap:      bool = False,
) -> str:
    """
    Classify history completeness.

    is_actual_limit:       Ticker is genuinely brand-new (IPO within ~6 months)
    is_lifetime_under_10y: Ticker < 10Y old; all available history cached
    is_provider_cap:       10Y request sent; provider returned truncated range;
                           ticker is known to be older than the returned range
    """
    if is_actual_limit and bar_count < _5Y_MIN_BARS:
        return "actual_ticker_history_limit"
    if is_lifetime_under_10y:
        # Young ticker — we have all its available history
        return "available_lifetime_under_10y"
    if is_provider_cap and bar_count > 0:
        return "provider_cap_detected"
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
    if bar_count >= _RECENT_MIN:
        return "recent_only"
    return "insufficient_history"


def classify_append_freshness(base_status: str, newest_bar_date: Optional[str]) -> str:
    """Refine available_10y into append-freshness sub-states."""
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


def depth_confidence(
    bar_count:             int,
    is_actual_limit:       bool = False,
    is_lifetime_under_10y: bool = False,
) -> float:
    """Data depth confidence (0.0–1.0)."""
    if is_lifetime_under_10y:
        # Confidence proportional to years of public history (max 0.85 since < 10Y)
        return round(min(0.85, 0.25 + (bar_count / _5Y_MIN_BARS) * 0.60), 2) if bar_count > 0 else 0.25
    if is_actual_limit:
        return round(min(0.85, 0.25 + (bar_count / _1Y_MIN_BARS) * 0.60), 2) if bar_count > 0 else 0.25
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
    bar_count:        int,
    weekly_bar_count:  int = 0,
    monthly_bar_count: int = 0,
) -> str:
    if bar_count >= _10Y_MIN_BARS:
        return "full_10y"
    if bar_count >= 756 and (weekly_bar_count >= 26 or weekly_bar_count == 0):
        return "multi_year"
    if bar_count >= _2Y_MIN_BARS:
        return "long"
    if bar_count >= _1Y_MIN_BARS:
        return "intermediate"
    if bar_count >= _RECENT_MIN:
        return "recent"
    return "insufficient"


def depth_limitation_reason(
    bar_count:       int,
    history_source:  str,
    is_actual_limit: bool = False,
    is_lifetime:     bool = False,
    is_provider_cap: bool = False,
) -> Optional[str]:
    if is_actual_limit:
        return "actual_ticker_history_limit"
    if is_lifetime:
        return "available_lifetime_under_10y"
    if is_provider_cap:
        return "provider_cap_detected"
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
    if bar_count >= _RECENT_MIN:
        return f"recent_only_{history_source}"
    return "insufficient_bars"


# ── Internal helpers ──────────────────────────────────────────────────────────

def _stale_hours(status: str) -> float:
    return {
        "available_10y":                        48.0,
        "available_10y_fresh":                  48.0,
        "available_10y_needs_append":           48.0,
        "available_10y_stale_but_usable":       48.0,
        "available_lifetime_under_10y":         96.0,
        "provider_cap_detected":                48.0,
        "available_5y_partial_long_history":    48.0,
        "available_3y_partial_history":         36.0,
        "partial_history":                      24.0,
        "intermediate_only":                    12.0,
        "recent_only":                           6.0,
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


def _null_volume_metrics(status: str = "unavailable") -> dict[str, object]:
    return {
        "volume_change_1d_pct": None,
        "volume_change_7d_pct": None,
        "volume_change_30d_pct": None,
        "volume_acceleration_pp": None,
        "volume_metrics_as_of": None,
        "volume_metrics_status": status,
    }


def _null_price_metrics() -> dict[str, object]:
    return {
        "change_7d": None,
        "change_30d": None,
    }


def _finite_float_or_none(value) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed != parsed or parsed in (float("inf"), float("-inf")):
        return None
    return parsed


def _completed_daily_bars(bars: list[dict]) -> list[tuple[str, float | None, float | None]]:
    """Return canonical daily bars strictly before the current New York date."""
    ny_today = datetime.now(ZoneInfo("America/New_York")).date()
    completed: list[tuple[str, float | None, float | None]] = []
    for bar in bars:
        date_raw = str(bar.get("date") or "")[:10]
        if not date_raw:
            continue
        try:
            bar_date = datetime.strptime(date_raw, "%Y-%m-%d").date()
        except ValueError:
            continue
        if bar_date >= ny_today:
            continue
        completed.append((
            date_raw,
            _finite_float_or_none(bar.get("close")),
            _finite_float_or_none(bar.get("volume")),
        ))
    return completed


def _compute_volume_metrics_from_completed(
    completed: list[tuple[str, float | None, float | None]],
) -> dict[str, object]:
    """Compute volume metrics from already-filtered canonical daily bars."""
    result = _null_volume_metrics()
    if not completed:
        return result

    result["volume_metrics_as_of"] = completed[-1][0]
    volumes = [volume for _, _, volume in completed]

    def _window_pct(window: int) -> float | None:
        need = window * 2
        if len(volumes) < need:
            return None
        latest = volumes[-window:]
        previous = volumes[-need:-window]
        if any(v is None or v < 0 for v in latest + previous):
            return None
        prev_avg = sum(previous) / window
        latest_avg = sum(latest) / window
        if prev_avg <= 0:
            return None
        return round(((latest_avg / prev_avg) - 1.0) * 100.0, 6)

    result["volume_change_1d_pct"] = _window_pct(1)
    result["volume_change_7d_pct"] = _window_pct(7)
    result["volume_change_30d_pct"] = _window_pct(30)

    v7 = result["volume_change_7d_pct"]
    v30 = result["volume_change_30d_pct"]
    if v7 is not None and v30 is not None:
        result["volume_acceleration_pp"] = round(v7 - v30, 6)

    metrics = (
        result["volume_change_1d_pct"],
        result["volume_change_7d_pct"],
        result["volume_change_30d_pct"],
        result["volume_acceleration_pp"],
    )
    if any(metric is not None for metric in metrics):
        result["volume_metrics_status"] = "ok" if all(metric is not None for metric in metrics) else "insufficient_history"
    else:
        result["volume_metrics_status"] = "insufficient_history"
    return result


def _compute_price_metrics_from_completed(
    completed: list[tuple[str, float | None, float | None]],
) -> dict[str, object]:
    """Compute completed-session close-to-close price returns for Watchlist rows.

    Semantics: calendar-day lookback.
      change_7d  — (last_close / close_on_or_before(last_date − 7 calendar days)  − 1) × 100
      change_30d — (last_close / close_on_or_before(last_date − 30 calendar days) − 1) × 100

    The comparison bar is the most recent completed session whose date is ≤
    (last completed bar date − calendar_days).  This preserves weekends, holidays,
    and early-close sessions without inventing a fixed session-count offset.

    Invariants enforced:
      • comparison_close must be finite and strictly positive (no division by zero).
      • latest_close must be finite and non-negative.
      • If no eligible comparison bar exists (insufficient history), returns None.
      • Bars must already be sorted chronologically (ensured by _completed_daily_bars).
    """
    result = _null_price_metrics()
    if not completed:
        return result

    latest_close = completed[-1][1]
    if latest_close is None or latest_close < 0:
        return result

    try:
        as_of_date = date.fromisoformat(completed[-1][0])
    except (ValueError, TypeError):
        return result

    def _calendar_day_return_pct(calendar_days: int) -> float | None:
        """Return pct change vs the most-recent bar on/before as_of_date − calendar_days."""
        target_date = as_of_date - timedelta(days=calendar_days)
        comparison_close: float | None = None
        # Walk backwards through completed[:-1] (skip the latest bar itself)
        for bar_date_str, close, _ in reversed(completed[:-1]):
            try:
                if date.fromisoformat(bar_date_str) <= target_date:
                    comparison_close = close
                    break
            except (ValueError, TypeError):
                continue
        if comparison_close is None or comparison_close <= 0:
            return None
        return round(((latest_close / comparison_close) - 1.0) * 100.0, 6)

    result["change_7d"] = _calendar_day_return_pct(7)
    result["change_30d"] = _calendar_day_return_pct(30)
    return result


def _compute_watchlist_market_metrics_from_bars(bars: list[dict]) -> dict[str, object]:
    """Compute all Watchlist history metrics from one completed-bar pass."""
    completed = _completed_daily_bars(bars)
    return {
        **_compute_volume_metrics_from_completed(completed),
        **_compute_price_metrics_from_completed(completed),
    }


def _compute_volume_metrics_from_bars(bars: list[dict]) -> dict[str, object]:
    """
    Completed-session volume metrics derived from canonical daily bars.

    Contract: only bars strictly before the current America/New_York date are
    eligible.  Canonical providers supply daily bars without a session marker,
    so this date boundary is the protection against a same-day partial bar.
    Intended for history-write time and for one-time metadata backfills.
    """
    return _compute_volume_metrics_from_completed(_completed_daily_bars(bars))


def _apply_watchlist_market_metrics_to_meta(meta: dict, bars: list[dict]) -> dict:
    return {**meta, **_compute_watchlist_market_metrics_from_bars(bars)}


def _write_index() -> None:
    """Write index atomically, merging with existing disk content."""
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
        complete  = sum(1 for v in _INDEX.values()
                        if is_10y_complete(v.get("history_status", "")))
        partial5y = sum(1 for v in _INDEX.values()
                        if v.get("history_status") == "available_5y_partial_long_history")
        print(
            f"[CANON_HIST] index loaded: {len(_INDEX)} symbols, "
            f"{complete} complete, {partial5y} available_5y_partial"
        )
        if not _INDEX:
            print("[CANON_HIST] no index on disk — will be created on first backfill")
    except Exception as exc:
        print(f"[CANON_HIST] index load error (non-fatal): {exc}")


def get_metadata(symbol: str) -> Optional[dict]:
    return _INDEX.get(symbol.upper())


def get_all_status() -> dict:
    return dict(_INDEX)


def get_volume_metrics_bulk(symbols: list[str]) -> dict[str, dict]:
    """Return Watchlist volume and price metrics from the canonical cache only.

    Existing metadata may predate the price-return fields.  In that case, load
    the already-cached gz payload once for that symbol, calculate both metric
    families in the existing in-memory index, and do not write or fetch.
    """
    if not _INDEX:
        try:
            preload_index()
        except Exception:
            pass
    out: dict[str, dict] = {}
    seen: set[str] = set()
    for raw_symbol in symbols:
        sym = str(raw_symbol or "").strip().upper()
        if not sym or sym in seen:
            continue
        seen.add(sym)
        meta = _INDEX.get(sym) or {}
        if any(field not in meta for field in _PRICE_METRIC_FIELDS):
            computed = {**_null_volume_metrics(), **_null_price_metrics()}
            bar_path = _bar_file(sym)
            if bar_path.exists():
                try:
                    with gzip.open(str(bar_path), "rt", encoding="utf-8") as fh:
                        payload = json.loads(fh.read())
                    computed = _compute_watchlist_market_metrics_from_bars(payload.get("bars") or [])
                except Exception as exc:
                    print(f"[CANON_HIST] watchlist market metrics read failed {sym}: {exc}")
            meta = {**meta, **computed}
            _INDEX[sym] = meta
        out[sym] = {
            field: meta.get(field)
            for field in _WATCHLIST_MARKET_METRIC_FIELDS
        }
        if out[sym].get("volume_metrics_status") is None:
            out[sym] = {
                **_null_volume_metrics("unavailable"),
                **_null_price_metrics(),
            }
    return out


def backfill_volume_metrics_metadata(symbols: Optional[list[str]] = None) -> dict[str, int]:
    """
    One-time metadata repair using existing canonical gz files.
    Runs off-request to persist volume summaries into _INDEX.
    """
    if not _INDEX:
        preload_index()
    selected = []
    seen: set[str] = set()
    for raw_symbol in (symbols or list(_INDEX.keys())):
        sym = str(raw_symbol or "").strip().upper()
        if sym and sym not in seen:
            selected.append(sym)
            seen.add(sym)

    file_reads = updated = skipped = 0
    for sym in selected:
        meta = _INDEX.get(sym)
        if not meta:
            skipped += 1
            continue
        if all(field in meta for field in _WATCHLIST_MARKET_METRIC_FIELDS):
            skipped += 1
            continue
        bar_path = _bar_file(sym)
        if not bar_path.exists():
            skipped += 1
            continue
        try:
            with gzip.open(str(bar_path), "rt", encoding="utf-8") as fh:
                payload = json.loads(fh.read())
            file_reads += 1
            bars = payload.get("bars") or []
            _INDEX[sym] = _apply_watchlist_market_metrics_to_meta(meta, bars)
            updated += 1
        except Exception as exc:
            print(f"[CANON_HIST] volume-metrics metadata repair failed {sym}: {exc}")
    if updated:
        _write_index()
    return {"updated": updated, "file_reads": file_reads, "skipped": skipped}


def is_fresh(symbol: str, max_age_h: Optional[float] = None) -> bool:
    """
    Return True when the cache for *symbol* is within its freshness window.
    Staleness does NOT mean unusable — it triggers append/upgrade only.
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
    """True when the newest cached bar is older than max_bar_age_days."""
    meta = _INDEX.get(symbol.upper())
    if not meta:
        return False
    status = meta.get("history_status", "")
    if not status or status in ("not_yet_backfilled", "fetch_failed",
                                "excluded_prefixed_symbol", "insufficient_history"):
        return False
    return _age_days_since_newest_bar(meta.get("newest_bar_date")) > max_bar_age_days


def is_stale_but_usable(symbol: str) -> bool:
    """True for any status in _ALWAYS_USABLE when stale — bars are still good for analysis."""
    meta = _INDEX.get(symbol.upper())
    if not meta:
        return False
    return is_always_usable(meta.get("history_status", ""))


def get_bars(symbol: str, require_fresh: bool = True) -> Optional[dict]:
    """
    Return canonical history payload dict (includes 'bars' key) or None.

    V4.2.5.5 usability semantics:
      Any status in _ALWAYS_USABLE with bar_count >= 40 ALWAYS returns bars,
      regardless of staleness. Stale means "needs append/upgrade" only.

      Returns None only when:
        - No index entry (symbol not yet backfilled)
        - Status is fetch_failed / not_yet_backfilled / excluded_prefixed_symbol
        - Cache file is missing or corrupt (sets status = cache_corrupt_needs_rebuild)
        - bar_count < 40 (insufficient for any analysis)

    Use require_fresh=False to read stale bars for non-usable statuses as last resort.
    """
    sym = symbol.upper()
    if not _INDEX:
        try:
            preload_index()
        except Exception:
            pass

    meta = _INDEX.get(sym)
    if meta:
        status    = meta.get("history_status", "")
        bar_count = int(meta.get("bar_count") or 0)

        # Non-usable statuses — always return None
        if status in ("not_yet_backfilled", "excluded_prefixed_symbol",
                      "fetch_failed", "insufficient_history",
                      "cache_corrupt_needs_rebuild"):
            return None

        # _ALWAYS_USABLE statuses — return bars regardless of freshness
        if status in _ALWAYS_USABLE and bar_count >= _RECENT_MIN:
            pass  # fall through to disk read
        elif require_fresh and not is_fresh(sym):
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
        if len(bars) < _RECENT_MIN:
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
    bar_count:             int,
    provider:              str,
    is_actual_limit:       bool,
    is_lifetime_under_10y: bool            = False,
    is_provider_cap:       bool            = False,
    tradier_capability:    Optional[str]   = None,
) -> str:
    """
    canonical_history_quality values (V4.2.5.5):

    full_10y_tradier_verified    Tradier >= 2200 bars, capability confirmed
    full_10y_tradier_unverified  Tradier >= 2200 bars, not yet cross-verified
    full_10y_fmp                 FMP >= 2200 bars
    available_lifetime_under_10y  Young ticker; all public history cached
    available_5y_partial_long_history  1100-2199 bars (Tradier typical depth)
    available_3y_partial_history  700-1099 bars
    provider_cap_detected        10Y request sent; provider returned truncated range
    actual_ticker_history_limit  Brand-new ticker
    partial_tradier              Tradier < 700 bars
    partial_fmp                  FMP < 700 bars
    stage_cache_fallback         400-bar emergency
    provider_failed              All empty
    not_yet_backfilled           No attempt
    """
    if is_lifetime_under_10y:
        return "available_lifetime_under_10y"
    if is_actual_limit:
        return "actual_ticker_history_limit"
    if is_provider_cap:
        return "provider_cap_detected"
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
    symbol:                str,
    bars:                  list[dict],
    provider:              str,
    is_actual_limit:       bool          = False,
    is_lifetime_under_10y: bool          = False,
    is_provider_cap:       bool          = False,
    error_reason:          Optional[str] = None,
    refresh_mode:          str           = "initial_full_backfill",
    tradier_capability:    Optional[str] = None,
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
        dates  = sorted(str(b.get("date", ""))[:10] for b in bars if b.get("date"))
        oldest = dates[0]  if dates else None
        newest = dates[-1] if dates else None
    else:
        oldest = newest = None

    years_avail = round(bar_count / 252, 1) if bar_count > 0 else 0.0
    base_status = (
        "fetch_failed" if error_reason
        else classify_history_status(
            bar_count,
            is_actual_limit=is_actual_limit,
            is_lifetime_under_10y=is_lifetime_under_10y,
            is_provider_cap=is_provider_cap,
        )
    )
    status      = classify_append_freshness(base_status, newest)
    dep_conf    = depth_confidence(bar_count, is_actual_limit, is_lifetime_under_10y)
    stale_h     = _stale_hours(status)
    now         = _now_ts()
    stale_after = (
        (datetime.now(timezone.utc) + timedelta(hours=stale_h)).isoformat()
        if stale_h > 0 else None
    )
    wk_approx = round(bar_count / 5)
    mo_approx = round(bar_count / 21)

    adjusted_status = "unknown"

    meta: dict = {
        "symbol":                            sym,
        "provider":                          provider,
        "canonical_history_provider":        provider,
        "canonical_history_provider_rank":   _provider_rank(provider),
        "canonical_history_quality":         _compute_quality(
                                                 bar_count, provider,
                                                 is_actual_limit,
                                                 is_lifetime_under_10y,
                                                 is_provider_cap,
                                                 tradier_capability),
        "canonical_history_adjusted_status": adjusted_status,
        "canonical_history_refresh_mode":    refresh_mode,
        "bar_count":                         bar_count,
        "oldest_bar_date":                   oldest,
        "newest_bar_date":                   newest,
        "years_available":                   years_avail,
        "fetched_at":                        now,
        "stale_after":                       stale_after,
        "history_status":                    status,
        "base_history_status":               base_status,
        "error_reason":                      error_reason,
        "source_priority":                   _provider_rank(provider),
        "is_actual_limit":                   is_actual_limit,
        "is_lifetime_under_10y":             is_lifetime_under_10y,
        "is_provider_cap":                   is_provider_cap,
        "depth_confidence":                  dep_conf,
        "fib_scope":                         fib_timeframe_scope(bar_count, wk_approx, mo_approx),
        "last_attempt_at":                   now,
        "next_retry_at":                     stale_after,
        "cache_usable":                      bar_count >= _RECENT_MIN,
        "is_10y_complete":                   is_10y_complete(status),
    }
    meta = _apply_watchlist_market_metrics_to_meta(meta, bars)

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


def append_bars(symbol: str, new_bars: list[dict], provider: str) -> Optional[dict]:
    """
    Incremental daily append — 1 provider call per symbol.
    2-day overlap is within that single request (start_date = newest - 2d).
    """
    sym      = symbol.upper()
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

    # Trim to 3660 most recent calendar days (prevents unbounded growth)
    if merged:
        cutoff = (date.today() - timedelta(days=3660)).isoformat()
        merged = [b for b in merged if str(b.get("date", ""))[:10] >= cutoff]

    return save_bars(
        sym, merged, provider,
        is_actual_limit=existing.get("is_actual_limit", False),
        is_lifetime_under_10y=existing.get("is_lifetime_under_10y", False),
        is_provider_cap=existing.get("is_provider_cap", False),
        refresh_mode="incremental_daily_append",
    )


def mark_failed(symbol: str, error_reason: str, provider: str = "unknown") -> None:
    sym      = symbol.upper()
    now      = _now_ts()
    stale    = (datetime.now(timezone.utc) + timedelta(hours=4)).isoformat()
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
    sym = symbol.upper()
    _INDEX[sym] = {
        "symbol":         sym,
        "history_status": "excluded_prefixed_symbol",
        "fetched_at":     _now_ts(),
    }
    _write_index()
