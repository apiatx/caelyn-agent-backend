"""
Theme options supplement.

Manages four concerns for the options-by-sector feature:

  1. Theme seed injection
     get_theme_proxy_symbols_for_supplement() — theme proxy symbols NOT in
     the static master seed lists.  Injected into _cycle_seeds on each
     prefilter cold rebuild so the master Stage-1 sweep sees them.

  2. No-options persistent tracking
     update_no_options_from_expiry_cache() — called after every master
     screener cycle.  Reads the existing in-process Stage-1 expiry dict
     (zero extra Tradier calls) and persists symbols confirmed to have no
     tradeable options to a 24-hour cache entry.

  3. Supplemental scan cache  (two layers)
     a. Fresh layer  — options_theme_supplement_v1 (4h TTL, in-memory)
        update_supplement_cache() merges new results; also writes disk LKG.
     b. LKG layer    — options_supplement_lkg_v1  (4h TTL, in-memory)
        _load_supplement_lkg_from_disk() populates this at startup from
        backend/data/options_supplement_lkg_v1.json so supplement data
        survives server restarts.

     Row _source values:
       "supplement"      — scanned this session (fresh, current loop)
       "supplement_lkg"  — loaded from disk LKG (previous session)

  4. Combined data accessor
     get_combined_ticker_data() merges master + fresh supplement + LKG
     supplement into one {ticker: row} dict.  Priority: live > supplement
     > supplement_lkg.

No new Tradier clients are created here.  All scan calls share the existing
TradierFlowEngine instance and _TRADIER_GLOBAL_SEM rate limiter.
"""
from __future__ import annotations

import json as _json
import pathlib as _pathlib
import time
from typing import Optional

_SUPPLEMENT_LKG_DISK_PATH    = _pathlib.Path(__file__).resolve().parent / "options_supplement_lkg_v1.json"
_SUPPLEMENT_LKG_DISK_MAX_AGE = 86400   # 24 h — reject snapshots older than this

_NO_OPTIONS_CACHE_KEY  = "options_no_options_tracking:v1"
_NO_OPTIONS_CACHE_TTL  = 86400   # 24 h

_SUPPLEMENT_CACHE_KEY  = "options_theme_supplement_v1"
_SUPPLEMENT_CACHE_TTL  = 14400   # 4 h — accumulates across batches within a session

_SUPPLEMENT_LKG_CACHE_KEY = "options_supplement_lkg_v1"
_SUPPLEMENT_LKG_CACHE_TTL = 14400   # 4 h — disk data loaded at startup

# ── Loop tracking (updated by main.py loop via update_scan_tracking()) ────────
_last_scanned_symbols: list[str] = []
_next_scan_at: float = 0.0

# ── Anti-duplication diagnostics (updated each batch by update_supplement_diag)
# Tracks how the supplement loop handles watchlist-overlap vs theme-only symbols.
# Lifetime counters increment across all batches since server start; snapshot
# fields reflect the most recent batch only.
_SUPP_DIAG: dict = {
    # ── Snapshot (latest batch) ──────────────────────────────────────────────
    "supplement_symbols_total":              0,    # len(pending) at last cycle
    "supplement_watchlist_overlap_symbols":  [],   # overlap syms in last batch
    "supplement_only_symbols":               [],   # theme-only syms in last batch
    # ── Lifetime counters ────────────────────────────────────────────────────
    "supplement_overlap_cache_hits":         0,    # skipped — fresh portfolio_opts cache
    "supplement_overlap_live_scans":         0,    # gap-fill live scans for overlap syms
    "supplement_overlap_live_scans_blocked": 0,    # overlap syms blocked by inflight guard
    "supplement_only_live_scans":            0,    # live scans for theme-only syms
    "supplement_duplicate_scans_blocked":    0,    # total inflight-guard blocks
    "last_updated_at":                       None,
}

# ── Static seed dedup (lazy) ──────────────────────────────────────────────────
_static_seed_set: Optional[set] = None


def _get_static_seeds() -> set[str]:
    global _static_seed_set
    if _static_seed_set is None:
        try:
            import main as _m  # type: ignore[import]
            _static_seed_set = {
                s.upper() for s in (
                    _m._OPTIONS_ETF_SEEDS
                    + _m._OPTIONS_MEGACAP_SEEDS
                    + _m._OPTIONS_LARGE_CAP_SEEDS
                    + _m._OPTIONS_SMALL_CAP_SEEDS
                )
            }
        except Exception:
            _static_seed_set = set()
    return _static_seed_set


# ── Theme proxy symbol helpers ────────────────────────────────────────────────

def get_theme_proxy_symbols_for_supplement(max_symbols: int = 60) -> list[str]:
    """
    Return theme proxy symbols NOT already in the static master seed lists,
    prioritised: ETF proxies first (better options liquidity), then stocks.

    This list is injected into _cycle_seeds on prefilter cold rebuilds so
    high-activity theme symbols can reach Stage 2 of the master screener
    naturally, without creating additional Tradier calls.
    """
    try:
        from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE
    except ImportError:
        return []

    static_seeds = _get_static_seeds()
    etf_proxies:   list[str] = []
    stock_proxies: list[str] = []
    seen: set[str] = set()

    for meta in ENRICHED_THEME_RS_UNIVERSE.values():
        for sym in (meta.get("proxy_symbols") or []):
            sym = sym.upper()
            if sym in seen or sym in static_seeds:
                continue
            seen.add(sym)
            is_etf = (
                meta.get("proxy_type") == "etf"
                or (3 <= len(sym) <= 5 and sym.isalpha())
            )
            if is_etf:
                etf_proxies.append(sym)
            else:
                stock_proxies.append(sym)

    return (etf_proxies + stock_proxies)[:max_symbols]


def _get_master_tickers() -> set[str]:
    """Live master screener tickers (primary cache → LKG fallback)."""
    try:
        from data.cache import cache
        snap = (
            cache.get("options_master_screener_v1")
            or cache.get("options_master_lkg_v1")
        )
        if snap:
            return {
                (r.get("ticker") or "").upper()
                for r in snap.get("tickers", [])
                if r.get("ticker")
            }
    except Exception:
        pass
    return set()


def get_theme_only_symbols_for_supplement() -> list[str]:
    """
    Return theme proxy symbols NOT in the master screener cache AND NOT
    confirmed as no-options AND NOT already in supplement caches.

    Sorted alphabetically for a deterministic rolling cursor.
    Prioritises symbols not yet in any supplement layer.
    """
    try:
        from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE
    except ImportError:
        return []

    all_syms: set[str] = {
        sym.upper()
        for meta in ENRICHED_THEME_RS_UNIVERSE.values()
        for sym in (meta.get("proxy_symbols") or [])
    }

    master_syms   = _get_master_tickers()
    no_opts       = get_no_options_symbols()
    supplement    = set(get_supplement_data_by_ticker().keys())

    return sorted(all_syms - master_syms - no_opts - supplement)


# ── Disk LKG persistence ──────────────────────────────────────────────────────

def _save_supplement_lkg_to_disk(ticker_data: dict) -> None:
    """
    Atomically persist supplement ticker_data to disk.  Same atomic-rename
    pattern as _save_master_lkg_to_disk in main.py.

    Called after every supplement batch so data survives server restarts.
    """
    if not ticker_data:
        return
    try:
        now = time.time()
        payload = {
            "ticker_data":  ticker_data,
            "saved_at":     now,
            "ticker_count": len(ticker_data),
        }
        tmp = _SUPPLEMENT_LKG_DISK_PATH.with_suffix(".json.tmp")
        _SUPPLEMENT_LKG_DISK_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_text(_json.dumps(payload, default=str), encoding="utf-8")
        tmp.replace(_SUPPLEMENT_LKG_DISK_PATH)
        print(f"[SUPP_LKG] Persisted {len(ticker_data)} supplement tickers to disk")
    except Exception as exc:
        print(f"[SUPP_LKG] Disk write failed (non-fatal): {exc}")


def _load_supplement_lkg_from_disk() -> None:
    """
    Load supplement LKG from disk at startup.  Synchronous — call before
    any request is served so the sectors endpoint has data immediately.

    Rows are tagged _source='supplement_lkg' to distinguish from fresh
    session scans.  Loaded into _SUPPLEMENT_LKG_CACHE_KEY (4h in-memory TTL).
    """
    if not _SUPPLEMENT_LKG_DISK_PATH.exists():
        print("[SUPP_LKG] No disk LKG — supplement data builds from scratch this session")
        return
    try:
        now = time.time()
        payload = _json.loads(_SUPPLEMENT_LKG_DISK_PATH.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            print("[SUPP_LKG] Disk LKG: not a dict — skipping")
            return
        saved_at = payload.get("saved_at", 0)
        age_s = int(now - saved_at)
        if age_s > _SUPPLEMENT_LKG_DISK_MAX_AGE:
            print(f"[SUPP_LKG] Disk LKG too old ({age_s}s > {_SUPPLEMENT_LKG_DISK_MAX_AGE}s) — skipping")
            return
        ticker_data: dict = payload.get("ticker_data", {})
        if not ticker_data:
            print("[SUPP_LKG] Disk LKG: empty ticker_data — skipping")
            return
        # Tag all rows as supplement_lkg so sectors endpoint can distinguish
        tagged = {sym: {**row, "_source": "supplement_lkg"} for sym, row in ticker_data.items()}
        from data.cache import cache
        cache.set(
            _SUPPLEMENT_LKG_CACHE_KEY,
            {"ticker_data": tagged, "loaded_at": now, "saved_at": saved_at},
            _SUPPLEMENT_LKG_CACHE_TTL,
        )
        print(f"[SUPP_LKG] Loaded {len(tagged)} supplement tickers from disk (age={age_s}s)")
    except Exception as exc:
        print(f"[SUPP_LKG] Disk load failed (non-fatal): {exc}")


# ── No-options tracking ───────────────────────────────────────────────────────

def get_no_options_symbols() -> set[str]:
    """Return the set of symbols confirmed to have no tradeable options."""
    try:
        from data.cache import cache
        tracking = cache.get(_NO_OPTIONS_CACHE_KEY) or {}
        return set(tracking.keys())
    except Exception:
        return set()


def update_no_options_from_expiry_cache(expiry_cache: dict) -> None:
    """
    Called after each master screener cycle (and after each supplement batch).

    Reads the in-process Stage-1 expiry dict and persists symbols with
    confirmed empty expirations to the long-lived no-options cache.
    Zero new Tradier calls — uses existing Stage-1 expiry data.

    expiry_cache format:  {ticker: ([exp_strings, ...], checked_at_float)}
    An empty expirations list means the ticker has no tradeable options.
    """
    if not expiry_cache:
        return
    try:
        from data.cache import cache
        existing: dict = cache.get(_NO_OPTIONS_CACHE_KEY) or {}
        now     = time.time()
        changed = False
        for sym, entry in expiry_cache.items():
            if not isinstance(entry, (list, tuple)) or len(entry) < 1:
                continue
            exps = entry[0]
            if isinstance(exps, list) and len(exps) == 0:
                if sym not in existing:
                    existing[sym] = {
                        "confirmed_at": entry[1] if len(entry) > 1 else now,
                        "updated_at":   now,
                    }
                    changed = True
        if changed:
            cache.set(_NO_OPTIONS_CACHE_KEY, existing, _NO_OPTIONS_CACHE_TTL)
    except Exception as exc:
        print(f"[THEME_SUPP] No-options tracking update error: {exc}")


# ── Supplement cache ──────────────────────────────────────────────────────────

def get_supplement_data_by_ticker() -> dict[str, dict]:
    """
    Return {ticker: options_row} merging fresh session scans + disk LKG.

    Priority: fresh supplement (this session) > LKG supplement (disk-loaded).
    Both layers are keyed separately so fresh data can be cleanly identified.
    """
    try:
        from data.cache import cache
        # Start with LKG (disk-loaded at startup, supplement_lkg tagged)
        lkg_snap  = cache.get(_SUPPLEMENT_LKG_CACHE_KEY) or {}
        combined  = dict(lkg_snap.get("ticker_data", {}))
        # Fresh session results override LKG
        fresh_snap = cache.get(_SUPPLEMENT_CACHE_KEY) or {}
        for sym, row in fresh_snap.get("ticker_data", {}).items():
            combined[sym] = row   # fresh always wins
        return combined
    except Exception:
        return {}


def update_supplement_cache(results: list[dict]) -> None:
    """
    Merge new supplement scan results into the fresh supplement cache and
    persist to disk LKG.

    Existing entries for tickers NOT in the new batch are preserved until
    the TTL expires.  New/updated entries replace old ones.  Each row is
    tagged _source='supplement' and _cached_at timestamp.

    Also calls _save_supplement_lkg_to_disk() so data survives restarts.
    """
    if not results:
        return
    try:
        from data.cache import cache
        existing   = cache.get(_SUPPLEMENT_CACHE_KEY) or {"ticker_data": {}, "cached_at": 0}
        ticker_data: dict = dict(existing.get("ticker_data", {}))
        now = time.time()
        for row in results:
            sym = (row.get("ticker") or "").upper()
            if sym:
                ticker_data[sym] = {**row, "_source": "supplement", "_cached_at": now}
        cache.set(
            _SUPPLEMENT_CACHE_KEY,
            {"ticker_data": ticker_data, "cached_at": now, "last_scan_at": now},
            _SUPPLEMENT_CACHE_TTL,
        )
        # Persist all accumulated fresh data to disk LKG
        _save_supplement_lkg_to_disk(ticker_data)
    except Exception as exc:
        print(f"[THEME_SUPP] Supplement cache update error: {exc}")


# ── Loop tracking ─────────────────────────────────────────────────────────────

def update_supplement_diag(diag: dict) -> None:
    """
    Merge one batch's anti-duplication stats into the module-level _SUPP_DIAG.

    Snapshot fields (supplement_symbols_total, supplement_watchlist_overlap_symbols,
    supplement_only_symbols) are replaced each call.  Lifetime counters are
    incremented.  Called by the supplement loop in main.py after the cache-first
    filter runs but before the live scan.
    """
    import time as _t
    global _SUPP_DIAG
    # Snapshot — replace each batch
    _SUPP_DIAG["supplement_symbols_total"]             = diag.get("supplement_symbols_total", 0)
    _SUPP_DIAG["supplement_watchlist_overlap_symbols"] = diag.get("supplement_watchlist_overlap_symbols", [])
    _SUPP_DIAG["supplement_only_symbols"]              = diag.get("supplement_only_symbols", [])
    # Lifetime — accumulate
    _SUPP_DIAG["supplement_overlap_cache_hits"]         += diag.get("supplement_overlap_cache_hits", 0)
    _SUPP_DIAG["supplement_overlap_live_scans"]         += diag.get("supplement_overlap_live_scans", 0)
    _SUPP_DIAG["supplement_overlap_live_scans_blocked"] += diag.get("supplement_overlap_live_scans_blocked", 0)
    _SUPP_DIAG["supplement_only_live_scans"]            += diag.get("supplement_only_live_scans", 0)
    _SUPP_DIAG["supplement_duplicate_scans_blocked"]    += diag.get("supplement_duplicate_scans_blocked", 0)
    _SUPP_DIAG["last_updated_at"]                        = _t.time()


def get_supplement_diag() -> dict:
    """Return a copy of the current anti-duplication diagnostics."""
    return dict(_SUPP_DIAG)


def update_scan_tracking(batch: list[str], next_at: float) -> None:
    """Called by the supplement loop after each batch to record tracking state."""
    global _last_scanned_symbols, _next_scan_at
    _last_scanned_symbols = (batch + _last_scanned_symbols)[:20]
    _next_scan_at = next_at


# ── Combined data accessor ────────────────────────────────────────────────────

def get_combined_ticker_data() -> dict[str, dict]:
    """
    Merge master screener cache + fresh supplement + supplement LKG + shared
    per-ticker portfolio options cache into one {ticker: row} dict.

    Priority:
      live > supplement (fresh) > supplement_lkg > watchlist_cache

    The watchlist_cache layer bridges portfolio_opts:{sym} keys written by the
    Watchlist and Portfolio Terminal scanners.  This makes every watchlist-scanned
    options row immediately visible to the Sectors aggregation layer without any
    additional Tradier calls — pure cache reads.

    Only theme-universe symbols that are still uncovered after the first three
    layers are checked against the per-ticker cache.
    """
    try:
        from data.cache import cache

        master_snap = (
            cache.get("options_master_screener_v1")
            or cache.get("options_master_lkg_v1")
        )
        combined: dict[str, dict] = {}

        if master_snap:
            for row in master_snap.get("tickers", []):
                sym = (row.get("ticker") or "").upper()
                if sym:
                    combined[sym] = {**row, "_source": "live"}

        for sym, row in get_supplement_data_by_ticker().items():
            if sym not in combined:
                combined[sym] = row   # already tagged by layer

        # ── Watchlist-cache bridge ────────────────────────────────────────────
        # For theme-universe symbols still missing from master+supplement, check
        # the shared per-ticker portfolio options cache (portfolio_opts:{sym}).
        # This lets one Watchlist scan warm the Sectors view for free.
        # Zero Tradier calls — read-only cache access.
        try:
            from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE
            from data.portfolio_options_service import _per_ticker_cache_key

            for meta in ENRICHED_THEME_RS_UNIVERSE.values():
                for sym in (meta.get("proxy_symbols") or []):
                    sym = sym.upper()
                    if sym in combined:
                        continue
                    row = cache.get(_per_ticker_cache_key(sym))
                    if row and isinstance(row, dict) and row.get("data_available"):
                        combined[sym] = {**row, "_source": "watchlist_cache"}
        except Exception:
            pass
        # ─────────────────────────────────────────────────────────────────────

        return combined
    except Exception:
        return {}


# ── Debug helpers ─────────────────────────────────────────────────────────────

def get_supplement_stats() -> dict:
    """Diagnostic stats shown by /api/options-flow/sectors/debug (legacy compat)."""
    try:
        from data.cache import cache

        no_opts_raw: dict = cache.get(_NO_OPTIONS_CACHE_KEY) or {}
        fresh_snap  = cache.get(_SUPPLEMENT_CACHE_KEY) or {}
        lkg_snap    = cache.get(_SUPPLEMENT_LKG_CACHE_KEY) or {}
        fresh_tickers = fresh_snap.get("ticker_data", {})
        lkg_tickers   = lkg_snap.get("ticker_data", {})
        all_supp      = set(fresh_tickers) | set(lkg_tickers)

        all_theme_syms: set[str] = set()
        try:
            from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE
            for meta in ENRICHED_THEME_RS_UNIVERSE.values():
                for sym in (meta.get("proxy_symbols") or []):
                    all_theme_syms.add(sym.upper())
        except Exception:
            pass

        master_syms = _get_master_tickers()
        theme_only  = all_theme_syms - master_syms

        return {
            "theme_universe_symbol_count":  len(all_theme_syms),
            "master_scan_ticker_count":     len(master_syms),
            "overlap_count":                len(all_theme_syms & master_syms),
            "theme_only_symbol_count":      len(theme_only),
            "no_options_confirmed_count":   len(no_opts_raw),
            "supplement_fresh_count":       len(fresh_tickers),
            "supplement_lkg_count":         len(lkg_tickers),
            "supplement_scanned_count":     len(all_supp),
            "pending_scan_count":           len(theme_only - set(no_opts_raw) - all_supp),
            "supplement_last_scan_at":      fresh_snap.get("last_scan_at"),
            "static_seed_count":            len(_get_static_seeds()),
            "extra_theme_seeds_for_inject": len(get_theme_proxy_symbols_for_supplement()),
        }
    except Exception as exc:
        return {"error": str(exc)}


def get_supplement_debug_info() -> dict:
    """
    Extended debug info for the /api/options-flow/sectors/debug endpoint.
    Includes next 20 pending, last 20 scanned, persistence status.
    """
    try:
        from data.cache import cache

        no_opts_raw: dict = cache.get(_NO_OPTIONS_CACHE_KEY) or {}
        fresh_snap  = cache.get(_SUPPLEMENT_CACHE_KEY) or {}
        lkg_snap    = cache.get(_SUPPLEMENT_LKG_CACHE_KEY) or {}
        fresh_tickers = fresh_snap.get("ticker_data", {})
        lkg_tickers   = lkg_snap.get("ticker_data", {})
        all_supp      = set(fresh_tickers) | set(lkg_tickers)

        all_theme_syms: set[str] = set()
        try:
            from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE
            for meta in ENRICHED_THEME_RS_UNIVERSE.values():
                for sym in (meta.get("proxy_symbols") or []):
                    all_theme_syms.add(sym.upper())
        except Exception:
            pass

        master_syms  = _get_master_tickers()
        pending_syms = sorted(all_theme_syms - master_syms - set(no_opts_raw) - all_supp)
        batch_size   = 20
        cadence_min  = 5

        return {
            "supplement_fresh_count":           len(fresh_tickers),
            "supplement_lkg_count":             len(lkg_tickers),
            "supplement_total_count":           len(all_supp),
            "no_options_confirmed_count":       len(no_opts_raw),
            "pending_count":                    len(pending_syms),
            "disk_lkg_exists":                  _SUPPLEMENT_LKG_DISK_PATH.exists(),
            "disk_lkg_path":                    str(_SUPPLEMENT_LKG_DISK_PATH),
            "last_scanned_symbols":             list(_last_scanned_symbols),
            "next_pending_symbols":             pending_syms[:20],
            "next_scan_at":                     _next_scan_at or None,
            "supplement_last_scan_at":          fresh_snap.get("last_scan_at"),
            "lkg_loaded_at":                    lkg_snap.get("loaded_at"),
            "batch_size":                       batch_size,
            "cadence_seconds":                  cadence_min * 60,
            "estimated_full_coverage_minutes":  round(len(pending_syms) / batch_size * cadence_min, 1),
        }
    except Exception as exc:
        return {"error": str(exc)}


# ── Sectors universe LKG ──────────────────────────────────────────────────────
# A separate LKG that captures ALL theme universe symbol states (including
# symbols covered by the master screener that would otherwise be lost on
# restart). This ensures high coverage immediately after restart.

_SECTORS_LKG_DISK_PATH    = _pathlib.Path(__file__).resolve().parent / "options_sectors_universe_lkg_v1.json"
_SECTORS_LKG_DISK_MAX_AGE = 86400   # 24 h

# Loop tracking (updated by _sectors_fast_backfill_loop via update_sectors_backfill_tracking)
_sectors_backfill_pass_count:      int   = 0
_sectors_backfill_next_at:         float = 0.0
_sectors_backfill_last_pass_at:    float = 0.0
_sectors_backfill_last_batch_syms: list  = []


def update_sectors_backfill_tracking(
    *,
    pass_count:   int   | None = None,
    next_at:      float | None = None,
    last_pass_at: float | None = None,
    batch_syms:   list  | None = None,
) -> None:
    global _sectors_backfill_pass_count, _sectors_backfill_next_at
    global _sectors_backfill_last_pass_at, _sectors_backfill_last_batch_syms
    if pass_count   is not None: _sectors_backfill_pass_count      = pass_count
    if next_at      is not None: _sectors_backfill_next_at         = next_at
    if last_pass_at is not None: _sectors_backfill_last_pass_at    = last_pass_at
    if batch_syms   is not None: _sectors_backfill_last_batch_syms = list(batch_syms)


def get_sectors_backfill_diag() -> dict:
    return {
        "pass_count":      _sectors_backfill_pass_count,
        "next_at":         _sectors_backfill_next_at or None,
        "last_pass_at":    _sectors_backfill_last_pass_at or None,
        "last_batch_syms": list(_sectors_backfill_last_batch_syms),
    }


def save_sectors_universe_lkg_to_disk() -> int:
    """
    Snapshot ALL theme universe symbols from the combined cache into the
    Sectors-specific LKG file.  Called after each complete backfill pass.

    Includes: live, supplement, supplement_lkg, and watchlist_cache rows.
    Confirmed no-options symbols are also included.

    Returns the number of symbols persisted.
    """
    try:
        from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE
    except ImportError:
        return 0

    all_theme_syms: set[str] = {
        sym.upper()
        for meta in ENRICHED_THEME_RS_UNIVERSE.values()
        for sym in (meta.get("proxy_symbols") or [])
    }

    combined = get_combined_ticker_data()
    no_opts  = get_no_options_symbols()
    now      = time.time()
    snapshot: dict[str, dict] = {}

    for sym in all_theme_syms:
        row = combined.get(sym)
        if row:
            # Re-tag as supplement so the row loads as supplement_lkg (stale) on restart
            snapshot[sym] = {**row, "_source": "supplement", "_sectors_lkg_at": now}
        elif sym in no_opts:
            snapshot[sym] = {
                "ticker":          sym,
                "scan_result":     "confirmed_no_options",
                "_source":         "supplement",
                "_sectors_lkg_at": now,
            }
        # Symbols with no data and not confirmed no-options are intentionally
        # NOT persisted — they re-enter the pending queue on next restart.

    if not snapshot:
        return 0

    try:
        payload = {
            "schema_version": 2,
            "ticker_data":    snapshot,
            "saved_at":       now,
            "ticker_count":   len(snapshot),
        }
        tmp = _SECTORS_LKG_DISK_PATH.with_suffix(".json.tmp")
        _SECTORS_LKG_DISK_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_text(_json.dumps(payload, default=str), encoding="utf-8")
        tmp.replace(_SECTORS_LKG_DISK_PATH)
        print(
            f"[SECTORS_LKG] Saved {len(snapshot)}/{len(all_theme_syms)} "
            f"theme universe symbols to disk"
        )
        return len(snapshot)
    except Exception as exc:
        print(f"[SECTORS_LKG] Disk write failed (non-fatal): {exc}")
        return 0


def load_sectors_universe_lkg_from_disk() -> None:
    """
    Load Sectors universe LKG at startup.  Merged into the supplement LKG
    cache (supplement LKG symbols win for any overlap so per-symbol data
    from the supplement loop is not overwritten).

    All rows are tagged _source='supplement_lkg' → _ticker_state() classifies
    them as stale_lkg until the fast backfill loop refreshes them this session.
    """
    if not _SECTORS_LKG_DISK_PATH.exists():
        print("[SECTORS_LKG] No disk LKG — Sectors backfill will build from scratch")
        return
    try:
        now     = time.time()
        payload = _json.loads(_SECTORS_LKG_DISK_PATH.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            print("[SECTORS_LKG] Disk LKG bad format — skipping")
            return

        saved_at = payload.get("saved_at", 0)
        age_s    = int(now - saved_at)
        if age_s > _SECTORS_LKG_DISK_MAX_AGE:
            print(f"[SECTORS_LKG] Disk LKG too old ({age_s}s) — skipping")
            return

        ticker_data: dict = payload.get("ticker_data", {})
        if not ticker_data:
            print("[SECTORS_LKG] Disk LKG empty — skipping")
            return

        # Tag all rows as supplement_lkg → shown as stale_lkg until refreshed
        tagged = {
            sym: {**row, "_source": "supplement_lkg"}
            for sym, row in ticker_data.items()
        }

        from data.cache import cache
        existing_lkg = cache.get(_SUPPLEMENT_LKG_CACHE_KEY) or {}
        existing_td  = dict(existing_lkg.get("ticker_data", {}))
        # Supplement LKG wins for any overlap; Sectors LKG fills in the rest
        merged = {**tagged, **existing_td}
        cache.set(
            _SUPPLEMENT_LKG_CACHE_KEY,
            {"ticker_data": merged, "loaded_at": now, "saved_at": saved_at},
            _SUPPLEMENT_LKG_CACHE_TTL,
        )
        print(
            f"[SECTORS_LKG] Loaded {len(tagged)} theme universe symbols from disk "
            f"(age={age_s}s) → {len(merged)} total in supplement_lkg cache"
        )
    except Exception as exc:
        print(f"[SECTORS_LKG] Disk load failed (non-fatal): {exc}")


def get_sectors_pending_symbols() -> list[str]:
    """
    Return theme universe symbols that need scanning this session.

    Includes:
    - generic_pending  — not in any cache at all
    - stale_lkg        — loaded from LKG (supplement_lkg source), needs refresh

    Excludes:
    - live / supplement / watchlist_cache  (current-session data, already good)
    - confirmed no-options symbols

    Sorted alphabetically for a deterministic rolling cursor.
    """
    try:
        from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE
    except ImportError:
        return []

    all_theme_syms: set[str] = {
        sym.upper()
        for meta in ENRICHED_THEME_RS_UNIVERSE.values()
        for sym in (meta.get("proxy_symbols") or [])
    }

    combined = get_combined_ticker_data()
    no_opts  = get_no_options_symbols()

    pending = []
    for sym in sorted(all_theme_syms):
        if sym in no_opts:
            continue
        row = combined.get(sym)
        if row is None:
            pending.append(sym)                       # generic_pending
        elif row.get("_source") == "supplement_lkg":
            pending.append(sym)                       # stale_lkg — needs refresh
        # live / supplement / watchlist_cache = current-session data → skip
    return pending
