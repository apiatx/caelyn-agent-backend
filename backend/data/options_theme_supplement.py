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
_SUPPLEMENT_LKG_DISK_MAX_AGE = 604800  # 7 days — keeps LKG live across full weekends + holidays
_SUPPLEMENT_LKG_STALE_TTL    = 604800  # 7 days — stale LKG persists until next regular-session scan
_SUPPLEMENT_LKG_FRESH_AGE    = 86400   # rows < 24 h old are "lkg_market_closed"; older = "stale_but_usable"

_NO_OPTIONS_CACHE_KEY  = "options_no_options_tracking:v1"
_NO_OPTIONS_CACHE_TTL  = 86400   # 24 h

_SUPPLEMENT_CACHE_KEY  = "options_theme_supplement_v1"
_SUPPLEMENT_CACHE_TTL  = 14400   # 4 h — accumulates across batches within a session

_SUPPLEMENT_LKG_CACHE_KEY = "options_supplement_lkg_v1"
# 7-day TTL so the in-memory LKG survives market close, overnight, and full weekends.
# Without this the 4 h TTL caused the LKG to expire during off-hours, making options
# data disappear from Watchlist/Sectors views after hours.
_SUPPLEMENT_LKG_CACHE_TTL = 604800  # 7 days (was 4 h)

# ── Confluence extra symbols — watchlist US tickers for supplement scanning ───
# Populated at startup from the active watchlists so the supplement scanner
# covers the full confluence universe, not just the theme proxy universe.
_CONFLUENCE_EXTRA_SYMBOLS: set[str] = set()

def set_confluence_extra_symbols(syms: set[str]) -> None:
    """Register US-listed watchlist tickers for the supplement scanner universe.
    Call at startup (or on watchlist change) with the full set of watchlist
    US tickers.  Foreign-exchange tickers should be excluded by the caller."""
    global _CONFLUENCE_EXTRA_SYMBOLS
    _CONFLUENCE_EXTRA_SYMBOLS = {s.upper() for s in syms if isinstance(s, str)}
    print(
        f"[SUPP] Confluence extra symbols: {len(_CONFLUENCE_EXTRA_SYMBOLS)} "
        f"watchlist US tickers registered for supplement scanning"
    )

# ── Foreign-exchange prefix list (no US options) ─────────────────────────────
_FOREIGN_PREFIXES: frozenset[str] = frozenset((
    "AIM:", "ASX:", "CSE:", "EPA:", "ETR:", "FRA:", "KRX:",
    "LON:", "OSL:", "SHA:", "STO:", "SWX:", "TPE:", "TPEX:",
    "TSX:", "TSXV:", "TYO:", "WSE:", "XSAT:", "OTC:",
))

# Age limit for using sectors-LKG confirmed_no_options entries independently —
# 7 days.  confirmed_no_options is a stable classification; a company rarely
# gains or loses listed options overnight.
_SECTORS_LKG_CNO_MAX_AGE = 604800  # 7 days

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
            try:
                from data.options_instrument_type_service import get_instrument_type as _get_itype
                _itype = _get_itype(sym)
            except Exception:
                _itype = "unknown"
            if _itype == "etf":
                etf_proxies.append(sym)
            elif _itype == "stock":
                stock_proxies.append(sym)
            else:
                if meta.get("proxy_type") == "etf":
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

    Also includes US-listed watchlist tickers registered via
    set_confluence_extra_symbols() so the supplement scanner covers the
    full confluence universe, not just the theme proxy universe.

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

    # Also include US-listed watchlist tickers (registered at startup via
    # set_confluence_extra_symbols) so the supplement scanner covers the
    # full confluence/watchlist universe on the next market session.
    if _CONFLUENCE_EXTRA_SYMBOLS:
        all_syms |= _CONFLUENCE_EXTRA_SYMBOLS

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

    MERGE STRATEGY: fresh supplement rows WIN over any existing disk entry for
    the same symbol (new scan data is always more current).  Old disk entries
    for symbols NOT in the current in-memory batch are PRESERVED so coverage
    accumulates across restarts rather than shrinking each time the server is
    restarted before a full pass completes.

    Entries older than _SUPPLEMENT_LKG_DISK_MAX_AGE are pruned on save.

    EMPTY OVERWRITE GUARD: if market is closed and the new batch is tiny
    compared with the existing disk LKG, the existing LKG is preserved and
    the batch is skipped.  Reason code: PRESERVED_LAST_GOOD_OPTIONS_LKG.
    """
    if not ticker_data:
        return
    # ── Empty overwrite guard ────────────────────────────────────────────────
    # Prevent a handful of closed-market supplement rows from erasing a large
    # Friday-close LKG that has accumulated 500+ tickers.
    try:
        if _SUPPLEMENT_LKG_DISK_PATH.exists():
            _existing = _json.loads(
                _SUPPLEMENT_LKG_DISK_PATH.read_text(encoding="utf-8")
            )
            _existing_count = len((_existing or {}).get("ticker_data", {}))
            _new_count = len(ticker_data)
            if (
                _existing_count > 50
                and _new_count < _existing_count * 0.5
            ):
                try:
                    from data.tradier_market_session import get_session as _gs
                    _sess = _gs()
                except Exception:
                    _sess = "unknown"
                if _sess not in ("regular", "pre", "post"):
                    print(
                        f"[SUPP_LKG] PRESERVED_LAST_GOOD_OPTIONS_LKG: "
                        f"existing={_existing_count} new={_new_count} "
                        f"session={_sess} — skipping overwrite"
                    )
                    return
    except Exception:
        pass  # guard failure is non-fatal — proceed with normal save
    try:
        now = time.time()
        merged: dict = {}

        # Load existing disk entries first (old data as base)
        if _SUPPLEMENT_LKG_DISK_PATH.exists():
            try:
                old_payload = _json.loads(
                    _SUPPLEMENT_LKG_DISK_PATH.read_text(encoding="utf-8")
                )
                if isinstance(old_payload, dict):
                    # Age controls status labels only (stale_long_term) — NEVER eligibility.
                    # Always load the full baseline so a partial save cannot discard
                    # tickers that weren't scanned in the current batch (T003 fix).
                    merged = dict(old_payload.get("ticker_data", {}))
            except Exception:
                pass  # corrupt file — start fresh

        # Fresh supplement rows override matching old entries
        merged.update(ticker_data)

        _total_count = len(merged)
        _fresh_count = len(ticker_data)
        _prev_count  = _total_count - _fresh_count  # approx old baseline size

        # ── Real safe-promotion gate ───────────────────────────────────────────
        # "Promoted" means this snapshot is authoritative for the supplement
        # universe — not just an incremental delta.  The check is based on:
        #   (a) structural validity: every row must have a ticker key,
        #   (b) minimum universe coverage vs. the expected supplement universe,
        #   (c) provider health: fresh batch must not be all-failures,
        #   (d) coverage vs. prior snapshot: fresh batch covers ≥10% of baseline
        #       OR the baseline is small (no rollover from a nearly empty state).
        # A snapshot failing any check is recorded as PARTIAL (promoted=False)
        # with the precise rejection reason logged for diagnostics.

        # (a) Structural validity — every merged entry must have a ticker key
        _struct_ok = all(isinstance(k, str) and k for k in merged)

        # (b) Universe coverage — compare vs. a reasonable expected size.
        # Supplement universe target: at least 80 distinct tickers total.
        _MIN_UNIVERSE = 80

        # (c) Provider health — reject promotion if fresh batch is all-error rows.
        # A row is an "error" if scan_result is one of the hard-fail sentinels AND
        # data_available is explicitly False.
        _HARD_FAIL = frozenset({
            "provider_error", "budget_pre_check_chain", "deferred_retry",
        })
        _all_errors = _fresh_count > 0 and all(
            v.get("scan_result") in _HARD_FAIL and v.get("data_available") is False
            for v in ticker_data.values()
        )

        # (d) Coverage vs. prior snapshot (sparse-batch guard)
        _SPARSE_RATIO = 0.10
        _sparse = (
            _prev_count >= 10
            and _fresh_count < _prev_count * _SPARSE_RATIO
        )

        _promoted      = False
        _promo_reason: str | None = None

        if not _struct_ok:
            _promo_reason = "INVALID_STRUCTURE: one or more rows missing ticker key"
        elif _all_errors:
            _promo_reason = (
                f"ALL_PROVIDER_ERRORS: {_fresh_count} fresh rows all hard-fail — "
                "provider likely down, preserving prior LKG"
            )
        elif _total_count < _MIN_UNIVERSE:
            _promo_reason = (
                f"BELOW_MIN_UNIVERSE total={_total_count} min={_MIN_UNIVERSE}"
            )
        elif _sparse:
            _promo_reason = (
                f"SPARSE_BATCH prev={_prev_count} fresh={_fresh_count} "
                f"ratio={_fresh_count/max(1,_prev_count):.2f} < {_SPARSE_RATIO}"
            )
        else:
            _promoted = True

        if _promo_reason:
            print(f"[SUPP_LKG] PROMOTION_PENDING: {_promo_reason}")

        payload = {
            "ticker_data":              merged,
            "saved_at":                 now,
            "ticker_count":             _total_count,
            "fresh_count":              _fresh_count,
            "promoted":                 _promoted,
            "promotion_rejection_reason": _promo_reason,
        }
        tmp = _SUPPLEMENT_LKG_DISK_PATH.with_suffix(".json.tmp")
        _SUPPLEMENT_LKG_DISK_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_text(_json.dumps(payload, default=str), encoding="utf-8")
        tmp.replace(_SUPPLEMENT_LKG_DISK_PATH)
        _promo_label = "PROMOTED" if _promoted else "PARTIAL"
        print(f"[SUPP_LKG] {_promo_label}: {_total_count} tickers persisted ({_fresh_count} fresh)")
    except Exception as exc:
        print(f"[SUPP_LKG] Disk write failed (non-fatal): {exc}")


def _load_supplement_lkg_from_disk() -> None:
    """
    Load supplement LKG from disk at startup.  Synchronous — call before
    any request is served so the sectors endpoint has data immediately.

    Rows are tagged _source='supplement_lkg' to distinguish from fresh
    session scans.  Loaded into _SUPPLEMENT_LKG_CACHE_KEY.

    Age policy (weekend-resilient):
      < 24 h  → lkg_market_closed  (same session, fresh)
      24–96 h → stale_but_usable   (Friday-close data on Sat/Sun/Mon restart)
      > 96 h  → rejected            (too stale to serve)

    Uses a 96 h in-memory TTL for stale loads so data survives until Monday
    market open when the live scan will overwrite it.
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
        # No age-based rejection — a structurally valid snapshot is always served.
        # Age controls only the status label; never blocks availability.
        ticker_data: dict = payload.get("ticker_data", {})
        if not ticker_data:
            print("[SUPP_LKG] Disk LKG: empty ticker_data — skipping")
            return

        # Age-based status label (informational only — does NOT gate availability)
        #   lkg_market_closed : < 24 h (same session or overnight)
        #   stale_but_usable  : 24 h – 7 days (weekend / short holiday)
        #   stale_long_term   : > 7 days (extended outage / multi-holiday window)
        _FRESH = _SUPPLEMENT_LKG_FRESH_AGE    # 24 h
        _WEEK  = 604800                        # 7 days
        _snap_status = (
            "lkg_market_closed" if age_s < _FRESH
            else "stale_but_usable" if age_s < _WEEK
            else "stale_long_term"
        )
        tagged = {
            sym: {
                **row,
                "_source":          "supplement_lkg",
                "_snapshot_status": _snap_status,
                "_lkg_age_s":       age_s,
            }
            for sym, row in ticker_data.items()
        }
        # Always use full TTL — LKG stays in memory until overwritten by live data
        _ttl = _SUPPLEMENT_LKG_CACHE_TTL
        from data.cache import cache
        cache.set(
            _SUPPLEMENT_LKG_CACHE_KEY,
            {"ticker_data": tagged, "loaded_at": now, "saved_at": saved_at},
            _ttl,
        )
        print(
            f"[SUPP_LKG] Loaded {len(tagged)} supplement tickers from disk "
            f"(age={age_s//3600:.0f}h, status={_snap_status}, ttl={_ttl//3600:.0f}h)"
        )
    except Exception as exc:
        print(f"[SUPP_LKG] Disk load failed (non-fatal): {exc}")


def _seed_no_options_from_sectors_lkg() -> None:
    """
    Inject confirmed_no_options tickers from the sectors universe LKG into the
    in-memory no-options tracking cache.  Called at startup so the supplement
    scanner excludes known non-optionable tickers on the very first pass and
    V4 can classify them correctly (confirmed_no_options earns confidence
    points; not_scanned does not).

    Uses _SECTORS_LKG_CNO_MAX_AGE (7 days) rather than the general 96 h limit
    because confirmed_no_options is a stable classification — companies rarely
    gain or lose listed options overnight.
    """
    if not _SECTORS_LKG_DISK_PATH.exists():
        return
    try:
        now = time.time()
        raw = _json.loads(_SECTORS_LKG_DISK_PATH.read_text(encoding="utf-8"))
        age_s = now - (raw.get("saved_at") or 0)
        if age_s > _SECTORS_LKG_CNO_MAX_AGE:
            print(
                f"[SUPP_LKG] Sectors LKG confirmed_no_options seed skipped "
                f"(age={age_s/3600:.1f}h > {_SECTORS_LKG_CNO_MAX_AGE/3600:.0f}h)"
            )
            return
        cno_rows = {
            sym.upper(): row
            for sym, row in (raw.get("ticker_data") or {}).items()
            if isinstance(row, dict) and row.get("scan_result") == "confirmed_no_options"
        }
        if not cno_rows:
            return
        from data.cache import cache
        existing = cache.get(_NO_OPTIONS_CACHE_KEY) or {}
        # existing (live session) entries win; sectors LKG only fills gaps
        merged = {**cno_rows, **existing}
        cache.set(_NO_OPTIONS_CACHE_KEY, merged, _NO_OPTIONS_CACHE_TTL)
        print(
            f"[SUPP_LKG] Seeded {len(cno_rows)} confirmed_no_options from sectors LKG "
            f"(age={age_s/3600:.1f}h); {len(merged)} total in no-opts tracking cache"
        )
    except Exception as exc:
        print(f"[SUPP_LKG] confirmed_no_options seed failed (non-fatal): {exc}")


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

    IMPORTANT: Only writes confirmed_no_options during active market sessions
    (premarket / regular / postmarket).  During off_hours or weekend, Tradier
    commonly returns empty chains for fully optionable stocks — trusting those
    results would cause false confirmed_no_options for tickers like AKAM, DELL.
    """
    if not expiry_cache:
        return
    try:
        from data.cache import cache
        from data.tradier_market_session import get_session as _get_session
        # Only confirm "no options" during regular market hours (09:30-16:00 ET).
        # Options chains are only tradeable during regular session; pre-market,
        # post-market, off-hours and weekends all return empty chains for tickers
        # that are fully optionable — trusting them causes false confirmed_no_options.
        if _get_session() != "regular":
            return
        existing: dict = cache.get(_NO_OPTIONS_CACHE_KEY) or {}
        now     = time.time()
        # Also load supplement + LKG caches once to protect tickers with
        # existing options data from being falsely confirmed as no-options.
        _fresh_td = (cache.get(_SUPPLEMENT_CACHE_KEY) or {}).get("ticker_data", {})
        _lkg_td   = (cache.get(_SUPPLEMENT_LKG_CACHE_KEY) or {}).get("ticker_data", {})
        changed = False
        for sym, entry in expiry_cache.items():
            if not isinstance(entry, (list, tuple)) or len(entry) < 1:
                continue
            exps = entry[0]
            if isinstance(exps, list) and len(exps) == 0:
                if sym not in existing:
                    # Extra guard: if the ticker already has supplement data
                    # (it was seen as optionable in a prior scan), don't add
                    # it — a single empty-expiry scan is not enough evidence.
                    if _fresh_td.get(sym) or _lkg_td.get(sym):
                        continue
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

    Auto-rehydrate: if the LKG in-memory cache has expired (e.g. after a very
    long off-hours period that exceeded the 7-day TTL), reload from disk before
    falling through to an empty set.  This ensures the LKG is always available
    as long as the disk file is within its own max-age window.
    """
    try:
        from data.cache import cache
        # Start with LKG (disk-loaded at startup, supplement_lkg tagged)
        lkg_snap = cache.get(_SUPPLEMENT_LKG_CACHE_KEY)
        if lkg_snap is None:
            # In-memory LKG expired — attempt disk rehydration before serving empty
            _load_supplement_lkg_from_disk()
            lkg_snap = cache.get(_SUPPLEMENT_LKG_CACHE_KEY) or {}
        combined = dict(lkg_snap.get("ticker_data", {}))
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
        fresh_snap = cache.get(_SUPPLEMENT_CACHE_KEY) or {"ticker_data": {}, "cached_at": 0}
        ticker_data: dict = dict(fresh_snap.get("ticker_data", {}))
        # Also read the LKG cache once so we can protect tickers that only exist
        # there (e.g. after a restart, before the first backfill batch completes).
        _lkg_snap    = cache.get(_SUPPLEMENT_LKG_CACHE_KEY) or {}
        _lkg_td: dict = _lkg_snap.get("ticker_data", {})
        now = time.time()
        _COVERAGE_ONLY = frozenset({"neutral_no_unusual_flow", "optionable_pending_chain"})
        for row in results:
            sym = (row.get("ticker") or "").upper()
            if not sym:
                continue
            new_scan_result = row.get("scan_result", "")

            # ── Guard 1: never write confirmed_no_options for a ticker that has
            # ANY existing data (fresh supplement or LKG).  Off-market Tradier calls
            # return empty chains for optionable stocks, producing false positives.
            # confirmed_no_options should only tag genuinely option-less tickers on
            # their FIRST scan ever (no prior record in any cache layer).
            if new_scan_result == "confirmed_no_options":
                if ticker_data.get(sym) or _lkg_td.get(sym):
                    continue   # preserve existing row, discard false positive

            # ── Guard 2: never overwrite a row with real premium data with a
            # zero-premium neutral/pending coverage row.  Coverage rows tag state
            # only — they must not erase confirmed unusual-flow data.
            if new_scan_result in _COVERAGE_ONLY:
                cur = ticker_data.get(sym) or _lkg_td.get(sym)
                if cur:
                    existing_prem = (cur.get("premium") or 0.0)
                    if existing_prem > 0 or cur.get("call_premium") or cur.get("put_premium"):
                        continue   # keep the existing row with real flow data

            # ── Guard 3: non-destructive merge — preserve existing rich fields ──────
            # A new supplement_v2 row carries OI/IV/EM/score.  However a re-scan
            # during off-peak can return None for these (e.g. spot price lookup
            # missed) while the existing LKG row has good values.  Never overwrite
            # a populated rich field with None.
            # Rich fields added by supplement_v2: call_oi, put_oi, total_oi,
            # call_iv, put_iv, combined_iv, iv_skew, expected_move_dollars,
            # expected_move_pct, options_score, options_signal, underlying_price.
            _RICH_FIELDS = (
                "call_oi", "put_oi", "total_oi",
                "call_iv", "put_iv", "combined_iv", "iv_skew",
                "expected_move_dollars", "expected_move_pct",
                "options_score", "options_signal",
                "underlying_price",
            )
            _existing = ticker_data.get(sym) or _lkg_td.get(sym)
            if _existing:
                _merged = {**row, "_source": "supplement", "_cached_at": now}
                for _rf in _RICH_FIELDS:
                    if _merged.get(_rf) is None and _existing.get(_rf) is not None:
                        _merged[_rf] = _existing[_rf]
                ticker_data[sym] = _merged
            else:
                ticker_data[sym] = {**row, "_source": "supplement", "_cached_at": now}
        cache.set(
            _SUPPLEMENT_CACHE_KEY,
            {"ticker_data": ticker_data, "cached_at": now, "last_scan_at": now},
            _SUPPLEMENT_CACHE_TTL,
        )
        # Invalidate sectors cache so the next request rebuilds from fresh data.
        # Without this, the 60-second sectors cache serves stale all-null data
        # even after the supplement loop has populated real options rows.
        try:
            cache.delete("options_flow_sectors:v1")
        except Exception:
            pass
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

        import time as _time_supp
        _now_supp = _time_supp.time()

        master_snap = (
            cache.get("options_master_screener_v1")
            or cache.get("options_master_lkg_v1")
        )
        combined: dict[str, dict] = {}

        if master_snap:
            _ms_cached_at = master_snap.get("cached_at") or 0
            _ms_age       = _now_supp - float(_ms_cached_at) if _ms_cached_at else 0
            _ms_status    = "available_live" if master_snap.get("source") != "disk_lkg" else "lkg_market_closed"
            for row in master_snap.get("tickers", []):
                sym = (row.get("ticker") or "").upper()
                if sym:
                    combined[sym] = {
                        **row,
                        "_source":           "live",
                        "_snapshot_status":  _ms_status,
                        "_cached_at":        _ms_cached_at,
                        "_lkg_age_s":        round(_ms_age),
                    }

        for sym, row in get_supplement_data_by_ticker().items():
            if sym not in combined:
                _src = row.get("_source") or "supplement"
                _supp_status = "available_cached" if "lkg" in _src else "available_live"
                combined[sym] = {
                    **row,
                    "_snapshot_status": _supp_status,
                }

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
                        combined[sym] = {
                            **row,
                            "_source":          "watchlist_cache",
                            "_snapshot_status": "available_cached",
                        }
        except Exception:
            pass
        # ─────────────────────────────────────────────────────────────────────

        # ── Sectors-LKG confirmed-no-options layer ────────────────────────────
        # Inject tickers confirmed as no-options from the sectors universe LKG
        # so the options-alignment / V4 pipeline classifies them as
        # confirmed_no_options (KNOWN state, earns confidence) rather than
        # not_scanned (UNKNOWN state, lowers confidence).
        # Runs unconditionally — NOT gated on combined being empty.
        # No age-based rejection — structurally valid confirmed_no_options rows
        # are kept until replaced by a successful live scan.
        try:
            if _SECTORS_LKG_DISK_PATH.exists():
                import json as _jcno
                _sec_raw = _jcno.loads(_SECTORS_LKG_DISK_PATH.read_text(encoding="utf-8"))
                _sec_age = _now_supp - (_sec_raw.get("saved_at") or 0)
                _cno_added = 0
                for sym, row in (_sec_raw.get("ticker_data") or {}).items():
                    sym = sym.upper()
                    if sym in combined:
                        continue
                    if isinstance(row, dict) and row.get("scan_result") == "confirmed_no_options":
                        combined[sym] = {
                            **row,
                            "_source":          "sectors_lkg_no_options",
                            "_snapshot_status": "confirmed_no_options",
                        }
                        _cno_added += 1
                if _cno_added:
                    print(
                        f"[OPTIONS_COMBINED] Sectors-LKG confirmed_no_options layer: "
                        f"+{_cno_added} tickers (age={_sec_age/3600:.1f}h)"
                    )
        except Exception:
            pass
        # ─────────────────────────────────────────────────────────────────────

        # ── Disk LKG per-ticker fallback ─────────────────────────────────────
        # For any ticker MISSING from all in-memory layers, fill in from disk
        # snapshots.  This is a PER-TICKER fallback — a small in-memory master
        # snapshot (e.g. 3 tickers) never suppresses hundreds of valid supplement
        # disk LKG rows.  The `if not combined:` global gate is intentionally
        # removed: per-ticker is the correct merge unit.
        #
        # No age-based rejection — structurally valid disk data is always served.
        # Age controls only the status label:
        #   lkg_market_closed : < 24 h
        #   stale_but_usable  : 24 h – 7 days
        #   stale_long_term   : > 7 days (extended outage / provider failure)
        try:
            import pathlib as _pl_supp, json as _js_supp

            # Layer A: supplement disk LKG (~600+ tickers)
            if _SUPPLEMENT_LKG_DISK_PATH.exists():
                try:
                    _sd_raw  = _js_supp.loads(_SUPPLEMENT_LKG_DISK_PATH.read_text())
                    _sd_sa   = _sd_raw.get("saved_at") or 0
                    _sd_age  = _now_supp - float(_sd_sa) if _sd_sa else 0
                    _sd_st   = (
                        "lkg_market_closed" if _sd_age < 86400
                        else "stale_but_usable" if _sd_age < 604800
                        else "stale_long_term"
                    )
                    _sd_added = 0
                    for sym, row in (_sd_raw.get("ticker_data") or {}).items():
                        sym = sym.upper()
                        if sym and sym not in combined:
                            combined[sym] = {
                                **row,
                                "_source":          "disk_lkg_supplement",
                                "_snapshot_status": _sd_st,
                                "_cached_at":       _sd_sa,
                                "_lkg_age_s":       round(_sd_age),
                            }
                            _sd_added += 1
                    if _sd_added:
                        print(
                            f"[OPTIONS_COMBINED] Supplement disk per-ticker fill: +{_sd_added} tickers "
                            f"(age={_sd_age/3600:.1f}h, status={_sd_st})"
                        )
                except Exception as _sd_exc:
                    print(f"[OPTIONS_COMBINED] Supplement disk fallback failed: {_sd_exc}")

            # Layer B: master screener disk LKG (~19 tickers)
            _master_disk = _pl_supp.Path(__file__).parent.parent / "data" / "options_master_lkg_v1.json"
            if _master_disk.exists():
                try:
                    _disk_raw   = _js_supp.loads(_master_disk.read_text())
                    _disk_ca    = _disk_raw.get("cached_at") or 0
                    _disk_age   = _now_supp - float(_disk_ca) if _disk_ca else 0
                    _disk_st    = (
                        "lkg_market_closed" if _disk_age < 86400
                        else "stale_but_usable" if _disk_age < 604800
                        else "stale_long_term"
                    )
                    _disk_as_of = _disk_raw.get("updated_at") or _disk_raw.get("cached_at")
                    _disk_added = 0
                    for row in _disk_raw.get("tickers", []):
                        sym = (row.get("ticker") or "").upper()
                        if sym and sym not in combined:
                            combined[sym] = {
                                **row,
                                "_source":          "disk_lkg",
                                "_snapshot_status": _disk_st,
                                "_cached_at":       _disk_ca,
                                "_lkg_age_s":       round(_disk_age),
                                "_as_of":           _disk_as_of,
                            }
                            _disk_added += 1
                    if _disk_added:
                        print(
                            f"[OPTIONS_COMBINED] Master disk per-ticker fill: +{_disk_added} tickers "
                            f"(age={_disk_age/3600:.1f}h, status={_disk_st})"
                        )
                except Exception as _disk_exc:
                    print(f"[OPTIONS_COMBINED] Master disk fallback failed: {_disk_exc}")
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
_SECTORS_LKG_DISK_MAX_AGE = 345600  # 96 h — weekend-resilient, same policy as supplement LKG

# Loop tracking (updated by _sectors_fast_backfill_loop via update_sectors_backfill_tracking)
_sectors_backfill_pass_count:      int   = 0
_sectors_backfill_next_at:         float = 0.0
_sectors_backfill_last_pass_at:    float = 0.0
_sectors_backfill_last_batch_syms: list  = []

# ── Sectors page active tracking ─────────────────────────────────────────────
# When the Sectors page is visited, register_sectors_active() is called.
# The backfill loop reads is_sectors_active() each cycle and uses a larger
# batch + shorter sleep + the "sectors" budget lane for priority refresh.
_SECTORS_ACTIVE_TS:  float = 0.0
_SECTORS_ACTIVE_TTL: int   = 300   # 5 minutes since last page visit

# Diagnostics updated by the backfill loop each cycle.
_SECTORS_REFRESH_DIAG: dict = {
    "sectors_active":                        False,
    "sectors_active_since":                  None,
    "sectors_refresh_queue_depth":           0,
    "sectors_refresh_inflight":              0,
    "sectors_refresh_completed_this_session": 0,
    "sectors_refresh_calls_last_60s":        0,
    "sectors_refresh_deferred_count":        0,
    "sectors_refresh_eta_seconds":           None,
    "sectors_lkg_rows_loaded":               0,
    "sectors_rows_with_premium":             0,
    "sectors_pending_no_lkg":               0,
}


def register_sectors_active() -> None:
    """
    Record that the Sectors page was just visited.

    The backfill loop calls is_sectors_active() each cycle; when True it
    switches to priority mode: larger batches, shorter sleep, sectors lane.
    """
    global _SECTORS_ACTIVE_TS
    _SECTORS_ACTIVE_TS = time.time()


def is_sectors_active() -> bool:
    """Return True if the Sectors page was visited within the last 5 minutes."""
    return time.time() - _SECTORS_ACTIVE_TS < _SECTORS_ACTIVE_TTL


def update_sectors_refresh_diag(updates: dict) -> None:
    """Merge *updates* into the sectors refresh diagnostics dict."""
    global _SECTORS_REFRESH_DIAG
    _SECTORS_REFRESH_DIAG.update(updates)


def get_sectors_active_diag() -> dict:
    """Return a copy of the sectors refresh diagnostics dict."""
    return dict(_SECTORS_REFRESH_DIAG)


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

    # ── Prior-session promotion (off-hours save) ──────────────────────────────
    # After the regular session ends, promote interval_* fields (delta since
    # last snapshot) to prior_session_* so they survive restarts and are shown
    # as "last session" data.  Only promotes when session is NOT "regular" and
    # prior_session_* fields not already present.
    _should_promote_ps = False
    try:
        from data.tradier_market_session import get_session as _gs_lkg
        _should_promote_ps = _gs_lkg() not in ("regular",)
    except Exception:
        pass

    _INT_TO_PS = [
        ("interval_ask_premium",                   "prior_session_ask_premium"),
        ("interval_bid_premium",                   "prior_session_bid_premium"),
        ("interval_midpoint_unknown_premium",       "prior_session_midpoint_premium"),
        ("interval_ask_premium_pct",               "prior_session_ask_premium_pct"),
        ("interval_bid_premium_pct",               "prior_session_bid_premium_pct"),
        ("interval_midpoint_unknown_premium_pct",  "prior_session_midpoint_premium_pct"),
    ]

    import datetime as _dt_lkg
    _today_str = _dt_lkg.datetime.utcfromtimestamp(now).strftime("%Y-%m-%d")

    for sym in all_theme_syms:
        row = combined.get(sym)
        if row:
            _out = {**row, "_source": "supplement", "_sectors_lkg_at": now}
            if _should_promote_ps:
                for _ik, _pk in _INT_TO_PS:
                    _v = row.get(_ik)
                    if _v is not None and _out.get(_pk) is None:
                        _out[_pk] = _v
                if _out.get("prior_session_date") is None:
                    _out["prior_session_date"] = (
                        row.get("prior_session_date") or _today_str
                    )
                if _out.get("prior_session_saved_at") is None:
                    _out["prior_session_saved_at"] = now
            snapshot[sym] = _out
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
        # No age-based rejection — structurally valid snapshot always loaded.
        # Age controls only the status label (informational).

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


# ── Priority queue for newly required theme symbols ───────────────────────────
# When a symbol is added to the Options Flow required universe mid-session
# (e.g. via a theme ticker override write), it starts as generic_pending.
# Without prioritisation it lands at an arbitrary alphabetical position in the
# missing_syms tier and may take the full backfill cycle (up to ~30 min in
# background mode) before being scanned.
#
# add_high_priority_symbols() marks symbols for front-of-queue placement.
# get_sectors_pending_symbols() puts high-priority AND pending symbols first.
# clear_scanned_high_priority() removes symbols after the backfill loop scans them.
#
# Dict value is the add_at timestamp for age diagnostics.
_HIGH_PRIORITY_SYMBOLS: dict[str, float] = {}

_PRIORITY_FILE = _pathlib.Path(__file__).resolve().parent / "options_priority_symbols.json"


def _load_priority_disk() -> None:
    """Load persisted high-priority symbols from disk at startup."""
    global _HIGH_PRIORITY_SYMBOLS
    try:
        if _PRIORITY_FILE.exists():
            with open(_PRIORITY_FILE) as f:
                d = _json.load(f)
            now = time.time()
            cutoff = now - 3600  # ignore entries older than 1 hour
            for sym, ts in (d.get("symbols") or {}).items():
                if isinstance(ts, (int, float)) and ts > cutoff:
                    if sym.upper() not in _HIGH_PRIORITY_SYMBOLS:
                        _HIGH_PRIORITY_SYMBOLS[sym.upper()] = ts
    except Exception:
        pass


def _save_priority_disk() -> None:
    """Atomically persist the current high-priority set to disk."""
    import os as _os
    try:
        payload = {"symbols": dict(_HIGH_PRIORITY_SYMBOLS), "saved_at": time.time()}
        tmp = str(_PRIORITY_FILE) + ".tmp"
        with open(tmp, "w") as f:
            _json.dump(payload, f)
        _os.replace(tmp, _PRIORITY_FILE)
    except Exception:
        pass


_load_priority_disk()


def add_high_priority_symbols(symbols: list[str]) -> None:
    """
    Mark symbols as high-priority for the next backfill pass.

    Called when a symbol is added to the Options Flow required universe
    mid-session (e.g. theme ticker override write).  Only marks symbols that
    are not yet scanned — symbols already in the supplement/live cache are
    ignored (they already have data).

    Safe to call from any sync or async context (pure in-memory write).
    """
    now = time.time()
    added = []
    for sym in symbols:
        s = sym.upper()
        if s not in _HIGH_PRIORITY_SYMBOLS:
            _HIGH_PRIORITY_SYMBOLS[s] = now
            added.append(s)
    if added:
        print(f"[PRIORITY_QUEUE] Marked {len(added)} symbol(s) high-priority: {added[:5]}")
        _save_priority_disk()


def clear_scanned_high_priority(symbols: list[str]) -> None:
    """
    Remove symbols from the high-priority set after the backfill loop scans them.
    Call this after every successful scan batch so the priority dict stays current.
    """
    cleared = False
    for sym in symbols:
        if _HIGH_PRIORITY_SYMBOLS.pop(sym.upper(), None) is not None:
            cleared = True
    if cleared:
        _save_priority_disk()


def get_priority_queue_diag() -> dict:
    """
    Return diagnostics for the priority queue — how many symbols are
    queued as high-priority and waiting for a scan.
    """
    pending = get_sectors_pending_symbols()
    pending_set = set(pending)
    now = time.time()
    hi_pending = [s for s in sorted(_HIGH_PRIORITY_SYMBOLS) if s in pending_set]
    oldest_ts = min(
        (_HIGH_PRIORITY_SYMBOLS[s] for s in hi_pending), default=None
    )
    return {
        "pending_total":                len(pending),
        "high_priority_pending":        len(hi_pending),
        "high_priority_pending_sample": hi_pending[:10],
        "high_priority_total_marked":   len(_HIGH_PRIORITY_SYMBOLS),
        "oldest_pending_age_seconds":   int(now - oldest_ts) if oldest_ts else None,
    }


def get_sectors_pending_symbols() -> list[str]:
    """
    Return theme universe symbols that need scanning this session, in priority
    order so the backfill loop drains the most important gaps first.

    Priority order (highest → lowest):
      1. high_priority_pending — symbols explicitly marked via add_high_priority_symbols()
                                 that are also generic_pending (not in any cache).
                                 These are symbols newly added to the theme universe
                                 mid-session that would otherwise wait at an arbitrary
                                 alphabetical position.
      2. generic_pending       — not in any cache at all (missing_data).
                                 Must be scanned before stale rows are refreshed.
      3. stale_lkg             — loaded from LKG (supplement_lkg source), needs refresh.
                                 Real prior-session data exists, but it should be freshed.

    Excludes:
    - live / supplement / watchlist_cache  (current-session data, already good)
    - confirmed no-options symbols

    Within each priority tier (except high_priority_pending), symbols are sorted
    alphabetically for a deterministic rolling cursor so every symbol in that
    tier is visited in a predictable order.
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

    missing_syms:  list[str] = []   # generic_pending — high priority
    stale_lkg_syms: list[str] = []  # supplement_lkg with real data — lower priority

    for sym in sorted(all_theme_syms):
        if sym in no_opts:
            continue
        row = combined.get(sym)
        if row is None:
            missing_syms.append(sym)               # generic_pending — FIRST
        elif row.get("_source") == "supplement_lkg":
            # Split LKG rows by data quality.
            # Rows with real scanned data (sectors_chain_summarized,
            # neutral_no_unusual_flow, or positive premium) go to stale_lkg_syms
            # so they show as cached_data until refreshed.
            # Rows that are coverage-only placeholders (deferred_retry,
            # optionable_pending_chain with zero premium) go to missing_syms —
            # they have no real data and must be filled by the backfill loop first.
            if _lkg_has_real_data(row):
                stale_lkg_syms.append(sym)         # stale_lkg — SECOND
            else:
                missing_syms.append(sym)           # placeholder, no real data — FIRST
        elif row.get("_source") == "live":
            # Master screener row: if it only has unusual-flow scope it has NO canonical
            # net-flow snapshot (_suppress_nf_premiums=True → nf_snapshot_pending=True).
            # These symbols need a chain-summarizer scan to get real call/put premium data.
            _scope = row.get("expiration_scope", "")
            if _scope == "top_unusual_contracts" or not _scope:
                missing_syms.append(sym)           # needs canonical net-flow scan — FIRST
        # supplement / watchlist_cache = current good data → skip

    # ── Priority queue ordering ────────────────────────────────────────────────
    # Tier 0 (very front): high-priority symbols OUTSIDE the theme universe.
    #   These are simple watchlist tickers (no ":" prefix) that were queued via
    #   add_high_priority_symbols() but are not in ENRICHED_THEME_RS_UNIVERSE.
    #   Without this tier they are silently invisible to the backfill loop
    #   because the loop only iterates over all_theme_syms.
    #   Examples: MSFT, NVDA, SMCI when not yet registered in any theme.
    #
    # Tier 1 (front of theme missing): high-priority inside theme universe.
    # Tier 2: other generic_pending theme symbols.
    # Tier 3: stale_lkg theme symbols.
    extra_hi: list[str] = []
    if _HIGH_PRIORITY_SYMBOLS:
        hi_set = set(_HIGH_PRIORITY_SYMBOLS.keys())

        # Tier 0 — high-priority outside theme universe (alphabetical for determinism)
        extra_hi = [
            s for s in sorted(hi_set)
            if s not in all_theme_syms
            and s not in no_opts
            and combined.get(s) is None
            and ":" not in s          # never queue prefixed foreign/OTC symbols
        ]

        # Tier 1 — high-priority inside theme universe, hoisted to front
        hi_missing    = [s for s in missing_syms if s in hi_set]
        other_missing = [s for s in missing_syms if s not in hi_set]
        missing_syms  = hi_missing + other_missing

    return extra_hi + missing_syms + stale_lkg_syms


def _lkg_has_real_data(row: dict) -> bool:
    """
    Return True if this supplement_lkg row contains real scanned options data
    (not a coverage-only placeholder written by the old supplement loop).

    Real data: either a known complete-scan result or positive premium.
    Placeholders: deferred_retry, optionable_pending_chain with zero premium.
    """
    scan_result = row.get("scan_result") or ""
    # Explicitly deferred → no real data
    if scan_result == "deferred_retry":
        return False
    # Known real-scan results — even if premium=0, the scan completed
    if scan_result in ("sectors_chain_summarized", "neutral_no_unusual_flow"):
        return True
    # For other/unknown scan_result values, check for actual premium data
    call_p = row.get("call_premium")
    put_p  = row.get("put_premium")
    if call_p is not None and put_p is not None and (call_p + put_p) > 0:
        return True
    prem = row.get("premium") or 0.0
    cpct = row.get("call_flow_pct")
    ppct = row.get("put_flow_pct")
    if prem > 0 and cpct is not None and ppct is not None:
        return True
    return False
