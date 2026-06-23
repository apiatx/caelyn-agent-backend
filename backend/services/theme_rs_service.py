"""
Themes by Relative Strength — production-hardened canonical service (v3).

Exposes:
    get_theme_rs_data(timeframe, force) → dict
    warmup_theme_rs()                  → call once at startup (non-blocking)

Provider hierarchy (no LLM calls):
  1D quote:        Tradier batch → Finnhub individual fallback
  7D/30D/YTD/1Y/5Y: FMP stable historical-price-eod date-ranged (last 1400 days)
                   → Tradier daily history fallback
                   → yfinance emergency fallback

Leader/laggard universe (per theme, dynamic):
  1. ETF holdings from primary proxy ETF (etf_holdings_service, 7-day cache)
  2. X/Grok consensus tickers from disk snapshot (read-only, no new calls)
  3. Static candidate_symbols as last-resort fallback seeds

DRAM special handling:
  memory_storage tries DRAM first for theme return; falls back to SMH/SOXX.

State:
  active / emerging / neutral / weakening / dead_zone
  + state_reason human-readable text for every theme.

Cache & freshness:
  key:  themes:relative_strength:v1:{tf}
  TTL:  1D       → 60s market hours / 3600s off-hours
        7D/5Y+ → 900s market hours / 3600s off-hours
  disk: backend/data/themes_rs_lkg.json (atomic write, never overwrite with bad data)

Refresh safety:
  - Per-timeframe asyncio.Lock prevents duplicate concurrent refreshes.
  - Stale-while-revalidate: cache miss with valid LKG → serve LKG immediately,
    kick background refresh. No user-facing cold wait after first load.
  - Background warmup loop: 1D every ~60s, 7D/30D/YTD/1Y/5Y every ~15min.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import statistics
import time
from datetime import datetime, date, timezone, timedelta
from pathlib import Path
from typing import Optional

import httpx

from data.cache import cache
from data.fmp_utils import fmp_hist_ttl
from services.theme_merge_layer import (
    ENRICHED_THEME_RS_UNIVERSE as THEME_RS_UNIVERSE,
    ENRICHED_ALL_PROXY_SYMBOLS as ALL_PROXY_SYMBOLS,
    ENRICHED_ALL_CANDIDATE_SYMBOLS as ALL_CANDIDATE_SYMBOLS,
)
from services.sector_rotation.analytics import _pct_change, _ytd_change, _sma
from services.sector_rotation.providers import (
    fetch_etf_history,          # yfinance executor (emergency fallback)
    _tradier_quotes_batch,
    _finnhub_quote_single,
    _tradier_key,
    _tradier_base,
)


# ── Constants ──────────────────────────────────────────────────────────────────

_CACHE_KEY       = "themes:relative_strength:v1"
_LKG_PATH        = Path(__file__).parent.parent / "data" / "themes_rs_lkg.json"
# Compact disk file for 1D intraday curves — survives restarts and off-hours.
# Stores {session_date, generated_at, curves: {theme_id: [{date, value_pct}]}}.
# Written after every successful 1D compute that has non-empty intraday data.
_1D_LKG_PATH     = Path(__file__).parent.parent / "data" / "themes_rs_1d_lkg.json"
# Bump this whenever a new timeframe or field is added to theme rows.
# warmup_theme_rs() uses it to skip seeding new TF caches from old LKG rows.
_LKG_SCHEMA_VER  = "v2_5y"
_XC_PATH     = Path(__file__).parent.parent / "data" / "x_consensus_weekly.json"
_HIST_TTL    = 3600       # 1h — daily bars don't change intraday

# Disk file storing per-TF last-computed unix timestamps (survives restarts).
_REFRESH_TS_PATH = Path(__file__).parent.parent / "data" / "theme_rs_refresh_ts.json"
# Historical TF refresh cadence — FMP history fetched at most once per day per TF.
# 1D is exempt: it uses Tradier quotes only (no FMP history calls).
_HIST_FETCH_CADENCE = 86_400   # 24 h

_BENCHMARKS  = ["SPY", "QQQ"]

_TIMEFRAME_BARS: dict[str, int] = {
    "1D":  1,
    "7D":  5,
    "30D": 22,
    "YTD": 0,    # special: _ytd_change()
    "1Y":  252,
    "5Y":  1250, # ~5Y trading days; FMP full-history returns ~1256 bars (need n+1)
}

# Top N holdings to pull from primary proxy ETF for leader/laggard universe
_ETF_HOLDINGS_TOP_N = 10

# Semaphore for FMP history calls (parallel bursting within rate-limit)
_FMP_HIST_SEM = asyncio.Semaphore(25)
# Semaphore for Tradier intraday timesales calls (raw httpx, bypasses TRADIER_LIMITER).
# Caps the burst on the first cold-start fetch (222 unique proxy symbols).
# Per-symbol 10-min cache means only ~1 burst per 10-min window during market hours.
_INTRADAY_SEM = asyncio.Semaphore(20)

# ── Per-timeframe TTL constants ────────────────────────────────────────────────
_TTL_1D_MARKET   = 60      # 1 minute  — 1D during market hours (Tradier, real-time)
_TTL_OFF_HOURS   = 3600    # 60 minutes — 1D off-hours (Tradier, prices don't move)
# 7D/30D/YTD/1Y: global rule — weekday 60 min, weekend cached until Monday 09:30 ET
# (see _fmp_hist_ttl(); _TTL_HIST_MARKET removed — was 15 min, no intraday value)

# ── Per-timeframe refresh locks & schedule trackers ───────────────────────────
# asyncio.Lock per timeframe: only one concurrent refresh per timeframe allowed.
_refresh_locks: dict[str, asyncio.Lock] = {}
# When each timeframe was last successfully computed (unix ts).
_last_computed: dict[str, float] = {tf: 0.0 for tf in ("1D", "7D", "30D", "YTD", "1Y", "5Y")}

# ── Stale-safe LKG constants & state ──────────────────────────────────────────
# Minimum theme count a fresh result must have to be allowed to overwrite the LKG.
# Partial results (e.g. FMP guard, rate-limit, data outage) produce < this count
# and must never poison the last-good snapshot.
# Derived dynamically: floor = 94 % of the current active universe size, so
# future theme additions/removals never require editing a magic constant.
_EXPECTED_THEME_COUNT: int = len(THEME_RS_UNIVERSE)
_MIN_LKG_THEME_FLOOR: int = max(1, int(_EXPECTED_THEME_COUNT * 0.94))

# Set True by invalidate_theme_rs_cache() after an admin universe edit.
# Cleared when a fresh full-count LKG is successfully written.
# Exposed via get_theme_rs_status() and in every payload as admin_refresh_pending.
_ADMIN_DIRTY: bool = False


# ── Basket membership helpers ───────────────────────────────────────────────────

def _basket_hash(symbols: list[str]) -> str:
    """
    Deterministic 6-char hash of a proxy-symbol basket.

    Normalised: uppercase, stripped, deduplicated, sorted, joined with '|'.
    Used to detect basket membership changes across LKG snapshots so stale
    curves are never served when a theme's proxy_symbols list has changed.
    """
    key = "|".join(sorted({s.strip().upper() for s in symbols if s}))
    return hashlib.sha1(key.encode()).hexdigest()[:6]


def _get_lock(tf: str) -> asyncio.Lock:
    """Return (or lazily create) the asyncio.Lock for a timeframe."""
    if tf not in _refresh_locks:
        _refresh_locks[tf] = asyncio.Lock()
    return _refresh_locks[tf]


# ── TTL helpers ────────────────────────────────────────────────────────────────

def _is_market_hours() -> bool:
    now = datetime.now(tz=timezone.utc)
    if now.weekday() >= 5:
        return False
    month = now.month
    utc_offset = 4 if 4 <= month <= 10 else 5
    et_min = (now.hour - utc_offset) % 24 * 60 + now.minute
    return 570 <= et_min <= 960   # 09:30–16:00 ET


def _ttl_for_timeframe(tf: str) -> int:
    """Return cache TTL seconds for the given timeframe.

    1D  (Tradier real-time): 60s market hours, 3600s off-hours.
    7D+ (FMP EOD data):      global rule via fmp_hist_ttl() from data.fmp_utils —
                             60 min weekdays, cached until Monday 09:30 ET on weekends.
    """
    if tf == "1D":
        return _TTL_1D_MARKET if _is_market_hours() else _TTL_OFF_HOURS
    return fmp_hist_ttl()


# ── LKG helpers ────────────────────────────────────────────────────────────────

def _lkg_has_5y(rows: list[dict]) -> bool:
    """Return True if any row has a non-null performance["5Y"] value."""
    return any(r.get("performance", {}).get("5Y") is not None for r in rows)


def _load_lkg() -> Optional[list[dict]]:
    try:
        if not _LKG_PATH.exists():
            return None
        raw = json.loads(_LKG_PATH.read_text())
        rows: Optional[list] = None
        if isinstance(raw, list) and raw:
            rows = raw
        # v2_5y format: {"_schema": ..., "rows": [...]}
        elif isinstance(raw, dict) and isinstance(raw.get("rows"), list) and raw["rows"]:
            rows = raw["rows"]
        if rows is None:
            return None
        # Count guard: treat a sub-floor LKG as poisoned — quarantine and ignore.
        # This handles the case where a partial result was written before the count
        # guard existed, or the file was corrupted in some other way.
        if len(rows) < _MIN_LKG_THEME_FLOOR:
            print(
                f"[THEME_RS] LKG on disk has only {len(rows)} themes "
                f"(< floor {_MIN_LKG_THEME_FLOOR}) — quarantining as poisoned"
            )
            try:
                quarantine = _LKG_PATH.with_suffix(".json.poisoned")
                _LKG_PATH.replace(quarantine)
            except Exception as qe:
                print(f"[THEME_RS] LKG quarantine error: {qe}")
            return None
        return rows
    except Exception as e:
        print(f"[THEME_RS] LKG load error: {e}")
    return None


def get_rs_payload_sync(tf: str = "1D") -> Optional[dict]:
    """
    Synchronous helper for callers that cannot await get_theme_rs_data().

    Returns the cached payload for *tf* if warm, otherwise wraps the LKG disk
    rows as a stale payload.  Returns None only when both cache and LKG are
    unavailable (cold start with no disk file).

    Used by ThematicContextProvider._build_snapshot() so it always gets RS data
    even when the 60-second 1D cache has just expired between refresh cycles.
    """
    from data.cache import cache as _c
    key = f"{_CACHE_KEY}:{tf.upper()}"
    cached = _c.get(key)
    if cached and isinstance(cached, dict):
        return cached
    lkg = _load_lkg()
    if lkg:
        return _lkg_payload(lkg, tf.upper(), source="lkg_tcp_fallback")
    return None


def _save_lkg(data: list[dict]) -> None:
    """
    Atomic write — never overwrites a valid snapshot with bad data.

    Two quality guards (checked in order):

    1. Count guard: if len(data) < _MIN_LKG_THEME_FLOOR AND the existing LKG
       already has a healthy count (>= _MIN_LKG_THEME_FLOOR), skip the write.
       This prevents a partial result (FMP guard / rate-limit / outage) from
       poisoning the last-good 60-theme snapshot.

    2. 5Y guard: do NOT overwrite a 5Y-capable LKG with a 5Y-null one.
    """
    global _ADMIN_DIRTY

    if not data:
        print("[THEME_RS] LKG save skipped — empty result")
        return

    # Load existing LKG once; reuse for both quality checks below.
    existing = _load_lkg()

    # ── 1. Count guard ────────────────────────────────────────────────────────
    if len(data) < _MIN_LKG_THEME_FLOOR:
        if existing and len(existing) >= _MIN_LKG_THEME_FLOOR:
            print(
                f"[THEME_RS] LKG save skipped — partial result ({len(data)} themes "
                f"< floor {_MIN_LKG_THEME_FLOOR}); existing LKG preserved "
                f"({len(existing)} themes)"
            )
            return

    # ── 2. 5Y guard ───────────────────────────────────────────────────────────
    if not _lkg_has_5y(data):
        if existing and _lkg_has_5y(existing):
            print("[THEME_RS] LKG save skipped — new data lacks 5Y, existing LKG has it")
            return

    # ── Atomic write ──────────────────────────────────────────────────────────
    try:
        _LKG_PATH.parent.mkdir(parents=True, exist_ok=True)
        payload = {"_schema": _LKG_SCHEMA_VER, "rows": data}
        tmp = _LKG_PATH.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, indent=2, default=str))
        tmp.replace(_LKG_PATH)
        # Clear dirty flag once a full-count snapshot replaces the old LKG.
        if _ADMIN_DIRTY and len(data) >= _MIN_LKG_THEME_FLOOR:
            _ADMIN_DIRTY = False
            print(
                f"[THEME_RS] Admin dirty flag cleared — fresh full LKG saved "
                f"({len(data)} themes)"
            )
    except Exception as e:
        print(f"[THEME_RS] LKG save error: {e}")


# ── 1D intraday LKG (durable across restarts & off-hours) ─────────────────────

def _save_1d_lkg(rows: list[dict]) -> None:
    """
    Persist intraday performance_curve for all 1D themes to disk.

    Only writes when at least _MIN_LKG_THEME_FLOOR themes have a non-empty curve
    whose first date contains "T" (i.e. is a real intraday ISO timestamp).
    This prevents a market-closed empty-curve run from overwriting a good snapshot.

    Format: compact JSON — {session_date, generated_at, curves: {theme_id: {curve, basket_hash}}}
    basket_hash is stored per-theme so warmup can skip curves whose proxy_symbols
    have since changed (new ticker added / removed by admin or static-file edit).
    """
    from datetime import date as _d
    curves: dict[str, dict] = {}
    for row in rows:
        curve = row.get("performance_curve") or []
        if curve and "T" in str((curve[0] or {}).get("date", "")):
            tid = row.get("theme_id")
            if tid:
                curves[tid] = {
                    "curve":       curve,
                    "basket_hash": _basket_hash(row.get("proxy_symbols") or []),
                }

    if len(curves) < _MIN_LKG_THEME_FLOOR:
        return  # market closed / pre-market — don't overwrite a good snapshot

    data = {
        "session_date": _d.today().isoformat(),
        "generated_at": time.time(),
        "curves":       curves,
    }
    tmp = _1D_LKG_PATH.with_suffix(".json.tmp")
    try:
        _1D_LKG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with tmp.open("w") as f:
            json.dump(data, f, separators=(",", ":"))
        tmp.replace(_1D_LKG_PATH)
        print(
            f"[THEME_RS] 1D intraday LKG saved "
            f"({len(curves)} themes, session={data['session_date']})"
        )
    except Exception as exc:
        print(f"[THEME_RS] 1D intraday LKG save error: {exc}")
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass


def _load_1d_lkg() -> Optional[dict]:
    """Load the 1D intraday LKG from disk. Returns None if absent or corrupt."""
    if not _1D_LKG_PATH.exists():
        return None
    try:
        with _1D_LKG_PATH.open() as f:
            data = json.load(f)
        if not isinstance(data.get("curves"), dict):
            return None
        return data
    except Exception as exc:
        print(f"[THEME_RS] 1D intraday LKG load error: {exc}")
        return None


# ── Refresh timestamp persistence ─────────────────────────────────────────────

def _load_refresh_ts() -> None:
    """
    Load per-TF last-computed timestamps from disk into _last_computed.
    Called once at startup so process restarts don't reset the daily cadence guard.
    Non-fatal: any error is logged and silently ignored.
    """
    try:
        if not _REFRESH_TS_PATH.exists():
            return
        data = json.loads(_REFRESH_TS_PATH.read_text())
        if not isinstance(data, dict):
            return
        loaded: list[str] = []
        now = time.time()
        for tf, ts in data.items():
            if tf in _last_computed and isinstance(ts, (int, float)) and 0 < ts <= now:
                _last_computed[tf] = float(ts)
                loaded.append(f"{tf}={int(now - ts)}s_ago")
        if loaded:
            print(f"[THEME_RS] Refresh timestamps loaded from disk: {loaded}")
        else:
            print("[THEME_RS] Refresh timestamps: disk file exists but no valid entries")
    except Exception as e:
        print(f"[THEME_RS] Refresh timestamp load error (non-fatal): {e}")


def _save_refresh_ts(tf: str, ts: float) -> None:
    """
    Atomically persist the last-computed timestamp for *tf* to disk.
    Merges with existing entries so other TFs are not overwritten.
    Non-fatal: any error is logged and silently ignored.
    """
    try:
        existing: dict = {}
        if _REFRESH_TS_PATH.exists():
            try:
                raw = json.loads(_REFRESH_TS_PATH.read_text())
                if isinstance(raw, dict):
                    existing = raw
            except Exception:
                pass
        existing[tf] = ts
        _REFRESH_TS_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = _REFRESH_TS_PATH.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(existing, indent=2))
        tmp.replace(_REFRESH_TS_PATH)
    except Exception as e:
        print(f"[THEME_RS] Refresh timestamp save error (non-fatal): {e}")


# ── Payload builders ───────────────────────────────────────────────────────────

def _make_payload(rows: list[dict], tf: str) -> dict:
    """Build a fresh live response payload from computed rows."""
    fmp_used     = any(s == "fmp"          for r in rows for s in r.get("proxy_source_health", {}).values())
    tradier_used = any(s == "tradier_hist" for r in rows for s in r.get("proxy_source_health", {}).values())
    yf_used      = any(s == "yfinance"     for r in rows for s in r.get("proxy_source_health", {}).values())
    ttl          = _ttl_for_timeframe(tf)
    now_ts       = time.time()
    return {
        "themes":                rows,
        "timeframe":             tf,
        "theme_count":           len(rows),
        "generated_at":          datetime.now(timezone.utc).isoformat(),
        "last_updated":          datetime.now(timezone.utc).isoformat(),
        "cache_ttl_s":           ttl,
        "cache_age_seconds":     0,
        "_cache_set_at":         now_ts,
        "is_market_hours":       _is_market_hours(),
        "source":                "live",
        "admin_refresh_pending": _ADMIN_DIRTY,
        "source_health": {
            "tradier_quotes":  any(r["performance"].get("1D") is not None for r in rows),
            "fmp_history":     fmp_used,
            "tradier_history": tradier_used,
            "yfinance":        yf_used,
        },
    }


def _lkg_payload(rows: list[dict], tf: str, source: str = "lkg") -> dict:
    """Build a stale/LKG response payload."""
    return {
        "themes":                rows,
        "timeframe":             tf,
        "theme_count":           len(rows),
        "generated_at":          None,
        "last_updated":          None,
        "cache_ttl_s":           None,
        "cache_age_seconds":     None,
        "_cache_set_at":         None,
        "is_market_hours":       _is_market_hours(),
        "source":                source,
        "admin_refresh_pending": _ADMIN_DIRTY,
        "source_health": {
            "tradier_quotes":  False,
            "fmp_history":     False,
            "tradier_history": False,
            "yfinance":        False,
        },
    }


def _add_freshness(payload: dict) -> dict:
    """Inject cache_age_seconds into a cached payload (mutates in-place)."""
    set_at = payload.get("_cache_set_at")
    payload["cache_age_seconds"] = round(time.time() - set_at) if set_at else None
    return payload


def _validate_basket_hashes(
    payload: dict,
    *,
    kick_refresh_tf: Optional[str] = None,
) -> tuple[dict, int]:
    """
    Validate per-theme basket_hash for every row in a cached or LKG payload.

    Applied at every cache-serving path so stale curve data is never returned
    to the frontend when the theme basket membership has changed.

    Decision matrix per row:
    ┌──────────────────────┬───────────────┬──────────────────────────────────────┐
    │ stored basket_hash   │ == current?   │ outcome                              │
    ├──────────────────────┼───────────────┼──────────────────────────────────────┤
    │ present              │ yes           │ curve_status="current" — serve as-is │
    │ present              │ no  (mismatch)│ performance_curve=[]                 │
    │                      │               │ curve_status="stale_membership"       │
    │                      │               │ basket_hash refreshed to current     │
    │ absent (old-format)  │ N/A           │ curve_status="stale_legacy_lkg"      │
    │                      │               │ basket_hash stamped with current     │
    │                      │               │ data still served (backward compat)  │
    └──────────────────────┴───────────────┴──────────────────────────────────────┘

    Returns (patched_payload, stale_count) where stale_count counts only confirmed
    hash-mismatch rows (absent-hash rows not counted in stale_count).

    When kick_refresh_tf is set and any row is stale or legacy, schedules an
    immediate _locked_refresh to replace the payload — bypassing the normal TTL
    gate so membership changes surface in the very next request cycle.
    """
    themes = payload.get("themes", [])
    if not themes:
        return payload, 0

    stale_count  = 0
    legacy_count = 0
    patched_rows: list[dict] = []

    for row in themes:
        tid          = row.get("theme_id", "")
        meta         = THEME_RS_UNIVERSE.get(tid) or {}
        current_syms = meta.get("proxy_symbols", [])
        current_hash = _basket_hash(current_syms)
        stored_hash  = row.get("basket_hash")

        if stored_hash is None:
            # Old-format row — no hash stored (pre-membership-hash LKG snapshot).
            # Serve the data as-is (may still be correct) but flag clearly.
            legacy_count += 1
            patched_rows.append({
                **row,
                "basket_hash":  current_hash,
                "curve_status": "stale_legacy_lkg",
            })

        elif stored_hash != current_hash:
            # Confirmed mismatch — basket changed since this row was computed.
            # Do NOT serve stale curve data; return empty so the frontend can
            # show a warming/pending state instead of a wrong-membership chart.
            stale_count += 1
            patched_rows.append({
                **row,
                "performance_curve":     [],
                "basket_hash":           current_hash,
                "curve_status":          "stale_membership",
                "curve_total_symbols":   len(current_syms),
                "curve_covered_symbols": 0,
                "curve_missing_symbols": sorted(current_syms),
                "curve_partial":         False,
            })

        else:
            # Hash matches — basket is current-membership.
            patched_rows.append({**row, "curve_status": "current"})

    patched_payload = {
        **payload,
        "themes":                 patched_rows,
        "theme_count":            len(patched_rows),
        "basket_hash_validated":  True,
        "stale_membership_count": stale_count,
        "legacy_lkg_count":       legacy_count,
    }

    # Bypass TTL — schedule an immediate background recompute whenever any row
    # is stale (confirmed mismatch) or legacy (no hash available for validation).
    # The lock check inside _locked_refresh makes duplicate kicks safe (no-op).
    if kick_refresh_tf and (stale_count > 0 or legacy_count > 0):
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.ensure_future(_locked_refresh(kick_refresh_tf))
        except Exception:
            pass

    return patched_payload, stale_count


# ── FMP historical price provider ──────────────────────────────────────────────

def _fmp_key() -> str:
    return os.getenv("FMP_API_KEY", "")


# Maximum calendar days to request.
# 5Y needs ~1250 trading bars; 1900 cal days × 252/365 ≈ 1311 trading bars —
# comfortable buffer above the 1250-bar threshold.
# (Previous value of 1400 cal days only yielded ~966 trading bars — insufficient.)
_FMP_HIST_RANGE_DAYS = 1900


async def _fetch_fmp_daily_history(symbol: str) -> list[dict]:
    """
    FMP stable/historical-price-eod — date-ranged (last _FMP_HIST_RANGE_DAYS
    calendar days).  Replaces the former /full call which pulled the entire
    ticker history and caused runaway API-call counts.

    Stampede guard: cache is re-checked INSIDE the semaphore so concurrent
    requests for the same symbol (e.g. from 5 parallel timeframe computes at
    startup) only ever issue one HTTP request — the first one writes to cache
    and all subsequent waiters read from it.

    Returns sorted list of {date, close} bars, oldest-first.
    Cached 4h in-process (key shared across all timeframes).
    """
    from services.fmp_full_guard import log_and_check as _fmp_guard

    sym = symbol.upper()
    cache_key = f"fmp_hist:{sym}"

    # ── Fast path: cache hit (outside semaphore, no wait) ────────────────────
    hit = cache.get(cache_key)
    if hit is not None:
        return hit

    # ── Guard: env-var emergency shutoff ─────────────────────────────────────
    # FMP_BLOCK_FULL_HISTORICAL=true  → skip ALL FMP history calls, return []
    # FMP_DIAGNOSTIC_DRY_RUN=true     → log cache-miss events, still proceed
    # (This function now calls the date-ranged endpoint, not /full, so the
    #  guard doubles as a general FMP history shutoff for theme_rs.)
    if _fmp_guard(
        sym,
        caller_func="_fetch_fmp_daily_history",
        caller_file="theme_rs_service.py",
        job_name="theme_rs_warmup_loop",
    ):
        return []  # FMP_BLOCK_FULL_HISTORICAL=true — no network call

    key = _fmp_key()
    if not key:
        return []

    from_date = (date.today() - timedelta(days=_FMP_HIST_RANGE_DAYS)).isoformat()
    to_date   = date.today().isoformat()

    async with _FMP_HIST_SEM:
        # ── Stampede guard: re-check cache INSIDE the semaphore ──────────────
        # Multiple concurrent callers (e.g. 5 TF computes at startup) all see
        # a cache miss before acquiring the semaphore.  Without this re-check
        # they would ALL issue an FMP request.  With it, only the first caller
        # hits the network; every subsequent caller finds the freshly written
        # cache entry and returns immediately.
        hit = cache.get(cache_key)
        if hit is not None:
            return hit

        try:
            async with httpx.AsyncClient(timeout=12.0) as client:
                resp = await client.get(
                    "https://financialmodelingprep.com/stable/historical-price-eod",
                    params={
                        "symbol": sym,
                        "from":   from_date,
                        "to":     to_date,
                        "apikey": key,
                    },
                )
            if resp.status_code not in (200, 201):
                if resp.status_code not in (403, 402, 404):
                    print(f"[THEME_RS][FMP hist] {sym} HTTP {resp.status_code}")
                return []
            raw = resp.json()
            if isinstance(raw, list):
                bars_raw = raw
            elif isinstance(raw, dict):
                bars_raw = raw.get("historical") or []
            else:
                return []

            bars = []
            for b in bars_raw:
                if not isinstance(b, dict):
                    continue
                d = b.get("date") or b.get("formattedDate") or ""
                c = b.get("close") or b.get("adjClose")
                if d and c is not None:
                    try:
                        bars.append({"date": str(d)[:10], "close": float(c)})
                    except (TypeError, ValueError):
                        pass

            bars.sort(key=lambda r: r["date"])
            if bars:
                print(
                    f"[THEME_RS][FMP hist] {sym}: {len(bars)} bars"
                    f" (from={from_date} to={to_date}) ✓"
                )
                cache.set(cache_key, bars, _HIST_TTL)
            return bars

        except Exception as e:
            print(f"[THEME_RS][FMP hist] {sym}: {e}")
            return []


async def _fetch_tradier_daily_history(symbol: str, days: int = 400) -> list[dict]:
    """
    Tradier /markets/history — secondary historical provider.
    Returns sorted {date, close} bars.
    Cached 1h.
    """
    sym = symbol.upper()
    cache_key = f"tdier_hist:{sym}:{days}"
    hit = cache.get(cache_key)
    if hit is not None:
        return hit

    key = _tradier_key()
    if not key:
        return []

    start = (date.today() - timedelta(days=days)).isoformat()
    end   = date.today().isoformat()
    base  = _tradier_base()

    try:
        async with httpx.AsyncClient(timeout=12.0) as client:
            resp = await client.get(
                f"{base}/markets/history",
                headers={"Authorization": f"Bearer {key}", "Accept": "application/json"},
                params={"symbol": sym, "interval": "daily", "start": start, "end": end},
            )
        if resp.status_code != 200:
            return []
        data    = resp.json()
        history = data.get("history") or {}
        days_raw = history.get("day") or []
        if isinstance(days_raw, dict):
            days_raw = [days_raw]

        bars = []
        for d in days_raw:
            dt  = d.get("date") or ""
            cls = d.get("close")
            if dt and cls is not None:
                try:
                    bars.append({"date": str(dt)[:10], "close": float(cls)})
                except (TypeError, ValueError):
                    pass

        bars.sort(key=lambda r: r["date"])
        if bars:
            cache.set(cache_key, bars, _HIST_TTL)
        return bars

    except Exception as e:
        print(f"[THEME_RS][Tradier hist] {sym}: {e}")
        return []


async def _fetch_intraday_bars(sym: str) -> list[dict]:
    """
    Fetch Tradier 5-min intraday bars for today's market session.
    Returns [{date: ISO-timestamp (Eastern), close: float}] oldest→newest.
    Cached 10 min per symbol per trading day — safe to call from the 60-s 1D
    warmup loop without hammering the Tradier rate-limit bucket.
    """
    from datetime import date as _d, datetime, timezone, timedelta as _td

    sym   = sym.upper()
    today = _d.today().isoformat()
    cache_key = f"theme_rs:intraday_5min:{sym}:{today}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    key = _tradier_key()
    if not key:
        return []

    base = _tradier_base()
    try:
        async with _INTRADAY_SEM:   # ≤20 concurrent Tradier timesales calls
            async with httpx.AsyncClient(timeout=12.0) as client:
                resp = await client.get(
                    f"{base}/markets/timesales",
                    headers={"Authorization": f"Bearer {key}", "Accept": "application/json"},
                    params={
                        "symbol":         sym,
                        "interval":       "5min",
                        "start":          f"{today} 09:30",
                        "end":            f"{today} 16:05",
                        "session_filter": "open",
                    },
                )
        if resp.status_code != 200:
            return []

        data   = resp.json()
        series = data.get("series") or {}
        raw    = series.get("data") or []
        if isinstance(raw, dict):
            raw = [raw]

        _EDT = timezone(_td(hours=-4))   # EDT; close enough for intraday display
        bars_out: list[dict] = []
        for t in raw:
            ts  = t.get("timestamp")
            cls = t.get("close")
            if ts is None or cls is None:
                continue
            try:
                dt_str = datetime.fromtimestamp(int(ts), tz=_EDT).isoformat()
                bars_out.append({"date": dt_str, "close": float(cls)})
            except Exception:
                continue

        bars_out.sort(key=lambda b: b["date"])
        if bars_out:
            cache.set(cache_key, bars_out, 600)   # 10-min TTL
        return bars_out

    except Exception as exc:
        print(f"[THEME_RS][intraday] {sym}: {exc}")
        return []


async def _fetch_proxy_history(symbol: str, days: int = 400) -> tuple[list[dict], str]:
    """
    FMP (primary) → Tradier daily (fallback) → yfinance (emergency).
    Returns (bars, source_name).

    days: lookback in calendar days passed to Tradier / yfinance fallbacks.
    FMP uses _FMP_HIST_RANGE_DAYS regardless (already long enough for 5Y).
    """
    bars = await _fetch_fmp_daily_history(symbol)
    if bars:
        return bars, "fmp"

    bars = await _fetch_tradier_daily_history(symbol, days=days)
    if bars:
        return bars, "tradier_hist"

    bars = await fetch_etf_history(symbol, days=days)
    if bars:
        return bars, "yfinance"

    return [], "unavailable"


# ── Quote fetchers ─────────────────────────────────────────────────────────────

async def _fetch_all_quotes(symbols: list[str]) -> dict[str, dict]:
    """Tradier batch → Finnhub per-symbol fallback."""
    result: dict[str, dict] = {}

    tradier_data = await _tradier_quotes_batch(symbols)
    if tradier_data:
        result.update(tradier_data)

    missing = [t for t in symbols if t not in result or not result[t].get("price")]
    if missing:
        async with httpx.AsyncClient(timeout=10.0) as session:
            fallbacks = await asyncio.gather(
                *[_finnhub_quote_single(t, session) for t in missing],
                return_exceptions=True,
            )
        for item in fallbacks:
            if not isinstance(item, Exception):
                t, q = item
                if q:
                    result[t] = q

    return {t: q for t, q in result.items() if q}


# ── Dynamic leader/laggard universe ───────────────────────────────────────────

def _read_xc_tickers() -> list[str]:
    """Read X/Grok consensus tickers from disk snapshot — no new API calls."""
    try:
        if not _XC_PATH.exists():
            return []
        raw = json.loads(_XC_PATH.read_text())
        saved_at = float(raw.get("_saved_at", 0))
        if saved_at and (time.time() - saved_at) > 8 * 86400:
            return []   # stale — older than 8 days
        top = raw.get("top_tickers") or []
        tickers = []
        for entry in top:
            if isinstance(entry, str):
                tickers.append(entry.upper())
            elif isinstance(entry, dict):
                sym = (entry.get("ticker") or entry.get("symbol") or "").upper()
                if sym:
                    tickers.append(sym)
        return tickers[:30]
    except Exception as e:
        print(f"[THEME_RS] XC read error: {e}")
        return []


async def _etf_holdings_for_proxy(etf_symbol: str) -> list[str]:
    """
    Get top N tickers from ETF holdings service.
    Returns clean list of stock tickers (no ETF self, no cash).
    """
    try:
        from services.sector_rotation.etf_holdings_service import get_etf_holdings
        data = await asyncio.wait_for(get_etf_holdings(etf_symbol), timeout=10.0)
        holdings = data.get("holdings") or []
        out = []
        for h in holdings[:_ETF_HOLDINGS_TOP_N]:
            sym = (h.get("ticker") or "").upper()
            if sym and sym != etf_symbol and len(sym) <= 6 and sym not in ("CASH", "USD"):
                out.append(sym)
        return out
    except Exception as e:
        print(f"[THEME_RS] ETF holdings error {etf_symbol}: {e}")
        return []


async def _build_leader_universe(
    theme_id: str,
    meta: dict,
    proxy_symbols_used: list[str],
) -> tuple[list[str], dict[str, list[str]], str]:
    """
    Build the leader/laggard universe for a theme dynamically.

    Priority:
      1a. [theme_basket mode] Seed universe directly from proxy_symbols — the
          curated basket IS the holdings. Never call _etf_holdings_for_proxy for
          custom/hybrid themes because their representative_symbol is an unrelated
          chart-only ETF whose holdings would pollute the leaders table.
      1b. [etf_holdings mode] ETF holdings from primary proxy ETF (used first),
          then secondary proxy ETFs (if primary < threshold).
      2. X/Grok consensus tickers matched to theme keywords (disk read-only).
      3. Static candidate_symbols as last-resort fallback seeds.

    Returns:
      (universe_tickers, discovery_sources_by_ticker, universe_source_label)
    """
    sources_by_ticker: dict[str, list[str]] = {}
    universe: list[str] = []
    label_parts: list[str] = []

    holdings_mode = meta.get("holdings_display_mode", "etf_holdings")

    # ── 1. Build initial universe based on holdings_display_mode ──────────────
    if holdings_mode == "theme_basket":
        # Custom/hybrid themes: the proxy basket IS the holdings.
        # Seed universe from every proxy symbol (used + unused) so the
        # leader/laggard table reflects the true curated basket, not the
        # holdings of an unrelated chart-only ETF (e.g. SOXX for substrates).
        basket = list(dict.fromkeys(
            (proxy_symbols_used or []) + meta.get("proxy_symbols", [])
        ))
        for sym in basket:
            sources_by_ticker.setdefault(sym, [])
            if "theme_basket" not in sources_by_ticker[sym]:
                sources_by_ticker[sym].append("theme_basket")
            if sym not in universe:
                universe.append(sym)
        if universe:
            label_parts.append("theme_basket")
    else:
        # ── 1b. ETF holdings from proxy ETFs (original behavior) ──────────────
        proxies_to_try = proxy_symbols_used[:3] if proxy_symbols_used else meta["proxy_symbols"][:3]

        for etf in proxies_to_try:
            holdings = await _etf_holdings_for_proxy(etf)
            if holdings:
                label_parts.append(f"etf_holding:{etf}")
                for sym in holdings:
                    sources_by_ticker.setdefault(sym, [])
                    src = f"etf_holding:{etf}"
                    if src not in sources_by_ticker[sym]:
                        sources_by_ticker[sym].append(src)
                    if sym not in universe:
                        universe.append(sym)
            if len(universe) >= _ETF_HOLDINGS_TOP_N:
                break

    # ── 2. X/Grok consensus filtered by theme keywords (disk, no API call) ─────
    if len(universe) < 5:
        xc_tickers  = _read_xc_tickers()
        keywords    = set(k.lower() for k in meta.get("keywords", []))
        sector_tags = set(s.lower() for s in meta.get("sector_tags", []))
        candidate_upper = set(s.upper() for s in meta.get("candidate_symbols", []))

        for sym in xc_tickers:
            if sym in candidate_upper or sym in (s.upper() for s in universe[:20]):
                sources_by_ticker.setdefault(sym, [])
                if "x_consensus" not in sources_by_ticker[sym]:
                    sources_by_ticker[sym].append("x_consensus")
                if sym not in universe:
                    universe.append(sym)
                    if not label_parts or "x_consensus" not in " ".join(label_parts):
                        label_parts.append("x_consensus")
            if len(universe) >= 20:
                break

    # ── 3. Static candidate_symbols as fallback seeds ──────────────────────────
    static_used = False
    for sym in meta.get("candidate_symbols", []):
        s = sym.upper()
        sources_by_ticker.setdefault(s, [])
        if "static_fallback" not in sources_by_ticker[s]:
            sources_by_ticker[s].append("static_fallback")
        if s not in universe:
            universe.append(s)
            static_used = True

    if static_used and not label_parts:
        label_parts.append("static_fallback")
    elif static_used:
        label_parts.append("static_fallback_seeds")

    source_label = " + ".join(label_parts) if label_parts else "static_fallback"
    return universe, sources_by_ticker, source_label


# ── Performance helpers ────────────────────────────────────────────────────────

def _perf_for_timeframe(
    hist: list[dict],
    tf: str,
    quote: dict,
) -> Optional[float]:
    if tf == "1D":
        v = (
            quote.get("change_percentage") or
            quote.get("change_pct") or
            quote.get("change_1d_pct") or
            quote.get("changesPercentage")
        )
        if v is not None:
            return round(float(v), 2)
        if len(hist) >= 2:
            try:
                prev = float(hist[-2]["close"])
                last = float(hist[-1]["close"])
                return round((last - prev) / prev * 100, 2) if prev else None
            except Exception:
                return None
        return None
    elif tf == "YTD":
        return _ytd_change(hist)
    else:
        return _pct_change(hist, _TIMEFRAME_BARS[tf])


def _compute_theme_perf(
    proxy_symbols: list[str],
    tf: str,
    quotes: dict[str, dict],
    histories: dict[str, tuple[list[dict], str]],
) -> tuple[Optional[float], list[str], dict[str, str]]:
    """
    Compute theme return for a timeframe using equal-weight arithmetic mean.

    Formula:  sum(valid_symbol_returns) / count(valid_symbol_returns)
    Missing/no-data symbols are excluded from both numerator and denominator.
    Returns None only when no valid symbols exist.

    Returns (mean_return_pct, used_proxies, source_health).
    """
    vals: list[float]        = []
    used: list[str]          = []
    health: dict[str, str]   = {}

    for sym in proxy_symbols:
        bars, src = histories.get(sym, ([], "unavailable"))
        q = quotes.get(sym, {})
        p = _perf_for_timeframe(bars, tf, q)
        health[sym] = src if bars else "unavailable"
        if p is not None:
            vals.append(p)
            used.append(sym)

    if not vals:
        return None, [], health

    return round(sum(vals) / len(vals), 2), used, health


def _build_perf_curve(
    proxy_syms: list[str],
    tf: str,        # retained for call-site compatibility; not used for date filtering
    histories: dict[str, tuple[list[dict], str]],
    max_points: int = 1300,
) -> list[dict]:
    """
    Build a normalized equal-weight performance curve using ALL available history.

    Each proxy symbol is normalized to its own oldest available close
    (value_pct = 0 at the first bar), then daily values are averaged equal-weight.
    Returns [{date, value_pct}, ...] ordered oldest → newest.

    Stores the full available history — TF-specific slicing and rebasing is
    applied at serve time by _apply_curves_for_tf() so that a single stored
    curve can correctly answer any TF window (1Y, 5Y, 30D, etc.) without
    cache-key collisions and without the LKG ever poisoning a 1Y response
    with a 5Y curve.
    No new API calls — uses the already-fetched histories dict.
    """
    sym_norm: dict[str, dict[str, float]] = {}
    for sym in proxy_syms:
        bars, _ = histories.get(sym, ([], ""))
        if len(bars) < 2:
            continue
        try:
            base_close = float(bars[0]["close"])
        except (TypeError, ValueError):
            continue
        if not base_close:
            continue
        sym_norm[sym] = {
            b["date"]: round((float(b["close"]) / base_close - 1) * 100, 3)
            for b in bars
        }

    if not sym_norm:
        return []

    all_dates = sorted({dt for nd in sym_norm.values() for dt in nd})
    points: list[dict] = []
    for dt in all_dates:
        vals = [sym_norm[s][dt] for s in sym_norm if dt in sym_norm[s]]
        if vals:
            points.append({"date": dt, "value_pct": round(sum(vals) / len(vals), 3)})

    if not points:
        return []

    # Keep all bars up to max_points — serve-time slicing needs the full range.
    if len(points) > max_points:
        step = max(1, len(points) // max_points)
        last = points[-1]
        points = points[::step]
        if points[-1]["date"] != last["date"]:
            points.append(last)

    return points


def _build_intraday_perf_curve(
    proxy_syms: list[str],
    intraday_bars: dict[str, list[dict]],
    max_pts: int = 78,   # 6.5h / 5min = 78 bars per full session
) -> list[dict]:
    """
    Build a normalized equal-weight intraday performance curve from 5-min bars.
    Each proxy ETF is normalized to its own first-bar close (value_pct = 0).
    Returns [{date: ISO-timestamp, value_pct: float}] oldest→newest.
    Returns [] if no intraday bars are available (market closed / no data).
    Only called when tf == "1D" — does not touch the daily LKG curve.
    """
    sym_norm: dict[str, dict[str, float]] = {}
    for sym in proxy_syms:
        bars = intraday_bars.get(sym, [])
        if len(bars) < 2:
            continue
        try:
            base_close = float(bars[0]["close"])
        except (TypeError, ValueError):
            continue
        if not base_close:
            continue
        sym_norm[sym] = {
            b["date"]: round((float(b["close"]) / base_close - 1) * 100, 3)
            for b in bars
        }

    if not sym_norm:
        return []

    all_ts = sorted({dt for nd in sym_norm.values() for dt in nd})
    points: list[dict] = []
    for dt in all_ts:
        vals = [sym_norm[s][dt] for s in sym_norm if dt in sym_norm[s]]
        if vals:
            points.append({"date": dt, "value_pct": round(sum(vals) / len(vals), 3)})

    if not points:
        return []

    if len(points) > max_pts:
        step = max(1, len(points) // max_pts)
        last = points[-1]
        points = points[::step]
        if points[-1]["date"] != last["date"]:
            points.append(last)

    return points


def _pct_rank(value: Optional[float], universe: list[Optional[float]]) -> float:
    valid = [v for v in universe if v is not None]
    if value is None or not valid:
        return 0.5
    below = sum(1 for v in valid if v < value)
    return below / len(valid)


def _assign_state(rs: float, accel: Optional[float]) -> str:
    if rs >= 70:
        return "active"
    if rs < 30:
        return "dead_zone"
    if 55 <= rs < 70 and (accel is None or accel >= 0):
        return "emerging"
    if 25 <= rs < 45 and accel is not None and accel < 0:
        return "weakening"
    return "neutral"


def _state_reason(
    state: str,
    rs_score: float,
    rs_vs_spy: Optional[float],
    rs_vs_qqq: Optional[float],
    breadth: Optional[float],
    tf_ret: Optional[float],
) -> str:
    """Deterministic human-readable explanation for every state label."""
    spy_str = f"{rs_vs_spy:+.1f}%" if rs_vs_spy is not None else "N/A"
    qqq_str = f"{rs_vs_qqq:+.1f}%" if rs_vs_qqq is not None else "N/A"
    brd_str = f"{breadth:.0f}%" if breadth is not None else "N/A"

    if state == "active":
        if breadth is not None and breadth >= 60:
            return (
                f"Top-percentile RS ({rs_score:.0f}/100) vs theme universe "
                f"with broad participation ({brd_str} advancing). "
                f"Outperforming SPY {spy_str}, QQQ {qqq_str}."
            )
        return (
            f"Top-percentile RS ({rs_score:.0f}/100) vs theme universe. "
            f"Outperforming SPY {spy_str}, QQQ {qqq_str}. "
            f"Breadth {brd_str}."
        )

    if state == "emerging":
        return (
            f"Rising RS ({rs_score:.0f}/100) with positive momentum acceleration. "
            f"Positive return but not yet top-tier vs theme universe. "
            f"SPY delta {spy_str}, QQQ delta {qqq_str}."
        )

    if state == "weakening":
        if rs_vs_spy is not None and rs_vs_spy < 0:
            return (
                f"Deteriorating RS ({rs_score:.0f}/100) with negative momentum. "
                f"Lagging SPY by {abs(rs_vs_spy):.1f}% and QQQ by {abs(rs_vs_qqq or 0):.1f}%. "
                f"Breadth {brd_str} — broad weakness."
            )
        return (
            f"RS score {rs_score:.0f}/100 with decelerating momentum. "
            f"Below-average performance vs peer themes. Breadth {brd_str}."
        )

    if state == "dead_zone":
        return (
            f"Bottom-quartile RS ({rs_score:.0f}/100) vs {len(THEME_RS_UNIVERSE)}-theme universe. "
            f"SPY delta {spy_str}, QQQ delta {qqq_str}. "
            f"Breadth {brd_str} — minimal buying pressure."
        )

    # neutral
    if tf_ret is not None and tf_ret > 0:
        return (
            f"Positive return but mid-table RS ({rs_score:.0f}/100) — "
            f"lagging stronger themes. SPY delta {spy_str}, QQQ delta {qqq_str}."
        )
    return (
        f"Mid-range RS ({rs_score:.0f}/100) vs peer themes. "
        f"SPY delta {spy_str}, QQQ delta {qqq_str}. Breadth {brd_str}."
    )


# ── Theme row builder ──────────────────────────────────────────────────────────

async def _build_theme_row(
    theme_id: str,
    meta: dict,
    quotes: dict[str, dict],
    histories: dict[str, tuple[list[dict], str]],
    tf: str,
    stock_perfs: dict[str, Optional[float]],   # sym → tf-return (may be None)
    stock_sources: dict[str, str],             # sym → discovery_source
    leaders: dict[str, dict] | None = None,   # theme_id → {leader_symbol, source}
    intraday_bars: dict[str, list[dict]] | None = None,  # sym → 5-min bars; 1D only
) -> Optional[dict]:
    """
    Build one theme row for the given timeframe.
    stock_perfs: pre-computed per-stock returns for the requested timeframe.
    """
    proxy_syms = meta["proxy_symbols"]

    # ── Resolve DRAM special handling for memory_storage ──────────────────────
    if theme_id == "memory_storage":
        dram_bars, dram_src = histories.get("DRAM", ([], "unavailable"))
        if dram_bars:
            # DRAM available — use as primary, keep SMH/SOXX as backup
            if "DRAM" not in proxy_syms:
                proxy_syms = ["DRAM"] + [s for s in proxy_syms if s != "DRAM"]
        else:
            # DRAM unavailable — use SMH/SOXX only
            proxy_syms = [s for s in proxy_syms if s != "DRAM"] or proxy_syms

    # ── Theme performance (primary timeframe + all others) ─────────────────────
    tf_ret, used_proxies, source_health = _compute_theme_perf(
        proxy_syms, tf, quotes, histories
    )
    # Per-symbol returns for the primary timeframe (same data used in _compute_theme_perf).
    # Stored so callers can verify the equal-weight average calculation without timing drift.
    per_symbol_returns: dict[str, Optional[float]] = {
        sym: _perf_for_timeframe(histories.get(sym, ([], ""))[0], tf, quotes.get(sym, {}))
        for sym in proxy_syms
    }

    # ── Tooltip member data (proxy symbols with current quote + TF return) ──────
    # Use `is not None` guards throughout — `or` would silently drop 0.0 values.
    members_out: list[dict] = []
    for sym in proxy_syms[:12]:
        q     = quotes.get(sym, {})
        # Tradier uses "last"; FMP/Finnhub use "price"; yfinance uses "close".
        price = next(
            (q[k] for k in ("price", "last", "close") if q.get(k) is not None),
            None,
        )
        # Tradier uses "change_percentage"; FMP uses "changesPercentage".
        chg = next(
            (q[k] for k in ("change_percentage", "change_pct", "change_1d_pct", "changesPercentage")
             if q.get(k) is not None),
            None,
        )
        members_out.append({
            "symbol":     sym,
            "price":      round(float(price), 2) if price is not None else None,
            "change_pct": round(float(chg),   2) if chg   is not None else None,
            "return_pct": per_symbol_returns.get(sym),
        })

    # All-timeframe performance for RS scoring
    all_perf: dict[str, Optional[float]] = {}
    for frame in _TIMEFRAME_BARS:
        if frame == tf:
            all_perf[frame] = tf_ret
        else:
            ret, _, _ = _compute_theme_perf(proxy_syms, frame, quotes, histories)
            all_perf[frame] = ret

    if tf_ret is None and not any(v is not None for v in all_perf.values()):
        if meta.get("proxy_type") != "custom":
            return None
        # Custom theme: still include row even when price data is unavailable
        # (e.g. a custom basket with no standard ETF proxy).  Performance fields will be None.

    # ── Representative price ───────────────────────────────────────────────────
    lead_sym   = None
    lead_price = None
    best_count = -1
    for sym in (used_proxies or proxy_syms):
        bars, _ = histories.get(sym, ([], ""))
        if len(bars) > best_count:
            best_count = len(bars)
            lead_sym   = sym
            q          = quotes.get(sym, {})
            lead_price = (
                q.get("price") or q.get("last") or q.get("close") or
                (float(bars[-1]["close"]) if bars else None)
            )

    # ── 50d SMA distance + trend acceleration ─────────────────────────────────
    pct50s: list[float] = []
    accels: list[float] = []
    for sym in (used_proxies or proxy_syms):
        bars, _ = histories.get(sym, ([], ""))
        q       = quotes.get(sym, {})
        price   = q.get("price") or q.get("last") or (float(bars[-1]["close"]) if bars else None)
        if price and bars:
            ma50 = _sma(bars, 50)
            if ma50:
                pct50s.append((price - ma50) / ma50 * 100)
        if len(bars) >= 21:
            recent = _pct_change(bars, 10)
            prior  = _pct_change(bars[-11:], 10)
            if recent is not None and prior is not None:
                accels.append(recent - prior)

    pct_from_50d = round(statistics.median(pct50s), 2) if pct50s else None
    trend_accel  = round(statistics.median(accels), 2)  if accels else None

    # ── Dynamic leader/laggard universe ───────────────────────────────────────
    universe, disc_sources, universe_src_label = await _build_leader_universe(
        theme_id, meta, used_proxies
    )

    # ── Leaders/laggards ranked by selected timeframe return ───────────────────
    sym_perfs: list[tuple[str, float, str]] = []   # (sym, return_pct, disc_src)
    for sym in universe:
        ret = stock_perfs.get(sym)
        if ret is not None:
            srcs = disc_sources.get(sym, ["unknown"])
            sym_perfs.append((sym, ret, srcs[0] if srcs else "unknown"))

    sym_perfs.sort(key=lambda x: x[1], reverse=True)

    def _make_entry(sym: str, ret: float, src: str) -> dict:
        return {
            "symbol":            sym,
            "return_pct":        ret,
            "timeframe":         tf,
            "source":            stock_sources.get(sym, "tradier_batch"),
            "discovery_sources": disc_sources.get(sym, []),
        }

    top_leaders  = [_make_entry(s, r, ds) for s, r, ds in sym_perfs[:3]]
    top_laggards = [_make_entry(s, r, ds) for s, r, ds in sym_perfs[-3:][::-1]]

    # ── Breadth (% advancing in the requested timeframe) ──────────────────────
    breadth: Optional[float] = None
    if sym_perfs:
        up = sum(1 for _, r, _ in sym_perfs if r > 0)
        breadth = round(up / len(sym_perfs) * 100, 1)

    # ── Performance curve (normalized daily series for the selected TF) ─────────
    if tf == "1D":
        # Use intraday 5-min bars for the 1D curve; daily bars are not available
        # for 1D compute and would produce a multi-month daily series instead.
        perf_curve = _build_intraday_perf_curve(proxy_syms, intraday_bars or {})
        # Coverage: a symbol "has data" when it has ≥2 intraday bars.
        _bars_map   = intraday_bars or {}
        _covered    = [s for s in proxy_syms if len(_bars_map.get(s, [])) >= 2]
        _missing    = [s for s in proxy_syms if len(_bars_map.get(s, [])) < 2]
    else:
        perf_curve = _build_perf_curve(proxy_syms, tf, histories)
        # Coverage: a symbol "has data" when its history list is non-empty.
        _covered    = [s for s in proxy_syms if (histories.get(s) or ([], ""))[0]]
        _missing    = [s for s in proxy_syms if not (histories.get(s) or ([], ""))[0]]

    # ── Weinstein Stage Analysis ───────────────────────────────────────────────
    stage_data: dict = {}
    try:
        from services.stage_analysis import analyze_theme_stage
        spy_daily, _ = histories.get("SPY", ([], ""))
        proxy_bars_map = {}
        for sym in proxy_syms:
            bars, _ = histories.get(sym, ([], ""))
            if bars:
                proxy_bars_map[sym] = bars
        stage_data = analyze_theme_stage(
            proxy_type=meta["proxy_type"],
            proxy_daily_bars_map=proxy_bars_map,
            spy_daily_bars=spy_daily if spy_daily else None,
        )
    except Exception as _stage_err:
        print(f"[THEME_RS][stage] {theme_id}: {_stage_err}")

    return {
        "theme_id":              theme_id,
        "display_name":          meta["display_name"],
        "classification":        meta.get("classification", "theme"),
        "parent_sector":         meta.get("parent_sector"),
        "sector_tags":           meta.get("sector_tags", []),
        "proxy_type":            meta["proxy_type"],
        # Stable chart symbol for Ticker column / TradingView popup.
        # Never "CUSTOM". Never a watchlist-added individual stock.
        # Separate from the performance basket (proxy_symbols).
        "representative_symbol":        meta.get("representative_symbol", meta["proxy_type"].upper()),
        "representative_symbol_source": meta.get("representative_symbol_source", "fallback_stock"),
        # tv_symbol: exchange-prefixed TradingView chart symbol.
        # Derived from representative_symbol + exchange lookup stamped by theme_merge_layer.
        # Falls back to bare representative_symbol (TradingView auto-resolves US tickers).
        "tv_symbol":                    meta.get("tv_symbol") or meta.get("representative_symbol", ""),
        # holdings_display_mode: how the frontend should populate the expanded table.
        #   "theme_basket" — show theme_holdings directly (custom/hybrid themes).
        #   "etf_holdings" — fetch ETF holdings for representative_symbol (ETF themes).
        # Always use theme_basket — the pre-computed proxy_syms are sent directly to
        # the frontend so it never needs to fetch live ETF holdings from FMP.
        # "etf_holdings" mode caused "ETF holdings unavailable on current data plan"
        # errors whenever the FMP ETF endpoint was rate-limited or plan-blocked.
        "holdings_display_mode":        "theme_basket",
        "theme_holdings":               sorted(proxy_syms),
        # leader_symbol / leader_source:
        #   Manual admin-selected stock ticker that represents this theme's current leader.
        #   Separate from representative_symbol (ETF/proxy for TradingView chart).
        #   leader_source: "manual" = admin-set, "none" = no manual leader set.
        "leader_symbol":  (leaders or {}).get(theme_id, {}).get("leader_symbol"),
        "leader_source":  "manual" if (leaders or {}).get(theme_id) else "none",
        "proxy_symbols":         proxy_syms,
        "proxy_symbols_used":    used_proxies,
        "proxy_source_health":   source_health,
        "performance_method":        "equal_weight_average",
        "performance_symbol_count":  len(proxy_syms),
        "valid_symbol_count":        len(used_proxies),
        "skipped_symbol_count":      len(proxy_syms) - len(used_proxies),
        "per_symbol_returns":        per_symbol_returns,
        "members":               members_out,
        "performance_curve":     perf_curve,
        # ── Curve coverage metadata ────────────────────────────────────────────
        # basket_hash: deterministic 6-char hash of proxy_symbols.
        #   Changes whenever tickers are added or removed from this theme's basket.
        #   The 1D intraday LKG stores this per-theme; on restart the warmup
        #   discards curves whose stored hash differs from the current basket.
        "basket_hash":            _basket_hash(proxy_syms),
        # curve_total_symbols: total proxy_symbols in the basket.
        "curve_total_symbols":    len(proxy_syms),
        # curve_covered_symbols: how many basket symbols contributed data for this TF.
        #   1D: symbols with ≥2 intraday bars.
        #   7D+: symbols with non-empty daily history.
        "curve_covered_symbols":  len(_covered),
        # curve_missing_symbols: basket tickers with no data for this TF.
        #   Empty list when all symbols have data.
        "curve_missing_symbols":  _missing,
        # curve_partial: True when ≥1 symbol missing but the curve was still built
        #   from the remaining symbols. False when all symbols present or curve empty.
        "curve_partial":          bool(_missing) and bool(perf_curve),
        "price":                 round(lead_price, 2) if lead_price else None,
        "lead_proxy":            lead_sym,
        "timeframe":             tf,
        "return_pct":            tf_ret,
        "performance": {
            "1D":  all_perf["1D"],
            "7D":  all_perf["7D"],
            "30D": all_perf["30D"],
            "YTD": all_perf["YTD"],
            "1Y":  all_perf["1Y"],
            "5Y":  all_perf["5Y"],
        },
        "breadth_pct":           breadth,
        "pct_from_50d":          pct_from_50d,
        "trend_accel_20d":       trend_accel,
        "leader_universe_source": universe_src_label,
        "leaders":               top_leaders,
        "laggards":              top_laggards,
        "last_updated":          datetime.now(timezone.utc).isoformat(),
        # Filled in by _score_and_state:
        "rs_score":              None,
        "rs_vs_spy":             None,
        "rs_vs_qqq":             None,
        "state":                 None,
        "state_reason":          None,
        "momentum_rank":         None,
        # ── Weinstein Stage Analysis ──────────────────────────────────────────
        "stage":                 stage_data.get("stage"),
        "stage_label":           stage_data.get("stage_label", "Unknown"),
        "stage_score":           stage_data.get("stage_score"),
        "stage_confidence":      stage_data.get("stage_confidence", "low"),
        "stage_reason":          stage_data.get("stage_reason"),
        "stage_signals":         stage_data.get("stage_signals", {}),
        "stage_updated_at":      stage_data.get("stage_updated_at"),
        "stage_source":          stage_data.get("stage_source", "fallback"),
    }


# ── RS scoring ─────────────────────────────────────────────────────────────────

def _score_and_state(
    rows: list[dict],
    tf: str,
    histories: dict[str, tuple[list[dict], str]],
    quotes: dict[str, dict],
) -> list[dict]:
    """Compute RS score (0-100), RS vs SPY/QQQ, state, state_reason for each row."""

    def _bench(sym: str) -> Optional[float]:
        bars, _ = histories.get(sym, ([], ""))
        q       = quotes.get(sym, {})
        return _perf_for_timeframe(bars, tf, q)

    spy_ret = _bench("SPY")
    qqq_ret = _bench("QQQ")

    perf_30d_all = [r["performance"].get("30D") for r in rows]
    perf_7d_all  = [r["performance"].get("7D")  for r in rows]
    perf_1y_all  = [r["performance"].get("1Y")  for r in rows]

    for row in rows:
        p = row["performance"]
        p30 = _pct_rank(p.get("30D"), perf_30d_all)
        p7  = _pct_rank(p.get("7D"),  perf_7d_all)
        p1y = _pct_rank(p.get("1Y"),  perf_1y_all)

        ma50_norm = 0.5
        pct50 = row.get("pct_from_50d")
        if pct50 is not None:
            capped    = max(-20.0, min(20.0, pct50))
            ma50_norm = (capped + 20.0) / 40.0

        accel_norm = 0.5
        accel = row.get("trend_accel_20d")
        if accel is not None:
            capped     = max(-5.0, min(5.0, accel))
            accel_norm = (capped + 5.0) / 10.0

        rs = round(
            p30 * 35.0 +
            p7  * 25.0 +
            p1y * 20.0 +
            ma50_norm  * 10.0 +
            accel_norm * 10.0,
            1,
        )
        row["rs_score"] = rs

        tf_ret = p.get(tf)
        row["rs_vs_spy"] = (
            round(tf_ret - spy_ret, 2)
            if tf_ret is not None and spy_ret is not None else None
        )
        row["rs_vs_qqq"] = (
            round(tf_ret - qqq_ret, 2)
            if tf_ret is not None and qqq_ret is not None else None
        )

        state = _assign_state(rs, accel)
        row["state"] = state
        row["state_reason"] = _state_reason(
            state, rs,
            row["rs_vs_spy"], row["rs_vs_qqq"],
            row.get("breadth_pct"), tf_ret,
        )

    sorted_rows = sorted(rows, key=lambda r: r.get("rs_score") or 0, reverse=True)
    for rank, row in enumerate(sorted_rows, start=1):
        row["momentum_rank"] = rank

    return rows


# ── Main compute pass ──────────────────────────────────────────────────────────

async def _compute(tf: str) -> list[dict]:
    """Full compute pass → quoted + history → scored theme rows."""
    print(f"[THEME_RS] Computing fresh data (tf={tf}) …")

    # ── 1. Collect all symbols needed ─────────────────────────────────────────
    # DRAM is now in ALL_PROXY_SYMBOLS (memory_storage primary proxy)
    all_proxy_with_bench = sorted(set(ALL_PROXY_SYMBOLS + _BENCHMARKS))
    proxy_syms_with_dram = all_proxy_with_bench   # alias for clarity below
    quote_syms = sorted(set(ALL_PROXY_SYMBOLS + ALL_CANDIDATE_SYMBOLS + _BENCHMARKS))
    # Dedupe accounting: proxy+bench go through FMP history; constituents do not.
    _raw_proxy_count = len(ALL_PROXY_SYMBOLS) + len(_BENCHMARKS)
    print(
        f"[THEME_RS] Symbol counts (tf={tf}): "
        f"{len(proxy_syms_with_dram)} unique proxy+bench ETFs "
        f"(raw={_raw_proxy_count}, deduped={_raw_proxy_count - len(proxy_syms_with_dram)} overlap) — "
        f"FMP history for proxy/bench only. "
        f"{len(ALL_CANDIDATE_SYMBOLS)} constituent stocks: quote+yfinance only, no FMP history."
    )

    # ── 2. Fetch quotes (Tradier batch → Finnhub fallback) ────────────────────
    print(f"[THEME_RS] Fetching quotes for {len(quote_syms)} symbols …")
    quotes = await _fetch_all_quotes(quote_syms)

    # ── 3. Fetch proxy + benchmark history (FMP primary → Tradier → yfinance) ─
    # 1D skips daily history entirely — intraday curve uses Tradier timesales.
    # 5Y needs at least 1250 bars; yfinance "5y" period gives ~1260 bars.
    # Other TFs need ~400 days at most — "2y" from yfinance is more than enough.
    histories: dict[str, tuple[list[dict], str]] = {}
    intraday_bars: dict[str, list[dict]] = {}

    if tf == "1D":
        for sym in proxy_syms_with_dram:
            histories[sym] = ([], "unavailable")
        print(f"[THEME_RS] 1D: skipping proxy daily history (intraday curve uses timesales)")
        # Batch-fetch 5-min intraday bars for all unique proxy ETFs across all themes.
        # Cached 10 min — safe on the 60-s warmup cadence (only ~40-60 unique symbols).
        uniq_proxies = list(dict.fromkeys(
            sym
            for meta in THEME_RS_UNIVERSE.values()
            for sym in meta["proxy_symbols"]
        ))
        print(f"[THEME_RS] 1D: fetching intraday bars for {len(uniq_proxies)} proxy ETFs …")
        intra_tasks   = [_fetch_intraday_bars(s) for s in uniq_proxies]
        intra_results = await asyncio.gather(*intra_tasks, return_exceptions=True)
        non_empty = 0
        for sym, result in zip(uniq_proxies, intra_results):
            if isinstance(result, list) and result:
                intraday_bars[sym] = result
                non_empty += 1
        print(f"[THEME_RS] 1D: intraday bars received for {non_empty}/{len(uniq_proxies)} ETFs")
    else:
        proxy_hist_days = 1900 if tf == "5Y" else 400
        print(f"[THEME_RS] Fetching proxy history for {len(proxy_syms_with_dram)} symbols (days={proxy_hist_days}) …")
        hist_tasks = [_fetch_proxy_history(s, days=proxy_hist_days) for s in proxy_syms_with_dram]
        hist_results = await asyncio.gather(*hist_tasks, return_exceptions=True)
        for sym, result in zip(proxy_syms_with_dram, hist_results):
            if isinstance(result, tuple):
                histories[sym] = result
            else:
                histories[sym] = ([], "unavailable")

    # ── 4. Discover all dynamic universe stocks across all themes ─────────────
    # We need ETF holdings per theme's primary proxy — gather now to dedup
    print("[THEME_RS] Discovering dynamic leader universes …")
    primary_proxies = []
    for meta in THEME_RS_UNIVERSE.values():
        proxies = meta["proxy_symbols"]
        if proxies:
            primary_proxies.append(proxies[0])
    # Also DRAM for memory_storage
    primary_proxies.append("DRAM")
    primary_proxies = list(dict.fromkeys(primary_proxies))  # dedup, preserve order

    holdings_tasks  = [_etf_holdings_for_proxy(p) for p in primary_proxies]
    holdings_results = await asyncio.gather(*holdings_tasks, return_exceptions=True)
    all_dynamic_stocks: set[str] = set()
    for res in holdings_results:
        if isinstance(res, list):
            all_dynamic_stocks.update(res)
    # Also include static candidate_symbols
    all_dynamic_stocks.update(ALL_CANDIDATE_SYMBOLS)
    all_dynamic_stocks -= set(proxy_syms_with_dram)   # already have proxy history
    all_dynamic_stocks_list = sorted(all_dynamic_stocks)

    # ── 5. Fetch history for all dynamic universe stocks ──────────────────────
    # For 1D: Tradier batch quotes sufficient — skip heavy history.
    # For 7D+: use yfinance thread-pool for individual stocks (fast, parallel).
    #   FMP primary is reserved for proxy ETFs (critical for theme performance).
    if tf == "1D":
        print(f"[THEME_RS] 1D: skipping per-stock history (Tradier quotes only)")
        for sym in all_dynamic_stocks_list:
            histories.setdefault(sym, ([], "unavailable"))
    else:
        # 5Y needs ~5 years of per-stock history for leader/laggard ranking;
        # all other historical timeframes need ~400 days (covers 30D/YTD/1Y easily).
        yf_days = 1350 if tf == "5Y" else 400
        print(f"[THEME_RS] {tf}: yfinance history ({yf_days}d) for {len(all_dynamic_stocks_list)} dynamic stocks …")
        yf_tasks = [fetch_etf_history(s, days=yf_days) for s in all_dynamic_stocks_list]
        yf_results = await asyncio.gather(*yf_tasks, return_exceptions=True)
        for sym, result in zip(all_dynamic_stocks_list, yf_results):
            if isinstance(result, list) and result:
                histories[sym] = (result, "yfinance")
            else:
                histories[sym] = ([], "unavailable")

    # Fetch quotes for dynamic stocks not yet covered
    missing_quotes = [s for s in all_dynamic_stocks_list if s not in quotes]
    if missing_quotes:
        extra_quotes = await _fetch_all_quotes(missing_quotes)
        quotes.update(extra_quotes)

    # ── 6. Pre-compute per-stock tf returns for leader/laggard ranking ────────
    # Cap at ±500% to filter yfinance adjusted-price anomalies from spin-offs etc.
    _STOCK_RET_CAP = 500.0
    stock_perfs: dict[str, Optional[float]] = {}
    stock_src_map: dict[str, str] = {}
    for sym in all_dynamic_stocks_list:
        bars, src = histories.get(sym, ([], "unavailable"))
        q   = quotes.get(sym, {})
        ret = _perf_for_timeframe(bars, tf, q)
        if ret is not None and abs(ret) > _STOCK_RET_CAP:
            ret = None   # discard obviously corrupt adjusted-price data
        stock_perfs[sym]   = ret
        stock_src_map[sym] = "tradier_batch" if tf == "1D" else src

    # ── 7. Build each theme row ────────────────────────────────────────────────
    # Load manual leader overrides once (bulk read — one Neon query for all themes).
    # Failures are non-fatal: leaders defaults to {} and rows get leader_source="none".
    leaders: dict[str, dict] = {}
    try:
        from data.pg_storage import get_theme_leaders_map
        leaders = get_theme_leaders_map()
    except Exception as _ldr_err:
        print(f"[THEME_RS] Warning: could not load theme leaders: {_ldr_err}")

    rows: list[dict] = []
    for theme_id, meta in THEME_RS_UNIVERSE.items():
        try:
            row = await _build_theme_row(
                theme_id, meta, quotes, histories, tf, stock_perfs, stock_src_map,
                leaders=leaders,
                intraday_bars=intraday_bars,
            )
            if row:
                rows.append(row)
            else:
                print(f"[THEME_RS] No data for '{theme_id}' — skipped")
        except Exception as e:
            print(f"[THEME_RS] Row error '{theme_id}': {e}")

    # ── 8. Score and sort ──────────────────────────────────────────────────────
    rows = _score_and_state(rows, tf, histories, quotes)
    rows.sort(key=lambda r: (r.get("rs_score") or 0, r["performance"].get(tf) or 0), reverse=True)

    print(f"[THEME_RS] Done: {len(rows)} themes")
    return rows


# ── Background refresh (lock-guarded) ──────────────────────────────────────────

async def _locked_refresh(tf: str, force: bool = False) -> None:
    """
    Acquire the per-timeframe lock and run a full compute pass.
    Safe to call concurrently — the lock ensures only one refresh per timeframe
    runs at a time.  In asyncio (single-threaded) the lock.locked() check is
    atomic with the subsequent async-with acquisition.

    Historical TFs (7D/30D/YTD/1Y/5Y) are guarded by a daily cadence limit:
    FMP history will not be fetched more than once every _HIST_FETCH_CADENCE
    seconds unless force=True.  1D is exempt (Tradier quotes, no FMP history).
    Timestamps are persisted to disk so restarts don't reset the guard.
    """
    # ── Daily cadence guard for historical TFs ────────────────────────────────
    if tf != "1D" and not force:
        age = time.time() - _last_computed.get(tf, 0.0)
        if age < _HIST_FETCH_CADENCE:
            remaining = int(_HIST_FETCH_CADENCE - age)
            print(
                f"[THEME_RS] {tf} refresh skipped — daily cadence guard"
                f" (last={int(age)}s ago, next eligible in {remaining}s)"
            )
            return

    lock = _get_lock(tf)
    if lock.locked():
        print(f"[THEME_RS] {tf} refresh skipped — already in progress")
        return

    async with lock:
        t0 = time.time()
        print(f"[THEME_RS] {tf} refresh started")
        try:
            rows = await _compute(tf)
            if rows:
                # ── Count guard: never write a partial result to TTL cache or LKG ──
                # Partial results occur when FMP is blocked/rate-limited/outaged.
                # Without this guard the TTL cache would serve 2 themes every 60s
                # even though the LKG has 60 good themes.
                if len(rows) < _MIN_LKG_THEME_FLOOR:
                    existing_lkg = _load_lkg()
                    if existing_lkg and len(existing_lkg) >= _MIN_LKG_THEME_FLOOR:
                        print(
                            f"[THEME_RS] {tf} PARTIAL result ({len(rows)} themes "
                            f"< floor {_MIN_LKG_THEME_FLOOR}) — NOT written to "
                            f"TTL cache or LKG (existing LKG has {len(existing_lkg)} themes)"
                        )
                        return   # keep existing TTL cache empty → next request serves LKG
                payload = _make_payload(rows, tf)
                cache.set(f"{_CACHE_KEY}:{tf}", payload, _ttl_for_timeframe(tf))
                _save_lkg(rows)
                if tf == "1D":
                    _save_1d_lkg(rows)   # durable intraday curve persistence
                _last_computed[tf] = time.time()
                _save_refresh_ts(tf, _last_computed[tf])   # persist across restarts
                print(
                    f"[THEME_RS] {tf} refresh done in {time.time()-t0:.1f}s "
                    f"({len(rows)} themes)"
                )
            else:
                print(f"[THEME_RS] {tf} refresh returned no rows — LKG/cache preserved")
        except Exception as exc:
            import traceback
            traceback.print_exc()
            print(f"[THEME_RS] {tf} refresh error: {exc}")


# ── Warmup & background loop ───────────────────────────────────────────────────

async def _warmup_loop() -> None:
    """
    Background loop that keeps theme RS caches fresh.
    - Market hours: 1D every ~60s (Tradier real-time).
                   7D/30D/YTD/1Y once per 24h cadence guard (_HIST_FETCH_CADENCE).
    - Off-hours / weekends: loop idles; 60-min weekday TTL and weekend TTL
      (cached until Monday 09:30 ET) cover serving from cache naturally.
    """
    print("[THEME_RS] Warmup loop started")

    # Initial kick: compute all timeframes in background
    for tf in list(_TIMEFRAME_BARS.keys()):
        asyncio.create_task(_locked_refresh(tf))

    while True:
        await asyncio.sleep(15)   # check every 15s — lightweight
        now = time.time()

        if not _is_market_hours():
            continue   # rely on long off-hours TTL

        # 1D: target 60s since last successful compute
        if now - _last_computed.get("1D", 0) >= _TTL_1D_MARKET:
            asyncio.create_task(_locked_refresh("1D"))

        # Historical: target _HIST_FETCH_CADENCE (24h) since last successful compute.
        # _locked_refresh() also enforces this guard internally; the loop check
        # just avoids creating unnecessary tasks.
        for tf in ("7D", "30D", "YTD", "1Y", "5Y"):
            if now - _last_computed.get(tf, 0) >= _HIST_FETCH_CADENCE:
                asyncio.create_task(_locked_refresh(tf))


async def warmup_theme_rs() -> None:
    """
    Called once at startup (non-blocking).
    Loads LKG into all timeframe caches immediately so the first user request
    is always fast, then starts the background refresh loop.

    Schema guard: if the on-disk LKG predates 5Y support (all performance["5Y"]
    values are None), we skip seeding the 5Y cache slot.  This forces the
    warmup loop to compute a fresh 5Y result with the correct bar threshold
    instead of poisoning the cache with all-None 5Y data.
    """
    lkg = _load_lkg()
    has_5y_lkg = _lkg_has_5y(lkg) if lkg else False

    if lkg:
        print(
            f"[THEME_RS] Startup: LKG loaded ({len(lkg)} themes, "
            f"5Y_capable={has_5y_lkg}) — seeding caches"
        )
        for tf in list(_TIMEFRAME_BARS.keys()):
            cache_key = f"{_CACHE_KEY}:{tf}"
            # Schema guard: skip 5Y seed if LKG predates 5Y support.
            if tf == "5Y" and not has_5y_lkg:
                print("[THEME_RS] Startup: skipping 5Y cache seed (pre-5Y LKG)")
                continue
            # 1D is seeded separately below from the intraday LKG so we don't
            # fill the 1D slot with daily date strings that would need clearing.
            if tf == "1D":
                continue
            if cache.get(cache_key) is None:
                seed = _lkg_payload(lkg, tf, source="lkg_startup")
                seed["_cache_set_at"] = time.time()
                cache.set(cache_key, seed, 120)

        # ── 1D: seed from the intraday LKG if available ───────────────────────
        # This provides last-session intraday curves immediately on restart /
        # after-hours / weekends without waiting for the background 1D warmup.
        lkg_1d = _load_1d_lkg()
        if lkg_1d and isinstance(lkg_1d.get("curves"), dict):
            curves_by_id = lkg_1d["curves"]
            # Inject persisted intraday curves — validate basket_hash per theme
            # so curves built from a different proxy_symbols set are discarded.
            rows_1d: list[dict] = []
            stale_count = 0
            for row in lkg:
                tid = row.get("theme_id", "")
                stored = curves_by_id.get(tid)
                if stored is None:
                    # New theme not yet in the LKG — leave curve empty.
                    rows_1d.append({**row, "performance_curve": []})
                    continue
                # Support both old format (bare list) and new format ({curve, basket_hash}).
                if isinstance(stored, list):
                    rows_1d.append({**row, "performance_curve": stored})
                    continue
                stored_curve = stored.get("curve", [])
                stored_hash  = stored.get("basket_hash")
                current_hash = _basket_hash(
                    THEME_RS_UNIVERSE.get(tid, {}).get("proxy_symbols", [])
                )
                if stored_hash and stored_hash != current_hash:
                    # Basket changed since LKG was written — discard stale curve.
                    stale_count += 1
                    rows_1d.append({**row, "performance_curve": []})
                else:
                    rows_1d.append({**row, "performance_curve": stored_curve})

            if stale_count:
                print(
                    f"[THEME_RS] Startup: 1D LKG: {stale_count} theme(s) skipped "
                    f"(basket changed since snapshot)"
                )
            seed_1d = _lkg_payload(rows_1d, "1D", source="lkg_1d_startup")
            seed_1d["_cache_set_at"] = time.time()
            # TTL: 60s if market open (background refresh fires soon),
            #       3600s if off-hours (last-session curves serve until next open)
            cache.set(f"{_CACHE_KEY}:1D", seed_1d, _ttl_for_timeframe("1D"))
            print(
                f"[THEME_RS] Startup: 1D cache seeded from intraday LKG "
                f"(session={lkg_1d.get('session_date', '?')}, "
                f"{len(curves_by_id)} themes with curves, {stale_count} stale)"
            )
        else:
            # No intraday LKG yet — seed 1D with daily LKG (curves will be
            # cleared to [] by _apply_curves_for_tf until first intraday compute)
            if cache.get(f"{_CACHE_KEY}:1D") is None:
                seed_1d = _lkg_payload(lkg, "1D", source="lkg_startup")
                seed_1d["_cache_set_at"] = time.time()
                cache.set(f"{_CACHE_KEY}:1D", seed_1d, 120)
            print("[THEME_RS] Startup: no 1D intraday LKG — 1D curves empty until first market-hours compute")
    else:
        print("[THEME_RS] Startup: no LKG found — cold start, background warmup will populate")

    # Invalidate any stale 5Y in-memory entry that has all-None 5Y values.
    # This handles the case where a previous session cached a pre-fix 5Y result
    # (computed with threshold=1260) that survived via the LKG seed path.
    _5y_key = f"{_CACHE_KEY}:5Y"
    stale = cache.get(_5y_key)
    if stale is not None:
        themes_in_cache = stale.get("themes", [])
        if themes_in_cache and not _lkg_has_5y(themes_in_cache):
            print("[THEME_RS] Startup: stale 5Y cache (all-None 5Y perf) — invalidating")
            cache.delete(_5y_key)

    # Load persisted refresh timestamps before starting the background loop so
    # the daily cadence guard has accurate last-computed values after a restart.
    # If a TF was computed < 24h ago the initial loop kick will be skipped and
    # LKG / cache will serve users until the next eligible window.
    _load_refresh_ts()

    asyncio.create_task(_warmup_loop())
    print("[THEME_RS] Warmup task created (non-blocking)")


# ── Public entry point ─────────────────────────────────────────────────────────

def _apply_classification_filter(payload: dict, classification: str) -> dict:
    """
    Return a shallow copy of *payload* with themes filtered by classification.
    classification must be one of: "all", "sector", "theme", "sub_theme".
    Invalid / unknown values fall back to "all".
    The cache always stores the full unfiltered payload; filtering is applied
    at response time so we never need separate cache keys per classification.
    """
    clf = classification.lower() if classification else "all"
    if clf == "all":
        return payload
    valid = {"sector", "theme", "sub_theme"}
    if clf not in valid:
        return payload
    filtered_themes = [r for r in payload.get("themes", []) if r.get("classification") == clf]
    return {
        **payload,
        "themes":       filtered_themes,
        "theme_count":  len(filtered_themes),
        "classification_filter": clf,
    }


def _tf_start_str(tf: str) -> Optional[str]:
    """Return the ISO date string marking the start of the given TF window.

    Returns None for 1D (no historical curve needed).
    Windows intentionally overshoot slightly (e.g. 380d for 1Y) to capture
    the calendar-year boundary without clipping weekend/holiday gaps.
    """
    from datetime import date as _d, timedelta as _td
    today = _d.today()
    if tf == "7D":   return (today - _td(days=12)).isoformat()
    if tf == "30D":  return (today - _td(days=35)).isoformat()
    if tf == "YTD":  return _d(today.year, 1, 1).isoformat()
    if tf == "1Y":   return (today - _td(days=380)).isoformat()
    if tf == "5Y":   return (today - _td(days=1900)).isoformat()
    return None  # 1D — no curve


def _slice_and_rebase_curve(
    points: list[dict],
    start_str: str,
    max_pts: int = 252,
) -> list[dict]:
    """
    Slice a full-history normalized curve to [start_str, latest] and rebase so
    the first returned point has value_pct = 0.

    Rebasing uses the compounding formula so the sliced curve is self-consistent:
        rebased = ((1 + raw/100) / (1 + base/100) - 1) * 100
    where base is the value_pct of the first point in the slice.

    Downsamples to max_pts via uniform stride after rebasing.
    """
    sliced = [p for p in points if p["date"] >= start_str]
    if not sliced:
        return []

    base_factor = 1.0 + sliced[0]["value_pct"] / 100.0
    if not base_factor:
        return []

    rebased = [
        {
            "date": p["date"],
            "value_pct": round(
                ((1.0 + p["value_pct"] / 100.0) / base_factor - 1.0) * 100.0, 3
            ),
        }
        for p in sliced
    ]

    if len(rebased) > max_pts:
        step = max(1, len(rebased) // max_pts)
        last = rebased[-1]
        rebased = rebased[::step]
        if rebased[-1]["date"] != last["date"]:
            rebased.append(last)

    return rebased


def _apply_curves_for_tf(payload: dict, tf: str) -> dict:
    """
    Return a new payload dict where performance_curve in every theme row has
    been sliced and rebased to the requested TF window.

    The cache and LKG always store the full-history curve built during whatever
    TF compute last ran (could be 5Y with ~1260 pts).  This function applies
    the correct date window at serve time, so a 1Y request always gets a ~252-pt
    1Y curve even if the only cached data was computed for 5Y.

    Creates shallow copies of theme dicts — the cached payload is never mutated.
    """
    start = _tf_start_str(tf)
    if not start:
        # 1D: keep curve only when it was built by a fresh intraday compute —
        # those points have ISO timestamps ("2026-06-22T09:35:00-04:00").
        # LKG / stale payloads carry bare daily date strings ("2021-04-09");
        # serving those as a "today" intraday curve would produce 200-300 wrong
        # daily points.  Clear any non-intraday curve so the frontend shows
        # an empty chart (then gets the real intraday data once the 1D warmup
        # completes, ~60 s after startup or first request).
        def _is_intraday_curve(c: list) -> bool:
            return bool(c) and "T" in str((c[0] or {}).get("date", ""))

        new_themes_1d = [
            theme if _is_intraday_curve(theme.get("performance_curve") or [])
            else {**theme, "performance_curve": []}
            for theme in payload.get("themes", [])
        ]
        return {**payload, "themes": new_themes_1d}

    new_themes = [
        {
            **theme,
            "performance_curve": _slice_and_rebase_curve(
                theme.get("performance_curve") or [], start
            ),
        }
        for theme in payload.get("themes", [])
    ]
    return {**payload, "themes": new_themes}


async def _get_theme_rs_data_raw(
    timeframe: str = "30D",
    force: bool = False,
    classification: str = "all",
) -> dict:
    """
    Internal implementation — returns cache/LKG payload with the full-history
    performance_curve stored as-built (no TF-specific slicing applied here).
    Call get_theme_rs_data() instead, which wraps this with _apply_curves_for_tf.

    Returns full themes-RS payload dict (optionally filtered by classification).

    classification: "all" | "sector" | "theme" | "sub_theme"
      - "all"       → all 60 rows (default, backward-compatible)
      - "sector"    → exactly 11 SPDR sector rows
      - "theme"     → broad theme rows
      - "sub_theme" → narrow sub-theme rows

    Freshness flow:
      1. Cache hit → return immediately (with cache_age_seconds injected).
      2. Cache miss, refresh in progress → return stale LKG immediately.
      3. Cache miss, refresh NOT in progress, LKG available →
           return LKG immediately + kick background refresh.
      4. Cache miss, no LKG (cold start) → run compute synchronously
           (with lock, so concurrent cold requests wait rather than stampede).
      5. Force → skip cache, run compute now (bypasses stale-while-revalidate).

    Classification filter is always applied AFTER fetching from cache so the
    cache always stores the full set regardless of which filter is requested.
    """
    tf = timeframe.upper()
    if tf not in _TIMEFRAME_BARS:
        tf = "30D"

    cache_key = f"{_CACHE_KEY}:{tf}"

    # ── 1. Cache hit ──────────────────────────────────────────────────────────
    if not force:
        hit = cache.get(cache_key)
        if hit is not None:
            # Evict poisoned payloads that slipped in before the count guard existed.
            if len(hit.get("themes", [])) < _MIN_LKG_THEME_FLOOR:
                print(
                    f"[THEME_RS] {tf} evicting poisoned TTL cache "
                    f"({len(hit.get('themes', []))} themes < floor {_MIN_LKG_THEME_FLOOR})"
                )
                try:
                    cache.delete(cache_key)
                except Exception:
                    pass
                # Fall through to LKG / recompute paths below
            else:
                # Validate basket_hash for every theme row before serving.
                # kick_refresh_tf=tf bypasses the active TTL when any row is stale
                # or legacy (no hash) — a background recompute fires immediately.
                validated, _sc = _validate_basket_hashes(hit, kick_refresh_tf=tf)
                return _apply_classification_filter(_add_freshness(validated), classification)

    # ── 2/3. Cache miss — check if refresh already in progress ────────────────
    lock = _get_lock(tf)
    lkg_rows = _load_lkg()

    if lock.locked():
        # Someone is already computing — serve stale immediately
        if lkg_rows:
            print(f"[THEME_RS] {tf} served from LKG — refresh already in progress")
            # Validate basket_hash; no kick_refresh_tf — refresh already running.
            _lkg = _lkg_payload(lkg_rows, tf, source="lkg_refresh_in_progress")
            validated, _ = _validate_basket_hashes(_lkg)
            return _apply_classification_filter(validated, classification)
        # No stale available — wait for the active refresh to finish
        print(f"[THEME_RS] {tf} waiting for in-progress refresh (no LKG available)")
        async with lock:
            hit = cache.get(cache_key)
            if hit is not None:
                validated, _ = _validate_basket_hashes(hit)
                return _apply_classification_filter(_add_freshness(validated), classification)
            # Refresh failed or produced no data — return error-safe empty
            return _apply_classification_filter(
                _lkg_payload([], tf, source="no_data"), classification
            )

    # ── 3. Stale-while-revalidate (LKG exists, not forced) ───────────────────
    if lkg_rows and not force:
        asyncio.create_task(_locked_refresh(tf))
        print(f"[THEME_RS] {tf} kicked background refresh — returning LKG immediately")
        # Validate basket_hash; no kick_refresh_tf — background refresh already queued.
        _lkg = _lkg_payload(lkg_rows, tf, source="lkg_stale_revalidating")
        validated, _ = _validate_basket_hashes(_lkg)
        return _apply_classification_filter(validated, classification)

    # ── 4/5. Cold start or forced — compute synchronously ────────────────────
    async with lock:
        # Double-check after acquiring (another coroutine may have finished)
        if not force:
            hit = cache.get(cache_key)
            if hit is not None:
                validated, _ = _validate_basket_hashes(hit)
                return _apply_classification_filter(_add_freshness(validated), classification)

        print(f"[THEME_RS] {tf} cold compute (force={force})")
        try:
            rows = await _compute(tf)
            if rows:
                # ── Count guard: partial result must not be cached or returned ────
                if len(rows) < _MIN_LKG_THEME_FLOOR:
                    # Prefer a stale-good LKG over serving a partial universe.
                    # Re-load in case _locked_refresh updated it since we last checked.
                    fresh_lkg = _load_lkg()
                    if fresh_lkg and len(fresh_lkg) >= _MIN_LKG_THEME_FLOOR:
                        print(
                            f"[THEME_RS] {tf} cold compute partial ({len(rows)} themes "
                            f"< floor {_MIN_LKG_THEME_FLOOR}) — serving LKG "
                            f"({len(fresh_lkg)} themes) instead"
                        )
                        _lkg = _lkg_payload(fresh_lkg, tf, source="lkg_partial_compute_guard")
                        validated, _ = _validate_basket_hashes(_lkg)
                        return _apply_classification_filter(validated, classification)
                    # No valid LKG either — return partial uncached (last resort).
                    print(
                        f"[THEME_RS] {tf} cold compute partial ({len(rows)} themes) "
                        f"and no valid LKG — returning partial (not cached)"
                    )
                    return _apply_classification_filter(
                        _lkg_payload(rows, tf, source="partial_no_lkg"), classification
                    )
                # Full result — write to TTL cache and LKG
                payload = _make_payload(rows, tf)
                cache.set(cache_key, payload, _ttl_for_timeframe(tf))
                _save_lkg(rows)
                _last_computed[tf] = time.time()
                return _apply_classification_filter(_add_freshness(payload), classification)

            # Empty result — return LKG or empty
            if lkg_rows:
                _lkg = _lkg_payload(lkg_rows, tf, source="lkg_compute_empty")
                validated, _ = _validate_basket_hashes(_lkg)
                return _apply_classification_filter(validated, classification)
            return _apply_classification_filter(
                _lkg_payload([], tf, source="no_data"), classification
            )

        except Exception as exc:
            import traceback
            traceback.print_exc()
            print(f"[THEME_RS] {tf} compute error: {exc} — falling back to LKG")
            lkg_rows2 = _load_lkg()
            if lkg_rows2:
                _lkg = _lkg_payload(lkg_rows2, tf, source="lkg_compute_error")
                validated, _ = _validate_basket_hashes(_lkg)
                return _apply_classification_filter(validated, classification)
            raise


async def get_theme_rs_data(
    timeframe: str = "30D",
    force: bool = False,
    classification: str = "all",
) -> dict:
    """
    Public entry point for the themes RS endpoint.

    Calls _get_theme_rs_data_raw() (cache/LKG/compute), then applies
    _apply_curves_for_tf() to slice and rebase performance_curve to the
    requested TF window before returning.  This ensures a 1Y request always
    receives a 1Y curve, even when the stored full-history curve was built
    during a 5Y compute pass.
    """
    result = await _get_theme_rs_data_raw(timeframe, force, classification)
    return _apply_curves_for_tf(result, timeframe.upper())


# ── Admin / telemetry ──────────────────────────────────────────────────────────

def invalidate_theme_rs_cache() -> None:
    """
    Mark theme RS caches dirty after an admin universe edit (stale-safe).

    Strategy (replaces the old destructive delete-LKG approach):
    - Clears all in-memory TTL cache keys so the next compute cycle uses the
      updated universe instead of a stale cached result.
    - Resets _last_computed so the cadence guard doesn't block an immediate
      background refresh.
    - Does NOT delete the LKG disk file.  The last-good snapshot is preserved
      on disk and served immediately via stale-while-revalidate until a complete
      replacement (>= _MIN_LKG_THEME_FLOOR themes) is available.
    - Sets _ADMIN_DIRTY flag so status endpoints and response payloads expose
      admin_refresh_pending=True.
    - Kicks a background 1D recompute immediately so the new universe is picked
      up as soon as possible without waiting for the next natural TTL expiry.

    Safe to call from sync or async context — no blocking I/O.
    """
    global _ADMIN_DIRTY

    # Clear in-memory TTL cache for every timeframe
    for tf in ("1D", "7D", "30D", "YTD", "1Y", "5Y"):
        key = f"{_CACHE_KEY}:{tf}"
        try:
            cache.delete(key)
        except Exception:
            pass
        _last_computed[tf] = 0.0

    _ADMIN_DIRTY = True
    lkg_count = len(_load_lkg() or [])
    print(
        f"[THEME_RS] Cache marked dirty — LKG preserved ({lkg_count} themes), "
        f"background 1D refresh queued"
    )

    # Kick immediate 1D background refresh (non-blocking, best-effort).
    # The refresh will update LKG only if it returns >= _MIN_LKG_THEME_FLOOR themes.
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.ensure_future(_locked_refresh("1D"))
    except Exception:
        pass


def get_theme_rs_status() -> dict:
    """
    Return a diagnostic snapshot for admin/status endpoints.
    Pure read — no FMP calls, no cache writes, no I/O beyond stat checks.
    """
    from services.theme_merge_layer import (
        ENRICHED_ALL_PROXY_SYMBOLS as ALL_PROXY_SYMBOLS,
        ENRICHED_ALL_CANDIDATE_SYMBOLS as ALL_CANDIDATE_SYMBOLS,
    )

    now = time.time()
    benchmarks = _BENCHMARKS
    unique_proxy_count = len(sorted(set(ALL_PROXY_SYMBOLS + benchmarks)))

    tf_status: dict[str, dict] = {}
    for tf in list(_TIMEFRAME_BARS.keys()):
        last_ts  = _last_computed.get(tf, 0.0)
        age_s    = int(now - last_ts) if last_ts > 0 else None
        cadence_s = _TTL_1D_MARKET if tf == "1D" else _HIST_FETCH_CADENCE
        if last_ts > 0:
            next_eligible_s = max(0, int(cadence_s - (now - last_ts)))
            eligible_now    = (now - last_ts) >= cadence_s
        else:
            next_eligible_s = 0
            eligible_now    = True
        tf_status[tf] = {
            "last_refresh_ts":    round(last_ts, 1) if last_ts > 0 else None,
            "last_refresh_ago_s": age_s,
            "next_eligible_in_s": next_eligible_s,
            "eligible_now":       eligible_now,
            "cadence_s":          cadence_s,
        }

    lkg_rows = _load_lkg()
    return {
        "unique_proxy_bench_count": unique_proxy_count,
        "candidate_symbol_count":   len(ALL_CANDIDATE_SYMBOLS),
        "hist_fetch_cadence_s":     _HIST_FETCH_CADENCE,
        "hist_endpoint_mode":       f"historical-price-eod date-ranged (last {_FMP_HIST_RANGE_DAYS} cal days)",
        "lkg_path":                 str(_LKG_PATH),
        "lkg_exists":               _LKG_PATH.exists(),
        "lkg_theme_count":          len(lkg_rows) if lkg_rows else 0,
        "expected_theme_count":     _EXPECTED_THEME_COUNT,
        "lkg_theme_floor":          _MIN_LKG_THEME_FLOOR,
        "admin_refresh_pending":    _ADMIN_DIRTY,
        "refresh_ts_path":          str(_REFRESH_TS_PATH),
        "refresh_ts_exists":        _REFRESH_TS_PATH.exists(),
        "timeframes":               tf_status,
    }
