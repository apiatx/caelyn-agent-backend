"""
earnings_clean_service.py — Safe FMP-only Upcoming Earnings service (v2).

Safety architecture (replaces the burst-prone v1):
  • ONE shared httpx.AsyncClient per top-level request — no new client per symbol.
  • SEQUENTIAL 7-day chunk fetches — no asyncio.gather burst on calendar calls.
  • Max 5 FMP calendar calls per request (30-day window / 7-day chunks).
  • Profile enrichment: opt-in only, concurrency=5 semaphore, hard cap 50 live fetches.
  • Circuit breaker: 429 → block all FMP calls from THIS service for 60 s.
    Does NOT touch FMP clients used by Home / Sectors / Macro routes.
  • All FMP errors are contained: return partial data, never propagate exceptions.

Public API:
  get_upcoming_clean(...)  → calendar counts + flat event list, logos via URL pattern
  get_day_clean(...)       → single-day events with full enrichment (name, logo, price, mktcap)

Response always includes: status, source, fmpCallsUsed, rateLimited, errors.
"""
from __future__ import annotations

import asyncio
import hashlib
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import httpx

from data.cache import cache

# ── Constants ──────────────────────────────────────────────────────────────────

_FMP_STABLE         = "https://financialmodelingprep.com/stable"
_FMP_LOGO_BASE      = "https://financialmodelingprep.com/image-stock"

_TTL_CAL_CHUNK      = 6  * 3600    # 6 h  — legacy raw FMP chunk (upcoming-clean only)
_TTL_RAW_CHUNK      = 24 * 3600    # 24 h — canonical Mon-Fri raw chunk (fresh)
_TTL_LKG            = 30 * 24 * 3600  # 30 d — last-known-good stale fallback
_TTL_UPCOMING       = 6  * 3600    # 6 h  — merged calendar result
_TTL_PROFILE        = 24 * 3600    # 24 h — company profile
_TTL_RESULT         = 6  * 3600    # 6 h  — top-level route result cache (filtered views)
_TTL_QUOTE          = 15 * 60      # 15 min — live quote / price data

# ── Curated weekly snapshot TTLs ───────────────────────────────────────────────
# Weekly snapshots are built by the background precompute loop (not on page load).
# They live long enough to survive the full Mon-Fri earnings week + weekend.
_TTL_SNAP           = 8 * 24 * 3600   # 8 days — curated weekly snapshot (in-memory)
_TTL_SNAP_LKG       = 45 * 24 * 3600  # 45 days — LKG snapshot (survives stale enrichment)
_SNAP_MAX_AGE_S     = 9 * 24 * 3600   # 9 days  — reject disk snapshots older than this

# ── Snapshot disk directory ─────────────────────────────────────────────────────
# Snapshots are written to disk so they survive server restarts.
# On startup, _load_all_earn_snaps_from_disk() pre-warms the in-memory cache from disk.
_SNAP_DIR = Path(__file__).resolve().parent.parent / "data"

# ── Per-week build state (used by precompute loop + status endpoint) ──────────
# Keyed by mon_str (e.g. "2026-05-11"). Updated by the precompute loop.
_week_states: dict[str, dict] = {}

def _init_week_state(mon: str) -> dict:
    return {
        "status":       "missing",  # fresh | lkg | missing | building | failed
        "last_built":   None,       # unix timestamp (float)
        "last_error":   None,       # str | None
        "last_attempt": None,       # unix timestamp (float)
        "next_retry_at": None,      # monotonic timestamp (float) for backoff
        "retry_count":  0,
    }

def get_week_state(mon: str) -> dict:
    """Return tracked precompute state for a week (by Monday date string)."""
    return _week_states.get(mon, _init_week_state(mon))

_DEFAULT_DAYS       = 14            # default upcoming window (2 weeks)
_MAX_RANGE_DAYS     = 30            # hard cap on date range (30 days)
_MAX_CHUNKS         = 5             # max sequential FMP calendar calls (5 × 7d = 35d covers 30d)

# ── Per-route FMP budget limits ───────────────────────────────────────────────
# Hard maximums on how many sequential calendar-chunk calls each route may use.
# Prevents a single request from exhausting the FMP quota.
_BUDGET_DAY_CHUNKS   = 1   # Day views  (1 × 7d chunk — always enough for 1 day)
_BUDGET_WEEK_CHUNKS  = 2   # Week views (2 × 7d chunks — enough for any Mon–Fri week)
_BUDGET_MONTH_CHUNKS = 5   # Month views (5 × 7d chunks — enough for any 28-31 day month)

# Month-all: zero live profile calls (lightweight calendar-only)
# Month-curated: caps inherited from week-clean engine (WEEK_MAX_LIVE=40 per week, cached)
_MONTH_ALL_LIVE_PROFILES = 0

_MAX_LIVE_DEFAULT   = 30            # default live profile fetches for day-clean enrichment
_MAX_LIVE_CAP       = 50            # absolute ceiling for live profile fetches
_ENRICH_CONCURRENCY = 5             # max concurrent live profile HTTP calls
_FMP_TIMEOUT        = 8.0           # seconds per FMP HTTP call (generous for profile)
_CB_BLOCK_SECS      = 60            # 429 circuit-breaker block duration

_US_MAJOR = {"NASDAQ", "NYSE", "AMEX", "NYSE ARCA", "NYSE MKT", "CBOE", "BATS"}

_MC_MEGA  = 200_000_000_000
_MC_LARGE =  10_000_000_000
_MC_MID   =   2_000_000_000
_MC_SMALL =     300_000_000
_MC_MICRO_FLOOR =  50_000_000   # $50 M — min MC for in-theme small-cap eligibility


# ── Circuit breaker (private — this service only) ─────────────────────────────

_fmp_block_until: float = 0.0   # monotonic timestamp; 0.0 = not blocked


def _is_blocked() -> bool:
    return time.monotonic() < _fmp_block_until


def _set_blocked() -> None:
    global _fmp_block_until
    _fmp_block_until = time.monotonic() + _CB_BLOCK_SECS
    print(f"[earn_clean] ⚠ 429 — blocking FMP for {_CB_BLOCK_SECS}s (this service only)")


# ── Structured telemetry logger (earnings routes only) ────────────────────────

def _log_telemetry(
    route:                   str,
    date_range:              str,
    raw_hits:                int  = 0,
    raw_misses:              int  = 0,
    fmp_calendar_http_calls: int  = 0,
    raw_events_count:        int  = 0,
    live_profile_calls:      int  = 0,
    stale_lkg:               bool = False,
    cache_hit:               bool = False,
    rate_limited:            bool = False,
) -> None:
    """
    Emit one structured telemetry line per earnings route request.
    Never logs API keys.

    Field semantics (all counts are actual FMP HTTP calls, never event counts):
      raw_hits              — Mon-Fri chunk cache hits (0 FMP HTTP calls each)
      raw_misses            — Mon-Fri chunk cache misses (1 FMP HTTP call each)
      fmp_cal_http          — actual HTTP calls to FMP /earnings-calendar endpoint
      raw_events            — number of raw FMP earnings rows returned (before filter)
      fmp_live              — actual HTTP calls to FMP /profile (or /quote) endpoint
      stale                 — true if any chunk was served from 30-day LKG fallback
      cache (HIT/MISS)      — top-level result-cache hit (0 FMP calls of any kind)
      rl                    — true if FMP returned 429 during this request
    """
    cache_str = "HIT" if cache_hit else "MISS"
    rl_str    = str(rate_limited).upper()
    print(
        f"[earn_clean][TEL] route={route} range={date_range} "
        f"raw_hits={raw_hits} raw_misses={raw_misses} "
        f"fmp_cal_http={fmp_calendar_http_calls} raw_events={raw_events_count} "
        f"fmp_live={live_profile_calls} "
        f"stale={str(stale_lkg).lower()} "
        f"cache={cache_str} rl={rl_str}"
    )


# ── Low-level FMP call (shared client, circuit-breaker aware) ─────────────────

async def _fmp_get(
    endpoint: str,
    params: dict,
    cache_key: str,
    ttl: int,
    api_key: str,
    client: httpx.AsyncClient,
    call_counter: list[int],
) -> tuple[Any, bool]:
    """
    GET /stable/{endpoint} with:
      • cache-first (returns immediately on hit, no HTTP)
      • circuit breaker check (returns [], True when blocked)
      • 429 detection → triggers circuit breaker
      • timeout handling (returns [], False)

    Returns (result, rate_limited).
    """
    hit = cache.get(cache_key)
    if hit is not None:
        return hit, False

    if _is_blocked():
        print(f"[earn_clean] CB active — skipping {endpoint}")
        return [], True

    p = dict(params)
    p["apikey"] = api_key
    call_counter[0] += 1
    t0 = time.monotonic()

    try:
        resp = await client.get(f"{_FMP_STABLE}/{endpoint}", params=p)
        ms = int((time.monotonic() - t0) * 1000)

        if resp.status_code == 429:
            _set_blocked()
            print(f"[earn_clean] FMP {endpoint} 429 ms={ms}")
            return [], True

        if resp.status_code not in (200, 201):
            print(f"[earn_clean] FMP {endpoint} status={resp.status_code} ms={ms}")
            return [], False

        result = resp.json()
        rows = len(result) if isinstance(result, list) else (1 if result else 0)
        print(f"[earn_clean] FMP {endpoint} 200 rows={rows} ms={ms}")
        if result:
            cache.set(cache_key, result, ttl)
        return result, False

    except httpx.TimeoutException:
        ms = int((time.monotonic() - t0) * 1000)
        print(f"[earn_clean] FMP {endpoint} timeout ms={ms}")
        return [], False
    except Exception as exc:
        ms = int((time.monotonic() - t0) * 1000)
        print(f"[earn_clean] FMP {endpoint} error={exc} ms={ms}")
        return [], False


# ══════════════════════════════════════════════════════════════════════════════
# CURATED WEEKLY SNAPSHOT — disk-backed, restart-safe, batch-built
# ──────────────────────────────────────────────────────────────────────────────
# Architecture:
#   • Enrichment runs in a BACKGROUND LOOP (not on page load).
#   • Results are persisted to disk as earnings_snap_{mon}_{fri}.json.
#   • On startup, _load_all_earn_snaps_from_disk() warms the in-memory cache.
#   • Page loads hit in-memory cache → disk → LKG; never trigger enrichment.
#   • Month view composes weekly snapshots via _get_snap_or_lkg_fast(); no inline enrichment.
#
# IMPORTANT — candidate universe rule:
#   Earnings discovery starts from FMP /stable/earnings-calendar events.
#   THEME_RS_UNIVERSE, ETF holdings, and proxy_symbols are NOT used as a hard filter.
#   Theme/anchor/bottleneck data may boost or label events (scoring only), not exclude them.
# ══════════════════════════════════════════════════════════════════════════════

def _snap_ck(mon: str, fri: str) -> str:
    """In-memory cache key for a curated weekly snapshot."""
    return f"earnings:snap:week:{mon}:{fri}"

def _snap_lkg_ck(mon: str, fri: str) -> str:
    """In-memory cache key for a LKG weekly snapshot."""
    return f"earnings:snap:lkg:week:{mon}:{fri}"

def _snap_disk_path(mon: str, fri: str) -> Path:
    """Disk path for a curated weekly snapshot (compact date format avoids path separator issues)."""
    m = mon.replace("-", "")   # "2026-04-27" → "20260427"
    f = fri.replace("-", "")   # "2026-05-01" → "20260501"
    return _SNAP_DIR / f"earnings_snap_{m}_{f}.json"

def _snap_lkg_disk_path(mon: str, fri: str) -> Path:
    m = mon.replace("-", "")
    f = fri.replace("-", "")
    return _SNAP_DIR / f"earnings_snap_lkg_{m}_{f}.json"

def _write_earn_snap_to_disk(path: Path, data: dict) -> None:
    """Atomic disk write: write to .tmp then rename so partial writes are never served."""
    import json as _js
    try:
        _SNAP_DIR.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(_js.dumps(data, separators=(",", ":")), encoding="utf-8")
        tmp.replace(path)
    except Exception as e:
        print(f"[earn_snap] disk write failed {path.name}: {e}")

def _read_earn_snap_from_disk(path: Path, max_age_s: int = _SNAP_MAX_AGE_S) -> Optional[dict]:
    """Read and validate a snapshot from disk. Returns None if missing, corrupt, or stale."""
    import json as _js
    try:
        if not path.exists() or path.suffix == ".tmp":
            return None
        data = _js.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return None
        cached_at = data.get("cached_at", 0)
        if isinstance(cached_at, (int, float)) and time.time() - cached_at > max_age_s:
            return None
        # Reject empty snapshots unless confirmed genuinely empty (genuine 0-event weeks)
        if not data.get("allEvents") and not data.get("genuinelyEmpty"):
            return None
        return data
    except Exception:
        return None

def _get_snap_or_lkg_fast(mon: str, fri: str) -> Optional[dict]:
    """
    Return a curated weekly snapshot WITHOUT triggering any enrichment or FMP calls.

    Priority: in-memory fresh → disk fresh → in-memory LKG → disk LKG → None.
    Used by month-curated and page loads to serve precomputed data instantly.
    Returns None only if no data at all is available for this week.
    """
    ck     = _snap_ck(mon, fri)
    lkg_ck = _snap_lkg_ck(mon, fri)

    # 1. Fresh in-memory snapshot (fastest path)
    snap = cache.get(ck)
    if snap is not None:
        return snap

    # 2. Fresh disk snapshot → warm in-memory cache
    snap = _read_earn_snap_from_disk(_snap_disk_path(mon, fri))
    if snap is not None:
        cache.set(ck, snap, _TTL_SNAP)
        return snap

    # 3. LKG in-memory (stale but usable)
    snap = cache.get(lkg_ck)
    if snap is not None:
        return {**snap, "staleLKG": True}

    # 4. LKG disk (stale but usable)
    snap = _read_earn_snap_from_disk(_snap_lkg_disk_path(mon, fri), max_age_s=_TTL_SNAP_LKG)
    if snap is not None:
        cache.set(lkg_ck, snap, _TTL_SNAP_LKG)
        return {**snap, "staleLKG": True}

    return None

def _load_all_earn_snaps_from_disk() -> None:
    """
    Load all curated weekly snapshots from disk into the in-memory cache at startup.
    Called synchronously at server start so the first page load never needs enrichment.
    Skips corrupted or stale files silently.
    """
    import glob as _glob
    import json as _js
    loaded = 0
    skipped = 0
    for path_str in _glob.glob(str(_SNAP_DIR / "earnings_snap_????????_????????.json")):
        path = Path(path_str)
        try:
            data = _read_earn_snap_from_disk(path)
            if data is None:
                skipped += 1
                continue
            # Parse mon/fri from filename: earnings_snap_20260427_20260501.json
            stem  = path.stem   # "earnings_snap_20260427_20260501"
            parts = stem.split("_")  # ["earnings", "snap", "20260427", "20260501"]
            if len(parts) < 4:
                skipped += 1
                continue
            mon_raw, fri_raw = parts[2], parts[3]
            if len(mon_raw) != 8 or len(fri_raw) != 8:
                skipped += 1
                continue
            mon = f"{mon_raw[:4]}-{mon_raw[4:6]}-{mon_raw[6:8]}"
            fri = f"{fri_raw[:4]}-{fri_raw[4:6]}-{fri_raw[6:8]}"
            cache.set(_snap_ck(mon, fri), data, _TTL_SNAP)
            loaded += 1
        except Exception as e:
            print(f"[earn_snap] startup load failed {path.name}: {e}")
            skipped += 1

    # Also load LKG files
    for path_str in _glob.glob(str(_SNAP_DIR / "earnings_snap_lkg_????????_????????.json")):
        path = Path(path_str)
        try:
            data = _read_earn_snap_from_disk(path, max_age_s=_TTL_SNAP_LKG)
            if data is None:
                continue
            stem  = path.stem   # "earnings_snap_lkg_20260427_20260501"
            parts = stem.split("_")  # ["earnings", "snap", "lkg", "20260427", "20260501"]
            if len(parts) < 5:
                continue
            mon_raw, fri_raw = parts[3], parts[4]
            if len(mon_raw) != 8 or len(fri_raw) != 8:
                continue
            mon = f"{mon_raw[:4]}-{mon_raw[4:6]}-{mon_raw[6:8]}"
            fri = f"{fri_raw[:4]}-{fri_raw[4:6]}-{fri_raw[6:8]}"
            cache.set(_snap_lkg_ck(mon, fri), data, _TTL_SNAP_LKG)
        except Exception:
            pass

    if loaded:
        print(f"[earn_snap] Pre-warmed {loaded} weekly curated snapshots from disk ({skipped} skipped/stale)")

# ── Canonical Mon-Fri chunk helper ────────────────────────────────────────────

def _canonical_week_chunks(
    from_date: str,
    to_date:   str,
    max_weeks: int = _MAX_CHUNKS,
) -> list[tuple[str, str]]:
    """
    Return canonical Mon-Fri week boundaries that overlap [from_date, to_date].

    Mon-Fri alignment means the SAME cache keys are produced regardless of
    which view (day / week / month / curated) triggers the fetch first.

    Example  — April 2026:
      weeks → [2026-03-30:2026-04-03, 2026-04-06:2026-04-10, …, 2026-04-27:2026-05-01]
    A subsequent call for week 2026-04-27→2026-05-01 reuses the last chunk.
    """
    try:
        start = datetime.strptime(from_date, "%Y-%m-%d").date()
        end   = datetime.strptime(to_date,   "%Y-%m-%d").date()
    except ValueError:
        return []
    # Snap back to Monday of the week containing start
    mon   = start - timedelta(days=start.weekday())
    weeks: list[tuple[str, str]] = []
    while mon <= end and len(weeks) < min(max_weeks, _MAX_CHUNKS):
        fri = mon + timedelta(days=4)
        weeks.append((mon.strftime("%Y-%m-%d"), fri.strftime("%Y-%m-%d")))
        mon += timedelta(days=7)
    return weeks


# ── Canonical raw FMP fetcher — shared by ALL six earnings views ──────────────

async def get_raw_earnings_chunks(
    from_date: str,
    to_date:   str,
    api_key:   str,
    max_weeks: int = _BUDGET_MONTH_CHUNKS,
) -> dict:
    """
    Canonical raw FMP earnings-calendar fetcher shared by Day/Week/Month All
    and Day/Week/Month Curated.

    Cache architecture
    ------------------
    Fresh  key:  earnings:raw:week:{mon}:{fri}  — TTL 6 h
    LKG    key:  earnings:raw:lkg:week:{mon}:{fri} — TTL 30 d
      • On every successful FMP fetch, both keys are written.
      • On FMP 429 / error, the LKG key is read and served with staleLKG=True.
      • Circuit breaker (this service only) also returns LKG when blocked.
      • Home / Sectors / Macro are on separate FMP clients — unaffected.

    Returns
    -------
    {
      "rows":         list[dict],   # raw FMP rows filtered to [from_date, to_date]
      "rawHits":      int,          # chunks served from fresh cache (0 FMP calls each)
      "rawMisses":    int,          # chunks fetched live from FMP
      "fmpCallsUsed": int,          # live FMP HTTP calls made
      "rateLimited":  bool,         # any 429 encountered
      "staleLKG":     bool,         # any stale LKG data returned
    }
    """
    weeks        = _canonical_week_chunks(from_date, to_date, max_weeks)
    raw_hits     = 0
    raw_misses   = 0
    fmp_calls    = 0
    rate_limited = False
    stale_lkg    = False
    merged:      list[dict]           = []
    seen:        set[tuple[str, str]] = set()

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(_FMP_TIMEOUT),
        follow_redirects=True,
    ) as client:
        for mon_str, fri_str in weeks:
            fresh_ck = f"earnings:raw:week:{mon_str}:{fri_str}"
            lkg_ck   = f"earnings:raw:lkg:week:{mon_str}:{fri_str}"

            # 1. Fresh cache hit — zero FMP cost
            fresh = cache.get(fresh_ck)
            if fresh is not None:
                raw_hits += 1
                for row in (fresh if isinstance(fresh, list) else []):
                    k = (row.get("symbol", ""), row.get("date", ""))
                    if k not in seen:
                        seen.add(k)
                        merged.append(row)
                continue

            raw_misses += 1

            # 2. Circuit-breaker active — fall back to LKG, don't call FMP
            if _is_blocked():
                lkg = cache.get(lkg_ck)
                if lkg is not None:
                    stale_lkg = True
                    for row in (lkg if isinstance(lkg, list) else []):
                        k = (row.get("symbol", ""), row.get("date", ""))
                        if k not in seen:
                            seen.add(k)
                            merged.append(row)
                rate_limited = True
                continue

            # 3. Live FMP fetch
            fmp_calls += 1
            t0 = time.monotonic()
            try:
                resp = await client.get(
                    f"{_FMP_STABLE}/earnings-calendar",
                    params={"from": mon_str, "to": fri_str, "apikey": api_key},
                )
                ms = int((time.monotonic() - t0) * 1000)

                if resp.status_code == 429:
                    _set_blocked()
                    print(f"[earn_clean] raw-chunk {mon_str}→{fri_str} 429 ms={ms}")
                    lkg = cache.get(lkg_ck)
                    if lkg is not None:
                        stale_lkg = True
                        for row in (lkg if isinstance(lkg, list) else []):
                            k = (row.get("symbol", ""), row.get("date", ""))
                            if k not in seen:
                                seen.add(k)
                                merged.append(row)
                    rate_limited = True
                    break  # stop — don't burn more quota

                if resp.status_code not in (200, 201):
                    print(f"[earn_clean] raw-chunk {mon_str}→{fri_str} status={resp.status_code} ms={ms}")
                    lkg = cache.get(lkg_ck)
                    if lkg is not None:
                        stale_lkg = True
                        for row in (lkg if isinstance(lkg, list) else []):
                            k = (row.get("symbol", ""), row.get("date", ""))
                            if k not in seen:
                                seen.add(k)
                                merged.append(row)
                    continue

                rows_data = resp.json()
                if not isinstance(rows_data, list):
                    rows_data = []
                print(f"[earn_clean] raw-chunk {mon_str}→{fri_str} 200 rows={len(rows_data)} ms={ms}")

                # Write both fresh and LKG on every successful fetch
                if rows_data:
                    cache.set(fresh_ck, rows_data, _TTL_RAW_CHUNK)
                    cache.set(lkg_ck,   rows_data, _TTL_LKG)

                for row in rows_data:
                    k = (row.get("symbol", ""), row.get("date", ""))
                    if k not in seen:
                        seen.add(k)
                        merged.append(row)

            except httpx.TimeoutException:
                ms = int((time.monotonic() - t0) * 1000)
                print(f"[earn_clean] raw-chunk {mon_str}→{fri_str} timeout ms={ms}")
                lkg = cache.get(lkg_ck)
                if lkg is not None:
                    stale_lkg = True
                    for row in (lkg if isinstance(lkg, list) else []):
                        k = (row.get("symbol", ""), row.get("date", ""))
                        if k not in seen:
                            seen.add(k)
                            merged.append(row)
            except Exception as exc:
                ms = int((time.monotonic() - t0) * 1000)
                print(f"[earn_clean] raw-chunk {mon_str}→{fri_str} error={exc} ms={ms}")

    # Filter merged rows to exact requested date range
    try:
        from_d = datetime.strptime(from_date, "%Y-%m-%d").date()
        to_d   = datetime.strptime(to_date,   "%Y-%m-%d").date()

        def _in_range(row: dict) -> bool:
            d_str = row.get("date", "")
            if not d_str:
                return False
            try:
                return from_d <= datetime.strptime(d_str, "%Y-%m-%d").date() <= to_d
            except ValueError:
                return False

        merged = [r for r in merged if _in_range(r)]
    except ValueError:
        pass  # bad date strings — return unfiltered

    merged.sort(key=lambda r: r.get("date", ""))
    print(
        f"[earn_clean] raw-chunks weeks={len(weeks)} rows={len(merged)} "
        f"hits={raw_hits} misses={raw_misses} fmp={fmp_calls} "
        f"range={from_date}→{to_date}"
    )
    return {
        "rows":         merged,
        "rawHits":      raw_hits,
        "rawMisses":    raw_misses,
        "fmpCallsUsed": fmp_calls,
        "rateLimited":  rate_limited,
        "staleLKG":     stale_lkg,
    }


# ── Legacy fetcher — used only by get_upcoming_clean ─────────────────────────

async def _fetch_earnings_range(
    from_date:  str,
    to_date:    str,
    api_key:    str,
    client:     httpx.AsyncClient,
    call_counter: list[int],
    max_chunks: int = _MAX_CHUNKS,
) -> tuple[list[dict], bool]:
    """
    Fetch FMP earnings-calendar in sequential 7-day chunks.

    Hard constraints:
      • Sequential — no asyncio.gather burst.
      • max_chunks (default _MAX_CHUNKS=5) FMP calls max; extra days are dropped.
        Per-route budgets: Day=1, Week=2, Month=5.
      • On 429, stops immediately and returns partial data.
      • 429 circuit breaker covers only this service — Home/Sectors/Macro unaffected.

    Returns (rows, rate_limited).
    """
    master_ck = f"fmp:earncln:cal:v3:{from_date}:{to_date}"
    hit = cache.get(master_ck)
    if hit is not None:
        return (hit if isinstance(hit, list) else []), False

    try:
        start = datetime.strptime(from_date, "%Y-%m-%d").date()
        end   = datetime.strptime(to_date,   "%Y-%m-%d").date()
    except ValueError:
        return [], False

    effective_max = min(max_chunks, _MAX_CHUNKS)   # never exceed global hard cap

    chunks: list[tuple[str, str]] = []
    cur = start
    while cur <= end and len(chunks) < effective_max:
        chunk_end = min(cur + timedelta(days=6), end)
        chunks.append((cur.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
        cur = chunk_end + timedelta(days=1)

    seen: set[tuple[str, str]] = set()
    merged: list[dict] = []
    rate_limited = False

    for f, t in chunks:                         # ← sequential, NOT gather
        ck = f"fmp:earncln:chunk:v3:{f}:{t}"
        rows, rl = await _fmp_get(
            "earnings-calendar", {"from": f, "to": t},
            ck, _TTL_CAL_CHUNK, api_key, client, call_counter,
        )
        if rl:
            rate_limited = True
            break                               # stop on 429 — don't burn more quota
        for row in (rows if isinstance(rows, list) else []):
            key = (row.get("symbol", ""), row.get("date", ""))
            if key not in seen:
                seen.add(key)
                merged.append(row)

    merged.sort(key=lambda r: r.get("date", ""))
    print(
        f"[earn_clean] calendar chunks={len(chunks)} rows={len(merged)} "
        f"range={from_date}→{to_date} calls_so_far={call_counter[0]}"
    )
    if merged:
        cache.set(master_ck, merged, _TTL_UPCOMING)
    return merged, rate_limited


# ── Helpers ───────────────────────────────────────────────────────────────────

def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _date_plus(days: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(days=days)).strftime("%Y-%m-%d")


def _clamp_range(from_date: str, to_date: str) -> tuple[str, str]:
    """Silently clamp to_date so range is at most _MAX_RANGE_DAYS days."""
    try:
        start   = datetime.strptime(from_date, "%Y-%m-%d").date()
        end     = datetime.strptime(to_date,   "%Y-%m-%d").date()
        max_end = start + timedelta(days=_MAX_RANGE_DAYS - 1)
        if end > max_end:
            return from_date, max_end.strftime("%Y-%m-%d")
        return from_date, to_date
    except ValueError:
        return from_date, to_date


def _safe_float(v) -> Optional[float]:
    try:
        return float(v) if v is not None and v != "" else None
    except (TypeError, ValueError):
        return None


def _mc_bucket(mc: Optional[float]) -> Optional[str]:
    if mc is None:
        return None
    if mc >= _MC_MEGA:  return "mega"
    if mc >= _MC_LARGE: return "large"
    if mc >= _MC_MID:   return "mid"
    if mc >= _MC_SMALL: return "small"
    return "micro"


def _event_id(symbol: str, date: str) -> str:
    return hashlib.md5(f"earnings:{symbol}:{date}".encode()).hexdigest()[:16]


def _is_polymarket_row(title: str, symbol: str) -> bool:
    if title.strip().lower().startswith("will "):
        return True
    if symbol and (len(symbol) > 12 or " " in symbol or "/" in symbol):
        return True
    return False


def _logo_url(sym: str) -> str:
    """Return the FMP image-stock CDN URL for a ticker — no API call needed."""
    return f"{_FMP_LOGO_BASE}/{sym}.png"


def _normalize_row(row: dict) -> Optional[dict]:
    sym  = (row.get("symbol") or "").strip().upper()
    date = (row.get("date")   or "").strip()
    if not sym or not date:
        return None

    name  = (row.get("name") or row.get("companyName") or sym).strip()
    title = f"{name} Earnings" if name != sym else f"{sym} Earnings"
    if _is_polymarket_row(title, sym):
        return None

    logo = _logo_url(sym)

    return {
        "id":               _event_id(sym, date),
        "date":             date,
        "symbol":           sym,
        "companyName":      name if name != sym else None,
        "logo":             logo,
        "image":            logo,
        "price":            None,
        "changesPercentage": None,
        "marketCap":        None,
        "marketCapBucket":  None,
        "eventType":        "earnings",
        "eventLabel":       "Earnings",
        "title":            title,
        "time":             row.get("time") or None,
        "period":           row.get("fiscalDateEnding") or row.get("period") or None,
        "epsEstimated":     _safe_float(row.get("epsEstimated")),
        "epsActual":        _safe_float(row.get("epsActual")),
        "revenueEstimated": _safe_float(row.get("revenueEstimated")),
        "revenueActual":    _safe_float(row.get("revenueActual")),
        "source":           "fmp",
        "raw":              row,
    }


# ── Safe profile enrichment (concurrency=5, shared client) ───────────────────

async def _enrich_events(
    events: list[dict],
    api_key: str,
    client: httpx.AsyncClient,
    call_counter: list[int],
    max_live: int,
    concurrency: int = _ENRICH_CONCURRENCY,
) -> tuple[list[dict], bool]:
    """
    Enrich events with FMP company profile data (companyName, logo, price, mktcap, etc).

    Rules:
      • Cache hits: resolved instantly — no HTTP, don't count against max_live.
      • Live fetches: capped at max_live (default 30, never > _MAX_LIVE_CAP=50).
      • Concurrency: semaphore(_ENRICH_CONCURRENCY=5) limits simultaneous HTTP calls.
      • Shared client: no new AsyncClient per symbol.
      • 429: circuit breaker triggers, remaining tasks return [] immediately.

    Returns (enriched_events, rate_limited).
    """
    ck_base = "fmp:co_profile:v2:"
    unique_syms = list(dict.fromkeys(ev["symbol"] for ev in events if ev.get("symbol")))

    cached_syms:   list[str] = []
    uncached_syms: list[str] = []
    for sym in unique_syms:
        if cache.get(f"{ck_base}{sym}") is not None:
            cached_syms.append(sym)
        else:
            uncached_syms.append(sym)

    live_syms = uncached_syms[:max_live]
    skipped   = len(uncached_syms) - len(live_syms)
    if skipped:
        print(f"[earn_clean] enrich: cached={len(cached_syms)} live={len(live_syms)} skipped={skipped}")

    all_syms = cached_syms + live_syms
    sem = asyncio.Semaphore(concurrency)

    async def _bounded_profile(sym: str) -> tuple[str, dict]:
        ck = f"{ck_base}{sym}"
        hit = cache.get(ck)
        if hit is not None:
            return sym, (hit if isinstance(hit, dict) else {})
        async with sem:
            rows, _ = await _fmp_get(
                "profile", {"symbol": sym},
                ck, _TTL_PROFILE, api_key, client, call_counter,
            )
        if isinstance(rows, list) and rows:
            row = rows[0]
            cache.set(ck, row, _TTL_PROFILE)
            return sym, row
        return sym, {}

    raw_results = await asyncio.gather(
        *[_bounded_profile(sym) for sym in all_syms],
        return_exceptions=True,
    )

    profiles: dict[str, dict] = {}
    for r in raw_results:
        if isinstance(r, Exception):
            continue
        sym, prof = r
        profiles[sym] = prof

    rate_limited = _is_blocked()

    out: list[dict] = []
    for ev in events:
        sym = ev.get("symbol", "")
        p   = profiles.get(sym, {})
        mc  = _safe_float(p.get("mktCap") or p.get("marketCap"))
        ev  = dict(ev)

        # Company identity — profile wins; fall back to ticker-derived logo
        profile_logo = p.get("image") or p.get("logo") or None
        ev["companyName"]      = (
            p.get("companyName") or p.get("name")
            or ev.get("companyName") or (sym or None)
        )
        ev["logo"]             = profile_logo or ev.get("logo") or _logo_url(sym)
        ev["image"]            = ev["logo"]
        ev["price"]            = _safe_float(p.get("price"))
        ev["changesPercentage"] = _safe_float(p.get("changePercentage") or p.get("changesPercentage") or p.get("changes"))
        ev["marketCap"]        = mc
        ev["marketCapBucket"]  = _mc_bucket(mc)
        ev["sector"]           = p.get("sector") or None
        ev["industry"]         = p.get("industry") or None
        ev["_exchange"]        = p.get("exchangeShortName") or p.get("exchange") or ""
        out.append(ev)

    return out, rate_limited


# ── Sort helpers ──────────────────────────────────────────────────────────────

def _sort_enriched(events: list[dict]) -> list[dict]:
    """US major → marketCap desc → revenueEst → epsEst → symbol."""
    def _key(ev: dict):
        ex      = (ev.get("_exchange") or "").upper()
        us      = 0 if ex in _US_MAJOR else 1
        mc      = ev.get("marketCap") or 0
        has_rev = 0 if ev.get("revenueEstimated") is not None else 1
        has_eps = 0 if ev.get("epsEstimated")      is not None else 1
        return (us, -mc, has_rev, has_eps, ev.get("symbol") or "")
    sorted_evs = sorted(events, key=_key)
    for ev in sorted_evs:
        ev.pop("_exchange", None)
    return sorted_evs


def _sort_basic(events: list[dict]) -> list[dict]:
    """revenueEstimated desc (None last), then symbol alpha."""
    def _key(ev: dict):
        rev = ev.get("revenueEstimated")
        return (-(rev if rev is not None else -1e18), ev.get("symbol") or "")
    return sorted(events, key=_key)


# ── Scope / search ────────────────────────────────────────────────────────────

def _apply_scope(
    events: list[dict],
    search: Optional[str],
    scope: Optional[str],
    watchlist: set[str],
    portfolio: set[str],
) -> list[dict]:
    out = []
    search_lc = search.strip().lower() if search else None
    for ev in events:
        sym   = (ev.get("symbol") or "").upper()
        cname = (ev.get("companyName") or "").lower()
        if scope == "watchlist" and sym not in watchlist:
            continue
        if scope == "portfolio" and sym not in portfolio:
            continue
        if search_lc and search_lc not in sym.lower() and search_lc not in cname:
            continue
        out.append(ev)
    return out


def _load_watchlist() -> set[str]:
    try:
        from services.watchlist_service import list_watchlists, load_watchlist
        syms: set[str] = set()
        for wl in (list_watchlists() or [])[:5]:
            wl_id = wl.get("id")
            if not wl_id:
                continue
            store = load_watchlist(wl_id)
            if not store:
                continue
            for t in store.get("tickers", []):
                if isinstance(t, str):
                    syms.add(t.upper())
                elif isinstance(t, dict) and t.get("symbol"):
                    syms.add(t["symbol"].upper())
        return syms
    except Exception:
        return set()


def _load_portfolio() -> set[str]:
    """Load portfolio symbols from the canonical Neon-backed portfolio store.

    Uses portfolio_store.load_active_holdings() — the exact same source as
    GET /api/portfolio/holdings — so scope=portfolio on the Earnings Calendar
    always reflects what the user saved on the Portfolio page.

    Never reads legacy JSON files, never globs portfolio_holdings_*.json, and
    never unions stale demo holdings.
    """
    try:
        from data.portfolio_store import load_active_holdings  # type: ignore
        holdings = load_active_holdings()
        syms: set[str] = set()
        for h in holdings:
            ticker = (h.get("ticker") or h.get("symbol") or "").upper().strip()
            if ticker:
                syms.add(ticker)
        print(
            f"[earnings_scope] _load_portfolio() → Neon-backed store: "
            f"count={len(syms)} first_20={sorted(syms)[:20]}"
        )
        return syms
    except Exception as e:
        print(f"[earnings_scope] _load_portfolio() error: {e}")
        return set()


# ── Shared enrichment for user-scope (portfolio / watchlist) events ────────────

async def _hydrate_user_scope_events(
    events: list[dict],
    api_key: str,
    universe: str = "portfolio",
) -> tuple[list[dict], str, str]:
    """
    Enrich user-scope earnings events with three layers.  Any layer failure
    leaves the events intact — events are never removed on enrichment error.

    Layer 1 — FMP company profile (companyName, logo, sector, price EOD baseline)
    Layer 2 — Tradier live quote  (price, change, changePercent, quoteSource,
               quoteUpdatedAt).  Falls back to FMP profile price when Tradier
               is unavailable or the symbol is not covered.
    Layer 3 — FMP last earnings   (lastEarningsDate, lastEpsEstimate/Actual,
               lastRevenueEstimate/Actual, lastFiscalPeriod, lastSource).
               Per-symbol 24-h cache — never blocks or fails the response.

    Returns (enriched_events, quote_status, last_earn_status).
    """
    if not events:
        return events, "empty", "empty"

    syms: list[str] = list(dict.fromkeys(
        (ev.get("symbol") or "").upper()
        for ev in events if ev.get("symbol")
    ))
    if not syms:
        return events, "no_symbols", "no_symbols"

    call_counter = [0]

    async with httpx.AsyncClient(timeout=10.0) as client:

        # ── Layer 1: FMP company profile ──────────────────────────────────────
        try:
            events, _ = await _enrich_events(
                events, api_key, client, call_counter,
                max_live=min(len(events), 30),
            )
        except Exception as _ep:
            print(f"[{universe}_hydrate] FMP profile error: {_ep}")

        # ── Layer 2: Tradier live quote ───────────────────────────────────────
        tradier_quotes: dict[str, dict] = {}
        quote_status = "skipped"
        try:
            import os as _os
            _tradier_key = _os.environ.get("TRADIER_API_KEY")
            if _tradier_key:
                from data.tradier_provider import TradierProvider as _TradierProvider  # type: ignore
                _tradier = _TradierProvider(_tradier_key)
                from data.tradier_budget import lane as _earn_lane
                with _earn_lane("quotes"):
                    _raw_q = await asyncio.wait_for(_tradier.get_quotes(syms), timeout=8.0)
                for q in (_raw_q or []):
                    s = (q.get("symbol") or "").upper()
                    if s:
                        tradier_quotes[s] = q
                quote_status = f"ok:{len(tradier_quotes)}/{len(syms)}"
            else:
                quote_status = "no_tradier_key"
        except Exception as _eq:
            print(f"[{universe}_hydrate] Tradier quote error: {_eq}")
            quote_status = "error"

        _quote_ts = datetime.utcnow().isoformat() + "Z"
        for ev in events:
            sym = (ev.get("symbol") or "").upper()
            q   = tradier_quotes.get(sym)
            if q:
                ev["price"]          = q.get("last")
                ev["change"]         = q.get("change")
                ev["changePercent"]  = q.get("change_percentage")
                ev["percentChange"]  = q.get("change_percentage")
                ev["quoteSource"]    = "tradier"
                ev["quoteUpdatedAt"] = _quote_ts
            else:
                # Keep FMP profile price if already set by Layer 1
                if ev.get("price") is not None:
                    ev.setdefault("quoteSource",    "fmp_profile")
                    ev.setdefault("quoteUpdatedAt", _quote_ts)
                ev.setdefault("change",        None)
                ev.setdefault("changePercent", None)
                ev.setdefault("percentChange", None)
                ev.setdefault("quoteSource",    None)
                ev.setdefault("quoteUpdatedAt", None)

        # ── Layer 3: FMP last (most-recent past) earnings ─────────────────────
        last_earn_status = "skipped"
        try:
            _today_s  = datetime.utcnow().strftime("%Y-%m-%d")
            _sem      = asyncio.Semaphore(5)

            async def _fetch_last(sym: str) -> tuple[str, dict | None]:
                ck_result = f"fmp:last_earn:v1:{sym}"
                hit = cache.get(ck_result)
                if hit is not None:
                    return sym, (hit if isinstance(hit, dict) else None)
                async with _sem:
                    rows, _ = await _fmp_get(
                        "historical/earning_calendar",
                        {"symbol": sym},
                        f"fmp:last_earn_raw:v1:{sym}", _TTL_PROFILE,
                        api_key, client, call_counter,
                    )
                if not isinstance(rows, list) or not rows:
                    return sym, None
                past = [r for r in rows if (r.get("date") or "") < _today_s]
                if not past:
                    return sym, None
                last = max(past, key=lambda r: r.get("date", ""))
                result: dict = {
                    "lastEarningsDate":    last.get("date"),
                    "lastEpsEstimate":     _safe_float(last.get("epsEstimated")),
                    "lastEpsActual":       _safe_float(last.get("epsActual")),
                    "lastRevenueEstimate": _safe_float(last.get("revenueEstimated")),
                    "lastRevenueActual":   _safe_float(last.get("revenueActual")),
                    "lastFiscalPeriod":    last.get("fiscalDateEnding") or last.get("period"),
                    "lastReportTime":      last.get("time"),
                    "lastSource":          "fmp",
                }
                cache.set(ck_result, result, _TTL_PROFILE)
                return sym, result

            _le_results = await asyncio.gather(
                *[_fetch_last(s) for s in syms],
                return_exceptions=True,
            )
            last_map: dict[str, dict] = {}
            for _r in _le_results:
                if isinstance(_r, Exception):
                    continue
                _s, _d = _r
                if _d:
                    last_map[_s] = _d

            for ev in events:
                sym = (ev.get("symbol") or "").upper()
                le  = last_map.get(sym)
                if le:
                    ev.update(le)
                else:
                    ev.setdefault("lastEarningsDate",    None)
                    ev.setdefault("lastEpsEstimate",     None)
                    ev.setdefault("lastEpsActual",       None)
                    ev.setdefault("lastRevenueEstimate", None)
                    ev.setdefault("lastRevenueActual",   None)
                    ev.setdefault("lastSource",          None)

            last_earn_status = f"ok:{len(last_map)}/{len(syms)}"
        except Exception as _ele:
            print(f"[{universe}_hydrate] last earnings error: {_ele}")
            last_earn_status = "error"

    print(
        f"[{universe}_hydrate] syms={len(syms)} fmp_calls={call_counter[0]} "
        f"quote={quote_status} last_earn={last_earn_status}"
    )
    return events, quote_status, last_earn_status


# ── Public API ─────────────────────────────────────────────────────────────────

async def get_upcoming_clean(
    api_key: str,
    from_date: Optional[str] = None,
    to_date:   Optional[str] = None,
    search:    Optional[str] = None,
    scope:     Optional[str] = None,
    limit:     int = 5000,
) -> dict:
    """
    FMP earnings calendar for a date range — counts + event list with logos.

    Hard limits:
      • Default window: today → today + 14 days.
      • Max window: 30 days (silently clamped — never 400, just truncated).
      • Max 5 sequential FMP calendar calls (5 × 7-day chunks).
      • Logos populated via FMP image-stock CDN URL (no API call needed).
      • No profile enrichment (names are ticker when FMP doesn't include them).
      • One shared AsyncClient for the whole request lifecycle.

    Response: { status, source, fmpCallsUsed, rateLimited, errors,
                from, to, events, eventsByDate, countsByDate }
    """
    frm = from_date or _today()
    to  = to_date   or _date_plus(_DEFAULT_DAYS)
    frm, to = _clamp_range(frm, to)

    call_counter: list[int] = [0]

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(_FMP_TIMEOUT),
        follow_redirects=True,
    ) as client:
        raw_rows, rate_limited = await _fetch_earnings_range(
            frm, to, api_key, client, call_counter,
        )

    events: list[dict] = []
    for row in raw_rows:
        ev = _normalize_row(row)
        if ev:
            events.append(ev)

    watchlist = _load_watchlist()
    portfolio = _load_portfolio()
    events = _apply_scope(events, search, scope, watchlist, portfolio)
    events.sort(key=lambda e: (e.get("date", ""), e.get("symbol", "")))

    # Build date aggregations from the FULL event set BEFORE applying the flat limit.
    # countsByDate and eventsByDate must reflect all FMP events in the range, not
    # just the first `limit` rows — otherwise days beyond the limit appear empty.
    events_by_date: dict[str, list] = {}
    counts_by_date: dict[str, int]  = {}
    for ev in events:
        d = ev.get("date", "")
        if d:
            events_by_date.setdefault(d, []).append(ev)
            counts_by_date[d] = counts_by_date.get(d, 0) + 1

    # Trim flat events list only (calendar tiles use countsByDate; day detail uses eventsByDate)
    if limit and len(events) > limit:
        events = events[:limit]

    return {
        "asOf":         _today(),
        "source":       "fmp",
        "from":         frm,
        "to":           to,
        "count":        len(events),
        "events":       events,
        "eventsByDate": events_by_date,
        "countsByDate": counts_by_date,
        "status":       "partial" if rate_limited else "ok",
        "fmpCallsUsed": call_counter[0],
        "rateLimited":  rate_limited,
        "errors":       (["FMP rate limit hit — partial data"] if rate_limited else []),
    }


async def get_day_clean(
    api_key:  str,
    date:     str,
    search:   Optional[str] = None,
    scope:    Optional[str] = None,
    limit:    int  = 500,
    enrich:   bool = True,
    max_live: int  = _MAX_LIVE_DEFAULT,
) -> dict:
    """
    FMP earnings events for a single selected date with full enrichment.

    enrich=True (default):
      • 1 FMP calendar call + up to max_live profile calls.
      • Returns companyName, logo (URL or CDN), price, changesPercentage, marketCap,
        sector, industry for each event.
      • Profiles are cached 24 h — subsequent requests for the same symbols are instant.
      • Concurrency: 5 simultaneous profile HTTP calls (semaphore).
      • Shared AsyncClient — no new client per symbol.
      • Timeout: 8 s per call.
      • 429: circuit breaker activates, enrichment stops, returns partial data.
      • Sort: US exchange → marketCap desc → revenueEst → epsEst → symbol.

    enrich=False:
      • 1 FMP calendar call only. Logos still populated via CDN URL.
      • Sort: revenueEstimated desc → symbol alpha.

    Response: { status, source, fmpCallsUsed, rateLimited, errors, date, count, events }
    """
    max_live = min(max(0, max_live), _MAX_LIVE_CAP)

    # ── Watchlist short-circuit ────────────────────────────────────────────────
    # For scope=watchlist we bypass FMP direct calendar calls and use the
    # symbol-driven user_earnings_service path so the day view shows only events
    # for the user's actual saved Watchlist tickers — not the FMP raw pool.
    if scope == "watchlist":
        return await _get_day_watchlist_scope(api_key, date, search, "clean")
    # ── End watchlist short-circuit ────────────────────────────────────────────

    # ── Portfolio short-circuit ────────────────────────────────────────────────
    if scope == "portfolio":
        return await _get_day_portfolio_scope(api_key, date, search, "clean")
    # ── End portfolio short-circuit ────────────────────────────────────────────

    # Phase 1: raw calendar fetch — shared Mon-Fri chunk cache
    raw_result   = await get_raw_earnings_chunks(
        date, date, api_key, max_weeks=_BUDGET_DAY_CHUNKS,
    )
    raw_rows     = raw_result["rows"]
    rate_limited = raw_result["rateLimited"]
    stale_lkg    = raw_result["staleLKG"]
    cal_calls    = raw_result["fmpCallsUsed"]
    raw_hits     = raw_result["rawHits"]
    raw_misses   = raw_result["rawMisses"]

    events: list[dict] = []
    for row in raw_rows:
        ev = _normalize_row(row)
        if ev:
            events.append(ev)

    watchlist = _load_watchlist()
    portfolio = _load_portfolio()
    events = _apply_scope(events, search, scope, watchlist, portfolio)

    enrich_calls = 0
    # Phase 2: live profile enrichment — separate client, only when needed
    if enrich and not rate_limited and events:
        enrich_counter: list[int] = [0]
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(_FMP_TIMEOUT),
            follow_redirects=True,
        ) as enrich_client:
            events, enrich_rl = await _enrich_events(
                events, api_key, enrich_client, enrich_counter, max_live,
            )
        enrich_calls = enrich_counter[0]
        if enrich_rl:
            rate_limited = True
        events = _sort_enriched(events)
    else:
        events = _sort_basic(events)

    if limit and len(events) > limit:
        events = events[:limit]

    return {
        "asOf":         _today(),
        "source":       "fmp",
        "date":         date,
        "count":        len(events),
        "events":       events,
        "status":       "partial" if rate_limited else "ok",
        "fmpCallsUsed": cal_calls + enrich_calls,
        "rateLimited":  rate_limited,
        "staleLKG":     stale_lkg,
        "errors":       (["FMP rate limit hit — partial data"] if rate_limited else []),
    }


# ══════════════════════════════════════════════════════════════════════════════
# WEEK-CLEAN — This Week curated earnings view
# ══════════════════════════════════════════════════════════════════════════════

# ── Theme baskets (deterministic — no AI) ─────────────────────────────────────

_THEME_BASKETS: dict[str, set[str]] = {
    "AI Buildout": {
        "NVDA", "AMD", "AVGO", "MRVL", "MU", "TSM", "ASML", "ARM",
        "SMCI", "DELL", "HPE", "ANET", "VRT", "ETN", "ORCL",
        "GOOGL", "MSFT", "AMZN", "META",
    },
    "AI Data Center / Power": {
        "VRT", "ETN", "PWR", "CEG", "VST", "NRG", "GE", "GEV",
        "SMR", "OKLO", "DELL", "SMCI",
    },
    "Semiconductor Bottlenecks": {
        "ASML", "AMAT", "LRCX", "KLAC", "TER", "AEHR", "ACLS", "FORM", "COHR",
    },
    "AI Networking": {
        "ANET", "AVGO", "MRVL", "CIEN", "CSCO", "NOK", "ERIC",
    },
    "Cloud / Software Infra": {
        "MSFT", "AMZN", "GOOGL", "META", "ORCL", "SNOW", "DDOG",
        "NET", "CRWD", "PANW", "MDB", "NOW",
    },
    "Aerospace / Defense / Space": {
        "RKLB", "ASTS", "LMT", "RTX", "NOC", "BA", "LHX",
        "TDG", "HEI", "PLTR", "KTOS", "IRDM",
    },
    "Nuclear / Power / Electrification": {
        "CEG", "VST", "NRG", "ETN", "PWR", "GEV", "SMR", "OKLO", "BWXT",
    },
    "Crypto / Fintech": {
        "COIN", "HOOD", "MSTR", "SQ", "PYPL", "IBKR", "CME",
    },
}

_THEME_ANCHORS:     set[str] = {"NVDA", "GOOGL", "AMZN", "MSFT", "META", "AVGO", "TSM", "ASML"}
_THEME_BOTTLENECKS: set[str] = {
    "MU", "MRVL", "VRT", "ETN", "ANET", "LRCX", "KLAC", "AMAT", "RKLB", "ASTS", "PWR", "GEV",
}

_ALL_THEME_SYMS: set[str] = {sym for basket in _THEME_BASKETS.values() for sym in basket}

# ── Extended thematic universe (static registry + dynamic cache — sync, ~0ms) ──
# Reads _STATIC_TICKER_TO_THEMES (built at import from THEME_MAP + THEME_ETF_UNIVERSE +
# THEME_RS_UNIVERSE — ~400 tickers) and the cached dynamic thematic universe.
# Refreshed at most every 15 minutes.  Never raises.  No HTTP calls.
_ext_theme_map_cache:     dict[str, list[str]] = {}
_ext_theme_map_built_at:  float                = 0.0
_EXT_THEME_TTL            = 15 * 60   # 15-minute refresh interval


def _get_ext_theme_map() -> dict[str, list[str]]:
    """
    Return {sym: [theme_name, …]} for all tickers in the platform's extended thematic universe.

    Sources (sync, no HTTP, no async):
      1. thematic_context_provider._STATIC_TICKER_TO_THEMES  — built at import (~400 tickers)
      2. dynamic_thematic_universe.get_cached_thematic_universe().theme_map — 15-min TTL

    Never raises.  Falls back to whatever partial data was collected.
    Cached in module-level dict; refreshed every _EXT_THEME_TTL seconds.
    """
    global _ext_theme_map_cache, _ext_theme_map_built_at
    now = time.monotonic()
    if _ext_theme_map_built_at > 0 and now - _ext_theme_map_built_at < _EXT_THEME_TTL:
        return _ext_theme_map_cache

    merged: dict[str, list[str]] = {}

    # Source 1: static registry (no I/O — built at Python import time)
    try:
        from services.thematic_context_provider import (
            _build_static_registry,
            _STATIC_TICKER_TO_THEMES,
        )
        _build_static_registry()
        for sym, themes in _STATIC_TICKER_TO_THEMES.items():
            if themes:
                merged[sym] = list(themes)
    except Exception as e:
        print(f"[earn_clean] ext_theme_map static: {e}")

    # Source 2: cached dynamic universe (sync read — ~0ms, never blocks even when cold)
    try:
        from services.dynamic_thematic_universe import get_cached_thematic_universe
        dyn = get_cached_thematic_universe()
        for sym, meta in (dyn.get("theme_map") or {}).items():
            sym_u = sym.upper()
            name  = (meta.get("theme_name") or "") if isinstance(meta, dict) else ""
            if not name:
                continue
            existing = merged.get(sym_u, [])
            if name not in existing:
                merged[sym_u] = existing + [name]
    except Exception as e:
        print(f"[earn_clean] ext_theme_map dynamic: {e}")

    _ext_theme_map_cache    = merged
    _ext_theme_map_built_at = now
    print(f"[earn_clean] ext_theme_map refreshed: {len(merged)} tickers across extended universe")
    return merged


# ── Quality / liquidity scoring constants ─────────────────────────────────────

# Matches preferred shares, warrants, units by symbol suffix pattern.
# Intentionally conservative — avoids hitting normal tickers like BRK.B, GOOGL.
_QUAL_PREF_RE = re.compile(
    r'[-.]P[A-Z]$'     # -PA, -PB, .PA … preferred class
    r'|^.{2,}-P$'      # TICKER-P  (e.g. JPM-P, WFC-P)
    r'|[/.]WS$'        # /WS, .WS  warrant
    r'|[-.]WT$'        # -WT       warrant alt
    r'|\^'             # ^ in symbol (Bloomberg-style preferred notation)
    r'|\.PFD$'         # .PFD      preferred
    r'|[-.]U$',        # -U, .U    SPAC unit
    re.IGNORECASE,
)

# Company name keywords that indicate preferred/warrant instruments
_QUAL_NAME_PREF_RE = re.compile(r'\bpreferred\b|\bwarrant\b', re.IGNORECASE)

# ADR / foreign ordinary share indication in company name (softer penalty)
_QUAL_NAME_ADR_RE  = re.compile(r'\bADR\b|\bordinary\s+share', re.IGNORECASE)

# Score adjustments
_QL_MAJOR_US_BOOST  =  15   # NYSE / Nasdaq / AMEX listed
_QL_OTC_PENALTY     = -10   # OTC / foreign / unknown (unless theme-exempt or large/mega cap)
_QL_PREF_SYM_HIT    = -25   # high-confidence preferred / warrant from symbol pattern
_QL_PREF_NAME_HIT   = -20   # preferred / warrant detected in company name
_QL_ADR_PENALTY     =  -8   # ADR (softer — often legitimate large-cap instruments)

_WEEK_CAND_MAX      = 400  # max raw candidates forwarded to enrichment (raised for ext theme coverage)
_MAX_WEEK_ENRICH    = 300  # max events passed to _enrich_events (raised; extras use profile cache)
_WEEK_MAX_LIVE      = 40   # hard cap on LIVE FMP profile HTTP calls (unchanged — FMP budget safe)
_WEEK_PER_DAY_FLOOR = 60   # guaranteed candidate slots per trading day (raised from 40 — helps Thu/Fri)
_WEEK_CONCURRENCY   = 2    # semaphore for week-clean enrichment
_WEEK_LIMIT_DEFAULT = 13   # default per-session cap (raised from 10 for ~30% more events)
_WEEK_TOTAL_DEFAULT = 100  # default topEvents cap (raised from 75 for ~33% more events)
_WEEK_LIMIT_MAX     = 26   # hard ceiling for limit_per_session (raised from 20)
_WEEK_TOTAL_MAX     = 165  # hard ceiling for max_total (raised from 125)

# importanceScore threshold above which an event is flagged isFocus=True.
# Anchors/bottlenecks typically score 60-110; well-covered mid-caps ~45-55.
_FOCUS_SCORE_THRESHOLD = 65

# ── Day-curated caps ───────────────────────────────────────────────────────────
_DAY_CAND_MAX      = 80   # max raw candidates after pre-filter
_DAY_MAX_ENRICH    = 80   # max events passed to enrichment (cache-first)
_DAY_MAX_LIVE      = 20   # hard cap on live FMP profile calls for day-curated
_DAY_CONCURRENCY   = 2    # semaphore for day-curated enrichment
_DAY_LIMIT_DEFAULT = 25   # default results returned (raised from 19 for ~30% more events)
_DAY_LIMIT_MAX     = 50   # hard ceiling for day-curated limit (raised from 38)

# ── Month-curated caps ─────────────────────────────────────────────────────────
_MONTH_CAND_MAX        = 500  # max raw candidates after pre-filter (whole month)
_MONTH_MAX_ENRICH      = 200  # max events passed to enrichment (cache-first)
_MONTH_MAX_LIVE        = 40   # hard cap on live FMP profile calls for month-curated
_MONTH_CONCURRENCY     = 2    # semaphore for month-curated enrichment
_MONTH_PER_DAY_DEFAULT = 9    # default max curated events per day (raised from 7 for ~30% more)
_MONTH_PER_DAY_MAX     = 16   # hard ceiling for max_per_day (raised from 12)

_WEEKDAY_NAMES = {0: "Monday", 1: "Tuesday", 2: "Wednesday",
                  3: "Thursday", 4: "Friday", 5: "Saturday", 6: "Sunday"}


# ── Week-clean helpers ─────────────────────────────────────────────────────────

def _symbol_themes(sym: str) -> list[str]:
    """Return list of theme basket names the symbol belongs to."""
    return [name for name, syms in _THEME_BASKETS.items() if sym in syms]


def _session(time_str: Optional[str]) -> str:
    """Map FMP time string to session key."""
    if not time_str:
        return "unknown"
    t = time_str.lower().strip()
    if t in {"bmo", "before market open", "pre-market", "pre market", "pre", "pm"}:
        return "preMarket"
    if t in {"amc", "after market close", "after-hours", "after hours", "after", "ah", "postmarket"}:
        return "afterHours"
    if t in {"dmh", "during market", "during market hours", "during", "midday"}:
        return "duringMarket"
    return "unknown"


def _pre_score(ev: dict, watchlist: set[str], portfolio: set[str]) -> dict:
    """
    Deterministic pre-score using only raw FMP calendar fields + theme knowledge.
    No profile data needed — safe to call before enrichment.

    Theme scoring (capped at 40 total):
      in hardcoded basket: +20 | anchor: +25 | bottleneck: +20
      in extended universe only (static registry / dynamic cache): +12 (softer boost)

    qualityLiquidity (partial — exchange component added after enrichment):
      preferred/warrant symbol pattern: -25
      preferred/warrant company name:   -20
      ADR / ordinary share name:         -8
      Any theme membership (hardcoded or extended) cancels pre-enrichment penalty.
    """
    sym = (ev.get("symbol") or "").upper()
    has_eps = ev.get("epsEstimated") is not None
    has_rev = ev.get("revenueEstimated") is not None

    is_anchor     = sym in _THEME_ANCHORS
    is_bottleneck = sym in _THEME_BOTTLENECKS
    in_theme      = sym in _ALL_THEME_SYMS

    # Extended thematic universe — ~400 tickers from static registry + dynamic cache.
    # Sync, ~0ms, no HTTP.  Only applied for tickers NOT already in hardcoded baskets.
    ext_map        = _get_ext_theme_map()
    ext_theme_list = [t for t in ext_map.get(sym, []) if t]
    in_ext         = bool(ext_theme_list) and not in_theme and not is_anchor and not is_bottleneck

    raw_theme = 0
    if in_theme:      raw_theme += 20
    if is_anchor:     raw_theme += 25
    if is_bottleneck: raw_theme += 20
    if in_ext:        raw_theme += 12   # softer boost for extended-only tickers
    theme_score = min(raw_theme, 40)   # cap at 40

    is_portfolio = sym in portfolio
    is_watchlist = sym in watchlist
    wp_score = 25 if is_portfolio else (20 if is_watchlist else 0)

    est_score = 0
    if has_eps: est_score += 5
    if has_rev: est_score += 5
    if has_eps and has_rev: est_score += 5

    # ── Quality / liquidity pre-score (symbol + name patterns only) ──────────
    cname = ev.get("companyName") or ""
    ql_pre = 0
    if _QUAL_PREF_RE.search(sym):
        ql_pre = _QL_PREF_SYM_HIT                  # -25: high-confidence preferred/warrant
    elif _QUAL_NAME_PREF_RE.search(cname):
        ql_pre = _QL_PREF_NAME_HIT                  # -20: name says "preferred" or "warrant"
    elif _QUAL_NAME_ADR_RE.search(cname):
        ql_pre = _QL_ADR_PENALTY                    # -8:  ADR / ordinary share

    # Any theme membership (hardcoded or extended) cancels the pre-enrichment penalty
    if ql_pre < 0 and (in_theme or is_anchor or is_bottleneck or in_ext):
        ql_pre = 0

    # Merge hardcoded + extended theme names for display (deduped, hardcoded first)
    hc_themes     = _symbol_themes(sym)
    merged_themes = hc_themes + [t for t in ext_theme_list if t not in hc_themes]

    return {
        "score":        theme_score + wp_score + est_score + ql_pre,
        "isAnchor":     is_anchor,
        "isBottleneck": is_bottleneck,
        "themes":       merged_themes,
        "breakdown": {
            "marketCap":            0,        # filled after enrichment
            "theme":                theme_score,
            "watchlistPortfolio":   wp_score,
            "estimateAvailability": est_score,
            "priceMomentum":        0,        # filled after enrichment
            "qualityLiquidity":     ql_pre,   # exchange component added in _apply_full_score
        },
    }


def _apply_full_score(ev: dict, pre: dict) -> tuple[int, dict]:
    """
    Compute final importance score after enrichment adds marketCap / price % / exchange.
    Returns (total_score, updated_breakdown).

    qualityLiquidity (exchange component — only for enriched events):
      Major US exchange (NYSE/Nasdaq/AMEX/…): +15
      OTC / foreign / unknown exchange:       -10
        Exception: skipped if large/mega cap OR in any theme basket.
    """
    mc     = ev.get("marketCap")
    pct    = ev.get("changesPercentage")
    bucket = _mc_bucket(mc)
    sym    = (ev.get("symbol") or "").upper()

    mc_score = {"mega": 40, "large": 30, "mid": 15, "small": 5}.get(bucket or "", 0)

    pm_score = 0
    if pct is not None:
        if pct > 7:
            pm_score = 8
        elif pct > 3:
            pm_score = 5
        elif pct < -5 and bucket in ("mega", "large"):
            pm_score = 5

    # ── Exchange quality component ────────────────────────────────────────────
    ql_pre      = pre["breakdown"].get("qualityLiquidity", 0)
    ql_exchange = 0
    exchange    = (ev.get("_exchange") or "").upper()

    if exchange:  # only present on enriched events
        is_major_us   = exchange in _US_MAJOR
        is_large_plus = bucket in ("mega", "large")
        is_theme_sym  = sym in _ALL_THEME_SYMS or pre.get("isAnchor") or pre.get("isBottleneck")

        if is_major_us:
            ql_exchange = _QL_MAJOR_US_BOOST          # +15
        elif not is_large_plus and not is_theme_sym:
            ql_exchange = _QL_OTC_PENALTY             # -10

    ql_total = ql_pre + ql_exchange

    breakdown = {
        "marketCap":            mc_score,
        "theme":                pre["breakdown"].get("theme", 0),
        "watchlistPortfolio":   pre["breakdown"].get("watchlistPortfolio", 0),
        "estimateAvailability": pre["breakdown"].get("estimateAvailability", 0),
        "priceMomentum":        pm_score,
        "qualityLiquidity":     ql_total,
    }
    return pre["score"] + mc_score + pm_score + ql_exchange, breakdown


def _build_empty_days(week_start, week_end) -> list[dict]:
    """Return a days[] with zero-count slots for each date in range."""
    days = []
    cur = week_start
    while cur <= week_end:
        d_str = cur.strftime("%Y-%m-%d")
        label = cur.strftime("%a, %b ") + str(cur.day)
        days.append({
            "date":         d_str,
            "label":        label,
            "weekday":      _WEEKDAY_NAMES.get(cur.weekday(), ""),
            "count":        0,
            "preMarket":    [],
            "afterHours":   [],
            "duringMarket": [],
            "unknown":      [],
            "entries":      [],
        })
        cur += timedelta(days=1)
    return days


def _decorate_event(ev: dict, pre: dict, score: int, breakdown: dict) -> dict:
    """Attach week-clean fields to a (possibly enriched) normalized event."""
    mc     = ev.get("marketCap")
    bucket = _mc_bucket(mc) or "unknown"
    base   = {k: v for k, v in ev.items() if k != "_exchange"}
    return {
        **base,
        "session":         _session(ev.get("time")),
        "priceChangePct":  ev.get("changesPercentage"),
        "marketCapBucket": bucket,
        "themeTags":       pre.get("themes", []),
        "isThemeAnchor":   pre.get("isAnchor", False),
        "isBottleneck":    pre.get("isBottleneck", False),
        "importanceScore": score,
        "isFocus":         score >= _FOCUS_SCORE_THRESHOLD,
        "scoreBreakdown":  breakdown,
        "source":          "fmp",
    }


# ── Week-clean candidate helpers ──────────────────────────────────────────────

# OTC/foreign ticker pattern: 4-5 uppercase letters ending in F (foreign) or Y (ADR OTC)
_OTC_TICKER_RE = re.compile(r'^[A-Z]{4,5}[FY]$')

# Broad "fund / trust / preferred / rights / note" guard for company names
_JUNK_NAME_RE = re.compile(
    r'\b(fund|trust\s+preferred|rights|notes?\s+due|unit\s+series|'
    r'acquisition\s+corp|blank\s+check)\b',
    re.IGNORECASE,
)


def _is_raw_candidate(ev: dict, watchlist: set[str], portfolio: set[str]) -> bool:
    """
    Fast pre-filter (no HTTP) — decide whether a raw FMP row is worth enriching.

    Returns True only for events that have a realistic chance of being eligible
    for the curated This Week board.
    """
    sym = (ev.get("symbol") or "").upper()

    # Always keep: explicit theme, anchor, bottleneck (hardcoded baskets)
    if sym in _ALL_THEME_SYMS or sym in _THEME_ANCHORS or sym in _THEME_BOTTLENECKS:
        return True

    # Always keep: extended thematic universe (static registry + dynamic cache — ~400 tickers)
    if sym in _get_ext_theme_map():
        return True

    # Always keep: user watchlist / portfolio
    if sym in watchlist or sym in portfolio:
        return True

    # Hard drop: preferred/warrant/unit symbol pattern
    if _QUAL_PREF_RE.search(sym):
        return False

    # Hard drop: preferred/warrant keyword in company name
    cname = ev.get("companyName") or ""
    if _QUAL_NAME_PREF_RE.search(cname) or _JUNK_NAME_RE.search(cname):
        return False

    # Soft drop: obvious OTC foreign ticker (4-5 letters + F or Y) unless has BOTH estimates
    if _OTC_TICKER_RE.match(sym):
        if ev.get("epsEstimated") is None or ev.get("revenueEstimated") is None:
            return False

    # Pass: has at least one estimate signal (most meaningful filter for unknown names)
    if ev.get("epsEstimated") is not None or ev.get("revenueEstimated") is not None:
        return True

    # Pass: short ticker (≤4 chars) — likely US-listed; enrich and decide later
    if len(sym) <= 4:
        return True

    # Default: exclude (5-letter tickers with no estimates and no theme = likely OTC junk)
    return False


def _is_eligible(ev: dict, pre: dict, watchlist: set[str], portfolio: set[str]) -> bool:
    """
    Post-enrichment eligibility gate for the curated This Week board.

    An event must pass at least one eligibility rule (A-D).
    Hard exclusions override everything except explicit theme / watchlist / portfolio.
    """
    sym      = (ev.get("symbol") or "").upper()
    mc       = ev.get("marketCap")
    exchange = (ev.get("_exchange") or "").upper()
    bucket   = _mc_bucket(mc)
    cname    = (ev.get("companyName") or "").strip()
    themes   = pre.get("themes", [])
    is_anchor    = pre.get("isAnchor", False)
    is_bottleneck = pre.get("isBottleneck", False)

    # Rule B: Explicit theme/anchor/bottleneck — always eligible
    if is_anchor or is_bottleneck or themes:
        return True

    # Rule C: User watchlist / portfolio — always eligible
    if sym in watchlist or sym in portfolio:
        return True

    # ── Hard exclusions (for everything not in theme / watchlist / portfolio) ──

    # Missing or trivial company name (enrichment didn't resolve it).
    # Exception: if it's on a major US exchange with both estimates, allow through —
    # FMP sometimes returns no companyName for legitimate listed companies.
    if not cname or cname.upper() == sym:
        _exc_exchange = (ev.get("_exchange") or "").upper()
        _has_both_est = (
            ev.get("epsEstimated") is not None
            and ev.get("revenueEstimated") is not None
        )
        if not (_exc_exchange in _US_MAJOR and _has_both_est):
            return False

    # Preferred/warrant pattern guard (belt-and-suspenders after pre-filter)
    if _QUAL_PREF_RE.search(sym) or _QUAL_NAME_PREF_RE.search(cname):
        return False

    # Junk name keywords
    if _JUNK_NAME_RE.search(cname):
        return False

    # Unknown market cap AND unknown/missing exchange → not enough signal
    if (bucket is None or bucket == "unknown") and exchange not in _US_MAJOR:
        return False

    # Missing market cap AND not on a major US exchange → exclude
    if mc is None and exchange not in _US_MAJOR:
        return False

    # ── Eligibility rules ─────────────────────────────────────────────────────

    # Rule A: Major/liquid company
    is_major_us  = exchange in _US_MAJOR
    is_large_mc  = mc is not None and mc >= 5_000_000_000   # ≥ $5 B
    is_mid_plus  = bucket in ("mega", "large", "mid")

    if is_major_us or is_large_mc or is_mid_plus:
        return True

    # Rule D: Known company + both estimates + not OTC-looking ticker
    has_rev      = ev.get("revenueEstimated") is not None
    has_eps      = ev.get("epsEstimated") is not None
    is_otc_like  = bool(_OTC_TICKER_RE.match(sym))

    if has_rev and has_eps and not is_otc_like:
        return True

    # Rule E: In-theme small cap ≥ $50 M — allow smaller names that are thematically
    # relevant but don't make major US exchanges or reach mid-cap size.
    # (theme/anchor/bottleneck already return True via Rule B above; this catches
    # edge cases where profile enrichment didn't resolve exchange but MC is known.)
    if mc is not None and mc >= _MC_MICRO_FLOOR and (themes or is_anchor or is_bottleneck):
        return True

    return False


# ── Shared curated pipeline ────────────────────────────────────────────────────

async def _run_curated_pipeline(
    raw_events:    list[dict],
    watchlist:     set[str],
    portfolio:     set[str],
    client:        "httpx.AsyncClient",
    api_key:       str,
    call_counter:  list[int],
    rate_limited:  bool,
    max_candidates: int = _WEEK_CAND_MAX,
    max_enrich:     int = _MAX_WEEK_ENRICH,
    max_live:       int = _WEEK_MAX_LIVE,
    concurrency:    int = _WEEK_CONCURRENCY,
) -> tuple[list[dict], bool]:
    """
    Shared curated scoring + eligibility pipeline used by week-clean,
    day-curated, and month-curated.

    Phases
    ------
    1. Pre-filter raw events → candidates (≤ max_candidates)
    2. Pre-score all candidates (no HTTP)
    3. Select top max_enrich by pre-score for enrichment
    4. Enrich pool (cache-first, max_live live calls, concurrency-capped semaphore)
    5. Eligibility gate → full score → decorate

    Returns (decorated_events, rate_limited).
    Decorated events carry the full week-clean shape:
      session, priceChangePct, marketCapBucket, themeTags,
      isThemeAnchor, isBottleneck, importanceScore, scoreBreakdown.
    """
    if not raw_events or rate_limited:
        return [], rate_limited

    # Phase 1 — pre-filter
    candidates = [ev for ev in raw_events if _is_raw_candidate(ev, watchlist, portfolio)]

    def _cand_key(ev: dict):
        sym      = (ev.get("symbol") or "").upper()
        in_theme = sym in _ALL_THEME_SYMS or sym in _THEME_ANCHORS or sym in _THEME_BOTTLENECKS
        in_wp    = sym in watchlist or sym in portfolio
        has_rev  = ev.get("revenueEstimated") is not None
        has_eps  = ev.get("epsEstimated") is not None
        return (
            0 if (in_theme or in_wp) else 1,
            0 if (has_rev and has_eps) else 1,
            0 if has_rev else 1,
            len(sym),
        )

    # Per-day floor: guarantee each trading day gets at least _WEEK_PER_DAY_FLOOR
    # candidate slots before the global max_candidates cap is applied.
    # Without this, days with no theme symbols can end up with zero candidates
    # when other days' theme/estimates events fill all global slots → blank days.
    _by_date: dict[str, list[dict]] = {}
    for ev in candidates:
        d = ev.get("date", "")
        _by_date.setdefault(d, []).append(ev)
    for d in _by_date:
        _by_date[d].sort(key=_cand_key)

    _floor     = _WEEK_PER_DAY_FLOOR
    guaranteed: list[dict] = []
    overflow:   list[dict] = []
    for d in sorted(_by_date):
        day_evs = _by_date[d]
        guaranteed.extend(day_evs[:_floor])
        overflow.extend(day_evs[_floor:])

    overflow.sort(key=_cand_key)
    _remaining = max(0, max_candidates - len(guaranteed))

    # Deduplicate by symbol across guaranteed + overflow fill
    _seen_syms: set[str] = set()
    candidates = []
    for ev in guaranteed + overflow[:_remaining]:
        sym = ev.get("symbol", "")
        if sym and sym not in _seen_syms:
            _seen_syms.add(sym)
            candidates.append(ev)

    # Phase 2 — pre-score
    pre_scores: dict[str, dict] = {}
    for ev in candidates:
        sym = ev.get("symbol", "")
        if sym and sym not in pre_scores:
            pre_scores[sym] = _pre_score(ev, watchlist, portfolio)

    # Phase 3 — select top for enrichment
    sym_best: dict[str, int] = {}
    for ev in candidates:
        sym = ev.get("symbol", "")
        s   = pre_scores.get(sym, {}).get("score", 0)
        if sym not in sym_best or s > sym_best[sym]:
            sym_best[sym] = s

    enrich_syms: set[str] = set(
        sorted(sym_best, key=lambda s: sym_best[s], reverse=True)[:max_enrich]
    )
    enrich_pool  = [ev for ev in candidates if ev.get("symbol") in enrich_syms]
    not_enriched = [ev for ev in candidates if ev.get("symbol") not in enrich_syms]

    # Phase 4 — enrich (cache-first)
    if enrich_pool and not rate_limited:
        enrich_pool, enrich_rl = await _enrich_events(
            enrich_pool, api_key, client, call_counter,
            max_live=max_live, concurrency=concurrency,
        )
        if enrich_rl:
            rate_limited = True

    # Phase 5 — eligibility gate + full score + decorate
    _empty_pre = {
        "score": 0, "isAnchor": False, "isBottleneck": False,
        "themes": [],
        "breakdown": {
            "marketCap": 0, "theme": 0, "watchlistPortfolio": 0,
            "estimateAvailability": 0, "priceMomentum": 0, "qualityLiquidity": 0,
        },
    }
    decorated: list[dict] = []

    for ev in enrich_pool:
        sym = ev.get("symbol", "")
        pre = pre_scores.get(sym, _empty_pre)
        if not _is_eligible(ev, pre, watchlist, portfolio):
            continue
        score, breakdown = _apply_full_score(ev, pre)
        decorated.append(_decorate_event(ev, pre, score, breakdown))

    # Not-enriched: include explicit theme / watchlist / portfolio AND
    # high-signal unknowns: ≤4-char ticker + both estimates present + not preferred/junk.
    # These get a partial score (no mc/exchange boost) but prevent blank days for
    # non-theme weeks where all enrichment slots were consumed by other days.
    for ev in not_enriched:
        sym = ev.get("symbol", "")
        pre = pre_scores.get(sym, _empty_pre)
        is_theme_wl = (
            pre.get("isAnchor") or pre.get("isBottleneck") or pre.get("themes")
            or sym in watchlist or sym in portfolio
        )
        has_both_est  = (ev.get("epsEstimated") is not None
                         and ev.get("revenueEstimated") is not None)
        short_ticker  = len(sym) <= 4
        not_preferred = not _QUAL_PREF_RE.search(sym)
        cname_ne      = (ev.get("companyName") or "").strip()
        not_junk_name = not (
            _QUAL_NAME_PREF_RE.search(cname_ne) or _JUNK_NAME_RE.search(cname_ne)
        )
        high_signal_unknown = (
            has_both_est and short_ticker and not_preferred and not_junk_name
        )
        if not (is_theme_wl or high_signal_unknown):
            continue
        decorated.append(_decorate_event(ev, pre, pre["score"], pre["breakdown"]))

    return decorated, rate_limited


# ══════════════════════════════════════════════════════════════════════════════
# CANONICAL CURATED ENGINE — single shared source used by all curated views
# ══════════════════════════════════════════════════════════════════════════════

async def get_curated_earnings_range(
    api_key:   str,
    from_date: str,
    to_date:   str,
    scope:     Optional[str] = None,
    search:    Optional[str] = None,
) -> dict:
    """
    Core curated earnings engine — THE single source of truth for all curated views.

    Always uses week-clean parameters:
      max_candidates=_WEEK_CAND_MAX (400), max_enrich=_MAX_WEEK_ENRICH (300),
      max_live=_WEEK_MAX_LIVE (40),        concurrency=_WEEK_CONCURRENCY (2)

    All curated endpoints (week-clean, day-curated, month-curated) MUST call
    this function so they produce identical events for the same date.
    FMP calendar data and profile caches are shared — subsequent calls for the
    same range cost no additional FMP calls.

    Returns
    -------
    {
      "eventsByDate": dict[str, list[dict]],  # date → curated events (sorted by score, deduped)
      "allEvents":    list[dict],             # all curated events sorted by importanceScore
      "rawCounts":    dict[str, int],         # date → total FMP events (pre-filter, for calendar totals)
      "status":       "ok" | "partial",
      "errors":       list[str],
      "fmpCallsUsed": int,
      "rateLimited":  bool,
    }
    """
    # Snapshot-first: check in-memory → disk → proceed with live enrichment only if needed.
    # Scope/search bypass snapshots (filtered views must not pollute the shared snapshot).
    _rng_ck     = _snap_ck(from_date, to_date)     # "earnings:snap:week:{mon}:{fri}"
    _rng_lkg_ck = _snap_lkg_ck(from_date, to_date)
    if not scope and not search:
        # 1. In-memory snapshot (8-day TTL — survives multiple server restarts if warmed from disk)
        _rng_cached = cache.get(_rng_ck)
        if _rng_cached is not None:
            return _rng_cached
        # 2. Disk snapshot — pre-built by background loop; warm in-memory and serve immediately
        _disk_snap = _read_earn_snap_from_disk(_snap_disk_path(from_date, to_date))
        if _disk_snap is not None:
            cache.set(_rng_ck, _disk_snap, _TTL_SNAP)
            return _disk_snap

    errors:       list[str] = []

    # Phase 1: raw calendar fetch — shared Mon-Fri chunk cache
    raw_result   = await get_raw_earnings_chunks(
        from_date, to_date, api_key, max_weeks=_BUDGET_WEEK_CHUNKS,
    )
    raw_rows     = raw_result["rows"]
    rate_limited = raw_result["rateLimited"]
    stale_lkg    = raw_result["staleLKG"]
    cal_calls    = raw_result["fmpCallsUsed"]
    raw_hits     = raw_result["rawHits"]
    raw_misses   = raw_result["rawMisses"]
    # enrich_counter tracks only live profile/quote HTTP calls — separate from cal_calls
    enrich_counter: list[int] = [0]

    watchlist = _load_watchlist()
    portfolio = _load_portfolio()

    # Capture raw totals BEFORE scope/normalise filtering (for accurate calendar counts)
    raw_counts: dict[str, int] = {}
    for row in (raw_rows if isinstance(raw_rows, list) else []):
        sym = (row.get("symbol") or "").strip().upper()
        d   = (row.get("date")   or "").strip()
        if not sym or not d:
            continue
        name  = (row.get("name") or row.get("companyName") or sym)
        title = f"{name} Earnings" if name != sym else f"{sym} Earnings"
        if _is_polymarket_row(title, sym):
            continue
        raw_counts[d] = raw_counts.get(d, 0) + 1

    raw_events: list[dict] = []
    for row in (raw_rows if isinstance(raw_rows, list) else []):
        ev = _normalize_row(row)
        if ev:
            raw_events.append(ev)

    raw_events = _apply_scope(raw_events, search, scope, watchlist, portfolio)

    if not raw_events:
        return {
            "eventsByDate":   {},
            "allEvents":      [],
            "rawCounts":      raw_counts,
            "status":         "partial" if rate_limited else "ok",
            "errors":         errors + (["FMP rate limit hit — partial data"] if rate_limited else []),
            "fmpCallsUsed":   cal_calls,
            "calHttpCalls":   cal_calls,   # actual HTTP calls to /earnings-calendar
            "enrichHttpCalls": 0,          # actual HTTP calls to /profile (none — skipped)
            "rawEventsCount": len(raw_rows),
            "rateLimited":    rate_limited,
            "staleLKG":       stale_lkg,
            "rawHits":        raw_hits,
            "rawMisses":      raw_misses,
        }

    # Phase 2: enrichment pipeline — own client, enrich_counter tracks profile HTTP calls only
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(_FMP_TIMEOUT),
        follow_redirects=True,
    ) as enrich_client:
        decorated, rate_limited = await _run_curated_pipeline(
            raw_events, watchlist, portfolio, enrich_client, api_key, enrich_counter, rate_limited,
            max_candidates=_WEEK_CAND_MAX,
            max_enrich=_MAX_WEEK_ENRICH,
            max_live=_WEEK_MAX_LIVE,
            concurrency=_WEEK_CONCURRENCY,
        )
    enrich_calls = enrich_counter[0]
    if rate_limited:
        errors.append("FMP rate limit during enrichment — partial data")

    # Group by date, sort by importanceScore desc, dedup by symbol per date
    all_events_sorted = sorted(decorated, key=lambda e: -(e.get("importanceScore") or 0))
    events_by_date:  dict[str, list[dict]] = {}
    seen_per_date:   dict[str, set[str]]   = {}
    for ev in all_events_sorted:
        d   = ev.get("date", "")
        sym = ev.get("symbol", "")
        if not d or not sym:
            continue
        seen = seen_per_date.setdefault(d, set())
        if sym in seen:
            continue
        seen.add(sym)
        events_by_date.setdefault(d, []).append(ev)

    print(
        f"[earn_clean] curated-range {from_date}→{to_date} "
        f"raw={len(raw_rows)} eligible={len(decorated)} dates={len(events_by_date)} "
        f"cal_http={cal_calls} enrich_http={enrich_calls}"
    )

    result = {
        "eventsByDate":   events_by_date,
        "allEvents":      all_events_sorted,
        "rawCounts":      raw_counts,
        "status":         "partial" if rate_limited else "ok",
        "errors":         errors,
        "fmpCallsUsed":   cal_calls + enrich_calls,
        "calHttpCalls":   cal_calls,      # actual HTTP calls to /earnings-calendar
        "enrichHttpCalls": enrich_calls,  # actual HTTP calls to /profile
        "rawEventsCount": len(raw_rows),  # raw FMP rows before filter/score
        "rateLimited":    rate_limited,
        "staleLKG":       stale_lkg,
        "rawHits":        raw_hits,
        "rawMisses":      raw_misses,
        "cached_at":      time.time(),    # timestamp for disk snapshot validation
        "genuinelyEmpty": (len(raw_rows) > 0 and len(all_events_sorted) == 0),
    }

    if not scope and not search:
        if not rate_limited:
            # Write fresh snapshot to memory + disk (8-day TTL — survives restarts)
            cache.set(_rng_ck, result, _TTL_SNAP)
            _write_earn_snap_to_disk(_snap_disk_path(from_date, to_date), result)
        # Always update LKG when we received any events (even partially rate-limited)
        if all_events_sorted or result["genuinelyEmpty"]:
            cache.set(_rng_lkg_ck, result, _TTL_SNAP_LKG)
            _write_earn_snap_to_disk(_snap_lkg_disk_path(from_date, to_date), result)

    return result


# ── Shared day watchlist helpers ──────────────────────────────────────────────


def _day_watchlist_response(
    date:        str,
    events:      list[dict],
    view:        str,        # "clean" | "curated"
    meta:        dict,
    wl_us_count: int,
    message:     Optional[str] = None,
) -> dict:
    """Standardised response dict for day watchlist short-circuit."""
    result: dict = {
        "asOf":         _today(),
        "source":       "fmp",
        "date":         date,
        "events":       events,
        "count":        len(events),
        "status":       "ok",
        "errors":       [],
        "scope":        "watchlist",
        "symbolsCount": wl_us_count,
        "meta":         meta,
    }
    if view == "clean":
        result["fmpCallsUsed"] = 0
        result["rateLimited"]  = False
        result["staleLKG"]     = False
    if message:
        result["message"] = message
    return result


async def _get_day_watchlist_scope(
    api_key: str,
    date:    str,
    search:  Optional[str],
    view:    str,   # "clean" | "curated"
) -> dict:
    """
    Symbol-driven watchlist earnings for day-clean and day-curated endpoints.

    Bypasses FMP direct calendar calls and the curated scoring pipeline.
    Loads the user's saved Watchlist symbols from Neon and reads the same
    120-day user_earnings_service cache used by week-clean — then filters to
    the specific requested date.

    Never returns All-calendar or curated-pool symbols.
    If the cache is empty (FMP transient failure) returns an empty day response.
    Response shapes match what day-clean and day-curated already return so
    the frontend needs no changes.
    """
    from services.user_earnings_service import get_or_sync_user_earnings  # lazy

    wl_syms_raw      = _load_watchlist()
    wl_syms_us_count = sum(1 for s in wl_syms_raw if s and ":" not in s)

    print(
        f"[day_watchlist] view={view} date={date} "
        f"neon_total={len(wl_syms_raw)} us_tickers={wl_syms_us_count}"
    )

    # ── Empty watchlist ────────────────────────────────────────────────────────
    if not wl_syms_raw:
        return _day_watchlist_response(
            date, [], view,
            meta={"cache_status": "empty"}, wl_us_count=0,
            message="No watchlist symbols found",
        )

    # ── Fetch from 120-day Neon cache, filtered to requested date ─────────────
    try:
        events, meta = await get_or_sync_user_earnings(
            universe  = "watchlist",
            symbols   = wl_syms_raw,
            fmp_key   = api_key,
            from_date = date,
            to_date   = date,
        )
    except Exception as _err:
        print(f"[day_watchlist] user_earnings_service error: {_err}")
        events, meta = [], {
            "cache_status":  "error",
            "symbols_count": wl_syms_us_count,
            "events_count":  0,
        }

    # ── Optional text search ───────────────────────────────────────────────────
    if search:
        sl = search.strip().lower()
        events = [
            e for e in events
            if sl in (e.get("symbol") or "").lower()
            or sl in (e.get("companyName") or "").lower()
        ]

    # ── Stamp scope / defaults on every event ─────────────────────────────────
    for ev in events:
        ev["scope"] = "watchlist"
        ev.setdefault("source", "fmp")
        ev.setdefault("session", "unknown")
        ev.setdefault("importanceScore", 0)
        ev.setdefault("themeTags", [])

    cache_status = meta.get("cache_status", "unknown")
    print(
        f"[day_watchlist] returning {len(events)} events "
        f"cache_status={cache_status} "
        f"symbols_count={meta.get('symbols_count', wl_syms_us_count)}"
    )

    message = None
    if wl_syms_raw and not events:
        message = f"No watchlist earnings found on {date}"

    return _day_watchlist_response(
        date, events, view, meta=meta, wl_us_count=wl_syms_us_count,
        message=message,
    )


# ── Shared day portfolio helpers ──────────────────────────────────────────────


def _day_portfolio_response(
    date:        str,
    events:      list[dict],
    view:        str,        # "clean" | "curated"
    meta:        dict,
    port_count:  int,
    message:     Optional[str] = None,
) -> dict:
    """Standardised response dict for day portfolio short-circuit."""
    result: dict = {
        "asOf":         _today(),
        "source":       "fmp",
        "date":         date,
        "events":       events,
        "count":        len(events),
        "status":       "ok",
        "errors":       [],
        "scope":        "portfolio",
        "symbolsCount": port_count,
        "meta":         meta,
    }
    if view == "clean":
        result["fmpCallsUsed"] = 0
        result["rateLimited"]  = False
        result["staleLKG"]     = False
    if message:
        result["message"] = message
    return result


async def _get_day_portfolio_scope(
    api_key: str,
    date:    str,
    search:  Optional[str],
    view:    str,   # "clean" | "curated"
) -> dict:
    """
    Symbol-driven portfolio earnings for day-clean and day-curated endpoints.

    Bypasses FMP direct calendar calls and the curated scoring pipeline.
    Loads the user's saved Portfolio holdings from disk and reads the same
    120-day user_earnings_service cache — then filters to the specific date.

    Never returns All-calendar or curated-pool symbols.
    """
    from services.user_earnings_service import get_or_sync_user_earnings  # lazy

    port_syms_raw = _load_portfolio()
    port_count    = len(port_syms_raw)

    print(
        f"[day_portfolio] view={view} date={date} "
        f"portfolio_symbols={port_count}"
    )

    # ── Empty portfolio ────────────────────────────────────────────────────────
    if not port_syms_raw:
        return _day_portfolio_response(
            date, [], view,
            meta={"cache_status": "empty"}, port_count=0,
            message="No portfolio holdings found",
        )

    # ── Fetch from 120-day Neon cache, filtered to requested date ─────────────
    try:
        events, meta = await get_or_sync_user_earnings(
            universe  = "portfolio",
            symbols   = port_syms_raw,
            fmp_key   = api_key,
            from_date = date,
            to_date   = date,
        )
    except Exception as _err:
        print(f"[day_portfolio] user_earnings_service error: {_err}")
        events, meta = [], {
            "cache_status":  "error",
            "symbols_count": port_count,
            "events_count":  0,
        }

    # ── Optional text search ───────────────────────────────────────────────────
    if search:
        sl = search.strip().lower()
        events = [
            e for e in events
            if sl in (e.get("symbol") or "").lower()
            or sl in (e.get("companyName") or "").lower()
        ]

    # ── Stamp scope / defaults on every event ─────────────────────────────────
    for ev in events:
        ev["scope"] = "portfolio"
        ev.setdefault("source", "fmp")
        ev.setdefault("session", "unknown")
        ev.setdefault("importanceScore", 0)
        ev.setdefault("themeTags", [])

    # ── Enrich: FMP profile + Tradier quotes + last earnings ─────────────────
    if events:
        try:
            events, _qs, _les = await _hydrate_user_scope_events(
                events, api_key, universe="portfolio"
            )
            meta["quoteStatus"]    = _qs
            meta["lastEarnStatus"] = _les
        except Exception as _he:
            print(f"[day_portfolio] hydrate error: {_he}")

    cache_status = meta.get("cache_status", "unknown")
    print(
        f"[day_portfolio] returning {len(events)} events "
        f"cache_status={cache_status} symbols_count={port_count}"
    )

    message = None
    if port_syms_raw and not events:
        message = f"No portfolio earnings found on {date}"

    return _day_portfolio_response(
        date, events, view, meta=meta, port_count=port_count,
        message=message,
    )


# ── Watchlist-scoped week-clean (symbol-driven, not curated-pool filtered) ─────

async def _get_week_clean_watchlist_scope(
    api_key:           str,
    from_str:          str,
    to_str:            str,
    week_start,
    week_end,
    search:            Optional[str],
    limit_per_session: int,
    max_total:         int,
) -> dict:
    """
    Symbol-driven watchlist earnings for the week-clean endpoint.

    Bypasses the curated pipeline entirely.  Loads the user's saved Watchlist
    symbols from Neon (same source as the Watchlist page), then reads/syncs a
    120-day earnings window from user_earnings_service (Neon cache keyed by
    universe="watchlist") and filters to the requested week window.

    Never returns All-calendar symbols — only symbols that are actually saved in
    the user's Watchlist.
    """
    from services.user_earnings_service import get_or_sync_user_earnings  # lazy

    # Load watchlist symbols from Neon (same source as Watchlist page)
    wl_syms_raw = _load_watchlist()

    # US-ticker count (for display). We do NOT filter foreign tickers out of the
    # symbol set passed to user_earnings_service — doing so would create a
    # different cache key than the one used by /api/catalysts/events (which uses
    # all symbols including foreign ones). A mismatched key forces a re-sync on
    # every request. FMP naturally returns no results for foreign-exchange tickers
    # (AIM:IQE, ASX:EOS, etc.) so they're silently excluded from events anyway.
    wl_syms_us_count = sum(1 for s in wl_syms_raw if s and ":" not in s)

    print(
        f"[week_clean_watchlist] scope=watchlist "
        f"neon_total={len(wl_syms_raw)} us_tickers={wl_syms_us_count} "
        f"window={from_str}→{to_str}"
    )
    if wl_syms_raw:
        us_only = sorted(s for s in wl_syms_raw if ":" not in s)
        print(f"[week_clean_watchlist] first 20 US tickers: {us_only[:20]}")

    # ── Empty watchlist ────────────────────────────────────────────────────────
    if not wl_syms_raw:
        return {
            "asOf":         _today(),
            "source":       "fmp",
            "weekStart":    from_str,
            "weekEnd":      to_str,
            "days":         _build_empty_days(week_start, week_end),
            "topEvents":    [],
            "status":       "ok",
            "errors":       [],
            "scope":        "watchlist",
            "symbolsCount": 0,
            "eventsCount":  0,
            "message":      "No watchlist symbols found",
        }

    # ── Fetch from 120-day Neon cache, filter to requested week ────────────────
    # Pass ALL raw symbols (including foreign) to match the cache key used by
    # other code paths that call user_earnings_service with the full symbol set.
    try:
        events, meta = await get_or_sync_user_earnings(
            universe  = "watchlist",
            symbols   = wl_syms_raw,
            fmp_key   = api_key,
            from_date = from_str,
            to_date   = to_str,
        )
    except Exception as _err:
        print(f"[week_clean_watchlist] user_earnings_service error: {_err}")
        events, meta = [], {
            "cache_status": "error",
            "universe":     "watchlist",
            "symbols_count": wl_syms_us_count,
            "events_count":  0,
        }

    # ── Optional text search ───────────────────────────────────────────────────
    if search:
        sl = search.strip().lower()
        events = [
            e for e in events
            if sl in (e.get("symbol") or "").lower()
            or sl in (e.get("companyName") or "").lower()
        ]

    # ── Stamp each event with scope/source/session defaults ───────────────────
    for ev in events:
        ev["scope"] = "watchlist"
        ev.setdefault("source", "fmp")
        ev.setdefault("session", "unknown")
        ev.setdefault("importanceScore", 0)
        ev.setdefault("themeTags", [])

    print(
        f"[week_clean_watchlist] returning {len(events)} events "
        f"cache_status={meta.get('cache_status')} "
        f"symbols_count={meta.get('symbols_count', wl_syms_us_count)}"
    )

    # ── Build days[] (same structure as standard week-clean) ──────────────────
    days_map: dict[str, list[dict]] = {}
    cur = week_start
    while cur <= week_end:
        days_map[cur.strftime("%Y-%m-%d")] = []
        cur += timedelta(days=1)

    seen: set[tuple[str, str]] = set()
    for ev in events:
        sym = (ev.get("symbol") or "").upper()
        d   = (ev.get("date") or "")
        key = (sym, d)
        if key in seen:
            continue
        seen.add(key)
        if d in days_map:
            days_map[d].append(ev)

    days: list[dict] = []
    for date_str in sorted(days_map.keys()):
        day_evs   = days_map[date_str]
        pre_mkt   = [e for e in day_evs if e.get("session") == "preMarket"][:limit_per_session]
        after_hrs = [e for e in day_evs if e.get("session") == "afterHours"][:limit_per_session]
        during    = [e for e in day_evs if e.get("session") == "duringMarket"][:limit_per_session]
        unknown   = [
            e for e in day_evs
            if e.get("session") not in ("preMarket", "afterHours", "duringMarket")
        ][:limit_per_session]
        entries   = pre_mkt + during + after_hrs + unknown
        d_obj     = datetime.strptime(date_str, "%Y-%m-%d")
        label     = d_obj.strftime("%a, %b ") + str(d_obj.day)
        days.append({
            "date":         date_str,
            "label":        label,
            "weekday":      _WEEKDAY_NAMES.get(d_obj.weekday(), ""),
            "count":        len(day_evs),
            "preMarket":    pre_mkt,
            "afterHours":   after_hrs,
            "duringMarket": during,
            "unknown":      unknown,
            "entries":      entries,
        })

    all_deduped = [ev for evs in days_map.values() for ev in evs]
    top_events  = sorted(
        all_deduped, key=lambda e: -(e.get("importanceScore") or 0)
    )[:max_total]

    # Empty-but-valid: watchlist has symbols but none report this week
    message = None
    if wl_syms_raw and not events:
        message = "No upcoming earnings found for your watchlist this week"

    result = {
        "asOf":         _today(),
        "source":       "fmp",
        "weekStart":    from_str,
        "weekEnd":      to_str,
        "days":         days,
        "topEvents":    top_events,
        "status":       "ok",
        "errors":       [],
        "scope":        "watchlist",
        "symbolsCount": wl_syms_us_count,
        "eventsCount":  len(events),
        "meta":         meta,
    }
    if message:
        result["message"] = message
    return result


# ── Portfolio-scoped week-clean (symbol-driven, not curated-pool filtered) ─────


async def _get_week_clean_portfolio_scope(
    api_key:           str,
    from_str:          str,
    to_str:            str,
    week_start,
    week_end,
    search:            Optional[str],
    limit_per_session: int,
    max_total:         int,
) -> dict:
    """
    Symbol-driven portfolio earnings for the week-clean endpoint.

    Bypasses the curated pipeline entirely.  Loads the user's saved Portfolio
    holdings from disk, then reads/syncs a 120-day earnings window from
    user_earnings_service (Neon cache keyed by universe="portfolio") and
    filters to the requested week window.

    Never returns All-calendar symbols — only symbols actually in the portfolio.
    """
    from services.user_earnings_service import get_or_sync_user_earnings  # lazy

    port_syms_raw = _load_portfolio()
    port_count    = len(port_syms_raw)

    print(
        f"[week_clean_portfolio] scope=portfolio "
        f"portfolio_symbols={port_count} "
        f"window={from_str}→{to_str}"
    )
    if port_syms_raw:
        print(f"[week_clean_portfolio] first 20 tickers: {sorted(port_syms_raw)[:20]}")

    # ── Empty portfolio ────────────────────────────────────────────────────────
    if not port_syms_raw:
        return {
            "asOf":         _today(),
            "source":       "fmp",
            "weekStart":    from_str,
            "weekEnd":      to_str,
            "days":         _build_empty_days(week_start, week_end),
            "topEvents":    [],
            "status":       "ok",
            "errors":       [],
            "scope":        "portfolio",
            "symbolsCount": 0,
            "eventsCount":  0,
            "message":      "No portfolio holdings found",
        }

    # ── Fetch from 120-day Neon cache, filter to requested week ────────────────
    try:
        events, meta = await get_or_sync_user_earnings(
            universe  = "portfolio",
            symbols   = port_syms_raw,
            fmp_key   = api_key,
            from_date = from_str,
            to_date   = to_str,
        )
    except Exception as _err:
        print(f"[week_clean_portfolio] user_earnings_service error: {_err}")
        events, meta = [], {
            "cache_status": "error",
            "universe":     "portfolio",
            "symbols_count": port_count,
            "events_count":  0,
        }

    # ── Optional text search ───────────────────────────────────────────────────
    if search:
        sl = search.strip().lower()
        events = [
            e for e in events
            if sl in (e.get("symbol") or "").lower()
            or sl in (e.get("companyName") or "").lower()
        ]

    # ── Stamp each event with scope/source/session defaults ───────────────────
    for ev in events:
        ev["scope"] = "portfolio"
        ev.setdefault("source", "fmp")
        ev.setdefault("session", "unknown")
        ev.setdefault("importanceScore", 0)
        ev.setdefault("themeTags", [])

    # ── Enrich: FMP profile + Tradier quotes + last earnings ─────────────────
    if events:
        try:
            events, _qs, _les = await _hydrate_user_scope_events(
                events, api_key, universe="portfolio"
            )
            meta["quoteStatus"]    = _qs
            meta["lastEarnStatus"] = _les
        except Exception as _he:
            print(f"[week_clean_portfolio] hydrate error: {_he}")

    print(
        f"[week_clean_portfolio] returning {len(events)} events "
        f"cache_status={meta.get('cache_status')} "
        f"symbols_count={meta.get('symbols_count', port_count)}"
    )

    # ── Build days[] (same structure as standard week-clean) ──────────────────
    days_map: dict[str, list[dict]] = {}
    cur = week_start
    while cur <= week_end:
        days_map[cur.strftime("%Y-%m-%d")] = []
        cur += timedelta(days=1)

    seen: set[tuple[str, str]] = set()
    for ev in events:
        sym = (ev.get("symbol") or "").upper()
        d   = (ev.get("date") or "")
        key = (sym, d)
        if key in seen:
            continue
        seen.add(key)
        if d in days_map:
            days_map[d].append(ev)

    days: list[dict] = []
    for date_str in sorted(days_map.keys()):
        day_evs   = days_map[date_str]
        pre_mkt   = [e for e in day_evs if e.get("session") == "preMarket"][:limit_per_session]
        after_hrs = [e for e in day_evs if e.get("session") == "afterHours"][:limit_per_session]
        during    = [e for e in day_evs if e.get("session") == "duringMarket"][:limit_per_session]
        unknown   = [
            e for e in day_evs
            if e.get("session") not in ("preMarket", "afterHours", "duringMarket")
        ][:limit_per_session]
        entries   = pre_mkt + during + after_hrs + unknown
        d_obj     = datetime.strptime(date_str, "%Y-%m-%d")
        label     = d_obj.strftime("%a, %b ") + str(d_obj.day)
        days.append({
            "date":         date_str,
            "label":        label,
            "weekday":      _WEEKDAY_NAMES.get(d_obj.weekday(), ""),
            "count":        len(day_evs),
            "preMarket":    pre_mkt,
            "afterHours":   after_hrs,
            "duringMarket": during,
            "unknown":      unknown,
            "entries":      entries,
        })

    all_deduped = [ev for evs in days_map.values() for ev in evs]
    top_events  = sorted(
        all_deduped, key=lambda e: -(e.get("importanceScore") or 0)
    )[:max_total]

    message = None
    if port_syms_raw and not events:
        message = "No upcoming earnings found for your portfolio this week"

    result = {
        "asOf":         _today(),
        "source":       "fmp",
        "weekStart":    from_str,
        "weekEnd":      to_str,
        "days":         days,
        "topEvents":    top_events,
        "status":       "ok",
        "errors":       [],
        "scope":        "portfolio",
        "symbolsCount": port_count,
        "eventsCount":  len(events),
        "meta":         meta,
    }
    if message:
        result["message"] = message
    return result


# ── Shared month watchlist helpers ────────────────────────────────────────────


def _build_month_watchlist_days(
    year:        int,
    month:       int,
    events:      list[dict],
    view:        str,        # "all" → topSymbols, "curated" → topEvents
    max_per_day: int,
) -> list[dict]:
    """
    Build the days[] array for watchlist month responses.
    view="all"     → month-all shape:     { count, topSymbols[] }
    view="curated" → month-curated shape: { count, topEvents[] }
    """
    import calendar as _cal
    today_str = _today()
    this_ym   = today_str[:7]
    month_ym  = f"{year:04d}-{month:02d}"
    num_days  = _cal.monthrange(year, month)[1]

    events_by_date: dict[str, list[dict]] = {}
    for ev in events:
        d = ev.get("date", "")
        if d:
            events_by_date.setdefault(d, []).append(ev)

    days: list[dict] = []
    for day_num in range(1, num_days + 1):
        d_str   = f"{year:04d}-{month:02d}-{day_num:02d}"
        day_evs = events_by_date.get(d_str, [])
        count   = len(day_evs)
        if view == "all":
            sorted_evs = sorted(
                day_evs,
                key=lambda e: -(e.get("importanceScore") or e.get("revenueEstimated") or 0),
            )
            top_syms = [e["symbol"] for e in sorted_evs[:max_per_day] if e.get("symbol")]
            days.append({
                "date":           d_str,
                "dayOfMonth":     day_num,
                "isCurrentMonth": month_ym == this_ym,
                "count":          count,
                "topSymbols":     top_syms,
            })
        else:
            sorted_evs = sorted(
                day_evs,
                key=lambda e: -(e.get("importanceScore") or 0),
            )
            days.append({
                "date":           d_str,
                "dayOfMonth":     day_num,
                "isCurrentMonth": month_ym == this_ym,
                "count":          count,
                "topEvents":      sorted_evs[:max_per_day],
            })
    return days


def _month_watchlist_response(
    year:        int,
    month:       int,
    month_label: str,
    month_start: str,
    month_end:   str,
    days:        list[dict],
    view:        str,
    meta:        dict,
    wl_us_count: int,
    message:     Optional[str] = None,
) -> dict:
    """Standardised response dict for month watchlist short-circuit."""
    total_events = sum(d.get("count", 0) for d in days)
    result: dict = {
        "asOf":         _today(),
        "source":       "fmp",
        "year":         year,
        "month":        month,
        "monthLabel":   month_label,
        "monthStart":   month_start,
        "monthEnd":     month_end,
        "days":         days,
        "status":       "ok",
        "errors":       [],
        "scope":        "watchlist",
        "symbolsCount": wl_us_count,
        "eventsCount":  total_events,
        "meta":         meta,
    }
    if view == "all":
        result["fmpCallsUsed"] = 0
        result["rateLimited"]  = False
        result["staleLKG"]     = False
    if message:
        result["message"] = message
    return result


async def _get_month_watchlist_scope(
    api_key:     str,
    year:        int,
    month:       int,
    search:      Optional[str],
    view:        str,        # "all" | "curated"
    max_per_day: int = 5,
) -> dict:
    """
    Symbol-driven watchlist earnings for month-all and month-curated endpoints.

    Bypasses the curated pipeline entirely.  Loads the user's saved Watchlist
    symbols from Neon (same source as the Watchlist page), reads/syncs the
    120-day user_earnings_service cache, then filters to the requested month
    window and builds the month-shaped response.

    Never returns All-calendar symbols — only symbols actually in the Watchlist.
    If the forward-window cache is empty (FMP transient failure), returns an
    empty month response rather than falling back to All curated data.
    """
    import calendar as _cal
    from services.user_earnings_service import get_or_sync_user_earnings  # lazy

    last_day_num  = _cal.monthrange(year, month)[1]
    month_start_d = datetime(year, month, 1).date()
    month_end_d   = datetime(year, month, last_day_num).date()
    month_label   = datetime(year, month, 1).strftime("%B %Y")
    month_start   = month_start_d.strftime("%Y-%m-%d")
    month_end     = month_end_d.strftime("%Y-%m-%d")

    # Load watchlist symbols from Neon (same source as Watchlist page and
    # _get_week_clean_watchlist_scope — must be the SAME set so the Neon cache
    # key is stable across all watchlist earnings code paths).
    wl_syms_raw      = _load_watchlist()
    wl_syms_us_count = sum(1 for s in wl_syms_raw if s and ":" not in s)

    print(
        f"[month_watchlist] view={view} {year}-{month:02d} "
        f"neon_total={len(wl_syms_raw)} us_tickers={wl_syms_us_count} "
        f"window={month_start}→{month_end}"
    )
    if wl_syms_raw:
        us_only = sorted(s for s in wl_syms_raw if ":" not in s)
        print(f"[month_watchlist] first 20 US tickers: {us_only[:20]}")

    # ── Empty watchlist ────────────────────────────────────────────────────────
    if not wl_syms_raw:
        days = _build_month_watchlist_days(year, month, [], view, max_per_day)
        return _month_watchlist_response(
            year, month, month_label, month_start, month_end, days,
            view, meta={"cache_status": "empty"}, wl_us_count=0,
            message="No watchlist symbols found",
        )

    # ── Fetch from 120-day Neon cache, filter to requested month ───────────────
    try:
        events, meta = await get_or_sync_user_earnings(
            universe  = "watchlist",
            symbols   = wl_syms_raw,
            fmp_key   = api_key,
            from_date = month_start,
            to_date   = month_end,
        )
    except Exception as _err:
        print(f"[month_watchlist] user_earnings_service error: {_err}")
        events, meta = [], {
            "cache_status":  "error",
            "symbols_count": wl_syms_us_count,
            "events_count":  0,
        }

    # ── Optional text search ───────────────────────────────────────────────────
    if search:
        sl = search.strip().lower()
        events = [
            e for e in events
            if sl in (e.get("symbol") or "").lower()
            or sl in (e.get("companyName") or "").lower()
        ]

    # ── Stamp scope / defaults on every event ─────────────────────────────────
    for ev in events:
        ev["scope"] = "watchlist"
        ev.setdefault("source", "fmp")
        ev.setdefault("session", "unknown")
        ev.setdefault("importanceScore", 0)
        ev.setdefault("themeTags", [])

    cache_status = meta.get("cache_status", "unknown")
    print(
        f"[month_watchlist] returning {len(events)} events "
        f"cache_status={cache_status} "
        f"symbols_count={meta.get('symbols_count', wl_syms_us_count)}"
    )

    days    = _build_month_watchlist_days(year, month, events, view, max_per_day)
    message = None
    if wl_syms_raw and not events:
        message = f"No upcoming earnings found for your watchlist in {month_label}"

    return _month_watchlist_response(
        year, month, month_label, month_start, month_end, days,
        view, meta=meta, wl_us_count=wl_syms_us_count, message=message,
    )


# ── Shared month portfolio helpers ────────────────────────────────────────────


def _month_portfolio_response(
    year:        int,
    month:       int,
    month_label: str,
    month_start: str,
    month_end:   str,
    days:        list[dict],
    view:        str,
    meta:        dict,
    port_count:  int,
    message:     Optional[str] = None,
) -> dict:
    """Standardised response dict for month portfolio short-circuit."""
    total_events = sum(d.get("count", 0) for d in days)
    result: dict = {
        "asOf":         _today(),
        "source":       "fmp",
        "year":         year,
        "month":        month,
        "monthLabel":   month_label,
        "monthStart":   month_start,
        "monthEnd":     month_end,
        "days":         days,
        "status":       "ok",
        "errors":       [],
        "scope":        "portfolio",
        "symbolsCount": port_count,
        "eventsCount":  total_events,
        "meta":         meta,
    }
    if view == "all":
        result["fmpCallsUsed"] = 0
        result["rateLimited"]  = False
        result["staleLKG"]     = False
    if message:
        result["message"] = message
    return result


async def _get_month_portfolio_scope(
    api_key:     str,
    year:        int,
    month:       int,
    search:      Optional[str],
    view:        str,        # "all" | "curated"
    max_per_day: int = 5,
) -> dict:
    """
    Symbol-driven portfolio earnings for month-all and month-curated endpoints.

    Bypasses the curated pipeline entirely.  Loads the user's saved Portfolio
    holdings from disk, reads/syncs the 120-day user_earnings_service cache,
    then filters to the requested month window and builds the month-shaped response.

    Never returns All-calendar symbols — only symbols actually in the portfolio.
    If the forward-window cache is empty (FMP transient failure), returns an
    empty month response rather than falling back to All curated data.
    """
    import calendar as _cal
    from services.user_earnings_service import get_or_sync_user_earnings  # lazy

    last_day_num  = _cal.monthrange(year, month)[1]
    month_start_d = datetime(year, month, 1).date()
    month_end_d   = datetime(year, month, last_day_num).date()
    month_label   = datetime(year, month, 1).strftime("%B %Y")
    month_start   = month_start_d.strftime("%Y-%m-%d")
    month_end     = month_end_d.strftime("%Y-%m-%d")

    port_syms_raw = _load_portfolio()
    port_count    = len(port_syms_raw)

    print(
        f"[month_portfolio] view={view} {year}-{month:02d} "
        f"portfolio_symbols={port_count} "
        f"window={month_start}→{month_end}"
    )
    if port_syms_raw:
        print(f"[month_portfolio] first 20 tickers: {sorted(port_syms_raw)[:20]}")

    # ── Empty portfolio ────────────────────────────────────────────────────────
    if not port_syms_raw:
        days = _build_month_watchlist_days(year, month, [], view, max_per_day)
        return _month_portfolio_response(
            year, month, month_label, month_start, month_end, days,
            view, meta={"cache_status": "empty"}, port_count=0,
            message="No portfolio holdings found",
        )

    # ── Fetch from 120-day Neon cache, filter to requested month ───────────────
    try:
        events, meta = await get_or_sync_user_earnings(
            universe  = "portfolio",
            symbols   = port_syms_raw,
            fmp_key   = api_key,
            from_date = month_start,
            to_date   = month_end,
        )
    except Exception as _err:
        print(f"[month_portfolio] user_earnings_service error: {_err}")
        events, meta = [], {
            "cache_status":  "error",
            "symbols_count": port_count,
            "events_count":  0,
        }

    # ── Optional text search ───────────────────────────────────────────────────
    if search:
        sl = search.strip().lower()
        events = [
            e for e in events
            if sl in (e.get("symbol") or "").lower()
            or sl in (e.get("companyName") or "").lower()
        ]

    # ── Stamp scope / defaults on every event ─────────────────────────────────
    for ev in events:
        ev["scope"] = "portfolio"
        ev.setdefault("source", "fmp")
        ev.setdefault("session", "unknown")
        ev.setdefault("importanceScore", 0)
        ev.setdefault("themeTags", [])

    # ── Enrich: FMP profile + Tradier quotes + last earnings ─────────────────
    if events:
        try:
            events, _qs, _les = await _hydrate_user_scope_events(
                events, api_key, universe="portfolio"
            )
            meta["quoteStatus"]    = _qs
            meta["lastEarnStatus"] = _les
        except Exception as _he:
            print(f"[month_portfolio] hydrate error: {_he}")

    cache_status = meta.get("cache_status", "unknown")
    print(
        f"[month_portfolio] returning {len(events)} events "
        f"cache_status={cache_status} symbols_count={port_count}"
    )

    days    = _build_month_watchlist_days(year, month, events, view, max_per_day)
    message = None
    if port_syms_raw and not events:
        message = f"No upcoming earnings found for your portfolio in {month_label}"

    return _month_portfolio_response(
        year, month, month_label, month_start, month_end, days,
        view, meta=meta, port_count=port_count, message=message,
    )


# ── Public API: get_week_clean ─────────────────────────────────────────────────

async def get_week_clean(
    api_key:           str,
    week_start:        Optional[str] = None,
    week_end:          Optional[str] = None,
    scope:             Optional[str] = None,
    search:            Optional[str] = None,
    limit_per_session: int = _WEEK_LIMIT_DEFAULT,
    max_total:         int = _WEEK_TOTAL_DEFAULT,
) -> dict:
    """
    FMP earnings calendar for a single business week — curated and scored.

    Safety rules:
      • One week only (Mon–Fri by default) — hard clamped to 7 days max.
      • Max 1-2 FMP calendar calls (one 7-day chunk; cache hit after first call).
      • Two-phase enrichment: pre-score all → enrich top 40 candidates only.
      • Enrichment: concurrency=2, circuit breaker, partial on 429.
      • No AI, no Finnhub, no Polymarket.

    Response: { asOf, source, weekStart, weekEnd, days[], topEvents[], status, errors }
    """
    limit_per_session = min(max(1, limit_per_session), _WEEK_LIMIT_MAX)
    max_total         = min(max(1, max_total), _WEEK_TOTAL_MAX)

    # -- Resolve week bounds
    today_d = datetime.now(timezone.utc).date()
    if week_start:
        try:
            ws = datetime.strptime(week_start, "%Y-%m-%d").date()
        except ValueError:
            ws = today_d - timedelta(days=today_d.weekday())
    else:
        ws = today_d - timedelta(days=today_d.weekday())   # Monday

    if week_end:
        try:
            we = datetime.strptime(week_end, "%Y-%m-%d").date()
        except ValueError:
            we = ws + timedelta(days=4)
    else:
        we = ws + timedelta(days=4)   # Friday

    # Hard clamp: never more than 7 days
    if we > ws + timedelta(days=6):
        we = ws + timedelta(days=6)

    from_str = ws.strftime("%Y-%m-%d")
    to_str   = we.strftime("%Y-%m-%d")

    # ── Watchlist short-circuit ────────────────────────────────────────────────
    # When scope=watchlist we bypass the curated All pipeline entirely and use
    # the symbol-driven user_earnings_service path so the frontend sees events
    # only for the user's actual saved Watchlist tickers — not the curated top
    # earnings subset that happens to overlap with the watchlist.
    if scope == "watchlist":
        return await _get_week_clean_watchlist_scope(
            api_key           = api_key,
            from_str          = from_str,
            to_str            = to_str,
            week_start        = ws,
            week_end          = we,
            search            = search,
            limit_per_session = limit_per_session,
            max_total         = max_total,
        )
    # ── End watchlist short-circuit ───────────────────────────────────────────

    # ── Portfolio short-circuit ────────────────────────────────────────────────
    if scope == "portfolio":
        return await _get_week_clean_portfolio_scope(
            api_key           = api_key,
            from_str          = from_str,
            to_str            = to_str,
            week_start        = ws,
            week_end          = we,
            search            = search,
            limit_per_session = limit_per_session,
            max_total         = max_total,
        )
    # ── End portfolio short-circuit ────────────────────────────────────────────

    # -- Result cache (skip for filtered views)
    _wk_ck = f"earnings:curated:week:{from_str}:{to_str}"
    if not scope and not search:
        _wk_cached = cache.get(_wk_ck)
        if _wk_cached is not None:
            _log_telemetry("week-curated", f"{from_str}→{to_str}", cache_hit=True)
            return _wk_cached

    # -- Snapshot-first: serve pre-built weekly snapshot without live enrichment.
    # Scope/search filtered views bypass snapshots (filtered results must not
    # replace the shared unfiltered snapshot).
    if not scope and not search:
        _snap = _get_snap_or_lkg_fast(from_str, to_str)
        if _snap is not None:
            if _snap.get("staleLKG") and api_key:
                asyncio.create_task(_background_build_week(api_key, from_str, to_str))
            rng = _snap
        else:
            # No snapshot — run live once; get_curated_earnings_range will write disk on success
            rng = await get_curated_earnings_range(api_key, from_str, to_str)
    else:
        rng = await get_curated_earnings_range(api_key, from_str, to_str, scope, search)

    all_events = rng["allEvents"]
    errors     = list(rng["errors"])

    if not all_events:
        return {
            "asOf":      _today(),
            "source":    "fmp",
            "weekStart": from_str,
            "weekEnd":   to_str,
            "days":      _build_empty_days(ws, we),
            "topEvents": [],
            "status":    rng["status"],
            "errors":    errors,
        }

    # -- Group by date and session, dedup (symbol+date+session), apply per-session cap
    days_map: dict[str, list[dict]] = {}
    cur = ws
    while cur <= we:
        days_map[cur.strftime("%Y-%m-%d")] = []
        cur += timedelta(days=1)

    seen_key: set[tuple[str, str, str]] = set()
    for ev in all_events:          # already sorted by importanceScore desc
        sym     = ev.get("symbol", "")
        d       = ev.get("date", "")
        sess    = ev.get("session", "unknown")
        dedup_k = (sym, d, sess)
        if dedup_k in seen_key:
            continue
        seen_key.add(dedup_k)
        if d in days_map:
            days_map[d].append(ev)

    days: list[dict] = []
    for date_str in sorted(days_map.keys()):
        day_evs = sorted(days_map[date_str], key=lambda e: -(e.get("importanceScore") or 0))

        pre_mkt   = [e for e in day_evs if e.get("session") == "preMarket"][:limit_per_session]
        after_hrs = [e for e in day_evs if e.get("session") == "afterHours"][:limit_per_session]
        during    = [e for e in day_evs if e.get("session") == "duringMarket"][:limit_per_session]
        unknown   = [e for e in day_evs if e.get("session") == "unknown"][:limit_per_session]
        entries   = pre_mkt + during + after_hrs + unknown

        d_obj = datetime.strptime(date_str, "%Y-%m-%d")
        label = d_obj.strftime("%a, %b ") + str(d_obj.day)

        days.append({
            "date":         date_str,
            "label":        label,
            "weekday":      _WEEKDAY_NAMES.get(d_obj.weekday(), ""),
            "count":        len(day_evs),
            "preMarket":    pre_mkt,
            "afterHours":   after_hrs,
            "duringMarket": during,
            "unknown":      unknown,
            "entries":      entries,
        })

    all_deduped = [ev for evs in days_map.values() for ev in evs]
    top_events  = sorted(all_deduped, key=lambda e: -(e.get("importanceScore") or 0))[:max_total]

    _wk_result = {
        "asOf":      _today(),
        "source":    "fmp",
        "weekStart": from_str,
        "weekEnd":   to_str,
        "days":      days,
        "topEvents": top_events,
        "status":    rng["status"],
        "errors":    errors,
    }

    if not rng["rateLimited"] and not scope and not search:
        cache.set(_wk_ck, _wk_result, _TTL_RESULT)

    _log_telemetry(
        "week-curated", f"{from_str}→{to_str}",
        raw_hits=rng.get("rawHits", 0),
        raw_misses=rng.get("rawMisses", 0),
        fmp_calendar_http_calls=rng.get("calHttpCalls", 0),
        raw_events_count=rng.get("rawEventsCount", 0),
        live_profile_calls=rng.get("enrichHttpCalls", 0),
        stale_lkg=rng.get("staleLKG", False),
        rate_limited=rng["rateLimited"],
    )
    return _wk_result


# ══════════════════════════════════════════════════════════════════════════════
# WEEK-ALL — Full FMP earnings list for a week (no scoring / filtering)
# ══════════════════════════════════════════════════════════════════════════════

async def get_week_all(
    api_key:    str,
    week_start: Optional[str] = None,
    week_end:   Optional[str] = None,
    scope:      Optional[str] = None,
    search:     Optional[str] = None,
    limit:      int = 5000,
) -> dict:
    """
    Full FMP earnings list for a business week — no scoring, no filtering.

    Mirrors the structure of upcoming-clean but scoped to one week.
    Identical safety constraints: sequential chunks, 429 circuit breaker,
    no enrichment, logos via CDN URL.

    Response: { asOf, source, weekStart, weekEnd, events[], eventsByDate,
                countsByDate, fmpCallsUsed, rateLimited, status, errors }
    """
    today_d = datetime.now(timezone.utc).date()
    if week_start:
        try:
            ws = datetime.strptime(week_start, "%Y-%m-%d").date()
        except ValueError:
            ws = today_d - timedelta(days=today_d.weekday())
    else:
        ws = today_d - timedelta(days=today_d.weekday())

    if week_end:
        try:
            we = datetime.strptime(week_end, "%Y-%m-%d").date()
        except ValueError:
            we = ws + timedelta(days=4)
    else:
        we = ws + timedelta(days=4)

    # Hard clamp: never more than 7 days
    if we > ws + timedelta(days=6):
        we = ws + timedelta(days=6)

    from_str = ws.strftime("%Y-%m-%d")
    to_str   = we.strftime("%Y-%m-%d")

    # -- Result cache (skip for filtered views)
    _wa_ck = f"earnings:all:week:{from_str}:{to_str}"
    if not scope and not search:
        _wa_cached = cache.get(_wa_ck)
        if _wa_cached is not None:
            _log_telemetry("week-all", f"{from_str}→{to_str}", cache_hit=True)
            return _wa_cached

    # Raw calendar fetch — shared Mon-Fri chunk cache
    raw_result   = await get_raw_earnings_chunks(
        from_str, to_str, api_key, max_weeks=_BUDGET_WEEK_CHUNKS,
    )
    raw_rows     = raw_result["rows"]
    rate_limited = raw_result["rateLimited"]
    stale_lkg    = raw_result["staleLKG"]
    cal_calls    = raw_result["fmpCallsUsed"]
    raw_hits     = raw_result["rawHits"]
    raw_misses   = raw_result["rawMisses"]

    events: list[dict] = []
    for row in raw_rows:
        ev = _normalize_row(row)
        if ev:
            events.append(ev)

    watchlist = _load_watchlist()
    portfolio = _load_portfolio()
    events = _apply_scope(events, search, scope, watchlist, portfolio)
    events.sort(key=lambda e: (e.get("date", ""), e.get("symbol", "")))

    events_by_date: dict[str, list] = {}
    counts_by_date: dict[str, int]  = {}
    for ev in events:
        d = ev.get("date", "")
        if d:
            events_by_date.setdefault(d, []).append(ev)
            counts_by_date[d] = counts_by_date.get(d, 0) + 1

    if limit and len(events) > limit:
        events = events[:limit]

    _wa_result = {
        "asOf":          _today(),
        "source":        "fmp",
        "weekStart":     from_str,
        "weekEnd":       to_str,
        "events":        events,
        "eventsByDate":  events_by_date,
        "countsByDate":  counts_by_date,
        "status":        "partial" if rate_limited else "ok",
        "fmpCallsUsed":  cal_calls,
        "rateLimited":   rate_limited,
        "staleLKG":      stale_lkg,
        "errors":        (["FMP rate limit hit — partial data"] if rate_limited else []),
    }

    if not rate_limited and not scope and not search:
        cache.set(_wa_ck, _wa_result, _TTL_RESULT)

    _log_telemetry(
        "week-all", f"{from_str}→{to_str}",
        raw_hits=raw_hits, raw_misses=raw_misses,
        fmp_calendar_http_calls=cal_calls,
        raw_events_count=len(raw_rows),
        live_profile_calls=0,
        stale_lkg=stale_lkg, rate_limited=rate_limited,
    )
    return _wa_result


# ══════════════════════════════════════════════════════════════════════════════
# MONTH-ALL — Lightweight FMP monthly calendar (zero profile enrichment)
# ══════════════════════════════════════════════════════════════════════════════

def _build_month_days_all(
    year:           int,
    month:          int,
    counts_by_date: dict[str, int],
    events_by_date: dict[str, list[dict]],
    top_n:          int,
) -> list[dict]:
    """
    Build month calendar for month-all.
    count     = total FMP events for the date (pre-filter).
    topSymbols = up to top_n symbol strings, sorted by revenueEstimated desc.
    No live profile calls — raw calendar data only.
    """
    import calendar as _cal
    today_str = _today()
    this_ym   = today_str[:7]
    month_ym  = f"{year:04d}-{month:02d}"
    num_days  = _cal.monthrange(year, month)[1]
    days: list[dict] = []
    for day_num in range(1, num_days + 1):
        d_str   = f"{year:04d}-{month:02d}-{day_num:02d}"
        day_evs = events_by_date.get(d_str, [])
        sorted_evs = sorted(day_evs, key=lambda e: -(e.get("revenueEstimated") or 0))
        top_syms   = [e["symbol"] for e in sorted_evs[:top_n] if e.get("symbol")]
        days.append({
            "date":           d_str,
            "dayOfMonth":     day_num,
            "isCurrentMonth": month_ym == this_ym,
            "count":          counts_by_date.get(d_str, 0),
            "topSymbols":     top_syms,
        })
    return days


async def get_month_all(
    api_key:     str,
    year:        int,
    month:       int,
    scope:       Optional[str] = None,
    search:      Optional[str] = None,
    top_n:       int = 5,
) -> dict:
    """
    Lightweight FMP earnings calendar for a full month — ZERO profile enrichment.

    Architecture:
      • Fetches via sequential 7-day chunks (max _BUDGET_MONTH_CHUNKS=5 FMP calls).
      • No profile/quote/marketCap calls — counts and raw calendar fields only.
      • Returns count-per-day + top N symbols sorted by revenueEstimated.
      • 429 circuit breaker (earnings-service only — does NOT affect Home/Sectors/Macro).
      • Cache key: earnings:all:month:{year}:{month}, TTL 6 h.

    FMP calls: max 5 calendar chunks.
    Live profile calls: 0 (month-all never calls profile or quote endpoints).

    Response: { asOf, source, year, month, monthLabel, monthStart, monthEnd,
                days[{date, dayOfMonth, isCurrentMonth, count, topSymbols[]}],
                fmpCallsUsed, rateLimited, status, errors }
    """
    import calendar as _cal

    top_n = min(max(1, top_n), 20)

    # ── Watchlist short-circuit ────────────────────────────────────────────────
    # For scope=watchlist we bypass the curated All pipeline entirely and use the
    # symbol-driven user_earnings_service path so the calendar shows only events
    # for the user's actual saved Watchlist tickers — not the curated pool.
    if scope == "watchlist":
        return await _get_month_watchlist_scope(
            api_key     = api_key,
            year        = year,
            month       = month,
            search      = search,
            view        = "all",
            max_per_day = top_n,
        )
    # ── End watchlist short-circuit ────────────────────────────────────────────

    # ── Portfolio short-circuit ────────────────────────────────────────────────
    if scope == "portfolio":
        return await _get_month_portfolio_scope(
            api_key     = api_key,
            year        = year,
            month       = month,
            search      = search,
            view        = "all",
            max_per_day = top_n,
        )
    # ── End portfolio short-circuit ────────────────────────────────────────────

    last_day_num  = _cal.monthrange(year, month)[1]
    month_start_d = datetime(year, month, 1).date()
    month_end_d   = datetime(year, month, last_day_num).date()
    month_label   = datetime(year, month, 1).strftime("%B %Y")
    month_start   = month_start_d.strftime("%Y-%m-%d")
    month_end     = month_end_d.strftime("%Y-%m-%d")

    # -- Result cache (skip for filtered views)
    _ma_ck = f"earnings:all:month:{year}:{month}"
    if not scope and not search:
        _ma_cached = cache.get(_ma_ck)
        if _ma_cached is not None:
            _log_telemetry("month-all", f"{month_start}→{month_end}", cache_hit=True)
            return _ma_cached

    # Raw calendar fetch — shared Mon-Fri chunk cache, zero profile enrichment
    raw_result   = await get_raw_earnings_chunks(
        month_start, month_end, api_key, max_weeks=_BUDGET_MONTH_CHUNKS,
    )
    raw_rows     = raw_result["rows"]
    rate_limited = raw_result["rateLimited"]
    stale_lkg    = raw_result["staleLKG"]
    cal_calls    = raw_result["fmpCallsUsed"]
    raw_hits     = raw_result["rawHits"]
    raw_misses   = raw_result["rawMisses"]
    # ← NO profile enrichment, NO quote calls — deliberately ends here

    watchlist = _load_watchlist()
    portfolio = _load_portfolio()

    raw_events: list[dict] = []
    for row in (raw_rows if isinstance(raw_rows, list) else []):
        ev = _normalize_row(row)
        if ev:
            raw_events.append(ev)

    raw_events = _apply_scope(raw_events, search, scope, watchlist, portfolio)

    counts_by_date: dict[str, int]        = {}
    events_by_date: dict[str, list[dict]] = {}
    for ev in raw_events:
        d = ev.get("date", "")
        if not d:
            continue
        counts_by_date[d] = counts_by_date.get(d, 0) + 1
        events_by_date.setdefault(d, []).append(ev)

    days = _build_month_days_all(year, month, counts_by_date, events_by_date, top_n)

    _ma_result = {
        "asOf":         _today(),
        "source":       "fmp",
        "year":         year,
        "month":        month,
        "monthLabel":   month_label,
        "monthStart":   month_start,
        "monthEnd":     month_end,
        "days":         days,
        "fmpCallsUsed": cal_calls,
        "rateLimited":  rate_limited,
        "staleLKG":     stale_lkg,
        "status":       "partial" if rate_limited else "ok",
        "errors":       (["FMP rate limit hit — partial data"] if rate_limited else []),
    }

    if not rate_limited and not scope and not search:
        cache.set(_ma_ck, _ma_result, _TTL_RESULT)

    _log_telemetry(
        "month-all", f"{month_start}→{month_end}",
        raw_hits=raw_hits, raw_misses=raw_misses,
        fmp_calendar_http_calls=cal_calls,
        raw_events_count=len(raw_rows),
        live_profile_calls=_MONTH_ALL_LIVE_PROFILES,   # always 0 — intentional
        stale_lkg=stale_lkg, rate_limited=rate_limited,
    )
    return _ma_result


# ══════════════════════════════════════════════════════════════════════════════
# DAY-CURATED — Curated earnings for a single date (same engine as week-clean)
# ══════════════════════════════════════════════════════════════════════════════

async def get_day_curated(
    api_key: str,
    date:    str,
    scope:   Optional[str] = None,
    search:  Optional[str] = None,
    limit:   int = _DAY_LIMIT_DEFAULT,
) -> dict:
    """
    Curated FMP earnings for a single date — same scoring / eligibility engine
    as week-clean.  Returns only high-signal events; never pads with junk.
    If only 5 qualifying events exist on that day, only 5 are returned.

    Safety rules:
      • 1 FMP calendar call (same chunk cache shared with day-clean).
      • Enrich up to 80 raw candidates, max 20 live profile calls, concurrency=2.
      • 429 circuit breaker → partial data.
      • No AI, no Finnhub, no Polymarket.

    Response: { asOf, source, date, events[], count, status, errors }

    Event shape matches week-clean: session, priceChangePct, marketCapBucket,
    themeTags, isThemeAnchor, isBottleneck, importanceScore, scoreBreakdown.
    """
    limit = min(max(1, limit), _DAY_LIMIT_MAX)

    # ── Watchlist short-circuit ────────────────────────────────────────────────
    # For scope=watchlist we bypass the curated scoring pipeline entirely and use
    # the symbol-driven user_earnings_service path so the day view shows only
    # events for the user's actual saved Watchlist tickers — not the curated pool.
    # The curated pipeline's importance-score filter would silently discard most
    # watchlist events that don't clear the signal threshold.
    if scope == "watchlist":
        return await _get_day_watchlist_scope(api_key, date, search, "curated")
    # ── End watchlist short-circuit ────────────────────────────────────────────

    # ── Portfolio short-circuit ────────────────────────────────────────────────
    if scope == "portfolio":
        return await _get_day_portfolio_scope(api_key, date, search, "curated")
    # ── End portfolio short-circuit ────────────────────────────────────────────

    # Determine the Mon–Fri week that contains this date.
    # Fetching the full week (same range as week-clean) ensures we reuse the
    # cached FMP calendar data and produce identical events.
    try:
        target_d = datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError:
        target_d = datetime.now(timezone.utc).date()

    mon = target_d - timedelta(days=target_d.weekday())   # Monday of that week
    fri = mon + timedelta(days=4)                          # Friday of that week
    from_str = mon.strftime("%Y-%m-%d")
    to_str   = fri.strftime("%Y-%m-%d")

    # Snapshot-first: read pre-built weekly snapshot, then filter to the requested day.
    # Avoids running live enrichment on every day-view click.
    # Scope/search filtered views bypass snapshots.
    if not scope and not search:
        _snap = _get_snap_or_lkg_fast(from_str, to_str)
        if _snap is not None:
            if _snap.get("staleLKG") and api_key:
                asyncio.create_task(_background_build_week(api_key, from_str, to_str))
            rng = _snap
        else:
            rng = await get_curated_earnings_range(api_key, from_str, to_str)
    else:
        rng = await get_curated_earnings_range(api_key, from_str, to_str, scope, search)
    events = rng["eventsByDate"].get(date, [])[:limit]

    print(
        f"[earn_clean] day-curated {date} (week {from_str}→{to_str}) "
        f"day_events={len(events)} total_eligible={len(rng['allEvents'])} "
        f"calls={rng['fmpCallsUsed']}"
    )

    return {
        "asOf":    _today(),
        "source":  "fmp",
        "date":    date,
        "events":  events,
        "count":   len(events),
        "status":  rng["status"],
        "errors":  list(rng["errors"]),
    }


# ══════════════════════════════════════════════════════════════════════════════
# MONTH-CURATED — Curated monthly calendar overview
# ══════════════════════════════════════════════════════════════════════════════

def _build_month_days(
    year:             int,
    month:            int,
    raw_counts:       dict[str, int],
    curated_by_date:  dict[str, list[dict]],
) -> list[dict]:
    """
    Build the month days array for month-curated response.
    count = total FMP events for the date (pre-filter).
    topEvents = curated scored events (post-pipeline, capped at max_per_day).
    """
    import calendar as _cal
    today_str   = _today()
    this_ym     = today_str[:7]          # "YYYY-MM"
    month_ym    = f"{year:04d}-{month:02d}"

    num_days = _cal.monthrange(year, month)[1]
    days: list[dict] = []
    for day_num in range(1, num_days + 1):
        d_str = f"{year:04d}-{month:02d}-{day_num:02d}"
        days.append({
            "date":           d_str,
            "dayOfMonth":     day_num,
            "isCurrentMonth": month_ym == this_ym,
            "count":          raw_counts.get(d_str, 0),
            "topEvents":      curated_by_date.get(d_str, []),
        })
    return days


async def get_month_curated(
    api_key:     str,
    year:        int,
    month:       int,
    scope:       Optional[str] = None,
    search:      Optional[str] = None,
    max_per_day: int = _MONTH_PER_DAY_DEFAULT,
) -> dict:
    """
    Curated monthly earnings calendar overview.

    For each calendar day:
      count     = total FMP earnings that day (raw, before any filtering)
      topEvents = curated scored events (max_per_day per day)

    Curated pipeline is identical to week-clean but spanning the whole month:
      • Raw fetch in sequential 7-day chunks (≤ 5 FMP calls — covers any month).
      • Pre-filter → pre-score → enrich top 200 → eligibility → score → decorate.
      • Enrichment: max 40 live profiles, concurrency=2.
      • 429 circuit breaker → partial result.
      • No AI, no Finnhub, no Polymarket.

    Response: { asOf, source, year, month, monthLabel, monthStart, monthEnd,
                days[], status, errors }
    """
    import calendar as _cal

    max_per_day = min(max(1, max_per_day), _MONTH_PER_DAY_MAX)

    # ── Watchlist short-circuit ────────────────────────────────────────────────
    # For scope=watchlist we bypass the curated snapshot pipeline entirely and
    # use the symbol-driven user_earnings_service path so the calendar shows
    # only events for the user's actual saved Watchlist tickers.
    if scope == "watchlist":
        return await _get_month_watchlist_scope(
            api_key     = api_key,
            year        = year,
            month       = month,
            search      = search,
            view        = "curated",
            max_per_day = max_per_day,
        )
    # ── End watchlist short-circuit ────────────────────────────────────────────

    # ── Portfolio short-circuit ────────────────────────────────────────────────
    if scope == "portfolio":
        return await _get_month_portfolio_scope(
            api_key     = api_key,
            year        = year,
            month       = month,
            search      = search,
            view        = "curated",
            max_per_day = max_per_day,
        )
    # ── End portfolio short-circuit ────────────────────────────────────────────

    last_day_num  = _cal.monthrange(year, month)[1]
    month_start_d = datetime(year, month, 1).date()
    month_end_d   = datetime(year, month, last_day_num).date()
    month_label   = datetime(year, month, 1).strftime("%B %Y")
    month_start   = month_start_d.strftime("%Y-%m-%d")
    month_end     = month_end_d.strftime("%Y-%m-%d")

    # -- Result cache (skip for filtered views)
    _mc_ck = f"earnings:curated:month:{year}:{month}"
    if not scope and not search:
        _mc_cached = cache.get(_mc_ck)
        if _mc_cached is not None:
            _log_telemetry("month-curated", f"{month_start}→{month_end}", cache_hit=True)
            return _mc_cached

    # Split the month into Mon–Fri weeks (same boundaries as week-clean).
    # Starting from the Monday of the week containing the 1st of the month
    # ensures that partial weeks at month edges are handled correctly.
    first_mon = month_start_d - timedelta(days=month_start_d.weekday())

    weeks: list[tuple[str, str]] = []
    cur_mon = first_mon
    while cur_mon <= month_end_d:
        cur_fri = cur_mon + timedelta(days=4)
        weeks.append((cur_mon.strftime("%Y-%m-%d"), cur_fri.strftime("%Y-%m-%d")))
        cur_mon += timedelta(days=7)

    # Snapshot-first month assembly: read pre-built weekly snapshots WITHOUT any live enrichment.
    # Month view NEVER runs get_curated_earnings_range() inline — that would trigger 5 sequential
    # enrichment pipelines (5 × 40 live FMP profile calls) blocking the page for minutes.
    # Missing weeks get a background build queued; the response returns partial with staleLKG=True.
    merged_events_by_date: dict[str, list[dict]] = {}
    merged_raw_counts:     dict[str, int]        = {}
    all_errors:            list[str]             = []
    any_partial                                   = False
    missing_weeks:         list[tuple[str, str]] = []

    for w_from, w_to in weeks:
        snap = _get_snap_or_lkg_fast(w_from, w_to)
        if snap is None:
            # No snapshot available — queue background build, mark partial
            missing_weeks.append((w_from, w_to))
            any_partial = True
            continue
        if snap.get("staleLKG"):
            any_partial = True
        if snap.get("rateLimited"):
            any_partial = True
        for err in snap.get("errors", []):
            if err not in all_errors:
                all_errors.append(err)
        for d, evs in snap.get("eventsByDate", {}).items():
            try:
                d_date = datetime.strptime(d, "%Y-%m-%d").date()
            except ValueError:
                continue
            if d_date < month_start_d or d_date > month_end_d:
                continue
            merged_events_by_date[d] = evs
        for d, cnt in snap.get("rawCounts", {}).items():
            try:
                d_date = datetime.strptime(d, "%Y-%m-%d").date()
            except ValueError:
                continue
            if d_date < month_start_d or d_date > month_end_d:
                continue
            merged_raw_counts[d] = cnt

    # Queue background builds for weeks with no snapshot data
    if missing_weeks and api_key:
        for w_from, w_to in missing_weeks:
            asyncio.create_task(_background_build_week(api_key, w_from, w_to))

    if missing_weeks:
        all_errors.append(
            f"Snapshots missing for {len(missing_weeks)} week(s) — background build queued. "
            "Reload in ~60 seconds for full data."
        )

    # Apply max_per_day cap (engine already deduped by symbol per date)
    curated_by_date: dict[str, list[dict]] = {
        d: evs[:max_per_day] for d, evs in merged_events_by_date.items()
    }

    days          = _build_month_days(year, month, merged_raw_counts, curated_by_date)
    total_curated = sum(len(v) for v in curated_by_date.values())

    _mc_result = {
        "asOf":          _today(),
        "source":        "fmp",
        "year":          year,
        "month":         month,
        "monthLabel":    month_label,
        "monthStart":    month_start,
        "monthEnd":      month_end,
        "days":          days,
        "status":        "partial" if any_partial else "ok",
        "partial":       any_partial,
        "missing_weeks": [
            {"week_start": w_from, "week_end": w_to}
            for w_from, w_to in missing_weeks
        ],
        "errors":        all_errors,
    }

    if not any_partial and not scope and not search:
        cache.set(_mc_ck, _mc_result, _TTL_RESULT)

    # Month view reads from snapshots — no live FMP HTTP calls are made here
    _log_telemetry(
        "month-curated", f"{month_start}→{month_end}",
        raw_hits=0,
        raw_misses=0,
        fmp_calendar_http_calls=0,
        raw_events_count=0,
        live_profile_calls=0,
        stale_lkg=any_partial,
        rate_limited=any_partial,
    )
    return _mc_result


# ══════════════════════════════════════════════════════════════════════════════
# BACKGROUND SNAPSHOT BUILDER + PRECOMPUTE LOOP
# ══════════════════════════════════════════════════════════════════════════════

async def _background_build_week(api_key: str, mon: str, fri: str) -> None:
    """Fire-and-forget coroutine that builds one weekly snapshot without blocking callers."""
    try:
        print(f"[earn_snap] background build started {mon}→{fri}")
        await build_curated_week_snapshot(api_key, mon, fri, force=False)
        print(f"[earn_snap] background build done    {mon}→{fri}")
    except Exception as e:
        print(f"[earn_snap] background build failed  {mon}→{fri}: {e}")


async def build_curated_week_snapshot(
    api_key: str,
    mon:     str,
    fri:     str,
    force:   bool = False,
) -> dict:
    """
    Build (or rebuild) the curated weekly snapshot for a Mon–Fri week.

    Runs the FULL enrichment pipeline, then persists to disk and in-memory cache.
    Called by: _earnings_curated_precompute_loop, admin backfill endpoint, _background_build_week.

    This function is NEVER called on normal page load — only by the background loop
    or an explicit admin/backfill request.

    force=True: bypass snapshot cache check and always rebuild (used on Saturday night batch).
    force=False: skip if a fresh snapshot already exists.
    """
    ck = _snap_ck(mon, fri)

    if not force:
        # Return existing fresh snapshot if available
        existing = cache.get(ck)
        if existing is not None:
            return existing
        existing = _read_earn_snap_from_disk(_snap_disk_path(mon, fri))
        if existing is not None:
            cache.set(ck, existing, _TTL_SNAP)
            return existing

    # Force rebuild: clear stale in-memory cache so get_curated_earnings_range
    # runs the live pipeline instead of serving the old snapshot
    cache.delete(ck)
    old_ck = f"earn:cur:rng:{mon}:{fri}"  # legacy key — delete too
    cache.delete(old_ck)

    t0 = time.monotonic()
    result = await get_curated_earnings_range(api_key, mon, fri)
    ms = int((time.monotonic() - t0) * 1000)

    n_events = len(result.get("allEvents", []))
    n_focus  = sum(1 for e in result.get("allEvents", []) if e.get("isFocus"))
    print(
        f"[earn_snap] built {mon}→{fri} "
        f"events={n_events} focus={n_focus} "
        f"cal_http={result.get('calHttpCalls',0)} enrich_http={result.get('enrichHttpCalls',0)} "
        f"ms={ms}"
    )
    return result


async def _earnings_curated_precompute_loop() -> None:
    """
    Background loop that pre-builds curated weekly snapshots so page loads never trigger
    live FMP enrichment.

    Schedule:
      • Startup: waits 3 minutes (lets Theme RS / Sectors / Macro finish FMP warmup),
        then builds any missing snapshots for the current week + next 5 weeks.
      • Every 6 hours (or 5 minutes if weeks are still missing): rescan and build.
      • Saturdays (ET): force-rebuilds all upcoming 6 weeks (full Saturday-night batch).
      • 30-second stagger between week builds to avoid FMP burst.
      • Exponential backoff on 429 / failure: 5 min → 10 min → 20 min … capped 60 min.

    State tracking (_week_states dict) is consumed by the admin snapshot-status endpoint.
    """
    try:
        from config import FMP_API_KEY as _api_key
    except ImportError:
        print("[earn_precompute] No FMP_API_KEY — precompute loop disabled")
        return

    if not _api_key:
        print("[earn_precompute] FMP_API_KEY empty — precompute loop disabled")
        return

    # Initial delay: let Theme RS, Sectors, Macro, and other startup FMP jobs finish
    # before earnings precompute starts hammering the same API key.
    _STARTUP_DELAY_S = 180   # 3 minutes
    print(f"[earn_precompute] Waiting {_STARTUP_DELAY_S}s before first build cycle…")
    await asyncio.sleep(_STARTUP_DELAY_S)
    print("[earn_precompute] Starting earnings curated precompute loop")

    while True:
        still_missing = 0
        try:
            now         = datetime.now(timezone.utc)
            today       = now.date()
            is_saturday = today.weekday() == 5
            mono_now    = time.monotonic()

            # Canonical Monday of current week
            mon0 = today - timedelta(days=today.weekday())

            # Current week + next 5 (6 total, priority order)
            weeks_to_check = [
                (
                    (mon0 + timedelta(weeks=i)).strftime("%Y-%m-%d"),
                    (mon0 + timedelta(weeks=i) + timedelta(days=4)).strftime("%Y-%m-%d"),
                )
                for i in range(6)
            ]

            built = 0

            for mon_str, fri_str in weeks_to_check:
                # Ensure state entry exists
                if mon_str not in _week_states:
                    _week_states[mon_str] = _init_week_state(mon_str)
                state = _week_states[mon_str]

                if is_saturday:
                    # Saturday night full-batch: always rebuild
                    needs_build = True
                else:
                    # Skip if a fresh snapshot already exists
                    if cache.get(_snap_ck(mon_str, fri_str)) is not None:
                        state["status"] = "fresh"
                        continue
                    if _read_earn_snap_from_disk(_snap_disk_path(mon_str, fri_str)) is not None:
                        state["status"] = "fresh"
                        continue

                    # Respect exponential backoff — don't retry too soon after failure
                    nr = state.get("next_retry_at")
                    if nr is not None and mono_now < nr:
                        wait_s = round(nr - mono_now)
                        print(f"[earn_precompute] {mon_str} backoff: retry in {wait_s}s")
                        still_missing += 1
                        continue

                    needs_build = True

                if not needs_build:
                    continue

                state["status"]       = "building"
                state["last_attempt"] = time.time()

                try:
                    result = await build_curated_week_snapshot(
                        _api_key, mon_str, fri_str, force=is_saturday
                    )
                    built += 1

                    if result.get("rateLimited"):
                        # Build partially completed but was rate-limited —
                        # treat as LKG + schedule a retry with exponential backoff
                        rc      = state.get("retry_count", 0) + 1
                        backoff = min(300 * (2 ** (rc - 1)), 3600)  # 5m→10m→20m… ≤60m
                        state["status"]        = "lkg"
                        state["last_error"]    = "rate_limited"
                        state["retry_count"]   = rc
                        state["next_retry_at"] = time.monotonic() + backoff
                        print(
                            f"[earn_precompute] {mon_str} rate-limited "
                            f"(attempt {rc}) → retry in {backoff//60}min"
                        )
                        still_missing += 1
                    else:
                        state["status"]        = "fresh"
                        state["last_built"]    = time.time()
                        state["last_error"]    = None
                        state["retry_count"]   = 0
                        state["next_retry_at"] = None

                    # 30-second stagger between week builds
                    await asyncio.sleep(30)

                except Exception as exc:
                    rc      = state.get("retry_count", 0) + 1
                    backoff = min(300 * (2 ** (rc - 1)), 3600)
                    state["status"]        = "failed"
                    state["last_error"]    = str(exc)
                    state["retry_count"]   = rc
                    state["next_retry_at"] = time.monotonic() + backoff
                    print(
                        f"[earn_precompute] build failed {mon_str}: {exc} "
                        f"(attempt {rc}) → retry in {backoff//60}min"
                    )
                    still_missing += 1
                    await asyncio.sleep(30)

            if built or still_missing:
                print(
                    f"[earn_precompute] cycle done: built={built} still_pending={still_missing}"
                )
            else:
                print(
                    f"[earn_precompute] cycle done: all {len(weeks_to_check)} weeks already cached"
                )

        except Exception as e:
            print(f"[earn_precompute] loop error: {e}")

        # If weeks are still pending, retry soon; otherwise full 6-hour cycle
        if still_missing:
            await asyncio.sleep(5 * 60)   # 5 minutes — quick retry
        else:
            await asyncio.sleep(6 * 3600)
