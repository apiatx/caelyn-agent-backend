"""
Screener Hub service.

Backs the four Screener Hub tabs:
  thematic              — one-or-more curated theme universes (THEME_RS_UNIVERSE keys)
  social                — tickers driven by the X/Grok consensus weekly snapshot
  bottlenecks           — Chain Reaction NODE_REGISTRY tickers
  watchlist_portfolio   — user watchlists + portfolio holdings

Layered cache design:
  - Universe symbols are persisted to screener_universe_snapshots (per tab/theme)
  - Fundamentals are persisted to screener_fundamentals_cache (weekly TTL, FMP)
  - Live quotes are persisted to screener_quote_cache (Tradier; short TTL)

Thematic universe source priority (per theme):
  A. ETF holdings   — direct disk read from data/etf_holdings/{ETF}.json (fast, 7-day cache)
  B. LKG leaders    — stocks from themes_rs_lkg.json (refreshed by theme_rs_service)
  C. FMP peers      — async stable/stock-peers from candidate anchors (only when A+B < threshold)
  D. candidate_symbols — static seed from THEME_RS_UNIVERSE (used_static_fallback=true when hit)
  E. proxy_symbols  — ETF tickers, absolute last resort only (no stocks found in A-D)

Guardrails enforced here (see CLAUDE.md):
  - Never overwrite a valid cached row with an empty/failed API response.
  - Never blank the whole table because one row failed enrichment.
  - Tradier quote refresh is page-aware: only the symbols requested by the
    current /api/screener-hub call are re-fetched.
  - If FMP fails for a symbol, we keep its previous cached row.
"""
from __future__ import annotations

import asyncio
import hashlib
import json as _json_mod
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import httpx

from data.cache import cache as _shared_lkg_cache
from data.screener_hub_store import (
    ensure_tables,
    upsert_fundamentals,
    get_fundamentals,
    fundamentals_fresh_symbols,
    fundamentals_table_stats,
    insert_universe_snapshot,
    get_latest_universe,
    universe_table_stats,
    upsert_quote,
    get_quotes,
    quote_table_stats,
    start_job_run,
    finish_job_run,
    latest_job_runs,
    get_returns,
    get_latest_chain_reaction_weekly,
    get_all_thematic_screener_meta,
    upsert_screener_options_oi,
    get_screener_options_oi,
    get_query_cache,
    set_query_cache,
)


# ── Config ─────────────────────────────────────────────────────────────────────

FMP_BASE = "https://financialmodelingprep.com/stable"
_FMP_TIMEOUT = 10.0
_PER_THEME_CAP = 75           # raised from 60 — gives screener candidates room
_SCREENER_RESERVE = 25        # slots reserved for FMP screener small/mid-cap names
_GLOBAL_TICKER_CAP = 400      # safety net per request
_FUNDAMENTALS_TTL_DAYS = 7
_QUOTE_TTL_OPEN_S = 90        # ~90s during US market open
_QUOTE_TTL_CLOSED_S = 30 * 60 # 30min when market closed
_FMP_SLEEP_BETWEEN_S = 0.75   # governed by fmp_governor; keep small

# Minimum dynamic-source symbols before FMP peers are tried.
# If ETF holdings + LKG leaders already give ≥ this many stocks for a theme,
# we skip the FMP peers API call (saves latency and rate-limit budget).
_MIN_DYN_BEFORE_PEERS = 8

# Max seconds for a live thematic universe build during a page-load request.
# If exceeded, return a safe 200 partial/empty instead of letting the proxy 502.
_THEMATIC_BUILD_TIMEOUT_S = 18.0

# ── Query-cache helpers ───────────────────────────────────────────────────────

def _compute_screener_cache_key(
    tab: str,
    theme: Optional[str],
    category: Optional[str],
    score_mode: bool,
    coc_filter: bool,
    market_cap_min: Optional[float],
    market_cap_max: Optional[float],
    min_volume: Optional[float],
    exchange: Optional[str],
    schema_version: str,
) -> str:
    """
    Deterministic SHA-256 cache key for a live screener query.

    Null/empty values are omitted so that
    theme=ai_networking (no minVolume) and theme=ai_networking&minVolume=0
    produce the same key when minVolume=0 is semantically equivalent to absent.
    """
    raw: dict = {
        "tab":              tab,
        "theme":            (theme or "").lower() or None,
        "category":         (category or "").lower() or None,
        "score_mode":       bool(score_mode),
        "coc_filter":       bool(coc_filter),
        "market_cap_min":   float(market_cap_min) if market_cap_min is not None else None,
        "market_cap_max":   float(market_cap_max) if market_cap_max is not None else None,
        "min_volume":       float(min_volume)      if min_volume      is not None else None,
        "exchange":         exchange.upper()        if exchange        else None,
        "schema_version":   schema_version,
        # Separate cache-bust version — increment when market cap units, filter
        # logic, or universe building is corrected so old entries are ignored.
        "cache_version":    SCREENER_QUERY_CACHE_VERSION,
    }
    # Drop None values so optional params don't change the key when absent
    params = {k: v for k, v in raw.items() if v is not None}
    canonical = _json_mod.dumps(params, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()

# Path to the config-driven theme → FMP industry mapping.
# Loaded lazily by _load_industry_map_config().
_INDUSTRY_MAP_PATH = Path(__file__).parent.parent / "data" / "theme_fmp_industry_map.json"

# ETF holdings disk cache directory
_ETF_HOLDINGS_DIR = Path(__file__).parent.parent / "data" / "etf_holdings"

# Dynamic Chain Reaction output file (written by Chain Reaction service if/when available)
_CHAIN_REACTION_OUTPUT = Path(__file__).parent.parent / "data" / "chain_reaction_output.json"

# Persisted OI snapshot for options_oi_change tracking (Part 2).
# Written lazily every _OI_PREV_TTL_H hours; read on every request (fast disk read).
_OI_PREV_PATH  = Path(__file__).parent.parent / "data" / "options_oi_prev.json"
_OI_PREV_TTL_H = 6.0   # hours between snapshot rotations

# ── Page-aware Tradier options enrichment config ───────────────────────────────
# All can be overridden via environment variables at runtime.
_OPT_REFRESH_ENABLED = os.getenv("OPTIONS_ACTIVE_PAGE_REFRESH_ENABLED", "true").lower() in ("1", "true", "yes")
_OPT_TTL_OPEN        = int(os.getenv("OPTIONS_ACTIVE_PAGE_TTL_SECONDS",        "120"))
_OPT_TTL_CLOSED      = int(os.getenv("OPTIONS_ACTIVE_PAGE_TTL_SECONDS_CLOSED", "900"))
_OPT_MAX_SYMS        = int(os.getenv("OPTIONS_ACTIVE_PAGE_MAX_SYMBOLS",        "40"))
_OPT_TIMEOUT         = float(os.getenv("OPTIONS_ACTIVE_PAGE_TIMEOUT_SECONDS",  "8"))
_OPT_CONCURRENCY     = int(os.getenv("OPTIONS_ACTIVE_PAGE_CONCURRENCY",        "3"))

# ── Non-equity / placeholder ticker detection ──────────────────────────────────
# Used to exclude money market funds, currency proxies, and zero-enriched ETF
# holding placeholders from the thematic stock screener.

# Money market fund tickers typically end in 2+ X's (FGXXX, VMFXX, SPAXX, etc.)
_MONEY_MARKET_TICKER_RE = re.compile(r"^[A-Z]{2,6}XX+$", re.IGNORECASE)

# ISO 4217 currency / FX codes that occasionally appear as tickers in ETF holdings
_KNOWN_FX_TICKERS: frozenset[str] = frozenset({
    "USD", "EUR", "GBP", "JPY", "CNY", "HKD", "KRW", "TWD", "AUD", "CAD",
    "CHF", "SGD", "MXN", "BRL", "INR", "RUB", "ZAR", "SEK", "NOK", "DKK",
    "NZD", "THB", "PHP", "IDR", "MYR", "VND", "CZK", "HUF", "PLN", "TRY",
    "ILS", "AED", "SAR", "QAR", "CLP", "PEN", "COP", "ARS", "EGP",
})


def _is_non_equity_row(row: dict) -> bool:
    """Return True if a row is a non-equity / placeholder instrument.

    Catches: ETF/mutual fund flags, money market fund tickers (end in XX+),
    ISO FX currency codes used as tickers, and zero-enriched ETF holding cash
    proxies (no market_cap, price, sector, or exchange and sourced from etf:*).

    Never over-filters: requires at least one hard signal before excluding.
    Valid ADRs with fundamentals data always pass through.
    """
    sym  = (row.get("symbol") or "").upper().strip()
    srcs = row.get("discovery_sources") or []

    # 1. Explicit ETF / fund flags from screener metadata
    if row.get("is_etf") or row.get("is_fund"):
        return True

    # 2. Money market fund ticker pattern (FGXXX, VMFXX, SPAXX, VMMXX …)
    if _MONEY_MARKET_TICKER_RE.match(sym):
        return True

    # 3. Known ISO 4217 FX / currency codes used as placeholder tickers (TWD, EUR …)
    if sym in _KNOWN_FX_TICKERS:
        return True

    # 4. Zero-enrichment + ETF-holding source = cash / currency proxy in ETF file
    # Valid stocks always have at least one of: market_cap, price, sector, exchange.
    _from_etf   = any(s.startswith("etf:") for s in srcs)
    _zero_enr   = (
        row.get("market_cap") is None
        and row.get("price")      is None
        and row.get("sector")     is None
        and row.get("exchange")   is None
    )
    if _from_etf and _zero_enr:
        return True

    return False


# ── Security junk / warrant / rights / SPAC detection ─────────────────────────
# Catches rights offerings, warrant stubs, unit offerings, blank-check SPACs.
# Applied client-side after FMP screener returns raw results.
_JUNK_NAME_RE = re.compile(
    r"("
    r"\bseries [a-z\d]+ right[s]?\b"     # "Series A Rights", "Series 2 Right"
    r"|[,\s]right[s]?\.?\s*$"            # ends with " Rights" / " Right"
    r"|\bwarrant[s]?\s*$"                # ends with "Warrant" / "Warrants"
    r"|\bwts?\s*$"                       # ends with "Wt" / "Wts" (warrant abbrev)
    r"|\bunits?\s*$"                     # ends with "Unit" / "Units"
    r"|\bacquisition corp\.?\b"          # "Acquisition Corp" / "Acquisition Corp."
    r"|\bspecial purpose acquisition\b"  # SPAC long form
    r"|\bblank[- ]check\b"              # blank check company
    r"|\bspac\b"                         # SPAC abbreviation
    r"|\bshell company\b"               # shell company
    r"|\bshell co\.?\b"                 # "Shell Co" / "Shell Co."
    r")",
    re.IGNORECASE,
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_theme_label(theme_key: str) -> str:
    """Return the human-readable display name for a theme key.

    Falls back to the key itself (title-cased) if the registry is unavailable
    or the theme is not found.
    """
    try:
        from services.theme_rs_universe import THEME_RS_UNIVERSE
        cfg = THEME_RS_UNIVERSE.get(theme_key) or {}
        label = cfg.get("display_name") or cfg.get("name")
        if label:
            return label
    except Exception:
        pass
    return theme_key.replace("_", " ").title()


def _is_market_open() -> bool:
    """Approximate US equity hours (NYSE) — used only to pick a quote TTL."""
    try:
        from zoneinfo import ZoneInfo
        et = ZoneInfo("America/New_York")
    except Exception:
        return False
    now = datetime.now(et)
    if now.weekday() >= 5:
        return False
    minutes = now.hour * 60 + now.minute
    return 9 * 60 + 30 <= minutes < 16 * 60


# ── Module-level ETF proxy set (all proxy_symbols across every theme entry) ────
# Built once at import time; used to exclude ETFs from thematic screener rows.

def _build_all_proxy_etfs() -> frozenset[str]:
    """Return the set of actual ETF ticker symbols used as theme proxies.

    Only includes proxy_symbols from themes whose proxy_type is "etf".
    Themes that set proxy_type="custom" use stock tickers (e.g. ANET, AVGO)
    as proxies — those must NOT be treated as ETFs or they will be stripped
    from the screener universe as if they were fund wrappers.
    """
    try:
        from services.theme_rs_universe import THEME_RS_UNIVERSE
        s: set[str] = set()
        for meta in THEME_RS_UNIVERSE.values():
            if (meta.get("proxy_type") or "etf") == "etf":
                for sym in (meta.get("proxy_symbols") or []):
                    if sym:
                        s.add(sym.upper())
        return frozenset(s)
    except Exception:
        return frozenset()


_ALL_PROXY_ETFS: frozenset[str] = _build_all_proxy_etfs()


# ── LKG theme-state helper ─────────────────────────────────────────────────────

def _get_theme_state_from_lkg(theme_key: str) -> dict:
    """Return {state, state_reason, rs_score} for *theme_key* from the themes LKG disk file.

    The Themes page (theme_rs_service) writes backend/data/themes_rs_lkg.json with a
    ``state`` field per row (active / emerging / neutral / weakening / dead_zone) and an
    ``rs_score`` (0–100).  Screener Hub reuses that data instead of computing a
    conflicting second system.

    Returns an empty dict when the LKG is unavailable or the key is not found.
    """
    try:
        import json
        lkg_path = Path(__file__).parent.parent / "data" / "themes_rs_lkg.json"
        if not lkg_path.exists():
            return {}
        raw = json.loads(lkg_path.read_text())
        rows: list[dict] = raw.get("rows", raw) if isinstance(raw, dict) else raw
        if not isinstance(rows, list):
            return {}
        for row in rows:
            if row.get("theme_id") == theme_key:
                return {
                    "state":        row.get("state"),
                    "state_reason": row.get("state_reason"),
                    "rs_score":     row.get("rs_score"),
                }
    except Exception as e:
        print(f"[SCREENER_HUB] _get_theme_state_from_lkg {theme_key} error: {e}")
    return {}


# ── Intraday default theme cache ───────────────────────────────────────────────
# Refreshed every _DEFAULT_THEME_TTL_S seconds so the default theme tracks live
# intraday RS scores (updated ~60s by theme_rs_service) rather than being locked
# to the first computation of the calendar day.
_DAILY_DEFAULT: dict = {}  # {"expires_at": float, "theme": str, "reason": str}
_DEFAULT_THEME_TTL_S = 600  # 10 minutes

# ── Per-theme on-demand refresh log ───────────────────────────────────────────
# Tracks when each theme last triggered a background universe refresh.
# Enforces a 24-hour cap so page loads can only schedule one rebuild per theme per day.
# NOTE: this dict is in-memory only. _theme_refresh_allowed() falls back to the DB
# snapshot timestamps when the key is missing (post-restart durability).
_THEME_REFRESH_LOG: dict[str, str] = {}  # theme_key → ISO timestamp of last scheduled refresh
_THEME_REFRESH_CAP_H   = 24.0   # hours between permitted on-demand background refreshes
_THEME_REFRESH_STALE_H = 24.0   # snapshot age (hours) that triggers a background refresh
_INLINE_REFRESH_TIMEOUT_S = 12.0  # max seconds to wait for inline selected-theme refresh

# ── Snapshot schema versioning ─────────────────────────────────────────────────
# Bump this string whenever the build pipeline changes in a way that invalidates
# old snapshots (e.g. new fmp_industries added, pre-filter removed, etc.).
# Selected-theme requests whose cached snapshot has a different (or missing)
# schema version AND zero fmp_screener rows get ONE bypass of the 24h cap so
# the new pipeline runs immediately.  After the bypass refresh the 24h cap
# applies normally to the fresh snapshot.
THEMATIC_REFRESH_SCHEMA_VERSION = "v2_inline_fmp_industry_map"
# Separate version for the query cache key — increment any time market cap
# unit normalization, filter logic, or universe building is corrected.
# Old cache entries with a different version hash are automatically ignored.
SCREENER_QUERY_CACHE_VERSION    = "v2_marketcap_units_ai_networking_fix"
_THEME_VERSION_REFRESHED: set[str] = set()  # themes that have done bypass this session

# ── In-flight refresh tracking ─────────────────────────────────────────────────
# Prevents duplicate tasks for the same theme and limits total FMP concurrency.
_THEME_REFRESH_INFLIGHT: set[str] = set()  # themes with an active background task
_FMP_REFRESH_MAX_CONCURRENT = 3            # max simultaneous FMP theme refresh tasks
_FMP_REFRESH_SEM: Optional["asyncio.Semaphore"] = None  # lazy-init inside event loop


def _get_fmp_refresh_sem() -> "asyncio.Semaphore":
    """Return (lazily-initialised) asyncio.Semaphore for FMP theme refresh tasks.

    Must be called from within a running event loop so the Semaphore is created
    in the correct loop.
    """
    global _FMP_REFRESH_SEM
    if _FMP_REFRESH_SEM is None:
        _FMP_REFRESH_SEM = asyncio.Semaphore(_FMP_REFRESH_MAX_CONCURRENT)
    return _FMP_REFRESH_SEM


def _get_daily_default_theme() -> tuple[Optional[str], str]:
    """Return (theme_key, reason) for the current default theme.

    Cached for _DEFAULT_THEME_TTL_S seconds (10 min) so the default theme
    tracks intraday RS scores — which theme_rs_service refreshes every ~60s —
    rather than being locked to the first computation of the calendar day.
    """
    now_mono = time.monotonic()
    if now_mono < _DAILY_DEFAULT.get("expires_at", 0.0) and _DAILY_DEFAULT.get("theme"):
        return _DAILY_DEFAULT["theme"], _DAILY_DEFAULT.get("reason", "intraday_cache")
    try:
        meta = _theme_metadata()
        dt   = meta.get("default_theme")
        rsn  = meta.get("default_theme_reason") or "computed"
        _DAILY_DEFAULT.update({"expires_at": now_mono + _DEFAULT_THEME_TTL_S,
                                "theme": dt, "reason": rsn})
        return dt, rsn
    except Exception as e:
        print(f"[SCREENER_HUB] default theme compute error: {e}")
        return None, "error"


def _theme_refresh_allowed(theme_key: str) -> bool:
    """Return True if at least _THEME_REFRESH_CAP_H hours have elapsed since the last
    on-demand background refresh was scheduled for this theme.

    Checks the in-memory log first (fast path). On a cache miss — which happens after
    every backend restart — falls back to the DB snapshot timestamp so the cap survives
    restarts and prevents re-hammering FMP for all 60 themes simultaneously.
    """
    last_iso = _THEME_REFRESH_LOG.get(theme_key)
    if last_iso:
        try:
            last_dt = datetime.fromisoformat(last_iso)
            return (datetime.now(timezone.utc) - last_dt).total_seconds() > _THEME_REFRESH_CAP_H * 3600
        except Exception:
            pass
        return True  # malformed entry → allow

    # In-memory log empty (post-restart) — check DB for most recent snapshot.
    try:
        from data.screener_hub_store import get_theme_last_refresh_ts
        db_dt = get_theme_last_refresh_ts(theme_key)
        if db_dt:
            elapsed_s = (datetime.now(timezone.utc) - db_dt).total_seconds()
            if elapsed_s < _THEME_REFRESH_CAP_H * 3600:
                # Hydrate in-memory log from DB so subsequent calls within
                # this process lifetime don't re-query the DB.
                _THEME_REFRESH_LOG[theme_key] = db_dt.isoformat()
                return False
    except Exception:
        pass
    return True


def _record_theme_refresh(theme_key: str) -> None:
    """Log the current UTC timestamp as the last refresh time for *theme_key*."""
    _THEME_REFRESH_LOG[theme_key] = datetime.now(timezone.utc).isoformat()


def _theme_refresh_next_allowed_iso(theme_key: str) -> Optional[str]:
    """Return ISO-8601 timestamp of when the next refresh is permitted, or None."""
    last_iso = _THEME_REFRESH_LOG.get(theme_key)
    if not last_iso:
        return None
    try:
        last_dt = datetime.fromisoformat(last_iso)
        return datetime.fromtimestamp(
            last_dt.timestamp() + _THEME_REFRESH_CAP_H * 3600, tz=timezone.utc
        ).isoformat()
    except Exception:
        return None


async def _background_refresh_theme(theme_key: str, *, reason: str = "stale") -> None:
    """Rebuild a theme's universe snapshot in the background.

    Runs with_fmp_screener=True so the new snapshot contains a populated
    screener_meta_by_symbol dict (sector, industry, market_cap, price, volume,
    exchange, beta …).  This is the primary enrichment source for thematic rows
    whose symbols haven't been seen by the FMP profile warm-job yet.

    Concurrency is limited to _FMP_REFRESH_MAX_CONCURRENT simultaneous tasks via
    an asyncio.Semaphore.  In-flight dedup is handled by the caller — this function
    just adds/removes the theme from _THEME_REFRESH_INFLIGHT and acquires the sem.
    """
    _THEME_REFRESH_INFLIGHT.add(theme_key)
    try:
        async with _get_fmp_refresh_sem():
            print(f"[SCREENER_HUB] background refresh started for theme={theme_key!r} reason={reason!r} inflight={len(_THEME_REFRESH_INFLIGHT)}")
            symbols_map, breakdowns = await asyncio.wait_for(
                _build_thematic_universe(theme_key, with_fmp_peers=False, with_fmp_screener=True),
                timeout=120.0,  # generous background-task timeout; no user waiting on this
            )
            syms = symbols_map.get(theme_key, [])
            bd   = breakdowns.get(theme_key, {})
            if syms:
                bd["_refresh_used_fmp_screener"]  = bool(bd.get("fmp_screener_calls_used", 0) > 0)
                bd["_refresh_reason"]             = reason
                bd["refresh_schema_version"]      = THEMATIC_REFRESH_SCHEMA_VERSION
                bd["build_pipeline_version"]      = THEMATIC_REFRESH_SCHEMA_VERSION
                bd["fmp_screener_used"]           = bool(bd.get("fmp_screener_calls_used", 0) > 0)
                bd["fmp_industries_used"]         = (
                    _load_industry_map_config()
                    .get("themes", {})
                    .get(theme_key, {})
                    .get("fmp_industries") or []
                )
                insert_universe_snapshot(
                    universe_type="thematic", theme_key=theme_key,
                    symbols=syms, source="on_demand_refresh",
                    status="ok", ttl_days=2,
                    metadata=bd,
                )
                # Invalidate all query-cache entries for this theme so that
                # responses written against the old (smaller) snapshot are not
                # served after the universe has been upgraded.  E.g. an "All"
                # entry written against a 5-symbol live-build snapshot must not
                # be served now that the snapshot has 30 symbols.
                try:
                    from data.screener_hub_store import expire_theme_query_cache
                    expire_theme_query_cache(theme_key)
                except Exception as _exp_err:
                    print(f"[SCREENER_HUB] expire_theme_query_cache error (non-fatal): {_exp_err}")
                print(
                    f"[SCREENER_HUB] background refresh done for {theme_key!r}: "
                    f"{len(syms)} symbols, fmp_screener_calls={bd.get('fmp_screener_calls_used', 0)}, "
                    f"screener_meta_symbols={len(bd.get('screener_meta_by_symbol') or {})}"
                )
            else:
                print(f"[SCREENER_HUB] background refresh for {theme_key!r}: no symbols returned — skipping write")
    except asyncio.TimeoutError:
        print(f"[SCREENER_HUB] background refresh timed out for theme={theme_key!r}")
    except Exception as exc:
        print(f"[SCREENER_HUB] background refresh error for theme={theme_key!r}: {exc}")
    finally:
        # Always remove from in-flight set so the dedup guard resets correctly.
        _THEME_REFRESH_INFLIGHT.discard(theme_key)


# ── ETF holdings disk reader ───────────────────────────────────────────────────

def _read_etf_holdings_from_disk(etf_sym: str) -> list[str]:
    """
    Read top holdings for an ETF from the 7-day disk cache.

    Filters out cross-listed non-US tickers (e.g. "LAR.TO", "600900.SS") and
    bond/cash entries. Returns an empty list on cache miss or parse error.
    """
    import json
    path = _ETF_HOLDINGS_DIR / f"{etf_sym.upper()}.json"
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text())
        holdings = data.get("holdings") or data.get("top_holdings") or []
        result: list[str] = []
        for h in holdings:
            if not isinstance(h, dict):
                continue
            raw = (h.get("ticker") or "").strip().upper()
            if not raw:
                continue
            # Skip cross-listed / non-US tickers (contain a dot with exchange suffix)
            if "." in raw:
                continue
            # Skip bond/cash/FX placeholders
            if raw in ("CASH", "USD", "EUR", "TBD", "OTHER",
                       "TWD", "JPY", "CNY", "KRW", "GBP", "CAD",
                       "AUD", "CHF", "HKD", "SGD", "NZD", "INR"):
                continue
            # Standard US equity ticker: 1–5 alpha chars, optionally followed by
            # one special char + alpha (e.g. BRK-B, BF-B).  Max 6 chars total.
            if len(raw) > 6:
                continue
            # Skip money-market / mutual-fund tickers (e.g. FGXXX, SPAXX)
            if raw.endswith("XXX"):
                continue
            result.append(raw)
        return result
    except Exception as e:
        print(f"[SCREENER_HUB] ETF holdings disk read {etf_sym}: {e}")
        return []


# ── LKG leaders/laggards loader ────────────────────────────────────────────────

def _load_lkg_leaders_map() -> dict[str, list[str]]:
    """
    Load leaders + laggards lists from themes_rs_lkg.json, keyed by theme_id.

    These are stocks discovered dynamically by theme_rs_service (via ETF holdings
    expansion + RS scoring) — not static seeds.  Refreshed whenever the Themes
    page is recomputed.
    """
    out: dict[str, list[str]] = {}
    try:
        import json
        path = Path(__file__).parent.parent / "data" / "themes_rs_lkg.json"
        if not path.exists():
            return out
        raw = json.loads(path.read_text())
        rows: list[dict] = raw.get("rows", raw) if isinstance(raw, dict) else raw
        if not isinstance(rows, list):
            return out
        for row in rows:
            tid = (row.get("theme_id") or "").strip()
            if not tid:
                continue
            leaders = [
                l.get("symbol") for l in (row.get("leaders") or [])
                if isinstance(l, dict) and l.get("symbol")
            ]
            laggards = [
                l.get("symbol") for l in (row.get("laggards") or [])
                if isinstance(l, dict) and l.get("symbol")
            ]
            out[tid] = leaders + laggards
    except Exception as e:
        print(f"[SCREENER_HUB] _load_lkg_leaders_map error: {e}")
    return out


# ── FMP peers (async, used as tertiary dynamic source) ─────────────────────────

async def _fmp_peers_for_anchors(
    anchors: list[str],
    max_peers_per_anchor: int = 8,
    timeout: float = 8.0,
) -> tuple[list[str], list[str]]:
    """
    Fetch FMP stable/stock-peers for each anchor ticker.

    Returns (peer_symbols, anchors_that_returned_results).
    Never raises; returns empty lists on failure or missing API key.
    Governor-protected: skips gracefully if daily/job budget is exhausted.
    """
    api_key = os.getenv("FMP_API_KEY") or ""
    if not api_key or not anchors:
        return [], []

    try:
        from services.fmp_governor import fmp_governor as _gov
    except Exception:
        _gov = None

    async def _one(anchor: str) -> list[str]:
        try:
            if _gov is not None:
                slot_ok = await _gov.acquire(job_name="fmp_peers")
                if not slot_ok:
                    print(f"[SCREENER_HUB] fmp_peers governor budget hit, skipping anchor={anchor}")
                    return []
            async with httpx.AsyncClient() as client:
                r = await client.get(
                    f"{FMP_BASE}/stock-peers",
                    params={"symbol": anchor.upper(), "apikey": api_key},
                    timeout=timeout,
                )
            if _gov is not None:
                _gov.record_call()
            if r.status_code != 200:
                return []
            raw = r.json()
            peers: list[str] = []
            if isinstance(raw, list):
                for item in raw:
                    if isinstance(item, str):
                        peers.append(item.upper())
                    elif isinstance(item, dict):
                        sym = item.get("symbol") or ""
                        if isinstance(sym, str) and sym:
                            peers.append(sym.upper())
                        pl = item.get("peersList") or []
                        if isinstance(pl, list):
                            peers.extend(s.upper() for s in pl if isinstance(s, str) and s)
            elif isinstance(raw, dict):
                pl = raw.get("peersList") or raw.get("peers") or []
                peers.extend(str(s).upper() for s in pl if s)
            return peers[:max_peers_per_anchor]
        except Exception as e:
            print(f"[SCREENER_HUB] FMP peers {anchor}: {e}")
            return []

    try:
        to_call = anchors[:3]
        results = await asyncio.wait_for(
            asyncio.gather(*[_one(a) for a in to_call], return_exceptions=True),
            timeout=timeout + 3.0,
        )
        found: list[str] = []
        anchors_used: list[str] = []
        for anchor, result in zip(to_call, results):
            if isinstance(result, list) and result:
                anchors_used.append(anchor)
                for p in result:
                    if p and p not in found:
                        found.append(p)
        return found, anchors_used
    except Exception as e:
        print(f"[SCREENER_HUB] _fmp_peers_for_anchors gather error: {e}")
        return [], []


# ── Overlap loaders (social / options / watchlist) ────────────────────────────

def _load_social_overlap() -> set[str]:
    """Top tickers from x_consensus_weekly.json (social screener)."""
    syms: set[str] = set()
    try:
        import json
        p = Path(__file__).parent.parent / "data" / "x_consensus_weekly.json"
        if p.exists():
            d = json.loads(p.read_text())
            for item in (d.get("top_tickers") or []):
                sym = item.get("symbol") if isinstance(item, dict) else (item if isinstance(item, str) else None)
                if sym:
                    syms.add(str(sym).upper())
    except Exception as e:
        print(f"[SCREENER_HUB] _load_social_overlap error: {e}")
    return syms


def _load_options_overlap() -> set[str]:
    """Tickers with notable options flow from all LKG files."""
    syms: set[str] = set()
    try:
        import json
        data_dir = Path(__file__).parent.parent / "data"
        for fname in [
            "options_master_lkg_v1.json",
            "options_lkg_v1_large_cap.json",
            "options_lkg_v1_small_cap.json",
            "options_lkg_v1_megacap.json",
        ]:
            p = data_dir / fname
            if not p.exists():
                continue
            d = json.loads(p.read_text())
            for t in (d.get("tickers") or []):
                sym = t.get("ticker") if isinstance(t, dict) else (t if isinstance(t, str) else None)
                if sym:
                    syms.add(str(sym).upper())
    except Exception as e:
        print(f"[SCREENER_HUB] _load_options_overlap error: {e}")
    return syms


def _load_options_detail_map() -> dict[str, dict]:
    """
    Build per-ticker options detail from all LKG files.

    Returns {sym: {options_oi, previous_options_oi, options_oi_change,
                   options_activity_score}}.
    Loaded once per request — never blocks on live API calls.

    options_oi             : total_oi from the current LKG snapshot
    previous_options_oi    : total_oi from the last persisted OI snapshot (if any)
    options_oi_change      : current_oi − previous_oi (absolute; None on first run)
    options_activity_score : vol/OI ratio scaled 0–10 (higher = more active flow)

    OI change uses a lightweight disk snapshot at _OI_PREV_PATH, refreshed
    lazily every _OI_PREV_TTL_H hours via a safe temp-file rename.
    On the very first request the snapshot is seeded and oi_change returns None;
    subsequent requests return the numeric difference against the prior snapshot.
    """
    detail: dict[str, dict] = {}
    try:
        import json
        data_dir = Path(__file__).parent.parent / "data"
        for fname in [
            "options_master_lkg_v1.json",
            "options_lkg_v1_large_cap.json",
            "options_lkg_v1_small_cap.json",
            "options_lkg_v1_megacap.json",
        ]:
            p = data_dir / fname
            if not p.exists():
                continue
            d = json.loads(p.read_text())
            for t in (d.get("tickers") or []):
                if not isinstance(t, dict):
                    continue
                sym = (t.get("ticker") or "").upper()
                if not sym or sym in detail:
                    continue
                total_oi  = t.get("total_oi")
                total_vol = t.get("total_volume")
                activity_score: Optional[float] = None
                if total_oi and total_vol and total_oi > 0:
                    activity_score = round(min(10.0, (total_vol / total_oi) * 2), 2)
                detail[sym] = {
                    "options_oi":             int(total_oi) if total_oi is not None else None,
                    "previous_options_oi":    None,
                    "options_oi_change":      None,
                    "options_activity_score": activity_score,
                }
    except Exception as e:
        print(f"[SCREENER_HUB] _load_options_detail_map error: {e}")

    # ── OI change: compare current OI against persisted previous snapshot ──────
    # The snapshot stores {sym: oi, "_as_of_ts": unix_float} on disk.
    # Safe atomic write (temp + rename) prevents partial reads.
    try:
        import json as _json
        prev_map: dict[str, int] = {}
        prev_ts:  float          = 0.0

        if _OI_PREV_PATH.exists():
            raw = _json.loads(_OI_PREV_PATH.read_text())
            if isinstance(raw, dict):
                prev_ts  = float(raw.get("_as_of_ts", 0))
                prev_map = {
                    k: int(v)
                    for k, v in raw.items()
                    if not k.startswith("_") and isinstance(v, (int, float)) and v >= 0
                }

        # Enrich detail entries with oi_change if a previous snapshot exists.
        if prev_map:
            for sym, v in detail.items():
                cur_oi = v.get("options_oi")
                p_oi   = prev_map.get(sym)
                if cur_oi is not None and p_oi is not None:
                    v["previous_options_oi"] = p_oi
                    v["options_oi_change"]   = cur_oi - p_oi

        # Refresh snapshot lazily: seed on first run, rotate when stale.
        now_ts = time.time()
        if not prev_map or (now_ts - prev_ts) > _OI_PREV_TTL_H * 3600:
            current_snap: dict = {
                sym: v["options_oi"]
                for sym, v in detail.items()
                if v.get("options_oi") is not None
            }
            current_snap["_as_of_ts"] = now_ts
            try:
                _tmp = _OI_PREV_PATH.with_suffix(".tmp")
                _tmp.write_text(_json.dumps(current_snap))
                _tmp.replace(_OI_PREV_PATH)
            except Exception as _we:
                print(f"[SCREENER_HUB] OI snapshot write error: {_we}")

    except Exception as e:
        print(f"[SCREENER_HUB] OI change tracking error: {e}")

    return detail


async def _fetch_tradier_oi_for_symbol(
    provider,
    symbol: str,
    *,
    timeout_per_call: float = 4.0,
) -> dict | None:
    """
    Fetch symbol-level OI summary from Tradier.

    Uses the 2 nearest expirations and sums openInterest + volume across
    all calls and puts.  Returns {total_oi, total_volume} or None on any
    failure.  Non-fatal — the caller handles None gracefully.

    Each Tradier call is individually timeout-guarded so a single slow
    expiration does not stall the whole request.
    """
    try:
        from data.tradier_budget import lane as _oi_lane
        with _oi_lane("saved_options"):   # OI enrichment is screener background data, not supplement
            expirations: list[str] = await asyncio.wait_for(
                provider.get_option_expirations(symbol),
                timeout=timeout_per_call,
            )
    except Exception:
        return None

    if not expirations:
        return None

    near_exps = expirations[:2]
    total_oi  = 0
    total_vol = 0

    for exp in near_exps:
        try:
            from data.tradier_budget import lane as _oi2_lane
            with _oi2_lane("saved_options"):   # OI chain fetch — screener background data
                chain: dict = await asyncio.wait_for(
                    provider.get_option_chain(symbol, exp),
                    timeout=timeout_per_call,
                )
        except Exception:
            continue
        for side in ("calls", "puts"):
            for c in (chain.get(side) or []):
                oi_val  = c.get("openInterest")
                vol_val = c.get("volume")
                try:
                    if oi_val  is not None: total_oi  += int(oi_val)
                except (TypeError, ValueError):
                    pass
                try:
                    if vol_val is not None: total_vol += int(vol_val)
                except (TypeError, ValueError):
                    pass

    return {"total_oi": total_oi, "total_volume": total_vol}


async def _bg_refresh_options_oi(
    stale: list[str],
    prev_cached_map: dict[str, dict],
) -> None:
    """
    Background task: fetch Tradier OI for stale symbols, persist to DB.

    Runs completely detached from the request path — never mutates response rows.
    Results are written to screener_options_oi_cache for subsequent requests.
    """
    api_key = os.getenv("TRADIER_API_KEY") or ""
    if not api_key:
        return
    try:
        from data.tradier_provider import TradierProvider
        provider = TradierProvider(api_key)
        sem = asyncio.Semaphore(_OPT_CONCURRENCY)
        timeout_per_call = max(5.0, _OPT_TIMEOUT / 2.0)
        now_ts = time.time()

        async def _throttled(sym: str):
            async with sem:
                return sym, await _fetch_tradier_oi_for_symbol(
                    provider, sym, timeout_per_call=timeout_per_call,
                )

        raw_results = await asyncio.gather(
            *[_throttled(s) for s in stale],
            return_exceptions=True,
        )

        to_upsert: list[dict] = []
        for item in raw_results:
            if isinstance(item, Exception):
                continue
            sym, fetched = item
            if fetched is None:
                continue

            new_oi  = fetched["total_oi"]
            new_vol = fetched["total_volume"]
            prev_oi = (prev_cached_map.get(sym) or {}).get("options_oi")

            oi_change: int | None = None
            oi_change_pct: float | None = None
            if prev_oi is not None and new_oi is not None:
                oi_change = new_oi - prev_oi
                if prev_oi > 0:
                    oi_change_pct = round(oi_change / prev_oi * 100, 4)

            activity_score: float | None = None
            if new_oi and new_oi > 0 and new_vol is not None:
                activity_score = round(min(10.0, (new_vol / new_oi) * 2), 4)

            to_upsert.append({
                "symbol":                sym,
                "options_oi":            new_oi,
                "previous_options_oi":   prev_oi,
                "options_oi_change":     oi_change,
                "options_oi_change_pct": oi_change_pct,
                "options_activity_score": activity_score,
                "total_volume":          new_vol,
                "provider":              "tradier",
            })

        if to_upsert:
            n = upsert_screener_options_oi(to_upsert)
            print(
                f"[OPT_ENRICH] bg: upserted {n}/{len(to_upsert)} rows "
                f"({len(stale)} stale requested)",
                flush=True,
            )
        else:
            print(
                f"[OPT_ENRICH] bg: 0/{len(stale)} symbols returned OI data",
                flush=True,
            )
    except Exception as _bge:
        print(f"[OPT_ENRICH] bg: error — {_bge}", flush=True)


async def _enrich_options_page_aware(rows: list[dict]) -> None:
    """
    Page-aware options enrichment for Screener Hub rows.

    Mutates each row dict in-place, adding/overriding:
        options_oi, previous_options_oi, options_oi_change,
        options_oi_change_pct, options_activity_score,
        options_updated_at, options_source

    Strategy — non-blocking, cache-first:
        1. Read all row symbols from the DB cache (screener_options_oi_cache).
        2. Merge cached entries into rows immediately — zero latency added.
        3. Identify stale/missing symbols.
        4. Fire a background asyncio Task to refresh those symbols from Tradier.
           The task writes results to DB; the NEXT request serves them as "cache".
        5. Symbols with no DB entry fall back to LKG data or "unavailable".

    options_source annotation:
        "cache"       — served from DB  (fresh within TTL)
        "lkg"         — no DB entry, fallback LKG JSON data
        "unavailable" — no data at all

    Non-fatal: if anything here fails the rows keep their LKG values.
    """
    if not _OPT_REFRESH_ENABLED:
        return
    if not rows:
        return

    symbols: list[str] = []
    sym_to_row: dict[str, dict] = {}
    for r in rows:
        sym = (r.get("symbol") or r.get("ticker") or "").upper()
        if sym and sym not in sym_to_row:
            sym_to_row[sym] = r
            symbols.append(sym)

    symbols = symbols[:_OPT_MAX_SYMS]
    if not symbols:
        return

    now_ts = time.time()
    ttl    = _OPT_TTL_OPEN if _is_market_open() else _OPT_TTL_CLOSED

    # ── 1. Load DB cache (fast, synchronous-style I/O) ────────────────────────
    cached_map: dict[str, dict] = {}
    try:
        cached_map = get_screener_options_oi(symbols) or {}
    except Exception as _ce:
        print(f"[OPT_ENRICH] DB cache read failed: {_ce}", flush=True)

    # ── 2. Identify stale/missing symbols ─────────────────────────────────────
    stale: list[str] = []
    for sym in symbols:
        entry = cached_map.get(sym)
        if entry is None:
            stale.append(sym)
        elif (now_ts - (entry.get("updated_at_ts") or 0)) > ttl:
            stale.append(sym)

    print(
        f"[OPT_ENRICH] {len(symbols)} syms — {len(cached_map)} cached, "
        f"{len(stale)} stale (ttl={ttl}s) — firing bg refresh",
        flush=True,
    )

    # ── 3. Fire background refresh for stale symbols (non-blocking) ───────────
    if stale and os.getenv("TRADIER_API_KEY"):
        asyncio.create_task(
            _bg_refresh_options_oi(stale[:_OPT_MAX_SYMS], dict(cached_map)),
        )

    # ── 4. Merge cached entries into rows immediately ─────────────────────────
    for sym in symbols:
        row = sym_to_row.get(sym)
        if row is None:
            continue

        db_entry = cached_map.get(sym)
        if db_entry is not None:
            # Serve from DB cache (fresh within TTL or recently stale — still useful)
            row["options_oi"]             = db_entry.get("options_oi")
            row["previous_options_oi"]    = db_entry.get("previous_options_oi")
            row["options_oi_change"]      = db_entry.get("options_oi_change")
            row["options_oi_change_pct"]  = db_entry.get("options_oi_change_pct")
            row["options_activity_score"] = db_entry.get("options_activity_score")
            row["options_updated_at"]     = db_entry.get("updated_at_iso")
            row["options_source"]         = "cache"
            # Compute pct on-the-fly if DB stored it as null but the inputs are
            # present (e.g. row written when prev_oi was 0 then updated, or when
            # oi_change=0 and an old schema wrote NULL instead of 0.0).
            _oi  = row.get("options_oi")
            _poi = row.get("previous_options_oi")
            if row.get("options_oi_change_pct") is None and _oi is not None:
                if _poi is not None and _poi > 0:
                    row["options_oi_change_pct"]    = round((_oi - _poi) / _poi * 100, 4)
                    row["options_oi_change_status"] = "computed"
                elif _poi is None:
                    row["options_oi_change_status"] = "no_prior_snapshot"
                else:
                    row["options_oi_change_status"] = "prior_zero"
        else:
            # Nothing in DB — use whatever LKG provided (may be None).
            # Compute options_oi_change_pct from the LKG absolute change when
            # previous_options_oi is known and > 0; otherwise emit a status flag
            # so the frontend can distinguish "no prior snapshot" from "pct is 0".
            lkg_oi  = row.get("options_oi")
            lkg_chg = row.get("options_oi_change")
            lkg_poi = row.get("previous_options_oi")
            row["options_updated_at"] = None
            row["options_source"]     = "lkg" if lkg_oi is not None else "unavailable"
            if lkg_oi is not None and lkg_poi is not None and lkg_poi > 0 and lkg_chg is not None:
                row["options_oi_change_pct"]    = round(lkg_chg / lkg_poi * 100, 4)
                row["options_oi_change_status"] = "computed_lkg"
            elif lkg_oi is not None and lkg_poi is None:
                row["options_oi_change_pct"]    = None
                row["options_oi_change_status"] = "no_prior_snapshot"
            elif lkg_poi is not None and lkg_poi == 0:
                row["options_oi_change_pct"]    = None
                row["options_oi_change_status"] = "prior_zero"
            else:
                if "options_oi_change_pct" not in row:
                    row["options_oi_change_pct"] = None


def _load_watchlist_set() -> set[str]:
    """All tickers from user watchlists and portfolio holdings."""
    syms: set[str] = set()
    try:
        from services.watchlist_service import list_watchlists, load_watchlist
        for wl in (list_watchlists() or [])[:10]:
            wl_id = wl.get("id") if isinstance(wl, dict) else None
            if not wl_id:
                continue
            store = load_watchlist(wl_id)
            if isinstance(store, dict):
                for t in (store.get("tickers") or []):
                    if isinstance(t, str):
                        syms.add(t.upper())
                    elif isinstance(t, dict) and t.get("symbol"):
                        syms.add(str(t["symbol"]).upper())
    except Exception as e:
        print(f"[SCREENER_HUB] _load_watchlist_set error: {e}")
    try:
        import json
        for p in Path(__file__).parent.parent.joinpath("data").glob("portfolio_holdings*.json"):
            try:
                data = json.loads(p.read_text())
                holdings = data.get("holdings", data) if isinstance(data, dict) else data
                if isinstance(holdings, list):
                    for h in holdings:
                        if isinstance(h, dict):
                            s = h.get("symbol") or h.get("ticker")
                            if s:
                                syms.add(str(s).upper())
            except Exception:
                continue
    except Exception as e:
        print(f"[SCREENER_HUB] _load_watchlist_set portfolio error: {e}")
    return syms


# ── Global screener-meta cache (shared across non-thematic tabs) ──────────────
# Lazily merges screener_meta_by_symbol from all stored thematic universe
# snapshots so Social / Bottlenecks / Watchlist+Portfolio rows get the same
# per-symbol enrichment (sector, industry, beta, volume, dollar_volume,
# last_annual_dividend, exchange …) that Thematic rows get from their own
# snapshot.  Pure DB read — no FMP calls. Refreshed every 30 min.

_GLOBAL_SCREENER_META_CACHE: tuple[dict, float] = ({}, 0.0)
_GLOBAL_SCREENER_META_TTL_S = 1800.0  # 30 minutes


def _load_global_screener_meta() -> dict[str, dict]:
    """
    Merged screener_meta_by_symbol from every stored thematic universe snapshot.

    Returns {symbol: scr_meta_dict} for any symbol that has ever appeared in a
    rebuilt thematic universe.  Used as screener_meta_by_symbol for Social,
    Bottlenecks, and Watchlist+Portfolio tabs so they get the same per-symbol
    enrichment (sector/industry/beta/volume/dollar_volume/dividend/exchange).

    Cache-only — no FMP calls, no network access.
    """
    global _GLOBAL_SCREENER_META_CACHE
    cached_data, cached_ts = _GLOBAL_SCREENER_META_CACHE
    if cached_data and (time.time() - cached_ts) < _GLOBAL_SCREENER_META_TTL_S:
        return cached_data
    try:
        merged = get_all_thematic_screener_meta()
    except Exception as e:
        print(f"[SCREENER_HUB] _load_global_screener_meta error: {e}")
        merged = cached_data or {}
    _GLOBAL_SCREENER_META_CACHE = (merged, time.time())
    if merged:
        print(f"[SCREENER_HUB] global screener meta refreshed: {len(merged)} symbols")
    return merged


def _compute_hidden_gem_score(
    *,
    distance_52w_high: Optional[float],
    volume_surge: Optional[float],
    accumulation: Optional[bool],
    chg_1d: Optional[float],
    return_2w: Optional[float],
    return_4w: Optional[float],
    return_10w: Optional[float],
    market_cap: Optional[float],
    is_social: bool,
    is_options: bool,
    is_watchlist: bool,
    theme_relevance_score: float = 0.5,
    source_confidence: str = "medium",
    liquidity_status: str = "unknown",
    options_oi: Optional[int] = None,
    dollar_volume: Optional[float] = None,
) -> float:
    """
    Score 0–10 rating how "hidden gem"-like a stock is.

    Spec priorities (Part 7):
      + theme-mapped industry relevance
      + $50M–$10B market cap range
      + volume above 100k
      + higher volume_to_market_cap (via liquidity tier)
      + higher dollar_volume (with diminishing returns)
      + Vol Surge + Accumulation
      + options OI / activity if available
      + social / options / watchlist confirmations

    Penalties: ETF-excluded, mega-cap, very low volume, liquidity very_thin.
    """
    score = 0.0

    # Theme relevance — core-industry names get a meaningful head-start
    if theme_relevance_score >= 0.85:        score += 1.5   # core industry
    elif theme_relevance_score >= 0.50:      score += 0.75  # adjacent
    elif theme_relevance_score >= 0.25:      score += 0.25  # weak adjacent w/ keyword boost
    # < 0.25 or unknown: no theme bonus

    # Source confidence — multi-source confirmed names rank higher
    if source_confidence == "high":          score += 0.75
    elif source_confidence == "medium":      score += 0.25
    # "low" → no bonus

    # RS / momentum signals — prefer real multi-week returns over 1D proxy
    rs_for_signal = return_4w if return_4w is not None else chg_1d
    if rs_for_signal is not None:
        if rs_for_signal > 10:
            score += 1.5
        elif rs_for_signal > 3:
            score += 1.0
        elif rs_for_signal > 0:
            score += 0.5
        elif rs_for_signal < -10:
            score -= 0.5

    # RS acceleration (shorter outperforming longer = momentum building)
    if return_2w is not None and return_4w is not None:
        accel = return_2w - return_4w
        if accel > 5:
            score += 1.5
        elif accel > 1:
            score += 0.75

    # Volume surge — smart money buying interest
    if volume_surge is not None:
        if volume_surge >= 3.0:
            score += 1.5
        elif volume_surge >= 2.0:
            score += 1.0
        elif volume_surge >= 1.5:
            score += 0.5

    # Accumulation: meaningful price + volume combo
    if accumulation:
        score += 0.75

    # Distance from 52w high:
    #   -30% to -5% = sweet spot (breakout setup, not broken stock)
    #   very extended (>-2%) = overcrowded / over-discovered
    #   free-fall (<-50%) = negative
    if distance_52w_high is not None:
        if -30 <= distance_52w_high <= -5:
            score += 1.5
        elif -5 < distance_52w_high <= 0:
            score += 0.25  # near ATH — valid but less "hidden"
        elif distance_52w_high < -50:
            score -= 0.5

    # Market cap: smaller = less discovered, but must justify quality
    if market_cap is not None:
        if market_cap < 1e9:       score += 1.5   # micro/small < $1B
        elif market_cap < 5e9:     score += 1.5   # small < $5B
        elif market_cap < 20e9:    score += 0.75  # mid < $20B
        elif market_cap > 200e9:   score -= 1.5   # mega > $200B — anchor penalty
        elif market_cap > 100e9:   score -= 0.75  # large-mega > $100B

    # Dollar volume — higher is better but don't let giant caps dominate
    if dollar_volume is not None:
        if dollar_volume >= 5_000_000:     score += 0.25  # solid liquidity
        elif dollar_volume < 500_000:      score -= 0.25  # very thin

    # Liquidity quality penalty (from volume/mcap ratio tier)
    if liquidity_status == "very_thin":  score -= 1.5
    elif liquidity_status == "thin":     score -= 0.5

    # Options open interest — real institutional interest signal
    if options_oi is not None:
        if options_oi >= 5_000:    score += 0.5
        elif options_oi >= 1_000:  score += 0.25

    # Multi-source confirmation boosts
    if is_social:    score += 0.5
    if is_options:   score += 0.5
    if is_watchlist: score += 0.25

    return round(max(0.0, min(10.0, score)), 2)


def _market_cap_bucket(market_cap: Optional[float]) -> str:
    """Return size bucket string for a market cap value."""
    if market_cap is None:
        return "unknown"
    if market_cap >= 200e9:
        return "mega"
    if market_cap >= 10e9:
        return "large"
    if market_cap >= 2e9:
        return "mid"
    if market_cap >= 300e6:
        return "small"
    return "micro"


def _assign_row_role(
    *,
    market_cap: Optional[float],
    hidden_gem_score: float,
    is_social: bool,
    is_options: bool,
    is_watchlist: bool,
    discovery_sources: list,
) -> str:
    """
    Assign a display role to each row.

    Roles (priority order):
      anchor               — mega-cap or well-known large-cap
      social_confirmed     — in X/Grok social screener consensus
      options_confirmed    — in options flow screener
      watchlist_overlap    — in user watchlists / portfolio
      supply_chain_player  — supply-chain / ETF sourced, not mega
      hidden_gem           — strong hidden-gem score + small/mid-cap
      emerging             — default
    """
    mcap = market_cap or 0
    if mcap > 100e9:
        return "anchor"
    if is_social:
        return "social_confirmed"
    if is_options:
        return "options_confirmed"
    if is_watchlist:
        return "watchlist_overlap"
    if hidden_gem_score >= 4.0 and mcap < 20e9:
        return "hidden_gem"
    # supply_chain_player: came from ETF holdings or FMP industry screener, mid/small-cap
    src_str = " ".join(str(s) for s in (discovery_sources or []))
    if ("etf:" in src_str or "fmp_screener:" in src_str or "sector_screener" in src_str) and mcap < 50e9:
        return "supply_chain_player"
    return "emerging"


# ── Universe builders ─────────────────────────────────────────────────────────

def _theme_keys() -> list[str]:
    try:
        from services.theme_rs_universe import THEME_RS_UNIVERSE
        return sorted(THEME_RS_UNIVERSE.keys())
    except Exception:
        return []


def _theme_metadata() -> dict:
    """All themes with display name, classification, and RS metadata.

    Returns a dict with keys:
      themes             — list of theme dicts (sorted)
      default_theme      — highest-RS theme with ≥15 snapshot rows
      default_theme_reason
      theme_rs_updated_at
      count
    """
    import json as _json
    from pathlib import Path as _Path

    _MIN_SNAP_ROWS = 15

    # ── 1. Base registry ──────────────────────────────────────────────────────
    out: list[dict] = []
    try:
        from services.theme_rs_universe import THEME_RS_UNIVERSE
        for key, meta in THEME_RS_UNIVERSE.items():
            label = meta.get("display_name") or key
            out.append({
                "theme_key":         key,
                "display_name":      label,
                "id":                key,
                "label":             label,
                "classification":    meta.get("classification") or "theme",
                "parent_sector":     meta.get("parent_sector"),
                "proxy_symbols":     list(meta.get("proxy_symbols") or [])[:5],
                "rs_score":          None,
                "momentum_rank":     None,
                "state":             None,
                "state_reason":      None,
                "stage":             None,
                "stage_label":       None,
                "return_pct":        None,
                "breadth_pct":       None,
                "trend_accel_20d":   None,
                "snapshot_row_count": None,
            })
    except Exception as e:
        print(f"[SCREENER_HUB] theme registry load error: {e}")
    out.sort(key=lambda r: (r.get("classification") or "", r.get("display_name") or ""))

    # ── 2. RS scores from themes_rs_lkg.json ─────────────────────────────────
    rs_by_id: dict = {}
    theme_rs_updated_at: Optional[str] = None
    try:
        lkg_path = _Path(__file__).parent.parent / "data" / "themes_rs_lkg.json"
        if lkg_path.exists():
            lkg_data = _json.loads(lkg_path.read_text())
            for row in lkg_data.get("rows", []):
                tid = row.get("theme_id")
                if tid:
                    rs_by_id[tid] = row
                    if theme_rs_updated_at is None and row.get("last_updated"):
                        theme_rs_updated_at = row["last_updated"]
    except Exception as e:
        print(f"[SCREENER_HUB] themes_rs_lkg load error: {e}")

    # ── 3. Snapshot sym-counts + fundamentals coverage from DB ────────────────
    # Single combined query returns per theme:
    #   total_syms       — snapshot symbol count (replaces old snap_counts)
    #   fund_covered     — symbols present in screener_fundamentals_cache
    #   small_mid_fund   — covered symbols whose market_cap is in the
    #                      $50M–$10B thematic display window; used to gate
    #                      default_theme so mega-cap-only themes aren't picked.
    _MIN_ELIGIBLE_PCT   = 20.0   # minimum small-mid-cap fund coverage % for default
    snap_counts:   dict[str, int]   = {}
    fund_coverage: dict[str, float] = {}   # theme_key → fund_covered / total_syms
    eligible_pct:  dict[str, float] = {}   # theme_key → small_mid_fund / total_syms
    try:
        from data.screener_hub_store import _get_conn, _put_conn
        _conn = _get_conn()
        _cur  = _conn.cursor()
        _cur.execute("""
            WITH latest_snaps AS (
                SELECT DISTINCT ON (theme_key)
                       theme_key, symbols_json
                FROM public.screener_universe_snapshots
                WHERE universe_type = 'thematic' AND theme_key IS NOT NULL
                  AND symbols_json IS NOT NULL
                ORDER BY theme_key, generated_at DESC
            ),
            snap_syms AS (
                SELECT ls.theme_key,
                       jsonb_array_elements_text(ls.symbols_json::jsonb) AS sym
                FROM latest_snaps ls
            ),
            coverage AS (
                SELECT ss.theme_key,
                       COUNT(*) AS total_syms,
                       COUNT(fc.symbol) AS fund_covered,
                       COUNT(
                           CASE WHEN fc.market_cap IS NOT NULL
                                 AND fc.market_cap BETWEEN 50000000 AND 10000000000
                               THEN 1 END
                       ) AS small_mid_fund
                FROM snap_syms ss
                LEFT JOIN public.screener_fundamentals_cache fc ON fc.symbol = ss.sym
                GROUP BY ss.theme_key
            )
            SELECT theme_key, total_syms, fund_covered, small_mid_fund
            FROM coverage
        """)
        for _row in _cur.fetchall():
            _tk, _total, _fund, _sm = _row
            snap_counts[_tk]   = int(_total) if _total else 0
            fund_coverage[_tk] = round(_fund  / _total * 100, 1) if _total else 0.0
            eligible_pct[_tk]  = round(_sm    / _total * 100, 1) if _total else 0.0
        _cur.close()
        _put_conn(_conn)
    except Exception as e:
        print(f"[SCREENER_HUB] snapshot coverage query error: {e}")

    # ── 4. Merge RS + coverage into theme entries ─────────────────────────────
    for t in out:
        key = t["theme_key"]
        rs_row = rs_by_id.get(key)
        if rs_row:
            t["rs_score"]        = rs_row.get("rs_score")
            t["momentum_rank"]   = rs_row.get("momentum_rank")
            t["state"]           = rs_row.get("state")
            t["state_reason"]    = rs_row.get("state_reason")
            t["stage"]           = rs_row.get("stage")
            t["stage_label"]     = rs_row.get("stage_label")
            t["return_pct"]      = rs_row.get("return_pct")
            t["breadth_pct"]     = rs_row.get("breadth_pct")
            t["trend_accel_20d"] = rs_row.get("trend_accel_20d")
        t["snapshot_row_count"]       = snap_counts.get(key)
        t["fund_coverage_pct"]        = fund_coverage.get(key)
        t["eligible_fund_coverage_pct"] = eligible_pct.get(key)
        # Warn if the theme's visible rows would mostly lack metadata.
        # eligible_fund_coverage_pct < _MIN_ELIGIBLE_PCT means almost none of
        # the snapshot symbols sit in the $50M–$10B display window with known
        # market_cap — rows will appear with blank sector/industry/beta/exchange.
        t["low_metadata_coverage"] = (
            (eligible_pct.get(key) or 0.0) < _MIN_ELIGIBLE_PCT
        )

    # ── 5. Pick default_theme ─────────────────────────────────────────────────
    # Gate 1: must have RS score + ≥ _MIN_SNAP_ROWS snapshot symbols
    # Gate 2: must have ≥ _MIN_ELIGIBLE_PCT of snapshot symbols with market_cap
    #         in the $50M–$10B thematic display window — prevents mega-cap-heavy
    #         themes (healthcare, energy, equal_weight_sp500, tech_mega_caps …)
    #         from being selected when their rows would show blank metadata.
    default_theme: Optional[str] = None
    default_theme_reason: str    = "fallback"
    ranked = [
        t for t in out
        if t.get("rs_score") is not None
        and (t.get("snapshot_row_count") or 0) >= _MIN_SNAP_ROWS
        and not t.get("low_metadata_coverage", True)
    ]
    if ranked:
        best = max(ranked, key=lambda r: (r["rs_score"] or 0))
        default_theme        = best["theme_key"]
        ep = eligible_pct.get(best["theme_key"], 0.0)
        default_theme_reason = (
            f"Highest RS score ({best['rs_score']}) "
            f"among themes with \u2265{_MIN_SNAP_ROWS} snapshot rows "
            f"and \u2265{_MIN_ELIGIBLE_PCT:.0f}% metadata coverage "
            f"(eligible_pct={ep:.1f}%)"
        )
    else:
        # Fallback: any theme with snap rows, ignoring coverage gate
        for t in out:
            if (t.get("snapshot_row_count") or 0) >= _MIN_SNAP_ROWS:
                default_theme        = t["theme_key"]
                default_theme_reason = "first available theme (no RS data or coverage gate unmet)"
                break

    return {
        "themes":               out,
        "default_theme":        default_theme,
        "default_theme_reason": default_theme_reason,
        "theme_rs_updated_at":  theme_rs_updated_at,
        "count":                len(out),
    }


def _load_industry_map_config() -> dict:
    """
    Load theme_fmp_industry_map.json.  Returns {} on any failure.
    Called at rebuild time only — not on page loads.
    """
    import json
    try:
        if _INDUSTRY_MAP_PATH.exists():
            return json.loads(_INDUSTRY_MAP_PATH.read_text())
    except Exception as e:
        print(f"[SCREENER_HUB] _load_industry_map_config error: {e}")
    return {}


# ── Quality / theme-relevance helpers ─────────────────────────────────────────

def _is_junk_security_name(company_name: str) -> bool:
    """Return True if company name matches junk security patterns (rights, warrants, SPACs)."""
    if not company_name:
        return False
    return bool(_JUNK_NAME_RE.search(company_name))


def _compute_theme_relevance_score(
    industry: str,
    company_name: str,
    theme_cfg: dict,
) -> tuple[float, str]:
    """
    Returns (theme_relevance_score 0.0–1.0, industry_tier).
    industry_tier: "core" | "adjacent" | "weak_adjacent" | "unknown"

    Base score by tier: core=1.0, adjacent=0.6, weak_adjacent=0.3, unknown=0.5.
    Positive keyword match boosts +0.1 (capped at 1.0).
    Negative keyword match penalises -0.2 (floored at 0.0).
    """
    core_inds     = set(theme_cfg.get("core_industries")          or [])
    adjacent_inds = set(theme_cfg.get("adjacent_industries")      or [])
    weak_inds     = set(theme_cfg.get("weak_adjacent_industries")  or [])
    pos_kws = [k.lower() for k in (theme_cfg.get("positive_keywords") or [])]
    neg_kws = [k.lower() for k in (theme_cfg.get("negative_keywords") or [])]

    if industry in core_inds:
        base_score, tier = 1.0, "core"
    elif industry in adjacent_inds:
        base_score, tier = 0.6, "adjacent"
    elif industry in weak_inds:
        base_score, tier = 0.3, "weak_adjacent"
    else:
        base_score, tier = 0.5, "unknown"   # in fmp_industries but untiered → medium

    name_lower = (company_name or "").lower()
    kw_delta = 0.0
    for kw in pos_kws:
        if kw in name_lower:
            kw_delta += 0.1
            break   # one positive boost only
    for kw in neg_kws:
        if kw in name_lower:
            kw_delta -= 0.2
            break   # one negative penalty only

    return round(max(0.0, min(1.0, base_score + kw_delta)), 3), tier


def _get_source_confidence(
    discovery_sources: list[str],
    theme_relevance_score: float,
) -> str:
    """
    Returns "high" | "medium" | "low" based on source diversity and theme relevance.

    High:   ETF/LKG + any overlap confirmation, OR multiple discovery types + relevant.
    Low:    Only screener source with weak relevance and no confirmations.
    Medium: Everything else.
    """
    src_str       = " ".join(discovery_sources)
    has_etf       = "etf:" in src_str
    has_lkg       = "lkg_leaders" in src_str
    # fmp_screener: and fmp_profile: are both screener-class discovery sources
    has_screener  = "fmp_screener:" in src_str or "fmp_profile:" in src_str
    has_social    = "social_overlap" in src_str
    has_options   = "options_overlap" in src_str
    has_watchlist = "watchlist_portfolio" in src_str
    has_peers     = "fmp_peers" in src_str

    n_confirmations = sum([has_social, has_options, has_watchlist])
    n_discovery     = sum([has_etf, has_lkg, has_screener, has_peers])

    if (has_etf or has_lkg) and n_confirmations >= 1:
        return "high"
    if n_discovery >= 2 and theme_relevance_score >= 0.5:
        return "high"
    if n_confirmations >= 2:
        return "high"
    if (has_screener and not has_etf and not has_lkg
            and n_confirmations == 0 and theme_relevance_score < 0.4):
        return "low"
    return "medium"


def score_theme_candidate(
    theme_cfg: dict,
    candidate_profile: dict,
    discovery_sources: list[str],
) -> dict:
    """
    Deterministic semantic scorer for a candidate against a theme.

    Inputs
    ------
    theme_cfg          : theme entry from theme_fmp_industry_map.json
    candidate_profile  : dict with any of:
                           company_name / name, sector, industry, description
    discovery_sources  : source tags (e.g. ["fmp_screener:Semiconductors"])

    Returns
    -------
    {
      semantic_score        : float 0.0–1.0,
      candidate_tier        : "core"|"verified_discovery"|"adjacent_discovery"
                              |"watch_candidate"|"excluded",
      matched_keywords      : list[str],
      membership_confidence : "high"|"medium"|"low",
      membership_reason     : str,
      rejected_reason       : str | None,
      industry_tier         : "core"|"adjacent"|"weak_adjacent"|"unknown",
    }
    """
    company_name = (
        candidate_profile.get("company_name")
        or candidate_profile.get("name") or ""
    )
    sector      = candidate_profile.get("sector")      or ""
    industry    = candidate_profile.get("industry")    or ""
    description = candidate_profile.get("description") or ""

    theme_type   = (theme_cfg.get("theme_type") or "pure_subtheme").lower()
    pos_kws      = [k.lower() for k in (
        theme_cfg.get("required_any_keywords")
        or theme_cfg.get("positive_keywords") or []
    )]
    weak_kws_set = {k.lower() for k in (theme_cfg.get("weak_keywords") or [])}
    excl_kws     = [k.lower() for k in (
        theme_cfg.get("exclude_keywords")
        or theme_cfg.get("negative_keywords") or []
    )]

    # ── Exclude-keyword gate (fast path) ──────────────────────────────────
    proof_str = " ".join(filter(None, [
        company_name.lower(), sector.lower(), industry.lower()
    ]))
    for ekw in excl_kws:
        if ekw in proof_str:
            return {
                "semantic_score":        0.0,
                "candidate_tier":        "excluded",
                "matched_keywords":      [],
                "membership_confidence": "low",
                "membership_reason":     f"exclude_keyword: {ekw}",
                "rejected_reason":       f"exclude_keyword: {ekw}",
                "industry_tier":         "unknown",
            }

    # ── Industry tier + base score ─────────────────────────────────────────
    base_score, industry_tier = _compute_theme_relevance_score(
        industry, company_name, theme_cfg
    )

    # ── Keyword matching: name/sector/industry then description ────────────
    name_ind_matched = [kw for kw in pos_kws if kw in proof_str]
    desc_matched: list[str] = []
    if description:
        desc_lower = description.lower()
        for kw in pos_kws:
            if kw not in name_ind_matched and kw in desc_lower:
                desc_matched.append(kw)

    all_matched    = list(dict.fromkeys(name_ind_matched + desc_matched))
    strong_matched = [kw for kw in all_matched if kw not in weak_kws_set]
    weak_only      = bool(all_matched) and not strong_matched

    # ── Semantic score ─────────────────────────────────────────────────────
    kw_delta = 0.0
    if strong_matched:
        kw_delta += 0.20
        if name_ind_matched and desc_matched:
            kw_delta += 0.05   # both name and description confirm
    elif all_matched:
        kw_delta += 0.03       # weak-only minor lift
    semantic_score = round(min(1.0, max(0.0, base_score + kw_delta)), 3)

    # ── Seed / manual short-circuit ────────────────────────────────────────
    src_str = " ".join(discovery_sources)
    if "static_seed" in src_str or "manual_include" in src_str:
        return {
            "semantic_score":        semantic_score,
            "candidate_tier":        "core",
            "matched_keywords":      all_matched,
            "membership_confidence": "high",
            "membership_reason":     "seed ticker",
            "rejected_reason":       None,
            "industry_tier":         industry_tier,
        }

    # ── Keyword-requirement gate ───────────────────────────────────────────
    require_kw = (
        bool(theme_cfg.get("require_name_keyword_match"))
        or (theme_type != "parent_rollup" and bool(pos_kws))
    )

    if require_kw and not all_matched:
        # No keyword evidence — include as watch_candidate only when
        # in a core or adjacent industry (industry bucket = partial proof).
        if industry_tier in ("core", "adjacent") and semantic_score >= 0.45:
            return {
                "semantic_score":        semantic_score,
                "candidate_tier":        "watch_candidate",
                "matched_keywords":      [],
                "membership_confidence": "low",
                "membership_reason":     (
                    f"industry-only proof ({industry}); no keyword match"
                ),
                "rejected_reason":       None,
                "industry_tier":         industry_tier,
            }
        return {
            "semantic_score":        semantic_score,
            "candidate_tier":        "excluded",
            "matched_keywords":      [],
            "membership_confidence": "low",
            "membership_reason":     "no keyword match",
            "rejected_reason":       "no_keyword_match",
            "industry_tier":         industry_tier,
        }

    # ── Classify by strength + industry tier ──────────────────────────────
    is_adjacent = industry_tier in ("adjacent", "weak_adjacent")

    if is_adjacent and not weak_only and all_matched:
        tier       = "adjacent_discovery"
        confidence = "medium" if strong_matched else "low"
        reason     = (
            f"adjacent industry ({industry_tier}); "
            f"keyword: '{all_matched[0]}'"
        )
    elif weak_only or semantic_score < 0.45:
        tier       = "watch_candidate"
        confidence = "low"
        reason     = (
            f"weak keyword only: '{all_matched[0]}'" if all_matched
            else f"industry match ({industry_tier})"
        )
    elif strong_matched:
        tier       = "verified_discovery"
        confidence = "high" if semantic_score >= 0.75 else "medium"
        reason     = (
            f"keyword: '{strong_matched[0]}' in "
            f"{'name/sector/industry' if name_ind_matched else 'description'}"
        )
    else:
        tier       = "watch_candidate"
        confidence = "low"
        reason     = (
            f"keyword: '{all_matched[0]}'" if all_matched
            else f"industry match ({industry_tier})"
        )

    return {
        "semantic_score":        semantic_score,
        "candidate_tier":        tier,
        "matched_keywords":      all_matched,
        "membership_confidence": confidence,
        "membership_reason":     reason,
        "rejected_reason":       None,
        "industry_tier":         industry_tier,
    }


def _compute_liquidity_status(
    volume: Optional[float],
    market_cap: Optional[float],
    price: Optional[float],
) -> tuple[str, Optional[float], Optional[float]]:
    """
    Returns (liquidity_status, dollar_volume, volume_to_market_cap).

    liquidity_status: "adequate" | "thin" | "very_thin" | "unknown"
    dollar_volume:          price × volume  (None if either is missing)
    volume_to_market_cap:   volume / market_cap  (None if either is missing)
    """
    dollar_volume: Optional[float] = None
    volume_to_market_cap: Optional[float] = None

    if volume is not None and price is not None:
        dollar_volume = round(volume * price, 2)
    if volume is not None and market_cap and market_cap > 0:
        volume_to_market_cap = round(volume / market_cap, 6)

    if volume is None:
        return "unknown", dollar_volume, volume_to_market_cap
    if volume >= 500_000:
        return "adequate", dollar_volume, volume_to_market_cap
    if volume >= 100_000:
        return "thin", dollar_volume, volume_to_market_cap
    return "very_thin", dollar_volume, volume_to_market_cap


async def _fmp_industry_screener(
    industries: list[str],
    *,
    market_cap_upper: Optional[int] = 10_000_000_000,
    market_cap_lower: Optional[int] = 50_000_000,
    min_volume: int = 100_000,
    screener_limit: int = 500,
    timeout: float = 12.0,
) -> tuple[list[dict], list[str], list[str], dict]:
    """
    FMP company-screener — ONE call per mapped industry.

    ONLY called from rebuild_universe / admin rebuild paths (with_fmp_screener=True).
    Never called on page loads.

    Quality filters enforced client-side (FMP often ignores query params):
      - market_cap >= market_cap_lower (default $50M)
      - market_cap <= market_cap_upper (default $10B)
      - volume >= min_volume (default 100k)
      - not ETF / not fund
      - symbol length ≤ 6, alphanumeric + hyphen only
      - not junk security name (rights, warrants, units, SPACs)

    Returns:
        candidates           : list of candidate metadata dicts
        industries_attempted : list of industries whose API call was made
        industries_errored   : list of industries that failed
        filtered_reasons     : dict of {reason: count} showing exclusion breakdown
    """
    api_key = os.getenv("FMP_API_KEY") or ""
    if not api_key or not industries:
        return [], [], [], {}

    try:
        from services.fmp_governor import fmp_governor as _gov
    except Exception:
        _gov = None

    candidates:           list[dict]      = []
    seen_syms:            set[str]        = set()
    industries_attempted: list[str]       = []
    industries_errored:   list[str]       = []
    filtered_reasons: dict[str, int] = {
        "total_raw":       0,
        "total_accepted":  0,
        "is_etf":          0,
        "is_fund":         0,
        "symbol_format":   0,
        "sub_min_cap":     0,
        "above_max_cap":   0,
        "low_volume":      0,
        "junk_name":       0,
        "duplicate":       0,
    }

    async def _screen_one(industry: str) -> tuple[str, list[dict], bool, dict]:
        """Returns (industry, candidate_dicts, had_error, local_filtered)."""
        local_fr: dict[str, int] = {k: 0 for k in filtered_reasons}
        if _gov is not None:
            slot_ok = await _gov.acquire(job_name="fmp_industry_screener")
            if not slot_ok:
                return industry, [], True, local_fr
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                # Build params conditionally: omit marketCapLowerThan when
                # market_cap_upper is None (no upper bound, allows mega-caps).
                _fmp_screen_params: dict = {
                    "industry":               industry,
                    "isEtf":                  "false",
                    "isFund":                 "false",
                    "isActivelyTrading":      "true",
                    "marketCapMoreThan":       str(market_cap_lower or 50_000_000),
                    "volumeMoreThan":          str(min_volume),
                    "includeAllShareClasses":  "false",
                    "limit":                  screener_limit,
                    "apikey":                 api_key,
                }
                if market_cap_upper is not None:
                    _fmp_screen_params["marketCapLowerThan"] = str(market_cap_upper)
                resp = await client.get(
                    f"{FMP_BASE}/company-screener",
                    params=_fmp_screen_params,
                )
            if _gov is not None:
                _gov.record_call()
            if resp.status_code != 200:
                print(f"[SCREENER_HUB] company-screener {industry!r} HTTP {resp.status_code}")
                return industry, [], True, local_fr
            raw = resp.json()
            hits: list[dict] = []
            for item in (raw if isinstance(raw, list) else []):
                local_fr["total_raw"] += 1
                sym = (item.get("symbol") or "").strip().upper()
                if not sym or len(sym) > 6:
                    local_fr["symbol_format"] += 1
                    continue
                if not sym.replace("-", "").isalpha():
                    local_fr["symbol_format"] += 1
                    continue
                if item.get("isEtf"):
                    local_fr["is_etf"] += 1
                    continue
                if item.get("isFund"):
                    local_fr["is_fund"] += 1
                    continue
                mcap = float(item.get("marketCap") or 0)
                vol  = float(item.get("volume") or 0)
                # Client-side quality filters (FMP may ignore query params)
                if market_cap_lower and mcap and mcap < market_cap_lower:
                    local_fr["sub_min_cap"] += 1
                    continue
                if market_cap_upper and mcap and mcap > market_cap_upper:
                    local_fr["above_max_cap"] += 1
                    continue
                if vol < min_volume:
                    local_fr["low_volume"] += 1
                    continue
                company_name = (item.get("companyName") or "").strip()
                if _is_junk_security_name(company_name):
                    local_fr["junk_name"] += 1
                    continue
                # Accepted — build enriched metadata dict
                beta_raw  = item.get("beta")
                price_raw = item.get("price")
                price_f   = float(price_raw) if price_raw is not None else None
                vol_f     = vol if vol else None
                mcap_f    = mcap if mcap else None
                liq_status, dv, vtmc = _compute_liquidity_status(vol_f, mcap_f, price_f)
                local_fr["total_accepted"] += 1
                div_raw = item.get("lastAnnualDividend")
                hits.append({
                    "symbol":               sym,
                    "company_name":         company_name,
                    "market_cap":           mcap_f,
                    "sector":               (item.get("sector") or "").strip(),
                    "industry":             (item.get("industry") or industry).strip(),
                    "beta":                 float(beta_raw) if beta_raw is not None else None,
                    "price":                price_f,
                    "last_annual_dividend": float(div_raw) if div_raw is not None else None,
                    "volume":               vol_f,
                    "exchange":             (item.get("exchangeShortName") or item.get("exchange") or "").strip(),
                    "country":              (item.get("country") or "").strip(),
                    "is_etf":               bool(item.get("isEtf")),
                    "is_fund":              bool(item.get("isFund")),
                    "liquidity_status":     liq_status,
                    "dollar_volume":        dv,
                    "volume_to_market_cap": vtmc,
                    "discovery_source":     f"fmp_screener:{industry}",
                    # theme_relevance_score and industry_tier computed later in _build_thematic_universe
                })
            return industry, hits, False, local_fr
        except Exception as e:
            print(f"[SCREENER_HUB] company-screener {industry!r} error: {e}")
            return industry, [], True, local_fr

    results = await asyncio.gather(
        *[_screen_one(ind) for ind in industries],
        return_exceptions=True,
    )
    for res in results:
        if isinstance(res, Exception):
            industries_errored.append("unknown")
            continue
        industry, hits, had_error, local_fr = res
        industries_attempted.append(industry)
        for k, v in local_fr.items():
            filtered_reasons[k] = filtered_reasons.get(k, 0) + v
        if had_error:
            industries_errored.append(industry)
            continue
        for item in hits:
            sym = item["symbol"]
            if sym not in seen_syms:
                seen_syms.add(sym)
                candidates.append(item)
            else:
                filtered_reasons["duplicate"] += 1

    # Sort smallest-cap first so they fill the per-theme cap ahead of larger names.
    # (Re-sorted by theme_relevance in _build_thematic_universe after scoring.)
    candidates.sort(key=lambda c: c.get("market_cap") or float("inf"))

    print(
        f"[SCREENER_HUB] fmp_industry_screener: {len(industries)} industries → "
        f"{filtered_reasons['total_raw']} raw → {len(candidates)} accepted "
        f"(sub_cap={filtered_reasons['sub_min_cap']}, "
        f"low_vol={filtered_reasons['low_volume']}, "
        f"junk={filtered_reasons['junk_name']}, "
        f"etf/fund={filtered_reasons['is_etf'] + filtered_reasons['is_fund']}, "
        f"errors={industries_errored or 'none'})"
    )
    return candidates, industries_attempted, industries_errored, filtered_reasons


async def _build_thematic_universe(
    theme_key: Optional[str],
    *,
    with_fmp_peers: bool = True,
    with_fmp_screener: bool = False,
) -> tuple[dict[str, list[str]], dict[str, dict]]:
    """
    Build per-theme stock universes using a dynamic-first, static-fallback strategy.

    Source priority per theme (spec order)
    ───────────────────────────────────────
    A. ETF holdings          — disk read from data/etf_holdings/{ETF}.json (fast)
    B. LKG leaders           — leaders + laggards from themes_rs_lkg.json
    C. FMP screener          — company-screener by mapped industry (rebuild only)
    D. FMP peers             — stable/stock-peers (thin themes only, budget-guarded)
    F. candidate_symbols     — static seed list (fallback; used_static_fallback=True)
    G. proxy_symbols         — ETF tickers, absolute last resort only

    Source C (FMP screener) candidates are sorted smallest-cap first and placed
    before static seeds so hidden-gem names get slots within _PER_THEME_CAP.
    _SCREENER_RESERVE ensures screener symbols are included even when ETF holdings
    fill most of the cap.

    Parameters
    ──────────
    theme_key            : single theme key, or None to build all themes.
    with_fmp_peers       : False for bulk/page-load builds (saves API calls).
    with_fmp_screener    : True only for scheduled/admin rebuild jobs. Calls
                           FMP company-screener to find small/mid-cap names.

    Returns
    ───────
    symbols_map   : {theme_key: [symbol, ...]}
    breakdown_map : {theme_key: breakdown_dict}
    """
    keys = [theme_key] if theme_key else _theme_keys()
    symbols_map:   dict[str, list[str]] = {}
    breakdown_map: dict[str, dict]      = {}

    lkg_map = _load_lkg_leaders_map()

    # Load config-driven industry map for ALL builds — seed_tickers and
    # exclude_tickers must apply on every page load, not just scheduled rebuilds.
    industry_map_cfg = _load_industry_map_config()
    industry_map_version = industry_map_cfg.get("industry_map_version", "none")
    themes_cfg = industry_map_cfg.get("themes") or {}

    try:
        from services.theme_rs_universe import THEME_RS_UNIVERSE
    except Exception:
        THEME_RS_UNIVERSE = {}

    for key in keys:
        meta = (THEME_RS_UNIVERSE or {}).get(key) or {}
        _proxy_type = meta.get("proxy_type") or "etf"
        # proxy_symbols are ETF tickers only when proxy_type="etf".
        # For proxy_type="custom" the symbols are stock tickers (e.g. ANET, AVGO)
        # and must NOT be used as ETF filenames for disk holdings lookup.
        # They are merged into candidate_syms instead, flowing through Source F.
        if _proxy_type == "etf":
            proxy_etfs: list[str] = [s.upper() for s in (meta.get("proxy_symbols") or []) if s]
        else:
            proxy_etfs = []
        candidate_syms: list[str] = [s.upper() for s in (meta.get("candidate_symbols") or []) if s]
        # For custom-proxy themes, also pull in proxy_symbols as candidate anchors
        # (proxy_symbols and candidate_symbols may overlap; dedup in Source F loop).
        if _proxy_type != "etf":
            for _ps in (meta.get("proxy_symbols") or []):
                _psu = _ps.upper() if _ps else ""
                if _psu and _psu not in candidate_syms:
                    candidate_syms.append(_psu)

        etf_holdings_syms:    list[str]  = []   # Source A
        lkg_syms:             list[str]  = []   # Source B
        screener_syms:        list[str]  = []   # Source C (FMP industry screener)
        fmp_peer_syms:        list[str]  = []   # Source D (FMP peers, thin only)
        static_syms:          list[str]  = []   # Source F (static candidate seed)
        etf_files_found:      list[str]  = []
        fmp_peer_anchors:     list[str]  = []
        # screener_meta_by_symbol: rich metadata from FMP screener per symbol
        screener_meta_by_symbol: dict[str, dict] = {}
        sources_by_symbol:    dict[str, list[str]] = {}
        sources_attempted:    list[str]  = ["etf_holdings", "lkg_leaders", "static_seed"]
        sources_failed:       list[str]  = []

        # Screener tracking
        fmp_screener_industries_attempted: list[str] = []
        fmp_screener_industries_errored:   list[str] = []
        fmp_screener_calls_used:           int       = 0
        fmp_profile_discovery_count:       int       = 0   # Source P: net-new from Neon profile cache
        fmp_profile_upgrade_count:         int       = 0   # Source P: watch→verified upgrades

        seen_dynamic: set[str] = set()

        # Load theme config early so seed/exclude/keyword gates are available
        # for ALL sources (A–F), not just Source C (FMP screener).
        theme_cfg  = themes_cfg.get(key) or {}

        # ── Theme membership overrides (from theme_fmp_industry_map.json) ─────
        # seed_tickers:             always included, front-of-list priority
        # exclude_tickers:          blocked from every source
        # require_name_keyword_match: Source C (FMP screener) must match a
        #                            positive keyword in the company name
        _seed_tickers: list[str] = [
            s.upper() for s in (theme_cfg.get("seed_tickers") or []) if s
        ]
        _exclude_set: set[str] = {
            s.upper() for s in (theme_cfg.get("exclude_tickers") or []) if s
        }
        _theme_type: str = (theme_cfg.get("theme_type") or "pure_subtheme").lower()
        # required_any_keywords: checked against company name + FMP sector + FMP industry
        # combined.  Falls back to legacy positive_keywords for backward compat.
        _pos_kws_lower: list[str] = [
            k.lower() for k in (
                theme_cfg.get("required_any_keywords")
                or theme_cfg.get("positive_keywords")
                or []
            )
        ]
        # weak_keywords: a named subset of required_any_keywords that are too broad
        # to serve as the SOLE proof for FMP screener candidates in pure_subtheme
        # themes.  Terms like "optical", "server", "automation", or "security" can
        # match many unrelated companies.  When a candidate only matches weak keywords
        # (no strong keyword — i.e. no keyword that is NOT in weak_keywords), the FMP
        # gate rejects it.  Seeds, ETF holdings, and LKG leaders bypass this entirely.
        _weak_kws_set: set[str] = {
            k.lower() for k in (theme_cfg.get("weak_keywords") or [])
        }
        # Semantic proof gate active for all non-parent_rollup themes that define
        # required_any_keywords.  Legacy require_name_keyword_match also activates it.
        _require_kwmatch: bool = (
            bool(theme_cfg.get("require_name_keyword_match"))
            or (_theme_type != "parent_rollup" and bool(_pos_kws_lower))
        )
        # exclude_keywords: if ANY appear in name+sector+industry, reject FMP candidate.
        # Blocks cross-contamination (e.g. defense companies in power/cooling themes).
        _excl_kws_lower: list[str] = [
            k.lower() for k in (
                theme_cfg.get("exclude_keywords")
                or theme_cfg.get("negative_keywords")
                or []
            )
        ]
        # manual_exclude: explicit per-theme symbol block list (applies to all sources
        # in addition to exclude_tickers; useful for naming false-positive exceptions).
        _manual_exclude_syms: set[str] = {
            s.upper() for s in (theme_cfg.get("manual_exclude") or []) if s
        }
        if _manual_exclude_syms:
            _exclude_set = _exclude_set | _manual_exclude_syms

        # ── Source E: manual_include — always-in, highest-priority tickers ────
        # These bypass ALL keyword/exclude gates and are pre-tagged in
        # sources_by_symbol as "manual_include" so the priority scan in the row
        # loop can assign them membership_source="manual_include" (rank #1).
        # They are also prepended into _seed_tickers so they enter combined[]
        # unconditionally, and they are removed from _exclude_set so an
        # accidental overlap never silences them.
        _manual_include: list[str] = [
            s.upper() for s in (theme_cfg.get("manual_include") or []) if s
        ]
        if _manual_include:
            _exclude_set -= set(_manual_include)
            for _mi in _manual_include:
                sources_by_symbol[_mi] = ["manual_include"]
            _seed_tickers = list(dict.fromkeys(list(_manual_include) + list(_seed_tickers)))

        # ── Source A: ETF holdings (disk read, fast) ──────────────────────────
        for etf in proxy_etfs:
            holdings = _read_etf_holdings_from_disk(etf)
            if holdings:
                etf_files_found.append(etf)
            for sym in holdings:
                if sym in _exclude_set:
                    continue
                if sym not in _ALL_PROXY_ETFS and sym not in seen_dynamic:
                    seen_dynamic.add(sym)
                    etf_holdings_syms.append(sym)
                    sources_by_symbol.setdefault(sym, []).append(f"etf:{etf}")

        if not etf_files_found and proxy_etfs:
            sources_failed.append("etf_holdings")

        # ── Source A2: supplemental ETF proxy from JSON config (proxy_etfs field) ──
        # Thin pure_subtheme themes can declare specific themed ETFs in
        # theme_fmp_industry_map.json under "proxy_etfs": ["SRVR", "ARKX", ...].
        # These are separate from the RS-universe proxy_symbols (used for RS charting).
        # Holdings are read from disk cache; first access triggers background fetch.
        for _cfg_etf in (theme_cfg.get("proxy_etfs") or []):
            _cfg_etf_u = _cfg_etf.upper() if isinstance(_cfg_etf, str) else ""
            if not _cfg_etf_u:
                continue
            if _cfg_etf_u in _ALL_PROXY_ETFS:
                continue  # already in global proxy set — skip
            if _cfg_etf_u in proxy_etfs:
                continue  # already handled in Source A above
            _cfg_holdings = _read_etf_holdings_from_disk(_cfg_etf_u)
            if _cfg_holdings:
                if _cfg_etf_u not in etf_files_found:
                    etf_files_found.append(_cfg_etf_u)
            for sym in _cfg_holdings:
                if sym in _exclude_set:
                    continue
                if sym not in _ALL_PROXY_ETFS and sym not in seen_dynamic:
                    seen_dynamic.add(sym)
                    etf_holdings_syms.append(sym)
                    sources_by_symbol.setdefault(sym, []).append(f"etf:{_cfg_etf_u}")

        # ── Source B: LKG leaders / laggards ──────────────────────────────────
        # Design rule (pure_subtheme themes):
        #   LKG leaders are RANKING / DISCOVERY signals only.  They are recorded in
        #   sources_by_symbol so they surface as enrichment context and ranking boost,
        #   but they do NOT independently qualify a ticker for universe membership.
        #   Membership requires one of: seed, manual_include, theme-ETF holding, or
        #   keyword/profile-proved FMP candidate.
        # Non-pure_subtheme themes (parent_rollup, curated_seed_core) may allow LKG
        #   to contribute new candidates to the universe pool.
        for sym in lkg_map.get(key) or []:
            su = sym.upper() if isinstance(sym, str) else ""
            if su in _exclude_set:
                continue
            if not su or su in _ALL_PROXY_ETFS or su in seen_dynamic:
                continue
            # Always record the LKG signal for ranking / enrichment context
            sources_by_symbol.setdefault(su, []).append("lkg_leaders")
            # pure_subtheme: signal recorded above; do NOT grant universe membership
            if _theme_type == "pure_subtheme":
                continue
            # Non-pure_subtheme: LKG may introduce new universe candidates
            seen_dynamic.add(su)
            lkg_syms.append(su)

        # ── Source C: FMP industry screener (rebuild jobs only) ───────────────
        # Uses config-driven theme → industry mapping from theme_fmp_industry_map.json.
        # Quality filters: min_market_cap=$50M, min_volume=100k, no junk names/ETFs.
        # Candidates sorted by (theme_relevance DESC, market_cap ASC) after scoring.
        # They get RESERVED slots in the final cap so ETF overflow doesn't crowd them out.
        # theme_cfg already loaded early (before Source A) so seeds/excludes apply everywhere.
        industries = theme_cfg.get("fmp_industries") or []
        # v2 schema fields with legacy compat fallbacks
        # Respect explicit null in config (null → no upper cap, allows mega-caps).
        # Themes like ai_networking set max_market_cap=null so ANET/AVGO/MRVL
        # are not excluded by the FMP screener's default $10B ceiling.
        if "max_market_cap" in theme_cfg and theme_cfg["max_market_cap"] is None:
            max_mcap = None  # no upper cap
        else:
            max_mcap = int(
                theme_cfg.get("max_market_cap")
                or theme_cfg.get("market_cap_upper_default")
                or 10_000_000_000
            )
        min_mcap = int(
            theme_cfg.get("min_market_cap")
            or theme_cfg.get("market_cap_lower_default")
            or 50_000_000
        )
        min_vol = int(theme_cfg.get("min_volume") or 100_000)
        filtered_reasons_breakdown: dict = {}
        _weak_kw_downgrade_count:   int  = 0  # FMP cands included at low-confidence (weak-only proof)

        if with_fmp_screener and industries:
            sources_attempted.append("fmp_screener")
            try:
                cands, ind_attempted, ind_errored, filtered_reasons_breakdown = (
                    await _fmp_industry_screener(
                        industries,
                        market_cap_upper=max_mcap,
                        market_cap_lower=min_mcap,
                        min_volume=min_vol,
                    )
                )
                fmp_screener_industries_attempted = ind_attempted
                fmp_screener_industries_errored   = ind_errored
                fmp_screener_calls_used           = len(ind_attempted)

                if ind_errored:
                    sources_failed.append(f"fmp_screener_partial({','.join(ind_errored[:3])})")

                # Compute theme_relevance_score per candidate, then re-sort:
                # highest theme relevance first, smallest-cap within each tier.
                for cand in cands:
                    trs, tier = _compute_theme_relevance_score(
                        cand.get("industry") or "",
                        cand.get("company_name") or "",
                        theme_cfg,
                    )
                    cand["theme_relevance_score"] = trs
                    cand["industry_tier"]         = tier
                    qflags: list[str] = []
                    beta = cand.get("beta")
                    if beta is not None and abs(beta) > 5:
                        qflags.append("very_high_beta")
                    cand["quality_flags"] = qflags

                cands.sort(
                    key=lambda c: (-(c.get("theme_relevance_score") or 0),
                                    c.get("market_cap") or float("inf")),
                )

                for cand in cands:
                    sym = cand["symbol"]
                    if sym in _exclude_set:
                        continue
                    # score_theme_candidate: semantic gate + tier assignment.
                    # Replaces the inline keyword/weak/exclude-gate logic with the
                    # reusable scorer.  Key behaviour change: companies in core
                    # industries without a keyword match in their name/sector/industry
                    # are now included as watch_candidate (low-confidence) rather than
                    # hard-dropped, so the screener surfaces them for analyst review.
                    # Explicit exclude_keywords and manual_exclude still reject.
                    _sc_ind_tag = f"fmp_screener:{cand.get('industry') or 'unknown'}"
                    _sc_result  = score_theme_candidate(theme_cfg, cand, [_sc_ind_tag])
                    if _sc_result["candidate_tier"] == "excluded":
                        continue
                    # Store all scorer outputs in cand for row-build lookup
                    cand["_kw_proof"]               = (_sc_result["matched_keywords"] or [""])[0]
                    cand["_all_matched_kws"]        = _sc_result["matched_keywords"]
                    cand["_weak_only"]              = (
                        _sc_result["candidate_tier"] == "watch_candidate"
                        and _sc_result["membership_confidence"] == "low"
                    )
                    cand["candidate_tier_override"] = _sc_result["candidate_tier"]
                    cand["membership_confidence_override"] = _sc_result["membership_confidence"]
                    cand["membership_reason"] = _sc_result.get("membership_reason") or ""
                    if cand["_weak_only"]:
                        _weak_kw_downgrade_count += 1
                    if sym not in _ALL_PROXY_ETFS and sym not in seen_dynamic:
                        seen_dynamic.add(sym)
                        screener_syms.append(sym)
                        screener_meta_by_symbol[sym] = cand
                        sources_by_symbol.setdefault(sym, []).append(_sc_ind_tag)
            except Exception as se:
                print(f"[SCREENER_HUB] fmp_screener {key} error: {se}")
                sources_failed.append("fmp_screener")
        elif with_fmp_screener and not industries:
            print(f"[SCREENER_HUB] fmp_screener: no industry mapping for theme={key!r}")

        # ── Source C2: FMP adjacent-industry screener (rebuild jobs only) ───────
        # Queries adjacent_industries from theme config with the same keyword gate
        # as Source C. Adjacent-industry candidates receive _is_adjacent=True in their
        # screener_meta, which causes:
        #   candidate_tier = "watch_candidate" for weak proof
        #   candidate_tier = "verified_discovery" for strong proof (non-weak keyword match)
        #   theme_role     = "adjacent"
        #   membership_reason prefix = "adjacent FMP:<industry> | ..."
        # Only active for pure_subtheme themes that define required_any_keywords.
        _adj_industries: list[str] = (theme_cfg.get("adjacent_industries") or [])
        if (with_fmp_screener
                and _adj_industries
                and _require_kwmatch
                and _pos_kws_lower
                and _theme_type == "pure_subtheme"):
            try:
                adj_cands, _adj_attempted, _adj_errored, _ = await _fmp_industry_screener(
                    _adj_industries,
                    market_cap_upper=max_mcap,
                    market_cap_lower=min_mcap,
                    min_volume=min_vol,
                )
                fmp_screener_industries_attempted += _adj_attempted
                fmp_screener_calls_used += len(_adj_attempted)
                _adj_added = 0
                for cand in adj_cands:
                    sym = cand["symbol"]
                    if sym in _exclude_set:
                        continue
                    # score_theme_candidate with adjacent-source context.
                    # Adjacent industries are more prone to FMP category soup, so the
                    # gate here is STRICT: no keyword proof → hard reject.
                    # Only watch_candidate rows with actual keyword matches are allowed.
                    _adj_ind   = cand.get("industry") or _adj_industries[0]
                    _adj_tag   = f"fmp_screener:{_adj_ind}"
                    _adj_sc    = score_theme_candidate(theme_cfg, cand, [_adj_tag])
                    if _adj_sc["candidate_tier"] == "excluded":
                        continue
                    if not _adj_sc["matched_keywords"]:
                        continue  # adjacent: industry-only proof insufficient
                    cand["_kw_proof"]               = (_adj_sc["matched_keywords"] or [""])[0]
                    cand["_all_matched_kws"]        = _adj_sc["matched_keywords"]
                    cand["_weak_only"]              = (
                        _adj_sc["candidate_tier"] == "watch_candidate"
                        and _adj_sc["membership_confidence"] == "low"
                    )
                    cand["candidate_tier_override"] = _adj_sc["candidate_tier"]
                    cand["membership_confidence_override"] = _adj_sc["membership_confidence"]
                    cand["membership_reason"] = _adj_sc.get("membership_reason") or ""
                    cand["_is_adjacent"]            = True
                    if cand["_weak_only"]:
                        _weak_kw_downgrade_count += 1
                    if sym not in _ALL_PROXY_ETFS and sym not in seen_dynamic:
                        seen_dynamic.add(sym)
                        screener_syms.append(sym)
                        screener_meta_by_symbol[sym] = cand
                        sources_by_symbol.setdefault(sym, []).append(_adj_tag)
                        _adj_added += 1
                if _adj_added:
                    print(f"[SCREENER_HUB] fmp_screener_adjacent {key!r}: +{_adj_added} adjacent candidates")
            except Exception as _adj_e:
                print(f"[SCREENER_HUB] fmp_screener_adjacent {key!r} error: {_adj_e}")

        # ── Source P: Neon profile-cache discovery ────────────────────────────
        # Queries screener_fundamentals_cache for all non-expired profiles whose
        # FMP industry is in the theme's core or adjacent industry list, then
        # scores each via score_theme_candidate (uses company description for
        # richer matching beyond just name/sector/industry).
        # Only activates for pure_subtheme on full rebuild jobs.
        # Skips symbols already in seen_dynamic (found via C/C2/seed/ETF).
        _prof_inds: list[str] = list(dict.fromkeys(
            (industries or []) + (_adj_industries or [])
        ))
        if with_fmp_screener and _theme_type == "pure_subtheme" and _prof_inds:
            try:
                from services.fmp_cache_service import get_profiles_by_industries as _get_prof_inds
                _prof_cands = _get_prof_inds(_prof_inds)
                _prof_added   = 0
                _prof_upgraded = 0
                for _pcand in _prof_cands:
                    _psym = _pcand["symbol"]
                    if _psym in _exclude_set:
                        continue
                    _p_src_tag = f"fmp_profile:{_pcand.get('industry', '')}"

                    if _psym in seen_dynamic:
                        # ── Re-scorer path ─────────────────────────────────────────
                        # Company already discovered by Source C/C2/ETF but only has
                        # name/sector/industry keyword evidence.  If the Neon profile
                        # has a description with stronger keyword proof, upgrade the
                        # candidate_tier_override (watch_candidate → verified/adjacent).
                        _existing = screener_meta_by_symbol.get(_psym)
                        if (
                            _existing is not None
                            and _existing.get("candidate_tier_override") == "watch_candidate"
                            and _pcand.get("description")          # only re-score when desc present
                        ):
                            _p_sc = score_theme_candidate(theme_cfg, _pcand, [_p_src_tag])
                            if _p_sc["candidate_tier"] in ("verified_discovery", "adjacent_discovery"):
                                _existing["candidate_tier_override"]        = _p_sc["candidate_tier"]
                                _existing["membership_confidence_override"] = _p_sc["membership_confidence"]
                                _existing["membership_reason"]              = _p_sc.get("membership_reason") or ""
                                _existing["_all_matched_kws"]               = _p_sc["matched_keywords"]
                                _existing["_kw_proof"]                    = (
                                    _p_sc["matched_keywords"] or [""]
                                )[0]
                                _existing["_weak_only"]                   = False
                                _existing["theme_relevance_score"]        = _p_sc["semantic_score"]
                                sources_by_symbol.setdefault(_psym, []).append(_p_src_tag)
                                _prof_upgraded += 1
                        continue

                    # ── New-candidate path ─────────────────────────────────────────
                    _p_sc = score_theme_candidate(theme_cfg, _pcand, [_p_src_tag])
                    if _p_sc["candidate_tier"] == "excluded":
                        continue
                    _pcand["_kw_proof"]                    = (_p_sc["matched_keywords"] or [""])[0]
                    _pcand["_all_matched_kws"]             = _p_sc["matched_keywords"]
                    _pcand["_weak_only"]                   = (
                        _p_sc["candidate_tier"] == "watch_candidate"
                        and _p_sc["membership_confidence"] == "low"
                    )
                    _pcand["candidate_tier_override"]        = _p_sc["candidate_tier"]
                    _pcand["membership_confidence_override"] = _p_sc["membership_confidence"]
                    _pcand["membership_reason"]              = _p_sc.get("membership_reason") or ""
                    _pcand["theme_relevance_score"]        = _p_sc["semantic_score"]
                    _pcand["industry_tier"]                = _p_sc["industry_tier"]
                    _pcand["_is_adjacent"]                 = (
                        _p_sc["industry_tier"] in ("adjacent", "weak_adjacent")
                    )
                    _pcand["discovery_source"]             = "fmp_profile"
                    seen_dynamic.add(_psym)
                    screener_syms.append(_psym)
                    screener_meta_by_symbol[_psym] = _pcand
                    sources_by_symbol.setdefault(_psym, []).append(_p_src_tag)
                    _prof_added += 1
                fmp_profile_discovery_count = _prof_added
                fmp_profile_upgrade_count   = _prof_upgraded
                if _prof_added or _prof_upgraded:
                    print(
                        f"[SCREENER_HUB] fmp_profile_discovery {key!r}: "
                        f"+{_prof_added} new, {_prof_upgraded} upgraded"
                    )
            except Exception as _pe:
                print(f"[SCREENER_HUB] fmp_profile_discovery {key!r} error: {_pe}")

        # ── Source D: FMP peers (thin themes, budget-guarded) ─────────────────
        # Skip for pure_subtheme: themes are too niche for FMP's generic peer
        # algorithm (anchors on industry peers, not keyword-matched companies),
        # and seed coverage is already comprehensive for these themes.
        if with_fmp_peers and _theme_type != "pure_subtheme":
            sources_attempted.append("fmp_peers")
            if len(seen_dynamic) < _MIN_DYN_BEFORE_PEERS and candidate_syms:
                try:
                    peers, fmp_peer_anchors = await _fmp_peers_for_anchors(candidate_syms[:3])
                    for sym in peers:
                        if sym in _exclude_set:
                            continue
                        if sym not in _ALL_PROXY_ETFS and sym not in seen_dynamic:
                            seen_dynamic.add(sym)
                            fmp_peer_syms.append(sym)
                            sources_by_symbol.setdefault(sym, []).append("fmp_peers")
                except Exception as pe:
                    print(f"[SCREENER_HUB] fmp_peers {key} error: {pe}")
                    sources_failed.append("fmp_peers")

        # ── Source F: candidate_symbols — static fallback ─────────────────────
        # For ETF-proxy themes, skip symbols already discovered dynamically
        # (ETF holdings / LKG / screener) to avoid bloating the universe.
        # For custom-proxy themes, candidate_symbols ARE the canonical universe
        # (e.g. ANET, AVGO, MRVL for ai_networking).  They must always appear
        # regardless of whether the FMP screener also returned them — the
        # screener sorts smallest-cap first and caps at SCREENER_RESERVE slots,
        # so mega-cap canonical symbols would otherwise be silently dropped.
        _custom_proxy = _proxy_type != "etf"
        for sym in candidate_syms:
            if sym not in seen_dynamic or _custom_proxy:
                if sym not in static_syms:  # dedupe within static_syms
                    static_syms.append(sym)
                    sources_by_symbol.setdefault(sym, []).append("static_seed")

        # ── Combine with SCREENER_RESERVE guarantee ───────────────────────────
        # ETF+LKG fill the base; screener candidates get guaranteed slots so they
        # aren't entirely crowded out when ETF holdings exceed the cap alone.
        etf_lkg_base    = etf_holdings_syms + lkg_syms
        etf_lkg_capped  = etf_lkg_base[: max(_PER_THEME_CAP - _SCREENER_RESERVE, 30)]
        screener_capped = screener_syms[:_SCREENER_RESERVE]

        # Include overflow ETF+LKG names after reserved screener slots
        etf_lkg_overflow = [
            s for s in etf_lkg_base[max(_PER_THEME_CAP - _SCREENER_RESERVE, 30):]
            if s not in set(etf_lkg_capped) and s not in set(screener_capped)
        ]

        combined = (etf_lkg_capped + screener_capped + fmp_peer_syms
                    + etf_lkg_overflow + static_syms)
        if not combined:
            combined = proxy_etfs  # absolute last resort (Source G)

        # Seed tickers always occupy front slots regardless of cap pressure.
        # _dedupe_filter removes later duplicates so each seed appears once.
        if _seed_tickers:
            combined = _seed_tickers + combined
            # Ensure every seed ticker is tagged in sources_by_symbol regardless
            # of whether it also appeared in candidate_syms (Source F).  A seed
            # can be in theme_fmp_industry_map.json but absent from
            # theme_rs_universe.py candidate_symbols — in that case it would
            # enter combined without a sources_by_symbol entry, causing the row
            # to display membership_source="unknown" at query time.
            for _st in _seed_tickers:
                if _st not in sources_by_symbol:
                    sources_by_symbol[_st] = ["static_seed"]
                elif "static_seed" not in sources_by_symbol[_st]:
                    sources_by_symbol[_st].append("static_seed")

        cleaned = _dedupe_filter(combined)[:_PER_THEME_CAP]

        n_dynamic = (len(etf_holdings_syms) + len(lkg_syms)
                     + len(fmp_peer_syms) + len(screener_syms))
        n_static  = len(static_syms)

        breakdown = {
            "etf_holdings_count":                  len(etf_holdings_syms),
            "lkg_leaders_count":                   len(lkg_syms),
            "fmp_peers_count":                     len(fmp_peer_syms),
            "fmp_peer_anchors":                    fmp_peer_anchors,
            "sector_screener_count":               len(screener_syms),  # compat key
            "fmp_screener_count":                  len(screener_syms),
            "fmp_profile_discovery_count":         fmp_profile_discovery_count,
            "fmp_profile_upgrade_count":           fmp_profile_upgrade_count,
            "fmp_screener_industries_attempted":   fmp_screener_industries_attempted,
            "fmp_screener_industries_errored":     fmp_screener_industries_errored,
            "fmp_screener_calls_used":             fmp_screener_calls_used,
            "fmp_screener_symbols_added":          len(screener_syms),
            "fmp_screener_filtered_reasons":       filtered_reasons_breakdown,
            "fmp_screener_weak_kw_downgraded":     _weak_kw_downgrade_count,
            "industry_map_version":                industry_map_version,
            "static_seed_count":                   n_static,
            "dynamic_symbols_count":               n_dynamic,
            "static_fallback_symbols_count":       n_static,
            "used_static_fallback":                n_static > 0,
            "membership_seed_count":               len(_seed_tickers),
            "membership_exclude_count":            len(_exclude_set),
            "membership_require_kw_match":         _require_kwmatch,
            "membership_theme_type":               _theme_type,
            "membership_fmp_semantic_gate":        "name+sector+industry" if _require_kwmatch else "none",
            "membership_excl_keywords_count":      len(_excl_kws_lower),
            "etf_files_found":                     etf_files_found,
            "sources_by_symbol":                   sources_by_symbol,
            "screener_meta_by_symbol":             screener_meta_by_symbol,
            "sources_attempted":                   sources_attempted,
            "sources_failed":                      sources_failed,
        }

        if cleaned:
            symbols_map[key] = cleaned
        breakdown_map[key] = breakdown

    return symbols_map, breakdown_map


def _build_social_universe() -> list[str]:
    """X consensus weekly top tickers + backend ranked tickers."""
    syms: list[str] = []
    try:
        import json
        path = Path(__file__).parent.parent / "data" / "x_consensus_weekly.json"
        if path.exists():
            data = json.loads(path.read_text())
            for item in (data.get("top_tickers") or []):
                if isinstance(item, dict):
                    s = item.get("symbol") or item.get("ticker")
                    if s:
                        syms.append(str(s).upper())
                elif isinstance(item, str):
                    syms.append(item.upper())
            for item in (data.get("_backend_ranked") or []):
                if isinstance(item, dict):
                    s = item.get("symbol") or item.get("ticker")
                    if s:
                        syms.append(str(s).upper())
    except Exception as e:
        print(f"[SCREENER_HUB] social universe load error: {e}")
    return _dedupe_filter(syms)[:_GLOBAL_TICKER_CAP]


def _build_bottlenecks_universe() -> tuple[list[str], dict]:
    """
    Chain Reaction supply-chain bottleneck universe.

    Dynamic source (primary): backend/data/chain_reaction_output.json
      — Written by Chain Reaction service when a weekly scoring run completes.
      — Expected fields: {"bottleneck_symbols": [...], "generated_at": "..."}
        or {"tickers": [...]} or {"symbols": [...]}.

    Static source (fallback): NODE_REGISTRY in supply_chain_graph.py
      — Curated dict of ~89 companies ranked by a hand-coded bottleneck_score.
      — Used when no chain_reaction_output.json exists (current default).

    Returns (symbols, metadata).
    metadata fields:
      is_dynamic                    — True if dynamic output was loaded
      source_registry               — "chain_reaction_dynamic" | "NODE_REGISTRY"
      last_dynamic_chain_reaction_run — generated_at from dynamic file, or None
      symbol_count                  — len(symbols)
      note                          — human-readable source description
    """
    is_dynamic = False
    last_dynamic_run: Optional[str] = None
    source_registry = "NODE_REGISTRY"
    symbols: list[str] = []

    # ── Try DB dynamic Chain Reaction weekly output first (primary) ──────────
    try:
        cr_row = get_latest_chain_reaction_weekly(max_age_days=10)
        if cr_row and cr_row.get("symbols"):
            raw = cr_row["symbols"]
            symbols = _dedupe_filter([str(s).upper() for s in raw if s])[:_GLOBAL_TICKER_CAP]
            if symbols:
                is_dynamic = True
                source_registry = "chain_reaction_weekly_output"
                last_dynamic_run = cr_row.get("generated_at")
                dynamic_rows_count = len(symbols)
                cr_meta = cr_row.get("metadata") or {}

                # Build per-symbol detail map from rows_json so the screener can
                # enrich each Bottlenecks row with CR scoring fields.
                details_by_symbol: dict[str, dict] = {}
                for rd in (cr_row.get("rows") or []):
                    sym = rd.get("bottleneck_ticker")
                    if sym:
                        details_by_symbol[str(sym).upper()] = rd

                metadata = {
                    "is_dynamic":    True,
                    "source_registry": "chain_reaction_weekly_output",
                    "last_dynamic_chain_reaction_run": last_dynamic_run,
                    "dynamic_rows_count": dynamic_rows_count,
                    "symbol_count":  len(symbols),
                    "week_start":    cr_row.get("week_start"),
                    "source_version": cr_row.get("source_version"),
                    "details_by_symbol": details_by_symbol,
                    "note": (
                        f"Dynamic Chain Reaction weekly output from DB "
                        f"(week {cr_row.get('week_start','?')}, "
                        f"generated {str(last_dynamic_run or '')[:10]}). "
                        f"Scored {cr_meta.get('scored_count', len(symbols))} nodes."
                    ),
                }
                print(f"[SCREENER_HUB] bottlenecks: loaded {len(symbols)} symbols + "
                      f"{len(details_by_symbol)} detail rows from DB weekly output")
                return symbols, metadata
    except Exception as e:
        print(f"[SCREENER_HUB] bottlenecks DB check error: {e}")

    # ── Try local JSON file (legacy dynamic output) ───────────────────────────
    if _CHAIN_REACTION_OUTPUT.exists():
        try:
            import json
            data = json.loads(_CHAIN_REACTION_OUTPUT.read_text())
            raw_syms = (
                data.get("bottleneck_symbols")
                or data.get("tickers")
                or data.get("symbols")
                or []
            )
            last_dynamic_run = (
                data.get("generated_at")
                or data.get("built_at")
                or data.get("as_of")
            )
            if raw_syms:
                symbols = _dedupe_filter(
                    [str(s).upper() for s in raw_syms if s]
                )[:_GLOBAL_TICKER_CAP]
                is_dynamic = True
                source_registry = "chain_reaction_dynamic"
                print(f"[SCREENER_HUB] bottlenecks: loaded {len(symbols)} from dynamic output")
        except Exception as e:
            print(f"[SCREENER_HUB] chain_reaction_output.json read error: {e}")

    # ── Fall back to static NODE_REGISTRY ────────────────────────────────────
    if not symbols:
        out: list[tuple[str, int]] = []
        try:
            from services.playbook.supply_chain_graph import NODE_REGISTRY
            for ticker, node in (NODE_REGISTRY or {}).items():
                if not isinstance(node, dict):
                    continue
                score = int(node.get("bottleneck_score") or 0)
                # Prefer the US-listed proxy when the native ticker isn't tradeable here
                us_proxy = (
                    node.get("us_access_proxy")
                    or node.get("adr_ticker")
                    or ticker
                )
                out.append((str(us_proxy).upper(), score))
        except Exception as e:
            print(f"[SCREENER_HUB] bottlenecks load error: {e}")
        out.sort(key=lambda r: r[1], reverse=True)
        seen: set[str] = set()
        for s, _ in out:
            if s and s not in seen:
                seen.add(s)
                symbols.append(s)
        symbols = symbols[:_GLOBAL_TICKER_CAP]

    metadata: dict = {
        "is_dynamic":    is_dynamic,
        "source_registry": source_registry,
        "last_dynamic_chain_reaction_run": last_dynamic_run,
        "symbol_count":  len(symbols),
        "note": (
            "Dynamic Chain Reaction scoring output loaded from data/chain_reaction_output.json."
            if is_dynamic
            else
            "Static curated NODE_REGISTRY (supply_chain_graph.py). "
            "No dynamic Chain Reaction output found at data/chain_reaction_output.json. "
            "NODE_REGISTRY is the authoritative bottleneck source until a weekly "
            "chain_reaction_output.json is produced."
        ),
    }
    return symbols, metadata


# ── Canonical theme helpers (THEME_RS_UNIVERSE — same registry as Themes page) ──
# Built once at import time via theme_ticker_mapper; pure O(1) dict lookups.
# No LLM calls, no network IO.

_CANON_NAME_NORMALIZE: dict[str, str] = {
    "Memory / Storage":      "Memory & Storage",
    "Robotics / Automation": "Robotics & Automation",
    "Datacenter / Compute":  "Data Center Infrastructure",
    "Aerospace / Defense":   "Defense",
}
_CANON_ID_NORMALIZE: dict[str, str] = {
    "memory_/_storage":      "memory_storage",
    "robotics_/_automation": "robotics_automation",
    "datacenter_/_compute":  "datacenter_infra",
    "aerospace_/_defense":   "defense",
}


def _get_canonical_theme_name(ticker: str) -> Optional[str]:
    """Return the canonical display_name for a ticker from THEME_RS_UNIVERSE, or None.

    Applies normalization so Source-2/3 "/" variants map to their THEME_RS_UNIVERSE
    canonical "&" equivalents (e.g. 'Memory / Storage' → 'Memory & Storage').
    """
    try:
        from services.theme_ticker_mapper import map_ticker_to_primary_theme
        name = map_ticker_to_primary_theme(ticker)
        return _CANON_NAME_NORMALIZE.get(name, name) if name else None
    except Exception:
        return None


def _get_canonical_theme_id(ticker: str) -> Optional[str]:
    """Return the canonical theme_id for a ticker from THEME_RS_UNIVERSE, or None.

    Normalizes "/" variant IDs to their THEME_RS_UNIVERSE canonical form.
    """
    try:
        from services.theme_ticker_mapper import map_ticker_to_theme_id
        tid = map_ticker_to_theme_id(ticker)
        return _CANON_ID_NORMALIZE.get(tid, tid) if tid else None
    except Exception:
        return None


def _build_watchlist_portfolio_universe() -> list[str]:
    syms: set[str] = set()
    try:
        from services.watchlist_service import list_watchlists, load_watchlist
        for wl in (list_watchlists() or [])[:10]:
            wl_id = wl.get("id") if isinstance(wl, dict) else None
            if not wl_id:
                continue
            store = load_watchlist(wl_id)
            if isinstance(store, dict):
                for t in (store.get("tickers") or []):
                    if isinstance(t, str):
                        syms.add(t.upper())
                    elif isinstance(t, dict) and t.get("symbol"):
                        syms.add(str(t["symbol"]).upper())
    except Exception as e:
        print(f"[SCREENER_HUB] watchlist load error: {e}")
    try:
        import json
        for p in Path(__file__).parent.parent.joinpath("data").glob("portfolio_holdings*.json"):
            try:
                data = json.loads(p.read_text())
                holdings = data.get("holdings", data) if isinstance(data, dict) else data
                if isinstance(holdings, list):
                    for h in holdings:
                        if isinstance(h, dict):
                            s = h.get("symbol") or h.get("ticker")
                            if s:
                                syms.add(str(s).upper())
            except Exception:
                continue
    except Exception as e:
        print(f"[SCREENER_HUB] portfolio load error: {e}")
    return _dedupe_filter(sorted(syms))


_BAD_PREFIX = ("$", ".", "^")


def _dedupe_filter(symbols: Iterable[str]) -> list[str]:
    """Dedupe + strip obviously-non-equity tickers. Order-preserving."""
    seen: set[str] = set()
    out: list[str] = []
    for raw in symbols:
        if not isinstance(raw, str):
            continue
        s = raw.strip().upper()
        if not s or len(s) > 6:
            continue
        if s.startswith(_BAD_PREFIX):
            continue
        if not s.replace(".", "").replace("-", "").isalnum():
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


# ── FMP fundamentals fetch (one symbol at a time) ─────────────────────────────

async def _fmp_get(client: httpx.AsyncClient, endpoint: str, params: dict) -> Any:
    api_key = os.getenv("FMP_API_KEY") or ""
    if not api_key:
        return None
    qp = dict(params or {})
    qp["apikey"] = api_key
    try:
        r = await client.get(f"{FMP_BASE}/{endpoint}", params=qp, timeout=_FMP_TIMEOUT)
        if r.status_code == 200:
            return r.json()
        return None
    except Exception as e:
        print(f"[SCREENER_HUB] FMP {endpoint} error: {e}")
        return None


async def _fetch_fundamentals_for_symbol(
    client: httpx.AsyncClient, symbol: str
) -> Optional[dict]:
    """One symbol → profile + key-metrics-ttm + ratios-ttm. Returns None on hard failure."""
    profile = await _fmp_get(client, "profile", {"symbol": symbol})
    if isinstance(profile, list) and profile:
        profile = profile[0]
    if not isinstance(profile, dict):
        # Profile is the anchor; if missing we treat the row as unfetched.
        return None
    metrics = await _fmp_get(client, "key-metrics-ttm", {"symbol": symbol})
    if isinstance(metrics, list) and metrics:
        metrics = metrics[0]
    if not isinstance(metrics, dict):
        metrics = {}
    ratios = await _fmp_get(client, "ratios-ttm", {"symbol": symbol})
    if isinstance(ratios, list) and ratios:
        ratios = ratios[0]
    if not isinstance(ratios, dict):
        ratios = {}
    return {
        "profile": profile,
        "metrics": metrics,
        "ratios":  ratios,
        "market_cap": _to_float(profile.get("marketCap") or profile.get("mktCap")),
        "sector":   profile.get("sector"),
        "industry": profile.get("industry"),
        "country":  profile.get("country"),
        "exchange": profile.get("exchangeShortName") or profile.get("exchange"),
    }


def _to_float(v: Any) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


# ── Warm fundamentals (used by jobs + synchronous best-effort path) ───────────

async def warm_fundamentals(
    symbols: Iterable[str],
    *,
    job_name: str,
    force: bool = False,
    sleep_between_s: float = _FMP_SLEEP_BETWEEN_S,
    max_calls: int = 250,
) -> dict:
    """Sequentially fetch FMP fundamentals for each symbol, respecting cache.

    Returns a summary dict. Records a row in screener_job_runs.
    """
    try:
        from services.fmp_governor import fmp_governor as _gov
    except Exception:
        _gov = None

    deduped = _dedupe_filter(symbols)
    run_id = start_job_run(job_name, symbols_count=len(deduped),
                           metadata={"force": bool(force)})
    if _gov is not None:
        _gov.start_job(job_name)

    completed = 0
    failed = 0
    api_calls = 0
    error_msg: Optional[str] = None
    budget_limited = False

    try:
        if not force:
            fresh = fundamentals_fresh_symbols(deduped, max_age_days=_FUNDAMENTALS_TTL_DAYS)
            queue = [s for s in deduped if s not in fresh]
            print(f"[SCREENER_HUB] {job_name}: {len(deduped)} total, {len(queue)} stale, {len(fresh)} fresh")
        else:
            queue = list(deduped)
            print(f"[SCREENER_HUB] {job_name}: force=True, processing all {len(queue)}")

        async with httpx.AsyncClient() as client:
            for idx, symbol in enumerate(queue):
                if api_calls >= max_calls:
                    print(f"[SCREENER_HUB] {job_name}: max_calls={max_calls} reached, stopping")
                    break
                # ── Governor check ──
                if _gov is not None:
                    slot_ok = await _gov.acquire(job_name=job_name)
                    if not slot_ok:
                        budget_limited = True
                        print(f"[SCREENER_HUB] {job_name}: governor budget hit at symbol={symbol}")
                        break
                try:
                    record = await _fetch_fundamentals_for_symbol(client, symbol)
                    if _gov is not None:
                        _gov.record_call()  # count profile+metrics+ratios as 1 batch
                    api_calls += 3  # profile + metrics + ratios
                    if record is None:
                        failed += 1
                    else:
                        ok = upsert_fundamentals(
                            symbol,
                            profile=record["profile"],
                            metrics=record["metrics"],
                            ratios=record["ratios"],
                            market_cap=record["market_cap"],
                            sector=record["sector"],
                            industry=record["industry"],
                            country=record["country"],
                            exchange=record["exchange"],
                            ttl_days=_FUNDAMENTALS_TTL_DAYS,
                        )
                        if ok:
                            completed += 1
                        else:
                            failed += 1
                except Exception as e:
                    failed += 1
                    print(f"[SCREENER_HUB] {job_name} {symbol} error: {e}")

                # Polite delay between calls (skip after the last one)
                # Governor handles minimum spacing; extra sleep only if explicitly set.
                if sleep_between_s > 0 and _gov is None and idx < len(queue) - 1:
                    await asyncio.sleep(sleep_between_s)

        status = (
            "partial_budget_limit" if budget_limited
            else ("ok" if failed == 0 else ("partial" if completed > 0 else "failed"))
        )
        finish_job_run(
            run_id, status=status,
            symbols_completed=completed, symbols_failed=failed,
            api_calls_used=api_calls,
            error=None,
            metadata={"queue_size": len(queue), "budget_limited": budget_limited},
        )
        if _gov is not None:
            _gov.finish_job(job_name, budget_limited=budget_limited)
        return {
            "job_name": job_name,
            "status": status,
            "symbols_count": len(deduped),
            "symbols_completed": completed,
            "symbols_failed": failed,
            "api_calls_used": api_calls,
            "budget_limited": budget_limited,
        }
    except Exception as e:
        error_msg = str(e)
        finish_job_run(
            run_id, status="failed",
            symbols_completed=completed, symbols_failed=failed,
            api_calls_used=api_calls, error=error_msg,
        )
        if _gov is not None:
            _gov.finish_job(job_name, budget_limited=False)
        return {
            "job_name": job_name,
            "status": "failed",
            "symbols_count": len(deduped),
            "symbols_completed": completed,
            "symbols_failed": failed,
            "api_calls_used": api_calls,
            "error": error_msg,
        }


# ── Quote refresh (page-aware; Tradier only) ──────────────────────────────────

async def refresh_quotes_for_page(symbols: Iterable[str]) -> dict:
    """Refresh Tradier quotes only for the symbols on the active page.

    Honors a short TTL to coalesce multiple frontend hits. Never blanks rows;
    on Tradier failure we leave the existing cached row in place.
    """
    deduped = _dedupe_filter(symbols)
    if not deduped:
        return {"status": "ok", "refreshed": 0, "cached_used": 0}

    ttl = _QUOTE_TTL_OPEN_S if _is_market_open() else _QUOTE_TTL_CLOSED_S
    cached = get_quotes(deduped)
    now_ts = time.time()
    stale: list[str] = []
    for s in deduped:
        row = cached.get(s)
        if not row or not row.get("fetched_at"):
            stale.append(s)
            continue
        try:
            fetched = datetime.fromisoformat(row["fetched_at"].replace("Z", "+00:00"))
        except Exception:
            stale.append(s)
            continue
        age = now_ts - fetched.timestamp()
        if age > ttl:
            stale.append(s)

    if not stale:
        return {"status": "ok", "refreshed": 0, "cached_used": len(cached)}

    api_key = os.getenv("TRADIER_API_KEY") or ""
    sandbox = (os.getenv("TRADIER_SANDBOX", "false") or "false").lower() in ("1", "true", "yes")
    if not api_key:
        return {"status": "no_provider", "refreshed": 0, "cached_used": len(cached)}

    try:
        from data.tradier_provider import TradierProvider
        provider = TradierProvider(api_key, sandbox=sandbox)
    except Exception as e:
        print(f"[SCREENER_HUB] Tradier init error: {e}")
        return {"status": "error", "refreshed": 0, "cached_used": len(cached)}

    refreshed = 0
    # Tradier accepts comma-separated batches; chunk to keep URLs short.
    batch_size = 50
    for i in range(0, len(stale), batch_size):
        chunk = stale[i:i + batch_size]
        try:
            from data.tradier_budget import lane as _shub_lane
            with _shub_lane("quotes"):
                quotes = await provider.get_quotes(chunk)
        except Exception as e:
            print(f"[SCREENER_HUB] Tradier batch error ({i}): {e}")
            continue
        if not quotes:
            continue
        for q in quotes:
            sym = (q.get("symbol") or "").upper()
            if not sym:
                continue
            price = q.get("last") if q.get("last") is not None else q.get("close")
            change_pct = q.get("change_percentage")
            if price is None and change_pct is None:
                # Don't overwrite a valid cached row with empty data.
                continue
            ok = upsert_quote(
                sym,
                quote=q,
                price=_to_float(price),
                change_percent_1d=_to_float(change_pct),
                provider="tradier_sandbox" if sandbox else "tradier",
            )
            if ok:
                refreshed += 1

    return {"status": "ok", "refreshed": refreshed, "cached_used": len(deduped) - refreshed}


# ── Classification (Leading / Improving / Weakening / Lagging) ────────────────

def _classify_row(
    metrics: dict,
    quote: dict,
    *,
    score_mode: bool,
    coc_filter: bool,
    return_2w: Optional[float] = None,
    return_4w: Optional[float] = None,
    return_10w: Optional[float] = None,
) -> dict:
    """
    Compute screener category + score using whatever signals we have.

    Parameters
    ----------
    return_2w/4w/10w : Real historical trailing returns from screener_returns_cache.
                       Pass None when the cache is cold — we never substitute fake
                       1D change for multi-week RS fields.
    """
    distance_52w_high: Optional[float] = None
    week_52_high = quote.get("week_52_high") if isinstance(quote, dict) else None
    last = quote.get("last") if isinstance(quote, dict) else None
    if week_52_high and last:
        try:
            if float(week_52_high) > 0:
                distance_52w_high = round(
                    (float(last) - float(week_52_high)) / float(week_52_high) * 100, 2
                )
        except (TypeError, ValueError):
            distance_52w_high = None

    chg_1d = _to_float(quote.get("change_percentage")) if isinstance(quote, dict) else None
    avg_vol = _to_float(quote.get("average_volume")) if isinstance(quote, dict) else None
    vol     = _to_float(quote.get("volume")) if isinstance(quote, dict) else None
    volume_surge: Optional[float] = None
    if vol and avg_vol and avg_vol > 0:
        volume_surge = round(vol / avg_vol, 3)

    # Real multi-week returns from historical cache.
    # If not cached yet → None (never substitute fake 1D change for RS fields).
    rs_0_2w  = return_2w
    rs_0_4w  = return_4w
    rs_0_10w = return_10w
    rs_accel: Optional[float] = None
    if rs_0_2w is not None and rs_0_4w is not None:
        rs_accel = round(rs_0_2w - rs_0_4w, 4)

    # Accumulation: meaningful positive day (>0.5%) on elevated volume (≥1.5×).
    accumulation: Optional[bool] = None
    if volume_surge is not None and chg_1d is not None:
        accumulation = bool(volume_surge >= 1.5 and chg_1d > 0.5)

    coc: Optional[bool] = None

    # Signal scoring — use real 4w return if available, else 1D change as proxy.
    # This prevents the "whole screener is Leading when the day is slightly green"
    # artifact that occurred when rs_0_4w was always equal to chg_1d.
    rs_signal = rs_0_4w if rs_0_4w is not None else chg_1d
    signals = {
        "rs_positive":   bool(rs_signal is not None and rs_signal > 0.5),
        "near_52w_high": bool(distance_52w_high is not None and distance_52w_high > -10),
        "vol_surge":     bool(volume_surge is not None and volume_surge >= 1.5),
        "accumulation":  bool(accumulation),
    }
    score = sum(1 for v in signals.values() if v)

    # Category — Leading / Improving / Weakening / Lagging
    if score >= 3:
        category = "Leading"
    elif score == 2:
        category = "Improving"
    elif score == 1:
        category = "Weakening"
    else:
        category = "Lagging"

    return {
        "rs_0_2w":  rs_0_2w,
        "rs_0_4w":  rs_0_4w,
        "rs_0_10w": rs_0_10w,
        "rs_accel": rs_accel,
        "distance_52w_high": distance_52w_high,
        "volume_surge": volume_surge,
        "accumulation": accumulation,
        "coc": coc,
        "score": score if score_mode else None,
        "category": category,
        "_signals": signals,
    }


def _row_passes_filters(row: dict, *, category_filter: Optional[str],
                        coc_filter: bool) -> bool:
    if category_filter and row.get("category") != category_filter:
        return False
    if coc_filter and row.get("coc") is not True:
        return False
    return True


# ── Main page query ───────────────────────────────────────────────────────────

def _next_sunday_rebuild_et() -> str:
    """ISO timestamp of the next thematic universe rebuild (Sun 00:30 US/Eastern)."""
    try:
        import datetime as _dt
        try:
            import zoneinfo as _zi
            _ET = _zi.ZoneInfo("America/New_York")
        except ImportError:
            import pytz as _pytz
            _ET = _pytz.timezone("America/New_York")
        _now  = _dt.datetime.now(tz=_ET)
        _days = (6 - _now.weekday()) % 7          # 0 = today is Sunday
        if _days == 0 and (_now.hour > 0 or _now.minute >= 30):
            _days = 7                               # already past 00:30 ET today
        _date = _now.date() + _dt.timedelta(days=_days)
        return _dt.datetime(_date.year, _date.month, _date.day, 0, 30, tzinfo=_ET).isoformat()
    except Exception:
        return "Sunday 00:30 ET"


def _extract_quote_fields(entry: dict) -> Optional[dict]:
    """
    Extract canonical quote fields from a cache entry that may be in either
    raw-Tradier shape (last / change_percentage / average_volume) or the
    normalized shape written by Home/Watchlist/Portfolio
    (price / change_pct / avg_volume).

    Returns None when no usable price is found.
    """
    canon_price = (
        _to_float(entry.get("last"))       # raw Tradier
        or _to_float(entry.get("price"))   # normalized
    )
    if canon_price is None:
        return None

    return {
        "price":          canon_price,
        "change_pct": (
            _to_float(entry.get("change_percentage"))   # raw Tradier
            or _to_float(entry.get("change_pct"))       # normalized
            or _to_float(entry.get("change_pct_1d"))
        ),
        "volume":         _to_float(entry.get("volume")),
        "average_volume": (
            _to_float(entry.get("average_volume"))      # raw Tradier
            or _to_float(entry.get("avg_volume"))       # normalized
        ),
        "bid":            _to_float(entry.get("bid")),
        "ask":            _to_float(entry.get("ask")),
        "quote_source": (
            entry.get("quote_source") or "tradier"
        ),
        "quote_is_stale": bool(entry.get("quote_is_stale", False)),
        "quote_fetched_at": (
            entry.get("fetched_at")
            or entry.get("last_updated")
            or entry.get("quote_fetched_at")
        ),
    }


def _apply_quote_to_row(row: dict, fields: dict) -> None:
    """
    Write extracted canonical quote fields into a Screener Hub row in-place.
    Recomputes derived fields (relative_volume, dollar_volume) from overlaid values.
    """
    row["price"]             = fields["price"]
    row["change_percent_1d"] = fields["change_pct"]
    row["quote_source"]      = fields["quote_source"]
    row["quote_is_stale"]    = fields["quote_is_stale"]
    row["quote_fetched_at"]  = fields["quote_fetched_at"]

    if fields["volume"] is not None:
        row["volume"] = fields["volume"]
    if fields["average_volume"] is not None:
        row["average_volume"] = fields["average_volume"]
    if fields["bid"] is not None:
        row["bid"] = fields["bid"]
    if fields["ask"] is not None:
        row["ask"] = fields["ask"]

    _ov = fields["volume"] if fields["volume"] is not None else row.get("volume")
    _av = fields["average_volume"] if fields["average_volume"] is not None else row.get("average_volume")
    if _ov is not None and _av and _av > 0:
        row["relative_volume"] = round(_ov / _av, 4)
    if fields["price"] is not None and _ov is not None:
        row["dollar_volume"] = round(fields["price"] * _ov)


# LKG TTL used when writing fresh Tradier results to quote:lkg (matches home_service)
_CANONICAL_LKG_TTL = 72 * 3600


async def _overlay_canonical_quotes_inplace(
    rows: list[dict],
    *,
    selected_theme: Optional[str] = None,
) -> None:
    """
    Three-pass canonical quote overlay for Screener Hub rows.

    Pass 1 — tradier:quote:sym:{SYM}  (60 s TTL, freshest — written by any
              TradierProvider.get_quotes() call anywhere in the app)
    Pass 2 — quote:lkg:{SYM}          (72 h TTL — written by Home Watchlist,
              Watchlist overlay, Portfolio, and Pass 3 below)
    Pass 3 — Live Tradier batch fetch  for symbols still missing after Passes 1+2.
              ONLY fires when selected_theme is a specific non-None theme string
              (i.e. a user has explicitly selected a theme from the dropdown).
              Skipped for watchlist_portfolio tab, all/default views, background
              refreshes, and query-cache warmup jobs — those tickers are already
              covered by Pass 1+2 from Home/Watchlist/Portfolio canonical cache.
              Also skipped when TRADIER_LIMITER is saturated or no API key.
              Results written to tradier:quote:sym (auto by provider) and
              quote:lkg so every other page can reuse them immediately.

    selected_theme — the explicit theme key the user selected (e.g.
                     "semiconductors", "ai_networking").  Pass None for
                     watchlist_portfolio, all/default, or non-thematic tabs.

    Snapshot preservation:
      snapshot_price / snapshot_change_percent_1d / snapshot_volume /
      snapshot_quote_fetched_at — set once before the first overlay so the
      frontend can show the before/after delta.

    Structured log line at every exit:
      selected_theme=<str|None> rows=<n> pass1=<n> pass2=<n>
      pass3_fetch=<n> pass3_skipped=<reason|None>

    Never raises: each symbol is wrapped in its own try/except.
    """
    if not rows:
        return

    # ── Snapshot: capture current SH values before any overlay ───────────────
    for row in rows:
        if "snapshot_price" not in row:
            row["snapshot_price"]             = row.get("price")
            row["snapshot_change_percent_1d"] = row.get("change_percent_1d")
            row["snapshot_volume"]            = row.get("volume")
            row["snapshot_quote_fetched_at"]  = (row.get("_meta") or {}).get("quote_fetched_at")

    still_missing: list[dict] = []   # rows with no canonical hit after Pass 1+2
    p1_hits = 0
    p2_hits = 0

    for row in rows:
        sym = (row.get("symbol") or "").upper()
        if not sym:
            continue
        try:
            # ── Pass 1: tradier:quote:sym (60 s short-TTL per-symbol cache) ──
            entry = _shared_lkg_cache.get(f"tradier:quote:sym:{sym}")
            if entry:
                fields = _extract_quote_fields(entry)
                if fields:
                    # tradier:quote:sym has no quote_source/stale fields — add them
                    fields["quote_source"]    = "tradier"
                    fields["quote_is_stale"]  = False
                    _apply_quote_to_row(row, fields)
                    p1_hits += 1
                    continue

            # ── Pass 2: quote:lkg (shared canonical, 72 h TTL) ───────────────
            lkg = _shared_lkg_cache.get(f"quote:lkg:{sym}")
            if lkg:
                fields = _extract_quote_fields(lkg)
                if fields:
                    _apply_quote_to_row(row, fields)
                    p2_hits += 1
                    continue

            # No canonical data found — queue for Pass 3
            still_missing.append(row)

        except Exception:
            pass

    def _log(pass3_fetch: int = 0, pass3_skipped: Optional[str] = None) -> None:
        print(
            f"[SCREENER_HUB] canonical_quote_overlay"
            f" selected_theme={selected_theme!r}"
            f" rows={len(rows)}"
            f" pass1={p1_hits}"
            f" pass2={p2_hits}"
            f" pass3_fetch={pass3_fetch}"
            f" pass3_skipped={pass3_skipped!r}"
        )

    # ── Pass 3: targeted Tradier fetch for still-missing active-page symbols ──
    if not still_missing:
        _log(pass3_fetch=0, pass3_skipped="nothing_missing")
        return

    # Guard 1: Pass 3 only runs for a user-triggered specific-theme request.
    # watchlist_portfolio tab, all/default views, background refreshes, and
    # query-cache warmups all pass selected_theme=None and skip Pass 3 here.
    # Their tickers are already covered by the Home/Watchlist/Portfolio
    # canonical cache via Passes 1+2.
    if not selected_theme:
        _log(pass3_fetch=0, pass3_skipped="no_active_theme")
        return

    # Guard 2: Saturation check — skip live call if rate window is full
    _saturated = False
    try:
        from data.tradier_provider import TRADIER_LIMITER as _TL
        _saturated = _TL.is_saturated()
    except Exception:
        pass

    api_key = os.getenv("TRADIER_API_KEY") or ""
    sandbox = (os.getenv("TRADIER_SANDBOX", "false") or "false").lower() in ("1", "true", "yes")

    if _saturated or not api_key:
        _log(pass3_fetch=0, pass3_skipped="saturated" if _saturated else "no_api_key")
        return

    # Build symbol→row map for fast lookup after fetch
    sym_to_row: dict[str, dict] = {
        (r.get("symbol") or "").upper(): r
        for r in still_missing
        if (r.get("symbol") or "").upper()
    }
    missing_syms = list(sym_to_row.keys())

    try:
        from data.tradier_provider import TradierProvider
        provider = TradierProvider(api_key, sandbox=sandbox)
    except Exception as e:
        print(f"[SCREENER_HUB] canonical_quote_overlay: Tradier init error: {e}")
        _log(pass3_fetch=0, pass3_skipped="init_error")
        return

    fetched = 0
    now_iso = datetime.now(timezone.utc).isoformat()
    batch_size = 50

    for i in range(0, len(missing_syms), batch_size):
        chunk = missing_syms[i : i + batch_size]
        try:
            # provider.get_quotes() auto-writes to tradier:quote:sym:{SYM}
            from data.tradier_budget import lane as _shub2_lane
            with _shub2_lane("quotes"):
                quotes = await provider.get_quotes(chunk)
        except Exception as e:
            print(f"[SCREENER_HUB] canonical_quote_overlay: Tradier batch error: {e}")
            continue

        for q in (quotes or []):
            sym = (q.get("symbol") or "").upper()
            row = sym_to_row.get(sym)
            if not row:
                continue
            try:
                fields = _extract_quote_fields(q)
                if not fields:
                    continue

                fields["quote_source"]    = "tradier"
                fields["quote_is_stale"]  = False
                fields["quote_fetched_at"] = now_iso

                _apply_quote_to_row(row, fields)

                # Write-back: populate quote:lkg so Home/Watchlist/Portfolio
                # can reuse this fresh Tradier data immediately
                lkg_entry = {
                    **q,
                    "quote_source":          "tradier",
                    "quote_is_stale":        False,
                    "quote_fallback_reason": None,
                    "fetched_at":            now_iso,
                }
                _shared_lkg_cache.set(f"quote:lkg:{sym}", lkg_entry, _CANONICAL_LKG_TTL)

                fetched += 1
            except Exception:
                pass

    _log(pass3_fetch=fetched, pass3_skipped=None)


async def get_screener_hub(
    *,
    tab: str,
    theme: Optional[str] = None,
    category: Optional[str] = None,
    score_mode: bool = False,
    coc_filter: bool = False,
    market_cap_min: Optional[float] = None,
    market_cap_max: Optional[float] = None,
    min_volume: Optional[float] = None,
    exchange: Optional[str] = None,
) -> dict:
    """Build the response payload for /api/screener-hub.

    The shape matches the contract requested by the frontend:
      { status, tab, theme, generated_at, fundamentals_cache_status,
        quote_cache_status, rows: [...] }

    Universe symbols come from the latest screener_universe_snapshots row.
    Fundamentals come from screener_fundamentals_cache (no live FMP fetch).
    Quotes come from screener_quote_cache; we refresh stale rows for *just*
    the active page before returning.

    Quote freshness policy for query cache hits:
      Full response is cached for 7 days as-is (hit_static_7d).
      Price/volume fields may be up to 7 days stale on a cache hit.
      This avoids a complex overlay rewrite and keeps the cache layer thin.
    """
    ensure_tables()

    tab = (tab or "").strip().lower()
    theme = (theme or "").strip().lower() or None
    if tab not in ("thematic", "social", "bottlenecks", "watchlist_portfolio"):
        return {
            "status": "error",
            "error": f"unknown tab '{tab}'",
            "tab": tab,
            "theme": theme,
            "generated_at": _now_iso(),
            "rows": [],
        }

    # ── Query cache read (thematic tab + selected theme only) ────────────────
    _qcache_key: Optional[str] = None
    _qcache_status = "not_applicable"
    if tab == "thematic" and theme:
        _qcache_key = _compute_screener_cache_key(
            tab=tab, theme=theme, category=category,
            score_mode=score_mode, coc_filter=coc_filter,
            market_cap_min=market_cap_min, market_cap_max=market_cap_max,
            min_volume=min_volume, exchange=exchange,
            schema_version=THEMATIC_REFRESH_SCHEMA_VERSION,
        )
        try:
            _hit = get_query_cache(_qcache_key, schema_version=THEMATIC_REFRESH_SCHEMA_VERSION)
            if _hit:
                _cached_resp = dict(_hit["response_json"])
                _cached_resp["query_cache_status"]     = "hit"
                _cached_resp["query_cache_key"]        = _qcache_key
                _cached_resp["query_cache_expires_at"] = _hit.get("expires_at")
                _cached_resp["query_cache_created_at"] = _hit.get("created_at")
                _cached_resp["served_at"]              = _now_iso()
                print(
                    f"[SCREENER_QUERY_CACHE] HIT theme={theme!r} "
                    f"rows={_hit.get('row_count')} "
                    f"expires={_hit.get('expires_at')}"
                )
                # Overlay canonical quotes even on cache hits so the returned
                # price/1D%/volume always matches Home/Watchlist/Portfolio.
                # Pass the selected theme so Pass 3 targets only its symbols.
                await _overlay_canonical_quotes_inplace(
                    _cached_resp.get("rows") or [],
                    selected_theme=theme,
                )
                return _cached_resp
        except Exception as _qe:
            print(f"[SCREENER_QUERY_CACHE] read error (non-fatal): {_qe}")
        _qcache_status = "miss"

    symbols: list[str] = []
    snap_status = "fresh"
    universe_source = "snapshot"
    theme_state_meta: dict = {}
    thematic_breakdown: dict = {}
    bottlenecks_meta: dict = {}

    # Snapshot freshness tracking (populated when a DB snapshot is loaded)
    snap_generated_at: Optional[str] = None
    snap_expires_at:   Optional[str] = None
    snap_db_source:    Optional[str] = None

    # Post-build filter tracking
    rows_before_filters: int = 0
    rows_after_filters:  int = 0
    filters_applied:     dict = {}

    # Quote-refresh tracking
    quote_refresh_started: bool = False

    # Theme selection tracking
    is_default_theme:              bool           = False
    default_theme_reason:          Optional[str]  = None
    theme_refresh_status:          str            = "not_applicable"
    theme_last_refreshed_at:       Optional[str]  = None
    theme_next_refresh_allowed_at: Optional[str]  = None

    # Non-equity and missing-field exclusion counters
    rows_excluded_non_equity:          int = 0
    rows_excluded_missing_market_cap:  int = 0
    rows_excluded_missing_volume:      int = 0

    # FMP refresh tracking
    fmp_refresh_used:   bool           = False
    fmp_refresh_reason: Optional[str]  = None

    # Row-source tally (populated during row-build loop; disclosed in response)
    _row_source_tally: dict[str, int] = {}

    # ── Load overlap sets (fast disk reads; used for per-row tagging) ──────────
    # These are loaded once per request and passed through to row building.
    social_overlap:     set[str]        = _load_social_overlap()
    options_overlap:    set[str]        = _load_options_overlap()
    # Watchlist NOT loaded for thematic tab — watchlist/portfolio is its own
    # separate tab and must not influence thematic discovery, scoring, or roles.
    watchlist_set:      set[str]        = set() if tab == "thematic" else _load_watchlist_set()
    options_detail_map: dict[str, dict] = _load_options_detail_map()

    # ── Resolve universe ──
    if tab == "thematic":
        # ── Validate theme against known registry ─────────────────────────────
        if theme:
            try:
                from services.theme_rs_universe import THEME_RS_UNIVERSE as _TRU
                if theme not in _TRU:
                    return {
                        "status":              "error",
                        "error_code":          "UNKNOWN_THEME",
                        "error":               (
                            f"Unknown theme '{theme}'. "
                            "See GET /api/screener-hub/themes for valid keys."
                        ),
                        "tab":                 tab,
                        "theme":               theme,
                        "selected_theme":      theme,
                        "is_default_theme":    False,
                        "generated_at":        _now_iso(),
                        "rows":                [],
                        "filters_applied":     {},
                        "rows_before_filters": 0,
                        "rows_after_filters":  0,
                    }
            except Exception:
                pass  # registry load error — proceed and let snapshot lookup be the gate

        # Pull theme state from Themes page LKG (reuse, don't duplicate).
        if theme:
            theme_state_meta = _get_theme_state_from_lkg(theme)

        if theme:
            snap = get_latest_universe("thematic", theme)
            if snap and snap.get("symbols"):
                # Capture real snapshot freshness metadata before anything else
                snap_generated_at = str(snap.get("generated_at") or "") or None
                snap_expires_at   = str(snap.get("expires_at")   or "") or None
                snap_db_source    = snap.get("source") or "unknown"

                raw_syms = list(snap.get("symbols") or [])
                # If snapshot is ETF-only (built before the dynamic universe fix),
                # discard it and rebuild live so stocks appear immediately.
                stock_syms = [s for s in raw_syms if s not in _ALL_PROXY_ETFS]
                if stock_syms:
                    symbols = raw_syms  # snapshot is good
                    # Hydrate full breakdown (including sources_by_symbol) from
                    # persisted snapshot metadata. Falls back gracefully for old
                    # snapshots that predate the metadata_json column.
                    snap_meta = snap.get("metadata") or {}
                    if snap_meta and isinstance(snap_meta, dict):
                        thematic_breakdown = snap_meta
                    else:
                        # Old snapshot — lightweight fallback, no provenance tags
                        thematic_breakdown = {
                            "source": "snapshot",
                            "snapshot_symbol_count": len(raw_syms),
                            "dynamic_symbols_count": len(stock_syms),
                            "static_fallback_symbols_count": 0,
                            "used_static_fallback": False,
                        }
                else:
                    # Snapshot contained only ETF proxies → rebuild live right now.
                    # Never call FMP peers on page load (with_fmp_peers=False) to
                    # avoid slow/blocking API calls that can cause proxy 502s.
                    print(f"[SCREENER_HUB] snapshot for {theme} is ETF-only — rebuilding live (no peers)")
                    try:
                        symbols_map, breakdowns = await asyncio.wait_for(
                            _build_thematic_universe(theme, with_fmp_peers=False),
                            timeout=_THEMATIC_BUILD_TIMEOUT_S,
                        )
                    except asyncio.TimeoutError:
                        print(f"[SCREENER_HUB] live build timed out for {theme}, returning empty")
                        symbols_map, breakdowns = {}, {}
                    symbols = symbols_map.get(theme, [])
                    thematic_breakdown = breakdowns.get(theme, {})
                    snap_status = "live_fallback"
                    universe_source = "live"
                    if symbols:
                        insert_universe_snapshot(
                            universe_type="thematic", theme_key=theme,
                            symbols=symbols, source="etf_only_refresh",
                            status="ok", ttl_days=7,
                            metadata=thematic_breakdown,
                        )
            else:
                # No snapshot yet — build one live from dynamic sources.
                # Never call FMP peers on page load (with_fmp_peers=False) to avoid
                # slow blocking API calls. Peers are fetched by the rebuild job instead.
                try:
                    symbols_map, breakdowns = await asyncio.wait_for(
                        _build_thematic_universe(theme, with_fmp_peers=False),
                        timeout=_THEMATIC_BUILD_TIMEOUT_S,
                    )
                except asyncio.TimeoutError:
                    print(f"[SCREENER_HUB] live build timed out for {theme}, returning empty")
                    symbols_map, breakdowns = {}, {}
                symbols = symbols_map.get(theme, [])
                thematic_breakdown = breakdowns.get(theme, {})
                snap_status = "live_fallback"
                universe_source = "live"
                if symbols:
                    insert_universe_snapshot(
                        universe_type="thematic", theme_key=theme,
                        symbols=symbols, source="live_build",
                        status="ok", ttl_days=7,
                        metadata=thematic_breakdown,
                    )
        else:
            # No theme → select today's daily default theme.
            # Never flatten all themes: that produces 200+ unrelated symbols and
            # is not a useful screener view.
            _def_theme, _def_reason = _get_daily_default_theme()
            if _def_theme:
                theme = _def_theme
                is_default_theme    = True
                default_theme_reason = _def_reason
                # Re-load LKG theme state for the resolved default
                theme_state_meta = _get_theme_state_from_lkg(theme)
                # Load the default theme's snapshot (same path as explicit theme)
                snap = get_latest_universe("thematic", theme)
                if snap and snap.get("symbols"):
                    snap_generated_at = str(snap.get("generated_at") or "") or None
                    snap_expires_at   = str(snap.get("expires_at")   or "") or None
                    snap_db_source    = snap.get("source") or "unknown"
                    raw_syms   = list(snap.get("symbols") or [])
                    stock_syms = [s for s in raw_syms if s not in _ALL_PROXY_ETFS]
                    if stock_syms:
                        symbols = raw_syms
                        snap_meta = snap.get("metadata") or {}
                        thematic_breakdown = snap_meta if isinstance(snap_meta, dict) else {}
                    else:
                        # ETF-only snapshot — rebuild live (fast path, no FMP)
                        print(f"[SCREENER_HUB] default theme {theme!r} snapshot is ETF-only — rebuilding live")
                        try:
                            symbols_map, breakdowns = await asyncio.wait_for(
                                _build_thematic_universe(theme, with_fmp_peers=False),
                                timeout=_THEMATIC_BUILD_TIMEOUT_S,
                            )
                        except asyncio.TimeoutError:
                            symbols_map, breakdowns = {}, {}
                        symbols = symbols_map.get(theme, [])
                        thematic_breakdown = breakdowns.get(theme, {})
                        snap_status    = "live_fallback"
                        universe_source = "live"
                else:
                    # No snapshot for default theme — build live
                    try:
                        symbols_map, breakdowns = await asyncio.wait_for(
                            _build_thematic_universe(theme, with_fmp_peers=False),
                            timeout=_THEMATIC_BUILD_TIMEOUT_S,
                        )
                    except asyncio.TimeoutError:
                        symbols_map, breakdowns = {}, {}
                    symbols = symbols_map.get(theme, [])
                    thematic_breakdown = breakdowns.get(theme, {})
                    snap_status    = "live_fallback"
                    universe_source = "live"
            else:
                # No default theme available (empty registry or all snapshots cold)
                return {
                    "status":              "empty",
                    "error_code":          "NO_DEFAULT_THEME",
                    "message": (
                        "No default theme could be selected. "
                        "Theme snapshots may be cold — run an admin rebuild to populate them."
                    ),
                    "tab":                 tab,
                    "theme":               None,
                    "selected_theme":      None,
                    "is_default_theme":    False,
                    "generated_at":        _now_iso(),
                    "rows":                [],
                    "filters_applied":     {},
                    "rows_before_filters": 0,
                    "rows_after_filters":  0,
                }

        # Strip ETF proxies from the final rows list for thematic tab.
        # They may remain in the cached universe for fundamentals warm-job
        # coverage, but the screener table should show stocks only.
        symbols = [s for s in symbols if s not in _ALL_PROXY_ETFS] or symbols

        # ── Per-theme refresh with 24h durable cap ─────────────────────────────
        # For EXPLICIT user-selected themes (not is_default_theme): refresh runs
        # INLINE (await) so rows are hydrated before this response is returned.
        # For the DEFAULT theme: background refresh only (create_task) — serving
        # cached data immediately is acceptable since the default is pre-warmed.
        #
        # Weak-snapshot detection: screener_meta_by_symbol is empty when the snapshot
        # was built by an ETF/static-only path that did not run the FMP screener.
        # A weak-but-fresh snapshot for an explicit selected theme warrants an inline
        # refresh just as much as a stale snapshot does.
        #
        # Guards (applied in order):
        #   1. In-flight dedup  — if a task is already running/queued, skip.
        #   2. Concurrency cap  — max _FMP_REFRESH_MAX_CONCURRENT simultaneous tasks.
        #   3. 24h durable cap  — in-memory log + DB fallback on restart.
        # A snapshot is "weak" only when it has no FMP metadata AND no seeds.
        # Seed-only themes (semicap_equipment, photonics_lasers, quantum, etc.)
        # legitimately have screener_meta_by_symbol={} — they must not be treated
        # as weak, or they would trigger an infinite inline-refresh loop that times
        # out and permanently returns 0 rows.
        _snap_seed_count = int(thematic_breakdown.get("membership_seed_count", 0) or 0)
        _snap_is_weak    = (
            not bool(thematic_breakdown.get("screener_meta_by_symbol"))
            and _snap_seed_count == 0          # ← only weak when seeds are also absent
        )
        _inline_eligible = not is_default_theme   # explicit selected theme
        theme_last_refreshed_at = _THEME_REFRESH_LOG.get(theme) if theme else None

        # ── Bad-snapshot / schema-version bypass ──────────────────────────────
        # Fires ONCE per theme per session for explicit selected-theme requests
        # when the cached snapshot was built with an old pipeline version OR is
        # genuinely empty (no FMP rows AND no seed rows).
        # Seed-only themes legitimately have fmp_screener_count=0 and must NOT
        # trigger the bypass — otherwise they loop-refresh on every request and
        # always serve stale/empty data.
        _snap_schema         = thematic_breakdown.get("refresh_schema_version", "")
        _snap_fmp_count      = int(thematic_breakdown.get("fmp_screener_count", 0) or 0)
        _fmp_map_themes_cfg  = _load_industry_map_config().get("themes", {})
        _theme_has_fmp       = bool(_fmp_map_themes_cfg.get(theme or "", {}).get("fmp_industries"))
        _snap_truly_empty    = (_snap_fmp_count == 0 and _snap_seed_count == 0)
        _snap_needs_version_upgrade = (
            _inline_eligible
            and theme not in _THEME_VERSION_REFRESHED
            and _theme_has_fmp
            and (_snap_schema != THEMATIC_REFRESH_SCHEMA_VERSION or _snap_truly_empty)
        )

        if tab == "thematic" and theme and snap_generated_at:
            try:
                _snap_dt    = datetime.fromisoformat(snap_generated_at.replace("Z", "+00:00"))
                _snap_age_h = (datetime.now(timezone.utc) - _snap_dt).total_seconds() / 3600
            except Exception:
                _snap_age_h = 0.0
            _needs_refresh = _snap_age_h > _THEME_REFRESH_STALE_H or _snap_is_weak
            if _needs_refresh or _snap_needs_version_upgrade:
                if theme in _THEME_REFRESH_INFLIGHT:
                    theme_refresh_status = "refreshing"
                elif len(_THEME_REFRESH_INFLIGHT) >= _FMP_REFRESH_MAX_CONCURRENT:
                    theme_refresh_status = "refresh_queued"
                    fmp_refresh_used     = False
                    fmp_refresh_reason   = "concurrency_cap"
                elif _theme_refresh_allowed(theme) or _snap_needs_version_upgrade:
                    _refresh_reason = (
                        "snapshot_version_invalid"
                            if (_snap_needs_version_upgrade
                                and _snap_schema != THEMATIC_REFRESH_SCHEMA_VERSION) else
                        "bad_snapshot_bypass"
                            if _snap_needs_version_upgrade else
                        "explicit_theme_weak"
                            if (_snap_is_weak and _inline_eligible) else
                        "explicit_theme_stale"
                            if _inline_eligible else
                        "weak_cache"
                            if _snap_is_weak else
                        "stale"
                    )
                    # Mark version-upgrade bypass consumed BEFORE the refresh so
                    # concurrent requests don't also bypass the cap.
                    if _snap_needs_version_upgrade:
                        _THEME_VERSION_REFRESHED.add(theme)
                        print(
                            f"[SCREENER_HUB] bad-snapshot bypass for theme={theme!r} "
                            f"schema={_snap_schema!r} fmp_count={_snap_fmp_count} "
                            f"reason={_refresh_reason!r}"
                        )
                    if _inline_eligible:
                        # Explicit selected theme: run refresh synchronously so
                        # this response contains hydrated rows, not stale shells.
                        # asyncio.shield keeps the task alive if we hit the timeout,
                        # so the snapshot IS eventually written to DB even when we
                        # fall back to cached data after the 12s window.
                        _THEME_REFRESH_INFLIGHT.add(theme)
                        _refresh_task = asyncio.ensure_future(
                            _background_refresh_theme(theme, reason=_refresh_reason)
                        )
                        try:
                            await asyncio.wait_for(
                                asyncio.shield(_refresh_task),
                                timeout=_INLINE_REFRESH_TIMEOUT_S,
                            )
                        except asyncio.TimeoutError:
                            print(f"[SCREENER_HUB] inline refresh timed out for "
                                  f"theme={theme!r} — serving cached data, "
                                  f"background task still running")
                        except Exception as _ire:
                            print(f"[SCREENER_HUB] inline refresh error for "
                                  f"theme={theme!r}: {_ire}")
                        finally:
                            _THEME_REFRESH_INFLIGHT.discard(theme)
                        _record_theme_refresh(theme)
                        # Reload the refreshed snapshot so rows use updated data.
                        _ref_snap = get_latest_universe("thematic", theme)
                        if _ref_snap and _ref_snap.get("symbols"):
                            _ref_raw   = list(_ref_snap.get("symbols") or [])
                            _ref_stock = [s for s in _ref_raw if s not in _ALL_PROXY_ETFS]
                            symbols    = (_ref_stock if _ref_stock else _ref_raw)
                            _ref_meta  = _ref_snap.get("metadata") or {}
                            if isinstance(_ref_meta, dict):
                                thematic_breakdown = _ref_meta
                            snap_generated_at = (
                                str(_ref_snap.get("generated_at") or "") or snap_generated_at
                            )
                        theme_refresh_status = "refreshed_inline"
                        fmp_refresh_used     = True
                        fmp_refresh_reason   = _refresh_reason
                    else:
                        # Default theme: background refresh, serve cache immediately.
                        asyncio.create_task(
                            _background_refresh_theme(theme, reason=_refresh_reason)
                        )
                        _record_theme_refresh(theme)
                        theme_last_refreshed_at = _THEME_REFRESH_LOG.get(theme)
                        theme_refresh_status    = "refresh_scheduled"
                        fmp_refresh_used        = True
                        fmp_refresh_reason      = _refresh_reason
                else:
                    theme_refresh_status = (
                        "stale_cap_active"
                        if _snap_age_h > _THEME_REFRESH_STALE_H
                        else "fresh"
                    )
            else:
                theme_refresh_status = "fresh"
            # Always compute next-allowed timestamp from the log (post-hydration).
            theme_next_refresh_allowed_at = _theme_refresh_next_allowed_iso(theme)
            theme_last_refreshed_at       = _THEME_REFRESH_LOG.get(theme)
        elif tab == "thematic" and theme and not snap_generated_at:
            theme_refresh_status = "live_build"

    elif tab == "social":
        snap = get_latest_universe("social")
        symbols = list(snap.get("symbols") or []) if snap else []
        if not symbols:
            symbols = _build_social_universe()
            snap_status = "live_fallback"
            universe_source = "live"

    elif tab == "bottlenecks":
        snap = get_latest_universe("bottlenecks")
        if snap and snap.get("symbols"):
            symbols = list(snap.get("symbols") or [])
        else:
            bn_syms, bottlenecks_meta = _build_bottlenecks_universe()
            symbols = bn_syms
            snap_status = "live_fallback"
            universe_source = "live"
        # Always compute metadata so it's present in the response
        if not bottlenecks_meta:
            _, bottlenecks_meta = _build_bottlenecks_universe()

    elif tab == "watchlist_portfolio":
        # Always live — depends on the user's current watchlists.
        symbols = _build_watchlist_portfolio_universe()
        universe_source = "live"

    symbols = _dedupe_filter(symbols)[:_GLOBAL_TICKER_CAP]

    # ── Per-symbol source map ─────────────────────────────────────────────────
    # Thematic: populated by the live build / snapshot metadata.
    # Other tabs: seeded below after cr_details is computed.
    sources_by_symbol: dict[str, list[str]] = thematic_breakdown.get("sources_by_symbol") or {}

    # ── FMP screener metadata — used to enrich rows when FMP cache is cold ────
    # Thematic: use the snapshot's own screener_meta_by_symbol (most precise —
    #   built from FMP screener results for this specific theme).
    # Social / Bottlenecks / Watchlist+Portfolio: use the merged global cache
    #   built from ALL stored thematic snapshots.  Covers any symbol that has
    #   ever appeared in a rebuilt thematic universe, giving those tabs the same
    #   per-symbol enrichment (sector, industry, beta, volume, dollar_volume,
    #   last_annual_dividend, exchange …) as the Thematic tab.
    #   Pure DB read — no FMP calls.
    if tab == "thematic":
        _snap_scr_meta: dict[str, dict] = (
            thematic_breakdown.get("screener_meta_by_symbol") or {}
        )
        # warm_job snapshots (source="warm_job") don't run the FMP screener and
        # therefore write an empty screener_meta_by_symbol.  When the snapshot
        # meta is empty, fall back to the merged global screener meta — which
        # picks the most recent thematic_rebuild snapshot *per theme* that has
        # non-empty screener_meta_by_symbol (fixed in get_all_thematic_screener_meta).
        # This gives ETF-sourced themes the same sector/industry/beta/exchange
        # enrichment as themes that did have FMP screener results, without any
        # FMP call.  Note: mega-caps that have never been in FMP screener results
        # will still show null metadata — screener_fundamentals_cache is the
        # only other source, and it is populated only by the Sunday warm job.
        screener_meta_by_symbol: dict[str, dict] = (
            _snap_scr_meta if _snap_scr_meta else _load_global_screener_meta()
        )
    else:
        screener_meta_by_symbol = _load_global_screener_meta()

    # ── Thematic: load allowed industries for the selected theme (leakage filter) ──
    # Read once from disk (no live FMP call). Used to filter rows whose known
    # industry does not belong to the selected theme (e.g. Solar in Semiconductors).
    _thematic_allowed_industries: set[str] = set()
    if tab == "thematic" and theme:
        _tind_cfg   = _load_industry_map_config()
        _tind_entry = (_tind_cfg.get("themes") or {}).get(theme) or {}
        _thematic_allowed_industries = set(
            (_tind_entry.get("fmp_industries") or [])
            + (_tind_entry.get("adjacent_industries") or [])
        )

    # ── Chain Reaction detail map (bottlenecks only) ────────────────────────────
    # Per-symbol scored node data from chain_reaction_weekly_outputs.rows_json.
    cr_details: dict[str, dict] = bottlenecks_meta.get("details_by_symbol") or {}

    # ── Seed sources_by_symbol for non-thematic tabs ─────────────────────────
    # Thematic sources come from the live/snapshot build (already set above).
    # For other tabs, seed each symbol with the tab's canonical source tag so
    # discovery_sources is always non-empty and meaningful.
    if tab == "social":
        sources_by_symbol = {s: ["social_consensus"] for s in symbols}
    elif tab == "bottlenecks":
        # Prefer CR per-row discovery_sources when available; fall back to generic tag.
        sources_by_symbol = {
            s: list((cr_details.get(s) or {}).get("discovery_sources") or ["chain_reaction"])
            for s in symbols
        }
    elif tab == "watchlist_portfolio":
        sources_by_symbol = {s: ["watchlist_portfolio"] for s in symbols}

    # ── Load real historical returns from cache (never blocks on FMP API) ──────
    returns_cache: dict[str, dict] = get_returns(symbols) if symbols else {}
    returns_cached_count = sum(1 for r in returns_cache.values() if r.get("return_4w") is not None)

    # ── Quotes: serve cache immediately, refresh stale symbols in background ──
    # This eliminates the ~8–12s Tradier block on cold quote caches.
    # The next request receives fresh quotes; this request serves whatever is cached.
    quote_cache_status = "skipped"
    if symbols:
        _qt_ttl    = _QUOTE_TTL_OPEN_S if _is_market_open() else _QUOTE_TTL_CLOSED_S
        _now_ts    = time.time()
        _cached_qs = get_quotes(symbols)
        _stale_syms: list[str] = []
        for _s in symbols:
            _qrow = _cached_qs.get(_s)
            if not _qrow or not _qrow.get("fetched_at"):
                _stale_syms.append(_s)
                continue
            try:
                _fa = datetime.fromisoformat(str(_qrow["fetched_at"]).replace("Z", "+00:00"))
                if _now_ts - _fa.timestamp() > _qt_ttl:
                    _stale_syms.append(_s)
            except Exception:
                _stale_syms.append(_s)
        if _stale_syms:
            asyncio.create_task(refresh_quotes_for_page(symbols))
            quote_refresh_started = True
        _fresh_count = len(symbols) - len(_stale_syms)
        quote_cache_status = (
            "fresh"   if not _stale_syms else
            "partial" if _fresh_count > 0 else
            "cold"
        )

    # ── Read fundamentals + quotes from cache ──
    fundamentals = get_fundamentals(symbols) if symbols else {}
    quotes_map   = get_quotes(symbols) if symbols else {}

    fund_total = len(fundamentals)
    fund_fresh = len(fundamentals_fresh_symbols(symbols, max_age_days=_FUNDAMENTALS_TTL_DAYS))
    fundamentals_cache_status = (
        "fresh" if fund_fresh == len(symbols) and len(symbols) > 0 else
        "partial" if fund_total > 0 else
        "cold"
    )

    # ── Build rows (NEVER omit a symbol just because enrichment failed) ──
    rows: list[dict] = []
    for sym in symbols:
        f = fundamentals.get(sym) or {}
        q_row = quotes_map.get(sym) or {}
        q = q_row.get("quote") if isinstance(q_row.get("quote"), dict) else {}

        profile = f.get("profile") or {}
        metrics = f.get("metrics") or {}
        ratios  = f.get("ratios")  or {}

        # FMP screener metadata — available immediately after rebuild even when
        # the FMP profile cache has not been warmed yet.
        scr_meta = screener_meta_by_symbol.get(sym) or {}

        # Real historical returns from cache (None when cache is cold)
        ret  = returns_cache.get(sym) or {}
        r2w  = ret.get("return_2w")
        r4w  = ret.get("return_4w")
        r10w = ret.get("return_10w")
        rs_status = (
            "real_historical" if r4w is not None else
            "partial"         if (r2w is not None or r10w is not None) else
            "cache_cold"
        )

        classification = _classify_row(
            metrics, q,
            score_mode=score_mode,
            coc_filter=coc_filter,
            return_2w=r2w,
            return_4w=r4w,
            return_10w=r10w,
        )

        # ── Overlap / confirmation flags ──
        is_social    = sym in social_overlap
        is_options   = sym in options_overlap
        is_watchlist = sym in watchlist_set

        # ── Per-row discovery sources ──
        # Merge universe-build source tags with request-time overlap sets.
        disc_src: list[str] = list(sources_by_symbol.get(sym) or [])
        if is_social and "social_overlap" not in disc_src:
            disc_src.append("social_overlap")
        if is_options and "options_overlap" not in disc_src:
            disc_src.append("options_overlap")
        if is_watchlist and "watchlist_portfolio" not in disc_src:
            disc_src.append("watchlist_portfolio")
        if not disc_src:
            disc_src = ["unknown"]

        # ── Derive membership_source / membership_reason from disc_src ─────────
        # Answers "why this ticker belongs in this theme", not "how it ranked
        # today."  Priority-ordered scan across ALL disc_src tags so that a seed
        # ticker that also appears in lkg_leaders correctly reports "seed", and an
        # ETF holding that also appeared in lkg_leaders reports the ETF.
        # lkg_leaders is a ranking/momentum signal, not a theme membership signal,
        # so it only wins when no stronger theme-membership source is present.
        #
        # Priority (highest → lowest):
        #   1. manual_include
        #   2. static_seed     ← always wins for explicitly configured seed tickers
        #   3. etf:<*>         ← theme-specific ETF holding (DRAM, etc.)
        #   4. fmp_screener:*  ← FMP industry screener / peer match
        #   5. lkg_leaders / lkg:<*>  ← only if no stronger source
        #   6. social_consensus / chain_reaction / watchlist_portfolio
        _MEMBERSHIP_PRIORITY: list[tuple[str, object]] = [
            ("manual_include", lambda s: s == "manual_include"),
            ("static_seed",    lambda s: s == "static_seed"),
            ("etf_holding",    lambda s: s.startswith("etf:")),
            ("fmp_screener",   lambda s: s.startswith("fmp_screener:") or s == "fmp_peers" or s.startswith("fmp_profile:")),
            ("lkg",            lambda s: s == "lkg_leaders" or s.startswith("lkg:")),
            ("social",         lambda s: s == "social_consensus"),
            ("chain_reaction", lambda s: s == "chain_reaction"),
            ("watchlist",      lambda s: s == "watchlist_portfolio"),
        ]
        _chosen_src: Optional[str] = None
        for _bucket, _pred in _MEMBERSHIP_PRIORITY:
            for _s in disc_src:
                if _pred(_s):
                    _chosen_src = _s
                    break
            if _chosen_src is not None:
                break
        if _chosen_src is None:
            _chosen_src = disc_src[0] if disc_src else "unknown"

        if _chosen_src == "manual_include":
            membership_source = "manual_include"
            membership_reason = "manually included"
        elif _chosen_src == "static_seed":
            membership_source = "seed"
            membership_reason = "seed ticker"
        elif _chosen_src.startswith("etf:"):
            membership_source = "etf_holding"
            membership_reason = f"{_chosen_src.split(':',1)[1]} ETF holding"
        elif _chosen_src.startswith("fmp_screener:"):
            membership_source = "fmp_screener"
            _ind_name  = _chosen_src.split(":", 1)[1]
            _kw_proof  = (scr_meta.get("_kw_proof") or "").strip()
            _kw_weak   = bool(scr_meta.get("_weak_only", False))
            _is_adj_src = bool(scr_meta.get("_is_adjacent", False))
            membership_reason = (
                f"{'adjacent ' if _is_adj_src else ''}FMP:{_ind_name}"
                + (f" | {'weak ' if _kw_weak else ''}proof: '{_kw_proof}' in name/sector/industry" if _kw_proof else "")
            )
        elif _chosen_src == "fmp_peers":
            membership_source = "fmp_screener"
            membership_reason = "FMP peer match"
        elif _chosen_src.startswith("fmp_profile:"):
            membership_source = "fmp_screener"
            _ind_name  = _chosen_src.split(":", 1)[1]
            _kw_proof  = (scr_meta.get("_kw_proof") or "").strip()
            _kw_weak   = bool(scr_meta.get("_weak_only", False))
            membership_reason = (
                f"FMP profile:{_ind_name}"
                + (
                    f" | {'weak ' if _kw_weak else ''}proof: '{_kw_proof}' in description"
                    if _kw_proof else " | description match"
                )
            )
        elif _chosen_src == "lkg_leaders":
            membership_source = "lkg"
            membership_reason = "LKG momentum leader"
        elif _chosen_src.startswith("lkg:"):
            membership_source = "lkg"
            membership_reason = f"LKG leader ({_chosen_src.split(':',1)[1]})"
        elif _chosen_src == "social_consensus":
            membership_source = "social"
            membership_reason = "social consensus"
        elif _chosen_src == "chain_reaction":
            membership_source = "chain_reaction"
            membership_reason = "supply chain reaction"
        elif _chosen_src == "watchlist_portfolio":
            membership_source = "watchlist"
            membership_reason = "watchlist / portfolio"
        else:
            membership_source = _chosen_src
            membership_reason = _chosen_src.replace("_", " ")

        # Weak-only FMP flag: candidate matched only broad/weak keywords.
        # Used to set confidence="low" and role="emerging" instead of "medium"/"supporting".
        _is_weak_fmp = (
            membership_source == "fmp_screener"
            and bool(scr_meta.get("_weak_only", False))
        )

        # Adjacent row flag: combines _is_adjacent (Source C2 marker) and
        # industry_tier from screener_meta (set by _compute_theme_relevance_score).
        _is_adjacent_row = (
            bool(scr_meta.get("_is_adjacent"))
            or scr_meta.get("industry_tier") in ("adjacent", "weak_adjacent")
        )

        # Resolved candidate_tier: uses scorer override when available.
        _tier_override  = scr_meta.get("candidate_tier_override")
        _candidate_tier = (
            "core"               if membership_source in ("seed", "manual_include") else
            "adjacent_discovery" if _is_adjacent_row and not _is_weak_fmp           else
            "watch_candidate"    if _is_weak_fmp                                    else
            (
                _tier_override
                if _tier_override
                else (
                    "verified_discovery"
                    if membership_source in ("etf_holding", "fmp_screener")
                    else "watch_candidate"
                )
            )
        )

        # ── Row-source tally: bucket each row into its primary source ──────────
        for _src_tag in disc_src:
            _src_key = (
                "etf_holdings" if _src_tag.startswith("etf:") else
                "lkg_leaders"  if _src_tag.startswith("lkg:") else
                _src_tag
            )
            _row_source_tally[_src_key] = _row_source_tally.get(_src_key, 0) + 1

        # ── Market cap: FMP cache first, screener meta fallback ──
        mcap = _to_float(f.get("market_cap") or profile.get("marketCap")
                         or scr_meta.get("market_cap"))
        chg_1d = _to_float(q_row.get("change_percent_1d")) if q_row else None

        # ── Sector / industry: FMP cache first, screener meta fallback ──
        row_sector   = f.get("sector")   or scr_meta.get("sector")   or None
        row_industry = f.get("industry") or scr_meta.get("industry") or None
        row_country  = f.get("country")  or scr_meta.get("country")  or None
        row_exchange = f.get("exchange") or scr_meta.get("exchange") or None

        # ── Company name: FMP profile first, screener meta fallback ──
        row_name = (profile.get("companyName") or profile.get("name")
                    or scr_meta.get("company_name") or sym)

        # ── ETF / fund flags from screener meta (always false for our universe) ──
        row_is_etf  = bool(scr_meta.get("is_etf",  False))
        row_is_fund = bool(scr_meta.get("is_fund",  False))

        # ── Beta: FMP key-metrics cache → FMP profile cache → screener meta ──
        row_beta = (
            _to_float(metrics.get("betaTTM"))
            or _to_float(profile.get("beta"))
            or _to_float(scr_meta.get("beta"))
        )

        # ── Price: Tradier last → quote-cache row price → screener meta ──
        row_price = (
            _to_float(q.get("last"))
            or _to_float(q_row.get("price") if q_row else None)
            or _to_float(scr_meta.get("price"))
        )

        # ── Volume: Tradier average_volume → Tradier daily volume → screener meta ──
        # average_volume (30-day) is a stable metric; daily spot volume is noisy.
        # screener_meta.volume is the FMP-reported daily volume at rebuild time.
        row_volume = (
            _to_float(q.get("average_volume"))
            or _to_float(q.get("volume"))
            or _to_float(scr_meta.get("volume"))
        )

        # ── Exchange ──
        row_exchange = f.get("exchange") or scr_meta.get("exchange") or None

        # ── Last annual dividend from screener meta ──
        row_dividend = _to_float(scr_meta.get("last_annual_dividend"))

        # ── New quality/relevance fields from screener meta ──
        # For screener-sourced rows: computed at rebuild time and stored in scr_meta.
        # For ETF/LKG rows: default to 0.5 (ETF-included → assumed thematically relevant).
        theme_relevance_score = _to_float(scr_meta.get("theme_relevance_score")) or 0.5
        industry_tier         = scr_meta.get("industry_tier") or "unknown"
        quality_flags: list[str] = list(scr_meta.get("quality_flags") or [])

        # ── Dollar volume / volume_to_market_cap / liquidity_status ──
        # Pre-computed at rebuild (FMP screener) is the preferred source for all three.
        # For ETF/LKG-sourced rows (no screener_meta), recompute from Tradier volume + price.
        liq_status = scr_meta.get("liquidity_status") or None
        row_dv     = _to_float(scr_meta.get("dollar_volume"))
        row_vtmc   = _to_float(scr_meta.get("volume_to_market_cap"))
        if liq_status is None or row_dv is None or row_vtmc is None:
            _liq, _dv, _vtmc = _compute_liquidity_status(row_volume, mcap, row_price)
            if liq_status is None: liq_status = _liq
            if row_dv    is None:  row_dv     = _dv
            if row_vtmc  is None:  row_vtmc   = _vtmc
        liq_status = liq_status or "unknown"

        # ── Options enrichment from cached LKG flow data (no live API call) ──
        opt_detail      = options_detail_map.get(sym) or {}
        opts_oi         = opt_detail.get("options_oi")
        opts_prev_oi    = opt_detail.get("previous_options_oi")
        opts_oi_chg     = opt_detail.get("options_oi_change")
        opts_act_scr    = opt_detail.get("options_activity_score")
        # Pre-compute pct + status from LKG data so rows beyond the
        # _OPT_MAX_SYMS DB-enrichment cap still get correct values.
        # _enrich_options_page_aware will override these for the first
        # _OPT_MAX_SYMS symbols with fresher DB cache entries.
        if (opts_oi is not None
                and opts_prev_oi is not None
                and opts_prev_oi > 0
                and opts_oi_chg is not None):
            opts_oi_chg_pct    = round(opts_oi_chg / opts_prev_oi * 100, 4)
            opts_oi_chg_status: Optional[str] = None
        elif opts_oi is not None and opts_prev_oi is None:
            opts_oi_chg_pct    = None
            opts_oi_chg_status = "no_prior_snapshot"
        elif opts_oi is not None and opts_prev_oi == 0:
            opts_oi_chg_pct    = None
            opts_oi_chg_status = "prior_zero"
        else:
            opts_oi_chg_pct    = None
            opts_oi_chg_status = None

        # ── Source confidence: computed from discovery sources + theme relevance ──
        source_conf = _get_source_confidence(disc_src, theme_relevance_score)

        hg_score = _compute_hidden_gem_score(
            distance_52w_high=classification.get("distance_52w_high"),
            volume_surge=classification.get("volume_surge"),
            accumulation=classification.get("accumulation"),
            chg_1d=chg_1d,
            return_2w=r2w,
            return_4w=r4w,
            return_10w=r10w,
            market_cap=mcap,
            is_social=is_social,
            is_options=is_options,
            is_watchlist=is_watchlist,
            theme_relevance_score=theme_relevance_score,
            source_confidence=source_conf,
            liquidity_status=liq_status,
            options_oi=opts_oi,
            dollar_volume=row_dv,
        )
        role = _assign_row_role(
            market_cap=mcap,
            hidden_gem_score=hg_score,
            is_social=is_social,
            is_options=is_options,
            is_watchlist=is_watchlist,
            discovery_sources=disc_src,
        )

        row = {
            # ── Base fields (Part 8 spec) ──
            "symbol":              sym,
            "name":                row_name,
            "company_name":        row_name,
            "market_cap":          mcap,
            "sector":              row_sector,
            "industry":            row_industry,
            "beta":                row_beta,
            "price":               row_price,
            "last_annual_dividend": row_dividend,
            "volume":              row_volume,
            "exchange":            row_exchange,
            # ── Calculated ──
            "dollar_volume":         row_dv,
            "volume_to_market_cap":  row_vtmc,
            # ── Live/quote ──
            "change_percent_1d":   q_row.get("change_percent_1d"),
            # ── Signals ──
            "volume_surge":        classification["volume_surge"],
            "accumulation":        classification["accumulation"],
            "hidden_gem_score":    hg_score,
            "score":               classification["score"],
            "role":                role,
            # ── Options (LKG fallback; overridden by _enrich_options_page_aware) ──
            "options_oi":              opts_oi,
            "previous_options_oi":     opts_prev_oi,
            "options_oi_change":       opts_oi_chg,
            "options_oi_change_pct":   opts_oi_chg_pct,
            "options_oi_change_status": opts_oi_chg_status,
            "options_activity_score":  opts_act_scr,
            "options_updated_at":      None,
            "options_source":          "lkg" if opts_oi is not None else "unavailable",
            # ── Metadata/debug ──
            "discovery_sources":     disc_src,
            "membership_source":     membership_source,
            "membership_reason":     membership_reason,
            "membership_confidence": (
                scr_meta.get("membership_confidence_override")
                or (
                    "high"   if membership_source in ("seed", "manual_include") else
                    "low"    if _is_weak_fmp else
                    "medium" if membership_source in ("etf_holding", "fmp_screener") else
                    "low"
                )
            ),
            "theme_role": (
                "core"       if membership_source in ("seed", "manual_include") else
                "emerging"   if _is_weak_fmp else
                "adjacent"   if _is_adjacent_row                                else
                "supporting" if membership_source in ("etf_holding", "fmp_screener") else
                "emerging"
            ),
            "candidate_tier":   _candidate_tier,
            "matched_keywords": scr_meta.get("_all_matched_kws") or [],
            "quality_flags":         quality_flags,
            "source_confidence":     source_conf,
            "rs_data_status":        rs_status,
            # ── Legacy compat fields (kept for non-thematic tabs + existing frontend) ──
            "history":               None,
            "category":              classification["category"],
            "rs_0_2w":               classification["rs_0_2w"],
            "rs_0_4w":               classification["rs_0_4w"],
            "rs_0_10w":              classification["rs_0_10w"],
            "rs_accel":              classification["rs_accel"],
            "performance_2w":        r2w,
            "performance_4w":        r4w,
            "performance_10w":       r10w,
            "distance_52w_high":     classification["distance_52w_high"],
            "coc":                   classification["coc"],
            "market_cap_bucket":     _market_cap_bucket(mcap),
            "is_etf":                row_is_etf,
            "is_fund":               row_is_fund,
            "performance_7d":        None,
            "performance_30d":       None,
            "performance_ytd":       None,
            "performance_1y":        None,
            "semantic_score":         theme_relevance_score,
            "theme_relevance_score": theme_relevance_score,
            "industry_tier":         industry_tier,
            "liquidity_status":      liq_status,
            # ── Canonical theme classification (from THEME_RS_UNIVERSE) ──────
            # Same taxonomy as the Themes page — no LLM invented names.
            "canonical_theme_name":  _get_canonical_theme_name(sym),
            "canonical_theme_id":    _get_canonical_theme_id(sym),
            "theme_source":          "canonical",
            "_meta": {
                "country":                 row_country,
                "exchange":                row_exchange,
                "beta":                    row_beta,
                "volume":                  row_volume,
                "fundamentals_fetched_at": f.get("fetched_at"),
                "quote_fetched_at":        q_row.get("fetched_at"),
                "fundamentals_provider":   f.get("provider"),
                "quote_provider":          q_row.get("provider"),
                "ratios_pe":               ratios.get("priceEarningsRatioTTM"),
                "ratios_ps":               ratios.get("priceToSalesRatioTTM"),
                "key_metric_roe":          metrics.get("roeTTM"),
                "signals":                 classification.get("_signals"),
                "returns_fetched_at":      ret.get("fetched_at"),
                "bars_count":              ret.get("bars_count"),
                "screener_meta_source":    scr_meta.get("discovery_source"),
            },
        }

        # ── Bottlenecks: enrich with Chain Reaction scored node detail ──────────
        if tab == "bottlenecks" and cr_details:
            cr = cr_details.get(sym) or {}
            if cr:
                row["bottleneck_ticker"]      = cr.get("bottleneck_ticker") or sym
                row["company_name"]           = cr.get("company_name") or row.get("name")
                row["anchor_ticker"]          = cr.get("anchor_ticker")
                row["anchor_theme"]           = cr.get("anchor_theme")
                row["supply_chain_role"]      = cr.get("supply_chain_role")
                row["bottleneck_type"]        = cr.get("bottleneck_type")
                row["layer"]                  = cr.get("layer")
                row["themes"]                 = cr.get("themes") or []
                row["theme_alignment_score"]  = cr.get("theme_alignment_score")
                row["bottleneck_score"]       = cr.get("bottleneck_score")
                row["momentum_score"]         = cr.get("momentum_score")
                row["volume_score"]           = cr.get("volume_score")
                row["fundamental_score"]      = cr.get("fundamental_score")
                row["social_score"]           = cr.get("social_score")
                row["options_score"]          = cr.get("options_score")
                row["final_score"]            = cr.get("final_score")
                row["evidence"]               = cr.get("evidence") or []
                # Merge CR discovery_sources with overlap-set tags already in disc_src
                cr_sources = cr.get("discovery_sources") or []
                for src in cr_sources:
                    if src and src not in row["discovery_sources"]:
                        row["discovery_sources"].append(src)

        # ── Thematic-only: wrong-theme industry leakage filter (Part 2) ──────
        # Exclude rows whose known industry is outside this theme's allowed set.
        #
        # Important: FMP uses TWO different industry taxonomies:
        #   • Screener API  → "Semiconductor Equipment & Materials"
        #   • Profile/fundamentals API → "Semiconductors"  (broader bucket)
        # _thematic_allowed_industries is built from the screener taxonomy (fmp_industries
        # in config).  row_industry comes from the fundamentals cache (profile taxonomy),
        # so they MISMATCH for many valid tickers.  scr_meta.get("industry") uses the
        # screener taxonomy and should be preferred when available.
        #
        # Seeds and manual_include tickers are TRUSTED — they must never be filtered
        # here, regardless of what FMP's fundamentals API says their industry is.
        #
        # Rows with no industry at all (cache cold) pass through unchanged.
        _filter_industry = scr_meta.get("industry") or row_industry
        if (
            tab == "thematic"
            and _thematic_allowed_industries
            and _filter_industry
            and membership_source not in ("seed", "manual_include")
        ):
            if _filter_industry not in _thematic_allowed_industries:
                continue

        if not _row_passes_filters(row, category_filter=category, coc_filter=coc_filter):
            continue
        rows.append(row)

    # ── Thematic: non-equity exclusion (Part 3) ───────────────────────────────
    # Remove money market funds, FX/currency proxies, and zero-enriched ETF cash
    # placeholders before exposing rows to user-specified filters.
    # Applied only to the thematic tab; other tabs can contain ETF rows legitimately.
    if tab == "thematic":
        _pre_ne = len(rows)
        rows    = [r for r in rows if not _is_non_equity_row(r)]
        rows_excluded_non_equity = _pre_ne - len(rows)
        if rows_excluded_non_equity:
            print(
                f"[SCREENER_HUB] non_equity exclusion: removed {rows_excluded_non_equity} "
                f"placeholder rows for theme={theme!r}"
            )

    # ── Post-build cache-only filters (no FMP, no rebuild) ────────────────────
    rows_before_filters = len(rows)

    # Part 2: when a market cap filter is active, null market_cap rows are EXCLUDED.
    # "Unknown market cap" is not the same as "under $10B" — do not let placeholder
    # instruments pass through a user's market cap filter.
    _mcap_filter_active = (market_cap_min is not None or market_cap_max is not None)

    if market_cap_min is not None:
        _before = len(rows)
        rows = [
            r for r in rows
            if r.get("market_cap") is not None and (r.get("market_cap") or 0) >= market_cap_min
        ]
        rows_excluded_missing_market_cap += _before - len(rows)
        filters_applied["marketCapMin"] = market_cap_min
    if market_cap_max is not None:
        _before = len(rows)
        rows = [
            r for r in rows
            if r.get("market_cap") is not None and (r.get("market_cap") or 0) <= market_cap_max
        ]
        rows_excluded_missing_market_cap += _before - len(rows)
        filters_applied["marketCapMax"] = market_cap_max
    if min_volume is not None:
        _before = len(rows)
        rows = [
            r for r in rows
            if r.get("volume") is not None and (r.get("volume") or 0) >= min_volume
        ]
        rows_excluded_missing_volume += _before - len(rows)
        filters_applied["min_volume"] = min_volume
    if exchange:
        _exch = exchange.upper()
        rows = [
            r for r in rows
            if r.get("exchange") is None or (r.get("exchange") or "").upper() == _exch
        ]
        filters_applied["exchange"] = _exch
    rows_after_filters = len(rows)

    # ── Sort bottlenecks by final_score → bottleneck_score → hidden_gem_score ──
    if tab == "bottlenecks" and cr_details:
        rows.sort(
            key=lambda r: (
                r.get("final_score") or 0,
                r.get("bottleneck_score") or 0,
                r.get("hidden_gem_score") or 0,
            ),
            reverse=True,
        )

    # ── Screen quality check (Part 4) ─────────────────────────────────────────
    # Compute per-field coverage so the frontend can show an appropriate warning
    # when the active theme/filter combination yields too few screenable rows.
    _screen_quality: dict[str, Any] = {}
    _low_result_quality = False
    _result_quality_warning: Optional[str] = None

    if tab == "thematic":
        _n = len(rows)
        _rw_mcap   = sum(1 for r in rows if r.get("market_cap") is not None)
        _rw_price  = sum(1 for r in rows if r.get("price")      is not None)
        _rw_sector = sum(1 for r in rows if r.get("sector")     is not None)
        _rw_vol    = sum(1 for r in rows if r.get("volume")     is not None)
        _meta_cov  = round(_rw_mcap / max(_n, 1) * 100, 1)

        _screen_quality = {
            "returned_rows":      _n,
            "rows_with_market_cap": _rw_mcap,
            "rows_with_price":    _rw_price,
            "rows_with_sector":   _rw_sector,
            "rows_with_volume":   _rw_vol,
            "metadata_coverage_pct": _meta_cov,
        }

        # Additional quality fields
        _rw_industry  = sum(1 for r in rows if r.get("industry")  is not None)
        _rw_exchange  = sum(1 for r in rows if r.get("exchange")  is not None)
        _screen_quality["rows_with_industry"] = _rw_industry
        _screen_quality["rows_with_exchange"] = _rw_exchange

        if _n == 0:
            _low_result_quality = True
            _result_quality_warning = (
                "No screenable rows found for this theme under the active filters. "
                "Try a broader market cap range or remove filters. "
                "A background refresh has been scheduled if the 24-hour cap allows it."
            )
        elif _n < 10 or _meta_cov < 50.0:
            _low_result_quality = True
            _result_quality_warning = (
                f"This theme has only {_n} screenable cached "
                f"{'stock' if _n == 1 else 'stocks'} under the active filters "
                f"(metadata coverage: {_meta_cov:.0f}%). "
                "Try a broader market cap filter or check back after the daily cache updates."
            )

        # ── Row-floor rescue audit log ─────────────────────────────────────
        # When the snapshot had a meaningful universe (≥ 10 symbols) but
        # post-filter rows collapsed to < 5, emit a structured log so ops
        # can diagnose over-filtering.  FMP soft-downgrade candidates that
        # survived as membership_confidence="low" count toward rescued_count.
        _snap_count = len(raw_syms) if raw_syms is not None else 0
        if tab == "thematic" and _snap_count >= 10 and _n < 5:
            _rescued = sum(
                1 for r in rows
                if (r.get("membership_confidence") or "") == "low"
            )
            print(
                f"[SCREENER_HUB] row_floor_rescue theme={theme!r} "
                f"snap_symbols={_snap_count} rows_after_filters={_n} "
                f"rescued_count={_rescued} pass_reason='row_floor_rescue'"
            )
            _screen_quality["row_floor_rescue"]               = True
            _screen_quality["row_floor_rescue_rescued_count"] = _rescued

        # ── Weak-cache trigger: fire background refresh even when snapshot is
        # "fresh" (< 24h old) but fundamentals coverage is poor.  This covers
        # warm_job and on_demand_refresh snapshots built without FMP screener
        # that returned no screener_meta_by_symbol.
        # Same three guards (in-flight dedup, concurrency cap, 24h cap) apply.
        if (
            _low_result_quality
            and theme_refresh_status not in ("refresh_scheduled", "stale_cap_active",
                                             "refreshing", "refresh_queued", "weak_cache")
        ):
            if theme in _THEME_REFRESH_INFLIGHT:
                theme_refresh_status = "refreshing"
            elif len(_THEME_REFRESH_INFLIGHT) >= _FMP_REFRESH_MAX_CONCURRENT:
                theme_refresh_status = "refresh_queued"
                fmp_refresh_used     = False
                fmp_refresh_reason   = "concurrency_cap"
            elif _theme_refresh_allowed(theme):
                asyncio.create_task(_background_refresh_theme(theme, reason="weak_cache"))
                _record_theme_refresh(theme)
                theme_refresh_status          = "weak_cache"
                fmp_refresh_used              = True
                fmp_refresh_reason            = "metadata_coverage_low"
                theme_next_refresh_allowed_at = _theme_refresh_next_allowed_iso(theme)
                theme_last_refreshed_at       = _THEME_REFRESH_LOG.get(theme)

        # Post-trigger: if the DB fallback hydrated _THEME_REFRESH_LOG during
        # this request (fresh snapshot, cap query, weak-cache check), make sure
        # the response fields reflect it so callers see last/next timestamps.
        if theme_last_refreshed_at is None:
            _post_log = _THEME_REFRESH_LOG.get(theme)
            if _post_log:
                theme_last_refreshed_at       = _post_log
                theme_next_refresh_allowed_at = _theme_refresh_next_allowed_iso(theme)

    # ── Page-aware options enrichment (Tradier, cache-first, non-blocking) ──────
    # Runs after rows are fully built and filtered so only active page symbols
    # are refreshed.  Mutates rows in-place; never crashes the endpoint.
    try:
        await _enrich_options_page_aware(rows)
    except Exception as _opt_exc:
        print(f"[SCREENER_HUB] options enrichment non-fatal error: {_opt_exc}")

    # ── Canonical quote overlay ────────────────────────────────────────────────
    # Final step: overlay price / change_percent_1d / volume / quote_source /
    # quote_is_stale from the shared in-memory quote:lkg cache so Screener Hub
    # always shows the same live Tradier data as Home Watchlist Snapshot,
    # Watchlist Ticker Table, and Portfolio.
    #
    # Pass 3 (targeted Tradier fetch) is enabled ONLY when a specific theme is
    # selected by the user (tab=thematic + explicit theme key).  All other tabs
    # (watchlist_portfolio, default/all views) receive selected_theme=None and
    # therefore skip Pass 3 — their tickers are already covered by Pass 1+2
    # from the Home/Watchlist/Portfolio canonical cache.
    _pass3_theme = theme if (tab == "thematic" and theme) else None
    await _overlay_canonical_quotes_inplace(rows, selected_theme=_pass3_theme)

    _served_at = _now_iso()

    payload: dict[str, Any] = {
        "status": "ok",
        "tab": tab,
        "theme": theme,
        # ── Theme selection metadata ──────────────────────────────────────────
        "selected_theme":            theme,
        "selected_theme_label": (
            _get_theme_label(theme) if theme else None
        ),
        "is_default_theme":          is_default_theme,
        "default_theme_reason":      default_theme_reason,
        # ── Per-theme refresh status ──────────────────────────────────────────
        "theme_refresh_status":         theme_refresh_status,
        "theme_last_refreshed_at":      theme_last_refreshed_at,
        "theme_next_refresh_allowed_at": theme_next_refresh_allowed_at,
        # ── Default-screen specific refresh metadata ──────────────────────────
        # default_last_refreshed_at = when the default theme's universe was last
        # built (any source: warm_job, on_demand_refresh, live_build).
        # next_default_refresh_at   = built_at + 24h (the universe is considered
        # stale and will trigger a background FMP refresh after that point).
        "default_last_refreshed_at": (
            snap_generated_at if is_default_theme else None
        ),
        "next_default_refresh_at": (
            (
                lambda _dt: datetime.fromtimestamp(
                    _dt.timestamp() + _THEME_REFRESH_STALE_H * 3600, tz=timezone.utc
                ).isoformat()
            )(datetime.fromisoformat(snap_generated_at.replace("Z", "+00:00")))
            if is_default_theme and snap_generated_at else None
        ),
        # ── FMP refresh context ───────────────────────────────────────────────
        "fmp_refresh_used":             fmp_refresh_used,
        "fmp_refresh_reason":           fmp_refresh_reason,
        # ── Concurrency guard metadata ────────────────────────────────────────
        "theme_refresh_inflight_count":    len(_THEME_REFRESH_INFLIGHT),
        "theme_refresh_concurrency_limit": _FMP_REFRESH_MAX_CONCURRENT,
        # backward-compat: keep generated_at = request time
        "generated_at":              _served_at,
        "served_at":                 _served_at,
        "fundamentals_cache_status": fundamentals_cache_status,
        "quote_cache_status":        quote_cache_status,
        "quote_refresh_mode":        "cache_first_background",
        "quote_refresh_started":     quote_refresh_started,
        "universe_source":           universe_source,
        "universe_status":           snap_status,
        "row_count":                 len(rows),
        "rows": rows,
        # ── Filter policy ─────────────────────────────────────────────────────
        "filter_policy": {
            "unknown_market_cap": (
                "excluded_when_market_cap_filter_active"
                if _mcap_filter_active else "pass_through"
            ),
            "unknown_volume": (
                "excluded_when_volume_filter_active"
                if min_volume is not None else "pass_through"
            ),
            "unknown_exchange": "pass_through",
            "note": (
                "When marketCapMin/marketCapMax or minVolume is active, rows with "
                "missing market_cap or volume are excluded — unknown values are not "
                "'under the limit'. Exchange filter passes through unknown values."
            ),
        },
        "unknown_market_cap_policy": (
            "excluded_when_market_cap_filter_active"
            if _mcap_filter_active else "pass_through"
        ),
        "unknown_volume_policy": (
            "excluded_when_volume_filter_active"
            if min_volume is not None else "pass_through"
        ),
        "unknown_exchange_policy": "pass_through",
        "rows_excluded_missing_market_cap": rows_excluded_missing_market_cap,
        "rows_excluded_missing_volume":     rows_excluded_missing_volume,
        "rows_excluded_missing_exchange":   0,
        # ── Non-equity exclusion (thematic tab) ───────────────────────────────
        "non_equity_policy":        "excluded_from_thematic_stock_screen",
        "rows_excluded_non_equity": rows_excluded_non_equity,
        # ── Screen quality (thematic tab) ─────────────────────────────────────
        "screen_quality":        _screen_quality if _screen_quality else None,
        "low_result_quality":    _low_result_quality,
        "result_quality_warning": _result_quality_warning,
        # ── Row-source breakdown ───────────────────────────────────────────────
        "row_source_breakdown":  _row_source_tally if _row_source_tally else None,
        "options_columns_info": {
            "options_oi":
                "Current total open interest contracts from cached Tradier options data for this symbol.",
            "options_oi_change_pct":
                "Percent change in total open interest versus the previous cached options snapshot. "
                "Percentage points: 12.5 = +12.5%, -8.2 = -8.2%. "
                "Null when no prior snapshot exists or prior OI was zero (see options_oi_change_status).",
            "options_activity_score":
                "Internal score (0–10) based on cached options activity signals such as "
                "open interest, change, and volume where available.",
            "options_updated_at":
                "Timestamp of the latest cached options snapshot used for this row.",
        },
    }

    # ── Real snapshot freshness fields ───────────────────────────────────────
    if snap_generated_at:
        payload["universe_built_at"] = snap_generated_at
        try:
            _snap_dt  = datetime.fromisoformat(snap_generated_at.replace("Z", "+00:00"))
            _age_h    = (datetime.now(timezone.utc) - _snap_dt).total_seconds() / 3600
            payload["universe_age_hours"] = round(_age_h, 2)
        except Exception:
            pass
    if snap_expires_at:
        payload["universe_expires_at"] = snap_expires_at
    if snap_db_source:
        payload["universe_db_source"] = snap_db_source
    payload["next_rebuild_at"] = _next_sunday_rebuild_et()

    # ── Post-build filter metadata ───────────────────────────────────────────
    payload["filters_applied"]     = filters_applied
    payload["rows_before_filters"] = rows_before_filters
    payload["rows_after_filters"]  = rows_after_filters

    # ── Thematic tab metadata ───────────────────────────────────────────────────
    if tab == "thematic":
        # discovery_mode + hidden_gem_method_version always present on thematic tab
        payload["discovery_mode"]            = "dynamic"
        payload["hidden_gem_method_version"] = "v1"
        payload["returns_cached_count"]      = returns_cached_count
        payload["rs_data_status"] = (
            "real_historical" if returns_cached_count > 0 else "no_historical_cache"
        )

        if thematic_breakdown:
            # Expanded source breakdown — fmp_screener uses both keys for compatibility
            fmp_scr_count = thematic_breakdown.get("fmp_screener_count",
                            thematic_breakdown.get("sector_screener_count", 0))
            payload["universe_source_breakdown"] = {
                "etf_holdings":               thematic_breakdown.get("etf_holdings_count", 0),
                "lkg_leaders":                thematic_breakdown.get("lkg_leaders_count", 0),
                "fmp_peers":                  thematic_breakdown.get("fmp_peers_count", 0),
                "fmp_screener":               fmp_scr_count,
                "social_overlap":             len(social_overlap),
                "options_overlap":            len(options_overlap),
                "watchlist_portfolio_overlap": len(watchlist_set),
                "static_seed":                thematic_breakdown.get("static_seed_count", 0),
            }
            if thematic_breakdown.get("sources_attempted"):
                payload["sources_attempted"] = thematic_breakdown["sources_attempted"]
            if thematic_breakdown.get("sources_failed"):
                payload["sources_failed"] = thematic_breakdown["sources_failed"]
            payload["dynamic_symbols_count"]          = thematic_breakdown.get("dynamic_symbols_count", 0)
            payload["static_fallback_symbols_count"]  = thematic_breakdown.get("static_fallback_symbols_count", 0)
            payload["used_static_fallback"]           = thematic_breakdown.get("used_static_fallback", False)
            if thematic_breakdown.get("etf_files_found"):
                payload["etf_files_used"] = thematic_breakdown["etf_files_found"]
            # FMP screener provenance fields
            if thematic_breakdown.get("fmp_screener_industries_attempted") is not None:
                payload["fmp_screener_industries_attempted"] = thematic_breakdown["fmp_screener_industries_attempted"]
                payload["fmp_screener_symbols_added"]        = thematic_breakdown.get("fmp_screener_symbols_added", 0)
                payload["fmp_screener_calls_used"]           = thematic_breakdown.get("fmp_screener_calls_used", 0)
                payload["fmp_screener_errors"]               = thematic_breakdown.get("fmp_screener_industries_errored", [])
                payload["industry_map_version"]              = thematic_breakdown.get("industry_map_version", "none")
        else:
            # Snapshot hit — overlaps still available for disclosure; counts from snapshot metadata
            snap_bd = thematic_breakdown  # may be {} but that's fine
            payload["universe_source_breakdown"] = {
                "etf_holdings": snap_bd.get("etf_holdings_count", 0),
                "lkg_leaders":  snap_bd.get("lkg_leaders_count", 0),
                "fmp_peers":    snap_bd.get("fmp_peers_count", 0),
                "fmp_screener": snap_bd.get("fmp_screener_count",
                                snap_bd.get("sector_screener_count", 0)),
                "social_overlap":             len(social_overlap),
                "options_overlap":            len(options_overlap),
                "watchlist_portfolio_overlap": len(watchlist_set),
                "static_seed": snap_bd.get("static_seed_count", 0),
            }
        # Schema version + FMP screener metadata — always present for thematic tab
        payload["refresh_schema_version"] = thematic_breakdown.get("refresh_schema_version", "none")
        payload["fmp_screener_used"]      = bool(
            thematic_breakdown.get("fmp_screener_used")
            or (thematic_breakdown.get("fmp_screener_calls_used", 0) or 0) > 0
        )
        payload["fmp_industries_used"]    = thematic_breakdown.get("fmp_industries_used", [])

        # For empty/thin themes, add informational error_code + message
        if not rows and tab == "thematic":
            payload["status"]     = "partial" if symbols else "empty"
            payload["error_code"] = "NO_ROWS_AFTER_FILTERS" if symbols else "EMPTY_UNIVERSE"
            payload["message"]    = (
                "No rows matched active filters for this theme. "
                "Try removing category or CoC filters, or run an admin rebuild."
            ) if symbols else (
                "No universe found for this theme. "
                "Run an admin rebuild to populate it via ETF holdings and FMP screener."
            )

        if theme_state_meta:
            payload["theme_state"]        = theme_state_meta.get("state")
            payload["theme_state_reason"] = theme_state_meta.get("state_reason")
            payload["theme_rs_score"]     = theme_state_meta.get("rs_score")

        # ── Metadata coverage warning (thematic tab only) ─────────────────────
        # Exposes per-theme fundamentals coverage so the frontend can warn when
        # the selected theme's rows will mostly have blank market_cap/sector/
        # industry/beta/exchange (e.g. healthcare: XLV/IYH hold mega-caps that
        # have never been in FMP screener results and aren't in fundamentals cache).
        # Single targeted DB query; ~5–20ms; non-fatal.
        _MIN_ELIG = 20.0  # % of snapshot symbols with market_cap in $50M–$10B
        if theme:
            try:
                from data.screener_hub_store import _get_conn as _cov_get, _put_conn as _cov_put
                _cov_conn = _cov_get()
                _cov_cur  = _cov_conn.cursor()
                _cov_cur.execute("""
                    WITH latest_snap AS (
                        SELECT DISTINCT ON (theme_key)
                               symbols_json
                        FROM public.screener_universe_snapshots
                        WHERE universe_type = 'thematic' AND theme_key = %s
                          AND symbols_json IS NOT NULL
                        ORDER BY theme_key, generated_at DESC
                        LIMIT 1
                    ),
                    snap_syms AS (
                        SELECT jsonb_array_elements_text(symbols_json::jsonb) AS sym
                        FROM latest_snap
                    ),
                    cov AS (
                        SELECT COUNT(*)                                               AS total,
                               COUNT(fc.symbol)                                       AS fund_covered,
                               COUNT(CASE WHEN fc.market_cap IS NOT NULL
                                           AND fc.market_cap BETWEEN 50000000
                                                                  AND 10000000000
                                         THEN 1 END)                                 AS sm_fund
                        FROM snap_syms ss
                        LEFT JOIN public.screener_fundamentals_cache fc
                               ON fc.symbol = ss.sym
                    )
                    SELECT total, fund_covered, sm_fund FROM cov
                """, (theme,))
                _cov_row = _cov_cur.fetchone()
                _cov_cur.close()
                _cov_put(_cov_conn)
                if _cov_row:
                    _cov_total, _cov_fund, _cov_sm = _cov_row
                    _fund_pct = round(_cov_fund / _cov_total * 100, 1) if _cov_total else 0.0
                    _elig_pct = round(_cov_sm   / _cov_total * 100, 1) if _cov_total else 0.0
                    payload["fund_coverage_pct"]          = _fund_pct
                    payload["eligible_fund_coverage_pct"] = _elig_pct
                    payload["low_metadata_coverage"]      = _elig_pct < _MIN_ELIG
                    if _elig_pct < _MIN_ELIG:
                        payload["metadata_coverage_warning"] = (
                            f"Theme '{theme}' snapshot contains mostly large-cap "
                            f"symbols outside the $50M\u2013$10B metadata window "
                            f"(eligible_pct={_elig_pct:.1f}%). "
                            "Rows will show blank market_cap/sector/industry/beta/"
                            "exchange until the Sunday warm_fundamentals job runs "
                            "for these symbols."
                        )
            except Exception as _cov_e:
                print(f"[SCREENER_HUB] theme coverage warning non-fatal: {_cov_e}")

    # ── Bottlenecks tab metadata ────────────────────────────────────────────────
    if tab == "bottlenecks" and bottlenecks_meta:
        payload["is_dynamic"]    = bottlenecks_meta.get("is_dynamic", False)
        payload["source_registry"] = bottlenecks_meta.get("source_registry", "NODE_REGISTRY")
        payload["last_dynamic_chain_reaction_run"] = bottlenecks_meta.get("last_dynamic_chain_reaction_run")
        payload["bottlenecks_source_note"] = bottlenecks_meta.get("note")
        if bottlenecks_meta.get("dynamic_rows_count") is not None:
            payload["dynamic_rows_count"] = bottlenecks_meta["dynamic_rows_count"]
        if bottlenecks_meta.get("week_start"):
            payload["chain_reaction_week_start"] = bottlenecks_meta["week_start"]

    # ── Query cache write (thematic tab + selected theme + non-empty result) ─
    if _qcache_key and tab == "thematic" and theme and payload.get("rows_after_filters", 0) > 0:
        try:
            _filters_snapshot = {
                k: payload.get(k) for k in (
                    "market_cap_min", "market_cap_max", "min_volume", "exchange",
                    "category", "coc_filter",
                )
                if payload.get(k) is not None
            }
            _ok, _exp = set_query_cache(
                cache_key=_qcache_key,
                tab=tab,
                theme_key=theme,
                filters_json=_filters_snapshot,
                query_params_json={
                    "score_mode":       score_mode,
                    "market_cap_min":   market_cap_min,
                    "market_cap_max":   market_cap_max,
                    "min_volume":       min_volume,
                    "exchange":         exchange,
                    "category":         category,
                    "coc_filter":       coc_filter,
                },
                response_json=payload,
                row_count=int(payload.get("rows_after_filters", 0)),
                refresh_schema_version=THEMATIC_REFRESH_SCHEMA_VERSION,
                metadata_json={
                    "universe_built_at": payload.get("universe_built_at"),
                    "theme_refresh_status": payload.get("theme_refresh_status"),
                    "fmp_screener_used": payload.get("fmp_screener_used"),
                },
                ttl_days=7,
            )
            if _ok:
                payload["query_cache_status"]     = "miss_stored"
                payload["query_cache_key"]        = _qcache_key
                payload["query_cache_expires_at"] = _exp
                print(
                    f"[SCREENER_QUERY_CACHE] STORED theme={theme!r} "
                    f"rows={payload.get('rows_after_filters')} "
                    f"expires={_exp}"
                )
            else:
                payload["query_cache_status"] = "miss_store_failed"
                payload["query_cache_key"]    = _qcache_key
        except Exception as _qwe:
            print(f"[SCREENER_QUERY_CACHE] write error (non-fatal): {_qwe}")
            payload["query_cache_status"] = "miss_store_error"
    elif _qcache_key and tab == "thematic" and theme:
        # Ran the pipeline but result was empty — don't cache empty results
        payload["query_cache_status"] = "miss_not_stored_empty"
        payload["query_cache_key"]    = _qcache_key
    else:
        payload["query_cache_status"] = _qcache_status

    return payload


# ── Rebuild orchestration (universes + warm jobs) ─────────────────────────────

async def rebuild_universe(
    tab: str,
    *,
    theme: Optional[str] = None,
    force: bool = False,
) -> dict:
    """Rebuild a single tab's universe snapshot(s).

    For thematic, rebuilds *all* themes when theme is None.
    """
    tab = tab.strip().lower()
    out: dict[str, Any] = {"tab": tab, "theme": theme, "force": bool(force)}

    if tab == "thematic":
        # with_fmp_peers=True only for single-theme rebuilds (not bulk).
        # with_fmp_screener=True here — rebuild jobs are the right place to call
        # the sector screener (FMP stock-screener by industry) for hidden-gem discovery.
        symbols_map, breakdowns = await _build_thematic_universe(
            theme,
            with_fmp_peers=(theme is not None),
            with_fmp_screener=True,
        )
        out["themes_built"] = []
        for k, syms in symbols_map.items():
            bd = breakdowns.get(k, {})
            ok = insert_universe_snapshot(
                universe_type="thematic", theme_key=k,
                symbols=syms, source="thematic_rebuild",
                status="ok", ttl_days=8,
                metadata=bd,
            )
            # Expire query cache so stale responses from the previous
            # (smaller) snapshot are not served after the rebuild.
            try:
                from data.screener_hub_store import expire_theme_query_cache
                expire_theme_query_cache(k)
            except Exception as _exp_err:
                print(f"[SCREENER_HUB] rebuild expire_theme_query_cache {k!r}: {_exp_err}")
            out["themes_built"].append({
                "theme":               k,
                "symbols_count":       len(syms),
                "ok":                  ok,
                "dynamic_count":       bd.get("dynamic_symbols_count", 0),
                "static_count":        bd.get("static_seed_count", 0),
                "used_static_fallback": bd.get("used_static_fallback", False),
                "fmp_screener_industries_attempted": bd.get("fmp_screener_industries_attempted", []),
                "fmp_screener_symbols_added":        bd.get("fmp_screener_symbols_added", 0),
                "fmp_screener_calls_used":           bd.get("fmp_screener_calls_used", 0),
                "fmp_screener_errors":               bd.get("fmp_screener_industries_errored", []),
                "fmp_screener_filtered_reasons":     bd.get("fmp_screener_filtered_reasons", {}),
                "industry_map_version":              bd.get("industry_map_version", "none"),
            })
    elif tab == "social":
        syms = _build_social_universe()
        ok = insert_universe_snapshot(
            universe_type="social", theme_key=None,
            symbols=syms, source="x_consensus", status="ok", ttl_days=2,
        )
        out["symbols_count"] = len(syms)
        out["snapshot_ok"]   = ok
    elif tab == "bottlenecks":
        syms, meta = _build_bottlenecks_universe()
        ok = insert_universe_snapshot(
            universe_type="bottlenecks", theme_key=None,
            symbols=syms, source=meta.get("source_registry", "chain_reaction"),
            status="ok", ttl_days=10,
        )
        out["symbols_count"] = len(syms)
        out["snapshot_ok"]   = ok
        out["bottlenecks_meta"] = meta
    elif tab == "watchlist_portfolio":
        syms = _build_watchlist_portfolio_universe()
        ok = insert_universe_snapshot(
            universe_type="watchlist_portfolio", theme_key=None,
            symbols=syms, source="user_data", status="ok", ttl_days=2,
        )
        out["symbols_count"] = len(syms)
        out["snapshot_ok"]   = ok
    elif tab == "all":
        out["thematic"]            = await rebuild_universe("thematic", force=force)
        out["social"]              = await rebuild_universe("social", force=force)
        out["bottlenecks"]         = await rebuild_universe("bottlenecks", force=force)
        out["watchlist_portfolio"] = await rebuild_universe("watchlist_portfolio", force=force)
    else:
        out["error"] = f"unknown tab '{tab}'"

    return out


async def warm_tab_fundamentals(
    tab: str,
    *,
    theme: Optional[str] = None,
    force: bool = False,
    max_calls: int = 250,
) -> dict:
    """Run the fundamentals warm job for a tab's universe."""
    tab = tab.strip().lower()
    if tab == "thematic":
        if theme:
            snap = get_latest_universe("thematic", theme)
            symbols = list((snap or {}).get("symbols") or [])
            if not symbols:
                symbols_map, _ = await _build_thematic_universe(theme)
                symbols = symbols_map.get(theme, [])
            return await warm_fundamentals(
                symbols, job_name=f"thematic_warm:{theme}", force=force, max_calls=max_calls,
            )
        else:
            # Aggregate all theme symbols (deduped).
            # with_fmp_peers=False for bulk build.
            symbols_map, breakdowns = await _build_thematic_universe(None, with_fmp_peers=False)
            agg: list[str] = []
            seen: set[str] = set()
            for k, syms in symbols_map.items():
                # Persist a snapshot per theme too (cheap, idempotent)
                insert_universe_snapshot(
                    universe_type="thematic", theme_key=k,
                    symbols=syms, source="warm_job", ttl_days=8,
                    metadata=breakdowns.get(k, {}),
                )
                for s in syms:
                    if s not in seen:
                        seen.add(s)
                        agg.append(s)
            return await warm_fundamentals(
                agg, job_name="thematic_warm:all", force=force, max_calls=max_calls,
            )
    if tab in ("social", "bottlenecks", "watchlist_portfolio"):
        snap = get_latest_universe(tab)
        symbols = list((snap or {}).get("symbols") or [])
        if not symbols:
            await rebuild_universe(tab, force=False)
            snap = get_latest_universe(tab)
            symbols = list((snap or {}).get("symbols") or [])
        return await warm_fundamentals(
            symbols, job_name=f"{tab}_warm", force=force, max_calls=max_calls,
        )
    if tab == "all":
        out: dict[str, Any] = {}
        for t in ("thematic", "social", "bottlenecks", "watchlist_portfolio"):
            out[t] = await warm_tab_fundamentals(t, force=force, max_calls=max_calls)
        return out
    return {"error": f"unknown tab '{tab}'"}


# ── Status / diagnostics ──────────────────────────────────────────────────────

def get_admin_status() -> dict:
    try:
        from services.fmp_governor import fmp_governor as _gov
        fmp_budget = _gov.status()
    except Exception:
        fmp_budget = None

    # FMP call audit: per-endpoint counts, cache hit/miss rates, recent errors.
    # Pulled from the in-process api_audit ring buffer — no DB or FMP calls.
    fmp_call_audit: Optional[dict] = None
    try:
        from services import api_audit as _audit
        _ar = _audit.get_report()
        fmp_rows = [
            r for r in _ar.get("aggregated_by_provider_endpoint", [])
            if r.get("provider") == "fmp"
        ]
        fmp_call_audit = {
            "fmp_calls_since_start":  _ar["totals_since_start"].get("fmp", 0),
            "total_cache_hits":       _ar.get("total_cache_hits", 0),
            "total_cache_misses":     _ar.get("total_cache_misses", 0),
            "fmp_force_429_active":   _ar.get("fmp_force_429_active", False),
            "fmp_by_endpoint":        fmp_rows[:25],
            "recent_fmp_errors": [
                c for c in _ar.get("recent_calls", [])
                if c.get("provider") == "fmp" and not c.get("success", True)
            ][-10:],
        }
    except Exception:
        pass

    return {
        "as_of": _now_iso(),
        "status": "ok",
        "fundamentals_cache": fundamentals_table_stats(),
        "universe_snapshots": universe_table_stats(),
        "quote_cache":        quote_table_stats(),
        "latest_job_runs":    latest_job_runs(limit=20),
        "fmp_budget":         fmp_budget,
        "fmp_call_audit":     fmp_call_audit,
    }
