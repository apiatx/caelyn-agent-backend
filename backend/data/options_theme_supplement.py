"""
Theme options supplement.

Manages three concerns for the options-by-sector feature:

  1. Theme seed injection
     get_theme_proxy_symbols_for_supplement() — theme proxy symbols NOT in
     the static master seed lists.  Injected into _cycle_seeds on each
     prefilter cold rebuild so the master Stage-1 sweep sees them.

  2. No-options persistent tracking
     update_no_options_from_expiry_cache() — called after every master
     screener cycle.  Reads the existing in-process Stage-1 expiry dict
     (zero extra Tradier calls) and persists symbols confirmed to have no
     tradeable options to a 24-hour cache entry.

  3. Supplemental scan cache
     Results from _theme_options_supplement_loop (main.py) are stored in
     options_theme_supplement_v1.  update_supplement_cache() merges new
     results into the existing entry so only the tickers in each batch
     are replaced.

  4. Combined data accessor
     get_combined_ticker_data() merges master screener cache + supplement
     cache into one {ticker: row} dict.  Master cache entries always win.

No new Tradier clients are created here.  All scan calls share the existing
TradierFlowEngine instance and _TRADIER_GLOBAL_SEM rate limiter.
"""
from __future__ import annotations

import time
from typing import Optional

_NO_OPTIONS_CACHE_KEY = "options_no_options_tracking:v1"
_NO_OPTIONS_CACHE_TTL = 86400        # 24 h — confirmed no-options status is stable

_SUPPLEMENT_CACHE_KEY = "options_theme_supplement_v1"
_SUPPLEMENT_CACHE_TTL = 1800         # 30 min — supplement data freshness window

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
            # Identify ETF proxies: themes with proxy_type="etf" or all-alpha
            # 3-5 char symbols (common ETF ticker pattern).
            is_etf = (
                meta.get("proxy_type") == "etf"
                or (3 <= len(sym) <= 5 and sym.isalpha())
            )
            if is_etf:
                etf_proxies.append(sym)
            else:
                stock_proxies.append(sym)

    result = etf_proxies + stock_proxies
    return result[:max_symbols]


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
    Return theme proxy symbols that are NOT in the current master screener
    cache AND NOT confirmed as no-options.

    These are the candidates the _theme_options_supplement_loop should
    scan next, sorted alphabetically (deterministic rolling cursor).
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

    master_syms = _get_master_tickers()
    no_opts     = get_no_options_symbols()

    return sorted(all_syms - master_syms - no_opts)


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
    """Return {ticker: options_row} from the supplement cache."""
    try:
        from data.cache import cache
        supp = cache.get(_SUPPLEMENT_CACHE_KEY) or {}
        return supp.get("ticker_data", {})
    except Exception:
        return {}


def update_supplement_cache(results: list[dict]) -> None:
    """
    Merge new supplement scan results into the supplement cache.

    Existing entries for tickers NOT in the new batch are preserved until
    the supplement TTL expires.  New/updated entries replace old ones.
    Each row is tagged with _source="supplement" and _cached_at timestamp.
    """
    if not results:
        return
    try:
        from data.cache import cache
        existing = cache.get(_SUPPLEMENT_CACHE_KEY) or {"ticker_data": {}, "cached_at": 0}
        ticker_data: dict = existing.get("ticker_data", {})
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
    except Exception as exc:
        print(f"[THEME_SUPP] Supplement cache update error: {exc}")


# ── Combined data accessor ────────────────────────────────────────────────────

def get_combined_ticker_data() -> dict[str, dict]:
    """
    Merge master screener cache + supplement cache into one {ticker: row} dict.

    Master cache rows are tagged with _source="live" and always take
    precedence over supplement rows.
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
                combined[sym] = row   # already tagged _source="supplement"

        return combined
    except Exception:
        return {}


# ── Debug stats ───────────────────────────────────────────────────────────────

def get_supplement_stats() -> dict:
    """Diagnostic stats shown by /api/options-flow/sectors/debug."""
    try:
        from data.cache import cache

        no_opts_raw: dict = cache.get(_NO_OPTIONS_CACHE_KEY) or {}
        supp_raw = cache.get(_SUPPLEMENT_CACHE_KEY) or {}
        supp_tickers = supp_raw.get("ticker_data", {})

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
            "theme_universe_symbol_count":   len(all_theme_syms),
            "master_scan_ticker_count":      len(master_syms),
            "overlap_count":                 len(all_theme_syms & master_syms),
            "theme_only_symbol_count":       len(theme_only),
            "no_options_confirmed_count":    len(no_opts_raw),
            "supplement_scanned_count":      len(supp_tickers),
            "pending_scan_count":            len(theme_only - set(no_opts_raw) - set(supp_tickers)),
            "supplement_last_scan_at":       supp_raw.get("last_scan_at"),
            "static_seed_count":             len(_get_static_seeds()),
            "extra_theme_seeds_for_inject":  len(get_theme_proxy_symbols_for_supplement()),
        }
    except Exception as exc:
        return {"error": str(exc)}
