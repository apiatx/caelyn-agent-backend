"""
Canonical Market Cap Resolver — Part 2 + Part 3 + Part 5 of spec.

Single entry point:  resolve_canonical_market_cap()
Helper:              get_live_price_for_mc()

Zero network I/O — reads only from in-memory caches that are already
populated by background loops (Tradier quote limiter, watchlist quote cache).

Canonical contract returned:
  market_cap_static / _source / _last_updated
  market_cap_price_at_static_refresh
  market_cap_implied_shares / _source
  market_cap_live / _source / _price / _price_source
  market_cap_display / _source / _freshness / _warning_codes

Rules (from spec Part 2):
  1. market_cap_static  — from watchlist_fundamentals_cache ("Market Cap" field).
                          Falls back to screener_fundamentals_cache override.
  2. market_cap_implied_shares = market_cap_static / market_cap_price_at_static_refresh
     (stored by FmpFundamentalsRefresher when profile is refreshed).
  3. market_cap_live = live_price * market_cap_implied_shares
  4. market_cap_display prefers live; falls back to static.
  5. Null/zero/negative/absurd values are rejected and flagged in warning_codes.
  6. No FMP calls — ever.
"""

from __future__ import annotations

import math
import time as _time
from typing import Any


# ── Sane-ness thresholds ──────────────────────────────────────────────────────
_MIN_MC   = 1_000_000          # $1M minimum (reject sub-penny shells)
_MAX_MC   = 20_000_000_000_000 # $20T maximum (reject corrupt data)
_MAX_LIVE_TO_STATIC_RATIO = 5.0  # live/static divergence guard
_MIN_LIVE_TO_STATIC_RATIO = 0.2  # live/static divergence guard (reverse)

# ── Disk LKG cache (module-level, 60 s TTL) ──────────────────────────────────
# Populated from backend/data/watchlist_quote_lkg.json which survives restarts
# and 4-day market closures.  Cached to avoid repeated file I/O per call.
_DISK_LKG: dict[str, dict] = {}
_DISK_LKG_TS: float = 0.0
_DISK_LKG_TTL = 60.0
_DISK_LKG_MAX_AGE = 96 * 3600  # 4-day max age matches watchlist_quote_cache.py


def _load_disk_lkg_price(symbol: str) -> float | None:
    """
    Return the last-known price from the watchlist quote disk LKG.
    Cache is refreshed at most once every 60 s to amortise file I/O.
    Returns None if file absent, symbol not found, or entry too stale.
    """
    global _DISK_LKG, _DISK_LKG_TS
    import os, json
    now_mono = _time.monotonic()
    if now_mono - _DISK_LKG_TS > _DISK_LKG_TTL:
        try:
            path = os.path.join(os.path.dirname(__file__), "..", "data", "watchlist_quote_lkg.json")
            with open(path, "r", encoding="utf-8") as _f:
                _DISK_LKG = json.load(_f)
            _DISK_LKG_TS = now_mono
        except Exception:
            _DISK_LKG_TS = now_mono  # don't spam retries on missing file
    entry = _DISK_LKG.get(symbol.upper()) or {}
    p = _positive(entry.get("price"))
    if p is None:
        return None
    # Respect max age: reject entries older than 4 days
    try:
        from datetime import datetime, timezone, timedelta
        upd_str = entry.get("quote_updated_at") or ""
        if upd_str:
            upd = datetime.fromisoformat(upd_str.rstrip("Z").replace("Z", "+00:00"))
            if upd.tzinfo is None:
                upd = upd.replace(tzinfo=timezone.utc)
            age_s = (datetime.now(timezone.utc) - upd).total_seconds()
            if age_s > _DISK_LKG_MAX_AGE:
                return None  # too old — don't use
    except Exception:
        pass  # parse error → accept the price rather than drop it
    return p


def _safe_float(v: Any) -> float | None:
    try:
        if v in (None, "", "-"):
            return None
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except Exception:
        return None


def _positive(v: Any) -> float | None:
    f = _safe_float(v)
    return f if (f is not None and f > 0) else None


# ── Price lookup — zero network I/O ──────────────────────────────────────────

def get_live_price_for_mc(symbol: str) -> tuple[float | None, str | None]:
    """
    Return (price, source_label) from in-memory / disk caches — no network calls.

    Priority (Part 3 of spec):
      1. tradier:quote:sym:{SYM}  — 60 s per-symbol cache written by TradierProvider
      2. quote:lkg:{SYM}          — 72 h LKG written by watchlist/home/portfolio
      3. watchlist_quote_cache     — 10 min module cache (_quote_cache)
      4. watchlist_quote_lkg.json — disk LKG, 4-day max age, survives restarts
    """
    sym = symbol.upper()

    # 1 + 2: shared in-memory cache (data.cache)
    try:
        from data.cache import cache as _c
        raw = _c.get(f"tradier:quote:sym:{sym}")
        if raw:
            p = _positive(raw.get("last") or raw.get("price"))
            if p:
                return p, "tradier_quote_sym"
        raw = _c.get(f"quote:lkg:{sym}")
        if raw:
            p = _positive(raw.get("price") or raw.get("last"))
            if p:
                return p, "quote_lkg"
    except Exception:
        pass

    # 3: watchlist module in-memory cache
    try:
        from services.watchlist_quote_cache import _quote_cache
        q = _quote_cache.get(sym) or {}
        p = _positive(q.get("price"))
        if p:
            return p, "watchlist_quote_cache"
    except Exception:
        pass

    # 4: watchlist disk LKG — survives restarts and multi-day closures
    p = _load_disk_lkg_price(sym)
    if p:
        return p, "watchlist_quote_disk_lkg"

    return None, None


# ── Canonical resolver ────────────────────────────────────────────────────────

def resolve_canonical_market_cap(
    symbol: str,
    fund_fields: dict | None,
    live_price: float | None = None,
    live_price_source: str | None = None,
    static_market_cap_override: float | None = None,
    fund_refreshed_at: str | None = None,
) -> dict:
    """
    Canonical market cap resolution.  Pure function — no I/O.

    Args:
      symbol                    — ticker (uppercase)
      fund_fields               — watchlist_fundamentals_cache.fields dict
                                  (may contain "Market Cap",
                                   "_market_cap_implied_shares",
                                   "_market_cap_price_at_refresh")
      live_price                — callers that already have the Tradier price
                                  can pass it directly (skips cache lookup)
      live_price_source         — label for passed-in live_price
      static_market_cap_override— screener_fundamentals_cache market_cap,
                                  used only when fund_fields has no Market Cap
      fund_refreshed_at         — ISO timestamp of the fund cache refresh

    Returns the full canonical dict described in the module docstring.
    """
    fund_fields = fund_fields or {}
    warning_codes: list[str] = []

    # ── 1. Static market cap ─────────────────────────────────────────────────
    mc_static: float | None = None
    mc_static_source: str | None = None
    mc_static_last_updated = fund_refreshed_at

    raw_mc = fund_fields.get("Market Cap") or fund_fields.get("market_cap")
    if raw_mc is not None:
        v = _positive(raw_mc)
        if v and _MIN_MC <= v <= _MAX_MC:
            mc_static = v
            mc_static_source = "watchlist_fundamentals_cache"
        elif v:
            warning_codes.append("static_market_cap_out_of_bounds")

    if mc_static is None and static_market_cap_override is not None:
        v = _positive(static_market_cap_override)
        if v and _MIN_MC <= v <= _MAX_MC:
            mc_static = v
            mc_static_source = "screener_fundamentals_cache"
            mc_static_last_updated = None  # age unknown from this source

    # ── 2. Implied shares ────────────────────────────────────────────────────
    mc_price_at_refresh: float | None = None
    mc_implied_shares: float | None = None
    mc_implied_shares_source: str | None = None

    raw_shares = fund_fields.get("_market_cap_implied_shares")
    raw_par    = fund_fields.get("_market_cap_price_at_refresh")

    if raw_shares is not None:
        s = _positive(raw_shares)
        if s:
            mc_implied_shares = s
            mc_implied_shares_source = fund_fields.get("_market_cap_static_source") or "fmp_profile"
            mc_price_at_refresh = _positive(raw_par)
    elif mc_static is not None:
        # Legacy rows (refreshed before this code was deployed) have no implied
        # shares stored.  They cannot compute live market cap, but static is fine.
        warning_codes.append("implied_shares_not_yet_stored_pending_next_refresh")

    # ── 3. Live market cap ───────────────────────────────────────────────────
    mc_live: float | None = None
    mc_live_source: str | None = None
    mc_live_price: float | None = None
    mc_live_price_source: str | None = None

    if live_price is not None and live_price > 0:
        mc_live_price = live_price
        mc_live_price_source = live_price_source or "caller"
    else:
        # Auto-lookup — no network
        _p, _src = get_live_price_for_mc(symbol)
        if _p:
            mc_live_price = _p
            mc_live_price_source = _src
        else:
            warning_codes.append("no_live_price_available")

    if mc_live_price and mc_implied_shares:
        candidate = round(mc_live_price * mc_implied_shares, 0)
        # Sanity: reject if diverges >5× from static (corporate action, bad data)
        if mc_static and mc_static > 0:
            ratio = candidate / mc_static
            if ratio > _MAX_LIVE_TO_STATIC_RATIO or ratio < _MIN_LIVE_TO_STATIC_RATIO:
                warning_codes.append(
                    f"live_market_cap_implausible_ratio_{ratio:.2f}_vs_static"
                )
                # Still surface the live value but also warn
                mc_live = candidate
                mc_live_source = "implied_shares_x_live_price_IMPLAUSIBLE"
            else:
                mc_live = candidate
                mc_live_source = "implied_shares_x_live_price"
        elif _MIN_MC <= candidate <= _MAX_MC:
            mc_live = candidate
            mc_live_source = "implied_shares_x_live_price"
        else:
            warning_codes.append("live_market_cap_out_of_bounds")
    elif mc_live_price and not mc_implied_shares:
        pass  # warning already added above

    # ── 4. Display (canonical single value) ──────────────────────────────────
    mc_display: float | None = None
    mc_display_source: str | None = None
    mc_display_freshness: str | None = None

    if mc_live is not None and "IMPLAUSIBLE" not in (mc_live_source or ""):
        mc_display = mc_live
        mc_display_source = "live"
        mc_display_freshness = "live"
    elif mc_static is not None:
        mc_display = mc_static
        mc_display_source = mc_static_source or "static"
        mc_display_freshness = "static"
        if mc_implied_shares:
            warning_codes.append("showing_static_market_cap_live_derivation_failed")
        # If live was implausible, still show it but with warning
        if mc_live is not None and "IMPLAUSIBLE" in (mc_live_source or ""):
            mc_display = mc_live
            mc_display_source = "live_implausible"
            mc_display_freshness = "live"
    else:
        warning_codes.append("market_cap_unavailable_no_static_or_live")
        mc_display_freshness = "unavailable"

    return {
        # Static basis (from FMP weekly profile snapshot)
        "market_cap_static":              mc_static,
        "market_cap_static_source":       mc_static_source,
        "market_cap_static_last_updated": mc_static_last_updated,
        # Share basis (derived at refresh time, stored for fast live computation)
        "market_cap_price_at_static_refresh": mc_price_at_refresh,
        "market_cap_implied_shares":          mc_implied_shares,
        "market_cap_implied_shares_source":   mc_implied_shares_source,
        # Live (price × implied shares, using current Tradier quote)
        "market_cap_live":              mc_live,
        "market_cap_live_source":       mc_live_source,
        "market_cap_live_price":        mc_live_price,
        "market_cap_live_price_source": mc_live_price_source,
        # Display — single canonical value the UI should show
        "market_cap_display":           mc_display,
        "market_cap_display_source":    mc_display_source,
        "market_cap_display_freshness": mc_display_freshness,
        "market_cap_display_warning_codes": warning_codes or None,
    }
