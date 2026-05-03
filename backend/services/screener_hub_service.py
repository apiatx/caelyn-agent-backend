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
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import httpx

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

# Path to the config-driven theme → FMP industry mapping.
# Loaded lazily by _load_industry_map_config().
_INDUSTRY_MAP_PATH = Path(__file__).parent.parent / "data" / "theme_fmp_industry_map.json"

# ETF holdings disk cache directory
_ETF_HOLDINGS_DIR = Path(__file__).parent.parent / "data" / "etf_holdings"

# Dynamic Chain Reaction output file (written by Chain Reaction service if/when available)
_CHAIN_REACTION_OUTPUT = Path(__file__).parent.parent / "data" / "chain_reaction_output.json"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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
    try:
        from services.theme_rs_universe import THEME_RS_UNIVERSE
        s: set[str] = set()
        for meta in THEME_RS_UNIVERSE.values():
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
            # Skip bond/cash placeholders
            if raw in ("CASH", "USD", "EUR", "TBD", "OTHER"):
                continue
            # Standard US equity ticker: 1–5 alpha chars, optionally followed by
            # one special char + alpha (e.g. BRK-B, BF-B).  Max 6 chars total.
            if len(raw) > 6:
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
    """
    api_key = os.getenv("FMP_API_KEY") or ""
    if not api_key or not anchors:
        return [], []

    async def _one(anchor: str) -> list[str]:
        try:
            async with httpx.AsyncClient() as client:
                r = await client.get(
                    f"{FMP_BASE}/stock-peers",
                    params={"symbol": anchor.upper(), "apikey": api_key},
                    timeout=timeout,
                )
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
) -> float:
    """
    Score 0–10 rating how "hidden gem"-like a stock is.

    Key signals: RS acceleration, volume surge, near-52w-high setup,
    small/mid-cap size, multi-source confirmation.

    Mega-caps are soft-penalized (they're anchors, not hidden gems).
    """
    score = 0.0

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
    #   very extended (>-2%) = overcrowded
    #   free-fall (<-50%) = negative
    if distance_52w_high is not None:
        if -30 <= distance_52w_high <= -5:
            score += 1.5
        elif -5 < distance_52w_high <= 0:
            score += 0.25  # near ATH — still valid but less "hidden"
        elif distance_52w_high < -50:
            score -= 0.5

    # Market cap: smaller → less discovered → bigger hidden-gem premium
    if market_cap is not None:
        if market_cap < 1e9:          # micro cap < $1B
            score += 2.0
        elif market_cap < 5e9:        # small cap < $5B
            score += 1.5
        elif market_cap < 20e9:       # mid cap < $20B
            score += 0.75
        elif market_cap > 100e9:      # mega cap > $100B — anchor, not hidden gem
            score -= 1.0

    # Multi-source confirmation boosts
    if is_social:
        score += 0.5
    if is_options:
        score += 0.5
    if is_watchlist:
        score += 0.25

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


def _theme_metadata() -> list[dict]:
    """All themes with display name + classification, for /api/screener-hub/themes."""
    out: list[dict] = []
    try:
        from services.theme_rs_universe import THEME_RS_UNIVERSE
        for key, meta in THEME_RS_UNIVERSE.items():
            label = meta.get("display_name") or key
            out.append({
                "theme_key":      key,
                "display_name":   label,
                # Aliases expected by the ScreenerHub frontend component
                "id":             key,
                "label":          label,
                "classification": meta.get("classification") or "theme",
                "parent_sector":  meta.get("parent_sector"),
                "proxy_symbols":  list(meta.get("proxy_symbols") or [])[:5],
            })
    except Exception as e:
        print(f"[SCREENER_HUB] theme registry load error: {e}")
    out.sort(key=lambda r: (r.get("classification") or "", r.get("display_name") or ""))
    return out


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


async def _fmp_industry_screener(
    industries: list[str],
    *,
    market_cap_upper: Optional[int] = 50_000_000_000,
    market_cap_lower: Optional[int] = None,
    screener_limit: int = 500,
    timeout: float = 12.0,
) -> tuple[list[dict], list[str], list[str]]:
    """
    FMP company-screener — ONE call per mapped industry.

    ONLY called from rebuild_universe / admin rebuild paths (with_fmp_screener=True).
    Never called on page loads.

    Returns:
        candidates           : list of candidate metadata dicts (rich, not just symbols)
        industries_attempted : list of industries whose API call was made
        industries_errored   : list of industries that failed or returned HTTP error

    Each candidate dict:
        symbol, company_name, market_cap, sector, industry,
        beta, price, volume, exchange, country, is_etf, is_fund,
        discovery_source  (e.g. "fmp_screener:Semiconductors")

    market_cap_upper/lower are applied client-side because FMP ignores those params.
    Candidates are returned sorted by market_cap ascending (smallest-cap first)
    so they fill the per-theme cap ahead of larger names.
    """
    api_key = os.getenv("FMP_API_KEY") or ""
    if not api_key or not industries:
        return [], [], []

    try:
        from services.fmp_governor import fmp_governor as _gov
    except Exception:
        _gov = None

    candidates:            list[dict] = []
    seen_syms:             set[str]   = set()
    industries_attempted:  list[str]  = []
    industries_errored:    list[str]  = []

    async def _screen_one(industry: str) -> tuple[str, list[dict], bool]:
        """Returns (industry, candidate_dicts, had_error)."""
        if _gov is not None:
            slot_ok = await _gov.acquire(job_name="fmp_industry_screener")
            if not slot_ok:
                return industry, [], True
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.get(
                    f"{FMP_BASE}/company-screener",
                    params={
                        "industry":          industry,
                        "isEtf":             "false",
                        "isFund":            "false",
                        "isActivelyTrading": "true",
                        "limit":             screener_limit,
                        "apikey":            api_key,
                    },
                )
            if _gov is not None:
                _gov.record_call()
            if resp.status_code != 200:
                print(f"[SCREENER_HUB] company-screener {industry!r} HTTP {resp.status_code}")
                return industry, [], True
            raw = resp.json()
            hits: list[dict] = []
            for item in (raw if isinstance(raw, list) else []):
                sym = (item.get("symbol") or "").strip().upper()
                if not sym or len(sym) > 5:
                    continue
                if not sym.replace("-", "").isalpha():
                    continue
                if item.get("isEtf") or item.get("isFund"):
                    continue
                mcap = float(item.get("marketCap") or 0)
                # Client-side market-cap filtering (FMP ignores the query params)
                if market_cap_upper and mcap and mcap > market_cap_upper:
                    continue
                if market_cap_lower and mcap and mcap < market_cap_lower:
                    continue
                beta_raw  = item.get("beta")
                price_raw = item.get("price")
                vol_raw   = item.get("volume")
                hits.append({
                    "symbol":           sym,
                    "company_name":     (item.get("companyName") or "").strip(),
                    "market_cap":       mcap if mcap else None,
                    "sector":           (item.get("sector") or "").strip(),
                    "industry":         (item.get("industry") or industry).strip(),
                    "beta":             float(beta_raw)  if beta_raw  is not None else None,
                    "price":            float(price_raw) if price_raw is not None else None,
                    "volume":           float(vol_raw)   if vol_raw   is not None else None,
                    "exchange":         (item.get("exchangeShortName") or item.get("exchange") or "").strip(),
                    "country":          (item.get("country") or "").strip(),
                    "is_etf":           bool(item.get("isEtf")),
                    "is_fund":          bool(item.get("isFund")),
                    "discovery_source": f"fmp_screener:{industry}",
                })
            return industry, hits, False
        except Exception as e:
            print(f"[SCREENER_HUB] company-screener {industry!r} error: {e}")
            return industry, [], True

    results = await asyncio.gather(
        *[_screen_one(ind) for ind in industries],
        return_exceptions=True,
    )
    for res in results:
        if isinstance(res, Exception):
            industries_errored.append("unknown")
            continue
        industry, hits, had_error = res
        industries_attempted.append(industry)
        if had_error:
            industries_errored.append(industry)
            continue
        for item in hits:
            sym = item["symbol"]
            if sym not in seen_syms:
                seen_syms.add(sym)
                candidates.append(item)

    # Sort smallest-cap first so they fill the per-theme cap ahead of larger names.
    candidates.sort(key=lambda c: c.get("market_cap") or float("inf"))

    print(
        f"[SCREENER_HUB] fmp_industry_screener: {len(industries)} industries → "
        f"{len(candidates)} candidates "
        f"(errors: {industries_errored or 'none'})"
    )
    return candidates, industries_attempted, industries_errored


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

    # Load config-driven industry map once for all themes.
    # Only needed when with_fmp_screener=True, but cheap to load always.
    industry_map_cfg = _load_industry_map_config() if with_fmp_screener else {}
    industry_map_version = industry_map_cfg.get("industry_map_version", "none")
    themes_cfg = industry_map_cfg.get("themes") or {}

    try:
        from services.theme_rs_universe import THEME_RS_UNIVERSE
    except Exception:
        THEME_RS_UNIVERSE = {}

    for key in keys:
        meta = (THEME_RS_UNIVERSE or {}).get(key) or {}
        proxy_etfs:     list[str] = [s.upper() for s in (meta.get("proxy_symbols") or []) if s]
        candidate_syms: list[str] = [s.upper() for s in (meta.get("candidate_symbols") or []) if s]

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

        seen_dynamic: set[str] = set()

        # ── Source A: ETF holdings (disk read, fast) ──────────────────────────
        for etf in proxy_etfs:
            holdings = _read_etf_holdings_from_disk(etf)
            if holdings:
                etf_files_found.append(etf)
            for sym in holdings:
                if sym not in _ALL_PROXY_ETFS and sym not in seen_dynamic:
                    seen_dynamic.add(sym)
                    etf_holdings_syms.append(sym)
                    sources_by_symbol.setdefault(sym, []).append(f"etf:{etf}")

        if not etf_files_found and proxy_etfs:
            sources_failed.append("etf_holdings")

        # ── Source B: LKG leaders / laggards ──────────────────────────────────
        for sym in lkg_map.get(key) or []:
            su = sym.upper() if isinstance(sym, str) else ""
            if su and su not in _ALL_PROXY_ETFS and su not in seen_dynamic:
                seen_dynamic.add(su)
                lkg_syms.append(su)
                sources_by_symbol.setdefault(su, []).append("lkg_leaders")

        # ── Source C: FMP industry screener (rebuild jobs only) ───────────────
        # Uses config-driven theme → industry mapping from theme_fmp_industry_map.json.
        # Candidates are sorted smallest-cap first (done inside _fmp_industry_screener).
        # They get RESERVED slots in the final cap so ETF overflow doesn't crowd them out.
        theme_cfg  = themes_cfg.get(key) or {}
        industries = theme_cfg.get("fmp_industries") or []
        cap_upper  = theme_cfg.get("market_cap_upper_default") or 50_000_000_000
        cap_lower  = theme_cfg.get("market_cap_lower_default")

        if with_fmp_screener and industries:
            sources_attempted.append("fmp_screener")
            try:
                cands, ind_attempted, ind_errored = await _fmp_industry_screener(
                    industries,
                    market_cap_upper=int(cap_upper) if cap_upper else None,
                    market_cap_lower=int(cap_lower) if cap_lower else None,
                )
                fmp_screener_industries_attempted = ind_attempted
                fmp_screener_industries_errored   = ind_errored
                fmp_screener_calls_used           = len(ind_attempted)

                if ind_errored:
                    sources_failed.append(f"fmp_screener_partial({','.join(ind_errored[:3])})")

                for cand in cands:
                    sym = cand["symbol"]
                    if sym not in _ALL_PROXY_ETFS and sym not in seen_dynamic:
                        seen_dynamic.add(sym)
                        screener_syms.append(sym)
                        screener_meta_by_symbol[sym] = cand
                        src_tag = cand.get("discovery_source") or "fmp_screener"
                        sources_by_symbol.setdefault(sym, []).append(src_tag)
            except Exception as se:
                print(f"[SCREENER_HUB] fmp_screener {key} error: {se}")
                sources_failed.append("fmp_screener")
        elif with_fmp_screener and not industries:
            print(f"[SCREENER_HUB] fmp_screener: no industry mapping for theme={key!r}")

        # ── Source D: FMP peers (thin themes, budget-guarded) ─────────────────
        if with_fmp_peers:
            sources_attempted.append("fmp_peers")
            if len(seen_dynamic) < _MIN_DYN_BEFORE_PEERS and candidate_syms:
                try:
                    peers, fmp_peer_anchors = await _fmp_peers_for_anchors(candidate_syms[:3])
                    for sym in peers:
                        if sym not in _ALL_PROXY_ETFS and sym not in seen_dynamic:
                            seen_dynamic.add(sym)
                            fmp_peer_syms.append(sym)
                            sources_by_symbol.setdefault(sym, []).append("fmp_peers")
                except Exception as pe:
                    print(f"[SCREENER_HUB] fmp_peers {key} error: {pe}")
                    sources_failed.append("fmp_peers")

        # ── Source F: candidate_symbols — static fallback ─────────────────────
        for sym in candidate_syms:
            if sym not in seen_dynamic:
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
            "fmp_screener_industries_attempted":   fmp_screener_industries_attempted,
            "fmp_screener_industries_errored":     fmp_screener_industries_errored,
            "fmp_screener_calls_used":             fmp_screener_calls_used,
            "fmp_screener_symbols_added":          len(screener_syms),
            "industry_map_version":                industry_map_version,
            "static_seed_count":                   n_static,
            "dynamic_symbols_count":               n_dynamic,
            "static_fallback_symbols_count":       n_static,
            "used_static_fallback":                n_static > 0,
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

async def get_screener_hub(
    *,
    tab: str,
    theme: Optional[str] = None,
    category: Optional[str] = None,
    score_mode: bool = False,
    coc_filter: bool = False,
) -> dict:
    """Build the response payload for /api/screener-hub.

    The shape matches the contract requested by the frontend:
      { status, tab, theme, generated_at, fundamentals_cache_status,
        quote_cache_status, rows: [...] }

    Universe symbols come from the latest screener_universe_snapshots row.
    Fundamentals come from screener_fundamentals_cache (no live FMP fetch).
    Quotes come from screener_quote_cache; we refresh stale rows for *just*
    the active page before returning.
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

    symbols: list[str] = []
    snap_status = "fresh"
    universe_source = "snapshot"
    theme_state_meta: dict = {}
    thematic_breakdown: dict = {}
    bottlenecks_meta: dict = {}

    # ── Load overlap sets (fast disk reads; used for per-row tagging) ──────────
    # These are loaded once per request and passed through to row building.
    social_overlap:   set[str] = _load_social_overlap()
    options_overlap:  set[str] = _load_options_overlap()
    watchlist_set:    set[str] = _load_watchlist_set()

    # ── Resolve universe ──
    if tab == "thematic":
        # Pull theme state from Themes page LKG (reuse, don't duplicate).
        if theme:
            theme_state_meta = _get_theme_state_from_lkg(theme)

        if theme:
            snap = get_latest_universe("thematic", theme)
            if snap and snap.get("symbols"):
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
            # No theme → flatten symbols across all themes (de-dupe).
            # with_fmp_peers=False to avoid 55 × peer API calls.
            symbols_map, breakdowns = await _build_thematic_universe(None, with_fmp_peers=False)
            seen: set[str] = set()
            for syms in symbols_map.values():
                for s in syms:
                    if s not in seen:
                        seen.add(s)
                        symbols.append(s)
            snap_status = "live_aggregated"
            universe_source = "live"

        # Strip ETF proxies from the final rows list for thematic tab.
        # They may remain in the cached universe for fundamentals warm-job
        # coverage, but the screener table should show stocks only.
        symbols = [s for s in symbols if s not in _ALL_PROXY_ETFS] or symbols

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

    # ── Per-symbol source map (thematic only; populated by live build) ─────────
    # Used below to build row.discovery_sources from ETF/LKG/peer/static tags.
    sources_by_symbol: dict[str, list[str]] = thematic_breakdown.get("sources_by_symbol") or {}

    # ── FMP screener metadata — used to enrich rows when FMP cache is cold ────
    # Populated during rebuild; stored in snapshot metadata_json. Provides
    # company_name, market_cap, sector, industry, beta, volume, exchange, country
    # without additional FMP profile calls.
    screener_meta_by_symbol: dict[str, dict] = thematic_breakdown.get("screener_meta_by_symbol") or {}

    # ── Chain Reaction detail map (bottlenecks only) ────────────────────────────
    # Per-symbol scored node data from chain_reaction_weekly_outputs.rows_json.
    cr_details: dict[str, dict] = bottlenecks_meta.get("details_by_symbol") or {}

    # ── Load real historical returns from cache (never blocks on FMP API) ──────
    returns_cache: dict[str, dict] = get_returns(symbols) if symbols else {}
    returns_cached_count = sum(1 for r in returns_cache.values() if r.get("return_4w") is not None)

    # ── Refresh page-aware quotes ──
    quote_cache_status = "skipped"
    if symbols:
        quote_summary = await refresh_quotes_for_page(symbols)
        quote_cache_status = quote_summary.get("status", "unknown")

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

        # ── Beta / volume from screener meta (FMP metrics preferred when warm) ──
        row_beta   = _to_float(metrics.get("betaTTM") or scr_meta.get("beta"))
        row_volume = _to_float(scr_meta.get("volume"))

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
            "symbol":       sym,
            "name":         row_name,
            "company_name": row_name,
            "history":      None,  # populated by frontend chart endpoint
            "category":          classification["category"],
            "rs_0_2w":           classification["rs_0_2w"],
            "rs_0_4w":           classification["rs_0_4w"],
            "rs_0_10w":          classification["rs_0_10w"],
            "rs_accel":          classification["rs_accel"],
            "performance_2w":    r2w,
            "performance_4w":    r4w,
            "performance_10w":   r10w,
            "rs_data_status":    rs_status,
            "distance_52w_high": classification["distance_52w_high"],
            "volume_surge":      classification["volume_surge"],
            "accumulation":      classification["accumulation"],
            "coc":               classification["coc"],
            "score":             classification["score"],
            "hidden_gem_score":  hg_score,
            "role":              role,
            "market_cap_bucket": _market_cap_bucket(mcap),
            "discovery_sources": disc_src,
            "market_cap":  mcap,
            "sector":      row_sector,
            "industry":    row_industry,
            "is_etf":      row_is_etf,
            "is_fund":     row_is_fund,
            "price":       q_row.get("price"),
            "change_percent_1d": q_row.get("change_percent_1d"),
            "performance_7d":  None,
            "performance_30d": None,
            "performance_ytd": None,
            "performance_1y":  None,
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

        if not _row_passes_filters(row, category_filter=category, coc_filter=coc_filter):
            continue
        rows.append(row)

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

    payload: dict[str, Any] = {
        "status": "ok",
        "tab": tab,
        "theme": theme,
        "generated_at": _now_iso(),
        "fundamentals_cache_status": fundamentals_cache_status,
        "quote_cache_status":        quote_cache_status,
        "universe_source":           universe_source,
        "universe_status":           snap_status,
        "row_count":                 len(rows),
        "rows": rows,
    }

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
    return {
        "as_of": _now_iso(),
        "status": "ok",
        "fundamentals_cache": fundamentals_table_stats(),
        "universe_snapshots": universe_table_stats(),
        "quote_cache":        quote_table_stats(),
        "latest_job_runs":    latest_job_runs(limit=20),
        "fmp_budget":         fmp_budget,
    }
