"""
dynamic_thematic_universe.py — Dynamic Thematic Universe Builder.

Converts active/emerging themes from ThematicContextProvider into ticker candidates
using live, dynamic data sources — not hardcoded lists.

Discovery pipeline per theme (in priority order):
  1. ETF holdings   — top N holdings from each proxy ETF (etf_holdings_service, 7-day cache)
  2. FMP peers      — peer expansion from anchor tickers (FMP stable/stock-peers)
  3. X consensus    — all tickers from x_consensus_weekly.json top_tickers
  4. Static anchors — related_tickers directly from ThematicContextProvider (always available)

Theme registry (THEME_ETF_UNIVERSE) provides:
  - proxy_etfs  (symbols field)  — ETF holdings source
  - seed anchors (representative_tickers) — FMP peer expansion
  - keywords / sector tags — metadata only, no API filtering

Cache:
  In-memory key: "dynamic_thematic_universe:v1"
  TTL:           15 minutes
  ETF holdings:  separately cached 7 days by etf_holdings_service
  Background:    _dynamic_thematic_universe_loop() refreshes every 15 min

Public API:
  get_dynamic_thematic_universe(...)  — async, builds or returns cached result
  get_cached_thematic_universe()      — sync, non-blocking read from cache only

Guarantees:
  - Never raises.  Returns best available data, empty structure on total failure.
  - No LLM calls.  No Tradier calls.  No Earnings data.
  - Never 500.  All vendor calls time-bounded.
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Optional

_CACHE_KEY = "dynamic_thematic_universe:v1"
_CACHE_TTL = 15 * 60           # 15-minute in-memory TTL

_ETF_HOLDINGS_TOP_N  = 25      # top holdings to extract per proxy ETF
_MAX_ETF_PER_THEME   = 3       # max proxy ETFs to expand per theme
_MAX_PEER_ANCHORS    = 3       # max anchor tickers for FMP peer expansion per theme
_MAX_PEERS_PER_ANCHOR = 8      # max peers returned per anchor call
_MAX_X_CONSENSUS     = 30      # max tickers from X consensus pool

_XC_PATH  = Path(__file__).parent.parent / "data" / "x_consensus_weekly.json"
_XC_MAX_AGE = 8 * 24 * 3600   # X consensus usable for 8 days

_FMP_PEERS_URL = "https://financialmodelingprep.com/stable/stock-peers"
_FMP_TIMEOUT   = 6.0


# ── Public API ─────────────────────────────────────────────────────────────────

async def get_dynamic_thematic_universe(
    active_only: bool = False,
    include_emerging: bool = True,
    max_tickers: int = 150,
    force_refresh: bool = False,
) -> dict:
    """
    Return the dynamic thematic ticker universe.

    Serves from in-memory cache when warm (< 15 min).
    On cache miss (or force_refresh), builds the universe using live data sources
    and caches the result.

    Returns:
    {
      "tickers":           [str],           # ordered, deduplicated, max_tickers
      "sources_by_ticker": {"CCJ": ["etf_holding:URA", "peer:LEU"]},
      "theme_map": {
        "CCJ": {
          "theme_name": "Nuclear & Uranium",
          "theme_state": "active",
          "regime_alignment_score": 0.8,
          "discovery_sources": ["etf_holding:URA", "peer:LEU"]
        }
      },
      "snapshot_status":   "fresh" | "stale_lkg" | "fallback_static" | "cold",
      "source_health":     {etf_holdings: str, fmp_peers: str, x_consensus: str},
      "built_at":          float,   # unix ts
      "ticker_count":      int,
    }
    """
    try:
        from data.cache import cache
        if not force_refresh:
            cached = cache.get(_CACHE_KEY)
            if cached and isinstance(cached, dict):
                return cached

        result = await _build_dynamic_universe(active_only, include_emerging, max_tickers)
        cache.set(_CACHE_KEY, result, _CACHE_TTL)
        return result

    except Exception as exc:
        print(f"[DTU] get_dynamic_thematic_universe error: {exc}")
        return _empty_result("error")


def get_cached_thematic_universe() -> dict:
    """
    Sync, non-blocking.  Read-only cache access — never triggers a build.

    Returns the last cached dynamic universe, or an empty structure if the
    cache is cold.  Callers should NOT block on this; use the empty-tickers
    check to detect cold cache and fall back gracefully.
    """
    try:
        from data.cache import cache
        cached = cache.get(_CACHE_KEY)
        if cached and isinstance(cached, dict):
            return cached
    except Exception:
        pass
    return _empty_result("cold")


# ── Internal builders ──────────────────────────────────────────────────────────

async def _build_dynamic_universe(
    active_only: bool,
    include_emerging: bool,
    max_tickers: int,
) -> dict:
    """Full async build of the dynamic thematic universe."""
    from services.thematic_context_provider import get_shared_thematic_context

    snap = get_shared_thematic_context()
    snapshot_status = snap.get("snapshot_status", "unknown")
    source_health: dict = {}

    active_themes   = snap.get("active_themes", [])
    emerging_themes = snap.get("emerging_themes", []) if not active_only else []
    themes_to_process = active_themes + (emerging_themes if include_emerging else [])

    # Per-ticker tracking
    sources_by_ticker: dict[str, list[str]] = {}
    theme_assignment:  dict[str, dict]      = {}   # ticker → {theme_name, theme_state, score}

    # Priority tiers — collected separately so ordering is deterministic
    tier1_active:   list[str] = []   # ETF holdings + peers from active themes
    tier2_emerging: list[str] = []   # ETF holdings + peers from emerging themes
    tier3_x:        list[str] = []   # X consensus picks
    tier4_static:   list[str] = []   # related_tickers fallback

    # ── Source 1 + 2: ETF holdings & FMP peers (concurrent per theme) ─────────
    etf_health_ok  = False
    peer_health_ok = False

    async def _expand_theme(theme: dict, state: str, score: float) -> list[str]:
        nonlocal etf_health_ok, peer_health_ok

        name     = theme.get("name", "")
        rel_etfs = (theme.get("related_etfs") or [])[:_MAX_ETF_PER_THEME]
        anchors  = (theme.get("related_tickers") or [])[:_MAX_PEER_ANCHORS]

        # Enrich from THEME_ETF_UNIVERSE for more ETF symbols
        rel_etfs = _augment_etfs_from_universe(name, rel_etfs)

        discovered: list[str] = []

        # 1a. ETF holdings
        etf_tickers, etf_src = await _etf_holdings_tickers(rel_etfs)
        if etf_tickers:
            etf_health_ok = True
        for sym, sources in etf_src.items():
            sources_by_ticker.setdefault(sym, [])
            for s in sources:
                if s not in sources_by_ticker[sym]:
                    sources_by_ticker[sym].append(s)
            if sym not in theme_assignment:
                theme_assignment[sym] = {
                    "theme_name":             name,
                    "theme_state":            state,
                    "regime_alignment_score": score,
                    "discovery_sources":      list(sources),
                }
            if sym not in discovered:
                discovered.append(sym)

        # 1b. Static anchor tickers (always available, no API call)
        for sym in anchors:
            s = sym.upper()
            sources_by_ticker.setdefault(s, [])
            src = f"static_anchor:{name}"
            if src not in sources_by_ticker[s]:
                sources_by_ticker[s].append(src)
            if s not in theme_assignment:
                theme_assignment[s] = {
                    "theme_name":             name,
                    "theme_state":            state,
                    "regime_alignment_score": score * 0.9,
                    "discovery_sources":      [src],
                }
            if s not in discovered:
                discovered.append(s)

        # 1c. FMP peers from anchor tickers
        peer_tickers, peer_src = await _fmp_peers_tickers(anchors)
        if peer_tickers:
            peer_health_ok = True
        for sym, sources in peer_src.items():
            sources_by_ticker.setdefault(sym, [])
            for s in sources:
                if s not in sources_by_ticker[sym]:
                    sources_by_ticker[sym].append(s)
            if sym not in theme_assignment:
                theme_assignment[sym] = {
                    "theme_name":             name,
                    "theme_state":            state,
                    "regime_alignment_score": score * 0.7,
                    "discovery_sources":      list(sources),
                }
            if sym not in discovered:
                discovered.append(sym)

        return discovered

    # Run theme expansion concurrently
    active_tasks   = [_expand_theme(t, "active",   0.8) for t in active_themes]
    emerging_tasks = [_expand_theme(t, "emerging", 0.5) for t in emerging_themes] if include_emerging else []

    active_results   = await asyncio.gather(*active_tasks,   return_exceptions=True)
    emerging_results = await asyncio.gather(*emerging_tasks, return_exceptions=True)

    for r in active_results:
        if isinstance(r, list):
            for sym in r:
                if sym not in tier1_active:
                    tier1_active.append(sym)

    for r in emerging_results:
        if isinstance(r, list):
            for sym in r:
                if sym not in tier2_emerging:
                    tier2_emerging.append(sym)

    # ── Source 3: X consensus tickers ─────────────────────────────────────────
    xc_tickers, xc_health = _x_consensus_tickers()
    source_health["x_consensus"] = xc_health
    for sym in xc_tickers[:_MAX_X_CONSENSUS]:
        sources_by_ticker.setdefault(sym, [])
        if "x_consensus" not in sources_by_ticker[sym]:
            sources_by_ticker[sym].append("x_consensus")
        if sym not in tier3_x:
            tier3_x.append(sym)
        # Don't override theme assignment if already set
        if sym not in theme_assignment:
            theme_assignment[sym] = {
                "theme_name":             "X Consensus",
                "theme_state":            "active",
                "regime_alignment_score": 0.4,
                "discovery_sources":      ["x_consensus"],
            }

    # ── Source 4: Static related_tickers fallback ──────────────────────────────
    # Any anchor ticker not already captured
    for theme in themes_to_process:
        name  = theme.get("name", "")
        state = "active" if theme in active_themes else "emerging"
        score = 0.8 if state == "active" else 0.5
        for sym in (theme.get("related_tickers") or []):
            s = sym.upper()
            sources_by_ticker.setdefault(s, [])
            src = f"static_related:{name}"
            if src not in sources_by_ticker[s]:
                sources_by_ticker[s].append(src)
            if s not in theme_assignment:
                theme_assignment[s] = {
                    "theme_name":             name,
                    "theme_state":            state,
                    "regime_alignment_score": score * 0.6,
                    "discovery_sources":      [src],
                }
            if s not in tier4_static:
                tier4_static.append(s)

    # Stamp discovery_sources from sources_by_ticker into theme_assignment
    for sym, entry in theme_assignment.items():
        entry["discovery_sources"] = sources_by_ticker.get(sym, entry.get("discovery_sources", []))

    # ── Build ordered, deduplicated universe ───────────────────────────────────
    ordered = list(dict.fromkeys([
        *tier1_active,
        *tier2_emerging,
        *tier3_x,
        *tier4_static,
    ]))
    ordered = ordered[:max_tickers]

    # Build theme_map restricted to returned tickers
    theme_map = {sym: theme_assignment[sym] for sym in ordered if sym in theme_assignment}

    source_health["etf_holdings"] = "ok" if etf_health_ok else "unavailable"
    source_health["fmp_peers"]    = "ok" if peer_health_ok else "unavailable"

    n_active   = len(tier1_active)
    n_emerging = len(tier2_emerging)
    n_xc       = len(tier3_x)
    print(
        f"[DTU] Built: {len(ordered)} tickers "
        f"(active={n_active} emerging={n_emerging} x_consensus={n_xc} static={len(tier4_static)}) "
        f"status={snapshot_status}"
    )

    return {
        "tickers":           ordered,
        "sources_by_ticker": {sym: sources_by_ticker[sym] for sym in ordered if sym in sources_by_ticker},
        "theme_map":         theme_map,
        "snapshot_status":   snapshot_status,
        "source_health":     source_health,
        "built_at":          time.time(),
        "ticker_count":      len(ordered),
        "active_count":      n_active,
        "emerging_count":    n_emerging,
    }


# ── Per-source helpers ─────────────────────────────────────────────────────────

_KEYWORD_ETF_MAP: dict[str, list[str]] = {
    # AI / Tech
    "ai":           ["SMH", "SOXX", "BOTZ"],
    "networking":   ["SRVR", "CIBR", "SMH"],
    "semiconductor": ["SMH", "SOXX", "XSD"],
    "semicap":      ["SOXX", "SMH"],
    "datacenter":   ["SRVR", "DTCR", "SMH"],
    "compute":      ["SMH", "SOXX"],
    "memory":       ["SMH", "SOXX"],
    "storage":      ["PSTG"],
    "photonics":    ["SMH", "SOXX"],
    "laser":        ["SMH"],
    "substrate":    ["SMH", "SOXX"],
    "packaging":    ["SMH", "SOXX"],
    "cloud":        ["SKYY", "CLOU", "IGV"],
    "software":     ["IGV", "SKYY"],
    "cyber":        ["CIBR", "HACK", "BUG"],
    "quantum":      ["QTUM"],
    "space":        ["ARKX", "UFO"],
    "robotics":     ["BOTZ", "ROBO"],
    "automation":   ["BOTZ", "ROBO", "ARKQ"],
    # Energy
    "nuclear":      ["URA", "URNM", "NLR"],
    "uranium":      ["URA", "URNM"],
    "oil":          ["OIH", "XES", "XLE"],
    "gas":          ["FCG", "UNG"],
    "lng":          ["FCG"],
    "energy":       ["XLE", "OIH"],
    "copper":       ["COPX", "PICK"],
    "metal":        ["PICK", "DBB"],
    "lithium":      ["LIT", "BATT"],
    "battery":      ["LIT", "BATT"],
    # Finance / Macro
    "bank":         ["KRE", "IAT", "XLF"],
    "fintech":      ["FINX", "ARKF"],
    "crypto":       ["BLOK", "IBIT", "BKCH"],
    "blockchain":   ["BLOK", "BITQ"],
    # Healthcare
    "biotech":      ["XBI", "IBB", "ARKG"],
    "medical":      ["IHI", "XLV"],
    # Consumer / Real Estate
    "housing":      ["XHB", "ITB"],
    "homebuilder":  ["XHB", "ITB"],
    # Defense
    "defense":      ["ITA", "XAR", "PPA"],
    "aerospace":    ["ITA", "XAR"],
}


def _augment_etfs_from_universe(theme_name: str, rel_etfs: list[str]) -> list[str]:
    """
    Augment rel_etfs with ETF proxy symbols from THEME_RS_UNIVERSE (canonical 60-entry
    registry) using display_name / alias word-overlap matching, plus a keyword lookup table.

    Falls back to old THEME_ETF_UNIVERSE if THEME_RS_UNIVERSE is unavailable.
    Keeps caller's list as priority; _KEYWORD_ETF_MAP is supplemental fallback.
    """
    import re as _re

    def _words(s: str) -> set:
        return set(_re.sub(r'[^a-z0-9]', ' ', s).split()) - {'and', 'or', 'the', 'of', 'a'}

    name_lower = theme_name.lower()
    name_words = _words(name_lower)
    augmented  = list(rel_etfs)

    # ── Primary: THEME_RS_UNIVERSE (canonical 60-entry registry) ──────────────
    try:
        from services.theme_rs_universe import THEME_RS_UNIVERSE

        for theme_id, meta in THEME_RS_UNIVERSE.items():
            display      = meta.get("display_name") or theme_id
            alias_list   = [a.replace("_", " ") for a in (meta.get("aliases") or [])]
            label_words  = _words(display.lower())

            # Check display_name overlap
            overlap = name_words & label_words

            # Also check aliases if no direct overlap yet
            if not overlap:
                for alias in alias_list:
                    if name_words & _words(alias.lower()):
                        overlap = True  # type: ignore[assignment]
                        break

            # Also check theme_id itself (e.g. "datacenter_infra" → "datacenter")
            if not overlap:
                tid_words = _words(theme_id.replace("_", " "))
                if name_words & tid_words:
                    overlap = True  # type: ignore[assignment]

            if overlap:
                for sym in meta.get("proxy_symbols", []):
                    if sym.upper() not in augmented:
                        augmented.append(sym.upper())

    except Exception:
        # Fallback to old THEME_ETF_UNIVERSE
        try:
            from services.sector_rotation.theme_universe import THEME_ETF_UNIVERSE
            for meta in THEME_ETF_UNIVERSE.values():
                label_words = _words((meta.get("label") or "").lower())
                if name_words & label_words:
                    for sym in meta.get("symbols", []):
                        if sym.upper() not in augmented:
                            augmented.append(sym.upper())
        except Exception:
            pass

    # ── Supplemental: keyword map for fine-grained sub-theme names ────────────
    for kw, etf_list in _KEYWORD_ETF_MAP.items():
        if kw in name_lower:
            for sym in etf_list:
                if sym not in augmented:
                    augmented.append(sym)

    return list(dict.fromkeys(augmented))[:_MAX_ETF_PER_THEME + 3]


async def _etf_holdings_tickers(
    proxy_etfs: list[str],
) -> tuple[list[str], dict[str, list[str]]]:
    """
    Fetch top holdings from each proxy ETF.
    Returns (unique_tickers, sources_by_ticker).
    sources_by_ticker: {sym: ["etf_holding:XYZ"]}
    """
    if not proxy_etfs:
        return [], {}

    from services.sector_rotation.etf_holdings_service import get_etf_holdings

    async def _one_etf(etf_sym: str) -> tuple[str, list[dict]]:
        try:
            data = await asyncio.wait_for(get_etf_holdings(etf_sym), timeout=10.0)
            return etf_sym, (data.get("holdings") or [])[:_ETF_HOLDINGS_TOP_N]
        except Exception as e:
            print(f"[DTU] ETF holdings error {etf_sym}: {e}")
            return etf_sym, []

    results = await asyncio.gather(*[_one_etf(e) for e in proxy_etfs])

    tickers: list[str]             = []
    sources: dict[str, list[str]]  = {}

    for etf_sym, holdings in results:
        src_label = f"etf_holding:{etf_sym}"
        for h in holdings:
            sym = (h.get("ticker") or "").upper()
            if not sym or len(sym) > 6:
                continue
            # Skip the ETF itself and common bond/cash entries
            if sym in (etf_sym, "CASH", "USD", "EUR", "TBD"):
                continue
            sources.setdefault(sym, [])
            if src_label not in sources[sym]:
                sources[sym].append(src_label)
            if sym not in tickers:
                tickers.append(sym)

    return tickers, sources


async def _fmp_peers_tickers(
    anchor_tickers: list[str],
) -> tuple[list[str], dict[str, list[str]]]:
    """
    Fetch FMP stock peers for each anchor ticker.
    Returns (unique_tickers, sources_by_ticker).
    sources_by_ticker: {sym: ["peer:ANCHOR"]}
    """
    if not anchor_tickers:
        return [], {}

    api_key = os.getenv("FMP_API_KEY", "")
    if not api_key:
        return [], {}

    import httpx

    async def _one_peer(anchor: str) -> tuple[str, list[str]]:
        try:
            async with httpx.AsyncClient(timeout=_FMP_TIMEOUT) as client:
                resp = await client.get(
                    _FMP_PEERS_URL,
                    params={"symbol": anchor.upper(), "apikey": api_key},
                )
            if resp.status_code != 200:
                return anchor, []
            raw = resp.json()
            # FMP returns: [{"symbol": "ASML", ...}, ...] or just a list of strings
            # Stable endpoint returns list of peer strings or list of dicts
            peers: list[str] = []
            if isinstance(raw, list):
                for item in raw:
                    if isinstance(item, str):
                        peers.append(item.upper())
                    elif isinstance(item, dict):
                        sym = (item.get("symbol") or item.get("peersList") or "")
                        if isinstance(sym, str) and sym:
                            peers.append(sym.upper())
                        elif isinstance(sym, list):
                            for s in sym:
                                if isinstance(s, str) and s:
                                    peers.append(s.upper())
            elif isinstance(raw, dict):
                # Some FMP responses wrap in {"peersList": [...]}
                pl = raw.get("peersList") or raw.get("peers") or []
                peers = [str(s).upper() for s in pl if s]
            return anchor, peers[:_MAX_PEERS_PER_ANCHOR]
        except Exception as e:
            print(f"[DTU] FMP peers error {anchor}: {e}")
            return anchor, []

    results = await asyncio.gather(*[_one_peer(a) for a in anchor_tickers[:_MAX_PEER_ANCHORS]])

    tickers: list[str]            = []
    sources: dict[str, list[str]] = {}

    for anchor, peers in results:
        src_label = f"peer:{anchor.upper()}"
        for sym in peers:
            if not sym or len(sym) > 6:
                continue
            sources.setdefault(sym, [])
            if src_label not in sources[sym]:
                sources[sym].append(src_label)
            if sym not in tickers:
                tickers.append(sym)

    return tickers, sources


def _x_consensus_tickers() -> tuple[list[str], str]:
    """
    Read X/Grok consensus tickers from the disk snapshot.
    Returns (tickers, health_status).
    """
    try:
        if not _XC_PATH.exists():
            return [], "missing"
        raw = json.loads(_XC_PATH.read_text())
        # Check age
        saved_at = float(raw.get("_saved_at", raw.get("generated_at_ts", 0)))
        if saved_at and time.time() - saved_at > _XC_MAX_AGE:
            return [], "stale"
        top = raw.get("top_tickers") or []
        tickers = []
        for entry in top:
            if isinstance(entry, str):
                tickers.append(entry.upper())
            elif isinstance(entry, dict):
                sym = (entry.get("ticker") or entry.get("symbol") or "").upper()
                if sym:
                    tickers.append(sym)
        return tickers[:_MAX_X_CONSENSUS], "ok"
    except Exception as e:
        print(f"[DTU] X consensus read error: {e}")
        return [], "error"


# ── Utility ────────────────────────────────────────────────────────────────────

def _empty_result(status: str = "cold") -> dict:
    return {
        "tickers":           [],
        "sources_by_ticker": {},
        "theme_map":         {},
        "snapshot_status":   status,
        "source_health":     {},
        "built_at":          None,
        "ticker_count":      0,
        "active_count":      0,
        "emerging_count":    0,
    }
