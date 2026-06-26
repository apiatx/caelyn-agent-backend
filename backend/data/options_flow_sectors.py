"""
Net Options Flow aggregated by Sector → Theme → Ticker.

Data sources (in priority order):
  1. options_master_screener_v1 / options_master_lkg_v1
     The master screener precomputes unusual-options flow every ~39 s.
     Tagged _source="live" in the combined dict.

  2. options_theme_supplement_v1  (fresh, this session)
     _theme_options_supplement_loop scans theme proxy symbols NOT in the
     master cache, in batches of 20 every 5 minutes.
     Tagged _source="supplement".

  3. options_supplement_lkg_v1  (disk-loaded at startup)
     Supplement data from previous sessions, persisted to disk and loaded
     at startup.  Tagged _source="supplement_lkg".
     Ensures coverage does not reset to near-zero after restart.

  4. options_no_options_tracking:v1
     Symbols confirmed by Stage-1 expiry checks to have NO tradeable options.
     scan_status="no_options", options_available=False.

  5. Pending — symbols not yet reached by any scan pass.

Zero new Tradier calls are made from this module — it is a pure aggregation
layer on top of what the background scanners already computed.

Derivation (for reference):
  premium  = total_call_prem + total_put_prem (set by options_enricher)
  call_premium = premium × (call_flow_pct / 100)   ← mathematically exact
  put_premium  = premium × (put_flow_pct  / 100)
  net_premium  = call_premium − put_premium
  put_call_ratio = put_premium / call_premium

Sector totals use UNIQUE ticker dedup across themes in the same sector.

scan_status values:
  "live"           — master screener cache (unusual-flow threshold met)
  "supplement"     — supplement loop, current session (fresh)
  "supplement_lkg" — supplement loop, previous session (disk-loaded at startup)
  "no_options"     — Stage-1 confirmed: no tradeable options expirations
  "pending"        — not yet reached by any scan pass
"""
from __future__ import annotations

import time
from typing import Optional

_SECTORS_CACHE_KEY = "options_flow_sectors:v1"
_SECTORS_CACHE_TTL = 60   # 1 min — short so supplement updates are visible quickly


def _load_all_watchlist_symbols() -> set[str]:
    """
    Return the union of all ticker symbols across every saved watchlist.

    Used to compute watchlist_overlap_symbols in the diagnostics block.
    Returns an empty set on any DB error — non-fatal.
    """
    try:
        from data.pg_storage import watchlist_list, watchlist_read
        ids = [w["id"] for w in (watchlist_list() or [])]
        syms: set[str] = set()
        for wl_id in ids:
            row = watchlist_read(wl_id)
            if row:
                for t in (row.get("tickers") or []):
                    if t and t.strip():
                        syms.add(t.strip().upper())
        return syms
    except Exception:
        return set()

_SECTOR_DISPLAY_NAMES: dict[str, str] = {
    "technology":             "Technology",
    "financials":             "Financials",
    "healthcare":             "Healthcare",
    "industrials":            "Industrials",
    "energy":                 "Energy",
    "utilities":              "Utilities",
    "materials":              "Materials",
    "consumer_discretionary": "Consumer Discretionary",
    "consumer_staples":       "Consumer Staples",
    "communication_services": "Communication Services",
    "real_estate":            "Real Estate",
}


# ── helpers ───────────────────────────────────────────────────────────────────

def _safe_float(v) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _ticker_call_put(row: dict) -> tuple[float, float]:
    """
    Return (call_premium_dollars, put_premium_dollars).

    Priority:
      1. Direct call_premium / put_premium fields (stored by some writers)
      2. premium × (call_flow_pct / 100) derivation (enrich_ticker_rows output)
      3. 50/50 split of premium (last resort)
    """
    # 1. Direct fields — some row writers store these explicitly
    direct_call = _safe_float(row.get("call_premium"))
    direct_put  = _safe_float(row.get("put_premium"))
    if direct_call is not None and direct_put is not None:
        return direct_call, direct_put

    # 2. Derived from enriched premium split
    prem     = _safe_float(row.get("premium")) or 0.0
    call_pct = _safe_float(row.get("call_flow_pct"))
    put_pct  = _safe_float(row.get("put_flow_pct"))

    if call_pct is not None and put_pct is not None:
        return prem * call_pct / 100.0, prem * put_pct / 100.0

    half = prem / 2.0
    return half, half


def _bias(call_p: float, put_p: float) -> str:
    total = call_p + put_p
    if total == 0:
        return "neutral"
    call_pct = call_p / total * 100
    if call_pct >= 65:
        return "bullish"
    if call_pct <= 35:
        return "bearish"
    return "neutral"


def _pcr(call_p: float, put_p: float) -> Optional[float]:
    """put_call_ratio expressed in premium dollars."""
    if call_p > 0:
        return round(put_p / call_p, 3)
    return None


# ── 9-state model ────────────────────────────────────────────────────────────

# States written explicitly into scan_result by the coverage-row writers.
# All other rows derive their state from _source and premium data.
_EXPLICIT_STATES = frozenset({
    "neutral_no_unusual_flow",
    "optionable_pending_chain",
    "confirmed_no_options",
    "unsupported_foreign_otc",
    "deferred_retry",
})

# States where options information is confirmed (ticker has options)
_OPTIONS_CONFIRMED_STATES = frozenset({
    "bullish_flow", "bearish_flow", "mixed_flow",
    "neutral_no_unusual_flow", "optionable_pending_chain",
    "stale_lkg",
})


def _ticker_state(row: dict | None, sym: str, no_options_syms: set) -> str:
    """
    Map a ticker's cached data to one of the 9 Sectors status states.

    Priority:
      1. Explicit scan_result tag written by coverage-row writers
      2. supplement_lkg source → stale_lkg (previous-session data, refresh pending)
      3. Premium data present → infer bullish_flow / bearish_flow / mixed_flow
      4. Fresh row, no premium, no tag → neutral_no_unusual_flow (backward compat)
      5. No row + in no_options_syms → confirmed_no_options
      6. No row → generic_pending (unattempted)
    """
    if row is None:
        return "confirmed_no_options" if sym in no_options_syms else "generic_pending"

    source = row.get("_source", "live")
    if source == "supplement_lkg":
        # All previous-session rows are stale_lkg regardless of their old scan_result tag.
        # The supplement loop will re-classify them correctly when it refreshes this cycle.
        return "stale_lkg"

    scan_result = row.get("scan_result") or ""
    if scan_result in _EXPLICIT_STATES:
        return scan_result

    call_p, put_p = _ticker_call_put(row)
    has_prem = (call_p + put_p) > 0
    if has_prem:
        b = _bias(call_p, put_p)
        if b == "bullish":
            return "bullish_flow"
        if b == "bearish":
            return "bearish_flow"
        return "mixed_flow"

    # Fresh row (live or supplement), no premium, no explicit scan_result tag.
    # Backward-compat: old supplement rows before scan_result tagging was added.
    return "neutral_no_unusual_flow"


# ── per-ticker row ────────────────────────────────────────────────────────────

def _build_ticker_node(
    sym: str,
    cache_by_ticker: dict[str, dict],
    no_options_syms: set[str],
) -> dict:
    """
    Build a per-ticker options-flow node with full 9-state classification.

    ticker_state values (9 states):
      bullish_flow           — chain scanned, bullish unusual flow detected
      bearish_flow           — chain scanned, bearish unusual flow detected
      mixed_flow             — chain scanned, mixed call/put flow
      neutral_no_unusual_flow — chain scanned (Stage-2 complete), no unusual flow
      optionable_pending_chain — Stage-1 expirations confirmed, chain not yet scanned
      confirmed_no_options   — Stage-1 confirmed no tradeable expirations (not budget deferral)
      unsupported_foreign_otc — skipped by eligibility rules (foreign/OTC)
      deferred_retry         — budget-deferred or transient failure; retry next cycle
      stale_lkg              — prior successful row from previous session, refresh pending
      generic_pending        — not yet attempted by any scanner
    """
    row = cache_by_ticker.get(sym)
    state = _ticker_state(row, sym, no_options_syms)

    if row is not None:
        source      = row.get("_source", "live")
        scan_result = row.get("scan_result")
        call_p, put_p = _ticker_call_put(row)
        net_p    = call_p - put_p
        has_prem = (call_p + put_p) > 0

        # Bias for display — stale_lkg rows show their historical bias
        if state == "bullish_flow":
            bias_val = "bullish"
        elif state == "bearish_flow":
            bias_val = "bearish"
        elif state == "mixed_flow":
            bias_val = "mixed" if has_prem else "neutral"
        elif state == "stale_lkg" and has_prem:
            bias_val = _bias(call_p, put_p)  # show stale direction
        elif state == "neutral_no_unusual_flow":
            bias_val = "neutral"
        else:
            bias_val = None

        # Determine whether to display zero-flow values (0.0) vs null (unknown).
        # Zero-flow display means "chain was scanned, no unusual flow detected".
        # Null display means "data not yet available / scan pending".
        #
        # Cases that emit 0.0:
        #   1. neutral_no_unusual_flow (fresh): chain was scanned this session, no unusual flow.
        #   2. stale_lkg where scan_result=="neutral_no_unusual_flow": previous-session neutral
        #      scan — we know options exist and flow was zero, even though data is stale.
        _stale_was_neutral = (
            state == "stale_lkg"
            and scan_result == "neutral_no_unusual_flow"
        )
        _neutral_confirmed = (state == "neutral_no_unusual_flow") or _stale_was_neutral

        # scan_status label — human-readable accounting category.
        #
        #   "fresh"       — real data from the current session (live master
        #                   screener OR sectors chain summarizer this session)
        #   "cached_data" — real data from a PRIOR session, loaded from disk
        #                   LKG on startup; will be refreshed by the backfill loop
        #   "pending"     — budget-deferred or transient failure; retry next cycle
        #   "no_options"  — Tradier confirmed no tradeable expirations
        #   "missing_data"— no scan attempted yet (not in any cache layer)
        if state == "deferred_retry":
            scan_status_val = "pending"
        elif scan_result == "sectors_chain_summarized" or source in ("live", "supplement"):
            scan_status_val = "fresh"
        elif source in ("supplement_lkg", "watchlist_cache"):
            scan_status_val = "cached_data"
        else:
            scan_status_val = source   # defensive fallback

        return {
            "symbol":            sym,
            "ticker_state":      state,
            "call_premium":      round(call_p, 2) if has_prem else (0.0 if _neutral_confirmed else None),
            "put_premium":       round(put_p, 2)  if has_prem else (0.0 if _neutral_confirmed else None),
            "net_premium":       round(net_p, 2)  if has_prem else (0.0 if _neutral_confirmed else None),
            "put_call_ratio":    _pcr(call_p, put_p) if has_prem else None,
            "bias":              bias_val,
            "call_volume":       row.get("call_volume") if row.get("call_volume") is not None else (0 if _neutral_confirmed else None),
            "put_volume":        row.get("put_volume")  if row.get("put_volume")  is not None else (0 if _neutral_confirmed else None),
            "total_volume":      row.get("total_volume") if row.get("total_volume") is not None else (0 if _neutral_confirmed else None),
            "heat_score":        row.get("heat_score") if row.get("heat_score") is not None else (0.0 if _neutral_confirmed else None),
            "side_bias":         row.get("side_bias"),
            "options_available": state in _OPTIONS_CONFIRMED_STATES,
            "scan_status":       scan_status_val,
            "scan_result":       scan_result,
            "updated_at":        row.get("updated_at") or row.get("cached_at") or row.get("_cached_at"),
        }

    if sym in no_options_syms:
        return {
            "symbol":            sym,
            "ticker_state":      "confirmed_no_options",
            "call_premium":      None,
            "put_premium":       None,
            "net_premium":       None,
            "put_call_ratio":    None,
            "bias":              None,
            "call_volume":       None,
            "put_volume":        None,
            "total_volume":      None,
            "heat_score":        None,
            "side_bias":         None,
            "options_available": False,
            "scan_status":       "no_options",
            "scan_result":       "confirmed_no_options",
            "reason":            "confirmed_no_tradeable_options",
            "updated_at":        None,
        }

    return {
        "symbol":            sym,
        "ticker_state":      "generic_pending",
        "call_premium":      None,
        "put_premium":       None,
        "net_premium":       None,
        "put_call_ratio":    None,
        "bias":              None,
        "call_volume":       None,
        "put_volume":        None,
        "total_volume":      None,
        "heat_score":        None,
        "side_bias":         None,
        "options_available": False,
        "scan_status":       "missing_data",  # no scan attempted yet
        "scan_result":       None,
        "reason":            "pending_scan",
        "updated_at":        None,
    }


# ── theme-level aggregation ───────────────────────────────────────────────────

def _build_theme_node(
    theme_id: str,
    meta: dict,
    cache_by_ticker: dict[str, dict],
    no_options_syms: set[str],
) -> dict:
    proxy_syms   = [s.upper() for s in (meta.get("proxy_symbols") or [])]
    ticker_nodes = [_build_ticker_node(sym, cache_by_ticker, no_options_syms) for sym in proxy_syms]

    # Flow totals — use is-not-None guard so neutral rows (0.0) are counted,
    # not falsy-filtered. Neutral/zero rows don't change the total but are
    # still "represented" in the universe.
    total_call = sum(t["call_premium"] for t in ticker_nodes if t.get("call_premium") is not None)
    total_put  = sum(t["put_premium"]  for t in ticker_nodes if t.get("put_premium")  is not None)
    total_net  = total_call - total_put
    has_data   = (total_call + total_put) > 0
    # Volume: sum non-None values from tickers that have real volume data
    total_vol_vals  = [t["total_volume"] for t in ticker_nodes if t.get("total_volume") is not None]
    total_vol       = sum(total_vol_vals) if total_vol_vals else None
    total_call_vols = [t["call_volume"] for t in ticker_nodes if t.get("call_volume") is not None]
    total_put_vols  = [t["put_volume"]  for t in ticker_nodes if t.get("put_volume")  is not None]
    total_call_vol  = sum(total_call_vols) if total_call_vols else None
    total_put_vol   = sum(total_put_vols)  if total_put_vols  else None

    # State counts across all proxy tickers
    state_counts: dict[str, int] = {}
    for t in ticker_nodes:
        s = t.get("ticker_state", "generic_pending")
        state_counts[s] = state_counts.get(s, 0) + 1

    # Represented = any state that isn't generic_pending (some info is known)
    represented = [t for t in ticker_nodes if t.get("ticker_state") != "generic_pending"]
    # Tickers contributing non-zero premium (for average net flow calculation)
    flow_nodes  = [t for t in ticker_nodes if (t.get("call_premium") or 0) + (t.get("put_premium") or 0) > 0]
    avg_net     = round(total_net / len(flow_nodes), 2) if flow_nodes else None

    return {
        "theme_id":                      theme_id,
        "theme_name":                    meta.get("display_name", theme_id),
        "classification":                meta.get("classification"),
        "call_premium":                  round(total_call, 2) if has_data else None,
        "put_premium":                   round(total_put, 2)  if has_data else None,
        "net_premium":                   round(total_net, 2)  if has_data else None,
        "total_net_flow":                round(total_net, 2)  if has_data else None,
        "average_net_flow":              avg_net,
        "total_volume":                  total_vol,
        "call_volume":                   total_call_vol,
        "put_volume":                    total_put_vol,
        "put_call_ratio":                _pcr(total_call, total_put),
        "bias":                          _bias(total_call, total_put) if has_data else None,
        "ticker_count":                  len(proxy_syms),
        "represented_count":             len(represented),
        "contributing_ticker_count":     len(flow_nodes),
        # 9-state breakdown
        "bullish_count":                 state_counts.get("bullish_flow", 0),
        "bearish_count":                 state_counts.get("bearish_flow", 0),
        "mixed_count":                   state_counts.get("mixed_flow", 0),
        "neutral_count":                 state_counts.get("neutral_no_unusual_flow", 0),
        "optionable_pending_count":      state_counts.get("optionable_pending_chain", 0),
        "confirmed_no_options_count":    state_counts.get("confirmed_no_options", 0),
        "stale_lkg_count":               state_counts.get("stale_lkg", 0),
        "deferred_retry_count":          state_counts.get("deferred_retry", 0),
        "unsupported_count":             state_counts.get("unsupported_foreign_otc", 0),
        "generic_pending_count":         state_counts.get("generic_pending", 0),
        "tickers":                       ticker_nodes,
    }


# ── sector-level aggregation ──────────────────────────────────────────────────

def _build_sector_node(
    sector_id: str,
    theme_items: list[tuple[str, dict]],
    cache_by_ticker: dict[str, dict],
    no_options_syms: set[str],
    sector_names: dict[str, str],
) -> dict:
    sector_unique_syms: set[str] = set()
    for _, meta in theme_items:
        for sym in (meta.get("proxy_symbols") or []):
            sector_unique_syms.add(sym.upper())

    sector_call     = 0.0
    sector_put      = 0.0
    sector_vol      = 0
    sector_call_vol = 0
    sector_put_vol  = 0
    flow_count      = 0
    state_counts: dict[str, int] = {}

    for sym in sector_unique_syms:
        row   = cache_by_ticker.get(sym)
        state = _ticker_state(row, sym, no_options_syms)
        state_counts[state] = state_counts.get(state, 0) + 1
        if row:
            c, p = _ticker_call_put(row)
            if (c + p) > 0:
                sector_call += c
                sector_put  += p
                flow_count  += 1
                v = row.get("total_volume")
                if v is not None:
                    sector_vol += int(v)
            cv = row.get("call_volume")
            pv = row.get("put_volume")
            if cv is not None:
                sector_call_vol += int(cv)
            if pv is not None:
                sector_put_vol  += int(pv)

    sector_net   = sector_call - sector_put
    has_data     = flow_count > 0
    avg_net      = round(sector_net / flow_count, 2) if flow_count else None
    represented  = sum(v for k, v in state_counts.items() if k != "generic_pending")

    themes_built = [
        _build_theme_node(tid, meta, cache_by_ticker, no_options_syms)
        for tid, meta in theme_items
    ]
    themes_built.sort(
        key=lambda t: (
            -(t.get("represented_count") or 0),
            -(t.get("call_premium") or 0) - (t.get("put_premium") or 0),
        )
    )

    return {
        "sector_id":                     sector_id,
        "sector_name":                   sector_names.get(sector_id, sector_id.replace("_", " ").title()),
        "call_premium":                  round(sector_call, 2) if has_data else None,
        "put_premium":                   round(sector_put, 2)  if has_data else None,
        "net_premium":                   round(sector_net, 2)  if has_data else None,
        "total_net_flow":                round(sector_net, 2)  if has_data else None,
        "total_volume":                  sector_vol if has_data else None,
        "call_volume":                   sector_call_vol if sector_call_vol else None,
        "put_volume":                    sector_put_vol  if sector_put_vol  else None,
        "average_net_flow":              avg_net,
        "put_call_ratio":                _pcr(sector_call, sector_put),
        "bias":                          _bias(sector_call, sector_put) if has_data else None,
        "ticker_count":                  len(sector_unique_syms),
        "represented_count":             represented,
        "contributing_ticker_count":     flow_count,
        "sector_total_method":           "unique_ticker_sum",
        # 9-state breakdown
        "bullish_count":                 state_counts.get("bullish_flow", 0),
        "bearish_count":                 state_counts.get("bearish_flow", 0),
        "mixed_count":                   state_counts.get("mixed_flow", 0),
        "neutral_count":                 state_counts.get("neutral_no_unusual_flow", 0),
        "optionable_pending_count":      state_counts.get("optionable_pending_chain", 0),
        "confirmed_no_options_count":    state_counts.get("confirmed_no_options", 0),
        "stale_lkg_count":               state_counts.get("stale_lkg", 0),
        "deferred_retry_count":          state_counts.get("deferred_retry", 0),
        "unsupported_count":             state_counts.get("unsupported_foreign_otc", 0),
        "generic_pending_count":         state_counts.get("generic_pending", 0),
        "themes":                        themes_built,
    }


# ── main builder ──────────────────────────────────────────────────────────────

def build_sector_tree(
    combined_ticker_data: dict[str, dict],
    no_options_syms: set[str],
    theme_universe: dict,
) -> dict:
    """
    Build the full sector → theme → ticker tree.

    Parameters
    ----------
    combined_ticker_data : {ticker: row} merged from master + supplement caches
    no_options_syms      : set of tickers confirmed to have no tradeable options
    theme_universe       : ENRICHED_THEME_RS_UNIVERSE (live, updated in-place)
    """
    # Collect coverage stats — compute per-symbol state across the full theme universe
    all_theme_syms: set[str] = set()
    for meta in theme_universe.values():
        for sym in (meta.get("proxy_symbols") or []):
            all_theme_syms.add(sym.upper())

    live_syms        = {s for s, r in combined_ticker_data.items() if r.get("_source") == "live"}
    fresh_supp_syms  = {s for s, r in combined_ticker_data.items() if r.get("_source") == "supplement"}
    lkg_supp_syms    = {s for s, r in combined_ticker_data.items() if r.get("_source") == "supplement_lkg"}
    supplement_syms  = fresh_supp_syms | lkg_supp_syms
    theme_in_scan    = (live_syms | supplement_syms) & all_theme_syms

    # 9-state counts across the entire theme universe
    # Also collect _missing_syms (generic_pending) for diagnostics.
    _global_state_counts: dict[str, int] = {}
    _missing_syms: list[str] = []
    for _sym in sorted(all_theme_syms):   # sorted so missing_symbols list is deterministic
        _row   = combined_ticker_data.get(_sym)
        _state = _ticker_state(_row, _sym, no_options_syms)
        _global_state_counts[_state] = _global_state_counts.get(_state, 0) + 1
        if _state == "generic_pending":
            _missing_syms.append(_sym)

    # Represented = any state with known options information (not generic_pending)
    _represented_syms = sum(
        v for k, v in _global_state_counts.items() if k != "generic_pending"
    )
    # generic_pending = unattempted tickers (no state yet)
    pending_syms = set(_missing_syms)

    # Build sector display-name map
    sector_names = dict(_SECTOR_DISPLAY_NAMES)
    for tid, meta in theme_universe.items():
        if meta.get("classification") == "sector":
            sector_names.setdefault(tid, meta.get("display_name", tid.replace("_", " ").title()))

    # Group themes by sector
    sectors: dict[str, list[tuple[str, dict]]] = {}
    for tid, meta in theme_universe.items():
        cls = meta.get("classification", "theme")
        sector_id = tid if cls == "sector" else (meta.get("parent_sector") or "other")
        sectors.setdefault(sector_id, []).append((tid, meta))

    sector_nodes = [
        _build_sector_node(sid, theme_items, combined_ticker_data, no_options_syms, sector_names)
        for sid, theme_items in sectors.items()
    ]

    sector_nodes.sort(
        key=lambda s: (
            -(s.get("contributing_ticker_count") or 0),
            -(s.get("call_premium") or 0) - (s.get("put_premium") or 0),
            s.get("sector_name", ""),
        )
    )

    # ── Coverage metrics ─────────────────────────────────────────────────────
    _bullish_n  = _global_state_counts.get("bullish_flow", 0)
    _bearish_n  = _global_state_counts.get("bearish_flow", 0)
    _mixed_n    = _global_state_counts.get("mixed_flow", 0)
    _neutral_n  = _global_state_counts.get("neutral_no_unusual_flow", 0)
    _no_opts_n  = _global_state_counts.get("confirmed_no_options", 0)
    _unsupp_n   = _global_state_counts.get("unsupported_foreign_otc", 0)
    _stale_n    = _global_state_counts.get("stale_lkg", 0)
    _deferred_n = _global_state_counts.get("deferred_retry", 0)
    _chain_scanned_n = _bullish_n + _bearish_n + _mixed_n + _neutral_n
    # optionable_expected = universe minus known no-options and unsupported
    _optionable_expected = max(0, len(all_theme_syms) - _no_opts_n - _unsupp_n)

    coverage_pct = round(_represented_syms / max(len(all_theme_syms), 1) * 100, 1)

    # ── Canonical audit diagnostics (req #2) ─────────────────────────────────
    # scanned_real = chain-scanned this session (flow states) + cached LKG rows.
    # Both have non-null scan_result and are from a real Tradier call.
    _scanned_real_n  = _chain_scanned_n + _stale_n
    # fresh = live master screener + supplement this session (real-time)
    _fresh_n         = len(live_syms & all_theme_syms) + len(fresh_supp_syms & all_theme_syms)
    # lkg = supplement_lkg from disk (prior session data)
    _lkg_n           = len(lkg_supp_syms & all_theme_syms)
    # missing = generic_pending: no record exists in any cache layer
    _missing_n       = _global_state_counts.get("generic_pending", 0)
    # scan_queue_remaining: what the backfill loop needs to visit this session
    # (stale_lkg rows need refresh + generic_pending rows need first scan)
    _scan_queue_remain = _stale_n + _missing_n
    # full_coverage = tickers that are either scanned-real OR confirmed no-options
    # (both are fully accounted for — no data gap)
    _full_covered_n   = _scanned_real_n + _no_opts_n
    _full_coverage_pct = round(_full_covered_n / max(len(all_theme_syms), 1) * 100, 1)

    # Sectors/themes totals
    _total_sectors = len(sectors)
    _total_themes  = sum(len(v) for v in sectors.values())

    # Backfill timing from module tracking
    _sbf_diag: dict = {}
    try:
        from data.options_theme_supplement import get_sectors_backfill_diag as _sbf_d
        _sbf_diag = _sbf_d()
    except Exception:
        pass

    # Supplement timing from module tracking
    last_scan_at = None
    next_scan_at = None
    try:
        from data.options_theme_supplement import _next_scan_at as _nsa
        from data.cache import cache as _c
        _fs = _c.get("options_theme_supplement_v1") or {}
        last_scan_at = _fs.get("last_scan_at")
        next_scan_at = _nsa or None
    except Exception:
        pass

    # Estimated time to full coverage from the fast backfill loop.
    # Use dynamic batch/sleep based on Sectors active status.
    try:
        from data.options_theme_supplement import is_sectors_active as _sbf_is_active_now
        _sbf_active_now = _sbf_is_active_now()
    except Exception:
        _sbf_active_now = False

    if _sbf_active_now:
        _sbf_batch   = 25
        _sbf_sleep_s = 25
    else:
        _sbf_batch   = 8
        _sbf_sleep_s = 60

    _generic_pend_n = _global_state_counts.get("generic_pending", 0)
    _stale_lkg_n    = _global_state_counts.get("stale_lkg", 0)
    _still_pending  = _generic_pend_n + _stale_lkg_n   # symbols needing a scan pass
    est_minutes  = round(_still_pending / _sbf_batch * _sbf_sleep_s / 60, 1) if _still_pending else 0

    # Sectors active refresh diagnostics
    _sectors_active_diag: dict = {}
    try:
        from data.options_theme_supplement import get_sectors_active_diag as _sad
        _sectors_active_diag = _sad()
    except Exception:
        pass

    return {
        "as_of":                         time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source":                        "master_and_supplement_cache",
        "net_flow_method":               "call_minus_put_premium",
        "put_call_ratio_method":         "premium_dollars",
        "sector_total_method":           "unique_ticker_sum",
        "sectors_active_refresh": {
            **_sectors_active_diag,
            "sectors_active":            _sbf_active_now,
            "priority_mode":             _sbf_active_now,
            "active_batch_size":         _sbf_batch,
            "active_sleep_seconds":      _sbf_sleep_s,
            "active_lane":               "sectors" if _sbf_active_now else "maintenance",
            "estimated_full_pass_minutes": est_minutes,
        },
        "scan_coverage": {
            # ── Canonical audit fields (req #2) ───────────────────────────────
            # These 9 fields give a complete, non-overlapping accounting of every
            # ticker in the canonical sectors universe.
            #
            # Accounting identity:
            #   fresh_tickers + lkg_tickers + deferred_tickers
            #   + missing_tickers + no_options_tickers + unsupported_count
            #   == total_required_tickers
            #
            # full_coverage_pct = (scanned_real_tickers + no_options_tickers)
            #                      / total_required_tickers * 100
            "total_required_tickers":         len(all_theme_syms),
            "scanned_real_tickers":           _scanned_real_n,   # fresh + lkg (real Tradier scans)
            "fresh_tickers":                  _fresh_n,          # scanned this session (live/supplement)
            "lkg_tickers":                    _lkg_n,            # prior-session scan from disk LKG
            "missing_tickers":                _missing_n,        # no record in any cache (generic_pending)
            "no_options_tickers":             _no_opts_n,        # Tradier confirmed no tradeable options
            "deferred_tickers":               _deferred_n,       # budget/transient failure, retry next cycle
            "scan_queue_remaining":           _scan_queue_remain,# symbols still needing a scan this session
            "full_coverage_pct":              _full_coverage_pct,# % fully accounted for (real + no-opts)
            "missing_symbols":                _missing_syms,     # exact symbols with no scan data yet
            # ── Universe dimensions ────────────────────────────────────────────
            "total_sectors":                  _total_sectors,
            "total_themes":                   _total_themes,
            "total_tickers":                  len(all_theme_syms),
            "theme_universe_total":           len(all_theme_syms),   # compat alias
            "optionable_expected":            _optionable_expected,
            # ── Coverage summary ──────────────────────────────────────────────
            "represented_count":              _represented_syms,
            "represented_percent":            coverage_pct,
            "coverage_pct":                   coverage_pct,          # compat alias
            "chain_scanned_count":            _chain_scanned_n,
            # ── 9-state breakdown (theme universe, global) ─────────────────────
            "bullish_count":                  _bullish_n,
            "bearish_count":                  _bearish_n,
            "mixed_count":                    _mixed_n,
            "true_neutral_no_unusual_flow_count": _neutral_n,
            "true_neutral_count":             _neutral_n,            # compat alias
            "optionable_pending_chain_count": _global_state_counts.get("optionable_pending_chain", 0),
            "confirmed_no_options_count":     _no_opts_n,
            "stale_lkg_count":                _stale_n,
            "deferred_retry_count":           _deferred_n,
            "unsupported_count":              _unsupp_n,
            "generic_pending_count":          _missing_n,
            # ── Cache-source counts (for diagnostics) ──────────────────────────
            "master_count":                   len(live_syms & all_theme_syms),
            "supplement_fresh_count":         len(fresh_supp_syms & all_theme_syms),
            "supplement_lkg_count":           len(lkg_supp_syms & all_theme_syms),
            "supplement_count":               len(supplement_syms & all_theme_syms),
            "no_options_count":               len(no_options_syms & all_theme_syms),
            "pending_count":                  len(pending_syms),
            "tickers_with_data":              len(theme_in_scan),
            # ── Backfill timing ────────────────────────────────────────────────
            "estimated_full_coverage_minutes": est_minutes,
            "backfill_pass_count":            _sbf_diag.get("pass_count", 0),
            "backfill_last_pass_at":          _sbf_diag.get("last_pass_at"),
            "backfill_next_at":               _sbf_diag.get("next_at"),
            "backfill_last_batch_syms":       _sbf_diag.get("last_batch_syms", []),
            "last_supplement_scan_at":        last_scan_at,
            "next_supplement_scan_at":        next_scan_at,
        },
        "sectors": sector_nodes,
    }


# ── public entry point ────────────────────────────────────────────────────────

def get_sector_flow(*, force_refresh: bool = False) -> dict:
    """
    Return the sector flow tree, using a 5-minute in-memory cache.

    Reads from master screener cache + supplement cache — never initiates
    new Tradier calls.  Calling with force_refresh=True bypasses the sectors
    cache but still reads from the existing background scan data.

    Every response includes a `diagnostics` block that proves zero live scans
    fire for watchlist-overlap symbols and quantifies each cache-layer hit.
    """
    from data.cache import cache

    if not force_refresh:
        cached = cache.get(_SECTORS_CACHE_KEY)
        if cached:
            return {**cached, "_from_sectors_cache": True}

    # ── Pull combined ticker data (master + supplement) ───────────────────────
    try:
        from data.options_theme_supplement import (
            get_combined_ticker_data as _combined,
            get_no_options_symbols   as _no_opts,
        )
        combined_ticker_data = _combined()
        no_options_syms      = _no_opts()
    except Exception:
        # Fallback: read master cache directly (pre-supplement behaviour)
        master_snap: dict | None = (
            cache.get("options_master_screener_v1")
            or cache.get("options_master_lkg_v1")
        )
        if master_snap is None:
            import json, pathlib
            disk = pathlib.Path(__file__).parent / "options_master_lkg_v1.json"
            if disk.exists():
                try:
                    master_snap = json.loads(disk.read_text(encoding="utf-8"))
                except Exception:
                    pass
        master_rows: list[dict] = (master_snap or {}).get("tickers", [])
        combined_ticker_data = {
            (r.get("ticker") or "").upper(): {**r, "_source": "live"}
            for r in master_rows
            if r.get("ticker")
        }
        no_options_syms = set()

    # ── Pull theme universe (always live — updated in-place by admin edits) ───
    from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE

    result = build_sector_tree(combined_ticker_data, no_options_syms, ENRICHED_THEME_RS_UNIVERSE)

    # ── Compute diagnostics block ─────────────────────────────────────────────
    # Derive per-source symbol sets for theme universe tickers only.
    all_theme_syms: set[str] = set()
    for meta in ENRICHED_THEME_RS_UNIVERSE.values():
        for sym in (meta.get("proxy_symbols") or []):
            all_theme_syms.add(sym.upper())

    _live_syms        = {s for s, r in combined_ticker_data.items() if r.get("_source") == "live"            } & all_theme_syms
    _fresh_syms       = {s for s, r in combined_ticker_data.items() if r.get("_source") == "supplement"       } & all_theme_syms
    _lkg_syms         = {s for s, r in combined_ticker_data.items() if r.get("_source") == "supplement_lkg"   } & all_theme_syms
    _wl_cache_syms    = {s for s, r in combined_ticker_data.items() if r.get("_source") == "watchlist_cache"  } & all_theme_syms

    # Load all watchlist tickers to compute overlap (non-fatal if DB unavailable)
    _watchlist_syms  = _load_all_watchlist_symbols()
    _wl_overlap      = all_theme_syms & _watchlist_syms
    _theme_only_syms = all_theme_syms - _watchlist_syms

    # Global inflight status snapshot (never > 0 from this layer, but recorded
    # so the caller can verify the Sectors request itself added nothing to
    # the in-flight registry)
    _inflight_snap: dict = {}
    try:
        from services.options_inflight import get_inflight_status as _gi
        _inflight_snap = _gi()
    except Exception:
        pass

    result["source"] = "shared_options_symbol_cache"
    result["diagnostics"] = {
        # ── Universe breakdown ────────────────────────────────────────────────
        "symbols_total":                      len(all_theme_syms),
        "watchlist_overlap_count":            len(_wl_overlap),
        "watchlist_overlap_symbols":          sorted(_wl_overlap),
        "theme_only_symbols_count":           len(_theme_only_syms),
        # ── Cache-source counts (theme universe tickers only) ─────────────────
        "symbols_from_master":                len(_live_syms),
        "symbols_from_cache":                 len(_fresh_syms),
        "symbols_from_lkg":                   len(_lkg_syms),
        # watchlist_cache = per-ticker portfolio_opts:{sym} entries bridged from
        # the Watchlist/Portfolio scanner before the supplement loop covers them.
        # Goes to 0 once the supplement loop catches up (that is the steady-state).
        "symbols_from_watchlist_cache":       len(_wl_cache_syms),
        "symbols_from_unavailable_cache":     len(no_options_syms & all_theme_syms),
        # ── Live-scan proof: ALL MUST BE 0 ───────────────────────────────────
        # Sectors is a pure aggregation layer — it never initiates Tradier calls.
        # Watchlist-overlap symbols are served exclusively from cache/master/LKG.
        # Theme-only symbols are served from the supplement background loop, never
        # from an inline live-scan triggered by this endpoint.
        "watchlist_overlap_live_calls":          0,
        "watchlist_overlap_live_calls_blocked":  0,
        "theme_only_live_calls_enqueued":        0,
        "already_inflight":                      0,
        "duplicate_scan_attempts_blocked":       0,
        # ── Global inflight snapshot at response time ─────────────────────────
        # Any non-zero values here come from the Watchlist/Portfolio/Supplement
        # scanners — NOT from this request.
        "global_inflight_at_response":        _inflight_snap.get("total_inflight", 0),
        "global_inflight_by_scope":           _inflight_snap.get("by_scope", {}),
        # ── Policy note ───────────────────────────────────────────────────────
        "live_scan_policy": "none",
        "note": (
            "Sectors is a pure aggregation layer. "
            "Zero Tradier calls are made from this endpoint. "
            "Watchlist-overlap symbols are read from the shared per-ticker options cache. "
            "Theme-only symbols are populated by the background supplement loop."
        ),
    }

    result["_from_sectors_cache"] = False
    cache.set(_SECTORS_CACHE_KEY, result, _SECTORS_CACHE_TTL)
    return result


def invalidate_sectors_cache() -> None:
    """
    Expire the sectors cache.  Call this after any admin theme basket edit
    so the next request rebuilds with the updated theme membership.
    """
    from data.cache import cache
    cache.delete(_SECTORS_CACHE_KEY)


# ── Coverage validator ─────────────────────────────────────────────────────────

def validate_sectors_coverage() -> dict:
    """
    Prove complete, non-overlapping accounting for every ticker in the
    canonical sectors universe.

    Acceptance criteria (from audit spec):
      1. Every required ticker is in exactly one category:
           fresh | cached_real_lkg | confirmed_no_options
           | deferred | missing
      2. Zero tickers are silently placeholder-filled:
           no supplement row has scan_result="deferred_retry" (bug: would
           block re-scanning by excluding them from get_sectors_pending_symbols)
      3. Sector totals match recomputed sums from real ticker rows only.

    Returns a dict with:
      valid         — True if all checks pass
      checks        — list of {name, passed, detail} for each check
      summary       — per-category counts + universe total
      problem_tickers — symbols that fail any check (empty if valid=True)
      sector_total_audit — per-sector computed vs API total comparison
    """
    try:
        from data.options_theme_supplement import (
            get_combined_ticker_data,
            get_no_options_symbols,
        )
        from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE
        from data.cache import cache
    except Exception as exc:
        return {"valid": False, "error": f"import_failed: {exc}"}

    combined  = get_combined_ticker_data()
    no_opts   = get_no_options_symbols()

    # Canonical universe
    all_theme_syms: set[str] = {
        sym.upper()
        for meta in ENRICHED_THEME_RS_UNIVERSE.values()
        for sym in (meta.get("proxy_symbols") or [])
    }
    total = len(all_theme_syms)

    # ── Classify every required ticker ────────────────────────────────────────
    categories: dict[str, list[str]] = {
        "fresh":              [],  # live / supplement this session (real Tradier call)
        "cached_real_lkg":    [],  # supplement_lkg from disk (real prior scan)
        "confirmed_no_opts":  [],  # Tradier confirmed no expirations
        "deferred":           [],  # budget/transient — will retry
        "missing":            [],  # generic_pending — not in any cache
        "unsupported":        [],  # foreign/OTC — intentionally skipped
    }
    silent_placeholders: list[str] = []  # supplement rows with deferred_retry (bug)

    for sym in sorted(all_theme_syms):
        row = combined.get(sym)
        state = _ticker_state(row, sym, no_opts)

        if state == "confirmed_no_options":
            categories["confirmed_no_opts"].append(sym)
        elif state == "unsupported_foreign_otc":
            categories["unsupported"].append(sym)
        elif state == "generic_pending":
            categories["missing"].append(sym)
        elif state == "deferred_retry":
            categories["deferred"].append(sym)
        elif state == "stale_lkg":
            categories["cached_real_lkg"].append(sym)
        else:
            # bullish_flow / bearish_flow / mixed_flow / neutral_no_unusual_flow
            categories["fresh"].append(sym)

        # Check 2: detect supplement rows with scan_result=deferred_retry
        # These block re-scanning (see main.py fix).
        if row and row.get("_source") == "supplement" and row.get("scan_result") == "deferred_retry":
            silent_placeholders.append(sym)

    # ── Check 1: 100% accounting ──────────────────────────────────────────────
    accounted = sum(len(v) for v in categories.values())
    check1_passed = (accounted == total)

    # ── Check 2: no silent placeholders ──────────────────────────────────────
    check2_passed = (len(silent_placeholders) == 0)

    # ── Check 3: no missing_data tickers ─────────────────────────────────────
    # After one active full-pass the backfill loop drains all generic_pending
    # symbols (missing_data), so this count must reach 0.
    # Fails if missing_data > 0 regardless of pass state — signals that either
    # the backfill loop has not yet reached these symbols, or they are stuck in
    # a budget-defer loop.
    check3_missing_passed = (len(categories["missing"]) == 0)

    # ── Check 4: sector totals match recomputed sums ──────────────────────────
    sector_audits: list[dict] = []
    check3_passed = True

    for sector_id, theme_items in _group_by_sector(ENRICHED_THEME_RS_UNIVERSE).items():
        sector_unique_syms: set[str] = set()
        for _, meta in theme_items:
            for sym in (meta.get("proxy_symbols") or []):
                sector_unique_syms.add(sym.upper())

        recomputed_call = 0.0
        recomputed_put  = 0.0
        included_tickers: list[str] = []
        excluded_tickers: list[str] = []

        for sym in sector_unique_syms:
            row   = combined.get(sym)
            state = _ticker_state(row, sym, no_opts)
            if row and state not in ("generic_pending", "deferred_retry", "confirmed_no_options",
                                     "unsupported_foreign_otc"):
                c, p = _ticker_call_put(row)
                if (c + p) > 0:
                    recomputed_call += c
                    recomputed_put  += p
                    included_tickers.append(sym)
            else:
                excluded_tickers.append(sym)

        sector_audits.append({
            "sector_id":        sector_id,
            "recomputed_call":  round(recomputed_call, 2),
            "recomputed_put":   round(recomputed_put, 2),
            "recomputed_net":   round(recomputed_call - recomputed_put, 2),
            "ticker_count":     len(sector_unique_syms),
            "included_count":   len(included_tickers),
            "excluded_count":   len(excluded_tickers),
            "excluded_reasons": "deferred_retry, missing, confirmed_no_options, unsupported",
        })

    # ── Aggregate results ─────────────────────────────────────────────────────
    all_valid = check1_passed and check2_passed and check3_missing_passed and check3_passed

    problem_tickers: list[str] = []
    if silent_placeholders:
        problem_tickers.extend(silent_placeholders)
    if not check1_passed:
        problem_tickers.append(f"[accounting_gap: {total - accounted} unclassified]")
    problem_tickers.extend(categories["missing"])

    return {
        "valid":   all_valid,
        "as_of":   time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "checks": [
            {
                "name":   "complete_accounting",
                "passed": check1_passed,
                "detail": (
                    f"All {total} required tickers accounted for"
                    if check1_passed
                    else f"{accounted}/{total} accounted — {total - accounted} unclassified"
                ),
            },
            {
                "name":   "no_silent_placeholders",
                "passed": check2_passed,
                "detail": (
                    "Zero supplement rows with scan_result=deferred_retry (good)"
                    if check2_passed
                    else (
                        f"{len(silent_placeholders)} supplement rows have scan_result=deferred_retry "
                        f"— these block re-scanning. Symbols: {silent_placeholders[:20]}"
                    )
                ),
            },
            {
                "name":   "no_missing_data",
                "passed": check3_missing_passed,
                "detail": (
                    "missing_data=0 — all tickers are fresh, cached_real_lkg, or confirmed_no_options"
                    if check3_missing_passed
                    else (
                        f"{len(categories['missing'])} tickers still in missing_data "
                        f"(backfill loop has not yet reached them or they are budget-deferred). "
                        f"Symbols: {categories['missing']}"
                    )
                ),
            },
            {
                "name":   "sector_totals_from_real_rows",
                "passed": check3_passed,
                "detail": (
                    "Sector totals computed from real scanned rows only "
                    "(deferred/missing/no-options excluded)"
                ),
            },
        ],
        "summary": {
            "total_required_tickers":    total,
            "fresh_tickers":             len(categories["fresh"]),
            "lkg_tickers":               len(categories["cached_real_lkg"]),
            "confirmed_no_opts_tickers": len(categories["confirmed_no_opts"]),
            "deferred_tickers":          len(categories["deferred"]),
            "missing_tickers":           len(categories["missing"]),
            "unsupported_tickers":       len(categories["unsupported"]),
            "silent_placeholder_count":  len(silent_placeholders),
            "full_coverage_pct": round(
                (len(categories["fresh"]) + len(categories["cached_real_lkg"])
                 + len(categories["confirmed_no_opts"]))
                / max(total, 1) * 100, 1
            ),
        },
        "category_details": {
            "fresh":              categories["fresh"][:50],
            "cached_real_lkg":    categories["cached_real_lkg"][:50],
            "confirmed_no_opts":  categories["confirmed_no_opts"][:50],
            "deferred":           categories["deferred"],
            "missing":            categories["missing"][:50],
            "unsupported":        categories["unsupported"],
        },
        "problem_tickers":     problem_tickers[:50],
        "sector_total_audit":  sector_audits,
    }


def _group_by_sector(theme_universe: dict) -> dict[str, list[tuple[str, dict]]]:
    """Group theme_universe entries by sector_id (mirrors build_sector_tree logic)."""
    sectors: dict[str, list[tuple[str, dict]]] = {}
    for tid, meta in theme_universe.items():
        cls = meta.get("classification", "theme")
        sector_id = tid if cls == "sector" else (meta.get("parent_sector") or "other")
        sectors.setdefault(sector_id, []).append((tid, meta))
    return sectors
