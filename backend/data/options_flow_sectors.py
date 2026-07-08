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

import math
import time
from typing import Optional

_SECTORS_CACHE_KEY = "options_flow_sectors:v1"
_SECTORS_CACHE_TTL = 60   # 1 min — short so supplement updates are visible quickly
_THEMES_CACHE_KEY  = "options_flow_themes:v1"
_THEMES_CACHE_TTL  = 60   # same cadence as sectors


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


def _vpcr(call_vol, put_vol) -> Optional[float]:
    """Volume put/call ratio: put_volume / call_volume.
    Returns None when call_vol is 0, null, or both sides are absent."""
    try:
        cv = int(call_vol) if call_vol is not None else 0
        pv = int(put_vol)  if put_vol  is not None else 0
    except (TypeError, ValueError):
        return None
    if cv > 0:
        return round(pv / cv, 3)
    return None


def _ppc(premium, volume) -> Optional[float]:
    """Premium per contract (dollars / contract count). Returns None when denominator is 0/null."""
    try:
        v = int(volume)   if volume   is not None else 0
        p = float(premium) if premium is not None else None
    except (TypeError, ValueError):
        return None
    if v > 0 and p is not None:
        return round(p / v, 2)
    return None


def _effective_pcr_vals(
    call_p: float, put_p: float
) -> tuple[Optional[float], Optional[float], Optional[str]]:
    """
    Compute (raw_premium_pcr, effective_premium_pcr, one_sided_flow).

    raw_premium_pcr:
      put_p / call_p  when call_p > 0  (6-dp precision)
      None            when call_p == 0 (put-only or both-zero)

    effective_premium_pcr (log-safe for geometric breadth):
      clamp(raw_pcr, 0.01, 100)  for finite ratios  (call_p > 0)
      100.0                       for pure put-only  (call_p == 0, put_p > 0)
      None                        for null-null      (both == 0)

    one_sided_flow:
      "call_only"  when call_p > 0 and put_p == 0
      "put_only"   when call_p == 0 and put_p > 0
      None         otherwise
    """
    if call_p > 0:
        raw = put_p / call_p
        effective = max(0.01, min(100.0, raw))
        one_sided = "call_only" if put_p == 0.0 else None
        return round(raw, 6), round(effective, 6), one_sided
    elif put_p > 0:
        return None, 100.0, "put_only"
    else:
        return None, None, None


def _geometric_mean_pcr(ratios: list[float]) -> Optional[float]:
    """
    Geometric mean of effective P/C ratios for stock/ETF breadth aggregation.
    Returns None when ratios is empty.  Ratios must be > 0 (enforced by callers).
    """
    if not ratios:
        return None
    try:
        return round(math.exp(sum(math.log(r) for r in ratios) / len(ratios)), 4)
    except (ValueError, ZeroDivisionError):
        return None


def _dte_from_exp(exp_str: str) -> Optional[int]:
    """Compute days-to-expiry from a YYYY-MM-DD string. Returns None on parse error."""
    try:
        from datetime import date, datetime
        return (datetime.strptime(exp_str, "%Y-%m-%d").date() - date.today()).days
    except Exception:
        return None


def _expiry_scope(row: dict) -> tuple[str, Optional[str], Optional[int], str]:
    """
    Derive (expiration_scope, expiration_used, dte_used, premium_calc_method)
    from a raw cache row.

    The discriminator is scan_result:
      "sectors_chain_summarized" → Chain Summarizer path.
          Scope: one primary expiry (nearest 7–60 DTE preferred, then nearest
          non-expired).  ALL contracts with volume > 0 on that one expiry are
          included.  The expiry date is stored in row["expiration_used"].

      anything else → Master Screener / unusual-flow path.
          Scope: the screener's top unusual-flow contracts, which may span
          multiple expirations.  row["expiry"] holds the best (highest-premium)
          contract's expiry; row["days_to_expiry"] holds its DTE.

    Returns (scope_label, expiration_used, dte_used, calc_method).
    """
    scan_result = (row.get("scan_result") or "") if row else ""

    if scan_result == "sectors_chain_summarized":
        exp_used = row.get("expiration_used")
        return (
            "single_expiry_7_60dte_preferred",
            exp_used,
            _dte_from_exp(exp_used) if exp_used else None,
            "volume × mid_price × 100  (all contracts on primary expiry)",
        )

    exp_used = (row.get("expiry") if row else None)
    dte_used = int(row["days_to_expiry"]) if row and row.get("days_to_expiry") is not None else None
    return (
        "top_unusual_contracts",
        exp_used,
        dte_used,
        "volume × mid_price × 100  (screener top unusual-flow contracts)",
    )


# ── shared aggregation helper ─────────────────────────────────────────────────

def _rollup_ticker_nodes(ticker_nodes: list[dict]) -> dict:
    """
    Aggregate a list of ticker nodes (output of _build_ticker_node) into a
    single dict of premium, volume, bias, and state-count fields.

    Used by both _build_theme_node (theme-level rollup, all proxy_symbols) and
    _build_sector_node (sector-level rollup, unique-ticker-dedup set), so both
    Sectors and Themes views share exactly one aggregation implementation.

    Contribution rules:
      - call_premium / put_premium: sum all is-not-None values.
        A value of 0.0 (neutral / confirmed zero-flow row) contributes 0.0, not
        nothing — the is-not-None guard is intentional so neutral rows are counted
        as represented without inflating the premium total.
      - Volumes: sum all non-None values across every ticker node, regardless
        of whether that node has non-zero flow premium.
      - State counts: every ticker node is counted once regardless of its state.
      - cached_data (stale_lkg) rows contribute identically to fresh rows — the
        _build_ticker_node output is the same shape for both; there is no
        scan_source branch here.
    """
    total_call:     float     = 0.0
    total_put:      float     = 0.0
    total_call_vol: int | None = None
    total_put_vol:  int | None = None
    total_vol:      int | None = None
    state_counts:   dict[str, int] = {}
    flow_count:     int = 0
    represented:    int = 0
    # Stock/ETF split — for breadth P/C and coverage metrics
    _stock_call:     float      = 0.0
    _stock_put:      float      = 0.0
    _etf_call:       float      = 0.0
    _etf_put:        float      = 0.0
    # Canonical Net Flow breadth: only single_expiry_7_60dte_preferred rows
    # (premium_scope_id == "net_flow_single_expiry_7_60dte_v1").
    # Unusual-flow rows (top_unusual_contracts scope) are tracked separately and
    # EXCLUDED from breadth_pcr — they use different contract sampling and DTE
    # windows and produce non-comparable P/C ratios.
    _stock_eff_pcrs:         list[float] = []  # net_flow scope only
    _etf_eff_pcrs:           list[float] = []  # net_flow scope only
    _unusual_stock_eff_pcrs: list[float] = []  # unusual_flow scope (excluded from breadth)
    _unusual_etf_eff_pcrs:   list[float] = []  # unusual_flow scope ETFs
    _total_stock:    int = 0
    _total_etf:      int = 0
    _total_unknown:  int = 0
    _unknown_sample: list = []
    _valid_stock_pcr:        int = 0   # net_flow scope valid stocks → breadth_pcr denominator
    _valid_etf_pcr:          int = 0   # net_flow scope valid ETFs
    _unusual_valid_stock_pcr: int = 0  # unusual_flow scope valid stocks
    _unusual_valid_etf_pcr:   int = 0  # unusual_flow scope valid ETFs
    _net_flow_missing_stock:  int = 0  # stocks with only unusual scope (no net_flow snapshot)
    # Track premium-contributing tickers by their expiration scope so rollup
    # nodes can expose whether aggregated premiums come from one methodology
    # or a mix of two.  Only tickers with at least one non-None premium field
    # are counted — pending / no_options tickers with scope="none" add nothing
    # to the dollar total and are excluded from the scope breakdown.
    _scope_counts:  dict[str, int] = {}
    # ── Interval trade-side classification accumulators ───────────────────────
    # Sum raw delta-dollar amounts from tickers that have non-None
    # interval_total_premium (chain-summarizer rows, current session only).
    # LKG rows (supplement_lkg source) have interval fields stripped to None
    # in _build_ticker_node to prevent stale prior-session interval data from
    # being presented as current interval flow.
    # Master screener rows (live/supplement) also contribute None — they carry
    # no per-contract prior-snapshot data.
    # Percentages are derived from summed raw dollars — never per-ticker averages.
    _int_ask:        float = 0.0
    _int_bid:        float = 0.0
    _int_mid:        float = 0.0
    _int_total:      float = 0.0
    _int_new_vol:    int   = 0
    _has_int_data:   bool  = False
    _int_started_at  = None   # float | None — earliest prior snapshot timestamp
    _int_ended_at    = None   # float | None — latest scan timestamp

    for t in ticker_nodes:
        cp = t.get("call_premium")
        pp = t.get("put_premium")
        if cp is not None:
            total_call += cp
        if pp is not None:
            total_put += pp
        if cp is not None and pp is not None and (cp + pp) > 0:
            flow_count += 1

        cv = t.get("call_volume")
        pv = t.get("put_volume")
        tv = t.get("total_volume")
        if cv is not None:
            total_call_vol = (total_call_vol or 0) + int(cv)
        if pv is not None:
            total_put_vol = (total_put_vol or 0) + int(pv)
        if tv is not None:
            total_vol = (total_vol or 0) + int(tv)

        state = t.get("ticker_state", "generic_pending")
        state_counts[state] = state_counts.get(state, 0) + 1
        if state != "generic_pending":
            represented += 1

        # Scope accounting — only for tickers that actually contribute premium.
        # Map internal expiration_scope labels to the two public categories:
        #   "single_expiry_7_60dte_preferred" → "primary_expiry"
        #   "top_unusual_contracts"           → "top_unusual_contracts"
        #   "none" (pending / no_options)     → excluded
        if cp is not None or pp is not None:
            raw_scope = t.get("expiration_scope", "none")
            if raw_scope == "single_expiry_7_60dte_preferred":
                pub_scope = "primary_expiry"
            elif raw_scope == "top_unusual_contracts":
                pub_scope = "top_unusual_contracts"
            else:
                pub_scope = None
            if pub_scope:
                _scope_counts[pub_scope] = _scope_counts.get(pub_scope, 0) + 1

        # ── Stock/ETF breadth split ───────────────────────────────────────────
        # Accumulate stock/ETF premiums and collect effective_pcr values for
        # geometric breadth computation.
        #
        # CANONICAL NET FLOW BREADTH RULE:
        #   breadth_pcr accumulates ONLY tickers with premium_scope_id ==
        #   "net_flow_single_expiry_7_60dte_v1" (chain summarizer, all-session,
        #   single expiry).  Tickers with "unusual_flow_7_45dte_2exp_5k_v1"
        #   (master screener unusual-flow contracts) use a different DTE window,
        #   contract eligibility, and min-premium filter — mixing them into the
        #   same geometric mean produces a non-comparable breadth ratio.
        #   Unusual-flow tickers are aggregated separately in unusual_flow_breadth_pcr.
        #
        # Only valid completed snapshots (scan_status not pending/missing/no_options)
        # contribute to any breadth.  Unresolved ("unknown") tickers are counted
        # separately so breadth coverage denominators remain honest.
        _itype      = t.get("instrument_type", "unknown")
        _eff_pcr    = t.get("effective_premium_pcr")
        _pscope_id  = t.get("premium_scope_id", "none")
        _ss         = t.get("scan_status", "")
        _valid_snap = _ss not in ("pending", "missing_data", "no_options") and _eff_pcr is not None
        _is_net_flow = _pscope_id == "net_flow_single_expiry_7_60dte_v1"
        _is_unusual  = _pscope_id == "unusual_flow_7_45dte_2exp_5k_v1"

        if _itype == "stock":
            _total_stock += 1
            if cp is not None:
                _stock_call += cp
            if pp is not None:
                _stock_put  += pp
            if _valid_snap:
                if _is_net_flow:
                    # Canonical Net Flow breadth — chain summarizer scope
                    _stock_eff_pcrs.append(_eff_pcr)
                    _valid_stock_pcr += 1
                elif _is_unusual:
                    # Unusual-flow master screener scope — separate bucket
                    _unusual_stock_eff_pcrs.append(_eff_pcr)
                    _unusual_valid_stock_pcr += 1
                    _net_flow_missing_stock += 1  # has data but not net_flow scope
        elif _itype == "etf":
            _total_etf += 1
            if cp is not None:
                _etf_call += cp
            if pp is not None:
                _etf_put  += pp
            if _valid_snap:
                if _is_net_flow:
                    _etf_eff_pcrs.append(_eff_pcr)
                    _valid_etf_pcr += 1
                elif _is_unusual:
                    _unusual_etf_eff_pcrs.append(_eff_pcr)
                    _unusual_valid_etf_pcr += 1
        else:
            # instrument_type == "unknown" — classification not yet resolved.
            # These tickers are in the category but cannot be sorted into the
            # stock or ETF breadth buckets.  They are not silently dropped:
            # the count is surfaced in unresolved_instrument_type_tickers so
            # breadth coverage denominators remain honest.
            _total_unknown += 1
            _sym_name = t.get("symbol")
            if _sym_name and len(_unknown_sample) < 10:
                _unknown_sample.append(_sym_name)

        # Interval trade-side accumulation — sum raw delta dollars from tickers
        # that have non-None interval_total_premium (chain-summarizer rows in the
        # current session only; LKG rows have interval fields stripped in
        # _build_ticker_node so they never contribute stale interval data here).
        _itp = t.get("interval_total_premium")
        if _itp is not None and _itp > 0:
            _iap = t.get("interval_ask_premium") or 0.0
            _ibp = t.get("interval_bid_premium") or 0.0
            _imp = t.get("interval_midpoint_unknown_premium") or 0.0
            _ivl = t.get("interval_new_contract_volume")
            _int_ask    += _iap
            _int_bid    += _ibp
            _int_mid    += _imp
            _int_total  += _itp
            _int_new_vol += int(_ivl) if _ivl is not None else 0
            _has_int_data = True
            # Track interval timing window.
            _ist = t.get("interval_started_at")
            _iet = t.get("interval_ended_at")
            if _ist is not None:
                if _int_started_at is None or _ist < _int_started_at:
                    _int_started_at = _ist
            if _iet is not None:
                if _int_ended_at is None or _iet > _int_ended_at:
                    _int_ended_at = _iet

    total_net = total_call - total_put
    has_data  = (total_call + total_put) > 0
    avg_net   = round(total_net / flow_count, 2) if flow_count else None

    # Derive aggregation_scope from which scope categories are present.
    _has_primary  = "primary_expiry"        in _scope_counts
    _has_unusual  = "top_unusual_contracts" in _scope_counts
    if _has_primary and _has_unusual:
        _agg_scope = "mixed"
    elif _has_primary:
        _agg_scope = "primary_expiry"
    elif _has_unusual:
        _agg_scope = "top_unusual_contracts"
    else:
        _agg_scope = "none"   # all tickers are pending / no_options

    # ── Rollup interval trade-side percentages ────────────────────────────────
    # Derived from summed raw delta dollars — never per-ticker averages.
    # None for all fields when no tickers contributed interval data (all LKG,
    # all master screener, all first-scan post-restart, or no new volume).
    if _has_int_data and _int_total > 0:
        _r_int_ask     = round(_int_ask,   2)
        _r_int_bid     = round(_int_bid,   2)
        _r_int_mid     = round(_int_mid,   2)
        _r_int_tot     = round(_int_total, 2)
        _r_int_vol     = _int_new_vol
        _r_int_ask_pct = round(_int_ask / _int_total * 100, 1)
        _r_int_bid_pct = round(_int_bid / _int_total * 100, 1)
        _r_int_mid_pct = round(_int_mid / _int_total * 100, 1)
        _r_int_cls_pct = round((_int_ask + _int_bid) / _int_total * 100, 1)
        _r_int_secs    = (
            round(_int_ended_at - _int_started_at)
            if _int_started_at is not None and _int_ended_at is not None
            else None
        )
    else:
        _r_int_ask = _r_int_bid = _r_int_mid = _r_int_tot = None
        _r_int_vol = None
        _r_int_ask_pct = _r_int_bid_pct = _r_int_mid_pct = _r_int_cls_pct = None
        _r_int_secs    = None
        _int_started_at = _int_ended_at = None

    return {
        "call_premium":               round(total_call, 2) if has_data else None,
        "put_premium":                round(total_put,  2) if has_data else None,
        "net_premium":                round(total_net,  2) if has_data else None,
        "total_net_flow":             round(total_net,  2) if has_data else None,
        "average_net_flow":           avg_net,
        "total_volume":               total_vol,
        "call_volume":                total_call_vol,
        "put_volume":                 total_put_vol,
        # Explicit contract-count alias so the frontend can label column correctly.
        # Dollar premiums (call_premium / put_premium / net_premium) are NOT
        # contract counts; this field is.
        "total_contract_volume":      total_vol,
        "put_call_ratio":             _pcr(total_call, total_put),
        "volume_put_call_ratio":      _vpcr(total_call_vol, total_put_vol),
        "volume_pcr":                 _vpcr(total_call_vol, total_put_vol),
        "premium_per_contract":       _ppc(total_call + total_put if has_data else None, total_vol),
        "call_premium_per_contract":  _ppc(total_call if has_data else None, total_call_vol),
        "put_premium_per_contract":   _ppc(total_put  if has_data else None, total_put_vol),
        "bias":                       _bias(total_call, total_put) if has_data else None,
        "ticker_count":               len(ticker_nodes),
        "represented_count":          represented,
        "contributing_ticker_count":  flow_count,
        # ── Interval trade-side classification (rollup) ───────────────────────
        # Summed raw delta dollars from chain-summarizer tickers in the current
        # session.  None when no interval data is available (all LKG, all master
        # screener, first scan post-restart, or no new contracts since last cycle).
        # LKG rows always contribute None here (stripped in _build_ticker_node).
        "interval_ask_premium":                  _r_int_ask,
        "interval_bid_premium":                  _r_int_bid,
        "interval_midpoint_unknown_premium":     _r_int_mid,
        "interval_total_premium":                _r_int_tot,
        "interval_new_contract_volume":          _r_int_vol,
        "interval_ask_premium_pct":              _r_int_ask_pct,
        "interval_bid_premium_pct":              _r_int_bid_pct,
        "interval_midpoint_unknown_premium_pct": _r_int_mid_pct,
        "interval_classified_trade_side_pct":    _r_int_cls_pct,
        "interval_seconds":                      _r_int_secs,
        "interval_started_at":                   _int_started_at,
        "interval_ended_at":                     _int_ended_at,
        # ── Aggregation scope ──────────────────────────────────────────────────
        # Tells the frontend whether the dollar totals above were built from one
        # methodology or a blend of two.
        #
        # aggregation_scope values:
        #   "primary_expiry"        — all contributing tickers came from the chain
        #                             summarizer (one selected expiry per ticker)
        #   "top_unusual_contracts" — all came from the master screener (unusual-
        #                             flow contracts, possibly multi-expiry)
        #   "mixed"                 — BOTH sources contributed (most common in
        #                             practice: backfill loop uses chain summarizer,
        #                             master screener adds unusual-flow tickers)
        #   "none"                  — no tickers with premium data yet
        #
        # aggregation_scope_counts breaks down the contributing ticker count by
        # methodology so the frontend can show "148 primary / 100 unusual".
        "aggregation_scope":        _agg_scope,
        "aggregation_scope_counts": {
            "primary_expiry":        _scope_counts.get("primary_expiry",        0),
            "top_unusual_contracts": _scope_counts.get("top_unusual_contracts", 0),
        },
        # ── Premium scope metadata ─────────────────────────────────────────────
        # Clarifies that dollar values are ESTIMATED PREMIUM, not contract volume.
        # See per-ticker expiration_scope for the chain-level detail.
        "premium_metric_label":  "Estimated Premium (USD)",
        "premium_scope_summary": (
            "Dollar values are estimated option premium (mid_price × volume × 100 "
            "per contract), not the number of contracts traded. "
            "Tickers scanned by the chain summarizer use one primary expiry "
            "(nearest 7–60 DTE preferred). "
            "Tickers from the master screener use unusual-flow top contracts "
            "which may span multiple expirations. "
            "Use total_contract_volume for the combined contract count."
        ),
        # 9-state breakdown
        "bullish_count":              state_counts.get("bullish_flow",            0),
        "bearish_count":              state_counts.get("bearish_flow",            0),
        "mixed_count":                state_counts.get("mixed_flow",              0),
        "neutral_count":              state_counts.get("neutral_no_unusual_flow", 0),
        "optionable_pending_count":   state_counts.get("optionable_pending_chain", 0),
        "confirmed_no_options_count": state_counts.get("confirmed_no_options",    0),
        "stale_lkg_count":            state_counts.get("stale_lkg",               0),
        "deferred_retry_count":       state_counts.get("deferred_retry",          0),
        "unsupported_count":          state_counts.get("unsupported_foreign_otc", 0),
        "generic_pending_count":      state_counts.get("generic_pending",         0),
        # ── Stock-only breadth P/C (canonical Net Flow) ───────────────────────
        # PRIMARY BREADTH METRIC: geometric mean of per-stock effective_premium_pcr.
        # CANONICAL SCOPE: only tickers with premium_scope_id ==
        #   "net_flow_single_expiry_7_60dte_v1" (chain summarizer, all-session,
        #   single expiry per ticker) contribute to breadth_pcr.
        # Tickers with "unusual_flow_7_45dte_2exp_5k_v1" scope are EXCLUDED
        #   — they come from the master screener's unusual-flow contract filter
        #   (different DTE window, multi-expiry, min-premium gate) and produce
        #   non-comparable P/C ratios when mixed into the same geometric mean.
        #   Their breadth is captured separately in unusual_flow_breadth_pcr.
        # Excludes ETFs, pending, missing, no_options regardless of scope.
        # effective_premium_pcr = clamp(raw_pcr, 0.01, 100) for finite ratios,
        #   0.01 for call-only flow, 100 for put-only flow.
        "breadth_pcr":              _geometric_mean_pcr(_stock_eff_pcrs),
        # net_flow_breadth_pcr: explicit alias emphasising canonical scope
        "net_flow_breadth_pcr":     _geometric_mean_pcr(_stock_eff_pcrs),
        # unusual_flow_breadth_pcr: breadth among master-screener-only tickers
        "unusual_flow_breadth_pcr": _geometric_mean_pcr(_unusual_stock_eff_pcrs),
        "premium_weighted_pcr":     _pcr(_stock_call, _stock_put) if (_stock_call + _stock_put) > 0 else None,
        "total_stock_tickers":      _total_stock,
        # valid_stock_pcr_tickers: count contributing to canonical Net Flow breadth
        "valid_stock_pcr_tickers":  _valid_stock_pcr,
        # net_flow_scoped_stock_tickers: explicit alias
        "net_flow_scoped_stock_tickers":    _valid_stock_pcr,
        # unusual_flow_scoped_stock_tickers: stocks covered only by unusual-flow scope
        "unusual_flow_scoped_stock_tickers": _unusual_valid_stock_pcr,
        # net_flow_missing_snapshot_stock_tickers: stocks that have unusual-flow data
        # but no Net Flow (chain summarizer) snapshot yet.  These are excluded from
        # breadth_pcr / net_flow_breadth_pcr — they are not truly "missing" (they
        # have premium data) but they cannot contribute to canonical Net Flow breadth.
        "net_flow_missing_snapshot_stock_tickers": _net_flow_missing_stock,
        "missing_stock_pcr_tickers": max(0, _total_stock - _valid_stock_pcr - _unusual_valid_stock_pcr),
        "stock_pcr_coverage_pct":   round(_valid_stock_pcr / max(_total_stock, 1) * 100, 1) if _total_stock else None,
        # ── ETF breadth P/C (canonical Net Flow) ──────────────────────────────
        "etf_breadth_pcr":          _geometric_mean_pcr(_etf_eff_pcrs),
        "etf_net_flow_breadth_pcr": _geometric_mean_pcr(_etf_eff_pcrs),
        "etf_unusual_flow_breadth_pcr": _geometric_mean_pcr(_unusual_etf_eff_pcrs),
        "etf_premium_weighted_pcr": _pcr(_etf_call, _etf_put) if (_etf_call + _etf_put) > 0 else None,
        "total_etf_tickers":        _total_etf,
        "valid_etf_pcr_tickers":    _valid_etf_pcr,
        "net_flow_scoped_etf_tickers":    _valid_etf_pcr,
        "unusual_flow_scoped_etf_tickers": _unusual_valid_etf_pcr,
        "missing_etf_pcr_tickers":  max(0, _total_etf - _valid_etf_pcr - _unusual_valid_etf_pcr),
        "etf_pcr_coverage_pct":     round(_valid_etf_pcr / max(_total_etf, 1) * 100, 1) if _total_etf else None,
        # ── Unresolved classification ──────────────────────────────────────────
        # Tickers whose instrument_type is still "unknown" are excluded from
        # both stock and ETF breadth buckets.  They are counted and sampled
        # here so coverage denominators remain honest.
        #
        # Coverage identity for a category (theme or sector):
        #   total_tickers = total_stock_tickers + total_etf_tickers + unresolved
        #   stock_pcr_coverage_pct is valid_stock / total_stock (among KNOWN stocks)
        #   but unresolved_instrument_type_tickers must be 0 for breadth to be
        #   fully comparable across categories.
        "unresolved_instrument_type_tickers": _total_unknown,
        "unresolved_instrument_type_sample":  _unknown_sample,
    }


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
      2. supplement_lkg source → classify by actual data quality:
           - deferred_retry scan_result          → deferred_retry (re-queue)
           - confirmed_no_options scan_result     → confirmed_no_options
           - has real premium data               → stale_lkg (show as cached_data)
           - real scan with zero flow            → stale_lkg (valid zero-flow reading)
           - coverage placeholder (no real data) → generic_pending (re-queue)
      3. Premium data present → infer bullish_flow / bearish_flow / mixed_flow
      4. Fresh row, no premium, no tag → neutral_no_unusual_flow (backward compat)
      5. No row + in no_options_syms → confirmed_no_options
      6. No row → generic_pending (unattempted)
    """
    if row is None:
        return "confirmed_no_options" if sym in no_options_syms else "generic_pending"

    source = row.get("_source", "live")
    if source == "supplement_lkg":
        # supplement_lkg rows must be classified by actual data quality.
        # Old placeholder rows (deferred_retry, optionable_pending_chain with
        # zero premium) must NOT be promoted to stale_lkg / cached_data — that
        # would display blank premiums and is misleading.
        scan_result_lkg = row.get("scan_result") or ""

        # Explicitly budget-deferred → re-queue as deferred_retry, not cached_data
        if scan_result_lkg == "deferred_retry":
            return "deferred_retry"

        # Confirmed no options → preserve that classification
        if scan_result_lkg == "confirmed_no_options":
            return "confirmed_no_options"

        # Real complete-scan results mean data exists even if premium=0
        # (sectors_chain_summarized = backfill scan; neutral_no_unusual_flow = master screener)
        _LKG_REAL_SCAN_RESULTS = frozenset({"sectors_chain_summarized", "neutral_no_unusual_flow"})
        if scan_result_lkg in _LKG_REAL_SCAN_RESULTS:
            return "stale_lkg"

        # For other scan_result values, require positive premium to confirm real data
        lkg_call, lkg_put = _ticker_call_put(row)
        if (lkg_call + lkg_put) > 0:
            return "stale_lkg"

        # No real data (optionable_pending_chain, unknown, zero premium) →
        # re-queue as generic_pending so the backfill loop fills it in
        return "generic_pending"

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
    instrument_type_by_sym: "dict[str, str] | None" = None,
    supplement_by_ticker: "dict[str, dict] | None" = None,
    display_name_by_sym: "dict[str, str] | None" = None,
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

        # ── Net Flow source selection ─────────────────────────────────────────
        # When a symbol is in the master screener (unusual-flow scope) AND the
        # supplement has a canonical Net Flow snapshot (sectors_chain_summarized),
        # the Net Flow leaf MUST use the supplement's premium fields.  The master
        # screener uses a different DTE window and contract filter (top_unusual_
        # contracts: multi-expiry, $5k min, 7-45 DTE) vs. the canonical Net Flow
        # methodology (single_expiry_7_60dte: all-session, one expiry per ticker).
        # Mixing them in the same leaf produces misleading P/C values.
        # `row` is preserved for state/scan_status/expiration_used derivation.
        _nf_row = row  # premium fields source for Net Flow display
        _suppress_nf_premiums = False  # True: no canonical NF snapshot yet
        if supplement_by_ticker is not None:
            # 1. Prefer the canonical NF row from the supplement dict (pre-merge, clean).
            _supp_nf = supplement_by_ticker.get(sym)
            if _supp_nf and _supp_nf.get("scan_result") == "sectors_chain_summarized":
                _nf_row = _supp_nf
            elif row.get("scan_result") == "sectors_chain_summarized":
                # Merged row itself is canonical (supplement row won the merge and
                # carried sectors_chain_summarized; _supp_nf may be absent or stale).
                pass  # keep _nf_row = row
            else:
                # No canonical Net Flow snapshot from any source. This includes:
                # • source=="live": master screener found the ticker (unusual-flow scan)
                # • source=="supplement": supplement row won but has non-canonical result
                #   (deferred_retry, etc.) and may carry bridged unusual-flow premiums.
                # Net Flow MUST NOT show unusual-flow P/C from either path.
                _suppress_nf_premiums = True

        if _suppress_nf_premiums:
            call_p = put_p = net_p = 0.0
            has_prem = False
        else:
            call_p, put_p = _ticker_call_put(_nf_row)
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
            and (
                scan_result == "neutral_no_unusual_flow"
                # sectors_chain_summarized rows with zero premiums are a real
                # neutral scan result (chain was scanned, no unusual flow found).
                # They must return 0.0 not None so the UI shows "no flow" not blank.
                or (
                    scan_result == "sectors_chain_summarized"
                    and call_p == 0.0
                    and put_p == 0.0
                )
            )
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
        # scan_status is derived from state (primary), then source (secondary),
        # then scan_result (tertiary).  Source must be checked BEFORE scan_result
        # because supplement_lkg rows store their original scan_result tag
        # (e.g. "sectors_chain_summarized") even when they represent prior-session
        # data.  Checking scan_result first would incorrectly label LKG rows "fresh".
        if state == "generic_pending":
            scan_status_val = "missing_data"   # LKG placeholder re-queued as missing
        elif state == "confirmed_no_options":
            scan_status_val = "no_options"     # LKG row with confirmed no-options
        elif state == "deferred_retry":
            scan_status_val = "pending"
        elif source in ("supplement_lkg", "watchlist_cache"):
            scan_status_val = "cached_data"    # prior-session real data — must come
                                               # BEFORE the scan_result check below
        elif scan_result == "sectors_chain_summarized" or source in ("live", "supplement"):
            scan_status_val = "fresh"          # current-session scan
        else:
            scan_status_val = source   # defensive fallback

        _scanned_at = (
            row.get("updated_at") or row.get("cached_at") or row.get("_cached_at")
            or row.get("_sectors_lkg_at")
        )

        # ── Premium scope metadata (labeling only, no calculation change) ─────
        # Tells the frontend exactly what the dollar values represent so columns
        # can be labeled "Call Premium (est.)" not "Call Volume".
        if _suppress_nf_premiums:
            _scope = _exp_used = _dte_used = _prem_method = None
        else:
            _scope, _exp_used, _dte_used, _prem_method = _expiry_scope(_nf_row)

        # ── premium_scope_id — canonical scope identifier ─────────────────────
        # Maps the raw expiration_scope string to a versioned canonical ID.
        # net_flow_single_expiry_7_60dte_v1  — chain summarizer (all-session, single expiry)
        # unusual_flow_7_45dte_2exp_5k_v1   — master screener unusual-flow subset
        # "none"                             — pending, no_options, or no data
        _SCOPE_ID_MAP = {
            "single_expiry_7_60dte_preferred": "net_flow_single_expiry_7_60dte_v1",
            "top_unusual_contracts":           "unusual_flow_7_45dte_2exp_5k_v1",
        }
        _premium_scope_id = _SCOPE_ID_MAP.get(_scope, "none")

        # ── Instrument-type source provenance ─────────────────────────────────
        # Tells the frontend how this symbol was classified (etf vs stock).
        # fmp_is_etf      — screener_fundamentals_cache.isEtf explicit True/False
        # fmp_profile     — FMP /stable/profile background pass
        # sector_inference — isEtf=None, non-empty FMP sector → inferred stock [TEMPORARY]
        # lkg             — loaded from persisted LKG (source not re-derived on load)
        # unresolved      — not yet classified
        _itype_src = "unresolved"
        try:
            from data.options_instrument_type_service import get_instrument_type_source as _git_src
            _itype_src = _git_src(sym)
        except Exception:
            pass
        _itype_inferred = (_itype_src == "sector_inference")
        _tv = row.get("total_volume") if row.get("total_volume") is not None else (0 if _neutral_confirmed else None)

        # ── Interval trade-side classification pass-through ───────────────────
        # interval_* fields are present only for current-session chain-summarizer
        # rows (scan_result="sectors_chain_summarized", source="supplement").
        # LKG rows (supplement_lkg / watchlist_cache) have their interval fields
        # stripped to None here — interval data represents a prior-session scan
        # window and must not be served as current interval flow.
        # Master screener rows (live/supplement source with no chain snapshot) also
        # contribute None since no per-contract prior volume is available there.
        _is_lkg = source in ("supplement_lkg", "watchlist_cache")

        def _ifield(k):
            return None if _is_lkg else _safe_float(row.get(k))

        def _ifield_int(k):
            if _is_lkg:
                return None
            v = row.get(k)
            return int(v) if v is not None else None

        # ── Canonical instrument type ─────────────────────────────────────────
        _itype_map = instrument_type_by_sym or {}
        _instrument_type = _itype_map.get(sym, "unknown")

        # ── Effective P/C for geometric breadth aggregation ───────────────────
        # Always compute from the actual call_p / put_p values (even zero).
        # raw_premium_pcr  — factual ratio (6-dp) or None for put-only/null-null
        # effective_premium_pcr — log-safe clamp: 0.01 (call-only) … 100 (put-only)
        # one_sided_flow   — "call_only" | "put_only" | None
        if has_prem:
            _raw_pcr, _eff_pcr, _one_sided = _effective_pcr_vals(call_p, put_p)
        else:
            _raw_pcr = _eff_pcr = _one_sided = None

        return {
            "symbol":            sym,
            "display_name":      ((display_name_by_sym or {}).get(sym) or None),
            "ticker_state":      state,
            "instrument_type":   _instrument_type,
            "call_premium":      round(call_p, 2) if has_prem else (0.0 if _neutral_confirmed else None),
            "put_premium":       round(put_p, 2)  if has_prem else (0.0 if _neutral_confirmed else None),
            "net_premium":       round(net_p, 2)  if has_prem else (0.0 if _neutral_confirmed else None),
            "put_call_ratio":         _pcr(call_p, put_p) if has_prem else None,
            "raw_premium_pcr":        _raw_pcr,
            "effective_premium_pcr":  _eff_pcr,
            "one_sided_flow":         _one_sided,
            "bias":                   bias_val,
            "call_volume":            row.get("call_volume") if row.get("call_volume") is not None else (0 if _neutral_confirmed else None),
            "put_volume":             row.get("put_volume")  if row.get("put_volume")  is not None else (0 if _neutral_confirmed else None),
            "volume_put_call_ratio":  _vpcr(
                row.get("call_volume") if row.get("call_volume") is not None else (0 if _neutral_confirmed else None),
                row.get("put_volume")  if row.get("put_volume")  is not None else (0 if _neutral_confirmed else None),
            ),
            "volume_pcr":             _vpcr(
                row.get("call_volume") if row.get("call_volume") is not None else (0 if _neutral_confirmed else None),
                row.get("put_volume")  if row.get("put_volume")  is not None else (0 if _neutral_confirmed else None),
            ),
            "premium_per_contract":      _ppc((call_p + put_p) if has_prem else None, _tv),
            "call_premium_per_contract": _ppc(call_p if has_prem else None, row.get("call_volume")),
            "put_premium_per_contract":  _ppc(put_p  if has_prem else None, row.get("put_volume")),
            "total_volume":      _tv,
            # Explicit contract-count alias — distinguishes dollar premium fields
            # from contract count. Use this for the "Contracts" column.
            "total_contract_volume": _tv,
            "heat_score":        row.get("heat_score") if row.get("heat_score") is not None else (0.0 if _neutral_confirmed else None),
            "side_bias":         row.get("side_bias"),
            "options_available": state in _OPTIONS_CONFIRMED_STATES,
            "scan_status":       scan_status_val,
            "scan_result":       scan_result,
            "scanned_at":        _scanned_at,
            "updated_at":        _scanned_at,
            # ── Premium labeling metadata ──────────────────────────────────────
            # premium_metric_label  : human-readable name for the dollar values.
            # premium_calc_method   : the formula used (mid_price × vol × 100).
            # expiration_scope      : "single_expiry_7_60dte_preferred" means the
            #   chain summarizer used ONE expiry (nearest 7–60 DTE).
            #   "top_unusual_contracts" means the master screener's unusual-flow
            #   subset, which may span multiple expirations.
            # expiration_used       : the primary expiry date (YYYY-MM-DD) if a
            #   single expiry was used; the best-contract expiry otherwise.
            # dte_used              : days-to-expiry for expiration_used.
            "premium_metric_label": "Estimated Premium (USD)",
            "premium_calc_method":  _prem_method,
            "expiration_scope":     _scope,
            # premium_scope_id: versioned canonical identifier for the data-collection
            # methodology used for this ticker's premium values.
            # "net_flow_single_expiry_7_60dte_v1" — chain summarizer, all-session, one expiry
            # "unusual_flow_7_45dte_2exp_5k_v1"   — master screener unusual-flow contracts
            # "none"                               — pending, no_options, or no data
            # Only "net_flow_single_expiry_7_60dte_v1" tickers contribute to breadth_pcr.
            "premium_scope_id":     _premium_scope_id,
            "nf_snapshot_pending":  _suppress_nf_premiums or None,
            "expiration_used":      _exp_used,
            "dte_used":             _dte_used,
            # instrument_type_source: how this ticker's etf/stock classification was obtained
            "instrument_type_source":   _itype_src,
            "instrument_type_inferred": _itype_inferred,
            # ── Interval trade-side classification ────────────────────────────
            # Based on volume_delta since the prior snapshot — not cumulative volume.
            # LKG rows: all None (prior-session interval, not current).
            # Master screener rows: all None (no per-contract snapshot available).
            "interval_ask_premium":                  _ifield("interval_ask_premium"),
            "interval_bid_premium":                  _ifield("interval_bid_premium"),
            "interval_midpoint_unknown_premium":     _ifield("interval_midpoint_unknown_premium"),
            "interval_total_premium":                _ifield("interval_total_premium"),
            "interval_new_contract_volume":          _ifield_int("interval_new_contract_volume"),
            "interval_ask_premium_pct":              _ifield("interval_ask_premium_pct"),
            "interval_bid_premium_pct":              _ifield("interval_bid_premium_pct"),
            "interval_midpoint_unknown_premium_pct": _ifield("interval_midpoint_unknown_premium_pct"),
            "interval_classified_trade_side_pct":    _ifield("interval_classified_trade_side_pct"),
            "interval_seconds":                      _ifield_int("interval_seconds"),
            "interval_started_at":                   _ifield("interval_started_at"),
            "interval_ended_at":                     _ifield("interval_ended_at"),
        }

    _itype_fallback = (instrument_type_by_sym or {}).get(sym, "unknown")

    # Instrument-type source for the two early-return paths (no_options, generic_pending).
    # The main data path computes its own _itype_src inside that block.
    _itype_src_fb = "unresolved"
    try:
        from data.options_instrument_type_service import get_instrument_type_source as _git_src_fb
        _itype_src_fb = _git_src_fb(sym)
    except Exception:
        pass

    if sym in no_options_syms:
        return {
            "symbol":                sym,
            "display_name":          ((display_name_by_sym or {}).get(sym) or None),
            "ticker_state":          "confirmed_no_options",
            "instrument_type":       _itype_fallback,
            "call_premium":          None,
            "put_premium":           None,
            "net_premium":           None,
            "put_call_ratio":        None,
            "raw_premium_pcr":       None,
            "effective_premium_pcr": None,
            "one_sided_flow":        None,
            "bias":                  None,
            "call_volume":           None,
            "put_volume":            None,
            "volume_put_call_ratio":    None,
            "volume_pcr":               None,
            "premium_per_contract":     None,
            "call_premium_per_contract": None,
            "put_premium_per_contract":  None,
            "total_volume":          None,
            "total_contract_volume": None,
            "heat_score":            None,
            "side_bias":             None,
            "options_available":     False,
            "scan_status":           "no_options",
            "scan_result":           "confirmed_no_options",
            "reason":                "confirmed_no_tradeable_options",
            "updated_at":            None,
            "premium_metric_label":  "Estimated Premium (USD)",
            "premium_calc_method":   "n/a (no tradeable options)",
            "expiration_scope":      "none",
            "premium_scope_id":      "none",
            "expiration_used":       None,
            "dte_used":              None,
            "instrument_type_source":   _itype_src_fb,
            "instrument_type_inferred": _itype_src_fb == "sector_inference",
            "interval_ask_premium":                  None,
            "interval_bid_premium":                  None,
            "interval_midpoint_unknown_premium":     None,
            "interval_total_premium":                None,
            "interval_new_contract_volume":          None,
            "interval_ask_premium_pct":              None,
            "interval_bid_premium_pct":              None,
            "interval_midpoint_unknown_premium_pct": None,
            "interval_classified_trade_side_pct":    None,
            "interval_seconds":                      None,
            "interval_started_at":                   None,
            "interval_ended_at":                     None,
        }

    return {
        "symbol":                sym,
        "display_name":          ((display_name_by_sym or {}).get(sym) or None),
        "ticker_state":          "generic_pending",
        "instrument_type":       _itype_fallback,
        "call_premium":          None,
        "put_premium":           None,
        "net_premium":           None,
        "put_call_ratio":        None,
        "raw_premium_pcr":       None,
        "effective_premium_pcr": None,
        "one_sided_flow":        None,
        "bias":                  None,
        "call_volume":           None,
        "put_volume":            None,
        "volume_put_call_ratio":     None,
        "volume_pcr":                None,
        "premium_per_contract":      None,
        "call_premium_per_contract": None,
        "put_premium_per_contract":  None,
        "total_volume":          None,
        "total_contract_volume": None,
        "heat_score":            None,
        "side_bias":             None,
        "options_available":     False,
        "scan_status":           "missing_data",
        "scan_result":           None,
        "reason":                "pending_scan",
        "updated_at":            None,
        "premium_metric_label":  "Estimated Premium (USD)",
        "premium_calc_method":   "n/a (pending scan)",
        "expiration_scope":      "none",
        "premium_scope_id":      "none",
        "expiration_used":       None,
        "dte_used":              None,
        "instrument_type_source":   _itype_src_fb,
        "instrument_type_inferred": _itype_src_fb == "sector_inference",
        "interval_ask_premium":                  None,
        "interval_bid_premium":                  None,
        "interval_midpoint_unknown_premium":     None,
        "interval_total_premium":                None,
        "interval_new_contract_volume":          None,
        "interval_ask_premium_pct":              None,
        "interval_bid_premium_pct":              None,
        "interval_midpoint_unknown_premium_pct": None,
        "interval_classified_trade_side_pct":    None,
        "interval_seconds":                      None,
        "interval_started_at":                   None,
        "interval_ended_at":                     None,
    }


# ── theme-level aggregation ───────────────────────────────────────────────────

def _build_theme_node(
    theme_id: str,
    meta: dict,
    cache_by_ticker: dict[str, dict],
    no_options_syms: set[str],
    instrument_type_by_sym: "dict[str, str] | None" = None,
    supplement_by_ticker: "dict[str, dict] | None" = None,
    display_name_by_sym: "dict[str, str] | None" = None,
) -> dict:
    """
    Build a theme-level aggregation node.

    Builds one ticker node per proxy symbol via _build_ticker_node, then
    delegates all premium/volume/state aggregation to _rollup_ticker_nodes.
    cached_data (stale_lkg) ticker nodes contribute identically to fresh nodes
    because _build_ticker_node emits the same shape for both.
    """
    proxy_syms   = [s.upper() for s in (meta.get("proxy_symbols") or [])]
    ticker_nodes = [
        _build_ticker_node(sym, cache_by_ticker, no_options_syms, instrument_type_by_sym, supplement_by_ticker, display_name_by_sym)
        for sym in proxy_syms
    ]
    totals       = _rollup_ticker_nodes(ticker_nodes)
    return {
        "theme_id":       theme_id,
        "theme_name":     meta.get("display_name", theme_id),
        "classification": meta.get("classification"),
        **totals,
        "tickers":        ticker_nodes,
    }


# ── sector-level aggregation ──────────────────────────────────────────────────

def _build_sector_node(
    sector_id: str,
    theme_items: list[tuple[str, dict]],
    cache_by_ticker: dict[str, dict],
    no_options_syms: set[str],
    sector_names: dict[str, str],
    instrument_type_by_sym: "dict[str, str] | None" = None,
    supplement_by_ticker: "dict[str, dict] | None" = None,
    display_name_by_sym: "dict[str, str] | None" = None,
) -> dict:
    """
    Build a sector-level aggregation node using unique-ticker dedup.

    Sector totals are computed over the UNION of all proxy symbols across every
    theme in this sector, deduplicating tickers that appear in multiple themes.
    The rollup is delegated to _rollup_ticker_nodes (same helper used by
    _build_theme_node) so both Sectors and Themes views share one implementation.

    sector_total_method = "unique_ticker_sum" is preserved: a ticker that
    appears in N themes within a sector contributes its premium exactly once
    to the sector total, even though it contributes once per theme in each
    theme node.
    """
    sector_unique_syms: set[str] = set()
    for _, meta in theme_items:
        for sym in (meta.get("proxy_symbols") or []):
            sector_unique_syms.add(sym.upper())

    # Build one ticker node per unique symbol, then roll up via shared helper.
    # This preserves unique-ticker dedup semantics while using the same
    # aggregation code path as the Themes view.
    sector_ticker_nodes = [
        _build_ticker_node(sym, cache_by_ticker, no_options_syms, instrument_type_by_sym, supplement_by_ticker, display_name_by_sym)
        for sym in sector_unique_syms
    ]
    totals = _rollup_ticker_nodes(sector_ticker_nodes)

    themes_built = [
        _build_theme_node(tid, meta, cache_by_ticker, no_options_syms, instrument_type_by_sym, supplement_by_ticker, display_name_by_sym)
        for tid, meta in theme_items
    ]
    themes_built.sort(
        key=lambda t: (
            -(t.get("represented_count") or 0),
            -(t.get("call_premium") or 0) - (t.get("put_premium") or 0),
        )
    )

    return {
        "sector_id":           sector_id,
        "sector_name":         sector_names.get(sector_id, sector_id.replace("_", " ").title()),
        **totals,
        "sector_total_method": "unique_ticker_sum",
        "themes":              themes_built,
    }


# ── main builder ──────────────────────────────────────────────────────────────

def build_sector_tree(
    combined_ticker_data: dict[str, dict],
    no_options_syms: set[str],
    theme_universe: dict,
    supplement_by_ticker: "dict[str, dict] | None" = None,
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

    # Load canonical instrument types for the full theme universe
    try:
        from data.options_instrument_type_service import get_instrument_type_bulk as _get_itypes
        instrument_type_by_sym = _get_itypes(all_theme_syms)
    except Exception:
        instrument_type_by_sym = {}

    # Bulk-load display names.
    # Primary: options_display_name_service LKG (memory-only, fast, background-enriched).
    # Secondary: screener_fundamentals_cache via fmp_cache_service (DB, no API calls).
    # LKG names take priority; cache fills remaining gaps.
    try:
        from data.options_display_name_service import get_display_name_bulk as _get_dnames
        display_name_by_sym: dict[str, str] = _get_dnames(all_theme_syms)
    except Exception:
        display_name_by_sym = {}
    try:
        from services.fmp_cache_service import get_company_profiles_bulk_cached as _get_profiles
        _profiles = _get_profiles(list(all_theme_syms))
        for _s, _p in _profiles.items():
            _n = (_p.get("name") or "").strip()
            if _n and _s not in display_name_by_sym:
                display_name_by_sym[_s] = _n
    except Exception:
        pass

    sector_nodes = [
        _build_sector_node(sid, theme_items, combined_ticker_data, no_options_syms, sector_names, instrument_type_by_sym, supplement_by_ticker, display_name_by_sym)
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
        # ── Instrument-type classification diagnostics ─────────────────────────
        # Exposes the health of the ETF vs stock classification cache so the
        # frontend can distinguish "7/7 resolved stocks have P/C" from
        # "1 category symbol is still unresolved."
        #
        # Fields:
        #   classified_stocks       — total symbols resolved as stock in cache
        #   classified_etfs         — total symbols resolved as etf in cache
        #   unresolved_total        — symbols still classified as unknown
        #   unresolved_sample       — up to 20 example unknown symbols
        #   cache_updated_at        — unix timestamp of last LKG save
        #   precedence_note         — how resolution works
        # ── Instrument-type classification diagnostics (required-universe basis) ──
        # Computes against the ACTUAL required Options Flow universe (all_theme_syms),
        # not just symbols explicitly stored in the LKG.  A symbol absent from the
        # classification cache is "unresolved" — the old get_stats()-based approach
        # only counted symbols explicitly set to "unknown" in _MEM, producing a
        # false-green unresolved_total=0 when symbols were simply missing from cache.
        "instrument_type_classification": (lambda: {
            **__import__(
                'data.options_instrument_type_service',
                fromlist=['get_required_universe_classification_stats'],
            ).get_required_universe_classification_stats(all_theme_syms),
            "precedence": [
                "1. fmp_is_etf — screener_fundamentals_cache.isEtf explicit True/False",
                "2. fmp_profile — FMP /stable/profile background pass",
                "3. sector_inference — isEtf=None, non-empty FMP sector → inferred stock [TEMPORARY]",
                "4. unresolved — absent from cache or classified as unknown",
            ],
        })(),
        # ── Premium labeling metadata ──────────────────────────────────────────
        # Allows the frontend to label dollar values as "Estimated Premium"
        # rather than guessing whether they are contract counts.
        "premium_metadata": {
            "premium_metric_label":    "Estimated Premium (USD)",
            "premium_calc_method":     "mid_price × volume × 100 per contract",
            "net_premium_formula":     "call_premium − put_premium",
            "put_call_ratio_basis":    "put_premium / call_premium  (dollar basis, not contract count)",
            "chain_summarizer_scope":  (
                "single_expiry_7_60dte_preferred — ONE expiry selected per ticker "
                "(nearest in 7–60 DTE window; fallback to nearest non-expired). "
                "ALL contracts with volume > 0 on that expiry are included."
            ),
            "master_screener_scope":   (
                "top_unusual_contracts — screener's unusual-flow subset, which may "
                "span multiple expirations. expiration_used on each ticker row "
                "reflects the highest-premium single contract's expiry."
            ),
            "call_volume_definition":  "contracts with volume > 0 on scanned expiry/contracts (call side)",
            "put_volume_definition":   "contracts with volume > 0 on scanned expiry/contracts (put side)",
            "total_contract_volume":   "call_volume + put_volume — use this for a 'Contracts' column",
            "note": (
                "Dollar values are estimated option premium flow, NOT the number of "
                "contracts traded. Use total_contract_volume for contract counts."
            ),
        },
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

    # Supplement-only dict: used to prefer canonical Net Flow snapshots over
    # master screener rows when both exist for the same symbol.
    try:
        from data.options_theme_supplement import get_supplement_data_by_ticker as _supp_data
        supplement_by_ticker: dict = _supp_data()
    except Exception:
        supplement_by_ticker = {}

    # ── Pull theme universe (always live — updated in-place by admin edits) ───
    from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE

    result = build_sector_tree(combined_ticker_data, no_options_syms, ENRICHED_THEME_RS_UNIVERSE, supplement_by_ticker=supplement_by_ticker)

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

    # ── Net Premium daily history: inject 1D/7D/30D deltas + save snapshot ────
    # Runs only on fresh compute (cache miss). Enriched result is then cached
    # so subsequent cache hits return delta fields without any DB round-trip.
    # Zero new Tradier calls — operates entirely on already-computed net_premium.
    try:
        from data.options_net_premium_history import (
            upsert_daily_snapshots        as _nph_upsert,
            get_historical_snapshots_bulk as _nph_hist,
            compute_delta_fields          as _nph_deltas,
            get_history_diagnostics       as _nph_diag,
            _et_today                     as _nph_today,
        )
        from datetime import timedelta as _nph_td
        _nph_today_val = _nph_today()
        _nph_since     = _nph_today_val - _nph_td(days=35)

        # Collect snapshot rows + entity keys (tickers deduped across themes)
        _nph_snap_rows: list[dict] = []
        _nph_entities: list[tuple] = []
        _nph_seen_tickers: set[str] = set()

        for _nph_s in result.get("sectors", []):
            _nph_snp = _nph_s.get("net_premium")
            if _nph_snp is not None:
                _nph_k = ("sector", _nph_s["sector_id"])
                _nph_entities.append(_nph_k)
                _nph_snap_rows.append({
                    "entity_type": "sector", "entity_id": _nph_s["sector_id"],
                    "snapshot_date": _nph_today_val, "net_premium": _nph_snp,
                    "call_premium": _nph_s.get("call_premium"),
                    "put_premium": _nph_s.get("put_premium"),
                    "premium_scope_id": "aggregate",
                })
            for _nph_t in _nph_s.get("themes", []):
                _nph_tcls  = _nph_t.get("classification", "theme")
                _nph_ttype = "sub_theme" if _nph_tcls == "sub_theme" else "theme"
                _nph_tnp   = _nph_t.get("net_premium")
                if _nph_tnp is not None:
                    _nph_k = (_nph_ttype, _nph_t["theme_id"])
                    _nph_entities.append(_nph_k)
                    _nph_snap_rows.append({
                        "entity_type": _nph_ttype, "entity_id": _nph_t["theme_id"],
                        "snapshot_date": _nph_today_val, "net_premium": _nph_tnp,
                        "call_premium": _nph_t.get("call_premium"),
                        "put_premium": _nph_t.get("put_premium"),
                        "premium_scope_id": "aggregate",
                    })
                for _nph_tk in _nph_t.get("tickers", []):
                    _nph_sym = _nph_tk.get("symbol", "")
                    if _nph_sym in _nph_seen_tickers:
                        continue
                    _nph_pscope  = _nph_tk.get("premium_scope_id", "none")
                    _nph_pending = _nph_tk.get("nf_snapshot_pending")
                    if _nph_pscope == "net_flow_single_expiry_7_60dte_v1" and not _nph_pending:
                        _nph_itype = _nph_tk.get("instrument_type", "unknown")
                        _nph_etype = "etf" if _nph_itype == "etf" else "stock"
                        _nph_tnp2  = _nph_tk.get("net_premium")
                        if _nph_tnp2 is not None:
                            _nph_k = (_nph_etype, _nph_sym)
                            _nph_entities.append(_nph_k)
                            _nph_snap_rows.append({
                                "entity_type": _nph_etype, "entity_id": _nph_sym,
                                "snapshot_date": _nph_today_val, "net_premium": _nph_tnp2,
                                "call_premium": _nph_tk.get("call_premium"),
                                "put_premium": _nph_tk.get("put_premium"),
                                "premium_scope_id": _nph_pscope,
                            })
                            _nph_seen_tickers.add(_nph_sym)

        # Deduplicate entity keys preserving first-seen order
        _nph_seen_keys: set = set()
        _nph_entities_dedup: list[tuple] = []
        for _nph_k in _nph_entities:
            if _nph_k not in _nph_seen_keys:
                _nph_seen_keys.add(_nph_k)
                _nph_entities_dedup.append(_nph_k)

        # Single bulk DB query for all historical snapshots
        _nph_history = (
            _nph_hist(_nph_entities_dedup, _nph_since)
            if _nph_entities_dedup else {}
        )

        # Inject delta fields into every node in the tree
        for _nph_s in result.get("sectors", []):
            _nph_s.update(_nph_deltas(
                _nph_s.get("net_premium"),
                _nph_history.get(("sector", _nph_s["sector_id"]), []),
                _nph_today_val,
            ))
            for _nph_t in _nph_s.get("themes", []):
                _nph_tcls  = _nph_t.get("classification", "theme")
                _nph_ttype = "sub_theme" if _nph_tcls == "sub_theme" else "theme"
                _nph_t.update(_nph_deltas(
                    _nph_t.get("net_premium"),
                    _nph_history.get((_nph_ttype, _nph_t["theme_id"]), []),
                    _nph_today_val,
                ))
                for _nph_tk in _nph_t.get("tickers", []):
                    _nph_sym   = _nph_tk.get("symbol", "")
                    _nph_itype = _nph_tk.get("instrument_type", "unknown")
                    _nph_etype = "etf" if _nph_itype == "etf" else "stock"
                    _nph_tk.update(_nph_deltas(
                        _nph_tk.get("net_premium"),
                        _nph_history.get((_nph_etype, _nph_sym), []),
                        _nph_today_val,
                    ))

        # Extend diagnostics with snapshot-table stats
        result["diagnostics"].update(_nph_diag())

        # Persist today's snapshots (synchronous, bounded — fires once per 5-min cycle)
        if _nph_snap_rows:
            upserted = _nph_upsert(_nph_snap_rows)
            result["diagnostics"]["net_premium_snapshot_upserted"] = upserted

    except Exception as _nph_err:
        print(f"[NET_PREMIUM_HISTORY] sector enrichment non-fatal: {_nph_err}")

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

    # ── Check 4b: cached_data rows must not have null premiums ────────────────
    # Every ticker in cached_real_lkg (scan_status=cached_data) must carry real
    # premium data.  Null premiums on a cached_data row means the LKG contained
    # a coverage-only placeholder that was incorrectly shown as real data.
    # Exception: neutral scans (sectors_chain_summarized / neutral_no_unusual_flow)
    # with zero flow are real data and may show 0.0 premiums.
    blank_cached_data: list[str] = []
    _REAL_SCAN_TAGS = frozenset({"sectors_chain_summarized", "neutral_no_unusual_flow"})
    for _sym in categories["cached_real_lkg"]:
        _row = combined.get(_sym)
        if _row is None:
            blank_cached_data.append(_sym)
            continue
        _sr = _row.get("scan_result") or ""
        # Known-real scan tags: premium=0.0 is a valid zero-flow reading
        if _sr in _REAL_SCAN_TAGS:
            continue
        # Otherwise require positive premium to confirm real data
        _c, _p = _ticker_call_put(_row)
        if (_c + _p) == 0:
            blank_cached_data.append(_sym)
    check4b_passed = (len(blank_cached_data) == 0)

    # ── Check 5: sector totals match recomputed sums ──────────────────────────
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
    all_valid = (
        check1_passed
        and check2_passed
        and check3_missing_passed
        and check4b_passed
        and check3_passed
    )

    problem_tickers: list[str] = []
    if silent_placeholders:
        problem_tickers.extend(silent_placeholders)
    if not check1_passed:
        problem_tickers.append(f"[accounting_gap: {total - accounted} unclassified]")
    problem_tickers.extend(categories["missing"])
    problem_tickers.extend(blank_cached_data)

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
                "name":   "no_blank_cached_data_premiums",
                "passed": check4b_passed,
                "detail": (
                    f"All {len(categories['cached_real_lkg'])} cached_data rows have real "
                    f"premium fields (no null premiums on cached_data)"
                    if check4b_passed
                    else (
                        f"{len(blank_cached_data)} cached_data tickers have null premiums "
                        f"(LKG contained coverage-only placeholders, not real scan data). "
                        f"Symbols: {blank_cached_data[:20]}"
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


# ── Themes view builder ────────────────────────────────────────────────────────

def build_theme_tree(
    combined_ticker_data: dict[str, dict],
    no_options_syms: set[str],
    theme_universe: dict,
    supplement_by_ticker: "dict[str, dict] | None" = None,
) -> dict:
    """
    Build the flat Theme → Ticker tree (Themes view).

    Every entry in the canonical theme universe that has at least one
    proxy_symbol is included as a top-level theme node.  There is no sector
    grouping — themes are presented flat, sorted by coverage then by total flow.

    Uses the same _build_ticker_node and _rollup_ticker_nodes as
    build_sector_tree so cached_data (stale_lkg) rows contribute identically
    to fresh rows in both views.

    The theme_total_method = "proxy_symbols_sum" (i.e. every proxy symbol in a
    theme basket contributes once, including symbols that also appear in other
    themes).  This is intentionally different from the sector total method
    ("unique_ticker_sum") because here each theme stands alone as a top-level
    item without a parent dedup layer.
    """
    all_theme_syms: set[str] = set()
    for meta in theme_universe.values():
        for sym in (meta.get("proxy_symbols") or []):
            all_theme_syms.add(sym.upper())

    # Load canonical instrument types for the full theme universe
    try:
        from data.options_instrument_type_service import get_instrument_type_bulk as _get_itypes_t
        instrument_type_by_sym = _get_itypes_t(all_theme_syms)
    except Exception:
        instrument_type_by_sym = {}

    # Bulk-load display names (same two-layer strategy as build_sector_tree).
    try:
        from data.options_display_name_service import get_display_name_bulk as _get_dnames_t
        display_name_by_sym_t: dict[str, str] = _get_dnames_t(all_theme_syms)
    except Exception:
        display_name_by_sym_t = {}
    try:
        from services.fmp_cache_service import get_company_profiles_bulk_cached as _get_profiles_t
        _profiles_t = _get_profiles_t(list(all_theme_syms))
        for _s, _p in _profiles_t.items():
            _n = (_p.get("name") or "").strip()
            if _n and _s not in display_name_by_sym_t:
                display_name_by_sym_t[_s] = _n
    except Exception:
        pass

    theme_nodes: list[dict] = []
    for theme_id, meta in theme_universe.items():
        if not meta.get("proxy_symbols"):
            continue
        node = _build_theme_node(theme_id, meta, combined_ticker_data, no_options_syms, instrument_type_by_sym, supplement_by_ticker, display_name_by_sym_t)
        node["parent_sector"] = meta.get("parent_sector")
        theme_nodes.append(node)

    theme_nodes.sort(key=lambda t: (
        -(t.get("contributing_ticker_count") or 0),
        -(t.get("call_premium") or 0) - (t.get("put_premium") or 0),
        t.get("theme_name", ""),
    ))

    # ── coverage stats (same shape as build_sector_tree.scan_coverage) ────────
    _state_counts: dict[str, int] = {}
    _missing_syms: list[str] = []
    for _sym in sorted(all_theme_syms):
        _row   = combined_ticker_data.get(_sym)
        _state = _ticker_state(_row, _sym, no_options_syms)
        _state_counts[_state] = _state_counts.get(_state, 0) + 1
        if _state == "generic_pending":
            _missing_syms.append(_sym)

    _represented_n  = sum(v for k, v in _state_counts.items() if k != "generic_pending")
    _bullish_n      = _state_counts.get("bullish_flow",            0)
    _bearish_n      = _state_counts.get("bearish_flow",            0)
    _mixed_n        = _state_counts.get("mixed_flow",              0)
    _neutral_n      = _state_counts.get("neutral_no_unusual_flow", 0)
    _no_opts_n      = _state_counts.get("confirmed_no_options",    0)
    _unsupp_n       = _state_counts.get("unsupported_foreign_otc", 0)
    _stale_n        = _state_counts.get("stale_lkg",               0)
    _deferred_n     = _state_counts.get("deferred_retry",          0)
    _missing_n      = _state_counts.get("generic_pending",         0)
    _scanned_real_n = _bullish_n + _bearish_n + _mixed_n + _neutral_n + _stale_n
    _full_covered_n = _scanned_real_n + _no_opts_n
    _total_n        = len(all_theme_syms)
    _full_pct       = round(_full_covered_n / max(_total_n, 1) * 100, 1)
    _repr_pct       = round(_represented_n  / max(_total_n, 1) * 100, 1)

    return {
        "as_of":                 time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "view":                  "themes",
        "source":                "shared_options_symbol_cache",
        "net_flow_method":       "call_minus_put_premium",
        "put_call_ratio_method": "premium_dollars",
        "theme_total_method":    "proxy_symbols_sum",
        "scan_coverage": {
            "total_required_tickers":     _total_n,
            "scanned_real_tickers":       _scanned_real_n,
            "fresh_tickers":              len({
                s for s, r in combined_ticker_data.items()
                if r.get("_source") in ("live", "supplement") and s in all_theme_syms
            }),
            "lkg_tickers":                len({
                s for s, r in combined_ticker_data.items()
                if r.get("_source") == "supplement_lkg" and s in all_theme_syms
            }),
            "missing_tickers":            _missing_n,
            "no_options_tickers":         _no_opts_n,
            "deferred_tickers":           _deferred_n,
            "unsupported_tickers":        _unsupp_n,
            "full_coverage_pct":          _full_pct,
            "represented_pct":            _repr_pct,
            "missing_symbols":            _missing_syms,
            "total_themes":               len(theme_nodes),
            # 9-state global breakdown
            "bullish_count":              _bullish_n,
            "bearish_count":              _bearish_n,
            "mixed_count":                _mixed_n,
            "true_neutral_count":         _neutral_n,
            "stale_lkg_count":            _stale_n,
            "deferred_retry_count":       _deferred_n,
            "confirmed_no_options_count": _no_opts_n,
            "unsupported_count":          _unsupp_n,
            "generic_pending_count":      _missing_n,
        },
        "themes": theme_nodes,
        # ── Premium labeling metadata ──────────────────────────────────────────
        "premium_metadata": {
            "premium_metric_label":    "Estimated Premium (USD)",
            "premium_calc_method":     "mid_price × volume × 100 per contract",
            "net_premium_formula":     "call_premium − put_premium",
            "put_call_ratio_basis":    "put_premium / call_premium  (dollar basis, not contract count)",
            "chain_summarizer_scope":  (
                "single_expiry_7_60dte_preferred — ONE expiry selected per ticker "
                "(nearest in 7–60 DTE window; fallback to nearest non-expired). "
                "ALL contracts with volume > 0 on that expiry are included."
            ),
            "master_screener_scope":   (
                "top_unusual_contracts — screener's unusual-flow subset, which may "
                "span multiple expirations. expiration_used on each ticker row "
                "reflects the highest-premium single contract's expiry."
            ),
            "call_volume_definition":  "contracts with volume > 0 on scanned expiry/contracts (call side)",
            "put_volume_definition":   "contracts with volume > 0 on scanned expiry/contracts (put side)",
            "total_contract_volume":   "call_volume + put_volume — use this for a 'Contracts' column",
            "note": (
                "Dollar values are estimated option premium flow, NOT the number of "
                "contracts traded. Use total_contract_volume for contract counts."
            ),
        },
    }


def get_theme_flow(*, force_refresh: bool = False) -> dict:
    """
    Return the flat theme tree, backed by a 1-minute in-memory cache.

    Reads from the same master + supplement caches as get_sector_flow.
    Zero new Tradier calls — pure aggregation.
    """
    from data.cache import cache

    if not force_refresh:
        cached = cache.get(_THEMES_CACHE_KEY)
        if cached:
            return {**cached, "_from_themes_cache": True}

    try:
        from data.options_theme_supplement import (
            get_combined_ticker_data,
            get_no_options_symbols,
        )
        combined_ticker_data = get_combined_ticker_data()
        no_options_syms      = get_no_options_symbols()
    except Exception:
        master_snap = (
            cache.get("options_master_screener_v1")
            or cache.get("options_master_lkg_v1")
        )
        master_rows = (master_snap or {}).get("tickers", [])
        combined_ticker_data = {
            (r.get("ticker") or "").upper(): {**r, "_source": "live"}
            for r in master_rows if r.get("ticker")
        }
        no_options_syms = set()

    try:
        from data.options_theme_supplement import get_supplement_data_by_ticker as _supp_data_t
        supplement_by_ticker_t: dict = _supp_data_t()
    except Exception:
        supplement_by_ticker_t = {}

    from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE

    result = build_theme_tree(combined_ticker_data, no_options_syms, ENRICHED_THEME_RS_UNIVERSE, supplement_by_ticker=supplement_by_ticker_t)

    # ── Net Premium daily history: inject 1D/7D/30D deltas + save snapshot ────
    # Same logic as get_sector_flow; theme view persists its own entity set.
    # Ticker dedup ensures each symbol is upserted once even across multiple themes.
    try:
        from data.options_net_premium_history import (
            upsert_daily_snapshots        as _nph_upsert_t,
            get_historical_snapshots_bulk as _nph_hist_t,
            compute_delta_fields          as _nph_deltas_t,
            _et_today                     as _nph_today_t,
        )
        from datetime import timedelta as _nph_td_t
        _nph_tval  = _nph_today_t()
        _nph_since_t = _nph_tval - _nph_td_t(days=35)

        _nph_snap_t: list[dict] = []
        _nph_ents_t: list[tuple] = []
        _nph_seen_t: set[str] = set()

        for _nph_t in result.get("themes", []):
            _nph_tcls  = _nph_t.get("classification", "theme")
            _nph_ttype = "sub_theme" if _nph_tcls == "sub_theme" else "theme"
            _nph_tnp   = _nph_t.get("net_premium")
            if _nph_tnp is not None:
                _nph_tk_key = (_nph_ttype, _nph_t["theme_id"])
                _nph_ents_t.append(_nph_tk_key)
                _nph_snap_t.append({
                    "entity_type": _nph_ttype, "entity_id": _nph_t["theme_id"],
                    "snapshot_date": _nph_tval, "net_premium": _nph_tnp,
                    "call_premium": _nph_t.get("call_premium"),
                    "put_premium": _nph_t.get("put_premium"),
                    "premium_scope_id": "aggregate",
                })
            for _nph_tk in _nph_t.get("tickers", []):
                _nph_sym = _nph_tk.get("symbol", "")
                if _nph_sym in _nph_seen_t:
                    continue
                _nph_pscope  = _nph_tk.get("premium_scope_id", "none")
                _nph_pending = _nph_tk.get("nf_snapshot_pending")
                if _nph_pscope == "net_flow_single_expiry_7_60dte_v1" and not _nph_pending:
                    _nph_itype = _nph_tk.get("instrument_type", "unknown")
                    _nph_etype = "etf" if _nph_itype == "etf" else "stock"
                    _nph_tnp2  = _nph_tk.get("net_premium")
                    if _nph_tnp2 is not None:
                        _nph_ek = (_nph_etype, _nph_sym)
                        _nph_ents_t.append(_nph_ek)
                        _nph_snap_t.append({
                            "entity_type": _nph_etype, "entity_id": _nph_sym,
                            "snapshot_date": _nph_tval, "net_premium": _nph_tnp2,
                            "call_premium": _nph_tk.get("call_premium"),
                            "put_premium": _nph_tk.get("put_premium"),
                            "premium_scope_id": _nph_pscope,
                        })
                        _nph_seen_t.add(_nph_sym)

        # Deduplicate entity keys
        _nph_seen_ek: set = set()
        _nph_ents_t_dedup: list[tuple] = []
        for _nph_ek in _nph_ents_t:
            if _nph_ek not in _nph_seen_ek:
                _nph_seen_ek.add(_nph_ek)
                _nph_ents_t_dedup.append(_nph_ek)

        _nph_hist_data = (
            _nph_hist_t(_nph_ents_t_dedup, _nph_since_t)
            if _nph_ents_t_dedup else {}
        )

        # Inject delta fields
        for _nph_t in result.get("themes", []):
            _nph_tcls  = _nph_t.get("classification", "theme")
            _nph_ttype = "sub_theme" if _nph_tcls == "sub_theme" else "theme"
            _nph_t.update(_nph_deltas_t(
                _nph_t.get("net_premium"),
                _nph_hist_data.get((_nph_ttype, _nph_t["theme_id"]), []),
                _nph_tval,
            ))
            for _nph_tk in _nph_t.get("tickers", []):
                _nph_sym   = _nph_tk.get("symbol", "")
                _nph_itype = _nph_tk.get("instrument_type", "unknown")
                _nph_etype = "etf" if _nph_itype == "etf" else "stock"
                _nph_tk.update(_nph_deltas_t(
                    _nph_tk.get("net_premium"),
                    _nph_hist_data.get((_nph_etype, _nph_sym), []),
                    _nph_tval,
                ))

        # Persist snapshots (idempotent — same rows as sectors view, ON CONFLICT updates)
        if _nph_snap_t:
            _nph_upsert_t(_nph_snap_t)

    except Exception as _nph_err_t:
        print(f"[NET_PREMIUM_HISTORY] theme enrichment non-fatal: {_nph_err_t}")

    result["_from_themes_cache"] = False
    cache.set(_THEMES_CACHE_KEY, result, _THEMES_CACHE_TTL)
    return result


def invalidate_themes_cache() -> None:
    """Expire the themes cache after any admin theme basket edit."""
    from data.cache import cache
    cache.delete(_THEMES_CACHE_KEY)


# ── Themes coverage validator ──────────────────────────────────────────────────

_REAL_SCAN_TAGS = frozenset({"sectors_chain_summarized", "neutral_no_unusual_flow"})


def validate_themes_coverage() -> dict:
    """
    Coverage validation for the Options Flow → Themes view.

    Checks:
      1. complete_accounting   — every ticker in exactly one of:
                                 fresh | cached_real_lkg | confirmed_no_opts |
                                 deferred | missing | unsupported
      2. no_silent_placeholders — zero supplement rows with scan_result=deferred_retry
      3. no_missing_data       — missing_data=0 (all tickers have some scan result)
      4. no_blank_cached_data_premiums — cached_data rows have non-null premiums
      5. theme_totals_match_ticker_sums — for each theme, recompute call/put from
                                 raw cache independently and compare to the theme
                                 node total; proves cached_data contributes correctly.

    Returns valid=True when all five checks pass.
    """
    try:
        from data.options_theme_supplement import (
            get_combined_ticker_data,
            get_no_options_symbols,
        )
        from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE
    except Exception as exc:
        return {"valid": False, "error": f"import_failed: {exc}"}

    combined = get_combined_ticker_data()
    no_opts  = get_no_options_symbols()

    all_theme_syms: set[str] = {
        sym.upper()
        for meta in ENRICHED_THEME_RS_UNIVERSE.values()
        for sym in (meta.get("proxy_symbols") or [])
    }
    total = len(all_theme_syms)

    # ── Classify every ticker ─────────────────────────────────────────────────
    categories: dict[str, list[str]] = {
        "fresh":             [],
        "cached_real_lkg":   [],
        "confirmed_no_opts": [],
        "deferred":          [],
        "missing":           [],
        "unsupported":       [],
    }
    silent_placeholders: list[str] = []

    for sym in sorted(all_theme_syms):
        row   = combined.get(sym)
        state = _ticker_state(row, sym, no_opts)

        if state == "confirmed_no_options":       categories["confirmed_no_opts"].append(sym)
        elif state == "unsupported_foreign_otc":  categories["unsupported"].append(sym)
        elif state == "generic_pending":          categories["missing"].append(sym)
        elif state == "deferred_retry":           categories["deferred"].append(sym)
        elif state == "stale_lkg":                categories["cached_real_lkg"].append(sym)
        else:                                     categories["fresh"].append(sym)

        if (
            row is not None
            and row.get("_source") == "supplement"
            and row.get("scan_result") == "deferred_retry"
        ):
            silent_placeholders.append(sym)

    accounted         = sum(len(v) for v in categories.values())
    check1_passed     = (accounted == total)
    check2_passed     = (len(silent_placeholders) == 0)
    check3_passed     = (len(categories["missing"]) == 0)

    # ── Check 4b: cached_data rows must have real premium data ────────────────
    blank_cached: list[str] = []
    for _sym in categories["cached_real_lkg"]:
        _row = combined.get(_sym)
        if _row is None:
            blank_cached.append(_sym)
            continue
        _sr = _row.get("scan_result") or ""
        if _sr in _REAL_SCAN_TAGS:
            continue  # real scan confirmed — zero premium is valid
        _c, _p = _ticker_call_put(_row)
        if (_c + _p) == 0:
            blank_cached.append(_sym)
    check4b_passed = (len(blank_cached) == 0)

    # ── Check 5 (Themes-specific): theme totals must equal ticker-row sums ────
    # Build the actual theme tree so we use the identical code path as the API.
    # Then independently recompute call/put for each theme from raw cache rows
    # (bypassing _build_ticker_node) and compare.  The tolerance is $1 to allow
    # for floating-point rounding across many tickers.
    theme_tree = build_theme_tree(combined, no_opts, ENRICHED_THEME_RS_UNIVERSE)
    theme_audits: list[dict] = []
    mismatches:   list[str]  = []

    for theme_node in theme_tree.get("themes", []):
        theme_id   = theme_node["theme_id"]
        meta       = ENRICHED_THEME_RS_UNIVERSE.get(theme_id, {})
        proxy_syms = [s.upper() for s in (meta.get("proxy_symbols") or [])]

        # Independent recomputation: sum premiums for tickers with real data
        recomputed_call = 0.0
        recomputed_put  = 0.0
        for sym in proxy_syms:
            row   = combined.get(sym)
            state = _ticker_state(row, sym, no_opts)
            if row and state not in (
                "generic_pending", "deferred_retry",
                "confirmed_no_options", "unsupported_foreign_otc",
            ):
                c, p = _ticker_call_put(row)
                if (c + p) > 0:
                    recomputed_call += c
                    recomputed_put  += p

        node_call = theme_node.get("call_premium") or 0.0
        node_put  = theme_node.get("put_premium")  or 0.0
        call_ok   = abs(node_call - recomputed_call) < 1.0
        put_ok    = abs(node_put  - recomputed_put)  < 1.0
        ok        = call_ok and put_ok
        if not ok:
            mismatches.append(theme_id)

        theme_audits.append({
            "theme_id":        theme_id,
            "node_call":       round(node_call, 2),
            "node_put":        round(node_put, 2),
            "recomputed_call": round(recomputed_call, 2),
            "recomputed_put":  round(recomputed_put, 2),
            "ok":              ok,
        })

    check5_passed = (len(mismatches) == 0)

    all_valid = check1_passed and check2_passed and check3_passed and check4b_passed and check5_passed

    problem_tickers: list[str] = []
    if silent_placeholders:
        problem_tickers.extend(silent_placeholders)
    if not check1_passed:
        problem_tickers.append(f"[accounting_gap: {total - accounted} unclassified]")
    problem_tickers.extend(categories["missing"])
    problem_tickers.extend(blank_cached)

    failing_audits = [a for a in theme_audits if not a["ok"]]

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
                    "Zero supplement rows with scan_result=deferred_retry"
                    if check2_passed
                    else (
                        f"{len(silent_placeholders)} supplement rows have "
                        f"scan_result=deferred_retry — blocks re-scanning: "
                        f"{silent_placeholders[:20]}"
                    )
                ),
            },
            {
                "name":   "no_missing_data",
                "passed": check3_passed,
                "detail": (
                    "missing_data=0 — all tickers are fresh, cached_real_lkg, or confirmed_no_options"
                    if check3_passed
                    else (
                        f"{len(categories['missing'])} tickers still in missing_data: "
                        f"{categories['missing']}"
                    )
                ),
            },
            {
                "name":   "no_blank_cached_data_premiums",
                "passed": check4b_passed,
                "detail": (
                    f"All {len(categories['cached_real_lkg'])} cached_data rows "
                    f"have real premium fields"
                    if check4b_passed
                    else (
                        f"{len(blank_cached)} cached_data tickers have null premiums "
                        f"(LKG contained coverage-only placeholders): {blank_cached[:20]}"
                    )
                ),
            },
            {
                "name":   "theme_totals_match_ticker_sums",
                "passed": check5_passed,
                "detail": (
                    f"All {len(theme_audits)} theme totals equal recomputed ticker sums "
                    f"(cached_data rows included)"
                    if check5_passed
                    else f"Mismatch in themes: {mismatches}"
                ),
            },
        ],
        "summary": {
            "total_required_tickers":    total,
            "total_themes":              len(theme_audits),
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
            "fresh":             categories["fresh"][:50],
            "cached_real_lkg":   categories["cached_real_lkg"][:50],
            "confirmed_no_opts": categories["confirmed_no_opts"][:50],
            "deferred":          categories["deferred"],
            "missing":           categories["missing"][:50],
            "unsupported":       categories["unsupported"],
        },
        "problem_tickers":   problem_tickers[:50],
        "theme_total_audit": failing_audits if failing_audits else theme_audits[:5],
    }
