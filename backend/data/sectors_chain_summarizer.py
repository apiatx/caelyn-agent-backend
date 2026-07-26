"""
sectors_chain_summarizer.py — Direct chain summarizer for the Sectors tab.

Fetches expirations + the primary expiration chain for each ticker and
summarizes ALL contracts (not filtered by unusual flow) to produce real
call/put premium and volume metrics.

Why this exists
---------------
run_live_scan() is designed to surface UNUSUAL flow — it only writes result
rows for tickers that exceed the unusual-flow threshold.  Neutral tickers
get coverage rows with premium=0.  The Sectors tab needs real call/put premium
data for EVERY optionable ticker regardless of whether flow is unusual.

Premium formula (per contract)
-------------------------------
  price  = mid(bid, ask) if both > 0 → last → bid → ask
  prem $ = volume × price × 100

Interval trade-side classification
-----------------------------------
  volume_delta = current_snapshot_volume − prior_snapshot_volume

  Only the delta volume (newly observed contracts since the previous scan
  cycle) is classified as ask-side, bid-side, or midpoint/unknown using the
  current snapshot last / bid / ask.  The prior cumulative session volume is
  never assigned to a side wholesale.

  On the first observation for a contract (no prior snapshot), volume_delta=0
  and no premium is classified.  This is the correct behavior after a backend
  restart or a new trading session.

  If current_volume < prior_volume (session rollover or anomalous reset),
  the baseline is reset and delta=0.

Budget
------
Caller must wrap scan_batch_for_sectors() in the appropriate lane() context:

  from data.tradier_budget import lane
  with lane("sectors"):          # when Sectors page is active
      results = await scan_batch_for_sectors(batch, tradier, expiry_cache)

  with lane("maintenance"):      # background / off-hours
      results = await scan_batch_for_sectors(batch, tradier, expiry_cache)

Expiry cache
------------
expiry_cache is a plain dict shared across backfill-loop iterations:
  {SYMBOL: ([exp_strings, ...], checked_at_float)}

Populated by this module on first expiry fetch; subsequent calls for the
same symbol skip the Tradier call.  Cache entries are reused across loop
iterations within the same server session so the expensive expiry call is
paid once.
"""
from __future__ import annotations

import asyncio
import time
from datetime import date, datetime
from typing import Optional

_EXPIRY_CACHE_TTL = 12 * 3600   # 12 hours

# Absolute dollar tolerance for last-price bid/ask side classification.
# Applied only to delta volume (new contracts since prior snapshot).
#   last >= ask - _SIDE_TOL  → "ask_side"
#   last <= bid + _SIDE_TOL  → "bid_side"
#   otherwise                → "midpoint_or_unknown"
_SIDE_TOL = 0.02

# ── Per-contract volume snapshot cache ────────────────────────────────────────
#
# Stores the most recently observed cumulative volume per option contract so the
# next scan cycle can compute:
#   volume_delta = current_snapshot_volume − prior_snapshot_volume
# and classify ONLY the newly observed contracts.
#
# Key  : provider option symbol (e.g. "AAPL250117C00150000") when available,
#         otherwise "{sym}:{expiry}:{strike:.2f}:{option_type}" as a fallback.
# Value: {"volume": int, "observed_at": float, "expiry": str}
#
# No disk persistence — survives between scan cycles within the same server
# session.  Cleared on restart (correct: cumulative session volume from a
# previous session must not be used as a prior baseline).
#
# Memory footprint estimate:
#   ~300–500 tickers × ~50–150 contracts per expiry × ~130 bytes per entry
#   → approximately 2–10 MB depending on universe size.  Well within budget.
#
# Eviction (rate-limited to once per _EVICTION_INTERVAL):
#   - Contracts whose expiry date has passed (options expired).
#   - Entries not refreshed within _SNAPSHOT_TTL seconds (26 h covers
#     overnight without evicting during an active session, while still
#     clearing symbols that drop out of the scan universe).
_CONTRACT_SNAPSHOTS: dict[str, dict] = {}
_SNAPSHOT_TTL       = 26 * 3600   # 26 hours
_EVICTION_INTERVAL  =  1 * 3600   # run eviction at most once per hour
_last_eviction: float = 0.0


# ── helpers ───────────────────────────────────────────────────────────────────

def _sf(v) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _si(v) -> int:
    try:
        return int(float(v)) if v is not None else 0
    except (TypeError, ValueError):
        return 0


def _best_price(bid, ask, last) -> Optional[float]:
    """
    Best-price priority for premium calculation:
      mid(bid, ask) → last → bid → ask

    Mirrors the _midpoint() logic used in options_flow_engine.
    This is the price basis for both cumulative and delta premium estimates.
    """
    b = _sf(bid)
    a = _sf(ask)
    l = _sf(last)
    if b is not None and a is not None and b > 0 and a >= b:
        return round((b + a) / 2.0, 4)
    if l is not None and l > 0:
        return round(l, 4)
    if b is not None and b > 0:
        return round(b, 4)
    if a is not None and a > 0:
        return round(a, 4)
    return None


def _classify_side(bid: Optional[float], ask: Optional[float],
                   last: Optional[float]) -> str:
    """
    Classify a contract's most recent trade as ask-side, bid-side, or
    midpoint/unknown.

    Applied ONLY to delta volume (newly observed contracts since the prior
    snapshot) — never to cumulative session volume.

    Tolerance _SIDE_TOL = $0.02 (absolute):
      last >= ask - _SIDE_TOL  → "ask_side"   (traded at/near the offer)
      last <= bid + _SIDE_TOL  → "bid_side"   (traded at/near the bid)
      otherwise                → "midpoint_or_unknown"

    Returns "midpoint_or_unknown" whenever last, bid, or ask are
    missing, zero, or produce an inverted spread (ask < bid).
    Does NOT force classification by option type.
    """
    if last is None or last <= 0:
        return "midpoint_or_unknown"
    if bid is None or ask is None or ask <= 0 or bid < 0 or ask < bid:
        return "midpoint_or_unknown"
    if last >= ask - _SIDE_TOL:
        return "ask_side"
    if last <= bid + _SIDE_TOL:
        return "bid_side"
    return "midpoint_or_unknown"


def _dte(exp_str: str) -> Optional[int]:
    try:
        return (datetime.strptime(exp_str, "%Y-%m-%d").date() - date.today()).days
    except Exception:
        return None


def _select_primary_exp(expirations: list[str]) -> Optional[str]:
    """
    Select primary expiration for chain summarization.

    Priority:
    1. Nearest expiration in the 7–60 DTE window (front-month premium).
    2. Nearest non-expired expiration outside that window.
    3. First element as last resort.
    """
    if not expirations:
        return None
    window = [(e, d) for e in expirations if (d := _dte(e)) is not None and 7 <= d <= 60]
    if window:
        return min(window, key=lambda x: x[1])[0]
    valid = [(e, d) for e in expirations if (d := _dte(e)) is not None and d >= 0]
    if valid:
        return min(valid, key=lambda x: x[1])[0]
    return expirations[0]


def _contract_key(opt_symbol: Optional[str], sym: str, expiry: str,
                  strike: Optional[float], option_type: Optional[str]) -> str:
    """
    Unique key for the per-contract snapshot cache.

    Prefers the provider option symbol (e.g. "AAPL250117C00150000") because it
    is already unique and compact.  Falls back to a composite key when the
    provider symbol is absent or empty.
    """
    if opt_symbol:
        return opt_symbol
    strike_s = f"{strike:.2f}" if strike is not None else "?"
    return f"{sym.upper()}:{expiry}:{strike_s}:{option_type or '?'}"


def _evict_stale_contracts() -> None:
    """
    Remove expired and idle entries from _CONTRACT_SNAPSHOTS.

    Rate-limited to once per _EVICTION_INTERVAL so repeated backfill-loop
    calls within the same hour pay no eviction cost after the first run.
    """
    global _last_eviction
    now = time.time()
    if now - _last_eviction < _EVICTION_INTERVAL:
        return
    _last_eviction = now
    today_str = date.today().isoformat()
    cutoff = now - _SNAPSHOT_TTL
    keys_to_delete = [
        key for key, snap in _CONTRACT_SNAPSHOTS.items()
        if snap.get("observed_at", 0) < cutoff           # idle too long
        or snap.get("expiry", "") < today_str             # expiry has passed
    ]
    for key in keys_to_delete:
        _CONTRACT_SNAPSHOTS.pop(key, None)


def snapshot_cache_stats() -> dict:
    """Return diagnostic stats for the per-contract snapshot cache."""
    return {
        "entry_count":       len(_CONTRACT_SNAPSHOTS),
        "last_eviction_ago": round(time.time() - _last_eviction) if _last_eviction else None,
        "snapshot_ttl_h":    _SNAPSHOT_TTL / 3600,
        "eviction_interval_h": _EVICTION_INTERVAL / 3600,
    }


def get_cached_underlying_prices(symbols: list[str]) -> dict[str, float]:
    """
    Build an {UPPERCASE_SYM: float_price} map from existing in-memory caches.

    Zero Tradier / FMP calls.  Priority order:
      1. services.watchlist_quote_cache._quote_cache  (10-min module-level dict)
      2. data.cache  tradier:quote:sym:{SYM}           (60-s per-symbol cache)
      3. data.cache  quote:lkg:{SYM}                  (72-h LKG cache)

    Returns only symbols with a valid positive price.  Never blocks or awaits.
    Safe to call from any sync context.
    """
    prices: dict[str, float] = {}
    syms_upper = [s.upper() for s in symbols]

    # ── Layer 1: watchlist_quote_cache module-level dict ──────────────────────
    try:
        import services.watchlist_quote_cache as _wqc
        _wq: dict = _wqc._quote_cache          # pure dict read — no lock needed
        for sym in syms_upper:
            q = _wq.get(sym) or {}
            try:
                p_raw = q.get("price")
                if p_raw is not None:
                    p = float(p_raw)
                    if p > 0 and p == p:       # finite and positive
                        prices[sym] = p
            except (TypeError, ValueError):
                pass
    except Exception:
        pass

    # ── Layer 2 + 3: per-symbol data.cache entries for remaining symbols ──────
    missing = [s for s in syms_upper if s not in prices]
    if missing:
        try:
            from data.cache import cache as _dc
            for sym in missing:
                for _key in (f"tradier:quote:sym:{sym}", f"quote:lkg:{sym}"):
                    raw = _dc.get(_key)
                    if not raw:
                        continue
                    p_raw = raw.get("last") or raw.get("price")
                    try:
                        p = float(p_raw) if p_raw is not None else None
                    except (TypeError, ValueError):
                        p = None
                    if p is not None and p > 0:
                        prices[sym] = p
                        break
        except Exception:
            pass

    return prices


# ── core chain summarizer ──────────────────────────────────────────────────────

def _budget_ok() -> bool:
    """
    Return True if the current lane has budget headroom right now.

    Used as a pre-check before Tradier calls to avoid budget deferrals
    being misinterpreted as "confirmed no options" (TradierProvider._get()
    returns None when budget is exceeded, which propagates as an empty
    expirations list).  This is a best-effort check — it reduces false
    confirmed_no_options but cannot fully eliminate the race window in a
    concurrent context.
    """
    try:
        import data.tradier_budget as _bgt
        from data.tradier_market_session import is_regular_options_session as _is_reg
        if not (_bgt.FORCE_ENFORCE or _is_reg()):
            return False  # options scans not allowed outside regular session
        return _bgt.check_budget(_bgt.get_current_lane())
    except Exception:
        return True   # default: proceed


async def summarize_ticker_chain(
    sym: str,
    tradier,
    expiry_cache: dict,
    *,
    underlying_price: Optional[float] = None,
) -> dict:
    """
    Fetch expirations + primary chain for *sym*, then summarize ALL contracts.

    Returns a dict containing:

    Cumulative session metrics:
      call_premium, put_premium, net_premium, call_volume, put_volume,
      total_volume — estimated dollar premium and contract counts summed
      over all contracts with volume > 0 in the selected expiry.

    Explicit P/C fields (supplement_v2+):
      premium_put_call_ratio — put_prem / call_prem  (dollar flow ratio)
      volume_put_call_ratio  — put_vol  / call_vol   (contract count ratio)
      put_call_ratio         — alias for premium_put_call_ratio (backward compat)

    Open interest (supplement_v2+):
      call_oi, put_oi, total_oi — summed across ALL contracts in expiry
                                   (not filtered by today's volume)

    IV (supplement_v2+):
      call_iv, put_iv, combined_iv, iv_skew — mean IV across active contracts

    Expected move (supplement_v2+, requires underlying_price):
      expected_move_dollars, expected_move_pct, expected_move_atm_strike

    Chain score (supplement_v2+, requires underlying_price):
      options_score, options_signal, score_components, score_method

    Incremental interval trade-side metrics (volume-delta based):
      interval_ask_premium, interval_bid_premium,
      interval_midpoint_unknown_premium, interval_total_premium,
      interval_new_contract_volume — estimated dollar premium and contract
      count from the delta volume observed since the prior snapshot.

      interval_ask_premium_pct, interval_bid_premium_pct,
      interval_midpoint_unknown_premium_pct,
      interval_classified_trade_side_pct — percentage breakdown derived from
      summed delta dollars (never per-contract averages).

      interval_seconds    — wall-clock seconds between the earliest prior
                            contract snapshot and this scan.
      interval_started_at — epoch of that earliest prior snapshot.
      interval_ended_at   — epoch of this scan (= now).

      All interval_* fields are None when:
        • This is the first scan for the symbol post-restart (no prior snapshots).
        • All contracts had identical volume to their prior snapshot (no new trades).
        • A session reset was detected for all contracts (current < prior volume).

    Special scan_result values:
      "sectors_chain_summarized"  — chain fetched and summarized (real data)
      "confirmed_no_options"      — Tradier returned no expirations
      "deferred_retry"            — budget exhausted or Tradier call failed

    Budget safety
    -------------
    TradierProvider._get() returns None when the lane budget is exceeded,
    propagating as [] from get_option_expirations().  A pre-check + post-check
    distinguishes genuine no-options from budget deferral.
    """
    sym = sym.upper()
    now = time.time()

    # ── 1. Get expirations (cache-first, _EXPIRY_CACHE_TTL) ───────────────────
    expirations: Optional[list[str]] = None
    cached = expiry_cache.get(sym)
    if cached and isinstance(cached, (list, tuple)) and len(cached) >= 2:
        _cached_exps, _cached_at = cached[0], cached[1]
        if now - _cached_at < _EXPIRY_CACHE_TTL:
            expirations = _cached_exps  # within TTL — skip Tradier call

    if expirations is None:
        if not _budget_ok():
            return {
                "ticker":      sym,
                "scan_result": "deferred_retry",
                "reason":      "budget_pre_check_expiry",
                "updated_at":  now,
            }
        try:
            raw = await tradier.get_option_expirations(sym)
        except Exception as exc:
            if cached and isinstance(cached, (list, tuple)) and cached[0]:
                expirations = cached[0]
                print(
                    f"[SECTORS_BF] {sym}: expiry refresh failed "
                    f"({str(exc)[:120]}), using stale list "
                    f"({len(expirations)} expirations, "
                    f"age={(now - cached[1]) / 3600:.1f}h)"
                )
            else:
                return {
                    "ticker":      sym,
                    "scan_result": "deferred_retry",
                    "error":       str(exc)[:200],
                    "updated_at":  now,
                }

        if expirations is None:
            if not raw and not _budget_ok():
                if cached and isinstance(cached, (list, tuple)) and cached[0]:
                    expirations = cached[0]
                    print(
                        f"[SECTORS_BF] {sym}: budget-gated expiry refresh, "
                        f"using stale list ({len(expirations)} expirations, "
                        f"age={(now - cached[1]) / 3600:.1f}h)"
                    )
                else:
                    return {
                        "ticker":      sym,
                        "scan_result": "deferred_retry",
                        "reason":      "budget_post_check_expiry",
                        "updated_at":  now,
                    }
            else:
                expiry_cache[sym] = (raw or [], now)
                expirations = raw or []

    if not expirations:
        return {
            "ticker":         sym,
            "scan_result":    "confirmed_no_options",
            "call_premium":   None,
            "put_premium":    None,
            "net_premium":    None,
            "call_volume":    0,
            "put_volume":     0,
            "total_volume":   0,
            "put_call_ratio": None,
            "updated_at":     now,
            "source":         "sectors_direct",
        }

    # ── 2. Select primary expiration ──────────────────────────────────────────
    primary_exp = _select_primary_exp(expirations) or expirations[0]

    # ── 2a. Record canonical scan fingerprint ─────────────────────────────────
    # Fingerprint = ticker:session_date:exp_scope:exp_set_hash:schema_version
    # Used by the inflight diagnostics to prove one chain fetch per workload.
    try:
        import datetime as _dt
        from services.options_inflight import (
            make_scan_fingerprint as _mkfp,
            record_scan_fingerprint as _recfp,
        )
        _session_date = _dt.datetime.now(
            _dt.timezone(_dt.timedelta(hours=-5))
        ).strftime("%Y-%m-%d")
        _fp = _mkfp(
            sym,
            _session_date,
            exp_scope="7_60dte",
            expirations=[str(primary_exp)],
        )
        _recfp(sym, _fp)
    except Exception:
        pass

    # ── 3. Fetch chain (with budget pre-check) ────────────────────────────────
    if not _budget_ok():
        return {
            "ticker":      sym,
            "scan_result": "deferred_retry",
            "reason":      "budget_pre_check_chain",
            "updated_at":  now,
        }
    try:
        chain = await tradier.get_option_chain(sym, primary_exp)
    except Exception as exc:
        return {
            "ticker":      sym,
            "scan_result": "deferred_retry",
            "error":       str(exc)[:200],
            "updated_at":  now,
        }

    calls = chain.get("calls", []) or []
    puts  = chain.get("puts",  []) or []

    # ── 4. Summarize ALL contracts (volume > 0 only) ──────────────────────────
    #
    # Cumulative session premium (call_prem, put_prem):
    #   Sum over ALL contracts with volume > 0.  Unchanged from original design.
    #   price = _best_price(bid, ask, last) = mid → last → bid → ask.
    #
    # Incremental interval trade-side classification (int_ask, int_bid, int_mid):
    #   For each contract:
    #     1. Look up prior snapshot in _CONTRACT_SNAPSHOTS.
    #     2. If no prior snapshot OR current_volume < prior_volume:
    #          Store current as new baseline.  delta = 0.  Classify nothing.
    #          (First-observation rule / session-reset rule.)
    #     3. Else:
    #          delta = current_volume - prior_volume
    #          If delta > 0:
    #            classify delta using _classify_side(bid, ask, last)
    #            accumulate delta_dollars = delta × price × 100
    #          Update snapshot with current volume and observed_at = now.
    #
    # Eviction of stale/expired snapshots is rate-limited to once per hour.

    _evict_stale_contracts()

    call_prem = 0.0
    put_prem  = 0.0
    call_vol  = 0
    put_vol   = 0

    # ── supplement_v2: OI and IV accumulators ─────────────────────────────────
    call_oi: int = 0
    put_oi:  int = 0
    call_iv_vals: list = []
    put_iv_vals:  list = []

    # Interval (incremental delta) accumulators.
    int_ask     = 0.0   # delta premium traded at/near the ask
    int_bid     = 0.0   # delta premium traded at/near the bid
    int_mid     = 0.0   # delta premium at midpoint or unclassifiable
    int_new_vol = 0     # total delta contract volume across all contracts

    # Earliest prior snapshot observed_at among contracts with non-zero delta.
    # Used to compute interval_seconds = now - int_started_at.
    int_started_at: Optional[float] = None

    for c in calls:
        vol = _si(c.get("volume"))

        # OI: sum across ALL call contracts (not filtered by today's volume)
        call_oi += _si(c.get("openInterest"))

        # IV: collect from contracts with any volume (active market)
        if vol > 0:
            _iv = _sf(c.get("iv"))
            if _iv is not None and _iv > 0:
                call_iv_vals.append(_iv)

        if vol <= 0:
            continue
        b, a, l = _sf(c.get("bid")), _sf(c.get("ask")), _sf(c.get("last"))
        price = _best_price(b, a, l)
        if price is None or price <= 0:
            continue

        call_prem += vol * price * 100.0
        call_vol  += vol

        # Volume-delta interval classification.
        ckey = _contract_key(
            c.get("symbol"), sym, primary_exp,
            _sf(c.get("strike")), "call",
        )
        prev = _CONTRACT_SNAPSHOTS.get(ckey)
        if prev is None or vol < prev["volume"]:
            # First observation or session reset — store baseline, no delta.
            _CONTRACT_SNAPSHOTS[ckey] = {
                "volume": vol, "observed_at": now, "expiry": primary_exp,
            }
        else:
            delta = vol - prev["volume"]
            if delta > 0:
                d_dollars = delta * price * 100.0
                side = _classify_side(b, a, l)
                if side == "ask_side":
                    int_ask += d_dollars
                elif side == "bid_side":
                    int_bid += d_dollars
                else:
                    int_mid += d_dollars
                int_new_vol += delta
                p_at = prev["observed_at"]
                if int_started_at is None or p_at < int_started_at:
                    int_started_at = p_at
            # Update snapshot (even for delta=0 — refreshes observed_at TTL).
            _CONTRACT_SNAPSHOTS[ckey] = {
                "volume": vol, "observed_at": now, "expiry": primary_exp,
            }

    for p in puts:
        vol = _si(p.get("volume"))

        # OI: sum across ALL put contracts (not filtered by today's volume)
        put_oi += _si(p.get("openInterest"))

        # IV: collect from contracts with any volume (active market)
        if vol > 0:
            _iv = _sf(p.get("iv"))
            if _iv is not None and _iv > 0:
                put_iv_vals.append(_iv)

        if vol <= 0:
            continue
        b, a, l = _sf(p.get("bid")), _sf(p.get("ask")), _sf(p.get("last"))
        price = _best_price(b, a, l)
        if price is None or price <= 0:
            continue

        put_prem += vol * price * 100.0
        put_vol  += vol

        # Volume-delta interval classification.
        ckey = _contract_key(
            p.get("symbol"), sym, primary_exp,
            _sf(p.get("strike")), "put",
        )
        prev = _CONTRACT_SNAPSHOTS.get(ckey)
        if prev is None or vol < prev["volume"]:
            _CONTRACT_SNAPSHOTS[ckey] = {
                "volume": vol, "observed_at": now, "expiry": primary_exp,
            }
        else:
            delta = vol - prev["volume"]
            if delta > 0:
                d_dollars = delta * price * 100.0
                side = _classify_side(b, a, l)
                if side == "ask_side":
                    int_ask += d_dollars
                elif side == "bid_side":
                    int_bid += d_dollars
                else:
                    int_mid += d_dollars
                int_new_vol += delta
                p_at = prev["observed_at"]
                if int_started_at is None or p_at < int_started_at:
                    int_started_at = p_at
            _CONTRACT_SNAPSHOTS[ckey] = {
                "volume": vol, "observed_at": now, "expiry": primary_exp,
            }

    total_vol = call_vol + put_vol
    net_prem  = call_prem - put_prem

    # ── supplement_v2: explicit P/C ratios ────────────────────────────────────
    # premium_put_call_ratio = put_prem / call_prem  (dollar flow direction)
    # volume_put_call_ratio  = put_vol  / call_vol   (contract count direction)
    # put_call_ratio is preserved as a backward-compat alias = premium P/C
    premium_pcr = round(put_prem / call_prem, 3) if call_prem > 0 else None
    volume_pcr  = round(put_vol  / call_vol,  3) if call_vol  > 0 else None
    pcr = premium_pcr  # backward-compat alias (keep existing field name)

    # ── supplement_v2: IV ────────────────────────────────────────────────────
    call_iv   = round(sum(call_iv_vals) / len(call_iv_vals), 4) if call_iv_vals else None
    put_iv    = round(sum(put_iv_vals)  / len(put_iv_vals),  4) if put_iv_vals  else None
    if call_iv is not None and put_iv is not None:
        combined_iv = round((call_iv + put_iv) / 2.0, 4)
        iv_skew     = round(put_iv - call_iv, 4)
    elif call_iv is not None:
        combined_iv, iv_skew = call_iv, None
    elif put_iv is not None:
        combined_iv, iv_skew = put_iv, None
    else:
        combined_iv = iv_skew = None

    # ── supplement_v2: underlying price (caller-supplied → master screener) ──
    spot = _sf(underlying_price) if underlying_price is not None else None
    if spot is not None and spot <= 0:
        spot = None
    _spot_source: Optional[str] = None
    if spot is not None:
        _spot_source = "canonical_quote_cache"
        _underlying_price_status = "canonical_quote_cache"
    else:
        # Fallback: master screener in-memory cache (covers tickers already
        # in options_master_screener_v1 — avoids a Tradier quote call).
        try:
            from data.cache import cache as _mc2
            _ms = _mc2.get("options_master_screener_v1") or _mc2.get("options_master_lkg_v1")
            if _ms:
                for _mr in (_ms.get("tickers") or []):
                    if (_mr.get("ticker") or "").upper() == sym:
                        _p = _sf(_mr.get("underlying_price") or _mr.get("price"))
                        if _p and _p > 0:
                            spot = _p
                            _spot_source = "master_screener_cache"
                            _underlying_price_status = "master_screener_cache"
                        break
        except Exception:
            pass
        if spot is None:
            _underlying_price_status = "not_found"

    # ── supplement_v2: expected move (ATM straddle) ───────────────────────────
    em_dict: Optional[dict] = None
    if spot and spot > 0:
        try:
            from data.chain_score_helper import estimate_expected_move as _em
            em_dict = _em(spot, calls, puts)
        except Exception:
            pass

    em_dollars = em_dict.get("expected_move_dollars") if em_dict else None
    em_pct     = em_dict.get("expected_move_pct")     if em_dict else None
    em_strike  = em_dict.get("atm_strike")            if em_dict else None

    if spot is None:
        _em_status = "no_underlying_price"
    elif em_pct is not None:
        _em_status = "calculated"
    else:
        _em_status = "no_valid_atm_straddle"

    # ── supplement_v2: chain scoring ─────────────────────────────────────────
    _score_result: dict = {}
    if spot and spot > 0:
        try:
            from data.chain_score_helper import score_chain_summary as _scs
            _score_result = _scs(sym, spot, calls, puts, expiration=primary_exp)
        except Exception:
            pass

    options_score      = _score_result.get("options_score")
    options_signal     = _score_result.get("options_signal")
    score_components   = _score_result.get("score_components") or {}
    score_method       = _score_result.get("score_method") or "chain_summary_v1"
    contracts_scored   = _score_result.get("contracts_scored") or 0

    if spot is None:
        _score_status = "no_underlying_price"
    elif options_score is not None:
        _score_status = "chain_summary_scored"
    else:
        _score_status = "chain_empty_or_no_contracts"

    _unavail_reason: Optional[str] = (
        "no_underlying_price" if spot is None else None
    )

    # ── Interval trade-side percentages ───────────────────────────────────────
    # Derived from summed delta dollars only — never per-contract averages.
    # All interval_* fields are None when no delta volume was classified.
    int_total    = int_ask + int_bid + int_mid
    has_interval = int_total > 0

    if has_interval:
        int_ask_pct = round(int_ask / int_total * 100, 1)
        int_bid_pct = round(int_bid / int_total * 100, 1)
        int_mid_pct = round(int_mid / int_total * 100, 1)
        int_cls_pct = round((int_ask + int_bid) / int_total * 100, 1)
        int_secs    = (
            round(now - int_started_at)
            if int_started_at is not None else None
        )
    else:
        int_ask_pct = int_bid_pct = int_mid_pct = int_cls_pct = None
        int_secs    = None

    return {
        "ticker":           sym,
        # ── Cumulative session metrics ─────────────────────────────────────────
        "call_premium":     round(call_prem, 2),
        "put_premium":      round(put_prem, 2),
        "net_premium":      round(net_prem, 2),
        # premium = total dollar flow (for backward compat with existing aggregators)
        "premium":          round(call_prem + put_prem, 2),
        "call_volume":      call_vol,
        "put_volume":       put_vol,
        "total_volume":     total_vol,
        # ── P/C ratios — explicit, unambiguous (supplement_v2) ─────────────────
        # premium_put_call_ratio: put dollar flow / call dollar flow
        # volume_put_call_ratio:  put contract count / call contract count
        # put_call_ratio:         backward-compat alias = premium_put_call_ratio
        "premium_put_call_ratio": premium_pcr,
        "volume_put_call_ratio":  volume_pcr,
        "put_call_ratio":         pcr,
        # ── Open interest (supplement_v2) ──────────────────────────────────────
        "call_oi":          call_oi,
        "put_oi":           put_oi,
        "total_oi":         call_oi + put_oi,
        # ── IV (supplement_v2) ─────────────────────────────────────────────────
        "call_iv":          call_iv,
        "put_iv":           put_iv,
        "combined_iv":      combined_iv,
        "iv_skew":          iv_skew,
        # ── Expected move (supplement_v2, requires underlying_price) ───────────
        "expected_move_dollars":  em_dollars,
        "expected_move_pct":      em_pct,
        "expected_move_atm_strike": em_strike,
        "underlying_price":        spot,
        "underlying_price_source": _spot_source,
        "underlying_price_status": _underlying_price_status,
        # ── Chain score (supplement_v2, requires underlying_price) ─────────────
        "options_score":           options_score,
        "options_signal":          options_signal,
        "score_components":        score_components,
        "score_method":            score_method,
        "contracts_scored":        contracts_scored,
        "options_score_version":   "chain_summary_v1",
        "options_score_source":    "chain_summary_v1" if options_score is not None else None,
        "options_score_status":    _score_status,
        # ── Expected move status ───────────────────────────────────────────────
        "expected_move_status":           _em_status,
        "rich_metrics_unavailable_reason": _unavail_reason,
        # schema version — downstream consumers can gate on this
        "supplement_schema_version": "supplement_v2",
        # ── Incremental interval trade-side classification ─────────────────────
        # Based on volume_delta = current_snapshot_volume − prior_snapshot_volume.
        # Only NEW contracts observed since the last scan cycle are classified.
        # The prior cumulative session volume is never assigned to a side.
        #
        # All interval_* fields are None when:
        #   • First scan for this symbol post-restart (no prior snapshots).
        #   • All contracts had identical volume to the prior snapshot.
        #   • A session reset was detected for every contract.
        #
        # interval_ask_premium  — delta premium where last >= ask − $0.02
        # interval_bid_premium  — delta premium where last <= bid + $0.02
        # interval_midpoint_unknown_premium — remainder (last between bid/ask,
        #   or bid/ask/last missing/invalid)
        # interval_total_premium  — ask + bid + mid (equals sum of the above)
        # interval_new_contract_volume — delta contract count classified
        # interval_classified_trade_side_pct — (ask+bid)/total, coverage signal
        # interval_started_at — observed_at of the earliest prior snapshot used
        # interval_ended_at   — timestamp of this scan (= now)
        # interval_seconds    — interval_ended_at − interval_started_at
        "interval_ask_premium":                  round(int_ask,   2) if has_interval else None,
        "interval_bid_premium":                  round(int_bid,   2) if has_interval else None,
        "interval_midpoint_unknown_premium":     round(int_mid,   2) if has_interval else None,
        "interval_total_premium":                round(int_total, 2) if has_interval else None,
        "interval_new_contract_volume":          int_new_vol if has_interval else None,
        "interval_ask_premium_pct":              int_ask_pct,
        "interval_bid_premium_pct":              int_bid_pct,
        "interval_midpoint_unknown_premium_pct": int_mid_pct,
        "interval_classified_trade_side_pct":    int_cls_pct,
        "interval_seconds":                      int_secs,
        "interval_started_at":                   int_started_at if has_interval else None,
        "interval_ended_at":                     now if has_interval else None,
        "scan_result":      "sectors_chain_summarized",
        "expiration_used":  primary_exp,
        "updated_at":       now,
        "source":           "sectors_direct",
    }


# ── batch scanner ─────────────────────────────────────────────────────────────

async def scan_batch_for_sectors(
    symbols: list[str],
    tradier,
    expiry_cache: dict,
    *,
    concurrency: int = 6,
    underlying_prices: Optional[dict] = None,
) -> list[dict]:
    """
    Scan a list of symbols concurrently (up to *concurrency* at a time).

    The caller must wrap this in the appropriate lane() context so the
    Tradier budget system charges the correct lane.

    underlying_prices — optional {SYMBOL: float} map pre-built by the caller
    from existing in-memory quote caches (zero Tradier calls).  When supplied,
    each ticker's price is passed directly into summarize_ticker_chain() so
    Expected Move and Score/Signal are computed without a separate quote fetch.
    Build with get_cached_underlying_prices(batch) before calling this.

    Example::

        with lane("sectors"):
            prices = get_cached_underlying_prices(batch)
            results = await scan_batch_for_sectors(
                batch, tradier, expiry_cache, underlying_prices=prices
            )

    Returns a list of result dicts — one per symbol, in order.
    """
    sem = asyncio.Semaphore(concurrency)
    _prices = underlying_prices or {}

    async def _one(sym: str) -> dict:
        async with sem:
            return await summarize_ticker_chain(
                sym,
                tradier,
                expiry_cache,
                underlying_price=_prices.get(sym.upper()),
            )

    raw = await asyncio.gather(*[_one(s) for s in symbols], return_exceptions=True)
    out: list[dict] = []
    for sym, r in zip(symbols, raw):
        if isinstance(r, Exception):
            out.append({
                "ticker":      sym.upper(),
                "scan_result": "deferred_retry",
                "error":       str(r)[:200],
                "updated_at":  time.time(),
            })
        else:
            out.append(r)
    return out
