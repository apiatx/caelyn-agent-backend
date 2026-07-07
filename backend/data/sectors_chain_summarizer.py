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

# How long to reuse a cached expiration list before refetching from Tradier.
# Exchange-listed expirations change slowly (new monthly series added ~quarterly,
# new weekly series added weekly on Thursday evening), so 12 h gives a good
# balance: stale lists are refreshed at least twice a day without creating extra
# Tradier load on every scan.
_EXPIRY_CACHE_TTL = 12 * 3600   # 12 hours

# Absolute dollar tolerance for last-price bid/ask side classification.
# A contract whose last traded price falls within _SIDE_TOL of the ask
# (or bid) is classified as ask-side (or bid-side).  Contracts between
# the two boundaries are left as midpoint_or_unknown.
_SIDE_TOL = 0.02


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


def _classify_side(bid: Optional[float], ask: Optional[float], last: Optional[float]) -> str:
    """
    Classify a single contract trade as ask-side, bid-side, or midpoint/unknown.

    Uses the last traded price relative to the current bid/ask spread:
      last >= ask - _SIDE_TOL  → "ask_side"   (traded at/near the offer)
      last <= bid + _SIDE_TOL  → "bid_side"   (traded at/near the bid)
      otherwise                → "midpoint_or_unknown"

    Never classifies by option type — calls are not automatically ask-side,
    puts are not automatically bid-side.

    Returns "midpoint_or_unknown" whenever last, bid, or ask are missing/invalid.
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
        from data.tradier_market_session import is_active_session as _is_act
        if not (_bgt.FORCE_ENFORCE or _is_act()):
            return True   # budget not enforced off-hours
        return _bgt.check_budget(_bgt.get_current_lane())
    except Exception:
        return True   # default: proceed


async def summarize_ticker_chain(
    sym: str,
    tradier,
    expiry_cache: dict,
) -> dict:
    """
    Fetch expirations + primary chain for *sym*, then summarize ALL contracts.

    Returns a dict with:
      ticker, call_premium, put_premium, net_premium,
      call_volume, put_volume, total_volume,
      put_call_ratio, scan_result, expiration_used, updated_at

    Special scan_result values:
      "sectors_chain_summarized"  — chain fetched and summarized (real data)
      "confirmed_no_options"      — Tradier returned no expirations (empty list,
                                    ONLY set when we had budget headroom before
                                    the call, reducing false no-options risk)
      "deferred_retry"            — budget exhausted or Tradier call failed;
                                    retry next cycle

    Budget safety
    -------------
    TradierProvider._get() returns None when the lane budget is exceeded.
    This propagates as [] from get_option_expirations(), which would be
    indistinguishable from a genuine "no options" result.  We add a
    pre-check: if the lane has no headroom, we return deferred_retry
    immediately without calling Tradier.  After the call, if we get []
    back but the budget is now exhausted, we return deferred_retry rather
    than confirmed_no_options.
    """
    sym = sym.upper()
    now = time.time()

    # ── 1. Get expirations (cache-first, _EXPIRY_CACHE_TTL) ───────────────────
    #
    # Cache format: {sym: ([exp_date_strings, ...], checked_at_float)}
    #
    # TTL logic:
    #   FRESH  (age < TTL)  → use cached list, skip fetch entirely.
    #   STALE  (age >= TTL) → refetch; on failure fall back to stale list.
    #   MISS   (no entry)   → fetch unconditionally.
    #
    # Stale-fallback rules:
    #   - Exception during fetch AND cached list is non-empty → use old list,
    #     log warning, leave checked_at unchanged so next scan retries sooner.
    #   - Budget-deferral post-check (empty result + no budget) AND cached list
    #     is non-empty → same stale fallback.
    #   - If cached list is empty (confirmed_no_options at prior scan) OR missing
    #     → return deferred_retry as before; do not fabricate a no-options result.
    expirations: Optional[list[str]] = None
    cached = expiry_cache.get(sym)
    if cached and isinstance(cached, (list, tuple)) and len(cached) >= 2:
        _cached_exps, _cached_at = cached[0], cached[1]
        if now - _cached_at < _EXPIRY_CACHE_TTL:
            expirations = _cached_exps  # within TTL — skip Tradier call

    if expirations is None:
        # Pre-check budget before any Tradier call.
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
            # Fetch failed — fall back to stale list if one exists and is non-empty.
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
            # `raw` was successfully fetched.  Apply budget post-check.
            if not raw and not _budget_ok():
                # Empty result while budget exhausted → budget deferral.
                # Fall back to stale list rather than marking confirmed_no_options.
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
                # Successful fetch (possibly empty = confirmed_no_options later).
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
    # For each contract we:
    #   a. compute estimated premium using _best_price (mid → last → bid → ask)
    #   b. classify the trade side using last relative to bid/ask
    #
    # Side buckets accumulate the same dollar amounts as the net-premium totals
    # so ask_prem + bid_prem + mid_prem == call_prem + put_prem (within rounding).
    call_prem = 0.0
    put_prem  = 0.0
    call_vol  = 0
    put_vol   = 0
    ask_prem  = 0.0   # contracts traded at/near the ask
    bid_prem  = 0.0   # contracts traded at/near the bid
    mid_prem  = 0.0   # midpoint or unclassifiable

    for c in calls:
        vol = _si(c.get("volume"))
        if vol <= 0:
            continue
        b, a, l = _sf(c.get("bid")), _sf(c.get("ask")), _sf(c.get("last"))
        price = _best_price(b, a, l)
        if price is not None and price > 0:
            dollars     = vol * price * 100.0
            call_prem  += dollars
            call_vol   += vol
            side = _classify_side(b, a, l)
            if side == "ask_side":
                ask_prem += dollars
            elif side == "bid_side":
                bid_prem += dollars
            else:
                mid_prem += dollars

    for p in puts:
        vol = _si(p.get("volume"))
        if vol <= 0:
            continue
        b, a, l = _sf(p.get("bid")), _sf(p.get("ask")), _sf(p.get("last"))
        price = _best_price(b, a, l)
        if price is not None and price > 0:
            dollars    = vol * price * 100.0
            put_prem  += dollars
            put_vol   += vol
            side = _classify_side(b, a, l)
            if side == "ask_side":
                ask_prem += dollars
            elif side == "bid_side":
                bid_prem += dollars
            else:
                mid_prem += dollars

    total_vol = call_vol + put_vol
    net_prem  = call_prem - put_prem
    pcr       = round(put_prem / call_prem, 3) if call_prem > 0 else None

    # Side-classification percentages.
    # Denominator = ask_prem + bid_prem + mid_prem (== total estimated premium).
    # classified_trade_side_pct = (ask + bid) / total — tells consumers what
    # fraction of the premium was definitively classified (not midpoint/unknown).
    _side_scope = ask_prem + bid_prem + mid_prem
    if _side_scope > 0:
        _ask_pct = round(ask_prem / _side_scope * 100, 1)
        _bid_pct = round(bid_prem / _side_scope * 100, 1)
        _mid_pct = round(mid_prem / _side_scope * 100, 1)
        _cls_pct = round((ask_prem + bid_prem) / _side_scope * 100, 1)
    else:
        _ask_pct = _bid_pct = _mid_pct = _cls_pct = None

    return {
        "ticker":           sym,
        "call_premium":     round(call_prem, 2),
        "put_premium":      round(put_prem, 2),
        "net_premium":      round(net_prem, 2),
        # premium = total dollar flow (for backward compat with existing aggregators)
        "premium":          round(call_prem + put_prem, 2),
        "call_volume":      call_vol,
        "put_volume":       put_vol,
        "total_volume":     total_vol,
        "put_call_ratio":   pcr,
        # ── Ask/Bid side classification ───────────────────────────────────────
        # Derived from last vs bid/ask per contract — zero extra Tradier calls.
        # ask_premium: total estimated premium where last >= ask - $0.02
        # bid_premium: total estimated premium where last <= bid + $0.02
        # midpoint_unknown_premium: remainder (last between bid and ask, or
        #   last/bid/ask fields absent/invalid for that contract)
        # classified_trade_side_pct: (ask+bid) / (ask+bid+mid) — coverage signal
        #   telling consumers what fraction of premium was definitively classified.
        "ask_premium":                  round(ask_prem, 2),
        "bid_premium":                  round(bid_prem, 2),
        "midpoint_unknown_premium":     round(mid_prem, 2),
        "classified_premium":           round(ask_prem + bid_prem, 2),
        "ask_premium_pct":              _ask_pct,
        "bid_premium_pct":              _bid_pct,
        "midpoint_unknown_premium_pct": _mid_pct,
        "classified_trade_side_pct":    _cls_pct,
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
) -> list[dict]:
    """
    Scan a list of symbols concurrently (up to *concurrency* at a time).

    The caller must wrap this in the appropriate lane() context so the
    Tradier budget system charges the correct lane.

    Example::

        with lane("sectors"):
            results = await scan_batch_for_sectors(batch, tradier, expiry_cache)

    Returns a list of result dicts — one per symbol, in order.
    """
    sem = asyncio.Semaphore(concurrency)

    async def _one(sym: str) -> dict:
        async with sem:
            return await summarize_ticker_chain(sym, tradier, expiry_cache)

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
