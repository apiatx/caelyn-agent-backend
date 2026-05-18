"""
portfolio_options_service.py

Reusable async function: scan_portfolio_options()

Reuses the exact scoring / signal / IV / expected-move logic as the master
options screener (/api/options/screener) but scoped to a custom ticker list.

Called by:
  - /api/portfolio/options  (HTTP endpoint in main.py)
  - CaelynTerminalProvider  (terminal _build in caelyn_terminal.py)
"""
from __future__ import annotations

import asyncio
from typing import Any

# ── Signal display mapping (matches Options Screener page) ─────────────────
_SIGNAL_DISPLAY: dict[str, str] = {
    "unusual_call_flow":    "UNUSUAL CALL FLOW",
    "unusual_put_flow":     "UNUSUAL PUT FLOW",
    "gamma_squeeze":        "GAMMA SQUEEZE",
    "asymmetric_risk":      "ASYMMETRIC RISK",
    "volatility_expansion": "VOL EXPANSION",
    "high_iv":              "HIGH IV",
    "options_activity":     "OPTIONS ACTIVITY",
    "low_activity":         "LOW ACTIVITY",
    "unusual_flow":         "UNUSUAL FLOW",
}

_CACHE_PER_TICKER_TTL  = 300   # 5 min per-ticker result
_CACHE_PORTFOLIO_TTL   = 300   # 5 min whole-portfolio scan result
_MAX_SYMBOLS           = 25    # safety cap
_SCAN_SEM              = 6     # max concurrent Tradier chain call batches
_SCAN_TIMEOUT_QUOTE    = 8.0
_SCAN_TIMEOUT_EXP      = 4.0
_SCAN_TIMEOUT_CHAIN    = 5.0


# ── Helpers ────────────────────────────────────────────────────────────────

def _portfolio_scan_cache_key(holdings_sig: str | None) -> str:
    return f"portfolio_opts_scan_v2:{holdings_sig or 'unknown'}"  # v2: risk fields


def _per_ticker_cache_key(sym: str) -> str:
    return f"portfolio_opts:{sym}"


def _sf(v: Any) -> float | None:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _si(v: Any) -> int:
    try:
        return int(v or 0)
    except (TypeError, ValueError):
        return 0


def _classify_signal(
    total_vol: int,
    pc_ratio: float | None,
    iv_current: float | None,
) -> tuple[str, str, str, str]:
    """Returns (raw_key, display_signal, put_call_direction, confidence)."""
    if total_vol > 5000:
        if pc_ratio is not None and pc_ratio < 0.5:
            raw, direction = "unusual_call_flow", "calls"
        elif pc_ratio is not None and pc_ratio > 2.0:
            raw, direction = "unusual_put_flow", "puts"
        elif iv_current and iv_current > 0.65:
            raw, direction = "high_iv", "neutral"
        else:
            raw, direction = "options_activity", "neutral"
        confidence = "medium"
    else:
        raw, direction = "low_activity", "neutral"
        confidence = "low"
    display = _SIGNAL_DISPLAY.get(raw, raw.upper().replace("_", " "))
    return raw, display, direction, confidence


def _composite_score(
    total_vol: int,
    iv_current: float | None,
    pc_ratio: float | None,
) -> float:
    """Exact same formula as existing /api/portfolio/options _scan_one."""
    vol_score = min(35, (total_vol / 10000) * 20) if total_vol > 0 else 0
    iv_score  = min(20, (iv_current or 0) * 40)
    dir_score = min(20, abs((pc_ratio or 1.0) - 1.0) * 15) if pc_ratio else 0
    return round(vol_score + iv_score + dir_score, 1)


def _normalize_master_row(sym: str, row: dict) -> dict:
    """
    Convert a master screener cache row (full TradierFlowEngine output) into
    the same frontend-ready format as a live _scan_one_symbol result.
    """
    oc     = row.get("options_context") or {}
    iv_raw = oc.get("iv_current") or row.get("avg_call_iv") or row.get("avg_put_iv")
    em_raw = oc.get("expected_move_from_atm_straddle")
    if isinstance(em_raw, dict):
        em = em_raw.get("value") or em_raw.get("move_pct") or em_raw.get("expected_move")
    else:
        em = em_raw

    iv_f = _sf(iv_raw)
    em_f = _sf(em)
    em_display = round(em_f * 100, 2) if em_f is not None else None

    score     = _sf(row.get("final_composite_score") or row.get("composite_score")) or 0.0
    pc        = _sf(row.get("pc_ratio"))
    total_vol = _si(row.get("total_volume"))

    raw_signal = (row.get("primary_signal") or "").lower().replace(" ", "_")
    display    = _SIGNAL_DISPLAY.get(raw_signal, raw_signal.upper().replace("_", " "))
    if pc is not None and pc < 0.5:
        direction = "calls"
    elif pc is not None and pc > 2.0:
        direction = "puts"
    else:
        direction = "neutral"

    return {
        "ticker":             sym,
        "symbol":             sym,
        "optionable":         True,
        "data_available":     True,
        "score":              round(float(score), 1),
        "p_c":                round(pc, 3) if pc is not None else None,
        "put_call":           round(pc, 3) if pc is not None else None,
        "iv":                 round(iv_f, 4) if iv_f is not None else None,
        "em":                 em_display,
        "expected_move":      em_display,
        "vol":                total_vol or None,
        "volume":             total_vol or None,
        "open_interest":      _si(row.get("open_interest")) or None,
        "signal":             display,
        "put_call_direction": direction,
        "source":             "portfolio_scoped_options_screener",
        "unavailable_reason": None,
    }


def _unavail_row(sym: str, reason: str, optionable: bool | None = None) -> dict:
    return {
        "ticker":             sym,
        "symbol":             sym,
        "optionable":         optionable,
        "data_available":     False,
        "score":              None,
        "p_c":                None,
        "put_call":           None,
        "iv":                 None,
        "em":                 None,
        "expected_move":      None,
        "vol":                None,
        "volume":             None,
        "open_interest":      None,
        "signal":             None,
        "source":             "portfolio_scoped_options_screener",
        "unavailable_reason": reason,
    }


def _compute_pullback_risk(row: dict) -> dict:
    """
    Portfolio-only pullback-risk signal — deterministic composite 0-100.
    Pure function: only adds new keys (risk_score, risk_level, risk_signal,
    risk_reasons, risk_confidence, risk_source). Never modifies existing fields.

    Scoring:
      Put/call pressure : max 35 pts
      IV stress         : max 25 pts
      Expected move     : max 20 pts
      Signal text       : max 15 pts / -5 offset for call-dominant low score
    Levels: HIGH >=75 | ELEVATED >=50 | WATCH >=25 | LOW <25 | UNKNOWN no data
    """
    _SOURCE = "portfolio_options_risk_v1"

    if not row.get("data_available"):
        reason = row.get("unavailable_reason") or "no options data"
        return {
            **row,
            "risk_score":      None,
            "risk_level":      "UNKNOWN",
            "risk_signal":     "UNKNOWN",
            "risk_reasons":    [f"No options data: {reason}"],
            "risk_confidence": "LOW",
            "risk_source":     _SOURCE,
        }

    pc  = _sf(row.get("p_c") or row.get("put_call"))
    iv  = _sf(row.get("iv"))                            # decimal: 0.65 = 65%
    em  = _sf(row.get("em") or row.get("expected_move")) # already pct: 10.5 = 10.5%
    vol = _si(row.get("vol") or row.get("volume") or 0)
    sig = (row.get("signal") or "").upper()

    score   = 0.0
    reasons: list[str] = []

    # 1. Put/call pressure (max 35 pts)
    if pc is not None:
        if pc >= 2.0:
            score += 35; reasons.append("Put/call ratio above 2.0")
        elif pc >= 1.5:
            score += 28; reasons.append("Put/call ratio above 1.5")
        elif pc >= 1.0:
            score += 20; reasons.append("Put/call ratio above 1.0")
        elif pc >= 0.7:
            score += 10; reasons.append("Put/call ratio above 0.7")

    # 2. IV stress (max 25 pts) — iv stored as decimal, threshold as pct
    if iv is not None:
        iv_pct = iv * 100
        if iv_pct >= 175:
            score += 25; reasons.append(f"IV above 175% ({iv_pct:.0f}%)")
        elif iv_pct >= 125:
            score += 18; reasons.append(f"IV above 125% ({iv_pct:.0f}%)")
        elif iv_pct >= 90:
            score += 10; reasons.append(f"IV above 90% ({iv_pct:.0f}%)")

    # 3. Expected move (max 20 pts) — em already in display-pct
    if em is not None:
        if em >= 20:
            score += 20; reasons.append(f"Expected move above 20% ({em:.1f}%)")
        elif em >= 15:
            score += 15; reasons.append(f"Expected move above 15% ({em:.1f}%)")
        elif em >= 10:
            score += 10; reasons.append(f"Expected move above 10% ({em:.1f}%)")
        elif em >= 7:
            score += 5;  reasons.append(f"Expected move above 7% ({em:.1f}%)")

    # 4. Signal text adjustment (max +15, min -5 on low-score rows)
    low_activity = False
    if "PUT" in sig or "BEARISH" in sig:
        score += 15; reasons.append("Unusual put flow detected")
    elif "HIGH IV" in sig:
        score += 8;  reasons.append("High IV signal detected")
    elif "LOW ACTIVITY" in sig:
        low_activity = True
        reasons.append("Low options volume; lower confidence signal")

    if "UNUSUAL CALL FLOW" in sig and score < 50:
        score = max(0.0, score - 5)
        reasons.append("Call flow dominant; bearish risk offset applied")

    # 5. Confidence — driven by volume and activity flag
    if vol < 500 or low_activity:
        confidence = "LOW"
        if vol < 500:
            reasons.append(f"Thin options volume ({vol:,}); signal lower confidence")
    elif vol < 2000:
        confidence = "MEDIUM"
    else:
        confidence = "HIGH"

    score = round(min(100.0, max(0.0, score)), 1)

    if score >= 75:
        level = "HIGH"
    elif score >= 50:
        level = "ELEVATED"
    elif score >= 25:
        level = "WATCH"
    else:
        level = "LOW"

    return {
        **row,
        "risk_score":      score,
        "risk_level":      level,
        "risk_signal":     level,
        "risk_reasons":    reasons,
        "risk_confidence": confidence,
        "risk_source":     _SOURCE,
    }


def _is_otc_or_foreign(sym: str) -> bool:
    """
    Heuristic: OTC/foreign tickers often end in 'F' (foreign) or
    have 5+ chars with unusual suffixes (SIVEF, SLOIF, IQEPF, FPLSF).
    Tradier response is the authoritative check — this only pre-classifies.
    """
    if len(sym) >= 5 and sym[-1] in ("F", "K", "Y"):
        return True
    return False


# ── Per-ticker live scan ───────────────────────────────────────────────────

async def _scan_one_symbol(
    sym: str,
    price: float,
    tradier,
    sem: asyncio.Semaphore,
) -> dict:
    """
    Live options scan for one ticker — same logic as existing _scan_one in
    /api/portfolio/options (main.py).  Returns a full frontend-ready row dict
    or a sentinel dict with _reason set.
    """
    async with sem:
        try:
            expirations = await asyncio.wait_for(
                tradier.get_option_expirations(sym), timeout=_SCAN_TIMEOUT_EXP
            )
            if not expirations:
                return {"_sym": sym, "_reason": "no_chain_returned"}

            chain_tasks = [
                asyncio.wait_for(
                    tradier.get_option_chain(sym, exp), timeout=_SCAN_TIMEOUT_CHAIN
                )
                for exp in expirations[:2]
            ]
            chains = await asyncio.gather(*chain_tasks, return_exceptions=True)

            calls_all, puts_all = [], []
            oi_total = 0
            for ch in chains:
                if isinstance(ch, Exception) or not isinstance(ch, dict):
                    continue
                for c in ch.get("calls", []):
                    calls_all.append(c)
                    oi_total += _si(c.get("open_interest") or c.get("openInterest"))
                for p in ch.get("puts", []):
                    puts_all.append(p)
                    oi_total += _si(p.get("open_interest") or p.get("openInterest"))

            if not calls_all and not puts_all:
                return {"_sym": sym, "_reason": "no_chain_returned"}

            call_vol  = sum(_si(c.get("volume")) for c in calls_all)
            put_vol   = sum(_si(c.get("volume")) for c in puts_all)
            total_vol = call_vol + put_vol
            pc_ratio  = round(put_vol / call_vol, 3) if call_vol else None

            # IV: prefer smv_vol (smoothed from ORATS), fall back to iv field
            iv_vals = []
            for c in (calls_all + puts_all)[:20]:
                iv = c.get("smv_vol") or c.get("iv")
                if iv:
                    iv_vals.append(float(iv))
            iv_current = round(sum(iv_vals) / len(iv_vals), 4) if iv_vals else None

            # ATM straddle expected move (front expiration only)
            expected_move_raw = None
            first_chain = chains[0] if chains and not isinstance(chains[0], Exception) else {}
            fc_calls = first_chain.get("calls", []) if isinstance(first_chain, dict) else []
            fc_puts  = first_chain.get("puts",  []) if isinstance(first_chain, dict) else []
            atm_c = min(fc_calls, key=lambda c: abs((c.get("strike") or 0) - price), default=None) if fc_calls else None
            atm_p = min(fc_puts,  key=lambda c: abs((c.get("strike") or 0) - price), default=None) if fc_puts  else None
            if atm_c and atm_p and price > 0:
                c_mid = ((atm_c.get("bid") or 0) + (atm_c.get("ask") or 0)) / 2
                p_mid = ((atm_p.get("bid") or 0) + (atm_p.get("ask") or 0)) / 2
                if c_mid + p_mid > 0:
                    expected_move_raw = (c_mid + p_mid) / price

            _, display, direction, confidence = _classify_signal(total_vol, pc_ratio, iv_current)
            score = _composite_score(total_vol, iv_current, pc_ratio)

            # em expressed as percentage (e.g. 4.0 = 4% expected move)
            em_display = round(expected_move_raw * 100, 2) if expected_move_raw is not None else None

            return {
                "ticker":             sym,
                "symbol":             sym,
                "optionable":         True,
                "data_available":     True,
                "score":              score,
                "p_c":                pc_ratio,
                "put_call":           pc_ratio,
                "iv":                 iv_current,
                "em":                 em_display,
                "expected_move":      em_display,
                "vol":                total_vol or None,
                "volume":             total_vol or None,
                "open_interest":      oi_total or None,
                "signal":             display,
                "put_call_direction": direction,
                "confidence":         confidence,
                "source":             "portfolio_scoped_options_screener",
                "unavailable_reason": None,
            }

        except asyncio.TimeoutError:
            return {"_sym": sym, "_reason": "provider_rate_limited"}
        except Exception as exc:
            return {"_sym": sym, "_reason": f"unknown_provider_error:{type(exc).__name__}"}


# ── Main entry point ───────────────────────────────────────────────────────

async def scan_portfolio_options(
    symbols: list[str],
    tradier,
    cache,
    master_snap: dict | None = None,
    holdings_sig: str | None = None,
) -> dict:
    """
    Portfolio-scoped options scan — reuses the same scoring / signal / IV /
    expected-move definitions as the master options screener.

    Three-layer cache strategy:
      1. Whole-portfolio cache  portfolio_opts_scan_v1:{sig}   (300s TTL)
      2. Per-ticker cache       portfolio_opts:{sym}           (300s TTL)
      3. Master screener cache rows (scored by TradierFlowEngine — best quality)
      4. Live Tradier scan for tickers not in any cache

    Args:
      symbols:      list of ticker strings for the current portfolio
      tradier:      Tradier provider instance (may be None → cache-only)
      cache:        in-memory cache store (get/set interface)
      master_snap:  optional pre-fetched master screener snapshot
      holdings_sig: stable hash of current holdings (used as whole-portfolio cache key)

    Returns dict:
      by_symbol, rows, available_count, unavailable_count,
      unavailable_reasons_by_symbol, cache_hit, provider_calls,
      source, options_cache_status
    """
    # 0. Whole-portfolio cache ──────────────────────────────────────────────
    _scan_key = _portfolio_scan_cache_key(holdings_sig)
    if holdings_sig:
        cached_scan = cache.get(_scan_key)
        if cached_scan and isinstance(cached_scan, dict):
            cached_scan = dict(cached_scan)   # shallow copy
            cached_scan["cache_hit"]    = True
            cached_scan["cache_source"] = "portfolio_scan"
            return cached_scan

    syms = [s.upper() for s in (symbols or []) if s.strip()][:_MAX_SYMBOLS]
    if not syms:
        return {
            "by_symbol": {}, "rows": [], "available_count": 0,
            "unavailable_count": 0, "unavailable_reasons_by_symbol": {},
            "cache_hit": False, "provider_calls": 0,
            "source": "portfolio_scoped_options_screener",
            "options_cache_status": "empty_universe",
        }

    # 1. Build master-snap lookup ───────────────────────────────────────────
    master_by_ticker: dict[str, dict] = {}
    if master_snap:
        for row in (master_snap.get("tickers") or []):
            t = (row.get("ticker") or "").upper()
            if t:
                master_by_ticker[t] = row

    # 2. Fill from per-ticker cache + master snap ───────────────────────────
    results:  dict[str, dict] = {}
    uncached: list[str]       = []
    provider_calls = 0

    for sym in syms:
        per_key = _per_ticker_cache_key(sym)
        hit = cache.get(per_key)
        if hit and isinstance(hit, dict):
            results[sym] = hit
        elif sym in master_by_ticker:
            norm = _normalize_master_row(sym, master_by_ticker[sym])
            cache.set(per_key, norm, _CACHE_PER_TICKER_TTL)
            results[sym] = norm
        else:
            uncached.append(sym)

    # 3. Live scan for uncached tickers ─────────────────────────────────────
    if uncached and tradier:
        # 3a. Batch quote fetch (1 Tradier call for all uncached symbols)
        try:
            raw_quotes = await asyncio.wait_for(
                tradier.get_quotes(uncached), timeout=_SCAN_TIMEOUT_QUOTE
            )
            provider_calls += 1
        except Exception as qe:
            print(f"[PORTFOLIO_OPTIONS_SVC] batch quote error: {qe}")
            raw_quotes = []

        quote_map: dict[str, dict] = {}
        for q in (raw_quotes or []):
            s = (q.get("symbol") or "").upper()
            if s:
                quote_map[s] = q

        # 3b. Concurrent chain scans (semaphore-gated)
        sem = asyncio.Semaphore(_SCAN_SEM)

        async def _do_scan(sym: str) -> tuple[str, dict]:
            nonlocal provider_calls
            q     = quote_map.get(sym, {})
            price = float(q.get("last") or 0)
            if price <= 0:
                reason = "otc_or_foreign_unsupported" if _is_otc_or_foreign(sym) else "not_in_tradier_coverage"
                return sym, {"_sym": sym, "_reason": reason}
            res = await _scan_one_symbol(sym, price, tradier, sem)
            provider_calls += 3  # ~1 expirations + 2 chains per ticker
            return sym, res

        scan_results = await asyncio.gather(
            *[_do_scan(s) for s in uncached], return_exceptions=True
        )

        for item in scan_results:
            if isinstance(item, Exception):
                continue
            sym, res = item
            if res.get("data_available"):
                # Full successful row
                cache.set(_per_ticker_cache_key(sym), res, _CACHE_PER_TICKER_TTL)
                results[sym] = res
            else:
                reason = res.get("_reason") or res.get("unavailable_reason") or "unknown_provider_error"
                otc = _is_otc_or_foreign(sym) or "otc" in reason or "not_in_tradier" in reason
                row = _unavail_row(sym, reason, optionable=None if otc else False)
                results[sym] = row

    elif uncached:
        # tradier unavailable — mark all uncached tickers
        for sym in uncached:
            results[sym] = _unavail_row(sym, "options_provider_unavailable")

    # 4. Ensure every requested sym has an entry ───────────────────────────
    for sym in syms:
        if sym not in results:
            results[sym] = _unavail_row(sym, "scan_not_reached")

    # 4.5. Portfolio-only pullback-risk enrichment ─────────────────────────
    # Pure post-processing: adds risk_score/level/signal/reasons/confidence
    # to every row. Does NOT touch Options Screener or any global flow.
    results = {sym: _compute_pullback_risk(row) for sym, row in results.items()}

    # 5. Build final output structure ──────────────────────────────────────
    available   = [r for r in results.values() if r.get("data_available")]
    unavailable = [r for r in results.values() if not r.get("data_available")]
    unavail_reasons = {
        r.get("ticker", r.get("symbol", "?")): r.get("unavailable_reason", "unknown")
        for r in unavailable
    }

    sorted_rows = sorted(available, key=lambda r: -(r.get("score") or 0))

    output = {
        "by_symbol":                     results,
        "rows":                          sorted_rows,
        "available_count":               len(available),
        "unavailable_count":             len(unavailable),
        "unavailable_reasons_by_symbol": unavail_reasons,
        "cache_hit":                     False,
        "provider_calls":                provider_calls,
        "source":                        "portfolio_scoped_options_screener",
        "options_cache_status":          "live_scan" if uncached else "all_cached",
    }

    # 6. Cache whole-portfolio result ──────────────────────────────────────
    if holdings_sig:
        cache.set(_scan_key, output, _CACHE_PORTFOLIO_TTL)

    return output
