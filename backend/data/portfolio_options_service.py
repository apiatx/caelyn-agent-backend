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
import json
from pathlib import Path
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


# ── Portfolio disk LKG (survives restarts + outside-market-hours) ──────────
# Written whenever a live scan produces data_available=True rows.
# Read as a fallback before falling through to live Tradier calls.

_PORTFOLIO_LKG_DISK = Path(__file__).resolve().parent / "portfolio_opts_lkg_v1.json"


def _load_portfolio_lkg() -> dict[str, dict]:
    """Load per-ticker portfolio options LKG from disk. Returns {} on any error."""
    try:
        if _PORTFOLIO_LKG_DISK.exists():
            data = json.loads(_PORTFOLIO_LKG_DISK.read_text())
            if isinstance(data, dict):
                return {k.upper(): v for k, v in data.items() if isinstance(v, dict)}
    except Exception as exc:
        print(f"[PORTFOLIO_OPTS_LKG] load error: {exc}")
    return {}


def _save_portfolio_lkg(results: dict[str, dict]) -> None:
    """Merge data_available=True rows into disk LKG.
    Existing rows for symbols not in current scan are preserved so a partial
    scan doesn't wipe rows for symbols that weren't requested this time."""
    try:
        existing = _load_portfolio_lkg()
        import datetime as _dt
        ts = _dt.datetime.utcnow().isoformat()
        updated = dict(existing)
        for sym, row in results.items():
            if row.get("data_available"):
                updated[sym.upper()] = {**row, "_lkg_saved_at": ts}
        _PORTFOLIO_LKG_DISK.write_text(json.dumps(updated, default=str))
    except Exception as exc:
        print(f"[PORTFOLIO_OPTS_LKG] save error: {exc}")


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

    # Derive call/put volume breakdown — prefer explicit master cache fields,
    # fall back to algebraic derivation from total_vol + pc_ratio.
    call_vol_m = _si(
        row.get("call_volume") or row.get("total_call_volume") or oc.get("call_volume")
    )
    put_vol_m  = _si(
        row.get("put_volume")  or row.get("total_put_volume")  or oc.get("put_volume")
    )
    if call_vol_m == 0 and put_vol_m == 0 and total_vol > 0 and pc is not None:
        denom = 1 + pc
        call_vol_m = round(total_vol / denom) if denom > 0 else 0
        put_vol_m  = total_vol - call_vol_m

    call_oi_m = _si(
        oc.get("call_open_interest") or row.get("call_open_interest")
    )
    put_oi_m  = _si(
        oc.get("put_open_interest")  or row.get("put_open_interest")
    )

    # Confidence driven by volume — mirrors _classify_signal logic
    if total_vol > 2000:
        confidence = "HIGH"
    elif total_vol > 500:
        confidence = "MEDIUM"
    else:
        confidence = "LOW"

    return {
        "ticker":              sym,
        "symbol":              sym,
        "optionable":          True,
        "data_available":      True,
        "score":               round(float(score), 1),
        "p_c":                 round(pc, 3) if pc is not None else None,
        "put_call":            round(pc, 3) if pc is not None else None,
        "iv":                  round(iv_f, 4) if iv_f is not None else None,
        "em":                  em_display,
        "expected_move":       em_display,
        "vol":                 total_vol or None,
        "volume":              total_vol or None,
        "call_volume":         call_vol_m or None,
        "put_volume":          put_vol_m or None,
        "open_interest":       _si(row.get("open_interest")) or None,
        "call_open_interest":  call_oi_m or None,
        "put_open_interest":   put_oi_m or None,
        "signal":              display,
        "put_call_direction":  direction,
        "confidence":          confidence,
        "source":              "portfolio_scoped_options_screener",
        "unavailable_reason":  None,
    }


def _unavail_row(sym: str, reason: str, optionable: bool | None = None) -> dict:
    return {
        "ticker":              sym,
        "symbol":              sym,
        "optionable":          optionable,
        "has_options":         None,
        "has_open_option_position": False,
        "data_available":      False,
        "score":               None,
        "p_c":                 None,
        "put_call":            None,
        "iv":                  None,
        "em":                  None,
        "expected_move":       None,
        "vol":                 None,
        "volume":              None,
        "call_volume":         None,
        "put_volume":          None,
        "open_interest":       None,
        "call_open_interest":  None,
        "put_open_interest":   None,
        "signal":              "NO OPTIONS" if "no_chain" in reason or "not_in_tradier" in reason or "otc" in reason else "NO DATA",
        "put_call_direction":  None,
        "confidence":          None,
        "source":              "portfolio_scoped_options_screener",
        "unavailable_reason":  reason,
    }


def _build_position_fallback_row(sym: str, positions: list[dict]) -> dict:
    """
    Build a partial options-flow row for an open option underlying when the
    chain scan has no result (empty chain, timeout, not in Tradier coverage).

    Uses only position metadata from the database — no Tradier calls.
    "NO OPTIONS" is never used here because the user holds open contracts
    which prove the underlying IS optionable.
    """
    # Sort positions by nearest expiration so the representative contract is
    # the most time-sensitive one.
    sorted_pos = sorted(
        positions,
        key=lambda p: (p.get("expiration_date") or "9999-12-31"),
    )
    rep = sorted_pos[0] if sorted_pos else {}

    all_calls = [p for p in positions if (p.get("option_type") or "").upper() == "CALL"]
    all_puts  = [p for p in positions if (p.get("option_type") or "").upper() == "PUT"]

    if all_calls and not all_puts:
        direction    = "calls"
        signal_label = "CONTRACT DATA ONLY"
    elif all_puts and not all_calls:
        direction    = "puts"
        signal_label = "CONTRACT DATA ONLY"
    else:
        direction    = "neutral"
        signal_label = "POSITION OPEN · FLOW STALE"

    total_contracts = sum(float(p.get("contracts_open") or 0) for p in positions)

    return {
        "ticker":                   sym,
        "symbol":                   sym,
        "optionable":               True,
        "has_options":              True,
        "has_open_option_position": True,
        "data_available":           False,   # no live flow/chain metrics
        "score":                    None,
        "p_c":                      None,
        "put_call":                 None,
        "iv":                       None,
        "em":                       None,
        "expected_move":            None,
        "vol":                      None,
        "volume":                   None,
        "call_volume":              None,
        "put_volume":               None,
        "open_interest":            None,
        "call_open_interest":       None,
        "put_open_interest":        None,
        "signal":                   signal_label,
        "put_call_direction":       direction,
        "confidence":               None,
        "source":                   "portfolio_option_position",
        "unavailable_reason":       "flow_scan_unavailable",
        # Position-level fields for the frontend to render contract details
        "position_contracts":       total_contracts,
        "position_count":           len(positions),
        "position_expiration":      rep.get("expiration_date"),
        "position_strike":          float(rep.get("strike") or 0),
        "position_option_type":     (rep.get("option_type") or "CALL").upper(),
        "position_avg_premium":     float(rep.get("avg_premium") or 0),
        "positions": [
            {
                "occ_key":         p.get("occ_key"),
                "expiration_date": p.get("expiration_date"),
                "strike":          float(p.get("strike") or 0),
                "option_type":     (p.get("option_type") or "").upper(),
                "contracts_open":  float(p.get("contracts_open") or 0),
                "avg_premium":     float(p.get("avg_premium") or 0),
                "cost_basis":      float(p.get("cost_basis") or 0),
            }
            for p in sorted_pos
        ],
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
            call_oi  = 0
            put_oi   = 0
            for ch in chains:
                if isinstance(ch, Exception) or not isinstance(ch, dict):
                    continue
                for c in ch.get("calls", []):
                    calls_all.append(c)
                    _c_oi = _si(c.get("open_interest") or c.get("openInterest"))
                    oi_total += _c_oi
                    call_oi  += _c_oi
                for p in ch.get("puts", []):
                    puts_all.append(p)
                    _p_oi = _si(p.get("open_interest") or p.get("openInterest"))
                    oi_total += _p_oi
                    put_oi   += _p_oi

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
                "ticker":              sym,
                "symbol":              sym,
                "optionable":          True,
                "data_available":      True,
                "score":               score,
                "p_c":                 pc_ratio,
                "put_call":            pc_ratio,
                "iv":                  iv_current,
                "em":                  em_display,
                "expected_move":       em_display,
                "vol":                 total_vol or None,
                "volume":              total_vol or None,
                "call_volume":         call_vol or None,
                "put_volume":          put_vol or None,
                "open_interest":       oi_total or None,
                "call_open_interest":  call_oi or None,
                "put_open_interest":   put_oi or None,
                "signal":              display,
                "put_call_direction":  direction,
                "confidence":          confidence,
                "source":              "portfolio_scoped_options_screener",
                "unavailable_reason":  None,
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
    open_option_positions: "dict[str, list[dict]] | None" = None,
) -> dict:
    """
    Portfolio-scoped options scan — reuses the same scoring / signal / IV /
    expected-move definitions as the master options screener.

    Three-layer cache strategy:
      1. Whole-portfolio cache  portfolio_opts_scan_v1:{sig}   (300s TTL)
      2. Per-ticker cache       portfolio_opts:{sym}           (300s TTL)
      3. Master screener cache rows (scored by TradierFlowEngine — best quality)
      4. Live Tradier scan for tickers not in any cache

    open_option_positions: dict mapping underlying symbol → list of open
      option position dicts (from option_trades_store). When provided, any
      symbol in this map will never be labelled "NO OPTIONS" — a position
      fallback row is built from the DB metadata if the chain scan fails.

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

    # 1.5. Load portfolio-specific disk LKG ────────────────────────────────
    # This file is written whenever a live scan succeeds so that options
    # signals survive restarts and outside-market-hours conditions without
    # falling through to a live Tradier call.
    disk_lkg = _load_portfolio_lkg()

    # 2. Fill from per-ticker cache → master snap → disk LKG ───────────────
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
        elif sym in disk_lkg and disk_lkg[sym].get("data_available"):
            # Disk LKG hit — warm the per-ticker memory cache so subsequent
            # requests in this session avoid disk I/O.
            row = {**disk_lkg[sym], "source": "portfolio_opts_lkg_disk", "from_lkg": True}
            cache.set(per_key, row, _CACHE_PER_TICKER_TTL)
            results[sym] = row
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

    # 4.25. Open option position override ──────────────────────────────────
    # Symbols with open option positions in the DB are KNOWN to be optionable
    # regardless of what the chain scan returned.  We must never label them
    # "NO OPTIONS" — that label is reserved for symbols Tradier confirms have
    # no options chain at all.
    #
    # Priority after this step (highest → lowest):
    #   (A) data_available=True row from live scan / master snap / disk LKG  ← keep as-is
    #   (B) position fallback row built from DB metadata                       ← built here
    #
    # Position fallback rows set data_available=False so the risk engine marks
    # them UNKNOWN, but signal is "CONTRACT DATA ONLY" / "POSITION OPEN · FLOW
    # STALE" — never "NO OPTIONS".
    if open_option_positions:
        _norm_opp: dict[str, list[dict]] = {
            k.upper(): v for k, v in open_option_positions.items()
            if k and v
        }
        for _sym, _positions in _norm_opp.items():
            if _sym not in syms:
                continue
            existing = results.get(_sym, {})
            if existing.get("data_available"):
                # Full flow data present — just tag with position flags
                results[_sym] = {
                    **existing,
                    "has_open_option_position": True,
                    "has_options":              True,
                }
            else:
                # Chain scan failed / no data — build position-based fallback
                fallback = _build_position_fallback_row(_sym, _positions)
                results[_sym] = fallback
                print(
                    f"[PORTFOLIO_OPTIONS_SVC] position-fallback applied "
                    f"sym={_sym}  "
                    f"contracts={fallback.get('position_contracts')}  "
                    f"signal={fallback.get('signal')!r}  "
                    f"prev_reason={existing.get('unavailable_reason')}"
                )

    # 4.5. Portfolio-only pullback-risk enrichment ─────────────────────────
    # Pure post-processing: adds risk_score/level/signal/reasons/confidence
    # to every row. Does NOT touch Options Screener or any global flow.
    results = {sym: _compute_pullback_risk(row) for sym, row in results.items()}

    # 4.6. Persist data_available=True rows to portfolio disk LKG ──────────
    # Only write when we have genuinely new live results (not just rows that
    # came from the disk LKG itself) so we don't churn disk with stale data.
    # "from_lkg" flag is set on rows loaded from disk in step 2; any row
    # without it that is data_available=True came from a live scan or master
    # snap and is worth persisting.
    fresh_available = {
        sym: r
        for sym, r in results.items()
        if r.get("data_available") and not r.get("from_lkg")
    }
    if fresh_available:
        _save_portfolio_lkg(fresh_available)

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


# ── Watchlist options-signal engine ─────────────────────────────────────────
# Reuses the same three-layer cache as scan_portfolio_options but:
#   • No _MAX_SYMBOLS cap on the cache-first lookup pass
#   • Returns immediately with cached/LKG results + stale placeholders for
#     uncached tickers
#   • Enqueues background Tradier scan (batches of _MAX_SYMBOLS = 25) via
#     asyncio.create_task — never blocks the request
#   • Shares portfolio_opts:{sym} cache keys with Portfolio Terminal so a
#     warm Portfolio cache immediately benefits Watchlist (and vice-versa)
#   • Does NOT call _compute_pullback_risk again — cached rows already have
#     risk fields applied by scan_portfolio_options before they were cached

_WL_SIGNAL_FIELD = "options_signal"   # sentinel to detect normalised rows


def _normalize_to_watchlist_row(sym: str, r: dict, is_stale: bool) -> dict:
    """Project an internal options row into the watchlist-signal field shape."""
    return {
        "ticker":                    sym,
        "options_score":             r.get("score"),
        "options_signal":            r.get("signal"),
        "options_put_call_ratio":    r.get("p_c"),
        "options_iv":                r.get("iv"),
        "options_expected_move":     r.get("em"),
        "options_volume":            r.get("vol"),
        "options_open_interest":     r.get("open_interest"),
        "options_call_volume":       r.get("call_volume"),
        "options_put_volume":        r.get("put_volume"),
        "options_updated_at":        r.get("_updated_at") or r.get("_lkg_saved_at"),
        "options_source":            r.get("source"),
        "options_stale":             is_stale,
        "options_unavailable_reason": r.get("unavailable_reason"),
        "options_data_available":    r.get("data_available", False),
        "options_risk_score":        r.get("risk_score"),
        "options_risk_level":        r.get("risk_level"),
        "options_confidence":        r.get("confidence"),
        "options_put_call_direction": r.get("put_call_direction"),
    }


async def scan_watchlist_options(
    symbols: list[str],
    tradier,
    cache,
    master_snap: dict | None = None,
    *,
    max_live_scan: int = 50,
) -> dict:
    """
    Cache-first options signal lookup for all watchlist tickers.

    Returns immediately — never blocks the request on Tradier calls.

    Three-layer cache (shared with Portfolio Terminal):
      1. Per-ticker memory cache  portfolio_opts:{sym}  (300s TTL)
      2. Master screener snapshot (pre-computed by TradierFlowEngine)
      3. Disk LKG portfolio_opts_lkg_v1.json           (survives restarts)

    Uncached tickers:
      • Returned immediately as stale placeholders (options_stale=True,
        options_unavailable_reason="scan_pending")
      • Up to max_live_scan uncached symbols are queued for background scan
        via asyncio.create_task in batches of _MAX_SYMBOLS (25), going
        through the existing Tradier rate-limiter/spacer automatically

    Response:
      {
        "signals":      {ticker: {options_score, options_signal, ...}},
        "options_meta": {scope, symbols_requested, cache_hits, cache_misses,
                         live_calls_enqueued, live_calls_completed,
                         rate_limited_or_deferred, generated_at, ttl_seconds}
      }
    """
    import asyncio as _aio
    import time as _tm
    import datetime as _dt

    _t0 = _tm.monotonic()

    syms = [s.upper() for s in (symbols or []) if s.strip()]
    if not syms:
        return {
            "signals": {},
            "options_meta": {
                "scope":                    "watchlist",
                "symbols_requested":        0,
                "cache_hits":               0,
                "cache_misses":             0,
                "live_calls_enqueued":      0,
                "live_calls_completed":     0,
                "rate_limited_or_deferred": 0,
                "generated_at":             _dt.datetime.utcnow().isoformat() + "Z",
                "ttl_seconds":              _CACHE_PER_TICKER_TTL,
            },
        }

    # 1. Build master-snap lookup (zero network calls — already fetched by caller)
    master_by_ticker: dict[str, dict] = {}
    if master_snap:
        for row in (master_snap.get("tickers") or []):
            t = (row.get("ticker") or "").upper()
            if t:
                master_by_ticker[t] = row

    # 2. Disk LKG (single file read, cached in memory after first access)
    disk_lkg = _load_portfolio_lkg()

    # 3. Cache-first pass — NO _MAX_SYMBOLS cap here
    # In-flight guard: symbols already queued for background scan carry a
    # 90s TTL marker (portfolio_opts_wl_inflight:{sym}).  If present, treat
    # the symbol as scan_pending but do NOT re-enqueue — avoids duplicate
    # Tradier batches when the user reloads before the first scan finishes.
    _INFLIGHT_PFX = "portfolio_opts_wl_inflight:"
    _INFLIGHT_TTL = 90   # seconds — slightly longer than a full batch scan

    results:   dict[str, dict] = {}
    uncached:  list[str]       = []   # truly uncached, needs enqueue
    inflight:  list[str]       = []   # scan already running, skip enqueue
    cache_hits = 0

    for sym in syms:
        per_key = _per_ticker_cache_key(sym)
        hit = cache.get(per_key)
        if hit and isinstance(hit, dict):
            results[sym] = hit
            cache_hits += 1
        elif sym in master_by_ticker:
            norm = _normalize_master_row(sym, master_by_ticker[sym])
            cache.set(per_key, norm, _CACHE_PER_TICKER_TTL)
            results[sym] = norm
            cache_hits += 1
        elif sym in disk_lkg and disk_lkg[sym].get("data_available"):
            row = {**disk_lkg[sym], "source": "portfolio_opts_lkg_disk",
                   "from_lkg": True}
            cache.set(per_key, row, _CACHE_PER_TICKER_TTL)
            results[sym] = row
            cache_hits += 1
        elif cache.get(_INFLIGHT_PFX + sym):
            # Background scan already in progress — return placeholder, skip re-enqueue
            inflight.append(sym)
            results[sym] = _unavail_row(sym, "scan_in_progress")
        else:
            uncached.append(sym)
            results[sym] = _unavail_row(sym, "scan_pending")

    # 4. Background scan for uncached symbols
    _to_scan  = uncached[:max_live_scan]
    _deferred = max(0, len(uncached) - max_live_scan)
    enqueued  = 0

    if _to_scan and tradier:
        enqueued = len(_to_scan)

        # Mark each symbol as in-flight before enqueueing so repeat calls
        # within the scan window don't dispatch duplicate batches
        for _s in _to_scan:
            cache.set(_INFLIGHT_PFX + _s, 1, _INFLIGHT_TTL)

        async def _bg_batch_scan(_batch: list[str]) -> None:
            try:
                await scan_portfolio_options(
                    _batch, tradier, cache, master_snap=master_snap
                )
            except Exception as _bge:
                print(f"[WATCHLIST_OPTIONS_BG] batch scan error ({_batch}): {_bge}")
            finally:
                # Clear in-flight markers so the next page load gets fresh data
                for _s in _batch:
                    try:
                        cache.delete(_INFLIGHT_PFX + _s)
                    except Exception:
                        pass

        # Chunk into _MAX_SYMBOLS batches — scan_portfolio_options has its own cap
        for _i in range(0, len(_to_scan), _MAX_SYMBOLS):
            _batch = _to_scan[_i : _i + _MAX_SYMBOLS]
            _aio.create_task(_bg_batch_scan(_batch))

    # 5. Build normalised signal map
    signals: dict[str, dict] = {}
    for sym in syms:
        r = results[sym]
        is_stale = sym in uncached or sym in inflight or bool(r.get("from_lkg"))
        signals[sym] = _normalize_to_watchlist_row(sym, r, is_stale)

    elapsed_ms = round((_tm.monotonic() - _t0) * 1000, 1)

    return {
        "signals": signals,
        "options_meta": {
            "scope":                      "watchlist",
            "symbols_requested":          len(syms),
            "cache_hits":                 cache_hits,
            "cache_misses":               len(uncached),
            "scan_in_progress":           len(inflight),
            "live_calls_enqueued":        enqueued,
            "live_calls_completed":       0,   # background — not yet done
            "rate_limited_or_deferred":   _deferred,
            "generated_at":               _dt.datetime.utcnow().isoformat() + "Z",
            "ttl_seconds":                _CACHE_PER_TICKER_TTL,
            "elapsed_ms":                 elapsed_ms,
        },
    }
