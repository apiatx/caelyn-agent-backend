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
_STALE_LKG_REFRESH_AGE = 3600  # LKG rows older than 1h are queued for background re-scan
_UNAVAIL_TTL_CONFIRMED  = 86400  # 24h: OTC, not in coverage, confirmed no-options
_UNAVAIL_TTL_TRANSIENT  = 1800   # 30min: rate-limit, empty chain (transient)
_UNAVAIL_TTL_PARTIAL    = 120    # 2min:  quote batch miss for standard US ticker
_QUOTE_RETRY_BATCH_SZ   = 15    # symbols per quote-retry batch
_QUOTE_RETRY_SLEEP_SEC  = 20    # seconds between quote-retry batches
_QUOTE_RETRY_MAX_ATT    = 3     # max retry attempts per symbol per cycle

# ── Freshness SLA thresholds ──────────────────────────────────────────────────
_SLA_FRESH_AGE_S = 900    # 15 min  — target freshness boundary
_SLA_WARN_AGE_S  = 3600   # 60 min  — SLA breach threshold during market hours


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

    Existing rows for symbols not in the current scan are preserved so a partial
    scan doesn't wipe rows for symbols that weren't requested this time.

    Prior-session capture: any row that contains interval_ask_premium /
    interval_bid_premium / interval_midpoint_unknown_premium (populated by the
    TradierFlowEngine master path) has those values also written to
    prior_session_* fields so they survive market close and server restarts.
    After the session ends, _normalize_to_watchlist_row() surfaces these as
    the last completed session's ask/bid/midpoint classification.
    """
    try:
        existing = _load_portfolio_lkg()
        import datetime as _dt
        ts       = _dt.datetime.utcnow().isoformat()
        date_str = ts[:10]
        updated  = dict(existing)
        _INT_TO_PS = [
            ("interval_ask_premium",                  "prior_session_ask_premium"),
            ("interval_bid_premium",                  "prior_session_bid_premium"),
            ("interval_midpoint_unknown_premium",     "prior_session_midpoint_premium"),
            ("interval_ask_premium_pct",              "prior_session_ask_premium_pct"),
            ("interval_bid_premium_pct",              "prior_session_bid_premium_pct"),
            ("interval_midpoint_unknown_premium_pct", "prior_session_midpoint_premium_pct"),
        ]
        for sym, row in results.items():
            if row.get("data_available"):
                row_to_save = {**row, "_lkg_saved_at": ts}
                _has_ps = False
                for _int_f, _ps_f in _INT_TO_PS:
                    _v = row.get(_int_f)
                    if _v is not None:
                        row_to_save[_ps_f] = _v
                        _has_ps = True
                if _has_ps:
                    row_to_save["prior_session_date"]     = date_str
                    row_to_save["prior_session_saved_at"] = ts
                updated[sym.upper()] = row_to_save
        _PORTFOLIO_LKG_DISK.write_text(json.dumps(updated, default=str))
    except Exception as exc:
        print(f"[PORTFOLIO_OPTS_LKG] save error: {exc}")


# ── LKG age / classification helpers ──────────────────────────────────────

def _lkg_age_seconds(row: dict) -> float | None:
    """Return how many seconds old a disk-LKG row is, or None if no timestamp."""
    import datetime as _dt2
    ts = row.get("_lkg_saved_at")
    if not ts:
        return None
    try:
        saved = _dt2.datetime.fromisoformat(str(ts).rstrip("Z"))
        return (_dt2.datetime.utcnow() - saved).total_seconds()
    except Exception:
        return None


def _is_stale_lkg(row: dict) -> bool:
    """True when the LKG row has no saved_at timestamp OR is older than _STALE_LKG_REFRESH_AGE."""
    age = _lkg_age_seconds(row)
    return age is None or age > _STALE_LKG_REFRESH_AGE


def _is_market_hours_et() -> bool:
    """True only during the regular US options session (09:30–16:00 ET, Mon–Fri, no holidays).
    Delegates to the canonical is_regular_options_session() gate.
    Conservative: returns True if the gate check fails (safe default: allow scan)."""
    try:
        from data.tradier_market_session import is_regular_options_session
        return is_regular_options_session()
    except Exception:
        return True


def _compute_syms_hash(syms_iter) -> str:
    """8-char hex hash of the sorted symbol set — changes whenever tickers are added/removed."""
    import hashlib
    return hashlib.sha256(",".join(sorted(syms_iter)).encode()).hexdigest()[:8]


def _classify_watchlist_sym(r: dict, is_stale: bool) -> str:
    """
    Return one of the eight canonical classification labels for a watchlist symbol.
      fresh_option_data        — live/master data, within TTL
      stale_option_data        — data_available row but from old LKG disk
      confirmed_no_options     — Tradier confirmed no options chain (no_expirations)
      unsupported_foreign_or_otc — OTC/foreign, not covered by Tradier
      transient_failure        — temporary: rate-limit, empty chain, unknown error
      queued_for_refresh       — scan_pending or recently enqueued
      inflight_refresh         — background scan currently running
      needs_retry              — transient failure but still inside short-TTL suppression
    """
    if r.get("data_available"):
        return "stale_option_data" if is_stale else "fresh_option_data"
    reason = r.get("unavailable_reason") or ""
    if reason in ("no_options", "no_expirations"):
        return "confirmed_no_options"
    if reason == "otc_or_foreign_unsupported":
        return "unsupported_foreign_or_otc"
    if reason in ("not_in_tradier_coverage", "quote_batch_partial_or_missing"):
        return "transient_failure"
    if reason == "scan_in_progress":
        return "inflight_refresh"
    if reason in ("scan_pending", "stale_lkg_queued"):
        return "queued_for_refresh"
    if reason in ("provider_rate_limited", "no_chain_returned", "unknown_provider_error") or \
            reason.startswith("unknown_provider_error:"):
        return "transient_failure"
    return "needs_retry"


# ── Module-level refresh tracking ──────────────────────────────────────────
import time as _wl_time
_WL_LAST_REFRESH_STARTED_AT:    float | None = None
_WL_LAST_REFRESH_COMPLETED_AT:  float | None = None
_WL_LATEST_SUCCESSFUL_REFRESH_AT: float | None = None

# ── Quote-retry state (reset on server restart) ─────────────────────────────
_WL_QUOTE_RETRY_QUEUE:    set[str]       = set()
_WL_QUOTE_RETRY_ATTEMPTS: dict[str, int] = {}
_WL_QUOTE_RETRY_STATS: dict = {
    "queued_total": 0, "attempted": 0, "succeeded": 0, "failed": 0,
}

# ── Per-watchlist symbol-set tracking (add/remove detection) ─────────────────
# Keyed by watchlist_id → frozenset of symbols from the last call.
# Compared on every scan_watchlist_options call to report adds/removes without
# requiring a restart.  Not persisted — resets to {} on server restart.
_WL_PREV_SYMS: dict[str, frozenset[str]] = {}


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


def normalize_expected_move_pct(v: "float | None", unit: str = "unknown") -> "float | None":
    """
    Always returns expected move as **percentage points** (e.g. 4.33 for a 4.33% move).

    Unit contract
    -------------
    "pct"      – value is already percentage points (e.g. 4.33).  Return as-is.
    "fraction" – value is a decimal fraction (e.g. 0.0433 = 4.33%).  Multiply by 100.
    "unknown"  – heuristic: abs(v) < 1.0 → treat as fraction (×100); else → pct already.

    Examples
    --------
      normalize_expected_move_pct(0.0433, "fraction") → 4.33
      normalize_expected_move_pct(4.33,   "pct")      → 4.33
      normalize_expected_move_pct(0.0433, "unknown")  → 4.33   (< 1 → fraction)
      normalize_expected_move_pct(5.42,   "unknown")  → 5.42   (≥ 1 → already pct)

    The frontend always receives percentage points.  Never multiply by 100 again.
    """
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if unit == "pct":
        return round(f, 4)
    if unit == "fraction":
        return round(f * 100.0, 4)
    # "unknown" heuristic — values below 1.0 are almost certainly decimal fractions
    if abs(f) < 1.0:
        return round(f * 100.0, 4)
    return round(f, 4)


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

    This is the CANONICAL projection used by every consumer (Options Flow,
    Watchlist, Portfolio, popup, Confluence, sectors/themes).  All premium,
    volume, OI, IV, Expected Move, score inputs, and interval classification
    fields come from the same master row — no consumer runs its own chain scan
    for a ticker already in the canonical snapshot.

    Score semantics:
      options_score_version    = "tradier_flow_v1"    (master formula)
      score_comparable_across_rows = True             (uniform across master rows)
    Fallback (portfolio_composite_v1 from _scan_one_symbol):
      score_comparable_across_rows = False  (exposed explicitly on fallback rows)
    """
    oc     = row.get("options_context") or {}
    iv_raw = oc.get("iv_current") or row.get("avg_call_iv") or row.get("avg_put_iv")
    em_raw = oc.get("expected_move_from_atm_straddle")
    if isinstance(em_raw, dict):
        # dict from _estimate_expected_move: {"atm_strike", "expected_move_dollars", "expected_move_pct"}
        # expected_move_pct is already in percentage points (e.g. 4.33 = 4.33% move)
        em_display = normalize_expected_move_pct(_sf(em_raw.get("expected_move_pct")), unit="pct")
    else:
        # float from options_context or legacy store — apply unknown-unit heuristic
        em_display = normalize_expected_move_pct(_sf(em_raw))

    iv_f = _sf(iv_raw)

    _raw_score_m = row.get("final_composite_score")
    if _raw_score_m is None:
        _raw_score_m = row.get("composite_score")
    score = _sf(_raw_score_m)  # None stays None; only 0.0 if chain was actually scored zero
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

    # ── Volume breakdown ───────────────────────────────────────────────────────
    # Prefer explicit master fields; derive algebraically from total_vol + pc_ratio
    # as a fallback (avoids losing coverage when the master row omits split counts).
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

    # ── Premium fields (canonical — from TradierFlowEngine / sectors chain) ───
    # call_premium / put_premium = total dollar premium for all contracts scanned
    # net_premium  = call_premium - put_premium  (positive → call-side dominance)
    # call_put_premium_ratio (master field) = call_prem / put_prem  →  invert for
    #   our canonical premium_put_call_ratio = put_prem / call_prem
    call_prem = _sf(row.get("call_premium") or oc.get("call_premium"))
    put_prem  = _sf(row.get("put_premium")  or oc.get("put_premium"))
    net_prem  = _sf(row.get("net_premium")  or oc.get("net_premium"))

    cpr = _sf(row.get("call_put_premium_ratio") or oc.get("call_put_premium_ratio"))
    if put_prem is not None and call_prem is not None and call_prem > 0:
        prem_pc = round(put_prem / call_prem, 4)
    elif cpr is not None and cpr > 0:
        prem_pc = round(1.0 / cpr, 4)
    else:
        prem_pc = None

    # ── Interval classification (ask-side / bid-side / midpoint) ─────────────
    # These come from the sectors_chain_summarizer flow classification and are
    # the most granular premium breakdown available.
    int_ask    = _sf(row.get("interval_ask_premium")                or oc.get("interval_ask_premium"))
    int_bid    = _sf(row.get("interval_bid_premium")                or oc.get("interval_bid_premium"))
    int_mid    = _sf(row.get("interval_midpoint_unknown_premium")    or oc.get("interval_midpoint_unknown_premium"))
    int_ask_p  = _sf(row.get("interval_ask_premium_pct")            or oc.get("interval_ask_premium_pct"))
    int_bid_p  = _sf(row.get("interval_bid_premium_pct")            or oc.get("interval_bid_premium_pct"))
    int_mid_p  = _sf(row.get("interval_midpoint_unknown_premium_pct") or oc.get("interval_midpoint_unknown_premium_pct"))

    # ── Confidence driven by volume (mirrors _classify_signal logic) ──────────
    if total_vol > 2000:
        confidence = "HIGH"
    elif total_vol > 500:
        confidence = "MEDIUM"
    else:
        confidence = "LOW"

    pc_rounded = round(pc, 3) if pc is not None else None
    return {
        "ticker":              sym,
        "symbol":              sym,
        "optionable":          True,
        "data_available":      True,
        "score":               round(float(score), 1) if score is not None else None,
        # ── Legacy P/C aliases (backward compat — do not remove) ──
        "p_c":                 pc_rounded,
        "put_call":            pc_rounded,
        "put_call_ratio":      pc_rounded,
        # ── Canonical P/C names ───────────────────────────────────────────────
        "volume_put_call_ratio":           pc_rounded,
        "premium_put_call_ratio":          prem_pc,
        # ── Premium dollar fields ─────────────────────────────────────────────
        "call_premium":                    call_prem,
        "put_premium":                     put_prem,
        "net_premium":                     net_prem,
        # ── Interval flow classification (ask / bid / midpoint) ───────────────
        "interval_ask_premium":                  int_ask,
        "interval_bid_premium":                  int_bid,
        "interval_midpoint_unknown_premium":     int_mid,
        "interval_ask_premium_pct":              int_ask_p,
        "interval_bid_premium_pct":              int_bid_p,
        "interval_midpoint_unknown_premium_pct": int_mid_p,
        # ── Score metadata ────────────────────────────────────────────────────
        # tradier_flow_v1 is the canonical master formula — comparable across
        # all master rows.  Fallback portfolio_composite_v1 rows explicitly
        # carry score_comparable_across_rows=False (set in _scan_one_symbol).
        "options_score_version":           "tradier_flow_v1",
        "options_score_source":            "options_master_screener",
        "options_score_inputs":            oc or {},
        "score_comparable_across_rows":    True,
        # ── Core metrics ──────────────────────────────────────────────────────
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
        "source":              "options_master_screener",
        "unavailable_reason":  None,
        # ── Canonical parity fields (alignment with options_flow_sectors) ──────
        "expiration_scope":              row.get("expiration_scope"),
        "expiration_used":               row.get("expiration_used"),
        "dte_used":                      row.get("dte_used"),
        "premium_scope_id":              row.get("premium_scope_id"),
        "net_premium_change_1d":         row.get("net_premium_change_1d"),
        "net_premium_change_7d":         row.get("net_premium_change_7d"),
        "net_premium_change_30d":        row.get("net_premium_change_30d"),
        "prior_session_ask_premium":     row.get("prior_session_ask_premium"),
        "prior_session_bid_premium":     row.get("prior_session_bid_premium"),
        "prior_session_midpoint_premium": row.get("prior_session_midpoint_premium"),
        "prior_session_date":            row.get("prior_session_date"),
        "prior_session_saved_at":        row.get("prior_session_saved_at"),
        "snapshot_source":               row.get("source") or row.get("_source"),
        "snapshot_as_of":                row.get("_cached_at") or row.get("_updated_at"),
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


# ═══════════════════════════════════════════════════════════════════════════════
# MULTI-SOURCE JOIN HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _first_non_null(*values):
    """Return first non-None value. Preserves 0, False, and empty string."""
    for v in values:
        if v is not None:
            return v
    return None


def _si_n(v) -> int | None:
    """Safe int — returns None for None input, int otherwise."""
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _classify_row_family(row: dict | None) -> str:
    """
    Detect the schema family of a raw options row.

    master_scored   — full TradierFlowEngine output (score + IV + OI present)
    premium_summary — sectors_chain_summarizer/supplement (premium+volume, no score)
    portfolio_lkg   — portfolio disk LKG (score+IV+OI+vol_pc, no premium dollar fields)
    unavailable     — error / placeholder
    """
    if not row:
        return "unavailable"
    # Supplement rows from get_combined_ticker_data() do NOT set data_available;
    # they are identified by the presence of call_premium/net_premium data.
    _has_prem_data = (
        row.get("call_premium") is not None
        or row.get("net_premium") is not None
        or row.get("call_volume") is not None
    )
    if not row.get("data_available") and not _has_prem_data:
        return "unavailable"
    oc = row.get("options_context") or {}
    has_score = (
        row.get("final_composite_score") is not None
        or row.get("composite_score") is not None
        or (row.get("score") is not None and
            row.get("options_score_version") is not None)
    )
    has_iv = (
        oc.get("iv_current") is not None
        or row.get("avg_call_iv") is not None
        or row.get("avg_put_iv") is not None
        or row.get("iv") is not None
    )
    has_call_prem = row.get("call_premium") is not None
    if has_score and has_iv:
        return "master_scored"
    if has_call_prem and not has_score:
        return "premium_summary"
    if has_score or has_iv or row.get("call_open_interest") is not None:
        return "portfolio_lkg"
    return "unknown"


def _build_snapshot_status(
    row: dict | None,
    is_stale: bool,
    market_hours: bool,
) -> str:
    """
    Return an explicit lifecycle status.  Never returns 'unknown'.

    live             — freshly scanned this session
    prior_session    — from disk LKG, data from last session (market hours)
    lkg_market_closed — from disk LKG, market currently closed
    stale_but_usable — supplement LKG or cached but aged
    stale_long_term  — LKG older than 24 h during market hours
    pending          — scan queued, no data yet
    unavailable      — no data, not pending
    """
    # Supplement rows don't set data_available — treat them as data-available
    # if they carry actual premium or volume data.
    _has_prem_data = (
        row and (
            row.get("call_premium") is not None
            or row.get("net_premium") is not None
            or row.get("call_volume") is not None
        )
    )
    if not row or (not row.get("data_available") and not _has_prem_data):
        reason = (row or {}).get("unavailable_reason") or ""
        if reason in ("scan_pending", "scan_in_progress"):
            return "pending"
        return "unavailable"

    snap_status = row.get("_snapshot_status") or row.get("snapshot_status") or ""
    scan_status = row.get("scan_status") or ""
    source      = row.get("source") or row.get("_source") or ""
    from_lkg    = bool(row.get("from_lkg"))

    if snap_status == "available_live" or scan_status == "live":
        return "live"
    if snap_status == "lkg_market_closed":
        return "lkg_market_closed"
    if from_lkg:
        lkg_age = _lkg_age_seconds(row)
        if lkg_age and lkg_age > 86400:
            return "stale_long_term"
        return "prior_session" if market_hours else "lkg_market_closed"
    if "supplement_lkg" in source or "supplement_lkg" in scan_status:
        return "lkg_market_closed" if not market_hours else "stale_but_usable"
    if "available_cached" in snap_status:
        return "stale_but_usable"
    if not market_hours:
        return "lkg_market_closed"
    return "stale_but_usable" if is_stale else "live"


def _merge_options_sources(
    sym: str,
    primary_row: dict | None,
    supplement_row: dict | None,
    lkg_row: dict | None,
    history_deltas: dict | None,
    is_stale: bool,
    market_hours: bool = True,
) -> dict:
    """
    Non-destructive field-aware join across all available options data layers.

    Field ownership (per spec):
      MASTER (primary_row):
        score, signal, confidence, IV, Expected Move, OI,
        volume P/C, expiration scope/DTE, master fingerprint.
      SUPPLEMENT (supplement_row):
        call/put/net premium, interval classification, premium P/C,
        premium scope ID, call/put/total volume counts (7-60 DTE scope).
      LKG (lkg_row):
        Fallback for score/IV/OI/signal/volume when primary is absent.
        LKG rows contain score+IV+OI+vol_pc but no call/put premium fields.
      HISTORY (history_deltas):
        net_premium_change_1d/7d/30d — exclusively from DB.

    Rules:
      • _first_non_null() is used instead of falsy `or` so real 0 is preserved.
      • Never derive volume P/C from premium values.
      • Never derive premium P/C from contract volume.
      • Different-scope volumes are used in order of scope quality.
    """
    _p = primary_row if (primary_row and primary_row.get("data_available")) else None
    # Supplement rows from get_combined_ticker_data() do NOT set data_available;
    # accept any row that has premium or volume data.
    _s = supplement_row if supplement_row and (
        supplement_row.get("data_available")
        or supplement_row.get("call_premium") is not None
        or supplement_row.get("net_premium") is not None
        or supplement_row.get("call_volume") is not None
    ) else None
    _l = lkg_row if (lkg_row and lkg_row.get("data_available")) else None

    data_available = bool(_p or _s or _l)
    if not data_available:
        return _unavail_row(sym, "scan_pending")

    # Richest available row for provenance metadata
    _prov = _p or _s or _l

    # ── Row family ────────────────────────────────────────────────────────────
    master_row_present = _classify_row_family(_p) == "master_scored"
    oc_p = (_p or {}).get("options_context") or {}
    oc_l = (_l or {}).get("options_context") or {}

    # ── Score / signal / confidence ───────────────────────────────────────────
    # Whether the supplement row was recovered from a saved Neon contract snapshot.
    # When True, its score/signal/iv/em take priority over a stale portfolio LKG
    # score=0.0 — never let an old fake-zero block a recovered chain score.
    _s_recovered = bool((_s or {}).get("recovered_from_neon"))

    raw_score = _first_non_null(
        (_p or {}).get("final_composite_score"),
        (_p or {}).get("composite_score"),
        # Neon-recovered supplement score outranks stale portfolio LKG score=0.0
        (_s or {}).get("options_score") if (not _p and _s_recovered) else None,
        (_l or {}).get("score"),
        (_l or {}).get("options_score"),
        (_l or {}).get("final_composite_score"),
        # supplement_v2: chain-scored rows from sectors_chain_summarizer
        (_s or {}).get("options_score"),
        (_s or {}).get("composite_score"),
    )
    score_f   = _sf(raw_score)
    score_val = round(float(score_f), 1) if score_f is not None else None

    score_version = _first_non_null(
        (_p or {}).get("options_score_version"),
        "tradier_flow_v1" if master_row_present else None,
        (_s or {}).get("options_score_version") if _s_recovered else None,
        (_l or {}).get("options_score_version"),
    )
    score_source = _first_non_null(
        (_p or {}).get("options_score_source"),
        "options_master_screener" if master_row_present else None,
        (_s or {}).get("options_score_source") if _s_recovered else None,
        (_l or {}).get("options_score_source"),
        (_l or {}).get("source"),
    )
    score_status = (
        "master_scored"  if master_row_present
        # Neon-recovered rows report their own provenance status
        else (_s or {}).get("options_score_status") if (not _p and _s_recovered and (_s or {}).get("options_score_status"))
        else "lkg_fallback" if (_l and _l.get("score") is not None)
        else "not_scored_by_master"
    )
    score_unavail_reason = (
        None if master_row_present
        else ("master_row_absent" if not _p else "primary_row_not_scored")
    )

    raw_signal = _first_non_null(
        (_p or {}).get("primary_signal"),
        (_l or {}).get("signal"),
        (_l or {}).get("options_signal"),
        # supplement_v2: chain-scored rows from sectors_chain_summarizer
        (_s or {}).get("options_signal"),
    )
    raw_signal_str = (raw_signal or "").lower().replace(" ", "_")
    signal_display = (
        _SIGNAL_DISPLAY.get(raw_signal_str, raw_signal_str.upper().replace("_", " "))
        if raw_signal_str else None
    )

    # ── IV / Expected Move ────────────────────────────────────────────────────
    iv_raw = _first_non_null(
        oc_p.get("iv_current"),
        (_p or {}).get("avg_call_iv"),
        (_p or {}).get("avg_put_iv"),
        (_l or {}).get("iv"),
        oc_l.get("iv_current"),
        # supplement_v2: IV from chain_score_helper
        (_s or {}).get("combined_iv"),
        (_s or {}).get("call_iv"),
        (_s or {}).get("put_iv"),
    )
    iv_f   = _sf(iv_raw)
    iv_out = round(iv_f, 4) if iv_f is not None else None

    _em_raw = _first_non_null(
        oc_p.get("expected_move_from_atm_straddle"),
        (_p or {}).get("expected_move"),
        (_l or {}).get("em"),
        (_l or {}).get("expected_move"),
    )
    if isinstance(_em_raw, dict):
        # dict from _estimate_expected_move: {"atm_strike", "expected_move_dollars",
        # "expected_move_pct"} — extract the pct key which is in percentage-point form.
        em_out = normalize_expected_move_pct(_sf(_em_raw.get("expected_move_pct")), unit="pct")
    else:
        # float from LKG (stored in pct form by scan path) or master screener raw value.
        # Apply "unknown" heuristic: <1.0 → fraction → ×100; else → already pct.
        em_out = normalize_expected_move_pct(_sf(_em_raw))
    # supplement_v2 fallback: expected_move_pct is explicitly percentage-point form
    if em_out is None:
        _s_em_pct = _sf((_s or {}).get("expected_move_pct"))
        if _s_em_pct is not None:
            em_out = normalize_expected_move_pct(_s_em_pct, unit="pct")

    # ── Volume ────────────────────────────────────────────────────────────────
    # Precedence: master → LKG (same scan, same scope) → supplement (7-60 DTE)
    call_vol = _si_n(_first_non_null(
        (_p or {}).get("call_volume"),
        (_p or {}).get("total_call_volume"),
        oc_p.get("call_volume"),
        (_l or {}).get("call_volume"),
        (_s or {}).get("call_volume"),
    ))
    put_vol = _si_n(_first_non_null(
        (_p or {}).get("put_volume"),
        (_p or {}).get("total_put_volume"),
        oc_p.get("put_volume"),
        (_l or {}).get("put_volume"),
        (_s or {}).get("put_volume"),
    ))
    total_vol = _si_n(_first_non_null(
        (_p or {}).get("total_volume"),
        (_l or {}).get("vol"),
        (_l or {}).get("volume"),
        (_s or {}).get("total_volume"),
    ))

    # Volume P/C: master pc_ratio → LKG p_c/put_call (same scan)
    # Only derive from call/put volumes when no direct pc_ratio available.
    raw_pc = _first_non_null(
        (_p or {}).get("pc_ratio"),
        (_l or {}).get("p_c"),
        (_l or {}).get("put_call"),
        (_l or {}).get("put_call_ratio"),
        (_l or {}).get("volume_put_call_ratio"),
    )
    vol_pc = _sf(raw_pc)
    if vol_pc is None and call_vol is not None and put_vol is not None and call_vol > 0:
        vol_pc = round(put_vol / call_vol, 4)
    elif vol_pc is not None:
        vol_pc = round(vol_pc, 3)

    # Reconstruct call/put from total + pc when direct counts absent
    if call_vol is None and put_vol is None and total_vol and vol_pc is not None:
        denom = 1 + vol_pc
        call_vol = round(total_vol / denom) if denom > 0 else 0
        put_vol  = total_vol - call_vol

    # Direction
    if vol_pc is not None and vol_pc < 0.5:
        direction = "calls"
    elif vol_pc is not None and vol_pc > 2.0:
        direction = "puts"
    else:
        direction = "neutral"

    # Confidence driven by volume
    _conf_vol = total_vol or 0
    confidence = _first_non_null(
        "HIGH"   if _conf_vol > 2000 else None,
        "MEDIUM" if _conf_vol > 500  else None,
        (_l or {}).get("confidence"),
        "LOW",
    )

    # ── OI ────────────────────────────────────────────────────────────────────
    call_oi = _si_n(_first_non_null(
        oc_p.get("call_open_interest"),
        (_p or {}).get("call_open_interest"),
        (_l or {}).get("call_open_interest"),
        (_l or {}).get("call_oi"),
        oc_l.get("call_open_interest"),
        # supplement_v2: OI from sectors_chain_summarizer
        (_s or {}).get("call_oi"),
    ))
    put_oi = _si_n(_first_non_null(
        oc_p.get("put_open_interest"),
        (_p or {}).get("put_open_interest"),
        (_l or {}).get("put_open_interest"),
        (_l or {}).get("put_oi"),
        oc_l.get("put_open_interest"),
        # supplement_v2: OI from sectors_chain_summarizer
        (_s or {}).get("put_oi"),
    ))
    total_oi = _si_n(_first_non_null(
        (_p or {}).get("open_interest"),
        (_l or {}).get("open_interest"),
        # supplement_v2: OI from sectors_chain_summarizer
        (_s or {}).get("total_oi"),
    ))

    # ── Premium fields ────────────────────────────────────────────────────────
    # Master owns premium if present; supplement is the primary fallback
    # (portfolio LKG rows have None for all premium dollar fields).
    call_prem = _sf(_first_non_null(
        (_p or {}).get("call_premium"),
        oc_p.get("call_premium"),
        (_s or {}).get("call_premium"),
        (_l or {}).get("call_premium"),
    ))
    put_prem = _sf(_first_non_null(
        (_p or {}).get("put_premium"),
        oc_p.get("put_premium"),
        (_s or {}).get("put_premium"),
        (_l or {}).get("put_premium"),
    ))
    net_prem = _sf(_first_non_null(
        (_p or {}).get("net_premium"),
        oc_p.get("net_premium"),
        (_s or {}).get("net_premium"),
        (_l or {}).get("net_premium"),
    ))
    # Derive net if call and put are present and net is missing
    if net_prem is None and call_prem is not None and put_prem is not None:
        net_prem = round(call_prem - put_prem, 2)

    # Premium P/C: put_prem / call_prem — never from contract volume
    cpr = _sf(_first_non_null(
        (_p or {}).get("call_put_premium_ratio"),
        oc_p.get("call_put_premium_ratio"),
        (_s or {}).get("call_put_premium_ratio"),
    ))
    if call_prem is not None and put_prem is not None and call_prem > 0:
        prem_pc = round(put_prem / call_prem, 4)
    elif cpr is not None and cpr > 0:
        prem_pc = round(1.0 / cpr, 4)
    else:
        prem_pc = None

    # ── Interval classification (ask / bid / midpoint) ────────────────────────
    int_ask   = _sf(_first_non_null((_p or {}).get("interval_ask_premium"),               oc_p.get("interval_ask_premium"),               (_s or {}).get("interval_ask_premium")))
    int_bid   = _sf(_first_non_null((_p or {}).get("interval_bid_premium"),               oc_p.get("interval_bid_premium"),               (_s or {}).get("interval_bid_premium")))
    int_mid   = _sf(_first_non_null((_p or {}).get("interval_midpoint_unknown_premium"),  oc_p.get("interval_midpoint_unknown_premium"),  (_s or {}).get("interval_midpoint_unknown_premium")))
    int_ask_p = _sf(_first_non_null((_p or {}).get("interval_ask_premium_pct"),           oc_p.get("interval_ask_premium_pct"),           (_s or {}).get("interval_ask_premium_pct")))
    int_bid_p = _sf(_first_non_null((_p or {}).get("interval_bid_premium_pct"),           oc_p.get("interval_bid_premium_pct"),           (_s or {}).get("interval_bid_premium_pct")))
    int_mid_p = _sf(_first_non_null((_p or {}).get("interval_midpoint_unknown_premium_pct"), oc_p.get("interval_midpoint_unknown_premium_pct"), (_s or {}).get("interval_midpoint_unknown_premium_pct")))

    # ── Prior-session premiums ────────────────────────────────────────────────
    _ps_ask  = _sf(_first_non_null((_prov or {}).get("prior_session_ask_premium"),    (_l or {}).get("prior_session_ask_premium"),    (_s or {}).get("prior_session_ask_premium")))
    _ps_bid  = _sf(_first_non_null((_prov or {}).get("prior_session_bid_premium"),    (_l or {}).get("prior_session_bid_premium"),    (_s or {}).get("prior_session_bid_premium")))
    _ps_mid  = _sf(_first_non_null((_prov or {}).get("prior_session_midpoint_premium"), (_l or {}).get("prior_session_midpoint_premium"), (_s or {}).get("prior_session_midpoint_premium")))
    _ps_date = _first_non_null((_prov or {}).get("prior_session_date"),    (_l or {}).get("prior_session_date"),    (_s or {}).get("prior_session_date"))
    _ps_at   = _first_non_null((_prov or {}).get("prior_session_saved_at"), (_l or {}).get("prior_session_saved_at"), (_s or {}).get("prior_session_saved_at"))
    if not market_hours and _ps_ask is None:
        _ps_ask  = int_ask
        _ps_bid  = int_bid
        _ps_mid  = int_mid
        _ps_date = _ps_date or ((_prov or {}).get("_lkg_saved_at") or "")[:10] or None
        _ps_at   = _ps_at or (_prov or {}).get("_lkg_saved_at")

    # ── Expiration / scope metadata ───────────────────────────────────────────
    exp_scope = _first_non_null((_p or {}).get("expiration_scope"), (_s or {}).get("expiration_scope"), (_l or {}).get("expiration_scope"))
    exp_used  = _first_non_null((_p or {}).get("expiration_used"),  (_s or {}).get("expiration_used"),  (_l or {}).get("expiration_used"))
    dte_used  = _first_non_null((_p or {}).get("dte_used"),         (_s or {}).get("dte_used"),         (_l or {}).get("dte_used"))
    scope_id  = _first_non_null((_p or {}).get("premium_scope_id"), (_s or {}).get("premium_scope_id"), (_l or {}).get("premium_scope_id"))

    # ── History deltas (1D/7D/30D from DB) ────────────────────────────────────
    _h = history_deltas or {}
    np_change_1d  = _sf(_first_non_null(_h.get("net_premium_delta_1d"),  (_prov or {}).get("net_premium_change_1d")))
    np_change_7d  = _sf(_first_non_null(_h.get("net_premium_delta_7d"),  (_prov or {}).get("net_premium_change_7d")))
    np_change_30d = _sf(_first_non_null(_h.get("net_premium_delta_30d"), (_prov or {}).get("net_premium_change_30d")))
    hist_status_1d  = "available" if np_change_1d  is not None else ("insufficient_1d_history"  if net_prem is not None else "history_not_ready")
    hist_status_7d  = "available" if np_change_7d  is not None else ("insufficient_7d_history"  if net_prem is not None else "history_not_ready")
    hist_status_30d = "available" if np_change_30d is not None else ("insufficient_30d_history" if net_prem is not None else "history_not_ready")

    # ── Snapshot provenance ───────────────────────────────────────────────────
    snap_status = _build_snapshot_status(_prov, is_stale, market_hours)
    snap_source = _first_non_null(
        (_p or {}).get("source"), (_p or {}).get("_source"),
        (_s or {}).get("_source"),
        (_l or {}).get("source"),
    )
    snap_as_of = _first_non_null(
        (_p or {}).get("_cached_at"), (_p or {}).get("_updated_at"),
        (_s or {}).get("_cached_at"),
        (_l or {}).get("_updated_at"), (_l or {}).get("_lkg_saved_at"),
    )
    import datetime as _mdt
    reg_session_date = _mdt.datetime.utcnow().strftime("%Y-%m-%d")

    vol_scope  = _first_non_null(scope_id, exp_scope, "7_60dte" if _s else None)
    vol_method = "total_chain_contracts" if master_row_present else "sectors_chain_interval"

    return {
        # Availability
        "data_available":          data_available,
        "optionable":              True,
        # Score fields
        "score":                   score_val,
        "options_score":           score_val,
        "options_score_version":   score_version,
        "options_score_source":    score_source,
        "options_score_status":    score_status,
        "options_score_inputs":    oc_p,
        "options_score_unavailable_reason": score_unavail_reason,
        "master_score_row_present": master_row_present,
        "score_comparable_across_rows": master_row_present,
        # Signal
        "signal":                  signal_display,
        "options_signal":          signal_display,
        "confidence":              confidence,
        "put_call_direction":      direction,
        # Volume P/C
        "p_c":                     vol_pc,
        "put_call":                vol_pc,
        "put_call_ratio":          vol_pc,
        "volume_put_call_ratio":   vol_pc,
        # Premium P/C
        "premium_put_call_ratio":  prem_pc,
        # Premium dollar fields
        "call_premium":            call_prem,
        "put_premium":             put_prem,
        "net_premium":             net_prem,
        # Interval classification
        "interval_ask_premium":                  int_ask,
        "interval_bid_premium":                  int_bid,
        "interval_midpoint_unknown_premium":     int_mid,
        "interval_ask_premium_pct":              int_ask_p,
        "interval_bid_premium_pct":              int_bid_p,
        "interval_midpoint_unknown_premium_pct": int_mid_p,
        # Prior-session
        "prior_session_ask_premium":      _ps_ask,
        "prior_session_bid_premium":      _ps_bid,
        "prior_session_midpoint_premium": _ps_mid,
        "prior_session_date":             _ps_date,
        "prior_session_saved_at":         _ps_at,
        # IV / EM
        "iv":              iv_out,
        "em":              em_out,
        "expected_move":   em_out,
        # Volume
        "vol":             total_vol,
        "volume":          total_vol,
        "call_volume":     call_vol,
        "put_volume":      put_vol,
        "options_volume_scope":  vol_scope,
        "options_volume_method": vol_method,
        # OI
        "open_interest":       total_oi,
        "call_open_interest":  call_oi,
        "put_open_interest":   put_oi,
        # History deltas
        "net_premium_change_1d":          np_change_1d,
        "net_premium_change_7d":          np_change_7d,
        "net_premium_change_30d":         np_change_30d,
        "net_premium_history_status_1d":  hist_status_1d,
        "net_premium_history_status_7d":  hist_status_7d,
        "net_premium_history_status_30d": hist_status_30d,
        "net_premium_1d_ago":    _h.get("net_premium_1d_ago"),
        "net_premium_7d_ago":    _h.get("net_premium_7d_ago"),
        "net_premium_30d_ago":   _h.get("net_premium_30d_ago"),
        "net_premium_trend_1d":  _h.get("net_premium_trend_1d"),
        "net_premium_trend_7d":  _h.get("net_premium_trend_7d"),
        "net_premium_trend_30d": _h.get("net_premium_trend_30d"),
        # Expiration / scope
        "expiration_scope":  exp_scope,
        "expiration_used":   exp_used,
        "dte_used":          dte_used,
        "premium_scope_id":  scope_id,
        # Snapshot provenance
        "snapshot_status":        snap_status,
        "snapshot_source":        snap_source,
        "snapshot_as_of":         snap_as_of,
        "regular_session_date":   reg_session_date,
        "scan_status":            snap_status,
        # Legacy compatibility for _normalize_to_watchlist_row
        "source":              snap_source,
        "_source":             snap_source,
        "_snapshot_status":    snap_status,
        "_cached_at":          snap_as_of,
        "_updated_at":         snap_as_of,
        "from_lkg":            bool((_prov or {}).get("from_lkg")),
        "retry_pending":       (_prov or {}).get("retry_pending"),
        "unavailable_reason":  None,
        # ── Neon recovery provenance ──────────────────────────────────────────
        # Passed through from the supplement row so _normalize_to_watchlist_row
        # can expose them to the frontend without another lookup.
        "recovered_from_neon":           (_s or {}).get("recovered_from_neon"),
        "recovery_snapshot_as_of":       (_s or {}).get("recovery_snapshot_as_of"),
        "awaiting_regular_session_scan": (
            (_s or {}).get("options_score_status") == "awaiting_regular_session_scan"
            or bool((_s or {}).get("awaiting_regular_session_scan"))
        ),
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
    Preflight classifier: returns True for symbols that Tradier options APIs
    will never serve — no need to call get_option_expirations/get_option_chain.

    Matches:
      • EXCHANGE:TICKER format  (AIM:ENSI, ASX:AXE, OTC:ATEYY, TSX:FLT, …)
        — the colon is the authoritative signal; any exchange prefix is foreign/OTC
      • 5+ char tickers ending in F/K/Y
        — common OTC pink-sheet suffixes (IQEPF, HGRAF, KRKNF, SIVEF, etc.)
    """
    if ":" in sym:
        return True
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
    # DISABLED — Portfolio/Watchlist no longer call Tradier directly.
    # All options data flows from the canonical master/supplement snapshot.
    # Direct Tradier method calls (get_quotes / get_option_expirations /
    # get_option_chain) have been fully removed from this file.
    # Any remaining call here is a programming error — raise clearly.
    raise RuntimeError(
        f"_scan_one_symbol({sym!r}): direct portfolio Tradier scans removed. "
        "Delegate via add_high_priority_symbols() in options_theme_supplement."
    )


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

    # 2.5. Combined-data fallback (supplement + LKG) for tickers not in master ─
    # get_combined_ticker_data() merges: live master → supplement → supplement LKG
    # → watchlist bridge.  A ticker found there uses the canonical row rather than
    # triggering a redundant independent chain scan.  This prevents Portfolio from
    # fetching data for a ticker that the supplement scanner already has.
    if uncached:
        try:
            from data.options_theme_supplement import get_combined_ticker_data as _gctd
            from services.options_inflight import record_cache_hit as _rch
            _combined = _gctd()
            _still_uncached: list[str] = []
            for sym in uncached:
                _comb = _combined.get(sym)
                if _comb and _comb.get("data_available") is not False:
                    _norm = _normalize_master_row(sym, _comb)
                    cache.set(_per_ticker_cache_key(sym), _norm, _CACHE_PER_TICKER_TTL)
                    results[sym] = _norm
                    _rch("supplement")
                else:
                    _still_uncached.append(sym)
            uncached = _still_uncached
        except Exception:
            pass  # non-fatal — fall through to live scan

    # 3. Delegate uncached symbols to the canonical supplement scanner ────────
    # Portfolio/Watchlist MUST NOT call Tradier directly.  All options data
    # must come from the canonical master/supplement snapshot or durable LKG.
    # During regular session: submit missing symbols to the canonical
    # high-priority queue; the supplement scanner picks them up within 6-10 min.
    # A brief 2 s yield lets a running supplement iteration complete first.
    if uncached:
        _sess_now = "unknown"
        try:
            from data.tradier_market_session import get_session as _get_sess
            _sess_now = _get_sess()
        except Exception:
            pass
        if _sess_now == "regular" and uncached:
            try:
                from data.options_theme_supplement import (
                    add_high_priority_symbols as _add_hi,
                    get_combined_ticker_data  as _gctd_3,
                )
                _us_uncached = [s for s in uncached if ":" not in s]
                if _us_uncached:
                    _add_hi(_us_uncached)
                    print(
                        f"[PORTFOLIO_OPTIONS_SVC] delegated {len(_us_uncached)} "
                        f"symbol(s) to canonical supplement scanner"
                    )
                await asyncio.sleep(2.0)
                _comb3 = _gctd_3()
                _remaining3: list[str] = []
                for _s3 in uncached:
                    _c3 = _comb3.get(_s3)
                    if _c3 and _c3.get("data_available") is not False:
                        _n3 = _normalize_master_row(_s3, _c3)
                        cache.set(_per_ticker_cache_key(_s3), _n3, _CACHE_PER_TICKER_TTL)
                        results[_s3] = _n3
                    else:
                        _remaining3.append(_s3)
                uncached = _remaining3
            except Exception as _delg_err:
                print(f"[PORTFOLIO_OPTIONS_SVC] supplement delegation error: {_delg_err}")

    # 3.5. Still-uncached: serve from disk LKG or unavailable placeholder ────
    # No Tradier calls — stale LKG is always better than an independent scan.
    for sym in list(uncached):
        _dlkg = disk_lkg.get(sym, {})
        if _dlkg.get("data_available"):
            _lkg_row = {**_dlkg, "source": "portfolio_opts_lkg_disk", "from_lkg": True}
            cache.set(_per_ticker_cache_key(sym), _lkg_row, _CACHE_PER_TICKER_TTL)
            results[sym] = _lkg_row
        elif _dlkg:
            results[sym] = {**_dlkg, "source": "portfolio_opts_lkg_disk", "from_lkg": True}
        else:
            results[sym] = _unavail_row(sym, "delegated_pending_canonical_scan")
    uncached = []  # all handled — no further scan path

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

# ── Process-local in-flight registry ─────────────────────────────────────────
# A plain Python set operated inside the asyncio event loop (single-threaded).
# No TTL — a symbol stays registered until the batch's finally block removes it,
# regardless of how long the Tradier rate-limiter queues the calls.
# This replaces the previous 90-second cache-key approach which could expire
# during a long cold scan on a large watchlist.
# Legacy module-level reference kept for any internal references within this file;
# the actual tracking now lives in services.options_inflight (global, cross-module).
# New code should import from services.options_inflight directly.
try:
    from services.options_inflight import (
        is_options_inflight   as _is_opts_inflight,
        claim_many            as _claim_opts_many,
        release_many          as _release_opts_many,
    )
    _GLOBAL_INFLIGHT_AVAILABLE = True
except ImportError:
    _GLOBAL_INFLIGHT_AVAILABLE = False
    _is_opts_inflight  = lambda sym: False          # type: ignore[assignment]
    _claim_opts_many   = lambda syms, scope: (syms, [])  # type: ignore[assignment]
    _release_opts_many = lambda syms, scope: None   # type: ignore[assignment]

_WL_INFLIGHT_SYMS: set[str] = set()  # kept for backward-compat; new code uses global guard


def _normalize_to_watchlist_row(
    sym: str, r: dict, is_stale: bool, market_hours: bool = True
) -> dict:
    """Project an internal options row into the watchlist-signal field shape.

    Both Options Flow and Watchlist/Portfolio endpoint projections read the
    SAME underlying canonical row and apply this projection.  All premium,
    volume, interval-classification, score, and prior-session fields are
    surfaced identically regardless of which page initiated the lookup.

    Prior-session fields:
      When the market is closed, interval_ask/bid/midpoint_premium reflect the
      last completed session (written by _save_portfolio_lkg).  These are also
      explicitly exposed under prior_session_* keys for clarity.
    """
    _lkg_age = _lkg_age_seconds(r) if r.get("from_lkg") else None
    _stale_over_sla = bool(
        _lkg_age is not None and _lkg_age >= _SLA_WARN_AGE_S
        and market_hours and is_stale
    )
    _priority = "high" if _stale_over_sla else ("normal" if is_stale else None)
    # Resolve canonical P/C names from whichever alias is populated
    _pc      = r.get("volume_put_call_ratio") or r.get("p_c") or r.get("put_call") or r.get("put_call_ratio")
    _prem_pc = r.get("premium_put_call_ratio")

    # Prior-session interval premiums: stored by _save_portfolio_lkg when the
    # master path provides interval_* classification.  When market is closed and
    # the row comes from LKG, these represent the last completed session.
    _ps_ask  = r.get("prior_session_ask_premium")
    _ps_bid  = r.get("prior_session_bid_premium")
    _ps_mid  = r.get("prior_session_midpoint_premium")
    _ps_date = r.get("prior_session_date")
    _ps_at   = r.get("prior_session_saved_at")
    # Fallback: when market is closed and row has interval_* from last scan,
    # surface them as prior-session even if the explicit ps_ keys are absent.
    if not market_hours and _ps_ask is None:
        _ps_ask  = r.get("interval_ask_premium")
        _ps_bid  = r.get("interval_bid_premium")
        _ps_mid  = r.get("interval_midpoint_unknown_premium")
        _ps_date = _ps_date or (r.get("_lkg_saved_at") or "")[:10] or None
        _ps_at   = _ps_at   or r.get("_lkg_saved_at")

    return {
        "ticker":                              sym,
        "options_score":                       r.get("score") or r.get("options_score"),
        "options_signal":                      r.get("signal") or r.get("options_signal"),
        # ── Legacy P/C alias (backward compat) ──
        "options_put_call_ratio":              _pc,
        # ── Canonical P/C names ──
        "volume_put_call_ratio":               _pc,
        "premium_put_call_ratio":              _prem_pc,
        # ── Premium dollar fields ─────────────────────────────────────────────
        "options_call_premium":                r.get("call_premium"),
        "options_put_premium":                 r.get("put_premium"),
        "options_net_premium":                 r.get("net_premium"),
        # ── Interval flow classification (ask / bid / midpoint) ───────────────
        "options_interval_ask_premium":                  r.get("interval_ask_premium"),
        "options_interval_bid_premium":                  r.get("interval_bid_premium"),
        "options_interval_midpoint_premium":             r.get("interval_midpoint_unknown_premium"),
        "options_interval_ask_pct":                      r.get("interval_ask_premium_pct"),
        "options_interval_bid_pct":                      r.get("interval_bid_premium_pct"),
        "options_interval_midpoint_pct":                 r.get("interval_midpoint_unknown_premium_pct"),
        # ── Prior-session ask/bid/midpoint (survives restart + market close) ──
        "prior_session_ask_premium":           _ps_ask,
        "prior_session_bid_premium":           _ps_bid,
        "prior_session_midpoint_premium":      _ps_mid,
        "prior_session_date":                  _ps_date,
        "prior_session_saved_at":              _ps_at,
        # ── Score metadata ────────────────────────────────────────────────────
        "options_score_version":               r.get("options_score_version", "portfolio_composite_v1"),
        "options_score_source":                r.get("options_score_source") or r.get("source"),
        "options_score_inputs":                r.get("options_score_inputs"),
        "score_comparable_across_rows":        r.get("score_comparable_across_rows", False),
        # ── Core fields ───────────────────────────────────────────────────────
        "options_iv":                          r.get("iv") or r.get("options_iv"),
        "options_expected_move":               r.get("em") or r.get("expected_move") or r.get("options_expected_move"),
        "options_volume":                      r.get("vol") or r.get("volume") or r.get("options_volume"),
        "options_open_interest":               r.get("open_interest") or r.get("options_open_interest"),
        "options_call_volume":                 r.get("call_volume"),
        "options_put_volume":                  r.get("put_volume"),
        "options_call_open_interest":          r.get("call_open_interest") or r.get("call_oi"),
        "options_put_open_interest":           r.get("put_open_interest") or r.get("put_oi"),
        "options_updated_at":                  r.get("_updated_at") or r.get("_lkg_saved_at"),
        "options_source":                      r.get("source") or r.get("options_source"),
        "options_stale":                       is_stale,
        "options_unavailable_reason":          r.get("unavailable_reason"),
        "options_data_available":              r.get("data_available", False),
        "options_risk_score":                  r.get("risk_score"),
        "options_risk_level":                  r.get("risk_level"),
        "options_confidence":                  r.get("confidence"),
        "options_put_call_direction":          r.get("put_call_direction"),
        "options_classification":              _classify_watchlist_sym(r, is_stale),
        "options_lkg_age_seconds":             _lkg_age,
        "options_last_successful_refresh_at":  r.get("_lkg_saved_at") or r.get("_updated_at"),
        "stale_over_sla":                      _stale_over_sla,
        "retry_pending":                       r.get("retry_pending"),
        "refresh_queued":                      bool(r.get("retry_pending") or r.get("from_lkg")),
        "refresh_priority":                    _priority,
        # ── Canonical scanner provenance (parity with options_flow_sectors) ──
        "scan_status":                         r.get("scan_status") or r.get("_source") or "unknown",
        "ticker_state":                        r.get("ticker_state"),
        "expiration_scope":                    r.get("expiration_scope"),
        "expiration_used":                     r.get("expiration_used"),
        "dte_used":                            r.get("dte_used"),
        "premium_scope_id":                    r.get("premium_scope_id"),
        "net_premium_change_1d":               r.get("net_premium_change_1d"),
        "net_premium_change_7d":               r.get("net_premium_change_7d"),
        "net_premium_change_30d":              r.get("net_premium_change_30d"),
        "snapshot_source":                     r.get("source") or r.get("_source"),
        "snapshot_as_of":                      r.get("snapshot_as_of") or r.get("_cached_at") or r.get("_updated_at"),
        # ── Explicit lifecycle status (never 'unknown') ────────────────────────
        "snapshot_status":                     r.get("snapshot_status") or r.get("_snapshot_status") or (
            "stale_but_usable" if r.get("data_available") else
            "pending" if (r.get("unavailable_reason") or "") in ("scan_pending", "scan_in_progress") else
            "unavailable"
        ),
        "regular_session_date":                r.get("regular_session_date"),
        # ── Score metadata extended ───────────────────────────────────────────
        "options_score_status":                r.get("options_score_status"),
        "options_score_unavailable_reason":    r.get("options_score_unavailable_reason"),
        "master_score_row_present":            r.get("master_score_row_present", False),
        # ── Neon recovery provenance ──────────────────────────────────────────
        "recovered_from_neon":                 r.get("recovered_from_neon"),
        "recovery_snapshot_as_of":             r.get("recovery_snapshot_as_of"),
        "awaiting_regular_session_scan":       r.get("awaiting_regular_session_scan") or False,
        # ── Volume scope metadata ─────────────────────────────────────────────
        "options_volume_scope":                r.get("options_volume_scope"),
        "options_volume_method":               r.get("options_volume_method"),
        # ── Net premium history metadata ──────────────────────────────────────
        "net_premium_history_status_1d":       r.get("net_premium_history_status_1d"),
        "net_premium_history_status_7d":       r.get("net_premium_history_status_7d"),
        "net_premium_history_status_30d":      r.get("net_premium_history_status_30d"),
        "net_premium_trend_1d":                r.get("net_premium_trend_1d"),
        "net_premium_trend_7d":                r.get("net_premium_trend_7d"),
        "net_premium_trend_30d":               r.get("net_premium_trend_30d"),
        "net_premium_1d_ago":                  r.get("net_premium_1d_ago"),
        "net_premium_7d_ago":                  r.get("net_premium_7d_ago"),
        "net_premium_30d_ago":                 r.get("net_premium_30d_ago"),
    }


async def _drain_deferred_watchlist(
    deferred_syms: list[str],
    tradier,
    cache,
    master_snap: dict | None,
) -> None:
    """
    Progressive background drain for symbols deferred beyond max_live_scan.

    Scans in batches of _MAX_SYMBOLS with a 35-second sleep between batches so
    the global Tradier rate-limiter bucket has time to refill after the initial
    burst from the main scan.  Never blocks the HTTP response — always called
    via asyncio.create_task().

    The global in-flight guard ensures that if a concurrent request or the
    supplement loop already claimed a symbol, it is skipped cleanly.
    """
    import asyncio as _aio

    _INTER_BATCH_SLEEP = 35  # seconds between deferred batches

    for i in range(0, len(deferred_syms), _MAX_SYMBOLS):
        batch = deferred_syms[i : i + _MAX_SYMBOLS]

        # Skip symbols already cached since this task was created
        still_needed = [
            s for s in batch if not cache.get(_per_ticker_cache_key(s))
        ]
        if not still_needed:
            if i + _MAX_SYMBOLS < len(deferred_syms):
                await _aio.sleep(5)   # small gap even for cache-hit batches
            continue

        claimed, _blocked = _claim_opts_many(still_needed, "watchlist_drain")
        if not claimed:
            if i + _MAX_SYMBOLS < len(deferred_syms):
                await _aio.sleep(5)
            continue

        try:
            scan_out = await scan_portfolio_options(
                claimed, tradier, cache, master_snap=master_snap
            )
            by_sym = scan_out.get("by_symbol", {}) if isinstance(scan_out, dict) else {}
            for _s, _row in by_sym.items():
                if _row.get("data_available"):
                    continue   # already written by scan_portfolio_options
                _pk = _per_ticker_cache_key(_s)
                if not cache.get(_pk):
                    reason = _row.get("unavailable_reason", "")
                    # Stale-while-revalidate: transient failures must never blank
                    # a row that has prior good disk LKG data.
                    _is_transient_fail = reason in (
                        "provider_rate_limited", "no_chain_returned",
                        "quote_batch_partial_or_missing"
                    ) and ":" not in _s
                    if _is_transient_fail:
                        _prior_lkg = _load_portfolio_lkg().get(_s, {})
                        if _prior_lkg.get("data_available"):
                            cache.set(_pk, {
                                **_prior_lkg,
                                "source":        "portfolio_opts_lkg_disk",
                                "from_lkg":      True,
                                "retry_pending": True,
                                "retry_reason":  reason,
                            }, _CACHE_PER_TICKER_TTL)
                            continue
                    ttl = (
                        _UNAVAIL_TTL_CONFIRMED
                        if reason in ("no_options", "no_expirations",
                                      "otc_or_foreign_unsupported")
                        else _UNAVAIL_TTL_PARTIAL
                        if reason == "quote_batch_partial_or_missing"
                        else _UNAVAIL_TTL_TRANSIENT
                    )
                    cache.set(_pk, _row, ttl)
        except Exception as _e:
            print(f"[WATCHLIST_OPTS_DRAIN] batch {i//25+1} error: {_e}")
        finally:
            _release_opts_many(claimed, "watchlist_drain")

        if i + _MAX_SYMBOLS < len(deferred_syms):
            await _aio.sleep(_INTER_BATCH_SLEEP)


async def _drain_stale_lkg(
    stale_syms: list[str],
    tradier,
    cache,
    master_snap: dict | None,
) -> None:
    """
    Background refresh for stale LKG rows (data_available=True but old timestamp).

    Uses a DIRECT live-scan path — bypasses scan_portfolio_options (which is
    cache-first and would re-serve the stale LKG from disk).  Replicates the
    uncached branch of scan_portfolio_options: quote batch → semaphore-gated
    chain scans → write results to memory cache + disk LKG.
    """
    import asyncio as _aio
    global _WL_LAST_REFRESH_COMPLETED_AT, _WL_LATEST_SUCCESSFUL_REFRESH_AT

    _INTER_BATCH_SLEEP = 35

    for i in range(0, len(stale_syms), _MAX_SYMBOLS):
        batch = stale_syms[i : i + _MAX_SYMBOLS]

        # Skip only if a genuinely fresh (non-LKG) scan result is already present
        still_needed = []
        for s in batch:
            # Never spend Tradier calls on foreign/OTC symbols even in LKG drain
            if _is_otc_or_foreign(s):
                _pk = _per_ticker_cache_key(s)
                if not cache.get(_pk):
                    _fr = _unavail_row(s, "otc_or_foreign_unsupported", optionable=None)
                    cache.set(_pk, _fr, _UNAVAIL_TTL_CONFIRMED)
                continue
            cached = cache.get(_per_ticker_cache_key(s))
            if (cached and isinstance(cached, dict)
                    and cached.get("data_available")
                    and not cached.get("from_lkg")):
                continue  # fresh live result already replaced this LKG row
            still_needed.append(s)

        if not still_needed:
            if i + _MAX_SYMBOLS < len(stale_syms):
                await _aio.sleep(5)
            continue

        claimed, _blocked = _claim_opts_many(still_needed, "watchlist_lkg_refresh")
        if not claimed:
            if i + _MAX_SYMBOLS < len(stale_syms):
                await _aio.sleep(5)
            continue

        try:
            # ── Delegate to canonical supplement scanner (no direct Tradier) ───
            # Stale LKG refresh submits symbols to the high-priority queue and
            # reads back any results the supplement loop already produced.
            # No get_quotes / get_option_expirations / get_option_chain calls.
            try:
                from data.options_theme_supplement import (
                    add_high_priority_symbols as _add_hi_lkg,
                    get_combined_ticker_data  as _gctd_lkg,
                )
                _us_claimed = [s for s in claimed if ":" not in s]
                if _us_claimed:
                    _add_hi_lkg(_us_claimed)
                    print(
                        f"[WATCHLIST_LKG_REFRESH] delegated {len(_us_claimed)} "
                        f"stale-LKG symbol(s) to canonical supplement scanner"
                    )
                await _aio.sleep(1.5)
                _comb_lkg = _gctd_lkg()
                _fresh_available: dict[str, dict] = {}
                for _s in claimed:
                    _c = _comb_lkg.get(_s)
                    if _c and _c.get("data_available") is not False:
                        _pk = _per_ticker_cache_key(_s)
                        _norm = _normalize_master_row(_s, _c)
                        cache.set(_pk, _norm, _CACHE_PER_TICKER_TTL * 2)
                        _fresh_available[_s] = _norm
                if _fresh_available:
                    _save_portfolio_lkg(_fresh_available)
                    _WL_LATEST_SUCCESSFUL_REFRESH_AT = _wl_time.time()
                    print(
                        f"[WATCHLIST_LKG_REFRESH] batch {i // _MAX_SYMBOLS + 1}: "
                        f"{len(_fresh_available)} refreshed from canonical snapshot"
                    )
            except Exception as _dlg_lkg:
                print(f"[WATCHLIST_LKG_REFRESH] delegation error: {_dlg_lkg}")

        except Exception as _e:
            print(f"[WATCHLIST_LKG_REFRESH] batch {i // _MAX_SYMBOLS + 1} error: {_e}")
        finally:
            _release_opts_many(claimed, "watchlist_lkg_refresh")

        if i + _MAX_SYMBOLS < len(stale_syms):
            await _aio.sleep(_INTER_BATCH_SLEEP)

    _WL_LAST_REFRESH_COMPLETED_AT = _wl_time.time()
    print(f"[WATCHLIST_LKG_REFRESH] Complete: {len(stale_syms)} stale LKG symbols processed")


async def _drain_quote_retry(
    syms_to_retry: list[str],
    tradier,
    cache,
    master_snap: dict | None,
) -> None:
    """
    Paced quote-retry background task for standard US tickers that returned no quote
    from the initial burst scan (reason: quote_batch_partial_or_missing).

    Retries in small batches (≤15 symbols, 20s spacing) so the Tradier rate-limiter
    bucket refills between bursts.  Routes through the existing Tradier rate limiter —
    no second limiter or bypass.

    If a quote is found  → immediately scans options chain with real price.
    Still no quote       → refreshes the 120s partial TTL (retried next API call).
    """
    import asyncio as _aio

    _sem = _aio.Semaphore(_SCAN_SEM)

    for i in range(0, len(syms_to_retry), _QUOTE_RETRY_BATCH_SZ):
        _batch = syms_to_retry[i: i + _QUOTE_RETRY_BATCH_SZ]

        # Skip symbols already refreshed since this task was created
        _still_need = [
            s for s in _batch
            if not (cache.get(_per_ticker_cache_key(s)) or {}).get("data_available")
        ]
        if not _still_need:
            if i + _QUOTE_RETRY_BATCH_SZ < len(syms_to_retry):
                await _aio.sleep(5)
            continue

        # ── Delegate to canonical supplement scanner (no direct Tradier) ─────
        # Quote retry no longer calls Tradier independently.  Submit symbols to
        # the canonical high-priority queue; the supplement scanner covers them.
        try:
            from data.options_theme_supplement import (
                add_high_priority_symbols as _add_hi_qr,
                get_combined_ticker_data  as _gctd_qr,
            )
            _us_still = [s for s in _still_need if ":" not in s]
            if _us_still:
                _add_hi_qr(_us_still)
            await _aio.sleep(1.5)
            _comb_qr = _gctd_qr()
            _WL_QUOTE_RETRY_STATS["attempted"] += len(_still_need)
            _resolved = 0
            for _s in _still_need:
                _c_qr = _comb_qr.get(_s)
                if _c_qr and _c_qr.get("data_available") is not False:
                    _pk = _per_ticker_cache_key(_s)
                    _norm_qr = _normalize_master_row(_s, _c_qr)
                    cache.set(_pk, _norm_qr, _CACHE_PER_TICKER_TTL)
                    _save_portfolio_lkg({_s: _norm_qr})
                    _resolved += 1
            _WL_QUOTE_RETRY_STATS["succeeded"] += _resolved
            _WL_QUOTE_RETRY_STATS["failed"]    += len(_still_need) - _resolved
        except Exception as _qr_err:
            print(f"[QUOTE_RETRY] delegation error: {_qr_err}")
            _WL_QUOTE_RETRY_STATS["failed"] += len(_still_need)

        if i + _QUOTE_RETRY_BATCH_SZ < len(syms_to_retry):
            await _aio.sleep(_QUOTE_RETRY_SLEEP_SEC)

    print(
        f"[QUOTE_RETRY] done: {len(syms_to_retry)} symbols | "
        f"succeeded={_WL_QUOTE_RETRY_STATS['succeeded']} "
        f"failed={_WL_QUOTE_RETRY_STATS['failed']}"
    )


async def scan_watchlist_options(
    symbols: list[str],
    tradier,
    cache,
    master_snap: dict | None = None,
    *,
    max_live_scan: int = 50,
    force_refresh: bool = False,
    watchlist_id: str | None = None,
) -> dict:
    """
    Cache-first options signal lookup for all watchlist tickers.

    Returns immediately — never blocks the request on Tradier calls.

    Three-layer cache (shared with Portfolio Terminal):
      1. Per-ticker memory cache  portfolio_opts:{sym}  (300s TTL)
      2. Master screener snapshot (pre-computed by TradierFlowEngine)
      3. Disk LKG portfolio_opts_lkg_v1.json           (survives restarts)

    In-flight guard:
      • Module-level set _WL_INFLIGHT_SYMS holds symbols whose background
        scan is currently queued/running.  No TTL — cleared only in the
        finally block after each batch completes, even if Tradier rate-limiting
        delays the batch by minutes.  Prevents re-enqueueing on repeat loads.

    Uncached tickers (not in any cache and not in-flight):
      • Returned immediately as stale placeholders
      • Up to max_live_scan queued for background scan via asyncio.create_task
        in batches of _MAX_SYMBOLS (25), routed through the existing
        Tradier _TradierRateLimiter — no second limiter created.

    Response:
      {
        "signals":      {ticker: {options_score, options_signal, ...}},
        "options_meta": {scope, symbols_requested, cache_hits, master_hits,
                         lkg_hits, cache_misses, live_calls_enqueued,
                         already_inflight, scan_in_progress,
                         rate_limited_or_deferred, generated_at, ttl_seconds}
      }
    """
    import asyncio as _aio
    import time as _tm
    import datetime as _dt

    _t0 = _tm.monotonic()

    syms = [s.upper() for s in (symbols or []) if s.strip()]

    # ── Per-watchlist symbol-set change tracking ──────────────────────────────
    # Detect adds/removes by comparing current syms against the previous call's
    # set for this watchlist_id.  No restart required — state lives in _WL_PREV_SYMS.
    _syms_set = frozenset(syms)
    _syms_hash = _compute_syms_hash(_syms_set)
    _active_us_count = sum(1 for s in syms if ":" not in s)
    if watchlist_id:
        _prev_syms = _WL_PREV_SYMS.get(watchlist_id)
        _syms_added   = sorted(_syms_set - _prev_syms) if _prev_syms is not None else []
        _syms_removed = sorted(_prev_syms - _syms_set) if _prev_syms is not None else []
        _WL_PREV_SYMS[watchlist_id] = _syms_set
    else:
        _prev_syms = None
        _syms_added = []
        _syms_removed = []

    _empty_meta = {
        "scope":                      "watchlist",
        "symbols_requested":          0,
        "cache_hits":                 0,
        "master_hits":                0,
        "lkg_hits":                   0,
        "cache_misses":               0,
        "live_calls_enqueued":        0,
        "already_inflight":           0,
        "scan_in_progress":           0,
        "rate_limited_or_deferred":   0,
        "generated_at":               _dt.datetime.utcnow().isoformat() + "Z",
        "ttl_seconds":                _CACHE_PER_TICKER_TTL,
    }
    if not syms:
        return {"signals": {}, "options_meta": _empty_meta}

    # 1. Build master-snap lookup (zero network calls — already fetched by caller)
    master_by_ticker: dict[str, dict] = {}
    if master_snap:
        for row in (master_snap.get("tickers") or []):
            t = (row.get("ticker") or "").upper()
            if t:
                master_by_ticker[t] = row

    # 2. Disk LKG (single file read, cached in memory after first access)
    disk_lkg = _load_portfolio_lkg()

    # 2.5 Supplement combined snapshot — premium/volume layer for all tickers.
    # get_combined_ticker_data() is a pure in-memory read (no network calls).
    # Supplement rows carry call/put/net premium, interval classification, and
    # volume counts that the portfolio LKG rows do NOT have.
    try:
        from data.options_theme_supplement import get_combined_ticker_data as _get_combined
        combined_snap: dict[str, dict] = _get_combined()
    except Exception as _cse:
        print(f"[WATCHLIST_OPTIONS] combined_snap load error: {_cse}")
        combined_snap = {}

    # 2.6 Batch Net Premium history — one DB round-trip for all US tickers.
    # Provides 1D/7D/30D delta fields. Non-blocking: any error yields empty dict.
    _hist_by_ticker: dict[str, dict] = {}
    try:
        from data.options_net_premium_history import (
            get_historical_snapshots_bulk as _get_hist_bulk,
            compute_delta_fields          as _compute_delta,
        )
        import datetime as _hdt
        from datetime import timedelta as _htd
        _us_syms_for_hist = [s for s in syms if ":" not in s]
        _hist_since = _hdt.date.today() - _htd(days=31)
        # Query both entity types (stock + etf) in one shot; stock wins on overlap
        _hist_entities = (
            [("stock", s) for s in _us_syms_for_hist]
            + [("etf",   s) for s in _us_syms_for_hist]
        )
        _hist_raw = _get_hist_bulk(_hist_entities, _hist_since)
        _hist_today = _hdt.date.today()
        for _hs in _us_syms_for_hist:
            _rows = _hist_raw.get(("stock", _hs)) or _hist_raw.get(("etf", _hs)) or []
            if _rows:
                # Current net_premium from supplement (most reliable) → disk LKG
                _src = combined_snap.get(_hs) or disk_lkg.get(_hs) or {}
                _curr_np = _sf(_src.get("net_premium"))
                _hist_by_ticker[_hs] = _compute_delta(_curr_np, _rows, _hist_today)
    except Exception as _he:
        print(f"[WATCHLIST_OPTIONS] history batch error: {_he}")

    # 3. Cache-first pass — NO _MAX_SYMBOLS cap
    # Priority: per-ticker memory cache → master snap → disk LKG
    #           → in-flight registry (already running) → truly uncached
    # force_refresh=True evicts stale LKG rows and transient-failure cache entries
    # so they fall through to uncached and get re-queued for a live scan.
    global _WL_LAST_REFRESH_STARTED_AT, _WL_LAST_REFRESH_COMPLETED_AT, _WL_LATEST_SUCCESSFUL_REFRESH_AT

    results:             dict[str, dict] = {}
    uncached:            list[str]       = []   # genuinely cold, needs enqueue
    inflight:            list[str]       = []   # in _WL_INFLIGHT_SYMS, skip enqueue
    stale_lkg_to_refresh: list[str]     = []   # LKG rows old enough to background-refresh
    memory_hits  = 0
    master_hits  = 0
    lkg_hits     = 0

    for sym in syms:
        per_key = _per_ticker_cache_key(sym)

        # ── Foreign/OTC preflight ────────────────────────────────────────────
        # EXCHANGE:TICKER format (AIM:, ASX:, OTC:, TSX:, …) and common OTC
        # suffixes are NEVER served by Tradier options APIs.  Classify them
        # immediately and cache with 24h TTL so no quote/chain calls are spent.
        if _is_otc_or_foreign(sym):
            hit = cache.get(per_key)
            if not (hit and isinstance(hit, dict)):
                hit = _unavail_row(sym, "otc_or_foreign_unsupported", optionable=None)
                cache.set(per_key, hit, _UNAVAIL_TTL_CONFIRMED)
            results[sym] = hit
            memory_hits += 1
            continue

        hit = cache.get(per_key)

        # force_refresh: ignore stale LKG and transient-failure cached rows
        if hit and isinstance(hit, dict) and force_refresh:
            _is_lkg_entry    = hit.get("from_lkg") or hit.get("source") == "portfolio_opts_lkg_disk"
            _is_transient    = hit.get("unavailable_reason") in (
                "provider_rate_limited", "no_chain_returned", "scan_pending",
                "not_in_tradier_coverage", "quote_batch_partial_or_missing"
            )
            if _is_lkg_entry or _is_transient:
                hit = None  # evict → fall through to uncached path

        # Stale-while-revalidate: if memory has a transient failure but disk LKG
        # has prior good data, serve the LKG row (retry_pending=True) — never blank.
        # This handles the case where a background scan wrote a failure to memory
        # after a burst-rate-limit, hiding the fact that prior good data exists.
        if hit and isinstance(hit, dict) and not hit.get("data_available") and ":" not in sym:
            _cached_reason = hit.get("unavailable_reason") or ""
            if _cached_reason in ("provider_rate_limited", "no_chain_returned",
                                  "quote_batch_partial_or_missing"):
                _lkg_candidate = disk_lkg.get(sym, {})
                if _lkg_candidate.get("data_available"):
                    hit = {**_lkg_candidate, "source": "portfolio_opts_lkg_disk",
                           "from_lkg": True, "retry_pending": True,
                           "retry_reason": _cached_reason}
                    cache.set(per_key, hit, _CACHE_PER_TICKER_TTL)
                    # Restored from disk LKG — queue for background refresh if stale
                    if _is_stale_lkg(hit) and sym not in stale_lkg_to_refresh \
                            and not _is_opts_inflight(sym):
                        stale_lkg_to_refresh.append(sym)

        if hit and isinstance(hit, dict):
            results[sym] = hit
            memory_hits += 1
            # Memory hit on a stale LKG row (e.g. from a previous force_refresh that
            # loaded disk LKG into memory cache) — ensure refresh is queued.
            if hit.get("from_lkg") and _is_stale_lkg(hit) \
                    and sym not in stale_lkg_to_refresh \
                    and not _is_opts_inflight(sym):
                stale_lkg_to_refresh.append(sym)
        elif sym in master_by_ticker:
            norm = _normalize_master_row(sym, master_by_ticker[sym])
            cache.set(per_key, norm, _CACHE_PER_TICKER_TTL)
            results[sym] = norm
            master_hits += 1
        elif sym in disk_lkg and disk_lkg[sym].get("data_available"):
            row = {**disk_lkg[sym], "source": "portfolio_opts_lkg_disk",
                   "from_lkg": True}
            cache.set(per_key, row, _CACHE_PER_TICKER_TTL)
            results[sym] = row
            lkg_hits += 1
            # If old, queue background refresh (serves stale row now, fresh later)
            if _is_stale_lkg(row):
                stale_lkg_to_refresh.append(sym)
        elif _is_opts_inflight(sym) or sym in _WL_INFLIGHT_SYMS:
            # Background scan already queued/running (global or module-level guard).
            # Return placeholder; do NOT re-enqueue.
            inflight.append(sym)
            results[sym] = _unavail_row(sym, "scan_in_progress")
        else:
            uncached.append(sym)
            results[sym] = _unavail_row(sym, "scan_pending")

    cache_hits = memory_hits + master_hits + lkg_hits

    # 4. Background scan for uncached symbols
    _to_scan  = uncached[:max_live_scan]
    _deferred = max(0, len(uncached) - max_live_scan)
    enqueued  = 0

    if _to_scan and tradier:
        # Claim symbols through the global guard.  Symbols already claimed by
        # another scope (supplement loop, portfolio scan) are excluded so no
        # duplicate scan fires.
        _claimed, _already_inflight = _claim_opts_many(_to_scan, "watchlist")
        _WL_INFLIGHT_SYMS.update(_claimed)  # backward-compat mirror

        if _already_inflight:
            # Move globally-in-flight symbols to inflight list so they get
            # scan_in_progress placeholder and are not re-enqueued.
            for _s in _already_inflight:
                if _s not in inflight:
                    inflight.append(_s)
                    results[_s] = _unavail_row(_s, "scan_in_progress")

        _to_scan = _claimed  # only scan what we actually claimed
        enqueued = len(_to_scan)
        _WL_LAST_REFRESH_STARTED_AT = _wl_time.time()

        async def _bg_batch_scan(_batch: list[str]) -> None:
            global _WL_LATEST_SUCCESSFUL_REFRESH_AT
            try:
                scan_out = await scan_portfolio_options(
                    _batch, tradier, cache, master_snap=master_snap
                )
                # scan_portfolio_options only caches data_available=True rows.
                # For unavailable rows write a TTL based on permanence:
                #   confirmed (OTC / no coverage / no expirations) → 24h
                #   transient (rate-limit / empty chain)           → 30min
                by_sym = scan_out.get("by_symbol", {}) if isinstance(scan_out, dict) else {}
                _any_fresh = False
                for _s, _row in by_sym.items():
                    if _row.get("data_available"):
                        _any_fresh = True
                        continue   # already cached by scan_portfolio_options
                    _pk = _per_ticker_cache_key(_s)
                    if not cache.get(_pk):
                        reason = _row.get("unavailable_reason", "")
                        # Stale-while-revalidate: transient failures must never blank
                        # a row that has prior good disk LKG data.
                        _is_transient_fail = reason in (
                            "provider_rate_limited", "no_chain_returned",
                            "quote_batch_partial_or_missing"
                        ) and ":" not in _s
                        if _is_transient_fail:
                            _prior_lkg = _load_portfolio_lkg().get(_s, {})
                            if _prior_lkg.get("data_available"):
                                cache.set(_pk, {
                                    **_prior_lkg,
                                    "source":        "portfolio_opts_lkg_disk",
                                    "from_lkg":      True,
                                    "retry_pending": True,
                                    "retry_reason":  reason,
                                }, _CACHE_PER_TICKER_TTL)
                                continue
                        ttl = (
                            _UNAVAIL_TTL_CONFIRMED
                            if reason in ("no_options", "no_expirations",
                                          "otc_or_foreign_unsupported")
                            else _UNAVAIL_TTL_PARTIAL
                            if reason == "quote_batch_partial_or_missing"
                            else _UNAVAIL_TTL_TRANSIENT
                        )
                        cache.set(_pk, _row, ttl)
                # Queue paced retry for transient-failed standard US symbols that
                # have NO prior LKG (symbols WITH LKG are served stale above and
                # retried by _drain_stale_lkg once rate-limiter capacity returns).
                _retry_syms = [
                    _s for _s, _row in by_sym.items()
                    if (_row.get("unavailable_reason") or "") in (
                        "provider_rate_limited", "no_chain_returned",
                        "quote_batch_partial_or_missing"
                    ) and ":" not in _s
                    and not _load_portfolio_lkg().get(_s, {}).get("data_available")
                ]
                if _retry_syms:
                    _WL_QUOTE_RETRY_STATS["queued_total"] += len(_retry_syms)
                    _aio.create_task(
                        _drain_quote_retry(_retry_syms, tradier, cache, master_snap)
                    )
                if _any_fresh:
                    _WL_LATEST_SUCCESSFUL_REFRESH_AT = _wl_time.time()
            except Exception as _bge:
                print(f"[WATCHLIST_OPTIONS_BG] batch scan error ({_batch}): {_bge}")
            finally:
                # Release from both the global guard and legacy module set
                _release_opts_many(_batch, "watchlist")
                for _s in _batch:
                    _WL_INFLIGHT_SYMS.discard(_s)

        # Chunk into _MAX_SYMBOLS batches — scan_portfolio_options caps at 25
        for _i in range(0, len(_to_scan), _MAX_SYMBOLS):
            _batch = _to_scan[_i : _i + _MAX_SYMBOLS]
            _aio.create_task(_bg_batch_scan(_batch))

        # Progressive drain: if more symbols are deferred beyond max_live_scan,
        # kick off a background task that scans them in batches of _MAX_SYMBOLS
        # with a 35-second sleep between batches so the rate-limiter bucket
        # refills before each subsequent burst.
        if _deferred > 0:
            _deferred_syms = uncached[max_live_scan:]
            _aio.create_task(
                _drain_deferred_watchlist(_deferred_syms, tradier, cache, master_snap)
            )
            print(
                f"[WATCHLIST_OPTIONS_DRAIN] Queued progressive drain: "
                f"{len(_deferred_syms)} symbols in "
                f"{-(-len(_deferred_syms) // _MAX_SYMBOLS)} batches"
            )

    # 4b. Background refresh for stale LKG rows (separate from uncached drain)
    _stale_lkg_queued = 0
    if stale_lkg_to_refresh and tradier:
        _aio.create_task(
            _drain_stale_lkg(stale_lkg_to_refresh, tradier, cache, master_snap)
        )
        _stale_lkg_queued = len(stale_lkg_to_refresh)
        print(
            f"[WATCHLIST_LKG_REFRESH] Queued {_stale_lkg_queued} stale LKG symbols "
            f"for background refresh"
        )

    # 5. Build normalised signal map using multi-source join
    # "Stale" means the data is OLD — not just that it came from disk LKG.
    # LKG entries written within the last 30 min are treated as current because
    # the drain just refreshed them; their memory-cache entry may have expired
    # (300s TTL) before the user re-called, so disk LKG re-served it as from_lkg.
    _FRESH_LKG_THRESHOLD = 1800  # 30 min — LKG entries younger than this are current
    _market_hours = _is_market_hours_et()
    signals: dict[str, dict] = {}
    _stale_set: set[str] = set(uncached) | set(inflight) | {
        s for s, r in results.items()
        if r.get("from_lkg") and (
            _lkg_age_seconds(r) is None or
            _lkg_age_seconds(r) > _FRESH_LKG_THRESHOLD
        )
    }
    for sym in syms:
        r          = results[sym]
        is_stale   = sym in _stale_set

        # Multi-source join:
        #   primary_row    = raw master screener row (score / IV / OI / signal)
        #   supplement_row = supplement combined row  (premium / volume / interval)
        #   lkg_row        = portfolio disk LKG       (score / IV / OI fallback)
        #                    or currently-cached row if no disk entry
        _master_raw  = master_by_ticker.get(sym)
        _supp_row    = combined_snap.get(sym)
        _disk_lkg_r  = disk_lkg.get(sym) if disk_lkg.get(sym, {}).get("data_available") else None
        # If no disk entry, use the currently-served row as fallback
        _lkg_or_cur  = _disk_lkg_r or (r if r.get("data_available") else None)

        has_any_data = bool(_master_raw or _supp_row or _lkg_or_cur)
        if has_any_data:
            merged = _merge_options_sources(
                sym,
                primary_row    = _master_raw,
                supplement_row = _supp_row,
                lkg_row        = _lkg_or_cur,
                history_deltas = _hist_by_ticker.get(sym),
                is_stale       = is_stale,
                market_hours   = _market_hours,
            )
            signals[sym] = _normalize_to_watchlist_row(sym, merged, is_stale, market_hours=_market_hours)
        else:
            signals[sym] = _normalize_to_watchlist_row(sym, r, is_stale, market_hours=_market_hours)

    # 6. Extended diagnostics
    from collections import Counter as _Counter
    _src_ctr:    _Counter = _Counter()
    _reason_ctr: _Counter = _Counter()
    _class_ctr:  _Counter = _Counter()
    _da_true  = 0
    _da_false = 0
    _no_opts_cnt         = 0
    _confirmed_unsup_cnt = 0
    _confirmed_noopts_cnt = 0
    _transient_cnt       = 0
    _oldest_lkg_age: float | None = None
    for _sym, _r in results.items():
        _src_ctr[_r.get("source", "unknown")] += 1
        if _r.get("data_available"):
            _da_true += 1
            if _r.get("from_lkg"):
                _age = _lkg_age_seconds(_r)
                if _age is not None:
                    _oldest_lkg_age = max(_oldest_lkg_age or 0.0, _age)
        else:
            _da_false += 1
            _rsn = _r.get("unavailable_reason") or "unknown"
            _reason_ctr[_rsn] += 1
            if _rsn in ("no_options", "no_expirations", "otc_or_foreign_unsupported"):
                _no_opts_cnt += 1
            if _rsn == "otc_or_foreign_unsupported":
                _confirmed_unsup_cnt += 1
            if _rsn in ("no_options", "no_expirations"):
                _confirmed_noopts_cnt += 1
            if _rsn in ("provider_rate_limited", "no_chain_returned",
                        "not_in_tradier_coverage",
                        "quote_batch_partial_or_missing") or \
                    _rsn.startswith("unknown_provider_error"):
                _transient_cnt += 1
        # classification breakdown for diagnostics
        _is_stale_sym = _sym in _stale_set
        _class_ctr[_classify_watchlist_sym(_r, _is_stale_sym)] += 1

    _stale_cnt = sum(1 for s in syms if s in _stale_set)
    _fresh_cnt = sum(1 for s in syms
                     if results[s].get("data_available") and s not in _stale_set)
    _optionable_cnt = sum(
        1 for _r in results.values()
        if _r.get("data_available") or _r.get("optionable") is True
    )
    # retry_queued: transient-failure symbols just added to uncached queue
    _retry_queued = sum(
        1 for s in _to_scan
        if results.get(s, {}).get("unavailable_reason") in (
            "provider_rate_limited", "no_chain_returned"
        )
    )
    # retry_suppressed: transient failures still in 30-min TTL cache (not re-queued)
    _retry_suppressed = max(0, _transient_cnt - _retry_queued)

    # ── Quote-retry and standard-US diagnostics ─────────────────────────────
    _quote_batch_partial_cnt = sum(
        1 for _r in results.values()
        if (_r.get("unavailable_reason") or "") == "quote_batch_partial_or_missing"
    )
    _quote_chain_ok_no_price_cnt = sum(
        1 for _r in results.values()
        if _r.get("data_available") and _r.get("quote_price_missing")
    )
    _std_us_transient_cnt = sum(
        1 for _s, _r in results.items()
        if ":" not in _s and (
            (_r.get("unavailable_reason") or "") in (
                "provider_rate_limited", "no_chain_returned",
                "not_in_tradier_coverage", "quote_batch_partial_or_missing"
            ) or (_r.get("unavailable_reason") or "").startswith("unknown_provider_error")
        )
    )
    # Truly blank: standard US, no data available, no LKG fallback, no retry_pending,
    # and a recoverable transient reason (not a permanent confirmed-unavailable state).
    _std_us_missing_after_retry_cnt = sum(
        1 for _s, _r in results.items()
        if ":" not in _s
        and not _r.get("data_available")
        and not _r.get("from_lkg")
        and not _r.get("retry_pending")
        and (_r.get("unavailable_reason") or "") in (
            "provider_rate_limited", "no_chain_returned",
            "not_in_tradier_coverage", "quote_batch_partial_or_missing",
            "unknown_provider_error",
        )
    )
    _std_us_with_prior_lkg_cnt = sum(
        1 for _s, _r in results.items()
        if ":" not in _s and _r.get("from_lkg") and _r.get("data_available")
    )

    # ── Freshness SLA metrics ────────────────────────────────────────────────
    # Collect (sym, lkg_age_s) for every data_available row that came from LKG.
    _sla_stale_ages: list[tuple[str, float]] = []
    for _s, _r in results.items():
        if not _r.get("data_available"):
            continue
        _age = _lkg_age_seconds(_r) if _r.get("from_lkg") else None
        if _age is not None:
            _sla_stale_ages.append((_s, _age))

    _stale_u15_cnt    = sum(1 for _, a in _sla_stale_ages if a < _SLA_FRESH_AGE_S)
    _stale_15_60_cnt  = sum(1 for _, a in _sla_stale_ages if _SLA_FRESH_AGE_S <= a < _SLA_WARN_AGE_S)
    _stale_over60_cnt = sum(1 for _, a in _sla_stale_ages if a >= _SLA_WARN_AGE_S)

    # SLA-breach symbols: standard US, stale ≥ 60 min, during market hours only.
    _sla_breach_syms: list[str] = sorted(
        _s for _s, _a in _sla_stale_ages
        if _a >= _SLA_WARN_AGE_S and ":" not in _s
    ) if _market_hours else []

    _sorted_stale_ages = sorted(a for _, a in _sla_stale_ages)
    _oldest_stale_age_s = _sorted_stale_ages[-1] if _sorted_stale_ages else None
    _median_stale_age_s = (
        _sorted_stale_ages[len(_sorted_stale_ages) // 2] if _sorted_stale_ages else None
    )

    # High-priority refresh: SLA-breached symbols are already in stale_lkg_to_refresh
    # (because _STALE_LKG_REFRESH_AGE == _SLA_WARN_AGE_S == 3600s).
    _hp_refresh_queued = len(_sla_breach_syms)

    # User-facing standard-US coverage breakdown
    _std_us_displayable = sum(
        1 for _s, _r in results.items()
        if ":" not in _s and _r.get("data_available")
    )
    _std_us_pending_no_values = sum(
        1 for _s, _r in results.items()
        if ":" not in _s
        and not _r.get("data_available")
        and (_r.get("unavailable_reason") or "") in ("scan_pending", "scan_in_progress")
    )
    _std_us_confirmed_no_opts = sum(
        1 for _s, _r in results.items()
        if ":" not in _s
        and (_r.get("unavailable_reason") or "") in ("no_options", "no_expirations")
    )
    _std_us_stale_over_sla = sum(1 for _s in _sla_breach_syms if ":" not in _s)

    # scan_pending symbols for diagnostics
    _scan_pending_syms: list[str] = sorted(
        _s for _s, _r in results.items()
        if ":" not in _s
        and (_r.get("unavailable_reason") or "") in ("scan_pending", "scan_in_progress")
    )

    # Orphaned cache entries: symbols removed from this watchlist that still
    # have live per-ticker memory cache entries (will expire naturally).
    _orphaned_cnt = 0
    if _syms_removed:
        for _rs in _syms_removed:
            if cache.get(_per_ticker_cache_key(_rs)):
                _orphaned_cnt += 1

    elapsed_ms = round((_tm.monotonic() - _t0) * 1000, 1)

    # Format module-level timestamps for JSON
    def _fmt_ts(t: float | None) -> str | None:
        if t is None:
            return None
        import datetime as _dt2
        return _dt2.datetime.utcfromtimestamp(t).isoformat() + "Z"

    return {
        "signals": signals,
        "options_meta": {
            "scope":                          "watchlist",
            "symbols_requested":              len(syms),
            # ── Cache layer hits ──────────────────────────────────────────
            "cache_hits":                     cache_hits,
            "master_hits":                    master_hits,
            "lkg_hits":                       lkg_hits,
            "cache_misses":                   len(uncached),
            # ── Availability breakdown ────────────────────────────────────
            "data_available_count":           _da_true,
            "data_unavailable_count":         _da_false,
            "stale_count":                    _stale_cnt,
            "fresh_count":                    _fresh_cnt,
            # ── Classification breakdown ──────────────────────────────────
            "classification_breakdown":       dict(_class_ctr.most_common()),
            # ── Source / reason breakdowns ────────────────────────────────
            "source_breakdown":               dict(_src_ctr.most_common()),
            "unavailable_reason_breakdown":   dict(_reason_ctr.most_common()),
            # ── Scan queue state ──────────────────────────────────────────
            "live_calls_enqueued":            enqueued,
            "deferred_symbols_count":         _deferred,
            "inflight_symbols_count":         len(inflight),
            "already_inflight":               len(inflight),
            "scan_in_progress":               len(inflight),   # kept for compat
            "rate_limited_or_deferred":       _deferred,       # kept for compat
            "no_options_cached_count":        _no_opts_cnt,
            "next_refresh_candidates_count":  len(uncached) - _deferred,
            # ── New required diagnostics ──────────────────────────────────
            "optionable_symbols_count":           _optionable_cnt,
            "confirmed_unsupported_count":         _confirmed_unsup_cnt,
            "confirmed_no_options_count":          _confirmed_noopts_cnt,
            "transient_failure_count":             _transient_cnt,
            "stale_lkg_refresh_candidates_count":  len(stale_lkg_to_refresh),
            # ── Quote-retry diagnostics ───────────────────────────────
            "quote_batch_partial_count":                    _quote_batch_partial_cnt,
            "quote_chain_ok_no_price_count":                _quote_chain_ok_no_price_cnt,
            "quote_retry_queued_count":                     _WL_QUOTE_RETRY_STATS["queued_total"],
            "quote_retry_attempted_count":                  _WL_QUOTE_RETRY_STATS["attempted"],
            "quote_retry_succeeded_count":                  _WL_QUOTE_RETRY_STATS["succeeded"],
            "quote_retry_failed_count":                     _WL_QUOTE_RETRY_STATS["failed"],
            "standard_us_transient_count":                  _std_us_transient_cnt,
            "standard_us_missing_after_retry_count":        _std_us_missing_after_retry_cnt,
            "standard_us_with_prior_lkg_count":             _std_us_with_prior_lkg_cnt,
            "standard_us_prior_lkg_refreshed_without_quote_count": _quote_chain_ok_no_price_cnt,
            "stale_lkg_queued_count":              _stale_lkg_queued,
            "retry_queued_count":                  _retry_queued,
            "retry_suppressed_count":              _retry_suppressed,
            "oldest_lkg_age_seconds":              round(_oldest_lkg_age, 0) if _oldest_lkg_age else None,
            "latest_successful_refresh_at":        _fmt_ts(_WL_LATEST_SUCCESSFUL_REFRESH_AT),
            "last_full_watchlist_refresh_started_at":   _fmt_ts(_WL_LAST_REFRESH_STARTED_AT),
            "last_full_watchlist_refresh_completed_at": _fmt_ts(_WL_LAST_REFRESH_COMPLETED_AT),
            "force_refresh_applied":               force_refresh,
            # ── Freshness SLA diagnostics ─────────────────────────────────────────
            "market_hours":                        _market_hours,
            "fresh_options_count":                 _fresh_cnt,
            "stale_under_15m_count":               _stale_u15_cnt,
            "stale_15m_to_60m_count":              _stale_15_60_cnt,
            "stale_over_60m_count":                _stale_over60_cnt,
            "oldest_stale_age_seconds":            round(_oldest_stale_age_s, 0) if _oldest_stale_age_s else None,
            "median_stale_age_seconds":            round(_median_stale_age_s, 0) if _median_stale_age_s else None,
            "stale_over_sla_symbols":              _sla_breach_syms,
            "high_priority_refresh_queued_count":  _hp_refresh_queued,
            "scan_pending_count":                  len(_scan_pending_syms),
            "scan_pending_symbols":                _scan_pending_syms,
            "last_successful_options_refresh_at":  _fmt_ts(_WL_LATEST_SUCCESSFUL_REFRESH_AT),
            # ── User-facing standard-US coverage breakdown ────────────────────────
            "standard_us_displayable_with_values": _std_us_displayable,
            "standard_us_pending_no_values":       _std_us_pending_no_values,
            "standard_us_confirmed_no_options":    _std_us_confirmed_no_opts,
            "standard_us_stale_over_sla":          _std_us_stale_over_sla,
            # ── Symbol-set change tracking (add/remove without restart) ──────────
            "watchlist_symbols_hash":          _syms_hash,
            "symbols_added_since_last_call":   _syms_added,
            "symbols_removed_since_last_call": _syms_removed,
            "active_watchlist_symbols_count":  _active_us_count,
            "orphaned_cache_symbols_count":    _orphaned_cnt,
            "refresh_queue_by_scope": {
                "uncached_scan_queued":     len(_to_scan),
                "stale_lkg_refresh_queued": len(stale_lkg_to_refresh),
                "deferred_background":      _deferred,
                "quote_retry_lifetime":     _WL_QUOTE_RETRY_STATS["queued_total"],
            },
            # ── Multi-source join diagnostics ─────────────────────────────────
            "master_rows_found":          sum(1 for s in syms if s in master_by_ticker),
            "premium_rows_found":         sum(1 for s in syms if combined_snap.get(s) and (
                combined_snap[s].get("call_premium") is not None
                or combined_snap[s].get("net_premium") is not None
            )),
            "history_rows_found":         len(_hist_by_ticker),
            "joined_rows":                sum(1 for s in syms if (
                master_by_ticker.get(s) or combined_snap.get(s)
                or (disk_lkg.get(s, {}).get("data_available"))
            )),
            "rows_with_score":            sum(1 for v in signals.values() if v.get("options_score") is not None),
            "rows_with_signal":           sum(1 for v in signals.values() if v.get("options_signal")),
            "rows_with_volume_pc":        sum(1 for v in signals.values() if v.get("volume_put_call_ratio") is not None),
            "rows_with_premium_pc":       sum(1 for v in signals.values() if v.get("premium_put_call_ratio") is not None),
            "rows_with_net_premium":      sum(1 for v in signals.values() if v.get("options_net_premium") is not None),
            "rows_with_iv":               sum(1 for v in signals.values() if v.get("options_iv") is not None),
            "rows_with_1d_history":       sum(1 for v in signals.values() if v.get("net_premium_change_1d") is not None),
            "rows_with_7d_history":       sum(1 for v in signals.values() if v.get("net_premium_change_7d") is not None),
            "rows_with_30d_history":      sum(1 for v in signals.values() if v.get("net_premium_change_30d") is not None),
            "supplement_coverage":        len(combined_snap),
            # ── Timing ───────────────────────────────────────────────────
            "generated_at":                   _dt.datetime.utcnow().isoformat() + "Z",
            "ttl_seconds":                    _CACHE_PER_TICKER_TTL,
            "elapsed_ms":                     elapsed_ms,
        },
    }
