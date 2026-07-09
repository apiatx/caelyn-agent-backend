"""
services/daily_alpha_board_service.py
======================================
Daily Alpha Board — cache-only cross-market signal ranking engine.

GET /api/home/daily-alpha-board

CRITICAL: ZERO external provider/API calls.
Reads ONLY from:
  - Disk JSON snapshots  (strategy_screener_lkg, themes_rs_lkg,
                          hyperliquid_signal_snapshots, x_consensus_weekly,
                          options_master_lkg_v1, earnings_snap_*)
  - In-memory TTLCache   (regime:current_v1, options LKG, social LKG)
  - Neon snapshot tables (watchlist rows, portfolio holdings)
  - In-process module caches (_matrix_cache in hyperliquid router)

If a source cannot be read without an external call, it is skipped and
marked as 'unavailable_cache_only' in source_health.
"""

from __future__ import annotations

import glob
import json
import time
import os
from datetime import datetime, timezone, date
from pathlib import Path
from typing import Any, Optional

# ─────────────────────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────────────────────
_DATA_DIR           = Path(__file__).parent.parent / "data"
_STRATEGY_LKG_PATH  = _DATA_DIR / "strategy_screener_lkg.json"
_THEMES_LKG_PATH    = _DATA_DIR / "themes_rs_lkg.json"
_HL_SNAP_PATH       = _DATA_DIR / "hyperliquid_signal_snapshots.json"
_X_CONSENSUS_PATH   = _DATA_DIR / "x_consensus_weekly.json"
_OPTIONS_PATHS      = [
    _DATA_DIR / "options_master_lkg_v1.json",
    _DATA_DIR / "options_lkg_v1_large_cap.json",
    _DATA_DIR / "options_lkg_v1_small_cap.json",
]

# ─────────────────────────────────────────────────────────────────────────────
# Aggregator TTL cache  (ranking result only — never provider data)
# ─────────────────────────────────────────────────────────────────────────────
_TTL_MARKET_HOURS = 60       # seconds — Mon-Fri 09:30–16:00 ET
_TTL_OFF_HOURS    = 300      # seconds — evenings / weekends

_BOARD_CACHE: dict[str, Any] = {
    "result": None,
    "ts":     0.0,
    "lkg":    None,   # last-known-good ranking result
    "ttl":    _TTL_OFF_HOURS,
}

# ─────────────────────────────────────────────────────────────────────────────
# Scoring weight tables
# ─────────────────────────────────────────────────────────────────────────────
_STOCK_WEIGHTS: dict[str, float] = {
    "theme":        0.20,
    "ta":           0.18,
    "rel_volume":   0.15,
    "catalyst":     0.12,
    "options":      0.10,
    "social":       0.10,
    "news":         0.08,
    "fundamentals": 0.05,
    "relevance":    0.02,
}

_CRYPTO_WEIGHTS: dict[str, float] = {
    "momentum":             0.25,
    "oi":                   0.18,
    "volume_velocity":      0.15,
    "funding_quality":      0.12,
    "liquidation":          0.10,
    "volatility_expansion": 0.08,
    "macro":                0.07,
    "social_news":          0.05,
}

# Minimum independent signal categories to qualify
_MIN_SIGNALS = 2

# Timing signals — at least one required for basic qualification
_STOCK_TIMING  = {"ta", "rel_volume", "catalyst", "options", "social", "news"}
_CRYPTO_TIMING = {"momentum", "oi", "volume_velocity", "liquidation"}

# External timing signals — required for Top-N qualification (not stage-derived)
# If the ONLY timing signals are theme+stage-ta (same file), candidate is watch_only
_STOCK_EXTERNAL_TIMING = {"rel_volume", "catalyst", "options", "social"}

# ── Crypto / Hyperliquid quality gates ────────────────────────────────────────
# Matrix signal labels that pass long quality gate
_CRYPTO_MATRIX_PASS  = {"LONG", "WATCH"}
_CRYPTO_MATRIX_AVOID = {"CROWDED", "AVOID"}

# Anti-pump thresholds (percentage, from oiDelta*Pct fields)
_PUMP_OI_5M_PCT_THRESH  = 15.0   # >15% OI spike in 5m = suspicious without TSM
_PUMP_OI_15M_PCT_THRESH = 20.0   # >20% in 15m = suspicious
_PUMP_24H_NO_TSM        = 0.20   # >20% 24h without structural quality = pump
_MIN_STRUCT_Q_HIGH      = 0.28   # structural quality below this = low quality

# High-confidence crypto thresholds
_MIN_STRUCT_Q_HARD_EXCLUDE = 0.35  # pump + below this → hard exclude (not penalize)
_MIN_STRUCT_Q_HIGH_CONF    = 0.45  # required for high confidence crypto ideas
_OI_ONLY_MAX_SIGNAL        = 0.68  # hard cap on all signals for OI-only fallback (~69/100 score)

# Module-level diagnostic counters populated by the Hyperliquid collector
_HL_DIAG: dict = {
    "rejected_pump":    [],
    "rejected_crowded": [],
    "accepted_tsm":     [],
}

# Module-level diagnostic counters populated by the long-bias safety pass
_LB_DIAG: dict = {
    "stock_shorts_suppressed":            0,
    "stock_short_candidates_converted_to_watch": 0,
    "stock_extension_notes_added":        0,
    "crypto_short_candidates":            [],
}

# ── Phase 1 — RS provenance diagnostics ───────────────────────────────────────
# Tracks whether the watchlist collector is consuming social rank vs price RS.
_RS_PROV_DIAG: dict = {
    "symbols_resolved_social_rank":     0,  # found in _backend_ranked → social_rank_score used
    "symbols_missing_canonical_social": 0,  # NOT in _backend_ranked → social_sig = None
    "symbols_legacy_rs_dropped":        0,  # had row.rs_score but it was dropped (correct)
}

# ── Phase 2 — Technical freshness diagnostics ─────────────────────────────────
_TECH_FRESH_DIAG: dict = {
    "technical_fresh_used":        0,
    "technical_stale_omitted":     0,
    "technical_missing_timestamp": 0,
    "technical_missing":           0,
}

# Max age for consuming a stored technical_score from Neon watchlist store.
# Matches the existing watchlist_stage2_service _FRESH_HOURS = 20h TTL.
# We use 24h here (slightly more lenient) so that an overnight analysis run
# is still considered fresh for the following trading session.
_TECH_MAX_AGE_S: float = 86400.0  # 24h


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _is_market_hours() -> bool:
    """True Mon-Fri 09:30–16:00 US/Eastern (approximated from UTC-4/UTC-5)."""
    import datetime as _dt
    now_utc = _dt.datetime.now(_dt.timezone.utc)
    weekday = now_utc.weekday()           # 0=Mon … 4=Fri
    if weekday >= 5:
        return False
    # rough ET offset — close enough for TTL selection
    hour_et = (now_utc.hour - 4) % 24    # EDT; EST would be -5
    return 9 <= hour_et < 16


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_json_safe(path: Path | str) -> Any | None:
    """Read a JSON file; return None on any error."""
    try:
        with open(path) as fh:
            return json.load(fh)
    except Exception:
        return None


def _load_social_backend_ranked() -> dict[str, dict]:
    """
    Load x_consensus_weekly.json _backend_ranked array keyed by uppercase ticker.

    Each entry contains: ticker, backend_score, freshness_score,
    recency_days_min, has_top_conviction, thesis_fragments, catalyst_list.

    Returns {} on any error.  Zero provider calls.
    Canonical social source for Phase 1 RS provenance fix and Phase 7 V2.
    """
    try:
        raw = _load_json_safe(_X_CONSENSUS_PATH)
        if not raw:
            return {}
        ranked = raw.get("_backend_ranked") or raw.get("top_tickers") or []
        result: dict[str, dict] = {}
        for r in ranked:
            ticker = (r.get("ticker") or "").upper().strip()
            if ticker:
                result[ticker] = r
        return result
    except Exception:
        return {}


def _staleness_factor(age_seconds: float | None) -> float:
    """Return a weight multiplier based on how old the source data is."""
    if age_seconds is None:
        return 0.85          # unknown age — slight penalty
    if age_seconds < 7_200:   # < 2 h   → fresh
        return 1.0
    if age_seconds < 43_200:  # 2 – 12 h → mildly stale
        return 0.90
    if age_seconds < 172_800: # 12 – 48 h → stale
        return 0.70
    return 0.0               # > 48 h → exclude


def _source_age_s(ts: float | None) -> float | None:
    if ts is None:
        return None
    return max(0.0, time.time() - ts)


def _clamp01(v: float | None) -> float | None:
    if v is None:
        return None
    return max(0.0, min(1.0, float(v)))


def _grade_to_score(grade: str | None) -> float | None:
    if not grade:
        return None
    return {"A": 0.95, "B": 0.75, "C": 0.55, "D": 0.35, "F": 0.15}.get(
        str(grade).upper(), None
    )


def _current_week_bounds() -> tuple[str, str]:
    """Return ISO dates for Mon-Fri of the current week."""
    today = date.today()
    mon = today - __import__("datetime").timedelta(days=today.weekday())
    fri = mon + __import__("datetime").timedelta(days=4)
    return mon.isoformat(), fri.isoformat()


# ── Stock summary / evidence text sanitiser ────────────────────────────────
# Rewrites short-biased phrases from watchlist action_notes so the main board
# always reads as a long-watchlist tool, never a short-recommendation tool.
import re as _re

_STOCK_TEXT_REPLACEMENTS: list[tuple] = [
    # Most specific patterns first
    (_re.compile(r'\bshort\s*/\s*reduce exposure\b', _re.I), "watch for better entry"),
    (_re.compile(r'\breduce exposure\b', _re.I),             "watch for better entry"),
    (_re.compile(r'\breduce position\b', _re.I),             "size carefully given extension"),
    (_re.compile(r'\btake profits?\b', _re.I),               "consider trimming into strength"),
    (_re.compile(r'\boverbought[,;]\s*fade\b', _re.I),       "extended near-term"),
    (_re.compile(r'\boverbought[,;]\s*short\b', _re.I),      "extended — better entry on pullback"),
    (_re.compile(r'\bsell due to rsi\b', _re.I),             "entry quality lower due to extension"),
    (_re.compile(r'\bbearish because rsi\b', _re.I),         "extended near-term — better entry on pullback"),
    (_re.compile(r'\bshort the\b', _re.I),                   "watch"),
    # "short" standalone as verb (not "short-term", "short-term setup", etc.)
    (_re.compile(r'(?<!\w)short(?!\s*[-–]term)(?!\s*term)(?!\w)', _re.I), "watch"),
]


def _clean_stock_text(text: str) -> str:
    """Rewrite short-biased phrases in stock watchlist text to long-bias framing."""
    if not text:
        return text
    for pattern, replacement in _STOCK_TEXT_REPLACEMENTS:
        text = pattern.sub(replacement, text)
    return text


# ─────────────────────────────────────────────────────────────────────────────
# Candidate factory
# ─────────────────────────────────────────────────────────────────────────────

def _make_candidate(
    symbol: str,
    *,
    name: str | None = None,
    asset_type: str = "stock",
    direction: str = "long",
    timeframe: str = "2-10d",
    score: float = 0.0,
    confidence: str = "low",
    status: str = "watch_only",
    setup_type: str = "",
    theme: str | None = None,
    sector: str | None = None,
    summary: str = "",
    trigger: str | None = None,
    invalidation: str | None = None,
    signals: dict | None = None,
    evidence: list[str] | None = None,
    risks: list[str] | None = None,
    source_pages: list[str] | None = None,
    updated_at: str | None = None,
) -> dict:
    base_signals: dict[str, Any] = {
        "ta": None, "fundamentals": None, "catalysts": None,
        "social": None, "news": None, "options": None,
        "theme": None, "macro": None, "hyperliquid": None,
        "momentum": None, "rel_volume": None,
    }
    if signals:
        base_signals.update(signals)
    _is_stock = asset_type not in ("crypto", "perp")
    return {
        "symbol":           symbol.upper().strip(),
        "name":             name,
        "asset_type":       asset_type,
        "direction":        direction,
        "timeframe":        timeframe,
        "score":            round(float(score), 4),
        "confidence":       confidence,
        "status":           status,
        "setup_type":       setup_type,
        "theme":            theme,
        "sector":           sector,
        "summary":          summary,
        "trigger":          trigger,
        "invalidation":     invalidation,
        "signals":          base_signals,
        "evidence":         list(evidence or []),
        "risks":            list(risks or []),
        "source_pages":     list(source_pages or []),
        "updated_at":       updated_at or _now_iso(),
        "long_bias":        _is_stock,
        "entry_quality":    None,
        "preferred_action": None,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Long-bias helper — assigns entry_quality + preferred_action for stocks/ETFs
# ─────────────────────────────────────────────────────────────────────────────

def _set_stock_long_fields(c: dict) -> None:
    """
    Mutates a stock/ETF candidate in-place:
      • Converts any remaining direction="short" → "watch"
      • Assigns entry_quality and preferred_action based on setup and extension signals
      • Never sets direction="short" for stocks/ETFs
    """
    at = c.get("asset_type", "stock")
    if at not in ("stock", "etf"):
        return

    # Hard rule: no short direction for stocks/ETFs
    if c.get("direction") == "short":
        c["direction"] = "watch"

    ext_risk    = c.get("extension_risk") or "low"
    setup_b     = c.get("setup_bucket") or ""
    timing_q    = c.get("timing_quality") or "medium"
    cat_window  = c.get("catalyst_window") or ""
    days_earn   = c.get("days_to_earnings")
    earn_result = c.get("earnings_result") or ""

    # entry_quality
    if ext_risk == "high":
        entry_q = "extended"
    elif setup_b in ("stage_1_to_2_base", "cup_handle_watch", "early_breakout"):
        entry_q = "early" if timing_q == "high" else "good"
    elif setup_b in ("dip_reversal_watch", "momentum_resumption"):
        entry_q = "good"
    elif earn_result == "Miss":
        entry_q = "wait_for_pullback"
    else:
        entry_q = "good"

    # preferred_action
    if earn_result == "Beat" and days_earn is not None and days_earn < 0:
        pref = "post_earnings_watch"
    elif cat_window in ("pre_earnings_core", "pre_earnings_extended"):
        pref = "pre_earnings_build"
    elif ext_risk == "high":
        pref = "wait_for_pullback"
    elif setup_b in ("stage_1_to_2_base", "cup_handle_watch"):
        pref = "watch_for_entry"
    elif setup_b in ("early_breakout", "momentum_resumption"):
        pref = "active_long_setup"
    elif earn_result == "Miss":
        pref = "watch_for_entry"
    else:
        pref = "momentum_continuation" if c.get("direction") == "long" else "watch_for_entry"

    c["entry_quality"]    = entry_q
    c["preferred_action"] = pref


# ─────────────────────────────────────────────────────────────────────────────
# Available-signal weighted scoring
# ─────────────────────────────────────────────────────────────────────────────

def _score_candidate(candidate: dict, regime: dict) -> float:
    """
    Compute and return a normalised score (0–100 on candidate["score"],
    raw 0–1 kept in candidate["score_raw"]) using available-signal weighted
    normalization.  Sets candidate["confidence"] and candidate["has_timing_signal"].
    Returns 0 if the candidate doesn't have enough qualifying signals.

    external_api_calls: 0 — reads only in-process candidate dict.
    """
    atype = candidate.get("asset_type", "stock")
    sigs  = candidate.get("signals", {})

    if atype in ("crypto", "perp"):
        weights = _CRYPTO_WEIGHTS
        sig_map = {
            "momentum":             sigs.get("momentum"),
            "oi":                   sigs.get("hyperliquid"),
            "volume_velocity":      sigs.get("rel_volume"),
            "funding_quality":      sigs.get("ta"),
            "liquidation":          None,
            "volatility_expansion": None,
            "macro":                sigs.get("macro"),
            "social_news":          sigs.get("social") or sigs.get("news"),
        }
        timing_keys = _CRYPTO_TIMING
    else:
        weights  = _STOCK_WEIGHTS
        sig_map  = {
            "theme":        sigs.get("theme"),
            "ta":           sigs.get("ta"),
            "rel_volume":   sigs.get("rel_volume"),
            "catalyst":     sigs.get("catalysts"),
            "options":      sigs.get("options"),
            "social":       sigs.get("social"),
            "news":         sigs.get("news"),
            "fundamentals": sigs.get("fundamentals"),
            "relevance":    sigs.get("macro"),
        }
        timing_keys = _STOCK_TIMING

    # Count independent signal categories present
    present = {k for k, v in sig_map.items() if v is not None}
    if len(present) < _MIN_SIGNALS:
        return 0.0

    # Require at least one timing signal (expanded set includes social/news)
    if not (present & timing_keys):
        return 0.0

    # Available-weight normalization
    avail_weight = sum(weights[k] for k in present)
    if avail_weight < 0.01:
        return 0.0

    raw = sum(float(sig_map[k] or 0) * weights[k] for k in present)
    score = raw / avail_weight   # 0-1

    # ── Regime modifier (cache-only) ──────────────────────────────────────────
    rlabel = (regime or {}).get("label", "neutral")
    conf   = (regime or {}).get("confidence", 0.5)
    if rlabel == "risk_on":
        if atype in ("crypto", "perp"):
            score = min(1.0, score * (1 + 0.06 * conf))
        elif candidate.get("theme"):
            score = min(1.0, score * (1 + 0.04 * conf))
    elif rlabel == "risk_off":
        if atype in ("crypto", "perp"):
            score = max(0.0, score * (1 - 0.08 * conf))
        elif candidate.get("status") == "extended":
            score = max(0.0, score * (1 - 0.05 * conf))
    elif rlabel == "major_macro_event_soon":
        if sigs.get("catalysts") is None:
            score = max(0.0, score * 0.92)

    # Relative strength boost in weak market
    rs_boost = candidate.get("_rs_boost", False)
    if rs_boost and rlabel in ("risk_off", "neutral"):
        score = min(1.0, score * 1.05)

    # ── Theme-only guard ──────────────────────────────────────────────────────
    # Candidates whose ONLY source is the themes LKG file carry stage-derived ta
    # and 1D-perf news — all from the same file, not independent market data.
    # They cannot be high-conviction Top-N unless they also have an external
    # actionable timing signal (rel_volume, catalyst, options, or social).
    src_pages = set(candidate.get("source_pages", []))
    has_external_timing = bool(present & _STOCK_EXTERNAL_TIMING)
    is_themes_only = (src_pages == {"themes"} and atype not in ("crypto", "perp"))

    if is_themes_only and not has_external_timing:
        # Apply score penalty and cap confidence — mark as watch_only
        score = max(0.0, score * 0.85)
        candidate["has_timing_signal"] = False
        candidate["watch_only_reason"] = "theme_only_no_external_timing"
    else:
        candidate["has_timing_signal"] = True

    # ── Confidence label (based on 0-100 scale thresholds) ───────────────────
    score_100 = score * 100
    if candidate.get("has_timing_signal", True) is False:
        conf_label = "medium" if score_100 >= 50 else "low"
    elif score_100 >= 72:
        conf_label = "high"
    elif score_100 >= 50:
        conf_label = "medium"
    else:
        conf_label = "low"

    # ── Store scores ──────────────────────────────────────────────────────────
    candidate["score_raw"]  = round(score, 4)           # 0-1 for internal use
    candidate["score"]      = round(score_100, 2)       # 0-100 for frontend
    candidate["confidence"] = conf_label
    return score


# ─────────────────────────────────────────────────────────────────────────────
# Deduplication / merge
# ─────────────────────────────────────────────────────────────────────────────

def _merge_candidates(candidates: list[dict]) -> list[dict]:
    """
    Collapse candidates with the same symbol into a single richer entry.
    For crypto/perps that share a display coin (e.g. BTC vs BTCUSDT),
    also dedupe by the base coin portion.
    """
    merged: dict[str, dict] = {}

    def _key(c: dict) -> str:
        sym = c["symbol"].upper().replace("-PERP", "").replace("USDT", "").strip("@")
        return sym

    for c in candidates:
        k = _key(c)
        if k not in merged:
            merged[k] = dict(c)
        else:
            m = merged[k]
            # Merge lists
            m["evidence"]     = list(dict.fromkeys(m["evidence"]     + c["evidence"]))
            m["risks"]        = list(dict.fromkeys(m["risks"]        + c["risks"]))
            m["source_pages"] = list(dict.fromkeys(m["source_pages"] + c["source_pages"]))
            # Merge signals — prefer non-None and higher values
            for sig, val in c["signals"].items():
                if val is not None:
                    existing = m["signals"].get(sig)
                    if existing is None or float(val) > float(existing):
                        m["signals"][sig] = val
            # Prefer richer metadata
            if not m.get("name") and c.get("name"):
                m["name"] = c["name"]
            if not m.get("theme") and c.get("theme"):
                m["theme"] = c["theme"]
            if not m.get("sector") and c.get("sector"):
                m["sector"] = c["sector"]
            if not m.get("trigger") and c.get("trigger"):
                m["trigger"] = c["trigger"]
            if not m.get("invalidation") and c.get("invalidation"):
                m["invalidation"] = c["invalidation"]
            # Prefer longer summary
            if len(c.get("summary", "")) > len(m.get("summary", "")):
                m["summary"] = c["summary"]
            if c.get("_rs_boost"):
                m["_rs_boost"] = True
            # Propagate extended fields from quality passes — keep first non-None value
            for _ext_key in (
                "catalyst_window", "days_to_earnings", "earnings_result",
                "eps_surprise_pct", "setup_bucket", "extension_risk",
                "timing_quality", "hyperliquid_quality_gate", "tsm_quality",
                "matrix_signal", "entry_quality", "preferred_action",
            ):
                if m.get(_ext_key) is None and c.get(_ext_key) is not None:
                    m[_ext_key] = c[_ext_key]

    return list(merged.values())


# ─────────────────────────────────────────────────────────────────────────────
# Source health tracker
# ─────────────────────────────────────────────────────────────────────────────

class _SourceHealth:
    STATUSES = ("ok", "missing", "stale", "unavailable_cache_only", "error")

    def __init__(self):
        self._h: dict[str, str] = {
            k: "missing" for k in (
                "watchlist", "portfolio", "social", "themes",
                "strategy", "catalysts", "options", "hyperliquid", "macro",
            )
        }
        self._counts: dict[str, int] = {k: 0 for k in self._h}

    def set(self, source: str, status: str, count: int = 0):
        self._h[source]      = status
        self._counts[source] = count

    @property
    def health(self) -> dict[str, str]:
        return dict(self._h)

    @property
    def counts(self) -> dict[str, int]:
        return dict(self._counts)


# ─────────────────────────────────────────────────────────────────────────────
# 1. Watchlist collector
# ─────────────────────────────────────────────────────────────────────────────

def collect_watchlist_cache_candidates() -> tuple[list[dict], str]:
    """
    Read all saved watchlists from Neon (pure DB read, zero API calls).
    Extracts ticker rows from analysis.sections[].tickers[].
    Returns (candidates, status).
    """
    try:
        from services.watchlist_service import list_watchlists, load_watchlist

        all_lists = list_watchlists()
        if not all_lists:
            return [], "missing"

        candidates: list[dict] = []
        seen: set[str] = set()

        # ── Phase 1: load canonical social lookup once per collector call ─────
        # Keys: uppercase ticker → {backend_score, freshness_score, ...}
        # This is the ONLY authoritative social source for this collector.
        social_lookup: dict[str, dict] = _load_social_backend_ranked()
        # Normalize backend_score across the ranked list for 0-1 scaling
        _soc_max = max(
            (float(v.get("backend_score") or 0) for v in social_lookup.values()),
            default=1.0,
        )
        _soc_max = max(_soc_max, 1.0)

        for meta in all_lists[:5]:      # cap at 5 watchlists to stay fast
            wl_id = meta.get("id")
            if not wl_id:
                continue
            store = load_watchlist(wl_id)
            if not store:
                continue

            sections = store.get("analysis", {}).get("sections", [])

            # Prefer updated_at (last re-analysis) over saved_at (initial creation)
            # Watchlist data is user-curated; apply a soft staleness floor — never
            # fully exclude it just because the Claude analysis is weeks old.
            ts_str = store.get("updated_at") or store.get("saved_at") or ""
            source_ts: float | None = None
            try:
                from dateutil import parser as _dp
                source_ts = _dp.parse(ts_str).timestamp() if ts_str else None
            except Exception:
                pass

            age_s = _source_age_s(source_ts)
            sf    = max(_staleness_factor(age_s), 0.55)  # floor: always use watchlist data

            for section in sections:
                for row in section.get("tickers", []):
                    sym = (row.get("symbol") or "").upper().strip()
                    if not sym or sym in seen:
                        continue
                    seen.add(sym)

                    # ── Stage / setup classification ─────────────────────────
                    stage       = row.get("stage")          # int 1-4 (Weinstein)
                    stage_label = (row.get("stage_label") or "").lower()
                    setup_type  = (row.get("setup_type") or row.get("pattern") or "").lower()
                    action_str  = (row.get("action") or "").lower()  # "strong buy", "buy", etc.
                    action_note = row.get("action_note") or ""

                    # ── TA signal (prefer technical_score over sentiment text) ─
                    tech_score = row.get("technical_score")   # 0-100
                    ta_sig: float | None = None

                    # Phase 2: Freshness gate — technical_score freshness is
                    # proxied by the parent watchlist store's updated_at age.
                    # If age > 24h, omit the numeric score; fall through to the
                    # action-label heuristic so the available-weight normalizer
                    # handles the missing signal cleanly.
                    if tech_score is not None:
                        if age_s is None:
                            _TECH_FRESH_DIAG["technical_missing_timestamp"] += 1
                            # Keep the score but note missing ts
                            try:
                                ta_sig = _clamp01(float(tech_score) / 100.0)
                            except Exception:
                                pass
                        elif age_s > _TECH_MAX_AGE_S:
                            _TECH_FRESH_DIAG["technical_stale_omitted"] += 1
                            tech_score = None  # omit stale score; use heuristic below
                        else:
                            _TECH_FRESH_DIAG["technical_fresh_used"] += 1
                            try:
                                ta_sig = _clamp01(float(tech_score) / 100.0)
                            except Exception:
                                pass
                    else:
                        _TECH_FRESH_DIAG["technical_missing"] += 1
                    if ta_sig is None:
                        # Fallback: parse action label
                        if "strong buy" in action_str or "bullish" in action_str:
                            ta_sig = 0.82
                        elif "buy" in action_str:
                            ta_sig = 0.68
                        elif "hold" in action_str or "neutral" in action_str:
                            ta_sig = 0.50
                        elif "sell" in action_str or "bearish" in action_str:
                            ta_sig = 0.25
                        else:
                            sentiment = (row.get("sentiment") or "").lower()
                            if "bullish" in sentiment or "buy" in sentiment:
                                ta_sig = 0.68
                            elif "bearish" in sentiment or "sell" in sentiment:
                                ta_sig = 0.28
                            elif sentiment in ("neutral", "watch"):
                                ta_sig = 0.50

                    # Stage modifier for TA signal
                    if stage is not None:
                        try:
                            st = int(stage)
                            if st == 2:   # advancing — boost
                                ta_sig = min(1.0, (ta_sig or 0.55) + 0.12)
                            elif st == 1: # base building — modest boost
                                ta_sig = min(1.0, (ta_sig or 0.50) + 0.06)
                            elif st == 3: # topping — penalize
                                ta_sig = max(0.0, (ta_sig or 0.40) - 0.10)
                            elif st == 4: # downtrend — hard penalize
                                ta_sig = max(0.0, (ta_sig or 0.30) - 0.20)
                        except Exception:
                            pass

                    # ── Relative volume signal ────────────────────────────────
                    vol_ratio = row.get("volume_ratio")   # e.g. 1.5 = 1.5x avg
                    vol_mc    = row.get("vol_mc_ratio")
                    rel_vol_sig: float | None = None
                    if vol_ratio is not None:
                        try:
                            # 0.5x→0.25, 1.0x→0.50, 2.0x→0.75, 4.0x→1.0
                            rel_vol_sig = _clamp01(float(vol_ratio) / 4.0)
                        except Exception:
                            pass
                    elif vol_mc is not None:
                        try:
                            rel_vol_sig = _clamp01(float(vol_mc) / 0.30)
                        except Exception:
                            pass

                    # ── Social signal — Phase 1: canonical provenance fix ─────
                    # NEVER use row.rs_score as social — it may be price RS (8w
                    # vs SPY, scale ~-20 to +20) or social rank (0-100) depending
                    # on which producer last wrote it.  Use ONLY _backend_ranked
                    # from x_consensus_weekly.json (social_lookup built above).
                    rs_score = row.get("rs_score")  # kept for display/evidence only
                    social_sig: float | None = None
                    _soc_entry = social_lookup.get(sym)
                    if _soc_entry is not None:
                        _bs = float(_soc_entry.get("backend_score") or 0)
                        social_sig = _clamp01(_bs / _soc_max)
                        if _soc_entry.get("has_top_conviction"):
                            social_sig = min(1.0, social_sig * 1.15)
                        _RS_PROV_DIAG["symbols_resolved_social_rank"] += 1
                        if rs_score is not None:
                            _RS_PROV_DIAG["symbols_legacy_rs_dropped"] += 1
                    else:
                        social_sig = None   # missing — normalizer handles it
                        _RS_PROV_DIAG["symbols_missing_canonical_social"] += 1
                        if rs_score is not None:
                            _RS_PROV_DIAG["symbols_legacy_rs_dropped"] += 1

                    # ── Theme signal ──────────────────────────────────────────
                    theme_id  = row.get("canonical_theme_id") or row.get("theme_source")
                    theme_sig = 0.6 if theme_id else None

                    # ── Momentum from returns ─────────────────────────────────
                    # Use 1d + 5d/7d returns as news/momentum proxy
                    chg_1d = row.get("change") or row.get("change_pct_1d") or row.get("change_pct")
                    chg_7d = row.get("change_7d") or row.get("change_5d")
                    news_sig: float | None = None
                    if chg_1d is not None:
                        try:
                            chg_f = float(chg_1d)
                            news_sig = _clamp01((chg_f + 5.0) / 10.0)
                        except Exception:
                            pass
                    if news_sig is None and chg_7d is not None:
                        try:
                            chg_f = float(chg_7d)
                            news_sig = _clamp01((chg_f + 10.0) / 20.0)
                        except Exception:
                            pass

                    # ── Setup bucket classification ───────────────────────────
                    setup_bucket = "watchlist_general"
                    try:
                        st = int(stage) if stage is not None else 0
                        sp = setup_type
                        if st in (1, 2) and ("base" in sp or "break" in sp or st == 2):
                            setup_bucket = "stage_1_to_2_base"
                        elif "cup" in sp or "handle" in sp:
                            setup_bucket = "cup_handle_watch"
                        elif "breakout" in sp or ("break" in sp and "breakdown" not in sp):
                            setup_bucket = "early_breakout"
                        elif "momentum" in sp or "trend_cont" in sp:
                            setup_bucket = "momentum_resumption"
                        elif "dip" in sp or "reversal" in sp or "reclaim" in sp:
                            setup_bucket = "dip_reversal_watch"
                        elif st == 3:
                            setup_bucket = "extended_momentum"
                    except Exception:
                        pass

                    # ── Extension risk (stage 3 or very high tech score) ──────
                    extension_risk = "high" if (
                        (stage is not None and int(stage or 0) >= 3) or
                        (tech_score is not None and float(tech_score or 0) > 88)
                    ) else "low"

                    # ── Direction (stocks: long or watch only — never short) ───
                    # Low TA signal or bearish sentiment → watch, not short.
                    # Overbought RSI / stage 3 → entry quality note, not short thesis.
                    direction = "watch"
                    if ta_sig is not None and ta_sig >= 0.65:
                        direction = "long"
                    # ta_sig <= 0.32 → watch (not short); entry_quality will reflect it

                    # Sanitize action_note text — remove short-biased language
                    # before it propagates to summary/evidence on the long board.
                    action_note = _clean_stock_text(action_note)

                    # ── Evidence ─────────────────────────────────────────────
                    evidence: list[str] = []
                    if action_note:
                        evidence.append(f"Watchlist: {action_note[:120]}")
                    if stage is not None and stage_label:
                        evidence.append(f"Stage {stage} — {stage_label}")
                    elif stage is not None:
                        evidence.append(f"Stage {stage}")
                    if setup_type and setup_type != "watchlist_general":
                        evidence.append(f"Setup: {setup_type[:60]}")
                    if rs_score is not None:
                        evidence.append(f"RS: {float(rs_score):.0f}/100")
                    catalyst = row.get("catalyst") or ""
                    if catalyst:
                        evidence.append(f"Catalyst: {catalyst[:100]}")

                    risks: list[str] = []
                    risk_level = row.get("risk_level") or ""
                    if risk_level:
                        risks.append(f"Risk: {risk_level}")
                    if extension_risk == "high":
                        risks.append("Extended near-term — avoid chasing vertical moves; better entry on consolidation or controlled pullback")

                    # Apply staleness factor to signals
                    def _sf(v: float | None) -> float | None:
                        return round(v * sf, 4) if v is not None else None

                    c = _make_candidate(
                        sym,
                        name=row.get("name"),
                        asset_type="stock",
                        direction=direction,
                        timeframe="2-10d",
                        theme=row.get("canonical_theme_name") or theme_id,
                        sector=row.get("sector"),
                        summary=action_note[:200] if action_note else f"{sym} watchlist ({setup_bucket})",
                        signals={
                            "theme":      _sf(theme_sig),
                            "ta":         _sf(ta_sig),
                            "rel_volume": _sf(rel_vol_sig),
                            "news":       _sf(news_sig),
                            "social":     _sf(social_sig),
                        },
                        evidence=evidence,
                        risks=risks,
                        source_pages=["watchlist"],
                    )
                    c["setup_bucket"]    = setup_bucket
                    c["extension_risk"]  = extension_risk
                    c["timing_quality"]  = "high" if setup_bucket in (
                        "stage_1_to_2_base", "early_breakout", "dip_reversal_watch"
                    ) else "medium"
                    candidates.append(c)

        symbols_ok = len(candidates) > 0
        status = "ok" if symbols_ok else "missing"
        if symbols_ok and sf < 1.0:
            status = "stale"

        wl_meta = {
            "symbols_status":    "ok" if symbols_ok else "missing",
            "signal_status":     "ok" if sf >= 1.0 else ("stale" if symbols_ok else "missing"),
            "symbol_count":      len(candidates),
            "signal_age_seconds": int(age_s) if age_s is not None else None,
        }
        return candidates, status, wl_meta

    except Exception as exc:
        print(f"[daily-alpha] skipped_source=watchlist reason={exc!r}")
        return [], "error", {
            "symbols_status": "error", "signal_status": "error",
            "symbol_count": 0, "signal_age_seconds": None,
        }


# ─────────────────────────────────────────────────────────────────────────────
# 2. Portfolio collector
# ─────────────────────────────────────────────────────────────────────────────

def collect_portfolio_cache_candidates() -> tuple[list[dict], str]:
    """
    Read active holdings from Neon portfolio_holdings table.
    Zero external API calls — pure DB read.
    """
    try:
        from data.portfolio_store import load_active_holdings

        holdings = load_active_holdings()
        if not holdings:
            return [], "missing"

        candidates: list[dict] = []
        for h in holdings:
            sym = (h.get("ticker") or h.get("symbol") or "").upper().strip()
            if not sym:
                continue

            # Fundamentals proxy from avg_cost vs current (if available)
            avg_cost     = h.get("avg_cost")
            current_price = h.get("current_price") or h.get("price")
            fund_sig: float | None = None
            if avg_cost and current_price:
                try:
                    pnl_pct = (float(current_price) - float(avg_cost)) / float(avg_cost)
                    fund_sig = _clamp01((pnl_pct + 0.50) / 1.00)   # [-50%, +50%] → [0, 1]
                except Exception:
                    pass

            asset_type = str(h.get("asset_type") or "stock").lower()
            if asset_type not in ("stock", "etf", "crypto", "perp"):
                asset_type = "stock"

            c = _make_candidate(
                sym,
                asset_type=asset_type,
                direction="long",
                timeframe="position",
                summary=f"{sym} active portfolio position",
                signals={"fundamentals": fund_sig},
                evidence=[f"Portfolio: {h.get('shares','')} shares @ avg ${avg_cost or '?'}"],
                source_pages=["portfolio"],
            )
            candidates.append(c)

        return candidates, "ok" if candidates else "missing"

    except Exception as exc:
        print(f"[daily-alpha] skipped_source=portfolio reason={exc!r}")
        return [], "error"


# ─────────────────────────────────────────────────────────────────────────────
# 3. Social / X Consensus collector
# ─────────────────────────────────────────────────────────────────────────────

def collect_social_screener_cache_candidates() -> tuple[list[dict], str]:
    """
    Read x_consensus_weekly.json (_backend_ranked array).
    Pure disk read — zero provider calls.
    """
    try:
        raw = _load_json_safe(_X_CONSENSUS_PATH)
        if not raw:
            return [], "missing"

        generated_at = raw.get("generated_at")
        source_ts: float | None = None
        try:
            from dateutil import parser as _dp
            source_ts = _dp.parse(generated_at).timestamp() if generated_at else None
        except Exception:
            pass

        age_s = _source_age_s(source_ts)
        sf    = _staleness_factor(age_s)
        if sf == 0.0:
            print("[daily-alpha] skipped_source=social reason=data_too_stale")
            return [], "stale"

        ranked = raw.get("_backend_ranked") or raw.get("top_tickers") or []
        if not ranked:
            return [], "missing"

        # Normalize backend_score — empirically max ~10, treat 10 as ceiling
        max_score = max((float(r.get("backend_score") or 0) for r in ranked), default=1.0)
        max_score = max(max_score, 1.0)

        candidates: list[dict] = []
        for row in ranked[:50]:
            sym = (row.get("ticker") or row.get("symbol") or "").upper().strip()
            if not sym:
                continue

            bs = float(row.get("backend_score") or 0)
            social_sig = _clamp01(bs / max_score * sf)

            conviction = row.get("has_top_conviction", False)
            if conviction:
                social_sig = min(1.0, float(social_sig) * 1.15)

            recency_days = row.get("recency_days_min")
            news_sig: float | None = None
            if recency_days is not None:
                news_sig = _clamp01(1.0 - float(recency_days) / 30.0)
                news_sig = round(news_sig * sf, 4)

            evidence: list[str] = []
            fragments = row.get("thesis_fragments") or []
            for frag in fragments[:2]:
                txt = frag.get("text") or ""
                if txt:
                    evidence.append(f"X/{frag.get('handle','')}: {txt[:120]}")
            cats = row.get("catalyst_list") or []
            if cats:
                evidence.append(f"Catalysts: {', '.join(str(c) for c in cats[:4])}")

            rationale = (
                row.get("rationale")
                or (fragments[0].get("text") if fragments else "")
                or f"{sym} X consensus mention"
            )

            c = _make_candidate(
                sym,
                direction="long",
                timeframe="2-10d",
                summary=str(rationale)[:220],
                signals={
                    "social": round(float(social_sig), 4),
                    "news":   news_sig,
                },
                evidence=evidence,
                source_pages=["social"],
            )
            candidates.append(c)

        status = "ok" if candidates else "missing"
        if sf < 1.0:
            status = "stale"
        return candidates, status

    except Exception as exc:
        print(f"[daily-alpha] skipped_source=social reason={exc!r}")
        return [], "error"


# ─────────────────────────────────────────────────────────────────────────────
# 4. Themes / Sector RS collector
# ─────────────────────────────────────────────────────────────────────────────

def collect_themes_cache_candidates() -> tuple[list[dict], str]:
    """
    Read themes_rs_lkg.json; for each top-performing theme, emit its
    proxy symbols as candidates carrying a theme-RS signal.
    Pure disk read — zero provider calls.
    """
    try:
        raw = _load_json_safe(_THEMES_LKG_PATH)
        if not raw:
            return [], "missing"

        rows = raw.get("rows") or (raw if isinstance(raw, list) else [])
        if not rows:
            return [], "missing"

        schema_raw = raw.get("_schema", {})
        schema = schema_raw if isinstance(schema_raw, dict) else {}
        last_updated_str = schema.get("updated_at") or schema.get("computed_at")

        # Fall back to file mtime if the schema dict has no timestamp
        source_ts: float | None = None
        try:
            from dateutil import parser as _dp
            source_ts = _dp.parse(last_updated_str).timestamp() if last_updated_str else None
        except Exception:
            pass
        if source_ts is None:
            try:
                source_ts = os.path.getmtime(str(_THEMES_LKG_PATH))
            except Exception:
                pass

        age_s = _source_age_s(source_ts)
        sf    = _staleness_factor(age_s)
        if sf == 0.0:
            print("[daily-alpha] skipped_source=themes reason=data_too_stale")
            return [], "stale"

        # Sort by rs_score descending, take top 20 themes
        top_themes = sorted(rows, key=lambda r: float(r.get("rs_score") or 0), reverse=True)[:20]

        candidates: list[dict] = []
        seen_syms: set[str] = set()

        for theme_row in top_themes:
            rs_score = float(theme_row.get("rs_score") or 0)
            if rs_score < 40:
                continue   # skip weak themes

            theme_sig = _clamp01(rs_score / 100.0 * sf)
            stage     = theme_row.get("stage")
            theme_id  = theme_row.get("theme_id", "")
            theme_name = theme_row.get("display_name") or theme_id

            # Stage 2 (advancing) → rs boost flag
            rs_boost = (stage == 2)

            perf = theme_row.get("performance") or {}
            perf_1d = perf.get("1D")
            news_sig: float | None = None
            if perf_1d is not None:
                try:
                    news_sig = _clamp01((float(perf_1d) + 3.0) / 6.0 * sf)
                except Exception:
                    pass

            # Stage → TA proxy
            ta_sig: float | None = None
            if stage == 2:
                ta_sig = round(0.70 * sf, 4)    # advancing
            elif stage == 1:
                ta_sig = round(0.55 * sf, 4)    # base building
            elif stage == 3:
                ta_sig = round(0.40 * sf, 4)    # topping
            elif stage == 4:
                ta_sig = round(0.20 * sf, 4)    # downtrend

            proxy_syms = theme_row.get("proxy_symbols") or []
            leaders    = [l.get("symbol") or l if isinstance(l, dict) else l
                          for l in (theme_row.get("leaders") or [])][:3]
            priority   = leaders + [s for s in proxy_syms if s not in leaders]

            for sym in priority[:6]:
                sym = str(sym).upper().strip()
                if not sym or sym in seen_syms:
                    continue
                seen_syms.add(sym)

                stage_label = theme_row.get("stage_label") or f"Stage {stage}"
                summary = (
                    f"{sym} — {theme_name} ({stage_label}, RS={rs_score:.0f})"
                )
                evidence = [
                    f"Theme RS {rs_score:.1f}/100 — {theme_name}",
                    f"Stage: {stage_label}",
                ]
                if perf_1d is not None:
                    evidence.append(f"1D perf: {perf_1d:+.2f}%")

                c = _make_candidate(
                    sym,
                    direction="long" if (stage or 0) <= 2 else "watch",
                    timeframe="swing",
                    theme=theme_id,
                    sector=theme_row.get("parent_sector"),
                    summary=summary,
                    signals={
                        "theme":      round(float(theme_sig), 4),
                        "ta":         ta_sig,
                        "news":       news_sig,
                    },
                    evidence=evidence,
                    source_pages=["themes"],
                )
                c["_rs_boost"] = rs_boost
                candidates.append(c)

        status = "ok" if candidates else "missing"
        if sf < 1.0:
            status = "stale"
        return candidates, status

    except Exception as exc:
        print(f"[daily-alpha] skipped_source=themes reason={exc!r}")
        return [], "error"


# ─────────────────────────────────────────────────────────────────────────────
# 5. Strategy / TA screener collector
# ─────────────────────────────────────────────────────────────────────────────

def collect_strategy_cache_candidates() -> tuple[list[dict], str]:
    """
    Read strategy_screener_lkg.json.
    Pure disk read — zero provider calls.
    """
    try:
        raw = _load_json_safe(_STRATEGY_LKG_PATH)
        if not raw:
            return [], "missing"

        generated_at = raw.get("generated_at")
        source_ts: float | None = None
        try:
            from dateutil import parser as _dp
            source_ts = _dp.parse(generated_at).timestamp() if generated_at else None
        except Exception:
            pass

        age_s = _source_age_s(source_ts)
        sf    = _staleness_factor(age_s)
        if sf == 0.0:
            print("[daily-alpha] skipped_source=strategy reason=data_too_stale")
            return [], "stale"

        results = raw.get("results") or []
        if not results:
            return [], "missing"

        # Regime context from the snapshot itself
        rc = raw.get("regime_context") or {}

        candidates: list[dict] = []
        for row in results:
            sym = (row.get("ticker") or "").upper().strip()
            if not sym:
                continue

            grade      = row.get("grade")
            blend_score = float(row.get("best_blend_score") or 0)   # 0-100
            bottleneck  = float(row.get("bottleneck_criticality_score") or 0)  # 0-100
            chain_role  = row.get("chain_role_type") or ""
            theme_name  = row.get("theme") or ""
            themes      = row.get("themes") or ([theme_name] if theme_name else [])

            ta_sig    = _clamp01(blend_score / 100.0 * sf)
            fund_sig  = _grade_to_score(grade)
            if fund_sig is not None:
                fund_sig = round(fund_sig * sf, 4)

            theme_sig: float | None = None
            if themes:
                # Use bottleneck score as theme-strength signal
                theme_sig = _clamp01(bottleneck / 100.0 * sf)

            evidence: list[str] = []
            summary_txt = row.get("one_line_summary") or row.get("thesis_summary") or ""
            if summary_txt:
                evidence.append(f"Strategy: {summary_txt[:140]}")
            why_now = row.get("why_now") or []
            if isinstance(why_now, list) and why_now:
                evidence.append(f"Why now: {why_now[0][:120]}")
            chain_layers = row.get("chain_layers") or []
            if chain_layers:
                evidence.append(f"Chain role: {chain_role} ({', '.join(str(l) for l in chain_layers[:2])})")

            risks: list[str] = []
            breaks = row.get("what_would_break_thesis")
            if isinstance(breaks, list) and breaks:
                risks.append(str(breaks[0])[:120])
            elif isinstance(breaks, str) and breaks:
                risks.append(breaks[:120])
            crowding = row.get("crowding_flags") or []
            if crowding:
                risks.append(f"Crowding: {', '.join(str(f) for f in crowding[:2])}")

            is_anchor = row.get("is_anchor", False)
            c = _make_candidate(
                sym,
                name=row.get("company_name"),
                direction="long",
                timeframe="2-10d",
                theme=theme_name or (themes[0] if themes else None),
                sector=row.get("chain_role_type"),
                setup_type=chain_role,
                summary=summary_txt[:200] if summary_txt else f"{sym} strategy screener candidate",
                signals={
                    "ta":           ta_sig,
                    "fundamentals": fund_sig,
                    "theme":        theme_sig,
                },
                evidence=evidence,
                risks=risks,
                source_pages=["strategy"],
            )
            if is_anchor:
                c["_rs_boost"] = True
            candidates.append(c)

        status = "ok" if candidates else "missing"
        if sf < 1.0:
            status = "stale"
        return candidates, status

    except Exception as exc:
        print(f"[daily-alpha] skipped_source=strategy reason={exc!r}")
        return [], "error"


# ─────────────────────────────────────────────────────────────────────────────
# 6. Catalyst / Earnings collector
# ─────────────────────────────────────────────────────────────────────────────

def collect_catalyst_cache_candidates() -> tuple[list[dict], str]:
    """
    Read current + adjacent week earnings snap disk files.
    Window: [-10d, +21d] from today.
      • Upcoming events (0..+21d): scored by importance + proximity.
      • Recent earnings (-10d..0d): scored by beat/miss magnitude + surprise %.
    Pure disk read — zero provider calls.
    """
    try:
        today_ord = date.today().toordinal()

        all_snap_files = sorted(
            glob.glob(str(_DATA_DIR / "earnings_snap_202*.json"))
        )
        if not all_snap_files:
            return [], "missing"

        # Select snaps whose filename date range overlaps with [today-10, today+21].
        import re as _re
        _snap_pat = _re.compile(r"earnings_snap_(\d{8})_(\d{8})\.json$")
        target_start = today_ord - 10
        target_end   = today_ord + 21

        snap_files: list[str] = []
        for fp in all_snap_files:
            m = _snap_pat.search(os.path.basename(fp))
            if not m:
                continue
            try:
                f_start = date(int(m.group(1)[:4]), int(m.group(1)[4:6]), int(m.group(1)[6:])).toordinal()
                f_end   = date(int(m.group(2)[:4]), int(m.group(2)[4:6]), int(m.group(2)[6:])).toordinal()
            except Exception:
                continue
            if f_end >= target_start and f_start <= target_end:
                snap_files.append(fp)

        # Fallback: if no files matched take 4 most recent
        if not snap_files:
            snap_files = all_snap_files[-4:]

        all_events: list[dict] = []
        for fp in snap_files:
            raw = _load_json_safe(fp)
            if not raw:
                continue
            events = raw.get("allEvents") or raw.get("events") or []
            all_events.extend(events)

        if not all_events:
            return [], "missing"

        # Snap files are pre-generated; validity is determined by event dates.
        sf = 1.0

        candidates: list[dict] = []
        seen: set[str] = set()

        for ev in all_events:
            sym = (ev.get("symbol") or "").upper().strip()
            if not sym or sym in seen:
                continue

            ev_date_str = ev.get("date") or ""
            if not ev_date_str:
                continue

            try:
                ev_ord    = date.fromisoformat(ev_date_str).toordinal()
                days_away = ev_ord - today_ord
            except Exception:
                continue

            # Window: recent earnings [-10d, 0d) and upcoming [-0d, +21d]
            if not (-10 <= days_away <= 21):
                continue

            seen.add(sym)

            importance    = float(ev.get("importanceScore") or ev.get("importance") or 50)
            is_anchor     = ev.get("isThemeAnchor", False)
            is_bottleneck = ev.get("isBottleneck", False)
            theme_tags    = ev.get("themeTags") or []
            ev_type       = ev.get("eventType") or "earnings"
            ev_label      = ev.get("eventLabel") or ev_type
            time_label    = ev.get("time") or ""

            theme_sig: float | None = None
            if is_anchor or theme_tags:
                theme_sig = round(0.65 * sf, 4)

            # ── Branch: recent earnings (days_away < 0) ───────────────────────
            if days_away < 0:
                eps_actual  = ev.get("epsActual")
                eps_est     = ev.get("epsEstimated")
                rev_actual  = ev.get("revenueActual")
                rev_est     = ev.get("revenueEstimated")
                surprise_pct = ev.get("surprisePercent")
                title        = ev.get("title") or ""

                # Only score recent events if we have actual results
                if eps_actual is None and rev_actual is None:
                    continue

                is_beat = False
                is_miss = False
                if eps_actual is not None and eps_est is not None:
                    try:
                        is_beat = float(eps_actual) >= float(eps_est)
                        is_miss = not is_beat
                    except Exception:
                        pass
                elif "(beat)" in title.lower():
                    is_beat = True
                elif "(miss)" in title.lower():
                    is_miss = True

                # Score from surprise magnitude
                surp_factor = 0.55   # base for beats
                if surprise_pct is not None:
                    try:
                        sp = abs(float(surprise_pct))
                        # 0%→0.55, 5%→0.65, 15%→0.80, 30%+→1.0
                        surp_factor = _clamp01(0.55 + sp / 50.0)
                    except Exception:
                        pass

                # Recency decay: older beats lose relevance
                recency = max(0.3, (10 + days_away) / 10.0)   # days_away is negative

                if is_beat:
                    cat_sig  = round(_clamp01(importance / 100.0 * 0.5 + surp_factor * 0.5) * recency, 4)
                    direction = "long"
                    result_label = "Beat"
                elif is_miss:
                    cat_sig   = round(_clamp01(importance / 100.0 * 0.4 + 0.30) * recency, 4)
                    direction  = "watch"   # stocks: never short — miss → watch for stabilization
                    result_label = "Miss"
                else:
                    # Reported but no clear beat/miss — watch
                    cat_sig      = round(_clamp01(importance / 100.0 * 0.4) * recency, 4)
                    direction    = "watch"
                    result_label = "Reported"

                evidence = [f"{ev_label} {result_label} on {ev_date_str} (T{days_away:+d}d)"]
                if eps_actual is not None and eps_est is not None:
                    try:
                        evidence.append(f"EPS: ${float(eps_actual):.2f} vs est ${float(eps_est):.2f}")
                    except Exception:
                        pass
                if surprise_pct is not None:
                    try:
                        evidence.append(f"Surprise: {float(surprise_pct):+.1f}%")
                    except Exception:
                        pass
                if rev_actual is not None and rev_est is not None:
                    try:
                        rev_surp_pct = (float(rev_actual) - float(rev_est)) / float(rev_est) * 100
                        evidence.append(f"Rev: ${float(rev_actual)/1e6:.0f}M vs est ${float(rev_est)/1e6:.0f}M ({rev_surp_pct:+.1f}%)")
                    except Exception:
                        pass
                if theme_tags:
                    evidence.append(f"Themes: {', '.join(str(t) for t in theme_tags[:3])}")

                trigger = f"{ev_label} {result_label} T{days_away:+d}d"
                summary = f"{sym} Earnings {result_label} T{days_away:+d}d — {ev.get('companyName','')[:55]}"

                c = _make_candidate(
                    sym,
                    name=ev.get("companyName"),
                    direction=direction,
                    timeframe="intraday",
                    theme=theme_tags[0] if theme_tags else None,
                    sector=ev.get("sector"),
                    summary=summary,
                    trigger=trigger,
                    signals={
                        "catalysts": cat_sig,
                        "theme":     theme_sig,
                    },
                    evidence=evidence,
                    source_pages=["catalysts"],
                )
                c["catalyst_window"]   = "recent_earnings"
                c["earnings_result"]   = result_label
                c["days_to_earnings"]  = days_away
                if surprise_pct is not None:
                    try:
                        c["eps_surprise_pct"] = round(float(surprise_pct), 2)
                    except Exception:
                        pass
                if is_anchor or is_bottleneck:
                    c["_rs_boost"] = True
                candidates.append(c)

            # ── Branch: upcoming earnings (days_away >= 0) ────────────────────
            else:
                eps_est = ev.get("epsEstimated")
                rev_est = ev.get("revenueEstimated")

                # Proximity boost: tighter window scores higher
                # Peak window 0-7d; extends to 21d with decay
                if days_away <= 7:
                    proximity_boost = max(0.0, (7 - days_away) / 7.0)
                else:
                    # 8-21d: weaker proximity
                    proximity_boost = max(0.0, (21 - days_away) / 42.0)

                cat_sig = _clamp01((importance / 100.0) * 0.70 + proximity_boost * 0.30)
                cat_sig = round(cat_sig * sf, 4)

                evidence = [f"{ev_label} on {ev_date_str} (T{days_away:+d}d)"]
                if eps_est is not None:
                    try:
                        evidence.append(f"EPS est: ${float(eps_est):.2f}")
                    except Exception:
                        pass
                if rev_est is not None:
                    try:
                        evidence.append(f"Rev est: ${float(rev_est)/1e6:.0f}M")
                    except Exception:
                        pass
                if theme_tags:
                    evidence.append(f"Themes: {', '.join(str(t) for t in theme_tags[:3])}")

                trigger = (f"{ev_label} in {days_away}d"
                           + (f" ({time_label})" if time_label else ""))
                window  = ("intraday" if days_away <= 1
                           else "2-10d" if days_away <= 10
                           else "10-21d")

                c = _make_candidate(
                    sym,
                    name=ev.get("companyName"),
                    direction="watch",
                    timeframe=window,
                    theme=theme_tags[0] if theme_tags else None,
                    sector=ev.get("sector"),
                    summary=f"{sym} {ev_label} T{days_away:+d}d — {ev.get('companyName','')[:60]}",
                    trigger=trigger,
                    signals={
                        "catalysts": cat_sig,
                        "theme":     theme_sig,
                    },
                    evidence=evidence,
                    source_pages=["catalysts"],
                )
                c["catalyst_window"]  = ("pre_earnings_core" if days_away <= 7
                                         else "pre_earnings_extended")
                c["days_to_earnings"] = days_away
                if is_anchor or is_bottleneck:
                    c["_rs_boost"] = True
                candidates.append(c)

        status = "ok" if candidates else "missing"
        return candidates, status

    except Exception as exc:
        print(f"[daily-alpha] skipped_source=catalysts reason={exc!r}")
        return [], "error"


# ─────────────────────────────────────────────────────────────────────────────
# 7. Options collector
# ─────────────────────────────────────────────────────────────────────────────

def collect_options_cache_candidates() -> tuple[list[dict], str]:
    """
    Read options_master_lkg_v1.json (and fallback files).
    Pure disk read — zero provider calls.
    """
    try:
        raw: dict | None = None
        source_path: Path | None = None

        for p in _OPTIONS_PATHS:
            if p.exists():
                d = _load_json_safe(p)
                if d and d.get("tickers"):
                    raw = d
                    source_path = p
                    break

        if not raw:
            return [], "missing"

        cached_at_str = raw.get("cached_at") or raw.get("updated_at")
        source_ts: float | None = None
        try:
            from dateutil import parser as _dp
            source_ts = _dp.parse(cached_at_str).timestamp() if cached_at_str else None
        except Exception:
            pass
        if source_ts is None and source_path:
            try:
                source_ts = os.path.getmtime(source_path)
            except Exception:
                pass

        age_s = _source_age_s(source_ts)
        sf    = _staleness_factor(age_s)
        if sf == 0.0:
            print("[daily-alpha] skipped_source=options reason=data_too_stale")
            return [], "stale"

        tickers = raw.get("tickers") or []
        candidates: list[dict] = []

        for row in tickers:
            if not isinstance(row, dict):
                continue
            sym = (row.get("ticker") or row.get("symbol") or "").upper().strip()
            if not sym:
                continue

            composite  = float(row.get("composite_score") or row.get("final_composite_score") or 0)
            conf_score = float(row.get("confidence_score") or 50)
            side_bias  = (row.get("side_bias") or "").lower()
            primary_sig = (row.get("primary_signal") or "").lower()

            opt_sig  = _clamp01(composite / 100.0 * sf)
            theme_sig: float | None = None
            if row.get("theme_name"):
                ts = float(row.get("regime_alignment_score") or 50)
                theme_sig = _clamp01(ts / 100.0 * sf)

            # Stocks/ETFs: bearish options signal → watch (not short); long bias enforced
            direction = "long" if side_bias == "bullish" else "watch"

            conf_label = "high" if conf_score >= 75 else \
                         "medium" if conf_score >= 50 else "low"

            evidence: list[str] = []
            thesis = row.get("thesis") or row.get("options_context_summary") or ""
            if thesis:
                evidence.append(f"Options: {str(thesis)[:150]}")
            if primary_sig:
                evidence.append(f"Signal: {primary_sig}")
            if row.get("theme_name"):
                evidence.append(f"Theme: {row['theme_name']} ({row.get('regime_alignment_label','')})")

            risks = []
            row_risks = row.get("risks") or []
            if isinstance(row_risks, list):
                risks = [str(r)[:100] for r in row_risks[:2]]
            elif isinstance(row_risks, str):
                risks = [row_risks[:100]]

            # PC ratio → additional signal
            pc = row.get("pc_ratio")
            ta_sig: float | None = None
            if pc is not None:
                try:
                    # Low put/call (< 0.7) = bullish; high (> 1.3) = bearish
                    pf = float(pc)
                    ta_sig = _clamp01(1.0 - (pf - 0.5) / 1.5) if 0.5 <= pf <= 2.0 else None
                    if ta_sig is not None:
                        ta_sig = round(ta_sig * sf, 4)
                except Exception:
                    pass

            c = _make_candidate(
                sym,
                direction=direction,
                confidence=conf_label,
                timeframe="intraday" if primary_sig in ("sweep", "block") else "2-10d",
                setup_type=primary_sig,
                theme=row.get("theme_name"),
                sector=row.get("category"),
                summary=str(thesis)[:200] if thesis else f"{sym} options flow: {primary_sig}",
                signals={
                    "options": opt_sig,
                    "theme":   theme_sig,
                    "ta":      ta_sig,
                },
                evidence=evidence,
                risks=risks,
                source_pages=["options"],
            )
            candidates.append(c)

        status = "ok" if candidates else "missing"
        if sf < 1.0:
            status = "stale"
        return candidates, status

    except Exception as exc:
        print(f"[daily-alpha] skipped_source=options reason={exc!r}")
        return [], "error"


# ─────────────────────────────────────────────────────────────────────────────
# 8. Hyperliquid / Crypto collector
# ─────────────────────────────────────────────────────────────────────────────

def collect_hyperliquid_cache_candidates() -> tuple[list[dict], str]:
    """
    ZERO external API calls. Uses only in-process matrix cache + disk OI snapshots.

    PRIMARY source: get_matrix_perps_snapshot() — matrix-ranked, multi-factor scored.
    OI velocity from hyperliquid_signal_snapshots.json is corroboration only.

    Confidence rules (matrix path):
      HIGH   → matrix_signal=LONG, structuralQuality>=0.45, momentum>=0.45,
                trend>=0.40, quality_gate=passed_tsm, no pump detected
      MEDIUM → matrix_signal in (LONG, WATCH), structuralQuality>=0.28,
                no pump, WATCH requires momentum+trend both >=0.70
      LOW    → everything else that passes hard gates

    Hard-exclude rules:
      • avoidScore>0.72 or collapseRiskScore>0.72
      • CROWDED/AVOID matrix signal for a long idea
      • pump detected AND structuralQuality < 0.35
      • cumulative penalty < 0.40 after soft gates

    OI-only fallback (matrix cache cold):
      • All candidates → direction=watch, confidence=medium
      • All signal values capped at 0.68 (~score ≤ 69/100)
      • hyperliquid_quality_gate = "matrix_cache_cold_oi_only"
      • Source status returned as "matrix_cache_cold"
    """
    try:
        # ── PRIMARY: matrix-ranked perps (in-process cache, zero API calls) ─────
        perps: list[dict] = []
        try:
            from services.hyperliquid.router import get_matrix_perps_snapshot as _gmp
            perps = _gmp()
        except Exception:
            pass

        # ── SECONDARY: OI velocity from disk snapshot (corroboration only) ───────
        oi_snap_vel: dict[str, float] = {}
        oi_snap_oi:  dict[str, float] = {}
        snap_sf = 0.85
        raw_snap = _load_json_safe(_HL_SNAP_PATH)
        if raw_snap:
            saved_at   = float(raw_snap.get("saved_at") or 0)
            snap_age_s = _source_age_s(saved_at if saved_at else None)
            snap_sf    = _staleness_factor(snap_age_s) or 0.85
            for coin, pts in (raw_snap.get("snapshots") or {}).items():
                if not pts or len(pts) < 2:
                    continue
                pts_s    = sorted(pts, key=lambda p: p.get("ts", 0))
                first_oi = float(pts_s[0].get("oi_usd") or 0)
                last_oi  = float(pts_s[-1].get("oi_usd") or 0)
                if first_oi > 0:
                    oi_snap_vel[coin.upper()] = (last_oi - first_oi) / first_oi
                    oi_snap_oi[coin.upper()]  = last_oi

        if not perps and not oi_snap_vel:
            return [], "missing"

        candidates:       list[dict] = []
        rejected_pump:    list[str]  = []
        rejected_crowded: list[str]  = []
        accepted_tsm:     list[str]  = []

        # ══════════════════════════════════════════════════════════════════════
        # PATH A: Matrix-backed candidates (rich signals, full confidence rules)
        # ══════════════════════════════════════════════════════════════════════
        seen_syms: set[str] = set()
        for asset in perps[:100]:
            sym = (asset.get("displaySymbol") or asset.get("coin") or "").upper().strip()
            if not sym or sym in seen_syms:
                continue
            seen_syms.add(sym)

            matrix_signal = (asset.get("matrixSignal") or "").upper()
            signal_dir    = (asset.get("signalDirection") or "watch").lower()

            composite    = float(asset.get("compositeSignal")        or 0)
            structural_q = float(asset.get("structuralQualityScore") or 0)
            momentum_sc  = float(asset.get("momentum")              or 0)
            trend_sc     = float(asset.get("trend")                 or 0)
            flow_sc      = float(asset.get("flow")                  or 0)
            avoid_sc     = float(asset.get("avoidScore")            or 0)
            exhaust_sc   = float(asset.get("exhaustionScore")       or 0)
            collapse_sc  = float(asset.get("collapseRiskScore")     or 0)

            funding_label = (asset.get("fundingLabel") or "").lower()
            market_type   = (asset.get("marketType")  or "perp").lower()

            oi_delta_5m  = asset.get("oiDelta5mPct")
            oi_delta_15m = asset.get("oiDelta15mPct")
            change_24h   = asset.get("change24hPct")

            oi_vel = oi_snap_vel.get(sym) or oi_snap_vel.get(sym.replace("-PERP", ""))
            oi_usd = oi_snap_oi.get(sym)  or oi_snap_oi.get(sym.replace("-PERP", "")) or 0

            # ── Hard-exclude gate 1: avoid/collapse ───────────────────────────
            if avoid_sc > 0.72 or collapse_sc > 0.72:
                rejected_pump.append(sym)
                continue

            # ── Hard-exclude gate 2: CROWDED/AVOID matrix signal for longs ────
            if matrix_signal in _CRYPTO_MATRIX_AVOID and signal_dir not in ("short", "bearish"):
                rejected_crowded.append(sym)
                continue

            # ── Pump detection (before hard-exclude gate 3) ───────────────────
            quality_gate  = "passed_tsm"
            penalty       = 1.0
            pump_detected = False

            if oi_delta_5m is not None and abs(oi_delta_5m) > _PUMP_OI_5M_PCT_THRESH:
                if structural_q < _MIN_STRUCT_Q_HIGH and trend_sc < 0.40:
                    pump_detected = True
                    quality_gate  = "failed_pump_filter"
                    penalty      *= 0.58

            if oi_delta_15m is not None and abs(oi_delta_15m) > _PUMP_OI_15M_PCT_THRESH:
                if structural_q < _MIN_STRUCT_Q_HIGH and momentum_sc < 0.35:
                    pump_detected = True
                    quality_gate  = "failed_pump_filter"
                    penalty      *= 0.55

            if change_24h is not None:
                chg24 = float(change_24h)
                if abs(chg24) > _PUMP_24H_NO_TSM and structural_q < _MIN_STRUCT_Q_HIGH:
                    pump_detected = True
                    quality_gate  = "failed_pump_filter"
                    penalty      *= 0.65

            # ── Hard-exclude gate 3: pump + structuralQuality too weak ─────────
            # Even soft-penalty cannot redeem a candidate that has a pump signal
            # and no structural foundation at all.
            if pump_detected and structural_q < _MIN_STRUCT_Q_HARD_EXCLUDE:
                rejected_pump.append(sym)
                continue

            # Drop if accumulated penalty is overwhelming
            if pump_detected and penalty < 0.40:
                rejected_pump.append(sym)
                continue

            # ── Soft-penalty: crowded funding ─────────────────────────────────
            if "crowded" in funding_label:
                if quality_gate == "passed_tsm":
                    quality_gate = "crowded_funding"
                penalty *= 0.80

            # ── Soft-penalty: exhaustion for long ideas ───────────────────────
            if exhaust_sc > 0.55 and signal_dir in ("long", "bullish"):
                if quality_gate == "passed_tsm":
                    quality_gate = "failed_sustained_momentum"
                penalty *= 0.75

            if quality_gate == "passed_tsm":
                accepted_tsm.append(sym)

            # ── Confidence derivation (matrix path) ───────────────────────────
            # HIGH: LONG signal + strong structural foundation + no red flags
            if (matrix_signal == "LONG"
                    and structural_q >= _MIN_STRUCT_Q_HIGH_CONF
                    and momentum_sc  >= 0.45
                    and trend_sc     >= 0.40
                    and quality_gate == "passed_tsm"
                    and not pump_detected):
                confidence_label = "high"
            # MEDIUM: LONG or WATCH + meets minimum quality bar
            elif (matrix_signal in ("LONG", "WATCH")
                  and structural_q >= _MIN_STRUCT_Q_HIGH
                  and not pump_detected
                  and quality_gate in ("passed_tsm", "crowded_funding")):
                if matrix_signal == "WATCH":
                    # WATCH → medium only if sustained momentum+trend are strong
                    confidence_label = "medium" if (momentum_sc >= 0.70 and trend_sc >= 0.70) else "low"
                else:
                    confidence_label = "medium"
            else:
                confidence_label = "low"

            # ── Score ─────────────────────────────────────────────────────────
            base = (composite * 0.40 + structural_q * 0.30
                    + momentum_sc * 0.15 + trend_sc * 0.10 + flow_sc * 0.05)

            if oi_vel is not None:
                if (oi_vel > 0.03 and signal_dir in ("long", "bullish")) or \
                   (oi_vel < -0.03 and signal_dir in ("short", "bearish")):
                    base = min(1.0, base * 1.08)

            oi_sig  = _clamp01(base * penalty)
            mom_sig = _clamp01(momentum_sc * penalty)
            vol_sig = _clamp01(composite * penalty)

            # ── Evidence ──────────────────────────────────────────────────────
            evidence = []
            mx_reason = asset.get("matrixSignalReason") or asset.get("matrixSignalDetail") or ""
            if mx_reason:
                evidence.append(f"Matrix: {str(mx_reason)[:120]}")
            if oi_vel is not None:
                oi_b = oi_usd / 1e9 if oi_usd else 0
                evidence.append(f"OI velocity: {oi_vel*100:+.1f}% (${oi_b:.2f}B)")
            if change_24h is not None:
                evidence.append(f"24h: {float(change_24h)*100:+.1f}%")
            fr = asset.get("funding")
            if fr is not None:
                evidence.append(f"Funding: {float(fr)*100:.4f}%/hr")
            if pump_detected:
                evidence.append(f"⚠ pump filter: {quality_gate}")

            # ── Direction ─────────────────────────────────────────────────────
            if signal_dir in ("long", "bullish"):
                direction = "long"
            elif signal_dir in ("short", "bearish"):
                direction = "short"
            else:
                direction = "watch"
            if pump_detected:
                direction = "watch"

            at = "perp" if market_type == "perp" else "crypto"
            c = _make_candidate(
                sym,
                asset_type=at,
                direction=direction,
                confidence=confidence_label,
                timeframe="intraday" if (oi_delta_5m or 0) > 5 else "2-10d",
                setup_type=asset.get("setupType") or "matrix_ranked",
                summary=(f"{sym} Matrix:{matrix_signal} "
                         f"Q={structural_q:.2f} Score={int(composite*100)}"),
                signals={
                    "hyperliquid": round(oi_sig, 4),
                    "momentum":    round(mom_sig, 4),
                    "rel_volume":  round(vol_sig, 4),
                },
                evidence=evidence,
                source_pages=["hyperliquid"],
            )
            c["hyperliquid_quality_gate"] = quality_gate
            c["tsm_quality"]             = round(structural_q, 4)
            c["matrix_signal"]           = matrix_signal
            candidates.append(c)

        # ══════════════════════════════════════════════════════════════════════
        # PATH B: OI-only fallback — matrix cache is cold (server cold start)
        # All candidates are watch/medium with score capped at ~69/100.
        # Prefer stock candidates — these are only included if no matrix data.
        # ══════════════════════════════════════════════════════════════════════
        oi_only_used = False
        if not candidates and oi_snap_vel and snap_sf > 0.0:
            oi_only_used = True
            oi_list = sorted(oi_snap_vel.items(), key=lambda x: abs(x[1]), reverse=True)
            for coin, velocity in oi_list[:20]:
                oi_usd  = oi_snap_oi.get(coin, 0)
                # Hard cap all signals at _OI_ONLY_MAX_SIGNAL so score ≤ ~69/100
                oi_sig  = min(_clamp01(min(abs(velocity) / 0.30, 1.0) * snap_sf), _OI_ONLY_MAX_SIGNAL)
                mom_sig = min(_clamp01(abs(velocity) / 0.20 * snap_sf), _OI_ONLY_MAX_SIGNAL)
                oi_b    = oi_usd / 1e9 if oi_usd else 0
                c = _make_candidate(
                    coin,
                    asset_type="perp",
                    direction="watch",       # never long/short without matrix
                    confidence="medium",     # hard cap — no matrix = no high confidence
                    timeframe="2-10d",
                    setup_type="oi_watch",
                    summary=f"{coin} OI vel {velocity*100:+.1f}% (matrix cache cold — watch only)",
                    signals={
                        "hyperliquid": round(oi_sig, 4),
                        "momentum":    round(mom_sig, 4),
                    },
                    evidence=[
                        f"OI velocity: {velocity*100:+.1f}% (${oi_b:.2f}B)",
                        "⚠ Matrix cache cold — no multi-factor confirmation available",
                    ],
                    source_pages=["hyperliquid"],
                )
                c["hyperliquid_quality_gate"] = "matrix_cache_cold_oi_only"
                c["tsm_quality"]             = None
                c["matrix_signal"]           = None
                candidates.append(c)

        # ── Diagnostic counters ───────────────────────────────────────────────
        _HL_DIAG["rejected_pump"]    = rejected_pump[:]
        _HL_DIAG["rejected_crowded"] = rejected_crowded[:]
        _HL_DIAG["accepted_tsm"]     = accepted_tsm[:]

        print(f"[daily-alpha] hyperliquid candidates={len(candidates)} "
              f"accepted_tsm={len(accepted_tsm)} rejected_pump={len(rejected_pump)} "
              f"rejected_crowded={len(rejected_crowded)} "
              f"oi_only_fallback={oi_only_used}")

        if not candidates:
            return [], "missing"

        status = "matrix_cache_cold" if oi_only_used else "ok"
        return candidates, status

    except Exception as exc:
        print(f"[daily-alpha] skipped_source=hyperliquid reason={exc!r}")
        return [], "error"


# ─────────────────────────────────────────────────────────────────────────────
# 9. Macro regime context  (NOT a candidate source — used as modifier)
# ─────────────────────────────────────────────────────────────────────────────

def collect_macro_cache_context() -> tuple[dict, str]:
    """
    Read cached macro regime from TTLCache or regime_engine module vars.
    Zero external calls — all paths read already-computed state.
    Returns (regime_dict, status).
    """
    regime_result: dict | None = None

    # Path 1: TTLCache write-through key
    try:
        from data.cache import cache as _cache
        regime_result = _cache.get("regime:current_v1")
    except Exception:
        pass

    # Path 2: module-level cache in regime_engine
    if not regime_result:
        try:
            import core.regime_engine as _re
            cached = _re._regime_cache
            if cached.get("result") and time.time() < cached.get("expires", 0):
                regime_result = cached["result"]
            elif _re._last_known_regime:
                regime_result = dict(_re._last_known_regime)
                regime_result["degraded"] = True
        except Exception:
            pass

    if not regime_result:
        return {
            "label":      "neutral",
            "summary":    "No cached regime data — neutral fallback",
            "drivers":    [],
            "confidence": 0.0,
        }, "missing"

    raw_label = regime_result.get("regime", "neutral")
    # Map internal labels to board labels
    label_map = {
        "risk_on":         "risk_on",
        "risk_off":        "risk_off",
        "neutral":         "neutral",
        "inflationary":    "risk_off",   # treat as risk-off for scoring
        "stagflationary":  "risk_off",
        "deflationary":    "risk_off",
    }
    label = label_map.get(raw_label, "neutral")

    confidence = float(regime_result.get("confidence") or 0.5)
    degraded   = regime_result.get("degraded", False)

    drivers: list[str] = []
    sigs = regime_result.get("signals") or {}
    if isinstance(sigs, dict):
        for k, v in list(sigs.items())[:4]:
            drivers.append(f"{k}: {v}")
    elif isinstance(sigs, list):
        drivers = [str(s) for s in sigs[:4]]

    regime = {
        "label":      label,
        "summary":    f"Regime: {raw_label} (confidence {confidence:.0%})" + (
                      " [degraded]" if degraded else ""
                      ),
        "drivers":    drivers,
        "confidence": confidence,
    }
    status = "stale" if degraded else "ok"
    return regime, status


# ─────────────────────────────────────────────────────────────────────────────
# Board builder
# ─────────────────────────────────────────────────────────────────────────────

def _apply_filters(
    candidates: list[dict],
    asset_type: str,
    scope: str,
    watchlist_syms: set[str],
    portfolio_syms: set[str],
) -> list[dict]:
    out = []
    for c in candidates:
        at = c.get("asset_type", "stock")
        if asset_type == "stocks" and at not in ("stock", "etf"):
            continue
        if asset_type == "crypto" and at not in ("crypto", "perp"):
            continue
        if scope == "watchlist" and c["symbol"] not in watchlist_syms:
            continue
        if scope == "portfolio" and c["symbol"] not in portfolio_syms:
            continue
        out.append(c)
    return out


def build_daily_alpha_board(
    limit: int = 10,
    asset_type: str = "all",
    scope: str = "all",
    refresh: bool = False,
    include_diagnostics: bool = False,
) -> dict:
    """
    Core entry-point.  Collects, scores, dedupes, ranks, and returns
    the top `limit` trade ideas from purely cached sources.

    external_api_calls is always 0.
    """
    t0 = time.time()

    # ── Aggregator TTL cache check ────────────────────────────────────────────
    ttl = _TTL_MARKET_HOURS if _is_market_hours() else _TTL_OFF_HOURS
    _BOARD_CACHE["ttl"] = ttl

    cache_age = time.time() - _BOARD_CACHE["ts"]
    cache_hit = (
        not refresh
        and _BOARD_CACHE["result"] is not None
        and cache_age < ttl
        # only exact same params hit cache; simple approach: cache is for default params
        and asset_type == "all"
        and scope == "all"
        and limit == 10
    )

    if cache_hit:
        result = dict(_BOARD_CACHE["result"])
        result["cache"] = {
            "hit":          True,
            "age_seconds":  int(cache_age),
            "ttl_seconds":  ttl,
            "stale_served": False,
        }
        print(f"[daily-alpha] cache_hit=True age={int(cache_age)}s")
        return result

    # ── Collect from all sources ─────────────────────────────────────────────
    sh = _SourceHealth()

    wl_cands,   wl_status,  wl_meta  = collect_watchlist_cache_candidates()
    port_cands, port_status          = collect_portfolio_cache_candidates()
    soc_cands,  soc_status           = collect_social_screener_cache_candidates()
    th_cands,   th_status            = collect_themes_cache_candidates()
    str_cands,  str_status           = collect_strategy_cache_candidates()
    cat_cands,  cat_status           = collect_catalyst_cache_candidates()
    opt_cands,  opt_status           = collect_options_cache_candidates()
    hl_cands,   hl_status            = collect_hyperliquid_cache_candidates()
    regime,     reg_status           = collect_macro_cache_context()

    sh.set("watchlist",   wl_status,   len(wl_cands))
    sh.set("portfolio",   port_status, len(port_cands))
    sh.set("social",      soc_status,  len(soc_cands))
    sh.set("themes",      th_status,   len(th_cands))
    sh.set("strategy",    str_status,  len(str_cands))
    sh.set("catalysts",   cat_status,  len(cat_cands))
    sh.set("options",     opt_status,  len(opt_cands))
    sh.set("hyperliquid", hl_status,   len(hl_cands))
    sh.set("macro",       reg_status,  0)

    all_raw = (
        wl_cands + port_cands + soc_cands +
        th_cands + str_cands + cat_cands +
        opt_cands + hl_cands
    )
    candidates_seen = len(all_raw)

    print(f"[daily-alpha] mode=cache_only external_api_calls=0 "
          f"candidates_seen={candidates_seen}")
    print(f"[daily-alpha] source_health={sh.health}")

    # ── Dedup (merge same symbol from multiple sources) ───────────────────────
    merged = _merge_candidates(all_raw)

    # ── Long-bias safety pass (board_mode = "long_watchlist") ────────────────
    # Hard rule: stocks/ETFs never get direction="short" on the main board.
    # Crypto/perp shorts are tracked separately in crypto_short_candidates.
    _LB_DIAG["stock_shorts_suppressed"]            = 0
    _LB_DIAG["stock_short_candidates_converted_to_watch"] = 0
    _LB_DIAG["stock_extension_notes_added"]        = 0
    _LB_DIAG["crypto_short_candidates"]            = []

    for _c in merged:
        _at = _c.get("asset_type", "stock")
        if _at in ("stock", "etf"):
            if _c.get("direction") == "short":
                _c["direction"] = "watch"
                _LB_DIAG["stock_short_candidates_converted_to_watch"] += 1
                _LB_DIAG["stock_shorts_suppressed"] += 1
            if _c.get("extension_risk") == "high":
                _LB_DIAG["stock_extension_notes_added"] += 1
            _set_stock_long_fields(_c)
        elif _at in ("crypto", "perp") and _c.get("direction") == "short":
            _LB_DIAG["crypto_short_candidates"].append(_c["symbol"])

    # ── Build scope filter sets ───────────────────────────────────────────────
    wl_syms   = {c["symbol"] for c in wl_cands}
    port_syms = {c["symbol"] for c in port_cands}

    # ── Apply query filters ───────────────────────────────────────────────────
    filtered = _apply_filters(merged, asset_type, scope, wl_syms, port_syms)

    # ── Score ─────────────────────────────────────────────────────────────────
    scored: list[dict] = []
    stocks_scored = 0
    crypto_scored = 0

    for c in filtered:
        s = _score_candidate(c, regime)
        if s <= 0.0:
            continue
        scored.append(c)
        if c.get("asset_type") in ("crypto", "perp"):
            crypto_scored += 1
        else:
            stocks_scored += 1

    candidates_qualified = len(scored)
    print(f"[daily-alpha] mode=cache_only external_api_calls=0 "
          f"provider_calls_blocked=true candidates_qualified={candidates_qualified} "
          f"stocks={stocks_scored} crypto={crypto_scored}")

    # ── Sort and cap — timing-signal ideas rank before watch_only ─────────────
    scored.sort(key=lambda c: c["score"], reverse=True)

    timing_ideas  = [c for c in scored if c.get("has_timing_signal", True)]
    watchonly_ideas = [c for c in scored if not c.get("has_timing_signal", True)]

    # Fill top-N with timing ideas first; pad with watch_only if slots remain
    top_candidates = timing_ideas[:limit]
    if len(top_candidates) < limit:
        top_candidates += watchonly_ideas[:limit - len(top_candidates)]

    # Remove internal helper keys before output
    ideas = []
    for c in top_candidates:
        out = {k: v for k, v in c.items() if not k.startswith("_")}
        ideas.append(out)

    top_syms = [c["symbol"] for c in ideas]
    watch_only_count = sum(1 for c in ideas if not c.get("has_timing_signal", True))
    print(f"[daily-alpha] top_symbols={top_syms} watch_only_in_top={watch_only_count}")

    # ── Unsafe/skipped sources summary ────────────────────────────────────────
    unsafe_sources_skipped = [
        {"source": k, "reason": "unavailable_cache_only"}
        for k, v in sh.health.items()
        if v == "unavailable_cache_only"
    ]

    # ── Source health with split watchlist detail ─────────────────────────────
    full_source_health = dict(sh.health)
    watchlist_detail = {
        "watchlist_symbols_status":    wl_meta.get("symbols_status", "missing"),
        "watchlist_signal_status":     wl_meta.get("signal_status", "missing"),
        "watchlist_symbol_count":      wl_meta.get("symbol_count", 0),
        "watchlist_signal_age_seconds": wl_meta.get("signal_age_seconds"),
    }

    # ── Build response ────────────────────────────────────────────────────────
    result: dict[str, Any] = {
        "ok":                     True,
        "generated_at":           _now_iso(),
        "mode":                   "cache_only",
        "board_mode":             "long_watchlist",
        "external_api_calls":     0,
        "provider_calls_blocked": True,
        "provider_call_attempts": [],
        "unsafe_sources_skipped": unsafe_sources_skipped,
        "limit":                  limit,
        "regime":                 regime,
        "ideas":                  ideas,
        "source_health":          full_source_health,
        **watchlist_detail,
        "counts": {
            "candidates_seen":       candidates_seen,
            "candidates_qualified":  candidates_qualified,
            "stocks_scored":         stocks_scored,
            "crypto_scored":         crypto_scored,
            "watchlist_candidates":  len(wl_cands),
            "portfolio_candidates":  len(port_cands),
            "watch_only_in_top":     watch_only_count,
        },
        "cache": {
            "hit":          False,
            "age_seconds":  0,
            "ttl_seconds":  ttl,
            "stale_served": False,
        },
        "diagnostics": {
            "mode":                   "cache_only",
            "provider_calls_blocked": True,
            "external_api_calls":     0,
            "provider_call_attempts": [],
            "unsafe_sources_skipped": unsafe_sources_skipped,
            "source_modes": {
                k: "cache_only" for k in sh.health
            },
            "long_watchlist_mode":   True,
            "stock_shorts_suppressed":            _LB_DIAG.get("stock_shorts_suppressed", 0),
            "stock_short_candidates_converted_to_watch": _LB_DIAG.get("stock_short_candidates_converted_to_watch", 0),
            "stock_extension_notes_added":        _LB_DIAG.get("stock_extension_notes_added", 0),
            "crypto_short_candidates":            _LB_DIAG.get("crypto_short_candidates", [])[:10],
            "crypto_short_candidates_count":      len(_LB_DIAG.get("crypto_short_candidates", [])),
            "crypto_quality_gates": {
                "accepted_tsm_count":     len(_HL_DIAG.get("accepted_tsm", [])),
                "rejected_pump_count":    len(_HL_DIAG.get("rejected_pump", [])),
                "rejected_crowded_count": len(_HL_DIAG.get("rejected_crowded", [])),
                "accepted_tsm":     _HL_DIAG.get("accepted_tsm", [])[:10],
                "rejected_pump":    _HL_DIAG.get("rejected_pump", [])[:10],
                "rejected_crowded": _HL_DIAG.get("rejected_crowded", [])[:10],
            },
        } if include_diagnostics else None,
    }

    # ── Write to aggregator cache (only for default-param calls) ──────────────
    if asset_type == "all" and scope == "all" and limit == 10:
        _BOARD_CACHE["result"] = result
        _BOARD_CACHE["ts"]     = time.time()
        if ideas:
            _BOARD_CACHE["lkg"] = result

    elapsed = time.time() - t0
    print(f"[daily-alpha] cache_hit=False elapsed={elapsed:.3f}s")

    return result


def build_daily_alpha_board_safe(
    limit: int = 10,
    asset_type: str = "all",
    scope: str = "all",
    refresh: bool = False,
    include_diagnostics: bool = False,
) -> dict:
    """
    Wrapper with LKG fallback — home page always gets a response.
    If ranking fails, serve last-known-good result (stale_served=True).
    """
    try:
        return build_daily_alpha_board(
            limit=limit,
            asset_type=asset_type,
            scope=scope,
            refresh=refresh,
            include_diagnostics=include_diagnostics,
        )
    except Exception as exc:
        print(f"[daily-alpha] BUILD_FAILED — serving LKG: {exc!r}")
        lkg = _BOARD_CACHE.get("lkg")
        if lkg:
            out = dict(lkg)
            out["cache"] = {
                "hit":          True,
                "age_seconds":  int(time.time() - _BOARD_CACHE["ts"]),
                "ttl_seconds":  _BOARD_CACHE.get("ttl", _TTL_OFF_HOURS),
                "stale_served": True,
            }
            out["ok"] = True
            return out
        return {
            "ok":                     False,
            "generated_at":           _now_iso(),
            "mode":                   "cache_only",
            "external_api_calls":     0,
            "provider_calls_blocked": True,
            "provider_call_attempts": [],
            "unsafe_sources_skipped": [],
            "limit":                  limit,
            "regime":                 {"label": "neutral", "summary": "unavailable", "drivers": [], "confidence": 0.0},
            "ideas":                  [],
            "source_health":          {k: "error" for k in (
                                       "watchlist","portfolio","social","themes",
                                       "strategy","catalysts","options","hyperliquid","macro")},
            "watchlist_symbols_status":    "error",
            "watchlist_signal_status":     "error",
            "watchlist_symbol_count":      0,
            "watchlist_signal_age_seconds": None,
            "counts":              {"candidates_seen":0,"candidates_qualified":0,
                                    "stocks_scored":0,"crypto_scored":0,
                                    "watchlist_candidates":0,"portfolio_candidates":0,
                                    "watch_only_in_top":0},
            "cache":               {"hit":False,"age_seconds":0,"ttl_seconds":300,"stale_served":False},
            "diagnostics":         None,
            "error":               str(exc),
        }


# ─────────────────────────────────────────────────────────────────────────────
# Diagnostics helper
# ─────────────────────────────────────────────────────────────────────────────

def build_diagnostics() -> dict:
    """
    Full diagnostics — top 20 pre-ranked candidates, source ages, cache info.
    """
    t0 = time.time()

    sh = _SourceHealth()
    all_source_data: dict[str, list] = {}

    wl_diag_meta: dict = {}
    for name, fn in [
        ("watchlist",   collect_watchlist_cache_candidates),
        ("portfolio",   collect_portfolio_cache_candidates),
        ("social",      collect_social_screener_cache_candidates),
        ("themes",      collect_themes_cache_candidates),
        ("strategy",    collect_strategy_cache_candidates),
        ("catalysts",   collect_catalyst_cache_candidates),
        ("options",     collect_options_cache_candidates),
        ("hyperliquid", collect_hyperliquid_cache_candidates),
    ]:
        raw_result = fn()
        if name == "watchlist":
            cands, status, wl_diag_meta = raw_result
        else:
            cands, status = raw_result
        sh.set(name, status, len(cands))
        all_source_data[name] = cands

    regime, reg_status = collect_macro_cache_context()
    sh.set("macro", reg_status, 0)

    all_raw = []
    for cands in all_source_data.values():
        all_raw.extend(cands)

    merged = _merge_candidates(all_raw)
    for c in merged:
        _score_candidate(c, regime)
    merged.sort(key=lambda c: c["score"], reverse=True)

    top20 = [{k: v for k, v in c.items() if not k.startswith("_")} for c in merged[:20]]

    # Check LKG file ages
    file_ages: dict[str, Any] = {}
    for label, path in [
        ("strategy_lkg",    _STRATEGY_LKG_PATH),
        ("themes_lkg",      _THEMES_LKG_PATH),
        ("hl_snapshots",    _HL_SNAP_PATH),
        ("x_consensus",     _X_CONSENSUS_PATH),
        ("options_master",  _OPTIONS_PATHS[0]),
    ]:
        try:
            if path.exists():
                age_s = _source_age_s(os.path.getmtime(str(path)))
                file_ages[label] = {
                    "age_seconds": int(age_s or 0),
                    "staleness_factor": _staleness_factor(age_s),
                    "path": str(path.name),
                }
            else:
                file_ages[label] = {"status": "file_not_found"}
        except Exception as e:
            file_ages[label] = {"error": str(e)}

    cache_age = int(time.time() - _BOARD_CACHE["ts"])

    unsafe_skipped = [
        {"source": k, "reason": "unavailable_cache_only"}
        for k, v in sh.health.items()
        if v == "unavailable_cache_only"
    ]

    return {
        "ok":                     True,
        "generated_at":           _now_iso(),
        "mode":                   "cache_only",
        "external_api_calls":     0,
        "provider_calls_blocked": True,
        "provider_call_attempts": [],
        "unsafe_sources_skipped": unsafe_skipped,
        "elapsed_ms":             round((time.time() - t0) * 1000, 1),
        "source_health":          sh.health,
        "source_counts":          sh.counts,
        "watchlist_symbols_status":     wl_diag_meta.get("symbols_status", "missing"),
        "watchlist_signal_status":      wl_diag_meta.get("signal_status", "missing"),
        "watchlist_symbol_count":       wl_diag_meta.get("symbol_count", 0),
        "watchlist_signal_age_seconds": wl_diag_meta.get("signal_age_seconds"),
        "regime":              regime,
        "file_ages":           file_ages,
        "aggregator_cache": {
            "has_result":       _BOARD_CACHE["result"] is not None,
            "has_lkg":          _BOARD_CACHE["lkg"] is not None,
            "age_seconds":      cache_age,
            "ttl_seconds":      _BOARD_CACHE.get("ttl", _TTL_OFF_HOURS),
        },
        "top_20_pre_ranked":   top20,
        "skipped_sources":     unsafe_skipped,
        # Phase 1 — RS provenance diagnostics
        "rs_provenance": dict(_RS_PROV_DIAG),
        # Phase 2 — technical freshness diagnostics
        "technical_freshness": dict(_TECH_FRESH_DIAG),
    }
