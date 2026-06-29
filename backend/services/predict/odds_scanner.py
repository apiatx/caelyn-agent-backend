"""
Prediction Market Odds Scanner — Full Catalog Architecture.

Discovery: Full active Polymarket catalog crawl via Gamma API pagination.
  GET /events?active=true&closed=false&limit=100&offset=N
  Continues until exhausted (~21 pages / ~29k markets).
  Flattens nested markets. Catalog held in memory (Option C — no DB persist).

Matching: All 26 tracked-odds families are matched against the FULL in-memory
catalog, not a top-N or tag-limited subset.  Sports + pop-culture exclusion,
CLOB price enrichment, delta computation, and 7-day snapshot persistence follow.

Persistence strategy:
  - Catalog (29k rows): MEMORY-ONLY (Option C).  Full crawl stored in
    self._last_raw_markets.  No DB upsert — avoids 90s+ timeout on large batch.
  - Snapshots (26 rows/cycle): persisted to prediction_market_odds_snapshots
    via executor thread with 30s timeout.  Powers the 7-day history endpoint.

Entry points
------------
odds_scanner.scan_and_persist()   → async; called by _odds_scanner_loop() in main.py
odds_scanner.get_live()           → sync; returns cached payload (warming stub if cold)
odds_scanner.get_live_payload()   → sync; returns cached dict or None if not warmed
odds_scanner.get_history(fk,days) → sync; returns time-series list from DB
odds_scanner.get_diagnostics()    → sync; returns full diagnostics dict (no DB calls)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import pathlib
import re
import tempfile
import time
from datetime import datetime, timezone
from typing import Any, Optional

log = logging.getLogger("odds_scanner")

# ── Internal imports (guarded) ────────────────────────────────────────────────

try:
    from services.predict.odds_registry import (
        ODDS_REGISTRY,
        REGISTRY_BY_KEY,
        is_sports_excluded,
    )
except Exception as _e:
    log.warning("[odds_scanner] odds_registry import error: %s", _e)
    ODDS_REGISTRY = []
    REGISTRY_BY_KEY = {}
    def is_sports_excluded(*a, **kw) -> bool:  # type: ignore
        return False

# Families that bypass the 72-h near-expiry gate during live scan.
# Expired entries from these families MUST be stripped from LKG / stale_db payloads
# so yesterday's direction market never re-surfaces as a current signal.
_DAILY_DIRECTION_FAMILY_KEYS: frozenset = frozenset(
    fdef["family_key"] for fdef in ODDS_REGISTRY if fdef.get("allow_near_expiry")
)

try:
    from data.predict_odds_store import (
        ensure_table,
        insert_snapshots,
        delete_old_snapshots,
        get_snapshots_before,
        get_history as _db_get_history,
        get_diagnostics as _db_get_diagnostics,
        get_latest_per_family as _db_get_latest_per_family,
    )
    _SNAP_STORE_OK = True
except Exception as _e:
    log.warning("[odds_scanner] predict_odds_store import error: %s", _e)
    _SNAP_STORE_OK = False
    def ensure_table(*a, **kw): return False   # type: ignore
    def insert_snapshots(*a, **kw): return 0   # type: ignore
    def delete_old_snapshots(*a, **kw): return 0   # type: ignore
    def get_snapshots_before(*a, **kw): return []    # type: ignore
    def _db_get_history(*a, **kw): return []         # type: ignore
    def _db_get_diagnostics(*a, **kw): return {}     # type: ignore
    def _db_get_latest_per_family(*a, **kw): return {}  # type: ignore

try:
    from data.predict_market_catalog_store import (
        ensure_catalog_table,
        upsert_catalog_rows,
        mark_stale_before,
        get_active_catalog_rows,
        get_catalog_diagnostics,
    )
    _CATALOG_STORE_OK = True
except Exception as _e:
    log.warning("[odds_scanner] predict_market_catalog_store import error: %s", _e)
    _CATALOG_STORE_OK = False
    def ensure_catalog_table(*a, **kw): return False   # type: ignore
    def upsert_catalog_rows(*a, **kw): return 0        # type: ignore
    def mark_stale_before(*a, **kw): return 0          # type: ignore
    def get_active_catalog_rows(*a, **kw): return []   # type: ignore
    def get_catalog_diagnostics(*a, **kw): return {}   # type: ignore

try:
    from data.cache import cache as _mem_cache
except Exception:
    class _FallbackCache:  # type: ignore
        _d: dict = {}
        def get(self, k, default=None): return self._d.get(k, default)
        def set(self, k, v, ttl=None): self._d[k] = v
    _mem_cache = _FallbackCache()

try:
    from services.predict.investor.exposure_resolver import (
        resolve_family_exposure as _resolve_family_exposure,
        build_canonical_ticker_map as _build_canonical_ticker_map,
    )
    _EXPOSURE_RESOLVER_OK = True
except Exception as _exp_e:
    log.warning("[odds_scanner] exposure_resolver import error: %s", _exp_e)
    _EXPOSURE_RESOLVER_OK = False
    def _resolve_family_exposure(*a, **kw): return "mixed", {}  # type: ignore
    def _build_canonical_ticker_map() -> dict: return {}  # type: ignore

try:
    from services.predict.kalshi_scanner import (
        scan_kalshi as _scan_kalshi,
        KALSHI_PRIMARY_FAMILIES as _KALSHI_PRIMARY_FAMILIES,
    )
    _KALSHI_SCANNER_OK = True
except Exception as _ks_e:
    log.warning("[odds_scanner] kalshi_scanner import error: %s", _ks_e)
    _KALSHI_SCANNER_OK = False
    async def _scan_kalshi() -> dict:  # type: ignore
        return {}
    _KALSHI_PRIMARY_FAMILIES: frozenset = frozenset()  # type: ignore


# ── Constants ─────────────────────────────────────────────────────────────────

_LIVE_CACHE_KEY  = "pm:odds:live"
_LIVE_CACHE_TTL  = 2100       # 35 min — slightly longer than scan cadence
_SCAN_LOCK       = asyncio.Lock()
_SNAPSHOTS_RETAIN_DAYS = 7

# LKG (Last Known Good) file path — written after every successful scan
_LKG_PATH = pathlib.Path(__file__).parent.parent.parent / "data" / "predict_odds_live_lkg.json"
_LKG_MAX_AGE_HOURS = 72    # serve file LKG up to 72 h old

# Delta computation window tolerances (seconds)
_DELTA_1H_TARGET   = 3600
_DELTA_24H_TARGET  = 86400
_DELTA_7D_TARGET   = 604800
_DELTA_1H_WINDOW   = 900    # ±15 min
_DELTA_24H_WINDOW  = 1800   # ±30 min
_DELTA_7D_WINDOW   = 7200   # ±2 h

# Resolving threshold — markets within 72h of end_date are excluded from matching
# (settlement volume, not trading interest)
_RESOLVING_HOURS   = 72


# ── Raw-dict helpers ──────────────────────────────────────────────────────────

def _raw_vol(m: dict) -> float:
    """Return 24h volume from either raw Gamma (volume24hr) or enriched (volume_24h) dict."""
    return float(m.get("volume24hr") or m.get("volume_24h") or 0)


def _is_active_raw(m: dict) -> bool:
    """
    Return True if a raw Gamma market dict represents a tradeable, non-resolving market.

    Filtering rules:
    - Exclude if closed=True (hard stop from Gamma)
    - Exclude if acceptingOrders is EXPLICITLY False (not null/missing — many event-nested
      market dicts omit this field entirely; treat missing as "accepting")
    - Exclude if end_date is in the past or within 72h (expired or resolving)
    """
    if m.get("closed") is True:
        return False

    # Only reject if acceptingOrders is explicitly False — NOT if it's null or missing.
    # Gamma's /events endpoint often omits acceptingOrders from nested market dicts.
    accepting = m.get("acceptingOrders")
    if accepting is not None and accepting is not True and not bool(accepting):
        return False

    end_raw = m.get("endDate") or m.get("endDateIso") or m.get("end_date")
    if end_raw:
        try:
            exp = datetime.fromisoformat(str(end_raw).replace("Z", "+00:00"))
            delta_h = (exp - datetime.now(timezone.utc)).total_seconds() / 3600
            if delta_h < _RESOLVING_HOURS:   # expired (<0) or resolving (<72h)
                return False
        except Exception:
            pass
    return True


def _is_active_direction(m: dict) -> bool:
    """
    Relaxed active-check for daily direction families (allow_near_expiry=True).

    Same closed / acceptingOrders rules as _is_active_raw(), but the end_date
    threshold is lowered to -1 h (only exclude markets already 1 h past expiry).
    This allows same-day markets (S&P up today?, BTC close higher today?) that
    would otherwise be killed by the standard 72-hour resolving gate.
    """
    if m.get("closed") is True:
        return False

    accepting = m.get("acceptingOrders")
    if accepting is not None and accepting is not True and not bool(accepting):
        return False

    end_raw = m.get("endDate") or m.get("endDateIso") or m.get("end_date")
    if end_raw:
        try:
            exp = datetime.fromisoformat(str(end_raw).replace("Z", "+00:00"))
            delta_h = (exp - datetime.now(timezone.utc)).total_seconds() / 3600
            if delta_h < -1.0:   # only exclude if already 1 h past expiry
                return False
        except Exception:
            pass
    return True


# ── Delta computation ─────────────────────────────────────────────────────────

def _compute_deltas(
    family_key: str,
    current_yes_pct: Optional[float],
    now_ts: float,
    api_fallbacks: dict,
) -> dict:
    """
    Compute 1h / 24h / 7d probability deltas in percentage-point (pp) units.
    Reads from DB history first; falls back to Polymarket's own price_change fields.

    Returns:
        {"delta_1h_pp": float|None, "delta_24h_pp": float|None, "delta_7d_pp": float|None}
    """
    if current_yes_pct is None:
        return {"delta_1h_pp": None, "delta_24h_pp": None, "delta_7d_pp": None}

    def _delta_from_db(target_secs: int, window_secs: int) -> Optional[float]:
        rows = get_snapshots_before(
            family_key,
            before_ts=now_ts - target_secs,
            window_seconds=window_secs,
            limit=1,
        )
        if not rows:
            return None
        prior_yes = rows[0].get("yes_probability")
        if prior_yes is None:
            return None
        return round(current_yes_pct - (float(prior_yes) * 100.0), 2)

    delta_1h  = _delta_from_db(_DELTA_1H_TARGET,  _DELTA_1H_WINDOW)
    delta_24h = _delta_from_db(_DELTA_24H_TARGET, _DELTA_24H_WINDOW)
    delta_7d  = _delta_from_db(_DELTA_7D_TARGET,  _DELTA_7D_WINDOW)

    if delta_1h  is None and api_fallbacks.get("delta_1h_api")  is not None:
        delta_1h  = round(float(api_fallbacks["delta_1h_api"]),  2)
    if delta_24h is None and api_fallbacks.get("delta_24h_api") is not None:
        delta_24h = round(float(api_fallbacks["delta_24h_api"]), 2)
    if delta_7d  is None and api_fallbacks.get("delta_7d_api")  is not None:
        delta_7d  = round(float(api_fallbacks["delta_7d_api"]),  2)

    return {
        "delta_1h_pp":  delta_1h,
        "delta_24h_pp": delta_24h,
        "delta_7d_pp":  delta_7d,
    }


# ── Display field helpers ─────────────────────────────────────────────────────

_MONTHS_PAT = (
    "january|february|march|april|may|june|july|august"
    "|september|october|november|december"
)
_CONTRACT_CTX_RE = re.compile(
    r"(?:after the\s+)?(" + _MONTHS_PAT + r")\s+(\d{4})\b",
    re.IGNORECASE,
)
_QUARTER_RE = re.compile(r"\b(Q[1-4])\s*(\d{4})\b", re.IGNORECASE)


def _parse_contract_context(question: str, event_title: str) -> str:
    """
    Extract a short meeting/date context from question or event_title.
    Returns empty string when no recognisable date pattern is found.
    No dates are invented — raw question is used as display_subtitle in that case.
    """
    for text in (question, event_title):
        if not text:
            continue
        m = _CONTRACT_CTX_RE.search(text)
        if m:
            return f"{m.group(1).title()} {m.group(2)}"
        m = _QUARTER_RE.search(text)
        if m:
            return f"{m.group(1).upper()} {m.group(2)}"
    return ""


def _build_display_fields(
    fdef: dict,
    best: dict,
    best_raw: dict,
    candidates_sorted: list[dict],
) -> dict:
    """
    Build the full display-context block for one tracked-odds row.

    Binary markets  → outcomes = [Yes, No] with groupItemTitle as display_label.
    negRisk markets → outcomes = all sibling buckets sharing the same event_slug,
                      each identified by their groupItemTitle.

    Returns a flat dict of fields to merge into live_pre.
    """
    label        = fdef.get("label", "")
    question     = best.get("question", "")
    event_title  = best.get("event_title", "") or best_raw.get("event_title", "")
    event_slug   = best.get("event_slug", "") or best_raw.get("event_slug", "")
    market_slug  = best.get("slug", "") or best_raw.get("slug", "")
    end_date     = best.get("end_date")
    git          = best.get("group_item_title", "") or best_raw.get("groupItemTitle", "") or ""
    neg_risk     = best.get("neg_risk", False) or bool(best_raw.get("negRisk"))
    clob_ids     = best.get("clob_token_ids") or []
    yes_pct      = best.get("yes_pct")
    yes_prob     = round(yes_pct / 100.0, 6) if yes_pct is not None else None
    no_pct       = round(100.0 - yes_pct, 1) if yes_pct is not None else None
    no_prob      = round(1.0 - yes_prob, 6) if yes_prob is not None else None

    # URL — prefer event page (richer context)
    if event_slug:
        url = f"https://polymarket.com/event/{event_slug}"
    elif market_slug:
        url = f"https://polymarket.com/market/{market_slug}"
    else:
        url = None

    # Contract context — parsed from text, never invented
    contract_context = _parse_contract_context(question, event_title)

    # ── outcomes[] ────────────────────────────────────────────────────────────
    if neg_risk:
        # Collect all sibling candidates that share the same event_slug.
        # Each sibling represents one outcome bucket labelled by groupItemTitle.
        ev_key  = event_slug or best_raw.get("event_slug", "")
        buckets: list[dict] = []
        seen_gits: set      = set()
        for cand in candidates_sorted:
            c_git     = cand.get("groupItemTitle", "")
            if not c_git:
                continue
            if c_git in seen_gits:
                continue
            # Only include siblings from the same event when we have an event_slug
            if ev_key:
                c_ev = cand.get("event_slug", "")
                if c_ev and c_ev != ev_key:
                    continue
            seen_gits.add(c_git)
            # Extract this bucket's probability from its outcomePrices[0]
            try:
                c_op = cand.get("outcomePrices")
                if isinstance(c_op, str):
                    c_op = json.loads(c_op)
                c_prob = float(c_op[0]) if c_op else 0.0
            except Exception:
                c_prob = 0.0
            c_cids = cand.get("clobTokenIds") or []
            if isinstance(c_cids, str):
                try:
                    c_cids = json.loads(c_cids)
                except Exception:
                    c_cids = []
            buckets.append({
                "label":          c_git,
                "display_label":  c_git,
                "probability":    round(c_prob, 6),
                "clob_token_id":  str(c_cids[0]) if c_cids else None,
                "side":           "yes",
            })
        buckets.sort(key=lambda b: b["probability"], reverse=True)
        outcomes = buckets
        priced_outcome_label = git if git else (buckets[0]["label"] if buckets else None)
        outcome_summary = " · ".join(
            f"{b['display_label']} {round(b['probability'] * 100, 1)}%"
            for b in buckets[:6]
        )
    else:
        # Binary Yes/No — groupItemTitle describes what "Yes" means
        yes_label = git if git else "Yes"
        yes_clob  = str(clob_ids[0]) if clob_ids else None
        no_clob   = str(clob_ids[1]) if len(clob_ids) > 1 else None
        outcomes  = [
            {
                "label":         "Yes",
                "display_label": yes_label,
                "probability":   yes_prob,
                "clob_token_id": yes_clob,
                "side":          "yes",
            },
            {
                "label":         "No",
                "display_label": "No",
                "probability":   no_prob,
                "clob_token_id": no_clob,
                "side":          "no",
            },
        ]
        priced_outcome_label = yes_label
        outcome_summary = (
            f"{yes_label} {yes_pct}% · No {no_pct}%"
            if yes_pct is not None
            else ""
        )

    # display_title: label + meeting context when parseable
    display_title = f"{label} — {contract_context}" if contract_context else label

    # display_subtitle: groupItemTitle (most informative) > raw question > ""
    # Never invent text — use what Polymarket provides verbatim
    display_subtitle = git if git else (question or "")

    return {
        "question":             question,
        "event_title":          event_title,
        "market_slug":          market_slug,
        "event_slug":           event_slug,
        "url":                  url,
        "end_date":             end_date,
        "display_title":        display_title,
        "display_subtitle":     display_subtitle,
        "contract_context":     contract_context,
        "priced_outcome":       "Yes",
        "priced_outcome_label": priced_outcome_label,
        "priced_probability":   yes_prob,
        "outcomes":             outcomes,
        "outcome_summary":      outcome_summary,
        "clob_token_ids":       clob_ids,
        "neg_risk":             neg_risk,
    }


# ── Family matching ───────────────────────────────────────────────────────────

def _match_family(fdef: dict, all_markets: list[dict]) -> list[dict]:
    """
    Return all markets that match this family definition (OR logic on keyword_patterns,
    AND NOT logic on exclude_patterns). Matching is against question.lower().
    Sports markets are pre-filtered from all_markets before this call.
    """
    patterns = [p.lower() for p in fdef.get("keyword_patterns", [])]
    exclude  = [p.lower() for p in fdef.get("exclude_patterns", [])]
    if not patterns:
        return []

    matches: list[dict] = []
    for m in all_markets:
        q = m.get("question", "").lower()
        if not q:
            continue
        if not any(pat in q for pat in patterns):
            continue
        if any(exc in q for exc in exclude):
            continue
        matches.append(m)
    return matches


# ── Daily direction expiry guard ──────────────────────────────────────────────

def _strip_expired_daily_directions(payload: dict, now_dt: datetime) -> tuple:
    """
    Remove daily-direction entries whose end_date has already passed.

    Daily direction families (allow_near_expiry=True) have same-day markets that
    expire within hours.  LKG / stale_db payloads written while the market was
    active must not re-surface those entries once end_date has passed.

    Expired entries are moved from odds[] to missing_families[] with
    reason="no_active_daily_market".  Non-direction families are untouched.

    Returns (modified_payload_copy, excluded_count).
    Modifies nothing if _DAILY_DIRECTION_FAMILY_KEYS is empty or no entry expired.
    """
    if not _DAILY_DIRECTION_FAMILY_KEYS:
        return payload, 0

    odds_in    = payload.get("odds") or []
    missing_in = list(payload.get("missing_families") or [])
    odds_out: list = []
    excluded   = 0
    missing_fkeys = {m.get("family_key") for m in missing_in}

    for entry in odds_in:
        fk = entry.get("family_key", "")
        if fk not in _DAILY_DIRECTION_FAMILY_KEYS:
            odds_out.append(entry)
            continue

        expired = False
        end_raw = entry.get("end_date")
        if end_raw:
            try:
                exp_dt = datetime.fromisoformat(str(end_raw).replace("Z", "+00:00"))
                if exp_dt.tzinfo is None:
                    exp_dt = exp_dt.replace(tzinfo=timezone.utc)
                if exp_dt <= now_dt:
                    expired = True
            except Exception:
                pass

        if expired:
            excluded += 1
            log.info(
                "[odds_scanner] daily-direction expiry strip: family=%s end_date=%s",
                fk, end_raw,
            )
            if fk not in missing_fkeys:
                missing_fkeys.add(fk)
                fdef = REGISTRY_BY_KEY.get(fk) or {}
                missing_in.append({
                    "family_key": fk,
                    "label":      fdef.get("label", fk),
                    "category":   fdef.get("category", ""),
                    "priority":   fdef.get("priority", 99),
                    "reason":     "no_active_daily_market",
                })
        else:
            odds_out.append(entry)

    if excluded == 0:
        return payload, 0

    payload = dict(payload)
    payload["odds"]                   = odds_out
    payload["missing_families"]       = sorted(missing_in, key=lambda m: m.get("priority", 99))
    payload["live_count"]             = len(odds_out)
    payload["matched_families"]       = len(odds_out)
    payload["missing_families_count"] = len(missing_in)
    return payload, excluded


# ── Kalshi live-entry builder ─────────────────────────────────────────────────

def _make_kalshi_live_entry(
    family_key: str,
    fdef: dict,
    krow: dict,
    now_ts: float,
    now_dt: datetime,
) -> dict:
    """
    Convert a normalized Kalshi row (from kalshi_scanner.scan_kalshi) into a
    live_pre entry with the same shape produced by the Polymarket matching loop.

    The _best_enriched stub has empty clob_token_ids so the CLOB gather step
    exits early (no Kalshi CLOB data exists).
    The _snap_row is included so Kalshi rows appear in the 7-day history DB.
    """
    yes_prob = krow.get("yes_probability")
    yes_pct  = krow.get("yes_pct")

    _best_enriched: dict = {
        "yes_pct":          yes_pct,
        "yes_price":        yes_prob,
        "volume_24h":       krow.get("volume_24h"),
        "liquidity":        krow.get("liquidity"),
        "clob_token_ids":   [],
        "_clob_price_used": False,
    }

    return {
        # ── Registry metadata ────────────────────────────────────────────────
        "family_key":        family_key,
        "label":             fdef.get("label", family_key),
        "category":          fdef.get("category", ""),
        "priority":          fdef.get("priority", 99),
        "dashboard_enabled": fdef.get("dashboard_enabled", True),
        "prophetik_enabled": fdef.get("prophetik_enabled", False),
        "preferred_outcome": fdef.get("preferred_outcome", "yes"),
        "description":       fdef.get("description", ""),
        # ── Pricing ──────────────────────────────────────────────────────────
        "yes_probability":   yes_prob,
        "yes_pct":           yes_pct,
        # ── Market identity ───────────────────────────────────────────────────
        "market_question":   krow.get("question", ""),
        "question":          krow.get("question", ""),
        "condition_id":      None,
        "slug":              krow.get("slug", ""),
        "market_slug":       krow.get("market_slug", ""),
        "event_slug":        krow.get("event_slug", ""),
        "event_title":       krow.get("event_title", ""),
        "url":               krow.get("url", ""),
        "end_date":          krow.get("end_date"),
        # ── Display / outcome context ─────────────────────────────────────────
        "display_title":        krow.get("display_title", ""),
        "display_subtitle":     krow.get("display_subtitle", ""),
        "contract_context":     krow.get("contract_context", ""),
        "priced_outcome":       krow.get("priced_outcome", "Yes"),
        "priced_outcome_label": krow.get("priced_outcome_label", ""),
        "priced_probability":   krow.get("priced_probability"),
        "outcomes":             krow.get("outcomes") or [],
        "outcome_summary":      krow.get("outcome_summary", ""),
        "clob_token_ids":       [],
        "neg_risk":             False,
        # ── Volume / liquidity ────────────────────────────────────────────────
        "volume_24h":        krow.get("volume_24h"),
        "liquidity":         krow.get("liquidity"),
        "candidate_count":   krow.get("candidate_count", 1),
        "driver_markets":    [],
        # ── Provider tag ──────────────────────────────────────────────────────
        "provider":                 "kalshi",
        "_kalshi_market_ticker":    krow.get("_kalshi_market_ticker", ""),
        "_kalshi_event_ticker":     krow.get("_kalshi_event_ticker", ""),
        "_kalshi_series_ticker":    krow.get("_kalshi_series_ticker", ""),
        # ── CLOB staging (Kalshi has no CLOB; empty tokens → skipped) ─────────
        "_best_enriched":    _best_enriched,
        # ── Delta staging fields (popped in step 8) ───────────────────────────
        "_api_1h":      None,
        "_api_24h":     None,
        "_api_7d":      None,
        "_yes_pct_raw": yes_pct,
        # ── Snapshot row (persisted to DB in step 7) ─────────────────────────
        "_snap_row": {
            "family_key":      family_key,
            "market_id":       (
                krow.get("_kalshi_market_ticker")
                or krow.get("slug", "")
                or family_key
            ),
            "market_slug":     krow.get("market_slug"),
            "question":        krow.get("question"),
            "source":          "kalshi",
            "yes_probability": yes_prob,
            "no_probability":  (
                round(1.0 - yes_prob, 6) if yes_prob is not None else None
            ),
            "best_bid":        None,
            "best_ask":        None,
            "volume_24h":      krow.get("volume_24h"),
            "liquidity":       krow.get("liquidity"),
            "end_date":        krow.get("end_date"),
            "captured_at":     now_dt,
            "raw_json": {
                "question":             krow.get("question"),
                "yes_pct":              yes_pct,
                "price_change_1h":      None,
                "price_change_1d":      None,
                "price_change_1wk":     None,
                "volume_24h":           krow.get("volume_24h"),
                "catalog_source":       "kalshi",
                "event_title":          krow.get("event_title", ""),
                "event_slug":           krow.get("event_slug", ""),
                "url":                  krow.get("url", ""),
                "outcomes":             krow.get("outcomes") or [],
                "outcome_summary":      krow.get("outcome_summary", ""),
                "priced_outcome_label": krow.get("priced_outcome_label", ""),
                "priced_probability":   krow.get("priced_probability"),
                "display_title":        krow.get("display_title", ""),
                "display_subtitle":     krow.get("display_subtitle", ""),
                "contract_context":     krow.get("contract_context", ""),
                "end_date":             krow.get("end_date"),
                "provider":             "kalshi",
                "kalshi_market_ticker": krow.get("_kalshi_market_ticker", ""),
                "kalshi_event_ticker":  krow.get("_kalshi_event_ticker", ""),
                "quality":              krow.get("quality", ""),
            },
        },
    }


# ── Core scanner ──────────────────────────────────────────────────────────────

class OddsScanner:
    """
    Full-catalog Polymarket scanner for the Tracked Odds Registry.

    Discovery: crawls ALL active Polymarket events via Gamma API pagination,
    flattens nested markets, and matches 26 tracked-odds families against the
    full universe — not a top-N or tag-limited subset.

    Persistence: catalog held in memory only (Option C).  Odds snapshots
    (26 rows/cycle) are persisted to prediction_market_odds_snapshots via executor.
    """

    def __init__(self) -> None:
        # Catalog-crawl state (survives across scan cycles in the same process)
        self._catalog_last_crawl_at: Optional[datetime] = None
        self._catalog_crawl_success: bool = False
        # In-memory catalog fallback (Option C: no DB upsert of 29k rows)
        self._last_raw_markets: list[dict] = []
        # Scan lifecycle tracking
        self._last_successful_scan_at: Optional[str] = None
        self._last_scan_error: Optional[str] = None
        self._scanner_running: bool = False
        # Last-good diagnostics — written after each scan, read by /diagnostics (no DB calls)
        self._last_diagnostics: dict = {
            "cache_warm":            False,
            "registry_family_count": len(ODDS_REGISTRY),
            "live_family_count":     0,
            "missing_family_count":  len(ODDS_REGISTRY),
            "odds_live_source":      "warming",
            "lkg_file_exists":       _LKG_PATH.exists(),
            "lkg_loaded":            False,
            "lkg_updated_at":        None,
            "lkg_age_seconds":       None,
            "db_snapshot_fallback_loaded": False,
            "last_successful_scan_at": None,
            "last_scan_error":       None,
            "scanner_running":       False,
        }

    # ── LKG (Last Known Good) helpers ─────────────────────────────────────────

    def _save_lkg(self, payload: dict) -> bool:
        """
        Atomically write the full /odds/live payload to the LKG file.
        Uses temp-file + rename so readers never see a partial write.
        Returns True on success.
        """
        try:
            _LKG_PATH.parent.mkdir(parents=True, exist_ok=True)
            tmp = _LKG_PATH.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f, default=str)
            os.replace(tmp, _LKG_PATH)
            log.info("[odds_scanner] LKG written → %s", _LKG_PATH)
            return True
        except Exception as exc:
            log.warning("[odds_scanner] LKG write failed: %s", exc)
            return False

    def _load_lkg(self, max_age_hours: float = _LKG_MAX_AGE_HOURS) -> Optional[dict]:
        """
        Load the LKG file if it exists and is not older than max_age_hours.
        Returns the payload dict (with status/stale fields injected) or None.
        """
        try:
            if not _LKG_PATH.exists():
                return None
            raw = _LKG_PATH.read_text(encoding="utf-8")
            payload = json.loads(raw)
            # Check age
            updated_raw = payload.get("scanned_at") or payload.get("updated_at")
            if updated_raw:
                try:
                    updated_dt = datetime.fromisoformat(str(updated_raw))
                    if updated_dt.tzinfo is None:
                        updated_dt = updated_dt.replace(tzinfo=timezone.utc)
                    age_s = (datetime.now(timezone.utc) - updated_dt).total_seconds()
                    if age_s > max_age_hours * 3600:
                        log.info("[odds_scanner] LKG too old (%.0fh), skipping", age_s / 3600)
                        return None
                    payload = dict(payload)
                    payload["cache_age_seconds"] = round(age_s)
                except Exception:
                    pass
            payload["status"]   = "lkg"
            payload["stale"]    = True
            payload["cache_warm"] = True
            payload["last_successful_scan_at"] = payload.get("scanned_at") or payload.get("updated_at")
            return payload
        except Exception as exc:
            log.warning("[odds_scanner] LKG load failed: %s", exc)
            return None

    def _hydrate_from_db_snapshots(self) -> Optional[dict]:
        """
        Reconstruct a /odds/live payload from the latest DB snapshot per family.
        Used as a second fallback when no LKG file is available.
        Returns a payload dict with status="stale_db" or None.
        """
        if not _SNAP_STORE_OK:
            return None
        try:
            latest: dict[str, dict] = _db_get_latest_per_family()
            if not latest:
                return None

            now_dt  = datetime.now(timezone.utc)
            entries: list[dict] = []
            missing: list[dict] = []

            for fdef in ODDS_REGISTRY:
                fk   = fdef["family_key"]
                snap = latest.get(fk)
                if snap is None:
                    missing.append({
                        "family_key": fk,
                        "label":      fdef.get("label", fk),
                        "category":   fdef.get("category", ""),
                        "priority":   fdef.get("priority", 99),
                    })
                    continue
                yes_prob = snap.get("yes_probability")
                yes_pct  = round(float(yes_prob) * 100, 2) if yes_prob is not None else None
                cap_at   = snap.get("captured_at")
                rj       = snap.get("raw_json") or {}

                # Reconstruct display fields from raw_json (persisted during scan)
                label_str = fdef.get("label", fk)
                _q        = rj.get("question") or snap.get("question") or ""
                _evt      = rj.get("event_title", "")
                _ev_slug  = rj.get("event_slug", "")
                _url      = rj.get("url")
                _git      = rj.get("group_item_title", "")
                _end      = rj.get("end_date") or snap.get("end_date")
                _ctx      = rj.get("contract_context", "")
                _d_title  = rj.get("display_title") or (f"{label_str} — {_ctx}" if _ctx else label_str)
                _d_sub    = rj.get("display_subtitle") or _git or _q or ""
                _pol      = rj.get("priced_outcome_label", _git or None)
                _outcomes = rj.get("outcomes")
                _o_sum    = rj.get("outcome_summary", "")
                _m_slug   = snap.get("market_slug") or ""

                # If raw_json had no outcomes, synthesise a minimal binary pair
                if not _outcomes and yes_prob is not None:
                    _no_prob = round(1.0 - float(yes_prob), 6)
                    _outcomes = [
                        {"label": "Yes", "display_label": _pol or "Yes",
                         "probability": float(yes_prob), "clob_token_id": None, "side": "yes"},
                        {"label": "No",  "display_label": "No",
                         "probability": _no_prob, "clob_token_id": None, "side": "no"},
                    ]
                    if not _o_sum:
                        _o_sum = f"{_pol or 'Yes'} {yes_pct}% · No {round((1.0-float(yes_prob))*100, 1)}%"

                entry = {
                    "family_key":       fk,
                    "label":            label_str,
                    "category":         fdef.get("category", ""),
                    "priority":         fdef.get("priority", 99),
                    "dashboard_enabled":  fdef.get("dashboard_enabled", True),
                    "prophetik_enabled":  fdef.get("prophetik_enabled", False),
                    "preferred_outcome":  fdef.get("preferred_outcome"),
                    "description":        fdef.get("description", ""),
                    "yes_probability":  yes_prob,
                    "yes_pct":          yes_pct,
                    "market_question":  _q,
                    "question":         _q,
                    "condition_id":     None,
                    "slug":             _m_slug,
                    "market_slug":      _m_slug,
                    "event_slug":       _ev_slug,
                    "event_title":      _evt,
                    "url":              _url,
                    "end_date":         _end,
                    "display_title":    _d_title,
                    "display_subtitle": _d_sub,
                    "contract_context": _ctx,
                    "priced_outcome":         "Yes",
                    "priced_outcome_label":   _pol,
                    "priced_probability":     float(yes_prob) if yes_prob is not None else None,
                    "outcomes":               _outcomes or [],
                    "outcome_summary":        _o_sum,
                    "volume_24h":       snap.get("volume_24h"),
                    "liquidity":        snap.get("liquidity"),
                    "candidate_count":  1,
                    "driver_markets":   [],
                    "delta_1h_pp":      None,
                    "delta_24h_pp":     None,
                    "delta_7d_pp":      None,
                    "market_read":      None,
                    "exposure":         None,
                    "_db_snap_at":      cap_at,
                }
                entries.append(entry)

            entries_sorted = sorted(entries, key=lambda e: e.get("priority", 99))
            missing_sorted = sorted(missing, key=lambda e: e.get("priority", 99))

            payload = {
                "updated_at":             now_dt.isoformat(),
                "scanned_at":             now_dt.isoformat(),
                "cache_age_seconds":      0,
                "scan_ms":                None,
                "live_count":             len(entries),
                "tracked_count":          len(ODDS_REGISTRY),
                "total_families":         len(ODDS_REGISTRY),
                "matched_families":       len(entries),
                "missing_families_count": len(missing),
                "odds":                   entries_sorted,
                "missing_families":       missing_sorted,
                "status":                 "stale_db",
                "stale":                  True,
                "cache_warm":             True,
                "last_successful_scan_at": None,
                "diagnostics": {
                    "cache_warm":          True,
                    "db_snapshot_fallback": True,
                    "db_families_loaded":  len(entries),
                    "odds_live_source":    "db_snapshot",
                },
            }
            log.info(
                "[odds_scanner] DB snapshot fallback: %d/%d families hydrated",
                len(entries), len(ODDS_REGISTRY),
            )
            return payload
        except Exception as exc:
            log.warning("[odds_scanner] DB snapshot hydration failed: %s", exc)
            return None

    # ── Catalog crawl ─────────────────────────────────────────────────────────

    async def _crawl_catalog(
        self,
        polymarket_intel: Any,
    ) -> tuple[list[dict], dict, bool]:
        """
        Fetch the full active Polymarket market catalog — memory-only (Option C).

        No DB persistence of the 29k-row catalog.  The full raw_markets list is
        stored on self._last_raw_markets for inter-cycle in-process fallback.
        Only odds snapshots (26 rows/cycle) are persisted to Neon.

        Returns:
            (raw_markets, crawl_stats, success)
            raw_markets — list of raw Gamma market dicts (tags + event_slug injected)
            crawl_stats — dict with catalog_* keys
            success     — True if live crawl succeeded, False if in-memory fallback
        """
        try:
            raw_markets, stats = await polymarket_intel.fetch_full_active_catalog()

            if not raw_markets:
                raise ValueError("fetch_full_active_catalog returned empty list")

            self._last_raw_markets = raw_markets
            self._catalog_last_crawl_at = datetime.now(timezone.utc)
            self._catalog_crawl_success = True
            log.info(
                "[odds_scanner] catalog crawl OK — %d events / %d markets / %d pages "
                "(memory-only, no DB upsert)",
                stats.get("catalog_events_total", 0),
                stats.get("catalog_markets_flattened", 0),
                stats.get("catalog_events_pages_fetched", 0),
            )
            return raw_markets, stats, True

        except Exception as exc:
            self._catalog_crawl_success = False
            log.warning(
                "[odds_scanner] catalog crawl failed (%s) — using in-memory fallback "
                "(%d markets from previous cycle)",
                exc, len(self._last_raw_markets),
            )
            fallback_stats = {
                "catalog_events_pages_fetched": 0,
                "catalog_events_total":         0,
                "catalog_markets_flattened":    len(self._last_raw_markets),
            }
            return self._last_raw_markets, fallback_stats, False

    # ── CLOB price enrichment ─────────────────────────────────────────────────

    async def _enrich_with_clob(
        self,
        enriched: dict,
        polymarket_intel: Any,
    ) -> tuple[dict, bool]:
        """
        Try to override the Gamma-derived yes_price/yes_pct with the CLOB midpoint.
        Returns (enriched_dict, clob_success_bool).
        The enriched dict is mutated in-place on success.
        """
        tokens = enriched.get("clob_token_ids") or []
        if not tokens or not isinstance(tokens, list):
            return enriched, False

        yes_token = str(tokens[0]) if tokens else ""
        if not yes_token:
            return enriched, False

        try:
            mid = await polymarket_intel.get_clob_midpoint(yes_token)
            if mid is not None:
                enriched["yes_price"] = round(mid, 4)
                enriched["yes_pct"]   = round(mid * 100.0, 1)
                enriched["no_price"]  = round(1.0 - mid, 4)
                enriched["no_pct"]    = round((1.0 - mid) * 100.0, 1)
                enriched["_clob_price_used"] = True
                return enriched, True
        except Exception:
            pass
        return enriched, False

    # ── Core scan ─────────────────────────────────────────────────────────────

    async def scan_and_persist(self) -> dict:
        """Full scan cycle. Thread-safe via _SCAN_LOCK."""
        self._scanner_running = True
        try:
            async with _SCAN_LOCK:
                return await self._do_scan()
        except Exception as exc:
            self._last_scan_error = str(exc)
            log.warning("[odds_scanner] scan_and_persist error: %s", exc)
            raise
        finally:
            self._scanner_running = False

    async def _do_scan(self) -> dict:
        t0 = time.time()

        # ── 1. Import providers ───────────────────────────────────────────────
        try:
            from services.predict.polymarket_intelligence import (
                polymarket_intel,
                _is_sports_market,
                _is_pop_culture_market,
            )
        except Exception as exc:
            log.warning("[odds_scanner] polymarket_intelligence import error: %s", exc)
            polymarket_intel = None
            def _is_sports_market(m: dict) -> bool:  # type: ignore
                return False
            def _is_pop_culture_market(m: dict) -> bool:  # type: ignore
                return False

        crawl_started_at = datetime.now(timezone.utc)

        # ── 1b. Launch Kalshi scan concurrently with Polymarket crawl ──────────
        _kalshi_task = asyncio.create_task(
            asyncio.wait_for(_scan_kalshi(), timeout=30.0)
        ) if _KALSHI_SCANNER_OK else None

        # ── 2. Full catalog crawl (memory-only, Option C) ─────────────────────
        if polymarket_intel is not None:
            raw_markets, crawl_stats, crawl_success = await self._crawl_catalog(
                polymarket_intel
            )
        else:
            raw_markets, crawl_stats, crawl_success = [], {
                "catalog_events_pages_fetched": 0,
                "catalog_events_total":         0,
                "catalog_markets_flattened":    0,
            }, False

        # ── 2b. Collect Kalshi result (already running in background) ──────────
        kalshi_scan_result: dict = {}
        if _kalshi_task is not None:
            try:
                kalshi_scan_result = await _kalshi_task
            except Exception as _ks_exc:
                log.warning("[odds_scanner] kalshi scan task error: %s", _ks_exc)
                kalshi_scan_result = {}

        catalog_markets_flattened    = crawl_stats.get("catalog_markets_flattened", 0)
        catalog_events_pages_fetched = crawl_stats.get("catalog_events_pages_fetched", 0)
        catalog_events_total         = crawl_stats.get("catalog_events_total", 0)

        # ── 3. Active filter + dedup by condition_id ──────────────────────────
        #
        # Two pools are built in one pass:
        #   active_pool        — standard 72-h near-expiry gate (all families)
        #   direction_raw_pool — relaxed -1h gate (only for allow_near_expiry families)
        #
        seen_cids: set[str] = set()
        active_pool:        list[dict] = []
        direction_raw_pool: list[dict] = []
        for m in raw_markets:
            cid = m.get("conditionId") or m.get("condition_id") or ""
            if cid and cid in seen_cids:
                continue
            if cid:
                seen_cids.add(cid)
            std_active = _is_active_raw(m)
            if std_active:
                active_pool.append(m)
                direction_raw_pool.append(m)
            elif _is_active_direction(m):
                # Near-expiry market: only allow for direction families
                direction_raw_pool.append(m)

        active_open_count = len(active_pool)   # after is_active_raw, before investor exclusion

        # ── 4. Investor eligibility exclusion (sports + pop culture) ──────────
        sports_excluded_count      = 0
        pop_culture_excluded_count = 0
        excluded_categories_seen: set[str] = set()
        clean_pool:           list[dict] = []
        direction_clean_pool: list[dict] = []
        for m in active_pool:
            if _is_sports_market(m):
                sports_excluded_count += 1
                for tag in (m.get("tags") or []):
                    excluded_categories_seen.add(tag if isinstance(tag, str) else str(tag))
            elif _is_pop_culture_market(m):
                pop_culture_excluded_count += 1
                for tag in (m.get("tags") or []):
                    excluded_categories_seen.add(tag if isinstance(tag, str) else str(tag))
            else:
                clean_pool.append(m)

        # direction_clean_pool: include near-expiry markets that passed _is_active_direction
        _direction_seen_cids: set[str] = {
            m.get("conditionId") or m.get("condition_id") or "" for m in clean_pool
        }
        direction_clean_pool = list(clean_pool)   # start with all standard eligible markets
        for m in direction_raw_pool:
            cid = m.get("conditionId") or m.get("condition_id") or ""
            if cid and cid in _direction_seen_cids:
                continue   # already in clean_pool
            if _is_sports_market(m) or _is_pop_culture_market(m):
                continue
            direction_clean_pool.append(m)
            if cid:
                _direction_seen_cids.add(cid)

        investor_excluded_count = sports_excluded_count + pop_culture_excluded_count
        candidate_pool_size     = len(clean_pool)

        # ── Daily direction diagnostic counters ───────────────────────────────
        _dir_fkeys = {
            fdef["family_key"] for fdef in ODDS_REGISTRY if fdef.get("allow_near_expiry")
        }
        _direction_terms: set[str] = {
            "s&p 500", "s&p500", "spx", "spy",
            "nasdaq", "qqq", "ndx",
            "dow", "djia", "dia",
            "bitcoin", "btc",
            "up today", "down today", "close higher", "close lower",
            "up or down", "green today", "red today",
        }

        def _is_direction_candidate(m: dict) -> bool:
            q = (m.get("question") or "").lower()
            return any(t in q for t in _direction_terms)

        spx_candidates_seen      = sum(1 for m in direction_raw_pool if any(
            t in (m.get("question") or "").lower() for t in ("s&p 500","s&p500","spx","spy ")))
        nasdaq_candidates_seen   = sum(1 for m in direction_raw_pool if any(
            t in (m.get("question") or "").lower() for t in ("nasdaq","qqq","ndx")))
        dow_candidates_seen      = sum(1 for m in direction_raw_pool if any(
            t in (m.get("question") or "").lower() for t in ("dow jones","djia","dow close","dow up","dow down")))
        btc_dir_candidates_seen  = sum(1 for m in direction_raw_pool if any(
            t in (m.get("question") or "").lower() for t in ("bitcoin","btc")))
        daily_dir_candidates_seen = sum(1 for m in direction_raw_pool if _is_direction_candidate(m))
        # excluded-by-std-gate count: in direction_raw_pool but NOT in active_pool
        _active_cids = {m.get("conditionId") or m.get("condition_id") or "" for m in active_pool}
        daily_dir_excl_near_expiry = sum(
            1 for m in direction_raw_pool
            if (m.get("conditionId") or m.get("condition_id") or "") not in _active_cids
            and _is_direction_candidate(m)
        )

        log.info(
            "[odds_scanner] pool: %d raw → %d active/open (+%d direction near-expiry) "
            "→ %d investor-eligible (%d sports + %d pop-culture excluded)",
            len(raw_markets), active_open_count,
            len(direction_clean_pool) - len(clean_pool),
            candidate_pool_size,
            sports_excluded_count, pop_culture_excluded_count,
        )

        now_ts = time.time()
        now_dt = datetime.fromtimestamp(now_ts, tz=timezone.utc)

        # ── 5. Match families against full pool ───────────────────────────────
        live_pre:              list[dict] = []   # pre-CLOB entries, for async CLOB step
        missing_list:          list[dict] = []
        snap_rows:             list[dict] = []
        families_from_catalog: list[str]  = []
        families_still_missing: list[str] = []
        daily_dir_matched: list[str]      = []

        for fdef in ODDS_REGISTRY:
            fk = fdef["family_key"]
            # Direction families use the relaxed near-expiry pool
            pool = direction_clean_pool if fdef.get("allow_near_expiry") else clean_pool
            candidates = _match_family(fdef, pool)

            if not candidates:
                families_still_missing.append(fk)
                _missing_entry: dict = {
                    "family_key":        fk,
                    "label":             fdef["label"],
                    "category":          fdef["category"],
                    "priority":          fdef["priority"],
                    "dashboard_enabled": fdef["dashboard_enabled"],
                    "prophetik_enabled": fdef["prophetik_enabled"],
                    "preferred_outcome": fdef["preferred_outcome"],
                    "description":       fdef.get("description", ""),
                }
                # Daily direction families have no multi-day markets — absence means
                # no active same-day market exists right now, not a data problem.
                if fdef.get("allow_near_expiry"):
                    _missing_entry["reason"] = "no_active_daily_market"
                missing_list.append(_missing_entry)
                continue

            # Best candidate = highest 24h volume
            candidates_sorted = sorted(candidates, key=_raw_vol, reverse=True)
            best_raw = candidates_sorted[0]

            # Enrich via _enrich_market() to get yes_pct, volumes, signals
            try:
                best = polymarket_intel._enrich_market(best_raw)
            except Exception:
                # Minimal fallback enrichment from raw fields
                prices = best_raw.get("outcomePrices") or ["0.5", "0.5"]
                try:
                    yes_p = float(prices[0])
                except Exception:
                    yes_p = 0.5
                best = {
                    "condition_id": best_raw.get("conditionId", ""),
                    "question":     best_raw.get("question", ""),
                    "yes_pct":      round(yes_p * 100.0, 1),
                    "yes_price":    round(yes_p, 4),
                    "volume_24h":   _raw_vol(best_raw),
                    "liquidity":    float(best_raw.get("liquidityNum") or 0),
                    "slug":         best_raw.get("slug", ""),
                    "clob_token_ids": best_raw.get("clobTokenIds") or [],
                    "price_change_1h":  float(best_raw.get("oneHourPriceChange") or 0) * 100,
                    "price_change_1d":  float(best_raw.get("oneDayPriceChange") or 0) * 100,
                    "price_change_1wk": float(best_raw.get("oneWeekPriceChange") or 0) * 100,
                }

            families_from_catalog.append(fk)
            if fdef.get("allow_near_expiry"):
                daily_dir_matched.append(fk)
            best_cid = best.get("condition_id") or ""
            yes_pct  = best.get("yes_pct")
            yes_prob = round(yes_pct / 100.0, 6) if yes_pct is not None else None

            driver_markets = [
                {
                    "question":     m.get("question", m.get("conditionId", "")),
                    "yes_pct":      None,   # filled after CLOB step if available
                    "volume_24h":   _raw_vol(m),
                    "delta_24h_pp": float(m.get("oneDayPriceChange") or 0) * 100,
                    "condition_id": m.get("conditionId") or m.get("condition_id", ""),
                    "slug":         m.get("slug", ""),
                }
                for m in candidates_sorted[:5]
            ]

            # Build display fields: question context, outcomes[], URL, etc.
            _display = _build_display_fields(fdef, best, best_raw, candidates_sorted)

            live_pre.append({
                "family_key":        fk,
                "label":             fdef["label"],
                "category":          fdef["category"],
                "priority":          fdef["priority"],
                "dashboard_enabled": fdef["dashboard_enabled"],
                "prophetik_enabled": fdef["prophetik_enabled"],
                "preferred_outcome": fdef["preferred_outcome"],
                "description":       fdef.get("description", ""),
                "yes_probability":   yes_prob,
                "yes_pct":           yes_pct,
                "market_question":   best.get("question", ""),
                "condition_id":      best_cid,
                "slug":              best.get("slug", ""),
                "volume_24h":        best.get("volume_24h"),
                "liquidity":         best.get("liquidity"),
                "candidate_count":   len(candidates),
                "driver_markets":    driver_markets,
                # ── Display / outcome context ─────────────────────────────────
                "question":             _display["question"],
                "event_title":          _display["event_title"],
                "market_slug":          _display["market_slug"],
                "event_slug":           _display["event_slug"],
                "url":                  _display["url"],
                "end_date":             _display["end_date"],
                "display_title":        _display["display_title"],
                "display_subtitle":     _display["display_subtitle"],
                "contract_context":     _display["contract_context"],
                "priced_outcome":       _display["priced_outcome"],
                "priced_outcome_label": _display["priced_outcome_label"],
                "priced_probability":   _display["priced_probability"],
                "outcomes":             _display["outcomes"],
                "outcome_summary":      _display["outcome_summary"],
                "clob_token_ids":       _display["clob_token_ids"],
                "neg_risk":             _display["neg_risk"],
                # ── Staging fields (popped during CLOB + delta steps) ─────────
                "_best_enriched":    best,
                "_api_1h":           best.get("price_change_1h"),
                "_api_24h":          best.get("price_change_1d"),
                "_api_7d":           best.get("price_change_1wk"),
                "_yes_pct_raw":      yes_pct,
                "_snap_row": {
                    "family_key":      fk,
                    "market_id":       best_cid or best.get("slug", "") or fk,
                    "market_slug":     best.get("slug"),
                    "question":        best.get("question"),
                    "source":          "polymarket",
                    "yes_probability": yes_prob,
                    "no_probability":  round(1.0 - yes_prob, 6) if yes_prob is not None else None,
                    "best_bid":        best.get("best_bid"),
                    "best_ask":        best.get("best_ask"),
                    "volume_24h":      best.get("volume_24h"),
                    "liquidity":       best.get("liquidity"),
                    "end_date":        best.get("end_date"),
                    "captured_at":     now_dt,
                    "raw_json": {
                        # Core price data
                        "question":        best.get("question"),
                        "yes_pct":         yes_pct,
                        "price_change_1h": best.get("price_change_1h"),
                        "price_change_1d": best.get("price_change_1d"),
                        "price_change_1wk": best.get("price_change_1wk"),
                        "volume_24h":      best.get("volume_24h"),
                        "catalog_source":  "full_crawl" if crawl_success else "neon_fallback",
                        # Display context — persisted so DB fallback can reconstruct
                        "event_title":          _display["event_title"],
                        "event_slug":           _display["event_slug"],
                        "url":                  _display["url"],
                        "group_item_title":     best_raw.get("groupItemTitle", ""),
                        "outcomes":             _display["outcomes"],
                        "outcome_summary":      _display["outcome_summary"],
                        "priced_outcome_label": _display["priced_outcome_label"],
                        "priced_probability":   _display["priced_probability"],
                        "display_title":        _display["display_title"],
                        "display_subtitle":     _display["display_subtitle"],
                        "contract_context":     _display["contract_context"],
                        "end_date":             _display["end_date"],
                    },
                },
            })

        # ── 5b. Inject Kalshi rows for Kalshi-primary families ────────────────
        #
        # For spx_daily_direction / nasdaq_daily_direction / spx_dec31_milestone:
        #   - Kalshi is the preferred provider.
        #   - Only inject if Polymarket did NOT already match the family.
        #   - Remove from missing_list if it was added there during step 5.
        # All other families remain Polymarket-primary.

        _ks_diag: dict = (kalshi_scan_result or {}).pop("_diagnostics", {})
        kalshi_injected_families: list[str] = []
        provider_selected_by_family: dict[str, str] = {}

        pm_matched_keys: set[str] = {e["family_key"] for e in live_pre}

        for fk, krow in list((kalshi_scan_result or {}).items()):
            if fk not in _KALSHI_PRIMARY_FAMILIES:
                continue
            fdef_k = REGISTRY_BY_KEY.get(fk)
            if not fdef_k:
                continue
            if fk in pm_matched_keys:
                # Kalshi wins for ALL its primary families — evict the Polymarket entry
                live_pre[:] = [e for e in live_pre if e["family_key"] != fk]
                pm_matched_keys.discard(fk)
                log.info("[odds_scanner] %s: Kalshi wins — Polymarket entry evicted", fk)
                # Fall through to inject Kalshi row below
            entry = _make_kalshi_live_entry(fk, fdef_k, krow, now_ts, now_dt)
            live_pre.append(entry)
            kalshi_injected_families.append(fk)
            provider_selected_by_family[fk] = "kalshi"
            pm_matched_keys.add(fk)
            if fdef_k.get("allow_near_expiry"):
                daily_dir_matched.append(fk)
            families_from_catalog.append(fk)
            missing_list[:] = [m for m in missing_list if m["family_key"] != fk]
            if fk in families_still_missing:
                families_still_missing.remove(fk)
            log.info(
                "[odds_scanner] Kalshi injected: %s (prob=%.1f%% vol=%.0f)",
                fk, (krow.get("yes_pct") or 0), (krow.get("volume_total") or 0),
            )

        for e in live_pre:
            fk = e["family_key"]
            if fk not in provider_selected_by_family:
                provider_selected_by_family[fk] = e.get("provider", "polymarket")

        # ── 6. Parallel CLOB price enrichment ────────────────────────────────
        clob_success_count = 0
        clob_fail_count    = 0
        gamma_price_fallback_count = 0

        if polymarket_intel is not None and live_pre:
            async def _try_clob(entry: dict) -> bool:
                enriched = entry["_best_enriched"]
                _, ok = await self._enrich_with_clob(enriched, polymarket_intel)
                return ok

            clob_results = await asyncio.gather(
                *[_try_clob(e) for e in live_pre],
                return_exceptions=True,
            )
            for i, res in enumerate(clob_results):
                if isinstance(res, Exception) or res is False:
                    clob_fail_count += 1
                    gamma_price_fallback_count += 1
                else:
                    clob_success_count += 1
                    # Update yes_pct/yes_prob in entry from enriched best
                    enriched = live_pre[i]["_best_enriched"]
                    if enriched.get("_clob_price_used"):
                        new_pct  = enriched.get("yes_pct")
                        new_prob = round(new_pct / 100.0, 6) if new_pct is not None else None
                        live_pre[i]["yes_pct"]          = new_pct
                        live_pre[i]["yes_probability"]  = new_prob
                        # Update snap_row too
                        live_pre[i]["_snap_row"]["yes_probability"] = new_prob
                        if new_prob is not None:
                            live_pre[i]["_snap_row"]["no_probability"] = round(1.0 - new_prob, 6)
        else:
            gamma_price_fallback_count = len(live_pre)
            clob_fail_count = len(live_pre)

        # ── 7. Persist snapshots (executor, 30s cap) ─────────────────────────
        snap_rows = [e.pop("_snap_row") for e in live_pre]
        snapshots_written          = 0
        snapshot_persist_success   = False
        snapshot_persist_error: Optional[str] = None
        snapshot_persist_duration_ms: Optional[int] = None

        if snap_rows:
            _snap_t0 = time.time()
            try:
                _snap_loop = asyncio.get_running_loop()
                snapshots_written = await asyncio.wait_for(
                    _snap_loop.run_in_executor(None, insert_snapshots, snap_rows),
                    timeout=30.0,
                )
                snapshot_persist_success     = snapshots_written > 0
                snapshot_persist_duration_ms = round((time.time() - _snap_t0) * 1000)
                log.info(
                    "[odds_scanner] persisted %d snapshot rows in %dms",
                    snapshots_written, snapshot_persist_duration_ms,
                )
            except asyncio.TimeoutError:
                snapshot_persist_error       = "timeout_30s"
                snapshot_persist_duration_ms = 30000
                log.warning("[odds_scanner] insert_snapshots timed out after 30s")
            except Exception as exc:
                snapshot_persist_error       = str(exc)
                snapshot_persist_duration_ms = round((time.time() - _snap_t0) * 1000)
                log.warning("[odds_scanner] insert_snapshots error: %s", exc)

        # ── 8. Compute deltas + strip staging fields + inject exposure ────────
        _exp_ctmap: dict = {}
        if _EXPOSURE_RESOLVER_OK:
            try:
                _exp_ctmap = _build_canonical_ticker_map()
            except Exception:
                pass

        live_entries: list[dict] = []
        for entry in live_pre:
            entry.pop("_best_enriched", None)
            deltas = _compute_deltas(
                family_key=entry["family_key"],
                current_yes_pct=entry.pop("_yes_pct_raw", None),
                now_ts=now_ts,
                api_fallbacks={
                    "delta_1h_api":  entry.pop("_api_1h", None),
                    "delta_24h_api": entry.pop("_api_24h", None),
                    "delta_7d_api":  entry.pop("_api_7d", None),
                },
            )
            entry.update(deltas)
            if _EXPOSURE_RESOLVER_OK:
                try:
                    _mr, _exp = _resolve_family_exposure(
                        family_key=entry["family_key"],
                        market_question=entry.get("market_question"),
                        yes_pct=entry.get("yes_pct"),
                        delta_24h=entry.get("delta_24h_pp"),
                        watchlist_syms=set(),  # canonical only — /live is a shared endpoint
                        canonical_ticker_map=_exp_ctmap,
                    )
                    entry["market_read"] = _mr
                    entry["exposure"]    = _exp
                except Exception:
                    pass
            live_entries.append(entry)

        entries_sorted = sorted(live_entries, key=lambda e: e.get("priority", 99))
        missing_sorted = sorted(missing_list, key=lambda e: e.get("priority", 99))

        scan_ms = round((time.time() - t0) * 1000)

        # ── 9. Build diagnostics block ────────────────────────────────────────
        diagnostics: dict = {
            # ── Catalog crawl pipeline ─────────────────────────────────────
            "catalog_events_pages_fetched":  catalog_events_pages_fetched,
            "catalog_events_seen":           catalog_events_total,
            "catalog_markets_flattened":     catalog_markets_flattened,
            "catalog_markets_active_open":   active_open_count,
            # ── Catalog persistence (Option C: memory-only, no DB upsert) ─
            "catalog_rows_upserted":         0,
            "catalog_persist_success":       False,
            "catalog_persist_error":         "option_c_memory_only",
            "catalog_persist_duration_ms":   None,
            "catalog_persist_note":          (
                "Catalog stored in memory only (Option C). "
                "29k-row DB upsert disabled to prevent connection stalls. "
                "Snapshot history uses prediction_market_odds_snapshots (26 rows/cycle)."
            ),
            # ── Snapshot persistence ───────────────────────────────────────
            "snapshots_written":             snapshots_written,
            "snapshots_retained_days":       _SNAPSHOTS_RETAIN_DAYS,
            "snapshot_persist_success":      snapshot_persist_success,
            "snapshot_persist_error":        snapshot_persist_error,
            "snapshot_persist_duration_ms":  snapshot_persist_duration_ms,
            # ── Exclusion breakdown ────────────────────────────────────────
            "candidate_pool_size":           candidate_pool_size,
            "sports_excluded_count":         sports_excluded_count,
            "pop_culture_excluded_count":    pop_culture_excluded_count,
            "investor_excluded_count":       investor_excluded_count,
            "excluded_categories_seen":      sorted(excluded_categories_seen),
            # ── Crawl metadata ─────────────────────────────────────────────
            "catalog_last_full_crawl_at":    (
                self._catalog_last_crawl_at.isoformat()
                if self._catalog_last_crawl_at else None
            ),
            "catalog_full_crawl_success":    crawl_success,
            # ── Registry matching stats ────────────────────────────────────
            "registry_family_count":         len(ODDS_REGISTRY),
            "live_family_count":             len(live_entries),
            "missing_family_count":          len(missing_list),
            "family_matches_from_full_catalog": families_from_catalog,
            "family_matches_from_tag_fast_path": [],
            "families_still_missing":        families_still_missing,
            # ── CLOB price enrichment ──────────────────────────────────────
            "clob_price_success_count":      clob_success_count,
            "clob_price_fail_count":         clob_fail_count,
            "gamma_price_fallback_count":    gamma_price_fallback_count,
            "hardcoded_sector_stocks_used":  False,
            # ── Daily direction diagnostics ────────────────────────────────
            "daily_direction_candidates_seen":       daily_dir_candidates_seen,
            "daily_direction_excluded_near_expiry_count": daily_dir_excl_near_expiry,
            "daily_direction_matched_count":         len(daily_dir_matched),
            "daily_direction_matched_families":      daily_dir_matched,
            "spx_candidates_seen":                   spx_candidates_seen,
            "nasdaq_candidates_seen":                nasdaq_candidates_seen,
            "dow_candidates_seen":                   dow_candidates_seen,
            "btc_direction_candidates_seen":         btc_dir_candidates_seen,
            "btc_direction_matched_count":           sum(1 for fk in daily_dir_matched if "btc" in fk or "bitcoin" in fk),
            # ── Kalshi provider diagnostics ────────────────────────────────
            "kalshi_scanner_ok":             _KALSHI_SCANNER_OK,
            "kalshi_public_api_ok":          _ks_diag.get("kalshi_public_api_ok", False),
            "kalshi_auth_ok":                _ks_diag.get("kalshi_auth_ok", False),
            "kalshi_auth_error_type":        _ks_diag.get("kalshi_auth_error_type", "not_attempted"),
            "kalshi_spx_daily_matched":      _ks_diag.get("kalshi_spx_daily_matched", False),
            "kalshi_nasdaq_daily_matched":   _ks_diag.get("kalshi_nasdaq_daily_matched", False),
            "kalshi_spx_dec31_matched":      _ks_diag.get("kalshi_spx_dec31_matched", False),
            "kalshi_rows_returned":          _ks_diag.get("kalshi_rows_returned", 0),
            "kalshi_injected_families":      kalshi_injected_families,
            "kalshi_scan_ms":               _ks_diag.get("kalshi_scan_ms"),
            "kalshi_error":                 _ks_diag.get("kalshi_error"),
            "provider_selected_by_family":  provider_selected_by_family,
            # ── Polymarket daily direction candidate counts ─────────────────
            "wti_daily_direction_candidates_seen":  sum(
                1 for m in direction_raw_pool
                if any(t in (m.get("question") or "").lower()
                       for t in ("wti", "crude oil", "crude"))
            ),
            "gold_daily_direction_candidates_seen": sum(
                1 for m in direction_raw_pool
                if any(t in (m.get("question") or "").lower()
                       for t in ("gold", "xauusd", "xau"))
            ),
            "nvda_daily_direction_candidates_seen": sum(
                1 for m in direction_raw_pool
                if any(t in (m.get("question") or "").lower()
                       for t in ("nvidia", "nvda"))
            ),
            "btc_daily_direction_candidates_seen":  btc_dir_candidates_seen,
            "micro_markets_excluded_count": sum(
                1 for m in direction_raw_pool
                if any(t in (m.get("question") or "").lower()
                       for t in ("bitcoin", "btc"))
                and any(tp in (m.get("question") or "").lower()
                        for tp in (":00pm", ":05pm", ":10pm", ":15pm", ":20pm",
                                   ":25pm", ":30pm", ":35pm", ":40pm", ":45pm",
                                   ":50pm", ":55pm", "pm et", "am et", "pm-", "am-"))
            ),
            # ── Scan timing ────────────────────────────────────────────────
            "scan_ms":                       scan_ms,
        }

        # ── 11. Unusual volume detection ──────────────────────────────────────
        unusual_prediction_markets: list[dict] = []
        try:
            _7D_WINDOW    = 7 * 86400
            _SPIKE_RATIO  = 2.0
            _MIN_HISTORY  = 3
            for _uv_entry in live_entries:
                _uv_fk  = _uv_entry.get("family_key") or ""
                _uv_vol = _uv_entry.get("volume_24h")
                if _uv_vol is None:
                    continue
                try:
                    _uv_cur = float(_uv_vol)
                except (TypeError, ValueError):
                    continue
                if _uv_cur <= 0:
                    continue
                try:
                    _uv_hist_rows = get_snapshots_before(
                        _uv_fk,
                        before_ts=now_ts,
                        window_seconds=_7D_WINDOW,
                        limit=100,
                    )
                except Exception:
                    continue
                _uv_vols: list[float] = []
                for _r in (_uv_hist_rows or []):
                    _rv = _r.get("volume_24h")
                    if _rv is not None:
                        try:
                            _uv_vols.append(float(_rv))
                        except (TypeError, ValueError):
                            pass
                if len(_uv_vols) < _MIN_HISTORY:
                    continue
                _uv_avg = sum(_uv_vols) / len(_uv_vols)
                if _uv_avg <= 0:
                    continue
                _uv_ratio = _uv_cur / _uv_avg
                if _uv_ratio >= _SPIKE_RATIO:
                    unusual_prediction_markets.append({
                        "family_key":        _uv_fk,
                        "label":             _uv_entry.get("label") or _uv_fk,
                        "provider":          _uv_entry.get("provider") or "polymarket",
                        "volume_24h":        round(_uv_cur, 2),
                        "volume_24h_avg_7d": round(_uv_avg, 2),
                        "spike_ratio":       round(_uv_ratio, 2),
                        "baseline_status":   "established",
                    })
        except Exception as _uv_exc:
            log.debug("[odds_scanner] unusual volume detection error: %s", _uv_exc)

        # ── 11b. Assemble payload ─────────────────────────────────────────────
        payload = {
            "updated_at":             now_dt.isoformat(),
            "scanned_at":             now_dt.isoformat(),   # backward compat alias
            "cache_age_seconds":      0,
            "scan_ms":                scan_ms,
            "live_count":             len(live_entries),
            "tracked_count":          len(ODDS_REGISTRY),
            "total_families":         len(ODDS_REGISTRY),   # backward compat
            "matched_families":       len(live_entries),    # backward compat
            "missing_families_count": len(missing_list),
            # Primary payload arrays
            "odds":              entries_sorted,
            "missing_families":  missing_sorted,
            # Unusual volume signals
            "unusual_prediction_markets": unusual_prediction_markets,
            # Diagnostics — top-level per spec, also accessible via get_diagnostics()
            "diagnostics":       diagnostics,
            # Private alias for get_diagnostics() helper (backward compat)
            "_scan_diag":        diagnostics,
        }

        # ── 11a. Mark status and inject LKG diagnostics fields ───────────────
        now_iso = now_dt.isoformat()
        self._last_successful_scan_at = now_iso
        self._last_scan_error         = None
        payload["status"]      = "ok"
        payload["stale"]       = False
        payload["cache_warm"]  = True
        payload["last_successful_scan_at"] = now_iso

        _mem_cache.set(_LIVE_CACHE_KEY, payload, _LIVE_CACHE_TTL)

        # ── 11b. Write LKG file (sync — fast local disk write) ────────────────
        self._save_lkg(payload)

        # ── 12. Retention (fire-and-forget) ───────────────────────────────────
        try:
            loop = asyncio.get_running_loop()
            loop.run_in_executor(None, delete_old_snapshots, _SNAPSHOTS_RETAIN_DAYS)
        except Exception:
            pass

        log.info(
            "[odds_scanner] scan complete — %d/%d matched "
            "(catalog=%d active=%d sports_excl=%d clob_ok=%d) ms=%d",
            len(live_entries), len(ODDS_REGISTRY),
            catalog_markets_flattened, candidate_pool_size,
            sports_excluded_count, clob_success_count, scan_ms,
        )

        # Store diagnostics for /diagnostics endpoint (pure in-memory, no DB calls)
        lkg_exists = _LKG_PATH.exists()
        self._last_diagnostics = {
            **diagnostics,
            "cache_warm":                True,
            "odds_live_source":          "memory",
            "lkg_file_exists":           lkg_exists,
            "lkg_loaded":                False,
            "lkg_updated_at":            now_iso,
            "lkg_age_seconds":           0,
            "db_snapshot_fallback_loaded": False,
            "last_successful_scan_at":   now_iso,
            "last_scan_error":           None,
            "scanner_running":           False,
        }

        return payload

    # ── Payload access ────────────────────────────────────────────────────────

    def get_live_payload(self) -> Optional[dict]:
        """Return cached live payload, or None if scanner has not yet run."""
        return _mem_cache.get(_LIVE_CACHE_KEY)

    def get_live(self) -> dict:
        """
        Return the best available /odds/live payload — never triggers an inline scan.

        Priority:
          1. In-memory cache  → status: "ok"  (set after successful scan)
          2. Local LKG file   → status: "lkg"  (persisted after last scan, survives restart)
          3. DB snapshot fallback → status: "stale_db"  (reconstructed from latest DB rows)
          4. Warming stub     → status: "warming"  (truly nothing available)
        """
        # ── Tier 1: in-memory cache ────────────────────────────────────────────
        cached = self.get_live_payload()
        if cached is not None:
            try:
                scanned_at = cached.get("scanned_at") or cached.get("updated_at")
                if scanned_at:
                    scanned_dt = datetime.fromisoformat(scanned_at)
                    if scanned_dt.tzinfo is None:
                        scanned_dt = scanned_dt.replace(tzinfo=timezone.utc)
                    age = (datetime.now(timezone.utc) - scanned_dt).total_seconds()
                    cached = dict(cached)
                    cached["cache_age_seconds"] = round(age)
            except Exception:
                pass
            cached.setdefault("status", "ok")
            cached.setdefault("stale", False)
            _now = datetime.now(timezone.utc)
            cached, _excl = _strip_expired_daily_directions(cached, _now)
            _d = dict(cached.get("diagnostics") or {})
            _d["expired_daily_direction_lkg_excluded_count"] = _excl
            cached = dict(cached)
            cached["diagnostics"] = _d
            return cached

        # ── Tier 2: LKG file (survives process restart) ────────────────────────
        lkg = self._load_lkg()
        if lkg is not None:
            log.info("[odds_scanner] /live served from LKG file (%d families)", len(lkg.get("odds", [])))
            # Patch diagnostics block so the source is visible
            diag = dict(lkg.get("diagnostics") or {})
            diag.update({
                "cache_warm":                True,
                "odds_live_source":          "file_lkg",
                "lkg_file_exists":           True,
                "lkg_loaded":                True,
                "lkg_updated_at":            lkg.get("last_successful_scan_at") or lkg.get("scanned_at"),
                "lkg_age_seconds":           lkg.get("cache_age_seconds"),
                "db_snapshot_fallback_loaded": False,
                "last_successful_scan_at":   lkg.get("last_successful_scan_at"),
                "last_scan_error":           self._last_scan_error,
                "scanner_running":           self._scanner_running,
            })
            lkg, _excl = _strip_expired_daily_directions(lkg, datetime.now(timezone.utc))
            diag["expired_daily_direction_lkg_excluded_count"] = _excl
            lkg["diagnostics"] = diag
            return lkg

        # ── Tier 3: DB snapshot fallback ───────────────────────────────────────
        log.info("[odds_scanner] /live: no memory/LKG — attempting DB snapshot fallback")
        db_payload = self._hydrate_from_db_snapshots()
        if db_payload is not None:
            diag = dict(db_payload.get("diagnostics") or {})
            diag.update({
                "lkg_file_exists":           False,
                "lkg_loaded":                False,
                "db_snapshot_fallback_loaded": True,
                "last_successful_scan_at":   None,
                "last_scan_error":           self._last_scan_error,
                "scanner_running":           self._scanner_running,
            })
            db_payload, _excl = _strip_expired_daily_directions(db_payload, datetime.now(timezone.utc))
            diag["expired_daily_direction_lkg_excluded_count"] = _excl
            db_payload["diagnostics"] = diag
            return db_payload

        # ── Tier 4: warming stub ───────────────────────────────────────────────
        return {
            "updated_at":        None,
            "scanned_at":        None,
            "cache_age_seconds": None,
            "live_count":        0,
            "tracked_count":     len(ODDS_REGISTRY),
            "total_families":    len(ODDS_REGISTRY),
            "matched_families":  0,
            "odds":              [],
            "missing_families":  [],
            "status":            "warming",
            "stale":             True,
            "cache_warm":        False,
            "diagnostics": {
                "cache_warm":                False,
                "odds_live_source":          "warming",
                "lkg_file_exists":           _LKG_PATH.exists(),
                "lkg_loaded":                False,
                "db_snapshot_fallback_loaded": False,
                "last_successful_scan_at":   None,
                "last_scan_error":           self._last_scan_error,
                "scanner_running":           self._scanner_running,
            },
        }

    # ── History endpoint ──────────────────────────────────────────────────────

    def get_history(self, family_key: str, days: int = 7) -> dict:
        """Return time-series probability history for a single family."""
        fdef = REGISTRY_BY_KEY.get(family_key)
        points_raw = _db_get_history(family_key, days=days)
        points = []
        for p in points_raw:
            yes_prob = p.get("yes_probability")
            points.append({
                "captured_at":    p.get("captured_at"),
                "yes_probability": yes_prob,
                "yes_pct":         round(float(yes_prob) * 100.0, 2) if yes_prob is not None else None,
                "volume_24h":      p.get("volume_24h"),
                "liquidity":       p.get("liquidity"),
            })
        return {
            "family_key":  family_key,
            "label":       fdef["label"] if fdef else family_key,
            "category":    fdef["category"] if fdef else "",
            "days":        days,
            "point_count": len(points),
            "points":      points,
        }

    # ── Diagnostics endpoint ──────────────────────────────────────────────────

    def get_diagnostics(self) -> dict:
        """
        Return last scanner diagnostics from memory — no DB calls, always fast.
        Updated by the background scanner after each completed scan cycle.
        """
        return dict(self._last_diagnostics)


# ── Module-level singleton ────────────────────────────────────────────────────

odds_scanner = OddsScanner()


