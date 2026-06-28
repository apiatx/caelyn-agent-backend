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
import logging
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

try:
    from data.predict_odds_store import (
        ensure_table,
        insert_snapshots,
        delete_old_snapshots,
        get_snapshots_before,
        get_history as _db_get_history,
        get_diagnostics as _db_get_diagnostics,
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


# ── Constants ─────────────────────────────────────────────────────────────────

_LIVE_CACHE_KEY  = "pm:odds:live"
_LIVE_CACHE_TTL  = 2100       # 35 min — slightly longer than scan cadence
_SCAN_LOCK       = asyncio.Lock()
_SNAPSHOTS_RETAIN_DAYS = 7

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
        # Last-good diagnostics — written after each scan, read by /diagnostics (no DB calls)
        self._last_diagnostics: dict = {
            "cache_warm":            False,
            "registry_family_count": len(ODDS_REGISTRY),
            "live_family_count":     0,
            "missing_family_count":  len(ODDS_REGISTRY),
        }

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
        async with _SCAN_LOCK:
            return await self._do_scan()

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

        catalog_markets_flattened    = crawl_stats.get("catalog_markets_flattened", 0)
        catalog_events_pages_fetched = crawl_stats.get("catalog_events_pages_fetched", 0)
        catalog_events_total         = crawl_stats.get("catalog_events_total", 0)

        # ── 3. Active filter + dedup by condition_id ──────────────────────────
        seen_cids: set[str] = set()
        active_pool: list[dict] = []
        for m in raw_markets:
            cid = m.get("conditionId") or m.get("condition_id") or ""
            if cid and cid in seen_cids:
                continue
            if cid:
                seen_cids.add(cid)
            if _is_active_raw(m):
                active_pool.append(m)

        active_open_count = len(active_pool)   # after is_active_raw, before investor exclusion

        # ── 4. Investor eligibility exclusion (sports + pop culture) ──────────
        sports_excluded_count      = 0
        pop_culture_excluded_count = 0
        excluded_categories_seen: set[str] = set()
        clean_pool: list[dict] = []
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

        investor_excluded_count = sports_excluded_count + pop_culture_excluded_count
        candidate_pool_size     = len(clean_pool)
        log.info(
            "[odds_scanner] pool: %d raw → %d active/open → %d investor-eligible "
            "(%d sports + %d pop-culture excluded)",
            len(raw_markets), active_open_count, candidate_pool_size,
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

        for fdef in ODDS_REGISTRY:
            fk = fdef["family_key"]
            candidates = _match_family(fdef, clean_pool)

            if not candidates:
                families_still_missing.append(fk)
                missing_list.append({
                    "family_key":        fk,
                    "label":             fdef["label"],
                    "category":          fdef["category"],
                    "priority":          fdef["priority"],
                    "dashboard_enabled": fdef["dashboard_enabled"],
                    "prophetik_enabled": fdef["prophetik_enabled"],
                    "preferred_outcome": fdef["preferred_outcome"],
                    "description":       fdef.get("description", ""),
                })
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
                # staging fields for CLOB + delta computation — popped below
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
                        "question":        best.get("question"),
                        "yes_pct":         yes_pct,
                        "price_change_1h": best.get("price_change_1h"),
                        "price_change_1d": best.get("price_change_1d"),
                        "price_change_1wk": best.get("price_change_1wk"),
                        "volume_24h":      best.get("volume_24h"),
                        "catalog_source":  "full_crawl" if crawl_success else "neon_fallback",
                    },
                },
            })

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
            # ── Scan timing ────────────────────────────────────────────────
            "scan_ms":                       scan_ms,
        }

        # ── 11. Assemble payload ──────────────────────────────────────────────
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
            # Diagnostics — top-level per spec, also accessible via get_diagnostics()
            "diagnostics":       diagnostics,
            # Private alias for get_diagnostics() helper (backward compat)
            "_scan_diag":        diagnostics,
        }

        _mem_cache.set(_LIVE_CACHE_KEY, payload, _LIVE_CACHE_TTL)

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
        self._last_diagnostics = {**diagnostics, "cache_warm": True}

        return payload

    # ── Payload access ────────────────────────────────────────────────────────

    def get_live_payload(self) -> Optional[dict]:
        """Return cached live payload, or None if scanner has not yet run."""
        return _mem_cache.get(_LIVE_CACHE_KEY)

    def get_live(self) -> dict:
        """
        Return cached live payload — never triggers an inline scan.
        If the scanner has not yet run, returns a 'warming' stub so callers
        always get a valid, fast response.
        """
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
            return cached
        # Cache cold — return warming stub (spec §D)
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
            "diagnostics":       {"cache_warm": False},
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


