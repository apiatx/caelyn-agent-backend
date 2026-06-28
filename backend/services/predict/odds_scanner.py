"""
Prediction Market Odds Scanner — Two-Lane Discovery.

Lane A: broad top-market scan (get_top_markets limit=400, unfiltered by tag)
Lane B: 6 parallel tag-specific fetches (economy/finance/crypto/geopolitics/tech/politics)
        that surface lower-volume but macro-relevant markets not in the top 400.

After deduplication by condition_id and sports exclusion, keyword-match all 26 registry
families against the merged pool, persist snapshots to Neon, and cache the live payload.

Entry points
------------
odds_scanner.scan_and_persist()   → async; called by _odds_scanner_loop() in main.py
odds_scanner.get_live()           → async; returns cached payload, builds on miss
odds_scanner.get_live_payload()   → sync; returns cached dict or None if not warmed
odds_scanner.get_history(fk,days) → sync; returns time-series list from DB
odds_scanner.get_diagnostics()    → sync; returns scan + DB diagnostics
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
        get_latest_per_family,
        get_snapshots_before,
        get_history as _db_get_history,
        get_diagnostics as _db_get_diagnostics,
    )
    _STORE_AVAILABLE = True
except Exception as _e:
    log.warning("[odds_scanner] predict_odds_store import error: %s", _e)
    _STORE_AVAILABLE = False
    def ensure_table(*a, **kw): return False   # type: ignore
    def insert_snapshots(*a, **kw): return 0   # type: ignore
    def delete_old_snapshots(*a, **kw): return 0   # type: ignore
    def get_latest_per_family(*a, **kw): return {}   # type: ignore
    def get_snapshots_before(*a, **kw): return []    # type: ignore
    def _db_get_history(*a, **kw): return []         # type: ignore
    def _db_get_diagnostics(*a, **kw): return {}     # type: ignore

try:
    from data.cache import cache as _mem_cache
except Exception:
    class _FallbackCache:  # type: ignore
        _d: dict = {}
        def get(self, k, default=None): return self._d.get(k, default)
        def set(self, k, v, ttl=None): self._d[k] = v
    _mem_cache = _FallbackCache()


# ── Constants ─────────────────────────────────────────────────────────────────

_LIVE_CACHE_KEY = "pm:odds:live"
_LIVE_CACHE_TTL = 2100       # 35 min — slightly longer than scan cadence
_SCAN_LOCK = asyncio.Lock()

# Window tolerances for delta computation (seconds)
_DELTA_1H_TARGET  = 3600
_DELTA_24H_TARGET = 86400
_DELTA_7D_TARGET  = 604800
_DELTA_1H_WINDOW  = 900    # ±15 min
_DELTA_24H_WINDOW = 1800   # ±30 min
_DELTA_7D_WINDOW  = 7200   # ±2 h

# Lane A: broad top-market fetch limit (sorted by 24h volume, unfiltered by tag)
_FETCH_LIMIT = 400

# Lane B: tag-specific fetches run in parallel alongside Lane A.
# Each tuple is (gamma_tag_label, max_markets_to_fetch).
# These tags cover all 26 registry families across their categories.
# economy     → Fed decisions, recession, CPI, tariffs, Hormuz
# finance     → stocks, oil, gold, earnings, Fed cuts
# crypto      → Bitcoin and other crypto milestones
# geopolitics → Russia/Ukraine, China/Taiwan, Israel/Gaza, Iran
# tech        → AI/chip export controls, semiconductor regulation
# politics    → macro policy, tariff legislation context
_LANE_B_TAGS: list[tuple[str, int]] = [
    ("economy",     300),
    ("finance",     300),
    ("crypto",      200),
    ("geopolitics", 200),
    ("tech",        150),
    ("politics",    150),
]


# ── Delta computation ─────────────────────────────────────────────────────────

def _compute_deltas(
    family_key: str,
    current_yes_pct: Optional[float],
    now_ts: float,
    api_fallbacks: dict,
) -> dict:
    """
    Compute 1h / 24h / 7d probability deltas in percentage-point (pp) units
    from stored DB snapshots.  Falls back to Polymarket's own price_change
    fields when DB history is too young.

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
        # yes_probability stored as 0–1; current_yes_pct is 0–100
        return round(current_yes_pct - (float(prior_yes) * 100.0), 2)

    delta_1h  = _delta_from_db(_DELTA_1H_TARGET, _DELTA_1H_WINDOW)
    delta_24h = _delta_from_db(_DELTA_24H_TARGET, _DELTA_24H_WINDOW)
    delta_7d  = _delta_from_db(_DELTA_7D_TARGET, _DELTA_7D_WINDOW)

    # Fall back to Polymarket's own API delta fields when DB history is shallow
    if delta_1h is None and api_fallbacks.get("delta_1h_api") is not None:
        delta_1h = round(float(api_fallbacks["delta_1h_api"]), 2)
    if delta_24h is None and api_fallbacks.get("delta_24h_api") is not None:
        delta_24h = round(float(api_fallbacks["delta_24h_api"]), 2)
    if delta_7d is None and api_fallbacks.get("delta_7d_api") is not None:
        delta_7d = round(float(api_fallbacks["delta_7d_api"]), 2)

    return {
        "delta_1h_pp":  delta_1h,
        "delta_24h_pp": delta_24h,
        "delta_7d_pp":  delta_7d,
    }


# ── Market-to-family matching ─────────────────────────────────────────────────

def _match_family(fdef: dict, all_markets: list[dict]) -> list[dict]:
    """
    Return all markets that match this family definition.
    A market matches if:
      - question.lower() contains at least one keyword_pattern (OR logic)
      - question.lower() contains NONE of the exclude_patterns
    Sports markets have already been filtered from all_markets before this call.
    """
    patterns = [p.lower() for p in fdef.get("keyword_patterns", [])]
    exclude  = [p.lower() for p in fdef.get("exclude_patterns", [])]

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
    Two-lane Polymarket discovery:
      Lane A — broad top-market scan (sorted by 24h volume, limit=400)
      Lane B — 6 parallel tag-specific fetches covering all registry categories

    After merging and deduplicating by condition_id, keyword-matches all 26
    registry families, persists snapshots to Neon, computes deltas, and caches
    the live payload.
    """

    async def scan_and_persist(self) -> dict:
        """
        Full scan cycle.  Thread-safe: acquires _SCAN_LOCK so concurrent
        callers don't double-scan.
        """
        async with _SCAN_LOCK:
            return await self._do_scan()

    # ── Lane B helper ──────────────────────────────────────────────────────

    async def _fetch_lane_b(self, polymarket_intel: Any) -> tuple[list[dict], dict]:
        """
        Run all Lane B tag fetches in parallel.
        Returns (enriched_markets, stats_dict).
        Each tag result is already enriched + active-filtered by get_top_markets().
        """
        tasks = [
            polymarket_intel.get_top_markets(limit=lim, tag=tag)
            for tag, lim in _LANE_B_TAGS
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_markets: list[dict] = []
        queries_run = 0
        for i, res in enumerate(results):
            queries_run += 1
            if isinstance(res, Exception):
                log.warning(
                    "[odds_scanner] Lane B tag=%r error: %s",
                    _LANE_B_TAGS[i][0], res,
                )
            elif isinstance(res, list):
                all_markets.extend(res)

        return all_markets, {"targeted_queries_run": queries_run}

    # ── Core scan ──────────────────────────────────────────────────────────

    async def _do_scan(self) -> dict:
        t0 = time.time()

        # ── 1. Import once inside scan ────────────────────────────────────
        try:
            from services.predict.polymarket_intelligence import (
                polymarket_intel,
                _is_sports_market,
            )
        except Exception as exc:
            log.warning("[odds_scanner] polymarket_intelligence import error: %s", exc)
            polymarket_intel = None
            def _is_sports_market(m: dict) -> bool:  # type: ignore
                return False

        # ── 2. Parallel Lane A + Lane B fetch ─────────────────────────────
        if polymarket_intel is not None:
            lane_a_task = polymarket_intel.get_top_markets(limit=_FETCH_LIMIT)
            lane_b_task = self._fetch_lane_b(polymarket_intel)
            ab = await asyncio.gather(lane_a_task, lane_b_task, return_exceptions=True)

            lane_a: list[dict] = ab[0] if not isinstance(ab[0], Exception) else []
            if isinstance(ab[0], Exception):
                log.warning("[odds_scanner] Lane A error: %s", ab[0])

            if isinstance(ab[1], Exception):
                log.warning("[odds_scanner] Lane B error: %s", ab[1])
                lane_b_markets: list[dict] = []
                lane_b_stats: dict = {"targeted_queries_run": 0}
            else:
                lane_b_markets, lane_b_stats = ab[1]
        else:
            lane_a = []
            lane_b_markets = []
            lane_b_stats = {"targeted_queries_run": 0}

        broad_candidates_seen  = len(lane_a)
        targeted_candidates_seen = len(lane_b_markets)

        # ── 3. Merge + dedupe by condition_id ─────────────────────────────
        # Lane A markets take precedence (already enriched + sorted by volume).
        # Lane B markets that share a condition_id with Lane A are skipped.
        broad_cids: set[str] = set()
        merged: list[dict] = []

        for m in lane_a:
            cid = m.get("condition_id") or ""
            broad_cids.add(cid)
            merged.append(m)

        dedupe_count = 0
        seen_cids: set[str] = set(broad_cids)
        for m in lane_b_markets:
            cid = m.get("condition_id") or ""
            if cid and cid in seen_cids:
                dedupe_count += 1
                continue
            if cid:
                seen_cids.add(cid)
            merged.append(m)

        merged_candidates_seen  = len(merged)
        candidate_dedupe_count  = dedupe_count

        # ── 4. Sports exclusion ───────────────────────────────────────────
        sports_excluded_count = 0
        clean_pool: list[dict] = []
        for m in merged:
            if _is_sports_market(m):
                sports_excluded_count += 1
            else:
                clean_pool.append(m)

        now_ts = time.time()
        now_dt = datetime.fromtimestamp(now_ts, tz=timezone.utc)

        # ── 5. Match families against merged pool ─────────────────────────
        live_entries:  list[dict] = []   # families with a live market
        missing_list:  list[dict] = []   # families with no match
        snap_rows:     list[dict] = []

        families_by_broad:    list[str] = []
        families_by_targeted: list[str] = []
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

            candidates_sorted = sorted(
                candidates, key=lambda m: m.get("volume_24h") or 0, reverse=True
            )
            best = candidates_sorted[0]
            best_cid = best.get("condition_id") or ""

            discovery_lane = "broad" if best_cid in broad_cids else "targeted"
            if discovery_lane == "broad":
                families_by_broad.append(fk)
            else:
                families_by_targeted.append(fk)

            yes_pct  = best.get("yes_pct")
            yes_prob = round(yes_pct / 100.0, 6) if yes_pct is not None else None

            snap_rows.append({
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
                "end_date":        best.get("end_date_iso") or best.get("end_date"),
                "captured_at":     now_dt,
                "raw_json": {
                    "question":          best.get("question"),
                    "yes_pct":           yes_pct,
                    "price_change_1h":   best.get("price_change_1h"),
                    "price_change_1d":   best.get("price_change_1d"),
                    "price_change_1wk":  best.get("price_change_1wk"),
                    "volume_24h":        best.get("volume_24h"),
                    "discovery_lane":    discovery_lane,
                },
            })

            driver_markets = [
                {
                    "question":     m.get("question", ""),
                    "yes_pct":      m.get("yes_pct"),
                    "volume_24h":   m.get("volume_24h", 0),
                    "delta_24h_pp": m.get("price_change_1d"),
                    "condition_id": m.get("condition_id", ""),
                    "slug":         m.get("slug", ""),
                }
                for m in candidates_sorted[:5]
            ]

            live_entries.append({
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
                "discovery_lane":    discovery_lane,
                # staging fields for delta computation — popped below
                "_api_1h":       best.get("price_change_1h"),
                "_api_24h":      best.get("price_change_1d"),
                "_api_7d":       best.get("price_change_1wk"),
                "_yes_pct_raw":  yes_pct,
            })

        # ── 6. Persist snapshots ──────────────────────────────────────────
        snapshots_written = 0
        if snap_rows:
            try:
                snapshots_written = insert_snapshots(snap_rows)
                log.debug("[odds_scanner] Persisted %d snapshot rows", snapshots_written)
            except Exception as exc:
                log.warning("[odds_scanner] insert_snapshots error: %s", exc)

        # ── 7. Compute deltas from DB history ─────────────────────────────
        for entry in live_entries:
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

        entries_sorted = sorted(live_entries, key=lambda e: e.get("priority", 99))
        missing_sorted = sorted(missing_list, key=lambda e: e.get("priority", 99))

        scan_ms = round((time.time() - t0) * 1000)

        # ── 8. Diagnostics block (stored inside payload) ──────────────────
        scan_diag: dict = {
            "broad_candidates_seen":              broad_candidates_seen,
            "targeted_queries_run":               lane_b_stats.get("targeted_queries_run", 0),
            "targeted_candidates_seen":           targeted_candidates_seen,
            "merged_candidates_seen":             merged_candidates_seen,
            "candidate_dedupe_count":             candidate_dedupe_count,
            "live_family_count":                  len(live_entries),
            "missing_family_count":               len(missing_list),
            "families_matched_by_broad_scan":     families_by_broad,
            "families_matched_by_targeted_search": families_by_targeted,
            "families_still_missing":             families_still_missing,
            "sports_excluded_count":              sports_excluded_count,
            "snapshots_written":                  snapshots_written,
        }

        payload = {
            "scanned_at":          now_dt.isoformat(),
            "scan_ms":             scan_ms,
            "total_families":      len(ODDS_REGISTRY),
            "matched_families":    len(live_entries),
            "missing_families_count": len(missing_list),
            # odds[] contains ONLY live matched families (no null stubs)
            "odds":                entries_sorted,
            # missing_families[] contains unmatched family metadata
            "missing_families":    missing_sorted,
            # internal diagnostics — exposed by get_diagnostics()
            "_scan_diag":          scan_diag,
        }

        _mem_cache.set(_LIVE_CACHE_KEY, payload, _LIVE_CACHE_TTL)

        # ── 9. Retention (fire-and-forget) ────────────────────────────────
        try:
            loop = asyncio.get_running_loop()
            loop.run_in_executor(None, delete_old_snapshots, 7)
        except Exception:
            pass

        log.info(
            "[odds_scanner] scan complete — %d/%d matched "
            "(broad=%d targeted=%d missing=%d) "
            "pool=%d(A=%d B=%d deduped=%d sports=%d) ms=%d",
            len(live_entries), len(ODDS_REGISTRY),
            len(families_by_broad), len(families_by_targeted), len(missing_list),
            merged_candidates_seen, broad_candidates_seen, targeted_candidates_seen,
            candidate_dedupe_count, sports_excluded_count, scan_ms,
        )
        return payload

    # ── Payload access ────────────────────────────────────────────────────────

    def get_live_payload(self) -> Optional[dict]:
        """Return cached live payload, or None if scanner has not yet run."""
        return _mem_cache.get(_LIVE_CACHE_KEY)

    async def get_live(self) -> dict:
        """Return live payload; triggers a scan on cache miss."""
        cached = self.get_live_payload()
        if cached is not None:
            return cached
        return await self.scan_and_persist()

    # ── History endpoint ──────────────────────────────────────────────────────

    def get_history(self, family_key: str, days: int = 7) -> dict:
        """
        Return time-series probability history for a single family.
        """
        fdef = REGISTRY_BY_KEY.get(family_key)
        points_raw = _db_get_history(family_key, days=days)
        points = []
        for p in points_raw:
            yes_prob = p.get("yes_probability")
            points.append({
                "captured_at":     p.get("captured_at"),
                "yes_probability":  yes_prob,
                "yes_pct":          round(float(yes_prob) * 100.0, 2) if yes_prob is not None else None,
                "volume_24h":       p.get("volume_24h"),
                "liquidity":        p.get("liquidity"),
            })
        return {
            "family_key":   family_key,
            "label":        fdef["label"] if fdef else family_key,
            "category":     fdef["category"] if fdef else "",
            "days":         days,
            "point_count":  len(points),
            "points":       points,
        }

    # ── Null stub helper (kept for backward-compat; not in odds[] anymore) ────

    @staticmethod
    def _null_entry(fdef: dict) -> dict:
        return {
            "family_key":        fdef["family_key"],
            "label":             fdef["label"],
            "category":          fdef["category"],
            "priority":          fdef["priority"],
            "dashboard_enabled": fdef["dashboard_enabled"],
            "prophetik_enabled": fdef["prophetik_enabled"],
            "preferred_outcome": fdef["preferred_outcome"],
            "description":       fdef.get("description", ""),
            "yes_probability":   None,
            "yes_pct":           None,
            "delta_1h_pp":       None,
            "delta_24h_pp":      None,
            "delta_7d_pp":       None,
            "volume_24h":        None,
            "liquidity":         None,
            "market_question":   None,
            "condition_id":      None,
            "slug":              None,
            "candidate_count":   0,
            "driver_markets":    [],
        }

    # ── Diagnostics ───────────────────────────────────────────────────────────

    def get_diagnostics(self) -> dict:
        payload  = self.get_live_payload()
        scan_diag = payload.get("_scan_diag", {}) if payload else {}
        return {
            "cache_warm":          payload is not None,
            "scanned_at":          payload.get("scanned_at") if payload else None,
            "scan_ms":             payload.get("scan_ms") if payload else None,
            "matched_families":    payload.get("matched_families") if payload else None,
            "missing_families_count": payload.get("missing_families_count") if payload else None,
            "total_families":      len(ODDS_REGISTRY),
            "store_available":     _STORE_AVAILABLE,
            "db_stats":            _db_get_diagnostics(),
            # Two-lane discovery stats
            "broad_candidates_seen":              scan_diag.get("broad_candidates_seen"),
            "targeted_queries_run":               scan_diag.get("targeted_queries_run"),
            "targeted_candidates_seen":           scan_diag.get("targeted_candidates_seen"),
            "merged_candidates_seen":             scan_diag.get("merged_candidates_seen"),
            "candidate_dedupe_count":             scan_diag.get("candidate_dedupe_count"),
            "live_family_count":                  scan_diag.get("live_family_count"),
            "missing_family_count":               scan_diag.get("missing_family_count"),
            "families_matched_by_broad_scan":     scan_diag.get("families_matched_by_broad_scan"),
            "families_matched_by_targeted_search": scan_diag.get("families_matched_by_targeted_search"),
            "families_still_missing":             scan_diag.get("families_still_missing"),
            "sports_excluded_count":              scan_diag.get("sports_excluded_count"),
            "snapshots_written":                  scan_diag.get("snapshots_written"),
            # Lane B tag config (for reference)
            "lane_b_tags": [{"tag": t, "limit": l} for t, l in _LANE_B_TAGS],
        }


# ── Module-level singleton ────────────────────────────────────────────────────

odds_scanner = OddsScanner()
