"""
Prediction Market Odds Scanner.

Matches registry families against live Polymarket markets, persists snapshots
to Neon/Postgres, computes 1h / 24h / 7d deltas from stored history, and
maintains an in-memory payload cache.

Entry points
------------
odds_scanner.scan_and_persist()   → async; called by _odds_scanner_loop() in main.py
odds_scanner.get_live()           → async; returns cached payload, builds on miss
odds_scanner.get_live_payload()   → sync; returns cached dict or None if not warmed
odds_scanner.get_history(fk,days) → sync; returns time-series list from DB
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

# How many markets to fetch from Polymarket for scanning
_FETCH_LIMIT = 400


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
      - NOT a sports market (excluded via sports keywords elsewhere)
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


def _best_candidate(candidates: list[dict]) -> dict:
    """Pick the highest-volume candidate as primary market."""
    return max(candidates, key=lambda m: m.get("volume_24h") or 0)


# ── Core scanner ──────────────────────────────────────────────────────────────

class OddsScanner:
    """
    Fetches live Polymarket markets, matches them to registry families,
    persists snapshots, computes deltas, and caches the payload.
    """

    async def scan_and_persist(self) -> dict:
        """
        Full scan cycle:
          1. Fetch top markets from Polymarket (unfiltered, 400 limit)
          2. Match each registry family to the best available market
          3. Persist snapshot rows to Neon
          4. Compute 1h/24h/7d deltas from DB history
          5. Build + cache live payload
          6. Run retention (delete rows >7 days old)

        Returns the live payload dict.
        Thread-safe: acquires _SCAN_LOCK so concurrent callers don't double-scan.
        """
        async with _SCAN_LOCK:
            return await self._do_scan()

    async def _do_scan(self) -> dict:
        t0 = time.time()

        # ── 1. Fetch markets ──────────────────────────────────────────────────
        try:
            from services.predict.polymarket_intelligence import polymarket_intel
            all_markets = await polymarket_intel.get_top_markets(limit=_FETCH_LIMIT)
        except Exception as exc:
            log.warning("[odds_scanner] Polymarket fetch error: %s", exc)
            all_markets = []

        now_ts  = time.time()
        now_dt  = datetime.fromtimestamp(now_ts, tz=timezone.utc)
        entries: list[dict] = []
        snap_rows: list[dict] = []

        # ── 2 & 3. Match families + build snapshot rows ───────────────────────
        for fdef in ODDS_REGISTRY:
            fk = fdef["family_key"]
            candidates = _match_family(fdef, all_markets)

            if not candidates:
                entries.append(self._null_entry(fdef))
                continue

            candidates_sorted = sorted(candidates, key=lambda m: m.get("volume_24h") or 0, reverse=True)
            best = _best_candidate(candidates_sorted)

            yes_pct = best.get("yes_pct")     # 0-100 or None
            yes_prob = round(yes_pct / 100.0, 6) if yes_pct is not None else None

            snap_rows.append({
                "family_key":    fk,
                "market_id":     best.get("condition_id", "") or best.get("slug", "") or fk,
                "market_slug":   best.get("slug"),
                "question":      best.get("question"),
                "source":        "polymarket",
                "yes_probability": yes_prob,
                "no_probability": round(1.0 - yes_prob, 6) if yes_prob is not None else None,
                "best_bid":      best.get("best_bid"),
                "best_ask":      best.get("best_ask"),
                "volume_24h":    best.get("volume_24h"),
                "liquidity":     best.get("liquidity"),
                "end_date":      best.get("end_date_iso") or best.get("end_date"),
                "captured_at":   now_dt,
                "raw_json":      {
                    "question":        best.get("question"),
                    "yes_pct":         yes_pct,
                    "price_change_1h": best.get("price_change_1h"),
                    "price_change_1d": best.get("price_change_1d"),
                    "price_change_1wk": best.get("price_change_1wk"),
                    "volume_24h":      best.get("volume_24h"),
                },
            })

            driver_markets = [
                {
                    "question":    m.get("question", ""),
                    "yes_pct":     m.get("yes_pct"),
                    "volume_24h":  m.get("volume_24h", 0),
                    "delta_24h_pp": m.get("price_change_1d"),
                    "condition_id": m.get("condition_id", ""),
                    "slug":         m.get("slug", ""),
                }
                for m in candidates_sorted[:5]
            ]

            entries.append({
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
                "condition_id":      best.get("condition_id", ""),
                "slug":              best.get("slug", ""),
                "volume_24h":        best.get("volume_24h"),
                "liquidity":         best.get("liquidity"),
                "candidate_count":   len(candidates),
                "driver_markets":    driver_markets,
                "_api_1h":           best.get("price_change_1h"),
                "_api_24h":          best.get("price_change_1d"),
                "_api_7d":           best.get("price_change_1wk"),
                "_yes_pct_raw":      yes_pct,
            })

        # ── 4. Persist to Neon (non-blocking if DB unavailable) ───────────────
        if snap_rows:
            try:
                inserted = insert_snapshots(snap_rows)
                log.debug("[odds_scanner] Persisted %d snapshot rows", inserted)
            except Exception as exc:
                log.warning("[odds_scanner] insert_snapshots error: %s", exc)

        # ── 5. Compute deltas from DB history ─────────────────────────────────
        for entry in entries:
            if entry.get("yes_probability") is None:
                entry["delta_1h_pp"]  = None
                entry["delta_24h_pp"] = None
                entry["delta_7d_pp"]  = None
                continue
            deltas = _compute_deltas(
                family_key=entry["family_key"],
                current_yes_pct=entry.get("_yes_pct_raw"),
                now_ts=now_ts,
                api_fallbacks={
                    "delta_1h_api":  entry.pop("_api_1h", None),
                    "delta_24h_api": entry.pop("_api_24h", None),
                    "delta_7d_api":  entry.pop("_api_7d", None),
                },
            )
            entry.update(deltas)
            # Remove private staging fields
            entry.pop("_yes_pct_raw", None)
            entry.pop("_api_1h", None)
            entry.pop("_api_24h", None)
            entry.pop("_api_7d", None)

        entries_sorted = sorted(entries, key=lambda e: e.get("priority", 99))

        scan_ms = round((time.time() - t0) * 1000)
        payload = {
            "scanned_at":    now_dt.isoformat(),
            "scan_ms":       scan_ms,
            "total_families": len(ODDS_REGISTRY),
            "matched_families": sum(1 for e in entries if e.get("yes_probability") is not None),
            "missing_families": sum(1 for e in entries if e.get("yes_probability") is None),
            "odds": entries_sorted,
        }

        _mem_cache.set(_LIVE_CACHE_KEY, payload, _LIVE_CACHE_TTL)

        # ── 6. Retention (fire-and-forget, don't block caller) ────────────────
        try:
            loop = asyncio.get_running_loop()
            loop.run_in_executor(None, delete_old_snapshots, 7)
        except Exception:
            pass

        log.info(
            "[odds_scanner] scan complete — %d/%d families matched, %d ms",
            payload["matched_families"], payload["total_families"], scan_ms,
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

        Response shape:
        {
          "family_key": "fed_rate_decision",
          "label": "...",
          "days": 7,
          "points": [
            {"captured_at": "...", "yes_probability": 0.63, "yes_pct": 63.0,
             "volume_24h": 45000.0, "liquidity": 12000.0},
            ...
          ]
        }
        """
        fdef = REGISTRY_BY_KEY.get(family_key)
        points_raw = _db_get_history(family_key, days=days)
        points = []
        for p in points_raw:
            yes_prob = p.get("yes_probability")
            points.append({
                "captured_at":   p.get("captured_at"),
                "yes_probability": yes_prob,
                "yes_pct":       round(float(yes_prob) * 100.0, 2) if yes_prob is not None else None,
                "volume_24h":    p.get("volume_24h"),
                "liquidity":     p.get("liquidity"),
            })
        return {
            "family_key": family_key,
            "label":      fdef["label"] if fdef else family_key,
            "category":   fdef["category"] if fdef else "",
            "days":       days,
            "point_count": len(points),
            "points":     points,
        }

    # ── Null stub helpers ─────────────────────────────────────────────────────

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
        payload = self.get_live_payload()
        return {
            "cache_warm":        payload is not None,
            "scanned_at":        payload.get("scanned_at") if payload else None,
            "matched_families":  payload.get("matched_families") if payload else None,
            "total_families":    len(ODDS_REGISTRY),
            "store_available":   _STORE_AVAILABLE,
            "db_stats":          _db_get_diagnostics(),
        }


# ── Module-level singleton ────────────────────────────────────────────────────

odds_scanner = OddsScanner()
