"""
Watchlist Router — FastAPI endpoints for multi-watchlist CRUD, news, refresh, and stock detail.
"""

from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from agent.model_policy import MODEL_CLAUDE_PREMIUM, MODEL_GROK, MODEL_GPT4O, MODEL_GEMINI

import asyncio
import json as _json
import math as _math
import re as _re
import time as _time
from datetime import datetime, timezone

import httpx
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict
from typing import Any, Dict, List, Optional

from services.watchlist_service import (
    save_watchlist,
    load_watchlist,
    list_watchlists,
    clear_watchlist,
    extract_tickers,
    fetch_news_for_tickers,
    get_stock_detail,
    _WATCHLIST_FILE,
)
from services.watchlist_analysis import run_analysis_pipeline
from services.news_major_service import build_major_developments as _build_major

router = APIRouter(prefix="/api/watchlist", tags=["watchlist"])

# ── Rel-vol rank snapshot: in-memory fallback (survives within process) ───────
# Keyed by watchlist_id → {"current": {SYM: {rank, rel_vol}},
#                           "previous": {SYM: {rank, rel_vol}} | None}
_rv_mem: dict[str, dict] = {}
_volmc_mem: dict[str, dict] = {}


# ── News endpoint LKG (last-known-good) cache ────────────────────────────────
# Keyed by watchlist_id (use "default" for the no-id endpoint).
# Stores the fully-built + scored response so callers get instant results while
# a background refresh runs silently in the background when data goes stale.
#
#   _NEWS_LKG_SERVE_TTL : serve stale data from cache and kick a bg refresh
#   per-ticker fetch TTL : controlled by _NEWS_CACHE_TTL in watchlist_service.py (30 min)
#
_news_lkg: dict[str, dict] = {}    # watchlist_id -> {"data": dict, "ts": float}
_news_bg_building: set[str] = set()
_NEWS_LKG_SERVE_TTL = 20 * 60      # 20 min — after this, serve stale + bg-refresh


# ── User identity helper (same pattern as routes/screener_hub._get_user_id) ──

def _get_user_id(request: Request) -> str:
    """
    Resolve user_id for the request.

    Priority:
      1. request.state.user_id (middleware, if ever re-enabled)
      2. Authorization: Bearer <JWT> → payload["sub"]
      3. "default" — unauthenticated / local-dev fallback
    """
    uid = getattr(request.state, "user_id", None)
    if uid:
        return str(uid)
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        token = auth[7:]
        try:
            from auth import verify_token
            payload = verify_token(token)
            sub = payload.get("sub")
            if sub:
                return str(sub)
        except Exception:
            pass
    return "default"


def _news_response(
    enriched_map: dict,
    major_summary: dict,
    ts: float,
    is_building: bool = False,
    debug_reason: str | None = None,
) -> dict:
    """Build the standardised Live News response."""
    age = round(_time.time() - ts)
    cached_at = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%dT%H:%M:%SZ")
    top_articles = major_summary.get("major_developments", [])
    resp = {
        # Original per-ticker map — preserved for any existing consumer
        "articles":         enriched_map,
        # Pre-ranked high-signal articles — use this for the Live News panel
        "top_articles":     top_articles,
        "high_signal_count": major_summary.get("high_signal_count", 0),
        "by_catalyst_type":  major_summary.get("by_catalyst_type", {}),
        "news_signal_meta":  major_summary.get("news_signal_meta", {}),
        # Cache metadata
        "cached_at":        cached_at,
        "cache_age_s":      age,
        "is_building":      is_building,
    }
    if debug_reason:
        resp["debug_reason"] = debug_reason
    elif not top_articles and not enriched_map:
        resp["debug_reason"] = "no_articles_returned"
    return resp


async def _bg_refresh_news(watchlist_id: str, tickers: list[str]) -> None:
    """Background task: silently refresh news LKG cache for a watchlist."""
    if watchlist_id in _news_bg_building:
        return
    _news_bg_building.add(watchlist_id)
    try:
        raw_map = await fetch_news_for_tickers(tickers)
        try:
            enriched_map, major_summary = _build_major(raw_map)
        except Exception as _e:
            print(f"[NEWS_LKG] major build error (non-fatal): {_e}")
            enriched_map, major_summary = raw_map, {}
        ts   = _time.time()
        data = _news_response(enriched_map, major_summary, ts)
        _news_lkg[watchlist_id] = {"data": data, "ts": ts}
        top_ct = len(data.get("top_articles") or [])
        art_ct = sum(len(v) for v in enriched_map.values())
        print(f"[NEWS_LKG] refresh done  wl={watchlist_id}  articles={art_ct}  top={top_ct}")
    except Exception as exc:
        print(f"[NEWS_LKG] refresh error wl={watchlist_id}: {exc}")
    finally:
        _news_bg_building.discard(watchlist_id)


async def _get_news_for_watchlist(watchlist_id: str, tickers: list[str]) -> dict:
    """
    Return a Live News response, using the LKG cache:
      - Hit  & fresh  (<20 min) → instant return, no I/O
      - Hit  & stale (≥20 min) → instant return of stale data + bg refresh
      - Miss (first call)       → blocking build, then cache
    """
    now = _time.time()
    lkg = _news_lkg.get(watchlist_id)

    if lkg:
        age  = now - lkg["ts"]
        data = dict(lkg["data"])
        data["cache_age_s"] = round(age)
        data["is_building"] = watchlist_id in _news_bg_building
        if age > _NEWS_LKG_SERVE_TTL and watchlist_id not in _news_bg_building:
            asyncio.create_task(_bg_refresh_news(watchlist_id, tickers))
            data["is_building"] = True
        return data

    # Cold start — build synchronously (only happens once per process restart)
    print(f"[NEWS_LKG] cold build  wl={watchlist_id}  tickers={len(tickers)}")
    cold_error: str | None = None
    try:
        raw_map = await fetch_news_for_tickers(tickers)
        try:
            enriched_map, major_summary = _build_major(raw_map)
        except Exception as _e:
            print(f"[NEWS_LKG] major build error (non-fatal): {_e}")
            enriched_map, major_summary = raw_map, {}
    except Exception as exc:
        print(f"[NEWS_LKG] cold build fetch error wl={watchlist_id}: {exc}")
        enriched_map, major_summary = {}, {}
        cold_error = f"fetch_error: {type(exc).__name__}"
    ts   = _time.time()
    data = _news_response(enriched_map, major_summary, ts,
                          debug_reason=cold_error)
    _news_lkg[watchlist_id] = {"data": data, "ts": ts}
    return data


# ── Market-cap string parser ─────────────────────────────────────────────────

def _parse_market_cap_str(raw: Any) -> float | None:
    """
    Parse a market-cap value from various CSV formats into a raw float (USD).

    Handles: "1.23B", "456.78M", "12.34K", "$1,234,567", "1234567890",
             "1.5T", numbers (int/float), None / empty / "-".
    Returns None when unparseable.
    """
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw) if raw > 0 else None
    s = str(raw).strip().replace(",", "").replace("$", "").replace(" ", "")
    if not s or s in ("-", "N/A", "n/a", "--"):
        return None
    multipliers = {"T": 1e12, "B": 1e9, "M": 1e6, "K": 1e3}
    upper = s.upper()
    for suffix, mult in multipliers.items():
        if upper.endswith(suffix):
            try:
                return float(upper[:-1]) * mult
            except ValueError:
                return None
    try:
        v = float(s)
        return v if v > 0 else None
    except ValueError:
        return None


def _vol_mc_fields(price: float | None, volume: float | None, market_cap: float | None) -> dict:
    """
    Compute Vol/MC ratio fields using the same formula as CaelynTerminalProvider.

    Returns dict with:
      market_cap, dollar_volume, vol_mc_ratio, vol_mc_pct,
      vol_mc_label, vol_mc_unavailable_reason
    All values are None-safe — never raises.
    """
    dollar_vol: float | None = None
    if price and volume and price > 0 and volume > 0:
        dollar_vol = round(price * volume, 2)

    vol_mc_ratio: float | None = None
    vol_mc_pct:   float | None = None
    vol_mc_label: str   | None = None
    vol_mc_unavail: str | None = None

    if dollar_vol is not None and market_cap and market_cap > 0:
        vol_mc_ratio = round(dollar_vol / market_cap, 6)
        vol_mc_pct   = round(vol_mc_ratio * 100, 4)
        if vol_mc_pct >= 10:
            vol_mc_label = "high"
        elif vol_mc_pct >= 5:
            vol_mc_label = "elevated"
        elif vol_mc_pct >= 1:
            vol_mc_label = "normal"
        else:
            vol_mc_label = "low"
    else:
        if not volume:
            vol_mc_unavail = "volume_unavailable"
        elif not market_cap:
            vol_mc_unavail = "market_cap_unavailable"
        else:
            vol_mc_unavail = "price_missing"

    return {
        "market_cap":                market_cap,
        "dollar_volume":             dollar_vol,
        "vol_mc_ratio":              vol_mc_ratio,
        "vol_mc_pct":                vol_mc_pct,
        "vol_mc_label":              vol_mc_label,
        "vol_mc_unavailable_reason": vol_mc_unavail,
    }


# ── Rel-vol rank + momentum helper ───────────────────────────────────────────

async def _apply_rv_rank_fields(
    watchlist_id: str,
    dedup_sections: list[dict],
    saved_normalized: list[str],
) -> list[dict]:
    """
    Compute relative-volume ranks for the current enrichment pass, compare
    against the previous snapshot (Neon-durable, in-memory fallback), and
    add the following fields to every ticker row:

        rel_vol_rank           int | None   — rank 1=highest rel_vol this pass
        rel_vol_prev_rank      int | None   — rank from previous snapshot
        rel_vol_rank_delta     int | None   — prev_rank - cur_rank (pos = moved up)
        rel_vol_trend          "up"|"down"|"flat"|"unknown"
        rel_vol_value_delta    float | None — cur_rel_vol - prev_rel_vol
        rel_vol_prev_value     float | None
        rel_vol_momentum_label "surging"|"rising"|"fading"|"falling"|"flat"|"unknown"

    Only writes a new snapshot when quote coverage is adequate (≥50% of
    US-eligible saved symbols have a numeric relative_volume), to avoid
    committing garbage signals during provider outages.

    Storage precedence: Neon (persistent across restarts) → in-memory dict
    (_rv_mem, survives within the process lifetime).
    """
    if not watchlist_id:
        return dedup_sections

    # ── 1. Build current snapshot from enriched rows ──────────────────────────
    eligible_rows: list[tuple[dict, float]] = []
    for section in dedup_sections:
        for row in section.get("tickers", []):
            rv = row.get("relative_volume")
            if isinstance(rv, (int, float)) and not _math.isnan(rv) and rv > 0:
                eligible_rows.append((row, float(rv)))

    eligible_rows.sort(key=lambda x: x[1], reverse=True)

    current_snap: dict[str, dict] = {}
    for rank_0, (row, rv) in enumerate(eligible_rows):
        sym = str(row.get("symbol", "")).upper()
        if sym:
            current_snap[sym] = {"rank": rank_0 + 1, "rel_vol": round(rv, 6)}

    # ── 2. Coverage guard — only save when ≥50% US-eligible symbols have rv ──
    us_eligible_count = sum(1 for s in saved_normalized if ":" not in s)
    rv_coverage = len(current_snap) / us_eligible_count if us_eligible_count > 0 else 0
    should_save = rv_coverage >= 0.5

    # ── 3. Load previous snapshot ─────────────────────────────────────────────
    prev_snap: dict[str, dict] | None = None

    # Try in-memory first (zero-latency within the process)
    mem_entry = _rv_mem.get(watchlist_id)
    if mem_entry:
        prev_snap = mem_entry.get("previous") or mem_entry.get("current")

    # Neon fallback (cross-restart persistence) — run in thread executor
    if prev_snap is None:
        try:
            loop = asyncio.get_event_loop()
            _cur, _prev = await loop.run_in_executor(
                None,
                lambda: _rv_neon_load(watchlist_id),
            )
            # On first run current==None, second run use current as previous
            prev_snap = _prev or _cur
        except Exception as _exc:
            print(f"[RV_RANK] Neon load error wl={watchlist_id}: {_exc}")

    # ── 4. Augment every row with rank/trend fields ───────────────────────────
    for section in dedup_sections:
        augmented: list[dict] = []
        for row in section.get("tickers", []):
            sym = str(row.get("symbol", "")).upper()
            cur_entry  = current_snap.get(sym)
            prev_entry = prev_snap.get(sym) if prev_snap else None

            cur_rank  = cur_entry["rank"]    if cur_entry  else None
            cur_rv    = cur_entry["rel_vol"] if cur_entry  else None
            prev_rank = prev_entry["rank"]    if prev_entry else None
            prev_rv   = prev_entry["rel_vol"] if prev_entry else None

            if cur_rank is not None and prev_rank is not None:
                delta = prev_rank - cur_rank   # positive = moved up (rank# fell)
                if delta > 0:
                    trend = "up"
                elif delta < 0:
                    trend = "down"
                else:
                    trend = "flat"
            else:
                delta = None
                trend  = "unknown"

            rv_delta = (
                round(cur_rv - prev_rv, 4)
                if cur_rv is not None and prev_rv is not None
                else None
            )

            if trend == "up" and delta is not None and delta >= 10:
                label = "surging"
            elif trend == "up":
                label = "rising"
            elif trend == "down" and delta is not None and delta <= -10:
                label = "falling"
            elif trend == "down":
                label = "fading"
            elif trend == "flat":
                label = "flat"
            else:
                label = "unknown"

            augmented.append({
                **row,
                "rel_vol_rank":           cur_rank,
                "rel_vol_prev_rank":      prev_rank,
                "rel_vol_rank_delta":     delta,
                "rel_vol_trend":          trend,
                "rel_vol_value_delta":    rv_delta,
                "rel_vol_prev_value":     prev_rv,
                "rel_vol_momentum_label": label,
            })
        section["tickers"] = augmented

    # ── 5. Save current snapshot (fire-and-forget) ────────────────────────────
    if should_save and current_snap:
        # Update in-memory immediately
        _rv_mem[watchlist_id] = {
            "previous": _rv_mem[watchlist_id]["current"] if watchlist_id in _rv_mem else None,
            "current":  current_snap,
        }
        # Persist to Neon in background
        loop = asyncio.get_event_loop()
        loop.run_in_executor(
            None,
            lambda: _rv_neon_save(watchlist_id, current_snap),
        )
        print(
            f"[RV_RANK] wl={watchlist_id} ranked={len(current_snap)} "
            f"prev_known={prev_snap is not None} "
            f"coverage={rv_coverage:.0%} saved={should_save}"
        )
    else:
        print(
            f"[RV_RANK] wl={watchlist_id} ranked={len(current_snap)} "
            f"coverage={rv_coverage:.0%} snapshot_skipped (coverage<50%)"
        )

    return dedup_sections


def _rv_neon_load(watchlist_id: str) -> tuple:
    """Thin sync wrapper around pg_storage.rv_snapshot_load."""
    try:
        from data.pg_storage import rv_snapshot_load
        return rv_snapshot_load(watchlist_id)
    except Exception as exc:
        print(f"[RV_RANK] pg load skipped: {exc}")
        return (None, None)


def _rv_neon_save(watchlist_id: str, current_snap: dict) -> None:
    """Thin sync wrapper around pg_storage.rv_snapshot_save."""
    try:
        from data.pg_storage import rv_snapshot_save
        rv_snapshot_save(watchlist_id, current_snap)
    except Exception as exc:
        print(f"[RV_RANK] pg save skipped: {exc}")


# ── Vol/MC rank + momentum helper ─────────────────────────────────────────────

_VOLMC_FLAT_THRESHOLD = 0.05   # percentage-point delta below which = "flat"
_VOLMC_SURGE_THRESHOLD = 1.0   # ppt delta for "surging" / "falling" labels


async def _apply_volmc_rank_fields(
    watchlist_id: str,
    dedup_sections: list[dict],
    saved_normalized: list[str],
) -> list[dict]:
    """
    Compute Vol/MC ranks for the current enrichment pass, compare against the
    previous snapshot (Neon-durable, in-memory fallback), and add the following
    additive fields to every ticker row:

        vol_mc_rank           int | None   — rank 1=highest vol_mc_pct this pass
        vol_mc_prev_rank      int | None   — rank from previous snapshot
        vol_mc_rank_delta     int | None   — prev_rank - cur_rank (pos = moved up)
        vol_mc_prev_pct       float | None — previous snapshot vol_mc_pct
        vol_mc_pct_delta      float | None — cur_vol_mc_pct - prev_vol_mc_pct
        vol_mc_trend          "up"|"down"|"flat"|"unknown"
        vol_mc_momentum_label "surging"|"rising"|"flat"|"fading"|"falling"|"unknown"

    Trend threshold: |vol_mc_pct_delta| < 0.05 ppt → "flat".
    Momentum labels use value delta magnitude:
        ≥ +1.0 ppt  → surging   |  < +1.0 ppt up   → rising
        ≤ -1.0 ppt  → falling   |  > -1.0 ppt down  → fading

    Coverage guard: only saves snapshot when ≥50% of US-eligible symbols have
    a valid vol_mc_pct (reuses same threshold as rv_rank to reject outage data).

    Storage: Neon (persistent across restarts) → _volmc_mem (process-local).
    """
    if not watchlist_id:
        return dedup_sections

    # ── 1. Build current snapshot ─────────────────────────────────────────────
    eligible_rows: list[tuple[dict, float]] = []
    for section in dedup_sections:
        for row in section.get("tickers", []):
            pct = row.get("vol_mc_pct")
            if isinstance(pct, (int, float)) and not _math.isnan(pct) and pct > 0:
                eligible_rows.append((row, float(pct)))

    eligible_rows.sort(key=lambda x: x[1], reverse=True)

    current_snap: dict[str, dict] = {}
    for rank_0, (row, pct) in enumerate(eligible_rows):
        sym = str(row.get("symbol", "")).upper()
        if sym:
            current_snap[sym] = {"rank": rank_0 + 1, "vol_mc_pct": round(pct, 6)}

    # ── 2. Coverage guard — only save when ≥50% US-eligible have vol_mc_pct ──
    us_eligible_count = sum(1 for s in saved_normalized if ":" not in s)
    vm_coverage = len(current_snap) / us_eligible_count if us_eligible_count > 0 else 0
    should_save = vm_coverage >= 0.5

    # ── 3. Load previous snapshot ─────────────────────────────────────────────
    prev_snap: dict[str, dict] | None = None

    mem_entry = _volmc_mem.get(watchlist_id)
    if mem_entry:
        prev_snap = mem_entry.get("previous") or mem_entry.get("current")

    if prev_snap is None:
        try:
            loop = asyncio.get_event_loop()
            _cur, _prev = await loop.run_in_executor(
                None,
                lambda: _volmc_neon_load(watchlist_id),
            )
            prev_snap = _prev or _cur
        except Exception as _exc:
            print(f"[VOLMC_RANK] Neon load error wl={watchlist_id}: {_exc}")

    # ── 4. Augment every row ──────────────────────────────────────────────────
    for section in dedup_sections:
        augmented: list[dict] = []
        for row in section.get("tickers", []):
            sym = str(row.get("symbol", "")).upper()
            cur_entry  = current_snap.get(sym)
            prev_entry = prev_snap.get(sym) if prev_snap else None

            cur_rank   = cur_entry["rank"]       if cur_entry  else None
            cur_pct    = cur_entry["vol_mc_pct"] if cur_entry  else None
            prev_rank  = prev_entry["rank"]       if prev_entry else None
            prev_pct   = prev_entry["vol_mc_pct"] if prev_entry else None

            # Rank delta (optional fields)
            rank_delta: int | None = None
            if cur_rank is not None and prev_rank is not None:
                rank_delta = prev_rank - cur_rank  # positive = moved up

            # Value delta + trend (primary signal)
            pct_delta: float | None = None
            if cur_pct is not None and prev_pct is not None:
                pct_delta = round(cur_pct - prev_pct, 6)

            if pct_delta is None:
                trend = "unknown"
            elif pct_delta > _VOLMC_FLAT_THRESHOLD:
                trend = "up"
            elif pct_delta < -_VOLMC_FLAT_THRESHOLD:
                trend = "down"
            else:
                trend = "flat"

            if trend == "up" and pct_delta is not None and pct_delta >= _VOLMC_SURGE_THRESHOLD:
                label = "surging"
            elif trend == "up":
                label = "rising"
            elif trend == "down" and pct_delta is not None and pct_delta <= -_VOLMC_SURGE_THRESHOLD:
                label = "falling"
            elif trend == "down":
                label = "fading"
            elif trend == "flat":
                label = "flat"
            else:
                label = "unknown"

            augmented.append({
                **row,
                "vol_mc_rank":           cur_rank,
                "vol_mc_prev_rank":      prev_rank,
                "vol_mc_rank_delta":     rank_delta,
                "vol_mc_prev_pct":       prev_pct,
                "vol_mc_pct_delta":      pct_delta,
                "vol_mc_trend":          trend,
                "vol_mc_momentum_label": label,
            })
        section["tickers"] = augmented

    # ── 5. Save current snapshot (fire-and-forget) ────────────────────────────
    if should_save and current_snap:
        _volmc_mem[watchlist_id] = {
            "previous": _volmc_mem[watchlist_id]["current"] if watchlist_id in _volmc_mem else None,
            "current":  current_snap,
        }
        loop = asyncio.get_event_loop()
        loop.run_in_executor(
            None,
            lambda: _volmc_neon_save(watchlist_id, current_snap),
        )
        print(
            f"[VOLMC_RANK] wl={watchlist_id} ranked={len(current_snap)} "
            f"prev_known={prev_snap is not None} "
            f"coverage={vm_coverage:.0%} saved={should_save}"
        )
    else:
        print(
            f"[VOLMC_RANK] wl={watchlist_id} ranked={len(current_snap)} "
            f"coverage={vm_coverage:.0%} snapshot_skipped (coverage<50%)"
        )

    return dedup_sections


def _volmc_neon_load(watchlist_id: str) -> tuple:
    """Thin sync wrapper around pg_storage.volmc_snapshot_load."""
    try:
        from data.pg_storage import volmc_snapshot_load
        return volmc_snapshot_load(watchlist_id)
    except Exception as exc:
        print(f"[VOLMC_RANK] pg load skipped: {exc}")
        return (None, None)


def _volmc_neon_save(watchlist_id: str, current_snap: dict) -> None:
    """Thin sync wrapper around pg_storage.volmc_snapshot_save."""
    try:
        from data.pg_storage import volmc_snapshot_save
        volmc_snapshot_save(watchlist_id, current_snap)
    except Exception as exc:
        print(f"[VOLMC_RANK] pg save skipped: {exc}")


# ── Weinstein Stage Analysis (cache-only, no live fetches) ───────────────────

def _get_stage2_breakout(sym: str) -> dict:
    """
    Return a slim stage2_breakout dict for *sym*.  Zero I/O — reads only from
    already-populated caches.  Never fetches bars during page render.

    Lookup order:
      1. watchlist_stage2_service LKG (disk-backed, populated by off-hours warmup)
      2. In-memory bar caches (fmp_hist:{SYM} / tdier_hist:{SYM}:400, set by
         theme_rs_service for ETF/stock proxy symbols)

    Returns {"score": float|None, "label": str|None, "reason": str|None}
    """
    _null: dict = {"score": None, "label": None, "reason": None}
    try:
        # ── 1. Disk-backed LKG (primary — covers all 302 watchlist tickers) ─
        from services.watchlist_stage2_service import get_stage2 as _gs2
        lkg = _gs2(sym)
        # Return if we have a real result OR an explicit null stored by warmup
        # (explicit null = computed_at is present in the internal dict)
        from services import watchlist_stage2_service as _s2svc
        if _s2svc._STAGE2_LKG.get(sym.upper()):
            return lkg   # includes deliberate nulls written by warmup

        # ── 2. In-memory bar probe (theme_rs proxy symbols — ETFs, stock proxies) ─
        from data.cache import cache as _cache
        from services.stage_analysis import weekly_bars_from_daily, analyze_symbol_stage

        s = sym.upper()
        daily: list[dict] | None = (
            _cache.get(f"fmp_hist:{s}")
            or _cache.get(f"tdier_hist:{s}:400")
        )
        if not daily:
            return _null

        weekly = weekly_bars_from_daily(daily)
        if len(weekly) < 35:
            return _null

        spy_daily: list[dict] | None = (
            _cache.get("fmp_hist:SPY")
            or _cache.get("tdier_hist:SPY:400")
        )
        spy_weekly = weekly_bars_from_daily(spy_daily) if spy_daily else None

        result = analyze_symbol_stage(
            weekly_bars=weekly,
            spy_weekly_bars=spy_weekly,
            source="watchlist_cached_bars",
        )
        return {
            "score":  result.get("stage_score"),
            "label":  result.get("stage_label"),
            "reason": result.get("stage_reason"),
        }
    except Exception as _e:
        print(f"[WATCHLIST_STAGE] {sym}: {_e}")
        return _null


# ── Quote enrichment helper ──────────────────────────────────────────────────

async def _enrich_store_with_quotes(store: dict) -> dict:
    """
    Enrich every ticker row in store['analysis']['sections'] with:
      - name         from Tradier quote description (if not already present)
      - price        from Tradier live price (or CSV Stock Price fallback)
      - change_pct_1d from Tradier 1D change %
      - quote_source / quote_updated_at

    Existing rich fields (catalyst, sentiment, action_note, etc.) are preserved.
    Foreign/exchange-prefixed tickers that Tradier cannot quote keep whatever
    fields they already have.  This function never blocks — it uses the LKG
    quote cache and triggers a background refresh when the 10-min TTL expires.

    FALLBACK: when analysis.sections is empty but tickers are saved (e.g. analysis
    has not yet completed for a large watchlist), a single synthetic "All Tickers"
    section is built from the raw ticker list + CSV data + quote cache so the
    frontend table never renders 0 rows while analysis is pending.
    """
    import time as _time
    _t0 = _time.monotonic()

    from services.watchlist_quote_cache import get_watchlist_quotes

    tickers: list[str]   = store.get("tickers", [])
    csv_data: list[dict] = store.get("csv_data", [])
    analysis: dict       = store.get("analysis") or {}
    sections: list[dict] = analysis.get("sections", [])

    # ── Phase 4A: register watchlist demand for quote priority ────────────────
    if tickers:
        try:
            import data.quote_demand_registry as _qdr
            _qdr.register(tickers, "watchlist", ttl=90)
        except Exception:
            pass

    if not tickers:
        return store

    # CSV fundamentals map — keyed by SYMBOL (uppercase)
    csv_map: dict[str, dict] = {}
    for row in csv_data:
        sym = (row.get("Symbol") or row.get("symbol") or row.get("Ticker") or "").strip().upper()
        if sym:
            csv_map[sym] = row

    # Get cached quotes (non-blocking); stale cache triggers background refresh
    quote_map: dict[str, dict] = {}
    try:
        quote_map = await get_watchlist_quotes(tickers)
    except Exception as _qe:
        print(f"[WATCHLIST_ENRICH] quote fetch failed (non-fatal): {_qe}")

    now_str = datetime.now(timezone.utc).isoformat() + "Z"

    # Load name overrides once for this enrichment pass — applied last in
    # _build_ticker_row so they win over Tradier description, FMP quote name,
    # and CSV columns.  Cache-backed; no DB hit if cache is warm.
    _name_overrides: dict[str, str] = {}
    try:
        from services.name_overrides import get_name_overrides as _get_name_overrides
        _name_overrides = _get_name_overrides("default")
    except Exception as _nov_err:
        print(f"[WATCHLIST_ENRICH] name overrides load failed (non-fatal): {_nov_err}")

    def _build_ticker_row(sym: str, base_row: dict) -> dict:
        """Build one enriched ticker row from quote + CSV data."""
        sym = sym.strip().upper()
        q       = quote_map.get(sym, {})
        csv_row = csv_map.get(sym, {})
        enriched = dict(base_row)

        # ── Name ──────────────────────────────────────────────────────────
        # Priority: name override (DB) > Tradier description > FMP quote name
        #           > CSV column > ticker symbol
        if not enriched.get("name"):
            enriched["name"] = (
                q.get("name")
                or csv_row.get("Company Name")
                or csv_row.get("Name")
                or sym
            )
        # Manual override wins over everything — applied after initial name
        # resolution so it can correct wrong API names without blocking them.
        _override_name = _name_overrides.get(sym)
        if _override_name:
            enriched["name"] = _override_name

        # ── Price ──────────────────────────────────────────────────────────
        if q.get("price") is not None:
            enriched["price"] = q["price"]
        elif not enriched.get("price"):
            csv_price = csv_row.get("Stock Price")
            if csv_price:
                try:
                    enriched["price"] = float(csv_price)
                except Exception:
                    pass

        # ── Live market fields (always overwrite with freshest Tradier data)
        if q:
            enriched["change_pct_1d"]  = q.get("change_pct_1d")
            enriched["volume"]         = q.get("volume")
            enriched["average_volume"] = q.get("average_volume")
            rel_vol = q.get("relative_volume")
            if rel_vol is None:
                v  = q.get("volume")
                av = q.get("average_volume")
                if v is not None and av:
                    try:
                        rel_vol = round(float(v) / float(av), 4)
                    except Exception:
                        rel_vol = None
            enriched["relative_volume"]  = rel_vol
            enriched["quote_source"]     = q.get("quote_source") or "tradier"
            enriched["quote_updated_at"] = q.get("quote_updated_at", now_str)

        # ── Vol/MC ratio ───────────────────────────────────────────────────
        _raw_mc = (
            csv_row.get("Market Cap")
            or csv_row.get("MarketCap")
            or csv_row.get("market_cap")
            or enriched.get("market_cap")
        )
        _mc = _parse_market_cap_str(_raw_mc)
        _price_f = enriched.get("price")
        _vol_f   = enriched.get("volume")
        try:
            _price_f = float(_price_f) if _price_f is not None else None
        except Exception:
            _price_f = None
        try:
            _vol_f = float(_vol_f) if _vol_f is not None else None
        except Exception:
            _vol_f = None
        enriched.update(_vol_mc_fields(_price_f, _vol_f, _mc))

        # ── More-specific unavail reasons ──────────────────────────────────
        # Foreign-exchange-prefixed tickers (AIM:, STO:, FRA:, etc.) cannot
        # be quoted by Tradier; "volume_unavailable" is technically correct but
        # misleading.  Replace with a clearer reason so the frontend can show
        # a meaningful tooltip.
        if ":" in sym and enriched.get("vol_mc_unavailable_reason") == "volume_unavailable":
            enriched["vol_mc_unavailable_reason"] = "foreign_exchange_ticker_unsupported"

        # For US tickers that have market_cap + price but still no volume after
        # quote enrichment, add a clearer "quote_unavailable" reason so it's
        # distinguishable from foreign-ticker gaps.
        if (
            ":" not in sym
            and enriched.get("vol_mc_unavailable_reason") == "volume_unavailable"
            and enriched.get("price") is not None
        ):
            enriched["vol_mc_unavailable_reason"] = "quote_unavailable"

        # ── Weinstein Stage Analysis (cache-only) ───────────────────────────
        _stage = _get_stage2_breakout(sym)
        enriched["stage2_breakout"] = _stage   # backward-compat field
        enriched["stage_analysis"]  = _stage   # canonical alias (same object)

        return enriched

    # ── FALLBACK: no sections yet (analysis pending / never completed) ─────────
    # Build one synthetic section from the raw tickers list so the frontend
    # table always renders saved symbols, even for large watchlists that are
    # still being analysed in the background.
    if not sections:
        # ── Pre-load theme mapper once for the entire skeleton pass ───────────
        _skl_theme_fn = None
        _skl_theme_id_fn = None
        _skl_ind_fn = None
        try:
            from services.theme_ticker_mapper import (
                map_ticker_to_primary_theme as _skl_theme_fn,
                map_ticker_to_theme_id      as _skl_theme_id_fn,
                map_industry_to_theme       as _skl_ind_fn,
            )
        except ImportError:
            pass

        skeleton: list[dict] = []
        for sym in tickers:
            _s = sym.strip().upper()

            # ── Canonical theme lookup (symbol → theme) ───────────────────────
            _canon_theme: str | None = None
            _canon_theme_id: str | None = None
            _theme_src: str | None = None

            if _skl_theme_fn:
                _canon_theme    = _skl_theme_fn(_s)
                _canon_theme_id = _skl_theme_id_fn(_s) if _skl_theme_id_fn else None
                if _canon_theme:
                    _theme_src = "canonical_map"

            # ── Industry fallback for unmapped symbols ────────────────────────
            if not _canon_theme and _skl_ind_fn:
                _csv_r = csv_map.get(_s) or {}
                _ind   = (_csv_r.get("Industry") or _csv_r.get("industry") or "").strip()
                if _ind:
                    _ind_result = _skl_ind_fn(_ind)
                    if _ind_result:
                        _canon_theme, _canon_theme_id = _ind_result
                        _theme_src = "industry_fallback"

            if not _canon_theme and _skl_theme_fn:
                _theme_src = "no_mapping"

            row = _build_ticker_row(_s, {
                "symbol":               _s,
                "catalyst":             None,
                "sentiment":            None,
                "action_note":          None,
                "conviction":           None,
                "theme":                _canon_theme,
                "canonical_theme_name": _canon_theme,
                "canonical_theme_id":   _canon_theme_id,
                "theme_source":         _theme_src,
            })
            skeleton.append(row)

        elapsed_ms = round((_time.monotonic() - _t0) * 1000)
        quoted_count = sum(1 for s in skeleton if s.get("price") is not None)
        print(
            f"[WATCHLIST_ENRICH] analysis_pending — built {len(skeleton)} skeleton rows "
            f"({quoted_count} quoted) from {len(tickers)} saved tickers in {elapsed_ms}ms"
        )
        return {
            **store,
            "analysis": {
                **analysis,
                "sections": [{
                    "name":              "All Tickers",
                    "id":                "all_tickers",
                    "subtitle":          "Showing saved tickers — AI analysis running in background",
                    "tickers":           skeleton,
                    "_analysis_pending": True,
                }],
                "_analysis_pending": True,
                "_skeleton_reason":  "analysis_not_yet_run",
            },
        }

    # ── NORMAL PATH: enrich existing LLM section rows ─────────────────────────
    # Normalize saved ticker list (dedup, strip whitespace, uppercase)
    saved_normalized: list[str] = []
    seen_saved: set[str] = set()
    for t in tickers:
        s = t.strip().upper()
        if s and s not in seen_saved:
            saved_normalized.append(s)
            seen_saved.add(s)

    enriched_sections: list[dict] = []
    symbols_in_sections: set[str] = set()
    total_in  = 0
    total_out = 0
    for section in sections:
        enriched_tickers: list[dict] = []
        for row in section.get("tickers", []):
            sym = str(row.get("symbol", "")).upper()
            if not sym:
                continue
            total_in += 1
            enriched_tickers.append(_build_ticker_row(sym, row))
            symbols_in_sections.add(sym)
            total_out += 1
        enriched_sections.append({**section, "tickers": enriched_tickers})

    # ── SECTION NORMALIZATION (GET-time) ──────────────────────────────────────
    # Merge overlapping section titles into their canonical equivalents so the
    # frontend never sees duplicate/redundant sections.
    # "Solar" → "Clean Energy", "Renewable Energy" → "Clean Energy", etc.
    _SECTION_ALIAS_MAP: dict[str, str] = {
        "Solar":                    "Clean Energy",
        "Renewable Energy":         "Clean Energy",
        "Alternative Energy":       "Clean Energy",
        "Fuel Cell":                "Clean Energy",
        "Hydrogen Energy":          "Clean Energy",
        "Energy Storage":           "Lithium & Battery Tech",
        "Optical Networking":       "AI Networking",
        "Networking":               "AI Networking",
        "AI Chips":                 "Semiconductors",
        "Chips":                    "Semiconductors",
        "Memory / Storage":         "Memory & Storage",
        "Robotics / Automation":    "Robotics & Automation",
        "Datacenter / Compute":     "Data Center Infrastructure",
        "Aerospace / Defense":      "Defense",
    }

    _aliases_applied = 0
    _merged_map: dict[str, dict] = {}   # canonical_title → section dict (with mutable tickers list)
    for _sec in enriched_sections:
        _raw = (_sec.get("title") or _sec.get("name") or _sec.get("id") or "").strip()
        _canon = _SECTION_ALIAS_MAP.get(_raw, _raw)
        if _canon in _merged_map:
            _merged_map[_canon]["tickers"].extend(_sec.get("tickers", []))
            _aliases_applied += 1
        else:
            _merged_map[_canon] = {**_sec, "title": _canon, "tickers": list(_sec.get("tickers", []))}
            if _canon != _raw:
                _aliases_applied += 1
    enriched_sections = list(_merged_map.values())

    # ── UNCATEGORIZED RECLASSIFICATION (GET-time) ─────────────────────────────
    # For tickers that ended up in "Other / Uncategorized" (either from a
    # previous BG_REFRESH or from the missing-append pass), use the CSV Industry
    # column as a deterministic fallback to move them into proper canonical
    # sections.  No AI calls — pure lookup, runs on every GET.
    _reclassified_count = 0
    try:
        from services.theme_ticker_mapper import map_industry_to_theme as _map_ind
    except ImportError:
        _map_ind = None

    if _map_ind:
        _unc_idx = next(
            (i for i, _s in enumerate(enriched_sections)
             if _s.get("id") == "other_uncategorized" or
             (_s.get("title") or "").lower() in ("other / uncategorized", "other/uncategorized")),
            None,
        )
        if _unc_idx is not None:
            _sec_title_idx: dict[str, int] = {
                (_s.get("title") or ""): i for i, _s in enumerate(enriched_sections)
            }
            _still_unc: list[dict] = []
            for _row in enriched_sections[_unc_idx].get("tickers", []):
                _sym = str(_row.get("symbol", "")).upper()
                _csv_r = csv_map.get(_sym) or csv_map.get(_sym.split(":")[-1] if ":" in _sym else _sym) or {}
                _ind = (_csv_r.get("Industry") or _csv_r.get("industry") or "").strip()
                _mapping = _map_ind(_ind)
                if _mapping:
                    _tgt_name, _tgt_id = _mapping
                    _tgt_name = _SECTION_ALIAS_MAP.get(_tgt_name, _tgt_name)
                    _enriched_row = {
                        **_row,
                        "canonical_theme_name": _tgt_name,
                        "canonical_theme_id":   _tgt_id,
                        "theme_source":         "industry_fallback",
                    }
                    if _tgt_name in _sec_title_idx:
                        _i = _sec_title_idx[_tgt_name]
                        enriched_sections[_i]["tickers"].append(_enriched_row)
                    else:
                        _new_sec = {
                            "id":           _tgt_id,
                            "title":        _tgt_name,
                            "subtitle":     "Classified via industry data",
                            "tickers":      [_enriched_row],
                            "theme_source": "industry_fallback",
                        }
                        _sec_title_idx[_tgt_name] = len(enriched_sections)
                        enriched_sections.append(_new_sec)
                    _reclassified_count += 1
                else:
                    _still_unc.append(_row)
            # Keep only the truly-unclassifiable tickers in uncategorized
            enriched_sections[_unc_idx] = {
                **enriched_sections[_unc_idx],
                "tickers": _still_unc,
            }
            # Drop empty sections (e.g. uncategorized that became fully reclassified)
            enriched_sections = [_s for _s in enriched_sections if _s.get("tickers")]
            if _reclassified_count:
                print(
                    f"[WATCHLIST_ENRICH] reclassified {_reclassified_count} uncategorized tickers "
                    f"→ canonical sections via industry fallback"
                )

    # ── MISSING-SYMBOL APPEND (industry-aware) ────────────────────────────────
    # Any saved symbol absent from the enriched sections gets a skeleton row.
    # Before defaulting to "Other / Uncategorized", try the CSV industry fallback
    # so skeleton rows land in the correct canonical section rather than a catch-all.
    # This ensures tickers missed by Claude's last BG_REFRESH are still displayed
    # in a meaningful thematic context.
    missing_syms = [s for s in saved_normalized if s not in symbols_in_sections]
    appended_count = 0

    if missing_syms:
        # Build a section-title → index map for routing skeleton rows
        _miss_sec_idx: dict[str, int] = {
            (s.get("title") or ""): i for i, s in enumerate(enriched_sections)
        }

        # Resolve industry-fallback mapper (may already be imported above)
        _miss_map_ind = _map_ind if "_map_ind" in dir() else None
        if _miss_map_ind is None:
            try:
                from services.theme_ticker_mapper import map_industry_to_theme as _miss_map_ind
            except ImportError:
                _miss_map_ind = None

        for sym in missing_syms:
            _csv_r = csv_map.get(sym) or csv_map.get(sym.split(":")[-1] if ":" in sym else sym) or {}
            _ind   = (_csv_r.get("Industry") or _csv_r.get("industry") or "").strip()
            _mapped = _miss_map_ind(_ind) if _miss_map_ind and _ind else None

            if _mapped:
                _tgt_name, _tgt_id = _mapped
                _tgt_name = _SECTION_ALIAS_MAP.get(_tgt_name, _tgt_name)
                _base_row = {
                    "symbol":              sym,
                    "catalyst":            None,
                    "sentiment":           None,
                    "action_note":         None,
                    "conviction":          None,
                    "theme":               None,
                    "canonical_theme_name": _tgt_name,
                    "canonical_theme_id":   _tgt_id,
                    "theme_source":         "missing_append_industry",
                }
            else:
                _tgt_name = "Other / Uncategorized"
                _tgt_id   = "other_uncategorized"
                _base_row = {
                    "symbol":              sym,
                    "catalyst":            None,
                    "sentiment":           None,
                    "action_note":         None,
                    "conviction":          None,
                    "theme":               None,
                    "canonical_theme_name": _tgt_name,
                    "canonical_theme_id":   _tgt_id,
                    "theme_source":         "missing_append",
                }

            _built_row = _build_ticker_row(sym, _base_row)
            appended_count += 1

            if _tgt_name in _miss_sec_idx:
                _i = _miss_sec_idx[_tgt_name]
                enriched_sections[_i]["tickers"].append(_built_row)
            else:
                _new_s = {
                    "id":       _tgt_id,
                    "title":    _tgt_name,
                    "subtitle": ("Saved tickers not categorized by AI analysis"
                                 if _tgt_id == "other_uncategorized"
                                 else "Classified via industry data"),
                    "tickers":  [_built_row],
                }
                _miss_sec_idx[_tgt_name] = len(enriched_sections)
                enriched_sections.append(_new_s)

        print(
            f"[WATCHLIST_ENRICH] appended {appended_count} missing symbols "
            f"(saved={len(saved_normalized)} in_sections={len(symbols_in_sections)}): "
            + ", ".join(missing_syms[:10]) + ("…" if len(missing_syms) > 10 else "")
        )

    # ── DEDUP + SAVED-FILTER PASS ─────────────────────────────────────────────
    # Two passes in one walk:
    #   a) Drop symbols Claude emitted that are not in the saved list.
    #      Example: saved has "OTC:CGEH", Claude strips prefix and emits "CGEH" —
    #      that bare row is an impostor; the correct OTC:CGEH row was already
    #      appended by the missing-append pass above.
    #   b) Drop duplicate symbols (Claude emits same ticker across chunks).
    #      First occurrence wins (always the richer Claude row since Claude
    #      sections are processed before missing-append rows).
    # Sections that become empty after filtering are dropped.
    seen_final: set[str] = set()
    dedup_sections: list[dict] = []
    dup_count       = 0
    not_saved_count = 0
    for section in enriched_sections:
        deduped: list[dict] = []
        for row in section.get("tickers", []):
            sym = str(row.get("symbol", "")).upper()
            if not sym:
                continue
            if sym not in seen_saved:
                # Claude invented or format-mismatched symbol — skip
                not_saved_count += 1
                continue
            if sym in seen_final:
                dup_count += 1
                continue
            seen_final.add(sym)
            deduped.append(row)
        if deduped:
            dedup_sections.append({**section, "tickers": deduped})

    if dup_count or not_saved_count:
        print(
            f"[WATCHLIST_ENRICH] dedup removed {dup_count} duplicate rows, "
            f"{not_saved_count} not-in-saved rows"
        )

    # ── REL-VOL RANK + MOMENTUM PASS ─────────────────────────────────────────
    # Additive pass: adds rel_vol_rank / trend / momentum_label to every row.
    # Must run AFTER the dedup pass (final row set is fixed here).
    # Does not reorder sections — only annotates existing rows.
    _wl_id_for_rank = store.get("id") or ""
    try:
        dedup_sections = await _apply_rv_rank_fields(
            _wl_id_for_rank, dedup_sections, saved_normalized
        )
    except Exception as _rv_err:
        print(f"[WATCHLIST_ENRICH] rv_rank pass failed (non-fatal): {_rv_err}")

    # ── VOL/MC RANK + MOMENTUM PASS ───────────────────────────────────────────
    # Additive pass: adds vol_mc_rank / vol_mc_prev_pct / vol_mc_pct_delta /
    # vol_mc_trend / vol_mc_momentum_label to every row.
    # Runs after rv_rank (both passes operate on the same final dedup_sections).
    try:
        dedup_sections = await _apply_volmc_rank_fields(
            _wl_id_for_rank, dedup_sections, saved_normalized
        )
    except Exception as _vm_err:
        print(f"[WATCHLIST_ENRICH] volmc_rank pass failed (non-fatal): {_vm_err}")

    total_rows = sum(len(s["tickers"]) for s in dedup_sections)
    elapsed_ms = round((_time.monotonic() - _t0) * 1000)
    print(
        f"[WATCHLIST_ENRICH] sections={len(dedup_sections)} "
        f"tickers_in={total_in} tickers_out={total_rows} "
        f"appended_missing={appended_count} dups_removed={dup_count} "
        f"quoted={sum(1 for s in dedup_sections for t in s['tickers'] if t.get('price') is not None)} "
        f"elapsed={elapsed_ms}ms"
    )

    # ── CATEGORY OVERRIDES (always last — manual assignments always win) ───────
    # Applies user-approved ticker→section corrections from the DB override table.
    # This ensures manual moves persist across AI re-analyses and server restarts.
    try:
        from services.category_overrides import apply_to_sections as _apply_cat_overrides
        dedup_sections = _apply_cat_overrides(dedup_sections, user_id="default")
    except Exception as _ov_err:
        print(f"[WATCHLIST_ENRICH] category overrides failed (non-fatal): {_ov_err}")

    return {
        **store,
        "analysis": {
            **analysis,
            "sections": dedup_sections,
            "_missing_symbols_appended_count": appended_count,
            "_duplicate_symbols_removed":      dup_count,
            "_not_in_saved_removed":           not_saved_count,
        },
    }


# ── Request / Response Models ────────────────────────────────────────────────

class WatchlistSaveRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    csv_data: List[Dict[str, Any]]
    analysis: Dict[str, Any]
    watchlist_id: Optional[str] = None
    name: Optional[str] = None


class WatchlistAnalyzeRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    tickers: Optional[List[str]] = None
    csv_data: Optional[List[Dict[str, Any]]] = None
    watchlist_id: Optional[str] = None


# ── Helper ──────────────────────────────────────────────────────────────────

def _get_agent():
    import main
    if main.agent is None:
        raise HTTPException(status_code=503, detail="Server is still starting up.")
    return main.agent


def _get_data_service():
    import main
    if main.data_service is None:
        raise HTTPException(status_code=503, detail="Server is still starting up.")
    return main.data_service


# ── Endpoints — STATIC paths first, then parameterized ──────────────────────

@router.get("/list")
async def list_endpoint():
    """List all saved watchlists (metadata only)."""
    return list_watchlists()


@router.post("/save")
async def save_endpoint(body: WatchlistSaveRequest):
    """Save CSV data + AI analysis to the watchlist store."""
    import asyncio as _aio
    global _UPLOAD_WARMUP_STATE
    result = save_watchlist(body.csv_data, body.analysis, body.watchlist_id, body.name)

    # ── Background Stage2 warmup for symbols missing from LKG ─────────────────
    # Fires a non-blocking task so the upload response returns immediately.
    # Eligible = US/non-foreign symbols (no colon). Skips symbols already in LKG
    # with a valid label (they'll be refreshed by the regular 20h cadence).
    try:
        from services.watchlist_stage2_service import warmup_stage2 as _ws2, _STAGE2_LKG as _lkg
        from services.watchlist_quote_cache import is_fmp_symbol_eligible as _elig
        import datetime as _dt

        _wl_id = result.get("watchlist_id") or body.watchlist_id
        _store = load_watchlist(_wl_id) if _wl_id else None
        _all_tickers: list[str] = [
            t.strip().upper() for t in (_store.get("tickers") or []) if t.strip()
        ] if _store else []

        _eligible  = [s for s in _all_tickers if _elig(s)]
        # Queue a symbol if it is absent from LKG, has a null label/score,
        # or has a valid stage entry but is missing Phase-2 technical_metrics
        # or technical_state (old-format entries from pre-Phase-2 warmups).
        _missing = [
            s for s in _eligible
            if (
                not _lkg.get(s)
                or _lkg[s].get("label")            is None
                or _lkg[s].get("score")            is None
                or _lkg[s].get("technical_metrics") is None
                or _lkg[s].get("technical_state")   is None
            )
        ]
        _in_lkg    = len(_eligible) - len(_missing)  # fully-covered (label + metrics)

        _UPLOAD_WARMUP_STATE.update({
            "watchlist_id":   _wl_id,
            "triggered_at":   _dt.datetime.utcnow().isoformat(),
            "finished_at":    None,
            "total_eligible": len(_eligible),
            "already_in_lkg": _in_lkg,
            "queued":         len(_missing),
            "running":        len(_missing) > 0,
        })

        if _missing:
            async def _run_upload_warmup() -> None:
                try:
                    await _ws2(_missing, force_nulls=True)
                finally:
                    _UPLOAD_WARMUP_STATE["running"]     = False
                    _UPLOAD_WARMUP_STATE["finished_at"] = _dt.datetime.utcnow().isoformat()

            _aio.create_task(_run_upload_warmup())
            print(
                f"[WATCHLIST_SAVE] stage2 warmup queued: "
                f"{len(_missing)} symbols (of {len(_eligible)} eligible, {_in_lkg} already in LKG)"
            )
    except Exception as _wup_e:
        print(f"[WATCHLIST_SAVE] stage2 warmup trigger skipped (non-fatal): {_wup_e}")

    # ── Background theme classifier for unmapped symbols ──────────────────────
    # Fires after stage2 warmup. Skips symbols already covered by canonical map
    # or industry fallback. Job-level lock prevents duplicate runs.
    try:
        from services.watchlist_theme_classifier import classify_watchlist_themes as _classify
        _wl_id_cls = result.get("watchlist_id") or body.watchlist_id
        if _wl_id_cls:
            _aio.create_task(_classify(_wl_id_cls, missing_only=True))
            print(f"[WATCHLIST_SAVE] theme classifier queued for watchlist {_wl_id_cls}")
    except Exception as _cls_e:
        print(f"[WATCHLIST_SAVE] theme classifier trigger skipped (non-fatal): {_cls_e}")

    return result


@router.post("/debug/fundamentals/refresh")
async def debug_fundamentals_refresh(
    symbols: Optional[str] = None,
    dev_force: bool = True,
    watchlist_id: Optional[str] = None,
):
    """
    DEV-ONLY: Force an immediate FMP fundamentals refresh for testing.
    Does not affect production weekly scheduler behavior.
    Never called by normal frontend page loads.

    Usage:
      POST /api/watchlist/debug/fundamentals/refresh?symbols=AAOI,NVDA,MU
      POST /api/watchlist/debug/fundamentals/refresh?dev_force=true           (all tickers in default watchlist)
      POST /api/watchlist/debug/fundamentals/refresh?watchlist_id=<id>&dev_force=true
    """
    import os as _os
    from services.watchlist_fundamentals_refresh import FmpFundamentalsRefresher

    _fmp_key = _os.getenv("FMP_API_KEY", "")
    if not _fmp_key:
        raise HTTPException(status_code=503, detail="FMP_API_KEY not configured")

    refresher = FmpFundamentalsRefresher(_fmp_key)

    # Resolve symbol list
    if symbols:
        sym_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
        wl_id = watchlist_id or "manual"
    else:
        wl_id = watchlist_id or "23eec278-074a-4706-a62a-c35d38b384ea"
        store = load_watchlist(wl_id)
        if not store:
            raise HTTPException(status_code=404, detail=f"Watchlist {wl_id} not found")
        sym_list = store.get("tickers") or []

    if not sym_list:
        raise HTTPException(status_code=400, detail="No symbols to refresh")

    result = await refresher.refresh_symbols(sym_list, wl_id, dev_force=dev_force)
    return result


# ── In-process backfill state ─────────────────────────────────────────────────
_backfill_state: dict = {"status": "idle", "refreshed": 0, "failed": 0, "total": 0,
                         "failed_symbols": [], "started_at": None, "finished_at": None}

# ── Upload-triggered Stage2 warmup state ──────────────────────────────────────
_UPLOAD_WARMUP_STATE: dict = {
    "watchlist_id":   None,
    "triggered_at":   None,
    "finished_at":    None,
    "total_eligible": 0,
    "already_in_lkg": 0,
    "queued":         0,
    "running":        False,
}

@router.post("/debug/fundamentals/backfill")
async def debug_fundamentals_backfill(
    watchlist_id: Optional[str] = None,
    dev_force: bool = True,
):
    """
    DEV-ONLY: Fire-and-forget full watchlist backfill.
    Spawns as asyncio background task inside the running server.
    Returns immediately; poll GET /debug/fundamentals/backfill/status for progress.
    """
    import asyncio as _aio, os as _os
    global _backfill_state

    if _backfill_state.get("status") == "running":
        return {"status": "already_running", "state": _backfill_state}

    wl_id = watchlist_id or "23eec278-074a-4706-a62a-c35d38b384ea"
    fmp_key = _os.getenv("FMP_API_KEY", "")
    if not fmp_key:
        raise HTTPException(status_code=503, detail="FMP_API_KEY not configured")

    store = load_watchlist(wl_id)
    if not store:
        raise HTTPException(status_code=404, detail=f"Watchlist {wl_id} not found")

    from services.watchlist_quote_cache import is_fmp_symbol_eligible as _elig
    from data.watchlist_fundamentals_store import get_snapshots_bulk as _snaps_bulk
    all_tickers = store.get("tickers") or []
    eligible = [s.strip().upper() for s in all_tickers if s and _elig(s.strip())]

    if dev_force:
        to_refresh = eligible
    else:
        snaps = _snaps_bulk(eligible)
        to_refresh = [s for s in eligible if s not in snaps or not (snaps[s].get("fields") or {})]

    _backfill_state.update({
        "status": "running", "refreshed": 0, "failed": 0,
        "total": len(to_refresh), "failed_symbols": [],
        "started_at": __import__("datetime").datetime.utcnow().isoformat(),
        "finished_at": None,
    })

    async def _run():
        global _backfill_state
        from services.watchlist_fundamentals_refresh import FmpFundamentalsRefresher
        from data.watchlist_fundamentals_store import upsert_snapshot as _upsert
        refresher = FmpFundamentalsRefresher(fmp_key)
        for sym in to_refresh:
            try:
                result = await refresher.normalize_symbol(sym)
                ok = _upsert(symbol=sym, watchlist_id=wl_id,
                             fields=result["fields"],
                             missing_fields=result["missing_fields"],
                             fmp_call_count=result["fmp_call_count"])
                if ok:
                    _backfill_state["refreshed"] += 1
                else:
                    _backfill_state["failed"] += 1
                    _backfill_state["failed_symbols"].append(sym)
            except Exception as _e:
                _backfill_state["failed"] += 1
                _backfill_state["failed_symbols"].append(sym)
                print(f"[BACKFILL] {sym} error: {_e}")
        _backfill_state["status"] = "done"
        _backfill_state["finished_at"] = __import__("datetime").datetime.utcnow().isoformat()
        print(f"[BACKFILL] complete: refreshed={_backfill_state['refreshed']} "
              f"failed={_backfill_state['failed']} total={_backfill_state['total']}")

    _aio.create_task(_run())
    return {"status": "started", "total_to_refresh": len(to_refresh), "state": _backfill_state}


@router.get("/debug/technical/provenance")
async def debug_technical_provenance(symbol: str = ""):
    """
    DEV-ONLY: Full technical diagnostics for a single symbol from LKG cache.
    No provider calls — reads only the in-memory LKG.

    Returns stage label/score/reason, provenance, all technical_metrics,
    technical_state, technical_timing_score, and missing_metric_reasons.
    """
    from services.watchlist_stage2_service import get_stage2, _STAGE2_LKG

    sym = (symbol or "").strip().upper()
    if not sym:
        return {"error": "symbol query param required"}

    raw = _STAGE2_LKG.get(sym)
    if raw is None:
        return {"error": f"{sym} not found in Stage2 LKG", "symbol": sym}

    tech = raw.get("technical_metrics") or {}
    return {
        "symbol":         sym,
        "stage_label":    raw.get("label"),
        "stage_score":    raw.get("score"),
        "stage_reason":   raw.get("reason"),
        "stage_confidence":        raw.get("stage_confidence"),
        "stage_confidence_reason": raw.get("stage_confidence_reason"),
        "history_source":     raw.get("history_source"),
        "bars_count":         raw.get("bars_count"),
        "history_start_date": raw.get("history_start_date"),
        "history_end_date":   raw.get("history_end_date"),
        "has_ohlcv":          raw.get("has_ohlcv"),
        "has_200d":           raw.get("has_200d"),
        "has_252d":           raw.get("has_252d"),
        "computed_at":        raw.get("computed_at"),
        "technical_state":         raw.get("technical_state"),
        "technical_timing_score":  raw.get("technical_timing_score"),
        "technical_metrics":       tech,
        "missing_metric_reasons":  tech.get("missing_metric_reasons", []),
        "signals":            raw.get("signals"),
        "status":             raw.get("status"),
    }


@router.get("/debug/technical/status")
async def debug_technical_status(watchlist_id: Optional[str] = None):
    """
    DEV-ONLY: Aggregate technical coverage stats across all LKG entries.
    No provider calls — reads only the in-memory LKG.

    Returns counts: total, valid stage labels, missing, has_ohlcv, FMP-history,
    Tradier-history, low-confidence stage, average bars_count, tickers missing
    200D, tickers missing 252D.
    """
    from services.watchlist_stage2_service import _STAGE2_LKG

    wl_id  = watchlist_id or "23eec278-074a-4706-a62a-c35d38b384ea"
    store  = load_watchlist(wl_id)
    tickers: list[str] = []
    if store:
        tickers = [t.strip().upper() for t in (store.get("tickers") or []) if t.strip()]

    # Collect LKG entries for this watchlist's tickers (fall back to all LKG if wl empty)
    scope: dict[str, dict] = {}
    if tickers:
        for sym in tickers:
            e = _STAGE2_LKG.get(sym)
            if e is not None:
                scope[sym] = e
    else:
        scope = dict(_STAGE2_LKG)

    total          = len(scope)
    valid_label    = 0
    missing_label  = 0
    has_ohlcv_cnt  = 0
    fmp_cnt        = 0
    tradier_cnt    = 0
    low_conf_cnt   = 0
    no_200d_syms: list[str] = []
    no_252d_syms: list[str] = []
    bars_counts: list[int]  = []

    for sym, e in scope.items():
        lbl = e.get("label")
        sc  = e.get("score")
        if lbl is not None and sc is not None:
            valid_label += 1
        else:
            missing_label += 1
        if e.get("has_ohlcv"):
            has_ohlcv_cnt += 1
        src = e.get("history_source") or "unknown"
        if src == "fmp":
            fmp_cnt += 1
        elif src == "tradier":
            tradier_cnt += 1
        if (e.get("stage_confidence") or "low") == "low":
            low_conf_cnt += 1
        bc = e.get("bars_count")
        if bc is not None:
            bars_counts.append(int(bc))
            if bc < 200:
                no_200d_syms.append(sym)
            if bc < 252:
                no_252d_syms.append(sym)

    avg_bars = round(sum(bars_counts) / len(bars_counts), 1) if bars_counts else None

    # Also tally technical_state distribution
    state_dist: dict[str, int] = {}
    tech_metrics_cnt = 0
    for e in scope.values():
        st = e.get("technical_state") or "unknown"
        state_dist[st] = state_dist.get(st, 0) + 1
        if e.get("technical_metrics") is not None:
            tech_metrics_cnt += 1

    # Tally all watchlist tickers not in LKG scope (ineligible or never computed)
    skipped_ineligible = 0
    not_in_lkg_cnt = 0
    if tickers:
        from services.watchlist_quote_cache import is_fmp_symbol_eligible as _st_elig
        for _s in tickers:
            if _STAGE2_LKG.get(_s) is None:
                if not _st_elig(_s):
                    skipped_ineligible += 1
                else:
                    not_in_lkg_cnt += 1

    return {
        "watchlist_id":                  wl_id,
        "scope":                         "watchlist" if tickers else "all_lkg",
        "total_watchlist_tickers":       len(tickers),
        "total_in_lkg":                  total,
        "valid_stage_labels":            valid_label,
        "missing_stage_labels":          missing_label,
        "existing_technical_metrics":    tech_metrics_cnt,
        "missing_technical_metrics":     total - tech_metrics_cnt,
        "not_in_lkg_eligible":           not_in_lkg_cnt,
        "skipped_ineligible":            skipped_ineligible,
        "has_ohlcv_count":               has_ohlcv_cnt,
        "fmp_history_count":             fmp_cnt,
        "tradier_history_count":         tradier_cnt,
        "low_confidence_count":          low_conf_cnt,
        "average_bars_count":            avg_bars,
        "tickers_missing_200d":          no_200d_syms,
        "tickers_missing_252d":          no_252d_syms,
        "technical_state_distribution":  state_dist,
        "upload_warmup":                 dict(_UPLOAD_WARMUP_STATE),
    }


@router.post("/debug/technical/backfill/start")
async def debug_technical_backfill_start(
    watchlist_id: Optional[str] = None,
    missing_only: bool = True,
    cap: int = 50,
):
    """
    DEV-ONLY: Fire-and-forget technical metrics backfill.

    Processes LKG entries that have stage labels but no technical_metrics.
    Merges technical fields only — does NOT overwrite score/label/reason/signals.
    Uses the same _fetch_bars() + compute_technical_metrics() path as warmup_stage2.
    Capped at `cap` symbols per call (resumable — re-POST picks up remaining).

    Returns immediately; poll GET /debug/technical/backfill/status for progress.
    """
    from services.watchlist_stage2_service import (
        backfill_technical_metrics,
        _TECH_BACKFILL_STATE,
        _STAGE2_LKG,
    )

    if _TECH_BACKFILL_STATE.get("status") == "running":
        return {"status": "already_running", "state": dict(_TECH_BACKFILL_STATE)}

    wl_id = watchlist_id or "23eec278-074a-4706-a62a-c35d38b384ea"
    tickers: list[str] = []
    try:
        store = load_watchlist(wl_id)
        if store:
            tickers = [t.strip().upper() for t in (store.get("tickers") or []) if t.strip()]
    except Exception:
        pass

    if not tickers:
        tickers = list(_STAGE2_LKG.keys())

    missing_count = sum(
        1 for sym in tickers
        if _STAGE2_LKG.get(sym) is not None
        and _STAGE2_LKG[sym].get("technical_metrics") is None
    )

    import asyncio as _asyncio
    _asyncio.create_task(
        backfill_technical_metrics(tickers, cap=cap, missing_only=missing_only)
    )

    return {
        "status":                    "started",
        "watchlist_id":              wl_id,
        "watchlist_tickers":         len(tickers),
        "missing_technical_metrics": missing_count,
        "cap":                       cap,
        "missing_only":              missing_only,
    }


@router.get("/debug/technical/backfill/status")
async def debug_technical_backfill_status():
    """DEV-ONLY: Poll progress of an in-progress technical metrics backfill."""
    from services.watchlist_stage2_service import _TECH_BACKFILL_STATE
    return dict(_TECH_BACKFILL_STATE)


@router.get("/debug/fundamentals/backfill/status")
async def debug_fundamentals_backfill_status():
    """DEV-ONLY: Poll progress of an in-progress backfill."""
    return _backfill_state


@router.get("/debug/fundamentals/status")
async def debug_fundamentals_status(watchlist_id: Optional[str] = None):
    """
    DEV-ONLY: Full per-symbol coverage audit for the watchlist FMP fundamentals cache.

    Returns:
      summary           — aggregate counts (total, eligible, has_snapshot, by status, earnings coverage)
      field_coverage    — per-column: fmp / csv / missing counts + csv_only_by_design flag
      per_symbol        — one entry per watchlist symbol with status, refreshed_at,
                          field counts, and earnings date provenance
    """
    from datetime import datetime, timezone
    from zoneinfo import ZoneInfo
    from data.watchlist_fundamentals_store import get_snapshots_bulk as _get_snaps
    from services.watchlist_quote_cache import is_fmp_symbol_eligible as _eligible

    _ET = ZoneInfo("America/New_York")
    today_et = datetime.now(tz=_ET).strftime("%Y-%m-%d")
    now_utc  = datetime.now(timezone.utc)

    wl_id = watchlist_id or "23eec278-074a-4706-a62a-c35d38b384ea"
    store = load_watchlist(wl_id)
    if not store:
        return {"error": f"Watchlist {wl_id} not found"}

    csv_data: list[dict] = store.get("csv_data") or []
    tickers:  list[str]  = store.get("tickers")  or []

    # Index CSV rows by symbol
    csv_by_sym: dict[str, dict] = {}
    for row in csv_data:
        sym = (row.get("Symbol") or row.get("symbol") or row.get("Ticker") or "").strip().upper()
        if sym:
            csv_by_sym[sym] = row

    all_syms       = [s.strip().upper() for s in tickers if s and s.strip()]
    eligible_syms  = [s for s in all_syms if _eligible(s)]
    ineligible_syms = [s for s in all_syms if not _eligible(s)]

    # Single bulk DB read for all eligible symbols
    snaps = _get_snaps(eligible_syms) if eligible_syms else {}

    # ── Tracked fields ──────────────────────────────────────────────────────
    FMP_FIELDS = [
        "Market Cap", "Revenue", "Revenue Growth (Q)", "Revenue Growth (YoY)",
        "Gross Margin", "FCF Margin", "Free Cash Flow", "Operating Income", "EBIT",
        "PE Ratio", "PS Ratio", "EV/EBITDA", "EPS Growth", "Debt / Equity",
        "Net Debt / EBITDA", "Earnings Date",
        "Rev Growth Next Quarter", "EPS Growth This Quarter",
    ]
    CSV_ONLY_FIELDS = [
        "Shares Insiders", "Revenue Growth Est.", "Rev Growth Next Year",
        "Rev Growth This Year", "EPS Growth Est.", "EPS Growth Next Quarter",
        "EPS Growth This Year", "EPS Growth Next Year",
    ]
    ALL_TRACKED = FMP_FIELDS + CSV_ONLY_FIELDS
    CSV_ONLY_SET = set(CSV_ONLY_FIELDS)

    field_coverage: dict = {
        f: {"fmp": 0, "csv": 0, "missing": 0, "csv_only_by_design": f in CSV_ONLY_SET}
        for f in ALL_TRACKED
    }

    # ── Aggregate counters ───────────────────────────────────────────────────
    summary: dict = {
        "total":                   len(all_syms),
        "fmp_eligible":            len(eligible_syms),
        "fmp_ineligible":          len(ineligible_syms),
        "has_snapshot":            0,
        "fmp_refreshed":           0,        # ≥10 FMP fields populated
        "fmp_missing_partial":     0,        # 1–9 FMP fields
        "csv_fallback_only":       0,        # eligible but snapshot is empty/failed
        "not_yet_scanned":         0,        # eligible, no snapshot at all
        "fmp_ineligible_count":    len(ineligible_syms),
        "has_future_earnings_date": 0,
        "stale_or_no_earnings_date": 0,
    }

    per_symbol: list[dict] = []

    for sym in all_syms:
        csv_row = csv_by_sym.get(sym, {})
        snap    = snaps.get(sym) if _eligible(sym) else None
        elig    = _eligible(sym)

        # ── Status classification ─────────────────────────────────────────
        if not elig:
            status = "fmp_ineligible"
            fmp_fields_count = 0
            missing_count    = 0
            refreshed_at     = None
            next_refresh_at  = None
        elif snap is None:
            status = "not_yet_scanned"
            fmp_fields_count = 0
            missing_count    = 0
            refreshed_at     = None
            next_refresh_at  = None
        else:
            snap_fields:  dict = snap.get("fields")        or {}
            snap_missing: list = snap.get("missing_fields") or []
            refreshed_at        = snap.get("refreshed_at")
            next_refresh_at     = snap.get("next_refresh_at")
            fmp_fields_count    = len(snap_fields)
            missing_count       = len(snap_missing)

            if fmp_fields_count >= 10:
                status = "fmp_refreshed"
            elif fmp_fields_count > 0:
                status = "fmp_missing_partial"
            elif missing_count > 0:
                status = "csv_fallback_only"   # ran, got nothing; all marked missing
            else:
                status = "not_yet_scanned"     # row exists but fields empty

        # ── Earnings date provenance (with stale rule) ────────────────────
        snap_fields_d: dict = (snap.get("fields") or {}) if snap else {}
        earn_fmp = (snap_fields_d.get("Earnings Date") or "").strip() or None
        earn_csv = (csv_row.get("Earnings Date") or "").strip() or None

        if earn_fmp and earn_fmp >= today_et:
            earn_final  = earn_fmp
            earn_source = "fmp"
            has_future  = True
        elif earn_csv and earn_csv >= today_et:
            earn_final  = earn_csv
            earn_source = "csv_fallback"
            has_future  = True
        else:
            earn_final  = None
            earn_source = "missing" if elig else "fmp_ineligible"
            has_future  = False

        # ── Counters ──────────────────────────────────────────────────────
        if snap:
            summary["has_snapshot"] += 1
        if status == "fmp_refreshed":       summary["fmp_refreshed"]       += 1
        elif status == "fmp_missing_partial": summary["fmp_missing_partial"] += 1
        elif status == "csv_fallback_only": summary["csv_fallback_only"]    += 1
        elif status == "not_yet_scanned":   summary["not_yet_scanned"]      += 1

        if has_future:
            summary["has_future_earnings_date"]    += 1
        else:
            summary["stale_or_no_earnings_date"]   += 1

        # ── Field-level coverage ──────────────────────────────────────────
        snap_missing_set: set = set((snap.get("missing_fields") or [])) if snap else set()
        for field in ALL_TRACKED:
            if not elig or snap is None:
                field_coverage[field]["missing"] += 1
            elif field in snap_fields_d and snap_fields_d[field] is not None:
                field_coverage[field]["fmp"] += 1
            elif field in snap_missing_set:
                csv_val = str(csv_row.get(field) or "").strip()
                if csv_val:
                    field_coverage[field]["csv"] += 1
                else:
                    field_coverage[field]["missing"] += 1
            else:
                csv_val = str(csv_row.get(field) or "").strip()
                if csv_val:
                    field_coverage[field]["csv"] += 1
                else:
                    field_coverage[field]["missing"] += 1

        per_symbol.append({
            "symbol":               sym,
            "status":               status,
            "fmp_eligible":         elig,
            "refreshed_at":         refreshed_at,
            "next_refresh_at":      next_refresh_at,
            "fmp_fields_count":     fmp_fields_count,
            "missing_fields_count": missing_count,
            "has_future_earnings_date":    has_future,
            "final_earnings_date":         earn_final,
            "final_earnings_date_source":  earn_source,
            "csv_earnings_date":           earn_csv,
            "fmp_earnings_date":           earn_fmp,
        })

    return {
        "watchlist_id": wl_id,
        "audited_at":   now_utc.isoformat(),
        "today_et":     today_et,
        "summary":      summary,
        "field_coverage": field_coverage,
        "per_symbol":   per_symbol,
    }


@router.get("/debug/fundamentals/provenance")
async def debug_fundamentals_provenance(
    symbol: str,
    watchlist_id: Optional[str] = None,
):
    """
    DEV-ONLY: Field-by-field provenance for one symbol.
    Returns final_value, source (fmp/csv_fallback/canonical_theme/missing),
    fmp_value, csv_value, source_endpoint, formula, missing_reason, refreshed_at.

    Read-only — inspects Neon cache + current watchlist row only.
    No live provider calls.
    """
    from data.watchlist_fundamentals_store import get_snapshot as _get_snap
    from services.watchlist_fundamentals_refresh import apply_fmp_overlays, merge_fmp_into_csv_row

    sym = symbol.strip().upper()
    wl_id = watchlist_id or "23eec278-074a-4706-a62a-c35d38b384ea"

    # ── Load CSV row ──────────────────────────────────────────────────────────
    csv_row: dict = {}
    try:
        wl_store = load_watchlist(wl_id)
        if wl_store:
            for r in (wl_store.get("csv_data") or []):
                if (r.get("Symbol") or r.get("Ticker") or "").upper() == sym:
                    csv_row = dict(r)
                    break
    except Exception:
        pass

    # ── Load FMP snapshot ─────────────────────────────────────────────────────
    snap = _get_snap(sym)
    fmp_fields: dict = snap.get("fields", {}) if snap else {}
    missing_fmp: list = snap.get("missing_fields", []) if snap else []
    refreshed_at: str = snap.get("refreshed_at", "") if snap else ""

    # ── Build merged row (same logic as GET endpoint) ─────────────────────────
    merged_row: dict = {}
    if csv_row and snap:
        merged_row = merge_fmp_into_csv_row(dict(csv_row), fmp_fields)
    elif csv_row:
        merged_row = dict(csv_row)

    # ── Field metadata table ──────────────────────────────────────────────────
    # Maps screener column name → (source_endpoint, formula, plan_limited)
    _META: dict[str, tuple[str, str, bool]] = {
        "Symbol":                   ("—",                        "direct",                        False),
        "Theme":                    ("canonical_watchlist",       "screener hub tag, not FMP",     False),
        "Market Cap":               ("profile",                   "marketCap direct",              False),
        "Revenue":                  ("income-statement 8Q",       "TTM sum rows[0:4].revenue",     False),
        "Revenue Growth (Q)":       ("income-statement 8Q",       "rows[0].rev / rows[4].rev - 1", False),
        "Revenue Growth (YoY)":     ("income-statement 8Q",       "TTM / prior_TTM - 1",           False),
        "Gross Margin":             ("ratios-ttm",                "grossProfitMarginTTM",          False),
        "FCF Margin":               ("cash-flow 5Q",              "TTM_FCF / TTM_rev × 100",       False),
        "Free Cash Flow":           ("cash-flow 5Q",              "TTM sum freeCashFlow",          False),
        "Operating Income":         ("income-statement 8Q",       "TTM sum operatingIncome",       False),
        "EBIT":                     ("income-statement 8Q",       "TTM sum ebit",                  False),
        "PE Ratio":                 ("ratios-ttm",                "priceToEarningsRatioTTM",       False),
        "PS Ratio":                 ("ratios-ttm",                "priceToSalesRatioTTM",          False),
        "EV/EBITDA":                ("key-metrics-ttm",           "evToEBITDATTM",                 False),
        "EPS Growth":               ("income-statement-growth 2Q","growthEPSDiluted × 100",        False),
        "Debt / Equity":            ("ratios-ttm",                "debtToEquityRatioTTM",          False),
        "Net Debt / EBITDA":        ("key-metrics-ttm",           "netDebtToEBITDATTM",            False),
        "Shares Insiders":          ("—",                         "no FMP Starter endpoint",       True),
        "Earnings Date":            ("earnings 8",                "future_earn[0].date",           False),
        "Revenue Growth Est.":      ("—",                         "analyst-estimates → 402 Starter",True),
        "Rev Growth Next Quarter":  ("earnings 8",                "future[0]_est / py_actual - 1", False),
        "Rev Growth Next Year":     ("—",                         "analyst-estimates → 402 Starter",True),
        "EPS Growth Est.":          ("—",                         "analyst-estimates → 402 Starter",True),
        "EPS Growth This Quarter":  ("earnings 8",                "future[0]_eps_est / py_eps - 1",False),
        "EPS Growth Next Quarter":  ("—",                         "only 1 future Q in earnings",   True),
        "EPS Growth This Year":     ("—",                         "analyst-estimates → 402 Starter",True),
        "EPS Growth Next Year":     ("—",                         "analyst-estimates → 402 Starter",True),
    }

    out: list[dict] = []
    all_cols = list(_META.keys())
    # Also include any extra CSV keys not in meta
    for k in csv_row:
        if k not in all_cols:
            all_cols.append(k)

    for col in all_cols:
        meta = _META.get(col, ("unknown", "unknown", False))
        src_endpoint, formula, plan_limited = meta

        fmp_val   = fmp_fields.get(col)        # may be None if not mapped
        csv_val   = csv_row.get(col)
        final_val = merged_row.get(col)

        # Classify source
        if col == "Theme":
            source = "canonical_theme"
            missing_reason = ""
        elif col == "Symbol":
            source = "csv_fallback"
            missing_reason = ""
        elif fmp_val is not None and str(final_val) == str(fmp_val):
            source = "fmp"
            missing_reason = ""
        elif col in missing_fmp and csv_val:
            source = "csv_fallback"
            missing_reason = (
                "plan_limited" if plan_limited else
                f"fmp_missing: {col} not in snapshot fields"
            )
        elif col in missing_fmp and not csv_val:
            source = "missing"
            missing_reason = (
                "plan_limited (no CSV value either)" if plan_limited else
                "fmp_missing + no_csv_value"
            )
        elif not snap:
            source = "csv_fallback"
            missing_reason = "no_fmp_snapshot"
        elif final_val == csv_val and fmp_val is None:
            source = "csv_fallback"
            missing_reason = "fmp_field_not_mapped_or_null"
        else:
            source = "csv_fallback"
            missing_reason = ""

        out.append({
            "column":          col,
            "final_value":     final_val,
            "source":          source,
            "fmp_value":       fmp_val,
            "csv_value":       csv_val,
            "source_endpoint": src_endpoint,
            "formula":         formula,
            "plan_limited":    plan_limited,
            "missing_reason":  missing_reason,
            "refreshed_at":    refreshed_at,
        })

    return {
        "symbol":         sym,
        "watchlist_id":   wl_id,
        "cache_exists":   snap is not None,
        "refreshed_at":   refreshed_at,
        "fmp_field_count": len(fmp_fields),
        "missing_count":  len(missing_fmp),
        "missing_fields": sorted(missing_fmp),
        "provenance":     out,
    }


# ── Theme classifier endpoints ─────────────────────────────────────────────────


@router.post("/debug/themes/classify/start")
async def debug_themes_classify_start(
    watchlist_id: Optional[str] = None,
    missing_only: bool = True,
    cap: int = 40,
):
    """
    Trigger background LLM batch classification for unmapped watchlist symbols.

    Skips symbols already in canonical map, industry fallback, or LLM overrides.
    Job-level lock — only one run at a time. Returns immediately; poll /status.

    Provider: THEME_CLASSIFIER_PROVIDER env var (gemini default, openai fallback).
    Model:    THEME_CLASSIFIER_MODEL env var (gemini-2.0-flash-lite / gpt-4o-mini default).

    Query params:
      watchlist_id  — target watchlist (None = all mapped watchlists)
      missing_only  — true: only classify unmapped symbols (default true)
      cap           — max symbols per batch (default 40)
    """
    from services.watchlist_theme_classifier import (
        classify_watchlist_themes,
        get_classifier_status,
        _lock,
    )

    if _lock().locked():
        return {
            "status": "already_running",
            "state":  get_classifier_status(),
        }

    await classify_watchlist_themes(watchlist_id, missing_only=missing_only, cap=cap)
    return {
        "status": "started",
        "state":  get_classifier_status(),
    }


@router.get("/debug/themes/classify/status")
async def debug_themes_classify_status(watchlist_id: Optional[str] = None):
    """
    Poll progress of the background theme classifier job.

    Returns:
      running, total_symbols, mapped_existing, mapped_from_fmp_industry,
      mapped_from_llm, needs_theme, queued, processed, failed,
      current_batch, started_at, updated_at, failures, last_provider, last_model
    """
    from services.watchlist_theme_classifier import get_classifier_status, get_needs_review

    state  = get_classifier_status()
    review = get_needs_review()

    if watchlist_id and state.get("watchlist_id") != watchlist_id:
        return {
            "note":          f"Last run was for watchlist {state.get('watchlist_id')!r}, not {watchlist_id!r}",
            "state":         state,
            "needs_review":  review,
        }

    return {
        "state":        state,
        "needs_review": review,
    }


@router.get("/debug/themes/provenance")
async def debug_themes_provenance(symbol: str):
    """
    Return full theme resolution trace for a single symbol.

    Shows:
      canonical_theme, canonical_theme_id, source (neon_manual_override /
      llm_classified / canonical_map_or_industry / none),
      llm_override entry, neon_override entry, needs_review reason, is_mapped.

    Read-only — no provider calls.
    """
    from services.watchlist_theme_classifier import get_theme_provenance
    return get_theme_provenance(symbol)


# ── Strategy report endpoints ─────────────────────────────────────────────────


@router.post("/strategy-report/generate")
async def strategy_report_generate(body: dict):
    """
    Generate a strategy report for a watchlist using only cached data.

    No FMP, Tradier, or LLM calls. Uses stage2 LKG (technical) +
    fundamentals store + BOTTLENECK_MAP (static) + theme mapper.

    POST body:
      {
        "watchlist_id": "...",
        "strategy_id":  "bottlenecks" | "asymmetry",   (or "serenity" | "sjcapital")
        "save":         true
      }

    strategy_id aliases:
      "bottlenecks" → serenity playbook (Bottlenecks strategy)
      "asymmetry"   → sjcapital playbook (Asymmetry strategy)
    """
    import asyncio
    from services.watchlist_strategy_report import generate_report

    watchlist_id = body.get("watchlist_id", "")
    strategy_id  = body.get("strategy_id", "")
    save         = bool(body.get("save", True))

    if not watchlist_id:
        raise HTTPException(status_code=400, detail="watchlist_id is required")
    if not strategy_id:
        raise HTTPException(status_code=400, detail="strategy_id is required (bottlenecks or asymmetry)")

    try:
        report = await generate_report(watchlist_id, strategy_id, save=save)
        return report
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Report generation failed: {exc}")


@router.get("/strategy-report/history")
async def strategy_report_history(watchlist_id: Optional[str] = None):
    """
    Return list of saved strategy report summaries.

    Query params:
      watchlist_id — filter by watchlist (optional)

    Returns list of {report_id, watchlist_id, strategy_id, strategy_name,
                      generated_at, ticker_count, matched_count}.
    """
    from services.watchlist_strategy_report import get_report_history
    return {"history": get_report_history(watchlist_id)}


@router.get("/strategy-report/{report_id}")
async def strategy_report_get(report_id: str):
    """
    Retrieve a saved strategy report by ID.

    Returns the full report including ranked_results, factor_scores,
    filter reasons, missing_data_notes, and cache_freshness.
    """
    from services.watchlist_strategy_report import get_report
    report = get_report(report_id)
    if report is None:
        raise HTTPException(status_code=404, detail=f"Report {report_id!r} not found")
    return report


@router.get("/debug")
async def debug_endpoint():
    """Debug endpoint — returns file path, existence, Postgres availability."""
    info: Dict[str, Any] = {
        "json_file_path": str(_WATCHLIST_FILE),
        "json_file_exists": _WATCHLIST_FILE.exists(),
    }
    try:
        from data.pg_storage import is_available, watchlist_list as pg_wl_list
        info["postgres_available"] = is_available()
        if is_available():
            entries = pg_wl_list()
            info["postgres_watchlist_count"] = len(entries)
            info["postgres_watchlists"] = entries
    except Exception as e:
        info["postgres_error"] = str(e)
    if _WATCHLIST_FILE.exists():
        try:
            content = _WATCHLIST_FILE.read_text()
            info["json_file_size_bytes"] = len(content)
            info["json_preview"] = content[:500]
        except Exception as e:
            info["json_read_error"] = str(e)
    return info


@router.get("/news")
async def news_endpoint():
    """
    Live News for the default watchlist — LKG cached, never blocks on stale data.

    Response shape:
      {
        "articles":          {TICKER: [article, ...]},   // per-ticker map
        "top_articles":      [...],                       // pre-ranked signal articles (use this)
        "high_signal_count": int,
        "by_catalyst_type":  {type: count},
        "news_signal_meta":  {...},
        "cached_at":         "ISO timestamp",
        "cache_age_s":       int,
        "is_building":       bool,                        // true = bg refresh in progress
      }
    """
    store = load_watchlist()
    if store is None:
        return _news_response({}, {}, _time.time())
    tickers = store.get("tickers", [])
    if not tickers:
        return _news_response({}, {}, _time.time())
    return await _get_news_for_watchlist("default", tickers)


@router.post("/refresh")
async def refresh_endpoint():
    """Re-run multi-source parallel analysis on the most recent watchlist."""
    agent = _get_agent()
    data_service = _get_data_service()

    store = load_watchlist()
    if store is None:
        raise HTTPException(status_code=404, detail="No watchlist saved. Upload a CSV first.")

    tickers = store.get("tickers", [])
    csv_data = store.get("csv_data", [])
    if not tickers:
        raise HTTPException(status_code=400, detail="Watchlist has no tickers")

    result = await run_analysis_pipeline(tickers, csv_data, agent, data_service)

    if isinstance(result, dict) and result.get("error") and "sections" not in result:
        raise HTTPException(status_code=500, detail=result["error"])

    # Save updated analysis back to the watchlist
    store_id = store.get("id", "default")
    store_name = store.get("name", "Watchlist")
    save_watchlist(csv_data, result, watchlist_id=store_id, name=store_name)

    return result


@router.post("/analyze")
async def analyze_endpoint(body: WatchlistAnalyzeRequest):
    """
    Run multi-source parallel analysis pipeline on a watchlist.

    Accepts either:
    - tickers + csv_data directly in the request body
    - watchlist_id to load from Postgres

    Fires all 5 data sources in parallel (Grok, Gemini, Claude, SEC Edgar, TA),
    then synthesizes through Claude with intelligence rules.
    Returns 6 sections × 4 tickers structured JSON.
    """
    agent = _get_agent()
    data_service = _get_data_service()

    tickers = body.tickers or []
    csv_data = body.csv_data or []

    # If watchlist_id provided, load tickers and CSV from stored watchlist
    if body.watchlist_id:
        store = load_watchlist(body.watchlist_id)
        if store is None:
            raise HTTPException(status_code=404, detail="Watchlist not found")
        if not tickers:
            tickers = store.get("tickers", [])
        if not csv_data:
            csv_data = store.get("csv_data", [])
    elif not tickers and csv_data:
        # Extract tickers from CSV data
        tickers = extract_tickers(csv_data)

    if not tickers:
        raise HTTPException(
            status_code=400,
            detail="No tickers provided. Send tickers, csv_data, or watchlist_id.",
        )

    result = await run_analysis_pipeline(tickers, csv_data, agent, data_service)

    if isinstance(result, dict) and result.get("error") and "sections" not in result:
        raise HTTPException(status_code=500, detail=result["error"])

    return result


@router.get("")
async def get_endpoint():
    """Return the most recent saved watchlist, or {empty: true}."""
    store = load_watchlist()
    if store is None:
        return {"empty": True}
    return store


@router.delete("")
async def delete_endpoint():
    """Clear the most recent watchlist and invalidate earnings cache."""
    result = clear_watchlist()
    try:
        from services.user_earnings_service import invalidate_user_earnings  # type: ignore
        invalidate_user_earnings("watchlist")
    except Exception as _e:
        print(f"[watchlist-delete] earnings invalidation skipped: {_e}")
    return result


# ── Watchlist Earnings ───────────────────────────────────────────────────────

@router.get("/earnings")
async def watchlist_earnings_endpoint(
    from_date: Optional[str] = None,
    to_date:   Optional[str] = None,
):
    """
    Return upcoming earnings for the current user's saved watchlist tickers.

    Reuses user_earnings_service.get_or_sync_user_earnings("watchlist", ...)
    which is the same Neon-cached FMP pipeline used by the Catalyst Calendar
    (30-day cache TTL, auto-re-syncs when the symbol set changes, invalidated
    on watchlist save/delete).

    Response shape is compatible with Portfolio Terminal earnings_calendar:
      ticker, company, next_date, est_eps, last_eps, wtd, in_watchlist,
      + FMP extras: revenue_estimated, revenue_actual, time, period,
                    market_cap, logo, importance

    Returns {earnings: [], meta: {cache_status: "empty"}} for an empty watchlist.
    """
    import time as _tm
    from datetime import date as _date, timedelta as _td

    _t0 = _tm.time()

    store = load_watchlist()
    if store is None or not store.get("tickers"):
        return {
            "earnings":    [],
            "meta": {
                "universe":      "watchlist",
                "symbols_count": 0,
                "events_count":  0,
                "cache_status":  "empty",
                "source":        "fmp",
            },
        }

    tickers: list[str] = [t.upper() for t in store["tickers"] if t]
    symbols: set[str]  = set(tickers)

    # Default date window: today → +90 days
    _today = _date.today().isoformat()
    _from  = from_date or _today
    _to    = to_date   or (_date.today() + _td(days=90)).isoformat()

    # FMP key
    try:
        from config import FMP_API_KEY as _fmp_key  # type: ignore
    except Exception:
        _fmp_key = os.getenv("FMP_API_KEY", "")

    if not _fmp_key:
        return {
            "earnings": [],
            "meta": {
                "universe":      "watchlist",
                "symbols_count": len(symbols),
                "events_count":  0,
                "cache_status":  "error",
                "error":         "fmp_key_unavailable",
            },
        }

    try:
        from services.user_earnings_service import get_or_sync_user_earnings  # type: ignore
        events, meta = await get_or_sync_user_earnings(
            universe  = "watchlist",
            symbols   = symbols,
            fmp_key   = _fmp_key,
            from_date = _from,
            to_date   = _to,
        )
    except Exception as _e:
        print(f"[WATCHLIST_EARNINGS] get_or_sync error: {_e}")
        return {
            "earnings": [],
            "meta": {
                "universe":      "watchlist",
                "symbols_count": len(symbols),
                "events_count":  0,
                "cache_status":  "error",
                "error":         str(_e),
            },
        }

    # ── Normalise to Portfolio Terminal-compatible shape ───────────────────
    def _fmt_date(dt_str: str | None) -> str:
        if not dt_str:
            return "N/A"
        try:
            from datetime import datetime as _dt
            return _dt.strptime(dt_str, "%Y-%m-%d").strftime("%b %-d")
        except Exception:
            return dt_str or "N/A"

    normalised = []
    for ev in (events or []):
        sym = (ev.get("symbol") or "").upper()
        if not sym:
            continue
        normalised.append({
            # ── Portfolio Terminal-compatible fields ──
            "ticker":       sym,
            "company":      ev.get("companyName") or ev.get("name") or sym,
            "in_watchlist": True,
            "next_date":    _fmt_date(ev.get("date")),
            "date_raw":     ev.get("date"),
            "est_eps":      ev.get("epsEstimated"),
            "last_eps":     ev.get("epsActual"),
            "wtd":          None,   # no position — watchlist only
            # ── FMP extras (superset of Finnhub shape) ──
            "revenue_estimated": ev.get("revenueEstimated"),
            "revenue_actual":    ev.get("revenueActual"),
            "time":              ev.get("time"),
            "period":            ev.get("period"),
            "market_cap":        ev.get("marketCap"),
            "logo":              ev.get("logo"),
            "importance":        ev.get("importance"),
        })

    # Sort by date_raw ascending
    normalised.sort(key=lambda x: x.get("date_raw") or "")

    _ms = round((_tm.time() - _t0) * 1000)
    meta["elapsed_ms"]   = _ms
    meta["from_date"]    = _from
    meta["to_date"]      = _to
    meta["events_count"] = len(normalised)

    print(
        f"[WATCHLIST_EARNINGS] symbols={len(symbols)} events={len(normalised)} "
        f"cache_status={meta.get('cache_status')} elapsed_ms={_ms}"
    )
    return {"earnings": normalised, "meta": meta}


# ── Stock Deep-Dive ─────────────────────────────────────────────────────────

class StockDeepDiveRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    models: List[str] = ["grok", "gemini", "claude"]
    report_model: str = "claude"


@router.post("/stock/{ticker}/deep-dive")
async def stock_deep_dive_endpoint(ticker: str, body: StockDeepDiveRequest):
    """
    Three-model concurrent deep-dive for a single stock ticker.

    Phase 1 (all parallel): Grok X/Twitter sentiment + Gemini Google News +
                            Claude/GPT deep fundamental analysis
    Phase 2: short final synthesis call → structured JSON output

    Model routing (static-by-design):
      Models are caller-specified (body.models / body.report_model), defaulting to
      ["grok", "gemini", "claude"] + claude synthesis.  The prompt_router /
      reasoning_mode machinery does NOT apply here — the caller selects the panel
      explicitly from the frontend deep-dive modal.  Do not add dynamic routing
      inside this endpoint.
    """
    import traceback as _tb

    try:
        ticker = ticker.strip().upper()
        models = [m.strip().lower() for m in (body.models or ["grok", "gemini", "claude"])]
        report_model = (body.report_model or "claude").strip().lower()

        # ── Look up any stored CSV fundamentals for this ticker ───────────────
        def _get_fundamentals() -> str:
            try:
                store = load_watchlist()
                if store:
                    for row in store.get("csv_data", []):
                        sym = (row.get("Symbol") or row.get("symbol") or "").strip().upper()
                        if sym == ticker:
                            parts = []
                            for label, key in [
                                ("Price", "Stock Price"), ("MCap", "Market Cap"),
                                ("PE", "PE Ratio"), ("FwdPE", "Forward PE"),
                                ("RSI", "Relative Strength Index (RSI)"),
                                ("RevGrowth", "Revenue Growth (YoY)"),
                                ("EPSEst", "EPS Growth Est."), ("FCF", "FCF Margin"),
                                ("GrossMargin", "Gross Margin"), ("DE", "Debt / Equity"),
                                ("ShortFloat", "Short % Float"), ("EarningsDate", "Earnings Date"),
                            ]:
                                val = row.get(key, "")
                                if val:
                                    parts.append(f"{label}={val}")
                            return ", ".join(parts)
            except Exception:
                pass
            return ""

        fundamentals = _get_fundamentals()
        fundamentals_str = f" Fundamentals: {fundamentals}." if fundamentals else ""

        # ── Grok: X/Twitter real-time social sentiment ────────────────────────
        async def call_grok() -> str:
            xai_key = os.getenv("XAI_API_KEY") or os.getenv("GROK_API_KEY", "")
            if not xai_key:
                return "[No XAI_API_KEY configured]"
            prompt = (
                f"Search X/Twitter for recent posts about {ticker}. "
                f"Summarize the current retail and institutional sentiment, any viral catalysts "
                f"or concerns, and notable accounts discussing it. Be specific — mention price "
                f"targets, meme activity, earnings reactions if relevant. "
                f"2-4 paragraphs, conversational tone."
            )
            try:
                async with httpx.AsyncClient(timeout=60.0) as client:
                    resp = await client.post(
                        "https://api.x.ai/v1/responses",
                        headers={
                            "Authorization": f"Bearer {xai_key}",
                            "Content-Type": "application/json",
                        },
                        json={
                            "model": MODEL_GROK,
                            "tools": [{"type": "x_search", "x_search": {}}],
                            "input": [{"role": "user", "content": prompt}],
                        },
                    )
                if resp.status_code != 200:
                    return f"[Grok API error {resp.status_code}: {resp.text[:300]}]"
                data = resp.json()
                for block in data.get("output", []):
                    if block.get("type") == "message":
                        for c in block.get("content", []):
                            if c.get("type") == "output_text":
                                return c.get("text", "") or "[Grok: empty output_text]"
                return "[Grok: no text in response]"
            except asyncio.TimeoutError:
                return "[Grok: timed out after 60s]"
            except Exception as exc:
                return f"[Grok error: {type(exc).__name__}: {exc}]"

        # ── Gemini: Google News headlines ─────────────────────────────────────
        async def call_gemini() -> str:
            gemini_key = os.getenv("GEMINI_API_KEY", "")
            if not gemini_key:
                return "[No GEMINI_API_KEY configured]"
            prompt = (
                f"Search Google News for {ticker} stock. Summarize the 3-5 most important "
                f"headlines from the last 30 days: analyst upgrades/downgrades, earnings surprises, "
                f"product launches, regulatory news, or macro headwinds. "
                f"Include the source and approximate date for each. "
                f"2-4 paragraphs."
            )
            try:
                url = (
                    "https://generativelanguage.googleapis.com/v1beta/models/"
                    f"{MODEL_GEMINI}:generateContent?key={gemini_key}"
                )
                async with httpx.AsyncClient(timeout=60.0) as client:
                    resp = await client.post(
                        url,
                        headers={"Content-Type": "application/json"},
                        json={
                            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
                            "tools": [{"google_search": {}}],
                            "generationConfig": {"maxOutputTokens": 2048, "temperature": 0.2},
                        },
                    )
                resp.raise_for_status()
                data = resp.json()
                candidates = data.get("candidates", [])
                if not candidates:
                    return "[Gemini: no candidates in response]"
                parts = candidates[0].get("content", {}).get("parts", [])
                text = "".join(p.get("text", "") for p in parts if "text" in p)
                return text or "[Gemini: empty response]"
            except asyncio.TimeoutError:
                return "[Gemini: timed out after 60s]"
            except Exception as exc:
                return f"[Gemini error: {type(exc).__name__}: {exc}]"

        # ── Claude: deep fundamental + technical analysis ─────────────────────
        async def call_claude_analysis() -> str:
            anthropic_key = os.getenv("ANTHROPIC_API_KEY", "")
            if not anthropic_key:
                return "[No ANTHROPIC_API_KEY configured]"
            prompt = (
                f"You are a senior equity analyst.{fundamentals_str} "
                f"Given the fundamentals for {ticker}, provide a structured analysis covering: "
                f"(1) a 2-3 sentence executive summary, (2) the bull case, (3) the bear case, "
                f"(4) top 3 risk factors, (5) key technical levels/pattern to watch, "
                f"(6) what sell-side consensus looks like. "
                f"Write in clear, specific prose — cite actual metrics where possible."
            )
            try:
                import anthropic as _anthropic
                client = _anthropic.AsyncAnthropic(
                    api_key=anthropic_key, timeout=60.0
                )
                response = await client.messages.create(
                    model=MODEL_CLAUDE_PREMIUM,
                    max_tokens=1500,
                    messages=[{"role": "user", "content": prompt}],
                )
                return "".join(b.text for b in response.content if hasattr(b, "text"))
            except asyncio.TimeoutError:
                return "[Claude: timed out after 60s]"
            except Exception as exc:
                return f"[Claude error: {type(exc).__name__}: {exc}]"

        # ── GPT: deep fundamental + technical analysis ────────────────────────
        async def call_gpt_analysis() -> str:
            openai_key = os.getenv("OPENAI_API_KEY", "")
            if not openai_key:
                return "[No OPENAI_API_KEY configured]"
            prompt = (
                f"You are a senior equity analyst.{fundamentals_str} "
                f"Given the fundamentals for {ticker}, provide a structured analysis covering: "
                f"(1) a 2-3 sentence executive summary, (2) the bull case, (3) the bear case, "
                f"(4) top 3 risk factors, (5) key technical levels/pattern to watch, "
                f"(6) what sell-side consensus looks like. "
                f"Write in clear, specific prose — cite actual metrics where possible."
            )
            try:
                from openai import AsyncOpenAI
                client = AsyncOpenAI(api_key=openai_key, timeout=60.0)
                resp = await client.chat.completions.create(
                    model=MODEL_GPT4O,
                    max_tokens=1500,
                    messages=[{"role": "user", "content": prompt}],
                )
                return resp.choices[0].message.content or "[GPT: empty response]"
            except asyncio.TimeoutError:
                return "[GPT: timed out after 60s]"
            except Exception as exc:
                return f"[GPT error: {type(exc).__name__}: {exc}]"

        # ── Phase 1: run ALL requested models concurrently ────────────────────
        coro_map: Dict[str, Any] = {}
        if "grok" in models:
            coro_map["grok"] = call_grok()
        if "gemini" in models:
            coro_map["gemini"] = call_gemini()
        # "claude" or "claude_gpt" in models → run the deep analysis persona
        run_claude = "claude" in models or "claude_gpt" in models
        run_gpt_model = "gpt" in models
        if run_claude and report_model != "gpt":
            coro_map["claude"] = call_claude_analysis()
        elif run_gpt_model or (run_claude and report_model == "gpt"):
            coro_map["gpt"] = call_gpt_analysis()

        keys = list(coro_map.keys())
        coros = list(coro_map.values())
        raw_results = await asyncio.gather(*coros, return_exceptions=True)

        model_outputs: Dict[str, str] = {}
        for k, v in zip(keys, raw_results):
            if isinstance(v, Exception):
                model_outputs[k] = f"[{k} exception: {v}]"
            else:
                model_outputs[k] = str(v)

        grok_text = model_outputs.get("grok", "")
        gemini_text = model_outputs.get("gemini", "")
        claude_text = model_outputs.get("claude", "")
        gpt_text = model_outputs.get("gpt", "")

        # ── Phase 2: final synthesis → structured JSON ────────────────────────
        synthesis_input_parts = []
        if grok_text and not grok_text.startswith("["):
            synthesis_input_parts.append(f"=== GROK (X/Twitter Sentiment) ===\n{grok_text}")
        if gemini_text and not gemini_text.startswith("["):
            synthesis_input_parts.append(f"=== GEMINI (Google News) ===\n{gemini_text}")
        analysis_text = claude_text or gpt_text
        if analysis_text and not analysis_text.startswith("["):
            synthesis_input_parts.append(f"=== ANALYST DEEP-DIVE ===\n{analysis_text}")

        synthesis_context = "\n\n".join(synthesis_input_parts)

        synthesis_prompt = (
            f"You are synthesizing a multi-source research report on {ticker}.{fundamentals_str}\n\n"
            f"{synthesis_context}\n\n"
            f"Based on all of the above, respond with ONLY a valid JSON object "
            f"(no markdown fences, no preamble) containing exactly these keys:\n"
            f'  "summary": "2-3 sentence executive summary",\n'
            f'  "bull_case": "bull case paragraph",\n'
            f'  "bear_case": "bear case paragraph",\n'
            f'  "risk_factors": ["risk 1", "risk 2", "risk 3"],\n'
            f'  "technical_outlook": "key technical levels and pattern to watch",\n'
            f'  "analyst_sentiment": "what sell-side analysts are saying"\n'
            f"Return ONLY the JSON object."
        )

        synthesis: dict = {}
        synth_key = os.getenv("ANTHROPIC_API_KEY", "")
        if report_model == "gpt" or not synth_key:
            openai_key = os.getenv("OPENAI_API_KEY", "")
            if openai_key:
                try:
                    from openai import AsyncOpenAI
                    _oa = AsyncOpenAI(api_key=openai_key, timeout=60.0)
                    _resp = await _oa.chat.completions.create(
                        model=MODEL_GPT4O,
                        max_tokens=1500,
                        messages=[{"role": "user", "content": synthesis_prompt}],
                        response_format={"type": "json_object"},
                    )
                    synthesis = _json.loads(_resp.choices[0].message.content or "{}")
                except Exception as exc:
                    print(f"[DEEP-DIVE] GPT synthesis failed: {exc}")
        if not synthesis and synth_key:
            try:
                import anthropic as _anthropic
                _ac = _anthropic.AsyncAnthropic(api_key=synth_key, timeout=60.0)
                _cr = await _ac.messages.create(
                    model=MODEL_CLAUDE_PREMIUM,
                    max_tokens=1500,
                    messages=[{"role": "user", "content": synthesis_prompt}],
                )
                synth_text = "".join(b.text for b in _cr.content if hasattr(b, "text"))
                synth_text = _re.sub(r"```json\s*", "", synth_text)
                synth_text = _re.sub(r"```\s*", "", synth_text).strip()
                m = _re.search(r'\{[\s\S]*\}', synth_text)
                if m:
                    synthesis = _json.loads(m.group())
            except Exception as exc:
                print(f"[DEEP-DIVE] Claude synthesis failed: {exc}")

        print(
            f"[DEEP-DIVE] {ticker}: "
            f"grok={'ok' if grok_text and not grok_text.startswith('[') else 'skip/err'}, "
            f"gemini={'ok' if gemini_text and not gemini_text.startswith('[') else 'skip/err'}, "
            f"claude={'ok' if claude_text and not claude_text.startswith('[') else 'skip/err'}, "
            f"gpt={'ok' if gpt_text and not gpt_text.startswith('[') else 'skip/err'}"
        )

        return {
            "grok":              grok_text or None,
            "gemini":            gemini_text or None,
            "claude":            claude_text or None,
            "gpt":               gpt_text or None,
            "summary":           synthesis.get("summary", ""),
            "bull_case":         synthesis.get("bull_case", ""),
            "bear_case":         synthesis.get("bear_case", ""),
            "risk_factors":      synthesis.get("risk_factors", []),
            "technical_outlook": synthesis.get("technical_outlook", ""),
            "analyst_sentiment": synthesis.get("analyst_sentiment", ""),
        }

    except HTTPException:
        raise
    except Exception as e:
        _tb.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ── Category override endpoints ──────────────────────────────────────────────

@router.patch("/category")
async def patch_category_endpoint(request: Request, body: dict):
    """
    Persist a single manual ticker→category assignment.

    Body: {ticker, category, source='manual', reason=null}

    Overrides are applied immediately on the next Watchlist/Chart Radar GET.
    They survive server restarts and AI re-analyses.
    """
    ticker   = str(body.get("ticker") or "").strip().upper()
    category = str(body.get("category") or "").strip()
    source   = str(body.get("source") or "manual")
    reason   = body.get("reason")

    if not ticker:
        raise HTTPException(status_code=400, detail="ticker is required")
    if not category:
        raise HTTPException(status_code=400, detail="category is required")

    try:
        from auth import verify_token as _vt
        token = (request.headers.get("Authorization") or "").removeprefix("Bearer ").strip()
        payload = _vt(token) if token else {}
        user_id = payload.get("sub") or "default"
    except Exception:
        user_id = "default"

    from services.category_overrides import upsert_override as _upsert
    ok = _upsert(user_id, ticker, category, source, reason)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to persist override")
    return {"success": True, "ticker": ticker, "category": category, "user_id": user_id}


@router.post("/categories/bulk")
async def bulk_categories_endpoint(request: Request, body: dict):
    """
    Persist multiple manual category assignments in one call.

    Body:
    {
      "updates": [
        {"ticker": "BTDR", "category": "Data Center Infrastructure", "source": "manual"},
        ...
      ],
      "categoryMoves": [
        {"from": "Crypto Equities / Blockchain", "to": "Data Center Infrastructure"},
        {"from": "Solar", "to": "Clean Energy"}
      ]
    }

    categoryMoves resolves which tickers are currently in the 'from' section
    and adds an override for each one pointing to 'to'.
    All operations are idempotent (upsert).
    """
    try:
        from auth import verify_token as _vt
        token = (request.headers.get("Authorization") or "").removeprefix("Bearer ").strip()
        payload = _vt(token) if token else {}
        user_id = payload.get("sub") or "default"
    except Exception:
        user_id = "default"

    updates: list[dict] = list(body.get("updates") or [])

    # Resolve categoryMoves → individual ticker overrides
    cat_moves: list[dict] = list(body.get("categoryMoves") or [])
    if cat_moves:
        try:
            from services.watchlist_service import load_watchlist as _lw
            store = _lw()
            if store:
                sections = store.get("analysis", {}).get("sections", [])
                for move in cat_moves:
                    from_cat = str(move.get("from") or "").strip()
                    to_cat   = str(move.get("to")   or "").strip()
                    if not from_cat or not to_cat:
                        continue
                    for sec in sections:
                        if (sec.get("title") or "").strip() == from_cat:
                            for row in sec.get("tickers", []):
                                sym = str(row.get("symbol") or "").strip().upper()
                                if sym:
                                    updates.append({
                                        "ticker":   sym,
                                        "category": to_cat,
                                        "source":   "manual",
                                        "reason":   f"category_move:{from_cat}→{to_cat}",
                                    })
        except Exception as _cm_err:
            print(f"[WATCHLIST] categoryMoves resolution failed (non-fatal): {_cm_err}")

    from services.category_overrides import bulk_upsert as _bulk
    count = _bulk(user_id, updates)
    return {
        "success": True,
        "upserted": count,
        "user_id": user_id,
    }


# ── Watchlist favorites ───────────────────────────────────────────────────────
# Pure user metadata — no market-data/provider calls.
# Endpoints must be defined BEFORE /{watchlist_id} catch-all routes.

class _AddFavoriteBody(BaseModel):
    ticker: str


@router.get("/favorites")
async def get_favorites(request: Request):
    """
    GET /api/watchlist/favorites
    Returns the current user's favorited tickers.
    """
    from data.pg_storage import list_watchlist_favorites, _ensure_wf_table
    _ensure_wf_table()
    user_id = _get_user_id(request)
    favorites = list_watchlist_favorites(user_id)
    return {"favorites": favorites, "count": len(favorites)}


@router.post("/favorites")
async def add_favorite(request: Request, body: _AddFavoriteBody):
    """
    POST /api/watchlist/favorites
    Body: {"ticker": "CRWV"}
    Idempotently adds ticker to favorites. Returns updated list.
    """
    from data.pg_storage import add_watchlist_favorite, list_watchlist_favorites, _ensure_wf_table
    _ensure_wf_table()
    raw = body.ticker.strip().upper()
    if not raw:
        raise HTTPException(status_code=422, detail="ticker must be a non-empty string")
    user_id = _get_user_id(request)
    ok = add_watchlist_favorite(user_id, raw)
    if not ok:
        raise HTTPException(status_code=503, detail="Database unavailable — could not save favorite")
    favorites = list_watchlist_favorites(user_id)
    return {"ticker": raw, "is_favorite": True, "favorites": favorites}


@router.delete("/favorites/{ticker}")
async def remove_favorite(request: Request, ticker: str):
    """
    DELETE /api/watchlist/favorites/{ticker}
    Idempotently removes ticker from favorites. Returns updated list.
    """
    from data.pg_storage import remove_watchlist_favorite, list_watchlist_favorites, _ensure_wf_table
    _ensure_wf_table()
    raw = ticker.strip().upper()
    if not raw:
        raise HTTPException(status_code=422, detail="ticker must be a non-empty string")
    user_id = _get_user_id(request)
    ok = remove_watchlist_favorite(user_id, raw)
    if not ok:
        raise HTTPException(status_code=503, detail="Database unavailable — could not remove favorite")
    favorites = list_watchlist_favorites(user_id)
    return {"ticker": raw, "is_favorite": False, "favorites": favorites}


# ── Defiance 2X enrichment helper (shared by watchlist alias + strategy route) ─

async def _build_defiance_rows(
    *,
    catalog: list[dict],
    get_quotes,
    quote_cache: dict,
    get_theme,
    get_ts,
    tab_key: str,
) -> dict:
    """
    Core enrichment for Defiance 2X Long ETF rows.

    Enrichment policy
    -----------------
    - underlying_symbol is the primary/row ticker; defiance_etf_ticker is metadata only.
    - `is_already_tracked` = underlying was already in the canonical quote cache *before*
      this request fired; no new Tradier call was needed for it.
    - `quote_reused` = same as is_already_tracked (alias kept for API clarity).
    - chart_symbol and quote_source_symbol always equal the underlying symbol.
    - The Defiance ETF ticker is NEVER used for price, market cap, VolX, or chart.

    Logging
    -------
    Emits a single [DEFIANCE_2X] summary line with:
      total_catalog | reused_quote_count | new_tradier_fetch_count | fetch_symbols
    """
    # Snapshot which underlying symbols are already in the canonical quote cache
    # BEFORE calling get_watchlist_quotes (which may pull new Tradier quotes).
    underlying_syms = [r["underlying_symbol"] for r in catalog if r.get("underlying_symbol")]
    already_tracked: set[str] = {s.upper() for s in underlying_syms if s.upper() in quote_cache}
    new_fetch_syms = [s for s in underlying_syms if s.upper() not in already_tracked]

    quotes = await get_quotes(underlying_syms)

    rows: list[dict] = []
    for entry in catalog:
        sym = entry.get("underlying_symbol")
        if not sym:
            continue

        etf_ticker = entry.get("defiance_etf_ticker", "")
        # Defensive guard: never allow a row where underlying == ETF ticker
        if sym.upper() == etf_ticker.upper():
            print(
                f"[DEFIANCE_2X] skipped row — underlying==etf_ticker: {sym!r}"
                f"  name={entry.get('defiance_etf_name')!r}"
            )
            continue

        sym_up  = sym.upper()
        q       = quotes.get(sym_up, {})
        price   = q.get("price")
        change  = q.get("change_pct_1d")
        volume  = q.get("volume")
        rel_vol = q.get("relative_volume")
        name    = q.get("name") or sym

        market_cap = q.get("market_cap")
        vmc = _vol_mc_fields(price, volume, market_cap)

        stage = _get_stage2_breakout(sym)
        theme = get_theme(sym)

        is_tracked = sym_up in already_tracked

        rows.append({
            "symbol":            sym,
            "chart_symbol":      sym,
            "quote_source_symbol": sym,
            "name":              name,
            "price":             price,
            "change_pct":        change,
            "market_cap":        vmc.get("market_cap"),
            "volume":            volume,
            "vol_x":             rel_vol,
            "vol_mc_ratio":      vmc.get("vol_mc_ratio"),
            "vol_mc_pct":        vmc.get("vol_mc_pct"),
            "vol_mc_label":      vmc.get("vol_mc_label"),
            "theme":             theme,
            "stage_analysis":    stage,
            "is_already_tracked": is_tracked,
            "quote_reused":      is_tracked,
            "defiance_etf": {
                "symbol":       etf_ticker,
                "name":         entry.get("defiance_etf_name", ""),
                "leverage":     entry.get("leverage", 2),
                "direction":    entry.get("direction", "long"),
                "source_url":   entry.get("source_url", ""),
                "last_seen_at": entry.get("last_seen_at"),
            },
        })

    # Sort: priced rows first, then market_cap desc, nulls last
    rows.sort(key=lambda r: (
        r.get("price") is None,
        r.get("market_cap") is None,
        -(r.get("market_cap") or 0),
    ))

    reused = sum(1 for r in rows if r["is_already_tracked"])
    new_fetched = len(rows) - reused
    print(
        f"[DEFIANCE_2X] response built: total_catalog={len(catalog)}"
        f"  rows={len(rows)}"
        f"  reused_quote_count={reused}"
        f"  new_tradier_fetch_count={len(new_fetch_syms)}"
        + (f"  fetch_syms={new_fetch_syms}" if new_fetch_syms else "")
    )

    ts = get_ts()
    updated_at = (
        datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        if ts else None
    )

    return {
        "tab":        tab_key,
        "title":      "Defiance 2× Long ETFs",
        "updated_at": updated_at,
        "count":      len(rows),
        "rows":       rows,
    }


# ── Defiance 2X Long ETF tab — backwards-compatible alias ────────────────────
# Primary endpoint: GET /api/strategy/defiance (Strategy page)
# This route is retained for any existing frontend consumers.

@router.get("/defiance-2x")
async def defiance_2x_endpoint(request: Request, force_refresh: bool = False):
    """
    Backwards-compatible alias for /api/strategy/defiance.

    The canonical endpoint lives on the Strategy page.  This route delegates
    to the same underlying logic so existing frontend consumers continue to
    work unchanged while the frontend migrates to /api/strategy/defiance.

    Response shape is identical to /api/strategy/defiance.
    """
    from services.defiance_leveraged_etfs_service import (
        get_catalog         as _d2x_catalog,
        refresh_catalog     as _d2x_refresh,
        get_last_refresh_ts as _d2x_ts,
        get_quarantined     as _d2x_quarantined,
    )
    from services.watchlist_quote_cache import (
        get_watchlist_quotes as _get_quotes,
        _quote_cache         as _qc,
    )
    from services.theme_ticker_mapper import map_ticker_to_primary_theme
    import os as _os

    # ── Admin force-refresh ───────────────────────────────────────────────────
    if force_refresh:
        _ak = _os.getenv("AGENT_API_KEY", "")
        _auth = request.headers.get("Authorization", "")
        if _ak and _auth != f"Bearer {_ak}":
            raise HTTPException(status_code=403, detail="Admin only")
        await _d2x_refresh(force=True)

    catalog = _d2x_catalog()

    if not catalog:
        import asyncio as _aio
        _aio.create_task(_d2x_refresh())
        return {
            "tab":        "defiance-2x",
            "title":      "Defiance 2× Long ETFs",
            "rows":       [],
            "count":      0,
            "updated_at": None,
            "status":     "warming_up",
        }

    return await _build_defiance_rows(
        catalog      = catalog,
        get_quotes   = _get_quotes,
        quote_cache  = _qc,
        get_theme    = map_ticker_to_primary_theme,
        get_ts       = _d2x_ts,
        tab_key      = "defiance-2x",
    )


# ── Parameterized endpoints (MUST be after static paths) ────────────────────

@router.patch("/{watchlist_id}/rename")
async def rename_endpoint(watchlist_id: str, body: dict):
    """Rename a specific watchlist."""
    new_name = body.get("name", "").strip()
    if not new_name:
        raise HTTPException(status_code=400, detail="Name is required")
    try:
        from data.pg_storage import watchlist_rename, is_available
        if is_available():
            ok = watchlist_rename(watchlist_id, new_name)
            if ok:
                return {"success": True, "name": new_name}
        return {"error": "Postgres unavailable"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{watchlist_id}")
async def get_by_id_endpoint(watchlist_id: str):
    """
    Return a specific watchlist by ID.

    Ticker rows are enriched on every GET with:
      - name          (from Tradier description)
      - price         (Tradier live, or CSV fallback)
      - change_pct_1d (Tradier 1D % change)
      - quote_source / quote_updated_at

    All existing LLM-generated fields (catalyst, sentiment, action_note, etc.)
    are preserved.  Quote data is served from a 10-minute in-memory cache;
    a background refresh is triggered automatically when the TTL expires.
    """
    import asyncio as _aio
    store = load_watchlist(watchlist_id)
    if store is None:
        return {"empty": True}
    try:
        store = await _enrich_store_with_quotes(store)
    except Exception as _enrich_err:
        print(f"[WATCHLIST] Quote enrichment failed (returning raw): {_enrich_err}")

    # ── FMP fundamentals overlay (weekly cache, non-blocking read) ────────────
    # Overlays cached FMP values onto csv_data. No-null overwrite: FMP null/missing
    # values never erase existing CSV values. Theme column is never sourced from FMP.
    try:
        from data.watchlist_fundamentals_store import get_snapshots_bulk as _get_fund_snaps
        from services.watchlist_fundamentals_refresh import apply_fmp_overlays as _apply_fmp
        _raw_csv = store.get("csv_data") or []
        if _raw_csv:
            _syms = [
                (r.get("Symbol") or r.get("symbol") or r.get("Ticker") or "").strip().upper()
                for r in _raw_csv
            ]
            _snaps = _get_fund_snaps([s for s in _syms if s])
            if _snaps:
                store["csv_data"] = _apply_fmp(_raw_csv, _snaps)
    except Exception as _fund_err:
        pass  # non-fatal — serve unmodified CSV data

    # ── Alert bus hook: watchlist full-activity metrics ───────────────────────
    # Fire-and-forget; runs after response is already built. No provider calls.
    async def _watchlist_alert_hook(store_snap: dict) -> None:
        try:
            from services.alert_signal_bus import record_signal_snapshot as _rs
            for _sec in store_snap.get("sections") or []:
                for _row in _sec.get("tickers") or []:
                    _sym = (_row.get("ticker") or _row.get("symbol") or "").upper().strip()
                    if not _sym:
                        continue
                    _chg = _row.get("change_pct_1d") or _row.get("price_change_pct")
                    _relvol = _row.get("relative_volume")
                    _mc = _row.get("market_cap")
                    _price = _row.get("price")
                    _vol = _row.get("volume")
                    _vol_mc_pct = _row.get("vol_mc_pct")
                    # Only record if we have at least one activity metric
                    if _chg is None and _relvol is None and _vol_mc_pct is None:
                        continue
                    await _rs(
                        "watchlist", "default", _sym,
                        {
                            "price":            _price,
                            "price_change_pct": _chg,
                            "volume":           _vol,
                            "rel_volume":       _relvol,
                            "market_cap":       _mc,
                            "vol_marketcap":    (_vol_mc_pct / 100.0) if _vol_mc_pct is not None else None,
                        }
                    )
        except Exception:
            pass

    _aio.create_task(_watchlist_alert_hook(store))
    return store


@router.post("/{watchlist_id}/analyze")
async def analyze_by_id_endpoint(watchlist_id: str):
    """Run multi-source parallel analysis pipeline for a specific watchlist."""
    agent = _get_agent()
    data_service = _get_data_service()

    store = load_watchlist(watchlist_id)
    if store is None:
        raise HTTPException(status_code=404, detail="Watchlist not found")

    tickers = store.get("tickers", [])
    csv_data = store.get("csv_data", [])

    if not tickers:
        raise HTTPException(status_code=400, detail="Watchlist has no tickers")

    result = await run_analysis_pipeline(tickers, csv_data, agent, data_service)

    if isinstance(result, dict) and result.get("error") and "sections" not in result:
        raise HTTPException(status_code=500, detail=result["error"])

    return result


async def _run_claude_analysis_background(watchlist_id: str) -> None:
    """
    Background task: run full per-group Claude analysis and save to DB.

    Runs AFTER the HTTP response has already been returned to the client.
    Sections use canonical THEME_RS_UNIVERSE names — Claude provides only
    per-ticker insight fields (catalyst, sentiment, action_note, etc.),
    never invents or renames sections.
    """
    try:
        store = load_watchlist(watchlist_id)
        if store is None:
            print(f"[BG_REFRESH] {watchlist_id}: watchlist disappeared — aborting")
            return

        tickers: list  = store.get("tickers", [])
        csv_data: list = store.get("csv_data", [])
        store_name: str = store.get("name", "Watchlist")

        if not tickers:
            return

        csv_map: dict = {}
        for row in csv_data:
            sym = row.get("Symbol") or row.get("symbol") or row.get("Ticker") or ""
            if sym:
                csv_map[sym.strip().upper()] = row

        # ── Classify tickers into canonical theme groups ───────────────────────
        try:
            from services.theme_ticker_mapper import (
                map_ticker_to_primary_theme,
                map_ticker_to_theme_id,
            )
        except Exception as _tm_err:
            print(f"[BG_REFRESH] theme_ticker_mapper unavailable: {_tm_err}")
            map_ticker_to_primary_theme = lambda s: None
            map_ticker_to_theme_id      = lambda s: None

        _OTHER_LABEL = "Other / Uncategorized"
        _OTHER_ID    = "other_uncategorized"

        # Section-name aliases: normalize overlapping/sub-theme names into canonical ones.
        # Claude sometimes invents sub-theme names; these guards ensure consistent grouping.
        _NAME_NORMALIZE: dict[str, str] = {
            # Clean energy consolidation (solar, hydrogen, fuel cells → Clean Energy)
            "Solar":                    "Clean Energy",
            "Renewable Energy":         "Clean Energy",
            "Alternative Energy":       "Clean Energy",
            "Fuel Cell":                "Clean Energy",
            "Fuel Cells":               "Clean Energy",
            "Hydrogen":                 "Clean Energy",
            "Hydrogen Energy":          "Clean Energy",
            "Energy Storage":           "Lithium & Battery Tech",
            # Networking variants
            "Optical Networking":       "AI Networking",
            "Networking":               "AI Networking",
            # Semiconductor variants
            "AI Chips":                 "Semiconductors",
            "Chips":                    "Semiconductors",
            # Slash-style legacy names
            "Memory / Storage":         "Memory & Storage",
            "Robotics / Automation":    "Robotics & Automation",
            "Datacenter / Compute":     "Data Center Infrastructure",
            "Aerospace / Defense":      "Defense",
        }
        _ID_NORMALIZE: dict[str, str] = {
            "solar":                    "clean_energy",
            "renewable_energy":         "clean_energy",
            "alternative_energy":       "clean_energy",
            "fuel_cell":                "clean_energy",
            "fuel_cells":               "clean_energy",
            "hydrogen":                 "clean_energy",
            "hydrogen_energy":          "clean_energy",
            "energy_storage":           "lithium_battery",
            "optical_networking":       "ai_networking",
            "networking":               "ai_networking",
            "ai_chips":                 "semiconductors",
            "chips":                    "semiconductors",
            "memory_/_storage":         "memory_storage",
            "robotics_/_automation":    "robotics_automation",
            "datacenter_/_compute":     "datacenter_infra",
            "aerospace_/_defense":      "defense",
        }

        # Industry fallback — used when primary mapper returns None
        try:
            from services.theme_ticker_mapper import map_industry_to_theme as _bg_map_ind
        except ImportError:
            _bg_map_ind = None

        ticker_to_canon_name: dict[str, str] = {}
        ticker_to_canon_id:   dict[str, str] = {}
        theme_groups: dict[str, list[str]]   = {}

        _stat_registry   = 0  # mapped by theme_ticker_mapper (primary registries)
        _stat_fallback   = 0  # mapped by CSV industry fallback
        _stat_other      = 0  # truly uncategorized
        _stat_aliases    = 0  # section names normalized via _NAME_NORMALIZE
        _stat_overridden = 0  # overridden by manual category override

        # Load manual overrides once — these always win over AI/static classification.
        # Applying them here ensures the SAVED DB sections reflect user corrections,
        # not just the display layer.  Without this, a background refresh would save
        # AI-classified sections to DB, even though the display layer re-applies
        # overrides on every GET.
        _bg_cat_overrides: dict[str, str] = {}
        try:
            from services.category_overrides import get_overrides as _bg_get_overrides
            _bg_cat_overrides = _bg_get_overrides("default")
        except Exception as _bg_ov_err:
            print(f"[BG_REFRESH] category overrides load failed (non-fatal): {_bg_ov_err}")

        for sym in tickers:
            raw_cname = map_ticker_to_primary_theme(sym)
            raw_cid   = map_ticker_to_theme_id(sym)

            # If primary mapper failed, try bare symbol (strips exchange prefix).
            # Example: "AIM:TRT" → try "TRT" — handles foreign-listed stocks where
            # the exchange prefix prevents a direct dict lookup.
            if raw_cname is None and ":" in sym:
                _base = sym.split(":")[-1]
                raw_cname = map_ticker_to_primary_theme(_base)
                raw_cid   = map_ticker_to_theme_id(_base)

            if raw_cname is not None:
                _stat_registry += 1
            elif _bg_map_ind:
                # CSV industry deterministic fallback
                csv_row  = csv_map.get(sym) or csv_map.get(sym.split(":")[-1] if ":" in sym else sym) or {}
                industry = (csv_row.get("Industry") or csv_row.get("industry") or "").strip()
                _ind_res = _bg_map_ind(industry)
                if _ind_res:
                    raw_cname, raw_cid = _ind_res
                    _stat_fallback += 1
                else:
                    raw_cname, raw_cid = _OTHER_LABEL, _OTHER_ID
                    _stat_other += 1
            else:
                raw_cname, raw_cid = _OTHER_LABEL, _OTHER_ID
                _stat_other += 1

            cname = _NAME_NORMALIZE.get(raw_cname, raw_cname)
            cid   = _ID_NORMALIZE.get(raw_cid or "", raw_cid or _OTHER_ID)
            if cname != raw_cname:
                _stat_aliases += 1

            # Manual override wins over everything — checked last so it cannot
            # be undone by any upstream classifier or alias normalization.
            _sym_upper = sym.strip().upper()
            _override_cat = (
                _bg_cat_overrides.get(_sym_upper)
                or _bg_cat_overrides.get(_sym_upper.split(":")[-1] if ":" in _sym_upper else _sym_upper)
            )
            if _override_cat:
                cname = _override_cat
                cid   = (
                    cname.lower()
                    .replace(" ", "_")
                    .replace("/", "_")
                    .replace("&", "and")
                    .replace("-", "_")
                )
                _stat_overridden += 1

            ticker_to_canon_name[sym] = cname
            ticker_to_canon_id[sym]   = cid
            theme_groups.setdefault(cname, []).append(sym)

        sorted_groups: list[tuple[str, list[str]]] = sorted(
            theme_groups.items(),
            key=lambda kv: (kv[0] == _OTHER_LABEL, kv[0]),
        )
        print(
            f"[BG_REFRESH] {watchlist_id}: {len(tickers)} tickers → "
            f"{len(sorted_groups)} canonical theme groups "
            f"(registry={_stat_registry} industry_fallback={_stat_fallback} "
            f"other={_stat_other} aliases={_stat_aliases} overridden={_stat_overridden})"
        )

        anthropic_key = os.getenv("ANTHROPIC_API_KEY", "")
        CHUNK_SIZE = 22

        async def analyze_theme_group(group_name: str, group_tickers: list[str]) -> dict | None:
            """One section per canonical theme; Claude provides per-ticker insights only."""
            all_ticker_rows: list[dict] = []
            group_subtitle = ""

            for chunk_start in range(0, len(group_tickers), CHUNK_SIZE):
                chunk = group_tickers[chunk_start : chunk_start + CHUNK_SIZE]

                ticker_summaries = []
                for sym in chunk:
                    row = csv_map.get(sym.upper(), {})
                    parts = [f"{sym}:"]
                    for label, key in [
                        ("Price",       "Stock Price"),
                        ("MCap",        "Market Cap"),
                        ("PE",          "PE Ratio"),
                        ("FwdPE",       "Forward PE"),
                        ("RSI",         "Relative Strength Index (RSI)"),
                        ("RevGrowth",   "Revenue Growth (YoY)"),
                        ("EPSEst",      "EPS Growth Est."),
                        ("FCF",         "FCF Margin"),
                        ("GrossMargin", "Gross Margin"),
                        ("DE",          "Debt / Equity"),
                    ]:
                        val = row.get(key, "")
                        if val:
                            parts.append(f"{label}={val}")
                    ticker_summaries.append(" ".join(parts))

                # Section title is locked — Claude must NOT rename it
                prompt = (
                    f"Analyze these {len(chunk)} stocks. "
                    f"They all belong to the '{group_name}' theme. "
                    f"Return ONLY a valid JSON object (no markdown fences, no extra text).\n\n"
                    f"Stocks:\n" + "\n".join(ticker_summaries) + "\n\n"
                    f"Return exactly this structure "
                    f"(do NOT change the theme name — it is fixed as '{group_name}'):\n"
                    f'{{\n'
                    f'  "subtitle": "<one-line market context for these {group_name} stocks>",\n'
                    f'  "tickers": [\n'
                    f'    {{\n'
                    f'      "symbol": "<ticker>",\n'
                    f'      "name": "<company name>",\n'
                    f'      "price": <float or null>,\n'
                    f'      "change_pct": <float or null>,\n'
                    f'      "catalyst": "<key near-term catalyst>",\n'
                    f'      "sentiment": "<bullish|neutral|bearish>",\n'
                    f'      "action_note": "<specific actionable note>",\n'
                    f'      "risk_level": "<low|medium|high>",\n'
                    f'      "key_insight": "<single most important insight>",\n'
                    f'      "technical_setup": "<technical pattern or level to watch>"\n'
                    f'    }}\n'
                    f'  ]\n'
                    f'}}'
                )

                try:
                    import anthropic as _anthropic
                    client = _anthropic.AsyncAnthropic(api_key=anthropic_key, timeout=90.0)
                    response = await client.messages.create(
                        model=MODEL_CLAUDE_PREMIUM,
                        max_tokens=4000,
                        messages=[{"role": "user", "content": prompt}],
                    )
                    text = "".join(b.text for b in response.content if hasattr(b, "text"))
                    text = _re.sub(r"```json\s*", "", text)
                    text = _re.sub(r"```\s*", "", text).strip()
                    m_json = _re.search(r"\{[\s\S]*\}", text)
                    if m_json:
                        chunk_parsed = _json.loads(m_json.group())
                        if not group_subtitle:
                            group_subtitle = chunk_parsed.get("subtitle", "")
                        ticker_rows = chunk_parsed.get("tickers", [])
                        for tr in ticker_rows:
                            sym_upper = str(tr.get("symbol", "")).upper()
                            tr["canonical_theme_name"] = group_name
                            tr["canonical_theme_id"]   = ticker_to_canon_id.get(sym_upper, _OTHER_ID)
                            tr["theme_source"]          = "canonical"
                        all_ticker_rows.extend(ticker_rows)
                        print(
                            f"[BG_REFRESH] '{group_name}' chunk {chunk_start}: "
                            f"{len(chunk)} tickers → {len(ticker_rows)} rows"
                        )
                    else:
                        print(f"[BG_REFRESH] '{group_name}' chunk {chunk_start}: no JSON in response")
                except Exception as e:
                    print(f"[BG_REFRESH] '{group_name}' chunk {chunk_start} failed: {type(e).__name__}: {e}")

            if not all_ticker_rows:
                # Fallback: keep skeleton rows so section still appears
                all_ticker_rows = [{"symbol": s} for s in group_tickers]

            canon_id = ticker_to_canon_id.get(group_tickers[0], _OTHER_ID)
            return {
                "id":                 canon_id,
                "title":              group_name,
                "subtitle":           group_subtitle,
                "canonical_theme_id": canon_id,
                "theme_source":       "canonical",
                "tickers":            all_ticker_rows,
            }

        # Run groups 3 at a time — respects Claude rate limits
        semaphore = asyncio.Semaphore(3)

        async def guarded_group(gname: str, gsyms: list[str]) -> dict | None:
            async with semaphore:
                return await analyze_theme_group(gname, gsyms)

        group_results = await asyncio.gather(
            *[guarded_group(n, s) for n, s in sorted_groups],
            return_exceptions=False,
        )
        sections = [r for r in group_results if r is not None]

        market_themes: list[str] = []
        seen_mt: set[str] = set()
        for section in sections:
            title = section.get("title", "")
            if title and title not in seen_mt:
                market_themes.append(title)
                seen_mt.add(title)

        analysis = {
            "sections":               sections,
            "market_themes":          market_themes[:8],
            "generated_at":           datetime.now(timezone.utc).isoformat() + "Z",
            "theme_source":           "canonical",
            "classification_method":  "canonical_theme_registry",
            "_classification_stats": {
                "saved_symbols_count":            len(tickers),
                "categorized_by_registry_count":  _stat_registry,
                "categorized_by_fallback_count":  _stat_fallback,
                "uncategorized_count":            _stat_other,
                "normalized_section_count":       len(sections),
                "section_aliases_applied_count":  _stat_aliases,
            },
        }

        try:
            save_watchlist(csv_data, analysis, watchlist_id=watchlist_id, name=store_name)
            total_enriched = sum(
                len(s.get("tickers", [])) for s in sections
                if s.get("id") != "other_uncategorized"
                   or any(len(t) > 1 for t in s.get("tickers", []))
            )
            print(
                f"[BG_REFRESH] {watchlist_id}: saved {len(sections)} sections, "
                f"{sum(len(s.get('tickers',[])) for s in sections)} tickers"
            )
        except Exception as save_err:
            print(f"[BG_REFRESH] Save failed: {save_err}")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"[BG_REFRESH] {watchlist_id}: top-level error: {e}")


@router.post("/{watchlist_id}/refresh")
async def refresh_by_id_endpoint(watchlist_id: str):
    """
    Trigger a full Claude re-analysis for a watchlist.

    Returns HTTP 200 immediately with the current enriched analysis (Tradier
    quotes + any previously-saved LLM insights).  The Claude per-group analysis
    runs as a background task and saves to the DB when complete — subsequent
    GET calls will show the updated data.

    This design eliminates the previous HTTP timeout that the frontend
    interpreted as "ANALYSIS FAILED: Backend returned 500".  Tradier quote
    data is force-refreshed synchronously before returning so the response
    always has fresh price / 1D-change data.
    """
    try:
        store = load_watchlist(watchlist_id)
        if store is None:
            raise HTTPException(status_code=404, detail="Watchlist not found")

        tickers: list = store.get("tickers", [])
        if not tickers:
            raise HTTPException(status_code=400, detail="Watchlist has no tickers")

        # Kick off the Claude background analysis immediately
        asyncio.create_task(_run_claude_analysis_background(watchlist_id))
        print(f"[REFRESH] {watchlist_id}: background Claude task started for {len(tickers)} tickers")

        # Force-refresh Tradier quotes so the response has live price data
        try:
            from services.watchlist_quote_cache import refresh_watchlist_quotes_now
            await refresh_watchlist_quotes_now(tickers)
        except Exception as _qe:
            print(f"[REFRESH] Tradier quote refresh failed (non-fatal): {_qe}")

        # Return the current analysis enriched with fresh quotes — always HTTP 200
        enriched = await _enrich_store_with_quotes(store)
        enriched["refresh_status"] = "running"
        return enriched

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Refresh failed: {e}")


@router.get("/{watchlist_id}/news/major")
async def news_major_by_id_endpoint(watchlist_id: str):
    """
    Return the top major developments for a watchlist (deduplicated, ranked).
    Uses the shared LKG news cache — instant if data is already warm.

    Response shape:
    {
      "major_developments":       [...top-20 articles...],
      "major_developments_count": int,
      "high_signal_count":        int,
      "by_catalyst_type":         {catalyst_type: count, ...},
      "news_signal_meta":         {...},
      "cached_at":                "ISO timestamp",
      "cache_age_s":              int,
      "is_building":              bool,
    }
    """
    store = load_watchlist(watchlist_id)
    if store is None:
        return {"major_developments": [], "major_developments_count": 0, "cache_age_s": 0}
    tickers = store.get("tickers", [])
    if not tickers:
        return {"major_developments": [], "major_developments_count": 0, "cache_age_s": 0}

    full = await _get_news_for_watchlist(watchlist_id, tickers)
    top  = full.get("top_articles") or []
    return {
        "major_developments":       top,
        "major_developments_count": len(top),
        "high_signal_count":        full.get("high_signal_count", 0),
        "by_catalyst_type":         full.get("by_catalyst_type", {}),
        "news_signal_meta":         full.get("news_signal_meta", {}),
        "cached_at":                full.get("cached_at"),
        "cache_age_s":              full.get("cache_age_s", 0),
        "is_building":              full.get("is_building", False),
    }


@router.get("/{watchlist_id}/news")
async def news_by_id_endpoint(watchlist_id: str):
    """
    Live News for a specific watchlist — LKG cached, never blocks on stale data.

    Response shape:
      {
        "articles":          {TICKER: [article, ...]},   // per-ticker map
        "top_articles":      [...],                       // pre-ranked signal articles (use this)
        "high_signal_count": int,
        "by_catalyst_type":  {type: count},
        "news_signal_meta":  {...},
        "cached_at":         "ISO timestamp",
        "cache_age_s":       int,
        "is_building":       bool,
      }
    """
    store = load_watchlist(watchlist_id)
    if store is None:
        return _news_response({}, {}, _time.time())
    tickers = store.get("tickers", [])
    if not tickers:
        return _news_response({}, {}, _time.time())
    return await _get_news_for_watchlist(watchlist_id, tickers)


@router.get("/{watchlist_id}/stock/{ticker}")
async def stock_detail_by_id_endpoint(watchlist_id: str, ticker: str):
    """Return enriched data for a single ticker within a specific watchlist."""
    agent = _get_agent()
    result = await get_stock_detail(ticker, agent, watchlist_id=watchlist_id)
    if result.get("error"):
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.delete("/{watchlist_id}")
async def delete_by_id_endpoint(watchlist_id: str):
    """Delete a specific watchlist and invalidate earnings cache."""
    result = clear_watchlist(watchlist_id)
    try:
        from services.user_earnings_service import invalidate_user_earnings  # type: ignore
        invalidate_user_earnings("watchlist")
    except Exception as _e:
        print(f"[watchlist-delete-id] earnings invalidation skipped: {_e}")
    return result
