"""
thematic_context_provider.py — shared read-only adapter for thematic/regime/sector context.

UPGRADE (Apr 2025 — v2)
-----------------------
Now functions as an upstream prefilter source, not just a post-fetch annotation layer.

v2 changes:
  A) LKG persistence   — snapshot persisted to disk; loaded on startup before any live cache
  B) Static fallback   — theme registry from THEME_MAP + THEME_ETF_UNIVERSE; active_themes
                         never empty even on full cold start
  C) Prefilter API     — get_thematic_prefilter_universe() returns prioritized ticker universe
  D/E integration     — used by options master screener and briefing precompute

SOURCES REUSED (NO new competing engine)
----------------------------------------
  regime:current_v1              ← core/regime_engine.detect_market_regime write-through
  sr:dashboard:v1                ← sector_rotation background loop (5 min TTL)
  sr:theme_data:v2               ← sector_rotation theme service (background loop)
  data/thematic_context_snapshot.json  ← LKG disk cache written by this module
  data/x_consensus_weekly.json   ← X consensus daily snapshot
  data/sector_rotation_analysis.json   ← Gemini disk fallback
  home_service.THEME_MAP         ← static ticker→theme mapping (10 themes, curated)
  THEME_ETF_UNIVERSE             ← ETF proxy → theme mapping (20 themes)

GUARANTEES
----------
  - Never raises.  Returns best available snapshot.
  - Never calls Claude / Gemini / Grok / Tradier / FMP.
  - Cache TTL: 10 minutes (thematic_context:snapshot:v1).
  - snapshot_status: "fresh" | "stale_lkg" | "fallback_static"
  - active_themes always non-empty (static fallback ensures coverage).
  - Does not modify any existing cache key or agent path.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

_SNAPSHOT_KEY = "thematic_context:snapshot:v1"
_SNAPSHOT_TTL = 10 * 60       # 10 minutes in-memory TTL

_LKG_PATH  = Path(__file__).parent.parent / "data" / "thematic_context_snapshot.json"
_XC_PATH   = Path(__file__).parent.parent / "data" / "x_consensus_weekly.json"
_SR_DISK   = Path(__file__).parent.parent / "data" / "sector_rotation_analysis.json"

_XC_MAX_AGE  = 8 * 24 * 3600   # X consensus usable for 8 days
_LKG_MAX_AGE = 7 * 24 * 3600   # LKG considered stale after 7 days (still usable as fallback)

# ── Static theme registry (built once at import from existing maps) ─────────────
# Provides full ticker coverage even when all live caches are cold.
_STATIC_REGISTRY_BUILT = False
_STATIC_THEMES: list[dict] = []   # [{name, tickers, proxies, sector_tags}]
_STATIC_TICKER_TO_THEMES: dict[str, list[str]] = {}


def _build_static_registry() -> None:
    """Build the static theme registry from THEME_MAP + THEME_ETF_UNIVERSE (once)."""
    global _STATIC_REGISTRY_BUILT, _STATIC_THEMES, _STATIC_TICKER_TO_THEMES
    if _STATIC_REGISTRY_BUILT:
        return
    _STATIC_REGISTRY_BUILT = True

    registry: dict[str, dict] = {}

    # Source 1: home_service.THEME_MAP (curated ticker lists per theme name)
    try:
        from services.home_service import THEME_MAP
        for name, tickers in THEME_MAP.items():
            if name not in registry:
                registry[name] = {"name": name, "tickers": [], "proxies": [], "sector_tags": []}
            for sym in tickers:
                s = sym.upper()
                if s not in registry[name]["tickers"]:
                    registry[name]["tickers"].append(s)
                _STATIC_TICKER_TO_THEMES.setdefault(s, [])
                if name not in _STATIC_TICKER_TO_THEMES[s]:
                    _STATIC_TICKER_TO_THEMES[s].append(name)
    except Exception as e:
        print(f"[THEMATIC_CTX] Static registry: THEME_MAP unavailable: {e}")

    # Source 2: THEME_ETF_UNIVERSE (ETF proxies + representative tickers)
    try:
        from services.sector_rotation.theme_universe import THEME_ETF_UNIVERSE
        for theme_id, meta in THEME_ETF_UNIVERSE.items():
            label  = meta.get("label") or theme_id
            etfs   = [s.upper() for s in (meta.get("symbols") or [])]
            reps   = [s.upper() for s in (meta.get("representative_tickers") or [])]
            parent = meta.get("parent_sector", "")

            if label not in registry:
                registry[label] = {"name": label, "tickers": [], "proxies": [], "sector_tags": []}

            for etf in etfs:
                if etf not in registry[label]["proxies"]:
                    registry[label]["proxies"].append(etf)

            for rep in reps:
                if rep not in registry[label]["tickers"]:
                    registry[label]["tickers"].append(rep)
                _STATIC_TICKER_TO_THEMES.setdefault(rep, [])
                if label not in _STATIC_TICKER_TO_THEMES[rep]:
                    _STATIC_TICKER_TO_THEMES[rep].append(label)

            if parent and parent not in registry[label]["sector_tags"]:
                registry[label]["sector_tags"].append(parent)
    except Exception as e:
        print(f"[THEMATIC_CTX] Static registry: THEME_ETF_UNIVERSE unavailable: {e}")

    _STATIC_THEMES = list(registry.values())
    print(f"[THEMATIC_CTX] Static registry built: {len(_STATIC_THEMES)} themes, "
          f"{len(_STATIC_TICKER_TO_THEMES)} tickers indexed")


def _get_static_theme_entries() -> list[dict]:
    """Return static registry formatted as theme entries (score=0, source=static_registry)."""
    _build_static_registry()
    return [
        {
            "name":             t["name"],
            "score":            0.0,
            "source":           "static_registry",
            "evidence":         ["Static fallback — live theme scores unavailable"],
            "related_etfs":     t["proxies"][:4],
            "related_tickers":  t["tickers"][:10],
        }
        for t in _STATIC_THEMES
        if t.get("tickers") or t.get("proxies")
    ]


# ── LKG disk persistence ───────────────────────────────────────────────────────

def _load_lkg_from_disk() -> Optional[dict]:
    """Load the last-known-good snapshot from disk. Returns None if missing/corrupt."""
    try:
        if not _LKG_PATH.exists():
            return None
        raw = json.loads(_LKG_PATH.read_text())
        if not isinstance(raw, dict):
            return None
        saved_at = float(raw.get("_saved_at", 0))
        if time.time() - saved_at > _LKG_MAX_AGE:
            return None   # Too old (7 days) — don't use as fallback
        return raw
    except Exception:
        return None


def _save_lkg_to_disk(snap: dict) -> None:
    """
    Persist snapshot to disk.
    Only saves if active_themes OR emerging_themes is non-empty
    AND the source is NOT purely static_registry (don't overwrite good LKG with cold data).
    """
    try:
        active   = snap.get("active_themes", [])
        emerging = snap.get("emerging_themes", [])

        # If all themes are static_registry, don't overwrite a potentially better LKG
        all_static = all(t.get("source") == "static_registry" for t in active + emerging)
        if all_static and _LKG_PATH.exists():
            return   # Keep existing LKG

        if not active and not emerging:
            return   # Nothing useful to save

        snap_to_save = dict(snap)
        snap_to_save["_saved_at"] = time.time()
        _LKG_PATH.write_text(json.dumps(snap_to_save, indent=2))
    except Exception as e:
        print(f"[THEMATIC_CTX] LKG save error (non-fatal): {e}")


def load_lkg_into_cache() -> None:
    """
    Load LKG from disk into the in-memory cache.
    Called on startup before background loops warm up.
    Marks loaded snap with snapshot_status="stale_lkg".
    """
    try:
        from data.cache import cache
        if cache.get(_SNAPSHOT_KEY):
            return   # Already warm
        lkg = _load_lkg_from_disk()
        if lkg and (lkg.get("active_themes") or lkg.get("emerging_themes")):
            saved_at   = float(lkg.get("_saved_at", time.time()))
            age_min    = round((time.time() - saved_at) / 60)
            lkg["snapshot_status"]       = "stale_lkg"
            lkg["snapshot_age_minutes"]  = age_min
            cache.set(_SNAPSHOT_KEY, lkg, _SNAPSHOT_TTL)
            print(f"[THEMATIC_CTX] LKG loaded from disk (age={age_min}m, "
                  f"active_themes={len(lkg.get('active_themes', []))})")
    except Exception as e:
        print(f"[THEMATIC_CTX] load_lkg_into_cache error: {e}")


# ── Public API ────────────────────────────────────────────────────────────────

def get_shared_thematic_context(force_refresh: bool = False) -> dict:
    """
    Return current thematic/regime/sector snapshot.

    Read order (fastest first):
      1. In-memory cache (thematic_context:snapshot:v1) — ~0ms
      2. Rebuild from all live caches — cache values from background loops
      3. If active_themes still empty: use disk LKG themes (marked stale_lkg)
      4. If still empty: use static registry (marked fallback_static)

    Always non-empty: active_themes is guaranteed to have entries via fallback.

    Output shape:
    {
      "macro_regime":         str | None,
      "active_themes":        [{name, score, source, evidence, related_etfs, related_tickers}],
      "emerging_themes":      [...],
      "dead_zones":           [...],
      "sector_leaders":       [{sector, ticker, score, posture}],
      "sector_laggards":      [...],
      "risk_notes":           [str],
      "last_updated":         ISO 8601 str,
      "snapshot_status":      "fresh" | "stale_lkg" | "fallback_static",
      "snapshot_age_minutes": int,
      "source_health":        {regime, sector_rotation, theme_rotation, x_consensus, polymarket}
    }
    """
    try:
        from data.cache import cache

        # ── Fast path: in-memory cache ────────────────────────────────────────
        if not force_refresh:
            cached = cache.get(_SNAPSHOT_KEY)
            if cached and isinstance(cached, dict):
                return cached

        # ── Rebuild ───────────────────────────────────────────────────────────
        lkg_on_disk = _load_lkg_from_disk()
        snap = _build_snapshot(lkg_on_disk=lkg_on_disk)

        # ── Persist non-static snapshots to disk ──────────────────────────────
        _save_lkg_to_disk(snap)

        cache.set(_SNAPSHOT_KEY, snap, _SNAPSHOT_TTL)
        return snap

    except Exception as exc:
        print(f"[THEMATIC_CTX] get_shared_thematic_context error: {exc}")
        return _empty_snapshot()


def get_thematic_prefilter_universe(
    include_active: bool = True,
    include_emerging: bool = True,
    include_watchlist: bool = True,
    include_megacap_fallback: bool = True,
    max_tickers: int = 150,
) -> dict:
    """
    Return a prioritized ticker universe for pre-scan use.

    Use this before expensive API fetches (Tradier options chains, TA scans)
    to focus on regime-aligned candidates first.

    Priority order (highest to lowest):
      1. Active theme representative tickers + ETFs
      2. Emerging theme representative tickers + ETFs
      3. Watchlist tickers (from cache if available)
      4. Megacap fallback (always-liquid names)

    Dead zone tickers are returned separately — callers can deprioritize them.

    Returns:
    {
      "tickers":               [str],   # ordered, deduplicated, max_tickers
      "active_theme_tickers":  [str],
      "emerging_theme_tickers":[str],
      "dead_zone_tickers":     [str],
      "watchlist_tickers":     [str],
      "theme_map":             {sym: {theme_name, theme_state, regime_alignment_score}},
      "snapshot_status":       "fresh" | "stale_lkg" | "fallback_static",
      "source_health":         {...}
    }
    """
    try:
        snap         = get_shared_thematic_context()
        active_th    = snap.get("active_themes", [])
        emerging_th  = snap.get("emerging_themes", [])
        dead_th      = snap.get("dead_zones", [])

        active_ticks: list[str]   = []
        emerging_ticks: list[str] = []
        dead_ticks: list[str]     = []
        theme_map: dict           = {}

        def _add_tickers(theme_list: list[dict], bucket: list[str], state: str, base_score: float) -> None:
            for t in theme_list:
                name = t.get("name", "")
                # Representative tickers first (most specific)
                for sym in (t.get("related_tickers") or []):
                    s = sym.upper()
                    if s not in bucket:
                        bucket.append(s)
                    theme_map.setdefault(s, {
                        "theme_name":             name,
                        "theme_state":            state,
                        "regime_alignment_score": base_score,
                    })
                # ETF proxies (broader)
                for etf in (t.get("related_etfs") or []):
                    e = etf.upper()
                    if e not in bucket:
                        bucket.append(e)
                    theme_map.setdefault(e, {
                        "theme_name":             name,
                        "theme_state":            state,
                        "regime_alignment_score": base_score * 0.7,
                    })

        if include_active:
            _add_tickers(active_th, active_ticks, "active", 0.8)
        if include_emerging:
            _add_tickers(emerging_th, emerging_ticks, "emerging", 0.5)
        _add_tickers(dead_th, dead_ticks, "dead_zone", -0.2)

        # Also add static registry tickers that aren't already included
        _build_static_registry()
        for sym, themes in _STATIC_TICKER_TO_THEMES.items():
            if sym not in theme_map:
                primary = themes[0] if themes else None
                if primary:
                    theme_map[sym] = {
                        "theme_name":             primary,
                        "theme_state":            "neutral",
                        "regime_alignment_score": 0.0,
                    }

        # Watchlist
        watchlist_ticks: list[str] = []
        if include_watchlist:
            try:
                from data.cache import cache
                for wl_key in ("user:watchlist:default", "watchlist:cached_v1"):
                    wl = cache.get(wl_key)
                    if wl and isinstance(wl, list):
                        for w in wl:
                            if isinstance(w, dict) and w.get("ticker"):
                                t = w["ticker"].upper()
                                if t not in watchlist_ticks:
                                    watchlist_ticks.append(t)
                        break
            except Exception:
                pass

        # Megacap fallback (always present for liquidity)
        _MEGACAP_FALLBACK = [
            "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA",
            "AVGO", "LLY", "JPM", "V", "UNH", "XOM", "MA", "JNJ",
        ]
        megacap = _MEGACAP_FALLBACK if include_megacap_fallback else []

        # Build ordered, deduplicated universe
        ordered = list(dict.fromkeys([
            *active_ticks,
            *emerging_ticks,
            *watchlist_ticks,
            *megacap,
        ]))
        ordered = ordered[:max_tickers]

        return {
            "tickers":                ordered,
            "active_theme_tickers":   active_ticks[:60],
            "emerging_theme_tickers": emerging_ticks[:40],
            "dead_zone_tickers":      dead_ticks[:20],
            "watchlist_tickers":      watchlist_ticks[:20],
            "theme_map":              theme_map,
            "snapshot_status":        snap.get("snapshot_status", "unknown"),
            "source_health":          snap.get("source_health", {}),
        }
    except Exception as exc:
        print(f"[THEMATIC_CTX] get_thematic_prefilter_universe error: {exc}")
        return {
            "tickers": [],
            "active_theme_tickers": [],
            "emerging_theme_tickers": [],
            "dead_zone_tickers": [],
            "watchlist_tickers": [],
            "theme_map": {},
            "snapshot_status": "error",
            "source_health": {},
        }


async def warmup_thematic_context() -> None:
    """
    Startup warmup coroutine.
    1. Load LKG from disk immediately (synchronous, fast).
    2. Rebuild from available caches (no API calls).
    3. Build static registry index.

    Call from lifespan handler so the snapshot is available before
    the first request arrives.
    """
    import asyncio
    try:
        print("[THEMATIC_CTX] Startup warmup: loading LKG...")
        _build_static_registry()
        load_lkg_into_cache()

        # Small delay so sector rotation background loop has a chance to warm up
        await asyncio.sleep(5)

        # Force-rebuild from whatever caches are available now
        print("[THEMATIC_CTX] Startup warmup: building fresh snapshot...")
        snap = get_shared_thematic_context(force_refresh=True)
        active_count   = len(snap.get("active_themes", []))
        emerging_count = len(snap.get("emerging_themes", []))
        status         = snap.get("snapshot_status", "?")
        print(f"[THEMATIC_CTX] Warmup complete: status={status} "
              f"active={active_count} emerging={emerging_count} "
              f"macro={snap.get('macro_regime')}")
    except Exception as e:
        print(f"[THEMATIC_CTX] warmup error (non-fatal): {e}")


# ── Internal builder ──────────────────────────────────────────────────────────

def _build_snapshot(lkg_on_disk: Optional[dict] = None) -> dict:
    """
    Assemble snapshot from all available cache sources.

    Theme data priority chain:
      1. sr:theme_data:v2 (live ETF RS scores)       → snapshot_status: "fresh"
      2. X consensus theme enrichment (in-memory)    → boosts above or adds new themes
      3. Disk LKG themes (thematic_context_snapshot) → snapshot_status: "stale_lkg"
      4. Static registry (THEME_MAP + ETF_UNIVERSE)  → snapshot_status: "fallback_static"

    Never raises.
    """
    health: dict[str, str] = {
        "regime":          "fallback",
        "sector_rotation": "fallback",
        "theme_rotation":  "fallback",
        "x_consensus":     "fallback",
        "polymarket":      "fallback",
    }
    macro_regime:    Optional[str] = None
    active_themes:   list          = []
    emerging_themes: list          = []
    dead_zones:      list          = []
    sector_leaders:  list          = []
    sector_laggards: list          = []
    risk_notes:      list          = []
    snapshot_status: str           = "fallback_static"
    saved_at_for_age: float        = time.time()

    try:
        from data.cache import cache

        # ── 1. Macro regime ───────────────────────────────────────────────────
        regime_snap = cache.get("regime:current_v1")
        if regime_snap and isinstance(regime_snap, dict) and regime_snap.get("regime"):
            macro_regime = regime_snap["regime"]
            conf         = float(regime_snap.get("confidence") or 0)
            if conf < 0.4:
                risk_notes.append(
                    f"Regime confidence low ({conf:.0%}); signals mixed"
                )
            health["regime"] = "ok"
        else:
            macro_regime = _regime_from_disk()
            if macro_regime:
                health["regime"] = "fallback"

        # ── 2. Sector leaders / laggards (sr:dashboard:v1) ────────────────────
        sr_dash = cache.get("sr:dashboard:v1")
        if sr_dash and isinstance(sr_dash, dict):
            for s in (sr_dash.get("sectors") or []):
                if not isinstance(s, dict):
                    continue
                ticker = (s.get("ticker") or "").upper()
                name   = s.get("name") or ticker
                score  = float(s.get("rotation_score") or 0)
                tag    = (s.get("regime_tag") or "").strip()
                entry  = {
                    "sector":  name,
                    "ticker":  ticker,
                    "score":   round(score, 2),
                    "posture": _trend_to_posture(tag),
                }
                if tag in ("Leading", "Improving"):
                    sector_leaders.append(entry)
                elif tag in ("Weakening", "Lagging"):
                    sector_laggards.append(entry)
            health["sector_rotation"] = "ok" if sr_dash.get("sectors") else "fallback"
        else:
            if _SR_DISK.exists():
                try:
                    sr_disk = json.loads(_SR_DISK.read_text())
                    _leaders_from_disk(sr_disk, sector_leaders, sector_laggards)
                    if sector_leaders or sector_laggards:
                        health["sector_rotation"] = "fallback"
                except Exception:
                    pass

        # ── 3. Theme ETF RS scores → active / emerging / dead_zones ──────────
        theme_data = cache.get("sr:theme_data:v2")
        if theme_data and isinstance(theme_data, dict):
            raw_themes = theme_data.get("themes") or []
            if not raw_themes and isinstance(theme_data, list):
                raw_themes = theme_data
            for t in raw_themes:
                if not isinstance(t, dict):
                    continue
                name     = (t.get("label") or t.get("name") or t.get("theme_id") or "").strip()
                rs_score = float(t.get("rs_score") or 0)
                trend    = (t.get("trend_state") or "").strip()
                rotation = (t.get("rotation_state") or "").strip()
                etfs     = t.get("symbols") or t.get("etfs") or []
                reps     = t.get("representative_tickers") or []

                evidence = []
                if trend:
                    evidence.append(f"Trend: {trend}")
                if rotation:
                    evidence.append(f"Rotation: {rotation}")

                entry = {
                    "name":             name,
                    "score":            round(rs_score, 1),
                    "source":           "sector_rotation_etf",
                    "evidence":         evidence,
                    "related_etfs":     etfs if isinstance(etfs, list) else [etfs],
                    "related_tickers":  reps if isinstance(reps, list) else [],
                }
                if trend in ("Leadership", "Improving"):
                    active_themes.append(entry)
                elif trend == "Neutral" and rotation == "Accelerating":
                    emerging_themes.append(entry)
                elif trend in ("Weakening", "Lagging"):
                    dead_zones.append(entry)

            active_themes.sort(key=lambda x: -x["score"])
            emerging_themes.sort(key=lambda x: -x["score"])
            dead_zones.sort(key=lambda x: x["score"])
            health["theme_rotation"] = "ok" if raw_themes else "fallback"
            if active_themes or emerging_themes:
                snapshot_status = "fresh"

        # ── 4. X consensus → theme enrichment ────────────────────────────────
        if _XC_PATH.exists():
            try:
                xc  = json.loads(_XC_PATH.read_text())
                age = time.time() - float(xc.get("_saved_at") or 0)
                if age < _XC_MAX_AGE:
                    _enrich_from_x_consensus(xc, active_themes, emerging_themes, macro_regime)
                    health["x_consensus"] = "ok"
                    if emerging_themes and snapshot_status == "fallback_static":
                        snapshot_status = "fresh"   # x_consensus provides real data
                else:
                    health["x_consensus"] = "stale"
            except Exception as xc_err:
                print(f"[THEMATIC_CTX] x_consensus read error: {xc_err}")
                health["x_consensus"] = "error"

        # ── 5. LKG disk fallback if themes still empty ────────────────────────
        if not active_themes and not emerging_themes:
            lkg = lkg_on_disk or _load_lkg_from_disk()
            if lkg and isinstance(lkg, dict):
                lkg_active   = lkg.get("active_themes", [])
                lkg_emerging = lkg.get("emerging_themes", [])
                lkg_dead     = lkg.get("dead_zones", [])
                if lkg_active or lkg_emerging:
                    active_themes   = [dict(t, source="lkg") for t in lkg_active]
                    emerging_themes = [dict(t, source="lkg") for t in lkg_emerging]
                    dead_zones      = dead_zones or [dict(t, source="lkg") for t in lkg_dead]
                    snapshot_status = "stale_lkg"
                    saved_at_for_age = float(lkg.get("_saved_at", time.time()))
                    health["theme_rotation"] = "fallback"

        # ── 6. Static registry fallback if STILL empty ───────────────────────
        if not active_themes and not emerging_themes:
            _build_static_registry()
            static = _get_static_theme_entries()
            # Put first 5 themes in active, rest in emerging (arbitrary split)
            active_themes   = static[:5]
            emerging_themes = static[5:10]
            snapshot_status = "fallback_static"
            health["theme_rotation"] = "static"
            risk_notes.append(
                "Active themes from static registry — live scores unavailable. "
                "Tickers present but theme strength unknown."
            )

    except Exception as exc:
        print(f"[THEMATIC_CTX] _build_snapshot error: {exc}")
        risk_notes.append(f"Snapshot build error: {type(exc).__name__}")

    age_minutes = round((time.time() - saved_at_for_age) / 60) if snapshot_status != "fresh" else 0

    return {
        "macro_regime":         macro_regime,
        "active_themes":        active_themes[:8],
        "emerging_themes":      emerging_themes[:5],
        "dead_zones":           dead_zones[:5],
        "sector_leaders":       sector_leaders[:5],
        "sector_laggards":      sector_laggards[:5],
        "risk_notes":           risk_notes,
        "last_updated":         datetime.now(timezone.utc).isoformat(),
        "snapshot_status":      snapshot_status,
        "snapshot_age_minutes": age_minutes,
        "source_health":        health,
    }


# ── X consensus enrichment ────────────────────────────────────────────────────

def _enrich_from_x_consensus(xc: dict,
                              active_themes: list,
                              emerging_themes: list,
                              macro_regime: Optional[str]) -> None:
    """
    Map x_consensus top_tickers → themes.
    Boosts existing theme scores; adds new x_consensus-only themes.
    Pure in-memory dict lookups — no network calls.
    """
    try:
        _build_static_registry()

        top_tickers = xc.get("top_tickers") or []
        if not top_tickers:
            return

        theme_mention_count: dict[str, int] = {}
        theme_evidence: dict[str, list[str]] = {}
        for row in top_tickers[:30]:
            sym = (row.get("symbol") or "").upper()
            if not sym:
                continue
            for theme in _STATIC_TICKER_TO_THEMES.get(sym, []):
                theme_mention_count[theme] = theme_mention_count.get(theme, 0) + 1
                theme_evidence.setdefault(theme, []).append(sym)

        if not theme_mention_count:
            return

        active_names   = {t["name"] for t in active_themes}
        emerging_names = {t["name"] for t in emerging_themes}

        for t in active_themes:
            cnt = theme_mention_count.get(t["name"], 0)
            if cnt > 0:
                t["score"] = round(t["score"] + cnt * 0.5, 1)
                t.setdefault("evidence", []).append(
                    f"X consensus: {cnt} ticker{'s' if cnt > 1 else ''} mentioned"
                )
        for t in emerging_themes:
            cnt = theme_mention_count.get(t["name"], 0)
            if cnt > 0:
                t["score"] = round(t["score"] + cnt * 0.25, 1)
                t.setdefault("evidence", []).append(
                    f"X consensus: {cnt} ticker{'s' if cnt > 1 else ''} mentioned"
                )

        # Add new X-consensus-only themes
        for theme, cnt in sorted(theme_mention_count.items(), key=lambda x: -x[1]):
            if cnt < 2 or theme in active_names or theme in emerging_names:
                continue
            # Get tickers from static registry
            st = next((s for s in _STATIC_THEMES if s["name"] == theme), {})
            entry = {
                "name":            theme,
                "score":           round(cnt * 1.5, 1),
                "source":          "x_consensus",
                "evidence":        [f"X consensus: {cnt} tickers — {', '.join(theme_evidence.get(theme, [])[:4])}"],
                "related_etfs":    st.get("proxies", [])[:3],
                "related_tickers": theme_evidence.get(theme, [])[:5],
            }
            emerging_themes.append(entry)

        emerging_themes.sort(key=lambda x: -x["score"])
    except Exception as e:
        print(f"[THEMATIC_CTX] _enrich_from_x_consensus error: {e}")


# ── Disk fallbacks ────────────────────────────────────────────────────────────

def _regime_from_disk() -> Optional[str]:
    if not _SR_DISK.exists():
        return None
    try:
        sr = json.loads(_SR_DISK.read_text())
        raw = sr.get("macro_regime") or sr.get("market_regime") or ""
        return _normalize_disk_regime(raw)
    except Exception:
        return None


def _normalize_disk_regime(raw: str) -> Optional[str]:
    if not raw:
        return None
    r = raw.lower()
    if any(w in r for w in ("risk on", "risk-on", "bull", "growth")):
        return "risk_on"
    if any(w in r for w in ("risk off", "risk-off", "bear", "defensive", "flight to safety")):
        return "risk_off"
    if any(w in r for w in ("inflat", "stagflat")):
        return "inflationary"
    if any(w in r for w in ("neutral", "mixed", "transitional")):
        return "neutral"
    return raw[:40] if raw else None


def _leaders_from_disk(sr: dict, leaders: list, laggards: list) -> None:
    leadership = sr.get("current_leadership") or ""
    if leadership:
        for chunk in leadership.replace(",", ";").split(";"):
            chunk = chunk.strip()
            if not chunk:
                continue
            toks = chunk.split()
            for tok in toks:
                tok = tok.upper().strip("()")
                if 2 < len(tok) <= 5 and tok.isalpha():
                    leaders.append({
                        "sector":  tok,
                        "ticker":  tok,
                        "score":   0.0,
                        "posture": "leading",
                    })


def _trend_to_posture(trend: str) -> str:
    mapping = {
        "Leading":   "leading",
        "Improving": "improving",
        "Neutral":   "neutral",
        "Weakening": "weakening",
        "Lagging":   "lagging",
        "Unknown":   "neutral",
    }
    return mapping.get(trend, trend.lower() if trend else "neutral")


def _empty_snapshot() -> dict:
    _build_static_registry()
    static = _get_static_theme_entries()
    return {
        "macro_regime":         None,
        "active_themes":        static[:5],
        "emerging_themes":      static[5:10],
        "dead_zones":           [],
        "sector_leaders":       [],
        "sector_laggards":      [],
        "risk_notes":           ["Snapshot build failed — using static fallback"],
        "last_updated":         datetime.now(timezone.utc).isoformat(),
        "snapshot_status":      "fallback_static",
        "snapshot_age_minutes": 0,
        "source_health": {
            "regime":          "error",
            "sector_rotation": "error",
            "theme_rotation":  "static",
            "x_consensus":     "error",
            "polymarket":      "fallback",
        },
    }
