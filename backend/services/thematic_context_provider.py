"""
thematic_context_provider.py — shared read-only adapter for thematic/regime/sector context.

PURPOSE
-------
Expose a single get_shared_thematic_context() function that returns a normalized
snapshot of macro regime, sector leaders/laggards, and active/emerging/dead-zone
themes — reusable by any endpoint without triggering expensive LLM or API calls.

SOURCES REUSED (audit cross-reference — NO new competing engine)
----------------------------------------------------------------
  regime:current_v1         ← core/regime_engine.detect_market_regime  (write-through added Apr 2025)
  sr:dashboard:v1           ← services/sector_rotation/service.py background loop (5 min TTL)
  sr:theme_data:v2          ← services/sector_rotation/theme_service.py (populated by background loop)
  notifai_weekly_summary_v2 ← same key as agent/context_broker.read_shared_context()
  fred:quick_macro          ← data/fred_provider.py background loop
  data/x_consensus_weekly.json   ← services/x_consensus_cache.py daily snapshot
  data/sector_rotation_analysis.json ← services/sector_rotation/gemini_analysis.py (disk fallback)

GUARANTEES
----------
  - Never raises.  Returns fallback snapshot on any error.
  - Never calls Claude / Gemini / Grok / Tradier / FMP during get_snapshot().
  - Cache TTL: 10 minutes (thematic_context:snapshot:v1).
  - All sources optional: missing cache → source_health="fallback", not 500.
  - Does not modify any existing cache key or agent path.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

_SNAPSHOT_KEY = "thematic_context:snapshot:v1"
_SNAPSHOT_TTL = 10 * 60  # 10 minutes

_XC_PATH  = Path(__file__).parent.parent / "data" / "x_consensus_weekly.json"
_SR_DISK  = Path(__file__).parent.parent / "data" / "sector_rotation_analysis.json"

# Max age for X consensus disk snapshot to be considered usable (8 days)
_XC_MAX_AGE = 8 * 24 * 3600


# ── Public API ────────────────────────────────────────────────────────────────

def get_shared_thematic_context(force_refresh: bool = False) -> dict:
    """
    Return current thematic/regime/sector snapshot.

    Normal read: serves from 10-min cache, then falls back to rebuilding from
    all available caches. Graceful fallback for every missing source.

    Force refresh: rebuilds from caches immediately (no API calls).

    Output shape:
    {
      "macro_regime":    str | None,
      "active_themes":   [{name, score, source, evidence, related_etfs, related_tickers}],
      "emerging_themes": [...],
      "dead_zones":      [...],
      "sector_leaders":  [{sector, ticker, score, posture}],
      "sector_laggards": [...],
      "risk_notes":      [str],
      "last_updated":    str (ISO 8601),
      "source_health":   {regime, sector_rotation, theme_rotation, x_consensus, polymarket}
    }
    """
    try:
        from data.cache import cache
        if not force_refresh:
            cached = cache.get(_SNAPSHOT_KEY)
            if cached and isinstance(cached, dict) and cached.get("macro_regime") is not None:
                return cached
        snap = _build_snapshot()
        cache.set(_SNAPSHOT_KEY, snap, _SNAPSHOT_TTL)
        return snap
    except Exception as exc:
        print(f"[THEMATIC_CTX] get_shared_thematic_context error: {exc}")
        return _empty_snapshot()


# ── Internal builder ──────────────────────────────────────────────────────────

def _build_snapshot() -> dict:
    """Assemble snapshot from all available cache sources. Never raises."""
    health: dict[str, str] = {
        "regime":          "fallback",
        "sector_rotation": "fallback",
        "theme_rotation":  "fallback",
        "x_consensus":     "fallback",
        "polymarket":      "fallback",   # not in current main-agent path; no cheap cache yet
    }
    macro_regime:    Optional[str] = None
    active_themes:   list          = []
    emerging_themes: list          = []
    dead_zones:      list          = []
    sector_leaders:  list          = []
    sector_laggards: list          = []
    risk_notes:      list          = []

    try:
        from data.cache import cache

        # ── 1. Macro regime ───────────────────────────────────────────────────
        regime_snap = cache.get("regime:current_v1")
        if regime_snap and isinstance(regime_snap, dict) and regime_snap.get("regime"):
            macro_regime = regime_snap["regime"]
            conf         = float(regime_snap.get("confidence") or 0)
            if conf < 0.4:
                risk_notes.append(
                    f"Regime confidence low ({conf:.0%}); signals mixed — treat regime label with caution"
                )
            health["regime"] = "ok"
        else:
            # Disk fallback: sector_rotation_analysis.json written by Gemini analysis loop
            macro_regime = _regime_from_disk()
            if macro_regime:
                health["regime"] = "fallback"

        # ── 2. Sector leaders / laggards (sr:dashboard:v1) ────────────────────
        sr_dash = cache.get("sr:dashboard:v1")
        if sr_dash and isinstance(sr_dash, dict):
            for s in (sr_dash.get("sectors") or []):
                if not isinstance(s, dict):
                    continue
                # SectorSnapshot uses 'ticker' field (not 'etf')
                ticker = (s.get("ticker") or "").upper()
                name   = s.get("name") or ticker
                score  = float(s.get("rotation_score") or 0)
                # regime_tag values: "Leading"|"Improving"|"Weakening"|"Lagging"|"Unknown"
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
            # Disk fallback from sector_rotation_analysis.json
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
            # Some versions store themes as top-level list
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
            dead_zones.sort(key=lambda x: x["score"])   # worst first
            health["theme_rotation"] = "ok" if raw_themes else "fallback"

        # ── 4. X consensus top tickers → theme boost ─────────────────────────
        if _XC_PATH.exists():
            try:
                xc  = json.loads(_XC_PATH.read_text())
                age = time.time() - float(xc.get("_saved_at") or 0)
                if age < _XC_MAX_AGE:
                    _enrich_from_x_consensus(
                        xc, active_themes, emerging_themes, macro_regime
                    )
                    health["x_consensus"] = "ok"
                else:
                    health["x_consensus"] = "stale"
            except Exception as xc_err:
                print(f"[THEMATIC_CTX] x_consensus read error: {xc_err}")
                health["x_consensus"] = "error"

    except Exception as exc:
        print(f"[THEMATIC_CTX] _build_snapshot error: {exc}")
        risk_notes.append(f"Snapshot build error: {type(exc).__name__}")

    return {
        "macro_regime":    macro_regime,
        "active_themes":   active_themes[:8],
        "emerging_themes": emerging_themes[:5],
        "dead_zones":      dead_zones[:5],
        "sector_leaders":  sector_leaders[:5],
        "sector_laggards": sector_laggards[:5],
        "risk_notes":      risk_notes,
        "last_updated":    datetime.now(timezone.utc).isoformat(),
        "source_health":   health,
    }


# ── X consensus enrichment ────────────────────────────────────────────────────

def _enrich_from_x_consensus(xc: dict,
                              active_themes: list,
                              emerging_themes: list,
                              macro_regime: Optional[str]) -> None:
    """
    Map x_consensus top_tickers → themes via theme_ticker_mapper.
    Boosts existing theme scores with X social signal count.
    Adds new themes supported by 2+ X mentions not already covered.
    Pure in-memory dict lookups — no network calls.
    """
    try:
        from services.theme_ticker_mapper import map_ticker_to_themes, get_theme_meta

        top_tickers = xc.get("top_tickers") or []
        if not top_tickers:
            return

        # Count X mentions per theme
        theme_mention_count: dict[str, int] = {}
        theme_evidence: dict[str, list[str]] = {}
        for row in top_tickers[:30]:
            sym = (row.get("symbol") or "").upper()
            if not sym:
                continue
            for theme in map_ticker_to_themes(sym):
                theme_mention_count[theme] = theme_mention_count.get(theme, 0) + 1
                theme_evidence.setdefault(theme, []).append(sym)

        if not theme_mention_count:
            return

        # Existing theme name sets
        active_names   = {t["name"] for t in active_themes}
        emerging_names = {t["name"] for t in emerging_themes}

        # Boost existing themes with X signal
        for t in active_themes:
            cnt = theme_mention_count.get(t["name"], 0)
            if cnt > 0:
                t["score"] = round(t["score"] + cnt * 0.5, 1)
                t.setdefault("evidence", []).append(
                    f"X consensus: {cnt} tick{'er' if cnt == 1 else 'ers'} mentioned"
                )
        for t in emerging_themes:
            cnt = theme_mention_count.get(t["name"], 0)
            if cnt > 0:
                t["score"] = round(t["score"] + cnt * 0.25, 1)
                t.setdefault("evidence", []).append(
                    f"X consensus: {cnt} ticker{'s' if cnt > 1 else ''} mentioned"
                )

        # Add new X-consensus-only themes if 2+ mentions and not already in list
        for theme, cnt in sorted(theme_mention_count.items(), key=lambda x: -x[1]):
            if cnt < 2:
                continue
            if theme in active_names or theme in emerging_names:
                continue
            meta = get_theme_meta(theme)
            entry = {
                "name":            theme,
                "score":           round(cnt * 1.5, 1),
                "source":          "x_consensus",
                "evidence":        [f"X consensus: {cnt} tickers — {', '.join(theme_evidence.get(theme, [])[:4])}"],
                "related_etfs":    meta.get("etfs", [])[:3],
                "related_tickers": theme_evidence.get(theme, [])[:5],
            }
            emerging_themes.append(entry)

        emerging_themes.sort(key=lambda x: -x["score"])
    except Exception as e:
        print(f"[THEMATIC_CTX] _enrich_from_x_consensus error: {e}")


# ── Disk fallbacks ────────────────────────────────────────────────────────────

def _regime_from_disk() -> Optional[str]:
    """Extract macro regime from sector_rotation_analysis.json disk cache."""
    if not _SR_DISK.exists():
        return None
    try:
        sr = json.loads(_SR_DISK.read_text())
        raw = sr.get("macro_regime") or sr.get("market_regime") or ""
        return _normalize_disk_regime(raw)
    except Exception:
        return None


def _normalize_disk_regime(raw: str) -> Optional[str]:
    """Map free-text regime descriptions to canonical tags."""
    if not raw:
        return None
    r = raw.lower()
    if any(w in r for w in ("risk on", "risk-on", "bull", "growth")):
        return "risk_on"
    if any(w in r for w in ("risk off", "risk-off", "bear", "defensive", "flight to safety")):
        return "risk_off"
    if any(w in r for w in ("inflat", "stagflat")):
        return "inflationary"
    # Return first meaningful word as-is if recognizable
    if any(w in r for w in ("neutral", "mixed", "transitional")):
        return "neutral"
    # Pass through the raw value (truncated) so it's not lost
    return raw[:40] if raw else None


def _leaders_from_disk(sr: dict, leaders: list, laggards: list) -> None:
    """
    Extract sector leaders/laggards from sector_rotation_analysis.json.
    The disk format uses free-text current_leadership and leadership_style fields.
    """
    leadership = sr.get("current_leadership") or ""
    leadership_style = sr.get("leadership_style") or ""

    if leadership:
        for chunk in leadership.replace(",", ";").split(";"):
            chunk = chunk.strip()
            if not chunk:
                continue
            # The disk format typically lists ETF symbols like "XLE, XLU dominating"
            toks = chunk.split()
            for tok in toks:
                tok = tok.upper().strip("()")
                if 2 < len(tok) <= 5 and tok.isalpha():
                    leaders.append({
                        "sector": tok,
                        "ticker": tok,
                        "score":  0.0,
                        "posture": "leading",
                    })


def _trend_to_posture(trend: str) -> str:
    """Map SectorSnapshot.regime_tag to posture label."""
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
    return {
        "macro_regime":    None,
        "active_themes":   [],
        "emerging_themes": [],
        "dead_zones":      [],
        "sector_leaders":  [],
        "sector_laggards": [],
        "risk_notes":      ["Snapshot build failed — all sources unavailable"],
        "last_updated":    datetime.now(timezone.utc).isoformat(),
        "source_health": {
            "regime":          "error",
            "sector_rotation": "error",
            "theme_rotation":  "error",
            "x_consensus":     "error",
            "polymarket":      "fallback",
        },
    }
