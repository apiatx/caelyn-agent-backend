"""
Top Catalysts aggregation — read-only, lightweight.

Aggregates already-cached calendar data into a single weekly "Top Catalysts"
view. Reads ONLY:
  • The 5 calendar snapshot tabs persisted in Neon/disk via
    calendar_snapshot_service (dividends, ipos, splits, economic_releases,
    treasury_macro).
  • The in-memory earnings cache populated by services.earnings_clean_service
    (key: ``earnings:curated:week:{from}:{to}``). Read-only — if the entry
    is missing, earnings are simply absent from the response.

NO request-time FMP / Finnhub / network calls.
NO profile enrichment.
NO new external APIs.
NO scheduler changes.
NO mutations of existing snapshot rows.

Garbage filtering, theme detection, watchlist/portfolio loading, and microcap
floor logic are reused from services.calendar_curation. Earnings rows already
carry their own importanceScore from the curated week-clean engine and are
NOT re-curated through calendar_curation (which would discard earnings rows
because that module intentionally excludes the earnings tab).

Response envelope (matches required spec):

    {
      "tab": "top_catalysts",
      "mode": "weekly",
      "current_week": [...normalized events, sorted by score desc...],
      "previous_week": [],
      "last_updated": "<iso8601>" | null,
      "status": "ready" | "stale" | "empty"
    }
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, Optional

from data.cache import cache
from services.calendar_curation import (
    CANONICAL_SYMBOL_MAP,
    MC_FLOOR,
    _canonical_symbol,
    _is_preferred_or_junk,
    _theme_score,
    _THEMES,
)
from services.calendar_snapshot_service import (
    TARGET_TABS as _SNAPSHOT_TABS,
    get_snapshot as _get_snapshot,
)

# Import lazily inside functions where appropriate to avoid heavy import-time work.

# ── Tunables ────────────────────────────────────────────────────────────────

# Hard cap on returned events. Spec says 25–50. Default 40.
DEFAULT_CAP: int = 40
MAX_CAP: int = 50
MIN_CAP: int = 25

# Importance ordering: earnings > IPO > macro > splits > dividends.
# Higher number = higher base weight.
_EVENT_TYPE_BASE: dict[str, float] = {
    "earnings_dates":     12.0,
    "earnings":           12.0,
    "ipo":                 9.0,
    "ipos":                9.0,
    "economic_release":    7.0,
    "economic_releases":   7.0,
    "treasury_rate":       6.0,
    "treasury_macro":      6.0,
    "stock_split":         5.0,
    "splits":              5.0,
    "dividend":            3.0,
    "dividends":           3.0,
}

# Importance string → bonus.
_IMPORTANCE_BONUS: dict[str, float] = {"high": 6.0, "medium": 3.0, "low": 0.0}


# ── Watchlist / portfolio (best-effort, no network) ─────────────────────────

def _load_watchlist_set() -> set[str]:
    try:
        from services.earnings_clean_service import _load_watchlist
        return _load_watchlist() or set()
    except Exception:
        return set()


def _load_portfolio_set() -> set[str]:
    try:
        from services.earnings_clean_service import _load_portfolio
        return _load_portfolio() or set()
    except Exception:
        return set()


# ── Week bounds ─────────────────────────────────────────────────────────────

def _week_bounds(today: Optional[date] = None) -> tuple[date, date]:
    today = today or datetime.now(timezone.utc).date()
    monday = today - timedelta(days=today.weekday())
    friday = monday + timedelta(days=4)
    return monday, friday


def _in_current_week(ev_date: Optional[str], monday: date, friday: date) -> bool:
    if not ev_date:
        return True  # Don't drop on missing date — let downstream sort handle it.
    try:
        d = datetime.strptime(ev_date[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return True
    return monday <= d <= friday


# ── Earnings cache read (no fetch) ──────────────────────────────────────────

def _read_earnings_week_cache(monday: date, friday: date) -> Optional[dict]:
    """
    Read the week-clean earnings result from the in-memory cache. Returns
    None if no cached entry — the route NEVER triggers a fresh fetch.
    Cache key mirrors services.earnings_clean_service.get_week_clean.
    """
    ck = f"earnings:curated:week:{monday.strftime('%Y-%m-%d')}:{friday.strftime('%Y-%m-%d')}"
    hit = cache.get(ck)
    if isinstance(hit, dict):
        return hit
    return None


# ── Normalization ───────────────────────────────────────────────────────────

def _is_earnings_event(ev: dict) -> bool:
    et = (ev.get("eventType") or "").lower()
    return et in ("earnings", "earnings_dates")


def _source_tab_for(et: str) -> str:
    et = (et or "").lower()
    if et in ("earnings", "earnings_dates"):
        return "earnings"
    if et in ("ipo", "ipos"):
        return "ipos"
    if et in ("dividend", "dividends"):
        return "dividends"
    if et in ("stock_split", "splits"):
        return "splits"
    if et in ("economic_release", "economic_releases"):
        return "economic_releases"
    if et in ("treasury_rate", "treasury_macro"):
        return "treasury_macro"
    return et or "unknown"


def _normalize_event(ev: dict, source_tab: str) -> dict:
    """
    Project an event from any source onto the common Top-Catalysts shape.
    All fields are best-effort — missing keys remain absent rather than null
    so the caller can rely on `in` checks.
    """
    if not isinstance(ev, dict):
        return {}
    sym_raw = ev.get("symbol")
    canon = _canonical_symbol(sym_raw) if sym_raw else None
    sym = canon if canon else sym_raw

    et = ev.get("eventType") or source_tab
    out: dict[str, Any] = {
        "symbol":      sym,
        "title":       ev.get("title") or ev.get("companyName") or ev.get("eventName") or sym or "",
        "date":        ev.get("date") or "",
        "eventType":   et,
        "sourceTab":   source_tab,
    }
    # Optional pass-through fields — only include when present.
    for k in ("time", "subtitle", "keyDetails", "details", "companyName",
              "sector", "industry", "marketCap", "marketCapBucket",
              "importance", "exchange", "country", "session",
              "epsEstimated", "epsActual", "revenueEstimated", "revenueActual",
              "surprise", "surprisePercent", "priceRange", "shares",
              "dividend", "splitRatio", "numerator", "denominator",
              "maturity", "indicatorName", "eventName", "themeTags",
              "isThemeAnchor", "isBottleneck", "scoreBreakdown",
              "relativeVolume"):
        if ev.get(k) is not None and ev.get(k) != "":
            out[k] = ev[k]
    # Always keep the raw payload for debugging / downstream consumers.
    out["raw"] = ev.get("raw") if isinstance(ev.get("raw"), dict) else {
        kk: vv for kk, vv in ev.items() if kk not in out
    }
    return out


# ── Garbage filter ──────────────────────────────────────────────────────────

def _passes_garbage_filter(ev: dict) -> bool:
    """
    Drop preferreds, warrants, units, rights, delisted symbols, and stale
    canonical aliases (FB→META). Mirrors services.calendar_curation rules
    but is permissive on missing metadata so we don't over-filter.
    """
    et = (ev.get("eventType") or "").lower()
    sym = ev.get("symbol") or ""
    name = ev.get("companyName") or ev.get("title") or ""

    # Symbol-bearing rows: enforce share-class / preferred / warrant rules.
    if et in ("earnings", "earnings_dates", "dividend", "dividends",
              "ipo", "ipos", "stock_split", "splits"):
        if _is_preferred_or_junk(sym, name):
            return False
        if sym:
            up = sym.strip().upper()
            if up in CANONICAL_SYMBOL_MAP and CANONICAL_SYMBOL_MAP[up] is None:
                return False
        # Microcap floor: only when marketCap is present.
        mc = ev.get("marketCap")
        try:
            mc_f = float(mc) if mc is not None else None
        except (TypeError, ValueError):
            mc_f = None
        if mc_f is not None and mc_f < MC_FLOOR:
            return False

    return True


# ── Scoring ─────────────────────────────────────────────────────────────────

def _surprise_bonus(ev: dict) -> tuple[float, list[str]]:
    """Earnings & IPO surprise/abnormal signals from existing fields only."""
    score = 0.0
    reasons: list[str] = []
    # Earnings surprise.
    sp = ev.get("surprisePercent")
    try:
        spf = float(sp) if sp is not None else None
    except (TypeError, ValueError):
        spf = None
    if spf is not None:
        if abs(spf) >= 10:
            score += 4
            reasons.append(f"large eps surprise {spf:+.1f}%")
        elif abs(spf) >= 3:
            score += 2
            reasons.append(f"eps surprise {spf:+.1f}%")

    # Earnings actuals already published (post-event).
    if ev.get("epsActual") is not None and ev.get("epsEstimated") is not None:
        try:
            beat = float(ev["epsActual"]) - float(ev["epsEstimated"])
            if abs(beat) > 0:
                # Only add if not already covered by surprisePercent.
                if spf is None:
                    score += 1
                    reasons.append("eps actual vs estimate available")
        except (TypeError, ValueError):
            pass

    # IPO notable bonus.
    et = (ev.get("eventType") or "").lower()
    if et in ("ipo", "ipos"):
        try:
            mc = float(ev.get("marketCap") or 0)
            if mc >= 5_000_000_000:
                score += 3
                reasons.append("notable IPO size")
            elif mc >= 1_000_000_000:
                score += 2
                reasons.append("sizeable IPO")
        except (TypeError, ValueError):
            pass
        if (ev.get("priceRange") or "").strip():
            score += 0.5

    # Splits ratio sign.
    if et in ("stock_split", "splits"):
        try:
            num = float(ev.get("numerator") or 0)
            den = float(ev.get("denominator") or 0)
            if num and den:
                if num > den:
                    score += 1.5
                    reasons.append("forward split")
                elif num < den:
                    score -= 1.5
                    reasons.append("reverse split")
        except (TypeError, ValueError):
            pass

    # Dividends — high yield hint (raw amount only; no price lookup).
    if et in ("dividend", "dividends"):
        try:
            d = float(ev.get("dividend") or 0)
            if d > 1.0:
                score += 0.5
                reasons.append("notable dividend amount")
        except (TypeError, ValueError):
            pass

    # Macro releases — high-impact bonus.
    if et in ("economic_release", "economic_releases"):
        # If importance already labeled high, _importance_bonus covers it; add
        # a small extra so high-impact macro outranks splits/dividends overall.
        if (ev.get("importance") or "").lower() == "high":
            score += 2
            reasons.append("high-impact macro")

    # Treasury — already covered by event-type base; nothing more here.

    return score, reasons


def _liquidity_bonus(ev: dict) -> tuple[float, list[str]]:
    """Use only fields already on the event — no fetch."""
    rel = ev.get("relativeVolume")
    if rel is None and isinstance(ev.get("raw"), dict):
        rel = ev["raw"].get("relativeVolume")
    try:
        rf = float(rel) if rel is not None else None
    except (TypeError, ValueError):
        rf = None
    if rf is None:
        return 0.0, []
    if rf >= 2.0:
        return 3.0, [f"relVol {rf:.1f}x"]
    if rf >= 1.3:
        return 1.0, [f"relVol {rf:.1f}x"]
    return 0.0, []


def _theme_bonus(ev: dict, watchlist: set[str]) -> tuple[float, list[str]]:
    """
    Score themes against existing fields. Returns score and reasons listing
    each theme that hit. Mirrors calendar_curation._theme_score but reports
    which themes matched.
    """
    sym = (ev.get("symbol") or "").upper()
    bag_parts = [
        sym,
        ev.get("companyName") or "",
        ev.get("title") or "",
        ev.get("sector") or "",
        ev.get("industry") or "",
        ev.get("eventName") or "",
    ]
    bag = " ".join(p for p in bag_parts if p).lower()
    if not bag.strip():
        return 0.0, []
    score = 0.0
    hits: list[str] = []
    for theme, kws in _THEMES.items():
        for kw in kws:
            if kw in bag:
                score += 1
                hits.append(theme)
                break
    return score, hits


def _score_event(ev: dict, watchlist: set[str], portfolio: set[str]) -> tuple[float, list[str], list[str]]:
    """
    Compute (score, scoreReasons, themeTags) using existing fields only.
    No network. No profile lookup.
    """
    et = (ev.get("eventType") or "").lower()
    score = float(_EVENT_TYPE_BASE.get(et, 1.0))
    reasons: list[str] = [f"{_source_tab_for(et)} event"]

    # Importance-string bonus (high/medium/low).
    imp = (ev.get("importance") or "").lower()
    score += _IMPORTANCE_BONUS.get(imp, 0.0)
    if imp == "high":
        reasons.append("high importance")
    elif imp == "medium":
        reasons.append("medium importance")

    # Earnings curated importanceScore — fold in if already present.
    iscore = ev.get("importanceScore")
    if isinstance(iscore, (int, float)):
        # Cap contribution so earnings doesn't sweep the whole list.
        score += min(float(iscore) / 4.0, 8.0)
        if iscore >= 60:
            reasons.append("earnings high importanceScore")

    # Watchlist / portfolio relevance.
    sym = (ev.get("symbol") or "").upper()
    if sym and sym in watchlist:
        score += 4
        reasons.append("watchlist hit")
    if sym and sym in portfolio:
        score += 6
        reasons.append("portfolio hit")

    # Theme tags.
    th_score, th_tags = _theme_bonus(ev, watchlist)
    if th_score:
        score += th_score
        if th_tags:
            reasons.append(f"themes: {', '.join(th_tags[:3])}")

    # Existing themeTags from earnings curation.
    pre_tags = ev.get("themeTags") or []
    if isinstance(pre_tags, list):
        for t in pre_tags:
            if isinstance(t, str) and t and t not in th_tags:
                th_tags.append(t)
        if pre_tags:
            score += 1.0  # small confidence bonus

    # Surprise / abnormal potential.
    sp_score, sp_reasons = _surprise_bonus(ev)
    score += sp_score
    reasons.extend(sp_reasons)

    # Liquidity if fields present.
    lq_score, lq_reasons = _liquidity_bonus(ev)
    score += lq_score
    reasons.extend(lq_reasons)

    # Mild market-cap log-bucket bonus (do not let it dominate).
    try:
        mc = float(ev.get("marketCap") or 0)
        if mc >= 200_000_000_000:
            score += 1.5
        elif mc >= 50_000_000_000:
            score += 1.0
        elif mc >= 10_000_000_000:
            score += 0.5
    except (TypeError, ValueError):
        pass

    return score, reasons, th_tags


# ── Dedup / merge by symbol ─────────────────────────────────────────────────

def _merge_dedup(events: list[dict]) -> list[dict]:
    """
    Merge multiple catalysts for the same symbol within the week.
    - Earnings dominates as the main event when present.
    - Otherwise the highest-scoring catalyst wins as the main event.
    - All secondary catalysts kept under raw['secondaryCatalysts'].
    - Score gets a multi-catalyst boost.
    """
    by_symbol: dict[str, list[dict]] = {}
    no_symbol: list[dict] = []

    for ev in events:
        sym = (ev.get("symbol") or "").upper()
        if not sym:
            no_symbol.append(ev)
        else:
            by_symbol.setdefault(sym, []).append(ev)

    merged: list[dict] = []
    for sym, group in by_symbol.items():
        if len(group) == 1:
            merged.append(group[0])
            continue

        # Pick the dominant event: earnings first, else highest score.
        earnings = [e for e in group if _is_earnings_event(e)]
        if earnings:
            primary = max(earnings, key=lambda e: e.get("score", 0.0))
        else:
            primary = max(group, key=lambda e: e.get("score", 0.0))

        secondaries = [e for e in group if e is not primary]
        new_ev = dict(primary)
        # Boost score for multiple catalysts.
        boost = 2.0 + min(len(secondaries) * 0.5, 2.5)
        new_ev["score"] = float(new_ev.get("score", 0.0)) + boost
        reasons = list(new_ev.get("scoreReasons") or [])
        reasons.append(f"multiple catalysts this week ({len(group)})")
        new_ev["scoreReasons"] = reasons

        # Stash the secondaries under raw, keeping the rest of raw intact.
        raw = dict(new_ev.get("raw") or {})
        raw["secondaryCatalysts"] = [
            {
                "eventType": e.get("eventType"),
                "sourceTab": e.get("sourceTab"),
                "date":      e.get("date"),
                "title":     e.get("title"),
                "score":     e.get("score"),
            }
            for e in secondaries
        ]
        new_ev["raw"] = raw
        merged.append(new_ev)

    # Symbol-less events (macro, treasury) kept individually — dedup by
    # (eventType, country, eventName/title, date) so that a single FOMC
    # appearing in both economic_releases and treasury_macro merges.
    seen: dict[tuple, dict] = {}
    for ev in no_symbol:
        key = (
            (ev.get("eventType") or "").lower(),
            (ev.get("country") or "").upper(),
            (ev.get("eventName") or ev.get("title") or "").lower(),
            ev.get("date") or "",
        )
        prev = seen.get(key)
        if prev is None or float(ev.get("score", 0.0)) > float(prev.get("score", 0.0)):
            seen[key] = ev
    merged.extend(seen.values())

    return merged


# ── Public entry point ──────────────────────────────────────────────────────

def get_top_catalysts(
    *,
    cap: int = DEFAULT_CAP,
    today: Optional[date] = None,
) -> dict:
    """
    Build the Top Catalysts envelope from already-cached data only.

    No network calls. No FMP. No profile enrichment.
    """
    cap = max(MIN_CAP, min(int(cap or DEFAULT_CAP), MAX_CAP))

    monday, friday = _week_bounds(today)
    watchlist = _load_watchlist_set()
    portfolio = _load_portfolio_set()

    pool: list[dict] = []
    last_updated_candidates: list[str] = []
    sources_seen: dict[str, int] = {}

    # 1. Snapshot tabs (dividends, ipos, splits, economic_releases, treasury_macro).
    for tab in _SNAPSHOT_TABS:
        try:
            env = _get_snapshot(tab) or {}
        except Exception as e:
            print(f"[top_catalysts] snapshot read failed tab={tab}: {e}")
            continue
        cw = env.get("current_week") or []
        if env.get("last_updated"):
            last_updated_candidates.append(env["last_updated"])
        n_added = 0
        for ev in cw:
            if not isinstance(ev, dict):
                continue
            if not _in_current_week(ev.get("date"), monday, friday):
                continue
            if not _passes_garbage_filter(ev):
                continue
            norm = _normalize_event(ev, source_tab=tab)
            if not norm:
                continue
            pool.append(norm)
            n_added += 1
        sources_seen[tab] = n_added

    # 2. Earnings — read-only cache lookup.
    earnings_added = 0
    earn = _read_earnings_week_cache(monday, friday)
    if earn:
        as_of = earn.get("asOf")
        if as_of:
            last_updated_candidates.append(as_of)
        # Prefer topEvents (already de-duped by week-clean engine).
        top_events = earn.get("topEvents") or []
        for ev in top_events:
            if not isinstance(ev, dict):
                continue
            if not _passes_garbage_filter(ev):
                continue
            norm = _normalize_event(ev, source_tab="earnings")
            pool.append(norm)
            earnings_added += 1
    sources_seen["earnings"] = earnings_added

    # 3. Score every pooled event (single pass, no network).
    for ev in pool:
        score, reasons, themes = _score_event(ev, watchlist, portfolio)
        ev["score"] = round(score, 3)
        ev["scoreReasons"] = reasons
        ev["themeTags"] = themes
        ev["sourceTab"] = ev.get("sourceTab") or _source_tab_for(ev.get("eventType") or "")

    # 4. Dedup + merge per-symbol (with earnings dominance).
    merged = _merge_dedup(pool)

    # 5. Sort by score desc, then date asc as tiebreak.
    merged.sort(
        key=lambda e: (-float(e.get("score") or 0.0), e.get("date") or "9999-12-31"),
    )

    # 6. Cap.
    final = merged[:cap]

    # 7. Status / last_updated.
    last_updated = max(last_updated_candidates) if last_updated_candidates else None
    if final:
        status = "ready"
    elif last_updated:
        status = "stale"
    else:
        status = "empty"

    print(
        f"[top_catalysts] week={monday}→{friday} pool={len(pool)} "
        f"merged={len(merged)} returned={len(final)} status={status} "
        f"sources={sources_seen}"
    )

    return {
        "tab":           "top_catalysts",
        "mode":          "weekly",
        "current_week":  final,
        "previous_week": [],
        "last_updated":  last_updated,
        "status":        status,
    }
