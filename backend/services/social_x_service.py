"""
Social page X-dashboard service.

Derives 4 Social sections from the existing x_consensus_cache snapshot —
NO additional Grok / XAI calls are made here.

Sections:
  A. x_consensus         — from raw.consensus_picks            [UNCHANGED]
  B. freshest_alpha      — deterministic from _mention_data    [REWRITTEN]
  C. theme_leadership    — from raw.hype_radar + raw.market_pulse [UNCHANGED]
  D. sentiment_accel     — deterministic from _mention_data    [REWRITTEN]

B and D were previously driven by Grok-written fields (raw.fresh_trades,
raw.consensus_picks delta).  They now use _mention_data — the per-account,
per-mention extraction saved during Phase-1 of each refresh cycle — to
derive rankings deterministically without any additional API calls.

freshest_alpha:
  • ONLY top_trader + above_average_trader accounts
  • Aggressive recency weighting (0d=3.0× vs 14d=0.3×)
  • Novelty boost for tickers absent from prior backend-ranked top-10
  • Goal: "what are my best active traders NEWLY calling?"

sentiment_acceleration:
  • ALL accounts (excl. macro_big_picture for ticker scoring)
  • Multi-window scoring: 3d / 7d / 14d / 30d / 90d
  • Acceleration = slope steepening (3d hotter than 7d hotter than 14d)
  • Goal: "which consensus names are getting stronger and stronger lately?"

Fallback: if _mention_data is absent (pre-existing snapshot before this
update), both sections fall back to the legacy Grok-field approach so the
dashboard never goes empty.
"""
from __future__ import annotations

import time
from typing import Any, Optional


# ── Freshest Alpha tier config ────────────────────────────────────────────────

# Only these two tiers are eligible for Freshest Alpha
_FA_ELIGIBLE_TIERS: frozenset[str] = frozenset({"top_trader", "above_average_trader"})

# Aggressive recency boosts for FA (much steeper than the general scoring buckets).
# "Fresh" means recent — a 14-day-old call is NOT freshest alpha.
_FA_RECENCY_BOOSTS: list[tuple[int, float]] = [
    (0,  3.0),   # today
    (1,  2.5),   # yesterday
    (3,  2.0),   # last 3 days
    (7,  1.0),   # last week
    (14, 0.3),   # 2 weeks — barely qualifies
]
_FA_RECENCY_FALLBACK = 0.05  # >14d effectively excluded


def _fa_recency_boost(days: int) -> float:
    for bound, w in _FA_RECENCY_BOOSTS:
        if days <= bound:
            return w
    return _FA_RECENCY_FALLBACK


# ── Snapshot loaders ─────────────────────────────────────────────────────────

def _load_snapshots() -> tuple[Optional[dict], Optional[dict]]:
    """Return (current_snap, prior_snap).  Either may be None if not on disk."""
    from services.x_consensus_cache import _load_disk_cache, _load_prior_cache
    return _load_disk_cache(), _load_prior_cache()


def _raw(snapshot: Optional[dict]) -> dict:
    """Extract the raw Grok dict from a snapshot; return {} if absent."""
    if not snapshot or not isinstance(snapshot, dict):
        return {}
    r = snapshot.get("raw")
    return r if isinstance(r, dict) else {}


def _name_lookup(current_snap: Optional[dict]) -> dict[str, tuple[str, str]]:
    """
    Build a {ticker: (name, tradingview_symbol)} lookup from consensus_picks.
    Used to annotate FA and SA results with display names without extra API calls.
    """
    lookup: dict[str, tuple[str, str]] = {}
    raw = _raw(current_snap)
    for p in (raw.get("consensus_picks") or []):
        if not isinstance(p, dict):
            continue
        t = (p.get("ticker") or "").upper().lstrip("$").strip()
        if t:
            lookup[t] = (
                p.get("name") or "",
                p.get("tradingview_symbol") or "",
            )
    # Also check fresh_trades (Grok may have names there too)
    for ft in (raw.get("fresh_trades") or []):
        if not isinstance(ft, dict):
            continue
        t = (ft.get("ticker") or "").upper().lstrip("$").strip()
        if t and t not in lookup:
            lookup[t] = (
                ft.get("name") or "",
                ft.get("tradingview_symbol") or "",
            )
    return lookup


# ── Section A — X Consensus ─────────────────────────────────────────────────

def _build_x_consensus(raw: dict) -> list[dict]:
    """
    Normalise consensus_picks into the Social-page row format.
    All fields come directly from the Grok contract output.
    UNCHANGED from original implementation.
    """
    picks = raw.get("consensus_picks") or []
    out: list[dict] = []
    for p in picks:
        if not isinstance(p, dict):
            continue
        ticker = (p.get("ticker") or "").upper().lstrip("$")
        if not ticker:
            continue
        out.append({
            "rank":               p.get("rank"),
            "ticker":             ticker,
            "name":               p.get("name") or "",
            "tradingview_symbol": p.get("tradingview_symbol") or "",
            "hype_score":         p.get("hype_score"),
            "trader_count":       p.get("trader_count"),
            "consensus_strength": p.get("consensus_strength") or "",
            "buzz_trend":         p.get("buzz_trend") or "",
            "thesis":             p.get("thesis") or "",
            "catalysts":          p.get("catalysts") or [],
            "risk":               p.get("risk") or "",
            "is_fresh_trade":     bool(p.get("is_fresh_trade")),
            "fresh_trade_note":   p.get("fresh_trade_note"),
            "trader_theses":      p.get("trader_theses") or [],
        })
    return out


# ── Section B — Freshest Alpha (rewritten) ───────────────────────────────────

def _build_freshest_alpha(
    current_snap: Optional[dict],
    prior_snap: Optional[dict],
) -> dict:
    """
    Surface newly emerging ticker calls from ONLY top_trader + above_average_trader
    accounts, using per-mention recency extracted during Phase-1.

    Scoring per qualifying mention:
      base = tier_weight × fa_recency_boost(recency_days) × conviction_mult
    Final ticker score:
      base_sum × novelty_mult × breadth_mult

    novelty_mult:
      • Ticker absent from prior _backend_ranked top-10:   ×1.5 (brand-new call)
      • Ticker in prior _backend_ranked rank 11-20:        ×1.2 (emerging)
      • Ticker in prior _backend_ranked rank 6-10:         ×0.9 (somewhat known)
      • Ticker in prior _backend_ranked rank 1-5:          ×0.6 (established fav)

    Only tickers with min_recency_days ≤ 14 are included (older is not fresh alpha).

    Fallback: if _mention_data is missing, falls back to Grok's raw.fresh_trades.
    """
    mention_data: list[dict] = (current_snap or {}).get("_mention_data") or []

    if not mention_data:
        # Legacy fallback — Grok wrote fresh_trades directly
        return _build_freshest_alpha_legacy(_raw(current_snap))

    # Prior backend-rank lookup for novelty detection
    prior_br: list[dict] = (prior_snap or {}).get("_backend_ranked") or []
    prior_rank_by_ticker: dict[str, int] = {
        s["ticker"]: i for i, s in enumerate(prior_br)
    }

    name_map = _name_lookup(current_snap)
    buckets: dict[str, dict] = {}

    for acct in mention_data:
        if acct.get("category") not in _FA_ELIGIBLE_TIERS:
            continue
        handle   = acct.get("handle", "")
        tier_w   = float(acct.get("weight") or 0.0)
        if tier_w <= 0:
            continue

        for m in (acct.get("mentions") or []):
            if not isinstance(m, dict):
                continue
            if (m.get("sentiment") or "neutral").lower() != "bullish":
                continue
            ticker = (m.get("ticker") or "").upper().strip().lstrip("$")
            if not ticker or len(ticker) > 12 or " " in ticker:
                continue

            rd_raw = m.get("recency_days")
            recency_days = int(rd_raw) if rd_raw is not None else 30
            recency_days = max(0, min(recency_days, 365))

            if recency_days > 14:
                continue  # too stale for freshest alpha

            fa_boost = _fa_recency_boost(recency_days)
            conviction = (m.get("conviction") or "medium").lower()
            conv_mult = {"high": 1.3, "medium": 1.0, "low": 0.6}.get(conviction, 1.0)

            mention_score = tier_w * fa_boost * conv_mult

            if ticker not in buckets:
                buckets[ticker] = {
                    "ticker":       ticker,
                    "score":        0.0,
                    "min_recency":  9999,
                    "accounts":     {},          # handle → contribution
                    "theses":       [],
                    "catalysts":    [],
                    "conviction_seen": set(),
                }
            b = buckets[ticker]
            b["score"] += mention_score
            b["min_recency"] = min(b["min_recency"], recency_days)
            b["accounts"][handle] = b["accounts"].get(handle, 0.0) + mention_score
            thesis = (m.get("thesis") or "").strip()
            if thesis:
                b["theses"].append({"handle": handle, "tier": acct.get("category"), "text": thesis})
            b["catalysts"].extend([str(c) for c in (m.get("catalysts") or []) if c])
            b["conviction_seen"].add(conviction)

    results: list[dict] = []
    for ticker, b in buckets.items():
        if b["min_recency"] > 14 or b["score"] <= 0:
            continue

        # Novelty multiplier (prior snapshot comparison)
        prior_rank = prior_rank_by_ticker.get(ticker)
        if prior_rank is None:
            novelty_mult = 1.5    # brand new — not in prior at all
        elif prior_rank >= 10:
            novelty_mult = 1.2    # appeared before but outside top-10
        elif prior_rank >= 5:
            novelty_mult = 0.9    # seen, moderately established
        else:
            novelty_mult = 0.6    # rank 0-4: well-established favorite

        n_accts = len(b["accounts"])
        breadth_mult = 1.0 + min(0.20 * (n_accts - 1), 0.40)

        final_score = b["score"] * novelty_mult * breadth_mult

        # Pick the highest-tier thesis
        tier_order = {"top_trader": 0, "above_average_trader": 1}
        top_thesis_entry = min(
            b["theses"],
            key=lambda t: tier_order.get(t["tier"], 9),
        ) if b["theses"] else None

        name, tv_sym = name_map.get(ticker, ("", ""))
        accts_sorted = sorted(b["accounts"].items(), key=lambda x: -x[1])

        results.append({
            # ── Backward-compat fields ───────────────────────────────────
            "ticker":              ticker,
            "name":                name,
            "tradingview_symbol":  tv_sym,
            "first_mentioned_by":  [f"@{h}" for h, _ in accts_sorted],
            "why_fresh": (
                f"{'New' if prior_rank is None else 'Emerging'} call "
                f"from {n_accts} top-quality trader(s) within last "
                f"{b['min_recency']}d"
                + (f" — {', '.join(sorted(b['conviction_seen']))} conviction" if b["conviction_seen"] else "")
            ),
            "entry_thesis":        top_thesis_entry["text"] if top_thesis_entry else "",
            "spotlight_badge":     False,   # set on top result below
            "spotlight_signal":    None,    # set on top result below
            # ── New enrichment fields ────────────────────────────────────
            "freshest_alpha_score":   round(final_score, 3),
            "min_recency_days":       b["min_recency"] if b["min_recency"] < 9999 else None,
            "quality_account_count":  n_accts,
            "is_brand_new":           prior_rank is None,
            "novelty_mult":           round(novelty_mult, 2),
            "catalysts":              list(dict.fromkeys(b["catalysts"]))[:5],
            "top_accounts":           [{"handle": h, "contribution": round(s, 3)} for h, s in accts_sorted],
        })

    results.sort(key=lambda x: -x["freshest_alpha_score"])

    # Mark the spotlight entry (highest score)
    if results:
        results[0]["spotlight_badge"]  = True
        results[0]["spotlight_signal"] = (
            f"{'Brand-new' if results[0]['is_brand_new'] else 'Emerging'} "
            f"— {results[0]['quality_account_count']} top trader(s), "
            f"recency ≤{results[0]['min_recency_days']}d"
        )

    spotlight = None
    if results:
        top = results[0]
        spotlight = {
            "ticker": top["ticker"],
            "signal": top["spotlight_signal"],
        }

    return {"trades": results[:10], "spotlight": spotlight}


def _build_freshest_alpha_legacy(raw: dict) -> dict:
    """Legacy fallback: read Grok-written fresh_trades + spotlight.freshest_alpha."""
    fresh_trades = raw.get("fresh_trades") or []
    spotlight    = raw.get("spotlight") or {}
    spotlight_fa = spotlight.get("freshest_alpha") or {}
    spotlight_ticker = (spotlight_fa.get("ticker") or "").upper().lstrip("$")

    trades: list[dict] = []
    for t in fresh_trades:
        if not isinstance(t, dict):
            continue
        ticker = (t.get("ticker") or "").upper().lstrip("$")
        if not ticker:
            continue
        is_spotlight = ticker == spotlight_ticker
        trades.append({
            "ticker":             ticker,
            "name":               t.get("name") or "",
            "tradingview_symbol": t.get("tradingview_symbol") or "",
            "first_mentioned_by": t.get("first_mentioned_by") or [],
            "why_fresh":          t.get("why_fresh") or "",
            "entry_thesis":       t.get("entry_thesis") or "",
            "spotlight_badge":    is_spotlight,
            "spotlight_signal":   spotlight_fa.get("signal") if is_spotlight else None,
        })

    return {
        "trades": trades,
        "spotlight": {
            "ticker": spotlight_ticker or None,
            "signal": spotlight_fa.get("signal") or None,
        } if spotlight_ticker else None,
    }


# ── Section C — Theme Leadership ─────────────────────────────────────────────

def _build_theme_leadership(raw: dict) -> dict:
    """
    Return hype_radar rows enriched with market_pulse context.
    Source: raw.hype_radar + raw.market_pulse.
    UNCHANGED from original implementation.
    """
    hype_radar   = raw.get("hype_radar") or []
    market_pulse = raw.get("market_pulse") or {}

    themes: list[dict] = []
    for h in hype_radar:
        if not isinstance(h, dict):
            continue
        buzz = (h.get("buzz_level") or "").lower()
        themes.append({
            "theme":       h.get("theme") or "",
            "buzz_level":  buzz,
            "key_tickers": h.get("key_tickers") or [],
            "why_hot":     h.get("why_hot") or "",
            "buzz_rank":   {"extreme": 0, "high": 1, "moderate": 2}.get(buzz, 3),
        })

    themes.sort(key=lambda t: t.pop("buzz_rank"))

    return {
        "themes": themes,
        "market_pulse": {
            "verdict": market_pulse.get("verdict") or "",
            "summary": market_pulse.get("summary") or "",
            "regime":  market_pulse.get("regime") or "",
        },
    }


# ── Section D — Sentiment Acceleration (rewritten) ──────────────────────────

# Multi-window definitions for acceleration analysis
# name, upper_bound_days_inclusive
_SA_WINDOWS: list[tuple[str, int]] = [
    ("w3",  3),
    ("w7",  7),
    ("w14", 14),
    ("w30", 30),
    ("w90", 90),
]


def _sa_consensus_strength(account_count: int, accel_score: float) -> str:
    if account_count >= 4 and accel_score >= 5.0:
        return "Very High"
    if account_count >= 3 or accel_score >= 3.0:
        return "High"
    if account_count >= 2 or accel_score >= 1.5:
        return "Medium"
    return "Low"


def _sa_buzz_trend(slope_7_to_3: float, slope_14_to_7: float, w3_vs_w14: float = 0.0) -> str:
    """
    Classify momentum trend from three complementary slope signals.

    slope_7_to_3  — w3/(w7+ε): > 1 means last 3d hotter than 7d
    slope_14_to_7 — w7/(w14+ε): > 1 means last 7d hotter than 14d
    w3_vs_w14     — w3/(w14+ε): fraction of 14d activity concentrated in last 3d.
                    Catches the case where ALL mentions are ≤3d (w3==w7, slope≈1).
    """
    # Classic steepening: each window hotter than the one before
    if slope_7_to_3 >= 1.5 and slope_14_to_7 >= 1.2:
        return "Accelerating"
    # Most 14d activity is concentrated in last 3 days (high recency clustering)
    if w3_vs_w14 >= 0.6 and slope_14_to_7 >= 0.6:
        return "Accelerating"
    if slope_7_to_3 >= 1.2 or w3_vs_w14 >= 0.7:
        return "Rising"
    if slope_7_to_3 >= 0.8 or w3_vs_w14 >= 0.35:
        return "Stable"
    return "Fading"


def _build_sentiment_accel(
    current_snap: Optional[dict],
    prior_snap: Optional[dict],
) -> list[dict]:
    """
    Surface tickers where social consensus is INTENSIFYING over time.

    Uses ALL account tiers (excl. macro_big_picture which has weight=0) and
    computes per-ticker weighted scores across five time windows:
      w3  = sum of tier×conviction×specificity for mentions with recency_days ≤ 3
      w7  = same for ≤ 7d
      w14 = same for ≤ 14d
      w30 = same for ≤ 30d
      w90 = same for ≤ 90d  (broadest; used as baseline)

    Acceleration score:
      base_intensity = w3×4.0 + w7×2.0 + w14×1.0 + w30×0.5
      accel_bonus    = 0 unless slopes are steepening:
        slope_7_to_3  = w3 / (w7+ε)   → bonus if > 0.5
        slope_14_to_7 = w7 / (w14+ε)  → bonus if > 0.5
        slope_30_to_14= w14/ (w30+ε)  → bonus if > 0.5
      breadth_mult   = 1 + 0.15×(n_accounts−1), capped at ×1.45
      final = base_intensity × (1+accel_bonus) × breadth_mult

    Tickers with no activity in the last 14 days are excluded.

    Fallback: if _mention_data is missing, falls back to the original
    hype_score delta approach.
    """
    mention_data: list[dict] = (current_snap or {}).get("_mention_data") or []

    if not mention_data:
        return _build_sentiment_accel_legacy(_raw(current_snap), _raw(prior_snap))

    name_map = _name_lookup(current_snap)

    # Prior backend ranked for trader_count comparison
    prior_br_by_ticker: dict[str, dict] = {
        s["ticker"]: s
        for s in ((prior_snap or {}).get("_backend_ranked") or [])
    }

    # Aggregate per-ticker across all windows
    buckets: dict[str, dict] = {}

    for acct in mention_data:
        category = acct.get("category", "")
        if category == "macro_big_picture":
            continue
        tier_w = float(acct.get("weight") or 0.0)
        if tier_w <= 0:
            continue
        handle = acct.get("handle", "")

        for m in (acct.get("mentions") or []):
            if not isinstance(m, dict):
                continue
            if (m.get("sentiment") or "neutral").lower() != "bullish":
                continue
            ticker = (m.get("ticker") or "").upper().strip().lstrip("$")
            if not ticker or len(ticker) > 12 or " " in ticker:
                continue

            rd_raw = m.get("recency_days")
            recency_days = int(rd_raw) if rd_raw is not None else 91
            recency_days = max(0, min(recency_days, 365))

            conviction = (m.get("conviction") or "medium").lower()
            conv_mult = {"high": 1.2, "medium": 1.0, "low": 0.7}.get(conviction, 1.0)
            catalysts = [str(c) for c in (m.get("catalysts") or []) if c]
            spec_mult = 1.2 if catalysts else 1.0
            base_score = tier_w * conv_mult * spec_mult

            if ticker not in buckets:
                buckets[ticker] = {
                    "ticker":      ticker,
                    "w3":  0.0, "w7": 0.0, "w14": 0.0, "w30": 0.0, "w90": 0.0,
                    "total":       0.0,
                    "accounts":    set(),
                    "min_recency": 9999,
                    "theses":      [],
                    "catalysts":   [],
                }
            b = buckets[ticker]
            b["total"] += base_score
            b["accounts"].add(handle)
            b["min_recency"] = min(b["min_recency"], recency_days)
            for win_name, win_days in _SA_WINDOWS:
                if recency_days <= win_days:
                    b[win_name] += base_score
            thesis = (m.get("thesis") or "").strip()
            if thesis:
                b["theses"].append({"handle": handle, "text": thesis})
            b["catalysts"].extend(catalysts)

    results: list[dict] = []
    for ticker, b in buckets.items():
        w3, w7, w14, w30, w90 = b["w3"], b["w7"], b["w14"], b["w30"], b["w90"]

        # Must have some activity in the last 14d to qualify
        if w14 <= 0:
            continue

        # Slope ratios (ε prevents div-by-zero; ratio > 1.0 means recent hotter)
        slope_7_to_3   = w3  / (w7  + 0.01)
        slope_14_to_7  = w7  / (w14 + 0.01)
        slope_30_to_14 = w14 / (w30 + 0.01)

        # Base intensity: heavily weight the most recent windows
        base_intensity = w3 * 4.0 + w7 * 2.0 + w14 * 1.0 + w30 * 0.5

        # Acceleration bonus: reward steepening slopes
        accel_bonus = 0.0
        if slope_7_to_3   > 0.5:
            accel_bonus += 0.3 * min(slope_7_to_3, 2.0)
        if slope_14_to_7  > 0.5:
            accel_bonus += 0.2 * min(slope_14_to_7, 2.0)
        if slope_30_to_14 > 0.5:
            accel_bonus += 0.1 * min(slope_30_to_14, 2.0)

        n_accts = len(b["accounts"])
        breadth_mult = 1.0 + min(0.15 * (n_accts - 1), 0.45)
        final_accel_score = base_intensity * (1.0 + accel_bonus) * breadth_mult

        # Rationale text
        parts: list[str] = []
        if slope_7_to_3 >= 1.5 and slope_14_to_7 >= 1.2:
            parts.append("Rapidly accelerating — stronger each window")
        elif slope_14_to_7 >= 1.2:
            parts.append("Building momentum (last 7d > 14d baseline)")
        elif slope_7_to_3 >= 1.2:
            parts.append("Late surge — 3d hotter than 7d")
        if w14 > 0 and w30 > 0 and w14 / (w30 + 0.01) > 0.8:
            parts.append("14d activity dominates 30d baseline")
        if n_accts >= 3:
            parts.append(f"{n_accts} accounts in consensus")
        elif n_accts == 2:
            parts.append("Cross-account agreement")

        # Top thesis
        top_thesis = b["theses"][0]["text"] if b["theses"] else ""

        # Prior comparison (for backward-compat fields)
        prior = prior_br_by_ticker.get(ticker)
        prior_acct_count = prior["bullish_account_count"] if prior else 0

        # Normalise accel_score to 0-100 range for hype_score compat
        # We cap at 50 as a practical upper bound for the normalised form
        norm_score = min(round(final_accel_score * 2.0), 100)
        prior_norm  = 0

        # Third slope signal: what fraction of 14d activity is in the last 3 days?
        # Catches the case where all mentions are ≤3d (w3==w7 → slope_7_to_3 ≈1.0 but
        # the pattern is still highly fresh / accelerating).
        w3_vs_w14    = w3 / (w14 + 0.01)
        buzz_trend   = _sa_buzz_trend(slope_7_to_3, slope_14_to_7, w3_vs_w14)
        con_strength = _sa_consensus_strength(n_accts, final_accel_score)

        name, tv_sym = name_map.get(ticker, ("", ""))

        results.append({
            # ── Backward-compat fields ───────────────────────────────────────
            "ticker":                    ticker,
            "name":                      name,
            "tradingview_symbol":        tv_sym,
            "current_hype_score":        norm_score,
            "prior_hype_score":          prior_norm,
            "hype_delta":                norm_score - prior_norm,
            "current_trader_count":      n_accts,
            "prior_trader_count":        prior_acct_count,
            "trader_count_delta":        n_accts - prior_acct_count,
            "current_consensus_strength": con_strength,
            "buzz_trend":                buzz_trend,
            "is_new_entry":              prior is None,
            "thesis":                    top_thesis,
            "why_now": "; ".join(parts) if parts else "Sustained momentum",
            # ── New enrichment fields ────────────────────────────────────────
            "accel_score":               round(final_accel_score, 3),
            "window_scores": {
                "w3":  round(w3,  3),
                "w7":  round(w7,  3),
                "w14": round(w14, 3),
                "w30": round(w30, 3),
                "w90": round(w90, 3),
            },
            "slope_7_to_3":              round(slope_7_to_3,  2),
            "slope_14_to_7":             round(slope_14_to_7, 2),
            "w3_vs_w14":                 round(w3_vs_w14, 2),
            "account_count":             n_accts,
            "min_recency_days":          b["min_recency"] if b["min_recency"] < 9999 else None,
            "catalysts":                 list(dict.fromkeys(b["catalysts"]))[:5],
        })

    results.sort(key=lambda x: -x["accel_score"])
    return results[:12]


def _build_sentiment_accel_legacy(current_raw: dict, prior_raw: dict) -> list[dict]:
    """
    Legacy fallback: compare current vs prior consensus_picks hype_score / trader_count.
    Used when _mention_data is not available in the snapshot (pre-update snapshots).
    """
    cur_picks  = current_raw.get("consensus_picks") or []
    prev_picks = prior_raw.get("consensus_picks") if prior_raw else []
    prev_picks = prev_picks or []

    prior_by_ticker: dict[str, dict] = {}
    for p in prev_picks:
        if not isinstance(p, dict):
            continue
        t = (p.get("ticker") or "").upper().lstrip("$")
        if t:
            prior_by_ticker[t] = p

    out: list[dict] = []
    for p in cur_picks:
        if not isinstance(p, dict):
            continue
        ticker = (p.get("ticker") or "").upper().lstrip("$")
        if not ticker:
            continue

        cur_score = p.get("hype_score") or 0
        cur_count = p.get("trader_count") or 0
        prior = prior_by_ticker.get(ticker)
        is_new = prior is None
        prev_score = (prior.get("hype_score") or 0) if prior else 0
        prev_count = (prior.get("trader_count") or 0) if prior else 0
        hype_delta   = cur_score - prev_score
        trader_delta = cur_count - prev_count

        if not is_new and hype_delta <= 0 and trader_delta <= 0:
            continue

        buzz_trend = p.get("buzz_trend") or ""
        parts = []
        if is_new:
            parts.append("New entry — not in prior snapshot")
        if hype_delta > 0:
            parts.append(f"Hype score up {hype_delta:+.0f}")
        if trader_delta > 0:
            parts.append(f"Trader count up {trader_delta:+d}")
        if buzz_trend in ("Accelerating", "New Mention"):
            parts.append(f"Buzz trend: {buzz_trend}")

        out.append({
            "ticker":                   ticker,
            "name":                     p.get("name") or "",
            "tradingview_symbol":        p.get("tradingview_symbol") or "",
            "current_hype_score":       cur_score,
            "prior_hype_score":         prev_score,
            "hype_delta":               hype_delta,
            "current_trader_count":     cur_count,
            "prior_trader_count":       prev_count,
            "trader_count_delta":       trader_delta,
            "current_consensus_strength": p.get("consensus_strength") or "",
            "buzz_trend":               buzz_trend,
            "is_new_entry":             is_new,
            "thesis":                   p.get("thesis") or "",
            "why_now": "; ".join(parts) if parts else "Continued momentum",
        })

    out.sort(key=lambda x: (not x["is_new_entry"], -x["hype_delta"], -x["current_hype_score"]))
    return out


# ── Metadata helpers ──────────────────────────────────────────────────────────

def _build_metadata(snapshot: Optional[dict]) -> dict:
    """Build the standard metadata block from a current snapshot.

    Key design: auto-schedule fields and manual-refresh fields are COMPLETELY
    SEPARATE so the frontend can enable/disable each button independently.

    Auto-schedule fields:
      auto_refresh_window_open    — True only 08:00–20:00 America/Chicago
      next_allowed_refresh_at     — next auto window open (ISO-8601 UTC); null if open now
      refresh_window_open         — kept for backward compat (same value as auto_refresh_window_open)

    Manual-refresh fields:
      manual_refresh_available    — True unless single-flight lock held OR cooldown active
      next_manual_refresh_allowed_at — ISO-8601 UTC cooldown expiry; null if never triggered
      manual_refresh_reason       — null when available; "refresh_in_progress" | "cooldown" when not
    """
    from services.x_consensus_cache import (
        _CACHE_TTL_SECONDS,
        _in_refresh_window,
        _next_window_open_iso,
        _REFRESH_LOCK,
        _manual_refresh_available,
        _next_manual_allowed_iso,
    )
    window_open      = _in_refresh_window()
    lock_held        = _REFRESH_LOCK.locked()
    cooldown_clear   = _manual_refresh_available()
    next_manual_iso  = _next_manual_allowed_iso()

    if lock_held:
        manual_available = False
        manual_reason    = "refresh_in_progress"
    elif not cooldown_clear:
        manual_available = False
        manual_reason    = "cooldown"
    else:
        manual_available = True
        manual_reason    = None

    if not snapshot:
        return {
            "updated_at":                    None,
            "data_state":                    "no_data_yet",
            "stale":                         True,
            "refresh_in_progress":           lock_held,
            "auto_refresh_window_open":      window_open,
            "refresh_window_open":           window_open,
            "next_allowed_refresh_at":       _next_window_open_iso() if not window_open else None,
            "manual_refresh_available":      manual_available,
            "next_manual_refresh_allowed_at": next_manual_iso,
            "manual_refresh_reason":         manual_reason,
            "source":                        "x_consensus_cache",
            "timezone":                      "America/Chicago",
        }

    saved_at = snapshot.get("_saved_at") or 0
    age_s    = time.time() - float(saved_at)
    from services.x_consensus_cache import _CACHE_TTL_SECONDS as _TTL
    is_stale = age_s >= _TTL

    return {
        "updated_at":                    snapshot.get("generated_at"),
        "data_state":                    "stale" if is_stale else "available",
        "stale":                         is_stale,
        "age_seconds":                   int(age_s),
        "refresh_in_progress":           lock_held,
        "auto_refresh_window_open":      window_open,
        "refresh_window_open":           window_open,
        "next_allowed_refresh_at":       _next_window_open_iso() if not window_open else None,
        "manual_refresh_available":      manual_available,
        "next_manual_refresh_allowed_at": next_manual_iso,
        "manual_refresh_reason":         manual_reason,
        "source":                        "x_consensus_cache",
        "timezone":                      "America/Chicago",
        "handles_count":                 len(snapshot.get("handles") or []),
    }


# ── Public entry point ────────────────────────────────────────────────────────

def build_x_dashboard() -> dict:
    """
    Build the Social X-dashboard payload from cached snapshots only.
    Zero Grok/XAI calls.

    Shape contract — the response is a MERGE of:

    A. The existing Home-style consensus payload (flat, unchanged):
         generated_at, top_tickers, key_themes, notable_accounts,
         is_stale, stale, data_state, age_seconds, refresh_in_progress,
         available, refresh_window_open, next_allowed_refresh_at, timezone

    B. Three Social-only sibling sections (additive):
         freshest_alpha       — deterministic from _mention_data (top_trader+above_avg only)
         theme_leadership     — from raw.hype_radar + raw.market_pulse [UNCHANGED]
         sentiment_acceleration — deterministic multi-window from _mention_data

    C. Extra convenience keys from the raw snapshot:
         market_pulse, portfolio_bias, spotlight

    D. Social-specific metadata (richer than the Home subset).
    """
    from services.x_consensus_cache import (
        _public_payload,
        _in_refresh_window,
        _REFRESH_LOCK,
    )

    current_snap, prior_snap = _load_snapshots()
    cur_raw = _raw(current_snap)

    window_open         = _in_refresh_window()
    refresh_in_progress = _REFRESH_LOCK.locked()
    home_payload = _public_payload(
        current_snap,
        refresh_in_progress=refresh_in_progress,
        window_open=window_open,
    )

    if not cur_raw:
        return {
            **home_payload,
            "market_pulse":           None,
            "portfolio_bias":         None,
            "spotlight":              None,
            "freshest_alpha":         {"trades": [], "spotlight": None},
            "theme_leadership":       {"themes": [], "market_pulse": None},
            "sentiment_acceleration": [],
            "metadata":               _build_metadata(None),
        }

    return {
        **home_payload,
        "market_pulse":   cur_raw.get("market_pulse"),
        "portfolio_bias": cur_raw.get("portfolio_bias"),
        "spotlight":      cur_raw.get("spotlight"),
        "freshest_alpha":         _build_freshest_alpha(current_snap, prior_snap),
        "theme_leadership":       _build_theme_leadership(cur_raw),
        "sentiment_acceleration": _build_sentiment_accel(current_snap, prior_snap),
        "metadata":               _build_metadata(current_snap),
    }
